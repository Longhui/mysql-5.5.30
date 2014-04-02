/**
 * 带死锁检测的通用锁表
 *
 * @author 李伟钊(liweizhao@corp.netease.com)
 */
#include "misc/DLDLockTable.h"
#include "misc/MemCtx.h"

namespace tnt {

/**
 * 锁授予
 */
void DldLock::lockGrant() {
	m_status = DLS_GRANTED;
	assert(NULL != m_lockOwner);
	m_lockOwner->unlinkFromWaiterList();
	m_lockOwner->setWaitLock(NULL);
	m_lockOwner->wakeUp();
}

/**
 * 取消正在等待的锁请求
 */
void DldLock::cancelWaitingAndRelease(bool needWake) {
	assert(m_status == DLS_WAIT);
	m_status = DLS_FREE;
	assert(NULL != m_lockOwner);
	removeFromEntryList();
	m_lockOwner->unlinkFromWaiterList();
	m_lockOwner->setWaitLock(NULL);
	if (needWake) {
		m_lockOwner->wakeUp();
	}
}

/**
 * 将lock从对应的entryList中删除
 * @pre 操作之前需要持有该entry的互斥锁
 */
void DldLock::removeFromEntryList() {
	m_entryLink.unLink();
}

/**
 * 判断锁模式mode1是否大于等于锁模式mode2
 * @param mode1 锁模式
 * @param mode2 锁模式
 * @return mode1大于等于mode2返回true，否则返回false
 */
bool DldLock::lockModeGreaterOrEq(DldLockMode mode1, DldLockMode mode2) {
	assert(mode1 == TL_X || mode1 == TL_S || mode1 == TL_IX
		  || mode1 == TL_IS || mode1 == TL_AUTO_INC);
	assert(mode2 == TL_X || mode2 == TL_S || mode2 == TL_IX
		  || mode2 == TL_IS || mode2 == TL_AUTO_INC);

	if (mode1 == TL_X) {
		return true;
	} else if (mode1 == TL_AUTO_INC && mode2 == TL_AUTO_INC) {
		return true;
	} else if (mode1 == TL_S
		   && (mode2 == TL_S || mode2 == TL_IS)) {
		return true;
	} else if (mode1 == TL_IS && mode2 == TL_IS) {
		return true;
	} else if (mode1 == TL_IX 
			&& (mode2 == TL_IX || mode2 == TL_IS)) {		
		return true;
	}
	return false;
}

/** 判断锁模式mode1与锁模式mode2是否相容
 * @param mode1 锁模式1
 * @param mode2 锁模式2
 * return 相容返回true，否则返回false
 */
bool DldLock::lockModeCompatible(DldLockMode mode1, DldLockMode mode2) {
	assert(mode1 == TL_X || mode1 == TL_S || mode1 == TL_IX
	      || mode1 == TL_IS || mode1 == TL_AUTO_INC);
	assert(mode2 == TL_X || mode2 == TL_S || mode2 == TL_IX
	      || mode2 == TL_IS || mode2 == TL_AUTO_INC);

	if (mode1 == TL_S && (mode2 == TL_IS || mode2 == TL_S)) {
		return true;
	} else if (mode1 == TL_X) {
		return false;
	} else if (mode1 == TL_AUTO_INC && (mode2 == TL_IS || mode2 == TL_IX)) {
		return true;
	} else if (mode1 == TL_IS && (mode2 == TL_IS || mode2 == TL_IX 
		|| mode2 == TL_AUTO_INC || mode2 == TL_S)) {
			return true;
	} else if (mode1 == TL_IX && (mode2 == TL_IS || mode2 == TL_AUTO_INC 
		|| mode2 == TL_IX)) {
			return true;
	}
	return false;

}

DldLockOwner::DldLockOwner(u64 txnId, MemoryContext *mtx) 
	: m_event(false), m_txnId(txnId), m_memctx(NULL), m_waitLock(NULL), 
	m_waitTimeout(false), m_chosenAsDeadlockVictim(false) {
		if (NULL == mtx) {
			m_memctx = new MemoryContext(DFL_MEMCTX_PAGE_SIZE, 1);
			assert(NULL != m_memctx);
			m_memctxAllocByMe = true;
		} else {
			m_memctx = mtx;
			m_memctxAllocByMe = false;
		}
		m_waiterLink.set(this);
		m_sp = INVALID_SAVE_POINT;
}

DldLockOwner::~DldLockOwner() {
	if (m_memctxAllocByMe) {
		m_memctx->reset();
		delete m_memctx;
		m_memctx = NULL;
	}
}

/**
 * 判断是否需要等待另一个锁请求
 * @param lockmode 需要加锁的类型
 * @param otherLock 是否需要等待的锁
 * @return 如果加lockMode类型的锁需要等待otherLock，返回true，否则返回false
 */
bool DldLockOwner::hasToWaitLock(DldLockMode lockMode, DldLock *otherLock) {
	assert(otherLock);
	if (this != otherLock->getLockOwner()
		&& !DldLock::lockModeCompatible(lockMode, otherLock->getLockMode())) {
			//TODO: 增加优先级判断
			//
			return true;
	}
	return false;
}

/**
 * 等待锁被授予
 * @param timeoutMs 超时时间，-1表示不超时
 * @return 发生死锁返回DLD_LOCK_DEAD_LOCK，超时返回DLD_LOCK_TIMEOUT，加锁成功返回DLD_LOCK_SUCCESS
 */
DldLockResult DldLockOwner::wait(int timeoutMs) {
	DldLockResult result = DLD_LOCK_SUCCESS;
	m_waitTimeout = false;
	int needWait = timeoutMs;
	u64 before = System::currentTimeMillis();
	u64 end = before + (timeoutMs> 0 ? timeoutMs : 0);

	while (true) {
		long sigToken = m_event.reset();
		// 防止被假唤醒，如果是加锁成功或者死锁检测失败，m_waitLock会设置为NULL
		if (NULL == m_waitLock) {
			result = m_chosenAsDeadlockVictim ? DLD_LOCK_DEAD_LOCK : DLD_LOCK_SUCCESS;
			break;
		}

		// 执行等待
		m_event.wait(MIN(needWait, 10000), sigToken);

		u64 now = System::currentTimeMillis();
		if (timeoutMs >= 0) {
			if (now >= end) {
				m_waitTimeout = true;
				result = DLD_LOCK_TIMEOUT;
				break;
			} else {
				needWait = (int)(end - now);
			}
		}
	}
	return result;
}

/** 如果key对应的lock是owner中最后一个lock，则将该lock从holdingList中删除，同时reset memctx
 * @param key 对应key值
 */
void DldLockOwner::releaseLockIfLast(u64 key) {
	DLink<DldLock *> *lock = m_lockHoldingList.removeLast();
	//key对应的lock必须为m_lockHoldingList的最后一个
	NTSE_ASSERT(lock->get()->getId() == key);
	
	if (m_sp == INVALID_SAVE_POINT) {
		return;
	}
	m_memctx->resetToSavepoint(m_sp);
	m_sp = INVALID_SAVE_POINT;
}

///////////////////////////////////////////////////////////////////////////////////////////////

/**
 * 判断正在等待的锁请求是否还不能马上授予
 * @param waitLock 正在等待的锁请求
 * @return 不能马上被授予的置true，否则为false
 */
bool DldLockEntry::stillHasToWaitInQueue(DldLock *waitLock) {
	assert(waitLock->getStatus() == DLS_WAIT);
	DldLockOwner *owner = waitLock->getLockOwner();
	DldLockMode lockMode = waitLock->getLockMode();

	Iterator it(this);
	while (it.hasNext()) {
		DldLock *nextLock = it.next();
		assert(nextLock->getStatus() == DLS_WAIT || nextLock->getStatus() == DLS_GRANTED);
		if (waitLock->getId() == nextLock->getId()) {
			if (nextLock == waitLock)
				break;			
			if (owner->hasToWaitLock(lockMode, nextLock))
				return true;
		}
	}
	return false;
}

/**
 * 判断请求加锁者是否已经持有了更高级别的锁
 * @param lockOwner 事务所有锁的拥有者
 * @param key 需要加锁的key
 * @param lockMode 需要加锁的类型
 * @return 返回已经被请求加锁者加上的高级别锁
 */
DldLock* DldLockEntry::hasLockImplicity(DldLockOwner *lockOwner, u64 key, DldLockMode lockMode) {
	Iterator it(this);
	while (it.hasNext()) {
		DldLock *lock = it.next();
		if (lock->getLockOwner() == lockOwner // 相同持有者
			&& lock->getId() == key // 相同键值
			&& DldLock::lockModeGreaterOrEq(lock->getLockMode(), lockMode)) { // 锁模式相等或更高
				return lock;
		}
	}
	return NULL;
}

/**
 * 判断请求对key加锁的模式是否与当前在key上锁模式发生冲突
 * @param lockOwner 事务锁的拥有者
 * @param key 需要加锁的key
 * @param lockMode 需要加锁的mode
 * @return 如果冲突，返回冲突的锁对象，否则返回null
 */
DldLock* DldLockEntry::otherHasConflicting(DldLockOwner *lockOwner, u64 key, DldLockMode lockMode) {
	Iterator it(this);
	while (it.hasNext()) {
		DldLock *lock = it.next();

		if (lock->getId() == key
			&& lockOwner->hasToWaitLock(lockMode, lock)) {
			return lock;
		}
	}
	return NULL;
}

/**
 * 查询某个键值是否存在已经使用的可锁对象中
 * @pre 操作之前需要持有该entry的互斥锁
 * @param key		查询的键值
 * @param equaler	比较函数对象
 * @return lock表示找到，NULL表示不存在
 */
DldLock* DldLockEntry::findLockObject(DldLockOwner *lockOwner, u64 key, 
									  Equaler<u64, u64> &equaler, DldLockMode lockMode) {
	Iterator it(this);
	while (it.hasNext()) {
		DldLock *lock = it.next();
		if (equaler(lock->getId(), key) // 同一个键值的锁
			&& lock->getLockOwner() == lockOwner // 同一Owner的锁
			&& lock->getLockMode() == lockMode) { // 相同锁模式
			return lock;
		}
	}
	return NULL;
}

/**
 * 增加一个可锁对象到本entry
 * @pre 操作之前需要持有该entry的互斥锁
 * @param lock	新的可锁对象
 */
void DldLockEntry::addToQueue(DldLock *lock) {
	m_hashList.addLast(&lock->m_entryLink);
}

/**
 * 当释放一个锁时，授予等待在同一对象上的可授予锁请求
 * @param releaseLock 被释放的锁
 */
void DldLockEntry::grantWaitingLocks(DldLock *releaseLock) {
	Iterator it(this);
	while (it.hasNext()) {
		DldLock *nextLock = it.next();
		if (releaseLock->getId() == nextLock->getId()) {
			if (nextLock->isWaiting()) {
				if (!stillHasToWaitInQueue(nextLock)) {
					nextLock->lockGrant();
				} else {
					break;
				}
			}
		}
	}
}

/** 获取对key加锁的锁对象
 * @param key 锁的key值
 * 获取相应的锁
 */
DldLock* DldLockEntry::pickLock(u64 key) {
	Iterator it(this);
	while (it.hasNext()) {
		DldLock *nextLock = it.next();
		if (nextLock->getId() == key
			&& nextLock->getLockMode() != TL_NO) {
				return nextLock;
		}
	}
	return NULL;
}

/** 比较事务锁first和second的优先级
 * @param first 事务1锁持有者
 * @param second 事务2锁持有者
 * return 如果选择first优先级高，返回，相等返回0，second优先级高则返回-1
 */
int dldLockOwnerCmpFunc(DldLockOwner *first, DldLockOwner *second) {
	uint firstHoldingListSize = first->getHoldingList()->getSize();
	uint secondHoldingListSize = second->getHoldingList()->getSize();
	if (firstHoldingListSize > secondHoldingListSize) {
		return 1;
	} else if (firstHoldingListSize == secondHoldingListSize) {
		return 0;
	} else {
		return -1;
	}
}

//////////////////////////////////////////////////////////////////////////////////////////////

/**
 * 创建一个锁管理器
 *  
 * @param maxLocks				最大支持的锁个数。注意由于性能考虑，锁管理器并不能保证锁对象不会超过maxLocks个。
 * @param entryCount			锁表slot数目, slotCount必须是2的整数次幂
 */
DldLockTable::DldLockTable(uint maxLocks, uint entryCount, int lockTimeoutMs) 
: m_lockTableMutex("DldLockTable Mutex", __FILE__, __LINE__), m_lockTimeoutMs(lockTimeoutMs) {
	assert(maxLocks && entryCount);
	init(maxLocks, entryCount);
}

/**
 * 创建一个锁管理器
 * 该锁表至少能放maxLocks把锁
 * @param maxLocks	最大支持的锁个数。注意由于性能考虑，锁管理器并不能保证锁对象不会超过maxLocks个。
 */
DldLockTable::DldLockTable(uint maxLocks, int lockTimeoutMs) 
: m_lockTableMutex("DldLockTable Mutex", __FILE__, __LINE__), m_lockTimeoutMs(lockTimeoutMs) {
	uint entryCount = flp2(maxLocks);
	init(maxLocks, entryCount);
}

/**
 * 锁管理器初始化
 * @param maxLocks	 最大支持的锁个数
 * @param entryCount 锁表入口数目，一定是2的整数次幂
 */
void DldLockTable::init(uint maxLocks, uint entryCount) {
	m_compareFunc = dldLockOwnerCmpFunc;
	m_entryCount = entryCount;
	m_entryMask = entryCount - 1;
	m_maxLocks = maxLocks;

	m_lockEntries = new DldLockEntry[m_entryCount];
	assert(m_lockEntries != NULL);

	// 初始化等待信息相关
	m_checkWaiterNum = 0;

	//memset(&m_status, 0, sizeof(struct LockTableStatus));
}

DldLockTable::~DldLockTable() {
	delete [] m_lockEntries;
}

/**
 * 判断指定键值的锁结点是否存在
 * @param key 键值
 * @param bePrecise 是否需要精确判断，如果不精确，则只要存在哈希入口都认为锁结点存在
 * @return 
 */
bool DldLockTable::pickLock(u64 key, bool bePrecise) {
	DldLockEntry *entry = getEntry(key);
	MutexGuard mutexGuard(&m_lockTableMutex, __FILE__, __LINE__);
	uint size = entry->getQueueSize();
	if (0 == size) {
		return false;
	} else if (bePrecise || size <= ENTRY_TRAVERSECNT_MAX) {
		return NULL != entry->pickLock(key);
	} else {
		return true;
	}
	//return bePrecise ? NULL != entry->pickLock(key) : true;
}

/**
 * 尝试加锁
 * @param lockOwner
 * @param key
 * @param lockMode
 * @param timeoutMs
 * @return 如果马上加锁成功返回DLD_LOCK_SUCCESS，失败返回DLD_LOCK_FAIL，内存不足返回DLD_LOCK_LACK_OF_MEM
 */
DldLockResult DldLockTable::tryLock(DldLockOwner *lockOwner, u64 key, DldLockMode lockMode) {
	DldLockResult ret = DLD_LOCK_SUCCESS;
	DldLockEntry *entry = getEntry(key);
	MutexGuard mutexGuard(&m_lockTableMutex, __FILE__, __LINE__);
	
	if (entry->hasLockImplicity(lockOwner, key, lockMode)) {
		// 自己已经持有更高级别的锁
		ret = DLD_LOCK_IMPLICITY;
	} else if (entry->otherHasConflicting(lockOwner, key, lockMode)) {// 遍历哈希表的拉链，检查锁模式是否冲突
		return DLD_LOCK_FAIL;
	} else {
		// 不冲突也不死锁，锁请求可以立刻授予
		++m_status.m_trylocks;
		if (NULL == createAndAddToQueue(lockOwner, key, lockMode, entry)) {
			ret = DLD_LOCK_LACK_OF_MEM;
		}
	}
	return ret;
}

/**
 * 加锁
 * @param lockOwner
 * @param key
 * @param lockMode
 * @param timeoutMs
 * @return 如果加锁成功返回DLD_LOCK_SUCCESS，检测到死锁返回DLD_LOCK_DEAD_LOCK，
 *  内存不足返回DLD_LOCK_LACK_OF_MEM
 */
DldLockResult DldLockTable::lock(DldLockOwner *lockOwner, u64 key, DldLockMode lockMode) {
	DldLockResult ret = DLD_LOCK_SUCCESS;
	DldLockEntry *entry = getEntry(key);
	LOCK(&m_lockTableMutex);
	
	if (entry->hasLockImplicity(lockOwner, key, lockMode)) {
		// 自己已经持有更高级别的锁
		ret = DLD_LOCK_IMPLICITY;
	} else if (entry->otherHasConflicting(lockOwner, key, lockMode)) {// 遍历哈希表的拉链，检查锁模式是否冲突
		// 检测到冲突，进入等待队列
		return enqueueWaiting(lockOwner, key, lockMode, entry);
	} else {
		// 不冲突也不死锁，锁请求可以立刻授予
		++m_status.m_locks;
		if (NULL == createAndAddToQueue(lockOwner, key, lockMode, entry)) {
			ret = DLD_LOCK_LACK_OF_MEM;
		}
	}	
	UNLOCK(&m_lockTableMutex);
	return ret;
}

/**
 * 解锁
 * @param lockOwner	加锁事务
 * @param lockMode  加锁模式
 * @param key		加锁对象键值
 * @return true表示解锁成功，false表示失败
 */
bool DldLockTable::unlock(DldLockOwner *lockOwner, u64 key, DldLockMode lockMode) {
	DldLockEntry *entry = getEntry(key);
	MutexGuard mutexGuard(&m_lockTableMutex, __FILE__, __LINE__);
	assert(NULL == lockOwner->getWaitLock());
	assert(NULL == lockOwner->m_waiterLink.getList());
	// 在哈希表的拉链中查找放锁者已经加上的锁对象
	DldLock *releaseLock = entry->findLockObject(lockOwner, key, m_ikeyEqualer, lockMode);

	// 如果成功找到，释放持有的锁
	if (releaseLock != NULL) {
		++m_status.m_unlocks;
		releaseLock->setStatus(DLS_FREE);
		// 从哈希表拉链中剔除
		releaseLock->removeFromEntryList();
	} else {
		fprintf(stderr, "unlock row could not find a %d mode lock on the record\n", (uint)lockMode);
		return false;
	}

	// 再次检查哈希表拉链，判断是否可以将锁授予正在等待的其他请求加锁者
	entry->grantWaitingLocks(releaseLock);
	//如果key对应的锁是最后一个锁，则进行释放，这样内存就得到更加有效的利用
	lockOwner->releaseLockIfLast(key);

	return true;
}

/**
 * 释放某个事务持有的所有的锁
 * @param lockOwner 加锁事务
 * @return true表示解锁成功，false表示失败
 */
bool DldLockTable::releaseAllLocks(DldLockOwner *lockOwner) {
	MutexGuard mutexGuard(&m_lockTableMutex, __FILE__, __LINE__);

	DList<DldLock*> *holdingList = lockOwner->getHoldingList();
	DLink<DldLock*> *header = holdingList->getHeader();
	DLink<DldLock*> *it = header->getPrev();
	uint count = 0;
	while (it != header) {
		DldLock *lock = it->get();
		assert(lock->getStatus() == DLS_GRANTED);
		it->unLink();// 从持有者的持有锁链表中删除
		removeFromQueue(lock);		
		count++;
		it = header->getPrev();
	}
	lockOwner->releaseResourceIfNeeded();

	++m_status.m_unlockAlls;
	return true;
}

/**
 * 判断请求加锁者是否已经持有了更高级别的锁
 * @param lockOwner	加锁者
 * @Param key		加锁对象KEY
 * @param lockMode  加锁模式
 * @return true表示加过锁，否则false
 */
bool DldLockTable::hasLockImplicity(DldLockOwner *lockOwner, u64 key, DldLockMode lockMode) {
	DldLockEntry *entry = getEntry(key);
	MutexGuard mutexGuard(&m_lockTableMutex, __FILE__, __LINE__);
	return NULL != entry->hasLockImplicity(lockOwner, key, lockMode);
}

/**
 * 判断请求加锁者是否已经持有指定模式的锁
 * @param lockOwner	加锁者
 * @Param key		加锁对象KEY
 * @param lockMode  加锁模式
 * @return true表示加过锁，否则false
 */
bool DldLockTable::isLocked(DldLockOwner *lockOwner, u64 key, DldLockMode lockMode) {
	DldLockEntry *entry = getEntry(key);
	MutexGuard mutexGuard(&m_lockTableMutex, __FILE__, __LINE__);
	return NULL != entry->findLockObject(lockOwner, key, m_ikeyEqualer, lockMode);
}

/**
 * 加锁成功时创建新的锁对象，并插入到对应的哈希表拉链中
 * @pre 已经通过了锁冲突检测
 * @param lockOwner
 * @param key
 * @param lockMode
 * @param entry
 * @return 锁表内存不足返回NULL，否则返回创建的锁
 */
DldLock* DldLockTable::createAndAddToQueue(DldLockOwner *lockOwner, u64 key, 
										   DldLockMode lockMode, DldLockEntry *entry) {
    assert(m_lockTableMutex.isLocked());
	DldLock *lock = lockOwner->createLock(key, lockMode);
	if (NULL != lock) {
		lock->setStatus(DLS_GRANTED);
		entry->addToQueue(lock);
		++m_status.m_activeLocks;
		return lock;
	} else {
		return NULL;
	}
}

/**
 * 加锁冲突的时候需要睡眠等待
 * @param lock		要等待加锁的锁对象
 * @param lockList	当前要加锁事务的DLDLockList
 * @param entry		等锁对象所在的Hash入口entry
 * @return 加锁成功返回DLD_LOCK_SUCCESS，检测到死锁返回DLD_LOCK_DEAD_LOCK，锁表内存不足返回DLD_LOCK_LACK_OF_MEM
 */
DldLockResult DldLockTable::enqueueWaiting(DldLockOwner *lockOwner, u64 key, 
										   DldLockMode lockMode, DldLockEntry *entry) {
	assert(entry);
	assert(m_lockTableMutex.isLocked());

	DldLock *lock = lockOwner->createLock(key, lockMode);
	if (NULL == lock) {
		UNLOCK(&m_lockTableMutex);
		return DLD_LOCK_LACK_OF_MEM;
	}

	lock->setStatus(DLS_WAIT);
	entry->addToQueue(lock);
	++m_status.m_activeLocks;

	lockOwner->linkWithWaiterList(&m_lockWaiterList);
	lockOwner->setWaitLock(lock);

	if (checkDeadlockOccurs(lockOwner)) {
		// 检测到发生死锁，并且自己被选择为牺牲者
		lock->cancelWaitingAndRelease(false);
		lockOwner->releaseLockIfLast(key);
		--m_status.m_activeLocks;
		UNLOCK(&m_lockTableMutex);
		return DLD_LOCK_DEAD_LOCK;
	}

	if (lockOwner->getWaitLock() == NULL) {
		// 如果发生死锁，但是在死锁检测中选择了其他的锁作为牺牲者，
		// 那么有可能当前的锁请求已经被授予，这时候直接返回加锁成功即可
		assert(!lock->isWaiting());
		UNLOCK(&m_lockTableMutex);
		return DLD_LOCK_SUCCESS;
	}

	// 当前锁请求还不能授予，进入等待状态
	lockOwner->markDeadlockVictim(false);

	++m_status.m_waits;

	UNLOCK(&m_lockTableMutex);
	SYNCHERE(SP_DLD_LOCK_ENQUEUE_BEFORE_WAIT);
	
	DldLockResult result = lockOwner->wait(m_lockTimeoutMs);
	if (DLD_LOCK_SUCCESS == result) {
		return result;
	}

	assert(DLD_LOCK_DEAD_LOCK == result || DLD_LOCK_TIMEOUT == result);
	LOCK(&m_lockTableMutex);
	//由于没有m_lockTableMutex的保护，所以在timeout时，lock又被授予了
	if (lock->m_status == DLS_GRANTED) {
		assert(DLD_LOCK_TIMEOUT == result);
		result = DLD_LOCK_SUCCESS; 
	} else {
		//被选为牺牲者，已经在checkDeadlockOccurs调用了cancelWaitingAndRelease
		if (lock->isWaiting()) {
			assert(DLD_LOCK_TIMEOUT == result);
			lock->cancelWaitingAndRelease(false);
		}
		//entry->removeFromQueue(lock);
		//lockOwner->setWaitLock(NULL);
		//lockOwner->unlinkFromWaiterList();
		lockOwner->releaseLockIfLast(key);
		--m_status.m_activeLocks;
	}
	UNLOCK(&m_lockTableMutex);

	return result;
}

/**
 * 将锁从哈希表拉链中移除
 * @param lock 要移除的锁
 */
void DldLockTable::removeFromQueue(DldLock *lock) {
	assert(m_lockTableMutex.isLocked());

	DldLockEntry *entry = getEntry(lock->getId());
	// 从哈希表链表中删除结点
	lock->removeFromEntryList();
	--m_status.m_activeLocks;

	// 再次检查哈希表拉链，判断是否可以将锁授予正在等待的其他请求加锁者
	entry->grantWaitingLocks(lock);
}

/**
 * 判断是否会死锁
 * @param lockOwner 新的锁请求
 * @return 如果lockOwner新的锁请求会引起死锁返回true，否则返回false
 */
bool DldLockTable::checkDeadlockOccurs(DldLockOwner *lockOwner) {
	assert(m_lockTableMutex.isLocked());
__retry:
	uint cost = 0;
	DLink<DldLockOwner*> *header = m_lockWaiterList.getHeader();
	DLink<DldLockOwner*> *next = header->getNext();
	while (next != header) {
		DldLockOwner *locker = next->get();
		assert(locker->getWaitLock() != NULL);
		locker->m_deadLockCheckedMark = 0;
		next = next->getNext();
	}

	DeadLockCheckResult ret = checkDeadlockRecursive(lockOwner, lockOwner, &cost, 0);
	switch (ret) {
		case LOCK_EXCEED_MAX_DEPTH:
			break;
		case LOCK_VICTIM_IS_START:
			++m_status.m_deadlocks;
			break;
		case LOCK_VICTIM_IS_OTHER:
			++m_status.m_deadlocks;
			goto __retry;
		default:
			return false;
	}

	return true;
}

/**
 * 判断从waiter开始是否在等待starter
 * @param starter 检查的终点
 * @param waiter 检查的起点
 * @param cost INOUT 执行次数
 * @param depth 递归深度
 * @return 如果存在从waiter到starter的等待链，且starter被选择为牺牲者，返回LOCK_VICTIM_IS_START
 *         如果是非starter被选择为牺牲者，返回LOCK_VICTIM_IS_OTHER
 *         如果不存在从waiter到starter的等待链，返回LOCK_DEADLOCK_FREE
 */
DeadLockCheckResult DldLockTable::checkDeadlockRecursive(DldLockOwner *starter, DldLockOwner *waiter, 
														 uint *cost, uint depth) {
	assert(m_lockTableMutex.isLocked());
	assert(waiter->getWaitLock() != NULL);

	if (waiter->m_deadLockCheckedMark) {//结点已经检查过
		return LOCK_DEADLOCK_FREE;
	}

	*cost = *cost + 1;
	DldLock *waitLock = waiter->getWaitLock();
	DldLock *lock = waitLock;
	DldLockEntry *entry = getEntry(waitLock->getId());

	while (true) {
		DLink<DldLock*> *prev = lock->m_entryLink.getPrev();
		assert(NULL != prev);
		//遍历hash entry中在该lock以前的所有lock，这样才能保证不发生死锁
		if (prev == entry->getHeader()) {
			waiter->m_deadLockCheckedMark = 1;
			return LOCK_DEADLOCK_FREE;
		}

		lock = prev->get();
		//该hash entry中锁对象不一致，则直接跳过
		if (waitLock->getId() != lock->getId())
			continue;

		if (waiter->hasToWaitLock(waitLock->getLockMode(), lock)) {//waitLock 和 lock不兼容
			bool tooFar = false;// FIXME
			DldLockOwner *locker = lock->getLockOwner();
			if (locker == starter) {// lock被starter持有，说明waiter正在等待starter，发生死锁
				fprintf(stderr, "Deadlock is detected in lock waiting graph.\n");

				if (compareWeight(starter, waiter) < 0) {
					starter->markDeadlockVictim(true);
					return LOCK_VICTIM_IS_START;
				} else {
					waiter->markDeadlockVictim(true);
					waitLock->cancelWaitingAndRelease(true);
					return LOCK_VICTIM_IS_OTHER;
				}
			}
			if (tooFar) {
				fprintf(stderr, "Deadlock search exceeds max steps or depth.\n");
				return LOCK_EXCEED_MAX_DEPTH;
			}
			if (locker->getWaitLock() != NULL) {
				DeadLockCheckResult ret = checkDeadlockRecursive(starter, locker, cost, depth + 1);
				if (ret != LOCK_DEADLOCK_FREE) {
					return ret;
				}
			}
		}
	}
	return LOCK_DEADLOCK_FREE;
}

}