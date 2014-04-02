/**
 * ����������ͨ������
 *
 * @author ��ΰ��(liweizhao@corp.netease.com)
 */
#include "misc/DLDLockTable.h"
#include "misc/MemCtx.h"

namespace tnt {

/**
 * ������
 */
void DldLock::lockGrant() {
	m_status = DLS_GRANTED;
	assert(NULL != m_lockOwner);
	m_lockOwner->unlinkFromWaiterList();
	m_lockOwner->setWaitLock(NULL);
	m_lockOwner->wakeUp();
}

/**
 * ȡ�����ڵȴ���������
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
 * ��lock�Ӷ�Ӧ��entryList��ɾ��
 * @pre ����֮ǰ��Ҫ���и�entry�Ļ�����
 */
void DldLock::removeFromEntryList() {
	m_entryLink.unLink();
}

/**
 * �ж���ģʽmode1�Ƿ���ڵ�����ģʽmode2
 * @param mode1 ��ģʽ
 * @param mode2 ��ģʽ
 * @return mode1���ڵ���mode2����true�����򷵻�false
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

/** �ж���ģʽmode1����ģʽmode2�Ƿ�����
 * @param mode1 ��ģʽ1
 * @param mode2 ��ģʽ2
 * return ���ݷ���true�����򷵻�false
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
 * �ж��Ƿ���Ҫ�ȴ���һ��������
 * @param lockmode ��Ҫ����������
 * @param otherLock �Ƿ���Ҫ�ȴ�����
 * @return �����lockMode���͵�����Ҫ�ȴ�otherLock������true�����򷵻�false
 */
bool DldLockOwner::hasToWaitLock(DldLockMode lockMode, DldLock *otherLock) {
	assert(otherLock);
	if (this != otherLock->getLockOwner()
		&& !DldLock::lockModeCompatible(lockMode, otherLock->getLockMode())) {
			//TODO: �������ȼ��ж�
			//
			return true;
	}
	return false;
}

/**
 * �ȴ���������
 * @param timeoutMs ��ʱʱ�䣬-1��ʾ����ʱ
 * @return ������������DLD_LOCK_DEAD_LOCK����ʱ����DLD_LOCK_TIMEOUT�������ɹ�����DLD_LOCK_SUCCESS
 */
DldLockResult DldLockOwner::wait(int timeoutMs) {
	DldLockResult result = DLD_LOCK_SUCCESS;
	m_waitTimeout = false;
	int needWait = timeoutMs;
	u64 before = System::currentTimeMillis();
	u64 end = before + (timeoutMs> 0 ? timeoutMs : 0);

	while (true) {
		long sigToken = m_event.reset();
		// ��ֹ���ٻ��ѣ�����Ǽ����ɹ������������ʧ�ܣ�m_waitLock������ΪNULL
		if (NULL == m_waitLock) {
			result = m_chosenAsDeadlockVictim ? DLD_LOCK_DEAD_LOCK : DLD_LOCK_SUCCESS;
			break;
		}

		// ִ�еȴ�
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

/** ���key��Ӧ��lock��owner�����һ��lock���򽫸�lock��holdingList��ɾ����ͬʱreset memctx
 * @param key ��Ӧkeyֵ
 */
void DldLockOwner::releaseLockIfLast(u64 key) {
	DLink<DldLock *> *lock = m_lockHoldingList.removeLast();
	//key��Ӧ��lock����Ϊm_lockHoldingList�����һ��
	NTSE_ASSERT(lock->get()->getId() == key);
	
	if (m_sp == INVALID_SAVE_POINT) {
		return;
	}
	m_memctx->resetToSavepoint(m_sp);
	m_sp = INVALID_SAVE_POINT;
}

///////////////////////////////////////////////////////////////////////////////////////////////

/**
 * �ж����ڵȴ����������Ƿ񻹲�����������
 * @param waitLock ���ڵȴ���������
 * @return �������ϱ��������true������Ϊfalse
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
 * �ж�����������Ƿ��Ѿ������˸��߼������
 * @param lockOwner ������������ӵ����
 * @param key ��Ҫ������key
 * @param lockMode ��Ҫ����������
 * @return �����Ѿ�����������߼��ϵĸ߼�����
 */
DldLock* DldLockEntry::hasLockImplicity(DldLockOwner *lockOwner, u64 key, DldLockMode lockMode) {
	Iterator it(this);
	while (it.hasNext()) {
		DldLock *lock = it.next();
		if (lock->getLockOwner() == lockOwner // ��ͬ������
			&& lock->getId() == key // ��ͬ��ֵ
			&& DldLock::lockModeGreaterOrEq(lock->getLockMode(), lockMode)) { // ��ģʽ��Ȼ����
				return lock;
		}
	}
	return NULL;
}

/**
 * �ж������key������ģʽ�Ƿ��뵱ǰ��key����ģʽ������ͻ
 * @param lockOwner ��������ӵ����
 * @param key ��Ҫ������key
 * @param lockMode ��Ҫ������mode
 * @return �����ͻ�����س�ͻ�������󣬷��򷵻�null
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
 * ��ѯĳ����ֵ�Ƿ�����Ѿ�ʹ�õĿ���������
 * @pre ����֮ǰ��Ҫ���и�entry�Ļ�����
 * @param key		��ѯ�ļ�ֵ
 * @param equaler	�ȽϺ�������
 * @return lock��ʾ�ҵ���NULL��ʾ������
 */
DldLock* DldLockEntry::findLockObject(DldLockOwner *lockOwner, u64 key, 
									  Equaler<u64, u64> &equaler, DldLockMode lockMode) {
	Iterator it(this);
	while (it.hasNext()) {
		DldLock *lock = it.next();
		if (equaler(lock->getId(), key) // ͬһ����ֵ����
			&& lock->getLockOwner() == lockOwner // ͬһOwner����
			&& lock->getLockMode() == lockMode) { // ��ͬ��ģʽ
			return lock;
		}
	}
	return NULL;
}

/**
 * ����һ���������󵽱�entry
 * @pre ����֮ǰ��Ҫ���и�entry�Ļ�����
 * @param lock	�µĿ�������
 */
void DldLockEntry::addToQueue(DldLock *lock) {
	m_hashList.addLast(&lock->m_entryLink);
}

/**
 * ���ͷ�һ����ʱ������ȴ���ͬһ�����ϵĿ�����������
 * @param releaseLock ���ͷŵ���
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

/** ��ȡ��key������������
 * @param key ����keyֵ
 * ��ȡ��Ӧ����
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

/** �Ƚ�������first��second�����ȼ�
 * @param first ����1��������
 * @param second ����2��������
 * return ���ѡ��first���ȼ��ߣ����أ���ȷ���0��second���ȼ����򷵻�-1
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
 * ����һ����������
 *  
 * @param maxLocks				���֧�ֵ���������ע���������ܿ��ǣ��������������ܱ�֤�����󲻻ᳬ��maxLocks����
 * @param entryCount			����slot��Ŀ, slotCount������2����������
 */
DldLockTable::DldLockTable(uint maxLocks, uint entryCount, int lockTimeoutMs) 
: m_lockTableMutex("DldLockTable Mutex", __FILE__, __LINE__), m_lockTimeoutMs(lockTimeoutMs) {
	assert(maxLocks && entryCount);
	init(maxLocks, entryCount);
}

/**
 * ����һ����������
 * �����������ܷ�maxLocks����
 * @param maxLocks	���֧�ֵ���������ע���������ܿ��ǣ��������������ܱ�֤�����󲻻ᳬ��maxLocks����
 */
DldLockTable::DldLockTable(uint maxLocks, int lockTimeoutMs) 
: m_lockTableMutex("DldLockTable Mutex", __FILE__, __LINE__), m_lockTimeoutMs(lockTimeoutMs) {
	uint entryCount = flp2(maxLocks);
	init(maxLocks, entryCount);
}

/**
 * ����������ʼ��
 * @param maxLocks	 ���֧�ֵ�������
 * @param entryCount ���������Ŀ��һ����2����������
 */
void DldLockTable::init(uint maxLocks, uint entryCount) {
	m_compareFunc = dldLockOwnerCmpFunc;
	m_entryCount = entryCount;
	m_entryMask = entryCount - 1;
	m_maxLocks = maxLocks;

	m_lockEntries = new DldLockEntry[m_entryCount];
	assert(m_lockEntries != NULL);

	// ��ʼ���ȴ���Ϣ���
	m_checkWaiterNum = 0;

	//memset(&m_status, 0, sizeof(struct LockTableStatus));
}

DldLockTable::~DldLockTable() {
	delete [] m_lockEntries;
}

/**
 * �ж�ָ����ֵ��������Ƿ����
 * @param key ��ֵ
 * @param bePrecise �Ƿ���Ҫ��ȷ�жϣ��������ȷ����ֻҪ���ڹ�ϣ��ڶ���Ϊ��������
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
 * ���Լ���
 * @param lockOwner
 * @param key
 * @param lockMode
 * @param timeoutMs
 * @return ������ϼ����ɹ�����DLD_LOCK_SUCCESS��ʧ�ܷ���DLD_LOCK_FAIL���ڴ治�㷵��DLD_LOCK_LACK_OF_MEM
 */
DldLockResult DldLockTable::tryLock(DldLockOwner *lockOwner, u64 key, DldLockMode lockMode) {
	DldLockResult ret = DLD_LOCK_SUCCESS;
	DldLockEntry *entry = getEntry(key);
	MutexGuard mutexGuard(&m_lockTableMutex, __FILE__, __LINE__);
	
	if (entry->hasLockImplicity(lockOwner, key, lockMode)) {
		// �Լ��Ѿ����и��߼������
		ret = DLD_LOCK_IMPLICITY;
	} else if (entry->otherHasConflicting(lockOwner, key, lockMode)) {// ������ϣ��������������ģʽ�Ƿ��ͻ
		return DLD_LOCK_FAIL;
	} else {
		// ����ͻҲ�������������������������
		++m_status.m_trylocks;
		if (NULL == createAndAddToQueue(lockOwner, key, lockMode, entry)) {
			ret = DLD_LOCK_LACK_OF_MEM;
		}
	}
	return ret;
}

/**
 * ����
 * @param lockOwner
 * @param key
 * @param lockMode
 * @param timeoutMs
 * @return ��������ɹ�����DLD_LOCK_SUCCESS����⵽��������DLD_LOCK_DEAD_LOCK��
 *  �ڴ治�㷵��DLD_LOCK_LACK_OF_MEM
 */
DldLockResult DldLockTable::lock(DldLockOwner *lockOwner, u64 key, DldLockMode lockMode) {
	DldLockResult ret = DLD_LOCK_SUCCESS;
	DldLockEntry *entry = getEntry(key);
	LOCK(&m_lockTableMutex);
	
	if (entry->hasLockImplicity(lockOwner, key, lockMode)) {
		// �Լ��Ѿ����и��߼������
		ret = DLD_LOCK_IMPLICITY;
	} else if (entry->otherHasConflicting(lockOwner, key, lockMode)) {// ������ϣ��������������ģʽ�Ƿ��ͻ
		// ��⵽��ͻ������ȴ�����
		return enqueueWaiting(lockOwner, key, lockMode, entry);
	} else {
		// ����ͻҲ�������������������������
		++m_status.m_locks;
		if (NULL == createAndAddToQueue(lockOwner, key, lockMode, entry)) {
			ret = DLD_LOCK_LACK_OF_MEM;
		}
	}	
	UNLOCK(&m_lockTableMutex);
	return ret;
}

/**
 * ����
 * @param lockOwner	��������
 * @param lockMode  ����ģʽ
 * @param key		���������ֵ
 * @return true��ʾ�����ɹ���false��ʾʧ��
 */
bool DldLockTable::unlock(DldLockOwner *lockOwner, u64 key, DldLockMode lockMode) {
	DldLockEntry *entry = getEntry(key);
	MutexGuard mutexGuard(&m_lockTableMutex, __FILE__, __LINE__);
	assert(NULL == lockOwner->getWaitLock());
	assert(NULL == lockOwner->m_waiterLink.getList());
	// �ڹ�ϣ��������в��ҷ������Ѿ����ϵ�������
	DldLock *releaseLock = entry->findLockObject(lockOwner, key, m_ikeyEqualer, lockMode);

	// ����ɹ��ҵ����ͷų��е���
	if (releaseLock != NULL) {
		++m_status.m_unlocks;
		releaseLock->setStatus(DLS_FREE);
		// �ӹ�ϣ���������޳�
		releaseLock->removeFromEntryList();
	} else {
		fprintf(stderr, "unlock row could not find a %d mode lock on the record\n", (uint)lockMode);
		return false;
	}

	// �ٴμ���ϣ���������ж��Ƿ���Խ����������ڵȴ����������������
	entry->grantWaitingLocks(releaseLock);
	//���key��Ӧ���������һ������������ͷţ������ڴ�͵õ�������Ч������
	lockOwner->releaseLockIfLast(key);

	return true;
}

/**
 * �ͷ�ĳ��������е����е���
 * @param lockOwner ��������
 * @return true��ʾ�����ɹ���false��ʾʧ��
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
		it->unLink();// �ӳ����ߵĳ�����������ɾ��
		removeFromQueue(lock);		
		count++;
		it = header->getPrev();
	}
	lockOwner->releaseResourceIfNeeded();

	++m_status.m_unlockAlls;
	return true;
}

/**
 * �ж�����������Ƿ��Ѿ������˸��߼������
 * @param lockOwner	������
 * @Param key		��������KEY
 * @param lockMode  ����ģʽ
 * @return true��ʾ�ӹ���������false
 */
bool DldLockTable::hasLockImplicity(DldLockOwner *lockOwner, u64 key, DldLockMode lockMode) {
	DldLockEntry *entry = getEntry(key);
	MutexGuard mutexGuard(&m_lockTableMutex, __FILE__, __LINE__);
	return NULL != entry->hasLockImplicity(lockOwner, key, lockMode);
}

/**
 * �ж�����������Ƿ��Ѿ�����ָ��ģʽ����
 * @param lockOwner	������
 * @Param key		��������KEY
 * @param lockMode  ����ģʽ
 * @return true��ʾ�ӹ���������false
 */
bool DldLockTable::isLocked(DldLockOwner *lockOwner, u64 key, DldLockMode lockMode) {
	DldLockEntry *entry = getEntry(key);
	MutexGuard mutexGuard(&m_lockTableMutex, __FILE__, __LINE__);
	return NULL != entry->findLockObject(lockOwner, key, m_ikeyEqualer, lockMode);
}

/**
 * �����ɹ�ʱ�����µ������󣬲����뵽��Ӧ�Ĺ�ϣ��������
 * @pre �Ѿ�ͨ��������ͻ���
 * @param lockOwner
 * @param key
 * @param lockMode
 * @param entry
 * @return �����ڴ治�㷵��NULL�����򷵻ش�������
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
 * ������ͻ��ʱ����Ҫ˯�ߵȴ�
 * @param lock		Ҫ�ȴ�������������
 * @param lockList	��ǰҪ���������DLDLockList
 * @param entry		�����������ڵ�Hash���entry
 * @return �����ɹ�����DLD_LOCK_SUCCESS����⵽��������DLD_LOCK_DEAD_LOCK�������ڴ治�㷵��DLD_LOCK_LACK_OF_MEM
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
		// ��⵽���������������Լ���ѡ��Ϊ������
		lock->cancelWaitingAndRelease(false);
		lockOwner->releaseLockIfLast(key);
		--m_status.m_activeLocks;
		UNLOCK(&m_lockTableMutex);
		return DLD_LOCK_DEAD_LOCK;
	}

	if (lockOwner->getWaitLock() == NULL) {
		// ����������������������������ѡ��������������Ϊ�����ߣ�
		// ��ô�п��ܵ�ǰ���������Ѿ������裬��ʱ��ֱ�ӷ��ؼ����ɹ�����
		assert(!lock->isWaiting());
		UNLOCK(&m_lockTableMutex);
		return DLD_LOCK_SUCCESS;
	}

	// ��ǰ�����󻹲������裬����ȴ�״̬
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
	//����û��m_lockTableMutex�ı�����������timeoutʱ��lock�ֱ�������
	if (lock->m_status == DLS_GRANTED) {
		assert(DLD_LOCK_TIMEOUT == result);
		result = DLD_LOCK_SUCCESS; 
	} else {
		//��ѡΪ�����ߣ��Ѿ���checkDeadlockOccurs������cancelWaitingAndRelease
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
 * �����ӹ�ϣ���������Ƴ�
 * @param lock Ҫ�Ƴ�����
 */
void DldLockTable::removeFromQueue(DldLock *lock) {
	assert(m_lockTableMutex.isLocked());

	DldLockEntry *entry = getEntry(lock->getId());
	// �ӹ�ϣ��������ɾ�����
	lock->removeFromEntryList();
	--m_status.m_activeLocks;

	// �ٴμ���ϣ���������ж��Ƿ���Խ����������ڵȴ����������������
	entry->grantWaitingLocks(lock);
}

/**
 * �ж��Ƿ������
 * @param lockOwner �µ�������
 * @return ���lockOwner�µ��������������������true�����򷵻�false
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
 * �жϴ�waiter��ʼ�Ƿ��ڵȴ�starter
 * @param starter �����յ�
 * @param waiter �������
 * @param cost INOUT ִ�д���
 * @param depth �ݹ����
 * @return ������ڴ�waiter��starter�ĵȴ�������starter��ѡ��Ϊ�����ߣ�����LOCK_VICTIM_IS_START
 *         ����Ƿ�starter��ѡ��Ϊ�����ߣ�����LOCK_VICTIM_IS_OTHER
 *         ��������ڴ�waiter��starter�ĵȴ���������LOCK_DEADLOCK_FREE
 */
DeadLockCheckResult DldLockTable::checkDeadlockRecursive(DldLockOwner *starter, DldLockOwner *waiter, 
														 uint *cost, uint depth) {
	assert(m_lockTableMutex.isLocked());
	assert(waiter->getWaitLock() != NULL);

	if (waiter->m_deadLockCheckedMark) {//����Ѿ�����
		return LOCK_DEADLOCK_FREE;
	}

	*cost = *cost + 1;
	DldLock *waitLock = waiter->getWaitLock();
	DldLock *lock = waitLock;
	DldLockEntry *entry = getEntry(waitLock->getId());

	while (true) {
		DLink<DldLock*> *prev = lock->m_entryLink.getPrev();
		assert(NULL != prev);
		//����hash entry���ڸ�lock��ǰ������lock���������ܱ�֤����������
		if (prev == entry->getHeader()) {
			waiter->m_deadLockCheckedMark = 1;
			return LOCK_DEADLOCK_FREE;
		}

		lock = prev->get();
		//��hash entry��������һ�£���ֱ������
		if (waitLock->getId() != lock->getId())
			continue;

		if (waiter->hasToWaitLock(waitLock->getLockMode(), lock)) {//waitLock �� lock������
			bool tooFar = false;// FIXME
			DldLockOwner *locker = lock->getLockOwner();
			if (locker == starter) {// lock��starter���У�˵��waiter���ڵȴ�starter����������
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