/**
 * 索引页面锁专用的锁管理器实现
 *
 * @author 苏斌(naturally@163.org)
 */

#ifndef _NTSE_INDICE_LOCK_MANAGER_H_
#define _NTSE_INDICE_LOCK_MANAGER_H_

#include <algorithm>
#include "Global.h"
#include "util/Hash.h"
#include "util/Sync.h"
#include "util/System.h"
#include "util/Thread.h"

namespace ntse {

const u64 INVALID_TXN_ID = (u64)-1;


/** 锁表各项统计信息 */
struct IndiceLockTableStatus {
	u64	m_locks;			/** 加锁请求次数 */
	u64	m_trylocks;			/** trylock次数 */
	u64	m_deadlocks;		/** 死锁次数 */
	u64	m_unlocks;			/** 解锁请求次数 */
	u64	m_unlockAlls;		/** 一次性放锁请求次数 */
	u64 m_waits;			/** 等待次数 */
	u64	m_sWaitTime;		/** 等待成功的等待时间（单位毫秒）*/
	u64	m_fWaitTime;		/** 等待失败的等待时间（单位毫秒）*/
	u64 m_avgConflictLen;	/** 冲突链表的平均长度 */
	u64 m_maxConflictLen;	/** 冲突链表的最大长度 */
	u64 m_inuseEntries;		/** 当前真正使用到的Entry数 */
	u32	m_activeTxn;		/** 活跃事务数 */
	u32	m_activeLocks;		/** 对象锁个数 */
};


/** 锁表
 * 该锁表主要包括：
 * 1. 只允许加互斥的X锁
 * 2. 允许锁重入
 * 3. 能预防检测死锁
 * 4. 能一次性释放某个事务持有的所有锁
 * 在NTSE里面，Session相当于一个事务
 *
 * @param K 锁对象键类型
 * @param KH 给锁对象键计算哈希值的函数对象，默认为Hasher<K>
 * @param KE 比较两个键是否相等的函数对象，默认为Equaler<K, K>
 */
template < typename K, typename KH = Hasher<K>, typename KE = Equaler<K, K> > 
class IndicesLockTable {
private:
	/**
 	 * 死锁定期检测类，如果检测到死锁，会标志需要杀死的事务，并且唤醒该事务
	 */
	class DeadLockChecker : public Task {
	public:
		DeadLockChecker(IndicesLockTable<K, KH, KE> *lockTable) : Task("DeadLockChecker", (uint)DEADLOCK_CHECK_INTERNAL) {
			m_lockTable = lockTable;
		}

		void run() {
			m_lockTable->deadlockDetecting();
		}

	private:
		IndicesLockTable<K, KH, KE> *m_lockTable;					/** 需要死锁检测的锁表对象 */

		static const uint DEADLOCK_CHECK_INTERNAL = 1000;	/** 死锁线程检测周期，单位毫秒 */
	};

	class LockList;
	/** 可锁对象描述 */
	struct Lock {
		K					m_objectId;		/** 可锁对象 */
		u64					m_txnId;		/** 持有锁的事务号 */
		DLink<Lock*>		m_holderLLLink;	/** 当前持有者LockList所拥有的Lock链表 */
		DList<LockList*>	m_waitingLLLink;/** 当前等待该可锁对象的事务的LockList链表 */
		Lock				*m_next;		/** 属于同一Entry的可锁对象链表或者空闲可锁对象链表 */
		u32					m_locks;		/** 当前持有者持有锁的次数 */

		Lock() {
			m_holderLLLink.set(this);
			init();
		}

		/**
		 * 初始化锁对象
		 */
		inline void init() {
			m_txnId = INVALID_TXN_ID;
			m_locks = 0;
			//m_objectId = 0;
			m_next = NULL;
			m_holderLLLink.unLink();
		}
		
		/**
		 * @pre 持有锁所在入口的互斥锁
		 * 返回当前锁有没有等待者
		 */
		inline bool hasWaiter() {
			return !m_waitingLLLink.isEmpty();
		}

		/**
		 * 将锁对象和拥有者lockList关联起来
		 * @pre 持有锁所在入口的互斥锁
		 * @param lockList	拥有者lockList对象
		 */
		inline void relateLockList(LockList *lockList) {
			m_txnId = lockList->m_txnId;
			lockList->m_holdingLocks.addLast(&m_holderLLLink);		// 按照顺序，始终加在尾部
		}

		/**
		 * 更改锁的拥有者
		 * @pre 持有锁所在入口的互斥锁
		 * @param lockList	新的拥有者lockList对象
		 */
		inline void changeOwner(LockList *lockList) {
			m_locks = 1;
			m_holderLLLink.unLink();
			relateLockList(lockList);
		}

		/**
		 * 将某个等该锁对象的LockList加入等待队列当中，根据LockList的权重，决定插入到等待队列的哪个位置
		 * @pre 要保证对锁入口对象加锁，对等待队列保护锁加锁
		 * @param lockList	等待该锁的LockList对象
		 */
		inline void addWaiter(LockList *lockList) {
			uint weight = lockList->m_holdingLocks.getSize();
			if (weight < LOCKLIST_LIGHTEST_WEIGHT || m_waitingLLLink.isEmpty())
				m_waitingLLLink.addLast(&lockList->m_waitingList);
			else {	// 权重比较大的LockList应该考虑排到等待队列前面提早得到锁
				DLink<LockList*> *waitingHeader = m_waitingLLLink.getHeader();
				DLink<LockList*> *curLL = waitingHeader->getNext();
				uint vipLLNum = (MAX_VIP_LOCKLIST_NUM < m_waitingLLLink.getSize() ? MAX_VIP_LOCKLIST_NUM : m_waitingLLLink.getSize());
				uint i = 0;
				for (; i < vipLLNum; i++, curLL = curLL->getNext()) {
					LockList *waitingLockList = curLL->get();
					if (waitingLockList->m_holdingLocks.getSize() < weight) {
						curLL->addBefore(&lockList->m_waitingList);
						break;
					}
				}

				if (i == vipLLNum)	// 如果权重不够，为了保证加锁时间不会太长有人饿死，排到最后
					m_waitingLLLink.addLast(&lockList->m_waitingList);
			}
		}

		static const uint LOCKLIST_LIGHTEST_WEIGHT = 2;
		static const uint MAX_VIP_LOCKLIST_NUM = 5;
	};

	/** 锁表一个Hash入口结构 */
	class LockEntry {
	public:
		Lock	m_embededLockObject;	/** 内嵌锁对象 */
		Mutex	m_mutex;				/** 维护该入口可锁对象一致性的互斥锁 */
		Lock	*m_lockLink;			/** 连接属于同一个Hash入口的可锁对象列表 */
		Lock	*m_freeObject;			/** 本入口空闲锁对象链表 */
		Lock	*m_prevLock;			/** 保存上一次调用findLockObject找到的可锁对象的前驱 */
		uint	m_freeObjectCount;		/** 本入口空闲对象个数 */
		u32		m_reservedLocks;		/** 本入口保留的可锁对象数 */
		u32		m_inuseLockObject;		/** 当前该入口管理的可锁对象数 */
		bool	m_embededValid;			/** 内嵌锁对象是否合法 */

		LockEntry() : m_mutex("LockEntry::m_mutex", __FILE__, __LINE__) {
			m_lockLink = m_freeObject = m_prevLock = NULL;
			m_freeObjectCount = m_reservedLocks = 0;
			m_inuseLockObject = 0;
			m_embededValid = true;
		}

		/**
		 * 查询某个键值是否存在已经使用的可锁对象中
		 * @pre 操作之前需要持有该entry的互斥锁
		 * @param key		查询的键值
		 * @param equaler	比较函数对象
		 * @return lock表示找到，NULL表示不存在
		 */
		inline Lock* findLockObject(K key, KE &equaler) {
			if (m_embededValid && equaler(m_embededLockObject.m_objectId, key)) {
				return &m_embededLockObject;
			}

			Lock *lock = m_lockLink;
			m_prevLock = NULL;
			while (lock != NULL) {
				if (equaler(lock->m_objectId, key))
					return lock;
				m_prevLock = lock;
				lock = lock->m_next;
			}
			return NULL;
		}

		/**
		 * 在空闲可锁对象链表中分配一个新的空闲对象
		 * @pre 操作之前需要持有该entry的互斥锁
		 * @return lock表示分配成功，NULL表示空闲链表已经分配光
		 */
		inline Lock* getFreeLock() {
			if (!m_embededValid) {
				m_embededValid = true;
				return &m_embededLockObject;
			}

			if (m_freeObjectCount == 0)
				return NULL;

			Lock *lock = m_freeObject;
			m_freeObject = lock->m_next;
			--m_freeObjectCount;

			return lock;
		}

		/**
		 * 增加一个可锁对象到本entry
		 * @pre 操作之前需要持有该entry的互斥锁
		 * @param lock	新的可锁对象
		 */
		inline void addLock(Lock *lock) {
			++m_inuseLockObject;
			if (&m_embededLockObject == lock)
				return;
			lock->m_next = m_lockLink;
			m_lockLink = lock;
		}

		/**
		 * 从entry当中移除一个lock对象
		 * @pre 操作之前需要持有该entry的互斥锁
		 * @param lock		要移除的可锁对象
		 * @param searched	当前lock的获得是否通过遍历entry获得，如果是，可以直接释放，否则还需要遍历一遍链表
		 */
		inline void removeLock(Lock *lock, bool searched) {
			assert(m_lockLink != NULL || m_embededValid);
			assert(lock->m_waitingLLLink.isEmpty());
			assert(!searched || (&m_embededLockObject == lock || (m_prevLock == NULL && m_lockLink == lock) || m_prevLock->m_next == lock));	// 这里断言在查找和释放之间不会有别人来操作本entry
			--m_inuseLockObject;
			if (lock == &m_embededLockObject)
				return;

			// 判断是否需要重新遍历链表得到lock的前驱
			if (!searched) {
				m_prevLock = NULL;
				Lock *link = m_lockLink;
				while (link != lock) {	// 这里默认一定能找到
					m_prevLock = link;
					link = link->m_next;
				}
			}
			// 从entry当中移除
			if (m_prevLock == NULL) {
				assert(m_lockLink == lock);
				m_lockLink = lock->m_next;
			} else {
				assert(m_prevLock->m_next == lock);
				m_prevLock->m_next = lock->m_next;
			}
		}

		/**
		 * 回收某个可锁对象到空闲链表
		 * @pre 操作之前需要持有该entry的互斥锁
		 * @param lock	可锁对象
		 * @return 是否被本entry回收成功
		 */
		inline bool recycleLock(Lock *lock) {
			if (lock == &m_embededLockObject) {
				m_embededValid = false;
				return true;
			}

			if (m_freeObjectCount >= m_reservedLocks)
				return false;
			// 回收到空闲链表
			lock->m_next = m_freeObject;
			m_freeObject = lock;
			++m_freeObjectCount;
			return true;
		}
	};

	/** 一个事务持有一个LockList，保存当前加锁信息 */
	class LockList {
	public:
		Event				m_event;		/** 各个LockList相互等待信号量 */
		DLink<LockList*>	m_waitingList;	/** 连接等待同一个可锁对象加锁的LockList队列的链表 */
		DLink<LockList*>	m_gWaitingList;	/** 连接全局等待加锁的LockList队列的链表 */
		u64					m_txnId;		/** 该LockList属于的事务号 */
		u64					m_waitTxnId;	/** 该LockList在等待的事务，没有为INVALID_TXN_ID，该信息的读写需要同步，通过m_waiterMutex */
		DList<Lock*>		m_holdingLocks;	/** 该LockList所持有的可锁对象链表 */
		u32					m_visit;		/** 死锁检测当中标志该事务已经被检查过不卷入死锁的信息，该信息的读写需要同步，通过m_waiterMutex */
		bool				m_success;		/** 加锁是否成功 */

		LockList() : m_event(false) {
			m_success = 0;
			m_txnId = m_waitTxnId = INVALID_TXN_ID;
			m_visit = 0;
			m_waitingList.set(this);
			m_gWaitingList.set(this);
		}

		/**
		 * 处理当前LockList被某个可锁对象放锁唤醒的情形
		 * @param lock	放锁的可锁对象
		 */
		void beWakenUp() {
			m_waitingList.unLink();
			m_success = true;
			m_event.signal();
		}
	};

public:
	/**
	 * 创建一个锁管理器
	 *  该锁表至少能放maxLocks把锁，至多放maxLocks + entryCount * reservedLockPerEntry把锁
	 *  
	 * @param maxTxns	最大支持的事务数
	 * @param maxLocks				最大支持的锁个数。注意由于性能考虑，锁管理器并不能保证锁对象不会超过maxLocks个。
	 * @param entryCount			锁表slot数目, slotCount必须是2的整数次幂
	 * @param reservedLockPerEntry	每个slot保留（预分配）的锁对象数目
	 */
	IndicesLockTable(uint maxTxns, uint maxLocks, uint entryCount, uint reservedLockPerEntry) : 
		m_freeLockMutex("LockTable::freeObjectMutex", __FILE__, __LINE__),
		m_waiterMutex("LockTable::waiterMutex", __FILE__, __LINE__) {
		assert(maxLocks && entryCount);
		init(maxTxns, maxLocks, entryCount, reservedLockPerEntry);
	}

	/**
	 * 创建一个锁管理器
	 * 该锁表至少能放maxLocks把锁
	 * @param maxTxns	最大支持的事务数
	 * @param maxLocks	最大支持的锁个数。注意由于性能考虑，锁管理器并不能保证锁对象不会超过maxLocks个。
	 */
	IndicesLockTable(uint maxTxns, uint maxLocks) : 
		m_freeLockMutex("LockTable::freeObjectMutex", __FILE__, __LINE__),
		m_waiterMutex("LockTable::waiterMutex", __FILE__, __LINE__) {
		uint entryCount = flp2(maxLocks);
		init(maxTxns, maxLocks, entryCount, 1);
	}

	~IndicesLockTable() {
		assert(m_deadLockChecker->isAlive());
		m_deadLockChecker->stop();
		m_deadLockChecker->join();
		delete m_deadLockChecker;

		delete [] m_lockEntries;
		delete [] m_lockList;
		delete [] m_lockObject;
		delete [] m_waiterGraph;
	}

	/**
	 * 尝试对可锁对象K加互斥锁
	 * @param txnId		加锁事务号(线程号)
	 * @param key		加锁对象键值
	 * @return true表示加锁成功，false表示加锁失败，此时失败是由于短期加不上锁
	 */
	bool tryLock(u64 txnId, const K& key) {
		LockEntry *entry = getEntry(key);
		LOCK(&entry->m_mutex);

		// 定位锁对象，如果不存在就创建一个新的
		Lock *lock = getLock(entry, key);
		// 说明当前的锁是第一次加上，或者本来就属于当前事务，可以直接返回成功
		if (lock != NULL && (lock->m_txnId == txnId || lock->m_txnId == INVALID_TXN_ID)) {
			if (lock->m_locks == 0) {
				LockList *lockList = &m_lockList[txnId];
				assert(lockList->m_gWaitingList.getList() == NULL && lockList->m_waitingList.getList() == NULL);
				lock->relateLockList(lockList);
			}
			++lock->m_locks;
			UNLOCK(&entry->m_mutex);
			++m_status.m_trylocks;
			return true;
		}

		UNLOCK(&entry->m_mutex);
		return false;
	}

	/**
	 * 对可锁对象K加互斥锁
	 * @param txnId		加锁事务号(线程号)
	 * @param key		加锁对象键值
	 * @return true表示加锁成功，false表示加锁失败，此时失败是由于死锁导致
	 * @throw 锁表内存不足等异常
	 */
	bool lock(u64 txnId, const K& key) throw(NtseException) {
		LockEntry *entry = getEntry(key);
		LOCK(&entry->m_mutex);

		// 定位锁对象，如果不存在就创建一个新的
		Lock *lock = getLock(entry, key);
		if (lock == NULL) {
			// 分配不到锁对象的空间，抛出异常
			UNLOCK(&entry->m_mutex);
			NTSE_THROW(NTSE_EC_TOO_MANY_ROWLOCK, "too many page locks in indices lock manager.");
		}

		LockList *lockList = &m_lockList[txnId];
		assert(lockList->m_gWaitingList.getList() == NULL && lockList->m_waitingList.getList() == NULL);
		// 说明当前的锁是第一次加上，或者本来就属于当前事务，可以直接返回成功
		if (lock->m_txnId == txnId || lock->m_txnId == INVALID_TXN_ID) {
			if (lock->m_locks == 0) {
				lock->relateLockList(lockList);
			}
			++lock->m_locks;
			UNLOCK(&entry->m_mutex);
			++m_status.m_locks;
			return true;
		}

		// 当前锁被其他事务持有，肯定需要等锁，进行睡眠
		return sleep(lock, lockList, entry);
	}

	/**
	 * 对指定对象解锁
	 * @param txnId		加锁事务号(线程号)
	 * @param key		加锁对象键值
	 * @return true表示解锁成功，false表示失败
	 */
	bool unlock(u64 txnId, const K& key) {
		LockEntry *entry = getEntry(key);
		LOCK(&entry->m_mutex);

		// 默认一定能找到可锁对象，逻辑由调用者保证
		Lock *lock = entry->findLockObject(key, m_ikeyEqualer);
		NTSE_ASSERT(lock != NULL && lock->m_txnId == txnId);
		
		// 减少锁计数表示放锁
		--lock->m_locks;
		if (lock->m_locks == 0) {
			// 如果释放之后该锁对象不被本事务持有，从本事务LockList当中移除lock对象
			if (lock->hasWaiter()) {
				// 有人等待，唤醒等待事务
				wakeup(lock);
			} else {
				// 没有其它等待者，可以释放并初始化锁对象
				recycleLock(entry, lock, true);
			}
		}

		UNLOCK(&entry->m_mutex);
		++m_status.m_unlocks;
		return true;
	}

	/**
	 * 释放某个事务持有的所有的锁，同时会初始化该事务对应locklist的信息
	 * @param txnId	加锁事务号(线程号)
	 * @return true表示解锁成功，false表示失败
	 */
	bool unlockAll(u64 txnId) {
		LockList *lockList = &m_lockList[txnId];
		DLink<Lock*> *lockHeader = lockList->m_holdingLocks.getHeader();
		DLink<Lock*> *cur = lockHeader->getNext();

		while (cur != lockHeader) {
			Lock *lock = cur->get();
			cur = cur->getNext();
			assert(cur != NULL);
			assert(lock->m_txnId == txnId);
			LockEntry *entry = getEntry(lock->m_objectId);
			LOCK(&entry->m_mutex);
			
			if (lock->hasWaiter()) {
				// 需要唤醒等待者
				wakeup(lock);
			} else {
				recycleLock(entry, lock, false);
			}

			UNLOCK(&entry->m_mutex);
		}

		++m_status.m_unlockAlls;
		return true;
	}

	/**
	 * 判断某个可锁对象是否由某个事务加上锁
	 * @param txnId		加锁事务号
	 * @Param key		加锁对象KEY
	 * @return true表示加过锁，否则false
	 */
	bool isLocked(u64 txnId, const K& key) {
		LockEntry *entry = getEntry(key);
		LOCK(&entry->m_mutex);

		// 尝试寻找可锁对象是否已经被加上锁
		Lock *lock = entry->findLockObject(key, m_ikeyEqualer);
		if (lock != NULL && lock->m_txnId == txnId) {
			UNLOCK(&entry->m_mutex);
			return true;
		}

		UNLOCK(&entry->m_mutex);
		return false;
	}

	/**
	 * 返回某个事务是否还持有为释放的锁资源
	 * @param txnId	事务号
	 * @return 非0表示有未释放的资源，0没有
	 */
	uint isHoldingLocks(u64 txnId) {
		return m_lockList[txnId].m_holdingLocks.getSize();
	}

	/**
	 * 得到某个锁对象是由那个事务持有，不被持有返回INVALID_TXN_ID
	 * 这里返回的事务号不一定准确，因为释放了entry的互斥锁才返回，可能已经被其他事务重新持有
	 * 因此该接口一般不适用于要求精确的场合，主要提供在单线程和调试的时候使用
	 * @param key	可锁对象ID
	 * @return 持有该可锁对象的事务ID，没有事务持有返回INVALID_TXN_ID
	 */
	u64 whoIsHolding(u64 key) {
		LockEntry *entry = getEntry(key);
		LOCK(&entry->m_mutex);

		// 寻找判断该entry当中是否包含指定可锁对象信息
		Lock *lock = entry->findLockObject(key, m_ikeyEqualer);
		if (lock == NULL) {
			UNLOCK(&entry->m_mutex);
			return INVALID_TXN_ID;
		}

		u64 txnId = lock->m_txnId;
		UNLOCK(&entry->m_mutex);
		return txnId;
	}

	IndiceLockTableStatus& getStatus() {
		// 有些数据需要当场计算
		u32 validEntries = 0;
		u32 totalLockObject = 0;
		u32 maxLockLen = 0;
		for (u32 i = 0; i < m_entryCount; i++) {
			u32 len = m_lockEntries[i].m_inuseLockObject;
			if (len == 0)
				continue;
			validEntries++;
			totalLockObject += len;
			if (len > maxLockLen)
				maxLockLen = len;
		}

		if (validEntries == 0)
			m_status.m_avgConflictLen = 0;
		else
			m_status.m_avgConflictLen = totalLockObject / validEntries;
		m_status.m_maxConflictLen = maxLockLen;
		m_status.m_inuseEntries = validEntries;

		return m_status;
	}

private:
	/**
	 * 锁管理器初始化
	 * @param maxTxns				最大支持的事务数
	 * @param maxLocks				最大支持的锁个数
	 * @param entryCount			锁表入口数目，一定是2的整数次幂
	 * @param reservedLockPerEntry	每个入口保留（预分配）的锁对象数目
	 */
	void init(uint maxTxns, uint maxLocks, uint entryCount, uint reservedLockPerEntry) {
		m_entryCount = entryCount;
		m_entryMask = entryCount - 1;
		m_maxTxns = maxTxns;

		m_lockEntries = new LockEntry[m_entryCount];
		m_lockList = new LockList[maxTxns];
		m_lockObject = new Lock[maxLocks + entryCount * reservedLockPerEntry];

		// 初始化LockList
		for (uint i = 0; i < maxTxns; i++)
			m_lockList[i].m_txnId = i;

		// 初始化各个锁表入口
		for (uint entry = 0; entry < entryCount; entry++) {
			LockEntry *lockEntry = &m_lockEntries[entry];
			lockEntry->m_reservedLocks = reservedLockPerEntry;
			lockEntry->m_freeObjectCount = reservedLockPerEntry;
			lockEntry->m_embededValid = false;

			uint curLockIdx = entry * reservedLockPerEntry;
			lockEntry->m_freeObject = &m_lockObject[curLockIdx];
			for (uint i = 0; i < reservedLockPerEntry - 1; ++i, ++curLockIdx)
				m_lockObject[curLockIdx].m_next = &m_lockObject[curLockIdx + 1];
			m_lockObject[curLockIdx].m_next = NULL;
		}

		// 初始化全局freeObject
		uint curLockIdx = entryCount * reservedLockPerEntry;
		m_freeLocks = &m_lockObject[curLockIdx];		
		for (uint i = 0; i < maxLocks - 1; ++i, ++curLockIdx)
			m_lockObject[curLockIdx].m_next = &m_lockObject[curLockIdx + 1];
		m_lockObject[curLockIdx].m_next = NULL;

		// 初始化等待信息相关
		m_waiters = 0;
		m_checkWaiterNum = 0;
		m_waiterGraph = new LockList*[maxTxns];
		memset(m_waiterGraph, 0, maxTxns * sizeof(m_waiterGraph[0]));

		// 启动死锁检测线程
		m_deadLockChecker = new DeadLockChecker(this);
		m_deadLockChecker->start();
		NTSE_ASSERT(m_deadLockChecker->isAlive());

		memset(&m_status, 0, sizeof(struct LockTableStatus));
	}

	/**
	 * 根据key获取entry
	 * @param key 锁对象关键字
	 * @return key所在的entry对象
	 */
	inline LockEntry* getEntry(const K& key) {
		uint hashCode = m_ikeyHash(key);
		return &m_lockEntries[hashCode & m_entryMask];
	}

	/**
	 * 在一个Hash入口当中寻找指定的可锁对象，如果成功，将可锁对象链入Entry队列
	 * @pre 对加锁对象所属的Entry加了互斥锁
	 * @post 如果分配成功，可锁对象的id赋值为key并且该对象加入entry链表当中
	 * @param entry	Hash入口对象
	 * @param key	可锁对象的键值
	 * @return key对应的可锁对象
	 */
	inline Lock* getLock(LockEntry *entry, const K& key) {
		Lock *lock = entry->findLockObject(key, m_ikeyEqualer);
		if (lock == NULL) {
			if ((lock = allocNewLock(entry)) != NULL) {
				// 将Lock链入Entry队列当中
				lock->m_objectId = key;
				entry->addLock(lock);
			}
		}
		return lock;
	}

	/**
	 * 在entry和全局空闲可锁对象链表当中分配一个空闲可锁对象
	 * @param entry	使用的entry
	 * @return 新分配的Lock，NULL表示当前分配不到
	 */
	inline Lock* allocNewLock(LockEntry *entry) {
		Lock *lock = entry->getFreeLock();
		if (lock == NULL) {	// 尝试从全局空闲队列分配
			LOCK(&m_freeLockMutex);

			if (m_freeLocks != NULL) {
				lock = m_freeLocks;
				m_freeLocks = lock->m_next;
			}

			UNLOCK(&m_freeLockMutex);
		}

		return lock;
	}

	/**
	 * 回收一个指定entry当中的可锁对象
	 * @pre 持有entry的入口锁
	 * @param entry		指定的entry
	 * @param lock		要回收的可锁对象
	 * @param searched	表示lock的获得是不是通过遍历entry链表获得，如果是，移除lock可以很快捷
	 */
	inline void recycleLock(LockEntry *entry, Lock *lock, bool searched) {
		entry->removeLock(lock, searched);

		lock->init();

		if (entry->recycleLock(lock))
			return;

		LOCK(&m_freeLockMutex);

		lock->m_next = m_freeLocks;
		m_freeLocks = lock;

		UNLOCK(&m_freeLockMutex);
	}

	/**
	 * 加锁冲突的时候需要睡眠等待
	 * @pre 对等待加锁对象所属的Entry加了互斥锁
	 * @post 无论等待失败或者成功，Entry的互斥锁被释放
	 * @param lock		要等待加锁的锁对象
	 * @param lockList	当前要加锁事务的LockList
	 * @param entry		等锁对象所在的Hash入口entry
	 * @return true加锁等待成功，false加锁等待失败
	 */
	bool sleep(Lock *lock, LockList *lockList, LockEntry *entry) {
		LOCK(&m_waiterMutex);
		lockList->m_waitTxnId = lock->m_txnId;
		lockList->m_success = false;
		// 首先进行死锁预防
		u64 victimTxn = deadlockCheckCycle(lockList, true, 1);
		if (victimTxn == lockList->m_txnId) {
			// 直接存在死锁，回滚自己
			lockList->m_waitTxnId = INVALID_TXN_ID;
			UNLOCK(&m_waiterMutex);
			UNLOCK(&entry->m_mutex);
			return false;
		}

		// 加入全局等待队列和锁等待队列
		assert(lockList->m_gWaitingList.getList() == NULL && lockList->m_waitingList.getList() == NULL);
		m_waitQueue.addLast(&lockList->m_gWaitingList);
		lock->addWaiter(lockList);
		UNLOCK(&m_waiterMutex);
		UNLOCK(&entry->m_mutex);

		++m_status.m_waits;
		u64 begin = System::currentTimeMillis();

		// 执行等待
		while (true) {
			long sigToken = lockList->m_event.reset();
			// 防止被假唤醒，如果是加锁成功或者死锁检测失败，m_wantTxnId都会设置为INVALID_TXN_ID
			if (lockList->m_waitTxnId == INVALID_TXN_ID)
				break;
			lockList->m_event.wait(5000, sigToken);
		}

		u64 end = System::currentTimeMillis();

		// 如果此时是被死锁检测线程杀死，需要从锁对象等待列表当中移除
		if (!lockList->m_success) {
			LOCK(&entry->m_mutex);
			lockList->m_waitingList.unLink();
			UNLOCK(&entry->m_mutex);
			m_status.m_fWaitTime += end - begin;
		} else {
			m_status.m_sWaitTime += end - begin;
			++m_status.m_locks;
		}

		assert(lockList->m_gWaitingList.getList() == NULL && lockList->m_waitingList.getList() == NULL);
		return lockList->m_success;
	}

	/**
	 * 唤醒在等候某个可锁对象的事务
	 * @pre 调用者需要持有lock所在entry的入口锁
	 * @param lock	刚执行放锁的可锁对象
	 */
	void wakeup(Lock *lock) {
		assert(lock->hasWaiter());
		u64 unlockTxnId = lock->m_txnId;
		DLink<LockList*> *waitingHeader = lock->m_waitingLLLink.getHeader();

		// 唤醒等待队列当中的第一个事务
		LockList *wakeupLockList = waitingHeader->getNext()->get();
		u64 wakeupTxnId = wakeupLockList->m_txnId;
		NTSE_ASSERT(wakeupLockList->m_waitTxnId == unlockTxnId);
		lock->changeOwner(wakeupLockList);
		LOCK(&m_waiterMutex);
		wakeupLockList->m_waitTxnId = INVALID_TXN_ID;
		wakeupLockList->m_gWaitingList.unLink();
		UNLOCK(&m_waiterMutex);
		wakeupLockList->beWakenUp();

		SYNCHERE(SP_ILT_AFTER_WAKEUP);

		// 遍历等待队列链表，修改剩余等待事务的等待信息
		DLink<LockList*> *curLL = waitingHeader->getNext();
		LOCK(&m_waiterMutex);
		while (curLL != waitingHeader) {
			LockList *waitingLockList = curLL->get();
			assert(waitingLockList->m_waitTxnId == unlockTxnId);
			waitingLockList->m_waitTxnId = wakeupTxnId;
			curLL = curLL->getNext();
		}
		UNLOCK(&m_waiterMutex);
	}

	/**
	 * 从指定LockList出发，检查当前是否存在死锁环
	 * @param lockList		指定检查起始的LockList
	 * @param cleanCheckInfo检查结束之后是否清楚本次检查遍历的LockList的标志信息，避免死锁检测过程中的重复检查
	 * @param visit`		访问信息的值，在一次死锁检测过程中visit从1开始递增
	 * @return -1表示不存在死锁环，否则表示存在一个环，同时可以通过杀死返回的事务号的事务消除死锁
	 */
	u64 deadlockCheckCycle(LockList *lockList, bool cleanCheckInfo, u32 visit) {
		assert(!cleanCheckInfo || (cleanCheckInfo && m_checkWaiterNum == 0));
		LockList *startLockList = &m_lockList[lockList->m_txnId];
		u64 victim = INVALID_TXN_ID;

		do {
			if (startLockList->m_visit == visit) {
				// 又走到某个本次遍历过程中遇到的节点，存在死锁
				victim = startLockList->m_txnId;
				nftrace(ts.irl, tout << "dl: " << victim << " wait for: " << m_lockList[victim].m_waitTxnId << " and " << m_waiterGraph[m_checkWaiterNum - 1]->m_txnId << " wait for me";);
				++m_status.m_deadlocks;
				goto finish;
			} else if (startLockList->m_visit != 0) {
				// 该节点在前面的某次遍历中检查过，不应该出现环，本支路无死锁
				assert(startLockList->m_visit < visit);
				goto finish;
			}

			startLockList->m_visit = visit;
			m_waiterGraph[m_checkWaiterNum++] = startLockList;
			startLockList = &m_lockList[startLockList->m_waitTxnId];
		} while (startLockList->m_waitTxnId != INVALID_TXN_ID);

	finish:
		if (cleanCheckInfo) {
			for (u32 i = 0; i < m_checkWaiterNum; i++)
				m_waiterGraph[i]->m_visit = 0;
			m_checkWaiterNum = 0;
		}

		return victim;
	}

	/**
	 * 检测死锁线程主体函数
	 * 从等待队列当中的每个事务出发遍历，采用标志避免遍历已经遍历过的事务
	 */
	void deadlockDetecting() {
		LOCK(&m_waiterMutex);

		// 遍历等待队列当中的每个事务，并且检查各个事务会不会导致死锁
		u32 visit = 1;
		DLink<LockList*> *header = m_waitQueue.getHeader();
		DLink<LockList*> *start = header->getNext();
		while (start != header) {
			u64 victim;
			LockList *lockList = start->get();
			if (lockList->m_waitTxnId == INVALID_TXN_ID || lockList->m_visit != 0) {
				// 已经检查过或者当前事务不会产生死锁
				start = start->getNext();
				continue;	
			}
			if ((victim = deadlockCheckCycle(lockList, false, visit)) != INVALID_TXN_ID) {
				// 当前事务会产生死锁，杀死返回的牺牲者事务
				LockList *victimLL = &m_lockList[victim];
				victimLL->m_waitTxnId = INVALID_TXN_ID;
				victimLL->m_gWaitingList.unLink();
				victimLL->m_event.signal();
				break;
			}
			visit++;
			start = start->getNext();
		}

		// 清除访问信息
		for (uint i = 0; i < m_checkWaiterNum; i++)
			m_waiterGraph[i]->m_visit = 0;
		m_checkWaiterNum = 0;

		UNLOCK(&m_waiterMutex);
	}

	/**
	 * 得到指定数值内最大的2的整数次幂的正数
	 * @param v	指定的数值
	 * @return 最大2的正数次幂
	 */
	inline uint flp2(uint v) {
		uint y = 0x80000000;  
		while (y > v)   
			y = y >> 1; 
		return y; 
	}

private:
	uint			m_maxTxns;				/** 锁表支持的最大事务数 */

	// 锁表以及Hash表相关变量
	KH				m_ikeyHash;				/** 哈希函数对象 */
	KE				m_ikeyEqualer;			/** 比较函数对象 */

	LockEntry		*m_lockEntries;			/** 锁表哈希各个入口项 */
	u32				m_entryCount;			/** 锁表入口项数 */
	u32				m_entryMask;			/** 入口Hash掩码 */

	// 锁表使用的LockList和Lock信息
	LockList		*m_lockList;			/** 锁表使用的所有LockList */
	Lock			*m_lockObject;			/** 锁表中所有的可锁对象列表 */
	Mutex			m_freeLockMutex;		/** 保护全局空闲Lock对象链表的互斥锁 */
	Lock			*m_freeLocks;			/** 全局空闲锁对象 */

	// 死锁检测相关变量
	Task			*m_deadLockChecker;		/** 死锁检测线程 */
	Mutex			m_waiterMutex;			/** 等待信息队列互斥锁 */
	u32				m_waiters;				/** 等待线程数目 */
	LockList		**m_waiterGraph;		/** 等待图，用于检测死锁 */
	u32				m_checkWaiterNum;		/** 已经检测过的等待者个数 */
	DList<LockList*> m_waitQueue;			/** 全局等待队列 */

	// 统计信息
	IndiceLockTableStatus m_status;			/** 锁表的使用统计信息，不做持久化 */
};

/** 锁管理器 */
class IndicesLockManager: public IndicesLockTable<u64> {
	typedef IndicesLockTable<u64>  SUPER;
public:
	/**
	 * 创建一个锁管理器
	 *  该锁表至少能放maxLocks把锁，至多放maxLocks + entryCount*reservedLockPerEntry把锁
	 * @param maxTxns	最大支持的事务数
	 * @param maxLocks 最大支持的锁个数。注意由于性能考虑，锁管理器并不能保证锁对象不会超过maxLocks个。
	 * @param entryCount 锁表entry数目
	 * @param reservedLockPerSlot 每个entry保留（预分配）的锁对象数目
	 */
	IndicesLockManager(uint maxTxns, uint maxLocks, uint entryCount, uint reservedLockPerEntry)
		: SUPER(maxTxns, maxLocks, entryCount, reservedLockPerEntry) {
	}


	/**
	 * 创建一个锁管理器
	 *
	 * @param maxTxns	最大支持的事务数
	 * @param maxLocks	最大支持的锁个数。注意由于性能考虑，锁管理器并不能保证锁对象不会超过maxLocks个。
	 */
	IndicesLockManager(uint maxTxns, uint maxLocks)
		: SUPER(maxTxns, maxLocks) {
	}
};

}

#endif
