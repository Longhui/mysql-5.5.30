/**
 * ����ҳ����ר�õ���������ʵ��
 *
 * @author �ձ�(naturally@163.org)
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


/** �������ͳ����Ϣ */
struct IndiceLockTableStatus {
	u64	m_locks;			/** ����������� */
	u64	m_trylocks;			/** trylock���� */
	u64	m_deadlocks;		/** �������� */
	u64	m_unlocks;			/** ����������� */
	u64	m_unlockAlls;		/** һ���Է���������� */
	u64 m_waits;			/** �ȴ����� */
	u64	m_sWaitTime;		/** �ȴ��ɹ��ĵȴ�ʱ�䣨��λ���룩*/
	u64	m_fWaitTime;		/** �ȴ�ʧ�ܵĵȴ�ʱ�䣨��λ���룩*/
	u64 m_avgConflictLen;	/** ��ͻ�����ƽ������ */
	u64 m_maxConflictLen;	/** ��ͻ�������󳤶� */
	u64 m_inuseEntries;		/** ��ǰ����ʹ�õ���Entry�� */
	u32	m_activeTxn;		/** ��Ծ������ */
	u32	m_activeLocks;		/** ���������� */
};


/** ����
 * ��������Ҫ������
 * 1. ֻ����ӻ����X��
 * 2. ����������
 * 3. ��Ԥ���������
 * 4. ��һ�����ͷ�ĳ��������е�������
 * ��NTSE���棬Session�൱��һ������
 *
 * @param K �����������
 * @param KH ��������������ϣֵ�ĺ�������Ĭ��ΪHasher<K>
 * @param KE �Ƚ��������Ƿ���ȵĺ�������Ĭ��ΪEqualer<K, K>
 */
template < typename K, typename KH = Hasher<K>, typename KE = Equaler<K, K> > 
class IndicesLockTable {
private:
	/**
 	 * �������ڼ���࣬�����⵽���������־��Ҫɱ�������񣬲��һ��Ѹ�����
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
		IndicesLockTable<K, KH, KE> *m_lockTable;					/** ��Ҫ��������������� */

		static const uint DEADLOCK_CHECK_INTERNAL = 1000;	/** �����̼߳�����ڣ���λ���� */
	};

	class LockList;
	/** ������������ */
	struct Lock {
		K					m_objectId;		/** �������� */
		u64					m_txnId;		/** ������������� */
		DLink<Lock*>		m_holderLLLink;	/** ��ǰ������LockList��ӵ�е�Lock���� */
		DList<LockList*>	m_waitingLLLink;/** ��ǰ�ȴ��ÿ�������������LockList���� */
		Lock				*m_next;		/** ����ͬһEntry�Ŀ�������������߿��п����������� */
		u32					m_locks;		/** ��ǰ�����߳������Ĵ��� */

		Lock() {
			m_holderLLLink.set(this);
			init();
		}

		/**
		 * ��ʼ��������
		 */
		inline void init() {
			m_txnId = INVALID_TXN_ID;
			m_locks = 0;
			//m_objectId = 0;
			m_next = NULL;
			m_holderLLLink.unLink();
		}
		
		/**
		 * @pre ������������ڵĻ�����
		 * ���ص�ǰ����û�еȴ���
		 */
		inline bool hasWaiter() {
			return !m_waitingLLLink.isEmpty();
		}

		/**
		 * ���������ӵ����lockList��������
		 * @pre ������������ڵĻ�����
		 * @param lockList	ӵ����lockList����
		 */
		inline void relateLockList(LockList *lockList) {
			m_txnId = lockList->m_txnId;
			lockList->m_holdingLocks.addLast(&m_holderLLLink);		// ����˳��ʼ�ռ���β��
		}

		/**
		 * ��������ӵ����
		 * @pre ������������ڵĻ�����
		 * @param lockList	�µ�ӵ����lockList����
		 */
		inline void changeOwner(LockList *lockList) {
			m_locks = 1;
			m_holderLLLink.unLink();
			relateLockList(lockList);
		}

		/**
		 * ��ĳ���ȸ��������LockList����ȴ����е��У�����LockList��Ȩ�أ��������뵽�ȴ����е��ĸ�λ��
		 * @pre Ҫ��֤������ڶ���������Եȴ����б���������
		 * @param lockList	�ȴ�������LockList����
		 */
		inline void addWaiter(LockList *lockList) {
			uint weight = lockList->m_holdingLocks.getSize();
			if (weight < LOCKLIST_LIGHTEST_WEIGHT || m_waitingLLLink.isEmpty())
				m_waitingLLLink.addLast(&lockList->m_waitingList);
			else {	// Ȩ�رȽϴ��LockListӦ�ÿ����ŵ��ȴ�����ǰ������õ���
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

				if (i == vipLLNum)	// ���Ȩ�ز�����Ϊ�˱�֤����ʱ�䲻��̫�����˶������ŵ����
					m_waitingLLLink.addLast(&lockList->m_waitingList);
			}
		}

		static const uint LOCKLIST_LIGHTEST_WEIGHT = 2;
		static const uint MAX_VIP_LOCKLIST_NUM = 5;
	};

	/** ����һ��Hash��ڽṹ */
	class LockEntry {
	public:
		Lock	m_embededLockObject;	/** ��Ƕ������ */
		Mutex	m_mutex;				/** ά������ڿ�������һ���ԵĻ����� */
		Lock	*m_lockLink;			/** ��������ͬһ��Hash��ڵĿ��������б� */
		Lock	*m_freeObject;			/** ����ڿ������������� */
		Lock	*m_prevLock;			/** ������һ�ε���findLockObject�ҵ��Ŀ��������ǰ�� */
		uint	m_freeObjectCount;		/** ����ڿ��ж������ */
		u32		m_reservedLocks;		/** ����ڱ����Ŀ��������� */
		u32		m_inuseLockObject;		/** ��ǰ����ڹ���Ŀ��������� */
		bool	m_embededValid;			/** ��Ƕ�������Ƿ�Ϸ� */

		LockEntry() : m_mutex("LockEntry::m_mutex", __FILE__, __LINE__) {
			m_lockLink = m_freeObject = m_prevLock = NULL;
			m_freeObjectCount = m_reservedLocks = 0;
			m_inuseLockObject = 0;
			m_embededValid = true;
		}

		/**
		 * ��ѯĳ����ֵ�Ƿ�����Ѿ�ʹ�õĿ���������
		 * @pre ����֮ǰ��Ҫ���и�entry�Ļ�����
		 * @param key		��ѯ�ļ�ֵ
		 * @param equaler	�ȽϺ�������
		 * @return lock��ʾ�ҵ���NULL��ʾ������
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
		 * �ڿ��п������������з���һ���µĿ��ж���
		 * @pre ����֮ǰ��Ҫ���и�entry�Ļ�����
		 * @return lock��ʾ����ɹ���NULL��ʾ���������Ѿ������
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
		 * ����һ���������󵽱�entry
		 * @pre ����֮ǰ��Ҫ���и�entry�Ļ�����
		 * @param lock	�µĿ�������
		 */
		inline void addLock(Lock *lock) {
			++m_inuseLockObject;
			if (&m_embededLockObject == lock)
				return;
			lock->m_next = m_lockLink;
			m_lockLink = lock;
		}

		/**
		 * ��entry�����Ƴ�һ��lock����
		 * @pre ����֮ǰ��Ҫ���и�entry�Ļ�����
		 * @param lock		Ҫ�Ƴ��Ŀ�������
		 * @param searched	��ǰlock�Ļ���Ƿ�ͨ������entry��ã�����ǣ�����ֱ���ͷţ�������Ҫ����һ������
		 */
		inline void removeLock(Lock *lock, bool searched) {
			assert(m_lockLink != NULL || m_embededValid);
			assert(lock->m_waitingLLLink.isEmpty());
			assert(!searched || (&m_embededLockObject == lock || (m_prevLock == NULL && m_lockLink == lock) || m_prevLock->m_next == lock));	// ��������ڲ��Һ��ͷ�֮�䲻���б�����������entry
			--m_inuseLockObject;
			if (lock == &m_embededLockObject)
				return;

			// �ж��Ƿ���Ҫ���±�������õ�lock��ǰ��
			if (!searched) {
				m_prevLock = NULL;
				Lock *link = m_lockLink;
				while (link != lock) {	// ����Ĭ��һ�����ҵ�
					m_prevLock = link;
					link = link->m_next;
				}
			}
			// ��entry�����Ƴ�
			if (m_prevLock == NULL) {
				assert(m_lockLink == lock);
				m_lockLink = lock->m_next;
			} else {
				assert(m_prevLock->m_next == lock);
				m_prevLock->m_next = lock->m_next;
			}
		}

		/**
		 * ����ĳ���������󵽿�������
		 * @pre ����֮ǰ��Ҫ���и�entry�Ļ�����
		 * @param lock	��������
		 * @return �Ƿ񱻱�entry���ճɹ�
		 */
		inline bool recycleLock(Lock *lock) {
			if (lock == &m_embededLockObject) {
				m_embededValid = false;
				return true;
			}

			if (m_freeObjectCount >= m_reservedLocks)
				return false;
			// ���յ���������
			lock->m_next = m_freeObject;
			m_freeObject = lock;
			++m_freeObjectCount;
			return true;
		}
	};

	/** һ���������һ��LockList�����浱ǰ������Ϣ */
	class LockList {
	public:
		Event				m_event;		/** ����LockList�໥�ȴ��ź��� */
		DLink<LockList*>	m_waitingList;	/** ���ӵȴ�ͬһ���������������LockList���е����� */
		DLink<LockList*>	m_gWaitingList;	/** ����ȫ�ֵȴ�������LockList���е����� */
		u64					m_txnId;		/** ��LockList���ڵ������ */
		u64					m_waitTxnId;	/** ��LockList�ڵȴ�������û��ΪINVALID_TXN_ID������Ϣ�Ķ�д��Ҫͬ����ͨ��m_waiterMutex */
		DList<Lock*>		m_holdingLocks;	/** ��LockList�����еĿ����������� */
		u32					m_visit;		/** ������⵱�б�־�������Ѿ���������������������Ϣ������Ϣ�Ķ�д��Ҫͬ����ͨ��m_waiterMutex */
		bool				m_success;		/** �����Ƿ�ɹ� */

		LockList() : m_event(false) {
			m_success = 0;
			m_txnId = m_waitTxnId = INVALID_TXN_ID;
			m_visit = 0;
			m_waitingList.set(this);
			m_gWaitingList.set(this);
		}

		/**
		 * ����ǰLockList��ĳ����������������ѵ�����
		 * @param lock	�����Ŀ�������
		 */
		void beWakenUp() {
			m_waitingList.unLink();
			m_success = true;
			m_event.signal();
		}
	};

public:
	/**
	 * ����һ����������
	 *  �����������ܷ�maxLocks�����������maxLocks + entryCount * reservedLockPerEntry����
	 *  
	 * @param maxTxns	���֧�ֵ�������
	 * @param maxLocks				���֧�ֵ���������ע���������ܿ��ǣ��������������ܱ�֤�����󲻻ᳬ��maxLocks����
	 * @param entryCount			����slot��Ŀ, slotCount������2����������
	 * @param reservedLockPerEntry	ÿ��slot������Ԥ���䣩����������Ŀ
	 */
	IndicesLockTable(uint maxTxns, uint maxLocks, uint entryCount, uint reservedLockPerEntry) : 
		m_freeLockMutex("LockTable::freeObjectMutex", __FILE__, __LINE__),
		m_waiterMutex("LockTable::waiterMutex", __FILE__, __LINE__) {
		assert(maxLocks && entryCount);
		init(maxTxns, maxLocks, entryCount, reservedLockPerEntry);
	}

	/**
	 * ����һ����������
	 * �����������ܷ�maxLocks����
	 * @param maxTxns	���֧�ֵ�������
	 * @param maxLocks	���֧�ֵ���������ע���������ܿ��ǣ��������������ܱ�֤�����󲻻ᳬ��maxLocks����
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
	 * ���ԶԿ�������K�ӻ�����
	 * @param txnId		���������(�̺߳�)
	 * @param key		���������ֵ
	 * @return true��ʾ�����ɹ���false��ʾ����ʧ�ܣ���ʱʧ�������ڶ��ڼӲ�����
	 */
	bool tryLock(u64 txnId, const K& key) {
		LockEntry *entry = getEntry(key);
		LOCK(&entry->m_mutex);

		// ��λ��������������ھʹ���һ���µ�
		Lock *lock = getLock(entry, key);
		// ˵����ǰ�����ǵ�һ�μ��ϣ����߱��������ڵ�ǰ���񣬿���ֱ�ӷ��سɹ�
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
	 * �Կ�������K�ӻ�����
	 * @param txnId		���������(�̺߳�)
	 * @param key		���������ֵ
	 * @return true��ʾ�����ɹ���false��ʾ����ʧ�ܣ���ʱʧ����������������
	 * @throw �����ڴ治����쳣
	 */
	bool lock(u64 txnId, const K& key) throw(NtseException) {
		LockEntry *entry = getEntry(key);
		LOCK(&entry->m_mutex);

		// ��λ��������������ھʹ���һ���µ�
		Lock *lock = getLock(entry, key);
		if (lock == NULL) {
			// ���䲻��������Ŀռ䣬�׳��쳣
			UNLOCK(&entry->m_mutex);
			NTSE_THROW(NTSE_EC_TOO_MANY_ROWLOCK, "too many page locks in indices lock manager.");
		}

		LockList *lockList = &m_lockList[txnId];
		assert(lockList->m_gWaitingList.getList() == NULL && lockList->m_waitingList.getList() == NULL);
		// ˵����ǰ�����ǵ�һ�μ��ϣ����߱��������ڵ�ǰ���񣬿���ֱ�ӷ��سɹ�
		if (lock->m_txnId == txnId || lock->m_txnId == INVALID_TXN_ID) {
			if (lock->m_locks == 0) {
				lock->relateLockList(lockList);
			}
			++lock->m_locks;
			UNLOCK(&entry->m_mutex);
			++m_status.m_locks;
			return true;
		}

		// ��ǰ��������������У��϶���Ҫ����������˯��
		return sleep(lock, lockList, entry);
	}

	/**
	 * ��ָ���������
	 * @param txnId		���������(�̺߳�)
	 * @param key		���������ֵ
	 * @return true��ʾ�����ɹ���false��ʾʧ��
	 */
	bool unlock(u64 txnId, const K& key) {
		LockEntry *entry = getEntry(key);
		LOCK(&entry->m_mutex);

		// Ĭ��һ�����ҵ����������߼��ɵ����߱�֤
		Lock *lock = entry->findLockObject(key, m_ikeyEqualer);
		NTSE_ASSERT(lock != NULL && lock->m_txnId == txnId);
		
		// ������������ʾ����
		--lock->m_locks;
		if (lock->m_locks == 0) {
			// ����ͷ�֮��������󲻱���������У��ӱ�����LockList�����Ƴ�lock����
			if (lock->hasWaiter()) {
				// ���˵ȴ������ѵȴ�����
				wakeup(lock);
			} else {
				// û�������ȴ��ߣ������ͷŲ���ʼ��������
				recycleLock(entry, lock, true);
			}
		}

		UNLOCK(&entry->m_mutex);
		++m_status.m_unlocks;
		return true;
	}

	/**
	 * �ͷ�ĳ��������е����е�����ͬʱ���ʼ���������Ӧlocklist����Ϣ
	 * @param txnId	���������(�̺߳�)
	 * @return true��ʾ�����ɹ���false��ʾʧ��
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
				// ��Ҫ���ѵȴ���
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
	 * �ж�ĳ�����������Ƿ���ĳ�����������
	 * @param txnId		���������
	 * @Param key		��������KEY
	 * @return true��ʾ�ӹ���������false
	 */
	bool isLocked(u64 txnId, const K& key) {
		LockEntry *entry = getEntry(key);
		LOCK(&entry->m_mutex);

		// ����Ѱ�ҿ��������Ƿ��Ѿ���������
		Lock *lock = entry->findLockObject(key, m_ikeyEqualer);
		if (lock != NULL && lock->m_txnId == txnId) {
			UNLOCK(&entry->m_mutex);
			return true;
		}

		UNLOCK(&entry->m_mutex);
		return false;
	}

	/**
	 * ����ĳ�������Ƿ񻹳���Ϊ�ͷŵ�����Դ
	 * @param txnId	�����
	 * @return ��0��ʾ��δ�ͷŵ���Դ��0û��
	 */
	uint isHoldingLocks(u64 txnId) {
		return m_lockList[txnId].m_holdingLocks.getSize();
	}

	/**
	 * �õ�ĳ�������������Ǹ�������У��������з���INVALID_TXN_ID
	 * ���ﷵ�ص�����Ų�һ��׼ȷ����Ϊ�ͷ���entry�Ļ������ŷ��أ������Ѿ��������������³���
	 * ��˸ýӿ�һ�㲻������Ҫ��ȷ�ĳ��ϣ���Ҫ�ṩ�ڵ��̺߳͵��Ե�ʱ��ʹ��
	 * @param key	��������ID
	 * @return ���иÿ������������ID��û��������з���INVALID_TXN_ID
	 */
	u64 whoIsHolding(u64 key) {
		LockEntry *entry = getEntry(key);
		LOCK(&entry->m_mutex);

		// Ѱ���жϸ�entry�����Ƿ����ָ������������Ϣ
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
		// ��Щ������Ҫ��������
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
	 * ����������ʼ��
	 * @param maxTxns				���֧�ֵ�������
	 * @param maxLocks				���֧�ֵ�������
	 * @param entryCount			���������Ŀ��һ����2����������
	 * @param reservedLockPerEntry	ÿ����ڱ�����Ԥ���䣩����������Ŀ
	 */
	void init(uint maxTxns, uint maxLocks, uint entryCount, uint reservedLockPerEntry) {
		m_entryCount = entryCount;
		m_entryMask = entryCount - 1;
		m_maxTxns = maxTxns;

		m_lockEntries = new LockEntry[m_entryCount];
		m_lockList = new LockList[maxTxns];
		m_lockObject = new Lock[maxLocks + entryCount * reservedLockPerEntry];

		// ��ʼ��LockList
		for (uint i = 0; i < maxTxns; i++)
			m_lockList[i].m_txnId = i;

		// ��ʼ�������������
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

		// ��ʼ��ȫ��freeObject
		uint curLockIdx = entryCount * reservedLockPerEntry;
		m_freeLocks = &m_lockObject[curLockIdx];		
		for (uint i = 0; i < maxLocks - 1; ++i, ++curLockIdx)
			m_lockObject[curLockIdx].m_next = &m_lockObject[curLockIdx + 1];
		m_lockObject[curLockIdx].m_next = NULL;

		// ��ʼ���ȴ���Ϣ���
		m_waiters = 0;
		m_checkWaiterNum = 0;
		m_waiterGraph = new LockList*[maxTxns];
		memset(m_waiterGraph, 0, maxTxns * sizeof(m_waiterGraph[0]));

		// ������������߳�
		m_deadLockChecker = new DeadLockChecker(this);
		m_deadLockChecker->start();
		NTSE_ASSERT(m_deadLockChecker->isAlive());

		memset(&m_status, 0, sizeof(struct LockTableStatus));
	}

	/**
	 * ����key��ȡentry
	 * @param key ������ؼ���
	 * @return key���ڵ�entry����
	 */
	inline LockEntry* getEntry(const K& key) {
		uint hashCode = m_ikeyHash(key);
		return &m_lockEntries[hashCode & m_entryMask];
	}

	/**
	 * ��һ��Hash��ڵ���Ѱ��ָ���Ŀ�����������ɹ�����������������Entry����
	 * @pre �Լ�������������Entry���˻�����
	 * @post �������ɹ������������id��ֵΪkey���Ҹö������entry������
	 * @param entry	Hash��ڶ���
	 * @param key	��������ļ�ֵ
	 * @return key��Ӧ�Ŀ�������
	 */
	inline Lock* getLock(LockEntry *entry, const K& key) {
		Lock *lock = entry->findLockObject(key, m_ikeyEqualer);
		if (lock == NULL) {
			if ((lock = allocNewLock(entry)) != NULL) {
				// ��Lock����Entry���е���
				lock->m_objectId = key;
				entry->addLock(lock);
			}
		}
		return lock;
	}

	/**
	 * ��entry��ȫ�ֿ��п������������з���һ�����п�������
	 * @param entry	ʹ�õ�entry
	 * @return �·����Lock��NULL��ʾ��ǰ���䲻��
	 */
	inline Lock* allocNewLock(LockEntry *entry) {
		Lock *lock = entry->getFreeLock();
		if (lock == NULL) {	// ���Դ�ȫ�ֿ��ж��з���
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
	 * ����һ��ָ��entry���еĿ�������
	 * @pre ����entry�������
	 * @param entry		ָ����entry
	 * @param lock		Ҫ���յĿ�������
	 * @param searched	��ʾlock�Ļ���ǲ���ͨ������entry�����ã�����ǣ��Ƴ�lock���Ժܿ��
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
	 * ������ͻ��ʱ����Ҫ˯�ߵȴ�
	 * @pre �Եȴ���������������Entry���˻�����
	 * @post ���۵ȴ�ʧ�ܻ��߳ɹ���Entry�Ļ��������ͷ�
	 * @param lock		Ҫ�ȴ�������������
	 * @param lockList	��ǰҪ���������LockList
	 * @param entry		�����������ڵ�Hash���entry
	 * @return true�����ȴ��ɹ���false�����ȴ�ʧ��
	 */
	bool sleep(Lock *lock, LockList *lockList, LockEntry *entry) {
		LOCK(&m_waiterMutex);
		lockList->m_waitTxnId = lock->m_txnId;
		lockList->m_success = false;
		// ���Ƚ�������Ԥ��
		u64 victimTxn = deadlockCheckCycle(lockList, true, 1);
		if (victimTxn == lockList->m_txnId) {
			// ֱ�Ӵ����������ع��Լ�
			lockList->m_waitTxnId = INVALID_TXN_ID;
			UNLOCK(&m_waiterMutex);
			UNLOCK(&entry->m_mutex);
			return false;
		}

		// ����ȫ�ֵȴ����к����ȴ�����
		assert(lockList->m_gWaitingList.getList() == NULL && lockList->m_waitingList.getList() == NULL);
		m_waitQueue.addLast(&lockList->m_gWaitingList);
		lock->addWaiter(lockList);
		UNLOCK(&m_waiterMutex);
		UNLOCK(&entry->m_mutex);

		++m_status.m_waits;
		u64 begin = System::currentTimeMillis();

		// ִ�еȴ�
		while (true) {
			long sigToken = lockList->m_event.reset();
			// ��ֹ���ٻ��ѣ�����Ǽ����ɹ������������ʧ�ܣ�m_wantTxnId��������ΪINVALID_TXN_ID
			if (lockList->m_waitTxnId == INVALID_TXN_ID)
				break;
			lockList->m_event.wait(5000, sigToken);
		}

		u64 end = System::currentTimeMillis();

		// �����ʱ�Ǳ���������߳�ɱ������Ҫ��������ȴ��б����Ƴ�
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
	 * �����ڵȺ�ĳ���������������
	 * @pre ��������Ҫ����lock����entry�������
	 * @param lock	��ִ�з����Ŀ�������
	 */
	void wakeup(Lock *lock) {
		assert(lock->hasWaiter());
		u64 unlockTxnId = lock->m_txnId;
		DLink<LockList*> *waitingHeader = lock->m_waitingLLLink.getHeader();

		// ���ѵȴ����е��еĵ�һ������
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

		// �����ȴ����������޸�ʣ��ȴ�����ĵȴ���Ϣ
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
	 * ��ָ��LockList��������鵱ǰ�Ƿ����������
	 * @param lockList		ָ�������ʼ��LockList
	 * @param cleanCheckInfo������֮���Ƿ�������μ�������LockList�ı�־��Ϣ�����������������е��ظ����
	 * @param visit`		������Ϣ��ֵ����һ��������������visit��1��ʼ����
	 * @return -1��ʾ�������������������ʾ����һ������ͬʱ����ͨ��ɱ�����ص�����ŵ�������������
	 */
	u64 deadlockCheckCycle(LockList *lockList, bool cleanCheckInfo, u32 visit) {
		assert(!cleanCheckInfo || (cleanCheckInfo && m_checkWaiterNum == 0));
		LockList *startLockList = &m_lockList[lockList->m_txnId];
		u64 victim = INVALID_TXN_ID;

		do {
			if (startLockList->m_visit == visit) {
				// ���ߵ�ĳ�����α��������������Ľڵ㣬��������
				victim = startLockList->m_txnId;
				nftrace(ts.irl, tout << "dl: " << victim << " wait for: " << m_lockList[victim].m_waitTxnId << " and " << m_waiterGraph[m_checkWaiterNum - 1]->m_txnId << " wait for me";);
				++m_status.m_deadlocks;
				goto finish;
			} else if (startLockList->m_visit != 0) {
				// �ýڵ���ǰ���ĳ�α����м�������Ӧ�ó��ֻ�����֧·������
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
	 * ��������߳����庯��
	 * �ӵȴ����е��е�ÿ������������������ñ�־��������Ѿ�������������
	 */
	void deadlockDetecting() {
		LOCK(&m_waiterMutex);

		// �����ȴ����е��е�ÿ�����񣬲��Ҽ���������᲻�ᵼ������
		u32 visit = 1;
		DLink<LockList*> *header = m_waitQueue.getHeader();
		DLink<LockList*> *start = header->getNext();
		while (start != header) {
			u64 victim;
			LockList *lockList = start->get();
			if (lockList->m_waitTxnId == INVALID_TXN_ID || lockList->m_visit != 0) {
				// �Ѿ��������ߵ�ǰ���񲻻��������
				start = start->getNext();
				continue;	
			}
			if ((victim = deadlockCheckCycle(lockList, false, visit)) != INVALID_TXN_ID) {
				// ��ǰ��������������ɱ�����ص�����������
				LockList *victimLL = &m_lockList[victim];
				victimLL->m_waitTxnId = INVALID_TXN_ID;
				victimLL->m_gWaitingList.unLink();
				victimLL->m_event.signal();
				break;
			}
			visit++;
			start = start->getNext();
		}

		// ���������Ϣ
		for (uint i = 0; i < m_checkWaiterNum; i++)
			m_waiterGraph[i]->m_visit = 0;
		m_checkWaiterNum = 0;

		UNLOCK(&m_waiterMutex);
	}

	/**
	 * �õ�ָ����ֵ������2���������ݵ�����
	 * @param v	ָ������ֵ
	 * @return ���2����������
	 */
	inline uint flp2(uint v) {
		uint y = 0x80000000;  
		while (y > v)   
			y = y >> 1; 
		return y; 
	}

private:
	uint			m_maxTxns;				/** ����֧�ֵ���������� */

	// �����Լ�Hash����ر���
	KH				m_ikeyHash;				/** ��ϣ�������� */
	KE				m_ikeyEqualer;			/** �ȽϺ������� */

	LockEntry		*m_lockEntries;			/** �����ϣ��������� */
	u32				m_entryCount;			/** ����������� */
	u32				m_entryMask;			/** ���Hash���� */

	// ����ʹ�õ�LockList��Lock��Ϣ
	LockList		*m_lockList;			/** ����ʹ�õ�����LockList */
	Lock			*m_lockObject;			/** ���������еĿ��������б� */
	Mutex			m_freeLockMutex;		/** ����ȫ�ֿ���Lock��������Ļ����� */
	Lock			*m_freeLocks;			/** ȫ�ֿ��������� */

	// ���������ر���
	Task			*m_deadLockChecker;		/** ��������߳� */
	Mutex			m_waiterMutex;			/** �ȴ���Ϣ���л����� */
	u32				m_waiters;				/** �ȴ��߳���Ŀ */
	LockList		**m_waiterGraph;		/** �ȴ�ͼ�����ڼ������ */
	u32				m_checkWaiterNum;		/** �Ѿ������ĵȴ��߸��� */
	DList<LockList*> m_waitQueue;			/** ȫ�ֵȴ����� */

	// ͳ����Ϣ
	IndiceLockTableStatus m_status;			/** �����ʹ��ͳ����Ϣ�������־û� */
};

/** �������� */
class IndicesLockManager: public IndicesLockTable<u64> {
	typedef IndicesLockTable<u64>  SUPER;
public:
	/**
	 * ����һ����������
	 *  �����������ܷ�maxLocks�����������maxLocks + entryCount*reservedLockPerEntry����
	 * @param maxTxns	���֧�ֵ�������
	 * @param maxLocks ���֧�ֵ���������ע���������ܿ��ǣ��������������ܱ�֤�����󲻻ᳬ��maxLocks����
	 * @param entryCount ����entry��Ŀ
	 * @param reservedLockPerSlot ÿ��entry������Ԥ���䣩����������Ŀ
	 */
	IndicesLockManager(uint maxTxns, uint maxLocks, uint entryCount, uint reservedLockPerEntry)
		: SUPER(maxTxns, maxLocks, entryCount, reservedLockPerEntry) {
	}


	/**
	 * ����һ����������
	 *
	 * @param maxTxns	���֧�ֵ�������
	 * @param maxLocks	���֧�ֵ���������ע���������ܿ��ǣ��������������ܱ�֤�����󲻻ᳬ��maxLocks����
	 */
	IndicesLockManager(uint maxTxns, uint maxLocks)
		: SUPER(maxTxns, maxLocks) {
	}
};

}

#endif
