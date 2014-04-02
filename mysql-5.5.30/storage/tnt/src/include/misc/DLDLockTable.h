/**
 * ����������ͨ������
 *
 * �ص㣺 
 * 1. ֧�ֶ�����ģʽ��IS, IX, S, X
 * 2. ����������
 * 3. ��Ԥ���������
 * 4. ��һ�����ͷ�ĳ��������е�������(��NTSE���棬Session�൱��һ������)
 *
 * @author ��ΰ��(liweizhao@corp.netease.com)
 */
#ifndef _NTSE_DLD_LOCK_TABLE_H_
#define _NTSE_DLD_LOCK_TABLE_H_

#include "Global.h"
#include "util/DList.h"
#include "util/Hash.h"
#include "util/Sync.h"
#include "util/System.h"
#include "util/Thread.h"
#include "misc/MemCtx.h"

using namespace ntse;
namespace tnt {

/** ��ģʽ */
enum DldLockMode {
	TL_NO = 0,		/** ����Ҫ���� */
	TL_IS,			/** IS������ʾ��ͼ��ȡ����ϸ���ȵ����ݣ�ֻ��X����ͻ */
	TL_IX,			/** IX������ʾ��ͼ�޸Ĳ���ϸ���ȵ����ݣ���S��SIX��X��U����ͻ */
	TL_S,			/** S������ʾ���ܶ�ȡ�������ݣ���IS��S��U������ */
	TL_X,           /** X�� */
	TL_AUTO_INC,    /** ������������ */
};

/** ������״̬ */
enum DldLockStatus {
	DLS_WAIT,    /** �ȴ����� */
	DLS_GRANTED, /** ������ */
	DLS_FREE,    /** ���� */
};

/** ��������� */
enum DldLockResult {
	DLD_LOCK_SUCCESS,     /** �����ɹ� */
	DLD_LOCK_IMPLICITY,   /** �������Ϊ���� */
	DLD_LOCK_FAIL,        /** ����ʧ�� */
	DLD_LOCK_DEAD_LOCK,   /** ��⵽���� */
	DLD_LOCK_TIMEOUT,     /** ������ʱ */
	DLD_LOCK_LACK_OF_MEM, /** �����ڴ�ռ䲻�� */
};

/** ��������� */
enum DeadLockCheckResult {
	LOCK_DEADLOCK_FREE = 0, /** ���ᷢ������ */
	LOCK_VICTIM_IS_START,   /** �����������Լ���ѡΪ������ */
	LOCK_VICTIM_IS_OTHER,   /** �������������˱�ѡΪ������ */
	LOCK_EXCEED_MAX_DEPTH,  /** �������ݹ���ȳ������� */
};

/** �������ͳ����Ϣ */
struct DldLockTableStatus {
	u64	m_locks;			/** ����������� */
	u64	m_trylocks;			/** trylock���� */
	u64	m_deadlocks;		/** �������� */
	u64	m_unlocks;			/** ����������� */
	u64	m_unlockAlls;		/** һ���Է���������� */
	u64 m_waits;			/** �ȴ����� */
	//u64	m_sWaitTime;	/** �ȴ��ɹ��ĵȴ�ʱ�䣨��λ���룩*/
	//u64	m_fWaitTime;	/** �ȴ�ʧ�ܵĵȴ�ʱ�䣨��λ���룩*/
	//u64 m_avgConflictLen;	/** ��ͻ�����ƽ������ */
	//u64 m_maxConflictLen;	/** ��ͻ�������󳤶� */
	//u64 m_inuseEntries;	/** ��ǰ����ʹ�õ���Entry�� */
	//u32	m_activeTxn;	/** ��Ծ������ */
	u32	m_activeLocks;		/** ���������� */
};

class DldLockTable;
class DldLockOwner;

typedef int (*DldLockOwnerCmpFunc)(DldLockOwner *first, DldLockOwner *second);

/** ���ṹ */
class DldLock {
public:
	DldLock(u64 id, DldLockMode lockMode, DldLockOwner *lockOwner) : m_id(id), 
		m_lockOwner(lockOwner), m_lockMode(lockMode), m_status(DLS_FREE) {
			m_holderLockLink.set(this);
			m_entryLink.set(this);
	}
	~DldLock() { /** don't add destroy operation here!! */ }

	/**
	 * �����ID
	 * @return 
	 */
	inline u64 getId() const {
		return m_id;
	}

	/**
	 * ������ID
	 * @param id 
	 */
	inline void setId(u64 id) {
		m_id = id;
	}

	/**
	 * �����״̬
	 * @return 
	 */
	inline DldLockStatus getStatus() const {
		return m_status;
	}

	/**
	 * ������״̬
	 * @param status
	 */
	inline void setStatus(DldLockStatus status) {
		m_status = status;
	}

	/**
	 * �����������
	 * @return 
	 */
	inline DldLockOwner* getLockOwner() const {
		return m_lockOwner;
	}

	/**
	 * �����ģʽ
	 * @return 
	 */
	inline DldLockMode getLockMode() const {
		return m_lockMode;
	}

	/**
	 * �ж��Ƿ��ڵȴ�״̬
	 * @return 
	 */
	inline bool isWaiting() const {
		return DLS_WAIT == m_status;
	}

	void lockGrant();
	void cancelWaitingAndRelease(bool needWake);
	void removeFromEntryList();
	static bool lockModeGreaterOrEq(DldLockMode mode1, DldLockMode mode2);
	static bool lockModeCompatible(DldLockMode mode1, DldLockMode mode2);

protected:
	u64					  m_id;		   /** ��������id */
	DldLockOwner		  *m_lockOwner;/** ���и��������� */
	DldLockMode           m_lockMode;  /** ����ģʽ */
	DldLockStatus         m_status;    /** ��״̬ */
	DLink<DldLock*>       m_holderLockLink;/** ������������ͬһ�����ߵ��� */
	DLink<DldLock*>		  m_entryLink;	   /** ����ͬһEntry�Ŀ����������� */

	friend class DldLockEntry;
	friend class DldLockTable;
	friend class DldLockOwner;
};

/** �������� */
class DldLockOwner {
public:
	static const uint DFL_MEMCTX_PAGE_SIZE = 1024;

public:
	DldLockOwner(u64 txnId, MemoryContext *mtx = NULL);
	virtual ~DldLockOwner();

	bool hasToWaitLock(DldLockMode lockMode, DldLock *otherLock);

	/**
	 * ���������󲢼������������
	 * @param key
	 * @param lockMode
	 * @return 
	 */
	DldLock* createLock(u64 key, DldLockMode lockMode) {
		DldLock *lock = NULL;
		m_sp = m_memctx->setSavepoint();
		void *data = m_memctx->alloc(sizeof(DldLock));
		if (data != NULL) {
			lock = new (data)DldLock(key, lockMode, this);
			m_lockHoldingList.addLast(&lock->m_holderLockLink);
		}
		return lock;
	}

	/**
	 * ��õ�ǰ���ڵȴ�����
	 * @return 
	 */
	inline DldLock* getWaitLock() const {
		return m_waitLock;
	}

	/**
	 * ���õ�ǰ���ڵȴ�����
	 * @param waitLock
	 * @return 
	 */
	inline void setWaitLock(DldLock *waitLock) {
		m_waitLock = waitLock;
	}

	/**
	 * �ͷ��ڴ���Դ
	 */
	void releaseResourceIfNeeded() {
		if (m_memctxAllocByMe && NULL != m_memctx) {
			m_memctx->reset();
		}
	}

	/**
	 * ���������е����б�
	 * @return 
	 */
	inline DList<DldLock*>* getHoldingList() { 
		return  &m_lockHoldingList;
	}
	
	/**
	 * ����Ƿ�ѡΪ����������
	 * @param chosen
	 */
	inline void markDeadlockVictim(bool chosen) { 
		m_chosenAsDeadlockVictim = chosen;
	}

	/**
	 * �Ƿ�ѡΪ����������
	 * @return 
	 */
	inline bool isChosenAsDeadlockVictim() const {
		return m_chosenAsDeadlockVictim;
	}

	/**
	 * ����ǰ�������߱�ĳ����������������ѵ�����
	 * @param lock	�����Ŀ�������
	 */
	inline void wakeUp() {
		m_event.signal();
	}

	/**
	 * ����Ƿ�����ʱ
	 * @param waitTimeout
	 */
	inline void setWaitTimeout(bool waitTimeout) {
		m_waitTimeout = waitTimeout;
	}

	/**
	 * �ж��Ƿ�ʱ
	 * @return 
	 */
	inline bool isWaitTimeout() const {
		return m_waitTimeout;
	}

	/**
	 * ���Լ����ӵ�ȫ�ֵȴ�������
	 * @param waiterList
	 */
	inline void linkWithWaiterList(DList<DldLockOwner *> *waiterList) {
		waiterList->addLast(&m_waiterLink);
	}

	/**
	 * ���Լ���ȫ�ֵȴ����������Ƴ�
	 */
	inline void unlinkFromWaiterList() {
		m_waiterLink.unLink();
	}

	DldLockResult wait(int timeoutMs);

	void releaseLockIfLast(u64 key);
#ifndef TNT_ENGINE
protected:
#endif
	Event			m_event;	      /** ���������໥�ȴ��¼� */
	u64	            m_txnId;		  /** ��������id */
	u64             m_sp;             /** �������һ��lock��savepoint */
	DList<DldLock*> m_lockHoldingList;/** ������е����б� */
	MemoryContext   *m_memctx;        /** ���ڷ������ṹ���ڴ���������� */
	DldLock         *m_waitLock;      /** ���ڵȴ����������û����ΪNULL */
	DLink<DldLockOwner*> m_waiterLink;/** �����������������ڵȴ������������� */
	u8              m_memctxAllocByMe:1;/** �ڴ�����������Ƿ������ҹ��� */
	u8              m_waitTimeout:1;    /** ���ȴ��Ƿ�ʱ */
	u8              m_chosenAsDeadlockVictim:1;/** �Ƿ�ѡΪ���������� */
	u8				m_deadLockCheckedMark:1;   /** ������⵱�б�־�������Ѿ���������������������Ϣ */
	u8              m_reservedData:4;

	friend class DldLockTable;
};

/** ����һ��Hash��ڽṹ */
class DldLockEntry {
public:
	struct Iterator {
		Iterator(DldLockEntry *entry) : m_entry(entry) {
			m_listHeader = entry->m_hashList.getHeader();
			m_current = m_listHeader->getNext();
		}

		inline DldLock* next() {
			assert(m_current != m_listHeader);
			DldLock *rtn = m_current->get();
			m_current = m_current->getNext();
			return rtn;
		}

		inline bool hasNext() {
			return m_current != m_listHeader;
		}

		DldLockEntry    *m_entry;
		DLink<DldLock*> *m_listHeader;
		DLink<DldLock*> *m_current;
	};

public:
	DldLockEntry() {}
	~DldLockEntry() {}

	bool stillHasToWaitInQueue(DldLock *lock);
	DldLock* hasLockImplicity(DldLockOwner *lockOwner, u64 key, DldLockMode lockMode);
	DldLock* otherHasConflicting(DldLockOwner *lockOwner, u64 key, DldLockMode lockMode);
	DldLock* findLockObject(DldLockOwner *lockOwner, u64 key, Equaler<u64, u64> &equaler, 
		DldLockMode lockMode);
	void addToQueue(DldLock *lock);
	void grantWaitingLocks(DldLock *lock);
	DldLock* pickLock(u64 key);

	/**
	 * ��ù�ϣ����ͷ
	 * @return 
	 */
	inline DLink<DldLock*> *getHeader() {
		return m_hashList.getHeader();
	}

	inline uint getQueueSize() const {
		return m_hashList.getSize();
	}

protected:
	DList<DldLock*>  m_hashList;        /** ��������ͬһ��ϣ��ڵ������� */
};

/** 
 * ��������������
 *
 * @param K �����������
 * @param KH ��������������ϣֵ�ĺ�������Ĭ��ΪHasher<K>
 * @param KE �Ƚ��������Ƿ���ȵĺ�������Ĭ��ΪEqualer<K, K>
 */
class DldLockTable {
public:
	static const uint LOCK_MAX_DEPTH_IN_DEADLOCK_CHECK = 1024;
	static const uint LOCK_MAX_N_STEPS_IN_DEADLOCK_CHECK = 1024;

public:
	DldLockTable(uint maxLocks, uint entryCount, int lockTimeoutMs = -1);
	DldLockTable(uint maxLocks, int lockTimeoutMs = -1);
	~DldLockTable();

	bool pickLock(u64 key, bool bePrecise = false);
	DldLockResult tryLock(DldLockOwner *lockOwner, u64 key, DldLockMode lockMode);
	DldLockResult lock(DldLockOwner *lockOwner, u64 key, DldLockMode lockMode);
	bool unlock(DldLockOwner *lockOwner, u64 key, DldLockMode lockMode);
	bool releaseAllLocks(DldLockOwner *lockOwner);
	bool hasLockImplicity(DldLockOwner *lockOwner, u64 key, DldLockMode lockMode);
	bool isLocked(DldLockOwner *lockOwner, u64 key, DldLockMode lockMode);

	/**
	 * ���ȫ��������ȴ�����
	 * @return 
	 */
	inline const DList<DldLockOwner*>* getWaiterList() {
		return &m_lockWaiterList;
	}

	/**
	 * ���ü�����Ȩ�رȽϺ���
	 * @param func
	 * @return 
	 */
	inline void setCompareFunc(DldLockOwnerCmpFunc func) {
		m_compareFunc = func;
	}

	/**
	 * ������������ʱʱ��
	 * @param lockTimeoutMs ��������ʱʱ�䣬��λ����
	 */
	inline void setLockTimeout(int lockTimeoutMs) {
		MutexGuard mutexGuard(&m_lockTableMutex, __FILE__, __LINE__);
		m_lockTimeoutMs = lockTimeoutMs;
	}

protected:
	void init(uint maxLocks, uint entryCount);
	DldLock* createAndAddToQueue(DldLockOwner *lockOwner, u64 key, DldLockMode lockMode, 
		DldLockEntry *entry);
	DldLockResult enqueueWaiting(DldLockOwner *lockOwner, u64 key, DldLockMode lockMode, 
		DldLockEntry *entry);
	void removeFromQueue(DldLock *lock);

	bool checkDeadlockOccurs(DldLockOwner *lockOwner);
	DeadLockCheckResult checkDeadlockRecursive(DldLockOwner *starter, DldLockOwner *waiter, 
		uint *cost, uint depth);
	
	/**
	 * ����key��ȡentry
	 * @param key ������ؼ���
	 * @return key���ڵ�entry����
	 */
	inline DldLockEntry* getEntry(const u64& key) {
		uint hashCode = m_ikeyHash(key);
		return &m_lockEntries[hashCode & m_entryMask];
	}

	inline int compareWeight(DldLockOwner *first, DldLockOwner *second) const {
		return NULL != m_compareFunc ? m_compareFunc(first, second) : 0;
	}

private:
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

protected:
	// �����Լ�Hash����ر���
	Mutex           m_lockTableMutex;       /** ��������ȫ����Ϣ�Ļ����� */
	Hasher<u64>		m_ikeyHash;				    /** ��ϣ�������� */
	Equaler<u64, u64> m_ikeyEqualer;		/** �ȽϺ������� */

	DldLockEntry	*m_lockEntries;			/** �����ϣ��������� */
	u32				m_entryCount;			/** ����������� */
	u32				m_entryMask;			/** ���Hash���� */
	int             m_lockTimeoutMs;        /** ���ȴ���ʱʱ�䣬��λ���� */
	uint            m_maxLocks;             /** ���֧�ֵ������� */

	// ���������ر���
	Task			*m_deadLockChecker;		/** ��������߳� */
	u32				m_checkWaiterNum;		/** �Ѿ������ĵȴ��߸��� */

	DList<DldLockOwner*> m_lockWaiterList;  /** ���ڵȴ��������������� */
	DldLockOwnerCmpFunc m_compareFunc;      /** Ȩ�رȽϺ��� */

	// ͳ����Ϣ
	DldLockTableStatus m_status;		/** �����ʹ��ͳ����Ϣ�������־û� */
	static const u16 ENTRY_TRAVERSECNT_MAX = 2;
};

}

#endif