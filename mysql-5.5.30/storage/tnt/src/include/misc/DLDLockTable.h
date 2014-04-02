/**
 * 带死锁检测的通用锁表
 *
 * 特点： 
 * 1. 支持多种锁模式：IS, IX, S, X
 * 2. 允许锁重入
 * 3. 能预防检测死锁
 * 4. 能一次性释放某个事务持有的所有锁(在NTSE里面，Session相当于一个事务)
 *
 * @author 李伟钊(liweizhao@corp.netease.com)
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

/** 锁模式 */
enum DldLockMode {
	TL_NO = 0,		/** 不需要加锁 */
	TL_IS,			/** IS锁，表示意图读取部分细粒度的数据，只与X锁冲突 */
	TL_IX,			/** IX锁，表示意图修改部分细粒度的数据，与S、SIX、X、U锁冲突 */
	TL_S,			/** S锁，表示可能读取所有数据，与IS、S、U锁兼容 */
	TL_X,           /** X锁 */
	TL_AUTO_INC,    /** 自增长属性锁 */
};

/** 锁对象状态 */
enum DldLockStatus {
	DLS_WAIT,    /** 等待加锁 */
	DLS_GRANTED, /** 持有锁 */
	DLS_FREE,    /** 空闲 */
};

/** 锁操作结果 */
enum DldLockResult {
	DLD_LOCK_SUCCESS,     /** 加锁成功 */
	DLD_LOCK_IMPLICITY,   /** 请求的锁为相容 */
	DLD_LOCK_FAIL,        /** 加锁失败 */
	DLD_LOCK_DEAD_LOCK,   /** 检测到死锁 */
	DLD_LOCK_TIMEOUT,     /** 加锁超时 */
	DLD_LOCK_LACK_OF_MEM, /** 锁表内存空间不足 */
};

/** 死锁检测结果 */
enum DeadLockCheckResult {
	LOCK_DEADLOCK_FREE = 0, /** 不会发生死锁 */
	LOCK_VICTIM_IS_START,   /** 发生死锁，自己被选为牺牲者 */
	LOCK_VICTIM_IS_OTHER,   /** 发生死锁，别人被选为牺牲者 */
	LOCK_EXCEED_MAX_DEPTH,  /** 死锁检测递归深度超过限制 */
};

/** 锁表各项统计信息 */
struct DldLockTableStatus {
	u64	m_locks;			/** 加锁请求次数 */
	u64	m_trylocks;			/** trylock次数 */
	u64	m_deadlocks;		/** 死锁次数 */
	u64	m_unlocks;			/** 解锁请求次数 */
	u64	m_unlockAlls;		/** 一次性放锁请求次数 */
	u64 m_waits;			/** 等待次数 */
	//u64	m_sWaitTime;	/** 等待成功的等待时间（单位毫秒）*/
	//u64	m_fWaitTime;	/** 等待失败的等待时间（单位毫秒）*/
	//u64 m_avgConflictLen;	/** 冲突链表的平均长度 */
	//u64 m_maxConflictLen;	/** 冲突链表的最大长度 */
	//u64 m_inuseEntries;	/** 当前真正使用到的Entry数 */
	//u32	m_activeTxn;	/** 活跃事务数 */
	u32	m_activeLocks;		/** 对象锁个数 */
};

class DldLockTable;
class DldLockOwner;

typedef int (*DldLockOwnerCmpFunc)(DldLockOwner *first, DldLockOwner *second);

/** 锁结构 */
class DldLock {
public:
	DldLock(u64 id, DldLockMode lockMode, DldLockOwner *lockOwner) : m_id(id), 
		m_lockOwner(lockOwner), m_lockMode(lockMode), m_status(DLS_FREE) {
			m_holderLockLink.set(this);
			m_entryLink.set(this);
	}
	~DldLock() { /** don't add destroy operation here!! */ }

	/**
	 * 获得锁ID
	 * @return 
	 */
	inline u64 getId() const {
		return m_id;
	}

	/**
	 * 设置锁ID
	 * @param id 
	 */
	inline void setId(u64 id) {
		m_id = id;
	}

	/**
	 * 获得锁状态
	 * @return 
	 */
	inline DldLockStatus getStatus() const {
		return m_status;
	}

	/**
	 * 设置锁状态
	 * @param status
	 */
	inline void setStatus(DldLockStatus status) {
		m_status = status;
	}

	/**
	 * 获得锁持有者
	 * @return 
	 */
	inline DldLockOwner* getLockOwner() const {
		return m_lockOwner;
	}

	/**
	 * 获得锁模式
	 * @return 
	 */
	inline DldLockMode getLockMode() const {
		return m_lockMode;
	}

	/**
	 * 判断是否处于等待状态
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
	u64					  m_id;		   /** 可锁对象id */
	DldLockOwner		  *m_lockOwner;/** 持有该锁的事务 */
	DldLockMode           m_lockMode;  /** 加锁模式 */
	DldLockStatus         m_status;    /** 锁状态 */
	DLink<DldLock*>       m_holderLockLink;/** 用于链接属于同一持有者的锁 */
	DLink<DldLock*>		  m_entryLink;	   /** 属于同一Entry的可锁对象链表 */

	friend class DldLockEntry;
	friend class DldLockTable;
	friend class DldLockOwner;
};

/** 锁持有者 */
class DldLockOwner {
public:
	static const uint DFL_MEMCTX_PAGE_SIZE = 1024;

public:
	DldLockOwner(u64 txnId, MemoryContext *mtx = NULL);
	virtual ~DldLockOwner();

	bool hasToWaitLock(DldLockMode lockMode, DldLock *otherLock);

	/**
	 * 创建锁对象并加入持有锁链表
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
	 * 获得当前正在等待的锁
	 * @return 
	 */
	inline DldLock* getWaitLock() const {
		return m_waitLock;
	}

	/**
	 * 设置当前正在等待的锁
	 * @param waitLock
	 * @return 
	 */
	inline void setWaitLock(DldLock *waitLock) {
		m_waitLock = waitLock;
	}

	/**
	 * 释放内存资源
	 */
	void releaseResourceIfNeeded() {
		if (m_memctxAllocByMe && NULL != m_memctx) {
			m_memctx->reset();
		}
	}

	/**
	 * 获得事务持有的锁列表
	 * @return 
	 */
	inline DList<DldLock*>* getHoldingList() { 
		return  &m_lockHoldingList;
	}
	
	/**
	 * 标记是否被选为死锁牺牲者
	 * @param chosen
	 */
	inline void markDeadlockVictim(bool chosen) { 
		m_chosenAsDeadlockVictim = chosen;
	}

	/**
	 * 是否被选为死锁牺牲者
	 * @return 
	 */
	inline bool isChosenAsDeadlockVictim() const {
		return m_chosenAsDeadlockVictim;
	}

	/**
	 * 处理当前锁持有者被某个可锁对象放锁唤醒的情形
	 * @param lock	放锁的可锁对象
	 */
	inline void wakeUp() {
		m_event.signal();
	}

	/**
	 * 标记是否锁超时
	 * @param waitTimeout
	 */
	inline void setWaitTimeout(bool waitTimeout) {
		m_waitTimeout = waitTimeout;
	}

	/**
	 * 判断是否超时
	 * @return 
	 */
	inline bool isWaitTimeout() const {
		return m_waitTimeout;
	}

	/**
	 * 将自己链接到全局等待者链表
	 * @param waiterList
	 */
	inline void linkWithWaiterList(DList<DldLockOwner *> *waiterList) {
		waiterList->addLast(&m_waiterLink);
	}

	/**
	 * 将自己从全局等待者链表中移除
	 */
	inline void unlinkFromWaiterList() {
		m_waiterLink.unLink();
	}

	DldLockResult wait(int timeoutMs);

	void releaseLockIfLast(u64 key);
#ifndef TNT_ENGINE
protected:
#endif
	Event			m_event;	      /** 锁持有者相互等待事件 */
	u64	            m_txnId;		  /** 锁持有者id */
	u64             m_sp;             /** 保存最后一个lock的savepoint */
	DList<DldLock*> m_lockHoldingList;/** 事务持有的锁列表 */
	MemoryContext   *m_memctx;        /** 用于分配锁结构的内存分配上下文 */
	DldLock         *m_waitLock;      /** 正在等待的锁，如果没有则为NULL */
	DLink<DldLockOwner*> m_waiterLink;/** 用于链接锁表中正在等待锁的锁请求者 */
	u8              m_memctxAllocByMe:1;/** 内存分配上下文是否是由我构造 */
	u8              m_waitTimeout:1;    /** 锁等待是否超时 */
	u8              m_chosenAsDeadlockVictim:1;/** 是否被选为死锁牺牲者 */
	u8				m_deadLockCheckedMark:1;   /** 死锁检测当中标志该事务已经被检查过不卷入死锁的信息 */
	u8              m_reservedData:4;

	friend class DldLockTable;
};

/** 锁表一个Hash入口结构 */
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
	 * 获得哈希链表头
	 * @return 
	 */
	inline DLink<DldLock*> *getHeader() {
		return m_hashList.getHeader();
	}

	inline uint getQueueSize() const {
		return m_hashList.getSize();
	}

protected:
	DList<DldLock*>  m_hashList;        /** 链接属于同一哈希入口的锁对象 */
};

/** 
 * 带死锁检测的锁表
 *
 * @param K 锁对象键类型
 * @param KH 给锁对象键计算哈希值的函数对象，默认为Hasher<K>
 * @param KE 比较两个键是否相等的函数对象，默认为Equaler<K, K>
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
	 * 获得全局锁请求等待链表
	 * @return 
	 */
	inline const DList<DldLockOwner*>* getWaiterList() {
		return &m_lockWaiterList;
	}

	/**
	 * 设置加锁者权重比较函数
	 * @param func
	 * @return 
	 */
	inline void setCompareFunc(DldLockOwnerCmpFunc func) {
		m_compareFunc = func;
	}

	/**
	 * 设置事务锁超时时间
	 * @param lockTimeoutMs 事务锁超时时间，单位毫秒
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
	 * 根据key获取entry
	 * @param key 锁对象关键字
	 * @return key所在的entry对象
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

protected:
	// 锁表以及Hash表相关变量
	Mutex           m_lockTableMutex;       /** 保护锁表全局信息的互斥量 */
	Hasher<u64>		m_ikeyHash;				    /** 哈希函数对象 */
	Equaler<u64, u64> m_ikeyEqualer;		/** 比较函数对象 */

	DldLockEntry	*m_lockEntries;			/** 锁表哈希各个入口项 */
	u32				m_entryCount;			/** 锁表入口项数 */
	u32				m_entryMask;			/** 入口Hash掩码 */
	int             m_lockTimeoutMs;        /** 锁等待超时时间，单位毫秒 */
	uint            m_maxLocks;             /** 最多支持的锁个数 */

	// 死锁检测相关变量
	Task			*m_deadLockChecker;		/** 死锁检测线程 */
	u32				m_checkWaiterNum;		/** 已经检测过的等待者个数 */

	DList<DldLockOwner*> m_lockWaiterList;  /** 正在等待锁的请求者链表 */
	DldLockOwnerCmpFunc m_compareFunc;      /** 权重比较函数 */

	// 统计信息
	DldLockTableStatus m_status;		/** 锁表的使用统计信息，不做持久化 */
	static const u16 ENTRY_TRAVERSECNT_MAX = 2;
};

}

#endif