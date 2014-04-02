/**
 * 锁管理器
 *
 * @author 余利华(ylh@163.org)
 */

#ifndef _NTSE_LOCK_MANAGER_H_
#define _NTSE_LOCK_MANAGER_H_

#include <iostream>
#include "Global.h"
#include "util/Hash.h"
#include "util/Sync.h"
#include "util/System.h"

using namespace std;

namespace ntse {

class Session;

/** 使用统计信息 */
struct LockTableStatus {
	u64			m_rlockCnt;			/** 加读锁操作次数 */
	u64			m_wlockCnt;			/** 加写锁操作次数 */
	u64			m_spinCnt;			/** 不能马上加上锁，进行自旋的次数 */
	u64			m_waitCnt;			/** 自旋后仍不能加上锁，进行等待的次数 */
	u64			m_waitTime;			/** 等待时间，单位毫秒 */
	double		m_avgConflictLen;	/** 冲突链表平均长度 */
	size_t 		m_maxConflictLen;	/** 冲突链表最大长度 */
	size_t 		m_activeReaders;	/** 活跃的读锁个数 */
	size_t 		m_activeWriters;	/** 活跃的写锁个数 */

	LockTableStatus() {
		memset(this, 0, sizeof(LockTableStatus));
	}
};

/** 锁表
 * @param K 锁对象键类型
 * @param KH 给锁对象键计算哈希值的函数对象，默认为Hasher<K>
 * @param KE 比较两个键是否相等的函数对象，默认为Equaler<K, K>
 */
template <typename K, typename KH = Hasher<K>, typename KE = Equaler<K, K> >
class LockTable {
	/** 锁对象*/
	struct LockObject {
		LockObject			*next;			/** 同一锁表槽的下一个锁对象				*/
		K					key;			/** 对象关键字								*/
		u16					readerCount;	/** 加读锁线程数，冲突时包括试图加锁的线程	*/
		u16					writerCount;	/** 加写锁线程数，冲突时包括试图加锁的线程	*/
		LockMode			curMode;		/** 锁对象当前状态							*/
		Atomic<int>			waiting;		/** 正在等待加锁的线程数					*/
		Event				event;			/** 用于唤醒等待线程的事件					*/
		u16					xHolder;		/** 写锁持有者								*/

		inline void init() {
			readerCount = 0;
			writerCount = 0;
			curMode = None;
			xHolder = 0;
		}
	};


	/** 锁表槽 */
	struct LockTableSlot {
		Mutex		mutex;				/** 短期内保护槽内数据结构 */
		LockObject	*locks;				/** 锁对象链表, 链表包含内嵌锁对象embededLock */
		LockObject	*freeObject;		/** 本slot空闲锁对象链表 */
		uint		freeObjectCount;	/** 本slot空闲对象个数 */
		bool		embededValid;		/** 内嵌锁对象是否合法 */
		LockObject	embededLockObject;  /** 内嵌锁对象 */
		uint		validObjectCount;	/** 槽内使用中的锁对象个数 */
		uint		readers;			/** 槽内加的读锁个数 */
		uint		writers;			/** 槽内加的写锁个数 */

		LockTableSlot(): mutex("LockTableSlot::mutex", __FILE__, __LINE__) {
		}
	};


public:
	/**
	 * 创建一个锁管理器
	 *  该锁表至少能放maxLocks把锁，至多放maxLocks + slotCount * reservedLockPerSlot把锁
	 *
	 * @param maxLocks 最大支持的锁个数。注意由于性能考虑，锁管理器并不能保证锁对象不会超过maxLocks个。
	 * @param slotCount 锁表slot数目, slotCount必须是2的整数次幂
	 * @param reservedLockPerSlot 每个slot保留（预分配）的锁对象数目
	 */
	LockTable(uint maxLocks, uint slotCount, uint reservedLockPerSlot):
		m_freeObjectsMutex("LockTable::freeObjectsMutex", __FILE__, __LINE__) {
			assert(maxLocks);
			assert(slotCount);
			init(maxLocks, slotCount, reservedLockPerSlot);
	  }


	/**
	 * 创建一个锁管理器
	 *
	 * @param maxLocks 最大支持的锁个数。注意由于性能考虑，锁管理器并不能保证锁对象不会超过maxLocks个。
	 */
	LockTable(uint maxLocks): m_freeObjectsMutex("LockTable::freeObjectsMutex", __FILE__, __LINE__) {
		assert(maxLocks);
		uint slotCount = flp2(maxLocks);	// 下去整到2的指数
		init(maxLocks, slotCount, 1);
	}


	~LockTable() {
		delete[] m_table;
		delete[] m_objects;
	}


	/**
	 * 尝试对指定对象加锁
	 *
	 * @param threadId 加锁的线程ID
	 * @param key 键值
	 * @param lockMode 锁模式
	 * @return 是否加锁成功。加锁不成功有可能是由于对象已经被锁定，也有可能是由于锁表已满
	 */
	bool tryLock(u16 threadId, K key, LockMode lockMode) {
		assert( lockMode == Shared || lockMode == Exclusived );

		lockMode == Shared ? ++m_usage.m_rlockCnt : ++m_usage.m_wlockCnt;

		LockTableSlot *slot = getSlot(key);
		LOCK(&slot->mutex);

		// 找到或者创建锁对象
		LockObject *lockObject = findOrAlloc(slot, key, false);
		if (!lockObject) {
			UNLOCK(&slot->mutex);
			return false;
		}

		if (lockObject->readerCount + lockObject->writerCount == 0) {// 第一个加锁者设置锁模式
			lockObject->curMode = lockMode;
			goto LOCK_SUCC;
		}

		if (lockMode == Shared && lockObject->curMode == Shared) // 没有锁冲突
			goto LOCK_SUCC;

		UNLOCK(&slot->mutex);
		return false;

LOCK_SUCC:
		if (lockMode == Shared) {
			++lockObject->readerCount;
			++slot->readers;
		} else {
			++lockObject->writerCount;
			++slot->writers;
			lockObject->xHolder = threadId;
		}
		UNLOCK(&slot->mutex);
		return true;
	}


	/**
	 * 对指定对象加锁
	 *
	 * @param threadId 加锁的线程ID
	 * @param key 键值
	 * @param lockMode 锁模式
	 * @param timeoutMs 等锁超时时间，单位毫秒，若为0或负数则不超时
	 * @return 是否加锁成功
	 * @throw NtseException 锁表满了
	 */
	bool lock(u16 threadId, K key, LockMode lockMode, int timeoutMs = -1) throw(NtseException) {
		assert( lockMode == Shared || lockMode == Exclusived );

		LockTableSlot *slot = getSlot(key);
		LOCK(&slot->mutex);

		// 找到或者创建锁对象
		LockObject *lockObject;
		try {
			lockObject = findOrAlloc(slot, key);
		} catch (NtseException &e) {
			UNLOCK(&slot->mutex);
			throw e;
		}

		if (lockObject->readerCount + lockObject->writerCount == 0) { // 第一个加锁者设置锁模式
			lockObject->curMode = lockMode;
			if (lockMode == Shared) {
				++lockObject->readerCount;
				++slot->readers;
				++m_usage.m_rlockCnt;
			} else {
				++lockObject->writerCount;
				++slot->writers;
				++m_usage.m_wlockCnt;
				lockObject->xHolder = threadId;
			}
			UNLOCK(&slot->mutex);
			return true;
		}

		if (lockMode == Shared) {
			++lockObject->readerCount;
			++slot->readers;
			++m_usage.m_rlockCnt;
			if (lockObject->curMode == Shared) {    // Shared 和 Shared 不冲突
				UNLOCK(&slot->mutex);
				return true;
			}
		} else {
			++lockObject->writerCount;
			++slot->writers;
			++m_usage.m_wlockCnt;
		}

		// 已经更新readerCount, writerCount
		// 此时释放slot->mutex不会导致锁对象删除
		UNLOCK(&slot->mutex);

		bool succ = lockConflict(slot, lockObject, lockMode, timeoutMs);
		if (succ && lockMode == Exclusived)
			lockObject->xHolder = threadId;
		return succ;
	}


	/**
	 * 解锁
	 *
	 * @param key 对象键值
	 * @param lockMode 锁模式
	 */
	void unlock(K key, LockMode lockMode) {
		UNREFERENCED_PARAMETER(lockMode);
		assert( lockMode == Shared || lockMode == Exclusived );

		LockTableSlot *slot = getSlot(key);
		LOCK(&slot->mutex);
		LockObject *lockObject = findLockObject(slot, key);

		assert( lockObject );
		assert( lockObject->curMode == lockMode );

		if (lockObject->curMode == Shared) {
			assert( lockObject->readerCount > 0);
			--lockObject->readerCount;
			--slot->readers;
			if (!lockObject->readerCount) {
				lockObject->curMode = None;
				if (lockObject->waiting.get() > 0)
					lockObject->event.signal();
			}
		} else {
			assert( lockObject->writerCount > 0);
			--lockObject->writerCount;
			--slot->writers;
			lockObject->curMode = None;
			lockObject->xHolder = 0;
			if (lockObject->waiting.get() > 0)
				lockObject->event.signal();
		}

		if (lockObject->writerCount + lockObject->readerCount == 0) { // 再也不存在加锁者，此时释放锁对象
			freeLockObject(slot, lockObject);
		}

		UNLOCK(&slot->mutex);
	}


	/**
	 * 返回指定的键值是否被指定线程加了互斥锁
	 *
	 * @param threadId 线程ID
	 * @param key 键值
	 * @return 键值key是否被会话session以Exclusived模式锁定
	 */
	bool isExclusivedLocked(u16 threadId, K key) {
		LockTableSlot *slot = getSlot(key);
		LOCK(&slot->mutex);
		LockObject *lockObject = findLockObject(slot, key);
		if (!lockObject || lockObject->curMode != Exclusived || lockObject->xHolder != threadId) {
			UNLOCK(&slot->mutex);
			return false;
		}
		UNLOCK(&slot->mutex);
		return true;
	}

	/**
	 * 返回指定的键值是否被加了共享锁
	 *
	 * @param key 键值
	 * @return 键值key是否被用Shared模式锁定
	 */
	bool isSharedLocked(K key) {
		LockTableSlot *slot = getSlot(key);
		LOCK(&slot->mutex);
		LockObject *lockObject = findLockObject(slot, key);
		if (!lockObject || lockObject->curMode != Shared) {
			UNLOCK(&slot->mutex);
			return false;
		}
		UNLOCK(&slot->mutex);
		return true;
	}

	/**
	 * 获得使用情况统计
	 *
	 * @return 使用情况统计
	 */
	const LockTableStatus& getStatus() {
		size_t activeReaders = 0;
		size_t activeWriters = 0;
		size_t totalLockObject = 0;
		size_t maxLockLen = 0;
		uint validSlotCount = 0;
		for (uint i = 0; i < m_slotCount; i++) {
			activeReaders += m_table[i].readers;
			activeWriters += m_table[i].writers;
			uint len = m_table[i].validObjectCount;
			if (!len)
				continue;
			validSlotCount++;
			totalLockObject += len;
			if (len > maxLockLen)
				maxLockLen = len;
		}
		if (!validSlotCount)
			m_usage.m_avgConflictLen = 0;
		else
			m_usage.m_avgConflictLen = totalLockObject / validSlotCount;
		m_usage.m_maxConflictLen = maxLockLen;
		m_usage.m_activeReaders = activeReaders;
		m_usage.m_activeWriters = activeWriters;
		return m_usage;
	}

	/** 打印使用情况统计
	 * @param out 输出流
	 */
	void printStatus(ostream &out) {
		getStatus();
		out << "== lock table status ======================================================" << endl;
		out << "rlocks: " << m_usage.m_rlockCnt << endl;
		out << "wlocks: " << m_usage.m_wlockCnt << endl;
		out << "spins: " << m_usage.m_spinCnt << endl;
		out << "waits: " << m_usage.m_waitCnt << endl;
		out << "wait_time: " << m_usage.m_waitTime << endl;
		out << "avg_conflict_len: " << m_usage.m_avgConflictLen << endl;
		out << "max_conflict_len: " << m_usage.m_maxConflictLen << endl;
		out << "active_readers: " << m_usage.m_activeReaders << endl;
		out << "active_writers: " << m_usage.m_activeWriters << endl;
	}
	
private:
	/**
	 * 锁管理器初始化
	 * @param maxLocks 最大支持的锁个数
	 * @param slotCount 锁表slot数目，一定是2的整数次幂
	 * @param reservedLockPerSlot 每个slot保留（预分配）的锁对象数目
	 */
	bool init(uint maxLocks, uint slotCount, uint reservedLockPerSlot) {
		assert(maxLocks > 0 && slotCount >0);

		m_reservedLockPerSlot = reservedLockPerSlot;
		m_slotCount = slotCount;
		m_slotMask = (m_slotCount - 1);
		assert((m_slotCount & m_slotMask) == 0);

		m_table = new LockTableSlot[slotCount];
		m_objects = new LockObject[maxLocks + m_reservedLockPerSlot * slotCount];

		for (uint slotIdx = 0; slotIdx < slotCount; ++slotIdx) {
			LockTableSlot* slot = &m_table[slotIdx];
			slot->locks = 0;
			slot->embededValid = false;
			slot->freeObjectCount = m_reservedLockPerSlot;
			if (m_reservedLockPerSlot) {
				// 构造freeObject链表，链表包含m_reservedLockPerSlot个对象
				uint objectIdx = slotIdx * m_reservedLockPerSlot;
				slot->freeObject = &m_objects[objectIdx];
				for (uint i = 0; i < m_reservedLockPerSlot - 1; ++i, ++objectIdx) {
					m_objects[objectIdx].init();
					m_objects[objectIdx].next = &m_objects[objectIdx + 1];
				}
				m_objects[objectIdx].init();
				m_objects[objectIdx].next = 0;
			} else {
				slot->freeObject = 0;
			}
			slot->embededLockObject.init();
			slot->readers = slot->writers = 0;
			slot->validObjectCount = 0;
		}

		// 初始化全局freeObject链
		uint objectIdx = slotCount * m_reservedLockPerSlot;
		m_freeObjects = &m_objects[objectIdx];
		for (uint i = 0; i < maxLocks - 1; ++i, ++objectIdx) {
			m_objects[objectIdx].init();
			m_objects[objectIdx].next = &m_objects[objectIdx+1];
		}
		m_objects[objectIdx].init();
		m_objects[objectIdx].next = 0;

		return true;
	}

	/**
	 * 根据key获取slot
	 * @param key 锁对象关键字
	 */
	inline LockTableSlot* getSlot(K key) {
		unsigned int hashCode = m_keyHash(key);
		return &m_table[hashCode & m_slotMask];
	}


	/**
	 * 查找或者创建锁对象
	 *
	 * @pre	slot->mutex被锁定
	 * @param slot 锁对象所在的slot
	 * @param key 锁对象关键字
	 * @param throwException 锁表满的时候，是否需要抛出异常
	 * @return key对应的锁对象，返回0表示失败
	 */
	inline LockObject* findOrAlloc(LockTableSlot *slot, K key, bool throwException = true) {
		LockObject *lockObject = findLockObject(slot, key);
		if (!lockObject) {
			lockObject = allocLockObject(slot);
			if (!lockObject) {
				if (throwException)
					NTSE_THROW(NTSE_EC_TOO_MANY_ROWLOCK, "too many row locks in lockmanager.");
			} else {
				lockObject->key = key;
			}
		} else {
			lockObject->key = key;
		}

		return lockObject;
	}


	/**
	 * 创建锁对象
	 * 先从slot预留空闲对象中分配，分配不成功再从全局空闲对象链表中分配
	 * @pre	slot->mutex被锁定
	 * @param slot 锁表槽
	 * @return 分配的锁对象，若锁表已满返回NULL
	 */
	inline LockObject* allocLockObject(LockTableSlot *slot) {
		if (!slot->embededValid) {
			slot->embededValid = true;
			++slot->validObjectCount;
			return &slot->embededLockObject;
		}

		LockObject *object = 0;
		if (slot->freeObject) {
			assert(slot->freeObjectCount > 0);
			--slot->freeObjectCount;
			object = slot->freeObject;
			slot->freeObject = object->next;

		} else if (m_freeObjects) {
			LOCK(&m_freeObjectsMutex);
			object = m_freeObjects;
			if (object)
				m_freeObjects = object->next;
			UNLOCK(&m_freeObjectsMutex);
		}

		if (object) {
			object->init();
			object->next = slot->locks;
			slot->locks = object;
			++slot->validObjectCount;
		}

		return object;
	}


	/**
	 * 查找锁对象
	 * @pre	slot->mutex被锁定
	 * @param slot 锁表槽
	 * @param key 锁对象关键字
	 * @return key对应的锁对象，返回NULL表示找不到
	 */
	inline LockObject* findLockObject(LockTableSlot *slot, K key) {
		if (slot->embededValid && m_keyEqualer(slot->embededLockObject.key, key)) {
			return &slot->embededLockObject;
		}

		LockObject *cur = slot->locks;
		while (cur) {
			if (m_keyEqualer(key, cur->key))
				return cur;
			cur = cur->next;
		}
		return 0;
	}

	/**
	 * 从链表中删除锁对象
	 * @pre	对应的锁表槽已经被锁定
	 * @param objectList 锁对象链表
	 * @param lockObject 要删除的锁对象
	 * @return 要删除的锁对象在指定链表中返回true，否则返回false
	 */
	inline bool removeLockObject(LockObject **objectList, LockObject *lockObject) {
		if ((*objectList) == lockObject) {
			*objectList = (*objectList)->next;
			return true;
		}
		LockObject *prev = (*objectList);
		LockObject *cur = prev->next;

		while (cur) {
			if (lockObject == cur) {
				prev->next = cur->next;
				return true;
			}
			prev = cur;
			cur = cur->next;
		}

		return false;
	}


	/**
	 * 释放锁对象
	 * @pre	slot->mutex被锁定
	 * @param slot 锁对象所在的slot
	 * @param lockObject 待释放的锁对象
	 */
	inline void freeLockObject(LockTableSlot *slot, LockObject *lockObject) {
		if (lockObject == &slot->embededLockObject) {
			slot->embededValid = false;
			--slot->validObjectCount;
			return;
		}
		removeLockObject(&slot->locks, lockObject);
		if (slot->freeObjectCount < m_reservedLockPerSlot) {
			lockObject->next = slot->freeObject;
			slot->freeObject = lockObject;
			++slot->freeObjectCount;
		} else { // 预留对象链已满
			LOCK(&m_freeObjectsMutex);
			lockObject->next = m_freeObjects;
			m_freeObjects = lockObject;
			UNLOCK(&m_freeObjectsMutex);
		}
		--slot->validObjectCount;
	}

	/**
	 * 锁冲突之后，尝试加锁
	 */
	inline bool tryLockOnConflict(LockTableSlot *slot, LockObject *lockObject, LockMode lockMode) {
		if (lockMode == Shared) {
			if (lockObject->curMode == Shared || lockObject->curMode == None) {
				LOCK(&slot->mutex);
				if (lockObject->curMode == Shared || lockObject->curMode == None) {
					lockObject->curMode = Shared;
					UNLOCK(&slot->mutex);
					return true;
				} else {
					UNLOCK(&slot->mutex);
				}
			}
		} else {
			if (lockObject->curMode == None) {
				LOCK(&slot->mutex);
				if (lockObject->curMode == None) {
					lockObject->curMode = Exclusived;
					UNLOCK(&slot->mutex);
					return true;
				}
				UNLOCK(&slot->mutex);
			}

		}

		return false;
	}

	/**
	 * 锁对象加锁冲突处理
	 *
	 * @pre slot->mutex 已经被加锁
	 *
	 * @param slot 包含锁对象的Slot
	 * @param lockObject 锁对象
	 * @param lockMode 目标锁模式
	 * @param timeoutMs 等锁超时时间，单位毫秒，若为0或负数则不超时
	 * @return 是否加锁成功
	 */
	inline bool lockConflict(LockTableSlot *slot, LockObject *lockObject, LockMode lockMode, int timeoutMs)	{
		// 忙等
		++m_usage.m_spinCnt;
		if (lockMode == Shared) {
			for (uint i = 0; i < Mutex::SPIN_COUNT; i++)
				if (lockObject->curMode == Shared || lockObject->curMode == None)
					break;
			if (tryLockOnConflict(slot, lockObject, lockMode))
				return true;

		} else {
			for (uint i = 0; i < Mutex::SPIN_COUNT; i++)
				if (lockObject->curMode == None)
					break;
			if (tryLockOnConflict(slot, lockObject, lockMode))
				return true;
		}

		++m_usage.m_waitCnt;
		// 等在event上
		u64 before = System::currentTimeMillis();
		int wait;
		if (timeoutMs > 0) {
			wait = timeoutMs;
		} else
			wait = 1000;
		while (true) {
			lockObject->waiting.increment();
			lockObject->event.wait(wait);
			lockObject->waiting.decrement();

			if (tryLockOnConflict(slot, lockObject, lockMode)) {
				m_usage.m_waitTime += (System::currentTimeMillis() - before);
				return true;
			}

			if (timeoutMs > 0) {    // 超时判断
				u64 now = System::currentTimeMillis();
				m_usage.m_waitTime += now - before;
				if (now - before >= (u64)timeoutMs)
					return false;
				wait = (int)(timeoutMs - (now - before));
			}
		}
	}


	inline uint flp2(uint v) {
		uint y = 0x80000000;
		while (y > v)
			y = y >> 1;
		return y;
	}


private:
	uint			m_slotCount;				/** 锁表Slot数目 */
	uint			m_slotMask;					/** 锁表Slot索引掩码 */
	uint			m_reservedLockPerSlot;		/** 每个Slot中保留的(预分配的)锁对象数目 */
	LockObject		*m_objects;					/** 锁表中所有的锁对象 */
	Mutex			m_freeObjectsMutex;			/** 空闲锁对象链表锁 */
	LockObject		*m_freeObjects;				/** 空闲锁对象链表 */
	LockTableSlot	*m_table;					/** 锁表，slot数组 */

	KH				m_keyHash;					/** 哈希函数对象 */
	KE				m_keyEqualer;				/** 等值比较函数对象 */

	LockTableStatus	m_usage;					/** 使用情况统计 */
};



/** 锁管理器 */
class LockManager: public LockTable<u64> {
	typedef LockTable<u64>  SUPER;
public:
	/**
	 * 创建一个锁管理器
	 *  该锁表至少能放maxLocks把锁，至多放maxLocks + slotCount*reservedLockPerSlot把锁
	 * @param maxLocks 最大支持的锁个数。注意由于性能考虑，锁管理器并不能保证锁对象不会超过maxLocks个。
	 * @param slotCount 锁表slot数目
	 * @param reservedLockPerSlot 每个slot保留（预分配）的锁对象数目
	 */
	LockManager(uint maxLocks, uint slotCount, uint reservedLockPerSlot)
		: SUPER(maxLocks, slotCount, reservedLockPerSlot) {
	}


	/**
	 * 创建一个锁管理器
	 *
	 * @param maxLocks 最大支持的锁个数。注意由于性能考虑，锁管理器并不能保证锁对象不会超过maxLocks个。
	 */
	LockManager(uint maxLocks)
		: SUPER(maxLocks) {
	}
};


}

#endif
