/**
 * ��������
 *
 * @author ������(ylh@163.org)
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

/** ʹ��ͳ����Ϣ */
struct LockTableStatus {
	u64			m_rlockCnt;			/** �Ӷ����������� */
	u64			m_wlockCnt;			/** ��д���������� */
	u64			m_spinCnt;			/** �������ϼ����������������Ĵ��� */
	u64			m_waitCnt;			/** �������Բ��ܼ����������еȴ��Ĵ��� */
	u64			m_waitTime;			/** �ȴ�ʱ�䣬��λ���� */
	double		m_avgConflictLen;	/** ��ͻ����ƽ������ */
	size_t 		m_maxConflictLen;	/** ��ͻ������󳤶� */
	size_t 		m_activeReaders;	/** ��Ծ�Ķ������� */
	size_t 		m_activeWriters;	/** ��Ծ��д������ */

	LockTableStatus() {
		memset(this, 0, sizeof(LockTableStatus));
	}
};

/** ����
 * @param K �����������
 * @param KH ��������������ϣֵ�ĺ�������Ĭ��ΪHasher<K>
 * @param KE �Ƚ��������Ƿ���ȵĺ�������Ĭ��ΪEqualer<K, K>
 */
template <typename K, typename KH = Hasher<K>, typename KE = Equaler<K, K> >
class LockTable {
	/** ������*/
	struct LockObject {
		LockObject			*next;			/** ͬһ����۵���һ��������				*/
		K					key;			/** ����ؼ���								*/
		u16					readerCount;	/** �Ӷ����߳�������ͻʱ������ͼ�������߳�	*/
		u16					writerCount;	/** ��д���߳�������ͻʱ������ͼ�������߳�	*/
		LockMode			curMode;		/** ������ǰ״̬							*/
		Atomic<int>			waiting;		/** ���ڵȴ��������߳���					*/
		Event				event;			/** ���ڻ��ѵȴ��̵߳��¼�					*/
		u16					xHolder;		/** д��������								*/

		inline void init() {
			readerCount = 0;
			writerCount = 0;
			curMode = None;
			xHolder = 0;
		}
	};


	/** ����� */
	struct LockTableSlot {
		Mutex		mutex;				/** �����ڱ����������ݽṹ */
		LockObject	*locks;				/** ����������, ���������Ƕ������embededLock */
		LockObject	*freeObject;		/** ��slot�������������� */
		uint		freeObjectCount;	/** ��slot���ж������ */
		bool		embededValid;		/** ��Ƕ�������Ƿ�Ϸ� */
		LockObject	embededLockObject;  /** ��Ƕ������ */
		uint		validObjectCount;	/** ����ʹ���е���������� */
		uint		readers;			/** ���ڼӵĶ������� */
		uint		writers;			/** ���ڼӵ�д������ */

		LockTableSlot(): mutex("LockTableSlot::mutex", __FILE__, __LINE__) {
		}
	};


public:
	/**
	 * ����һ����������
	 *  �����������ܷ�maxLocks�����������maxLocks + slotCount * reservedLockPerSlot����
	 *
	 * @param maxLocks ���֧�ֵ���������ע���������ܿ��ǣ��������������ܱ�֤�����󲻻ᳬ��maxLocks����
	 * @param slotCount ����slot��Ŀ, slotCount������2����������
	 * @param reservedLockPerSlot ÿ��slot������Ԥ���䣩����������Ŀ
	 */
	LockTable(uint maxLocks, uint slotCount, uint reservedLockPerSlot):
		m_freeObjectsMutex("LockTable::freeObjectsMutex", __FILE__, __LINE__) {
			assert(maxLocks);
			assert(slotCount);
			init(maxLocks, slotCount, reservedLockPerSlot);
	  }


	/**
	 * ����һ����������
	 *
	 * @param maxLocks ���֧�ֵ���������ע���������ܿ��ǣ��������������ܱ�֤�����󲻻ᳬ��maxLocks����
	 */
	LockTable(uint maxLocks): m_freeObjectsMutex("LockTable::freeObjectsMutex", __FILE__, __LINE__) {
		assert(maxLocks);
		uint slotCount = flp2(maxLocks);	// ��ȥ����2��ָ��
		init(maxLocks, slotCount, 1);
	}


	~LockTable() {
		delete[] m_table;
		delete[] m_objects;
	}


	/**
	 * ���Զ�ָ���������
	 *
	 * @param threadId �������߳�ID
	 * @param key ��ֵ
	 * @param lockMode ��ģʽ
	 * @return �Ƿ�����ɹ����������ɹ��п��������ڶ����Ѿ���������Ҳ�п�����������������
	 */
	bool tryLock(u16 threadId, K key, LockMode lockMode) {
		assert( lockMode == Shared || lockMode == Exclusived );

		lockMode == Shared ? ++m_usage.m_rlockCnt : ++m_usage.m_wlockCnt;

		LockTableSlot *slot = getSlot(key);
		LOCK(&slot->mutex);

		// �ҵ����ߴ���������
		LockObject *lockObject = findOrAlloc(slot, key, false);
		if (!lockObject) {
			UNLOCK(&slot->mutex);
			return false;
		}

		if (lockObject->readerCount + lockObject->writerCount == 0) {// ��һ��������������ģʽ
			lockObject->curMode = lockMode;
			goto LOCK_SUCC;
		}

		if (lockMode == Shared && lockObject->curMode == Shared) // û������ͻ
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
	 * ��ָ���������
	 *
	 * @param threadId �������߳�ID
	 * @param key ��ֵ
	 * @param lockMode ��ģʽ
	 * @param timeoutMs ������ʱʱ�䣬��λ���룬��Ϊ0�����򲻳�ʱ
	 * @return �Ƿ�����ɹ�
	 * @throw NtseException ��������
	 */
	bool lock(u16 threadId, K key, LockMode lockMode, int timeoutMs = -1) throw(NtseException) {
		assert( lockMode == Shared || lockMode == Exclusived );

		LockTableSlot *slot = getSlot(key);
		LOCK(&slot->mutex);

		// �ҵ����ߴ���������
		LockObject *lockObject;
		try {
			lockObject = findOrAlloc(slot, key);
		} catch (NtseException &e) {
			UNLOCK(&slot->mutex);
			throw e;
		}

		if (lockObject->readerCount + lockObject->writerCount == 0) { // ��һ��������������ģʽ
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
			if (lockObject->curMode == Shared) {    // Shared �� Shared ����ͻ
				UNLOCK(&slot->mutex);
				return true;
			}
		} else {
			++lockObject->writerCount;
			++slot->writers;
			++m_usage.m_wlockCnt;
		}

		// �Ѿ�����readerCount, writerCount
		// ��ʱ�ͷ�slot->mutex���ᵼ��������ɾ��
		UNLOCK(&slot->mutex);

		bool succ = lockConflict(slot, lockObject, lockMode, timeoutMs);
		if (succ && lockMode == Exclusived)
			lockObject->xHolder = threadId;
		return succ;
	}


	/**
	 * ����
	 *
	 * @param key �����ֵ
	 * @param lockMode ��ģʽ
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

		if (lockObject->writerCount + lockObject->readerCount == 0) { // ��Ҳ�����ڼ����ߣ���ʱ�ͷ�������
			freeLockObject(slot, lockObject);
		}

		UNLOCK(&slot->mutex);
	}


	/**
	 * ����ָ���ļ�ֵ�Ƿ�ָ���̼߳��˻�����
	 *
	 * @param threadId �߳�ID
	 * @param key ��ֵ
	 * @return ��ֵkey�Ƿ񱻻Ựsession��Exclusivedģʽ����
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
	 * ����ָ���ļ�ֵ�Ƿ񱻼��˹�����
	 *
	 * @param key ��ֵ
	 * @return ��ֵkey�Ƿ���Sharedģʽ����
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
	 * ���ʹ�����ͳ��
	 *
	 * @return ʹ�����ͳ��
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

	/** ��ӡʹ�����ͳ��
	 * @param out �����
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
	 * ����������ʼ��
	 * @param maxLocks ���֧�ֵ�������
	 * @param slotCount ����slot��Ŀ��һ����2����������
	 * @param reservedLockPerSlot ÿ��slot������Ԥ���䣩����������Ŀ
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
				// ����freeObject�����������m_reservedLockPerSlot������
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

		// ��ʼ��ȫ��freeObject��
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
	 * ����key��ȡslot
	 * @param key ������ؼ���
	 */
	inline LockTableSlot* getSlot(K key) {
		unsigned int hashCode = m_keyHash(key);
		return &m_table[hashCode & m_slotMask];
	}


	/**
	 * ���һ��ߴ���������
	 *
	 * @pre	slot->mutex������
	 * @param slot ���������ڵ�slot
	 * @param key ������ؼ���
	 * @param throwException ��������ʱ���Ƿ���Ҫ�׳��쳣
	 * @return key��Ӧ�������󣬷���0��ʾʧ��
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
	 * ����������
	 * �ȴ�slotԤ�����ж����з��䣬���䲻�ɹ��ٴ�ȫ�ֿ��ж��������з���
	 * @pre	slot->mutex������
	 * @param slot �����
	 * @return �������������������������NULL
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
	 * ����������
	 * @pre	slot->mutex������
	 * @param slot �����
	 * @param key ������ؼ���
	 * @return key��Ӧ�������󣬷���NULL��ʾ�Ҳ���
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
	 * ��������ɾ��������
	 * @pre	��Ӧ��������Ѿ�������
	 * @param objectList ����������
	 * @param lockObject Ҫɾ����������
	 * @return Ҫɾ������������ָ�������з���true�����򷵻�false
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
	 * �ͷ�������
	 * @pre	slot->mutex������
	 * @param slot ���������ڵ�slot
	 * @param lockObject ���ͷŵ�������
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
		} else { // Ԥ������������
			LOCK(&m_freeObjectsMutex);
			lockObject->next = m_freeObjects;
			m_freeObjects = lockObject;
			UNLOCK(&m_freeObjectsMutex);
		}
		--slot->validObjectCount;
	}

	/**
	 * ����ͻ֮�󣬳��Լ���
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
	 * �����������ͻ����
	 *
	 * @pre slot->mutex �Ѿ�������
	 *
	 * @param slot �����������Slot
	 * @param lockObject ������
	 * @param lockMode Ŀ����ģʽ
	 * @param timeoutMs ������ʱʱ�䣬��λ���룬��Ϊ0�����򲻳�ʱ
	 * @return �Ƿ�����ɹ�
	 */
	inline bool lockConflict(LockTableSlot *slot, LockObject *lockObject, LockMode lockMode, int timeoutMs)	{
		// æ��
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
		// ����event��
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

			if (timeoutMs > 0) {    // ��ʱ�ж�
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
	uint			m_slotCount;				/** ����Slot��Ŀ */
	uint			m_slotMask;					/** ����Slot�������� */
	uint			m_reservedLockPerSlot;		/** ÿ��Slot�б�����(Ԥ�����)��������Ŀ */
	LockObject		*m_objects;					/** ���������е������� */
	Mutex			m_freeObjectsMutex;			/** ���������������� */
	LockObject		*m_freeObjects;				/** �������������� */
	LockTableSlot	*m_table;					/** ����slot���� */

	KH				m_keyHash;					/** ��ϣ�������� */
	KE				m_keyEqualer;				/** ��ֵ�ȽϺ������� */

	LockTableStatus	m_usage;					/** ʹ�����ͳ�� */
};



/** �������� */
class LockManager: public LockTable<u64> {
	typedef LockTable<u64>  SUPER;
public:
	/**
	 * ����һ����������
	 *  �����������ܷ�maxLocks�����������maxLocks + slotCount*reservedLockPerSlot����
	 * @param maxLocks ���֧�ֵ���������ע���������ܿ��ǣ��������������ܱ�֤�����󲻻ᳬ��maxLocks����
	 * @param slotCount ����slot��Ŀ
	 * @param reservedLockPerSlot ÿ��slot������Ԥ���䣩����������Ŀ
	 */
	LockManager(uint maxLocks, uint slotCount, uint reservedLockPerSlot)
		: SUPER(maxLocks, slotCount, reservedLockPerSlot) {
	}


	/**
	 * ����һ����������
	 *
	 * @param maxLocks ���֧�ֵ���������ע���������ܿ��ǣ��������������ܱ�֤�����󲻻ᳬ��maxLocks����
	 */
	LockManager(uint maxLocks)
		: SUPER(maxLocks) {
	}
};


}

#endif
