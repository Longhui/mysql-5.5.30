/**
 * ���߳�ͬ������ඨ��
 *
 * @author ��Դ(wangyuan@corp.netease.com, wy@163.org)
 */

#ifndef _NTSE_SYNC_H_
#define _NTSE_SYNC_H_

#include <ostream>
#include <memory.h>
#include "util/System.h"
#include "util/DList.h"
#include <assert.h>
#ifdef WIN32
#include <Windows.h>
#include <intrin.h>
#else
#include <pthread.h>
#include <stdlib.h>
#include <sys/time.h>
#endif

namespace ntse {

/**
 * �޸Ĳ���Ϊԭ�Ӳ������������ӿ�ģ��Java�е�AtomicInteger�ࡣ
 *
 * @param T ������[unsigned] char/short/int/long���������ͣ�������ʹ��u64/s64���ͣ�������������32λ�������޷�֧��
 */
template<typename T>
struct Atomic {
public:
	/**
	 * ���캯����ԭ������ֵ����ʼ��Ϊ0
	 */
	Atomic() {
		m_v = 0;
	}

	/** 
	 * ���캯����ԭ������ֵ����ʼ��Ϊָ����ֵ
	 *
	 * @param v ԭ�����ĳ�ʼֵ
	 */
	Atomic(T v) {
		m_v = v;
	}

	/**
	 * �õ�ԭ�����ĵ�ǰֵ
	 *
	 * @return ԭ�����ĵ�ǰֵ
	 */
	inline T get() const {
#ifdef WIN32
		MemoryBarrier();
#else
		__sync_synchronize();
#endif
		return m_v;
	}

	/** 
	 * ����ԭ�����ĵ�ǰֵ
	 *
	 * @param v ָ����ֵ
	 */
	inline void set(T v) {
#ifdef WIN32
		MemoryBarrier();
#else
		__sync_synchronize();
#endif
		m_v = v;
	}

	/**
	 * �õ�ԭ�����ĵ�ǰֵ�����ҽ�ԭ������1
	 *
	 * @return ԭ������1֮ǰ��ֵ
	 */
	inline T getAndIncrement() {
#ifdef WIN32
		T v = m_v;
		interlockedIncrement(&m_v);
		return v;
#else
		return __sync_fetch_and_add(&m_v, 1);
#endif
	}

	/**
	 * ��ԭ������1�������޸ĺ��ֵ
	 *
	 * @return ԭ������1֮���ֵ
	 */
	inline T incrementAndGet() {
#ifdef WIN32
		return interlockedIncrement(&m_v);
#else
		return __sync_add_and_fetch(&m_v, 1);
#endif
	}

	/**
	 * ��ԭ������1����������������getAndIncrement�죬�����������ֻ���ô��뿴��ȥ����ȻһЩ��
	 */
	inline void increment() {
		incrementAndGet();
	}

	/**
	 * �õ�ԭ�����ĵ�ǰֵ�����ҽ�ԭ������1
	 *
	 * @return ԭ������1֮ǰ��ֵ
	 */
	inline T getAndDecrement() {
#ifdef WIN32
		T v = m_v;
		interlockedDecrement(&m_v);
		return v;
#else
		return __sync_fetch_and_sub(&m_v, 1);
#endif
	}

	/**
	 * ��ԭ������1�������޸ĺ��ֵ
	 *
	 * @return ԭ������1֮���ֵ
	 */
	inline T decrementAndGet() {
#ifdef WIN32
		return interlockedDecrement(&m_v);
#else
		return __sync_sub_and_fetch(&m_v, 1);
#endif
	}

	/**
	 * ��ԭ������1����������������getAndDecrement�죬�����������ֻ���ô��뿴��ȥ����ȻһЩ��
	 */
	inline void decrement() {
		decrementAndGet();
	}

	/** ��ԭ��������ָ����ֵ�����ҷ��ؼ�֮��Ľ��
	 * @param delta Ҫ���ϵ�ֵ
	 * @return ��֮��Ľ��
	 */
	inline T addAndGet(T delta) {
#ifdef WIN32
		return InterlockedExchangeAdd(&m_v, delta) + delta;
#else
		return __sync_add_and_fetch(&m_v, delta);
#endif
	}

	/**
	 * ԭ��CAS����������ԭ�����ĵ�ǰֵ�Ƿ���expect��ȣ����������ֵ�滻Ϊupdate��
	 * �����޸�ԭ������ֵ
	 *
	 * @param expect �ж�ԭ������ǰֵ�Ƿ�������ֵ
	 * @param update ��������ʱ��ԭ�������³����ֵ
	 *
	 * @return �Ƿ�������滻
	 */
	inline bool compareAndSwap(T expect, T update) {
#ifdef WIN32
		return interlockedCompareAndSwap(&m_v, update, expect);
#else
		return __sync_bool_compare_and_swap(&m_v, expect, update);
#endif
	}

	// Windows��ʹ��ϵͳ�ṩ��InterlockedXXXϵ�к���ʵ�֣�������XP�²�֧��64λ��ԭ�Ӳ���
	// ����ʵ����64λԭ������ʵ���ǲ���ȷ��
#ifdef WIN32
#pragma intrinsic(_InterlockedIncrement)
#pragma intrinsic(_InterlockedDecrement)
#pragma intrinsic(_InterlockedCompareExchange)
private:
	inline int interlockedIncrement(volatile int *p) {
		return _InterlockedIncrement((LONG *)p);
	}

	inline long interlockedIncrement(volatile long *p) {
		return _InterlockedIncrement((LONG *)p);
	}

	inline int interlockedDecrement(volatile int *p) {
		return _InterlockedDecrement((LONG *)p);
	}

	inline long interlockedDecrement(volatile long *p) {
		return _InterlockedDecrement((LONG *)p);
	}

	inline bool interlockedCompareAndSwap(volatile int *p, int exchange, int comparand) {
		return _InterlockedCompareExchange((LONG *)p, (LONG)exchange, (LONG)comparand) == comparand;
	}

	inline bool interlockedCompareAndSwap(volatile long *p, long exchange, long comparand) {
		return _InterlockedCompareExchange((LONG *)p, (LONG)exchange, (LONG)comparand) == comparand;
	}

	inline int InterlockedExchangeAdd(volatile int *p, int delta) {
		return _InterlockedExchangeAdd((LONG *)p, (LONG)delta);
	}

	inline int InterlockedExchangeAdd(volatile long *p, long delta) {
		return _InterlockedExchangeAdd((LONG *)p, (LONG)delta);
	}
#endif

private:
	volatile T	m_v;	/** ԭ�����ĵ�ǰֵ */
};


/** �¼�����ʹ��ͳ����Ϣ */
struct EventUsage {
	u64		m_sigCnt;		/** �¼������������� */
	u64		m_waitCnt;		/** �¼��ȴ��������� */
	u64		m_waitTime;
	u64		m_longestWait;
};


struct WaitEvent; //�Ⱥ��¼��ṹ��

/** �¼� */
struct Event {
public:
	static const int WAIT_UP_PERIOD = 5;		/** ��ʱ���ѳ�ʱ��ȴ��¼������ڣ�s��*/
	static const u32 WAIT_TIME_THRESHOLD = 10;	/** �����߳����ٵȴ���ʱ��(s) */

public:
	Event(bool autoReset = true);
	~Event();
	long reset();
	bool wait(int timeoutMs, long resetToken = UNSPECIFIED_TOKEN);
	void signal(bool broadcast = false);
	const EventUsage& getUsage() const;

	static const long UNSPECIFIED_TOKEN = 0;	/** waitʱû��ָ��token */

private:
	void lock();
	void unlock();
	bool checkNoWait(long resetToken = UNSPECIFIED_TOKEN);
	void addWaitEvent(DLink<WaitEvent *> *plnk, bool noAction);
	void removeWaitEvent(DLink<WaitEvent *> *plnk, bool noAction);
	
private:
	bool			m_autoReset;/** �Զ�RESET? */
	EventUsage		*m_usage;	/** ʹ�����ͳ�� */
	long			m_sigToken;	/** �¼�����״̬ */

#ifdef WIN32
	CRITICAL_SECTION	m_lock;	/** ���ڱ����ڲ�״̬ */
	HANDLE			m_osEvent;	/** ����ϵͳ�¼���� */	
	static CRITICAL_SECTION m_eventListLock;	/** ���ڱ����ȴ��¼��б� */

#else
	pthread_mutex_t	m_lock;		/** ���ڱ����ڲ�״̬ */
	pthread_cond_t	m_cond;		/** �������� */
	static pthread_mutex_t m_eventListLock;	/** ���ڱ����ȴ��¼��б� */
	bool			m_set;		/** �Ƿ���δ��wait��signal */
#endif
	static DList<WaitEvent *> *m_waitEventList; /** �ȴ��¼����� */

	friend class EventMonitorHelper;
};

#ifdef WIN32
#define PAUSE_INSTRUCTION()	 {__asm {pause}}
#else
#define PAUSE_INSTRUCTION() {__asm__ __volatile__ ("pause");}
#endif

/** ʹ�ö༶��ʱ��׼�����������͡�ʵ��ʱ��������߼�������Ը��ߵ����ȼ���
 * ��������������X������������������˶���Ҫ�ȴ������Ƶ����X��SIX��S��Щ
 * �����ᵼ����������ͻ���Ҹ��ͼ�������ȴ���������Ƶ�Ŀ����Ϊ�˷�ֹ
 * ����Щ�߼�����ʱ������ʱ�������
 *
 * ��ģʽ�����Ծ���
 * 		IS	IX	S	SIX	U	X
 * IS	y	y	y	y	y	n		
 * IX	y	y	n	n	n	n
 * S		y	n	y	n	y	n
 * SIX	y	n	n	n	n	n
 * U		y	n	y	n	n	n
 * X		n	n	n	n	n	n
 *
 * �����ȼ�
 * U,X > S,SIX > IS,IX
 */
enum ILMode {
	IL_NO = 0,		/** û���� */
	IL_IS,			/** IS������ʾ��ͼ��ȡ����ϸ���ȵ����ݣ�ֻ��X����ͻ */
	IL_IX,			/** IX������ʾ��ͼ�޸Ĳ���ϸ���ȵ����ݣ���S��SIX��X��U����ͻ */
	IL_S,			/** S������ʾ���ܶ�ȡ�������ݣ���IS��S��U������ */
	IL_SIX,			/** SIX������ʾ���ܶ�ȡ�������ݣ����޸Ĳ������ݣ�ֻ��IS������ */
	IL_U,			/** U������ʾ��Ҫ�޸��������ݣ�ֻ��IS��S���� */
	IL_X,			/** X������ʾ�����޸��������ݣ�������������ͻ */
	IL_MAX			/** ������ʾ���������͸���������һ���������� */
};

/** �༶��ʹ�����ͳ�� */
struct ILUsage {
	u64		m_lockCnt[IL_MAX];	/** ����ģʽ�������� */
	u64		m_spinCnt;			/** �������� */
	u64		m_waitCnt;			/** �ȴ����� */
	u64		m_waitTime;			/** �ܵĵȴ�ʱ�䣬��λ������ */

	ILUsage() {
		memset(this, 0, sizeof(ILUsage));
	}
};

/** ���������͵Ļ��� */
struct Lock {
protected:
	Lock(const char *name, const char *file, uint line);
	virtual ~Lock();

public:
	const char	*m_name;	/** ���������ƣ�����ʹ�� */
	const char	*m_file;	/** ����������ʱ�Ĵ����ļ� */
	uint		m_line;		/** ����������ʱ���к� */
	static const bool CONFLICT_MATRIX[IL_MAX][IL_MAX];	/* ��ģʽ��ͻ���� */
};

/** ��ģʽ */
enum LockMode {
	None,			/** ������ */
	Shared,			/** ������ */
	Exclusived,		/** ������ */
};

/** ������ͳ�ƶ���ɨ���� */
class MuScanHandle {
private:
	size_t		m_curPos;		/** ��ǰɨ��λ�� */
friend struct MutexUsage;
};

/** ������ʹ�����ͳ����Ļ��� */
struct LockUsage {
	const char	*m_allocFile;	/** ���������ʱ�Ĵ����ļ��� */
	uint		m_allocLine;	/** ���������ʱ�Ĵ����к� */
	const char	*m_name;		/** ���� */
	uint		m_instanceCnt;	/** ʹ����һͳ�ƶ������������� */
	bool		m_shared;		/** �Ƿ񱻶���������� */
	u64			m_spinCnt;		/** �������ϼ����������������Ĵ��� */
	u64			m_waitCnt;		/** �������Բ��ܼ����������еȴ��Ĵ��� */
	u64			m_waitTime;		/** �ȴ�ʱ�䣬��λ���� */
	
	LockUsage(const char *name, const char *allocFile, uint allocLine);
	virtual ~LockUsage();
	virtual void print(std::ostream &out) const;
};

/** ������ʹ�����ͳ�� */
struct MutexUsage: public LockUsage {
	u64			m_lockCnt;		/** ������������ */

	MutexUsage(const char *name, const char *allocFile, uint allocLine);
	virtual void print(std::ostream &out) const;
	static void beginScan(MuScanHandle *h);
	static const MutexUsage* getNext(MuScanHandle *h);
	static void endScan(MuScanHandle *h);
	static void printAll(std::ostream &out);
};


#pragma region NEW_MUTEX

/** �°滥����������5.4��InnoDB��ʵ�� */
struct Mutex: public Lock {
public:
	Mutex(const char *name, const char *file, uint line);
	virtual ~Mutex();

	/**
	 * �ӻ�����
	 *
	 * @param file �ļ���
	 * @param line �к�
	 */
	inline void lock(const char *file, uint line) {
		if (!m_lockWord.compareAndSwap(0, 1)) 
			lockConflict(-1);
		m_file = file;
		m_line = line;
#ifdef NTSE_SYNC_DEBUG
		m_thread = System::currentOSThreadID();
#endif
		m_usage->m_lockCnt++;
	}

	/**
	 * �ӻ�����������ָ����ʱʱ�䡣
	 *
	 * @param timeoutMs	��ʱʱ�䣬��Ϊ<0�򲻳�ʱ����Ϊ0�����ϳ�ʱ���൱��tryLock
	 * @param file		�ļ���
	 * @param line		�к�
	 * @return �Ƿ�����ɹ���
	 */
	inline bool timedLock(int timeoutMs, const char *file, uint line) {
		if (tryLock(file, line))
			return true;
		if (timeoutMs == 0)
			return false;
		if (lockConflict(timeoutMs)) {
			m_file = file;
			m_line = line;
#ifdef NTSE_SYNC_DEBUG
			m_thread = System::currentOSThreadID();
#endif
			return true;
		} else
			return false;
	}

	/**
	 * ���Լӻ�������
	 *
	 * @param file �ļ���
	 * @param line �к�
	 * @return �Ƿ�����ɹ�
	 */
	inline bool tryLock(const char *file, uint line) {
		m_usage->m_lockCnt++;
		if (!m_lockWord.compareAndSwap(0, 1))
			return false;
		m_file = file;
		m_line = line;
		return true;
	}

	/**
	 * �ж��Ƿ��Ѿ���������
	 *
	 * @return �Ƿ�������
	 */
	inline bool isLocked() const {
		return m_lockWord.get() > 0;
	}

	/**
	 * ����
	 */
	inline void unlock() {
		assert(m_lockWord.get() == 1);
		m_lockWord.set(0);
		if (m_waiting.get() > 0)
			m_event.signal(true);
	}

	const MutexUsage* getUsage() const;

private:
	bool lockConflict(int timeoutMs);

public:
	static const uint	SPIN_COUNT = 30;		/** ����ʱ���������� */
	static const uint	SPIN_DELAY = 6;		/** ����ÿһ����������ʱ�䣨ʱ����������*/

private:
	Atomic<int>			m_lockWord;	/** ����״̬��0��ʾδ��������1��ʾ������ */
	Atomic<int>			m_waiting;	/** ���ڵȴ��������߳��� */
	Event				m_event;	/** ���ڻ��ѵȴ��̵߳��¼� */
	const char			*m_file;	/** ���������ļ��� */
	uint				m_line;		/** ���������к� */
#ifdef NTSE_SYNC_DEBUG
	uint				m_thread;	/** �������߳�ID */
#endif
	MutexUsage			*m_usage;	/** ʹ�����ͳ�� */
};

#pragma endregion NEW_MUTEX


/** �Ƽ�ʹ������ĺ���м�/�������Զ���¼����ʱ���ļ����к� */
#define LOCK(mutex)		(mutex)->lock(__FILE__, __LINE__)
#define UNLOCK(mutex)	(mutex)->unlock()
#define TRYLOCK(mutex)	(mutex)->tryLock(__FILE__, __LINE__)

/** ��д��ͳ�ƶ���ɨ���� */
class RwluScanHandle {
private:
	size_t		m_curPos;		/** ��ǰɨ��λ�� */
	friend struct RWLockUsage;
};

/** ��д��ʹ�����ͳ�� */
struct RWLockUsage: public LockUsage {
	u64			m_rlockCnt;		/** �Ӷ����������� */
	u64			m_wlockCnt;		/** ��д���������� */
	
	RWLockUsage(const char *name, const char *allocFile, uint allocLine);
	virtual void print(std::ostream &out) const;
	static void beginScan(RwluScanHandle *h);
	static const RWLockUsage* getNext(RwluScanHandle *h);
	static void endScan(RwluScanHandle *h);
	static void printAll(std::ostream &out);
};


/** �°��д������ʵ�ֲο���MySQL 5.4��InnoDB�Ķ�д����Thanks Google
 * ��֧�ֵݹ��������һ���̲߳��ܼӶ�����ٽ�����
 */
struct RWLock: public Lock {
public:
	RWLock(const char *name, const char *file, uint line, bool preferWriter = false);
	~RWLock();
	/**
	 * �Ӷ�д��
	 *
	 * @param lockMode ��ģʽ
	 * @param file �ļ���
	 * @param line �к�
	 */
	inline void lock(LockMode lockMode, const char *file, uint line) {
		if (lockMode == Shared) {
			m_usage->m_rlockCnt++;
			if (tryLockR(file, line))
				return;
			else {
				rlockConflict(file, line, NEVER_TIMEOUT);
				return;
			}
		} else {
			assert(lockMode == Exclusived);
			m_usage->m_wlockCnt++;
			if (tryLockW(file, line))
				return;
			else {
				wlockConflict(file, line, NEVER_TIMEOUT);
				return;
			}
		}
	}
	/**
	 * �Ӷ�д��������ָ����ʱʱ��
	 *
	 * @param lockMode ��ģʽ
	 * @param timeoutMs <0��ʾ����ʱ��=0ʱ�������������������ϳ�ʱ��>0Ϊ����Ϊ��λ�ĳ�ʱʱ��
	 * @param file �ļ���
	 * @param line �к�
	 */
	inline bool timedLock(LockMode lockMode, int timeoutMs, const char *file, uint line) {
		if (lockMode == Shared) {
			m_usage->m_rlockCnt++;
			if (tryLockR(file, line))
				return true;
			else {
				if (timeoutMs == 0)
					return false;
				return rlockConflict(file, line, timeoutMs < 0? NEVER_TIMEOUT: System::currentTimeMillis() + timeoutMs);
			}
		} else {
			assert(lockMode == Exclusived);
			m_usage->m_wlockCnt++;
			if (tryLockW(file, line))
				return true;
			else {
				if (timeoutMs == 0)
					return false;
				return wlockConflict(file, line, timeoutMs < 0? NEVER_TIMEOUT: System::currentTimeMillis() + timeoutMs);
			}
		}
	}
	/**
	 * ���ԼӶ�д��
	 *
	 * @param lockMode ��ģʽ
	 * @param file �ļ���
	 * @param line �к�
	 * @return �Ƿ�ɹ�
	 */
	inline bool tryLock(LockMode lockMode, const char *file, uint line) {
		if (lockMode == Shared) {
			m_usage->m_rlockCnt++;
			return tryLockR(file, line);
		} else {
			assert(lockMode == Exclusived);
			m_usage->m_wlockCnt++;
			return tryLockW(file, line);
		}
	}
	bool tryUpgrade(const char *file, uint line);
	bool isLocked(LockMode lockMode) const;
	LockMode getLockMode() const;
	/** �ͷŶ�д��
	 * @param ��ģʽ
	 */
	inline void unlock(LockMode lockMode) {
		if (lockMode == Shared) {
			assert(m_lockWord.get() != 0);
			assert(m_lockWord.get() < WLOCK_DEC);
			if (incrLockWord(RLOCK_DEC) == 0) {
				m_forNextWriter.signal(true);
			}
		} else {
			assert(lockMode == Exclusived);
			assert(m_lockWord.get() == 0);
			incrLockWord(WLOCK_DEC);
			if (m_waiters.get()) {
				m_waiters.compareAndSwap(1, 0);
				m_forOthers.signal(true);
			}
		}
	}
	const RWLockUsage* getUsage() const;
	/** �õ���ģʽ��Ӧ���ַ�����ʾ
	 * @mode ��ģʽ
	 * @return ��Ӧ���ַ�������
	 */
	static const char* getModeStr(LockMode mode) {
		switch (mode) {
		case None:
			return "None";
		case Shared:
			return "Shared";
		default:
			assert(mode == Exclusived);
			return "Exclusived";
		}
	}

private:
	/** ���ԼӶ���
	 * @param file �ļ���
	 * @param line �к�
	 */
	inline bool tryLockR(const char *file, uint line) {
		if (decrLockWord(RLOCK_DEC)) {
			m_rlockFile = file;
			m_rlockLine = line;
			return true;
		}
		return false;
	}
	/** ���Լ�д��
	 * @param file �ļ���
	 * @param line �к�
	 */
	inline bool tryLockW(const char *file, uint line) {
		if (decrLockWord(WLOCK_DEC)) {
			if (m_lockWord.get() == 0) {
				m_wlockFile = file;
				m_wlockLine = line;
				return true;
			}
			incrLockWord(WLOCK_DEC);
			return false;
		}
		return false;
	}
	bool acquireLockWordW(u64 absTimeout, bool *hasWait);
	/** ���Դ�m_lockWord�м�ȥamount
	 * @param amount Ҫ��ȥ��ֵ
	 * @return �Ƿ�ɹ�
	 */
	inline bool decrLockWord(int amount) {
		assert(amount == WLOCK_DEC || amount == RLOCK_DEC);

		int localLockWord = m_lockWord.get();
		while (localLockWord > 0 || (!m_preferWriter && amount == RLOCK_DEC && localLockWord != 0)) {
			if (m_lockWord.compareAndSwap(localLockWord, localLockWord - amount))
				return true;
			localLockWord = m_lockWord.get();
		}
		return false;
	}
	/** m_lockWord����ָ����ֵ
	 * @param amount Ҫ���ϵ�ֵ
	 * @return m_lockWord����amount֮�����ֵ
	 */
	inline int incrLockWord(int amount) {
		return m_lockWord.addAndGet(amount);
	}
	bool rlockConflict(const char *file, uint line, u64 absTimeout);
	bool wlockConflict(const char *file, uint line, u64 absTimeout);
	bool doWait(Event *evt, u64 absTimeout, long token);
	bool waitForReaders(u64 absTimeout, bool *hasWait);

private:
	static const int WLOCK_DEC = 1000000;	/** ��д��ʱ��m_lockWord�м�ȥ���ֵ */
	static const int RLOCK_DEC = 1;			/** �Ӷ���ʱ��m_lockWord�м�ȥ���ֵ */
	static const u64 NEVER_TIMEOUT = (u64)-1;	/** ������ʱ���� */

	bool			m_preferWriter;	/** �Ƿ����д���Ը������ȼ� */
	Atomic<int>		m_lockWord;		/** ����״̬
									 * ��ʼֵ: WLOCK_DEC����ʾû�м���
									 * 0 < m_lockWord < WLOCK_DEC: ���˶���������û�������д��
									 * = 0: ����д��
									 * -WLOCK_DEC < m_lockWord < 0: ���˶��������������ڵȴ���д��
									 */
	Atomic<int>		m_waiters;		/** �Ƿ������ڵȴ�m_forOthers�¼���������m_forNextWriter�¼���
									 * ��ֻ�Ǹ�����ֵ������ʾ�ж������ڵȴ�����˻���ʱ��ʹ��
									 * �㲥��ʽ
									 * ����ʱ�ĵȴ��ڳ�ʱʱҲ������ֵ��Ϊ0����Ϊ���ܻ�������
									 * ��Ҳ�ڵȴ�
									 */
	Event		m_forNextWriter;/** �Ѿ���ѡ����Ϊ��д����Ψһ��ѡ�˵ȴ���һ�¼� */
	Event		m_forOthers;	/** �����˵ȴ���һ�¼� */
	const char		*m_rlockFile;	/** ���һ���������������ļ��� */
	uint			m_rlockLine;	/** ���һ���������������к� */
	const char		*m_wlockFile;	/** ���һ��д�����������ļ��� */
	uint			m_wlockLine;	/** ���һ��д�����������к� */
	RWLockUsage		*m_usage;		/** ʹ�����ͳ�� */
};

/** �Ƽ�ʹ������ĺ���м�/�������Զ���¼����ʱ���ļ����к� */
#define RWLOCK(rwlock, mode)	(rwlock)->lock(mode, __FILE__, __LINE__)
#define RWTIMEDLOCK(rwLock, mode, timeoutMs) (rwLock)->timedLock(mode, timeoutMs, __FILE__, __LINE__)
#define RWTRYLOCK(rwlock, mode)	(rwlock)->tryLock(mode, __FILE__, __LINE__)
#define RWUNLOCK(rwlock, mode)	(rwlock)->unlock(mode)

/** ��ӵ������Ϣ */
struct LockerInfo {
	static const u16 WAIT_FOR_NOTHING = 0xFFFF;	/** ��ʾ���ڵȴ� */

	u16			m_mode;		/** ��ģʽ */
	u16			m_waitFor;	/** �ȴ��ĸ��û�����ΪWAIT_FOR_NOTHING��ʾ���ڵȴ� */
	const char	*m_file;	/** ����ʱ�Ĵ����ļ� */
	uint		m_line;		/** ����ʱ�Ĵ����к� */
	uint		m_threadId;	/** ����ϵͳ�̺߳� */

	LockerInfo();
	void set(u16 mode, const char *file, uint line, uint threadId, u16 waitFor);
	void reset();
};

/** �����û����Ķ�д��������ʱ��Ҫָ���û�ID */
struct LURWLock: public Lock {
public:
	LURWLock(u16 maxUserId, const char *name, const char *file, uint line, bool preferWriter = false);
	virtual ~LURWLock();
	/**
	 * �Ӷ�д��
	 *
	 * @param userId �û�ID
	 * @param lockMode ��ģʽ
	 * @param file �ļ���
	 * @param line �к�
	 */
	inline void lock(u16 userId, LockMode lockMode, const char *file, uint line) {
#ifdef NTSE_SYNC_DEBUG
		assert(userId <= m_maxUserId);
		if (tryLock(userId, lockMode, file, line)) {
			return;
		}
		m_userInfos[userId].m_waitFor = calcWaitFor(userId, lockMode);
		m_rwLock.lock(lockMode, file, line);
		m_userInfos[userId].set((u16)lockMode, file, line, System::currentOSThreadID(), LockerInfo::WAIT_FOR_NOTHING);
#else
		UNREFERENCED_PARAMETER(userId);
		m_rwLock.lock(lockMode, file, line);
#endif
	}
	/**
	 * �Ӷ�д��������ָ����ʱʱ��
	 *
	 * @param userId �û�ID
	 * @param lockMode ��ģʽ
	 * @param timeoutMs <0��ʾ����ʱ��=0ʱ�������������������ϳ�ʱ��>0Ϊ����Ϊ��λ�ĳ�ʱʱ��
	 * @param file �ļ���
	 * @param line �к�
	 */
	inline bool timedLock(u16 userId, LockMode lockMode, int timeoutMs, const char *file, uint line) {
#ifdef NTSE_SYNC_DEBUG
		assert(userId <= m_maxUserId);
		if (tryLock(userId, lockMode, file, line)) {
			return true;
		}
		m_userInfos[userId].m_waitFor = calcWaitFor(userId, lockMode);
		if (m_rwLock.timedLock(lockMode, timeoutMs, file, line)) {
			m_userInfos[userId].set((u16)lockMode, file, line, System::currentOSThreadID(), LockerInfo::WAIT_FOR_NOTHING);
			return true;
		} else {
			m_userInfos[userId].m_waitFor = LockerInfo::WAIT_FOR_NOTHING;
			return false;
		}
#else
		UNREFERENCED_PARAMETER(userId);
		return m_rwLock.timedLock(lockMode, timeoutMs, file, line);
#endif
	}
	/**
	 * ���ԼӶ�д��
	 *
	 * @param userId �û�ID
	 * @param lockMode ��ģʽ
	 * @param file �ļ���
	 * @param line �к�
	 * @return �Ƿ�ɹ�
	 */
	inline bool tryLock(u16 userId, LockMode lockMode, const char *file, uint line) {
#ifdef NTSE_SYNC_DEBUG
		assert(userId <= m_maxUserId);
		if (m_rwLock.tryLock(lockMode, file, line)) {
			m_userInfos[userId].set((u16)lockMode, file, line, System::currentOSThreadID(), LockerInfo::WAIT_FOR_NOTHING);
			return true;
		}
		return false;
#else
		UNREFERENCED_PARAMETER(userId);
		return m_rwLock.tryLock(lockMode, file, line);
#endif
	}
	/**
	 * ���Խ���������
	 *
	 * @param userId �û�ID
	 * @param file �ļ���
	 * @param line �к�
	 * @return �Ƿ�ɹ�
	 */
	inline bool tryUpgrade(u16 userId, const char *file, uint line) {
#ifdef NTSE_SYNC_DEBUG
		assert(userId <= m_maxUserId);
		if (m_rwLock.tryUpgrade(file, line)) {
			m_userInfos[userId].set(Exclusived, file, line, System::currentOSThreadID(), LockerInfo::WAIT_FOR_NOTHING);
			return true;
		}
		return false;
#else
		UNREFERENCED_PARAMETER(userId);
		return m_rwLock.tryUpgrade(file, line);
#endif
	}
	/**
	 * �ж϶�д���Ƿ��Ѿ�����ָ����ģʽ����
	 *
	 * @param lockMode ��ģʽ
	 * @return ��д���Ƿ��Ѿ�����ָ����ģʽ����
	 */
	inline bool isLocked(LockMode lockMode) const {
		return m_rwLock.isLocked(lockMode);
	}
	/**
	 * ��õ�ǰ�ӵĶ�д����ģʽ
	 * @return 
	 */
	inline LockMode getLockMode() const {
		return m_rwLock.getLockMode();
	}
	/** �ͷŶ�д��
	 * @param userId �û�ID
	 * @param ��ģʽ
	 */
	inline void unlock(u16 userId, LockMode lockMode) {
#ifdef NTSE_SYNC_DEBUG
		assert(userId <= m_maxUserId);
		m_rwLock.unlock(lockMode);
		m_userInfos[userId].reset();
#else
		UNREFERENCED_PARAMETER(userId);
		m_rwLock.unlock(lockMode);
#endif
	}
	/**
	 * ���ʹ�����ͳ��
	 *
	 * @return ʹ�����ͳ��
	 */
	inline const RWLockUsage* getUsage() const {
		return m_rwLock.getUsage();
	}

private:
#ifdef NTSE_SYNC_DEBUG
	u16 calcWaitFor(u16 userId, LockMode mode);
#endif

private:
	RWLock		m_rwLock;		/** ��д�� */
#ifdef NTSE_SYNC_DEBUG
	u16			m_maxUserId;	/** �����û�ID */
	LockerInfo	*m_userInfos;	/** ���û�������� */
#endif
};

/** ������ͳ�ƶ���ɨ���� */
class IntentionLockUsageScanHandle {
private:
	size_t		m_curPos;		/** ��ǰɨ��λ�� */
	friend struct IntentionLockUsage;
};

/** ������ʹ�����ͳ�� */
struct IntentionLockUsage: public LockUsage {
	u64			m_lockCnt[IL_MAX];		/** ����ģʽ������������ */
	u64			m_failCnt;				/** ����ʱʧ�ܴ���*/

	IntentionLockUsage(const char *name, const char *allocFile, uint allocLine);
	virtual void print(std::ostream &out) const;
	static void beginScan(IntentionLockUsageScanHandle *h);
	static const IntentionLockUsage* getNext(IntentionLockUsageScanHandle *h);
	static void endScan(IntentionLockUsageScanHandle *h);
	static void printAll(std::ostream &out);
};

/** ��׼�Ķ༶������������ */
struct IntentionLock: public Lock {
public:
	IntentionLock(u16 maxUsers, const char *name, const char *file, uint line);
	~IntentionLock();
	bool lock(u16 userId, ILMode mode, int timeoutMs, const char *file, uint line);
	bool upgrade(u16 userId, ILMode oldMode, ILMode newMode, int timeoutMs, const char *file, uint line);
	void unlock(u16 userId, ILMode mode);
	void downgrade(u16 userId, ILMode oldMode, ILMode newMode, const char *file, uint line);
	bool isLocked(u16 userId, ILMode mode);
	ILMode getLock(u16 userId) const;
	/*const ILUsage* getUsage() const;*/
	static const char* getLockStr(ILMode mode);
	bool isSelfConflict(ILMode mode);
	static bool isConflict(ILMode mode1, ILMode mode2);
	static bool isRead(ILMode mode);
	static bool isWrite(ILMode mode);
	static bool isHigher(ILMode low, ILMode high);
	const IntentionLockUsage* getUsage() const;

private:
	bool tryLock(u16 userId, ILMode mode, const char *file, uint line);
	bool tryUpgrade(u16 userId, ILMode oldMode, ILMode newMode, const char *file, uint line);
	void setWant(ILMode mode);
	void resetWant(ILMode mode);
	void setGot(ILMode mode);
	void resetGot(ILMode mode);
	void setWait(u16 userId, ILMode mode);
	void resetWait(u16 userId);

private:
	u16			m_maxUsers;	/** ʹ�ø������û����� */
	Mutex		m_lock;		/** ������������ */
	//ILUsage		m_usage;	/** ʹ�����ͳ�� */
	LockerInfo	*m_state;	/** �����û��ļ������ */
	uint		m_wants[IL_MAX];	/** ��Ӹ�����ģʽ���û����������Ѿ��������� */
	uint		m_gots[IL_MAX];		/** �Ѿ����ϸ�����ģʽ���û��� */
	Event		m_event;	/** ����ʵ�ֵȴ�֪ͨ */
	Atomic<int>	m_waiting;	/** ���ڵȴ����û��� */

	IntentionLockUsage	*m_usage;	/** ʹ�����ͳ�� */
};

/** �˳������ʱ�Զ����� */
struct MutexGuard {
public:
	/**
	 * ���캯��������
	 *
	 * @param lock ������	 
	 * @param file �ļ���
	 * @param line �к�
	 */
	MutexGuard(Mutex *lock, const char *file, uint line) {
		m_lock = lock;
		m_locked = false;
		m_lock->lock(file, line);
		m_locked = true;
	}

	/**
	 * �����������������������
	 */
	~MutexGuard() {
		if (m_locked)
			UNLOCK(m_lock);
	}

	/**
	 * ����
	 * @pre �Ѿ�����unlock����
	 * @param file �ļ���
	 * @param line �к�
	 */	
	void lock(const char *file, uint line) {
		assert(!m_locked);
		m_lock->lock(file, line);
		m_locked = true;
	}

	/** 
	 * ����
	 * @pre û�е���unlock�������Ѿ�����lock������
	 */
	void unlock() {
		assert(m_locked);
		UNLOCK(m_lock);
		m_locked = false;
	}
private:
	Mutex	*m_lock;	/** ������ */
	bool	m_locked;	/** �Ƿ��Ҽ����� */
};

/** �˳������ʱ�Զ����� */
struct RWLockGuard {
public:
	/**
	 * ���캯��������
	 *
	 * @param lock ��д��
	 * @param lockMode ��ģʽ
	 * @param file �ļ���
	 * @param line �к�
	 */
	RWLockGuard(RWLock *lock, LockMode lockMode, const char *file, uint line) {
		m_lock = lock;
		m_lockMode = lockMode;
		m_lock->lock(m_lockMode, file, line);
		m_locked = true;
	}

	/**
	 * ��������������
	 */
	~RWLockGuard() {
		if (m_locked)
			RWUNLOCK(m_lock, m_lockMode);
	}

	/**
	 * ����
	 * @pre �Ѿ�����unlock����
	 * @param file �ļ���
	 * @param line �к�
	 */	
	void lock(const char *file, uint line) {
		assert(!m_locked);
		m_lock->lock(m_lockMode, file, line);
		m_locked = true;
	}

	/** 
	 * ����
	 * @pre û�е���unlock�������Ѿ�����lock������
	 */
	void unlock() {
		assert(m_locked);
		RWUNLOCK(m_lock, m_lockMode);
		m_locked = false;
	}
private:
	RWLock		*m_lock;	/** ��д�� */
	LockMode	m_lockMode;	/** ��ģʽ */
	bool		m_locked;	/** �Ƿ��Ҽ����� */
};

/**
 * ͬ���飬ʹ�÷���������Java�е�synchronized�ؼ��֡���
 *
 * Mutex lock;
 * SYNC(&lock, 
 *     // �����
 * );
 */
#define SYNC(mutex, block)									\
	do {													\
		MutexGuard __syncLock(mutex, __FILE__, __LINE__);	\
		block;												\
	} while(0);

}

#endif
