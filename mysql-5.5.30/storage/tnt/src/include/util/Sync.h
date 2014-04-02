/**
 * 多线程同步相关类定义
 *
 * @author 汪源(wangyuan@corp.netease.com, wy@163.org)
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
 * 修改操作为原子操作的整数。接口模仿Java中的AtomicInteger类。
 *
 * @param T 必须是[unsigned] char/short/int/long等整数类型，不允许使用u64/s64类型，这两个类型在32位机器上无法支持
 */
template<typename T>
struct Atomic {
public:
	/**
	 * 构造函数。原子量的值被初始化为0
	 */
	Atomic() {
		m_v = 0;
	}

	/** 
	 * 构造函数。原子量的值被初始化为指定的值
	 *
	 * @param v 原子量的初始值
	 */
	Atomic(T v) {
		m_v = v;
	}

	/**
	 * 得到原子量的当前值
	 *
	 * @return 原子量的当前值
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
	 * 设置原子量的当前值
	 *
	 * @param v 指定的值
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
	 * 得到原子量的当前值，并且将原子量加1
	 *
	 * @return 原子量加1之前的值
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
	 * 将原子量加1并返回修改后的值
	 *
	 * @return 原子量加1之后的值
	 */
	inline T incrementAndGet() {
#ifdef WIN32
		return interlockedIncrement(&m_v);
#else
		return __sync_add_and_fetch(&m_v, 1);
#endif
	}

	/**
	 * 将原子量加1（这个函数并不会比getAndIncrement快，定义这个函数只是让代码看上去更自然一些）
	 */
	inline void increment() {
		incrementAndGet();
	}

	/**
	 * 得到原子量的当前值，并且将原子量减1
	 *
	 * @return 原子量减1之前的值
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
	 * 将原子量减1并返回修改后的值
	 *
	 * @return 原子量减1之后的值
	 */
	inline T decrementAndGet() {
#ifdef WIN32
		return interlockedDecrement(&m_v);
#else
		return __sync_sub_and_fetch(&m_v, 1);
#endif
	}

	/**
	 * 将原子量减1（这个函数并不会比getAndDecrement快，定义这个函数只是让代码看上去更自然一些）
	 */
	inline void decrement() {
		decrementAndGet();
	}

	/** 将原子量加上指定的值，并且返回加之后的结果
	 * @param delta 要加上的值
	 * @return 加之后的结果
	 */
	inline T addAndGet(T delta) {
#ifdef WIN32
		return InterlockedExchangeAdd(&m_v, delta) + delta;
#else
		return __sync_add_and_fetch(&m_v, delta);
#endif
	}

	/**
	 * 原子CAS操作，比如原子量的当前值是否与expect相等，若相等则将其值替换为update，
	 * 否则不修改原子量的值
	 *
	 * @param expect 判断原子量当前值是否等于这个值
	 * @param update 满足条件时将原子量更新成这个值
	 *
	 * @return 是否进行了替换
	 */
	inline bool compareAndSwap(T expect, T update) {
#ifdef WIN32
		return interlockedCompareAndSwap(&m_v, update, expect);
#else
		return __sync_bool_compare_and_swap(&m_v, expect, update);
#endif
	}

	// Windows下使用系统提供的InterlockedXXX系列函数实现，但由于XP下不支持64位的原子操作
	// 以下实现中64位原子量的实现是不正确的
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
	volatile T	m_v;	/** 原子量的当前值 */
};


/** 事件对象使用统计信息 */
struct EventUsage {
	u64		m_sigCnt;		/** 事件触发操作次数 */
	u64		m_waitCnt;		/** 事件等待操作次数 */
	u64		m_waitTime;
	u64		m_longestWait;
};


struct WaitEvent; //等候事件结构。

/** 事件 */
struct Event {
public:
	static const int WAIT_UP_PERIOD = 5;		/** 定时唤醒长时间等待事件的周期（s）*/
	static const u32 WAIT_TIME_THRESHOLD = 10;	/** 唤醒线程至少等待的时间(s) */

public:
	Event(bool autoReset = true);
	~Event();
	long reset();
	bool wait(int timeoutMs, long resetToken = UNSPECIFIED_TOKEN);
	void signal(bool broadcast = false);
	const EventUsage& getUsage() const;

	static const long UNSPECIFIED_TOKEN = 0;	/** wait时没有指定token */

private:
	void lock();
	void unlock();
	bool checkNoWait(long resetToken = UNSPECIFIED_TOKEN);
	void addWaitEvent(DLink<WaitEvent *> *plnk, bool noAction);
	void removeWaitEvent(DLink<WaitEvent *> *plnk, bool noAction);
	
private:
	bool			m_autoReset;/** 自动RESET? */
	EventUsage		*m_usage;	/** 使用情况统计 */
	long			m_sigToken;	/** 事件触发状态 */

#ifdef WIN32
	CRITICAL_SECTION	m_lock;	/** 用于保护内部状态 */
	HANDLE			m_osEvent;	/** 操作系统事件句柄 */	
	static CRITICAL_SECTION m_eventListLock;	/** 用于保护等待事件列表 */

#else
	pthread_mutex_t	m_lock;		/** 用于保护内部状态 */
	pthread_cond_t	m_cond;		/** 条件变量 */
	static pthread_mutex_t m_eventListLock;	/** 用于保护等待事件列表 */
	bool			m_set;		/** 是否有未被wait的signal */
#endif
	static DList<WaitEvent *> *m_waitEventList; /** 等待事件队列 */

	friend class EventMonitorHelper;
};

#ifdef WIN32
#define PAUSE_INSTRUCTION()	 {__asm {pause}}
#else
#define PAUSE_INSTRUCTION() {__asm__ __volatile__ ("pause");}
#endif

/** 使用多级锁时标准的意向锁类型。实现时，给予更高级别的锁以更高的优先级，
 * 比如如果有人想加X锁，则想加其它锁的人都需要等待。类似的想加X，SIX，S这些
 * 锁都会导致想加与其冲突并且更低级别的锁等待。这样设计的目的是为了防止
 * 加这些高级别锁时经常超时或饿死。
 *
 * 锁模式兼容性矩阵
 * 		IS	IX	S	SIX	U	X
 * IS	y	y	y	y	y	n		
 * IX	y	y	n	n	n	n
 * S		y	n	y	n	y	n
 * SIX	y	n	n	n	n	n
 * U		y	n	y	n	n	n
 * X		n	n	n	n	n	n
 *
 * 锁优先级
 * U,X > S,SIX > IS,IX
 */
enum ILMode {
	IL_NO = 0,		/** 没加锁 */
	IL_IS,			/** IS锁，表示意图读取部分细粒度的数据，只与X锁冲突 */
	IL_IX,			/** IX锁，表示意图修改部分细粒度的数据，与S、SIX、X、U锁冲突 */
	IL_S,			/** S锁，表示可能读取所有数据，与IS、S、U锁兼容 */
	IL_SIX,			/** SIX锁，表示可能读取所有数据，并修改部分数据，只与IS锁兼容 */
	IL_U,			/** U锁，表示将要修改所有数据，只与IS、S兼容 */
	IL_X,			/** X锁，表示可能修改所有数据，与所有锁都冲突 */
	IL_MAX			/** 用来表示意向锁类型个数，不是一种真正的锁 */
};

/** 多级锁使用情况统计 */
struct ILUsage {
	u64		m_lockCnt[IL_MAX];	/** 各种模式加锁次数 */
	u64		m_spinCnt;			/** 自旋次数 */
	u64		m_waitCnt;			/** 等待次数 */
	u64		m_waitTime;			/** 总的等待时间，单位毫秒数 */

	ILUsage() {
		memset(this, 0, sizeof(ILUsage));
	}
};

/** 所有锁类型的基类 */
struct Lock {
protected:
	Lock(const char *name, const char *file, uint line);
	virtual ~Lock();

public:
	const char	*m_name;	/** 锁对象名称，拷贝使用 */
	const char	*m_file;	/** 创建锁对象时的代码文件 */
	uint		m_line;		/** 创建锁对象时的行号 */
	static const bool CONFLICT_MATRIX[IL_MAX][IL_MAX];	/* 锁模式冲突矩阵 */
};

/** 锁模式 */
enum LockMode {
	None,			/** 不加锁 */
	Shared,			/** 共享锁 */
	Exclusived,		/** 互斥锁 */
};

/** 互斥锁统计对象扫描句柄 */
class MuScanHandle {
private:
	size_t		m_curPos;		/** 当前扫描位置 */
friend struct MutexUsage;
};

/** 所有锁使用情况统计类的基类 */
struct LockUsage {
	const char	*m_allocFile;	/** 锁对象分配时的代码文件名 */
	uint		m_allocLine;	/** 锁对象分配时的代码行号 */
	const char	*m_name;		/** 名称 */
	uint		m_instanceCnt;	/** 使用这一统计对象的锁对象个数 */
	bool		m_shared;		/** 是否被多个锁对象共享 */
	u64			m_spinCnt;		/** 不能马上加上锁，进行自旋的次数 */
	u64			m_waitCnt;		/** 自旋后仍不能加上锁，进行等待的次数 */
	u64			m_waitTime;		/** 等待时间，单位毫秒 */
	
	LockUsage(const char *name, const char *allocFile, uint allocLine);
	virtual ~LockUsage();
	virtual void print(std::ostream &out) const;
};

/** 互斥锁使用情况统计 */
struct MutexUsage: public LockUsage {
	u64			m_lockCnt;		/** 加锁操作次数 */

	MutexUsage(const char *name, const char *allocFile, uint allocLine);
	virtual void print(std::ostream &out) const;
	static void beginScan(MuScanHandle *h);
	static const MutexUsage* getNext(MuScanHandle *h);
	static void endScan(MuScanHandle *h);
	static void printAll(std::ostream &out);
};


#pragma region NEW_MUTEX

/** 新版互斥锁。参照5.4中InnoDB的实现 */
struct Mutex: public Lock {
public:
	Mutex(const char *name, const char *file, uint line);
	virtual ~Mutex();

	/**
	 * 加互斥锁
	 *
	 * @param file 文件名
	 * @param line 行号
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
	 * 加互斥锁，允许指定超时时间。
	 *
	 * @param timeoutMs	超时时间，若为<0则不超时，若为0则马上超时，相当于tryLock
	 * @param file		文件名
	 * @param line		行号
	 * @return 是否加锁成功。
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
	 * 尝试加互斥锁。
	 *
	 * @param file 文件名
	 * @param line 行号
	 * @return 是否加锁成功
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
	 * 判断是否已经被锁定。
	 *
	 * @return 是否被锁定。
	 */
	inline bool isLocked() const {
		return m_lockWord.get() > 0;
	}

	/**
	 * 解锁
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
	static const uint	SPIN_COUNT = 30;		/** 自旋时自旋多少轮 */
	static const uint	SPIN_DELAY = 6;		/** 自旋每一轮自旋多少时间（时钟周期数）*/

private:
	Atomic<int>			m_lockWord;	/** 加锁状态，0表示未被锁定，1表示被锁定 */
	Atomic<int>			m_waiting;	/** 正在等待加锁的线程数 */
	Event				m_event;	/** 用于唤醒等待线程的事件 */
	const char			*m_file;	/** 加锁代码文件名 */
	uint				m_line;		/** 加锁代码行号 */
#ifdef NTSE_SYNC_DEBUG
	uint				m_thread;	/** 加锁的线程ID */
#endif
	MutexUsage			*m_usage;	/** 使用情况统计 */
};

#pragma endregion NEW_MUTEX


/** 推荐使用下面的宏进行加/解锁，自动记录加锁时的文件和行号 */
#define LOCK(mutex)		(mutex)->lock(__FILE__, __LINE__)
#define UNLOCK(mutex)	(mutex)->unlock()
#define TRYLOCK(mutex)	(mutex)->tryLock(__FILE__, __LINE__)

/** 读写锁统计对象扫描句柄 */
class RwluScanHandle {
private:
	size_t		m_curPos;		/** 当前扫描位置 */
	friend struct RWLockUsage;
};

/** 读写锁使用情况统计 */
struct RWLockUsage: public LockUsage {
	u64			m_rlockCnt;		/** 加读锁操作次数 */
	u64			m_wlockCnt;		/** 加写锁操作次数 */
	
	RWLockUsage(const char *name, const char *allocFile, uint allocLine);
	virtual void print(std::ostream &out) const;
	static void beginScan(RwluScanHandle *h);
	static const RWLockUsage* getNext(RwluScanHandle *h);
	static void endScan(RwluScanHandle *h);
	static void printAll(std::ostream &out);
};


/** 新版读写锁。其实现参考了MySQL 5.4中InnoDB的读写锁，Thanks Google
 * 不支持递归加锁，即一个线程不能加多次锁再解多次锁
 */
struct RWLock: public Lock {
public:
	RWLock(const char *name, const char *file, uint line, bool preferWriter = false);
	~RWLock();
	/**
	 * 加读写锁
	 *
	 * @param lockMode 锁模式
	 * @param file 文件名
	 * @param line 行号
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
	 * 加读写锁，允许指定超时时间
	 *
	 * @param lockMode 锁模式
	 * @param timeoutMs <0表示不超时，=0时若不能立即加上锁马上超时，>0为毫秒为单位的超时时间
	 * @param file 文件名
	 * @param line 行号
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
	 * 尝试加读写锁
	 *
	 * @param lockMode 锁模式
	 * @param file 文件名
	 * @param line 行号
	 * @return 是否成功
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
	/** 释放读写锁
	 * @param 锁模式
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
	/** 得到锁模式对应的字符串表示
	 * @mode 锁模式
	 * @return 对应的字符串常量
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
	/** 尝试加读锁
	 * @param file 文件名
	 * @param line 行号
	 */
	inline bool tryLockR(const char *file, uint line) {
		if (decrLockWord(RLOCK_DEC)) {
			m_rlockFile = file;
			m_rlockLine = line;
			return true;
		}
		return false;
	}
	/** 尝试加写锁
	 * @param file 文件名
	 * @param line 行号
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
	/** 尝试从m_lockWord中减去amount
	 * @param amount 要减去的值
	 * @return 是否成功
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
	/** m_lockWord加上指定的值
	 * @param amount 要加上的值
	 * @return m_lockWord加上amount之后的新值
	 */
	inline int incrLockWord(int amount) {
		return m_lockWord.addAndGet(amount);
	}
	bool rlockConflict(const char *file, uint line, u64 absTimeout);
	bool wlockConflict(const char *file, uint line, u64 absTimeout);
	bool doWait(Event *evt, u64 absTimeout, long token);
	bool waitForReaders(u64 absTimeout, bool *hasWait);

private:
	static const int WLOCK_DEC = 1000000;	/** 加写锁时从m_lockWord中减去这个值 */
	static const int RLOCK_DEC = 1;			/** 加读锁时从m_lockWord中减去这个值 */
	static const u64 NEVER_TIMEOUT = (u64)-1;	/** 永不超时常量 */

	bool			m_preferWriter;	/** 是否给予写锁以更高优先级 */
	Atomic<int>		m_lockWord;		/** 加锁状态
									 * 初始值: WLOCK_DEC，表示没有加锁
									 * 0 < m_lockWord < WLOCK_DEC: 加了读锁，并且没有人想加写锁
									 * = 0: 加了写锁
									 * -WLOCK_DEC < m_lockWord < 0: 加了读锁，并且有人在等待加写锁
									 */
	Atomic<int>		m_waiters;		/** 是否有人在等待m_forOthers事件，不考虑m_forNextWriter事件，
									 * 这只是个布尔值，不表示有多少人在等待，因此唤醒时是使用
									 * 广播形式
									 * 带超时的等待在超时时也不将本值设为0，因为可能会有其它
									 * 人也在等待
									 */
	Event		m_forNextWriter;/** 已经被选定成为加写锁的唯一候选人等待这一事件 */
	Event		m_forOthers;	/** 其它人等待这一事件 */
	const char		*m_rlockFile;	/** 最后一个读锁加锁代码文件名 */
	uint			m_rlockLine;	/** 最后一个读锁加锁代码行号 */
	const char		*m_wlockFile;	/** 最后一个写锁加锁代码文件名 */
	uint			m_wlockLine;	/** 最后一个写锁加锁代码行号 */
	RWLockUsage		*m_usage;		/** 使用情况统计 */
};

/** 推荐使用下面的宏进行加/解锁，自动记录加锁时的文件和行号 */
#define RWLOCK(rwlock, mode)	(rwlock)->lock(mode, __FILE__, __LINE__)
#define RWTIMEDLOCK(rwLock, mode, timeoutMs) (rwLock)->timedLock(mode, timeoutMs, __FILE__, __LINE__)
#define RWTRYLOCK(rwlock, mode)	(rwlock)->tryLock(mode, __FILE__, __LINE__)
#define RWUNLOCK(rwlock, mode)	(rwlock)->unlock(mode)

/** 锁拥有者信息 */
struct LockerInfo {
	static const u16 WAIT_FOR_NOTHING = 0xFFFF;	/** 表示不在等待 */

	u16			m_mode;		/** 锁模式 */
	u16			m_waitFor;	/** 等待哪个用户，若为WAIT_FOR_NOTHING表示不在等待 */
	const char	*m_file;	/** 加锁时的代码文件 */
	uint		m_line;		/** 加锁时的代码行号 */
	uint		m_threadId;	/** 操作系统线程号 */

	LockerInfo();
	void set(u16 mode, const char *file, uint line, uint threadId, u16 waitFor);
	void reset();
};

/** 限制用户数的读写锁，操作时需要指定用户ID */
struct LURWLock: public Lock {
public:
	LURWLock(u16 maxUserId, const char *name, const char *file, uint line, bool preferWriter = false);
	virtual ~LURWLock();
	/**
	 * 加读写锁
	 *
	 * @param userId 用户ID
	 * @param lockMode 锁模式
	 * @param file 文件名
	 * @param line 行号
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
	 * 加读写锁，允许指定超时时间
	 *
	 * @param userId 用户ID
	 * @param lockMode 锁模式
	 * @param timeoutMs <0表示不超时，=0时若不能立即加上锁马上超时，>0为毫秒为单位的超时时间
	 * @param file 文件名
	 * @param line 行号
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
	 * 尝试加读写锁
	 *
	 * @param userId 用户ID
	 * @param lockMode 锁模式
	 * @param file 文件名
	 * @param line 行号
	 * @return 是否成功
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
	 * 尝试进行锁升级
	 *
	 * @param userId 用户ID
	 * @param file 文件名
	 * @param line 行号
	 * @return 是否成功
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
	 * 判断读写锁是否已经被用指定的模式锁定
	 *
	 * @param lockMode 锁模式
	 * @return 读写锁是否已经被用指定的模式锁定
	 */
	inline bool isLocked(LockMode lockMode) const {
		return m_rwLock.isLocked(lockMode);
	}
	/**
	 * 获得当前加的读写锁的模式
	 * @return 
	 */
	inline LockMode getLockMode() const {
		return m_rwLock.getLockMode();
	}
	/** 释放读写锁
	 * @param userId 用户ID
	 * @param 锁模式
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
	 * 获得使用情况统计
	 *
	 * @return 使用情况统计
	 */
	inline const RWLockUsage* getUsage() const {
		return m_rwLock.getUsage();
	}

private:
#ifdef NTSE_SYNC_DEBUG
	u16 calcWaitFor(u16 userId, LockMode mode);
#endif

private:
	RWLock		m_rwLock;		/** 读写锁 */
#ifdef NTSE_SYNC_DEBUG
	u16			m_maxUserId;	/** 最大的用户ID */
	LockerInfo	*m_userInfos;	/** 各用户加锁情况 */
#endif
};

/** 意向锁统计对象扫描句柄 */
class IntentionLockUsageScanHandle {
private:
	size_t		m_curPos;		/** 当前扫描位置 */
	friend struct IntentionLockUsage;
};

/** 意向锁使用情况统计 */
struct IntentionLockUsage: public LockUsage {
	u64			m_lockCnt[IL_MAX];		/** 各种模式加锁操作次数 */
	u64			m_failCnt;				/** 锁超时失败次数*/

	IntentionLockUsage(const char *name, const char *allocFile, uint allocLine);
	virtual void print(std::ostream &out) const;
	static void beginScan(IntentionLockUsageScanHandle *h);
	static const IntentionLockUsage* getNext(IntentionLockUsageScanHandle *h);
	static void endScan(IntentionLockUsageScanHandle *h);
	static void printAll(std::ostream &out);
};

/** 标准的多级锁（意向锁） */
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
	u16			m_maxUsers;	/** 使用该锁的用户个数 */
	Mutex		m_lock;		/** 保护并发的锁 */
	//ILUsage		m_usage;	/** 使用情况统计 */
	LockerInfo	*m_state;	/** 各个用户的加锁情况 */
	uint		m_wants[IL_MAX];	/** 想加各种锁模式的用户数，包括已经加上锁的 */
	uint		m_gots[IL_MAX];		/** 已经加上各种锁模式的用户数 */
	Event		m_event;	/** 用于实现等待通知 */
	Atomic<int>	m_waiting;	/** 正在等待的用户数 */

	IntentionLockUsage	*m_usage;	/** 使用情况统计 */
};

/** 退出代码块时自动放锁 */
struct MutexGuard {
public:
	/**
	 * 构造函数，加锁
	 *
	 * @param lock 互斥量	 
	 * @param file 文件名
	 * @param line 行号
	 */
	MutexGuard(Mutex *lock, const char *file, uint line) {
		m_lock = lock;
		m_locked = false;
		m_lock->lock(file, line);
		m_locked = true;
	}

	/**
	 * 析构函数，若被锁定则解锁
	 */
	~MutexGuard() {
		if (m_locked)
			UNLOCK(m_lock);
	}

	/**
	 * 加锁
	 * @pre 已经调用unlock放锁
	 * @param file 文件名
	 * @param line 行号
	 */	
	void lock(const char *file, uint line) {
		assert(!m_locked);
		m_lock->lock(file, line);
		m_locked = true;
	}

	/** 
	 * 解锁
	 * @pre 没有调用unlock解锁若已经调用lock加了锁
	 */
	void unlock() {
		assert(m_locked);
		UNLOCK(m_lock);
		m_locked = false;
	}
private:
	Mutex	*m_lock;	/** 互斥量 */
	bool	m_locked;	/** 是否被我加了锁 */
};

/** 退出代码块时自动放锁 */
struct RWLockGuard {
public:
	/**
	 * 构造函数，加锁
	 *
	 * @param lock 读写锁
	 * @param lockMode 锁模式
	 * @param file 文件名
	 * @param line 行号
	 */
	RWLockGuard(RWLock *lock, LockMode lockMode, const char *file, uint line) {
		m_lock = lock;
		m_lockMode = lockMode;
		m_lock->lock(m_lockMode, file, line);
		m_locked = true;
	}

	/**
	 * 析构函数，解锁
	 */
	~RWLockGuard() {
		if (m_locked)
			RWUNLOCK(m_lock, m_lockMode);
	}

	/**
	 * 加锁
	 * @pre 已经调用unlock放锁
	 * @param file 文件名
	 * @param line 行号
	 */	
	void lock(const char *file, uint line) {
		assert(!m_locked);
		m_lock->lock(m_lockMode, file, line);
		m_locked = true;
	}

	/** 
	 * 解锁
	 * @pre 没有调用unlock解锁若已经调用lock加了锁
	 */
	void unlock() {
		assert(m_locked);
		RWUNLOCK(m_lock, m_lockMode);
		m_locked = false;
	}
private:
	RWLock		*m_lock;	/** 读写锁 */
	LockMode	m_lockMode;	/** 锁模式 */
	bool		m_locked;	/** 是否被我加了锁 */
};

/**
 * 同步块，使用方法类似于Java中的synchronized关键字。例
 *
 * Mutex lock;
 * SYNC(&lock, 
 *     // 代码块
 * );
 */
#define SYNC(mutex, block)									\
	do {													\
		MutexGuard __syncLock(mutex, __FILE__, __LINE__);	\
		block;												\
	} while(0);

}

#endif
