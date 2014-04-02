/**
 * 线程
 *
 * @author 汪源(wangyuan@corp.netease.com, wy@163.org)
 */

#ifndef _NTSE_THREAD_H_
#define _NTSE_THREAD_H_

#include "util/Sync.h"
#include "misc/Global.h"
#ifdef WIN32
#include <winbase.h>
#endif

namespace ntse {

/** 线程 */
class Thread {
public:
	Thread(const char *name);
	virtual ~Thread();
	uint getId();
	void start();
	virtual void run() = 0;
	bool isAlive();
	bool join(int timeoutMs = -1);
	void main();
    static Thread* currentThread();
	static uint currentOSThreadID();
#ifdef NTSE_UNIT_TEST
	void enableSyncPoint(SyncPoint syncPoint);
	void disableSyncPoint(SyncPoint syncPoint);
	void waitSyncPoint(SyncPoint syncPoint);
	void notifySyncPoint(SyncPoint syncPoint);
	bool isWaitingSyncPoint(SyncPoint syncPoint);
	void joinSyncPoint(SyncPoint syncPoint);
	void setWaitingLock(void *lock);
	bool isWaitingLock(void *lock);
	void joinLock(void *lock);
#endif
	static uint delay(uint cc);
	static void yield();

public:
	static void msleep(uint millis);
	
protected:
	virtual void doRun();

protected:
	static Atomic<int>	m_nextId;
	char	*m_name;				/** 线程名 */
	uint	m_id;					/** 线程ID，由操作系统底层实现决定 */
	Mutex	*m_mutex;				/** 用于保护是否多个运行状态的锁 */
	Event	m_startEvent;			/** 用于等待线程真正启动的事件 */
	bool	m_started;				/** 线程是否已经启动 */
	bool	m_joined;				/** 线程是否已经回收 */
	bool	m_alive;				/** 线程是否活跃，即线程是否在其主函数中 */
#ifdef WIN32
	HANDLE	m_osThread;				/** 操作系统线程句柄 */
#else
	pthread_t	m_osThread;			/** 操作系统线程句柄 */
#endif
#ifdef NTSE_UNIT_TEST
	bool	m_syncPointStates[SP_MAX];	/** 各同步点是否生效状态 */
	Event	m_spEvent;				/** 用于实现同步点等待通告的事件 */
	SyncPoint	m_waitingSp;		/** 正在等待的同步点 */
	void	*m_waitingLock;			/** 正在等待的锁 */
#endif
	Mutex	*m_joinMutex;			/** 保护并发调用join的锁 */
};

/** 任务线程 */
class Task: public Thread {
public:
	Task(const char *name, uint interval);
	virtual ~Task();
	/** 只会在任务线程创建后调用一次的函数 */
	virtual void setUp() {}
	/** 只会在任务线程退出之前调用一次的函数 */
	virtual void tearDown() {}
	/**	避免子类在析构时，调用Thread::run纯虚函数 */
	virtual void run()	{}
	void stop();
	void pause(bool waitIfRunning = false);
	void resume();
	void signal();
	bool isPaused();
	bool isRunning();
	uint getInterval();
	void setInterval(uint newInterval);
	virtual void doRun();

protected:
	uint	m_interval;		/** 运行周期 */
	bool	m_paused;		/** 任务是否被暂停 */
	bool	m_stopped;		/** 任务是否被停止 */
	bool	m_running;		/** 是否现在正在执行任务操作 */
	bool	m_signaled;		/** 是否有激活请求 */
	Event	m_evt;			/** 用于等待和激活的事件 */
};

}

#endif
