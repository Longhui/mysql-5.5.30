/**
 * 线程实例
 *
 * @author 汪源(wangyuan@corp.netease.com, wy@163.org)
 */

#include "util/Thread.h"
#include "util/System.h"
#include "util/Sync.h"
#ifndef WIN32
#include <time.h>
#endif
#include <iostream>

using namespace ntse;

namespace ntse {

/** 当前线程 */
static TLS Thread* s_currentThread;

Atomic<int> Thread::m_nextId;

// 这里代码重复是为了让SourceInsight能分析出代码结构
#ifdef WIN32
DWORD WINAPI threadProc(LPVOID arg) {
	Thread *thread = (Thread *)arg;
	s_currentThread = thread;
	thread->main();
	return 0;
}
#else
void* threadProc(void *arg) {
	Thread *thread = (Thread *)arg;
	s_currentThread = thread;
	thread->main();
	return 0;
}
#endif

/** 线程入口函数 */
void Thread::main() {
	SYNC(m_mutex,
		if (m_joined)	// 如果线程没有启动就被停止，则不执行线程处理函数
			return;
		m_alive = true;
	);
	m_startEvent.signal();
	doRun();
	m_alive = false;
}

/**
 * 构造函数
 *
 * @param name 线程名称。使用拷贝
 */
Thread::Thread(const char *name) {
	m_name = System::strdup(name);
	m_id = m_nextId.incrementAndGet();
	m_alive = false;
	m_started = false;
	m_joined = false;
#ifdef NTSE_UNIT_TEST
	memset(m_syncPointStates, 0, sizeof(bool) * sizeof(m_syncPointStates));
	m_waitingSp = SP_NONE;
	m_waitingLock = NULL;
#endif
	m_joinMutex = new Mutex("Thread::joinMutex", __FILE__, __LINE__);
	m_mutex = new Mutex("Thread::mutex", __FILE__, __LINE__);
}

Thread::~Thread() {
	join();
	delete []m_name;
	delete m_joinMutex;
	delete m_mutex;
}

/**
 * 返回线程ID
 *
 * @return 线程ID
 */
uint Thread::getId() {
	return m_id;
}

/**
 * 启动线程。如果线程已经被join回收，则启动线程时不执行任何操作
 * @pre 线程没有启动，即不能重复启动线程
 */
void Thread::start() {
	assert(!m_started);
	SYNC(m_mutex,
		if (m_joined)
			return;
	);
#ifdef WIN32
	m_osThread = ::CreateThread(0, 0, threadProc, this, 0, NULL);
#else

	pthread_attr_t attr;
	pthread_attr_init(&attr);
	pthread_attr_setstacksize(&attr, 256 * 1024);
	pthread_create(&m_osThread, &attr, threadProc, this);
#endif
	m_started = true;
	// 等待线程真正开始，否则线程可能在开始运行前被join然后被delete
	// 然后系统再进入线程入口函数，访问已被delete的对象
	m_startEvent.wait(0);
}

/**
 * 等待指定的线程结束，若线程没有启动或线程已经停止，直接返回
 *
 * @param timeoutMs 超时时间，单位为毫秒，若<=0则不超时
 * @return 指定线程是否结束
 */
bool Thread::join(int timeoutMs) {
	MutexGuard guard(m_mutex, __FILE__, __LINE__);
	if (!m_started) {			// 还没开始运行就join，就不跑了
		m_joined = true;
		return true;
	}
	guard.unlock();

	u64 before = System::currentTimeMillis();
	if (!m_joinMutex->timedLock(timeoutMs, __FILE__, __LINE__))
		return false;
	if (m_joined) {				// 已经join成功
		UNLOCK(m_joinMutex);
		return true;
	}
	u64 now = System::currentTimeMillis();
	if (timeoutMs > 0) {
		if ((now - before) >= (u64)timeoutMs) {
			// 加joinMutex用时若已经超过了指定的超时时间，则直接返回false
			// 不再去调用系统的join了，以保证指定的超时控制生效
			UNLOCK(m_joinMutex);
			return false;
		}
		// 修正超时控制，控制总的用时
		timeoutMs = timeoutMs - (int)(now - before);
	}
#ifdef WIN32
	DWORD code = ::WaitForSingleObject(m_osThread, (timeoutMs <= 0)? INFINITE: timeoutMs);
	m_joined = code != WAIT_FAILED && code != WAIT_TIMEOUT;
#else
	if (timeoutMs <= 0) {
		char *r;
		m_joined = pthread_join(m_osThread, (void **)&r) == 0;
	} else {
		u64 before, now;
		u64 sleepInterval = timeoutMs / 10;
		before = System::currentTimeMillis();
		while (m_alive) {
			msleep(sleepInterval);
			now = System::currentTimeMillis();
			if ((int)(now - before) >= timeoutMs)
				return false;
		}
		char *r;
		m_joined = pthread_join(m_osThread, (void **)&r) == 0;
	}
#endif
	UNLOCK(m_joinMutex);
	// 这里返回m_joined而不是我调用系统join的结果，由于只可能将
	// m_joined从false改成true，没有问题
	return m_joined;
}

/** 
 * 让当前线程休眠指定的时间
 *
 * @param millis 要休眠的时间，单位为毫秒
 */
void Thread::msleep(uint millis) {
#ifdef WIN32
	Sleep(millis);
#else
	struct timespec reg;
	reg.tv_sec = millis / 1000;
	reg.tv_nsec = (millis % 1000) * 1000000;
	nanosleep(&reg, NULL);
#endif
}

/**
 * 返回线程是否正在运行
 *
 * @return 线程是否正在运行
 */
bool Thread::isAlive() {
	return m_alive;
}

void Thread::doRun() {
	run();
}

/**
 * 获得当前线程
 *
 * @return 当前线程
 */
Thread* Thread::currentThread() {
	return s_currentThread;
}

/** 得到当前线程在操作系统内的ID
 *
 * @return 当前线程在操作系统内的ID
 */
uint Thread::currentOSThreadID() {
#ifdef WIN32
	return GetCurrentThreadId();
#else
	return pthread_self();
#endif
}

/** 模拟一段CPU负载
 * @param cc 要模拟的CPU负载占用的时钟周期数
 * @return 返回值没什么意义，其作用是防止本函数中的代码被优化器优化掉
 */
uint Thread::delay(uint cc) {
	uint i, j = 0;
	for (i = 0; i < cc / 2; i++) {
		PAUSE_INSTRUCTION();
		j += i;
	}
	return j;
}

/** 建议操作系统在当前时间片剩余时间中放弃执行本线程 */
void Thread::yield() {
#ifdef WIN32
	Sleep(0);
#else
	pthread_yield();
#endif
}

#ifdef NTSE_UNIT_TEST
/**
 * 激活一个同步点
 *
 * @param syncPoint 同步点
 */
void Thread::enableSyncPoint(SyncPoint syncPoint) {
	assert(syncPoint < SP_MAX);
	m_syncPointStates[syncPoint] = true;
}

/**
 * 取消一个同步点
 *
 * @param syncPoint 同步点
 */
void Thread::disableSyncPoint(SyncPoint syncPoint) {
	assert(syncPoint < SP_MAX);
	m_syncPointStates[syncPoint] = false;
}

/**
 * 等待同步点被通告
 *
 * @param syncPoint 同步点
 */
void Thread::waitSyncPoint(SyncPoint syncPoint) {
	assert(syncPoint < SP_MAX);
	if (m_syncPointStates[syncPoint]) {
		m_waitingSp = syncPoint;
		do {
			m_spEvent.wait(0);
		} while (m_waitingSp == syncPoint);
	}
}

/**
 * 同步点通告
 *
 * @param syncPoint 同步点
 */
void Thread::notifySyncPoint(SyncPoint syncPoint) {
	assert(syncPoint < SP_MAX);
	if (m_waitingSp == syncPoint) {
		m_waitingSp = SP_NONE;
		m_spEvent.signal();
	}
}

/**
 * 判断线程是否等待在指定同步点上
 *
 * @param syncPoint 同步点
 */
bool Thread::isWaitingSyncPoint(SyncPoint syncPoint) {
	assert(syncPoint < SP_MAX);
	return m_waitingSp == syncPoint;
}

/**
 * 直到线程等待在指定的同步点上才返回
 * 
 * @param syncPoint 同步点
 */
void Thread::joinSyncPoint(SyncPoint syncPoint) {
	assert(syncPoint < SP_MAX);
	while (true) {
		if (isWaitingSyncPoint(syncPoint))
			return;
		msleep(10);
	}
}

/**
 * 设置线程正在等待的锁对象
 * 
 * @param lock 锁对象，可能是Mutex/RWLock/BiasedRWLock/ThinMutex/ThinRWLock等
 */
void Thread::setWaitingLock(void *lock) {
	m_waitingLock = lock;
}

/**
 * 判断线程是否在等待指定的锁对象
 * 
 * @param lock 锁对象，见setWaitingLock说明
 */
bool Thread::isWaitingLock(void *lock) {
	return m_waitingLock == lock;
}

/**
 * 直到线程等待在指定的锁对象上才返回
 * 
 * @param lock 锁对象，见setWaitingLock说明
 */
void Thread::joinLock(void *lock) {
	while (true) {
		if (isWaitingLock(lock))
			return;
		msleep(100);
	}
}

#endif

/**
 * 创建一个任务线程，任务将在interval时间之后第一次执行，以后每interval时间执行一次
 *
 * @param name 任务线程名称
 * @param interval 运行周期，单位毫秒
 */
Task::Task(const char *name, uint interval): Thread(name) {
	m_interval = interval;
	m_paused = false;
	m_running = false;
	m_signaled = false;
	m_stopped = false;
}

/** 析构函数，自动停止任务线程 */
Task::~Task() {
	stop();
}

void Task::doRun() {
	setUp();

	int waitTime = m_interval;
	while (true) {
		if (waitTime > 0) {
			u64 beforeWait = System::currentTimeMillis();
			bool receiveSignal = m_evt.wait(waitTime);
			if (receiveSignal && !m_signaled && !m_stopped) {
				//如果等待事件是被系统激活的话，则还需要继续等待
				waitTime = waitTime - (int)(System::currentTimeMillis() - beforeWait);
				continue;
			}
		}
		if (m_stopped)
			break;
		if (m_paused) {
			waitTime = m_interval;
			continue;
		}
		SYNC(m_mutex,
			if (m_signaled)
				m_signaled = false;
			m_running = true;
		);

		u64 before, after;
		before = System::currentTimeMillis();
		run();
		after = System::currentTimeMillis();
		if (m_signaled)
			waitTime = 0;
		else if (after - before < m_interval)
			waitTime = (int)(m_interval - (after - before));
		else
			waitTime = 0;

		SYNC(m_mutex,
			m_running = false;
		);
	}

	tearDown();
}

/**
 * 停止任务，本函数返回时确保任务已经终止运行
 */
void Task::stop() {
	SYNC(m_mutex,
		if (!m_alive || m_stopped)
			return;
		m_stopped = true;
	);
	m_evt.signal();
	join();
}

/**
 * 暂停任务
 *
 * @param waitIfRunning 若为true，则在任务正在执行时等待，从而保证本函数返回时任务不可能在执行，若为false，
 *   则只设置标志，本函数返回时，任务可能还在执行过程中
 */
void Task::pause(bool waitIfRunning) {
	m_paused = true;
	while (waitIfRunning && m_running)
		msleep(100);
}

/** 
 * 恢复被暂停的任务
 */
void Task::resume() {
	m_paused = false;
}

/**
 * 立即激活任务。如果任务被暂停，这一函数同时也恢复了被暂停的任务
 */
void Task::signal() {
	m_paused = false;

	SYNC(m_mutex,
		if (!m_alive || m_stopped)
			return;
		if (m_running)
			m_signaled = true;
		else if (!m_signaled) {
			m_signaled = true;
			m_evt.signal();
		}
	);
}

/**
 * 返回任务是否被暂停
 *
 * @return 任务是否被暂停
 */
bool Task::isPaused() {
	return m_paused;
}

/**
 * 返回任务是否正在执行
 *
 * @return 任务是否正在执行
 */
bool Task::isRunning() {
	return m_running;
}

/**
 * 返回任务运行周期
 *
 * @return 任务运行周期，单位毫秒
 */
uint Task::getInterval() {
	return m_interval;
}

/**
 * 修改任务执行周期
 *
 * @param 新的任务执行周期
 */
void Task::setInterval(uint newInterval) {
	m_interval = newInterval;
}

}

