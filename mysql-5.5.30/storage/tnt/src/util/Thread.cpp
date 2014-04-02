/**
 * �߳�ʵ��
 *
 * @author ��Դ(wangyuan@corp.netease.com, wy@163.org)
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

/** ��ǰ�߳� */
static TLS Thread* s_currentThread;

Atomic<int> Thread::m_nextId;

// ��������ظ���Ϊ����SourceInsight�ܷ���������ṹ
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

/** �߳���ں��� */
void Thread::main() {
	SYNC(m_mutex,
		if (m_joined)	// ����߳�û�������ͱ�ֹͣ����ִ���̴߳�����
			return;
		m_alive = true;
	);
	m_startEvent.signal();
	doRun();
	m_alive = false;
}

/**
 * ���캯��
 *
 * @param name �߳����ơ�ʹ�ÿ���
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
 * �����߳�ID
 *
 * @return �߳�ID
 */
uint Thread::getId() {
	return m_id;
}

/**
 * �����̡߳�����߳��Ѿ���join���գ��������߳�ʱ��ִ���κβ���
 * @pre �߳�û���������������ظ������߳�
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
	// �ȴ��߳�������ʼ�������߳̿����ڿ�ʼ����ǰ��joinȻ��delete
	// Ȼ��ϵͳ�ٽ����߳���ں����������ѱ�delete�Ķ���
	m_startEvent.wait(0);
}

/**
 * �ȴ�ָ�����߳̽��������߳�û���������߳��Ѿ�ֹͣ��ֱ�ӷ���
 *
 * @param timeoutMs ��ʱʱ�䣬��λΪ���룬��<=0�򲻳�ʱ
 * @return ָ���߳��Ƿ����
 */
bool Thread::join(int timeoutMs) {
	MutexGuard guard(m_mutex, __FILE__, __LINE__);
	if (!m_started) {			// ��û��ʼ���о�join���Ͳ�����
		m_joined = true;
		return true;
	}
	guard.unlock();

	u64 before = System::currentTimeMillis();
	if (!m_joinMutex->timedLock(timeoutMs, __FILE__, __LINE__))
		return false;
	if (m_joined) {				// �Ѿ�join�ɹ�
		UNLOCK(m_joinMutex);
		return true;
	}
	u64 now = System::currentTimeMillis();
	if (timeoutMs > 0) {
		if ((now - before) >= (u64)timeoutMs) {
			// ��joinMutex��ʱ���Ѿ�������ָ���ĳ�ʱʱ�䣬��ֱ�ӷ���false
			// ����ȥ����ϵͳ��join�ˣ��Ա�ָ֤���ĳ�ʱ������Ч
			UNLOCK(m_joinMutex);
			return false;
		}
		// ������ʱ���ƣ������ܵ���ʱ
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
	// ���ﷵ��m_joined�������ҵ���ϵͳjoin�Ľ��������ֻ���ܽ�
	// m_joined��false�ĳ�true��û������
	return m_joined;
}

/** 
 * �õ�ǰ�߳�����ָ����ʱ��
 *
 * @param millis Ҫ���ߵ�ʱ�䣬��λΪ����
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
 * �����߳��Ƿ���������
 *
 * @return �߳��Ƿ���������
 */
bool Thread::isAlive() {
	return m_alive;
}

void Thread::doRun() {
	run();
}

/**
 * ��õ�ǰ�߳�
 *
 * @return ��ǰ�߳�
 */
Thread* Thread::currentThread() {
	return s_currentThread;
}

/** �õ���ǰ�߳��ڲ���ϵͳ�ڵ�ID
 *
 * @return ��ǰ�߳��ڲ���ϵͳ�ڵ�ID
 */
uint Thread::currentOSThreadID() {
#ifdef WIN32
	return GetCurrentThreadId();
#else
	return pthread_self();
#endif
}

/** ģ��һ��CPU����
 * @param cc Ҫģ���CPU����ռ�õ�ʱ��������
 * @return ����ֵûʲô���壬�������Ƿ�ֹ�������еĴ��뱻�Ż����Ż���
 */
uint Thread::delay(uint cc) {
	uint i, j = 0;
	for (i = 0; i < cc / 2; i++) {
		PAUSE_INSTRUCTION();
		j += i;
	}
	return j;
}

/** �������ϵͳ�ڵ�ǰʱ��Ƭʣ��ʱ���з���ִ�б��߳� */
void Thread::yield() {
#ifdef WIN32
	Sleep(0);
#else
	pthread_yield();
#endif
}

#ifdef NTSE_UNIT_TEST
/**
 * ����һ��ͬ����
 *
 * @param syncPoint ͬ����
 */
void Thread::enableSyncPoint(SyncPoint syncPoint) {
	assert(syncPoint < SP_MAX);
	m_syncPointStates[syncPoint] = true;
}

/**
 * ȡ��һ��ͬ����
 *
 * @param syncPoint ͬ����
 */
void Thread::disableSyncPoint(SyncPoint syncPoint) {
	assert(syncPoint < SP_MAX);
	m_syncPointStates[syncPoint] = false;
}

/**
 * �ȴ�ͬ���㱻ͨ��
 *
 * @param syncPoint ͬ����
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
 * ͬ����ͨ��
 *
 * @param syncPoint ͬ����
 */
void Thread::notifySyncPoint(SyncPoint syncPoint) {
	assert(syncPoint < SP_MAX);
	if (m_waitingSp == syncPoint) {
		m_waitingSp = SP_NONE;
		m_spEvent.signal();
	}
}

/**
 * �ж��߳��Ƿ�ȴ���ָ��ͬ������
 *
 * @param syncPoint ͬ����
 */
bool Thread::isWaitingSyncPoint(SyncPoint syncPoint) {
	assert(syncPoint < SP_MAX);
	return m_waitingSp == syncPoint;
}

/**
 * ֱ���̵߳ȴ���ָ����ͬ�����ϲŷ���
 * 
 * @param syncPoint ͬ����
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
 * �����߳����ڵȴ���������
 * 
 * @param lock �����󣬿�����Mutex/RWLock/BiasedRWLock/ThinMutex/ThinRWLock��
 */
void Thread::setWaitingLock(void *lock) {
	m_waitingLock = lock;
}

/**
 * �ж��߳��Ƿ��ڵȴ�ָ����������
 * 
 * @param lock �����󣬼�setWaitingLock˵��
 */
bool Thread::isWaitingLock(void *lock) {
	return m_waitingLock == lock;
}

/**
 * ֱ���̵߳ȴ���ָ�����������ϲŷ���
 * 
 * @param lock �����󣬼�setWaitingLock˵��
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
 * ����һ�������̣߳�������intervalʱ��֮���һ��ִ�У��Ժ�ÿintervalʱ��ִ��һ��
 *
 * @param name �����߳�����
 * @param interval �������ڣ���λ����
 */
Task::Task(const char *name, uint interval): Thread(name) {
	m_interval = interval;
	m_paused = false;
	m_running = false;
	m_signaled = false;
	m_stopped = false;
}

/** �����������Զ�ֹͣ�����߳� */
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
				//����ȴ��¼��Ǳ�ϵͳ����Ļ�������Ҫ�����ȴ�
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
 * ֹͣ���񣬱���������ʱȷ�������Ѿ���ֹ����
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
 * ��ͣ����
 *
 * @param waitIfRunning ��Ϊtrue��������������ִ��ʱ�ȴ����Ӷ���֤����������ʱ���񲻿�����ִ�У���Ϊfalse��
 *   ��ֻ���ñ�־������������ʱ��������ܻ���ִ�й�����
 */
void Task::pause(bool waitIfRunning) {
	m_paused = true;
	while (waitIfRunning && m_running)
		msleep(100);
}

/** 
 * �ָ�����ͣ������
 */
void Task::resume() {
	m_paused = false;
}

/**
 * ���������������������ͣ����һ����ͬʱҲ�ָ��˱���ͣ������
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
 * ���������Ƿ���ͣ
 *
 * @return �����Ƿ���ͣ
 */
bool Task::isPaused() {
	return m_paused;
}

/**
 * ���������Ƿ�����ִ��
 *
 * @return �����Ƿ�����ִ��
 */
bool Task::isRunning() {
	return m_running;
}

/**
 * ����������������
 *
 * @return �����������ڣ���λ����
 */
uint Task::getInterval() {
	return m_interval;
}

/**
 * �޸�����ִ������
 *
 * @param �µ�����ִ������
 */
void Task::setInterval(uint newInterval) {
	m_interval = newInterval;
}

}

