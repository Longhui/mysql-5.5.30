/**
 * �߳�
 *
 * @author ��Դ(wangyuan@corp.netease.com, wy@163.org)
 */

#ifndef _NTSE_THREAD_H_
#define _NTSE_THREAD_H_

#include "util/Sync.h"
#include "misc/Global.h"
#ifdef WIN32
#include <winbase.h>
#endif

namespace ntse {

/** �߳� */
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
	char	*m_name;				/** �߳��� */
	uint	m_id;					/** �߳�ID���ɲ���ϵͳ�ײ�ʵ�־��� */
	Mutex	*m_mutex;				/** ���ڱ����Ƿ�������״̬���� */
	Event	m_startEvent;			/** ���ڵȴ��߳������������¼� */
	bool	m_started;				/** �߳��Ƿ��Ѿ����� */
	bool	m_joined;				/** �߳��Ƿ��Ѿ����� */
	bool	m_alive;				/** �߳��Ƿ��Ծ�����߳��Ƿ������������� */
#ifdef WIN32
	HANDLE	m_osThread;				/** ����ϵͳ�߳̾�� */
#else
	pthread_t	m_osThread;			/** ����ϵͳ�߳̾�� */
#endif
#ifdef NTSE_UNIT_TEST
	bool	m_syncPointStates[SP_MAX];	/** ��ͬ�����Ƿ���Ч״̬ */
	Event	m_spEvent;				/** ����ʵ��ͬ����ȴ�ͨ����¼� */
	SyncPoint	m_waitingSp;		/** ���ڵȴ���ͬ���� */
	void	*m_waitingLock;			/** ���ڵȴ����� */
#endif
	Mutex	*m_joinMutex;			/** ������������join���� */
};

/** �����߳� */
class Task: public Thread {
public:
	Task(const char *name, uint interval);
	virtual ~Task();
	/** ֻ���������̴߳��������һ�εĺ��� */
	virtual void setUp() {}
	/** ֻ���������߳��˳�֮ǰ����һ�εĺ��� */
	virtual void tearDown() {}
	/**	��������������ʱ������Thread::run���麯�� */
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
	uint	m_interval;		/** �������� */
	bool	m_paused;		/** �����Ƿ���ͣ */
	bool	m_stopped;		/** �����Ƿ�ֹͣ */
	bool	m_running;		/** �Ƿ���������ִ��������� */
	bool	m_signaled;		/** �Ƿ��м������� */
	Event	m_evt;			/** ���ڵȴ��ͼ�����¼� */
};

}

#endif
