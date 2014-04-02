/**
 * �����̲߳���
 *
 * @author ��Դ(wangyuan@corp.netease.com, wy@163.org)
 */

#include <cppunit/config/SourcePrefix.h>
#include "util/TestThread.h"
#include "util/Thread.h"
#include "util/System.h"
#include "misc/EventMonitor.h"

using namespace ntse;


class SimpleThread: public Thread {
public:
	SimpleThread(): Thread("SimpleThread") {
	}

	virtual void run() {
		m_currentThread = Thread::currentThread();
		msleep(100);
	}

	Thread	*m_currentThread;
};

const char* ThreadTestCase::getName() {
	return "Thread test";
}

const char* ThreadTestCase::getDescription() {
	return "Test Thread, Task.";
}

bool ThreadTestCase::isBig() {
	return false;
}

void ThreadTestCase::testThread() {
	// �������ܲ���
	SimpleThread simpleThread;
	CPPUNIT_ASSERT(!simpleThread.isAlive());
	simpleThread.start();
	Thread::msleep(50);
	CPPUNIT_ASSERT(simpleThread.isAlive());
	CPPUNIT_ASSERT(simpleThread.join());
	CPPUNIT_ASSERT(simpleThread.join());
	CPPUNIT_ASSERT(!simpleThread.isAlive());

	// ����msleep����
	u64 before, after;
	before = System::currentTimeMillis();
	Thread::msleep(500);
	after = System::currentTimeMillis();
	CPPUNIT_ASSERT((after - before) >= 400);
	CPPUNIT_ASSERT((after - before) <= 600);

	before = System::currentTimeMillis();
	Thread::msleep(2500);
	after = System::currentTimeMillis();
	CPPUNIT_ASSERT((after - before) >= 2400);
	CPPUNIT_ASSERT((after - before) <= 2600);

	// ����joinʱ�߳��Ѿ��������ʱ�����
	SimpleThread simpleThread2;
	simpleThread2.start();
	while (simpleThread2.isAlive())
		Thread::msleep(200);
	CPPUNIT_ASSERT(simpleThread2.join());

	// ���Ի�û��start��join
	SimpleThread simpleThread3;
	CPPUNIT_ASSERT(simpleThread3.join());
}

void ThreadTestCase::testTls() {
	SimpleThread t1;
	SimpleThread t2;
	t1.start();
	t2.start();
	Thread::msleep(50);
	CPPUNIT_ASSERT(t1.m_currentThread == &t1);
	CPPUNIT_ASSERT(t2.m_currentThread == &t2);
}

class SimpleTask: public Task {
public:
	SimpleTask(int interval): Task("SimpleTask", interval) {
		m_runCount = 0;
	}

	virtual void run() {
		m_runCount++;
	}

	int m_runCount;
};

class SimpleTask2: public Task {
public:
	SimpleTask2(): Task("SimpleTask2", 100) {
	}

	virtual void run() {
		msleep(50);
	}
};

class SimpleTask3: public Task {
public:
	SimpleTask3(int interval): Task("SimpleTask3", interval) {
		m_lastExecuteTime = System::currentTimeMillis();
		m_lastInterval = 0;
		m_runCount = 0;
	}

	virtual void run() {
		u64 now = System::currentTimeMillis();
		m_lastInterval = (int)(now - m_lastExecuteTime);
		m_lastExecuteTime = now;
		m_runCount++;
	}

	int getLastInterval() {
		return m_lastInterval;
	}

	u64 m_lastExecuteTime;
	int m_lastInterval;
	int m_runCount;
};

void ThreadTestCase::testTask() {
	SimpleTask task(500);
	task.start();
	Thread::msleep(100);
	CPPUNIT_ASSERT(task.isAlive());	
	CPPUNIT_ASSERT(task.m_runCount == 0);

	// ��֤����ÿ��500�����ִ��һ��
	Thread::msleep(600);
	CPPUNIT_ASSERT(task.m_runCount == 1);	// ������һ��
	CPPUNIT_ASSERT(!task.isPaused());
	Thread::msleep(500);
	CPPUNIT_ASSERT(task.m_runCount == 2);	// ���еڶ���
	CPPUNIT_ASSERT(!task.isPaused());

	// ֹͣ����
	task.stop();
	Thread::msleep(10);
	CPPUNIT_ASSERT(!task.isAlive());
	task.join(-1);

	// ����isRunning
	SimpleTask2 task2;
	task2.start();
	CPPUNIT_ASSERT(!task2.isRunning());
	Thread::msleep(120);
	CPPUNIT_ASSERT(task2.isRunning());
	Thread::msleep(50);
	CPPUNIT_ASSERT(!task2.isRunning());
	task2.stop();
	task2.join(-1);

	// ��������ʱ�Զ�ֹͣ
	{
		SimpleTask task3(100);
		task3.start();
		Thread::msleep(200);
	}
}

/* ����BUG����#57496 */
void ThreadTestCase::testBug57496() {
	uint interval = (Event::WAIT_TIME_THRESHOLD + 5) * 1000;
	SimpleTask3 task(interval);
	task.start();
	Thread::msleep(100);
	CPPUNIT_ASSERT(task.isAlive());	
	CPPUNIT_ASSERT(task.m_runCount == 0);

	// ��֤һ������������ִ������ȷ�Ĵ���
	printf("\nWait for %d seconds...\n", interval / 1000);
	Thread::msleep(interval + 500);
	CPPUNIT_ASSERT(task.m_runCount == 1);
	CPPUNIT_ASSERT(!task.isPaused());
	u64 waitTime = interval * 4;
	printf("Wait for %d seconds...\n", (uint)(waitTime / 1000));
	Thread::msleep((uint)waitTime);
	CPPUNIT_ASSERT(task.m_runCount == 5);	
	CPPUNIT_ASSERT(!task.isPaused());
	int lastInterval = task.getLastInterval();
	CPPUNIT_ASSERT(lastInterval < interval + 100);
	CPPUNIT_ASSERT(lastInterval > interval - 100);
}

/** ����������ͣ/�ָ����� */
void ThreadTestCase::testTaskPause() {
	SimpleTask task(500);
	task.start();
	// ��ͣ������֤������ִ��
	task.pause();
	CPPUNIT_ASSERT(task.isPaused());
	Thread::msleep(800);
	CPPUNIT_ASSERT(task.m_runCount == 0);

	// �ָ�������֤�����ּ���ִ��
	task.resume();
	CPPUNIT_ASSERT(!task.isPaused());
	Thread::msleep(300);
	CPPUNIT_ASSERT(task.m_runCount == 1);
}

/** �������񼤻�� */
void ThreadTestCase::testTaskSignal() {
	SimpleTask task(500);
	task.start();
	// ������ʱ������ִ��
	task.signal();
	Thread::msleep(100);
	CPPUNIT_ASSERT(task.m_runCount == 1);
	task.signal();
	Thread::msleep(100);
	CPPUNIT_ASSERT(task.m_runCount == 2);
}

