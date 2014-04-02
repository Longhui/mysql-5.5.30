/**
 * 测试原子量、锁等同步机制
 *
 * @author 汪源(wangyuan@corp.netease.com, wy@163.org)
 */

#include "util/TestSync.h"
#include "util/Sync.h"
#include "util/Thread.h"
#include "util/System.h"
#include <cppunit/config/SourcePrefix.h>
#include <iostream>


#ifndef WIN32
#include <pthread.h>
#endif
#include "util/Qrl.h"

using namespace std;
using namespace ntse;

class LockConflictThread: public Thread {
public:
	LockConflictThread(IntentionLock *lock, ILMode mode): Thread("LockConflictThread") {
		m_lock = lock;
		m_mode1 = mode;
	}

	virtual void run() {
		m_currentThread = Thread::currentThread();
		CPPUNIT_ASSERT(m_lock->getLock(1) == m_mode1);
		for (int j = IL_IS; j < IL_MAX; j++) {
			CPPUNIT_ASSERT(m_lock->getLock(1) == m_mode1);
			ILMode mode2 = (ILMode)j;
			bool conflict = IntentionLock::isConflict(m_mode1, mode2);
			if (conflict)
				conflict = !m_lock->isSelfConflict(mode2);
			u64 n2 = m_lock->getUsage()->m_lockCnt[mode2];
			u64 spinBefore = m_lock->getUsage()->m_spinCnt;
			u64 waitBefore = m_lock->getUsage()->m_waitCnt;
			u64 waitTimeBefore = m_lock->getUsage()->m_waitTime;
			CPPUNIT_ASSERT(m_lock->lock(2, mode2, 100, __FILE__, __LINE__) == !conflict);
			CPPUNIT_ASSERT(m_lock->isLocked(2, mode2) == !conflict);
			CPPUNIT_ASSERT(m_lock->getUsage()->m_lockCnt[mode2] == n2 + 1);
			if (conflict) {
				CPPUNIT_ASSERT(m_lock->getLock(2) == IL_NO);
				CPPUNIT_ASSERT(m_lock->getUsage()->m_spinCnt == spinBefore + 1);
				CPPUNIT_ASSERT(m_lock->getUsage()->m_waitCnt == waitBefore + 1);
				CPPUNIT_ASSERT(m_lock->getUsage()->m_waitTime > waitTimeBefore + 90);
				CPPUNIT_ASSERT(m_lock->getUsage()->m_waitTime < waitTimeBefore + 200);
			} else {
				CPPUNIT_ASSERT(m_lock->getLock(2) == mode2);
				CPPUNIT_ASSERT(m_lock->getUsage()->m_spinCnt == spinBefore);
				CPPUNIT_ASSERT(m_lock->getUsage()->m_waitCnt == waitBefore);
				CPPUNIT_ASSERT(m_lock->getUsage()->m_waitTime == waitTimeBefore);
				m_lock->unlock(2, mode2);
				CPPUNIT_ASSERT(m_lock->getLock(2) == IL_NO);
			}
			msleep(10);
		}
	}

	Thread	*m_currentThread;
	IntentionLock *m_lock;
	ILMode m_mode1;
};

class UpgradeConflictThread: public Thread {
public:
	UpgradeConflictThread(IntentionLock *lock, ILMode mode): Thread("UpgradeConflictThread") {
		m_lock = lock;
		m_mode1 = mode;
	}

	virtual void run() {
		m_currentThread = Thread::currentThread();
		for (int j = IL_IS; j < IL_MAX; j++) {
			ILMode mode2 = (ILMode)j;
			bool conflict = IntentionLock::isConflict(m_mode1, mode2);
			if (conflict)
				conflict = !m_lock->isSelfConflict(mode2);
			if (mode2 < IL_SIX && !conflict) {
				CPPUNIT_ASSERT(m_lock->lock(2, mode2, 100, __FILE__, __LINE__) == !conflict);
				bool upconflict = IntentionLock::isConflict(m_mode1, IL_SIX);
				CPPUNIT_ASSERT(m_lock->upgrade(2, mode2, IL_SIX, 100, __FILE__, __LINE__) == !upconflict);
				if (m_lock->getLock(2) != IL_NO)
					m_lock->unlock(2, m_lock->getLock(2));
			}
			if (mode2 < IL_X && !conflict) {
				CPPUNIT_ASSERT(m_lock->lock(2, mode2, 100, __FILE__, __LINE__) == !conflict);
				bool upconflict = IntentionLock::isConflict(m_mode1, IL_X);
				CPPUNIT_ASSERT(m_lock->upgrade(2, mode2, IL_X, 100, __FILE__, __LINE__) == !upconflict);
				if (m_lock->getLock(2) != IL_NO)
					m_lock->unlock(2, m_lock->getLock(2));
			}
			msleep(10);
		}
	}

	Thread	*m_currentThread;
	IntentionLock *m_lock;
	ILMode m_mode1;
};

const char* SyncTestCase::getName() {
	return "Other synchronization facilities test";
}

const char* SyncTestCase::getDescription() {
	return "Test Atomic, Event and other Sync ultilities except mutex and rwlock.";
}

bool SyncTestCase::isBig() {
	return false;
}

void SyncTestCase::testAtomic() {
	int v;

	// int类型的原子量
	Atomic<int> a1;
	CPPUNIT_ASSERT(a1.get() == 0);
	v = a1.getAndIncrement();
	CPPUNIT_ASSERT(v == 0);
	CPPUNIT_ASSERT(a1.get() == 1);

	v = a1.incrementAndGet();
	CPPUNIT_ASSERT(v == 2);
	CPPUNIT_ASSERT(a1.get() == 2);

	a1.increment();
	CPPUNIT_ASSERT(a1.get() == 3);

	v = a1.getAndDecrement();
	CPPUNIT_ASSERT(v == 3);
	CPPUNIT_ASSERT(a1.get() == 2);

	v = a1.decrementAndGet();
	CPPUNIT_ASSERT(v == 1);
	CPPUNIT_ASSERT(a1.get() == 1);

	a1.decrement();
	CPPUNIT_ASSERT(a1.get() == 0);

	a1.set(1);
	CPPUNIT_ASSERT(a1.get() == 1);

	a1.set(0);
	a1.increment();
	CPPUNIT_ASSERT(a1.compareAndSwap(1, 10));
	CPPUNIT_ASSERT(a1.get() == 10);

	CPPUNIT_ASSERT(!a1.compareAndSwap(1, 2));
	CPPUNIT_ASSERT(a1.get() == 10);

	a1.set(0);
	CPPUNIT_ASSERT(a1.addAndGet(10) == 10);
	CPPUNIT_ASSERT(a1.get() == 10);
	CPPUNIT_ASSERT(a1.addAndGet(-20) == -10);
	CPPUNIT_ASSERT(a1.get() == -10);

	// long类型的原子量
	long v2;
	Atomic<long> a2;
	CPPUNIT_ASSERT(a2.get() == 0);
	v2 = a2.getAndIncrement();
	CPPUNIT_ASSERT(v2 == 0);
	CPPUNIT_ASSERT(a2.get() == 1);

	// 其它类型
	Atomic<unsigned int> a3;
	Atomic<short> a4;

	// 初始值不是0
	Atomic<int> a5(100);
	CPPUNIT_ASSERT(a5.get() == 100);
}

class EventTestThread: public Thread {
public:
	EventTestThread(Event *evt, int sleepTime): Thread("EventTestThread") {
		m_evt = evt;
		m_sleepTime = sleepTime;
		m_wakenup = false;
	}

	void run() {
		m_evt->wait(m_sleepTime);
		m_wakenup = true;
	}

	bool isWakenup() {
		return m_wakenup;
	}

private:
	Event	*m_evt;
	int		m_sleepTime;
	bool	m_wakenup;
};

void SyncTestCase::testEvent() {
	Event	evt;

	// 测试wait和signal是否发挥作用
	EventTestThread	t1(&evt, 1000);
	t1.start();
	Thread::msleep(100);
	CPPUNIT_ASSERT(!t1.isWakenup());
	evt.signal();
	Thread::msleep(100);
	CPPUNIT_ASSERT(t1.isWakenup());

	// 测试Event.wait超时
	EventTestThread	t2(&evt, 200);
	t2.start();
	Thread::msleep(100);
	CPPUNIT_ASSERT(!t2.isWakenup());
	Thread::msleep(200);
	CPPUNIT_ASSERT(t2.isWakenup());
}

/** 测试意向锁冲突判断 */
void SyncTestCase::testILConflict() {
	// IL_NO
	CPPUNIT_ASSERT(!IntentionLock::isConflict(IL_NO, IL_NO));
	CPPUNIT_ASSERT(!IntentionLock::isConflict(IL_NO, IL_IS));
	CPPUNIT_ASSERT(!IntentionLock::isConflict(IL_NO, IL_S));
	CPPUNIT_ASSERT(!IntentionLock::isConflict(IL_NO, IL_IX));
	CPPUNIT_ASSERT(!IntentionLock::isConflict(IL_NO, IL_SIX));
	CPPUNIT_ASSERT(!IntentionLock::isConflict(IL_NO, IL_X));
	CPPUNIT_ASSERT(!IntentionLock::isConflict(IL_NO, IL_U));

	// IL_IS
	CPPUNIT_ASSERT(!IntentionLock::isConflict(IL_IS, IL_NO));
	CPPUNIT_ASSERT(!IntentionLock::isConflict(IL_IS, IL_IS));
	CPPUNIT_ASSERT(!IntentionLock::isConflict(IL_IS, IL_S));
	CPPUNIT_ASSERT(!IntentionLock::isConflict(IL_IS, IL_IX));
	CPPUNIT_ASSERT(!IntentionLock::isConflict(IL_IS, IL_SIX));
	CPPUNIT_ASSERT(IntentionLock::isConflict(IL_IS, IL_X));
	CPPUNIT_ASSERT(!IntentionLock::isConflict(IL_IS, IL_U));

	// IL_S
	CPPUNIT_ASSERT(!IntentionLock::isConflict(IL_S, IL_NO));
	CPPUNIT_ASSERT(!IntentionLock::isConflict(IL_S, IL_IS));
	CPPUNIT_ASSERT(!IntentionLock::isConflict(IL_S, IL_S));
	CPPUNIT_ASSERT(IntentionLock::isConflict(IL_S, IL_IX));
	CPPUNIT_ASSERT(IntentionLock::isConflict(IL_S, IL_SIX));
	CPPUNIT_ASSERT(IntentionLock::isConflict(IL_S, IL_X));
	CPPUNIT_ASSERT(!IntentionLock::isConflict(IL_S, IL_U));

	// IL_IX
	CPPUNIT_ASSERT(!IntentionLock::isConflict(IL_IX, IL_NO));
	CPPUNIT_ASSERT(!IntentionLock::isConflict(IL_IX, IL_IS));
	CPPUNIT_ASSERT(IntentionLock::isConflict(IL_IX, IL_S));
	CPPUNIT_ASSERT(!IntentionLock::isConflict(IL_IX, IL_IX));
	CPPUNIT_ASSERT(IntentionLock::isConflict(IL_IX, IL_SIX));
	CPPUNIT_ASSERT(IntentionLock::isConflict(IL_IX, IL_X));
	CPPUNIT_ASSERT(IntentionLock::isConflict(IL_IX, IL_U));

	// IL_SIX
	CPPUNIT_ASSERT(!IntentionLock::isConflict(IL_SIX, IL_NO));
	CPPUNIT_ASSERT(!IntentionLock::isConflict(IL_SIX, IL_IS));
	CPPUNIT_ASSERT(IntentionLock::isConflict(IL_SIX, IL_S));
	CPPUNIT_ASSERT(IntentionLock::isConflict(IL_SIX, IL_IX));
	CPPUNIT_ASSERT(IntentionLock::isConflict(IL_SIX, IL_SIX));
	CPPUNIT_ASSERT(IntentionLock::isConflict(IL_SIX, IL_X));
	CPPUNIT_ASSERT(IntentionLock::isConflict(IL_SIX, IL_U));

	// IL_X
	CPPUNIT_ASSERT(!IntentionLock::isConflict(IL_X, IL_NO));
	CPPUNIT_ASSERT(IntentionLock::isConflict(IL_X, IL_IS));
	CPPUNIT_ASSERT(IntentionLock::isConflict(IL_X, IL_S));
	CPPUNIT_ASSERT(IntentionLock::isConflict(IL_X, IL_IX));
	CPPUNIT_ASSERT(IntentionLock::isConflict(IL_X, IL_SIX));
	CPPUNIT_ASSERT(IntentionLock::isConflict(IL_X, IL_X));
	CPPUNIT_ASSERT(IntentionLock::isConflict(IL_X, IL_U));

	// IL_U
	CPPUNIT_ASSERT(!IntentionLock::isConflict(IL_U, IL_NO));
	CPPUNIT_ASSERT(!IntentionLock::isConflict(IL_U, IL_IS));
	CPPUNIT_ASSERT(!IntentionLock::isConflict(IL_U, IL_S));
	CPPUNIT_ASSERT(IntentionLock::isConflict(IL_U, IL_IX));
	CPPUNIT_ASSERT(IntentionLock::isConflict(IL_U, IL_SIX));
	CPPUNIT_ASSERT(IntentionLock::isConflict(IL_U, IL_X));
	CPPUNIT_ASSERT(IntentionLock::isConflict(IL_U, IL_U));
}

/** 测试意向锁功能,不同线程的不同session并发情形 */
void SyncTestCase::testILLockThreads() {
	// 加锁,冲突及统计信息
	IntentionLock lock(10, __FUNCTION__, __FILE__, __LINE__);
	for (int i = IL_IS; i < IL_MAX; i++) {
		ILMode mode1 = (ILMode)i;
		u64 n = lock.getUsage()->m_lockCnt[mode1];
		CPPUNIT_ASSERT(lock.lock(1, mode1, 100, __FILE__, __LINE__));
		CPPUNIT_ASSERT(lock.isLocked(1, mode1));
		CPPUNIT_ASSERT(lock.getLock(1) == mode1);
		CPPUNIT_ASSERT(lock.getUsage()->m_lockCnt[mode1] == n + 1);
		LockConflictThread syncThread(&lock, mode1);
		syncThread.start();
		Thread::msleep(10000);
		CPPUNIT_ASSERT(syncThread.join());
		CPPUNIT_ASSERT(!syncThread.isAlive());
		lock.unlock(1, mode1);
		CPPUNIT_ASSERT(!lock.isLocked(1, mode1));
		CPPUNIT_ASSERT(lock.getLock(1) == IL_NO);
	}

	// 锁升级
	for (int i = IL_IS; i < IL_MAX; i++) {
		ILMode mode1 = (ILMode)i;
		u64 n = lock.getUsage()->m_lockCnt[mode1];
		CPPUNIT_ASSERT(lock.lock(1, mode1, 100, __FILE__, __LINE__));
		CPPUNIT_ASSERT(lock.isLocked(1, mode1));
		CPPUNIT_ASSERT(lock.getLock(1) == mode1);
		CPPUNIT_ASSERT(lock.getUsage()->m_lockCnt[mode1] == n + 1);
		UpgradeConflictThread syncThread(&lock, mode1);
		syncThread.start();
		Thread::msleep(10000);
		CPPUNIT_ASSERT(syncThread.join());
		CPPUNIT_ASSERT(!syncThread.isAlive());
		lock.unlock(1, mode1);
		CPPUNIT_ASSERT(!lock.isLocked(1, mode1));
		CPPUNIT_ASSERT(lock.getLock(1) == IL_NO);
	}
}

/** 测试意向锁功能 */
void SyncTestCase::testILLock() {
	// 加锁，冲突及统计信息
	IntentionLock lock(10, __FUNCTION__, __FILE__, __LINE__);
	for (int i = IL_IS; i < IL_MAX; i++) {
		ILMode mode1 = (ILMode)i;
		u64 n = lock.getUsage()->m_lockCnt[mode1];
		CPPUNIT_ASSERT(lock.lock(1, mode1, 100, __FILE__, __LINE__));
		CPPUNIT_ASSERT(lock.isLocked(1, mode1));
		CPPUNIT_ASSERT(lock.getLock(1) == mode1);
		CPPUNIT_ASSERT(lock.getUsage()->m_lockCnt[mode1] == n + 1);
		for (int j = IL_IS; j < IL_MAX; j++) {
			ILMode mode2 = (ILMode)j;
			bool conflict = IntentionLock::isConflict(mode1, mode2);
			if (conflict)
				conflict = !lock.isSelfConflict(mode2);
			u64 n2 = lock.getUsage()->m_lockCnt[mode2];
			u64 spinBefore = lock.getUsage()->m_spinCnt;
			u64 waitBefore = lock.getUsage()->m_waitCnt;
			u64 waitTimeBefore = lock.getUsage()->m_waitTime;
			CPPUNIT_ASSERT(lock.lock(2, mode2, 100, __FILE__, __LINE__) == !conflict);
			CPPUNIT_ASSERT(lock.isLocked(2, mode2) == !conflict);
			CPPUNIT_ASSERT(lock.getUsage()->m_lockCnt[mode2] == n2 + 1);
			if (conflict) {
				CPPUNIT_ASSERT(lock.getLock(2) == IL_NO);
				CPPUNIT_ASSERT(lock.getUsage()->m_spinCnt == spinBefore + 1);
				CPPUNIT_ASSERT(lock.getUsage()->m_waitCnt == waitBefore + 1);
				CPPUNIT_ASSERT(lock.getUsage()->m_waitTime > waitTimeBefore + 90);
				CPPUNIT_ASSERT(lock.getUsage()->m_waitTime < waitTimeBefore + 200);
			} else {
				CPPUNIT_ASSERT(lock.getLock(2) == mode2);
				CPPUNIT_ASSERT(lock.getUsage()->m_spinCnt == spinBefore);
				CPPUNIT_ASSERT(lock.getUsage()->m_waitCnt == waitBefore);
				CPPUNIT_ASSERT(lock.getUsage()->m_waitTime == waitTimeBefore);
				lock.unlock(2, mode2);
				CPPUNIT_ASSERT(lock.getLock(2) == IL_NO);
			}
		}
		lock.unlock(1, mode1);
		CPPUNIT_ASSERT(!lock.isLocked(1, mode1));
		CPPUNIT_ASSERT(lock.getLock(1) == IL_NO);
	}

	// 锁升级
	lock.lock(1, IL_IS, -1, __FILE__, __LINE__);
	lock.lock(2, IL_IS, -1, __FILE__, __LINE__);
	CPPUNIT_ASSERT(lock.upgrade(1, IL_IS, IL_S, -1, __FILE__, __LINE__));
	CPPUNIT_ASSERT(lock.isLocked(1, IL_S));
	CPPUNIT_ASSERT(lock.upgrade(1, IL_S, IL_X, 100, __FILE__, __LINE__));
	CPPUNIT_ASSERT(lock.isLocked(1, IL_X));
	CPPUNIT_ASSERT(lock.upgrade(2, IL_IS, IL_X, 100, __FILE__, __LINE__));
	CPPUNIT_ASSERT(lock.isLocked(2, IL_X));
	lock.unlock(1, lock.getLock(1));
	lock.unlock(2, lock.getLock(2));

	lock.lock(1, IL_IX, -1, __FILE__, __LINE__);
	lock.lock(2, IL_IX, -1, __FILE__, __LINE__);
	CPPUNIT_ASSERT(lock.upgrade(1, IL_IX, IL_SIX, 100, __FILE__, __LINE__));
	CPPUNIT_ASSERT(lock.isLocked(1, IL_SIX));
	CPPUNIT_ASSERT(lock.upgrade(2, IL_IX, IL_SIX, 100, __FILE__, __LINE__));
	CPPUNIT_ASSERT(lock.isLocked(2, IL_SIX));
	lock.unlock(1, lock.getLock(1));
	lock.unlock(2, lock.getLock(2));

	lock.lock(1, IL_S, -1, __FILE__, __LINE__);
	lock.lock(2, IL_IS, -1, __FILE__, __LINE__);
	CPPUNIT_ASSERT(lock.upgrade(1, IL_S, IL_X, 100, __FILE__, __LINE__));
	CPPUNIT_ASSERT(lock.isLocked(1, IL_X));
	CPPUNIT_ASSERT(lock.upgrade(2, IL_IS, IL_X, 100, __FILE__, __LINE__));
	CPPUNIT_ASSERT(lock.isLocked(2, IL_X));
	lock.unlock(1, lock.getLock(1));
	lock.unlock(2, lock.getLock(2));

	lock.lock(1, IL_U, -1, __FILE__, __LINE__);
	lock.lock(2, IL_S, -1, __FILE__, __LINE__);
	CPPUNIT_ASSERT(lock.upgrade(1, IL_U, IL_X, 100, __FILE__, __LINE__));
	CPPUNIT_ASSERT(lock.isLocked(1, IL_X));
	CPPUNIT_ASSERT(lock.upgrade(2, IL_S, IL_X, 100, __FILE__, __LINE__));
	CPPUNIT_ASSERT(lock.isLocked(2, IL_X));
	lock.unlock(1, lock.getLock(1));
	lock.unlock(2, lock.getLock(2));
}

const char* SyncBigTest::getName() {
	return "Other synchronization facilities performance test";
}

const char* SyncBigTest::getDescription() {
	return "Test performance of Atomic and other Synchronization utilities except Mutex and RWLock.";
}

bool SyncBigTest::isBig() {
	return true;
}

void SyncBigTest::testAtomic() {
	cout << "  Test performance of Atomic<int>" << endl;
	
	int repeat = 10000000;
	Atomic<int> a;
	u64 before = System::clockCycles();
	for (int i = 0; i < repeat; i++) {
		a.increment();
	}
	u64 after = System::clockCycles();
	cout << "  clock cycles per atomic increment: " << (after - before) / repeat << endl;

	cout << "  Test performance of Atomic<long>" << endl;
	Atomic<long> a2;
	before = System::clockCycles();
	for (int i = 0; i < repeat; i++) {
		a2.increment();
	}
	after = System::clockCycles();
	cout << "  clock cycles per atomic increment: " << (after - before) / repeat << endl;
}

#ifndef WIN32
void SyncBigTest::testPthreadMutex() {
	cout << "  Test performance of pthread mutex" << endl;

	int repeat = 10000000;
	pthread_mutex_t m;
	pthread_mutex_init(&m, NULL);
	u64 before = System::clockCycles();
	for (int i = 0; i < repeat; i++) {
		pthread_mutex_lock(&m);
		pthread_mutex_unlock(&m);
	}	
	u64 after = System::clockCycles();
	pthread_mutex_destroy(&m);
	cout << "  clock cycles per lock/unlock pair: " << (after - before) / repeat << endl;
}
#endif



void SyncBigTest::testQrlST() {
	qrltas_lock lock;
	cout << "  Test performance of QRL" << endl;

	qrltas_initialize(&lock);
	int repeat = 100000000;
	u64 before = System::clockCycles();
	for (int i = 0; i < repeat; i++) {
		int quick = qrltas_acquire(&lock, 1);
		qrltas_release(&lock, quick);	
	}	
	u64 after = System::clockCycles();
	cout << "  clock cycles per lock/unlock pair: " << (after - before) / repeat << endl;
}

class PingPongTestThread: public Thread {
public:
	PingPongTestThread(Event *waitEvt, Event *signalEvt): Thread("PingPongTestThread") {
		m_waitEvt = waitEvt;
		m_signalEvt = signalEvt;
		m_stopped = false;
	}

	void run() {
		do {
			m_waitEvt->wait(0);
			m_signalEvt->signal();
		} while (!m_stopped);
	}

	void stop() {
		m_stopped = true;
	}

private:
	Event	*m_waitEvt;
	Event	*m_signalEvt;
	bool	m_stopped;
};
/**
 * 两个线程使用两个Event交替的等待唤醒
 * 性能日志
 *   2008/8/1
 *   round/ms: 97
 */
void SyncBigTest::testEventPingPong() {
	int loop = 1000000;
	Event evt1, evt2;

	PingPongTestThread *thread = new PingPongTestThread(&evt1, &evt2);
	thread->start();
	Thread::msleep(100);

	u64 before = System::currentTimeMillis();
	for (int i = 0; i < loop; i++) {
		evt1.signal();
		evt2.wait(0);
	}

	thread->stop();
	evt1.signal();
	thread->join();
	u64 after = System::currentTimeMillis();

	cout << "  round per ms: " << loop / (after - before) << endl;
}

