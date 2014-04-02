/**
 * ����RWLock��ͬ������
 *
 * @author ������(niemingjun@corp.netease.com, niemingjun@163.org)
 */
#include "util/TestRWLock.h"
#include "util/Thread.h"
#include "util/Sync.h"
#include "util/InnoSync.h"
#include "util/System.h"
#include "misc/EventMonitor.h"

#include <cppunit/config/SourcePrefix.h>
#include <iostream>


#ifndef WIN32
#include <pthread.h>
#endif

using namespace std;
using namespace ntse;

//��д������ͳ��ȫ�ֱ���

static u64			g_rlockCnt = 0;
static u64			g_wlockCnt = 0;
static u64			g_totalRLockTime = 0;
static u64			g_totalWLockTime = 0;
static Atomic<int>	g_maxRLockTime;
static Atomic<int>	g_maxWLockTime;


const char* RWLockTestCase::getName() {
	return "RWLock Synchronization test";
}
const char* RWLockTestCase::getDescription() {
	return "RWLock test";
}
bool RWLockTestCase::isBig() {
	return false;
}

void RWLockTestCase::testRWLock() {
	RWLock lock("testRWLock", __FILE__, __LINE__);

	//1.�Ӷ���
	RWLOCK(&lock, Shared);
	//��״̬
	CPPUNIT_ASSERT(lock.isLocked(Shared));
	CPPUNIT_ASSERT(!lock.isLocked(Exclusived));
	//���д��ʱ��ʧ��
	CPPUNIT_ASSERT(!lock.timedLock(Exclusived,100, __FILE__, __LINE__));
	//���д��ʧ��
	CPPUNIT_ASSERT(!RWTRYLOCK(&lock, Exclusived));
	//�Ӷ���
	CPPUNIT_ASSERT(RWTRYLOCK(&lock, Shared));
	//��time����
	CPPUNIT_ASSERT(lock.timedLock(Shared, 100, __FILE__, __LINE__));
	
	//���� & ��״̬
	RWUNLOCK(&lock, Shared);
	CPPUNIT_ASSERT(lock.isLocked(Shared));
	RWUNLOCK(&lock, Shared);
	CPPUNIT_ASSERT(lock.isLocked(Shared));
	RWUNLOCK(&lock, Shared);
	CPPUNIT_ASSERT(!lock.isLocked(Shared));

	//2.����д��
	RWLOCK(&lock, Exclusived);
	CPPUNIT_ASSERT(!lock.isLocked(Shared));
	CPPUNIT_ASSERT(lock.isLocked(Exclusived));
	//���(try)д��ʧ��
	CPPUNIT_ASSERT(!RWTRYLOCK(&lock, Exclusived));
	//���(try)����ʧ��
	CPPUNIT_ASSERT(!RWTRYLOCK(&lock, Shared));
	//���д����ʱ
	CPPUNIT_ASSERT(!lock.timedLock(Exclusived, 100, __FILE__, __LINE__));
	//��Ӷ�����ʱ
	CPPUNIT_ASSERT(!lock.timedLock(Shared, 100, __FILE__, __LINE__));

	//����
	RWUNLOCK(&lock, Exclusived);
	CPPUNIT_ASSERT(!lock.isLocked(Shared));
	CPPUNIT_ASSERT(!lock.isLocked(Exclusived));

	//����д����try�ɹ�
	CPPUNIT_ASSERT(RWTRYLOCK(&lock, Exclusived));

	CPPUNIT_ASSERT(lock.getUsage()->m_rlockCnt == 5);
	CPPUNIT_ASSERT(lock.getUsage()->m_wlockCnt == 6);

	CPPUNIT_ASSERT(!lock.timedLock(Shared, 100, __FILE__, __LINE__));
	CPPUNIT_ASSERT(!lock.timedLock(Exclusived, 100, __FILE__, __LINE__));
	//����
	RWUNLOCK(&lock, Exclusived);
	CPPUNIT_ASSERT(!lock.isLocked(Shared));
	CPPUNIT_ASSERT(!lock.isLocked(Exclusived));
	//���ж���
	RWLOCK(&lock, Shared);
	//��������
	CPPUNIT_ASSERT(lock.tryUpgrade(__FILE__, __LINE__));
	CPPUNIT_ASSERT(lock.isLocked(Exclusived));
}

/** 
 *  ����<code>RWLockGuard</code>
 */
void RWLockTestCase::testRWLockGuard() {
	RWLock lock("testRWLockGuard", __FILE__, __LINE__);

	{
		RWLockGuard guard(&lock, Shared, __FILE__, __LINE__);
		CPPUNIT_ASSERT(!RWTRYLOCK(&lock, Exclusived));
		CPPUNIT_ASSERT(RWTRYLOCK(&lock, Shared));
		RWUNLOCK(&lock, Shared);
	}
	CPPUNIT_ASSERT(RWTRYLOCK(&lock, Exclusived));
	RWUNLOCK(&lock, Exclusived);

	{
		RWLockGuard guard(&lock, Exclusived, __FILE__, __LINE__);
		CPPUNIT_ASSERT(!RWTRYLOCK(&lock, Exclusived));
		CPPUNIT_ASSERT(!RWTRYLOCK(&lock, Shared));
	}
	CPPUNIT_ASSERT(RWTRYLOCK(&lock, Exclusived));
	RWUNLOCK(&lock, Exclusived);

	{
		RWLockGuard guard(&lock, Exclusived, __FILE__, __LINE__);
		CPPUNIT_ASSERT(!RWTRYLOCK(&lock, Shared));
		guard.unlock();
		CPPUNIT_ASSERT(RWTRYLOCK(&lock, Shared));
	}
	CPPUNIT_ASSERT(!RWTRYLOCK(&lock, Exclusived));
	RWUNLOCK(&lock, Shared);

	{
		RWLockGuard guard(&lock, Exclusived, __FILE__, __LINE__);
		CPPUNIT_ASSERT(!RWTRYLOCK(&lock, Shared));
		guard.unlock();
		CPPUNIT_ASSERT(RWTRYLOCK(&lock, Shared));
		RWUNLOCK(&lock, Shared);
		guard.lock(__FILE__, __LINE__);
		CPPUNIT_ASSERT(!RWTRYLOCK(&lock, Shared));
	}
	CPPUNIT_ASSERT(RWTRYLOCK(&lock, Exclusived));
	RWUNLOCK(&lock, Exclusived);
}


/** 
 * Mutex�����̸߳����ࡣ
 * ��;��ִ��һ��lock������holdס��timeout ������
 */
class RWLockTestThread : public Thread {
public:
	/**
	 * ���캯����
	 * @param pRWLock		��
	 * @param holdTimeMs	����������������
	 * @param lockMode		��ģʽ
	 */
	RWLockTestThread(RWLock *pRWLock, int holdTimeMs, LockMode lockMode) : Thread("TestThread") {
		m_rWLock = pRWLock;
		m_holdTimeMs = holdTimeMs;
		m_lockMode = lockMode;
	}
	/**
	 * ���С�
	 * @see <code>Thread::run()</code> ������
	 */
	void run() {
		assert(NULL != m_rWLock);
		assert(NULL != m_rWLock->getUsage());
		m_rWLock->lock(m_lockMode,__FILE__, __LINE__);
		Thread::msleep(m_holdTimeMs);
		m_rWLock->unlock(m_lockMode);
	}
private:
	RWLock *m_rWLock;	/* �� */
	int m_holdTimeMs;	/* �������������� */
	LockMode m_lockMode;		/* ����ģʽ */
};

/** 
 * ����RWLock�ڳ�ͻ�µȴ�������ȡ���ɹ���ʧ�ܡ�
 */
void RWLockTestCase::testRWLockConflictGain() {
	RWLock lock("testRWLockConflictGain", __FILE__, __LINE__);

	//1.�����߳� �Ӷ���
	RWLockTestThread t(&lock, 2 * 1000, Shared);
	t.start();
	//�ȴ������߳�����
	Thread::msleep(500);
	bool ok = false;
	//���Եȴ���ȡд��ʧ��
	ok = lock.timedLock(Exclusived, 500, __FILE__, __LINE__);
	CPPUNIT_ASSERT(!ok);
	//���Եȴ���ȡд���ɹ�
	ok = lock.timedLock(Exclusived, 2 * 1000, __FILE__, __LINE__);
	CPPUNIT_ASSERT(ok);
	lock.unlock(Exclusived);
	t.join();

	//2. �����߳� ��д��
	RWLockTestThread t2(&lock, 2 * 1000, Exclusived);
	t2.start();
	//�ȴ������߳�����
	Thread::msleep(500);
	ok = false;
	//���Եȴ���ȡ����ʧ��
	ok = lock.timedLock(Shared, 500, __FILE__, __LINE__);
	CPPUNIT_ASSERT(!ok);
	//���Եȴ���ȡ�����ɹ�
	ok = lock.timedLock(Exclusived, 2 * 1000, __FILE__, __LINE__);
	CPPUNIT_ASSERT(ok);
	lock.unlock(Exclusived);
	t2.join();
}

void RWLockTestCase::testPrintUsage() {
	RWLock l1(__FUNCTION__, __FILE__, __LINE__);
	RWLock l2(__FUNCTION__, __FILE__, __LINE__);

	l1.lock(Shared, __FILE__, __LINE__);
	l2.lock(Shared, __FILE__, __LINE__);

	RWLockUsage::printAll(cout);
}

template<typename LockType>
class RWLockMT: public Task {
public:
	RWLockMT(LockType **lockArray, size_t numLock, int writePercent, int timedPercent, int timeoutMs): Task("RWLockMT", 1000) {
		m_lockArray = lockArray;
		m_numLock = numLock;
		m_stopped = false;
		m_writePercent = writePercent;
		m_timedPercent = timedPercent;
		m_timeoutMs = timeoutMs;
	}

	void run() {
		System::srandom((int)System::microTime());
		int idx = (int)(System::random() % m_numLock);
		while (!isPaused()) {
			LockMode mode = Shared;
			if (System::random() % 100 < m_writePercent)
				mode = Exclusived;
			u64 before = System::microTime();
			if (System::random() % 100 < m_timedPercent) {
				while (!RWTIMEDLOCK(m_lockArray[idx], mode, m_timeoutMs));
			} else {
				RWLOCK(m_lockArray[idx], mode);
			}
			u64 after = System::microTime();
			int lockTime = (int)(after - before);
			Atomic<int> *maxLockTime = &g_maxRLockTime;
			if (mode == Exclusived) {
				maxLockTime = &g_maxWLockTime;
			}
			while (lockTime > maxLockTime->get()) {
				int oldLockTime = maxLockTime->get();
				if (oldLockTime >= lockTime)
					break;
				if (maxLockTime->compareAndSwap(oldLockTime, lockTime))
					break;
			}
			if (mode == Shared) {
				g_totalRLockTime += lockTime;
				g_rlockCnt++;
			} else {
				g_totalWLockTime += lockTime;
				g_wlockCnt++;
			}
			int newIdx = idx;
			for (int n = 0; n < 2000; n++)
				newIdx = (newIdx + 7 + m_id) % m_numLock;
			RWUNLOCK(m_lockArray[idx], mode);
			idx = newIdx;
		}
	}

private:
	LockType	**m_lockArray;
	size_t	m_numLock;
	bool	m_stopped;
	int		m_writePercent;
	int		m_timedPercent;
	int		m_timeoutMs;
};

class InnoRWLockMT: public Task {
public:
	InnoRWLockMT(rw_lock_t **lockArray, size_t numLock, int writePercent, int timedPercent, int timeoutMs): Task("RWLockMT", 1000) {
		m_lockArray = lockArray;
		m_numLock = numLock;
		m_stopped = false;
		m_writePercent = writePercent;
		m_timedPercent = timedPercent;
		m_timeoutMs = timeoutMs;
	}

	void run() {
		System::srandom((int)System::microTime());
		int idx = (int)(System::random() % m_numLock);
		while (!isPaused()) {
			LockMode mode = Shared;
			if (System::random() % 100 < m_writePercent)
				mode = Exclusived;
			u64 before = System::microTime();	
			if (mode == Shared)
				rw_lock_s_lock_func(m_lockArray[idx], 0, __FILE__, __LINE__);
			else
				rw_lock_x_lock_func(m_lockArray[idx], 0, __FILE__, __LINE__);
			u64 after = System::microTime();
			int lockTime = (int)(after - before);
			Atomic<int> *maxLockTime = &g_maxRLockTime;
			if (mode == Exclusived) {
				maxLockTime = &g_maxWLockTime;
			}
			while (lockTime > maxLockTime->get()) {
				int oldLockTime = maxLockTime->get();
				if (oldLockTime >= lockTime)
					break;
				if (maxLockTime->compareAndSwap(oldLockTime, lockTime))
					break;
			}
			if (mode == Shared) {
				g_totalRLockTime += lockTime;
				g_rlockCnt++;
			} else {
				g_totalWLockTime += lockTime;
				g_wlockCnt++;
			}
			int newIdx = idx;
			for (int n = 0; n < 2000; n++)
				newIdx = (newIdx + 7 + m_id) % m_numLock;
			if (mode == Shared)
				rw_lock_s_unlock_func(m_lockArray[idx]);
			else
				rw_lock_x_unlock_func(m_lockArray[idx]);
			idx = newIdx;
		}
	}

private:
	rw_lock_t	**m_lockArray;
	size_t	m_numLock;
	bool	m_stopped;
	int		m_writePercent;
	int		m_timedPercent;
	int		m_timeoutMs;
};


/** ��д�����̲߳������ܲ��ԣ���д�ۺϣ�������ʱ
 * ������־
 * 2008/6/10:
 *   ʹ��pthread_mutex_tʵ��
 *   ������: 600 ops/ms
 * 2008/6/11:
 *   ʹ��Atomicʵ��
 *   ������: 1108 - 1263 ops/ms
 * 2008/7/20:
 *   ͳ��Thread::startʱ��
 *   ������: 468 ops/ms
 */
void RWLockBigTestCase::testRWLockMTPerf() {
	doRWLockMTTest(180, 1, 20, 0, 0);
}

/** ��д�����̲߳������ܲ��ԣ���д�ۺϣ�����ʱ
 */
void RWLockBigTestCase::testRWLockMTTimedPerf() {
	doRWLockMTTest(180, 100, 100, 50, 100);
}


const char* RWLockBigTestCase::getName() {
	return "RWLock Synchronization performance/stabilization test";
}
const char* RWLockBigTestCase::getDescription() {
	return "RWLock performance/stabilization test";
}
bool RWLockBigTestCase::isBig() {
	return true;
}


/** ������־
 * 2008/6/10:
 *   ʹ��pthread_rwlock_tʵ��
 *   lock/unlock: 87 cc
 * 2008/6/11:
 *   ʹ��Atomicʵ��
 *   lock/unlock(Shared): 46 cc
 *   lock/unlock(Exclusived): 26 cc
 */
void RWLockBigTestCase::testRWLockSTPerf() {
	cout << "  Test performance of RWLock(Shared)" << endl;

	int repeat = 10000000;
	RWLock lock("testRWLockST", __FILE__, __LINE__);
	u64 before = System::clockCycles();
	for (int i = 0; i < repeat; i++) {
		RWLOCK(&lock, Shared);
		RWUNLOCK(&lock, Shared);
	}	
	u64 after = System::clockCycles();
	cout << "  clock cycles per lock/unlock pair: " << (after - before) / repeat << endl;

	cout << "  Test performance of RWLock(Exclusive)" << endl;

	before = System::clockCycles();
	for (int i = 0; i < repeat; i++) {
		RWLOCK(&lock, Exclusived);
		RWUNLOCK(&lock, Exclusived);
	}	
	after = System::clockCycles();
	cout << "  clock cycles per lock/unlock pair: " << (after - before) / repeat << endl;
}

/** ��д�����̲߳������ԣ����Ӷ��� */
void RWLockBigTestCase::testRWLockMTAllReadPerf() {
	doRWLockMTTest(180, 1, 0, 0, 0);
}


/**
 * �Ա� <code> RWLock </code> �Լ�InnoDB RWLock <code> rw_lock_t </code>�����ܡ�
 */
void RWLockBigTestCase::testRWLockVsInnoRWLock() {
	doRWLockMTTest(3600, 100, 20, 30, 100, USE_RWLOCK);
	doRWLockMTTest(3600, 100, 20, 30, 100, USE_INNO_RWLOCK);	
}

/** 
 * �°��д��<code>NewRWLock</code>���̲߳������ԣ���ʱ���ۺϲ���
 */
void RWLockBigTestCase::testRWLockMTStable() {
	//ѭ��������
	const int TEST_LOOPS = 12;
	const int TEST_TIME_SECONDS = 60 * 100;
	cout<<"�����°��д����Ԥ����Ҫ"<<TEST_LOOPS * TEST_TIME_SECONDS / 60<<"����"<<endl;
	for (int i = 0; i < TEST_LOOPS; i++) {
		//����TEST_TIME_SECONDS�룬20%д��������30%��ʱ��������ʱʱ��Ϊ100ms
		doRWLockMTTest(TEST_TIME_SECONDS, 20, 30, 100);

		cout<<"����:"<<TEST_LOOPS<<"-"<<(i+1)<<"���"<<endl;
	}
}

/** 
 * ����ȫ��
DB-10 ���Խ����
NewRWLockTestCase::testAllRead
�°棺
Time: 130447 ms
Throughput: 40 ops/ms
Max rlock time: 129,133,122 us
Max wlock time: 0 us
Avg rlock time: 674 us
InnoDB��
Time: 154312 ms
Throughput: 40 ops/ms
Max rlock time: 45,142,738 us
Max wlock time: 0 us
Avg rlock time: 158 us
 */
void RWLockBigTestCase::testAllRead() {
	doRWLockMTTest(120, 1, 0, 0, 0, RWLockBigTestCase::USE_RWLOCK);
	doRWLockMTTest(120, 1, 0, 0, 0, RWLockBigTestCase::USE_INNO_RWLOCK);
}

/** 
 * ����ȫд
 DB-10 ���Խ����
�°棺
Time: 120107 ms
Throughput: 40 ops/ms
Max rlock time: 0 us
Max wlock time: 44,611,718 us
Avg wlock time: 23556 us
InnoDb��
Time: 127289 ms
Throughput: 39 ops/ms
Max rlock time: 0 us
Max wlock time: 84,578,165 us
Avg wlock time: 23953 us

 */
void RWLockBigTestCase::testAllWrite() {
	doRWLockMTTest(120, 1, 100, 0, 0, RWLockBigTestCase::USE_RWLOCK);
	doRWLockMTTest(120, 1, 100, 0, 0, RWLockBigTestCase::USE_INNO_RWLOCK);
}

/** 
 * ����һ������д
10%:
New:
Time: 120125 ms
Throughput: 40 ops/ms
Max rlock time: 22766952 us
Max wlock time: 18543817 us
Avg rlock time: 21347 us
Avg wlock time: 45733 us
InnoDB
Time: 123282 ms
Throughput: 40 ops/ms
Max rlock time: 44839656 us
Max wlock time: 26473097 us
Avg rlock time: 24712 us
Avg wlock time: 14772 us

20%:

new:
Time: 120098 ms
Throughput: 40 ops/ms
Max rlock time: 20216835 us
Max wlock time: 21059483 us
Avg rlock time: 21567 us
Avg wlock time: 32701 us
innoDB:
Time: 121907 ms
Throughput: 40 ops/ms
Max rlock time: 27260707 us
Max wlock time: 18988363 us
Avg rlock time: 27872 us
Avg wlock time: 8196 us

30%
new:
Time: 120288 ms
Throughput: 40 ops/ms
Max rlock time: 19946422 us
Max wlock time: 26061700 us
Avg rlock time: 21722 us
Avg wlock time: 29366 us
innoDB:
Time: 120796 ms
Throughput: 40 ops/ms
Max rlock time: 9415774 us
Max wlock time: 3770118 us
Avg rlock time: 28074 us
Avg wlock time: 13367 us

40%:
new:
Time: 121342 ms
Throughput: 40 ops/ms
Max rlock time: 16050307 us
Max wlock time: 18159075 us
Avg rlock time: 21524 us
Avg wlock time: 27804 us
innoDB:
Time: 120319 ms
Throughput: 40 ops/ms
Max rlock time: 14124075 us
Max wlock time: 7057035 us
Avg rlock time: 30335 us
Avg wlock time: 14219 us

 */
void RWLockBigTestCase::testPercentWrite() {
	for (int p = 10; p < 50; p += 10) {
		cout<<"����"<<p<<"%д����"<<endl;
		doRWLockMTTest(120, 1, p, 0, 0, RWLockBigTestCase::USE_RWLOCK);
		doRWLockMTTest(120, 1, p, 0, 0, RWLockBigTestCase::USE_INNO_RWLOCK);
	}
}

/**
 * ���Ա���д����д����

10%��
new��
Time: 120111 ms
Throughput: 40 ops/ms
Max rlock time: 47210307 us
Max wlock time: 43083218 us
Avg rlock time: 23151 us
Avg wlock time: 26142 us
InnoDB:
Time: 121964 ms
Throughput: 40 ops/ms
Max rlock time: 44758045 us
Max wlock time: 30083872 us
Avg rlock time: 24812 us
Avg wlock time: 13409 us

20%:
new:
Time: 120150 ms
Throughput: 40 ops/ms
Max rlock time: 36456264 us
Max wlock time: 36433312 us
Avg rlock time: 23539 us
Avg wlock time: 24868 us
InnoDB:
Time: 120635 ms
Throughput: 40 ops/ms
Max rlock time: 35504442 us
Max wlock time: 28009951 us
Avg rlock time: 26835 us
Avg wlock time: 10144 us

30%:
new:
Time: 121259 ms
Throughput: 40 ops/ms
Max rlock time: 25214701 us
Max wlock time: 23613505 us
Avg rlock time: 23733 us
Avg wlock time: 24144 us
innoDB:
Time: 120855 ms
Throughput: 40 ops/ms
Max rlock time: 25224731 us
Max wlock time: 22260721 us
Avg rlock time: 28699 us
Avg wlock time: 11871 us

40%:
new:
Time: 120565 ms
Throughput: 40 ops/ms
Max rlock time: 18255951 us
Max wlock time: 18536996 us
Avg rlock time: 23594 us
Avg wlock time: 24616 us
innoDB:
Time: 120471 ms
Throughput: 40 ops/ms
Max rlock time: 11873169 us
Max wlock time: 3951740 us
Avg rlock time: 30171 us
Avg wlock time: 14010 us


 */
void RWLockBigTestCase::testPercentWritePreferWrite() {
	for (int p = 10; p < 50; p += 10) {
		cout<<"����"<<p<<"%д����"<<endl;
		doRWLockMTTest(120, 1, p, 0, 0, RWLockBigTestCase::USE_RWLOCK, true);
		doRWLockMTTest(120, 1, p, 0, 0, RWLockBigTestCase::USE_INNO_RWLOCK);
	}
}


/**
 * �°�RWLock���Ը����߳��ࡣ
 * @author ������(niemingjun@corp.netease.com, niemingjun@163.org)
 * @version 0.3
 */
template<typename LockType>
class NewRWLockMT: public Thread {
public:
	/**
	 * ���캯��
	 * @param lockArray		������
	 * @param numLock		������
	 * @param writePercent	д���İٷֱ���
	 * @param timedPercent	��ʱ���İٷֱ���
	 * @param timeoutMs		��ʱ������ʱʱ�������
	 * @param stopTime		�߳����е���ֹ����ʱ��
	 */
	NewRWLockMT(LockType **lockArray, size_t numLock, int writePercent, int timedPercent, int timeoutMs, time_t stopTime): Thread("NewRWLockMT") {
		m_lockArray = lockArray;
		m_numLock = numLock;
		m_writePercent = writePercent;
		m_timedPercent = timedPercent;
		m_timeoutMs = timeoutMs;
		m_stopTime = stopTime;
	}

	/** 
	 * ����
	 * @see <code>Thread::run()</code>
	 */
	void run() {
		System::srandom((int)System::microTime());
		int idx = (int)(System::random() % m_numLock);
		while (time(NULL) < m_stopTime) {
			LockMode mode = Shared;
			if (System::random() % 100 < m_writePercent)
				mode = Exclusived;
			u64 before = System::microTime();
			if (System::random() % 100 < m_timedPercent) {
				while (!RWTIMEDLOCK(m_lockArray[idx], mode, m_timeoutMs));
			} else {
				RWLOCK(m_lockArray[idx], mode);
			}
			u64 after = System::microTime();
			int lockTime = (int)(after - before);
			Atomic<int> *maxLockTime = &g_maxRLockTime;
			if (mode == Exclusived) {
				maxLockTime = &g_maxWLockTime;
			}
			while (lockTime > maxLockTime->get()) {
				int oldLockTime = maxLockTime->get();
				if (oldLockTime >= lockTime)
					break;
				if (maxLockTime->compareAndSwap(oldLockTime, lockTime))
					break;
			}
			
			if (Shared == mode) {
				g_totalRLockTime += lockTime;
				g_rlockCnt++;

				//�������Ƿ��𵽱������á�
				assert(RWLockBigTestCase::NUM_RLOCK_CHECK_VALUE == RWLockBigTestCase::testNumArr[idx]);

			} else {
				g_totalWLockTime += lockTime;
				g_wlockCnt++;
				//���д���Ƿ��𵽱������á�
				assert(RWLockBigTestCase::NUM_RLOCK_CHECK_VALUE == RWLockBigTestCase::testNumArr[idx]);
				RWLockBigTestCase::testNumArr[idx] = RWLockBigTestCase::NUM_WLOCK_CHECK_VALUE;
			}
			int newIdx = idx;
			for (int n = 0; n < 2000; n++)
				newIdx = (newIdx + 7 + m_id) % m_numLock;
			
			//����д���Ƿ��𵽱������á�
			if (Shared == mode) {
				assert(RWLockBigTestCase::NUM_RLOCK_CHECK_VALUE == RWLockBigTestCase::testNumArr[idx]);
			} else {
				assert(RWLockBigTestCase::NUM_WLOCK_CHECK_VALUE == RWLockBigTestCase::testNumArr[idx]);
				RWLockBigTestCase::testNumArr[idx] = RWLockBigTestCase::NUM_RLOCK_CHECK_VALUE;
			}
			
			RWUNLOCK(m_lockArray[idx], mode);
			idx = newIdx;
		}
	}

private:
	LockType	**m_lockArray;	/* �������� */
	size_t	m_numLock;			/* ������ */
	int		m_writePercent;		/* д�̰߳ٷֱ��� */
	int		m_timedPercent;		/* ��ʱ�̰߳ٷֱ��� */
	int		m_timeoutMs;		/* ��ʱ������ */
	time_t m_stopTime;			/* ֹͣʱ�� */
};

int *RWLockBigTestCase::testNumArr = NULL;

/** ִ���°��д�����̲߳���. ����NUM_THREADS=1000 ���̣߳�ʹ��NUM_TEST=95������
 * @param totalTime		�ܲ���ʱ�䣬��λ��
 * @param writePercent	д���ٷֱ�
 * @param timedPercent	����ʱ�����ٷֱ�
 * @param timeoutMs		����ʱ�����ĳ�ʱʱ�䣬��λ����
 */
void RWLockBigTestCase::doRWLockMTTest(int totalTime, int writePercent, int timedPercent, int timeoutMs) {
	//�Ƚ���̨ɨ��ȴ��̹߳ر�
	EventMonitorHelper::pauseMonitor();

	//��ʼ�����Ͳ�������
	RWLock	*locks[NUM_TEST];
	testNumArr = new int[NUM_TEST];
	for (int i = 0; i < sizeof(locks) / sizeof(locks[0]); i++) {
		locks[i] = new RWLock("NewRWLockMTTestStable", __FILE__, __LINE__);
		RWLockBigTestCase::testNumArr[i] = RWLockBigTestCase::NUM_RLOCK_CHECK_VALUE;
	}
	time_t stopTime = time(NULL) + totalTime;

	//��ʼ���߳�
	NewRWLockMT<RWLock>	*threads[NUM_THREADS];
	for (size_t i = 0; i < sizeof(threads) / sizeof(threads[0]); i++) {
		threads[i] = new NewRWLockMT<RWLock>(locks, sizeof(locks) / sizeof(locks[0]), writePercent, timedPercent, timeoutMs, stopTime);		
	}
	//���������߳�
	for (size_t i = 0; i < sizeof(threads) / sizeof(threads[0]); i++) {
		threads[i]->start();
	}
	//�ȴ������߳̽���
	for (size_t i = 0; i < sizeof(threads) / sizeof(threads[0]); i++) {
		threads[i]->join();
	}
	//�������Ϣ
	for (int i = 0; i < sizeof(locks) / sizeof(locks[0]); i++) {
		delete locks[i];
	}
	//�������������Դ
	delete[] testNumArr;
	testNumArr = NULL;
}

/** ִ�ж�д�����̲߳���
 * @param totalTime �ܲ���ʱ�䣬��λ��
 * @param runs �ֳɶ���
 * @param writePercent д���ٷֱ�
 * @param timedPercent ����ʱ�����ٷֱ�
 * @param timeoutMs ����ʱ�����ĳ�ʱʱ�䣬��λ����
 */
void RWLockBigTestCase::doRWLockMTTest(int totalTime, int runs, int writePercent, int timedPercent, int timeoutMs, int useLockType/*=USE_NEW_RWLOCK*/, bool preferWrite/*=false*/) {	

	//���ü�����
	g_rlockCnt = g_wlockCnt = 0;
	g_maxRLockTime.set(0);
	g_maxWLockTime.set(0);
	g_totalRLockTime = g_totalWLockTime = 0;

	u64 before = 0L;
	u64 after = 0L;

	RWLock	*newLocks[NUM_TEST];
	rw_lock_t	*innoLocks[NUM_TEST];

	RWLockMT<RWLock>	*newThreads[NUM_THREADS];
	InnoRWLockMT	*innoThreads[NUM_THREADS];
	switch (useLockType) {
		case USE_RWLOCK: //NewRWLock
			
			for (int i = 0; i < sizeof(newLocks) / sizeof(newLocks[0]); i++) {
				newLocks[i] = new RWLock("RWLockMTTestStable", __FILE__, __LINE__, preferWrite);
			}
			
			for (size_t i = 0; i < sizeof(newThreads) / sizeof(newThreads[0]); i++) {
				newThreads[i] = new RWLockMT<RWLock>(newLocks, sizeof(newLocks) / sizeof(newLocks[0]), writePercent, timedPercent, timeoutMs);
			}

			//��������
			before = System::currentTimeMillis();
			for (size_t i = 0; i < sizeof(newThreads) / sizeof(newThreads[0]); i++) {
				newThreads[i]->start();
			}
			
			for (int r = 0; r < runs; r++) {
				Thread::msleep(totalTime / runs * 1000);
				for (size_t i = 0; i < sizeof(newThreads) / sizeof(newThreads[0]); i++) {
					newThreads[i]->pause();
				}
				for (size_t i = 0; i < sizeof(newThreads) / sizeof(newThreads[0]); i++) {
					while (newThreads[i]->isRunning())
						Thread::msleep(10);
				}
				if (r < runs - 1) {
					cout << "run " << r + 1 << endl;
					for (size_t i = 0; i < sizeof(newThreads) / sizeof(newThreads[0]); i++) {
						newThreads[i]->signal();
					}
				}
			}
			for (size_t i = 0; i < sizeof(newThreads) / sizeof(newThreads[0]); i++) {
				newThreads[i]->stop();
			}

			for (size_t i = 0; i < sizeof(newThreads) / sizeof(newThreads[0]); i++) {
				newThreads[i]->join();
			}
			after = System::currentTimeMillis();
			
			for (size_t i = 0; i < sizeof(newThreads) / sizeof(newThreads[0]); i++) {
				delete newThreads[i];
			}
			
			cout << "Time: " << (after - before) << " ms" << endl;
			cout << "Throughput: " << (g_rlockCnt + g_wlockCnt) / (after - before) << " ops/ms" << endl;
			cout << "Max rlock time: " << g_maxRLockTime.get() << " us" << endl;
			cout << "Max wlock time: " << g_maxWLockTime.get() << " us" << endl;
			if (g_rlockCnt > 0) {
				cout << "Avg rlock time: " << g_totalRLockTime / g_rlockCnt << " us" << endl;
			}
			if (g_wlockCnt > 0) {
				cout << "Avg wlock time: " << g_totalWLockTime / g_wlockCnt << " us" << endl;
			}

		for (int i = 0; i < sizeof(newLocks) / sizeof(newLocks[0]); i++) {
				//newLocks[i]->getUsage()->print();
				delete newLocks[i];
			}

			break; 

		
		case USE_INNO_RWLOCK: //����InnoDBRWLock
			
			for (int i = 0; i < sizeof(innoLocks) / sizeof(innoLocks[0]); i++) {
				innoLocks[i] = (rw_lock_t *)malloc(sizeof(rw_lock_t));
				rw_lock_create_func(innoLocks[i], __FILE__, __LINE__);
			}

			//rw_lock_t **lockArray, size_t numLock, uint loopCount
			for (size_t i = 0; i < sizeof(innoThreads) / sizeof(innoThreads[0]); i++) {
				innoThreads[i] = new InnoRWLockMT(innoLocks, sizeof(innoLocks) / sizeof(innoLocks[0]), writePercent, timedPercent, timeoutMs);
			}

				//��������
			before = System::currentTimeMillis();
			for (size_t i = 0; i < sizeof(innoThreads) / sizeof(innoThreads[0]); i++) {
				innoThreads[i]->start();
			}
			
			for (int r = 0; r < runs; r++) {
				Thread::msleep(totalTime / runs * 1000);
				for (size_t i = 0; i < sizeof(innoThreads) / sizeof(innoThreads[0]); i++) {
					innoThreads[i]->pause();
				}
				for (size_t i = 0; i < sizeof(innoThreads) / sizeof(innoThreads[0]); i++) {
					while (innoThreads[i]->isRunning())
						Thread::msleep(10);
				}
				if (r < runs - 1) {
					for (size_t i = 0; i < sizeof(innoThreads) / sizeof(innoThreads[0]); i++) {
						innoThreads[i]->signal();
					}
				}
			}
			for (size_t i = 0; i < sizeof(innoThreads) / sizeof(innoThreads[0]); i++) {
				innoThreads[i]->stop();
			}

			for (size_t i = 0; i < sizeof(innoThreads) / sizeof(innoThreads[0]); i++) {
				innoThreads[i]->join();
			}
			after = System::currentTimeMillis();
			
			for (size_t i = 0; i < sizeof(innoThreads) / sizeof(innoThreads[0]); i++) {
				delete innoThreads[i];
			}
			cout<<"����InnoDB��RWLock���ܣ�"<<endl;
			cout << "Time: " << (after - before) << " ms" << endl;
			cout << "Throughput: " << (g_rlockCnt + g_wlockCnt) / (after - before) << " ops/ms" << endl;
			cout << "Max rlock time: " << g_maxRLockTime.get() << " us" << endl;
			cout << "Max wlock time: " << g_maxWLockTime.get() << " us" << endl;
			if (g_rlockCnt > 0) {
				cout << "Avg rlock time: " << g_totalRLockTime / g_rlockCnt << " us" << endl;
			}
			if (g_wlockCnt > 0) {
				cout << "Avg wlock time: " << g_totalWLockTime / g_wlockCnt << " us" << endl;
			}

			for (int i = 0; i < sizeof(innoLocks) / sizeof(innoLocks[0]); i++) {
				rw_lock_free(innoLocks[i]);
				free(innoLocks[i]);
			}	
	
			break;
		default:
			//nothing
			break;
	}

}
