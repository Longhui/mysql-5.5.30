#include <cppunit/config/SourcePrefix.h>
#include "misc/TestLockManager.h"
#include "misc/LockManager.h"
#include "misc/IndiceLockManager.h"
#include "util/Thread.h"
#include "util/Sync.h"
#include <iostream>
#include <list>

using namespace std;
using namespace ntse;

const char* LockManagerTestCase::getName() {
	return "LockManager test";
}

const char* LockManagerTestCase::getDescription() {
	return "Test Lock Manager.";
}

bool LockManagerTestCase::isBig() {
	return false;
}
/**
 * �����������
 *	case 1. tryLock����ͻ����
 *	case 2. ��ͬ�ؼ��ֵ�������ͻ
 */
void LockManagerTestCase::testLockOpers() {
	uint maxLocks = 100;
	uint slotCount = 16;
	uint resvedObjectPerSlot = 1;
	LockManager lm(maxLocks, slotCount, resvedObjectPerSlot);

	// trylocks
	CPPUNIT_ASSERT(lm.tryLock(NULL, 1, Shared));
	CPPUNIT_ASSERT(lm.tryLock(NULL, 1, Shared));
	CPPUNIT_ASSERT(!lm.tryLock(NULL, 1, Exclusived));
	lm.unlock(1, Shared);
	lm.unlock(1, Shared);

	CPPUNIT_ASSERT(lm.tryLock(NULL, 1, Exclusived));
	CPPUNIT_ASSERT(!lm.tryLock(NULL, 1, Exclusived));
	CPPUNIT_ASSERT(!lm.tryLock(NULL, 1, Shared));
	lm.unlock(1, Exclusived);

	// locks
	CPPUNIT_ASSERT(lm.lock(NULL, 1, Shared));
	CPPUNIT_ASSERT(lm.lock(NULL, 1, Shared));
	lm.unlock(1, Shared);
	lm.unlock(1, Shared);


	// ��ͬ�ؽ����ϼ�������Ӧ�ó�ͻ
	for (uint i = 0; i < maxLocks; ++i) {
		CPPUNIT_ASSERT(lm.lock(NULL, i, Exclusived));
	}
	for (uint i = 0; i < maxLocks; ++i) {
		lm.unlock(i, Exclusived);
	}

	for (uint i = 0; i < maxLocks; ++i) {
		if (i&0x1) {
			CPPUNIT_ASSERT(lm.lock(NULL, i, Exclusived));
		} else {
			CPPUNIT_ASSERT(lm.lock(NULL, i, Shared));
		}
	}
	for (uint i = 0; i < maxLocks; ++i) {
		if (i&0x1) 
			lm.unlock(i, Exclusived);
		else 
			lm.unlock(i, Shared);
	}
}

/**
 * ������������µ��������
 *	case 1. ����������ʱ��������һ���µĹؼ����ϼ������׳�NTSE_EC_TOO_MANY_ROWLOCK�쳣
 *	case 2. slot���������Ǹ������������Ը�����ʵ�ֵ���ȷ��
 */
void LockManagerTestCase::testSpecialLM() {
	{
		LockManager lm(1, 1, 0); // �������ֻ�ܷ�����������
		CPPUNIT_ASSERT(lm.lock(NULL, 100, Shared)); // �õ�embeded
		CPPUNIT_ASSERT(lm.lock(NULL, 101, Shared)); // �õ�ȫ�ֿ�������
		CPPUNIT_ASSERT(lm.lock(NULL, 101, Shared)); // ��ͬkey����ռ��������
		try {
			CPPUNIT_ASSERT(!lm.tryLock(NULL, 102, Shared));
			lm.lock(NULL, 102, Shared);
		} catch(NtseException &e) {
			CPPUNIT_ASSERT(e.getErrorCode() == NTSE_EC_TOO_MANY_ROWLOCK);
			lm.unlock(100, Shared);
			lm.unlock(101, Shared);
			lm.unlock(101, Shared);
		}
	}
	{
		LockManager lm(1); // �������ֻ�ܷ�����������
		CPPUNIT_ASSERT(lm.lock(NULL, 100, Shared)); // �õ�embeded
		CPPUNIT_ASSERT(lm.lock(NULL, 101, Shared)); // �õ�slot��������
		CPPUNIT_ASSERT(lm.lock(NULL, 102, Shared)); // �õ�ȫ�ֿ�������
		try {
			CPPUNIT_ASSERT(!lm.tryLock(NULL, 103, Shared));
			lm.lock(NULL, 103, Shared);
		} catch(NtseException &e) {
			CPPUNIT_ASSERT(e.getErrorCode() == NTSE_EC_TOO_MANY_ROWLOCK);
			lm.unlock(100, Shared);
			lm.unlock(101, Shared);
			lm.unlock(102, Shared);
		}
	}
	{ // slot��������Ƚϳ�
		uint reservedLockPerSlot = 20;
		LockManager lm(1, 1, reservedLockPerSlot);
		// �պ���������������
		for (uint i = 0; i < reservedLockPerSlot + 2; ++i)
			CPPUNIT_ASSERT(lm.lock(NULL, i, Shared));
		for (uint i = 0; i < reservedLockPerSlot + 2; ++i)
			lm.unlock(i, Shared);
	}
}
/**
 * ����IsLocked��������
 * ��������:
 *	case 1. δ����
 *	case 2. ��S����isLock��ģʽW
 *	case 3. ��S����isLock��ģʽR
 */
void LockManagerTestCase::testIsLocked() {
	uint maxLocks = 100;
	uint slotCount = 16;
	uint resvedObjectPerSlot = 1;
	LockManager lm(maxLocks, slotCount, resvedObjectPerSlot);

	lm.lock(1, 1, Shared);
	lm.unlock(1, Shared);
	CPPUNIT_ASSERT(!lm.isSharedLocked(1));
	CPPUNIT_ASSERT(!lm.isExclusivedLocked(1, 1));

	lm.lock(1, 1, Shared);
	CPPUNIT_ASSERT(lm.isSharedLocked(1));
	CPPUNIT_ASSERT(!lm.isExclusivedLocked(1, 1));
	lm.unlock(1, Shared);

	lm.lock(1, 1, Exclusived);
	CPPUNIT_ASSERT(!lm.isSharedLocked(1));
	CPPUNIT_ASSERT(lm.isExclusivedLocked(1, 1));
	lm.unlock(1, Exclusived);
}

/** �����߳� */
class LockThread : public Thread {
public:
	LockThread(LockManager *lm, u64 key, LockMode mode, int timeOut = -1)
		: Thread("LockThread"), m_lm(lm), m_key(key), m_mode(mode)
		, m_timeOut(timeOut), m_lockFinish(false)
	{}
	/** �����ɹ�֮�����ñ���m_lockFinishΪtrue */
	void run() {
		m_lm->lock(NULL, m_key, m_mode, m_timeOut);
		m_lockFinish = true;
		m_lm->unlock(m_key, m_mode);
	}
public:
	bool m_lockFinish;
private:
	LockManager		*m_lm;
	u64				m_key;
	LockMode		m_mode;
	int				m_timeOut;
};
/**
 * ��������ͻ����
 * �ֱ���ԡ�д-����������-д������д-д�������ֳ�ͻ������֤����-��������ͻ
 * �������̣�
 *	1. ���̼߳�X��
 *	2. ���������߳�
 *	3. �����̼߳�R��
 *	4. ���߳�С˯һ���
 *	5. ��֤�����̼߳������ɹ�
 *	6. ���̷߳���
 *	7. ���߳�С˯һ���
 *	8. ��֤�����̼߳����ɹ�
 */
void LockManagerTestCase::testConflict() {
	Event e;
	LockManager lm(9);
	u64 key = 10;
	int timeOut[2] = {1, 2000};
	for (int i = 0; i < sizeof(timeOut) / sizeof(timeOut[0]); ++i) {
		{
			lm.lock(NULL, key, Exclusived);
			LockThread thr(&lm, key, Shared);
			thr.start();
			e.wait(timeOut[i]);
			CPPUNIT_ASSERT(!thr.m_lockFinish);
			lm.unlock(key, Exclusived);
			e.wait(timeOut[i]);
			CPPUNIT_ASSERT(thr.m_lockFinish);
			thr.join();
		}
		{
			lm.lock(NULL, key, Exclusived);
			LockThread thr(&lm, key, Shared, 10000);
			thr.start();
			e.wait(timeOut[i]);
			CPPUNIT_ASSERT(!thr.m_lockFinish);
			lm.unlock(key, Exclusived);
			e.wait(timeOut[i]);
			CPPUNIT_ASSERT(thr.m_lockFinish);
			thr.join();
		}
		{
			lm.lock(NULL, key, Exclusived);
			LockThread thr(&lm, key, Exclusived);
			thr.start();
			e.wait(timeOut[i]);
			CPPUNIT_ASSERT(!thr.m_lockFinish);
			lm.unlock(key, Exclusived);
			e.wait(timeOut[i]);
			CPPUNIT_ASSERT(thr.m_lockFinish);
			thr.join();
		}
		{
			lm.lock(NULL, key, Shared);
			LockThread thr(&lm, key, Exclusived);
			thr.start();
			e.wait(timeOut[i]);
			CPPUNIT_ASSERT(!thr.m_lockFinish);
			lm.unlock(key, Shared);
			e.wait(timeOut[i]);
			CPPUNIT_ASSERT(thr.m_lockFinish);
			thr.join();
		}
		{
			lm.lock(NULL, key, Shared);
			LockThread thr(&lm, key, Shared);
			thr.start();
			e.wait(timeOut[i]);
			CPPUNIT_ASSERT(thr.m_lockFinish);
			lm.unlock(key, Shared);
			thr.join();
		}
	}
}


const char* LockManagerBigTest::getName() {
	return "LockManager performance test";
}


const char* LockManagerBigTest::getDescription() {
	return "Test performance of LockManager.";
}


bool LockManagerBigTest::isBig() {
	return true;
}

/**
 * ���Ե��̼߳���Ч�ʣ�ֻ�Ե�һ�ؼ��ּ�����
 * 2008/07/18
 * rlock/unlock 191
 * wlock/unlock 188
 * 
 * after inline Hash function
 * rlock/unlock 188
 * wlock/unlock 184
 * 
 * after inline Equal Function, and add embdededLockObject
 * rlock/unlock 182
 * wlock/unlock 177
 *
 * �����ϣͰ% -> &
 * rlock/unlock 104
 * wlock/unlock 110
 *
 * ����һ��lockObject->init()����
 * rlock/unlock 94
 * wlock/unlock 95
 */
void LockManagerBigTest::testSKST()
{
	cout << "  Test performance of LockManager SingleKey(Shared)" << endl;


	int repeat = 10000000;
	uint maxLocks = (1<<10);
	LockManager lm(maxLocks);

	u64 before = System::clockCycles();
	for (int i = 0; i < repeat; i++) {
		lm.lock(NULL, 1, Shared);
		lm.unlock(1, Shared);
	}	
	u64 after = System::clockCycles();
	cout << "  clock cycles per lock/unlock pair: " << (after - before) / repeat << endl;

	cout << "  Test performance of LockManager SingleKey(Exclusive)" << endl;

	before = System::clockCycles();
	for (int i = 0; i < repeat; i++) {
		lm.lock(NULL, 1, Exclusived);
		lm.unlock(1, Exclusived);
	}	
	after = System::clockCycles();
	cout << "  clock cycles per lock/unlock pair: " << (after - before) / repeat << endl;
}

/**
 * ���Ե��̼߳���Ч�ʣ��Բ�ͬ�ؼ��ּ���)
 */
void LockManagerBigTest::testScanST()
{
	cout << "  Test performance of LockManager Scan(Shared)" << endl;


	int repeat = 1000000;
	uint maxLocks = 100;
	LockManager lm(100);

	u64 before = System::clockCycles();
	for (int i = 0; i < repeat; i++) {
		lm.lock(NULL, repeat, Shared);
		lm.unlock(repeat, Shared);
	}	
	u64 after = System::clockCycles();
	cout << "  clock cycles per lock/unlock pair: " << (after - before) / repeat << endl;

	cout << "  Test performance of LockManager Scan(Exclusive)" << endl;

	before = System::clockCycles();
	for (int i = 0; i < repeat; i++) {
		lm.lock(NULL, repeat, Exclusived);
		lm.unlock(repeat, Exclusived);
	}	
	after = System::clockCycles();
	cout << "  clock cycles per lock/unlock pair: " << (after - before) / repeat << endl;
}

class LockMT: public Thread {
public:
	LockMT(LockManager *lm, u64 key, uint loopCount, u64 *shared): Thread("LockManagerMT") {
		m_lm = lm;
		m_loopCount = loopCount;
		m_key = key;
		m_shared = shared;
		assert(loopCount % 2 == 0);
	}

	void run() {
		u64 v = 0;
		for (uint i = 0; i < m_loopCount; i++) {
			if (i % 2 == 0) {
				m_lm->lock(NULL, m_key, Shared);
				v = *m_shared;
				uint dummy = 0;
				for (uint j = 0; j < 100; ++j) {
					dummy = dummy * j / 1023;
				}
				CPPUNIT_ASSERT(*m_shared == v);
				m_lm->unlock(m_key, Shared);
			} else {
				m_lm->lock(NULL, m_key, Exclusived);
				v = *m_shared;
				uint dummy = 0;
				for (uint j = 0; j < 100; ++j) {
					dummy = dummy * j / 1023;
				}
				CPPUNIT_ASSERT(*m_shared == v);
				*m_shared = v + 1;
				m_lm->unlock(m_key, Exclusived);
			}
		}
	}

private:
	LockManager *m_lm;
	size_t		m_numLock;
	uint		m_loopCount;
	u64			m_key;
	u64*		m_shared;
};
/** 
 * ���Զ��̼߳ӽ�����ȷ��
 * ��������
 *	1. ����100�������߳�
 *	2. ÿ���߳�5000�ζ�ȡ�������sharedValue(�Ӷ�������5000��++sharedValue����д����
 *	3. ���������߳�
 *	4. ��֤sharedValue == 5000 * 100
 */
void LockManagerBigTest::testMT() {
	LockMT	*threads[100];
	uint loopCount = 10000;
	LockManager lm(100);
	u64 key = 16;
	u64 sharedValue = 0;
	size_t threadCount = sizeof(threads) / sizeof(threads[0]);
	for (size_t i = 0; i < threadCount; i++)
		threads[i] = new LockMT(&lm, key, loopCount, &sharedValue);

	for (size_t i = 0; i < threadCount; i++)
		threads[i]->start();

	for (size_t i = 0; i < threadCount; i++)
		threads[i]->join();

	for (size_t i = 0; i < threadCount; i++) 
		delete threads[i];

	CPPUNIT_ASSERT(sharedValue == (u64)loopCount * threadCount / 2);
}


static u64			g_rlockCnt = 0;
static u64			g_wlockCnt = 0;
static u64			g_totalRLockTime = 0;
static u64			g_totalWLockTime = 0;
static Atomic<int>	g_maxRLockTime;
static Atomic<int>	g_maxWLockTime;
static u64			g_slowLockThreshold = 10;	// in milliseconds
static u64			g_slowLockCnt = 0;

/** �߳�ͻ�����ʲ����߳� */
class HCTTestThread: public Thread {
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
	HCTTestThread(bool lmOrRwlock, LockManager *lm, u64 key, Mutex *lock, time_t stopTime): Thread("HCTTestThread") {
		m_lmOrRwlock = lmOrRwlock;
		m_lm = lm;
		m_key = key;
		m_lock = lock;
		m_stopTime = stopTime;
	}

	/** 
	 * ����
	 * @see <code>Thread::run()</code>
	 */
	void run() {
		System::srandom((int)System::microTime());
		while (time(NULL) < m_stopTime) {
			u64 before = System::microTime();
			if (m_lmOrRwlock)
				m_lm->lock(0, m_key, Exclusived);
			else
				m_lock->lock(/*Exclusived,*/ __FILE__, __LINE__);
			u64 after = System::microTime();
			int lockTime = (int)(after - before);
			if (lockTime > g_slowLockThreshold * 1000)
				g_slowLockCnt++;
			while (lockTime > g_maxWLockTime.get()) {
				int oldLockTime = g_maxWLockTime.get();
				if (oldLockTime >= lockTime)
					break;
				if (g_maxWLockTime.compareAndSwap(oldLockTime, lockTime))
					break;
			}
			
			g_totalWLockTime += lockTime;
			g_wlockCnt++;
			int loops = 1000 + System::random() % 1000;
			for (int n = 0; n < loops; n++)
				m_dummy = (m_dummy + 7 + m_id) % 97;
			
			if (m_lmOrRwlock)	
				m_lm->unlock(m_key, Exclusived);
			else
				m_lock->unlock(/*Exclusived*/);
		}
	}

private:
	LockManager	*m_lm;		/** �������� */
	u64			m_key;		/** Ҫ�����ļ�ֵ */
	bool		m_lmOrRwlock;
	Mutex		*m_lock;	
	int			m_dummy;
	time_t m_stopTime;		/* ֹͣʱ�� */
};

/** 100���߳̾���ͬһ��д���������ڸ߳�ͻʱ���������������
 */
void LockManagerBigTest::testHighConflictThroughput() {
	u64 before = 0L;
	u64 after = 0L;	

	HCTTestThread	*threads[100];
	LockManager lm(100);
	//RWLock	lock(__FUNCTION__, __FILE__, __LINE__);
	Mutex	lock(__FUNCTION__, __FILE__, __LINE__);
	u64 key = 16;
	size_t threadCount = sizeof(threads) / sizeof(threads[0]);
	int testDuration = 120;

	cout << "Test performance of lock manager for " << testDuration << " seconds" << endl;

	g_rlockCnt = g_wlockCnt = 0;
	g_slowLockCnt = 0;
	g_maxRLockTime.set(0);
	g_maxWLockTime.set(0);
	g_totalRLockTime = g_totalWLockTime = 0;

	for (size_t i = 0; i < threadCount; i++)
		threads[i] = new HCTTestThread(true, &lm, key, &lock, time(NULL) + testDuration);

	before = System::currentTimeMillis();
	for (size_t i = 0; i < threadCount; i++)
		threads[i]->start();

	for (size_t i = 0; i < threadCount; i++)
		threads[i]->join();
	after = System::currentTimeMillis();

	for (size_t i = 0; i < threadCount; i++) 
		delete threads[i];

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
	cout << "Slow locks: " << g_slowLockCnt << endl;

	cout << "Test performance of rwlock" << endl;

	g_rlockCnt = g_wlockCnt = 0;
	g_slowLockCnt = 0;
	g_maxRLockTime.set(0);
	g_maxWLockTime.set(0);
	g_totalRLockTime = g_totalWLockTime = 0;

	for (size_t i = 0; i < threadCount; i++)
		threads[i] = new HCTTestThread(false, &lm, key, &lock, time(NULL) + testDuration);

	before = System::currentTimeMillis();
	for (size_t i = 0; i < threadCount; i++)
		threads[i]->start();

	for (size_t i = 0; i < threadCount; i++)
		threads[i]->join();
	after = System::currentTimeMillis();

	for (size_t i = 0; i < threadCount; i++) 
		delete threads[i];

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
	cout << "Slow locks: " << g_slowLockCnt << endl;
}


/************************************************************************/
/* ��������ʹ�õ��������ȷ�Ժ�����                                     */
/************************************************************************/

const char* IndicesLockManagerTestCase::getName() {
	return "IndicesLockManager test";
}

const char* IndicesLockManagerTestCase::getDescription() {
	return "Test Indices Lock Manager.";
}

bool IndicesLockManagerTestCase::isBig() {
	return false;
}

/**
* ����˳���������ȷ�Ժ����ܣ������ܵĻ�������
* @case 1: �������������ӷ���������tryLock���������в������ɹ���Ȼ����ִ��һ����ͬ˳���Ծɳɹ�����֤������Ϣ���µ���ȷ��
* @case 2: �������������Ӷ����������ĳ��������ڱ�����ʹ�ô��������������ȫ�ֱ�������ʹ����ȷ
*/
void IndicesLockManagerTestCase::testSequenceLock() {
	IndicesLockManager ilm(1024, 100, 20, 5);
	u64 txnId1 = 0, txnId2 = 1;

	// ��������������������
	CPPUNIT_ASSERT(ilm.tryLock(txnId1, 10));
	CPPUNIT_ASSERT(ilm.tryLock(txnId1, 20));
	CPPUNIT_ASSERT(ilm.tryLock(txnId1, 30));
	CPPUNIT_ASSERT(!ilm.tryLock(txnId2, 10));
	CPPUNIT_ASSERT(!ilm.tryLock(txnId2, 20));
	CPPUNIT_ASSERT(!ilm.tryLock(txnId2, 30));
	CPPUNIT_ASSERT(ilm.lock(txnId2, 35));
	CPPUNIT_ASSERT(!ilm.tryLock(txnId1, 35));
	CPPUNIT_ASSERT(ilm.unlock(txnId1, 20));
	CPPUNIT_ASSERT(ilm.lock(txnId1, 40));
	CPPUNIT_ASSERT(ilm.lock(txnId2, 45));
	CPPUNIT_ASSERT(ilm.unlock(txnId2, 35));
	CPPUNIT_ASSERT(ilm.lock(txnId2, 55));
	CPPUNIT_ASSERT(ilm.lock(txnId1, 50));
	CPPUNIT_ASSERT(ilm.unlock(txnId1, 50));
	CPPUNIT_ASSERT(ilm.lock(txnId1, 60));
	CPPUNIT_ASSERT(ilm.unlock(txnId1, 30));
	CPPUNIT_ASSERT(ilm.isLocked(txnId1, 10));
	CPPUNIT_ASSERT(ilm.isLocked(txnId1, 40));
	CPPUNIT_ASSERT(!ilm.isLocked(txnId1, 20));
	CPPUNIT_ASSERT(!ilm.isLocked(txnId1, 30));
	CPPUNIT_ASSERT(!ilm.isLocked(txnId1, 35));
	CPPUNIT_ASSERT(!ilm.isLocked(txnId1, 55));
	CPPUNIT_ASSERT(!ilm.isLocked(txnId1, 26));
	CPPUNIT_ASSERT(ilm.isHoldingLocks(txnId1));
	CPPUNIT_ASSERT(ilm.isHoldingLocks(txnId2));
	CPPUNIT_ASSERT(ilm.whoIsHolding(10) == txnId1);
	CPPUNIT_ASSERT(ilm.whoIsHolding(40) == txnId1);
	CPPUNIT_ASSERT(ilm.whoIsHolding(45) == txnId2);
	CPPUNIT_ASSERT(ilm.unlockAll(txnId1));
	CPPUNIT_ASSERT(ilm.unlockAll(txnId2));
	CPPUNIT_ASSERT(ilm.whoIsHolding(10) == INVALID_TXN_ID);
	CPPUNIT_ASSERT(ilm.whoIsHolding(40) == INVALID_TXN_ID);
	CPPUNIT_ASSERT(ilm.whoIsHolding(45) == INVALID_TXN_ID);
	CPPUNIT_ASSERT(!ilm.isHoldingLocks(txnId1));
	CPPUNIT_ASSERT(!ilm.isHoldingLocks(txnId2));
	// ��ִ��һ����ͬ�Ĳ�������֤�����Ծ���ȷ
	CPPUNIT_ASSERT(ilm.lock(txnId1, 10));
	CPPUNIT_ASSERT(ilm.lock(txnId1, 20));
	CPPUNIT_ASSERT(ilm.lock(txnId1, 30));
	CPPUNIT_ASSERT(ilm.lock(txnId2, 35));
	CPPUNIT_ASSERT(ilm.unlock(txnId1, 20));
	CPPUNIT_ASSERT(ilm.lock(txnId1, 40));
	CPPUNIT_ASSERT(ilm.lock(txnId2, 45));
	CPPUNIT_ASSERT(ilm.unlock(txnId2, 35));
	CPPUNIT_ASSERT(ilm.lock(txnId2, 55));
	CPPUNIT_ASSERT(ilm.lock(txnId1, 50));
	CPPUNIT_ASSERT(ilm.unlock(txnId1, 50));
	CPPUNIT_ASSERT(ilm.lock(txnId1, 60));
	CPPUNIT_ASSERT(ilm.unlock(txnId1, 30));
	CPPUNIT_ASSERT(ilm.unlockAll(txnId1));
	CPPUNIT_ASSERT(ilm.unlockAll(txnId2));
	// �Ӹ������������ĳ��Entry���ű�
	CPPUNIT_ASSERT(ilm.lock(txnId1, 10));
	CPPUNIT_ASSERT(ilm.lock(txnId1, 30));
	CPPUNIT_ASSERT(ilm.lock(txnId1, 50));
	CPPUNIT_ASSERT(ilm.lock(txnId1, 70));
	CPPUNIT_ASSERT(ilm.lock(txnId1, 90));
	CPPUNIT_ASSERT(ilm.lock(txnId1, 110));
	CPPUNIT_ASSERT(ilm.lock(txnId1, 130));
	CPPUNIT_ASSERT(ilm.lock(txnId1, 150));
	CPPUNIT_ASSERT(ilm.lock(txnId1, 170));
	CPPUNIT_ASSERT(ilm.lock(txnId1, 190));
	CPPUNIT_ASSERT(ilm.lock(txnId1, 210));
	CPPUNIT_ASSERT(ilm.lock(txnId1, 230));
	CPPUNIT_ASSERT(ilm.lock(txnId1, 250));
	CPPUNIT_ASSERT(ilm.lock(txnId1, 270));
	CPPUNIT_ASSERT(ilm.lock(txnId1, 290));
	CPPUNIT_ASSERT(ilm.lock(txnId2, 20));
	CPPUNIT_ASSERT(ilm.lock(txnId2, 40));
	CPPUNIT_ASSERT(ilm.lock(txnId2, 60));
	CPPUNIT_ASSERT(ilm.lock(txnId2, 80));
	CPPUNIT_ASSERT(ilm.lock(txnId2, 100));
	CPPUNIT_ASSERT(ilm.lock(txnId2, 120));
	CPPUNIT_ASSERT(ilm.lock(txnId2, 140));
	CPPUNIT_ASSERT(ilm.lock(txnId2, 160));
	CPPUNIT_ASSERT(ilm.lock(txnId2, 180));
	CPPUNIT_ASSERT(ilm.lock(txnId2, 200));
	CPPUNIT_ASSERT(ilm.lock(txnId2, 220));
	CPPUNIT_ASSERT(ilm.lock(txnId2, 240));
	CPPUNIT_ASSERT(ilm.lock(txnId2, 260));
	CPPUNIT_ASSERT(ilm.lock(txnId2, 280));
	CPPUNIT_ASSERT(ilm.lock(txnId2, 300));
	CPPUNIT_ASSERT(ilm.unlockAll(txnId1));
	CPPUNIT_ASSERT(ilm.unlockAll(txnId2));

	// ���������С�����
	IndicesLockManager ilm1(1024, 1);
	// ���ʱ�������Ӧ��ֻ�ܳ��������������ĸ��������ʧ��
	CPPUNIT_ASSERT(ilm1.lock(txnId2, 30));
	CPPUNIT_ASSERT(ilm1.lock(txnId2, 40));
	CPPUNIT_ASSERT(ilm1.lock(txnId2, 50));
	try {
		ilm1.lock(txnId2, 60);
		CPPUNIT_ASSERT(false);
	} catch (NtseException &e) {
		CPPUNIT_ASSERT(e.getErrorCode() == NTSE_EC_TOO_MANY_ROWLOCK);
	}
	CPPUNIT_ASSERT(ilm1.unlockAll(txnId2));
}


/**
* �������������
* @case 1: ���������ӷ�100�������ƽ������
*/
void IndicesLockManagerBigTestCase::testSequenceLockPerformance() {
	// ���������ӷ�100w����ͬ�����������
	IndicesLockManager bigilm(1024, 100);
	u64 before, after;
	u64 repeat = 1000000;
	u64 txnId1 = 0;

	//LockManager lm(100);
	//before = System::clockCycles();
	//for (int i = 0; i < repeat; i++) {
	//	lm.lock(NULL, i, Shared);
	//	//lm.lock(NULL, i + 64, Shared);
	//	lm.unlock(i, Shared);
	//	//lm.unlock(i + 64, Shared);
	//}	
	//after = System::clockCycles();
	//cout << "  clock cycles per lock/unlock pair: " << (after - before) / repeat << endl;

	before = System::clockCycles();
	for (uint i = 0; i < repeat; i++) {
		bigilm.lock(txnId1, i);
		//bigilm.lock(txnId1, i + 64);
		bigilm.unlock(txnId1, i);
		//bigilm.unlock(txnId1, i + 64);
	}
	after = System::clockCycles();
	cout << "  clock cycles per lock/unlock pair: " << (after - before) / repeat << endl;

	before = System::clockCycles();
	for (uint i = 0; i < repeat; i++) {
		bigilm.tryLock(txnId1, i);
		//bigilm.lock(txnId1, i + 64);
		bigilm.unlock(txnId1, i);
		//bigilm.unlock(txnId1, i + 64);
	}
	after = System::clockCycles();
	cout << "  clock cycles per trylock/unlock pair: " << (after - before) / repeat << endl;
}


/**
* ģ���μ�����һ�η��������ܲ���
* @case 1: ����ģ�����������������ȼ����ɸ�����һ���ͷŵ�100��β�����ƽ������
*/
void IndicesLockManagerBigTestCase::testRangeLockPerformance() {
	IndicesLockManager bigilm(1024, 100);
	u64 before, after;
	u64 repeat = 1000000;
	u64 txnId1 = 0;

	// ���Լ��������һ���Է����ĳ���������ÿ�����̶߳�����10������Ȼ��һ���Էŵ�
	// �����ģ����10w���ã�ÿ�����Ԥ��20������20�����
	IndicesLockManager bigilm1(1024, 100000, 20, 20);
	before = System::clockCycles();
	for (uint i = 0; i < repeat; i++) {
		bigilm1.tryLock(txnId1, i);
		bigilm1.tryLock(txnId1, i + 1);
		bigilm1.tryLock(txnId1, i + 2);
		bigilm1.tryLock(txnId1, i + 3);
		bigilm1.tryLock(txnId1, i + 4);
		bigilm1.tryLock(txnId1, i + 5);
		bigilm1.tryLock(txnId1, i + 6);
		bigilm1.tryLock(txnId1, i + 7);
		bigilm1.tryLock(txnId1, i + 8);
		bigilm1.tryLock(txnId1, i + 9);
		bigilm1.unlockAll(txnId1);
	}
	after = System::clockCycles();
	cout << "  clock cycles per lock/unlock pair: " << (after - before) / repeat << endl;
}



class ILockThread : public Thread {
public:
	ILockThread(IndicesLockManager *ilm) : Thread("ILockThread"), m_mutex("ILockThreadMutex", __FILE__, __LINE__) {
		m_ilm = ilm;
		m_txnId = INVALID_TXN_ID;
		m_key = 0;
		m_success = false;
		m_command = (u8)-1;
		m_busy = false;
	}

	void run() {
		while (true) {
			u64 txnId;
			u64 key;
			u8 command;
			bool doCommand = false;
			LOCK(&m_mutex);
			if (m_command != (u8)-1) {
				if (m_command == EXIT_MARK) {
					UNLOCK(&m_mutex);
					break;
				}
				doCommand = true;
				txnId = m_txnId;
				key = m_key;
				command = m_command;
			}
			UNLOCK(&m_mutex);

			if (doCommand) {
				if (command == 0) {	// ����
					m_success = m_ilm->lock(txnId, key);
				} else if (command == 1) {
					m_success = m_ilm->unlock(txnId, key);
				} else if (command == 2) {
					m_success = m_ilm->unlockAll(txnId);
				} else if (command == 3) {
					m_success = m_ilm->tryLock(txnId, key);
				} else {
				}
			}
			LOCK(&m_mutex);
			m_busy = false;
			if (m_command != EXIT_MARK)
				m_command = (u8)-1;
			UNLOCK(&m_mutex);

			Thread::msleep(100);
		}
	}

	/**
	* �õ�ǰһ��ָ���Ƿ�ִ�н���״̬
	*/
	bool getFinish() {
		MutexGuard guard(&m_mutex, __FILE__, __LINE__);
		return !m_busy;
	}

	/**
	* �õ�Ƕһ��ָ������ĳɹ����
	*/
	bool getResult() {
		MutexGuard guard(&m_mutex, __FILE__, __LINE__);
		if (m_busy)
			return false;
		return m_success;
	}

	/**
	* ָ�����������ĳ������
	* @param txnId		�����
	* @param key		���������ֵ
	* @param command	�������ָ����
	*/
	void setLock(u64 txnId, u64 key, u8 command) {
		LOCK(&m_mutex);
		m_txnId = txnId;
		m_key = key;
		m_command = command;
		m_busy = true;
		UNLOCK(&m_mutex);
	}

	/**
	* ���õ�ǰ�߳��˳�
	*/
	void setExit() {
		LOCK(&m_mutex);
		m_command = EXIT_MARK;
		UNLOCK(&m_mutex);
	}

private:
	IndicesLockManager	*m_ilm;			/** �����߳�ʹ�õ�������� */

	Mutex				m_mutex;		/** ����������������Ͷ�ȡ��������Ļ����� */
	u8					m_command;		/** 0��ʾ������1��ʾ������2��ʾ�����е�����3��ʾ�߳̽��� */
	u64					m_txnId;		/** ĳ�β���ʹ�õ������ */
	u64					m_key;			/** ���������ֵ */
	bool				m_success;		/** �����Ƿ�ɹ������ɹ�˵�������� */
	bool				m_busy;			/** ��ʶ��ǰ�߳�״̬��true��ʾ����ִ�в�����false��ʾ״̬���� */

	static const u8 EXIT_MARK = (u8)-2;	/** �߳��˳������־ **/
};

/**
* ָ��ͨ��ĳ���߳̽ӿ�ִ��ĳ���ӷ���������������֤�������ȷ��
* @param ilt		����������߳�
* @param txnId		���������
* @Param key		������ֵ
* @param command	�����ľ�������
* @param success	
*/
bool controlLockThread(ILockThread *ilt, u64 txnId, u64 key, u8 command, bool success) {
	ilt->setLock(txnId, key, command);
	Thread::msleep(1000);
	if (success) {
		return (ilt->getFinish() && ilt->getResult());
	} else {
		// trylockҪ���⴦��
		if (command == 3)
			return (ilt->getFinish() && !ilt->getResult());
		return (!ilt->getFinish());
	}
}

/**
* ���Լ����ĳ�ͻ�ԣ�ĳ������һ����һ���̼߳����ɹ��������̱߳���ȴ�
* @case 1:���Բ�ͬ�̼߳Ӳ�ͬ�����ǲ����ͻ��
* @case 2:���Բ�ͬ�̼߳�ͬһ�������ǻ��ͻ�ģ�ͬʱ�������������ȷ��
*/
void IndicesLockManagerTestCase::testConflict() {
	IndicesLockManager ilm(1024, 100);

	u64 txnId1 = 0;
	u64 txnId2 = 1;
	u64 txnId3 = 2;

	ILockThread ilt1(&ilm);
	ILockThread ilt2(&ilm);
	ILockThread ilt3(&ilm);

	ilt1.start();
	ilt2.start();
	ilt3.start();

	// ���Բ�ͬ�̼߳Ӳ�ͬ������Ӧ�ö��ܹ��ɹ�
	CPPUNIT_ASSERT(controlLockThread(&ilt1, txnId1, 10, 0, true));
	CPPUNIT_ASSERT(controlLockThread(&ilt1, txnId1, 11, 0, true));
	CPPUNIT_ASSERT(controlLockThread(&ilt1, txnId1, 20, 0, true));
	CPPUNIT_ASSERT(controlLockThread(&ilt2, txnId2, 31, 0, true));
	CPPUNIT_ASSERT(controlLockThread(&ilt3, txnId3, 40, 0, true));
	CPPUNIT_ASSERT(controlLockThread(&ilt3, txnId3, 41, 0, true));

	CPPUNIT_ASSERT(controlLockThread(&ilt1, txnId1, 0, 2, true));
	CPPUNIT_ASSERT(controlLockThread(&ilt2, txnId2, 0, 2, true));
	CPPUNIT_ASSERT(controlLockThread(&ilt3, txnId3, 0, 2, true));

	// ���Լ�������һ����ĳ���̼߳������ϣ������̱߳���Ӳ�������ͬʱ�������������ȷ��

	CPPUNIT_ASSERT(controlLockThread(&ilt1, txnId1, 10, 3, true));	// ����1��10���Լ����ɹ�

	CPPUNIT_ASSERT(controlLockThread(&ilt2, txnId2, 10, 0, false));	// ����2��10�����ȴ�

	CPPUNIT_ASSERT(controlLockThread(&ilt3, txnId3, 10, 0, false));	// ����3��10�����ȴ�

	CPPUNIT_ASSERT(controlLockThread(&ilt1, txnId1, 10, 0, true));	// ����1��������
	CPPUNIT_ASSERT(controlLockThread(&ilt1, txnId1, 10, 0, true));	// ����1��������
	CPPUNIT_ASSERT(controlLockThread(&ilt1, txnId1, 10, 3, true));	// ����1���Լ�������

	CPPUNIT_ASSERT(controlLockThread(&ilt1, txnId1, 10, 1, true));	// ����1����1��

	CPPUNIT_ASSERT(!ilt2.getFinish() && !ilt3.getFinish());			// ��������2/3��ʱ����Ҫ�ȴ�

	CPPUNIT_ASSERT(controlLockThread(&ilt1, txnId1, 0, 2, true));	// ����1ȫ������

	CPPUNIT_ASSERT(ilt2.getFinish() && ilt2.getResult());			// ��������2��ʱ���Լ����ɹ�
	CPPUNIT_ASSERT(!ilt3.getFinish());								// ��������3�Ծ��ڵȴ�

	CPPUNIT_ASSERT(controlLockThread(&ilt2, txnId2, 10, 1, true));	// ����2����

	CPPUNIT_ASSERT(ilt3.getFinish() && ilt3.getResult());			// ��������3��ʱ���Լ����ɹ�

	CPPUNIT_ASSERT(controlLockThread(&ilt3, txnId3, 10, 1, true));	// ����3����

	CPPUNIT_ASSERT(ilm.lock(txnId1, 10));						// ���Դ�ʱû�˼����������Գɹ�
	CPPUNIT_ASSERT(ilm.unlock(txnId1, 10));						// ���Է����ɹ�

	ilt1.setExit();
	ilt2.setExit();
	ilt3.setExit();
	ilt1.join();
	ilt2.join();
	ilt3.join();
}


/**
* ��������������ȷ��
* @case 1: ����������3����������������ȷ��
* @case 2: ���������������ڱ��������֮������ȴ���ϵ��ɵģ����ʱ���������ɼ���̷߳���
*/
void IndicesLockManagerTestCase::testDeadLock() {
	IndicesLockManager ilm(1024, 100);

	u64 txnId1 = 0;
	u64 txnId2 = 1;
	u64 txnId3 = 2;
	u64 txnId4 = 3;

	ILockThread ilt1(&ilm);
	ILockThread ilt2(&ilm);
	ILockThread ilt3(&ilm);
	ILockThread ilt4(&ilm);

	ilt1.start();
	ilt2.start();
	ilt3.start();
	ilt4.start();

	// ��������ļ�����
	CPPUNIT_ASSERT(controlLockThread(&ilt1, txnId1, 10, 0, true));	// ����1��10����

	CPPUNIT_ASSERT(controlLockThread(&ilt2, txnId2, 20, 0, true));	// ����2��20����

	CPPUNIT_ASSERT(controlLockThread(&ilt3, txnId3, 30, 0, true));	// ����3��30����

	CPPUNIT_ASSERT(controlLockThread(&ilt1, txnId1, 20, 3, false));	// ����1��20���Լ���ʧ��

	CPPUNIT_ASSERT(controlLockThread(&ilt1, txnId1, 20, 0, false));	// ����1��20����

	CPPUNIT_ASSERT(controlLockThread(&ilt2, txnId2, 30, 3, false));	// ����2��30���Լ���ʧ��

	CPPUNIT_ASSERT(controlLockThread(&ilt2, txnId2, 30, 0, false));	// ����2��30����

	ilt3.setLock(txnId3, 10, 0);									// ����3��10����
	Thread::msleep(500);											// �ȴ�һ��ʱ��

	if (ilt1.getFinish() && !ilt1.getResult()) {					// ����������1��ɱ��
		cout << endl << "Txn 1 was killed" << endl;
		CPPUNIT_ASSERT(controlLockThread(&ilt1, txnId1, 0, 2, true));	// ����1����
		CPPUNIT_ASSERT(ilt3.getFinish() && ilt3.getResult());		// ��������3��ʱ�����ɹ�
		CPPUNIT_ASSERT(controlLockThread(&ilt3, txnId3, 0, 2, true));	// ����3����
		CPPUNIT_ASSERT(ilt2.getFinish() && ilt2.getResult());		// ��������2��ʱ�����ɹ�
		CPPUNIT_ASSERT(controlLockThread(&ilt2, txnId2, 0, 2, true));	// ����2����
	} else if (ilt2.getFinish() && !ilt2.getResult()) {				// ����������2��ɱ��
		cout << endl << "Txn 2 was killed" << endl;
		CPPUNIT_ASSERT(controlLockThread(&ilt2, txnId2, 0, 2, true));	// ����2����
		CPPUNIT_ASSERT(ilt1.getFinish() && ilt1.getResult());		// ��������1��ʱ�����ɹ�
		CPPUNIT_ASSERT(controlLockThread(&ilt1, txnId1, 0, 2, true));	// ����1����
		CPPUNIT_ASSERT(ilt3.getFinish() && ilt3.getResult());		// ��������3��ʱ�����ɹ�
		CPPUNIT_ASSERT(controlLockThread(&ilt3, txnId3, 0, 2, true));	// ����3����
	} else if (ilt3.getFinish() && !ilt3.getResult()) {				// ����3��ɱ��
		cout << endl << "Txn 3 was killed" << endl;
		CPPUNIT_ASSERT(controlLockThread(&ilt3, txnId3, 0, 2, true));	// ����3����
		CPPUNIT_ASSERT(ilt2.getFinish() && ilt2.getResult());		// ��������2��ʱ�����ɹ�
		CPPUNIT_ASSERT(controlLockThread(&ilt2, txnId2, 0, 2, true));	// ����2����
		CPPUNIT_ASSERT(ilt1.getFinish() && ilt1.getResult());	// ��������1��ʱ�����ɹ�
		CPPUNIT_ASSERT(controlLockThread(&ilt1, txnId1, 0, 2, true));	// ����1����
	} else {
		cout << "impossible" << endl;
		CPPUNIT_ASSERT(false);
	}

	CPPUNIT_ASSERT(!ilm.isHoldingLocks(txnId1));
	CPPUNIT_ASSERT(!ilm.isHoldingLocks(txnId2));
	CPPUNIT_ASSERT(!ilm.isHoldingLocks(txnId3));

	// ���Ի��ѵ�������
	CPPUNIT_ASSERT(controlLockThread(&ilt2, txnId2, 5, 0, true));	// ����2����5����
	CPPUNIT_ASSERT(controlLockThread(&ilt2, txnId2, 10, 0, true));	// ����2����10����
	CPPUNIT_ASSERT(controlLockThread(&ilt3, txnId3, 40, 0, true));	// ����3����40����
	CPPUNIT_ASSERT(controlLockThread(&ilt1, txnId1, 20, 0, true));	// ����1����20����
	CPPUNIT_ASSERT(controlLockThread(&ilt2, txnId2, 20, 0, false));	// ����2�ȴ�20����
	CPPUNIT_ASSERT(controlLockThread(&ilt3, txnId3, 20, 0, false));	// ����3�ȴ�20����

	ilt1.enableSyncPoint(SP_ILT_AFTER_WAKEUP);
	ilt1.setLock(txnId1, 20, 1);									// ����1�ͷ�20����
	// ���ʱ������̻߳��Ȼ�������2��Ȼ��ȴ����޸�����3�ĵȴ�������
	ilt1.joinSyncPoint(SP_ILT_AFTER_WAKEUP);

	// ָ������2������40�������ȴ�
	Thread::msleep(2000);
	CPPUNIT_ASSERT(ilt2.getFinish() && ilt2.getResult());			// ��������2�ļ�����ɹ�
	CPPUNIT_ASSERT(controlLockThread(&ilt2, txnId2, 40, 0, false));	// ����2�ȴ�40����

	// ��������1�ķ����̲߳��������ʱ����γ�����
	ilt1.disableSyncPoint(SP_ILT_AFTER_WAKEUP);
	ilt1.notifySyncPoint(SP_ILT_AFTER_WAKEUP);

	// �ȴ�һ��ʱ��֮�󣬶�������2����3��һ��������������߳�ɱ��
	Thread::msleep(1000);

	if (ilt2.getFinish() && !ilt2.getResult()) {					// ˵������2��ɱ��
		CPPUNIT_ASSERT(controlLockThread(&ilt2, txnId2, 0, 2, true));	// ��������2�����ɹ�
		CPPUNIT_ASSERT(ilt3.getFinish() && ilt3.getResult());		// ����3�����ɹ�
		CPPUNIT_ASSERT(controlLockThread(&ilt3, txnId3, 0, 2, true));	// ��������3�����ɹ�
	} else if (ilt3.getFinish() && !ilt3.getResult()) {				// ˵������3��ɱ��
		CPPUNIT_ASSERT(controlLockThread(&ilt3, txnId3, 0, 2, true));	// ��������3�����ɹ�
		CPPUNIT_ASSERT(ilt2.getFinish() && ilt2.getResult());		// ����2�����ɹ�
		CPPUNIT_ASSERT(controlLockThread(&ilt2, txnId2, 0, 2, true));	// ��������2�����ɹ�
	} else {
		CPPUNIT_ASSERT(false);
	}

	ilt1.setExit();
	ilt2.setExit();
	ilt3.setExit();
	ilt4.setExit();
	ilt1.join();
	ilt2.join();
	ilt3.join();
	ilt4.join();
}

class IMTLocker : public Thread {
public:
	IMTLocker(IndicesLockManager *ilm, u64 txnId) : Thread("IMTLocker") {
		m_ilm = ilm;
		m_running = true;
		m_txnId = txnId;
		m_token = 0;
	}

	void run() {
		while (true) {
			if (!m_running) {
				m_ilm->unlockAll(m_txnId);
				CPPUNIT_ASSERT(m_ilm->isHoldingLocks(m_txnId) == 0);
				//cout << "Txn " << m_txnId << " unlocks all succeed" << endl;
				//cout << "Txn " << m_txnId << " finished." << endl;
				break;
			}

			u8 command = rand() % 30;

			if (m_locked.size() < MAX_HOLDING_LOCKS && command < 10) {	// ������м�������
				u64 key = rand() % MAX_KEY_VALUES;
				if (m_ilm->lock(m_txnId, key)) {	// �����ɹ�
					m_locked.push_back(key);
					//cout << "Txn " << m_txnId << " locks " << key << " succeed" << endl;
				} else {	// �������ɹ������������������в���
					CPPUNIT_ASSERT(!m_locked.empty() && m_ilm->isHoldingLocks(m_txnId) != 0);
					m_ilm->unlockAll(m_txnId);
					m_locked.clear();
					CPPUNIT_ASSERT(m_ilm->isHoldingLocks(m_txnId) == 0);
					//cout << "Txn " << m_txnId << " locks " << key << " failed and rollbacked" << endl;
				}
			} else if (m_locked.size() < MAX_HOLDING_LOCKS && command < 15 && command >= 10) {	// �������trylock����
				u64 key = rand() % MAX_KEY_VALUES;
				if (m_ilm->tryLock(m_txnId, key)) {
					m_locked.push_back(key);
					//cout << "Txn " << m_txnId << " trylocks " << key << " succeed" << endl;
				}
			} else if (m_locked.size() >= 1 && command >= 15 && command < 25) {	// �������һ�ν�������
				size_t size = (rand() % m_locked.size());
				list<u64>::iterator iter;
				for (iter = m_locked.begin(); size > 0; iter++, size--);
				u64 key = (u64)*iter;
				CPPUNIT_ASSERT(m_ilm->unlock(m_txnId, key));
				m_locked.erase(iter);
				//cout << "Txn " << m_txnId << " unlocks " << key << " succeed" << endl;
			} else if (m_locked.size() >= 1 && command >= 25) {	// �����ͷ�����������е���
				CPPUNIT_ASSERT(m_ilm->unlockAll(m_txnId));
				m_locked.clear();
				CPPUNIT_ASSERT(m_ilm->isHoldingLocks(m_txnId) == 0);
				//cout << "Txn " << m_txnId << " unlocks all succeed" << endl;
			}

			m_token++;
		}
	}

	void setStop() {
		m_running = false;
	}

	bool checkStatus() {
		bool everAlive = (m_token != 0);
		m_token = 0;
		return everAlive;
	}

private:
	IndicesLockManager	*m_ilm;				/** �������� */
	list<u64>			m_locked;			/** ������ǰ���˶��������� */
	bool				m_running;			/** �������ñ�ǵ�ǰ�߳��Ƿ���ֹ */
	u64					m_txnId;			/** ����ʹ�õ������ */
	u64					m_token;			/** ��־��ÿ�β����ɹ�һ�Σ���ֵ��1������̻߳�ȷ�ϸ�ֵ��Ϊ0��Ȼ��������0����һ��ʱ���ټ�� */

	static const uint	MAX_HOLDING_LOCKS = 10;	/** ������ͬʱ���е����� */
	static const uint	MAX_KEY_VALUES = 10000;/** ������ֵ������� */
};


class IMTChecker : public Thread {
public:
	IMTChecker(uint threads, IMTLocker **lockers) : Thread("IMTChecker") {
		m_checkThreads = threads;
		m_checkLockers = lockers;
		m_running = true;
	}

	void run() {
		while (m_running) {
			Thread::msleep(CHECK_DURATION);
			bool error = false;
			for (uint i = 0; i < m_checkThreads; i++) {
				if (!m_checkLockers[i]->checkStatus()) {
					cout << "[Warnings]: Thread " << i << " is not alive" << endl;
					error = true;
				}
			}

			if (!error) {
				cout << "All lock threads are healthy." << endl;
			}
		}
	}

	void setStop() {
		m_running = false;
	}

private:
	uint m_checkThreads;		/** Ҫ�����߳��� */
	IMTLocker **m_checkLockers;	/** Ҫ���ĸ�����������߳� */
	bool m_running;				/** �������ñ�ǵ�ǰ�߳��Ƿ���ֹ */

	static const uint	CHECK_DURATION = 60 * 1000;	/** ������ڣ���λ���� */
};


/************************************************************************/
/* ��������ʹ�õ��������ȷ�Ժ�����                                     */
/************************************************************************/

const char* IndicesLockManagerBigTestCase::getName() {
	return "IndicesLockManager multi-thread test";
}

const char* IndicesLockManagerBigTestCase::getDescription() {
	return "Multi test Indices Lock Manager.";
}

bool IndicesLockManagerBigTestCase::isBig() {
	return true;
}


/**
* ִ�ж��̲߳��Ե�������ƺ���
*/
const uint THREADS = 500;
void IndicesLockManagerBigTestCase::testMT() {
	srand((unsigned)time(NULL));

	IndicesLockManager ilm(1024, 8000, 100, 5);

	IMTLocker **imtlockers = new IMTLocker*[THREADS];
	for (uint i = 0; i < THREADS; i++) {
		imtlockers[i] = new IMTLocker(&ilm, i);
		imtlockers[i]->start();
	}

	IMTChecker *imtchecker = new IMTChecker(THREADS, imtlockers);
	imtchecker->start();

	Thread::msleep(5 * 60 * 60 * 1000);

	imtchecker->setStop();
	imtchecker->join();

	for (uint i = 0; i < THREADS; i++) {
		imtlockers[i]->setStop();
		imtlockers[i]->join();
		delete imtlockers[i];
	}

	delete [] imtlockers;
}


static u64			g_idxlockCnt = 0;
static u64			g_totalIdxLockTime = 0;
static Atomic<int>	g_maxIdxLockTime;
//static u64			g_slowLockThreshold = 10;	// in milliseconds
//static u64			g_slowLockCnt = 0;

/** �߳�ͻ�����ʲ����߳� */
class IDXHCTTestThread: public Thread {
public:
	IDXHCTTestThread(IndicesLockManager *ilm, u64 key, time_t stopTime): Thread("IDXHCTTestThread") {
		m_ilm = ilm;
		m_key = key;
		m_stopTime = stopTime;
	}

	/** 
	 * ����
	 * @see <code>Thread::run()</code>
	 */
	void run() {
		System::srandom((int)System::microTime());
		while (time(NULL) < m_stopTime) {
			u64 before = System::microTime();
			m_ilm->lock(m_id, m_key);
			u64 after = System::microTime();
			int lockTime = (int)(after - before);
			if (lockTime > g_slowLockThreshold * 1000)
				g_slowLockCnt++;
			while (lockTime > g_maxIdxLockTime.get()) {
				int oldLockTime = g_maxIdxLockTime.get();
				if (oldLockTime >= lockTime)
					break;
				if (g_maxIdxLockTime.compareAndSwap(oldLockTime, lockTime))
					break;
			}
			
			g_totalIdxLockTime += lockTime;
			g_idxlockCnt++;
			int loops = 1000 + System::random() % 1000;
			for (int n = 0; n < loops; n++)
				m_dummy = (m_dummy + 7 + m_id) % 97;
			
			m_ilm->unlock(m_id, m_key);
		}
	}

private:
	IndicesLockManager	*m_ilm;		/** �������� */
	u64			m_key;				/** Ҫ�����ļ�ֵ */
	int			m_dummy;
	time_t m_stopTime;				/* ֹͣʱ�� */
};

/** 100���߳̾���ͬһ��д���������ڸ߳�ͻʱ���������������
 */
void IndicesLockManagerBigTestCase::testHighConflictThroughput() {
	u64 before = 0L;
	u64 after = 0L;	

	IDXHCTTestThread	*threads[100];
	IndicesLockManager ilm(1024, 100);
	Mutex	lock(__FUNCTION__, __FILE__, __LINE__);
	u64 key = 16;
	size_t threadCount = sizeof(threads) / sizeof(threads[0]);
	int testDuration = 120;

	cout << "Test performance of indices lock manager for " << testDuration << " seconds" << endl;

	g_idxlockCnt = 0;
	g_slowLockCnt = 0;
	g_maxIdxLockTime.set(0);
	g_totalIdxLockTime = 0;

	for (size_t i = 0; i < threadCount; i++)
		threads[i] = new IDXHCTTestThread(&ilm, key, time(NULL) + testDuration);

	before = System::currentTimeMillis();
	for (size_t i = 0; i < threadCount; i++)
		threads[i]->start();

	for (size_t i = 0; i < threadCount; i++)
		threads[i]->join();
	after = System::currentTimeMillis();

	for (size_t i = 0; i < threadCount; i++) 
		delete threads[i];

	cout << "Time: " << (after - before) << " ms" << endl;
	cout << "Throughput: " << g_idxlockCnt / (after - before) << " ops/ms" << endl;
	cout << "Max lock time: " << g_maxIdxLockTime.get() << " us" << endl;
	if (g_idxlockCnt > 0) {
		cout << "Avg idx lock time: " << g_totalIdxLockTime / g_idxlockCnt << " us" << endl;
	}
	cout << "Slow locks: " << g_slowLockCnt << endl;
}