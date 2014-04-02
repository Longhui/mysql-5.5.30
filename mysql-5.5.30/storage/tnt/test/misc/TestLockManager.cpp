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
 * 测试锁表操作
 *	case 1. tryLock锁冲突矩阵
 *	case 2. 不同关键字的锁不冲突
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


	// 不同关健词上加锁，不应该冲突
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
 * 测试特殊情况下的锁表操作
 *	case 1. 用完锁对象时，尝试在一个新的关键字上加锁，抛出NTSE_EC_TOO_MANY_ROWLOCK异常
 *	case 2. slot空闲链表是个单向链表，测试该链表实现的正确性
 */
void LockManagerTestCase::testSpecialLM() {
	{
		LockManager lm(1, 1, 0); // 这个锁表只能放两个锁对象
		CPPUNIT_ASSERT(lm.lock(NULL, 100, Shared)); // 用掉embeded
		CPPUNIT_ASSERT(lm.lock(NULL, 101, Shared)); // 用掉全局空闲链表
		CPPUNIT_ASSERT(lm.lock(NULL, 101, Shared)); // 相同key，不占用锁对象
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
		LockManager lm(1); // 这个锁表只能放三个锁对象
		CPPUNIT_ASSERT(lm.lock(NULL, 100, Shared)); // 用掉embeded
		CPPUNIT_ASSERT(lm.lock(NULL, 101, Shared)); // 用掉slot空闲链表
		CPPUNIT_ASSERT(lm.lock(NULL, 102, Shared)); // 用掉全局空闲链表
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
	{ // slot空闲链表比较长
		uint reservedLockPerSlot = 20;
		LockManager lm(1, 1, reservedLockPerSlot);
		// 刚好用完所有锁对象
		for (uint i = 0; i < reservedLockPerSlot + 2; ++i)
			CPPUNIT_ASSERT(lm.lock(NULL, i, Shared));
		for (uint i = 0; i < reservedLockPerSlot + 2; ++i)
			lm.unlock(i, Shared);
	}
}
/**
 * 测试IsLocked函数功能
 * 测试流程:
 *	case 1. 未上锁
 *	case 2. 上S锁，isLock锁模式W
 *	case 3. 上S锁，isLock锁模式R
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

/** 加锁线程 */
class LockThread : public Thread {
public:
	LockThread(LockManager *lm, u64 key, LockMode mode, int timeOut = -1)
		: Thread("LockThread"), m_lm(lm), m_key(key), m_mode(mode)
		, m_timeOut(timeOut), m_lockFinish(false)
	{}
	/** 加锁成功之后，设置变量m_lockFinish为true */
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
 * 测试锁冲突处理
 * 分别测试“写-读”，“读-写”，“写-写”这三种冲突，并验证“读-读”不冲突
 * 测试流程：
 *	1. 主线程加X锁
 *	2. 启动加锁线程
 *	3. 加锁线程加R锁
 *	4. 主线程小睡一会儿
 *	5. 验证加锁线程加锁不成功
 *	6. 主线程放锁
 *	7. 主线程小睡一会儿
 *	8. 验证加锁线程加锁成功
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
 * 测试单线程加锁效率（只对单一关键字加锁）
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
 * 计算哈希桶% -> &
 * rlock/unlock 104
 * wlock/unlock 110
 *
 * 减少一次lockObject->init()调用
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
 * 测试单线程加锁效率（对不同关键字加锁)
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
 * 测试多线程加解锁正确性
 * 测试流程
 *	1. 启动100个操作线程
 *	2. 每个线程5000次读取共享变量sharedValue(加读锁），5000次++sharedValue（加写锁）
 *	3. 结束操作线程
 *	4. 验证sharedValue == 5000 * 100
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

/** 高冲突吞吐率测试线程 */
class HCTTestThread: public Thread {
public:
	/**
	 * 构造函数
	 * @param lockArray		锁数组
	 * @param numLock		锁个数
	 * @param writePercent	写锁的百分比例
	 * @param timedPercent	超时锁的百分比例
	 * @param timeoutMs		超时锁定超时时间毫秒数
	 * @param stopTime		线程运行的终止绝对时间
	 */
	HCTTestThread(bool lmOrRwlock, LockManager *lm, u64 key, Mutex *lock, time_t stopTime): Thread("HCTTestThread") {
		m_lmOrRwlock = lmOrRwlock;
		m_lm = lm;
		m_key = key;
		m_lock = lock;
		m_stopTime = stopTime;
	}

	/** 
	 * 运行
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
	LockManager	*m_lm;		/** 锁管理器 */
	u64			m_key;		/** 要锁定的键值 */
	bool		m_lmOrRwlock;
	Mutex		*m_lock;	
	int			m_dummy;
	time_t m_stopTime;		/* 停止时间 */
};

/** 100个线程竞争同一个写锁，测试在高冲突时的吞吐率与均衡性
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
/* 测试索引使用的锁表的正确性和性能                                     */
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
* 测试顺序加锁的正确性和性能，锁表功能的基本测试
* @case 1: 两个事务连续加放锁，包括tryLock，断言所有操作都成功，然后再执行一遍相同顺序仍旧成功，保证锁表信息更新的正确性
* @case 2: 两个事务连续加多次锁，制造某个锁表入口备用锁使用殆尽的情况，测试全局备用锁的使用正确
*/
void IndicesLockManagerTestCase::testSequenceLock() {
	IndicesLockManager ilm(1024, 100, 20, 5);
	u64 txnId1 = 0, txnId2 = 1;

	// 两个事务连续加锁放锁
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
	// 再执行一遍相同的操作，验证操作仍旧正确
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
	// 加更多的锁，导致某个Entry都撑爆
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

	// 测试锁表很小得情况
	IndicesLockManager ilm1(1024, 1);
	// 这个时候的锁表应该只能承受三个锁，第四个锁申请会失败
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
* 测试锁表的性能
* @case 1: 测试连续加放100万次锁的平均性能
*/
void IndicesLockManagerBigTestCase::testSequenceLockPerformance() {
	// 测试连续加放100w个不同锁对象的性能
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
* 模拟多次加锁后一次放锁的性能测试
* @case 1: 测试模拟索引加锁操作，先加若干个锁在一起释放的100万次操作的平均性能
*/
void IndicesLockManagerBigTestCase::testRangeLockPerformance() {
	IndicesLockManager bigilm(1024, 100);
	u64 before, after;
	u64 repeat = 1000000;
	u64 txnId1 = 0;

	// 测试加了锁最后一次性放锁的场景，假设每个锁线程都加了10把锁，然后一次性放掉
	// 锁表规模按照10w设置，每个入口预留20把锁，20个入口
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
				if (command == 0) {	// 加锁
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
	* 得到前一条指令是否执行结束状态
	*/
	bool getFinish() {
		MutexGuard guard(&m_mutex, __FILE__, __LINE__);
		return !m_busy;
	}

	/**
	* 得到嵌一个指令操作的成功与否
	*/
	bool getResult() {
		MutexGuard guard(&m_mutex, __FILE__, __LINE__);
		if (m_busy)
			return false;
		return m_success;
	}

	/**
	* 指定对锁表进行某个操作
	* @param txnId		事务号
	* @param key		加锁对象键值
	* @param command	锁表操作指令编号
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
	* 设置当前线程退出
	*/
	void setExit() {
		LOCK(&m_mutex);
		m_command = EXIT_MARK;
		UNLOCK(&m_mutex);
	}

private:
	IndicesLockManager	*m_ilm;			/** 加锁线程使用的锁表对象 */

	Mutex				m_mutex;		/** 用来互斥设置命令和读取命令操作的互斥量 */
	u8					m_command;		/** 0表示加锁，1表示放锁，2表示放所有的锁，3表示线程结束 */
	u64					m_txnId;		/** 某次操作使用的事务号 */
	u64					m_key;			/** 加锁对象键值 */
	bool				m_success;		/** 加锁是否成功，不成功说明是死锁 */
	bool				m_busy;			/** 标识当前线程状态，true表示正在执行操作，false表示状态空闲 */

	static const u8 EXIT_MARK = (u8)-2;	/** 线程退出命令标志 **/
};

/**
* 指定通过某个线程接口执行某个加放锁操作，并且验证结果的正确性
* @param ilt		锁表操作的线程
* @param txnId		操作事务号
* @Param key		操作键值
* @param command	操作的具体命令
* @param success	
*/
bool controlLockThread(ILockThread *ilt, u64 txnId, u64 key, u8 command, bool success) {
	ilt->setLock(txnId, key, command);
	Thread::msleep(1000);
	if (success) {
		return (ilt->getFinish() && ilt->getResult());
	} else {
		// trylock要特殊处理
		if (command == 3)
			return (ilt->getFinish() && !ilt->getResult());
		return (!ilt->getFinish());
	}
}

/**
* 测试加锁的冲突性，某个对象一旦被一个线程加锁成功，其他线程必须等待
* @case 1:测试不同线程加不同的锁是不会冲突的
* @case 2:测试不同线程加同一个锁，是会冲突的，同时测试锁重入的正确性
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

	// 测试不同线程加不同的锁，应该都能够成功
	CPPUNIT_ASSERT(controlLockThread(&ilt1, txnId1, 10, 0, true));
	CPPUNIT_ASSERT(controlLockThread(&ilt1, txnId1, 11, 0, true));
	CPPUNIT_ASSERT(controlLockThread(&ilt1, txnId1, 20, 0, true));
	CPPUNIT_ASSERT(controlLockThread(&ilt2, txnId2, 31, 0, true));
	CPPUNIT_ASSERT(controlLockThread(&ilt3, txnId3, 40, 0, true));
	CPPUNIT_ASSERT(controlLockThread(&ilt3, txnId3, 41, 0, true));

	CPPUNIT_ASSERT(controlLockThread(&ilt1, txnId1, 0, 2, true));
	CPPUNIT_ASSERT(controlLockThread(&ilt2, txnId2, 0, 2, true));
	CPPUNIT_ASSERT(controlLockThread(&ilt3, txnId3, 0, 2, true));

	// 测试加锁对象一旦被某个线程加锁加上，其他线程必须加不上锁，同时测试锁重入的正确性

	CPPUNIT_ASSERT(controlLockThread(&ilt1, txnId1, 10, 3, true));	// 事务1对10尝试加锁成功

	CPPUNIT_ASSERT(controlLockThread(&ilt2, txnId2, 10, 0, false));	// 事务2对10加锁等待

	CPPUNIT_ASSERT(controlLockThread(&ilt3, txnId3, 10, 0, false));	// 事务3对10加锁等待

	CPPUNIT_ASSERT(controlLockThread(&ilt1, txnId1, 10, 0, true));	// 事务1加锁重入
	CPPUNIT_ASSERT(controlLockThread(&ilt1, txnId1, 10, 0, true));	// 事务1加锁重入
	CPPUNIT_ASSERT(controlLockThread(&ilt1, txnId1, 10, 3, true));	// 事务1尝试加锁重入

	CPPUNIT_ASSERT(controlLockThread(&ilt1, txnId1, 10, 1, true));	// 事务1放锁1次

	CPPUNIT_ASSERT(!ilt2.getFinish() && !ilt3.getFinish());			// 断言事务2/3此时还需要等待

	CPPUNIT_ASSERT(controlLockThread(&ilt1, txnId1, 0, 2, true));	// 事务1全部放锁

	CPPUNIT_ASSERT(ilt2.getFinish() && ilt2.getResult());			// 断言事务2此时可以加锁成功
	CPPUNIT_ASSERT(!ilt3.getFinish());								// 断言事务3仍旧在等待

	CPPUNIT_ASSERT(controlLockThread(&ilt2, txnId2, 10, 1, true));	// 事务2放锁

	CPPUNIT_ASSERT(ilt3.getFinish() && ilt3.getResult());			// 断言事务3此时可以加锁成功

	CPPUNIT_ASSERT(controlLockThread(&ilt3, txnId3, 10, 1, true));	// 事务3放锁

	CPPUNIT_ASSERT(ilm.lock(txnId1, 10));						// 断言此时没人加上锁，可以成功
	CPPUNIT_ASSERT(ilm.unlock(txnId1, 10));						// 断言放锁成功

	ilt1.setExit();
	ilt2.setExit();
	ilt3.setExit();
	ilt1.join();
	ilt2.join();
	ilt3.join();
}


/**
* 测试死锁检测的正确性
* @case 1: 建立环测试3个事务的死锁检测正确性
* @case 2: 测试死锁可能由于别的事务唤醒之后调整等待关系造成的，这个时候死锁会由检测线程发现
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

	// 三个事务的简单死锁
	CPPUNIT_ASSERT(controlLockThread(&ilt1, txnId1, 10, 0, true));	// 事务1加10的锁

	CPPUNIT_ASSERT(controlLockThread(&ilt2, txnId2, 20, 0, true));	// 事务2加20的锁

	CPPUNIT_ASSERT(controlLockThread(&ilt3, txnId3, 30, 0, true));	// 事务3加30的锁

	CPPUNIT_ASSERT(controlLockThread(&ilt1, txnId1, 20, 3, false));	// 事务1对20尝试加锁失败

	CPPUNIT_ASSERT(controlLockThread(&ilt1, txnId1, 20, 0, false));	// 事务1对20加锁

	CPPUNIT_ASSERT(controlLockThread(&ilt2, txnId2, 30, 3, false));	// 事务2对30尝试加锁失败

	CPPUNIT_ASSERT(controlLockThread(&ilt2, txnId2, 30, 0, false));	// 事务2对30加锁

	ilt3.setLock(txnId3, 10, 0);									// 事务3对10加锁
	Thread::msleep(500);											// 等待一段时间

	if (ilt1.getFinish() && !ilt1.getResult()) {					// 这个情况事务1被杀死
		cout << endl << "Txn 1 was killed" << endl;
		CPPUNIT_ASSERT(controlLockThread(&ilt1, txnId1, 0, 2, true));	// 事务1放锁
		CPPUNIT_ASSERT(ilt3.getFinish() && ilt3.getResult());		// 断言事务3此时加锁成功
		CPPUNIT_ASSERT(controlLockThread(&ilt3, txnId3, 0, 2, true));	// 事务3放锁
		CPPUNIT_ASSERT(ilt2.getFinish() && ilt2.getResult());		// 断言事务2此时加锁成功
		CPPUNIT_ASSERT(controlLockThread(&ilt2, txnId2, 0, 2, true));	// 事务2放锁
	} else if (ilt2.getFinish() && !ilt2.getResult()) {				// 这个情况事务2被杀死
		cout << endl << "Txn 2 was killed" << endl;
		CPPUNIT_ASSERT(controlLockThread(&ilt2, txnId2, 0, 2, true));	// 事务2放锁
		CPPUNIT_ASSERT(ilt1.getFinish() && ilt1.getResult());		// 断言事务1此时加锁成功
		CPPUNIT_ASSERT(controlLockThread(&ilt1, txnId1, 0, 2, true));	// 事务1放锁
		CPPUNIT_ASSERT(ilt3.getFinish() && ilt3.getResult());		// 断言事务3此时加锁成功
		CPPUNIT_ASSERT(controlLockThread(&ilt3, txnId3, 0, 2, true));	// 事务3放锁
	} else if (ilt3.getFinish() && !ilt3.getResult()) {				// 事务3被杀死
		cout << endl << "Txn 3 was killed" << endl;
		CPPUNIT_ASSERT(controlLockThread(&ilt3, txnId3, 0, 2, true));	// 事务3放锁
		CPPUNIT_ASSERT(ilt2.getFinish() && ilt2.getResult());		// 断言事务2此时加锁成功
		CPPUNIT_ASSERT(controlLockThread(&ilt2, txnId2, 0, 2, true));	// 事务2放锁
		CPPUNIT_ASSERT(ilt1.getFinish() && ilt1.getResult());	// 断言事务1此时加锁成功
		CPPUNIT_ASSERT(controlLockThread(&ilt1, txnId1, 0, 2, true));	// 事务1放锁
	} else {
		cout << "impossible" << endl;
		CPPUNIT_ASSERT(false);
	}

	CPPUNIT_ASSERT(!ilm.isHoldingLocks(txnId1));
	CPPUNIT_ASSERT(!ilm.isHoldingLocks(txnId2));
	CPPUNIT_ASSERT(!ilm.isHoldingLocks(txnId3));

	// 测试唤醒导致死锁
	CPPUNIT_ASSERT(controlLockThread(&ilt2, txnId2, 5, 0, true));	// 事务2持有5的锁
	CPPUNIT_ASSERT(controlLockThread(&ilt2, txnId2, 10, 0, true));	// 事务2持有10的锁
	CPPUNIT_ASSERT(controlLockThread(&ilt3, txnId3, 40, 0, true));	// 事务3持有40的锁
	CPPUNIT_ASSERT(controlLockThread(&ilt1, txnId1, 20, 0, true));	// 事务1持有20的锁
	CPPUNIT_ASSERT(controlLockThread(&ilt2, txnId2, 20, 0, false));	// 事务2等待20的锁
	CPPUNIT_ASSERT(controlLockThread(&ilt3, txnId3, 20, 0, false));	// 事务3等待20的锁

	ilt1.enableSyncPoint(SP_ILT_AFTER_WAKEUP);
	ilt1.setLock(txnId1, 20, 1);									// 事务1释放20的锁
	// 这个时候放锁线程会先唤醒事务2，然后等待在修改事务3的等待链表上
	ilt1.joinSyncPoint(SP_ILT_AFTER_WAKEUP);

	// 指定事务2继续加40的锁，等待
	Thread::msleep(2000);
	CPPUNIT_ASSERT(ilt2.getFinish() && ilt2.getResult());			// 断言事务2的加锁会成功
	CPPUNIT_ASSERT(controlLockThread(&ilt2, txnId2, 40, 0, false));	// 事务2等待40的锁

	// 继续事务1的放锁线程操作，这个时候会形成死锁
	ilt1.disableSyncPoint(SP_ILT_AFTER_WAKEUP);
	ilt1.notifySyncPoint(SP_ILT_AFTER_WAKEUP);

	// 等待一段时间之后，断言事务2或者3有一个事务被死锁检测线程杀死
	Thread::msleep(1000);

	if (ilt2.getFinish() && !ilt2.getResult()) {					// 说明事务2被杀死
		CPPUNIT_ASSERT(controlLockThread(&ilt2, txnId2, 0, 2, true));	// 断言事务2放锁成功
		CPPUNIT_ASSERT(ilt3.getFinish() && ilt3.getResult());		// 事务3加锁成功
		CPPUNIT_ASSERT(controlLockThread(&ilt3, txnId3, 0, 2, true));	// 断言事务3放锁成功
	} else if (ilt3.getFinish() && !ilt3.getResult()) {				// 说明事务3被杀死
		CPPUNIT_ASSERT(controlLockThread(&ilt3, txnId3, 0, 2, true));	// 断言事务3放锁成功
		CPPUNIT_ASSERT(ilt2.getFinish() && ilt2.getResult());		// 事务2加锁成功
		CPPUNIT_ASSERT(controlLockThread(&ilt2, txnId2, 0, 2, true));	// 断言事务2放锁成功
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

			if (m_locked.size() < MAX_HOLDING_LOCKS && command < 10) {	// 允许进行加锁操作
				u64 key = rand() % MAX_KEY_VALUES;
				if (m_ilm->lock(m_txnId, key)) {	// 加锁成功
					m_locked.push_back(key);
					//cout << "Txn " << m_txnId << " locks " << key << " succeed" << endl;
				} else {	// 加锁不成功，有死锁，回退所有操作
					CPPUNIT_ASSERT(!m_locked.empty() && m_ilm->isHoldingLocks(m_txnId) != 0);
					m_ilm->unlockAll(m_txnId);
					m_locked.clear();
					CPPUNIT_ASSERT(m_ilm->isHoldingLocks(m_txnId) == 0);
					//cout << "Txn " << m_txnId << " locks " << key << " failed and rollbacked" << endl;
				}
			} else if (m_locked.size() < MAX_HOLDING_LOCKS && command < 15 && command >= 10) {	// 允许进行trylock操作
				u64 key = rand() % MAX_KEY_VALUES;
				if (m_ilm->tryLock(m_txnId, key)) {
					m_locked.push_back(key);
					//cout << "Txn " << m_txnId << " trylocks " << key << " succeed" << endl;
				}
			} else if (m_locked.size() >= 1 && command >= 15 && command < 25) {	// 允许进行一次解锁操作
				size_t size = (rand() % m_locked.size());
				list<u64>::iterator iter;
				for (iter = m_locked.begin(); size > 0; iter++, size--);
				u64 key = (u64)*iter;
				CPPUNIT_ASSERT(m_ilm->unlock(m_txnId, key));
				m_locked.erase(iter);
				//cout << "Txn " << m_txnId << " unlocks " << key << " succeed" << endl;
			} else if (m_locked.size() >= 1 && command >= 25) {	// 允许释放整个事务持有的锁
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
	IndicesLockManager	*m_ilm;				/** 加锁锁表 */
	list<u64>			m_locked;			/** 保留当前加了多少锁对象 */
	bool				m_running;			/** 用来设置标记当前线程是否终止 */
	u64					m_txnId;			/** 加锁使用的事务号 */
	u64					m_token;			/** 标志，每次操作成功一次，该值加1，检测线程会确认该值不为0，然后重新置0，隔一段时间再检测 */

	static const uint	MAX_HOLDING_LOCKS = 10;	/** 最多可以同时持有的锁数 */
	static const uint	MAX_KEY_VALUES = 10000;/** 加锁键值的最大数 */
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
	uint m_checkThreads;		/** 要检查的线程数 */
	IMTLocker **m_checkLockers;	/** 要检查的各个锁表操作线程 */
	bool m_running;				/** 用来设置标记当前线程是否终止 */

	static const uint	CHECK_DURATION = 60 * 1000;	/** 检查周期，单位毫秒 */
};


/************************************************************************/
/* 测试索引使用的锁表的正确性和性能                                     */
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
* 执行多线程测试的主体控制函数
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

/** 高冲突吞吐率测试线程 */
class IDXHCTTestThread: public Thread {
public:
	IDXHCTTestThread(IndicesLockManager *ilm, u64 key, time_t stopTime): Thread("IDXHCTTestThread") {
		m_ilm = ilm;
		m_key = key;
		m_stopTime = stopTime;
	}

	/** 
	 * 运行
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
	IndicesLockManager	*m_ilm;		/** 锁管理器 */
	u64			m_key;				/** 要锁定的键值 */
	int			m_dummy;
	time_t m_stopTime;				/* 停止时间 */
};

/** 100个线程竞争同一个写锁，测试在高冲突时的吞吐率与均衡性
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