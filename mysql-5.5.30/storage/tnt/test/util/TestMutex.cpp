/**
* 测试Mutex同步机制.
*
* @author 聂明军(niemingjun@corp.netease.com, niemingjun@163.org)
* @version 0.3
*/

#include "util/TestMutex.h"
#include "util/Sync.h"
#include "util/Thread.h"
#include "util/System.h"
#include "misc/EventMonitor.h"
#include "util/InnoMutex.h"
#include "util/TestSync.h"

#include <cppunit/config/SourcePrefix.h>
#include <iostream>
using namespace std;
using namespace ntse;



//Mutex测试实现。

const char* MutexTestCase::getName() {
	return "Mutex Synchronization facilities test";
}

const char* MutexTestCase::getDescription() {
	return "Test Mutex";
}


/** 
 * 测试类型
 * 
 * @return false, 小型单元测试。
 */
bool MutexTestCase::isBig() {
	return false; 
}

/**
 * Mutex 基本功能测试。
 */
void MutexTestCase::testMutex() {
	Mutex lock("testMutex", __FILE__, __LINE__);

	const int LOOP_COUNT = 3;
	for (int i = 0; i < LOOP_COUNT; ++i) {
		//测试lock
		LOCK(&lock);
		CPPUNIT_ASSERT(lock.isLocked());
		CPPUNIT_ASSERT(!TRYLOCK(&lock));

		//测试unlock
		UNLOCK(&lock);
		CPPUNIT_ASSERT(!lock.isLocked());		
		}//end of for.

	//测试单线程 非locked下 tryLock
	CPPUNIT_ASSERT(TRYLOCK(&lock));

	//测试lock usage的 lock count 计数。
	CPPUNIT_ASSERT(lock.getUsage()->m_lockCnt == (LOOP_COUNT * 2 + 1));

	//测试lock usage的spin count计数。
	CPPUNIT_ASSERT(0 == lock.getUsage()->m_spinCnt);

	//测试lock usage的wait count计数。
	CPPUNIT_ASSERT(0 == lock.getUsage()->m_waitCnt);

	//测试单线程 locked下 timeLock
	CPPUNIT_ASSERT(!lock.timedLock(100, __FILE__, __LINE__));

	//测试lock usage的spin count计数。
	CPPUNIT_ASSERT(1 == lock.getUsage()->m_spinCnt);
	//测试lock usage的wait count计数。
	CPPUNIT_ASSERT(1 == lock.getUsage()->m_waitCnt);
}

/**
 *Test <code>Mutex(const char *name, const char *file, uint line)</code> constructor.
 */
void MutexTestCase::testNewMutexConstructor() {
	Mutex *pMutex =  new Mutex("NAME", __FILE__, __LINE__);
	CPPUNIT_ASSERT(NULL != pMutex);
	CPPUNIT_ASSERT(!pMutex->isLocked());
	CPPUNIT_ASSERT(NULL != pMutex->getUsage());	
	//lock usage的spin count计数。
	CPPUNIT_ASSERT(0 == (pMutex->getUsage()->m_spinCnt));
	//lock usage的wait count计数。
	CPPUNIT_ASSERT(0 == (pMutex->getUsage()->m_waitCnt));
	delete pMutex;
	pMutex = NULL;
}

/**
 *Accuracy test of<code>tryLock(const char *file, uint line)</code> method.
 */
void MutexTestCase::testTryLock() {
	Mutex mutex("NewMutexTestCase::testTryLock", __FILE__, __LINE__);
	bool succeeded = false;
	
	//try lock.
	succeeded = mutex.tryLock(__FILE__, __LINE__);
	CPPUNIT_ASSERT(succeeded);
	CPPUNIT_ASSERT(mutex.isLocked());
	
	//try lock again, lock fail expected.
	succeeded = mutex.tryLock(__FILE__, __LINE__);
	CPPUNIT_ASSERT(!succeeded);
	CPPUNIT_ASSERT(mutex.isLocked());
	
	//unlock.
	mutex.unlock();
	CPPUNIT_ASSERT(!mutex.isLocked());

	//try lock again after unlock.
	succeeded = mutex.tryLock(__FILE__, __LINE__);
	CPPUNIT_ASSERT(succeeded);
	CPPUNIT_ASSERT(mutex.isLocked());

	//unlock.
	mutex.unlock();
}

/**
 * Accuracy test of<code>lock(const char *file, uint line)</code> method.
 */
void MutexTestCase::testLock() {
	Mutex mutex("NewMutexTestCase::testLock", __FILE__, __LINE__);

	//lock.
	mutex.lock(__FILE__, __LINE__);
	CPPUNIT_ASSERT(mutex.isLocked());

	//unlock
	mutex.unlock();
	CPPUNIT_ASSERT(!mutex.isLocked());
	
	//lock again
	mutex.lock(__FILE__, __LINE__);
	CPPUNIT_ASSERT(mutex.isLocked());
	
	//unlock again.
	mutex.unlock();
	CPPUNIT_ASSERT(!mutex.isLocked());
}

/**
 *Accuracy test of<code>timedLock(int timeoutMs, const char *file, uint line)</code> method.
 */
void MutexTestCase::testTimedLock() {
	Mutex mutex("NewMutexTestCase::testTimedLock", __FILE__, __LINE__);
	bool succeeded = false;

	//1: lock. 
	succeeded = mutex.timedLock(-1, __FILE__, __LINE__);
	CPPUNIT_ASSERT(succeeded);
	CPPUNIT_ASSERT(mutex.isLocked());
	//unlock
	mutex.unlock();
	CPPUNIT_ASSERT(!mutex.isLocked());

	//2: timeout 0 lock.
	succeeded = mutex.timedLock(0, __FILE__, __LINE__);
	CPPUNIT_ASSERT(succeeded);
	CPPUNIT_ASSERT(mutex.isLocked());
	//unlock
	mutex.unlock();
	CPPUNIT_ASSERT(!mutex.isLocked());

	//3: timed lock with certain time
	succeeded = mutex.timedLock(someTime, __FILE__, __LINE__);
	CPPUNIT_ASSERT(succeeded);
	CPPUNIT_ASSERT(mutex.isLocked());
	//unlock
	mutex.unlock();
	CPPUNIT_ASSERT(!mutex.isLocked());

	//4: 2 successive timed locks.
	succeeded = mutex.timedLock(someTime, __FILE__, __LINE__);
	CPPUNIT_ASSERT(succeeded);
	CPPUNIT_ASSERT(mutex.isLocked());
	//fail expected.
	succeeded = mutex.timedLock(someTime, __FILE__, __LINE__);
	CPPUNIT_ASSERT(!succeeded);
	CPPUNIT_ASSERT(mutex.isLocked());
	//unlock
	mutex.unlock();
	CPPUNIT_ASSERT(!mutex.isLocked());
}

/**
 *Accuracy test of<code>testUnlock()</code> method.
 */
void MutexTestCase::testUnlock() {
	Mutex mutex("NewMutexTestCase::testUnlock", __FILE__, __LINE__);

	//1: lock - unlock.
	mutex.lock(__FILE__, __LINE__);
	assert(mutex.isLocked());
	mutex.unlock();
	assert(!mutex.isLocked());

	//2: timedLock - unlock.
	mutex.timedLock(-1, __FILE__, __LINE__);
	assert(mutex.isLocked());
	mutex.unlock();
	assert(!mutex.isLocked());

	//3: tryLock - unlock.
	mutex.tryLock(__FILE__, __LINE__);
	assert(mutex.isLocked());
	mutex.unlock();
	assert(!mutex.isLocked());
}

/**
 * Accuracy test combination of lock methods.
 */
void MutexTestCase::testLockMethodMisc() {
	Mutex mutex("NewMutexTestCase::testLockMethodMisc", __FILE__, __LINE__);
	int noTime = 0;
	bool succeeded = false;

	//1: lock, tryLock, timedLock
	CPPUNIT_ASSERT(!mutex.isLocked());
	mutex.lock(__FILE__, __LINE__);
	CPPUNIT_ASSERT(mutex.isLocked());
	
	succeeded = mutex.tryLock(__FILE__, __LINE__);
	CPPUNIT_ASSERT(!succeeded);
	CPPUNIT_ASSERT(mutex.isLocked());

	succeeded = mutex.timedLock(noTime, __FILE__, __LINE__);
	CPPUNIT_ASSERT(!succeeded);
	CPPUNIT_ASSERT(mutex.isLocked());

	succeeded = mutex.timedLock(someTime, __FILE__, __LINE__);
	CPPUNIT_ASSERT(!succeeded);
	CPPUNIT_ASSERT(mutex.isLocked());
	
	//unlock
	mutex.unlock();

	//2: timedLock, tryLock, timedLock
	succeeded = mutex.timedLock(someTime, __FILE__, __LINE__);
	CPPUNIT_ASSERT(succeeded);
	CPPUNIT_ASSERT(mutex.isLocked());

	succeeded = mutex.tryLock(__FILE__, __LINE__);
	CPPUNIT_ASSERT(!succeeded);
	CPPUNIT_ASSERT(mutex.isLocked());

	succeeded = mutex.timedLock(someTime, __FILE__, __LINE__);
	CPPUNIT_ASSERT(!succeeded);
	CPPUNIT_ASSERT(mutex.isLocked());

	//unlock.
	mutex.unlock();

	//3: tryLock, timedLock, tryLock
	succeeded = mutex.tryLock(__FILE__, __LINE__);
	CPPUNIT_ASSERT(succeeded);
	CPPUNIT_ASSERT(mutex.isLocked());

	succeeded = mutex.timedLock(someTime, __FILE__, __LINE__);
	CPPUNIT_ASSERT(!succeeded);
	CPPUNIT_ASSERT(mutex.isLocked());

	succeeded = mutex.tryLock(__FILE__, __LINE__);
	CPPUNIT_ASSERT(!succeeded);
	CPPUNIT_ASSERT(mutex.isLocked());

	//unlock.
	mutex.unlock();

	//4: arbitrary lock-unlock, tryLock-unlock, timedLock-unlock pair.
	System::srandom((int)System::microTime());
	succeeded = false;
	for (int loops = 0; loops < 100; ++loops) {		
		int num = (System::random()%4);
		switch (num) {
			case 0:
				mutex.lock(__FILE__, __LINE__);
				assert(mutex.isLocked());
				break;
			case 1:
				succeeded = mutex.tryLock(__FILE__, __LINE__);
				assert(succeeded);
				assert(mutex.isLocked());
				break;
			case 2:
				succeeded = mutex.timedLock(someTime, __FILE__, __LINE__);
				assert(succeeded);
				assert(mutex.isLocked());
				break;
			case 3:
				succeeded = mutex.timedLock(noTime, __FILE__, __LINE__);
				assert(succeeded);
				assert(mutex.isLocked());
				break;
			default:
				CPPUNIT_ASSERT(false);//shall not happen.
				break;
		}
		mutex.unlock();
		assert(!mutex.isLocked());
	}
}

/**
 * Accuracy test of <code>isLocked</code> methods.
 */
void MutexTestCase::testIsLocked() {
	Mutex mutex("NewMutexTestCase::testIsLocked", __FILE__, __LINE__);

	CPPUNIT_ASSERT(!mutex.isLocked());
	mutex.lock(__FILE__, __LINE__);
	CPPUNIT_ASSERT(mutex.isLocked());
	mutex.unlock();

	CPPUNIT_ASSERT(!mutex.isLocked());
	mutex.tryLock(__FILE__, __LINE__);
	CPPUNIT_ASSERT(mutex.isLocked());
	mutex.unlock();

	CPPUNIT_ASSERT(!mutex.isLocked());
	mutex.timedLock(-1,__FILE__, __LINE__);
	CPPUNIT_ASSERT(mutex.isLocked());
	mutex.unlock();

	CPPUNIT_ASSERT(!mutex.isLocked());
	mutex.timedLock(0,__FILE__, __LINE__);
	CPPUNIT_ASSERT(mutex.isLocked());
	mutex.unlock();

	CPPUNIT_ASSERT(!mutex.isLocked());
	mutex.timedLock(someTime,__FILE__, __LINE__);
	CPPUNIT_ASSERT(mutex.isLocked());
	mutex.unlock();

}

/**
 * Accuracy test of <code>getUsage</code> methods.
 */
void MutexTestCase::testGetUsage() {
	Mutex mutex("NewMutexTestCase::testGetUsage", __FILE__, __LINE__);
	const MutexUsage *pUsage = NULL;
	pUsage = mutex.getUsage();
	CPPUNIT_ASSERT(NULL != pUsage);
	CPPUNIT_ASSERT(0 == pUsage->m_lockCnt);
	CPPUNIT_ASSERT(1 == pUsage->m_instanceCnt);

	const int loops = 100;

	for (int i = 1; i <= loops; i++) {
		switch (i%5) {
			case 0:
				mutex.lock(__FILE__, __LINE__);
				break;
			case 1: 
				mutex.tryLock(__FILE__, __LINE__);
				break;
			case 2:
				mutex.timedLock(-1, __FILE__, __LINE__);
				break;
			case 3:
				mutex.timedLock(0,__FILE__, __LINE__);
				break;
			case 4:
				mutex.timedLock(someTime, __FILE__, __LINE__);
				break;
			default:
				break;
		}
		mutex.unlock();
		CPPUNIT_ASSERT(i == pUsage->m_lockCnt);	
	}
	CPPUNIT_ASSERT(1 == pUsage->m_instanceCnt);	
}

/** 
 * 测试MutexGuid
 */
void MutexTestCase::testMutexGuard() {
	Mutex lock("testMutexGuard", __FILE__, __LINE__);

	{
		MutexGuard guard(&lock, __FILE__, __LINE__);
		CPPUNIT_ASSERT(!TRYLOCK(&lock));
	}
	CPPUNIT_ASSERT(TRYLOCK(&lock));
	UNLOCK(&lock);

	{
		MutexGuard guard(&lock, __FILE__, __LINE__);
		CPPUNIT_ASSERT(!TRYLOCK(&lock));
		guard.unlock();
		CPPUNIT_ASSERT(TRYLOCK(&lock));
	}
	CPPUNIT_ASSERT(!TRYLOCK(&lock));
	UNLOCK(&lock);
	
	{
		MutexGuard guard(&lock, __FILE__, __LINE__);
		CPPUNIT_ASSERT(!TRYLOCK(&lock));
		guard.unlock();
		CPPUNIT_ASSERT(TRYLOCK(&lock));
		UNLOCK(&lock);
		guard.lock(__FILE__, __LINE__);
		CPPUNIT_ASSERT(!TRYLOCK(&lock));
	}
	CPPUNIT_ASSERT(TRYLOCK(&lock));
	UNLOCK(&lock);
}

/** 
 * Mutex测试线程辅助类。
 * 用途：执行一次lock，持续hold住锁timeout 毫秒在
 */
class TestThread : public Thread {
public:
	/**
	 * 构造函数。
	 * @param pMutex, 锁
	 * @param holdTimeMs, 持有锁定毫秒数。
	 */
	TestThread(Mutex *pMutex, int holdTimeMs) : Thread("TestThread") {
		m_mutex = pMutex;
		m_holdTimeMs = holdTimeMs;
	}
	/**
	 * 运行。
	 * @see <code>Thread::run()</code> 方法。
	 */
	void run() {
		m_mutex->lock(__FILE__, __LINE__);
		Thread::msleep(m_holdTimeMs);
		m_mutex->unlock();
	}
private:
	Mutex *m_mutex;		/* 锁 */
	int m_holdTimeMs;	/* 持有锁定毫秒数 */
};
/** 
 * 测试timeout lock初次尝试失败，在未timout期间内成功获取锁的情况。
 */
void MutexTestCase::testTimeLockConflictGain() {
	Mutex mutex(__FUNCTION__, __FILE__, __LINE__);
	TestThread t(&mutex, 2 * 1000);
	t.start();
	Thread::msleep(500);
	bool ok = false;
	ok = mutex.timedLock(2 * 1000, __FILE__, __LINE__);
	CPPUNIT_ASSERT(ok);
	mutex.unlock();
	t.join();

	TestThread t2(&mutex, 2000);
	t2.start();
	Thread::msleep(500);
	ok = false;
	ok = mutex.timedLock(500, __FILE__, __LINE__);
	CPPUNIT_ASSERT(!ok);
	t2.join();
}

void MutexTestCase::testPrintUsage() {
	Mutex l1(__FUNCTION__, __FILE__, __LINE__);
	Mutex l2(__FUNCTION__, __FILE__, __LINE__);

	l1.lock(__FILE__, __LINE__);
	l2.lock(__FILE__, __LINE__);

	MutexUsage::printAll(cout);
}


/**
 * <code>Mutex</code>类多线程测试类。
 */
class MutexHelperMT: public Thread {
public:
	/**
	 * 构造函数。
	 * 
	 * @param mutexArray,	互斥锁
	 * @param numMutex,		互斥锁个数
	 * @param loopCount,	每次锁住后执行操作循环次数
	 * @param stopTime,		测试停止时间
	 */
	MutexHelperMT(Mutex **mutexArray, size_t numMutex, uint loopCount, time_t stopTime, int tryLockPercent, int someTimeLockPercent): Thread("MutexHelperMT") {
		assert(mutexArray);
		assert(numMutex > 0);
		m_mutexArray = mutexArray;
		m_numMutex = numMutex;
		m_loopCount = loopCount;
		m_stopTime = stopTime;
		m_tryLockPercent = tryLockPercent;
		m_someTimeLockPercent= someTimeLockPercent	;
	}

	/**
	 * 运行。
	 * @see <code>Thread::run()</code> 方法。
	 */
	void run() {
		while (time(NULL) < m_stopTime) {
			int idx = (int)(System::random() % m_numMutex);
			for (uint i = 0; i < m_loopCount; i++) {
				System::srandom((int)System::microTime());
				if (System::random()%100 < m_tryLockPercent) {
					while(!m_mutexArray[idx]->tryLock(__FILE__, __LINE__));
				} else if (System::random()%100 < m_tryLockPercent + m_someTimeLockPercent) {
					while(!m_mutexArray[idx]->timedLock(1000, __FILE__, __LINE__));
				} else {
					LOCK(m_mutexArray[idx]);
				}
				assert(0 == MutexBigTestCase::testNumMutexArr[idx]);
				MutexBigTestCase::testNumMutexArr[idx]++;
				
				int newIdx = idx;
				for (int n = 0; n < 200; n++)
					newIdx = (newIdx + 7 + m_id) % m_numMutex;			

				Thread::msleep(System::random()%10);
				assert(1 == MutexBigTestCase::testNumMutexArr[idx]);
				MutexBigTestCase::testNumMutexArr[idx]--;
				UNLOCK(m_mutexArray[idx]);

				idx = newIdx;
			}
		}
	}

private:
	Mutex	**m_mutexArray;	/* 互斥锁数组 */
	size_t	m_numMutex;			/* 锁个数*/
	uint	m_loopCount;		/* 内部运行循环次数 */
	time_t m_stopTime;			/* 内部线程停止运行的绝对时间 */

	int m_tryLockPercent;		/* tryLock的百分比例。 */
	int m_someTimeLockPercent;	/* 限时lock的百分比例 */

};

/**
 * Accuracy test of <code>Mutex</code> with multiple threads.
 * 
 */
void MutexBigTestCase::testMutexStableMT() {
	EventMonitorHelper::pauseMonitor();
	const int MUTEX_NUM = 95;
	const int THREAD_NUM = 1000;
	uint loopCount = 200;

	const int TEST_DURATION_SECOND = 60 * 10;
	
	int tryLockPercent = 30;
	int someTimeLockPercent = 20;

	//测试线程全部启动-竞争->全部执行完毕的循环次数。
	const int TEST_TOTAL_TIMES = 10; 

	Mutex	*mutexes[MUTEX_NUM];
	MutexHelperMT	*threads[THREAD_NUM];
	MutexBigTestCase::testNumMutexArr = new int[MUTEX_NUM];

	cout<<"测试Mutex的正确性测试：testMutexSmallMT 开始@"<<time(NULL)<<endl;
	cout<<"测试大致需要"<<TEST_DURATION_SECOND * TEST_TOTAL_TIMES / 60<<"分钟"<<endl;
	for (int testTime = 0; testTime < TEST_TOTAL_TIMES; testTime++) {	
	
		//调整线程运行结束时间
		time_t stopTime = time(NULL) + TEST_DURATION_SECOND;
		for (int i = 0; i < sizeof(mutexes) / sizeof(mutexes[0]); i++) {
			mutexes[i] = new Mutex("testMutexMT", __FILE__, __LINE__);
			MutexBigTestCase::testNumMutexArr[i] = 0;
		}
		
		for (size_t i = 0; i < sizeof(threads) / sizeof(threads[0]); i++) {
			threads[i] = new MutexHelperMT(mutexes, sizeof(mutexes) / sizeof(mutexes[0]), loopCount, stopTime, tryLockPercent, someTimeLockPercent);
		}
		u64 before = System::currentTimeMillis();
		for (size_t i = 0; i < sizeof(threads) / sizeof(threads[0]); i++) {
			threads[i]->start();
		}

		for (size_t i = 0; i < sizeof(threads) / sizeof(threads[0]); i++) {
			threads[i]->join();
		}
		u64 after = System::currentTimeMillis();

		cout << "New Mutex small MT test Time: " << (after - before) << " ms" << endl;
		cout<<"第"<<testTime<<"/"<<TEST_TOTAL_TIMES<<"次 NewMutex测试序列循环 完成"<<endl;
		
		for (size_t i = 0; i < sizeof(threads) / sizeof(threads[0]); i++) {
			delete threads[i];
		}		
		
		for (int i = 0; i < sizeof(mutexes) / sizeof(mutexes[0]); i++) {
			delete mutexes[i];	
		}	
	}
	delete[] MutexBigTestCase::testNumMutexArr;	
}



const char* MutexBigTestCase::getName() {
	return "Mutex Synchronization performance/stabilization test";
}

const char* MutexBigTestCase::getDescription() {
	return "Test Mutex performance and stabilization";
}

int *MutexBigTestCase::testNumMutexArr = NULL;

/** 
 * 测试类型
 * 
 * @return false, 非小型单元测试。
 */
bool MutexBigTestCase::isBig() {
	return true; 
}

/** 性能日志
 * 2008/6/10:
 *   使用pthread_mutex_t实现
 *   lock/unlock: 54 cc
 * 2008/6/10:
 *   使用Atomic实现
 *   lock/unlock: 27 cc
 * 2008/6/11:
 *   使用Atomic实现，发生冲突时的代码不inline
 *   lock/unlock: 26 cc
 * 2009/5/25th: @DB-10 33 cc
 */
void MutexBigTestCase::testMutexST() {
	cout << "  Test performance of Mutex" << endl;

	int repeat = 10000000;
	Mutex m("testMutexST", __FILE__, __LINE__);
	u64 before = System::clockCycles();
	for (int i = 0; i < repeat; i++) {
		LOCK(&m);
		UNLOCK(&m);
	}	
	u64 after = System::clockCycles();
	cout << "  clock cycles per lock/unlock pair: " << (after - before) / repeat << endl;
	}

/**
 * 测试InnoDB Mutex的性能
 */
void MutexBigTestCase::testInnoMutexST() {
	cout << "  Test performance of Inno Mutex" << endl;
	mutex_t *mutex = new mutex_t;
	mutex->lock_word.set(0);
	int repeat = 10000000;	
	mutex_create(mutex);

	u64 before = System::clockCycles();
	for (int i = 0; i < repeat; i++) {
		mutex_enter_func(mutex, __FILE__, __LINE__);
		mutex_exit(mutex);
	}
	u64 after = System::clockCycles();
	cout << "  clock cycles per lock/unlock pair: " << (after - before) / repeat << endl;
	mutex_free(mutex);
	mutex = NULL;
}

/*统计Mutex运行*/
static Atomic<int>	g_maxMutexLockTime = -1;
static Atomic<int> g_minMutexLockTime = 99999;
static u64			g_mutexLockCnt = 0;
static u64			g_totalMutexLockTime = 0;



class MutexMT: public Thread {
public:
	MutexMT(Mutex **mutexArray, size_t numMutex, uint loopCount): Thread("MutexMT") {
		m_mutexArray = mutexArray;
		m_numMutex = numMutex;
		m_loopCount = loopCount;
	}

	void run() {
		int idx = (int)(System::random() % m_numMutex);
		for (uint i = 0; i < m_loopCount; i++) {
			u64 before = System::microTime();

			LOCK(m_mutexArray[idx]);
			int newIdx = idx;
			for (int n = 0; n < 200; n++)
				newIdx = (newIdx + 7 + m_id) % m_numMutex;
			UNLOCK(m_mutexArray[idx]);

			u64 after = System::microTime();
			int lockTime = (int)(after - before);
			Atomic<int> *maxLockTime = &g_maxMutexLockTime;
			while (lockTime > maxLockTime->get()) {
				int oldLockTime = maxLockTime->get();
				if (oldLockTime >= lockTime)
					break;
				if (maxLockTime->compareAndSwap(oldLockTime, lockTime))
					break;
			}

			g_mutexLockCnt++;
			g_totalMutexLockTime += lockTime;


			idx = newIdx;


		}
	}

private:
	Mutex	**m_mutexArray;
	size_t	m_numMutex;
	uint	m_loopCount;
};




/**测试Mutex以及InnoDB Mutex多线程性能.
 *
 *	Note:	测试结果：DB-10 2009-05-15th 16:32
 *		
 *		Testing performance of <code>Mutex</code> with multiple threads
 *		Time: 168666  ms
 *		Throughput: 296  ops/ms
 *		Avg Mutex(new) time: 275  ms
 *		Max Mutex(new) time:3744,119us
 *		
 *		Testing performance of Innodb Mutex with multiple threads
 *		Time: 167034 ms
 *		Throughput: 299 ops/ms
 *		Avg Mutex(innodb) lock time: 282 ms
 *		Max Mutex(innodb) lock time:4529874us
 *		 : OK
 *		OK (1)
 */
void MutexBigTestCase::testMutexVsInnoMutex() {	
	//运行Mutex多线程
	testMutexMT();
	//运行InnoDb Mutex多线程测试
	testMutexInnoDbMT();
}

/**
 * InnoDb Mutex测试线程类
 */
class MutexInnoDbMT : public Thread{
public:
	MutexInnoDbMT(mutex_t **mutexArray, size_t numMutex, uint loopCount): Thread("MutexInnoDbMT") {
		m_mutexArray = mutexArray;
		m_numMutex = numMutex;
		m_loopCount = loopCount;
		}
	void run() {
		int idx = (int)(System::random() % m_numMutex);
		for (uint i = 0; i < m_loopCount; i++) {
			u64 before = System::microTime();
			mutex_enter(m_mutexArray[idx]);
			int newIdx = idx;
			for (int n = 0; n < 200; n++)
				newIdx = (newIdx + 7 + m_id) % m_numMutex;
			mutex_exit(m_mutexArray[idx]);

			u64 after = System::microTime();
			int lockTime = (int)(after - before);
			Atomic<int> *maxLockTime = &g_maxMutexLockTime;
			while (lockTime > maxLockTime->get()) {
				int oldLockTime = maxLockTime->get();
				if (oldLockTime >= lockTime)
					break;
				if (maxLockTime->compareAndSwap(oldLockTime, lockTime))
					break;
				}

			g_mutexLockCnt++;
			g_totalMutexLockTime += lockTime;

			idx = newIdx;
			}
		}
private:
	mutex_t	**m_mutexArray;	/*mutex数组*/
	size_t	m_numMutex;		/*mutex个数*/
	uint	m_loopCount;	/*循环次数*/
	};

/**
 *InnoDb mutex 性能测试
 */
void MutexBigTestCase::testMutexInnoDbMT() {
	cout << "Testing performance of Innodb Mutex with multiple threads" << endl;

	/*初始化统计*/
	g_maxMutexLockTime = -1;
	//g_minMutexLockTime = 99999;
	g_mutexLockCnt = 0;
	g_totalMutexLockTime = 0;

	mutex_t	*mutexes[95];
	for (int i = 0; i < sizeof(mutexes) / sizeof(mutexes[0]); i++) {
		mutexes[i] = new mutex_t;
		mutex_create(mutexes[i]);		
	}
	MutexInnoDbMT	*threads[100];
	uint loopCount = 500000;
	for (size_t i = 0; i < sizeof(threads) / sizeof(threads[0]); i++)
		threads[i] = new MutexInnoDbMT(mutexes, sizeof(mutexes) / sizeof(mutexes[0]), loopCount);

	u64 before = System::currentTimeMillis();
	for (size_t i = 0; i < sizeof(threads) / sizeof(threads[0]); i++)
		threads[i]->start();

	for (size_t i = 0; i < sizeof(threads) / sizeof(threads[0]); i++)
		threads[i]->join();
	u64 after = System::currentTimeMillis();

	for (size_t i = 0; i < sizeof(threads) / sizeof(threads[0]); i++) 
		delete threads[i];

	cout << "Time: " << (after - before) << " ms" << endl;
	cout << "Throughput: " << (sizeof(threads) / sizeof(threads[0]) * loopCount) / (after - before) << " ops/ms" << endl;
	if (g_mutexLockCnt > 0) {
		cout << "Avg Mutex(innodb) lock time: " << g_totalMutexLockTime / g_mutexLockCnt << " ms" << endl;
		cout<<"Max Mutex(innodb) lock time:"<<g_maxMutexLockTime.get()<<"us"<<endl;
		}

	for (int i = 0; i < sizeof(mutexes) / sizeof(mutexes[0]); i++)
		mutex_free(mutexes[i]);
}

/**
 * 测试<code>Mutex</code>的多线程性能。
 */
void MutexBigTestCase::testMutexMT() {
	cout << "Testing performance of New Mutex with multiple threads" << endl;

	/*初始化统计*/
	g_maxMutexLockTime = -1;
	g_minMutexLockTime = 99999;
	g_mutexLockCnt = 0;
	g_totalMutexLockTime = 0;

	Mutex	*mutexes[95];
	for (int i = 0; i < sizeof(mutexes) / sizeof(mutexes[0]); i++)
		mutexes[i] = new Mutex("testMutexMT", __FILE__, __LINE__);
	MutexMT	*threads[100];
	uint loopCount = 500000;
	for (size_t i = 0; i < sizeof(threads) / sizeof(threads[0]); i++)
		threads[i] = new MutexMT(mutexes, sizeof(mutexes) / sizeof(mutexes[0]), loopCount);

	u64 before = System::currentTimeMillis();
	for (size_t i = 0; i < sizeof(threads) / sizeof(threads[0]); i++)
		threads[i]->start();

	for (size_t i = 0; i < sizeof(threads) / sizeof(threads[0]); i++)
		threads[i]->join();
	u64 after = System::currentTimeMillis();
	
	for (size_t i = 0; i < sizeof(threads) / sizeof(threads[0]); i++) 
		delete threads[i];
	
	cout << "Time: " << (after - before) << " ms" << endl;
	cout << "Throughput: " << (sizeof(threads) / sizeof(threads[0]) * loopCount) / (after - before) << " ops/ms" << endl;
	if (g_mutexLockCnt > 0) {
		cout << "Avg Mutex(new) time: " << g_totalMutexLockTime / g_mutexLockCnt << " ms" << endl;
		cout<<"Max Mutex(new) time:"<<g_maxMutexLockTime.get()<<"us"<<endl;
	}
	for (int i = 0; i < sizeof(mutexes) / sizeof(mutexes[0]); i++)
		delete mutexes[i];	
}


/** 
 * 单向链表节点
 */
struct ListNode {
	uint tid;
	ListNode *next;
};
/** 全局链表 */
ListNode *g_list;
/** 下一个应该分配的节点 */
ListNode *g_lastRecycledNode;

/** 
 * 测试测试Mutex用于保护全局链表辅助线程类。
 */
class ListOperatorThread : public Thread {
public:
	/**
	 * 构造函数。
	 * @param pMutex, 锁
	 */
	ListOperatorThread(Mutex *pMutex) : Thread("ListOperatorThread") {
		assert(NULL != pMutex);
		m_mutex = pMutex;
		m_list = NULL;
		count = 0;
		m_flag = true;	
	}	

	/**	 
	* 取消运行，停止死循环，让线程退出
	*/
	void cancel() {		
		m_flag = false;	
	}	

	/**	 
	* 运行。	 
	* @see <code>Thread::run()</code> 方法。	 
	*/	
	void run() {		
		while (m_flag) {		
			int num = System::random()%6;
			switch (num) {
				case 0:
				case 1:
				case 2:
					fetchOne();
					break;
				case 3:
				case 4:
					releaseOne();
					break;
				case 5:
					releaseAll();
					break;
				default:
					NTSE_ASSERT(false);
			}
		}
	}

private:
	/** 
	 * 从全局列表获取一个节点。
	 */
	void fetchOne() {
		if (count == MAX_COUNT) {
			return;
		}

		ListNode *localNodes = m_list;
		m_mutex->lock(__FILE__, __LINE__);
		NTSE_ASSERT(NULL != g_list);
		m_list = g_list;
		g_list = g_list->next;
		m_mutex->unlock();
		m_list->next = localNodes;
		m_list->tid = Thread::currentThread()->getId();		
		++count;
	}
	/** 
	 * 回收释放一个节点到全局列表。
	 */
	void releaseOne() {
		if (0 == count) {
			return;
		}
		int index = System::random() % count;
		ListNode *targetNode;
		ListNode *preTargetNode = m_list;
		targetNode = m_list;
		while (index > 0) {
			preTargetNode = targetNode;
			targetNode = targetNode->next;
			--index;
		}
		if (targetNode == m_list) {
			m_list = m_list->next;
		} else {
			preTargetNode->next = targetNode->next;
		}
		NTSE_ASSERT(Thread::currentThread()->getId() == targetNode->tid);

		m_mutex->lock(__FILE__, __LINE__);
		
		targetNode->next = g_list;
		g_list = targetNode;		
		//cout<<"released"<< Thread::currentThread()->getId() <<endl;
		m_mutex->unlock();
		--count;
	}
	/** 
	 * 回收释放所有节点到全局列表。
	 */
	void releaseAll() {
		while (count > 0) {
			releaseOne();
		}
	}
private:
	Mutex *m_mutex;		/* 锁 */
	uint count;			/* 拥有节点数目*/
	ListNode *m_list;	/* 拥有的节点链表 */
	static const int MAX_COUNT = 5;	/* 拥有最大的节点数 */
	bool m_flag;		/* 停止测试标记位 */
};
/** 
 * 测试Mutex保护链表
 */
void MutexBigTestCase::testProtectListMT() {
	g_list = new ListNode;
	g_list->tid = -1;
	g_list->next = NULL;
	for (int i = 0; i < 300; i++) {
		ListNode *tmpNode = new ListNode;
		tmpNode->tid = -1;
		tmpNode->next = g_list;
		g_list = tmpNode;
	}
	ListOperatorThread	*threads[50];
	Mutex *mutex = new Mutex("ListOperatorThread Lock", __FILE__, __LINE__);
	for (size_t i = 0; i < sizeof(threads) / sizeof(threads[0]); i++)
		threads[i] = new ListOperatorThread(mutex);
	
	for (size_t i = 0; i < sizeof(threads) / sizeof(threads[0]); i++)
		threads[i]->start();

	Thread::msleep(1000 * 120);// test for two minutes

	for (size_t i = 0;i < sizeof(threads) / sizeof(threads[0]); i++)
		threads[i]->cancel();

	for (size_t i = 0; i < sizeof(threads) / sizeof(threads[0]); i++)
		threads[i]->join();
	
	for (size_t i = 0; i < sizeof(threads) / sizeof(threads[0]); i++) 
		delete threads[i];

	for (int i = 0; i < 300; i++) {
		ListNode *tmpNode = g_list->next;
		delete g_list;
		g_list = tmpNode;
	}
	delete g_list;
	g_list = NULL;

	delete mutex;
	mutex = NULL;
	
}