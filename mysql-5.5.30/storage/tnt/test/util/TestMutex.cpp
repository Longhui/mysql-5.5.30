/**
* ����Mutexͬ������.
*
* @author ������(niemingjun@corp.netease.com, niemingjun@163.org)
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



//Mutex����ʵ�֡�

const char* MutexTestCase::getName() {
	return "Mutex Synchronization facilities test";
}

const char* MutexTestCase::getDescription() {
	return "Test Mutex";
}


/** 
 * ��������
 * 
 * @return false, С�͵�Ԫ���ԡ�
 */
bool MutexTestCase::isBig() {
	return false; 
}

/**
 * Mutex �������ܲ��ԡ�
 */
void MutexTestCase::testMutex() {
	Mutex lock("testMutex", __FILE__, __LINE__);

	const int LOOP_COUNT = 3;
	for (int i = 0; i < LOOP_COUNT; ++i) {
		//����lock
		LOCK(&lock);
		CPPUNIT_ASSERT(lock.isLocked());
		CPPUNIT_ASSERT(!TRYLOCK(&lock));

		//����unlock
		UNLOCK(&lock);
		CPPUNIT_ASSERT(!lock.isLocked());		
		}//end of for.

	//���Ե��߳� ��locked�� tryLock
	CPPUNIT_ASSERT(TRYLOCK(&lock));

	//����lock usage�� lock count ������
	CPPUNIT_ASSERT(lock.getUsage()->m_lockCnt == (LOOP_COUNT * 2 + 1));

	//����lock usage��spin count������
	CPPUNIT_ASSERT(0 == lock.getUsage()->m_spinCnt);

	//����lock usage��wait count������
	CPPUNIT_ASSERT(0 == lock.getUsage()->m_waitCnt);

	//���Ե��߳� locked�� timeLock
	CPPUNIT_ASSERT(!lock.timedLock(100, __FILE__, __LINE__));

	//����lock usage��spin count������
	CPPUNIT_ASSERT(1 == lock.getUsage()->m_spinCnt);
	//����lock usage��wait count������
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
	//lock usage��spin count������
	CPPUNIT_ASSERT(0 == (pMutex->getUsage()->m_spinCnt));
	//lock usage��wait count������
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
 * ����MutexGuid
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
 * Mutex�����̸߳����ࡣ
 * ��;��ִ��һ��lock������holdס��timeout ������
 */
class TestThread : public Thread {
public:
	/**
	 * ���캯����
	 * @param pMutex, ��
	 * @param holdTimeMs, ����������������
	 */
	TestThread(Mutex *pMutex, int holdTimeMs) : Thread("TestThread") {
		m_mutex = pMutex;
		m_holdTimeMs = holdTimeMs;
	}
	/**
	 * ���С�
	 * @see <code>Thread::run()</code> ������
	 */
	void run() {
		m_mutex->lock(__FILE__, __LINE__);
		Thread::msleep(m_holdTimeMs);
		m_mutex->unlock();
	}
private:
	Mutex *m_mutex;		/* �� */
	int m_holdTimeMs;	/* �������������� */
};
/** 
 * ����timeout lock���γ���ʧ�ܣ���δtimout�ڼ��ڳɹ���ȡ���������
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
 * <code>Mutex</code>����̲߳����ࡣ
 */
class MutexHelperMT: public Thread {
public:
	/**
	 * ���캯����
	 * 
	 * @param mutexArray,	������
	 * @param numMutex,		����������
	 * @param loopCount,	ÿ����ס��ִ�в���ѭ������
	 * @param stopTime,		����ֹͣʱ��
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
	 * ���С�
	 * @see <code>Thread::run()</code> ������
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
	Mutex	**m_mutexArray;	/* ���������� */
	size_t	m_numMutex;			/* ������*/
	uint	m_loopCount;		/* �ڲ�����ѭ������ */
	time_t m_stopTime;			/* �ڲ��߳�ֹͣ���еľ���ʱ�� */

	int m_tryLockPercent;		/* tryLock�İٷֱ����� */
	int m_someTimeLockPercent;	/* ��ʱlock�İٷֱ��� */

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

	//�����߳�ȫ������-����->ȫ��ִ����ϵ�ѭ��������
	const int TEST_TOTAL_TIMES = 10; 

	Mutex	*mutexes[MUTEX_NUM];
	MutexHelperMT	*threads[THREAD_NUM];
	MutexBigTestCase::testNumMutexArr = new int[MUTEX_NUM];

	cout<<"����Mutex����ȷ�Բ��ԣ�testMutexSmallMT ��ʼ@"<<time(NULL)<<endl;
	cout<<"���Դ�����Ҫ"<<TEST_DURATION_SECOND * TEST_TOTAL_TIMES / 60<<"����"<<endl;
	for (int testTime = 0; testTime < TEST_TOTAL_TIMES; testTime++) {	
	
		//�����߳����н���ʱ��
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
		cout<<"��"<<testTime<<"/"<<TEST_TOTAL_TIMES<<"�� NewMutex��������ѭ�� ���"<<endl;
		
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
 * ��������
 * 
 * @return false, ��С�͵�Ԫ���ԡ�
 */
bool MutexBigTestCase::isBig() {
	return true; 
}

/** ������־
 * 2008/6/10:
 *   ʹ��pthread_mutex_tʵ��
 *   lock/unlock: 54 cc
 * 2008/6/10:
 *   ʹ��Atomicʵ��
 *   lock/unlock: 27 cc
 * 2008/6/11:
 *   ʹ��Atomicʵ�֣�������ͻʱ�Ĵ��벻inline
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
 * ����InnoDB Mutex������
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

/*ͳ��Mutex����*/
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




/**����Mutex�Լ�InnoDB Mutex���߳�����.
 *
 *	Note:	���Խ����DB-10 2009-05-15th 16:32
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
	//����Mutex���߳�
	testMutexMT();
	//����InnoDb Mutex���̲߳���
	testMutexInnoDbMT();
}

/**
 * InnoDb Mutex�����߳���
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
	mutex_t	**m_mutexArray;	/*mutex����*/
	size_t	m_numMutex;		/*mutex����*/
	uint	m_loopCount;	/*ѭ������*/
	};

/**
 *InnoDb mutex ���ܲ���
 */
void MutexBigTestCase::testMutexInnoDbMT() {
	cout << "Testing performance of Innodb Mutex with multiple threads" << endl;

	/*��ʼ��ͳ��*/
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
 * ����<code>Mutex</code>�Ķ��߳����ܡ�
 */
void MutexBigTestCase::testMutexMT() {
	cout << "Testing performance of New Mutex with multiple threads" << endl;

	/*��ʼ��ͳ��*/
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
 * ��������ڵ�
 */
struct ListNode {
	uint tid;
	ListNode *next;
};
/** ȫ������ */
ListNode *g_list;
/** ��һ��Ӧ�÷���Ľڵ� */
ListNode *g_lastRecycledNode;

/** 
 * ���Բ���Mutex���ڱ���ȫ���������߳��ࡣ
 */
class ListOperatorThread : public Thread {
public:
	/**
	 * ���캯����
	 * @param pMutex, ��
	 */
	ListOperatorThread(Mutex *pMutex) : Thread("ListOperatorThread") {
		assert(NULL != pMutex);
		m_mutex = pMutex;
		m_list = NULL;
		count = 0;
		m_flag = true;	
	}	

	/**	 
	* ȡ�����У�ֹͣ��ѭ�������߳��˳�
	*/
	void cancel() {		
		m_flag = false;	
	}	

	/**	 
	* ���С�	 
	* @see <code>Thread::run()</code> ������	 
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
	 * ��ȫ���б��ȡһ���ڵ㡣
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
	 * �����ͷ�һ���ڵ㵽ȫ���б�
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
	 * �����ͷ����нڵ㵽ȫ���б�
	 */
	void releaseAll() {
		while (count > 0) {
			releaseOne();
		}
	}
private:
	Mutex *m_mutex;		/* �� */
	uint count;			/* ӵ�нڵ���Ŀ*/
	ListNode *m_list;	/* ӵ�еĽڵ����� */
	static const int MAX_COUNT = 5;	/* ӵ�����Ľڵ��� */
	bool m_flag;		/* ֹͣ���Ա��λ */
};
/** 
 * ����Mutex��������
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