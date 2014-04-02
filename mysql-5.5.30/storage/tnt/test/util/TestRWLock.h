/**
 * <code>RWLock</code>的测试
 *
 * @author  聂明军(niemingjun@corp.netease.com, niemingjun@163.org)
 */

#ifndef _NTSE_TESTNEWRWLOCK_H_
#define _NTSE_TESTNEWRWLOCK_H_

#include <cppunit/extensions/HelperMacros.h>

class RWLockTestCase : public CPPUNIT_NS::TestFixture {
	CPPUNIT_TEST_SUITE(RWLockTestCase);
	CPPUNIT_TEST(testRWLock);
	CPPUNIT_TEST(testRWLockGuard);
	CPPUNIT_TEST(testRWLockConflictGain);
	CPPUNIT_TEST(testPrintUsage);

	CPPUNIT_TEST_SUITE_END();

public:
	static const char* getName();
	static const char* getDescription();
	static bool isBig();

protected:	
	void testRWLock();
	void testRWLockGuard();	
	void testRWLockConflictGain();
	void testPrintUsage();
};

class RWLockBigTestCase : public CPPUNIT_NS::TestFixture {
	CPPUNIT_TEST_SUITE(RWLockBigTestCase);

	CPPUNIT_TEST(testRWLockSTPerf);	
	CPPUNIT_TEST(testRWLockMTAllReadPerf);
	CPPUNIT_TEST(testRWLockMTPerf);
	CPPUNIT_TEST(testRWLockMTTimedPerf);
	
	CPPUNIT_TEST(testRWLockVsInnoRWLock);
	CPPUNIT_TEST(testRWLockMTStable);
	CPPUNIT_TEST(testAllRead);
	CPPUNIT_TEST(testAllWrite);
	CPPUNIT_TEST(testPercentWrite);
	CPPUNIT_TEST(testPercentWritePreferWrite);

	CPPUNIT_TEST_SUITE_END();

public:
	static const char* getName();
	static const char* getDescription();
	static bool isBig();

	#ifdef NTSE_MEM_CHECK
	static const int NUM_THREADS = 100;/* 测试线程数目 */
	#else
	static const int NUM_THREADS = 1000;/* 测试线程数目 */
	#endif
	static const int NUM_TEST = 95;		/* 测试RWLock数目 */
	static int *testNumArr;	/* 测试多线程的共享处理数字 */
	static const int NUM_WLOCK_CHECK_VALUE = 87658; /* 写锁进入时的检查值 */
	static const int NUM_RLOCK_CHECK_VALUE = 41234; /* 写锁进入时的检查值 */
	static const int USE_RWLOCK = 0; 
	static const int USE_INNO_RWLOCK = 1;
protected:

	
	void testRWLockSTPerf();
	void testRWLockMTAllReadPerf();
	void testRWLockMTPerf();
	void testRWLockMTTimedPerf();

	void testRWLockVsInnoRWLock();
	void testRWLockMTStable();
	
	void testAllRead();
	void testAllWrite();
	void testPercentWrite();
	void testPercentWritePreferWrite();


private:
	void doRWLockMTTest(int totalTime, int writePercent, int timedPercent, int timeoutMs);
	void doRWLockMTTest(int totalTime, int runs, int writePercent, int timedPercent, int timeoutMs, int useLockType = USE_RWLOCK, bool preferWrite = false);
	
};


#endif
