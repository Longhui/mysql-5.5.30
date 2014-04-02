/**
* 测试Mutex同步机制.
*
* @author 聂明军(niemingjun@corp.netease.com, niemingjun@163.org)
*/

#ifndef _NTSETEST_MUTEX_H_
#define _NTSETEST_MUTEX_H_

#include <cppunit/extensions/HelperMacros.h>



/** Mutex同步机制功能测试 */
class MutexTestCase : public CPPUNIT_NS::TestFixture {
	CPPUNIT_TEST_SUITE(MutexTestCase);
	CPPUNIT_TEST(testMutex);

	CPPUNIT_TEST(testNewMutexConstructor);
	CPPUNIT_TEST(testTryLock);
	CPPUNIT_TEST(testLock);
	CPPUNIT_TEST(testTimedLock);
	CPPUNIT_TEST(testUnlock);	
	CPPUNIT_TEST(testLockMethodMisc);
	CPPUNIT_TEST(testIsLocked);
	CPPUNIT_TEST(testGetUsage);
	CPPUNIT_TEST(testMutexGuard);
	CPPUNIT_TEST(testTimeLockConflictGain);
	CPPUNIT_TEST(testPrintUsage);

	CPPUNIT_TEST_SUITE_END();

public:
	static const char* getName();
	static const char* getDescription();
	static bool isBig();

protected:
	void testMutex();

	void testNewMutexConstructor();
	void testTryLock();
	void testLock();
	void testTimedLock();
	void testUnlock();
	void testLockMethodMisc();
	void testIsLocked();
	void testGetUsage();
	void testMutexGuard();
	void testTimeLockConflictGain();
	void testPrintUsage();

private:
	static const int someTime = 100; /*timeout时间*/	
};


class MutexBigTestCase : public CPPUNIT_NS::TestFixture {
	CPPUNIT_TEST_SUITE(MutexBigTestCase);

	CPPUNIT_TEST(testMutexST);
	CPPUNIT_TEST(testInnoMutexST);

	CPPUNIT_TEST(testMutexMT);
	CPPUNIT_TEST(testProtectListMT);
	CPPUNIT_TEST(testMutexInnoDbMT);

	CPPUNIT_TEST(testMutexVsInnoMutex);
	
	CPPUNIT_TEST(testMutexStableMT);

	CPPUNIT_TEST_SUITE_END();

public:
	static const char* getName();
	static const char* getDescription();
	static bool isBig();

	static int *testNumMutexArr; /*测试多线程的共享处理数组*/

	#ifdef NTSE_MEM_CHECK
	static const int NUM_THREADS = 100;/* 测试线程数目 */
	#else
	static const int NUM_THREADS = 1000;/* 测试线程数目 */
	#endif
	static const int NUM_TEST = 95;		/* 测试Mutex数目 */
protected:
	void testMutexST();
	void testInnoMutexST();
	void testMutexMT();	
	void testProtectListMT();
	void testMutexInnoDbMT();
	
	void testMutexStableMT();
	void testMutexVsInnoMutex();

private:
	static const int someTime = 100; /*timeout时间*/	
};

#endif
