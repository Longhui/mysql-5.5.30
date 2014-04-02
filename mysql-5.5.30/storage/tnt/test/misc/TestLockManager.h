/**
 * 测试锁表功能
 * 
 * @author 余利华(yulihua@corp.netease.com, ylh@163.org)
 */


#ifndef _NTSETEST_LOCK_MANAGER_H_
#define _NTSETEST_LOCK_MANAGER_H_

#include <cppunit/extensions/HelperMacros.h>



/** 同步机制功能测试 */
class LockManagerTestCase: public CPPUNIT_NS::TestFixture {
	CPPUNIT_TEST_SUITE(LockManagerTestCase);
	CPPUNIT_TEST(testLockOpers);
	CPPUNIT_TEST(testSpecialLM);
	CPPUNIT_TEST(testConflict);
	CPPUNIT_TEST(testIsLocked);
	CPPUNIT_TEST_SUITE_END();

public:
	static const char* getName();
	static const char* getDescription();
	static bool isBig();

protected:
	void testLockOpers();
	void testSpecialLM();
	void testConflict();
	void testIsLocked();
};


class LockManagerBigTest: public CPPUNIT_NS::TestFixture {
	CPPUNIT_TEST_SUITE(LockManagerBigTest);
	CPPUNIT_TEST(testSKST);
	CPPUNIT_TEST(testMT);
	CPPUNIT_TEST(testScanST);
	CPPUNIT_TEST(testHighConflictThroughput);
	CPPUNIT_TEST_SUITE_END();

public:
	static const char* getName();
	static const char* getDescription();
	static bool isBig();

protected:
	void testSKST();
	void testScanST();
	void testMT();
	void testHighConflictThroughput();
};


class IndicesLockManagerTestCase : public CPPUNIT_NS::TestFixture {
	CPPUNIT_TEST_SUITE(IndicesLockManagerTestCase);
	CPPUNIT_TEST(testSequenceLock);
	CPPUNIT_TEST(testConflict);
	CPPUNIT_TEST(testDeadLock);
	CPPUNIT_TEST_SUITE_END();

public:
	static const char* getName();
	static const char* getDescription();
	static bool isBig();

protected:
	void testSequenceLock();
	void testConflict();
	void testDeadLock();
};


class IndicesLockManagerBigTestCase : public CPPUNIT_NS::TestFixture {
	CPPUNIT_TEST_SUITE(IndicesLockManagerBigTestCase);
	CPPUNIT_TEST(testMT);
	CPPUNIT_TEST(testSequenceLockPerformance);
	CPPUNIT_TEST(testRangeLockPerformance);
	CPPUNIT_TEST(testHighConflictThroughput);
	CPPUNIT_TEST_SUITE_END();

public:
	static const char* getName();
	static const char* getDescription();
	static bool isBig();

protected:
	void testMT();
	void testSequenceLockPerformance();
	void testRangeLockPerformance();
	void testHighConflictThroughput();
};


#endif // _NTSETEST_LOCK_MANAGER_H_
