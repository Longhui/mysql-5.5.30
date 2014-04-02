/**
 * ����ԭ����������ͬ������
 *
 * @author ��Դ(wangyuan@corp.netease.com, wy@163.org)
 */

#ifndef _NTSETEST_SYNC_H_
#define _NTSETEST_SYNC_H_

#include <cppunit/extensions/HelperMacros.h>

/** ͬ�����ƹ��ܲ��� */
class SyncTestCase: public CPPUNIT_NS::TestFixture {
	CPPUNIT_TEST_SUITE(SyncTestCase);
	CPPUNIT_TEST(testAtomic);	
	CPPUNIT_TEST(testEvent);
	CPPUNIT_TEST(testILConflict);
	CPPUNIT_TEST(testILLock);
	CPPUNIT_TEST(testILLockThreads);
	CPPUNIT_TEST_SUITE_END();

public:
	static const char* getName();
	static const char* getDescription();
	static bool isBig();

protected:
	void testAtomic();
	void testEvent();
	void testILConflict();
	void testILLock();
	void testILLockThreads();
};

/** ͬ���������ܲ��� */
class SyncBigTest: public CPPUNIT_NS::TestFixture {
	CPPUNIT_TEST_SUITE(SyncBigTest);
	CPPUNIT_TEST(testAtomic);
#ifndef WIN32
	CPPUNIT_TEST(testPthreadMutex);
#endif

	CPPUNIT_TEST(testQrlST);
	CPPUNIT_TEST(testEventPingPong);
	CPPUNIT_TEST_SUITE_END();

public:
	static const char* getName();
	static const char* getDescription();
	static bool isBig();

	static const int NUM_THREADS = 1000;/* �����߳���Ŀ */
	static const int NUM_TEST = 95;		/* ����RWLock��Ŀ */


protected:
	void testAtomic();
#ifndef WIN32
	void testPthreadMutex();
#endif
	void testQrlST();
	void testEventPingPong();

private:
};

#endif
