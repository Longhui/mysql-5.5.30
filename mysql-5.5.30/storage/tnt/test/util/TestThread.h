/**
 * 测试线程操作
 *
 * @author 汪源(wangyuan@corp.netease.com, wy@163.org)
 */

#ifndef _NTSETEST_THREAD_H_
#define _NTSETEST_THREAD_H_

#include <cppunit/extensions/HelperMacros.h>

/** 线程操作测试用例 */
class ThreadTestCase: public CPPUNIT_NS::TestFixture {
	CPPUNIT_TEST_SUITE(ThreadTestCase);
	CPPUNIT_TEST(testThread);
	CPPUNIT_TEST(testTls);
	CPPUNIT_TEST(testBug57496);
	CPPUNIT_TEST(testTask);
	CPPUNIT_TEST(testTaskPause);
	CPPUNIT_TEST(testTaskSignal);
	CPPUNIT_TEST_SUITE_END();

public:
	static const char* getName();
	static const char* getDescription();
	static bool isBig();

protected:
	void testThread();
	void testTls();
	void testBug57496();
	void testTask();
	void testTaskPause();
	void testTaskSignal();
};

#endif
