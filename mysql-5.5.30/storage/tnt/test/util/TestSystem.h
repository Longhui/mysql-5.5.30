/**
 * 测试System.h中声明的可移植性相关函数等
 *
 * @author 汪源(wangyuan@corp.netease.com, wy@163.org)
 */

#ifndef _NTSETEST_SYSTEM_H_
#define _NTSETEST_SYSTEM_H_

#include <cppunit/extensions/HelperMacros.h>

/** 功能测试 */
class SystemTestCase: public CPPUNIT_NS::TestFixture {
	CPPUNIT_TEST_SUITE(SystemTestCase);
	CPPUNIT_TEST_SUITE_END();

public:
	static const char* getName();
	static const char* getDescription();
	static bool isBig();
};

/** 性能测试 */
class SystemBigTest: public CPPUNIT_NS::TestFixture {
	CPPUNIT_TEST_SUITE(SystemBigTest);
	CPPUNIT_TEST(testTiming);
	CPPUNIT_TEST_SUITE_END();

public:
	static const char* getName();
	static const char* getDescription();
	static bool isBig();

protected:
	void testTiming();
};

#endif
