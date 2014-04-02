/**
 * 测试链表、数组等简单的辅助工具
 *
 * @author 汪源(wangyuan@corp.netease.com, wy@163.org)
 */

#ifndef _NTSETEST_UTIL_H_
#define _NTSETEST_UTIL_H_

#include <cppunit/extensions/HelperMacros.h>
#include "util/Array.h"

using namespace ntse;
/** 同步机制功能测试 */
class UtilTestCase: public CPPUNIT_NS::TestFixture {
	CPPUNIT_TEST_SUITE(UtilTestCase);
	CPPUNIT_TEST(testDList);
	CPPUNIT_TEST(testArray);
	CPPUNIT_TEST(testSmartPtr);
	CPPUNIT_TEST(testSystem);
	CPPUNIT_TEST(testObjectPool);
	CPPUNIT_TEST(testStream);
	CPPUNIT_TEST_SUITE_END();

public:
	static const char* getName();
	static const char* getDescription();
	static bool isBig();

protected:
	void testDList();
	void testArray();
	void testSmartPtr();
	void testSystem();
	void testObjectPool();
	void testStream();

private:
	void doArrayTest(Array<u64> &a, uint pageSize);
};

#endif
