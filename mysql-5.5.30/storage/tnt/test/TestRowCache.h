/**
 * 测试记录缓存功能
 *
 * @author 汪源(wy@163.org)
 */

#ifndef _NTSETEST_ROW_CACHE_H_
#define _NTSETEST_ROW_CACHE_H_

#include <cppunit/extensions/HelperMacros.h>
#include "RowCache.h"

using namespace ntse;

class RowCacheTestCase: public CPPUNIT_NS::TestFixture {
	CPPUNIT_TEST_SUITE(RowCacheTestCase);
	CPPUNIT_TEST(testBasic);
	CPPUNIT_TEST(testExceedMemLimit);
	CPPUNIT_TEST(testLob);
	CPPUNIT_TEST_SUITE_END();

public:
	static const char* getName();
	static const char* getDescription();
	static bool isBig();

protected:
	void testBasic();
	void testExceedMemLimit();
	void testLob();
};
 
#endif
