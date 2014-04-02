/**
 * ≤‚ ‘ƒ⁄¥Ê“≥≥ÿ
 *
 * @author ÕÙ‘¥(wangyuan@corp.netease.com, wy@163.org)
 */

#ifndef _NTSETEST_POOL_H_
#define _NTSETEST_POOL_H_


#include <cppunit/extensions/HelperMacros.h>

class PoolTestCase: public CPPUNIT_NS::TestFixture {
	CPPUNIT_TEST_SUITE(PoolTestCase);
	CPPUNIT_TEST(testAlloc);
	CPPUNIT_TEST(testDynamicAdjust);
	CPPUNIT_TEST(testLock);
	CPPUNIT_TEST(testAlign);
	CPPUNIT_TEST(testScan);
	CPPUNIT_TEST_SUITE_END();

public:
	static const char* getName();
	static const char* getDescription();
	static bool isBig();

protected:
	void testAlloc();
	void testDynamicAdjust();
	void testLock();
	void testAlign();
	void testScan();
};

#endif
