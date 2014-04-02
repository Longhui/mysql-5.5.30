/**
 * 位图类测试
 *
 * @author 余利华(yulihua@corp.netease.com, ylh@163.org)
 */

#ifndef _NTSETEST_BITMAP_H_
#define _NTSETEST_BITMAP_H_

#include <cppunit/extensions/HelperMacros.h>

/** 位图类测试用例 */
class BitmapTestCase: public CPPUNIT_NS::TestFixture {
	CPPUNIT_TEST_SUITE(BitmapTestCase);
	CPPUNIT_TEST(testBitmap);
	CPPUNIT_TEST(testBitmapOper);
	CPPUNIT_TEST_SUITE_END();

public:
	static const char* getName();
	static const char* getDescription();
	static bool isBig();

protected:
	void testBitmap();
	void testBitmapOper();
};


#endif // _NTSETEST_BITMAP_H_

