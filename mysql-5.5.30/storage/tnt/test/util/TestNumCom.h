/**
 * 有序整数压缩算法测试
 *
 * @author 余利华(yulihua@corp.netease.com, ylh@163.org)
 */

#ifndef _NTSETEST_NUMBERCOMPRESS_H_
#define _NTSETEST_NUMBERCOMPRESS_H_

#include <cppunit/extensions/HelperMacros.h>

/** 有序整数压缩算法测试用例 */
class NumberCompressTestCase: public CPPUNIT_NS::TestFixture {
	CPPUNIT_TEST_SUITE(NumberCompressTestCase);
	CPPUNIT_TEST(testCompress);
	CPPUNIT_TEST_SUITE_END();

public:
	static const char* getName();
	static const char* getDescription();
	static bool isBig();

protected:
	void testCompress();
};

/** 有序整数压缩算法测试用例 */
class NumberCompressBigTest: public CPPUNIT_NS::TestFixture {
	CPPUNIT_TEST_SUITE(NumberCompressBigTest);
	CPPUNIT_TEST(testMisc);
	CPPUNIT_TEST_SUITE_END();

public:
	static const char* getName();
	static const char* getDescription();
	static bool isBig();

protected:
	void testMisc();
};

#endif // _NTSETEST_NUMBERCOMPRESS_H_


