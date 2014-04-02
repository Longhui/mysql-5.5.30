/**
* 内存堆单元测试用例
*/
#ifndef _NTSETEST_UTIL_TEST_H_
#define _NTSETEST_UTIL_TEST_H_

#include <cppunit/extensions/HelperMacros.h>


class UtilTestCase: public CPPUNIT_NS::TestFixture {
	CPPUNIT_TEST_SUITE(UtilTestCase);
	CPPUNIT_TEST(testRandDist);
	CPPUNIT_TEST(testZipfRand);
	CPPUNIT_TEST(testZipfParams);
	CPPUNIT_TEST(testZipfCreate);
	CPPUNIT_TEST_SUITE_END();

public:
	void testRandDist();
	void testZipfRand();
	void testZipfParams();
	void testZipfCreate();


protected:


private:

};

#endif // _NTSETEST_UTIL_TEST_H_
