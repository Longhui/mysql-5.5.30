/**
 * 测试各类哈希表实现
 * 
 * @author 汪源(wangyuan@corp.netease.com, wy@163.org)
 */

#ifndef _NTSETEST_HASH_H_
#define _NTSETEST_HASH_H_

#include <cppunit/extensions/HelperMacros.h>

class HashTestCase: public CPPUNIT_NS::TestFixture {
	CPPUNIT_TEST_SUITE(HashTestCase);
	CPPUNIT_TEST(testDynHash);
	CPPUNIT_TEST(testHash);
	CPPUNIT_TEST_SUITE_END();

public:
	static const char* getName();
	static const char* getDescription();
	static bool isBig();

protected:
	void testDynHash();
	void testHash();
};

class HashBigTest: public CPPUNIT_NS::TestFixture {
	CPPUNIT_TEST_SUITE(HashBigTest);
	CPPUNIT_TEST(testDynHash);
	CPPUNIT_TEST(testHash);
	CPPUNIT_TEST_SUITE_END();

public:
	static const char* getName();
	static const char* getDescription();
	static bool isBig();

protected:
	void testDynHash();
	void testHash();
};

#endif

