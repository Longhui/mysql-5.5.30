/**
 * 测试文件操作
 * 
 * @author 汪源(wangyuan@corp.netease.com, wy@163.org)
 */

#ifndef _NTSETEST_FILE_H_
#define _NTSETEST_FILE_H_

#include <cppunit/extensions/HelperMacros.h>
#include "util/File.h"

using namespace ntse;

class FileTestCase: public CPPUNIT_NS::TestFixture {
	CPPUNIT_TEST_SUITE(FileTestCase);
	CPPUNIT_TEST(testBasic);
	CPPUNIT_TEST(testAutoDelete);
	CPPUNIT_TEST(testRename);
	CPPUNIT_TEST(testSetSize);
	CPPUNIT_TEST(testReadWrite);
	CPPUNIT_TEST(testDir);
	CPPUNIT_TEST(testCopy);
	CPPUNIT_TEST(testCopyDir);
	CPPUNIT_TEST(testAioWrite);
	CPPUNIT_TEST_SUITE_END();

public:
	static const char* getName();
	static const char* getDescription();
	static bool isBig();
	void setUp();
	void tearDown();

protected:
	void testBasic();
	void testAutoDelete();
	void testRename();
	void testSetSize();
	void testReadWrite();
	void testDir();
	void testCopy();
	void testCopyDir();
	void testAioWrite();
private:
	void doReadWriteTest(File *file, char *buf1, char *buf2);
};

class FileBigTest: public CPPUNIT_NS::TestFixture {
	CPPUNIT_TEST_SUITE(FileBigTest);
	CPPUNIT_TEST(testSeq);
	CPPUNIT_TEST(testRandom);
	CPPUNIT_TEST_SUITE_END();

public:
	static const char* getName();
	static const char* getDescription();
	static bool isBig();
	void setUp();

protected:
	void testSeq();
	void testRandom();
};

#endif

