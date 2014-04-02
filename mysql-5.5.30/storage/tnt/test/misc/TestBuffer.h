/**
 * ≤‚ ‘“≥√Êª∫¥Ê
 *
 * @author ÕÙ‘¥(wangyuan@corp.netease.com, wy@163.org)
 */

#ifndef _NTSETEST_BUFFER_H_
#define _NTSETEST_BUFFER_H_

#include <cppunit/extensions/HelperMacros.h>
#include "util/File.h"
#include "util/PagePool.h"
#include "misc/Buffer.h"

using namespace ntse;


class BufferTestCase: public CPPUNIT_NS::TestFixture {
	CPPUNIT_TEST_SUITE(BufferTestCase);
	CPPUNIT_TEST(testGet);
	CPPUNIT_TEST(testLock);
	CPPUNIT_TEST(testScavenger);
	CPPUNIT_TEST(testFreePages);
	CPPUNIT_TEST(testFlush);
	CPPUNIT_TEST(testPrefetch);
	CPPUNIT_TEST(testFreeSomePages);
	CPPUNIT_TEST(testLru);
	CPPUNIT_TEST(testScan);
	CPPUNIT_TEST(testBatchWrite);
	CPPUNIT_TEST_SUITE_END();

public:
	static const char* getName();
	static const char* getDescription();
	static bool isBig();
	void setUp();
	void tearDown();

protected:
	void testGet();
	void testLock();
	void testScavenger();
	void testFreePages();
	void testFlush();
	void testPrefetch();
	void testFreeSomePages();
	void testLru();
	void testScan();
	void testBatchWrite();

private:
	void init(uint bufferSize, uint numPages);
	void cleanUp();

	PagePool	*m_pool;
	uint	m_numPages;
	Buffer	*m_buffer;
	File	*m_file;
	Syslog	*m_syslog;
	DBObjStats *m_dbObjStats;
};

class BufferBigTest: public CPPUNIT_NS::TestFixture {
	CPPUNIT_TEST_SUITE(BufferBigTest);
	CPPUNIT_TEST(testBasicOperations);
	CPPUNIT_TEST(testReplacePolicy);
	CPPUNIT_TEST(testScanST);
	CPPUNIT_TEST(testMT);
	CPPUNIT_TEST(testLoadData);
	CPPUNIT_TEST_SUITE_END();

public:
	static const char* getName();
	static const char* getDescription();
	static bool isBig();
	void setUp();
	void tearDown();

protected:
	void testBasicOperations();
	void testReplacePolicy();
	void testScanST();
	void testMT();
	void testLoadData();

private:
	void init(uint bufferSize, uint numPages);
	void cleanUp();

	PagePool	*m_pool;
	uint	m_bufferSize;
	uint	m_numPages;
	Syslog	*m_syslog;
	Buffer	*m_buffer;
	DBObjStats *m_dbObjStats;
	File	*m_file;
};

#endif
