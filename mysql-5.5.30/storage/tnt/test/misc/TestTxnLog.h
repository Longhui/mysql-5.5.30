#ifndef _NTSETEST_TXNLOG_H_
#define _NTSETEST_TXNLOG_H_

#include <cppunit/extensions/HelperMacros.h>
#include "api/Database.h"
#include "misc/Txnlog.h"

using namespace ntse;

class TxnLogTestCase: public CPPUNIT_NS::TestFixture {
	CPPUNIT_TEST_SUITE(TxnLogTestCase);
	CPPUNIT_TEST(testCreateDrop);
	CPPUNIT_TEST(testOneLogRecord);
	CPPUNIT_TEST(testMultiLogRecord);
	CPPUNIT_TEST(testSwitchLogFile);
	CPPUNIT_TEST(testCpstLog);
	CPPUNIT_TEST(testLogtailAtFileEnd);
	CPPUNIT_TEST(testCkptAtFileEnd);
	CPPUNIT_TEST(testReclaimOverflowSpace);
	CPPUNIT_TEST(testRecoverdLogtailAtFileEnd);
	CPPUNIT_TEST(testIncompleteLogEntry);
	CPPUNIT_TEST(testTruncate);
	CPPUNIT_TEST(testChangeLogfileSize);
	CPPUNIT_TEST(testSetOnlineLsn);
	CPPUNIT_TEST(testLimits);
	CPPUNIT_TEST_SUITE_END();

public:
	static const char* getName();
	static const char* getDescription();
	static bool isBig();
	void setUp();
	void tearDown();

protected:
	void init();
	void destroy();
	void mount();
	void unmount();
	void testCreateDrop();
	void testOneLogRecord();
	void testMultiLogRecord();
	void testSwitchLogFile();
	void testCpstLog();
	void testLogtailAtFileEnd();
	void testCkptAtFileEnd();
	void testReclaimOverflowSpace();
	void testRecoverdLogtailAtFileEnd();
	void testIncompleteLogEntry();
	void testChangeLogfileSize();
	void testSetOnlineLsn();
	void testLimits();

	void testTruncate();

	void writeLogAndVerify(uint recCnt, size_t logRecSize);
	void doTestSwitchLogFile(u16 recCnt, size_t logSize);
	void scanVerify(const vector<LsnType> lsnVec, const LogEntry *le, LsnType startLsn = Txnlog::MIN_LSN, LsnType endLsn = Txnlog::MAX_LSN);
	void fillLogFile(vector<LsnType> &lsnVec, const LogEntry *le, uint fileCnt);
	void doTestIncompleteLogEntry(bool isLong, int truncType);
private:
	Database *m_db;
	Config m_config;
	Txnlog *m_txnLog;
};

class TxnLogBigTest: public CPPUNIT_NS::TestFixture {
	CPPUNIT_TEST_SUITE(TxnLogBigTest);
	CPPUNIT_TEST(testLogOpers);
	CPPUNIT_TEST(testMT);
	CPPUNIT_TEST(testStability);
	CPPUNIT_TEST(testWritePerformance);
	CPPUNIT_TEST(testTraceVerify);
	CPPUNIT_TEST_SUITE_END();

public:
	static const char* getName();
	static const char* getDescription();
	static bool isBig();
	void setUp();
	void tearDown();

protected:
	void init();
	void destroy();
	void mount();
	void unmount();

	void testLogOpers();
	void testMT();
	void testStability();
	void testTraceVerify();
	void testWritePerformance();
	void doTestWritePerformance(size_t logEntrySize, int loopCnt, int threadCnt);
private:
	Database *m_db;
	Config m_config;
	Txnlog *m_txnLog;
};

#endif // _NTSETEST_TXNLOG_H_

