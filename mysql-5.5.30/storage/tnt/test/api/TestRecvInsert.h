/**
 * INSERT操作恢复流程测试
 *
 * @author 汪源(wangyuan@corp.netease.com, wy@163.org)
 */

#ifndef _NTSETEST_RECV_INSERT_H_
#define _NTSETEST_RECV_INSERT_H_

#include <cppunit/extensions/HelperMacros.h>
#include "api/Database.h"
#include "api/TestRecover.h"
#include <string>
#include <util/System.h>
#include <util/Sync.h>

using namespace std;
using namespace ntse;

class RecvInsertTestCase: public RecoverTestCase {
	CPPUNIT_TEST_SUITE(RecvInsertTestCase);
	CPPUNIT_TEST(testSucc);
	CPPUNIT_TEST(testDup);
	CPPUNIT_TEST(testDupCrashBeforeDelete);
	CPPUNIT_TEST(testCrashBeforeHeapInsert);
	CPPUNIT_TEST(testCrashBeforeIndex);
	CPPUNIT_TEST(testCrashBeforeUniqueIndex);
	CPPUNIT_TEST(testCrashAfterUniqueIndex);
	CPPUNIT_TEST(testCrashBeforeSecondIndex);
	CPPUNIT_TEST(testCrashAfterSecondIndex);
	CPPUNIT_TEST(testCrashDuringRecoverCrashBeforeSecondIndex);
	CPPUNIT_TEST(testCrashBeforeEnd);
	CPPUNIT_TEST(testLobSucc);
	CPPUNIT_TEST(testLobSuccNull);
	CPPUNIT_TEST(testLobDup);
	CPPUNIT_TEST(testLobDupNull);
	CPPUNIT_TEST(testLobCrashDuringLob);
	CPPUNIT_TEST(testLobCrashDuringLobNull);
	CPPUNIT_TEST(testLobCrashBeforeHeapInsert);
	CPPUNIT_TEST(testLobCrashBeforeIndex);
	CPPUNIT_TEST(testLobCrashDuringIndex);
	CPPUNIT_TEST(testNoIdx);
	CPPUNIT_TEST(testIdxSmo);
	CPPUNIT_TEST(testIOT);
	CPPUNIT_TEST(testIOTDup);
	CPPUNIT_TEST_SUITE_END();

public:
	static const char* getName();
	static const char* getDescription();
	static bool isBig();

protected:
	void testSucc();
	void testDup();
	void testDupCrashBeforeDelete();
	void testCrashBeforeHeapInsert();
	void testCrashBeforeIndex();
	void testCrashBeforeUniqueIndex();
	void testCrashAfterUniqueIndex();
	void testCrashBeforeSecondIndex();
	void testCrashAfterSecondIndex();
	void testCrashDuringRecoverCrashBeforeSecondIndex();
	void testCrashBeforeEnd();
	void testLobSucc();
	void testLobSuccNull();
	void testLobDup();
	void testLobDupNull();
	void testLobCrashDuringLob();
	void testLobCrashDuringLobNull();
	void testLobCrashBeforeHeapInsert();
	void testLobCrashBeforeIndex();
	void testLobCrashDuringIndex();
	void testNoIdx();
	void testIdxSmo();
	void testIOT();
	void testIOTDup();

protected:
	void doInsertRecvTest(LogType crashOnThisLog, int nthLog, bool inclusive, uint insertRows, 
		bool repeatInsert, uint succRows, bool recoverTwice = true);
	Record** doRecvInsertTestPhase1(uint insertRows, bool repeatInsert, LogType crashOnThisLog,
		int nthLog, bool inclusive, uint succRows);
	void doInsertLobRecvTest(LogType crashOnThisLog, int nthLog, uint insertRows, 
		bool repeatInsert, uint succRows, bool lobNotNull, bool recoverTwice = true);
};

#endif

