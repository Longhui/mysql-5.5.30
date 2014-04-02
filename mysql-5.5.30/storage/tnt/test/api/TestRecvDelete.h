/**
 * DELETE操作恢复流程测试
 *
 * @author 汪源(wangyuan@corp.netease.com, wy@163.org)
 */

#ifndef _NTSETEST_RECV_DELETE_H_
#define _NTSETEST_RECV_DELETE_H_

#include <cppunit/extensions/HelperMacros.h>
#include "api/Database.h"
#include "api/TestRecover.h"
#include <string>
#include <util/System.h>
#include <util/Sync.h>

using namespace std;
using namespace ntse;

class RecvDeleteTestCase: public RecoverTestCase {
	CPPUNIT_TEST_SUITE(RecvDeleteTestCase);
	CPPUNIT_TEST(testSucc);
	CPPUNIT_TEST(testCrashAfterBegin);
	CPPUNIT_TEST(testCrashBeforeIndex);
	CPPUNIT_TEST(testCrashBeforeUniqueIndex);
	CPPUNIT_TEST(testCrashAfterUniqueIndex);
	CPPUNIT_TEST(testCrashBeforeHeap);
	CPPUNIT_TEST(testLobSucc);
	CPPUNIT_TEST(testLobSuccNull);
	CPPUNIT_TEST(testLobCrashDuringIndex);
	CPPUNIT_TEST(testLobCrashBeforeDeleteLob);
	CPPUNIT_TEST(testLobCrashBeforeDeleteHeap);
	CPPUNIT_TEST(testDeleteHitMms);
	CPPUNIT_TEST(testDeleteLobHitMms);
	CPPUNIT_TEST(testNoIdx);
	CPPUNIT_TEST(testNoIdxLob);
	CPPUNIT_TEST(testIOT);
	CPPUNIT_TEST(testIOTVarlen);
	CPPUNIT_TEST_SUITE_END();

public:
	static const char* getName();
	static const char* getDescription();
	static bool isBig();

protected:
	void testSucc();
	void testCrashAfterBegin();
	void testCrashBeforeIndex();
	void testCrashBeforeUniqueIndex();
	void testCrashAfterUniqueIndex();
	void testCrashBeforeHeap();
	void testLobSucc();
	void testLobSuccNull();
	void testLobCrashDuringIndex();
	void testLobCrashBeforeDeleteLob();
	void testLobCrashBeforeDeleteHeap();
	void testDeleteHitMms();
	void testDeleteLobHitMms();
	void testNoIdx();
	void testNoIdxLob();
	void testIOT();
	void testIOTVarlen();

protected:
	void doDeleteRecvTest(LogType crashOnThisLog, int nthLog, uint insertRows, 
		uint succRows, bool recoverTwice = true);
	void doDeleteLobRecvTest(LogType crashOnThisLog, int nthLog, uint insertRows, 
		uint succRows, bool lobNotNull, bool recoverTwice = true);
	void deleteAllRecords(Session *session, const char *pkeyCol);
};

#endif

