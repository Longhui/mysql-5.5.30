/**
 * UPDATE≤‚ ‘ª÷∏¥¡˜≥Ã≤‚ ‘
 *
 * @author ÕÙ‘¥(wangyuan@corp.netease.com, wy@163.org)
 */

#ifndef _NTSETEST_RECV_UPDATE_H_
#define _NTSETEST_RECV_UPDATE_H_

#include <cppunit/extensions/HelperMacros.h>
#include "api/Database.h"
#include "api/TestRecover.h"
#include <string>
#include <util/System.h>
#include <util/Sync.h>

using namespace std;
using namespace ntse;

class RecvUpdateTestCase: public RecoverTestCase {
	CPPUNIT_TEST_SUITE(RecvUpdateTestCase);
	CPPUNIT_TEST(testSuccHeap);
	CPPUNIT_TEST(testSuccMms);
	CPPUNIT_TEST(testSuccHeapHasDictionary);
	CPPUNIT_TEST(testSuccMmsHasDictionary);
	CPPUNIT_TEST(testCrashAfterBegin);
	CPPUNIT_TEST(testCrashAfterPreUpdate);
	CPPUNIT_TEST(testCrashBeforeEnd);
	CPPUNIT_TEST(testCrashBeforeHeap);
	CPPUNIT_TEST(testDoubleUpdateMmsCrash);
	CPPUNIT_TEST(testLobReverseSizeSucc);
	CPPUNIT_TEST(testLobKeepSizeSucc);
	CPPUNIT_TEST(testLobDup);
	CPPUNIT_TEST(testLobCrashDuringLob);
	CPPUNIT_TEST(testLobCrashBeforeInsertLarge);
	CPPUNIT_TEST(testLobCrashDuringIndex);
	CPPUNIT_TEST(testLobToNull);
	CPPUNIT_TEST(testLobToNullCrash);
	CPPUNIT_TEST(testLobFromNull);
	CPPUNIT_TEST(testLobFromNullCrash);
	CPPUNIT_TEST(testIdxOnly);
	CPPUNIT_TEST(testIdxOnlyCrash);
	CPPUNIT_TEST(testLobOnly);
	CPPUNIT_TEST(testLobOnlyCrash);
	CPPUNIT_TEST(testLargeToUncompress);
	CPPUNIT_TEST(testSmallToUncompress);
	CPPUNIT_TEST(testMmsToUncompress);
	CPPUNIT_TEST(testNoIdx);
	CPPUNIT_TEST(testCrashBeforeUniqueIndex);
	CPPUNIT_TEST(testCrashAfterUniqueIndex);
	CPPUNIT_TEST(testCrashBeforeDelInUpdate);
	CPPUNIT_TEST(testUpdateLobKeepNull);
	CPPUNIT_TEST(testMmsUpdateHeap);
	CPPUNIT_TEST(testMmsFlushLog);
	CPPUNIT_TEST(testIOT);
	CPPUNIT_TEST(testIOTVarlen);
	CPPUNIT_TEST_SUITE_END();

public:
	static const char* getName();
	static const char* getDescription();
	static bool isBig();

protected:
	void testSuccHeap();
	void testSuccMms();
	void testSuccHeapHasDictionary();
	void testSuccMmsHasDictionary();
	void testCrashAfterBegin();
	void testCrashAfterPreUpdate();
	void testCrashBeforeEnd();
	void testCrashBeforeHeap();
	void testDoubleUpdateMmsCrash();
	void testLobReverseSizeSucc();
	void testLobKeepSizeSucc();
	void testLobDup();
	void testLobCrashDuringLob();
	void testLobCrashBeforeInsertLarge();
	void testLobCrashDuringIndex();
	void testLobToNull();
	void testLobToNullCrash();
	void testLobFromNull();
	void testLobFromNullCrash();
	void testIdxOnly();
	void testIdxOnlyCrash();
	void testLobOnly();
	void testLobOnlyCrash();
	void testLargeToCompress();
	void testLargeToUncompress();
	void testSmallToCompress();
	void testSmallToUncompress();
	void testMmsToCompress();
	void testMmsToUncompress();
	void testNoIdx();
	void testCrashBeforeUniqueIndex();
	void testCrashAfterUniqueIndex();
	void testCrashBeforeDelInUpdate();
	void testUpdateLobKeepNull();
	void testMmsUpdateHeap();
	void testMmsFlushLog();
	void testIOT();
	void testIOTVarlen();

protected:
	static const int UPDATE_REVERSE_SMALL_LARGE = 1;
	static const int UPDATE_KEEP_SMALL_LARGE = 2;
	static const int UPDATE_TO_NULL = 3;
	static const int UPDATE_FROM_NULL = 4;
	static const int UPDATE_DUP = 5;
	static const int UPDATE_IDX_ONLY = 6;
	static const int UPDATE_LOB_ONLY = 7;

	void doUpdateRecvTest(LogType crashOnThisLog, int nthLog, uint insertRows, 
		uint succRows, bool recoverTwice = true, bool hitMms = true, bool createDict = false);
	void doUpdateLobRecvTest(LogType crashOnThisLog, int nthLog, uint insertRows, 
		uint succRows, bool recoverTwice, int howToUpdate);
	void doReverseCompressRecvTest(bool useMms, bool oldIsCompressed, uint size);
	void doUniqueIndexTest(bool beforeUnique, bool beforeDiu, bool doubleNonUnique, bool noUnique);
};

#endif

