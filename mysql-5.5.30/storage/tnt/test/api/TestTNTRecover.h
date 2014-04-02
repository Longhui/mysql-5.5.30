/**
 * ²âÊÔTNTDatabase recover²Ù×÷
 *
 * @author xindingfeng
 */
#ifndef _NTSETEST_TNTRECOVER_
#define _NTSETEST_TNTRECOVER_

#include <cppunit/extensions/HelperMacros.h>
#include "api/TNTDatabase.h"
#include "api/TNTTblScan.h"

using namespace tnt;
using namespace ntse;

struct RecordVal;

class TNTRecoverTestCase : public CPPUNIT_NS::TestFixture {
	CPPUNIT_TEST_SUITE(TNTRecoverTestCase);
//	CPPUNIT_TEST(testDump1);
//	CPPUNIT_TEST(testDump2);
//	CPPUNIT_TEST(testDumpCompensateRecModify);
	CPPUNIT_TEST(testPurgeCompensateRecModify);
	CPPUNIT_TEST(testPurge);
	CPPUNIT_TEST(testPurgeWaitActiveTrx);
	CPPUNIT_TEST(testRecoverIUD);
	CPPUNIT_TEST(testPrepareRecover);
	CPPUNIT_TEST(testRollBack);
	CPPUNIT_TEST(testRedoPurgePhrase1);
	CPPUNIT_TEST(testRedoPurgePhrase2);
	CPPUNIT_TEST(testRedoBeginPurge);
	CPPUNIT_TEST(testRedoEndPurge);
	CPPUNIT_TEST(testRedoPurgeEndHeap);
//	CPPUNIT_TEST(testPermissionConflict);
	CPPUNIT_TEST(testDefrag);
	CPPUNIT_TEST(testPurgeNoDump);
//	CPPUNIT_TEST(testConcurrentTrxDump);
	CPPUNIT_TEST(testPurgeRecover);
	CPPUNIT_TEST(testRecoverTblDrop);
//	CPPUNIT_TEST(testDumpUpdate);
	CPPUNIT_TEST(testRollBackDefrag);
	CPPUNIT_TEST(testDefragHashIndex);
	CPPUNIT_TEST(testBug34);
	CPPUNIT_TEST_SUITE_END();

public:
	static const char* getName();
	static const char* getDescription();
	static bool isBig();
	void setUp();
	void tearDown();

	static SubRecord *createSubRecord(TableDef *tableDef, const std::vector<u16> &columns, const std::vector<void *>&datas);
	static void deepCopyRecord(Record *dest, Record *src);
	static void compareRecord(Session *session, TNTTable *table, RecordVal *recVal);
	static void compareNtseRecord(Session *session, Table *table, RecordVal *recVal, bool exist);
	static TNTTransaction *startTrx(TNTTrxSys *trxSys, Connection *conn, TLockMode lockMode = TL_NO, bool log = false);
	static void commitTrx(TNTTrxSys *trxSys, TNTTransaction *trx);
	static void rollBackTrx(TNTTrxSys *trxSys, TNTTransaction *trx);
	static void initTNTOpInfo(TNTOpInfo &opInfo, TLockMode mode) {
		opInfo.m_selLockType = mode;
		opInfo.m_sqlStatStart = true;
		opInfo.m_mysqlHasLocked = false;
		opInfo.m_mysqlOper = true;
	}

	static void update(Session *session, TNTTable *table, RecordVal *recs, const u32 begin, const u32 end, u8 factor, bool save);
	static void remove(Session *session, TNTTable *table, RecordVal *recs, const u32 begin, const u32 end, bool save);

protected:
	void testDump1();
	void testDump2();
	void testDumpCompensateRecModify();
	void testPurgeCompensateRecModify();
	void testPurge();
	void testPurgeWaitActiveTrx();
	void testRecoverIUD();
	void testPrepareRecover();
	void testRollBack();
	void testRedoPurgePhrase1();
	void testRedoPurgePhrase2();
	void testRedoBeginPurge();
	void testRedoEndPurge();
	void testRedoPurgeEndHeap();
//	void testPermissionConflict();
	void testDefrag();
	void testPurgeNoDump();
	void testConcurrentTrxDump();
	void testPurgeRecover();
	void testRecoverTblDrop();
	void testDumpUpdate();
	void testRollBackDefrag();
	void testDefragHashIndex();
	void testBug34();

private:
	TableDef *createStudentTableDef();
	void updateAll(RecordVal *recs, const u32 count, u8 factor, bool save, bool log);
	void insert(RecordVal *recs, const u32 count, bool log);
	void update(RecordVal *recs, const u32 begin, const u32 end, u8 factor, bool save, bool log);
	void remove(RecordVal *recs, const u32 begin, const u32 end, bool save, bool log);
	void insert(Session *session, RecordVal *recs, const u32 count);
	void update(Session *session, RecordVal *recs, const u32 begin, const u32 end, u8 factor, bool save);
	void remove(Session *session, RecordVal *recs, const u32 begin, const u32 end, bool save);
	Record* createStudentRec(u32 stuNo, const char *name, u16 age, u32 clsNo, float gpa);

	void reOpenDb(bool flushLog, bool flushData);
	void compareAllRecord(RecordVal *recs, u32 count);
	void compareAllNtseRecord(RecordVal *recs, u32 count);

	void setAllDelete(RecordVal *recs, u32 count);
	void clearAllDelete(RecordVal *recs, u32 count);
	RecordVal *copyAllRecordVal(RecordVal *recs, u32 count);
	void releaseAllRecordVal(RecordVal *recs, u32 count);
private:
	TNTDatabase	*m_db;
	Session		*m_session;
	Connection	*m_conn;
	TNTTable	*m_table;
	TNTConfig   m_config;
};
#endif