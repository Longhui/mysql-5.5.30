/**
 * ²âÊÔTNTDatabase²Ù×÷
 *
 * @author ºÎµÇ³É
 */
#ifndef _NTSETEST_TNTTABLE_
#define _NTSETEST_TNTTABLE_

#include <cppunit/extensions/HelperMacros.h>
#include "api/TNTDatabase.h"
#include "misc/TNTControlFile.h"
#include "util/PagePool.h"
#include "misc/TNTIMPageManager.h"
#include "misc/MemCtx.h"
#include "misc/TableDef.h"
#include "api/TNTTable.h"
#include "api/TNTTblScan.h"
#include "rec/Records.h"
#include "rec/MRecords.h"
#include "btree/TNTIndex.h"
#include "btree/MIndex.h"
#include "misc/RecordHelper.h"

using namespace tnt;
using namespace ntse;

class TNTTableTestCase : public CPPUNIT_NS::TestFixture {
	CPPUNIT_TEST_SUITE(TNTTableTestCase);
	CPPUNIT_TEST(testCommon);
	CPPUNIT_TEST(testInsert);
	CPPUNIT_TEST(testUpdate);
	CPPUNIT_TEST(testDelete);
	CPPUNIT_TEST(testUpdDel);
	CPPUNIT_TEST(testReplace);
	CPPUNIT_TEST(testReplace2);
	CPPUNIT_TEST(testIndexUpdate);
	CPPUNIT_TEST(testIndexDelete);
	CPPUNIT_TEST(testIndexUpdDel);

	CPPUNIT_TEST(testTableScan);
	CPPUNIT_TEST(testIndexRangeScan);
	CPPUNIT_TEST(testIndexUniqueScan);
	CPPUNIT_TEST(testIndexRangeAfterUpd);
	CPPUNIT_TEST(testIndexRangeAfterDel);
	CPPUNIT_TEST(testIndexRangeAfterUD);
	CPPUNIT_TEST(testIndexRangeAfterMultiUpd);
	CPPUNIT_TEST(testIndexScanUncovered);
	CPPUNIT_TEST(testIndexUpdRollback);

	CPPUNIT_TEST(testPKFetch);
	CPPUNIT_TEST(testPosScan);

	CPPUNIT_TEST(testSnapScanNoRB);
	CPPUNIT_TEST(testSnapScanAfterUpd);
	CPPUNIT_TEST(testSnapScanAfterDel);
	CPPUNIT_TEST(testSnapScanAfterUD);
	CPPUNIT_TEST(testSnapScanAfterMultiUpd);

	CPPUNIT_TEST(testIndexScanAfterAdd);
	CPPUNIT_TEST(testAutoInc);
	CPPUNIT_TEST(testPurge);
	CPPUNIT_TEST(testLockRowConflict);
	CPPUNIT_TEST(testGetNextWithException);
	CPPUNIT_TEST(testLockOneRowTwice);
	
	CPPUNIT_TEST(testPurgeDeleteRowReuse);

	CPPUNIT_TEST_SUITE_END();

public:
	static const char* getName();
	static const char* getDescription();
	static bool isBig();
	void setUp();
	void tearDown();

	static const uint SMALL_LOB_SIZE;
	static const uint LARGE_LOB_SIZE;

private:
	void init();
	void clear();
	void testCommon();
	void testInsert();
	void testUpdate();
	void testDelete();
	void testUpdDel();
	void testReplace();
	void testReplace2();
	void testIndexUpdate();
	void testIndexDelete();
	void testIndexUpdDel();

	void testTableScan();
	void testIndexRangeScan();
	void testIndexUniqueScan();
	void testIndexRangeAfterUpd();
	void testIndexRangeAfterDel();
	void testIndexRangeAfterUD();
	void testIndexRangeAfterMultiUpd();
	void testIndexScanUncovered();
	void testIndexUpdRollback();

	void testPKFetch();
	void testPosScan();
	
	void testSnapScanNoRB();
	void testSnapScanAfterUpd();
	void testSnapScanAfterDel();
	void testSnapScanAfterUD();
	void testSnapScanAfterMultiUpd();

	void testIndexScanAfterAdd();

	void testAutoInc();

	void testPurge();
	void testPurgeDeleteRowReuse();

	void testLockRowConflict();
	void testGetNextWithException();
	void testLockOneRowTwice();

private:
	uint fetchBlogCount(Record **rows, uint numRows, OpType opType, bool tableScan);
	uint fetchBlogCountFromSecIndex(OpType opType);
	void fetchBlogCountOnDiffRV(bool tableScan = true);
	void doPKeyFetchTest(Record **rows, uint numRows);
	void doPosScanTest(Record **rows, uint numRows);
	void doMultiUpdAndScan(bool tableScan);

	Record** populateBlogCount(TNTDatabase *db, TNTTable *table, uint numRows);
	uint updDelBlogCount(TNTDatabase *db, TNTTable *table, OpType opType, bool tableScan, Record **rows, uint numRows);
	uint updDelBlogCount(TNTDatabase *db, TNTTable *table, OpType opType, Record **rows, uint numRows);
	TableDef* getBlogCountDef(bool useMms = false, bool onlineIdx = false);
	void verifyTable();
	void blogCountScanTest(bool useMms, OpType opType, bool tableScan);
	static int compareRecord(TNTTable *table, const byte *rec1, const byte *rec2, u16 numCols, const u16 *columns);
	static int compareRecord(const TableDef *tableDef, const byte *rec1, const byte *rec2, u16 numCols, const u16 *columns);

	void initTNTOpInfo(TLockMode lockMode);
	TNTTransaction* beginTransaction(OpType intention, u64 *trxIds = NULL, uint activeTrans = 0);
	void commitTransaction(TNTTransaction *trx);
	void rollbackTransaction(TNTTransaction *trx);
	void printSubRecord(SubRecord *subRec);

private:
	TNTDatabase	*m_db;
	TNTConfig	config;
	Session		*m_session;
	Connection	*m_conn;
	TableDef	*m_tableDef;
	TNTTable	*m_table;
	TNTOpInfo	m_opInfo;
};


class RowLockConflictThread: public Thread {
public:
	RowLockConflictThread(TNTDatabase *db, TNTTable *table, TableDef *tableDef);
	void run();
private:
	TNTTransaction* beginTransaction(OpType intention, u64 *trxIds = NULL, uint activeTrans = 0);
	void blogCountScanTest(OpType intention, bool tableScan) throw (NtseException);
	void commitTransaction();
	void rollbackTransaction();

	TNTDatabase			*m_db;
	TNTTable			*m_table;
	TableDef			*m_tableDef;
	TNTOpInfo			m_opInfo;
	Session				*m_session;
	Connection			*m_conn;
	TNTTransaction		*m_trx;
};


class PurgeTableThread: public Thread {
public:
	PurgeTableThread(TNTDatabase *db, TNTTable *table);
	void run();

private:
	TNTTransaction* beginTransaction(OpType intention, u64 *trxIds = NULL, uint activeTrans = 0);
	void commitTransaction();

	TNTDatabase			*m_db;
	TNTTable			*m_table;
	TNTOpInfo			m_opInfo;
	Session				*m_session;
	Connection			*m_conn;
	TNTTransaction		*m_trx;
};

#endif