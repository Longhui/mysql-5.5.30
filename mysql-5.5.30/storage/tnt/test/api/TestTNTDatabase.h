/**
 * 测试TNTDatabase操作
 *
 * @author 何登成
 */
#ifndef _NTSETEST_TNTDATABASE_
#define _NTSETEST_TNTDATABASE_

#include <cppunit/extensions/HelperMacros.h>
#include "api/TNTDatabase.h"
#include "misc/TNTControlFile.h"
#include "util/PagePool.h"
#include "misc/TNTIMPageManager.h"
#include "misc/MemCtx.h"
#include "misc/TableDef.h"
#include "rec/Records.h"
#include "misc/RecordHelper.h"

using namespace tnt;
using namespace ntse;

class TNTDatabaseTestCase : public CPPUNIT_NS::TestFixture {
	CPPUNIT_TEST_SUITE(TNTDatabaseTestCase);
	CPPUNIT_TEST(testCommon);
	CPPUNIT_TEST(testOpenWithoutClose);
	CPPUNIT_TEST(testPinTableIfOpen);
	CPPUNIT_TEST(testOpenSystemTable);
	CPPUNIT_TEST(testAddDropIndex);
	CPPUNIT_TEST(testSerialDDL);
	CPPUNIT_TEST(testDropTableFailed);
	CPPUNIT_TEST(testRenameTableFailed);
	CPPUNIT_TEST(testXaTrans);
	CPPUNIT_TEST(testPartialRollback);
	CPPUNIT_TEST(testClose);
	CPPUNIT_TEST(testCleanClose);
	CPPUNIT_TEST(testPurgeDumpDefrag);
	CPPUNIT_TEST(testOpen);
	CPPUNIT_TEST(testSwitchVersionPool);
	CPPUNIT_TEST(testMultiThreadWithDropDDL);
	CPPUNIT_TEST(testMultiThreadWithRenameDDL);
	CPPUNIT_TEST(testTruncateCurrentOpenTable);
	CPPUNIT_TEST(testTableOpenCache);
	CPPUNIT_TEST(testReclaimMemIndex);
	CPPUNIT_TEST(testReportStatus);
	CPPUNIT_TEST_SUITE_END();

public:
	static const char* getName();
	static const char* getDescription();
	static bool isBig();
	void setUp();
	void tearDown();

private:
	void init();
	void clear();
	void testCommon();

	bool createBlogCount();
	Record** populateBlogCount(TNTDatabase *db, TNTTable *table, uint numRows, uint insFirstKey = 1);
	uint updDelBlogCount(TNTDatabase *db, TNTTable *table, OpType opType, Record **rows, uint numRows);
	void blogCountScanTest(OpType intention, bool tableScan, uint numRows);
	void blogCountScanAndVerify(Record **rows, uint numRows, OpType opType, bool tableScan, uint searchKey, bool beForward = true);
	void printSubRecord(SubRecord *subRec);
	static int compareRecord(TNTTable *table, const byte *rec1, const byte *rec2, u16 numCols, const u16 *columns);
	static int compareRecord(const TableDef *tableDef, const byte *rec1, const byte *rec2, u16 numCols, const u16 *columns);

	void openSystemTable() throw (NtseException);
	void openBlogCount() throw (NtseException);
	void pinBlogCount();
	void closeBlogCount();
	bool dropBlogCount();
	void renameBlogCount();
	void truncateBlogCount();
	void ddlOpenFailed(uint ddlOpType);

	bool createTestTable(char *tableName, vector<TableDef*> *vector);
	void openTestTable(char *tableName, vector<TNTTable*> *vector) throw (NtseException);
	void closeTestTable(TNTTable *table);

	// ----------test cases------------
	void testOpenWithoutClose();
	void testPinTableIfOpen();
	void testOpenSystemTable();
	void testAddDropIndex();
	void testSerialDDL();
	void testClose();
	void testCleanClose();
	void testDropTableFailed();
	void testRenameTableFailed();

	// add transaction test from database call
	void testXaTrans();
	void testPartialRollback();
	void testPurgeDumpDefrag();
	void testOpen();

	// add switch version pool test
	void testSwitchVersionPool();

	// 测试多线程下，DDL并发情况
	void testMultiThreadWithDropDDL();
	void testMultiThreadWithRenameDDL();
	void testTruncateCurrentOpenTable();

	// 测试table open cache 的功能
	void testTableOpenCache();

	// 测试内存索引回收功能
	void testReclaimMemIndex();

	// 测试reportSTatus
	void testReportStatus();

private:
	TableDef* getBlogCountDef(bool useMms = false, bool onlineIdx = false);
	TableDef* getTestTableDef(char *tableName);
	TNTTransaction* beginTransaction(OpType intention, u64 *trxIds = NULL, uint activeTrans = 0);
	void commitTransaction();
	void rollbackTransaction();

private:
	TNTDatabase	*m_db;
	TNTConfig	m_config;
	Session		*m_session;
	Connection	*m_conn;
	TableDef	*m_tableDef;
	TNTTable	*m_table;
	TNTTransaction *m_trx;
};

#define RENAME_OP	0
#define DROP_OP		1

class DDLThread: public Thread {
public:
	DDLThread(TNTDatabase *db, uint ddlOp);
	void run();
private:
	void beginTransaction();
	void commitTransaction();
	void rollbackTransaction();
private:
	TNTDatabase			*m_db;
	Session				*m_session;
	Connection			*m_conn;
	TNTTransaction		*m_trx;
	uint				m_ddlOp;
};

class TruncateThread: public Thread {
public:
	TruncateThread(TNTDatabase *db);
	void run();
private:
	void openBlogCount();
	void closeBlogCount();

	void beginTransaction();
	void commitTransaction();
	void rollbackTransaction();
private:
	TNTDatabase			*m_db;
	Session				*m_session;
	Connection			*m_conn;
	TNTTransaction		*m_trx;
	TNTTable			*m_table;
};

#endif