/**
 * 测试表管理操作
 *
 * @author 汪源(wangyuan@corp.netease.com, wy@163.org)
 */

#ifndef _NTSETEST_TABLE_H_
#define _NTSETEST_TABLE_H_

#include <cppunit/extensions/HelperMacros.h>
#include "api/Table.h"
#include "api/Database.h"

using namespace ntse;

class TableTestCase: public CPPUNIT_NS::TestFixture {
	CPPUNIT_TEST_SUITE(TableTestCase);
	CPPUNIT_TEST(testBasicDDL);
	CPPUNIT_TEST(testInsert);
	CPPUNIT_TEST(testTableScanNonMms);
	CPPUNIT_TEST(testTableScanMms);
	CPPUNIT_TEST(testTableScanUpdateNonMms);
	CPPUNIT_TEST(testTableScanUpdateMms);
	CPPUNIT_TEST(testTableScanDeleteNonMms);
	CPPUNIT_TEST(testTableScanDeleteMms);
	CPPUNIT_TEST(testIndexScanNonMms);
	CPPUNIT_TEST(testIndexScanMms);
	CPPUNIT_TEST(testIndexScanUpdateNonMms);
	CPPUNIT_TEST(testIndexScanUpdateMms);
	CPPUNIT_TEST(testIndexScanDeleteNonMms);
	CPPUNIT_TEST(testIndexScanDeleteMms);
	CPPUNIT_TEST(testLob);
	CPPUNIT_TEST(testPKeyFetchNonMms);
	CPPUNIT_TEST(testPKeyFetchMms);
	CPPUNIT_TEST(testSingleFetch);
	CPPUNIT_TEST(testPosScanNonMms);
	CPPUNIT_TEST(testPosScanMms);
	CPPUNIT_TEST(testPosScanMmsChange);
	CPPUNIT_TEST(testReplace);
	CPPUNIT_TEST(testCreateFail);
	CPPUNIT_TEST(testOpenFail);
	CPPUNIT_TEST(testRename);
	CPPUNIT_TEST(testTruncate);
	CPPUNIT_TEST(testAddIndex);
	CPPUNIT_TEST(testOnlineAddIndex);
	CPPUNIT_TEST(testDropIndex);
	CPPUNIT_TEST(testDropPKeyWhenCacheUpdate);
	CPPUNIT_TEST(testNull);
	CPPUNIT_TEST(testDdlLock);
	CPPUNIT_TEST(testUpdatePKeyMatchDirtyMms);
	CPPUNIT_TEST(testDeleteAll);
	CPPUNIT_TEST(bug469);
	CPPUNIT_TEST(testUpdateCache);
	CPPUNIT_TEST(testNoPKey);
	CPPUNIT_TEST(testLongRecord);
	CPPUNIT_TEST(testUpdatePutMmsFail);
	CPPUNIT_TEST(testOptimize);
	CPPUNIT_TEST(testCoverageIndexMmsUpdate);
	CPPUNIT_TEST(testMmsCheckpoint);
	CPPUNIT_TEST(testUpdateDelete);
	CPPUNIT_TEST(testCreateDictionary);
	CPPUNIT_TEST(testCompressedLongRecordDML);
	CPPUNIT_TEST(testUpdateLobInCompressTable);
	CPPUNIT_TEST_SUITE_END();

public:
	static const char* getName();
	static const char* getDescription();
	static bool isBig();
	void setUp();
	void tearDown();

protected:
	void testBasicDDL();
	void testInsert();
	void testTableScanNonMms();
	void testTableScanMms();
	void testTableScanUpdateNonMms();
	void testTableScanUpdateMms();
	void testTableScanDeleteNonMms();
	void testTableScanDeleteMms();
	void testIndexScanNonMms();
	void testIndexScanMms();
	void testIndexScanUpdateNonMms();
	void testIndexScanUpdateMms();
	void testIndexScanDeleteNonMms();
	void testIndexScanDeleteMms();
	void testLob();
	void testPKeyFetchNonMms();
	void testPKeyFetchMms();
	void testSingleFetch();
	void testPosScanNonMms();
	void testPosScanMms();
	void testPosScanMmsChange();
	void testReplace();
	void testCreateFail();
	void testOpenFail();
	void testRename();
	void testTruncate();
	void testAddIndex();
	void testOnlineAddIndex();
	void testDropIndex();
	void testDropPKeyWhenCacheUpdate();
	void testNull();
	void testDdlLock();
	void testUpdatePKeyMatchDirtyMms();
	void testDeleteAll();
	void bug469();
	void testUpdateCache();
	void testNoPKey();
	void testLongRecord();
	void testUpdatePutMmsFail();
	void testOptimize();
	void testCoverageIndexMmsUpdate();
	void testMmsCheckpoint();
	void testUpdateDelete();
	void testCreateDictionary();
	void testCompressedLongRecordDML();
	void testUpdateLobInCompressTable();

public:
	static const uint SMALL_LOB_SIZE;
	static const uint LARGE_LOB_SIZE;
	
	static TableDef* getBlogCountDef(bool useMms, bool onlineIdx = false);
	static void createBlogCount(Database *db, bool useMms = true);
	static Record** populateBlogCount(Database *db, Table *table, uint numRows);
	static void populateMmsBlogCount(Database *db, Table *table, uint numRows);
	static void createBlog(Database *db, bool useMms = true);
	static TableDef* getBlogDef(bool useMms, bool onlineIdx = false);
	static Record** populateBlog(Database *db, Table *table, uint numRows, bool doubleInsert, bool lobNotNull, u64 idstart = 1);
	static int compareRecord(Table *table, const byte *rec1, const byte *rec2, u16 numCols, const u16 *columns, RecFormat recFormat = REC_REDUNDANT);
	static int compareRecord(const TableDef *tableDef, const byte *rec1, const byte *rec2, u16 numCols, const u16 *columns);
	static int compareUppMysqlRecord(const TableDef *tableDef, const byte *rec1, const byte *rec2, u16 numCols, const u16 *columns);
	static void verifyTable(Database *db, Table *table);

private:
	Table* openTable(const char *path);
	void closeTable(Table *table);
	void blogCountScanTest(bool useMms, OpType opType, bool tableScan);
	void doPKeyFetchTest(bool useMms);
	void doPosScanTest(bool useMms);
	
	Database	*m_db;
	Config		m_config;
	Table		*m_table;
};

class TblInterface;
class TableBigTest: public CPPUNIT_NS::TestFixture {
	CPPUNIT_TEST_SUITE(TableBigTest);
	CPPUNIT_TEST(testBuzz);
	CPPUNIT_TEST(testPKeySearch);
	CPPUNIT_TEST(testMmsSearch);
	CPPUNIT_TEST_SUITE_END();

public:
	static const char* getName();
	static const char* getDescription();
	static bool isBig();
	void setUp();
	void tearDown();

protected:
	void testBuzz();
	void testPKeySearch();
	void testMmsSearch();

	Database	*m_db;
	Config		m_config;
	TblInterface	*m_table;
};
#endif
