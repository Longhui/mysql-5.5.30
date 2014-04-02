/**
 * 在线维护功能单元测试
 *
 * @author 谢可(ken@163.org)
 */

#ifndef _NTSETEST_TBLMNT_H_
#define _NTSETEST_TBLMNT_H_

#include <cppunit/extensions/HelperMacros.h>
#include "api/Database.h"
#include "util/Thread.h"
#include "misc/Config.h"
#include "api/TblMnt.h"
#include <vector>

using namespace std;
using namespace ntse;

//class Database;
//class Table;

class TblMntTestCase: public CPPUNIT_NS::TestFixture {
	CPPUNIT_TEST_SUITE(TblMntTestCase);
	CPPUNIT_TEST(testLogReplay);
	CPPUNIT_TEST(testAlterIndex);
	CPPUNIT_TEST(testAlterIndexFLR);
	CPPUNIT_TEST(testAlterIndexCompressTbl);
	CPPUNIT_TEST(testAlterIndexException);
	CPPUNIT_TEST(testRedoAlterIndex);
	CPPUNIT_TEST(testNtseRidMapping);
	CPPUNIT_TEST(testOptimizeTable);
	CPPUNIT_TEST(testRedoAlterColumn);
	CPPUNIT_TEST(testAlterColumn);
	CPPUNIT_TEST(testAlterColumnFLR);
	CPPUNIT_TEST(testRedoAlterColumnDelLob);
	CPPUNIT_TEST(testAlterColumnException);
	CPPUNIT_TEST_SUITE_END();

public:
	static const char* getName();
	static const char* getDescription();
	static bool isBig();
	void setUp();
	void tearDown();
protected:
	void testLogReplay();
	void testAlterIndex();
	void testAlterIndexFLR();
	void testAlterIndexCompressTbl();
	void testAlterIndexException();
	void testOptimizeTable();
	void testRedoAlterIndex();
	void testRedoAlterIndexFLR();
	void testNtseRidMapping();
	void testRedoAlterColumn();
	void testRedoAlterColumnDelLob();
	void testAlterColumn();
	void testAlterColumnException();
	void testAlterColumnFLR();
private:
	Database	*m_db;
	Table		*m_blogTable;
	Table		*m_blogCntTable;
	Config		m_config;
	void createBlogTable(bool usemms, bool cacheUpdate, Database *db = NULL);
	void createBlogCountTable(bool usemms, bool cacheUpdate, Database *db = NULL);

	void closeTable(Table *table, bool flushDirty, Database *db = NULL);
	Table* openTable(const char * path, Database *db = NULL);

public:
	static uint massInsertBlogCount(Database *db, Table* table, uint numRows, u64 startBlogID = 0, u64 stepBlogID = 1);
	static uint massInsertBlog(Database *db, Table* table, uint numRows, u64 startID = 0, u64 stepID = 1);
	static void scanDelete(Database *db, Table* table, uint percent);
	static void scanUpdateBlog(Database *db, Table *table, uint percent, uint priupdratio);

	static uint recCountBlog(Database *db, Table *table);
	static uint recCountBlogCount(Database *db, Table *table);
};


/**
* 数据校验类
*/
class DataChecker {
public:
	static bool checkIndiceWithTable(Session *sessiontbl, Database *db, Table *table, DrsIndice *indice, TableDef *tblDef);
	static bool checkTableWithTable(Database *db, Connection *conn, Table *orig, Table *conv, NtseRidMapping *ridmap, Session *origSess = NULL, Session *convSess = NULL);
	static bool checkIndiceWithHeap(Session *sessionhp, Database *db, DrsHeap *heap, DrsIndice *indice, TableDef *tblDef);
	static bool checkIndiceUnique(Session *session, const TableDef *tableDef, DrsIndice *indice, IndexDef **indiceDef);
	static u16* getAllColumns(TableDef *tableDef);
	static bool checkColumn(TableDef *tableDef, u16 colNo, byte *cols1, byte *cols2);
	static bool checkKeyToRecord(TableDef *tableDef, IndexDef *indexDef, byte *key, byte *record);
	static SubRecord* formIdxKeyFromData(TableDef *tableDef, IndexDef *indexDef, RowId rowId, byte *record);
	static Record* formRecordFromData(TableDef *tableDef, RowId rowId, byte *record);
	static int searchIndex(Session *session, DrsIndex *index, const IndexDef *indexDef, SubRecord *key, TableDef *tbdef, u64 *out = NULL);
};

bool defEq(const IndexDef *left, const IndexDef *right);

#endif