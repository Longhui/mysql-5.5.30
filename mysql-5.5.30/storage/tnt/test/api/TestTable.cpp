#include <iostream>
#include "api/TestTable.h"
#include "Test.h"
#include "misc/Session.h"
#include "misc/RecordHelper.h"
#include "mms/Mms.h"
#include "api/TestHelper.h"
#include "btree/Index.h"
#include "misc/ControlFile.h"

using namespace std;
using namespace ntse;

const uint TableTestCase::SMALL_LOB_SIZE = Limits::PAGE_SIZE / 4;
const uint TableTestCase::LARGE_LOB_SIZE = Limits::PAGE_SIZE * 2;

const char* TableTestCase::getName() {
	return "Table test";
}

const char* TableTestCase::getDescription() {
	return "test DDL operations such as create/drop/open/close and DML operation such as scan/insert/update/delete";
}

bool TableTestCase::isBig() {
	return false;
}

void TableTestCase::setUp() {
	m_db = NULL;
	m_table = NULL;
	Database::drop(".");
	File dir("space");
	dir.rmdir(true);
	EXCPT_OPER(m_db = Database::open(&m_config, true));
	Table::drop(m_db, "BlogCount");
	Table::drop(m_db, "Blog");
	Table::drop(m_db, "BlogBak");
	Table::drop(m_db, "Test");
}

void TableTestCase::tearDown() {
	if (m_table) {
		closeTable(m_table);
	}
	if (m_db) {
		Table::drop(m_db, "BlogCount");
		Table::drop(m_db, "Blog");
		Table::drop(m_db, "BlogBak");
		Table::drop(m_db, "Test");
		m_db->close(false, false);
		delete m_db;
		m_db = NULL;
	}
	Database::drop(".");
	File dir("space");
	dir.rmdir(true);
}

/**
 * 测试表创建、删除、打开、关闭四个基本功能
 */
void TableTestCase::testBasicDDL() {
	// 建表
	EXCPT_OPER(createBlogCount(m_db));
	EXCPT_OPER(createBlog(m_db));

	// 打开表
	EXCPT_OPER(m_table = openTable("BlogCount"));
	EXCPT_OPER(closeTable(m_table));
	m_table = NULL;
	EXCPT_OPER(m_table = openTable("Blog"));
	EXCPT_OPER(closeTable(m_table));
	m_table = NULL;

	EXCPT_OPER(Table::drop(m_db, "BlogCount"));
	EXCPT_OPER(Table::drop(m_db, "Blog"));
}

/**
 * 详细测试创建表时异常等各种情况
 */
void TableTestCase::testCreateFail() {
	// 索引长度过长
	{
		TableDefBuilder tdb(1, "test", "Test");
		tdb.addColumnS("col1", CT_VARCHAR, DrsIndice::IDX_MAX_KEY_LEN / 2 + 10, false);
		tdb.addColumnS("col2", CT_VARCHAR, DrsIndice::IDX_MAX_KEY_LEN / 2 + 10, false);
		tdb.addIndex("idx_test_col12", true, true, false, "col1", 0, "col2", 0, NULL);

		TableDef *tableDef = tdb.getTableDef();
		tableDef->m_id = TableDef::MIN_NORMAL_TABLEID;

		Connection *conn = m_db->getConnection(false);
		Session *session = m_db->getSessionManager()->allocSession("TableTestCase::testCreate", conn);
		try {
			Table::create(m_db, session, "Test", tableDef);
			CPPUNIT_FAIL("Create a table with too long primary key should fail");
		} catch (NtseException &e) {
			CPPUNIT_ASSERT(e.getErrorCode() == NTSE_EC_EXCEED_LIMIT);
		}
		m_db->getSessionManager()->freeSession(session);
		m_db->freeConnection(conn);

		delete tableDef;
	}

	// 由于文件已存在导致建表失败
	{
		u64 code;
		bool exist;
		Connection *conn = m_db->getConnection(false);
		Session *session = m_db->getSessionManager()->allocSession("TableTestCase::testCreate", conn);

		TableDef *tableDef = getBlogDef(true);
		tableDef->m_id = TableDef::MIN_NORMAL_TABLEID;

		File heapFile("Blog.nsd");
		code = heapFile.create(false, false);
		CPPUNIT_ASSERT(code == File::E_NO_ERROR);
		heapFile.close();
		try {
			Table::create(m_db, session, "Blog", tableDef);
			CPPUNIT_FAIL("Create a table where heap file already exist should fail");
		} catch (NtseException &e) {
			CPPUNIT_ASSERT(e.getErrorCode() == NTSE_EC_FILE_EXIST);
		}
		CPPUNIT_ASSERT((code = heapFile.isExist(&exist)) == File::E_NO_ERROR);
		CPPUNIT_ASSERT(exist);
		CPPUNIT_ASSERT((code = heapFile.remove()) == File::E_NO_ERROR);

		File indexFile("Blog.nsi");
		code = indexFile.create(false, false);
		CPPUNIT_ASSERT(code == File::E_NO_ERROR);
		indexFile.close();
		try {
			Table::create(m_db, session, "Blog", tableDef);
			CPPUNIT_FAIL("Create a table where index file already exist should fail");
		} catch (NtseException &e) {
			CPPUNIT_ASSERT(e.getErrorCode() == NTSE_EC_FILE_EXIST);
		}
		CPPUNIT_ASSERT((code = heapFile.isExist(&exist)) == File::E_NO_ERROR);
		CPPUNIT_ASSERT(!exist);
		CPPUNIT_ASSERT((code = indexFile.isExist(&exist)) == File::E_NO_ERROR);
		CPPUNIT_ASSERT(exist);
		CPPUNIT_ASSERT((code = indexFile.remove()) == File::E_NO_ERROR);

		File lobFile("Blog.nsld");
		code = lobFile.create(false, false);
		CPPUNIT_ASSERT(code == File::E_NO_ERROR);
		lobFile.close();
		try {
			Table::create(m_db, session, "Blog", tableDef);
			CPPUNIT_FAIL("Create a table where lob file already exist should fail");
		} catch (NtseException &e) {
			CPPUNIT_ASSERT(e.getErrorCode() == NTSE_EC_FILE_EXIST);
		}
		CPPUNIT_ASSERT((code = heapFile.isExist(&exist)) == File::E_NO_ERROR);
		CPPUNIT_ASSERT(!exist);
		CPPUNIT_ASSERT((code = indexFile.isExist(&exist)) == File::E_NO_ERROR);
		CPPUNIT_ASSERT(!exist);
		CPPUNIT_ASSERT((code = lobFile.isExist(&exist)) == File::E_NO_ERROR);
		CPPUNIT_ASSERT(exist);
		CPPUNIT_ASSERT((code = lobFile.remove()) == File::E_NO_ERROR);

		m_db->getSessionManager()->freeSession(session);
		m_db->freeConnection(conn);
		delete tableDef;
	}
}

/**
 * 测试由于文件不全等打开表失败时的处理
 */
void TableTestCase::testOpenFail() {
	EXCPT_OPER(createBlogCount(m_db, true));
	File indexFile("BlogCount.nsi");
	CPPUNIT_ASSERT(indexFile.remove() == File::E_NO_ERROR);
	try {
		openTable("BlogCount");
		CPPUNIT_FAIL("Open a table without index file should fail");
	} catch (NtseException &e) {
		CPPUNIT_ASSERT(e.getErrorCode() == NTSE_EC_FILE_NOT_EXIST);
	}
}

/**
 * 测试表重命名功能，验证成功时文件被重命名且表名被同步更新
 * 失败时重命名的文件被回退
 */
void TableTestCase::testRename() {
	Connection *conn = m_db->getConnection(false);
	Session *session = m_db->getSessionManager()->allocSession("TableTestCase::testCreate", conn);

	TableDef *tableDef = getBlogDef(true);
	tableDef->m_id = TableDef::MIN_NORMAL_TABLEID;
	File dir("space");
	dir.mkdir();
	EXCPT_OPER(Table::create(m_db, session, "space/Blog", tableDef));
	delete tableDef;
	tableDef = NULL;

	// 成功重命名一个表，验证成功后表名会被对应修改
	EXCPT_OPER(Table::rename(m_db, session, "space/Blog", "space/BlogBak", true, false));
	Table *table;
	EXCPT_OPER(table = Table::open(m_db, session, "space/BlogBak"));
	CPPUNIT_ASSERT(!strcmp(table->getTableDef()->m_schemaName, "space"));
	CPPUNIT_ASSERT(!strcmp(table->getTableDef()->m_name, "BlogBak"));
	table->close(session, false);
	delete table;

	// 目标文件已存在失败，验证已经被重命名的文件会回退
	File lobFile("space/Blog.nsld");
	CPPUNIT_ASSERT(lobFile.create(false, false) == File::E_NO_ERROR);
	lobFile.close();

	try {
		Table::checkRename(m_db, "space/BlogBak", "space/Blog", true);
		CPPUNIT_FAIL("Rename a table to existing file should fail");
	} catch (NtseException &e) {
		CPPUNIT_ASSERT(e.getErrorCode() == NTSE_EC_FILE_EXIST);
		File heapFile("space/Blog.nsd");
		File indexFile("space/Blog.nsi");
		bool exist;
		CPPUNIT_ASSERT(heapFile.isExist(&exist) == File::E_NO_ERROR);
		CPPUNIT_ASSERT(!exist);
		CPPUNIT_ASSERT(indexFile.isExist(&exist) == File::E_NO_ERROR);
		CPPUNIT_ASSERT(!exist);
	}

	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(conn);
}

class Truncator: public Thread {
public:
	Truncator(Database *db, Table *table): Thread("Truncator") {
		m_db = db;
		m_table = table;
	}

	virtual void run() {
		Connection *conn = m_db->getConnection(false);
		Session *session = m_db->getSessionManager()->allocSession("TableTestCase::testCreate", conn);

		try {
			m_table->truncate(session);
			CPPUNIT_FAIL("This should fail");
		} catch (NtseException &e) {
			CPPUNIT_ASSERT(e.getErrorCode() == NTSE_EC_FILE_EXIST);
		}

		m_db->getSessionManager()->freeSession(session);
		m_db->freeConnection(conn);
	}

private:
	Database	*m_db;
	Table		*m_table;
};

/**
 * 测试表截断功能，验证数据被消除但表定义不发生变化
 */
void TableTestCase::testTruncate() {
	EXCPT_OPER(createBlog(m_db, false));
	EXCPT_OPER(m_table = openTable("Blog"));
	uint numRows = 5000;
	Record **rows = populateBlog(m_db, m_table, numRows, false, true);
	for (uint i = 0; i < numRows; i++) {
		byte *abs =  *((byte **)(rows[i]->m_data + m_table->getTableDef()->m_columns[5]->m_mysqlOffset + m_table->getTableDef()->m_columns[5]->m_mysqlSize - 8));
		delete [] abs;
		byte *content =  *((byte **)(rows[i]->m_data + m_table->getTableDef()->m_columns[6]->m_mysqlOffset + m_table->getTableDef()->m_columns[6]->m_mysqlSize - 8));
		delete [] content;
		freeRecord(rows[i]);
	}
	delete [] rows;
	rows = NULL;

	Connection *conn = m_db->getConnection(false);
	Session *session = m_db->getSessionManager()->allocSession("TableTestCase::testCreate", conn);

	//修改表定义
	m_table->lockMeta(session, IL_X, 2000, __FILE__, __LINE__);
	TableDef *tblDef = m_table->m_tableDef;
	tblDef->m_isCompressedTbl = true;
	tblDef->m_rowCompressCfg = new RowCompressCfg();
	tblDef->m_rowCompressCfg->setDicSize(256);
	tblDef->setDefaultColGrps();
	m_table->writeTableDef();
	m_table->unlockMeta(session, IL_X);

	//启用压缩
	ILMode metaLockMode;
	ILMode dataLockMode;
	m_table->lockMeta(session, IL_U, 2000, __FILE__, __LINE__);
	EXCPT_OPER(m_table->createDictionary(session, &metaLockMode, &dataLockMode));
	m_table->unlock(session, dataLockMode);
	m_table->unlockMeta(session, metaLockMode);

	TableDef oldDef(m_table->getTableDef());

	u64 oldWlCnt = m_table->getLockUsage()->m_lockCnt[IL_X];
	bool newHasDict;
	EXCPT_OPER(m_table->truncate(session, true, &newHasDict));
	CPPUNIT_ASSERT(oldDef == *m_table->getTableDef());
	CPPUNIT_ASSERT(newHasDict);

	// 验证是否加了TRUNCATE锁
	CPPUNIT_ASSERT(m_table->getLockUsage()->m_lockCnt[IL_X] == oldWlCnt + 1);

	// 验证一下表里是不是没数据了
	SubRecordBuilder srb(m_table->getTableDef(), REC_REDUNDANT);
	SubRecord *subRec = srb.createEmptySbById(m_table->getTableDef()->m_maxRecSize, "0 1 4");

	TblScan *scanHandle = m_table->tableScan(session, OP_READ, subRec->m_numCols, subRec->m_columns);
	CPPUNIT_ASSERT(!m_table->getNext(scanHandle, subRec->m_data));
	m_table->endScan(scanHandle);

	// 验证TRUNCATE之后表还能用
	numRows = 10;
	rows = populateBlog(m_db, m_table, numRows, false, true);
	for (uint i = 0; i < numRows; i++) {
		byte *abs =  *((byte **)(rows[i]->m_data + m_table->getTableDef()->m_columns[5]->m_mysqlOffset + m_table->getTableDef()->m_columns[5]->m_mysqlSize - 8));
		delete [] abs;
		byte *content =  *((byte **)(rows[i]->m_data + m_table->getTableDef()->m_columns[6]->m_mysqlOffset + m_table->getTableDef()->m_columns[6]->m_mysqlSize - 8));
		delete [] content;
		freeRecord(rows[i]);
	}
	delete [] rows;

	scanHandle = m_table->tableScan(session, OP_READ, subRec->m_numCols, subRec->m_columns);
	for (uint i = 0; i < 10; i++)
		CPPUNIT_ASSERT(m_table->getNext(scanHandle, subRec->m_data));
	CPPUNIT_ASSERT(!m_table->getNext(scanHandle, subRec->m_data));
	m_table->endScan(scanHandle);

	freeSubRecord(subRec);
	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(conn);

	// 以下测试用例由于没有通过Database::closeTable来关闭表会失败
	//// DROP之后CREATE时失败
	//{
	//	Truncator *truncator = new Truncator(m_db, m_table);
	//	truncator->enableSyncPoint(SP_TBL_TRUNCATE_AFTER_DROP);
	//	truncator->start();
	//	truncator->joinSyncPoint(SP_TBL_TRUNCATE_AFTER_DROP);

	//	File heapFile("BlogCount.nsd");
	//	CPPUNIT_ASSERT(heapFile.create(false, false) == File::E_NO_ERROR);
	//	heapFile.close();

	//	truncator->notifySyncPoint(SP_TBL_TRUNCATE_AFTER_DROP);

	//	truncator->join();
	//	delete truncator;
	//	delete m_table;
	//	m_table = NULL;
	//}
}

/**
 * 测试增加索引功能
 */
void TableTestCase::testAddIndex() {
	Connection *conn = m_db->getConnection(false);
	Session *session = m_db->getSessionManager()->allocSession("TableTestCase::testCreate", conn);

	TableDefBuilder *builder = new TableDefBuilder(TableDef::INVALID_TABLEID, "space", "BlogCount");

	builder->addColumn("BlogID", CT_BIGINT, false)->addColumn("CommentCount", CT_INT);
	builder->addColumn("TrackbackCount", CT_SMALLINT)->addColumn("AccessCount", CT_INT);
	builder->addColumnS("Title", CT_CHAR, 50);
	builder->addColumn("UserID", CT_BIGINT, false);
	builder->addIndex("PRIMARY", true, true, false, "BlogID", 0, NULL);

	TableDef *tableDef = builder->getTableDef();
	EXCPT_OPER(m_db->createTable(session, "BlogCount", tableDef));
	delete tableDef;
	m_table = m_db->openTable(session, "BlogCount");

	builder->addIndex("IDX_BLOGCOUNT_UID_AC", false, false, false, "UserID", 0, "AccessCount", 0, NULL);
	tableDef = builder->getTableDef();

	u64 oldRlCnt = m_table->getLockUsage()->m_lockCnt[IL_S];
	u64 oldWlCnt = m_table->getLockUsage()->m_lockCnt[IL_X];
	EXCPT_OPER(m_table->addIndex(session, 1, (const IndexDef **)&tableDef->m_indice[1]));
	CPPUNIT_ASSERT(m_table->getLockUsage()->m_lockCnt[IL_S] == oldRlCnt + 1);
	CPPUNIT_ASSERT(m_table->getLockUsage()->m_lockCnt[IL_X] == oldWlCnt + 1);

	tableDef->m_id = m_table->getTableDef()->m_id;
	CPPUNIT_ASSERT(*tableDef == *m_table->getTableDef());

	// 验证加了索引后能操作
	Record **rows = populateBlogCount(m_db, m_table, 10);
	for (uint i = 0; i < 10; i++) {
		freeRecord(rows[i]);
	}
	delete [] rows;

	// 用用新加的索引看看
	{
		u64 blogId = 1;
		SubRecordBuilder keyBuilder(m_table->getTableDef(), KEY_PAD);
		SubRecord *key = keyBuilder.createSubRecordByName("BlogID", &blogId);
		key->m_rowId = INVALID_ROW_ID;

		SubRecordBuilder srb(m_table->getTableDef(), REC_REDUNDANT);
		SubRecord *subRec = srb.createEmptySbById(m_table->getTableDef()->m_maxRecSize, "0 1 4");

		IndexScanCond cond(0, key, true, true, false);
		TblScan *scanHandle = m_table->indexScan(session, OP_READ, &cond, subRec->m_numCols, subRec->m_columns);
		for (uint i = 0; i < 10; i++)
			CPPUNIT_ASSERT(m_table->getNext(scanHandle, subRec->m_data));
		CPPUNIT_ASSERT(!m_table->getNext(scanHandle, subRec->m_data));
		m_table->endScan(scanHandle);

		freeSubRecord(subRec);
		freeSubRecord(key);
	}

	// 关闭再打开表，验证表定义持久化正确
	closeTable(m_table);
	EXCPT_OPER(m_table = openTable("BlogCount"));
	CPPUNIT_ASSERT(*tableDef == *m_table->getTableDef());

	delete tableDef;
	delete builder;
	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(conn);
}

/** 测试使用在线数据库维护模块进行的在线增加索引操作 */
void TableTestCase::testOnlineAddIndex() {
	TableDefBuilder tdb(TableDef::INVALID_TABLEID, "space", "BlogCount");
	tdb.addColumn("a", CT_INT, false);
	tdb.addColumn("b", CT_INT);
	tdb.addColumn("c", CT_INT);
	tdb.addIndex("a", false, false, false, "a", 0, NULL);
	TableDef *tableDef = tdb.getTableDef();

	TblInterface ti(m_db, "BlogCount");
	EXCPT_OPER(ti.create(tableDef));

	EXCPT_OPER(ti.open());
	CPPUNIT_ASSERT(ti.insertRow(NULL, 1, 1, 1));
	CPPUNIT_ASSERT(ti.insertRow(NULL, 2, 2, 2));

	IndexDef **indice = new IndexDef *[2];
	u32 prefixs[1] = {0};
	indice[0] = new IndexDef("b", 1, &ti.getTableDef()->m_columns[1], prefixs, false, false, true);
	indice[1] = new IndexDef("c", 1, &ti.getTableDef()->m_columns[2], prefixs, false, false, true);

	EXCPT_OPER(ti.addIndex((const IndexDef **)indice, 2));

	delete indice[0];
	delete indice[1];
	delete []indice;

	EXCPT_OPER(ti.close());
	EXCPT_OPER(ti.open());

	CPPUNIT_ASSERT(ti.getTableDef()->m_numIndice == 3);
	ti.close(true);
	delete tableDef;
}

/**
 * 测试删除索引功能
 */
void TableTestCase::testDropIndex() {
	EXCPT_OPER(createBlogCount(m_db, false));
	EXCPT_OPER(m_table = openTable("BlogCount"));
	Record **rows = populateBlogCount(m_db, m_table, 10);
	for (uint i = 0; i < 10; i++) {
		freeRecord(rows[i]);
	}
	delete [] rows;

	Connection *conn = m_db->getConnection(false);
	Session *session = m_db->getSessionManager()->allocSession("TableTestCase::testCreate", conn);

	u64 oldRlCnt = m_table->getLockUsage()->m_lockCnt[IL_S];
	u64 oldWlCnt = m_table->getLockUsage()->m_lockCnt[IL_X];
	EXCPT_OPER(m_table->dropIndex(session, 1));
	CPPUNIT_ASSERT(m_table->getLockUsage()->m_lockCnt[IL_S] == oldRlCnt + 2);
	CPPUNIT_ASSERT(m_table->getLockUsage()->m_lockCnt[IL_X] == oldWlCnt + 1);
	CPPUNIT_ASSERT(m_table->getTableDef()->m_numIndice == 1);

	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(conn);
}

/**
 * 测试启用MMS缓存更新时删除主键索引
 */
void TableTestCase::testDropPKeyWhenCacheUpdate() {
	EXCPT_OPER(createBlogCount(m_db, true));
	EXCPT_OPER(m_table = openTable("BlogCount"));
	Record **rows = populateBlogCount(m_db, m_table, 10);
	for (uint i = 0; i < 10; i++) {
		freeRecord(rows[i]);
	}
	delete [] rows;

	Connection *conn = m_db->getConnection(false);
	Session *session = m_db->getSessionManager()->allocSession("TableTestCase::testCreate", conn);

	m_table->alterCacheUpdate(session, true, -1);
	CPPUNIT_ASSERT(m_table->getTableDef()->m_cacheUpdate == true);

	EXCPT_OPER(m_table->dropIndex(session, 0));
	CPPUNIT_ASSERT(m_table->getTableDef()->m_numIndice == 1);
	CPPUNIT_ASSERT(m_table->getTableDef()->m_cacheUpdate == false);

	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(conn);
}

void TableTestCase::testInsert() {
	uint numRows = Limits::PAGE_SIZE / 80 * 10;
	Record **rows;

	EXCPT_OPER(createBlogCount(m_db, false));
	EXCPT_OPER(m_table = openTable("BlogCount"));
	EXCPT_OPER(rows = populateBlogCount(m_db, m_table, numRows));
	verifyTable(m_db, m_table);

	for (uint i = 0; i < numRows; i++)
		freeRecord(rows[i]);
	delete [] rows;
}

/** 最简单的情况，不使用MMS，只读 */
void TableTestCase::testTableScanNonMms() {
	blogCountScanTest(false, OP_READ, true);
}

/** 使用MMS，只读 */
void TableTestCase::testTableScanMms() {
	blogCountScanTest(true, OP_READ, true);
}

/** 不使用MMS，UPDATE */
void TableTestCase::testTableScanUpdateNonMms() {
	blogCountScanTest(false, OP_UPDATE, true);
}

/** 使用MMS，UPDATE */
void TableTestCase::testTableScanUpdateMms() {
	blogCountScanTest(true, OP_UPDATE, true);
}

/** 不使用MMS，DELETE */
void TableTestCase::testTableScanDeleteNonMms() {
	blogCountScanTest(false, OP_DELETE, true);
}

/** 使用MMS，DELETE */
void TableTestCase::testTableScanDeleteMms() {
	blogCountScanTest(true, OP_DELETE, true);
}

void TableTestCase::testIndexScanNonMms() {
	blogCountScanTest(false, OP_READ, false);
}

void TableTestCase::testIndexScanMms() {
	blogCountScanTest(true, OP_READ, false);
}

void TableTestCase::testIndexScanUpdateNonMms() {
	blogCountScanTest(false, OP_UPDATE, false);
}

void TableTestCase::testIndexScanUpdateMms() {
	blogCountScanTest(true, OP_UPDATE, false);
}

void TableTestCase::testIndexScanDeleteNonMms() {
	blogCountScanTest(false, OP_DELETE, false);
}

void TableTestCase::testIndexScanDeleteMms() {
	blogCountScanTest(true, OP_DELETE, false);
}

/** 创建BlogCount表，为定长表，定义如下
 CREATE TABLE BlogCount (
   BlogID bigint NOT NULL,
   CommentCount int,
   TrackbackCount smallint,
   AccessCount int,
   Title char(50),
   UserID bigint NOT NULL,
   PRIMARY KEY (BlogID),
   KEY IDX_BLOGCOUNT_UID_AC(UserID, AccessCount)
 );
 */
void TableTestCase::createBlogCount(Database *db, bool useMms) {
	TableDef *tableDef = getBlogCountDef(useMms);

	Connection *conn = db->getConnection(false);
	Session *session = db->getSessionManager()->allocSession("TableTestCase::createBlogCount", conn);
	db->createTable(session, "BlogCount", tableDef);
	delete tableDef;
	db->getSessionManager()->freeSession(session);
	db->freeConnection(conn);
}

/**
 * 生成BlogCount表定义
 *
 * @param useMms 是否使用MMS
 * @return BlogCount表定义
 */
TableDef* TableTestCase::getBlogCountDef(bool useMms, bool onlineIdx) {
	TableDefBuilder *builder = new TableDefBuilder(TableDef::INVALID_TABLEID, "space", "BlogCount");

	builder->addColumn("BlogID", CT_BIGINT, false)->addColumn("CommentCount", CT_INT);
	builder->addColumn("TrackbackCount", CT_SMALLINT)->addColumn("AccessCount", CT_INT);
	builder->addColumnS("Title", CT_CHAR, 50);
	builder->addColumn("UserID", CT_BIGINT, false);
	builder->addIndex("PRIMARY", true, true, onlineIdx, "BlogID", 0, NULL);
	builder->addIndex("IDX_BLOGCOUNT_UID_AC", false, false, onlineIdx, "UserID", 0, "AccessCount", 0, NULL);

	TableDef *tableDef = builder->getTableDef();
	tableDef->m_useMms = useMms;

	delete builder;
	return tableDef;
}

/**
 * 生成BlogCount表测试数据，生成的数据的规则为：第n(从0开始)行的各属性按以下规则生成
 * - BlogID: n + 1
 * - CommentCount: 10
 * - TrackbackCount: 2
 * - AccessCount: 100
 * - Title: 在A-Z中随机生成，长为25的字符串
 * - UserID: n / 5 + 1
 *
 * @param db 数据库对象
 * @param table BlogCount表
 * @param numRows 要插入的数据
 * @return 各记录数据，为REC_MYSQL格式
 */
Record** TableTestCase::populateBlogCount(Database *db, Table *table, uint numRows) {
	Record **rows = new Record *[numRows];

	System::srandom(1);
	for (uint n = 0; n < numRows; n++) {
		RecordBuilder rb(table->getTableDef(), RID(0, 0), REC_REDUNDANT);
		rb.appendBigInt(n + 1);
		rb.appendInt(10);
		rb.appendSmallInt(2);
		rb.appendInt(100);
		char title[25];
		for (size_t l = 0; l < sizeof(title) - 1; l++)
			title[l] = (char )('A' + System::random() % 26);
		title[sizeof(title) - 1] = '\0';
		rb.appendChar(title);
		rb.appendBigInt(n / 5 + 1);

		Record *rec = rb.getRecord(table->getTableDef()->m_maxRecSize);
		Connection *conn = db->getConnection(false);
		Session *session = db->getSessionManager()->allocSession("TableTestCase::populateBlogCount", conn);
		uint dupIndex;
		rec->m_format = REC_MYSQL;

		u64 ixlockCntOld = table->getLockUsage()->m_lockCnt[IL_IX];
		CPPUNIT_ASSERT((rec->m_rowId = table->insert(session, rec->m_data, true, &dupIndex)) != INVALID_ROW_ID);
		CPPUNIT_ASSERT(table->getLockUsage()->m_lockCnt[IL_IX] == ixlockCntOld + 1);

		db->getSessionManager()->freeSession(session);
		db->freeConnection(conn);

		rows[n] = rec;
	}

	return rows;
}

/**
 * 将BlogCount表中的记录加载到MMS中
 * @pre 表中必须包含不少于numRows条记录
 * @pre BlogID必须>=0
 *
 * @param db 数据库对象
 * @param table BlogCount表
 * @param numRows 要加载到MMS中的记录
 */
void TableTestCase::populateMmsBlogCount(Database *db, Table *table, uint numRows) {
	SubRecordBuilder srb(table->getTableDef(), REC_REDUNDANT);
	SubRecord *subRec = srb.createEmptySbByName(table->getTableDef()->m_maxRecSize, "Title");
	SubRecordBuilder keyBuilder(table->getTableDef(), KEY_PAD);
	TblScan *scanHandle;
	SubRecord *key = NULL;
	u64 id = 0;
	key = keyBuilder.createSubRecordByName("BlogID", &id);
	key->m_rowId = INVALID_ROW_ID;
	IndexScanCond cond(0, key, true, true, false);

	Connection *conn = db->getConnection(false);
	Session *session = db->getSessionManager()->allocSession("populateMmsBlogCount", conn);

	scanHandle = table->indexScan(session, OP_READ, &cond,
		subRec->m_numCols, subRec->m_columns);
	u64 mmsQueriesOld = table->getMmsTable()->getStatus().m_recordQueries;
	for (uint n = 0; n < numRows; n++) {
		CPPUNIT_ASSERT(table->getNext(scanHandle, subRec->m_data));
		CPPUNIT_ASSERT(table->getMmsTable()->getStatus().m_recordInserts == n + 1);
		CPPUNIT_ASSERT(table->getMmsTable()->getStatus().m_recordQueries == mmsQueriesOld + n + 1);
	}
	table->endScan(scanHandle);

	db->getSessionManager()->freeSession(session);
	db->freeConnection(conn);
	freeSubRecord(key);
	freeSubRecord(subRec);
}

/**
 * 进行表扫描测试，扫描BlogCount表
 *
 * @param useMms 表是否使用MMS
 * @param intention 操作意向
 */
void TableTestCase::blogCountScanTest(bool useMms, OpType intention, bool tableScan) {
	uint numRows = 10;
	int newAccessCount = 50;

	EXCPT_OPER(createBlogCount(m_db, useMms));
	EXCPT_OPER(m_table = openTable("BlogCount"));
	Record **rows = populateBlogCount(m_db, m_table, numRows);

	SubRecordBuilder srb(m_table->getTableDef(), REC_REDUNDANT);
	// 要求返回BlogID, CommentCount, Title字段
	SubRecord *subRec = srb.createEmptySbById(m_table->getTableDef()->m_maxRecSize, "0 1 4");
	SubRecord *key = NULL;

	Connection *conn = m_db->getConnection(false);
	Session *session = m_db->getSessionManager()->allocSession("TableTestCase::testScan", conn);

	// 使用MMS时，先进行一次索引扫描向MMS中插入一半数据
	if (useMms) {
		SubRecordBuilder keyBuilder(m_table->getTableDef(), KEY_PAD);
		u16 columns[3] = {0, 1, 2};
		IndexScanCond cond(0, NULL, true, true, false);
		TblScan *scanHandle = m_table->indexScan(session, OP_READ, &cond, 3, columns);
		for (uint n = 0; n < numRows / 2; n++) {
			CPPUNIT_ASSERT(m_table->getNext(scanHandle, subRec->m_data));
			CPPUNIT_ASSERT(m_table->getMmsTable()->getStatus().m_records == n + 1);
		}
		m_table->endScan(scanHandle);
	}

	u64 rlockCntOld, wlockCntOld;
	rlockCntOld = m_table->getLockUsage()->m_lockCnt[IL_IS];
	wlockCntOld = m_table->getLockUsage()->m_lockCnt[IL_IX];

	TblScan *scanHandle;
	if (tableScan)
		scanHandle = m_table->tableScan(session, intention, subRec->m_numCols, subRec->m_columns);
	else {
		// 使用主键索引，指定起始条件为BlogID >= 0，取所有记录
		u64 blogId = 0;
		SubRecordBuilder keyBuilder(m_table->getTableDef(), KEY_PAD);
		key = keyBuilder.createSubRecordByName("BlogID", &blogId);
		key->m_rowId = INVALID_ROW_ID;
		IndexScanCond cond(0, key, true, true, false);
		scanHandle = m_table->indexScan(session, intention, &cond, subRec->m_numCols, subRec->m_columns);
	}
	u16 updateColumns[1] = {3};
	if (intention == OP_UPDATE) {
		scanHandle->setUpdateColumns(1, updateColumns);
	}
	u64 spAfterScan = session->getMemoryContext()->setSavepoint();
	if (intention == OP_READ) {
		CPPUNIT_ASSERT(m_table->getLockUsage()->m_lockCnt[IL_IS] == rlockCntOld + 1);
		CPPUNIT_ASSERT(m_table->getLockUsage()->m_lockCnt[IL_IX] == wlockCntOld);
	} else {
		CPPUNIT_ASSERT(m_table->getLockUsage()->m_lockCnt[IL_IS] == rlockCntOld);
		CPPUNIT_ASSERT(m_table->getLockUsage()->m_lockCnt[IL_IX] == wlockCntOld + 1);
	}

	uint gotRows = 0;
	memset(subRec->m_data, 0, m_table->getTableDef()->m_maxRecSize);
	while (m_table->getNext(scanHandle, subRec->m_data)) {
		CPPUNIT_ASSERT(scanHandle->getCurrentRid() == rows[gotRows]->m_rowId);

		// 确认表扫描所得记录数据正确
		Record rec(rows[gotRows]->m_rowId, REC_FIXLEN, rows[gotRows]->m_data, rows[gotRows]->m_size);
		SubRecord *subRec2 = srb.createEmptySbById(m_table->getTableDef()->m_maxRecSize, "0 1 4");
		memset(subRec2->m_data, 0, m_table->getTableDef()->m_maxRecSize);
		RecordOper::extractSubRecordFR(m_table->getTableDef(), &rec, subRec2);
		u16 columns[3] = {0, 1, 4};
		CPPUNIT_ASSERT(!compareRecord(m_table, subRec->m_data, subRec2->m_data, 3, columns));

		if ((gotRows % 2) == 0) {
			if (intention == OP_UPDATE) {
				// 更新在索引(UserID, AccessCount)中的AccessCount字段
				SubRecord *updateRec = srb.createSubRecordByName("AccessCount", &newAccessCount);
				CPPUNIT_ASSERT(m_table->updateCurrent(scanHandle, updateRec->m_data));
				freeSubRecord(updateRec);
			} else if (intention == OP_DELETE) {
				m_table->deleteCurrent(scanHandle);
			}
		}

		freeSubRecord(subRec2);

		gotRows++;
		memset(subRec->m_data, 0, m_table->getTableDef()->m_maxRecSize);
	}
	CPPUNIT_ASSERT(numRows == gotRows);
	m_table->endScan(scanHandle);

	verifyTable(m_db, m_table);

	// 如果进行和更新或删除操作，再扫描一遍验证更新产生正确结果
	if (intention != OP_READ) {
		freeSubRecord(subRec);

		subRec = srb.createEmptySbById(m_table->getTableDef()->m_maxRecSize, "0 1 3 4");
		TblScan *scanHandle = m_table->tableScan(session, OP_READ, subRec->m_numCols, subRec->m_columns);
		gotRows = 0;
		memset(subRec->m_data, 0, m_table->getTableDef()->m_maxRecSize);
		while (m_table->getNext(scanHandle, subRec->m_data)) {
			uint matchRow = (intention == OP_UPDATE)? gotRows: gotRows * 2 + 1;
			CPPUNIT_ASSERT(scanHandle->getCurrentRid() == rows[matchRow]->m_rowId);

			// 确认表扫描所得记录数据正确
			Record rec(rows[matchRow]->m_rowId, REC_FIXLEN, rows[matchRow]->m_data, rows[matchRow]->m_size);
			SubRecord *subRec2 = srb.createEmptySbById(m_table->getTableDef()->m_maxRecSize, "0 1 3 4");
			memset(subRec2->m_data, 0, m_table->getTableDef()->m_maxRecSize);
			RecordOper::extractSubRecordFR(m_table->getTableDef(), &rec, subRec2);
			if (intention == OP_UPDATE && (gotRows % 2) == 0)
				*((int *)(subRec2->m_data + m_table->getTableDef()->m_columns[3]->m_offset)) = newAccessCount;
			u16 columns[4] = {0, 1, 3, 4};
			CPPUNIT_ASSERT(!compareRecord(m_table, subRec->m_data, subRec2->m_data, 4, columns));

			freeSubRecord(subRec2);

			gotRows++;
		}
		CPPUNIT_ASSERT(!m_table->getNext(scanHandle, subRec->m_data));
		if (intention == OP_UPDATE)
			CPPUNIT_ASSERT(numRows == gotRows);
		else
			CPPUNIT_ASSERT(numRows == gotRows * 2);
		m_table->endScan(scanHandle);
	}

	for (uint i = 0; i < numRows; i++) {
		freeRecord(rows[i]);
	}
	delete [] rows;
	freeSubRecord(subRec);
	if (key)
		freeSubRecord(key);

	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(conn);
}

/** 创建Blog表，为包含大对象的变长表，定义如下
CREATE TABLE Blog (
  ID bigint NOT NULL,
  UserID bigint NOT NULL,
  PublishTime bigint,
  Title varchar(511),
  Tags varchar(200),
  Abstract text,
  Content mediumtext,
  PRIMARY KEY (ID),
  KEY IDX_BLOG_PUTTIME(PublishTime),
  KEY IDX_BLOG_PUBTIME(UserID, PublishTime)
);
 */
void TableTestCase::createBlog(Database *db, bool useMms) {
	TableDef *tableDef = getBlogDef(useMms);

	Connection *conn = db->getConnection(false);
	Session *session = db->getSessionManager()->allocSession("TableTestCase::createBlog", conn);
	db->createTable(session, "Blog", tableDef);
	delete tableDef;
	db->getSessionManager()->freeSession(session);
	db->freeConnection(conn);
}

/**
 * 生成Blog表定义
 *
 * @param useMms 是否使用MMS
 * @return Blog表定义
 */
TableDef* TableTestCase::getBlogDef(bool useMms, bool onlineIdx) {
	TableDefBuilder *builder = new TableDefBuilder(TableDef::INVALID_TABLEID, "space", "Blog");

	builder->addColumn("ID", CT_BIGINT, false)->addColumn("UserID", CT_BIGINT, false);
	builder->addColumn("PublishTime", CT_BIGINT);
	builder->addColumnS("Title", CT_VARCHAR, 411)->addColumnS("Tags", CT_VARCHAR, 2000);
	builder->addColumn("Abstract", CT_SMALLLOB)->addColumn("Content", CT_MEDIUMLOB);
	builder->addIndex("PRIMARY", true, true, onlineIdx, "ID", 0, NULL);
	builder->addIndex("IDX_BLOG_PUTTIME", false, false, onlineIdx, "PublishTime", 0, NULL);
	builder->addIndex("IDX_BLOG_PUBTIME", false, false, onlineIdx, "UserID", 0, "PublishTime", 0, NULL);
	builder->addIndex("IDX_BLOG_USERID_TAGS_CONTENT", false, false, onlineIdx, "UserId", 0, "TAGS", 5, "Content", 8, NULL);

	TableDef *tableDef = builder->getTableDef();
	tableDef->m_useMms = useMms;

	delete builder;
	return tableDef;
}

/**
 * 生成Blog表测试数据，生成的数据的规则为：第n(从0开始)行的各属性按以下规则生成
 * - ID: n + 1
 * - UserID: n / 5 + 1
 * - PublishTime: 当前时间毫秒数
 * - Title: 长度为100的随机字符串
 * - Tags: 长度为100的随机字符串
 * - Abstract: 长度为100的随机字符串，若lobNotNull为false则奇数行为NULL
 * - Content: 奇数行为大型大对象，偶数行为小型大对象，小型大对象大小为SMALL_LOB_SIZE，
 *   大型大对象大小为LARGE_LOB_SIZE，若lobNotNull为false则偶数行为NULL
 *
 * @param db 数据库对象
 * @param table BlogCount表
 * @param numRows 要插入的数据
 * @param doubleInsert 每条记录是否重复插入两次
 * @return 各记录数据，为MySQL格式
 */
Record** TableTestCase::populateBlog(Database *db, Table *table, uint numRows, bool doubleInsert, bool lobNotNull, u64 idstart) {
	Record **rows = new Record *[numRows];

	for (uint n = 0; n < numRows; n++) {
		RecordBuilder rb(table->getTableDef(), 0, REC_UPPMYSQL);
		u64 id = n + idstart;
		u64 userId = n / 5 + 1;
		u64 publishTime = System::currentTimeMillis();
		char* title = randomStr(100);

		char* tags = randomStr(100);
		rb.appendBigInt(id);
		rb.appendBigInt(userId);
		rb.appendBigInt(publishTime);
		rb.appendVarchar(title);
		rb.appendVarchar(tags);

		char *abs;
		if(!lobNotNull && (n % 2 == 0)){
			rb.appendNull();
		} else{
			abs = randomStr(100);
			rb.appendSmallLob((const byte*)abs);
		}

		size_t contentSize;
		if(n % 2) {
			if (lobNotNull)
				contentSize = TableTestCase::SMALL_LOB_SIZE;
			else
				contentSize = 0;
		} else
			contentSize = TableTestCase::LARGE_LOB_SIZE;

		char *content = NULL;
		if(contentSize) {
			content = randomStr(contentSize);
			rb.appendMediumLob((const byte*)content);
		} else {
			rb.appendNull();
		}

		Record *rec = rb.getRecord();

		Connection *conn = db->getConnection(false);
		Session *session = db->getSessionManager()->allocSession("TableTestCase::populateBlog", conn);
		uint dupIndex;
		rec->m_format = REC_UPPMYSQL;
		CPPUNIT_ASSERT((rec->m_rowId = table->insert(session, rec->m_data, false, &dupIndex)) != INVALID_ROW_ID);
		if (doubleInsert)
			CPPUNIT_ASSERT((rec->m_rowId = table->insert(session, rec->m_data, false, &dupIndex)) == INVALID_ROW_ID);
		db->getSessionManager()->freeSession(session);
		db->freeConnection(conn);

		rows[n] = rec;
		delete []title;
		delete []tags;
	}

	return rows;
}

/**
 * 测试大对象功能
 */
void TableTestCase::testLob() {
	int numRows = 10;
	
	EXCPT_OPER(createBlog(m_db, true));
	EXCPT_OPER(m_table = openTable("Blog"));

	byte **newContents = new byte *[numRows];

	System::srandom(1);

	SubRecordBuilder srb(m_table->getTableDef(), REC_UPPMYSQL, RID(0, 0));
	SubRecord *subRec = srb.createEmptySbById(m_table->getTableDef()->m_maxMysqlRecSize, "0 1 4 5");
	u16 columns[5] = {0, 1, 4, 5, 6};

	Record **rows = populateBlog(m_db, m_table, numRows, true, true);
	verifyTable(m_db, m_table);

	// 进行一次索引扫描，向MMS中插入一半数据，包括大对象的MMS
	{
		Connection *conn = m_db->getConnection(false);
		Session *session = m_db->getSessionManager()->allocSession("TableTestCase::testLob", conn);

		IndexScanCond cond(0, NULL, true, true, false);
		TblScan *scanHandle = m_table->indexScan(session, OP_READ, &cond, 5, columns);
		for (int n = 0; n < numRows / 2; n++) {
			CPPUNIT_ASSERT(m_table->getNext(scanHandle, subRec->m_data, INVALID_ROW_ID, true));
			CPPUNIT_ASSERT(m_table->getMmsTable()->getStatus().m_records == n + 1);
			CPPUNIT_ASSERT(!compareRecord(m_table, rows[n]->m_data, subRec->m_data, 5, columns, REC_UPPMYSQL));
		}
		m_table->endScan(scanHandle);

		m_db->getSessionManager()->freeSession(session);
		m_db->freeConnection(conn);
	}

	verifyTable(m_db, m_table);

	// 扫描并且更新数据，将小型大对象更新为大型大对象，将大型大对象更新为小型大对象
	{
		Connection *conn = m_db->getConnection(false);
		Session *session = m_db->getSessionManager()->allocSession("TableTestCase::testLob", conn);

		TblScan *scanHandle = m_table->tableScan(session, OP_UPDATE, 5, columns);
		u16 updateCols[3] = {3, 4, 6};
		scanHandle->setUpdateColumns(3, updateCols);
		uint gotRows = 0;
		while (m_table->getNext(scanHandle, subRec->m_data, INVALID_ROW_ID, true)) {
			CPPUNIT_ASSERT(!compareRecord(m_table, rows[gotRows]->m_data, subRec->m_data, 5, columns, REC_UPPMYSQL));

			size_t newTitleSize = 50;
			size_t newTagsSize = 80;
			char *title = randomStr(newTitleSize);
			char *tags = randomStr(newTagsSize);

			size_t newLobSize = (gotRows % 2)? LARGE_LOB_SIZE: SMALL_LOB_SIZE;
			char *content = randomStr(newLobSize);

			RecordBuilder rb(m_table->getTableDef(), 0, REC_UPPMYSQL);

			rb.appendBigInt(0);		// id
			rb.appendBigInt(0);		// userid
			rb.appendBigInt(0);		// publishtime
			rb.appendVarchar(title);// title
			rb.appendVarchar(tags); // tags
			rb.appendNull();		// abstract
			rb.appendMediumLob((const byte*)content); // content

			Record *updateRec = rb.getRecord();

			m_table->updateCurrent(scanHandle, updateRec->m_data, false);

			delete []title;
			delete []tags;
			freeRecord(updateRec);

			newContents[gotRows] = (byte *)content;
			gotRows++;
		}
		m_table->endScan(scanHandle);
		CPPUNIT_ASSERT(gotRows == numRows);

		m_db->getSessionManager()->freeSession(session);
		m_db->freeConnection(conn);
	}

	// 再扫描一遍，验证更新后的数据是否正确，同时删除一半数据
	verifyTable(m_db, m_table);
	{
		Connection *conn = m_db->getConnection(false);
		Session *session = m_db->getSessionManager()->allocSession("TableTestCase::testLob", conn);

		TblScan *scanHandle = m_table->tableScan(session, OP_DELETE, 5, columns);
		uint gotRows = 0;
		while (m_table->getNext(scanHandle, subRec->m_data)) {
			uint lobSize = RecordOper::readLobSize(subRec->m_data, m_table->getTableDef()->m_columns[6]);
			byte *lob = RecordOper::readLob(subRec->m_data, m_table->getTableDef()->m_columns[6]);
			if (gotRows % 2)
				CPPUNIT_ASSERT(lobSize == LARGE_LOB_SIZE);
			else
				CPPUNIT_ASSERT(lobSize == SMALL_LOB_SIZE);
			CPPUNIT_ASSERT(!memcmp(lob, newContents[gotRows], lobSize));
			if (gotRows % 2)
				m_table->deleteCurrent(scanHandle);
			gotRows++;
		}
		m_table->endScan(scanHandle);
		CPPUNIT_ASSERT(gotRows == numRows);

		m_db->getSessionManager()->freeSession(session);
		m_db->freeConnection(conn);
	}

	// 再扫描一遍，验证数据删除正确
	verifyTable(m_db, m_table);
	{
		Connection *conn = m_db->getConnection(false);
		Session *session = m_db->getSessionManager()->allocSession("TableTestCase::testLob", conn);

		TblScan *scanHandle = m_table->tableScan(session, OP_READ, 5, columns);
		uint gotRows = 0;
		while (m_table->getNext(scanHandle, subRec->m_data)) {
			uint matchRow = gotRows * 2;
			uint lobSize = RecordOper::readLobSize(subRec->m_data, m_table->getTableDef()->m_columns[6]);
			byte *lob = RecordOper::readLob(subRec->m_data, m_table->getTableDef()->m_columns[6]);
			if (matchRow % 2)
				CPPUNIT_ASSERT(lobSize == LARGE_LOB_SIZE);
			else
				CPPUNIT_ASSERT(lobSize == SMALL_LOB_SIZE);
			CPPUNIT_ASSERT(!memcmp(lob, newContents[matchRow], lobSize));
			gotRows++;
		}
		m_table->endScan(scanHandle);
		CPPUNIT_ASSERT(gotRows == numRows / 2);

		m_db->getSessionManager()->freeSession(session);
		m_db->freeConnection(conn);
	}

	for (int i = 0; i < numRows; i++) {
		byte *abs =  *((byte **)(rows[i]->m_data + m_table->getTableDef()->m_columns[5]->m_mysqlOffset + m_table->getTableDef()->m_columns[5]->m_mysqlSize - 8));
		delete [] abs;
		byte *content =  *((byte **)(rows[i]->m_data + m_table->getTableDef()->m_columns[6]->m_mysqlOffset + m_table->getTableDef()->m_columns[6]->m_mysqlSize - 8));
		delete [] content;
		freeRecord(rows[i]);
		delete [] newContents[i];
	}
	delete [] rows;
	delete [] newContents;
	freeSubRecord(subRec);
}

/**
 * 表不使用MMS时测试根据主键获取并更新、删除记录功能
 */
void TableTestCase::testPKeyFetchNonMms() {
	doPKeyFetchTest(false);
}

/**
 * 表使用MMS时测试根据主键获取并更新、删除记录功能
 */
void TableTestCase::testPKeyFetchMms() {
	doPKeyFetchTest(true);
}

void TableTestCase::doPKeyFetchTest(bool useMms) {
	uint numRows = 10;
	int newAccessCount = 50;

	EXCPT_OPER(createBlogCount(m_db, useMms));
	EXCPT_OPER(m_table = openTable("BlogCount"));
	Record **rows = populateBlogCount(m_db, m_table, numRows);

	SubRecordBuilder srb(m_table->getTableDef(), REC_REDUNDANT);
	// 要求返回BlogID, CommentCount, Title字段
	SubRecord *subRec = srb.createEmptySbById(m_table->getTableDef()->m_maxRecSize, "0 1 4");
	SubRecordBuilder keyBuilder(m_table->getTableDef(), KEY_PAD);
	u16 columns[3] = {0, 1, 4};

	// 根据主键获取前一半记录
	{
		for (uint n = 0; n < numRows / 2; n++) {
			u64 blogId = n + 1;
			SubRecord *key = keyBuilder.createSubRecordByName("BlogID", &blogId);
			key->m_rowId = INVALID_ROW_ID;

			Connection *conn = m_db->getConnection(false);
			Session *session = m_db->getSessionManager()->allocSession("TableTestCase::testPKeyFetch", conn);

			IndexScanCond cond(0, key, true, true, true);
			TblScan *scanHandle = m_table->indexScan(session, OP_READ, &cond, 3, columns);
			CPPUNIT_ASSERT(m_table->getNext(scanHandle, subRec->m_data));
			if (useMms)
				CPPUNIT_ASSERT(m_table->getMmsTable()->getStatus().m_records == n + 1);
			CPPUNIT_ASSERT(!compareRecord(m_table, rows[n]->m_data, subRec->m_data, 3, columns));
			CPPUNIT_ASSERT(!m_table->getNext(scanHandle, subRec->m_data));
			m_table->endScan(scanHandle);

			m_db->getSessionManager()->freeSession(session);
			m_db->freeConnection(conn);

			freeSubRecord(key);
		}
	}

	// 根据主键获取并更新记录
	{
		for (uint n = 0; n < numRows; n++) {
			u64 blogId = n + 1;
			SubRecord *key = keyBuilder.createSubRecordByName("BlogID", &blogId);
			key->m_rowId = INVALID_ROW_ID;

			Connection *conn = m_db->getConnection(false);
			Session *session = m_db->getSessionManager()->allocSession("TableTestCase::testPKeyFetch", conn);

			IndexScanCond cond(0, key, true, true, true);
			TblScan *scanHandle = m_table->indexScan(session, OP_UPDATE, &cond, 1, columns);
			u16 updateCols[1] = {0};
			scanHandle->setUpdateColumns(1, updateCols);

			u64 newBlogId = numRows + n + 1;
			SubRecord *updateRec = srb.createSubRecordByName("BlogID", &newBlogId);

			CPPUNIT_ASSERT(m_table->getNext(scanHandle, subRec->m_data));
			CPPUNIT_ASSERT(!compareRecord(m_table, rows[n]->m_data, subRec->m_data, 1, columns));
			CPPUNIT_ASSERT(m_table->updateCurrent(scanHandle, updateRec->m_data));
			*((u64 *)(rows[n]->m_data + m_table->getTableDef()->m_columns[0]->m_offset)) = newBlogId;

			m_table->endScan(scanHandle);

			m_db->getSessionManager()->freeSession(session);
			m_db->freeConnection(conn);

			freeSubRecord(key);
			freeSubRecord(updateRec);
		}

		// 再扫描一遍看更新对不对
		verifyTable(m_db, m_table);
		for (uint n = 0; n < numRows; n++) {
			u64 blogId = numRows + n + 1;
			SubRecord *key = keyBuilder.createSubRecordByName("BlogID", &blogId);
			key->m_rowId = INVALID_ROW_ID;

			Connection *conn = m_db->getConnection(false);
			Session *session = m_db->getSessionManager()->allocSession("TableTestCase::testPKeyFetch", conn);

			IndexScanCond cond(0, key, true, true, true);
			TblScan *scanHandle = m_table->indexScan(session, OP_READ, &cond, 3, columns);
			CPPUNIT_ASSERT(m_table->getNext(scanHandle, subRec->m_data));
			CPPUNIT_ASSERT(!compareRecord(m_table, rows[n]->m_data, subRec->m_data, 3, columns));
			CPPUNIT_ASSERT(!m_table->getNext(scanHandle, subRec->m_data));
			m_table->endScan(scanHandle);

			m_db->getSessionManager()->freeSession(session);
			m_db->freeConnection(conn);

			freeSubRecord(key);
		}
	}

	// 根据主键获取并删除记录
	{
		for (uint n = 0; n < numRows; n++) {
			u64 blogId = numRows + n + 1;
			SubRecord *key = keyBuilder.createSubRecordByName("BlogID", &blogId);
			key->m_rowId = INVALID_ROW_ID;

			Connection *conn = m_db->getConnection(false);
			Session *session = m_db->getSessionManager()->allocSession("TableTestCase::testPKeyFetch", conn);

			IndexScanCond cond(0, key, true, true, true);
			TblScan *scanHandle = m_table->indexScan(session, OP_DELETE, &cond, 3, columns);

			CPPUNIT_ASSERT(m_table->getNext(scanHandle, subRec->m_data));
			CPPUNIT_ASSERT(!compareRecord(m_table, rows[n]->m_data, subRec->m_data, 3, columns));
			m_table->deleteCurrent(scanHandle);

			m_table->endScan(scanHandle);

			m_db->getSessionManager()->freeSession(session);
			m_db->freeConnection(conn);

			freeSubRecord(key);
		}

		// 再扫描一遍是不是删除了
		verifyTable(m_db, m_table);

		Connection *conn = m_db->getConnection(false);
		Session *session = m_db->getSessionManager()->allocSession("TableTestCase::testPKeyFetch", conn);

		TblScan *scanHandle = m_table->tableScan(session, OP_READ, 3, columns);
		CPPUNIT_ASSERT(!m_table->getNext(scanHandle, subRec->m_data));
		m_table->endScan(scanHandle);

		m_db->getSessionManager()->freeSession(session);
		m_db->freeConnection(conn);
	}

	for (uint i = 0; i < numRows; i++)
		freeRecord(rows[i]);
	delete [] rows;
	freeSubRecord(subRec);
}

/**
 * 测试非主键唯一性索引单条记录获取操作
 */
void TableTestCase::testSingleFetch() {
	TableDefBuilder *builder = new TableDefBuilder(TableDef::INVALID_TABLEID, "space", "Test");

	builder->addColumn("A", CT_INT, false)->addColumn("B", CT_INT, false);
	builder->addIndex("PRIMARY", true, true, false, "A", 0, NULL);
	builder->addIndex("IDX_TEST_B", false, true, false, "B", 0, NULL);

	TableDef *tableDef = builder->getTableDef();
	tableDef->m_useMms = true;
	delete builder;

	TblInterface ti(m_db, "Test");
	ti.create(tableDef);
	ti.open();

	ti.insertRow(NULL, 1, 1);
	ti.insertRow(NULL, 2, 2);
	IdxRange range(tableDef, 1, IdxKey(tableDef, 1, 1, 2), IdxKey(tableDef, 1, 1, 2), true, true);
	ResultSet *rs = ti.selectRows(range, NULL);
	CPPUNIT_ASSERT(rs->getNumRows() == 1);
	delete rs;

	CPPUNIT_ASSERT(ti.updateRows(range, "B", 3) == 1);
	rs = ti.selectRows(range, NULL);
	CPPUNIT_ASSERT(rs->getNumRows() == 0);
	delete rs;

	ti.close(true);

	delete tableDef;
}

/**
 * 测试表的定位扫描功能，定位扫描功能用于实现延迟更新，
 * 即对某些UPDATE语句不能进行流水线更新时，先扫描一遍，
 * 记录要更新的行的RID，然后进行定位扫描，直接用RID取出
 * 记录来更新。本测试用例即模拟这一行为。
 */
void TableTestCase::testPosScanNonMms() {
	doPosScanTest(false);
}

/**
 * 表使用MMS时测试定位扫描功能
 */
void TableTestCase::testPosScanMms() {
	doPosScanTest(true);
}

void TableTestCase::doPosScanTest(bool useMms) {
	uint numRows = 10;
	int newAccessCount = 50;

	EXCPT_OPER(createBlogCount(m_db, useMms));
	EXCPT_OPER(m_table = openTable("BlogCount"));
	Record **rows = populateBlogCount(m_db, m_table, numRows);

	SubRecordBuilder srb(m_table->getTableDef(), REC_REDUNDANT);
	// 要求返回BlogID, CommentCount, Title字段
	SubRecord *subRec = srb.createEmptySbById(m_table->getTableDef()->m_maxRecSize, "0 1 4");
	SubRecordBuilder keyBuilder(m_table->getTableDef(), KEY_PAD);
	u16 columns[3] = {0, 1, 4};

	// 进行定位扫描更新
	{
		u64 startId = 1;
		SubRecord *key = keyBuilder.createSubRecordByName("BlogID", &startId);
		key->m_rowId = INVALID_ROW_ID;

		Connection *conn = m_db->getConnection(false);
		Session *session = m_db->getSessionManager()->allocSession("TableTestCase::testPosScan", conn);

		if (useMms) {
			// 先加载一半记录到MMS中
			uint gotRows = 0;
			IndexScanCond cond(0, key, true, true, false);
			TblScan *scanHandle = m_table->indexScan(session, OP_READ, &cond, 3, columns);

			while (m_table->getNext(scanHandle, subRec->m_data)) {
				CPPUNIT_ASSERT(scanHandle->getCurrentRid() == rows[gotRows]->m_rowId);
				CPPUNIT_ASSERT(!compareRecord(m_table, rows[gotRows]->m_data, subRec->m_data, 3, columns));
				gotRows++;
				if (gotRows == numRows / 2)
					break;
			}

			m_table->endScan(scanHandle);
		}

		u64 rlockCntOld = m_table->getLockUsage()->m_lockCnt[IL_IS];
		u64 wlockCntOld = m_table->getLockUsage()->m_lockCnt[IL_IX];

		TblScan *scanHandle = m_table->positionScan(session, OP_UPDATE, 3, columns);

		CPPUNIT_ASSERT(m_table->getLockUsage()->m_lockCnt[IL_IS] == rlockCntOld);
		CPPUNIT_ASSERT(m_table->getLockUsage()->m_lockCnt[IL_IX] == wlockCntOld + 1);

		u16 updateCols[1] = {0};
		scanHandle->setUpdateColumns(1, updateCols);
		for (uint n = 0; n < numRows; n++) {
			CPPUNIT_ASSERT(m_table->getNext(scanHandle, subRec->m_data, rows[n]->m_rowId));
			CPPUNIT_ASSERT(scanHandle->getCurrentRid() == rows[n]->m_rowId);

			u64 newBlogId = numRows + n + 1;
			SubRecord *updateRec = srb.createSubRecordByName("BlogID", &newBlogId);

			CPPUNIT_ASSERT(m_table->updateCurrent(scanHandle, updateRec->m_data));
			*((u64 *)(rows[n]->m_data + m_table->getTableDef()->m_columns[0]->m_offset)) = newBlogId;

			freeSubRecord(updateRec);
		}
		// 取一条不存在的记录
		CPPUNIT_ASSERT(!m_table->getNext(scanHandle, subRec->m_data, rows[numRows - 1]->m_rowId + 1));
		m_table->endScan(scanHandle);

		m_db->getSessionManager()->freeSession(session);
		m_db->freeConnection(conn);

		freeSubRecord(key);
	}

	// 验证更新结果
	verifyTable(m_db, m_table);
	{
		for (uint n = 0; n < numRows; n++) {
			u64 blogId = numRows + n + 1;
			SubRecord *key = keyBuilder.createSubRecordByName("BlogID", &blogId);
			key->m_rowId = INVALID_ROW_ID;

			Connection *conn = m_db->getConnection(false);
			Session *session = m_db->getSessionManager()->allocSession("TableTestCase::testPosScan", conn);

			IndexScanCond cond(0, key, true, true, true);
			TblScan *scanHandle = m_table->indexScan(session, OP_READ, &cond, 3, columns);
			CPPUNIT_ASSERT(m_table->getNext(scanHandle, subRec->m_data));
			CPPUNIT_ASSERT(!compareRecord(m_table, rows[n]->m_data, subRec->m_data, 3, columns));
			CPPUNIT_ASSERT(!m_table->getNext(scanHandle, subRec->m_data));
			m_table->endScan(scanHandle);

			m_db->getSessionManager()->freeSession(session);
			m_db->freeConnection(conn);
			freeSubRecord(key);
		}
	}

	freeSubRecord(subRec);
	for (uint i = 0; i < numRows; i++)
		freeRecord(rows[i]);
	delete [] rows;
}

class PosScanMmsChangeTester: public Thread {
public:
	PosScanMmsChangeTester(Database *db, Table *table, RowId rid): Thread("PosScanMmsChangeTester") {
		m_db = db;
		m_table = table;
		m_rid = rid;
	}

	virtual void run() {
		Connection *conn = m_db->getConnection(false);
		Session *session = m_db->getSessionManager()->allocSession("PosScanMmsChangeTester", conn);

		SubRecordBuilder srb(m_table->getTableDef(), REC_REDUNDANT);
		SubRecord *subRec = srb.createEmptySbById(m_table->getTableDef()->m_maxRecSize, "0 1 4");
		TblScan *scanHandle = m_table->positionScan(session, OP_READ, subRec->m_numCols, subRec->m_columns);
		CPPUNIT_ASSERT(m_table->getNext(scanHandle, subRec->m_data, m_rid));
		m_table->endScan(scanHandle);

		freeSubRecord(subRec);
		m_db->getSessionManager()->freeSession(session);
		m_db->freeConnection(conn);
	}

private:
	Database	*m_db;
	Table		*m_table;
	RowId		m_rid;
};

/**
 * 测试定位扫描时，第一次搜索MMS时记录不在内存中，第二次搜索时记录在内存中时的情况
 */
void TableTestCase::testPosScanMmsChange() {
	uint numRows = 1;
	int newAccessCount = 50;

	EXCPT_OPER(createBlogCount(m_db, true));
	EXCPT_OPER(m_table = openTable("BlogCount"));
	Record **rows = populateBlogCount(m_db, m_table, numRows);

	PosScanMmsChangeTester *testThread = new PosScanMmsChangeTester(m_db, m_table, rows[0]->m_rowId);
	testThread->enableSyncPoint(SP_REC_BF_AFTER_SEARCH_MMS);
	testThread->start();
	testThread->joinSyncPoint(SP_REC_BF_AFTER_SEARCH_MMS);

	// 将记录加载到MMS中
	Connection *conn = m_db->getConnection(false);
	Session *session = m_db->getSessionManager()->allocSession("TableTestCase::testPosScanMmsChange", conn);

	u64 blogId = 1;
	SubRecordBuilder srb(m_table->getTableDef(), REC_REDUNDANT);
	SubRecord *subRec = srb.createEmptySbById(m_table->getTableDef()->m_maxRecSize, "0 1 4");
	SubRecordBuilder keyBuilder(m_table->getTableDef(), KEY_PAD);
	SubRecord *key = keyBuilder.createSubRecordByName("BlogID", &blogId);
	key->m_rowId = INVALID_ROW_ID;

	IndexScanCond cond(0, key, true, true, true);
	TblScan *scanHandle = m_table->indexScan(session, OP_READ, &cond, subRec->m_numCols, subRec->m_columns);
	CPPUNIT_ASSERT(m_table->getNext(scanHandle, subRec->m_data));
	CPPUNIT_ASSERT(m_table->getMmsTable()->getStatus().m_records == 1);
	m_table->endScan(scanHandle);

	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(conn);
	freeSubRecord(key);
	freeSubRecord(subRec);

	testThread->notifySyncPoint(SP_REC_BF_AFTER_SEARCH_MMS);
	testThread->join();
	delete testThread;

	verifyTable(m_db, m_table);

	for (uint i = 0; i < numRows; i++)
		freeRecord(rows[i]);
	delete [] rows;
}

/** REPLACE测试线程 */
class ReplaceTester: public Thread {
public:
	ReplaceTester(Database *db, Table *table): Thread("ReplaceTester") {
		m_db = db;
		m_table = table;
	}

	virtual void run() {
		Connection *conn = m_db->getConnection(false);
		Session *session = m_db->getSessionManager()->allocSession("TableTestCase::testReplace", conn);

		// INSERT INTO BlogCount(BlogID, CommentCount, TrackbackCount, AccessCount, Title, UserID)
		// VALUES(3, ...) ON DUPLICATE KEY SET BlogID = 2;
		RecordBuilder rb(m_table->getTableDef(), RID(0, 0), REC_REDUNDANT);
		rb.appendBigInt(3);
		rb.appendInt(10);
		rb.appendSmallInt(2);
		rb.appendInt(100);
		char title[25];
		for (size_t l = 0; l < sizeof(title) - 1; l++)
			title[l] = (char )('A' + System::random() % 26);
		title[sizeof(title) - 1] = '\0';
		rb.appendChar(title);
		rb.appendBigInt(2);

		Record *rec = rb.getRecord(m_table->getTableDef()->m_maxRecSize);

		IUSequence<TblScan *> *iuSeq = m_table->insertForDupUpdate(session, rec->m_data);
		CPPUNIT_ASSERT(!iuSeq);

		freeRecord(rec);

		m_db->getSessionManager()->freeSession(session);
		m_db->freeConnection(conn);
	}

private:
	Database	*m_db;
	Table		*m_table;
};

/**
 * 测试REPLACE功能
 */
void TableTestCase::testReplace() {
	// INSERT INTO BlogCount(BlogID, CommentCount, TrackbackCount, AccessCount, Title, UserID)
	// VALUES(1, ...)
	EXCPT_OPER(createBlogCount(m_db, true));
	EXCPT_OPER(m_table = openTable("BlogCount"));
	Record **rows = populateBlogCount(m_db, m_table, 1);

	// INSERT INTO BlogCount(BlogID, CommentCount, TrackbackCount, AccessCount, Title, UserID)
	// VALUES(2, ...) ON DUPLICATE KEY SET BlogID = 3;
	// 不冲突
	{
		Connection *conn = m_db->getConnection(false);
		Session *session = m_db->getSessionManager()->allocSession("TableTestCase::testReplace", conn);

		*((u64 *)(rows[0]->m_data + m_table->getTableDef()->m_columns[0]->m_offset)) = 2;
		IUSequence<TblScan *> *iuSeq = m_table->insertForDupUpdate(session, rows[0]->m_data);
		CPPUNIT_ASSERT(!iuSeq);

		m_db->getSessionManager()->freeSession(session);
		m_db->freeConnection(conn);

		verifyTable(m_db, m_table);
	}

	// INSERT INTO BlogCount(BlogID, CommentCount, TrackbackCount, AccessCount, Title, UserID)
	// VALUES(1, ...) ON DUPLICATE KEY SET BlogID = 3;
	// 冲突，更新后成功
	{
		Connection *conn = m_db->getConnection(false);
		Session *session = m_db->getSessionManager()->allocSession("TableTestCase::testReplace", conn);

		*((u64 *)(rows[0]->m_data + m_table->getTableDef()->m_columns[0]->m_offset)) = 1;
		IUSequence<TblScan *> *iuSeq = m_table->insertForDupUpdate(session, rows[0]->m_data);
		CPPUNIT_ASSERT(iuSeq);
		*((u64 *)(rows[0]->m_data + m_table->getTableDef()->m_columns[0]->m_offset)) = 3;
		u16 columns[1] = {0};
		CPPUNIT_ASSERT(m_table->updateDuplicate(iuSeq, rows[0]->m_data, 1, columns));

		m_db->getSessionManager()->freeSession(session);
		m_db->freeConnection(conn);

		verifyTable(m_db, m_table);
	}

	// INSERT INTO BlogCount(BlogID, CommentCount, TrackbackCount, AccessCount, Title, UserID)
	// VALUES(3, ...) ON DUPLICATE KEY SET BlogID = 2;
	// 冲突，更新后仍不成功
	{
		Connection *conn = m_db->getConnection(false);
		Session *session = m_db->getSessionManager()->allocSession("TableTestCase::testReplace", conn);

		*((u64 *)(rows[0]->m_data + m_table->getTableDef()->m_columns[0]->m_offset)) = 3;
		IUSequence<TblScan *> *iuSeq = m_table->insertForDupUpdate(session, rows[0]->m_data);
		CPPUNIT_ASSERT(iuSeq);
		*((u64 *)(rows[0]->m_data + m_table->getTableDef()->m_columns[0]->m_offset)) = 2;
		u16 columns[1] = {0};
		CPPUNIT_ASSERT(!m_table->updateDuplicate(iuSeq, rows[0]->m_data, 1, columns));

		m_db->getSessionManager()->freeSession(session);
		m_db->freeConnection(conn);

		verifyTable(m_db, m_table);
	}

	// INSERT INTO BlogCount(BlogID, CommentCount, TrackbackCount, AccessCount, Title, UserID)
	// VALUES(3, ...) ON DUPLICATE KEY SET BlogID = 2;
	// 中一个线程在第一个线程发现冲突之后，扫描之前将记录删除，使更新最后成功
	{
		ReplaceTester *testThread = new ReplaceTester(m_db, m_table);
		testThread->enableSyncPoint(SP_TBL_REPLACE_AFTER_DUP);
		testThread->start();
		testThread->joinSyncPoint(SP_TBL_REPLACE_AFTER_DUP);

		// 删除BlogID = 3的记录
		Connection *conn = m_db->getConnection(false);
		Session *session = m_db->getSessionManager()->allocSession("TableTestCase::testReplace", conn);

		u64 blogId = 3;
		SubRecordBuilder keyBuilder(m_table->getTableDef(), KEY_PAD);
		SubRecord *key = keyBuilder.createSubRecordByName("BlogID", &blogId);
		key->m_rowId = INVALID_ROW_ID;

		SubRecordBuilder srb(m_table->getTableDef(), REC_REDUNDANT);
		SubRecord *subRec = srb.createEmptySbByName(m_table->getTableDef()->m_maxRecSize, "BlogID");

		IndexScanCond cond(0, key, true, true, true);
		TblScan *scanHandle = m_table->indexScan(session, OP_DELETE, &cond, subRec->m_numCols, subRec->m_columns);
		CPPUNIT_ASSERT(m_table->getNext(scanHandle, subRec->m_data));
		m_table->deleteCurrent(scanHandle);
		m_table->endScan(scanHandle);

		freeSubRecord(key);
		freeSubRecord(subRec);

		m_db->getSessionManager()->freeSession(session);
		m_db->freeConnection(conn);

		testThread->notifySyncPoint(SP_TBL_REPLACE_AFTER_DUP);
		testThread->join();
		delete testThread;

		verifyTable(m_db, m_table);
	}

	freeRecord(rows[0]);
	delete [] rows;
}

/**
 * 测试NULL的处理，使用包含大对象的Blog表，并进行如下操作
 * - 插入两条记录，一条非大对象为NULL，且为索引属性，一条大对象为NULL，
 * - 进行表扫描读出两条记录，验证对空值的处理正确
 * - 根据NULL键值进行索引扫描，验证索引对空值的处理正确
 * - 更新记录，将非NULL更新为NULL，将NULL更新为非NULL，更新后读取验证正确性
 * - 更新大对象，NULL仍然更新为NULL
 * - 删除记录，考查删除NULL大对象和删除NULL索引键值时的处理流程
 */
void TableTestCase::testNull() {
	EXCPT_OPER(createBlog(m_db, true));
	EXCPT_OPER(m_table = openTable("Blog"));

	Connection *conn = m_db->getConnection(false);
	Session *session = m_db->getSessionManager()->allocSession("TableTestCase::testNull", conn);

	// INSERT INTO Blog(1, 1, NULL, NULL, 'tags', 'abstract', 'content');
	// INSERT INTO Blog(2, 1, 1000, 'title', 'tags', NULL, NULL);
	RecordBuilder *rb = new RecordBuilder(m_table->getTableDef(), INVALID_ROW_ID, REC_REDUNDANT);
	rb->appendBigInt(1)->appendBigInt(1)->appendNull()->appendNull()->appendSmallLob((const byte*)("tags"));
	Record *rec1 = rb->getRecord(m_table->getTableDef()->m_maxRecSize);
	rec1->m_format = REC_MYSQL;
	delete rb;
	RecordOper::writeLobSize(rec1->m_data, m_table->getTableDef()->m_columns[5], (uint)strlen("abstract"));
	RecordOper::writeLob(rec1->m_data, m_table->getTableDef()->m_columns[5], (byte *)"abstract");
	RecordOper::writeLobSize(rec1->m_data, m_table->getTableDef()->m_columns[6], (uint)strlen("content"));
	RecordOper::writeLob(rec1->m_data, m_table->getTableDef()->m_columns[6], (byte *)"content");
	uint dupIndex;
	CPPUNIT_ASSERT(m_table->insert(session, rec1->m_data, true, &dupIndex) != INVALID_ROW_ID);

	rb = new RecordBuilder(m_table->getTableDef(), INVALID_ROW_ID, REC_REDUNDANT);
	rb->appendBigInt(2)->appendBigInt(1)->appendBigInt(1000)->appendVarchar("title")->appendSmallLob((const byte*)("tags"));
	rb->appendNull()->appendNull();
	Record *rec2 = rb->getRecord(m_table->getTableDef()->m_maxRecSize);
	rec2->m_format = REC_MYSQL;
	delete rb;
	RecordOper::writeLobSize(rec2->m_data, m_table->getTableDef()->m_columns[5], 0);
	RecordOper::writeLob(rec2->m_data, m_table->getTableDef()->m_columns[5], NULL);
	RecordOper::writeLobSize(rec2->m_data, m_table->getTableDef()->m_columns[6], 0);
	RecordOper::writeLob(rec2->m_data, m_table->getTableDef()->m_columns[6], NULL);
	CPPUNIT_ASSERT(m_table->insert(session, rec2->m_data, false, &dupIndex) != INVALID_ROW_ID);

	SubRecordBuilder srb(m_table->getTableDef(), REC_REDUNDANT);
	SubRecord *subRec = srb.createEmptySbByName(m_table->getTableDef()->m_maxRecSize, "ID UserID PublishTime Title Tags Abstract Content");

	// 表扫描一遍，取出数据看是否正确
	{
		TblScan *scanHandle = m_table->tableScan(session, OP_READ, subRec->m_numCols, subRec->m_columns);

		memset(subRec->m_data, 0, m_table->getTableDef()->m_maxRecSize);
		CPPUNIT_ASSERT(m_table->getNext(scanHandle, subRec->m_data));
		CPPUNIT_ASSERT(!compareRecord(m_table, rec1->m_data, subRec->m_data, subRec->m_numCols, subRec->m_columns));

		memset(subRec->m_data, 0, m_table->getTableDef()->m_maxRecSize);
		CPPUNIT_ASSERT(m_table->getNext(scanHandle, subRec->m_data));
		CPPUNIT_ASSERT(!compareRecord(m_table, rec2->m_data, subRec->m_data, subRec->m_numCols, subRec->m_columns));

		m_table->endScan(scanHandle);
	}

	// 根据NULL键值进行索引扫描，验证索引对空值的处理正确
	{
		u64 userId = 1;
		SubRecordBuilder keyBuilder(m_table->getTableDef(), KEY_PAD);
		SubRecord *key = keyBuilder.createSubRecordByName("UserID PublishTime", &userId, NULL);
		key->m_rowId = INVALID_ROW_ID;

		IndexScanCond cond(2, key, true, true, false);
		TblScan *scanHandle = m_table->indexScan(session, OP_READ, &cond,
			subRec->m_numCols, subRec->m_columns);

		memset(subRec->m_data, 0, m_table->getTableDef()->m_maxRecSize);
		CPPUNIT_ASSERT(m_table->getNext(scanHandle, subRec->m_data));
		CPPUNIT_ASSERT(!compareRecord(m_table, rec1->m_data, subRec->m_data, subRec->m_numCols, subRec->m_columns));

		m_table->endScan(scanHandle);

		freeSubRecord(key);

		verifyTable(m_db, m_table);
	}

	// 更新记录，更新NULL非大对象属性为非NULL，更新非NULL大对象属性为NULL
	{
		u64 publishTime = 1000;
		SubRecord *updateRec = srb.createSubRecordByName("PublishTime Title Abstract Content", &publishTime, "title1", NULL, NULL);

		u64 id = 1;
		SubRecordBuilder keyBuilder(m_table->getTableDef(), KEY_PAD);
		SubRecord *key = keyBuilder.createSubRecordByName("ID", &id);
		key->m_rowId = INVALID_ROW_ID;

		IndexScanCond cond(0, key, true, true, true);
		TblScan *scanHandle = m_table->indexScan(session, OP_UPDATE, &cond,
			subRec->m_numCols, subRec->m_columns);
		scanHandle->setUpdateColumns(updateRec->m_numCols, updateRec->m_columns);

		memset(subRec->m_data, 0, m_table->getTableDef()->m_maxRecSize);
		CPPUNIT_ASSERT(m_table->getNext(scanHandle, subRec->m_data));
		CPPUNIT_ASSERT(!compareRecord(m_table, rec1->m_data, subRec->m_data, subRec->m_numCols, subRec->m_columns));

		CPPUNIT_ASSERT(m_table->updateCurrent(scanHandle, updateRec->m_data));
		rec1->m_format = REC_REDUNDANT;
		rec1->m_size = m_table->getTableDef()->m_maxRecSize;
		RecordOper::updateRecordRR(m_table->getTableDef(), rec1, updateRec);
		rec1->m_format = REC_MYSQL;
		RecordOper::writeLobSize(rec1->m_data, m_table->getTableDef()->m_columns[5], 0);
		RecordOper::writeLob(rec1->m_data, m_table->getTableDef()->m_columns[5], NULL);
		RecordOper::writeLobSize(rec1->m_data, m_table->getTableDef()->m_columns[6], 0);
		RecordOper::writeLob(rec1->m_data, m_table->getTableDef()->m_columns[6], NULL);

		m_table->endScan(scanHandle);

		freeSubRecord(key);
		freeSubRecord(updateRec);

		u64 userId = 1;
		key = keyBuilder.createSubRecordByName("UserID PublishTime", &userId, &publishTime);
		key->m_rowId = INVALID_ROW_ID;
		IndexScanCond cond2(2, key, true, true, false);
		scanHandle = m_table->indexScan(session, OP_READ, &cond2,
			subRec->m_numCols, subRec->m_columns);

		memset(subRec->m_data, 0, m_table->getTableDef()->m_maxRecSize);
		CPPUNIT_ASSERT(m_table->getNext(scanHandle, subRec->m_data));
		CPPUNIT_ASSERT(!compareRecord(m_table, rec1->m_data, subRec->m_data, subRec->m_numCols, subRec->m_columns));

		m_table->endScan(scanHandle);

		freeSubRecord(key);

		verifyTable(m_db, m_table);
	}

	// 更新记录，更新非NULL非大对象为NULL，NULL大对象为非NULL
	{
		SubRecord *updateRec = srb.createSubRecordByName("PublishTime Title", NULL, NULL);
		updateRec->m_numCols = 4;
		u16 *old = updateRec->m_columns;
		updateRec->m_columns = new u16[4];
		memcpy(updateRec->m_columns, old, sizeof(u16) * 2);
		updateRec->m_columns[2] = 5;
		updateRec->m_columns[3] = 6;
		delete []old;
		RecordOper::writeLobSize(updateRec->m_data, m_table->getTableDef()->m_columns[5], (uint)strlen("abstract2"));
		RecordOper::writeLob(updateRec->m_data, m_table->getTableDef()->m_columns[5], (byte *)"abstract2");
		RecordOper::writeLobSize(updateRec->m_data, m_table->getTableDef()->m_columns[6], (uint)strlen("content2"));
		RecordOper::writeLob(updateRec->m_data, m_table->getTableDef()->m_columns[6], (byte *)"content2");

		u64 id = 2;
		SubRecordBuilder keyBuilder(m_table->getTableDef(), KEY_PAD);
		SubRecord *key = keyBuilder.createSubRecordByName("ID", &id);
		key->m_rowId = INVALID_ROW_ID;

		IndexScanCond cond(0, key, true, true, true);
		TblScan *scanHandle = m_table->indexScan(session, OP_UPDATE, &cond,
			subRec->m_numCols, subRec->m_columns);
		scanHandle->setUpdateColumns(updateRec->m_numCols, updateRec->m_columns);

		memset(subRec->m_data, 0, m_table->getTableDef()->m_maxRecSize);
		CPPUNIT_ASSERT(m_table->getNext(scanHandle, subRec->m_data));
		CPPUNIT_ASSERT(!compareRecord(m_table, rec2->m_data, subRec->m_data, subRec->m_numCols, subRec->m_columns));

		CPPUNIT_ASSERT(m_table->updateCurrent(scanHandle, updateRec->m_data));
		rec2->m_format = REC_REDUNDANT;
		rec2->m_size = m_table->getTableDef()->m_maxRecSize;
		RecordOper::updateRecordRR(m_table->getTableDef(), rec2, updateRec);
		rec2->m_format = REC_MYSQL;
		RecordOper::writeLobSize(rec2->m_data, m_table->getTableDef()->m_columns[5], (uint)strlen("abstract2"));
		RecordOper::writeLob(rec2->m_data, m_table->getTableDef()->m_columns[5], (byte *)"abstract2");
		RecordOper::writeLobSize(rec2->m_data, m_table->getTableDef()->m_columns[6], (uint)strlen("content2"));
		RecordOper::writeLob(rec2->m_data, m_table->getTableDef()->m_columns[6], (byte *)"content2");

		m_table->endScan(scanHandle);

		freeSubRecord(updateRec);
		freeSubRecord(key);

		u64 userId = 1;
		key = keyBuilder.createSubRecordByName("UserID PublishTime", &userId, NULL);
		key->m_rowId = INVALID_ROW_ID;
		IndexScanCond cond2(2, key, true, true, false);
		scanHandle = m_table->indexScan(session, OP_READ, &cond2,
			subRec->m_numCols, subRec->m_columns);

		memset(subRec->m_data, 0, m_table->getTableDef()->m_maxRecSize);
		CPPUNIT_ASSERT(m_table->getNext(scanHandle, subRec->m_data));
		CPPUNIT_ASSERT(!compareRecord(m_table, rec2->m_data, subRec->m_data, subRec->m_numCols, subRec->m_columns));

		m_table->endScan(scanHandle);

		freeSubRecord(key);

		verifyTable(m_db, m_table);
	}

	// 更新NULL大对象仍然为NULL
	{
		SubRecord *updateRec = srb.createSubRecordByName("Abstract Content", NULL, NULL);

		u64 id = 1;
		SubRecordBuilder keyBuilder(m_table->getTableDef(), KEY_PAD);
		SubRecord *key = keyBuilder.createSubRecordByName("ID", &id);
		key->m_rowId = INVALID_ROW_ID;

		IndexScanCond cond(0, key, true, true, true);
		TblScan *scanHandle = m_table->indexScan(session, OP_UPDATE, &cond,
			subRec->m_numCols, subRec->m_columns);
		scanHandle->setUpdateColumns(updateRec->m_numCols, updateRec->m_columns);

		memset(subRec->m_data, 0, m_table->getTableDef()->m_maxRecSize);
		CPPUNIT_ASSERT(m_table->getNext(scanHandle, subRec->m_data));
		CPPUNIT_ASSERT(!compareRecord(m_table, rec1->m_data, subRec->m_data, subRec->m_numCols, subRec->m_columns));

		CPPUNIT_ASSERT(m_table->updateCurrent(scanHandle, updateRec->m_data));


		m_table->endScan(scanHandle);

		freeSubRecord(key);
		freeSubRecord(updateRec);

		key = keyBuilder.createSubRecordByName("ID", &id);
		key->m_rowId = INVALID_ROW_ID;
		IndexScanCond cond2(0, key, true, true, true);
		scanHandle = m_table->indexScan(session, OP_READ, &cond2,
			subRec->m_numCols, subRec->m_columns);

		memset(subRec->m_data, 0, m_table->getTableDef()->m_maxRecSize);
		CPPUNIT_ASSERT(m_table->getNext(scanHandle, subRec->m_data));
		CPPUNIT_ASSERT(!compareRecord(m_table, rec1->m_data, subRec->m_data, subRec->m_numCols, subRec->m_columns));

		m_table->endScan(scanHandle);

		freeSubRecord(key);

		verifyTable(m_db, m_table);
	}

	// 删除记录
	{
		TblScan *scanHandle = m_table->tableScan(session, OP_DELETE, subRec->m_numCols, subRec->m_columns);

		memset(subRec->m_data, 0, m_table->getTableDef()->m_maxRecSize);
		CPPUNIT_ASSERT(m_table->getNext(scanHandle, subRec->m_data));
		CPPUNIT_ASSERT(!compareRecord(m_table, rec1->m_data, subRec->m_data, subRec->m_numCols, subRec->m_columns));
		m_table->deleteCurrent(scanHandle);

		memset(subRec->m_data, 0, m_table->getTableDef()->m_maxRecSize);
		CPPUNIT_ASSERT(m_table->getNext(scanHandle, subRec->m_data));
		CPPUNIT_ASSERT(!compareRecord(m_table, rec2->m_data, subRec->m_data, subRec->m_numCols, subRec->m_columns));
		m_table->deleteCurrent(scanHandle);

		m_table->endScan(scanHandle);

		verifyTable(m_db, m_table);
	}

	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(conn);
	freeSubRecord(subRec);
	freeRecord(rec1);
	freeRecord(rec2);
}

/**
 * 测试各个操作是否正确的加了表级锁
 */
void TableTestCase::testDdlLock() {
	EXCPT_OPER(createBlog(m_db, true));
	EXCPT_OPER(m_table = openTable("Blog"));
	m_db->getConfig()->m_tlTimeout = 1;

	Connection *conn1 = m_db->getConnection(false);
	Session *session1 = m_db->getSessionManager()->allocSession("TableTestCase::testNull", conn1);

	Connection *conn2 = m_db->getConnection(false);
	Session *session2 = m_db->getSessionManager()->allocSession("TableTestCase::testNull", conn2);

	SubRecordBuilder srb(m_table->getTableDef(), REC_REDUNDANT);
	SubRecord *subRec = srb.createEmptySbByName(m_table->getTableDef()->m_maxRecSize, "ID UserID PublishTime Title Abstract Content");

	u64 id = 1;
	SubRecordBuilder keyBuilder(m_table->getTableDef(), KEY_PAD);
	SubRecord *key = keyBuilder.createSubRecordByName("ID", &id);
	key->m_rowId = INVALID_ROW_ID;


	// 加读锁，验证扫描成功、删索引失败，TRUNCATE失败
	EXCPT_OPER(m_table->lockMeta(session1, IL_S, -1, __FILE__, __LINE__));
	EXCPT_OPER(m_table->lock(session1, IL_IS, -1, __FILE__, __LINE__));
	{
		TblScan *scanHandle;
		scanHandle = m_table->tableScan(session2, OP_DELETE, subRec->m_numCols, subRec->m_columns);
		m_table->endScan(scanHandle);

		IndexScanCond cond(0, key, true, true, true);
		scanHandle = m_table->indexScan(session2, OP_READ, &cond, subRec->m_numCols, subRec->m_columns);
		m_table->endScan(scanHandle);

		scanHandle = m_table->positionScan(session2, OP_READ, subRec->m_numCols, subRec->m_columns);
		m_table->endScan(scanHandle);

		IndexDef idxBak(m_table->getTableDef()->m_indice[2]);
		try {
			m_table->dropIndex(session2, 2);
		} catch (NtseException &e) {
			CPPUNIT_ASSERT(e.getErrorCode() == NTSE_EC_LOCK_TIMEOUT);
		}

		try {
			m_table->truncate(session2);
		} catch (NtseException &e) {
			CPPUNIT_ASSERT(e.getErrorCode() == NTSE_EC_LOCK_TIMEOUT);
		}
	}

	m_table->unlock(session1, IL_IS);
	m_table->unlockMeta(session1, IL_S);

	m_db->getSessionManager()->freeSession(session1);
	m_db->freeConnection(conn1);
	m_db->getSessionManager()->freeSession(session2);
	m_db->freeConnection(conn2);
	freeSubRecord(key);
	freeSubRecord(subRec);
}

/**
 * 测试UPDATE主键时记录在MMS中并且为脏时的情况
 */
void TableTestCase::testUpdatePKeyMatchDirtyMms() {
	EXCPT_OPER(createBlogCount(m_db, true));
	EXCPT_OPER(m_table = openTable("BlogCount"));
	Record **rows = populateBlogCount(m_db, m_table, 2);
	rows[0]->m_format = REC_REDUNDANT;

	Connection *conn = m_db->getConnection(false);
	Session *session = m_db->getSessionManager()->allocSession("TableTestCase", conn);

	// 更新CommentCount，并且加记录加载到MMS
	{
		u64 blogId = 1;
		SubRecordBuilder keyBuilder(m_table->getTableDef(), KEY_PAD);
		SubRecord *key = keyBuilder.createSubRecordByName("BlogID", &blogId);
		key->m_rowId = INVALID_ROW_ID;

		int commentCount = 5;
		SubRecordBuilder srb(m_table->getTableDef(), REC_REDUNDANT);
		SubRecord *subRec = srb.createEmptySbByName(m_table->getTableDef()->m_maxRecSize, "CommentCount");
		SubRecord *updateRec = srb.createSubRecordByName("CommentCount", &commentCount);

		IndexScanCond cond(0, key, true, true, true);
		TblScan *scanHandle = m_table->indexScan(session, OP_UPDATE, &cond, subRec->m_numCols, subRec->m_columns);
		scanHandle->setUpdateColumns(updateRec->m_numCols, updateRec->m_columns);
		CPPUNIT_ASSERT(m_table->getNext(scanHandle, subRec->m_data));
		CPPUNIT_ASSERT(m_table->updateCurrent(scanHandle, updateRec->m_data));
		m_table->endScan(scanHandle);

		RecordOper::updateRecordRR(m_table->getTableDef(), rows[0], updateRec);

		freeSubRecord(subRec);
		freeSubRecord(updateRec);
		freeSubRecord(key);

		verifyTable(m_db, m_table);
	}

	// 更新MMS中的记录主键
	{
		u64 blogId = 1;
		SubRecordBuilder keyBuilder(m_table->getTableDef(), KEY_PAD);
		SubRecord *key = keyBuilder.createSubRecordByName("BlogID", &blogId);
		key->m_rowId = INVALID_ROW_ID;

		blogId = 2;
		int commentCount = 8;
		SubRecordBuilder srb(m_table->getTableDef(), REC_REDUNDANT);
		SubRecord *subRec = srb.createEmptySbByName(m_table->getTableDef()->m_maxRecSize, "BlogID CommentCount");
		SubRecord *updateRec = srb.createSubRecordByName("BlogID CommentCount", &blogId, &commentCount);

		IndexScanCond cond(0, key, true, true, true);
		TblScan *scanHandle = m_table->indexScan(session, OP_UPDATE, &cond, subRec->m_numCols, subRec->m_columns);
		scanHandle->setUpdateColumns(updateRec->m_numCols, updateRec->m_columns);
		CPPUNIT_ASSERT(m_table->getNext(scanHandle, subRec->m_data));
		CPPUNIT_ASSERT(!m_table->updateCurrent(scanHandle, updateRec->m_data));
		m_table->endScan(scanHandle);

		freeSubRecord(subRec);
		freeSubRecord(updateRec);
		freeSubRecord(key);

		verifyTable(m_db, m_table);
	}

	// 验证数据正确性
	{
		verifyTable(m_db, m_table);

		SubRecordBuilder srb(m_table->getTableDef(), REC_REDUNDANT);
		SubRecord *subRec = srb.createEmptySbByName(m_table->getTableDef()->m_maxRecSize,
			"BlogID CommentCount");

		TblScan *scanHandle = m_table->tableScan(session, OP_READ,
			subRec->m_numCols, subRec->m_columns);
		CPPUNIT_ASSERT(m_table->getNext(scanHandle, subRec->m_data));
		CPPUNIT_ASSERT(!compareRecord(m_table, rows[0]->m_data,
			subRec->m_data, subRec->m_numCols, subRec->m_columns));
		m_table->endScan(scanHandle);

		freeSubRecord(subRec);
	}

	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(conn);

	freeRecord(rows[0]);
	freeRecord(rows[1]);
	delete [] rows;
}

/** 测试删除所有记录时的情况 */
void TableTestCase::testDeleteAll() {
	EXCPT_OPER(createBlogCount(m_db, true));
	EXCPT_OPER(m_table = openTable("BlogCount"));
	Record **rows = populateBlogCount(m_db, m_table, 2);

	Connection *conn = m_db->getConnection(false);
	Session *session = m_db->getSessionManager()->allocSession("TableTestCase", conn);

	SubRecordBuilder srb(m_table->getTableDef(), REC_REDUNDANT);
	SubRecord *subRec = srb.createEmptySbByName(m_table->getTableDef()->m_maxRecSize, "Title");
	SubRecordBuilder keyBuilder(m_table->getTableDef(), KEY_PAD);
	TblScan *scanHandle;
	SubRecord *key = NULL;
	u64 id = 0;
	key = keyBuilder.createSubRecordByName("BlogID", &id);
	key->m_rowId = INVALID_ROW_ID;
	IndexScanCond cond(0, key, true, true, false);
	scanHandle = m_table->indexScan(session, OP_DELETE, &cond,
		subRec->m_numCols, subRec->m_columns);
	while (m_table->getNext(scanHandle, subRec->m_data))
		m_table->deleteCurrent(scanHandle);
	m_table->endScan(scanHandle);
	freeSubRecord(subRec);
	freeSubRecord(key);

	verifyTable(m_db, m_table);

	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(conn);

	freeRecord(rows[0]);
	freeRecord(rows[1]);
	delete [] rows;
}

/**
 * Ticket #469的回归测试用例
 * 索引扫描时要读取的属性若都为所扫描索引的属性，则UPDATE/DELETE时没有同步更新MMS
 */
void TableTestCase::bug469() {
	EXCPT_OPER(createBlogCount(m_db, true));
	EXCPT_OPER(m_table = openTable("BlogCount"));
	Record **rows = populateBlogCount(m_db, m_table, 1);

	Connection *conn = m_db->getConnection(false);
	Session *session = m_db->getSessionManager()->allocSession("TableTestCase", conn);

	// 进行一次非coverage索引扫描，将记录加载到MMS中
	populateMmsBlogCount(m_db, m_table, 1);

	// 进行coverage索引扫描并删除，验证MMS中的对应记录被删除
	SubRecordBuilder srb(m_table->getTableDef(), REC_REDUNDANT);
	SubRecord *subRec = srb.createEmptySbByName(m_table->getTableDef()->m_maxRecSize, "BlogID");
	SubRecordBuilder keyBuilder(m_table->getTableDef(), KEY_PAD);
	TblScan *scanHandle;
	SubRecord *key = NULL;
	u64 id = 0;
	key = keyBuilder.createSubRecordByName("BlogID", &id);
	key->m_rowId = INVALID_ROW_ID;
	IndexScanCond cond(0, key, true, true, false);
	u64 mmsQueriesOld = m_table->getMmsTable()->getStatus().m_recordQueries;
	scanHandle = m_table->indexScan(session, OP_DELETE, &cond,
		subRec->m_numCols, subRec->m_columns);
	CPPUNIT_ASSERT(m_table->getNext(scanHandle, subRec->m_data));
	CPPUNIT_ASSERT(m_table->getMmsTable()->getStatus().m_recordQueries == mmsQueriesOld);// 扫描时没有去查询MMS
	m_table->deleteCurrent(scanHandle);
	CPPUNIT_ASSERT(m_table->getMmsTable()->getStatus().m_recordQueries > mmsQueriesOld);
	CPPUNIT_ASSERT(m_table->getMmsTable()->getStatus().m_recordDeletes == 1);
	m_table->endScan(scanHandle);
	freeSubRecord(subRec);

	freeSubRecord(key);

	verifyTable(m_db, m_table);

	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(conn);

	freeRecord(rows[0]);
	delete [] rows;
}

/** 测试更新缓存 */
void TableTestCase::testUpdateCache() {
	u32 updateCacheTime = 5;

	// 创建启用更新缓存的表
	TableDef *tableDef = getBlogCountDef(true);
	tableDef->m_cacheUpdate = true;
	tableDef->m_updateCacheTime = updateCacheTime;
	tableDef->m_columns[tableDef->getColumnNo("CommentCount")]->m_cacheUpdate = true;

	Connection *conn = m_db->getConnection(false);
	Session *session = m_db->getSessionManager()->allocSession("TableTestCase::createBlogCount", conn);
	m_db->createTable(session, "BlogCount", tableDef);
	delete tableDef;
	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(conn);

	EXCPT_OPER(m_table = openTable("BlogCount"));

	// 生成测试数据，并加载到MMS中
	Record **rows = populateBlogCount(m_db, m_table, 1);
	populateMmsBlogCount(m_db, m_table, 1);

	// 只更新启用更新缓存的字段
	{
		SubRecordBuilder srb(m_table->getTableDef(), REC_REDUNDANT);
		SubRecord *readRec = srb.createEmptySbByName(m_table->getTableDef()->m_maxRecSize, "BlogID CommentCount");

		SubRecordBuilder keyBuilder(m_table->getTableDef(), KEY_PAD);
		SubRecord *key = NULL;
		u64 id = 0;
		key = keyBuilder.createSubRecordByName("BlogID", &id);
		IndexScanCond cond(0, key, true, true, false);

		Connection *conn = m_db->getConnection(false);
		Session *session = m_db->getSessionManager()->allocSession("populateMmsBlogCount", conn);

		RedRecord updateRec(m_table->getTableDef());
		u64 lsnBefore = session->getLastLsn();
		u64 mmsHitsBefore = m_table->getMmsTable()->getStatus().m_recordQueryHits;
		int lastAc = -1;
		for (int i = 0; i < 5; i++) {
			key->m_rowId = INVALID_ROW_ID;
			u16 updateCols[1] = {m_table->getTableDef()->getColumnNo("CommentCount")};
			TblScan *scanHandle = m_table->indexScan(session, OP_UPDATE, &cond,
				readRec->m_numCols, readRec->m_columns);
			scanHandle->setUpdateColumns(1, updateCols);

			CPPUNIT_ASSERT(m_table->getNext(scanHandle, readRec->m_data));
			int ac = RedRecord::readInt(m_table->getTableDef(), readRec->m_data, m_table->getTableDef()->getColumnNo("CommentCount"));
			if (lastAc > 0)
				CPPUNIT_ASSERT(ac == lastAc);
			updateRec.writeNumber(m_table->getTableDef()->getColumnNo("CommentCount"), ac + 1);
			lastAc = ac + 1;

			m_table->updateCurrent(scanHandle, updateRec.getRecord()->m_data);
			uint hitPerRow = 1;
#ifdef NTSE_VERIFY_EX
			if (vs.tbl)
				hitPerRow = 3;
#endif
			CPPUNIT_ASSERT(m_table->getMmsTable()->getStatus().m_recordQueryHits == mmsHitsBefore + (i + 1) * hitPerRow);
			m_table->endScan(scanHandle);
		}
		CPPUNIT_ASSERT(session->getLastLsn() == lsnBefore);	// TODO: 不是绝对完全，但通常没问题

		verifyTable(m_db, m_table);

		m_db->getSessionManager()->freeSession(session);
		m_db->freeConnection(conn);

		freeSubRecord(readRec);
		freeSubRecord(key);
	}

	freeRecord(rows[0]);
	delete [] rows;
}

/** 测试表没有主键时的DDL操作 */
void TableTestCase::testNoPKey() {
	TableDef *tableDef = getBlogCountDef(false);
	IndexDef idxUidAc(tableDef->m_indice[1]);
	IndexDef idxPkey(tableDef->m_indice[0]);
	for (int idx = tableDef->m_numIndice - 1; idx >= 0; idx--) {
		tableDef->removeIndex(idx);
	}

	TblInterface ti(m_db, "BlogCount");

	// 创建没有任何索引的表
	EXCPT_OPER(ti.create(tableDef));

	EXCPT_OPER(ti.open());

	CPPUNIT_ASSERT(ti.insertRow(NULL, (s64)1, 3, (s16)6, 23, "title 1", (s64)1));
	CPPUNIT_ASSERT(ti.insertRow(NULL, (s64)1, 3, (s16)6, 23, "title 1", (s64)1));
	CPPUNIT_ASSERT(ti.updateRows(-1, 0, "AccessCount", 24));

	// 创建非唯一索引，应该能成功
	IndexDef *uidAc = &idxUidAc;
	IndexDef *pkey = &idxPkey;
	EXCPT_OPER(ti.addIndex((const IndexDef **)&uidAc));
	// 有重复记录，创建唯一索引不能成功
	try {
		ti.addIndex((const IndexDef **)&pkey);
		CPPUNIT_FAIL("Create unique index when table has duplicate rows should fail.");
	} catch (NtseException &) {
	}
	// 删除重复记录，增加主键索引应该能成功
	CPPUNIT_ASSERT(ti.deleteRows(1, 0));
	EXCPT_OPER(ti.addIndex((const IndexDef **)&pkey));

	CPPUNIT_ASSERT(!ti.insertRow(NULL, (s64)1, 3, (s16)6, 23, "title 1", (s64)1));
	EXCPT_OPER(ti.dropIndex(1));
	CPPUNIT_ASSERT(ti.insertRow(NULL, (s64)1, 3, (s16)6, 23, "title 1", (s64)1));

	EXCPT_OPER(ti.dropIndex(0));

	try {
		ti.addIndex((const IndexDef **)&pkey);
		CPPUNIT_FAIL("Create unique index when table has duplicate rows should fail.");
	} catch (NtseException &) {
	}
	CPPUNIT_ASSERT(ti.deleteRows(1, 0));
	EXCPT_OPER(ti.addIndex((const IndexDef **)&pkey));

	ti.close(true);
	delete tableDef;
}

/**
 * 测试超长记录的处理
 */
void TableTestCase::testLongRecord() {
	// 定长堆，创建的时候就应该报错
	{
		TableDefBuilder tdb(TableDef::INVALID_TABLEID, "test", "Test");
		tdb.addColumnS("A", CT_CHAR, Limits::MAX_REC_SIZE + 1, false);
		TableDef *tableDef = tdb.getTableDef();
		tableDef->m_useMms = false;
		TblInterface ti(m_db, "Test");
		try {
			ti.create(tableDef);
			CPPUNIT_FAIL("Create fixed length table with too long record should fail");
		} catch (NtseException &e) {
			CPPUNIT_ASSERT(e.getErrorCode() == NTSE_EC_ROW_TOO_LONG);
		}
		delete tableDef;
	}

	// 变长堆，创建的时候不报错，但插入和更新超长时报错
	{
		TableDefBuilder tdb(TableDef::INVALID_TABLEID, "test", "Test");
		tdb.addColumn("A", CT_BIGINT)->addColumnS("B", CT_VARCHAR, Limits::MAX_REC_SIZE * 2, false);
		TableDef *tableDef = tdb.getTableDef();
		tableDef->m_useMms = false;
		TblInterface ti(m_db, "Test");
		EXCPT_OPER(ti.create(tableDef));
		ti.open();

		// 插入不超长的记录应该成功
		ti.insertRow(NULL, (u64)1, "abc");
		// 插入超长的记录应该失败
		char *tooLongB = randomStr(Limits::MAX_REC_SIZE);
		try {
			ti.insertRow(NULL, (u64)2, tooLongB);
			CPPUNIT_FAIL("Insert too long record should fail");
		} catch (NtseException &e) {
			CPPUNIT_ASSERT(e.getErrorCode() == NTSE_EC_ROW_TOO_LONG);
		}

		// 更新记录超长应该失败
		try {
			ti.updateRows(-1, 0, "B", tooLongB);
			CPPUNIT_FAIL("Update to too long record should fail");
		} catch (NtseException &e) {
			CPPUNIT_ASSERT(e.getErrorCode() == NTSE_EC_ROW_TOO_LONG);
		}

		ti.close(true);
		ti.drop();
		delete tableDef;
		delete []tooLongB;
	} 
	// 更新多个大对象导致预更新日志超长
	{
		tearDown();
		m_config.m_logFileSize = LogConfig::MIN_LOGFILE_SIZE;
		m_config.m_logBufSize = LogConfig::MIN_LOG_BUFFER_SIZE;
		setUp();

		TableDefBuilder tdb(TableDef::INVALID_TABLEID, "test", "Test");
		tdb.addColumn("A", CT_BIGINT)->addColumn("B", CT_MEDIUMLOB)->addColumn("C", CT_MEDIUMLOB);
		TableDef *tableDef = tdb.getTableDef();
		tableDef->m_useMms = false;
		TblInterface ti(m_db, "Test");
		EXCPT_OPER(ti.create(tableDef));
		ti.open();

		char *lob1 = randomStr(16 * 1024 * 1024 - 1);
		char *lob2 = randomStr(16 * 1024 * 1024 - 1);
		ti.insertRow(NULL, (u64)1, lob1, lob2);
		try {
			ti.updateRows(-1, 0, "B C", lob1, lob2);
			CPPUNIT_FAIL("should fail");
		} catch (NtseException &e) {
			CPPUNIT_ASSERT(e.getErrorCode() == NTSE_EC_EXCEED_LIMIT);
		}

		ti.close(true);
		ti.drop();
		delete tableDef;
		delete []lob1;
		delete []lob2;
	}
}

/** 测试插入到MMS中失败时的UPDATE处理 */
void TableTestCase::testUpdatePutMmsFail() {
	TableDefBuilder tdb(TableDef::INVALID_TABLEID, "test", "Test");
	tdb.addColumn("ID", CT_INT, false)->addColumnS("A", CT_CHAR, Limits::PAGE_SIZE / 2, false)->addColumnS("B", CT_VARCHAR, 10);
	tdb.addIndex("IDX_TEST_ID", true, true, false, "ID", 0, NULL);
	TableDef *tableDef = tdb.getTableDef();
	CPPUNIT_ASSERT(tableDef->m_useMms);

	TblInterface ti(m_db, "Test");
	EXCPT_OPER(ti.create(tableDef));
	ti.open();

	ti.insertRow(NULL, 1, "aaa1", "bbb1");
	ti.insertRow(NULL, 2, "aaa2", "bbb2");

	ti.getTable()->getMmsTable()->setMaxRecordCount(1);
	// SELECT * FROM Test WHERE ID = 1
	ResultSet *rs = ti.selectRows(IdxRange(ti.getTableDef(), 0, IdxKey(ti.getTableDef(), 0, 1, 1), IdxKey(ti.getTableDef(), 0, 1, 1), true, true), NULL);
	CPPUNIT_ASSERT(rs->getNumRows() == 1);
	CPPUNIT_ASSERT(ti.getTable()->getMmsTable()->getStatus().m_records == 1);
	RowId rid = rs->m_rids[0];
	delete rs;
	// pin住记录使得替换不会成功
	Connection *conn = m_db->getConnection(false);
	Session *session = m_db->getSessionManager()->allocSession(__FUNCTION__, conn);
	MmsRecord *mmsRec = ti.getTable()->getMmsTable()->getByRid(session, rid, true, NULL, None);

	// SELECT * FROM Test WHERE ID = 2
	IdxRange idIs2(ti.getTableDef(), 0, IdxKey(ti.getTableDef(), 0, 1, 2), IdxKey(ti.getTableDef(), 0, 1, 2), true, true);
	rs = ti.selectRows(idIs2, NULL);
	delete rs;
	CPPUNIT_ASSERT(ti.getTable()->getMmsTable()->getStatus().m_records == 1);

	// UPDATE Test SET A = 'aaaa' WHERE ID = 2
	int id = 2;
	SubRecordBuilder srb(ti.getTableDef(), KEY_PAD, INVALID_ROW_ID);
	SubRecord *key = NULL;
	key = srb.createSubRecordByName("ID", &id);
	IndexScanCond cond(0, key, true, true, true);

	u16 numReadCols = 1;
	u16 readCols[1] = {2};

	TblScan *h = ti.getTable()->indexScan(session, OP_UPDATE, &cond, numReadCols, readCols);
	h->setUpdateColumns(numReadCols, readCols);

	byte buf[Limits::PAGE_SIZE];
	CPPUNIT_ASSERT(ti.getTable()->getNext(h, buf));

	CPPUNIT_ASSERT(ti.getTable()->updateCurrent(h, buf));
	CPPUNIT_ASSERT(ti.getTable()->getMmsTable()->getStatus().m_records == 1);
	ti.getTable()->endScan(h);

	ti.getTable()->getMmsTable()->unpinRecord(session, mmsRec);
	freeSubRecord(key);
	ti.close(true);
	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(conn);
	delete tableDef;
}

// 测试OPTIMIZE功能
void TableTestCase::testOptimize() {
	// 不包含大对象
	{
		TableDef *tableDef = getBlogCountDef(true);

		TblInterface ti(m_db, "BlogCount");
		EXCPT_OPER(ti.create(tableDef));
		ti.open();

		uint numRows = 10;
		Record **rows = populateBlogCount(m_db, ti.getTable(), numRows);

		ResultSet *rs1 = ti.selectRows(NULL);

		EXCPT_OPER(ti.optimize());
		ti.verify();

		ResultSet *rs2 = ti.selectRows(NULL);
		CPPUNIT_ASSERT(*rs1 == *rs2);

		for (uint i = 0; i < numRows; i++)
			freeMysqlRecord(ti.getTableDef(), rows[i]);
		delete []rows;

		ti.close(true);

		delete rs1;
		delete rs2;
		delete tableDef;
	}
	// 包含大对象
	{
		TableDef *tableDef = getBlogDef(true);

		TblInterface ti(m_db, "Blog");
		EXCPT_OPER(ti.create(tableDef));
		ti.open();

		uint numRows = 10;
		Record **rows = populateBlog(m_db, ti.getTable(), numRows, false, true);

		ResultSet *rs1 = ti.selectRows(NULL);

		EXCPT_OPER(ti.optimize());

		ResultSet *rs2 = ti.selectRows(NULL);
		CPPUNIT_ASSERT(*rs1 == *rs2);

		for (uint i = 0; i < numRows; i++)
			freeUppMysqlRecord(ti.getTableDef(), rows[i]);
		delete []rows;

		ti.close(true);

		delete rs1;
		delete rs2;

		delete tableDef;
	}
}


/** 测试索引覆盖扫描后UPDATE，要插入到MMS中 */
void TableTestCase::testCoverageIndexMmsUpdate() {
	// 定长表
	{
		TableDefBuilder tdb(TableDef::INVALID_TABLEID, "Test", "Test");
		tdb.addColumn("A", CT_INT, false);
		tdb.addColumn("B", CT_INT, true);
		tdb.addIndex("A", true, true, false, "A", 0, NULL);
		TableDef *tableDef = tdb.getTableDef();
		tableDef->m_useMms = true;

		TblInterface ti(m_db, "BlogCount");
		EXCPT_OPER(ti.create(tableDef));
		ti.open();

		ti.insertRow(NULL, 1, 1);
		ti.updateRows(ti.buildRange(0, 1, true, true, 1, 1), "B", 2);
		CPPUNIT_ASSERT(ti.getTable()->getMmsTable()->getStatus().m_records == 1);
		
		ti.close(true);
		ti.drop();
		delete tableDef;
	}
	// 变长表
	{
		TableDefBuilder tdb(TableDef::INVALID_TABLEID, "Test", "Test");
		tdb.addColumn("A", CT_INT, false);
		tdb.addColumnS("B", CT_VARCHAR, 10, false, true);
		tdb.addIndex("A", true, true, false, "A", 0, NULL);
		TableDef *tableDef = tdb.getTableDef();
		tableDef->m_useMms = true;

		TblInterface ti(m_db, "BlogCount");
		EXCPT_OPER(ti.create(tableDef));
		ti.open();

		ti.insertRow(NULL, 1, "aaa");
		ti.updateRows(ti.buildRange(0, 1, true, true, 1, 1), "B", "bbb");
		CPPUNIT_ASSERT(ti.getTable()->getMmsTable()->getStatus().m_records == 1);
		
		ti.close(true);
		ti.drop();
		delete tableDef;
	}
}



void TableTestCase::testMmsCheckpoint() {
	uint numRows = Limits::PAGE_SIZE / 80 * 2;
	Record **rows;

	EXCPT_OPER(createBlog(m_db, true));
	EXCPT_OPER(m_table = openTable("Blog"));
	EXCPT_OPER(rows = populateBlog(m_db, m_table, numRows, false, true, 0));
	

	Connection *conn = m_db->getConnection(false);
	Session *session = m_db->getSessionManager()->allocSession("TableTestCase::testMmsCheckpoint", conn);
	m_table->flush(session);
	m_db->setCheckpointEnabled(false);
	// 记录装载到mms
	u16 updCols[2] = {2, 5}; // publishtime和abstract
	byte buf[Limits::PAGE_SIZE];
	{
		IndexScanCond cond(0, NULL, true, true, false);
		TblScan *scanHandle = m_table->indexScan(session, OP_READ, &cond, sizeof(updCols)/sizeof(updCols[0]), updCols);
		while(m_table->getNext(scanHandle, buf))
			;
		m_table->endScan(scanHandle);
		CPPUNIT_ASSERT(m_table->getMmsTable()->getStatus().m_records == numRows);
	}
	// 清洗buffer
	/*
	Record **rcdArr = NULL;
	EXCPT_OPER(rcdArr = populateBlog(m_db, m_table, m_config.m_pageBufSize * 4, false, true, numRows));
	for (uint i = 0; i < numRows; i++) {
		freeMysqlRecord(m_table->getTableDef(), rcdArr[i]);
		rcdArr[i] = NULL;
	}
	delete [] rcdArr;
	rcdArr = NULL;
	*/

	vector<int> missRecords;
	for (uint n = 0; n < numRows; ++n) {
		if (!m_db->getPageBuffer()->hasPage(m_table->getHeap()->getHeapFile(), PAGE_HEAP, RID_GET_PAGE(rows[n]->m_rowId)))
			missRecords.push_back(n);
	}
	cout << "page miss mms record count " << missRecords.size() << endl;
	

	// 更改记录
	for (uint n = 0; n < numRows; ++n) {
		TblScan *scanHdl = m_table->positionScan(session, OP_UPDATE, sizeof(updCols)/sizeof(updCols[0]), updCols);
		scanHdl->setUpdateColumns(sizeof(updCols)/sizeof(updCols[0]), updCols);
		RedRecord::writeNumber(m_table->getTableDef(), 2, rows[n]->m_data, (u64)rand());
		void *org = 0;
		size_t orgsize = 0;
		UppMysqlRecord::readLob(m_table->getTableDef(), rows[n]->m_data, 5, &org, &orgsize);
		char *abs = randomStr(50);
		UppMysqlRecord::writeLob(m_table->getTableDef(), rows[n]->m_data, 5, (byte *)abs, 50);
		delete[] (char *)org;
		CPPUNIT_ASSERT(m_table->getNext(scanHdl, buf, rows[n]->m_rowId));
		CPPUNIT_ASSERT(m_table->updateCurrent(scanHdl, rows[n]->m_data, false));
		m_table->endScan(scanHdl);
	}

	cout << "mms dirty records: " << m_table->getMmsTable()->getStatus().m_dirtyRecords << endl;
	CPPUNIT_ASSERT(m_table->getMmsTable()->getStatus().m_dirtyRecords == numRows);
	m_db->setCheckpointEnabled(true);
	m_db->checkpoint(session);
	cout << "after first checkpoint. mms dirty records: " << m_table->getMmsTable()->getStatus().m_dirtyRecords << endl;
	// 做完检查点，验证仍保留有脏记录
	CPPUNIT_ASSERT(m_table->getMmsTable()->getStatus().m_dirtyRecords == missRecords.size());

	m_db->checkpoint(session);
	cout << "after second checkpoint. mms dirty records: " << m_table->getMmsTable()->getStatus().m_dirtyRecords << endl;
	// 再次完检查点，验证仍保留有脏记录
	CPPUNIT_ASSERT(m_table->getMmsTable()->getStatus().m_dirtyRecords == missRecords.size());
	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(conn);


	// 恢复正确性
	m_db->close(true, false);
	delete m_db;
	m_db = NULL;
	EXCPT_OPER(m_db = Database::open(&m_config, false, 1));
	EXCPT_OPER(m_table = openTable("Blog"));
	conn = m_db->getConnection(false);
	session = m_db->getSessionManager()->allocSession("TableTestCase::testMmsCheckpoint", conn);

	u16 allCols[7] = {0, 1, 2, 3, 4, 5, 6};
	for (uint n = 0; n < numRows; ++n) {
		TblScan *scanHdl = m_table->positionScan(session, OP_READ, sizeof(allCols)/sizeof(allCols[0]), allCols);
		CPPUNIT_ASSERT(m_table->getNext(scanHdl, buf, rows[n]->m_rowId, true));
		Record rec(rows[n]->m_rowId, REC_UPPMYSQL, buf, m_table->getTableDef()->m_maxMysqlRecSize);
		CPPUNIT_ASSERT(RecordOper::isRecordEq(m_table->getTableDef(), &rec, rows[n]));
		m_table->endScan(scanHdl);
	}

	for (uint i = 0; i < numRows; i++)
		freeUppMysqlRecord(m_table->getTableDef(), rows[i]);
	delete [] rows;
}



/** 测试开始SCAN后既UPDATE又DELETE */
void TableTestCase::testUpdateDelete() {
	TableDefBuilder tdb(TableDef::INVALID_TABLEID, "Test", "Test");
	tdb.addColumn("A", CT_INT, false);
	tdb.addColumn("B", CT_INT, true);
	tdb.addColumn("C", CT_INT, true);
	tdb.addIndex("A", true, true, false, "A", 0, NULL);
	tdb.addIndex("B", false, false, false, "B", 0, NULL);
	TableDef *tableDef = tdb.getTableDef();
	tableDef->m_useMms = true;

	TblInterface ti(m_db, "BlogCount");
	EXCPT_OPER(ti.create(tableDef));
	ti.open();

	ti.insertRow(NULL, 1, 1, 1);
	ti.insertRow(NULL, 2, 2, 2);
	
	Connection *conn = m_db->getConnection(false, __FUNC__);
	Session *session = m_db->getSessionManager()->allocSession(__FUNC__, conn);

	Table *table = ti.getTable();
	SubRecordBuilder srb(table->getTableDef(), REC_REDUNDANT);
	SubRecord *subRec = srb.createEmptySbById(table->getTableDef()->m_maxRecSize, "0 2");
	memset(subRec->m_data, 0, subRec->m_size);
	
	TblScan *scan = ti.getTable()->tableScan(session, OP_WRITE, subRec->m_numCols, subRec->m_columns);

	CPPUNIT_ASSERT(table->getNext(scan, subRec->m_data));
	u16 updCols[1] = {1};
	scan->setUpdateColumns(1, updCols);
	CPPUNIT_ASSERT(table->updateCurrent(scan, subRec->m_data, NULL));

	CPPUNIT_ASSERT(table->getNext(scan, subRec->m_data));
	table->deleteCurrent(scan);

	table->endScan(scan);
	
	ti.close(true);
	ti.drop();
	freeSubRecord(subRec);
	delete tableDef;
}

/**
 * 测试创建压缩全局字典
 */
void TableTestCase::testCreateDictionary() {
	uint numRows = 10;
	const uint dictSize = 256;

	EXCPT_OPER(createBlog(m_db, true));
	EXCPT_OPER(m_table = openTable("Blog"));
	Record **rows = populateBlog(m_db, m_table, numRows, false, true);
	for (size_t i = 0; i < numRows; i++) {
		byte *abs =  *((byte **)(rows[i]->m_data + m_table->getTableDef()->m_columns[5]->m_mysqlOffset + m_table->getTableDef()->m_columns[5]->m_mysqlSize - 8));
		delete [] abs;
		byte *content =  *((byte **)(rows[i]->m_data + m_table->getTableDef()->m_columns[6]->m_mysqlOffset + m_table->getTableDef()->m_columns[6]->m_mysqlSize - 8));
		delete [] content;
		freeRecord(rows[i]);
	}
	delete [] rows;
	rows = NULL;

	Connection *conn = m_db->getConnection(false);
	Session *session = m_db->getSessionManager()->allocSession("TableTestCase::testScan", conn);

	//修改表定义
	m_table->lockMeta(session, IL_U, 2000, __FILE__, __LINE__);
	TableDef *tblDef = m_table->m_tableDef;
	tblDef->m_isCompressedTbl = true;
	tblDef->m_rowCompressCfg = new RowCompressCfg();
	tblDef->m_rowCompressCfg->setDicSize(dictSize);
	tblDef->setDefaultColGrps();
	m_table->writeTableDef();

	/***************************************************************************/
	/* create create dictionary failed for not enough records                  */
	/***************************************************************************/
	ILMode metaLockMode, dataLockMode;
	NEED_EXCPT(m_table->createDictionary(session, &metaLockMode, &dataLockMode));
	CPPUNIT_ASSERT(metaLockMode == IL_U);
	CPPUNIT_ASSERT(dataLockMode == IL_NO);
	CPPUNIT_ASSERT(m_table->getMetaLock(session) == IL_U);
	CPPUNIT_ASSERT(m_table->getLock(session) == IL_NO);
	m_table->unlockMeta(session, metaLockMode);

	//插入记录
	uint oldNumRows = numRows;
	numRows = 5000;
	
	rows = populateBlog(m_db, m_table, numRows, false, true, oldNumRows + 1);

	/***************************************************************************/
	/* create create dictionary successful                                        */
	/***************************************************************************/

	//创建垃圾字典文件，创建字典前会被清理
	string trashFilePath = string(m_config.m_basedir) + NTSE_PATH_SEP + "Blog" + Limits::NAME_TEMP_GLBL_DIC_EXT;
	File trashFile(trashFilePath.c_str());
	CPPUNIT_ASSERT(File::getNtseError(trashFile.create(false, true)) == File::E_NO_ERROR);
	CPPUNIT_ASSERT(File::getNtseError(trashFile.close()) == File::E_NO_ERROR);

	m_table->lockMeta(session, IL_U, 2000, __FILE__, __LINE__);
	EXCPT_OPER(m_table->createDictionary(session, &metaLockMode, &dataLockMode));

	Records *records = m_table->getRecords();
	CPPUNIT_ASSERT(records && records->hasValidDictionary());
	CPPUNIT_ASSERT(metaLockMode == IL_X);
	CPPUNIT_ASSERT(dataLockMode == IL_X);
	RowCompressMng *rowCompressMng = records->getRowCompressMng();
	CPPUNIT_ASSERT(rowCompressMng && rowCompressMng->getDictionary() != NULL);

	RCDictionary *dict = records->getDictionary();
	CPPUNIT_ASSERT(dict->getTblId() == tblDef->m_id);
	CPPUNIT_ASSERT(dict->size() == dictSize);

	m_table->unlock(session, dataLockMode);
	m_table->unlockMeta(session, metaLockMode);	

	for (size_t i = 0; i < numRows; i++) {
		byte *abs =  *((byte **)(rows[i]->m_data + m_table->getTableDef()->m_columns[5]->m_mysqlOffset + m_table->getTableDef()->m_columns[5]->m_mysqlSize - 8));
		delete [] abs;
		byte *content =  *((byte **)(rows[i]->m_data + m_table->getTableDef()->m_columns[6]->m_mysqlOffset + m_table->getTableDef()->m_columns[6]->m_mysqlSize - 8));
		delete [] content;
		freeRecord(rows[i]);
	}
	delete [] rows;
	
	//关闭表及数据库
	closeTable(m_table);
	tblDef = NULL;

	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(conn);	
	if (m_db) {
		m_db->close();
		delete m_db;
	}

	//测试重新正常打开
	EXCPT_OPER(m_db = Database::open(&m_config, false));
	EXCPT_OPER(m_table = openTable("Blog"));
	
	tblDef = m_table->getTableDef();
	CPPUNIT_ASSERT(m_db->getControlFile()->hasCprsDict(m_table->getTableDef()->m_id));
	records = m_table->getRecords();
	CPPUNIT_ASSERT(records && records->hasValidDictionary());
	rowCompressMng = records->getRowCompressMng();
	CPPUNIT_ASSERT(rowCompressMng && rowCompressMng->getDictionary() != NULL);

	dict = records->getDictionary();
	CPPUNIT_ASSERT(dict->getTblId() == tblDef->m_id);
	CPPUNIT_ASSERT(dict->size() == dictSize);
}
/**
 * 测试在压缩表中插入和更新超长记录
 */
void TableTestCase::testCompressedLongRecordDML() {
	m_db->setCheckpointEnabled(false);

	TableDefBuilder tdb(TableDef::INVALID_TABLEID, "test", "Test");
	tdb.addColumn("A", CT_BIGINT)->addColumnS("B", CT_VARCHAR, 1024, false);
	tdb.addColumn("C", CT_MEDIUMLOB)->addColumnS("D", CT_VARCHAR, Limits::MAX_REC_SIZE * 2, false);
	TableDef *tableDef = tdb.getTableDef();
	tableDef->m_useMms = true;
	TblInterface ti(m_db, "Test");
	EXCPT_OPER(ti.create(tableDef));
	delete tableDef;

	ti.open();
	Table *table = ti.getTable();
	tableDef = table->getTableDef();

	Connection *conn = m_db->getConnection(false);
	Session *session = m_db->getSessionManager()->allocSession("TableTestCase::testScan", conn);

	//设置为压缩表并创建字典
	table->lockMeta(session, IL_U, 2000, __FILE__, __LINE__);
	tableDef->m_isCompressedTbl = true;
	tableDef->m_rowCompressCfg = new RowCompressCfg();
	tableDef->setDefaultColGrps();
	table->writeTableDef();

	// 插入不超长的记录应该成功
	size_t numInsertRows = 5000;
	for (size_t i = 0; i < numInsertRows; i++) {
		char *c1 = randomStr(1000);
		char *c2 = randomStr(1024);
		CPPUNIT_ASSERT(ti.insertRow(NULL, (u64)(i + 1), c1, c2, "abcdefg"));
		delete []c1;
		delete []c2;
	}

	//创建字典
	ILMode metaLockMode, dataLockMode;
	EXCPT_OPER(table->createDictionary(session, &metaLockMode, &dataLockMode));
	table->unlock(session, dataLockMode);
	table->unlockMeta(session, metaLockMode);

	// 插入超长的记录应该失败
	char *tooLongD = randomStr(Limits::MAX_REC_SIZE);
	char *goodLongC = randomStr(4000);
	try {
		ti.insertRow(NULL, (u64)numInsertRows + 1, "abc", "abc", tooLongD);
		CPPUNIT_FAIL("Insert too long record should fail");
	} catch (NtseException &e) {
		CPPUNIT_ASSERT(e.getErrorCode() == NTSE_EC_ROW_TOO_LONG);
	}

	//更新不超长记录应该成功
	EXCPT_OPER(ti.updateRows(-1, 1, "C", goodLongC));
	EXCPT_OPER(ti.updateRows(-1, 1, "D", "abc"));

	// 更新记录超长应该失败
//	try {
//		ti.updateRows(-1, 1, "D", tooLongD);
//		CPPUNIT_FAIL("Update to too long record should fail");
//	} catch (NtseException &e) {
//		CPPUNIT_ASSERT(e.getErrorCode() == NTSE_EC_ROW_TOO_LONG);
//	}

	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(conn);	

	ti.close(true);
	ti.drop();
	delete []goodLongC;
	delete []tooLongD;
}

/**
 * 测试更新压缩表中的大对象，详见#BUG92850
 */
void TableTestCase::testUpdateLobInCompressTable() {
	m_db->setCheckpointEnabled(false);

	TableDefBuilder tdb(TableDef::INVALID_TABLEID, "test", "Test");
	tdb.addColumn("A", CT_BIGINT, false)->addColumnS("B", CT_VARCHAR, 1024, false)->
		addColumn("C", CT_MEDIUMLOB)->addColumnS("D", CT_VARCHAR, Limits::MAX_REC_SIZE * 2, false);
	tdb.addIndex("PRIMARY", true, true, false, "A", 0, NULL);

	TableDef *tableDef = tdb.getTableDef();
	tableDef->m_useMms = true;
	TblInterface ti(m_db, "Test");
	EXCPT_OPER(ti.create(tableDef));
	delete tableDef;

	ti.open();
	Table *table = ti.getTable();
	tableDef = table->getTableDef();
	IndexDef *indexDef = tableDef->getIndexDef(0);

	Connection *conn = m_db->getConnection(false);
	Session *session = m_db->getSessionManager()->allocSession("TableTestCase::testScan", conn);

	//设置为压缩表并创建字典
	table->lockMeta(session, IL_U, 2000, __FILE__, __LINE__);
	tableDef->m_isCompressedTbl = true;
	tableDef->m_rowCompressCfg = new RowCompressCfg();
	tableDef->setDefaultColGrps();
	table->writeTableDef();

	//准备数据
	size_t numInsertRows = 5000;
	for (size_t i = 0; i < numInsertRows; i++) {
		char *c1 = randomStr(1000);
		char *c2 = randomStr(1024);
		CPPUNIT_ASSERT(ti.insertRow(NULL, (u64)(i + 1), c1, c2, "abcdefg"));
		delete []c1;
		delete []c2;
	}

	//创建字典
	ILMode metaLockMode, dataLockMode;
	EXCPT_OPER(table->createDictionary(session, &metaLockMode, &dataLockMode));
	table->unlock(session, dataLockMode);
	table->unlockMeta(session, metaLockMode);

	//将表改为非压缩表
	EXCPT_OPER(table->alterCompressRows(session, false, -1));
	CPPUNIT_ASSERT_EQUAL(false, tableDef->m_isCompressedTbl);

	//插入大对象为空的行，因为表已经被改为非压缩表，所以新插入的记录不会进行压缩
	u64 colA = numInsertRows + 1;
	char *colB = randomStr(1000);
	CPPUNIT_ASSERT(ti.insertRow(NULL, colA, colB, NULL, "abcdefg"));
	verifyTable(m_db, table);

	//重新将表改为压缩表
	EXCPT_OPER(table->alterCompressRows(session, true, -1));
	CPPUNIT_ASSERT_EQUAL(true, tableDef->m_isCompressedTbl);

	//更新刚插入的行的大对象
	IdxRange idxRange = ti.buildRange(0, 1, true, true, colA, colA);
	try {
		char *colC = randomStr(1000);
		ti.updateRows(idxRange, "C", colC);
		delete []colC;
	} catch (NtseException &e) {
		UNREFERENCED_PARAMETER(e);
		CPPUNIT_FAIL("Should update successfully!");
	}

	//查询一次使之进入mms
	ResultSet *rs1 = ti.selectRows(idxRange, "A B C D", -1, 0);
	u16 colNos[] = { 0, 1, 2, 3 };
	CPPUNIT_ASSERT_EQUAL((u64)1, rs1->getNumRows());

	verifyTable(m_db, table);

	ResultSet *rs2 = ti.selectRows(idxRange, "A B C D", -1, 0);
	CPPUNIT_ASSERT_EQUAL((u64)1, rs2->getNumRows());

	//将大对象重新更新为空
	try {
		ti.updateRows(idxRange, "C", NULL);
	} catch (NtseException &e) {
		UNREFERENCED_PARAMETER(e);
		CPPUNIT_FAIL("Should update successfully!");
	}

	verifyTable(m_db, table);

	ResultSet *rs3 = ti.selectRows(idxRange, "A B C D", -1, 0);
	CPPUNIT_ASSERT_EQUAL((u64)1, rs3->getNumRows());

	delete [] colB;
	delete rs1;
	delete rs2;
	delete rs3;

	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(conn);	

	ti.close(true);
	ti.drop();
}

void TableTestCase::closeTable(Table *table) {
	Connection *conn = m_db->getConnection(false);
	Session *session = m_db->getSessionManager()->allocSession("TableTestCase::closeTable", conn);
	m_db->closeTable(session, table);
	m_db->realCloseTableIfNeed(session, table);
	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(conn);
}

Table* TableTestCase::openTable(const char *path) {
	Connection *conn = m_db->getConnection(false);
	Session *session = m_db->getSessionManager()->allocSession("TableTestCase::openTable", conn);
	try {
		Table *table = m_db->openTable(session, path);
		m_db->getSessionManager()->freeSession(session);
		m_db->freeConnection(conn);
		return table;
	} catch (NtseException &e) {
		m_db->getSessionManager()->freeSession(session);
		m_db->freeConnection(conn);
		throw e;
	}
}

int TableTestCase::compareRecord(Table *table, const byte *rec1, const byte *rec2, u16 numCols, const u16 *columns, RecFormat recFormat) {
	if (recFormat == REC_UPPMYSQL)
		return compareUppMysqlRecord(table->getTableDef(), rec1, rec2, numCols, columns);
	else
		return compareRecord(table->getTableDef(), rec1, rec2, numCols, columns);
}

int TableTestCase::compareRecord(const TableDef *tableDef, const byte *rec1, const byte *rec2, u16 numCols, const u16 *columns) {
	for (u16 i = 0; i < numCols; i++) {
		u16 cno = columns[i];
		Record r1(INVALID_ROW_ID, REC_REDUNDANT, (byte *)rec1, tableDef->m_maxRecSize);
		Record r2(INVALID_ROW_ID, REC_REDUNDANT, (byte *)rec2, tableDef->m_maxRecSize);
		bool isNull1 = RecordOper::isNullR(tableDef, &r1, cno);
		bool isNull2 = RecordOper::isNullR(tableDef, &r2, cno);
		if (isNull1 && !isNull2)
			return 1;
		else if (!isNull1 && isNull2)
			return -1;
		else if (!isNull1 && !isNull2) {
			if (tableDef->m_columns[cno]->isLob()) {
				void *lob1, *lob2;
				size_t lobSize1, lobSize2;
				RedRecord::readLob(tableDef, rec1, cno, &lob1, &lobSize1);
				RedRecord::readLob(tableDef, rec1, cno, &lob2, &lobSize2);
				size_t minSize = lobSize1 > lobSize2? lobSize2 : lobSize1;
				int r = memcmp(lob1, lob2, minSize);
				if (r)
					return r;
				else if (lobSize1 > lobSize2)
					return 1;
				else if (lobSize1 < lobSize2)
					return -1;
			} else if (tableDef->m_columns[cno]->m_type == CT_VARCHAR) {
				u16 offset = tableDef->m_columns[cno]->m_offset;
				u16 size1, size2;
				u16 lenBytes = tableDef->m_columns[cno]->m_lenBytes;
				if (lenBytes == 1) {
					size1 = *(rec1 + offset);
					size2 = *(rec2 + offset);
				} else {
					assert(lenBytes == 2);
					size1 = *((u16 *)(rec1 + offset));
					size2 = *((u16 *)(rec2 + offset));
				}
				u16 minSize = size1 > size2? size2: size1;
				int r = memcmp(rec1 + offset + lenBytes, rec2 + offset + lenBytes, minSize);
				if (r)
					return r;
				else if (size1 > size2)
					return 1;
				else if (size1 < size2)
					return -1;
			} else {
				u16 offset = tableDef->m_columns[cno]->m_offset;
				u16 size = tableDef->m_columns[cno]->m_size;
				int r = memcmp(rec1 + offset, rec2 + offset, size);
				if (r)
					return r;
			}
		}
	}
	return 0;
}

int TableTestCase::compareUppMysqlRecord(const TableDef *tableDef, const byte *rec1, const byte *rec2, u16 numCols, const u16 *columns) {
	for (u16 i = 0; i < numCols; i++) {
		u16 cno = columns[i];
		Record r1(INVALID_ROW_ID, REC_UPPMYSQL, (byte *)rec1, tableDef->m_maxMysqlRecSize);
		Record r2(INVALID_ROW_ID, REC_UPPMYSQL, (byte *)rec2, tableDef->m_maxMysqlRecSize);
		bool isNull1 = RecordOper::isNullR(tableDef, &r1, cno);
		bool isNull2 = RecordOper::isNullR(tableDef, &r2, cno);
		if (isNull1 && !isNull2)
			return 1;
		else if (!isNull1 && isNull2)
			return -1;
		else if (!isNull1 && !isNull2) {
			if (tableDef->m_columns[cno]->isLongVar()) {
				u16 offset = tableDef->m_columns[cno]->m_mysqlOffset;
				u16 size1, size2;
				u16 lenBytes = 2;

				size1 = *((u16 *)(rec1 + offset));
				size2 = *((u16 *)(rec2 + offset));

				u16 minSize = size1 > size2? size2: size1;
				int r = memcmp(rec1 + offset + lenBytes, rec2 + offset + lenBytes, minSize);
				if (r)
					return r;
				else if (size1 > size2)
					return 1;
				else if (size1 < size2)
					return -1;
			} else if (tableDef->m_columns[cno]->isLob()) {
				void *lob1, *lob2;
				size_t lobSize1, lobSize2;
				UppMysqlRecord::readLob(tableDef, rec1, cno, &lob1, &lobSize1);
				UppMysqlRecord::readLob(tableDef, rec1, cno, &lob2, &lobSize2);
				size_t minSize = lobSize1 > lobSize2? lobSize2 : lobSize1;
				int r = memcmp(lob1, lob2, minSize);
				if (r)
					return r;
				else if (lobSize1 > lobSize2)
					return 1;
				else if (lobSize1 < lobSize2)
					return -1;
			} else if (tableDef->m_columns[cno]->m_type == CT_VARCHAR) {
				u16 offset = tableDef->m_columns[cno]->m_mysqlOffset;
				u16 size1, size2;
				u16 lenBytes = tableDef->m_columns[cno]->m_lenBytes;
				if (lenBytes == 1) {
					size1 = *(rec1 + offset);
					size2 = *(rec2 + offset);
				} else {
					assert(lenBytes == 2);
					size1 = *((u16 *)(rec1 + offset));
					size2 = *((u16 *)(rec2 + offset));
				}
				u16 minSize = size1 > size2? size2: size1;
				int r = memcmp(rec1 + offset + lenBytes, rec2 + offset + lenBytes, minSize);
				if (r)
					return r;
				else if (size1 > size2)
					return 1;
				else if (size1 < size2)
					return -1;
			} else {
				u16 offset = tableDef->m_columns[cno]->m_mysqlOffset;
				u16 size = tableDef->m_columns[cno]->m_size;
				int r = memcmp(rec1 + offset, rec2 + offset, size);
				if (r)
					return r;
			}
		}
	}
	return 0;
}


void TableTestCase::verifyTable(Database *db, Table *table) {
	Connection *conn = db->getConnection(false);
	Session *session = db->getSessionManager()->allocSession("RecoverTestCase", conn);

	table->verify(session);

	db->getSessionManager()->freeSession(session);
	db->freeConnection(conn);
}

/////////////////////////////////////////////////////////////////////////////
// Big Test
/////////////////////////////////////////////////////////////////////////////

const char* TableBigTest::getName() {
	return "Table Performance Test";
}

const char* TableBigTest::getDescription() {
	return "Test performance of various table level operations";
}

bool TableBigTest::isBig() {
	return true;
}

void TableBigTest::setUp() {
	m_db = NULL;
	m_table = NULL;
	File dir("testdb");
	dir.rmdir(true);
	dir.mkdir();
	m_config.setBasedir("testdb");
	m_config.m_logLevel = EL_WARN;
	m_config.m_pageBufSize = 50000;
	m_config.m_mmsSize = 20000;
	m_config.m_logFileSize = 1024 * 1024 * 128;
	EXCPT_OPER(m_db = Database::open(&m_config, true));
}

void TableBigTest::tearDown() {
	if (m_table) {
		if (m_table->getTable())
			m_table->close();
		delete m_table;
	}
	if (m_db) {
		m_db->close(false, false);
		delete m_db;
	}
	File dir("testdb");
	dir.rmdir(true);
}

/**
 * 测试说明                                                              
 * 表定义 buzz(userid bigint, time int, buzzid bigint, primary key(userid, time, buzzid)) engine = ntse comment = 'usemms:false;index_only:true';
 * 数据生成机制
 * userid: 1~10万随机生成
 * time: 当前时间
 * buzzid: 从1递增
 */
void TableBigTest::testBuzz() {
	int numRows = 5000000;
	int numUsers = 100000;
	bool persistSession = true;
	bool lockTable = false;

	TableDefBuilder tdb(TableDef::INVALID_TABLEID, "testdb", "Test");
	tdb.addColumn("userid", CT_BIGINT, false);
	tdb.addColumn("time", CT_INT, false);
	tdb.addColumn("buzzid", CT_BIGINT, false);
	tdb.addIndex("PRIMARY", true, true, false, "userid", 0, "time", 0, "buzzid", 0, NULL);
	TableDef *tableDef = tdb.getTableDef();
	tableDef->m_useMms = false;
	tableDef->m_indexOnly = true;

	m_table = new TblInterface(m_db, "Test");
	EXCPT_OPER(m_table->create(tableDef));
	m_table->open();
	m_db->getPageBuffer()->disableScavenger();
	m_db->setCheckpointEnabled(false);

	RedRecord rec(m_table->getTableDef());

//	m_db->getPageBuffer()->printStatus(cout);

	cout << "Loading " << numRows << " rows" << endl;

	Connection *conn = m_db->getConnection(false, __FUNC__);
	Session *session = m_db->getSessionManager()->allocSession(__FUNC__, conn), *session2 = NULL;
	session->disableLogging();

	u64 before, after, pre;
	pre = before = System::currentTimeMillis();
	for (int i = 0; i < numRows; i++) {
		u64 userId = System::random() % numUsers;
		int time = System::fastTime();
		u64 buzzId = i + 1;
		rec.writeNumber(0, userId)->writeNumber(1, time)->writeNumber(2, buzzId);

		if (!persistSession) {
			session2 = m_db->getSessionManager()->allocSession(__FUNC__, conn);
			session2->disableLogging();
		}

		m_table->getTable()->insert(persistSession? session: session2, rec.getRecord()->m_data, true, NULL, lockTable);

		if (!persistSession)
			m_db->getSessionManager()->freeSession(session);

		if ((i % 100000) == 99999) {
			cout << ".";
			if ((i % 1000000) == 999999) {
				u64 now = System::currentTimeMillis();
				cout << "(" << 1000000 / (now - pre) << ")" << endl;
				pre = now;
			}
		}
	}
	after = System::currentTimeMillis();

	cout << "opms: " << numRows / (after - before) << endl;
	cout << "size: " << m_table->getTable()->getIndexLength(session) << endl;
//	m_db->getPageBuffer()->printStatus(cout);
	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(conn);
}

/** 测试主键唯一性条件搜索性能 */
void TableBigTest::testPKeySearch() {
	int numRows = 1000000;
	
	TableDefBuilder tdb(TableDef::INVALID_TABLEID, "testdb", "Test");
	tdb.addColumn("id", CT_BIGINT, false);
	tdb.addColumn("v", CT_INT, false);
	tdb.addIndex("PRIMARY", true, true, false, "id", 0, NULL);
	TableDef *tableDef = tdb.getTableDef();
	
	m_table = new TblInterface(m_db, "Test");
	EXCPT_OPER(m_table->create(tableDef));
	m_table->open();

	Connection *conn = m_db->getConnection(false, __FUNC__);
	Session *session = m_db->getSessionManager()->allocSession(__FUNC__, conn);
	RedRecord rec(m_table->getTableDef());

	cout << "Loading " << numRows << " rows" << endl;

	for (int i = 0; i < numRows; i++) {
		u64 id = i + 1;
		int v = 0;
		rec.writeNumber(0, id)->writeNumber(1, v);
		m_table->getTable()->insert(session, rec.getRecord()->m_data, true, NULL);
		if ((i % 10000) == 9999) {
			cout << "." << flush;
			if ((i % 100000) == 99999) {
				cout << endl;
			}
		}
	}

	int numLookups = 1000000;
	cout << "Do " << numLookups << " lookups" << endl;

	u16 keyCols[1] = {0};
	byte keyDat[Limits::PAGE_SIZE];
	SubRecord key(KEY_PAD, 1, keyCols, keyDat, m_table->getTableDef()->m_indice[0]->m_maxKeySize);

	cout << "Test performance of pure index lookup" << endl;
	u64 before = System::currentTimeMillis();
	for (int n = 0; n < numLookups; n++) {
		u16 id = (System::random() % numRows) + 1;
		rec.writeNumber(0, id);
		RecordOper::extractKeyRP(m_table->getTableDef(), m_table->getTableDef()->m_indice[0], rec.getRecord(), NULL, &key);
		RowId rid;
		RowLockHandle *rlh = NULL;
		m_table->getTable()->getIndice()->getIndex(0)->getByUniqueKey(session, &key, Shared, &rid, NULL, &rlh, NULL);
		if (rlh)
			session->unlockRow(&rlh);		
	}
	u64 after = System::currentTimeMillis();
	cout << "ops: " << (double)numLookups * 1000 / (after - before) << endl;

	cout << "Test performance of Table.indexScan/endScan" << endl;
	u16 numReadCols = 1;
	u16 readCols[1] = {1};
	before = System::currentTimeMillis();
	for (int n = 0; n < numLookups; n++) {
		McSavepoint mcSave(session->getMemoryContext());

		u16 id = (System::random() % numRows) + 1;
		rec.writeNumber(0, id);
		RecordOper::extractKeyRP(m_table->getTableDef(), m_table->getTableDef()->m_indice[0], rec.getRecord(), NULL, &key);
		IndexScanCond cond(0, &key, true, true, true);
		TblScan *scan = m_table->getTable()->indexScan(session, OP_READ, &cond, numReadCols, readCols);
		m_table->getTable()->endScan(scan);
	}
	after = System::currentTimeMillis();
	cout << "ops: " << (double)numLookups * 1000 / (after - before) << endl;
}

/** 测试MMS搜索性能 */
void TableBigTest::testMmsSearch() {
	int numRows = 500000;
	
	TableDefBuilder tdb(TableDef::INVALID_TABLEID, "testdb", "Test");
	tdb.addColumn("id", CT_BIGINT, false);
	tdb.addColumn("v", CT_INT, false);
	tdb.addIndex("PRIMARY", true, true, false, "id", 0, NULL);
	TableDef *tableDef = tdb.getTableDef();
	
	m_table = new TblInterface(m_db, "Test");
	EXCPT_OPER(m_table->create(tableDef));
	m_table->open();

	Connection *conn = m_db->getConnection(false, __FUNC__);
	Session *session = m_db->getSessionManager()->allocSession(__FUNC__, conn);
	RedRecord rec(m_table->getTableDef());

	vector<RowId> rids;
	cout << "Loading " << numRows << " rows" << endl;

	u16 readCols[2] = {0, 1};
	byte buf[Limits::PAGE_SIZE];
	SubToSubExtractor *srExtractor = SubToSubExtractor::createInst(session->getMemoryContext(), m_table->getTableDef(), m_table->getTableDef()->m_indice[0], 1, m_table->getTableDef()->m_indice[0]->m_columns,
		2, readCols, KEY_COMPRESS, REC_REDUNDANT);
	SubRecord sr(REC_REDUNDANT, 2, readCols, buf, m_table->getTableDef()->m_maxRecSize);

	for (int i = 0; i < numRows; i++) {
		u64 id = i + 1;
		int v = 0;
		rec.writeNumber(0, id)->writeNumber(1, v);
		RowId rid = m_table->getTable()->insert(session, rec.getRecord()->m_data, true, NULL);
		rids.push_back(rid);

		if ((i % 10000) == 9999) {
			cout << "." << flush;
			if ((i % 100000) == 99999) {
				cout << endl;
			}
		}
	}

	m_table->selectRows(m_table->buildLRange(0, 1, true, 0), NULL);
	CPPUNIT_ASSERT(m_table->getTable()->getMmsTable()->getStatus().m_records == numRows);
	
	cout << "Test performance of MmsTable.getByRid" << endl;
	u64 before = System::currentTimeMillis();
	int loops = 10;
	for (int l = 0; l < loops; l++) {
		for (int n = 0; n < numRows; n++) {
			MmsRecord *record = m_table->getTable()->getMmsTable()->getByRid(session, rids[n], true, NULL, None);
			m_table->getTable()->getMmsTable()->unpinRecord(session, record);
		}
	}
	u64 after = System::currentTimeMillis();
	cout << "ops: " << (double)numRows * loops * 1000 / (after - before) << endl;
}

