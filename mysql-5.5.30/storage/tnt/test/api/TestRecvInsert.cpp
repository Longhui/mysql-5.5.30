#include "api/TestRecvInsert.h"
#include "api/TestTable.h"
#include "api/Table.h"
#include "api/Database.h"
#include "misc/Session.h"
#include "misc/RecordHelper.h"
#include "misc/Buffer.h"
#include "misc/ControlFile.h"
#include "misc/TableDef.h"
#include "misc/ControlFile.h"
#include "util/File.h"
#include "util/Thread.h"
#include "util/SmartPtr.h"
#include "heap/Heap.h"
#include "lob/Lob.h"
#include "btree/Index.h"
#include <string>
#include "Test.h"
#include "api/TestHelper.h"

using namespace std;
using namespace ntse;

const char* RecvInsertTestCase::getName() {
	return "Insert recovery test.";
}

const char* RecvInsertTestCase::getDescription() {
	return "Test recovery of insert operations.";
}

bool RecvInsertTestCase::isBig() {
	return false;
}

/**
 * 测试不包含大对象的插入恢复流程: 操作正常完成
 */
void RecvInsertTestCase::testSucc() {
	doInsertRecvTest(LOG_MAX, 0, false, 2, false, 2, false);
}

/**
 * 测试不包含大对象的插入恢复流程: 索引插入时由于唯一性冲突失败，操作正常完成
 */
void RecvInsertTestCase::testDup() {
	doInsertRecvTest(LOG_MAX, 0, false, 2, true, 2, false);
}

/**
 * 测试不包含大对象的插入恢复流程: 索引插入时由于唯一性冲突失败，未删除前崩溃
 */
void RecvInsertTestCase::testDupCrashBeforeDelete() {
	doInsertRecvTest(LOG_HEAP_DELETE, 0, false, 2, true, 2);
}

/**
 * 测试不包含大对象的插入恢复流程: 写了事务开始日志后崩溃
 */
void RecvInsertTestCase::testCrashBeforeHeapInsert() {
	doInsertRecvTest(LOG_HEAP_INSERT, 1, false, 2, false, 1);
}

/**
 * 测试不包含大对象的插入恢复流程: 写了堆插入日志后崩溃
 */
void RecvInsertTestCase::testCrashBeforeIndex() {
	doInsertRecvTest(LOG_IDX_DML_BEGIN, 1, false, 2, false, 1);
}

/**
 * 测试不包含大对象的插入恢复流程: 索引插入时完成唯一性索引更新之前崩溃
 */
void RecvInsertTestCase::testCrashBeforeUniqueIndex() {
	doInsertRecvTest(LOG_IDX_DMLDONE_IDXNO, 0, false, 1, false, 0);
}

/**
 * 测试不包含大对象的插入恢复流程: 索引插入时完成唯一性索引更新之后崩溃
 */
void RecvInsertTestCase::testCrashAfterUniqueIndex() {
	doInsertRecvTest(LOG_IDX_DMLDONE_IDXNO, 0, true, 1, false, 1);
}

/**
 * 测试不包含大对象的插入恢复流程: 索引插入时完成第二个索引更新之前崩溃
 */
void RecvInsertTestCase::testCrashBeforeSecondIndex() {
	doInsertRecvTest(LOG_IDX_DMLDONE_IDXNO, 1, false, 1, false, 1);
}

/**
 * 测试不包含大对象的插入恢复流程: 索引插入时完成第二个索引更新之后崩溃
 */
void RecvInsertTestCase::testCrashAfterSecondIndex() {
	doInsertRecvTest(LOG_IDX_DMLDONE_IDXNO, 1, true, 1, false, 1);
}

/**
 * 测试不包含大对象的插入恢复流程: 索引插入时完成第二个索引更新之前崩溃，恢复时再崩溃
 */
void RecvInsertTestCase::testCrashDuringRecoverCrashBeforeSecondIndex() {
	Record **rows = doRecvInsertTestPhase1(1, false, LOG_IDX_DMLDONE_IDXNO, 1, false, 1);

	freeConnectionAndSession();
	closeTable();
	m_db->close(true, false);
	delete m_db;
	m_db = NULL;

	truncateLog(LOG_IDX_DMLDONE_IDXNO, 1);

	restoreTable("BlogCount", false);
	EXCPT_OPER(m_db = Database::open(&m_config, false, 1));
	openTable("BlogCount");
	checkRecords(m_table, rows, 1);

	freeConnectionAndSession();
	closeTable();
	m_db->close(true, false);
	delete m_db;
	m_db = NULL;

	restoreTable("BlogCount", false);
	EXCPT_OPER(m_db = Database::open(&m_config, false, 1));
	openTable("BlogCount");
	checkRecords(m_table, rows, 1);

	freeMySQLRecords(m_table->getTableDef(), rows, 1);
}

/**
 * 测试不包含大对象的插入恢复流程: 索引插入完成，但没写事务结束日志时崩溃
 */
void RecvInsertTestCase::testCrashBeforeEnd() {
	doInsertRecvTest(LOG_TXN_END, 1, false, 2, false, 2);
}

/**
 * 执行不包含大对象的插入恢复流程
 *
 * @param crashOnThisLog 模拟在写此类日志时崩溃的情况，若为LOG_MAX则不截断日志
 * @param nthLog 第几次出现指定日志时崩溃，从0开始编号
 * @param inclusive 为true表示在截断后日志中包含指定的日志，否则不包含指定日志
 * @param insertRows 要插入的记录数
 * @param repeatInsert 是否重复插入记录，造成主键冲突
 * @param succRows 恢复后应成功插入的记录数
 * @param recoverTwice 是否恢复两遍
 */
void RecvInsertTestCase::doInsertRecvTest(LogType crashOnThisLog, int nthLog, bool inclusive, uint insertRows, 
	bool repeatInsert, uint succRows, bool recoverTwice) {
	Record **rows = doRecvInsertTestPhase1(insertRows, repeatInsert, crashOnThisLog, nthLog, inclusive, succRows);

	// 再恢复一次
	if (recoverTwice) {
		closeDatabase();
		
		restoreTable("BlogCount", false);
		EXCPT_OPER(m_db = Database::open(&m_config, false, 1));
		openTable("BlogCount");
		checkRecords(m_table, rows, succRows);
	}
	
	freeMySQLRecords(m_table->getTableDef(), rows, insertRows);
}

Record** RecvInsertTestCase::doRecvInsertTestPhase1(uint insertRows, bool repeatInsert, LogType crashOnThisLog,
	int nthLog, bool inclusive, uint succRows) {
	prepareBlogCount(true);

	backupTable("BlogCount", false);

	// 准备测试数据
	loadConnectionAndSession();

	EXCPT_OPER(m_table = m_db->openTable(m_session, "BlogCount"));
	Record **rows = TableTestCase::populateBlogCount(m_db, m_table, insertRows);
	if (repeatInsert) {
		for (uint n = 0; n < insertRows; n++) {
			uint dupIndex = 0;
			CPPUNIT_ASSERT(m_table->insert(m_session, rows[n]->m_data, true, &dupIndex) == INVALID_ROW_ID);
		}
	}

	closeDatabase();

	// 进行恢复
	truncateLog(crashOnThisLog, nthLog, inclusive);
	restoreTable("BlogCount", false);
	EXCPT_OPER(m_db = Database::open(&m_config, false, 1));

	// 验证恢复结果
	openTable("BlogCount");
	checkRecords(m_table, rows, succRows);

	return rows;
}

/**
 * 测试包含大对象的记录插入恢复流程: 大对象都不是NULL时，操作正常完成
 */
void RecvInsertTestCase::testLobSucc() {
	doInsertLobRecvTest(LOG_MAX, 0, 2, false, 2, true, false);
}

/**
 * 测试包含大对象的记录插入恢复流程: 部分大对象是NULL时，操作正常完成
 */
void RecvInsertTestCase::testLobSuccNull() {
	doInsertLobRecvTest(LOG_MAX, 0, 2, false, 2, false, false);
}

/**
 * 测试包含大对象的记录插入恢复流程: 大对象都不是NULL时，由于唯一性冲突失败，操作正常完成
 */
void RecvInsertTestCase::testLobDup() {
	doInsertLobRecvTest(LOG_MAX, 0, 2, true, 2, true, false);
}

/**
 * 测试包含大对象的记录插入恢复流程: 部分大对象是NULL时，由于唯一性冲突失败，操作正常完成
 */
void RecvInsertTestCase::testLobDupNull() {
	doInsertLobRecvTest(LOG_MAX, 0, 2, true, 2, false, false);
}

/**
 * 测试包含大对象的记录插入恢复流程: 大对象都不是NULL时，插入大对象过程中崩溃
 */
void RecvInsertTestCase::testLobCrashDuringLob() {
	doInsertLobRecvTest(LOG_LOB_INSERT, 0, 1, false, 0, true, true);
}

/**
 * 测试包含大对象的记录插入恢复流程: 部分大对象是NULL时，插入大对象过程中崩溃
 */
void RecvInsertTestCase::testLobCrashDuringLobNull() {
	doInsertLobRecvTest(LOG_HEAP_INSERT, 0, 2, false, 0, false, true);
}

/**
 * 测试包含大对象的记录插入恢复流程: 插入大对象完成，未插入堆时崩溃
 */
void RecvInsertTestCase::testLobCrashBeforeHeapInsert() {
	doInsertLobRecvTest(LOG_HEAP_INSERT, 4, 2, false, 1, true, true);
}

/**
 * 测试包含大对象的记录插入恢复流程: 堆插入后崩溃
 */
void RecvInsertTestCase::testLobCrashBeforeIndex() {
	doInsertLobRecvTest(LOG_IDX_DML_BEGIN, 1, 2, false, 1, true, true);
}

/**
 * 测试包含大对象的记录插入恢复流程: 索引插入过程中未完成唯一性索引时崩溃
 */
void RecvInsertTestCase::testLobCrashDuringIndex() {
	doInsertLobRecvTest(LOG_IDX_DMLDONE_IDXNO, 3, 2, false, 1, true, true);
}

/**
 * 执行包含大对象的插入恢复流程
 *
 * @param crashOnThisLog 模拟在写此类日志时崩溃的情况，若为LOG_MAX则不截断日志
 * @param nthLog 第几次出现指定日志时崩溃，从0开始编号
 * @param insertRows 要插入的记录数
 * @param repeatInsert 是否重复插入记录，造成主键冲突
 * @param succRows 恢复后应成功插入的记录数
 * @param lobNotNull 大对象是否全部不为NULL
 * @param recoverTwice 是否恢复两遍
 */
void RecvInsertTestCase::doInsertLobRecvTest(LogType crashOnThisLog, int nthLog, uint insertRows, 
	bool repeatInsert, uint succRows, bool lobNotNull, bool recoverTwice) {
	prepareBlog(true);

	backupTable("Blog", true);

	// 准备测试数据
	loadConnectionAndSession();

	EXCPT_OPER(m_table = m_db->openTable(m_session, "Blog"));
	CPPUNIT_ASSERT(m_table->getTableDef()->m_numIndice == 4);
	CPPUNIT_ASSERT(m_table->getTableDef()->getNumIndice(true) == 1);
	Record **rows = TableTestCase::populateBlog(m_db, m_table, insertRows, false, lobNotNull);

	Record **redRows = new Record* [insertRows];
	for (int i = 0; i < insertRows; i++) {
		byte *data = new byte[m_table->getTableDef()->m_maxRecSize];
		redRows[i] = new Record(INVALID_ROW_ID, REC_MYSQL, data, m_table->getTableDef()->m_maxRecSize);
		RecordOper::convertRecordMUpToEngine(m_table->getTableDef(), rows[i], redRows[i]);
	}

	if (repeatInsert) {
		for (uint n = 0; n < insertRows; n++) {
			uint dupIndex = 0;
			CPPUNIT_ASSERT(m_table->insert(m_session, rows[n]->m_data, false, &dupIndex) == INVALID_ROW_ID);
		}
	}
	
	closeDatabase();

	// 进行恢复
	truncateLog(crashOnThisLog, nthLog);
	restoreTable("Blog", true);
	EXCPT_OPER(m_db = Database::open(&m_config, false, 1));
	
	// 验证恢复结果
	openTable("Blog");
	checkRecords(m_table, redRows, succRows);

	// 再恢复一次
	if (recoverTwice) {
		closeDatabase();
		
		restoreTable("Blog", true);
		EXCPT_OPER(m_db = Database::open(&m_config, false, 1));
		openTable("Blog");
		checkRecords(m_table, redRows, succRows);
	}
	
	freeUppMySQLRecords(m_table->getTableDef(), rows, insertRows);
	for(int i = 0; i < insertRows; i++) {
		freeRecord(redRows[i]);
	}
	delete []redRows;
}

/** 测试表中没有索引时的情况 */
void RecvInsertTestCase::testNoIdx() {
	TableDef *tableDef = TableTestCase::getBlogCountDef(false);
	while (tableDef->m_numIndice > 0)
		tableDef->removeIndex(0);

	Database::create(&m_config);
	EXCPT_OPER(m_db = Database::open(&m_config, false));

	TblInterface ti(m_db, "BlogCount");
	EXCPT_OPER(ti.create(tableDef));
	ti.checkpoint();
	string bakPath = ti.backup(tableDef->hasLob());

	EXCPT_OPER(ti.open());
	CPPUNIT_ASSERT(ti.insertRow(NULL, (s64)1, 3, (s16)6, 23, "title 1", (s64)1));
	ResultSet *rs1 = ti.selectRows(NULL);

	ti.close(true);
	ti.restore(bakPath, tableDef->hasLob());
	m_db->close(true, false);
	delete m_db;
	m_db = NULL;

	EXCPT_OPER(m_db = Database::open(&m_config, false, 1));
	ti.reconnect(m_db);
	EXCPT_OPER(ti.open());

	ResultSet *rs2 = ti.selectRows(NULL);
	CPPUNIT_ASSERT(*rs1 == *rs2);
	ti.close(true);

	delete tableDef;
	delete rs1;
	delete rs2;
}

/** 测试索引SMO的恢复 */
void RecvInsertTestCase::testIdxSmo() {
	const int IDX_ATTR_SIZE = Limits::PAGE_SIZE / 10;
	TableDefBuilder tdb(TableDef::INVALID_TABLEID, "space", "BlogCount");
	tdb.addColumn("id", CT_INT, false);
	tdb.addColumnS("a", CT_CHAR, IDX_ATTR_SIZE, false);
	tdb.addIndex("pkey", true, true, false, "id", 0, NULL);
	tdb.addIndex("a", false, false, false, "a", 0, NULL);
	TableDef *tableDef = tdb.getTableDef();

	Database::create(&m_config);
	EXCPT_OPER(m_db = Database::open(&m_config, false));
	TblInterface ti(m_db, "BlogCount");
	EXCPT_OPER(ti.create(tableDef));
	ti.checkpoint();

	ti.open();

	string datBak = ti.backup(tableDef->hasLob());
	CPPUNIT_ASSERT(File::copyFile("dbtestdir/ntse_ctrl.bak", "dbtestdir/ntse_ctrl", true) == File::E_NO_ERROR);

	uint n = 20;
	for (uint i = 0; i < n; i++) {
		byte buf[IDX_ATTR_SIZE];
		memset(buf, 0, sizeof(buf));
		buf[0] = (byte)i;
		ti.insertRow(NULL, i, buf);
	}
	m_db->getTxnlog()->flush(m_db->getTxnlog()->tailLsn());

	ti.close(true);
	closeDatabase(true, false);

	// 成功
	EXCPT_OPER(m_db = Database::open(&m_config, false, 1));
	ti.reconnect(m_db);
	ti.open();
	ti.verify();

	ti.close(true);
	closeDatabase(true, false);

	// 作SMO时失败
	ti.restore(datBak, tableDef->hasLob(), "dbtestdir/BlogCount");
	CPPUNIT_ASSERT(File::copyFile("dbtestdir/ntse_ctrl", "dbtestdir/ntse_ctrl.bak", true) == File::E_NO_ERROR);
	truncateLog(LOG_IDX_SMO, 1, true);

	EXCPT_OPER(m_db = Database::open(&m_config, false, 1));
	ti.reconnect(m_db);
	ti.open();
	ti.verify();

	ti.close(true);
	closeDatabase(true, false);

	EXCPT_OPER(m_db = Database::open(&m_config, false, 1));
	ti.reconnect(m_db);
	ti.open();
	ti.verify();

	ti.close(true);
	closeDatabase(true, false);
	delete tableDef;
}

void RecvInsertTestCase::testIOT() {
	TableDefBuilder tdb(TableDef::INVALID_TABLEID, "space", "BlogCount");
	tdb.addColumn("a", CT_INT, false);
	tdb.addColumn("b", CT_INT, false);
	tdb.addIndex("pkey", true, true, false, "a", 0, "b", 0, NULL);
	TableDef *tableDef = tdb.getTableDef();
	tableDef->m_indexOnly = true;
	tableDef->m_useMms = false;

	Database::create(&m_config);
	EXCPT_OPER(m_db = Database::open(&m_config, false));
	TblInterface ti(m_db, "BlogCount");
	EXCPT_OPER(ti.create(tableDef));
	ti.checkpoint();

	ti.open();

	string datBak = ti.backup(tableDef->hasLob());
	CPPUNIT_ASSERT(File::copyFile("dbtestdir/ntse_ctrl.bak", "dbtestdir/ntse_ctrl", true) == File::E_NO_ERROR);

	ti.insertRow(NULL, 1, 1);
	ResultSet *rs1 = ti.selectRows(ti.buildLRange(0, 1, true, 0), NULL);
	CPPUNIT_ASSERT(rs1->getNumRows() == 1);
	m_db->getTxnlog()->flush(m_db->getTxnlog()->tailLsn());

	ti.close(true);
	closeDatabase(true, false);

	// 成功
	EXCPT_OPER(m_db = Database::open(&m_config, false, 1));
	ti.reconnect(m_db);
	ti.open();
	ResultSet *rs2 = ti.selectRows(ti.buildLRange(0, 1, true, 0), NULL);
	CPPUNIT_ASSERT(*rs1 == *rs2);
	delete rs2;

	ti.close(true);
	closeDatabase(true, false);

	// 索引操作完成，未写END日志时失败
	ti.restore(datBak, tableDef->hasLob(), "dbtestdir/BlogCount");
	CPPUNIT_ASSERT(File::copyFile("dbtestdir/ntse_ctrl", "dbtestdir/ntse_ctrl.bak", true) == File::E_NO_ERROR);
	truncateLog(LOG_IDX_DML_END, 0, false);

	EXCPT_OPER(m_db = Database::open(&m_config, false, 1));
	ti.reconnect(m_db);
	ti.open();
	ResultSet *rs3 = ti.selectRows(ti.buildLRange(0, 1, true, 0), NULL);
	CPPUNIT_ASSERT(*rs1 == *rs3);
	delete rs3;

	ti.close(true);
	closeDatabase(true, false);

	// 索引操作过程中失败
	ti.restore(datBak, tableDef->hasLob(), "dbtestdir/BlogCount");
	CPPUNIT_ASSERT(File::copyFile("dbtestdir/ntse_ctrl", "dbtestdir/ntse_ctrl.bak", true) == File::E_NO_ERROR);
	truncateLog(LOG_IDX_DMLDONE_IDXNO, 0, false);

	EXCPT_OPER(m_db = Database::open(&m_config, false, 1));
	ti.reconnect(m_db);
	ti.open();
	ResultSet *rs4 = ti.selectRows(ti.buildLRange(0, 1, true, 0), NULL);
	CPPUNIT_ASSERT(rs4->getNumRows() == 0);
	delete rs4;

	ti.close(true);
	closeDatabase(true, false);

	delete rs1;
	delete tableDef;
}

void RecvInsertTestCase::testIOTDup() {
	TableDefBuilder tdb(TableDef::INVALID_TABLEID, "space", "BlogCount");
	tdb.addColumn("a", CT_INT, false);
	tdb.addColumn("b", CT_INT, false);
	tdb.addIndex("pkey", true, true, false, "a", 0, "b", 0, NULL);
	TableDef *tableDef = tdb.getTableDef();
	tableDef->m_indexOnly = true;
	tableDef->m_useMms = false;

	Database::create(&m_config);
	EXCPT_OPER(m_db = Database::open(&m_config, false));
	TblInterface ti(m_db, "BlogCount");
	EXCPT_OPER(ti.create(tableDef));
	ti.checkpoint();

	ti.open();

	string datBak = ti.backup(tableDef->hasLob());

	ti.insertRow(NULL, 1, 1);
	ti.insertRow(NULL, 1, 1);
	ResultSet *rs1 = ti.selectRows(ti.buildLRange(0, 1, true, 0), NULL);
	CPPUNIT_ASSERT(rs1->getNumRows() == 1);
	m_db->getTxnlog()->flush(m_db->getTxnlog()->tailLsn());

	CPPUNIT_ASSERT(File::copyFile("dbtestdir/ntse_ctrl.bak", "dbtestdir/ntse_ctrl", true) == File::E_NO_ERROR);

	ti.close(true);
	closeDatabase(true, false);

	// 成功
	EXCPT_OPER(m_db = Database::open(&m_config, false, 1));
	ti.reconnect(m_db);
	ti.open();
	ResultSet *rs2 = ti.selectRows(ti.buildLRange(0, 1, true, 0), NULL);
	CPPUNIT_ASSERT(*rs1 == *rs2);
	delete rs1;
	delete rs2;

	ti.close(true);
	closeDatabase(true, false);

	delete tableDef;
}
