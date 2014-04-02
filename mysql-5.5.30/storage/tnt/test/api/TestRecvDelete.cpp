#include "api/TestRecvDelete.h"
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

const char* RecvDeleteTestCase::getName() {
	return "Delete recovery test.";
}

const char* RecvDeleteTestCase::getDescription() {
	return "Test recovery of delete operations.";
}

bool RecvDeleteTestCase::isBig() {
	return false;
}

/**
 * ���Բ�����������ɾ���ָ�����: �����������
 */
void RecvDeleteTestCase::testSucc() {
	doDeleteRecvTest(LOG_MAX, 0, 2, 2, false);
}

/**
 * ���Բ�����������ɾ���ָ�����: ����տ�ʼ������
 */
void RecvDeleteTestCase::testCrashAfterBegin() {
	doDeleteRecvTest(LOG_PRE_DELETE, 1, 2, 1, true);
}

/**
 * ���Բ�����������ɾ���ָ�����: ɾ������֮ǰ����
 */
void RecvDeleteTestCase::testCrashBeforeIndex() {
	doDeleteRecvTest(LOG_IDX_DML_BEGIN, 1, 2, 1, true);
}

/**
 * ���Բ�����������ɾ���ָ�����: ɾ���������Ψһ����������֮ǰ����
 */
void RecvDeleteTestCase::testCrashBeforeUniqueIndex() {
	doDeleteRecvTest(LOG_IDX_DMLDONE_IDXNO, 2, 2, 1, true);
}

/**
 * ���Բ�����������ɾ���ָ�����: ɾ���������Ψһ������֮��֮ǰ����
 */
void RecvDeleteTestCase::testCrashAfterUniqueIndex() {
	doDeleteRecvTest(LOG_IDX_DMLDONE_IDXNO, 3, 2, 2, true);
}

/**
 * ���Բ�����������ɾ���ָ�����: ɾ�����м�¼֮ǰ����
 */
void RecvDeleteTestCase::testCrashBeforeHeap() {
	doDeleteRecvTest(LOG_HEAP_DELETE, 1, 2, 2, true);
}

/**
 * ִ�в�����������ɾ���ָ�����
 *
 * @param crashOnThisLog ģ����д������־ʱ�������������ΪLOG_MAX�򲻽ض���־
 * @param nthLog �ڼ��γ���ָ����־ʱ��������0��ʼ���
 * @param insertRows Ҫ����ļ�¼��
 * @param succRows �ָ���Ӧ�ɹ�ɾ���ļ�¼��
 * @param recoverTwice �Ƿ�ָ�����
 */
void RecvDeleteTestCase::doDeleteRecvTest(LogType crashOnThisLog, int nthLog, uint insertRows, 
	uint succRows, bool recoverTwice) {
	prepareBlogCount(true);

	// ׼����������
	loadConnectionAndSession();

	EXCPT_OPER(m_table = m_db->openTable(m_session, "BlogCount"));
	Record **rows = TableTestCase::populateBlogCount(m_db, m_table, insertRows);

	m_db->checkpoint(m_session);

	backupTable("BlogCount", false);

	deleteAllRecords(m_session, "BlogID");

	closeDatabase();

	// ���лָ�
	truncateLog(crashOnThisLog, nthLog);
	restoreTable("BlogCount", false);
	EXCPT_OPER(m_db = Database::open(&m_config, false, 1));
	
	// ��֤�ָ����
	openTable("BlogCount");
	checkRecords(m_table, rows + (insertRows - succRows), insertRows - succRows);

	// �ٻָ�һ��
	if (recoverTwice) {
		closeDatabase();
		
		restoreTable("BlogCount", false);
		EXCPT_OPER(m_db = Database::open(&m_config, false, 1));
		openTable("BlogCount");
		checkRecords(m_table, rows + (insertRows - succRows), insertRows - succRows);
	}
	
	freeMySQLRecords(m_table->getTableDef(), rows, insertRows);
}

/**
 * ���Բ�����������ɾ���ָ�����: �ָ�ɾ������ʱ����MMS�м�¼
 */
void RecvDeleteTestCase::testDeleteHitMms() {
	prepareBlogCount(true);

	// ׼����������
	loadConnectionAndSession();

	EXCPT_OPER(m_table = m_db->openTable(m_session, "BlogCount"));
	uint insertRows = 2, succRows = 2;
	Record **rows = TableTestCase::populateBlogCount(m_db, m_table, insertRows);

	m_db->checkpoint(m_session);

	backupTable("BlogCount", false);

	{
		SubRecordBuilder srb(m_table->getTableDef(), REC_REDUNDANT);
		SubRecord *subRec = srb.createEmptySbByName(m_table->getTableDef()->m_maxRecSize, "Title");
		SubRecordBuilder keyBuilder(m_table->getTableDef(), KEY_PAD);
		TblScan *scanHandle;
		SubRecord *key = NULL;
		u64 id = 0;
		key = keyBuilder.createSubRecordByName("BlogID", &id);
		key->m_rowId = INVALID_ROW_ID;
		char *title = randomStr(32);
		SubRecord *updateRec = srb.createSubRecordByName("Title", title);
		IndexScanCond cond(0, key, true, true, false);
		scanHandle = m_table->indexScan(m_session, OP_UPDATE, &cond, 
			subRec->m_numCols, subRec->m_columns);
		scanHandle->setUpdateColumns(updateRec->m_numCols, updateRec->m_columns);
		for (uint i = 0; i < insertRows; i++) {
			CPPUNIT_ASSERT(m_table->getNext(scanHandle, subRec->m_data));
			m_table->updateCurrent(scanHandle, subRec->m_data);

			rows[i]->m_format = REC_REDUNDANT;
			RecordOper::updateRecordRR(m_table->getTableDef(), rows[i], updateRec);
			rows[i]->m_format = REC_MYSQL;
		}
		CPPUNIT_ASSERT(!m_table->getNext(scanHandle, subRec->m_data));
		m_table->endScan(scanHandle);

		freeSubRecord(subRec);
		freeSubRecord(key);
		freeSubRecord(updateRec);
		delete [] title;
	}
	deleteAllRecords(m_session, "BlogID");

	closeDatabase();

	// ���лָ�
	truncateLog(LOG_HEAP_DELETE, 1);
	restoreTable("BlogCount", false);
	EXCPT_OPER(m_db = Database::open(&m_config, false, 1));

	// ��֤�ָ����
	openTable("BlogCount");
	checkRecords(m_table, rows + (insertRows - succRows), insertRows - succRows);

	// �ٻָ�һ��
	{
		closeDatabase();

		restoreTable("BlogCount", false);
		EXCPT_OPER(m_db = Database::open(&m_config, false, 1));
		openTable("BlogCount");
		checkRecords(m_table, rows + (insertRows - succRows), insertRows - succRows);
	}

	freeMySQLRecords(m_table->getTableDef(), rows, insertRows);
}

/**
 * ���԰���������ɾ���ָ�����: �ָ�ɾ������ʱ����MMS�м�¼
 */
void RecvDeleteTestCase::testDeleteLobHitMms() {
	prepareBlog(true);

	// ׼����������
	loadConnectionAndSession();

	EXCPT_OPER(m_table = m_db->openTable(m_session, "Blog"));
	uint insertRows = 2, succRows = 1;
	Record **rows = TableTestCase::populateBlog(m_db, m_table, insertRows, false, true);
	Record **redRows = new Record* [insertRows];
	for (int i = 0; i < insertRows; i++) {
		byte *data = new byte[m_table->getTableDef()->m_maxRecSize];
		redRows[i] = new Record(INVALID_ROW_ID, REC_MYSQL, data, m_table->getTableDef()->m_maxRecSize);
		RecordOper::convertRecordMUpToEngine(m_table->getTableDef(), rows[i], redRows[i]);
	}

	m_db->checkpoint(m_session);

	backupTable("Blog", true);

	{
		SubRecordBuilder srb(m_table->getTableDef(), REC_REDUNDANT);
		SubRecord *subRec = srb.createEmptySbByName(m_table->getTableDef()->m_maxRecSize, "Title");
		SubRecordBuilder keyBuilder(m_table->getTableDef(), KEY_PAD);
		TblScan *scanHandle;
		SubRecord *key = NULL;
		u64 id = 0;
		key = keyBuilder.createSubRecordByName("ID", &id);
		key->m_rowId = INVALID_ROW_ID;
		char *title = randomStr(32);
		SubRecord *updateRec = srb.createSubRecordByName("Title", title);
		IndexScanCond cond(0, key, true, true, false);
		scanHandle = m_table->indexScan(m_session, OP_UPDATE, &cond, 
			subRec->m_numCols, subRec->m_columns);
		scanHandle->setUpdateColumns(updateRec->m_numCols, updateRec->m_columns);
		for (uint i = 0; i < insertRows; i++) {
			CPPUNIT_ASSERT(m_table->getNext(scanHandle, subRec->m_data));
			m_table->updateCurrent(scanHandle, updateRec->m_data);

			redRows[i]->m_format = REC_REDUNDANT;
			RecordOper::updateRecordRR(m_table->getTableDef(), redRows[i], updateRec);
			redRows[i]->m_format = REC_MYSQL;
		}
		CPPUNIT_ASSERT(!m_table->getNext(scanHandle, subRec->m_data));
		m_table->endScan(scanHandle);

		freeSubRecord(subRec);
		freeSubRecord(key);
		freeSubRecord(updateRec);
		delete [] title;
	}
	deleteAllRecords(m_session, "ID");

	closeDatabase();

	// ���лָ�
	truncateLog(LOG_LOB_DELETE, 0);
	restoreTable("Blog", true);
	EXCPT_OPER(m_db = Database::open(&m_config, false, 1));

	// ��֤�ָ����
	openTable("Blog");
	checkRecords(m_table, redRows + (insertRows - succRows), insertRows - succRows);

	// �ٻָ�һ��
	{
		closeDatabase();

		restoreTable("Blog", true);
		EXCPT_OPER(m_db = Database::open(&m_config, false, 1));
		openTable("Blog");
		checkRecords(m_table, redRows + (insertRows - succRows), insertRows - succRows);
	}

	freeEngineMySQLRecords(m_table->getTableDef(), redRows, insertRows);
	for(int i = 0; i < insertRows; i++) {
		freeRecord(rows[i]);
	}
	delete []rows;
}

/**
 * ���԰��������ļ�¼ɾ���ָ�����: ����󶼲���NULLʱ�������������
 */
void RecvDeleteTestCase::testLobSucc() {
	doDeleteLobRecvTest(LOG_MAX, 0, 2, 2, true, false);
}

/**
 * ���԰��������ļ�¼ɾ���ָ�����: ���ִ������NULLʱ�������������
 */
void RecvDeleteTestCase::testLobSuccNull() {
	doDeleteLobRecvTest(LOG_MAX, 0, 2, 2, false, false);
}

/**
 * ���԰��������ļ�¼����ָ�����: ���ִ������NULLʱ��ɾ�����������б���
 */
void RecvDeleteTestCase::testLobCrashDuringIndex() {
	doDeleteLobRecvTest(LOG_IDX_DML_END, 1, 2, 2, false, true);
}

/**
 * ���԰��������ļ�¼����ָ�����: ���ִ������NULLʱ��ɾ�������֮ǰ����
 */
void RecvDeleteTestCase::testLobCrashBeforeDeleteLob() {
	doDeleteLobRecvTest(LOG_LOB_DELETE, 0, 2, 1, false, true);
}

/**
 * ���԰��������ļ�¼����ָ�����: ���ִ������NULLʱ��ɾ�����м�¼ǰ����
 */
void RecvDeleteTestCase::testLobCrashBeforeDeleteHeap() {
	doDeleteLobRecvTest(LOG_HEAP_DELETE, 0, 2, 1, false, true);
}

/**
 * ִ�а���������ɾ���ָ�����
 *
 * @param crashOnThisLog ģ����д������־ʱ�������������ΪLOG_MAX�򲻽ض���־
 * @param nthLog �ڼ��γ���ָ����־ʱ��������0��ʼ���
 * @param insertRows Ҫ����ļ�¼��
 * @param succRows �ָ���Ӧ�ɹ�ɾ���ļ�¼��
 * @param lobNotNull ������Ƿ�ȫ����ΪNULL
 * @param recoverTwice �Ƿ�ָ�����
 */
void RecvDeleteTestCase::doDeleteLobRecvTest(LogType crashOnThisLog, int nthLog, uint insertRows, 
	uint succRows, bool lobNotNull, bool recoverTwice) {
	prepareBlog(true);

	// ׼����������
	loadConnectionAndSession();

	EXCPT_OPER(m_table = m_db->openTable(m_session, "Blog"));
	Record **rows = TableTestCase::populateBlog(m_db, m_table, insertRows, false, lobNotNull);
	Record **redRows = new Record* [insertRows];
	for (int i = 0; i < insertRows; i++) {
		byte *data = new byte[m_table->getTableDef()->m_maxRecSize];
		redRows[i] = new Record(INVALID_ROW_ID, REC_MYSQL, data, m_table->getTableDef()->m_maxRecSize);
		RecordOper::convertRecordMUpToEngine(m_table->getTableDef(), rows[i], redRows[i]);
	}


	m_db->checkpoint(m_session);

	backupTable("Blog", true);

	deleteAllRecords(m_session, "ID");

	closeDatabase();

	// ���лָ�
	truncateLog(crashOnThisLog, nthLog);
	restoreTable("Blog", true);
	EXCPT_OPER(m_db = Database::open(&m_config, false, 1));
	
	// ��֤�ָ����
	openTable("Blog");
	checkRecords(m_table, redRows + (insertRows - succRows), insertRows - succRows);

	// �ٻָ�һ��
	if (recoverTwice) {
		closeDatabase();
		
		restoreTable("Blog", true);
		EXCPT_OPER(m_db = Database::open(&m_config, false, 1));
		openTable("Blog");
		checkRecords(m_table, redRows + (insertRows - succRows), insertRows - succRows);
	}
	
	freeEngineMySQLRecords(m_table->getTableDef(), redRows, insertRows);
	for(int i = 0; i < insertRows; i++) {
		freeRecord(rows[i]);
	}
	delete []rows;
}

void RecvDeleteTestCase::deleteAllRecords(Session *session, const char *pkeyCol) {
	SubRecordBuilder srb(m_table->getTableDef(), REC_REDUNDANT);
	SubRecord *subRec = srb.createEmptySbByName(m_table->getTableDef()->m_maxRecSize, "Title");
	SubRecordBuilder keyBuilder(m_table->getTableDef(), KEY_PAD);
	TblScan *scanHandle;
	SubRecord *key = NULL;
	u64 id = 0;
	key = keyBuilder.createSubRecordByName(pkeyCol, &id);
	key->m_rowId = INVALID_ROW_ID;
	IndexScanCond cond(0, key, true, true, false);
	scanHandle = m_table->indexScan(session, OP_DELETE, &cond, 
		subRec->m_numCols, subRec->m_columns);
	while (m_table->getNext(scanHandle, subRec->m_data))
		m_table->deleteCurrent(scanHandle);
	m_table->endScan(scanHandle);
	freeSubRecord(subRec);
	freeSubRecord(key);
}

/** ���Ա���û��������û�д����ʱ����� */
void RecvDeleteTestCase::testNoIdx() {
	TableDef *tableDef = TableTestCase::getBlogCountDef(false);
	while (tableDef->m_numIndice > 0)
		tableDef->removeIndex(0);

	Database::create(&m_config);
	EXCPT_OPER(m_db = Database::open(&m_config, false));

	TblInterface ti(m_db, "BlogCount");
	EXCPT_OPER(ti.create(tableDef));
	EXCPT_OPER(ti.open());
	CPPUNIT_ASSERT(ti.insertRow(NULL, (s64)1, 3, (s16)6, 23, "title 1", (s64)1));

	ti.checkpoint();

	string bakPath = ti.backup(tableDef->hasLob());

	CPPUNIT_ASSERT(ti.deleteRows() == 1);

	ti.close(true);
	ti.restore(bakPath, tableDef->hasLob());
	m_db->close(true, false);
	delete m_db;
	m_db = NULL;

	EXCPT_OPER(m_db = Database::open(&m_config, false, 1));
	ti.reconnect(m_db);
	EXCPT_OPER(ti.open());

	ResultSet *rs = ti.selectRows(NULL);
	CPPUNIT_ASSERT(rs->getNumRows() == 0);
	ti.close(true);

	delete tableDef;
	delete rs;
}

/** ���Ա���û���������д����ʱ����� */
void RecvDeleteTestCase::testNoIdxLob() {
	TableDef *tableDef = TableTestCase::getBlogDef(false);
	while (tableDef->m_numIndice > 0)
		tableDef->removeIndex(0);

	Database::create(&m_config);
	EXCPT_OPER(m_db = Database::open(&m_config, false));

	TblInterface ti(m_db, "Blog");
	EXCPT_OPER(ti.create(tableDef));
	EXCPT_OPER(ti.open());
	CPPUNIT_ASSERT(ti.insertRow(NULL, (s64)1, (s64)1, (s64)System::currentTimeMillis(), "title 1", "tags", "abstract", "content"));

	ti.checkpoint();
	string bakPath = ti.backup(tableDef->hasLob());

	CPPUNIT_ASSERT(ti.deleteRows() == 1);

	ti.close(true);
	ti.restore(bakPath, tableDef->hasLob());
	m_db->close(true, false);
	delete m_db;
	m_db = NULL;

	EXCPT_OPER(m_db = Database::open(&m_config, false, 1));
	ti.reconnect(m_db);
	EXCPT_OPER(ti.open());

	ResultSet *rs = ti.selectRows(NULL);
	CPPUNIT_ASSERT(rs->getNumRows() == 0);
	ti.close(true);

	delete tableDef;
	delete rs;
}

void RecvDeleteTestCase::testIOT() {
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
	ti.deleteRows(ti.buildLRange(0, 1, true, 0));
	ResultSet *rs2 = ti.selectRows(ti.buildLRange(0, 1, true, 0), NULL);
	CPPUNIT_ASSERT(rs2->getNumRows() == 0);
	m_db->getTxnlog()->flush(m_db->getTxnlog()->tailLsn());

	ti.close(true);
	closeDatabase(true, false);

	// �ɹ�
	EXCPT_OPER(m_db = Database::open(&m_config, false, 1));
	ti.reconnect(m_db);
	ti.open();
	ResultSet *rs3 = ti.selectRows(ti.buildLRange(0, 1, true, 0), NULL);
	CPPUNIT_ASSERT(*rs3 == *rs2);
	delete rs3;

	ti.close(true);
	closeDatabase(true, false);

	// DELETE����������ɣ�δдEND��־ʱʧ��
	ti.restore(datBak, tableDef->hasLob(), "dbtestdir/BlogCount");
	CPPUNIT_ASSERT(File::copyFile("dbtestdir/ntse_ctrl", "dbtestdir/ntse_ctrl.bak", true) == File::E_NO_ERROR);
	truncateLog(LOG_IDX_DML_END, 1, false);

	EXCPT_OPER(m_db = Database::open(&m_config, false, 1));
	ti.reconnect(m_db);
	ti.open();
	ResultSet *rs4 = ti.selectRows(ti.buildLRange(0, 1, true, 0), NULL);
	CPPUNIT_ASSERT(*rs4 == *rs2);
	delete rs4;

	ti.close(true);
	closeDatabase(true, false);

	// DELETE��������������ʧ��
	ti.restore(datBak, tableDef->hasLob(), "dbtestdir/BlogCount");
	CPPUNIT_ASSERT(File::copyFile("dbtestdir/ntse_ctrl", "dbtestdir/ntse_ctrl.bak", true) == File::E_NO_ERROR);
	truncateLog(LOG_IDX_DMLDONE_IDXNO, 1, false);

	EXCPT_OPER(m_db = Database::open(&m_config, false, 1));
	ti.reconnect(m_db);
	ti.open();
	ResultSet *rs5 = ti.selectRows(ti.buildLRange(0, 1, true, 0), NULL);
	CPPUNIT_ASSERT(*rs5 == *rs1);
	delete rs5;

	ti.close(true);
	closeDatabase(true, false);

	delete rs1;
	delete rs2;
	delete tableDef;
}

void RecvDeleteTestCase::testIOTVarlen() {
	TableDefBuilder tdb(TableDef::INVALID_TABLEID, "space", "BlogCount");
	tdb.addColumn("a", CT_INT, false);
	tdb.addColumnS("b", CT_VARCHAR, 16, false, false);
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

	ti.insertRow(NULL, 1, "aaabbb");
	ResultSet *rs1 = ti.selectRows(ti.buildLRange(0, 1, true, 0), NULL);
	CPPUNIT_ASSERT(rs1->getNumRows() == 1);
	ti.deleteRows(ti.buildLRange(0, 1, true, 0));
	ResultSet *rs2 = ti.selectRows(ti.buildLRange(0, 1, true, 0), NULL);
	CPPUNIT_ASSERT(rs2->getNumRows() == 0);
	m_db->getTxnlog()->flush(m_db->getTxnlog()->tailLsn());

	ti.close(true);
	closeDatabase(true, false);

	// �ɹ�
	EXCPT_OPER(m_db = Database::open(&m_config, false, 1));
	ti.reconnect(m_db);
	ti.open();
	ResultSet *rs3 = ti.selectRows(ti.buildLRange(0, 1, true, 0), NULL);
	CPPUNIT_ASSERT(*rs3 == *rs2);
	delete rs3;

	ti.close(true);
	closeDatabase(true, false);

	// DELETE����������ɣ�δдEND��־ʱʧ��
	ti.restore(datBak, tableDef->hasLob(), "dbtestdir/BlogCount");
	CPPUNIT_ASSERT(File::copyFile("dbtestdir/ntse_ctrl", "dbtestdir/ntse_ctrl.bak", true) == File::E_NO_ERROR);
	truncateLog(LOG_IDX_DML_END, 1, false);

	EXCPT_OPER(m_db = Database::open(&m_config, false, 1));
	ti.reconnect(m_db);
	ti.open();
	ResultSet *rs4 = ti.selectRows(ti.buildLRange(0, 1, true, 0), NULL);
	CPPUNIT_ASSERT(*rs4 == *rs2);
	delete rs4;

	ti.close(true);
	closeDatabase(true, false);

	// DELETE��������������ʧ��
	ti.restore(datBak, tableDef->hasLob(), "dbtestdir/BlogCount");
	CPPUNIT_ASSERT(File::copyFile("dbtestdir/ntse_ctrl", "dbtestdir/ntse_ctrl.bak", true) == File::E_NO_ERROR);
	truncateLog(LOG_IDX_DMLDONE_IDXNO, 1, false);

	EXCPT_OPER(m_db = Database::open(&m_config, false, 1));
	ti.reconnect(m_db);
	ti.open();
	ResultSet *rs5 = ti.selectRows(ti.buildLRange(0, 1, true, 0), NULL);
	CPPUNIT_ASSERT(*rs5 == *rs1);
	delete rs5;

	ti.close(true);
	closeDatabase(true, false);

	delete rs1;
	delete rs2;
	delete tableDef;
}
