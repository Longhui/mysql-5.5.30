#include "api/TestRecvDDL.h"
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
#include "api/TestHelper.h"
#include <string>
#include "Test.h"

using namespace std;
using namespace ntse;

const char* RecvDDLTestCase::getName() {
	return "DDL recovery test.";
}

const char* RecvDDLTestCase::getDescription() {
	return "Test recovery of DDL operations.";
}

bool RecvDDLTestCase::isBig() {
	return false;
}

/**
 * ����CREATE TABLE�����Ļָ�����: �����Ѿ��ɹ�Ӧ�����������
 */
void RecvDDLTestCase::testRecvCreateTableSucc() {
	Database::create(&m_config);
	EXCPT_OPER(m_db = Database::open(&m_config, false));

	TblInterface ti(m_db, "BlogCount");
	TableDef *tblDef = TableTestCase::getBlogCountDef(true);
	ti.create(tblDef);
	delete tblDef;
	tblDef = NULL;
	
	m_db->close(true, false);
	delete m_db;
	m_db = NULL;

	// �����ݿ�ǿ�ƽ��лָ�
	EXCPT_OPER(m_db = Database::open(&m_config, false, 1));

	ti.reconnect(m_db);
	EXCPT_OPER(ti.open());
	ti.close(true);
}

class CreateTableThread: public Thread {
public:
	CreateTableThread(TblInterface *ti, TableDef *tableDef): Thread("CreateTableThread") {
		m_ti = ti;
		m_tableDef = tableDef;
	}

	void run() {
		m_ti->create(m_tableDef);
	}

private:
	TblInterface	*m_ti;
	TableDef		*m_tableDef;
};

/**
 * ����CREATE TABLE�����Ļָ�����: ���Ѿ������������ļ�û�и���
 * �������ʱ�����������һ����ӱ��ݻָ�ʱ�п���
 */
void RecvDDLTestCase::testRecvCreateTableOldCtrlFile() {
	Database::create(&m_config);
	EXCPT_OPER(m_db = Database::open(&m_config, false));

	TblInterface ti(m_db, "BlogCount");
	TableDef *tableDef = TableTestCase::getBlogCountDef(true);
	
	CreateTableThread createThread(&ti, tableDef);
	createThread.enableSyncPoint(SP_DB_CREATE_TABLE_AFTER_LOG);

	createThread.start();
	createThread.joinSyncPoint(SP_DB_CREATE_TABLE_AFTER_LOG);

	CPPUNIT_ASSERT(File::copyFile("dbtestdir/ntse_ctrl.bak", "dbtestdir/ntse_ctrl", true) == File::E_NO_ERROR);

	createThread.notifySyncPoint(SP_DB_CREATE_TABLE_AFTER_LOG);
	createThread.join();

	m_db->close(true, false);
	delete m_db;
	m_db = NULL;
	delete tableDef;

	// �����ݿ�ǿ�ƽ��лָ�
	File("dbtestdir/ntse_ctrl.bak").move("dbtestdir/ntse_ctrl", true);
	EXCPT_OPER(m_db = Database::open(&m_config, false, 1));

	ti.reconnect(m_db);
	EXCPT_OPER(ti.open());
	ti.close(true);
}

/**
 * ����DROP TABLE�����Ļָ�����
 */
void RecvDDLTestCase::testRecvDropTable() {
	Database::create(&m_config);
	EXCPT_OPER(m_db = Database::open(&m_config, false));

	TblInterface ti(m_db, "BlogCount");
	TableDef *tblDef = TableTestCase::getBlogCountDef(true);
	EXCPT_OPER(ti.create(tblDef));
	delete tblDef;
	tblDef = NULL;
	CPPUNIT_ASSERT(File::copyFile("dbtestdir/ntse_ctrl.bak", "dbtestdir/ntse_ctrl", true) == File::E_NO_ERROR);
	bool hasLob = false;
	string bak = ti.backup(hasLob);

	EXCPT_OPER(ti.drop());
	m_db->close(true, false);
	delete m_db;
	m_db = NULL;

	// ��д����־�������ļ��ͱ�û�и���
	File("dbtestdir/ntse_ctrl.bak").move("dbtestdir/ntse_ctrl", true);
	ti.restore(bak, hasLob, "dbtestdir/BlogCount");
	CPPUNIT_ASSERT(File::isExist("dbtestdir/BlogCount.nsd"));
	EXCPT_OPER(m_db = Database::open(&m_config, false, 1));
	CPPUNIT_ASSERT(m_db->getControlFile()->getTableId("BlogCount") == TableDef::INVALID_TABLEID);
	CPPUNIT_ASSERT(!File::isExist("dbtestdir/BlogCount.nsd"));
	m_db->close(true, false);
	delete m_db;
	m_db = NULL;

	// �����ļ��Ѿ����£���û��ɾ�����ָ�ʱ������������ɾ����
	ti.restore(bak, hasLob, "dbtestdir/BlogCount");
	CPPUNIT_ASSERT(File::isExist("dbtestdir/BlogCount.nsd"));
	EXCPT_OPER(m_db = Database::open(&m_config, false, 1));
	CPPUNIT_ASSERT(m_db->getControlFile()->getTableId("BlogCount") == TableDef::INVALID_TABLEID);
	CPPUNIT_ASSERT(File::isExist("dbtestdir/BlogCount.nsd"));
	Table::drop(m_db, "BlogCount");
	CPPUNIT_ASSERT(!File::isExist("dbtestdir/BlogCount.nsd"));
	m_db->close(true, false);
	delete m_db;
	m_db = NULL;

	// �����ļ��ͱ��Ѿ�ɾ����
	EXCPT_OPER(m_db = Database::open(&m_config, false, 1));
	CPPUNIT_ASSERT(m_db->getControlFile()->getTableId("BlogCount") == TableDef::INVALID_TABLEID);
	CPPUNIT_ASSERT(!File::isExist("dbtestdir/BlogCount.nsd"));
	m_db->close(true, false);
	delete m_db;
	m_db = NULL;
}

/**
 * ����TRUNCATE TABLE�����Ļָ�����
 */
void RecvDDLTestCase::testRecvTruncate() {
	Database::create(&m_config);
	EXCPT_OPER(m_db = Database::open(&m_config, false));

	Connection *conn = m_db->getConnection(false);
	Session *session = m_db->getSessionManager()->allocSession("RecvDDLTestCase::testRecvTruncate", conn);

	TableDef *tableDef = TableTestCase::getBlogCountDef(true);
	m_db->createTable(session, "BlogCount", tableDef);
	delete tableDef;
	tableDef = NULL;

	EXCPT_OPER(m_table = m_db->openTable(session, "BlogCount"));
	Record **rows = TableTestCase::populateBlogCount(m_db, m_table, 1);
	freeMySQLRecords(m_table->getTableDef(), rows, 1);
	rows = NULL;

	m_db->checkpoint(session);

	// �رձ�󣬱��ݰ������ݵ��ļ�
	EXCPT_OPER(m_db->closeTable(session, m_table));
	CPPUNIT_ASSERT(File::copyFile("dbtestdir/BlogCount.nsd-bak", "dbtestdir/BlogCount.nsd", false) == File::E_NO_ERROR);
	CPPUNIT_ASSERT(File::copyFile("dbtestdir/BlogCount.nsi-bak", "dbtestdir/BlogCount.nsi", false) == File::E_NO_ERROR);

	EXCPT_OPER(m_table = m_db->openTable(session, "BlogCount"));
	EXCPT_OPER(m_db->truncateTable(session, m_table));

	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(conn);

	m_db->close(true, false);
	delete m_db;
	m_db = NULL;

	// �ָ����ݵ��ļ�
	CPPUNIT_ASSERT(File::copyFile("dbtestdir/BlogCount.nsd", "dbtestdir/BlogCount.nsd-bak", true) == File::E_NO_ERROR);
	CPPUNIT_ASSERT(File::copyFile("dbtestdir/BlogCount.nsi", "dbtestdir/BlogCount.nsi-bak", true) == File::E_NO_ERROR);

	// �����ݿ�ǿ�ƽ��лָ�
	
	EXCPT_OPER(m_db = Database::open(&m_config, false, 1));

	conn = m_db->getConnection(false);
	session = m_db->getSessionManager()->allocSession("RecvDDLTestCase::testRecvTruncate", conn);

	EXCPT_OPER(m_table = m_db->openTable(session, "BlogCount"));

	// ��֤���е������Ѿ���TRUNCATE��û����
	u16 columns[1] = {0};
	byte buf[Limits::PAGE_SIZE];
	TblScan *scanHandle = m_table->tableScan(session, OP_READ, 1, columns);
	if (m_table->getNext(scanHandle, buf)) {
		m_table->endScan(scanHandle);
		CPPUNIT_ASSERT(false);
	}
	m_table->endScan(scanHandle);

	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(conn);

	m_db->close(false, false);
	delete m_db;
	m_db = NULL;
	Database::drop("dbtestdir");
}

/**
 * ����RENAME�ָ�����: �ָ�ʱ����Ϊ��ʼ״̬
 */
void RecvDDLTestCase::testRecvRenameBefore() {
	prepareBlogCount(true);
	openTable("BlogCount");
	Record **rows = TableTestCase::populateBlogCount(m_db, m_table, 1);
	freeMySQLRecords(m_table->getTableDef(), rows, 1);
	closeTable();

	Connection *conn = m_db->getConnection(false);
	Session *session = m_db->getSessionManager()->allocSession("RecvDDLTestCase::testRecvTruncate", conn);

	File::copyFile("dbtestdir/ntse_ctrl.bak", "dbtestdir/ntse_ctrl", false);
	EXCPT_OPER(m_db->renameTable(session, "BlogCount", "BlogCount2"));

	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(conn);

	m_db->close(true, false);
	delete m_db;
	m_db = NULL;

	File::copyFile("dbtestdir/ntse_ctrl", "dbtestdir/ntse_ctrl.bak", true);
	File("dbtestdir/BlogCount2.nsd").move("dbtestdir/BlogCount.nsd");
	File("dbtestdir/BlogCount2.nsi").move("dbtestdir/BlogCount.nsi");
	File("dbtestdir/BlogCount2.nstd").move("dbtestdir/BlogCount.nstd");

	// �����ݿ�ǿ�ƽ��лָ�
	EXCPT_OPER(m_db = Database::open(&m_config, false, 1));

	conn = m_db->getConnection(false);
	session = m_db->getSessionManager()->allocSession(__FUNCTION__, conn);
	EXCPT_OPER(m_table = m_db->openTable(session, "BlogCount2"));
	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(conn);
}

/**
 * ����RENAME�ָ����ܣ��ָ�ʱ����Ϊ����״̬
 */
void RecvDDLTestCase::testRecvRenameAfter() {
	Database::create(&m_config);
	EXCPT_OPER(m_db = Database::open(&m_config, false));
	m_db->setCheckpointEnabled(false);

	TblInterface ti(m_db, "BlogCount");
	TableDef *tableDef = TableTestCase::getBlogCountDef(true);
	ti.create(tableDef);
	delete tableDef;

	ti.open();

	CPPUNIT_ASSERT(ti.insertRow(NULL, (s64)1, 10, (short)2, 23, "title 1", (s64)1));

	ResultSet *rs1 = ti.selectRows(NULL);

	ti.close(true);

	EXCPT_OPER(ti.rename("BlogCount2"));

	// ���лָ�
	m_db->close(true, false);
	delete m_db;
	m_db = NULL;
	m_db = Database::open(&m_config, false, 1);
	m_db->setCheckpointEnabled(false);

	ti.reconnect(m_db);
	ti.open();
	ti.verify();
	ResultSet *rs2 = ti.selectRows(NULL);
	CPPUNIT_ASSERT(*rs2 == *rs1);

	delete rs1;
	delete rs2;
}

/**
 * ���Դ������������ָ�����: �����������
 */
void RecvDDLTestCase::testRecvAddIndexSucc() {
	doAddIndexTest(LOG_MAX, true);
}

/**
 * ���Դ������������ָ�����: ����ʧ��
 */
void RecvDDLTestCase::testRecvAddIndexFail() {
	doAddIndexTest(LOG_IDX_CREATE_END, false);
}

/**
* ִ�д�����������
*
* @param truncateOnThisLog ģ����д������־ʱ�������������ΪLOG_MAX�򲻽ض���־
* @param succ ���������Ƿ�Ӧ�óɹ�
*/
void RecvDDLTestCase::doAddIndexTest(LogType truncateOnThisLog, bool succ) {
	prepareBlogCount(true);

	// ����û��������������
	backupTable("BlogCount", false);

	openTable("BlogCount");

	Connection *conn = m_db->getConnection(false);
	Session *session = m_db->getSessionManager()->allocSession("RecvDDLTestCase::testRecvTruncate", conn);

	ColumnDef *columns[1] = {m_table->getTableDef()->m_columns[3]};
	u32 prefixLensAttr[1] = {0};
	IndexDef *newIdx = new IndexDef("IDX_BLOGCOUNT_AC", 1, columns, prefixLensAttr);

	u16 numIndiceBefore = m_table->getTableDef()->m_numIndice;
	EXCPT_OPER(m_table->addIndex(session, 1, (const IndexDef **)&newIdx));
	delete newIdx;
	newIdx = NULL;

	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(conn);

	m_db->close(true, false);
	delete m_db;
	m_db = NULL;

	truncateLog(truncateOnThisLog);

	// �ָ�û��������������
	restoreTable("BlogCount", false);

	// �����ݿ�ǿ�ƽ��лָ�
	EXCPT_OPER(m_db = Database::open(&m_config, false, 1));

	openTable("BlogCount");
	if (succ) {
		CPPUNIT_ASSERT(m_table->getTableDef()->m_numIndice == numIndiceBefore + 1);
		CPPUNIT_ASSERT(!strcmp(m_table->getTableDef()->m_indice[m_table->getTableDef()->m_numIndice - 1]->m_name, "IDX_BLOGCOUNT_AC"));
	} else {
		CPPUNIT_ASSERT(m_table->getTableDef()->m_numIndice == numIndiceBefore);
		CPPUNIT_ASSERT(strcmp(m_table->getTableDef()->m_indice[m_table->getTableDef()->m_numIndice - 1]->m_name, "IDX_BLOGCOUNT_AC"));

		// ʧ�ܵ�������ٻָ�һ�Σ���֤������־�����Ƿ���ȷ
		closeTable();
		m_db->close(true, false);
		delete m_db;
		m_db = NULL;

		restoreTable("BlogCount", false);
		EXCPT_OPER(m_db = Database::open(&m_config, false, 1));
		openTable("BlogCount");
		CPPUNIT_ASSERT(m_table->getTableDef()->m_numIndice == numIndiceBefore);
		CPPUNIT_ASSERT(strcmp(m_table->getTableDef()->m_indice[m_table->getTableDef()->m_numIndice - 1]->m_name, "IDX_BLOGCOUNT_AC"));
	}
}

/** ����ʹ��readonly�㷨��������ʱ�Ļָ����� */
void RecvDDLTestCase::testRecvAddIndexReadonly() {
	const int IDX_ATTR_SIZE = 100;
	TableDefBuilder tdb(TableDef::INVALID_TABLEID, "space", "BlogCount");
	tdb.addColumn("id", CT_INT, false);
	tdb.addColumnS("a", CT_CHAR, IDX_ATTR_SIZE);
	tdb.addIndex("pkey", true, true, false, "id", 0, NULL);
	TableDef *tableDef = tdb.getTableDef();

	Database::create(&m_config);
	EXCPT_OPER(m_db = Database::open(&m_config, false));
	TblInterface ti(m_db, "BlogCount");
	EXCPT_OPER(ti.create(tableDef));
	ti.checkpoint();

	string emptyBak = ti.backup(tableDef->hasLob());
	string emptyLogBak = ti.backupTxnlogs(m_config.m_basedir, m_config.m_basedir);
	CPPUNIT_ASSERT(File::copyFile("dbtestdir/ntse_ctrl.empty.bak", "dbtestdir/ntse_ctrl", true) == File::E_NO_ERROR);

	ti.open();

	uint n = 100;
	for (uint i = 0; i < n; i++) {
		byte buf[IDX_ATTR_SIZE];
		memset(buf, 0, sizeof(buf));
		buf[0] = (byte)(i / 10);
		ti.insertRow(NULL, i, buf);
	}

	ResultSet *rs1 = ti.selectRows(NULL);

	ti.checkpoint();
	string nonEmptyBak = ti.backup(tableDef->hasLob());
	string nonEmptyLogBak = ti.backupTxnlogs(m_config.m_basedir, m_config.m_basedir);
	CPPUNIT_ASSERT(File::copyFile("dbtestdir/ntse_ctrl.nonempty.bak", "dbtestdir/ntse_ctrl", true) == File::E_NO_ERROR);

	ColumnDef *columns[1] = {ti.getTable()->getTableDef()->m_columns[1]};
	u32 prefixLensAttr[1] = {0};
	IndexDef *newIdx = new IndexDef("IDX_BLOGCOUNT_A", 1, columns, prefixLensAttr);
	// ���������ɹ�
	ti.addIndex((const IndexDef **)&newIdx);
	ti.close(true);
	closeDatabase(true, false);
	
	ti.restore(nonEmptyBak, tableDef->hasLob(), "dbtestdir/BlogCount");
	EXCPT_OPER(m_db = Database::open(&m_config, false, 1));
	ti.reconnect(m_db);
	ti.open();
	CPPUNIT_ASSERT(ti.getTable()->getTableDef()->m_numIndice == 2);
	CPPUNIT_ASSERT(*ti.getTable()->getTableDef()->m_indice[1] == *newIdx);
	ti.verify();
	ResultSet *rs2 = ti.selectRows(NULL);
	CPPUNIT_ASSERT(*rs1 == *rs2);

	// ���������б���
	ti.close(true);
	closeDatabase(true, false);
	ti.restore(nonEmptyBak, tableDef->hasLob(), "dbtestdir/BlogCount");
	truncateLog(LOG_IDX_CREATE_END);
	EXCPT_OPER(m_db = Database::open(&m_config, false, 1));
	ti.reconnect(m_db);
	ti.open();
	CPPUNIT_ASSERT(ti.getTable()->getTableDef()->m_numIndice == 1);
	ti.verify();

	/// �ٻָ�һ�Σ����ԶԲ�����־�Ĵ���
	ti.close(true);
	closeDatabase(true, false);
	ti.restore(nonEmptyBak, tableDef->hasLob(), "dbtestdir/BlogCount");
	EXCPT_OPER(m_db = Database::open(&m_config, false, 1));
	ti.reconnect(m_db);
	ti.open();
	CPPUNIT_ASSERT(ti.getTable()->getTableDef()->m_numIndice == 1);
	ti.verify();
	ResultSet *rs3 = ti.selectRows(NULL);
	CPPUNIT_ASSERT(*rs1 == *rs3);

	ti.close(true);
	closeDatabase(true, false);

	// ����Ψһ����������Ψһ�Գ�ͻʧ��
	ti.restore(nonEmptyBak, tableDef->hasLob(), "dbtestdir/BlogCount");
	ti.restoreTxnlogs(nonEmptyLogBak, m_config.m_basedir);
	CPPUNIT_ASSERT(File::copyFile("dbtestdir/ntse_ctrl", "dbtestdir/ntse_ctrl.nonempty.bak", true) == File::E_NO_ERROR);

	EXCPT_OPER(m_db = Database::open(&m_config, false, 1));
	ti.reconnect(m_db);
	ti.open();

	newIdx->m_unique = true;
	try {
		ti.addIndex((const IndexDef **)&newIdx, 1);
		CPPUNIT_FAIL("Create unique index with non unique values should fail");
	} catch (NtseException &) {}
	ti.verify();

	ti.close(true);
	closeDatabase(true, false);
	ti.restore(nonEmptyBak, tableDef->hasLob(), "dbtestdir/BlogCount");
	EXCPT_OPER(m_db = Database::open(&m_config, false, 1));
	ti.reconnect(m_db);
	ti.open();
	CPPUNIT_ASSERT(ti.getTable()->getTableDef()->m_numIndice == 1);
	ti.verify();
	ti.close(true);
	closeDatabase(true, false);
	
	// �ձ�
	ti.restore(emptyBak, tableDef->hasLob(), "dbtestdir/BlogCount");
	ti.restoreTxnlogs(emptyLogBak, m_config.m_basedir);
	CPPUNIT_ASSERT(File::copyFile("dbtestdir/ntse_ctrl", "dbtestdir/ntse_ctrl.empty.bak", true) == File::E_NO_ERROR);

	EXCPT_OPER(m_db = Database::open(&m_config, false, 1));
	ti.reconnect(m_db);
	ti.open();

	newIdx->m_unique = true;
	ti.addIndex((const IndexDef **)&newIdx);
	
	ti.close(true);
	closeDatabase(true, false);
	ti.restore(emptyBak, tableDef->hasLob(), "dbtestdir/BlogCount");
	EXCPT_OPER(m_db = Database::open(&m_config, false, 1));
	ti.reconnect(m_db);
	ti.open();
	CPPUNIT_ASSERT(ti.getTable()->getTableDef()->m_numIndice == 2);
	CPPUNIT_ASSERT(*ti.getTable()->getTableDef()->m_indice[1] == *newIdx);
	ti.close(true);
	closeDatabase(true, false);

	delete tableDef;
	delete newIdx;
	delete rs1;
	delete rs2;
	delete rs3;
}

/** ����ʹ��online�㷨��������ʱ�Ļָ����� */
void RecvDDLTestCase::testRecvAddIndexOnline() {
	// TODO
}

/**
 * ����ɾ�������ָ�����
 */
void RecvDDLTestCase::testRecvDropIndex() {
	prepareBlogCount(true);
	backupTable("BlogCount", false);

	openTable("BlogCount");
	u16 numIndiceBefore = m_table->getTableDef()->m_numIndice;

	Connection *conn = m_db->getConnection(false);
	Session *session = m_db->getSessionManager()->allocSession("RecvDDLTestCase::testRecvTruncate", conn);

	EXCPT_OPER(m_table->dropIndex(session, 1));

	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(conn);

	m_db->close(true, false);
	delete m_db;
	m_db = NULL;

	// �ָ����ݲ����лָ�
	restoreTable("BlogCount", false);
	EXCPT_OPER(m_db = Database::open(&m_config, false, 1));

	openTable("BlogCount");
	CPPUNIT_ASSERT(m_table->getTableDef()->m_numIndice == numIndiceBefore - 1);
}

/**
 * ��������DDL��DML���ۺ��Բ�������
 */
void RecvDDLTestCase::testMisc() {
	Database::create(&m_config);
	EXCPT_OPER(m_db = Database::open(&m_config, false));
	m_db->setCheckpointEnabled(false);

	TblInterface ti(m_db, "BlogCount");
	TableDef *tableDef = TableTestCase::getBlogCountDef(true);
	ti.create(tableDef);
	delete tableDef;

	string bakPath = ti.backup(false);
	CPPUNIT_ASSERT(File::copyFile("dbtestdir/ntse_ctrl.bak", "dbtestdir/ntse_ctrl", true) == File::E_NO_ERROR);

	ti.open();
	CPPUNIT_ASSERT(ti.insertRow(NULL, (s64)1, 10, (short)2, 23, "title 1", (s64)1));
	CPPUNIT_ASSERT(ti.insertRow(NULL, (s64)2, 10, (short)2, 23, "title 2", (s64)1));
	CPPUNIT_ASSERT(!ti.insertRow(NULL, (s64)1, 10, (short)2, 23, "title 1", (s64)1));	// duplicate
	// SET AccessCount = 24;
	CPPUNIT_ASSERT(ti.updateRows(ti.buildLRange(0, 0, true), "AccessCount", 24) == 2);
	// SET BlogID = 2 WHERE BlogID <= 1;
	CPPUNIT_ASSERT(ti.updateRows(ti.buildHRange(0, 1, true, (s64)1), "BlogID", (s64)2) == 0);
	// DELETE WHERE BlogID >= 2;
	CPPUNIT_ASSERT(ti.deleteRows(ti.buildLRange(0, 1, true, (s64)2)) == 1);
	// ����Ӧ��ֻ��BlogID=1�ļ�¼����AccessCountΪ24

	IndexDef idxDefBak(ti.getIndexDef(ti.findIndex("IDX_BLOGCOUNT_UID_AC")));
	ti.dropIndex(ti.findIndex("IDX_BLOGCOUNT_UID_AC"));

	CPPUNIT_ASSERT(ti.insertRow(NULL, (s64)2, 10, (short)2, 23, "title 2", (s64)1));
	CPPUNIT_ASSERT(ti.updateRows(ti.buildLRange(0, 0, true), "AccessCount", 25) == 2);
	CPPUNIT_ASSERT(ti.updateRows(ti.buildHRange(0, 1, true, (s64)1), "BlogID", (s64)2) == 0);
	CPPUNIT_ASSERT(ti.deleteRows(ti.buildLRange(0, 1, true, (s64)2)));
	// ����Ӧ��ֻ��BlogID=1�ļ�¼����AccessCountΪ25

	IndexDef *idxDef = &idxDefBak;
	ti.addIndex((const IndexDef **)&idxDef);
	CPPUNIT_ASSERT(ti.insertRow(NULL, (s64)2, 10, (short)2, 23, "title 2", (s64)1));
	CPPUNIT_ASSERT(ti.updateRows(ti.buildLRange(0, 0, true), "AccessCount", 26) == 2);
	CPPUNIT_ASSERT(ti.updateRows(ti.buildHRange(0, 1, true, (s64)1), "BlogID", (s64)2) == 0);
	CPPUNIT_ASSERT(ti.deleteRows(ti.buildLRange(0, 1, true, (s64)2)));
	// ����Ӧ��ֻ��BlogID=1�ļ�¼����AccessCountΪ26

	ResultSet *rsBeforeTruncate = ti.selectRows(NULL);
	CPPUNIT_ASSERT(rsBeforeTruncate->getNumRows() == 1);

	ti.truncate();

	ResultSet *rsAfterTruncate = ti.selectRows(NULL);
	CPPUNIT_ASSERT(rsAfterTruncate->getNumRows() == 0);

	delete rsBeforeTruncate;
	delete rsAfterTruncate;

	CPPUNIT_ASSERT(ti.insertRow(NULL, (s64)2, 10, (short)2, 23, "title 2", (s64)1));
	CPPUNIT_ASSERT(ti.updateRows(ti.buildLRange(0, 0, true), "AccessCount", 27) == 1);
	// ����Ӧ��ֻ��BlogID=2�ļ�¼����AccessCountΪ27

	ResultSet *rs1 = ti.selectRows(NULL);

	// ���е�һ�λָ����ָ�ʱ����Ϊ��ԭʼ״̬
	ti.close(true);
	ti.restore(bakPath, false);

	m_db->close(true, false);
	delete m_db;
	m_db = NULL;
	
	CPPUNIT_ASSERT(File("dbtestdir/ntse_ctrl.bak").move("dbtestdir/ntse_ctrl", true) == File::E_NO_ERROR);

	m_db = Database::open(&m_config, false, 1);
	m_db->setCheckpointEnabled(false);

	ti.reconnect(m_db);
	ti.open();
	ti.verify();
	ResultSet *rs2 = ti.selectRows(NULL);
	CPPUNIT_ASSERT(*rs2 == *rs1);

	// ���еڶ��λָ����ָ�ʱ�����Ѿ�������״̬
	ti.close(true);
	m_db->close(true, false);
	delete m_db;
	m_db = NULL;
	m_db = Database::open(&m_config, false, 1);

	ti.reconnect(m_db);
	ti.open();
	ti.verify();
	ResultSet *rs3 = ti.selectRows(NULL);
	CPPUNIT_ASSERT(*rs3 == *rs1);

	delete rs1;
	delete rs2;
	delete rs3;
}

/**
 * ������������ƽ̨bug #657: �ָ���������־��Ӧ�ı��Ѿ���ɾ��ʱϵͳ����
 */
void RecvDDLTestCase::bug657() {
	prepareBlogCount(true);
	openTable("BlogCount");
	m_db->setCheckpointEnabled(false);

	Record **rows = TableTestCase::populateBlogCount(m_db, m_table, 1);
	freeMySQLRecords(m_table->getTableDef(), rows, 1);

	Connection *conn = m_db->getConnection(false);
	Session *session = m_db->getSessionManager()->allocSession(__FUNCTION__, conn);
	closeTable();
	m_db->dropTable(session, "BlogCount");
	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(conn);

	// �رղ�ǿ�ƻָ�
	m_db->close(true, false);
	delete m_db;
	m_db = Database::open(&m_config, false, 1);
}

/**
 * ������������ƽ̨bug #700: ���ظ�����ʱ�ָ�ʧ��
 */
void RecvDDLTestCase::bug700() {
	Database::create(&m_config);
	EXCPT_OPER(m_db = Database::open(&m_config, false));
	m_db->setCheckpointEnabled(false);

	Connection *conn = m_db->getConnection(false);
	Session *session = m_db->getSessionManager()->allocSession(__FUNCTION__, conn);

	TableDef *tableDef = TableTestCase::getBlogCountDef(true);
	m_db->createTable(session, "BlogCount", tableDef);
	delete tableDef;

	openTable("BlogCount");

	Record **rows = TableTestCase::populateBlogCount(m_db, m_table, 1);
	freeMySQLRecords(m_table->getTableDef(), rows, 1);

	closeTable();
	m_db->dropTable(session, "BlogCount");

	tableDef = TableTestCase::getBlogCountDef(true);
	m_db->createTable(session, "BlogCount", tableDef);
	delete tableDef;

	openTable("BlogCount");
	rows = TableTestCase::populateBlogCount(m_db, m_table, 1);
	freeMySQLRecords(m_table->getTableDef(), rows, 1);

	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(conn);

	// �رղ�ǿ�ƻָ�
	closeTable();
	m_db->close(true, false);
	delete m_db;
	m_db = NULL;
	m_db = Database::open(&m_config, false, 1);
}

/** �������ߴ�������ʱ��д��־������UPDATE/DELETE�����Ļָ� */
void RecvDDLTestCase::testUpdateWhenAddingIndex() {
	prepareBlogCount(true);
	openTable("BlogCount");
	m_db->setCheckpointEnabled(false);

	TblInterface ti(m_db, "BlogCount");
	EXCPT_OPER(ti.open());

	CPPUNIT_ASSERT(ti.insertRow(NULL, (s64)1, 3, (s16)6, 23, "title 1", (s64)1));
	CPPUNIT_ASSERT(ti.insertRow(NULL, (s64)2, 3, (s16)6, 23, "title 2", (s64)1));

	string bak = ti.backup(false);

	// ����UserID, CommentCount����
	ColumnDef *columns[2];
	columns[0] = m_table->getTableDef()->getColumnDef("UserID");
	columns[1] = m_table->getTableDef()->getColumnDef("CommentCount");
	u32 prefixs[2] = {0,0};
	IndexDef *newIndex = new IndexDef("userid_commentcount", 2, columns, prefixs, false, false);

	Connection *conn = m_db->getConnection(false);
	Session *session = m_db->getSessionManager()->allocSession(__FUNCTION__, conn);
	m_table->lockMeta(session, IL_X, -1, __FILE__, __LINE__);
	const IndexDef *indice[1] = {newIndex};
	m_table->setOnlineAddingIndice(session, 1, indice);
	m_table->unlockMeta(session, IL_X);

	CPPUNIT_ASSERT(ti.updateRows(-1, 0, "AccessCount", 24) == 2);
	CPPUNIT_ASSERT(ti.deleteRows(1, 0) == 1);

	m_table->lockMeta(session, IL_X, -1, __FILE__, __LINE__);
	m_table->resetOnlineAddingIndice(session);
	m_table->unlockMeta(session, IL_X);

	ResultSet *rs1 = ti.selectRows(NULL);

	// �ر����ݿ���лָ�
	ti.close(true);
	ti.freeConnection();
	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(conn);
	closeTable();
	ti.restore(bak, false);

	m_db->close(true, false);
	delete m_db;
	m_db = NULL;
	
	m_db = Database::open(&m_config, false, 1);

	ti.reconnect(m_db);
	ti.open();
	ResultSet *rs2 = ti.selectRows(NULL);
	CPPUNIT_ASSERT(*rs1 == *rs2);

	delete rs1;
	delete rs2;
	delete newIndex;
}

/** �����������������OPTIMIZE�����Ļָ� */
void RecvDDLTestCase::testRecvOptimize() {
	EXCPT_OPER(Database::create(&m_config));
	EXCPT_OPER(m_db = Database::open(&m_config, false));
	m_db->setCheckpointEnabled(false);

	TableDef *tableDef = TableTestCase::getBlogCountDef(true);
	TblInterface ti(m_db, "space/BlogCount");
	EXCPT_OPER(ti.create(tableDef));
	EXCPT_OPER(ti.open());

	CPPUNIT_ASSERT(ti.insertRow(NULL, (s64)1, 3, (s16)6, 23, "title 1", (s64)1));
	CPPUNIT_ASSERT(ti.insertRow(NULL, (s64)2, 3, (s16)6, 23, "title 2", (s64)1));
	ti.flush();

	ResultSet *rs1 = ti.selectRows(NULL);

	string bak = ti.backup(false);
	
	ti.optimize();

	ti.close(true);
	ti.freeConnection();

	ti.restore(bak, false);

	m_db->close(true, false);
	delete m_db;
	m_db = NULL;

	//��Ҫ�ָ��Ĵ�
	m_db = Database::open(&m_config, false, 1);

	ti.reconnect(m_db);
	ti.open();
	ResultSet *rs2 = ti.selectRows(NULL);
	CPPUNIT_ASSERT(*rs1 == *rs2);
	ti.close(true);
	ti.freeConnection();
	m_db->close(true, true);
	delete m_db;
	m_db = NULL;

	//����Ҫ�ָ��Ĵ�
	m_db = Database::open(&m_config, false, 0);

	ti.reconnect(m_db);
	ti.open();
	ResultSet *rs3 = ti.selectRows(NULL);
	CPPUNIT_ASSERT(*rs1 == *rs3);

	delete rs1;
	delete rs2;
	delete rs3;
	delete tableDef;
}

/** ������������������޸����������Ļָ� */
void RecvDDLTestCase::testRecvAlterIndex() {
	EXCPT_OPER(Database::create(&m_config));
	EXCPT_OPER(m_db = Database::open(&m_config, false));
	m_db->setCheckpointEnabled(false);

	TableDef *tableDef = TableTestCase::getBlogCountDef(true);
	TblInterface ti(m_db, "space/BlogCount");
	EXCPT_OPER(ti.create(tableDef));
	EXCPT_OPER(ti.open());
	u16 numIdxOld = ti.getTableDef()->m_numIndice;

	CPPUNIT_ASSERT(ti.insertRow(NULL, (s64)1, 3, (s16)6, 23, "title 1", (s64)1));
	CPPUNIT_ASSERT(ti.insertRow(NULL, (s64)2, 3, (s16)6, 23, "title 2", (s64)1));
	ti.flush();

	ResultSet *rs1 = ti.selectRows(NULL);

	string bak = ti.backup(false);
	u32 prefixsAttr[1] = {0};
	IndexDef *index = new IndexDef("newidx", 1, &tableDef->m_columns[tableDef->m_numCols - 1], prefixsAttr);
	index->m_online = true;
	ti.addIndex((const IndexDef **)&index, 1);
	delete index;

	ti.close(true);
	ti.freeConnection();

	ti.restore(bak, false);

	m_db->close(true, false);
	delete m_db;
	m_db = NULL;
	
	m_db = Database::open(&m_config, false, 1);

	ti.reconnect(m_db);
	ti.open();
	ResultSet *rs2 = ti.selectRows(NULL);
	CPPUNIT_ASSERT(*rs1 == *rs2);
	// ע�⣬�ָ�ʱ����������������������
	CPPUNIT_ASSERT(ti.getTableDef()->m_numIndice == numIdxOld);

	delete rs1;
	delete rs2;
	delete tableDef;
}

void RecvDDLTestCase::testRecvCreateDictonary() {
	EXCPT_OPER(Database::create(&m_config));
	EXCPT_OPER(m_db = Database::open(&m_config, false));
	m_db->setCheckpointEnabled(false);

	TableDef *tableDef = TableTestCase::getBlogDef(true);
	TblInterface ti(m_db, "Blog");
	EXCPT_OPER(ti.create(tableDef));
	EXCPT_OPER(ti.open());

	uint numRows = 5000;
	Record ** rows = TableTestCase::populateBlog(m_db, ti.getTable(), numRows, false, true);
	for (uint i = 0; i < numRows; i++) {
		byte *abs =  *((byte **)(rows[i]->m_data + tableDef->m_columns[5]->m_mysqlOffset + tableDef->m_columns[5]->m_mysqlSize - 8));
		delete [] abs;
		byte *content =  *((byte **)(rows[i]->m_data + tableDef->m_columns[6]->m_mysqlOffset + tableDef->m_columns[6]->m_mysqlSize - 8));
		delete [] content;
		freeRecord(rows[i]);
	} 
	delete [] rows;
	rows = NULL;
	ti.flush();

	//����
	string bak = ti.backup(true);

	Connection *conn = m_db->getConnection(false);
	Session *session = m_db->getSessionManager()->allocSession("RecvDDLTestCase::testRecvCreateDictonary", conn);
	
	//��ѹ�����ܴ����ֵ�
	Table *tbl = ti.getTable();
	CPPUNIT_ASSERT(!tbl->getTableDef()->m_isCompressedTbl);
	NEED_EXCPT(m_db->createCompressDic(session, tbl));

	EXCPT_OPER(m_db->alterTableArgument(session, tbl, "COMPRESS_ROWS", "TRUE", 1000));
	CPPUNIT_ASSERT(tbl->getTableDef()->m_isCompressedTbl);
	CPPUNIT_ASSERT(tbl->getTableDef()->m_rowCompressCfg);

	//���������ֵ�
	EXCPT_OPER(m_db->createCompressDic(session, tbl));
	CPPUNIT_ASSERT(tbl->hasCompressDict());
	CPPUNIT_ASSERT(m_db->getControlFile()->hasCprsDict(tbl->getTableDef()->m_id));

	//����һЩ����
	uint oldMaxId = numRows;
	numRows= 10;
	Record **rcd = TableTestCase::populateBlog(m_db, tbl, numRows, false, true, oldMaxId + 1);
	for (uint i = 0; i < numRows; i++) {
		byte *abs =  *((byte **)(rcd[i]->m_data + tableDef->m_columns[5]->m_mysqlOffset + tableDef->m_columns[5]->m_mysqlSize - 8));
		delete [] abs;
		byte *content =  *((byte **)(rcd[i]->m_data + tableDef->m_columns[6]->m_mysqlOffset + tableDef->m_columns[6]->m_mysqlSize - 8));
		delete [] content;
		freeRecord(rcd[i]);
	}
	delete [] rcd;

	//��������
	CPPUNIT_ASSERT(ti.updateRows(10, 0, "UserID", 1234) == 10);
	CPPUNIT_ASSERT(ti.updateRows(10, 0, "Content", "11") == 10);
	const uint contentLen = 4096;
	char content[contentLen];
	for (int i = contentLen - 2; i >= 0; i--) {
		content[i] = 'a';
	}
	content[contentLen - 1] = '\0';
	CPPUNIT_ASSERT(ti.updateRows(2, 0, "Content", content) == 2);
	ResultSet *rs1 = ti.selectRows(NULL);

	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(conn);
	ti.close(true);
	ti.freeConnection();

	//�ָ�
	ti.restore(bak, true);

	m_db->close(true, false);
	delete m_db;
	m_db = NULL;

	//���´����ݿ⣬����ָ�����
	EXCPT_OPER(m_db = Database::open(&m_config, false, 1));

	ti.reconnect(m_db);
	EXCPT_OPER(ti.open());

	ResultSet *rs2 = ti.selectRows(NULL);
	CPPUNIT_ASSERT(*rs1 == *rs2);

	delete rs1;
	delete rs2;
	delete tableDef;

	//���Ѿ����ֵ�ı��д����ֵ�
	Connection *conn2 = m_db->getConnection(false);
	Session *session2 = m_db->getSessionManager()->allocSession("RecvDDLTestCase::testRecvCreateDictonary", conn2);
	NEED_EXCPT(m_db->createCompressDic(session2, ti.getTable()));
	m_db->getSessionManager()->freeSession(session2);
	m_db->freeConnection(conn2);
}
