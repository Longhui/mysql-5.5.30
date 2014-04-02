#include "api/TestRecover.h"
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

const char* RecoverTestCase::getName() {
	return "Recovery test";
}

const char* RecoverTestCase::getDescription() {
	return "Base class for all recovery tests.";
}

bool RecoverTestCase::isBig() {
	return false;
}

void RecoverTestCase::setUp() {
	m_db = NULL;
	File dir("dbtestdir");
	dir.rmdir(true);
	dir.mkdir();
	File("dbtestdir/space").mkdir();
	m_config.setBasedir("dbtestdir");
	m_table = NULL;
	m_conn = NULL;
	m_session = NULL;
}

void RecoverTestCase::tearDown() {
	if (m_db) {
		if (m_table && m_session) {
			if (m_table->getLock(m_session) != IL_NO)
				m_table->unlock(m_session, m_table->getLock(m_session));
			if (m_table->getMetaLock(m_session) != IL_NO)
				m_table->unlockMeta(m_session, m_table->getMetaLock(m_session));
			m_db->closeTable(m_session, m_table);
			m_table = NULL;
		}
		m_db->close();
		delete m_db;
		m_db = NULL;
	}
	File dir("dbtestdir");
	dir.rmdir(true);
}

void RecoverTestCase::testRecoveryFailed() {
	m_config.setBasedir("errorData");
	try {
		m_db = Database::open(&m_config, false, 1);
	} catch(NtseException &) {
	}
}

/** ����#14617: �ָ������жѲ�һ�µ���REDO MMS����ʱ�Ӷ��ж���¼ʧ�� */
void RecoverTestCase::testBug14617() {
	TableDefBuilder tdb(TableDef::INVALID_TABLEID, "space", "BlogCount");
	tdb.addColumn("id", CT_INT, false);
	tdb.addColumnS("a", CT_VARCHAR, Limits::PAGE_SIZE / 3, false);
	tdb.addIndex("pkey", true, true, false, "id", 0, NULL);
	TableDef *tableDef = tdb.getTableDef();
	tableDef->m_pctFree = TableDef::MIN_PCT_FREE;

	Database::create(&m_config);
	EXCPT_OPER(m_db = Database::open(&m_config, false));
	TblInterface ti(m_db, "BlogCount");
	EXCPT_OPER(ti.create(tableDef));
	ti.open();

	char *v = randomStr(Limits::PAGE_SIZE / 8);
	m_db->getPageBuffer()->disableScavenger();	// ��ֹд��ҳ��

	for (int i = 0; i < 7; i++)		// Ӧ������һ��ҳ��
		ti.insertRow(NULL, i, v);
	CPPUNIT_ASSERT_EQUAL((u64)(Limits::PAGE_SIZE * 4), ti.getTable()->getHeap()->getStatus().m_dataLength);	// ����1��ͷҳ��һ������λͼ��һ���ط�λͼ
	ti.checkpoint();

	// ���º�䳤��Ӧ�����ӳ�ȥ
	delete []v;
	v = randomStr(Limits::PAGE_SIZE / 3);
	ti.updateRows(1, 0, "a", v);
	CPPUNIT_ASSERT(ti.getTable()->getHeap()->getStatus().m_dataLength == Limits::PAGE_SIZE * 5);

	// ���ص�MMS
	ResultSet *rs = ti.selectRows(ti.buildRange(0, 1, true, true, 0, 0), NULL);
	CPPUNIT_ASSERT(rs->getNumRows() == 1);
	CPPUNIT_ASSERT(ti.getTable()->getMmsTable()->getStatus().m_records == 1);
	delete rs;
	rs = NULL;

	// ��������MMS
	CPPUNIT_ASSERT(ti.updateRows(ti.buildRange(0, 1, true, true, 0, 0), "a", v) == 1);
	CPPUNIT_ASSERT(ti.getTable()->getMmsTable()->getStatus().m_dirtyRecords == 1);

	// ɾ����¼
	CPPUNIT_ASSERT(ti.deleteRows(ti.buildRange(0, 1, true, true, 0, 0)) == 1);

	// �����¼��ԭ����λ��
	ti.insertRow(NULL, 7, v);
	rs = ti.selectRows(ti.buildLRange(0, 1, true, 7), NULL);
	CPPUNIT_ASSERT(rs->getNumRows() == 1);
	CPPUNIT_ASSERT(RID_GET_PAGE(rs->m_rids[0]) == 4);	// ��֤�ǲ��뵽�ڶ���ҳ��
	delete rs;
	rs = NULL;

	delete []v;
	v = NULL;

	// ǿ��д���ڶ���ҳ��
	Connection *conn = m_db->getConnection(false, "RecoverTestCase::testBug14617");
	Session *session = m_db->getSessionManager()->allocSession("RecoverTestCase::testBug14617", conn);
	BufferPageHandle *page = GET_PAGE(session, ti.getTable()->getHeap()->getHeapFile(), PAGE_HEAP, 4, Shared, ti.getTable()->getHeap()->getDBObjStats(), NULL);
	m_db->getPageBuffer()->writePage(session, page->getPage());
	session->releasePage(&page);
	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(conn);

	closeDatabase(true, false);
	
	EXCPT_OPER(m_db = Database::open(&m_config, false, 1));
	ti.reconnect(m_db);

	delete tableDef;
	tableDef = NULL;
}

/** �������ڲ������޸ĵ��»ָ�ʧ�� */
void RecoverTestCase::testModifyConfig() {
	TableDefBuilder tdb(TableDef::INVALID_TABLEID, "space", "BlogCount");
	tdb.addColumn("id", CT_INT, false);
	tdb.addColumnS("a", CT_VARCHAR, Limits::PAGE_SIZE / 3, false);
	tdb.addIndex("pkey", true, true, false, "id", 0, NULL);
	TableDef *tableDef = tdb.getTableDef();

	Database::create(&m_config);
	EXCPT_OPER(m_db = Database::open(&m_config, false));
	TblInterface ti(m_db, "BlogCount");
	EXCPT_OPER(ti.create(tableDef));
	ti.open();
	ti.insertRow(NULL, 1, "aaa");
	ti.close(true);

	closeDatabase(true, false);
	m_config.m_maxSessions--;
	try {
		Database::open(&m_config, false, 1);
		CPPUNIT_FAIL("Should can not get session");
	} catch (NtseException &) {}

	m_config.m_maxSessions++;
	EXCPT_OPER(m_db = Database::open(&m_config, false));

	delete tableDef;
}

/**
 * Ϊ�ָ�����׼����BlogCount��
 *
 * @param useMms �Ƿ�ʹ��MMS
 */
void RecoverTestCase::prepareBlogCount(bool useMms) {
	TableDef *tableDef = TableTestCase::getBlogCountDef(useMms);
	prepareTable(tableDef);
	delete tableDef;
}

void RecoverTestCase::prepareBlog(bool useMms) {
	TableDef *tableDef = TableTestCase::getBlogDef(useMms);
	prepareTable(tableDef);
	delete tableDef;
}

/**
 * �򿪱����򿪵ı�洢��m_table��
 *
 * @param path ��·��
 */
void RecoverTestCase::openTable(const char *path) throw(NtseException) {
	Connection *conn = m_db->getConnection(false);
	Session *session = m_db->getSessionManager()->allocSession("RecoverTestCase", conn);
	try {
		m_table = m_db->openTable(session, path);
		m_db->getSessionManager()->freeSession(session);
		m_db->freeConnection(conn);
	} catch (NtseException &e) {
		m_db->getSessionManager()->freeSession(session);
		m_db->freeConnection(conn);
		throw e;
	}
}

/**
 * �رղ���ʱʹ�õı�m_table
 */
void RecoverTestCase::closeTable() {
	Connection *conn = m_db->getConnection(false);
	Session *session = m_db->getSessionManager()->allocSession("RecoverTestCase", conn);
	
	m_db->closeTable(session, m_table);
	m_table = NULL;

	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(conn);
}

/**
 * ����һ����������ļ��������ļ���-bak��׺����
 *
 * @param path ��·���������basedir
 * @param hasLob ���Ƿ���������
 */
void RecoverTestCase::backupTable(const char *path, bool hasLob) {
	string base = string("dbtestdir") + NTSE_PATH_SEP + path;

	string from = base + Limits::NAME_HEAP_EXT;
	string to = from + "-bak";
	CPPUNIT_ASSERT(File::copyFile(to.c_str(), from.c_str(), false) == File::E_NO_ERROR);

	from = base + Limits::NAME_TBLDEF_EXT;
	to = from + "-bak";
	CPPUNIT_ASSERT(File::copyFile(to.c_str(), from.c_str(), false) == File::E_NO_ERROR);
	
	from = base + Limits::NAME_IDX_EXT;
	to = from + "-bak";
	CPPUNIT_ASSERT(File::copyFile(to.c_str(), from.c_str(), false) == File::E_NO_ERROR);

	if (hasLob) {
		from = base + Limits::NAME_LOBD_EXT;
		to = from + "-bak";
		CPPUNIT_ASSERT(File::copyFile(to.c_str(), from.c_str(), false) == File::E_NO_ERROR);

		from = base + Limits::NAME_LOBI_EXT;
		to = from + "-bak";
		CPPUNIT_ASSERT(File::copyFile(to.c_str(), from.c_str(), false) == File::E_NO_ERROR);

		from = base + Limits::NAME_SOBH_TBLDEF_EXT;
		to = from + "-bak";
		CPPUNIT_ASSERT(File::copyFile(to.c_str(), from.c_str(), false) == File::E_NO_ERROR);

		from = base + Limits::NAME_SOBH_EXT;
		to = from + "-bak";
		CPPUNIT_ASSERT(File::copyFile(to.c_str(), from.c_str(), false) == File::E_NO_ERROR);
	}
}

/**
 * ���ݱ��ݻָ�һ����������ļ��������ļ���-bak��׺����
 *
 * @param path ��·���������basedir
 * @param hasLob ���Ƿ���������
 */
void RecoverTestCase::restoreTable(const char *path, bool hasLob) {
	string base = string("dbtestdir") + NTSE_PATH_SEP + path;

	string to = base + Limits::NAME_HEAP_EXT;
	string from = to + "-bak";
	CPPUNIT_ASSERT(File::copyFile(to.c_str(), from.c_str(), true) == File::E_NO_ERROR);

	to = base + Limits::NAME_TBLDEF_EXT;
	from = to + "-bak";
	CPPUNIT_ASSERT(File::copyFile(to.c_str(), from.c_str(), true) == File::E_NO_ERROR);
	
	to = base + Limits::NAME_IDX_EXT;
	from = to + "-bak";
	CPPUNIT_ASSERT(File::copyFile(to.c_str(), from.c_str(), true) == File::E_NO_ERROR);

	if (hasLob) {
		to = base + Limits::NAME_LOBD_EXT;
		from = to + "-bak";
		CPPUNIT_ASSERT(File::copyFile(to.c_str(), from.c_str(), true) == File::E_NO_ERROR);

		to = base + Limits::NAME_LOBI_EXT;
		from = to + "-bak";
		CPPUNIT_ASSERT(File::copyFile(to.c_str(), from.c_str(), true) == File::E_NO_ERROR);

		to = base + Limits::NAME_SOBH_TBLDEF_EXT;
		from = to + "-bak";
		CPPUNIT_ASSERT(File::copyFile(to.c_str(), from.c_str(), true) == File::E_NO_ERROR);

		to = base + Limits::NAME_SOBH_EXT;
		from = to + "-bak";
		CPPUNIT_ASSERT(File::copyFile(to.c_str(), from.c_str(), true) == File::E_NO_ERROR);
	}
}

/**
 * Ϊ�ָ�����׼���ñ�
 *
 * @param tableDef ����
 */
void RecoverTestCase::prepareTable(TableDef *tableDef) {
	Database::create(&m_config);
	EXCPT_OPER(m_db = Database::open(&m_config, false));

	Connection *conn = m_db->getConnection(false);
	Session *session = m_db->getSessionManager()->allocSession("DatabaseTestCase::prepareTable", conn);

	m_db->createTable(session, tableDef->m_name, tableDef);

	m_db->checkpoint(session);

	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(conn);
}

/**
 * ��֤���������Ƿ���ָ����¼һ��
 *
 * @param table ��
 * @param records ��¼
 * @param numRows ��¼��
 */
void RecoverTestCase::checkRecords(Table *table, Record **records, uint numRows) {
	TableTestCase::verifyTable(m_db, table);

	loadConnectionAndSession();

	SubRecordBuilder srb(table->getTableDef(), REC_REDUNDANT);
	string allColNames = table->getTableDef()->m_columns[0]->m_name;
	for (u16 i = 1; i < table->getTableDef()->m_numCols; i++)
		allColNames = allColNames + " " + table->getTableDef()->m_columns[i]->m_name;
	SubRecord *subRec = srb.createEmptySbByName(table->getTableDef()->m_maxRecSize, allColNames.c_str());

	TblScan *scanHandle = m_table->tableScan(m_session, OP_READ, subRec->m_numCols, subRec->m_columns);
	for (uint n = 0; n < numRows; n++) {
		CPPUNIT_ASSERT(m_table->getNext(scanHandle, subRec->m_data));
		if (TableTestCase::compareRecord(m_table, records[n]->m_data, subRec->m_data, subRec->m_numCols, subRec->m_columns)) {
			m_table->endScan(scanHandle);
			CPPUNIT_ASSERT(!TableTestCase::compareRecord(m_table, records[n]->m_data, subRec->m_data, subRec->m_numCols, subRec->m_columns));
		}
	}
	CPPUNIT_ASSERT(!m_table->getNext(scanHandle, subRec->m_data));
	m_table->endScan(scanHandle);

	freeSubRecord(subRec);
}

/** ����������Ự���洢��m_conn��m_session��
 * @return true��ʾ��η����������Ự��false��ʾ������Ự�Ѿ����ڣ����û�з���
 */
bool RecoverTestCase::loadConnectionAndSession() {
	if (!m_conn) {
		assert(!m_session);
		m_conn = m_db->getConnection(false, __FUNCTION__);
		m_session = m_db->getSessionManager()->allocSession(__FUNCTION__, m_conn);
		return true;
	}
	return false;
}

/** �ͷ�m_conn��m_session */
void RecoverTestCase::freeConnectionAndSession() {
	if (m_session) {
		assert(m_conn);
		m_db->getSessionManager()->freeSession(m_session);
		m_db->freeConnection(m_conn);
	}
	m_session = NULL;
	m_conn = NULL;
}

/** �رձ����ݿ������ӵ���Դ
 * @param flushLog �Ƿ�ˢ��־
 * @param flushData �Ƿ�ˢ����
 */
void RecoverTestCase::closeDatabase(bool flushLog, bool flushData) {
	loadConnectionAndSession();
	if (m_table) {
		m_db->closeTable(m_session, m_table);
		m_table = NULL;
	}
	freeConnectionAndSession();
	m_db->close(flushLog, flushData);
	delete m_db;
	m_db = NULL;
}

/**
 * �ض���־
 *
 * @param logType �ض���־�ļ��д�����־�����������־
 * @param nth �ڵ�nth�γ��ִ�����־ʱ�ضϣ���0��ʼ���
 * @param inclusive Ϊtrue��ʾ�ڽضϺ���־�а���ָ������־�����򲻰���ָ����־
 */
void RecoverTestCase::truncateLog(LogType logType, int nth, bool inclusive) {
	EXCPT_OPER(m_db = Database::open(&m_config, false, -1));
	Txnlog *txnlog = m_db->getTxnlog();
	LogScanHandle *logScan = txnlog->beginScan(m_db->getControlFile()->getCheckpointLSN(), txnlog->lastLsn());
	int n = 0;
	while (txnlog->getNext(logScan)) {
		const LogEntry *logEntry = logScan->logEntry();
		LogType type = logEntry->m_logType;
		if (type == logType) {
			if (n == nth) {
				if (inclusive) {
					if (txnlog->getNext(logScan))
						txnlog->truncate(logScan->curLsn());
				} else {
					txnlog->truncate(logScan->curLsn());
				}
				break;
			}
			n++;
		}
	}
	txnlog->endScan(logScan);
	EXCPT_OPER(m_db->close(false, false));
	delete m_db;
	m_db = NULL;
}

/**
 * �ͷŶ���REC_MYSQL��ʽ�ļ�¼�������ͷŴ����ռ�õĿռ�
 *
 * @param tableDef ��¼��������
 * @param records ��¼����
 * @param numRows ��¼��
 */
void RecoverTestCase::freeEngineMySQLRecords(const TableDef *tableDef, Record **records, uint numRows) {
	for (uint i = 0; i < numRows; i++)
		freeEngineMySQLRecord(tableDef, records[i]);
	delete [] records;
}

/**
 * �ͷ�һ��REC_MYSQL��ʽ�ļ�¼���������ͷŴ����ռ�õĿռ�
 *
 * @param tableDef ��¼��������
 * @param record ��¼����
 */
void RecoverTestCase::freeEngineMySQLRecord(const TableDef *tableDef, Record *record) {
	assert(record->m_format == REC_MYSQL);
	::freeEngineMysqlRecord(tableDef, record);
}


/**
 * �ͷŶ���REC_UPPMYSQL��ʽ�ļ�¼�������ͷŴ����ռ�õĿռ�
 *
 * @param tableDef ��¼��������
 * @param records ��¼����
 * @param numRows ��¼��
 */
void RecoverTestCase::freeUppMySQLRecords(const TableDef *tableDef, Record **records, uint numRows) {
	for (uint i = 0; i < numRows; i++)
		freeUppMySQLRecord(tableDef, records[i]);
	delete [] records;
}

/**
 * �ͷ�һ��REC_UPPMYSQL��ʽ�ļ�¼���������ͷŴ����ռ�õĿռ�
 *
 * @param tableDef ��¼��������
 * @param record ��¼����
 */
void RecoverTestCase::freeUppMySQLRecord(const TableDef *tableDef, Record *record) {
	assert(record->m_format == REC_UPPMYSQL);
	::freeUppMysqlRecord(tableDef, record);
}


/**
 * �ͷŶ���REC_MYSQL��ʽ�ļ�¼�������ͷŴ����ռ�õĿռ�
 *
 * @param tableDef ��¼��������
 * @param records ��¼����
 * @param numRows ��¼��
 */
void RecoverTestCase::freeMySQLRecords(const TableDef *tableDef, Record **records, uint numRows) {
	for (uint i = 0; i < numRows; i++)
		freeMySQLRecord(tableDef, records[i]);
	delete [] records;
}

/**
 * �ͷ�һ��REC_MYSQL��ʽ�ļ�¼���������ͷŴ����ռ�õĿռ�
 *
 * @param tableDef ��¼��������
 * @param record ��¼����
 */
void RecoverTestCase::freeMySQLRecord(const TableDef *tableDef, Record *record) {
	assert(record->m_format == REC_MYSQL);
	::freeMysqlRecord(tableDef, record);
}