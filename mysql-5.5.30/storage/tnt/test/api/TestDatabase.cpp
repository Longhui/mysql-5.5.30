#include "api/TestDatabase.h"
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
#include "TestHelper.h"

using namespace std;
using namespace ntse;

const char* DatabaseTestCase::getName() {
	return "Database test";
}

const char* DatabaseTestCase::getDescription() {
	return "Test database operations such as create/drop/open/close database and create/drop tables.";
}

bool DatabaseTestCase::isBig() {
	return false;
}

void DatabaseTestCase::setUp() {
	m_db = NULL;
	File dir("dbtestdir");
	dir.rmdir(true);
	dir.mkdir();
	m_config.setBasedir("dbtestdir");
}

void DatabaseTestCase::tearDown() {
	if (m_db) {
		m_db->close();
		delete m_db;
		m_db = NULL;
	}
	File dir("dbtestdir");
	dir.rmdir(true);
}

/** ���Դ���/ɾ��/�ر�/�����ݿ�Ȼ������� */
void DatabaseTestCase::testBasic() {
	Database::create(&m_config);
	m_db = Database::open(&m_config, false);
	m_db->close();
	delete m_db;
	m_db = NULL;

	m_db = Database::open(&m_config, false);
	m_db->close();
	delete m_db;
	m_db = NULL;
	Database::drop(m_config.m_basedir);
}

/** ����ҳ�滺��ֲ�ͳ�ƹ��� */
void DatabaseTestCase::testBufferDistr() {
	Database::create(&m_config);
	m_db = Database::open(&m_config, false);
	TableDef *tableDef1 = TableTestCase::getBlogDef(true);
	TableDef *tableDef2 = TableTestCase::getBlogDef(true);

	Connection *conn = m_db->getConnection(false);
	Session *session = m_db->getSessionManager()->allocSession("testBufferDistr", conn);
	m_db->createTable(session, "blog1", tableDef1);
	m_db->createTable(session, "blog2", tableDef2);
	delete tableDef1;
	delete tableDef2;
	Table *t1 = m_db->openTable(session, "blog1");
	Table *t2 = m_db->openTable(session, "blog2");
	uint numRows = 5;
	// ���ɵļ�¼����Ϊ249�ֽڣ�Abstract������СΪ107��Content������Ϊ���ʹ����ż����ΪС�ʹ����
	// Content���ʹ����ÿ��ռ��3��ҳ
	size_t recSize = 249 * numRows;
	uint smallCnt = numRows / 2;
	uint largeCnt = numRows - smallCnt;
	size_t slobSize = 107 * numRows + TableTestCase::SMALL_LOB_SIZE * smallCnt;
	Record **rows1 = TableTestCase::populateBlog(m_db, t1, numRows, false, true);
	Record **rows2 = TableTestCase::populateBlog(m_db, t2, numRows, false, true);

	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(conn);
	m_db->close(true, true);
	delete m_db;
	m_db = NULL;

	m_db = Database::open(&m_config, false);
	conn = m_db->getConnection(false);
	session = m_db->getSessionManager()->allocSession("testBufferDistr", conn);
	t1 = m_db->openTable(session, "blog1");
	t2 = m_db->openTable(session, "blog2");

	// ���ر����ݵ��ڴ���
	TblInterface ti1(m_db, "blog1");
	TblInterface ti2(m_db, "blog2");
	ti1.open();
	ti2.open();
	ResultSet *rs = ti1.selectRows(IdxRange(t1->getTableDef(), 0), NULL);
	delete rs;
	rs = ti1.selectRows(IdxRange(t1->getTableDef(), 1), NULL);
	delete rs;
	rs = ti1.selectRows(IdxRange(t1->getTableDef(), 2), NULL);
	delete rs;
	rs = ti2.selectRows(IdxRange(t1->getTableDef(), 0), NULL);
	delete rs;
	rs = ti2.selectRows(IdxRange(t1->getTableDef(), 1), NULL);
	delete rs;
	rs = ti2.selectRows(IdxRange(t1->getTableDef(), 2), NULL);
	delete rs;
	ti1.close();
	ti2.close();
	

	// ��֤�ֲ��Ƿ���ȷ������û�з����滻�����򽫺�����֤
	CPPUNIT_ASSERT(m_db->getPageBuffer()->getCurrentSize() < m_db->getPageBuffer()->getTargetSize());
	Array<BufUsage *> *usageArr = m_db->getBufferDistr(session);
	printBufUsages(usageArr);
	CPPUNIT_ASSERT(getBufPages(usageArr, t1->getTableDef()->m_id, DBO_Heap) == 3 + recSize / Limits::PAGE_SIZE);
	CPPUNIT_ASSERT(getBufPages(usageArr, t1->getTableDef()->m_id, DBO_Indice) == 1);
	CPPUNIT_ASSERT(getBufPages(usageArr, t1->getTableDef()->m_id, DBO_LlobDir) == 2);
	CPPUNIT_ASSERT(getBufPages(usageArr, t1->getTableDef()->m_id, DBO_Slob) == 3 + slobSize / Limits::PAGE_SIZE);
	CPPUNIT_ASSERT(getBufPages(usageArr, t1->getTableDef()->m_id, DBO_Index, "IDX_BLOG_PUTTIME") == 1);
	CPPUNIT_ASSERT(getBufPages(usageArr, t1->getTableDef()->m_id, DBO_Index, "PRIMARY") == 1);
	CPPUNIT_ASSERT(getBufPages(usageArr, t1->getTableDef()->m_id, DBO_Index, "IDX_BLOG_PUBTIME") == 1);
	CPPUNIT_ASSERT(getBufPages(usageArr, t1->getTableDef()->m_id, DBO_LlobDat) == largeCnt * 3);
	freeBufUsages(usageArr);

	for (int i = 0; i < 5; i++)
		freeUppMysqlRecord(t1->getTableDef(), rows1[i]);
	delete [] rows1;
	for (int i = 0; i < 5; i++)
		freeUppMysqlRecord(t1->getTableDef(), rows2[i]);
	delete [] rows2;
	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(conn);
	m_db->close(true, true);
	delete m_db;
	m_db = NULL;

	m_db = Database::open(&m_config, false);
	conn = m_db->getConnection(false);
	session = m_db->getSessionManager()->allocSession("testBufferDistr", conn);
	t1 = m_db->openTable(session, "blog1");
	t2 = m_db->openTable(session, "blog2");

	usageArr = m_db->getBufferDistr(session);
	printBufUsages(usageArr);
	CPPUNIT_ASSERT(getBufPages(usageArr, t1->getTableDef()->m_id, DBO_Heap) == 2);
	CPPUNIT_ASSERT(getBufPages(usageArr, t1->getTableDef()->m_id, DBO_Indice) == 1);
	CPPUNIT_ASSERT(getBufPages(usageArr, t1->getTableDef()->m_id, DBO_LlobDir) == 1);
	CPPUNIT_ASSERT(getBufPages(usageArr, t1->getTableDef()->m_id, DBO_Slob) == 2);
	freeBufUsages(usageArr);

	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(conn);
	m_db->close(true, true);
	delete m_db;
	m_db = NULL;
}

/* �����Ƿ���NTSE�ļ� */
void DatabaseTestCase::testIsNtseFile() {
	CPPUNIT_ASSERT(Database::isFileOfNtse("a.nsi"));
	CPPUNIT_ASSERT(Database::isFileOfNtse("a.nsd"));
	CPPUNIT_ASSERT(Database::isFileOfNtse("a.nsso"));
	CPPUNIT_ASSERT(Database::isFileOfNtse("a.nsli"));
	CPPUNIT_ASSERT(Database::isFileOfNtse("a.nsld"));
	CPPUNIT_ASSERT(Database::isFileOfNtse("a.nsld"));
	CPPUNIT_ASSERT(Database::isFileOfNtse("ntse_ctrl"));
	CPPUNIT_ASSERT(Database::isFileOfNtse("ntse_log.0"));
	CPPUNIT_ASSERT(Database::isFileOfNtse("ntse.log"));
	CPPUNIT_ASSERT(Database::isFileOfNtse("a.ndic"));
	CPPUNIT_ASSERT(Database::isFileOfNtse("a.tmpndic"));
}

void DatabaseTestCase::printBufUsages(Array<BufUsage*> *usageArr) {
	cout << "== begin dump buffer distribution" << endl;
	for (size_t i = 0; i < usageArr->getSize(); i++) {
		BufUsage *usage = (*usageArr)[i];
		cout << "  " << usage->m_tblId << "," << usage->m_tblSchema << "." << usage->m_tblName << ",";
		cout << BufUsage::getTypeStr(usage->m_type) << "," << usage->m_idxName << "," << usage->m_numPages;
		cout << endl;
	}
}

void DatabaseTestCase::freeBufUsages(Array<BufUsage*> *usageArr) {
	for (size_t i = 0; i < usageArr->getSize(); i++)
		delete (*usageArr)[i];
	delete usageArr;
}

u64 DatabaseTestCase::getBufPages(Array<BufUsage *> *usageArr, u16 tblId, DBObjType type, const char *idxName) {
	for (size_t i = 0; i < usageArr->getSize(); i++) {
		BufUsage *usage = (*usageArr)[i];
		if (usage->m_tblId == tblId && usage->m_type == type && !strcmp(usage->m_idxName, idxName))
			return usage->m_numPages;
	}
	return 0;
}
