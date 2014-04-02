#include "api/TestBackup.h"
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
#include "lob/LobIndex.h"
#include "btree/Index.h"
#include <string>
#include <iostream>
#include "Test.h"

using namespace std;
using namespace ntse;


LsnType backupDb(Database *db, const char *backupDir);

#define BLOG_ID					"ID"
#define BLOG_UserID				"UserID"
#define BLOG_PublishTime		"PublishTime"
#define BLOG_Title				"Title"
#define BLOG_Abstract			"Abstract"
#define BLOG_Content			"Content"

#define BLOG_ID_CNO				0
#define BLOG_UserID_CNO			1
#define BLOG_PublishTime_CNO	2
#define BLOG_Title_CNO			3
#define BLOG_Abstract_CNO		4
#define BLOG_Content_CNO		5
/*
CREATE TABLE Blog (
ID bigint NOT NULL,
UserID bigint NOT NULL,
PublishTime bigint,
Title varchar(255),
Abstract text,
Content mediumtext,
PRIMARY KEY (ID),
KEY IDX_BLOG_PUTTIME(PublishTime),
KEY IDX_BLOG_PUBTIME(UserID, PublishTime)
);
*/
class BlogTable {
public:
	static void create(Database *db, bool useMms = false, const string& mysqldb = "");
	static BlogTable *open(Database *db, const string& mysqldb = "");
	void close();
	static TableDef *makeTableDef(bool useMms = false);
	TableDef *getTableDef();
	Table *getTable();
	void populate(u64 startBlogId, uint numRows);
	void randUpdateGE(u64 blogId);
	void randDeleteGE(u64 blogId);
private:
	Database *m_db;
	Session *m_session;
	Connection *m_conn;
	Table *m_table;
};

void BlogTable::create(ntse::Database *db, bool useMms, const string& mysqldb) {
	Connection *conn = db->getConnection(false);
	Session *session = db->getSessionManager()->allocSession("BlogTable::create", conn);
	string path = (mysqldb == "") ? "blog" : mysqldb + NTSE_PATH_SEP + "blog";
	
	TableDef *tableDef = makeTableDef(useMms);

	EXCPT_OPER(db->createTable(session, path.c_str(), tableDef));
	string frmpath = string(db->getConfig()->m_basedir) + NTSE_PATH_SEP + path + ".frm";
	File frmFile(frmpath.c_str());
	frmFile.create(false, false);
	frmFile.close();

	db->getSessionManager()->freeSession(session);
	db->freeConnection(conn);
	delete tableDef;
}

BlogTable *BlogTable::open(Database *db, const string& mysqldb) {
	BlogTable *blogTable = new BlogTable();
	blogTable->m_db = db;
	blogTable->m_conn = db->getConnection(false);
	blogTable->m_session = db->getSessionManager()->allocSession("BlogTable::open", blogTable->m_conn);
	string path = (mysqldb == "") ? "blog" : mysqldb + NTSE_PATH_SEP + "blog";
	blogTable->m_table = db->openTable(blogTable->m_session, path.c_str());

	return blogTable;
}

void BlogTable::close() {
	m_db->closeTable(m_session, m_table);
	m_db->getSessionManager()->freeSession(m_session);
	m_db->freeConnection(m_conn);
}

TableDef *BlogTable::makeTableDef(bool useMms /* = false */) {
	TableDefBuilder *builder = new TableDefBuilder(1, "space", "Blog");
	builder->addColumn("ID", CT_BIGINT, false)->addColumn("UserID", CT_BIGINT, false);
	builder->addColumn("PublishTime", CT_BIGINT);
	builder->addColumnS("Title", CT_VARCHAR, 255)->addColumn("Abstract", CT_SMALLLOB)->addColumn("Content", CT_MEDIUMLOB);
	builder->addIndex("PRIMARY", true, true, false, "ID", 0, NULL);
	builder->addIndex("IDX_BLOG_PUTTIME", false, false, false, "PublishTime", 0, NULL);
	builder->addIndex("IDX_BLOG_PUBTIME", false, false, false, "UserID", 0, "PublishTime", 0, NULL);
	TableDef *tableDef = builder->getTableDef();
	tableDef->m_id = TableDef::INVALID_TABLEID;
	tableDef->m_useMms = useMms;
	tableDef->m_incrSize = TableDef::MIN_INCR_SIZE;
	delete builder;
	return tableDef;
}

void makeRandString(char *str, size_t size) {
	for (size_t l = 0; l < size - 1; l++)
		str[l] = (char )('A' + System::random() % 26);
	str[size - 1] = '\0';
}

void BlogTable::populate(u64 startBlogId, uint numRows) {
	SubRecordBuilder srb(m_table->getTableDef(), REC_REDUNDANT, RID(0, 0));
	RedRecord redRecord(m_table->getTableDef());
	for (uint n = 0; n < numRows; ++n) {
		redRecord.writeNumber(BLOG_ID_CNO, startBlogId + n);
		redRecord.writeNumber(BLOG_UserID_CNO, startBlogId + n);
		redRecord.writeNumber(BLOG_PublishTime_CNO, (u64)System::currentTimeMillis());
		char title[25];
		makeRandString(title, sizeof(title));
		redRecord.writeChar(BLOG_Title_CNO, title, sizeof(title));
		// С�ʹ����
		char abs[100];
		makeRandString(abs, sizeof(abs));
		redRecord.writeLob(BLOG_Abstract_CNO, (byte *)abs, sizeof(abs));
		// ���ʹ����
		char content[Limits::PAGE_SIZE];
		makeRandString(content, sizeof(content));
		const char *watermark = "1234";
		strcpy(content + sizeof(content) - strlen(watermark), watermark);
		redRecord.writeLob(BLOG_Content_CNO, (byte *)content, sizeof(content));
		Connection *conn = m_db->getConnection(false);
		Session *session = m_db->getSessionManager()->allocSession("BlogTable::populate", conn);
		uint dupIndex;
		CPPUNIT_ASSERT((m_table->insert(session, redRecord.getRecord()->m_data, true, &dupIndex)) != INVALID_ROW_ID);
		m_db->getSessionManager()->freeSession(session);
		m_db->freeConnection(conn);
	}
	// ������������ȥ�ͷ��ڴ�
	redRecord.setNull(BLOG_Abstract_CNO);
	redRecord.setNull(BLOG_Content_CNO);
}



/**
 * �������>= blogId�ļ�¼
 */
void BlogTable::randUpdateGE(u64 blogId) {
	Connection *conn = m_db->getConnection(false);
	Session *session = m_db->getSessionManager()->allocSession(__FUNCTION__, conn);

	SubRecordBuilder keyBuilder(m_table->getTableDef(), KEY_PAD);
	SubRecord *key = keyBuilder.createSubRecordByName(BLOG_ID, &blogId);
	key->m_rowId = INVALID_ROW_ID;
	u16 columns[] = {BLOG_ID_CNO, BLOG_PublishTime_CNO, BLOG_Abstract_CNO};


	RedRecord updRedRec(m_table->getTableDef());
	updRedRec.writeNumber(BLOG_PublishTime_CNO, System::currentTimeMillis());
	char abs[100];
	makeRandString(abs, sizeof(abs));
	updRedRec.writeLob(BLOG_Abstract_CNO, (byte *)abs, sizeof(abs));

	IndexScanCond cond(0, key, true, true, false);
	Record *record = RecordBuilder::createEmptyRecord(INVALID_ROW_ID, REC_MYSQL, m_table->getTableDef()->m_maxRecSize);
	TblScan *scanHandle = m_table->indexScan(session, OP_UPDATE, &cond, (u16)(sizeof(columns)/sizeof(columns[0])), columns);
	u16 updCols[] = {BLOG_PublishTime_CNO, BLOG_Abstract_CNO};
	scanHandle->setUpdateColumns((u16)(sizeof(updCols)/sizeof(updCols[0])), updCols);
	if (m_table->getNext(scanHandle, record->m_data)) {
		m_table->updateCurrent(scanHandle, updRedRec.getRecord()->m_data);
	}
	m_table->endScan(scanHandle);
	updRedRec.setNull(BLOG_Abstract_CNO);
	freeSubRecord(key);
	freeRecord(record);
	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(conn);
}
/**
 * ���ɾ��>= blogId�ļ�¼
 */
void BlogTable::randDeleteGE(u64 blogId) {
	Connection *conn = m_db->getConnection(false);
	Session *session = m_db->getSessionManager()->allocSession(__FUNCTION__, conn);

	SubRecordBuilder keyBuilder(m_table->getTableDef(), KEY_PAD);
	SubRecord *key = keyBuilder.createSubRecordByName(BLOG_ID, &blogId);
	key->m_rowId = INVALID_ROW_ID;
	u16 columns[] = {BLOG_ID_CNO};
	IndexScanCond cond(0, key, true, true, false);
	Record *record = RecordBuilder::createEmptyRecord(INVALID_ROW_ID, REC_MYSQL, m_table->getTableDef()->m_maxRecSize);
	TblScan *scanHandle = m_table->indexScan(session, OP_DELETE, &cond, (u16)(sizeof(columns)/sizeof(columns[0])), columns);
	if (m_table->getNext(scanHandle, record->m_data)) {
		m_table->deleteCurrent(scanHandle);
	}
	m_table->endScan(scanHandle);
	freeSubRecord(key);
	freeRecord(record);
	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(conn);
}
//////////////////////////////////////////////////////////////////////////
class BlogInsertThread: public Thread {
public:
	BlogInsertThread(Database *db, BackupTest *backupTest, uint ops)
		: Thread("BlogInsertThread"), m_db(db)
		, m_backupTest(backupTest), m_numOp(ops) {

	}

	void run() {
		const uint step = 10;
		BlogTable *blogTable = BlogTable::open(m_db);
		for (uint i = 0; i < m_numOp; i += step) {
			LOCK(&m_backupTest->m_mutex);
			u64 blogId = m_backupTest->m_blogId;
			m_backupTest->m_blogId += step;
			UNLOCK(&m_backupTest->m_mutex);
			blogTable->populate(blogId, step);
		}
		blogTable->close();
		delete blogTable;
	}
private:
	BackupTest *m_backupTest;
	Database *m_db;
	uint m_numOp;
};

class BlogUpdateThread: public Thread {
public:
	BlogUpdateThread(Database *db, BackupTest *backupTest, uint ops)
		: Thread("BlogUpdateThread"), m_db(db)
		, m_backupTest(backupTest), m_numOp(ops) {

	}
	void run() {
		BlogTable *blogTable = BlogTable::open(m_db);
		for (uint i = 0; i < m_numOp; ++i) {
			blogTable->randUpdateGE((u64)System::random() % m_backupTest->m_blogId);
		}
		blogTable->close();
		delete blogTable;
	}
private:
	BackupTest *m_backupTest;
	Database *m_db;
	uint m_numOp;
};

class BlogDeleteThread: public Thread {
public:
	BlogDeleteThread(Database *db, BackupTest *backupTest, uint ops)
		: Thread("BlogDeleteThread"), m_backupTest(backupTest), m_db(db)
		,  m_numOp(ops) {

	}
	void run() {
		BlogTable *blogTable = BlogTable::open(m_db);
		for (uint i = 0; i < m_numOp; ++i) {
			blogTable->randDeleteGE((u64)System::random() % m_backupTest->m_blogId);
		}
		blogTable->close();
		delete blogTable;
	}
private:
	BackupTest *m_backupTest;
	Database *m_db;
	uint m_numOp;
};

class BackupThread: public Thread {
public:
	BackupThread(Database *db, BackupTest *backupTest)
		: Thread("BackupThread"), m_backupTest(backupTest), m_db(db)
	{

	}

	void run() {
		this->msleep(500);
		EXCPT_OPER(m_backupTest->m_backupTailLsn  = backupDb(m_db, m_backupTest->m_backupDir.c_str()));
	}

private:
	BackupTest *m_backupTest;
	Database *m_db;
};

class CkptThread: public Thread {
public:
	CkptThread(Thread *backupThread, BackupTest *backupTest, Database *db, uint ops)
		: Thread("CkptThread"), m_backupThread(backupThread)
		, m_backupTest(backupTest), m_db(db), m_numOp(ops) {
	}

	void run() {
		m_backupThread->enableSyncPoint(SP_DB_BEFORE_BACKUP_CTRLFILE);
		m_backupThread->enableSyncPoint(SP_DB_BEFORE_BACKUP_LOG);

		m_backupThread->joinSyncPoint(SP_DB_BEFORE_BACKUP_CTRLFILE);
		checkpoint();
		m_backupThread->disableSyncPoint(SP_DB_BEFORE_BACKUP_CTRLFILE);
		m_backupThread->notifySyncPoint(SP_DB_BEFORE_BACKUP_CTRLFILE);

		m_backupThread->joinSyncPoint(SP_DB_BEFORE_BACKUP_LOG);
		checkpoint();
		m_backupThread->disableSyncPoint(SP_DB_BEFORE_BACKUP_LOG);
		m_backupThread->notifySyncPoint(SP_DB_BEFORE_BACKUP_LOG);
	}

	void checkpoint() {
		
		Connection *conn = m_db->getConnection(false, "CkptThread connection");
		Session *session = m_db->getSessionManager()->allocSession("CkptThread session", conn);
		m_db->checkpoint(session);
		m_db->getSessionManager()->freeSession(session);
		m_db->freeConnection(conn);
		BlogTable *blogTable = BlogTable::open(m_db);
		for (uint i = 0; i < m_numOp; ++i) {
			blogTable->randUpdateGE((u64)System::random() % m_backupTest->m_blogId);
		}
		blogTable->close();
		delete blogTable;
	}

private:
	Thread *m_backupThread;
	BackupTest *m_backupTest;
	Database *m_db;
	uint m_numOp;
};
//////////////////////////////////////////////////////////////////////////
const char* BackupTestCase::getName() {
	return "Backup/Restore test";
}

const char* BackupTestCase::getDescription() {
	return "Test database backup and restore.";
}

bool BackupTestCase::isBig() {
	return false;
}

void BackupTestCase::setUp() {
	m_db = 0;
	Database::drop(".");
	initBackupTest();
}

void BackupTestCase::tearDown() {
	if (m_db)
		m_db->close();
	Database::drop(".");
	destroyBackupTest();
}

/** �������ݿ⵽ָ��Ŀ¼ */
LsnType backupDb(Database *db, const char *backupDir) {
	BackupProcess *bp = 0;
	try {
		bp = db->initBackup(backupDir);
		db->doBackup(bp);
		db->finishingBackupAndLock(bp);
	} catch(NtseException &e) {
		printf("Error: %s\n", e.getMessage());
		CPPUNIT_ASSERT(false);
		db->doneBackup(bp);
		assert(false);
	}
	LsnType tailLsn = bp->m_tailLsn;
	db->doneBackup(bp);
	return tailLsn;
}

/** ������־�ļ� */
void backupTxnlog(Database *db, const char *backupDir, LsnType startLsn) throw(NtseException) {
	LogBackuper logBackuper(db->getTxnlog(), startLsn);
	u64 fileSize = logBackuper.getSize();
	string backuppath = string(backupDir) + NTSE_PATH_SEP + Limits::NAME_TXNLOG;
	File backupFile(backuppath.c_str());
	backupFile.remove();
	u64 errNo = backupFile.create(false, false);
	if (errNo != File::E_NO_ERROR)
		NTSE_THROW(errNo, "Create log file %s failed", backupFile.getPath());
	if ((errNo = backupFile.setSize(fileSize)) != File::E_NO_ERROR)
		NTSE_THROW(errNo, "setSize "I64FORMAT"u on %s failed", fileSize, backupFile.getPath());

	uint bufPageCnt = 256;
	AutoPtr<byte> buf(new byte[bufPageCnt * Limits::PAGE_SIZE], true);
	uint pageCnt;
	u64 writtenSize = 0;
	while ((pageCnt = logBackuper.getPages(buf, bufPageCnt, true))) {
		u64 errNo;
		uint dataSize = pageCnt * LogConfig::LOG_PAGE_SIZE;
		if (writtenSize + dataSize > fileSize) { // ��չ�ļ�
			if ((errNo = backupFile.setSize(writtenSize + dataSize)) != File::E_NO_ERROR)
				NTSE_THROW(errNo, "setSize of backup file %s failed", backupFile.getPath());
		}
		errNo = backupFile.write(writtenSize, dataSize, (byte *)buf);
		if (errNo != File::E_NO_ERROR)
			NTSE_THROW(errNo, "write backup file %s failed", backupFile.getPath());
		writtenSize += dataSize;
	}
	backupFile.close();
}

inline bool compareFileVerbose(File *src, File *dst, u64 start = 0, bool ignoreLsn = false) {
	bool same = compareFile(src, dst, start, (u64)-1, ignoreLsn);
	if (!same)
		cerr << src->getPath() << " != " << dst->getPath() << endl;
	return same;
}

/** �Ƚϱ��Ƿ���ͬ */
bool compareTable(Table *t1, Table *t2) {
	const int maxFileCnt = 3;
	AutoPtr<File *> files1(new File*[maxFileCnt], true);
	AutoPtr<PageType> pageTypes1(new PageType[maxFileCnt], true);
	AutoPtr<File *> files2(new File*[maxFileCnt], true);
	AutoPtr<PageType> pageTypes2(new PageType[maxFileCnt], true);
	// �Ƚ϶��ļ�
	DrsHeap *heap1 = t1->getHeap();
	DrsHeap *heap2 = t2->getHeap();
	int fileCnt1 = heap1->getFiles(files1, pageTypes1, maxFileCnt);
	int fileCnt2 = heap2->getFiles(files2, pageTypes2, maxFileCnt);
	assert(fileCnt1 == fileCnt2);
	for (int i = 0; i < fileCnt1; ++i)
		if (!compareFileVerbose(files1[i], files2[i], 0, t1->getTableDef()->m_useMms))
			return false;;

	LobStorage *lobStore1 = t1->getLobStorage();
	LobStorage *lobStore2 = t2->getLobStorage();
	if (lobStore1) { // �Ƚϴ��������
		int fileCnt1 = lobStore1->getFiles(files1, pageTypes1, maxFileCnt);
		int fileCnt2 = lobStore2->getFiles(files2, pageTypes2, maxFileCnt);
		assert(fileCnt1 == fileCnt2);
		for (int i = 0; i < fileCnt1; ++i) {
			u64 start = 0;
			bool ignoreLsn = false;
			string path(files1[i]->getPath());
			size_t dotPos = path.rfind(".");
			if (dotPos != string::npos && path.substr(dotPos) == Limits::NAME_LOBI_EXT) {
				// ����������ļ���ҳ�е�ͳ����Ϣ���ܲ�׼ȷ����Ҫ���⴦��
				byte* pageBuf1 = (byte *)System::virtualAlloc(Limits::PAGE_SIZE);
				byte* pageBuf2 = (byte *)System::virtualAlloc(Limits::PAGE_SIZE);
				u64 errNo;
				if ((errNo = files1[i]->read(0, Limits::PAGE_SIZE, pageBuf1)) != File::E_NO_ERROR)
					NTSE_THROW(errNo, "read file %s failed", files1[i]->getPath());
				if ((errNo = files2[i]->read(0, Limits::PAGE_SIZE, pageBuf2)) != File::E_NO_ERROR)
					NTSE_THROW(errNo, "read file %s failed", files2[i]->getPath());
				LIFileHeaderPageInfo *hdrPage1 = (LIFileHeaderPageInfo *)pageBuf1;
				LIFileHeaderPageInfo *hdrPage2 = (LIFileHeaderPageInfo *)pageBuf2;
				if (hdrPage1->m_bph.m_lsn != hdrPage2->m_bph.m_lsn
					|| hdrPage1->m_fileLen != hdrPage2->m_fileLen
					|| hdrPage1->m_firstFreePageNum != hdrPage2->m_firstFreePageNum 
					|| hdrPage1->m_blobFileLen != hdrPage2->m_blobFileLen
					|| hdrPage1->m_blobFileTail != hdrPage2->m_blobFileTail
					|| hdrPage1->m_tableId != hdrPage2->m_tableId)
					return false;
				start = Limits::PAGE_SIZE;
				System::virtualFree(pageBuf1);
				System::virtualFree(pageBuf2);
			} else if (dotPos != string::npos && path.substr(dotPos) == Limits::NAME_SOBH_EXT) {
				// С�ʹ����
				ignoreLsn = t1->getTableDef()->m_useMms;
			}
			if (!compareFileVerbose(files1[i], files2[i], start, ignoreLsn))
				return false;
		}
	}
	DrsIndice *indice1 = t1->getIndice();
	DrsIndice *indice2 = t2->getIndice();
	if (indice1) { // ������������
		int fileCnt1 = indice1->getFiles(files1, pageTypes1, maxFileCnt);
		int fileCnt2 = indice1->getFiles(files2, pageTypes2, maxFileCnt);
		for (int i = 0; i < fileCnt1; ++i)
			if (!compareFileVerbose(files1[i], files2[i]))
				return false;
	}
	return true;
}
/** �Ƚ����ݿ��Ƿ���ͬ */
bool compareDb(Database *db1, Database *db2) {
	Connection *conn1 = db1->getConnection(false);
	Session *session1 = db1->getSessionManager()->allocSession("compareDb", conn1);
	Connection *conn2 = db2->getConnection(false);
	Session *session2 = db2->getSessionManager()->allocSession("compareDb", conn2);

	db1->getPageBuffer()->flushAll(session1);
	db2->getPageBuffer()->flushAll(session2);
	// ��ȡ����Ϣ
	ControlFile *ctrlFile1 = db1->getControlFile();
	u16 numTables = ctrlFile1->getNumTables();
	AutoPtr<u16> tableIds1(new u16[numTables], true);
	ctrlFile1->listAllTables(tableIds1, numTables);

	ControlFile *ctrlFile2 = db2->getControlFile();
	if (numTables != ctrlFile2->getNumTables()) {
		db1->getSessionManager()->freeSession(session1);
		db1->freeConnection(conn1);
		db2->getSessionManager()->freeSession(session2);
		db2->freeConnection(conn2);
		return false;
	}
	// �Ƚ�ÿ�ű������
	bool res = true;
	for (u16 i = 0; i < numTables; ++i) {
		u16 tableId = tableIds1[i];
		string path1 = ctrlFile1->getTablePath(tableId);
		string path2 = ctrlFile2->getTablePath(tableId);
		Table *table1 = db1->openTable(session1, path1.c_str());
		Table *table2 = db2->openTable(session2, path2.c_str());
		if (!compareTable(table1, table2)) {
			res = false;
			break;
		}
	}
	db1->getSessionManager()->freeSession(session1);
	db1->freeConnection(conn1);
	db2->getSessionManager()->freeSession(session2);
	db2->freeConnection(conn2);
	return res;
}


void BackupTestCase::initBackupTest() {
	m_backupTest.setUp();
}

void BackupTestCase::destroyBackupTest() {
	m_backupTest.tearDown();
}


void BackupTestCase::verifyDb() {
	assert(!m_db);
	EXCPT_OPER(m_db = Database::open(&m_backupTest.m_dbConfig, false));
	m_db->close();
	delete m_db;
	m_db = 0;
	// ��֤���ݿ��д��ȷ��

}

/**
 * �����������ݿ�
 *	1. �����ݿ�
 *	2. checkpoint = 0�����ݿ�
 */
void BackupTestCase::testBackupSpecial() {
	// �����ݿ�
	{
		EXCPT_OPER(m_db = Database::open(&m_backupTest.m_dbConfig, true));
		backupDb(m_db, m_backupTest.m_backupDir.c_str());
		m_db->close();
		delete m_db;
		m_db = 0;
		EXCPT_OPER(Database::restore(m_backupTest.m_backupDir.c_str(), m_backupTest.m_dbConfig.m_basedir));
		verifyDb();
	}
	initBackupTest();
	// û����checkpoint�����ݿ�
	{
		EXCPT_OPER(m_db = Database::open(&m_backupTest.m_dbConfig, true));
		m_db->setCheckpointEnabled(false);
		BlogTable::create(m_db);
		backupDb(m_db, m_backupTest.m_backupDir.c_str());
		m_db->close();
		delete m_db;
		m_db = 0;

		EXCPT_OPER(Database::restore(m_backupTest.m_backupDir.c_str(), m_backupTest.m_dbConfig.m_basedir));
		verifyDb();
	}
	initBackupTest();
	// checkpointҳ���룬��checkpoint֮��û����־�����ݿ�
	// ����־�ɱ���
	{
		EXCPT_OPER(m_db = Database::open(&m_backupTest.m_dbConfig, true));
		BlogTable::create(m_db);

		Connection *conn = m_db->getConnection(false);
		Session *session = m_db->getSessionManager()->allocSession(__FUNC__, conn);
		
		m_db->checkpoint(session);
		
		m_db->getSessionManager()->freeSession(session);
		m_db->freeConnection(conn);
		
		assert(m_db->getTxnlog()->tailLsn() % LogConfig::LOG_PAGE_SIZE ==0);
		m_db->getTxnlog()->setCheckpointLsn(m_db->getTxnlog()->tailLsn());
		backupDb(m_db, m_backupTest.m_backupDir.c_str());
		m_db->close();
		delete m_db;
		m_db = 0;

		EXCPT_OPER(Database::restore(m_backupTest.m_backupDir.c_str(), m_backupTest.m_dbConfig.m_basedir));
		verifyDb();
	}
	initBackupTest();
	// checkpoint֮��û����־�����ݿ�
	// ֻ����checkpoint lsn������־ҳ
	{
		EXCPT_OPER(m_db = Database::open(&m_backupTest.m_dbConfig, true));
		BlogTable::create(m_db);

		Connection *conn = m_db->getConnection(false);
		Session *session = m_db->getSessionManager()->allocSession(__FUNC__, conn);

		m_db->checkpoint(session);

		m_db->getSessionManager()->freeSession(session);
		m_db->freeConnection(conn);

		backupDb(m_db, m_backupTest.m_backupDir.c_str());
		m_db->close();
		delete m_db;
		m_db = 0;

		EXCPT_OPER(Database::restore(m_backupTest.m_backupDir.c_str(), m_backupTest.m_dbConfig.m_basedir));
		verifyDb();
	}
}

/**
 * �������ݲ���(�޲�������)
 *	��������: ���ݿ������������ݱ�blog��mysql/blog,���߿���mms
 *	�������̣�
	 һ����������

	 1. װ�أ����ݿ�db�ı��в����ʼ����;
	 2. ���㣺checkpoint cp;
	 3. �ο����ݣ��������ݿ⵽refBackupDir;
	 4. ���У����и������ݿ����;
	 5. ���ݣ����е�ĳһ��ʱ�̣��������ݿ⵽backupDir������¼���ݵ�lsn;
	 6. ֹͣ���У�ֹͣ���ݿ����������дLSN��lsn֮ǰ����־�����̣�
	 7. ��־���ݣ�������־�ļ���refBackupDir;
	 ������֤���

	 1. �ӱ���backupDir�ָ����ݿ�db��
	 2. �ӱ���refBackupDir�ָ����ݿ�refdb���ָ���lsnʱ�̣�
	 3. ��֤db��refdb�Ƿ���ͬ��
 */
void BackupTestCase::testBackupBasic() {
	// 1. 2. 3
	EXCPT_OPER(m_backupTest.prepare());
	// 4. ����
	Database *db;
	BlogTable *blogTable, *anotherBlogTable;
	EXCPT_OPER(db = db->open(&m_backupTest.m_dbConfig, false));
	db->setCheckpointEnabled(false);
	EXCPT_OPER(blogTable = BlogTable::open(db));
	EXCPT_OPER(anotherBlogTable = BlogTable::open(db, m_backupTest.m_mysqldb));
	blogTable->populate(m_backupTest.m_blogId, 200);
	anotherBlogTable->populate(m_backupTest.m_blogId, 200);
	m_backupTest.m_blogId += 200;
	// 5. ����
	EXCPT_OPER(m_backupTest.m_backupTailLsn = backupDb(db, m_backupTest.m_backupDir.c_str()));
	blogTable->populate(m_backupTest.m_blogId, 200);
	anotherBlogTable->populate(m_backupTest.m_blogId, 200);
	m_backupTest.m_blogId += 200;
	// 6.ֹͣ����
	Connection *conn = db->getConnection(false);
	Session *session = db->getSessionManager()->allocSession(__FUNC__, conn);
	
	db->getPageBuffer()->flushAll(session);
	
	db->getSessionManager()->freeSession(session);
	session = NULL;
	db->freeConnection(conn);
	conn = NULL;

	db->getTxnlog()->truncate(m_backupTest.m_backupTailLsn);
	 
	// 7. ������־
	EXCPT_OPER(backupTxnlog(db, m_backupTest.m_refBackupDir.c_str(), db->getControlFile()->getCheckpointLSN()));
	blogTable->close();
	anotherBlogTable->close();
	db->close();
	delete db;
	delete blogTable;
	delete anotherBlogTable;

	// ----- ��֤ ----
	EXCPT_OPER(m_backupTest.verify());
}

/**
* ���ݲ���(�в�������)
*	��������: ���ݿ������������ݱ�blog��mysql/blog,���߿���mms
*	�������̣�
	һ����������
	1. װ�أ����ݿ�db�ı��в����ʼ����;
	2. ���㣺checkpoint cp;
	3. �ο����ݣ��������ݿ⵽refBackupDir;
	4. ���У��������и������ݿ����;
	5. ���ݣ����е�ĳһ��ʱ�̣��������ݿ⵽backupDir������¼���ݵ�lsn;
	6. ֹͣ���У�ֹͣ���ݿ����������дLSN��lsn֮ǰ����־�����̣�
	7. ��־���ݣ�������־�ļ���refBackupDir;
	������֤���

	1. �ӱ���backupDir�ָ����ݿ�db��
	2. �ӱ���refBackupDir�ָ����ݿ�refdb���ָ���lsnʱ�̣�
	3. ��֤db��refdb�Ƿ���ͬ��
*/
void BackupTestCase::testBackupMT() {
	EXCPT_OPER(m_backupTest.prepare());

	Database *db;
	EXCPT_OPER(db = Database::open(&m_backupTest.m_dbConfig, false));
	LsnType onlineLsn = db->getControlFile()->getCheckpointLSN();
	int token = db->getTxnlog()->setOnlineLsn(onlineLsn);
	// 4. ���������в����߳�
	const uint threadCnt = 2;
	BlogInsertThread* insertThreads[threadCnt];
	BlogUpdateThread* updateThreads[threadCnt];
	BlogDeleteThread* deleteThreads[threadCnt];
	uint ops = 500;
	for (uint i = 0; i < sizeof(insertThreads) / sizeof(insertThreads[0]); ++i) {
		insertThreads[i] = new BlogInsertThread(db, &m_backupTest, ops);
		insertThreads[i]->start();
	}
	for (uint i = 0; i < sizeof(updateThreads) / sizeof(updateThreads[0]); ++i) {
		updateThreads[i] = new BlogUpdateThread(db, &m_backupTest, ops);
		updateThreads[i]->start();
	}
	for (uint i = 0; i < sizeof(deleteThreads) / sizeof(deleteThreads[0]); ++i) {
		deleteThreads[i] = new BlogDeleteThread(db, &m_backupTest, ops);
		deleteThreads[i]->start();
	}

	Thread::msleep(5000);
	// 5. ����
	BackupThread backupThread(db, &m_backupTest);
	CkptThread ckptThread(&backupThread, &m_backupTest, db, ops / 2);
	backupThread.start();
	ckptThread.start();
	backupThread.join();
	// 6. ֹͣ����
	for (uint i = 0; i < sizeof(insertThreads) / sizeof(insertThreads[0]); ++i) {
		insertThreads[i]->join();
		delete insertThreads[i];
	}
	for (uint i = 0; i < sizeof(updateThreads) / sizeof(updateThreads[0]); ++i) {
		updateThreads[i]->join();
		delete updateThreads[i];
	}
	for (uint i = 0; i < sizeof(deleteThreads) / sizeof(deleteThreads[0]); ++i) {
		deleteThreads[i]->join();
		delete deleteThreads[i];
	}
	ckptThread.join();
	db->getPageBuffer()->flushAll();
	db->getTxnlog()->truncate(m_backupTest.m_backupTailLsn);
	// 7. ������־
	EXCPT_OPER(backupTxnlog(db, m_backupTest.m_refBackupDir.c_str(), onlineLsn));
	db->close();
	delete db;
	// ----- ��֤ ----
	EXCPT_OPER(m_backupTest.verify());
}


/**
 * ����BUG��58235
 *	���ݵ��������ȵõ����ļ���С��Ȼ�󴴽������ļ�����������
 *	����õ����ļ����ȣ���������֮ǰ���ѱ���չ��
 *  ��ô�����ļ���ʵ�ʳ������ͷҳ�м�¼�Ķѳ��Ȳ�һ�£��Ӷ����»ָ�����
 *	��������Ŀ�ľ��ǹ�����ôһ����������Ŀ���Ǹ���BUG
 */
void BackupTestCase::testBackupHeapExtended() {

	// ��ʼ�����ݿ�ͱ�
	Database *db = Database::open(&m_backupTest.m_dbConfig, true);
	BlogTable::create(db);
	BlogTable* blogTable = BlogTable::open(db);
	blogTable->populate(m_backupTest.m_blogId, 100);
	m_backupTest.m_blogId += 100;
	db->close();
	delete db;
	delete blogTable;
	db = Database::open(&m_backupTest.m_dbConfig, false);
	// ���ʵ���ʱ������չ��
	BackupThread backupThread(db, &m_backupTest);
	backupThread.enableSyncPoint(SP_DB_BACKUP_BEFORE_HEAP);
	backupThread.start();
	backupThread.joinSyncPoint(SP_DB_BACKUP_BEFORE_HEAP);
	backupThread.enableSyncPoint(SP_DB_BACKUPFILE_AFTER_GETSIZE);
	backupThread.notifySyncPoint(SP_DB_BACKUP_BEFORE_HEAP);
	backupThread.disableSyncPoint(SP_DB_BACKUP_BEFORE_HEAP);
	backupThread.joinSyncPoint(SP_DB_BACKUPFILE_AFTER_GETSIZE);
	blogTable = BlogTable::open(db);
	blogTable->populate(m_backupTest.m_blogId, 500);
	m_backupTest.m_blogId += 500;
	backupThread.notifySyncPoint(SP_DB_BACKUPFILE_AFTER_GETSIZE);
	backupThread.disableSyncPoint(SP_DB_BACKUPFILE_AFTER_GETSIZE);
	backupThread.join();
	db->close();
	delete db;
	delete blogTable;
	// �ָ����ݿ�
	Database::restore(m_backupTest.m_backupDir.c_str(), m_backupTest.m_dbdir.c_str());
	db = Database::open(&m_backupTest.m_dbConfig, false, 1); // �ָ����ݿ�
	db->close();
	delete db;
}


/**
 * �ٴ�����verify
 * ���ڳ������
 * �Ѵ�������(����testdb_backup, testdb_refbackup����Ŀ¼���ŵ�errorDataĿ¼��
 */
void BackupTestCase::testReVerifyOnError() {
	// û�д���ʱ������������
	if (!File::isExist("errorData"))
		return;
	
	m_backupTest.m_backupDir = string("errorData") + NTSE_PATH_SEP + m_backupTest.m_backupDir;
	m_backupTest.m_refBackupDir = string("errorData") + NTSE_PATH_SEP + m_backupTest.m_refBackupDir;

	EXCPT_OPER(m_backupTest.verify());
}
//////////////////////////////////////////////////////////////////////////
/** ����׼������ */
void BackupTest::prepare() throw(NtseException) {
	Database::create(&m_dbConfig);
	Database *db;
	db = Database::open(&m_dbConfig, false);
	BlogTable::create(db);
	BlogTable::create(db, true, m_mysqldb);
	// ----- �������� ----
	// 1. װ������
	BlogTable* blogTable, *anotherBlogTable;
	blogTable = BlogTable::open(db);
	anotherBlogTable = BlogTable::open(db, m_mysqldb);
	blogTable->populate(m_blogId, 100);
	anotherBlogTable->populate(m_blogId, 100);
	m_blogId += 100;
	// 2. ����
	Connection *conn = db->getConnection(false);
	Session *session = db->getSessionManager()->allocSession(__FUNC__, conn);

	db->checkpoint(session);

	db->getSessionManager()->freeSession(session);
	db->freeConnection(conn);
	// 3. �ο�����
	backupDb(db, m_refBackupDir.c_str());
	blogTable->close();
	anotherBlogTable->close();
	// �Ѿ�����checkpoint�ˣ���˲���ˢ���ݺ���־
	// ���ܻ�д��־!!
	// ��Ϊ���ݻ������־��tailLsn���������close(true, true)
	// ����backupTxnlogʧЧ
	db->close(false, false);
	delete db;
	delete blogTable;
	delete anotherBlogTable;
}
/** ��֤�����ȷ�� */
void BackupTest::verify() throw(NtseException) {
	// 1. �ָ����ݿ�
	Database::restore(m_backupDir.c_str(), m_dbdir.c_str());
	Database *db = Database::open(&m_dbConfig, false, 1); // �ָ����ݿ�
	// 2. �ָ��ο����ݿ�
	Database::restore(m_refBackupDir.c_str(), m_refdbdir.c_str());
	Config dbConfig1;
	delete [] dbConfig1.m_basedir;
	dbConfig1.m_basedir = System::strdup(m_refdbdir.c_str());
	dbConfig1.m_logFileSize = m_logFileSize;
	Database *db1 = Database::open(&dbConfig1, false, 1); // �ָ����ݿ�
	// 3. �Ƚ����ݿ�
	db->close();
	delete db;
	db1->close();
	delete db1;
	db = Database::open(&m_dbConfig, false);
	if (m_backupTailLsn != INVALID_LSN)
		CPPUNIT_ASSERT(db->getTxnlog()->tailLsn() == m_backupTailLsn);
	db1 = Database::open(&dbConfig1, false);
	if (m_backupTailLsn != INVALID_LSN)
		CPPUNIT_ASSERT(db1->getTxnlog()->tailLsn() == m_backupTailLsn);
	CPPUNIT_ASSERT(compareDb(db, db1));
	db->close();
	delete db;
	db1->close();
	delete db1;
}
void BackupTest::setUp() {
	tearDown();
	{File(m_refdbdir.c_str()).mkdir();}
	{File(m_dbdir.c_str()).mkdir();}
	{File((m_dbdir + NTSE_PATH_SEP + m_mysqldb).c_str()).mkdir();}
	{File(m_refBackupDir.c_str()).mkdir();}
	{File(m_backupDir.c_str()).mkdir();}
}
void BackupTest::tearDown() {
	{File(m_refdbdir.c_str()).rmdir(true);}
	{File(m_refBackupDir.c_str()).rmdir(true);}
	{File(m_dbdir.c_str()).rmdir(true);}
	{File(m_backupDir.c_str()).rmdir(true);}
}

