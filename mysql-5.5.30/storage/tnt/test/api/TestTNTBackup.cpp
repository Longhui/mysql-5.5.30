#include "TestTNTBackup.h"
#include "Test.h"
#include "api/TNTTable.h"
#include "api/Table.h"
#include "misc/RecordHelper.h"

#define TNT_STU_SCHEMA_NAME "Olympic"
#define	TNT_STU_TBL_NAME "student"
#define TNT_STU_SNO "sno"
#define TNT_STU_NAME "name"
#define TNT_STU_AGE "age"
#define TNT_STU_CLASS "class"
#define TNT_STU_GPA "gpa"
#define TNT_STU_COMMENT "comment"

#define TNT_DB_PATH  "testdb"
#define TNT_LOG_PATH "testlog"

struct StuRec {
	u32			m_stuNo;
	char        *m_name;
	u16			m_age;
	u32			m_clsNo;
	float		m_gpa;
	char        *m_comment;
	RowId		m_rid;
	Record		*m_redRec;
	bool        m_del;
};

class TNTBackupUpdateThread: public Thread {
public:
	TNTBackupUpdateThread(TNTDatabase *db, TNTTable *table, StuRec *recs, u32 count, u8 factor, bool save, bool log): Thread("TNTBackupUpdateThread") {
		m_db = db;
		m_table = table;
		m_recs = recs;
		m_count = count;
		m_factor = factor;
		m_save = save;
		m_log = log;
	}

	void run() {
		Connection *conn = m_db->getNtseDb()->getConnection(false);
		Session *session = m_db->getNtseDb()->getSessionManager()->allocSession("UpdateThread::run", conn);
		MemoryContext *ctx = session->getMemoryContext();
		McSavepoint msp(ctx);

		m_table->lockMeta(session, IL_S, -1, __FILE__, __LINE__);
		TNTTrxSys *trxSys = m_db->getTransSys();
		TNTTransaction *trx = TNTBackupTestCase::startTrx(trxSys, conn, TL_X, m_log);
		session->setTrans(trx);
		trx->lockTable(TL_IX, m_table->getNtseTable()->getTableDef()->m_id);

		TNTBackupTestCase::update(session, m_table, m_recs, 0, m_count*4/5, m_factor, m_save);

		//commit会释放表锁
		TNTBackupTestCase::commitTrx(trxSys, trx);
		m_table->unlockMeta(session, IL_S);

		m_db->getNtseDb()->getSessionManager()->freeSession(session);
		m_db->getNtseDb()->freeConnection(conn);
	}
private:
	TNTDatabase       *m_db;
	TNTTable          *m_table;
	StuRec            *m_recs;
	u32               m_count;
	u8                m_factor;
	bool              m_save;
	bool              m_log;
};

const char *TNTBackupTestCase::getName(void) {
	return "TNTDatabase Backup test";
}

const char *TNTBackupTestCase::getDescription(void) {
	return "Test TNTDatabase Backup";
}

bool TNTBackupTestCase::isBig() {
	return false;
}

void TNTBackupTestCase::setUp() {
	init(TNT_DB_PATH, TNT_LOG_PATH, true);
}

void TNTBackupTestCase::init(const char *base, const char *log, bool create) {
	string path(base);
	string logPath(log);

	if (create) {
		File dir(path.c_str());
		dir.rmdir(true);
		dir.mkdir();

		File logDir(logPath.c_str());
		logDir.rmdir(true);
		logDir.mkdir();
	}

	m_config.setNtseBasedir(path.c_str());
	m_config.setTntBasedir(path.c_str());
	m_config.setTntDumpdir(path.c_str());
	m_config.setTxnLogdir(logPath.c_str());
	m_config.m_tntLogLevel = EL_WARN;
	m_config.m_tntBufSize = 200;
	//m_config.m_tntLogfileSize = 1024 * 1024 * 128;
	m_config.m_ntseConfig.m_logFileSize = 1024 * 1024 * 128;
	m_config.m_verpoolCnt = 3;
	m_config.m_purgeBeforeClose = false;
	m_config.m_dumpBeforeClose = false;

	EXCPT_OPER(m_db = TNTDatabase::open(&m_config, true, 0));

	Database *db = m_db->getNtseDb();
	m_conn = db->getConnection(false);
	m_conn->setTrx(true);
	m_session = db->getSessionManager()->allocSession("TNTRecoverTestCase", m_conn);

	TNTTrxSys *trxSys = m_db->getTransSys();
	//该记录在tnt hash索引中不存在，认为在ntse中存在
	TNTTransaction *trx = startTrx(trxSys, m_conn);
	m_session->setTrans(trx);

	assert(m_db != NULL && m_session != NULL && m_conn != NULL);

	string tablePath("./");
	tablePath.append(TNT_STU_SCHEMA_NAME).append(NTSE_PATH_SEP).append(TNT_STU_TBL_NAME);
	if (create) {
		TableDef *tableDef = createStudentTableDef();
		path.append(NTSE_PATH_SEP).append(TNT_STU_SCHEMA_NAME);
		File dbFile(path.c_str());
		dbFile.mkdir();

		m_db->createTable(m_session, tablePath.c_str(), tableDef);
		delete tableDef;
	}
	m_table = m_db->openTable(m_session, tablePath.c_str());
	CPPUNIT_ASSERT(m_table != NULL);
	commitTrx(trxSys, trx);
}

void TNTBackupTestCase::tearDown() {
	shutDown(TNT_DB_PATH, TNT_LOG_PATH);
}

void TNTBackupTestCase::shutDown(const char *base, const char *log) {
	if (m_db != NULL) {
		if (m_table != NULL) {
			m_db->closeTable(m_session, m_table);
		}
		Database *db = m_db->getNtseDb();
		db->getSessionManager()->freeSession(m_session);
		db->freeConnection(m_conn);
		m_db->close(false, false);

		delete m_db;
		m_db = NULL;
	}

	File dir(base);
	dir.rmdir(true);

	File logDir(log);
	logDir.rmdir(true);
}

TableDef *TNTBackupTestCase::createStudentTableDef() {
	TableDefBuilder tb(TableDef::INVALID_TABLEID, TNT_STU_SCHEMA_NAME, TNT_STU_TBL_NAME);
	tb.addColumn(TNT_STU_SNO, CT_INT);
	tb.addColumnS(TNT_STU_NAME, CT_VARCHAR, 30, false, false, COLL_LATIN1);
	tb.addColumn(TNT_STU_AGE, CT_SMALLINT);
	tb.addColumn(TNT_STU_CLASS, CT_MEDIUMINT);
	tb.addColumn(TNT_STU_GPA, CT_FLOAT);
	tb.addColumnS(TNT_STU_COMMENT, CT_VARCHAR, 200, false, true, COLL_LATIN1);
	TableDef *tblDef = tb.getTableDef();
	tblDef->setTableStatus(TS_TRX);
	return tblDef;
}

SubRecord *TNTBackupTestCase::createSubRecord(TableDef *tableDef, const std::vector<u16> &columns, const std::vector<void *>&datas) {
	SubRecord *subRecord = NULL;
	SubRecordBuilder sb(tableDef, REC_REDUNDANT);
	subRecord = sb.createSubRecord(columns, datas);
	return subRecord;
}

Record* TNTBackupTestCase::createStudentRec(u32 stuNo, const char *name, u16 age, u32 clsNo, float gpa) {
	RecordBuilder rb(m_table->getNtseTable()->getTableDef(), INVALID_ROW_ID, REC_REDUNDANT);
	rb.appendInt(stuNo);
	rb.appendVarchar(name);
	rb.appendSmallInt(age);
	rb.appendMediumInt(clsNo);
	rb.appendFloat(gpa);
	rb.appendNull();

	return rb.getRecord();
}

TNTTransaction *TNTBackupTestCase::startTrx(TNTTrxSys *trxSys, Connection *conn, TLockMode lockMode/* = TL_NO*/,  bool log/* = false*/) {
	TNTTransaction *trx = trxSys->allocTrx();
	if (!log) {
		trx->disableLogging();
	}
	trx->startTrxIfNotStarted(conn);
	return trx;
}

void TNTBackupTestCase::commitTrx(TNTTrxSys *trxSys, TNTTransaction *trx) {
	trx->commitTrx(CS_NORMAL);
	trxSys->freeTrx(trx);
}

void TNTBackupTestCase::rollBackTrx(TNTTrxSys *trxSys, TNTTransaction *trx) {
	trx->rollbackTrx(RBS_NORMAL);
	trxSys->freeTrx(trx);
}

void TNTBackupTestCase::insert(StuRec *recs, const u32 count, bool log) {
	TNTTrxSys *trxSys = m_db->getTransSys();
	McSavepoint msp(m_session->getMemoryContext());
	TNTTransaction *trx = startTrx(trxSys, m_conn, TL_X, log);
	m_session->setTrans(trx);
	insert(m_session, recs, count);
	commitTrx(trxSys, trx);
}

void TNTBackupTestCase::insert(Session *session, StuRec *recs, const u32 count) {
	assert(session->getTrans() != NULL);
	TNTOpInfo opInfo;
	initTNTOpInfo(opInfo, TL_X);
	uint dupIndex = 0;
	u32 i = 0;
	McSavepoint msp(session->getMemoryContext());
	for (i = 0; i < count; i++) {
		McSavepoint msp1(session->getMemoryContext());
		recs[i].m_stuNo = 5001 + i;
		recs[i].m_name = new char[30];
		sprintf(recs[i].m_name, "netease%d", i);
		recs[i].m_age = 25 + (i % 10);
		recs[i].m_clsNo = 3001 + (i % 100) ;
		recs[i].m_gpa = (float)(3.5 + (i % 15)*0.1);
		recs[i].m_comment = new char[200];
		memset(recs[i].m_comment, 0, 200*sizeof(char));
		recs[i].m_redRec = createStudentRec(recs[i].m_stuNo, recs[i].m_name, recs[i].m_age, recs[i].m_clsNo, recs[i].m_gpa);
		recs[i].m_rid = m_table->insert(session, recs[i].m_redRec->m_data, &dupIndex, &opInfo);
		CPPUNIT_ASSERT(dupIndex == 0);
		CPPUNIT_ASSERT(recs[i].m_rid != INVALID_ROW_ID);
		recs[i].m_del = false;
	}
}

void TNTBackupTestCase::updateAll(StuRec *recs, const u32 count, u8 factor, bool save, bool log) {
	update(recs, 0, count - 1, factor, save, log);
}

void TNTBackupTestCase::update(StuRec *recs, const u32 begin, const u32 end, u8 factor, bool save, bool log) {
	MemoryContext *ctx = m_session->getMemoryContext();
	McSavepoint msp(ctx);

	TNTTrxSys *trxSys = m_db->getTransSys();
	TNTTransaction *trx = startTrx(trxSys, m_conn, TL_X, log);
	m_session->setTrans(trx);
	trx->lockTable(TL_IX, m_table->getNtseTable()->getTableDef()->m_id);
	update(m_session, recs, begin, end, factor, save);
	commitTrx(trxSys, trx);
}

void TNTBackupTestCase::update(Session *session, StuRec *recs, const u32 begin, const u32 end, u8 factor, bool save) {
	update(session, m_table, recs, begin, end, factor, save);
}

void TNTBackupTestCase::update(Session *session, TNTTable *table, StuRec *recs, const u32 begin, const u32 end, u8 factor, bool save) {
	assert(session->getTrans() != NULL);
	TNTTblScan *scan = NULL;
	TNTOpInfo opInfo;
	initTNTOpInfo(opInfo, TL_X);

	u32 i = 0;
	TableDef *tableDef = table->getNtseTable()->getTableDef();
	MemoryContext *ctx = session->getMemoryContext();
	McSavepoint msp(ctx);
	byte *mysqlRow = (byte *)ctx->alloc(tableDef->m_maxRecSize);
	u16 *readCols = (u16 *)ctx->alloc(sizeof(u16) * tableDef->m_numCols);
	for (i = 0; i < tableDef->m_numCols; ++i) {
		readCols[i] = i;
	}

	StuRec *rec = NULL;
	if (!save) {
		rec = (StuRec *)ctx->alloc(sizeof(StuRec));
		rec->m_name = (char *)ctx->alloc(30*sizeof(char));
		rec->m_comment = (char *)ctx->alloc(200*sizeof(char));
	}

	char chr = 'a' + i % ('z' - 'a');

	SubRecord *updateSub = NULL;
	std::vector<u16> columns;
	std::vector<void *> datas;
	columns.push_back(2);
	columns.push_back(3);
	columns.push_back(5);

	for (i = begin; i <= end; i++) {
		if (save) {
			rec = recs + i;
		}
		McSavepoint msp1(ctx);
		datas.clear();
		rec->m_age = recs[i].m_age + factor % 10;
		rec->m_clsNo = recs[i].m_clsNo + (factor*10) % 8;
		memset(rec->m_comment, 0, 200);
		memset(rec->m_comment, chr , (factor * 10) % 200 + 1);

		datas.push_back(&rec->m_age);
		datas.push_back(&rec->m_clsNo);
		datas.push_back(rec->m_comment);
		updateSub = createSubRecord(tableDef, columns, datas);
		session->getTrans()->lockRow(TL_X, recs[i].m_rid, tableDef->m_id);
		scan = table->positionScan(session, OP_UPDATE, &opInfo, tableDef->m_numCols, readCols);
		scan->setUpdateSubRecord(updateSub->m_numCols, updateSub->m_columns);
		CPPUNIT_ASSERT(table->getNext(scan, mysqlRow, recs[i].m_rid));
		//起到了getNext作用
		//scan->setCurrentRid(recs[i].m_rid);
		//deepCopyRecord(scan->getRecord(), recs[i].m_redRec);
		CPPUNIT_ASSERT(table->updateCurrent(scan, updateSub->m_data));
		table->endScan(scan);

		delete[] updateSub->m_columns;
		delete[] updateSub->m_data;
		delete updateSub;
	}
}

void TNTBackupTestCase::remove(StuRec *recs, const u32 begin, const u32 end, bool save, bool log) {
	TNTTrxSys *trxSys = m_db->getTransSys();
	TNTTransaction *trx = startTrx(trxSys, m_conn, TL_X, log);
	m_session->setTrans(trx);
	trx->lockTable(TL_IX, m_table->getNtseTable()->getTableDef()->m_id);
	remove(m_session, recs, begin, end, save);
	commitTrx(trxSys, trx);
}

void TNTBackupTestCase::remove(Session *session, StuRec *recs, const u32 begin, const u32 end, bool save) {
	remove(session, m_table, recs, begin, end, save);
}

void TNTBackupTestCase::remove(Session *session, TNTTable *table, StuRec *recs, const u32 begin, const u32 end, bool save) {
	assert(session->getTrans() != NULL);
	TNTTblScan *scan = NULL;
	TNTOpInfo opInfo;
	initTNTOpInfo(opInfo, TL_X);

	TableDef *tableDef = table->getNtseTable()->getTableDef();
	MemoryContext *ctx = session->getMemoryContext();
	McSavepoint msp(ctx);
	byte *mysqlRow = (byte *)ctx->alloc(tableDef->m_maxRecSize);
	u16 *readCols = (u16 *)ctx->alloc(sizeof(u16) * tableDef->m_numCols);
	for (u16 i = 0; i < tableDef->m_numCols; ++i) {
		readCols[i] = i;
	}

	for (u32 i = begin; i <= end; i++) {
		McSavepoint msp1(ctx);
		session->getTrans()->lockRow(TL_X, recs[i].m_rid, tableDef->m_id);
		scan = table->positionScan(session, OP_DELETE, &opInfo, tableDef->m_numCols, readCols);
		CPPUNIT_ASSERT(table->getNext(scan, mysqlRow, recs[i].m_rid));
		//scan->setCurrentRid(recs[i].m_rid);
		//deepCopyRecord(scan->getRecord(), recs[i].m_redRec);
		table->deleteCurrent(scan);
		table->endScan(scan);
		if (save) {
			recs[i].m_del = true;
		}
	}
}

void TNTBackupTestCase::releaseAllRecordVal(StuRec *recs, u32 count) {
	for (u32 i = 0; i < count; i++) {
		delete[] recs[i].m_comment;
		delete[] recs[i].m_name;
		delete[] recs[i].m_redRec->m_data;
		delete recs[i].m_redRec;
	}
	delete[] recs;
}

void TNTBackupTestCase::testBackupAndRestoreBeforeDump() {
	testBackupAndRestore(false);
}

void TNTBackupTestCase::testBackupAndRestoreAfterDump() {
	testBackupAndRestore(true);
}

void TNTBackupTestCase::testBackupAndRestore(bool afterDump) {
	string frmpath = string(m_config.m_ntseConfig.m_basedir) + NTSE_PATH_SEP + m_table->getNtseTable()->getPath() + ".frm";
	File frmFile(frmpath.c_str());
	frmFile.create(false, false);
	frmFile.close();

	u32 count = 10000;
	StuRec *recs = new StuRec[count];
	insert(recs, count, true);
	updateAll(recs, count,  0, true, true);
	updateAll(recs, count,  3, true, true);
	if (afterDump) {
		m_db->purgeAndDumpTntim(PT_PURGEPHASE2, false, true, true);
	}
	updateAll(recs, count, 8, true, true);
	
	string backupPath("backup");
	File backupDir(backupPath.c_str());
	backupDir.rmdir(true);
	backupDir.mkdir();

	TNTBackupProcess *bp = NULL;
	try {
		bp = m_db->initBackup(backupPath.c_str());
		m_db->doBackup(bp);
		m_db->finishingBackupAndLock(bp);
	} catch (NtseException &e) {
		CPPUNIT_FAIL(e.getMessage());
	}
	m_db->doneBackup(bp);
	shutDown(TNT_DB_PATH, TNT_LOG_PATH);

	File dbDir(TNT_DB_PATH);
	dbDir.mkdir();

	File logDir(TNT_LOG_PATH);
	logDir.mkdir();

	try {
		TNTDatabase::restore(backupPath.c_str(), TNT_DB_PATH, TNT_LOG_PATH);
	} catch (NtseException &e) {
		CPPUNIT_FAIL(e.getMessage());
	}
	backupDir.rmdir(true);
	init(TNT_DB_PATH, TNT_LOG_PATH, false);
	
	compareAllRecord(recs, count);

	releaseAllRecordVal(recs, count);
}

void TNTBackupTestCase::testBackupAndRestoreMultiThr() {
	string frmpath = string(m_config.m_ntseConfig.m_basedir) + NTSE_PATH_SEP + m_table->getNtseTable()->getPath() + ".frm";
	File frmFile(frmpath.c_str());
	frmFile.create(false, false);
	frmFile.close();

	u32 count = 10000;
	StuRec *recs = new StuRec[count];
	insert(recs, count, true);
	updateAll(recs, count,  0, true, true);
	updateAll(recs, count,  3, true, true);

	string backupPath("backup");
	File backupDir(backupPath.c_str());
	backupDir.rmdir(true);
	backupDir.mkdir();

	TNTBackupProcess *bp = NULL;
	try {
		bp = m_db->initBackup(backupPath.c_str());
		// 启动一个事务，防止tnt buffer数据被之后的purge摘除
		TNTTransaction *trx = m_db->getTransSys()->allocTrx();
		trx->disableLogging();
		trx->startTrxIfNotStarted(m_conn);

		TNTBackupUpdateThread updateThr1(m_db, m_table, recs, count, 5, true, true);
		updateThr1.start();
		m_db->doBackup(bp);
		m_db->purgeAndDumpTntim(PT_PURGEPHASE2, false, true, true);
		TNTBackupUpdateThread updateThr2(m_db, m_table, recs, count, 9, true, true);
		updateThr2.enableSyncPoint(SP_MRECS_UPDATE_PTR_MODIFY);
		updateThr2.start();
		updateThr2.joinSyncPoint(SP_MRECS_UPDATE_PTR_MODIFY);
		updateThr2.disableSyncPoint(SP_MRECS_UPDATE_PTR_MODIFY);
		updateThr2.notifySyncPoint(SP_MRECS_UPDATE_PTR_MODIFY);
		m_db->finishingBackupAndLock(bp);
		commitTrx(m_db->getTransSys(), trx);
		updateThr1.join(-1);
		updateThr2.join(-1);
		//CPPUNIT_ASSERT(!updateThr1.isAlive());
		//CPPUNIT_ASSERT(!updateThr2.isAlive());
	} catch (NtseException &e) {
		CPPUNIT_FAIL(e.getMessage());
	}
	m_db->doneBackup(bp);
	shutDown(TNT_DB_PATH, TNT_LOG_PATH);

	File dbDir(TNT_DB_PATH);
	dbDir.mkdir();

	File logDir(TNT_LOG_PATH);
	logDir.mkdir();

	try {
		TNTDatabase::restore(backupPath.c_str(), TNT_DB_PATH, TNT_LOG_PATH);
	} catch (NtseException &e) {
		CPPUNIT_FAIL(e.getMessage());
	}
	backupDir.rmdir(true);
	init(TNT_DB_PATH, TNT_LOG_PATH, false);

	compareAllRecord(recs, count);

	releaseAllRecordVal(recs, count);

}

void TNTBackupTestCase::compareAllRecord(StuRec *recs, u32 count) {
	TNTTrxSys *trxSys = m_db->getTransSys();
	TNTTransaction *trx = startTrx(trxSys, m_conn, TL_S);
	m_session->setTrans(trx);
	for (u32 i = 0; i < count; i++) {
		compareRecord(m_session, m_table, recs + i);
	}
	commitTrx(trxSys, trx);
}

void TNTBackupTestCase::compareRecord(Session *session, TNTTable *table, StuRec *recVal) {
	TNTTblScan *scan = NULL;
	TNTOpInfo opInfo;
	initTNTOpInfo(opInfo, TL_S);

	TableDef *tableDef = table->getNtseTable()->getTableDef();
	MemoryContext *ctx = session->getMemoryContext();
	McSavepoint msp(ctx);
	u16 *readCols = (u16 *)ctx->alloc(sizeof(u16) * tableDef->m_numCols);
	for (u16 i = 0; i < tableDef->m_numCols; ++i) {
		readCols[i] = i;
	}

	byte *mysqlRow = (byte *)ctx->alloc(tableDef->m_maxRecSize);
	scan = table->positionScan(session, OP_READ, &opInfo, tableDef->m_numCols, readCols);
	//scan->setCurrentRid(recVal->m_rid);
	//deepCopyRecord(scan->getRecord(), recVal->m_redRec);
	if (recVal->m_del) {
		CPPUNIT_ASSERT(!table->getNext(scan, mysqlRow, recVal->m_rid));
	} else {
		CPPUNIT_ASSERT(table->getNext(scan, mysqlRow, recVal->m_rid));
		Record rec(recVal->m_rid, REC_MYSQL, mysqlRow, tableDef->m_maxRecSize);

		size_t size;
		u16 cno = 0;
		RedRecord redRec(tableDef, &rec);
		CPPUNIT_ASSERT(redRec.readInt(cno++) == recVal->m_stuNo);
		char *name = NULL;
		redRec.readVarchar(cno++, &name, &size);
		CPPUNIT_ASSERT(size == strlen(recVal->m_name));
		CPPUNIT_ASSERT(memcmp(name, recVal->m_name, size) == 0);
		u16 age = redRec.readSmallInt(cno);
		CPPUNIT_ASSERT(redRec.readSmallInt(cno++) == recVal->m_age);
		CPPUNIT_ASSERT(redRec.readMediumInt(cno++) == recVal->m_clsNo);
		CPPUNIT_ASSERT(redRec.readFloat(cno++) == recVal->m_gpa);
		char *comment = NULL;
		redRec.readVarchar(cno++, &comment, &size);
		CPPUNIT_ASSERT(size == strlen(recVal->m_comment));
		if (size > 0) {
			CPPUNIT_ASSERT(memcmp(comment, recVal->m_comment, size) == 0);
		} else {
			CPPUNIT_ASSERT(comment == NULL);
		}
	}

	table->endScan(scan);
}