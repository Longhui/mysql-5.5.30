#include "TestTNTRecover.h"
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

struct RecordVal {
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

class DumpThread: public Thread {
public:
	DumpThread(TNTDatabase *db): Thread("DumpThread") {
		m_db = db;
	}

	void run() {
		try {
			m_db->purgeAndDumpTntim(PT_NONE, false, true, false);
		} catch (NtseException &e) {
			printf("dump error: %s\n", e.getMessage());
		}
	}
private:
	TNTDatabase       *m_db;
};

class PurgeThread: public Thread {
public:
	PurgeThread(TNTDatabase *db, PurgeTarget purgeTarget = PT_PURGEPHASE2): Thread("PurgeThread") {
		m_db = db;
		m_purgeTarget = purgeTarget;
	}
	void run() {
		try {
			m_db->purgeAndDumpTntim(PT_PURGEPHASE2, false, true, true);
		} catch (NtseException &e) {
			printf("purge error: %s\n", e.getMessage());
		}
	}
private:
	TNTDatabase       *m_db;
	PurgeTarget       m_purgeTarget;
};

class DefragThread: public Thread {
public:
	DefragThread(TNTDatabase *db): Thread("DefragThread") {
		m_db = db;
	}
	void run() {
		try {
			m_db->purgeAndDumpTntim(PT_DEFRAG, false, false, false);
		} catch (NtseException &e) {
			printf("defrag error: %s\n", e.getMessage());
		}
	}
private:
	TNTDatabase       *m_db;
};

class UpdateThread: public Thread {
public:
	UpdateThread(TNTDatabase *db, TNTTable *table, RecordVal *recs, u32 count, u8 factor, bool save, bool log): Thread("UpdateThread") {
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
		conn->setTrx(true);
		Session *session = m_db->getNtseDb()->getSessionManager()->allocSession("UpdateThread::run", conn);
		MemoryContext *ctx = session->getMemoryContext();
		McSavepoint msp(ctx);

		TNTTrxSys *trxSys = m_db->getTransSys();
		TNTTransaction *trx = TNTRecoverTestCase::startTrx(trxSys, conn, TL_X, m_log);
		session->setTrans(trx);

		TNTRecoverTestCase::update(session, m_table, m_recs, 0, m_count*4/5, m_factor, m_save);

		TNTRecoverTestCase::commitTrx(trxSys, trx);

		m_db->getNtseDb()->getSessionManager()->freeSession(session);
		m_db->getNtseDb()->freeConnection(conn);
	}
private:
	TNTDatabase       *m_db;
	TNTTable          *m_table;
	RecordVal         *m_recs;
	u32               m_count;
	u8                m_factor;
	bool              m_save;
	bool              m_log;
};

class GetThread: public Thread {
public:
	GetThread(TNTDatabase *db, TNTTable *table, RecordVal *recs, u32 count): Thread("GetThread") {
		m_db = db;
		m_table = table;
		m_recs = recs;
		m_count = count;
	}

	void run() {
		Connection *conn = m_db->getNtseDb()->getConnection(false);
		conn->setTrx(true);
		Session *session = m_db->getNtseDb()->getSessionManager()->allocSession("GetThread::run", conn);
		MemoryContext *ctx = session->getMemoryContext();
		McSavepoint msp(ctx);

		TNTTrxSys *trxSys = m_db->getTransSys();
		TNTTransaction *trx = TNTRecoverTestCase::startTrx(trxSys, conn, TL_X, false);
		session->setTrans(trx);

		for(u32 i = 0; i < m_count; i++) {
			u64 sp1 = ctx->setSavepoint();
			TNTRecoverTestCase::compareRecord(session, m_table, m_recs + i);
			ctx->resetToSavepoint(sp1);
		}

		TNTRecoverTestCase::commitTrx(trxSys, trx);

		m_db->getNtseDb()->getSessionManager()->freeSession(session);
		m_db->getNtseDb()->freeConnection(conn);
	}
private:
	TNTDatabase       *m_db;
	TNTTable          *m_table;
	RecordVal         *m_recs;
	u32               m_count;
};

class RollBackUpdateThread: public Thread {
public:
	RollBackUpdateThread(TNTDatabase *db, TNTTable *table, RecordVal *recs, u32 count, u8 factor, bool save, bool log): Thread("RollBackUpdateThread") {
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
		conn->setTrx(true);
		Session *session = m_db->getNtseDb()->getSessionManager()->allocSession("UpdateThread::run", conn);
		MemoryContext *ctx = session->getMemoryContext();
		McSavepoint msp(ctx);

		TNTTrxSys *trxSys = m_db->getTransSys();
		TNTTransaction *trx = TNTRecoverTestCase::startTrx(trxSys, conn, TL_X, m_log);
		session->setTrans(trx);

		TNTRecoverTestCase::update(session, m_table, m_recs, 0, m_count, m_factor, m_save);

		TNTRecoverTestCase::rollBackTrx(trxSys, trx);

		m_db->getNtseDb()->getSessionManager()->freeSession(session);
		m_db->getNtseDb()->freeConnection(conn);
	}
private:
	TNTDatabase       *m_db;
	TNTTable          *m_table;
	RecordVal         *m_recs;
	u32               m_count;
	u8                m_factor;
	bool              m_save;
	bool              m_log;
};

const char* TNTRecoverTestCase::getName() {
	return "TNTDatabase Recover test";
}

const char* TNTRecoverTestCase::getDescription() {
	return "Test TNTDatabase recover, and also contain dump, purge, defrag";
}

bool TNTRecoverTestCase::isBig() {
	return false;
}

void TNTRecoverTestCase::setUp() {
	string path("testdb");
	File dir(path.c_str());
	dir.rmdir(true);
	dir.mkdir();
	m_config.setNtseBasedir("testdb");
	m_config.setTntBasedir("testdb");
	m_config.setTntDumpdir("testdb");
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

	TableDef *tableDef = createStudentTableDef();
	path.append(NTSE_PATH_SEP).append(TNT_STU_SCHEMA_NAME);
	File dbFile(path.c_str());
	dbFile.mkdir();

	string tablePath("./");
	tablePath.append(TNT_STU_SCHEMA_NAME).append(NTSE_PATH_SEP).append(TNT_STU_TBL_NAME);
	m_db->createTable(m_session, tablePath.c_str(), tableDef);
	m_table = m_db->openTable(m_session, tablePath.c_str());
	CPPUNIT_ASSERT(m_table != NULL);
	commitTrx(trxSys, trx);

	delete tableDef;
}

void TNTRecoverTestCase::tearDown() {
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

	File dir("testdb");
	dir.rmdir(true);
}

TableDef *TNTRecoverTestCase::createStudentTableDef() {
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

SubRecord *TNTRecoverTestCase::createSubRecord(TableDef *tableDef, const std::vector<u16> &columns, const std::vector<void *>&datas) {
	SubRecord *subRecord = NULL;
	SubRecordBuilder sb(tableDef, REC_REDUNDANT);
	subRecord = sb.createSubRecord(columns, datas);
	return subRecord;
}

void TNTRecoverTestCase::deepCopyRecord(Record *dest, Record *src) {
	dest->m_rowId = src->m_rowId;
	dest->m_format = src->m_format;
	dest->m_size = src->m_size;
	memcpy(dest->m_data, src->m_data, src->m_size);
}

void TNTRecoverTestCase::compareRecord(Session *session, TNTTable *table, RecordVal *recVal) {
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

void TNTRecoverTestCase::compareNtseRecord(Session *session, Table *table, RecordVal *recVal, bool exist) {
	TblScan *scan = NULL;

	TableDef *tableDef = table->getTableDef();
	MemoryContext *ctx = session->getMemoryContext();
	McSavepoint msp(ctx);
	u16 *readCols = (u16 *)ctx->alloc(sizeof(u16) * tableDef->m_numCols);
	for (u16 i = 0; i < tableDef->m_numCols; ++i) {
		readCols[i] = i;
	}

	byte *mysqlRow = (byte *)ctx->alloc(tableDef->m_maxRecSize);
	scan = table->positionScan(session, OP_READ, tableDef->m_numCols, readCols);
	CPPUNIT_ASSERT(table->getNext(scan, mysqlRow, recVal->m_rid) == exist);
	if (exist) {
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
		assert(age == recVal->m_age);
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

TNTTransaction *TNTRecoverTestCase::startTrx(TNTTrxSys *trxSys, Connection *conn, TLockMode lockMode/* = TL_NO*/,  bool log/* = false*/) {
	TNTTransaction *trx = trxSys->allocTrx();
	if (!log) {
		trx->disableLogging();
	}
	trx->startTrxIfNotStarted(conn);
	return trx;
}

void TNTRecoverTestCase::commitTrx(TNTTrxSys *trxSys, TNTTransaction *trx) {
	trx->commitTrx(CS_NORMAL);
	trxSys->freeTrx(trx);
}

void TNTRecoverTestCase::rollBackTrx(TNTTrxSys *trxSys, TNTTransaction *trx) {
	trx->rollbackTrx(RBS_NORMAL);
	trxSys->freeTrx(trx);
}

void TNTRecoverTestCase::reOpenDb(bool flushLog, bool flushData) {
	m_db->closeTable(m_session, m_table);
	Database *db = m_db->getNtseDb();
	db->getSessionManager()->freeSession(m_session);
	db->freeConnection(m_conn);
	m_db->close(flushLog, flushData);

	delete m_db;
	m_db = NULL;

	EXCPT_OPER(m_db = TNTDatabase::open(&m_config, false, 0));

	db = m_db->getNtseDb();
	m_conn = db->getConnection(false);
	m_session = db->getSessionManager()->allocSession("TNTRecoverTestCase", m_conn);

	string tablePath("./");
	tablePath.append(TNT_STU_SCHEMA_NAME).append(NTSE_PATH_SEP).append(TNT_STU_TBL_NAME);
	TNTTrxSys *trxSys = m_db->getTransSys();
	TNTTransaction *trx = startTrx(trxSys, m_conn);
	m_session->setTrans(trx);
	m_table = m_db->openTable(m_session, tablePath.c_str());
	CPPUNIT_ASSERT(m_table != NULL);
	commitTrx(trxSys, trx);
}

Record* TNTRecoverTestCase::createStudentRec(u32 stuNo, const char *name, u16 age, u32 clsNo, float gpa) {
	RecordBuilder rb(m_table->getNtseTable()->getTableDef(), INVALID_ROW_ID, REC_REDUNDANT);
	rb.appendInt(stuNo);
	rb.appendVarchar(name);
	rb.appendSmallInt(age);
	rb.appendMediumInt(clsNo);
	rb.appendFloat(gpa);
	rb.appendNull();

	return rb.getRecord();
}

void TNTRecoverTestCase::insert(RecordVal *recs, const u32 count, bool log) {
	TNTTrxSys *trxSys = m_db->getTransSys();
	McSavepoint msp(m_session->getMemoryContext());
	TNTTransaction *trx = startTrx(trxSys, m_conn, TL_X, log);
	m_session->setTrans(trx);
	insert(m_session, recs, count);
	commitTrx(trxSys, trx);
}

void TNTRecoverTestCase::insert(Session *session, RecordVal *recs, const u32 count) {
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

RecordVal *TNTRecoverTestCase::copyAllRecordVal(RecordVal *recs, u32 count) {
	RecordVal *recsCopy = new RecordVal[count];
	memcpy(recsCopy, recs, count*sizeof(RecordVal));
	for (u32 i = 0; i < count; i++) {
		recsCopy[i].m_name = new char[30];
		strcpy(recsCopy[i].m_name, recs[i].m_name);
		recsCopy[i].m_comment = new char[200];
		strcpy(recsCopy[i].m_comment, recs[i].m_comment);
		recsCopy[i].m_redRec = createStudentRec(recsCopy[i].m_stuNo, recsCopy[i].m_name, recsCopy[i].m_age, recsCopy[i].m_clsNo, recsCopy[i].m_gpa);
	}
	return recsCopy;
}

void TNTRecoverTestCase::releaseAllRecordVal(RecordVal *recs, u32 count) {
	for (u32 i = 0; i < count; i++) {
		delete[] recs[i].m_comment;
		delete[] recs[i].m_name;
		delete[] recs[i].m_redRec->m_data;
		delete recs[i].m_redRec;
	}
	delete[] recs;
}

void TNTRecoverTestCase::updateAll(RecordVal *recs, const u32 count, u8 factor, bool save, bool log) {
	update(recs, 0, count - 1, factor, save, log);
}

void TNTRecoverTestCase::update(RecordVal *recs, const u32 begin, const u32 end, u8 factor, bool save, bool log) {
	MemoryContext *ctx = m_session->getMemoryContext();
	McSavepoint msp(ctx);

	TNTTrxSys *trxSys = m_db->getTransSys();
	TNTTransaction *trx = startTrx(trxSys, m_conn, TL_X, log);
	m_session->setTrans(trx);
	update(m_session, recs, begin, end, factor, save);
	commitTrx(trxSys, trx);
}

void TNTRecoverTestCase::update(Session *session, RecordVal *recs, const u32 begin, const u32 end, u8 factor, bool save) {
	update(session, m_table, recs, begin, end, factor, save);
}

void TNTRecoverTestCase::update(Session *session, TNTTable *table, RecordVal *recs, const u32 begin, const u32 end, u8 factor, bool save) {
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

	RecordVal *rec = NULL;
	if (!save) {
		rec = (RecordVal *)ctx->alloc(sizeof(RecordVal));
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

void TNTRecoverTestCase::remove(RecordVal *recs, const u32 begin, const u32 end, bool save, bool log) {
	TNTTrxSys *trxSys = m_db->getTransSys();
	TNTTransaction *trx = startTrx(trxSys, m_conn, TL_X, log);
	m_session->setTrans(trx);
	remove(m_session, recs, begin, end, save);
	commitTrx(trxSys, trx);
}

void TNTRecoverTestCase::remove(Session *session, RecordVal *recs, const u32 begin, const u32 end, bool save) {
	remove(session, m_table, recs, begin, end, save);
}

void TNTRecoverTestCase::remove(Session *session, TNTTable *table, RecordVal *recs, const u32 begin, const u32 end, bool save) {
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

void TNTRecoverTestCase::compareAllRecord(RecordVal *recs, u32 count) {
	TNTTrxSys *trxSys = m_db->getTransSys();
	TNTTransaction *trx = startTrx(trxSys, m_conn, TL_S);
	m_session->setTrans(trx);
	for (u32 i = 0; i < count; i++) {
		compareRecord(m_session, m_table, recs + i);
	}
	commitTrx(trxSys, trx);
}

void TNTRecoverTestCase::compareAllNtseRecord(RecordVal *recs, u32 count) {
	for (u32 i = 0; i < count; i++) {
		compareNtseRecord(m_session, m_table->getNtseTable(), recs + i, !recs[i].m_del);
	}
}

void TNTRecoverTestCase::testDump1() {
	u32 count = 1000;
	RecordVal *recs = new RecordVal[count];
	insert(recs, count, false);
	updateAll(recs, count,  0, true, false);
	updateAll(recs, count,  3, true, false);
	DumpThread dumpThr1(m_db);
	dumpThr1.enableSyncPoint(SP_MHEAP_DUMPREALWORK_BEGIN);
	dumpThr1.enableSyncPoint(SP_MHEAP_DUMPREALWORK_REC_MODIFY);
	dumpThr1.start();
	dumpThr1.joinSyncPoint(SP_MHEAP_DUMPREALWORK_BEGIN);
	u32 otherCount = 200;
	RecordVal *otherRecs = new RecordVal[otherCount];
	insert(otherRecs, otherCount, false);
	updateAll(otherRecs, otherCount, 0, false, false);
	updateAll(otherRecs, otherCount, 3, false, false);
	remove(otherRecs, 0, otherCount/4, false, false);
	dumpThr1.disableSyncPoint(SP_MHEAP_DUMPREALWORK_BEGIN);
	dumpThr1.notifySyncPoint(SP_MHEAP_DUMPREALWORK_BEGIN);
	
	dumpThr1.joinSyncPoint(SP_MHEAP_DUMPREALWORK_REC_MODIFY);
	updateAll(recs, count, 6, false, false);
	remove(recs, 0, count/4, false, false);
	dumpThr1.disableSyncPoint(SP_MHEAP_DUMPREALWORK_REC_MODIFY);
	dumpThr1.notifySyncPoint(SP_MHEAP_DUMPREALWORK_REC_MODIFY);
	dumpThr1.join(-1);

	reOpenDb(false, false);
	compareAllRecord(recs, count);

	releaseAllRecordVal(recs, count);
	releaseAllRecordVal(otherRecs, otherCount);
}

void TNTRecoverTestCase::testDump2() {
	u32 count = 1000;
	RecordVal *recs = new RecordVal[count];
	insert(recs, count, false);
	updateAll(recs, count,  0, true, false);
	updateAll(recs, count,  3, true, false);
	m_db->purgeAndDumpTntim(PT_DEFRAG, false, false, false);
	DumpThread dumpThr1(m_db);
	dumpThr1.enableSyncPoint(SP_MHEAP_DUMPREALWORK_REC_MODIFY);
	dumpThr1.start();
	dumpThr1.joinSyncPoint(SP_MHEAP_DUMPREALWORK_REC_MODIFY);
	updateAll(recs, count, 8, false, false);
	dumpThr1.disableSyncPoint(SP_MHEAP_DUMPREALWORK_REC_MODIFY);
	dumpThr1.notifySyncPoint(SP_MHEAP_DUMPREALWORK_REC_MODIFY);
	dumpThr1.join(-1);

	reOpenDb(false, false);
	compareAllRecord(recs, count);

	releaseAllRecordVal(recs, count);
}

void TNTRecoverTestCase::testDumpCompensateRecModify() {
	u32 count = 1000;
	RecordVal *recs = new RecordVal[count];
	insert(recs, count, false);
	updateAll(recs, count,  0, true, false);
	updateAll(recs, count,  2, true, false);
	m_db->purgeAndDumpTntim(PT_DEFRAG, false, false, false);
	DumpThread dumpThr1(m_db);
	dumpThr1.enableSyncPoint(SP_MHEAP_DUMPREALWORK_REC_MODIFY);
	dumpThr1.enableSyncPoint(SP_MHEAP_DUMPCOMPENSATE_REC_MODIFY);
	dumpThr1.start();
	dumpThr1.joinSyncPoint(SP_MHEAP_DUMPREALWORK_REC_MODIFY);
	updateAll(recs, count, 5, false, false);
	dumpThr1.disableSyncPoint(SP_MHEAP_DUMPREALWORK_REC_MODIFY);
	dumpThr1.notifySyncPoint(SP_MHEAP_DUMPREALWORK_REC_MODIFY);

	dumpThr1.joinSyncPoint(SP_MHEAP_DUMPCOMPENSATE_REC_MODIFY);
	updateAll(recs, count, 8, false, false);
	dumpThr1.disableSyncPoint(SP_MHEAP_DUMPCOMPENSATE_REC_MODIFY);
	dumpThr1.notifySyncPoint(SP_MHEAP_DUMPCOMPENSATE_REC_MODIFY);
	dumpThr1.join(-1);

	reOpenDb(false, false);
	compareAllRecord(recs, count);

	releaseAllRecordVal(recs, count);
}

void TNTRecoverTestCase::testPurgeCompensateRecModify() {
	u32 count = 1000;
	RecordVal *recs = new RecordVal[count];
	insert(recs, count, false);
	updateAll(recs, count,  0, true, false);
	updateAll(recs, count,  2, true, false);
	PurgeThread purgeThr1(m_db);
	purgeThr1.enableSyncPoint(SP_MHEAP_PURGENEXT_REC_MODIFY);
	purgeThr1.start();
	purgeThr1.joinSyncPoint(SP_MHEAP_PURGENEXT_REC_MODIFY);
	updateAll(recs, count, 5, false, false);
	purgeThr1.disableSyncPoint(SP_MHEAP_PURGENEXT_REC_MODIFY);
	purgeThr1.notifySyncPoint(SP_MHEAP_PURGENEXT_REC_MODIFY);
	purgeThr1.join(-1);

	reOpenDb(false, false);
	compareAllNtseRecord(recs, count);

	releaseAllRecordVal(recs, count);
}

void TNTRecoverTestCase::testPurge() {
	m_config.m_purgeAfterRecover = true;
	u32 count = 1;
	RecordVal *recs = new RecordVal[count];
	insert(recs, count, false);
	updateAll(recs, count,  0, true, false);
	updateAll(recs, count,  2, true, false);
	m_db->purgeAndDumpTntim(PT_DEFRAG, false, false, false);
	updateAll(recs, count,  4, true, false);
	PurgeThread purgeThr1(m_db);
	purgeThr1.enableSyncPoint(SP_MHEAP_PURGENEXT_REC_MODIFY);
	purgeThr1.start();
	purgeThr1.joinSyncPoint(SP_MHEAP_PURGENEXT_REC_MODIFY);
	updateAll(recs, count, 7, true, false);
	remove(recs, 0, count/4, true, true);

	u32 otherCount = 1;
	RecordVal *otherRecs = new RecordVal[otherCount];
	insert(otherRecs, otherCount, false);
	updateAll(otherRecs, otherCount, 0, false, false);
	//updateAll(otherRecs, otherCount, 7, false, false);
	purgeThr1.disableSyncPoint(SP_MHEAP_PURGENEXT_REC_MODIFY);
	purgeThr1.notifySyncPoint(SP_MHEAP_PURGENEXT_REC_MODIFY);
	purgeThr1.join(-1);

	reOpenDb(true, true);
	compareAllNtseRecord(recs, count);

	releaseAllRecordVal(recs, count);
	releaseAllRecordVal(otherRecs, otherCount);
}

void TNTRecoverTestCase::testPurgeNoDump() {
	m_config.m_purgeBeforeClose = false;
	m_config.m_dumpBeforeClose = false;
	u32 count = 1000;
	RecordVal *recs = new RecordVal[count];
	insert(recs, count, false);
	updateAll(recs, count,  0, true, false);
	updateAll(recs, count,  2, true, false);
	m_db->purgeAndDumpTntim(PT_DEFRAG, false, false, false);
	updateAll(recs, count,  4, true, false);
	m_db->purgeAndDumpTntim(PT_PURGEPHASE2, false, false, true);
	reOpenDb(true, true);
	//recover purge第二阶段时会将内存记录摘除
	compareAllNtseRecord(recs, count);

	releaseAllRecordVal(recs, count);
}

void TNTRecoverTestCase::testPurgeWaitActiveTrx() {
	m_config.m_purgeAfterRecover = true;
	u32 count1 = 1000;
	RecordVal *recs1 = new RecordVal[count1];
	insert(recs1, count1, true);
	updateAll(recs1, count1,  2, true, true);
	updateAll(recs1, count1,  6, true, true);

	u32 count2 = 500;
	RecordVal *recs2 = new RecordVal[count2];
	TNTTrxSys *trxSys = m_db->getTransSys();
	TNTTransaction *trx = startTrx(trxSys, m_conn, TL_X, true);
	m_session->setTrans(trx);
	insert(m_session, recs2, count2);
	update(m_session, recs2, 0, 3*count2/4, 1, false);

	PurgeThread purgeThr(m_db);
	//purgeThr.enableSyncPoint(SP_TNTDB_PURGEWAITTRX_WAIT);
	purgeThr.enableSyncPoint(SP_TNTDB_PURGE_PHASE2_BEFORE);
	purgeThr.start();
	//purgeThr.joinSyncPoint(SP_TNTDB_PURGEWAITTRX_WAIT);
	purgeThr.joinSyncPoint(SP_TNTDB_PURGE_PHASE2_BEFORE);

	update(m_session, recs2, 0, 3*count2/4, 5, false);
	remove(m_session, recs2, 0, count2 - 1, true);
	trx->prepareForMysql();
	trx->commitTrx(CS_NORMAL);
	trxSys->freeTrx(trx);

	//purgeThr.disableSyncPoint(SP_TNTDB_PURGEWAITTRX_WAIT);
	purgeThr.disableSyncPoint(SP_TNTDB_PURGE_PHASE2_BEFORE);
	//purgeThr.notifySyncPoint(SP_TNTDB_PURGEWAITTRX_WAIT);
	purgeThr.notifySyncPoint(SP_TNTDB_PURGE_PHASE2_BEFORE);
	purgeThr.join(-1);

	compareAllNtseRecord(recs1, count1);
	for (u32 i = 0; i < count2; i++) {
		compareNtseRecord(m_session, m_table->getNtseTable(), recs2 + i, true);
	}

	compareAllRecord(recs1, count1);
	compareAllRecord(recs2, count2);

	reOpenDb(true, true);

	compareAllNtseRecord(recs1, count1);
	//reopen的recover后会purge
	compareAllNtseRecord(recs2, count2);
	compareAllRecord(recs1, count1);
	compareAllRecord(recs2, count2);

	releaseAllRecordVal(recs1, count1);
	releaseAllRecordVal(recs2, count2);
}

void TNTRecoverTestCase::testBug34() {
	u32 count1 = 2000;
	RecordVal *recs1 = new RecordVal[count1];
	insert(recs1, count1, true);
	updateAll(recs1, count1, 2, true, true);
	updateAll(recs1, count1, 8, true, true);

	GetThread getThr(m_db, m_table, recs1 + 1960, 1);
	getThr.enableSyncPoint(SP_MRECS_GETRECORD_PTR_MODIFY);
	getThr.start();
	getThr.joinSyncPoint(SP_MRECS_GETRECORD_PTR_MODIFY);
	m_db->purgeAndDumpTntim(PT_DEFRAG, false, false, false);
	
	u32 count2 = 10000;
	RecordVal *recs2 = new RecordVal[count2];
	insert(recs2, count2, true);
	getThr.disableSyncPoint(SP_MRECS_GETRECORD_PTR_MODIFY);
	getThr.notifySyncPoint(SP_MRECS_GETRECORD_PTR_MODIFY);
	getThr.join(-1);
	
	releaseAllRecordVal(recs1, count1);
	releaseAllRecordVal(recs2, count2);
}

void TNTRecoverTestCase::testRecoverIUD() {
	u32 count = 1000;
	RecordVal *recs = new RecordVal[count];
	insert(recs, count, true);
	update(recs, count/4,  count/2, 1, true, true);
	update(recs, 2*count/5, 3*count/5, 2, true, true);
	remove(recs, count/4, 3*count/4, true, true);

	u32 noCommitCount = 50;
	RecordVal *noCommitRecs = new RecordVal[noCommitCount];
	TNTTrxSys *trxSys = m_db->getTransSys();
	TNTTransaction *trx = startTrx(trxSys, m_conn, TL_X, true);
	m_session->setTrans(trx);
	insert(m_session, noCommitRecs, noCommitCount);
	update(m_session, noCommitRecs, 0, 3*noCommitCount/4, 1, true);
	update(m_session, noCommitRecs, 1*noCommitCount/4, noCommitCount/2, 2, true);
	remove(m_session, noCommitRecs, noCommitCount/4, noCommitCount - 1, true);
	setAllDelete(noCommitRecs, noCommitCount);
	//对该事务并未commit,而是直接移除
	trxSys->freeTrx(trx);

	reOpenDb(false, false);
	compareAllRecord(recs, count);
	compareAllRecord(noCommitRecs, noCommitCount);

	trxSys = m_db->getTransSys();
	trx = startTrx(trxSys, m_conn, TL_X, true);
	m_session->setTrans(trx);
	update(m_session, recs, 0, count/5, 2, false);
	rollBackTrx(trxSys, trx);
	compareAllRecord(recs, count);

	update(recs, 4*count/5, 9*count/10, 5, true, false);
	remove(recs, 0, count/4 - 1, true, false);
	compareAllRecord(recs, count);

	releaseAllRecordVal(recs, count);
	releaseAllRecordVal(noCommitRecs, noCommitCount);
}

void TNTRecoverTestCase::testDefrag() {
	u32 count1 = 10000;
	RecordVal *recs1 = new RecordVal[count1];
	insert(recs1, count1, true);
	updateAll(recs1, count1,  2, true, true);
	updateAll(recs1, count1,  6, true, true);

	u32 count2 = 20000;
	RecordVal *recs2 = new RecordVal[count2];
	TNTTrxSys *trxSys = m_db->getTransSys();
	TNTTransaction *trx = startTrx(trxSys, m_conn, TL_X, true);
	m_session->setTrans(trx);
	insert(m_session, recs2, count2);
	update(m_session, recs2, 0, 3*count2/4, 1, true);
	update(m_session, recs2, 0, 3*count2/4, 5, true);

	m_db->purgeAndDumpTntim(PT_DEFRAG, false, false, false);

	trx->commitTrx(CS_NORMAL);
	trxSys->freeTrx(trx);

	compareAllRecord(recs1, count1);
	compareAllRecord(recs2, count2);

	reOpenDb(true, true);

	compareAllRecord(recs1, count1);
	compareAllRecord(recs2, count2);

	releaseAllRecordVal(recs1, count1);
	releaseAllRecordVal(recs2, count2);
}

void TNTRecoverTestCase::testPrepareRecover() {
	u32 prepareCnt = 50;
	RecordVal *prepareRecs = new RecordVal[prepareCnt];
	TNTTrxSys *trxSys = m_db->getTransSys();
	TNTTransaction *trx = startTrx(trxSys, m_conn, TL_X, true);

	XID xid;
	memset(&xid, 0, sizeof(XID));
	xid.formatID		= 1;
	xid.gtrid_length	= 20;
	xid.bqual_length	= 0;
	char *ch = "MySQLXid    00000001";
	memcpy(xid.data, ch, xid.gtrid_length);
	trx->setXId((const XID &)xid);

	m_session->setTrans(trx);
	trx->markSqlStatEnd();
	CPPUNIT_ASSERT(trx->getTrxLastLsn() == INVALID_LSN);
	CPPUNIT_ASSERT(trx->getLastStmtLsn() == INVALID_LSN);
	insert(m_session, prepareRecs, prepareCnt);
	update(m_session, prepareRecs, 0, 3*prepareCnt/4, 1, true);
	trx->markSqlStatEnd();
	CPPUNIT_ASSERT(trx->getTrxLastLsn() != INVALID_LSN);
	CPPUNIT_ASSERT(trx->getLastStmtLsn() != INVALID_LSN);
	update(m_session, prepareRecs, 1*prepareCnt/4, prepareCnt/2, 2, true);
	remove(m_session, prepareRecs, prepareCnt/4, prepareCnt - 1, true);
	trx->prepareForMysql();
	//对该事务prepare后,而是直接移除
	trxSys->freeTrx(trx);

	reOpenDb(false, false);
	trxSys = m_db->getTransSys();

	XID xids[10];
	memset(xids, 0, 10*sizeof(XID));
	uint size = trxSys->getPreparedTrxForMysql(xids, 10);
	CPPUNIT_ASSERT(1 == size);
	CPPUNIT_ASSERT(memcmp(&xid, xids, sizeof(XID)) == 0);
	trx = trxSys->getTrxByXID(xids);
	commitTrx(trxSys, trx);
	compareAllRecord(prepareRecs, prepareCnt);

	releaseAllRecordVal(prepareRecs, prepareCnt);
}

void TNTRecoverTestCase::testRollBack() {
	u32 i = 0;
	u32 count = 1000;
	RecordVal *recs = new RecordVal[count];

	//对插入记录做rollBack
	TNTTrxSys *trxSys = m_db->getTransSys();
	TNTTransaction *trx = startTrx(trxSys, m_conn, TL_X, true);
	m_session->setTrans(trx);
	insert(m_session, recs, count);
	rollBackTrx(trxSys, trx);
	setAllDelete(recs, count);
	compareAllRecord(recs, count);

	for (i = 0; i < count; i++) {
		delete[] recs[i].m_comment;
		delete[] recs[i].m_name;
		delete[] recs[i].m_redRec->m_data;
		delete recs[i].m_redRec;
	}

	//对首次更新和非首次更新记录做rollBack
	insert(recs, count, true);
	trx = startTrx(trxSys, m_conn, TL_X, true);
	m_session->setTrans(trx);
	update(m_session, recs, 0, count/2, 2, false);
	update(m_session, recs, count/4, 3*count/4, 5, false);
	rollBackTrx(trxSys, trx);
	compareAllRecord(recs, count);
	for (i = 0; i < count; i++) {
		delete[] recs[i].m_comment;
		delete[] recs[i].m_name;
		delete[] recs[i].m_redRec->m_data;
		delete recs[i].m_redRec;
	}

	//对首次删除和非首次删除记录做rollBack
	insert(recs, count, true);
	update(recs, 0, count/2, 1, true, true);
	update(recs, count/4, 3*count/4, 5, true, true);
	trx = startTrx(trxSys, m_conn, TL_X, true);
	m_session->setTrans(trx);
	remove(m_session, recs, 0, 4*count/5, false);
	rollBackTrx(trxSys, trx);
	compareAllRecord(recs, count);

	reOpenDb(true, true);
	compareAllRecord(recs, count);
	
	releaseAllRecordVal(recs, count);
}

void TNTRecoverTestCase::testRedoPurgePhrase1() {
	m_config.m_purgeAfterRecover = false;
	u32 count = 1000;
	RecordVal *recs = new RecordVal[count];
	insert(recs, count, true);
	updateAll(recs, count,  0, true, true);
	updateAll(recs, count,  2, true, true);
	m_db->purgeAndDumpTntim(PT_DEFRAG, false, false, false);
	updateAll(recs, count,  4, true, true);

	TNTTrxSys *trxSys = m_db->getTransSys();
	TNTTransaction *trx = startTrx(trxSys, m_conn, TL_X, true);
	m_session->setTrans(trx);
	m_db->writePurgeBeginLog(m_session, trx->getTrxId(), trx->getTrxLastLsn());
	m_table->writePurgePhase1(m_session, trx->getTrxId(), trx->getTrxLastLsn(), trx->getTrxId());
	trx->flushTNTLog(trx->getTrxLastLsn());
	trxSys->freeTrx(trx);

	reOpenDb(true, true);
	compareAllNtseRecord(recs, count);

	releaseAllRecordVal(recs, count);
}

void TNTRecoverTestCase::testRedoPurgePhrase2() {
	m_config.m_purgeAfterRecover = false;
	u32 count = 1000;
	RecordVal *recs = new RecordVal[count];
	insert(recs, count, true);
	updateAll(recs, count,  0, false, true);
	updateAll(recs, count,  2, false, true);
	m_db->purgeAndDumpTntim(PT_DEFRAG, false, false, false);
	updateAll(recs, count,  4, false, true);

	TNTTrxSys *trxSys = m_db->getTransSys();
	TNTTransaction *trx = startTrx(trxSys, m_conn, TL_X, true);
	m_session->setTrans(trx);
	m_db->writePurgeBeginLog(m_session, trx->getTrxId(), trx->getTrxLastLsn());
	m_table->writePurgePhase1(m_session, trx->getTrxId(), trx->getTrxLastLsn(), trx->getTrxId());
	m_table->writePurgePhase2(m_session, trx->getTrxId(), trx->getTrxLastLsn());
	trx->flushTNTLog(trx->getTrxLastLsn());
	trxSys->freeTrx(trx);

	reOpenDb(true, true);
	compareAllNtseRecord(recs, count);
	compareAllRecord(recs, count);

	releaseAllRecordVal(recs, count);
}

void TNTRecoverTestCase::testRedoBeginPurge() {
	m_config.m_purgeAfterRecover = false;
	u32 count = 1;
	RecordVal *recs = new RecordVal[count];
	insert(recs, count, true);
	RecordVal *ntseRecs = copyAllRecordVal(recs, count);
	updateAll(recs, count,  0, true, true);
	updateAll(recs, count,  2, true, true);
	m_db->purgeAndDumpTntim(PT_DEFRAG, false, false, false);
	updateAll(recs, count,  4, true, true);

	TNTTrxSys *trxSys = m_db->getTransSys();
	TNTTransaction *trx = startTrx(trxSys, m_conn, TL_X, true);
	m_session->setTrans(trx);
	m_db->writePurgeBeginLog(m_session, trx->getTrxId(), trx->getTrxLastLsn());
	trx->flushTNTLog(trx->getTrxLastLsn());
	trxSys->freeTrx(trx);

	reOpenDb(true, true);
	compareAllRecord(recs, count);
	compareAllNtseRecord(ntseRecs, count);

	releaseAllRecordVal(recs, count);
	releaseAllRecordVal(ntseRecs, count);
}

void TNTRecoverTestCase::testRedoEndPurge() {
	m_config.m_purgeAfterRecover = false;
	u32 count = 1000;
	RecordVal *recs = new RecordVal[count];
	insert(recs, count, true);
	updateAll(recs, count,  0, false, true);
	updateAll(recs, count,  2, false, true);
	m_db->purgeAndDumpTntim(PT_DEFRAG, false, false, false);
	updateAll(recs, count,  4, false, true);

	TNTTrxSys *trxSys = m_db->getTransSys();
	TNTTransaction *trx = startTrx(trxSys, m_conn, TL_X, true);
	m_session->setTrans(trx);
	m_db->writePurgeBeginLog(m_session, trx->getTrxId(), trx->getTrxLastLsn());
	m_table->writePurgePhase1(m_session, trx->getTrxId(), trx->getTrxLastLsn(), trx->getTrxId());
	m_table->writePurgePhase2(m_session, trx->getTrxId(), trx->getTrxLastLsn());
	m_table->writePurgeTableEnd(m_session, trx->getTrxId(), trx->getTrxLastLsn());
	m_db->writePurgeEndLog(m_session, trx->getTrxId(), trx->getTrxLastLsn());
	trx->flushTNTLog(trx->getTrxLastLsn());
	trxSys->freeTrx(trx);

	reOpenDb(true, true);
	
	//在redo purge第二阶段时会将内存记录摘除
	compareAllRecord(recs, count);
	compareAllNtseRecord(recs, count);

	releaseAllRecordVal(recs, count);
}

void TNTRecoverTestCase::testRedoPurgeEndHeap() {
	m_config.m_purgeAfterRecover = false;
	u32 count = 1000;
	RecordVal *recs = new RecordVal[count];
	insert(recs, count, true);
	updateAll(recs, count,  0, false, true);
	updateAll(recs, count,  2, false, true);
	m_db->purgeAndDumpTntim(PT_DEFRAG, false, false, false);
	updateAll(recs, count,  4, false, true);

	TNTTrxSys *trxSys = m_db->getTransSys();
	TNTTransaction *trx = startTrx(trxSys, m_conn, TL_X, true);
	m_session->setTrans(trx);
	m_db->writePurgeBeginLog(m_session, trx->getTrxId(), trx->getTrxLastLsn());
	m_table->writePurgePhase1(m_session, trx->getTrxId(), trx->getTrxLastLsn(), trx->getTrxId());
	m_table->writePurgePhase2(m_session, trx->getTrxId(), trx->getTrxLastLsn());
	m_table->writePurgeTableEnd(m_session, trx->getTrxId(), trx->getTrxLastLsn());
	trx->flushTNTLog(trx->getTrxLastLsn());
	trxSys->freeTrx(trx);

	reOpenDb(true, true);
	
	//在redo purge第二阶段时会将内存记录摘除
	compareAllRecord(recs, count);
	compareAllNtseRecord(recs, count);

	releaseAllRecordVal(recs, count);
}

void TNTRecoverTestCase::setAllDelete(RecordVal *recs, u32 count) {
	for (u32 i = 0; i < count; i++) {
		recs[i].m_del = true;
	}

}

void TNTRecoverTestCase::clearAllDelete(RecordVal *recs, u32 count) {
	for (u32 i = 0; i < count; i++) {
		recs[i].m_del = false;
	}

}

// void TNTRecoverTestCase::testPermissionConflict() {
// 	bool catchException = false;
// 	//purge正在进行，去进行dump和defrag
// 	PurgeThread purgeThr(m_db);
// 	purgeThr.enableSyncPoint(SP_TNTDB_ACQUIREPERMISSION_WAIT);
// 	purgeThr.start();
// 	purgeThr.joinSyncPoint(SP_TNTDB_ACQUIREPERMISSION_WAIT);
// 
// 	try {
// 		m_db->dumpTntim();
// 		CPPUNIT_FAIL("can't dump, because purge is running");
// 	} catch (NtseException &e) {
// 		catchException = true;
// 		CPPUNIT_ASSERT(e.getErrorCode() == NTSE_EC_IN_PURGING);
// 	}
// 	CPPUNIT_ASSERT(catchException == true);
// 	catchException = false;
// 
// 	try {
// 		m_db->defragTntim();
// 		CPPUNIT_FAIL("can't defrag, because purge is running");
// 	} catch (NtseException &e) {
// 		catchException = true;
// 		CPPUNIT_ASSERT(e.getErrorCode() == NTSE_EC_IN_PURGING);
// 	}
// 	CPPUNIT_ASSERT(catchException == true);
// 	catchException = false;
// 	purgeThr.disableSyncPoint(SP_TNTDB_ACQUIREPERMISSION_WAIT);
// 	purgeThr.notifySyncPoint(SP_TNTDB_ACQUIREPERMISSION_WAIT);
// 	purgeThr.join(-1);
// 
// 	//dump正在进行，去进行purge和defrag
// 	DumpThread dumpThr(m_db);
// 	dumpThr.enableSyncPoint(SP_TNTDB_ACQUIREPERMISSION_WAIT);
// 	dumpThr.start();
// 	dumpThr.joinSyncPoint(SP_TNTDB_ACQUIREPERMISSION_WAIT);
// 
// 	try {
// 		m_db->purgeTntim(PT_PURGEPHASE2);
// 		CPPUNIT_FAIL("can't purge, because dump is running");
// 	} catch (NtseException &e) {
// 		catchException = true;
// 		CPPUNIT_ASSERT(e.getErrorCode() == NTSE_EC_IN_DUMPING);
// 	}
// 	CPPUNIT_ASSERT(catchException == true);
// 	catchException = false;
// 
// 	try {
// 		m_db->defragTntim();
// 		CPPUNIT_FAIL("can't defrag, because dump is running");
// 	} catch (NtseException &e) {
// 		catchException = true;
// 		CPPUNIT_ASSERT(e.getErrorCode() == NTSE_EC_IN_DUMPING);
// 	}
// 	CPPUNIT_ASSERT(catchException == true);
// 	catchException = false;
// 	dumpThr.disableSyncPoint(SP_TNTDB_ACQUIREPERMISSION_WAIT);
// 	dumpThr.notifySyncPoint(SP_TNTDB_ACQUIREPERMISSION_WAIT);
// 	dumpThr.join(-1);
// 
// 	//defrag正在进行，去进行purge和dump
// 	DefragThread defragThr(m_db);
// 	defragThr.enableSyncPoint(SP_TNTDB_ACQUIREPERMISSION_WAIT);
// 	defragThr.start();
// 	defragThr.joinSyncPoint(SP_TNTDB_ACQUIREPERMISSION_WAIT);
// 
// 	try {
// 		m_db->purgeTntim(PT_PURGEPHASE2);
// 		CPPUNIT_FAIL("can't purge, because defrag is running");
// 	} catch (NtseException &e) {
// 		catchException = true;
// 		CPPUNIT_ASSERT(e.getErrorCode() == NTSE_EC_IN_DEFRAGING);
// 	}
// 	CPPUNIT_ASSERT(catchException == true);
// 	catchException = false;
// 
// 	try {
// 		m_db->dumpTntim();
// 		CPPUNIT_FAIL("can't dump, because defrag is running");
// 	} catch (NtseException &e) {
// 		catchException = true;
// 		CPPUNIT_ASSERT(e.getErrorCode() == NTSE_EC_IN_DEFRAGING);
// 	}
// 	CPPUNIT_ASSERT(catchException == true);
// 	catchException = false;
// 	defragThr.disableSyncPoint(SP_TNTDB_ACQUIREPERMISSION_WAIT);
// 	defragThr.notifySyncPoint(SP_TNTDB_ACQUIREPERMISSION_WAIT);
// 	defragThr.join(-1);
// }

void TNTRecoverTestCase::testConcurrentTrxDump() {
	m_config.m_purgeBeforeClose = false;
	m_config.m_dumpBeforeClose = false;
	m_config.m_purgeAfterRecover = false;
	u32 recCnt1 = 50;
	RecordVal *recs1 = new RecordVal[recCnt1];
	u32 recCnt2 = 100;
	RecordVal *recs2 = new RecordVal[recCnt2];
	u32 recCnt3 = 150;
	RecordVal *recs3 = new RecordVal[recCnt3];

	TNTTrxSys *trxSys = m_db->getTransSys();

	Connection *conn1 = m_db->getNtseDb()->getConnection(false);
	Session *session1 = m_db->getNtseDb()->getSessionManager()->allocSession("testDumpNoRecover session1", conn1);
	TNTTransaction *trx1 = startTrx(trxSys, conn1, TL_X, true);

	Connection *conn2 = m_db->getNtseDb()->getConnection(false);
	Session *session2 = m_db->getNtseDb()->getSessionManager()->allocSession("testDumpNoRecover session2", conn2);
	TNTTransaction *trx2 = startTrx(trxSys, conn2, TL_X, true);
	XID xid;
	memset(&xid, 0, sizeof(XID));
	xid.formatID		= 1;
	xid.gtrid_length	= 20;
	xid.bqual_length	= 0;
	char *ch = "MySQLXid    00000001";
	memcpy(xid.data, ch, xid.gtrid_length);
	trx1->setXId((const XID &)xid);
	session1->setTrans(trx1);
	insert(session1, recs1, recCnt1);
	RecordVal *ntseRecs = copyAllRecordVal(recs1, recCnt1);
	update(session1, recs1, 0, 3*recCnt1/4, 1, true);
	update(session1, recs1, recCnt1/4, 3*recCnt1/5, 3, true);

	session2->setTrans(trx2);
	insert(session2, recs2, recCnt2);
	update(session2, recs2, recCnt2/4, 3*recCnt2/5, 3, false);

	Connection *conn3 = m_db->getNtseDb()->getConnection(false);
	Session *session3 = m_db->getNtseDb()->getSessionManager()->allocSession("testDumpNoRecover session3", conn3);
	TNTTransaction *trx3 = startTrx(trxSys, conn3, TL_X, true);
	session3->setTrans(trx3);
	insert(session3, recs3, recCnt3);
	remove(session3, recs3, 0, recCnt3/2, true);

	trx1->markSqlStatEnd();
	remove(session1, recs1, recCnt1/4, recCnt1/2, false);
	trx1->rollbackLastStmt(RBS_NORMAL, session1);
	trx1->prepareForMysql();
	CPPUNIT_ASSERT(trx1->commitTrx(CS_NORMAL));
	trxSys->freeTrx(trx1);
	m_db->getNtseDb()->getSessionManager()->freeSession(session1);
	m_db->getNtseDb()->freeConnection(conn1);

	trx2->rollbackTrx(RBS_NORMAL, session2);
	trxSys->freeTrx(trx2);
	setAllDelete(recs2, recCnt2);
	m_db->getNtseDb()->getSessionManager()->freeSession(session2);
	m_db->getNtseDb()->freeConnection(conn2);
	
	m_db->purgeAndDumpTntim(PT_NONE, false, true, false);
	trx3->commitTrx(CS_NORMAL);
	trxSys->freeTrx(trx3);
	m_db->getNtseDb()->getSessionManager()->freeSession(session3);
	m_db->getNtseDb()->freeConnection(conn3);

	//reopen
	m_db->closeTable(m_session, m_table);
	Database *db = m_db->getNtseDb();
	db->getSessionManager()->freeSession(m_session);
	db->freeConnection(m_conn);
	m_db->close(true, true);

	delete m_db;
	m_db = NULL;

	//不进行recover
	EXCPT_OPER(m_db = TNTDatabase::open(&m_config, false, 1));

	db = m_db->getNtseDb();
	m_conn = db->getConnection(false);
	m_session = db->getSessionManager()->allocSession("TNTRecoverTestCase", m_conn);

	string tablePath("./");
	tablePath.append(TNT_STU_SCHEMA_NAME).append(NTSE_PATH_SEP).append(TNT_STU_TBL_NAME);
	trxSys = m_db->getTransSys();
	TNTTransaction *trx = startTrx(trxSys, m_conn);
	m_session->setTrans(trx);
	m_table = m_db->openTable(m_session, tablePath.c_str());
	CPPUNIT_ASSERT(m_table != NULL);
	commitTrx(trxSys, trx);

	compareAllRecord(recs1, recCnt1);
	compareAllNtseRecord(ntseRecs, recCnt1);
	compareAllRecord(recs2, recCnt2);
	compareAllNtseRecord(recs2, recCnt2);
	compareAllRecord(recs3, recCnt3);

	releaseAllRecordVal(ntseRecs, recCnt1);
	releaseAllRecordVal(recs1, recCnt1);
	releaseAllRecordVal(recs2, recCnt2);
	releaseAllRecordVal(recs3, recCnt3);
}

void TNTRecoverTestCase::testPurgeRecover() {
	u32 count = 1000;
	RecordVal *recs = new RecordVal[count];
	insert(recs, count, false);
	updateAll(recs, count,  0, true, false);
	updateAll(recs, count,  2, true, false);
	m_db->purgeAndDumpTntim(PT_DEFRAG, false, false, false);
	updateAll(recs, count,  4, true, false);
	PurgeThread purgeThr1(m_db);
	purgeThr1.enableSyncPoint(SP_TNTDB_PURGE_PHASE2_BEFORE);
	purgeThr1.start();
	purgeThr1.joinSyncPoint(SP_TNTDB_PURGE_PHASE2_BEFORE);

	u32 otherCount = 200;
	RecordVal *otherRecs = new RecordVal[otherCount];
	TNTTrxSys *trxSys = m_db->getTransSys();
	TNTTransaction *trx = startTrx(trxSys, m_conn, TL_X, true);
	m_session->setTrans(trx);
	insert(m_session, otherRecs, otherCount);
	update(m_session, otherRecs, 0, otherCount/2, 2, true);
	purgeThr1.disableSyncPoint(SP_TNTDB_PURGE_PHASE2_BEFORE);
	purgeThr1.notifySyncPoint(SP_TNTDB_PURGE_PHASE2_BEFORE);
	purgeThr1.join(-1);

	trx->commitTrx(CS_NORMAL);
	trxSys->freeTrx(trx);

	reOpenDb(true, true);
	compareAllRecord(recs, count);
	compareAllRecord(otherRecs, otherCount);
	compareAllNtseRecord(recs, count);

	releaseAllRecordVal(recs, count);
	releaseAllRecordVal(otherRecs, otherCount);
}

void TNTRecoverTestCase::testRecoverTblDrop() {
	m_config.m_purgeBeforeClose = false;
	m_config.m_dumpBeforeClose = false;
	u32 count = 1000;
	RecordVal *recs = new RecordVal[count];
	insert(recs, count, true);
	updateAll(recs, count,  0, true, true);
	updateAll(recs, count,  2, true, true);
	m_db->purgeAndDumpTntim(PT_PURGEPHASE2, false, false, true);
	m_db->closeTable(m_session, m_table);
	m_table = NULL;

	string tablePath("./");
	tablePath.append(TNT_STU_SCHEMA_NAME).append(NTSE_PATH_SEP).append(TNT_STU_TBL_NAME);
	EXCPT_OPER(m_db->dropTable(m_session, tablePath.c_str(), -1));

	//reopen
	Database *db = m_db->getNtseDb();
	db->getSessionManager()->freeSession(m_session);
	db->freeConnection(m_conn);
	m_db->close(true, true);
	delete m_db;
	m_db = NULL;

	//不进行recover
	EXCPT_OPER(m_db = TNTDatabase::open(&m_config, false, 0));

	db = m_db->getNtseDb();
	m_conn = db->getConnection(false);
	m_session = db->getSessionManager()->allocSession("TNTRecoverTestCase", m_conn);

	bool catchException = false;
	TNTTrxSys *trxSys = m_db->getTransSys();
	TNTTransaction *trx = startTrx(trxSys,m_conn);
	m_session->setTrans(trx);
	try {
		m_table = m_db->openTable(m_session, tablePath.c_str());
	} catch (NtseException &) {
		catchException = true;
	}

	CPPUNIT_ASSERT(catchException == true);
	CPPUNIT_ASSERT(m_table == NULL);
	commitTrx(trxSys, trx);

	releaseAllRecordVal(recs, count);
}

void TNTRecoverTestCase::testDumpUpdate() {
	m_config.m_purgeBeforeClose = false;
	m_config.m_dumpBeforeClose = false;
	m_config.m_purgeAfterRecover = false;
	u32 count = 1000;
	RecordVal *recs = new RecordVal[count];
	insert(recs, count, false);
	updateAll(recs, count,  0, true, false);
	updateAll(recs, count,  2, true, false);
	m_db->purgeAndDumpTntim(PT_DEFRAG, false, false, false);
	updateAll(recs, count,  5, true, false);

	DumpThread dumpThr(m_db);
	dumpThr.enableSyncPoint(SP_MRECS_DUMP_AFTER_DUMPREALWORK);
	dumpThr.start();
	dumpThr.joinSyncPoint(SP_MRECS_DUMP_AFTER_DUMPREALWORK);

	UpdateThread updateThr(m_db, m_table, recs, count, 9, false, false);
	updateThr.enableSyncPoint(SP_MHEAP_UPDATE_WAIT_DUMP_FINISH);
	updateThr.start();
	updateThr.joinSyncPoint(SP_MHEAP_UPDATE_WAIT_DUMP_FINISH);

	dumpThr.disableSyncPoint(SP_MRECS_DUMP_AFTER_DUMPREALWORK);
	dumpThr.notifySyncPoint(SP_MRECS_DUMP_AFTER_DUMPREALWORK);
	dumpThr.join(-1);

	updateThr.disableSyncPoint(SP_MHEAP_UPDATE_WAIT_DUMP_FINISH);
	updateThr.notifySyncPoint(SP_MHEAP_UPDATE_WAIT_DUMP_FINISH);
	updateThr.join(-1);

	reOpenDb(true, true);
	compareAllRecord(recs, count);

	releaseAllRecordVal(recs, count);
}

void TNTRecoverTestCase::testRollBackDefrag() {
	m_config.m_purgeBeforeClose = false;
	m_config.m_dumpBeforeClose = false;
	m_config.m_purgeAfterRecover = false;
	u32 count = 2000;
	RecordVal *recs = new RecordVal[count];
	insert(recs, count, false);
	updateAll(recs, count,  0, true, false);
	m_db->purgeAndDumpTntim(PT_DEFRAG, false, false, false);
	updateAll(recs, count,  5, true, false);

	TNTTrxSys *trxSys = m_db->getTransSys();
	TNTTransaction *trx1 = startTrx(trxSys, m_conn, TL_X, false);
	m_session->setTrans(trx1);
	remove(m_session, m_table, recs, 1, count - 3, true);
	trx1->commitTrx(CS_NORMAL);
	trxSys->freeTrx(trx1);

	TNTTransaction *trx2 = startTrx(trxSys, m_conn, TL_X, false);
	m_session->setTrans(trx2);
	update(m_session, m_table, recs, 0, 0, 6, true);
	update(m_session, m_table, recs, count - 1, count - 1, 6, true);
	PurgeThread purgeThr(m_db, PT_PURGEPHASE2);
	//purgeThr.enableSyncPoint(SP_TNTDB_PURGEWAITTRX_WAIT);
	purgeThr.enableSyncPoint(SP_TNTDB_PURGE_PHASE2_BEFORE);
	purgeThr.start();
	//purgeThr.joinSyncPoint(SP_TNTDB_PURGEWAITTRX_WAIT);
	purgeThr.joinSyncPoint(SP_TNTDB_PURGE_PHASE2_BEFORE);
	trx2->commitTrx(CS_NORMAL);
	trxSys->freeTrx(trx2);
	//purgeThr.disableSyncPoint(SP_TNTDB_PURGEWAITTRX_WAIT);
	purgeThr.disableSyncPoint(SP_TNTDB_PURGE_PHASE2_BEFORE);
	//purgeThr.notifySyncPoint(SP_TNTDB_PURGEWAITTRX_WAIT);
	purgeThr.notifySyncPoint(SP_TNTDB_PURGE_PHASE2_BEFORE);
	purgeThr.join(-1);

	//rollback必须logging为true
	RollBackUpdateThread rollBackUpdateThr(m_db, m_table, recs + count - 2, 1, 9, false, true);
	rollBackUpdateThr.enableSyncPoint(SP_MRECS_ROLLBACK_BEFORE_GETHEAPREC);
	rollBackUpdateThr.start();
	rollBackUpdateThr.joinSyncPoint(SP_MRECS_ROLLBACK_BEFORE_GETHEAPREC);
	m_db->purgeAndDumpTntim(PT_DEFRAG, false, false, false);
	rollBackUpdateThr.disableSyncPoint(SP_MRECS_ROLLBACK_BEFORE_GETHEAPREC);
	rollBackUpdateThr.notifySyncPoint(SP_MRECS_ROLLBACK_BEFORE_GETHEAPREC);
	rollBackUpdateThr.join(-1);

	compareAllRecord(recs, count);

	releaseAllRecordVal(recs, count);
}

void TNTRecoverTestCase::testDefragHashIndex() {
	HashIndexEntry *entry = NULL;
	MemoryContext *ctx = NULL;
	u32 count = 800;
	RecordVal *recs = new RecordVal[count];
	insert(recs, count, false);
	compareAllRecord(recs, count);

	TNTTrxSys *trxSys = m_db->getTransSys();
	TNTTransaction *trx1 = startTrx(trxSys, m_conn, TL_IS, false);
	m_session->setTrans(trx1);
	for (u32 i = 0; i < count; i++) {
		ctx = m_session->getMemoryContext();
		McSavepoint msp(ctx);
		entry = m_table->getMRecords()->getHashIndex()->get(recs[i].m_rid, ctx);
		CPPUNIT_ASSERT(entry != NULL);
		CPPUNIT_ASSERT(entry->m_rowId = recs[i].m_rid);
		CPPUNIT_ASSERT(entry->m_type = HIT_TXNID);
	}
	trx1->commitTrx(CS_NORMAL);
	trxSys->freeTrx(trx1);

	uint total = m_db->defragHashIndex(false);

	compareAllRecord(recs, count);

	u32 nullCount = 0;
	TNTTransaction *trx2 = startTrx(trxSys, m_conn, TL_IS, false);
	m_session->setTrans(trx2);
	for (u32 i = 0; i < count; i++) {
		ctx = m_session->getMemoryContext();
		McSavepoint msp(ctx);
		entry = m_table->getMRecords()->getHashIndex()->get(recs[i].m_rid, ctx);
		if (entry == NULL) {
			nullCount++;
		}
	}
	trx2->commitTrx(CS_NORMAL);
	trxSys->freeTrx(trx2);
	CPPUNIT_ASSERT(nullCount == total);

	releaseAllRecordVal(recs, count);
}