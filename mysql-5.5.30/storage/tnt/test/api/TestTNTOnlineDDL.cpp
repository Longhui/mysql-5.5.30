#include "TestTNTOnlineDDL.h"
#include "Test.h"
#include "api/TNTTable.h"
#include "api/Table.h"
#include "misc/RecordHelper.h"
#include "api/IdxPreAlter.h"
#include "misc/OptimizeThread.h"

#define TNT_STU_SCHEMA_NAME "Olympic"
#define	TNT_STU_TBL_NAME "student"

#define TNT_STU_SNO "sno"
#define TNT_STU_NAME "name"
#define TNT_STU_AGE "age"
#define TNT_STU_CLASS "class"
#define TNT_STU_GPA "gpa"
#define TNT_STU_COMMENT "comment"

#define TNT_STU_IDX_PRIMARY "primary_idx_stu_sno"

#define TNT_TEST_LIMIT_COUNT 100000

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

class OnlineIdxThread: public Thread {
public:
	OnlineIdxThread(TNTDatabase *db): Thread("OnlineIdxThread") {
		m_db = db;
	}

	void run() {
		Connection *conn = m_db->getNtseDb()->getConnection(false);
		conn->setTrx(true);
		try {
			std::string sql = std::string("add index on ") + TNT_STU_SCHEMA_NAME + "." + TNT_STU_TBL_NAME 
				+ " " + TNT_STU_IDX_PRIMARY + "(" + TNT_STU_SNO + ")";
			Parser *parser = new Parser(sql.c_str());
			assert(System::stricmp(parser->nextToken(), "add") == 0);
			IdxPreAlter idxPreAlter(m_db, conn, parser);
			idxPreAlter.createOnlineIndex();
		} catch (NtseException &e) {
			printf("create online index error: %s\n", e.getMessage());
		}
		m_db->getNtseDb()->freeConnection(conn);
	}

private:
	TNTDatabase   *m_db;
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

const char* TNTOnlineDDLTestCase::getName() {
	return "TNTDatabase Online DDL test";
}

const char* TNTOnlineDDLTestCase::getDescription() {
	return "Test TNTDatabase Online DDL(create online index and online optimize)";
}

bool TNTOnlineDDLTestCase::isBig() {
	return false;
}

void TNTOnlineDDLTestCase::setUp() {
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
}

void TNTOnlineDDLTestCase::tearDown() {
	if (m_db != NULL) {
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

void TNTOnlineDDLTestCase::openTable(bool idx) {
	assert(m_db != NULL && m_session != NULL && m_conn != NULL);

	string path("testdb");
	path.append(NTSE_PATH_SEP).append(TNT_STU_SCHEMA_NAME);
	File dbFile(path.c_str());
	dbFile.mkdir();

	TableDef *tableDef = createStudentTableDef(idx);

	string tablePath("./");
	tablePath.append(TNT_STU_SCHEMA_NAME).append(NTSE_PATH_SEP).append(TNT_STU_TBL_NAME);

	TNTTrxSys *trxSys = m_db->getTransSys();
	//该记录在tnt hash索引中不存在，认为在ntse中存在
	TNTTransaction *trx = startTrx(trxSys, m_conn);
	m_session->setTrans(trx);
	m_db->createTable(m_session, tablePath.c_str(), tableDef);
	m_table = m_db->openTable(m_session, tablePath.c_str());
	CPPUNIT_ASSERT(m_table != NULL);
	commitTrx(trxSys, trx);

	delete tableDef;
}

void TNTOnlineDDLTestCase::closeTable() {
	if (m_table != NULL) {
		m_db->closeTable(m_session, m_table);
		m_table = NULL;
	}
}

TableDef *TNTOnlineDDLTestCase::createStudentTableDef(bool idx) {
	TableDefBuilder tb(TableDef::INVALID_TABLEID, TNT_STU_SCHEMA_NAME, TNT_STU_TBL_NAME);
	tb.addColumn(TNT_STU_SNO, CT_INT);
	tb.addColumnS(TNT_STU_NAME, CT_VARCHAR, 30, false, false, COLL_LATIN1);
	tb.addColumn(TNT_STU_AGE, CT_SMALLINT);
	tb.addColumn(TNT_STU_CLASS, CT_MEDIUMINT);
	tb.addColumn(TNT_STU_GPA, CT_FLOAT);
	tb.addColumnS(TNT_STU_COMMENT, CT_VARCHAR, 200, false, true, COLL_LATIN1);
	if (idx) {
		tb.addIndex(TNT_STU_IDX_PRIMARY, false, false, false, TNT_STU_SNO, 0, NULL);
	}
	TableDef *tblDef = tb.getTableDef();
	tblDef->setTableStatus(TS_TRX);
	return tblDef;
}

SubRecord *TNTOnlineDDLTestCase::createSubRecord(TableDef *tableDef, RecFormat format, const std::vector<u16> &columns, const std::vector<void *>&datas) {
	SubRecord *subRecord = NULL;
	SubRecordBuilder sb(tableDef, format);
	subRecord = sb.createSubRecord(columns, datas);
	return subRecord;
}

TNTTransaction *TNTOnlineDDLTestCase::startTrx(TNTTrxSys *trxSys, Connection *conn, TLockMode lockMode/* = TL_NO*/,  bool log/* = false*/) {
	TNTTransaction *trx = trxSys->allocTrx();
	if (!log) {
		trx->disableLogging();
	}
	trx->startTrxIfNotStarted(conn);
	return trx;
}

void TNTOnlineDDLTestCase::commitTrx(TNTTrxSys *trxSys, TNTTransaction *trx) {
	trx->commitTrx(CS_NORMAL);
	trxSys->freeTrx(trx);
}

Record* TNTOnlineDDLTestCase::createStudentRec(u32 stuNo, const char *name, u16 age, u32 clsNo, float gpa) {
	RecordBuilder rb(m_table->getNtseTable()->getTableDef(), INVALID_ROW_ID, REC_REDUNDANT);
	rb.appendInt(stuNo);
	rb.appendVarchar(name);
	rb.appendSmallInt(age);
	rb.appendMediumInt(clsNo);
	rb.appendFloat(gpa);
	rb.appendNull();

	return rb.getRecord();
}

void TNTOnlineDDLTestCase::testOnlineOptimize() {
	openTable(true);
	u32 count = 1000;
	assert(count < TNT_TEST_LIMIT_COUNT);
	RecordVal *recs = new RecordVal[count];
	insert(recs, count, true);
	updateAll(recs, count,  1, true, true);
	m_db->purgeAndDumpTntim(PT_PURGEPHASE2);
	OptimizeThread onlineOptimizeThr(m_db, m_db->getNtseDb()->getBgCustomThreadManager(), m_table, false, false, m_conn->isTrx());
	onlineOptimizeThr.enableSyncPoint(SP_TBL_ALTCOL_AFTER_GET_LSNSTART);
	onlineOptimizeThr.start();
	onlineOptimizeThr.joinSyncPoint(SP_TBL_ALTCOL_AFTER_GET_LSNSTART);
	updateAll(recs, count, 5, true, true);
	remove(recs, 0, count/5, true, true);
	m_db->purgeAndDumpTntim(PT_PURGEPHASE2);
	update(recs, count/4, 3*count/4, 7, true, true);
	remove(recs, count/4, count/2, true, true);
	onlineOptimizeThr.disableSyncPoint(SP_TBL_ALTCOL_AFTER_GET_LSNSTART);
	onlineOptimizeThr.notifySyncPoint(SP_TBL_ALTCOL_AFTER_GET_LSNSTART);

	m_db->purgeAndDumpTntim(PT_PURGEPHASE2);
	onlineOptimizeThr.join(-1);
	compareAllRecordByIndexScan(m_session, m_table, recs, count);
	closeTable();

	releaseAllRecordVal(recs, count);
}

void TNTOnlineDDLTestCase::testCreateOnlineIndex() {
	openTable(false);
	u32 count = 1000;
	assert(count < TNT_TEST_LIMIT_COUNT);
	RecordVal *recs = new RecordVal[count];
	insert(recs, count, true);
	updateAll(recs, count,  1, true, true);
	m_db->purgeAndDumpTntim(PT_PURGEPHASE2);
	OnlineIdxThread onlineIdxThr(m_db);
	onlineIdxThr.enableSyncPoint(SP_TBL_ALTIDX_BEFORE_GET_LSNEND);
	onlineIdxThr.start();
	onlineIdxThr.joinSyncPoint(SP_TBL_ALTIDX_BEFORE_GET_LSNEND);
	updateAll(recs, count, 5, true, true);
	remove(recs, 0, count/5, true, true);
	m_db->purgeAndDumpTntim(PT_PURGEPHASE2);
	update(recs, count/4, 3*count/4, 7, true, true);
	remove(recs, count/4, count/2, true, true);
	onlineIdxThr.disableSyncPoint(SP_TBL_ALTIDX_BEFORE_GET_LSNEND);
	onlineIdxThr.notifySyncPoint(SP_TBL_ALTIDX_BEFORE_GET_LSNEND);

	onlineIdxThr.join(-1);
	compareAllRecordByIndexScan(m_session, m_table, recs, count);
	closeTable();

	releaseAllRecordVal(recs, count);
}

void TNTOnlineDDLTestCase::testOnlineOptimizeUpdate() {
	openTable(true);
	u32 count = 1000;
	assert(count < TNT_TEST_LIMIT_COUNT);
	RecordVal *recs = new RecordVal[count];
	insert(recs, count, true);
	updateAll(recs, count,  1, true, true);
	m_db->purgeAndDumpTntim(PT_PURGEPHASE2);
	OptimizeThread onlineOptimizeThr(m_db, m_db->getNtseDb()->getBgCustomThreadManager(), m_table, false, false, m_conn->isTrx());
	onlineOptimizeThr.enableSyncPoint(SP_TBL_ALTCOL_BEFORE_GET_LSNSTART);
	onlineOptimizeThr.start();
	onlineOptimizeThr.joinSyncPoint(SP_TBL_ALTCOL_BEFORE_GET_LSNSTART);
	updateAll(recs, count, 5, true, true);
	
	PurgeThread purgeThr(m_db);
	purgeThr.enableSyncPoint(SP_TBL_UPDATE_AFTER_STARTTXN_LOG);
	purgeThr.start();
	purgeThr.joinSyncPoint(SP_TBL_UPDATE_AFTER_STARTTXN_LOG);

	onlineOptimizeThr.enableSyncPoint(SP_MNT_ALTERCOLUMN_BEFORE_S_LOCKTABLE);

	onlineOptimizeThr.disableSyncPoint(SP_TBL_ALTCOL_BEFORE_GET_LSNSTART);
	onlineOptimizeThr.notifySyncPoint(SP_TBL_ALTCOL_BEFORE_GET_LSNSTART);

	onlineOptimizeThr.joinSyncPoint(SP_MNT_ALTERCOLUMN_BEFORE_S_LOCKTABLE);
	purgeThr.disableSyncPoint(SP_TBL_UPDATE_AFTER_STARTTXN_LOG);
	purgeThr.notifySyncPoint(SP_TBL_UPDATE_AFTER_STARTTXN_LOG);
	purgeThr.join(-1);

	onlineOptimizeThr.disableSyncPoint(SP_MNT_ALTERCOLUMN_BEFORE_S_LOCKTABLE);
	onlineOptimizeThr.notifySyncPoint(SP_MNT_ALTERCOLUMN_BEFORE_S_LOCKTABLE);
	onlineOptimizeThr.join(-1);
	compareAllRecordByIndexScan(m_session, m_table, recs, count);
	closeTable();

	releaseAllRecordVal(recs, count);
}

void TNTOnlineDDLTestCase::testCreateOnlineIndexUpdate() {
	openTable(false);
	u32 count = 1000;
	assert(count < TNT_TEST_LIMIT_COUNT);
	RecordVal *recs = new RecordVal[count];
	insert(recs, count, true);
	updateAll(recs, count,  1, true, true);
	m_db->purgeAndDumpTntim(PT_PURGEPHASE2);
	OnlineIdxThread onlineIdxThr(m_db);
	onlineIdxThr.enableSyncPoint(SP_TBL_ALTIDX_BEFORE_GET_LSNSTART);
	onlineIdxThr.start();
	onlineIdxThr.joinSyncPoint(SP_TBL_ALTIDX_BEFORE_GET_LSNSTART);
	updateAll(recs, count, 5, true, true);
	remove(recs, 0, count/5, true, true);
	PurgeThread purgeThr(m_db);
	purgeThr.enableSyncPoint(SP_TBL_UPDATE_AFTER_STARTTXN_LOG);
	purgeThr.start();
	purgeThr.joinSyncPoint(SP_TBL_UPDATE_AFTER_STARTTXN_LOG);

	onlineIdxThr.enableSyncPoint(SP_MNT_ALTERINDICE_BEFORE_UPD_METALOCK);
	onlineIdxThr.disableSyncPoint(SP_TBL_ALTIDX_BEFORE_GET_LSNSTART);
	onlineIdxThr.notifySyncPoint(SP_TBL_ALTIDX_BEFORE_GET_LSNSTART);

	onlineIdxThr.joinSyncPoint(SP_MNT_ALTERINDICE_BEFORE_UPD_METALOCK);
	purgeThr.disableSyncPoint(SP_TBL_UPDATE_AFTER_STARTTXN_LOG);
	purgeThr.notifySyncPoint(SP_TBL_UPDATE_AFTER_STARTTXN_LOG);
	purgeThr.join(-1);

	onlineIdxThr.disableSyncPoint(SP_MNT_ALTERINDICE_BEFORE_UPD_METALOCK);
	onlineIdxThr.notifySyncPoint(SP_MNT_ALTERINDICE_BEFORE_UPD_METALOCK);
	onlineIdxThr.join(-1);
	compareAllRecordByIndexScan(m_session, m_table, recs, count);
	closeTable();

	releaseAllRecordVal(recs, count);
}

void TNTOnlineDDLTestCase::releaseAllRecordVal(RecordVal *recs, u32 count) {
	for (u32 i = 0; i < count; i++) {
		delete[] recs[i].m_comment;
		delete[] recs[i].m_name;
		delete[] recs[i].m_redRec->m_data;
		delete recs[i].m_redRec;
	}
	delete[] recs;
}

SubRecord *TNTOnlineDDLTestCase::createKey(TableDef *tblDef, int idxNo, Record *rec) {
	int i = 0;
	char ids[200];
	memset(ids, 0, sizeof(ids));
	SubRecord *key = NULL;
	IndexDef *idxDef = tblDef->getIndexDef(idxNo);
	for (i = 0; i < idxDef->m_numCols; i++) {
		size_t pos = strlen(ids);
		System::snprintf_mine(ids + pos, sizeof(ids) - pos, "%d ", idxDef->m_columns[i]);
	}

	SubRecordBuilder sb(tblDef, KEY_PAD);
	key = sb.createEmptySbById(tblDef->m_maxRecSize, ids);

	RecordOper::extractKeyRP(tblDef, tblDef->m_indice[idxNo], rec, NULL, key);
	return key;
}

void TNTOnlineDDLTestCase::compareAllRecordByIndexScan(Session *session, TNTTable *table, RecordVal *recs, u32 count) {
	TNTTrxSys *trxSys = m_db->getTransSys();
	TNTTransaction *trx = startTrx(trxSys, m_conn, TL_S);
	session->setTrans(trx);
	for (u32 i = 0; i < count; i++) {
		compareRecordByIndexScan(session, table, recs + i);
	}
	commitTrx(trxSys, trx);
}

void TNTOnlineDDLTestCase::compareRecordByIndexScan(Session *session, TNTTable *table, RecordVal *recVal) {
	TableDef *tblDef = table->getNtseTable()->getTableDef();
	int idxNo = tblDef->getIndexNo(TNT_STU_IDX_PRIMARY);
	if (idxNo == -1) {
		printf("error no index defination\n");
		assert(false);
	}
	TNTTblScan *scan = NULL;
	TNTOpInfo opInfo;
	initTNTOpInfo(opInfo, TL_S);
	SubRecord *key = createKey(tblDef, idxNo, recVal->m_redRec);
	IndexScanCond idxScanCond(idxNo, key, true, true, false);

	TableDef *tableDef = table->getNtseTable()->getTableDef();
	MemoryContext *ctx = session->getMemoryContext();
	McSavepoint msp(ctx);
	u16 *readCols = (u16 *)ctx->alloc(sizeof(u16) * tableDef->m_numCols);
	for (u16 i = 0; i < tableDef->m_numCols; ++i) {
		readCols[i] = i;
	}

	byte *mysqlRow = (byte *)ctx->alloc(tableDef->m_maxRecSize);
	scan = table->indexScan(session, OP_READ, &opInfo, &idxScanCond, tableDef->m_numCols, readCols, false);
	Record rec(INVALID_ROW_ID, REC_MYSQL, mysqlRow, tableDef->m_maxRecSize);
	CPPUNIT_ASSERT(table->getNext(scan, mysqlRow));
	size_t size;
	u16 cno = 0;
	RedRecord redRec(tableDef, &rec);
	if (recVal->m_del) {
		CPPUNIT_ASSERT(redRec.readInt(cno++) != recVal->m_stuNo);
	} else {
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

	delete[] key->m_columns;
	delete[] key->m_data;
	delete key;
}

void TNTOnlineDDLTestCase::insert(RecordVal *recs, const u32 count, bool log) {
	TNTTrxSys *trxSys = m_db->getTransSys();
	McSavepoint msp(m_session->getMemoryContext());
	TNTTransaction *trx = startTrx(trxSys, m_conn, TL_X, log);
	m_session->setTrans(trx);
	insert(m_session, recs, count);
	commitTrx(trxSys, trx);
}

void TNTOnlineDDLTestCase::insert(Session *session, RecordVal *recs, const u32 count) {
	assert(session->getTrans() != NULL);
	m_table->lockMeta(session, IL_S, -1, __FILE__, __LINE__);
	TNTOpInfo opInfo;
	initTNTOpInfo(opInfo, TL_X);
	uint dupIndex = 0;
	u32 i = 0;
	McSavepoint msp(session->getMemoryContext());
	for (i = 0; i < count; i++) {
		McSavepoint msp1(session->getMemoryContext());
		recs[i].m_stuNo = 1 + i;
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
	m_table->unlockMeta(session, IL_S);
}

void TNTOnlineDDLTestCase::updateAll(RecordVal *recs, const u32 count, u8 factor, bool save, bool log) {
	update(recs, 0, count - 1, factor, save, log);
}

void TNTOnlineDDLTestCase::update(RecordVal *recs, const u32 begin, const u32 end, u8 factor, bool save, bool log) {
	MemoryContext *ctx = m_session->getMemoryContext();
	McSavepoint msp(ctx);

	TNTTrxSys *trxSys = m_db->getTransSys();
	TNTTransaction *trx = startTrx(trxSys, m_conn, TL_X, log);
	m_session->setTrans(trx);
	update(m_session, recs, begin, end, factor, save);
	commitTrx(trxSys, trx);
}

void TNTOnlineDDLTestCase::update(Session *session, RecordVal *recs, const u32 begin, const u32 end, u8 factor, bool save) {
	update(session, m_table, recs, begin, end, factor, save);
}

void TNTOnlineDDLTestCase::update(Session *session, TNTTable *table, RecordVal *recs, const u32 begin, const u32 end, u8 factor, bool save) {
	assert(session->getTrans() != NULL);
	table->lockMeta(session, IL_S, -1, __FILE__, __LINE__);
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
	columns.push_back(0);
	columns.push_back(2);
	columns.push_back(3);
	columns.push_back(5);

	for (i = begin; i <= end; i++) {
		if (save) {
			rec = recs + i;
		}
		McSavepoint msp1(ctx);
		datas.clear();
		rec->m_stuNo = recs[i].m_stuNo + TNT_TEST_LIMIT_COUNT;
		rec->m_age = recs[i].m_age + factor % 10;
		rec->m_clsNo = recs[i].m_clsNo + (factor*10) % 8;
		memset(rec->m_comment, 0, 200);
		memset(rec->m_comment, chr , (factor * 10) % 200 + 1);

		datas.push_back(&rec->m_stuNo);
		datas.push_back(&rec->m_age);
		datas.push_back(&rec->m_clsNo);
		datas.push_back(rec->m_comment);
		updateSub = createSubRecord(tableDef, REC_REDUNDANT, columns, datas);
		scan = table->positionScan(session, OP_UPDATE, &opInfo, tableDef->m_numCols, readCols);
		scan->setUpdateSubRecord(updateSub->m_numCols, updateSub->m_columns);
		CPPUNIT_ASSERT(table->getNext(scan, mysqlRow, recs[i].m_rid));
		CPPUNIT_ASSERT(table->updateCurrent(scan, updateSub->m_data));
		RecordOper::updateRecordRR(tableDef, rec->m_redRec, updateSub);
		table->endScan(scan);

		delete[] updateSub->m_columns;
		delete[] updateSub->m_data;
		delete updateSub;
	}
	table->unlockMeta(session, IL_S);
}

void TNTOnlineDDLTestCase::remove(RecordVal *recs, const u32 begin, const u32 end, bool save, bool log) {
	TNTTrxSys *trxSys = m_db->getTransSys();
	TNTTransaction *trx = startTrx(trxSys, m_conn, TL_X, log);
	m_session->setTrans(trx);
	remove(m_session, recs, begin, end, save);
	commitTrx(trxSys, trx);
}

void TNTOnlineDDLTestCase::remove(Session *session, RecordVal *recs, const u32 begin, const u32 end, bool save) {
	remove(session, m_table, recs, begin, end, save);
}

void TNTOnlineDDLTestCase::remove(Session *session, TNTTable *table, RecordVal *recs, const u32 begin, const u32 end, bool save) {
	assert(session->getTrans() != NULL);
	table->lockMeta(session, IL_S, -1, __FILE__, __LINE__);
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
		table->deleteCurrent(scan);
		table->endScan(scan);
		if (save) {
			recs[i].m_del = true;
		}
	}
	table->unlockMeta(session, IL_S);
}