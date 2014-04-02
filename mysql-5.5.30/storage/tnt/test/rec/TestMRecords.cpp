#include "TestMRecords.h"
#include "util/File.h"
#include "misc/RecordHelper.h"
#include "Test.h"

#define TNT_STU_NAME "name"
#define TNT_STU_SNO "sno"
#define TNT_STU_AGE "age"
#define TNT_STU_SEX "sex"
#define TNT_STU_CLASS "class"
#define TNT_STU_GPA "gpa"
#define TNT_STU_GRADE "grade"

GetRecordThread::GetRecordThread(TNTDatabase *db, TableDef *tableDef, TNTTable *table): Thread("GetRecordThread") {
	m_db = db;
	m_tableDef = tableDef;
	m_table = table;
}

UpdateDefragThread::UpdateDefragThread(TNTDatabase *db, TableDef *tableDef, TNTTable *table, RecAndFieldVal *rec): Thread("UpdateDefragThread") {
	m_db = db;
	m_tableDef = tableDef;
	m_table = table;
	m_rec = rec;
}

RemoveDefragThread::RemoveDefragThread(TNTDatabase *db, TableDef *tableDef, TNTTable *table, RecAndFieldVal *rec): Thread("RemoveDefragThread") {
	m_db = db;
	m_tableDef = tableDef;
	m_table = table;
	m_rec = rec;
}

const char* MRecordsTestCase::getName() {
	return "TNT records test";
}

const char* MRecordsTestCase::getDescription() {
	return "Test various operations of tnt records which contain tnt heap, hash index, transaction, version pool";
}

bool MRecordsTestCase::isBig() {
	return false;
}

void MRecordsTestCase::setUp() {
	File dir("testdb");
	dir.rmdir(true);
	dir.mkdir();
	m_config.setNtseBasedir("testdb");
	m_config.setTntBasedir("testdb");
	m_config.setTntDumpdir("testdb");
	m_config.m_tntLogLevel = EL_WARN;
	m_config.m_tntBufSize = 200;
	//m_config.m_tntLogfileSize = 1024 * 1024 * 128;
	m_config.m_ntseConfig.m_logFileSize = 128 << 20;
	m_config.m_verpoolCnt = 2;

	EXCPT_OPER(m_db = TNTDatabase::open(&m_config, true, 0));

	Database *db = m_db->getNtseDb();
	m_conn = db->getConnection(false);
	m_conn->setTrx(true);
	m_session = db->getSessionManager()->allocSession("MRecordsTestCase", m_conn);
	assert(m_db != NULL && m_session != NULL && m_conn != NULL);
}

void MRecordsTestCase::tearDown() {
	Database *db = m_db->getNtseDb();
	db->getSessionManager()->freeSession(m_session);
	db->freeConnection(m_conn);
	m_db->close(false, false);

	delete m_db;
	m_db = NULL;

	delete m_tableDef;
	m_tableDef = NULL;

	File dir("testdb");
	dir.rmdir(true);
}

void MRecordsTestCase::initTable(bool fix) {
	TNTTrxSys *trxSys = m_db->getTransSys();
	//该记录在tnt hash索引中不存在，认为在ntse中存在
	TNTTransaction *trx = startTrx(trxSys, m_conn, 10);
	m_session->setTrans(trx);

	m_tableDef = createStudentTableDef(fix);

	m_db->createTable(m_session, m_tableDef->m_name, m_tableDef);
	m_table = m_db->openTable(m_session, m_tableDef->m_name);
	CPPUNIT_ASSERT(m_table != NULL);
	m_records = m_table->getMRecords();
	commitTrx(trxSys, trx);
}

void MRecordsTestCase::shutDownTable() {
	m_db->closeTable(m_session, m_table);
}

void MRecordsTestCase::confirmRedRecord(TableDef *tableDef, Record *rec, const char *name, u32 stuNo, u16 age, const char *sex, u32 clsNo, u64 grade) {
	NTSE_ASSERT(rec->m_format == REC_REDUNDANT);
	RedRecord redRec(tableDef, rec);
	u16 index = 0;
	size_t size = 0;
	char *pName = NULL;
	if (tableDef->m_recFormat == REC_FIXLEN) {
		redRec.readChar(index++, &pName, &size);
		CPPUNIT_ASSERT(size == 30);
		size = strlen(name);
		CPPUNIT_ASSERT(memcmp(name, pName, size) == 0);
	} else {
		redRec.readVarchar(index++, &pName, &size);
		CPPUNIT_ASSERT(strlen(name) == size);
		CPPUNIT_ASSERT(memcmp(name, pName, size) == 0);
	}

	CPPUNIT_ASSERT(redRec.readInt(index++) == stuNo);
	CPPUNIT_ASSERT(redRec.readSmallInt(index++) == age);

	char *pSex = NULL;
	redRec.readChar(index++, &pSex, &size);
	CPPUNIT_ASSERT(strlen(sex) == size);
	CPPUNIT_ASSERT(memcmp(pSex, sex, size) == 0);

	CPPUNIT_ASSERT(redRec.readMediumInt(index++) == clsNo);
	index++; //gpa为null
	CPPUNIT_ASSERT(redRec.readBigInt(index++) == grade);
}

SubRecord *MRecordsTestCase::createRedSubRecord(TableDef *tableDef, const std::vector<u16> &columns, const std::vector<void *>&datas) {
	SubRecord *subRecord = NULL;
	SubRecordBuilder sb(tableDef, REC_REDUNDANT);
	subRecord = sb.createSubRecord(columns, datas);
	return subRecord;
}

void MRecordsTestCase::freeSubRecord(SubRecord *sub) {
	delete[] sub->m_data;
	delete[] sub->m_columns;
	delete sub;
}

void MRecordsTestCase::deepCopyRecord(Record *dest, Record *src) {
	dest->m_rowId = src->m_rowId;
	dest->m_format = src->m_format;
	dest->m_size = src->m_size;
	memcpy(dest->m_data, src->m_data, src->m_size);
}

TNTTransaction *MRecordsTestCase::startTrx(TNTTrxSys *trxSys, Connection *conn, TrxId trxId, TLockMode lockMode/*= TL_NO*/, bool log/*= false*/, TrxId* activeTrxIds/*= NULL*/, u16 activeCnt/*= 0*/) {
	TNTTransaction *trx = trxSys->allocTrx(trxId);
	if (!log) {
		trx->disableLogging();
	}
	trx->startTrxIfNotStarted(conn);
	if (lockMode != TL_NO) {
		return trx;
	}

	trxSys->setMaxTrxId(trx->getTrxId() + 1);
	if (activeCnt == 0) {
		trx->trxAssignReadView();
	} else {
		trx->trxAssignReadView(activeTrxIds, activeCnt);
	}
	return trx;
}

void MRecordsTestCase::commitTrx(TNTTrxSys *trxSys, TNTTransaction *trx) {
	trx->commitTrx(CS_NORMAL);
	trxSys->freeTrx(trx);
}

void MRecordsTestCase::reOpenAllTable() {
	m_db->closeOpenTables();
	m_db->closeTNTTableBases();

	m_table = m_db->openTable(m_session, m_tableDef->m_name);
	CPPUNIT_ASSERT(m_table != NULL);
	m_records = m_table->getMRecords();
}

TableDef *MRecordsTestCase::createStudentTableDef(bool fix) {
	TableDefBuilder tb(TableDef::INVALID_TABLEID, "Olympic", "student");
	if (fix) {
		tb.addColumnS(TNT_STU_NAME, CT_CHAR, 30, false, false, COLL_LATIN1);
	} else {
		tb.addColumnS(TNT_STU_NAME, CT_VARCHAR, 30, false, false, COLL_LATIN1);
	}
	tb.addColumn(TNT_STU_SNO, CT_INT);
	tb.addColumn(TNT_STU_AGE, CT_SMALLINT);
	tb.addColumnS(TNT_STU_SEX, CT_CHAR, 1, false, true, COLL_LATIN1);
	tb.addColumn(TNT_STU_CLASS, CT_MEDIUMINT);
	tb.addColumn(TNT_STU_GPA, CT_FLOAT);
	PrType prtype;
	prtype.setUnsigned();
	tb.addColumnN(TNT_STU_GRADE, CT_BIGINT, prtype);
	TableDef *tblDef = tb.getTableDef();
	tblDef->setTableStatus(TS_TRX);
	return tblDef;
}

Record* MRecordsTestCase::createStudentRec(RowId rid, const char *name, u32 stuNo, u16 age, const char *sex, u32 clsNo, u64 grade) {
	RecordBuilder rb(m_tableDef, rid, REC_REDUNDANT);
	if (isFixTable()) {
		rb.appendChar(name);
	} else {
		rb.appendVarchar(name);
	}
	rb.appendInt(stuNo);
	rb.appendSmallInt(age);
	rb.appendChar(sex);
	rb.appendMediumInt(clsNo);
	rb.appendNull();
	rb.appendBigInt(grade);
	
	return rb.getRecord();
}

LogScanHandle *MRecordsTestCase::getNextTNTLog(Txnlog *txnLog, LogScanHandle *logHdl) {
	LogScanHandle *ret = NULL;
	while (NULL != (ret = txnLog->getNext(logHdl))) {
		if (!logHdl->logEntry()->isTNTLog()) {
			continue;
		} else {
			break;
		}
	}
	return ret;
}

void MRecordsTestCase::testFixLenInsert() {
	initTable(true);
	testInsert();
	shutDownTable();
}

void MRecordsTestCase::testVarLenInsert() {
	initTable(false);
	testInsert();
	shutDownTable();
}

void MRecordsTestCase::testInsert() {
	TNTOpInfo scanOpInfo;
	m_records->autoIncrementVersion();
	u16 version = m_records->getVersion();
	u64 txnId1 = 10005;
	RowId rid1 = 50005;
	CPPUNIT_ASSERT(m_records->insert(m_session, txnId1, rid1));
	bool ntseVisible = true;
	bool tntVisible = true;
	TNTTblScan *scan = NULL;
	TNTTrxSys *trxSys = m_db->getTransSys();

	u16 *columns = (u16 *)m_session->getMemoryContext()->alloc(sizeof(u16) * m_tableDef->m_numCols);
	for (u16 i = 0; i < m_tableDef->m_numCols; ++i) {
		columns[i] = i;
	}

	//事务号1011，
	//该记录在tnt hash索引中不存在，认为在ntse中存在
	TNTTransaction *trx1 = startTrx(trxSys, m_conn, 10011);
	m_session->setTrans(trx1);

	initTNTOpInfo(scanOpInfo, TL_NO);
	scan = m_table->positionScan(m_session, OP_READ, &scanOpInfo, m_tableDef->m_numCols, columns);
	scan->setCurrentRid(50003);
	tntVisible = m_records->getRecord(scan, version, &ntseVisible);
	CPPUNIT_ASSERT(tntVisible == false);
	CPPUNIT_ASSERT(ntseVisible == true);
	m_table->endScan(scan);

	//该记录在tnt hash索引中存在，但version过期，认为已经被刷入外存
	initTNTOpInfo(scanOpInfo, TL_NO);
	scan = m_table->positionScan(m_session, OP_READ, &scanOpInfo, m_tableDef->m_numCols, columns);
	scan->setCurrentRid(rid1);
	tntVisible = m_records->getRecord(scan, version - 1, &ntseVisible);
	CPPUNIT_ASSERT(tntVisible == false);
	CPPUNIT_ASSERT(ntseVisible == true);
	m_table->endScan(scan);
	commitTrx(trxSys, trx1);

	//事务号10004，ntse不可见
	TNTTransaction *trx2 = startTrx(trxSys, m_conn, 10004);
	m_session->setTrans(trx2);
	initTNTOpInfo(scanOpInfo, TL_NO);
	scan = m_table->positionScan(m_session, OP_READ, &scanOpInfo, m_tableDef->m_numCols, columns);
	scan->setCurrentRid(rid1);
	tntVisible = m_records->getRecord(scan, version, &ntseVisible);
	CPPUNIT_ASSERT(tntVisible == false);
	CPPUNIT_ASSERT(ntseVisible == false);
	m_table->endScan(scan);
	commitTrx(trxSys, trx2);

	//事务号10005，当前事务可见
	TNTTransaction *trx3 = startTrx(trxSys, m_conn, 10005);
	m_session->setTrans(trx3);
	initTNTOpInfo(scanOpInfo, TL_NO);
	scan = m_table->positionScan(m_session, OP_READ, &scanOpInfo, m_tableDef->m_numCols, columns);
	scan->setCurrentRid(rid1);
	tntVisible = m_records->getRecord(scan, version, &ntseVisible);
	CPPUNIT_ASSERT(tntVisible == false);
	CPPUNIT_ASSERT(ntseVisible == true);
	m_table->endScan(scan);
	commitTrx(trxSys, trx3);

	//事务号10006，ntse不可见
	u64 trxIds4[] = {10005, 10004};
	TNTTransaction *trx4 = startTrx(trxSys, m_conn, 10006, TL_NO, false, trxIds4, 2);
	m_session->setTrans(trx4);
	initTNTOpInfo(scanOpInfo, TL_NO);
	scan = m_table->positionScan(m_session, OP_READ, &scanOpInfo, m_tableDef->m_numCols, columns);
	scan->setCurrentRid(rid1);
	tntVisible = m_records->getRecord(scan, version, &ntseVisible);
	CPPUNIT_ASSERT(tntVisible == false);
	CPPUNIT_ASSERT(ntseVisible == false);
	m_table->endScan(scan);
	commitTrx(trxSys, trx4);

	//事务号10007，ntse可见
	u64 trxIds5[] = {10004};
	TNTTransaction *trx5 = startTrx(trxSys, m_conn, 10007, TL_NO, false, trxIds5, 1);
	m_session->setTrans(trx5);
	initTNTOpInfo(scanOpInfo, TL_NO);
	scan = m_table->positionScan(m_session, OP_READ, &scanOpInfo, m_tableDef->m_numCols, columns);
	scan->setCurrentRid(rid1);
	tntVisible = m_records->getRecord(scan, version, &ntseVisible);
	CPPUNIT_ASSERT(tntVisible == false);
	CPPUNIT_ASSERT(ntseVisible == true);
	m_table->endScan(scan);
	commitTrx(trxSys, trx5);
}

void GetRecordThread::run() {
	TNTOpInfo scanOpInfo;
	TNTTblScan *scan = NULL;
	bool ntseVisible = true;
	bool tntVisible = true;
	u16 version = m_table->getMRecords()->getVersion();
	Database *db = m_db->getNtseDb();
	Connection *conn = db->getConnection(false);
	Session *session = db->getSessionManager()->allocSession("GetRecordThread::run", conn);
	TNTTrxSys *trxSys = m_db->getTransSys();

	MemoryContext *ctx = session->getMemoryContext();
	McSavepoint msp(ctx);
	u16 *readCols = (u16 *)ctx->alloc(sizeof(u16) * m_tableDef->m_numCols);
	for (u16 i = 0; i < m_tableDef->m_numCols; ++i) {
		readCols[i] = i;
	}

	char *name3 = "netease123";
	u32 stuNo2 = 5002;
	u16 age3 = 29;
	const char *sex1 = "F";
	u32 clsNo3 = 5003;
	u64 grade1 = 8001;
	RowId rid1 = 10008;

	u64 txnId3 = 30005;
	//第三条tnt记录可见
	TNTTransaction *trx5 = MRecordsTestCase::startTrx(trxSys, conn, txnId3 + 10);
	session->setTrans(trx5);
	MRecordsTestCase::initTNTOpInfo(scanOpInfo, TL_NO);
	scan = m_table->positionScan(session, OP_READ, &scanOpInfo, m_tableDef->m_numCols, readCols);
	scan->setCurrentRid(rid1);
	tntVisible = m_table->getMRecords()->getRecord(scan, version, &ntseVisible);
	CPPUNIT_ASSERT(tntVisible == true);
	Record *redRec3 = scan->getRecord();
	MRecordsTestCase::confirmRedRecord(m_tableDef, redRec3, name3, stuNo2, age3, sex1, clsNo3, grade1);
	m_table->endScan(scan);
	MRecordsTestCase::commitTrx(trxSys, trx5);

	db->getSessionManager()->freeSession(session);
	db->freeConnection(conn);
}

void MRecordsTestCase::testFixLenUpdate() {
	initTable(true);
	testUpdate();
	shutDownTable();
}

void MRecordsTestCase::testVarLenUpdate() {
	initTable(false);
	testUpdate();
	shutDownTable();
}

void MRecordsTestCase::testUpdate() {
	TNTOpInfo scanOpInfo;
	TNTTblScan *scan = NULL;
	TNTTrxSys *trxSys = m_db->getTransSys();

	bool ntseVisible = true;
	bool tntVisible = true;

	u8 vTblIndex = 0;
	u16 version = m_records->getVersion();
	MemoryContext *ctx = m_session->getMemoryContext();
	u64 sp = ctx->setSavepoint();
	u16 *readCols = (u16 *)ctx->alloc(sizeof(u16) * m_tableDef->m_numCols);
	for (u16 i = 0; i < m_tableDef->m_numCols; ++i) {
		readCols[i] = i;
	}

	u64 txnId1 = 10005;
	const char *name1 = "netease1";
	u32 stuNo1 = 5001;
	u16 age1 = 25;
	const char *sex1 = "F";
	u32 clsNo1 = 3001;
	u64 grade1 = 8001;
	RowId rid1 = 10008;
	Record *redRec1 = createStudentRec(rid1, name1, stuNo1, age1, sex1, clsNo1, grade1);
	SubRecord allRedSub1(redRec1->m_format, m_tableDef->m_numCols, readCols, redRec1->m_data, redRec1->m_size, redRec1->m_rowId);
	CPPUNIT_ASSERT(m_records->insert(m_session, txnId1, rid1));

	RowId rid2 = 20008;
	CPPUNIT_ASSERT(m_records->insert(m_session, txnId1, rid2));

	std::vector<u16> columns;
	std::vector<void *> datas;

	//第一次更新
	u64 txnId2 = 20005;
	u32 stuNo2 = 5002;
	u16 age2 = 28;
	columns.push_back(1);
	datas.push_back(&stuNo2);
	columns.push_back(2);
	datas.push_back(&age2);
	SubRecord *updateSub1 = createRedSubRecord(m_tableDef, columns, datas);
	RecordOper::mergeSubRecordRR(m_tableDef, updateSub1, &allRedSub1);

	TNTTransaction *updateTrx1 = startTrx(trxSys, m_conn, txnId2, TL_X);
	m_session->setTrans(updateTrx1);
	initTNTOpInfo(scanOpInfo, TL_X);
	scan = m_table->positionScan(m_session, OP_UPDATE, &scanOpInfo, m_tableDef->m_numCols, readCols);
	scan->setCurrentRid(rid1);
	deepCopyRecord(scan->getRecord(), redRec1);
	CPPUNIT_ASSERT(m_records->prepareUD(scan));
	CPPUNIT_ASSERT(m_records->update(scan, updateSub1));
	m_table->endScan(scan);

	scan = m_table->positionScan(m_session, OP_UPDATE, &scanOpInfo, m_tableDef->m_numCols, readCols);
	scan->setCurrentRid(rid2);
	deepCopyRecord(scan->getRecord(), redRec1);
	scan->getRecord()->m_rowId = rid2;
	CPPUNIT_ASSERT(m_records->prepareUD(scan));
	CPPUNIT_ASSERT(m_records->update(scan, updateSub1));
	m_table->endScan(scan);
	commitTrx(trxSys, updateTrx1);

	//测试当前读
	TNTTransaction *trx1 = startTrx(trxSys, m_conn, txnId2 + 1, TL_S);
	m_session->setTrans(trx1);
	initTNTOpInfo(scanOpInfo, TL_S);
	scan = m_table->positionScan(m_session, OP_READ, &scanOpInfo, m_tableDef->m_numCols, readCols);
	scan->setCurrentRid(rid1);
	deepCopyRecord(scan->getRecord(), redRec1);
	tntVisible = m_records->getRecord(scan, 0, &ntseVisible);
	CPPUNIT_ASSERT(tntVisible == true);
	Record *redRec2 = scan->getRecord();
	confirmRedRecord(m_tableDef, redRec2, name1, stuNo2, age2, sex1, clsNo1, grade1);
	m_table->endScan(scan);
	commitTrx(trxSys, trx1);

	GetRecordThread getRecThread(m_db, m_tableDef, m_table);
	getRecThread.enableSyncPoint(SP_MRECS_GETRECORD_PTR_MODIFY);
	getRecThread.start();
	getRecThread.joinSyncPoint(SP_MRECS_GETRECORD_PTR_MODIFY);
	//第二次更新，做增长更新，因为现在内存堆中有2条记录，所以对第一条记录做增长更新必然改变ptr
	u64 txnId3 = 30005;
	char *name3 = "netease123";
	u16 age3 = 29;
	u32 clsNo3 = 5003;
	columns.clear();
	datas.clear();
	columns.push_back(0);
	datas.push_back(name3);
	columns.push_back(2);
	datas.push_back(&age3);
	columns.push_back(4);
	datas.push_back(&clsNo3);
	SubRecord *updateSub2 = createRedSubRecord(m_tableDef, columns, datas);
	allRedSub1.m_data = updateSub1->m_data;
	RecordOper::mergeSubRecordRR(m_tableDef, updateSub2, &allRedSub1);

	TNTTransaction *updateTrx2 = startTrx(trxSys, m_conn, txnId3, TL_X);
	m_session->setTrans(updateTrx2);
	initTNTOpInfo(scanOpInfo, TL_X);
	scan = m_table->positionScan(m_session, OP_UPDATE, &scanOpInfo, m_tableDef->m_numCols, readCols);
	scan->setCurrentRid(rid1);
	CPPUNIT_ASSERT(!m_records->prepareUD(scan));
	CPPUNIT_ASSERT(m_records->update(scan, updateSub2));
	m_table->endScan(scan);
	commitTrx(trxSys, updateTrx2);

	freeSubRecord(updateSub1);
	freeSubRecord(updateSub2);

	getRecThread.disableSyncPoint(SP_MRECS_GETRECORD_PTR_MODIFY);
	getRecThread.notifySyncPoint(SP_MRECS_GETRECORD_PTR_MODIFY);
	getRecThread.join(-1);

	//ntse不可见
	u64 trxIds2[] = {txnId1};
	TNTTransaction *trx2 = startTrx(trxSys, m_conn, txnId1 + 10, TL_NO, false, trxIds2, 1);
	m_session->setTrans(trx2);
	initTNTOpInfo(scanOpInfo, TL_NO);
	scan = m_table->positionScan(m_session, OP_READ, &scanOpInfo, m_tableDef->m_numCols, readCols);
	scan->setCurrentRid(rid1);
	tntVisible = m_records->getRecord(scan, version, &ntseVisible);
	CPPUNIT_ASSERT(tntVisible == false);
	CPPUNIT_ASSERT(ntseVisible == false);
	m_table->endScan(scan);
	commitTrx(trxSys, trx2);

	//ntse可见
	TNTTransaction *trx3 = startTrx(trxSys, m_conn, txnId1 + 12);
	m_session->setTrans(trx3);
	initTNTOpInfo(scanOpInfo, TL_NO);
	scan = m_table->positionScan(m_session, OP_READ, &scanOpInfo, m_tableDef->m_numCols, readCols);
	scan->setCurrentRid(rid1);
	tntVisible = m_records->getRecord(scan, version, &ntseVisible);
	CPPUNIT_ASSERT(tntVisible == false);
	CPPUNIT_ASSERT(ntseVisible == true);
	m_table->endScan(scan);
	commitTrx(trxSys, trx3);

	//第二条tnt记录可见
	TNTTransaction *trx4 = startTrx(trxSys, m_conn, (txnId2 + txnId3)/2);
	m_session->setTrans(trx4);
	initTNTOpInfo(scanOpInfo, TL_NO);
	scan = m_table->positionScan(m_session, OP_READ, &scanOpInfo, m_tableDef->m_numCols, readCols);
	scan->setCurrentRid(rid1);
	tntVisible = m_records->getRecord(scan, version, &ntseVisible);
	CPPUNIT_ASSERT(tntVisible == true);
	redRec2 = scan->getRecord();
	confirmRedRecord(m_tableDef, redRec2, name1, stuNo2, age2, sex1, clsNo1, grade1);
	m_table->endScan(scan);
	commitTrx(trxSys, trx4);

	//第三条tnt记录可见
	TNTTransaction *trx5 = startTrx(trxSys, m_conn, txnId3 + 10);
	m_session->setTrans(trx5);
	initTNTOpInfo(scanOpInfo, TL_NO);
	scan = m_table->positionScan(m_session, OP_READ, &scanOpInfo, m_tableDef->m_numCols, readCols);
	scan->setCurrentRid(rid1);
	tntVisible = m_records->getRecord(scan, version, &ntseVisible);
	CPPUNIT_ASSERT(tntVisible == true);
	Record *redRec3 = scan->getRecord();
	confirmRedRecord(m_tableDef, redRec3, name3, stuNo2, age3, sex1, clsNo3, grade1);
	m_table->endScan(scan);
	commitTrx(trxSys, trx5);
	
	delete [] redRec1->m_data;
	delete redRec1;
	ctx->resetToSavepoint(sp);
}

void MRecordsTestCase::testFixLenRemove() {
	initTable(true);
	testRemove();
	shutDownTable();
}

void MRecordsTestCase::testVarLenRemove() {
	initTable(false);
	testRemove();
	shutDownTable();
}

void MRecordsTestCase::testRemove() {
	TNTOpInfo scanOpInfo;
	TNTTblScan *scan = NULL;
	TNTTrxSys *trxSys = m_db->getTransSys();
	bool ntseVisible = true;
	bool tntVisible = true;

	u8 vTblIndex = 0;
	u16 version = m_records->getVersion();
	MemoryContext *ctx = m_session->getMemoryContext();
	u64 sp = ctx->setSavepoint();
	u16 *readCols = (u16 *)ctx->alloc(sizeof(u16) * m_tableDef->m_numCols);
	for (u16 i = 0; i < m_tableDef->m_numCols; ++i) {
		readCols[i] = i;
	}

	u64 txnId1 = 10005;
	const char *name1 = "netease1";
	u32 stuNo1 = 5001;
	u16 age1 = 25;
	const char *sex1 = "F";
	u32 clsNo1 = 3001;
	u64 grade1 = 8001;
	RowId rid1 = 10008;
	Record *redRec1 = createStudentRec(rid1, name1, stuNo1, age1, sex1, clsNo1, grade1);
	SubRecord allRedSub1(redRec1->m_format, m_tableDef->m_numCols, readCols, redRec1->m_data, redRec1->m_size, redRec1->m_rowId);
	CPPUNIT_ASSERT(m_records->insert(m_session, txnId1, rid1));

	std::vector<u16> columns;
	std::vector<void *> datas;

	//第一次更新
	u64 txnId2 = 20005;
	u32 stuNo2 = 5002;
	u16 age2 = 28;
	columns.push_back(1);
	datas.push_back(&stuNo2);
	columns.push_back(2);
	datas.push_back(&age2);
	SubRecord *updateSub1 = createRedSubRecord(m_tableDef, columns, datas);
	RecordOper::mergeSubRecordRR(m_tableDef, updateSub1, &allRedSub1);

	TNTTransaction *updateTrx1 = startTrx(trxSys, m_conn, txnId2, TL_X);
	m_session->setTrans(updateTrx1);
	initTNTOpInfo(scanOpInfo, TL_X);
	scan = m_table->positionScan(m_session, OP_UPDATE, &scanOpInfo, m_tableDef->m_numCols, readCols);
	scan->setCurrentRid(rid1);
	deepCopyRecord(scan->getRecord(), redRec1);
	CPPUNIT_ASSERT(m_records->prepareUD(scan));
	CPPUNIT_ASSERT(m_records->update(scan, updateSub1));
	m_table->endScan(scan);
	commitTrx(trxSys, updateTrx1);
	freeSubRecord(updateSub1);

	//测试当前读
	TNTTransaction *trx0 = startTrx(trxSys, m_conn, txnId2 + 1, TL_S);
	m_session->setTrans(trx0);
	initTNTOpInfo(scanOpInfo, TL_S);
	scan = m_table->positionScan(m_session, OP_READ, &scanOpInfo, m_tableDef->m_numCols, readCols);
	scan->setCurrentRid(rid1);
	deepCopyRecord(scan->getRecord(), redRec1);
	tntVisible = m_records->getRecord(scan, 0, &ntseVisible);
	CPPUNIT_ASSERT(tntVisible == true);
	Record *redRec2 = scan->getRecord();
	confirmRedRecord(m_tableDef, redRec2, name1, stuNo2, age2, sex1, clsNo1, grade1);
	m_table->endScan(scan);
	commitTrx(trxSys, trx0);

	//然后对该记录进行删除
	u64 txnId3 = 30005;
	TNTTransaction *delTrx1 = startTrx(trxSys, m_conn, txnId3, TL_X);
	m_session->setTrans(delTrx1);
	initTNTOpInfo(scanOpInfo, TL_X);
	scan = m_table->positionScan(m_session, OP_DELETE, &scanOpInfo, m_tableDef->m_numCols, readCols);
	scan->setCurrentRid(rid1);
	CPPUNIT_ASSERT(!m_records->prepareUD(scan));
	CPPUNIT_ASSERT(m_records->remove(scan));
	m_table->endScan(scan);
	commitTrx(trxSys, delTrx1);

	//测试当前读
	TNTTransaction *trx1 = startTrx(trxSys, m_conn, txnId3 + 1, TL_S);
	m_session->setTrans(trx1);
	initTNTOpInfo(scanOpInfo, TL_S);
	scan = m_table->positionScan(m_session, OP_READ, &scanOpInfo, m_tableDef->m_numCols, readCols);
	scan->setCurrentRid(rid1);
	deepCopyRecord(scan->getRecord(), redRec1);
	tntVisible = m_records->getRecord(scan, 0, &ntseVisible);
	CPPUNIT_ASSERT(tntVisible == false);
	CPPUNIT_ASSERT(ntseVisible == false);
	m_table->endScan(scan);
	commitTrx(trxSys, trx1);

	//ntse不可见
	u64 trxIds2[] = {txnId1};
	TNTTransaction *trx2 = startTrx(trxSys, m_conn, txnId1 + 10, TL_NO, false, trxIds2, 1);
	m_session->setTrans(trx2);
	initTNTOpInfo(scanOpInfo, TL_NO);
	scan = m_table->positionScan(m_session, OP_READ, &scanOpInfo, m_tableDef->m_numCols, readCols);
	scan->setCurrentRid(rid1);
	tntVisible = m_records->getRecord(scan, version, &ntseVisible);
	CPPUNIT_ASSERT(tntVisible == false);
	CPPUNIT_ASSERT(ntseVisible == false);
	m_table->endScan(scan);
	commitTrx(trxSys, trx2);

	//ntse可见
	TNTTransaction *trx3 = startTrx(trxSys, m_conn, txnId1 + 10);
	m_session->setTrans(trx3);
	initTNTOpInfo(scanOpInfo, TL_NO);
	scan = m_table->positionScan(m_session, OP_READ, &scanOpInfo, m_tableDef->m_numCols, readCols);
	scan->setCurrentRid(rid1);
	tntVisible = m_records->getRecord(scan, version, &ntseVisible);
	CPPUNIT_ASSERT(tntVisible == false);
	CPPUNIT_ASSERT(ntseVisible == true);
	m_table->endScan(scan);
	commitTrx(trxSys, trx3);

	//第二条tnt记录可见
	TNTTransaction *trx4 = startTrx(trxSys, m_conn, (txnId2 + txnId3)/2);
	m_session->setTrans(trx4);
	initTNTOpInfo(scanOpInfo, TL_NO);
	scan = m_table->positionScan(m_session, OP_READ, &scanOpInfo, m_tableDef->m_numCols, readCols);
	scan->setCurrentRid(rid1);
	tntVisible = m_records->getRecord(scan, version, &ntseVisible);
	CPPUNIT_ASSERT(tntVisible == true);
	redRec2 = scan->getRecord();
	confirmRedRecord(m_tableDef, redRec2, name1, stuNo2, age2, sex1, clsNo1, grade1);
	m_table->endScan(scan);
	commitTrx(trxSys, trx4);

	//第三条tnt记录可见
	TNTTransaction *trx5 = startTrx(trxSys, m_conn, txnId3 + 10);
	m_session->setTrans(trx5);
	initTNTOpInfo(scanOpInfo, TL_NO);
	scan = m_table->positionScan(m_session, OP_READ, &scanOpInfo, m_tableDef->m_numCols, readCols);
	scan->setCurrentRid(rid1);
	tntVisible = m_records->getRecord(scan, version, &ntseVisible);
	CPPUNIT_ASSERT(tntVisible == false);
	CPPUNIT_ASSERT(ntseVisible == false);
	m_table->endScan(scan);
	commitTrx(trxSys, trx5);
	
	///////////////以下情况测试插入马上删除////////////////
	u64 txnId4 = 80005;
	RowId rid2 = 20008;
	redRec1->m_rowId = rid2;
	CPPUNIT_ASSERT(m_records->insert(m_session, txnId4, rid2));

	//然后对该记录进行删除
	u64 txnId5 = 90005;
	TNTTransaction *delTrx2 = startTrx(trxSys, m_conn, txnId5, TL_X);
	m_session->setTrans(delTrx2);
	initTNTOpInfo(scanOpInfo, TL_X);
	scan = m_table->positionScan(m_session, OP_DELETE, &scanOpInfo, m_tableDef->m_numCols, readCols);
	scan->setCurrentRid(rid2);
	deepCopyRecord(scan->getRecord(), redRec1);
	CPPUNIT_ASSERT(m_records->prepareUD(scan));
	CPPUNIT_ASSERT(m_records->remove(scan));
	m_table->endScan(scan);
	commitTrx(trxSys, delTrx2);

	//测试当前读，ntse不可见
	TNTTransaction *trx6 = startTrx(trxSys, m_conn, txnId5 + 1, TL_S);
	m_session->setTrans(trx6);
	initTNTOpInfo(scanOpInfo, TL_S);
	scan = m_table->positionScan(m_session, OP_READ, &scanOpInfo, m_tableDef->m_numCols, readCols);
	scan->setCurrentRid(rid2);
	deepCopyRecord(scan->getRecord(), redRec1);
	tntVisible = m_records->getRecord(scan, 0, &ntseVisible);
	CPPUNIT_ASSERT(tntVisible == false);
	CPPUNIT_ASSERT(ntseVisible == false);
	m_table->endScan(scan);
	commitTrx(trxSys, trx6);

	//del可见，那么ntse不可见
	TNTTransaction *trx7 = startTrx(trxSys, m_conn, txnId5 + 20);
	m_session->setTrans(trx7);
	initTNTOpInfo(scanOpInfo, TL_NO);
	scan = m_table->positionScan(m_session, OP_READ, &scanOpInfo, m_tableDef->m_numCols, readCols);
	scan->setCurrentRid(rid2);
	tntVisible = m_records->getRecord(scan, version, &ntseVisible);
	CPPUNIT_ASSERT(tntVisible == false);
	CPPUNIT_ASSERT(ntseVisible == false);
	m_table->endScan(scan);
	commitTrx(trxSys, trx7);

	//ntse可见
	TNTTransaction *trx8 = startTrx(trxSys, m_conn, (txnId4 + txnId5)/2);
	m_session->setTrans(trx8);
	initTNTOpInfo(scanOpInfo, TL_NO);
	scan = m_table->positionScan(m_session, OP_READ, &scanOpInfo, m_tableDef->m_numCols, readCols);
	scan->setCurrentRid(rid2);
	tntVisible = m_records->getRecord(scan, version, &ntseVisible);
	CPPUNIT_ASSERT(tntVisible == false);
	CPPUNIT_ASSERT(ntseVisible == true);
	m_table->endScan(scan);
	commitTrx(trxSys, trx8);

	delete [] redRec1->m_data;
	delete redRec1;
	ctx->resetToSavepoint(sp);
}

void MRecordsTestCase::testFixLenRollBackRecord() {
	initTable(true);
	testRollBackRecord();
	shutDownTable();
}

void MRecordsTestCase::testVarLenRollBackRecord() {
	initTable(false);
	testRollBackRecord();
	shutDownTable();
}

void MRecordsTestCase::testRollBackRecord() {
	Record *beforeImg = NULL;
	TNTOpInfo scanOpInfo;
	TNTTblScan *scan = NULL;
	TNTTrxSys *trxSys = m_db->getTransSys();

	bool ntseVisible = true;
	bool tntVisible = true;

	u8 vTblIndex = 0;
	u16 version = m_records->getVersion();
	MemoryContext *ctx = m_session->getMemoryContext();
	u64 sp = ctx->setSavepoint();
	u16 *readCols = (u16 *)ctx->alloc(sizeof(u16) * m_tableDef->m_numCols);
	for (u16 i = 0; i < m_tableDef->m_numCols; ++i) {
		readCols[i] = i;
	}
	Record *afterImg = new (ctx->alloc(sizeof(Record))) Record(INVALID_ROW_ID, REC_REDUNDANT, (byte *)ctx->alloc(m_tableDef->m_maxRecSize), m_tableDef->m_maxRecSize);

	u64 txnId1 = 10005;
	const char *name1 = "netease1";
	u32 stuNo1 = 5001;
	u16 age1 = 25;
	const char *sex1 = "F";
	u32 clsNo1 = 3001;
	u64 grade1 = 8001;
	RowId rid1 = 30008;
	Record *redRec1 = createStudentRec(rid1, name1, stuNo1, age1, sex1, clsNo1, grade1);
	SubRecord allRedSub1(redRec1->m_format, m_tableDef->m_numCols, readCols, redRec1->m_data, redRec1->m_size, redRec1->m_rowId);
	CPPUNIT_ASSERT(m_records->insert(m_session, txnId1, rid1));

	//当前读，ntse可见
	TNTTransaction *trx1 = startTrx(trxSys, m_conn, txnId1 - 10, TL_S);
	m_session->setTrans(trx1);
	initTNTOpInfo(scanOpInfo, TL_S);
	scan = m_table->positionScan(m_session, OP_READ, &scanOpInfo, m_tableDef->m_numCols, readCols);
	scan->setCurrentRid(rid1);
	deepCopyRecord(scan->getRecord(), redRec1);
	tntVisible = m_records->getRecord(scan, version, &ntseVisible);
	CPPUNIT_ASSERT(tntVisible == false);
	CPPUNIT_ASSERT(ntseVisible == true);
	m_table->endScan(scan);
	commitTrx(trxSys, trx1);

	std::vector<u16> columns;
	std::vector<void *> datas;

	//第一次更新
	u64 txnId2 = 20005;
	u32 stuNo2 = 5002;
	u16 age2 = 28;
	columns.push_back(1);
	datas.push_back(&stuNo2);
	columns.push_back(2);
	datas.push_back(&age2);
	SubRecord *updateSub1 = createRedSubRecord(m_tableDef, columns, datas);
	RecordOper::mergeSubRecordRR(m_tableDef, updateSub1, &allRedSub1);

	TNTTransaction *updateTrx1 = startTrx(trxSys, m_conn, txnId2, TL_X);
	m_session->setTrans(updateTrx1);
	initTNTOpInfo(scanOpInfo, TL_X);
	scan = m_table->positionScan(m_session, OP_READ, &scanOpInfo, m_tableDef->m_numCols, readCols);
	scan->setCurrentRid(rid1);
	deepCopyRecord(scan->getRecord(), redRec1);
	CPPUNIT_ASSERT(m_records->prepareUD(scan));
	CPPUNIT_ASSERT(m_records->update(scan, updateSub1));
	m_table->endScan(scan);
	commitTrx(trxSys, updateTrx1);

	//测试当前读
	TNTTransaction *trx2 = startTrx(trxSys, m_conn, txnId2 - 10, TL_S);
	m_session->setTrans(trx2);

	initTNTOpInfo(scanOpInfo, TL_S);
	scan = m_table->positionScan(m_session, OP_READ, &scanOpInfo, m_tableDef->m_numCols, readCols);
	scan->setCurrentRid(rid1);
	deepCopyRecord(scan->getRecord(), redRec1);
	tntVisible = m_records->getRecord(scan, 0, &ntseVisible);
	CPPUNIT_ASSERT(tntVisible == true);
	Record *redRec2 = scan->getRecord();
	confirmRedRecord(m_tableDef, redRec2, name1, stuNo2, age2, sex1, clsNo1, grade1);
	m_table->endScan(scan);
	commitTrx(trxSys, trx2);

	//第二次更新
	u64 txnId3 = 30005;
	u16 age3 = 29;
	u32 clsNo3 = 5003;
	columns.clear();
	datas.clear();
	columns.push_back(2);
	datas.push_back(&age3);
	columns.push_back(4);
	datas.push_back(&clsNo3);
	SubRecord *updateSub2 = createRedSubRecord(m_tableDef, columns, datas);
	allRedSub1.m_data = updateSub1->m_data;
	RecordOper::mergeSubRecordRR(m_tableDef, updateSub2, &allRedSub1);

	TNTTransaction *updateTrx2 = startTrx(trxSys, m_conn, txnId3, TL_X);
	m_session->setTrans(updateTrx2);
	initTNTOpInfo(scanOpInfo, TL_X);
	scan = m_table->positionScan(m_session, OP_UPDATE, &scanOpInfo, m_tableDef->m_numCols, readCols);
	scan->setCurrentRid(rid1);
	CPPUNIT_ASSERT(!m_records->prepareUD(scan));
	CPPUNIT_ASSERT(m_records->update(scan, updateSub2));
	m_table->endScan(scan);
	commitTrx(trxSys, updateTrx2);

	freeSubRecord(updateSub1);
	freeSubRecord(updateSub2);

	//当前读，第三条tnt记录可见
	TNTTransaction *trx3 = startTrx(trxSys, m_conn, txnId3 - 10, TL_S);
	m_session->setTrans(trx3);
	initTNTOpInfo(scanOpInfo, TL_S);
	scan = m_table->positionScan(m_session, OP_READ, &scanOpInfo, m_tableDef->m_numCols, readCols);
	scan->setCurrentRid(rid1);
	tntVisible = m_records->getRecord(scan, 0, &ntseVisible);
	CPPUNIT_ASSERT(tntVisible == true);
	Record *redRec3 = scan->getRecord();
	confirmRedRecord(m_tableDef, redRec3, name1, stuNo2, age3, sex1, clsNo3, grade1);
	m_table->endScan(scan);
	commitTrx(trxSys, trx3);

	MHeapRec *rollBackRec = m_records->getBeforeAndAfterImageForRollback(m_session, rid1, &beforeImg, afterImg);
	m_records->rollBackRecord(m_session, rid1, rollBackRec);
	//beforeImg = m_records->rollBackRecord(m_session, rid1, afterImg);
	CPPUNIT_ASSERT(beforeImg != NULL);
	confirmRedRecord(m_tableDef, afterImg, name1, stuNo2, age3, sex1, clsNo3, grade1);
	confirmRedRecord(m_tableDef, beforeImg, name1, stuNo2, age2, sex1, clsNo1, grade1);

	//测试当前读，第二条tnt记录可见
	TNTTransaction *trx4 = startTrx(trxSys, m_conn, txnId3 - 9, TL_S);
	m_session->setTrans(trx4);
	initTNTOpInfo(scanOpInfo, TL_S);
	scan = m_table->positionScan(m_session, OP_READ, &scanOpInfo, m_tableDef->m_numCols, readCols);
	scan->setCurrentRid(rid1);
	tntVisible = m_records->getRecord(scan, 0, &ntseVisible);
	CPPUNIT_ASSERT(tntVisible == true);
	redRec2 = scan->getRecord();
	confirmRedRecord(m_tableDef, redRec2, name1, stuNo2, age2, sex1, clsNo1, grade1);
	m_table->endScan(scan);
	commitTrx(trxSys, trx4);


	rollBackRec = m_records->getBeforeAndAfterImageForRollback(m_session, rid1, &beforeImg, afterImg);
	m_records->rollBackRecord(m_session, rid1, rollBackRec);
	//beforeImg = m_records->rollBackRecord(m_session, rid1, afterImg);
	confirmRedRecord(m_tableDef, afterImg, name1, stuNo2, age2, sex1, clsNo1, grade1);
	CPPUNIT_ASSERT(beforeImg == NULL);

	//测试当前读，第一条tnt记录可见
	Record *ntseRedRec = createStudentRec(rid1, name1, stuNo1, age1, sex1, clsNo1, grade1);
	TNTTransaction *trx5 = startTrx(trxSys, m_conn, txnId3 - 8, TL_S);
	m_session->setTrans(trx5);
	initTNTOpInfo(scanOpInfo, TL_S);
	scan = m_table->positionScan(m_session, OP_READ, &scanOpInfo, m_tableDef->m_numCols, readCols);
	scan->setCurrentRid(rid1);
	deepCopyRecord(scan->getRecord(), ntseRedRec);
	tntVisible = m_records->getRecord(scan, 0, &ntseVisible);
	CPPUNIT_ASSERT(tntVisible == false);
	CPPUNIT_ASSERT(ntseVisible == true);
	confirmRedRecord(m_tableDef, scan->getRecord(), name1, stuNo1, age1, sex1, clsNo1, grade1);
	m_table->endScan(scan);
	delete [] ntseRedRec->m_data;
	delete ntseRedRec;
	commitTrx(trxSys, trx5);
	
	afterImg->m_rowId = INVALID_ROW_ID;
	
	rollBackRec = m_records->getBeforeAndAfterImageForRollback(m_session, rid1, &beforeImg, afterImg);
	m_records->rollBackRecord(m_session, rid1, rollBackRec);
//	beforeImg = m_records->rollBackRecord(m_session, rid1, afterImg);
	CPPUNIT_ASSERT(afterImg->m_rowId == INVALID_ROW_ID);
	CPPUNIT_ASSERT(beforeImg == NULL);

	delete [] redRec1->m_data;
	delete redRec1;
	ctx->resetToSavepoint(sp);
}

void MRecordsTestCase::testFixLenRedoInsert() {
	initTable(true);
	testRedoInsert();
	shutDownTable();
}

void MRecordsTestCase::testVarLenRedoInsert() {
	initTable(false);
	testRedoInsert();
	shutDownTable();
}

void MRecordsTestCase::testRedoInsert() {
	TNTOpInfo scanOpInfo;
	TNTTblScan *scan = NULL;
	TNTTrxSys *trxSys = m_db->getTransSys();
	MemoryContext *ctx = m_session->getMemoryContext();
	u64 sp = ctx->setSavepoint();
	HashIndexEntry *entry = NULL;
	u16 version = m_records->getVersion();
	RowId rid = 50005;
	TrxId txnId = 1002;
	m_records->redoInsert(m_session, rid, txnId, RT_COMMIT);
	entry = m_records->m_hashIndex->get(rid, ctx);
	CPPUNIT_ASSERT(entry == NULL);

	m_records->redoInsert(m_session, rid, txnId, RT_PREPARE);
	entry = m_records->m_hashIndex->get(rid, ctx);
	CPPUNIT_ASSERT(entry != NULL);
	CPPUNIT_ASSERT(entry->m_value == txnId);
	m_records->m_hashIndex->remove(rid);

	u16 *readCols = (u16 *)ctx->alloc(sizeof(u16) * m_tableDef->m_numCols);
	for (u16 i = 0; i < m_tableDef->m_numCols; ++i) {
		readCols[i] = i;
	}

	//rid被重用的测试用例
	u64 txnId1 = 10005;
	const char *name1 = "netease1";
	u32 stuNo1 = 5001;
	u16 age1 = 25;
	const char *sex1 = "F";
	u32 clsNo1 = 3001;
	u64 grade1 = 8001;
	RowId rid1 = 10008;
	Record *redRec1 = createStudentRec(rid, name1, stuNo1, age1, sex1, clsNo1, grade1);
	CPPUNIT_ASSERT(m_records->insert(m_session, txnId1, rid));
	m_records->redoInsert(m_session, rid, txnId1, RT_COMMIT);
	entry = m_records->m_hashIndex->get(rid, ctx);
	CPPUNIT_ASSERT(entry == NULL);
	delete[] redRec1->m_data;
	delete redRec1;

	redRec1 = createStudentRec(INVALID_ROW_ID/*rid*/, name1, stuNo1, age1, sex1, clsNo1, grade1);
	SubRecord allRedSub1(redRec1->m_format, m_tableDef->m_numCols, readCols, redRec1->m_data, redRec1->m_size, redRec1->m_rowId);
	uint dupIndex = 0;
	TNTTransaction *insertTrx1 = startTrx(trxSys, m_conn, txnId1, TL_X);
	m_session->setTrans(insertTrx1);
	initTNTOpInfo(scanOpInfo, TL_X);
	rid = m_table->insert(m_session, redRec1->m_data, &dupIndex, &scanOpInfo);
	redRec1->m_rowId = rid;
	commitTrx(trxSys, insertTrx1);

	std::vector<u16> columns;
	std::vector<void *> datas;

	//第一次更新
	u64 txnId2 = 20005;
	u32 stuNo2 = 5002;
	u16 age2 = 28;
	columns.push_back(1);
	datas.push_back(&stuNo2);
	columns.push_back(2);
	datas.push_back(&age2);
	SubRecord *updateSub1 = createRedSubRecord(m_tableDef, columns, datas);
	RecordOper::mergeSubRecordRR(m_tableDef, updateSub1, &allRedSub1);

	TNTTransaction *updateTrx1 = startTrx(trxSys, m_conn, txnId2, TL_X, true);
	m_session->setTrans(updateTrx1);
	initTNTOpInfo(scanOpInfo, TL_X);
	scan = m_table->positionScan(m_session, OP_UPDATE, &scanOpInfo, m_tableDef->m_numCols, readCols);
	scan->setCurrentRid(rid);
	deepCopyRecord(scan->getRecord(), redRec1);
	CPPUNIT_ASSERT(m_records->prepareUD(scan));
	CPPUNIT_ASSERT(m_records->update(scan, updateSub1));
	m_table->endScan(scan);
	commitTrx(trxSys, updateTrx1);

	freeSubRecord(updateSub1);

	m_records->redoInsert(m_session, rid, txnId2, RT_COMMIT);
	entry = m_records->m_hashIndex->get(rid, ctx);
	CPPUNIT_ASSERT(entry == NULL);
	delete[] redRec1->m_data;
	delete redRec1;

	ctx->resetToSavepoint(sp);
}

void MRecordsTestCase::testFixLenRedoFirUpdate() {
	initTable(true);
	testRedoFirUpdate();
	shutDownTable();
}

void MRecordsTestCase::testVarLenRedoFirUpdate() {
	initTable(false);
	testRedoFirUpdate();
	shutDownTable();
}

void MRecordsTestCase::testRedoFirUpdate() {
	TNTOpInfo scanOpInfo;
	TNTTblScan *scan = NULL;
	TNTTrxSys *trxSys = m_db->getTransSys();
	bool ntseVisible = true;
	bool tntVisible = true;
	LsnType beginLsn;

	u8 vTblIndex = 0;
	u16 version = m_records->getVersion();
	MemoryContext *ctx = m_session->getMemoryContext();
	u64 sp = ctx->setSavepoint();
	u16 *readCols = (u16 *)ctx->alloc(sizeof(u16) * m_tableDef->m_numCols);
	for (u16 i = 0; i < m_tableDef->m_numCols; ++i) {
		readCols[i] = i;
	}

	u64 txnId1 = 10005;
	const char *name1 = "netease1";
	u32 stuNo1 = 5001;
	u16 age1 = 25;
	const char *sex1 = "F";
	u32 clsNo1 = 3001;
	u64 grade1 = 8001;
	Record *redRec1 = createStudentRec(INVALID_ROW_ID/*rid*/, name1, stuNo1, age1, sex1, clsNo1, grade1);
	SubRecord allRedSub1(redRec1->m_format, m_tableDef->m_numCols, readCols, redRec1->m_data, redRec1->m_size, redRec1->m_rowId);
	uint dupIndex = 0;
	TNTTransaction *insertTrx1 = startTrx(trxSys, m_conn, txnId1, TL_X);
	m_session->setTrans(insertTrx1);
	initTNTOpInfo(scanOpInfo, TL_X);
	RowId rid = m_table->insert(m_session, redRec1->m_data, &dupIndex, &scanOpInfo);
	redRec1->m_rowId = rid;
	commitTrx(trxSys, insertTrx1);

	std::vector<u16> columns;
	std::vector<void *> datas;

	//第一次更新
	u64 txnId2 = 20005;
	u32 stuNo2 = 5002;
	u16 age2 = 28;
	columns.push_back(1);
	datas.push_back(&stuNo2);
	columns.push_back(2);
	datas.push_back(&age2);
	SubRecord *updateSub1 = createRedSubRecord(m_tableDef, columns, datas);
	RecordOper::mergeSubRecordRR(m_tableDef, updateSub1, &allRedSub1);

	TNTTransaction *updateTrx1 = startTrx(trxSys, m_conn, txnId2, TL_X, true);
	m_session->setTrans(updateTrx1);
	initTNTOpInfo(scanOpInfo, TL_X);
	scan = m_table->positionScan(m_session, OP_UPDATE, &scanOpInfo, m_tableDef->m_numCols, readCols);
	scan->setCurrentRid(rid);
	deepCopyRecord(scan->getRecord(), redRec1);
	CPPUNIT_ASSERT(m_records->prepareUD(scan));
	CPPUNIT_ASSERT(m_records->update(scan, updateSub1));
	m_table->endScan(scan);
	beginLsn = updateTrx1->getTrxBeginLsn();
	commitTrx(trxSys, updateTrx1);
	freeSubRecord(updateSub1);

	reOpenAllTable();

	//测试当前读
	TNTTransaction *trx1 = startTrx(trxSys, m_conn, txnId2 + 1, TL_S);
	m_session->setTrans(trx1);
	initTNTOpInfo(scanOpInfo, TL_S);
	scan = m_table->positionScan(m_session, OP_READ, &scanOpInfo, m_tableDef->m_numCols, readCols);
	scan->setCurrentRid(rid);
	deepCopyRecord(scan->getRecord(), redRec1);
	tntVisible = m_records->getRecord(scan, 0, &ntseVisible);
	CPPUNIT_ASSERT(tntVisible == false);
	CPPUNIT_ASSERT(ntseVisible == true);
	Record *redRec2 = scan->getRecord();
	confirmRedRecord(m_tableDef, redRec2, name1, stuNo1, age1, sex1, clsNo1, grade1);
	m_table->endScan(scan);
	commitTrx(trxSys, trx1);

	// 检查日志并做redo
	LogScanHandle *logHdl = m_db->getTNTLog()->beginScan(beginLsn, Txnlog::MAX_LSN);
	CPPUNIT_ASSERT(getNextTNTLog(m_db->getTNTLog(), logHdl));
	CPPUNIT_ASSERT(logHdl->logEntry()->m_logType == TNT_BEGIN_TRANS);
	CPPUNIT_ASSERT(getNextTNTLog(m_db->getTNTLog(), logHdl));
	CPPUNIT_ASSERT(logHdl->logEntry()->m_logType == TNT_U_I_LOG);
	m_table->redoFirUpdate(m_session, logHdl->logEntry(), RT_COMMIT);
	CPPUNIT_ASSERT(getNextTNTLog(m_db->getTNTLog(), logHdl));
	CPPUNIT_ASSERT(logHdl->logEntry()->m_logType == TNT_COMMIT_TRANS);
	CPPUNIT_ASSERT(!getNextTNTLog(m_db->getTNTLog(), logHdl));
	m_db->getTNTLog()->endScan(logHdl);

	//测试当前读
	TNTTransaction *trx2 = startTrx(trxSys, m_conn, txnId2 + 2, TL_S);
	m_session->setTrans(trx2);
	initTNTOpInfo(scanOpInfo, TL_S);
	scan = m_table->positionScan(m_session, OP_READ, &scanOpInfo, m_tableDef->m_numCols, readCols);
	scan->setCurrentRid(rid);
	deepCopyRecord(scan->getRecord(), redRec1);
	tntVisible = m_records->getRecord(scan, 0, &ntseVisible);
	CPPUNIT_ASSERT(tntVisible == true);
	Record *redRec3 = scan->getRecord();
	confirmRedRecord(m_tableDef, redRec3, name1, stuNo2, age2, sex1, clsNo1, grade1);
	m_table->endScan(scan);
	commitTrx(trxSys, trx2);
	
	delete[] redRec1->m_data;
	delete redRec1;
	ctx->resetToSavepoint(sp);
}

void MRecordsTestCase::testFixLenRedoSecUpdate() {
	initTable(true);
	testRedoSecUpdate();
	shutDownTable();
}

void MRecordsTestCase::testVarLenRedoSecUpdate() {
	initTable(false);
	testRedoSecUpdate();
	shutDownTable();
}

void MRecordsTestCase::testRedoSecUpdate() {
	TNTOpInfo scanOpInfo;
	TNTTblScan *scan = NULL;
	TNTTrxSys *trxSys = m_db->getTransSys();
	bool ntseVisible = true;
	bool tntVisible = true;
	LsnType beginLsn;

	u8 vTblIndex = 0;
	u16 version = m_records->getVersion();
	MemoryContext *ctx = m_session->getMemoryContext();
	u64 sp = ctx->setSavepoint();
	u16 *readCols = (u16 *)ctx->alloc(sizeof(u16) * m_tableDef->m_numCols);
	for (u16 i = 0; i < m_tableDef->m_numCols; ++i) {
		readCols[i] = i;
	}

	u64 txnId1 = 10005;
	const char *name1 = "netease1";
	u32 stuNo1 = 5001;
	u16 age1 = 25;
	const char *sex1 = "F";
	u32 clsNo1 = 3001;
	u64 grade1 = 8001;
	Record *redRec1 = createStudentRec(INVALID_ROW_ID/*rid*/, name1, stuNo1, age1, sex1, clsNo1, grade1);
	SubRecord allRedSub1(redRec1->m_format, m_tableDef->m_numCols, readCols, redRec1->m_data, redRec1->m_size, redRec1->m_rowId);
	uint dupIndex = 0;
	TNTTransaction *insertTrx1 = startTrx(trxSys, m_conn, txnId1, TL_X);
	m_session->setTrans(insertTrx1);
	initTNTOpInfo(scanOpInfo, TL_X);
	RowId rid = m_table->insert(m_session, redRec1->m_data, &dupIndex, &scanOpInfo);
	redRec1->m_rowId = rid;
	commitTrx(trxSys, insertTrx1);

	std::vector<u16> columns;
	std::vector<void *> datas;

	//第一次更新
	u64 txnId2 = 20005;
	u32 stuNo2 = 5002;
	u16 age2 = 28;
	columns.push_back(1);
	datas.push_back(&stuNo2);
	columns.push_back(2);
	datas.push_back(&age2);
	SubRecord *updateSub1 = createRedSubRecord(m_tableDef, columns, datas);
	RecordOper::mergeSubRecordRR(m_tableDef, updateSub1, &allRedSub1);

	TNTTransaction *updateTrx1 = startTrx(trxSys, m_conn, txnId2, TL_X, true);
	m_session->setTrans(updateTrx1);
	initTNTOpInfo(scanOpInfo, TL_X);
	scan = m_table->positionScan(m_session, OP_UPDATE, &scanOpInfo, m_tableDef->m_numCols, readCols);
	scan->setCurrentRid(rid);
	deepCopyRecord(scan->getRecord(), redRec1);
	CPPUNIT_ASSERT(m_records->prepareUD(scan));
	CPPUNIT_ASSERT(m_records->update(scan, updateSub1));
	m_table->endScan(scan);
	beginLsn = updateTrx1->getTrxBeginLsn();
	commitTrx(trxSys, updateTrx1);

	//第二次更新
	u64 txnId3 = 30005;
	u16 age3 = 29;
	u32 clsNo3 = 5003;
	columns.clear();
	datas.clear();
	columns.push_back(2);
	datas.push_back(&age3);
	columns.push_back(4);
	datas.push_back(&clsNo3);
	SubRecord *updateSub2 = createRedSubRecord(m_tableDef, columns, datas);
	allRedSub1.m_data = updateSub1->m_data;
	RecordOper::mergeSubRecordRR(m_tableDef, updateSub2, &allRedSub1);
	
	TNTTransaction *updateTrx2 = startTrx(trxSys, m_conn, txnId3, TL_X, true);
	m_session->setTrans(updateTrx2);
	initTNTOpInfo(scanOpInfo, TL_X);
	scan = m_table->positionScan(m_session, OP_UPDATE, &scanOpInfo, m_tableDef->m_numCols, readCols);
	scan->setCurrentRid(rid);
	CPPUNIT_ASSERT(!m_records->prepareUD(scan));
	CPPUNIT_ASSERT(m_records->update(scan, updateSub2));
	m_table->endScan(scan);
	commitTrx(trxSys, updateTrx2);

	freeSubRecord(updateSub1);
	freeSubRecord(updateSub2);

	reOpenAllTable();

	// 检查日志并做redo
	LogScanHandle *logHdl = m_db->getTNTLog()->beginScan(beginLsn, Txnlog::MAX_LSN);
	CPPUNIT_ASSERT(getNextTNTLog(m_db->getTNTLog(), logHdl));
	CPPUNIT_ASSERT(logHdl->logEntry()->m_logType == TNT_BEGIN_TRANS);
	CPPUNIT_ASSERT(getNextTNTLog(m_db->getTNTLog(), logHdl));
	CPPUNIT_ASSERT(logHdl->logEntry()->m_logType == TNT_U_I_LOG);
	m_table->redoFirUpdate(m_session, logHdl->logEntry(), RT_COMMIT);
	CPPUNIT_ASSERT(getNextTNTLog(m_db->getTNTLog(), logHdl));
	CPPUNIT_ASSERT(logHdl->logEntry()->m_logType == TNT_COMMIT_TRANS);


	CPPUNIT_ASSERT(getNextTNTLog(m_db->getTNTLog(), logHdl));
	CPPUNIT_ASSERT(logHdl->logEntry()->m_logType == TNT_BEGIN_TRANS);
	CPPUNIT_ASSERT(getNextTNTLog(m_db->getTNTLog(), logHdl));
	CPPUNIT_ASSERT(logHdl->logEntry()->m_logType == TNT_U_U_LOG);
	m_table->redoSecUpdate(m_session, logHdl->logEntry(), RT_COMMIT);
	CPPUNIT_ASSERT(getNextTNTLog(m_db->getTNTLog(), logHdl));
	CPPUNIT_ASSERT(logHdl->logEntry()->m_logType == TNT_COMMIT_TRANS);
	CPPUNIT_ASSERT(!getNextTNTLog(m_db->getTNTLog(), logHdl));
	m_db->getTNTLog()->endScan(logHdl);

	//当前读，第三条tnt记录可见
	TNTTransaction *trx2 = startTrx(trxSys, m_conn, txnId3 + 1, TL_S);
	initTNTOpInfo(scanOpInfo, TL_S);
	scan = m_table->positionScan(m_session, OP_READ, &scanOpInfo, m_tableDef->m_numCols, readCols);
	scan->setCurrentRid(rid);
	//memcpy(scan->getRecord(), redRec1, sizeof(Record));
	tntVisible = m_records->getRecord(scan, 0, &ntseVisible);
	CPPUNIT_ASSERT(tntVisible == true);
	Record *redRec2 = scan->getRecord();
	confirmRedRecord(m_tableDef, redRec2, name1, stuNo2, age3, sex1, clsNo3, grade1);
	commitTrx(trxSys, trx2);

	//第二条tnt记录可见
	TNTTransaction *trx3 = startTrx(trxSys, m_conn, (txnId2 + txnId3)/2);
	m_session->setTrans(trx3);
	initTNTOpInfo(scanOpInfo, TL_NO);
	scan = m_table->positionScan(m_session, OP_READ, &scanOpInfo, m_tableDef->m_numCols, readCols);
	scan->setCurrentRid(rid);
	tntVisible = m_records->getRecord(scan, version, &ntseVisible);
	CPPUNIT_ASSERT(tntVisible == true);
	Record *redRec3 = scan->getRecord();
	confirmRedRecord(m_tableDef, redRec3, name1, stuNo2, age2, sex1, clsNo1, grade1);
	m_table->endScan(scan);
	commitTrx(trxSys, trx3);
	
	delete[] redRec1->m_data;
	delete redRec1;
	ctx->resetToSavepoint(sp);
}

void MRecordsTestCase::testFixLenRedoFirRemove() {
	initTable(true);
	testRedoFirRemove();
	shutDownTable();
}

void MRecordsTestCase::testVarLenRedoFirRemove() {
	initTable(false);
	testRedoFirRemove();
	shutDownTable();
}

void MRecordsTestCase::testRedoFirRemove() {
	TNTOpInfo scanOpInfo;
	TNTTblScan *scan = NULL;
	TNTTrxSys *trxSys = m_db->getTransSys();
	bool ntseVisible = true;
	bool tntVisible = true;
	LsnType beginLsn;

	u8 vTblIndex = 0;
	u16 version = m_records->getVersion();
	MemoryContext *ctx = m_session->getMemoryContext();
	u64 sp = ctx->setSavepoint();
	u16 *readCols = (u16 *)ctx->alloc(sizeof(u16) * m_tableDef->m_numCols);
	for (u16 i = 0; i < m_tableDef->m_numCols; ++i) {
		readCols[i] = i;
	}

	u64 txnId1 = 10005;
	const char *name1 = "netease1";
	u32 stuNo1 = 5001;
	u16 age1 = 25;
	const char *sex1 = "F";
	u32 clsNo1 = 3001;
	u64 grade1 = 8001;
	RowId rid1 = 10008;
	Record *redRec1 = createStudentRec(INVALID_ROW_ID, name1, stuNo1, age1, sex1, clsNo1, grade1);
	uint dupIndex = 0;
	TNTTransaction *insertTrx1 = startTrx(trxSys, m_conn, txnId1, TL_X);
	m_session->setTrans(insertTrx1);
	initTNTOpInfo(scanOpInfo, TL_X);
	rid1 = m_table->insert(m_session, redRec1->m_data, &dupIndex, &scanOpInfo);
	CPPUNIT_ASSERT(INVALID_ROW_ID != rid1);
	redRec1->m_rowId = rid1;
	commitTrx(trxSys, insertTrx1);
	//CPPUNIT_ASSERT(m_records->insert(m_session, txnId1, rid1));

	//然后对该记录进行删除
	u64 txnId2 = 20005;
	TNTTransaction *delTrx1 = startTrx(trxSys, m_conn, txnId2, TL_X, true);
	m_session->setTrans(delTrx1);
	initTNTOpInfo(scanOpInfo, TL_X);
	scan = m_table->positionScan(m_session, OP_DELETE, &scanOpInfo, m_tableDef->m_numCols, readCols);
	scan->setCurrentRid(rid1);
	deepCopyRecord(scan->getRecord(), redRec1);
	CPPUNIT_ASSERT(m_records->prepareUD(scan));
	CPPUNIT_ASSERT(m_records->remove(scan));
	m_table->endScan(scan);
	beginLsn = delTrx1->getTrxBeginLsn();
	commitTrx(trxSys, delTrx1);

	reOpenAllTable();

	// 检查日志并做redo
	LogScanHandle *logHdl = m_db->getTNTLog()->beginScan(beginLsn, Txnlog::MAX_LSN);
	CPPUNIT_ASSERT(getNextTNTLog(m_db->getTNTLog(), logHdl));
	CPPUNIT_ASSERT(logHdl->logEntry()->m_logType == TNT_BEGIN_TRANS);
	CPPUNIT_ASSERT(getNextTNTLog(m_db->getTNTLog(), logHdl));
	CPPUNIT_ASSERT(logHdl->logEntry()->m_logType == TNT_D_I_LOG);
	m_table->redoFirRemove(m_session, logHdl->logEntry(), RT_COMMIT);
	CPPUNIT_ASSERT(getNextTNTLog(m_db->getTNTLog(), logHdl));
	CPPUNIT_ASSERT(logHdl->logEntry()->m_logType == TNT_COMMIT_TRANS);
	CPPUNIT_ASSERT(!getNextTNTLog(m_db->getTNTLog(), logHdl));
	m_db->getTNTLog()->endScan(logHdl);

	//测试当前读
	TNTTransaction *trx1 = startTrx(trxSys, m_conn, txnId2 + 1, TL_S);
	m_session->setTrans(trx1);
	initTNTOpInfo(scanOpInfo, TL_S);
	scan = m_table->positionScan(m_session, OP_READ, &scanOpInfo, m_tableDef->m_numCols, readCols);
	scan->setCurrentRid(rid1);
	deepCopyRecord(scan->getRecord(), redRec1);
	tntVisible = m_records->getRecord(scan, 0, &ntseVisible);
	CPPUNIT_ASSERT(tntVisible == false);
	CPPUNIT_ASSERT(ntseVisible == false);
	m_table->endScan(scan);
	commitTrx(trxSys, trx1);

	//ntse可见
	TNTTransaction *trx2 = startTrx(trxSys, m_conn, (txnId1 + txnId2)/2);
	m_session->setTrans(trx2);
	initTNTOpInfo(scanOpInfo, TL_NO);
	scan = m_table->positionScan(m_session, OP_READ, &scanOpInfo, m_tableDef->m_numCols, readCols);
	scan->setCurrentRid(rid1);
	tntVisible = m_records->getRecord(scan, version, &ntseVisible);
	CPPUNIT_ASSERT(tntVisible == false);
	CPPUNIT_ASSERT(ntseVisible == true);
	m_table->endScan(scan);
	commitTrx(trxSys, trx2);

	delete [] redRec1->m_data;
	delete redRec1;
	ctx->resetToSavepoint(sp);
}

void MRecordsTestCase::testFixLenRedoSecRemove() {
	initTable(true);
	testRedoSecRemove();
	shutDownTable();
}

void MRecordsTestCase::testVarLenRedoSecRemove() {
	initTable(false);
	testRedoSecRemove();
	shutDownTable();
}

void MRecordsTestCase::testRedoSecRemove() {
	TNTOpInfo scanOpInfo;
	TNTTblScan *scan = NULL;
	TNTTrxSys *trxSys = m_db->getTransSys();
	bool ntseVisible = true;
	bool tntVisible = true;
	LsnType beginLsn;

	u8 vTblIndex = 0;
	u16 version = m_records->getVersion();
	MemoryContext *ctx = m_session->getMemoryContext();
	u64 sp = ctx->setSavepoint();
	u16 *readCols = (u16 *)ctx->alloc(sizeof(u16) * m_tableDef->m_numCols);
	for (u16 i = 0; i < m_tableDef->m_numCols; ++i) {
		readCols[i] = i;
	}

	u64 txnId1 = 10005;
	const char *name1 = "netease1";
	u32 stuNo1 = 5001;
	u16 age1 = 25;
	const char *sex1 = "F";
	u32 clsNo1 = 3001;
	u64 grade1 = 8001;
	RowId rid1 = 10008;
	Record *redRec1 = createStudentRec(rid1, name1, stuNo1, age1, sex1, clsNo1, grade1);
	SubRecord allRedSub1(redRec1->m_format, m_tableDef->m_numCols, readCols, redRec1->m_data, redRec1->m_size, redRec1->m_rowId);
	uint dupIndex = 0;
	TNTTransaction *insertTrx1 = startTrx(trxSys, m_conn, txnId1, TL_X);
	m_session->setTrans(insertTrx1);
	initTNTOpInfo(scanOpInfo, TL_X);
	rid1 = m_table->insert(m_session, redRec1->m_data, &dupIndex, &scanOpInfo);
	CPPUNIT_ASSERT(INVALID_ROW_ID != rid1);
	redRec1->m_rowId = rid1;
	commitTrx(trxSys, insertTrx1);
	//CPPUNIT_ASSERT(m_records->insert(m_session, txnId1, rid1));

	std::vector<u16> columns;
	std::vector<void *> datas;

	//第一次更新
	u64 txnId2 = 20005;
	u32 stuNo2 = 5002;
	u16 age2 = 28;
	columns.push_back(1);
	datas.push_back(&stuNo2);
	columns.push_back(2);
	datas.push_back(&age2);
	SubRecord *updateSub1 = createRedSubRecord(m_tableDef, columns, datas);
	RecordOper::mergeSubRecordRR(m_tableDef, updateSub1, &allRedSub1);

	TNTTransaction *updateTrx1 = startTrx(trxSys, m_conn, txnId2, TL_X, true);
	m_session->setTrans(updateTrx1);
	initTNTOpInfo(scanOpInfo, TL_X);
	scan = m_table->positionScan(m_session, OP_UPDATE, &scanOpInfo, m_tableDef->m_numCols, readCols);
	scan->setCurrentRid(rid1);
	deepCopyRecord(scan->getRecord(), redRec1);
	CPPUNIT_ASSERT(m_records->prepareUD(scan));
	CPPUNIT_ASSERT(m_records->update(scan, updateSub1));
	m_table->endScan(scan);
	beginLsn = updateTrx1->getTrxBeginLsn();
	commitTrx(trxSys, updateTrx1);

	//然后对该记录进行删除
	u64 txnId3 = 30005;
	TNTTransaction *delTrx1 = startTrx(trxSys, m_conn, txnId3, TL_X, true);
	m_session->setTrans(delTrx1);
	initTNTOpInfo(scanOpInfo, TL_X);
	scan = m_table->positionScan(m_session, OP_DELETE, &scanOpInfo, m_tableDef->m_numCols, readCols);
	scan->setCurrentRid(rid1);
	CPPUNIT_ASSERT(!m_records->prepareUD(scan));
	CPPUNIT_ASSERT(m_records->remove(scan));
	m_table->endScan(scan);
	commitTrx(trxSys, delTrx1);

	reOpenAllTable();

	// 检查日志并做redo
	LogScanHandle *logHdl = m_db->getTNTLog()->beginScan(beginLsn, Txnlog::MAX_LSN);
	CPPUNIT_ASSERT(getNextTNTLog(m_db->getTNTLog(), logHdl));
	CPPUNIT_ASSERT(logHdl->logEntry()->m_logType == TNT_BEGIN_TRANS);
	CPPUNIT_ASSERT(getNextTNTLog(m_db->getTNTLog(), logHdl));
	CPPUNIT_ASSERT(logHdl->logEntry()->m_logType == TNT_U_I_LOG);
	m_table->redoFirUpdate(m_session, logHdl->logEntry(), RT_COMMIT);
	CPPUNIT_ASSERT(getNextTNTLog(m_db->getTNTLog(), logHdl));
	CPPUNIT_ASSERT(logHdl->logEntry()->m_logType == TNT_COMMIT_TRANS);

	CPPUNIT_ASSERT(getNextTNTLog(m_db->getTNTLog(), logHdl));
	CPPUNIT_ASSERT(logHdl->logEntry()->m_logType == TNT_BEGIN_TRANS);
	CPPUNIT_ASSERT(getNextTNTLog(m_db->getTNTLog(), logHdl));
	CPPUNIT_ASSERT(logHdl->logEntry()->m_logType == TNT_D_U_LOG);
	m_table->redoSecRemove(m_session, logHdl->logEntry(), RT_COMMIT);
	CPPUNIT_ASSERT(getNextTNTLog(m_db->getTNTLog(), logHdl));
	CPPUNIT_ASSERT(logHdl->logEntry()->m_logType == TNT_COMMIT_TRANS);
	CPPUNIT_ASSERT(!getNextTNTLog(m_db->getTNTLog(), logHdl));
	m_db->getTNTLog()->endScan(logHdl);

	//测试当前读
	TNTTransaction *trx1 = startTrx(trxSys, m_conn, txnId3 + 1, TL_S);
	m_session->setTrans(trx1);
	initTNTOpInfo(scanOpInfo, TL_S);
	scan = m_table->positionScan(m_session, OP_READ, &scanOpInfo, m_tableDef->m_numCols, readCols);
	scan->setCurrentRid(rid1);
	deepCopyRecord(scan->getRecord(), redRec1);
	tntVisible = m_records->getRecord(scan, 0, &ntseVisible);
	CPPUNIT_ASSERT(tntVisible == false);
	CPPUNIT_ASSERT(ntseVisible == false);
	m_table->endScan(scan);
	commitTrx(trxSys, trx1);

	//ntse不可见
	u64 trxIds2[] = {txnId1};
	TNTTransaction *trx2 = startTrx(trxSys, m_conn, txnId1 + 10, TL_NO, false, trxIds2, 1);
	m_session->setTrans(trx2);
	initTNTOpInfo(scanOpInfo, TL_NO);
	scan = m_table->positionScan(m_session, OP_READ, &scanOpInfo, m_tableDef->m_numCols, readCols);
	scan->setCurrentRid(rid1);
	tntVisible = m_records->getRecord(scan, version, &ntseVisible);
	CPPUNIT_ASSERT(tntVisible == false);
	CPPUNIT_ASSERT(ntseVisible == false);
	m_table->endScan(scan);
	commitTrx(trxSys, trx2);

	//ntse可见
	TNTTransaction *trx3 = startTrx(trxSys, m_conn, txnId1 + 10);
	m_session->setTrans(trx3);
	initTNTOpInfo(scanOpInfo, TL_NO);
	scan = m_table->positionScan(m_session, OP_READ, &scanOpInfo, m_tableDef->m_numCols, readCols);
	scan->setCurrentRid(rid1);
	tntVisible = m_records->getRecord(scan, version, &ntseVisible);
	CPPUNIT_ASSERT(tntVisible == false);
	CPPUNIT_ASSERT(ntseVisible == true);
	m_table->endScan(scan);
	commitTrx(trxSys, trx3);

	//第二条tnt记录可见
	TNTTransaction *trx4 = startTrx(trxSys, m_conn, (txnId2 + txnId3)/2);
	m_session->setTrans(trx4);
	initTNTOpInfo(scanOpInfo, TL_NO);
	scan = m_table->positionScan(m_session, OP_READ, &scanOpInfo, m_tableDef->m_numCols, readCols);
	scan->setCurrentRid(rid1);
	tntVisible = m_records->getRecord(scan, version, &ntseVisible);
	CPPUNIT_ASSERT(tntVisible == true);
	Record *redRec2 = scan->getRecord();
	confirmRedRecord(m_tableDef, redRec2, name1, stuNo2, age2, sex1, clsNo1, grade1);
	m_table->endScan(scan);
	commitTrx(trxSys, trx4);

	//第三条tnt记录可见
	TNTTransaction *trx5 = startTrx(trxSys, m_conn, txnId3 + 10);
	m_session->setTrans(trx5);
	initTNTOpInfo(scanOpInfo, TL_NO);
	scan = m_table->positionScan(m_session, OP_READ, &scanOpInfo, m_tableDef->m_numCols, readCols);
	scan->setCurrentRid(rid1);
	tntVisible = m_records->getRecord(scan, version, &ntseVisible);
	CPPUNIT_ASSERT(tntVisible == false);
	CPPUNIT_ASSERT(ntseVisible == false);
	m_table->endScan(scan);
	commitTrx(trxSys, trx5);

	freeSubRecord(updateSub1);
	delete [] redRec1->m_data;
	delete redRec1;
	ctx->resetToSavepoint(sp);
}

void MRecordsTestCase::testFixLenUndoInsert() {
	initTable(true);
	testUndoInsert();
	shutDownTable();
}

void MRecordsTestCase::testVarLenUndoInsert() {
	initTable(false);
	testUndoInsert();
	shutDownTable();
}

void MRecordsTestCase::testUndoInsert() {
	MemoryContext *ctx = m_session->getMemoryContext();
	u64 sp = ctx->setSavepoint();

	RowId rid = 10008;
	u64 txnId1 = 10005;
	const char *name1 = "netease1";
	u32 stuNo1 = 5001;
	u16 age1 = 25;
	const char *sex1 = "F";
	u32 clsNo1 = 3001;
	u64 grade1 = 8001;
	RowId rid1 = 10008;
	Record *redRec1 = createStudentRec(rid, name1, stuNo1, age1, sex1, clsNo1, grade1);
	CPPUNIT_ASSERT(m_records->insert(m_session, txnId1, rid));
	m_records->undoInsert(m_session, rid);
	HashIndexEntry *entry = m_records->m_hashIndex->get(rid, ctx);
	CPPUNIT_ASSERT(entry == NULL);
	delete[] redRec1->m_data;
	delete redRec1;

	ctx->resetToSavepoint(sp);
}

void MRecordsTestCase::testFixLenUndoUpdate() {
	initTable(true);
	testUndoUpdate();
	shutDownTable();
}

void MRecordsTestCase::testVarLenUndoUpdate() {
	initTable(false);
	testUndoUpdate();
	shutDownTable();
}

void MRecordsTestCase::testUndoUpdate() {
	TNTOpInfo scanOpInfo;
	TNTTblScan *scan = NULL;
	TNTTrxSys *trxSys = m_db->getTransSys();

	bool ntseVisible = true;
	bool tntVisible = true;
	LsnType beginLsn;

	u8 vTblIndex = 0;
	u16 version = m_records->getVersion();
	MemoryContext *ctx = m_session->getMemoryContext();
	u64 sp = ctx->setSavepoint();
	u16 *readCols = (u16 *)ctx->alloc(sizeof(u16) * m_tableDef->m_numCols);
	for (u16 i = 0; i < m_tableDef->m_numCols; ++i) {
		readCols[i] = i;
	}

	u64 txnId1 = 10005;
	const char *name1 = "netease1";
	u32 stuNo1 = 5001;
	u16 age1 = 25;
	const char *sex1 = "F";
	u32 clsNo1 = 3001;
	u64 grade1 = 8001;
	RowId rid = 10008;
	Record *redRec1 = createStudentRec(rid, name1, stuNo1, age1, sex1, clsNo1, grade1);
	SubRecord allRedSub1(redRec1->m_format, m_tableDef->m_numCols, readCols, redRec1->m_data, redRec1->m_size, redRec1->m_rowId);
	CPPUNIT_ASSERT(m_records->insert(m_session, txnId1, rid));

	std::vector<u16> columns;
	std::vector<void *> datas;

	//第一次更新
	u64 txnId2 = 20005;
	u32 stuNo2 = 5002;
	u16 age2 = 28;
	columns.push_back(1);
	datas.push_back(&stuNo2);
	columns.push_back(2);
	datas.push_back(&age2);
	SubRecord *updateSub1 = createRedSubRecord(m_tableDef, columns, datas);
	RecordOper::mergeSubRecordRR(m_tableDef, updateSub1, &allRedSub1);

	TNTTransaction *updateTrx1 = startTrx(trxSys, m_conn, txnId2, TL_X, true);
	m_session->setTrans(updateTrx1);
	initTNTOpInfo(scanOpInfo, TL_X);
	scan = m_table->positionScan(m_session, OP_UPDATE, &scanOpInfo, m_tableDef->m_numCols, readCols);
	scan->setCurrentRid(rid);
	deepCopyRecord(scan->getRecord(), redRec1);
	CPPUNIT_ASSERT(m_records->prepareUD(scan));
	CPPUNIT_ASSERT(m_records->update(scan, updateSub1));
	m_table->endScan(scan);
	beginLsn = updateTrx1->getTrxBeginLsn();
	commitTrx(trxSys, updateTrx1);

	//第二次更新
	u64 txnId3 = 30005;
	u16 age3 = 29;
	u32 clsNo3 = 5003;
	columns.clear();
	datas.clear();
	columns.push_back(2);
	datas.push_back(&age3);
	columns.push_back(4);
	datas.push_back(&clsNo3);
	SubRecord *updateSub2 = createRedSubRecord(m_tableDef, columns, datas);
	allRedSub1.m_data = updateSub1->m_data;
	RecordOper::mergeSubRecordRR(m_tableDef, updateSub2, &allRedSub1);
	
	TNTTransaction *updateTrx2 = startTrx(trxSys, m_conn, txnId3, TL_X, true);
	m_session->setTrans(updateTrx2);
	initTNTOpInfo(scanOpInfo, TL_X);
	scan = m_table->positionScan(m_session, OP_UPDATE, &scanOpInfo, m_tableDef->m_numCols, readCols);
	scan->setCurrentRid(rid);
	CPPUNIT_ASSERT(!m_records->prepareUD(scan));
	CPPUNIT_ASSERT(m_records->update(scan, updateSub2));
	m_table->endScan(scan);
	commitTrx(trxSys, updateTrx2);
	freeSubRecord(updateSub1);
	freeSubRecord(updateSub2);

	//测试当前读
	TNTTransaction *trx1 = startTrx(trxSys, m_conn, txnId3 + 1, TL_S);
	m_session->setTrans(trx1);
	initTNTOpInfo(scanOpInfo, TL_S);
	scan = m_table->positionScan(m_session, OP_READ, &scanOpInfo, m_tableDef->m_numCols, readCols);
	scan->setCurrentRid(rid);
	deepCopyRecord(scan->getRecord(), redRec1);
	tntVisible = m_records->getRecord(scan, 0, &ntseVisible);
	CPPUNIT_ASSERT(tntVisible == true);
	Record *redRec2 = scan->getRecord();
	confirmRedRecord(m_tableDef, redRec2, name1, stuNo2, age3, sex1, clsNo3, grade1);
	m_table->endScan(scan);
	commitTrx(trxSys, trx1);

	// 检查日志并做undo
	LogScanHandle *logHdl = m_db->getTNTLog()->beginScan(beginLsn, Txnlog::MAX_LSN);
	CPPUNIT_ASSERT(getNextTNTLog(m_db->getTNTLog(), logHdl));
	CPPUNIT_ASSERT(logHdl->logEntry()->m_logType == TNT_BEGIN_TRANS);
	CPPUNIT_ASSERT(getNextTNTLog(m_db->getTNTLog(), logHdl));
	CPPUNIT_ASSERT(logHdl->logEntry()->m_logType == TNT_U_I_LOG);
	LogEntry *firUpdateLog = TNTDatabase::copyLog(logHdl->logEntry(), m_session->getMemoryContext());
	CPPUNIT_ASSERT(getNextTNTLog(m_db->getTNTLog(), logHdl));
	CPPUNIT_ASSERT(logHdl->logEntry()->m_logType == TNT_COMMIT_TRANS);

	CPPUNIT_ASSERT(getNextTNTLog(m_db->getTNTLog(), logHdl));
	CPPUNIT_ASSERT(logHdl->logEntry()->m_logType == TNT_BEGIN_TRANS);
	CPPUNIT_ASSERT(getNextTNTLog(m_db->getTNTLog(), logHdl));
	CPPUNIT_ASSERT(logHdl->logEntry()->m_logType == TNT_U_U_LOG);
	LogEntry *secUpdateLog = TNTDatabase::copyLog(logHdl->logEntry(), m_session->getMemoryContext());
	CPPUNIT_ASSERT(getNextTNTLog(m_db->getTNTLog(), logHdl));
	CPPUNIT_ASSERT(logHdl->logEntry()->m_logType == TNT_COMMIT_TRANS);
	CPPUNIT_ASSERT(!getNextTNTLog(m_db->getTNTLog(), logHdl));
	m_db->getTNTLog()->endScan(logHdl);

	//测试undoSecUpdate
	m_table->undoSecUpdate(m_session, secUpdateLog, false);

	//测试当前读
	TNTTransaction *trx2 = startTrx(trxSys, m_conn, txnId3 + 2, TL_S);
	m_session->setTrans(trx2);
	initTNTOpInfo(scanOpInfo, TL_S);
	scan = m_table->positionScan(m_session, OP_READ, &scanOpInfo, m_tableDef->m_numCols, readCols);
	scan->setCurrentRid(rid);
	deepCopyRecord(scan->getRecord(), redRec1);
	tntVisible = m_records->getRecord(scan, 0, &ntseVisible);
	CPPUNIT_ASSERT(tntVisible == true);
	Record *redRec3 = scan->getRecord();
	confirmRedRecord(m_tableDef, redRec3, name1, stuNo2, age2, sex1, clsNo1, grade1);
	m_table->endScan(scan);
	commitTrx(trxSys, trx2);

	//测试undoFirUpdate
	m_table->undoFirUpdate(m_session, firUpdateLog, false);

	//当前读，tnt不可见，ntse可见
	TNTTransaction *trx3 = startTrx(trxSys, m_conn, txnId3 + 2, TL_S);
	m_session->setTrans(trx3);
	initTNTOpInfo(scanOpInfo, TL_S);
	scan = m_table->positionScan(m_session, OP_READ, &scanOpInfo, m_tableDef->m_numCols, readCols);
	scan->setCurrentRid(rid);
	deepCopyRecord(scan->getRecord(), redRec1);
	tntVisible = m_records->getRecord(scan, 0, &ntseVisible);
	CPPUNIT_ASSERT(tntVisible == false);
	CPPUNIT_ASSERT(ntseVisible == true);
	m_table->endScan(scan);
	commitTrx(trxSys, trx3);
	
	delete [] redRec1->m_data;
	delete redRec1;
	ctx->resetToSavepoint(sp);
}

void MRecordsTestCase::testFixLenUndoFirRemove() {
	initTable(true);
	testUndoFirRemove();
	shutDownTable();
}

void MRecordsTestCase::testVarLenUndoFirRemove() {
	initTable(false);
	testUndoFirRemove();
	shutDownTable();
}

void MRecordsTestCase::testUndoFirRemove() {
	TNTOpInfo scanOpInfo;
	TNTTblScan *scan = NULL;
	TNTTrxSys *trxSys = m_db->getTransSys();
	bool ntseVisible = true;
	bool tntVisible = true;
	LsnType beginLsn;

	u8 vTblIndex = 0;
	u16 version = m_records->getVersion();
	MemoryContext *ctx = m_session->getMemoryContext();
	u64 sp = ctx->setSavepoint();
	u16 *readCols = (u16 *)ctx->alloc(sizeof(u16) * m_tableDef->m_numCols);
	for (u16 i = 0; i < m_tableDef->m_numCols; ++i) {
		readCols[i] = i;
	}

	u64 txnId1 = 10005;
	const char *name1 = "netease1";
	u32 stuNo1 = 5001;
	u16 age1 = 25;
	const char *sex1 = "F";
	u32 clsNo1 = 3001;
	u64 grade1 = 8001;
	RowId rid1 = 10008;
	Record *redRec1 = createStudentRec(rid1, name1, stuNo1, age1, sex1, clsNo1, grade1);
	CPPUNIT_ASSERT(m_records->insert(m_session, txnId1, rid1));

	//然后对该记录进行删除
	u64 txnId2 = 30005;
	TNTTransaction *delTrx1 = startTrx(trxSys, m_conn, txnId2, TL_X, true);
	m_session->setTrans(delTrx1);
	initTNTOpInfo(scanOpInfo, TL_X);
	scan = m_table->positionScan(m_session, OP_DELETE, &scanOpInfo, m_tableDef->m_numCols, readCols);
	scan->setCurrentRid(rid1);
	deepCopyRecord(scan->getRecord(), redRec1);
	CPPUNIT_ASSERT(m_records->prepareUD(scan));
	CPPUNIT_ASSERT(m_records->remove(scan));
	m_table->endScan(scan);
	beginLsn = delTrx1->getTrxBeginLsn();
	commitTrx(trxSys, delTrx1);

	//测试当前读，ntse和tnt都不可见
	TNTTransaction *trx1 = startTrx(trxSys, m_conn, txnId2 + 1, TL_S);
	m_session->setTrans(trx1);
	initTNTOpInfo(scanOpInfo, TL_S);
	scan = m_table->positionScan(m_session, OP_READ, &scanOpInfo, m_tableDef->m_numCols, readCols);
	scan->setCurrentRid(rid1);
	deepCopyRecord(scan->getRecord(), redRec1);

	tntVisible = m_records->getRecord(scan, 0, &ntseVisible);
	CPPUNIT_ASSERT(tntVisible == false);
	CPPUNIT_ASSERT(ntseVisible == false);
	m_table->endScan(scan);
	commitTrx(trxSys, trx1);

	// 检查日志并做redo
	LogScanHandle *logHdl = m_db->getTNTLog()->beginScan(beginLsn, Txnlog::MAX_LSN);
	CPPUNIT_ASSERT(getNextTNTLog(m_db->getTNTLog(), logHdl));
	CPPUNIT_ASSERT(logHdl->logEntry()->m_logType == TNT_BEGIN_TRANS);
	CPPUNIT_ASSERT(getNextTNTLog(m_db->getTNTLog(), logHdl));
	CPPUNIT_ASSERT(logHdl->logEntry()->m_logType == TNT_D_I_LOG);
	m_table->undoFirRemove(m_session, logHdl->logEntry(), false);
	CPPUNIT_ASSERT(getNextTNTLog(m_db->getTNTLog(), logHdl));
	CPPUNIT_ASSERT(logHdl->logEntry()->m_logType == TNT_COMMIT_TRANS);
	CPPUNIT_ASSERT(!getNextTNTLog(m_db->getTNTLog(), logHdl));
	m_db->getTNTLog()->endScan(logHdl);

	//当前读，ntse可见，tnt不可见
	TNTTransaction *trx2 = startTrx(trxSys, m_conn, txnId2 + 2, TL_S);
	m_session->setTrans(trx2);
	initTNTOpInfo(scanOpInfo, TL_S);
	scan = m_table->positionScan(m_session, OP_READ, &scanOpInfo, m_tableDef->m_numCols, readCols);
	scan->setCurrentRid(rid1);
	tntVisible = m_records->getRecord(scan, version, &ntseVisible);
	CPPUNIT_ASSERT(tntVisible == false);
	CPPUNIT_ASSERT(ntseVisible == true);
	m_table->endScan(scan);
	commitTrx(trxSys, trx2);

	delete [] redRec1->m_data;
	delete redRec1;
	ctx->resetToSavepoint(sp);
}

void MRecordsTestCase::testFixLenUndoSecRemove() {
	initTable(true);
	testUndoSecRemove();
	shutDownTable();
}

void MRecordsTestCase::testVarLenUndoSecRemove() {
	initTable(false);
	testUndoSecRemove();
	shutDownTable();
}

void MRecordsTestCase::testUndoSecRemove() {
	TNTOpInfo scanOpInfo;
	TNTTblScan *scan = NULL;
	TNTTrxSys *trxSys = m_db->getTransSys();
	bool ntseVisible = true;
	bool tntVisible = true;
	TrxId nextTxnId = 0;
	LsnType beginLsn;

	u8 vTblIndex = 0;
	u16 version = m_records->getVersion();
	MemoryContext *ctx = m_session->getMemoryContext();
	u64 sp = ctx->setSavepoint();
	u16 *readCols = (u16 *)ctx->alloc(sizeof(u16) * m_tableDef->m_numCols);
	for (u16 i = 0; i < m_tableDef->m_numCols; ++i) {
		readCols[i] = i;
	}

	u64 txnId1 = 10005;
	const char *name1 = "netease1";
	u32 stuNo1 = 5001;
	u16 age1 = 25;
	const char *sex1 = "F";
	u32 clsNo1 = 3001;
	u64 grade1 = 8001;
	RowId rid1 = 10008;
	Record *redRec1 = createStudentRec(rid1, name1, stuNo1, age1, sex1, clsNo1, grade1);
	SubRecord allRedSub1(redRec1->m_format, m_tableDef->m_numCols, readCols, redRec1->m_data, redRec1->m_size, redRec1->m_rowId);
	CPPUNIT_ASSERT(m_records->insert(m_session, txnId1, rid1));

	std::vector<u16> columns;
	std::vector<void *> datas;

	//第一次更新
	u64 txnId2 = 20005;
	u32 stuNo2 = 5002;
	u16 age2 = 28;
	columns.push_back(1);
	datas.push_back(&stuNo2);
	columns.push_back(2);
	datas.push_back(&age2);
	SubRecord *updateSub1 = createRedSubRecord(m_tableDef, columns, datas);
	RecordOper::mergeSubRecordRR(m_tableDef, updateSub1, &allRedSub1);

	TNTTransaction *updateTrx1 = startTrx(trxSys, m_conn, txnId2, TL_X);
	m_session->setTrans(updateTrx1);
	initTNTOpInfo(scanOpInfo, TL_X);
	scan = m_table->positionScan(m_session, OP_UPDATE, &scanOpInfo, m_tableDef->m_numCols, readCols);
	scan->setCurrentRid(rid1);
	deepCopyRecord(scan->getRecord(), redRec1);
	CPPUNIT_ASSERT(m_records->prepareUD(scan));
	CPPUNIT_ASSERT(m_records->update(scan, updateSub1));
	m_table->endScan(scan);
	nextTxnId = updateTrx1->getTrxId() + 1;
	beginLsn = updateTrx1->getTrxBeginLsn();
	commitTrx(trxSys, updateTrx1);
	freeSubRecord(updateSub1);

	//然后对该记录进行删除
	u64 txnId3 = 30005;
	TNTTransaction *delTrx1 = startTrx(trxSys, m_conn, txnId3, TL_X, true);
	m_session->setTrans(delTrx1);
	initTNTOpInfo(scanOpInfo, TL_X);
	scan = m_table->positionScan(m_session, OP_DELETE, &scanOpInfo, m_tableDef->m_numCols, readCols);
	scan->setCurrentRid(rid1);
	CPPUNIT_ASSERT(!m_records->prepareUD(scan));
	CPPUNIT_ASSERT(m_records->remove(scan));
	m_table->endScan(scan);
	nextTxnId = delTrx1->getTrxId() + 1;
	beginLsn = delTrx1->getTrxBeginLsn();
	commitTrx(trxSys, delTrx1);

	//测试当前读，ntse和tnt都不可见
	TNTTransaction *trx1 = startTrx(trxSys, m_conn, nextTxnId, TL_S);
	m_session->setTrans(trx1);
	initTNTOpInfo(scanOpInfo, TL_S);
	scan = m_table->positionScan(m_session, OP_READ, &scanOpInfo, m_tableDef->m_numCols, readCols);
	scan->setCurrentRid(rid1);
	tntVisible = m_records->getRecord(scan, 0, &ntseVisible);
	CPPUNIT_ASSERT(tntVisible == false);
	CPPUNIT_ASSERT(ntseVisible == false);
	m_table->endScan(scan);
	nextTxnId = trx1->getTrxId() + 1;
	commitTrx(trxSys, trx1);

	// 检查日志并做redo
	LogScanHandle *logHdl = m_db->getTNTLog()->beginScan(beginLsn, Txnlog::MAX_LSN);
	CPPUNIT_ASSERT(getNextTNTLog(m_db->getTNTLog(), logHdl));
	CPPUNIT_ASSERT(logHdl->logEntry()->m_logType == TNT_BEGIN_TRANS);
	CPPUNIT_ASSERT(getNextTNTLog(m_db->getTNTLog(), logHdl));
	CPPUNIT_ASSERT(logHdl->logEntry()->m_logType == TNT_D_U_LOG);
	m_table->undoSecRemove(m_session, logHdl->logEntry(), false);
	CPPUNIT_ASSERT(getNextTNTLog(m_db->getTNTLog(), logHdl));
	CPPUNIT_ASSERT(logHdl->logEntry()->m_logType == TNT_COMMIT_TRANS);
	CPPUNIT_ASSERT(!getNextTNTLog(m_db->getTNTLog(), logHdl));
	m_db->getTNTLog()->endScan(logHdl);

	//测试当前读，tnt可见
	TNTTransaction *trx2 = startTrx(trxSys, m_conn, nextTxnId, TL_S);
	m_session->setTrans(trx2);
	initTNTOpInfo(scanOpInfo, TL_S);
	scan = m_table->positionScan(m_session, OP_READ, &scanOpInfo, m_tableDef->m_numCols, readCols);
	scan->setCurrentRid(rid1);
	tntVisible = m_records->getRecord(scan, 0, &ntseVisible);
	CPPUNIT_ASSERT(tntVisible == true);
	Record *redRec2 = scan->getRecord();
	confirmRedRecord(m_tableDef, redRec2, name1, stuNo2, age2, sex1, clsNo1, grade1);
	m_table->endScan(scan);
	nextTxnId = trx2->getTrxId() + 1;
	commitTrx(trxSys, trx2);

	delete [] redRec1->m_data;
	delete redRec1;
	ctx->resetToSavepoint(sp);
}

void MRecordsTestCase::testFixLenDump() {
	initTable(true);
	testDump();
	shutDownTable();
}

void MRecordsTestCase::testVarLenDump() {
	initTable(false);
	testDump();
	shutDownTable();
}

void MRecordsTestCase::testDump() {
	TNTOpInfo scanOpInfo;
	TNTTblScan *scan = NULL;
	TrxId nextTxnId = 0;
	u8 vTblIndex = 0;
	u16 version = m_records->getVersion();
	MemoryContext *ctx = m_session->getMemoryContext();
	McSavepoint msp(ctx);
	u16 *readCols = (u16 *)ctx->alloc(sizeof(u16) * m_tableDef->m_numCols);
	for (u16 i = 0; i < m_tableDef->m_numCols; ++i) {
		readCols[i] = i;
	}

	u64 sp1 = ctx->setSavepoint();
	u32 totalRec = 2000;
	u32 i = 0;
	RecAndFieldVal *recs = new RecAndFieldVal[totalRec];
	u64 txnId1 = 10005;

	for (i = 0; i < totalRec; i++) {
		recs[i].m_name = new char[30];
		sprintf(recs[i].m_name, "netease%d", i);
		recs[i].m_stuNo = 5001 + i;
		recs[i].m_age = 25 + (i % 10);
		recs[i].m_sex = (char *)(i % 2 == 0 ?"F": "M");
		recs[i].m_clsNo = 3001 + i;
		recs[i].m_grade = 8001 + (i % 5);
		recs[i].m_rid = 10008 + i;
		recs[i].m_redRec = createStudentRec(recs[i].m_rid, recs[i].m_name, recs[i].m_stuNo, recs[i].m_age, recs[i].m_sex, recs[i].m_clsNo, recs[i].m_grade);
		CPPUNIT_ASSERT(m_records->insert(m_session, txnId1, recs[i].m_rid));
	}

	std::vector<u16> columns;
	std::vector<void *> datas;

	//第一次更新
	u64 txnId2 = 20005;
	columns.push_back(1);
	columns.push_back(2);
	
	TNTTrxSys *trxSys = m_db->getTransSys();
	TNTTransaction *updateTrx1 = startTrx(trxSys, m_conn, txnId2, TL_X);
	m_session->setTrans(updateTrx1);
	initTNTOpInfo(scanOpInfo, TL_X);
	for (i = 0; i < totalRec; i++) {
		SubRecord allRedSub(recs[i].m_redRec->m_format, m_tableDef->m_numCols, readCols, recs[i].m_redRec->m_data, recs[i].m_redRec->m_size, recs[i].m_redRec->m_rowId);
		recs[i].m_stuNo = recs[i].m_stuNo - 500;
		recs[i].m_age = recs[i].m_age + 1;
		datas.push_back(&recs[i].m_stuNo);
		datas.push_back(&recs[i].m_age);
		SubRecord *updateSub = createRedSubRecord(m_tableDef, columns, datas);
		RecordOper::mergeSubRecordRR(m_tableDef, updateSub, &allRedSub);

		scan = m_table->positionScan(m_session, OP_UPDATE, &scanOpInfo, m_tableDef->m_numCols, readCols);
		scan->setCurrentRid(recs[i].m_rid);
		deepCopyRecord(scan->getRecord(), recs[i].m_redRec);
		CPPUNIT_ASSERT(m_records->prepareUD(scan));
		CPPUNIT_ASSERT(m_records->update(scan, updateSub));
		m_table->endScan(scan);

		datas.clear();
		freeSubRecord(updateSub);
	}
	nextTxnId = updateTrx1->getTrxId() + 1;
	commitTrx(trxSys, updateTrx1);

	u64 txnId3 = 30005;
	TNTTransaction *delTrx1 = startTrx(trxSys, m_conn, txnId3, TL_X);
	m_session->setTrans(delTrx1);
	initTNTOpInfo(scanOpInfo, TL_X);
	for (i = 0; i < (totalRec >> 1); i++) {
		scan = m_table->positionScan(m_session, OP_DELETE, &scanOpInfo, m_tableDef->m_numCols, readCols);
		scan->setCurrentRid(recs[i].m_rid);
		CPPUNIT_ASSERT(!m_records->prepareUD(scan));
		CPPUNIT_ASSERT(m_records->remove(scan));
		m_table->endScan(scan);
	}
	nextTxnId = delTrx1->getTrxId() + 1;
	commitTrx(trxSys, delTrx1);
	
	//执行dump操作
	char dumpFilePath[50];
	sprintf(dumpFilePath, "%s/dumpFile.dat", m_db->getConfig()->m_dumpdir);
	File *file = new File(dumpFilePath);
	u64 offset = 0;
	CPPUNIT_ASSERT(file->create(false, false) == File::E_NO_ERROR);
	CPPUNIT_ASSERT(file->setSize(MRecordPage::TNTIM_PAGE_SIZE) == File::E_NO_ERROR);
	u64 txnId4 = (txnId2 + txnId3) >> 1;
	TNTTransaction *dumpTrx = startTrx(trxSys, m_conn, txnId4);
	EXCPT_OPER(m_records->dump(m_session, dumpTrx->getReadView(), file, &offset));
	file->close();
	commitTrx(trxSys, dumpTrx);
	ctx->resetToSavepoint(sp1);

	//关闭并重新open
	reOpenAllTable();
	sp1 = ctx->setSavepoint();
	CPPUNIT_ASSERT(file->open(false) == File::E_NO_ERROR);
	offset = 0;
	CPPUNIT_ASSERT(m_records->readDump(m_session, file, &offset));
	file->close();
	delete file;

	//测试当前读
	bool tntVisible, ntseVisible;
	Record *redRec = NULL;
	TNTTransaction *trx1 = startTrx(trxSys, m_conn, nextTxnId, TL_S);
	m_session->setTrans(trx1);
	initTNTOpInfo(scanOpInfo, TL_S);
	for (i = 0; i < totalRec; i++) {
		scan = m_table->positionScan(m_session, OP_READ, &scanOpInfo, m_tableDef->m_numCols, readCols);
		scan->setCurrentRid(recs[i].m_rid);
		deepCopyRecord(scan->getRecord(), recs[i].m_redRec);
		tntVisible = m_records->getRecord(scan, 0, &ntseVisible);
		CPPUNIT_ASSERT(tntVisible == true);
		redRec = scan->getRecord();
		confirmRedRecord(m_tableDef, redRec, recs[i].m_name, recs[i].m_stuNo, recs[i].m_age, recs[i].m_sex, recs[i].m_clsNo, recs[i].m_grade);
		m_table->endScan(scan);
	}
	commitTrx(trxSys, trx1);

	//释放内存
	//释放内存
	for (i = 0; i < totalRec; i++) {
		delete[] recs[i].m_redRec->m_data;
		delete recs[i].m_redRec;
		delete[] recs[i].m_name;
	}

	delete[] recs;

	ctx->resetToSavepoint(sp1);
}

//更新过程中在各个点上发生记录移动整理，测试更新是否能成功完成
void UpdateDefragThread::run() {
	static int i = 50;
	TNTOpInfo scanOpInfo;
	TNTTblScan *scan = NULL;
	bool ntseVisible = true;
	bool tntVisible = true;
	u16 version = m_table->getMRecords()->getVersion();
	Database *db = m_db->getNtseDb();
	Connection *conn = db->getConnection(false);
	Session *session = db->getSessionManager()->allocSession("UpdateDefragThread::run", conn);

	MemoryContext *ctx = session->getMemoryContext();
	McSavepoint msp(ctx);
	u16 *readCols = (u16 *)ctx->alloc(sizeof(u16) * m_tableDef->m_numCols);
	for (u16 i = 0; i < m_tableDef->m_numCols; ++i) {
		readCols[i] = i;
	}

	SubRecord allRedSub1(m_rec->m_redRec->m_format, m_tableDef->m_numCols, readCols, m_rec->m_redRec->m_data, m_rec->m_redRec->m_size, m_rec->m_redRec->m_rowId);
	//更新记录
	u64 txnId = 20005 + 10*i;
	TNTTrxSys *trxSys = m_db->getTransSys();
	m_rec->m_stuNo = m_rec->m_stuNo + i;
	m_rec->m_age = m_rec->m_age + i % 9;
	std::vector<u16> columns;
	std::vector<void *> datas;
	columns.push_back(1);
	columns.push_back(2);
	datas.push_back(&m_rec->m_stuNo);
	datas.push_back(&m_rec->m_age);
	SubRecord *updateSub1 = MRecordsTestCase::createRedSubRecord(m_tableDef, columns, datas);
	RecordOper::mergeSubRecordRR(m_tableDef, updateSub1, &allRedSub1);

	TNTTransaction *updateTrx = MRecordsTestCase::startTrx(trxSys, conn, txnId, TL_X);
	session->setTrans(updateTrx);
	MRecordsTestCase::initTNTOpInfo(scanOpInfo, TL_X);
	scan = m_table->positionScan(session, OP_UPDATE, &scanOpInfo, m_tableDef->m_numCols, readCols);
	scan->setCurrentRid(m_rec->m_rid);
	MRecordsTestCase::deepCopyRecord(scan->getRecord(), m_rec->m_redRec);
	CPPUNIT_ASSERT(!m_table->getMRecords()->prepareUD(scan));
	CPPUNIT_ASSERT(m_table->getMRecords()->update(scan, updateSub1));
	m_table->endScan(scan);
	MRecordsTestCase::commitTrx(trxSys, updateTrx);
	freeSubRecord(updateSub1);

	//测试当前读
	TNTTransaction *trx1 = MRecordsTestCase::startTrx(trxSys, conn, txnId + 1, TL_S);
	session->setTrans(trx1);
	MRecordsTestCase::initTNTOpInfo(scanOpInfo, TL_S);
	scan = m_table->positionScan(session, OP_READ, &scanOpInfo, m_tableDef->m_numCols, readCols);
	scan->setCurrentRid(m_rec->m_rid);
	MRecordsTestCase::deepCopyRecord(scan->getRecord(), m_rec->m_redRec);
	tntVisible = m_table->getMRecords()->getRecord(scan, 0, &ntseVisible);
	CPPUNIT_ASSERT(tntVisible == true);
	MRecordsTestCase::confirmRedRecord(m_tableDef, scan->getRecord(), m_rec->m_name, m_rec->m_stuNo, m_rec->m_age, m_rec->m_sex, m_rec->m_clsNo, m_rec->m_grade);
	m_table->endScan(scan);
	MRecordsTestCase::commitTrx(trxSys, trx1);

	db->getSessionManager()->freeSession(session);
	db->freeConnection(conn);

	i++;
}

void MRecordsTestCase::testFixLenUpdateDefrag() {
	initTable(true);
	testUpdateDefrag();
	shutDownTable();
}

void MRecordsTestCase::testVarLenUpdateDefrag() {
	initTable(false);
	testUpdateDefrag();
	shutDownTable();
}

void MRecordsTestCase::testUpdateDefrag() {
	TNTOpInfo scanOpInfo;
	TNTTblScan *scan = NULL;
	u8 vTblIndex = 0;
	u16 version = m_records->getVersion();
	MemoryContext *ctx = m_session->getMemoryContext();
	McSavepoint msp(ctx);
	u16 *readCols = (u16 *)ctx->alloc(sizeof(u16) * m_tableDef->m_numCols);
	for (u16 i = 0; i < m_tableDef->m_numCols; ++i) {
		readCols[i] = i;
	}

	u64 sp1 = ctx->setSavepoint();
	u32 totalRec = 1000;
	u32 i = 0;
	RecAndFieldVal *recs = new RecAndFieldVal[totalRec];

	u64 txnId1 = 10005;

	for (i = 0; i < totalRec; i++) {
		recs[i].m_name = new char[30];
		sprintf(recs[i].m_name, "netease%d", i);
		recs[i].m_stuNo = 5001 + i;
		recs[i].m_age = 25 + (i % 10);
		recs[i].m_sex = (char *)(i % 2 == 0 ?"F": "M");
		recs[i].m_clsNo = 3001 + i;
		recs[i].m_grade = 8001 + (i % 5);
		recs[i].m_rid = 10008 + i;
		recs[i].m_redRec = createStudentRec(recs[i].m_rid, recs[i].m_name, recs[i].m_stuNo, recs[i].m_age, recs[i].m_sex, recs[i].m_clsNo, recs[i].m_grade);
		CPPUNIT_ASSERT(m_records->insert(m_session, txnId1, recs[i].m_rid));
	}

	std::vector<u16> columns;
	std::vector<void *> datas;

	//第一次更新
	u64 txnId2 = 20005;
	columns.push_back(1);
	columns.push_back(2);
	
	TNTTrxSys *trxSys = m_db->getTransSys();
	TNTTransaction *updateTrx1 = startTrx(trxSys, m_conn, txnId2, TL_X);
	m_session->setTrans(updateTrx1);
	initTNTOpInfo(scanOpInfo, TL_X);
	for (i = 0; i < totalRec; i++) {
		SubRecord allRedSub1(recs[i].m_redRec->m_format, m_tableDef->m_numCols, readCols, recs[i].m_redRec->m_data, recs[i].m_redRec->m_size, recs[i].m_redRec->m_rowId);
		recs[i].m_stuNo = recs[i].m_stuNo - 500;
		recs[i].m_age = recs[i].m_age + 1;
		datas.push_back(&recs[i].m_stuNo);
		datas.push_back(&recs[i].m_age);
		SubRecord *updateSub = createRedSubRecord(m_tableDef, columns, datas);
		RecordOper::mergeSubRecordRR(m_tableDef, updateSub, &allRedSub1);

		scan = m_table->positionScan(m_session, OP_UPDATE, &scanOpInfo, m_tableDef->m_numCols, readCols);
		scan->setCurrentRid(recs[i].m_rid);
		deepCopyRecord(scan->getRecord(), recs[i].m_redRec);
		CPPUNIT_ASSERT(m_records->prepareUD(scan));
		CPPUNIT_ASSERT(m_records->update(scan, updateSub));
		m_table->endScan(scan);

		datas.clear();
		freeSubRecord(updateSub);
	}
	commitTrx(trxSys, updateTrx1);

	MRecordPage *page = NULL;
	HashIndexEntry *indexEntry = NULL;
	//在prepare阶段由于记录的整理，ptr被改变
	indexEntry = m_records->m_hashIndex->get(recs[0].m_rid, m_session->getMemoryContext());
	CPPUNIT_ASSERT(indexEntry != NULL);
	CPPUNIT_ASSERT(m_records->m_heap->removeHeapRecordAndHash(m_session, recs[0].m_rid));
	UpdateDefragThread updateDefragThread1(m_db, m_tableDef, m_table, recs + 1);
	updateDefragThread1.enableSyncPoint(SP_MRECS_PREPAREUD_PTR_MODIFY);
	updateDefragThread1.start();
	updateDefragThread1.joinSyncPoint(SP_MRECS_PREPAREUD_PTR_MODIFY);
	page = (MRecordPage *)GET_PAGE_HEADER((void *)indexEntry->m_value);
	page->defrag(m_records->m_hashIndex);
	updateDefragThread1.disableSyncPoint(SP_MRECS_PREPAREUD_PTR_MODIFY);
	updateDefragThread1.notifySyncPoint(SP_MRECS_PREPAREUD_PTR_MODIFY);
	updateDefragThread1.join(-1);

	//在update阶段由于记录的整理，ptr被改变
	indexEntry = m_records->m_hashIndex->get(recs[1].m_rid, m_session->getMemoryContext());
	CPPUNIT_ASSERT(m_records->m_heap->removeHeapRecordAndHash(m_session, recs[1].m_rid));
	UpdateDefragThread updateDefragThread2(m_db, m_tableDef, m_table, recs + 2);
	updateDefragThread2.enableSyncPoint(SP_MRECS_UPDATE_PTR_MODIFY);
	updateDefragThread2.start();
	updateDefragThread2.joinSyncPoint(SP_MRECS_UPDATE_PTR_MODIFY);
	page = (MRecordPage *)GET_PAGE_HEADER((void *)indexEntry->m_value);
	page->defrag(m_records->m_hashIndex);
	updateDefragThread2.disableSyncPoint(SP_MRECS_UPDATE_PTR_MODIFY);
	updateDefragThread2.notifySyncPoint(SP_MRECS_UPDATE_PTR_MODIFY);
	updateDefragThread2.join(-1);

	//在updateMem阶段由于记录的整理，ptr被改变
	indexEntry = m_records->m_hashIndex->get(recs[2].m_rid, m_session->getMemoryContext());
	CPPUNIT_ASSERT(m_records->m_heap->removeHeapRecordAndHash(m_session, recs[2].m_rid));
	UpdateDefragThread updateDefragThread3(m_db, m_tableDef, m_table, recs + 3);
	updateDefragThread3.enableSyncPoint(SP_MRECS_UPDATEMEM_PTR_MODIFY);
	updateDefragThread3.start();
	updateDefragThread3.joinSyncPoint(SP_MRECS_UPDATEMEM_PTR_MODIFY);
	page = (MRecordPage *)GET_PAGE_HEADER((void *)indexEntry->m_value);
	page->defrag(m_records->m_hashIndex);
	updateDefragThread3.disableSyncPoint(SP_MRECS_UPDATEMEM_PTR_MODIFY);
	updateDefragThread3.notifySyncPoint(SP_MRECS_UPDATEMEM_PTR_MODIFY);
	updateDefragThread3.join(-1);

	//释放内存
	for (i = 0; i < totalRec; i++) {
		delete[] recs[i].m_redRec->m_data;
		delete recs[i].m_redRec;
		delete[] recs[i].m_name;
	}

	delete[] recs;

	ctx->resetToSavepoint(sp1);
}

//删除过程中在各个点上发生记录移动整理，测试更新是否能成功完成
void RemoveDefragThread::run() {
	static int i = 50;
	TNTOpInfo scanOpInfo;
	TNTTblScan *scan = NULL;
	bool ntseVisible = true;
	bool tntVisible = true;
	u16 version = m_table->getMRecords()->getVersion();
	Database *db = m_db->getNtseDb();
	Connection *conn = db->getConnection(false);
	Session *session = db->getSessionManager()->allocSession("RemoveDefragThread::run", conn);

	MemoryContext *ctx = session->getMemoryContext();
	McSavepoint msp(ctx);
	u16 *readCols = (u16 *)ctx->alloc(sizeof(u16) * m_tableDef->m_numCols);
	for (u16 i = 0; i < m_tableDef->m_numCols; ++i) {
		readCols[i] = i;
	}

	//更新记录
	u64 txnId = 20005 + 10*i;
	TNTTrxSys *trxSys = m_db->getTransSys();
	TNTTransaction *removeTrx = MRecordsTestCase::startTrx(trxSys, conn, txnId, TL_X);
	session->setTrans(removeTrx);
	MRecordsTestCase::initTNTOpInfo(scanOpInfo, TL_X);
	scan = m_table->positionScan(session, OP_DELETE, &scanOpInfo, m_tableDef->m_numCols, readCols);
	scan->setCurrentRid(m_rec->m_rid);
	MRecordsTestCase::deepCopyRecord(scan->getRecord(), m_rec->m_redRec);
	CPPUNIT_ASSERT(!m_table->getMRecords()->prepareUD(scan));
	CPPUNIT_ASSERT(m_table->getMRecords()->remove(scan));
	m_table->endScan(scan);
	MRecordsTestCase::commitTrx(trxSys, removeTrx);

	//测试当前读
	TNTTransaction *trx1 = MRecordsTestCase::startTrx(trxSys, conn, txnId + 1, TL_S);
	session->setTrans(trx1);
	MRecordsTestCase::initTNTOpInfo(scanOpInfo, TL_S);
	scan = m_table->positionScan(session, OP_READ, &scanOpInfo, m_tableDef->m_numCols, readCols);
	scan->setCurrentRid(m_rec->m_rid);
	MRecordsTestCase::deepCopyRecord(scan->getRecord(), m_rec->m_redRec);
	tntVisible = m_table->getMRecords()->getRecord(scan, 0, &ntseVisible);
	CPPUNIT_ASSERT(tntVisible == false);
	CPPUNIT_ASSERT(ntseVisible == false);
	m_table->endScan(scan);
	MRecordsTestCase::commitTrx(trxSys, trx1);

	db->getSessionManager()->freeSession(session);
	db->freeConnection(conn);

	i++;
}

void MRecordsTestCase::testFixLenRemoveDefrag() {
	initTable(true);
	testRemoveDefrag();
	shutDownTable();
}

void MRecordsTestCase::testVarLenRemoveDefrag() {
	initTable(false);
	testRemoveDefrag();
	shutDownTable();
}

void MRecordsTestCase::testRemoveDefrag() {
	TNTOpInfo scanOpInfo;
	TNTTblScan *scan = NULL;
	u8 vTblIndex = 0;
	u16 version = m_records->getVersion();
	MemoryContext *ctx = m_session->getMemoryContext();
	McSavepoint msp(ctx);
	u16 *readCols = (u16 *)ctx->alloc(sizeof(u16) * m_tableDef->m_numCols);
	for (u16 i = 0; i < m_tableDef->m_numCols; ++i) {
		readCols[i] = i;
	}

	u64 sp1 = ctx->setSavepoint();
	u32 totalRec = 1000;
	u32 i = 0;
	RecAndFieldVal *recs = new RecAndFieldVal[totalRec];

	u64 txnId1 = 10005;

	for (i = 0; i < totalRec; i++) {
		recs[i].m_name = new char[30];
		sprintf(recs[i].m_name, "netease%d", i);
		recs[i].m_stuNo = 5001 + i;
		recs[i].m_age = 25 + (i % 10);
		recs[i].m_sex = (char *)(i % 2 == 0 ?"F": "M");
		recs[i].m_clsNo = 3001 + i;
		recs[i].m_grade = 8001 + (i % 5);
		recs[i].m_rid = 10008 + i;
		recs[i].m_redRec = createStudentRec(recs[i].m_rid, recs[i].m_name, recs[i].m_stuNo, recs[i].m_age, recs[i].m_sex, recs[i].m_clsNo, recs[i].m_grade);
		CPPUNIT_ASSERT(m_records->insert(m_session, txnId1, recs[i].m_rid));
	}

	std::vector<u16> columns;
	std::vector<void *> datas;

	//第一次更新
	u64 txnId2 = 20005;
	columns.push_back(1);
	columns.push_back(2);
	
	TNTTrxSys *trxSys = m_db->getTransSys();
	TNTTransaction *updateTrx1 = startTrx(trxSys, m_conn, txnId2, TL_X);
	m_session->setTrans(updateTrx1);
	initTNTOpInfo(scanOpInfo, TL_X);
	for (i = 0; i < totalRec; i++) {
		SubRecord allRedSub1(recs[i].m_redRec->m_format, m_tableDef->m_numCols, readCols, recs[i].m_redRec->m_data, recs[i].m_redRec->m_size, recs[i].m_redRec->m_rowId);
		recs[i].m_stuNo = recs[i].m_stuNo - 500;
		recs[i].m_age = recs[i].m_age + 1;
		datas.push_back(&recs[i].m_stuNo);
		datas.push_back(&recs[i].m_age);
		SubRecord *updateSub = createRedSubRecord(m_tableDef, columns, datas);
		RecordOper::mergeSubRecordRR(m_tableDef, updateSub, &allRedSub1);

		scan = m_table->positionScan(m_session, OP_UPDATE, &scanOpInfo, m_tableDef->m_numCols, readCols);
		scan->setCurrentRid(recs[i].m_rid);
		deepCopyRecord(scan->getRecord(), recs[i].m_redRec);
		CPPUNIT_ASSERT(m_records->prepareUD(scan));
		CPPUNIT_ASSERT(m_records->update(scan, updateSub));
		m_table->endScan(scan);

		datas.clear();
		freeSubRecord(updateSub);
	}
	commitTrx(trxSys, updateTrx1);

	MRecordPage *page = NULL;
	HashIndexEntry *indexEntry = NULL;
	//在prepare阶段由于记录的整理，ptr被改变
	indexEntry = m_records->m_hashIndex->get(recs[0].m_rid, m_session->getMemoryContext());
	CPPUNIT_ASSERT(m_records->m_heap->removeHeapRecordAndHash(m_session, recs[0].m_rid));
	RemoveDefragThread removeDefragThread1(m_db, m_tableDef, m_table, recs + 1);
	removeDefragThread1.enableSyncPoint(SP_MRECS_PREPAREUD_PTR_MODIFY);
	removeDefragThread1.start();
	removeDefragThread1.joinSyncPoint(SP_MRECS_PREPAREUD_PTR_MODIFY);
	page = (MRecordPage *)GET_PAGE_HEADER((void *)indexEntry->m_value);
	page->defrag(m_records->m_hashIndex);
	removeDefragThread1.disableSyncPoint(SP_MRECS_PREPAREUD_PTR_MODIFY);
	removeDefragThread1.notifySyncPoint(SP_MRECS_PREPAREUD_PTR_MODIFY);
	removeDefragThread1.join(-1);

	//在remove阶段由于记录的整理，ptr被改变
	indexEntry = m_records->m_hashIndex->get(recs[2].m_rid, m_session->getMemoryContext());
	CPPUNIT_ASSERT(m_records->m_heap->removeHeapRecordAndHash(m_session, recs[2].m_rid));
	RemoveDefragThread removeDefragThread2(m_db, m_tableDef, m_table, recs + 3);
	removeDefragThread2.enableSyncPoint(SP_MRECS_REMOVE_PTR_MODIFY_BEFORE_VERSIONPOOL);
	removeDefragThread2.start();
	removeDefragThread2.joinSyncPoint(SP_MRECS_REMOVE_PTR_MODIFY_BEFORE_VERSIONPOOL);
	page = (MRecordPage *)GET_PAGE_HEADER((void *)indexEntry->m_value);
	page->defrag(m_records->m_hashIndex);
	removeDefragThread2.disableSyncPoint(SP_MRECS_REMOVE_PTR_MODIFY_BEFORE_VERSIONPOOL);
	removeDefragThread2.notifySyncPoint(SP_MRECS_REMOVE_PTR_MODIFY_BEFORE_VERSIONPOOL);
	removeDefragThread2.join(-1);

	//在remove阶段由于记录的整理，ptr被改变
	indexEntry = m_records->m_hashIndex->get(recs[4].m_rid, m_session->getMemoryContext());
	CPPUNIT_ASSERT(m_records->m_heap->removeHeapRecordAndHash(m_session, recs[4].m_rid));
	RemoveDefragThread removeDefragThread3(m_db, m_tableDef, m_table, recs + 5);
	removeDefragThread3.enableSyncPoint(SP_MRECS_REMOVE_PTR_MODIFY_BEFORE_MHEAP);
	removeDefragThread3.start();
	removeDefragThread3.joinSyncPoint(SP_MRECS_REMOVE_PTR_MODIFY_BEFORE_MHEAP);
	page = (MRecordPage *)GET_PAGE_HEADER((void *)indexEntry->m_value);
	page->defrag(m_records->m_hashIndex);
	removeDefragThread3.disableSyncPoint(SP_MRECS_REMOVE_PTR_MODIFY_BEFORE_MHEAP);
	removeDefragThread3.notifySyncPoint(SP_MRECS_REMOVE_PTR_MODIFY_BEFORE_MHEAP);
	removeDefragThread3.join(-1);

	//释放内存
	for (i = 0; i < totalRec; i++) {
		delete[] recs[i].m_redRec->m_data;
		delete recs[i].m_redRec;
		delete[] recs[i].m_name;
	}

	delete[] recs;

	ctx->resetToSavepoint(sp1);
}