/**
 * ����TNTTable DML����
 *
 * @author �εǳ�
 */

#include "api/TestTNTTable.h"
#include "api/Table.h"
#include "Test.h"

const uint TNTTableTestCase::SMALL_LOB_SIZE = Limits::PAGE_SIZE / 4;
const uint TNTTableTestCase::LARGE_LOB_SIZE = Limits::PAGE_SIZE * 16;

const char* TNTTableTestCase::getName() {
	return "TNTTable operation test";
}

const char* TNTTableTestCase::getDescription() {
	return "Test TNTTable operations";
}

bool TNTTableTestCase::isBig() {
	return false;
}

void TNTTableTestCase::setUp() {
	init();
}

void TNTTableTestCase::tearDown() {
	clear();
}

/**	����/�� TNTDatabase
*
*/
void TNTTableTestCase::init() {
	File dir("testdb");
	dir.rmdir(true);
	dir.mkdir();
	config.m_tntBufSize = 100;
	config.setNtseBasedir("testdb");
	config.setTntBasedir("testdb");
	config.setTntDumpdir("testdb");
	config.m_verpoolCnt = 2;

	EXCPT_OPER(m_db = TNTDatabase::open(&config, true, 0));

	Database *db = m_db->getNtseDb();
	m_conn = db->getConnection(false);
	m_conn->setTrx(true);
	m_session = db->getSessionManager()->allocSession("TNTTableTestCase::init", m_conn);

	assert(m_db != NULL && m_session != NULL && m_conn != NULL);

	//table = m_db->openTable(session, "BlogCount");
	//if (table == NULL) {
	TNTTransaction *trx = beginTransaction(OP_WRITE);
	m_tableDef = getBlogCountDef();

	try {
		m_db->createTable(m_session, "BlogCount", m_tableDef);
	} catch (NtseException &e) {
		printf("%s", e.getMessage());
	}

	m_table = m_db->openTable(m_session, "BlogCount");
	//}
	assert(m_table != NULL);
	m_db->getTransSys()->setMaxTrxId(100);

	commitTransaction(trx);

	// ��ʼ��TNTOpInfo
	m_opInfo.m_sqlStatStart = true;
	m_opInfo.m_mysqlOper	= true;
	m_opInfo.m_mysqlHasLocked = false;
	m_opInfo.m_selLockType	= TL_NO;
}

/**	ɾ��/�ر� TNTDatabase
*
*/
void TNTTableTestCase::clear() {
	assert(m_db != NULL && m_session != NULL && m_conn != NULL);

	TNTTransaction *trx = beginTransaction(OP_WRITE);

	if (m_table != NULL) {
		m_db->closeTable(m_session, m_table);
		m_table = NULL;

		m_db->dropTable(m_session, "BlogCount", -1);
	} 
	else {

	}

	commitTransaction(trx);

	if (m_db != NULL) {
		Database *db = m_db->getNtseDb();
		db->getSessionManager()->freeSession(m_session);
		db->freeConnection(m_conn);
		m_db->close();
		delete m_db;
		m_db = NULL;
		// TNTDatabase::drop(".");
	}

	delete m_tableDef;

	File dir("testdb");
	dir.rmdir(true);
}

void TNTTableTestCase::testCommon() {
	//init();

	//clear();
	return;
}

/**
 * ����BlogCount����
 *
 * @param useMms �Ƿ�ʹ��MMS
 * @return BlogCount����
 */
TableDef* TNTTableTestCase::getBlogCountDef(bool useMms, bool onlineIdx) {
	TableDefBuilder *builder = new TableDefBuilder(TableDef::INVALID_TABLEID, "space", "BlogCount");

	builder->addColumn("BlogID", CT_BIGINT, false)->addColumn("CommentCount", CT_INT);
	builder->addColumn("TrackbackCount", CT_SMALLINT)->addColumn("AccessCount", CT_INT);
	builder->addColumnS("Title", CT_CHAR, 50);
	builder->addColumn("UserID", CT_BIGINT, false);
	builder->addIndex("PRIMARY", true, true, onlineIdx, "BlogID", 0, NULL);
	builder->addIndex("IDX_BLOGCOUNT_UID_AC", false, false, onlineIdx, "UserID", 0, "AccessCount", 0, NULL);

	TableDef *tableDef = builder->getTableDef();
	tableDef->m_useMms = useMms;
	tableDef->m_tableStatus = TS_TRX;

	delete builder;
	return tableDef;
}

/** ����insert��������ȷ�� */
void TNTTableTestCase::testInsert() {
	uint numRows = Limits::PAGE_SIZE / 80 * 10;
	Record **rows;

	assert(m_table != NULL);

	numRows = 100;

	EXCPT_OPER(rows = populateBlogCount(m_db, m_table, numRows));

	verifyTable();

	for (uint i = 0; i < numRows; i++)
		freeRecord(rows[i]);

	delete [] rows;
}

void TNTTableTestCase::testUpdate() {
	uint numRows = 100, rowsUpdated;
	Record **rows;

	assert(m_table != NULL);

	EXCPT_OPER(rows = populateBlogCount(m_db, m_table, numRows));
	printf("%d rows inserted.\n", numRows);

	m_opInfo.m_selLockType = TL_X;

	EXCPT_OPER(rowsUpdated = updDelBlogCount(m_db, m_table, OP_UPDATE, true, rows, numRows));

	for (uint i = 0; i < numRows; i++)
		freeRecord(rows[i]);

	delete [] rows;
}
void TNTTableTestCase::testDelete() {
	uint numRows = 100, rowsUpdated;
	Record **rows;

	assert(m_table != NULL);

	EXCPT_OPER(rows = populateBlogCount(m_db, m_table, numRows));

	m_opInfo.m_selLockType = TL_X;

	EXCPT_OPER(rowsUpdated = updDelBlogCount(m_db, m_table, OP_DELETE, true, rows, numRows));

	for (uint i = 0; i < numRows; i++)
		freeRecord(rows[i]);

	delete [] rows;
}

/** �Ƚ���update��Ȼ��del */
void TNTTableTestCase::testUpdDel() {
	uint numRows = 100, rowsUpdated, rowsDeleted;
	Record **rows;

	assert(m_table != NULL);

	EXCPT_OPER(rows = populateBlogCount(m_db, m_table, numRows));
	printf("%d rows inserted.\n", numRows);

	m_opInfo.m_selLockType = TL_X;

	EXCPT_OPER(rowsUpdated = updDelBlogCount(m_db, m_table, OP_UPDATE, true, rows, numRows));
	printf("\n\nRows Updated = %d.\n", rowsUpdated);

	// update���֮�󣬽���delete
	EXCPT_OPER(rowsDeleted = updDelBlogCount(m_db, m_table, OP_DELETE, true, rows, numRows));
	printf("\n\nRows Deleted = %d.\n", rowsDeleted);

	for (uint i = 0; i < numRows; i++)
		freeRecord(rows[i]);

	delete [] rows;
}

void TNTTableTestCase::testIndexUpdate() {
	uint numRows = 100, rowsUpdated;
	Record **rows;

	assert(m_table != NULL);

	EXCPT_OPER(rows = populateBlogCount(m_db, m_table, numRows));
	printf("%d rows inserted.\n", numRows);

	m_opInfo.m_selLockType = TL_X;

	EXCPT_OPER(rowsUpdated = updDelBlogCount(m_db, m_table, OP_UPDATE, false, rows, numRows));

	for (uint i = 0; i < numRows; i++)
		freeRecord(rows[i]);

	delete [] rows;
}

void TNTTableTestCase::testIndexDelete() {
	uint numRows = 100, rowsUpdated;
	Record **rows;

	assert(m_table != NULL);

	EXCPT_OPER(rows = populateBlogCount(m_db, m_table, numRows));

	m_opInfo.m_selLockType = TL_X;

	EXCPT_OPER(rowsUpdated = updDelBlogCount(m_db, m_table, OP_DELETE, false, rows, numRows));

	for (uint i = 0; i < numRows; i++)
		freeRecord(rows[i]);

	delete [] rows;
}

void TNTTableTestCase::testIndexUpdDel() {
	uint numRows = 100, rowsUpdated, rowsDeleted;
	Record **rows;

	assert(m_table != NULL);

	EXCPT_OPER(rows = populateBlogCount(m_db, m_table, numRows));
	printf("%d rows inserted.\n", numRows);

	m_opInfo.m_selLockType = TL_X;

	EXCPT_OPER(rowsUpdated = updDelBlogCount(m_db, m_table, OP_UPDATE, false, rows, numRows));
	printf("\n\nRows Updated = %d.\n", rowsUpdated);

	// update���֮�󣬽���delete
	EXCPT_OPER(rowsDeleted = updDelBlogCount(m_db, m_table, OP_DELETE, false, rows, numRows));
	printf("\n\nRows Deleted = %d.\n", rowsDeleted);

	for (uint i = 0; i < numRows; i++)
		freeRecord(rows[i]);

	delete [] rows;
}

/** ��򵥵������ȫ��ɨ�裬��¼���ɼ� or �����ɼ� */
void TNTTableTestCase::testTableScan() {
	m_opInfo.m_selLockType = TL_NO;
	blogCountScanTest(false, OP_READ, true);
}

/** ȫ��ɨ�裬�Ը�����������ּ�¼�ɼ������ּ�¼���ɼ� */
void TNTTableTestCase::testSnapScanNoRB() {
	uint numRows = 500, retRows;

	// insert 50����¼��ÿ����insert 1��������id��101��ʼ��150����
	Record **rows = populateBlogCount(m_db, m_table, numRows);

	// ����scan���񣬽���ȫ��ɨ��
	// scan�����idΪ108
	// scan����ʼʱ���Ծɻ�Ծ������Ϊ103��105
	// ��ô��scan�ܹ�����������Ϊ101,102,104,106,107,108
	u64	trxArray[10];
	int activeTrxCnt;
	TNTTrxSys *trxSys = m_db->getTransSys();
	TNTTransaction *trx = trxSys->allocTrx();
	trx->startTrxIfNotStarted(m_conn);

	trxArray[0] = 200;
	trxArray[1] = 105;
	trxArray[2] = 103;
	activeTrxCnt= 3;

	trx->setTrxId(208);
	trxSys->trxAssignReadView(trx, trxArray, activeTrxCnt);
	m_session->setTrans(trx);

	m_opInfo.m_selLockType = TL_NO;

	retRows = fetchBlogCount(rows, numRows, OP_READ, true);

	printf("Number rows returned = %d.\n", retRows);

	for (uint i = 0; i < numRows; i++)
		freeRecord(rows[i]);

	delete [] rows;
	commitTransaction(trx);
}
/**
 * ����BlogCount��������ݣ����ɵ����ݵĹ���Ϊ����n(��0��ʼ)�еĸ����԰����¹�������
 * - BlogID: n + 1
 * - CommentCount: 10
 * - TrackbackCount: 2
 * - AccessCount: 100
 * - Title: ��A-Z��������ɣ���Ϊ25���ַ���
 * - UserID: n / 5 + 1
 *
 * @param db ���ݿ����
 * @param table BlogCount��
 * @param numRows Ҫ���������
 * @return ����¼���ݣ�ΪREC_MYSQL��ʽ
 */
Record** TNTTableTestCase::populateBlogCount(TNTDatabase *db, TNTTable *table, uint numRows) {
	Record **rows = new Record *[numRows];
	uint	insRows = numRows;
	System::srandom(1);

	for (uint n = 0; n < insRows; n++) {
		RecordBuilder rb(m_tableDef, RID(0, 0), REC_REDUNDANT);
		rb.appendBigInt(n + 1);
		rb.appendInt(10);
		rb.appendSmallInt(2);
		rb.appendInt(100);
		char title[25];
		for (size_t l = 0; l < sizeof(title) - 1; l++)
			title[l] = (char )('A' + System::random() % 26);
		title[sizeof(title) - 1] = '\0';
		rb.appendChar(title);
		rb.appendBigInt(n / 5 + 1);

		Record *rec = rb.getRecord(m_tableDef->m_maxRecSize);

		uint dupIndex = 15555;
		rec->m_format = REC_MYSQL;

		TNTTransaction *trx = beginTransaction(OP_WRITE);
		//initTNTOpInfo(TL_X);

		CPPUNIT_ASSERT((rec->m_rowId = table->insert(m_session, rec->m_data, &dupIndex, &m_opInfo)) != INVALID_ROW_ID);

		rows[n] = rec;
		commitTransaction(trx);
		//printf("%d: %I64d %d.\n", n, rec->m_rowId, dupIndex);
	}
	
	return rows;
}

void TNTTableTestCase::verifyTable() {
	assert(m_table != NULL);

	m_table->getNtseTable()->verify(m_session);
}

TNTTransaction* TNTTableTestCase::beginTransaction(OpType intention, u64 *trxIds, uint activeTrans) {
	TLockMode lockMode;
	if (intention == OP_READ)
		lockMode = TL_NO;
	else 
		lockMode = TL_X;

	m_opInfo.m_selLockType = lockMode;

	// u64 trxArray[10];
	TNTTrxSys *trxSys = m_db->getTransSys();
	TNTTransaction *trx = trxSys->allocTrx();

	trx->startTrxIfNotStarted(m_conn);

	initTNTOpInfo(lockMode);

	if (intention == OP_READ) {
		// ע������һ�䣬ȫ���ɼ��������ڵ�ǰ������ע�ͣ�ȫ�����ɼ�
		// trx->setTrxId(1);
		//trxSys->trxAssignReadView(trx);
	}
	m_session->setTrans(trx);

	return trx;
}

void TNTTableTestCase::commitTransaction(TNTTransaction *trx) {
	trx->commitTrx(CS_NORMAL);
	m_db->getTransSys()->freeTrx(trx);
	trx = NULL;
	m_session->setTrans(NULL);
}

void TNTTableTestCase::rollbackTransaction(TNTTransaction *trx) {
	trx->rollbackTrx(RBS_NORMAL);
	m_db->getTransSys()->freeTrx(trx);
	trx = NULL;
	m_session->setTrans(NULL);
}

/**
 * ���б�ɨ����ԣ�ɨ��BlogCount��
 *
 * @param useMms ���Ƿ�ʹ��MMS
 * @param intention ��������
 */
void TNTTableTestCase::blogCountScanTest(bool useMms, OpType intention, bool tableScan) {
	uint numRows = 10;
	int newAccessCount = 50;

	Record **rows = populateBlogCount(m_db, m_table, numRows);

	SubRecordBuilder srb(m_tableDef, REC_REDUNDANT);
	// Ҫ�󷵻�BlogID, CommentCount, Title�ֶ�
	SubRecord *subRec = srb.createEmptySbById(m_tableDef->m_maxRecSize, "0 1 4");
	SubRecord *key = NULL;

	TNTTblScan *scanHandle;
	TNTTransaction *trx =beginTransaction(intention);
	if (tableScan)
		scanHandle = m_table->tableScan(m_session, intention, &m_opInfo, subRec->m_numCols, subRec->m_columns);
	else {
		// ʹ������������ָ����ʼ����ΪBlogID >= 0��ȡ���м�¼
		u64 blogId = 0;
		SubRecordBuilder keyBuilder(m_tableDef, KEY_PAD);
		key = keyBuilder.createSubRecordByName("BlogID", &blogId);
		key->m_rowId = INVALID_ROW_ID;
		IndexScanCond cond(0, key, true, true, false);
		scanHandle = m_table->indexScan(m_session, intention, &m_opInfo, &cond, subRec->m_numCols, subRec->m_columns);
	}
	
	u64 spAfterScan = m_session->getMemoryContext()->setSavepoint();

	uint gotRows = 0;
	memset(subRec->m_data, 0, m_tableDef->m_maxRecSize);
	while (m_table->getNext(scanHandle, subRec->m_data)) {
		CPPUNIT_ASSERT(scanHandle->getCurrentRid() == rows[gotRows]->m_rowId);

		// ȷ�ϱ�ɨ�����ü�¼������ȷ
		Record rec(rows[gotRows]->m_rowId, REC_FIXLEN, rows[gotRows]->m_data, rows[gotRows]->m_size);
		SubRecord *subRec2 = srb.createEmptySbById(m_tableDef->m_maxRecSize, "0 1 4");
		memset(subRec2->m_data, 0, m_tableDef->m_maxRecSize);
		RecordOper::extractSubRecordFR(m_tableDef, &rec, subRec2);
		u16 columns[3] = {0, 1, 4};
		CPPUNIT_ASSERT(!compareRecord(m_table, subRec->m_data, subRec2->m_data, 3, columns));

		printSubRecord(subRec);

		freeSubRecord(subRec2);

		gotRows++;
		memset(subRec->m_data, 0, m_tableDef->m_maxRecSize);
	}
	// CPPUNIT_ASSERT(numRows == gotRows);
	m_table->endScan(scanHandle);

	commitTransaction(trx);

	verifyTable();

	for (uint i = 0; i < numRows; i++) {
		freeRecord(rows[i]);
	}
	delete [] rows;
	freeSubRecord(subRec);
	if (key != NULL) {
		freeSubRecord(key);
	}
}


int TNTTableTestCase::compareRecord(TNTTable *table, const byte *rec1, const byte *rec2, u16 numCols, const u16 *columns) {
	return compareRecord(table->getNtseTable()->getTableDef(), rec1, rec2, numCols, columns);
}

int TNTTableTestCase::compareRecord(const TableDef *tableDef, const byte *rec1, const byte *rec2, u16 numCols, const u16 *columns) {
	for (u16 i = 0; i < numCols; i++) {
		u16 cno = columns[i];
		Record r1(INVALID_ROW_ID, REC_REDUNDANT, (byte *)rec1, tableDef->m_maxRecSize);
		Record r2(INVALID_ROW_ID, REC_REDUNDANT, (byte *)rec2, tableDef->m_maxRecSize);
		bool isNull1 = RecordOper::isNullR(tableDef, &r1, cno);
		bool isNull2 = RecordOper::isNullR(tableDef, &r2, cno);
		if (isNull1 && !isNull2)
			return 1;
		else if (!isNull1 && isNull2)
			return -1;
		else if (!isNull1 && !isNull2) {
			if (tableDef->m_columns[cno]->isLob()) {
				void *lob1, *lob2;
				size_t lobSize1, lobSize2;
				RedRecord::readLob(tableDef, rec1, cno, &lob1, &lobSize1);
				RedRecord::readLob(tableDef, rec1, cno, &lob2, &lobSize2);
				size_t minSize = lobSize1 > lobSize2? lobSize2 : lobSize1;
				int r = memcmp(lob1, lob2, minSize);
				if (r)
					return r;
				else if (lobSize1 > lobSize2)
					return 1;
				else if (lobSize1 < lobSize2)
					return -1;
			} else if (tableDef->m_columns[cno]->m_type == CT_VARCHAR) {
				u16 offset = tableDef->m_columns[cno]->m_offset;
				u16 size1, size2;
				u16 lenBytes = tableDef->m_columns[cno]->m_lenBytes;
				if (lenBytes == 1) {
					size1 = *(rec1 + offset);
					size2 = *(rec2 + offset);
				} else {
					assert(lenBytes == 2);
					size1 = *((u16 *)(rec1 + offset));
					size2 = *((u16 *)(rec2 + offset));
				}
				u16 minSize = size1 > size2? size2: size1;
				int r = memcmp(rec1 + offset + lenBytes, rec2 + offset + lenBytes, minSize);
				if (r)
					return r;
				else if (size1 > size2)
					return 1;
				else if (size1 < size2)
					return -1;
			} else {
				u16 offset = tableDef->m_columns[cno]->m_offset;
				u16 size = tableDef->m_columns[cno]->m_size;
				int r = memcmp(rec1 + offset, rec2 + offset, size);
				if (r)
					return r;
			}
		}
	}
	return 0;
}

uint TNTTableTestCase::fetchBlogCount(Record **rows, uint numRows, OpType opType, bool tableScan) {
	
	TableDef *tableDef = m_table->getNtseTable()->getTableDef();
	SubRecordBuilder srb(tableDef, REC_REDUNDANT);
	// Ҫ�󷵻�BlogID, CommentCount, Title�ֶ�
	SubRecord *subRec = srb.createEmptySbById(tableDef->m_maxRecSize, "0 1 4");
	SubRecord *key = NULL;

	TNTTblScan *scanHandle;
	if (tableScan)
		scanHandle = m_table->tableScan(m_session, opType, &m_opInfo, subRec->m_numCols, subRec->m_columns);
	else {
		// ʹ������������ָ����ʼ����ΪBlogID >= 0��ȡ���м�¼
		u64 blogId = 0;
		SubRecordBuilder keyBuilder(tableDef, KEY_PAD);
		key = keyBuilder.createSubRecordByName("BlogID", &blogId);
		key->m_rowId = INVALID_ROW_ID;
		IndexScanCond cond(0, key, true, true, false);
		scanHandle = m_table->indexScan(m_session, opType, &m_opInfo, &cond, subRec->m_numCols, subRec->m_columns);
	}

	u64 spAfterScan = m_session->getMemoryContext()->setSavepoint();

	uint gotRows = 0;
	memset(subRec->m_data, 0, tableDef->m_maxRecSize);
	while (m_table->getNext(scanHandle, subRec->m_data)) {
		// CPPUNIT_ASSERT(scanHandle->getCurrentRid() == rows[gotRows]->m_rowId);
		// ��Է��صĿɼ���¼���ҵ�����rows�����е�λ��
		// printf("%d: %I64d.\n", gotRows, scanHandle->getCurrentRid());

		if (rows != NULL) {
			uint rowPos;
			for (uint i = 0; i < numRows; i++) {
				if (scanHandle->getCurrentRid() == rows[i]->m_rowId) {
					rowPos = i;
					break;
				}
			}

			assert(rowPos != numRows);
			// ȷ�ϱ�ɨ�����ü�¼������ȷ
			Record rec(rows[rowPos]->m_rowId, REC_FIXLEN, rows[rowPos]->m_data, rows[rowPos]->m_size);
			SubRecord *subRec2 = srb.createEmptySbById(tableDef->m_maxRecSize, "0 1 4");
			memset(subRec2->m_data, 0, tableDef->m_maxRecSize);
			RecordOper::extractSubRecordFR(tableDef, &rec, subRec2);
			u16 columns[3] = {0, 1, 4};
			CPPUNIT_ASSERT(!compareRecord(m_table, subRec->m_data, subRec2->m_data, 3, columns));

			freeSubRecord(subRec2);
		}
		
		// ��ӡɨ��ļ�¼
		printSubRecord(subRec);

		gotRows++;
		memset(subRec->m_data, 0, tableDef->m_maxRecSize);
	}
	m_table->endScan(scanHandle);

	freeSubRecord(subRec);
	if (key != NULL) {
		freeSubRecord(key);
	}

	return gotRows;
}

/*
* ��BlogCount��Ķ�������(IDX_BLOGCOUNT_UID_AC)�Ͻ�������ɨ��
*/
uint TNTTableTestCase::fetchBlogCountFromSecIndex(OpType opType) {
	TableDef *tableDef = m_table->getNtseTable()->getTableDef();
	SubRecordBuilder srb(tableDef, REC_REDUNDANT);
	// Ҫ�󷵻�AccessCount, UserID�ֶΣ����Խ���IDX_BLOGCOUNT_UID_AC������coverage scan
	SubRecord *subRec = srb.createEmptySbById(tableDef->m_maxRecSize, "3 5");
	SubRecord *key = NULL;

	TNTTblScan *scanHandle;
	
	// ʹ��IDX_BLOGCOUNT_UID_AC������ָ����ʼ����ΪUserID >= 1��ȡ���м�¼
	u64 userId = 1;
	u64 accessCount = 0;
	SubRecordBuilder keyBuilder(tableDef, KEY_PAD);
	key = keyBuilder.createSubRecordByName("UserID AccessCount", &userId, &accessCount);
	key->m_rowId = INVALID_ROW_ID;
	IndexScanCond cond(1, key, true, true, false);
	scanHandle = m_table->indexScan(m_session, opType, &m_opInfo, &cond, subRec->m_numCols, subRec->m_columns);
	
	u64 spAfterScan = m_session->getMemoryContext()->setSavepoint();

	uint gotRows = 0;
	memset(subRec->m_data, 0, tableDef->m_maxRecSize);
	while (m_table->getNext(scanHandle, subRec->m_data)) {
		// ��ӡɨ��ļ�¼
		printSubRecord(subRec);

		gotRows++;
		memset(subRec->m_data, 0, tableDef->m_maxRecSize);
	}
	m_table->endScan(scanHandle);

	freeSubRecord(subRec);
	if (key != NULL) {
		freeSubRecord(key);
	}

	return gotRows;
}

/** ����TNTTable�ĸ��£�ɾ������ */
uint TNTTableTestCase::updDelBlogCount(TNTDatabase *db, TNTTable *m_table, OpType opType, bool tableScan, Record **rows, uint numRows) {
	uint newAccessCount = 50, updDelRows = 0;
	TableDef *tableDef = m_table->getNtseTable()->getTableDef();

	SubRecordBuilder srb(tableDef, REC_REDUNDANT);
	// Ҫ�󷵻�BlogID, CommentCount, Title�ֶ�
	SubRecord *subRec = srb.createEmptySbById(tableDef->m_maxRecSize, "0 1 4");
	SubRecord *key = NULL;

	TNTTblScan *scanHandle;
	TNTTransaction *trx = beginTransaction(opType);
	if (tableScan)
		scanHandle = m_table->tableScan(m_session, opType, &m_opInfo, subRec->m_numCols, subRec->m_columns);
	else {
		// ʹ������������ָ����ʼ����ΪBlogID >= 0��ȡ���м�¼
		u64 blogId = 0;
		SubRecordBuilder keyBuilder(tableDef, KEY_PAD);
		key = keyBuilder.createSubRecordByName("BlogID", &blogId);
		key->m_rowId = INVALID_ROW_ID;
		IndexScanCond cond(0, key, true, true, false);
		scanHandle = m_table->indexScan(m_session, opType, &m_opInfo, &cond, subRec->m_numCols, subRec->m_columns);
	}

	u16 updateColumns[1] = {3};
	if (opType == OP_UPDATE && (!scanHandle->isUpdateSubRecordSet())) {
		scanHandle->setUpdateSubRecord(1, updateColumns);
	}

	u64 spAfterScan = m_session->getMemoryContext()->setSavepoint();

	if (opType == OP_UPDATE)
		printf("\nbegin update\n");
	else 
		printf("\nbegin delete\n");

	uint gotRows = 0;
	memset(subRec->m_data, 0, tableDef->m_maxRecSize);
	while (m_table->getNext(scanHandle, subRec->m_data)) {
		CPPUNIT_ASSERT(scanHandle->getCurrentRid() == rows[gotRows]->m_rowId);

		// ȷ�ϱ�ɨ�����ü�¼������ȷ
		Record rec(rows[gotRows]->m_rowId, REC_FIXLEN, rows[gotRows]->m_data, rows[gotRows]->m_size);
		SubRecord *subRec2 = srb.createEmptySbById(tableDef->m_maxRecSize, "0 1 4");
		memset(subRec2->m_data, 0, tableDef->m_maxRecSize);
		RecordOper::extractSubRecordFR(tableDef, &rec, subRec2);
		u16 columns[3] = {0, 1, 4};
		CPPUNIT_ASSERT(!compareRecord(m_table, subRec->m_data, subRec2->m_data, 3, columns));

		if ((gotRows % 2) == 0) {
			if (opType == OP_UPDATE) {
				// ����������(UserID, AccessCount)�е�AccessCount�ֶ�
				SubRecord *updateRec = srb.createSubRecordByName("AccessCount", &newAccessCount);
				CPPUNIT_ASSERT(m_table->updateCurrent(scanHandle, updateRec->m_data));
				updDelRows++;
				freeSubRecord(updateRec);
			} else if (opType == OP_DELETE) {
				m_table->deleteCurrent(scanHandle);
				updDelRows++;
			}
		}

		freeSubRecord(subRec2);

		gotRows++;
		memset(subRec->m_data, 0, tableDef->m_maxRecSize);
	}

	m_table->endScan(scanHandle);

	printf("\n%d rows fetched.\n", gotRows);
	if (opType == OP_UPDATE)
		printf("\n%d rows updated.\n", updDelRows);
	else
		printf("\n%d rows deleted.\n", updDelRows);

	commitTransaction(trx);

	verifyTable();
	freeSubRecord(subRec);
	if (key != NULL) {
		freeSubRecord(key);
	}

	// ���»�ɾ����������ɨ��һ����֤���²�����ȷ���
	trx =beginTransaction(OP_READ);

	printf("\n recheck\n");

	subRec = srb.createEmptySbById(tableDef->m_maxRecSize, "0 1 3 4");
	m_opInfo.m_selLockType = TL_NO;
	scanHandle = m_table->tableScan(m_session, OP_READ, &m_opInfo, subRec->m_numCols, subRec->m_columns);
	gotRows = 0;
	memset(subRec->m_data, 0, tableDef->m_maxRecSize);
	while (m_table->getNext(scanHandle, subRec->m_data)) {
		uint matchRow = (opType == OP_UPDATE)? gotRows: gotRows * 2 + 1;
		CPPUNIT_ASSERT(scanHandle->getCurrentRid() == rows[matchRow]->m_rowId);

		// ȷ�ϱ�ɨ�����ü�¼������ȷ
		Record rec(rows[matchRow]->m_rowId, REC_FIXLEN, rows[matchRow]->m_data, rows[matchRow]->m_size);
		SubRecord *subRec2 = srb.createEmptySbById(tableDef->m_maxRecSize, "0 1 3 4");
		memset(subRec2->m_data, 0,tableDef->m_maxRecSize);
		RecordOper::extractSubRecordFR(tableDef, &rec, subRec2);
		if (opType == OP_UPDATE && (gotRows % 2) == 0)
			*((int *)(subRec2->m_data + tableDef->m_columns[3]->m_offset)) = newAccessCount;
		u16 columns[4] = {0, 1, 3, 4};
		CPPUNIT_ASSERT(!compareRecord(m_table, subRec->m_data, subRec2->m_data, 4, columns));

		freeSubRecord(subRec2);

		gotRows++;
	}
	CPPUNIT_ASSERT(!m_table->getNext(scanHandle, subRec->m_data));
	if (opType == OP_UPDATE)
		CPPUNIT_ASSERT(numRows == gotRows);
	else
		CPPUNIT_ASSERT(numRows == gotRows * 2);
	m_table->endScan(scanHandle);

	commitTransaction(trx);

	freeSubRecord(subRec);

	printf("\n%d rows rechecked.", gotRows);

	return updDelRows;
}

uint TNTTableTestCase::updDelBlogCount(TNTDatabase *db, TNTTable *table, OpType opType, Record **rows, uint numRows) {
	assert(opType == OP_UPDATE || opType == OP_DELETE);
	uint commentCount, accessCount, updDelRows = 0;
	SubRecordBuilder srb(m_tableDef, REC_REDUNDANT);
	MemoryContext *ctx = m_session->getMemoryContext();
	McSavepoint msp(ctx);

	u16 *readCols = (u16 *)ctx->alloc(sizeof(u16) * m_tableDef->m_numCols);
	for (u16 i = 0; i < m_tableDef->m_numCols; ++i) {
		readCols[i] = i;
	}

	u16 updateColumns[2] = {1, 3};
	TNTTblScan *scan = NULL;
	byte *mysqlRow = (byte *)ctx->alloc(m_tableDef->m_maxRecSize);
	TNTTransaction *trx = beginTransaction(opType);
	for (uint i = 0; i < numRows; i++) {
		scan = m_table->positionScan(m_session, opType, &m_opInfo, m_tableDef->m_numCols, readCols);
		CPPUNIT_ASSERT(m_table->getNext(scan, mysqlRow, rows[i]->m_rowId));
		if (opType == OP_UPDATE) {
			commentCount = 105 + i;
			accessCount = 50 + i % 100;
			scan->setUpdateSubRecord(2, updateColumns);
			SubRecord *updateRec = srb.createSubRecordByName("CommentCount AccessCount", &commentCount, &accessCount);
			CPPUNIT_ASSERT(m_table->updateCurrent(scan, updateRec->m_data));
			rows[i]->m_rowId = scan->getRecord()->m_rowId;
			rows[i]->m_format = scan->getRecord()->m_format;
			rows[i]->m_size = scan->getRecord()->m_size;
			memcpy(rows[i]->m_data, updateRec->m_data, rows[i]->m_size);
			freeSubRecord(updateRec);
		} else {
			assert(opType == OP_DELETE);
			m_table->deleteCurrent(scan);
		}
		updDelRows++;
		m_table->endScan(scan);
	}
	commitTransaction(trx);

	return updDelRows;
}

/** �˺��������첻ͬ��ReadView�������ز�ͬ�����ݼ� */
void TNTTableTestCase::fetchBlogCountOnDiffRV(bool tableScan) {
	// ���Ը��º��¼�ɼ�
	TNTTrxSys *trxSys = m_db->getTransSys();
	TNTTransaction *trx = beginTransaction(OP_READ);

	uint retRows = fetchBlogCount(NULL, 0, OP_READ, true);

	printf("\n\n\nNumber rows returned = %d.\n", retRows);

	commitTransaction(trx);
	trx = NULL;

	// ���Ը���ǰ��¼�ɼ�
	trx = beginTransaction(OP_READ);
	trx->setTrxId(200);
	trx->trxAssignReadView();
	retRows = fetchBlogCount(NULL, 0, OP_READ, true);

	printf("\n\n\nNumber rows returned = %d.\n", retRows);

	commitTransaction(trx);
	trx = NULL;

	// ���Ը���ǰ��¼���ֿɼ�
	trx = beginTransaction(OP_READ);
	trx->setTrxId(150);
	trx->trxAssignReadView();
	retRows = fetchBlogCount(NULL, 0, OP_READ, true);

	printf("\n\n\nNumber rows returned = %d.\n", retRows);

	commitTransaction(trx);
	trx = NULL;

	// ���Ը���ǰ���¼�����ɼ�
	trx = beginTransaction(OP_READ);
	trx->setTrxId(1);
	trx->trxAssignReadView();
	retRows = fetchBlogCount(NULL, 0, OP_READ, true);

	printf("\n\n\nNumber rows returned = %d.\n", retRows);

	commitTransaction(trx);
	trx = NULL;
}

void TNTTableTestCase::testSnapScanAfterUpd() {
	testUpdate();
	fetchBlogCountOnDiffRV();
}

void TNTTableTestCase::testSnapScanAfterDel() {
	testDelete();
	fetchBlogCountOnDiffRV();
}

/** ���Ա�������Update��Delete��Ŀɼ����ж� */
void TNTTableTestCase::testSnapScanAfterUD() {
	testUpdDel();
	
	// ���Ը��º��¼�ɼ�
	// 50��
	TNTTrxSys *trxSys = m_db->getTransSys();
	TNTTransaction *trx = beginTransaction(OP_READ);

	uint retRows = fetchBlogCount(NULL, 0, OP_READ, true);

	printf("\n\n\nNumber rows returned = %d and we should see 50.\n", retRows);

	commitTransaction(trx);
	trx = NULL;

	// ���Ը���ǰ��¼�ɼ�
	trx = beginTransaction(OP_READ);
	trx->setTrxId(200);
	trx->trxAssignReadView();
	retRows = fetchBlogCount(NULL, 0, OP_READ, true);

	printf("\n\n\nNumber rows returned = %d and we should see 100.\n", retRows);

	commitTransaction(trx);
	trx = NULL;

	// ���Ը��¼�¼�ɼ�
	trx = beginTransaction(OP_READ);
	trx->setTrxId(202);
	trx->trxAssignReadView();
	retRows = fetchBlogCount(NULL, 0, OP_READ, true);

	printf("\n\n\nNumber rows returned = %d and we should see 100.\n", retRows);

	commitTransaction(trx);
	trx = NULL;

	// ����ɾ����¼�����ɼ�
	trx = beginTransaction(OP_READ);
	trx->setTrxId(203);
	trx->trxAssignReadView();
	retRows = fetchBlogCount(NULL, 0, OP_READ, true);

	printf("\n\n\nNumber rows returned = %d and we should see 50.\n", retRows);

	commitTransaction(trx);
	trx = NULL;

	// ���Ը���ǰ���¼�����ɼ�
	trx = beginTransaction(OP_READ);
	trx->setTrxId(1);
	trx->trxAssignReadView();
	retRows = fetchBlogCount(NULL, 0, OP_READ, true);

	printf("\n\n\nNumber rows returned = %d and we should see 0.\n", retRows);

	commitTransaction(trx);
	trx = NULL;
}

void TNTTableTestCase::testIndexRangeScan() {
	m_opInfo.m_selLockType = TL_NO;
	blogCountScanTest(false, OP_READ, false);	
}

void TNTTableTestCase::testIndexUniqueScan() {
	
}

void TNTTableTestCase::testIndexRangeAfterUpd() {
	testIndexUpdate();
	fetchBlogCountOnDiffRV(false);
}

void TNTTableTestCase::testIndexRangeAfterDel() {
	testIndexDelete();
	fetchBlogCountOnDiffRV(false);
}

void TNTTableTestCase::testIndexRangeAfterUD() {
	testIndexUpdDel();
	
	// ���Ը��º��¼�ɼ�
	// 50��
	TNTTrxSys *trxSys = m_db->getTransSys();
	TNTTransaction *trx = beginTransaction(OP_READ);

	uint retRows = fetchBlogCount(NULL, 0, OP_READ, false);

	printf("\n\n\nNumber rows returned = %d and we should see 50.\n", retRows);

	commitTransaction(trx);
	trx = NULL;

	// ���Ը���ǰ��¼�ɼ�
	trx = beginTransaction(OP_READ);
	trx->setTrxId(200);
	trx->trxAssignReadView();
	retRows = fetchBlogCount(NULL, 0, OP_READ, false);

	printf("\n\n\nNumber rows returned = %d and we should see 100.\n", retRows);

	commitTransaction(trx);
	trx = NULL;

	// ���Ը��¼�¼�ɼ�
	trx = beginTransaction(OP_READ);
	trx->setTrxId(202);
	trx->trxAssignReadView();
	retRows = fetchBlogCount(NULL, 0, OP_READ, false);

	printf("\n\n\nNumber rows returned = %d and we should see 100.\n", retRows);

	commitTransaction(trx);
	trx = NULL;

	// ����ɾ����¼�����ɼ�
	trx = beginTransaction(OP_READ);
	trx->setTrxId(203);
	trx->trxAssignReadView();
	retRows = fetchBlogCount(NULL, 0, OP_READ, false);

	printf("\n\n\nNumber rows returned = %d and we should see 50.\n", retRows);

	commitTransaction(trx);
	trx = NULL;

	// ���Ը���ǰ���¼�����ɼ�
	trx = beginTransaction(OP_READ);
	trx->setTrxId(1);
	trx->trxAssignReadView();
	retRows = fetchBlogCount(NULL, 0, OP_READ, false);

	printf("\n\n\nNumber rows returned = %d and we should see 0.\n", retRows);

	commitTransaction(trx);
	trx = NULL;
}

void TNTTableTestCase::doPKeyFetchTest(Record **rows, uint numRows) {
	TNTTransaction	*trx;

	TableDef *tableDef = m_table->getNtseTable()->getTableDef();

	SubRecordBuilder srb(tableDef, REC_REDUNDANT);
	// Ҫ�󷵻�BlogID, CommentCount, Title�ֶ�
	SubRecord *subRec = srb.createEmptySbById(tableDef->m_maxRecSize, "0 1 3");
	SubRecordBuilder keyBuilder(tableDef, KEY_PAD);
	u16 columns[3] = {0, 1, 3};

	printf("\n");
	// ����������ȡǰһ���¼
	{
		trx = beginTransaction(OP_READ, NULL, 0);
		m_session->setTrans(trx);

		for (uint n = 0; n < numRows / 2; n++) {
			u64 blogId = n + 1;
			SubRecord *key = keyBuilder.createSubRecordByName("BlogID", &blogId);
			key->m_rowId = INVALID_ROW_ID;

			IndexScanCond cond(0, key, true, true, true);
			m_opInfo.m_selLockType = TL_NO;
			TNTTblScan *scanHandle = m_table->indexScan(m_session, OP_READ, &m_opInfo, &cond, 3, columns, true);
			CPPUNIT_ASSERT(m_table->getNext(scanHandle, subRec->m_data));
			CPPUNIT_ASSERT(!compareRecord(m_table, rows[n]->m_data, subRec->m_data, 3, columns));
			CPPUNIT_ASSERT(!m_table->getNext(scanHandle, subRec->m_data));
			m_table->endScan(scanHandle);

			freeSubRecord(key);

			printf("\nRow %d: ", n);
			printSubRecord(subRec);
		}

		commitTransaction(trx);
		m_session->setTrans(NULL);
	}

	printf("\n");
	// ����������ȡ�����¼�¼
	{
		trx = beginTransaction(OP_UPDATE, NULL, 0);
		m_session->setTrans(trx);

		for (uint n = 0; n < numRows; n++) {
			u64 blogId = n + 1;
			SubRecord *key = keyBuilder.createSubRecordByName("BlogID", &blogId);
			key->m_rowId = INVALID_ROW_ID;

			IndexScanCond cond(0, key, true, true, true);
			m_opInfo.m_selLockType = TL_X;
			TNTTblScan *scanHandle = m_table->indexScan(m_session, OP_UPDATE, &m_opInfo, &cond, 1, columns, true);
			u16 updateCols[1] = {0};
			scanHandle->setUpdateSubRecord(1, updateCols);

			u64 newBlogId = numRows + n + 1;
			SubRecord *updateRec = srb.createSubRecordByName("BlogID", &newBlogId);

			CPPUNIT_ASSERT(m_table->getNext(scanHandle, subRec->m_data));
			CPPUNIT_ASSERT(!compareRecord(m_table, rows[n]->m_data, subRec->m_data, 1, columns));
			CPPUNIT_ASSERT(m_table->updateCurrent(scanHandle, updateRec->m_data));
			*((u64 *)(rows[n]->m_data + tableDef->m_columns[0]->m_offset)) = newBlogId;

			m_table->endScan(scanHandle);

			freeSubRecord(key);
			freeSubRecord(updateRec);
		}

		commitTransaction(trx);
		m_session->setTrans(NULL);

		trx = beginTransaction(OP_READ, NULL, 0);
		m_session->setTrans(trx);

		for (uint n = 0; n < numRows; n++) {
			u64 blogId = numRows + n + 1;
			SubRecord *key = keyBuilder.createSubRecordByName("BlogID", &blogId);
			key->m_rowId = INVALID_ROW_ID;

			IndexScanCond cond(0, key, true, true, true);
			m_opInfo.m_selLockType = TL_NO;
			TNTTblScan *scanHandle = m_table->indexScan(m_session, OP_READ, &m_opInfo, &cond, 3, columns, true);
			CPPUNIT_ASSERT(m_table->getNext(scanHandle, subRec->m_data));
			CPPUNIT_ASSERT(!compareRecord(m_table, rows[n]->m_data, subRec->m_data, 3, columns));
			CPPUNIT_ASSERT(!m_table->getNext(scanHandle, subRec->m_data));
			m_table->endScan(scanHandle);

			freeSubRecord(key);

			printf("\nRow %d: ", n);
			printSubRecord(subRec);
		}

		commitTransaction(trx);
		m_session->setTrans(NULL);
	}

	printf("\n");
	// ����������ȡ��ɾ����¼
	{
		trx = beginTransaction(OP_DELETE, NULL, 0);
		m_session->setTrans(trx);

		for (uint n = 0; n < numRows; n++) {
			u64 blogId = numRows + n + 1;
			SubRecord *key = keyBuilder.createSubRecordByName("BlogID", &blogId);
			key->m_rowId = INVALID_ROW_ID;

			IndexScanCond cond(0, key, true, true, true);
			m_opInfo.m_selLockType = TL_X;
			TNTTblScan *scanHandle = m_table->indexScan(m_session, OP_DELETE, &m_opInfo, &cond, 3, columns, true);

			CPPUNIT_ASSERT(m_table->getNext(scanHandle, subRec->m_data));
			CPPUNIT_ASSERT(!compareRecord(m_table, rows[n]->m_data, subRec->m_data, 3, columns));
			m_table->deleteCurrent(scanHandle);

			m_table->endScan(scanHandle);

			freeSubRecord(key);
		}

		commitTransaction(trx);
		m_session->setTrans(NULL);

		trx = beginTransaction(OP_READ, NULL, 0);
		m_session->setTrans(trx);

		m_opInfo.m_selLockType = TL_NO;

		TNTTblScan *scanHandle = m_table->tableScan(m_session, OP_READ, &m_opInfo, 3, columns);
		CPPUNIT_ASSERT(!m_table->getNext(scanHandle, subRec->m_data));
		m_table->endScan(scanHandle);

		commitTransaction(trx);
		m_session->setTrans(NULL);
	}

	freeSubRecord(subRec);
}

void TNTTableTestCase::testPKFetch() {
	uint numRows = 10;

	Record **rows = populateBlogCount(m_db, m_table, numRows);

	doPKeyFetchTest(rows, numRows);

	for (uint i = 0; i < numRows; i++) {
		freeRecord(rows[i]);
	}
	
	delete [] rows;
}

void TNTTableTestCase::testPosScan() {
	uint numRows = 10;

	Record **rows = populateBlogCount(m_db, m_table, numRows);

	doPosScanTest(rows, numRows);

	for (uint i = 0; i < numRows; i++) {
		freeRecord(rows[i]);
	}

	delete [] rows;
}

void TNTTableTestCase::doPosScanTest(Record **rows, uint numRows) {
	TNTTransaction *trx  = NULL;
	TableDef *tableDef = m_table->getNtseTable()->getTableDef();

	SubRecordBuilder srb(tableDef, REC_REDUNDANT);
	// Ҫ�󷵻�BlogID, CommentCount, Title�ֶ�
	SubRecord *subRec = srb.createEmptySbById(tableDef->m_maxRecSize, "0 1 3");
	SubRecordBuilder keyBuilder(tableDef, KEY_PAD);
	u16 columns[3] = {0, 1, 3};

	printf("\n");
	// ���ж�λɨ�����
	{
		trx = beginTransaction(OP_UPDATE, NULL, 0);
		m_session->setTrans(trx);

		u64 startId = 1;
		SubRecord *key = keyBuilder.createSubRecordByName("BlogID", &startId);
		key->m_rowId = INVALID_ROW_ID;

		m_opInfo.m_selLockType = TL_X;
		TNTTblScan *scanHandle = m_table->positionScan(m_session, OP_UPDATE, &m_opInfo, 3, columns);

		u16 updateCols[1] = {0};
		scanHandle->setUpdateSubRecord(1, updateCols);

		for (uint n = 0; n < numRows; n++) {
			CPPUNIT_ASSERT(m_table->getNext(scanHandle, subRec->m_data, rows[n]->m_rowId));
			CPPUNIT_ASSERT(scanHandle->getCurrentRid() == rows[n]->m_rowId);

			printf("\nRow %d ", n);
			printSubRecord(subRec);

			u64 newBlogId = numRows + n + 1;
			SubRecord *updateRec = srb.createSubRecordByName("BlogID", &newBlogId);

			CPPUNIT_ASSERT(m_table->updateCurrent(scanHandle, updateRec->m_data));
			*((u64 *)(rows[n]->m_data +tableDef->m_columns[0]->m_offset)) = newBlogId;

			freeSubRecord(updateRec);
		}
		// ȡһ�������ڵļ�¼
		CPPUNIT_ASSERT(!m_table->getNext(scanHandle, subRec->m_data, rows[numRows - 1]->m_rowId + 1));
		m_table->endScan(scanHandle);

		commitTransaction(trx);
		m_session->setTrans(NULL);

		freeSubRecord(key);
	}

	printf("\n");
	// ��֤���½��
	{
		trx = beginTransaction(OP_READ, NULL, 0);
		m_session->setTrans(trx);

		for (uint n = 0; n < numRows; n++) {
			u64 blogId = numRows + n + 1;
			SubRecord *key = keyBuilder.createSubRecordByName("BlogID", &blogId);
			key->m_rowId = INVALID_ROW_ID;

			IndexScanCond cond(0, key, true, true, true);

			m_opInfo.m_selLockType = TL_NO;
			TNTTblScan *scanHandle = m_table->indexScan(m_session, OP_READ, &m_opInfo, &cond, 3, columns, true);
			CPPUNIT_ASSERT(m_table->getNext(scanHandle, subRec->m_data));
			CPPUNIT_ASSERT(!compareRecord(m_table, rows[n]->m_data, subRec->m_data, 3, columns));
			CPPUNIT_ASSERT(!m_table->getNext(scanHandle, subRec->m_data));
			m_table->endScan(scanHandle);

			freeSubRecord(key);

			printf("\nRow %d ", n);
			printSubRecord(subRec);
		}

		commitTransaction(trx);
		m_session->setTrans(NULL);
	}

	freeSubRecord(subRec);
}

/**
 * ����REPLACE���ܣ���һ�ִ�����ʽ����replaceת��Ϊon duplicate key update�߼�
 */
void TNTTableTestCase::testReplace() {
	TableDef *tableDef = m_table->getNtseTable()->getTableDef();
	TNTTransaction *trx  = NULL;
	uint dupIndex;

	// INSERT INTO BlogCount(BlogID, CommentCount, TrackbackCount, AccessCount, Title, UserID)
	// VALUES(1, ...)
	Record **rows = populateBlogCount(m_db, m_table, 1);

	// INSERT INTO BlogCount(BlogID, CommentCount, TrackbackCount, AccessCount, Title, UserID)
	// VALUES(2, ...) ON DUPLICATE KEY SET BlogID = 3;
	// ����ͻ
	//initTNTOpInfo(TL_X);
	{
		trx = beginTransaction(OP_WRITE, NULL, 0);
		m_session->setTrans(trx);

		*((u64 *)(rows[0]->m_data + tableDef->m_columns[0]->m_offset)) = 2;
		IUSequence<TNTTblScan *> *iuSeq = m_table->insertForDupUpdate(m_session, rows[0]->m_data, &m_opInfo);
		CPPUNIT_ASSERT(!iuSeq);

		commitTransaction(trx);
		m_session->setTrans(NULL);
	}

	// INSERT INTO BlogCount(BlogID, CommentCount, TrackbackCount, AccessCount, Title, UserID)
	// VALUES(1, ...) ON DUPLICATE KEY SET BlogID = 3;
	// ��ͻ�����º�ɹ�
	//initTNTOpInfo(TL_X);
	{
		trx = beginTransaction(OP_WRITE, NULL, 0);
		m_session->setTrans(trx);

		*((u64 *)(rows[0]->m_data + tableDef->m_columns[0]->m_offset)) = 1;
		IUSequence<TNTTblScan *> *iuSeq = m_table->insertForDupUpdate(m_session, rows[0]->m_data, &m_opInfo);
		CPPUNIT_ASSERT(iuSeq);
		*((u64 *)(rows[0]->m_data + tableDef->m_columns[0]->m_offset)) = 3;
		u16 columns[1] = {0};
		CPPUNIT_ASSERT(m_table->updateDuplicate(iuSeq, rows[0]->m_data, 1, columns, &dupIndex));

		commitTransaction(trx);
		m_session->setTrans(NULL);
	}

	// ��ӡ��ʱ���еļ�¼
	{
		TNTTransaction *trx = beginTransaction(OP_READ);

		uint retRows = fetchBlogCount(NULL, 0, OP_READ, false);

		printf("\n\n\nNumber rows returned = %d\n", retRows);

		commitTransaction(trx);
		trx = NULL;
	}
	

	// INSERT INTO BlogCount(BlogID, CommentCount, TrackbackCount, AccessCount, Title, UserID)
	// VALUES(3, ...) ON DUPLICATE KEY SET BlogID = 2;
	// ��ͻ�����º��Բ��ɹ�
	//initTNTOpInfo(TL_X);
	{
		trx = beginTransaction(OP_WRITE, NULL, 0);
		m_session->setTrans(trx);

		*((u64 *)(rows[0]->m_data + tableDef->m_columns[0]->m_offset)) = 3;
		IUSequence<TNTTblScan *> *iuSeq = m_table->insertForDupUpdate(m_session, rows[0]->m_data, &m_opInfo);
		CPPUNIT_ASSERT(iuSeq);
		*((u64 *)(rows[0]->m_data + tableDef->m_columns[0]->m_offset)) = 2;
		u16 columns[1] = {0};
		CPPUNIT_ASSERT(!m_table->updateDuplicate(iuSeq, rows[0]->m_data, 1, columns, &dupIndex));

		commitTransaction(trx);
		m_session->setTrans(NULL);
	}
	

	// INSERT INTO BlogCount(BlogID, CommentCount, TrackbackCount, AccessCount, Title, UserID)
	// VALUES(3, ...) ON DUPLICATE KEY SET BlogID = 2;
	// ��һ���߳��ڵ�һ���̷߳��ֳ�ͻ֮��ɨ��֮ǰ����¼ɾ����ʹ�������ɹ�
	/*
	{
		ReplaceTester *testThread = new ReplaceTester(m_db, m_table);
		testThread->enableSyncPoint(SP_TBL_REPLACE_AFTER_DUP);
		testThread->start();
		testThread->joinSyncPoint(SP_TBL_REPLACE_AFTER_DUP);

		// ɾ��BlogID = 3�ļ�¼
		Connection *conn = m_db->getConnection(false);
		Session *session = m_db->getSessionManager()->allocSession("TableTestCase::testReplace", conn);

		u64 blogId = 3;
		SubRecordBuilder keyBuilder(m_table->getTableDef(), KEY_PAD);
		SubRecord *key = keyBuilder.createSubRecordByName("BlogID", &blogId);
		key->m_rowId = INVALID_ROW_ID;

		SubRecordBuilder srb(m_table->getTableDef(), REC_REDUNDANT);
		SubRecord *subRec = srb.createEmptySbByName(m_table->getTableDef()->m_maxRecSize, "BlogID");

		IndexScanCond cond(0, key, true, true, true);
		TblScan *scanHandle = m_table->indexScan(session, OP_DELETE, &cond, subRec->m_numCols, subRec->m_columns);
		CPPUNIT_ASSERT(m_table->getNext(scanHandle, subRec->m_data));
		m_table->deleteCurrent(scanHandle);
		m_table->endScan(scanHandle);

		freeSubRecord(key);
		freeSubRecord(subRec);

		m_db->getSessionManager()->freeSession(session);
		m_db->freeConnection(conn);

		testThread->notifySyncPoint(SP_TBL_REPLACE_AFTER_DUP);
		testThread->join();
		delete testThread;

		verifyTable(m_db, m_table);
	}
	*/

	freeRecord(rows[0]);
	delete [] rows;
}

/** replace���ԣ��ڶ��ִ�����ʽ��ԭ��replace��������ͻ����delete����insert */
void TNTTableTestCase::testReplace2() {
	TableDef *tableDef = m_table->getNtseTable()->getTableDef();
	TNTTransaction *trx  = NULL;

	// INSERT INTO BlogCount(BlogID, CommentCount, TrackbackCount, AccessCount, Title, UserID)
	// VALUES(1, ...)
	Record **rows = populateBlogCount(m_db, m_table, 1);

	// replace into BlogCount() values (2, ...);
	// ����ͻ
	//initTNTOpInfo(TL_X);
	{
		trx = beginTransaction(OP_WRITE, NULL, 0);
		m_session->setTrans(trx);

		*((u64 *)(rows[0]->m_data + tableDef->m_columns[0]->m_offset)) = 2;
		IUSequence<TNTTblScan *> *iuSeq = m_table->insertForDupUpdate(m_session, rows[0]->m_data, &m_opInfo);
		CPPUNIT_ASSERT(!iuSeq);

		commitTransaction(trx);
		m_session->setTrans(NULL);
	}

	// replace into BlogCount() values (1,...);
	// ��ͻ��ɾ��ԭ�м�¼�������¼�¼
	//initTNTOpInfo(TL_X);
	{
		trx = beginTransaction(OP_WRITE, NULL, 0);
		m_session->setTrans(trx);

		*((u64 *)(rows[0]->m_data + tableDef->m_columns[0]->m_offset)) = 1;
		IUSequence<TNTTblScan *> *iuSeq = m_table->insertForDupUpdate(m_session, rows[0]->m_data, &m_opInfo);
		CPPUNIT_ASSERT(iuSeq);
		
		// ɾ��duplicate ��
		m_table->deleteDuplicate(iuSeq);

		// �ٴ�insert
		iuSeq = m_table->insertForDupUpdate(m_session, rows[0]->m_data, &m_opInfo);
		CPPUNIT_ASSERT(!iuSeq);

		commitTransaction(trx);
		m_session->setTrans(NULL);
	}

	// ��ӡ��ʱ���еļ�¼
	{
		TNTTransaction *trx = beginTransaction(OP_READ);

		uint retRows = fetchBlogCount(NULL, 0, OP_READ, false);

		printf("\n\n\nNumber rows returned = %d\n", retRows);

		commitTransaction(trx);
		trx = NULL;
	}

	freeRecord(rows[0]);
	delete [] rows;
}

/** ���Զ�һ����¼�����ж�θ���֮�󣬰汾�ɼ������� */
void TNTTableTestCase::doMultiUpdAndScan(bool tableScan) {
	TableDef *tableDef = m_table->getNtseTable()->getTableDef();
	uint numRows = 1;
	TNTTrxSys *trxSys = m_db->getTransSys();

	// INSERT INTO BlogCount(BlogID, CommentCount, TrackbackCount, AccessCount, Title, UserID)
	// VALUES(1, ...)
	Record **rows = populateBlogCount(m_db, m_table, numRows);

	uint succUpdTimes = 0;

	SubRecordBuilder srb(tableDef, REC_REDUNDANT);
	// Ҫ�󷵻�BlogID, CommentCount, Title�ֶ�
	SubRecord *subRec = srb.createEmptySbById(tableDef->m_maxRecSize, "0 1 3");
	SubRecord *key = NULL;

	for (uint i = 101; i <= 200; i++) {
		TNTTblScan		*scanHandle;

		TNTTransaction	*trx =beginTransaction(OP_UPDATE);
		m_session->setTrans(trx);

		m_opInfo.m_selLockType = TL_X;

		if (tableScan)
			scanHandle = m_table->tableScan(m_session, OP_UPDATE, &m_opInfo, subRec->m_numCols, subRec->m_columns);
		else {
			// ʹ������������ָ����ʼ����ΪBlogID >= 0��ȡ���м�¼
			u64 blogId = 0;
			SubRecordBuilder keyBuilder(tableDef, KEY_PAD);
			key = keyBuilder.createSubRecordByName("BlogID", &blogId);
			key->m_rowId = INVALID_ROW_ID;
			IndexScanCond cond(0, key, true, true, false);
			scanHandle = m_table->indexScan(m_session, OP_UPDATE, &m_opInfo, &cond, subRec->m_numCols, subRec->m_columns);
		}

		u16 updateColumns[1] = {3};
		scanHandle->setUpdateSubRecord(1, updateColumns);

		memset(subRec->m_data, 0, tableDef->m_maxRecSize);
		while (m_table->getNext(scanHandle, subRec->m_data)) {
			// ����������(UserID, AccessCount)�е�AccessCount�ֶ�
			SubRecord *updateRec = srb.createSubRecordByName("AccessCount", &i);
			CPPUNIT_ASSERT(m_table->updateCurrent(scanHandle, updateRec->m_data));
			succUpdTimes++;
			freeSubRecord(updateRec);

			memset(subRec->m_data, 0, tableDef->m_maxRecSize);
		}

		m_table->endScan(scanHandle);
		commitTransaction(trx);
		m_session->setTrans(NULL);
		if (key != NULL) {
			freeSubRecord(key);
			key = NULL;
		}
	}

	printf("\nRow one been updated for %d times.\n", succUpdTimes);

	for (uint i = 0; i < numRows; i++)
		freeRecord(rows[i]);

	delete [] rows;

	// ��ȡ��¼�����а汾
	for (uint i = 0; i < 100; i++) {
		TNTTblScan		*scanHandle;

		TNTTransaction	*trx =beginTransaction(OP_READ);
		trx->setTrxId(i + 100);

		m_session->setTrans(trx);
		trx->trxAssignReadView();

		m_opInfo.m_selLockType = TL_NO;

		if (tableScan)
			scanHandle = m_table->tableScan(m_session, OP_READ, &m_opInfo, subRec->m_numCols, subRec->m_columns);
		else {
			// ʹ������������ָ����ʼ����ΪBlogID >= 0��ȡ���м�¼
			u64 blogId = 0;
			SubRecordBuilder keyBuilder(tableDef, KEY_PAD);
			key = keyBuilder.createSubRecordByName("BlogID", &blogId);
			key->m_rowId = INVALID_ROW_ID;
			IndexScanCond cond(0, key, true, true, false);
			scanHandle = m_table->indexScan(m_session, OP_READ, &m_opInfo, &cond, subRec->m_numCols, subRec->m_columns);
		}

		memset(subRec->m_data, 0, tableDef->m_maxRecSize);
		while (m_table->getNext(scanHandle, subRec->m_data)) {
			// ��ӡ���صļ�¼
			printf("\n Version %d = TrxId = %d ", i, 100+i);
			printSubRecord(subRec);
			
			memset(subRec->m_data, 0, tableDef->m_maxRecSize);
		}

		m_table->endScan(scanHandle);
		commitTransaction(trx);
		m_session->setTrans(NULL);
		if (key != NULL) {
			freeSubRecord(key);
			key = NULL;
		}
	}

	freeSubRecord(subRec);
}

void TNTTableTestCase::testSnapScanAfterMultiUpd() {
	doMultiUpdAndScan(true);
}

void TNTTableTestCase::testIndexRangeAfterMultiUpd() {
	doMultiUpdAndScan(false);
}

/*
* ����TNTTable�У�δ���ǵ��ĳ�����������
* 1. index coverage scan�����������ɼ�
* 2. index coverage scan��������¼��ͬ�����ɼ�
* 3. update�����¶�������
*/
void TNTTableTestCase::testIndexScanUncovered() {
	// Insertһ����¼������IDΪ10
	{
		RecordBuilder rb(m_tableDef, RID(0, 0), REC_REDUNDANT);
		rb.appendBigInt(1);
		rb.appendInt(10);
		rb.appendSmallInt(2);
		rb.appendInt(100);
		char title[25];
		for (size_t l = 0; l < sizeof(title) - 1; l++)
			title[l] = (char )('A' + System::random() % 26);
		title[sizeof(title) - 1] = '\0';
		rb.appendChar(title);
		rb.appendBigInt(1);

		Record *rec = rb.getRecord(m_tableDef->m_maxRecSize);

		uint dupIndex = 15555;
		rec->m_format = REC_MYSQL;

		TNTTransaction *trx = beginTransaction(OP_WRITE);
		trx->setTrxId(10);
		//initTNTOpInfo(TL_X);

		CPPUNIT_ASSERT((rec->m_rowId = m_table->insert(m_session, rec->m_data, &dupIndex, &m_opInfo)) != INVALID_ROW_ID);

		commitTransaction(trx);

		freeRecord(rec);
	}

	// update�������ֶ�CommentCount������IDΪ20
	{
		TableDef *tableDef = m_table->getNtseTable()->getTableDef();

		SubRecordBuilder srb(tableDef, REC_REDUNDANT);
		// Ҫ�󷵻�BlogID, CommentCount, Title�ֶ�
		SubRecord *subRec = srb.createEmptySbById(tableDef->m_maxRecSize, "0 1 4");

		TNTTblScan *scanHandle;
		TNTTransaction *trx = beginTransaction(OP_WRITE);

		trx->setTrxId(20);

		scanHandle = m_table->tableScan(m_session, OP_WRITE, &m_opInfo, subRec->m_numCols, subRec->m_columns);

		u16 updateColumns[1] = {1};
		if ( !scanHandle->isUpdateSubRecordSet()) {
			scanHandle->setUpdateSubRecord(1, updateColumns);
		}
		uint commentCount = 20;

		u64 spAfterScan = m_session->getMemoryContext()->setSavepoint();

		memset(subRec->m_data, 0, tableDef->m_maxRecSize);
		while (m_table->getNext(scanHandle, subRec->m_data)) {
			
			// ���·������ֶ�CommentCount
			SubRecord *updateRec = srb.createSubRecordByName("CommentCount", &commentCount);
			CPPUNIT_ASSERT(m_table->updateCurrent(scanHandle, updateRec->m_data));

			freeSubRecord(updateRec);
			
			memset(subRec->m_data, 0, tableDef->m_maxRecSize);
		}

		m_table->endScan(scanHandle);

		commitTransaction(trx);
		freeSubRecord(subRec);
	}

	// update�����ֶ�AccessCount������IDΪ30
	{
		TableDef *tableDef = m_table->getNtseTable()->getTableDef();

		SubRecordBuilder srb(tableDef, REC_REDUNDANT);
		// Ҫ�󷵻�BlogID, CommentCount, Title�ֶ�
		SubRecord *subRec = srb.createEmptySbById(tableDef->m_maxRecSize, "0 1 4");

		TNTTblScan *scanHandle;
		TNTTransaction *trx = beginTransaction(OP_WRITE);

		trx->setTrxId(30);

		scanHandle = m_table->tableScan(m_session, OP_WRITE, &m_opInfo, subRec->m_numCols, subRec->m_columns);

		u16 updateColumns[1] = {3};
		if ( !scanHandle->isUpdateSubRecordSet()) {
			scanHandle->setUpdateSubRecord(1, updateColumns);
		}
		u64 userId = 50;

		u64 spAfterScan = m_session->getMemoryContext()->setSavepoint();

		memset(subRec->m_data, 0, tableDef->m_maxRecSize);
		while (m_table->getNext(scanHandle, subRec->m_data)) {

			// ����������(UserID, AccessCount)�е�AccessCount�ֶ�
			SubRecord *updateRec = srb.createSubRecordByName("AccessCount", &userId);
			CPPUNIT_ASSERT(m_table->updateCurrent(scanHandle, updateRec->m_data));

			freeSubRecord(updateRec);

			memset(subRec->m_data, 0, tableDef->m_maxRecSize);
		}

		m_table->endScan(scanHandle);

		commitTransaction(trx);
		freeSubRecord(subRec);
	}

	TNTTrxSys *trxSys = m_db->getTransSys();
	TNTTransaction *trx;
	u64	trxArray[10];
	int activeTrxCnt;

	// 1. ���� index coverage scan�����������ɼ�������0����¼
	trx = beginTransaction(OP_READ);
	trxArray[0] = 30;
	trxArray[1] = 20;
	trxArray[2] = 10;
	activeTrxCnt=3;
	trxSys->trxAssignReadView(trx, trxArray, activeTrxCnt);
	uint retRows = fetchBlogCountFromSecIndex(OP_READ);
	
	CPPUNIT_ASSERT(retRows == 0);
	commitTransaction(trx);
	trx = NULL;

	// 2. ���� index coverage scan���ڴ淵�ؼ�¼�����ɼ�������double checkһ�γɹ���һ��ʧ��
	trx = beginTransaction(OP_READ);
	trxArray[0] = 30;
	trxArray[1] = 20;
	activeTrxCnt= 2;

	trxSys->trxAssignReadView(trx, trxArray, activeTrxCnt);
	retRows = fetchBlogCountFromSecIndex(OP_READ);

	CPPUNIT_ASSERT(retRows == 1);
	commitTransaction(trx);
	trx = NULL;
}

/*
* �����������������֮��Ļع������Ƿ���ȷ
*/
void TNTTableTestCase::testIndexUpdRollback() {
	TableDef *tableDef = m_table->getNtseTable()->getTableDef();

	Record *rec = NULL;

	// Insertһ����¼������IDΪ10
	{
		RecordBuilder rb(m_tableDef, RID(0, 0), REC_REDUNDANT);
		rb.appendBigInt(1);
		rb.appendInt(10);
		rb.appendSmallInt(2);
		rb.appendInt(100);
		char title[25];
		for (size_t l = 0; l < sizeof(title) - 1; l++)
			title[l] = (char )('A' + System::random() % 26);
		title[sizeof(title) - 1] = '\0';
		rb.appendChar(title);
		rb.appendBigInt(1);

		rec = rb.getRecord(m_tableDef->m_maxRecSize);

		uint dupIndex = 15555;
		rec->m_format = REC_MYSQL;

		TNTTransaction *trx = beginTransaction(OP_WRITE);
		trx->setTrxId(10);
		//initTNTOpInfo(TL_X);

		CPPUNIT_ASSERT((rec->m_rowId = m_table->insert(m_session, rec->m_data, &dupIndex, &m_opInfo)) != INVALID_ROW_ID);

		commitTransaction(trx);
		
		// ��ʱ��free record��������Ҫ�Ա���֤��ȷ��
	}

	// update�������ֶ�CommentCount������IDΪ20
	{
		SubRecordBuilder srb(tableDef, REC_REDUNDANT);
		// Ҫ�󷵻�BlogID, CommentCount, Title�ֶ�
		SubRecord *subRec = srb.createEmptySbById(tableDef->m_maxRecSize, "0 1 4");

		TNTTblScan *scanHandle;
		TNTTransaction *trx = beginTransaction(OP_WRITE);

		trx->setTrxId(20);

		scanHandle = m_table->tableScan(m_session, OP_WRITE, &m_opInfo, subRec->m_numCols, subRec->m_columns);

		u16 updateColumns[1] = {2};
		if ( !scanHandle->isUpdateSubRecordSet()) {
			scanHandle->setUpdateSubRecord(1, updateColumns);
		}
		uint commentCount = 10;

		u64 spAfterScan = m_session->getMemoryContext()->setSavepoint();

		memset(subRec->m_data, 0, tableDef->m_maxRecSize);
		while (m_table->getNext(scanHandle, subRec->m_data)) {

			// ���·������ֶ�CommentCount
			SubRecord *updateRec = srb.createSubRecordByName("CommentCount", &commentCount);
			CPPUNIT_ASSERT(m_table->updateCurrent(scanHandle, updateRec->m_data));

			freeSubRecord(updateRec);

			memset(subRec->m_data, 0, tableDef->m_maxRecSize);
		}

		m_table->endScan(scanHandle);

		commitTransaction(trx);
		freeSubRecord(subRec);
	}

	// update�����ֶ�AccessCount������IDΪ30
	// ��������Ҫ��Rollback
	SubRecordBuilder srb(tableDef, REC_REDUNDANT);
	// Ҫ�󷵻�BlogID, CommentCount, Title�ֶ�
	SubRecord *subRec = srb.createEmptySbById(tableDef->m_maxRecSize, "0 1 4");

	TNTTblScan *scanHandle;
	TNTTransaction *trx = beginTransaction(OP_WRITE);

	trx->setTrxId(30);

	scanHandle = m_table->tableScan(m_session, OP_WRITE, &m_opInfo, subRec->m_numCols, subRec->m_columns);

	u16 updateColumns[1] = {3};
	if ( !scanHandle->isUpdateSubRecordSet()) {
		scanHandle->setUpdateSubRecord(1, updateColumns);
	}
	u64 userId = 50;

	u64 spAfterScan = m_session->getMemoryContext()->setSavepoint();

	memset(subRec->m_data, 0, tableDef->m_maxRecSize);
	while (m_table->getNext(scanHandle, subRec->m_data)) {

		// ����������(UserID, AccessCount)�е�AccessCount�ֶ�
		SubRecord *updateRec = srb.createSubRecordByName("AccessCount", &userId);
		CPPUNIT_ASSERT(m_table->updateCurrent(scanHandle, updateRec->m_data));

		freeSubRecord(updateRec);

		memset(subRec->m_data, 0, tableDef->m_maxRecSize);
	}

	m_table->endScan(scanHandle);

	// �ع�����
	rollbackTransaction(trx);
	freeSubRecord(subRec);

	// ����һ��scan����֤���ݵ���ȷ��
	trx = beginTransaction(OP_READ);
	fetchBlogCount(&rec, 1, OP_READ, false);
	commitTransaction(trx);

	trx = beginTransaction(OP_READ);
	fetchBlogCount(&rec, 1, OP_READ, true);
	commitTransaction(trx);

	freeRecord(rec);
}

void TNTTableTestCase::printSubRecord(SubRecord *subRec) {
/*
	u16			m_numCols;	
	u16			*m_columns;
	uint		m_size;		
	byte		*m_data;	
*/
	TableDef *tableDef = m_table->getNtseTable()->getTableDef();

	for (u16 i = 0; i < subRec->m_numCols; i++) {
		u16 cno = subRec->m_columns[i];
		u64 num1 = 0;
		uint num2 = 0;

		u16 offset = tableDef->m_columns[cno]->m_offset;
		u16 size = tableDef->m_columns[cno]->m_size;
		byte *b = subRec->m_data + offset; 
		
		switch (tableDef->getColumnDef(cno)->m_type) {
			case CT_BIGINT: 
				num1 = (u64)(b[0] | b[1]<<8 | b[2]<<16 | b[3]<<24 | b[4]<<32 | b[5]<<40 | b[6]<<48 | b[7]<<56);
				printf("\t %I64d", num1);
				break;
			case CT_INT:
				num2 = (uint)(b[0] | b[1]<<8 | b[2]<<16 | b[3]<<24);
				printf("\t %d", num2);
				break;
			case CT_TINYINT:
				break;;
			default:
				break;
		}
	}
}

/**
 * ����������������
 */
void TNTTableTestCase::testIndexScanAfterAdd() {
	
	TableDefBuilder *builder = new TableDefBuilder(TableDef::INVALID_TABLEID, "space", "BlogCount");
	TableDef *tableDef = NULL;

	builder->addColumn("BlogID", CT_BIGINT, false)->addColumn("CommentCount", CT_INT);
	builder->addColumn("TrackbackCount", CT_SMALLINT)->addColumn("AccessCount", CT_INT);
	builder->addColumnS("Title", CT_CHAR, 50);
	builder->addColumn("UserID", CT_BIGINT, false);
	builder->addIndex("PRIMARY", true, true, false, "BlogID", 0, NULL);

	builder->addIndex("IDX_BLOGCOUNT_UID_AC", false, false, false, "UserID", 0, "AccessCount", 0, NULL);
	tableDef = builder->getTableDef();
	delete builder;

	// ɾ��Init������Blog���ϵ�IDX_BLOGCOUNT_UID_AC����
	TNTTransaction *trx = beginTransaction(OP_WRITE);
	m_session->setTrans(trx);

	m_table->lockMeta(m_session, IL_S, -1, __FILE__, __LINE__);

	EXCPT_OPER(m_db->dropIndex(m_session, m_table, 1));

	CPPUNIT_ASSERT(m_table->getMetaLock(m_session) == IL_S);
	m_table->unlockMeta(m_session,m_table->getMetaLock(m_session));

	commitTransaction(trx);
	m_session->setTrans(NULL);


	//��Ӳ���������
	Record **rows = populateBlogCount(m_db, m_table, 10);
	m_opInfo.m_selLockType = TL_X;
	uint rowsUpdated = 0;
	EXCPT_OPER(rowsUpdated = updDelBlogCount(m_db, m_table, OP_UPDATE, true, rows, 10));
	assert(rowsUpdated == 5);

	for (uint i = 0; i < 10; i++) {
		freeRecord(rows[i]);
	}
	delete [] rows;
	
	trx = beginTransaction(OP_WRITE);
	m_session->setTrans(trx);

	// add index
	EXCPT_OPER(m_db->addIndex(m_session, m_table, 1, (const IndexDef **)&tableDef->m_indice[1]));

	commitTransaction(trx);
	m_session->setTrans(NULL);

	tableDef->m_id = m_table->getNtseTable()->getTableDef()->m_id;

	// ���¼ӵ�������ȡ��¼
	{
		trx = beginTransaction(OP_READ);
		m_session->setTrans(trx);

		// u64 blogId = 1;
		u64 userId = 1;
// 		SubRecordBuilder keyBuilder(m_table->getNtseTable()->getTableDef(), KEY_PAD);
// 		SubRecord *key = keyBuilder.createSubRecordByName("UserID", &userId);
// 		key->m_rowId = INVALID_ROW_ID;

		SubRecordBuilder srb(m_table->getNtseTable()->getTableDef(), REC_REDUNDANT);
		SubRecord *subRec = srb.createEmptySbById(m_table->getNtseTable()->getTableDef()->m_maxRecSize, "0 1 3 5");

		IndexScanCond cond(1, NULL, true, true, false);

		m_opInfo.m_selLockType = TL_NO;
		TNTTblScan * scanHandle = m_table->indexScan(m_session, OP_READ, &m_opInfo, &cond, subRec->m_numCols, subRec->m_columns);
		for (uint i = 0; i < 10; i++) {
			CPPUNIT_ASSERT(m_table->getNext(scanHandle, subRec->m_data));
			printf("\n %d Row: ", i);
			printSubRecord(subRec);
		}
		CPPUNIT_ASSERT(!m_table->getNext(scanHandle, subRec->m_data));
		m_table->endScan(scanHandle);

		commitTransaction(trx);
		m_session->setTrans(NULL);

		freeSubRecord(subRec);
//		freeSubRecord(key);
	}

	// drop index 
	trx = beginTransaction(OP_WRITE);
	m_session->setTrans(trx);

	m_table->lockMeta(m_session, IL_S, -1, __FILE__, __LINE__);

	EXCPT_OPER(m_db->dropIndex(m_session, m_table, 1));

	CPPUNIT_ASSERT(m_table->getMetaLock(m_session) == IL_S);
	m_table->unlockMeta(m_session,m_table->getMetaLock(m_session));

	commitTransaction(trx);
	m_session->setTrans(NULL);


	// ɾ������֮������һ�β��ԣ�ͨ������������ȡ��¼
	{
		trx = beginTransaction(OP_READ);
		m_session->setTrans(trx);

		u64 blogId = 1;
		// u64	userId = 200;
		SubRecordBuilder keyBuilder(m_table->getNtseTable()->getTableDef(), KEY_PAD);
		SubRecord *key = keyBuilder.createSubRecordByName("BlogID", &blogId);
		key->m_rowId = INVALID_ROW_ID;

		SubRecordBuilder srb(m_table->getNtseTable()->getTableDef(), REC_REDUNDANT);
		SubRecord *subRec = srb.createEmptySbById(m_table->getNtseTable()->getTableDef()->m_maxRecSize, "0 1 3 5");

		IndexScanCond cond(0, key, true, true, false);

		// m_opInfo.m_selLockType = TL_NO;
		TNTTblScan * scanHandle = m_table->indexScan(m_session, OP_READ, &m_opInfo, &cond, subRec->m_numCols, subRec->m_columns);
		for (uint i = 0; i < 10; i++) {
			CPPUNIT_ASSERT(m_table->getNext(scanHandle, subRec->m_data));
			printf("\n %d Row: ", i);
			printSubRecord(subRec);
		}
		CPPUNIT_ASSERT(!m_table->getNext(scanHandle, subRec->m_data));
		m_table->endScan(scanHandle);

		commitTransaction(trx);
		m_session->setTrans(NULL);

		freeSubRecord(subRec);
		freeSubRecord(key);
	}

	// ɾ���������� 
	m_table->getNtseTable()->getTableDef()->m_cacheUpdate = true;
	m_table->getNtseTable()->getRecords()->alterUseMms(m_session, true);


	trx = beginTransaction(OP_WRITE);
	m_session->setTrans(trx);

	m_table->lockMeta(m_session, IL_S, -1, __FILE__, __LINE__);

	EXCPT_OPER(m_db->dropIndex(m_session, m_table, 0));

	CPPUNIT_ASSERT(m_table->getMetaLock(m_session) == IL_S);
	m_table->unlockMeta(m_session,m_table->getMetaLock(m_session));

	commitTransaction(trx);
	m_session->setTrans(NULL);

	// ɾ������֮������һ�β��ԣ�ȫ��ɨ���ȡ��¼
	{
		trx = beginTransaction(OP_READ);
		m_session->setTrans(trx);

		SubRecordBuilder srb(m_table->getNtseTable()->getTableDef(), REC_REDUNDANT);
		SubRecord *subRec = srb.createEmptySbById(m_table->getNtseTable()->getTableDef()->m_maxRecSize, "0 1 3 5");


		// m_opInfo.m_selLockType = TL_NO;
	//	TNTTblScan * scanHandle = m_table->indexScan(m_session, OP_READ, &m_opInfo, &cond, subRec->m_numCols, subRec->m_columns);
		TNTTblScan * scanHandle = m_table->tableScan(m_session, OP_READ, &m_opInfo, subRec->m_numCols, subRec->m_columns);
		for (uint i = 0; i < 10; i++) {
			CPPUNIT_ASSERT(m_table->getNext(scanHandle, subRec->m_data));
			printf("\n %d Row: ", i);
			printSubRecord(subRec);
		}
		CPPUNIT_ASSERT(!m_table->getNext(scanHandle, subRec->m_data));
		m_table->endScan(scanHandle);

		commitTransaction(trx);
		m_session->setTrans(NULL);

		freeSubRecord(subRec);
	}



	delete tableDef;
}

/** ����TNT Table����AutoInc���� */
void TNTTableTestCase::testAutoInc() {
	TableDef *tableDef = m_table->getNtseTable()->getTableDef();
	TNTTransaction *trx  = NULL;

	// INSERT INTO BlogCount(BlogID, CommentCount, TrackbackCount, AccessCount, Title, UserID)
	// VALUES(1, ...)
	Record **rows = populateBlogCount(m_db, m_table, 1);

	// ��ʼ��AutoIncȡֵ
	m_table->enterAutoincMutex();
	m_table->initAutoinc(1);
	m_table->exitAutoincMutex();

	// INSERT INTO BlogCount(BlogID, CommentCount, TrackbackCount, AccessCount, Title, UserID) VALUES(2, ...) 
	// ���У�BlogIDͨ��AutoInc����
	{
		trx = beginTransaction(OP_WRITE, NULL, 0);
		m_session->setTrans(trx);
		//initTNTOpInfo(TL_X);

		u64 blogId;

		m_table->enterAutoincMutex();
		blogId = m_table->getAutoinc();
		CPPUNIT_ASSERT(blogId == 1);
		blogId = 2;
		m_table->updateAutoincIfGreater(blogId);
		CPPUNIT_ASSERT(m_table->getAutoinc() == 2);
		m_table->exitAutoincMutex();

		*((u64 *)(rows[0]->m_data + tableDef->m_columns[0]->m_offset)) = blogId;
		IUSequence<TNTTblScan *> *iuSeq = m_table->insertForDupUpdate(m_session, rows[0]->m_data, &m_opInfo);
		CPPUNIT_ASSERT(!iuSeq);

		commitTransaction(trx);
		m_session->setTrans(NULL);
	}


	// ��ӡ��ʱ���еļ�¼
	{
		TNTTransaction *trx = beginTransaction(OP_READ);

		uint retRows = fetchBlogCount(NULL, 0, OP_READ, false);

		printf("\n\n\nNumber rows returned = %d\n", retRows);

		commitTransaction(trx);
		trx = NULL;
	}

	freeRecord(rows[0]);
	delete [] rows;	
}

void TNTTableTestCase::testPurge() {
	TNTTblScan *scan = NULL;
	u8 vTblIndex = 0;
	TNTTrxSys *trxSys = m_db->getTransSys();
	u16 version = m_table->getMRecords()->getVersion();
	MemoryContext *ctx = m_session->getMemoryContext();
	McSavepoint msp(ctx);
	u16 *readCols = (u16 *)ctx->alloc(sizeof(u16) * m_tableDef->m_numCols);
	for (u16 i = 0; i < m_tableDef->m_numCols; ++i) {
		readCols[i] = i;
	}

	u64 sp1 = ctx->setSavepoint();
	//��������
	u64 txnId1 = 10005;
	trxSys->setMaxTrxId(txnId1 - 1);
	uint numRows = 1000;
	uint midRows = numRows/2;
	Record **redRecs = populateBlogCount(m_db, m_table, numRows);

	//�������ݣ������������ڴ���������ݣ�Ϊ�״θ���
	u64 txnId2 = 20005;
	uint rowsUpdated = 0;
	trxSys->setMaxTrxId(txnId2 - 1);
	EXCPT_OPER(rowsUpdated = updDelBlogCount(m_db, m_table, OP_UPDATE, redRecs, numRows));
	CPPUNIT_ASSERT(rowsUpdated == numRows);
	//�����������
	m_table->getMRecords()->m_heap->defragFreeList(m_session);

	u64 txnId3 = 30005;
	uint delNumRow = midRows;
	uint rowsDel = 0;
	trxSys->setMaxTrxId(txnId3 - 1);
	EXCPT_OPER(rowsDel = updDelBlogCount(m_db, m_table, OP_DELETE, redRecs, delNumRow));
	CPPUNIT_ASSERT(rowsDel == delNumRow);

	//check������tnt�ɼ�
	bool ntseVisible, tntVisible;
	Record *redRec1 = NULL;
	u64 txnId4 = (txnId2 + txnId3) >> 1;
	trxSys->setMaxTrxId(txnId4 - 1);
	TNTTransaction *trx1 = beginTransaction(OP_WRITE);
	byte *mysqlRow = (byte *)ctx->alloc(m_tableDef->m_maxRecSize);
	for (u32 i = 0; i < numRows; i++) {
		scan = m_table->positionScan(m_session, OP_READ, &m_opInfo, m_tableDef->m_numCols, readCols);
		if (i < delNumRow) {
			//�����Ѿ���delete��
			CPPUNIT_ASSERT(!m_table->getNext(scan, mysqlRow, redRecs[i]->m_rowId));
		} else {
			CPPUNIT_ASSERT(m_table->getNext(scan, mysqlRow, redRecs[i]->m_rowId));
			redRec1 = scan->getRecord();
			CPPUNIT_ASSERT(redRec1->m_format == redRecs[i]->m_format);
			CPPUNIT_ASSERT(redRec1->m_rowId == redRecs[i]->m_rowId);
			CPPUNIT_ASSERT(redRec1->m_size == redRecs[i]->m_size);
			CPPUNIT_ASSERT(memcmp(redRec1->m_data, redRecs[i]->m_data, redRecs[i]->m_size) == 0);
		}
		m_table->endScan(scan);

		scan = m_table->positionScan(m_session, OP_READ, &m_opInfo, m_tableDef->m_numCols, readCols);
		scan->setCurrentRid(redRecs[i]->m_rowId);
		tntVisible = m_table->getMRecords()->getRecord(scan, 0, &ntseVisible);
		if (i < delNumRow) {
			CPPUNIT_ASSERT(tntVisible == false);
			CPPUNIT_ASSERT(ntseVisible == false);
		} else {
			CPPUNIT_ASSERT(tntVisible == true);
		}
		m_table->endScan(scan);
	}
	commitTransaction(trx1);
	ctx->resetToSavepoint(sp1);

	trxSys->setMaxTrxId(txnId4 - 1);
	TNTTransaction *purgeTrx = beginTransaction(OP_READ);
	purgeTrx->trxAssignReadView();
	sp1 = ctx->setSavepoint();
	//purge phase 1
	m_table->purgePhase1(m_session, purgeTrx);
	//purge phase 2
	m_table->purgePhase2(m_session, purgeTrx);
	commitTransaction(purgeTrx);

	//check������֤tnt���ɼ�ntse�ɼ�
	Record *redRec2 = NULL;
	trxSys->setMaxTrxId(txnId4 - 1);
	mysqlRow = (byte *)ctx->alloc(m_tableDef->m_maxRecSize);
	TNTTransaction *trx2 = beginTransaction(OP_WRITE);
	for (u32 i = 0; i < numRows; i++) {
		scan = m_table->positionScan(m_session, OP_READ, &m_opInfo, m_tableDef->m_numCols, readCols);
		if (i < rowsDel) {
			//�����Ѿ���delete��
			CPPUNIT_ASSERT(!m_table->getNext(scan, mysqlRow, redRecs[i]->m_rowId));
		} else {
			CPPUNIT_ASSERT(m_table->getNext(scan, mysqlRow, redRecs[i]->m_rowId));
			redRec2 = scan->getRecord();
			CPPUNIT_ASSERT(redRec2->m_format == redRecs[i]->m_format);
			CPPUNIT_ASSERT(redRec2->m_rowId == redRecs[i]->m_rowId);
			CPPUNIT_ASSERT(redRec2->m_size == redRecs[i]->m_size);
			CPPUNIT_ASSERT(memcmp(redRec2->m_data, redRecs[i]->m_data, redRecs[i]->m_size) == 0);
		}
		m_table->endScan(scan);

		scan = m_table->positionScan(m_session, OP_READ, &m_opInfo, m_tableDef->m_numCols, readCols);
		scan->setCurrentRid(redRecs[i]->m_rowId);
		tntVisible = m_table->getMRecords()->getRecord(scan, 0, &ntseVisible);
		if (i < delNumRow) {
			CPPUNIT_ASSERT(tntVisible == false);
			CPPUNIT_ASSERT(ntseVisible == false);
		} else {
			CPPUNIT_ASSERT(tntVisible == false);
			CPPUNIT_ASSERT(ntseVisible == true);
		}
		m_table->endScan(scan);
	}
	commitTransaction(trx2);

	//�ڴ��ͷ�
	for (u32 i = 0; i < numRows; i++) {
		freeRecord(redRecs[i]);
	}
	delete[] redRecs;

	ctx->resetToSavepoint(sp1);
}


// ���BUG JIRA NTSETNT-187
void TNTTableTestCase::testPurgeDeleteRowReuse() {

	TNTTrxSys *trxSys = m_db->getTransSys();

	MemoryContext *ctx = m_session->getMemoryContext();
	McSavepoint msp(ctx);

	u16 *readCols = (u16 *)ctx->alloc(sizeof(u16) * m_tableDef->m_numCols);
	for (u16 i = 0; i < m_tableDef->m_numCols; ++i) {
		readCols[i] = i;
	}

	u64 sp1 = ctx->setSavepoint();
	//��������
	u64 txnId1 = 10005;
	trxSys->setMaxTrxId(txnId1 - 1);
	uint numRows = 1;
	Record **redRecs = populateBlogCount(m_db, m_table, numRows);

	RowId recRowId = redRecs[0]->m_rowId;

	//ɾ�����ݣ������������ڴ���������ݣ�Ϊ�״�ɾ��
	u64 txnId2 = 20005;
	u64 rowsDeleted;
	trxSys->setMaxTrxId(txnId2 - 1);
	EXCPT_OPER(rowsDeleted = updDelBlogCount(m_db, m_table, OP_DELETE, redRecs, numRows));
	CPPUNIT_ASSERT(rowsDeleted == numRows);

	TNTTransaction *lockTrx = beginTransaction(OP_WRITE);
	lockTrx->lockRow(TL_X, recRowId, m_tableDef->m_id);
	
	// ����һ��purge����һ�׶�ɾ������¼���ڶ��׶��ڽ��е����������������м�¼����
	u64 txnId3 = 30005;
	trxSys->setMaxTrxId(txnId3 - 1);
	TNTTransaction *purgeTrx = beginTransaction(OP_READ);
	purgeTrx->trxAssignReadView();
	//purge phase 1
	m_table->purgePhase1(m_session, purgeTrx);
	//purge phase 2
	m_table->purgePhase2(m_session, purgeTrx);
	commitTransaction(purgeTrx);
	commitTransaction(lockTrx);

	//�ڴ��ͷ�
	for (u32 i = 0; i < numRows; i++) {
		freeRecord(redRecs[i]);
	}
	delete[] redRecs;

	if(m_table->getMRecords()->getMHeapStat().m_total == 0) {
		return;
	} else {
		u64 txnId4 = 50005;
		trxSys->setMaxTrxId(txnId4 - 1);
		PurgeTableThread purgeTableThread(m_db, m_table);
		purgeTableThread.enableSyncPoint(SP_MHEAP_BEGIN_PURGE_COPY_PAGE_AFTER);
		purgeTableThread.start();
		purgeTableThread.joinSyncPoint(SP_MHEAP_BEGIN_PURGE_COPY_PAGE_AFTER);

		// ������������֮ǰɾ����rowId
		u64 txnId5 = 80005;
		trxSys->setMaxTrxId(txnId5 - 1);
		Record **newRedRecs = populateBlogCount(m_db, m_table, numRows);
		assert(newRedRecs[0]->m_rowId == recRowId);

		

		purgeTableThread.disableSyncPoint(SP_MHEAP_BEGIN_PURGE_COPY_PAGE_AFTER);
		purgeTableThread.notifySyncPoint(SP_MHEAP_BEGIN_PURGE_COPY_PAGE_AFTER);
		purgeTableThread.join(-1);



		//check������֤tnt���ɼ�ntse�ɼ�
		Record *redRec2 = NULL;
		byte *mysqlRow = (byte *)ctx->alloc(m_tableDef->m_maxRecSize);
		u64 txnId6 = 90005;
		trxSys->setMaxTrxId(txnId6 - 1);
		TNTTransaction *trx2 = beginTransaction(OP_WRITE);
		for (u32 i = 0; i < numRows; i++) {
			TNTTblScan *scan = m_table->positionScan(m_session, OP_READ, &m_opInfo, m_tableDef->m_numCols, readCols);
			NTSE_ASSERT(m_table->getNext(scan, mysqlRow, newRedRecs[i]->m_rowId));
			m_table->endScan(scan);
		}
		commitTransaction(trx2);

		//�ڴ��ͷ�
		for (u32 i = 0; i < numRows; i++) {
			freeRecord(newRedRecs[i]);
		}
		delete[] newRedRecs;
	}
}



void TNTTableTestCase::initTNTOpInfo(TLockMode lockMode) {
	
	m_opInfo.m_selLockType = lockMode;
	m_opInfo.m_sqlStatStart= true;
}



/*
* ���Լ�����ͻ/������ʱ�����
*/
void TNTTableTestCase::testLockRowConflict() {
	uint numRows = 1;
	Record **rows;

	EXCPT_OPER(rows = populateBlogCount(m_db, m_table, numRows));

	for (uint i = 0; i < numRows; i++)
		freeRecord(rows[i]);
	delete [] rows;

	RowLockConflictThread rowLockThread(m_db, m_table, m_tableDef);

	rowLockThread.enableSyncPoint(SP_ROW_LOCK_BEFORE_LOCK);
	rowLockThread.enableSyncPoint(SP_ROW_LOCK_AFTER_TRYLOCK);

	rowLockThread.start();
	rowLockThread.joinSyncPoint(SP_ROW_LOCK_BEFORE_LOCK);

	// ���߳̽���ȫ��ɨ��
	TNTTransaction *trx = beginTransaction(OP_WRITE);

	SubRecordBuilder srb(m_tableDef, REC_REDUNDANT);
	// Ҫ�󷵻�BlogID, CommentCount, Title�ֶ�
	SubRecord *subRec = srb.createEmptySbById(m_tableDef->m_maxRecSize, "0 1 4");
	SubRecord *key = NULL;

	TNTTblScan *scanHandle;
	scanHandle = m_table->tableScan(m_session, OP_WRITE, &m_opInfo, subRec->m_numCols, subRec->m_columns);

	memset(subRec->m_data, 0, m_tableDef->m_maxRecSize);

	while (m_table->getNext(scanHandle, subRec->m_data)) 
		memset(subRec->m_data, 0, m_tableDef->m_maxRecSize);

	m_table->endScan(scanHandle);

	freeSubRecord(subRec);

	// ����rowLockThread
	rowLockThread.disableSyncPoint(SP_ROW_LOCK_BEFORE_LOCK);
	rowLockThread.notifySyncPoint(SP_ROW_LOCK_BEFORE_LOCK);

	// �ȴ�rowLockThread���е�LockRow
	rowLockThread.joinSyncPoint(SP_ROW_LOCK_AFTER_TRYLOCK);

	commitTransaction(trx);

	rowLockThread.disableSyncPoint(SP_ROW_LOCK_AFTER_TRYLOCK);
	rowLockThread.notifySyncPoint(SP_ROW_LOCK_AFTER_TRYLOCK);
	
	rowLockThread.join(-1);
}

/*
* ����TNTTable��getNext�����׳��쳣�������Lock Timeout�쳣
*/
void TNTTableTestCase::testGetNextWithException() {
	uint numRows = 1;
	Record **rows;

	EXCPT_OPER(rows = populateBlogCount(m_db, m_table, numRows));

	for (uint i = 0; i < numRows; i++)
		freeRecord(rows[i]);
	delete [] rows;

	RowLockConflictThread rowLockThread(m_db, m_table, m_tableDef);

	rowLockThread.enableSyncPoint(SP_ROW_LOCK_BEFORE_LOCK);
	rowLockThread.enableSyncPoint(SP_ROW_LOCK_AFTER_TRYLOCK);

	rowLockThread.start();
	rowLockThread.joinSyncPoint(SP_ROW_LOCK_BEFORE_LOCK);

	// ���߳̽���ȫ��ɨ��
	TNTTransaction *trx = beginTransaction(OP_WRITE);

	SubRecordBuilder srb(m_tableDef, REC_REDUNDANT);
	// Ҫ�󷵻�BlogID, CommentCount, Title�ֶ�
	SubRecord *subRec = srb.createEmptySbById(m_tableDef->m_maxRecSize, "0 1 4");
	SubRecord *key = NULL;

	TNTTblScan *scanHandle;
	scanHandle = m_table->tableScan(m_session, OP_WRITE, &m_opInfo, subRec->m_numCols, subRec->m_columns);

	memset(subRec->m_data, 0, m_tableDef->m_maxRecSize);

	while (m_table->getNext(scanHandle, subRec->m_data)) 
		memset(subRec->m_data, 0, m_tableDef->m_maxRecSize);

	m_table->endScan(scanHandle);

	freeSubRecord(subRec);

	// ����rowLockThread
	rowLockThread.disableSyncPoint(SP_ROW_LOCK_BEFORE_LOCK);
	rowLockThread.notifySyncPoint(SP_ROW_LOCK_BEFORE_LOCK);

	// �ȴ�rowLockThread���е�LockRow
	rowLockThread.joinSyncPoint(SP_ROW_LOCK_AFTER_TRYLOCK);
	rowLockThread.disableSyncPoint(SP_ROW_LOCK_AFTER_TRYLOCK);
	rowLockThread.notifySyncPoint(SP_ROW_LOCK_AFTER_TRYLOCK);

	// sleep һ��ʱ�䣬��֤�̼߳����ȴ���ʱ��msΪ��λ
	Thread::msleep(10000);

	commitTransaction(trx);

	rowLockThread.join(-1);
}

/*
* ����һ�������һ�������������μ������ڶ��β�������������ǰ����(TNTTable::getNext��)�����
* ��֤����ڶ��μӷ���û�н���һ�εļ����������������
*/
void TNTTableTestCase::testLockOneRowTwice() {
	uint numRows = 1;
	Record **rows;

	// ���в���һ����¼
	EXCPT_OPER(rows = populateBlogCount(m_db, m_table, numRows));

	for (uint i = 0; i < numRows; i++)
		freeRecord(rows[i]);

	delete [] rows;

	TableDef *tableDef  = m_table->getNtseTable()->getTableDef();
	TNTTransaction *trx = beginTransaction(OP_WRITE);

	// ɾ�����еļ�¼
	{
		uint delRows = 0;

		SubRecordBuilder srb(tableDef, REC_REDUNDANT);
		// Ҫ�󷵻�BlogID, CommentCount, Title�ֶ�
		SubRecord *subRec = srb.createEmptySbById(tableDef->m_maxRecSize, "0 1 4");

		TNTTblScan *scanHandle;
		scanHandle = m_table->tableScan(m_session, OP_WRITE, &m_opInfo, subRec->m_numCols, subRec->m_columns);

		u64 spAfterScan = m_session->getMemoryContext()->setSavepoint();

		printf("\nbegin delete\n");

		memset(subRec->m_data, 0, tableDef->m_maxRecSize);
		while (m_table->getNext(scanHandle, subRec->m_data)) {

			m_table->deleteCurrent(scanHandle);
			delRows++;
			memset(subRec->m_data, 0, tableDef->m_maxRecSize);
		}

		// һ��ֻɾ����һ�м�¼
		CPPUNIT_ASSERT(delRows == 1);

		m_table->endScan(scanHandle);

		freeSubRecord(subRec);
	}
	

	// ��ǰ�����ٴν��е�ǰ����ɾ����ɼ����Ὣɾ�������
	{
		uint gotRows = 0;

		SubRecordBuilder srb(tableDef, REC_REDUNDANT);
		// Ҫ�󷵻�BlogID, CommentCount, Title�ֶ�
		SubRecord *subRec = srb.createEmptySbById(tableDef->m_maxRecSize, "0 1 4");

		TNTTblScan *scanHandle;
		scanHandle = m_table->tableScan(m_session, OP_WRITE, &m_opInfo, subRec->m_numCols, subRec->m_columns);

		u64 spAfterScan = m_session->getMemoryContext()->setSavepoint();

		printf("\nbegin second scan\n");

		memset(subRec->m_data, 0, tableDef->m_maxRecSize);
		while (m_table->getNext(scanHandle, subRec->m_data)) {
			CPPUNIT_ASSERT(false);
		}

		// һ��ֻɾ����һ�м�¼
		CPPUNIT_ASSERT(gotRows == 0);

		m_table->endScan(scanHandle);

		freeSubRecord(subRec);
	}
	
	// ���գ��ύ����
	commitTransaction(trx);
}

RowLockConflictThread::RowLockConflictThread(TNTDatabase *db, TNTTable *table, TableDef *tableDef): Thread("RowLockConflictThread") {
	m_db		= db;
	m_table		= table;
	m_tableDef	= tableDef;

	m_opInfo.m_mysqlHasLocked = false;
	m_opInfo.m_mysqlOper = true;
	m_opInfo.m_selLockType = TL_X;
	m_opInfo.m_sqlStatStart = true;
}

void RowLockConflictThread::run() {
	m_conn = m_db->getNtseDb()->getConnection(false);
	m_session = m_db->getNtseDb()->getSessionManager()->allocSession("TNTTable_Lock_Confict", m_conn);

	m_trx = beginTransaction(OP_WRITE);

	try {
		blogCountScanTest(OP_WRITE, true);
		commitTransaction();
	} catch (NtseException &e) {
		printf("%s \n", e.getMessage());

		rollbackTransaction();
	}
	
	m_db->getNtseDb()->getSessionManager()->freeSession(m_session);
	m_db->getNtseDb()->freeConnection(m_conn);
}

void RowLockConflictThread::blogCountScanTest(OpType intention, bool tableScan) throw (NtseException) {
	SubRecordBuilder srb(m_tableDef, REC_REDUNDANT);
	// Ҫ�󷵻�BlogID, CommentCount, Title�ֶ�
	SubRecord *subRec = srb.createEmptySbById(m_tableDef->m_maxRecSize, "0 1 4");
	SubRecord *key = NULL;

	TNTTblScan *scanHandle;
	if (tableScan)
		scanHandle = m_table->tableScan(m_session, intention, &m_opInfo, subRec->m_numCols, subRec->m_columns);
	else {
		// ʹ������������ָ����ʼ����ΪBlogID >= 0��ȡ���м�¼
		u64 blogId = 0;
		SubRecordBuilder keyBuilder(m_tableDef, KEY_PAD);
		key = keyBuilder.createSubRecordByName("BlogID", &blogId);
		key->m_rowId = INVALID_ROW_ID;
		IndexScanCond cond(0, key, true, true, false);
		scanHandle = m_table->indexScan(m_session, intention, &m_opInfo, &cond, subRec->m_numCols, subRec->m_columns);
	}

	u64 spAfterScan = m_session->getMemoryContext()->setSavepoint();

	uint gotRows = 0;
	memset(subRec->m_data, 0, m_tableDef->m_maxRecSize);
	
	try {
		while (m_table->getNext(scanHandle, subRec->m_data)) {

			gotRows++;
			memset(subRec->m_data, 0, m_tableDef->m_maxRecSize);
		}
	} catch (NtseException &e) {
		m_table->endScan(scanHandle);
		freeSubRecord(subRec);
		if (key != NULL) {
			freeSubRecord(key);
		}

		throw e;
	}
	
	// ����ֻ��һ����¼
	CPPUNIT_ASSERT(1 == gotRows);

	m_table->endScan(scanHandle);

	freeSubRecord(subRec);
	if (key != NULL) {
		freeSubRecord(key);
	}
}

TNTTransaction* RowLockConflictThread::beginTransaction(OpType intention, u64 *trxIds, uint activeTrans) {
	TLockMode lockMode;
	if (intention == OP_READ)
		lockMode = TL_NO;
	else 
		lockMode = TL_X;

	m_opInfo.m_selLockType = lockMode;

	// u64 trxArray[10];
	TNTTrxSys *trxSys = m_db->getTransSys();
	TNTTransaction *trx = trxSys->allocTrx();

	trx->startTrxIfNotStarted(m_conn);

	m_opInfo.m_selLockType = lockMode;
	m_opInfo.m_sqlStatStart= true;

	if (intention == OP_READ) {
		// ע������һ�䣬ȫ���ɼ��������ڵ�ǰ������ע�ͣ�ȫ�����ɼ�
		// trx->setTrxId(1);
		trxSys->trxAssignReadView(trx);
	}

	m_session->setTrans(trx);

	return trx;
}

void RowLockConflictThread::commitTransaction() {
	m_trx->commitTrx(CS_NORMAL);
	m_db->getTransSys()->freeTrx(m_trx);
	m_trx = NULL;
	m_session->setTrans(NULL);
}

void RowLockConflictThread::rollbackTransaction() {
	m_trx->rollbackTrx(RBS_NORMAL);
	m_db->getTransSys()->freeTrx(m_trx);
	m_trx = NULL;
	m_session->setTrans(NULL);
}


PurgeTableThread::PurgeTableThread(TNTDatabase *db, TNTTable *table): Thread("PurgeTableThread") {
	m_db = db;
	m_table = table;

	m_opInfo.m_mysqlHasLocked = false;
	m_opInfo.m_mysqlOper = true;
	m_opInfo.m_selLockType = TL_X;
	m_opInfo.m_sqlStatStart = true;
}

void PurgeTableThread::run() {
	m_conn = m_db->getNtseDb()->getConnection(true);
	m_session = m_db->getNtseDb()->getSessionManager()->allocSession("TNTTable_Purge", m_conn);

	m_db->getTransSys()->setMaxTrxId(50005 - 1);
	m_trx = beginTransaction(OP_READ);
	m_trx->trxAssignPurgeReadView();
	m_session->setTrans(m_trx);
	//purge phase 1
	m_table->purgePhase1(m_session, m_trx);
	//purge phase 2
	m_table->purgePhase2(m_session, m_trx);
	commitTransaction();


	m_db->getNtseDb()->getSessionManager()->freeSession(m_session);
	m_db->getNtseDb()->freeConnection(m_conn);
}


TNTTransaction* PurgeTableThread::beginTransaction(OpType intention, u64 *trxIds, uint activeTrans) {
	TLockMode lockMode;
	if (intention == OP_READ)
		lockMode = TL_NO;
	else 
		lockMode = TL_X;

	m_opInfo.m_selLockType = lockMode;

	// u64 trxArray[10];
	TNTTrxSys *trxSys = m_db->getTransSys();
	TNTTransaction *trx = trxSys->allocTrx();

	trx->startTrxIfNotStarted(m_conn);

	m_opInfo.m_selLockType = lockMode;
	m_opInfo.m_sqlStatStart= true;

	if (intention == OP_READ) {
		// ע������һ�䣬ȫ���ɼ��������ڵ�ǰ������ע�ͣ�ȫ�����ɼ�
		// trx->setTrxId(1);
		//trxSys->trxAssignReadView(trx);
	}
	m_session->setTrans(trx);

	return trx;
}

void PurgeTableThread::commitTransaction() {
	m_trx->commitTrx(CS_PURGE);
	m_db->getTransSys()->freeTrx(m_trx);
	m_trx = NULL;
	m_session->setTrans(NULL);
}