/**
 * 测试TNTDatabase操作
 *
 * @author 何登成
 */

#include "api/TestTNTDatabase.h"
#include "api/TNTTable.h"
#include "api/Table.h"
#include "Test.h"

const char* TNTDatabaseTestCase::getName() {
	return "TNTDatabase operation test";
}

const char* TNTDatabaseTestCase::getDescription() {
	return "Test TNTDatabase operations";
}

bool TNTDatabaseTestCase::isBig() {
	return false;
}

void TNTDatabaseTestCase::setUp() {
	File dir("testdb");
	dir.rmdir(true);
	dir.mkdir();
	init();
}

void TNTDatabaseTestCase::tearDown() {
	clear();
	File dir("testdb");
	dir.rmdir(true);
}

/**	创建TNTDatabase
*
*/
void TNTDatabaseTestCase::init() {
	// TNTDatabase::drop(".");
	m_config.m_tntBufSize = 100;
	m_config.setNtseBasedir("testdb");
	m_config.setTntBasedir("testdb");
	m_config.setTntDumpdir("testdb");
	m_config.setTxnLogdir("testdb");
	m_config.m_verpoolCnt = 2;

	EXCPT_OPER(m_db = TNTDatabase::open(&m_config, true, 0));

	Database *db = m_db->getNtseDb();
	m_conn = db->getConnection(false);
	m_conn->setTrx(true);
	m_session = db->getSessionManager()->allocSession("TNTDatabaseTestCase::init", m_conn);
	m_trx = beginTransaction(OP_WRITE);

	m_tableDef	= NULL;
	m_table		= NULL;
}

/**	删除TNTDatabase
*
*/
void TNTDatabaseTestCase::clear() {
	if (m_db != NULL && m_table != NULL && m_trx == NULL)
		m_trx = beginTransaction(OP_WRITE);

	if (m_table != NULL) {
		m_db->closeTable(m_session, m_table);
		m_table = NULL;

		m_db->dropTable(m_session, "./BlogCount", -1);
	}

	if (m_db != NULL) {
		Database *db = m_db->getNtseDb();
		if (m_trx != NULL)
			commitTransaction();
		db->getSessionManager()->freeSession(m_session);
		db->freeConnection(m_conn);
		m_db->close();
		//测试重复close
		m_db->close();
		delete m_db;
		m_db = NULL;
		TNTDatabase::drop("testdb", "testdb");
	}

	if (m_tableDef != NULL) {
		delete m_tableDef;
		m_tableDef = NULL;
	}
}

void TNTDatabaseTestCase::testCommon() {
	//init();

	//clear();
	return;
}

/**	创建测试表
*	
*/
bool TNTDatabaseTestCase::createBlogCount() {
	//init();

	assert(m_db != NULL && m_session != NULL && m_conn != NULL);
	Database *db = m_db->getNtseDb();
	if (m_tableDef != NULL) {
		delete m_tableDef;
		m_tableDef = NULL;
	}
	m_tableDef = getBlogCountDef();

	try {
		m_db->createTable(m_session, "./BlogCount", m_tableDef);
	} catch (NtseException &e) {
		printf("%s", e.getMessage());

		return false;
	}

	return true;
}

/**
 * 生成BlogCount表测试数据，生成的数据的规则为：第n(从0开始)行的各属性按以下规则生成
 * - BlogID: insFirstKey + 1
 * - CommentCount: 10
 * - TrackbackCount: 2
 * - AccessCount: 100
 * - Title: 在A-Z中随机生成，长为25的字符串
 * - UserID: n / 5 + 1
 *
 * @param db 数据库对象
 * @param table BlogCount表
 * @param numRows 要插入的数据
 * @return 各记录数据，为REC_MYSQL格式
 */
Record** TNTDatabaseTestCase::populateBlogCount(TNTDatabase *db, TNTTable *table, uint numRows, uint insFirstKey) {
	Record		**rows = new Record *[numRows];
	uint		insRows = numRows;
	uint		insPrimaryKey = insFirstKey;
	TNTOpInfo	opInfo;
	System::srandom(1);

	opInfo.m_mysqlHasLocked = false;
	opInfo.m_mysqlOper = true;
	opInfo.m_selLockType = TL_X;
	opInfo.m_sqlStatStart = true;

	for (uint n = 0; n < insRows; n++) {
		RecordBuilder rb(m_tableDef, RID(0, 0), REC_REDUNDANT);
		rb.appendBigInt(insPrimaryKey);
		insPrimaryKey++;

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

		CPPUNIT_ASSERT((rec->m_rowId = table->insert(m_session, rec->m_data, &dupIndex, &opInfo)) != INVALID_ROW_ID);

		rows[n] = rec;
		//printf("%d: %I64d %d.\n", n, rec->m_rowId, dupIndex);
	}
	
	return rows;
}


// 更新BlogCount表
uint TNTDatabaseTestCase::updDelBlogCount(TNTDatabase *db, TNTTable *table, OpType opType, Record **rows, uint numRows) {
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
	TNTOpInfo opInfo;
	opInfo.m_sqlStatStart = true;
	opInfo.m_mysqlOper	= true;
	opInfo.m_mysqlHasLocked = false;
	opInfo.m_selLockType = TL_X;
	for (uint i = 0; i < numRows; i++) {
		scan = m_table->positionScan(m_session, opType, &opInfo, m_tableDef->m_numCols, readCols);
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

	return updDelRows;
}




/**
 * 进行表扫描测试，扫描BlogCount表
 *
 * @param intention 操作意向
 */
void TNTDatabaseTestCase::blogCountScanTest(OpType intention, bool tableScan, uint numRows) {
	int		newAccessCount = 50;
	TNTOpInfo	opInfo;

	opInfo.m_mysqlHasLocked = false;
	opInfo.m_mysqlOper = true;
	opInfo.m_selLockType = TL_NO;
	opInfo.m_sqlStatStart = true;

	SubRecordBuilder srb(m_tableDef, REC_REDUNDANT);
	// 要求返回BlogID, CommentCount, Title字段
	SubRecord *subRec = srb.createEmptySbById(m_tableDef->m_maxRecSize, "0 1 4");
	SubRecord *key = NULL;

	TNTTblScan *scanHandle;

	if (tableScan)
		scanHandle = m_table->tableScan(m_session, intention, &opInfo, subRec->m_numCols, subRec->m_columns);
	else {
		// 使用主键索引，指定起始条件为BlogID >= 0，取所有记录
		u64 blogId = 0;
		SubRecordBuilder keyBuilder(m_tableDef, KEY_PAD);
		key = keyBuilder.createSubRecordByName("BlogID", &blogId);
		key->m_rowId = INVALID_ROW_ID;
		IndexScanCond cond(0, key, true, true, false);
		scanHandle = m_table->indexScan(m_session, intention, &opInfo, &cond, subRec->m_numCols, subRec->m_columns);
	}
	
	u64 spAfterScan = m_session->getMemoryContext()->setSavepoint();

	uint gotRows = 0;
	memset(subRec->m_data, 0, m_tableDef->m_maxRecSize);
	while (m_table->getNext(scanHandle, subRec->m_data)) {

		printSubRecord(subRec);

		gotRows++;
		memset(subRec->m_data, 0, m_tableDef->m_maxRecSize);
	}

	CPPUNIT_ASSERT(numRows == gotRows);

	m_table->endScan(scanHandle);

	freeSubRecord(subRec);
	if (key != NULL) {
		freeSubRecord(key);
	}
}


void TNTDatabaseTestCase::blogCountScanAndVerify(Record **rows, uint numRows, OpType opType, bool tableScan, uint searchKey, bool beForward) {
	TNTOpInfo	opInfo;

	opInfo.m_mysqlHasLocked = false;
	opInfo.m_mysqlOper = true;
	opInfo.m_selLockType = TL_NO;
	opInfo.m_sqlStatStart = true;

	SubRecordBuilder srb(m_tableDef, REC_REDUNDANT);
	// 要求返回BlogID, CommentCount, Title字段
	SubRecord *subRec = srb.createEmptySbById(m_tableDef->m_maxRecSize, "0 1 4");
	SubRecord *key = NULL;

	TNTTblScan *scanHandle;

	if (tableScan)
		scanHandle = m_table->tableScan(m_session, opType, &opInfo, subRec->m_numCols, subRec->m_columns);
	else {
		// 使用主键索引，指定起始条件为BlogID，根据BlogID进行索引正向/反向扫描
		u64 blogId = searchKey;
		SubRecordBuilder keyBuilder(m_tableDef, KEY_PAD);
		key = keyBuilder.createSubRecordByName("BlogID", &blogId);
		key->m_rowId = INVALID_ROW_ID;
		IndexScanCond cond(0, key, beForward, true, false);
		scanHandle = m_table->indexScan(m_session, opType, &opInfo, &cond, subRec->m_numCols, subRec->m_columns);
	}

	u64 spAfterScan = m_session->getMemoryContext()->setSavepoint();

	// 根据扫描的正反项，对比的值也不同
	uint gotRows = 0;

	memset(subRec->m_data, 0, m_tableDef->m_maxRecSize);
	while (m_table->getNext(scanHandle, subRec->m_data)) {

		// CPPUNIT_ASSERT(scanHandle->getCurrentRid() == rows[gotRows]->m_rowId);
		// 针对返回的可见记录，找到其在rows数组中的位置
		// printf("%d: %I64d.\n", gotRows, scanHandle->getCurrentRid());

		gotRows++;

		if (rows != NULL) {
			uint rowPos;

			for (uint i = 0; i < numRows; i++) {
				if (scanHandle->getCurrentRid() == rows[i]->m_rowId) {
					rowPos = i;
					break;
				}
			}

			assert(rowPos != numRows);
			// 确认表扫描所得记录数据正确
			Record rec(rows[rowPos]->m_rowId, REC_FIXLEN, rows[rowPos]->m_data, rows[rowPos]->m_size);
			SubRecord *subRec2 = srb.createEmptySbById(m_tableDef->m_maxRecSize, "0 1 4");
			memset(subRec2->m_data, 0, m_tableDef->m_maxRecSize);
			RecordOper::extractSubRecordFR(m_tableDef, &rec, subRec2);
			u16 columns[3] = {0, 1, 4};
			CPPUNIT_ASSERT(!compareRecord(m_table, subRec->m_data, subRec2->m_data, 3, columns));

			freeSubRecord(subRec2);
		}

		memset(subRec->m_data, 0, m_tableDef->m_maxRecSize);
	}

	CPPUNIT_ASSERT(numRows == gotRows);

	m_table->endScan(scanHandle);

	freeSubRecord(subRec);
	if (key != NULL) {
		freeSubRecord(key);
	}
}

void TNTDatabaseTestCase::printSubRecord(SubRecord *subRec) {
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

int TNTDatabaseTestCase::compareRecord(TNTTable *table, const byte *rec1, const byte *rec2, u16 numCols, const u16 *columns) {
	return compareRecord(table->getNtseTable()->getTableDef(), rec1, rec2, numCols, columns);
}

int TNTDatabaseTestCase::compareRecord(const TableDef *tableDef, const byte *rec1, const byte *rec2, u16 numCols, const u16 *columns) {
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



void TNTDatabaseTestCase::openSystemTable() throw (NtseException) {
	assert(m_db != NULL && m_session != NULL && m_conn != NULL);
	Database *db = m_db->getNtseDb();

	try {
		m_table = m_db->openTable(m_session, "./SYS_VersionPool0");
	} catch (NtseException &e) {
		printf("%s", e.getMessage());

		throw e;
	}
}


/**
 * 生成BlogCount表定义
 *
 * @param useMms 是否使用MMS
 * @return BlogCount表定义
 */
TableDef* TNTDatabaseTestCase::getBlogCountDef(bool useMms, bool onlineIdx) {
	TableDefBuilder *builder = new TableDefBuilder(TableDef::INVALID_TABLEID, "space", "BlogCount");

	builder->addColumn("BlogID", CT_BIGINT, false)->addColumn("CommentCount", CT_INT);
	builder->addColumn("TrackbackCount", CT_SMALLINT)->addColumn("AccessCount", CT_INT);
	builder->addColumnS("Title", CT_CHAR, 50);
	builder->addColumn("UserID", CT_BIGINT, false);
	builder->addIndex("PRIMARY", true, true, onlineIdx, "BlogID", 0, NULL);
	builder->addIndex("IDX_BLOGCOUNT_UID_AC", false, false, onlineIdx, "UserID", 0, "AccessCount", 0, NULL);

	TableDef *tableDef = builder->getTableDef();
	tableDef->m_useMms = useMms;
	tableDef->setTableStatus(TS_TRX);

	delete builder;
	return tableDef;
}

void TNTDatabaseTestCase::openBlogCount() throw (NtseException) {
	assert(m_db != NULL && m_session != NULL && m_conn != NULL);
	Database *db = m_db->getNtseDb();
	
	try {
		m_table = m_db->openTable(m_session, "./BlogCount");
	} catch (NtseException &e) {
		printf("%s", e.getMessage());

		throw e;
	}
}


void TNTDatabaseTestCase::pinBlogCount() {
	assert(m_db != NULL && m_session != NULL && m_conn != NULL);
	Database *db = m_db->getNtseDb();

	try {
		m_table = m_db->pinTableIfOpened(m_session, "./BlogCount", m_db->getNtseDb()->getConfig()->m_tlTimeout);
	} catch (NtseException &e) {
		printf("%s", e.getMessage());
		throw e;
	}
}

void TNTDatabaseTestCase::closeBlogCount() {
	assert(m_db != NULL && m_session != NULL && m_conn != NULL);

	if (m_trx == NULL)
		beginTransaction(OP_WRITE);
	
	if (m_table == NULL) {
		try {
			m_table = m_db->openTable(m_session, "./BlogCount");
		} catch (NtseException &e) {
			printf("%s", e.getMessage());
		}
	}

	assert(m_table != NULL);
	m_db->closeTable(m_session, m_table);
	m_table = NULL;
}

bool TNTDatabaseTestCase::dropBlogCount() {
	assert(m_db != NULL && m_session != NULL && m_conn != NULL);

	if (m_table) {
		m_db->closeTable(m_session, m_table);
		m_table = NULL;
	}

	assert(m_table == NULL);

	try {
		m_db->dropTable(m_session, "./BlogCount", -1);
	} catch (NtseException &e) {
		printf("%s. \n", e.getMessage());

		return false;
	}
	
	return true;
}

void TNTDatabaseTestCase::renameBlogCount() {
	assert(m_db != NULL && m_session != NULL && m_conn != NULL);
	bool succ = true;

	if (m_table) {
		m_db->closeTable(m_session, m_table);
		m_table = NULL;
	}
	// rename to
	m_db->renameTable(m_session, "./BlogCount", "./BlogCount1");
	// rename back
	m_db->renameTable(m_session, "./BlogCount1", "./BlogCount");
	// rename for the same name, failed
	try {
		m_db->renameTable(m_session, "./BlogCount", "./BlogCount");
	} catch (NtseException &e) {
		printf("%s. \n", e.getMessage());

		succ = false;
	}

	CPPUNIT_ASSERT(succ == false);

	// open
	openBlogCount();
	assert(m_table != NULL);

	closeBlogCount();
}

void TNTDatabaseTestCase::truncateBlogCount() {
	assert(m_db != NULL && m_session != NULL && m_conn != NULL);

	TNTTransaction *trx = m_db->getTransSys()->allocTrx();

	m_session->setTrans(trx);
	
	if (m_table == NULL)
		openBlogCount();
	if (m_table == NULL) {
		createBlogCount();
		openBlogCount();
		assert(m_table != NULL);
	}

	// Truncate Table操作，不属于DDL，在external_lock中加Meta Lock
	m_table->lockMeta(m_session, IL_X, -1, __FILE__, __LINE__);
	m_db->truncateTable(m_session, m_table, true);

	CPPUNIT_ASSERT(m_table->getMetaLock(m_session) == IL_X);
	m_table->unlockMeta(m_session, IL_X);

	closeBlogCount();

	trx->commitTrx(CS_INNER);
	m_db->getTransSys()->freeTrx(trx);	

	trx = NULL;
}

/**	创建测试表
*	
*/
bool TNTDatabaseTestCase::createTestTable(char *tableName, vector<TableDef*> *vector) {
	//init();

	assert(m_db != NULL && m_session != NULL && m_conn != NULL);
	Database *db = m_db->getNtseDb();
	
	TableDef *tableDef = getBlogCountDef();
	vector->push_back(tableDef);

	try {
		m_db->createTable(m_session, tableName, tableDef);
	} catch (NtseException &e) {
		printf("%s", e.getMessage());

		return false;
	}
	
	return true;
}
/**
 * 生成BlogCount表定义
 *
 * @param useMms 是否使用MMS
 * @return BlogCount表定义
 */
TableDef* TNTDatabaseTestCase::getTestTableDef(char *tableName) {
	TableDefBuilder *builder = new TableDefBuilder(TableDef::INVALID_TABLEID, "space", tableName);

	builder->addColumn("BlogID", CT_BIGINT, false)->addColumn("CommentCount", CT_INT);
	builder->addColumn("TrackbackCount", CT_SMALLINT)->addColumn("AccessCount", CT_INT);
	builder->addColumnS("Title", CT_CHAR, 50);
	builder->addColumn("UserID", CT_BIGINT, false);
	builder->addIndex("PRIMARY", true, true, false, "BlogID", 0, NULL);
	builder->addIndex("IDX_BLOGCOUNT_UID_AC", false, false, false, "UserID", 0, "AccessCount", 0, NULL);

	TableDef *tableDef = builder->getTableDef();
	tableDef->m_useMms = true;
	tableDef->setTableStatus(TS_TRX);

	delete builder;
	return tableDef;
}

void TNTDatabaseTestCase::openTestTable(char *tableName, vector<TNTTable*> *vector) throw (NtseException) {
	assert(m_db != NULL && m_session != NULL && m_conn != NULL);
	Database *db = m_db->getNtseDb();
	TNTTable* table = NULL;
	try {
		table = m_db->openTable(m_session, tableName);
	} catch (NtseException &e) {
		printf("%s", e.getMessage());

		throw e;
	}
	vector->push_back(table);
}

void TNTDatabaseTestCase::closeTestTable(TNTTable *table) {
	assert(m_db != NULL && m_session != NULL && m_conn != NULL);
	if (m_trx == NULL)
		beginTransaction(OP_WRITE);
	assert(table != NULL);
	m_db->closeTable(m_session,table);
	table = NULL;
}


void TNTDatabaseTestCase::testSerialDDL()
{
	// create
	createBlogCount();

	// recreate the same table, return error
	bool succ = createBlogCount();
	CPPUNIT_ASSERT(succ == false);

	// open
	openBlogCount();

	if (m_table != NULL) {
		// close
		closeBlogCount();
		// drop
		dropBlogCount();
	}
	// create
	createBlogCount();
	// open
	openBlogCount();
	assert(m_table != NULL);
	// truncate
	truncateBlogCount();
	// close
	closeBlogCount();
	// rename
	renameBlogCount();

	// drop
	dropBlogCount();

	// drop twice, failed
	succ = dropBlogCount();
	CPPUNIT_ASSERT(succ == false);

	// recreate
	createBlogCount();
}

#define DDL_OP_DROP			1
#define DDL_OP_TRUNCATE		2
#define DDL_OP_RENAME		3

void TNTDatabaseTestCase::ddlOpenFailed(uint ddlOpType) {
#ifdef WIN32
	Record **rows = NULL;
	uint numRows;

	bool succ = true;
	const char *path = "./BlogCount";
	// create
	createBlogCount();
	// open
	openBlogCount();
	assert(m_table != NULL);

	numRows = 10;

	EXCPT_OPER(rows = populateBlogCount(m_db, m_table, numRows, 1));

	for (uint i = 0; i < numRows; i++)
		freeRecord(rows[i]);

	delete [] rows;

	commitTransaction();

	m_trx = beginTransaction(OP_WRITE);

	// close
	closeBlogCount();

	// 此处，通过File接口，直接读取NTSE表文件
	File *heapFile = new File("testdb/./BlogCount.nsd");
	assert(heapFile);
	u64 errCode = heapFile->open(true);
	if (File::E_NO_ERROR != File::getNtseError(errCode)) {
		delete heapFile;

		CPPUNIT_ASSERT(false);
	}

	try {
		switch (ddlOpType)
		{
		case DDL_OP_DROP:
			m_db->dropTable(m_session, path, -1);
			break;
		case DDL_OP_RENAME:
			m_db->renameTable(m_session, path, "./BlogCount1", -1);
			break;
		case DDL_OP_TRUNCATE:
			//openBlogCount();
			//m_db->truncateTable(m_session, m_table);
			break;
		default:
			break;
		}
	} catch (NtseException &e) {
		printf("%s. \n", e.getMessage());

		//closeBlogCount();

		succ = false;
	}

	CPPUNIT_ASSERT(succ == false);

	succ = true;

	// 关闭打开的NTSE表文件
	u64 err = heapFile->close();
	CPPUNIT_ASSERT(err == 0);
	delete heapFile;
	heapFile = NULL;

	// 再次尝试DDL
	try {
		switch (ddlOpType)
		{
		case DDL_OP_DROP:
			m_db->dropTable(m_session, path, -1);
			break;
		case DDL_OP_RENAME:
			m_db->renameTable(m_session, path, "./BlogCount1", -1);
			break;
		case DDL_OP_TRUNCATE:
			//openBlogCount();
			//m_db->truncateTable(m_session, m_table);
			break;
		default:
			break;
		}
	} catch (NtseException &e) {
		printf("%s. \n", e.getMessage());

		//if (ddlOpType == DDL_OP_TRUNCATE) {
		//	closeBlogCount();
		//}

		succ = false;
	}

	// 由于ControlFile中的信息已经被删除，因此本次Drop仍旧失败
	CPPUNIT_ASSERT(succ == false);

	// Rename操作，由于索引文件已经完成Rename，因此表仍旧无法打开
	try {
		succ = true;
		openBlogCount();
	} catch (NtseException &e) {
		printf("%s. \n", e.getMessage());
		
		succ = false;
	}

	CPPUNIT_ASSERT(succ == false);
#else
	// Linux下，通过file打开文件，并不会导致drop与rename失败
	// Linux统计文件的引用计数，删除时并不真正删除，只有当引用计数为0时才真正删除
#endif
}

/* 测试删除表失败的情况 */
void TNTDatabaseTestCase::testDropTableFailed() {
	ddlOpenFailed(DDL_OP_DROP);
}

void TNTDatabaseTestCase::testRenameTableFailed() {
	ddlOpenFailed(DDL_OP_RENAME);
}

void TNTDatabaseTestCase::testClose() {
	m_config.m_purgeBeforeClose = true;
	m_config.m_dumpBeforeClose = false;
	// create
	createBlogCount();
	openBlogCount();
	closeBlogCount();
}

void TNTDatabaseTestCase::testCleanClose() {
	m_config.m_purgeBeforeClose = true;
	m_config.m_dumpBeforeClose = true;
	// create
	createBlogCount();
	openBlogCount();
	closeBlogCount();
}

/**
* 测试open表之后，不close，直接关闭数据库
*/
void TNTDatabaseTestCase::testOpenWithoutClose() {
	// create
	createBlogCount();

	// open
	openBlogCount();

	// close db
	if (m_db != NULL) {
		Database *db = m_db->getNtseDb();
		if (m_trx != NULL)
			commitTransaction();
		db->getSessionManager()->freeSession(m_session);
		db->freeConnection(m_conn);
		m_db->close();
		delete m_db;
		m_db = NULL;
		m_table = NULL;
		TNTDatabase::drop("testdb", "testdb");
	}
}


/**
* 测试PinTableIfOpen
*/
void TNTDatabaseTestCase::testPinTableIfOpen() {
	// create
	createBlogCount();

	// 对于没有打开过的表，pintable返回NULL
	pinBlogCount();
	CPPUNIT_ASSERT(m_table == NULL);

	// open
	openBlogCount();

	// close, 表不会被真正关闭，只是refCnt降为0
	closeBlogCount();


	// pin table
	pinBlogCount();
	CPPUNIT_ASSERT(m_table);
	// close
	closeBlogCount();

	// close db
	if (m_db != NULL) {
		Database *db = m_db->getNtseDb();
		if (m_trx != NULL)
			commitTransaction();
		db->getSessionManager()->freeSession(m_session);
		db->freeConnection(m_conn);
		m_db->close();
		delete m_db;
		m_db = NULL;
		m_table = NULL;
		TNTDatabase::drop("testdb", "testdb");
	}
}


void TNTDatabaseTestCase::testOpenSystemTable() {
	try {
		openSystemTable();
	} catch (NtseException) {
		
	}
	CPPUNIT_ASSERT(m_table == NULL);

	// close db
	if (m_db != NULL) {
		Database *db = m_db->getNtseDb();
		if (m_trx != NULL)
			commitTransaction();
		db->getSessionManager()->freeSession(m_session);
		db->freeConnection(m_conn);
		m_db->close();
		delete m_db;
		m_db = NULL;
		m_table = NULL;
		TNTDatabase::drop("testdb", "testdb");
	}
}


/**
 * 测试增加索引功能
 */
void TNTDatabaseTestCase::testAddDropIndex() {
	TableDefBuilder *builder = new TableDefBuilder(TableDef::INVALID_TABLEID, "space", "BlogCount");
	TableDef *tableDef = NULL;

	builder->addColumn("BlogID", CT_BIGINT, false)->addColumn("CommentCount", CT_INT);
	builder->addColumn("TrackbackCount", CT_SMALLINT)->addColumn("AccessCount", CT_INT);
	builder->addColumnS("Title", CT_CHAR, 50);
	builder->addColumn("UserID", CT_BIGINT, false);
	builder->addIndex("PRIMARY", true, true, false, "BlogID", 0, NULL);

	tableDef = builder->getTableDef();
	tableDef->setTableStatus(TS_TRX);
	// create table
	EXCPT_OPER(m_db->createTable(m_session, "./BlogCount", tableDef));
	delete tableDef;
	m_table = m_db->openTable(m_session, "./BlogCount");

	builder->addIndex("IDX_BLOGCOUNT_UID_AC", false, false, false, "UserID", 0, "AccessCount", 0, NULL);
	tableDef = builder->getTableDef();
	tableDef->setTableStatus(TS_TRX);

	// add index
	// 在正常运行过程中，Meta Lock在exteral_lock中加上
	m_table->lockMeta(m_session, IL_S, -1, __FILE__, __LINE__);
	EXCPT_OPER(m_db->addIndex(m_session, m_table, 1, (const IndexDef **)&tableDef->m_indice[1]));
	CPPUNIT_ASSERT(m_table->getMetaLock(m_session) == IL_S);
	m_table->unlockMeta(m_session, m_table->getMetaLock(m_session));

	// drop index
	// 在正常运行过程中，Meta Lock在exteral_lock中加上
	m_table->lockMeta(m_session, IL_S, -1, __FILE__, __LINE__);
	EXCPT_OPER(m_db->dropIndex(m_session, m_table, 1));
	CPPUNIT_ASSERT(m_table->getMetaLock(m_session) == IL_S);
	m_table->unlockMeta(m_session, m_table->getMetaLock(m_session));

	// close table
	closeBlogCount();

	// drop table
	dropBlogCount();

	delete builder;
	delete tableDef;
}

/** 开始一个事务 */
TNTTransaction* TNTDatabaseTestCase::beginTransaction(OpType intention, u64 *trxIds, uint activeTrans) {
	TLockMode lockMode;
	if (intention == OP_READ)
		lockMode = TL_NO;
	else 
		lockMode = TL_X;

	// u64 trxArray[10];

	TNTTrxSys *trxSys = m_db->getTransSys();
	TNTTransaction *trx = trxSys->allocTrx();

	trx->startTrxIfNotStarted(m_conn);

	if (intention == OP_READ) {
		// 注释下面一句，全部可见，类似于当前读；打开注释，全部不可见
		// trx->setTrxId(1);
		trxSys->trxAssignReadView(trx);
	}

	m_session->setTrans(trx);

	m_trx = trx;

	return trx;
}

/** 提交一个事务 */
void TNTDatabaseTestCase::commitTransaction() {
	m_trx->commitTrx(CS_NORMAL);
	TNTTrxSys *trxSys = m_db->getTransSys();
	trxSys->freeTrx(m_trx);
	m_trx = NULL;
	m_session->setTrans(NULL);
}

void TNTDatabaseTestCase::rollbackTransaction() {
	m_trx->rollbackTrx(RBS_NORMAL);
	TNTTrxSys *trxSys = m_db->getTransSys();
	trxSys->freeTrx(m_trx);
	m_trx = NULL;
	m_session->setTrans(NULL);
}

/*
* 测试TNT对于XA事务的支持
*/
void TNTDatabaseTestCase::testXaTrans() {
	Record **rows = NULL;
	uint numRows = 0;

	// create
	createBlogCount();

	// open
	openBlogCount();

	if (m_trx == NULL)
		m_trx = beginTransaction(OP_WRITE);

	numRows = 10;
	EXCPT_OPER(rows = populateBlogCount(m_db, m_table, numRows));

	XID xid;
	memset(&xid, 0, sizeof(XID));
	xid.formatID		= 1;
	xid.gtrid_length	= 20;
	xid.bqual_length	= 0;
	char *ch = "MySQLXid    00000001";
	memcpy(xid.data, ch, xid.gtrid_length);

	m_trx->setXId((const XID &)xid);

	m_trx->prepareForMysql();

	commitTransaction();

	for (uint i = 0; i < numRows; i++)
		freeRecord(rows[i]);

	delete [] rows;
}

/*
* 测试事务的partial rollback功能
*/
void TNTDatabaseTestCase::testPartialRollback() {
	Record **rows;
	uint numRows;

	// create
	createBlogCount();

	// open
	openBlogCount();

	if (m_trx == NULL)
		m_trx = beginTransaction(OP_WRITE);

	// statement 1
	numRows = 10;
	EXCPT_OPER(rows = populateBlogCount(m_db, m_table, numRows, 1));
	m_trx->markSqlStatEnd();

	for (uint i = 0; i < numRows; i++)
		freeRecord(rows[i]);

	delete [] rows;

	// statement 2
	numRows = 20;
	EXCPT_OPER(rows = populateBlogCount(m_db, m_table, numRows, 100));
	
	// partial rollback
	m_trx->rollbackLastStmt(RBS_NORMAL);

	for (uint i = 0; i < numRows; i++)
		freeRecord(rows[i]);

	delete [] rows;

	// statement 3
	numRows = 30;
	EXCPT_OPER(rows = populateBlogCount(m_db, m_table, numRows, 200));

	// partial rollback twice
	m_trx->rollbackLastStmt(RBS_NORMAL);
	m_trx->rollbackLastStmt(RBS_NORMAL);

	// full rollback
	rollbackTransaction();

	for (uint i = 0; i < numRows; i++)
		freeRecord(rows[i]);

	delete [] rows;

	m_trx = beginTransaction(OP_READ);
	blogCountScanTest(OP_READ, true, 0);
	commitTransaction();

	m_trx = beginTransaction(OP_WRITE);

	// statement 4
	numRows = 10;
	EXCPT_OPER(rows = populateBlogCount(m_db, m_table, numRows, 300));
	m_trx->markSqlStatEnd();

	for (uint i = 0; i < numRows; i++)
		freeRecord(rows[i]);

	delete [] rows;

	// statement 5
	numRows = 20;
	EXCPT_OPER(rows = populateBlogCount(m_db, m_table, numRows, 400));

	// partial rollback
	m_trx->rollbackLastStmt(RBS_NORMAL);

	for (uint i = 0; i < numRows; i++)
		freeRecord(rows[i]);

	delete [] rows;

	// statement 6
	numRows = 30;
	EXCPT_OPER(rows = populateBlogCount(m_db, m_table, numRows, 500));

	m_trx->markSqlStatEnd();

	for (uint i = 0; i < numRows; i++)
		freeRecord(rows[i]);

	delete [] rows;

	// statement 7
	numRows = 30;
	EXCPT_OPER(rows = populateBlogCount(m_db, m_table, numRows, 600));

	// partial rollback twice
	m_trx->rollbackLastStmt(RBS_NORMAL);
	m_trx->rollbackLastStmt(RBS_NORMAL);

	for (uint i = 0; i < numRows; i++)
		freeRecord(rows[i]);

	delete [] rows;

	// commit transaction
	// leaving 40 records insert successful
	// [300, 309]
	// [500, 529]
	commitTransaction();

	m_trx = beginTransaction(OP_READ);
	blogCountScanTest(OP_READ, true, 40);
	commitTransaction();
}

void TNTDatabaseTestCase::testPurgeDumpDefrag() {
	//测试在无表情况下的purge dump和defrag
	try {
		m_db->purgeAndDumpTntim(PT_PURGEPHASE2, false, false, true);
	} catch (NtseException &) {
		CPPUNIT_FAIL("defrag purge or dump error");
	}
}

void TNTDatabaseTestCase::testOpen() {
	bool exceptionCatch = false;
	//把setup建立的数据库给删除
	clear();
	File dir("testdb");
	TNTDatabase *db = NULL;

	//测试ntse ctrl与tnt ctrl都不存在
	dir.mkdir();
	try {
		db = TNTDatabase::open(&m_config, false, 0);
		CPPUNIT_FAIL("can't open file which is no existed");
	} catch (NtseException &e) {
		char *prefixBothNoExist = "Both ctrl files";
		exceptionCatch = true;
		CPPUNIT_ASSERT(NTSE_EC_FILE_NOT_EXIST == e.getErrorCode());
		CPPUNIT_ASSERT(memcmp(e.getMessage(), prefixBothNoExist, strlen(prefixBothNoExist)) == 0);
	}
	CPPUNIT_ASSERT(exceptionCatch == true);
	dir.rmdir(true);

	//测试tnt ctrl不存在但ntse ctrl存在
	dir.mkdir();
	exceptionCatch = false;
	Database *ntsedb = Database::open(&m_config.m_ntseConfig, true, -1);
	try {
		db = TNTDatabase::open(&m_config, false, 0);
		CPPUNIT_FAIL("can't open file which tnt ctrl file is no existed");
	} catch (NtseException &e) {
		char *prefixTntNoExist = "Tnt ctrl file";
		exceptionCatch = true;
		CPPUNIT_ASSERT(NTSE_EC_FILE_NOT_EXIST == e.getErrorCode());
		CPPUNIT_ASSERT(memcmp(e.getMessage(), prefixTntNoExist, strlen(prefixTntNoExist)) == 0);
	}
	CPPUNIT_ASSERT(exceptionCatch == true);
	ntsedb->close(false, false);
	delete ntsedb;
	ntsedb = NULL;
	Database::drop(m_config.m_ntseConfig.m_basedir, m_config.m_ntseConfig.m_logdir);
	dir.rmdir(true);

	//测试ntse ctrl不存在但tnt ctrl存在
	dir.mkdir();
	exceptionCatch = false;
	string basedir(m_config.m_tntBasedir);
	string tntPath = basedir + NTSE_PATH_SEP + Limits::NAME_TNT_CTRL_FILE;
	File file(tntPath.c_str());
	file.create(false, false);
	file.close();
	try {
		db = TNTDatabase::open(&m_config, false, 0);
		CPPUNIT_FAIL("can't open file which ntse ctrl file is no existed");
	} catch (NtseException &e) {
		char *prefixNtseNoExist = "Ntse ctrl file";
		exceptionCatch = true;
		CPPUNIT_ASSERT(NTSE_EC_FILE_NOT_EXIST == e.getErrorCode());
		CPPUNIT_ASSERT(memcmp(e.getMessage(), prefixNtseNoExist, strlen(prefixNtseNoExist)) == 0);
	}
	CPPUNIT_ASSERT(exceptionCatch == true);
	file.remove();
	dir.rmdir(true);

	//测试日志文件被删除
	dir.mkdir();
	exceptionCatch = false;
	try {
		db = TNTDatabase::open(&m_config, true, 0);
	} catch (NtseException &) {
		exceptionCatch = true;
	}
	CPPUNIT_ASSERT(!exceptionCatch);
	db->close();
	delete db;
	db = NULL;
	//删除日志文件后，还能正常启动
	Txnlog::drop(m_config.m_ntseConfig.m_logdir, Limits::NAME_TXNLOG, m_config.m_ntseConfig.m_logFileCntHwm);
	try {
		db = TNTDatabase::open(&m_config, false, 0);
	} catch (NtseException &) {
		exceptionCatch = true;
	}
	CPPUNIT_ASSERT(!exceptionCatch);
	db->close();
	delete db;
	db = NULL;
	dir.rmdir(true);

	//测试TNT日志文件长度发生变更
	dir.mkdir();
	exceptionCatch = false;
	try {
		db = TNTDatabase::open(&m_config, true, 0);
	} catch (NtseException &) {
		exceptionCatch = true;
	}
	CPPUNIT_ASSERT(!exceptionCatch);
	db->close();
	delete db;
	db = NULL;
	//删除日志文件后，还能正常启动
	Txnlog::drop(m_config.m_ntseConfig.m_logdir, Limits::NAME_TXNLOG, m_config.m_ntseConfig.m_logFileCntHwm);
	Txnlog::create(m_config.m_ntseConfig.m_logdir, Limits::NAME_TXNLOG, m_config.m_ntseConfig.m_logFileSize/2, m_config.m_ntseConfig.m_logFileCntHwm);
	try {
		db = TNTDatabase::open(&m_config, false, 0);
		CPPUNIT_FAIL("txn log file size id modify, so it must throw exception");
	} catch (NtseException &e) {
		CPPUNIT_ASSERT(e.getErrorCode() == NTSE_EC_CORRUPTED_LOGFILE);
		exceptionCatch = true;
	}
	CPPUNIT_ASSERT(exceptionCatch);
	dir.rmdir(true);

	//测试NTSE日志文件长度发生变更
	dir.mkdir();
	exceptionCatch = false;
	try {
		db = TNTDatabase::open(&m_config, true, 0);
	} catch (NtseException &) {
		exceptionCatch = true;
	}
	CPPUNIT_ASSERT(!exceptionCatch);
	db->close();
	delete db;
	db = NULL;
	//删除日志文件后，还能正常启动
	Txnlog::drop(m_config.m_ntseConfig.m_logdir, Limits::NAME_TXNLOG, m_config.m_ntseConfig.m_logFileCntHwm);
	Txnlog::create(m_config.m_ntseConfig.m_logdir, Limits::NAME_TXNLOG, m_config.m_ntseConfig.m_logFileSize/2, m_config.m_ntseConfig.m_logFileCntHwm);
	try {
		db = TNTDatabase::open(&m_config, false, 0);
		CPPUNIT_FAIL("ntse log file size id modify, so it must throw exception");
	} catch (NtseException &e) {
		CPPUNIT_ASSERT(e.getErrorCode() == NTSE_EC_CORRUPTED_LOGFILE);
		exceptionCatch = true;
	}
	CPPUNIT_ASSERT(exceptionCatch);
	dir.rmdir(true);

	//测试第二次打开数据库时，丢失部分系统表(现为丢失版本池表)
	dir.mkdir();
	exceptionCatch = false;
	try {
		db = TNTDatabase::open(&m_config, true, 0);
	} catch (NtseException &) {
		exceptionCatch = true;
	}
	CPPUNIT_ASSERT(!exceptionCatch);
	db->close();
	delete db;
	db = NULL;
	//删去其中一个版本池文件
	char *path = (char *)alloca(strlen(m_config.m_tntBasedir) + strlen(Limits::NAME_HEAP_EXT) + 20);
	sprintf(path, "%s/%s%d%s", m_config.m_tntBasedir, VersionPool::VTABLE_NAME, 1, Limits::NAME_HEAP_EXT);
	File versionFile(path);
	CPPUNIT_ASSERT(File::isExist(path));
	versionFile.remove();
	try {
		db = TNTDatabase::open(&m_config, false, 0);
		CPPUNIT_FAIL("can't open table which remove a version file");
	} catch (NtseException &e) {
		CPPUNIT_ASSERT(e.getErrorCode() == NTSE_EC_FILE_NOT_EXIST);
		exceptionCatch = true;
	}
	CPPUNIT_ASSERT(exceptionCatch);
	dir.rmdir(true);

}

/*
* 测试TNT引擎VersionPool切换的功能
* 测试方案：
* 1. 新建两个事务T1，T2，并做DML操作
* 2. 切换版本池
* 3. 新建两个事务T3，T4，并做DML操作
* 4. T2，T3回滚，T1，T4提交
* 5. 验证最后表中记录的正确性
*/
void TNTDatabaseTestCase::testSwitchVersionPool() {

	Record **rows1;
	Record **rows2;
	Record **rows3;
	Record **rows4;

	uint numRows1, numRows2, numRows3, numRows4;

	// create
	createBlogCount();

	// open
	openBlogCount();

	// T1
	if (m_trx == NULL)
		m_trx = beginTransaction(OP_WRITE);

	// statement 1
	numRows1 = 10;
	EXCPT_OPER(rows1 = populateBlogCount(m_db, m_table, numRows1, 1));

	// commit T1
	commitTransaction();

	// T2
	m_trx = beginTransaction(OP_WRITE);

	// statement 2
	numRows2 = 20;
	EXCPT_OPER(rows2 = populateBlogCount(m_db, m_table, numRows2, 100));

	for (uint i = 0; i < numRows2; i++)
		freeRecord(rows2[i]);

	delete [] rows2;

	// switch version pool, not needed
	m_config.m_verpoolFileSize = 81920;
	m_db->switchActiveVerPoolIfNeeded();
	
	// switch version pool, needed
	m_config.m_verpoolFileSize = 8192;
	m_db->switchActiveVerPoolIfNeeded();

	// rollback T2
	rollbackTransaction();

	// T3
	m_trx = beginTransaction(OP_WRITE);

	// statement 3
	numRows3 = 30;
	EXCPT_OPER(rows3 = populateBlogCount(m_db, m_table, numRows3, 200));

	// rollback T3
	rollbackTransaction();

	for (uint i = 0; i < numRows3; i++)
		freeRecord(rows3[i]);

	delete [] rows3;

	// T4
	m_trx = beginTransaction(OP_WRITE);

	numRows4 = 40;
	EXCPT_OPER(rows4 = populateBlogCount(m_db, m_table, numRows4, 400));

	// commit T4
	commitTransaction();

	// 验证表中最后数据的正确性

	// 1. 验证T1事务产生的数据
	m_trx = beginTransaction(OP_WRITE);

	blogCountScanAndVerify(rows1, numRows1, OP_READ, false, 20, false);

	commitTransaction();

	// 2. 验证T4事务产生的数据
	m_trx = beginTransaction(OP_WRITE);

	blogCountScanAndVerify(rows4, numRows4, OP_READ, false, 40, true);

	commitTransaction();

	// 释放T1，T4事务的数据
	for (uint i = 0; i < numRows1; i++)
		freeRecord(rows1[i]);

	delete [] rows1;

	for (uint i = 0; i < numRows4; i++)
		freeRecord(rows4[i]);

	delete [] rows4;

	// 
}

void TNTDatabaseTestCase::testMultiThreadWithDropDDL() {
	// create & open table
	createBlogCount();
	openBlogCount();

	DDLThread ddlThread(m_db, DROP_OP);

	ddlThread.start();

	// 主线程 sleep 一段时间，保证ddl线程第一次dropTable失败，第二次成功
	Thread::msleep(20000);

	closeBlogCount();

	// 等待 ddlThread 完成退出
	ddlThread.join(-1);

	// 重新打开被 ddlThread drop的表
	try {
		openBlogCount();
	} catch (NtseException &) {
		printf("Table Not Existed. \n");
	}
	
	CPPUNIT_ASSERT(m_table == NULL);
}

void TNTDatabaseTestCase::testMultiThreadWithRenameDDL() {
	// create & open table
	createBlogCount();
	openBlogCount();

	DDLThread ddlThread(m_db, RENAME_OP);

	ddlThread.start();

	// 主线程 sleep 一段时间，保证ddl线程第一次dropTable失败，第二次成功
	Thread::msleep(20000);

	closeBlogCount();

	// 等待 ddlThread 完成退出
	ddlThread.join(-1);

	// 重新打开被 ddlThread drop的表
	try {
		openBlogCount();
	} catch (NtseException &) {
		printf("Table Not Existed. \n");
	}

	CPPUNIT_ASSERT(m_table == NULL);
}

/* 测试Truncate过程中有并发的OpenTable操作的正确性 */
void TNTDatabaseTestCase::testTruncateCurrentOpenTable() {
	Record	**rows;
	uint	numRows;

	createBlogCount();
	// 首先open表，插入记录
	openBlogCount();

	if (m_trx != NULL) {
		commitTransaction();
	}
	
	beginTransaction(OP_WRITE);

	numRows = 100;
	EXCPT_OPER(rows = populateBlogCount(m_db, m_table, numRows, 100));

	for (uint i = 0; i < numRows; i++)
		freeRecord(rows[i]);

	delete [] rows;

	closeBlogCount();

	commitTransaction();

	TruncateThread truncateThread(m_db);

	truncateThread.enableSyncPoint(SP_DB_TRUNCATE_CURRENT_OPEN);

	truncateThread.start();

	// 等待TruncateThread运行到NTSE Truncate结束，TNT Truncate开始前暂停
	truncateThread.joinSyncPoint(SP_DB_TRUNCATE_CURRENT_OPEN);
	
	// 验证Open操作一定成功
	try {
		beginTransaction(OP_WRITE);
		// 唤醒TruncateThread，完成操作
		truncateThread.disableSyncPoint(SP_DB_TRUNCATE_CURRENT_OPEN);
		truncateThread.notifySyncPoint(SP_DB_TRUNCATE_CURRENT_OPEN);

		openBlogCount();
		commitTransaction();
	} catch (NtseException &e) {
		printf("%s. \n", e.getMessage());

		CPPUNIT_ASSERT(false);
	}
	
	// 等待Truncate操作结束
	truncateThread.join(-1);

	// 主线程进行DML操作，验证正确性
	m_table->lockMeta(m_session, IL_S, -1, __FILE__, __LINE__);

	beginTransaction(OP_WRITE);

	numRows = 10;
	EXCPT_OPER(rows = populateBlogCount(m_db, m_table, numRows, 1));

	commitTransaction();

	m_table->unlockMeta(m_session, IL_S);

	// 表扫描
	m_table->lockMeta(m_session, IL_S, -1, __FILE__, __LINE__);

	beginTransaction(OP_READ);

	blogCountScanAndVerify(rows, numRows, OP_READ, true, 1, true);

	// 索引扫描
	blogCountScanAndVerify(rows, numRows, OP_READ, false, 10, false);

	commitTransaction();
	m_table->unlockMeta(m_session, IL_S);

	for (uint i = 0; i < numRows; i++)
		freeRecord(rows[i]);

	delete [] rows;

	closeBlogCount();
}


void TNTDatabaseTestCase::testTableOpenCache() {
	// 设置open table cache 大小
	int reserveTabeCnt = 3;
	m_db->getTNTConfig()->m_openTableCnt = reserveTabeCnt + m_db->getTNTConfig()->m_verpoolCnt * 2;

	vector<TNTTable*> tableList;
	vector<TableDef*> tblDefList;

	// 创建5张测试表
	createTestTable("t1", &tblDefList);
	createTestTable("t2", &tblDefList);
	createTestTable("t3", &tblDefList);
	createTestTable("t4", &tblDefList);
	createTestTable("t5", &tblDefList);

	// 打开表
	openTestTable("t1", &tableList);
	openTestTable("t2", &tableList);
	openTestTable("t3", &tableList);
	openTestTable("t4", &tableList);
	openTestTable("t5", &tableList);

	// 关闭表
	closeTestTable(tableList[0]);
	closeTestTable(tableList[1]);
	closeTestTable(tableList[2]);
	closeTestTable(tableList[3]);
	closeTestTable(tableList[4]);

	// 确认所有的表都没有被real close
	for (int i = 0; i < tableList.size(); i++) {
		CPPUNIT_ASSERT(m_db->getIdToTablesHash()->get(tableList[i]->getNtseTable()->getTableDef()->m_id) != NULL);
	}

	// 将超过open table cache size 数量的表关闭
	m_db->closeOpenTablesIfNeed();
	int a = m_db->getIdToTablesHash()->getSize();
	CPPUNIT_ASSERT(m_db->getIdToTablesHash()->getSize() == reserveTabeCnt);


	// drop 表
	m_db->dropTable(m_session, "t1", -1);
	m_db->dropTable(m_session, "t2", -1);
	m_db->dropTable(m_session, "t3", -1);
	m_db->dropTable(m_session, "t4", -1);
	m_db->dropTable(m_session, "t5", -1);
	
	// 删除表定义
	for(int i = 0; i < tblDefList.size(); i++) {
		delete tblDefList[i];
	}
}


void TNTDatabaseTestCase::testReclaimMemIndex() {
	Record **rows1;
	
	uint numRows1;

	// create
	createBlogCount();

	// open
	openBlogCount();

	// T1
	if (m_trx == NULL)
		m_trx = beginTransaction(OP_WRITE);

	// statement 1
	numRows1 = 10000;
	EXCPT_OPER(rows1 = populateBlogCount(m_db, m_table, numRows1, 1));

	// commit T1
	commitTransaction();

	// T2
	if (m_trx == NULL)
		m_trx = beginTransaction(OP_WRITE);

	// statement 2
	uint rowsUpdated = 0;
	EXCPT_OPER(rowsUpdated = updDelBlogCount(m_db, m_table, OP_UPDATE, rows1, numRows1));
	
	// commit T2
	commitTransaction();

	// purge
	m_db->purgeAndDumpTntim(PT_PURGEPHASE2, false, false, true);


	// reclaimMemIndex
	m_config.m_reclaimMemIndexHwm = 0;
	m_config.m_reclaimMemIndexLwm = 0;
	m_db->reclaimMemIndex();

	for (uint i = 0; i < m_table->getIndice()->getIndexNum(); i++) {
		CPPUNIT_ASSERT(m_table->getIndice()->getMemIndice()->getIndex(0)->getStatus().m_numAllocPage.get() - 
			m_table->getIndice()->getMemIndice()->getIndex(i)->getStatus().m_numFreePage.get());
	}

	closeBlogCount();

	for (uint i = 0; i < numRows1; i++)
		freeRecord(rows1[i]);

	delete [] rows1;
}

void TNTDatabaseTestCase::testReportStatus() {
	TNTGlobalStatus status;
	createBlogCount();
	openBlogCount();
	m_db->reportStatus(&status);

	CPPUNIT_ASSERT(!strcmp("NULL", status.m_purgeMaxBeginTime));
	CPPUNIT_ASSERT(!strcmp("NULL", status.m_switchVerPoolLastTime));
	CPPUNIT_ASSERT(!strcmp("NULL", status.m_reclaimVerPoolLastBeginTime));
	CPPUNIT_ASSERT(!strcmp("NULL", status.m_reclaimVerPoolLastEndTime));


	// purge
	m_db->purgeAndDumpTntim(PT_PURGEPHASE2, false, false, true);

	//
	m_config.m_verpoolFileSize = 1;
	Record **rows1;

	uint numRows1;

	if (m_trx == NULL)
		m_trx = beginTransaction(OP_WRITE);

	numRows1 = 10000;
	EXCPT_OPER(rows1 = populateBlogCount(m_db, m_table, numRows1, 1));

	commitTransaction();

	if (m_trx == NULL)
		m_trx = beginTransaction(OP_WRITE);

	uint rowsUpdated = 0;
	EXCPT_OPER(rowsUpdated = updDelBlogCount(m_db, m_table, OP_UPDATE, rows1, numRows1));

	commitTransaction();

	for (uint i = 0; i < numRows1; i++)
		freeRecord(rows1[i]);

	delete [] rows1;

	// switch verpool
	m_db->switchActiveVerPoolIfNeeded();

	// reclaim verpool
	m_db->reclaimLob();

	m_db->reportStatus(&status);

	CPPUNIT_ASSERT(strcmp("NULL", status.m_purgeMaxBeginTime));
	CPPUNIT_ASSERT(strcmp("NULL", status.m_switchVerPoolLastTime));
	CPPUNIT_ASSERT(strcmp("NULL", status.m_reclaimVerPoolLastBeginTime));
	CPPUNIT_ASSERT(strcmp("NULL", status.m_reclaimVerPoolLastEndTime));

	closeBlogCount();
}


DDLThread::DDLThread(TNTDatabase *db, uint ddlOp) : Thread("DDLThread") {
	m_db	= db;
	m_ddlOp	= ddlOp;
}

void DDLThread::run() {
	m_conn = m_db->getNtseDb()->getConnection(false);
	m_conn->setTrx(true);
	m_session = m_db->getNtseDb()->getSessionManager()->allocSession("TNTDatabase_DDL_Thread", m_conn);

	// 测试dropTable，等待超时
	beginTransaction();

	try {
		if (m_ddlOp == RENAME_OP) {
			m_db->renameTable(m_session, "./BlogCount", "./BlogCount1", 5);
		}
		else {
			CPPUNIT_ASSERT(m_ddlOp == DROP_OP);

			m_db->dropTable(m_session, "./BlogCount", 5);
		}
	} catch (NtseException &e) {
		printf("%s \n", e.getMessage());
		
		rollbackTransaction();
	}

	CPPUNIT_ASSERT(m_trx == NULL);

	// 测试dropTable，成功
	beginTransaction();

	try {
		if (m_ddlOp == RENAME_OP) {
			m_db->renameTable(m_session, "./BlogCount", "./BlogCount1", -1);
		}
		else {
			CPPUNIT_ASSERT(m_ddlOp == DROP_OP);

			m_db->dropTable(m_session, "./BlogCount", -1);
		}
	} catch (NtseException &) {
		CPPUNIT_ASSERT(false);
	}

	if (m_ddlOp == RENAME_OP) {
		try {
			m_db->dropTable(m_session, "./BlogCount1", -1);
		} catch (NtseException &) {
			CPPUNIT_ASSERT(false);
		}	
	}

	CPPUNIT_ASSERT(m_trx != NULL);

	commitTransaction();

	m_db->getNtseDb()->getSessionManager()->freeSession(m_session);
	m_db->getNtseDb()->freeConnection(m_conn);
}

void DDLThread::beginTransaction() {
	m_trx = m_db->getTransSys()->allocTrx();
	m_trx->startTrxIfNotStarted(m_conn);

	m_session->setTrans(m_trx);
}

void DDLThread::commitTransaction() {
	m_trx->commitTrx(CS_NORMAL);
	m_db->getTransSys()->freeTrx(m_trx);
	m_trx = NULL;
	m_session->setTrans(NULL);
}

void DDLThread::rollbackTransaction() {
	m_trx->rollbackTrx(RBS_NORMAL);
	m_db->getTransSys()->freeTrx(m_trx);
	m_trx = NULL;
	m_session->setTrans(NULL);
}

TruncateThread::TruncateThread(TNTDatabase *db) : Thread("TruncateThread") {
	m_db	= db;
}

void TruncateThread::run() {
	m_conn = m_db->getNtseDb()->getConnection(false);
	m_conn->setTrx(true);
	m_session = m_db->getNtseDb()->getSessionManager()->allocSession("TNTDatabase_Truncate_Thread", m_conn);

	beginTransaction();

	// Open BlogCount Table
	openBlogCount();
	
	// 模拟外部truncate操作处理流程，首先加Meta X Lock
	m_table->lockMeta(m_session, IL_X, -1, __FILE__, __LINE__);

	try {
		m_db->truncateTable(m_session, m_table);
	} catch (NtseException &e) {
		printf("%s. \n", e.getMessage());

		// truncate 一定成功
		CPPUNIT_ASSERT(false);
	}

	commitTransaction();

	// truncate完成之后，释放Meta Lock，并关闭BlogCount表
	m_table->unlockMeta(m_session, IL_X);
	closeBlogCount();

	m_db->getNtseDb()->getSessionManager()->freeSession(m_session);
	m_db->getNtseDb()->freeConnection(m_conn);
}

void TruncateThread::openBlogCount() {
	assert(m_db != NULL && m_session != NULL && m_conn != NULL);
	Database *db = m_db->getNtseDb();

	try {
		m_table = m_db->openTable(m_session, "./BlogCount");
	} catch (NtseException &e) {
		printf("%s", e.getMessage());
	}
}

void TruncateThread::closeBlogCount() {
	assert(m_db != NULL && m_session != NULL && m_conn != NULL);

	if (m_trx == NULL)
		beginTransaction();

	if (m_table == NULL) {
		try {
			m_table = m_db->openTable(m_session, "./BlogCount");
		} catch (NtseException &e) {
			printf("%s", e.getMessage());

			CPPUNIT_ASSERT(false);
		}
	}

	assert(m_table != NULL);
	m_db->closeTable(m_session, m_table);
	m_table = NULL;

	if (m_trx)
		commitTransaction();
}

void TruncateThread::beginTransaction() {
	m_trx = m_db->getTransSys()->allocTrx();
	m_trx->startTrxIfNotStarted(m_conn);

	m_session->setTrans(m_trx);
}

void TruncateThread::commitTransaction() {
	m_trx->commitTrx(CS_NORMAL);
	m_db->getTransSys()->freeTrx(m_trx);
	m_trx = NULL;
	m_session->setTrans(NULL);
}

void TruncateThread::rollbackTransaction() {
	m_trx->rollbackTrx(RBS_NORMAL);
	m_db->getTransSys()->freeTrx(m_trx);
	m_trx = NULL;
	m_session->setTrans(NULL);
}