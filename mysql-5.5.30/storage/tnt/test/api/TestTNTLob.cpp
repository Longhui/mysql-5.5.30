/**
 * 测试TNTLobTable的 DML操作以及事务回滚及恢复
 *
 * @author 胡炜
 */

#include "api/TestTNTLob.h"
#include "api/Table.h"
#include "Test.h"


const uint TNTLobTestCase::SMALL_LOB_SIZE = Limits::PAGE_SIZE / 4;
const uint TNTLobTestCase::LARGE_LOB_SIZE = Limits::PAGE_SIZE * 16;

const char* TNTLobTestCase::getName() {
	return "TNTLob operation test";
}

const char* TNTLobTestCase::getDescription() {
	return "Test TNTLob operations";
}

bool TNTLobTestCase::isBig() {
	return false;
}

void TNTLobTestCase::setUp() {
	init();
}

void TNTLobTestCase::tearDown() {
	clear();
}

TNTTransaction* TNTLobTestCase::beginTransaction(OpType intention, u64 *trxIds, uint activeTrans) {
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
		// 注释下面一句，全部可见，类似于当前读；打开注释，全部不可见
		// trx->setTrxId(1);
		//trxSys->trxAssignReadView(trx);
	}
	m_session->setTrans(trx);

	return trx;
}

void TNTLobTestCase::commitTransaction(TNTTransaction *trx) {
	trx->commitTrx(CS_NORMAL);
	m_db->getTransSys()->freeTrx(trx);
	trx = NULL;
	m_session->setTrans(NULL);
}

void TNTLobTestCase::rollbackTransaction(TNTTransaction *trx) {
	trx->rollbackTrx(RBS_NORMAL);
	m_db->getTransSys()->freeTrx(trx);
	trx = NULL;
	m_session->setTrans(NULL);
}

/**	创建/打开 TNTDatabase
*
*/
void TNTLobTestCase::init() {
	File dir("testdb");
	dir.rmdir(true);
	dir.mkdir();
	m_config.m_tntBufSize = 100;
	m_config.setNtseBasedir("testdb");
	m_config.setTntBasedir("testdb");
	m_config.setTntDumpdir("testdb");
	m_config.m_verpoolCnt = 2;

	EXCPT_OPER(m_db = TNTDatabase::open(&m_config, true, 0));

	Database *db = m_db->getNtseDb();
	m_conn = db->getConnection(false);
	m_conn->setTrx(true);
	m_session = db->getSessionManager()->allocSession("TNTLobTestCase::init", m_conn);

	assert(m_db != NULL && m_session != NULL && m_conn != NULL);

	//table = m_db->openTable(session, "BlogCount");
	//if (table == NULL) {
	TNTTransaction *trx = beginTransaction(OP_WRITE);
	m_tableDef = getBlogDef();

	try {
		m_db->createTable(m_session, "Blog", m_tableDef);
	} catch (NtseException &e) {
		printf("%s", e.getMessage());
	}

	m_table = m_db->openTable(m_session, "Blog");

	assert(m_table != NULL);
	m_db->getTransSys()->setMaxTrxId(100);

	commitTransaction(trx);

	// 初始化TNTOpInfo
	m_opInfo.m_sqlStatStart = true;
	m_opInfo.m_mysqlOper	= true;
	m_opInfo.m_mysqlHasLocked = false;
	m_opInfo.m_selLockType	= TL_NO;
}

/**	删除/关闭 TNTDatabase
*
*/
void TNTLobTestCase::clear() {
	assert(m_db != NULL && m_session != NULL && m_conn != NULL);

	TNTTransaction *trx = beginTransaction(OP_WRITE);

	if (m_table != NULL) {
		m_db->closeTable(m_session, m_table);
		m_table = NULL;

		m_db->dropTable(m_session, "Blog", -1);
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


void TNTLobTestCase::initTNTOpInfo(TLockMode lockMode) {

	m_opInfo.m_selLockType = lockMode;
	m_opInfo.m_sqlStatStart= true;
}

/**
 * 生成Blog表测试数据，生成的数据的规则为：第n(从0开始)行的各属性按以下规则生成
 * - ID: n + 1
 * - UserID: n / 5 + 1
 * - PublishTime: 当前时间毫秒数
 * - Title: 长度为100的随机字符串
 * - Tags: 长度为100的随机字符串
 * - Abstract: 长度为100的随机字符串，若lobNotNull为false则奇数行为NULL
 * - Content: 奇数行为大型大对象，偶数行为小型大对象，小型大对象大小为SMALL_LOB_SIZE，
 *   大型大对象大小为LARGE_LOB_SIZE，若lobNotNull为false则偶数行为NULL
 *
 * @param db 数据库对象
 * @param table Blog表
 * @param numRows 要插入的数据
 * @param rows 插入记录的数组
 * @param lobNotNull 是否插入NULL大对象
 * @param contentArray out/插入的Content数组
 * @param idstart  主键id的起始值
 * @return 各记录数据，为REC_MYSQL格式
 */
void TNTLobTestCase::populateBlog(TNTDatabase *db, TNTTable *table, uint numRows, Record **rows, bool lobNotNull, byte **contentArray, u64 idstart) {
	SubRecordBuilder srb(table->getNtseTable()->getTableDef(), REC_REDUNDANT);
	for(uint n = 0; n < numRows; n++){
		RecordBuilder rb(table->getNtseTable()->getTableDef(), 0, REC_UPPMYSQL);
		u64 id = n + idstart;
		u64 userId = n / 5 + 1;
		u64 publishTime = System::currentTimeMillis();
		char* title = randomStr(100);

		char* tags = randomStr(100);
		rb.appendBigInt(id);
		rb.appendBigInt(userId);
		rb.appendBigInt(publishTime);
		rb.appendVarchar(title);
		rb.appendVarchar(tags);
		
		char *abs;
		if(!lobNotNull && (n % 2 == 0)){
			rb.appendNull();
		} else{
			abs = randomStr(100);
			rb.appendSmallLob((const byte*)abs);
		}

		size_t contentSize;
		if(!(n % 2)) {
			if (lobNotNull)
				contentSize = TNTLobTestCase::SMALL_LOB_SIZE;
			else
				contentSize = 0;
		} else
			contentSize = TNTLobTestCase::LARGE_LOB_SIZE;

		char *content = NULL;
		if(contentSize) {
			content = randomStr(contentSize);
			rb.appendMediumLob((const byte*)content);
		} else {
			rb.appendNull();
		}

		Record *rec = rb.getRecord();
		Connection *conn = db->getNtseDb()->getConnection(false);
		uint dupIndex;

		//插入指定数据
		CPPUNIT_ASSERT((rec->m_rowId = table->insert(m_session, rec->m_data, &dupIndex, &m_opInfo, true)) != INVALID_ROW_ID);

		contentArray[n] = (byte *)content;

		rows[n] = rec;
		delete []title;
		delete []tags;
		if(abs != NULL)
			delete[] abs;
	}
}


/**
 * 生成Blog表定义
 *
 * @param useMms 是否使用MMS
 * @return BlogCount表定义
 */
TableDef* TNTLobTestCase::getBlogDef(bool useMms, bool onlineIdx) {
	TableDefBuilder *builder = new TableDefBuilder(TableDef::INVALID_TABLEID, "space", "Blog");
	
	builder->addColumn("ID", CT_BIGINT, false)->addColumn("UserID", CT_BIGINT, false);
	builder->addColumn("PublishTime", CT_BIGINT);
	builder->addColumnS("Title", CT_VARCHAR, 411)->addColumnS("Tags", CT_VARCHAR, 2048);
	builder->addColumn("Abstract", CT_SMALLLOB)->addColumn("Content", CT_MEDIUMLOB);
	builder->addIndex("PRIMARY", true, true, onlineIdx, "ID", 0, NULL);
	builder->addIndex("IDX_BLOG_TITLE", false, false, onlineIdx, "Title", 0, NULL);
	builder->addIndex("IDX_BLOG_PUTTIME", false, false, onlineIdx, "PublishTime", 0, NULL);
	builder->addIndex("IDX_BLOG_PUBTIME", false, false, onlineIdx, "UserID", 0, "PublishTime", 0, NULL);

	TableDef *tableDef = builder->getTableDef();
	tableDef->m_useMms = useMms;
	tableDef->setTableStatus(TS_TRX);

	delete builder;
	return tableDef;
}

/**
 * 测试大对象更新
 *
 * @param db 数据库
 * @param table 数据表
 * @param rows  数据库中行（用于检测扫描到的行rowid是否相等）
 * @param numRows 更新的行数
 * @param resContents 更新的大对象Contents后项
 * @param cout 用于区别多次更新同一行或者是一次更新不同行

 * @return 更新的行数目
 */
uint TNTLobTestCase::updateBlog(TNTDatabase *db, TNTTable *table,  Record **rows, uint numRows, byte** resContents, int cout) {
	uint  updDelRows = 0;
	TableDef *tableDef = table->getNtseTable()->getTableDef();
	SubRecordBuilder srb(tableDef, REC_REDUNDANT);
	//要求返回ID, UserID, Tags, Abstract 字段
	byte *mysqlRowUpperLayer = new byte[table->getNtseTable()->getTableDef()->m_maxMysqlRecSize];
	u16 columns[4] = {0, 1, 4, 5};
	
	System::srandom(1);
	TNTTblScan *scanHandle;
	
	int idx = 0;
	IndexScanCond idxCond(idx, NULL, true, false, false);
	scanHandle = table->indexScan(m_session, OP_WRITE, &m_opInfo, &idxCond, 4, columns);
	u16 updateColumns[3] = {3, 4, 6};

	scanHandle->setUpdateSubRecord(3, updateColumns);
	

	u64 spAfterScan = m_session->getMemoryContext()->setSavepoint();


	uint gotRows = 0;
	while(table->getNext(scanHandle, mysqlRowUpperLayer)){
		CPPUNIT_ASSERT(scanHandle->getCurrentRid() == rows[gotRows]->m_rowId); //判断rowID 是否相等
		size_t newTitleSize = 50;
		size_t newTagsSize = 80;
		char *title = randomStr(newTitleSize);
		char *tags = randomStr(newTagsSize);
		size_t newLobSize = (gotRows % 2) ? LARGE_LOB_SIZE: SMALL_LOB_SIZE;
		char *content = randomStr(newLobSize);
		
		RecordBuilder rb(table->getNtseTable()->getTableDef(), 0, REC_UPPMYSQL);
	
		rb.appendBigInt(0);		// id
		rb.appendBigInt(0);		// userid
		rb.appendBigInt(0);		// publishtime
		rb.appendVarchar(title);// title
		rb.appendVarchar(tags); // tags
		rb.appendNull();		// abstract
		rb.appendMediumLob((const byte*)content); // content

		Record *upRec = rb.getRecord();
	
		CPPUNIT_ASSERT(table->updateCurrent(scanHandle, upRec->m_data));
		updDelRows++;
		delete []title;
		delete []tags;
		resContents[cout] = (byte *)content;
		cout++;
		
		gotRows++;
		freeRecord(upRec);
	}	
	delete []mysqlRowUpperLayer;
	table->endScan(scanHandle);
	CPPUNIT_ASSERT(gotRows == numRows);
	

	return gotRows;
}

//测试TNTTable大对象的删除
uint TNTLobTestCase::deleteBlog(TNTDatabase *db, TNTTable *table, uint numRows){
	uint  updDelRows = 0;
	//要求返回ID, UserID, Tags, Abstract 字段
	byte *mysqlRowUpperLayer = new byte[table->getNtseTable()->getTableDef()->m_maxMysqlRecSize];
	u16 columns[4] = {0, 1, 4, 5};


	System::srandom(1);

	TNTTblScan *scanHandle;

	int idx = 0;
	IndexScanCond idxCond(idx, NULL, true, false, false);
	scanHandle = table->indexScan(m_session, OP_WRITE, &m_opInfo, &idxCond, 4, columns);
	u16 updateColumns[3] = {3, 4, 6};

	u64 spAfterScan = m_session->getMemoryContext()->setSavepoint();

	uint gotRows = 0;

	while(table->getNext(scanHandle, mysqlRowUpperLayer)){
		//CPPUNIT_ASSERT(scanHandle->getCurrentRid() == rows[gotRows]->m_rowId); //判断rowID 是否相等		
		table->deleteCurrent(scanHandle);
		updDelRows++;
	
		gotRows++;
	}	
	table->endScan(scanHandle);
//	CPPUNIT_ASSERT(gotRows == numRows);
	delete []mysqlRowUpperLayer;
	printf("\n%d rows fetched.\n", gotRows);
	
	m_session->setTrans(NULL);
	return updDelRows;
}



uint TNTLobTestCase::crRead(TNTDatabase *db, TNTTable *table, byte **resContents, int times){
	TNTTransaction *trx = beginTransaction(OP_READ);
	TrxId txId = trx->getTrxId();
	trx->setReadView(NULL);
	trx->setTrxId(101 + times);
	m_session->setTrans(trx);
	TNTTrxSys *trxSys = m_db->getTransSys();
	u64 trxArray[100];
	trxArray[0] = 100;
	//trxArray[1] = 100;
	TrxId maxTrxId = trxSys->getMaxTrxId(); 
	trxSys->setMaxTrxId(102+times);
	trxSys->trxAssignReadView(trx,trxArray, 1);


	m_opInfo.m_selLockType = TL_NO;

	byte *mysqlRowUpperLayer = new byte[table->getNtseTable()->getTableDef()->m_maxMysqlRecSize];
	byte *mysqlRow = new byte[table->getNtseTable()->getTableDef()->m_maxRecSize];

	u16 columns[1] = {6};
	SubRecord upperMysqlRow(REC_UPPMYSQL, 1, columns, mysqlRowUpperLayer, table->getNtseTable()->getTableDef()->m_maxMysqlRecSize);
	SubRecord engineMysqlRow(REC_MYSQL, 1, columns, mysqlRow, table->getNtseTable()->getTableDef()->m_maxRecSize);

	MemoryContext* lobCtx = new MemoryContext(Limits::PAGE_SIZE, 1);
	TNTTblScan *scanHandle = table->tableScan(m_session, OP_READ, &m_opInfo, 1, columns, false, lobCtx);
	int gotRows = 0;
	while(table->getNext(scanHandle, mysqlRowUpperLayer, INVALID_ROW_ID, true)){
		uint matchRow = gotRows;
		RecordOper::convertSubRecordMUpToEngine(table->getNtseTable()->getTableDef(), &upperMysqlRow, &engineMysqlRow);
		uint lobSize = RecordOper::readLobSize(mysqlRow, m_tableDef->m_columns[6]);
		byte *lob = RecordOper::readLob(mysqlRow, m_tableDef->m_columns[6]);
		if(gotRows % 2)
			CPPUNIT_ASSERT(lobSize == LARGE_LOB_SIZE);
		else
			CPPUNIT_ASSERT(lobSize == SMALL_LOB_SIZE);
		if(times > 99)
			times = 99;
		CPPUNIT_ASSERT(!memcmp(lob, resContents[times], lobSize));
		gotRows++;
	}
	table->endScan(scanHandle);

	// 读取前缀索引
	IndexScanCond *prefixIdxCond = new IndexScanCond(2, NULL, true, false, false);
	TNTTblScan *scanHandle1 = table->indexScan(m_session, OP_READ, &m_opInfo, prefixIdxCond, 1, columns, false, false, lobCtx);
	int gotRows1 = 0;
	while(table->getNext(scanHandle1, mysqlRowUpperLayer, INVALID_ROW_ID, true)){
		uint matchRow = gotRows1;
		RecordOper::convertSubRecordMUpToEngine(table->getNtseTable()->getTableDef(), &upperMysqlRow, &engineMysqlRow);
		
		uint lobSize = RecordOper::readLobSize(mysqlRow, m_tableDef->m_columns[6]);
		byte *lob = RecordOper::readLob(mysqlRow, m_tableDef->m_columns[6]);
		//验证得到的数据的正确性
		if(gotRows1 % 2)
			CPPUNIT_ASSERT(lobSize == LARGE_LOB_SIZE);
		else
			CPPUNIT_ASSERT(lobSize == SMALL_LOB_SIZE);
		CPPUNIT_ASSERT(!memcmp(lob, resContents[gotRows1], lobSize));
	
		gotRows1++;
	}
	table->endScan(scanHandle1);
	delete prefixIdxCond;

	// 表扫描和索引扫描结果数据一致
	CPPUNIT_ASSERT(gotRows == gotRows1);

	trx->setTrxId(txId);
	commitTransaction(trx);
	delete []mysqlRowUpperLayer;
	delete []mysqlRow;
	delete lobCtx;

	trxSys->setMaxTrxId(maxTrxId);
	
	return gotRows;	
}

void TNTLobTestCase::reOpenDb() {
	m_db->closeTable(m_session, m_table);
	Database *db = m_db->getNtseDb();
	db->getSessionManager()->freeSession(m_session);
	db->freeConnection(m_conn);
	m_db->close(false, false);

	delete m_tableDef;
	m_tableDef = NULL;
	delete m_db;
	m_db = NULL;

	EXCPT_OPER(m_db = TNTDatabase::open(&m_config, false, 0));

	db = m_db->getNtseDb();
	m_conn = db->getConnection(false);
	m_conn->setTrx(true);
	m_session = db->getSessionManager()->allocSession("TNTLobTestCase::reOpen", m_conn);
	assert(m_db != NULL && m_session != NULL && m_conn != NULL);
	TNTTransaction *trx = beginTransaction(OP_WRITE);
	m_tableDef = getBlogDef();
	try {
		m_table = m_db->openTable(m_session, "Blog");
	} catch (NtseException &e) {
		printf("%s", e.getMessage());
	}
	assert(m_table != NULL);
	commitTransaction(trx);

}


/**
 * 读表中的数据
 *
 * @param db 数据库
 * @param table 数据表
 * @param scanType  扫描类型
 * @param contentArray  校验用的大对象数组
 * @param readIdxCol  可选参数，是否读取索引1的属性列“Title”
 * @param recordNum 可选参数，用于positionScan  
 * @param rows 可选参数，用于positionScan

 * @return 读到的行数目
 */
uint TNTLobTestCase::currentRead(TNTDatabase *db, TNTTable *table, ScanType scanType, byte **contentArray, bool readIdxCol, uint recordNum, Record **rows){
	int gotRows = 0;
	int gotRows1 = 0;
	TNTTransaction * trx = beginTransaction(OP_READ);
	trx->trxAssignReadView();
	m_opInfo.m_selLockType = TL_NO;
	byte *mysqlRowUpperLayer = new byte[table->getNtseTable()->getTableDef()->m_maxMysqlRecSize];
	byte *mysqlRow = new byte[table->getNtseTable()->getTableDef()->m_maxRecSize];

	u16 columns[1];
	if (readIdxCol)
		columns[0] = 3;
	else
		columns[0] = 6;

	SubRecord upperMysqlRow(REC_UPPMYSQL, 1, columns, mysqlRowUpperLayer, table->getNtseTable()->getTableDef()->m_maxMysqlRecSize);
	SubRecord engineMysqlRow(REC_MYSQL, 1, columns, mysqlRow, table->getNtseTable()->getTableDef()->m_maxRecSize);

	MemoryContext* lobCtx = new MemoryContext(Limits::PAGE_SIZE, 1);

	if(scanType == ST_TBL_SCAN) {
		TNTTblScan *scanHandle = table->tableScan(m_session, OP_READ, &m_opInfo, 1, columns, false, lobCtx);
		while(table->getNext(scanHandle, mysqlRowUpperLayer, INVALID_ROW_ID, true)){
			uint matchRow = gotRows;
			// 将上层Mysql格式再转换成引擎层格式
			RecordOper::convertSubRecordMUpToEngine(table->getNtseTable()->getTableDef(), &upperMysqlRow, &engineMysqlRow);
			uint lobSize = RecordOper::readLobSize(mysqlRow, m_tableDef->m_columns[6]);
			byte *lob = RecordOper::readLob(mysqlRow, m_tableDef->m_columns[6]);
			//验证得到的数据的正确性
			if(gotRows % 2)
					CPPUNIT_ASSERT(lobSize == LARGE_LOB_SIZE);
				else
					CPPUNIT_ASSERT(lobSize == SMALL_LOB_SIZE);
			CPPUNIT_ASSERT(!memcmp(lob, contentArray[gotRows], lobSize));
			gotRows++;
		}
		table->endScan(scanHandle);
	}

	else if(scanType == ST_IDX_SCAN) {
		int idx = 0;
		if(readIdxCol)
			idx = 1;
		IndexScanCond *idxCond = new IndexScanCond(idx, NULL, true, false, false);
		TNTTblScan *scanHandle = table->indexScan(m_session, OP_READ, &m_opInfo, idxCond, 1, columns, false, false, lobCtx);
		while(table->getNext(scanHandle, mysqlRowUpperLayer, INVALID_ROW_ID, true)){
			uint matchRow = gotRows;
 			RecordOper::convertSubRecordMUpToEngine(table->getNtseTable()->getTableDef(), &upperMysqlRow, &engineMysqlRow);
			if(readIdxCol == false) {
				uint lobSize = RecordOper::readLobSize(mysqlRow, m_tableDef->m_columns[6]);
				byte *lob = RecordOper::readLob(mysqlRow, m_tableDef->m_columns[6]);
				//验证得到的数据的正确性
				if(gotRows % 2)
					CPPUNIT_ASSERT(lobSize == LARGE_LOB_SIZE);
				else
					CPPUNIT_ASSERT(lobSize == SMALL_LOB_SIZE);
				CPPUNIT_ASSERT(!memcmp(lob, contentArray[gotRows], lobSize));
			}
			gotRows++;
		}
		table->endScan(scanHandle);
		delete idxCond;

		// 测试前缀索引读
		IndexScanCond *prefixIdxCond = new IndexScanCond(2, NULL, true, false, false);
		TNTTblScan *scanHandle1 = table->indexScan(m_session, OP_READ, &m_opInfo, prefixIdxCond, 1, columns, false, false, lobCtx);
		while(table->getNext(scanHandle1, mysqlRowUpperLayer, INVALID_ROW_ID, true)){
			uint matchRow = gotRows1;
			RecordOper::convertSubRecordMUpToEngine(table->getNtseTable()->getTableDef(), &upperMysqlRow, &engineMysqlRow);
			if(readIdxCol == false) {
				uint lobSize = RecordOper::readLobSize(mysqlRow, m_tableDef->m_columns[6]);
				byte *lob = RecordOper::readLob(mysqlRow, m_tableDef->m_columns[6]);
				//验证得到的数据的正确性
				if(gotRows1 % 2)
					CPPUNIT_ASSERT(lobSize == LARGE_LOB_SIZE);
				else
					CPPUNIT_ASSERT(lobSize == SMALL_LOB_SIZE);
				CPPUNIT_ASSERT(!memcmp(lob, contentArray[gotRows1], lobSize));
			}
			gotRows1++;
		}
		table->endScan(scanHandle1);
		delete prefixIdxCond;
	}

	else if(scanType == ST_POS_SCAN) {
		TNTTblScan *scanHandle = table->positionScan(m_session, OP_READ, &m_opInfo, 1, columns, false, lobCtx);
		int cout = 0;
		while(table->getNext(scanHandle, mysqlRowUpperLayer, rows[cout]->m_rowId, true)){
			uint matchRow = gotRows;
			RecordOper::convertSubRecordMUpToEngine(table->getNtseTable()->getTableDef(), &upperMysqlRow, &engineMysqlRow);
			uint lobSize = RecordOper::readLobSize(mysqlRow, m_tableDef->m_columns[6]);
			byte *lob = RecordOper::readLob(mysqlRow, m_tableDef->m_columns[6]);
			//验证得到的数据的正确性
			if(gotRows % 2)
				CPPUNIT_ASSERT(lobSize == LARGE_LOB_SIZE);
			else
				CPPUNIT_ASSERT(lobSize == SMALL_LOB_SIZE);
			CPPUNIT_ASSERT(!memcmp(lob, contentArray[gotRows], lobSize));
			gotRows++;

			cout++;
			if(cout == recordNum)
				break;
		}
		table->endScan(scanHandle);
	}

	commitTransaction(trx);

	delete lobCtx;
	
	delete []mysqlRowUpperLayer;
	delete []mysqlRow;
	return gotRows;	
}


void TNTLobTestCase::testLobInsert() {
	uint numRows = 100;
	Record **rows = new Record *[numRows];
	byte **contentArray = new byte *[numRows];

	//向表中插入数据
	TNTTransaction *trx = beginTransaction(OP_WRITE);
	EXCPT_OPER(populateBlog(m_db, m_table, numRows, rows, true, contentArray));
	commitTransaction(trx);
	printf("%d rows inserted.\n", numRows);

	
	//三种表扫描方式验证数据正确性
	uint readRows = 0;

 	readRows = currentRead(m_db, m_table, ST_IDX_SCAN, contentArray);
	assert(readRows == numRows);


	//清理保存结果的数组
	for(uint i = 0; i < numRows; i++) {
		delete[] contentArray[i];
		freeRecord(rows[i]);
	}

	delete[] rows;
	delete[] contentArray;

}

void TNTLobTestCase::testLobUpdate() {
	uint numRows = 100;
	Record **rows = new Record *[numRows];
	byte **contentArray = new byte *[numRows];
	byte **upContentArray = new byte *[numRows];

	//向表中插入数据
	TNTTransaction *trx = beginTransaction(OP_WRITE);
	EXCPT_OPER(populateBlog(m_db, m_table, numRows, rows, true, contentArray));
	commitTransaction(trx);
	printf("%d rows inserted.\n", numRows);


	//更新数据
	System::srandom(1);
	uint rowsUpdated = 0;
	m_opInfo.m_selLockType = TL_X;
	TNTTransaction *trx1 = beginTransaction(OP_UPDATE);
	EXCPT_OPER(rowsUpdated = updateBlog(m_db, m_table,  rows, numRows, upContentArray, 0));
	commitTransaction(trx1);
	
	//验证数据正确性
	uint readRows = 0;
	readRows = currentRead(m_db, m_table, ST_TBL_SCAN, upContentArray);
	assert(readRows == numRows);

	//清理保存结果的数组
	for(uint i = 0; i < numRows; i++) {
		delete[] contentArray[i];
		delete[] upContentArray[i];
		freeRecord(rows[i]);
	}

	delete[] rows;
	delete[] contentArray;
	delete[] upContentArray;

}

void TNTLobTestCase::testLobCRRead() {
	uint numRows = 1;
	uint crNum = 100;
	Record **rows = new Record *[numRows];
	byte **contentArray = new byte *[numRows];
	byte **upContentArray = new byte *[crNum];	//用于保存多次更新的大对象

	//向表中插入数据
	TNTTransaction *trx = beginTransaction(OP_WRITE);
	EXCPT_OPER(populateBlog(m_db, m_table, numRows, rows, true, contentArray));
	commitTransaction(trx);
	printf("%d rows inserted.\n", numRows);

	//更新数据
	System::srandom(1);
	uint rowsUpdated = 0;
	m_opInfo.m_selLockType = TL_X;
	for(uint i = 0; i < crNum; i++ ) {
		TNTTransaction *trx1 = beginTransaction(OP_UPDATE);
		EXCPT_OPER(rowsUpdated = updateBlog(m_db, m_table, rows, numRows, upContentArray, i));
		commitTransaction(trx1);
	}
	
	//测试多版本读
	for(uint i = 0; i < crNum; i++){	
		crRead(m_db, m_table, upContentArray, i);
	}

	//清理保存结果的数组
	for(uint i = 0; i < crNum; i++) {
		delete[] upContentArray[i];
	}
	for(uint i = 0; i < numRows; i++) {
		freeRecord(rows[i]);
		delete[] contentArray[i];
	}
	delete[] rows;
	delete[] contentArray;
	delete[] upContentArray;

}

void TNTLobTestCase::testLobDelete() {
	uint numRows = 100;
	Record **rows = new Record *[numRows];
	byte **contentArray = new byte *[numRows];

	//向表中插入数据
	TNTTransaction *trx = beginTransaction(OP_WRITE);
	EXCPT_OPER(populateBlog(m_db, m_table, numRows, rows, true, contentArray));
	commitTransaction(trx);
	printf("%d rows inserted.\n", numRows);
	
	//测试大对象删除    
	uint rowsDeleted = 0;;
	m_opInfo.m_selLockType = TL_X;
	TNTTransaction *trx1 = beginTransaction(OP_DELETE);
	EXCPT_OPER(rowsDeleted = deleteBlog(m_db, m_table, numRows));
	commitTransaction(trx1);

	//验证数据
	uint readRows = 0;
	EXCPT_OPER(readRows = currentRead(m_db, m_table, ST_TBL_SCAN, contentArray));
	assert(readRows == 0);
 	
	
	//清理保存结果的数组
	for(uint i = 0; i < numRows; i++) {
		freeRecord(rows[i]);
		delete[] contentArray[i];
	}
	delete[] rows;
	delete[] contentArray;
}


void TNTLobTestCase::testLobPurge() {
	uint numRows = 100;
	Record **rows = new Record *[numRows];
	byte **contentArray = new byte *[numRows];
	byte **upContentArray = new byte *[numRows];	//用于保存多次更新的大对象

	//向表中插入数据
	TNTTransaction *trx = beginTransaction(OP_WRITE);
	EXCPT_OPER(populateBlog(m_db, m_table, numRows, rows, true, contentArray, 1000));
	commitTransaction(trx);
	printf("%d rows inserted.\n", numRows);

	//更新数据
	System::srandom(1);
	uint rowsUpdated = 0;
	m_opInfo.m_selLockType = TL_X;
	TNTTransaction *trx1 = beginTransaction(OP_UPDATE);
	EXCPT_OPER(rowsUpdated = updateBlog(m_db, m_table,  rows, numRows, upContentArray, 0));
	commitTransaction(trx1);
	

	//测试purge中的大对象更新  // 1
	TNTTransaction *purgeTrx = beginTransaction(OP_READ);
	purgeTrx->trxAssignReadView();
	//purge phase 1
	m_table->purgePhase1(m_session, purgeTrx);
	//purge phase 2
	m_table->purgePhase2(m_session, purgeTrx);
	commitTransaction(purgeTrx);
	

	//验证数据
	uint readRows = 0;
	EXCPT_OPER(readRows = currentRead(m_db, m_table, ST_TBL_SCAN, upContentArray));
	assert(readRows == numRows);


	//删除数据     //0
	uint rowsDeleted = 0;;
	m_opInfo.m_selLockType = TL_X;
	TNTTransaction *trx2 = beginTransaction(OP_DELETE);
	EXCPT_OPER(rowsDeleted = deleteBlog(m_db, m_table, numRows));
	commitTransaction(trx2);

	//测试purge中的大对象删除  //0
	
	TNTTransaction *purgeTrx2 = beginTransaction(OP_READ);
	purgeTrx2->trxAssignReadView();
	//purge phase 1
	m_table->purgePhase1(m_session, purgeTrx2);
	//purge phase 2
	m_table->purgePhase2(m_session, purgeTrx2);
	commitTransaction(purgeTrx2);

	//验证数据
	uint readRows2 = 0;
	EXCPT_OPER(readRows2 = currentRead(m_db, m_table, ST_TBL_SCAN, upContentArray));
	assert(readRows2 == 0);

	//清理保存结果的数组
	for(uint i = 0; i < numRows; i++) {
		freeRecord(rows[i]);
		delete[] contentArray[i];
		delete[] upContentArray[i];
	}
	delete[] rows;
	delete[] contentArray;
	delete[] upContentArray;

}

void TNTLobTestCase::testLobPurge2() {
	uint numRows = 1;
	Record **rows = new Record *[numRows];
	byte **contentArray = new byte *[numRows];
	byte **upContentArray = new byte *[numRows];	//用于保存多次更新的大对象

	//向表中插入数据
	TNTTransaction *trx = beginTransaction(OP_WRITE);
	EXCPT_OPER(populateBlog(m_db, m_table, numRows, rows, true, contentArray));
	commitTransaction(trx);
	printf("%d rows inserted.\n", numRows);

	//更新数据
	System::srandom(1);
	uint rowsUpdated = 0;
	m_opInfo.m_selLockType = TL_X;
	TNTTransaction *trx1 = beginTransaction(OP_UPDATE);
	EXCPT_OPER(rowsUpdated = updateBlog(m_db, m_table,  rows, numRows, upContentArray, 0));
	commitTransaction(trx1);

	for(uint i = 0; i < numRows; i++) {
		delete[] upContentArray[i];
	}

	//测试purge
	RealPurgeLobThread rPurgeThread(m_db, m_table, m_opInfo);
	rPurgeThread.enableSyncPoint(SP_TNT_TABLE_PURGE_PHASE2);
	rPurgeThread.start();
	rPurgeThread.joinSyncPoint(SP_TNT_TABLE_PURGE_PHASE2);

	Thread::msleep(1000);
	//更新数据
	uint rowsUpdated1 = 0;
	m_opInfo.m_selLockType = TL_X;
	TNTTransaction *trx2 = beginTransaction(OP_UPDATE);
	EXCPT_OPER(rowsUpdated = updateBlog(m_db, m_table,  rows, numRows, upContentArray, 0));
	commitTransaction(trx2);

	//验证数据(表)
	uint readRows1 = 0;
	EXCPT_OPER(readRows1 = currentRead(m_db, m_table, ST_TBL_SCAN, upContentArray));
	assert(readRows1 == numRows);

	//验证数据(索引)
	uint readRows2 = 0;
	EXCPT_OPER(readRows2 = currentRead(m_db, m_table, ST_IDX_SCAN, upContentArray, true));
	assert(readRows2 == numRows);

	rPurgeThread.disableSyncPoint(SP_TNT_TABLE_PURGE_PHASE2);
	rPurgeThread.notifySyncPoint(SP_TNT_TABLE_PURGE_PHASE2);
	rPurgeThread.join(-1);
	

	//清理保存结果的数组
	for(uint i = 0; i < numRows; i++) {
		freeRecord(rows[i]);
		delete[] contentArray[i];
		delete[] upContentArray[i];
	}
	delete[] rows;
	delete[] contentArray;
	delete[] upContentArray;
}


void TNTLobTestCase::testLobPurge3() {
	uint numRows = 1;
	Record **rows = new Record *[numRows];
	byte **contentArray = new byte *[numRows];

	//向表中插入数据
	TNTTransaction *trx = beginTransaction(OP_WRITE);
	EXCPT_OPER(populateBlog(m_db, m_table, numRows, rows, true, contentArray));
	commitTransaction(trx);
	printf("%d rows inserted.\n", numRows);

	//删除数据
	uint rowsDeleted = 0;;
	m_opInfo.m_selLockType = TL_X;
	TNTTransaction *trx1 = beginTransaction(OP_DELETE);
	EXCPT_OPER(rowsDeleted = deleteBlog(m_db, m_table, numRows));
	commitTransaction(trx1);


	//测试purge
	RealPurgeLobThread rPurgeThread(m_db, m_table, m_opInfo);
	rPurgeThread.enableSyncPoint(SP_TNT_TABLE_PURGE_PHASE2);
	rPurgeThread.start();
	rPurgeThread.joinSyncPoint(SP_TNT_TABLE_PURGE_PHASE2);

	Thread::msleep(1000);

	//验证数据(表)
	uint readRows1 = 0;
	EXCPT_OPER(readRows1 = currentRead(m_db, m_table, ST_TBL_SCAN, contentArray));
	assert(readRows1 == 0);

	//验证数据(索引)
	uint readRows2 = 0;
	EXCPT_OPER(readRows2 = currentRead(m_db, m_table, ST_IDX_SCAN, contentArray, true));
	assert(readRows2 == 0);

	rPurgeThread.disableSyncPoint(SP_TNT_TABLE_PURGE_PHASE2);
	rPurgeThread.notifySyncPoint(SP_TNT_TABLE_PURGE_PHASE2);
	rPurgeThread.join(-1);


	//清理保存结果的数组
	for(uint i = 0; i < numRows; i++) {
		freeRecord(rows[i]);
		delete[] contentArray[i];
	}
	delete[] rows;
	delete[] contentArray;
}

void TNTLobTestCase::testLobPurge4() {
	uint numRows = 1;
	Record **rows = new Record *[numRows];
	byte **contentArray = new byte *[numRows];
	byte **upContentArray = new byte *[numRows];	//用于保存多次更新的大对象
	byte **upContentArray1 = new byte *[numRows];	//用于保存多次更新的大对象

	//向表中插入数据
	TNTTransaction *trx = beginTransaction(OP_WRITE);
	EXCPT_OPER(populateBlog(m_db, m_table, numRows, rows, true, contentArray));
	commitTransaction(trx);
	printf("%d rows inserted.\n", numRows);

	//更新数据
	System::srandom(1);
	uint rowsUpdated = 0;
	m_opInfo.m_selLockType = TL_X;
	TNTTransaction *trx1 = beginTransaction(OP_UPDATE);
	EXCPT_OPER(rowsUpdated = updateBlog(m_db, m_table,  rows, numRows, upContentArray, 0));
	commitTransaction(trx1);

// 	for(uint i = 0; i < numRows; i++) {
// 		delete[] upContentArray[i];
// 	}

	//更新数据
	uint rowsUpdated1 = 0;
	m_opInfo.m_selLockType = TL_X;
	TNTTransaction *trx2 = beginTransaction(OP_UPDATE);
	EXCPT_OPER(rowsUpdated = updateBlog(m_db, m_table,  rows, numRows, upContentArray1, 0));
	//	commitTransaction(trx2);


	//测试purge
	RealPurgeLobThread rPurgeThread(m_db, m_table, m_opInfo);
	rPurgeThread.enableSyncPoint(SP_TNT_TABLE_PURGE_PHASE3);
	rPurgeThread.start();
	rPurgeThread.joinSyncPoint(SP_TNT_TABLE_PURGE_PHASE3);

	Thread::msleep(1000);
	rollbackTransaction(trx2);


	rPurgeThread.disableSyncPoint(SP_TNT_TABLE_PURGE_PHASE3);
	rPurgeThread.notifySyncPoint(SP_TNT_TABLE_PURGE_PHASE3);
	rPurgeThread.join(-1);

	//验证数据(表)
	uint readRows1 = 0;
	EXCPT_OPER(readRows1 = currentRead(m_db, m_table, ST_TBL_SCAN, upContentArray));
	assert(readRows1 == numRows);

	//清理保存结果的数组
	for(uint i = 0; i < numRows; i++) {
		freeRecord(rows[i]);
		delete[] contentArray[i];
		delete[] upContentArray[i];
		delete[] upContentArray1[i];

	}
	delete[] rows;
	delete[] contentArray;
	delete[] upContentArray;
	delete[] upContentArray1;
}

void TNTLobTestCase::testLobRollback() {
	//由于插入回滚后再插入，前后两次页面分配的顺序可能不一致，所以这里要限制插入数据的行数保证不会发生上述情况
	uint numRows = 20;	
	Record **rows = new Record *[numRows];
	byte **contentArray = new byte *[numRows];
	byte **upContentArray = new byte *[numRows];	//用于保存多次更新的大对象

	//向表中插入数据，并且回滚
	TNTTransaction *trx = beginTransaction(OP_WRITE);
	EXCPT_OPER(populateBlog(m_db, m_table, numRows, rows, true, contentArray));
	rollbackTransaction(trx);
	printf("%d rows inserted.\n", numRows);

	//验证数据
	uint readRows = 0;
	EXCPT_OPER(readRows = currentRead(m_db, m_table, ST_TBL_SCAN, contentArray));
	assert(readRows == 0);


	//清理保存结果的数组
	for(uint i = 0; i < numRows; i++) {
		freeRecord(rows[i]);
		delete[] contentArray[i];
	}

	//向表中插入数据
	TNTTransaction *trx1 = beginTransaction(OP_WRITE);
	EXCPT_OPER(populateBlog(m_db, m_table, numRows, rows, true, contentArray));
	commitTransaction(trx1);
	printf("%d rows inserted.\n", numRows);


	//更新并且回滚
	System::srandom(1);
	uint rowsUpdated = 0;
	m_opInfo.m_selLockType = TL_X;
	TNTTransaction *trx2 = beginTransaction(OP_UPDATE);
	EXCPT_OPER(rowsUpdated = updateBlog(m_db, m_table,  rows, numRows, upContentArray, 0));
	rollbackTransaction(trx2);

	//验证数据
	uint readRows2 = 0;
	EXCPT_OPER(readRows2 = currentRead(m_db, m_table, ST_IDX_SCAN, contentArray));
	assert(readRows2 == numRows);

	//清理保存结果的数组
	for(uint i = 0; i < numRows; i++) {
		delete[] upContentArray[i];
	}


	//删除数据并且回滚
	uint rowsDeleted = 0;;
	m_opInfo.m_selLockType = TL_X;
	TNTTransaction *trx3 = beginTransaction(OP_DELETE);
	EXCPT_OPER(rowsDeleted = deleteBlog(m_db, m_table, numRows));
	rollbackTransaction(trx3);

	//验证数据
	uint readRows3 = 0;
	EXCPT_OPER(readRows3 = currentRead(m_db, m_table, ST_IDX_SCAN, contentArray));
	assert(readRows3 == numRows);

	reOpenDb();

	//验证数据
	uint readRows4 = 0;
	EXCPT_OPER(readRows4 = currentRead(m_db, m_table, ST_IDX_SCAN, contentArray));
	assert(readRows4 == numRows);

	//清理保存结果的数组
	for(uint i = 0; i < numRows; i++) {
		delete[] contentArray[i];
		freeRecord(rows[i]);
	}

	delete[] rows;
	delete[] contentArray;
	delete[] upContentArray;
	
}

void TNTLobTestCase::testLobPartialRollback() {
	uint numRows = 100;	
	Record **rows = new Record *[numRows];
	byte **contentArray = new byte *[numRows];
	byte **upContentArray = new byte *[numRows];	//用于保存多次更新的大对象


	//向表中插入数据
	TNTTransaction *trx = beginTransaction(OP_WRITE);
	EXCPT_OPER(populateBlog(m_db, m_table, numRows, rows, true, contentArray));
	printf("%d rows inserted.\n", numRows);

	//设置lastStatmentLSN
	LsnType lsn = trx->getTrxLastLsn();
	trx->setLastStmtLsn(lsn);
	//更新数据
	System::srandom(1);
	uint rowsUpdated = 0;
	m_opInfo.m_selLockType = TL_X;
	EXCPT_OPER(rowsUpdated = updateBlog(m_db, m_table,  rows, numRows, upContentArray, 0));

	trx->rollbackLastStmt(RBS_NORMAL);
	m_db->getTransSys()->freeTrx(trx);
 
	//验证数据
	uint readRows3 = 0;
	EXCPT_OPER(readRows3 = currentRead(m_db, m_table, ST_TBL_SCAN, contentArray));
	assert(readRows3 == numRows);

	reOpenDb();

	//清理保存结果的数组
	for(uint i = 0; i < numRows; i++) {
		delete[] contentArray[i];
		delete[] upContentArray[i];
		freeRecord(rows[i]);
	}

	delete[] rows;
	delete[] contentArray;
	delete[] upContentArray;
}

void TNTLobTestCase::testLobRecovery() {

	uint numRows = 1;	
	Record **rows = new Record *[numRows];
	byte **contentArray = new byte *[numRows];
	byte **upContentArray = new byte *[numRows];	//用于保存多次更新的大对象


	//向表中插入数据
	TNTTransaction *trx = beginTransaction(OP_WRITE);
	EXCPT_OPER(populateBlog(m_db, m_table, numRows, rows, true, contentArray));
	commitTransaction(trx);
	printf("%d rows inserted.\n", numRows);

	//恢复
	reOpenDb();

	//验证数据
	uint readRows = 0;
	EXCPT_OPER(readRows = currentRead(m_db, m_table, ST_TBL_SCAN, contentArray));
	assert(readRows == numRows);



	//更新数据
	System::srandom(1);
	uint rowsUpdated1 = 0;
	m_opInfo.m_selLockType = TL_X;
	TNTTransaction *trx3 = beginTransaction(OP_UPDATE);
	EXCPT_OPER(rowsUpdated1 = updateBlog(m_db, m_table,  rows, numRows, upContentArray, 0));
	commitTransaction(trx3);

	//验证数据
	EXCPT_OPER(readRows = currentRead(m_db, m_table, ST_TBL_SCAN, upContentArray));
	assert(readRows == numRows);

	//更新purge恢复
	TNTTransaction *purgeTrx = beginTransaction(OP_READ);
	purgeTrx->trxAssignReadView();
	//purge phase 1
	m_table->purgePhase1(m_session, purgeTrx);
	//purge phase 2
	 m_db->getTransSys()->freeTrx(purgeTrx);
	reOpenDb();

	//验证数据
	EXCPT_OPER(readRows = currentRead(m_db, m_table, ST_TBL_SCAN, upContentArray));
	assert(readRows == numRows);

	//删除数据
	uint rowsDeleted = 0;;
	m_opInfo.m_selLockType = TL_X;
	TNTTransaction *trx4 = beginTransaction(OP_DELETE);
	EXCPT_OPER(rowsDeleted = deleteBlog(m_db, m_table, numRows));
	commitTransaction(trx4);

	//验证数据
	EXCPT_OPER(readRows = currentRead(m_db, m_table, ST_TBL_SCAN, upContentArray));
	assert(readRows == 0);

	//删除purge恢复
	TNTTransaction *purgeTrx2 = beginTransaction(OP_READ);
	purgeTrx2->trxAssignReadView();
	//purge phase 1
	m_table->purgePhase1(m_session, purgeTrx2);
	//purge phase 2
	 m_db->getTransSys()->freeTrx(purgeTrx2);
	reOpenDb();

	//验证数据
	EXCPT_OPER(readRows = currentRead(m_db, m_table, ST_TBL_SCAN, upContentArray));
	assert(readRows == 0);

	//清理保存结果的数组
	for(uint i = 0; i < numRows; i++) {
		delete[] upContentArray[i];
		delete[] contentArray[i];
		freeRecord(rows[i]);
	}

	delete[] rows;
	delete[] contentArray;
	delete[] upContentArray;

}
void TNTLobTestCase::testLobIdReuseInRollBack() {
	InsertLobThread insertThread(m_db, m_table, m_opInfo);
	insertThread.enableSyncPoint(SP_LOB_ROLLBACK_DELETE);
	insertThread.start();
	insertThread.joinSyncPoint(SP_LOB_ROLLBACK_DELETE);

	uint numRows = 100;	
	Record **rows = new Record *[numRows];
	byte **contentArray = new byte *[numRows];

	//向表中插入数据
	TNTTransaction *trx = beginTransaction(OP_WRITE);
	EXCPT_OPER(populateBlog(m_db, m_table, numRows, rows, true, contentArray));
	commitTransaction(trx);
	printf("%d rows inserted.\n", numRows);
	
	m_db->getTransSys()->freeTrx(insertThread.m_trx);
	reOpenDb();

	insertThread.disableSyncPoint(SP_LOB_ROLLBACK_DELETE);
	insertThread.notifySyncPoint(SP_LOB_ROLLBACK_DELETE);
	insertThread.join(-1);

	
	//清理保存结果的数组
	for(uint i = 0; i < numRows; i++) {
		delete[] contentArray[i];
		freeRecord(rows[i]);
	}
	delete[] rows;
	delete[] contentArray;
}
void TNTLobTestCase::testReclaimLob() {
	uint numRows = 1000;	
	Record **rows = new Record *[numRows];
	byte **contentArray = new byte *[numRows];
	byte **upContentArray = new byte *[numRows];	//用于保存多次更新的大对象


	//向表中插入数据
	TNTTransaction *trx = beginTransaction(OP_WRITE);
	EXCPT_OPER(populateBlog(m_db, m_table, numRows, rows, true, contentArray));
	commitTransaction(trx);
	printf("%d rows inserted.\n", numRows);


	//更新数据
	System::srandom(1);
	uint rowsUpdated = 0;
	m_opInfo.m_selLockType = TL_X;
	TNTTransaction *trx2 = beginTransaction(OP_UPDATE);
	EXCPT_OPER(rowsUpdated = updateBlog(m_db, m_table,  rows, numRows, upContentArray, 0));
	commitTransaction(trx2);

	//切换版本池的情况下，reclaimLob进行大对象回收
	m_config.m_verpoolFileSize = 1024;
	m_db->switchActiveVerPoolIfNeeded();
	m_db->reclaimLob();

	//清理保存结果的数组
	for(uint i = 0; i < numRows; i++) {
		delete[] upContentArray[i];
	}

	//更新数据
	System::srandom(1);
	uint rowsUpdated1 = 0;
	m_opInfo.m_selLockType = TL_X;
	TNTTransaction *trx3 = beginTransaction(OP_UPDATE);
	EXCPT_OPER(rowsUpdated1 = updateBlog(m_db, m_table,  rows, numRows, upContentArray, 0));
	commitTransaction(trx3);

	//不切换版本池的情况下，reclaimLob不进行回收
	m_config.m_verpoolFileSize = 81920;
	m_db->switchActiveVerPoolIfNeeded();
	m_db->reclaimLob();

	//切换版本池的情况下，reclaimLob进行大对象回收
	m_config.m_verpoolFileSize = 1024;
	m_db->switchActiveVerPoolIfNeeded();
	m_db->reclaimLob();

//	reOpenDb();

	//清理保存结果的数组
	for(uint i = 0; i < numRows; i++) {
		delete[] upContentArray[i];
		delete[] contentArray[i];
		freeRecord(rows[i]);
	}

	delete[] rows;
	delete[] contentArray;
	delete[] upContentArray;

}


void TNTLobTestCase::testLobIdReuseInReclaim() {
	uint numRows = 100;	
	Record **rows = new Record *[numRows];
	byte **contentArray = new byte *[numRows];
	byte **upContentArray = new byte *[numRows];	//用于保存多次更新的大对象


	//向表中插入数据
	TNTTransaction *trx = beginTransaction(OP_WRITE);
	EXCPT_OPER(populateBlog(m_db, m_table, numRows, rows, true, contentArray));
	commitTransaction(trx);
	printf("%d rows inserted.\n", numRows);
	


	//更新数据
	System::srandom(1);
	uint rowsUpdated = 0;
	m_opInfo.m_selLockType = TL_X;
	TNTTransaction *trx2 = beginTransaction(OP_UPDATE);
	EXCPT_OPER(rowsUpdated = updateBlog(m_db, m_table,  rows, numRows, upContentArray, 0));
	commitTransaction(trx2);


	//清理保存结果的数组
	for(uint i = 0; i < numRows; i++) {
		delete[] upContentArray[i];
	}

	//更新数据
	System::srandom(1);
	uint rowsUpdated1 = 0;
	m_opInfo.m_selLockType = TL_X;
	TNTTransaction *trx3 = beginTransaction(OP_UPDATE);
	EXCPT_OPER(rowsUpdated1 = updateBlog(m_db, m_table,  rows, numRows, upContentArray, 0));
	commitTransaction(trx3);

	m_db->getTNTConfig()->m_verpoolFileSize = 1024;
	m_db->switchActiveVerPoolIfNeeded();

	ReclaimLobThread reclaimThread(m_db, m_table, m_opInfo);
	reclaimThread.enableSyncPoint(SP_LOB_RECLAIM_DELETE);
	reclaimThread.start();
	reclaimThread.joinSyncPoint(SP_LOB_RECLAIM_DELETE);

	
	//清理保存结果的数组
	for(uint i = 0; i < numRows; i++) {
		freeRecord(rows[i]);
		delete[] contentArray[i];
	}
	//向表中插入数据
	TNTTransaction *trx4 = beginTransaction(OP_WRITE);
	EXCPT_OPER(populateBlog(m_db, m_table, numRows, rows, true, contentArray, 500));
	commitTransaction(trx4);
	printf("%d rows inserted.\n", numRows);
		
	//恢复
	m_db->getTransSys()->freeTrx(reclaimThread.m_trx);
	reOpenDb();

	reclaimThread.disableSyncPoint(SP_LOB_RECLAIM_DELETE);
	reclaimThread.notifySyncPoint(SP_LOB_RECLAIM_DELETE);
	reclaimThread.join(-1);

	//清理保存结果的数组
	for(uint i = 0; i < numRows; i++) {
		delete[] upContentArray[i];
		delete[] contentArray[i];
		freeRecord(rows[i]);
	}

	delete[] rows;
	delete[] contentArray;
	delete[] upContentArray;
}


void TNTLobTestCase::testLobIdReuseInPurge() {
	uint numRows = 100;
	Record **rows = new Record *[numRows];
	byte **contentArray = new byte *[numRows];

	//向表中插入数据
	TNTTransaction *trx = beginTransaction(OP_WRITE);
	EXCPT_OPER(populateBlog(m_db, m_table, numRows, rows, true, contentArray));
	commitTransaction(trx);
	printf("%d rows inserted.\n", numRows);

	//清理保存结果的数组
	for(uint i = 0; i < numRows; i++) {
		freeRecord(rows[i]);
		delete[] contentArray[i];
	}

	//删除数据     //0
	uint rowsDeleted = 0;;
	m_opInfo.m_selLockType = TL_X;
	TNTTransaction *trx2 = beginTransaction(OP_DELETE);
	EXCPT_OPER(rowsDeleted = deleteBlog(m_db, m_table, numRows));
	commitTransaction(trx2);

	PurgeLobThread purgeLobThread(m_db, m_table, m_opInfo);
	purgeLobThread.enableSyncPoint(SP_LOB_PURGE_DELETE);
	purgeLobThread.start();
	purgeLobThread.joinSyncPoint(SP_LOB_PURGE_DELETE);

	//向表中插入数据
	TNTTransaction *trx3 = beginTransaction(OP_WRITE);
	EXCPT_OPER(populateBlog(m_db, m_table, numRows, rows, true, contentArray, 500));
	commitTransaction(trx3);
	printf("%d rows inserted.\n", numRows);

	purgeLobThread.disableSyncPoint(SP_LOB_PURGE_DELETE);
	purgeLobThread.notifySyncPoint(SP_LOB_PURGE_DELETE);

	m_db->getTransSys()->freeTrx(purgeLobThread.m_trx);
	reOpenDb();

	//清理保存结果的数组
	for(uint i = 0; i < numRows; i++) {
		freeRecord(rows[i]);
		delete[] contentArray[i];
	}
	delete[] rows;
	delete[] contentArray;
} 


LobThread::LobThread(TNTDatabase *db, TNTTable *lobTable, TNTOpInfo	opInfo): Thread("InsertThread"){
	m_db = db;
	m_table = lobTable;
	m_opInfo = opInfo;
	
	//config = config;
}


TNTTransaction* LobThread::beginTransaction(OpType intention, u64 *trxIds, uint activeTrans) {
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
		// 注释下面一句，全部可见，类似于当前读；打开注释，全部不可见
		// trx->setTrxId(1);
		//trxSys->trxAssignReadView(trx);
	}

	m_session->setTrans(trx);

	return trx;
}

void LobThread::commitTransaction(TNTTransaction *trx){
	trx->commitTrx(CS_NORMAL);
	m_db->getTransSys()->freeTrx(trx);
	trx = NULL;
	m_session->setTrans(NULL);
}


void LobThread::populateBlog(TNTDatabase *db, TNTTable *table, uint numRows, bool lobNotNull,  u64 idstart) {	
	for(uint n = 0; n < numRows; n++){
		RecordBuilder rb(table->getNtseTable()->getTableDef(), 0, REC_UPPMYSQL);
		u64 id = n + idstart;
		u64 userId = n / 5 + 1;
		u64 publishTime = System::currentTimeMillis();
		char* title = randomStr(100);

		char* tags = randomStr(100);
		rb.appendBigInt(id);
		rb.appendBigInt(userId);
		rb.appendBigInt(publishTime);
		rb.appendVarchar(title);
		rb.appendVarchar(tags);

		char *abs;
		if(!lobNotNull && (n % 2 == 0)){
			rb.appendNull();
		} else{
			abs = randomStr(100);
			rb.appendSmallLob((const byte*)abs);
		}

		size_t contentSize;
		if(!(n % 2)) {
			if (lobNotNull)
				contentSize = TNTLobTestCase::SMALL_LOB_SIZE;
			else
				contentSize = 0;
		} else
			contentSize = TNTLobTestCase::LARGE_LOB_SIZE;

		char *content = NULL;
		if(contentSize) {
			content = randomStr(contentSize);
			rb.appendMediumLob((const byte*)content);
		} else {
			rb.appendNull();
		}

		Record *rec = rb.getRecord();
		uint dupIndex;

		//插入指定数据
		CPPUNIT_ASSERT((rec->m_rowId = table->insert(m_session, rec->m_data, &dupIndex, &m_opInfo, true)) != INVALID_ROW_ID);

		freeRecord(rec);
		delete[] title;
		delete[] tags;
		if(abs != NULL)
			delete[] abs;
		if(content != NULL)
			delete[] content;
	}
}

void InsertLobThread::run() {
	m_conn = m_db->getNtseDb()->getConnection(false);
	m_conn->setTrx(true);
	m_session = m_db->getNtseDb()->getSessionManager()->allocSession("TNTTable_Insert_Lob", m_conn);
	TNTTransaction *trx = beginTransaction(OP_WRITE);
	m_trx = trx;

	//插入数据
	uint numRows = 10;
	EXCPT_OPER(populateBlog(m_db, m_table, numRows, true, 500));

	//写开始回滚的日志
	trx->writeBeginRollBackTrxLog();
	//	m_db->getTransSys()->freeTrx(trx);
	SYNCHERE(SP_LOB_ROLLBACK_DELETE);	

	// 	m_db->getNtseDb()->getSessionManager()->freeSession(m_session);
	// 	m_db->getNtseDb()->freeConnection(m_conn);
}


void ReclaimLobThread::run() {
	m_conn = m_db->getNtseDb()->getConnection(false);
	m_conn->setTrx(true);
	m_session = m_db->getNtseDb()->getSessionManager()->allocSession("TNTTable_Insert_Lob", m_conn);
	TNTTransaction *trx = beginTransaction(OP_WRITE);
	m_trx = trx;

	m_db->getTNTControlFile()->writeBeginReclaimPool(0);
	SYNCHERE(SP_LOB_RECLAIM_DELETE);
}


void PurgeLobThread::run() {
	m_conn = m_db->getNtseDb()->getConnection(false);
	m_conn->setTrx(true);
	m_session = m_db->getNtseDb()->getSessionManager()->allocSession("TNTTable_Insert_Lob", m_conn);
	TNTTransaction *trx = beginTransaction(OP_WRITE);
	m_trx = trx;
	//写开始回收大对象日志
	m_db->writePurgeBeginLog(m_session, trx->getTrxId(), trx->getTrxLastLsn());
	m_table->writePurgePhase1(m_session, trx->getTrxId(), trx->getTrxLastLsn(), m_db->getTransSys()->findMinReadViewInActiveTrxs());
	
	SYNCHERE(SP_LOB_PURGE_DELETE);
}

void RealPurgeLobThread::run() {
	m_conn = m_db->getNtseDb()->getConnection(false);
	m_conn->setTrx(true);
	m_session = m_db->getNtseDb()->getSessionManager()->allocSession("TNTTable_Insert_Lob", m_conn);
// 	TNTTransaction *purgeTrx = beginTransaction(OP_READ);
// 	purgeTrx->trxAssignReadView();
	//purge phase 1
//	m_table->purgePhase1(m_session, purgeTrx);
	//purge phase 2
//	m_table->purgePhase2(m_session, purgeTrx);
	m_db->purgeAndDumpTntim(PT_PURGEPHASE2, false, false, true);
//	commitTransaction(purgeTrx);
}