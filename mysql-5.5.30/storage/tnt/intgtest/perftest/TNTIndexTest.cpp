#include "TNTIndexTest.h"

#include "TNTDbConfigs.h"
#include "api/TNTTable.h"
#include "api/TNTDatabase.h"
#include "api/Table.h"
#include "misc/Session.h"
#include "misc/RecordHelper.h"
#include "mms/Mms.h"
#include "TNTAccountTable.h"
#include "TNTLongCharTable.h"
#include "TNTCountTable.h"
#include "TNTBlogTable.h"
#include "TNTTableHelper.h"
#include <sstream>

namespace tntperf{
uint TNTIndexTest::BATCH_OP_SIZE = 1000;

//u32 TNTIndexScanTest::VOLUMN_RATIO = 100;

/** 
 * 构造函数
 *
 * @param useMms 使用MMS
 * @param recInMms 记录是否在MMS中
 * @param isVar 是否为变长表测试
 */
TNTIndexTest::TNTIndexTest(const char * tableName, Scale scale, bool useMms, bool recInMms, bool isVar, enum TNT_INDEX_TESTCASE testcase)
{
 	m_useMms = useMms;
	m_recInMms = recInMms;
 	m_isVar = isVar;
	
	/* 根据表名判断表分类 */
	m_tableName = tableName;
	if (0 == strcmp(m_tableName, TNTTABLE_NAME_COUNT)) {
		m_testTable = COUNT_TABLE;
		m_ratio = 0.7;
	} else if (0 == strcmp(m_tableName, TNTTABLE_NAME_LONGCHAR)) {
		m_testTable = LONGCHAR_TABLE;
		m_ratio = 0.7;
	} else if (0 == strcmp(m_tableName, TNTTABLE_NAME_ACCOUNT)) {
		m_testTable = ACCOUNT_TABLE;
		m_ratio = 0.7;
	} else if (0 == strcmp(m_tableName, TNTTABLE_NAME_BLOG)) {
		m_testTable = BLOG_TABLE;
		m_ratio = 0.7;
	} else {
		assert(false);
	}
	m_scale = scale;
	m_enableCache = false;
	m_recCnt = 0;
	m_dataSize = 0;
	m_totalMillis = 0;
	
	//是否回表
	m_backTable = false;

	//是否用ntse接口
	m_ntseTest = true;

	//指定测试类型
	m_testcase = testcase;
}

/** 
 * 获取测试用例名 
 *
 * @return 测试用例名
 */	    
string TNTIndexTest::getName() const
{
	stringstream ss;
	ss << "TNTIndexTest(";
	if (m_useMms)
		ss << "useMMs,";
	if (m_recInMms)
		ss <<"recInMms,";
	
	if (m_isVar)
		ss <<"varTable";
    
	ss << ")";

	switch (m_testTable) {
		case COUNT_TABLE:
			ss << "TNTCount ";
			break;
		case LONGCHAR_TABLE:
			ss << "TNTLongChar ";
			break;
		case ACCOUNT_TABLE:
			ss << "TNTAccount ";
			break;
		case BLOG_TABLE:
			ss << "TNTBlog";
	}

	switch (m_scale) {
		case SMALL:
			ss << "[Small] ";
			break;
		case MEDIUM:
			ss << "[Medium] ";
			break;
		case BIG:
			ss << "[Big] ";
			break;
	}

	if (m_useMms)
		ss << "with mms";

	switch (m_testcase) {
		case PURGE_TABLE:
			ss << "op: purge table ";
			break;
		case BEGIN_END_SCAN:
			ss << "op: begin end table ";
			break;
		case UNIQUE_SCAN:
			ss << "op: unique scan ";
			break;
		case RANGE_SCAN:
			ss << "op: range scan ";
			break;
		case TABLE_SCAN:
			ss << "op: table scan ";
			break;
	}


	return ss.str();
}

/** 
 * 获取测试用例描述信息
 *
 * @return 测试用例描述信息
 */
string TNTIndexTest::getDescription() const
{
	return "TNTIndexScan in Memory Performance";
}

/**
 * 测试数据缓存名(默认是测试用例名)
 *	相同表名的测试有相同的测试数据
 *
 * @return 缓存名
 */
string TNTIndexTest::getCachedName() const	
{
    return string("TNTIndexScanTest-BLOG") ;
}

/** 
 * 加载测试数据
 *
 * @param totalRecSize [out] 测试数据量
 * @param recCnt [out] 测试数据记录数
 * @post 保持数据库为关闭状态
 */
void TNTIndexTest::loadData(u64 *totalRecSize, u64 *recCnt) {
	Database *ntsedb = m_db->getNtseDb();
	Connection *conn = m_db->getNtseDb()->getConnection(false);
	Session *session = m_db->getNtseDb()->getSessionManager()->allocSession("TNTTableScanTest::loadData", conn);
	
	TableDef *tblDef = NULL;
	if (m_testTable == COUNT_TABLE) {
		tblDef = new TableDef(TNTCountTable::getTableDef(m_useMms));
	} else if (m_testTable == LONGCHAR_TABLE) {
		tblDef = new TableDef(TNTLongCharTable::getTableDef(m_useMms));
	} else if (m_testTable == ACCOUNT_TABLE) {
		tblDef = new TableDef(TNTAccountTable::getTableDef(m_useMms));
	} else if (m_testTable == BLOG_TABLE){
		tblDef = new TableDef(TNTBlogTable::getTableDef(m_useMms));
	} else {
		assert(false);
	}

	TNTTransaction *trx = startTrx(m_db->getTransSys(), session);
	m_db->createTable(session, m_tableName, tblDef);
	commitTrx(m_db->getTransSys(), trx);
	ntsedb->getSessionManager()->freeSession(session);
	ntsedb->freeConnection(conn);

	TNTOpInfo opInfo;
	int recSize = 0;
	//记录大小与内存堆真正存放数据与hash索引值的差值
	uint deltaSize = sizeof(HashIndexEntry) + sizeof(MHeapRec) - sizeof(byte *) - sizeof(uint);
	uint dupIdx = (uint)(-1);
	uint totalRecBufSize = (uint)(m_config->m_tntBufSize*TNTIMPage::TNTIM_PAGE_SIZE*m_ratio);
	while (m_dataSize < totalRecBufSize) {
		conn = ntsedb->getConnection(false);
		session = ntsedb->getSessionManager()->allocSession("TNTIndexPerfTest::loadData", conn);

		initTNTOpInfo(&opInfo, TL_X);
		trx = startTrx(m_db->getTransSys(), session);
		m_table = m_db->openTable(session, m_tableName);
	
		u64 i = 0;
		RowId rid = INVALID_ROW_ID;
		u64 sp1 = 0;
		while (m_dataSize < totalRecBufSize && i < BATCH_OP_SIZE) {
			i++;
			sp1 = session->getMemoryContext()->setSavepoint();
			Record *rec = NULL;
			if (m_testTable == COUNT_TABLE) {
				rec = TNTCountTable::createRecord(m_recCnt + i,  (int)((i + m_recCnt) % 20));
				recSize = TNTCountTable::getRecordSize();
			} else if (m_testTable == LONGCHAR_TABLE) {
				rec = TNTLongCharTable::createRecord(m_recCnt + i);
				recSize = TNTLongCharTable::getRecordSize();
			} else if (m_testTable == ACCOUNT_TABLE) {
				rec = TNTAccountTable::createRecord(m_recCnt + i, &recSize);
			} else if (m_testTable == BLOG_TABLE) {
				rec = TNTBlogTable::createRecord(m_recCnt + i);
			} else {
				assert(false);
			}

			recSize += deltaSize;
			try {
				rid = m_table->insert(session, rec->m_data, &dupIdx, &opInfo);
			} catch (NtseException &) {
				assert(false);
			}
			assert(dupIdx == (uint)(-1));
			m_dataSize += recSize;
			freeRecord(rec);
			session->getMemoryContext()->resetToSavepoint(sp1);
		}
		m_db->closeTable(session, m_table);
		commitTrx(m_db->getTransSys(), trx);
		m_recCnt += i;
		ntsedb->getSessionManager()->freeSession(session);
		ntsedb->freeConnection(conn);
	}

	*totalRecSize = m_dataSize;
	*recCnt = m_recCnt;
}
void TNTIndexTest::warmUp()
{
	Database *ntsedb = m_db->getNtseDb();
	Connection *conn = ntsedb->getConnection(false);
	Session *session = ntsedb->getSessionManager()->allocSession("TNTPurgePerfTest::warmup", conn);
	TNTTransaction *trx = startTrx(m_db->getTransSys(), session);
	bool needUpdate = false;
	m_table = m_db->openTable(session, m_tableName);
	
	//是否要更新
	if(needUpdate) {
		TNTOpInfo opInfo;
		initTNTOpInfo(&opInfo, TL_X);
		TNTTransaction *trx = startTrx(m_db->getTransSys(), session);
		TableDef *tblDef = m_table->getNtseTable()->getTableDef();
		MemoryContext *ctx = session->getMemoryContext();
		u16 upColNum = 0;
		u16 updateCols[4] = {0, 1, 2, 6};

		if (m_testTable == COUNT_TABLE || m_testTable == LONGCHAR_TABLE || m_testTable == ACCOUNT_TABLE) {
			upColNum = 1;
		} else {
			upColNum = 4;
		}

		updateTable(m_db, m_table);


	} else { //把索引数据读到内存中
		
		u16 columns[1] = {0};
		u16 colNum = 1;

//		u64 scaned = ::TNTIndex0Scan(m_db, m_table, 1, columns);
		
		if(m_testTable == BLOG_TABLE) {
			columns[0] = 6 ;
//			::TNTIndex0Scan(m_db, m_table, 1, columns);
			columns[0] = 1;
			columns[1] = 2;
//			::TNTIndex0Scan(m_db, m_table, 2, columns);
		}
	}

	commitTrx(m_db->getTransSys(), trx);
	ntsedb->getSessionManager()->freeSession(session);
	ntsedb->freeConnection(conn);
}
void TNTIndexTest::run()
{
// 	cout << "1. Purge Table " << endl;
// 	cout << "2. Begin IdxScan End IdxScan" << endl;
// 	cout << "3. Unique IdxScan" << endl;
// 	cout << "4. Range IdxScan" << endl;

	//cin >> op;

	//purge数据
	if(m_testcase == PURGE_TABLE)
		purgeTable(m_db, m_table);
	//重复的beginScan和EndScan
	else if(m_testcase == BEGIN_END_SCAN)
		beginAndEndIdxScan(m_db, m_table);
	else if(m_testcase == UNIQUE_SCAN)
		uniqueIdxScan(m_db, m_table);
	else if(m_testcase == RANGE_SCAN)
		rangeIdxScan(m_db, m_table);
	else if(m_testcase == TABLE_SCAN)
		tableRangeScan(m_db, m_table);
}
void TNTIndexTest::setUp()
{
	switch (m_scale) {
		case SMALL:
			setConfig(new TNTConfig(TNTCommonDbConfig::getSmall()));
			m_config->m_tntBufSize = 128*1024*1024/TNTIMPage::TNTIM_PAGE_SIZE;
			//m_config->m_tntBufSize = 32*1024*1024/TNTIMPage::TNTIM_PAGE_SIZE;
			m_config->m_ntseConfig.m_pageBufSize = 2*m_config->m_tntBufSize;
			m_config->m_ntseConfig.m_commonPoolSize = 128*1024*1024/TNTIMPage::TNTIM_PAGE_SIZE;
			break;
		case MEDIUM:
			setConfig(new TNTConfig(TNTCommonDbConfig::getMedium()));
			m_config->m_tntBufSize = 512*1024*1024/TNTIMPage::TNTIM_PAGE_SIZE;
			m_config->m_ntseConfig.m_pageBufSize = 2*m_config->m_tntBufSize;
			m_config->m_ntseConfig.m_commonPoolSize = 512*1024*1024/TNTIMPage::TNTIM_PAGE_SIZE;
			break;
	}
	
	m_config->m_purgeInterval = 10*60*60;
	m_config->m_purgeThreshold = 99;
	m_config->m_dumpInterval = 10*60*60;
	m_config->m_dumponRedoSize = 1024*1024*1024;
	m_config->m_ntseConfig.m_logLevel = EL_LOG;

	ntse::File dir(m_config->m_ntseConfig.m_basedir);
	dir.rmdir(true);
	dir.mkdir();
	m_db = TNTDatabase::open(m_config, true, 0);
}

void TNTIndexTest::tearDown() {
	m_db->close();
	delete m_db;
	ntse::File dir(m_config->m_ntseConfig.m_basedir);
	dir.rmdir(true);

	delete m_config;
}

//测试purge
void TNTIndexTest::purgeTable(TNTDatabase *db, TNTTable *table) {
	//建立连接
	Connection *conn = db->getNtseDb()->getConnection(false);
	Session *session = db->getNtseDb()->getSessionManager()->allocSession("TNTTable_Insert_Lob", conn);	
	//开启事务
	TNTTransaction *trx = startTrx(m_db->getTransSys(), session, true);
	trx->trxAssignReadView();
	
	//Purge指定表记录
	table->purgePhase1(session, trx);
	table->purgePhase2(session, trx);
	
	//提交事务
	commitTrx(m_db->getTransSys(),trx);

}

void TNTIndexTest::beginAndEndIdxScan(TNTDatabase *db, TNTTable *table) {
	int loop = 10000000;
	
	//建立连接
	Connection *conn = db->getNtseDb()->getConnection(false);
	Session *session = db->getNtseDb()->getSessionManager()->allocSession("TNTTable_Insert_Lob", conn);	
	
	TNTOpInfo opInfo;
	opInfo.m_selLockType = TL_X;
	opInfo.m_sqlStatStart= false;
	opInfo.m_mysqlOper	 = false;
	
	int idx = 0;
	
	u16 columns[1] = {0};
	IndexScanCond cond (idx, NULL, true, false, false);

	if(m_ntseTest == false) {
		for(int i = 0; i < loop ; i++) {
			McSavepoint sp(session->getMemoryContext());
			TNTTblScan *scan = table->indexScan(session, OP_WRITE, &opInfo, &cond, 1, columns);
			table->endScan(scan);
		}
	} else {
		for(int i = 0; i < loop ; i++) {
			McSavepoint sp(session->getMemoryContext());
			TblScan *scan = table->getNtseTable()->indexScan(session, OP_WRITE, &cond, 1, columns);
			table->getNtseTable()->endScan(scan);
		}

	}
	
}

void TNTIndexTest::uniqueIdxScan(TNTDatabase *db, TNTTable *table) {
	int loop = 1000;
	//建立连接
	Connection *conn = db->getNtseDb()->getConnection(false);
	Session *session = db->getNtseDb()->getSessionManager()->allocSession("TNTTable_Insert_Lob", conn);	

	int idx = 0;

	u16 columns[1] = {0};
	TableDef *tableDef = table->getNtseTable()->getTableDef();

	SubRecordBuilder srb(tableDef, REC_REDUNDANT); 
	SubRecord * subRec;
	if(m_backTable)
		subRec = srb.createEmptySbById(tableDef->m_maxRecSize, "0 1 2 3 4");
	else
		subRec = srb.createEmptySbById(tableDef->m_maxRecSize, "0");
	SubRecordBuilder srb2(tableDef, KEY_PAD); 

	srand((unsigned)time( NULL ) );
	if(!m_ntseTest) {
		//开启事务
		TNTTransaction *trx = startTrx(m_db->getTransSys(), session, true);
		trx->trxAssignReadView();
		TNTOpInfo opInfo;
		opInfo.m_selLockType = TL_X;
		opInfo.m_sqlStatStart= false;
		opInfo.m_mysqlOper	 = false;

		for(int i = 0; i < loop ; i++) {
			McSavepoint sp(session->getMemoryContext());
			int blogId = rand() % m_recCnt;
			SubRecord *searchKey = srb2.createSubRecordById("0", &blogId);
			IndexScanCond cond (idx, searchKey, true, false, false);	
			TNTTblScan *scan = table->indexScan(session, OP_WRITE, &opInfo, &cond, 1, columns, true);
			table->getNext(scan, subRec->m_data);
			table->endScan(scan);
			freeSubRecord(searchKey);
		}
		commitTrx(m_db->getTransSys(),trx);
	} else {
		for(int i = 0; i < loop; i++) {
			McSavepoint sp (session->getMemoryContext());
			int blogId = rand() % m_recCnt;
			SubRecord *searchKey = srb2.createSubRecordById("0", &blogId);
			searchKey->m_rowId = INVALID_ROW_ID;
			IndexScanCond cond (idx, searchKey, true, false, false);	
			TblScan *scan = table->getNtseTable()->indexScan(session, OP_WRITE, &cond, 1, columns);
			table->getNtseTable()->getNext(scan, subRec->m_data);
			table->getNtseTable()->endScan(scan);
			freeSubRecord(searchKey);
		}
	}
		
	freeSubRecord(subRec);
}

void TNTIndexTest::rangeIdxScan(TNTDatabase *db, TNTTable *table) {
	//建立连接
	Connection *conn = db->getNtseDb()->getConnection(false);
	Session *session = db->getNtseDb()->getSessionManager()->allocSession("TNTTable_Insert_Lob", conn);	
	
	TableDef *tableDef = table->getNtseTable()->getTableDef();
	int idx = 1;

	u16 columns[1] = {6};

	uint redRows = 0;
	SubRecordBuilder srb(tableDef, REC_REDUNDANT); 
	SubRecord * subRec = srb.createEmptySbById(tableDef->m_maxRecSize, "6");
	IndexScanCond cond (idx, NULL, true, false, false);
	
	if(!m_ntseTest) {
		//开启事务
		TNTTransaction *trx = startTrx(m_db->getTransSys(), session, true);
		trx->trxAssignReadView();

		TNTOpInfo opInfo;
		opInfo.m_selLockType = TL_S;
		opInfo.m_sqlStatStart= false;
		opInfo.m_mysqlOper	 = false;

		TNTTblScan *scan = table->indexScan(session, OP_READ, &opInfo, &cond, 1, columns, false);
		while(table->getNext(scan, subRec->m_data)){
			redRows++;
		}		
		table->endScan(scan);
		commitTrx(m_db->getTransSys(),trx);
	} else {
		TblScan *scan = table->getNtseTable()->indexScan(session, OP_READ, &cond, 1, columns);
		while(table->getNtseTable()->getNext(scan, subRec->m_data)){
			redRows++;
		}
		table->getNtseTable()->endScan(scan);
	}
		
	freeSubRecord(subRec);
}

void TNTIndexTest::tableRangeScan(TNTDatabase *db, TNTTable *table) {
	//建立连接
	Connection *conn = db->getNtseDb()->getConnection(false);
	Session *session = db->getNtseDb()->getSessionManager()->allocSession("TNTTable_Insert_Lob", conn);	
	u16 columns[1] = {6};
	uint redRows = 0;
	TableDef *tableDef = table->getNtseTable()->getTableDef();
	SubRecordBuilder srb(tableDef, REC_REDUNDANT); 
	SubRecord * subRec = srb.createEmptySbById(tableDef->m_maxRecSize, "6");
	if(!m_ntseTest) {
		//开启事务
		TNTTransaction *trx = startTrx(m_db->getTransSys(), session, true);
		trx->trxAssignReadView();

		TNTOpInfo opInfo;
		opInfo.m_selLockType = TL_S;
		opInfo.m_sqlStatStart= false;
		opInfo.m_mysqlOper	 = false;

		TNTTblScan *scan = table->tableScan(session, OP_READ, &opInfo, 1, columns);
		while (table->getNext(scan, subRec->m_data)) {
			redRows++;
		}
		table->endScan(scan);
	} else {
		TblScan *scan = table->getNtseTable()->tableScan(session, OP_READ, 1, columns);
		while(table->getNtseTable()->getNext(scan, subRec->m_data)) {
			redRows++;
		}
		table->getNtseTable()->endScan(scan);
	}
	freeSubRecord(subRec);
}


void TNTIndexTest::insertRecord(TNTDatabase *db, TNTTable *table, u64 count) {
	Connection *conn = db->getNtseDb()->getConnection(false);
	Session *session = db->getNtseDb()->getSessionManager()->allocSession("TNTTableScanTest::loadData", conn);
	u64 rowsInsert = count;
	TNTOpInfo opInfo;
	::initTNTOpInfo(&opInfo, TL_X);
	//插入
	TNTTransaction *trx = startTrx(db->getTransSys(), session);
	m_recCntLoaded = TNTBlogTable::populate(session, table, &opInfo, &rowsInsert);
	commitTrx(db->getTransSys(), trx);

	NTSE_ASSERT(count == rowsInsert);
	db->getNtseDb()->getSessionManager()->freeSession(session);
	db->getNtseDb()->freeConnection(conn);
}


void TNTIndexTest::updateTable(TNTDatabase *db, TNTTable *table) {
	Connection *conn = db->getNtseDb()->getConnection(false);
	Session *session = db->getNtseDb()->getSessionManager()->allocSession("TNTTableScanTest::loadData", conn);
	TNTTransaction *trx = startTrx(db->getTransSys(), session);

	TableDef *tableDef = table->getNtseTable()->getTableDef();
	SubRecordBuilder srb(tableDef, REC_REDUNDANT);

	SubRecord *subRec = srb.createEmptySbById(tableDef->m_maxRecSize, "0");
	
	TNTTblScan *scanHandle;
	TNTOpInfo opInfo;
	::initTNTOpInfo(&opInfo, TL_X);
	
	scanHandle = table->tableScan(session, OP_UPDATE, &opInfo, subRec->m_numCols, subRec->m_columns );


	u16 updateColumns[4] = {0, 1, 6};
	u16 upColNum = 0;
	if(m_testTable == BLOG_TABLE)
		upColNum = 3;
	else 
		upColNum = 1;
	scanHandle->setUpdateSubRecord(upColNum, updateColumns);
	u64 spAfterScan = session->getMemoryContext()->setSavepoint();


	memset(subRec->m_data, 0, tableDef->m_maxRecSize);
	uint gotRows = 0;
	while(table->getNext(scanHandle, subRec->m_data)) {
		//如果是Blog表，更新三个索引属性
		if(m_testTable == BLOG_TABLE) {
			size_t newPermalinkSize = System::random() % 100;
			char *permalink = randomStr(newPermalinkSize);
			
			int blogId = gotRows + 1000000;
			int blogUserId = gotRows + 1000000;

			SubRecord *updateRec = srb.createSubRecordByName("ID UserID Permalink", &blogId, &blogUserId, permalink);

			table->updateCurrent(scanHandle, updateRec->m_data);

			delete[] permalink;
			freeSubRecord(updateRec);
			memset(subRec->m_data, 0, tableDef->m_maxRecSize);
		} else {
			int pk = gotRows + 1000000;
			SubRecord *updateRec = srb.createSubRecordById("0", &pk);
			table->updateCurrent(scanHandle, updateRec->m_data);
			freeSubRecord(updateRec);
			memset(subRec->m_data, 0, tableDef->m_maxRecSize);
		}

		gotRows++;
	}
	freeSubRecord(subRec);
	table->endScan(scanHandle);
	commitTrx(db->getTransSys(), trx);
	cout << "update Row:" << gotRows << endl;
}

/**
 * 生成指定长度的随机字符串
 *
 * @return 字符串，使用new分配内存
 */
char* TNTIndexTest::randomStr(size_t size) {
	char *s = new char[size + 1];
	for (size_t i = 0; i < size; i++)
		s[i] = (char )('A' + System::random() % 26);
	s[size] = '\0';
	return s;
}


u64 TNTIndexTest::getMillis() {
	return -1;
}

}