#include "TNTInsert.h"
#include "api/Table.h"
#include "TNTAccountTable.h"
#include "TNTLongCharTable.h"
#include "TNTCountTable.h"
#include "misc/RecordHelper.h"
#include "TNTTableHelper.h"
#include "TNTDbConfigs.h"
#include <sstream>
#include <iostream>

namespace tntperf {
/** TNT表记录插入 */
class TNTFLRInserter : public Thread {
public:
	TNTFLRInserter();
	virtual void init(uint tid, const u64 idList[], TestTable testTbl, const char *tableName, TNTDatabase *db);
protected:
	uint		m_tid;				/** 线程的TID */
	const u64	*m_recID;			/** 插入记录的ID列表 */
	const char  *m_tableName;
	TNTTable	*m_table;			/** 表 */
	TNTDatabase	*m_db;				/** 数据库 */
	TestTable	m_testTable;		/** 测试用表 */
	static const uint  BATCH_OP_SIZE = 1000;

	virtual void run();
};

//TNTFLRInserter::BATCH_OP_SIZE = 100;

/*** TNT表插入操作线程 ***/
TNTFLRInserter::TNTFLRInserter() : Thread("TNTFLRInserter Thread"){
	m_tid = 0;
	m_recID = NULL;
	m_tableName = NULL;
	m_table = NULL;
	m_db = NULL;
}

void TNTFLRInserter::run() {
	Record *rec = NULL;
	switch (m_testTable) {
		case COUNT_TABLE:
			rec = TNTAccountTable::createRecord(0, 0);
			break;
		case LONGCHAR_TABLE:
			rec = TNTLongCharTable::createRecord(0);
			break;
		default:
			assert(false);
	}
	RowId rid;
	uint dupIdx = (uint)(-1);

	TNTOpInfo opInfo;
	initTNTOpInfo(&opInfo, TL_X);
	Database *ntsedb = m_db->getNtseDb();
	Connection *conn = ntsedb->getConnection(false);
	Session *session = ntsedb->getSessionManager()->allocSession("TNTInserter::run", conn);
	TNTTransaction *trx = NULL;

	int i = 0;
	while (m_recID[i] != (u64)-1) {
		trx = startTrx(m_db->getTransSys(), session, true);
		m_table = m_db->openTable(session, m_tableName);
		while (m_recID[i] != (u64)-1) {
			switch (m_testTable) {
				case COUNT_TABLE:
					TNTCountTable::updateId(rec, m_recID[i]);
					break;
				case LONGCHAR_TABLE:
					TNTLongCharTable::updateId(rec, m_recID[i]);
					break;
				default:
					assert(false);
			}
			
			rid = m_table->insert(session, rec->m_data, &dupIdx, &opInfo);
			assert(dupIdx == (uint)-1);
			i++;
			//i++后不可能为0
			if (i % BATCH_OP_SIZE == 0) {
				break;
			}
		}
		m_db->closeTable(session, m_table);
		commitTrx(m_db->getTransSys(), trx);
	}

	ntsedb->getSessionManager()->freeSession(session);
	ntsedb->freeConnection(conn);
	freeRecord(rec);
}

/** 初始化一个TNTTable操作线程的参数 **/
void TNTFLRInserter::init(uint tid, const u64 idList[], TestTable testTbl, const char *tableName, TNTDatabase *db) {
	m_tid = tid;
	m_recID = idList;
	m_tableName = tableName;
	m_db = db;
	m_testTable = testTbl;
}

/*** 变长记录插入线程 ***/
class TNTVLRInserter : public TNTFLRInserter {
public:
	TNTVLRInserter(): TNTFLRInserter() {};
	virtual void init(uint tid, const u64 idList[], TestTable testTbl, const char *tableName, TNTDatabase *db, u64* dataSize) {
		((TNTFLRInserter*)this)->init(tid, idList, testTbl, tableName, db);
		m_dataSize = dataSize;
	}
protected:
	u64 *m_dataSize; /** 传出数据量 */
	virtual void run();
};


void TNTVLRInserter::run() {
	static const int count = 32;// 我们创建16个记录

	Record *recs[count]; 
	int recLen[count];
	assert(m_testTable == ACCOUNT_TABLE);
	for (int i = 0; i < count; ++i) {
		recs[i] = TNTAccountTable::createRecord(0, recLen + i, 150, 200);
	}
	*m_dataSize = 0;

	RowId rid;
	uint dupIdx = (uint)(-1);
	TNTOpInfo opInfo;
	initTNTOpInfo(&opInfo, TL_X);
	Database *db = m_db->getNtseDb();
	Connection *conn = db->getConnection(false);
	Session *session = db->getSessionManager()->allocSession("TNTVLRInserter::run", conn);
	TNTTransaction *trx = NULL;

	int i = 0;
	while (m_recID[i] != (u64)-1) {
		trx = startTrx(m_db->getTransSys(), session);
		m_table = m_db->openTable(session, m_tableName);
		while (m_recID[i] != (u64)-1) {
			TNTAccountTable::updateId(recs[i % count], m_recID[i]);
			rid = m_table->insert(session, recs[i % count]->m_data, &dupIdx, &opInfo);
			assert(dupIdx == (uint)-1);
			*m_dataSize += recLen[i % count];
			i++;
			//i++后不可能为0
			if (i % BATCH_OP_SIZE == 0) {
				break;
			}
		}
		m_db->closeTable(session, m_table);
		commitTrx(m_db->getTransSys(), trx);
	}
	db->getSessionManager()->freeSession(session);
	db->freeConnection(conn);

	for (int i = 0; i < count; ++i) {
		freeRecord(recs[i]);
	}
}

//TNT测试类
TNTInsertPerfTest::TNTInsertPerfTest(const char * tableName, Scale scale, uint threadCount, Order idOrder, double dataSizeFact)
{
	/* 根据表名判断表分类 */
	m_tableName = tableName;
	if (0 == strcmp(m_tableName, TNTTABLE_NAME_COUNT))
		m_testTable = COUNT_TABLE;
	else if (0 == strcmp(m_tableName, TNTTABLE_NAME_LONGCHAR))
		m_testTable = LONGCHAR_TABLE;
	else if (0 == strcmp(m_tableName, TNTTABLE_NAME_ACCOUNT))
		m_testTable = ACCOUNT_TABLE;
	else
		assert(false);
	m_scale = scale;
	m_threadCnt = threadCount;
	m_idOrder = idOrder;
	m_dataSizeFact = dataSizeFact;
	m_tntRatio = 0.7;
}

TNTInsertPerfTest::~TNTInsertPerfTest(void)
{
}

string TNTInsertPerfTest::getName() const {
	stringstream ss;
	ss << "TNT Insert Test: ";
	ss << m_threadCnt;
	ss << " thread";
	if (m_threadCnt > 1) ss <<"s";
	ss << " on table ";
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
	}

	switch (m_scale) {
		case SMALL:
			ss << "[Small] ";
			break;
		case MEDIUM:
			ss << "[Medium] ";
			break;
	}

	switch (m_idOrder) {
		case ASCENDANT:
			ss << "id order ascendant.";
			break;
		case DESCENDANT:
			ss << "id order descendant.";
			break;
		case RANDOM:
			ss << "id order random.";
			break;
	}
	return ss.str();
}

string TNTInsertPerfTest::getDescription() const {
	return "TNT Insert operation performance test.";
}

void TNTInsertPerfTest::run() {
	switch (m_testTable) {
		case COUNT_TABLE:
		case LONGCHAR_TABLE:
			{
				/* 创建插入线程 */
				u64 *idArray = new u64[m_recCnt + m_threadCnt];
				TNTFLRInserter *inserters = new TNTFLRInserter[m_threadCnt];

				for (uint i = 0; i < m_threadCnt; ++i) {
					/* 生成recID数据 */
					u64 *idList = idArray + i * (m_recCntPerThd + 1);
					/* 生成随机ID序列 */
					Generator::generateArray(idList, m_recCntPerThd, (u64)i, (u64)m_threadCnt, m_idOrder);
					/* 最后一个ID设置为最大值，作为终结的标志 */
					idList[m_recCntPerThd] = (u64)-1;
					/* 初始化操作线程 */
					inserters[i].init(i, idList, m_testTable, m_tableName, m_db);
				}

				/* 计时 */
				u64 startTime = System::currentTimeMillis();

				for (uint i = 0; i < m_threadCnt; ++i) {
					inserters[i].start();
				}
				for (uint i = 0; i < m_threadCnt; ++i) {
					inserters[i].join();
				}

				u64 endTime = System::currentTimeMillis();

				m_totalMillis = endTime - startTime;

				delete [] inserters;
				delete [] idArray;
			}
			break;
		case ACCOUNT_TABLE: //变长
			{
				u64 *idArray = new u64[m_recCnt + m_threadCnt];
				TNTVLRInserter *inserters = new TNTVLRInserter[m_threadCnt];
				u64 *dataSize = new u64[m_threadCnt];

				for (uint i = 0; i < m_threadCnt; ++i) {
					/* 生成recID数据 */
					u64 *idList = idArray + i * (m_recCntPerThd + 1);
					/* 随机ID */
					Generator::generateArray(idList, m_recCntPerThd, (u64)i, (u64)m_threadCnt, m_idOrder);
					/* ID岗哨 */
					idList[m_recCntPerThd] = (u64)-1;
					/* 初始化线程 */
					inserters[i].init(i, idList, m_testTable, m_tableName, m_db, &dataSize[i]);
				}
				/* 计时 */
				u64 startTime = System::currentTimeMillis();

				for (uint i = 0; i < m_threadCnt; ++i) {
					inserters[i].start();
				}
				for (uint i = 0; i < m_threadCnt; ++i) {
					inserters[i].join();
				}

				u64 endTime = System::currentTimeMillis();
				m_totalMillis = endTime - startTime;

				/* 变长堆的总数据量需要精确累加 */
				m_dataSize = 0;
				for (int i = 0; i < m_threadCnt; ++i) {
					m_dataSize += dataSize[i];
				}

				delete [] dataSize;
				delete [] inserters;
				delete [] idArray;
			}
			break;
	}
}

/** 本次测试的操作数 */
u64 TNTFLRInsertTest::getOpCnt() {
	return m_opCnt;
}
/** 本次测试的数据量 */
u64 TNTFLRInsertTest::getDataSize() {
	return m_dataSize;
}

/** 本次测试占用的运行时间 */
u64 TNTFLRInsertTest::getMillis() {
	return m_totalMillis;
}

/** 
 * 插入记录, 记录数据量为ntse页缓存的一半
 * @post 数据库状态为关闭
 */
void TNTInsertPerfTest::loadData(u64 *totalRecSize, u64 *recCnt) {
	Database *ntsedb = m_db->getNtseDb();
	// 创建Session
	Connection *conn = ntsedb->getConnection(false);
	Session *session = ntsedb->getSessionManager()->allocSession("TNTInsertPerfTest::loadData", conn);
	//插入首先考虑ntse buffer大小，因为tnt buffer只占用了hash index
	m_dataSize = (u64)(m_cfg->m_ntseConfig.m_pageBufSize * Limits::PAGE_SIZE * m_dataSizeFact);
	// 创建表并计算操作记录数
	createTableAndRecCnt(session);

	m_recCntPerThd = (uint) (m_recCnt / m_threadCnt);

	ntsedb->getSessionManager()->freeSession(session);
	ntsedb->freeConnection(conn);

	*recCnt = 0;
	*totalRecSize = 0;

	m_opCnt = m_recCntPerThd * m_threadCnt;
	m_dataSize = m_opCnt * getRecordSize();
}
/** 
 * 进行一遍表扫描
 * @post 数据库状态为open
 */
void TNTInsertPerfTest::warmUp() {
	if (m_dataSizeFact >= 1.0) {
		return;
	}

	/* 如果数据量小于buffer size，则认为做的是cpu测试，预热数据 */
	Record *rec = NULL;
	switch (m_testTable) {
		case COUNT_TABLE:
			rec = TNTCountTable::createRecord(0, 0);
			rec->m_format = REC_FIXLEN;
			break;
	}

	Database *ntsedb = m_db->getNtseDb();
	Connection *conn = ntsedb->getConnection(false);
	Session *session = ntsedb->getSessionManager()->allocSession("TNTInsertPerfTest::warmup", conn);
	TNTTransaction *trx = startTrx(m_db->getTransSys(), session);
	m_table = m_db->openTable(session, m_tableName);

	vector<RowId> ridVec;
	RowId rid;
	/*TNTOpInfo opInfo;
	initTNTOpInfo(&opInfo, TL_X);*/
	// 插入
	for (u64 i = 0; i < m_recCntPerThd * m_threadCnt; ++i) {
		rid = m_table->getNtseTable()->getHeap()->insert(session, rec, NULL);
		ridVec.push_back(rid);
	}
	// 删除
	for (u64 i = 0; i < ridVec.size(); ++i) {
		bool success = m_table->getNtseTable()->getHeap()->del(session, ridVec[i]);
		assert(success);
	}
	m_db->closeTable(session, m_table);
	commitTrx(m_db->getTransSys(), trx);
	ntsedb->getSessionManager()->freeSession(session);
	ntsedb->freeConnection(conn);
	ridVec.clear();
}

void TNTInsertPerfTest::setUp() {
	m_cfg = getTNTConfig();
	if (m_cfg->m_ntseConfig.m_maxSessions < m_threadCnt) {
		m_cfg->m_ntseConfig.m_maxSessions = (u16)m_threadCnt;
	}
	ntse::File dir(m_cfg->m_ntseConfig.m_basedir);
	dir.rmdir(true);
	dir.mkdir();
	m_db = TNTDatabase::open(m_cfg, true, 0);
}

void TNTInsertPerfTest::tearDown() {
	m_db->close();
	delete m_db;
	m_db = NULL;
	ntse::File dir(m_cfg->m_ntseConfig.m_basedir);
	dir.rmdir(true);

	delete m_cfg;
}

/** 根据参数获取不同大小的运行配置 **/
TNTConfig *TNTInsertPerfTest::getTNTConfig() {
	TNTConfig *ret = NULL;
	switch (m_scale) {
		case SMALL:
			ret = new TNTConfig(TNTCommonDbConfig::getSmall());
			ret->m_tntBufSize = 64*1024*1024/TNTIMPage::TNTIM_PAGE_SIZE;
			ret->m_ntseConfig.m_pageBufSize = 2*ret->m_tntBufSize;
			ret->m_ntseConfig.m_commonPoolSize = 128*1024*1024/TNTIMPage::TNTIM_PAGE_SIZE;
			break;
		case MEDIUM:
			ret = new TNTConfig(TNTCommonDbConfig::getMedium());
			ret->m_tntBufSize = 512*1024*1024/TNTIMPage::TNTIM_PAGE_SIZE;
			ret->m_ntseConfig.m_pageBufSize = 2*ret->m_tntBufSize;
			ret->m_ntseConfig.m_commonPoolSize = 512*1024*1024/TNTIMPage::TNTIM_PAGE_SIZE;
			break;
	}

	if (ret != NULL) {
		ret->m_purgeInterval = 10*60*60;
		ret->m_purgeThreshold = 99;
		ret->m_dumpInterval = 10*60*60;
		ret->m_dumponRedoSize = 1024*1024*1024;
		ret->m_ntseConfig.m_logLevel = EL_LOG;
	}

	return ret;
}


/**
 * 创建表并且根据数据量算出记录数
 */
void TNTInsertPerfTest::createTableAndRecCnt(Session *session) {
	TNTTransaction *trx = startTrx(m_db->getTransSys(), session);
	switch (m_testTable) {
		case COUNT_TABLE:
			m_db->createTable(session, TNTTABLE_NAME_COUNT, new TableDef(TNTCountTable::getTableDef(false)));
			m_recCnt = m_dataSize / TNTCountTable::getRecordSize();
			break;
		case LONGCHAR_TABLE:
			m_db->createTable(session, TNTTABLE_NAME_LONGCHAR, new TableDef(TNTLongCharTable::getTableDef(false)));
			m_recCnt = m_dataSize / TNTLongCharTable::getRecordSize();
			break;
		case ACCOUNT_TABLE:
			m_db->createTable(session, TNTTABLE_NAME_ACCOUNT, new TableDef(TNTAccountTable::getTableDef(false)));
			m_recCnt = m_dataSize / 200; // 平均200
			break;
	}
	commitTrx(m_db->getTransSys(), trx);
}

/**
 * 获得平均记录长度
 */
uint TNTInsertPerfTest::getRecordSize() {
	switch (m_testTable) {
		case COUNT_TABLE:
			return TNTCountTable::getRecordSize();
		case LONGCHAR_TABLE:
			return TNTLongCharTable::getRecordSize();
		default:
			assert(ACCOUNT_TABLE == m_testTable);
			return 200;
	}
}

}