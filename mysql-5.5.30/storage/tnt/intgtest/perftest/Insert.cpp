#include "Insert.h"
#include "DbConfigs.h"
#include "api/Table.h"
#include "api/Database.h"
#include "misc/Session.h"
#include "misc/RecordHelper.h"
#include "CountTable.h"
#include "LongCharTable.h"
#include "AccountTable.h"
#include "util/Thread.h"
#include <sstream>
#include <iostream>
#include "Generator.h"

using namespace ntseperf;
using namespace std;

/** 定长记录插入 */
class FLRInserter : public Thread {
public:
	FLRInserter();
	virtual void init(uint tid, const u64 idList[], TestTable testTbl, Table *table, Database *db);
protected:
	uint		m_tid;				/** 线程的TID */
	const u64	*m_recID;			/** 插入记录的ID列表 */
	Table		*m_table;			/** 表 */
	Database	*m_db;				/** 数据库 */
	TestTable	m_testTable;		/** 测试用表 */
	virtual void run();
};

/*** 定长堆插入操作线程 ***/
FLRInserter::FLRInserter() : Thread("FLRInserter Thread"){
	m_tid = 0;
	m_recID = NULL;
	m_table = NULL;
	m_db = NULL;
}

void FLRInserter::run() {
	Record *rec = NULL;
	switch (m_testTable) {
		case COUNT_TABLE:
			rec = CountTable::createRecord(0, 0);
			break;
		case LONGCHAR_TABLE:
			rec = LongCharTable::createRecord(0);
			break;
		default:
			assert(false);
	}
	RowId rid;
	uint dupIdx;

	Connection *conn = m_db->getConnection(false);
	Session *session = NULL;

	int i = 0;
	while (m_recID[i] != (u64)-1) {
		switch (m_testTable) {
			case COUNT_TABLE:
				CountTable::updateId(rec, m_recID[i]);
				break;
			case LONGCHAR_TABLE:
				LongCharTable::updateId(rec, m_recID[i]);
				break;
			default:
				assert(false);
		}
		session = m_db->getSessionManager()->allocSession("FLRInserter::run", conn);
		rid = m_table->insert(session, rec->m_data, &dupIdx);
		m_db->getSessionManager()->freeSession(session);
		++i;
	}
	m_db->freeConnection(conn);
	freeRecord(rec);
}

/** 初始化一个定长堆操作线程的参数 **/
void FLRInserter::init(uint tid, const u64 idList[], TestTable testTbl, Table *table, Database *db) {
	m_tid = tid;
	m_recID = idList;
	m_table = table;
	m_db = db;
	m_testTable = testTbl;
}


/*** 变长记录插入线程 ***/
class VLRInserter : public FLRInserter {
public:
	VLRInserter():FLRInserter() {};
	virtual void init(uint tid, const u64 idList[], TestTable testTbl, Table *table, Database *db, u64* dataSize) {
		((FLRInserter*)this)->init(tid, idList, testTbl, table, db);
		m_dataSize = dataSize;
	}
protected:
	u64 *m_dataSize; /** 传出数据量 */
	virtual void run();
};


void VLRInserter::run() {
	static const int count = 32;// 我们创建16个记录

	Record *recs[count]; 
	int recLen[count];
	assert(m_testTable == ACCOUNT_TABLE);
	for (int i = 0; i < count; ++i) {
		recs[i] = AccountTable::createRecord(0, recLen + i, 150, 200);
	}
	*m_dataSize = 0;
	
	RowId rid;
	uint dupIdx;
	Connection *conn = m_db->getConnection(false);
	Session *session = NULL;

	int i = 0;
	while (m_recID[i] != (u64)-1) {
		AccountTable::updateId(recs[i % count], m_recID[i]);

		session = m_db->getSessionManager()->allocSession("FLRInserter::run", conn);
		rid = m_table->insert(session, recs[i % count]->m_data, &dupIdx);
		m_db->getSessionManager()->freeSession(session);

		*m_dataSize += recLen[i % count];
		++i;
	}

	m_db->freeConnection(conn);

	for (int i = 0; i < count; ++i) {
		freeRecord(recs[i]);
	}
}




/*** 集成插入测试操作的框架 ***/
InsertPerfTest::InsertPerfTest(const char * tableName, HeapVersion ver, Scale scale, uint threadCount, Order idOrder, double dataSizeFact) {
	/* 根据表名判断表分类 */
	m_tableName = tableName;
	if (0 == strcmp(m_tableName, TABLE_NAME_COUNT))
		m_testTable = COUNT_TABLE;
	else if (0 == strcmp(m_tableName, TABLE_NAME_LONGCHAR))
		m_testTable = LONGCHAR_TABLE;
	else if (0 == strcmp(m_tableName, TABLE_NAME_ACCOUNT))
		m_testTable = ACCOUNT_TABLE;
	else
		assert(false);
	m_scale = scale;
	m_heapVer = ver;
	m_threadCnt = threadCount;
	m_idOrder = idOrder;
	m_dataSizeFact = dataSizeFact;
}

/** 返回测试用例的名称 **/
string InsertPerfTest::getName() const {
	stringstream ss;
	ss << "Insert Test: ";
	ss << m_threadCnt;
	ss << " thread";
	if (m_threadCnt > 1) ss <<"s";
	ss << " on table ";
	switch (m_testTable) {
		case COUNT_TABLE:
			ss << "Count ";
			break;
		case LONGCHAR_TABLE:
			ss << "LongChar ";
			break;
		case ACCOUNT_TABLE:
			ss << "Account ";
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

string InsertPerfTest::getDescription() const {
	return "Insert operation performance test.";
}

void InsertPerfTest::run() {
	switch (m_testTable) {
		case COUNT_TABLE:
		case LONGCHAR_TABLE:
			{
				/* 创建插入线程 */
				u64 *idArray = new u64[m_recCnt + m_threadCnt];
				FLRInserter *inserters = new FLRInserter[m_threadCnt];

				for (uint i = 0; i < m_threadCnt; ++i) {
					/* 生成recID数据 */
					u64 *idList = idArray + i * (m_recCntPerThd + 1);
					/* 生成随机ID序列 */
					Generator::generateArray(idList, m_recCntPerThd, (u64)i, (u64)m_threadCnt, m_idOrder);
					/* 最后一个ID设置为最大值，作为终结的标志 */
					idList[m_recCntPerThd] = (u64)-1;
					/* 初始化操作线程 */
					inserters[i].init(i, idList, m_testTable, m_currTab, m_db);
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
		case ACCOUNT_TABLE:
			{
				u64 *idArray = new u64[m_recCnt + m_threadCnt];
				VLRInserter *inserters = new VLRInserter[m_threadCnt];
				u64 *dataSize = new u64[m_threadCnt];

				for (uint i = 0; i < m_threadCnt; ++i) {
					/* 生成recID数据 */
					u64 *idList = idArray + i * (m_recCntPerThd + 1);
					/* 随机ID */
					Generator::generateArray(idList, m_recCntPerThd, (u64)i, (u64)m_threadCnt, m_idOrder);
					/* ID岗哨 */
					idList[m_recCntPerThd] = (u64)-1;
					/* 初始化线程 */
					inserters[i].init(i, idList, m_testTable, m_currTab, m_db, &dataSize[i]);
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
u64 FLRInsertTest::getOpCnt() {
	return m_opCnt;
}
/** 本次测试的数据量 */
u64 FLRInsertTest::getDataSize() {
	return m_dataSize;
}

/** 本次测试占用的运行时间 */
u64 FLRInsertTest::getMillis() {
	return m_totalMillis;
}

/** 
 * 插入记录, 记录数据量为ntse页缓存的一半
 * @post 数据库状态为关闭
 */
void InsertPerfTest::loadData(u64 *totalRecSize, u64 *recCnt) {
	// 创建数据库
	m_cfg = getConfig();
	if (m_cfg->m_maxSessions < m_threadCnt) m_cfg->m_maxSessions = m_threadCnt;
	Database::drop(m_cfg->m_basedir);
	m_db = Database::open(m_cfg, true);
	// 创建Session
	Connection *conn = m_db->getConnection(false);
	Session *session = m_db->getSessionManager()->allocSession("InsertPerfTest::loadData", conn);
	m_dataSize = m_cfg->m_pageBufSize * Limits::PAGE_SIZE * m_dataSizeFact;
	// 创建表并计算操作记录数
	createTableAndRecCnt(session);

	m_recCntPerThd = (uint) (m_recCnt / m_threadCnt);

	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(conn);

	/* 关闭数据库 */
	m_db->close();
	delete m_db;

	*recCnt = 0;
	*totalRecSize = 0;

	m_opCnt = m_recCntPerThd * m_threadCnt;
	m_dataSize = m_opCnt * getRecordSize();
}
/** 
 * 进行一遍表扫描
 * @post 数据库状态为open
 */
void InsertPerfTest::warmUp() {
	m_db = Database::open(m_cfg, false);
	Connection *conn = m_db->getConnection(false);
	Session *session = m_db->getSessionManager()->allocSession("InsertPerfTest::warmup", conn);
	m_currTab = m_db->openTable(session, m_tableName);
	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(conn);

	if (m_dataSizeFact < 1.0) {
		/* 如果数据量小于buffer size，则认为做的是cpu测试，预热数据 */
		Record *rec;
		switch (m_testTable) {
			case COUNT_TABLE:
				rec = CountTable::createRecord(0, 0);
				rec->m_format = REC_FIXLEN;
				break;
		}

		vector<RowId> ridVec;
		RowId rid;
		conn = m_db->getConnection(false);
		session = m_db->getSessionManager()->allocSession("InsertPerfTest::warmup", conn);
		// 插入
		for (u64 i = 0; i < m_recCntPerThd * m_threadCnt; ++i) {
			rid = m_currTab->getHeap()->insert(session, rec, NULL);
			ridVec.push_back(rid);
		}
		// 删除
		for (u64 i = 0; i < ridVec.size(); ++i) {
			bool success = m_currTab->getHeap()->del(session, ridVec[i]);
			assert(success);
		}
		m_db->getSessionManager()->freeSession(session);
		m_db->freeConnection(conn);
		ridVec.clear();
		/* 关掉检查点和清道夫，避免IO操作 */
		m_db->setCheckpointEnabled(false);
		m_db->getPageBuffer()->disableScavenger();
		// TODO: 关掉日志
		m_db->getTxnlog()->enableFlusher(false);
	}

}


void InsertPerfTest::tearDown() {
	if (m_dataSizeFact < 1.0) {
		m_db->setCheckpointEnabled(true);
		m_db->getPageBuffer()->enableScavenger();
		m_db->getTxnlog()->enableFlusher(true);
	}
	
	Connection *conn = m_db->getConnection(false);
	Session *session = m_db->getSessionManager()->allocSession("InsertPerfTest::tearDown", conn);
	m_db->closeTable(session, m_currTab);
	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(conn);
	m_currTab = NULL;
	
	m_db->close();
	delete m_db;
	m_db = NULL;

	delete m_cfg;
}

/** 根据参数获取不同大小的运行配置 **/
Config *InsertPerfTest::getConfig() {
	switch (m_scale) {
		case SMALL:
			return new Config(CommonDbConfig::getSmall());
			break;
		case MEDIUM:
			return new Config(CommonDbConfig::getMedium());
			break;
	}
}


/**
 * 创建表并且根据数据量算出记录数
 */
void InsertPerfTest::createTableAndRecCnt(Session *session) {
	switch (m_testTable) {
		case COUNT_TABLE:
			m_db->createTable(session, TABLE_NAME_COUNT, new TableDef(CountTable::getTableDef(false)));
			m_recCnt = m_dataSize / CountTable::getRecordSize();
			break;
		case LONGCHAR_TABLE:
			m_db->createTable(session, TABLE_NAME_LONGCHAR, new TableDef(LongCharTable::getTableDef(false)));
			m_recCnt = m_dataSize / LongCharTable::getRecordSize();
			break;
		case ACCOUNT_TABLE:
			m_db->createTable(session, TABLE_NAME_ACCOUNT, new TableDef(AccountTable::getTableDef(false)));
			//m_recCnt = m_dataSize / AccountTable::
			m_recCnt = m_dataSize / 200; // 平均200
			break;
	}
}

/**
 * 获得平均记录长度
 */
uint InsertPerfTest::getRecordSize() {
	switch (m_testTable) {
		case COUNT_TABLE:
			return CountTable::getRecordSize();
		case LONGCHAR_TABLE:
			return LongCharTable::getRecordSize();
		case ACCOUNT_TABLE:
			return 200;
	}
}


