#include "MemoryTableScan.h"
#include "DbConfigs.h"
#include "api/Table.h"
#include "api/Database.h"
#include "misc/Session.h"
#include "misc/RecordHelper.h"
#include "mms/Mms.h"
#include "SmallLobInsert.h"
#include "PaperTable.h"
#include <sstream>
#include <string>

using namespace ntseperf;
using namespace std;


LobInsertTest::LobInsertTest(bool useMms, bool isSmallConfig, bool isSmall ) {
	m_enableCache = false;
	m_useMms = useMms;
	m_isSmall = isSmallConfig;
	m_isSmallLob = isSmall;
	if (m_isSmall) 
		m_enableCache = false;
	setConfig(CommonDbConfig::getMedium());
	setTableDef(PaperTable::getTableDef());
}

string LobInsertTest::getName() const {
	stringstream ss;
	ss << "SmallLobInsertTest(";
	if (m_useMms)
		ss << "useMMs,";
	if (m_isSmall)
		ss << "m_isSmall,";
	if (m_isSmallLob)
		ss << "m_isSmallLob";
	ss << ")";
	return ss.str();
}
string LobInsertTest::getDescription() const {
	return "Small lob insert Performance";
}

void LobInsertTest::run() {
	InsertOneTime();
}

/** 本次测试的操作数 */
u64 LobInsertTest::getOpCnt() {
	return m_opCnt;
}

/** 本地测试的数据量 */
u64 LobInsertTest::getDataSize() {
	return m_totalRecSizeLoaded;
}


/** 计算有多少记录 */
u64 LobInsertTest::getRecCount() {
	uint len;
	if (m_isSmall) {
		len =  SMALL_LOB_INSERT_SMALL_LEN;
	} else {
		if (m_isSmallLob)
			len = SMALL_LOB_INSERT_BIG_LEN;
		else 
			len = BIG_LOB_INSERT_BIG_LEN;
	}
	return m_totalRecSizeLoaded / (len + sizeof(u64));

}
/** 
 * 构造记录
 * @post 数据库状态为关闭
 */
void LobInsertTest::loadData(u64 *totalRecSize, u64 *recCnt){
	// 创建数据库和表
	openTable(true);
	
	// 创建Session
	Connection *conn = m_db->getConnection(false);
	Session *session = m_db->getSessionManager()->allocSession("SmallLobInsertTest::loadData", conn);
	if (m_isSmall) {
		m_totalRecSizeLoaded = m_config->m_pageBufSize * Limits::PAGE_SIZE / SMALL_LOB_INSERT_DATASIZE_SMALL_RATIO;
		m_recCntLoaded = getRecCount();
		m_records = PaperTable::populate(session, m_table, &m_totalRecSizeLoaded, SMALL_LOB_INSERT_SMALL_LEN, m_recCntLoaded);
	}
	else {
		//进行大数据量
		if (m_isSmallLob) {
			m_totalRecSizeLoaded = m_config->m_pageBufSize * Limits::PAGE_SIZE * SMALL_LOB_INSERT_DATASIZE_BIG_RATIO;
			m_recCntLoaded = getRecCount();
			m_recCntLoaded  = (m_recCntLoaded + (THREAD_COUNT  - 1))/ THREAD_COUNT * THREAD_COUNT;
			m_totalRecSizeLoaded = m_recCntLoaded * (SMALL_LOB_INSERT_BIG_LEN + sizeof(u64));
			m_records  = PaperTable::populate(session, m_table, SMALL_LOB_INSERT_BIG_LEN, THREAD_COUNT);
		} else {
			m_totalRecSizeLoaded = m_config->m_pageBufSize * Limits::PAGE_SIZE * BIG_LOB_INSERT_DATASIZE_BIG_RATIO;
			m_recCntLoaded = getRecCount();
			m_recCntLoaded  = (m_recCntLoaded + (THREAD_COUNT  - 1)) / THREAD_COUNT * THREAD_COUNT;
			m_totalRecSizeLoaded = m_recCntLoaded * (BIG_LOB_INSERT_BIG_LEN + sizeof(u64));
			m_records  = PaperTable::populate(session, m_table, BIG_LOB_INSERT_BIG_LEN, THREAD_COUNT);
		}
	}
	
	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(conn);
	closeDatabase();
	*totalRecSize = m_totalRecSizeLoaded;
	*recCnt = m_recCntLoaded;
	m_opCnt = m_recCntLoaded;
	m_opDataSize = m_totalRecSizeLoaded;
}

/** 
 * 创建测试的线程，然后测试对应的数据
 */
void LobInsertTest::warmUp() {
	
	openTable(false);
	m_threads = new LobTestThread <LobInsertTest> *[THREAD_COUNT];
	//创建100个线程
	for (u16 i = 0; i < THREAD_COUNT; i++) {
		stringstream ss; 
		ss << i;
		string str = string ("InsertThread") + ss.str();
		m_threads[i] =  new LobTestThread<LobInsertTest>(str.c_str(), i, this);
	}
}

/**
 * 测试的主要方法，这里为了方便做成接口
 * 
 * @param tid 寻找记录在数组的步长，这里就线程数
 */
void LobInsertTest::test(u16 tid ) {
	insertRecord(tid);
}

/**
 * 插入含大对象的记录
 * 
 * @param tid 寻找记录在数组的步长，这里就线程数
 */
void LobInsertTest::insertRecord(u16 tid) {
	
	Connection *conn = m_db->getConnection(false);
	Session *session = m_db->getSessionManager()->allocSession("SmallLobInsertTest::insertRecord", conn);
	//假如是小型配置
	if (m_isSmall) { 
		uint dupIndex;
		u64 index = tid;
		while (m_recCntLoaded > index) {
			m_table->insert(session, m_records[index]->m_data, &dupIndex);
			index += THREAD_COUNT;
		} 
	} else {//假如大数据量
		uint dupIndex;
		u64 index = tid;
		u64 num = m_recCntLoaded / THREAD_COUNT;
		Record *rec = m_records[tid];
		while (num > 0) {
			m_table->insert(session, rec->m_data, &dupIndex);
			index += THREAD_COUNT;
			PaperTable::updateId(rec, index);
			num--;
		}
	}
	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(conn);
}

/**
 * 测试插入大对象
 */
void LobInsertTest::InsertOneTime() {


	//启动所有的线程
	for (uint i = 0; i< THREAD_COUNT; i++) {
		m_threads[i]->start();
	}
	
	//等待结束
	for (uint i = 0; i< THREAD_COUNT; i++) {
		m_threads[i]->join();
	}

}

void LobInsertTest::tearDown() {
	Connection *conn = m_db->getConnection(false);
	Session *session = m_db->getSessionManager()->allocSession("SmallLobInsertTest::tearDown", conn);
	m_table->close(session, false);
	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(conn);
	// delete m_countTab;
	m_table = NULL;
	m_db->close();
	delete m_db;
	m_db = NULL;
  
	//清理数据
	if (m_isSmall) {
		for(uint i = 0; i<m_recCntLoaded; i++) { 
			freeRecord(m_records[i]);
			m_records[i] = NULL;
		}
	} else {
		for(uint i = 0; i<THREAD_COUNT; i++) { 
			freeRecord(m_records[i]);
			m_records[i] = NULL;
		}
	}
	//清理线程
	for (uint i = 0; i< THREAD_COUNT; i++) {
		delete m_threads[i];
		m_threads[i] = NULL;
	}
}

