#include "MemoryTableScan.h"
#include "DbConfigs.h"
#include "api/Table.h"
#include "api/Database.h"
#include "misc/Session.h"
#include "misc/RecordHelper.h"
#include "mms/Mms.h"
#include "LobInsert.h"
#include "PaperTable.h"
#include <sstream>
#include <string>

using namespace ntseperf;
using namespace std;


LobInsertTest::LobInsertTest(bool useMms, u64 dataSize, u32 lobSize, bool inMemory) {
	//测试insert这里把数据不cache
	m_enableCache = false;
	m_useMms = useMms;
	m_dataSize = dataSize;
	m_isMemoryCase = inMemory;
	m_lobSize = lobSize;
	setConfig(CommonDbConfig::getMedium());
	setTableDef(PaperTable::getTableDef());
}

string LobInsertTest::getName() const {
	stringstream ss;
	ss << "LobInsertTest(";
	if (m_useMms)
		ss << "useMMs, ";
	if (m_isMemoryCase)
		ss << "inMemory, ";
	ss << m_dataSize;
	ss << ", ";
	ss << m_lobSize;
	ss << ", ";
	ss << ")";
	return ss.str();
}
string LobInsertTest::getDescription() const {
	return "Lob Insert Performance";
}

void LobInsertTest::run() {
	InsertOneTime();
}

/** 计算有多少记录 */
u64 LobInsertTest::getRecCount() {
	return m_totalRecSizeLoaded / (m_lobSize + sizeof(u64));

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
	Session *session = m_db->getSessionManager()->allocSession("LobInsertTest::loadData", conn);
	if (m_isMemoryCase) {
		m_totalRecSizeLoaded = m_dataSize;
		m_recCntLoaded = getRecCount();
		m_records = PaperTable::populate(session, m_table, &m_totalRecSizeLoaded, m_lobSize, m_recCntLoaded);
	}
	else {
		//进行大数据量
		m_totalRecSizeLoaded = m_dataSize;
		m_recCntLoaded = getRecCount();
		m_recCntLoaded  = (m_recCntLoaded + (THREAD_COUNT  - 1)) / THREAD_COUNT * THREAD_COUNT;
		m_totalRecSizeLoaded = m_recCntLoaded * (m_lobSize + sizeof(u64));
		m_records  = PaperTable::populate(session, m_table, m_lobSize, THREAD_COUNT);
	}

	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(conn);
	closeDatabase();
	*totalRecSize = 0;
	*recCnt = 0;
	m_opCnt = m_recCntLoaded;
	m_opDataSize = m_totalRecSizeLoaded;
	m_totalRecSizeLoaded = 0;
	m_recCntLoaded = 0;
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
	if (m_isMemoryCase) { 
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
	m_db->closeTable(session, m_table);
	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(conn);
	// delete m_countTab;
	m_table = NULL;
	m_db->close();
	delete m_db;
	m_db = NULL;

	//清理数据
	if (m_isMemoryCase) {
		for(uint i = 0; i<m_opCnt; i++) { 
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
	delete m_threads;
}

