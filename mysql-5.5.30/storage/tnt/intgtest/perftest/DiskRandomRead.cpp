#include "DiskRandomRead.h"
#include "DbConfigs.h"
#include "api/Table.h"
#include "api/Database.h"
#include "misc/Session.h"
#include "misc/RecordHelper.h"
#include "mms/Mms.h"
#include "LongCharTable.h"
#include "AccountTable.h"
#include "Random.h"
#include <sstream>

using namespace ntseperf;
using namespace std;

u32 DiskRandomReadTest::VOLUMN_RATIO = 10;
/** 
 * 构造函数
 *
 * @param useMms 使用MMS
 * @param isVar 是否为变长表
 * @param threadCount 线程个数
 * @param loopCount 线程内循环次数
 */
DiskRandomReadTest::DiskRandomReadTest(bool useMms, bool isVar, int threadCount, int loopCount) {
	m_useMms = useMms;
	m_isVar = isVar;
	m_nrThreads = threadCount;
	m_loopCount = loopCount;
}

void DiskRandomReadTest::setUp() {
	m_threads = new DiskRandomReadThread *[m_nrThreads];
	for (int i = 0; i < m_nrThreads; i++)
		m_threads[i] = new DiskRandomReadThread(this, m_loopCount);
	setConfig(CommonDbConfig::getSmall());
	setTableDef(m_isVar ? AccountTable::getTableDef(m_useMms) : LongCharTable::getTableDef(m_useMms));
	m_totalRecSizeLoaded = m_config->m_pageBufSize * Limits::PAGE_SIZE * VOLUMN_RATIO;
}

/** 
 * 获取测试用例名
 *
 * @return 测试用例名
 */
string DiskRandomReadTest::getName() const {
	stringstream ss;
	ss << "DiskRandomReadTest(";
	if (m_useMms)
		ss << "useMMs,";;
	if (m_isVar)
		ss <<"varTable";
	ss << ")";
	return ss.str();
}

/** 
 * 测试用例描述信息
 *
 * @return 测试用例描述
 */
string DiskRandomReadTest::getDescription() const {
	return "Random Read on Disk I/O Performance";
}

/**
 * 测试数据缓存名(默认是测试用例名)
 *	相同表名的测试有相同的测试数据
 * 
 * @return 缓存名
 */
string DiskRandomReadTest::getCachedName() const {
	return string("DiskRandomReadTest-") + m_tableDef->m_name;
}

/** 
 * 测试用例运行函数
 */
void DiskRandomReadTest::run() {
	for (int i = 0; i < m_nrThreads; i++)
		m_threads[i]->start();
	for(int i = 0; i < m_nrThreads; i++)
		m_threads[i]->join();

	m_opDataSize = m_nrThreads * m_loopCount * m_totalRecSizeLoaded / m_recCntLoaded;
	m_opCnt = m_nrThreads * m_loopCount;
}

/**
 * 随机读
 *
 * @param count 循环次数
 */
void DiskRandomReadTest::randomRead(int count) {
	u64 id = 0;
	Connection *conn = m_db->getConnection(false);
	Session *session;
	byte *buf = (byte *)malloc(m_table->getTableDef()->m_maxRecSize);
	TblScan *scanHandle;
	bool scanSucc;

	SubRecordBuilder keyBuilder(m_table->getTableDef(), KEY_PAD);
	SubRecord *key = keyBuilder.createSubRecordById("0", &id);
	u16 columns[2] = {0, 1};
	u16 columnsVar[3] = {0, 1, 2};
	key->m_rowId = INVALID_ROW_ID;
	for (int i = 0; i < count; i++) {
		session = m_db->getSessionManager()->allocSession("DiskRandomReadTest::randomRead", conn);
		id = RandomGen::nextInt(0, m_recCntLoaded);
		if (m_isVar) {
			key = AccountTable::updateKey(key, id);
			IndexScanCond cond(0, key, true, true, false);
			scanHandle = m_table->indexScan(session, OP_READ, &cond, 3, columnsVar);
		} else {
			key = LongCharTable::updateKey(key, id);
			IndexScanCond cond(0, key, true, true, false);
			scanHandle = m_table->indexScan(session, OP_READ, &cond, 2, columns);
		}
		scanSucc = m_table->getNext(scanHandle, buf);
		assert(scanSucc);
		m_table->endScan(scanHandle);
		m_db->getSessionManager()->freeSession(session);
	}
	freeSubRecord(key);
	free(buf);
	m_db->freeConnection(conn);	
}

/** 
 * 加载数据
 *
 * @param totalRecSize [OUT] 总记录大小
 * @param recCnt [OUT] 记录个数
 * @post 数据库状态为关闭
 */
void DiskRandomReadTest::loadData(u64 *totalRecSize, u64 *recCnt) {
	// 创建数据库
	openTable(true);
	// 创建Session
	Connection *conn = m_db->getConnection(false);
	Session *session = m_db->getSessionManager()->allocSession("DiskRandomReadTest::loadData", conn);
	
	if (m_isVar) { // 变长表
		m_recCntLoaded = AccountTable::populate(session, m_table, &m_totalRecSizeLoaded);
	} else { // 定长表
		m_recCntLoaded = LongCharTable::populate(session, m_table, &m_totalRecSizeLoaded);
	}

	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(conn);
	closeDatabase();
	*totalRecSize = m_totalRecSizeLoaded;
	*recCnt = m_recCntLoaded;
}

/** 
 * 预热，进行一遍索引扫描
 * @post 数据库状态为open
 */
void DiskRandomReadTest::warmUp() {
	openTable(false);
	scanIndexOneTime();
}

/** 
 * 一遍索引扫描
 */
void DiskRandomReadTest::scanIndexOneTime() {
	SubRecordBuilder srb(m_table->getTableDef(), REC_REDUNDANT);
	Connection *conn = m_db->getConnection(false);
	Session *session = m_db->getSessionManager()->allocSession("MemoryIndexScanTest::scanIndexOneTime", conn);
	SubRecordBuilder keyBuilder(m_table->getTableDef(), KEY_PAD);
	u16 columns[2] = {0, 1};
	byte *buf = (byte *)malloc(m_table->getTableDef()->m_maxRecSize);
	IndexScanCond cond(0, NULL, true, true, false);
	TblScan *scanHandle = m_table->indexScan(session, OP_READ, &cond, 2, columns);
	for (uint n = 0; n < m_opCnt; n++) {
		m_table->getNext(scanHandle, buf);
		assert(m_table->getMmsTable()->getStatus().m_records == n + 1);
	}
	m_table->endScan(scanHandle);
	free(buf);
	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(conn);	
}

/** 
 * 关闭测试用例
 */
void DiskRandomReadTest::tearDown() {
	EmptyTestCase::tearDown();
	for (int i = 0; i < m_nrThreads; i++)
		delete m_threads[i];
	delete [] m_threads;
}

