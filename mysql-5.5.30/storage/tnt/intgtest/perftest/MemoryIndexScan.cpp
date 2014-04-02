#include "MemoryIndexScan.h"
#include "DbConfigs.h"
#include "api/Table.h"
#include "api/Database.h"
#include "misc/Session.h"
#include "misc/RecordHelper.h"
#include "mms/Mms.h"
#include "CountTable.h"
#include "AccountTable.h"
#include "TableHelper.h"
#include <sstream>

using namespace ntseperf;
using namespace std;

/** 
 * 构造方法
 *
 * @param useMms 使用MMS
 * @param recInMms 记录是否在MMS中
 * @param isVar 是否为变长表
 */
MemoryIndexScanTest::MemoryIndexScanTest(bool useMms, bool recInMms, bool isVar) {
	m_useMms = useMms;
	m_recInMms = recInMms;
	m_isVar = isVar;

	// 开启测试数据缓存
	m_enableCache = true;
	// 内存测试用例
	m_isMemoryCase = true;
}

void MemoryIndexScanTest::setUp() {
	setConfig(CommonDbConfig::getMedium());
	setTableDef(m_isVar ? AccountTable::getTableDef(m_useMms) : CountTable::getTableDef(m_useMms));
	m_totalRecSizeLoaded = m_config->m_pageBufSize * Limits::PAGE_SIZE / MEM_INDEX_SCAN_REDUCE_RATIO;
	if (m_useMms && m_recInMms)
		m_config->m_mmsSize = m_totalRecSizeLoaded * MEM_INDEX_SCAN_MMS_MULTIPLE / Limits::PAGE_SIZE;
}

/** 
 * 获取测试用例名
 *
 * @return 测试用例名
 */
string MemoryIndexScanTest::getName() const {
	stringstream ss;
	ss << "MemoryIndexScanTest(";
	if (m_useMms)
		ss << "useMMs,";
	if (m_recInMms)
		ss <<"recInMms,";
	if (m_isVar)
		ss <<"varTable";
	ss << ")";
	return ss.str();
}

/** 
 * 获取测试用例描述信息
 *
 * @return 测试用例描述
 */
string MemoryIndexScanTest::getDescription() const {
	return "IndexScan in Memory Performance";
}

/**
 * 测试数据缓存名(默认是测试用例名)
 *	相同表名的测试有相同的测试数据
 *
 * @return 缓存名
 */
string MemoryIndexScanTest::getCachedName() const {
	return string("MemoryIndexScanTest-") + m_tableDef->m_name;
}

/** 
 * 测试用例运行函数
 */
void MemoryIndexScanTest::run() {
	m_opCnt = scanIndexOneTime();
	m_opDataSize = m_totalRecSizeLoaded; // 操作数据量 = 表数据量
}

/** 
 * 加载数据
 * 
 * @param totalRecSize [OUT] 总记录大小 
 * @param recCnt [OUT] 记录个数
 * @post 数据库状态为关闭
 */
void MemoryIndexScanTest::loadData(u64 *totalRecSize, u64 *recCnt) {
	// 创建数据库
	openTable(true);
	Connection *conn = m_db->getConnection(false);
	Session *session = m_db->getSessionManager()->allocSession("MemoryIndexScanTest::loadData", conn);
	
	if (m_isVar) { // 变长表
		m_recCntLoaded = AccountTable::populate(session, m_table, &m_totalRecSizeLoaded);
	} else { // 定长表
		m_recCntLoaded = CountTable::populate(session, m_table, &m_totalRecSizeLoaded);
	}
	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(conn);
	closeDatabase();
	*totalRecSize = m_totalRecSizeLoaded;
	*recCnt = m_recCntLoaded;
}

/** 
 * 预热，进行一遍索引扫描
 *
 * @post 数据库状态为open
 */
void MemoryIndexScanTest::warmUp() {
	openTable(false);
	scanIndexOneTime();
}

/** 
 * 一遍索引扫描
 *
 * @return 扫描记录个数
 */
u64 MemoryIndexScanTest::scanIndexOneTime() {
	u16 columns[2] = {0, 1};
	u64 scanCnt = index0Scan(m_db, m_table, 2, columns);
	if (m_useMms && m_recInMms)
		NTSE_ASSERT(m_table->getMmsTable()->getStatus().m_records == scanCnt);
	NTSE_ASSERT(scanCnt == m_recCntLoaded);
	return scanCnt;
}

