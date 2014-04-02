#include "MemoryTableScan.h"
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
 * 构造函数
 *
 * @param useMms 使用MMS
 * @param recInMms 记录是否在MMS中
 * @param isVar 是否为变长表测试
 */
MemoryTableScanTest::MemoryTableScanTest(bool useMms, bool recInMms, bool isVar) {
	m_useMms = useMms;
	m_recInMms = recInMms;
	m_isVar = isVar;
}

void MemoryTableScanTest::setUp() {
	// 开启测试数据缓存
	m_enableCache = true;
	// 内存测试用例
	m_isMemoryCase = true;
	// 用例数据库配置
	setConfig(CommonDbConfig::getMedium());
	// 用例表模式
	setTableDef(m_isVar ? AccountTable::getTableDef(m_useMms) : CountTable::getTableDef(m_useMms));
	// 初始表数据量
	m_totalRecSizeLoaded = m_config->m_pageBufSize * Limits::PAGE_SIZE / MEM_TABLE_SCAN_REDUCE_RATIO;
	if (m_useMms && m_recInMms)
		m_config->m_mmsSize = m_totalRecSizeLoaded * MEM_TABLE_SCAN_MMS_MULTIPLE / Limits::PAGE_SIZE;
}

/** 
 * 获取测试用例名 
 *
 * @return 测试用例名
 */
string MemoryTableScanTest::getName() const {
	stringstream ss;
	ss << "MemoryTableScanTest(";
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
 * @return 测试用例描述信息
 */
string MemoryTableScanTest::getDescription() const {
	return "TableScan in Memory Performance";
}

/**
 * 测试数据缓存名(默认是测试用例名)
 *	相同表名的测试有相同的测试数据
 *
 * @return 缓存名
 */
string MemoryTableScanTest::getCachedName() const {
	return string("MemoryTableScanTest-") + m_tableDef->m_name;
}

/** 
 * 测试用例运行函数
 */
void MemoryTableScanTest::run() {
	m_opCnt = m_isVar ? AccountTable::scanTable(m_db, m_table) : CountTable::scanTable(m_db, m_table);
	m_opDataSize = m_totalRecSizeLoaded; // 操作数据量 = 表数据量
}

/** 
 * 加载测试数据
 *
 * @param totalRecSize [out] 测试数据量
 * @param recCnt [out] 测试数据记录数
 * @post 保持数据库为关闭状态
 */
void MemoryTableScanTest::loadData(u64 *totalRecSize, u64 *recCnt) {
	openTable(true);
	Connection *conn = m_db->getConnection(false);
	Session *session = m_db->getSessionManager()->allocSession("MemoryTableScanTest::loadData", conn);
	if (m_isVar) {
		m_recCntLoaded = AccountTable::populate(session, m_table, &m_totalRecSizeLoaded);
	} else {
		m_recCntLoaded = CountTable::populate(session, m_table, &m_totalRecSizeLoaded);
	}
	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(conn);
	closeDatabase();
	*totalRecSize = m_totalRecSizeLoaded;
	*recCnt = m_recCntLoaded;
}

/** 
 * 预热，进行一遍表扫描
 *
 * @post 数据库状态为open
 */
void MemoryTableScanTest::warmUp() {
	openTable(false);
	m_isVar ? AccountTable::scanTable(m_db, m_table) : CountTable::scanTable(m_db, m_table);
	if (m_recInMms) {
		u16 columns[2] = {0, 1};
		u64 scaned = ::index0Scan(m_db, m_table, 2, columns);
		NTSE_ASSERT(m_table->getMmsTable()->getStatus().m_records == scaned);
	}
}

