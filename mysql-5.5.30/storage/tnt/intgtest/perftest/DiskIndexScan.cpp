#include "DiskIndexScan.h"
#include "DbConfigs.h"
#include "api/Table.h"
#include "api/Database.h"
#include "misc/Session.h"
#include "misc/RecordHelper.h"
#include "mms/Mms.h"
#include "LongCharTable.h"
#include "AccountTable.h"
#include "TableHelper.h"
#include <sstream>

using namespace ntseperf;
using namespace std;

u32 DiskIndexScanTest::VOLUMN_RATIO = 100;
/** 
 * 构造函数
 *
 * @param isVar 是否为变长表
 */
DiskIndexScanTest::DiskIndexScanTest(bool isVar) {
	m_isVar = isVar;
}

void DiskIndexScanTest::setUp() {
	setConfig(CommonDbConfig::getSmall());
	setTableDef(m_isVar ? AccountTable::getTableDef(true) : LongCharTable::getTableDef(true));
	m_totalRecSizeLoaded = m_config->m_pageBufSize * Limits::PAGE_SIZE * VOLUMN_RATIO;
}

/** 
 * 获取测试用例名
 *
 * @return 测试用例名
 */
string DiskIndexScanTest::getName() const {
	stringstream ss;
	ss << "DiskIndexScanTest(";
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
string DiskIndexScanTest::getDescription() const {
	return "IndexScan on disk I/O Performance";
}

/**
 * 测试数据缓存名(默认是测试用例名)
 *	相同表名的测试有相同的测试数据
 *
 * @return 缓存名
 */
string DiskIndexScanTest::getCachedName() const {
	return string("DiskIndexScanTest-") + m_tableDef->m_name;
}

/** 
 * 测试用例运行函数
 */
void DiskIndexScanTest::run() {
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
void DiskIndexScanTest::loadData(u64 *totalRecSize, u64 *recCnt) {
	openTable(true);
	
	// 创建Session
	Connection *conn = m_db->getConnection(false);
	Session *session = m_db->getSessionManager()->allocSession("DiskIndexScanTest::loadData", conn);
	
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
 * 预热
 * @post 数据库状态为open
 */
void DiskIndexScanTest::warmUp() {
	openTable(false);
	u64 scanned = scanIndexOneTime(DISK_INDEX_SCAN_MMS_HIT_RATIO);
	assert(m_table->getMmsTable()->getStatus().m_records == scanned);
}

/** 
 * 一遍记录扫描
 * 
 * @param mmsRatio MMS缓存率
 * @return 扫描记录数
 */
u64 DiskIndexScanTest::scanIndexOneTime(float mmsRatio) {
	u64 scanCnt = (u64)((float)m_recCntLoaded * mmsRatio);
	u16 columns[2] = {0, 1};
	u64 scaned = ::index0Scan(m_db, m_table, 2, columns, scanCnt);
	assert(scaned == scanCnt);
	return scaned;
}

