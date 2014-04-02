#include "DiskTableScan.h"
#include "DbConfigs.h"
#include "api/Table.h"
#include "api/Database.h"
#include "misc/Session.h"
#include "misc/RecordHelper.h"
#include "mms/Mms.h"
#include "LongCharTable.h"
#include "AccountTable.h"
#include <sstream>

using namespace ntseperf;
using namespace std;


u32 DiskTableScanTest::VOLUMN_RATIO = 100;
/** 
 * 构造函数
 *
 * @param useMms 使用MMS
 * @param recInMms 记录是否在MMS
 * @param isVar 是否为变长表
 */
DiskTableScanTest::DiskTableScanTest(bool useMms, bool recInMms, bool isVar) {
	m_useMms = useMms;
	m_recInMms = recInMms;
	m_isVar = isVar;
}

void DiskTableScanTest::setUp() {
	setConfig(CommonDbConfig::getSmall());
	setTableDef(m_isVar ? AccountTable::getTableDef(m_useMms) : LongCharTable::getTableDef(m_useMms));
	m_totalRecSizeLoaded = m_config->m_pageBufSize * Limits::PAGE_SIZE * VOLUMN_RATIO;
	m_opDataSize = m_totalRecSizeLoaded;
}

/** 
 * 获取测试用例名
 *
 * @return 测试用例名
 */
string DiskTableScanTest::getName() const {
	stringstream ss;
	ss << "DiskTableScanTest(";
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
 * 测试数据缓存名(默认是测试用例名)
 *	相同表名的测试有相同的测试数据
 *
 * @return 缓存名
 */
string DiskTableScanTest::getCachedName() const {
	return string("DiskTableScanTest-") + m_tableDef->m_name;
}

/** 
 * 测试用例描述信息
 *
 * @return 测试用例描述
 */
string DiskTableScanTest::getDescription() const {
	return "TableScan on disk I/O Performance";
}

/** 
 * 测试用例运行函数
 */
void DiskTableScanTest::run() {
	m_opCnt = m_isVar ? AccountTable::scanTable(m_db, m_table) : LongCharTable::scanTable(m_db, m_table);
	m_opDataSize = m_totalRecSizeLoaded;
}

/** 
 * 加载数据
 * 
 * @param totalRecSize [OUT] 总记录大小
 * @param recCnt [OUT] 记录个数
 * @post 数据库状态为关闭
 */
void DiskTableScanTest::loadData(u64 *totalRecSize, u64 *recCnt) {
	openTable(true);
	// 创建Session
	Connection *conn = m_db->getConnection(false);
	Session *session = m_db->getSessionManager()->allocSession("DiskTableScanTest::loadData", conn);
	
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
 *
 * @post 数据库状态为open
 */
void DiskTableScanTest::warmUp() {
	openTable(false);
	
	if (m_recInMms) {
		// 创建Session
		
		SubRecordBuilder srb(m_table->getTableDef(), REC_REDUNDANT);
		Connection *conn = m_db->getConnection(false);
		Session *session = m_db->getSessionManager()->allocSession("DiskTableScanTest::warmUp", conn);
		SubRecordBuilder keyBuilder(m_table->getTableDef(), KEY_PAD);
		SubRecord *key = keyBuilder.createSubRecordById("");
		key->m_rowId = INVALID_ROW_ID;
		u16 columns[2] = {0, 1};
		byte *buf = (byte *)malloc(m_table->getTableDef()->m_maxRecSize);
		IndexScanCond cond(0, key, true, true, false);
		TblScan *scanHandle = m_table->indexScan(session, OP_READ, &cond, 2, columns);
		uint scanCnt = (uint) ((float)m_opCnt * DISK_TABLE_SCAN_MMS_HITRATIO);
		for (uint n = 0; n < scanCnt; n++) {
			m_table->getNext(scanHandle, buf);
			assert(m_table->getMmsTable()->getStatus().m_records == n + 1);
		}
		m_table->endScan(scanHandle);
		free(buf);
		freeSubRecord(key);
		m_db->getSessionManager()->freeSession(session);
		m_db->freeConnection(conn);
	}
}

