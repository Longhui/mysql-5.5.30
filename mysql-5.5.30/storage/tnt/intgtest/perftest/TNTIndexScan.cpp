#include "TNTIndexScan.h"

#include "TNTDbConfigs.h"
#include "api/TNTTable.h"
#include "api/TNTDatabase.h"
#include "api/Table.h"
#include "misc/Session.h"
#include "misc/RecordHelper.h"
#include "mms/Mms.h"

#include "TNTAccountTable.h"
#include "TNTTableHelper.h"
#include <sstream>

using namespace tntperf;
using namespace std;

//u32 TNTIndexScanTest::VOLUMN_RATIO = 100;

/** 
 * 构造函数
 *
 * @param useMms 使用MMS
 * @param recInMms 记录是否在MMS中
 * @param isVar 是否为变长表测试
 */
TNTIndexScanTest::TNTIndexScanTest(bool useMms, bool recInMms, bool isVar)
{
	m_useMms = useMms;
	m_recInMms = recInMms;
	m_isVar = isVar;
}

/** 
 * 获取测试用例名 
 *
 * @return 测试用例名
 */	    
string TNTIndexScanTest::getName() const
{
	stringstream ss;
	ss << "TNTIndexScanTest(";
	if (m_useMms)
		ss << "useMMs,";
	if (m_recInMms)
		ss <<"recInMms,";
	/*
	if (m_isVar)
		ss <<"varTable";
    */
	ss << ")";
	return ss.str();
}

/** 
 * 获取测试用例描述信息
 *
 * @return 测试用例描述信息
 */
string TNTIndexScanTest::getDescription() const
{
	return "TNTIndexScan in Memory Performance";
}

/**
 * 测试数据缓存名(默认是测试用例名)
 *	相同表名的测试有相同的测试数据
 *
 * @return 缓存名
 */
string TNTIndexScanTest::getCachedName() const	
{
    return string("TNTIndexScanTest-") + m_tableDef->m_name;
}

/** 
 * 加载测试数据
 *
 * @param totalRecSize [out] 测试数据量
 * @param recCnt [out] 测试数据记录数
 * @post 保持数据库为关闭状态
 */
void TNTIndexScanTest::loadData(u64 *totalRecSize, u64 *recCnt)
{
	openTable(true);
	Connection *conn = m_db->getNtseDb()->getConnection(false);
	Session *session = m_db->getNtseDb()->getSessionManager()->allocSession("TNTTableScanTest::loadData", conn);
	/*
	if (m_isVar) {
		m_recCntLoaded = AccountTable::populate(session, m_table, &m_totalRecSizeLoaded);
	} else {
		m_recCntLoaded = CountTable::populate(session, m_table, &m_totalRecSizeLoaded);
	}
	*/
	TNTOpInfo opInfo;
	::initTNTOpInfo(&opInfo, TL_NO);
	TNTTransaction *trx = startTrx(m_db->getTransSys(), session);
    m_recCntLoaded = TNTAccountTable::populate(session, m_table,&opInfo,&m_totalRecSizeLoaded);
	commitTrx(m_db->getTransSys(), trx);
	m_db->getNtseDb()->getSessionManager()->freeSession(session);
	m_db->getNtseDb()->freeConnection(conn);
	closeDatabase();
	*totalRecSize = m_totalRecSizeLoaded;
	*recCnt = m_recCntLoaded;
}
void TNTIndexScanTest::warmUp()
{
	openTable(false);
	//m_isVar ? AccountTable::scanTable(m_db, m_table) : CountTable::scanTable(m_db, m_table);
    TNTAccountTable::scanTable(m_db, m_table);
	if (m_recInMms) {
		u16 columns[2] = {0, 1};
		u64 scaned = ::TNTIndex0Scan(m_db, m_table, 2, columns);
		//NTSE_ASSERT(m_table->getNtseTable()->getMmsTable()->getStatus().m_records == scaned);
	}
}
void TNTIndexScanTest::run()
{
	u16 columns[2] = {0, 1};
	m_opCnt =::TNTIndex0Scan(m_db,m_table,2,columns);
	m_opDataSize = m_totalRecSizeLoaded; // 操作数据量 = 表数据量
}
void TNTIndexScanTest::setUp()
{
	// 开启测试数据缓存
	m_enableCache = true;
	// 内存测试用例
	m_isMemoryCase = true;
	// 用例数据库配置
	setConfig(TNTCommonDbConfig::getMedium());
	// 用例表模式
	//setTableDef(m_isVar ? AccountTable::getTableDef(m_useMms) : CountTable::getTableDef(m_useMms));
	setTableDef(TNTAccountTable::getTableDef(m_useMms));
	// 初始表数据量
	m_totalRecSizeLoaded = m_config->m_ntseConfig.m_pageBufSize * Limits::PAGE_SIZE / TNT_INDEX_SCAN_REDUCE_RATIO;
	if (m_useMms && m_recInMms)
		m_config->m_ntseConfig.m_mmsSize = m_totalRecSizeLoaded * TNT_INDEX_SCAN_MMS_MULTIPLE / Limits::PAGE_SIZE;
}