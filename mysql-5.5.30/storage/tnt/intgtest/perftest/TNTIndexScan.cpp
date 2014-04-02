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
 * ���캯��
 *
 * @param useMms ʹ��MMS
 * @param recInMms ��¼�Ƿ���MMS��
 * @param isVar �Ƿ�Ϊ�䳤�����
 */
TNTIndexScanTest::TNTIndexScanTest(bool useMms, bool recInMms, bool isVar)
{
	m_useMms = useMms;
	m_recInMms = recInMms;
	m_isVar = isVar;
}

/** 
 * ��ȡ���������� 
 *
 * @return ����������
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
 * ��ȡ��������������Ϣ
 *
 * @return ��������������Ϣ
 */
string TNTIndexScanTest::getDescription() const
{
	return "TNTIndexScan in Memory Performance";
}

/**
 * �������ݻ�����(Ĭ���ǲ���������)
 *	��ͬ�����Ĳ�������ͬ�Ĳ�������
 *
 * @return ������
 */
string TNTIndexScanTest::getCachedName() const	
{
    return string("TNTIndexScanTest-") + m_tableDef->m_name;
}

/** 
 * ���ز�������
 *
 * @param totalRecSize [out] ����������
 * @param recCnt [out] �������ݼ�¼��
 * @post �������ݿ�Ϊ�ر�״̬
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
	m_opDataSize = m_totalRecSizeLoaded; // ���������� = ��������
}
void TNTIndexScanTest::setUp()
{
	// �����������ݻ���
	m_enableCache = true;
	// �ڴ��������
	m_isMemoryCase = true;
	// �������ݿ�����
	setConfig(TNTCommonDbConfig::getMedium());
	// ������ģʽ
	//setTableDef(m_isVar ? AccountTable::getTableDef(m_useMms) : CountTable::getTableDef(m_useMms));
	setTableDef(TNTAccountTable::getTableDef(m_useMms));
	// ��ʼ��������
	m_totalRecSizeLoaded = m_config->m_ntseConfig.m_pageBufSize * Limits::PAGE_SIZE / TNT_INDEX_SCAN_REDUCE_RATIO;
	if (m_useMms && m_recInMms)
		m_config->m_ntseConfig.m_mmsSize = m_totalRecSizeLoaded * TNT_INDEX_SCAN_MMS_MULTIPLE / Limits::PAGE_SIZE;
}