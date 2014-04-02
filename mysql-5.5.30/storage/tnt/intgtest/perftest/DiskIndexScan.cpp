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
 * ���캯��
 *
 * @param isVar �Ƿ�Ϊ�䳤��
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
 * ��ȡ����������
 *
 * @return ����������
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
 * ��������������Ϣ
 *
 * @return ������������
 */
string DiskIndexScanTest::getDescription() const {
	return "IndexScan on disk I/O Performance";
}

/**
 * �������ݻ�����(Ĭ���ǲ���������)
 *	��ͬ�����Ĳ�������ͬ�Ĳ�������
 *
 * @return ������
 */
string DiskIndexScanTest::getCachedName() const {
	return string("DiskIndexScanTest-") + m_tableDef->m_name;
}

/** 
 * �����������к���
 */
void DiskIndexScanTest::run() {
	m_opCnt = scanIndexOneTime();
	m_opDataSize = m_totalRecSizeLoaded; // ���������� = ��������
}

/** 
 * ��������
 * 
 * @param totalRecSize [OUT] �ܼ�¼��С
 * @param recCnt [OUT] ��¼����
 * @post ���ݿ�״̬Ϊ�ر�
 */
void DiskIndexScanTest::loadData(u64 *totalRecSize, u64 *recCnt) {
	openTable(true);
	
	// ����Session
	Connection *conn = m_db->getConnection(false);
	Session *session = m_db->getSessionManager()->allocSession("DiskIndexScanTest::loadData", conn);
	
	if (m_isVar) { // �䳤��
		m_recCntLoaded = AccountTable::populate(session, m_table, &m_totalRecSizeLoaded);
	} else { // ������
		m_recCntLoaded = LongCharTable::populate(session, m_table, &m_totalRecSizeLoaded);
	}

	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(conn);
	closeDatabase();
	*totalRecSize = m_totalRecSizeLoaded;
	*recCnt = m_recCntLoaded;
}

/** 
 * Ԥ��
 * @post ���ݿ�״̬Ϊopen
 */
void DiskIndexScanTest::warmUp() {
	openTable(false);
	u64 scanned = scanIndexOneTime(DISK_INDEX_SCAN_MMS_HIT_RATIO);
	assert(m_table->getMmsTable()->getStatus().m_records == scanned);
}

/** 
 * һ���¼ɨ��
 * 
 * @param mmsRatio MMS������
 * @return ɨ���¼��
 */
u64 DiskIndexScanTest::scanIndexOneTime(float mmsRatio) {
	u64 scanCnt = (u64)((float)m_recCntLoaded * mmsRatio);
	u16 columns[2] = {0, 1};
	u64 scaned = ::index0Scan(m_db, m_table, 2, columns, scanCnt);
	assert(scaned == scanCnt);
	return scaned;
}

