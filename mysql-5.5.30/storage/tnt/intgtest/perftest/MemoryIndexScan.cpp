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
 * ���췽��
 *
 * @param useMms ʹ��MMS
 * @param recInMms ��¼�Ƿ���MMS��
 * @param isVar �Ƿ�Ϊ�䳤��
 */
MemoryIndexScanTest::MemoryIndexScanTest(bool useMms, bool recInMms, bool isVar) {
	m_useMms = useMms;
	m_recInMms = recInMms;
	m_isVar = isVar;

	// �����������ݻ���
	m_enableCache = true;
	// �ڴ��������
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
 * ��ȡ����������
 *
 * @return ����������
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
 * ��ȡ��������������Ϣ
 *
 * @return ������������
 */
string MemoryIndexScanTest::getDescription() const {
	return "IndexScan in Memory Performance";
}

/**
 * �������ݻ�����(Ĭ���ǲ���������)
 *	��ͬ�����Ĳ�������ͬ�Ĳ�������
 *
 * @return ������
 */
string MemoryIndexScanTest::getCachedName() const {
	return string("MemoryIndexScanTest-") + m_tableDef->m_name;
}

/** 
 * �����������к���
 */
void MemoryIndexScanTest::run() {
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
void MemoryIndexScanTest::loadData(u64 *totalRecSize, u64 *recCnt) {
	// �������ݿ�
	openTable(true);
	Connection *conn = m_db->getConnection(false);
	Session *session = m_db->getSessionManager()->allocSession("MemoryIndexScanTest::loadData", conn);
	
	if (m_isVar) { // �䳤��
		m_recCntLoaded = AccountTable::populate(session, m_table, &m_totalRecSizeLoaded);
	} else { // ������
		m_recCntLoaded = CountTable::populate(session, m_table, &m_totalRecSizeLoaded);
	}
	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(conn);
	closeDatabase();
	*totalRecSize = m_totalRecSizeLoaded;
	*recCnt = m_recCntLoaded;
}

/** 
 * Ԥ�ȣ�����һ������ɨ��
 *
 * @post ���ݿ�״̬Ϊopen
 */
void MemoryIndexScanTest::warmUp() {
	openTable(false);
	scanIndexOneTime();
}

/** 
 * һ������ɨ��
 *
 * @return ɨ���¼����
 */
u64 MemoryIndexScanTest::scanIndexOneTime() {
	u16 columns[2] = {0, 1};
	u64 scanCnt = index0Scan(m_db, m_table, 2, columns);
	if (m_useMms && m_recInMms)
		NTSE_ASSERT(m_table->getMmsTable()->getStatus().m_records == scanCnt);
	NTSE_ASSERT(scanCnt == m_recCntLoaded);
	return scanCnt;
}

