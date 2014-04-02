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
 * ���캯��
 *
 * @param useMms ʹ��MMS
 * @param recInMms ��¼�Ƿ���MMS��
 * @param isVar �Ƿ�Ϊ�䳤�����
 */
MemoryTableScanTest::MemoryTableScanTest(bool useMms, bool recInMms, bool isVar) {
	m_useMms = useMms;
	m_recInMms = recInMms;
	m_isVar = isVar;
}

void MemoryTableScanTest::setUp() {
	// �����������ݻ���
	m_enableCache = true;
	// �ڴ��������
	m_isMemoryCase = true;
	// �������ݿ�����
	setConfig(CommonDbConfig::getMedium());
	// ������ģʽ
	setTableDef(m_isVar ? AccountTable::getTableDef(m_useMms) : CountTable::getTableDef(m_useMms));
	// ��ʼ��������
	m_totalRecSizeLoaded = m_config->m_pageBufSize * Limits::PAGE_SIZE / MEM_TABLE_SCAN_REDUCE_RATIO;
	if (m_useMms && m_recInMms)
		m_config->m_mmsSize = m_totalRecSizeLoaded * MEM_TABLE_SCAN_MMS_MULTIPLE / Limits::PAGE_SIZE;
}

/** 
 * ��ȡ���������� 
 *
 * @return ����������
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
 * ��ȡ��������������Ϣ
 *
 * @return ��������������Ϣ
 */
string MemoryTableScanTest::getDescription() const {
	return "TableScan in Memory Performance";
}

/**
 * �������ݻ�����(Ĭ���ǲ���������)
 *	��ͬ�����Ĳ�������ͬ�Ĳ�������
 *
 * @return ������
 */
string MemoryTableScanTest::getCachedName() const {
	return string("MemoryTableScanTest-") + m_tableDef->m_name;
}

/** 
 * �����������к���
 */
void MemoryTableScanTest::run() {
	m_opCnt = m_isVar ? AccountTable::scanTable(m_db, m_table) : CountTable::scanTable(m_db, m_table);
	m_opDataSize = m_totalRecSizeLoaded; // ���������� = ��������
}

/** 
 * ���ز�������
 *
 * @param totalRecSize [out] ����������
 * @param recCnt [out] �������ݼ�¼��
 * @post �������ݿ�Ϊ�ر�״̬
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
 * Ԥ�ȣ�����һ���ɨ��
 *
 * @post ���ݿ�״̬Ϊopen
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

