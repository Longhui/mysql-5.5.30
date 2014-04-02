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
 * ���캯��
 *
 * @param useMms ʹ��MMS
 * @param recInMms ��¼�Ƿ���MMS
 * @param isVar �Ƿ�Ϊ�䳤��
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
 * ��ȡ����������
 *
 * @return ����������
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
 * �������ݻ�����(Ĭ���ǲ���������)
 *	��ͬ�����Ĳ�������ͬ�Ĳ�������
 *
 * @return ������
 */
string DiskTableScanTest::getCachedName() const {
	return string("DiskTableScanTest-") + m_tableDef->m_name;
}

/** 
 * ��������������Ϣ
 *
 * @return ������������
 */
string DiskTableScanTest::getDescription() const {
	return "TableScan on disk I/O Performance";
}

/** 
 * �����������к���
 */
void DiskTableScanTest::run() {
	m_opCnt = m_isVar ? AccountTable::scanTable(m_db, m_table) : LongCharTable::scanTable(m_db, m_table);
	m_opDataSize = m_totalRecSizeLoaded;
}

/** 
 * ��������
 * 
 * @param totalRecSize [OUT] �ܼ�¼��С
 * @param recCnt [OUT] ��¼����
 * @post ���ݿ�״̬Ϊ�ر�
 */
void DiskTableScanTest::loadData(u64 *totalRecSize, u64 *recCnt) {
	openTable(true);
	// ����Session
	Connection *conn = m_db->getConnection(false);
	Session *session = m_db->getSessionManager()->allocSession("DiskTableScanTest::loadData", conn);
	
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
 *
 * @post ���ݿ�״̬Ϊopen
 */
void DiskTableScanTest::warmUp() {
	openTable(false);
	
	if (m_recInMms) {
		// ����Session
		
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

