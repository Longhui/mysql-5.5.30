#include "MemoryTableScan.h"
#include "DbConfigs.h"
#include "api/Table.h"
#include "api/Database.h"
#include "misc/Session.h"
#include "misc/RecordHelper.h"
#include "mms/Mms.h"
#include "SmallLobInsert.h"
#include "PaperTable.h"
#include <sstream>
#include <string>

using namespace ntseperf;
using namespace std;


LobInsertTest::LobInsertTest(bool useMms, bool isSmallConfig, bool isSmall ) {
	m_enableCache = false;
	m_useMms = useMms;
	m_isSmall = isSmallConfig;
	m_isSmallLob = isSmall;
	if (m_isSmall) 
		m_enableCache = false;
	setConfig(CommonDbConfig::getMedium());
	setTableDef(PaperTable::getTableDef());
}

string LobInsertTest::getName() const {
	stringstream ss;
	ss << "SmallLobInsertTest(";
	if (m_useMms)
		ss << "useMMs,";
	if (m_isSmall)
		ss << "m_isSmall,";
	if (m_isSmallLob)
		ss << "m_isSmallLob";
	ss << ")";
	return ss.str();
}
string LobInsertTest::getDescription() const {
	return "Small lob insert Performance";
}

void LobInsertTest::run() {
	InsertOneTime();
}

/** ���β��ԵĲ����� */
u64 LobInsertTest::getOpCnt() {
	return m_opCnt;
}

/** ���ز��Ե������� */
u64 LobInsertTest::getDataSize() {
	return m_totalRecSizeLoaded;
}


/** �����ж��ټ�¼ */
u64 LobInsertTest::getRecCount() {
	uint len;
	if (m_isSmall) {
		len =  SMALL_LOB_INSERT_SMALL_LEN;
	} else {
		if (m_isSmallLob)
			len = SMALL_LOB_INSERT_BIG_LEN;
		else 
			len = BIG_LOB_INSERT_BIG_LEN;
	}
	return m_totalRecSizeLoaded / (len + sizeof(u64));

}
/** 
 * �����¼
 * @post ���ݿ�״̬Ϊ�ر�
 */
void LobInsertTest::loadData(u64 *totalRecSize, u64 *recCnt){
	// �������ݿ�ͱ�
	openTable(true);
	
	// ����Session
	Connection *conn = m_db->getConnection(false);
	Session *session = m_db->getSessionManager()->allocSession("SmallLobInsertTest::loadData", conn);
	if (m_isSmall) {
		m_totalRecSizeLoaded = m_config->m_pageBufSize * Limits::PAGE_SIZE / SMALL_LOB_INSERT_DATASIZE_SMALL_RATIO;
		m_recCntLoaded = getRecCount();
		m_records = PaperTable::populate(session, m_table, &m_totalRecSizeLoaded, SMALL_LOB_INSERT_SMALL_LEN, m_recCntLoaded);
	}
	else {
		//���д�������
		if (m_isSmallLob) {
			m_totalRecSizeLoaded = m_config->m_pageBufSize * Limits::PAGE_SIZE * SMALL_LOB_INSERT_DATASIZE_BIG_RATIO;
			m_recCntLoaded = getRecCount();
			m_recCntLoaded  = (m_recCntLoaded + (THREAD_COUNT  - 1))/ THREAD_COUNT * THREAD_COUNT;
			m_totalRecSizeLoaded = m_recCntLoaded * (SMALL_LOB_INSERT_BIG_LEN + sizeof(u64));
			m_records  = PaperTable::populate(session, m_table, SMALL_LOB_INSERT_BIG_LEN, THREAD_COUNT);
		} else {
			m_totalRecSizeLoaded = m_config->m_pageBufSize * Limits::PAGE_SIZE * BIG_LOB_INSERT_DATASIZE_BIG_RATIO;
			m_recCntLoaded = getRecCount();
			m_recCntLoaded  = (m_recCntLoaded + (THREAD_COUNT  - 1)) / THREAD_COUNT * THREAD_COUNT;
			m_totalRecSizeLoaded = m_recCntLoaded * (BIG_LOB_INSERT_BIG_LEN + sizeof(u64));
			m_records  = PaperTable::populate(session, m_table, BIG_LOB_INSERT_BIG_LEN, THREAD_COUNT);
		}
	}
	
	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(conn);
	closeDatabase();
	*totalRecSize = m_totalRecSizeLoaded;
	*recCnt = m_recCntLoaded;
	m_opCnt = m_recCntLoaded;
	m_opDataSize = m_totalRecSizeLoaded;
}

/** 
 * �������Ե��̣߳�Ȼ����Զ�Ӧ������
 */
void LobInsertTest::warmUp() {
	
	openTable(false);
	m_threads = new LobTestThread <LobInsertTest> *[THREAD_COUNT];
	//����100���߳�
	for (u16 i = 0; i < THREAD_COUNT; i++) {
		stringstream ss; 
		ss << i;
		string str = string ("InsertThread") + ss.str();
		m_threads[i] =  new LobTestThread<LobInsertTest>(str.c_str(), i, this);
	}
}

/**
 * ���Ե���Ҫ����������Ϊ�˷������ɽӿ�
 * 
 * @param tid Ѱ�Ҽ�¼������Ĳ�����������߳���
 */
void LobInsertTest::test(u16 tid ) {
	insertRecord(tid);
}

/**
 * ���뺬�����ļ�¼
 * 
 * @param tid Ѱ�Ҽ�¼������Ĳ�����������߳���
 */
void LobInsertTest::insertRecord(u16 tid) {
	
	Connection *conn = m_db->getConnection(false);
	Session *session = m_db->getSessionManager()->allocSession("SmallLobInsertTest::insertRecord", conn);
	//������С������
	if (m_isSmall) { 
		uint dupIndex;
		u64 index = tid;
		while (m_recCntLoaded > index) {
			m_table->insert(session, m_records[index]->m_data, &dupIndex);
			index += THREAD_COUNT;
		} 
	} else {//�����������
		uint dupIndex;
		u64 index = tid;
		u64 num = m_recCntLoaded / THREAD_COUNT;
		Record *rec = m_records[tid];
		while (num > 0) {
			m_table->insert(session, rec->m_data, &dupIndex);
			index += THREAD_COUNT;
			PaperTable::updateId(rec, index);
			num--;
		}
	}
	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(conn);
}

/**
 * ���Բ�������
 */
void LobInsertTest::InsertOneTime() {


	//�������е��߳�
	for (uint i = 0; i< THREAD_COUNT; i++) {
		m_threads[i]->start();
	}
	
	//�ȴ�����
	for (uint i = 0; i< THREAD_COUNT; i++) {
		m_threads[i]->join();
	}

}

void LobInsertTest::tearDown() {
	Connection *conn = m_db->getConnection(false);
	Session *session = m_db->getSessionManager()->allocSession("SmallLobInsertTest::tearDown", conn);
	m_table->close(session, false);
	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(conn);
	// delete m_countTab;
	m_table = NULL;
	m_db->close();
	delete m_db;
	m_db = NULL;
  
	//��������
	if (m_isSmall) {
		for(uint i = 0; i<m_recCntLoaded; i++) { 
			freeRecord(m_records[i]);
			m_records[i] = NULL;
		}
	} else {
		for(uint i = 0; i<THREAD_COUNT; i++) { 
			freeRecord(m_records[i]);
			m_records[i] = NULL;
		}
	}
	//�����߳�
	for (uint i = 0; i< THREAD_COUNT; i++) {
		delete m_threads[i];
		m_threads[i] = NULL;
	}
}

