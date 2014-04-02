#include "MemoryTableScan.h"
#include "DbConfigs.h"
#include "api/Table.h"
#include "api/Database.h"
#include "misc/Session.h"
#include "misc/RecordHelper.h"
#include "mms/Mms.h"
#include "LobInsert.h"
#include "PaperTable.h"
#include <sstream>
#include <string>

using namespace ntseperf;
using namespace std;


LobInsertTest::LobInsertTest(bool useMms, u64 dataSize, u32 lobSize, bool inMemory) {
	//����insert��������ݲ�cache
	m_enableCache = false;
	m_useMms = useMms;
	m_dataSize = dataSize;
	m_isMemoryCase = inMemory;
	m_lobSize = lobSize;
	setConfig(CommonDbConfig::getMedium());
	setTableDef(PaperTable::getTableDef());
}

string LobInsertTest::getName() const {
	stringstream ss;
	ss << "LobInsertTest(";
	if (m_useMms)
		ss << "useMMs, ";
	if (m_isMemoryCase)
		ss << "inMemory, ";
	ss << m_dataSize;
	ss << ", ";
	ss << m_lobSize;
	ss << ", ";
	ss << ")";
	return ss.str();
}
string LobInsertTest::getDescription() const {
	return "Lob Insert Performance";
}

void LobInsertTest::run() {
	InsertOneTime();
}

/** �����ж��ټ�¼ */
u64 LobInsertTest::getRecCount() {
	return m_totalRecSizeLoaded / (m_lobSize + sizeof(u64));

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
	Session *session = m_db->getSessionManager()->allocSession("LobInsertTest::loadData", conn);
	if (m_isMemoryCase) {
		m_totalRecSizeLoaded = m_dataSize;
		m_recCntLoaded = getRecCount();
		m_records = PaperTable::populate(session, m_table, &m_totalRecSizeLoaded, m_lobSize, m_recCntLoaded);
	}
	else {
		//���д�������
		m_totalRecSizeLoaded = m_dataSize;
		m_recCntLoaded = getRecCount();
		m_recCntLoaded  = (m_recCntLoaded + (THREAD_COUNT  - 1)) / THREAD_COUNT * THREAD_COUNT;
		m_totalRecSizeLoaded = m_recCntLoaded * (m_lobSize + sizeof(u64));
		m_records  = PaperTable::populate(session, m_table, m_lobSize, THREAD_COUNT);
	}

	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(conn);
	closeDatabase();
	*totalRecSize = 0;
	*recCnt = 0;
	m_opCnt = m_recCntLoaded;
	m_opDataSize = m_totalRecSizeLoaded;
	m_totalRecSizeLoaded = 0;
	m_recCntLoaded = 0;
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
	if (m_isMemoryCase) { 
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
	m_db->closeTable(session, m_table);
	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(conn);
	// delete m_countTab;
	m_table = NULL;
	m_db->close();
	delete m_db;
	m_db = NULL;

	//��������
	if (m_isMemoryCase) {
		for(uint i = 0; i<m_opCnt; i++) { 
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
	delete m_threads;
}

