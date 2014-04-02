#include "DbConfigs.h"
#include "api/Table.h"
#include "api/Database.h"
#include "misc/Session.h"
#include "misc/RecordHelper.h"
#include "mms/Mms.h"
#include "PaperTable.h"
#include "LobUpdate.h"
#include <sstream>
#include <string>

using namespace ntseperf;
using namespace std;

extern byte* creatLob(uint);

LobUpdateTest::LobUpdateTest(bool useMms, u64 dataSize, u32 lobSize, u32 upLobSize, bool inMemory) {
	m_enableCache = true;
	m_useMms = useMms;
	m_dataSize = dataSize;
	m_lobSize = lobSize;
	m_newLobLen = upLobSize;	
	setConfig(CommonDbConfig::getMedium());
	setTableDef(PaperTable::getTableDef());
}

string LobUpdateTest::getName() const {
	stringstream ss;
	ss << "LobUpdateTest(";
	if (m_useMms)
		ss << "useMMs,";
	if (m_inMemory)
		ss << "m_inMemory,";
	ss << m_dataSize;
	ss << ",";
	ss << m_lobSize;
	ss << ",";
	ss << m_newLobLen;
	ss << ")";
	return ss.str();
}
string LobUpdateTest::getDescription() const {
	return "Lob Update Performance";
}

void LobUpdateTest::run() {
	updateOneTime();
}


/** 
 * �����¼
 * @post ���ݿ�״̬Ϊ�ر�
 */
void LobUpdateTest::loadData(u64 *totalRecSize, u64 *recCnt){
	// �������ݿ�ͱ�
	openTable(true);

	// ����Session
	Connection *conn = m_db->getConnection(false);
	Session *session = m_db->getSessionManager()->allocSession("LobUpdateTest::loadData", conn);

	m_totalRecSizeLoaded = m_dataSize;
	m_recCntLoaded = PaperTable::populate(session, m_table, &m_totalRecSizeLoaded, m_lobSize, false);

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
void LobUpdateTest::warmUp() {

	openTable(false);
	m_threads = new LobTestThread <LobUpdateTest> *[UPDATE_THREAD_COUNT];
	//����100���߳�
	for (u16 i = 0; i < UPDATE_THREAD_COUNT; i++) {
		stringstream ss; 
		ss << i;
		string str = string ("UpdateThread") + ss.str();
		m_threads[i] =  new LobTestThread<LobUpdateTest>(str.c_str(), i, this);
	}
	//�����µĴ����
	m_lob = creatLob(m_newLobLen);
	m_opDataSize = m_opCnt * m_newLobLen;
	
}

/**
 * ���Ե���Ҫ����������Ϊ�˷������ɽӿ�
 * 
 * @param tid Ѱ�Ҽ�¼������Ĳ�����������߳���
 */
void LobUpdateTest::test(u16 tid ) {
	updateRecord(tid);
}

/**
 * ���º������ļ�¼
 * 
 * @param tid Ѱ�Ҽ�¼������Ĳ�����������߳���
 */
void LobUpdateTest::updateRecord(u16 tid) {

	Connection *conn = m_db->getConnection(false);
	Session *session = m_db->getSessionManager()->allocSession("LobUpdateTest::updateRecord", conn);
    
	//ͨ��IndexScan�����¶�Ӧ��¼
	uint index = tid;
	SubRecordBuilder keyBuilder(m_table->getTableDef(), KEY_PAD);
	SubRecord *key = keyBuilder.createSubRecordByName(PAPER_ID, &index);
	key->m_rowId = INVALID_ROW_ID;

	SubRecordBuilder srb(m_table->getTableDef(), REC_REDUNDANT);
	SubRecord *subRec = srb.createEmptySbById(m_table->getTableDef()->m_maxRecSize, "0 1");
	IndexScanCond cond(0, key, true, true, false);
	TblScan *scanHandle = m_table->indexScan(session, OP_UPDATE, &cond, subRec->m_numCols, subRec->m_columns);
	u16 columns2[1] = {1}; 
	scanHandle->setUpdateColumns(1, columns2);
	ColumnDef *columnDef = m_table->getTableDef()->m_columns[0];
	while(m_table->getNext(scanHandle, subRec->m_data)) {
		u64 oneid = *(u64 *)(subRec->m_data + columnDef->m_offset);
		if (oneid == index)  {
			SubRecord *updateRec = srb.createSubRecordById("");
			RecordOper::writeLob(updateRec->m_data, m_table->getTableDef()->m_columns[1], (byte *)m_lob);
			RecordOper::writeLobSize(updateRec->m_data, m_table->getTableDef()->m_columns[1], (uint)m_newLobLen);
			m_table->updateCurrent(scanHandle, updateRec->m_data);
			index +=  UPDATE_THREAD_COUNT;
		}
	}
	m_table->endScan(scanHandle);
	freeSubRecord(subRec);

	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(conn);
}

/**
 * ���Ը��´����
 */
void LobUpdateTest::updateOneTime() {


	//�������е��߳�
	for (uint i = 0; i< UPDATE_THREAD_COUNT; i++) {
		m_threads[i]->start();
	}

	//�ȴ�����
	for (uint i = 0; i< UPDATE_THREAD_COUNT; i++) {
		m_threads[i]->join();
	}
}

void LobUpdateTest::tearDown() {
	Connection *conn = m_db->getConnection(false);
	Session *session = m_db->getSessionManager()->allocSession("LobDelTest::tearDown", conn);
	m_db->closeTable(session, m_table);
	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(conn);
	// delete m_countTab;
	m_table = NULL;
	m_db->close();
	delete m_db;
	m_db = NULL;

	//�����߳�
	for (uint i = 0; i< UPDATE_THREAD_COUNT; i++) {
		delete m_threads[i];
		m_threads[i] = NULL;
	}
	delete m_threads;

	//��������
	delete[] m_lob;
}

