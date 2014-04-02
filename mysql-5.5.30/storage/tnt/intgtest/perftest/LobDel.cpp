#include "MemoryTableScan.h"
#include "DbConfigs.h"
#include "api/Table.h"
#include "api/Database.h"
#include "misc/Session.h"
#include "misc/RecordHelper.h"
#include "mms/Mms.h"
#include "PaperTable.h"
#include "LobDel.h"
#include <sstream>
#include <string>

using namespace ntseperf;
using namespace std;


LobDelTest::LobDelTest(bool useMms, u64 dataSize, u32 lobSize, bool inMemory) {
	m_enableCache = true;
	m_useMms = useMms;
	m_dataSize = dataSize;
	m_lobSize = lobSize;
	m_inMemory = inMemory;
	setConfig(CommonDbConfig::getMedium());
	setTableDef(PaperTable::getTableDef());
}

string LobDelTest::getName() const {
	stringstream ss;
	ss << "LobDelTest(";
	if (m_useMms)
		ss << "m_useMms,";
	if (m_inMemory)
		ss << "m_inMemory,";
	ss << m_dataSize;
	ss << ",";
	ss << m_lobSize;
	ss << ")";
	return ss.str();
}
string LobDelTest::getDescription() const {
	return "Lob Del Performance";
}

void LobDelTest::run() {
	delOneTime();
}

/** 
 * �����¼
 * @post ���ݿ�״̬Ϊ�ر�
 */
void LobDelTest::loadData(u64 *totalRecSize, u64 *recCnt){
	// �������ݿ�ͱ�
	openTable(true);

	// ����Session
	Connection *conn = m_db->getConnection(false);
	Session *session = m_db->getSessionManager()->allocSession("LobDelTest::loadData", conn);

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
void LobDelTest::warmUp() {

	openTable(false);
	m_threads = new LobTestThread <LobDelTest> *[DEL_THREAD_COUNT];
	//����100���߳�
	for (u16 i = 0; i < DEL_THREAD_COUNT; i++) {
		stringstream ss; 
		ss << i;
		string str = string ("DelThread") + ss.str();
		m_threads[i] =  new LobTestThread<LobDelTest>(str.c_str(), i, this);
	}
}

/**
 * ���Ե���Ҫ����������Ϊ�˷������ɽӿ�
 * 
 * @param tid Ѱ�Ҽ�¼������Ĳ�����������߳���
 */
void LobDelTest::test(u16 tid ) {
	delRecord(tid);
}

/**
 * ɾ���������ļ�¼
 * 
 * @param tid Ѱ�Ҽ�¼������Ĳ�����������߳���
 */
void LobDelTest::delRecord(u16 tid) {

	Connection *conn = m_db->getConnection(false);
	Session *session = m_db->getSessionManager()->allocSession("LobDelTest::delRecord", conn);
	
	//ͨ��IndexScan��ɾ����Ӧ��¼
	u64 blogId = 0;
	u64 oneid;
	SubRecordBuilder keyBuilder(m_table->getTableDef(), KEY_PAD);
	SubRecord *key = keyBuilder.createSubRecordByName(PAPER_ID, &blogId);
	key->m_rowId = INVALID_ROW_ID;

	SubRecordBuilder srb(m_table->getTableDef(), REC_REDUNDANT);
	SubRecord *subRec = srb.createEmptySbById(m_table->getTableDef()->m_maxRecSize, "0 1");
	IndexScanCond cond(0, key, true, true, false);
	TblScan *scanHandle = m_table->indexScan(session, OP_DELETE, &cond, subRec->m_numCols, subRec->m_columns);
	while (m_table->getNext(scanHandle, subRec->m_data)) {
		ColumnDef *columnDef = m_table->getTableDef()->m_columns[0];
		oneid = *(u64 *)(subRec->m_data + columnDef->m_offset);
		if (oneid == tid)
			 m_table->deleteCurrent(scanHandle);
		oneid += DEL_THREAD_COUNT;
	}
	m_table->endScan(scanHandle);
	freeSubRecord(subRec);
	freeSubRecord(key);
	
	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(conn);
}

/**
 * ����ɾ�������
 */
void LobDelTest::delOneTime() {


	//�������е��߳�
	for (uint i = 0; i< DEL_THREAD_COUNT; i++) {
		m_threads[i]->start();
	}

	//�ȴ�����
	for (uint i = 0; i< DEL_THREAD_COUNT; i++) {
		m_threads[i]->join();
	}
}

void LobDelTest::tearDown() {
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
	for (uint i = 0; i< DEL_THREAD_COUNT; i++) {
		delete m_threads[i];
		m_threads[i] = NULL;
	}
	delete m_threads;
}

