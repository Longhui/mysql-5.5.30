#include "Insert.h"
#include "DbConfigs.h"
#include "api/Table.h"
#include "api/Database.h"
#include "misc/Session.h"
#include "misc/RecordHelper.h"
#include "CountTable.h"
#include "LongCharTable.h"
#include "AccountTable.h"
#include "util/Thread.h"
#include <sstream>
#include <iostream>
#include "Generator.h"

using namespace ntseperf;
using namespace std;

/** ������¼���� */
class FLRInserter : public Thread {
public:
	FLRInserter();
	virtual void init(uint tid, const u64 idList[], TestTable testTbl, Table *table, Database *db);
protected:
	uint		m_tid;				/** �̵߳�TID */
	const u64	*m_recID;			/** �����¼��ID�б� */
	Table		*m_table;			/** �� */
	Database	*m_db;				/** ���ݿ� */
	TestTable	m_testTable;		/** �����ñ� */
	virtual void run();
};

/*** �����Ѳ�������߳� ***/
FLRInserter::FLRInserter() : Thread("FLRInserter Thread"){
	m_tid = 0;
	m_recID = NULL;
	m_table = NULL;
	m_db = NULL;
}

void FLRInserter::run() {
	Record *rec = NULL;
	switch (m_testTable) {
		case COUNT_TABLE:
			rec = CountTable::createRecord(0, 0);
			break;
		case LONGCHAR_TABLE:
			rec = LongCharTable::createRecord(0);
			break;
		default:
			assert(false);
	}
	RowId rid;
	uint dupIdx;

	Connection *conn = m_db->getConnection(false);
	Session *session = NULL;

	int i = 0;
	while (m_recID[i] != (u64)-1) {
		switch (m_testTable) {
			case COUNT_TABLE:
				CountTable::updateId(rec, m_recID[i]);
				break;
			case LONGCHAR_TABLE:
				LongCharTable::updateId(rec, m_recID[i]);
				break;
			default:
				assert(false);
		}
		session = m_db->getSessionManager()->allocSession("FLRInserter::run", conn);
		rid = m_table->insert(session, rec->m_data, &dupIdx);
		m_db->getSessionManager()->freeSession(session);
		++i;
	}
	m_db->freeConnection(conn);
	freeRecord(rec);
}

/** ��ʼ��һ�������Ѳ����̵߳Ĳ��� **/
void FLRInserter::init(uint tid, const u64 idList[], TestTable testTbl, Table *table, Database *db) {
	m_tid = tid;
	m_recID = idList;
	m_table = table;
	m_db = db;
	m_testTable = testTbl;
}


/*** �䳤��¼�����߳� ***/
class VLRInserter : public FLRInserter {
public:
	VLRInserter():FLRInserter() {};
	virtual void init(uint tid, const u64 idList[], TestTable testTbl, Table *table, Database *db, u64* dataSize) {
		((FLRInserter*)this)->init(tid, idList, testTbl, table, db);
		m_dataSize = dataSize;
	}
protected:
	u64 *m_dataSize; /** ���������� */
	virtual void run();
};


void VLRInserter::run() {
	static const int count = 32;// ���Ǵ���16����¼

	Record *recs[count]; 
	int recLen[count];
	assert(m_testTable == ACCOUNT_TABLE);
	for (int i = 0; i < count; ++i) {
		recs[i] = AccountTable::createRecord(0, recLen + i, 150, 200);
	}
	*m_dataSize = 0;
	
	RowId rid;
	uint dupIdx;
	Connection *conn = m_db->getConnection(false);
	Session *session = NULL;

	int i = 0;
	while (m_recID[i] != (u64)-1) {
		AccountTable::updateId(recs[i % count], m_recID[i]);

		session = m_db->getSessionManager()->allocSession("FLRInserter::run", conn);
		rid = m_table->insert(session, recs[i % count]->m_data, &dupIdx);
		m_db->getSessionManager()->freeSession(session);

		*m_dataSize += recLen[i % count];
		++i;
	}

	m_db->freeConnection(conn);

	for (int i = 0; i < count; ++i) {
		freeRecord(recs[i]);
	}
}




/*** ���ɲ�����Բ����Ŀ�� ***/
InsertPerfTest::InsertPerfTest(const char * tableName, HeapVersion ver, Scale scale, uint threadCount, Order idOrder, double dataSizeFact) {
	/* ���ݱ����жϱ���� */
	m_tableName = tableName;
	if (0 == strcmp(m_tableName, TABLE_NAME_COUNT))
		m_testTable = COUNT_TABLE;
	else if (0 == strcmp(m_tableName, TABLE_NAME_LONGCHAR))
		m_testTable = LONGCHAR_TABLE;
	else if (0 == strcmp(m_tableName, TABLE_NAME_ACCOUNT))
		m_testTable = ACCOUNT_TABLE;
	else
		assert(false);
	m_scale = scale;
	m_heapVer = ver;
	m_threadCnt = threadCount;
	m_idOrder = idOrder;
	m_dataSizeFact = dataSizeFact;
}

/** ���ز������������� **/
string InsertPerfTest::getName() const {
	stringstream ss;
	ss << "Insert Test: ";
	ss << m_threadCnt;
	ss << " thread";
	if (m_threadCnt > 1) ss <<"s";
	ss << " on table ";
	switch (m_testTable) {
		case COUNT_TABLE:
			ss << "Count ";
			break;
		case LONGCHAR_TABLE:
			ss << "LongChar ";
			break;
		case ACCOUNT_TABLE:
			ss << "Account ";
			break;
	}
	switch (m_idOrder) {
		case ASCENDANT:
			ss << "id order ascendant.";
				break;
		case DESCENDANT:
			ss << "id order descendant.";
				break;
		case RANDOM:
			ss << "id order random.";
				break;
	}
	return ss.str();
}

string InsertPerfTest::getDescription() const {
	return "Insert operation performance test.";
}

void InsertPerfTest::run() {
	switch (m_testTable) {
		case COUNT_TABLE:
		case LONGCHAR_TABLE:
			{
				/* ���������߳� */
				u64 *idArray = new u64[m_recCnt + m_threadCnt];
				FLRInserter *inserters = new FLRInserter[m_threadCnt];

				for (uint i = 0; i < m_threadCnt; ++i) {
					/* ����recID���� */
					u64 *idList = idArray + i * (m_recCntPerThd + 1);
					/* �������ID���� */
					Generator::generateArray(idList, m_recCntPerThd, (u64)i, (u64)m_threadCnt, m_idOrder);
					/* ���һ��ID����Ϊ���ֵ����Ϊ�ս�ı�־ */
					idList[m_recCntPerThd] = (u64)-1;
					/* ��ʼ�������߳� */
					inserters[i].init(i, idList, m_testTable, m_currTab, m_db);
				}

				/* ��ʱ */
				u64 startTime = System::currentTimeMillis();

				for (uint i = 0; i < m_threadCnt; ++i) {
					inserters[i].start();
				}
				for (uint i = 0; i < m_threadCnt; ++i) {
					inserters[i].join();
				}

				u64 endTime = System::currentTimeMillis();

				m_totalMillis = endTime - startTime;

				delete [] inserters;
				delete [] idArray;
			}
			break;
		case ACCOUNT_TABLE:
			{
				u64 *idArray = new u64[m_recCnt + m_threadCnt];
				VLRInserter *inserters = new VLRInserter[m_threadCnt];
				u64 *dataSize = new u64[m_threadCnt];

				for (uint i = 0; i < m_threadCnt; ++i) {
					/* ����recID���� */
					u64 *idList = idArray + i * (m_recCntPerThd + 1);
					/* ���ID */
					Generator::generateArray(idList, m_recCntPerThd, (u64)i, (u64)m_threadCnt, m_idOrder);
					/* ID���� */
					idList[m_recCntPerThd] = (u64)-1;
					/* ��ʼ���߳� */
					inserters[i].init(i, idList, m_testTable, m_currTab, m_db, &dataSize[i]);
				}
				/* ��ʱ */
				u64 startTime = System::currentTimeMillis();

				for (uint i = 0; i < m_threadCnt; ++i) {
					inserters[i].start();
				}
				for (uint i = 0; i < m_threadCnt; ++i) {
					inserters[i].join();
				}

				u64 endTime = System::currentTimeMillis();
				m_totalMillis = endTime - startTime;

				/* �䳤�ѵ�����������Ҫ��ȷ�ۼ� */
				m_dataSize = 0;
				for (int i = 0; i < m_threadCnt; ++i) {
					m_dataSize += dataSize[i];
				}

				delete [] dataSize;
				delete [] inserters;
				delete [] idArray;
			}
			break;
	}
}

/** ���β��ԵĲ����� */
u64 FLRInsertTest::getOpCnt() {
	return m_opCnt;
}
/** ���β��Ե������� */
u64 FLRInsertTest::getDataSize() {
	return m_dataSize;
}

/** ���β���ռ�õ�����ʱ�� */
u64 FLRInsertTest::getMillis() {
	return m_totalMillis;
}

/** 
 * �����¼, ��¼������Ϊntseҳ�����һ��
 * @post ���ݿ�״̬Ϊ�ر�
 */
void InsertPerfTest::loadData(u64 *totalRecSize, u64 *recCnt) {
	// �������ݿ�
	m_cfg = getConfig();
	if (m_cfg->m_maxSessions < m_threadCnt) m_cfg->m_maxSessions = m_threadCnt;
	Database::drop(m_cfg->m_basedir);
	m_db = Database::open(m_cfg, true);
	// ����Session
	Connection *conn = m_db->getConnection(false);
	Session *session = m_db->getSessionManager()->allocSession("InsertPerfTest::loadData", conn);
	m_dataSize = m_cfg->m_pageBufSize * Limits::PAGE_SIZE * m_dataSizeFact;
	// ���������������¼��
	createTableAndRecCnt(session);

	m_recCntPerThd = (uint) (m_recCnt / m_threadCnt);

	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(conn);

	/* �ر����ݿ� */
	m_db->close();
	delete m_db;

	*recCnt = 0;
	*totalRecSize = 0;

	m_opCnt = m_recCntPerThd * m_threadCnt;
	m_dataSize = m_opCnt * getRecordSize();
}
/** 
 * ����һ���ɨ��
 * @post ���ݿ�״̬Ϊopen
 */
void InsertPerfTest::warmUp() {
	m_db = Database::open(m_cfg, false);
	Connection *conn = m_db->getConnection(false);
	Session *session = m_db->getSessionManager()->allocSession("InsertPerfTest::warmup", conn);
	m_currTab = m_db->openTable(session, m_tableName);
	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(conn);

	if (m_dataSizeFact < 1.0) {
		/* ���������С��buffer size������Ϊ������cpu���ԣ�Ԥ������ */
		Record *rec;
		switch (m_testTable) {
			case COUNT_TABLE:
				rec = CountTable::createRecord(0, 0);
				rec->m_format = REC_FIXLEN;
				break;
		}

		vector<RowId> ridVec;
		RowId rid;
		conn = m_db->getConnection(false);
		session = m_db->getSessionManager()->allocSession("InsertPerfTest::warmup", conn);
		// ����
		for (u64 i = 0; i < m_recCntPerThd * m_threadCnt; ++i) {
			rid = m_currTab->getHeap()->insert(session, rec, NULL);
			ridVec.push_back(rid);
		}
		// ɾ��
		for (u64 i = 0; i < ridVec.size(); ++i) {
			bool success = m_currTab->getHeap()->del(session, ridVec[i]);
			assert(success);
		}
		m_db->getSessionManager()->freeSession(session);
		m_db->freeConnection(conn);
		ridVec.clear();
		/* �ص����������򣬱���IO���� */
		m_db->setCheckpointEnabled(false);
		m_db->getPageBuffer()->disableScavenger();
		// TODO: �ص���־
		m_db->getTxnlog()->enableFlusher(false);
	}

}


void InsertPerfTest::tearDown() {
	if (m_dataSizeFact < 1.0) {
		m_db->setCheckpointEnabled(true);
		m_db->getPageBuffer()->enableScavenger();
		m_db->getTxnlog()->enableFlusher(true);
	}
	
	Connection *conn = m_db->getConnection(false);
	Session *session = m_db->getSessionManager()->allocSession("InsertPerfTest::tearDown", conn);
	m_db->closeTable(session, m_currTab);
	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(conn);
	m_currTab = NULL;
	
	m_db->close();
	delete m_db;
	m_db = NULL;

	delete m_cfg;
}

/** ���ݲ�����ȡ��ͬ��С���������� **/
Config *InsertPerfTest::getConfig() {
	switch (m_scale) {
		case SMALL:
			return new Config(CommonDbConfig::getSmall());
			break;
		case MEDIUM:
			return new Config(CommonDbConfig::getMedium());
			break;
	}
}


/**
 * �������Ҹ��������������¼��
 */
void InsertPerfTest::createTableAndRecCnt(Session *session) {
	switch (m_testTable) {
		case COUNT_TABLE:
			m_db->createTable(session, TABLE_NAME_COUNT, new TableDef(CountTable::getTableDef(false)));
			m_recCnt = m_dataSize / CountTable::getRecordSize();
			break;
		case LONGCHAR_TABLE:
			m_db->createTable(session, TABLE_NAME_LONGCHAR, new TableDef(LongCharTable::getTableDef(false)));
			m_recCnt = m_dataSize / LongCharTable::getRecordSize();
			break;
		case ACCOUNT_TABLE:
			m_db->createTable(session, TABLE_NAME_ACCOUNT, new TableDef(AccountTable::getTableDef(false)));
			//m_recCnt = m_dataSize / AccountTable::
			m_recCnt = m_dataSize / 200; // ƽ��200
			break;
	}
}

/**
 * ���ƽ����¼����
 */
uint InsertPerfTest::getRecordSize() {
	switch (m_testTable) {
		case COUNT_TABLE:
			return CountTable::getRecordSize();
		case LONGCHAR_TABLE:
			return LongCharTable::getRecordSize();
		case ACCOUNT_TABLE:
			return 200;
	}
}


