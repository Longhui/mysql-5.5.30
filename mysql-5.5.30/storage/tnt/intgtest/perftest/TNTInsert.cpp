#include "TNTInsert.h"
#include "api/Table.h"
#include "TNTAccountTable.h"
#include "TNTLongCharTable.h"
#include "TNTCountTable.h"
#include "misc/RecordHelper.h"
#include "TNTTableHelper.h"
#include "TNTDbConfigs.h"
#include <sstream>
#include <iostream>

namespace tntperf {
/** TNT���¼���� */
class TNTFLRInserter : public Thread {
public:
	TNTFLRInserter();
	virtual void init(uint tid, const u64 idList[], TestTable testTbl, const char *tableName, TNTDatabase *db);
protected:
	uint		m_tid;				/** �̵߳�TID */
	const u64	*m_recID;			/** �����¼��ID�б� */
	const char  *m_tableName;
	TNTTable	*m_table;			/** �� */
	TNTDatabase	*m_db;				/** ���ݿ� */
	TestTable	m_testTable;		/** �����ñ� */
	static const uint  BATCH_OP_SIZE = 1000;

	virtual void run();
};

//TNTFLRInserter::BATCH_OP_SIZE = 100;

/*** TNT���������߳� ***/
TNTFLRInserter::TNTFLRInserter() : Thread("TNTFLRInserter Thread"){
	m_tid = 0;
	m_recID = NULL;
	m_tableName = NULL;
	m_table = NULL;
	m_db = NULL;
}

void TNTFLRInserter::run() {
	Record *rec = NULL;
	switch (m_testTable) {
		case COUNT_TABLE:
			rec = TNTAccountTable::createRecord(0, 0);
			break;
		case LONGCHAR_TABLE:
			rec = TNTLongCharTable::createRecord(0);
			break;
		default:
			assert(false);
	}
	RowId rid;
	uint dupIdx = (uint)(-1);

	TNTOpInfo opInfo;
	initTNTOpInfo(&opInfo, TL_X);
	Database *ntsedb = m_db->getNtseDb();
	Connection *conn = ntsedb->getConnection(false);
	Session *session = ntsedb->getSessionManager()->allocSession("TNTInserter::run", conn);
	TNTTransaction *trx = NULL;

	int i = 0;
	while (m_recID[i] != (u64)-1) {
		trx = startTrx(m_db->getTransSys(), session, true);
		m_table = m_db->openTable(session, m_tableName);
		while (m_recID[i] != (u64)-1) {
			switch (m_testTable) {
				case COUNT_TABLE:
					TNTCountTable::updateId(rec, m_recID[i]);
					break;
				case LONGCHAR_TABLE:
					TNTLongCharTable::updateId(rec, m_recID[i]);
					break;
				default:
					assert(false);
			}
			
			rid = m_table->insert(session, rec->m_data, &dupIdx, &opInfo);
			assert(dupIdx == (uint)-1);
			i++;
			//i++�󲻿���Ϊ0
			if (i % BATCH_OP_SIZE == 0) {
				break;
			}
		}
		m_db->closeTable(session, m_table);
		commitTrx(m_db->getTransSys(), trx);
	}

	ntsedb->getSessionManager()->freeSession(session);
	ntsedb->freeConnection(conn);
	freeRecord(rec);
}

/** ��ʼ��һ��TNTTable�����̵߳Ĳ��� **/
void TNTFLRInserter::init(uint tid, const u64 idList[], TestTable testTbl, const char *tableName, TNTDatabase *db) {
	m_tid = tid;
	m_recID = idList;
	m_tableName = tableName;
	m_db = db;
	m_testTable = testTbl;
}

/*** �䳤��¼�����߳� ***/
class TNTVLRInserter : public TNTFLRInserter {
public:
	TNTVLRInserter(): TNTFLRInserter() {};
	virtual void init(uint tid, const u64 idList[], TestTable testTbl, const char *tableName, TNTDatabase *db, u64* dataSize) {
		((TNTFLRInserter*)this)->init(tid, idList, testTbl, tableName, db);
		m_dataSize = dataSize;
	}
protected:
	u64 *m_dataSize; /** ���������� */
	virtual void run();
};


void TNTVLRInserter::run() {
	static const int count = 32;// ���Ǵ���16����¼

	Record *recs[count]; 
	int recLen[count];
	assert(m_testTable == ACCOUNT_TABLE);
	for (int i = 0; i < count; ++i) {
		recs[i] = TNTAccountTable::createRecord(0, recLen + i, 150, 200);
	}
	*m_dataSize = 0;

	RowId rid;
	uint dupIdx = (uint)(-1);
	TNTOpInfo opInfo;
	initTNTOpInfo(&opInfo, TL_X);
	Database *db = m_db->getNtseDb();
	Connection *conn = db->getConnection(false);
	Session *session = db->getSessionManager()->allocSession("TNTVLRInserter::run", conn);
	TNTTransaction *trx = NULL;

	int i = 0;
	while (m_recID[i] != (u64)-1) {
		trx = startTrx(m_db->getTransSys(), session);
		m_table = m_db->openTable(session, m_tableName);
		while (m_recID[i] != (u64)-1) {
			TNTAccountTable::updateId(recs[i % count], m_recID[i]);
			rid = m_table->insert(session, recs[i % count]->m_data, &dupIdx, &opInfo);
			assert(dupIdx == (uint)-1);
			*m_dataSize += recLen[i % count];
			i++;
			//i++�󲻿���Ϊ0
			if (i % BATCH_OP_SIZE == 0) {
				break;
			}
		}
		m_db->closeTable(session, m_table);
		commitTrx(m_db->getTransSys(), trx);
	}
	db->getSessionManager()->freeSession(session);
	db->freeConnection(conn);

	for (int i = 0; i < count; ++i) {
		freeRecord(recs[i]);
	}
}

//TNT������
TNTInsertPerfTest::TNTInsertPerfTest(const char * tableName, Scale scale, uint threadCount, Order idOrder, double dataSizeFact)
{
	/* ���ݱ����жϱ���� */
	m_tableName = tableName;
	if (0 == strcmp(m_tableName, TNTTABLE_NAME_COUNT))
		m_testTable = COUNT_TABLE;
	else if (0 == strcmp(m_tableName, TNTTABLE_NAME_LONGCHAR))
		m_testTable = LONGCHAR_TABLE;
	else if (0 == strcmp(m_tableName, TNTTABLE_NAME_ACCOUNT))
		m_testTable = ACCOUNT_TABLE;
	else
		assert(false);
	m_scale = scale;
	m_threadCnt = threadCount;
	m_idOrder = idOrder;
	m_dataSizeFact = dataSizeFact;
	m_tntRatio = 0.7;
}

TNTInsertPerfTest::~TNTInsertPerfTest(void)
{
}

string TNTInsertPerfTest::getName() const {
	stringstream ss;
	ss << "TNT Insert Test: ";
	ss << m_threadCnt;
	ss << " thread";
	if (m_threadCnt > 1) ss <<"s";
	ss << " on table ";
	switch (m_testTable) {
		case COUNT_TABLE:
			ss << "TNTCount ";
			break;
		case LONGCHAR_TABLE:
			ss << "TNTLongChar ";
			break;
		case ACCOUNT_TABLE:
			ss << "TNTAccount ";
			break;
	}

	switch (m_scale) {
		case SMALL:
			ss << "[Small] ";
			break;
		case MEDIUM:
			ss << "[Medium] ";
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

string TNTInsertPerfTest::getDescription() const {
	return "TNT Insert operation performance test.";
}

void TNTInsertPerfTest::run() {
	switch (m_testTable) {
		case COUNT_TABLE:
		case LONGCHAR_TABLE:
			{
				/* ���������߳� */
				u64 *idArray = new u64[m_recCnt + m_threadCnt];
				TNTFLRInserter *inserters = new TNTFLRInserter[m_threadCnt];

				for (uint i = 0; i < m_threadCnt; ++i) {
					/* ����recID���� */
					u64 *idList = idArray + i * (m_recCntPerThd + 1);
					/* �������ID���� */
					Generator::generateArray(idList, m_recCntPerThd, (u64)i, (u64)m_threadCnt, m_idOrder);
					/* ���һ��ID����Ϊ���ֵ����Ϊ�ս�ı�־ */
					idList[m_recCntPerThd] = (u64)-1;
					/* ��ʼ�������߳� */
					inserters[i].init(i, idList, m_testTable, m_tableName, m_db);
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
		case ACCOUNT_TABLE: //�䳤
			{
				u64 *idArray = new u64[m_recCnt + m_threadCnt];
				TNTVLRInserter *inserters = new TNTVLRInserter[m_threadCnt];
				u64 *dataSize = new u64[m_threadCnt];

				for (uint i = 0; i < m_threadCnt; ++i) {
					/* ����recID���� */
					u64 *idList = idArray + i * (m_recCntPerThd + 1);
					/* ���ID */
					Generator::generateArray(idList, m_recCntPerThd, (u64)i, (u64)m_threadCnt, m_idOrder);
					/* ID���� */
					idList[m_recCntPerThd] = (u64)-1;
					/* ��ʼ���߳� */
					inserters[i].init(i, idList, m_testTable, m_tableName, m_db, &dataSize[i]);
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
u64 TNTFLRInsertTest::getOpCnt() {
	return m_opCnt;
}
/** ���β��Ե������� */
u64 TNTFLRInsertTest::getDataSize() {
	return m_dataSize;
}

/** ���β���ռ�õ�����ʱ�� */
u64 TNTFLRInsertTest::getMillis() {
	return m_totalMillis;
}

/** 
 * �����¼, ��¼������Ϊntseҳ�����һ��
 * @post ���ݿ�״̬Ϊ�ر�
 */
void TNTInsertPerfTest::loadData(u64 *totalRecSize, u64 *recCnt) {
	Database *ntsedb = m_db->getNtseDb();
	// ����Session
	Connection *conn = ntsedb->getConnection(false);
	Session *session = ntsedb->getSessionManager()->allocSession("TNTInsertPerfTest::loadData", conn);
	//�������ȿ���ntse buffer��С����Ϊtnt bufferֻռ����hash index
	m_dataSize = (u64)(m_cfg->m_ntseConfig.m_pageBufSize * Limits::PAGE_SIZE * m_dataSizeFact);
	// ���������������¼��
	createTableAndRecCnt(session);

	m_recCntPerThd = (uint) (m_recCnt / m_threadCnt);

	ntsedb->getSessionManager()->freeSession(session);
	ntsedb->freeConnection(conn);

	*recCnt = 0;
	*totalRecSize = 0;

	m_opCnt = m_recCntPerThd * m_threadCnt;
	m_dataSize = m_opCnt * getRecordSize();
}
/** 
 * ����һ���ɨ��
 * @post ���ݿ�״̬Ϊopen
 */
void TNTInsertPerfTest::warmUp() {
	if (m_dataSizeFact >= 1.0) {
		return;
	}

	/* ���������С��buffer size������Ϊ������cpu���ԣ�Ԥ������ */
	Record *rec = NULL;
	switch (m_testTable) {
		case COUNT_TABLE:
			rec = TNTCountTable::createRecord(0, 0);
			rec->m_format = REC_FIXLEN;
			break;
	}

	Database *ntsedb = m_db->getNtseDb();
	Connection *conn = ntsedb->getConnection(false);
	Session *session = ntsedb->getSessionManager()->allocSession("TNTInsertPerfTest::warmup", conn);
	TNTTransaction *trx = startTrx(m_db->getTransSys(), session);
	m_table = m_db->openTable(session, m_tableName);

	vector<RowId> ridVec;
	RowId rid;
	/*TNTOpInfo opInfo;
	initTNTOpInfo(&opInfo, TL_X);*/
	// ����
	for (u64 i = 0; i < m_recCntPerThd * m_threadCnt; ++i) {
		rid = m_table->getNtseTable()->getHeap()->insert(session, rec, NULL);
		ridVec.push_back(rid);
	}
	// ɾ��
	for (u64 i = 0; i < ridVec.size(); ++i) {
		bool success = m_table->getNtseTable()->getHeap()->del(session, ridVec[i]);
		assert(success);
	}
	m_db->closeTable(session, m_table);
	commitTrx(m_db->getTransSys(), trx);
	ntsedb->getSessionManager()->freeSession(session);
	ntsedb->freeConnection(conn);
	ridVec.clear();
}

void TNTInsertPerfTest::setUp() {
	m_cfg = getTNTConfig();
	if (m_cfg->m_ntseConfig.m_maxSessions < m_threadCnt) {
		m_cfg->m_ntseConfig.m_maxSessions = (u16)m_threadCnt;
	}
	ntse::File dir(m_cfg->m_ntseConfig.m_basedir);
	dir.rmdir(true);
	dir.mkdir();
	m_db = TNTDatabase::open(m_cfg, true, 0);
}

void TNTInsertPerfTest::tearDown() {
	m_db->close();
	delete m_db;
	m_db = NULL;
	ntse::File dir(m_cfg->m_ntseConfig.m_basedir);
	dir.rmdir(true);

	delete m_cfg;
}

/** ���ݲ�����ȡ��ͬ��С���������� **/
TNTConfig *TNTInsertPerfTest::getTNTConfig() {
	TNTConfig *ret = NULL;
	switch (m_scale) {
		case SMALL:
			ret = new TNTConfig(TNTCommonDbConfig::getSmall());
			ret->m_tntBufSize = 64*1024*1024/TNTIMPage::TNTIM_PAGE_SIZE;
			ret->m_ntseConfig.m_pageBufSize = 2*ret->m_tntBufSize;
			ret->m_ntseConfig.m_commonPoolSize = 128*1024*1024/TNTIMPage::TNTIM_PAGE_SIZE;
			break;
		case MEDIUM:
			ret = new TNTConfig(TNTCommonDbConfig::getMedium());
			ret->m_tntBufSize = 512*1024*1024/TNTIMPage::TNTIM_PAGE_SIZE;
			ret->m_ntseConfig.m_pageBufSize = 2*ret->m_tntBufSize;
			ret->m_ntseConfig.m_commonPoolSize = 512*1024*1024/TNTIMPage::TNTIM_PAGE_SIZE;
			break;
	}

	if (ret != NULL) {
		ret->m_purgeInterval = 10*60*60;
		ret->m_purgeThreshold = 99;
		ret->m_dumpInterval = 10*60*60;
		ret->m_dumponRedoSize = 1024*1024*1024;
		ret->m_ntseConfig.m_logLevel = EL_LOG;
	}

	return ret;
}


/**
 * �������Ҹ��������������¼��
 */
void TNTInsertPerfTest::createTableAndRecCnt(Session *session) {
	TNTTransaction *trx = startTrx(m_db->getTransSys(), session);
	switch (m_testTable) {
		case COUNT_TABLE:
			m_db->createTable(session, TNTTABLE_NAME_COUNT, new TableDef(TNTCountTable::getTableDef(false)));
			m_recCnt = m_dataSize / TNTCountTable::getRecordSize();
			break;
		case LONGCHAR_TABLE:
			m_db->createTable(session, TNTTABLE_NAME_LONGCHAR, new TableDef(TNTLongCharTable::getTableDef(false)));
			m_recCnt = m_dataSize / TNTLongCharTable::getRecordSize();
			break;
		case ACCOUNT_TABLE:
			m_db->createTable(session, TNTTABLE_NAME_ACCOUNT, new TableDef(TNTAccountTable::getTableDef(false)));
			m_recCnt = m_dataSize / 200; // ƽ��200
			break;
	}
	commitTrx(m_db->getTransSys(), trx);
}

/**
 * ���ƽ����¼����
 */
uint TNTInsertPerfTest::getRecordSize() {
	switch (m_testTable) {
		case COUNT_TABLE:
			return TNTCountTable::getRecordSize();
		case LONGCHAR_TABLE:
			return TNTLongCharTable::getRecordSize();
		default:
			assert(ACCOUNT_TABLE == m_testTable);
			return 200;
	}
}

}