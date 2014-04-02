#include "Delete.h"
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
#include "util/File.h"
#include "TableHelper.h"


using namespace ntseperf;
using namespace std;

#define INFO_TBL_NAME "RowId_backup"

/**
 * 获取测试用例名称
 */
string DeletePerfTest::getName() const {
	stringstream ss;
	ss << "Delete Test, ";
	ss << m_threadCnt << " threads on ";
	switch (m_scale) {
		case SMALL:
			ss << "small config, IO consuming test.";
			break;
		case MEDIUM:
			ss << "medium config, CPU consuming test.";
			break;
	}
	return ss.str();
}


string DeletePerfTest::getDescription() const {
	return "Delete operation performance test.";
}

/** 本次测试的操作数 **/
u64 DeletePerfTest::getOpCnt() {
	return m_threadCnt * m_recCntPerThd;
}
/** 本次测试的数据量 **/
u64 DeletePerfTest::getDataSize() {
	return getOpCnt() * 200;
}

/** 本次测试占用的运行时间 **/
u64 DeletePerfTest::getMillis() {
	return m_totalMillis;
}

void DeletePerfTest::setUp() {
	switch (m_scale) {
		case SMALL:
			m_cfg = new Config(CommonDbConfig::getSmall());
			break;
		case MEDIUM:
			m_cfg = new Config(CommonDbConfig::getMedium());
			break;
	}
	if (m_cfg->m_maxSessions < m_threadCnt) m_cfg->m_maxSessions = m_threadCnt;

	Database::drop(m_cfg->m_basedir);
	m_db = Database::open(m_cfg, true);


	u64 dataSize = m_cfg->m_pageBufSize * Limits::PAGE_SIZE * m_dataSizeFact;

	m_recCntPerThd = dataSize /2 / 200 / m_threadCnt;
	m_recCnt = (dataSize / 200 / m_threadCnt) * m_threadCnt;
	m_db->close();
}

void DeletePerfTest::loadData(u64 *totalRecSize, u64 *recCnt) {
	m_db = Database::open(m_cfg, true);

	Connection *conn = m_db->getConnection(false);
	Session *session = m_db->getSessionManager()->allocSession("DeletePerfTest::loadData", conn);
	m_db->createTable(session, TABLE_NAME_ACCOUNT, new TableDef(AccountTable::getTableDef(true)));
	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(conn);

	conn = m_db->getConnection(false);
	session = m_db->getSessionManager()->allocSession("DeletePerfTest::loadData", conn);
	m_currTab = m_db->openTable(session, TABLE_NAME_ACCOUNT);

	Connection *conn2 = m_db->getConnection(false);
	Session *sess2 = m_db->getSessionManager()->allocSession("DeletePerfTest::loadData", conn2);
	m_db->createTable(sess2, INFO_TBL_NAME, new TableDef(CountTable::getTableDef(false)));
	Table *infoTbl = m_db->openTable(sess2, INFO_TBL_NAME);

	m_dataSize = 0;
	for (u64 i = 0; i < m_recCnt; ++i) {
		int recSize;
		Record *rec = AccountTable::createRecord(i, &recSize, 150, 200);
		uint dupIdx;
		m_dataSize += recSize;
		RowId rid = m_currTab->insert(session, rec->m_data, &dupIdx);
		freeRecord(rec);
		Record * infoRec = CountTable::createRecord(rid, recSize);
		infoTbl->insert(sess2, infoRec->m_data, &dupIdx);
		freeRecord(infoRec);
	}
	m_db->closeTable(sess2, infoTbl);
	m_db->getSessionManager()->freeSession(sess2);
	m_db->freeConnection(conn2);

	m_db->closeTable(session, m_currTab);
	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(conn);

	m_db->close();

	*totalRecSize = m_dataSize;
	*recCnt = m_recCnt;
}


void DeletePerfTest::warmUp() {
	m_db = Database::open(m_cfg, false);
	Connection *conn = m_db->getConnection(false);
	Session *session = m_db->getSessionManager()->allocSession("DeletePerfTest::warmUp", conn);
	m_currTab = m_db->openTable(session, TABLE_NAME_ACCOUNT);
	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(conn);

	m_dataSize = 0;
	Connection *conn2 = m_db->getConnection(false);
	Session *sess2 = m_db->getSessionManager()->allocSession("DeletePerfTest::warmUp", conn2);
	Table *infoTbl = m_db->openTable(sess2, INFO_TBL_NAME);
	SubRecordBuilder srb(infoTbl->getTableDef(), REC_REDUNDANT);
	u64 id = 0; int count = 0;
	SubRecord *info = srb.createSubRecordByName(COUNT_ID" "COUNT_COUNT, &id, &count);
	TblScan *scanHdl = infoTbl->tableScan(sess2, OP_READ, info->m_numCols, info->m_columns);
	while (infoTbl->getNext(scanHdl, info->m_data)) {
		id = RedRecord::readBigInt(infoTbl->getTableDef(), info->m_data, COUNT_ID_CNO);
		count = RedRecord::readInt(infoTbl->getTableDef(), info->m_data, COUNT_COUNT_CNO);
		m_IdRowIdVec.push_back((RowId)id);
		m_dataSize += count;
	}
	infoTbl->endScan(scanHdl);
	freeSubRecord(info);
	m_db->closeTable(sess2, infoTbl);
	m_db->getSessionManager()->freeSession(sess2);
	m_db->freeConnection(conn2);

	if (m_dataSizeFact < 1.0) {
		/* 关掉检查点和清道夫，避免IO操作 */
		m_db->setCheckpointEnabled(false);
		m_db->getPageBuffer()->disableScavenger();
		// 关掉日志
		m_db->getTxnlog()->enableFlusher(false);
		// 索引扫描
		indexFullScan(m_db, m_currTab);
	}
}


void DeletePerfTest::run() {
	Deleter **deleters = new Deleter*[m_threadCnt];

	for (uint i = 0; i < m_threadCnt; ++i) {
		deleters[i] = new Deleter(&m_IdRowIdVec, i, m_recCntPerThd, m_db, m_currTab);
	}

	u64 start = System::currentTimeMillis();
	for (uint i = 0; i < m_threadCnt; ++i) {
		(deleters[i])->start();
	}
	for (uint i = 0; i < m_threadCnt; ++i) {
		(deleters[i])->join();
	}
	u64 end = System::currentTimeMillis();

	m_totalMillis = end - start;

	for (uint i = 0; i < m_threadCnt; ++i) {
		delete deleters[i];
	}

	delete [] deleters;
}

void DeletePerfTest::tearDown() {

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


void Deleter::run() {
	Connection *conn = m_db->getConnection(false);
	Session *session = m_db->getSessionManager()->allocSession("Deleter::run", conn);

	byte buf[Limits::PAGE_SIZE];
	u16 readCols[1] = {0};
	TblScan *scanHdl = m_tbl->positionScan(session, OP_DELETE, 1, readCols);

	for (uint i = 0; i < m_delCnt; ++i) {
		u64 ID = i + m_threadId * m_delCnt;
		RowId rid = (*m_ridVec)[ID];
		m_tbl->getNext(scanHdl, buf, rid);
		m_tbl->deleteCurrent(scanHdl);
	}

	m_tbl->endScan(scanHdl);
	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(conn);
}