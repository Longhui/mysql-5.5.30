#include "TNTUpdate.h"
#include "Random.h"
#include "Generator.h"
#include "api/Table.h"
#include "misc/RecordHelper.h"
#include "TNTTableHelper.h"
#include "TNTDbConfigs.h"
#include <sstream>
#include <iostream>

using namespace std;

#define INFO_TBL_NAME "RowId_backup"
#define PKEY_TBL_NAME "Id_backup"

namespace tntperf {

static TNTConfig * getTNTConfig(Scale scale) {
	TNTConfig *ret = NULL;
	switch (scale) {
		case SMALL:
			ret = new TNTConfig(TNTCommonDbConfig::getSmall());
			ret->m_tntBufSize = 32*1024*1024/TNTIMPage::TNTIM_PAGE_SIZE;
			ret->m_ntseConfig.m_pageBufSize = 2*ret->m_tntBufSize;
			ret->m_ntseConfig.m_commonPoolSize = 128*1024*1024/TNTIMPage::TNTIM_PAGE_SIZE;
		case MEDIUM:
			ret = new TNTConfig(TNTCommonDbConfig::getMedium());
			ret->m_tntBufSize = 512*1024*1024/TNTIMPage::TNTIM_PAGE_SIZE;
			ret->m_ntseConfig.m_pageBufSize = 2*ret->m_tntBufSize;
			ret->m_ntseConfig.m_commonPoolSize = 512*1024*1024/TNTIMPage::TNTIM_PAGE_SIZE;
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

class TNTFLRUpdater: public Thread  {
public:
	TNTFLRUpdater(vector<RowId>* ridVec, TNTDatabase *db, const char *tblName, uint tid, u64 maxID, int loop): Thread("TNTFLRUpdater") {
		m_maxID = maxID;
		u64 *IDs = new u64[maxID + 1];
		Generator::generateArray(IDs, maxID + 1, (u64)0, (u64)1, RANDOM);
		for (int i = 0; i < sizeof(m_ID) / sizeof(m_ID[0]); ++i) {
			m_ID[i] = IDs[i];
		}
		delete [] IDs;
		sort(m_ID, m_ID + sizeof(m_ID) / sizeof(m_ID[0]) - 1);

		m_ridVec = ridVec;
		m_db = db;
		m_tableName = tblName;
		m_tbl = NULL;
		m_tid = tid;
		m_loop = loop;
	}
protected:
	virtual void run();
	u64 m_maxID;
	u64 m_ID[100];

	vector<RowId> *m_ridVec;
	TNTDatabase *m_db;
	TNTTable *m_tbl;
	const char *m_tableName;
	int m_loop;
	uint m_tid;
};

void TNTFLRUpdater::run() {
	Database *ntsedb = m_db->getNtseDb();
	Connection *conn = ntsedb->getConnection(false);
	Session *session = ntsedb->getSessionManager()->allocSession("TNTFLRUpdater::run", conn);

	byte buf[Limits::PAGE_SIZE];
	u16 updCols[1] = {COUNT_ID_CNO};

	u64 newID, ID = 0;
	TNTOpInfo opInfo;
	initTNTOpInfo(&opInfo, TL_X);
	TNTTransaction *trx = startTrx(m_db->getTransSys(), session);
	m_tbl = m_db->openTable(session, m_tableName);
	SubRecordBuilder srb(m_tbl->getNtseTable()->getTableDef(), REC_REDUNDANT);
	SubRecord *updRec = srb.createSubRecordByName(COUNT_ID, &ID);

	TNTTblScan *scanHdl = m_tbl->positionScan(session, OP_UPDATE, &opInfo, 1, updCols);
	scanHdl->setUpdateSubRecord(1, updCols);
	for (int i = 0; i < m_loop; ++i) {
		newID = m_maxID + 1 + m_loop * m_tid + i;
		ID = RandomGen::nextInt(0, sizeof(m_ID) / sizeof(m_ID[0]));

		m_tbl->getNext(scanHdl, buf, (*m_ridVec)[m_ID[ID]]);
		RedRecord::writeNumber(m_tbl->getNtseTable()->getTableDef(), COUNT_ID_CNO, updRec->m_data, newID);
		NTSE_ASSERT(m_tbl->updateCurrent(scanHdl, updRec->m_data));
	}
	m_tbl->endScan(scanHdl);
	m_db->closeTable(session, m_tbl);
	commitTrx(m_db->getTransSys(), trx);
	ntsedb->getSessionManager()->freeSession(session);
	ntsedb->freeConnection(conn);
	m_tbl = NULL;

	freeSubRecord(updRec);
}

class TNTVLRUpdater: public TNTFLRUpdater  {
public:
	TNTVLRUpdater(vector<RowId>* ridVec, TNTDatabase *db, const char *tblName, uint tid, u64 maxID, int loop): 
	  TNTFLRUpdater(ridVec, db, tblName, tid, maxID, loop) {
	}
protected:
	virtual void run();
};

void TNTVLRUpdater::run() {
	Database *ntsedb = m_db->getNtseDb();
	Connection *conn = ntsedb->getConnection(false);
	Session *session = ntsedb->getSessionManager()->allocSession("TNTVLRUpdater::run", conn);

	byte buf[Limits::PAGE_SIZE];
	u16 updCols[1] = {ACCOUNT_ID_CNO};
	u64 newID, ID = 0;

	TNTOpInfo opInfo;
	initTNTOpInfo(&opInfo, TL_X);
	TNTTransaction *trx = startTrx(m_db->getTransSys(), session);
	m_tbl = m_db->openTable(session, m_tableName);
	SubRecordBuilder srb(m_tbl->getNtseTable()->getTableDef(), REC_REDUNDANT);
	SubRecord *updRec = srb.createSubRecordByName(ACCOUNT_ID, &ID);
	TNTTblScan *scanHdl = m_tbl->positionScan(session, OP_UPDATE, &opInfo, 1, updCols);
	scanHdl->setUpdateSubRecord(1, updCols);
	for (int i = 0; i < m_loop; ++i) {
		newID = m_maxID + 1 + m_loop * m_tid + i;
		ID = RandomGen::nextInt(0, sizeof(m_ID) / sizeof(m_ID[0]));

		m_tbl->getNext(scanHdl, buf, (*m_ridVec)[m_ID[ID]]);
		RedRecord::writeNumber(m_tbl->getNtseTable()->getTableDef(), ACCOUNT_ID_CNO, updRec->m_data, newID);
		NTSE_ASSERT(m_tbl->updateCurrent(scanHdl, updRec->m_data));
	}
	m_tbl->endScan(scanHdl);
	m_db->closeTable(session, m_tbl);
	commitTrx(m_db->getTransSys(), trx);
	ntsedb->getSessionManager()->freeSession(session);
	ntsedb->freeConnection(conn);

	m_tbl = NULL;
	freeSubRecord(updRec);
}

string TNTFLRUpdatePerfTest::getName() const {
	stringstream ss;
	ss << "TNT FLR Update concurrency update primary key on table " << m_tblName;
	if (m_scale == SMALL) {
		ss << "[Small]";
	} else if (m_scale == MEDIUM) {
		ss << "[Medium]";
	} else {
		assert(false);
	}
	
	if (m_useMms) {
		ss << ", with mms.";
	} else { 
		ss << ", without mms.";
	}

	return ss.str();
}


string TNTFLRUpdatePerfTest::getDescription() const {
	stringstream ss;
	ss << "TNT FLR Update primary key performance test.";
	return ss.str();
}

void TNTFLRUpdatePerfTest::setUp() {
	m_cfg = getTNTConfig(m_scale);
	if (m_cfg->m_ntseConfig.m_maxSessions < m_threadCnt) {
		m_cfg->m_ntseConfig.m_maxSessions = m_threadCnt;
	}

	m_recCnt = m_cfg->m_ntseConfig.m_pageBufSize * Limits::PAGE_SIZE / TNTCountTable::getRecordSize();
	m_maxID = m_recCnt - 1;
}

void TNTFLRUpdatePerfTest::loadData(u64 *totalRecSize, u64 *recCnt) {
	TNTDatabase::drop(m_cfg->m_ntseConfig.m_basedir);
	m_db = TNTDatabase::open(m_cfg, true, 0);
	Database *ntsedb = m_db->getNtseDb();

	Connection *conn = ntsedb->getConnection(false);
	Session *session = ntsedb->getSessionManager()->allocSession("TNTFLRUpdatePerfTest::loadData", conn);
	TableDef *tbDef = new TableDef(TNTCountTable::getTableDef(m_useMms));
	TNTTransaction *trx1 = startTrx(m_db->getTransSys(), session);
	m_db->createTable(session, TNTTABLE_NAME_COUNT, tbDef);
	commitTrx(m_db->getTransSys(), trx1);

	TNTOpInfo opInfo1;
	initTNTOpInfo(&opInfo1, TL_X);
	trx1 = startTrx(m_db->getTransSys(), session);
	m_currTab = m_db->openTable(session, TNTTABLE_NAME_COUNT);
	
	Connection *conn2 = ntsedb->getConnection(false);
	Session *sess2 = ntsedb->getSessionManager()->allocSession("TNTFLRUpdatePerfTest::loadData", conn2);
	TNTTransaction *trx2 = startTrx(m_db->getTransSys(), sess2);
	m_db->createTable(sess2, INFO_TBL_NAME, new TableDef(TNTCountTable::getTableDef(false)));
	commitTrx(m_db->getTransSys(), trx2);

	TNTOpInfo opInfo2;
	initTNTOpInfo(&opInfo2, TL_X);
	trx2 = startTrx(m_db->getTransSys(), sess2);
	TNTTable *infoTbl = m_db->openTable(sess2, INFO_TBL_NAME);

	for (u64 i = 0; i < m_recCnt; ++i) {
		Record *rec = TNTCountTable::createRecord(i, 0);
		uint dupIdx;
		RowId rid = m_currTab->insert(session, rec->m_data, &dupIdx, &opInfo1);
		freeRecord(rec);
		Record *infoRec = TNTCountTable::createRecord(rid, TNTCountTable::getRecordSize());
		infoTbl->insert(sess2, infoRec->m_data, &dupIdx, &opInfo2);
		freeRecord(infoRec);
	}
	m_db->closeTable(sess2, infoTbl);
	commitTrx(m_db->getTransSys(), trx1);
	ntsedb->getSessionManager()->freeSession(sess2);
	ntsedb->freeConnection(conn2);

	m_db->closeTable(session, m_currTab);
	commitTrx(m_db->getTransSys(), trx2);
	ntsedb->getSessionManager()->freeSession(session);
	ntsedb->freeConnection(conn);
	ntsedb->close();

	*totalRecSize = m_recCnt * TNTCountTable::getRecordSize();
	*recCnt = m_recCnt;
}

u64 TNTFLRUpdatePerfTest::getDataSize() {
	return m_threadCnt * m_thdLoop * TNTCountTable::getRecordSize();
}

u64 TNTFLRUpdatePerfTest::getOpCnt() {
	return m_threadCnt * m_thdLoop;
}

void TNTFLRUpdatePerfTest::warmUp() {
	m_db = TNTDatabase::open(m_cfg, false, 0);

	Database *ntsedb = m_db->getNtseDb();
	Connection *conn2 = ntsedb->getConnection(false);
	Session *sess2 = ntsedb->getSessionManager()->allocSession("TNTFLRUpdatePerfTest::warmUp", conn2);

	TNTOpInfo opInfo2;
	initTNTOpInfo(&opInfo2, TL_NO);
	TNTTransaction *trx2 = startTrx(m_db->getTransSys(), sess2);
	TNTTable *infoTbl = m_db->openTable(sess2, INFO_TBL_NAME);
	SubRecordBuilder srb(infoTbl->getNtseTable()->getTableDef(), REC_REDUNDANT);
	u64 id = 0;
	SubRecord *info = srb.createSubRecordByName(COUNT_ID, &id);
	TNTTblScan *scanHdl = infoTbl->tableScan(sess2, OP_READ, &opInfo2, info->m_numCols, info->m_columns);
	while (infoTbl->getNext(scanHdl, info->m_data)) {
		id = RedRecord::readBigInt(infoTbl->getNtseTable()->getTableDef(), info->m_data, COUNT_ID_CNO);
		m_IdRowIdVec.push_back((RowId)id);
	}
	infoTbl->endScan(scanHdl);
	freeSubRecord(info);
	m_db->closeTable(sess2, infoTbl);
	commitTrx(m_db->getTransSys(), trx2);
	ntsedb->getSessionManager()->freeSession(sess2);
	ntsedb->freeConnection(conn2);

	Connection *conn = ntsedb->getConnection(false);
	Session *session = ntsedb->getSessionManager()->allocSession("TNTFLRUpdatePerfTest::warmUp", conn);
	TNTOpInfo opInfo;
	initTNTOpInfo(&opInfo, TL_X);
	TNTTransaction *trx = startTrx(m_db->getTransSys(), session);
	m_currTab = m_db->openTable(session, TNTTABLE_NAME_COUNT);
	TNTIndexFullScan(m_db, m_currTab, &opInfo);
	commitTrx(m_db->getTransSys(), trx);
	ntsedb->getSessionManager()->freeSession(session);
	ntsedb->freeConnection(conn);
}


void TNTFLRUpdatePerfTest::run() {
	TNTFLRUpdater **updaters = new TNTFLRUpdater*[m_threadCnt];

	for (uint i = 0; i < m_threadCnt; ++i) {
		updaters[i] = new TNTFLRUpdater(&m_IdRowIdVec, m_db, m_tblName, i, m_recCnt - 1, m_thdLoop);
	}

	u64 start = System::currentTimeMillis();
	for (uint i = 0; i < m_threadCnt; ++i) {
		(updaters[i])->start();
	}
	for (uint i = 0; i < m_threadCnt; ++i) {
		(updaters[i])->join();
	}
	u64 end = System::currentTimeMillis();

	m_totalMillis = end - start;

	for (uint i = 0; i < m_threadCnt; ++i) {
		delete updaters[i];
	}
	delete [] updaters;
}

void TNTFLRUpdatePerfTest::tearDown() {
	Database *ntsedb = m_db->getNtseDb();
	Connection *conn = ntsedb->getConnection(false);
	Session *session = ntsedb->getSessionManager()->allocSession("TNTFLRUpdatePerfTest::tearDown", conn);
	TNTTransaction *trx = startTrx(m_db->getTransSys(), session);
	m_db->closeTable(session, m_currTab);
	commitTrx(m_db->getTransSys(), trx);
	ntsedb->getSessionManager()->freeSession(session);
	ntsedb->freeConnection(conn);
	m_currTab = NULL;
	m_db->close();
	delete m_db;
	m_db = NULL;
	delete m_cfg;
}

string TNTVLRUpdatePerfTest::getName() const {
	stringstream ss;
	ss << "TNT VLR Update concurrency update primary key on table " << m_tblName;
	if (m_scale == SMALL) {
		ss << "[Small]";
	} else if (m_scale == MEDIUM) {
		ss << "[Medium]";
	} else {
		assert(false);
	}

	if (m_useMms) {
		ss << ", with mms.";
	} else { 
		ss << ", without mms.";
	}
	return ss.str();
}


string TNTVLRUpdatePerfTest::getDescription() const {
	stringstream ss;
	ss << "TNT VLR Update primary key performance test.";
	return ss.str();
}

void TNTVLRUpdatePerfTest::setUp() {
	m_cfg = getTNTConfig(m_scale);
	if (m_cfg->m_ntseConfig.m_maxSessions < m_threadCnt) {
		m_cfg->m_ntseConfig.m_maxSessions = m_threadCnt;
	}
	m_recCnt = m_cfg->m_ntseConfig.m_pageBufSize * Limits::PAGE_SIZE / m_avgRecSize;
}

void TNTVLRUpdatePerfTest::loadData(u64 *totalRecSize, u64 *recCnt) {
	TNTDatabase::drop(m_cfg->m_ntseConfig.m_basedir);
	m_db = TNTDatabase::open(m_cfg, true, 0);

	Database *ntsedb = m_db->getNtseDb();
	Connection *conn = ntsedb->getConnection(false);
	Session *session = ntsedb->getSessionManager()->allocSession("TNTVLRUpdatePerfTest::loadData", conn);
	TableDef *tbDef = new TableDef(TNTAccountTable::getTableDef(m_useMms));
	TNTTransaction *trx = startTrx(m_db->getTransSys(), session);
	m_db->createTable(session, TNTTABLE_NAME_ACCOUNT, tbDef);
	commitTrx(m_db->getTransSys(), trx);
	ntsedb->getSessionManager()->freeSession(session);
	ntsedb->freeConnection(conn);

	conn = ntsedb->getConnection(false);
	session = ntsedb->getSessionManager()->allocSession("TNTVLRUpdatePerfTest::loadData", conn);
	TNTOpInfo opInfo;
	initTNTOpInfo(&opInfo, TL_X);
	trx = startTrx(m_db->getTransSys(), session);
	m_currTab = m_db->openTable(session, TNTTABLE_NAME_ACCOUNT);
	int recSize;
	uint dupIdx;

	Connection *conn2 = ntsedb->getConnection(false);
	Session *sess2 = ntsedb->getSessionManager()->allocSession("TNTVLRUpdatePerfTest::loadData", conn2);
	TNTOpInfo opInfo2;
	initTNTOpInfo(&opInfo2, TL_X);
	TNTTransaction *trx2 = startTrx(m_db->getTransSys(), sess2);
	m_db->createTable(sess2, INFO_TBL_NAME, new TableDef(TNTCountTable::getTableDef(false)));
	commitTrx(m_db->getTransSys(), trx2);
	trx2 = startTrx(m_db->getTransSys(), sess2);
	TNTTable *infoTbl = m_db->openTable(sess2, INFO_TBL_NAME);

	m_dataSize = 0;
	for (u64 i = 0; i < m_recCnt; ++i) {
		Record *rec = TNTAccountTable::createRecord(i, &recSize, 8, m_avgRecSize);
		RowId rid = m_currTab->insert(session, rec->m_data, &dupIdx, &opInfo);
		m_dataSize += recSize;
		freeRecord(rec);
		Record *infoRec = TNTCountTable::createRecord(rid, recSize);
		infoTbl->insert(sess2, infoRec->m_data, &dupIdx, &opInfo2);
		freeRecord(infoRec);
	}

	m_db->closeTable(sess2, infoTbl);
	commitTrx(m_db->getTransSys(), trx2);
	ntsedb->getSessionManager()->freeSession(sess2);
	ntsedb->freeConnection(conn2);

	m_db->closeTable(session, m_currTab);
	commitTrx(m_db->getTransSys(), trx);
	ntsedb->getSessionManager()->freeSession(session);
	ntsedb->freeConnection(conn);
	m_db->close();

	*totalRecSize = m_dataSize;
	*recCnt = m_recCnt;
}

void TNTVLRUpdatePerfTest::warmUp() {
	m_db = TNTDatabase::open(m_cfg, false, 0);
	Database *ntsedb = m_db->getNtseDb();

	m_dataSize = 0;
	Connection *conn2 = ntsedb->getConnection(false);
	Session *sess2 = ntsedb->getSessionManager()->allocSession("TNTVLRUpdatePerfTest::warmUp", conn2);
	TNTOpInfo opInfo2;
	initTNTOpInfo(&opInfo2, TL_X);
	TNTTransaction *trx2 = startTrx(m_db->getTransSys(), sess2);
	TNTTable *infoTbl = m_db->openTable(sess2, INFO_TBL_NAME);
	SubRecordBuilder srb(infoTbl->getNtseTable()->getTableDef(), REC_REDUNDANT);
	u64 id = 0; int count = 0;
	SubRecord *info = srb.createSubRecordByName(COUNT_ID" "COUNT_COUNT, &id, &count);
	TNTTblScan *scanHdl = infoTbl->tableScan(sess2, OP_READ, &opInfo2, info->m_numCols, info->m_columns);
	while (infoTbl->getNext(scanHdl, info->m_data)) {
		id = RedRecord::readBigInt(infoTbl->getNtseTable()->getTableDef(), info->m_data, COUNT_ID_CNO);
		count = RedRecord::readInt(infoTbl->getNtseTable()->getTableDef(), info->m_data, COUNT_COUNT_CNO);
		m_IdRowIdVec.push_back((RowId)id);
		m_dataSize += count;
	}
	infoTbl->endScan(scanHdl);
	freeSubRecord(info);
	m_db->closeTable(sess2, infoTbl);
	commitTrx(m_db->getTransSys(), trx2);
	ntsedb->getSessionManager()->freeSession(sess2);
	ntsedb->freeConnection(conn2);

	Connection *conn = ntsedb->getConnection(false);
	Session *session = ntsedb->getSessionManager()->allocSession("TNTVLRUpdatePerfTest::warmUp", conn);
	TNTOpInfo opInfo;
	initTNTOpInfo(&opInfo, TL_X);
	TNTTransaction *trx = startTrx(m_db->getTransSys(), session);
	m_currTab = m_db->openTable(session, TNTTABLE_NAME_ACCOUNT);
	TNTIndexFullScan(m_db, m_currTab, &opInfo);
	commitTrx(m_db->getTransSys(), trx);
	ntsedb->getSessionManager()->freeSession(session);
	ntsedb->freeConnection(conn);
}

void TNTVLRUpdatePerfTest::tearDown() {
	Database *ntsedb = m_db->getNtseDb();
	Connection *conn = ntsedb->getConnection(false);
	Session *session = ntsedb->getSessionManager()->allocSession("TNTVLRUpdatePerfTest::tearDown", conn);
	TNTTransaction *trx = startTrx(m_db->getTransSys(), session);
	m_db->closeTable(session, m_currTab);
	commitTrx(m_db->getTransSys(), trx);
	ntsedb->getSessionManager()->freeSession(session);
	ntsedb->freeConnection(conn);
	m_db->close();
	delete m_db;
	m_db = NULL;
	delete m_cfg;
}

void TNTVLRUpdatePerfTest::run() {
	TNTVLRUpdater **updaters = new TNTVLRUpdater*[m_threadCnt];

	for (uint i = 0; i < m_threadCnt; ++i) {
		updaters[i] = new TNTVLRUpdater(&m_IdRowIdVec, m_db, m_tblName, i, m_recCnt - 1, m_thdLoop);
	}

	u64 start = System::currentTimeMillis();
	for (uint i = 0; i < m_threadCnt; ++i) {
		(updaters[i])->start();
	}
	for (uint i = 0; i < m_threadCnt; ++i) {
		(updaters[i])->join();
	}
	u64 end = System::currentTimeMillis();

	m_totalMillis = end - start;

	for (uint i = 0; i < m_threadCnt; ++i) {
		delete updaters[i];
	}
	delete [] updaters;
}

u64 TNTVLRUpdatePerfTest::getDataSize() {
	return m_threadCnt * m_thdLoop * m_avgRecSize;
}
}
