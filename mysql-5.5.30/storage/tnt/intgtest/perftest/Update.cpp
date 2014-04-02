#include "Update.h"
#include "DbConfigs.h"
#include "api/Table.h"
#include "api/Database.h"
#include "misc/Session.h"
#include "misc/RecordHelper.h"
#include "CountTable.h"
#include "LongCharTable.h"
#include "AccountTable.h"
#include "util/Thread.h"
#include "Random.h"
#include <sstream>
#include <iostream>
#include "Generator.h"
#include "TableHelper.h"

using namespace ntseperf;
using namespace std;

#define INFO_TBL_NAME "RowId_backup"
#define PKEY_TBL_NAME "Id_backup"


static Config * getConfig(Scale scale) {
	switch (scale) {
		case SMALL:
			return new Config(CommonDbConfig::getSmall());
		case MEDIUM:
			return new Config(CommonDbConfig::getMedium());
	}
	return NULL;
}

/* Updater threads */

class Updater_1 : public Thread {
public:
	Updater_1(vector<RowId>* ridVec, Database *db, Table *tbl, uint tid, int loop) : Thread("Updater_1") {
		m_ridVec = ridVec;
		m_db = db;
		m_tbl = tbl;
		m_loop = loop;
		m_tid = tid;
	}
protected:
	vector<RowId> *m_ridVec;
	Database *m_db;
	Table *m_tbl;
	int m_loop;
	uint m_tid;

	virtual void run();
};

class Updater_2 : public Updater_1 {
public:
	Updater_2(vector<RowId>* ridVec, vector<int>* recSize, Database *db, Table *tbl, uint tid, int* dataSize, int loop) : Updater_1(ridVec, db, tbl, tid, loop) {
		m_dataSize = dataSize;
		m_recSizeVec = recSize;
	}
protected:
	int *m_dataSize;
	vector<int> *m_recSizeVec;
	virtual void run();
};

class Updater_3 : public Updater_1 {
public:
	Updater_3(vector<RowId>* ridVec, Database *db, Table *tbl, uint tid, u64 maxID, int loop) : Updater_1(ridVec, db, tbl, tid, loop) {
		m_maxID = maxID;
		u64 *IDs = new u64[maxID + 1];
		Generator::generateArray(IDs, maxID + 1, (u64)0, (u64)1, RANDOM);
		for (int i = 0; i < sizeof(m_ID) / sizeof(m_ID[0]); ++i) {
			m_ID[i] = IDs[i];
		}
		delete [] IDs;
		sort(m_ID, m_ID + sizeof(m_ID) / sizeof(m_ID[0]) - 1);
	}
protected:
	u64 m_maxID;
	u64 m_ID[100];
	virtual void run();
};

class Updater_4 : public Updater_3 {
public:
	Updater_4(vector<RowId>* ridVec, Database *db, Table *tbl, uint tid, u64 maxID, int loop) : Updater_3(ridVec, db, tbl, tid, maxID, loop) {
	}
protected:
	virtual void run();
};


class Updater_5 : public Updater_1 {
public:
	Updater_5(Database *db, Table *tbl, uint tid, vector<u64> *idVec, int loop) : Updater_1(NULL, db, tbl, tid, loop) {
		m_idVec = idVec;
	}
protected:
	vector<u64> *m_idVec;
	virtual void run();
};

class Updater_6 : public Updater_5 {
public:
	Updater_6(Database *db, Table *tbl, uint tid, vector<u64> *idVec, int loop) : Updater_5(db, tbl, tid, idVec, loop) {
	}
protected:
	virtual void run();
};




string UpdatePerfTest_1::getName() const {
	stringstream ss;
	//ss << "Update Test 4.1.5.1: ";
	ss << "Update concurrency test on table " << m_tblName << ", ";
	if (m_useMms)
		ss << "with mms, ";
	if (m_delayUpd)
		ss << "with delay update, ";
	return ss.str();
}

string UpdatePerfTest_1::getDescription() const {
	stringstream ss;
	ss << "Update operation performance test.";
	return ss.str();
}

void UpdatePerfTest_1::loadData(u64 *totalRecSize, u64 *recCnt) {
	m_cfg = getConfig(m_scale);
	if (m_cfg->m_maxSessions < m_threadCnt) m_cfg->m_maxSessions = m_threadCnt;

	Database::drop(m_cfg->m_basedir);
	m_db = Database::open(m_cfg, true);


	Connection *conn = m_db->getConnection(false);
	Session *session = m_db->getSessionManager()->allocSession("UpdatePerfTest_1::loadData", conn);
	TableDef *tbDef = new TableDef(CountTable::getTableDef(m_useMms));
	tbDef->m_columns[COUNT_COUNT_CNO]->m_cacheUpdate = m_delayUpd; /* 是否使用delay update，本测试只更新COUNT字段 */
	m_db->createTable(session, TABLE_NAME_COUNT, tbDef);
	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(conn);


	m_recCnt = 100;


	conn = m_db->getConnection(false);
	session = m_db->getSessionManager()->allocSession("UpdatePerfTest_1::loadData", conn);

	m_currTab = m_db->openTable(session, TABLE_NAME_COUNT);


	for (u64 i = 0; i < m_recCnt; ++i) {
		Record *rec = CountTable::createRecord(i, 0);
		uint dupIdx;
		RowId rid = m_currTab->insert(session, rec->m_data, &dupIdx);
		m_IdRowIdVec.push_back(rid);
		freeRecord(rec);
	}

	m_db->closeTable(session, m_currTab);
	//m_currTab->close(session, true);

	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(conn);


	m_db->close();

	m_opCnt = m_threadCnt * m_thdLoop;

	m_dataSize = m_opCnt * CountTable::getRecordSize();

	*recCnt = m_recCnt;
	*totalRecSize = *recCnt * CountTable::getRecordSize();

}

void UpdatePerfTest_1::warmUp() {
	m_db = Database::open(m_cfg, false);
	Connection *conn = m_db->getConnection(false);
	Session *session = m_db->getSessionManager()->allocSession("UpdatePerfTest_1::warmUp", conn);
	m_currTab = m_db->openTable(session, TABLE_NAME_COUNT);
	indexFullScan(m_db, m_currTab);
	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(conn);
}

void UpdatePerfTest_1::tearDown() {
 	Connection *conn = m_db->getConnection(false);
	Session *session = m_db->getSessionManager()->allocSession("UpdatePerfTest_1::tearDown", conn);
	m_db->closeTable(session, m_currTab);
	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(conn);
	m_currTab = NULL;
	m_db->close();
	delete m_db;
	m_db = NULL;
	delete m_cfg;
}

void UpdatePerfTest_1::run() {
	Updater_1 **updaters = new Updater_1*[m_threadCnt];

	for (uint i = 0; i < m_threadCnt; ++i) {
		updaters[i] = new Updater_1(&m_IdRowIdVec, m_db, m_currTab, i, m_thdLoop);
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

void Updater_1::run() {
	Connection *conn = m_db->getConnection(false);
	Session *session = m_db->getSessionManager()->allocSession("Updater_1::run", conn);

	byte buf[Limits::PAGE_SIZE];
	u16 updCols[1] = {COUNT_COUNT_CNO};

	int count = 0;
	u64 ID = 0;

	SubRecordBuilder srb(m_tbl->getTableDef(), REC_REDUNDANT);
	SubRecord *updRec = srb.createSubRecordByName(COUNT_COUNT, &count);

	TblScan *scanHdl = m_tbl->positionScan(session, OP_UPDATE, 1, updCols);

	scanHdl->setUpdateColumns(1, updCols);

	for (int i = 0; i < m_loop; ++i) {
		count = m_loop * m_tid + i;
		ID = RandomGen::nextInt(0, 100);
		m_tbl->getNext(scanHdl, buf, (*m_ridVec)[ID]);
		RedRecord::writeNumber(m_tbl->getTableDef(), COUNT_COUNT_CNO, updRec->m_data, count);
		NTSE_ASSERT(m_tbl->updateCurrent(scanHdl, updRec->m_data));
	}

	m_tbl->endScan(scanHdl);
	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(conn);

	freeSubRecord(updRec);
}

void UpdatePerfTest_2::loadData(u64 *totalRecSize, u64 *recCnt) {
	m_cfg = getConfig(m_scale);
	if (m_cfg->m_maxSessions < m_threadCnt) m_cfg->m_maxSessions = m_threadCnt;

	Database::drop(m_cfg->m_basedir);
	m_db = Database::open(m_cfg, true);

	Connection *conn = m_db->getConnection(false);
	Session *session = m_db->getSessionManager()->allocSession("UpdatePerfTest_2::loadData", conn);
	TableDef *tbDef = new TableDef(AccountTable::getTableDef(m_useMms));
	tbDef->m_columns[ACCOUNT_USERNAME_CNO]->m_cacheUpdate = m_delayUpd; /* 是否使用delay update，本测试只更新username字段 */
	m_db->createTable(session, TABLE_NAME_ACCOUNT, tbDef);
	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(conn);

	m_recCnt = 100;

	conn = m_db->getConnection(false);
	session = m_db->getSessionManager()->allocSession("UpdatePerfTest_2::loadData", conn);
	m_currTab = m_db->openTable(session, TABLE_NAME_ACCOUNT);
	*totalRecSize = 0;

	for (u64 i = 0; i < m_recCnt; ++i) {
		int recSize;
		Record *rec = AccountTable::createRecord(i, &recSize, 8, 12);
		m_recSize.push_back(recSize);
		uint dupIdx;
		RowId rid = m_currTab->insert(session, rec->m_data, &dupIdx);
		m_IdRowIdVec.push_back(rid);
		freeRecord(rec);
		*totalRecSize += recSize;
	}

	m_db->closeTable(session, m_currTab);

	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(conn);

	m_db->close();

	m_opCnt = m_threadCnt * m_thdLoop;

	*recCnt = m_recCnt;
}

void UpdatePerfTest_2::warmUp() {
	m_db = Database::open(m_cfg, false);
	Connection *conn = m_db->getConnection(false);
	Session *session = m_db->getSessionManager()->allocSession("UpdatePerfTest_2::warmUp", conn);
	m_currTab = m_db->openTable(session, TABLE_NAME_ACCOUNT);
	indexFullScan(m_db, m_currTab);
	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(conn);
}

void UpdatePerfTest_2::run() {
	Updater_2 **updaters = new Updater_2*[m_threadCnt];
	int *dataSize = new int[m_threadCnt];

	for (uint i = 0; i < m_threadCnt; ++i) {
		updaters[i] = new Updater_2(&m_IdRowIdVec, &m_recSize, m_db, m_currTab, i, &dataSize[i], m_thdLoop);
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

void UpdatePerfTest_2::tearDown() {
 	Connection *conn = m_db->getConnection(false);
	Session *session = m_db->getSessionManager()->allocSession("UpdatePerfTest_2::tearDown", conn);
	//m_currTab->close(session, false);
	m_db->closeTable(session, m_currTab);
	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(conn);
	// delete m_currTab; delete db时会删掉
	m_currTab = NULL;
	m_db->close();
	delete m_db;
	m_db = NULL;
	delete m_cfg;
}


void Updater_2::run() {
	static const char *usernameArray = "FEDCBA9876543210";

	Connection *conn = m_db->getConnection(false);
	Session *session = m_db->getSessionManager()->allocSession("Updater_2::run", conn);

	byte buf[Limits::PAGE_SIZE];
	u16 updCols[1] = {ACCOUNT_USERNAME_CNO};


	int unameLen;// = RandomGen::nextInt(8, 17); // 8 ~ 16
	const char *username = "Noname";
	u64 ID = 0;
	

	//SubRecordBuilder srb(m_tbl->getTableDef(), REC_REDUNDANT);
	//SubRecord *updRec = srb.createSubRecordByName(ACCOUNT_USERNAME, username);

	TblScan *scanHdl = m_tbl->positionScan(session, OP_UPDATE, 1, updCols);
	scanHdl->setUpdateColumns(1, updCols);

	for (int i = 0; i < m_loop; ++i) {
		ID = RandomGen::nextInt(0, 100);
		unameLen = RandomGen::nextInt(8, 17); // 8 ~ 16
		username = usernameArray + (16 - unameLen);
		m_tbl->getNext(scanHdl, buf, (*m_ridVec)[ID]);
		u64 ID = RedRecord::readBigInt(m_tbl->getTableDef(), buf, ACCOUNT_ID_CNO);
		//RedRecord::writeVarchar(m_tbl->getTableDef(), updRec->m_data, ACCOUNT_USERNAME_CNO, username);
		Record *upd = AccountTable::createRecord(ID, NULL, 8, 12);
		NTSE_ASSERT(m_tbl->updateCurrent(scanHdl, upd->m_data));
		freeRecord(upd);
	}

	m_tbl->endScan(scanHdl);
	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(conn);

	//freeSubRecord(updRec);
}

void Updater_3::run() {
	Connection *conn = m_db->getConnection(false);
	Session *session = m_db->getSessionManager()->allocSession("Updater_3::run", conn);

	byte buf[Limits::PAGE_SIZE];
	u16 updCols[1] = {COUNT_ID_CNO};

	u64 newID, ID = 0;

	SubRecordBuilder srb(m_tbl->getTableDef(), REC_REDUNDANT);
	SubRecord *updRec = srb.createSubRecordByName(COUNT_ID, &ID);

	TblScan *scanHdl = m_tbl->positionScan(session, OP_UPDATE, 1, updCols);

	scanHdl->setUpdateColumns(1, updCols);

	for (int i = 0; i < m_loop; ++i) {
		newID = m_maxID + 1 + m_loop * m_tid + i;
		ID = RandomGen::nextInt(0, sizeof(m_ID) / sizeof(m_ID[0]));

		m_tbl->getNext(scanHdl, buf, (*m_ridVec)[m_ID[ID]]);
		RedRecord::writeNumber(m_tbl->getTableDef(), COUNT_ID_CNO, updRec->m_data, newID);
		NTSE_ASSERT(m_tbl->updateCurrent(scanHdl, updRec->m_data));
	}

	m_tbl->endScan(scanHdl);
	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(conn);

	freeSubRecord(updRec);
}

void Updater_4::run() {
	Connection *conn = m_db->getConnection(false);
	Session *session = m_db->getSessionManager()->allocSession("Updater_4::run", conn);

	byte buf[Limits::PAGE_SIZE];
	u16 updCols[1] = {ACCOUNT_ID_CNO};

	u64 newID, ID = 0;

	SubRecordBuilder srb(m_tbl->getTableDef(), REC_REDUNDANT);
	SubRecord *updRec = srb.createSubRecordByName(ACCOUNT_ID, &ID);

	TblScan *scanHdl = m_tbl->positionScan(session, OP_UPDATE, 1, updCols);

	scanHdl->setUpdateColumns(1, updCols);

	for (int i = 0; i < m_loop; ++i) {
		newID = m_maxID + 1 + m_loop * m_tid + i;
		ID = RandomGen::nextInt(0, sizeof(m_ID) / sizeof(m_ID[0]));

		m_tbl->getNext(scanHdl, buf, (*m_ridVec)[m_ID[ID]]);
		RedRecord::writeNumber(m_tbl->getTableDef(), ACCOUNT_ID_CNO, updRec->m_data, newID);
		NTSE_ASSERT(m_tbl->updateCurrent(scanHdl, updRec->m_data));
	}

	m_tbl->endScan(scanHdl);
	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(conn);

	freeSubRecord(updRec);
}

void Updater_5::run() {
	Connection *conn = m_db->getConnection(false);
	//cout << "rid vec size is " << m_idVec->size() << endl;

	byte buf[Limits::PAGE_SIZE];
	u16 updCols[1] = {LONGCHAR_NAME_CNO};
	u16 scanCols[1] = {LONGCHAR_ID_CNO};

	for (int i = 0; i < m_loop; ++i) {
		u64 r = (*m_idVec)[RandomGen::nextInt(0, m_idVec->size())];
		Session *session = m_db->getSessionManager()->allocSession("Updater_5::run", conn);

		SubRecordBuilder keyBuilder(m_tbl->getTableDef(), KEY_PAD, INVALID_ROW_ID);
		SubRecord *findKey = keyBuilder.createSubRecordByName(LONGCHAR_ID, &r);
		IndexScanCond cond(0, findKey, true, true, true);

		TblScan *scanHandle = m_tbl->indexScan(session, OP_UPDATE, &cond, 1, scanCols);
		if (m_tbl->getNext(scanHandle, buf) == false)
		{
			// do nothing
			assert(false);
		} else {
			u64 ID = RedRecord::readBigInt(m_tbl->getTableDef(), buf, LONGCHAR_ID_CNO);
			Record *rec = LongCharTable::createRecord(ID);
			scanHandle->setUpdateColumns(1, updCols);
			NTSE_ASSERT(m_tbl->updateCurrent(scanHandle, rec->m_data));
			freeRecord(rec);
		}
		freeSubRecord(findKey);
		m_tbl->endScan(scanHandle);

		m_db->getSessionManager()->freeSession(session);
	}

	m_db->freeConnection(conn);
}

void Updater_6::run() {
	Connection *conn = m_db->getConnection(false);
	//cout << "rid vec size is " << m_idVec->size() << endl;

	byte buf[Limits::PAGE_SIZE];
	u16 updCols[1] = {ACCOUNT_USERNAME_CNO};
	u16 scanCols[1] = {ACCOUNT_ID_CNO};

	for (int i = 0; i < m_loop; ++i) {
		u64 r = (*m_idVec)[RandomGen::nextInt(0, m_idVec->size())];
		Session *session = m_db->getSessionManager()->allocSession("Updater_6::run", conn);

		SubRecordBuilder keyBuilder(m_tbl->getTableDef(), KEY_PAD, INVALID_ROW_ID);
		SubRecord *findKey = keyBuilder.createSubRecordByName(ACCOUNT_ID, &r);
		IndexScanCond cond(0, findKey, true, true, true);

		TblScan *scanHandle = m_tbl->indexScan(session, OP_UPDATE, &cond, 1, scanCols);
		if (m_tbl->getNext(scanHandle, buf) == false)
		{
			// do nothing
			assert(false);
		} else {
			u64 ID = RedRecord::readBigInt(m_tbl->getTableDef(), buf, ACCOUNT_ID_CNO);
			//Record *rec = LongCharTable::createRecord(ID);
			int recSize;
			Record *rec = AccountTable::createRecord(ID, &recSize, 150, 200);
			scanHandle->setUpdateColumns(1, updCols);
			//m_tbl->updateCurrent(scanHandle, buf);
			NTSE_ASSERT(m_tbl->updateCurrent(scanHandle, rec->m_data));
			freeRecord(rec);
		}
		freeSubRecord(findKey);
		m_tbl->endScan(scanHandle);

		m_db->getSessionManager()->freeSession(session);
	}
	m_db->freeConnection(conn);
}


string UpdatePerfTest_3::getName() const {
	stringstream ss;
	ss << "Update concurrency update primary key on table " << m_tblName << ", ";
	if (m_useMms)
		ss << "with mms.";
	else 
		ss << "without mms.";
	return ss.str();
}


string UpdatePerfTest_3::getDescription() const {
	stringstream ss;
	ss << "Update primary key performance test.";
	return ss.str();
}

void UpdatePerfTest_3::setUp() {
	m_cfg = getConfig(m_scale);
	if (m_cfg->m_maxSessions < m_threadCnt) m_cfg->m_maxSessions = m_threadCnt;

	m_recCnt = m_cfg->m_pageBufSize * Limits::PAGE_SIZE / CountTable::getRecordSize();
	m_maxID = m_recCnt - 1;
}

void UpdatePerfTest_3::loadData(u64 *totalRecSize, u64 *recCnt) {

	Database::drop(m_cfg->m_basedir);
	m_db = Database::open(m_cfg, true);

	Connection *conn = m_db->getConnection(false);
	Session *session = m_db->getSessionManager()->allocSession("UpdatePerfTest_3::loadData", conn);
	TableDef *tbDef = new TableDef(CountTable::getTableDef(m_useMms));
	m_db->createTable(session, TABLE_NAME_COUNT, tbDef);
	m_currTab = m_db->openTable(session, TABLE_NAME_COUNT);


	Connection *conn2 = m_db->getConnection(false);
	Session *sess2 = m_db->getSessionManager()->allocSession("DeletePerfTest::loadData", conn2);
	m_db->createTable(sess2, INFO_TBL_NAME, new TableDef(CountTable::getTableDef(false)));
	Table *infoTbl = m_db->openTable(sess2, INFO_TBL_NAME);

	for (u64 i = 0; i < m_recCnt; ++i) {
		Record *rec = CountTable::createRecord(i, 0);
		uint dupIdx;
		RowId rid = m_currTab->insert(session, rec->m_data, &dupIdx);
		freeRecord(rec);
		Record *infoRec = CountTable::createRecord(rid, CountTable::getRecordSize());
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

	*totalRecSize = m_recCnt * CountTable::getRecordSize();
	*recCnt = m_recCnt;
}

u64 UpdatePerfTest_3::getDataSize() {
	return m_threadCnt * m_thdLoop * CountTable::getRecordSize();
}

u64 UpdatePerfTest_3::getOpCnt() {
	return m_threadCnt * m_thdLoop;
}

void UpdatePerfTest_3::warmUp() {
	m_db = Database::open(m_cfg, false);

	Connection *conn2 = m_db->getConnection(false);
	Session *sess2 = m_db->getSessionManager()->allocSession("UpdatePerfTest_3::warmUp", conn2);
	Table *infoTbl = m_db->openTable(sess2, INFO_TBL_NAME);
	SubRecordBuilder srb(infoTbl->getTableDef(), REC_REDUNDANT);
	u64 id = 0;
	SubRecord *info = srb.createSubRecordByName(COUNT_ID, &id);
	TblScan *scanHdl = infoTbl->tableScan(sess2, OP_READ, info->m_numCols, info->m_columns);
	while (infoTbl->getNext(scanHdl, info->m_data)) {
		id = RedRecord::readBigInt(infoTbl->getTableDef(), info->m_data, COUNT_ID_CNO);
		m_IdRowIdVec.push_back((RowId)id);
	}
	infoTbl->endScan(scanHdl);
	freeSubRecord(info);
	m_db->closeTable(sess2, infoTbl);
	m_db->getSessionManager()->freeSession(sess2);
	m_db->freeConnection(conn2);

	Connection *conn = m_db->getConnection(false);
	Session *session = m_db->getSessionManager()->allocSession("UpdatePerfTest_3::warmUp", conn);
	m_currTab = m_db->openTable(session, TABLE_NAME_COUNT);
	indexFullScan(m_db, m_currTab);
	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(conn);
}


void UpdatePerfTest_3::run() {
	Updater_3 **updaters = new Updater_3*[m_threadCnt];

	for (uint i = 0; i < m_threadCnt; ++i) {
		updaters[i] = new Updater_3(&m_IdRowIdVec, m_db, m_currTab, i, m_recCnt - 1, m_thdLoop);
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

void UpdatePerfTest_3::tearDown() {
 	Connection *conn = m_db->getConnection(false);
	Session *session = m_db->getSessionManager()->allocSession("UpdatePerfTest_3::tearDown", conn);
	m_db->closeTable(session, m_currTab);
	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(conn);
	m_currTab = NULL;
	m_db->close();
	delete m_db;
	m_db = NULL;
	delete m_cfg;
}

void UpdatePerfTest_4::setUp() {
	m_cfg = getConfig(m_scale);
	if (m_cfg->m_maxSessions < m_threadCnt) m_cfg->m_maxSessions = m_threadCnt;
	m_recCnt = m_cfg->m_pageBufSize * Limits::PAGE_SIZE / m_avgRecSize;
}

void UpdatePerfTest_4::loadData(u64 *totalRecSize, u64 *recCnt) {

	Database::drop(m_cfg->m_basedir);
	m_db = Database::open(m_cfg, true);

	Connection *conn = m_db->getConnection(false);
	Session *session = m_db->getSessionManager()->allocSession("UpdatePerfTest_4::loadData", conn);
	TableDef *tbDef = new TableDef(AccountTable::getTableDef(m_useMms));
	m_db->createTable(session, TABLE_NAME_ACCOUNT, tbDef);
	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(conn);


	conn = m_db->getConnection(false);
	session = m_db->getSessionManager()->allocSession("UpdatePerfTest_4::loadData", conn);
	m_currTab = m_db->openTable(session, TABLE_NAME_ACCOUNT);
	int recSize;
	uint dupIdx;

	Connection *conn2 = m_db->getConnection(false);
	Session *sess2 = m_db->getSessionManager()->allocSession("DeletePerfTest::loadData", conn2);
	m_db->createTable(sess2, INFO_TBL_NAME, new TableDef(CountTable::getTableDef(false)));
	Table *infoTbl = m_db->openTable(sess2, INFO_TBL_NAME);

	m_dataSize = 0;
	for (u64 i = 0; i < m_recCnt; ++i) {
		Record *rec = AccountTable::createRecord(i, &recSize, 8, m_avgRecSize);
		RowId rid = m_currTab->insert(session, rec->m_data, &dupIdx);
		m_dataSize += recSize;
		freeRecord(rec);
		Record *infoRec = CountTable::createRecord(rid, recSize);
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

void UpdatePerfTest_4::warmUp() {
	m_db = Database::open(m_cfg, false);

	m_dataSize = 0;
	Connection *conn2 = m_db->getConnection(false);
	Session *sess2 = m_db->getSessionManager()->allocSession("UpdatePerfTest_4::warmUp", conn2);
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

	Connection *conn = m_db->getConnection(false);
	Session *session = m_db->getSessionManager()->allocSession("UpdatePerfTest_4::warmUp", conn);
	m_currTab = m_db->openTable(session, TABLE_NAME_ACCOUNT);
	indexFullScan(m_db, m_currTab);
	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(conn);
}

void UpdatePerfTest_4::tearDown() {
 	Connection *conn = m_db->getConnection(false);
	Session *session = m_db->getSessionManager()->allocSession("UpdatePerfTest_4::tearDown", conn);
	m_db->closeTable(session, m_currTab);
	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(conn);
	m_db->close();
	delete m_db;
	m_db = NULL;
	delete m_cfg;
}

void UpdatePerfTest_4::run() {
	Updater_4 **updaters = new Updater_4*[m_threadCnt];

	for (uint i = 0; i < m_threadCnt; ++i) {
		updaters[i] = new Updater_4(&m_IdRowIdVec, m_db, m_currTab, i, m_recCnt - 1, m_thdLoop);
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

u64 UpdatePerfTest_4::getDataSize() {
	return m_threadCnt * m_thdLoop * m_avgRecSize;
}


string UpdatePerfTest_5::getName() const {
	stringstream ss;
	ss << "Update IO performance on table " << m_tblName << ", ";
	switch (m_scale) {
		case SMALL:
			ss << "small config, ";
			break;
		case MEDIUM:
			ss << "medium config, ";
			break;
	}
	ss << m_threadCnt << " threads, ";
	if (m_useMms)
		ss << "with mms.";
	else
		ss << "without mms.";
	return ss.str();
}


string UpdatePerfTest_5::getDescription() const {
	stringstream ss;
	ss << "Update IO performance test.";
	return ss.str();
}

void UpdatePerfTest_5::setUp() {
	m_cfg = getConfig(m_scale);
	if (m_cfg->m_maxSessions < m_threadCnt) m_cfg->m_maxSessions = m_threadCnt;

	m_recCnt = m_cfg->m_pageBufSize * Limits::PAGE_SIZE * m_dataSizeFact / LongCharTable::getRecordSize();
	m_dataSize = m_recCnt * LongCharTable::getRecordSize();
	m_opCnt = m_threadCnt * m_thdLoop;
}

void UpdatePerfTest_5::loadData(u64 *totalRecSize, u64 *recCnt) {
	Database::drop(m_cfg->m_basedir);
	m_db = Database::open(m_cfg, true);

	Connection *conn = m_db->getConnection(false);
	Session *session = m_db->getSessionManager()->allocSession("UpdatePerfTest_5::loadData", conn);
	TableDef *tbDef = new TableDef(LongCharTable::getTableDef(m_useMms));
	m_db->createTable(session, TABLE_NAME_LONGCHAR, tbDef);
	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(conn);

	vector<RowId> idRowIdVec;

	conn = m_db->getConnection(false);
	session = m_db->getSessionManager()->allocSession("UpdatePerfTest_5::loadData", conn);
	m_currTab = m_db->openTable(session, TABLE_NAME_LONGCHAR);

	for (u64 i = 0; i < m_recCnt; ++i) {
		Record *rec = LongCharTable::createRecord(RandomGen::nextInt());
		uint dupIdx;
		RowId rid = m_currTab->insert(session, rec->m_data, &dupIdx);
		freeRecord(rec);
		if (rid == INVALID_ROW_ID) {
			--i;
			continue;
		}
		idRowIdVec.push_back(rid);
	}
	

	// 插入删除更新各5000次
	// 5000 insert
	for (int i = 0; i < 5000; ++i) {
		Record *rec = LongCharTable::createRecord(RandomGen::nextInt());
		uint dupIdx;
		RowId rid = m_currTab->insert(session, rec->m_data, &dupIdx);
		freeRecord(rec);
		if (rid == INVALID_ROW_ID) {
			--i;
			continue;
		}
		idRowIdVec.push_back(rid);
	}
	// 5000 del
	u16 delCols[] = {LONGCHAR_ID_CNO};
	byte buf[Limits::PAGE_SIZE];
	TblScan *scanHdl = m_currTab->positionScan(session, OP_DELETE, 1, delCols);
	for (int i = 0; i < 5000; ++i) {
		int idx = RandomGen::nextInt(0, idRowIdVec.size());
		RowId rid = idRowIdVec[idx];
		if (rid == 0) {
			--i;
			continue;
		}
		m_currTab->getNext(scanHdl, buf, rid);
		m_currTab->deleteCurrent(scanHdl);
		idRowIdVec[idx] = 0;
	}
	m_currTab->endScan(scanHdl);
	// 5000 update
	u16 updCols[] = {LONGCHAR_ID_CNO};
	Record *upd = LongCharTable::createRecord(RandomGen::nextInt(0, m_recCnt));
	scanHdl = m_currTab->positionScan(session, OP_UPDATE, 1, updCols);
	scanHdl->setUpdateColumns(1, updCols);
	for (int i = 0; i < 5000; ++i) {
		int idx = RandomGen::nextInt(0, idRowIdVec.size());
		RowId rid = idRowIdVec[idx];
		if (rid == 0) {
			--i;
			continue;
		}
		RedRecord::writeNumber(m_currTab->getTableDef(), LONGCHAR_ID_CNO, upd->m_data, (u64)RandomGen::nextInt());
		m_currTab->getNext(scanHdl, buf, rid);
		bool success = m_currTab->updateCurrent(scanHdl, upd->m_data);
		if (!success) { // 主键索引冲突
			--i;
			continue;
		}
	}
	m_currTab->endScan(scanHdl);
	freeRecord(upd);
	assert(idRowIdVec.size() == m_recCnt + 5000);

	Connection *conn2 = m_db->getConnection(false);
	Session *sess2 = m_db->getSessionManager()->allocSession("DeletePerfTest::loadData", conn2);
	m_db->createTable(sess2, PKEY_TBL_NAME, new TableDef(CountTable::getTableDef(false)));
	Table *pkeyTbl = m_db->openTable(sess2, PKEY_TBL_NAME);
	Record *sr = CountTable::createRecord(0, 0);

	scanHdl = m_currTab->positionScan(session, OP_READ, 1, delCols);
	int count = 0;
	for (vector<RowId>::iterator it = idRowIdVec.begin(); it != idRowIdVec.end(); ++it) {
		if (*it == 0) continue;
		uint dupIdx;
		assert(*it != INVALID_ROW_ID);
		NTSE_ASSERT(m_currTab->getNext(scanHdl, buf, *it));
		u64 ID = RedRecord::readBigInt(m_currTab->getTableDef(), buf, LONGCHAR_ID_CNO); // 读出ID
		RedRecord::writeNumber(pkeyTbl->getTableDef(), COUNT_ID_CNO, sr->m_data, ID); // 写入ID到记录
		RowId prid = pkeyTbl->insert(sess2, sr->m_data, &dupIdx);
		assert(prid != INVALID_ROW_ID);
		++count;
	}
	m_currTab->endScan(scanHdl);
	assert(count == m_recCnt);
	m_db->closeTable(sess2, pkeyTbl);
	m_db->getSessionManager()->freeSession(sess2);
	m_db->freeConnection(conn2);



	m_db->closeTable(session, m_currTab);
	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(conn);
	m_db->close();

	*totalRecSize = m_dataSize;
	*recCnt = m_recCnt;
}

void UpdatePerfTest_5::warmUp() {
	m_db = Database::open(m_cfg, false);


	//vector<u64> idVec;

	Connection *conn2 = m_db->getConnection(false);
	Session *sess2 = m_db->getSessionManager()->allocSession("UpdatePerfTest_5::warmUp", conn2);
	Table *pkeyTbl = m_db->openTable(sess2, PKEY_TBL_NAME);
	SubRecordBuilder srb(pkeyTbl->getTableDef(), REC_REDUNDANT);
	u64 id = 0;
	SubRecord *pkey = srb.createSubRecordByName(COUNT_ID, &id);
	TblScan *scanHdl = pkeyTbl->tableScan(sess2, OP_READ, pkey->m_numCols, pkey->m_columns);
	while (pkeyTbl->getNext(scanHdl, pkey->m_data)) {
		id = RedRecord::readBigInt(pkeyTbl->getTableDef(), pkey->m_data, COUNT_ID_CNO);
		m_idVec.push_back(id);
	}
	pkeyTbl->endScan(scanHdl);
	freeSubRecord(pkey);
	m_db->closeTable(sess2, pkeyTbl);
	m_db->getSessionManager()->freeSession(sess2);
	m_db->freeConnection(conn2);

	assert(m_idVec.size() == m_recCnt);

	/* 至此，所有可用id已经放在一个vector里面了 */

	Connection *conn = m_db->getConnection(false);
	Session *session = m_db->getSessionManager()->allocSession("UpdatePerfTest_5::warmUp", conn);
	m_currTab = m_db->openTable(session, TABLE_NAME_LONGCHAR);
	indexFullScan(m_db, m_currTab);
	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(conn);
}


void UpdatePerfTest_5::run() {
	assert(m_idVec.size() == m_recCnt);

	Updater_5 **updaters = new Updater_5*[m_threadCnt];
	for (uint i = 0; i < m_threadCnt; ++i) {
		updaters[i] = new Updater_5(m_db, m_currTab, i, &m_idVec, m_thdLoop);
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



void UpdatePerfTest_5::tearDown() {
 	Connection *conn = m_db->getConnection(false);
	Session *session = m_db->getSessionManager()->allocSession("UpdatePerfTest_5::tearDown", conn);
	//m_currTab->close(session, false);
	m_db->closeTable(session, m_currTab);
	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(conn);
	m_currTab = NULL;
	m_db->close();
	delete m_db;
	m_db = NULL;
	delete m_cfg;
}

void UpdatePerfTest_6::setUp() {
	m_cfg = getConfig(m_scale);
	if (m_cfg->m_maxSessions < m_threadCnt) m_cfg->m_maxSessions = m_threadCnt;
	m_recCnt = m_cfg->m_pageBufSize * Limits::PAGE_SIZE * m_dataSizeFact / m_avgRecSize; // 概数
	m_dataSize = m_recCnt * m_avgRecSize;
	m_opCnt = m_threadCnt * m_thdLoop;
}

void UpdatePerfTest_6::loadData(u64 *totalRecSize, u64 *recCnt) {
	Database::drop(m_cfg->m_basedir);
	m_db = Database::open(m_cfg, true);

	Connection *conn = m_db->getConnection(false);
	Session *session = m_db->getSessionManager()->allocSession("UpdatePerfTest_6::loadData", conn);
	TableDef *tbDef = new TableDef(AccountTable::getTableDef(m_useMms));
	m_db->createTable(session, TABLE_NAME_ACCOUNT, tbDef);
	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(conn);

	vector<RowId> idRowIdVec;

	conn = m_db->getConnection(false);
	session = m_db->getSessionManager()->allocSession("UpdatePerfTest_5::loadData", conn);
	m_currTab = m_db->openTable(session, TABLE_NAME_ACCOUNT);

	for (u64 i = 0; i < m_recCnt; ++i) {
		int recSize;
		Record *rec = AccountTable::createRecord((u64)RandomGen::nextInt(), &recSize, 150, m_avgRecSize);
		uint dupIdx;
		RowId rid = m_currTab->insert(session, rec->m_data, &dupIdx);
		freeRecord(rec);
		if (rid == INVALID_ROW_ID) {
			--i;
			continue;
		}
		idRowIdVec.push_back(rid);
	}


	// 插入删除更新各5000次
	// 5000 insert
	for (int i = 0; i < 5000; ++i) {
		int recSize;
		Record *rec = AccountTable::createRecord((u64)RandomGen::nextInt(), &recSize, 150, m_avgRecSize);
		uint dupIdx;
		RowId rid = m_currTab->insert(session, rec->m_data, &dupIdx);
		freeRecord(rec);
		if (rid == INVALID_ROW_ID) {
			--i;
			continue;
		}
		idRowIdVec.push_back(rid);
	}
	// 5000 del
	u16 delCols[] = {ACCOUNT_ID_CNO};
	byte buf[Limits::PAGE_SIZE];
	TblScan *scanHdl = m_currTab->positionScan(session, OP_DELETE, 1, delCols);
	for (int i = 0; i < 5000; ++i) {
		int idx = RandomGen::nextInt(0, idRowIdVec.size());
		RowId rid = idRowIdVec[idx];
		if (rid == 0) {
			--i;
			continue;
		}
		m_currTab->getNext(scanHdl, buf, rid);
		m_currTab->deleteCurrent(scanHdl);
		idRowIdVec[idx] = 0;
	}
	m_currTab->endScan(scanHdl);
	// 5000 update
	u16 updCols[] = {ACCOUNT_ID_CNO};
	scanHdl = m_currTab->positionScan(session, OP_UPDATE, 1, updCols);
	scanHdl->setUpdateColumns(1, updCols);
	for (int i = 0; i < 5000; ++i) {
		int idx = RandomGen::nextInt(0, idRowIdVec.size());
		RowId rid = idRowIdVec[idx];
		if (rid == 0) {
			--i;
			continue;
		}
		int recSize;
		Record *upd = AccountTable::createRecord((u64)RandomGen::nextInt(), &recSize, 150, m_avgRecSize);
		m_currTab->getNext(scanHdl, buf, rid);
		bool success = m_currTab->updateCurrent(scanHdl, upd->m_data);
		freeRecord(upd);
		if (!success) { // 主键索引冲突
			--i;
			continue;
		}
	}
	m_currTab->endScan(scanHdl);
	assert(idRowIdVec.size() == m_recCnt + 5000);

	Connection *conn2 = m_db->getConnection(false);
	Session *sess2 = m_db->getSessionManager()->allocSession("UpdatePerfTest_6::loadData", conn2);
	m_db->createTable(sess2, PKEY_TBL_NAME, new TableDef(CountTable::getTableDef(false)));
	Table *pkeyTbl = m_db->openTable(sess2, PKEY_TBL_NAME);
	Record *sr = CountTable::createRecord(0, 0);

	scanHdl = m_currTab->positionScan(session, OP_READ, 1, delCols);
	int count = 0;
	for (vector<RowId>::iterator it = idRowIdVec.begin(); it != idRowIdVec.end(); ++it) {
		if (*it == 0) continue;
		uint dupIdx;
		assert(*it != INVALID_ROW_ID);
		NTSE_ASSERT(m_currTab->getNext(scanHdl, buf, *it));
		u64 ID = RedRecord::readBigInt(m_currTab->getTableDef(), buf, ACCOUNT_ID_CNO); // 读出ID
		RedRecord::writeNumber(pkeyTbl->getTableDef(), COUNT_ID_CNO, sr->m_data, ID); // 写入ID到记录
		RowId prid = pkeyTbl->insert(sess2, sr->m_data, &dupIdx);
		assert(prid != INVALID_ROW_ID);
		++count;
	}
	m_currTab->endScan(scanHdl);
	assert(count == m_recCnt);
	m_db->closeTable(sess2, pkeyTbl);
	m_db->getSessionManager()->freeSession(sess2);
	m_db->freeConnection(conn2);



	m_db->closeTable(session, m_currTab);
	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(conn);
	m_db->close();

	*totalRecSize = m_dataSize;
	*recCnt = m_recCnt;
	

}

void UpdatePerfTest_6::warmUp() {
	m_db = Database::open(m_cfg, false);

	Connection *conn2 = m_db->getConnection(false);
	Session *sess2 = m_db->getSessionManager()->allocSession("UpdatePerfTest_6::warmUp", conn2);
	Table *pkeyTbl = m_db->openTable(sess2, PKEY_TBL_NAME);
	SubRecordBuilder srb(pkeyTbl->getTableDef(), REC_REDUNDANT);
	u64 id = 0;
	SubRecord *pkey = srb.createSubRecordByName(COUNT_ID, &id);
	TblScan *scanHdl = pkeyTbl->tableScan(sess2, OP_READ, pkey->m_numCols, pkey->m_columns);
	while (pkeyTbl->getNext(scanHdl, pkey->m_data)) {
		id = RedRecord::readBigInt(pkeyTbl->getTableDef(), pkey->m_data, COUNT_ID_CNO);
		m_idVec.push_back(id);
	}
	pkeyTbl->endScan(scanHdl);
	freeSubRecord(pkey);
	m_db->closeTable(sess2, pkeyTbl);
	m_db->getSessionManager()->freeSession(sess2);
	m_db->freeConnection(conn2);

	assert(m_idVec.size() == m_recCnt);

	/* 至此，所有可用id已经放在一个vector里面了 */

	Connection *conn = m_db->getConnection(false);
	Session *session = m_db->getSessionManager()->allocSession("UpdatePerfTest_6::warmUp", conn);
	m_currTab = m_db->openTable(session, TABLE_NAME_ACCOUNT);
	indexFullScan(m_db, m_currTab);
	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(conn);

}

void UpdatePerfTest_6::tearDown() {
 	Connection *conn = m_db->getConnection(false);
	Session *session = m_db->getSessionManager()->allocSession("UpdatePerfTest_6::tearDown", conn);
	m_db->closeTable(session, m_currTab);
	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(conn);
	m_currTab = NULL;
	m_db->close();
	delete m_db;
	m_db = NULL;
	delete m_cfg;
}

void UpdatePerfTest_6::run() {
	assert(m_idVec.size() == m_recCnt);

	Updater_6 **updaters = new Updater_6*[m_threadCnt];
	for (uint i = 0; i < m_threadCnt; ++i) {
		updaters[i] = new Updater_6(m_db, m_currTab, i, &m_idVec, m_thdLoop);
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
