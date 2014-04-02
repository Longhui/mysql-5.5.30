
#include "TNTDelete.h"

#include "TNTDbConfigs.h"
#include "api/TNTTable.h"
#include "api/TNTDatabase.h"
#include "api/Table.h"
#include "api/Database.h"
#include "misc/Session.h"
#include "misc/RecordHelper.h"
#include "CountTable.h"
#include "LongCharTable.h"
#include "TNTAccountTable.h"
#include "util/Thread.h"
#include <sstream>
#include <iostream>
#include "Generator.h"
#include "util/File.h"
#include "TNTTableHelper.h"

using namespace tnt;
using namespace std;
using namespace tntperf;

string TNTDeleteTest::getName() const
{
	stringstream ss;
	ss << "TNT Delete Test, ";
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
string TNTDeleteTest::getDescription() const
{
    return "TNT Delete operation performance test.";
}

u64 TNTDeleteTest::getOpCnt()
{
	return m_threadCnt * m_recCntPerThd;
}

u64 TNTDeleteTest::getDataSize()
{
	return getOpCnt() * 200;
}

u64 TNTDeleteTest::getMillis()
{
	return m_totalMillis;
}

void TNTDeleteTest::loadData(u64 *totalRecSize, u64 *recCnt)
{
	openTable(true);
	Connection *conn = m_db->getNtseDb()->getConnection(false);
	Session *session = m_db->getNtseDb()->getSessionManager()->allocSession("TNTDeleteTest::loadData", conn);

	TNTOpInfo opInfo;
	::initTNTOpInfo(&opInfo, TL_NO);
	TNTTransaction *trx = startTrx(m_db->getTransSys(), session);
    m_recCntLoaded = TNTAccountTable::populate(session, m_table,&opInfo,&m_totalRecSizeLoaded);
	commitTrx(m_db->getTransSys(), trx);
	m_db->getNtseDb()->getSessionManager()->freeSession(session);
	m_db->getNtseDb()->freeConnection(conn);
	closeDatabase();
	*totalRecSize = m_totalRecSizeLoaded;
	*recCnt = m_recCntLoaded;
}

void TNTDeleteTest::warmUp()
{
	openTable(false);
	//m_isVar ? AccountTable::scanTable(m_db, m_table) : CountTable::scanTable(m_db, m_table);
    TNTAccountTable::scanTable(m_db, m_table);
	if (m_recInMms) {
		u16 columns[2] = {0, 1};
		u64 scaned = ::TNTIndex0Scan(m_db, m_table, 2, columns);
		//NTSE_ASSERT(m_table->getNtseTable()->getMmsTable()->getStatus().m_records == scaned);
	}
}

void TNTDeleteTest::run()
{

	TNTDeleter *deleter = new TNTDeleter(&m_IdRowIdVec, 0, m_recCntPerThd, m_db, m_table);
	u64 start = System::currentTimeMillis();

	deleter->start();
	deleter->join();

	u64 end = System::currentTimeMillis();
	m_totalMillis = end - start;

	delete deleter;

}

void TNTDeleteTest::setUp()
{
	switch (m_scale) {
		case SMALL:
			setConfig(TNTCommonDbConfig::getSmall());
			//m_config = new Config(CommonDbConfig::getSmall());
			break;
		case MEDIUM:
			//m_config = new Config(CommonDbConfig::getMedium());
			setConfig(TNTCommonDbConfig::getMedium());
			break;
	}

    setTableDef(TNTAccountTable::getTableDef(m_useMms));

	if (m_config->m_ntseConfig.m_maxSessions < m_threadCnt) m_config->m_ntseConfig.m_maxSessions = m_threadCnt;

	//TNTDatabase::drop(m_config->m_basedir);
	//m_db = TNTDatabase::open(m_config, true);


	u64 dataSize = m_config->m_ntseConfig.m_pageBufSize * Limits::PAGE_SIZE * m_dataSizeFact;

	m_recCntPerThd = dataSize /2 / 200 / m_threadCnt;
	m_recCntLoaded = (dataSize / 200 / m_threadCnt) * m_threadCnt;
	//m_db->close();
}

void TNTDeleter::run()
{
	TNTOpInfo opInfo;
	initTNTOpInfo(&opInfo, TL_NO);
	Database *ntsedb = m_db->getNtseDb();
	Connection *conn = ntsedb->getConnection(false);
	Session *session = ntsedb->getSessionManager()->allocSession("tntperf::TNTDeleter", conn);
	TNTTransaction *trx = startTrx(m_db->getTransSys(), session);

	byte buf[Limits::PAGE_SIZE];
	u16 readCols[1] = {0};
	TNTTblScan *scanHdl = m_tbl->positionScan(session,OP_DELETE,&opInfo, 1, readCols);

	for (uint i = 0; i < m_delCnt; ++i) {
		u64 ID = i + m_threadId * m_delCnt;
		RowId rid = (*m_ridVec)[ID];
		m_tbl->getNext(scanHdl, buf, rid);
		m_tbl->deleteCurrent(scanHdl);
	}

	m_tbl->endScan(scanHdl);

	commitTrx(m_db->getTransSys(), trx);
	ntsedb->getSessionManager()->freeSession(session);
	ntsedb->freeConnection(conn);
}
