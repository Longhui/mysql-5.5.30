#include "TNTPurge.h"
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
TNTPurgePerfTest::TNTPurgePerfTest(const char * tableName, Scale scale, bool useMms)
{
	/* 根据表名判断表分类 */
	m_tableName = tableName;
	if (0 == strcmp(m_tableName, TNTTABLE_NAME_COUNT)) {
		m_testTable = COUNT_TABLE;
		m_ratio = 0.7;
	} else if (0 == strcmp(m_tableName, TNTTABLE_NAME_LONGCHAR)) {
		m_testTable = LONGCHAR_TABLE;
		m_ratio = 0.7;
	} else if (0 == strcmp(m_tableName, TNTTABLE_NAME_ACCOUNT)) {
		m_testTable = ACCOUNT_TABLE;
		m_ratio = 0.7;
	} else {
		assert(false);
	}
	m_scale = scale;
	m_useMms = useMms;
	m_enableCache = false;
	m_recCnt = 0;
	m_dataSize = 0;
	m_totalMillis = 0;
}

TNTPurgePerfTest::~TNTPurgePerfTest(void)
{
}

string TNTPurgePerfTest::getName() const {
	stringstream ss;
	ss << "TNT Purge Test: ";
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
		case BIG:
			ss << "[Big] ";
			break;
	}

	if (m_useMms)
		ss << "with mms";
	
	return ss.str();
}

string TNTPurgePerfTest::getDescription() const {
	return "TNT Purge operation performance test.";
}

void TNTPurgePerfTest::setUp() {
	switch (m_scale) {
		case SMALL:
			setConfig(new TNTConfig(TNTCommonDbConfig::getSmall()));
			m_config->m_tntBufSize = 128*1024*1024/TNTIMPage::TNTIM_PAGE_SIZE;
			//m_config->m_tntBufSize = 32*1024*1024/TNTIMPage::TNTIM_PAGE_SIZE;
			m_config->m_ntseConfig.m_pageBufSize = 2*m_config->m_tntBufSize;
			m_config->m_ntseConfig.m_commonPoolSize = 128*1024*1024/TNTIMPage::TNTIM_PAGE_SIZE;
			break;
		case MEDIUM:
			setConfig(new TNTConfig(TNTCommonDbConfig::getMedium()));
			m_config->m_tntBufSize = 512*1024*1024/TNTIMPage::TNTIM_PAGE_SIZE;
			m_config->m_ntseConfig.m_pageBufSize = 2*m_config->m_tntBufSize;
			m_config->m_ntseConfig.m_commonPoolSize = 512*1024*1024/TNTIMPage::TNTIM_PAGE_SIZE;
			break;
	}
	m_config->m_purgeInterval = 10*60*60;
	m_config->m_purgeThreshold = 99;
	m_config->m_dumpInterval = 10*60*60;
	m_config->m_dumponRedoSize = 1024*1024*1024;
	m_config->m_ntseConfig.m_logLevel = EL_LOG;

	ntse::File dir(m_config->m_ntseConfig.m_basedir);
	dir.rmdir(true);
	dir.mkdir();
	m_db = TNTDatabase::open(m_config, true, 0);
}

void TNTPurgePerfTest::tearDown() {
	m_db->close();
	delete m_db;
	ntse::File dir(m_config->m_ntseConfig.m_basedir);
	dir.rmdir(true);

	delete m_config;
}

void TNTPurgePerfTest::run() {
	m_db->purgeTntim(PT_DUMPAFTERPURGE);
}

void TNTPurgePerfTest::loadData(u64 *totalRecSize, u64 *recCnt) {
	Database *ntsedb = m_db->getNtseDb();
	Connection *conn = ntsedb->getConnection(false);
	Session *session = ntsedb->getSessionManager()->allocSession("TNTPurgePerfTest::loadData", conn);
	TableDef *tblDef = NULL;
	if (m_testTable == COUNT_TABLE) {
		tblDef = new TableDef(TNTCountTable::getTableDef(m_useMms));
	} else if (m_testTable == LONGCHAR_TABLE) {
		tblDef = new TableDef(TNTLongCharTable::getTableDef(m_useMms));
	} else if (m_testTable == ACCOUNT_TABLE) {
		tblDef = new TableDef(TNTAccountTable::getTableDef(m_useMms));
	} else {
		assert(false);
	}

	TNTTransaction *trx = startTrx(m_db->getTransSys(), session);
	m_db->createTable(session, m_tableName, tblDef);
	commitTrx(m_db->getTransSys(), trx);
	ntsedb->getSessionManager()->freeSession(session);
	ntsedb->freeConnection(conn);

	TNTOpInfo opInfo;
	int recSize = 0;
	//记录大小与内存堆真正存放数据与hash索引值的差值
	uint deltaSize = sizeof(HashIndexEntry) + sizeof(MHeapRec) - sizeof(byte *) - sizeof(uint);
	uint dupIdx = (uint)(-1);
	uint totalRecBufSize = (uint)(m_config->m_tntBufSize*TNTIMPage::TNTIM_PAGE_SIZE*m_ratio);
	while (m_dataSize < totalRecBufSize) {
		conn = ntsedb->getConnection(false);
		session = ntsedb->getSessionManager()->allocSession("TNTPurgePerfTest::loadData", conn);
		
		initTNTOpInfo(&opInfo, TL_X);
		trx = startTrx(m_db->getTransSys(), session);
		m_table = m_db->openTable(session, m_tableName);
		u64 i = 0;
		RowId rid = INVALID_ROW_ID;
		u64 sp1 = 0;
		while (m_dataSize < totalRecBufSize && i < BATCH_OP_SIZE) {
			i++;
			sp1 = session->getMemoryContext()->setSavepoint();
			Record *rec = NULL;
			if (m_testTable == COUNT_TABLE) {
				rec = TNTCountTable::createRecord(m_recCnt + i,  (int)((i + m_recCnt) % 20));
				recSize = TNTCountTable::getRecordSize();
			} else if (m_testTable == LONGCHAR_TABLE) {
				rec = TNTLongCharTable::createRecord(m_recCnt + i);
				recSize = TNTLongCharTable::getRecordSize();
			} else if (m_testTable == ACCOUNT_TABLE) {
				rec = TNTAccountTable::createRecord(m_recCnt + i, &recSize);
			} else {
				assert(false);
			}

			recSize += deltaSize;
			try {
				rid = m_table->insert(session, rec->m_data, &dupIdx, &opInfo);
			} catch (NtseException &) {
				assert(false);
			}
			assert(dupIdx == (uint)(-1));
			m_dataSize += recSize;
			freeRecord(rec);
			session->getMemoryContext()->resetToSavepoint(sp1);
		}

		m_db->closeTable(session, m_table);
		commitTrx(m_db->getTransSys(), trx);
		m_recCnt += i;
		ntsedb->getSessionManager()->freeSession(session);
		ntsedb->freeConnection(conn);
	}

	*totalRecSize = m_dataSize;
	*recCnt = m_recCnt;
}

//purge warm up主要做update和delete操作
void TNTPurgePerfTest::warmUp() {
	Database *ntsedb = m_db->getNtseDb();
	Connection *conn = ntsedb->getConnection(false);
	Session *session = ntsedb->getSessionManager()->allocSession("TNTPurgePerfTest::warmup", conn);

	TNTOpInfo opInfo;
	initTNTOpInfo(&opInfo, TL_X);
	TNTTransaction *trx = startTrx(m_db->getTransSys(), session);
	m_table = m_db->openTable(session, m_tableName);

	TableDef *tblDef = m_table->getNtseTable()->getTableDef();
	MemoryContext *ctx = session->getMemoryContext();
	u16 *updateCols = (u16 *)ctx->alloc(sizeof(u16));
	if (m_testTable == COUNT_TABLE) {
		updateCols[0] = 1;
	} else {
		assert(m_testTable == LONGCHAR_TABLE || m_testTable == ACCOUNT_TABLE);
		updateCols[0] = 0;
	}

	int count = 100;
	u64 id = m_recCnt + 1;
	Record *rec = NULL;
	byte *mysqlRow = (byte *)ctx->alloc(tblDef->m_maxRecSize*sizeof(byte));
	TNTTblScan *scan = m_table->tableScan(session, OP_UPDATE, &opInfo, 1, updateCols);
	scan->setUpdateSubRecord(1, updateCols);
	while (m_table->getNext(scan, mysqlRow)) {
		McSavepoint msp(ctx);
		rec = new (ctx->alloc(sizeof(Record))) Record(scan->getCurrentRid(), REC_REDUNDANT,
			(byte *)ctx->alloc(tblDef->m_maxRecSize), tblDef->m_maxRecSize);
		if (m_testTable == COUNT_TABLE) {
			rec = TNTCountTable::updateCount(rec, (count++) % 100);
		} else if (m_testTable == LONGCHAR_TABLE) {
			rec = TNTLongCharTable::updateId(rec, id++);
		} else if (m_testTable == ACCOUNT_TABLE) {
			rec = TNTAccountTable::updateId(rec, id++);
		} else {
			assert(false);
		}
		m_table->updateCurrent(scan, rec->m_data);
	}
	m_table->endScan(scan);

	commitTrx(m_db->getTransSys(), trx);
	m_db->closeTable(session, m_table);
	ntsedb->getSessionManager()->freeSession(session);
	ntsedb->freeConnection(conn);

	return;
}

u64 TNTPurgePerfTest::getOpCnt() {
	return m_recCnt;
}

u64 TNTPurgePerfTest::getDataSize() {
	return m_dataSize;
}

u64 TNTPurgePerfTest::getMillis() {
	return (u64)(-1);
}
}
