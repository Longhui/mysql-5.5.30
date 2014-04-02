#include "TestHashIndex.h"
#include "Test.h"

const char* HashIndexTestCase::getName() {
	return "Hash index test";
}

const char* HashIndexTestCase::getDescription() {
	return "Test various operations of hash index which is used in tnt heap";
}

bool HashIndexTestCase::isBig() {
	return false;
}

void HashIndexTestCase::setUp() {
	uint targetSize = 500;
	uint additionalSize = 600;
	m_pool = new PagePool(4, Limits::PAGE_SIZE);
	m_pageManager = new TNTIMPageManager(targetSize, additionalSize, m_pool);
	m_pool->registerUser(m_pageManager);
	m_pool->init();

	m_ctx = new MemoryContext(Limits::PAGE_SIZE, 20);
}

void HashIndexTestCase::tearDown() {
	delete m_pool;
	m_pool = NULL;
	delete m_pageManager;
	m_pageManager = NULL;
	
	delete m_ctx;
	m_ctx = NULL;
}

void HashIndexTestCase::insert2HashIndex(HashIndex *hashIndex, u32 total) {
	RowId rowId;
	u64   value = 0;
	u16   version;
	HashIndexType indexType;
	HashIndexEntry *indexEntry = NULL;

	u32 i = 0;
	u64 sp = 0;

	for (i = 1; i <= total; i++) {
		rowId = i;
		value = (i << 20) | (i << 10);
		if (i % 2 == 0) {
			version = 100;
			indexType = HIT_TXNID;
		} else {
			version = 101;
			indexType = HIT_MHEAPREC;
		}
		CPPUNIT_ASSERT(hashIndex->insert(rowId, value, version, indexType) == true);
	}

	for (rowId = 1; rowId <= total; rowId++) {
		sp = m_ctx->setSavepoint();
		indexEntry = hashIndex->get(rowId, m_ctx);
		CPPUNIT_ASSERT(indexEntry != NULL);
		CPPUNIT_ASSERT(indexEntry->m_rowId == rowId);
		CPPUNIT_ASSERT((indexEntry->m_value) >> 10 == (rowId << 10) + rowId);
		CPPUNIT_ASSERT((indexEntry->m_value) >> 20 == rowId);
		if (rowId % 2 == 0) {
			CPPUNIT_ASSERT(indexEntry->m_version == 100);
			CPPUNIT_ASSERT(indexEntry->m_type == HIT_TXNID);
		} else {
			CPPUNIT_ASSERT(indexEntry->m_version == 101);
			CPPUNIT_ASSERT(indexEntry->m_type == HIT_MHEAPREC);
		}
		m_ctx->resetToSavepoint(sp);
	}

	for (i = 1; i < 10; i++) {
		rowId = total + i;
		CPPUNIT_ASSERT(!hashIndex->get(rowId, m_ctx));
	}
}

void HashIndexTestCase::testInsert() {
	u32 total = 100;
	HashIndex *hashIndex = new HashIndex(m_pageManager);
	insert2HashIndex(hashIndex, total);

	delete hashIndex;
}

void HashIndexTestCase::testRemove() {
	u32 i = 0;
	u32 total = 100;
	HashIndex *hashIndex = new HashIndex(m_pageManager);
	insert2HashIndex(hashIndex, total);
	RowId rowId;
	u64   sp;
	for (rowId = 1; rowId <= total; rowId++) {
		sp = m_ctx->setSavepoint();
		CPPUNIT_ASSERT(hashIndex->get(rowId, m_ctx) != NULL);
		CPPUNIT_ASSERT(hashIndex->remove(rowId));
		CPPUNIT_ASSERT(!hashIndex->get(rowId, m_ctx));
		m_ctx->resetToSavepoint(sp);
	}

	for (i = 1; i < 10; i++) {
		rowId = total + i;
		CPPUNIT_ASSERT(!hashIndex->remove(rowId));
	}

	delete hashIndex;
}

void HashIndexTestCase::testInsertOrUpdate() {
	RowId rowId;
	u64   value = NULL;
	u16   version;
	HashIndexType indexType;
	HashIndexEntry *indexEntry = NULL;
	HashIndex *hashIndex = new HashIndex(m_pageManager);

	u32 total = 50;
	u32 i = 0;
	u64 sp = 0;

	for (i = 1; i <= total; i++) {
		rowId = i;
		value = (i << 20) | (i << 10);
		if (i % 2 == 0) {
			version = 100;
			indexType = HIT_TXNID;
		} else {
			version = 101;
			indexType = HIT_MHEAPREC;
		}
		CPPUNIT_ASSERT(!hashIndex->insertOrUpdate(rowId, value, version, indexType));
	}

	for (rowId = 1; rowId <= total; rowId++) {
		sp = m_ctx->setSavepoint();
		indexEntry = hashIndex->get(rowId, m_ctx);
		CPPUNIT_ASSERT(indexEntry != NULL);
		CPPUNIT_ASSERT(indexEntry->m_rowId == rowId);
		CPPUNIT_ASSERT((indexEntry->m_value) >> 10 == (rowId << 10) + rowId);
		CPPUNIT_ASSERT((indexEntry->m_value) >> 20 == rowId);
		if (rowId % 2 == 0) {
			CPPUNIT_ASSERT(indexEntry->m_version == 100);
			CPPUNIT_ASSERT(indexEntry->m_type == HIT_TXNID);
		} else {
			CPPUNIT_ASSERT(indexEntry->m_version == 101);
			CPPUNIT_ASSERT(indexEntry->m_type == HIT_MHEAPREC);
		}
		m_ctx->resetToSavepoint(sp);
	}

	for (i = 1; i < 10; i++) {
		rowId = total + i;
		CPPUNIT_ASSERT(!hashIndex->get(rowId, m_ctx));
	}

	for (rowId = 1; rowId <= total; rowId++) {
		if (rowId % 2 == 0) {
			version = 101;
		} else {
			version = 100;
		}
		CPPUNIT_ASSERT(hashIndex->insertOrUpdate(rowId, value, version, HIT_NONE));
	}

	for (rowId = 1; rowId <= total; rowId++) {
		sp = m_ctx->setSavepoint();
		indexEntry = hashIndex->get(rowId, m_ctx);
		CPPUNIT_ASSERT(indexEntry != NULL);
		CPPUNIT_ASSERT(indexEntry->m_rowId == rowId);
		CPPUNIT_ASSERT(indexEntry->m_value == value);
		if (rowId % 2 == 0) {
			CPPUNIT_ASSERT(indexEntry->m_version == 101);
			CPPUNIT_ASSERT(indexEntry->m_type == HIT_TXNID);
		} else {
			CPPUNIT_ASSERT(indexEntry->m_version == 100);
			CPPUNIT_ASSERT(indexEntry->m_type == HIT_MHEAPREC);
		}
		m_ctx->resetToSavepoint(sp);
	}

	/*目前对不存在的rowId进行update直接报assert错误
	for (i = 1; i < 10; i++) {
		rowId = total + i;
		CPPUNIT_ASSERT(!hashIndex->update(rowId, ptr, 1000));
	}*/

	delete hashIndex;
}

void HashIndexTestCase::testUpdate() {
	u64 sp = 0;
	u32 i = 0;
	u32 total = 100;
	u32 rowId = 0;
	u16 version = 0;
	HashIndexEntry *indexEntry = NULL;
	u64 value = 0x00120012;
	HashIndex *hashIndex = new HashIndex(m_pageManager);
	insert2HashIndex(hashIndex, total);

	for (rowId = 1; rowId <= total; rowId++) {
		if (rowId % 2 == 0) {
			version = 101;
		} else {
			version = 100;
		}
		hashIndex->update(rowId, value, version);
	}

	for (rowId = 1; rowId <= total; rowId++) {
		sp = m_ctx->setSavepoint();
		indexEntry = hashIndex->get(rowId, m_ctx);
		CPPUNIT_ASSERT(indexEntry != NULL);
		CPPUNIT_ASSERT(indexEntry->m_rowId == rowId);
		CPPUNIT_ASSERT(indexEntry->m_value == value);
		if (rowId % 2 == 0) {
			CPPUNIT_ASSERT(indexEntry->m_version == 101);
			CPPUNIT_ASSERT(indexEntry->m_type == HIT_TXNID);
		} else {
			CPPUNIT_ASSERT(indexEntry->m_version == 100);
			CPPUNIT_ASSERT(indexEntry->m_type == HIT_MHEAPREC);
		}
		m_ctx->resetToSavepoint(sp);
	}

	/*目前对不存在的rowId进行update直接报assert错误
	for (i = 1; i < 10; i++) {
		rowId = total + i;
		CPPUNIT_ASSERT(!hashIndex->update(rowId, ptr, 1000));
	}*/

	delete hashIndex;
}

void HashIndexTestCase::testCount() {
	int total = 5;
	HashIndex *hashIndex = new HashIndex(m_pageManager);
	insert2HashIndex(hashIndex, total);
	
	hashIndex->remove(3);
	hashIndex->remove(1);
	hashIndex->remove(4);
	hashIndex->remove(2);
	CPPUNIT_ASSERT(hashIndex->m_mapEntries[0]->m_valuePool.m_data.getSize() == total);
	hashIndex->remove(5);
	CPPUNIT_ASSERT(hashIndex->m_mapEntries[0]->m_valuePool.m_data.isEmpty());
	delete hashIndex;
}

void HashIndexTestCase::testDefrag() {
	RowId rowId;
	u64   value = 0;
	TrxId minReadView = 0;
	u16   version;
	HashIndexType indexType;
	HashIndexEntry *indexEntry = NULL;
	HashIndex *hashIndex = new HashIndex(m_pageManager);

	u32 total = 50;
	u32 mid = total/2;
	u32 i = 0;
	u64 sp = 0;

	for (i = 1; i <= total; i++) {
		rowId = RID(i, i);
		value = (i << 20) | (i << 10);
		version = 100;
		indexType = HIT_TXNID;
		CPPUNIT_ASSERT(hashIndex->insert(rowId, value, version, indexType));
	}

	for (i = 1; i <= total; i++) {
		rowId = RID(i, i);
		sp = m_ctx->setSavepoint();
		indexEntry = hashIndex->get(rowId, m_ctx);
		CPPUNIT_ASSERT(indexEntry != NULL);
		CPPUNIT_ASSERT(indexEntry->m_rowId == rowId);
		CPPUNIT_ASSERT((indexEntry->m_value) >> 10 == (i << 10) + i);
		CPPUNIT_ASSERT((indexEntry->m_value) >> 20 == i);
		CPPUNIT_ASSERT(indexEntry->m_version == 100);
		CPPUNIT_ASSERT(indexEntry->m_type == HIT_TXNID);
		m_ctx->resetToSavepoint(sp);
	}

	File dir("testdb");
	dir.rmdir(true);
	dir.mkdir();
	TNTConfig config;
	TNTDatabase *db = NULL;
	config.setNtseBasedir("testdb");
	config.setTntBasedir("testdb");
	config.setTntDumpdir("testdb");
	config.m_tntLogLevel = EL_WARN;
	config.m_tntBufSize = 200;
	//config.m_tntLogfileSize = 1024 * 1024 * 128;
	config.m_ntseConfig.m_logFileSize = 128 << 20;
	config.m_verpoolCnt = 2;
	config.m_purgeBeforeClose = false;
	config.m_dumpBeforeClose = false;

	EXCPT_OPER(db = TNTDatabase::open(&config, true, 0));
	Database *ntsedb = db->getNtseDb();
	Connection *conn = ntsedb->getConnection(false);
	Session *session = ntsedb->getSessionManager()->allocSession("HashIndexTestCase", conn);
	TNTTrxSys *trxSys = db->getTransSys();
	TNTTransaction *trx = startTrx(trxSys, conn);
	session->setTrans(trx);

	minReadView = (mid << 20) | (mid << 10);
	u32 count = hashIndex->defrag(session, 5, minReadView, 32*100);
	CPPUNIT_ASSERT(count == mid - 1);
	commitTrx(trxSys, trx);
	ntsedb->getSessionManager()->freeSession(session);
	ntsedb->freeConnection(conn);
	db->close(false, false);
	delete db;
	dir.rmdir(true);

	for (i = 1; i <= total; i++) {
		rowId = RID(i, i);
		sp = m_ctx->setSavepoint();
		indexEntry = hashIndex->get(rowId, m_ctx);
		if (i < mid) {
			CPPUNIT_ASSERT(indexEntry == NULL);
		} else {
			CPPUNIT_ASSERT(indexEntry != NULL);
			CPPUNIT_ASSERT(indexEntry->m_rowId == rowId);
			CPPUNIT_ASSERT((indexEntry->m_value) >> 10 == (i << 10) + i);
			CPPUNIT_ASSERT((indexEntry->m_value) >> 20 == i);
			CPPUNIT_ASSERT(indexEntry->m_version == 100);
			CPPUNIT_ASSERT(indexEntry->m_type == HIT_TXNID);
		}
		m_ctx->resetToSavepoint(sp);
	}

	delete hashIndex;
}

TNTTransaction *HashIndexTestCase::startTrx(TNTTrxSys *trxSys, Connection *conn, TLockMode lockMode/* = TL_NO*/,  bool log/* = false*/) {
	TNTTransaction *trx = trxSys->allocTrx();
	if (!log) {
		trx->disableLogging();
	}
	trx->startTrxIfNotStarted(conn);
	return trx;
}

void HashIndexTestCase::commitTrx(TNTTrxSys *trxSys, TNTTransaction *trx) {
	trx->commitTrx(CS_NORMAL);
	trxSys->freeTrx(trx);
}