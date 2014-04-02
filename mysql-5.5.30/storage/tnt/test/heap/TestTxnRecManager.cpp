#include "TestTxnRecManager.h"
#include "util/File.h"
#include "api/Database.h"
#include "Test.h"

const char* TxnRecManagerTestCase::getName() {
	return "TNT transaction record manager test";
}

const char* TxnRecManagerTestCase::getDescription() {
	return "Test various operations of tnt transactions record manager which is used for updating record firstly";
}

bool TxnRecManagerTestCase::isBig() {
	return false;
}

void TxnRecManagerTestCase::setUp() {
	uint targetSize = 50;
	m_pool = new PagePool(4, Limits::PAGE_SIZE);
	m_pageManager = new TNTIMPageManager(targetSize, m_pool);
	m_pool->registerUser(m_pageManager);
	m_pool->init();

	File dir("testdb");
	dir.rmdir(true);
	dir.mkdir();
	m_config.setBasedir("testdb");
	m_config.m_logLevel = EL_WARN;
	m_config.m_pageBufSize = 50;
	m_config.m_mmsSize = 20;
	m_config.m_logFileSize = 1024 * 1024 * 128;
	EXCPT_OPER(m_db = Database::open(&m_config, true));

	m_conn = m_db->getConnection(false);
	m_session = m_db->getSessionManager()->allocSession("TxnRecManagerTestCase", m_conn);

	m_txnRecManager = new TxnRecManager(m_pageManager);
	m_txnRecManager->init(m_session);
}

void TxnRecManagerTestCase::tearDown() {
	delete m_txnRecManager;
	m_txnRecManager = NULL;

	delete m_pool;
	m_pool = NULL;
	delete m_pageManager;
	m_pageManager = NULL;

	m_db->getSessionManager()->freeSession(m_session);
	m_db->freeConnection(m_conn);
	m_db->close(false, false);

	delete m_db;
	m_db = NULL;

	File dir("testdb");
	dir.rmdir(true);
}

void TxnRecManagerTestCase::testPush() {
	TxnRecPage *curPage = NULL;
	TxnRec *rec = NULL;
	u32 i = 0;
	CPPUNIT_ASSERT(m_txnRecManager->m_curPage == m_txnRecManager->m_head);
	for (i = 1; i <= m_txnRecManager->m_recPerPage; i++) {
		TxnRec txnRec(i, i);
		rec = m_txnRecManager->push(m_session, &txnRec);
		CPPUNIT_ASSERT((byte *)rec == (byte *)m_txnRecManager->m_curPage + sizeof(TxnRecPage) + (i - 1)*sizeof(TxnRec));
		CPPUNIT_ASSERT(rec->m_rowId == txnRec.m_rowId);
		CPPUNIT_ASSERT(rec->m_txnId == txnRec.m_txnId);
		CPPUNIT_ASSERT(m_txnRecManager->m_curPage->m_minTxnId == 1);
		CPPUNIT_ASSERT(m_txnRecManager->m_curPage->m_maxTxnId == i);
		CPPUNIT_ASSERT(m_txnRecManager->m_curPage->m_recCnt == i);
	}

	//当前页已经满页
	curPage = m_txnRecManager->m_curPage;
	rec = m_txnRecManager->push(m_session, &TxnRec(i, i));
	CPPUNIT_ASSERT(curPage != m_txnRecManager->m_curPage);
	CPPUNIT_ASSERT(m_txnRecManager->m_curPage == m_txnRecManager->m_head->m_next);
	CPPUNIT_ASSERT((byte *)rec == (byte *)m_txnRecManager->m_curPage + sizeof(TxnRecPage));
	CPPUNIT_ASSERT(rec->m_rowId == i);
	CPPUNIT_ASSERT(rec->m_txnId == i);
	CPPUNIT_ASSERT(m_txnRecManager->m_curPage->m_minTxnId == i);
	CPPUNIT_ASSERT(m_txnRecManager->m_curPage->m_maxTxnId == i);
	CPPUNIT_ASSERT(m_txnRecManager->m_curPage->m_recCnt == 1);
	i++;

	//在新的页面插入事务记录
	for (; i <= 2*m_txnRecManager->m_recPerPage; i++) {
		TxnRec txnRec(i, i);
		rec = m_txnRecManager->push(m_session, &txnRec);
		CPPUNIT_ASSERT((byte *)rec == (byte *)m_txnRecManager->m_curPage + sizeof(TxnRecPage) + (i - m_txnRecManager->m_recPerPage - 1)*sizeof(TxnRec));
		CPPUNIT_ASSERT(rec->m_rowId == txnRec.m_rowId);
		CPPUNIT_ASSERT(rec->m_txnId == txnRec.m_txnId);
		CPPUNIT_ASSERT(m_txnRecManager->m_curPage->m_minTxnId == m_txnRecManager->m_recPerPage + 1);
		CPPUNIT_ASSERT(m_txnRecManager->m_curPage->m_maxTxnId == i);
		CPPUNIT_ASSERT(m_txnRecManager->m_curPage->m_recCnt == i - m_txnRecManager->m_recPerPage);
	}
}

void TxnRecManagerTestCase::testDefrag() {
	u16 version = 10;
	u32 i = 0;
	u32 totalPage = 20;
	u32 totalRecSize = m_txnRecManager->m_recPerPage*totalPage;
	TxnRec *rec = NULL;
	HashIndex *hashIndex = new HashIndex(m_pageManager);
	TxnRecPage *curPage = NULL;
	u64 sp = 0;
	MemoryContext *ctx = new MemoryContext(Limits::PAGE_SIZE, 20);
	for (i = 1; i <= totalRecSize; i++) {
		TxnRec txnRec(i, i);
		rec = m_txnRecManager->push(m_session, &txnRec);
		curPage = m_txnRecManager->m_curPage;
		CPPUNIT_ASSERT((byte *)rec == (byte *)curPage + sizeof(TxnRecPage) + ((i - 1) % m_txnRecManager->m_recPerPage)*sizeof(TxnRec));
		CPPUNIT_ASSERT(rec->m_rowId == txnRec.m_rowId);
		CPPUNIT_ASSERT(rec->m_txnId == txnRec.m_txnId);
		CPPUNIT_ASSERT(curPage->m_recCnt == (i - 1) % m_txnRecManager->m_recPerPage + 1);
		CPPUNIT_ASSERT(hashIndex->insert(txnRec.m_rowId, rec, 10, TXNID));
	}

	u32 limit = 10;
	CPPUNIT_ASSERT(totalPage > limit);
	u32 limitRecSize = m_txnRecManager->m_recPerPage*limit;
	HashIndexEntry *indexEntry = NULL;
	for (i = 1; i <= limitRecSize; i++) {
		sp = ctx->setSavepoint();
		indexEntry = hashIndex->get(i, ctx);
		CPPUNIT_ASSERT(indexEntry != NULL);
		ctx->resetToSavepoint(sp);
	}

	m_txnRecManager->defrag(m_session, limitRecSize + 1, hashIndex);
	sp = ctx->setSavepoint();
	for (i = 1; i <= limitRecSize; i++) {
		indexEntry = hashIndex->get(i, ctx);
		CPPUNIT_ASSERT(indexEntry == NULL);
	}
	ctx->resetToSavepoint(sp);

	u32 validPage = 0;
	TxnRecPage *p = NULL;
	for (p = m_txnRecManager->m_head; p != m_txnRecManager->m_curPage->m_next; p = p->m_next) {
		validPage++;
		CPPUNIT_ASSERT(p->m_next->m_prev == p);
		CPPUNIT_ASSERT(p->m_maxTxnId >= (limitRecSize + 1));
	}
	CPPUNIT_ASSERT(validPage == (totalPage - limit));

	delete ctx;
	ctx = NULL;
	delete hashIndex;
	hashIndex = NULL;
}

void TxnRecManagerTestCase::testPageAllocAndFree() {
	u16 total = 0;
	u16 incrSize = 8;
	//txnRecManager初始化时也会分配一定数量的页面
	u16 srcPageSize = m_txnRecManager->m_pageSize;
	m_txnRecManager->allocPage(m_session, incrSize);
	CPPUNIT_ASSERT(m_txnRecManager->m_pageSize == (incrSize + srcPageSize));
	TxnRecPage *head = m_txnRecManager->m_head;
	TxnRecPage *tail = m_txnRecManager->m_tail;
	TxnRecPage *p = NULL;
	for (p = head; p != NULL; p = p->m_next) {
		total++;
		if (p == tail) {
			break;
		}
		CPPUNIT_ASSERT(p->m_next->m_prev == p);
	}
	CPPUNIT_ASSERT(p->m_next == NULL);
	CPPUNIT_ASSERT(total == (incrSize + srcPageSize));

	//将allocPage分配来的页面全free
	u16 realFreePageSize = m_txnRecManager->freeSomePage(m_session, incrSize);
	CPPUNIT_ASSERT(realFreePageSize == incrSize);
	total = 0;
	tail = m_txnRecManager->m_tail;
	for (p = head; p != NULL; p = p->m_next) {
		total++;
		if (p == tail) {
			break;
		}
		CPPUNIT_ASSERT(p->m_next->m_prev == p);
	}
	CPPUNIT_ASSERT(p->m_next == NULL);
	CPPUNIT_ASSERT(total == srcPageSize);
	CPPUNIT_ASSERT(srcPageSize == m_txnRecManager->m_pageSize);

	realFreePageSize = m_txnRecManager->freeSomePage(m_session, srcPageSize);
	CPPUNIT_ASSERT((realFreePageSize + 1) == srcPageSize);
	//只剩下最后一个页面
	CPPUNIT_ASSERT(m_txnRecManager->m_pageSize == 1);
	CPPUNIT_ASSERT(m_txnRecManager->m_head == m_txnRecManager->m_curPage);
	CPPUNIT_ASSERT(m_txnRecManager->m_head == m_txnRecManager->m_tail);
	CPPUNIT_ASSERT(m_txnRecManager->m_head->m_next == NULL);
}