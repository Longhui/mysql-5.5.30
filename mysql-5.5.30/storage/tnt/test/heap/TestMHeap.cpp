#include "TestMHeap.h"
#include "util/File.h"
#include "misc/RecordHelper.h"
#include "Test.h"

#define TNT_STU_NAME "name"
#define TNT_STU_SNO "sno"
#define TNT_STU_AGE "age"
#define TNT_STU_SEX "sex"
#define TNT_STU_CLASS "class"
#define TNT_STU_GPA "gpa"
#define TNT_STU_GRADE "grade"

Record* createStudentRec(TableDef *tableDef, RowId rid, const char *name, u32 stuNo, u16 age, const char *sex, u32 clsNo, u64 grade) {
	RecordBuilder rb(tableDef, rid, REC_VARLEN);
	rb.appendVarchar(name);
	rb.appendInt(stuNo);
	rb.appendSmallInt(age);
	rb.appendChar(sex);
	rb.appendMediumInt(clsNo);
	rb.appendNull();
	rb.appendBigInt(grade);
	
	return rb.getRecord();
}

class InsertHeapRecThread: public Thread {
public:
	InsertHeapRecThread(Database *db, MHeap *heap, TableDef *tableDef): Thread("InsertHeapRecThread") {
		m_db = db;
		m_mheap = heap;
		m_tableDef = tableDef;
	}
	void run();
private:
	Database	  *m_db;
	MHeap         *m_mheap;
	TableDef      *m_tableDef;
};

void InsertHeapRecThread::run() {
	HashIndexEntry *indexEntry = NULL;
	u16 version = 20;
	Connection *conn = m_db->getConnection(false);
	Session *session = m_db->getSessionManager()->allocSession("InsertHeapRecThread::run", conn);
	MemoryContext *ctx = session->getMemoryContext();
	McSavepoint msp(ctx);

	u64 txnId1 = 90001;
	RowId rollBackId1 = 1001;
	u8 vTableIndex1 = 1;
	Record *rec1 = createStudentRec(m_tableDef, 2001/*rid*/, "netease1"/*name*/, 3001/*stuNo*/, 30/*age*/, "F"/*sex*/, 4001/*clsNo*/, 5001/*grade*/);
	u8 delbit1 = 0;
	CPPUNIT_ASSERT(m_mheap->insertHeapRecordAndHash(session, txnId1, rollBackId1, vTableIndex1, rec1, delbit1, version));
	indexEntry = m_mheap->m_hashIndexOperPolicy->get(rec1->m_rowId, ctx);
	CPPUNIT_ASSERT(indexEntry != NULL);
	CPPUNIT_ASSERT(indexEntry->m_type == HIT_MHEAPREC);
	void *ptr1 = (void *)indexEntry->m_value;
	CPPUNIT_ASSERT(ptr1 != NULL);

	u64 txnId2 = 90002;
	RowId rollBackId2 = 1002;
	u8 vTableIndex2 = 2;
	Record *rec2 = createStudentRec(m_tableDef, 2002/*rid*/, "netease2"/*name*/, 3002/*stuNo*/, 29/*age*/, "M"/*sex*/, 4002/*clsNo*/, 5002/*grade*/);
	u8 delbit2 = 0;
	CPPUNIT_ASSERT(m_mheap->insertHeapRecordAndHash(session, txnId2, rollBackId2, vTableIndex2, rec2, delbit2, version));
	indexEntry = m_mheap->m_hashIndexOperPolicy->get(rec2->m_rowId, ctx);
	CPPUNIT_ASSERT(indexEntry != NULL);
	CPPUNIT_ASSERT(indexEntry->m_type == HIT_MHEAPREC);
	void *ptr2 = (void *)indexEntry->m_value;
	CPPUNIT_ASSERT(ptr2 != NULL);
	
	MHeapRec *heapRec = m_mheap->getHeapRecord(session, ptr1, rec1->m_rowId);
	CPPUNIT_ASSERT(heapRec->m_txnId == txnId1);
	CPPUNIT_ASSERT(heapRec->m_rollBackId == rollBackId1);
	CPPUNIT_ASSERT(heapRec->m_vTableIndex == vTableIndex1);
	CPPUNIT_ASSERT(heapRec->m_del == delbit1);
	CPPUNIT_ASSERT(heapRec->m_rec.m_format == rec1->m_format);
	CPPUNIT_ASSERT(heapRec->m_rec.m_rowId == rec1->m_rowId);
	CPPUNIT_ASSERT(heapRec->m_rec.m_size == rec1->m_size);
	CPPUNIT_ASSERT(memcmp(heapRec->m_rec.m_data, rec1->m_data, rec1->m_size) == 0);

	heapRec = m_mheap->getHeapRecord(session, ptr2, rec2->m_rowId);
	CPPUNIT_ASSERT(heapRec->m_txnId == txnId2);
	CPPUNIT_ASSERT(heapRec->m_rollBackId == rollBackId2);
	CPPUNIT_ASSERT(heapRec->m_vTableIndex == vTableIndex2);
	CPPUNIT_ASSERT(heapRec->m_del == delbit2);
	CPPUNIT_ASSERT(heapRec->m_rec.m_format == rec2->m_format);
	CPPUNIT_ASSERT(heapRec->m_rec.m_rowId == rec2->m_rowId);
	CPPUNIT_ASSERT(heapRec->m_rec.m_size == rec2->m_size);
	CPPUNIT_ASSERT(memcmp(heapRec->m_rec.m_data, rec2->m_data, rec2->m_size) == 0);

	//ptr1指向的记录不是rec2->m_rowId
	heapRec = m_mheap->getHeapRecord(session, ptr1, rec2->m_rowId);
	NTSE_ASSERT(heapRec == NULL);

	delete[] rec1->m_data;
	delete[] rec2->m_data;
	delete rec1;
	delete rec2;
	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(conn);
}

class UpdateHeapRecThread: public Thread {
public:
	UpdateHeapRecThread(Database *db, MHeap *heap, MHeapRec *afterImg): Thread("updateHeapRecThread") {
		m_db = db;
		m_mheap = heap;
		m_afterImg = afterImg;
	}
	void run();
private:
	Database	  *m_db;
	MHeap         *m_mheap;
	MHeapRec      *m_afterImg;
};

void UpdateHeapRecThread::run() {
	Connection *conn = m_db->getConnection(false);
	Session *session = m_db->getSessionManager()->allocSession("UpdateHeapRecThread::run", conn);
	MemoryContext *ctx = session->getMemoryContext();
	McSavepoint msp(ctx);

	CPPUNIT_ASSERT(m_mheap->updateHeapRecordAndHash(session, m_afterImg->m_rec.m_rowId, NULL, m_afterImg));
	
	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(conn);
}

class MHeapDefragThread: public Thread {
public:
	MHeapDefragThread(Database * db, MHeap *heap): Thread("MHeapDefragThread") {
		m_db = db;
		m_mheap = heap;
	}
	void run() {
		Connection *conn = m_db->getConnection(false);
		Session *session = m_db->getSessionManager()->allocSession("MHeapDefragThread::run", conn);
		m_mheap->defragFreeList(session);
		m_db->getSessionManager()->freeSession(session);
		m_db->freeConnection(conn);
	}
private:
	Database   *m_db;
	MHeap      *m_mheap;
};

const char* MHeapTestCase::getName() {
	return "TNT heap test";
}

const char* MHeapTestCase::getDescription() {
	return "Test various operations of tnt heap";
}

bool MHeapTestCase::isBig() {
	return false;
}

TableDef* MHeapTestCase::createStudentTableDef() {
	TableDefBuilder tb(8, "Olympic", "student");
	tb.addColumnS(TNT_STU_NAME, CT_VARCHAR, 30, false, false, COLL_LATIN1);
	tb.addColumn(TNT_STU_SNO, CT_INT);
	tb.addColumn(TNT_STU_AGE, CT_SMALLINT);
	tb.addColumnS(TNT_STU_SEX, CT_CHAR, 1, false, true, COLL_LATIN1);
	tb.addColumn(TNT_STU_CLASS, CT_MEDIUMINT);
	tb.addColumn(TNT_STU_GPA, CT_FLOAT);
	PrType prtype;
	prtype.setUnsigned();
	tb.addColumnN(TNT_STU_GRADE, CT_BIGINT, prtype);
	return tb.getTableDef();
}

void MHeapTestCase::setUp() {
	File dir("testdb");
	dir.rmdir(true);
	dir.mkdir();
	m_config.setBasedir("testdb");
	m_config.m_logLevel = EL_WARN;
	m_config.m_pageBufSize = 200;
	m_config.m_mmsSize = 20;
	m_config.m_logFileSize = 1024 * 1024 * 128;
	EXCPT_OPER(m_db = Database::open(&m_config, true));


	m_conn = m_db->getConnection(false);
	m_session = m_db->getSessionManager()->allocSession("MHeapTestCase", m_conn);

	m_tableDef = createStudentTableDef();

	uint targetSize = 500;
	uint additionalSize =600;
	m_pool = new PagePool(4, Limits::PAGE_SIZE);
	m_pageManager = new TNTIMPageManager(targetSize, additionalSize, m_pool);
	m_pool->registerUser(m_pageManager);
	m_pool->init();

	m_hashIndex = new HashIndex(m_pageManager);
	m_mheap = new MHeap(&m_tableDef, m_pageManager, m_hashIndex);
}

void MHeapTestCase::tearDown() {
	delete m_mheap;
	m_mheap = NULL;

	delete m_hashIndex;
	m_hashIndex = NULL;

	delete m_pool;
	m_pool = NULL;
	delete m_pageManager;
	m_pageManager = NULL;

	m_db->getSessionManager()->freeSession(m_session);
	m_db->freeConnection(m_conn);
	m_db->close(false, false);

	delete m_db;
	m_db = NULL;

	delete m_tableDef;
	m_tableDef = NULL;

	File dir("testdb");
	dir.rmdir(true);
}

void MHeapTestCase::testAddAndRemoveFromFreeList() {
	u8 i = 0;
	u16 max = 1;
	u8  pageTotal = 10;
	u16 tableId = m_tableDef->m_id;
	MRecordPage *page = NULL;
	MemoryContext *ctx = m_session->getMemoryContext();
	McSavepoint msp(ctx);
	MRecordPage **pages = (MRecordPage**) ctx->alloc(pageTotal*sizeof(MRecordPage*));
	for (i = 0; i < pageTotal; i++) {
		pages[i] = (MRecordPage *)m_pageManager->getPage(m_session->getId(), PAGE_MEM_HEAP, true);
		pages[i]->init(MPT_MHEAP, tableId);
		m_mheap->addToFreeList(pages[i]);
		m_pageManager->unlatchPage(m_session->getId(), pages[i], Exclusived);
	}

	FreeNode *freeNode = m_mheap->m_freeList[m_mheap->m_gradeNum - 1];
	freeNode->m_lock.lock(Exclusived, __FILE__, __LINE__);
	for (i = 0, page = freeNode->m_node; i < pageTotal; i++, page = page->m_next) {
		CPPUNIT_ASSERT(page == pages[pageTotal - 1 - i]);
	}
	CPPUNIT_ASSERT(page == NULL);

	m_mheap->removeFromFreeListSafe(freeNode, pages[0]);
	for (i = 0, page = freeNode->m_node; i < pageTotal - 1; i++, page = page->m_next) {
		CPPUNIT_ASSERT(page == pages[pageTotal - 1 - i]);
	}
	CPPUNIT_ASSERT(page == NULL);

	m_mheap->removeFromFreeListSafe(freeNode, pages[pageTotal - 1]);
	for (i = 1, page = freeNode->m_node; i < pageTotal - 1; i++, page = page->m_next) {
		CPPUNIT_ASSERT(page == pages[pageTotal - 1 - i]);
	}
	CPPUNIT_ASSERT(page == NULL);

	u8 middPageNo = (pageTotal - 1)/2;
	assert(middPageNo > 0 && middPageNo < pageTotal - 1);
	m_mheap->removeFromFreeListSafe(freeNode, pages[middPageNo]);
	for (i = 1, page = freeNode->m_node; i < pageTotal - 1; i++) {
		if (pageTotal - 1 - i == middPageNo) {
			continue;
		}
		CPPUNIT_ASSERT(page == pages[pageTotal - 1 - i]);
		page = page->m_next;
	}
	CPPUNIT_ASSERT(page == NULL);
	freeNode->m_lock.unlock(Exclusived);
}

void MHeapTestCase::testAllocAndDefrag() {
	bool isNew = false;
	u16 max = 10;
	u16 fullSize = MRecordPage::TNTIM_PAGE_SIZE - sizeof(MRecordPage);
	u16 rec1Size = fullSize*2/5;
	MRecordPage *page1 = m_mheap->allocRecordPage(m_session, rec1Size, max, false, &isNew);
	page1->m_freeSize -= rec1Size;
	page1->m_recordCnt++;
	CPPUNIT_ASSERT(isNew);
	m_mheap->addToFreeList(page1);
	UNLATCH_TNTIM_PAGE(m_session, m_pageManager, page1, Exclusived);
	CPPUNIT_ASSERT(m_mheap->m_freeList[2]->m_node == page1);
	CPPUNIT_ASSERT(m_mheap->m_freeList[2]->m_node->m_next == NULL);

	isNew = false;
	u16 rec3Size = 15;
	u16 rec2Size = fullSize - rec1Size - 2*rec3Size;
	MRecordPage *page2 = m_mheap->allocRecordPage(m_session, rec2Size, max, false, &isNew);
	CPPUNIT_ASSERT(page1 == page2);
	page2->m_freeSize -= rec2Size;
	page2->m_recordCnt++;
	UNLATCH_TNTIM_PAGE(m_session, m_pageManager, page2, Exclusived);
	CPPUNIT_ASSERT(!isNew);
	CPPUNIT_ASSERT(m_mheap->m_freeList[2]->m_node == page2);
	CPPUNIT_ASSERT(m_mheap->m_freeList[2]->m_node->m_next == NULL);

	MHeapDefragThread defragThread(m_db, m_mheap);
	defragThread.enableSyncPoint(SP_MHEAP_DEFRAG_WAIT);
	defragThread.start();
	defragThread.joinSyncPoint(SP_MHEAP_DEFRAG_WAIT);

	isNew = false;
	MRecordPage *page3 = m_mheap->allocRecordPage(m_session, rec3Size, max, false, &isNew);
	CPPUNIT_ASSERT(page3 != NULL);
	CPPUNIT_ASSERT(page1 != page3);
	page3->m_freeSize -= rec3Size;
	page3->m_recordCnt++;
	CPPUNIT_ASSERT(isNew);
	m_mheap->addToFreeList(page3);
	UNLATCH_TNTIM_PAGE(m_session, m_pageManager, page3, Exclusived);
	CPPUNIT_ASSERT(m_mheap->m_freeList[3]->m_node == page3);
	CPPUNIT_ASSERT(m_mheap->m_freeList[3]->m_node->m_next == NULL);

	defragThread.disableSyncPoint(SP_MHEAP_DEFRAG_WAIT);
	defragThread.notifySyncPoint(SP_MHEAP_DEFRAG_WAIT);
	defragThread.join(-1);

	MRecordPage *page4 = m_mheap->allocRecordPage(m_session, rec3Size, max, false, &isNew);
	CPPUNIT_ASSERT(page4 != NULL);
	CPPUNIT_ASSERT(page1 == page4);
	CPPUNIT_ASSERT(!isNew);
	page4->m_freeSize -= rec3Size;
	page4->m_recordCnt++;
	UNLATCH_TNTIM_PAGE(m_session, m_pageManager, page4, Exclusived);
}

void MHeapTestCase::testUsePageAndDefrag() {
	u16 max = 10;
	bool isNew = false;
	u16 fullSize = MRecordPage::TNTIM_PAGE_SIZE - sizeof(MRecordPage);
	u16 rec1Size = 10;
	MRecordPage *page1 = m_mheap->allocRecordPage(m_session, rec1Size, max, false, &isNew);
	page1->m_freeSize -= rec1Size;
	page1->m_recordCnt++;
	CPPUNIT_ASSERT(isNew);
	m_mheap->addToFreeList(page1);
	UNLATCH_TNTIM_PAGE(m_session, m_pageManager, page1, Exclusived);
	CPPUNIT_ASSERT(m_mheap->m_freeList[3]->m_node == page1);
	CPPUNIT_ASSERT(m_mheap->m_freeList[3]->m_node->m_next == NULL);

	LATCH_TNTIM_PAGE(m_session, m_pageManager, page1, Exclusived);
	page1->m_freeSize -= fullSize*2/5 - rec1Size;
	UNLATCH_TNTIM_PAGE(m_session, m_pageManager, page1, Exclusived);
	MHeapDefragThread defragThread(m_db, m_mheap);
	defragThread.enableSyncPoint(SP_MHEAP_DEFRAG_WAIT);
	defragThread.start();
	defragThread.joinSyncPoint(SP_MHEAP_DEFRAG_WAIT);

	LATCH_TNTIM_PAGE(m_session, m_pageManager, page1, Exclusived);
	page1->m_freeSize += fullSize*2/5 - rec1Size;
	UNLATCH_TNTIM_PAGE(m_session, m_pageManager, page1, Exclusived);

	defragThread.disableSyncPoint(SP_MHEAP_DEFRAG_WAIT);
	defragThread.notifySyncPoint(SP_MHEAP_DEFRAG_WAIT);
	defragThread.join(-1);

	CPPUNIT_ASSERT(m_mheap->m_freeList[3]->m_node == page1);
	CPPUNIT_ASSERT(m_mheap->m_freeList[3]->m_node->m_next == NULL);
}

void MHeapTestCase::testAllocRecordPage() {
	u16 max = 1;
	bool isNew = false;
	u16 fullSize = MRecordPage::TNTIM_PAGE_SIZE - sizeof(MRecordPage);
	u16 size = fullSize - 5;
	MRecordPage *page1 = m_mheap->allocRecordPage(m_session, size, max, false, &isNew);
	page1->m_freeSize -= size;
	page1->m_recordCnt++;
	CPPUNIT_ASSERT(isNew);
	m_mheap->addToFreeList(page1);
	UNLATCH_TNTIM_PAGE(m_session, m_pageManager, page1, Exclusived);
	CPPUNIT_ASSERT(m_mheap->m_freeList[0]->m_node == page1);
	CPPUNIT_ASSERT(m_mheap->m_freeList[0]->m_node->m_next == NULL);

	size = fullSize - 10;
	MRecordPage *page2 = m_mheap->allocRecordPage(m_session, size, max, false, &isNew);
	page2->m_freeSize -= size;
	page2->m_recordCnt++;
	CPPUNIT_ASSERT(isNew);
	m_mheap->addToFreeList(page2);
	UNLATCH_TNTIM_PAGE(m_session, m_pageManager, page2, Exclusived);
	CPPUNIT_ASSERT(m_mheap->m_freeList[0]->m_node == page2);
	CPPUNIT_ASSERT(m_mheap->m_freeList[0]->m_node->m_next == page1);
	CPPUNIT_ASSERT(m_mheap->m_freeList[0]->m_node->m_next->m_next == NULL);

	size = 15;
	MRecordPage *page3 = m_mheap->allocRecordPage(m_session, size, max, false, &isNew);
	page3->m_freeSize -= size;
	page3->m_recordCnt++;
	CPPUNIT_ASSERT(isNew);
	m_mheap->addToFreeList(page3);
	UNLATCH_TNTIM_PAGE(m_session, m_pageManager, page3, Exclusived);
	CPPUNIT_ASSERT(m_mheap->m_freeList[3]->m_node == page3);
	CPPUNIT_ASSERT(m_mheap->m_freeList[3]->m_node->m_next == NULL);

	size = fullSize - 20;
	MRecordPage *page4 = m_mheap->allocRecordPage(m_session, size, max, false, &isNew);
	page4->m_freeSize -= size;
	page4->m_recordCnt++;
	UNLATCH_TNTIM_PAGE(m_session, m_pageManager, page4, Exclusived);
	CPPUNIT_ASSERT(!isNew);
	CPPUNIT_ASSERT(page3 == page4);
	CPPUNIT_ASSERT(m_mheap->m_freeList[3]->m_node == page4);

	size = 20;
	MRecordPage *page5 = m_mheap->allocRecordPage(m_session, size, max, false, &isNew);
	page5->m_freeSize -= size;
	page5->m_recordCnt++;
	CPPUNIT_ASSERT(isNew);
	m_mheap->addToFreeList(page5);
	UNLATCH_TNTIM_PAGE(m_session, m_pageManager, page5, Exclusived);

	LATCH_TNTIM_PAGE(m_session, m_pageManager, page5, Exclusived);
	MRecordPage *page6 = m_mheap->allocRecordPage(m_session, size, max, false, &isNew);
	page6->m_freeSize -= size;
	page6->m_recordCnt++;
	CPPUNIT_ASSERT(isNew);
	m_mheap->addToFreeList(page6);
	CPPUNIT_ASSERT(page5 != page6);

	UNLATCH_TNTIM_PAGE(m_session, m_pageManager, page6, Exclusived);
	UNLATCH_TNTIM_PAGE(m_session, m_pageManager, page5, Exclusived);

	MRecordPage *page7 = m_mheap->allocRecordPage(m_session, size, max, false, &isNew);
	CPPUNIT_ASSERT(!isNew);
	CPPUNIT_ASSERT(page6 == page7);
	UNLATCH_TNTIM_PAGE(m_session, m_pageManager, page7, Exclusived);
}

void MHeapTestCase::testFreeRecordPage() {
	u16 max = 1;
	bool isNew = false;
	u16 fullSize = MRecordPage::TNTIM_PAGE_SIZE - sizeof(MRecordPage);
	u16 size = fullSize - 5;
	MRecordPage *page1 = m_mheap->allocRecordPage(m_session, size, max, false, &isNew);
	page1->m_freeSize -= size;
	page1->m_recordCnt++;
	CPPUNIT_ASSERT(isNew);
	m_mheap->addToFreeList(page1);
	UNLATCH_TNTIM_PAGE(m_session, m_pageManager, page1, Exclusived);
	CPPUNIT_ASSERT(m_mheap->m_freeList[0]->m_node == page1);
	CPPUNIT_ASSERT(m_mheap->m_freeList[0]->m_node->m_next == NULL);

	size = fullSize - 10;
	MRecordPage *page2 = m_mheap->allocRecordPage(m_session, size, max, false, &isNew);
	page2->m_freeSize -= size;
	page2->m_recordCnt++;
	CPPUNIT_ASSERT(isNew);
	m_mheap->addToFreeList(page2);
	UNLATCH_TNTIM_PAGE(m_session, m_pageManager, page2, Exclusived);
	CPPUNIT_ASSERT(m_mheap->m_freeList[0]->m_node == page2);
	CPPUNIT_ASSERT(m_mheap->m_freeList[0]->m_node->m_next == page1);
	CPPUNIT_ASSERT(m_mheap->m_freeList[0]->m_node->m_next->m_next == NULL);

	size = fullSize - 15;
	MRecordPage *page3 = m_mheap->allocRecordPage(m_session, size, max, false, &isNew);
	page3->m_freeSize -= size;
	page3->m_recordCnt++;
	CPPUNIT_ASSERT(isNew);
	m_mheap->addToFreeList(page3);
	UNLATCH_TNTIM_PAGE(m_session, m_pageManager, page3, Exclusived);
	CPPUNIT_ASSERT(m_mheap->m_freeList[0]->m_node == page3);
	CPPUNIT_ASSERT(m_mheap->m_freeList[0]->m_node->m_next == page2);
	CPPUNIT_ASSERT(m_mheap->m_freeList[0]->m_node->m_next->m_next == page1);
	CPPUNIT_ASSERT(m_mheap->m_freeList[0]->m_node->m_next->m_next->m_next == NULL);

	//此时page2的m_recordCnt不为零，所以不能为回收
	m_mheap->freeRecordPage(m_session, m_mheap->m_freeList[0], page2);
	CPPUNIT_ASSERT(m_mheap->m_freeList[0]->m_node == page3);
	CPPUNIT_ASSERT(m_mheap->m_freeList[0]->m_node->m_next == page2);
	CPPUNIT_ASSERT(m_mheap->m_freeList[0]->m_node->m_next->m_next == page1);
	CPPUNIT_ASSERT(m_mheap->m_freeList[0]->m_node->m_next->m_next->m_next == NULL);

	page2->m_recordCnt = 0;
	page2->m_freeSize = fullSize;
	m_mheap->freeRecordPage(m_session, m_mheap->m_freeList[0], page2);
	CPPUNIT_ASSERT(m_mheap->m_freeList[0]->m_node == page3);
	CPPUNIT_ASSERT(m_mheap->m_freeList[0]->m_node->m_next == page1);
	CPPUNIT_ASSERT(m_mheap->m_freeList[0]->m_node->m_next->m_next == NULL);
}

void MHeapTestCase::testFreeSomePage() {
	u16 version = 10;
	HashIndexEntry *indexEntry = NULL;
	HashIndexType indexType = HIT_MHEAPREC;
	u16 size = 0;
	u16 fullSize = MRecordPage::TNTIM_PAGE_SIZE - sizeof(MRecordPage);
	MemoryContext *ctx = m_session->getMemoryContext();
	u64 sp = ctx->setSavepoint();
	byte *buf = (byte *)ctx->alloc(fullSize);

	Record *rec1 = createStudentRec(m_tableDef, 2001/*rid*/, "netease1"/*name*/, 3001/*stuNo*/, 30/*age*/, "F"/*sex*/, 4001/*clsNo*/, 5001/*grade*/);
	MHeapRec heapRec1(90001/*txnId*/, 1001/*rollBackId*/, 0/*vTableIndex*/, rec1, 0/*del*/);

	CPPUNIT_ASSERT(m_mheap->insertHeapRecordAndHash(m_session, &heapRec1, version));
	indexEntry = m_hashIndex->get(heapRec1.m_rec.m_rowId, ctx);
	CPPUNIT_ASSERT(indexEntry != NULL);
	CPPUNIT_ASSERT(indexEntry->m_type == HIT_MHEAPREC);
	void *ptr1 = (void *)indexEntry->m_value;
	CPPUNIT_ASSERT(ptr1 != NULL);
	MRecordPage *page1 = m_mheap->m_freeList[3]->m_node;
	CPPUNIT_ASSERT(page1 == (MRecordPage *)GET_PAGE_HEADER(ptr1));
	size = page1->m_freeSize - sizeof(s16) - 10;
	*((u16 *)buf) = size;
	s16* free1 = page1->appendRecord(buf, size);

	Record *rec2 = createStudentRec(m_tableDef, 2002/*rid*/, "netease2"/*name*/, 3002/*stuNo*/, 29/*age*/, "M"/*sex*/, 4002/*clsNo*/, 5002/*grade*/);
	MHeapRec heapRec2(90002/*txnId*/, 1002/*rollBackId*/, 1/*vTableIndex*/, rec2, 0/*del*/);
	CPPUNIT_ASSERT(m_mheap->insertHeapRecordAndHash(m_session, &heapRec2, version));
	indexEntry = m_hashIndex->get(heapRec2.m_rec.m_rowId, ctx);
	CPPUNIT_ASSERT(indexEntry != NULL);
	CPPUNIT_ASSERT(indexEntry->m_type == HIT_MHEAPREC);
	void *ptr2 = (void *)indexEntry->m_value;
	CPPUNIT_ASSERT(ptr2 != NULL);
	MRecordPage *page2 = m_mheap->m_freeList[3]->m_node;
	CPPUNIT_ASSERT(page2 == (MRecordPage *)GET_PAGE_HEADER(ptr2));
	CPPUNIT_ASSERT(page1 == page2->m_next);
	size = page2->m_freeSize - sizeof(s16) - 15;
	*((u16 *)buf) = size;
	s16* free2 = page2->appendRecord(buf, size);

	Record *rec3 = createStudentRec(m_tableDef, 2003/*rid*/, "neteasexindingfeng"/*name*/, 3003/*stuNo*/, 30/*age*/, "M"/*sex*/, 4003/*clsNo*/, 5003/*grade*/);
	MHeapRec heapRec3(90003/*txnId*/, 1003/*rollBackId*/, 2/*vTableIndex*/, rec3, 0/*del*/);
	CPPUNIT_ASSERT(m_mheap->insertHeapRecordAndHash(m_session, &heapRec3, version));
	indexEntry = m_hashIndex->get(heapRec3.m_rec.m_rowId, ctx);
	CPPUNIT_ASSERT(indexEntry != NULL);
	CPPUNIT_ASSERT(indexEntry->m_type == HIT_MHEAPREC);
	void *ptr3 = (void *)indexEntry->m_value;
	CPPUNIT_ASSERT(ptr3 != NULL);
	MRecordPage *page3 = m_mheap->m_freeList[3]->m_node;
	CPPUNIT_ASSERT(page3 == (MRecordPage *)GET_PAGE_HEADER(ptr3));
	CPPUNIT_ASSERT(page2 == page3->m_next);
	CPPUNIT_ASSERT(page1 == page2->m_next);
	size = page3->m_freeSize - sizeof(s16) - 5;
	*((u16 *)buf) = size;
	s16 *free3 = page3->appendRecord(buf, size);

	page1->removeRecord(free1);
	page2->removeRecord(free2);
	page3->removeRecord(free3);

	int realFreePageSize = m_mheap->freeSomePage(m_session, 6);
	CPPUNIT_ASSERT(realFreePageSize == 2);
	CPPUNIT_ASSERT(m_mheap->m_freeList[3]->m_node != NULL);
	CPPUNIT_ASSERT(m_mheap->m_freeList[3]->m_node->m_next == NULL);

	HashIndexEntry *index1 = m_hashIndex->get(rec1->m_rowId, ctx);
	CPPUNIT_ASSERT(index1 != NULL);
	CPPUNIT_ASSERT(index1->m_type == HIT_MHEAPREC);
	HashIndexEntry *index2 = m_hashIndex->get(rec2->m_rowId, ctx);
	CPPUNIT_ASSERT(index2 != NULL);
	CPPUNIT_ASSERT(index2->m_type == HIT_MHEAPREC);
	HashIndexEntry *index3 = m_hashIndex->get(rec3->m_rowId, ctx);
	CPPUNIT_ASSERT(index3 != NULL);
	CPPUNIT_ASSERT(index3->m_type == HIT_MHEAPREC);

	MHeapRec *pHeapRec = m_mheap->getHeapRecord(m_session, (void *)index1->m_value, index1->m_rowId);
	CPPUNIT_ASSERT(m_mheap->m_freeList[3]->m_node == (MRecordPage *)GET_PAGE_HEADER((void *)index1->m_value));
	CPPUNIT_ASSERT(pHeapRec != NULL);
	CPPUNIT_ASSERT(pHeapRec->m_txnId == heapRec1.m_txnId);
	CPPUNIT_ASSERT(pHeapRec->m_rollBackId == heapRec1.m_rollBackId);
	CPPUNIT_ASSERT(pHeapRec->m_vTableIndex == heapRec1.m_vTableIndex);
	CPPUNIT_ASSERT(pHeapRec->m_del == heapRec1.m_del);
	CPPUNIT_ASSERT(pHeapRec->m_rec.m_format == rec1->m_format);
	CPPUNIT_ASSERT(pHeapRec->m_rec.m_rowId == rec1->m_rowId);
	CPPUNIT_ASSERT(pHeapRec->m_rec.m_size == rec1->m_size);
	CPPUNIT_ASSERT(memcmp(pHeapRec->m_rec.m_data, rec1->m_data, rec1->m_size) == 0);

	pHeapRec = m_mheap->getHeapRecord(m_session, (void *)index2->m_value, index2->m_rowId);
	CPPUNIT_ASSERT(m_mheap->m_freeList[3]->m_node == (MRecordPage *)GET_PAGE_HEADER((void *)index2->m_value));
	CPPUNIT_ASSERT(pHeapRec != NULL);
	CPPUNIT_ASSERT(pHeapRec->m_txnId == heapRec2.m_txnId);
	CPPUNIT_ASSERT(pHeapRec->m_rollBackId == heapRec2.m_rollBackId);
	CPPUNIT_ASSERT(pHeapRec->m_vTableIndex == heapRec2.m_vTableIndex);
	CPPUNIT_ASSERT(pHeapRec->m_del == heapRec2.m_del);
	CPPUNIT_ASSERT(pHeapRec->m_rec.m_format == rec2->m_format);
	CPPUNIT_ASSERT(pHeapRec->m_rec.m_rowId == rec2->m_rowId);
	CPPUNIT_ASSERT(pHeapRec->m_rec.m_size == rec2->m_size);
	CPPUNIT_ASSERT(memcmp(pHeapRec->m_rec.m_data, rec2->m_data, rec2->m_size) == 0);

	pHeapRec = m_mheap->getHeapRecord(m_session, (void *)index3->m_value, index3->m_rowId);
	CPPUNIT_ASSERT(m_mheap->m_freeList[3]->m_node == (MRecordPage *)GET_PAGE_HEADER((void *)index3->m_value));
	CPPUNIT_ASSERT(pHeapRec != NULL);
	CPPUNIT_ASSERT(pHeapRec->m_txnId == heapRec3.m_txnId);
	CPPUNIT_ASSERT(pHeapRec->m_rollBackId == heapRec3.m_rollBackId);
	CPPUNIT_ASSERT(pHeapRec->m_vTableIndex == heapRec3.m_vTableIndex);
	CPPUNIT_ASSERT(pHeapRec->m_del == heapRec3.m_del);
	CPPUNIT_ASSERT(pHeapRec->m_rec.m_format == rec3->m_format);
	CPPUNIT_ASSERT(pHeapRec->m_rec.m_rowId == rec3->m_rowId);
	CPPUNIT_ASSERT(pHeapRec->m_rec.m_size == rec3->m_size);
	CPPUNIT_ASSERT(memcmp(pHeapRec->m_rec.m_data, rec3->m_data, rec3->m_size) == 0);

	CPPUNIT_ASSERT(m_mheap->removeHeapRecordAndHash(m_session, index1->m_rowId));
	CPPUNIT_ASSERT(m_mheap->removeHeapRecordAndHash(m_session, index2->m_rowId));
	CPPUNIT_ASSERT(m_mheap->removeHeapRecordAndHash(m_session, index3->m_rowId));
	realFreePageSize = m_mheap->freeSomePage(m_session, 6);
	CPPUNIT_ASSERT(realFreePageSize == 1);

	m_session->getMemoryContext()->resetToSavepoint(sp);

	delete[] rec1->m_data;
	delete[] rec2->m_data;
	delete[] rec3->m_data;
	delete rec1;
	delete rec2;
	delete rec3;
}

void MHeapTestCase::testFreeSomePageLarge() {
	HashIndexEntry *indexEntry = NULL;
	u16 version = 20;
	MemoryContext *ctx = m_session->getMemoryContext();
	McSavepoint msp(ctx);
	int freePageCnt = 0;
	u32 i = 0;
	bool ret = true;
	u32 totalRec = 2000;

	u64 txnId = 1001;
	RowId rollBackId = 10001;
	u8 vTableIndex = 1;
	RowId rid = 18000;
	char name[50];
	u32 stuNo = 5000;
	u16 age = 30;
	char *sex = NULL;
	u32 clsNo = 4001;
	u32 grade = 5001;
	u8 delbit = 0;
	Record **recs = (Record **)ctx->alloc(totalRec*sizeof(Record*));
	void *ptr = NULL;
	for (i = 0; i < totalRec; i++) {
		if (i % 2 == 0) {
			sex = "F";
		} else {
			sex = "M";
		}
		System::snprintf_mine(name, 50, "netease%d", i);
		recs[i] = createStudentRec(m_tableDef, rid + i, name, stuNo + i, (age + i)%20, sex, (clsNo + i)%1000, (grade + i)%8000);
		CPPUNIT_ASSERT(m_mheap->insertHeapRecordAndHash(m_session, txnId, rollBackId, vTableIndex, recs[i], delbit, version));
		indexEntry = m_mheap->m_hashIndexOperPolicy->get(recs[i]->m_rowId, ctx);
		CPPUNIT_ASSERT(indexEntry != NULL);
		CPPUNIT_ASSERT(indexEntry->m_type == HIT_MHEAPREC);
		ptr = (void *)indexEntry->m_value;
		CPPUNIT_ASSERT(ptr != NULL);
	}
	
	freePageCnt = m_mheap->freeSomePage(m_session, 1000);
	CPPUNIT_ASSERT(freePageCnt == 0);

	for (i = 0; i < totalRec; i++) {
		//10项中保留1项未被删除
		if (i % 10 == 0) {
			continue;
		}
		ret = m_mheap->removeHeapRecordAndHash(m_session, recs[i]->m_rowId);
		CPPUNIT_ASSERT(ret == true);
	}
	freePageCnt = m_mheap->freeSomePage(m_session, 1000);
	CPPUNIT_ASSERT(freePageCnt > 0);

	//测试读
	for (i = 0; i < totalRec; i++) {
		indexEntry = m_mheap->m_hashIndexOperPolicy->get(recs[i]->m_rowId, ctx);
		if (i % 10 != 0) {
			CPPUNIT_ASSERT(indexEntry == NULL);
			continue;
		}
		CPPUNIT_ASSERT(indexEntry != NULL);
		CPPUNIT_ASSERT(indexEntry->m_type == HIT_MHEAPREC);
		ptr = (void *)indexEntry->m_value;
		MHeapRec *heapRec = m_mheap->getHeapRecord(m_session, ptr, recs[i]->m_rowId);
		CPPUNIT_ASSERT(heapRec->m_txnId == txnId);
		CPPUNIT_ASSERT(heapRec->m_rollBackId == rollBackId);
		CPPUNIT_ASSERT(heapRec->m_vTableIndex == vTableIndex);
		CPPUNIT_ASSERT(heapRec->m_del == delbit);
		CPPUNIT_ASSERT(heapRec->m_rec.m_format == recs[i]->m_format);
		CPPUNIT_ASSERT(heapRec->m_rec.m_rowId == recs[i]->m_rowId);
		CPPUNIT_ASSERT(heapRec->m_rec.m_size == recs[i]->m_size);
		CPPUNIT_ASSERT(memcmp(heapRec->m_rec.m_data, recs[i]->m_data, recs[i]->m_size) == 0);
	}


	for (i = 0; i < totalRec; i++) {
		delete[] recs[i]->m_data;
		delete recs[i];
	}
}

void MHeapTestCase::testFreeAllPage() {
	testAllocRecordPage();
	m_mheap->freeAllPage(m_session);
	CPPUNIT_ASSERT(m_mheap->m_freeList[0]->m_node == NULL);
	CPPUNIT_ASSERT(m_mheap->m_freeList[0]->m_lastFind == NULL);
	CPPUNIT_ASSERT(m_mheap->m_freeList[1]->m_node == NULL);
	CPPUNIT_ASSERT(m_mheap->m_freeList[1]->m_lastFind == NULL);
	CPPUNIT_ASSERT(m_mheap->m_freeList[2]->m_node == NULL);
	CPPUNIT_ASSERT(m_mheap->m_freeList[3]->m_lastFind == NULL);
	CPPUNIT_ASSERT(m_mheap->m_freeList[3]->m_node == NULL);
	CPPUNIT_ASSERT(m_mheap->m_freeList[3]->m_lastFind == NULL);
}

void MHeapTestCase::testDefrag() {
	u16 max = 1;
	bool isNew = false;
	u16 fullSize = MRecordPage::TNTIM_PAGE_SIZE - sizeof(MRecordPage);
	u16 size = fullSize - 5;
	MRecordPage *page1 = m_mheap->allocRecordPage(m_session, size, max, false, &isNew);
	page1->m_freeSize -= size;
	page1->m_recordCnt++;
	CPPUNIT_ASSERT(isNew);
	m_mheap->addToFreeList(page1);
	UNLATCH_TNTIM_PAGE(m_session, m_pageManager, page1, Exclusived);
	
	CPPUNIT_ASSERT(m_mheap->m_freeList[0]->m_node == page1);
	CPPUNIT_ASSERT(m_mheap->m_freeList[0]->m_node->m_next == NULL);

	size = fullSize - 10;
	MRecordPage *page2 = m_mheap->allocRecordPage(m_session, size, max, false, &isNew);
	page2->m_freeSize -= size;
	page2->m_recordCnt++;
	CPPUNIT_ASSERT(isNew);
	m_mheap->addToFreeList(page2);
	UNLATCH_TNTIM_PAGE(m_session, m_pageManager, page2, Exclusived);
	CPPUNIT_ASSERT(m_mheap->m_freeList[0]->m_node == page2);
	CPPUNIT_ASSERT(m_mheap->m_freeList[0]->m_node->m_next == page1);
	CPPUNIT_ASSERT(m_mheap->m_freeList[0]->m_node->m_next->m_next == NULL);

	size = fullSize - 15;
	MRecordPage *page3 = m_mheap->allocRecordPage(m_session, size, max, false, &isNew);
	page3->m_freeSize -= size;
	page3->m_recordCnt++;
	CPPUNIT_ASSERT(isNew);
	m_mheap->addToFreeList(page3);
	UNLATCH_TNTIM_PAGE(m_session, m_pageManager, page3, Exclusived);
	CPPUNIT_ASSERT(m_mheap->m_freeList[0]->m_node == page3);
	CPPUNIT_ASSERT(m_mheap->m_freeList[0]->m_node->m_next == page2);
	CPPUNIT_ASSERT(m_mheap->m_freeList[0]->m_node->m_next->m_next == page1);
	CPPUNIT_ASSERT(m_mheap->m_freeList[0]->m_node->m_next->m_next->m_next == NULL);

	page2->m_freeSize = fullSize - 8;
	m_mheap->defragFreeList(m_session);
	CPPUNIT_ASSERT(m_mheap->m_freeList[0]->m_node == page3);
	CPPUNIT_ASSERT(m_mheap->m_freeList[0]->m_node->m_next == page1);
	CPPUNIT_ASSERT(m_mheap->m_freeList[0]->m_node->m_next->m_next == NULL);
	CPPUNIT_ASSERT(m_mheap->m_freeList[3]->m_node == page2);

	page1->m_freeSize = (u16)((FLAG_TNT_00_LIMIT + FLAG_TNT_01_LIMIT)*fullSize/2);
	m_mheap->defragFreeList(m_session);
	CPPUNIT_ASSERT(m_mheap->m_freeList[0]->m_node == page3);
	CPPUNIT_ASSERT(m_mheap->m_freeList[0]->m_node->m_next == NULL);
	CPPUNIT_ASSERT(m_mheap->m_freeList[1]->m_node == page1);
	CPPUNIT_ASSERT(m_mheap->m_freeList[3]->m_node == page2);

	page3->m_freeSize = (u16)((FLAG_TNT_01_LIMIT + FLAG_TNT_10_LIMIT)*fullSize/2);
	page2->m_freeSize = (u16)((FLAG_TNT_01_LIMIT + FLAG_TNT_10_LIMIT)*fullSize/2);
	m_mheap->defragFreeList(m_session);
	CPPUNIT_ASSERT(m_mheap->m_freeList[0]->m_node == NULL);
	CPPUNIT_ASSERT(m_mheap->m_freeList[1]->m_node == page1);
	CPPUNIT_ASSERT(m_mheap->m_freeList[2]->m_node == page2);
	CPPUNIT_ASSERT(m_mheap->m_freeList[2]->m_node->m_next == page3);
	CPPUNIT_ASSERT(m_mheap->m_freeList[2]->m_node->m_next->m_next == NULL);
	CPPUNIT_ASSERT(m_mheap->m_freeList[3]->m_node == NULL);
}

void MHeapTestCase::testInsertHeapRecord() {
	HashIndexEntry *indexEntry = NULL;
	u16 version = 20;
	MemoryContext *ctx = m_session->getMemoryContext();
	u64 sp = ctx->setSavepoint();

	u64 txnId1 = 90001;
	RowId rollBackId1 = 1001;
	u8 vTableIndex1 = 1;
	Record *rec1 = createStudentRec(m_tableDef, 2001/*rid*/, "netease1"/*name*/, 3001/*stuNo*/, 30/*age*/, "F"/*sex*/, 4001/*clsNo*/, 5001/*grade*/);
	u8 delbit1 = 0;
	CPPUNIT_ASSERT(m_mheap->insertHeapRecordAndHash(m_session, txnId1, rollBackId1, vTableIndex1, rec1, delbit1, version));
	indexEntry = m_hashIndex->get(rec1->m_rowId, ctx);
	CPPUNIT_ASSERT(indexEntry != NULL);
	CPPUNIT_ASSERT(indexEntry->m_type == HIT_MHEAPREC);
	void *ptr1 = (void *)indexEntry->m_value;
	CPPUNIT_ASSERT(ptr1 != NULL);

	u64 txnId2 = 90002;
	RowId rollBackId2 = 1002;
	u8 vTableIndex2 = 2;
	Record *rec2 = createStudentRec(m_tableDef, 2002/*rid*/, "netease2"/*name*/, 3002/*stuNo*/, 29/*age*/, "M"/*sex*/, 4002/*clsNo*/, 5002/*grade*/);
	u8 delbit2 = 0;
	CPPUNIT_ASSERT(m_mheap->insertHeapRecordAndHash(m_session, txnId2, rollBackId2, vTableIndex2, rec2, delbit2, version));
	indexEntry = m_hashIndex->get(rec2->m_rowId, ctx);
	CPPUNIT_ASSERT(indexEntry != NULL);
	CPPUNIT_ASSERT(indexEntry->m_type == HIT_MHEAPREC);
	void *ptr2 = (void *)indexEntry->m_value;
	CPPUNIT_ASSERT(ptr2 != NULL);
	
	MHeapRec *heapRec = m_mheap->getHeapRecord(m_session, ptr1, rec1->m_rowId);
	CPPUNIT_ASSERT(heapRec->m_txnId == txnId1);
	CPPUNIT_ASSERT(heapRec->m_rollBackId == rollBackId1);
	CPPUNIT_ASSERT(heapRec->m_vTableIndex == vTableIndex1);
	CPPUNIT_ASSERT(heapRec->m_del == delbit1);
	CPPUNIT_ASSERT(heapRec->m_rec.m_format == rec1->m_format);
	CPPUNIT_ASSERT(heapRec->m_rec.m_rowId == rec1->m_rowId);
	CPPUNIT_ASSERT(heapRec->m_rec.m_size == rec1->m_size);
	CPPUNIT_ASSERT(memcmp(heapRec->m_rec.m_data, rec1->m_data, rec1->m_size) == 0);

	heapRec = m_mheap->getHeapRecord(m_session, ptr2, rec2->m_rowId);
	CPPUNIT_ASSERT(heapRec->m_txnId == txnId2);
	CPPUNIT_ASSERT(heapRec->m_rollBackId == rollBackId2);
	CPPUNIT_ASSERT(heapRec->m_vTableIndex == vTableIndex2);
	CPPUNIT_ASSERT(heapRec->m_del == delbit2);
	CPPUNIT_ASSERT(heapRec->m_rec.m_format == rec2->m_format);
	CPPUNIT_ASSERT(heapRec->m_rec.m_rowId == rec2->m_rowId);
	CPPUNIT_ASSERT(heapRec->m_rec.m_size == rec2->m_size);
	CPPUNIT_ASSERT(memcmp(heapRec->m_rec.m_data, rec2->m_data, rec2->m_size) == 0);

	//ptr1指向的记录不是rec2->m_rowId
	heapRec = m_mheap->getHeapRecord(m_session, ptr1, rec2->m_rowId);
	NTSE_ASSERT(heapRec == NULL);

	ctx->resetToSavepoint(sp);
	delete[] rec1->m_data;
	delete[] rec2->m_data;
	delete rec1;
	delete rec2;
}

void MHeapTestCase::testUpdateHeapRecord() {
	u16 version = 20;
	HashIndexEntry *indexEntry = NULL;
	MemoryContext *ctx = m_session->getMemoryContext();
	u64 sp = ctx->setSavepoint();

	u64 txnId1 = 90001;
	RowId rollBackId1 = 1001;
	u8 vTableIndex1 = 1;
	Record *rec1 = createStudentRec(m_tableDef, 2001/*rid*/, "netease1"/*name*/, 3001/*stuNo*/, 30/*age*/, "F"/*sex*/, 4001/*clsNo*/, 5001/*grade*/);
	u8 delbit1 = 0;
	CPPUNIT_ASSERT(m_mheap->insertHeapRecordAndHash(m_session, txnId1, rollBackId1, vTableIndex1, rec1, delbit1, version));
	indexEntry = m_hashIndex->get(rec1->m_rowId, ctx);
	CPPUNIT_ASSERT(indexEntry != NULL);
	CPPUNIT_ASSERT(indexEntry->m_type == HIT_MHEAPREC);
	void *ptr1 = (void *)indexEntry->m_value;
	CPPUNIT_ASSERT(ptr1 != NULL);

	u64 txnId2 = 90002;
	RowId rollBackId2 = 1002;
	u8 vTableIndex2 = 2;
	Record *rec2 = createStudentRec(m_tableDef, 2001/*rid*/, "netease163"/*name*/, 3002/*stuNo*/, 29/*age*/, "M"/*sex*/, 4002/*clsNo*/, 5002/*grade*/);
	u8 delbit2 = 0;
	MHeapRec heapRec2(txnId2, rollBackId2, vTableIndex2, rec2, delbit2);

	u64 txnId3 = 90003;
	RowId rollBackId3 = 1003;
	u8 vTableIndex3 = 1;
	Record *rec3 = createStudentRec(m_tableDef, 3001/*rid*/, "netease3"/*name*/, 3003/*stuNo*/, 32/*age*/, "F"/*sex*/, 4003/*clsNo*/, 5003/*grade*/);
	u8 delbit3 = 0;
	CPPUNIT_ASSERT(m_mheap->insertHeapRecordAndHash(m_session, txnId3, rollBackId3, vTableIndex3, rec3, delbit3, version));
	indexEntry = m_hashIndex->get(rec3->m_rowId, ctx);
	CPPUNIT_ASSERT(indexEntry != NULL);
	CPPUNIT_ASSERT(indexEntry->m_type == HIT_MHEAPREC);
	void *ptr3 = (void *)indexEntry->m_value;
	CPPUNIT_ASSERT(ptr3 != NULL);
	
	MHeapRec *heapRec = m_mheap->getHeapRecord(m_session, ptr1, rec1->m_rowId);
	CPPUNIT_ASSERT(heapRec->m_txnId == txnId1);
	CPPUNIT_ASSERT(heapRec->m_rollBackId == rollBackId1);
	CPPUNIT_ASSERT(heapRec->m_vTableIndex == vTableIndex1);
	CPPUNIT_ASSERT(heapRec->m_del == delbit1);
	CPPUNIT_ASSERT(heapRec->m_rec.m_format == rec1->m_format);
	CPPUNIT_ASSERT(heapRec->m_rec.m_rowId == rec1->m_rowId);
	CPPUNIT_ASSERT(heapRec->m_rec.m_size == rec1->m_size);
	CPPUNIT_ASSERT(memcmp(heapRec->m_rec.m_data, rec1->m_data, rec1->m_size) == 0);

	heapRec = m_mheap->getHeapRecord(m_session, ptr3, rec3->m_rowId);
	CPPUNIT_ASSERT(heapRec->m_txnId == txnId3);
	CPPUNIT_ASSERT(heapRec->m_rollBackId == rollBackId3);
	CPPUNIT_ASSERT(heapRec->m_vTableIndex == vTableIndex3);
	CPPUNIT_ASSERT(heapRec->m_del == delbit3);
	CPPUNIT_ASSERT(heapRec->m_rec.m_format == rec3->m_format);
	CPPUNIT_ASSERT(heapRec->m_rec.m_rowId == rec3->m_rowId);
	CPPUNIT_ASSERT(heapRec->m_rec.m_size == rec3->m_size);
	CPPUNIT_ASSERT(memcmp(heapRec->m_rec.m_data, rec3->m_data, rec3->m_size) == 0);

	u64 txnId4 = 90002;
	RowId rollBackId4 = 1002;
	u8 vTableIndex4 = 2;
	Record *noExistRec = createStudentRec(m_tableDef, 99999/*rid*/, "netease163"/*name*/, 3002/*stuNo*/, 29/*age*/, "M"/*sex*/, 4002/*clsNo*/, 5002/*grade*/);
	u8 delbit4 = 0;
	MHeapRec heapRec4(txnId4, rollBackId4, vTableIndex4, noExistRec, delbit4);
	//CPPUNIT_ASSERT(!m_mheap->updateHeapRecordAndHash(m_session, noExistRec->m_rowId, &heapRec4, version));

	CPPUNIT_ASSERT(m_mheap->updateHeapRecordAndHash(m_session, rec1->m_rowId, NULL, &heapRec2, version));
	indexEntry = m_hashIndex->get(heapRec2.m_rec.m_rowId, ctx);
	CPPUNIT_ASSERT(indexEntry != NULL);
	CPPUNIT_ASSERT(indexEntry->m_type == HIT_MHEAPREC);
	void *ptr2 = (void *)indexEntry->m_value;
	CPPUNIT_ASSERT(ptr2 != ptr1);
	heapRec = m_mheap->getHeapRecord(m_session, ptr1, rec1->m_rowId);
	CPPUNIT_ASSERT(heapRec == NULL);

	heapRec = m_mheap->getHeapRecord(m_session, ptr2, rec2->m_rowId);
	CPPUNIT_ASSERT(heapRec->m_txnId == txnId2);
	CPPUNIT_ASSERT(heapRec->m_rollBackId == rollBackId2);
	CPPUNIT_ASSERT(heapRec->m_vTableIndex == vTableIndex2);
	CPPUNIT_ASSERT(heapRec->m_del == delbit2);
	CPPUNIT_ASSERT(heapRec->m_rec.m_format == rec2->m_format);
	CPPUNIT_ASSERT(heapRec->m_rec.m_rowId == rec2->m_rowId);
	CPPUNIT_ASSERT(heapRec->m_rec.m_size == rec2->m_size);
	CPPUNIT_ASSERT(memcmp(heapRec->m_rec.m_data, rec2->m_data, rec2->m_size) == 0);

	ctx->resetToSavepoint(sp);

	delete[] rec1->m_data;
	delete[] rec2->m_data;
	delete[] rec3->m_data;
	delete[] noExistRec->m_data;
	delete rec1;
	delete rec2;
	delete rec3;
	delete noExistRec;
}

void MHeapTestCase::testRemoveHeapRecord() {
	u16 version = 30;
	HashIndexEntry *indexEntry = NULL;
	MemoryContext *ctx = m_session->getMemoryContext();
	u64 sp = ctx->setSavepoint();

	u64 txnId1 = 90001;
	RowId rollBackId1 = 1001;
	u8 vTableIndex1 = 1;
	Record *rec1 = createStudentRec(m_tableDef, 2001/*rid*/, "netease1"/*name*/, 3001/*stuNo*/, 30/*age*/, "F"/*sex*/, 4001/*clsNo*/, 5001/*grade*/);
	u8 delbit1 = 0;
	CPPUNIT_ASSERT(m_mheap->insertHeapRecordAndHash(m_session, txnId1, rollBackId1, vTableIndex1, rec1, delbit1, version));
	indexEntry = m_hashIndex->get(rec1->m_rowId, ctx);
	CPPUNIT_ASSERT(indexEntry != NULL);
	void *ptr1 = (void *)indexEntry->m_value;
	CPPUNIT_ASSERT(ptr1 != NULL);
	
	MHeapRec *heapRec = m_mheap->getHeapRecord(m_session, ptr1, rec1->m_rowId);
	CPPUNIT_ASSERT(heapRec->m_txnId == txnId1);
	CPPUNIT_ASSERT(heapRec->m_rollBackId == rollBackId1);
	CPPUNIT_ASSERT(heapRec->m_vTableIndex == vTableIndex1);
	CPPUNIT_ASSERT(heapRec->m_del == delbit1);
	CPPUNIT_ASSERT(heapRec->m_rec.m_format == rec1->m_format);
	CPPUNIT_ASSERT(heapRec->m_rec.m_rowId == rec1->m_rowId);
	CPPUNIT_ASSERT(heapRec->m_rec.m_size == rec1->m_size);
	CPPUNIT_ASSERT(memcmp(heapRec->m_rec.m_data, rec1->m_data, rec1->m_size) == 0);

	RowId noExistRid = 99999;
	CPPUNIT_ASSERT(!m_mheap->removeHeapRecordAndHash(m_session, noExistRid));

	CPPUNIT_ASSERT(m_mheap->removeHeapRecordAndHash(m_session, heapRec->m_rec.m_rowId));
	ptr1 = m_mheap->getHeapRecord(m_session, ptr1, rec1->m_rowId);
	CPPUNIT_ASSERT(ptr1 == NULL);

	ctx->resetToSavepoint(sp);
	delete[] rec1->m_data;
	delete rec1;
}

void MHeapTestCase::testGetHeapRedRecord() {
	u16 version = 30;
	byte *buf = NULL;
	HashIndexEntry *indexEntry = NULL;
	MemoryContext *ctx = m_session->getMemoryContext();
	u64 sp = ctx->setSavepoint();

	u64 txnId1 = 90001;
	RowId rollBackId1 = 1001;
	u8 vTableIndex1 = 1;
	Record *rec1 = createStudentRec(m_tableDef, 2001/*rid*/, "netease1"/*name*/, 3001/*stuNo*/, 30/*age*/, "F"/*sex*/, 4001/*clsNo*/, 5001/*grade*/);
	u8 delbit1 = 0;
	CPPUNIT_ASSERT(m_mheap->insertHeapRecordAndHash(m_session, txnId1, rollBackId1, vTableIndex1, rec1, delbit1, version));
	buf = (byte *)ctx->alloc(m_tableDef->m_maxRecSize);
	memset(buf, 0, m_tableDef->m_maxRecSize);
	Record *redRec1 = new (ctx->alloc(sizeof(Record))) Record(rec1->m_rowId, REC_REDUNDANT, buf, m_tableDef->m_maxRecSize);
	RecordOper::convertRecordVR(m_tableDef, rec1, redRec1);

	indexEntry = m_hashIndex->get(rec1->m_rowId, ctx);
	CPPUNIT_ASSERT(indexEntry != NULL);
	void *ptr1 = (void *)indexEntry->m_value;
	CPPUNIT_ASSERT(ptr1 != NULL);
	
	//ptr对应记录的rowid不为3001
	RowId errorRid = 3001;
	buf = (byte *)ctx->alloc(m_tableDef->m_maxRecSize);
	memset(buf, 0, m_tableDef->m_maxRecSize);
	Record *redRec2 = new (ctx->alloc(sizeof(Record))) Record(INVALID_ROW_ID, REC_REDUNDANT, buf, m_tableDef->m_maxRecSize);
	MHeapRec *heapRec = m_mheap->getHeapRedRecord(m_session, ptr1, errorRid, redRec2);
	CPPUNIT_ASSERT(heapRec == NULL);

	heapRec = m_mheap->getHeapRedRecord(m_session, ptr1, rec1->m_rowId, redRec2);
	CPPUNIT_ASSERT(heapRec->m_txnId == txnId1);
	CPPUNIT_ASSERT(heapRec->m_rollBackId == rollBackId1);
	CPPUNIT_ASSERT(heapRec->m_vTableIndex == vTableIndex1);
	CPPUNIT_ASSERT(heapRec->m_del == delbit1);
	CPPUNIT_ASSERT(heapRec->m_rec.m_format == redRec1->m_format);
	CPPUNIT_ASSERT(heapRec->m_rec.m_rowId == redRec1->m_rowId);
	CPPUNIT_ASSERT(heapRec->m_rec.m_size == redRec1->m_size);
	CPPUNIT_ASSERT(memcmp(heapRec->m_rec.m_data, redRec1->m_data, redRec1->m_size) == 0);

	CPPUNIT_ASSERT(heapRec->m_rec.m_data == redRec2->m_data);

	ctx->resetToSavepoint(sp);
	delete[] rec1->m_data;
	delete rec1;
}

void MHeapTestCase::testGetHeapRedRecordSafe() {
	u16 version = 30;
	HashIndexEntry *indexEntry = NULL;
	byte *buf = NULL;
	MemoryContext *ctx = m_session->getMemoryContext();
	u64 sp = ctx->setSavepoint();

	u64 txnId1 = 90001;
	RowId rollBackId1 = 1001;
	u8 vTableIndex1 = 1;
	Record *rec1 = createStudentRec(m_tableDef, 2001/*rid*/, "netease1"/*name*/, 3001/*stuNo*/, 30/*age*/, "F"/*sex*/, 4001/*clsNo*/, 5001/*grade*/);
	u8 delbit1 = 0;
	CPPUNIT_ASSERT(m_mheap->insertHeapRecordAndHash(m_session, txnId1, rollBackId1, vTableIndex1, rec1, delbit1, version));
	buf = (byte *)ctx->alloc(m_tableDef->m_maxRecSize);
	memset(buf, 0, m_tableDef->m_maxRecSize);
	Record *redRec1 = new (ctx->alloc(sizeof(Record))) Record(rec1->m_rowId, REC_REDUNDANT, buf, m_tableDef->m_maxRecSize);
	RecordOper::convertRecordVR(m_tableDef, rec1, redRec1);

	indexEntry = m_hashIndex->get(rec1->m_rowId, ctx);
	CPPUNIT_ASSERT(indexEntry != NULL);
	void *ptr1 = (void *)indexEntry->m_value;
	CPPUNIT_ASSERT(ptr1 != NULL);
	
	//ptr对应记录的rowid不为3001
	RowId errorRid = 3001;
	CPPUNIT_ASSERT(!m_mheap->checkAndLatchPage(m_session, ptr1, errorRid));

	CPPUNIT_ASSERT(m_mheap->checkAndLatchPage(m_session, ptr1, rec1->m_rowId));
	buf = (byte *)ctx->alloc(m_tableDef->m_maxRecSize);
	memset(buf, 0, m_tableDef->m_maxRecSize);
	Record *redRec2 = new (ctx->alloc(sizeof(Record))) Record(INVALID_ROW_ID, REC_REDUNDANT, buf, m_tableDef->m_maxRecSize);

	MHeapRec *heapRec = m_mheap->getHeapRedRecordSafe(m_session, ptr1, rec1->m_rowId, redRec2);
	CPPUNIT_ASSERT(heapRec->m_txnId == txnId1);
	CPPUNIT_ASSERT(heapRec->m_rollBackId == rollBackId1);
	CPPUNIT_ASSERT(heapRec->m_vTableIndex == vTableIndex1);
	CPPUNIT_ASSERT(heapRec->m_del == delbit1);
	CPPUNIT_ASSERT(heapRec->m_rec.m_format == redRec1->m_format);
	CPPUNIT_ASSERT(heapRec->m_rec.m_rowId == redRec1->m_rowId);
	CPPUNIT_ASSERT(heapRec->m_rec.m_size == redRec1->m_size);
	CPPUNIT_ASSERT(memcmp(heapRec->m_rec.m_data, redRec1->m_data, redRec1->m_size) == 0);

	CPPUNIT_ASSERT(heapRec->m_rec.m_data == redRec2->m_data);

	ctx->resetToSavepoint(sp);
	delete[] rec1->m_data;
	delete rec1;
}

void MHeapTestCase::testMulThreadInsert() {
	HashIndexEntry *indexEntry = NULL;
	u16 version = 20;
	MemoryContext *ctx = m_session->getMemoryContext();
	McSavepoint msp(ctx);
	//此数字要小心，因为InsertHeapRecThread的syncPoint处是不释放链表的锁的，
	//所以如果不停添加记录，导致需要重新分配新页面来容纳新记录时需要链表的X锁，此时就死锁了
	//但具体的实际情况是不存在的，因为不可能只有链表的S锁，然后一直被挂起
	u32 totalRec = 0;
	if (m_pool->getPageSize() == 8192) {
		totalRec = 144;
	} else if (m_pool->getPageSize() == 4096) {
		totalRec = 71;
	} else {
		totalRec = 10;
	}

	InsertHeapRecThread insertHeapRecThread(m_db, m_mheap, m_tableDef);
	insertHeapRecThread.enableSyncPoint(SP_MHEAP_ALLOC_PAGE_RACE);
	insertHeapRecThread.start();
	insertHeapRecThread.joinSyncPoint(SP_MHEAP_ALLOC_PAGE_RACE);

	u64 txnId = 1001;
	RowId rollBackId = 10001;
	u8 vTableIndex = 1;
	RowId rid = 18000;
	char name[50];
	u32 stuNo = 5000;
	u16 age = 30;
	char *sex = NULL;
	u32 clsNo = 4001;
	u32 grade = 5001;
	u8 delbit = 0;
	Record **recs = (Record **)ctx->alloc(totalRec*sizeof(Record*));
	void **ptrs = (void **)ctx->alloc(totalRec*sizeof(void *));
	for (u32 i = 0; i < totalRec; i++) {
		if (i % 2 == 0) {
			sex = "F";
		} else {
			sex = "M";
		}
		System::snprintf_mine(name, 50, "netease%d", i);
		recs[i] = createStudentRec(m_tableDef, rid + i, name, stuNo + i, (age + i)%20, sex, (clsNo + i)%1000, (grade + i)%8000);
		CPPUNIT_ASSERT(m_mheap->insertHeapRecordAndHash(m_session, txnId, rollBackId, vTableIndex, recs[i], delbit, version));
		indexEntry = m_mheap->m_hashIndexOperPolicy->get(recs[i]->m_rowId, ctx);
		CPPUNIT_ASSERT(indexEntry != NULL);
		CPPUNIT_ASSERT(indexEntry->m_type == HIT_MHEAPREC);
		ptrs[i] = (void *)indexEntry->m_value;
		CPPUNIT_ASSERT(ptrs[i] != NULL);
	}
	
	insertHeapRecThread.disableSyncPoint(SP_MHEAP_ALLOC_PAGE_RACE);
	insertHeapRecThread.notifySyncPoint(SP_MHEAP_ALLOC_PAGE_RACE);
	insertHeapRecThread.join(-1);

	//测试读
	for (u32 i = 0; i < totalRec; i++) {
		MHeapRec *heapRec = m_mheap->getHeapRecord(m_session, ptrs[i], recs[i]->m_rowId);
		CPPUNIT_ASSERT(heapRec->m_txnId == txnId);
		CPPUNIT_ASSERT(heapRec->m_rollBackId == rollBackId);
		CPPUNIT_ASSERT(heapRec->m_vTableIndex == vTableIndex);
		CPPUNIT_ASSERT(heapRec->m_del == delbit);
		CPPUNIT_ASSERT(heapRec->m_rec.m_format == recs[i]->m_format);
		CPPUNIT_ASSERT(heapRec->m_rec.m_rowId == recs[i]->m_rowId);
		CPPUNIT_ASSERT(heapRec->m_rec.m_size == recs[i]->m_size);
		CPPUNIT_ASSERT(memcmp(heapRec->m_rec.m_data, recs[i]->m_data, recs[i]->m_size) == 0);
	}


	for (u32 i = 0; i < totalRec; i++) {
		delete[] recs[i]->m_data;
		delete recs[i];
	}
}

void MHeapTestCase::testMulThreadUpdate() {
	u16 version = 20;
	HashIndexEntry *indexEntry = NULL;
	MemoryContext *ctx = m_session->getMemoryContext();
	u64 sp = ctx->setSavepoint();

	u64 txnId1 = 90001;
	RowId rollBackId1 = 1001;
	u8 vTableIndex1 = 1;
	Record *rec1 = createStudentRec(m_tableDef, 2001/*rid*/, "netease1"/*name*/, 3001/*stuNo*/, 30/*age*/, "F"/*sex*/, 4001/*clsNo*/, 5001/*grade*/);
	u8 delbit1 = 0;
	CPPUNIT_ASSERT(m_mheap->insertHeapRecordAndHash(m_session, txnId1, rollBackId1, vTableIndex1, rec1, delbit1, version));
	indexEntry = m_hashIndex->get(rec1->m_rowId, ctx);
	CPPUNIT_ASSERT(indexEntry != NULL);
	CPPUNIT_ASSERT(indexEntry->m_type == HIT_MHEAPREC);
	void *ptr1 = (void *)indexEntry->m_value;
	CPPUNIT_ASSERT(ptr1 != NULL);

	u64 txnId2 = 90002;
	RowId rollBackId2 = 1002;
	u8 vTableIndex2 = 2;
	Record *rec2 = createStudentRec(m_tableDef, 2001/*rid*/, "netease163"/*name*/, 3002/*stuNo*/, 29/*age*/, "M"/*sex*/, 4002/*clsNo*/, 5002/*grade*/);
	u8 delbit2 = 0;
	MHeapRec heapRec2(txnId2, rollBackId2, vTableIndex2, rec2, delbit2);

	u64 txnId3 = 90003;
	RowId rollBackId3 = 1003;
	u8 vTableIndex3 = 1;
	Record *rec3 = createStudentRec(m_tableDef, 2001/*rid*/, "netease163163"/*name*/, 3003/*stuNo*/, 32/*age*/, "F"/*sex*/, 4003/*clsNo*/, 5003/*grade*/);
	u8 delbit3 = 0;
	MHeapRec heapRec3(txnId3, rollBackId3, vTableIndex3, rec3, delbit3);

	u64 txnId4 = 90004;
	RowId rollBackId4 = 1004;
	u8 vTableIndex4 = 1;
	Record *rec4 = createStudentRec(m_tableDef, 3001/*rid*/, "netease3"/*name*/, 3003/*stuNo*/, 32/*age*/, "F"/*sex*/, 4003/*clsNo*/, 5003/*grade*/);
	u8 delbit4 = 0;
	CPPUNIT_ASSERT(m_mheap->insertHeapRecordAndHash(m_session, txnId4, rollBackId4, vTableIndex4, rec4, delbit4, version));
	indexEntry = m_hashIndex->get(rec4->m_rowId, ctx);
	CPPUNIT_ASSERT(indexEntry != NULL);
	CPPUNIT_ASSERT(indexEntry->m_type == HIT_MHEAPREC);
	void *ptr4 = (void *)indexEntry->m_value;
	CPPUNIT_ASSERT(ptr4 != NULL);

	MHeapRec *heapRec = m_mheap->getHeapRecord(m_session, ptr1, rec1->m_rowId);
	CPPUNIT_ASSERT(heapRec->m_txnId == txnId1);
	CPPUNIT_ASSERT(heapRec->m_rollBackId == rollBackId1);
	CPPUNIT_ASSERT(heapRec->m_vTableIndex == vTableIndex1);
	CPPUNIT_ASSERT(heapRec->m_del == delbit1);
	CPPUNIT_ASSERT(heapRec->m_rec.m_format == rec1->m_format);
	CPPUNIT_ASSERT(heapRec->m_rec.m_rowId == rec1->m_rowId);
	CPPUNIT_ASSERT(heapRec->m_rec.m_size == rec1->m_size);
	CPPUNIT_ASSERT(memcmp(heapRec->m_rec.m_data, rec1->m_data, rec1->m_size) == 0);

	heapRec = m_mheap->getHeapRecord(m_session, ptr4, rec4->m_rowId);
	CPPUNIT_ASSERT(heapRec->m_txnId == txnId4);
	CPPUNIT_ASSERT(heapRec->m_rollBackId == rollBackId4);
	CPPUNIT_ASSERT(heapRec->m_vTableIndex == vTableIndex4);
	CPPUNIT_ASSERT(heapRec->m_del == delbit4);
	CPPUNIT_ASSERT(heapRec->m_rec.m_format == rec4->m_format);
	CPPUNIT_ASSERT(heapRec->m_rec.m_rowId == rec4->m_rowId);
	CPPUNIT_ASSERT(heapRec->m_rec.m_size == rec4->m_size);
	CPPUNIT_ASSERT(memcmp(heapRec->m_rec.m_data, rec4->m_data, rec4->m_size) == 0);

	UpdateHeapRecThread updateHeapRecThread(m_db, m_mheap, &heapRec3);
	updateHeapRecThread.enableSyncPoint(SP_MHEAP_UPDATEHEAPRECORDANDHASH_REC_MODIFY);
	updateHeapRecThread.start();
	updateHeapRecThread.joinSyncPoint(SP_MHEAP_UPDATEHEAPRECORDANDHASH_REC_MODIFY);

	CPPUNIT_ASSERT(m_mheap->updateHeapRecordAndHash(m_session, heapRec2.m_rec.m_rowId, NULL, &heapRec2));
	indexEntry = m_hashIndex->get(rec1->m_rowId, ctx);
	CPPUNIT_ASSERT(indexEntry != NULL);
	CPPUNIT_ASSERT(indexEntry->m_type == HIT_MHEAPREC);
	void *ptr2 = (void *)indexEntry->m_value;
	CPPUNIT_ASSERT(ptr2 != NULL);
	heapRec = m_mheap->getHeapRecord(m_session, ptr2, rec2->m_rowId);
	CPPUNIT_ASSERT(heapRec->m_txnId == txnId2);
	CPPUNIT_ASSERT(heapRec->m_rollBackId == rollBackId2);
	CPPUNIT_ASSERT(heapRec->m_vTableIndex == vTableIndex2);
	CPPUNIT_ASSERT(heapRec->m_del == delbit2);
	CPPUNIT_ASSERT(heapRec->m_rec.m_format == rec2->m_format);
	CPPUNIT_ASSERT(heapRec->m_rec.m_rowId == rec2->m_rowId);
	CPPUNIT_ASSERT(heapRec->m_rec.m_size == rec2->m_size);
	CPPUNIT_ASSERT(memcmp(heapRec->m_rec.m_data, rec2->m_data, rec2->m_size) == 0);

	updateHeapRecThread.disableSyncPoint(SP_MHEAP_UPDATEHEAPRECORDANDHASH_REC_MODIFY);
	updateHeapRecThread.notifySyncPoint(SP_MHEAP_UPDATEHEAPRECORDANDHASH_REC_MODIFY);
	updateHeapRecThread.join(-1);
	
	indexEntry = m_hashIndex->get(rec1->m_rowId, ctx);
	CPPUNIT_ASSERT(indexEntry != NULL);
	CPPUNIT_ASSERT(indexEntry->m_type == HIT_MHEAPREC);
	void *ptr3 = (void *)indexEntry->m_value;
	CPPUNIT_ASSERT(ptr3 != NULL);
	heapRec = m_mheap->getHeapRecord(m_session, ptr3, rec1->m_rowId);
	CPPUNIT_ASSERT(heapRec->m_txnId == txnId3);
	CPPUNIT_ASSERT(heapRec->m_rollBackId == rollBackId3);
	CPPUNIT_ASSERT(heapRec->m_vTableIndex == vTableIndex3);
	CPPUNIT_ASSERT(heapRec->m_del == delbit3);
	CPPUNIT_ASSERT(heapRec->m_rec.m_format == rec3->m_format);
	CPPUNIT_ASSERT(heapRec->m_rec.m_rowId == rec3->m_rowId);
	CPPUNIT_ASSERT(heapRec->m_rec.m_size == rec3->m_size);
	CPPUNIT_ASSERT(memcmp(heapRec->m_rec.m_data, rec3->m_data, rec3->m_size) == 0);

	ctx->resetToSavepoint(sp);

	delete[] rec1->m_data;
	delete[] rec2->m_data;
	delete[] rec3->m_data;
	delete[] rec4->m_data;
	delete rec1;
	delete rec2;
	delete rec3;
	delete rec4;
}

void MHeapTestCase::testGetHeapRecAndRemove() {
	u16 version = 30;
	HashIndexEntry *indexEntry = NULL;
	byte *buf = NULL;
	MemoryContext *ctx = m_session->getMemoryContext();
	u64 sp = ctx->setSavepoint();

	u64 txnId = 90001;
	RowId rollBackId = 1001;
	u8 vTableIndex = 1;
	Record *rec = createStudentRec(m_tableDef, 2001/*rid*/, "netease1"/*name*/, 3001/*stuNo*/, 30/*age*/, "F"/*sex*/, 4001/*clsNo*/, 5001/*grade*/);
	u8 delbit = 0;
	CPPUNIT_ASSERT(m_mheap->insertHeapRecordAndHash(m_session, txnId, rollBackId, vTableIndex, rec, delbit, version));
	indexEntry = m_hashIndex->get(rec->m_rowId, ctx);
	CPPUNIT_ASSERT(indexEntry != NULL);
	void *ptr = (void *)indexEntry->m_value;
	CPPUNIT_ASSERT(ptr != NULL);
	CPPUNIT_ASSERT(m_mheap->checkAndLatchPage(m_session, ptr, rec->m_rowId, Shared));
	MHeapRec *heapRec = m_mheap->getHeapRecordSafe(m_session, ptr, rec->m_rowId);
	CPPUNIT_ASSERT(heapRec->m_txnId == txnId);
	CPPUNIT_ASSERT(heapRec->m_rollBackId == rollBackId);
	CPPUNIT_ASSERT(heapRec->m_vTableIndex == vTableIndex);
	CPPUNIT_ASSERT(heapRec->m_del == delbit);
	CPPUNIT_ASSERT(heapRec->m_rec.m_format == rec->m_format);
	CPPUNIT_ASSERT(heapRec->m_rec.m_rowId == rec->m_rowId);
	CPPUNIT_ASSERT(heapRec->m_rec.m_size == rec->m_size);
	CPPUNIT_ASSERT(memcmp(heapRec->m_rec.m_data, rec->m_data, rec->m_size) == 0);
	m_mheap->unLatchPageByPtr(m_session, ptr, Shared);

	CPPUNIT_ASSERT(m_mheap->removeHeapRecordAndHash(m_session, indexEntry->m_rowId));
	CPPUNIT_ASSERT(!m_mheap->checkAndLatchPage(m_session, ptr, rec->m_rowId, Shared));
	CPPUNIT_ASSERT(m_mheap->freeSomePage(m_session, 20) == 1);
	CPPUNIT_ASSERT(!m_mheap->checkAndLatchPage(m_session, ptr, rec->m_rowId, Shared));

	heapRec = m_mheap->getHeapRecord(m_session, ptr, rec->m_rowId);
	CPPUNIT_ASSERT(heapRec == NULL);

	MRecordPage *page = (MRecordPage *)GET_PAGE_HEADER(ptr);
	CPPUNIT_ASSERT(m_mheap->m_pageManager->getPageLatchMode(page) == None);
	ctx->resetToSavepoint(sp);
	delete[] rec->m_data;
	delete rec;
}