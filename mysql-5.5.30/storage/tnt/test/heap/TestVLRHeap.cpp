#include "heap/TestVLRHeap.h"
#include "api/Table.h"
#include "heap/Heap.h"
#include "api/Database.h"
#include "util/File.h"
#include "Test.h"
#include "misc/Session.h"
#include "misc/RecordHelper.h"
#include "misc/Trace.h"
#include "misc/Verify.h"
#include <cstdlib>
#include "heap/TestFLRHeap.h"
#include <sstream>
#include <iostream>
#include <string>
#include <iomanip>
using namespace std;



static int randomInt(int max);

/***** �������� *****/

class DaVince : public Worker {
public:
	DaVince(Database *db, Connection *conn, DrsHeap *heap, TableDef *tblDef, const char *name, void *data) : Worker(db, conn, heap, tblDef, name, data) {};
private:
	virtual void run();
};

struct DataDaVince {
	int m_rows;
	Record *m_record;
	RowLockHandle **m_rlh;
public:
	DataDaVince() {
		m_rows = 1;
		m_record = 0;
		m_rlh = NULL;
	}
};

class Connie : public Worker {
public:
	Connie(Database *db, Connection *conn, DrsHeap *heap, TableDef *tblDef, const char *name, void *data) : Worker(db, conn, heap, tblDef, name, data) {};
private:
	virtual void run();
};

class Dracula : public Worker {
public:
	Dracula(Database *db, Connection *conn, DrsHeap *heap, TableDef *tblDef, const char *name, void *data) : Worker(db, conn, heap, tblDef, name, data) {};
private:
	virtual void run();
};

class VanHelsing : public Worker {
public:
	VanHelsing(Database *db, Connection *conn, DrsHeap *heap, TableDef *tblDef, const char *name, void *data) : Worker(db, conn, heap, tblDef, name, data) {};
private:
	virtual void run();
};

struct DataVanHelsing {
	LockMode m_lockMode;
	bool m_returnLinkSrc;
	SubRecord *m_subRec;
};

class Indiana : public Worker {
public:
	Indiana(Database *db, Connection *conn, DrsHeap *heap, TableDef *tblDef, const char *name, void *data) : Worker(db, conn, heap, tblDef, name, data) {};
private:
	virtual void run();
};

struct DataIndiana {
	Record *m_outRec;
	RowId m_rowId;
	bool m_got;
};


/*** end �������� ***/


const char* VLRHeapTestCase::getName() {
	return "Variable length record heap test";
}

/* �ļ������� */
#define VLR_HEAP "vlrtestheap.nsd"
#define VLR_TBLDEF "vlrtestheap.nstd"
#define VLR_BACK_01 "vlrtestheap.bak01"
#define VLR_BACK_02 "vlrtestheap.bak02"
#define VLR_BACK_03 "vlrtestheap.bak03"
#define VLR_BACK_04 "vlrtestheap.bak04"
#define VLR_BACK_05 "vlrtestheap.bak05"
#define VLR_BACK_06 "vlrtestheap.bak06"

const char* VLRHeapTestCase::getDescription() {
	return "Test various operations of variable length record heap and common features of heap";
}

bool VLRHeapTestCase::isBig() {
	return false;
}


/** 
 * setUp����
 *		���̣�	1. �������
 *				2. �������ݿ�Ͳ����ö�
 */
void VLRHeapTestCase::setUp() {
	//ts.hp = true;
	//vs.hp = true;
	m_conn = NULL;
	m_db = NULL;
	m_heap = NULL;
	Database::drop(".");

	int fact = Limits::PAGE_SIZE / 1024;

	// �������ݿ�
	File oldfile(VLR_HEAP); // ɾ���϶��ļ�
	oldfile.remove();
	m_db = Database::open(&m_config, 1);

	// ����TableDef
	TableDefBuilder *builder = new TableDefBuilder(1, "space", "Blog");
	builder->addColumn("ID", CT_BIGINT, false)->addColumn("UserID", CT_BIGINT);
	builder->addColumn("PublishTime", CT_BIGINT)->addColumn("ModifyTime", CT_BIGINT);
	builder->addColumn("IsPublished", CT_TINYINT)->addColumnS("Permalink", CT_VARCHAR, 50 * fact);
	builder->addColumnS("TrackbackUrl", CT_VARCHAR, 50 * fact)->addColumn("CommentCount", CT_INT);
	builder->addColumnS("Title", CT_VARCHAR, 50 * fact)->addColumnS("Abstract", CT_VARCHAR, 400 * fact, false);
	builder->addIndex("PRIMARY", true, true, false, "ID", 0, NULL)->addIndex("IDX_BLOG_PERMA", false, false, false, "Permalink", 0, NULL);
	builder->addIndex("PRIMARY", true, true, false, "ID", 0, NULL);
	TableDef *tableDef = builder->getTableDef();

	delete builder;

	m_tblDef = tableDef;
	tableDef->writeFile(VLR_TBLDEF);
	DrsHeap::create(m_db, VLR_HEAP, tableDef);

	// get connection
	m_conn = m_db->getConnection(true);

	Session *session = m_db->getSessionManager()->allocSession("VLRHeapTestCase::setUp", m_conn);
	EXCPT_OPER(m_heap = DrsHeap::open(m_db, session, VLR_HEAP, tableDef));
	m_db->getSessionManager()->freeSession(session);

	m_vheap = (VariableLengthRecordHeap *)m_heap;
}

/** 
 * tearDown����
 *		���̣�	1. �رղ�drop���Զ�
 *				2. �ر�connection
 *				3. �رղ�drop���ݿ�
 */
void VLRHeapTestCase::tearDown() {
	if (m_heap) {
		Session *session = m_db->getSessionManager()->allocSession("VLRHeapTestCase::tearDown", m_conn);

		EXCPT_OPER(m_heap->close(session, true));
		m_db->getSessionManager()->freeSession(session);
		EXCPT_OPER(DrsHeap::drop(VLR_HEAP));
		delete m_heap;
		m_heap = NULL;
		m_vheap = NULL;
	}

	if (m_tblDef != NULL) {
		TableDef::drop(VLR_TBLDEF);
		delete m_tblDef;
		m_tblDef = NULL;
	}

	if (m_conn) {
		m_db->freeConnection(m_conn);
		m_conn = NULL;
	}
	if (m_db) {
		m_db->close();
		Database::drop(".");
        delete m_db;
		m_db = NULL;
	}
}

/** 
 * ����insert����
 *		���̣�	1. �������ܲ���
 *				2. �����֧���ǲ��ԣ����ݼ��ڲ�����ע�ͣ�
 */
void VLRHeapTestCase::testInsert() {
	Session *session;
	u8 flag;

	Record *rec;
	RowId rid, rid1, rid2, rid3, rid4;
	u64 pageNum;
	s16 slotNum;

	byte *buf = (byte *)System::virtualAlloc(Limits::PAGE_SIZE);
	Record record;
	record.m_data = buf;
	record.m_format = REC_VARLEN;
	bool canDel;

	u16 offset, offset2;

	CPPUNIT_ASSERT(m_heap->getStatus().m_dboStats->m_statArr[DBOBJ_ITEM_INSERT] == 0);

	/* ����һ��tiny record */
	rec = createTinyRawRecord(111);
	session = m_db->getSessionManager()->allocSession("VLRHeapTestCase::testInsert", m_conn);
	rid = m_heap->insert(session, rec, NULL);

#ifdef NTSE_TRACE
	nftrace(true, tout << (DrsHeap *)NULL);
	nftrace(true, tout << m_heap);
	nftrace(true, tout << (HeapHeaderPageInfo *)NULL);
	nftrace(true, tout << (HeapHeaderPageInfo *)m_vheap->getHeaderPage());
#endif

	u64 firstPage;
#ifdef NTSE_VERIFY_EX
	firstPage = RID_GET_PAGE(rid);
	//BufferPageHandle *secondPageHdl = GET_PAGE(session, m_heap->getHeapFile(), PAGE_HEAP, firstPage + 1, Shared, m_heap->getDBObjStats(), NULL);
	//((VLRHeapRecordPageInfo *)secondPageHdl->getPage())->verifyPage();
	//session->releasePage(&secondPageHdl);
	BufferPageHandle *firstPageHdl = GET_PAGE(session, m_heap->getHeapFile(), PAGE_HEAP, firstPage, Shared, m_heap->getDBObjStats(), NULL);
	((VLRHeapRecordPageInfo *)firstPageHdl->getPage())->verifyPage();
	session->releasePage(&firstPageHdl);
#endif

	CPPUNIT_ASSERT(rid == RID(m_vheap->getCBMCount() + 1 + 1, 0));
	CPPUNIT_ASSERT(m_heap->getStatus().m_dboStats->m_statArr[DBOBJ_ITEM_INSERT] == 1);

	m_db->getSessionManager()->freeSession(session);
	freeRecord(rec);
	/* ����short recordֱ��flag�ı� */
	do {
		rec = createShortRawRecord(109);
		session = m_db->getSessionManager()->allocSession("VLRHeapTestCase::testInsert", m_conn);
		rid = m_heap->insert(session, rec, NULL);
		pageNum = RID_GET_PAGE(rid);
		slotNum = RID_GET_SLOT(rid);
		flag = m_vheap->getPageFlag(session, pageNum);
		m_db->getSessionManager()->freeSession(session);
		freeRecord(rec);
	} while (flag == 3);
	//cout<<"slotNum is "<<slotNum<<endl;
	/* �ٲ���һ��short record */
	rec = createShortRawRecord(108);
 	session = m_db->getSessionManager()->allocSession("VLRHeapTestCase::testInsert", m_conn);
	rid2 = m_heap->insert(session, rec, NULL);

#ifdef NTSE_TRACE
	BufferPageHandle *bmphdl = m_vheap->getBitmap(session, 0, Shared);
	BitmapPage *bmp = (BitmapPage *)bmphdl->getPage();
	ftrace(true, tout << bmp);
	nftrace(true, tout << (BitmapPage *)NULL);
	session->releasePage(&bmphdl);
#endif


	m_db->getSessionManager()->freeSession(session);
	freeRecord(rec);

	

	/* ����һ��long record, Ӧ�ûỻҳ */
	/** ��Ϊlong record������������hackһ�� **/
	u16 maxRecSize = m_tblDef->m_maxRecSize;
	m_tblDef->m_maxRecSize = Limits::PAGE_SIZE;

	rec = createLongRawRecord(107);
	session = m_db->getSessionManager()->allocSession("VLRHeapTestCase::testInsert", m_conn);
	rid3 = m_heap->insert(session, rec, NULL);
	pageNum = RID_GET_PAGE(rid);
	slotNum = RID_GET_SLOT(rid);
	flag = m_vheap->getPageFlag(session, pageNum);
	/* ����offset */
	offset = m_vheap->getRecordOffset(session, rid3);
	//cout<<"long record's offset is "<<offset<<endl;
	m_db->getSessionManager()->freeSession(session);
	freeRecord(rec);
	CPPUNIT_ASSERT(RID_GET_PAGE(rid2) != RID_GET_PAGE(rid3));

	/** �ָ�m_maxRecSize **/
	m_tblDef->m_maxRecSize = maxRecSize;


	/* �ٲ���һ��short record��Ӧ���ڸղ�δ�õ�ҳ���� */
	rec = createShortRawRecord(108);
 	session = m_db->getSessionManager()->allocSession("VLRHeapTestCase::testInsert", m_conn);
	rid = m_heap->insert(session, rec, NULL);
	m_db->getSessionManager()->freeSession(session);
	freeRecord(rec);
	CPPUNIT_ASSERT(RID_GET_PAGE(rid) == RID_GET_PAGE(rid2));

	/*** ���²���insert�е���defrag�������ڶ�ҳ֮��long recordɾ���ٲ��� ***/
	/* ����short recordֱ��rid3����ҳ�� */
	int i = 0;
	session = m_db->getSessionManager()->allocSession("VLRHeapTestCase::testInsert", m_conn);
	do {
		++i;
		rec = createShortRawRecord(104);
		rid4 = m_heap->insert(session, rec, NULL);
		//cout<<"page is "<<RID_GET_PAGE(rid)<<", slot is"<<RID_GET_SLOT(rid)<<endl;
		freeRecord(rec);
	} while (RID_GET_PAGE(rid4) >= RID_GET_PAGE(rid3));
	m_db->getSessionManager()->freeSession(session);
	//cout<<"i is "<<i<<endl;
	/* ����ɾ��long record */
	session = m_db->getSessionManager()->allocSession("VLRHeapTestCase::testInsert", m_conn);
	canDel = m_heap->del(session, rid3);
	CPPUNIT_ASSERT(canDel);
	m_db->getSessionManager()->freeSession(session);

	/* ����short record��ֱ����ҳ����slot��rid2֮�� */
	i = 0;
	session = m_db->getSessionManager()->allocSession("VLRHeapTestCase::testInsert", m_conn);
	do {
		++i;
		rec = createShortRawRecord(103);
		//cout<<"rid4 is "<<rid4<<endl<<"rid is "<<rid<<endl<<endl;
		rid = m_heap->insert(session, rec, NULL);
		offset2 = m_vheap->getRecordOffset(session, rid);
		freeRecord(rec);
	} while (offset2 < offset);
	m_db->getSessionManager()->freeSession(session);
	//cout<<"i is "<<i<<endl;

	/*** ���뵽�ѱ仯�Ĳ��� ***/
	/* �����Լ��Ҫ�ļ�¼�� */
	rec = createShortRawRecord(101);
	u16 slotLeng = rec->m_size + sizeof(VLRHeader);
	u16 pageSlot = (Limits::PAGE_SIZE - sizeof(VLRHeapRecordPageInfo)) / slotLeng + 1; // ���������һ��
	//int rec2heapExtending = pageSlot * DrsHeap::HEAP_INCREASE_SIZE;
	int rec2heapExtending = pageSlot * m_tblDef->m_incrSize;
	int rec2anotherBmp = pageSlot * BitmapPage::CAPACITY;
	//cout<<"BitmapPage::CAPACITY is "<<BitmapPage::CAPACITY<<endl;
	//cout<<"rec2heapExtending is "<<rec2heapExtending<<endl<<"rec2anotherBmp is "<<rec2anotherBmp<<endl;
	freeRecord(rec);
	u64 maxPageNum1 = m_heap->getMaxPageNum();
	u64 maxUsedPgN1 = m_heap->getMaxUsedPageNum();
	/* ���뵽��չ�� */
	session = m_db->getSessionManager()->allocSession("VLRHeapTestCase::testInsert", m_conn);
	rec = createShortRawRecord(102);
	for (i = 0; i < rec2heapExtending; ++i) {
		m_heap->insert(session, rec, NULL);
	}
	freeRecord(rec);
	m_db->getSessionManager()->freeSession(session);
	u64 maxPageNum2 = m_heap->getMaxPageNum();//m_heap->getMaxPageNum();
	u64 maxUsedPgN2 = m_heap->getMaxUsedPageNum();
	CPPUNIT_ASSERT(maxUsedPgN2 > maxUsedPgN1);
	//CPPUNIT_ASSERT(maxPageNum2 == maxPageNum1 + DrsHeap::HEAP_INCREASE_SIZE);
	//CPPUNIT_ASSERT(maxPageNum2 == maxPageNum1 + m_tblDef->m_incrSize);


	/* ·��������һ��long record�Ա����� */
	/** ��Ϊlong record������������hackһ�� **/
	maxRecSize = m_tblDef->m_maxRecSize;
	m_tblDef->m_maxRecSize = Limits::PAGE_SIZE;
	rec = createLongRawRecord(107);
	session = m_db->getSessionManager()->allocSession("VLRHeapTestCase::testInsert", m_conn);
	rid3 = m_heap->insert(session, rec, NULL);
	m_db->getSessionManager()->freeSession(session);
	freeRecord(rec);
	CPPUNIT_ASSERT(RID_GET_PAGE(rid2) != RID_GET_PAGE(rid3));
	/** �ָ�m_maxRecSize **/
	m_tblDef->m_maxRecSize = maxRecSize;


//#if NTSE_PAGE_SIZE == 1024  // СҶģʽ�������ܺں�rewind����
#if 0

	/* ���뵽������9��λͼҳ */
	rec = createShortRawRecord(99);
	session = m_db->getSessionManager()->allocSession("VLRHeapTestCase::testInsert", m_conn);
	for (i = 0; i < rec2anotherBmp * 8; ++i) {
		m_heap->insert(session, rec, NULL);
	}
	m_db->getSessionManager()->freeSession(session);
	freeRecord(rec);
	/* �ٲ�һ�� */
	session = m_db->getSessionManager()->allocSession("VLRHeapTestCase::testInsert", m_conn);
	rec = createShortRawRecord(99);
	m_heap->insert(session, rec, NULL);
	freeRecord(rec);
	m_db->getSessionManager()->freeSession(session);

	
	/*** ����rewind ***/
	/* ɾ��long record */
	session = m_db->getSessionManager()->allocSession("VLRHeapTestCase::testInsert", m_conn);
	canDel = m_heap->del(session, rid3);
	CPPUNIT_ASSERT(canDel);


	/* ����ֱ���ﵽrid3����һҳ��˵����rewind���� */
	i = 0;
	rec = createShortRawRecord(98);
	do {
		++i;
		rid = m_heap->insert(session, rec, NULL);
		pageNum = RID_GET_PAGE(rid);
		if (i >= rec2heapExtending) CPPUNIT_ASSERT(false); //û��rewind?
	} while (pageNum != RID_GET_PAGE(rid3));
	freeRecord(rec);
	m_db->getSessionManager()->freeSession(session);
	//cout<<"i is "<<i<<endl;

#else  // ��ҳ����ԣ�ֻ����α�ںв���

#include "cbmp.h"
	//cout << sizeof(CBMP) << endl;
	session = m_db->getSessionManager()->allocSession("VLRHeapTestCase::testInsert", m_conn);
	for (uint i = 0; i < m_vheap->getCBMCount(); ++i) {
		const byte *page = CBMP + i * Limits::PAGE_SIZE;
		//
		m_heap->getBuffer()->lockPage(session->getId(), m_vheap->getCBMPage(i), Exclusived, true);
		memcpy(m_vheap->getCBMPage(i), page, Limits::PAGE_SIZE);
		m_heap->getBuffer()->markDirty(session, m_vheap->getCBMPage(i));
		m_heap->getBuffer()->unlockPage(session->getId(), m_vheap->getCBMPage(i), Exclusived);
	}
	int idx = m_vheap->CBfindFirstFitBitmap(2, 8);
	//cout << "idx is " << idx <<endl;
	m_db->getSessionManager()->freeSession(session);

#ifdef NTSE_TRACE

#endif


	CPPUNIT_ASSERT(0 == idx);

	CPPUNIT_ASSERT(m_heap->getStatus().m_dataLength > 0);

#endif




	/******* ����Ϊ���ǲ��ԣ�����findFreePage�е�
	if (maxPageNum != m_maxPageNum) { // �Ѿ��������߳���չ�˶ѣ������ٴ���չ
		unlockHeaderPage(session, &headerPageHdl);
		bmpIdx = lastBmpIdx;
		goto findFreePage_search_central_bmp;
	}
	һ�δ��� *******/
	tearDown();
	setUp();
  
	rec = createShortRawRecord(119);


	DataDaVince ddata;
	//ddata.m_rows = (Limits::PAGE_SIZE / rec->m_size) * DrsHeap::HEAP_INCREASE_SIZE;
	ddata.m_rows = (Limits::PAGE_SIZE / rec->m_size) * m_tblDef->m_incrSize;
	ddata.m_record = rec;
	DaVince d1(m_db, m_conn, m_heap, m_tblDef, "Insert thread 1", &ddata);
	DaVince d2(m_db, m_conn, m_heap, m_tblDef, "Insert thread 2", &ddata);

	d1.enableSyncPoint(SP_HEAP_VLR_FINDFREEPAGE_BEFORE_EXTEND_HEAP);
	d1.start();
	d1.joinSyncPoint(SP_HEAP_VLR_FINDFREEPAGE_BEFORE_EXTEND_HEAP);
	u64 maxPage = m_heap->getMaxPageNum();
	d2.start();
	d2.join();
	//CPPUNIT_ASSERT(m_heap->getMaxPageNum() >= maxPage + DrsHeap::HEAP_INCREASE_SIZE);
	CPPUNIT_ASSERT(m_heap->getMaxPageNum() >= maxPage + m_tblDef->m_incrSize);
	d1.disableSyncPoint(SP_HEAP_VLR_FINDFREEPAGE_BEFORE_EXTEND_HEAP);
	d1.notifySyncPoint(SP_HEAP_VLR_FINDFREEPAGE_BEFORE_EXTEND_HEAP);
	d1.join();

	freeRecord(rec);
	/***** ���ǲ������ ******/


	/*******
	���´������findFreeInBMP�й���rewind���ҵĴ���
	********/
	tearDown();
	setUp();
	
	m_tblDef->m_incrSize = 64;

	rec = createShortRawRecord(125);
	session = m_db->getSessionManager()->allocSession("VLRHeapTestCase::testInsert", m_conn);
	rid1 = 0;
	/* �Ȳ��룬��ȡmax page number*/
	do {
		rid = m_heap->insert(session, rec, NULL);
		//cout<<"rid page is "<<RID_GET_PAGE(rid)<<endl;
		if (rid1 == 0)
			rid1 = rid;
		if (RID_GET_PAGE(rid) == RID_GET_PAGE(rid1)) {
			slotNum = RID_GET_SLOT(rid) - RID_GET_SLOT(rid1) + 1;
			//cout<<"Slot count is "<<slotNum<<endl;
		}
		else
			break;
	} while (true);
	u16 incsize = m_tblDef->m_incrSize;


	/* �嵽���һҳ */
	u16 slots = slotNum;
	maxPage = m_heap->getMaxPageNum();
	if (RID_GET_PAGE(rid) == maxPage)
		--slots;
	do {
		rid = m_heap->insert(session, rec, NULL);
	} while (RID_GET_PAGE(rid) != maxPage);
	/* �������һҳ */
	while (--slots > 0) {
		rid = m_heap->insert(session, rec, NULL);
	}
	//CPPUNIT_ASSERT(m_heap->getMaxPageNum() == maxPage);
	/* ��ʱӦ���Ѿ�������ɾ��rid1ҳ������м�¼ */
	slots = 0;
	for (; slots < slotNum; ++slots) {
		canDel = m_heap->del(session, RID(RID_GET_PAGE(rid1), slots));
		CPPUNIT_ASSERT(canDel);
	}
	/* ��ʱ���룬Ӧ�û���rewind�������ڵ�һ��ҳ�� */
	rid2 = m_heap->insert(session, rec, NULL);
	//cout<<"page is "<<RID_GET_PAGE(rid2)<<", slot is "<<RID_GET_SLOT(rid2)<<endl;
	CPPUNIT_ASSERT(RID_GET_PAGE(rid2) == RID_GET_PAGE(rid1));

	m_db->getSessionManager()->freeSession(session);
	freeRecord(rec);


	/******* end ����findFreeInBMP�й���rewind���ҵĴ��� ******/

	/****** ��RowLockHandle�Ĳ��� ******/
	RowLockHandle *rowLockHdl = NULL;
	rec = createShortRawRecord(231);

	session = m_db->getSessionManager()->allocSession("VLRHeapTestCase::testInsert", m_conn);	
	rid = m_heap->insert(session, rec, &rowLockHdl);
	CPPUNIT_ASSERT(rowLockHdl);
	session->unlockRow(&rowLockHdl);
	m_db->getSessionManager()->freeSession(session);

	freeRecord(rec);



	/**** �����������insert������
	
	// �����Ҫ����������
	if (rlh && !rowLocked) {
		*rlh = TRY_LOCK_ROW(session, m_tableDef->m_id, rowId, Exclusived);
		if (!*rlh) { // TRY LOCK���ɹ�
			session->unlockPage(&freePageHdl);
			......
	�ȴ����� ******/

	tearDown(); setUp();

	firstPage = m_vheap->getCBMCount() + 2;

	rec = createRecord(m_tblDef);
	rowLockHdl = NULL;


	DataDaVince dvdata1;
	dvdata1.m_record = rec;
	dvdata1.m_rows = 1;
	dvdata1.m_rlh = &rowLockHdl;

	DaVince dv1(m_db, m_conn, m_heap, m_tblDef, "Insert thread to be blocked.", &dvdata1);
	dv1.enableSyncPoint(SP_HEAP_VLR_INSERT_BEFORE_TRY_LOCK_ROW);
	dv1.enableSyncPoint(SP_HEAP_VLR_INSERT_TRY_LOCK_ROW_FAILED);
	dv1.enableSyncPoint(SP_HEAP_VLR_INSERT_UNLOCK_PAGE_AFTER_TRY_LOCK_ROW_FAILED);
	dv1.enableSyncPoint(SP_HEAP_VLR_INSERT_BEFORE_REFIND_FREE_SLOT);
	dv1.enableSyncPoint(SP_HEAP_VLR_INSERT_SLOT_FREE_BUT_NO_SPACE);

	dv1.start();
	dv1.joinSyncPoint(SP_HEAP_VLR_INSERT_BEFORE_TRY_LOCK_ROW);

	session = m_db->getSessionManager()->allocSession("VLRHeapTestCase::testInsert", m_conn);
	/* �����谭dv1�߳� */
	RowLockHandle *rlock = LOCK_ROW(session, m_tblDef->m_id, RID(firstPage, 0), Exclusived);
	assert(rlock);

	dv1.notifySyncPoint(SP_HEAP_VLR_INSERT_BEFORE_TRY_LOCK_ROW);
	dv1.joinSyncPoint(SP_HEAP_VLR_INSERT_TRY_LOCK_ROW_FAILED);

	/* ���� */
	session->unlockRow(&rlock);

	dv1.notifySyncPoint(SP_HEAP_VLR_INSERT_TRY_LOCK_ROW_FAILED);
	dv1.joinSyncPoint(SP_HEAP_VLR_INSERT_UNLOCK_PAGE_AFTER_TRY_LOCK_ROW_FAILED);
	/* ��ʱdv1�����ˣ��������κ��� */

	/* ���Ǽ������뵽��һҳ�� */
	int midRecPerPage = 0;
	while (true) {
		rid = m_heap->insert(session, rec, NULL);
		if (RID_GET_PAGE(rid) == firstPage)
			++midRecPerPage;
		else
			break;
	}
	for (int i = 0; i < midRecPerPage - 1; ++i) { // ������ǰҳ
		m_heap->insert(session, rec, NULL); 
	}

	/* ��������dv1����Ϊ�Ǹ����Ѿ��������ˣ����Ի��ۻ�����findFreePage*/
	dv1.notifySyncPoint(SP_HEAP_VLR_INSERT_UNLOCK_PAGE_AFTER_TRY_LOCK_ROW_FAILED);
	/* Ȼ���ֻ�ͣ��TRY LOCK ROW���� */
	dv1.joinSyncPoint(SP_HEAP_VLR_INSERT_BEFORE_TRY_LOCK_ROW);

	/* �ٴ�����֮��ע�⣬��������в�ͬ */
	rlock = LOCK_ROW(session, m_tblDef->m_id, RID(firstPage + 2, 0), Exclusived);

	/* ��dv1�ڳ��Լ���ʧ�ܺ�ס */
	dv1.notifySyncPoint(SP_HEAP_VLR_INSERT_BEFORE_TRY_LOCK_ROW);
	dv1.joinSyncPoint(SP_HEAP_VLR_INSERT_TRY_LOCK_ROW_FAILED);

	/* ���� */
	session->unlockRow(&rlock);

	dv1.notifySyncPoint(SP_HEAP_VLR_INSERT_TRY_LOCK_ROW_FAILED);
	dv1.joinSyncPoint(SP_HEAP_VLR_INSERT_UNLOCK_PAGE_AFTER_TRY_LOCK_ROW_FAILED);
	/* ��ʱdv1�����ˣ��������κ��� */
	


	/* ����һ����¼���������������ˣ�����ҳ��ռ仹�㹻��Ӧ�û���ת�����»�ȡfree slot�Ĵ���� */
	m_heap->insert(session, rec, NULL);

	/* Ӧ��ͣ�� ���� */
	dv1.notifySyncPoint(SP_HEAP_VLR_INSERT_UNLOCK_PAGE_AFTER_TRY_LOCK_ROW_FAILED);
	dv1.joinSyncPoint(SP_HEAP_VLR_INSERT_BEFORE_REFIND_FREE_SLOT);


	/* ��������dv1����Ϊ�Ǹ����Ѿ��������ˣ����Ի��ۻ�����findFreePage*/
	dv1.notifySyncPoint(SP_HEAP_VLR_INSERT_BEFORE_REFIND_FREE_SLOT);
	/* Ȼ���ֻ�ͣ��TRY LOCK ROW���� */
	dv1.joinSyncPoint(SP_HEAP_VLR_INSERT_BEFORE_TRY_LOCK_ROW);

	/* �ٴ�����֮��ע�⣬��������в�ͬ */
	rid = RID(firstPage + 2, 1);
	rlock = LOCK_ROW(session, m_tblDef->m_id, rid, Exclusived);

	/* ��dv1�ڳ��Լ���ʧ�ܺ�ס */
	dv1.notifySyncPoint(SP_HEAP_VLR_INSERT_BEFORE_TRY_LOCK_ROW);
	dv1.joinSyncPoint(SP_HEAP_VLR_INSERT_TRY_LOCK_ROW_FAILED);

	/* ���� */
	session->unlockRow(&rlock);

	dv1.notifySyncPoint(SP_HEAP_VLR_INSERT_TRY_LOCK_ROW_FAILED);
	dv1.joinSyncPoint(SP_HEAP_VLR_INSERT_UNLOCK_PAGE_AFTER_TRY_LOCK_ROW_FAILED);
	/* ��ʱdv1�����ˣ��������κ��� */


	/* ����һҳ��������tinyRecord */
	Record *smallRec = createRecordSmall(m_tblDef);
	while (true) {
		rid1 = m_heap->insert(session, smallRec, NULL);
		if (RID_GET_PAGE(rid1) > firstPage + 2)
			break;
	}
	canDel = m_heap->del(session, rid);
	CPPUNIT_ASSERT(canDel);

	/* dv1���ߵ�slotΪ�գ����ǿռ䲻���ķ�֧ */
	dv1.notifySyncPoint(SP_HEAP_VLR_INSERT_UNLOCK_PAGE_AFTER_TRY_LOCK_ROW_FAILED);
	dv1.joinSyncPoint(SP_HEAP_VLR_INSERT_SLOT_FREE_BUT_NO_SPACE);

	/* ���� */
	dv1.notifySyncPoint(SP_HEAP_VLR_INSERT_SLOT_FREE_BUT_NO_SPACE);
	dv1.joinSyncPoint(SP_HEAP_VLR_INSERT_BEFORE_TRY_LOCK_ROW);
	dv1.notifySyncPoint(SP_HEAP_VLR_INSERT_BEFORE_TRY_LOCK_ROW);

	m_db->getSessionManager()->freeSession(session);

	dv1.join();

	freeRecord(rec);
	freeRecord(smallRec);


	/************* ���ǲ���end ************/


	/******* ����insertIntoSlot��
			if (!m_lastHeaderPos) {
				assert(!m_recordNum);
				m_lastHeaderPos = sizeof(VLRHeapRecordPageInfo) - sizeof(VLRHeader); // �������
			}
	 �Ĳ������� *******/

	tearDown(); setUp();

	rec = createRecordSmall(m_tblDef);
	session = m_db->getSessionManager()->allocSession("VLRHeapTestCase::testInsert", m_conn);

	rid1 = m_heap->insert(session, rec, NULL);
	rid2 = m_heap->insert(session, rec, NULL);
	rid3 = m_heap->insert(session, rec, NULL);

	canDel = m_heap->del(session, rid2);
	CPPUNIT_ASSERT(canDel);

	dvdata1.m_record = rec;
	dvdata1.m_rows = 1;
	assert(!rowLockHdl);
	dvdata1.m_rlh = &rowLockHdl;

	DaVince dv2(m_db, m_conn, m_heap, m_tblDef, "Insert thread to be blocked.", &dvdata1);
	dv2.enableSyncPoint(SP_HEAP_VLR_INSERT_UNLOCK_PAGE_AFTER_TRY_LOCK_ROW_FAILED);

	RowLockHandle *rowLockHdl2 = LOCK_ROW(session, m_tblDef->m_id, rid2, Exclusived);


	dv2.start();
	dv2.joinSyncPoint(SP_HEAP_VLR_INSERT_UNLOCK_PAGE_AFTER_TRY_LOCK_ROW_FAILED);
	CPPUNIT_ASSERT(dv2.isWaitingSyncPoint(SP_HEAP_VLR_INSERT_UNLOCK_PAGE_AFTER_TRY_LOCK_ROW_FAILED));

	session->unlockRow(&rowLockHdl2);

	canDel = m_heap->del(session, rid1);
	CPPUNIT_ASSERT(canDel);
	canDel = m_heap->del(session, rid3);
	CPPUNIT_ASSERT(canDel);



	dv2.disableSyncPoint(SP_HEAP_VLR_INSERT_UNLOCK_PAGE_AFTER_TRY_LOCK_ROW_FAILED);
	dv2.notifySyncPoint(SP_HEAP_VLR_INSERT_UNLOCK_PAGE_AFTER_TRY_LOCK_ROW_FAILED);
	dv2.join();

	m_db->getSessionManager()->freeSession(session);

	freeRecord(rec);
	/************* ���ǲ���end ************/


	/********** ����doGet�л�ȡ����ʱ�ͷ�ҳ��᲻�ᵼ�´��� ***************/
	tearDown();setUp();

	session = m_db->getSessionManager()->allocSession("VLRHeapTestCase::testInsert", m_conn);
	smallRec = createRecordSmall(m_tblDef);
	Record * middleRec = createRecord(m_tblDef);

	while (true) {
		rid1 = m_heap->insert(session, smallRec, NULL);
		if (RID_GET_PAGE(rid1) > firstPage)
			break;
	}
	canDel = m_heap->del(session, rid1);
	CPPUNIT_ASSERT(canDel);
	rid2 = RID(firstPage, 0);
	bool canUpdate = m_heap->update(session, rid2, middleRec);
	CPPUNIT_ASSERT(canUpdate);
	rid3 = m_vheap->getTargetRowId(session, rid2);
	CPPUNIT_ASSERT(rid3 != rid2);
	//cout<<"rid3 page is "<< RID_GET_PAGE(rid3)<<" , rid2 page is "<<RID_GET_PAGE(rid2)<<endl;
	CPPUNIT_ASSERT(RID_GET_PAGE(rid3) != RID_GET_PAGE(rid2));
	//u16 targetSlot = RID_GET_SLOT(rid3);

	DataIndiana dataIndy;
	dataIndy.m_outRec = &record;
	dataIndy.m_rowId = rid2;
	Indiana indy(m_db, m_conn, m_heap, m_tblDef, "Indiana 1", &dataIndy);
	indy.enableSyncPoint(SP_HEAP_VLR_DOGET_AFTER_RELEASE_SOURCE_PAGE);

	indy.start();

	indy.joinSyncPoint(SP_HEAP_VLR_DOGET_AFTER_RELEASE_SOURCE_PAGE);
	CPPUNIT_ASSERT(indy.isWaitingSyncPoint(SP_HEAP_VLR_DOGET_AFTER_RELEASE_SOURCE_PAGE));

	
	canDel = m_heap->del(session, rid2);
	CPPUNIT_ASSERT(canDel);

	for (u16 i = 1; true; ++i) {
		rid4 = RID(firstPage, i);
		canUpdate = m_heap->update(session, rid4, middleRec);
		if (!canUpdate) {
			assert(false);
		}
		rid = m_vheap->getTargetRowId(session, rid4);
		if (rid == rid3) break;
	}

	indy.disableSyncPoint(SP_HEAP_VLR_DOGET_AFTER_RELEASE_SOURCE_PAGE);
	indy.notifySyncPoint(SP_HEAP_VLR_DOGET_AFTER_RELEASE_SOURCE_PAGE);
	indy.join();

	/*
	cout << "get success is "<< dataIndy.m_got<< endl;
	cout << "get rowid is " << dataIndy.m_rowId << endl;
	cout << "get record length is " << dataIndy.m_outRec->m_size <<endl;

	bool canRead =  m_heap->getRecord(session, rid2, &record);
	cout << "RowId " << rid2 << " read " << canRead <<endl;
	*/
	CPPUNIT_ASSERT(dataIndy.m_got == false);


	freeRecord(smallRec);
	freeRecord(middleRec);
	m_db->getSessionManager()->freeSession(session);
	/*********** doGet link���Խ��� ***********/


	/** ���ǲ���1 */
	session = m_db->getSessionManager()->allocSession("VLRHeapTestCase::testInsert", m_conn);

	//uint (*pInsertRecord)(const Record *record, RowId srcRid);
	//*pInsertRecord = VariableLengthRecordHeap::insertRecord;
	VLRHeapRecordPageInfo *page = (VLRHeapRecordPageInfo *)buf;
	page->m_recordNum = 1;
	page->m_freeSpaceSize = Limits::PAGE_SIZE - 22 - 4 - 6;
	page->m_lastHeaderPos = 22;
	page->m_freeSpaceTailPos = 30;
	VLRHeader *header = (VLRHeader *)((byte *)page + page->m_lastHeaderPos);
	header->m_ifEmpty = 0;
	header->m_ifLink = 0;
	header->m_ifTarget = 0;
	header->m_offset = 30;
	header->m_size = 6;

	Record recins(INVALID_ROW_ID, REC_VARLEN, (byte *)session->getMemoryContext()->alloc(200), 200);
	memset(recins.m_data, 255, 200);

	page->spaceIsEnough(2, 100);
	
	page->insertRecord(&recins);
	page->deleteSlot(0);
	page->insertRecord(&recins);

	/* ���� */
	memset(page, 0, Limits::PAGE_SIZE);
	page->m_recordNum = 1;
	page->m_freeSpaceSize = Limits::PAGE_SIZE - 22 - 4 - 6;
	page->m_lastHeaderPos = 22 + sizeof(VLRHeader);
	page->m_freeSpaceTailPos = 30 + sizeof(VLRHeader);
	header = (VLRHeader *)((byte *)page + page->m_lastHeaderPos);
	header->m_ifEmpty = 0;
	header->m_ifLink = 0;
	header->m_ifTarget = 0;
	header->m_offset = page->m_freeSpaceTailPos;
	header->m_size = 6;
	//(header-1)->m_ifEmpty = 1;
	//(header+1)->m_ifEmpty = 1;
	header = (VLRHeader *)((byte *)page + sizeof(VLRHeapRecordPageInfo));
	header->m_ifEmpty = 1;


	//Record recins(INVALID_ROW_ID, REC_VARLEN, (byte *)session->getMemoryContext()->alloc(200), 200);
	//memset(recins.m_data, 255, 200);

	page->spaceIsEnough(2, 100);

	page->insertRecord(&recins);

	m_db->getSessionManager()->freeSession(session);


	/** ���ǲ���2 */
	tearDown();setUp();

	m_vheap->m_maxPageNum =  2;
	m_vheap->m_bitmapNum = 1;
	m_vheap->afterExtendHeap(1);

	session = m_db->getSessionManager()->allocSession("VLRHeapTestCase::testInsert", m_conn);
	m_heap->close(session, false);
	DrsHeap::drop(VLR_HEAP);
	delete m_heap;
	m_heap = m_vheap = NULL;

	m_db->getSessionManager()->freeSession(session);

	System::virtualFree(buf);
}


/** 
 * ����get����
 *		���̣�	1. �������ܲ���
 *				2. �����֧���ǲ��ԣ����ݼ��ڲ�����ע�ͣ�
 */
void VLRHeapTestCase::testGet() {
	Session *session;
	u8 flag;

	Record *rec, *rec2;
	RowId rid, rid1, rid2, rid3;
	u64 pageNum;
	s16 slotNum;

	byte *buf = (byte *)System::virtualAlloc(Limits::PAGE_SIZE);
	Record record;
	record.m_data = buf;
	record.m_format = REC_VARLEN;
	bool canRead, canUpdate;

	/* ����һ��tiny record */
	rec = createTinyRawRecord(111);
	session = m_db->getSessionManager()->allocSession("VLRHeapTestCase::testGet", m_conn);
	rid1 = m_heap->insert(session, rec, NULL);
	CPPUNIT_ASSERT(rid1 == RID(m_vheap->getCBMCount() + 1 + 1, 0));
	/* ��������֤һ�� */
	canRead = m_heap->getRecord(session, rid1, &record);
	CPPUNIT_ASSERT(canRead);
	CPPUNIT_ASSERT(record.m_rowId == rid1);
	CPPUNIT_ASSERT(record.m_size == rec->m_size);
	CPPUNIT_ASSERT(0 == memcmp(record.m_data, rec->m_data, rec->m_size));
	m_db->getSessionManager()->freeSession(session);
	freeRecord(rec);
	/* ����short recordֱ��flag�ı� */
	do {
		rec = createShortRawRecord(109);
		session = m_db->getSessionManager()->allocSession("VLRHeapTestCase::testGet", m_conn);
		rid = m_heap->insert(session, rec, NULL);
		pageNum = RID_GET_PAGE(rid);
		slotNum = RID_GET_SLOT(rid);
		flag = m_vheap->getPageFlag(session, pageNum);
		/* ��������֤һ�� */
		canRead = m_heap->getRecord(session, rid, &record);
		CPPUNIT_ASSERT(canRead);
		CPPUNIT_ASSERT(record.m_rowId == rid);
		CPPUNIT_ASSERT(record.m_size == rec->m_size);
		CPPUNIT_ASSERT(0 == memcmp(record.m_data, rec->m_data, rec->m_size));
		m_db->getSessionManager()->freeSession(session);
		//CPPUNIT_ASSERT(rid == RID(m_vheap->getCBMCount() + 1 + 1, 1));
		freeRecord(rec);
	} while (flag == 3);
	//cout<<"slotNum is "<<slotNum<<endl;
	/* �ٲ���һ��short record */
	rec = createShortRawRecord(108);
 	session = m_db->getSessionManager()->allocSession("VLRHeapTestCase::testGet", m_conn);
	rid2 = m_heap->insert(session, rec, NULL);
	m_db->getSessionManager()->freeSession(session);
	freeRecord(rec);

	/* ����һ��long record, Ӧ�ûỻҳ */
	/** ��Ϊlong record������������hackһ�� **/
	u16 maxRecSize = m_tblDef->m_maxRecSize;
	m_tblDef->m_maxRecSize = Limits::PAGE_SIZE;
	
	rec = createLongRawRecord(107);
	session = m_db->getSessionManager()->allocSession("VLRHeapTestCase::testGet", m_conn);
	rid3 = m_heap->insert(session, rec, NULL);
	pageNum = RID_GET_PAGE(rid);
	slotNum = RID_GET_SLOT(rid);
	flag = m_vheap->getPageFlag(session, pageNum);
	/* ��������֤һ�� */
	canRead = m_heap->getRecord(session, rid3, &record);
	CPPUNIT_ASSERT(canRead);
	CPPUNIT_ASSERT(record.m_rowId == rid3);
	CPPUNIT_ASSERT(record.m_size == rec->m_size);
	CPPUNIT_ASSERT(0 == memcmp(record.m_data, rec->m_data, rec->m_size));
	m_db->getSessionManager()->freeSession(session);
	freeRecord(rec);
	CPPUNIT_ASSERT(RID_GET_PAGE(rid2) != RID_GET_PAGE(rid3));

	/** �ָ�m_maxRecSize **/
	m_tblDef->m_maxRecSize = maxRecSize;

	/* �ٲ���һ��short record��Ӧ���ڸղ�δ�õ�ҳ���� */
	rec = createShortRawRecord(108);
 	session = m_db->getSessionManager()->allocSession("VLRHeapTestCase::testGet", m_conn);
	rid = m_heap->insert(session, rec, NULL);
	/* ��������֤һ�� */
	canRead = m_heap->getRecord(session, rid, &record);
	CPPUNIT_ASSERT(canRead);
	CPPUNIT_ASSERT(record.m_rowId == rid);
	CPPUNIT_ASSERT(record.m_size == rec->m_size);
	CPPUNIT_ASSERT(0 == memcmp(record.m_data, rec->m_data, rec->m_size));
	m_db->getSessionManager()->freeSession(session);
	freeRecord(rec);
	CPPUNIT_ASSERT(RID_GET_PAGE(rid) == RID_GET_PAGE(rid2));


	/***** SubRecord��get���� ******/
	SubRecordBuilder compBuilder(m_tblDef, REC_REDUNDANT);
	SubRecord *subRec = compBuilder.createEmptySbById(Limits::PAGE_SIZE, "0 2 3");

	int rows = 200;
	session = m_db->getSessionManager()->allocSession("VLRHeapTestCase::testGet", m_conn);
	for (int i = 0; i < rows; ++i) {
		rec = createRecord(m_tblDef, 0);


		
		rid = m_heap->insert(session, rec, NULL);
		//CPPUNIT_ASSERT(rid == RID(m_vheap->getCBMCount() + 1 + 1, 0));

		/* ������¼get */
		canRead = m_heap->getRecord(session, rid, &record);
		CPPUNIT_ASSERT(canRead);
		//cout<<"record.m_rowId is "<<record.m_rowId<<endl;
		CPPUNIT_ASSERT(record.m_rowId == rid);
		CPPUNIT_ASSERT(record.m_size = rec->m_size);
		CPPUNIT_ASSERT(0 == memcmp(record.m_data, rec->m_data, record.m_size));

		/* ���Լ�get */
		SubrecExtractor *srExtractor = SubrecExtractor::createInst(session, m_tblDef, subRec);
		canRead = m_heap->getSubRecord(session, rid, srExtractor, subRec);
		CPPUNIT_ASSERT(canRead);
		CPPUNIT_ASSERT(record.m_rowId == rid);
		bool falseUpdate = m_heap->update(session, RID(2, 0), subRec);
		CPPUNIT_ASSERT(!falseUpdate);
		freeRecord(rec);
	}
	m_db->getSessionManager()->freeSession(session);


	/*** ������link��¼��get���� ***/
	rec = createRecord(m_tblDef, 1000);

	rec2 = createShortRawRecord(100);
	session = m_db->getSessionManager()->allocSession("VLRHeapTestCase::testGet", m_conn);
	do {
		u16 freeSpace = m_vheap->getPageFreeSpace(session, RID_GET_PAGE(rid1));
		if (freeSpace < rec->m_size - sizeof(RowId)) break;
		//cout<<"freeSpace is "<<freeSpace<<endl;
		if (RID_GET_PAGE(m_heap->insert(session, rec2, NULL)) > RID_GET_PAGE(rid1))
			break;
	} while (true);
	m_db->getSessionManager()->freeSession(session);
	freeRecord(rec2);

	SubRecord *subRec1 = compBuilder.createEmptySbById(Limits::PAGE_SIZE, "0 4 5");
	SubRecord *subRec2 = compBuilder.createEmptySbById(Limits::PAGE_SIZE, "0 4 5");
	RecordOper::extractSubRecordVR(m_tblDef, rec, subRec1);

	session = m_db->getSessionManager()->allocSession("VLRHeapTestCase::testGet", m_conn);

	canUpdate = m_heap->update(session, rid1, rec);
	CPPUNIT_ASSERT(canUpdate);
	rid = m_vheap->getTargetRowId(session, rid1);
	//CPPUNIT_ASSERT(rid != rid1);

	SubrecExtractor *srExtractor2 = SubrecExtractor::createInst(session, m_tblDef, subRec);
	canRead = m_heap->getSubRecord(session, rid1, srExtractor2, subRec2);
	CPPUNIT_ASSERT(canRead);
	//cout<<"subRec2->m_size is "<<subRec2->m_size<<endl;
	CPPUNIT_ASSERT(subRec2->m_rowId == rid1);
	//CPPUNIT_ASSERT(0 == memcmp(subRec1->m_data, subRec2->m_data, subRec2->m_size));
	freeSubRecord(subRec1);


	m_db->getSessionManager()->freeSession(session);

	freeRecord(rec);

	/******** ���Դ�������get ************/
	tearDown(); setUp();

	Record *rec1 = createRecordSmall(m_tblDef);
	rec2 = createRecord(m_tblDef);
	Record *rec3 = createRecordLong(m_tblDef);
	subRec1 = compBuilder.createEmptySbById(Limits::PAGE_SIZE, "0 4 5");

	RowLockHandle *rlockHdl = NULL;
	session = m_db->getSessionManager()->allocSession("VLRHeapTestCase::testGet", m_conn);
	rid1 = 0;
	while (true) {
		rid = m_heap->insert(session, rec1, NULL);
		if (!rid1) rid1 = rid;
		if (RID_GET_PAGE(rid) != RID_GET_PAGE(rid1)) break;
	}

	canRead = m_heap->getRecord(session, rid, &record, Exclusived, &rlockHdl);
	CPPUNIT_ASSERT(canRead);
	CPPUNIT_ASSERT(rlockHdl->getRid() == rid);
	session->unlockRow(&rlockHdl);
	canRead = m_heap->getRecord(session, rid1, &record, Exclusived, &rlockHdl);
	CPPUNIT_ASSERT(canRead);
	CPPUNIT_ASSERT(rlockHdl->getRid() == rid1);
	session->unlockRow(&rlockHdl);

	SubrecExtractor *srExtractor1 = SubrecExtractor::createInst(session, m_tblDef, subRec);
	canRead = m_heap->getSubRecord(session, rid, srExtractor1, subRec1, Exclusived, &rlockHdl);
	CPPUNIT_ASSERT(canRead);
	CPPUNIT_ASSERT(rlockHdl->getRid() == rid);
	session->unlockRow(&rlockHdl);
	canRead = m_heap->getSubRecord(session, rid1, srExtractor1, subRec1, Exclusived, &rlockHdl);
	CPPUNIT_ASSERT(canRead);
	CPPUNIT_ASSERT(rlockHdl->getRid() == rid1);
	session->unlockRow(&rlockHdl);

	/* ɾ��rid1 */
	m_heap->del(session, rid1);
	canRead = m_heap->getSubRecord(session, rid1, srExtractor1, subRec1, Exclusived, &rlockHdl);
	CPPUNIT_ASSERT(!canRead);
	CPPUNIT_ASSERT(!rlockHdl);
	canRead = m_heap->getRecord(session, rid1, &record, Exclusived, &rlockHdl);
	CPPUNIT_ASSERT(!canRead);
	CPPUNIT_ASSERT(!rlockHdl);



	
	m_db->getSessionManager()->freeSession(session);


        freeSubRecord(subRec);
        freeSubRecord(subRec1);
        freeSubRecord(subRec2);
        freeRecord(rec1);
        freeRecord(rec2);
        freeRecord(rec3);

	System::virtualFree(buf);
}

/** 
 * ����del����
 *		���̣�	1. �������ܲ���
 *				2. �����֧���ǲ��ԣ����ݼ��ڲ�����ע�ͣ�
 */
void VLRHeapTestCase::testDelete() {
	Session *session;
	u8 flag;

	Record *rec, *rec1, *rec2;
	RowId rid, rid1, rid2, rid3, rid4;
	u64 pageNum;
	s16 slotNum;

	byte *buf = (byte *)System::virtualAlloc(Limits::PAGE_SIZE);
	Record record;
	record.m_data = buf;
	record.m_format = REC_VARLEN;
	bool canRead, canDel, canUpdate;

	CPPUNIT_ASSERT(m_heap->getStatus().m_dboStats->m_statArr[DBOBJ_ITEM_DELETE] == 0);

	/* ����һ��tiny record */
	rec = createTinyRawRecord(111);
	session = m_db->getSessionManager()->allocSession("VLRHeapTestCase::testDelete", m_conn);
	rid1 = m_heap->insert(session, rec, NULL);
	CPPUNIT_ASSERT(rid1 == RID(m_vheap->getCBMCount() + 1 + 1, 0));
	/* ��������֤һ�� */
	canRead = m_heap->getRecord(session, rid1, &record);
	CPPUNIT_ASSERT(canRead);
	CPPUNIT_ASSERT(record.m_rowId == rid1);
	CPPUNIT_ASSERT(record.m_size == rec->m_size);
	CPPUNIT_ASSERT(0 == memcmp(record.m_data, rec->m_data, rec->m_size));
	m_db->getSessionManager()->freeSession(session);
	freeRecord(rec);
	/* ����short recordֱ��flag�ı� */
	do {
		rec = createShortRawRecord(109);
		session = m_db->getSessionManager()->allocSession("VLRHeapTestCase::testDelete", m_conn);
		rid = m_heap->insert(session, rec, NULL);
		pageNum = RID_GET_PAGE(rid);
		slotNum = RID_GET_SLOT(rid);
		flag = m_vheap->getPageFlag(session, pageNum);
		/* ��������֤һ�� */
		canRead = m_heap->getRecord(session, rid, &record);
		CPPUNIT_ASSERT(canRead);
		CPPUNIT_ASSERT(record.m_rowId == rid);
		CPPUNIT_ASSERT(record.m_size == rec->m_size);
		CPPUNIT_ASSERT(0 == memcmp(record.m_data, rec->m_data, rec->m_size));
		m_db->getSessionManager()->freeSession(session);
		//CPPUNIT_ASSERT(rid == RID(m_vheap->getCBMCount() + 1 + 1, 1));
		freeRecord(rec);
	} while (flag == 3);
	//cout<<"slotNum is "<<slotNum<<endl;
	/* �ٲ���һ��short record */
	rec = createShortRawRecord(108);
 	session = m_db->getSessionManager()->allocSession("VLRHeapTestCase::testDelete", m_conn);
	rid2 = m_heap->insert(session, rec, NULL);
	m_db->getSessionManager()->freeSession(session);
	freeRecord(rec);

	/* ����һ��long record, Ӧ�ûỻҳ */
	/** ��Ϊlong record������������hackһ�� **/
	u16 maxRecSize = m_tblDef->m_maxRecSize;
	m_tblDef->m_maxRecSize = Limits::PAGE_SIZE;
	
	rec = createLongRawRecord(107);
	session = m_db->getSessionManager()->allocSession("VLRHeapTestCase::testDelete", m_conn);
	rid3 = m_heap->insert(session, rec, NULL);
	pageNum = RID_GET_PAGE(rid);
	slotNum = RID_GET_SLOT(rid);
	flag = m_vheap->getPageFlag(session, pageNum);
	/* ��������֤һ�� */
	canRead = m_heap->getRecord(session, rid3, &record);
	CPPUNIT_ASSERT(canRead);
	CPPUNIT_ASSERT(record.m_rowId == rid3);
	CPPUNIT_ASSERT(record.m_size == rec->m_size);
	CPPUNIT_ASSERT(0 == memcmp(record.m_data, rec->m_data, rec->m_size));
	m_db->getSessionManager()->freeSession(session);
	freeRecord(rec);
	CPPUNIT_ASSERT(RID_GET_PAGE(rid2) != RID_GET_PAGE(rid3));

	/** �ָ�m_maxRecSize **/
	m_tblDef->m_maxRecSize = maxRecSize;

	/* �ٲ���һ��short record��Ӧ���ڸղ�δ�õ�ҳ���� */
	rec = createShortRawRecord(108);
	session = m_db->getSessionManager()->allocSession("VLRHeapTestCase::testDelete", m_conn);
	rid = m_heap->insert(session, rec, NULL);
	/* ��������֤һ�� */
	canRead = m_heap->getRecord(session, rid, &record);
	CPPUNIT_ASSERT(canRead);
	CPPUNIT_ASSERT(record.m_rowId == rid);
	CPPUNIT_ASSERT(record.m_size == rec->m_size);
	CPPUNIT_ASSERT(0 == memcmp(record.m_data, rec->m_data, rec->m_size));
	m_db->getSessionManager()->freeSession(session);
	freeRecord(rec);
	CPPUNIT_ASSERT(RID_GET_PAGE(rid) == RID_GET_PAGE(rid2));


	session = m_db->getSessionManager()->allocSession("VLRHeapTestCase::testDelete", m_conn);
	/** ɾ��rid1,rid2,rid3��Ӧ�ļ�¼ **/
	canDel = m_heap->del(session, rid1);
	CPPUNIT_ASSERT(canDel);
	canDel = m_heap->del(session, rid1);
	CPPUNIT_ASSERT(!canDel);
	canRead = m_heap->getRecord(session, rid1, &record);
	CPPUNIT_ASSERT(!canRead);

	canDel = m_heap->del(session, rid2);
	CPPUNIT_ASSERT(canDel);
	canDel = m_heap->del(session, rid2);
	CPPUNIT_ASSERT(!canDel);
	canRead = m_heap->getRecord(session, rid2, &record);
	CPPUNIT_ASSERT(!canRead);

	canDel = m_heap->del(session, rid3);
	CPPUNIT_ASSERT(canDel);
	canDel = m_heap->del(session, rid3);
	CPPUNIT_ASSERT(!canDel);
	canRead = m_heap->getRecord(session, rid3, &record);
	CPPUNIT_ASSERT(!canRead);
	/** ɾ����� **/
	m_db->getSessionManager()->freeSession(session);

	CPPUNIT_ASSERT(m_heap->getStatus().m_dboStats->m_statArr[DBOBJ_ITEM_DELETE] == 3);

	/*** ���²���link��record ***/
	tearDown();
	setUp();
	/* ��tiny record������һҳ */
	rec = createTinyRawRecord(99);
	session = m_db->getSessionManager()->allocSession("VLRHeapTestCase::testDelete", m_conn);
	rid1 = 0;
	do {
		rid = m_heap->insert(session, rec, NULL);
		if (!rid1) rid1 = rid;
		else if (RID_GET_PAGE(rid) > RID_GET_PAGE(rid1)) break;
	} while (true);
	//cout<<"rid is "<<rid<<" , rid1 is "<<rid1<<endl;
	m_db->getSessionManager()->freeSession(session);
	freeRecord(rec);
	/* ��short record���滻rid1��tiny record����Ȼ�����ҳ�����link */
	rec = createShortRawRecord(100);
	session = m_db->getSessionManager()->allocSession("VLRHeapTestCase::testDelete", m_conn);
	canUpdate = m_heap->update(session, rid1, rec);
	CPPUNIT_ASSERT(canUpdate);
	rid2 = m_vheap->getTargetRowId(session, rid1);
	CPPUNIT_ASSERT(rid2 != rid1);
	rid3 = m_vheap->getTargetRowId(session, rid2);
	CPPUNIT_ASSERT(rid3 == rid1);
	m_db->getSessionManager()->freeSession(session);
	freeRecord(rec);
	/* ɾ��rid1�����Ǹ�link record */
	session = m_db->getSessionManager()->allocSession("VLRHeapTestCase::testDelete", m_conn);
	canDel = m_heap->del(session, rid1);
	CPPUNIT_ASSERT(canDel);
	m_db->getSessionManager()->freeSession(session);


	/****** ����λͼ�仯���� *******/
	tearDown();
	setUp();


	/* �Ȳ���һ��һ��ҳ������ɾ���ٿ��Ըı�flag */
	int tinyRecPerPage = 0;
	rid1 = 0;
	rec1 = createTinyRawRecord(102);
	session = m_db->getSessionManager()->allocSession("VLRHeapTestCase::testDelete", m_conn);
	do {
		rid = m_heap->insert(session, rec1, NULL);
		if (!rid1) rid1 = rid;
		if (RID_GET_PAGE(rid) > RID_GET_PAGE(rid1))
			break;
		++tinyRecPerPage;
	} while (true);
	//cout<<"tinyRecPerPage is "<<tinyRecPerPage<<endl;
	flag = m_vheap->getPageFlag(session, RID_GET_PAGE(rid1));
	CPPUNIT_ASSERT(flag == 0x0);
	int tinyRecDec;
	for (int i = 1; i <= tinyRecPerPage; ++i) {
		canDel = m_heap->del(session, RID(RID_GET_PAGE(rid1), i - 1));
		CPPUNIT_ASSERT(canDel);
		if (m_vheap->getPageFlag(session, RID_GET_PAGE(rid1)) != flag) {
			tinyRecDec = i;
			break;
		}
	}

	
	m_db->getSessionManager()->freeSession(session);
	//cout<<"Tiny rec limit is "<<tinyRecDec<<endl;
	/** ����֪�������� **/
	tearDown();
	setUp();
	/* ������һҳ */
	session = m_db->getSessionManager()->allocSession("VLRHeapTestCase::testDelete", m_conn);
	for (int i = 0; i < tinyRecPerPage; ++i) {
		rid3 = m_heap->insert(session, rec1, NULL);
	}
	/* ����tinyRecDec����long record��update */
	//rec2 = createLongRawRecord(103);
	rec2 = createRecordLong(m_tblDef, 103);
	rid1 = RID(RID_GET_PAGE(rid3), tinyRecDec - 1);
	canUpdate = m_heap->update(session, rid1, rec2);
	CPPUNIT_ASSERT(canUpdate);
	rid2 = m_vheap->getTargetRowId(session, rid1);
	CPPUNIT_ASSERT(rid2 != rid1);
	/* ɾ��tinyRecDec - 1�� */
	for (int i = 0; i < tinyRecDec - 1; ++i) {
		canDel = m_heap->del(session, RID(RID_GET_PAGE(rid3), i));
		CPPUNIT_ASSERT(canDel);
	}
	flag = m_vheap->getPageFlag(session, RID_GET_PAGE(rid3));
	CPPUNIT_ASSERT(flag == 0);
	/* ����ɾ��������ӣ�����λͼ����仯�� */
	canDel = m_heap->del(session, rid1);
	CPPUNIT_ASSERT(canDel);
	CPPUNIT_ASSERT(flag != m_vheap->getPageFlag(session, RID_GET_PAGE(rid3)));

	m_db->getSessionManager()->freeSession(session);
	freeRecord(rec1);
	freeRecord(rec2);

	/****** �����pct freeΪ0ʱ���һ��ҳ���ö��ٵ�tinyRecord ******/
	tearDown();
	setUp();
	rec1 = createTinyRawRecord(99);
	session = m_db->getSessionManager()->allocSession("VLRHeapTestCase::testDelete", m_conn);
	//try {
	//	m_heap->setPctFree(session, 99);
	//	CPPUNIT_ASSERT(false);
	//} catch (NtseException) {
	//	CPPUNIT_ASSERT(true);
	//}
	m_heap->setPctFree(session, 0);
	rid1 = rid2 = 0;
	int tinyRecPerPagePct0 = 0;
	while (true) {
		rid1 = m_heap->insert(session, rec1, NULL);
		if (!rid2) rid2 = rid1;
		if (RID_GET_PAGE(rid1) != RID_GET_PAGE(rid2)) break;
		++tinyRecPerPagePct0;
	}
	m_db->getSessionManager()->freeSession(session);
	//cout<<"tinyRecPerPagePct0 is "<<tinyRecPerPagePct0<<endl;
	freeRecord(rec1);


	/****** ����del������sourceҳ��flag�ı䣬��target�������� *******/
	tearDown();
	setUp();
	rec1 = createTinyRawRecord(77);
	rec2 = createRecordSmall(m_tblDef);

	/* insert tiny */
	session = m_db->getSessionManager()->allocSession("VLRHeapTestCase::testDelete", m_conn);
	/* set pct free to 0 */
	m_heap->setPctFree(session, 0);
	for (int i = 0; i < tinyRecPerPagePct0; ++i) {
		rid1 = m_heap->insert(session, rec1, NULL);
	}
	m_db->getSessionManager()->freeSession(session);


	/* update */
	session = m_db->getSessionManager()->allocSession("VLRHeapTestCase::testDelete", m_conn);

	for (int i = 0; i < tinyRecPerPagePct0; ++i) {
		rid2 = RID(RID_GET_PAGE(rid1), i);
		canUpdate = m_heap->update(session, rid2, rec2);
		CPPUNIT_ASSERT(canUpdate);
		rid3 = m_vheap->getTargetRowId(session, rid2);
		if (rid3 == rid2 ) cout<<endl<<i<<endl;
		CPPUNIT_ASSERT(rid3 != rid2);
	}
	m_db->getSessionManager()->freeSession(session);

	/* delete */
	session = m_db->getSessionManager()->allocSession("VLRHeapTestCase::testDelete", m_conn);
	for (int i = 0; i < tinyRecPerPagePct0; ++i) {
		rid4 = RID(RID_GET_PAGE(rid1), i);
		canDel = m_heap->del(session, rid4);
		CPPUNIT_ASSERT(canDel);
	}
	m_db->getSessionManager()->freeSession(session);

	freeRecord(rec1);
	freeRecord(rec2);

	/****** end ******/


	System::virtualFree(buf);
}

/** 
* ����update����
*		���̣�	1. �������ܲ��ԣ�Record update��SubRecord update��
*				2. �����֧���ǲ��ԣ����ݼ��ڲ�����ע�ͣ�
*/
void VLRHeapTestCase::testUpdate() {
	if (Limits::PAGE_SIZE == 8192) {
	Session *session;
	u64 firstPage = m_vheap->getCBMCount() + 2;

	Record *rec1, *rec2, *rec3;
	RowId rid, rid1, rid2, rid3;
	s16 slotNum;
	int smallRecPerPage = 0;
	int updS2Mfill = 0;


	MemoryContext mc(Limits::PAGE_SIZE, 128);

	//byte *buf = (byte *)System::virtualAlloc(Limits::PAGE_SIZE * 2);
	Record record1, record2;
	record1.m_data = (byte *)mc.alloc(Limits::PAGE_SIZE);
	record2.m_data = (byte *)mc.alloc(Limits::PAGE_SIZE);
	record2.m_format = record1.m_format = REC_VARLEN;
	bool canDel, canUpdate;

	rec1 = createRecordSmall(m_tblDef, 10);
	rec2 = createRecord(m_tblDef, 20);
	rec3 = createRecordLong(m_tblDef, 30);

	/*******
	 *newTagPgN < srcPageNum < oldTagPgN
	 */
	//���ÿҳ��¼��
	session = m_db->getSessionManager()->allocSession("VLRHeapTestCase::testUpdate", m_conn);
	while (true) {
		rid = m_heap->insert(session, rec1, NULL);
		if (RID_GET_PAGE(rid) == firstPage) {
			smallRecPerPage++;
		} else {
			break;
		}
	}
	//�����ڶ�ҳ
	for (int i = 0; i < smallRecPerPage-1; ++i) {
		rid = m_heap->insert(session, rec1, NULL);
		CPPUNIT_ASSERT(RID_GET_PAGE(rid) == firstPage + 1);
	}
	//����������ҳ
	while (true) {
		rid = RID(firstPage + 1, updS2Mfill);
		m_heap->update(session, rid, rec2);
		rid1 = m_vheap->getTargetRowId(session, rid);
		if (rid1 == rid || RID_GET_PAGE(rid1) == RID_GET_PAGE(rid) + 1) {
			updS2Mfill++;
		} else {
			CPPUNIT_ASSERT(RID_GET_PAGE(rid1) == firstPage + 3);
			break;
		}
	}
	//�޸�������־λ
	for (int i = 0; i < 4; ++i) {
		CPPUNIT_ASSERT(m_vheap->m_position[i].m_bitmapIdx == 0);
		m_vheap->m_position[i].m_pageIdx = 2;
	}
	//��������ҳ
	while (true) {
		rid = m_heap->insert(session, rec1, NULL);
		if (RID_GET_PAGE(rid) > firstPage + 3)
			break;
	}
	//ɾ���һҳ
	for (int i = 0; i < smallRecPerPage; ++i) {
		rid = RID(firstPage, i);
		bool success = m_heap->del(session, rid);
		CPPUNIT_ASSERT(success);
	}
	//�޸�������־λ
	for (int i = 0; i < 4; ++i) {
		CPPUNIT_ASSERT(m_vheap->m_position[i].m_bitmapIdx == 0);
		m_vheap->m_position[i].m_pageIdx = 0;
	}
	// ���µڶ�ҳ�ĵ�n��¼
	rid = RID(firstPage + 1, updS2Mfill - 1);
	rid1 = m_vheap->getTargetRowId(session, rid);
	//CPPUNIT_ASSERT(RID_GET_PAGE(rid1) == firstPage + 2);
	canUpdate = m_heap->update(session, rid, rec3);
	CPPUNIT_ASSERT(canUpdate);
	rid1 = m_vheap->getTargetRowId(session, rid);
	CPPUNIT_ASSERT(RID_GET_PAGE(rid1) == firstPage);



	m_db->getSessionManager()->freeSession(session);

	/** ����һ�Σ�����lockThirdMinPage�ķ񶨷�֧ */
	tearDown();setUp();
	session = m_db->getSessionManager()->allocSession("VLRHeapTestCase::testUpdate", m_conn);
	smallRecPerPage = 0;
	while (true) {
		rid = m_heap->insert(session, rec1, NULL);
		if (RID_GET_PAGE(rid) == firstPage) {
			smallRecPerPage++;
		} else {
			break;
		}
	}
	//�����ڶ�ҳ
	for (int i = 0; i < smallRecPerPage-1; ++i) {
		rid = m_heap->insert(session, rec1, NULL);
		CPPUNIT_ASSERT(RID_GET_PAGE(rid) == firstPage + 1);
	}
	//����������ҳ
	updS2Mfill = 0;
	while (true) {
		rid = RID(firstPage + 1, updS2Mfill);
		m_heap->update(session, rid, rec2);
		rid1 = m_vheap->getTargetRowId(session, rid);
		if (rid1 == rid || RID_GET_PAGE(rid1) == RID_GET_PAGE(rid) + 1) {
			updS2Mfill++;
		} else {
			CPPUNIT_ASSERT(RID_GET_PAGE(rid1) == firstPage + 3);
			break;
		}
	}
	//�޸�������־λ
	for (int i = 0; i < 4; ++i) {
		CPPUNIT_ASSERT(m_vheap->m_position[i].m_bitmapIdx == 0);
		m_vheap->m_position[i].m_pageIdx = 2;
	}
	//��������ҳ
	while (true) {
		rid = m_heap->insert(session, rec1, NULL);
		if (RID_GET_PAGE(rid) > firstPage + 3)
			break;
	}
	//ɾ���һҳ
	for (int i = 0; i < smallRecPerPage; ++i) {
		rid = RID(firstPage, i);
		bool success = m_heap->del(session, rid);
		CPPUNIT_ASSERT(success);
	}
	//�޸�������־λ
	for (int i = 0; i < 4; ++i) {
		CPPUNIT_ASSERT(m_vheap->m_position[i].m_bitmapIdx == 0);
		m_vheap->m_position[i].m_pageIdx = 0;
	}
	// ���µڶ�ҳ�ĵ�n��¼
	rid = RID(firstPage + 1, updS2Mfill - 1);
	rid1 = m_vheap->getTargetRowId(session, rid);
	//CPPUNIT_ASSERT(RID_GET_PAGE(rid1) == firstPage + 2);

	//canUpdate = m_heap->update(session, rid, rec3);
	rec3->m_rowId = rid;
	Connie c1(m_db, m_conn, m_heap, m_tblDef, "UPdate it", rec3);
	c1.enableSyncPoint(SP_HEAP_VLR_LOCK3RDPAGE_BEFORE_TRYLOCK);
	c1.enableSyncPoint(SP_HEAP_VLR_LOCKTHIRDPAGE_AFTER_RELEASE);
	c1.start();
	c1.joinSyncPoint(SP_HEAP_VLR_LOCK3RDPAGE_BEFORE_TRYLOCK);
	c1.disableSyncPoint(SP_HEAP_VLR_LOCK3RDPAGE_BEFORE_TRYLOCK);

	BufferPageHandle *importantPageHdl = GET_PAGE(session, m_heap->getHeapFile(), PAGE_HEAP, firstPage, Shared, m_heap->getDBObjStats(), NULL);

	c1.notifySyncPoint(SP_HEAP_VLR_LOCK3RDPAGE_BEFORE_TRYLOCK);
	c1.joinSyncPoint(SP_HEAP_VLR_LOCKTHIRDPAGE_AFTER_RELEASE);

	session->releasePage(&importantPageHdl);
	// ��һҳ����
	for (int i = 0; i < smallRecPerPage; ++i) {
		rid = m_heap->insert(session, rec1, NULL);
		if (RID_GET_PAGE(rid) > firstPage)
			break;
	}


	c1.disableSyncPoint(SP_HEAP_VLR_LOCKTHIRDPAGE_AFTER_RELEASE);
	c1.notifySyncPoint(SP_HEAP_VLR_LOCKTHIRDPAGE_AFTER_RELEASE);

	c1.join();


	CPPUNIT_ASSERT(canUpdate);
	rid1 = m_vheap->getTargetRowId(session, rid);
	CPPUNIT_ASSERT(RID_GET_PAGE(rid1) == firstPage);


	BufferPageHandle *firstPageHdl, *secondPageHdl;
	m_vheap->getTwoBitmap(session, firstPage, firstPage+1, &firstPageHdl, &secondPageHdl);
	session->releasePage(&firstPageHdl);
	session->releasePage(&secondPageHdl);
	m_vheap->getTwoBitmap(session, firstPage, firstPage-1, &firstPageHdl, &secondPageHdl);
	session->releasePage(&firstPageHdl);
	session->releasePage(&secondPageHdl);

	m_db->getSessionManager()->freeSession(session);


	/* ���doUpdate������ */
	tearDown();setUp();
	session = m_db->getSessionManager()->allocSession("VLRHeapTestCase::testUpdate", m_conn);
	// ����һ��ҳ
	for (int i = 0; i < smallRecPerPage * 2; ++i) {
		rid = m_heap->insert(session, rec1, NULL);
	}
	canUpdate = m_heap->update(session, RID(firstPage+2, 0), rec2);
	CPPUNIT_ASSERT(!canUpdate);
	// ɾ����һҳ
	for (int i = 0; i < smallRecPerPage; ++i) {
		rid1 = RID(firstPage, i);
		CPPUNIT_ASSERT(m_heap->del(session, rid1));
	}
	// �л�ָ�뵽��һҳ
	for (int i = 0; i < 4; ++i) {
		CPPUNIT_ASSERT(m_vheap->m_position[i].m_bitmapIdx == 0);
		m_vheap->m_position[i].m_pageIdx = 0;
	}
	rec3->m_rowId = rid;
	Connie c2(m_db, m_conn, m_heap, m_tblDef, "UPdate it", rec3);
	c2.enableSyncPoint(SP_HEAP_VLR_LOCKSECONDPAGE_BEFORE_TRYLOCK);
	c2.start();
	c2.joinSyncPoint(SP_HEAP_VLR_LOCKSECONDPAGE_BEFORE_TRYLOCK);

	// ������һҳ
	importantPageHdl = GET_PAGE(session, m_heap->getHeapFile(), PAGE_HEAP, firstPage, Shared, m_heap->getDBObjStats(), NULL);

	c2.disableSyncPoint(SP_HEAP_VLR_LOCKSECONDPAGE_BEFORE_TRYLOCK);
	c2.enableSyncPoint(SP_HEAP_VLR_LOCKSECONDPAGE_AFTER_RELEASE_FIRST);
	c2.notifySyncPoint(SP_HEAP_VLR_LOCKSECONDPAGE_BEFORE_TRYLOCK);
	c2.joinSyncPoint(SP_HEAP_VLR_LOCKSECONDPAGE_AFTER_RELEASE_FIRST);

	session->releasePage(&importantPageHdl);
	/*
	//������һҳ
	for (int i = 0; i < smallRecPerPage; ++i) {
		m_heap->insert(session, rec1, NULL);
	}
	*/
	// ɾ���ڶ�ҳ
	for (int i = 0; i < smallRecPerPage; ++i) {
		rid = RID(firstPage + 1, i);
		if (rid != rec3->m_rowId)
			m_heap->del(session, rid);
	}

	c2.disableSyncPoint(SP_HEAP_VLR_LOCKSECONDPAGE_AFTER_RELEASE_FIRST);
	c2.notifySyncPoint(SP_HEAP_VLR_LOCKSECONDPAGE_AFTER_RELEASE_FIRST);
	c2.join();

	// �л�ָ�뵽��һҳ
	for (int i = 0; i < 4; ++i) {
		CPPUNIT_ASSERT(m_vheap->m_position[i].m_bitmapIdx == 0);
		m_vheap->m_position[i].m_pageIdx = 0;
	}
	// ����ֱ������ҳ
	while (true) {
		rid = m_heap->insert(session, rec1, NULL);
		if (RID_GET_PAGE(rid) == firstPage + 4)
			break;
	}
	// ɾ����һҳ
	for (int  i = 0; i < smallRecPerPage; ++i) {
		rid = RID(firstPage, i);
		m_heap->del(session, rid);
	}
	// ָ���Ƶ���һҳ
	for (int i = 0; i < 4; ++i) {
		CPPUNIT_ASSERT(m_vheap->m_position[i].m_bitmapIdx == 0);
		m_vheap->m_position[i].m_pageIdx = 0;
	}
	// ���µ���ҳ�ĵ�һ����¼
	//rec3->m_rowId = RID(firstPage +2, 0);
	for (int i = 0; i < 1000; ++i) {
		rid = RID(firstPage +3, i);
		canUpdate = m_heap->update(session, rid, rec3);
		rid1 = m_vheap->getTargetRowId(session, rid);
		if (RID_GET_PAGE(rid1) == firstPage)
			break;
	}

	rec3->m_rowId = rid;
	/* ���Ը�����oldTarget�� */
	Connie c3(m_db, m_conn, m_heap, m_tblDef, "UPdate it", rec3);
	c3.enableSyncPoint(SP_HEAP_VLR_LOCKSECONDPAGE_BEFORE_TRYLOCK);
	c3.start();

	c3.joinSyncPoint(SP_HEAP_VLR_LOCKSECONDPAGE_BEFORE_TRYLOCK);
	// ������һҳ
	importantPageHdl = GET_PAGE(session, m_heap->getHeapFile(), PAGE_HEAP, firstPage, Shared, m_heap->getDBObjStats(), NULL);

	c3.disableSyncPoint(SP_HEAP_VLR_LOCKSECONDPAGE_BEFORE_TRYLOCK);
	c3.enableSyncPoint(SP_HEAP_VLR_LOCKSECONDPAGE_AFTER_RELEASE_FIRST);
	c3.notifySyncPoint(SP_HEAP_VLR_LOCKSECONDPAGE_BEFORE_TRYLOCK);
	c3.joinSyncPoint(SP_HEAP_VLR_LOCKSECONDPAGE_AFTER_RELEASE_FIRST);

	session->releasePage(&importantPageHdl);

	c3.disableSyncPoint(SP_HEAP_VLR_LOCKSECONDPAGE_AFTER_RELEASE_FIRST);
	c3.notifySyncPoint(SP_HEAP_VLR_LOCKSECONDPAGE_AFTER_RELEASE_FIRST);
	c3.join();
	/* ���Ը�����source�� */
	Connie c4(m_db, m_conn, m_heap, m_tblDef, "UPdate it", rec3);
	c4.enableSyncPoint(SP_HEAP_VLR_LOCKSECONDPAGE_BEFORE_TRYLOCK);
	c4.start();

	c4.joinSyncPoint(SP_HEAP_VLR_LOCKSECONDPAGE_BEFORE_TRYLOCK);
	// ������һҳ
	importantPageHdl = GET_PAGE(session, m_heap->getHeapFile(), PAGE_HEAP, firstPage, Shared, m_heap->getDBObjStats(), NULL);

	c4.disableSyncPoint(SP_HEAP_VLR_LOCKSECONDPAGE_BEFORE_TRYLOCK);
	c4.enableSyncPoint(SP_HEAP_VLR_LOCKSECONDPAGE_AFTER_RELEASE_FIRST);
	c4.notifySyncPoint(SP_HEAP_VLR_LOCKSECONDPAGE_BEFORE_TRYLOCK);
	c4.joinSyncPoint(SP_HEAP_VLR_LOCKSECONDPAGE_AFTER_RELEASE_FIRST);

	session->releasePage(&importantPageHdl);

	for (int i = 0; i < smallRecPerPage; ++i) {
		rid = RID(RID_GET_PAGE(rec3->m_rowId), i);
		if (rid != rec3->m_rowId)
			m_heap->del(session, rid);
	}

	c4.disableSyncPoint(SP_HEAP_VLR_LOCKSECONDPAGE_AFTER_RELEASE_FIRST);
	c4.notifySyncPoint(SP_HEAP_VLR_LOCKSECONDPAGE_AFTER_RELEASE_FIRST);
	c4.join();

	m_db->getSessionManager()->freeSession(session);

	/***********/
	tearDown();setUp();
	session = m_db->getSessionManager()->allocSession("VLRHeapTestCase::testUpdate", m_conn);
	/*for (u64 i = firstPage; i < firstPage + 10; ++i) {
		for (u16 j = 0; j < smallRecPerPage; ++j) {
			rid = RID(i, j);
			m_heap->del(session, rid);
		}
	}
	// ָ���Ƶ���һҳ
	for (int i = 0; i < 4; ++i) {
		CPPUNIT_ASSERT(m_vheap->m_position[i].m_bitmapIdx == 0);
		m_vheap->m_position[i].m_pageIdx = 0;
	}
	*/
	// ������ҳ
	for (int i = 0; i < smallRecPerPage * 2; ++i) {
		rid = m_heap->insert(session, rec1, NULL);
	}
	CPPUNIT_ASSERT(RID_GET_SLOT(rid) == smallRecPerPage - 1);
	// ���µ�����ҳ
	slotNum = 0;
	while (true) {
		rid = RID(firstPage, slotNum);
		canUpdate = m_heap->update(session, rid, rec2);
		rid1 = m_vheap->getTargetRowId(session, rid);
		if (RID_GET_PAGE(rid) != RID_GET_PAGE(rid1))
			break;
		else
			slotNum++;
	}
	//����������
	while (true) {
		rid2 = m_heap->insert(session, rec1, NULL);
		if (RID_GET_PAGE(rid2) > RID_GET_PAGE(rid1))
			break;
	}
	// ɾ���ڶ�ҳ
	for (int i = 0; i < smallRecPerPage; ++i) {
		rid3 = RID(firstPage +1, i);
		m_heap->del(session, rid3);
	}
	// ָ���Ƶ���һҳ
	for (int i = 0; i < 4; ++i) {
		CPPUNIT_ASSERT(m_vheap->m_position[i].m_bitmapIdx == 0);
		m_vheap->m_position[i].m_pageIdx = 0;
	}

	rec3->m_rowId = rid;
	Connie c5(m_db, m_conn, m_heap, m_tblDef, "UPdate it", rec3);
	c5.enableSyncPoint(SP_HEAP_VLR_LOCKSECONDPAGE_BEFORE_TRYLOCK);
	c5.start();

	c5.joinSyncPoint(SP_HEAP_VLR_LOCKSECONDPAGE_BEFORE_TRYLOCK);
	// �����ڶ�ҳ
	importantPageHdl = GET_PAGE(session, m_heap->getHeapFile(), PAGE_HEAP, firstPage+1, Shared, m_heap->getDBObjStats(), NULL);


	c5.disableSyncPoint(SP_HEAP_VLR_LOCKSECONDPAGE_BEFORE_TRYLOCK);
	c5.enableSyncPoint(SP_HEAP_VLR_LOCKSECONDPAGE_AFTER_RELEASE_FIRST);
	c5.notifySyncPoint(SP_HEAP_VLR_LOCKSECONDPAGE_BEFORE_TRYLOCK);
	c5.joinSyncPoint(SP_HEAP_VLR_LOCKSECONDPAGE_AFTER_RELEASE_FIRST);

	session->releasePage(&importantPageHdl);

	for (int i = 0; i < smallRecPerPage; ++i) {
		rid = RID(RID_GET_PAGE(rid1), i);
		if (rid != rid1)
			m_heap->del(session, rid);
	}

	c5.disableSyncPoint(SP_HEAP_VLR_LOCKSECONDPAGE_AFTER_RELEASE_FIRST);
	c5.notifySyncPoint(SP_HEAP_VLR_LOCKSECONDPAGE_AFTER_RELEASE_FIRST);
	c5.join();

	/**** new < src < old *****/
	for (u64 i = firstPage; i < firstPage + 10; ++i) {
		for (u16 j = 0; j < smallRecPerPage; ++j) {
			rid = RID(i, j);
			m_heap->del(session, rid);
		}
	}
	// ָ���Ƶ���һҳ
	for (int i = 0; i < 4; ++i) {
		CPPUNIT_ASSERT(m_vheap->m_position[i].m_bitmapIdx == 0);
		m_vheap->m_position[i].m_pageIdx = 0;
	}
	// ������ҳ
	for (int i = 0; i < smallRecPerPage * 2; ++i) {
		rid = m_heap->insert(session, rec1, NULL);
	}
	CPPUNIT_ASSERT(RID_GET_SLOT(rid) == smallRecPerPage - 1);
	// ���µ�����ҳ
	slotNum = 0;
	while (true) {
		rid = RID(firstPage + 1, slotNum);
		canUpdate = m_heap->update(session, rid, rec2);
		rid1 = m_vheap->getTargetRowId(session, rid);
		if (RID_GET_PAGE(rid) != RID_GET_PAGE(rid1))
			break;
		else
			slotNum++;
	}
	//����������
	while (true) {
		rid2 = m_heap->insert(session, rec1, NULL);
		if (RID_GET_PAGE(rid2) > RID_GET_PAGE(rid1))
			break;
	}
	// ɾ����һҳ
	for (int i = 0; i < smallRecPerPage; ++i) {
		rid3 = RID(firstPage, i);
		m_heap->del(session, rid3);
	}
	// ָ���Ƶ���һҳ
	for (int i = 0; i < 4; ++i) {
		CPPUNIT_ASSERT(m_vheap->m_position[i].m_bitmapIdx == 0);
		m_vheap->m_position[i].m_pageIdx = 0;
	}

	rec3->m_rowId = rid;
	Connie c6(m_db, m_conn, m_heap, m_tblDef, "UPdate it", rec3);
	c6.enableSyncPoint(SP_HEAP_VLR_BEFORE_GET_MIN_NEWPAGE);
	c6.start();

	c6.joinSyncPoint(SP_HEAP_VLR_BEFORE_GET_MIN_NEWPAGE);
	// ����������һҳ
	for (int i = 0; i < smallRecPerPage; ++i) {
		//rid = RID(firstPage, i);
		rid = m_heap->insert(session, rec1, NULL);
		CPPUNIT_ASSERT(RID_GET_PAGE(rid) == firstPage);
	}

	c6.enableSyncPoint(SP_HEAP_VLR_BEFORE_GET_MAX_NEWPAGE);
	c6.notifySyncPoint(SP_HEAP_VLR_BEFORE_GET_MIN_NEWPAGE);

	c6.joinSyncPoint(SP_HEAP_VLR_BEFORE_GET_MAX_NEWPAGE);
	// ��������ҳ
	while (true) {
		rid = m_heap->insert(session, rec1, NULL);
		if (RID_GET_PAGE(rid) > firstPage + 3)
			break;
	}

	c6.disableSyncPoint(SP_HEAP_VLR_BEFORE_GET_MAX_NEWPAGE);
	c6.notifySyncPoint(SP_HEAP_VLR_BEFORE_GET_MAX_NEWPAGE);

	c6.join();


	m_db->getSessionManager()->freeSession(session);

	/**************/
	tearDown();setUp();
	session = m_db->getSessionManager()->allocSession("VLRHeapTestCase::testUpdate", m_conn);
	// ������ҳ
	for (int i = 0; i < smallRecPerPage * 2; ++i) {
		rid = m_heap->insert(session, rec1, NULL);
	}
	// ɾ��һҳ
	for (int i = 0; i < smallRecPerPage; ++i) {
		rid = RID(firstPage, i);
		canDel = m_heap->del(session, rid);
		CPPUNIT_ASSERT(canDel);
	}
	// ָ���Ƶ���һҳ
	for (int i = 0; i < 4; ++i) {
		CPPUNIT_ASSERT(m_vheap->m_position[i].m_bitmapIdx == 0);
		m_vheap->m_position[i].m_pageIdx = 0;
	}
	// ���µ���һҳ
	slotNum = 0;
	while (true) {
		rid = RID(firstPage + 1, slotNum);
		canUpdate = m_heap->update(session, rid, rec2);
		rid1 = m_vheap->getTargetRowId(session, rid);
		if (RID_GET_PAGE(rid) != RID_GET_PAGE(rid1))
			break;
		else
			slotNum++;
	}
	// ����������ҳ
	while(true) {
		rid2 = m_heap->insert(session, rec1, NULL);
		//pageNum = RID_GET_PAGE(rid2)
		if (RID_GET_PAGE(rid2) >= firstPage+2)
			break;
	}

	rec3->m_rowId = rid;
	Connie c7(m_db, m_conn, m_heap, m_tblDef, "UPdate it", rec3);
	c7.start();
	c7.join();

	/******** ������� *******/
	for (u64 i = firstPage; i < firstPage + 10; ++i) {
		for (u16 j = 0; j < smallRecPerPage; ++j) {
			rid = RID(i, j);
			m_heap->del(session, rid);
		}
	}
	// ָ���Ƶ���һҳ
	for (int i = 0; i < 4; ++i) {
		CPPUNIT_ASSERT(m_vheap->m_position[i].m_bitmapIdx == 0);
		m_vheap->m_position[i].m_pageIdx = 0;
	}
	// ����
	// ������ҳ
	for (int i = 0; i < smallRecPerPage * 3; ++i) {
		rid = m_heap->insert(session, rec1, NULL);
	}
	// ɾ��һҳ
	for (int i = 0; i < smallRecPerPage; ++i) {
		rid = RID(firstPage, i);
		canDel = m_heap->del(session, rid);
		CPPUNIT_ASSERT(canDel);
	}
	// ָ���Ƶ���һҳ
	for (int i = 0; i < 4; ++i) {
		CPPUNIT_ASSERT(m_vheap->m_position[i].m_bitmapIdx == 0);
		m_vheap->m_position[i].m_pageIdx = 0;
	}
	// ���µ���һҳ
	slotNum = 0;
	while (true) {
		rid = RID(firstPage + 2, slotNum);
		canUpdate = m_heap->update(session, rid, rec2);
		rid1 = m_vheap->getTargetRowId(session, rid);
		if (RID_GET_PAGE(rid) != RID_GET_PAGE(rid1))
			break;
		else
			slotNum++;
	}
	// ������һҳ������ҳ
	while(true) {
		rid2 = m_heap->insert(session, rec1, NULL);
		//pageNum = RID_GET_PAGE(rid2)
		if (RID_GET_PAGE(rid2) >= firstPage+2)
			break;
	}
	// ɾ�ڶ�ҳ
	for (int i = 0; i < smallRecPerPage; ++i) {
		rid3 = RID(firstPage+1, i);
		canDel = m_heap->del(session, rid3);
		CPPUNIT_ASSERT(canDel);
	}
	// ָ���Ƶ��ڶ�ҳ
	for (int i = 0; i < 4; ++i) {
		CPPUNIT_ASSERT(m_vheap->m_position[i].m_bitmapIdx == 0);
		m_vheap->m_position[i].m_pageIdx = 1;
	}

	rec3->m_rowId = rid;
	Connie c8(m_db, m_conn, m_heap, m_tblDef, "UPdate it", rec3);
	c8.start();
	c8.join();

	/******����һ����relock��source�ռ��㹻��֧ */
	for (u64 i = firstPage; i < firstPage + 10; ++i) {
		for (u16 j = 0; j < smallRecPerPage; ++j) {
			rid = RID(i, j);
			m_heap->del(session, rid);
		}
	}
	// ָ���Ƶ���һҳ
	for (int i = 0; i < 4; ++i) {
		CPPUNIT_ASSERT(m_vheap->m_position[i].m_bitmapIdx == 0);
		m_vheap->m_position[i].m_pageIdx = 0;
	}
	// ����
	// ������ҳ
	for (int i = 0; i < smallRecPerPage * 3; ++i) {
		rid = m_heap->insert(session, rec1, NULL);
	}
	// ɾ��һҳ
	for (int i = 0; i < smallRecPerPage; ++i) {
		rid = RID(firstPage, i);
		canDel = m_heap->del(session, rid);
		CPPUNIT_ASSERT(canDel);
	}
	// ָ���Ƶ���һҳ
	for (int i = 0; i < 4; ++i) {
		CPPUNIT_ASSERT(m_vheap->m_position[i].m_bitmapIdx == 0);
		m_vheap->m_position[i].m_pageIdx = 0;
	}
	// ���µ���һҳ
	slotNum = 0;
	while (true) {
		rid = RID(firstPage + 2, slotNum);
		canUpdate = m_heap->update(session, rid, rec2);
		rid1 = m_vheap->getTargetRowId(session, rid);
		if (RID_GET_PAGE(rid) != RID_GET_PAGE(rid1))
			break;
		else
			slotNum++;
	}
	// ������һҳ������ҳ
	while(true) {
		rid2 = m_heap->insert(session, rec1, NULL);
		//pageNum = RID_GET_PAGE(rid2)
		if (RID_GET_PAGE(rid2) >= firstPage+2)
			break;
	}
	// ɾ�ڶ�ҳ
	for (int i = 0; i < smallRecPerPage; ++i) {
		rid3 = RID(firstPage+1, i);
		canDel = m_heap->del(session, rid3);
		CPPUNIT_ASSERT(canDel);
	}
	// ָ���Ƶ��ڶ�ҳ
	for (int i = 0; i < 4; ++i) {
		CPPUNIT_ASSERT(m_vheap->m_position[i].m_bitmapIdx == 0);
		m_vheap->m_position[i].m_pageIdx = 1;
	}

	rec3->m_rowId = rid;
	Connie c9(m_db, m_conn, m_heap, m_tblDef, "UPdate it", rec3);
	c9.enableSyncPoint(SP_HEAP_VLR_LOCKSECONDPAGE_AFTER_RELEASE_FIRST);

	importantPageHdl = GET_PAGE(session, m_heap->getHeapFile(), PAGE_HEAP, firstPage+1, Shared, m_heap->getDBObjStats(), NULL);
	
	
	c9.start();
	c9.joinSyncPoint(SP_HEAP_VLR_LOCKSECONDPAGE_AFTER_RELEASE_FIRST);

	session->releasePage(&importantPageHdl);
	// ɾ����ҳ
	for(int i = 0; i < smallRecPerPage; ++i) {
		rid = RID(firstPage + 2, i);
		if (rid != rec3->m_rowId)
			m_heap->del(session, rid);
	}


	c9.disableSyncPoint(SP_HEAP_VLR_LOCKSECONDPAGE_AFTER_RELEASE_FIRST);
	c9.notifySyncPoint(SP_HEAP_VLR_LOCKSECONDPAGE_AFTER_RELEASE_FIRST);
	
	c9.join();


	/********����new���ľ��ķ�֧ *******/
	for (u64 i = firstPage; i < firstPage + 10; ++i) {
		for (u16 j = 0; j < smallRecPerPage; ++j) {
			rid = RID(i, j);
			m_heap->del(session, rid);
		}
	}
	// ָ���Ƶ���һҳ
	for (int i = 0; i < 4; ++i) {
		CPPUNIT_ASSERT(m_vheap->m_position[i].m_bitmapIdx == 0);
		m_vheap->m_position[i].m_pageIdx = 0;
	}
	// ����
	// ������ҳ
	for (int i = 0; i < smallRecPerPage * 3; ++i) {
		rid = m_heap->insert(session, rec1, NULL);
	}
	// ɾ��һҳ
	for (int i = 0; i < smallRecPerPage; ++i) {
		rid = RID(firstPage, i);
		canDel = m_heap->del(session, rid);
		CPPUNIT_ASSERT(canDel);
	}
	// ָ���Ƶ���һҳ
	for (int i = 0; i < 4; ++i) {
		CPPUNIT_ASSERT(m_vheap->m_position[i].m_bitmapIdx == 0);
		m_vheap->m_position[i].m_pageIdx = 0;
	}
	// ���µ���һҳ
	slotNum = 0;
	while (true) {
		rid = RID(firstPage + 2, slotNum);
		canUpdate = m_heap->update(session, rid, rec2);
		rid1 = m_vheap->getTargetRowId(session, rid);
		if (RID_GET_PAGE(rid) != RID_GET_PAGE(rid1))
			break;
		else
			slotNum++;
	}
	// ������һҳ������ҳ
	while(true) {
		rid2 = m_heap->insert(session, rec1, NULL);
		//pageNum = RID_GET_PAGE(rid2)
		if (RID_GET_PAGE(rid2) >= firstPage+2)
			break;
	}
	// ɾ�ڶ�ҳ
	for (int i = 0; i < smallRecPerPage; ++i) {
		rid3 = RID(firstPage+1, i);
		canDel = m_heap->del(session, rid3);
		CPPUNIT_ASSERT(canDel);
	}
	// ָ���Ƶ��ڶ�ҳ
	for (int i = 0; i < 4; ++i) {
		CPPUNIT_ASSERT(m_vheap->m_position[i].m_bitmapIdx == 0);
		m_vheap->m_position[i].m_pageIdx = 1;
	}

	rec3->m_rowId = rid;
	Connie c10(m_db, m_conn, m_heap, m_tblDef, "UPdate it", rec3);
	c10.enableSyncPoint(SP_HEAP_VLR_LOCKSECONDPAGE_AFTER_RELEASE_FIRST);

	importantPageHdl = GET_PAGE(session, m_heap->getHeapFile(), PAGE_HEAP, firstPage+1, Shared, m_heap->getDBObjStats(), NULL);


	c10.start();
	c10.joinSyncPoint(SP_HEAP_VLR_LOCKSECONDPAGE_AFTER_RELEASE_FIRST);

	session->releasePage(&importantPageHdl);
	// �����ڶ�ҳ
	for (int i =0; i < smallRecPerPage; ++i) {
		rid = m_heap->insert(session, rec1, NULL);
		CPPUNIT_ASSERT(RID_GET_PAGE(rid) == firstPage + 1);
	}


	c10.disableSyncPoint(SP_HEAP_VLR_LOCKSECONDPAGE_AFTER_RELEASE_FIRST);
	c10.notifySyncPoint(SP_HEAP_VLR_LOCKSECONDPAGE_AFTER_RELEASE_FIRST);

	c10.join();



	m_db->getSessionManager()->freeSession(session);

	/******************/
	//SP_HEAP_VLR_BEFORE_RELOCK_SECOND_PAGE_NEW_SMALLER_THAN_SRC
	session = m_db->getSessionManager()->allocSession("VLRHeapTestCase::testUpdate", m_conn);
	// �������
	for (u64 i = firstPage; i < firstPage + 10; ++i) {
		for (u16 j = 0; j < smallRecPerPage; ++j) {
			rid = RID(i, j);
			m_heap->del(session, rid);
		}
	}
	// ָ���Ƶ�����ҳ
	for (int i = 0; i < 4; ++i) {
		CPPUNIT_ASSERT(m_vheap->m_position[i].m_bitmapIdx == 0);
		m_vheap->m_position[i].m_pageIdx = 2;
	}
	// ��������ҳ
	for (int i = 0; i < smallRecPerPage; ++i) {
		rid = m_heap->insert(session, rec1, NULL);
		CPPUNIT_ASSERT(RID_GET_PAGE(rid) == firstPage + 2);
	}
	// ָ���Ƶ��ڶ�ҳ
	for (int i = 0; i < 4; ++i) {
		CPPUNIT_ASSERT(m_vheap->m_position[i].m_bitmapIdx == 0);
		m_vheap->m_position[i].m_pageIdx = 1;
	}
	// ���µ��ڶ�ҳ
	slotNum = 0;
	while (true) {
		rid1 = RID(firstPage + 2, slotNum);
		canUpdate = m_heap->update(session, rid1, rec2);
		rid2 = m_vheap->getTargetRowId(session, rid1);
		if (RID_GET_PAGE(rid2) == firstPage + 1)
			break;
		slotNum++;
	}
	// ���뵽����ҳ
	while (true) {
		rid3 = m_heap->insert(session, rec1, NULL);
		if (RID_GET_PAGE(rid3) == firstPage + 3)
			break;
	}
	// ɾ����һҳ
	for (int i = 0; i < smallRecPerPage; ++i) {
		rid3 = RID(firstPage, i);
		canDel = m_heap->del(session, rid3);
		CPPUNIT_ASSERT(canDel);
	}
	// ָ���Ƶ���һҳ
	for (int i = 0; i < 4; ++i) {
		CPPUNIT_ASSERT(m_vheap->m_position[i].m_bitmapIdx == 0);
		m_vheap->m_position[i].m_pageIdx = 0;
	}

	rec3->m_rowId = rid1;
	Connie c11(m_db, m_conn, m_heap, m_tblDef, "UPdate it", rec3);
	c11.start();
	c11.join();

	/*******************/
	// �������
	for (u64 i = firstPage; i < firstPage + 10; ++i) {
		for (u16 j = 0; j < smallRecPerPage; ++j) {
			rid = RID(i, j);
			m_heap->del(session, rid);
		}
	}
	// ָ���Ƶ�����ҳ
	for (int i = 0; i < 4; ++i) {
		CPPUNIT_ASSERT(m_vheap->m_position[i].m_bitmapIdx == 0);
		m_vheap->m_position[i].m_pageIdx = 2;
	}
	// ��������ҳ
	for (int i = 0; i < smallRecPerPage; ++i) {
		rid = m_heap->insert(session, rec1, NULL);
		CPPUNIT_ASSERT(RID_GET_PAGE(rid) == firstPage + 2);
	}
	// ָ���Ƶ��ڶ�ҳ
	for (int i = 0; i < 4; ++i) {
		CPPUNIT_ASSERT(m_vheap->m_position[i].m_bitmapIdx == 0);
		m_vheap->m_position[i].m_pageIdx = 1;
	}
	// ���µ��ڶ�ҳ
	slotNum = 0;
	while (true) {
		rid1 = RID(firstPage + 2, slotNum);
		canUpdate = m_heap->update(session, rid1, rec2);
		rid2 = m_vheap->getTargetRowId(session, rid1);
		if (RID_GET_PAGE(rid2) == firstPage + 1)
			break;
		slotNum++;
	}
	// ���뵽����ҳ
	while (true) {
		rid3 = m_heap->insert(session, rec1, NULL);
		if (RID_GET_PAGE(rid3) == firstPage + 3)
			break;
	}
	// ɾ����һҳ
	for (int i = 0; i < smallRecPerPage; ++i) {
		rid3 = RID(firstPage, i);
		canDel = m_heap->del(session, rid3);
		CPPUNIT_ASSERT(canDel);
	}
	// ָ���Ƶ���һҳ
	for (int i = 0; i < 4; ++i) {
		CPPUNIT_ASSERT(m_vheap->m_position[i].m_bitmapIdx == 0);
		m_vheap->m_position[i].m_pageIdx = 0;
	}

	rec3->m_rowId = rid1;
	Connie c12(m_db, m_conn, m_heap, m_tblDef, "UPdate it", rec3);
	c12.enableSyncPoint(SP_HEAP_VLR_LOCK3RDPAGE_BEFORE_TRYLOCK);
	c12.enableSyncPoint(SP_HEAP_VLR_LOCKTHIRDPAGE_AFTER_RELEASE);
	c12.start();

	c12.joinSyncPoint(SP_HEAP_VLR_LOCK3RDPAGE_BEFORE_TRYLOCK);

	importantPageHdl = GET_PAGE(session, m_heap->getHeapFile(), PAGE_HEAP, firstPage, Shared, m_heap->getDBObjStats(), NULL);

	c12.disableSyncPoint(SP_HEAP_VLR_LOCK3RDPAGE_BEFORE_TRYLOCK);
	c12.notifySyncPoint(SP_HEAP_VLR_LOCK3RDPAGE_BEFORE_TRYLOCK);

	c12.joinSyncPoint(SP_HEAP_VLR_LOCKTHIRDPAGE_AFTER_RELEASE);

	session->releasePage(&importantPageHdl);

	for (int i = 0; i < smallRecPerPage; ++i) {
		rid = RID(firstPage + 2, i);
		if (rid != rec3->m_rowId)
			m_heap->del(session, rid);
	}

	c12.disableSyncPoint(SP_HEAP_VLR_LOCKTHIRDPAGE_AFTER_RELEASE);
	c12.notifySyncPoint(SP_HEAP_VLR_LOCKTHIRDPAGE_AFTER_RELEASE);
	c12.join();

	/***********************/
	for (u64 i = firstPage; i < firstPage + 10; ++i) {
		for (u16 j = 0; j < smallRecPerPage; ++j) {
			rid = RID(i, j);
			m_heap->del(session, rid);
		}
	}
	// ָ���Ƶ�����ҳ
	for (int i = 0; i < 4; ++i) {
		CPPUNIT_ASSERT(m_vheap->m_position[i].m_bitmapIdx == 0);
		m_vheap->m_position[i].m_pageIdx = 2;
	}
	// ��������ҳ
	for (int i = 0; i < smallRecPerPage; ++i) {
		rid = m_heap->insert(session, rec1, NULL);
		CPPUNIT_ASSERT(RID_GET_PAGE(rid) == firstPage + 2);
	}
	// ָ���Ƶ��ڶ�ҳ
	for (int i = 0; i < 4; ++i) {
		CPPUNIT_ASSERT(m_vheap->m_position[i].m_bitmapIdx == 0);
		m_vheap->m_position[i].m_pageIdx = 1;
	}
	// ���µ��ڶ�ҳ
	slotNum = 0;
	while (true) {
		rid1 = RID(firstPage + 2, slotNum);
		canUpdate = m_heap->update(session, rid1, rec2);
		rid2 = m_vheap->getTargetRowId(session, rid1);
		if (RID_GET_PAGE(rid2) == firstPage + 1)
			break;
		slotNum++;
	}
	// ���뵽����ҳ
	while (true) {
		rid3 = m_heap->insert(session, rec1, NULL);
		if (RID_GET_PAGE(rid3) == firstPage + 3)
			break;
	}
	// ɾ����һҳ
	for (int i = 0; i < smallRecPerPage; ++i) {
		rid3 = RID(firstPage, i);
		canDel = m_heap->del(session, rid3);
		CPPUNIT_ASSERT(canDel);
	}
	// ָ���Ƶ���һҳ
	for (int i = 0; i < 4; ++i) {
		CPPUNIT_ASSERT(m_vheap->m_position[i].m_bitmapIdx == 0);
		m_vheap->m_position[i].m_pageIdx = 0;
	}

	rec3->m_rowId = rid1;
	Connie c13(m_db, m_conn, m_heap, m_tblDef, "UPdate it", rec3);
	c13.enableSyncPoint(SP_HEAP_VLR_LOCK3RDPAGE_BEFORE_TRYLOCK);
	c13.start();

	c13.joinSyncPoint(SP_HEAP_VLR_LOCK3RDPAGE_BEFORE_TRYLOCK);

	//������һҳ
	for (int i = 0; i < smallRecPerPage; ++i) {
		rid = m_heap->insert(session, rec1, NULL);
		CPPUNIT_ASSERT(RID_GET_PAGE(rid) == firstPage);
	}

	c13.disableSyncPoint(SP_HEAP_VLR_LOCK3RDPAGE_BEFORE_TRYLOCK);
	c13.notifySyncPoint(SP_HEAP_VLR_LOCK3RDPAGE_BEFORE_TRYLOCK);
	c13.join();


	m_db->getSessionManager()->freeSession(session);
	/****************************/

	freeRecord(rec1);
	freeRecord(rec2);
	freeRecord(rec3);
    }
}




/** 
 * ����open����
 *		���̣�	1. �������޶Ѳ���
 *				2. close��open
 *				3. �Ƚ��ٴ�open��Ķ�״̬
 */
void VLRHeapTestCase::testOpen() {
	Record *rec = createRecord(m_tblDef);
	Session *session = m_db->getSessionManager()->allocSession("VLRHeapTestCase::testOpen", m_conn);
	RowId rid = m_heap->insert(session, rec, NULL);
	u64 maxPage = m_heap->getMaxPageNum();
	u64 maxUsedPage = m_heap->getMaxUsedPageNum();
	u8 flag = m_vheap->getPageFlag(session, RID_GET_PAGE(rid));
	u16 freeSpace = m_vheap->getPageFreeSpace(session, RID_GET_PAGE(rid));
	u16 recOffset = m_vheap->getRecordOffset(session, rid);
	/* �ر� */
	m_heap->close(session, true);
	m_db->getSessionManager()->freeSession(session);
	delete m_heap;
	/* �� */
	session = m_db->getSessionManager()->allocSession("VLRHeapTestCase::testOpen", m_conn);
	m_heap = DrsHeap::open(m_db, session, VLR_HEAP, m_tblDef);
	m_vheap = (VariableLengthRecordHeap *)m_heap;
	/* ��֤ */
	Record record;
	record.m_size = Limits::PAGE_SIZE;
	record.m_format = REC_VARLEN;
	record.m_data = new byte[Limits::PAGE_SIZE];
	bool canRead = m_heap->getRecord(session, rid, &record);
	CPPUNIT_ASSERT(canRead);
	CPPUNIT_ASSERT(record.m_size == rec->m_size);
	CPPUNIT_ASSERT(0 == memcmp(record.m_data, rec->m_data, rec->m_size));
	CPPUNIT_ASSERT(m_heap->getMaxPageNum() == maxPage);
	CPPUNIT_ASSERT(m_heap->getMaxUsedPageNum() == maxUsedPage);
	CPPUNIT_ASSERT(m_vheap->getPageFlag(session, RID_GET_PAGE(rid)) == flag);
	CPPUNIT_ASSERT(m_vheap->getPageFreeSpace(session, RID_GET_PAGE(rid)) == freeSpace);
	CPPUNIT_ASSERT(m_vheap->getRecordOffset(session, rid) == recOffset);

	m_db->getSessionManager()->freeSession(session);
	freeRecord(rec);
    delete [] record.m_data;
}

/** 
 * ���Ա�ɨ�����
 *		���̣�	1. �������ܲ���
 *				2. �����֧���ǲ��ԣ����ݼ��ڲ�����ע�ͣ�
 */
void VLRHeapTestCase::testTableScan() {
	Record *rec1, *rec2;
	RowId rid, rid1, rid2, rid3, rid4;
	Record record1, record2;
	byte data[Limits::PAGE_SIZE * 2];
	record1.m_format = record2.m_format = REC_VARLEN;
	record1.m_data = data;
	record2.m_data = data + Limits::PAGE_SIZE;
	record1.m_size = record2.m_size = Limits::PAGE_SIZE;
	rec1 = createRecordSmall(m_tblDef, 99);
	rec2 = createRecord(m_tblDef, 100);
	int smallRecPerPage = 0;
	u64 pageNum1, pageNum2;
	bool canRead, canDel, canUpdate;
	int count;

	CPPUNIT_ASSERT(m_heap->getStatus().m_dboStats->m_statArr[DBOBJ_SCAN] == 0 && m_heap->getStatus().m_dboStats->m_statArr[DBOBJ_SCAN_ITEM] == 0);

	/* �Ȳ���һ��һ��ҳ�ܲ���ٸ��̼�¼ */
	rid1 = 0;
	Session *session = m_db->getSessionManager()->allocSession("VLRHeapTestCase::testTableScan", m_conn);
	do {
		rid = m_heap->insert(session, rec1, NULL);
		if (!rid1) rid1 = rid;
		if (RID_GET_PAGE(rid) > RID_GET_PAGE(rid1))
			break;
		++smallRecPerPage;
	} while (true);
	m_db->getSessionManager()->freeSession(session);
	cout<<"smallRecPerPage is "<<smallRecPerPage<<endl;

	/* ���� */
	tearDown();
	setUp();
	count = 0;
	/* ����ͷ��ҳ */
	session = m_db->getSessionManager()->allocSession("VLRHeapTestCase::testTableScan", m_conn);
	for (int i = 0; i < smallRecPerPage * 2; ++i) {
		rid = m_heap->insert(session, rec1, NULL);
		++count;
	}
	pageNum2 = RID_GET_PAGE(rid);

	/* update�ڶ�ҳ�ļ�¼������ҳ��ֱ����ҳ������Ҫ����ҳ�� */
	int i = 0;
	do {
		rid = RID(pageNum2, i);
		canUpdate = m_heap->update(session, rid, rec2);
		rid2 = m_vheap->getTargetRowId(session, rid);
		//CPPUNIT_ASSERT(rid2 != rid);
		if (RID_GET_PAGE(rid2) > pageNum2 + 1)
			break;
		++i;
	} while (true);

	/* �Ѹղ�update�Ķ�update���� */
	for (; i >= 0; --i) {
		rid = RID(pageNum2, i);
		rid2 = m_vheap->getTargetRowId(session, rid);
		//CPPUNIT_ASSERT(rid2 != rid);
		canUpdate = m_heap->update(session, rid, rec1);
		CPPUNIT_ASSERT(canUpdate);
		rid2 = m_vheap->getTargetRowId(session, rid);
		CPPUNIT_ASSERT(rid2 == rid);
	}
	/* �ٲ��롢��֤��ҳ */
	for(int i = 0; i < smallRecPerPage; ++i) {
		rid = m_heap->insert(session, rec1, NULL);
		CPPUNIT_ASSERT(RID_GET_PAGE(rid) == pageNum2 + 1);
		++count;
	}
	for(int i = 0; i < smallRecPerPage; ++i) {
		rid = m_heap->insert(session, rec1, NULL);
		CPPUNIT_ASSERT(RID_GET_PAGE(rid) == pageNum2 + 2);
		++count;
	}
	/* ����ֱ�����һҳ */
	pageNum1 = m_heap->getMaxPageNum();
	while (true) {
		rid = m_heap->insert(session, rec1, NULL);
		++count;
		if (RID_GET_PAGE(rid) >= pageNum1)
			break;
	}
	pageNum1 = m_heap->getMaxPageNum();
	/* ����һҳɾ�� */
	for (int i = 0; i < smallRecPerPage; ++i) {
		canDel = m_heap->del(session, RID(pageNum2 - 1, i));
		--count;
		CPPUNIT_ASSERT(canDel);
	}
	/* �ѵڶ�ҳ�������һ����¼ȫ��updateΪshort record */
	for (int i = 0; i < smallRecPerPage - 1; ++i) {
		rid = RID(pageNum2, i);
		canUpdate = m_heap->update(session, rid, rec2);
		CPPUNIT_ASSERT(canUpdate);
		rid2 = m_vheap->getTargetRowId(session, rid);
		//CPPUNIT_ASSERT(rid2 != rid);
	}
	/* ɾ���ڶ�ҳ��һ����¼�����һ���ռ�¼ͷ */
	canDel = m_heap->del(session, RID(pageNum2, 0));
	CPPUNIT_ASSERT(canDel);
	--count;
	CPPUNIT_ASSERT(m_heap->getMaxPageNum() > pageNum1);
	m_db->getSessionManager()->freeSession(session);

	/** ���ˣ�������� */
	SubRecordBuilder compBuilder(m_tblDef, REC_REDUNDANT);
	SubRecord *subRec = compBuilder.createEmptySbById(Limits::PAGE_SIZE, "0 2 3");
	SubRecord *subRec2 = compBuilder.createEmptySbById(Limits::PAGE_SIZE, "0 2 3");

	/* table scan */
	DrsHeapScanHandle *scanhdl;
	/* �ȹ���һ������link targetɨ���scan handle */
	session = m_db->getSessionManager()->allocSession("VLRHeapTestCase::testTableScan", m_conn);
	scanhdl = m_heap->beginScan(session, SubrecExtractor::createInst(session, m_tblDef, subRec), None, NULL, false);
	int next = 0;
	rid3 = rid2 = 0;
	while (m_heap->getNext(scanhdl, subRec)) {
		++next;
		if (rid3) rid2 = rid3;
		rid3 = subRec->m_rowId;
		CPPUNIT_ASSERT(rid3 != rid2);
	}
	CPPUNIT_ASSERT(count == next);
	//cout<<"next is "<<next<<endl;
	//cout<<"count is "<<count<<endl;
	m_heap->endScan(scanhdl);
	m_db->getSessionManager()->freeSession(session);

	CPPUNIT_ASSERT(m_heap->getStatus().m_dboStats->m_statArr[DBOBJ_SCAN] == 1);
	CPPUNIT_ASSERT(m_heap->getStatus().m_dboStats->m_statArr[DBOBJ_SCAN_ITEM] == next);

	/* ����һ����link sourceɨ���scan handle */
	session = m_db->getSessionManager()->allocSession("VLRHeapTestCase::testTableScan", m_conn);
	scanhdl = m_heap->beginScan(session, SubrecExtractor::createInst(session, m_tblDef, subRec), None, NULL, true);

	next = 0;
	rid3 = rid2 = 0;
	while (m_heap->getNext(scanhdl, subRec)) {
		++next;
		if (rid3) rid2 = rid3;
		rid3 = subRec->m_rowId;
		CPPUNIT_ASSERT(rid3 > rid2);
	}
	CPPUNIT_ASSERT(count == next);

	m_heap->endScan(scanhdl);
	m_db->getSessionManager()->freeSession(session);


	/* ����һ����link targetɨ�裬���Ҵ�������scan handle������updateCurrent��deleteCurrent */
	RowLockHandle *rowLockHdl = NULL;	
	session = m_db->getSessionManager()->allocSession("VLRHeapTestCase::testTableScan", m_conn);
	//scanhdl = m_heap->beginScan(session, None, NULL, 0, NULL, true);
	scanhdl = m_heap->beginScan(session, SubrecExtractor::createInst(session, m_tblDef, subRec), Exclusived, &rowLockHdl, true);

	next = 0;
	rid3 = rid2 = 0;
	SubrecExtractor *srExtractor2 = SubrecExtractor::createInst(session, m_tblDef, subRec2);
	SubrecExtractor *srExtractor = SubrecExtractor::createInst(session, m_tblDef, subRec);
	while (m_heap->getNext(scanhdl, subRec)) {
		++next;
		if (rid3) rid2 = rid3;
		rid3 = subRec->m_rowId;
		CPPUNIT_ASSERT(rid3 > rid2);
		CPPUNIT_ASSERT(rowLockHdl != NULL);
		if (rid3 == RID(pageNum2, 1)) { 
			/* ����updateCurrent */
 			canRead = m_heap->getSubRecord(session, rid3, srExtractor2, subRec2);
			CPPUNIT_ASSERT(canRead);
			rid4 = m_vheap->getTargetRowId(session, rid3);
			CPPUNIT_ASSERT(rid4 != rid3);
			RecordOper::extractSubRecordVR(m_tblDef, rec1, subRec2);
			m_heap->updateCurrent(scanhdl, subRec2);
			canRead = m_heap->getSubRecord(session, rid3, srExtractor, subRec);
			//CPPUNIT_ASSERT(0 == memcmp(subRec->m_data, subRec2->m_data, subRec->m_size));
			RecordOper::isSubRecordEq(m_tblDef, subRec, subRec2);
		}
		if (rid3 == RID(pageNum2, 2)) {
			/* ����deleteCurrent */
			canRead = m_heap->getSubRecord(session, rid3, srExtractor2, subRec2);
			CPPUNIT_ASSERT(canRead);
			m_heap->deleteCurrent(scanhdl);
			canRead = m_heap->getSubRecord(session, rid3, srExtractor2, subRec2);
			CPPUNIT_ASSERT(!canRead);
			/* ��¼Ӧ�ü�����һ�����ٱ�ɨ��һ����֤һ�� */
		}
		session->unlockRow(&rowLockHdl);
	}
	CPPUNIT_ASSERT(count == next);

	m_heap->endScan(scanhdl);

	scanhdl = m_heap->beginScan(session, SubrecExtractor::createInst(session, m_tblDef, subRec), None, NULL, true);

	next = 0;
	while (m_heap->getNext(scanhdl, subRec)) {
		++next;
		rid3 = subRec->m_rowId;
	}
	/* ��Ϊ�մ�deleteCurrent��һ����������һ�� */
	CPPUNIT_ASSERT(count == next + 1);

	m_heap->endScan(scanhdl);


	m_db->getSessionManager()->freeSession(session);


	/******** ���¹������� ********/
	tearDown();
	setUp();
	count = 0;
	/* ����ͷ��ҳ */
	session = m_db->getSessionManager()->allocSession("VLRHeapTestCase::testTableScan", m_conn);
	for (int i = 0; i < smallRecPerPage * 2; ++i) {
		rid = m_heap->insert(session, rec1, NULL);
		++count;
	}
	pageNum2 = RID_GET_PAGE(rid);
	/* update�ڶ�ҳ�ļ�¼������ҳ��ֱ����ҳ������Ҫ����ҳ�� */
	i = 0;
	do {
		rid = RID(pageNum2, i);
		canUpdate = m_heap->update(session, rid, rec2);
		rid2 = m_vheap->getTargetRowId(session, rid);
		//CPPUNIT_ASSERT(rid2 != rid);
		if (RID_GET_PAGE(rid2) > pageNum2 + 1)
			break;
		++i;
	} while (true);
	/* �Ѹղ�update�Ķ�update���� */
	for (; i >= 0; --i) {
		rid = RID(pageNum2, i);
		rid2 = m_vheap->getTargetRowId(session, rid);
		//CPPUNIT_ASSERT(rid2 != rid);
		canUpdate = m_heap->update(session, rid, rec1);
		CPPUNIT_ASSERT(canUpdate);
		rid2 = m_vheap->getTargetRowId(session, rid);
		CPPUNIT_ASSERT(rid2 == rid);
	}
	/* �ٲ��롢��֤��ҳ */
	for(int i = 0; i < smallRecPerPage; ++i) {
		rid = m_heap->insert(session, rec1, NULL);
		CPPUNIT_ASSERT(RID_GET_PAGE(rid) == pageNum2 + 1);
		++count;
	}
	for(int i = 0; i < smallRecPerPage; ++i) {
		rid = m_heap->insert(session, rec1, NULL);
		CPPUNIT_ASSERT(RID_GET_PAGE(rid) == pageNum2 + 2);
		++count;
	}
	/* ����ֱ�����һҳ */
	pageNum1 = m_heap->getMaxPageNum();
	while (true) {
		rid = m_heap->insert(session, rec1, NULL);
		++count;
		if (RID_GET_PAGE(rid) >= pageNum1)
			break;
	}
	/* ����һҳɾ�� */
	for (int i = 0; i < smallRecPerPage; ++i) {
		canDel = m_heap->del(session, RID(pageNum2 - 1, i));
		--count;
		CPPUNIT_ASSERT(canDel);
	}
	/* �ѵڶ�ҳ�������һ����¼ȫ��updateΪshort record */
	for (int i = 0; i < smallRecPerPage - 1; ++i) {
		rid = RID(pageNum2, i);
		canUpdate = m_heap->update(session, rid, rec2);
		CPPUNIT_ASSERT(canUpdate);
		rid2 = m_vheap->getTargetRowId(session, rid);
	}
	/* ɾ���ڶ�ҳ��һ����¼�����һ���ռ�¼ͷ */
	canDel = m_heap->del(session, RID(pageNum2, 0));
	CPPUNIT_ASSERT(canDel);
	--count;
	CPPUNIT_ASSERT(m_heap->getMaxPageNum() > pageNum1);
	m_db->getSessionManager()->freeSession(session);

	/** ���ˣ�������� */

	DataVanHelsing vdata1, vdata2;
	vdata1.m_lockMode = Exclusived;
	vdata1.m_returnLinkSrc = true;
	vdata1.m_subRec = subRec;
	vdata2.m_lockMode = Exclusived;
	vdata2.m_returnLinkSrc = true;
	vdata2.m_subRec = subRec2;

	VanHelsing v1(m_db, m_conn, m_heap, m_tblDef, "Table scan 1", &vdata1);
	
	session = m_db->getSessionManager()->allocSession("VLRHeapTestCase::testTableScan", m_conn);
	Connection *conn2 = m_db->getConnection(false);
	Session *session2 = m_db->getSessionManager()->allocSession("VLRHeapTestCase::testTableScan", conn2);
	rowLockHdl = TRY_LOCK_ROW(session, m_tblDef->m_id, RID(pageNum2, 1), Exclusived);
	RowLockHandle *rowLockHdl2 = TRY_LOCK_ROW(session2, m_tblDef->m_id, RID(pageNum2, 2), Exclusived);

	v1.start();
	Thread::msleep(1000);

	canDel = m_heap->del(session, rowLockHdl->getRid());
	CPPUNIT_ASSERT(canDel);
	session->unlockRow(&rowLockHdl);


	Thread::msleep(1000);
	session2->unlockRow(&rowLockHdl2);

	v1.join();

	m_db->getSessionManager()->freeSession(session);
	m_db->getSessionManager()->freeSession(session2);
	m_db->freeConnection(conn2);

	/* �������� */
	freeRecord(rec1);
	freeRecord(rec2);
	/************ end ************/



	/************ ���� getNext ��
				if ((recHdr->m_ifTarget && RID_READ(page->getRecordData(recHdr) - LINK_SIZE) != sourceRid)) {
					// RID���ˣ������´���ǰ��¼
					session->unlockRow(&rowLockHdl);
					goto getNext_get_header;
				}
	************/
	tearDown(); setUp();


	rec1 = createRecordSmall(m_tblDef);
	rec2 = createRecord(m_tblDef);
	session = m_db->getSessionManager()->allocSession("VLRHeapTestCase::testTableScan", m_conn);

	for (int i = 0; i < smallRecPerPage * 2; ++i) {
		m_heap->insert(session, rec1, NULL);
	}

	m_db->getSessionManager()->freeSession(session);

	/* ���ñ�ɨ���߳� */
	vdata2.m_lockMode = Exclusived;
	vdata2.m_returnLinkSrc = false;
	vdata2.m_subRec = subRec;
	VanHelsing v2(m_db, m_conn, m_heap, m_tblDef, "Table scan 2", &vdata2);
	v2.enableSyncPoint(SP_HEAP_VLR_GETNEXT_UNLOCKPAGE_TO_GET_ROWLOCK);

	rid1 = RID(m_vheap->getCBMCount() + 2, 0);
	rid2 = RID(m_vheap->getCBMCount() + 3, 0);

	session = m_db->getSessionManager()->allocSession("VLRHeapTestCase::testTableScan", m_conn);
	m_heap->update(session, rid1, rec2);
	rid3 = m_vheap->getTargetRowId(session, rid1);
	CPPUNIT_ASSERT(rid1 != rid3);
	rowLockHdl = LOCK_ROW(session, m_tblDef->m_id, rid1, Exclusived);

	v2.start();
	v2.joinSyncPoint(SP_HEAP_VLR_GETNEXT_UNLOCKPAGE_TO_GET_ROWLOCK);

	session->unlockRow(&rowLockHdl);

	/* ������¼*/
	m_heap->update(session, rid1, rec1);
	m_heap->update(session, rid2, rec2);
	rid4 = m_vheap->getTargetRowId(session, rid2);
	CPPUNIT_ASSERT(rid3 == rid4);

	v2.disableSyncPoint(SP_HEAP_VLR_GETNEXT_UNLOCKPAGE_TO_GET_ROWLOCK);
	v2.notifySyncPoint(SP_HEAP_VLR_GETNEXT_UNLOCKPAGE_TO_GET_ROWLOCK);
	v2.join();

	m_db->getSessionManager()->freeSession(session);

	freeRecord(rec1);
	freeRecord(rec2);
	/************* end ************/


	/************ ����
				// δ������
				if (lockSecondPage(session, pageNum, RID_GET_PAGE(targetRid), &pageHdl, &targetPageHdl, Shared)) {
					//page = (VLRHeapRecordPageInfo *)pageHdl->getPage();
					//scanHandle->setPage(pageHdl);
					//recHdr = page->getVLRHeader(slotNum);
					if (!recHdr || recHdr->m_ifEmpty || !recHdr->m_ifLink
						|| RID_READ(page->getRecordData(recHdr)) != targetRid) {
							// ��¼�Ѿ�ʧЧ�����Ǽ���ȡ��һ��
							session->releasePage(&targetPageHdl);
							goto getNext_get_header; // slotNum��+1����Ϊ����ɨ�跽ʽ�ǰ�target
					}
				}
	************/
	tearDown(); setUp();

	rec1 = createRecordSmall(m_tblDef);
	rec2 = createRecord(m_tblDef);

	session = m_db->getSessionManager()->allocSession("VLRHeapTestCase::testTableScan", m_conn);
	u64 maxPage = 0;
	/* ���� */
	while (true) {
		rid = m_heap->insert(session, rec1, NULL);
		if (!maxPage) {
			maxPage = m_heap->getMaxPageNum() + 1;
			if (m_vheap->isPageBmp(maxPage)) --maxPage;
		}
		if (RID_GET_PAGE(rid) == maxPage && RID_GET_SLOT(rid) == smallRecPerPage - 1)
			break;
	}
	/* ɾ����һҳ */
	for (int i = 0; i < smallRecPerPage; ++i) {
		rid = RID(m_vheap->getCBMCount() + 2, i);
		canDel = m_heap->del(session, rid);
		CPPUNIT_ASSERT(canDel);
	}
	rid1 = RID(maxPage, 0);
	canUpdate = m_heap->update(session, rid1, rec2);
	rid2 = m_vheap->getTargetRowId(session, rid1);
	PageId pageId1 = RID_GET_PAGE(rid2);
	PageId pageId2 = m_vheap->getCBMCount() + 2;
	CPPUNIT_ASSERT(pageId1 == pageId2);

	m_db->getSessionManager()->freeSession(session);

	
	/* ���ñ�ɨ���߳� */
	vdata2.m_lockMode = None;
	vdata2.m_returnLinkSrc = true;
	vdata2.m_subRec = subRec;
	VanHelsing v3(m_db, m_conn, m_heap, m_tblDef, "Table scan 3", &vdata2);
	v3.enableSyncPoint(SP_HEAP_VLR_GETNEXT_BEFORE_LOCK_SECOND_PAGE);
	v3.enableSyncPoint(SP_HEAP_VLR_LOCKSECONDPAGE_AFTER_RELEASE_FIRST);

	/* ���ø����߳� */
	rid3 = RID(m_vheap->getCBMCount() + 2, smallRecPerPage);
	Dracula d1(m_db, m_conn, m_heap, m_tblDef, "Delete thread 1", &rid3);
	d1.enableSyncPoint(SP_HEAP_VLR_DEL_LOCKED_THE_PAGE);

	/* ��ʼ����v3��ʹ���ڳ��Ի�ȡ����ҳ���ʱ��ס */
	v3.start();
	v3.joinSyncPoint(SP_HEAP_VLR_GETNEXT_BEFORE_LOCK_SECOND_PAGE);

	/* ���и����̣߳�������һ��ҳ�� */
	d1.start();
	d1.joinSyncPoint(SP_HEAP_VLR_DEL_LOCKED_THE_PAGE);

	/* ������v3�������ڳ���������ҳ��ʧ�ܣ��ͷŵ�һҳ�����ס */
	v3.disableSyncPoint(SP_HEAP_VLR_GETNEXT_BEFORE_LOCK_SECOND_PAGE);
	v3.notifySyncPoint(SP_HEAP_VLR_GETNEXT_BEFORE_LOCK_SECOND_PAGE);

	v3.joinSyncPoint(SP_HEAP_VLR_LOCKSECONDPAGE_AFTER_RELEASE_FIRST);


	/* �ø����߳������� */
	d1.disableSyncPoint(SP_HEAP_VLR_DEL_LOCKED_THE_PAGE);
	d1.notifySyncPoint(SP_HEAP_VLR_DEL_LOCKED_THE_PAGE);
	d1.join();

	/* update���� */
	session = m_db->getSessionManager()->allocSession("VLRHeapTestCase::testTableScan", m_conn);
	canUpdate = m_heap->update(session, rid1, rec1); // update�ص�sourceҳ��
	m_db->getSessionManager()->freeSession(session);

	v3.disableSyncPoint(SP_HEAP_VLR_LOCKSECONDPAGE_AFTER_RELEASE_FIRST);
	v3.notifySyncPoint(SP_HEAP_VLR_LOCKSECONDPAGE_AFTER_RELEASE_FIRST);
	v3.join();


	freeRecord(rec1);
	freeRecord(rec2);
	freeSubRecord(subRec);
	freeSubRecord(subRec2);


	/***************************************/
	tearDown(); setUp();

	rec1 = createRecordSmall(m_tblDef);
	rec2 = createRecord(m_tblDef);

	session = m_db->getSessionManager()->allocSession("VLRHeapTestCase::testTableScan", m_conn);
	u64 lastbmpn = 0;
	/* ���� */
	count = 0;
	next = 0;
	while (true) {
		rid = m_heap->insert(session, rec2, NULL);
		++count;
		if (!lastbmpn) {
			lastbmpn = m_vheap->getMaxBmpNum() + (BitmapPage::CAPACITY + 1);
		}
		if (RID_GET_PAGE(rid) > lastbmpn + 1)
			break;
	}

	subRec = compBuilder.createEmptySbById(Limits::PAGE_SIZE, "0 2 3");
	scanhdl = m_heap->beginScan(session, SubrecExtractor::createInst(session, m_tblDef, subRec), None, NULL, false);
	while (m_heap->getNext(scanhdl, subRec)) {
		++next;
	}
	CPPUNIT_ASSERT(count == next);
	m_heap->endScan(scanhdl);

	m_db->getSessionManager()->freeSession(session);

	freeRecord(rec1);
	freeRecord(rec2);
	freeSubRecord(subRec);

}

/** 
 * ����redoCreate����
 *		���̣�	1. ����redoCreate�ĸ�������
 *				2. ����redoCreate
 *				3. �鿴redo�Ľ���Ƿ���ͬԤ��
 */
void VLRHeapTestCase::testRedoCreate() {
	/* clone tabel definition. */
	TableDef *tableDef;
	tableDef = new TableDef(m_tblDef);

	/* delete heap */
	Session *session = m_db->getSessionManager()->allocSession("VLRHeapTestCase::testRedoCreate", m_conn);
	m_heap->close(session, true);
	m_db->getSessionManager()->freeSession(session);
	delete m_heap;
	m_heap = NULL;

	File f(VLR_HEAP);
	bool exist;
	f.isExist(&exist);
	CPPUNIT_ASSERT(exist);
	u64 errCode = f.remove();
	f.isExist(&exist);
	CPPUNIT_ASSERT(!exist);

	/* redoCreate */
	session = m_db->getSessionManager()->allocSession("VLRHeapTestCase::testRedoCreate", m_conn);
	DrsHeap::redoCreate(m_db, session, VLR_HEAP, tableDef);
	m_db->getSessionManager()->freeSession(session);

	session = m_db->getSessionManager()->allocSession("VLRHeapTestCase::testRedoCreate", m_conn);
	m_heap = DrsHeap::open(m_db, session, VLR_HEAP, tableDef);
	m_vheap = (VariableLengthRecordHeap *)m_heap;
	m_db->getSessionManager()->freeSession(session);

	CPPUNIT_ASSERT(DrsHeap::getVersionFromTableDef(tableDef) == HEAP_VERSION_VLR);


	// �ļ���ʹ����
	session = m_db->getSessionManager()->allocSession("VLRHeapTestCase::testRedoCreate", m_conn);
	try {
		DrsHeap::redoCreate(m_db, session, VLR_HEAP, tableDef);
	} catch (NtseException &) {
		CPPUNIT_ASSERT(true);
	}
	m_db->getSessionManager()->freeSession(session);

	/* ���ļ����Ȳ�����redoCreate */
	session = m_db->getSessionManager()->allocSession("VLRHeapTestCase::testRedoCreate", m_conn);
	m_heap->close(session, true);
	delete m_heap;
	m_heap = NULL;

	DrsHeap::redoCreate(m_db, session, VLR_HEAP, tableDef);
	m_heap = DrsHeap::open(m_db, session, VLR_HEAP, tableDef);
	m_db->getSessionManager()->freeSession(session);

	/* ����redo�Ĳ��� */
	Record *rec = createRecordSmall(tableDef);
	session = m_db->getSessionManager()->allocSession("VLRHeapTestCase::testRedoCreate", m_conn);
	m_heap->insert(session, rec, NULL);
	m_heap->close(session, true);
	delete m_heap;
	m_heap = NULL;
	m_db->getSessionManager()->freeSession(session);

	session = m_db->getSessionManager()->allocSession("VLRHeapTestCase::testRedoCreate", m_conn);
	DrsHeap::redoCreate(m_db, session, VLR_HEAP, tableDef);
	m_db->getSessionManager()->freeSession(session);

	delete tableDef;
	freeRecord(rec);
}

/** 
 * ����redoInsert����
 *		���̣�	1. ����redoInsert�ĸ��ֳ�ʼ����
 *				2. ����redoInsert
 *				3. �鿴redo�Ľ���Ƿ���ͬԤ��
 *				4. ���������ο��ڲ�ע��
 */
void VLRHeapTestCase::testRedoInsert() {
	Record *rec;
	byte buf[Limits::PAGE_SIZE * 2];
	Session *session;
	u64 rid;
	Record readOut;
	byte *readBuf = buf + Limits::PAGE_SIZE;
	readOut.m_data = readBuf;
	readOut.m_format = REC_VARLEN;
	readOut.m_size = Limits::PAGE_SIZE;
	bool canRead, canDel;

	/* ��ʼ���� */
	m_heap->getBuffer()->flushAll();
	backupHeapFile(m_heap->getHeapFile(), VLR_BACK_01);

	vector<RowId> ridVec;

	/* ����log�������� */
	rec = createRecordSmall(m_tblDef);
	//int rows = (Limits::PAGE_SIZE / rec->m_size) * DrsHeap::HEAP_INCREASE_SIZE + 1;//1000;//100;
	int rows = (Limits::PAGE_SIZE / rec->m_size) * m_tblDef->m_incrSize + 1;
	session = m_db->getSessionManager()->allocSession("VLRHeapTestCase::testRedoInsert", m_conn);
	for (int i = 0; i < rows; ++i) {
		rid = m_heap->insert(session, rec, NULL);
		ridVec.push_back(rid);
	}
	m_db->getSessionManager()->freeSession(session);

	// �ر����ݿ��ٴ�
	session = m_db->getSessionManager()->allocSession("VLRHeapTestCase::testRedoInsert", m_conn);
	m_heap->close(session, true);
	delete m_heap;
	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(m_conn);
	/*m_db->close();
	delete m_db;
	m_db = Database::open(&m_config, 1);*/
	m_conn = m_db->getConnection(true);
	session = m_db->getSessionManager()->allocSession("VLRHeapTestCase::testRedoInsert", m_conn);
	m_heap = DrsHeap::open(m_db, session, VLR_HEAP, m_tblDef);
	m_vheap = (VariableLengthRecordHeap *)m_heap;
	m_db->getSessionManager()->freeSession(session);


	// ���ڼ��log
	LogScanHandle *logHdl = m_db->getTxnlog()->beginScan(Txnlog::MIN_LSN, Txnlog::MAX_LSN);
	int j = 0;
	session = m_db->getSessionManager()->allocSession("VLRHeapTestCase::testRedoInsert", m_conn);
	while (m_db->getTxnlog()->getNext(logHdl)) {
		if (logHdl->logEntry()->m_logType == LOG_HEAP_INSERT) {
			//m_heap->getRecordFromInsertlog((LogEntry *)logHdl->logEntry(), &readOut);
			j++;
		}
	}
	/* Ӧ�õ���rows */
	CPPUNIT_ASSERT(j == rows);
	m_db->getSessionManager()->freeSession(session);
	m_db->getTxnlog()->endScan(logHdl);


	/* ��������һЩ������� */

	/* backup file */
	session = m_db->getSessionManager()->allocSession("VLRHeapTestCase::testRedoInsert", m_conn);
	m_heap->syncMataPages(session);
	m_db->getSessionManager()->freeSession(session);
	m_heap->getBuffer()->flushAll();
	backupHeapFile(m_heap->getHeapFile(), VLR_BACK_02);
	
	/* �ر����ݿ��ٴ� */
	session = m_db->getSessionManager()->allocSession("VLRHeapTestCase::testRedoInsert", m_conn);
	m_heap->close(session, true);
	delete m_heap;
	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(m_conn);
	/*m_db->close();
	delete m_db;
	m_db = Database::open(&m_config, 1);*/
	m_conn = m_db->getConnection(true);
	/* �ó�ʼ�������ָ� */
	restoreHeapFile(VLR_BACK_01, VLR_HEAP);
	session = m_db->getSessionManager()->allocSession("VLRHeapTestCase::testRedoInsert", m_conn);
	m_heap = DrsHeap::open(m_db, session, VLR_HEAP, m_tblDef);
	m_vheap = (VariableLengthRecordHeap *)m_heap;
	/* ����Ӧ���������ݶ�ȡ���� */
	for (vector<RowId>::iterator i = ridVec.begin(); i != ridVec.end(); ++i) {
		canRead = m_heap->getRecord(session, *i, &readOut);
		CPPUNIT_ASSERT(!canRead);
	}

	m_db->getSessionManager()->freeSession(session);

	Record parseOut(INVALID_ROW_ID, REC_VARLEN, NULL, Limits::PAGE_SIZE);
	// ���ڼ��log
	logHdl = m_db->getTxnlog()->beginScan(Txnlog::MIN_LSN, Txnlog::MAX_LSN);
	session = m_db->getSessionManager()->allocSession("VLRHeapTestCase::testRedoInsert", m_conn);
	while (m_db->getTxnlog()->getNext(logHdl)) {
		if (logHdl->logEntry()->m_logType == LOG_HEAP_INSERT) {
			//u64 lsn = logHdl->curLsn();
			m_heap->getRecordFromInsertlog((LogEntry *)logHdl->logEntry(), &parseOut);
			u64 gotrid = DrsHeap::getRowIdFromInsLog(logHdl->logEntry());
			CPPUNIT_ASSERT(gotrid);
			m_heap->redoInsert(session, logHdl->curLsn(), logHdl->logEntry()->m_data, (uint)(logHdl->logEntry()->m_size), &readOut);
		}
	}
	m_db->getSessionManager()->freeSession(session);
	m_db->getTxnlog()->endScan(logHdl);

	/* ��֤�ļ� */
	session = m_db->getSessionManager()->allocSession("VLRHeapTestCase::testRedoInsert", m_conn);
	for (vector<RowId>::iterator i = ridVec.begin(); i != ridVec.end(); ++i) {
		canRead = m_heap->getRecord(session, *i, &readOut);
		CPPUNIT_ASSERT(canRead);
		CPPUNIT_ASSERT(readOut.m_size == rec->m_size);
		CPPUNIT_ASSERT(0 == memcmp(readOut.m_data, rec->m_data, readOut.m_size));
	}
	m_db->getSessionManager()->freeSession(session);

	/* �ؽ�һ��central bitmap */
	session = m_db->getSessionManager()->allocSession("VLRHeapTestCase::testRedoInsert", m_conn);
	//m_vheap->redoCentralBitmap(session);
	m_heap->redoFinish(session);
	m_db->getSessionManager()->freeSession(session);

	session = m_db->getSessionManager()->allocSession("VLRHeapTestCase::testRedoInsert", m_conn);
	m_heap->syncMataPages(session);
	m_db->getSessionManager()->freeSession(session);
	m_heap->getBuffer()->flushAll();

	File back02(VLR_BACK_02);
	back02.open(true);
	int cmp = compareFile(m_heap->getHeapFile(), &back02, 1);
	//cout<<"Different in "<<cmp<<" page"<<endl;
	CPPUNIT_ASSERT(0 == cmp);


	freeRecord(rec);

	/* ɾ�������ļ� */
	File b01(VLR_BACK_01);
	b01.remove();
	File b02(VLR_BACK_02);
	b02.remove();


	/****** ���²���Ϊ����insertIntoSlot����������defrag�ķ�֧ ******/
	tearDown();
	setUp();
	
	Record *rec1 = createRecordSmall(m_tblDef);
	session = m_db->getSessionManager()->allocSession("VLRHeapTestCase::testRedoInsert", m_conn);
	int smallRecPerPage = 0;
	while (true) {
		rid = m_heap->insert(session, rec1, NULL);
		if (RID_GET_PAGE(rid) != m_vheap->getCBMCount() + 1 + 1) break;
		smallRecPerPage++;
	}
	m_db->getSessionManager()->freeSession(session);
	//cout<<"Small rec per page is "<<smallRecPerPage<<endl;

	/* ok, reset */
	tearDown();
	setUp();

	/* ��ʼ���� */
	m_heap->getBuffer()->flushAll();
	backupHeapFile(m_heap->getHeapFile(), VLR_BACK_03);

	Record *rec2 = createRecordLong(m_tblDef);
	int big2small = (rec2->m_size / rec1->m_size > (uint)smallRecPerPage / 2) ? smallRecPerPage / 2 : rec2->m_size / rec1->m_size;
	//cout<<"big2small is "<<big2small<<endl;

	session = m_db->getSessionManager()->allocSession("VLRHeapTestCase::testRedoInsert", m_conn);
	for (int i = 0; i < smallRecPerPage; ++i) {
		rid = m_heap->insert(session, rec1, NULL);
	}
	for (int i = 0; i < big2small; ++i) {
		canDel = m_heap->del(session, RID(RID_GET_PAGE(rid), i));
		CPPUNIT_ASSERT(canDel);
	}

	for (int i = 0; i <big2small; ++i) {
		rid = m_heap->insert(session, rec1, NULL);
		CPPUNIT_ASSERT(m_vheap->getCBMCount() + 2 == RID_GET_PAGE(rid));
	}
	for (int i = 0; i < big2small; ++i) {
		canDel = m_heap->del(session, RID(RID_GET_PAGE(rid), smallRecPerPage - 1 - i));
		CPPUNIT_ASSERT(canDel);
	}
	for (int i = 0; i <big2small; ++i) {
		rid = m_heap->insert(session, rec1, NULL);
		CPPUNIT_ASSERT(m_vheap->getCBMCount() + 2 == RID_GET_PAGE(rid));
	}

	m_db->getSessionManager()->freeSession(session);

	/* ���� */
	session = m_db->getSessionManager()->allocSession("VLRHeapTestCase::testRedoInsert", m_conn);
	m_heap->syncMataPages(session);
	m_db->getSessionManager()->freeSession(session);
	m_heap->getBuffer()->flushAll();
	backupHeapFile(m_heap->getHeapFile(), VLR_BACK_04);


	/* �ر����ݿ��ٴ� */
	session = m_db->getSessionManager()->allocSession("VLRHeapTestCase::testRedoInsert", m_conn);
	m_heap->close(session, true);
	delete m_heap;
	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(m_conn);
	/*m_db->close();
	delete m_db;
	m_db = Database::open(&m_config, 1);*/
	m_conn = m_db->getConnection(true);
	/* �ó�ʼ�������ָ� */
	restoreHeapFile(VLR_BACK_03, VLR_HEAP);
	session = m_db->getSessionManager()->allocSession("VLRHeapTestCase::testRedoInsert", m_conn);
	m_heap = DrsHeap::open(m_db, session, VLR_HEAP, m_tblDef);
	m_vheap = (VariableLengthRecordHeap *)m_heap;
	m_db->getSessionManager()->freeSession(session);

	// redo����֤�ļ�һ��
	logHdl = m_db->getTxnlog()->beginScan(Txnlog::MIN_LSN, Txnlog::MAX_LSN);
	int dtime = 0;
	int itime = 0;
	session = m_db->getSessionManager()->allocSession("VLRHeapTestCase::testRedoInsert", m_conn);
	while (m_db->getTxnlog()->getNext(logHdl)) {
		switch (logHdl->logEntry()->m_logType) {
		case LOG_HEAP_INSERT:
			++itime;
			m_heap->redoInsert(session, logHdl->curLsn(), logHdl->logEntry()->m_data, (uint)(logHdl->logEntry()->m_size), &readOut);
			break;
		case LOG_HEAP_DELETE:
			++dtime;
			m_heap->redoDelete(session, logHdl->curLsn(), logHdl->logEntry()->m_data, (uint)(logHdl->logEntry()->m_size));
			break;
		}
	}
	m_db->getSessionManager()->freeSession(session);
	m_db->getTxnlog()->endScan(logHdl);

	/* �ؽ�һ��central bitmap */
	session = m_db->getSessionManager()->allocSession("VLRHeapTestCase::testRedoInsert", m_conn);
	//m_vheap->redoCentralBitmap(session);
	m_heap->redoFinish(session);
	m_db->getSessionManager()->freeSession(session);

	session = m_db->getSessionManager()->allocSession("VLRHeapTestCase::testRedoInsert", m_conn);
	m_heap->syncMataPages(session);
	m_db->getSessionManager()->freeSession(session);
	m_heap->getBuffer()->flushAll();

	/* �Ƚ��ļ� */
	File back04(VLR_BACK_04);
	back04.open(true);
	cmp = compareFile(m_heap->getHeapFile(), &back04, 1);
	//cout<<"Different in "<<cmp<<" page"<<endl;
	CPPUNIT_ASSERT(0 == cmp);


	/* ɾ�������ļ� */
	File b03(VLR_BACK_03);
	b03.remove();
	File b04(VLR_BACK_04);
	b04.remove();

	/* finally */
	freeRecord(rec1);
	freeRecord(rec2);

#if 0
//#if NTSE_PAGE_SIZE == 1024 //������Զ��ڴ�ҳ����˵̫���ˣ����Կ�����Ϊ�Ժ��Ż�����
	/******* ���� redo �ۺϣ��Լ���ҳ�Ļָ� ********/
	tearDown(); setUp();

	Record *recs[3];
	recs[0] = createRecordSmall(m_tblDef);
	recs[1] = createRecord(m_tblDef);
	recs[2] = createRecordLong(m_tblDef);

	/* �����ѣ�Ȼ�����ɾһЩ1/10��Ȼ���������һЩ3/10, Ȼ��ɾһЩ1/10 */
	/* �����Ѻ� ����ʼ���� */
	SubRecordBuilder compBuilder(m_tblDef, REC_REDUNDANT);
	SubRecord *subRec[3];
	for (int i = 0; i < 3; ++i) {
		subRec[i] = compBuilder.createEmptySbByName(Limits::PAGE_SIZE, "Permalink TrackbackUrl Title Abstract");
		RecordOper::extractSubRecordVR(m_tblDef, recs[i], subRec[i]);
	}

	ridVec.clear();
	session = m_db->getSessionManager()->allocSession("VLRHeapTestCase::testRedoInsert", m_conn);
	u64 lastBmpNum = 0;
	while (true) {
		rid = m_heap->insert(session, recs[randomInt(3)], NULL);
		ridVec.push_back(rid);
		if (!lastBmpNum) lastBmpNum = m_vheap->getMaxBmpNum();
		if (m_vheap->getMaxBmpNum() > lastBmpNum)
			break;
	}
	//cout<<ridVec.size()<<" records inserted"<<endl;
	m_db->getSessionManager()->freeSession(session);

	/* �����ļ� */
	backupHeapFile(m_heap->getHeapFile(), VLR_BACK_05);

	vector<SubRecord *> subRecVec;

	/* �������� */

	session = m_db->getSessionManager()->allocSession("VLRHeapTestCase::testRedoInsert", m_conn);
	int onetenth = (int)(ridVec.size() / 10);
	for (int i = 0; i < onetenth; ++i) {
		int ranI = randomInt(int(ridVec.size()));
		if (!ridVec[ranI]) {
			for (uint j = 0; j < ridVec.size(); ++j) {
				if (ridVec[j] != 0) {
					ranI = j;
					break;
				}
			}
		}
		canDel = m_heap->del(session, ridVec[ranI]);
		CPPUNIT_ASSERT(canDel);
		ridVec[ranI] = 0;
	}
	m_db->getSessionManager()->freeSession(session);

	session = m_db->getSessionManager()->allocSession("VLRHeapTestCase::testRedoInsert", m_conn);

	for (int i = 0; i < onetenth * 3; ++i) {
		int ranI = randomInt((int)ridVec.size());
		if (!ridVec[ranI]) {
			for (uint j = 0; j < ridVec.size(); ++j) {
				if (ridVec[j] != 0) {
					ranI = j;
					break;
				}
			}
		}
		SubRecord *sr = subRec[randomInt(3)];
		bool canUpdate = m_heap->update(session, ridVec[ranI], sr);
		CPPUNIT_ASSERT(canUpdate);
		subRecVec.push_back(sr);
	}
	m_db->getSessionManager()->freeSession(session);


	session = m_db->getSessionManager()->allocSession("VLRHeapTestCase::testRedoInsert", m_conn);
	for (int i = 0; i < onetenth; ++i) {
		int ranI = randomInt((int)ridVec.size());
		if (!ridVec[ranI]) {
			for (uint j = 0; j < ridVec.size(); ++j) {
				if (ridVec[j] != 0) {
					ranI = j;
					break;
				}
			}
		}
		canDel = m_heap->del(session, ridVec[ranI]);
		CPPUNIT_ASSERT(canDel);
		ridVec[ranI] = 0;
	}
	m_db->getSessionManager()->freeSession(session);


	/* ���ݶ� */
	session = m_db->getSessionManager()->allocSession("VLRHeapTestCase::testRedoInsert", m_conn);
	m_heap->flush(session);
	m_db->getSessionManager()->freeSession(session);
	m_heap->getBuffer()->flushAll();
	backupHeapFile(m_heap->getHeapFile(), VLR_BACK_06);

	/* �ر����ݿ��ٴ� */
	session = m_db->getSessionManager()->allocSession("VLRHeapTestCase::testRedoInsert", m_conn);
	m_heap->close(session, true);
	delete m_heap;
	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(m_conn);
	m_db->close();
	delete m_db;
	m_db = Database::open(&m_config, 1);
	m_conn = m_db->getConnection(true);
	/* �ó�ʼ�������ָ� */
	restoreHeapFile(VLR_BACK_05, VLR_HEAP);
	session = m_db->getSessionManager()->allocSession("VLRHeapTestCase::testRedoInsert", m_conn);
	m_heap = DrsHeap::open(m_db, session, VLR_HEAP);
	m_vheap = (VariableLengthRecordHeap *)m_heap;
	m_db->getSessionManager()->freeSession(session);

	/* redo */
	logHdl = m_db->getTxnlog()->beginScan(Txnlog::MIN_LSN, Txnlog::MAX_LSN);
	session = m_db->getSessionManager()->allocSession("VLRHeapTestCase::testRedoInsert", m_conn);
	int subRecIdx = 0;
	itime = 0;
	dtime = 0;
	int utime = 0;
	while (m_db->getTxnlog()->getNext(logHdl)) {
		switch (logHdl->logEntry()->m_logType) {
		case LOG_HEAP_INSERT:
			++itime;
			m_heap->redoInsert(session, logHdl->curLsn(), logHdl->logEntry()->m_data, (uint)(logHdl->logEntry()->m_size), &readOut);
			break;
		case LOG_HEAP_DELETE:
			++dtime;
			m_heap->redoDelete(session, logHdl->curLsn(), logHdl->logEntry()->m_data, (uint)(logHdl->logEntry()->m_size));
			break;
		case LOG_HEAP_UPDATE:
			++utime;
			m_heap->redoUpdate(session, logHdl->curLsn(), logHdl->logEntry()->m_data, (uint)(logHdl->logEntry()->m_size), subRecVec[subRecIdx++]);
			break;
		}
	}
	m_db->getSessionManager()->freeSession(session);
	m_db->getTxnlog()->endScan(logHdl);

	/* �ؽ�һ��central bitmap */
	session = m_db->getSessionManager()->allocSession("VLRHeapTestCase::testRedoInsert", m_conn);
	m_heap->redoFinish(session);
	m_db->getSessionManager()->freeSession(session);

	session = m_db->getSessionManager()->allocSession("VLRHeapTestCase::testRedoInsert", m_conn);
	m_heap->flush(session);
	m_db->getSessionManager()->freeSession(session);
	m_heap->getBuffer()->flushAll();

	/* �Ƚ��ļ� */
	File back06(VLR_BACK_06);
	back06.open(true);
	int diffIdx = 24;
	cmp = compareFile(m_heap->getHeapFile(), &back06, 0, &diffIdx);
	//cout<<"Different in "<<cmp<<" page"<<endl;
	//cout<<"Different in "<<diffIdx<<" byte"<<endl;
	CPPUNIT_ASSERT(0 == cmp);

	/* ɾ�������ļ� */
	File b05(VLR_BACK_05);
	b05.remove();
	File b06(VLR_BACK_06);
	b06.remove();



	freeRecord(recs[0]);
	freeRecord(recs[1]);
	freeRecord(recs[2]);
	freeSubRecord(subRec[0]);
	freeSubRecord(subRec[1]);
	freeSubRecord(subRec[2]);
#endif

}

/** 
 * ����redoDelete����
 *		���̣�	1. ����redoDelete�ĸ��ֳ�ʼ����
 *				2. ����redoDelete
 *				3. �鿴redo�Ľ���Ƿ���ͬԤ��
 *				4. ���������ο��ڲ�ע��
 */
void VLRHeapTestCase::testRedoDelete() {
	Record *rec1, *rec2;
	byte buf[Limits::PAGE_SIZE * 2];
	Session *session;
	RowId rid, rid1, rid2, rid3, rid4;
	Record readOut;
	byte *readBuf = buf + Limits::PAGE_SIZE;
	readOut.m_data = readBuf;
	readOut.m_format = REC_VARLEN;
	readOut.m_size = Limits::PAGE_SIZE;
	bool canRead, canDel, canUpdate;

	rec1 = createRecordSmall(m_tblDef);
	rec2 = createRecord(m_tblDef);

	//int rows = (Limits::PAGE_SIZE / rec1->m_size) * DrsHeap::HEAP_INCREASE_SIZE + 1;
	int rows = (Limits::PAGE_SIZE / rec1->m_size) * m_tblDef->m_incrSize + 1;
	//cout<<"rows is "<<rows<<endl;
	session = m_db->getSessionManager()->allocSession("VLRHeapTestCase::testRedoDelete", m_conn);
	for (int i = 0; i < rows; ++i) {
		rid = m_heap->insert(session, rec1, NULL);
	}
	rid1 = rid;
	m_heap->update(session, rid1, rec2);
	rid2 = RID(m_vheap->getCBMCount() + 1 + 1, 0);
	m_heap->update(session, rid2, rec2);
	m_db->getSessionManager()->freeSession(session);

	/* ���� */
	session = m_db->getSessionManager()->allocSession("VLRHeapTestCase::testRedoDelete", m_conn);
	m_heap->syncMataPages(session);
	m_db->getSessionManager()->freeSession(session);
	m_heap->getBuffer()->flushAll();
	backupHeapFile(m_heap->getHeapFile(), VLR_BACK_01);

	/* �����Ѿ��м���log */
	LogScanHandle *logHdl = m_db->getTxnlog()->beginScan(Txnlog::MIN_LSN, Txnlog::MAX_LSN);
	session = m_db->getSessionManager()->allocSession("VLRHeapTestCase::testRedoDelete", m_conn);
	int j = 0;
	while (m_db->getTxnlog()->getNext(logHdl)) {
		j++;
	}
	//cout<<"Before delete there are "<<j<<" logs."<<endl;
	m_db->getSessionManager()->freeSession(session);
	m_db->getTxnlog()->endScan(logHdl);


	/* ɾ��������¼ */
	session = m_db->getSessionManager()->allocSession("VLRHeapTestCase::testRedoDelete", m_conn);
	canDel = m_heap->del(session, rid1);
	CPPUNIT_ASSERT(canDel);
	canDel = m_heap->del(session, rid2);
	CPPUNIT_ASSERT(canDel);
	rid3 = RID(m_vheap->getCBMCount() + 1 + 1 + 1, 0);
	canDel = m_heap->del(session, rid3);
	CPPUNIT_ASSERT(canDel);
	
	canRead = m_heap->getRecord(session, rid1, &readOut);
	CPPUNIT_ASSERT(!canRead);
	canRead = m_heap->getRecord(session, rid2, &readOut);
	CPPUNIT_ASSERT(!canRead);
	canRead = m_heap->getRecord(session, rid3, &readOut);
	CPPUNIT_ASSERT(!canRead);
	
	m_db->getSessionManager()->freeSession(session);


	/* �����Ѿ��м���log */
	m_heap->getBuffer()->flushAll();
	logHdl = m_db->getTxnlog()->beginScan(Txnlog::MIN_LSN, Txnlog::MAX_LSN);
	session = m_db->getSessionManager()->allocSession("VLRHeapTestCase::testRedoDelete", m_conn);
	int k = 0;
	while (m_db->getTxnlog()->getNext(logHdl)) {
		k++;
	}
	//cout<<"After delete there are "<<k<<" logs."<<endl;
	m_db->getSessionManager()->freeSession(session);
	m_db->getTxnlog()->endScan(logHdl);
	CPPUNIT_ASSERT(k == j + 3);


	/* ���� */
	session = m_db->getSessionManager()->allocSession("VLRHeapTestCase::testRedoDelete", m_conn);
	m_heap->syncMataPages(session);
	m_db->getSessionManager()->freeSession(session);
	m_heap->getBuffer()->flushAll();
	backupHeapFile(m_heap->getHeapFile(), VLR_BACK_02);
	
	/* �ر��ٴ� */
	session = m_db->getSessionManager()->allocSession("VLRHeapTestCase::testRedoDelete", m_conn);
	m_heap->close(session, true);
	delete m_heap;
	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(m_conn);
	/*m_db->close();
	delete m_db;
	m_db = Database::open(&m_config, 1);*/
	m_conn = m_db->getConnection(true);
	/* �ó�ʼ�������ָ� */
	restoreHeapFile(VLR_BACK_01, VLR_HEAP);
	session = m_db->getSessionManager()->allocSession("VLRHeapTestCase::testRedoDelete", m_conn);
	m_heap = DrsHeap::open(m_db, session, VLR_HEAP, m_tblDef);
	m_vheap = (VariableLengthRecordHeap *)m_heap;
	m_db->getSessionManager()->freeSession(session);

	/* ȷ��ʹ�ûָ��� */
	session = m_db->getSessionManager()->allocSession("VLRHeapTestCase::testRedoDelete", m_conn);
	canRead = m_heap->getRecord(session, rid1, &readOut);
	CPPUNIT_ASSERT(canRead);
	canRead = m_heap->getRecord(session, rid2, &readOut);
	CPPUNIT_ASSERT(canRead);
	canRead = m_heap->getRecord(session, rid3, &readOut);
	CPPUNIT_ASSERT(canRead);
	m_db->getSessionManager()->freeSession(session);


	/* redo delete */
	logHdl = m_db->getTxnlog()->beginScan(Txnlog::MIN_LSN, Txnlog::MAX_LSN);
	session = m_db->getSessionManager()->allocSession("VLRHeapTestCase::testRedoDelete", m_conn);
	while (m_db->getTxnlog()->getNext(logHdl)) {
		u64 lsn = logHdl->curLsn();
		if (logHdl->logEntry()->m_logType == LOG_HEAP_DELETE) {
			m_heap->redoDelete(session, lsn, logHdl->logEntry()->m_data, (uint)logHdl->logEntry()->m_size);
			j++;
		}
	}
	m_db->getSessionManager()->freeSession(session);
	m_db->getTxnlog()->endScan(logHdl);

 	/* �ؽ�һ��central bitmap */
	session = m_db->getSessionManager()->allocSession("VLRHeapTestCase::testRedoDelete", m_conn);
	m_heap->redoFinish(session);
	m_db->getSessionManager()->freeSession(session);


	session = m_db->getSessionManager()->allocSession("VLRHeapTestCase::testRedoDelete", m_conn);
	m_heap->syncMataPages(session);
	m_db->getSessionManager()->freeSession(session);
	m_heap->getBuffer()->flushAll();

	File back02(VLR_BACK_02);
	back02.open(true);
	int cmp = compareFile(m_heap->getHeapFile(), &back02, 1);
	//cout<<"page "<<cmp - 1<<" is different."<<endl;
	CPPUNIT_ASSERT(0 == cmp);


	/*** ���첻һ�µİ��Ʒҳ���ٽ���redo���ԣ�֮һ�� ***/
	tearDown();
	setUp();

	u64 pageNum = m_vheap->getCBMCount() + 1 + 1;
	session = m_db->getSessionManager()->allocSession("VLRHeapTestCase::testRedoDelete", m_conn);
	while (true) {
		rid = m_heap->insert(session, rec1, NULL);
		if (RID_GET_PAGE(rid) != pageNum) break;
	}
	m_db->getSessionManager()->freeSession(session);
	rid1 = RID(pageNum, 0);

	session = m_db->getSessionManager()->allocSession("VLRHeapTestCase::testRedoDelete", m_conn);
	canUpdate = m_heap->update(session, rid1, rec2);
	CPPUNIT_ASSERT(canUpdate);

	rid2 = m_vheap->getTargetRowId(session, rid1);
	CPPUNIT_ASSERT(RID_GET_PAGE(rid2) == pageNum + 1);
	m_db->getSessionManager()->freeSession(session);

	/* ����pageNumҳ */
	m_heap->getBuffer()->flushAll();
	session = m_db->getSessionManager()->allocSession("VLRHeapTestCase::testRedoDelete", m_conn);
	backupPage(session, m_heap, pageNum, buf);
	m_db->getSessionManager()->freeSession(session);

	/* delete֮ */
	session = m_db->getSessionManager()->allocSession("VLRHeapTestCase::testRedoDelete", m_conn);
	canDel = m_heap->del(session, rid1);
	m_db->getSessionManager()->freeSession(session);
	CPPUNIT_ASSERT(canDel);
	/* ȷ��delete */
	session = m_db->getSessionManager()->allocSession("VLRHeapTestCase::testRedoDelete", m_conn);
	rid3 = m_vheap->getTargetRowId(session, rid2);
	CPPUNIT_ASSERT(rid3 == 0);
	m_db->getSessionManager()->freeSession(session);

	/* �ظ�pageNumҳ��Ҳ���ǻָ�srcҳ */
	session = m_db->getSessionManager()->allocSession("VLRHeapTestCase::testRedoDelete", m_conn);
	restorePage(session, m_heap, pageNum, buf);
	m_db->getSessionManager()->freeSession(session);
	/* ȷ�ϻָ� */
	session = m_db->getSessionManager()->allocSession("VLRHeapTestCase::testRedoDelete", m_conn);
	rid4 = m_vheap->getTargetRowId(session, rid1);
	CPPUNIT_ASSERT(rid4 == rid2);
	m_db->getSessionManager()->freeSession(session);

	/* redo */
	m_heap->getBuffer()->flushAll();
	logHdl = m_db->getTxnlog()->beginScan(Txnlog::MIN_LSN, Txnlog::MAX_LSN);
	session = m_db->getSessionManager()->allocSession("VLRHeapTestCase::testRedoDelete", m_conn);
	while (m_db->getTxnlog()->getNext(logHdl)) {
		u64 lsn = logHdl->curLsn();
		if (logHdl->logEntry()->m_logType == LOG_HEAP_DELETE) {
			m_heap->redoDelete(session, lsn, logHdl->logEntry()->m_data, (uint)logHdl->logEntry()->m_size);
		}
	}
	m_db->getSessionManager()->freeSession(session);
	m_db->getTxnlog()->endScan(logHdl);

	/* ȷ��redo�ɹ� */
	session = m_db->getSessionManager()->allocSession("VLRHeapTestCase::testRedoDelete", m_conn);
	rid4 = m_vheap->getTargetRowId(session, rid1);
	CPPUNIT_ASSERT(rid4 == 0);
	m_db->getSessionManager()->freeSession(session);



	/*** ���첻һ�µİ��Ʒҳ���ٽ���redo���ԣ�֮���� ***/
	tearDown();
	setUp();

	session = m_db->getSessionManager()->allocSession("VLRHeapTestCase::testRedoDelete", m_conn);
	while (true) {
		rid = m_heap->insert(session, rec1, NULL);
		if (RID_GET_PAGE(rid) != pageNum) break;
	}
	m_db->getSessionManager()->freeSession(session);
	rid1 = RID(pageNum, 0);

	session = m_db->getSessionManager()->allocSession("VLRHeapTestCase::testRedoDelete", m_conn);
	canUpdate = m_heap->update(session, rid1, rec2);
	CPPUNIT_ASSERT(canUpdate);

	rid2 = m_vheap->getTargetRowId(session, rid1);
	CPPUNIT_ASSERT(RID_GET_PAGE(rid2) == pageNum + 1);
	m_db->getSessionManager()->freeSession(session);

	/* ����pageNum + 1ҳ��λͼҳ */
	m_heap->getBuffer()->flushAll();
	session = m_db->getSessionManager()->allocSession("VLRHeapTestCase::testRedoDelete", m_conn);
	backupPage(session, m_heap, pageNum + 1, buf);
	backupPage(session, m_heap, m_vheap->getCBMCount() + 1, buf + Limits::PAGE_SIZE);
	m_db->getSessionManager()->freeSession(session);

	/* delete֮ */
	session = m_db->getSessionManager()->allocSession("VLRHeapTestCase::testRedoDelete", m_conn);
	canDel = m_heap->del(session, rid1);
	m_db->getSessionManager()->freeSession(session);
	CPPUNIT_ASSERT(canDel);
	/* ȷ��delete */
	session = m_db->getSessionManager()->allocSession("VLRHeapTestCase::testRedoDelete", m_conn);
	rid3 = m_vheap->getTargetRowId(session, rid2);
	CPPUNIT_ASSERT(rid3 == 0);
	m_db->getSessionManager()->freeSession(session);

	/* �ظ�λͼҳ��pageNum+1ҳ��Ҳ���ǻָ�targetҳ */
	session = m_db->getSessionManager()->allocSession("VLRHeapTestCase::testRedoDelete", m_conn);
	restorePage(session, m_heap, m_vheap->getCBMCount() + 1, buf + Limits::PAGE_SIZE);
	restorePage(session, m_heap, pageNum + 1, buf);
	m_db->getSessionManager()->freeSession(session);
	/* ȷ�ϻָ� */
	session = m_db->getSessionManager()->allocSession("VLRHeapTestCase::testRedoDelete", m_conn);
	rid4 = m_vheap->getTargetRowId(session, rid2);
	CPPUNIT_ASSERT(rid4 == rid1);
	u64 lsn = m_heap->getPageLSN(session, m_vheap->getCBMCount() + 1, m_heap->getDBObjStats());
	CPPUNIT_ASSERT(lsn < m_heap->getPageLSN(session, pageNum, m_heap->getDBObjStats()));
	m_db->getSessionManager()->freeSession(session);


	/* redo */
	m_heap->getBuffer()->flushAll();
	logHdl = m_db->getTxnlog()->beginScan(Txnlog::MIN_LSN, Txnlog::MAX_LSN);
	session = m_db->getSessionManager()->allocSession("VLRHeapTestCase::testRedoDelete", m_conn);
	while (m_db->getTxnlog()->getNext(logHdl)) {
		u64 lsn = logHdl->curLsn();
		if (logHdl->logEntry()->m_logType == LOG_HEAP_DELETE) {
			m_heap->redoDelete(session, lsn, logHdl->logEntry()->m_data, (uint)logHdl->logEntry()->m_size);
		}
	}
	m_db->getSessionManager()->freeSession(session);
	m_db->getTxnlog()->endScan(logHdl);

	/* ȷ��redo�ɹ� */
	session = m_db->getSessionManager()->allocSession("VLRHeapTestCase::testRedoDelete", m_conn);
	rid4 = m_vheap->getTargetRowId(session, rid2);
	CPPUNIT_ASSERT(rid4 == 0);
	lsn = m_heap->getPageLSN(session, m_vheap->getCBMCount() + 1, m_heap->getDBObjStats());
	m_db->getSessionManager()->freeSession(session);



	/***** ����updateBitmapд���Сlsn����� *****/
	tearDown();
	setUp();

	session = m_db->getSessionManager()->allocSession("VLRHeapTestCase::testRedoDelete", m_conn);
	int smallRecPerPage = 0;
	while (true) {
		rid = m_heap->insert(session, rec1, NULL);
		if (RID_GET_PAGE(rid) != m_vheap->getCBMCount() + 1 + 1) break;
		smallRecPerPage++;
	}
	int smallRecDecTo10 = 0;
	while (true) {
		rid = RID(m_vheap->getCBMCount() + 1 + 1, smallRecDecTo10);
		m_heap->del(session, rid);
		++smallRecDecTo10;
		if (m_vheap->getPageFlag(session, RID_GET_PAGE(rid)) != 0) break;
	}
	//cout<<"smallRecDecTo10 is "<<smallRecDecTo10<<endl;
	m_db->getSessionManager()->freeSession(session);

	/* ���� */
	tearDown();
	setUp();
	/* ������ҳ */
	session = m_db->getSessionManager()->allocSession("VLRHeapTestCase::testRedoDelete", m_conn);
	for (int i = 0; i < smallRecPerPage * 2; ++i) {
		rid = m_heap->insert(session, rec1, NULL);
	}
	
	/* ÿҳ��ɾ����һ���flag */
	for (int i = 0; i < smallRecDecTo10 - 1; ++i) {
		canDel = m_heap->del(session, RID(m_vheap->getCBMCount() + 2, i));
		CPPUNIT_ASSERT(canDel);
		canDel = m_heap->del(session, RID(m_vheap->getCBMCount() + 3, i));
		CPPUNIT_ASSERT(canDel);
	}
	//cout<<"page flag is "<<(int)m_vheap->getPageFlag(session, m_vheap->getCBMCount() + 2)<<endl;
	//cout<<"page flag is "<<(int)m_vheap->getPageFlag(session, m_vheap->getCBMCount() + 3)<<endl;
	CPPUNIT_ASSERT(m_vheap->getPageFlag(session, m_vheap->getCBMCount() + 2) == 0);
	CPPUNIT_ASSERT(m_vheap->getPageFlag(session, m_vheap->getCBMCount() + 3) == 0);

	m_db->getSessionManager()->freeSession(session);
	


	freeRecord(rec1);
	freeRecord(rec2);

	/* ɾ�������ļ� */
	File b01(VLR_BACK_01);
	b01.remove();
	File b02(VLR_BACK_02);
	b02.remove();


	/************* ����
		if (tagBitmapModified && !sameBitmap) {
			redoVerifyBitmap(session, tagPgN, tagPage->m_pageBitmapFlag, logLSN);
		}
	************/
	tearDown(); setUp();


	int tinyRecPerPage = 0;
	rid1 = 0;
	rec1 = createTinyRawRecord(102);
	session = m_db->getSessionManager()->allocSession("VLRHeapTestCase::testRedoDelete", m_conn);
	do {
		rid = m_heap->insert(session, rec1, NULL);
		if (!rid1) rid1 = rid;
		if (RID_GET_PAGE(rid) > RID_GET_PAGE(rid1))
			break;
		++tinyRecPerPage;
	} while (true);
	//cout<<"tinyRecPerPage is "<<tinyRecPerPage<<endl;
	u8 flag = m_vheap->getPageFlag(session, RID_GET_PAGE(rid1));
	CPPUNIT_ASSERT(flag == 0x0);
	int tinyRecDec;
	for (int i = 1; i <= tinyRecPerPage; ++i) {
		canDel = m_heap->del(session, RID(RID_GET_PAGE(rid1), i - 1));
		CPPUNIT_ASSERT(canDel);
		if (m_vheap->getPageFlag(session, RID_GET_PAGE(rid1)) != flag) {
			tinyRecDec = i;
			break;
		}
	}
	m_db->getSessionManager()->freeSession(session);

	/** ����֪�������� **/
	tearDown();
	setUp();
	/* ������һҳ */
	session = m_db->getSessionManager()->allocSession("VLRHeapTestCase::testRedoDelete", m_conn);
	for (int i = 0; i < tinyRecPerPage; ++i) {
		rid3 = m_heap->insert(session, rec1, NULL);
	}
	/* ����tinyRecDec����long record��update */
	//rec2 = createLongRawRecord(103);
	rec2 = createRecordLong(m_tblDef, 103);
	rid1 = RID(RID_GET_PAGE(rid3), tinyRecDec - 1);
	canUpdate = m_heap->update(session, rid1, rec2);
	CPPUNIT_ASSERT(canUpdate);
	rid2 = m_vheap->getTargetRowId(session, rid1);
	CPPUNIT_ASSERT(rid2 != rid1);
	m_db->getSessionManager()->freeSession(session);

	/* ���� */
	session = m_db->getSessionManager()->allocSession("VLRHeapTestCase::testRedoDelete", m_conn);
	m_heap->syncMataPages(session);
	m_db->getSessionManager()->freeSession(session);
	m_heap->getBuffer()->flushAll();
	backupHeapFile(m_heap->getHeapFile(), VLR_BACK_03);


	session = m_db->getSessionManager()->allocSession("VLRHeapTestCase::testRedoDelete", m_conn);
	/* ɾ��tinyRecDec - 1�� */
	for (int i = 0; i < tinyRecDec - 1; ++i) {
		canDel = m_heap->del(session, RID(RID_GET_PAGE(rid3), i));
		CPPUNIT_ASSERT(canDel);
	}
	flag = m_vheap->getPageFlag(session, RID_GET_PAGE(rid3));
	CPPUNIT_ASSERT(flag == 0);
	/* ����ɾ��������ӣ�����λͼ����仯�� */
	canDel = m_heap->del(session, rid1);
	CPPUNIT_ASSERT(canDel);
	CPPUNIT_ASSERT(flag != m_vheap->getPageFlag(session, RID_GET_PAGE(rid3)));

	m_db->getSessionManager()->freeSession(session);


	/* ���� */
	session = m_db->getSessionManager()->allocSession("VLRHeapTestCase::testRedoDelete", m_conn);
	m_heap->syncMataPages(session);
	m_db->getSessionManager()->freeSession(session);
	m_heap->getBuffer()->flushAll();
	backupHeapFile(m_heap->getHeapFile(), VLR_BACK_04);

	/* �ر��ٴ� */
	session = m_db->getSessionManager()->allocSession("VLRHeapTestCase::testRedoDelete", m_conn);
	m_heap->close(session, true);
	delete m_heap;
	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(m_conn);
	/*m_db->close();
	delete m_db;
	//Config cfg;
	m_db = Database::open(&m_config, 1);*/
	m_conn = m_db->getConnection(true);
	/* �ó�ʼ�������ָ� */
	restoreHeapFile(VLR_BACK_03, VLR_HEAP);
	session = m_db->getSessionManager()->allocSession("VLRHeapTestCase::testRedoDelete", m_conn);
	m_heap = DrsHeap::open(m_db, session, VLR_HEAP, m_tblDef);
	m_vheap = (VariableLengthRecordHeap *)m_heap;
	m_db->getSessionManager()->freeSession(session);

	/* redo */
	m_heap->getBuffer()->flushAll();
	logHdl = m_db->getTxnlog()->beginScan(Txnlog::MIN_LSN, Txnlog::MAX_LSN);
	session = m_db->getSessionManager()->allocSession("VLRHeapTestCase::testRedoDelete", m_conn);
	while (m_db->getTxnlog()->getNext(logHdl)) {
		u64 lsn = logHdl->curLsn();
		if (logHdl->logEntry()->m_logType == LOG_HEAP_DELETE) {
			m_heap->redoDelete(session, lsn, logHdl->logEntry()->m_data, (uint)logHdl->logEntry()->m_size);
		}
	}
	m_db->getSessionManager()->freeSession(session);
	m_db->getTxnlog()->endScan(logHdl);

	
	/* �ؽ�һ��central bitmap */
	session = m_db->getSessionManager()->allocSession("VLRHeapTestCase::testRedoDelete", m_conn);
	m_heap->redoFinish(session);
	m_db->getSessionManager()->freeSession(session);

	session = m_db->getSessionManager()->allocSession("VLRHeapTestCase::testRedoDelete", m_conn);
	m_heap->syncMataPages(session);
	m_db->getSessionManager()->freeSession(session);
	m_heap->getBuffer()->flushAll();

	File back04(VLR_BACK_04);
	back04.open(true);
	cmp = compareFile(m_heap->getHeapFile(), &back04, 1);
	//cout<<"page "<<cmp - 1<<" is different."<<endl;
	CPPUNIT_ASSERT(0 == cmp);
	back04.close();


	/* ɾ�������ļ� */
	File b03(VLR_BACK_03);
	b03.remove();
	File b04(VLR_BACK_04);
	b04.remove();

        freeRecord(rec1);
        freeRecord(rec2);

}

/** 
 * ����redoUpdate����
 *		���̣�	1. ����redoUpdate�ĸ��ֳ�ʼ����
 *				2. ����redoUpdate
 *				3. �鿴redo�Ľ���Ƿ���ͬԤ��
 *				4. ���������ο��ڲ�ע��
 */
void VLRHeapTestCase::testRedoUpdate() {
	m_db->setCheckpointEnabled(false);
	Record *rec1, *rec2, *rec3;
	byte buf[Limits::PAGE_SIZE * 2], recBuf[Limits::PAGE_SIZE * 3];
	Session *session;
	RowId rid;
	Record readOut, record1, record2, record3;
	byte *readBuf = buf + Limits::PAGE_SIZE;
	readOut.m_data = readBuf;
	readOut.m_format = REC_VARLEN;
	readOut.m_size = Limits::PAGE_SIZE;

	record1.m_format = record2.m_format = record3.m_format = REC_VARLEN;
	record1.m_size = record2.m_size = record3.m_size = Limits::PAGE_SIZE;
	record1.m_data = recBuf; record2.m_data = recBuf + Limits::PAGE_SIZE; record3.m_data = recBuf + Limits::PAGE_SIZE * 2;

	u16 len1, len2, len3;

	bool canRead, canUpdate;

	rec1 = createRecordSmall(m_tblDef);
	rec2 = createRecord(m_tblDef);
	rec3 = createRecordLong(m_tblDef);

	u64 lsn;


	len1 = rec1->m_size;


	SubRecordBuilder compBuilder(m_tblDef, REC_REDUNDANT);
	SubRecord *subRec1 = compBuilder.createEmptySbByName(Limits::PAGE_SIZE, "Permalink TrackbackUrl Title Abstract");
	SubRecord *subRec2 = compBuilder.createEmptySbByName(Limits::PAGE_SIZE, "Permalink TrackbackUrl Title Abstract");
	SubRecord *subRec3 = compBuilder.createEmptySbByName(Limits::PAGE_SIZE, "Permalink TrackbackUrl Title Abstract");

	
	RecordOper::extractSubRecordVR(m_tblDef, rec2, subRec2);
	RecordOper::extractSubRecordVR(m_tblDef, rec3, subRec3);

	len2 = record2.m_size = RecordOper::getUpdateSizeVR(m_tblDef, rec1, subRec2);
	RecordOper::updateRecordVR(m_tblDef, rec1, subRec2, record2.m_data);

	len3 = record3.m_size = RecordOper::getUpdateSizeVR(m_tblDef, &record2, subRec3);
	RecordOper::updateRecordVR(m_tblDef, &record2, subRec3, record3.m_data);


	//int rows = (Limits::PAGE_SIZE / rec1->m_size) * DrsHeap::HEAP_INCREASE_SIZE + 1;
	int rows = (Limits::PAGE_SIZE / rec1->m_size) * m_tblDef->m_incrSize + 1;
	vector<RowId> ridVec;
	session = m_db->getSessionManager()->allocSession("VLRHeapTestCase::testRedoUpdate", m_conn);
	for (int i = 0; i < rows; ++i) {
		rid = m_heap->insert(session, rec1, NULL);
		canRead = m_heap->getRecord(session, rid, &readOut);
		CPPUNIT_ASSERT(canRead && readOut.m_size == len1);
		ridVec.push_back(rid);
	}
	//cout<<"Number of items is "<<ridVec.size()<<endl;
	m_db->getSessionManager()->freeSession(session);

	LogScanHandle *logHdl;
	int j;

	/* ���� */
	session = m_db->getSessionManager()->allocSession("VLRHeapTestCase::testRedoUpdate", m_conn);
	m_heap->syncMataPages(session);
	m_db->getSessionManager()->freeSession(session);
	m_heap->getBuffer()->flushAll();
	backupHeapFile(m_heap->getHeapFile(), VLR_BACK_01);


	vector<RowId>::iterator iter;

	/* С��¼���³��м�¼ */
	session = m_db->getSessionManager()->allocSession("VLRHeapTestCase::testRedoUpdate", m_conn);
	for (iter = ridVec.begin(); iter != ridVec.end(); ++iter) {
		canUpdate = m_heap->update(session, *iter, subRec2);
		canRead = m_heap->getRecord(session, *iter, &readOut);
		CPPUNIT_ASSERT(canUpdate && canRead && readOut.m_size == len2);
		CPPUNIT_ASSERT(canUpdate);
	}
	m_db->getSessionManager()->freeSession(session);	


	/* ���� */
	session = m_db->getSessionManager()->allocSession("VLRHeapTestCase::testRedoUpdate", m_conn);
	m_heap->syncMataPages(session);
	m_db->getSessionManager()->freeSession(session);
	m_heap->getBuffer()->flushAll();
	backupHeapFile(m_heap->getHeapFile(), VLR_BACK_03);


	/* �м�¼���³ɳ���¼ */
	session = m_db->getSessionManager()->allocSession("VLRHeapTestCase::testRedoUpdate", m_conn);
	for (iter = ridVec.begin(); iter != ridVec.end(); ++iter) {
		canUpdate = m_heap->update(session, *iter, subRec3);
		canRead = m_heap->getRecord(session, *iter, &readOut);
		CPPUNIT_ASSERT(canUpdate && canRead && readOut.m_size == len3);
	}
	m_db->getSessionManager()->freeSession(session);	


	/* ���� */
	session = m_db->getSessionManager()->allocSession("VLRHeapTestCase::testRedoUpdate", m_conn);
	m_heap->syncMataPages(session);
	m_db->getSessionManager()->freeSession(session);
	m_heap->getBuffer()->flushAll();
	backupHeapFile(m_heap->getHeapFile(), VLR_BACK_02);


	/* �ر��ٴ� */
	session = m_db->getSessionManager()->allocSession("VLRHeapTestCase::testRedoUpdate", m_conn);
	m_heap->close(session, true);
	delete m_heap;
	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(m_conn);
	/*m_db->close();
	delete m_db;*/
	m_heap = NULL;
	//m_db = NULL;
	m_conn = NULL;

	//m_db = Database::open(&m_config, 1);
	m_conn = m_db->getConnection(true);

	/* �ó�ʼ�������ָ� */
	restoreHeapFile(VLR_BACK_01, VLR_HEAP);
	session = m_db->getSessionManager()->allocSession("VLRHeapTestCase::testRedoUpdate", m_conn);
	m_heap = DrsHeap::open(m_db, session, VLR_HEAP, m_tblDef);
	m_vheap = (VariableLengthRecordHeap *)m_heap;
	m_db->getSessionManager()->freeSession(session);

	/* ȷ�ϱ��ݻָ��� */
	session = m_db->getSessionManager()->allocSession("VLRHeapTestCase::testRedoUpdate", m_conn);
	for (iter = ridVec.begin(); iter != ridVec.end(); ++iter) {
		CPPUNIT_ASSERT(m_vheap->getTargetRowId(session, *iter) == *iter);
	}
	m_db->getSessionManager()->freeSession(session);	


	/* ��ʼredo */
	m_heap->getBuffer()->flushAll();
	logHdl = m_db->getTxnlog()->beginScan(Txnlog::MIN_LSN, Txnlog::MAX_LSN);
	session = m_db->getSessionManager()->allocSession("VLRHeapTestCase::testRedoUpdate", m_conn);
	for (j = 0; j < rows; ++j) {
		m_db->getTxnlog()->getNext(logHdl);
		LogScanHandle *lsh;
		while (logHdl->logEntry()->m_logType != LOG_HEAP_INSERT)
			lsh = m_db->getTxnlog()->getNext(logHdl);
		CPPUNIT_ASSERT(logHdl->logEntry()->m_logType == LOG_HEAP_INSERT);
	}
	for (j = 0, iter = ridVec.begin(); j < rows; ++j, ++iter) {
		m_db->getTxnlog()->getNext(logHdl);
		lsn = logHdl->curLsn();
		CPPUNIT_ASSERT(logHdl->logEntry()->m_logType == LOG_HEAP_UPDATE);

		canRead = m_heap->getRecord(session, *iter, &readOut);
		CPPUNIT_ASSERT(readOut.m_size == len1);
		m_heap->redoUpdate(session, lsn, logHdl->logEntry()->m_data, (uint)logHdl->logEntry()->m_size, subRec2);
		canRead = m_heap->getRecord(session, *iter, &readOut);
		CPPUNIT_ASSERT(readOut.m_size == len2);
	}
	for (j = 0, iter = ridVec.begin(); j < rows; ++j, ++iter) {
		m_db->getTxnlog()->getNext(logHdl);
		lsn = logHdl->curLsn();
		CPPUNIT_ASSERT(logHdl->logEntry()->m_logType == LOG_HEAP_UPDATE);

		canRead = m_heap->getRecord(session, *iter, &readOut);
		CPPUNIT_ASSERT(readOut.m_size == len2);
		m_heap->redoUpdate(session, lsn, logHdl->logEntry()->m_data, (uint)logHdl->logEntry()->m_size, subRec3);
		canRead = m_heap->getRecord(session, *iter, &readOut);
		CPPUNIT_ASSERT(readOut.m_size == len3);
	}
	CPPUNIT_ASSERT(!m_db->getTxnlog()->getNext(logHdl));
	//cout<<"Before restore there are "<<j<<" logs."<<endl;
	m_db->getSessionManager()->freeSession(session);
	m_db->getTxnlog()->endScan(logHdl);

	/* �ؽ�һ��central bitmap */
	session = m_db->getSessionManager()->allocSession("VLRHeapTestCase::testRedoUpdate", m_conn);
	m_heap->redoFinish(session);
	m_db->getSessionManager()->freeSession(session);


	session = m_db->getSessionManager()->allocSession("VLRHeapTestCase::testRedoUpdate", m_conn);
	m_heap->syncMataPages(session);
	m_db->getSessionManager()->freeSession(session);
	m_heap->getBuffer()->flushAll();

	File back02(VLR_BACK_02);
	back02.open(true);
	int cmp = compareFile(m_heap->getHeapFile(), &back02, 1);
	//cout<<"Different in "<<cmp<<" page"<<endl;
	CPPUNIT_ASSERT(0 == cmp);

	/* ɾ�������ļ� */
	File b01(VLR_BACK_01);
	b01.remove();
	File b02(VLR_BACK_02);
	b02.remove();
	File b03(VLR_BACK_03);
	b03.remove();

	m_db->setCheckpointEnabled(true);

	/*************** end ***************/


	/************* ����
			} else if (!updateInOldTag) {
				assert(hasRecord);
				//doSrcLocalUpdate = true;
				srcPage->updateLocalRecord(&rec, srcSlot);
				srcFlag = getRecordPageFlag(srcPage->m_pageBitmapFlag, srcPage->m_freeSpaceSize, false);
				srcPage->m_pageBitmapFlag = srcFlag;
			} else if (updateInOldTag) { // ���µ����أ�����δȡ��
	***************/
	tearDown(); setUp();

	u64 firstPage = m_vheap->getCBMCount() + 2;
	session = m_db->getSessionManager()->allocSession("VLRHeapTestCase::testRedoUpdate", m_conn);
	RowId rid1 = m_heap->insert(session, rec1, NULL);
	int smallRecPerPage = 0;
	while (true) {
		rid = m_heap->insert(session, rec1, NULL);
		++smallRecPerPage;
		if (RID_GET_PAGE(rid) != firstPage) break;
	}
	/* ���� */
	u64 maxPage = m_heap->getMaxPageNum();
	while (true) {
		rid = m_heap->insert(session, rec1, NULL);
		if (RID_GET_PAGE(rid) == maxPage && RID_GET_SLOT(rid) == smallRecPerPage - 1)
			break;
	}
	//cout<<"smallRecPerPage is "<<smallRecPerPage<<endl;

	/* ɾ�����һҳ */
	for (int i = 0; i < smallRecPerPage; ++i) {
		rid = RID(maxPage, i);
		bool canDel = m_heap->del(session, rid);
		CPPUNIT_ASSERT(canDel);
	}
	canUpdate = m_heap->update(session, rid1, rec3);
	RowId rid2 = m_vheap->getTargetRowId(session, rid1);
	CPPUNIT_ASSERT(RID_GET_PAGE(rid2) == maxPage);
	m_db->getSessionManager()->freeSession(session);


	/* ��ʼ���� */
	session = m_db->getSessionManager()->allocSession("VLRHeapTestCase::testRedoUpdate", m_conn);
	m_heap->syncMataPages(session);
	m_db->getSessionManager()->freeSession(session);
	m_heap->getBuffer()->flushAll();
	backupHeapFile(m_heap->getHeapFile(), VLR_BACK_04);


	session = m_db->getSessionManager()->allocSession("VLRHeapTestCase::testRedoUpdate", m_conn);
	/* ɾ���һҳ��rid1֮�� */
	for (int i = 0; i < smallRecPerPage; ++i) {
		rid = RID(firstPage, i);
		if (rid != rid1) m_heap->del(session, rid);
	}
	/* ���һҳ����ֱ��flag�ı䣬������Ҫ�弸�� */
	u8 flag;
	flag = m_vheap->getPageFlag(session, firstPage);
	int flagChange = 0;
	while (true) {
		rid = m_heap->insert(session, rec1, NULL);
		if (RID_GET_PAGE(rid) == firstPage) {
			++flagChange;
			if (m_vheap->getPageFlag(session, firstPage) != flag)
				break;
		}
	}
	m_db->getSessionManager()->freeSession(session);

	session = m_db->getSessionManager()->allocSession("VLRHeapTestCase::testRedoUpdate", m_conn);
	//cout<<"flagChange is "<<flagChange<<endl;
	/* ɾ���һҳ��rid1֮�� */
	for (int i = 0; i < smallRecPerPage; ++i) {
		rid = RID(firstPage, i);
		if (rid != rid1) m_heap->del(session, rid);
	}
	/* ���뵽��һ����¼�ͱ�flag */
	for (int i = 0; i < flagChange - 1;) {
		rid = m_heap->insert(session, rec1, NULL);
		if (RID_GET_PAGE(rid) == firstPage) ++i;
	}
	CPPUNIT_ASSERT(m_vheap->getPageFlag(session, firstPage) == flag);

	u8 flagFirst, flagMax;
	flagFirst = m_vheap->getPageFlag(session, firstPage);
	flagMax = m_vheap->getPageFlag(session, maxPage);
	
	m_db->getSessionManager()->freeSession(session);

	/* ȡ��subRec2 */
	RecordOper::extractSubRecordVR(m_tblDef, rec2, subRec2);
	/* ȡ��subRec3 */
	RecordOper::extractSubRecordVR(m_tblDef, rec3, subRec3);

	session = m_db->getSessionManager()->allocSession("VLRHeapTestCase::testRedoUpdate", m_conn);
	m_heap->update(session, rid1, rec2);
	CPPUNIT_ASSERT(flagFirst != m_vheap->getPageFlag(session, firstPage));
	CPPUNIT_ASSERT(flagMax != m_vheap->getPageFlag(session, maxPage));
	m_db->getSessionManager()->freeSession(session);


	/* �ٴα��� */
	session = m_db->getSessionManager()->allocSession("VLRHeapTestCase::testRedoUpdate", m_conn);
	m_heap->syncMataPages(session);
	m_db->getSessionManager()->freeSession(session);
	m_heap->getBuffer()->flushAll();
	backupHeapFile(m_heap->getHeapFile(), VLR_BACK_05);


	/* �ر��ٴ� */
	session = m_db->getSessionManager()->allocSession("VLRHeapTestCase::testRedoUpdate", m_conn);
	m_heap->close(session, true);
	delete m_heap;
	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(m_conn);
	/*m_db->close();
	delete m_db;*/
	m_heap = NULL;
	//m_db = NULL;
	m_conn = NULL;
	//Config cfg;
	//m_db = Database::open(&m_config, 1);
	m_conn = m_db->getConnection(true);

	/* �ó�ʼ�������ָ� */
	restoreHeapFile(VLR_BACK_04, VLR_HEAP);
	session = m_db->getSessionManager()->allocSession("VLRHeapTestCase::testRedoUpdate", m_conn);
	m_heap = DrsHeap::open(m_db, session, VLR_HEAP, m_tblDef);
	m_vheap = (VariableLengthRecordHeap *)m_heap;
	m_db->getSessionManager()->freeSession(session);

	/* redo */
	m_heap->getBuffer()->flushAll();
	logHdl = m_db->getTxnlog()->beginScan(Txnlog::MIN_LSN, Txnlog::MAX_LSN);
	session = m_db->getSessionManager()->allocSession("VLRHeapTestCase::testRedoUpdate", m_conn);
	int i = 0;
	while (m_db->getTxnlog()->getNext(logHdl)) {
		u64 lsn = logHdl->curLsn();
		switch (logHdl->logEntry()->m_logType) {
		case LOG_HEAP_DELETE:
			m_heap->redoDelete(session, lsn, logHdl->logEntry()->m_data, (uint)logHdl->logEntry()->m_size);
			break;
		case LOG_HEAP_UPDATE:
			m_heap->redoUpdate(session, lsn, logHdl->logEntry()->m_data, (uint)logHdl->logEntry()->m_size, subRec2);
			break;
		case LOG_HEAP_INSERT:
			m_heap->redoInsert(session, lsn, logHdl->logEntry()->m_data, (uint)logHdl->logEntry()->m_size, &record1);
			break;
		}
		++i;
	}
	m_db->getSessionManager()->freeSession(session);
	m_db->getTxnlog()->endScan(logHdl);

	
	/* �ؽ�һ��central bitmap */
	session = m_db->getSessionManager()->allocSession("VLRHeapTestCase::testRedoUpdate", m_conn);
	m_heap->redoFinish(session);
	m_db->getSessionManager()->freeSession(session);

	session = m_db->getSessionManager()->allocSession("VLRHeapTestCase::testRedoUpdate", m_conn);
	m_heap->syncMataPages(session);
	m_db->getSessionManager()->freeSession(session);
	m_heap->getBuffer()->flushAll();

	/* �Ƚ��ļ� */
	File back05(VLR_BACK_05);
	back05.open(true);
	static const int numFiles = 2;
	File *files[numFiles];
	PageType pageType[numFiles];
	int numF = m_heap->getFiles(files, pageType, numFiles);
	CPPUNIT_ASSERT(numF == 1);
	cmp = compareFile(files[0], &back05, 1);
	//cout<<"page "<<cmp - 1<<" is different."<<endl;
	CPPUNIT_ASSERT(0 == cmp);

	File b04(VLR_BACK_04);
	b04.remove();
	File b05(VLR_BACK_05);
	b05.remove();

	/************ end ***********/


	freeRecord(rec1);
	freeRecord(rec2);
	freeRecord(rec3);
	freeSubRecord(subRec1);
	freeSubRecord(subRec2);
	freeSubRecord(subRec3);

	
}

/** 
 * ���Լ�¼����
 * ����һ���ڲ����ԣ�����������Ĳ�����
 */
void VLRHeapTestCase::testRecord() {
	cout<<"MAX_VLR_LENGTH is "<<VariableLengthRecordHeap::MAX_VLR_LENGTH<<endl;

	Record *rec1;

	rec1 = createRecordSmall(m_tblDef);
	Record record1;
	byte buf[Limits::PAGE_SIZE];
	record1.m_data = buf;
	record1.m_size = Limits::PAGE_SIZE;
	record1.m_format = REC_VARLEN;

	VLRHeapHeaderPageInfo *header = (VLRHeapHeaderPageInfo *)m_heap->getHeaderPage();

	Session *session;

	session = m_db->getSessionManager()->allocSession("VLRHeapTestCase::testRecord", m_conn);
	m_heap->syncMataPages(session);
	m_db->getSessionManager()->freeSession(session);
	m_heap->getBuffer()->flushAll();
	backupHeapFile(m_heap->getHeapFile(), VLR_BACK_01, m_heap->getBuffer());


	/* ���� */
	session = m_db->getSessionManager()->allocSession("VLRHeapTestCase::testRecord", m_conn);
	m_heap->insert(session, rec1, NULL);
	m_db->getSessionManager()->freeSession(session);

	/* �ٴα��� */
	session = m_db->getSessionManager()->allocSession("VLRHeapTestCase::testRecord", m_conn);
	m_heap->syncMataPages(session);
	m_db->getSessionManager()->freeSession(session);
	m_heap->getBuffer()->flushAll();
	backupHeapFile(m_heap->getHeapFile(), VLR_BACK_02);

	/* �ر��ٴ� */
	session = m_db->getSessionManager()->allocSession("VLRHeapTestCase::testRecord", m_conn);
	m_heap->close(session, true);
	delete m_heap;
	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(m_conn);
	/*m_db->close();
	delete m_db;*/
	m_heap = NULL;
	//m_db = NULL;
	m_conn = NULL;
	//Config cfg;
	//m_db = Database::open(&m_config, 1);
	m_conn = m_db->getConnection(true);

	/* �ó�ʼ�������ָ� */
	restoreHeapFile(VLR_BACK_01, VLR_HEAP);
	session = m_db->getSessionManager()->allocSession("VLRHeapTestCase::testRecord", m_conn);
	m_heap = DrsHeap::open(m_db, session, VLR_HEAP, m_tblDef);
	m_vheap = (VariableLengthRecordHeap *)m_heap;
	header = (VLRHeapHeaderPageInfo *)m_heap->getHeaderPage();
	m_db->getSessionManager()->freeSession(session);

	/* redo */
	//m_heap->getBuffer()->flushAll();
	LogScanHandle *logHdl = m_db->getTxnlog()->beginScan(Txnlog::MIN_LSN, Txnlog::MAX_LSN);
	session = m_db->getSessionManager()->allocSession("VLRHeapTestCase::testRecord", m_conn);
	while (m_db->getTxnlog()->getNext(logHdl)) {
		u64 lsn = logHdl->curLsn();
		switch (logHdl->logEntry()->m_logType) {
		case LOG_HEAP_INSERT:
			m_heap->redoInsert(session, lsn, logHdl->logEntry()->m_data, (uint)logHdl->logEntry()->m_size, &record1);
			break;
		}
	}
	m_db->getSessionManager()->freeSession(session);
	m_db->getTxnlog()->endScan(logHdl);

	/* �ؽ�һ��central bitmap */
	session = m_db->getSessionManager()->allocSession("VLRHeapTestCase::testRedoUpdate", m_conn);
	m_heap->redoFinish(session);
	m_db->getSessionManager()->freeSession(session);

	session = m_db->getSessionManager()->allocSession("VLRHeapTestCase::testRedoUpdate", m_conn);
	m_heap->syncMataPages(session);
	m_db->getSessionManager()->freeSession(session);
	m_heap->getBuffer()->flushAll();

	/* �Ƚ��ļ� */
	File back02(VLR_BACK_02);
	back02.open(true);
	static const int numFiles = 2;
	File *files[numFiles];
	PageType pageType[numFiles];
	int numF = m_heap->getFiles(files, pageType, numFiles);
	CPPUNIT_ASSERT(numF == 1);
	int cmp = compareFile(files[0], &back02, 1);
	CPPUNIT_ASSERT(0 == cmp);

	File b01(VLR_BACK_01);
	b01.remove();
	File b02(VLR_BACK_02);
	b02.remove();

}

void VLRHeapTestCase::testSample() {
	u64 firstPage = 1 + m_vheap->getCBMCount();
	u64 maxPage = firstPage + 500;
	int loop = 3;
	//int loopInsert = 2000;
	Session *session = m_db->getSessionManager()->allocSession("VLRHeapTestCase::testSample", m_conn);
	Record *recS = createRecordSmall(m_tblDef, System::random());
	Record *recM = createRecord(m_tblDef, System::random());
	Record *recB = createRecordLong(m_tblDef, System::random());

	SubRecordBuilder compBuilder(m_tblDef, REC_REDUNDANT);
	SubRecord *subRecS = compBuilder.createEmptySbByName(Limits::PAGE_SIZE, "Permalink TrackbackUrl Title Abstract");
	SubRecord *subRecM = compBuilder.createEmptySbByName(Limits::PAGE_SIZE, "Permalink TrackbackUrl Title Abstract");
	SubRecord *subRecB = compBuilder.createEmptySbByName(Limits::PAGE_SIZE, "Permalink TrackbackUrl Title Abstract");
	SubRecord *subRec = compBuilder.createEmptySbByName(Limits::PAGE_SIZE, "Permalink TrackbackUrl Title Abstract");
	RecordOper::extractSubRecordVR(m_tblDef, recS, subRecS);
	RecordOper::extractSubRecordVR(m_tblDef, recM, subRecM);
	RecordOper::extractSubRecordVR(m_tblDef, recB, subRecB);


	/* ��������Ϣ */
	const HeapStatus& status = m_heap->getStatus();
	CPPUNIT_ASSERT(status.m_dboStats->m_statArr[DBOBJ_ITEM_INSERT] == 0
		&& status.m_dboStats->m_statArr[DBOBJ_ITEM_DELETE] == 0
		&& status.m_dboStats->m_statArr[DBOBJ_ITEM_UPDATE] == 0);

	/* �������� */
	m_heap->updateExtendStatus(session, 1000);
	const HeapStatusEx& statusEx = m_heap->getStatusEx();
	assert(statusEx.m_numLinks == 0 && statusEx.m_numRows == 0 && statusEx.m_pctUsed == .0);

	/* ����������� */
	NTSE_ASSERT(m_heap->insert(session, recS, NULL));
	NTSE_ASSERT(m_heap->insert(session, recS, NULL));
	m_heap->updateExtendStatus(session, 16);
	m_heap->updateExtendStatus(session, 5000);
	

	while (true) {
		RowId rid = m_heap->insert(session, recS, NULL);
		if (RID_GET_PAGE(rid) == maxPage) break;
	}

	//cout << "max page is " << m_heap->getMaxPageNum() << endl;

	for (int i= 0; i < loop; ++i) {
		RowLockHandle *rlh = NULL;
		DrsHeapScanHandle *scanHdl = m_heap->beginScan(session, SubrecExtractor::createInst(session, m_tblDef, subRec), Exclusived, &rlh, true);
		while (m_heap->getNext(scanHdl, subRec)) {
			switch (System::random() % 50) {
				case 0:
					m_heap->deleteCurrent(scanHdl);
					break;
				case 1: case 2: case 3:
					switch (System::random() % 5) {
						case 0:
							m_heap->updateCurrent(scanHdl, subRecS);
						case 1: case 2:
							m_heap->updateCurrent(scanHdl, subRecM);
						case 3: case 4:
							m_heap->updateCurrent(scanHdl, subRecB);
					}
					break;
				default:
					break;
			}
			session->unlockRow(&rlh);
		}
		m_heap->endScan(scanHdl);
	}


	int links = 0, recs = 0, freebytes = 0, pages = 0;
	for (u64 pgn = firstPage; pgn <= m_heap->getMaxUsedPageNum(); ++pgn) {
		if (m_vheap->isPageBmp(pgn)) continue;
		BufferPageHandle *bph = GET_PAGE(session, m_heap->getHeapFile(), PAGE_HEAP, pgn, Shared, m_heap->getDBObjStats(), NULL);
		VLRHeapRecordPageInfo *vpage = (VLRHeapRecordPageInfo *)bph->getPage();
#ifdef NTSE_VERIFY_EX
		m_vheap->heavyVerify(vpage);
#endif


		for (int offset = sizeof(VLRHeapRecordPageInfo); offset <= vpage->m_lastHeaderPos; offset += sizeof(VLRHeader)) {
			VLRHeader *header = (VLRHeader *)((byte *)vpage + offset);
			if (!header->m_ifEmpty) {
				if (header->m_ifLink)
					links++;
				else {
					recs++;
				}
			}
		}

		freebytes += vpage->m_freeSpaceSize;
		pages++;
		session->releasePage(&bph);
	}

	cout << "link avg is " << (double)links / (double)pages << endl;
	cout << "rec avg is " << (double)recs / (double)pages << endl;
	cout << "freebytes avg is " << (double)freebytes / (double)pages << endl;


	freeRecord(recS); freeRecord(recM); freeRecord(recB);
	freeSubRecord(subRecS); freeSubRecord(subRecM); freeSubRecord(subRecB); freeSubRecord(subRec);


	cout << "max page is " << m_heap->getMaxPageNum() << endl;

        
	u64 sampleCnt = (m_heap->getMaxPageNum() / 10 > 3200) ? 3200 : (m_heap->getMaxPageNum() / 10);
	cout << "sample count is " << sampleCnt << endl;
	SampleResult * result = SampleAnalyse::sampleAnalyse(session, m_heap, (int)sampleCnt*2, 50);
	cout << "field name is " << result->m_numFields << endl;
	cout << "sample num is " << result->m_numSamples << endl;
	cout << "link avg is " << result->m_fieldCalc[1].m_average << endl;
	cout << "link delta is " << result->m_fieldCalc[1].m_delta << endl;
	cout << "rec avg is " << result->m_fieldCalc[0].m_average << endl;
	cout << "rec delta is " << result->m_fieldCalc[0].m_delta << endl;
	cout << "freebytes avg is " << result->m_fieldCalc[2].m_average << endl;
	cout << "freebytes delta is " << result->m_fieldCalc[2].m_delta << endl;
	delete result;
    
	m_heap->getStatus();
	CPPUNIT_ASSERT(status.m_dboStats->m_statArr[DBOBJ_ITEM_INSERT] > 0
		&& status.m_dboStats->m_statArr[DBOBJ_ITEM_DELETE] > 0
		&& status.m_dboStats->m_statArr[DBOBJ_ITEM_UPDATE] > 0);
	m_db->getSessionManager()->freeSession(session);
}


void VLRHeapTestCase::testSample2() {
	//cout << "Shit happens." <<endl;
	// ����TableDef
	TableDefBuilder *builder = new TableDefBuilder(1, "test", "testtable");
	builder->addColumn("ID", CT_BIGINT, false)->addColumn("Count", CT_BIGINT);
	builder->addColumn("Another", CT_BIGINT)->addColumnS("Content", CT_VARCHAR, 40);
	builder->addIndex("PRIMARY", true, true, false, "ID", 0, NULL);//->addIndex("Count_idx", false, false, "Count ", NULL);
	TableDef *tableDef = builder->getTableDef();

	delete builder;

	const char *tbl = "tabletest";
	File tblfile(tbl);
	tblfile.remove();
	DrsHeap::create(m_db, tbl, tableDef);

	// get connection
	//m_conn = m_db->getConnection(true);
	Connection *conn = m_db->getConnection(true);

	Session *session = m_db->getSessionManager()->allocSession("VLRHeapTestCase::testSample2", conn);

	DrsHeap *heap;
	EXCPT_OPER(heap = DrsHeap::open(m_db, session, tbl, m_tblDef));
	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(conn);

	VariableLengthRecordHeap *vheap = (VariableLengthRecordHeap *)heap;
	delete tableDef;

	conn = m_db->getConnection(false);
	session = m_db->getSessionManager()->allocSession("VLRHeapTestCase::testSample2", conn);

	RowId rid;
	for (int i = 0; i < 100; ++i) {
		heap->updateExtendStatus(session, 1000);
		RecordBuilder rb(m_tblDef, 0, REC_VARLEN);
		rb.appendBigInt(i)->appendBigInt(i)->appendBigInt(i);
		rb.appendVarchar("abcdefghijklmnopqrstuvwxyz1234567890");
		Record *rec = rb.getRecord();
		try {
			rid = heap->insert(session, rec, NULL);
		} catch (NtseException &e) {
			cout << e.getMessage() <<endl;
		}
		cout << rid <<endl;
		freeRecord(rec);
		assert(rid);
		heap->updateExtendStatus(session, 1000);
	}

	heap->updateExtendStatus(session, 1000);

	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(conn);
}

/* ���ߺ��� */
/**
  ����䳤��¼
  ���random��Ϊ0������̶��ļ�¼
  ���randomΪ0������������ȼ�¼
  �����TableDefΪ�䳤�ѵ�tableDef
 */
Record* createRecord(TableDef *tableDef, int random) {
	u64 rid = 0;
	RecordBuilder rb(tableDef, rid, REC_VARLEN);
	int fact = Limits::PAGE_SIZE / 1024;

	u64 id = random ? (((u64)random << 32) + random) : (u64)((float)((u64)-1) * rand() / RAND_MAX);
	u64 uid = random ? random : (u64)((float)((u32)-1) * rand() / RAND_MAX);
	u64 publishTime = random ? random + 1999 : (u64)((float)((u32)-1) * rand() / RAND_MAX);
	u64 modifyTime = random ? random + 2000 : (u64)((float)((u32)-1) * rand() / RAND_MAX);
	int isPublished = random ? 1 : ((rand() / RAND_MAX > 0.5) ? 1 : 0);
	char permalink[200];
	sprintf(permalink, "http://xxx.%d.com\0", 
		random ? random : rand());
	char permalinkFinal[1000];
	for (int i = 0; i < fact; ++i) {
		memcpy(permalinkFinal + i * strlen(permalink), permalink, strlen(permalink));
		if (i == fact - 1)
			permalinkFinal[(i + 1) * strlen(permalink)] = '\0';
	}
	char trackbackUrl[200];
	sprintf(trackbackUrl, "http//xxx.yyyyyyyyyyyyy.%d.com/zzz",
		random ? (((u64)random << 32) + random) : (u64)((float)((u32)-1) * rand() / RAND_MAX));
	char trackbackUrlFinal[1000];
	for (int i = 0; i < fact; ++i) {
		memcpy(trackbackUrlFinal + i * strlen(trackbackUrl), trackbackUrl, strlen(trackbackUrl));
		if (i == fact - 1)
			trackbackUrlFinal[(i + 1) * strlen(trackbackUrl)] = '\0';
	}

	int commentCount = random ? (random / 10) : (u32)((float)((u32)10000) * rand() / RAND_MAX);

	char title[200];
	sprintf(title, "A story about %d",
		random ? 0x7FFFFFFF & (~((u32)random)) : (u32)((float)((u32)10000) * rand() / RAND_MAX + 10000));
	char titleFinal[1000];
	for (int i = 0; i < fact; ++i) {
		memcpy(titleFinal + i * strlen(title), title, strlen(title));
		if (i == fact - 1)
			titleFinal[(i + 1) * strlen(title)] = '\0';
	}

	char abstracT[500];
	sprintf(abstracT, "long long ago , these was a tiny kingdom. The king, the queen, and the princess lived a happy life, until the day %d come...",
		random ? (0x7FFF & random) : (u32)((float)((u32)1000) * rand() / RAND_MAX + 999));
	char abstractFinal[4000];
	for (int i = 0; i < fact; ++i) {
		memcpy(abstractFinal + i * strlen(abstracT), abstracT, strlen(abstracT));
		if (i == fact - 1)
			abstractFinal[(i + 1) * strlen(abstracT)] = '\0';
	}

	rb.appendBigInt(id)->appendBigInt(uid);
	rb.appendBigInt(publishTime)->appendBigInt(modifyTime);
	rb.appendTinyInt(isPublished)->appendVarchar(permalinkFinal);
	rb.appendVarchar(trackbackUrlFinal)->appendInt(commentCount);
	rb.appendVarchar(titleFinal)->appendVarchar(abstractFinal);

	return rb.getRecord(0);
}

/**
 * ����һ��С��¼��С��¼����˼�ǣ�һ����С��¼�Ŀռ�仯����Ӱ��flag�ı仯
 */
Record* createRecordSmall(TableDef *tableDef, int random) {
	u64 rid = 0;
	RecordBuilder rb(tableDef, rid, REC_VARLEN);


	u64 id = random ? (((u64)random << 32) + random) : (u64)((float)((u64)-1) * rand() / RAND_MAX);
	u64 uid = random ? random : (u64)((float)((u32)-1) * rand() / RAND_MAX);
	u64 publishTime = random ? random + 1999 : (u64)((float)((u32)-1) * rand() / RAND_MAX);
	u64 modifyTime = random ? random + 2000 : (u64)((float)((u32)-1) * rand() / RAND_MAX);
	int isPublished = random ? 1 : ((rand() / RAND_MAX > 0.5) ? 1 : 0);
	char permalink[200];
	sprintf(permalink, "\0");
	char trackbackUrl[200];
	sprintf(trackbackUrl, "\0");
	int commentCount = random ? (random / 10) : (u32)((float)((u32)10000) * rand() / RAND_MAX);
	char title[200];
	sprintf(title, "\0");
	char abstracT[500];
	sprintf(abstracT, "\0");

	rb.appendBigInt(id)->appendBigInt(uid);
	rb.appendBigInt(publishTime)->appendBigInt(modifyTime);
	rb.appendTinyInt(isPublished)->appendVarchar(permalink);
	rb.appendVarchar(trackbackUrl)->appendInt(commentCount);
	rb.appendVarchar(title)->appendVarchar(abstracT);

	return rb.getRecord(0);
}

/**
 * ����һ�����¼���������ɾ��������¼����Ȼ������ҳ��flag�仯
 */
Record* createRecordLong(TableDef *tableDef, int random) {
	u64 rid = 0;
	RecordBuilder rb(tableDef, rid, REC_VARLEN);
	int fact = Limits::PAGE_SIZE / 1024;

	u64 id = random ? (((u64)random << 32) + random) : (u64)((float)((u64)-1) * rand() / RAND_MAX);
	u64 uid = random ? random : (u64)((float)((u32)-1) * rand() / RAND_MAX);
	u64 publishTime = random ? random + 1999 : (u64)((float)((u32)-1) * rand() / RAND_MAX);
	u64 modifyTime = random ? random + 2000 : (u64)((float)((u32)-1) * rand() / RAND_MAX);
	int isPublished = random ? 1 : ((rand() / RAND_MAX > 0.5) ? 1 : 0);

	char permalink[100];
	sprintf(permalink, "http://xxxxxxxxxxxxxxxxxxxxxxxxxxxxxx.10000.com\0", 
		(u32)(random ? random : rand()));
	char permalinkFinal[1000];
	for (int i = 0; i < fact; ++i) {
		memcpy(permalinkFinal + i * strlen(permalink), permalink, strlen(permalink));
		if (i == fact - 1)
			permalinkFinal[(i + 1) * strlen(permalink)] = '\0';
	}
	//cout<<"permalink strlen is "<<strlen(permalinkFinal)<<endl;

	char trackbackUrl[200];
	sprintf(trackbackUrl, "http//xxx.yyyyyyyxxxxxxxxxxxyy.%d.com/zzz",
		random ? (((u64)random << 32) + random) : (u64)((float)((u32)-1) * rand() / RAND_MAX));
	char trackbackUrlFinal[1000];
	for (int i = 0; i < fact; ++i) {
		memcpy(trackbackUrlFinal + i * strlen(trackbackUrl), trackbackUrl, strlen(trackbackUrl));
		if (i == fact - 1)
			trackbackUrlFinal[(i + 1) * strlen(trackbackUrl)] = '\0';
	}
	//cout<<"trackbackUrl strlen is "<<strlen(trackbackUrlFinal)<<endl;

	int commentCount = random ? (random / 10) : (u32)((float)((u32)10000) * rand() / RAND_MAX);
	
	char title[200];
	sprintf(title, "A story aaaaaaaaaaaaaaaaaaaaaaaabout %d",
		random ? 0x7FFFFFFF & (~((u32)random)) : (u32)((float)((u32)10000) * rand() / RAND_MAX + 10000));
	char titleFinal[1000];
	for (int i = 0; i < fact; ++i) {
		memcpy(titleFinal + i * strlen(title), title, strlen(title));
		if (i == fact - 1)
			titleFinal[(i + 1) * strlen(title)] = '\0';
	}
	//cout<<"title strlen is "<<strlen(titleFinal)<<endl;

	char abstracT[500];
	sprintf(abstracT, "long long ago , these was a tiny kingdom. The king, the queen, and the princess lived a happy life, until the day %d come.................................................................................. .........................................................................................................................................................................................",
		random ? (0x7FFF & random) : (u32)((float)((u32)1000) * rand() / RAND_MAX + 999));
	char abstractFinal[4000];
	for (int i = 0; i < fact; ++i) {
		memcpy(abstractFinal + i * strlen(abstracT), abstracT, strlen(abstracT));
		if (i == fact - 1)
			abstractFinal[(i + 1) * strlen(abstracT)] = '\0';
	}
	//cout<<"abstracT strlen is "<<strlen(abstractFinal)<<endl;


	rb.appendBigInt(id)->appendBigInt(uid);
	rb.appendBigInt(publishTime)->appendBigInt(modifyTime);
	rb.appendTinyInt(isPublished)->appendVarchar(permalinkFinal);
	rb.appendVarchar(trackbackUrlFinal)->appendInt(commentCount);
	rb.appendVarchar(titleFinal)->appendVarchar(abstractFinal);

	return rb.getRecord(0);
}


/**
 * ����ΪLINK_SIZE�ļ�¼�������ܴ��ڣ����ﹹ��һ�����������
 */
Record* createTinyRawRecord(int i) {
	assert(i >= 0 && i <= 255);
	Record *rec = new Record();
	byte *data = new byte[4];
	rec->m_format = REC_VARLEN;
	rec->m_rowId = 0;
	rec->m_data = data;
	rec->m_size = 4;
	for (uint j = 0; j < rec->m_size; ++j) {
		data[j] = (byte)i;
	}
	return rec;
}

Record* createShortRawRecord(int i) {
	assert(i >= 0 && i <= 255);
	int dataSize = VariableLengthRecordHeap::getSpaceLimit(0) * 2 / 3;
	Record *rec = new Record();
	byte *data = new byte[dataSize];
	rec->m_format = REC_VARLEN;
	rec->m_rowId = 0;
	rec->m_data = data;
	rec->m_size = dataSize;
	for (uint j = 0; j < rec->m_size; ++j) {
		data[j] = (byte)i;
	}
	return rec;
}

Record* createLongRawRecord(int i) {
	assert(i >= 0 && i <= 255);
	int dataSize = VariableLengthRecordHeap::getSpaceLimit(2) - 20;
	Record *rec = new Record();
	byte *data = new byte[dataSize];
	rec->m_format = REC_VARLEN;
	rec->m_rowId = 0;
	rec->m_data = data;
	rec->m_size = dataSize;
	for (uint j = 0; j < rec->m_size; ++j) {
		data[j] = (byte)i;
	}
	return rec;
}

/* DaVince�̲߳���rows����¼ */
void DaVince::run() {	
	DataDaVince *data = (DataDaVince *)m_data;
	
	/*
	Thread::sleep(1000);

	hex(cout);
	cout<<"record->m_data = "<<(uint)(data->m_record->m_data)<<endl;
	dec(cout);
	assert((uint)(data->m_record->m_data) != 0xdddddddd);
	*/

	int rows = data->m_rows;
	Connection *conn = m_db->getConnection(false);
	Session *session = m_db->getSessionManager()->allocSession("DaVince::run", conn);
	while (rows--) {
		m_heap->insert(session, data->m_record, data->m_rlh);
	}
	if (data->m_rlh) session->unlockRow(data->m_rlh);
	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(conn);
}

/* Connie�̸߳���һ����¼ */
void Connie::run() {
	Record *rec = (Record *)m_data;
	Connection *conn = m_db->getConnection(false);
	Session *session = m_db->getSessionManager()->allocSession("Connie::run", conn);
	m_heap->update(session, rec->m_rowId, rec);
    m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(conn);
}

/* Dracula�߳�ɾ��һ����¼ */
void Dracula::run() {
	RowId rid = *(RowId *)m_data;
	Connection *conn = m_db->getConnection(false);
	Session *session = m_db->getSessionManager()->allocSession("Dracula::run", conn);
	m_heap->del(session, rid);
    m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(conn);
}

/* VanHelsing�̰߳�װ�趨�������б�ɨ�� */
void VanHelsing::run() {
	DataVanHelsing *data = (DataVanHelsing *)m_data;
	bool returnLinkSrc = data->m_returnLinkSrc;
	LockMode lockMode = data->m_lockMode;
	SubRecord *subRec = data->m_subRec;
	RowLockHandle *rowlockHdl;
	Connection *conn = m_db->getConnection(false);
	Session *session = m_db->getSessionManager()->allocSession("VanHelsing::run", conn);
	
	DrsHeapScanHandle *scanHdl = m_heap->beginScan(session, SubrecExtractor::createInst(session, m_tblDef, subRec), lockMode, (lockMode == None) ? NULL : &rowlockHdl, returnLinkSrc);
	while (m_heap->getNext(scanHdl, subRec)) {
		if (lockMode != None) {
			CPPUNIT_ASSERT(rowlockHdl != NULL);
			session->unlockRow(&rowlockHdl);
		}
	}
	m_heap->endScan(scanHdl);
	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(conn);
}

/* Indiana�߳�getһ����¼ */
void Indiana::run() {
	DataIndiana *data = (DataIndiana *)m_data;
	Connection *conn = m_db->getConnection(false);
	Session *session = m_db->getSessionManager()->allocSession("Indiana::run", conn);
	bool success = m_heap->getRecord(session, data->m_rowId, data->m_outRec);
	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(conn);
	data->m_got = success;
}

/* ��������� */
static int randomInt(int max) {
	int ranInt = (int)(max * rand() / RAND_MAX);
	ranInt = ranInt % max;
	return ranInt;
}

