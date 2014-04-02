#include "heap/TestFLRHeap.h"
#include "api/Table.h"
#include "heap/Heap.h"
#include "api/Database.h"
#include "util/File.h"
#include "misc/Global.h"
#include "misc/RecordHelper.h"
#include "misc/Session.h"
#include "Test.h"
#include "util/Portable.h"
#include "util/Thread.h"
#include "misc/Txnlog.h"
#include "misc/Trace.h"
#include "misc/Verify.h"
#include "misc/Sample.h"
#include "heap/VariableLengthRecordHeap.h"
#include <cstdlib>
#include <iostream>
#include "util/Hash.h"

using namespace std;
using namespace ntse;

static int randomInt(int max);

#define SMALL_TBLDEF "smallflrtd.nstd"
#define SMALL_HEAP "smallflrheap.nsd"
#define BIG_TBLDEF "bigflrtd.nstd"
#define BIG_HEAP "bigflrheap.nsd"
#define TBLDEF_BACKUP "backupTblDef.bak"
#define HEAP_BACKUP "backupHeapFile.bak"


/** 机器人类 **/
class Frank : public Robot {
public:
	Frank(Database *db, Connection *conn, TableDef *tblDef, DrsHeap *heap, const char *name, void *data = NULL) : Robot(db, conn, tblDef, heap, name, data) {};
private:
	virtual void run();
};

class Mooly : public Robot {
public:
	Mooly(Database *db, Connection *conn, TableDef *tblDef, char *heapname, DrsHeap **heapPt) : Robot(db, conn, tblDef, NULL, heapname, heapPt) {};
private:
	virtual void run();
};

class Julia : public Robot {
public:
	Julia(Database *db, Connection *conn, TableDef *tblDef, DrsHeap *heap, const char *name, void *data = NULL) : Robot(db, conn, tblDef, heap, name, data) {};
private:
	virtual void run();
};

class Gump : public Robot {
public:
	Gump(Database *db, Connection *conn, TableDef *tblDef, DrsHeap *heap, const char *name, int *times) :
	  Robot(db, conn, tblDef, heap, name, (void *)times) {};
private:
	virtual void run();
};

class Depp : public Robot {
public:
	Depp(Database *db, Connection *conn, TableDef *tblDef, DrsHeap *heap, const char *name) :
	  Robot(db, conn, tblDef, heap, name, (void *)0) {};
private:
	virtual void run();
};

class Michael : public Thread {
public:
	Michael(Database *db, u16 tableId, u64 rowId, LockMode lockMode) : Thread("Robot not Robot") {
		m_db = db;
		m_tid = tableId;
		m_rid = rowId;
		m_lmode = lockMode;
	}
private:
	Database *m_db;
	u16 m_tid;
	u64 m_rid;
	LockMode m_lmode;
	virtual void run();
};

class Susan : public Robot {
public:
	Susan(Database *db, Connection *conn, TableDef *tblDef, DrsHeap *heap, const char *name, int *data) :
	  Robot(db, conn, tblDef, heap, name, (void *)data) {};
private:
	virtual void run();

};

class Sergey : public Robot {
public:
	Sergey(Database *db, Connection *conn, TableDef *tblDef, DrsHeap *heap, bool lockrow, u64 delcnt = 0):
	  Robot(db, conn, tblDef, heap, "Sergey", NULL), m_delCnt(delcnt), m_lockrow(lockrow) {};
private:
	virtual void run();
	u64 m_delCnt;
	bool m_lockrow;
};

/****************/

TableDef *FLRHeapTestCase::createSmallTableDef() {
	File oldsmallfile(SMALL_TBLDEF);
	oldsmallfile.remove();

	m_db = Database::open(&m_config, 1);		
	TableDefBuilder *builder;
	TableDef *tableDef;

	// 创建小堆
	builder = new TableDefBuilder(99, "inventory", "User");
	builder->addColumn("UserId", CT_BIGINT, false)->addColumnS("UserName", CT_CHAR, 50);
	builder->addColumn("BankAccount", CT_BIGINT)->addColumn("Balance", CT_INT);
	builder->addIndex("PRIMARY", true, true, false, "UserId", 0, NULL);
	tableDef = builder->getTableDef();
	EXCPT_OPER(tableDef->writeFile(SMALL_TBLDEF));
	delete builder;

	return tableDef;
}

/* 测试类成员*/
/**
 *  创建一个小记录的定长堆
 */
DrsHeap* FLRHeapTestCase::createSmallHeap(TableDef *tableDef) {
	File oldsmallfile(SMALL_HEAP);
	oldsmallfile.remove();

	DrsHeap *heap;
	EXCPT_OPER(DrsHeap::create(m_db, SMALL_HEAP, tableDef));
	m_conn = m_db->getConnection(true);
	Session *session = m_db->getSessionManager()->allocSession("FLRHeapTestCase::createSmallHeap", m_conn);
	heap = DrsHeap::open(m_db, session, SMALL_HEAP, tableDef);
	m_db->getSessionManager()->freeSession(session);

	return heap;
}

const char* FLRHeapTestCase::getName() {
	return "Fixed length record heap test";
}

const char* FLRHeapTestCase::getDescription() {
	return "Test various operations of fixed length record heap and common features of heap";
}

bool FLRHeapTestCase::isBig() {
	return false;
}

/** 
 * setUp过程
 *		流程：	1. 清空数据
 *				2. 调用createSmallHeap创建数据库和小堆
 */
void FLRHeapTestCase::setUp() {
	//ts.hp = true;
	//vs.hp = true;
	m_conn = NULL;
	m_db = NULL;
	m_heap = NULL;
	m_tblDef = NULL;
	Database::drop(".");
	m_tblDef = createSmallTableDef();
	m_heap = createSmallHeap(m_tblDef);
}

/** 
 * tearDown过程
 *		流程：	1. 关闭并drop测试堆
 *				2. 关闭connection
 *				3. 关闭并drop数据库
 */
void FLRHeapTestCase::tearDown() {
	/* 关闭堆，删除数据库 */
	if (m_tblDef != NULL) {
		TableDef::drop(SMALL_TBLDEF);
		delete m_tblDef;
	}

	if (m_heap) {
		dropSmallHeap();
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
 * close并drop测试用堆
 */
void FLRHeapTestCase::dropSmallHeap() {
	Session *session = m_db->getSessionManager()->allocSession("FLRHeapTestCase::dropSmallHeap", m_conn);
	EXCPT_OPER(m_heap->close(session, true));
	m_db->getSessionManager()->freeSession(session);
	EXCPT_OPER(DrsHeap::drop(SMALL_HEAP));
	delete m_heap;
	m_heap = NULL;
}

/**
 * close测试用堆，并且释放堆对象
 */
void FLRHeapTestCase::closeSmallHeap() {
	Session *session = m_db->getSessionManager()->allocSession("FLRHeapTestCase::closeSmallHeap", m_conn);
	EXCPT_OPER(m_heap->close(session, true));
	m_db->getSessionManager()->freeSession(session);
	delete m_heap;
	m_heap = NULL;
}

/** 
 * 测试open过程
 *		流程：	1. 进行有限堆操作
 *				2. close再open
 *				3. 比较再次open后的堆状态
 */
void FLRHeapTestCase::testOpen() {
	Session *s = m_db->getSessionManager()->allocSession("FLRHeapTestCase::testOpen", m_conn);
	Record *rec = createRecord(999, 999, "Shakespear", 999, 999, m_tblDef);
	RowId rid = m_heap->insert(s, rec, NULL);
	u64 maxPage = m_heap->getMaxPageNum();//m_heap->getMaxPageNum();
	u64 maxUsedPage = m_heap->getMaxUsedPageNum();//m_heap->getMaxUsedPageNum();
	u64 lsn = m_heap->getPageLSN(s, RID_GET_PAGE(rid), m_heap->getDBObjStats());
	m_db->getSessionManager()->freeSession(s);
	/* 关闭再打开 */
	closeSmallHeap();
	delete m_tblDef;
	m_tblDef = NULL;
	File f(SMALL_HEAP);
	bool exist;
	f.isExist(&exist);
	CPPUNIT_ASSERT(exist);
	s = m_db->getSessionManager()->allocSession("FLRHeapTestCase::testOpen", m_conn);
	try {
		m_tblDef = TableDef::open(SMALL_TBLDEF);
		m_heap = DrsHeap::open(m_db, s, SMALL_HEAP, m_tblDef);
	} catch (NtseException &) {
		CPPUNIT_ASSERT(false);
	}
	/* 验证 */
	Record record;
	record.m_size = Limits::PAGE_SIZE;
	record.m_format = REC_FIXLEN;
	record.m_data = new byte[Limits::PAGE_SIZE];
	bool canGet = m_heap->getRecord(s, rid, &record);
	CPPUNIT_ASSERT(canGet);
	CPPUNIT_ASSERT(record.m_size == rec->m_size);
	CPPUNIT_ASSERT(0 == memcmp(record.m_data, rec->m_data, rec->m_size));
	CPPUNIT_ASSERT(maxPage == m_heap->getMaxPageNum());
	CPPUNIT_ASSERT(maxUsedPage == m_heap->getMaxUsedPageNum()/*m_heap->getMaxUsedPageNum()*/);
	CPPUNIT_ASSERT(lsn == m_heap->getPageLSN(s, RID_GET_PAGE(rid), m_heap->getDBObjStats()));

	m_db->getSessionManager()->freeSession(s);

	CPPUNIT_ASSERT(DrsHeap::getVersionFromTableDef(m_tblDef) == HEAP_VERSION_FLR);

	m_tblDef->m_recFormat = REC_REDUNDANT;
	CPPUNIT_ASSERT(DrsHeap::getVersionFromTableDef(m_tblDef) == HEAP_VERSION_VLR);

	delete [] record.m_data;
    freeRecord(rec);

	tearDown();
	setUp();
	closeSmallHeap();

	Mooly m1(m_db, m_conn, m_tblDef, SMALL_HEAP, &m_heap);
	m1.enableSyncPoint(SP_HEAP_AFTER_GET_HEADER_PAGE);
	
	m1.start();
	m1.joinSyncPoint(SP_HEAP_AFTER_GET_HEADER_PAGE);

	//m1.disableSyncPoint(SP_HEAP_AFTER_GET_HEADER_PAGE);
	m1.notifySyncPoint(SP_HEAP_AFTER_GET_HEADER_PAGE);
	m1.join();

}

/** 
 * 测试drop过程
 *		流程：	1. 关闭并drop堆
 *				2. 确认drop成功
 */
void FLRHeapTestCase::testDrop() {

	CPPUNIT_ASSERT(m_heap);
	File f(SMALL_HEAP);

	bool exist;
	f.isExist(&exist);
	CPPUNIT_ASSERT(exist);

	closeSmallHeap();
	f.isExist(&exist);
	CPPUNIT_ASSERT(exist);
	u64 size;
	f.open(true);
	f.getSize(&size);
	CPPUNIT_ASSERT(size == Limits::PAGE_SIZE);
	f.close();

	EXCPT_OPER(DrsHeap::drop(SMALL_HEAP));
	f.isExist(&exist);
	CPPUNIT_ASSERT(!exist);
}

/** 
 * 测试insert过程
 *		流程：	1. 基本功能测试
 *				2. 代码分支覆盖测试（内容见内部具体注释）
 */
void FLRHeapTestCase::testInsert() {
	RowId rid;
	u64 pageNum;
	s16 slotNum;
	CPPUNIT_ASSERT(m_heap);
	int i = 0;
	pageNum = 0;
	int recPerPage = 0;
	Record *rec;
	//u64 rows = ((FixedLengthRecordHeap *)m_heap)->getRecPerPage() * (DrsHeap::HEAP_INCREASE_SIZE + 1);
	u64 rows = ((FixedLengthRecordHeap *)m_heap)->getRecPerPage() * (m_tblDef->m_incrSize + 1);
	//cout <<"rows is "<<rows<<endl;

	CPPUNIT_ASSERT(m_heap->getStatus().m_dboStats->m_statArr[DBOBJ_ITEM_INSERT] == 0);
 	CPPUNIT_ASSERT(m_heap->getStatus().m_dataLength == Limits::PAGE_SIZE);
	

	// 计时
	//startTime = System::currentTimeMillis();
	for (; i < 500;) {
		rid = insertRecord(++i, &rec, m_db, m_conn, m_heap, m_tblDef);
		if (pageNum == 0) pageNum = RID_GET_PAGE(rid);
		if (pageNum != 0 && pageNum != RID_GET_PAGE(rid) && !recPerPage)
			recPerPage = i - 1;
		//delete rec;
                freeRecord(rec);
	}
	CPPUNIT_ASSERT(m_heap->getUsedSize() > 0);
	CPPUNIT_ASSERT(((FixedLengthRecordHeap *)m_heap)->getRecPerPage() == recPerPage);
	//cout<<"Rec per page is "<<recPerPage<<endl;
	for (i = 500; i < rows;) { // 记录顺序插入测试
		rid = insertRecord(++i, &rec, m_db, m_conn, m_heap, m_tblDef);
		pageNum = RID_GET_PAGE(rid);
		slotNum = RID_GET_SLOT(rid);
		//cout<<"i is "<<i<<endl<<"pageNum is "<<pageNum<<endl<<"slotNum is "<<slotNum<<endl;
		CPPUNIT_ASSERT(pageNum == (i - 1) / recPerPage + 1);
		CPPUNIT_ASSERT(slotNum == (i - 1) % recPerPage);
		freeRecord(rec);
	}
	// 计时
	//endTime = System::currentTimeMillis();
	//cout << "One hundred thousand inserts take time "<<endTime - startTime<<" milliseconds."<<endl;

	CPPUNIT_ASSERT(m_heap->getStatus().m_dboStats->m_statArr[DBOBJ_ITEM_INSERT] == i);
	CPPUNIT_ASSERT(m_heap->getStatus().m_dataLength > 0);
	
#ifdef NTSE_TRACE
	nftrace(true, tout << m_heap);
	nftrace(true, tout << (HeapHeaderPageInfo *)m_heap->getHeaderPage());
#endif
	//Session *tmpSe = m_db->getSessionManager()->allocSession("FLRHeapTestCase::testInsert", m_conn);
	//m_heap->writeTableDef(tmpSe);
	//m_db->getSessionManager()->freeSession(tmpSe);
	m_heap->printInfo();



	/*** 以下为覆盖FixedLengthRecordHeap.cpp 中约第145行起始的多线程分支代码：
	if (headerPage->m_firstFreePageNum != pageNum) { //现在的页面不是空闲页面链表首页
	***/
	tearDown(); setUp();

	//i = 20000;
	//i = TESTROWS;
	//i = (int)(((FixedLengthRecordHeap *)m_heap)->getRecPerPage() * (DrsHeap::HEAP_INCREASE_SIZE + 1));
	i = (int)(((FixedLengthRecordHeap *)m_heap)->getRecPerPage() * (m_tblDef->m_incrSize + 1));

	Gump g1(m_db, m_conn, m_tblDef, m_heap, "Insert thread 1", &i);

	int s1data[2];
	s1data[0] = 1;
	s1data[1] = recPerPage;
	Susan s1(m_db, m_conn, m_tblDef, m_heap, "Delete thread 1", s1data);

	g1.enableSyncPoint(SP_HEAP_FLR_INSERT_BEFORE_REVERSE_LOCK_HEADER_PAGE);
	g1.start();

	g1.joinSyncPoint(SP_HEAP_FLR_INSERT_BEFORE_REVERSE_LOCK_HEADER_PAGE);
	g1.notifySyncPoint(SP_HEAP_FLR_INSERT_BEFORE_REVERSE_LOCK_HEADER_PAGE);
	g1.joinSyncPoint(SP_HEAP_FLR_INSERT_BEFORE_REVERSE_LOCK_HEADER_PAGE); // 第二次停住在这里，此时在操作页面二。
	//Thread::msleep(100);

	//cout<<"First free page num is "<<((FixedLengthRecordHeap *)m_heap)->getFirstFreePageNum()<<endl;
	s1.start();
	s1.join();
	//Thread::msleep(200);
	//cout<<"First free page num is "<<((FixedLengthRecordHeap *)m_heap)->getFirstFreePageNum()<<endl;

	g1.disableSyncPoint(SP_HEAP_FLR_INSERT_BEFORE_REVERSE_LOCK_HEADER_PAGE);
	g1.notifySyncPoint(SP_HEAP_FLR_INSERT_BEFORE_REVERSE_LOCK_HEADER_PAGE);
	g1.join();
	/*** 覆盖测试完毕 ***/

	/*** 加行锁测试 ***/
	RowLockHandle *rowLockHdl = NULL;

	Record *newRec = createRecord(999, 999, "Shakespear", 999, 999, m_tblDef);

	Session *s = m_db->getSessionManager()->allocSession("FLRHeapTestCase::testInsert", m_conn);
	m_heap->insert(s, newRec, &rowLockHdl);
	CPPUNIT_ASSERT(rowLockHdl);
	s->unlockRow(&rowLockHdl);
	m_db->getSessionManager()->freeSession(s);

	freeRecord(newRec);


	/*** 以下为覆盖FixedLengthRecordHeap.cpp 中
		if (-1 == freePage->m_firstFreeSlot) { // 没有空闲槽了，重新来过
			session->releasePage(&freePageHdl);
			goto findFreePage_begin;
		}
	     的覆盖测试 ***/

	/*
	s = m_db->getSessionManager()->allocSession("FLRHeapTestCase::testInsert", m_conn);
	m_heap->truncate(s);
	m_db->getSessionManager()->freeSession(s);
	*/
	tearDown(); setUp();

	i = recPerPage * 10;
	Gump gump1(m_db, m_conn, m_tblDef, m_heap, "Insert and stop", &i);
	gump1.enableSyncPoint(SP_HEAP_FLR_INSERT_BEFORE_LOCK_A_USEFULL_PAGE);

	gump1.start();

	gump1.joinSyncPoint(SP_HEAP_FLR_INSERT_BEFORE_LOCK_A_USEFULL_PAGE);


	Gump gump2(m_db, m_conn, m_tblDef, m_heap, "Insert and no stop", &i);
	gump2.start();
	gump2.join();


	// 让gump1跑起来
	gump1.disableSyncPoint(SP_HEAP_FLR_INSERT_BEFORE_LOCK_A_USEFULL_PAGE);
	gump1.notifySyncPoint(SP_HEAP_FLR_INSERT_BEFORE_LOCK_A_USEFULL_PAGE);

	gump1.join();
	/*** 覆盖测试完毕 ***/


	/*** 以下为覆盖FixedLengthRecordHeap.cpp 中
		if (-1 == freePage->m_firstFreeSlot) { // 没有空闲槽了，重新来过
			session->releasePage(&freePageHdl);
			goto findFreePage_begin;
		}
	的覆盖测试 ***/
	/*
	s = m_db->getSessionManager()->allocSession("FLRHeapTestCase::testInsert", m_conn);
	m_heap->truncate(s);
	m_db->getSessionManager()->freeSession(s);
	*/
	tearDown(); setUp();

	//i = recPerPage * (DrsHeap::HEAP_INCREASE_SIZE + 1);
	i = recPerPage * (m_tblDef->m_incrSize + 1);
	Gump gump3(m_db, m_conn, m_tblDef, m_heap, "Insert and stop before extend heap.", &i);
	gump3.enableSyncPoint(SP_HEAP_FLR_FINDFREEPAGE_WANT_TO_EXTEND);
	gump3.start();
	gump3.joinSyncPoint(SP_HEAP_FLR_FINDFREEPAGE_WANT_TO_EXTEND);

	// 停住了
	i = recPerPage;
	Gump gump4(m_db, m_conn, m_tblDef, m_heap, "Insert and stop before extend heap.", &i);
	gump4.start();
	gump4.join();

	// 让gump3跑起来
	gump3.disableSyncPoint(SP_HEAP_FLR_FINDFREEPAGE_WANT_TO_EXTEND);
	gump3.notifySyncPoint(SP_HEAP_FLR_FINDFREEPAGE_WANT_TO_EXTEND);

	gump3.join();


	/**** 覆盖 insert中尝试加行锁不成功的情况，且这条记录是最后一条记录 *****/
	tearDown(); setUp();

	rec = createRecord(999, 999, "Connie Cong", 999, 999, m_tblDef);
	
	Session *session = m_db->getSessionManager()->allocSession("FLRHeapTestCase::testInsert", m_conn);
	for (i = 0; i < 2 * recPerPage - 1; ++i) {
		rid = m_heap->insert(session, rec, NULL);
	}
	CPPUNIT_ASSERT(RID_GET_PAGE(rid) == 2);
	CPPUNIT_ASSERT(RID_GET_SLOT(rid) == recPerPage - 2);
	m_db->getSessionManager()->freeSession(session);

	/* 构造完毕，第二页还剩一个slot */
	i = 1;
	Depp d1(m_db, m_conn, m_tblDef, m_heap, "Insert a record and lock");
	d1.enableSyncPoint(SP_HEAP_FLR_INSERT_BEFORE_REVERSE_LOCK_HEADER_PAGE);
	d1.enableSyncPoint(SP_HEAP_FLR_INSERT_BEFORE_TRY_LOCK_ROW);
	d1.enableSyncPoint(SP_HEAP_FLR_INSERT_TRY_LOCK_ROW_FAILED);
	d1.enableSyncPoint(SP_HEAP_FLR_INSERT_UNLOCK_PAGE_AFTER_TRY_LOCK_ROW_FAILED);
	d1.enableSyncPoint(SP_HEAP_FLR_INSERT_No_Free_Slot_After_Relock);

	d1.start();
	d1.joinSyncPoint(SP_HEAP_FLR_INSERT_BEFORE_REVERSE_LOCK_HEADER_PAGE);
	/* g5停住在锁首页 */
	d1.notifySyncPoint(SP_HEAP_FLR_INSERT_BEFORE_REVERSE_LOCK_HEADER_PAGE); // 放行
	d1.joinSyncPoint(SP_HEAP_FLR_INSERT_BEFORE_TRY_LOCK_ROW);

	session = m_db->getSessionManager()->allocSession("FLRHeapTestCase::testInsert", m_conn);
	// 取得Depp想要申请的行锁
	rowLockHdl = NULL;
	rowLockHdl = LOCK_ROW(session, m_tblDef->m_id, RID(2, recPerPage - 1), Exclusived);
	assert(rowLockHdl);

	d1.notifySyncPoint(SP_HEAP_FLR_INSERT_BEFORE_TRY_LOCK_ROW);
	d1.joinSyncPoint(SP_HEAP_FLR_INSERT_TRY_LOCK_ROW_FAILED);
	d1.notifySyncPoint(SP_HEAP_FLR_INSERT_TRY_LOCK_ROW_FAILED);
	d1.joinSyncPoint(SP_HEAP_FLR_INSERT_UNLOCK_PAGE_AFTER_TRY_LOCK_ROW_FAILED);

	session->unlockRow(&rowLockHdl);

	d1.notifySyncPoint(SP_HEAP_FLR_INSERT_UNLOCK_PAGE_AFTER_TRY_LOCK_ROW_FAILED);
	d1.joinSyncPoint(SP_HEAP_FLR_INSERT_BEFORE_REVERSE_LOCK_HEADER_PAGE);



	// 现在第二次执行到反向锁定首页这步，我们将第一页删光，使其插入第一首页
	for (i = 0; i < recPerPage; ++i) {
		rid = RID(1, i);
		bool canDel = m_heap->del(session, rid);
		CPPUNIT_ASSERT(canDel);
	}

	// insert会从头开始，然后又达到尝试锁定之前
	d1.notifySyncPoint(SP_HEAP_FLR_INSERT_BEFORE_REVERSE_LOCK_HEADER_PAGE);
	d1.joinSyncPoint(SP_HEAP_FLR_INSERT_BEFORE_TRY_LOCK_ROW);


	/* 我们再次阻挠d1线程直接锁定成功 */
	rowLockHdl = LOCK_ROW(session, m_tblDef->m_id, RID(1, recPerPage - 1), Exclusived);
	d1.notifySyncPoint(SP_HEAP_FLR_INSERT_BEFORE_TRY_LOCK_ROW);
	d1.joinSyncPoint(SP_HEAP_FLR_INSERT_TRY_LOCK_ROW_FAILED);

	
	d1.notifySyncPoint(SP_HEAP_FLR_INSERT_TRY_LOCK_ROW_FAILED);
	d1.joinSyncPoint(SP_HEAP_FLR_INSERT_UNLOCK_PAGE_AFTER_TRY_LOCK_ROW_FAILED);

	/* 释放行锁 */
	session->unlockRow(&rowLockHdl);

	/* 插入一条记录，占住Depp的rowlock的slot */
	m_heap->insert(session, rec, NULL);

	d1.notifySyncPoint(SP_HEAP_FLR_INSERT_UNLOCK_PAGE_AFTER_TRY_LOCK_ROW_FAILED);
	/* d1会重新取得slotNum，然后再次尝试加锁 */
	d1.joinSyncPoint(SP_HEAP_FLR_INSERT_BEFORE_TRY_LOCK_ROW);

	/* 我们再次阻挠d1通过try取得行锁 */
	rowLockHdl = LOCK_ROW(session, m_tblDef->m_id, RID(1, recPerPage - 2), Exclusived);
	/* 让d1尝试加锁失败 */
	d1.notifySyncPoint(SP_HEAP_FLR_INSERT_BEFORE_TRY_LOCK_ROW);
	d1.joinSyncPoint(SP_HEAP_FLR_INSERT_TRY_LOCK_ROW_FAILED);

	/* 释放行锁 */
	session->unlockRow(&rowLockHdl);

	d1.notifySyncPoint(SP_HEAP_FLR_INSERT_TRY_LOCK_ROW_FAILED);
	d1.joinSyncPoint(SP_HEAP_FLR_INSERT_UNLOCK_PAGE_AFTER_TRY_LOCK_ROW_FAILED);

	/* 插入，将第一页占满 */
	for (i = 0; i < recPerPage - 1; ++i) {
		rid = m_heap->insert(session, rec, NULL);
	}

	d1.notifySyncPoint(SP_HEAP_FLR_INSERT_UNLOCK_PAGE_AFTER_TRY_LOCK_ROW_FAILED);
	d1.joinSyncPoint(SP_HEAP_FLR_INSERT_No_Free_Slot_After_Relock);

	d1.notifySyncPoint(SP_HEAP_FLR_INSERT_No_Free_Slot_After_Relock);
	d1.joinSyncPoint(SP_HEAP_FLR_INSERT_BEFORE_REVERSE_LOCK_HEADER_PAGE);

	d1.notifySyncPoint(SP_HEAP_FLR_INSERT_BEFORE_REVERSE_LOCK_HEADER_PAGE);
	d1.joinSyncPoint(SP_HEAP_FLR_INSERT_BEFORE_TRY_LOCK_ROW);

	/* 放行d1 */
	d1.notifySyncPoint(SP_HEAP_FLR_INSERT_BEFORE_TRY_LOCK_ROW);

	((HeapHeaderPageInfo *)m_heap->getHeaderPage())->m_maxUsed = m_heap->getMaxUsedPageNum() - 1;
	m_heap->flush(session);

	m_db->getSessionManager()->freeSession(session);
	freeRecord(rec);
}

/** 
 * 测试get过程
 *		流程：	1. 基本功能测试
 *				2. 代码分支覆盖测试（内容见内部具体注释）
 */
void FLRHeapTestCase::testGet() {
	u64 startTime, endTime;
	bool canRead;
	RowId rid, wrongPage, wrongSlot;
	u64 pageNum, tmpPgN = 0;
	s16 slotNum;
	int recPerPage = 0;
	Record readOut;
	Record *rec;
	byte *data = (byte *)System::virtualAlloc(Limits::PAGE_SIZE);
	readOut.m_data = data;
	readOut.m_format = REC_FIXLEN;
	//u64 rows = ((FixedLengthRecordHeap *)m_heap)->getRecPerPage() * (DrsHeap::HEAP_INCREASE_SIZE + 1);
	u64 rows = ((FixedLengthRecordHeap *)m_heap)->getRecPerPage() * (m_tblDef->m_incrSize + 1);

	CPPUNIT_ASSERT(m_heap->getStatus().m_dboStats->m_statArr[DBOBJ_ITEM_READ] == 0 && m_heap->getStatus().m_rowsReadRecord == 0 && m_heap->getStatus().m_rowsReadSubRec == 0);

	for (int i = 0; i < rows;) { // 顺序get测试
		rid = insertRecord(++i, &rec, m_db, m_conn, m_heap, m_tblDef);
		if (tmpPgN == 0) tmpPgN = RID_GET_PAGE(rid);
		if (tmpPgN != 0 && tmpPgN != RID_GET_PAGE(rid) && !recPerPage)
			recPerPage = i - 1;
		pageNum = RID_GET_PAGE(rid);
		slotNum = RID_GET_SLOT(rid);
		wrongPage = RID(pageNum + 1, slotNum);
		wrongSlot = RID(pageNum, slotNum + 1);
		canRead = readRecord(rid, &readOut, m_db, m_conn, m_heap);
		CPPUNIT_ASSERT(canRead);
		CPPUNIT_ASSERT(readOut.m_rowId == rid);
		CPPUNIT_ASSERT(readOut.m_format == rec->m_format);
		CPPUNIT_ASSERT(readOut.m_size == rec->m_size);
		CPPUNIT_ASSERT(0 == memcmp(readOut.m_data, rec->m_data, readOut.m_size));
		// wrong page should fail.
		canRead = readRecord(wrongPage, &readOut, m_db, m_conn, m_heap);
		CPPUNIT_ASSERT(!canRead);
		// wrong slot should fail either.
		canRead = readRecord(wrongSlot, &readOut, m_db, m_conn, m_heap);
		CPPUNIT_ASSERT(!canRead);
		freeRecord(rec);
	}
	CPPUNIT_ASSERT(m_heap->getStatus().m_rowsReadRecord == rows);
	//cout<<"Start one hundred thousand get testing..."<<endl;
	startTime = System::currentTimeMillis();
	for (int i = 0; i < rows; ++i) { // get测试
		//cout<<"i is "<<i<<endl;
		pageNum = i / recPerPage + 1;
		slotNum = i % recPerPage;
		rid = RID(pageNum, slotNum);
		canRead = readRecord(rid, &readOut, m_db, m_conn, m_heap);
		CPPUNIT_ASSERT(canRead);
	}
	endTime = System::currentTimeMillis();
	//cout<<"One hundred thousand record get takes time "<<endTime - startTime<<" milliseconds."<<endl;

	SubRecordBuilder compBuilder(m_tblDef, REC_REDUNDANT);
	SubRecord *subRec = compBuilder.createEmptySbById(Limits::PAGE_SIZE, "0 2 3");

	for (int i = 0; i < rows; ++i) { // 失败 SubRecord get测试
		if (i%2) {
			if (i%4) pageNum = 0;
			else pageNum = (u64)-1;
			slotNum = i % recPerPage;
		}
		else {
			pageNum = i / recPerPage + 1;
			slotNum = recPerPage;
		}
		rid = RID(pageNum, slotNum);
		canRead = readSubRecord(rid, subRec, m_db, m_conn, m_heap, m_tblDef);
		CPPUNIT_ASSERT(!canRead);
	}


	startTime = System::currentTimeMillis();
	for (int i = 0; i < rows; ++i) { // SubRecord get测试
		pageNum = i / recPerPage + 1;
		slotNum = i % recPerPage;
		rid = RID(pageNum, slotNum);
		canRead = readSubRecord(rid, subRec, m_db, m_conn, m_heap, m_tblDef);
		CPPUNIT_ASSERT(canRead);
	}
	endTime = System::currentTimeMillis();
	CPPUNIT_ASSERT(m_heap->getStatus().m_rowsReadSubRec == rows);
	//cout<<"One hundred thousand sub record get takes time "<<endTime - startTime<<" milliseconds."<<endl;

	/*** 带行锁的get测试 ***/
	tearDown(); setUp();


	vector<RowId> ridVec;
	for (int i = 0; i < rows;) {
		rid = insertRecord(++i, &rec, m_db, m_conn, m_heap, m_tblDef);
		ridVec.push_back(rid);
		freeRecord(rec);
	}

	RowLockHandle *rlockHdl = NULL;
	Session *session = m_db->getSessionManager()->allocSession("FLRHeapTestCase::testGet", m_conn);
	for (uint i = 0; i < ridVec.size(); ++i) {
		canRead = m_heap->getRecord(session, ridVec[i], &readOut, Exclusived, &rlockHdl);
		CPPUNIT_ASSERT(canRead);
		CPPUNIT_ASSERT(rlockHdl);
		CPPUNIT_ASSERT(rlockHdl->getRid() == ridVec[i]);
		if (rlockHdl) session->unlockRow(&rlockHdl);
	}
	SubrecExtractor *srExtractor = SubrecExtractor::createInst(session, m_tblDef, subRec);
	for (uint i = 0; i < ridVec.size(); ++i) {
		canRead = m_heap->getSubRecord(session, ridVec[i], srExtractor, subRec, Exclusived, &rlockHdl);
		CPPUNIT_ASSERT(canRead);
		CPPUNIT_ASSERT(rlockHdl);
		CPPUNIT_ASSERT(rlockHdl->getRid() == ridVec[i]);
		if (rlockHdl) session->unlockRow(&rlockHdl);
	}
	m_heap->del(session, ridVec[0]);
	canRead = m_heap->getRecord(session, ridVec[0], &readOut, Exclusived, &rlockHdl);
	CPPUNIT_ASSERT(!canRead);
	CPPUNIT_ASSERT(!rlockHdl);
	canRead = m_heap->getSubRecord(session, ridVec[0], srExtractor, subRec, Exclusived, &rlockHdl);
	CPPUNIT_ASSERT(!canRead);
	CPPUNIT_ASSERT(!rlockHdl);
	m_db->getSessionManager()->freeSession(session);

	/* 释放资源 */
    freeSubRecord(subRec);
	System::virtualFree(data);
}


/** 
 * 测试del过程
 *		流程：	1. 基本功能测试
 *				2. 代码分支覆盖测试（内容见内部具体注释）
 */
void FLRHeapTestCase::testDel() {
	u64 startTime, endTime;
	bool canDel;
	RowId tmpRid, rid = 0;
	u64 pageNum = 0;
	s16 slotNum;
	int recPerPage = 0;
	int freeSlotReserved = 0;
	Record readOut;
	Record *rec;
	byte *data = (byte *)System::virtualAlloc(Limits::PAGE_SIZE);
	readOut.m_data = data;
	u64 rows = ((FixedLengthRecordHeap *)m_heap)->getRecPerPage() * (m_tblDef->m_incrSize + 1);

	CPPUNIT_ASSERT(m_heap->getStatus().m_dboStats->m_statArr[DBOBJ_ITEM_DELETE] == 0);

	for (int i = 0; i < 500;) { // 插入后马上Delete
		tmpRid = insertRecord(++i, &rec, m_db, m_conn, m_heap, m_tblDef);
		if (rid == 0) rid = tmpRid;
		CPPUNIT_ASSERT(rid == tmpRid);
		canDel = deleteRecord(rid, m_db, m_conn, m_heap);
		CPPUNIT_ASSERT(canDel);
		freeRecord(rec);
	}
	CPPUNIT_ASSERT(m_heap->getStatus().m_dboStats->m_statArr[DBOBJ_ITEM_DELETE] == 500);
	for (int i = 0; i < 500;) { // 测试del后的页面保留空间
		rid = insertRecord(++i, &rec, m_db, m_conn, m_heap, m_tblDef);
		if (pageNum == 0) pageNum = RID_GET_PAGE(rid);
		if (pageNum != 0 && pageNum != RID_GET_PAGE(rid) && !recPerPage) {
			recPerPage = i - 1;
			canDel = deleteRecord(rid, m_db, m_conn, m_heap);
			CPPUNIT_ASSERT(canDel);
			freeRecord(rec);
			break; // 至此第一页全满
		}
		freeRecord(rec);
	}
	//cout<<"Max records count per page is "<<recPerPage<<"."<<endl;
	for (int i = 0; i < recPerPage; ++i) {
		//cout<<"i is "<<i<<endl;
		tmpRid = RID(pageNum, i);
		canDel = deleteRecord(tmpRid, m_db, m_conn, m_heap);
		CPPUNIT_ASSERT(canDel);
		tmpRid = insertRecord(i, &rec, m_db, m_conn, m_heap, m_tblDef);
		if (RID_GET_PAGE(tmpRid) == pageNum && !freeSlotReserved)
			freeSlotReserved = i + 1;
		canDel = deleteRecord(tmpRid, m_db, m_conn, m_heap);
		freeRecord(rec);
	}
	//cout<<"Preserved free slots count is "<<freeSlotReserved<<"."<<endl;

	startTime = System::currentTimeMillis();
	for (int i = 0; i < rows; ++i) { // 失败删除测试
		pageNum = i / recPerPage + 1;
		slotNum = i % recPerPage;
		canDel = deleteRecord(RID(pageNum, slotNum), m_db, m_conn, m_heap);
		CPPUNIT_ASSERT(!canDel);
	}
	endTime = System::currentTimeMillis();
	//cout<<"One hundred thousand failed del takes time "<<endTime - startTime<<" milliseconds."<<endl;

	vector<RowId> ridVec;
	for (int i = 0; i < rows; ++i) {
		rid = insertRecord(i, &rec, m_db, m_conn, m_heap, m_tblDef);
		ridVec.push_back(rid);
		freeRecord(rec);
	}

	vector<RowId>::iterator iter = ridVec.begin();
	startTime = System::currentTimeMillis();
	for (; iter != ridVec.end(); ++iter) { // 十万成功删除测试
		canDel = deleteRecord(*iter, m_db, m_conn, m_heap);
		CPPUNIT_ASSERT(canDel);
	}
	endTime = System::currentTimeMillis();
	///cout<<"One hundred thousand successful del takes time "<<endTime - startTime<<" milliseconds."<<endl;

	System::virtualFree(data);
}

/** 
 * 测试update过程
 *		流程：	1. 基本功能测试（Record update和SubRecord update）
 *				2. 代码分支覆盖测试（内容见内部具体注释）
 */
void FLRHeapTestCase::testUpdate() {
	RowId rid;
	u64 pageNum;
	CPPUNIT_ASSERT(m_heap);
	int i = 0;
	pageNum = 0;
	Record *rec;
	Record readOut1, readOut2;
	byte *data = (byte *)System::virtualAlloc(Limits::PAGE_SIZE * 2);
	readOut1.m_data = data; readOut2.m_data = data + Limits::PAGE_SIZE;
	readOut1.m_format = readOut2.m_format = REC_FIXLEN;
	bool canRead, canUpdate;
	//u64 rows = ((FixedLengthRecordHeap *)m_heap)->getRecPerPage() * (DrsHeap::HEAP_INCREASE_SIZE + 1);
	u64 rows = ((FixedLengthRecordHeap *)m_heap)->getRecPerPage() * (m_tblDef->m_incrSize + 1);

	// 计时
	vector<RowId> ridVec;
	for (i = 0; i < rows;) { // 记录顺序插入测试
		rid = insertRecord(++i, &rec, m_db, m_conn, m_heap, m_tblDef);
		freeRecord(rec);
		CPPUNIT_ASSERT(rid);
		ridVec.push_back(rid);
	}

	vector<SubRecord *> subRecVec; 
	SubRecordBuilder compBuilder(m_tblDef, REC_REDUNDANT);
	SubRecord *subRec;
	for (uint i = 0; i < ridVec.size(); ++i) {
		u64 j = i + 10;
		u32 k = i * 41;
		subRec = compBuilder.createSubRecordByName("UserId Balance", &j, &k);
		subRecVec.push_back(subRec);
	}

	for (uint i = 0; i < ridVec.size(); ++i) {
		canRead = readRecord(ridVec[i], &readOut1, m_db, m_conn, m_heap);
		CPPUNIT_ASSERT(canRead);
		RecordOper::updateRecordFR(m_tblDef, &readOut1, subRecVec[i]);
		canUpdate = updateRecord(ridVec[i], subRecVec[i], m_db, m_conn, m_heap);
		CPPUNIT_ASSERT(canUpdate);
		canRead = readRecord(ridVec[i], &readOut2, m_db, m_conn, m_heap);
		CPPUNIT_ASSERT(canRead);
		CPPUNIT_ASSERT(readOut1.m_rowId == readOut2.m_rowId);
		CPPUNIT_ASSERT(readOut1.m_format == readOut2.m_format);
		CPPUNIT_ASSERT(readOut1.m_size == readOut2.m_size);
		CPPUNIT_ASSERT(0 == memcmp(readOut1.m_data, readOut2.m_data, readOut1.m_size));
	}
	CPPUNIT_ASSERT(m_heap->getStatus().m_rowsUpdateSubRec == ridVec.size());

	/** 失败测试 */
	canUpdate = updateRecord(RID((u64)-1, 0), subRecVec[1], m_db, m_conn, m_heap);
	CPPUNIT_ASSERT(!canUpdate);
	canUpdate = updateRecord(RID(1,((FixedLengthRecordHeap *)m_heap)->getRecPerPage()), subRecVec[1], m_db, m_conn, m_heap);
	CPPUNIT_ASSERT(!canUpdate);
	canUpdate = updateRecord(RID(m_heap->getMaxPageNum() + 1, 0), subRecVec[1], m_db, m_conn, m_heap);
	CPPUNIT_ASSERT(!canUpdate);

	for (uint i = 0; i < subRecVec.size(); ++i) {
		freeSubRecord(subRecVec[i]);
	}


	/* Record update 测试 */
	Record *newRec = createRecord(999, 999, "Connie Cong", 999, 999, m_tblDef);
	//cout<<"newRec->m_format is "<<newRec->m_format<<endl;
	for (uint i = 0; i < ridVec.size(); ++i) {
		canRead = readRecord(ridVec[i], &readOut1, m_db, m_conn, m_heap);
		CPPUNIT_ASSERT(canRead);
		canUpdate = updateRecord(ridVec[i], newRec, m_db, m_conn, m_heap);
		CPPUNIT_ASSERT(canUpdate);
		canRead = readRecord(ridVec[i], &readOut2, m_db, m_conn, m_heap);
		CPPUNIT_ASSERT(canRead);
		CPPUNIT_ASSERT(newRec->m_format == readOut2.m_format);
		CPPUNIT_ASSERT(newRec->m_size == readOut2.m_size);
		CPPUNIT_ASSERT(0 == memcmp(newRec->m_data, readOut2.m_data, newRec->m_size));
	}
	/** 失败测试 */
	canUpdate = updateRecord(RID((u64)-1, 0), newRec, m_db, m_conn, m_heap);
	CPPUNIT_ASSERT(!canUpdate);
	canUpdate = updateRecord(RID(1,((FixedLengthRecordHeap *)m_heap)->getRecPerPage()), newRec, m_db, m_conn, m_heap);
	CPPUNIT_ASSERT(!canUpdate);
	canUpdate = updateRecord(RID(m_heap->getMaxPageNum() + 1, 0), newRec, m_db, m_conn, m_heap);
	CPPUNIT_ASSERT(!canUpdate);

	freeRecord(newRec);
	System::virtualFree(data);
}

void FLRHeapTestCase::testHash() {
	/*
	const int size = 50000;
	Hash<SampleID, Sample *> hashTbl(size);
	Sample *samples[size];
	for (int i = 0; i < size; ++i) {
		samples[i] = new Sample(2);
		samples[i]->m_ID = 2 * i + 1;
		hashTbl.put(samples[i]->m_ID, samples[i]);
	}
	for (int i = 0; i < size; ++i) {
		SampleID odd, even;
		odd = 2 * i + 1;
		even = 2 * i;
		Sample *sid;
		sid = hashTbl.get(even);
		assert(sid == (Sample *)0);
		sid = hashTbl.get(odd);
		assert(odd);
		assert(sid->m_ID == odd);
		delete sid;
		hashTbl.remove(odd);
		sid = hashTbl.get(odd);
		assert(sid == (Sample *)0);
	}
	*/
}

void FLRHeapTestCase::testSample() {
	u64 firstPage = 1;
	int recPerPage = 0;
	u64 maxPage = firstPage + 500;
	Session *session = m_db->getSessionManager()->allocSession("FLRHeapTestCase::testSample", m_conn);
	Record *rec = createRecord(0, 130, "test record", 1<<17, 1234, m_tblDef);

	/* 基本堆信息 */
	CPPUNIT_ASSERT(m_heap->getStatus().m_dboStats->m_statArr[DBOBJ_ITEM_INSERT] == 0
		&& m_heap->getStatus().m_dboStats->m_statArr[DBOBJ_ITEM_DELETE] == 0
		&& m_heap->getStatus().m_dboStats->m_statArr[DBOBJ_ITEM_UPDATE] == 0);


	/* 零插入采样 */
	m_heap->updateExtendStatus(session, 1000);
	m_heap->updateExtendStatus(session, 4);
	const HeapStatusEx& statusEx = m_heap->getStatusEx();
	assert(statusEx.m_numLinks == 0 && statusEx.m_numRows == 0 && statusEx.m_pctUsed == .0);

	/* 少量插入采样 */
	NTSE_ASSERT(m_heap->insert(session, rec, NULL)); ++recPerPage;
	NTSE_ASSERT(m_heap->insert(session, rec, NULL)); ++recPerPage;
	m_heap->updateExtendStatus(session, 16);
	m_heap->updateExtendStatus(session, 4);


	while (true) {
		RowId rid = m_heap->insert(session, rec, NULL);
		if (RID_GET_PAGE(rid) == firstPage) ++recPerPage;
		if (RID_GET_PAGE(rid) == maxPage) break;
	}
	freeRecord(rec);

	//cout << "rec per page is " << recPerPage << endl;
        /*
	int delCnt = recPerPage * (int)(maxPage - firstPage) / 5;
	for (int i = 0; i < delCnt; ++i) {
		RowId rid = RID((randomInt(maxPage) + firstPage), (randomInt(recPerPage)));
		bool success = m_heap->del(session, rid);
                if (!success) delCnt--;
	}
        */
        

	SubRecordBuilder compBuilder(m_tblDef, REC_REDUNDANT);
	SubRecord *subRec = compBuilder.createEmptySbById(Limits::PAGE_SIZE, "0 2 3");


	RowLockHandle *rlh = NULL;
	DrsHeapScanHandle *delHdl = m_heap->beginScan(session, SubrecExtractor::createInst(session, m_tblDef, subRec), Exclusived, &rlh, false);
	while (m_heap->getNext(delHdl, subRec)) {
		if (randomInt(recPerPage / (Limits::PAGE_SIZE / 1024 * 2)) == 0) {
			m_heap->deleteCurrent(delHdl);
		}
		session->unlockRow(&rlh);
	}
	m_heap->endScan(delHdl);


	int recCnt = 0;
	DrsHeapScanHandle *scanhdl = m_heap->beginScan(session, SubrecExtractor::createInst(session, m_tblDef, subRec), None, NULL, false);
	while (m_heap->getNext(scanhdl, subRec)) {
		++recCnt;
	}
	m_heap->endScan(scanhdl);



	freeSubRecord(subRec);


	cout << "rec per page is " << recPerPage << endl;
	SampleResult *result = SampleAnalyse::sampleAnalyse(session, m_heap, 1024 * 8, 50);
	cout << "field num is " << result->m_numFields << endl;
	cout << "sample num is " << result->m_numSamples << endl;
	cout << "average is " << result->m_fieldCalc[0].m_average << endl;
	cout << "delta is " << result->m_fieldCalc[0].m_delta << endl;
	delete result;

	cout << "total page num is " << m_heap->getMaxUsedPageNum() << endl;
	cout << "total rec num is " << recCnt << endl;
	cout << "rec per page is " << (double)recCnt / (double)m_heap->getMaxUsedPageNum() << endl;

	CPPUNIT_ASSERT(m_heap->getStatus().m_dboStats->m_statArr[DBOBJ_ITEM_INSERT] > 0
		&& m_heap->getStatus().m_dboStats->m_statArr[DBOBJ_ITEM_DELETE] > 0
		&& m_heap->getStatus().m_dboStats->m_statArr[DBOBJ_ITEM_UPDATE] == 0);

	m_db->getSessionManager()->freeSession(session);
}

/** 
 * 测试表扫描过程
 *		流程：	1. 基本功能测试
 *				2. 代码分支覆盖测试（内容见内部具体注释）
 */
void FLRHeapTestCase::testTableScan() {
	Session *session;
	RowId rid, rid2del;
	SubRecordBuilder compBuilder(m_tblDef, REC_REDUNDANT);
	SubRecord *subRec = compBuilder.createEmptySbById(Limits::PAGE_SIZE, "0");
	SubRecord *subRec2 = compBuilder.createEmptySbById(Limits::PAGE_SIZE, "0");

	CPPUNIT_ASSERT(m_heap);
	char name[50];
	Record *rec;
	int recPerPage = (int)((FixedLengthRecordHeap *)m_heap)->getRecPerPage();
	//u64 rows = recPerPage * (DrsHeap::HEAP_INCREASE_SIZE + 1);
	u64 rows = recPerPage * (m_tblDef->m_incrSize + 1);

	Record readOut;
	byte *data = (byte *)System::virtualAlloc(Limits::PAGE_SIZE * 2);
	readOut.m_data = data;
	readOut.m_format = REC_FIXLEN;
	Record readOut2;
	readOut2.m_data = data + Limits::PAGE_SIZE;
	readOut2.m_format = REC_FIXLEN;

	bool canRead;
	vector<Record *> recVec;

	for (int i = 0; i < rows; ++i) {
		sprintf(name, "Kenneth Tse %d\0", i +1);
		rec = createRecord(0, 130 + i, name, (i+1)<<17, ((u32)-1) - i, m_tblDef);
		recVec.push_back(rec);
	}
	CPPUNIT_ASSERT(recVec.size() == rows);

	session = m_db->getSessionManager()->allocSession("FLRHeapTestCase::testTableScan", m_conn);
	for (int i = 0 ; i < rows; ++i) {
		rec = recVec[i];
		EXCPT_OPER(rid = m_heap->insert(session, rec, NULL));
		rec->m_rowId = rid;
		CPPUNIT_ASSERT(rid > 0);
		EXCPT_OPER(canRead = m_heap->getRecord(session, rid, &readOut));
		CPPUNIT_ASSERT(canRead);
		CPPUNIT_ASSERT(readOut.m_rowId == rec->m_rowId);
		CPPUNIT_ASSERT(readOut.m_format == rec->m_format);
		CPPUNIT_ASSERT(readOut.m_size == rec->m_size);
		CPPUNIT_ASSERT(0 == memcmp(readOut.m_data, rec->m_data, readOut.m_size));
		
		if (i == (((rows/2) - (rows/2)%recPerPage) - 1)) {
			rid2del = rid;
			//cout<<"rid2del is "<<rid2del<<endl;
		}
	}
	m_db->getSessionManager()->freeSession(session);


	// 删掉一行
	session = m_db->getSessionManager()->allocSession("FLRHeapTestCase::testTableScan", m_conn);
	m_heap->del(session, rid2del);
	m_db->getSessionManager()->freeSession(session);

	// 现在用getNext，每次读出都要验证的
	int j = 0;
	session = m_db->getSessionManager()->allocSession("FLRHeapTestCase::testTableScan", m_conn);
	DrsHeapScanHandle *scanhdl = m_heap->beginScan(session, SubrecExtractor::createInst(session, m_tblDef, subRec), None, NULL, false);
	while (m_heap->getNext(scanhdl, subRec)) {
		RecordOper::extractSubRecordFR(m_tblDef, recVec[j], subRec2);
		if (subRec2->m_rowId == rid2del) {
			RecordOper::extractSubRecordFR(m_tblDef, recVec[++j], subRec2);
		}
		CPPUNIT_ASSERT(subRec->m_size == subRec2->m_size);
		CPPUNIT_ASSERT(subRec->m_rowId == subRec2->m_rowId);
		CPPUNIT_ASSERT(subRec->m_numCols == subRec2->m_numCols);
		CPPUNIT_ASSERT(0 == memcmp(subRec->m_columns, subRec2->m_columns, sizeof(u16) * subRec->m_numCols));
		//CPPUNIT_ASSERT(0 == memcmp(subRec->m_data, subRec2->m_data, subRec->m_size));
		CPPUNIT_ASSERT(RecordOper::isSubRecordEq(m_tblDef, subRec, subRec2));

		++j;
	}
	CPPUNIT_ASSERT(j == rows);

	m_heap->endScan(scanhdl);
	m_db->getSessionManager()->freeSession(session);

	CPPUNIT_ASSERT(m_heap->getStatus().m_dboStats->m_statArr[DBOBJ_SCAN] == 1);
	CPPUNIT_ASSERT(m_heap->getStatus().m_dboStats->m_statArr[DBOBJ_SCAN_ITEM] == j - 1);


	//////// 截断堆，重新来
	tearDown(); setUp();

	/*** 以下测试带lock的getNext ***/

	session = m_db->getSessionManager()->allocSession("FLRHeapTestCase::testTableScan", m_conn);
	for (int i = 0 ; i < rows; ++i) {
		rec = recVec[i];
		EXCPT_OPER(rid = m_heap->insert(session, rec, NULL));
		rec->m_rowId = rid;
		CPPUNIT_ASSERT(rid > 0);
	}
	m_db->getSessionManager()->freeSession(session);

	j = 0;
	Michael *m1 = NULL;
	u64 nextPageNum;
	s16 slotNum;
	session = m_db->getSessionManager()->allocSession("FLRHeapTestCase::testTableScan", m_conn);
	RowLockHandle *rowLockHdl = NULL;
	SubrecExtractor *srExtractor = SubrecExtractor::createInst(session, m_tblDef, subRec2);
	scanhdl = m_heap->beginScan(session, SubrecExtractor::createInst(session, m_tblDef, subRec), Shared, &rowLockHdl, false);
	while (m_heap->getNext(scanhdl, subRec)) {
		//cout<<"rowLockHdl is "<<(int)rowLockHdl<<", j is "<<j<<endl;
		RecordOper::extractSubRecordFR(m_tblDef, recVec[j], subRec2);
		CPPUNIT_ASSERT(subRec->m_size == subRec2->m_size);
		CPPUNIT_ASSERT(subRec->m_rowId == subRec2->m_rowId);
		CPPUNIT_ASSERT(subRec->m_numCols == subRec2->m_numCols);
		CPPUNIT_ASSERT(0 == memcmp(subRec->m_columns, subRec2->m_columns, sizeof(u16) * subRec->m_numCols));
		CPPUNIT_ASSERT(0 == memcmp(subRec->m_data, subRec2->m_data, subRec->m_size));
		CPPUNIT_ASSERT(rowLockHdl);
		if (j == rows / 2) {
			nextPageNum = RID_GET_PAGE(rowLockHdl->getRid()) + 1;
			slotNum = 0;
			m1 = new Michael(m_db, m_tblDef->m_id, RID(nextPageNum, slotNum), Exclusived);

			m1->start();
			Thread::msleep(1000);
		} else if (j == rows * 3 / 4) {
			/*** deleteCurrent 测试 ***/
			rid = rowLockHdl->getRid();
			EXCPT_OPER(m_heap->deleteCurrent(scanhdl));
			canRead = m_heap->getSubRecord(session, rid, srExtractor, subRec2);
			CPPUNIT_ASSERT(!canRead);
		} else 	if (j == rows * 5 / 6) {
			/*** updateCurrent 测试 ***/
			rid = rowLockHdl->getRid();
			canRead = m_heap->getRecord(session, rid, &readOut);
			CPPUNIT_ASSERT(canRead);
			EXCPT_OPER(m_heap->updateCurrent(scanhdl, subRec2));
			canRead = m_heap->getRecord(session, rid, &readOut2);
			CPPUNIT_ASSERT(canRead);
		}
		session->unlockRow(&rowLockHdl);
		++j;
	}
	CPPUNIT_ASSERT(j == rows);
	if (m1) {
          m1->join();
          delete m1;
        }
	m_heap->endScan(scanhdl);
	m_db->getSessionManager()->freeSession(session);

	for (uint i = 0; i < recVec.size(); ++i) {
		freeRecord(recVec[i]);
	}

	/** 测试加行锁后记录不存在了的情况*/
	tearDown();setUp();
	session = m_db->getSessionManager()->allocSession("FLRHeapTestCase::testTableScan", m_conn);
	for (int i = 0; i < 1; ++i) {
		sprintf(name, "Kenneth Tse %d\0", i +1);
		rec = createRecord(0, 130 + i, name, (i+1)<<17, ((u32)-1) - i, m_tblDef);
		rid = m_heap->insert(session, rec, NULL);
		delete [] rec->m_data;
		delete rec;
		rec = NULL;
	}


	Sergey sg1(m_db, m_conn, m_tblDef, m_heap, true, 1);
	sg1.enableSyncPoint(SP_HEAP_FLR_BEFORE_UNLOCKROW);
	sg1.start();
	sg1.joinSyncPoint(SP_HEAP_FLR_BEFORE_UNLOCKROW);

	Sergey sg2(m_db, m_conn, m_tblDef, m_heap, true);
	sg2.enableSyncPoint(SP_HEAP_FLR_AFTER_TRYLOCK_UNLOCKPAGE);
	sg2.start();
	sg2.joinSyncPoint(SP_HEAP_FLR_AFTER_TRYLOCK_UNLOCKPAGE);

	sg1.notifySyncPoint(SP_HEAP_FLR_BEFORE_UNLOCKROW);

	sg2.notifySyncPoint(SP_HEAP_FLR_AFTER_TRYLOCK_UNLOCKPAGE);



	sg2.join();
	sg1.join();


	m_db->getSessionManager()->freeSession(session);

	
	freeSubRecord(subRec);
	freeSubRecord(subRec2);
	System::virtualFree(data);
}

/** 
 * 测试页面LSN工作是否正常
 *		流程：	1. 构造若干顺序操作
 *				2. 确认被操作页面的lsn是顺序的
 */
void FLRHeapTestCase::testLSN() {
	bool canRead, canUpdate, canDel;
	RowId rid;
	u64 tmpPgN = 0;
	s16 slotNum;
	int recPerPage = 0;
	byte *buf = (byte *)System::virtualAlloc(Limits::PAGE_SIZE);
	Record readOut;
	Record *rec;
	byte *data = (byte *)System::virtualAlloc(Limits::PAGE_SIZE);
	readOut.m_data = data;
	readOut.m_format = REC_FIXLEN;
	//u64 rows = ((FixedLengthRecordHeap *)m_heap)->getRecPerPage() * (DrsHeap::HEAP_INCREASE_SIZE + 1);
	u64 rows = ((FixedLengthRecordHeap *)m_heap)->getRecPerPage() * (m_tblDef->m_incrSize + 1);

	for (int i = 0; i < rows;) { // 顺序get测试
		rid = insertRecord(++i, &rec, m_db, m_conn, m_heap, m_tblDef);
		freeRecord(rec);
	}

	Session *session;

	session = m_db->getSessionManager()->allocSession("FLRHeapTestCase::testLSN", m_conn);
	u64 oldLSN = 0, LSN = 0;
	for (int i = 1; i <= m_heap->getMaxUsedPageNum(); ++i) {
		LSN = m_heap->getPageLSN(session, i, m_heap->getDBObjStats());
		CPPUNIT_ASSERT(LSN > oldLSN);
		//cout<<"LSN is "<<LSN<<endl;
		oldLSN = LSN;
	}
	m_db->getSessionManager()->freeSession(session);

	// get 不改变LSN
	SubRecordBuilder compBuilder(m_tblDef, REC_REDUNDANT);
	SubRecord *subRec = compBuilder.createEmptySbById(Limits::PAGE_SIZE, "0");

	session = m_db->getSessionManager()->allocSession("FLRHeapTestCase::testLSN", m_conn);
	oldLSN = m_heap->getPageLSN(session, 1, m_heap->getDBObjStats());
	SubrecExtractor *srExtractor = SubrecExtractor::createInst(session, m_tblDef, subRec);
	canRead = m_heap->getSubRecord(session, RID(1, 0), srExtractor, subRec);
	CPPUNIT_ASSERT(canRead);

	LSN = m_heap->getPageLSN(session, 1, m_heap->getDBObjStats());
	CPPUNIT_ASSERT(LSN == oldLSN);
	//cout<<"LSN is "<<LSN<<endl;

	canUpdate = m_heap->update(session, RID(1, 1), subRec);
	CPPUNIT_ASSERT(canUpdate);
	LSN = m_heap->getPageLSN(session, 1, m_heap->getDBObjStats());
	CPPUNIT_ASSERT(LSN > oldLSN);
	oldLSN = LSN;
	//cout<<"LSN is "<<LSN<<endl;

	backupPage(session, m_heap, 1, buf);

	canDel = m_heap->del(session, RID(1, 2));
	CPPUNIT_ASSERT(canDel);
	LSN = m_heap->getPageLSN(session, 1, m_heap->getDBObjStats());
	CPPUNIT_ASSERT(LSN > oldLSN);
	//cout<<"LSN is "<<LSN<<endl;

	// 测试页面恢复
	restorePage(session, m_heap, 1, buf);
	LSN = m_heap->getPageLSN(session, 1, m_heap->getDBObjStats());
	CPPUNIT_ASSERT(LSN == oldLSN);
	//cout<<"LSN is "<<LSN<<endl;

	m_db->getSessionManager()->freeSession(session);


	do {
		rid = insertRecord(1000, &rec, m_db, m_conn, m_heap, m_tblDef);
		slotNum = RID_GET_SLOT(rid);
		freeRecord(rec);
	} while (slotNum != ((FixedLengthRecordHeap *)m_heap)->getRecPerPage() - 2);

	session = m_db->getSessionManager()->allocSession("FLRHeapTestCase::testLSN", m_conn);
	oldLSN = m_heap->getPageLSN(session, 0, m_heap->getDBObjStats());
	m_db->getSessionManager()->freeSession(session);

	rid = insertRecord(1001, &rec, m_db, m_conn, m_heap, m_tblDef);
	freeRecord(rec);

	session = m_db->getSessionManager()->allocSession("FLRHeapTestCase::testLSN", m_conn);
	LSN = m_heap->getPageLSN(session, 0, m_heap->getDBObjStats());
	m_db->getSessionManager()->freeSession(session);

	CPPUNIT_ASSERT(LSN > oldLSN);
	
	freeSubRecord(subRec);
	System::virtualFree(buf);
	System::virtualFree(data);
}

/** 
 * 测试redoCreate过程
 *		流程：	1. 构造redoCreate的各种情形
 *				2. 调用redoCreate
 *				3. 查看redo的结果是否如同预期
 */
void FLRHeapTestCase::testRedoCreate() {
	/* clone tabel definition. */
	TableDef *tableDef;
	tableDef = new TableDef(m_tblDef);

	closeSmallHeap();
	File f(SMALL_HEAP);
	bool exist;
	f.isExist(&exist);
	CPPUNIT_ASSERT(exist);
	u64 errCode = f.remove();
	//cout<<"err code is "<<errCode<<endl;
	f.isExist(&exist);
	CPPUNIT_ASSERT(!exist);


	Session *s = m_db->getSessionManager()->allocSession("FLRHeapTestCase::testOpen", m_conn);
	EXCPT_OPER(DrsHeap::redoCreate(m_db, s, SMALL_HEAP, tableDef));
	m_db->getSessionManager()->freeSession(s);

	s = m_db->getSessionManager()->allocSession("FLRHeapTestCase::testOpen", m_conn);
	EXCPT_OPER(m_heap = DrsHeap::open(m_db, s, SMALL_HEAP, tableDef));
	m_db->getSessionManager()->freeSession(s);
	CPPUNIT_ASSERT(DrsHeap::getVersionFromTableDef(tableDef) == HEAP_VERSION_FLR);



	// 文件在使用中
	s = m_db->getSessionManager()->allocSession("FLRHeapTestCase::testOpen", m_conn);
	try {
		DrsHeap::redoCreate(m_db, s, SMALL_HEAP, tableDef);
		//CPPUNIT_ASSERT(false);
	} catch (NtseException &) {
		CPPUNIT_ASSERT(true);
	}
	m_db->getSessionManager()->freeSession(s);


	// 文件长度不够
	closeSmallHeap();
	s = m_db->getSessionManager()->allocSession("FLRHeapTestCase::testOpen", m_conn);
	EXCPT_OPER(DrsHeap::redoCreate(m_db, s, SMALL_HEAP, tableDef));
	m_db->getSessionManager()->freeSession(s);

	s = m_db->getSessionManager()->allocSession("FLRHeapTestCase::testOpen", m_conn);
	EXCPT_OPER(m_heap = DrsHeap::open(m_db, s, SMALL_HEAP, tableDef));
	m_db->getSessionManager()->freeSession(s);


	// 不用recreate
	Record *rec;
	insertRecord(100, &rec, m_db, m_conn, m_heap, m_tblDef);
	freeRecord(rec);

	closeSmallHeap();
	s = m_db->getSessionManager()->allocSession("FLRHeapTestCase::testOpen", m_conn);
	EXCPT_OPER(DrsHeap::redoCreate(m_db, s, SMALL_HEAP, tableDef));
	m_db->getSessionManager()->freeSession(s);

	s = m_db->getSessionManager()->allocSession("FLRHeapTestCase::testOpen", m_conn);
	EXCPT_OPER(m_heap = DrsHeap::open(m_db, s, SMALL_HEAP, tableDef));
	m_db->getSessionManager()->freeSession(s);


	delete tableDef;
}


/**
 * TODO: 这个函数只是临时根据定长堆实现编写,很多依赖,最好能消除依赖
 */
void FLRHeapTestCase::initHeapInvalidPage( FLRHeapRecordPageInfo * page, DrsHeap *heap , const TableDef *tblDef) 
{
	page->m_bph.m_lsn = 0;
	page->m_bph.m_checksum = BufferPageHdr::CHECKSUM_NO;
	page->m_inFreePageList = true;
	page->m_recordNum = 0;
	page->m_firstFreeSlot	= 0;
	/* nextFreePageNum依次指向0 */
	page->m_nextFreePageNum = 0;
	/* 初始化每一个slot */
	for (uint j = 0; j < ((FixedLengthRecordHeap*)m_heap)->getRecPerPage(); j++) {
		uint slotLength = tblDef->m_maxRecSize + sizeof(u8);
		FLRSlotInfo *slot = (FLRSlotInfo*)((byte*)page + sizeof(FLRHeapRecordPageInfo) + (s16)j * slotLength);/*((FixedLengthRecordHeap*)m_heap)->getSlot(page, (s16)j);*/
		slot->m_free = true;
		if (j < ((FixedLengthRecordHeap*)m_heap)->getRecPerPage() - 1)
			slot->u.m_nextFreeSlot = (s16)(j + 1);
		else
			slot->u.m_nextFreeSlot = -1;
	}
}



/** 
 * 测试redoInsert过程
 *		流程：	1. 构造redoInsert的各种初始情形
 *				2. 调用redoInsert
 *				3. 查看redo的结果是否如同预期
 *				4. 具体用例参看内部注释
 */
void FLRHeapTestCase::testRedoInsert() {
	Record *rec;
	byte *buf = (byte *)System::virtualAlloc(Limits::PAGE_SIZE);
	Session *session;
	u64 history, today, history2, today2, history3, today3;
	u64 rid;
	Record readOut;
	byte *readBuf = (byte *)System::virtualAlloc(Limits::PAGE_SIZE);
	readOut.m_data = readBuf;
	readOut.m_format = REC_FIXLEN;
	bool canRead;


	// 测试log功能正常
	rid = insertRecord(1, &rec, m_db, m_conn, m_heap, m_tblDef); //
	session = m_db->getSessionManager()->allocSession("FLRHeapTestCase::testRedoInsert", m_conn);
	history = m_heap->getPageLSN(session, 1, m_heap->getDBObjStats());
	m_heap->getRecord(session, rid, &readOut);
	m_db->getSessionManager()->freeSession(session);
	// 比较
	CPPUNIT_ASSERT(rec->m_size == readOut.m_size);
	CPPUNIT_ASSERT(0 == memcmp(rec->m_data, readOut.m_data, readOut.m_size));

	// 关闭数据库再打开
	session = m_db->getSessionManager()->allocSession("FLRHeapTestCase::testRedoInsert", m_conn);
	m_heap->close(session, true);
	delete m_heap;
	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(m_conn);
	//m_db->close();
	//delete m_db;
	//EXCPT_OPER(m_db = Database::open(&m_config, 1));
	m_conn = m_db->getConnection(true);
	session = m_db->getSessionManager()->allocSession("FLRHeapTestCase::testRedoInsert", m_conn);
	EXCPT_OPER(m_heap = DrsHeap::open(m_db, session, SMALL_HEAP, m_tblDef));
	m_db->getSessionManager()->freeSession(session);

	// 数据应该没变过
	session = m_db->getSessionManager()->allocSession("FLRHeapTestCase::testRedoInsert", m_conn);
	m_heap->getRecord(session, rid, &readOut);
	m_db->getSessionManager()->freeSession(session);
	// 比较
	CPPUNIT_ASSERT(rec->m_size == readOut.m_size);
	CPPUNIT_ASSERT(0 == memcmp(rec->m_data, readOut.m_data, readOut.m_size));



	// 现在检查log
	LogScanHandle *logHdl = m_db->getTxnlog()->beginScan(Txnlog::MIN_LSN, Txnlog::MAX_LSN);
	int j = 0;
	session = m_db->getSessionManager()->allocSession("FLRHeapTestCase::testRedoInsert", m_conn);
	while (m_db->getTxnlog()->getNext(logHdl)) {
		byte *logData = logHdl->logEntry()->m_data;
#ifdef NTSE_VERIFY_EX
		logData += sizeof(u64);
		logData += sizeof(u64);
#endif
		CPPUNIT_ASSERT(*((u64 *)logData) == rid);
		logData += sizeof(u64);
		CPPUNIT_ASSERT(*((bool *)logData) == false);
		logData += sizeof(bool);
		CPPUNIT_ASSERT(*((uint *)logData) == rec->m_size);
		logData += sizeof(uint);
		CPPUNIT_ASSERT(0 == memcmp(logData, rec->m_data, rec->m_size));

		j++;
	}
	m_db->getSessionManager()->freeSession(session);
	m_db->getTxnlog()->endScan(logHdl);
	CPPUNIT_ASSERT(j == 1);

	freeRecord(rec);


	////////////// 完全重新来过 ///////////////////////
	tearDown();
	setUp();

	session = m_db->getSessionManager()->allocSession("FLRHeapTestCase::testRedoInsert", m_conn);
	int recPerPage = (int)((FixedLengthRecordHeap *)m_heap)->getRecPerPage();
	backupPage(session, m_heap, 0, buf);
	for (int i = 0; i <= recPerPage; ++i) {
		if (i == recPerPage - 1) {
			backupPage(session, m_heap, 1, buf);
			history = m_heap->getPageLSN(session, 1, m_heap->getDBObjStats());
		}

		rid = insertRecord(100 + i, &rec, m_db, m_conn, m_heap, m_tblDef);
		if (i == recPerPage - 1) {
			today = m_heap->getPageLSN(session, 1, m_heap->getDBObjStats());
			CPPUNIT_ASSERT(today != history);
		}
		//cout<<"Page "<<i/recPerPage + 1<<" lsn is "<<m_heap->getPageLSN(session, i/recPerPage + 1)<<endl;
		freeRecord(rec);
		if (i == recPerPage - 1) { // 第二页插入前
			restorePage(session, m_heap, 1, buf); // 第一页最后一条记录的修改就没有生效
			today = m_heap->getPageLSN(session, 1, m_heap->getDBObjStats());
			CPPUNIT_ASSERT(today == history);

			//backupPage(session, m_heap, 2, buf); // 准备第二页。
			//history = m_heap->getPageLSN(session, 2, m_heap->getDBObjStats());
		}
		if (i == recPerPage) {
			history3 = m_heap->getPageLSN(session, 2, m_heap->getDBObjStats());
			canRead = m_heap->getRecord(session, rid, &readOut);
			CPPUNIT_ASSERT(canRead);
		}
	}

	//today = m_heap->getPageLSN(session, 2, m_heap->getDBObjStats());
	//CPPUNIT_ASSERT(today != history);
	memset(buf, 0, Limits::PAGE_SIZE);
	FLRHeapRecordPageInfo *hpage = (FLRHeapRecordPageInfo*)buf;
	initHeapInvalidPage(hpage, m_heap, m_tblDef);

	restorePage(session, m_heap, 2, buf); // 回复到最后一个插入前
	today = m_heap->getPageLSN(session, 2, m_heap->getDBObjStats());
	//CPPUNIT_ASSERT(today == history);


	// 关闭再打开
	m_heap->close(session, true);
	delete m_heap;
	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(m_conn);
	//m_db->close();
	//delete m_db;

	//EXCPT_OPER(m_db = Database::open(&m_config, 1));
	m_conn = m_db->getConnection(true);
	session = m_db->getSessionManager()->allocSession("FLRHeapTestCase::testRedoInsert", m_conn);
	EXCPT_OPER(m_heap = DrsHeap::open(m_db, session, SMALL_HEAP, m_tblDef));
	m_db->getSessionManager()->freeSession(session);



	// 检查日志
	Record record;
	byte *data = (byte *)System::virtualAlloc(Limits::PAGE_SIZE);
	record.m_format = REC_FIXLEN;
	record.m_data = data;
	
	logHdl = m_db->getTxnlog()->beginScan(Txnlog::MIN_LSN, Txnlog::MAX_LSN);
	j = 0;
	session = m_db->getSessionManager()->allocSession("FLRHeapTestCase::testRedoInsert", m_conn);
	Record parseOut(INVALID_ROW_ID, REC_FIXLEN, NULL, Limits::PAGE_SIZE);
	while (m_db->getTxnlog()->getNext(logHdl)) {
		++j;
		u64 lsn = logHdl->curLsn();
		
		
		if (j == recPerPage) {
			history = m_heap->getPageLSN(session, 1, m_heap->getDBObjStats());
			history2 = m_heap->getPageLSN(session, 0, m_heap->getDBObjStats());
		}
		if (j == recPerPage + 1) {
			today3 = m_heap->getPageLSN(session, 2, m_heap->getDBObjStats());
			CPPUNIT_ASSERT(today3 != history3);
		}


		m_heap->getRecordFromInsertlog((LogEntry *)logHdl->logEntry(), &parseOut);
		u64 gotrid = DrsHeap::getRowIdFromInsLog(logHdl->logEntry());
		CPPUNIT_ASSERT(gotrid);
		m_heap->redoInsert(session, lsn, logHdl->logEntry()->m_data, (uint)logHdl->logEntry()->m_size, &record);
		
		
		
		if (j == recPerPage) {
			today = m_heap->getPageLSN(session, 1, m_heap->getDBObjStats());
			today2 = m_heap->getPageLSN(session, 0, m_heap->getDBObjStats());
			CPPUNIT_ASSERT(today != history);
			CPPUNIT_ASSERT(today2 == history2);
		}
		if (j == recPerPage + 1) {
			today3 = m_heap->getPageLSN(session, 2, m_heap->getDBObjStats());
			CPPUNIT_ASSERT(today3 == history3);

			m_heap->getRecord(session, rid, &record);
			CPPUNIT_ASSERT(record.m_size == readOut.m_size);
			CPPUNIT_ASSERT(record.m_format == readOut.m_format);
			CPPUNIT_ASSERT(record.m_rowId == readOut.m_rowId);
			//cout<<"record.m_rowId is "<<record.m_rowId<<endl<<"rec->m_rowId is "<<rec->m_rowId<<endl;
			CPPUNIT_ASSERT(0 == memcmp(record.m_data, readOut.m_data, record.m_size));
		}
	}

	m_db->getSessionManager()->freeSession(session);
	m_db->getTxnlog()->endScan(logHdl);

	session = m_db->getSessionManager()->allocSession("FLRHeapTestCase::testRedoInsert", m_conn);
	m_heap->redoFinish(session);
	m_db->getSessionManager()->freeSession(session);


	////////////////重新来过 覆盖分支
	tearDown();
	setUp();

	// 备份还未插入的文件
	backupHeapFile(m_heap->getHeapFile(), HEAP_BACKUP);

	for (int i = 0; i <= recPerPage; ++i) {
		rid = insertRecord(100 + i, &rec, m_db, m_conn, m_heap, m_tblDef);
		freeRecord(rec);
	}
	//CPPUNIT_ASSERT((1 + DrsHeap::HEAP_INCREASE_SIZE) * Limits::PAGE_SIZE == m_heap->getHeapFileSize());
	u64 fileSize;	

	session = m_db->getSessionManager()->allocSession("FLRHeapTestCase::testRedoInsert", m_conn);
	m_heap->close(session, true);
	delete m_heap;
	m_db->getSessionManager()->freeSession(session);

	// 恢复刚才备份的文件，刚才的插入就无用功了。
	restoreHeapFile(HEAP_BACKUP, SMALL_HEAP);

	// 再次打开小堆文件
	session = m_db->getSessionManager()->allocSession("FLRHeapTestCase::testRedoInsert", m_conn);
	m_heap = DrsHeap::open(m_db, session, SMALL_HEAP, m_tblDef);
	m_db->getSessionManager()->freeSession(session);

	//CPPUNIT_ASSERT(Limits::PAGE_SIZE == m_heap->getHeapFileSize());
	//u64 fileSize;
	m_heap->getHeapFile()->getSize(&fileSize);
	CPPUNIT_ASSERT(Limits::PAGE_SIZE == fileSize);


	// 关闭再打开
	session = m_db->getSessionManager()->allocSession("FLRHeapTestCase::testRedoInsert", m_conn);
	m_heap->close(session, true);
	delete m_heap;
	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(m_conn);
	//m_db->close();
	//delete m_db;
	//EXCPT_OPER(m_db = Database::open(&m_config, 1));
	m_conn = m_db->getConnection(true);
	session = m_db->getSessionManager()->allocSession("FLRHeapTestCase::testRedoInsert", m_conn);
	EXCPT_OPER(m_heap = DrsHeap::open(m_db, session, SMALL_HEAP, m_tblDef));
	m_db->getSessionManager()->freeSession(session);


	logHdl = m_db->getTxnlog()->beginScan(Txnlog::MIN_LSN, Txnlog::MAX_LSN);
	j = 0;
	session = m_db->getSessionManager()->allocSession("FLRHeapTestCase::testRedoInsert", m_conn);
	Record tempRec(INVALID_ROW_ID, REC_FIXLEN, (byte *)session->getMemoryContext()->alloc(m_tblDef->m_maxRecSize), m_tblDef->m_maxRecSize);
	while (m_db->getTxnlog()->getNext(logHdl)) {
		++j;
		u64 lsn = logHdl->curLsn();
		//cout<<"LSN is "<<lsn<<endl;
		CPPUNIT_ASSERT(logHdl->logEntry()->m_logType == LOG_HEAP_INSERT);
		FixedLengthRecordHeap::getRecordFromInsertlog((LogEntry*)logHdl->logEntry(), &tempRec);
		m_heap->redoInsert(session, lsn, logHdl->logEntry()->m_data, (uint)logHdl->logEntry()->m_size, &readOut);
	}
	m_db->getSessionManager()->freeSession(session);
	m_db->getTxnlog()->endScan(logHdl);

	session = m_db->getSessionManager()->allocSession("FLRHeapTestCase::testRedoInsert", m_conn);
	m_heap->redoFinish(session);
	m_db->getSessionManager()->freeSession(session);

	System::virtualFree(buf);
	System::virtualFree(readBuf);
	System::virtualFree(data);
}


/** 
 * 测试redoUpdate过程
 *		流程：	1. 构造redoUpdate的各种初始情形
 *				2. 调用redoUpdate
 *				3. 查看redo的结果是否如同预期
 *				4. 具体用例参看内部注释
 */
void FLRHeapTestCase::testRedoUpdate() {

	u64 rid;
	Record *rec;
	Record readOut1, readOut2, readOut3, readOut4, readOut;
	byte *buf = (byte *)System::virtualAlloc(Limits::PAGE_SIZE * 5);
	readOut.m_format = readOut1.m_format = readOut2.m_format = readOut3.m_format = readOut4.m_format = REC_FIXLEN;
	readOut.m_data = buf;
	readOut1.m_data = buf + Limits::PAGE_SIZE * 1;
	readOut2.m_data = buf + Limits::PAGE_SIZE * 2;
	readOut3.m_data = buf + Limits::PAGE_SIZE * 3;
	readOut4.m_data = buf + Limits::PAGE_SIZE * 4;
	SubRecordBuilder compBuilder(m_tblDef, REC_REDUNDANT);
	SubRecord *subRec;
	bool canUpdate, canRead;
	char uname[40];
	u64 lsn1, lsn2, lsn3, lsn4;

	u64 rows = ((FixedLengthRecordHeap *)m_heap)->getRecPerPage() * 3;

	vector<RowId> ridVec;
	for (int i = 0; i < rows; ++i) {
		rid = insertRecord(i + 1, &rec, m_db, m_conn, m_heap, m_tblDef);
		freeRecord(rec);
		ridVec.push_back(rid);
	}

	vector<SubRecord *> subRecVec;

	for (int i = 0; i < rows; ++i) {
		u64 uid = i + 100;
		sprintf(uname, "Connie %d\0", i+1);
		subRec = compBuilder.createSubRecordById("0 1", &uid, uname);
		subRecVec.push_back(subRec);
	}

	Session *session;

	// 取得update前的数据
	session = m_db->getSessionManager()->allocSession("FLRHeapTestCase::testRedoUpdate", m_conn);
	// 取得第一个记录
	canRead = m_heap->getRecord(session, RID(1, 0), &readOut1);
	CPPUNIT_ASSERT(canRead);
	// 取得最后一个记录
	lsn1 = m_heap->getPageLSN(session, 1, m_heap->getDBObjStats());
	m_heap->getRecord(session, rid, &readOut2);
	CPPUNIT_ASSERT(canRead);
	lsn2 = m_heap->getPageLSN(session, RID_GET_PAGE(rid), m_heap->getDBObjStats());
	m_db->getSessionManager()->freeSession(session);

	// 关闭数据库再打开
	session = m_db->getSessionManager()->allocSession("FLRHeapTestCase::testRedoUpdate", m_conn);
	m_heap->close(session, true);
	delete m_heap;
	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(m_conn);
	//m_db->close();
	//delete m_db;
	//EXCPT_OPER(m_db = Database::open(&m_config, 1));
	m_conn = m_db->getConnection(true);
	session = m_db->getSessionManager()->allocSession("FLRHeapTestCase::testRedoUpdate", m_conn);
	EXCPT_OPER(m_heap = DrsHeap::open(m_db, session, SMALL_HEAP, m_tblDef));
	m_db->getSessionManager()->freeSession(session);

	// update之前备份文件
	m_heap->getBuffer()->flushAll(NULL);
	backupHeapFile(m_heap->getHeapFile(), HEAP_BACKUP);

	session = m_db->getSessionManager()->allocSession("FLRHeapTestCase::testRedoUpdate", m_conn);
	for (int i = 0; i < rows; ++i) {
		if (i < rows / 2) {
			m_heap->getRecord(session, ridVec[i], &readOut);
			RecordOper::updateRecordFR(m_tblDef, &readOut, subRecVec[i]);
			canUpdate = m_heap->update(session, ridVec[i], &readOut);
		} else {
			canUpdate = m_heap->update(session, ridVec[i], subRecVec[i]);
		}
		CPPUNIT_ASSERT(canUpdate);
	}
	m_db->getSessionManager()->freeSession(session);

	// 更新后应该不等了
	session = m_db->getSessionManager()->allocSession("FLRHeapTestCase::testRedoUpdate", m_conn);
	m_heap->getRecord(session, RID(1, 0), &readOut3);
	CPPUNIT_ASSERT(0 != memcmp(readOut3.m_data, readOut1.m_data, readOut3.m_size));
	m_heap->getRecord(session, rid, &readOut4);
	CPPUNIT_ASSERT(0 != memcmp(readOut4.m_data, readOut2.m_data, readOut3.m_size));

	lsn3 = m_heap->getPageLSN(session, 1, m_heap->getDBObjStats());
	lsn4 = m_heap->getPageLSN(session, RID_GET_PAGE(rid), m_heap->getDBObjStats());
	CPPUNIT_ASSERT(lsn3 > lsn1 && lsn4 > lsn2);

	m_db->getSessionManager()->freeSession(session);



	// 关闭数据库再打开
	session = m_db->getSessionManager()->allocSession("FLRHeapTestCase::testRedoUpdate", m_conn);
	m_heap->close(session, true);
    delete m_heap;
	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(m_conn);
	//m_db->close();
	//delete m_db;
	//EXCPT_OPER(m_db = Database::open(&m_config, 1));
	m_conn = m_db->getConnection(true);
	// 用更新前备份来替代
	restoreHeapFile(HEAP_BACKUP, SMALL_HEAP);
	session = m_db->getSessionManager()->allocSession("FLRHeapTestCase::testRedoUpdate", m_conn);
	EXCPT_OPER(m_heap = DrsHeap::open(m_db, session, SMALL_HEAP, m_tblDef));
	m_db->getSessionManager()->freeSession(session);

	// 确认镜像已经恢复
	session = m_db->getSessionManager()->allocSession("FLRHeapTestCase::testRedoUpdate", m_conn);
	m_heap->getRecord(session, RID(1, 0), &readOut);
	CPPUNIT_ASSERT(0 == memcmp(readOut.m_data, readOut1.m_data, readOut.m_size));
	m_heap->getRecord(session, rid, &readOut);
	CPPUNIT_ASSERT(0 == memcmp(readOut.m_data, readOut2.m_data, readOut.m_size));
	m_db->getSessionManager()->freeSession(session);


	///// 按日志 redo 
	LogScanHandle *logHdl = m_db->getTxnlog()->beginScan(Txnlog::MIN_LSN, Txnlog::MAX_LSN);
	int j = 0;
	session = m_db->getSessionManager()->allocSession("FLRHeapTestCase::testRedoUpdate", m_conn);
	while (m_db->getTxnlog()->getNext(logHdl)) {
		u64 lsn = logHdl->curLsn();
		//cout<<"LSN is "<<lsn<<endl;
		switch (logHdl->logEntry()->m_logType) {
		case LOG_HEAP_INSERT:
			break;
		case LOG_HEAP_UPDATE:
			m_heap->redoUpdate(session, lsn, logHdl->logEntry()->m_data, (uint)logHdl->logEntry()->m_size, subRecVec[j]);
			++j;		
			break;
		default:
			break;
		}
	}
        m_db->getTxnlog()->endScan(logHdl);

	j = 0;
	logHdl = m_db->getTxnlog()->beginScan(Txnlog::MIN_LSN, Txnlog::MAX_LSN);
	while (m_db->getTxnlog()->getNext(logHdl)) {
		u64 lsn = logHdl->curLsn();
		switch (logHdl->logEntry()->m_logType) {
		case LOG_HEAP_INSERT:
			break;
		case LOG_HEAP_UPDATE:
			m_heap->redoUpdate(session, lsn, logHdl->logEntry()->m_data, (uint)logHdl->logEntry()->m_size, subRecVec[j]);
			++j;		
			break;
		default:
			break;
		}
	}
        m_db->getTxnlog()->endScan(logHdl);
	m_db->getSessionManager()->freeSession(session);

	session = m_db->getSessionManager()->allocSession("FLRHeapTestCase::testRedoInsert", m_conn);
	m_heap->redoFinish(session);
	m_db->getSessionManager()->freeSession(session);


	// 验证redoUpdate的结果正确
	session = m_db->getSessionManager()->allocSession("FLRHeapTestCase::testRedoInsert", m_conn);
	m_heap->getRecord(session, RID(1, 0), &readOut);
	CPPUNIT_ASSERT(0 == memcmp(readOut.m_data, readOut3.m_data, readOut3.m_size));
	m_heap->getRecord(session, rid, &readOut);
	CPPUNIT_ASSERT(0 == memcmp(readOut.m_data, readOut4.m_data, readOut4.m_size));

	lsn1 = m_heap->getPageLSN(session, 1, m_heap->getDBObjStats());
	lsn2 = m_heap->getPageLSN(session, RID_GET_PAGE(rid), m_heap->getDBObjStats());
	CPPUNIT_ASSERT(lsn1 == lsn3 && lsn2 == lsn4);
	
	m_db->getSessionManager()->freeSession(session);

	for (int i = 0; i < rows; ++i) {
		freeSubRecord(subRecVec[i]);
	}
	System::virtualFree(buf);
}

/** 
 * 测试redoDelete过程
 *		流程：	1. 构造redoDelete的各种初始情形
 *				2. 调用redoDelete
 *				3. 查看redo的结果是否如同预期
 *				4. 具体用例参看内部注释
 */
void FLRHeapTestCase::testRedoDelete() {
	u64 rid;
	Record *rec;
	Record readOut1, readOut2, readOut3, readOut4, readOut;
	byte *buf = (byte *)System::virtualAlloc(Limits::PAGE_SIZE * 5);
	readOut.m_format = readOut1.m_format = readOut2.m_format = readOut3.m_format = readOut4.m_format = REC_FIXLEN;
	readOut.m_data = buf;
	readOut1.m_data = buf + Limits::PAGE_SIZE * 1;
	readOut2.m_data = buf + Limits::PAGE_SIZE * 2;
	readOut3.m_data = buf + Limits::PAGE_SIZE * 3;
	readOut4.m_data = buf + Limits::PAGE_SIZE * 4;
	SubRecordBuilder compBuilder(m_tblDef, REC_REDUNDANT);
	SubRecord *subRec = compBuilder.createEmptySbById(Limits::PAGE_SIZE, "0");
	bool canRead;
	//u64 rows = ((FixedLengthRecordHeap *)m_heap)->getRecPerPage() * (DrsHeap::HEAP_INCREASE_SIZE + 1); // we'll talk about it later.
	u64 rows = ((FixedLengthRecordHeap *)m_heap)->getRecPerPage() * (m_tblDef->m_incrSize + 1); // we'll talk about it later.

	vector<RowId> ridVec;
	for (int i = 0; i < rows; ++i) {
		rid = insertRecord(i + 1, &rec, m_db, m_conn, m_heap, m_tblDef);
		ridVec.push_back(rid);
		freeRecord(rec);
	}

	// 关闭数据库再打开
	Session *session = m_db->getSessionManager()->allocSession("FLRHeapTestCase::testRedoDelete", m_conn);
	m_heap->close(session, true);
	delete m_heap;
	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(m_conn);
	//m_db->close();
	//delete m_db;
	//EXCPT_OPER(m_db = Database::open(&m_config, 1));
	m_conn = m_db->getConnection(true);
	session = m_db->getSessionManager()->allocSession("FLRHeapTestCase::testRedoDelete", m_conn);
	EXCPT_OPER(m_heap = DrsHeap::open(m_db, session, SMALL_HEAP, m_tblDef));
	m_db->getSessionManager()->freeSession(session);

	// delete之前备份文件
	m_heap->getBuffer()->flushAll(NULL);
	backupHeapFile(m_heap->getHeapFile(), HEAP_BACKUP);

	
	// 确认
	session = m_db->getSessionManager()->allocSession("FLRHeapTestCase::testRedoDelete", m_conn);
	canRead = m_heap->getRecord(session, RID(1, 0), &readOut);
	CPPUNIT_ASSERT(canRead);
	canRead = m_heap->getRecord(session, rid, &readOut); //最后一个
	CPPUNIT_ASSERT(canRead);
	m_db->getSessionManager()->freeSession(session);


	// 删掉都
	for (int i = 0; i < (int)rows; ++i){
		rid = ridVec[(int)rows - i - 1];
		deleteRecord(rid, m_db, m_conn, m_heap);
	}

	// 现在应该没有了
	session = m_db->getSessionManager()->allocSession("FLRHeapTestCase::testRedoDelete", m_conn);
	canRead = m_heap->getRecord(session, RID(1, 0), &readOut);
	CPPUNIT_ASSERT(!canRead);
	canRead = m_heap->getRecord(session, rid, &readOut); //最后一个
	CPPUNIT_ASSERT(!canRead);
	m_db->getSessionManager()->freeSession(session);


	// 关闭数据库再打开
	session = m_db->getSessionManager()->allocSession("FLRHeapTestCase::testRedoDelete", m_conn);
	m_heap->close(session, true);
	delete m_heap;
	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(m_conn);
	//m_db->close();
	//delete m_db;
	//EXCPT_OPER(m_db = Database::open(&m_config, 1));
	m_conn = m_db->getConnection(true);
	// 用更新前备份来替代
	restoreHeapFile(HEAP_BACKUP, SMALL_HEAP);
	session = m_db->getSessionManager()->allocSession("FLRHeapTestCase::testRedoDelete", m_conn);
	EXCPT_OPER(m_heap = DrsHeap::open(m_db, session, SMALL_HEAP, m_tblDef));
	m_db->getSessionManager()->freeSession(session);


	// 现在应该又有了
	session = m_db->getSessionManager()->allocSession("FLRHeapTestCase::testRedoDelete", m_conn);
	canRead = m_heap->getRecord(session, RID(1, 0), &readOut);
	CPPUNIT_ASSERT(canRead);
	canRead = m_heap->getRecord(session, rid, &readOut); //最后一个
	CPPUNIT_ASSERT(canRead);
	m_db->getSessionManager()->freeSession(session);

	///// 按日志 redo 
	LogScanHandle *logHdl = m_db->getTxnlog()->beginScan(Txnlog::MIN_LSN, Txnlog::MAX_LSN);
	int j = 0;
	session = m_db->getSessionManager()->allocSession("FLRHeapTestCase::testRedoDelete", m_conn);
	while (m_db->getTxnlog()->getNext(logHdl)) {
		++j;
		u64 lsn = logHdl->curLsn();
		//cout<<"LSN is "<<lsn<<endl;
		switch (logHdl->logEntry()->m_logType) {
		case LOG_HEAP_INSERT:
			break;
		case LOG_HEAP_UPDATE:
			break;
		case LOG_HEAP_DELETE:
			m_heap->redoDelete(session, lsn, logHdl->logEntry()->m_data, (uint)logHdl->logEntry()->m_size);
			break;
		default:
			break;
		}
		
	}
	m_db->getSessionManager()->freeSession(session);
	m_db->getTxnlog()->endScan(logHdl);

	
	session = m_db->getSessionManager()->allocSession("FLRHeapTestCase::testRedoDelete", m_conn);
	m_heap->redoFinish(session);
	m_db->getSessionManager()->freeSession(session);
	


	// 现在应该没有了
	session = m_db->getSessionManager()->allocSession("FLRHeapTestCase::testRedoDelete", m_conn);
	canRead = m_heap->getRecord(session, RID(1, 0), &readOut);
	CPPUNIT_ASSERT(!canRead);
	canRead = m_heap->getRecord(session, rid, &readOut); //最后一个
	CPPUNIT_ASSERT(!canRead);
	m_db->getSessionManager()->freeSession(session);

	freeSubRecord(subRec);
	System::virtualFree(buf);
}



/**** 性能测试用例 ****/
const char* FLRHeapPerformanceTestCase::getName() {
	return "Fixed length record heap performance test";
}

const char* FLRHeapPerformanceTestCase::getDescription() {
	return "Test various operations of performance focused features of heap";
}

bool FLRHeapPerformanceTestCase::isBig() {
	return true;
}


void FLRHeapPerformanceTestCase::setUp() {
	Database::drop(".");
	m_tblDef = createTableDef();
	m_heap = createHeap(m_tblDef);
}

void FLRHeapPerformanceTestCase::tearDown() {
	if (m_heap != NULL) {
		dropHeap();
	}
	
	if (m_tblDef != NULL) {
		dropTableDef();
	}

	if (m_conn) {
		m_db->freeConnection(m_conn);
		m_conn = NULL;
	}
	if (m_db != NULL) {
		m_db->close();
		Database::drop(".");
        delete m_db;
		m_db = NULL;
	}
}

TableDef *FLRHeapPerformanceTestCase::createTableDef() {
	File oldsmallfile(SMALL_TBLDEF);
	oldsmallfile.remove();

	m_config.m_pageBufSize = 10000;
	EXCPT_OPER(m_db = Database::open(&m_config, 1));		
	TableDefBuilder *builder;
	TableDef *tableDef;

	// 创建小堆
	builder = new TableDefBuilder(99, "inventory", "User");
	builder->addColumn("UserId", CT_BIGINT, false)->addColumnS("UserName", CT_CHAR, 50);
	builder->addColumn("BankAccount", CT_BIGINT)->addColumn("Balance", CT_INT);
	builder->addIndex("PRIMARY", true, true, false, "UserId", 0, NULL);
	tableDef = builder->getTableDef();
	EXCPT_OPER(tableDef->writeFile(SMALL_TBLDEF));
	delete builder;

	return tableDef;
}

/** 
 * 创建测试用堆过程
 */
DrsHeap* FLRHeapPerformanceTestCase::createHeap(TableDef *tblDef) {
	DrsHeap* heap;
	EXCPT_OPER(DrsHeap::create(m_db, SMALL_HEAP, tblDef));
	m_conn = m_db->getConnection(true);
	Session *session = m_db->getSessionManager()->allocSession("FLRHeapPerformanceTestCase::createHeap", m_conn);
	EXCPT_OPER(heap = DrsHeap::open(m_db, session, SMALL_HEAP, tblDef));
	m_db->getSessionManager()->freeSession(session);
	return heap;
}

/** 
 * drop测试用堆
 */
void FLRHeapPerformanceTestCase::dropHeap() {
	Session *session = m_db->getSessionManager()->allocSession("FLRHeapPerformanceTestCase::dropHeap", m_conn);
	EXCPT_OPER(m_heap->close(session, true));
	m_db->getSessionManager()->freeSession(session);
	EXCPT_OPER(DrsHeap::drop(SMALL_HEAP));
	delete m_heap;
	m_heap = NULL;
}

void FLRHeapPerformanceTestCase::dropTableDef() {
	TableDef::drop(SMALL_TBLDEF);
	delete m_tblDef;
	m_tblDef = NULL;
}

/** 
 * 关闭测试用堆过程
 */
void FLRHeapPerformanceTestCase::closeHeap() {
	Session *session = m_db->getSessionManager()->allocSession("FLRHeapPerformanceTestCase::closeHeap", m_conn);
	EXCPT_OPER(m_heap->close(session, true));
	m_db->getSessionManager()->freeSession(session);
	delete m_heap;
	m_heap = NULL;
}


/** 
 * TableScan的性能测试
 */
void FLRHeapPerformanceTestCase::testTableScanPerf() {
	Session *session;
	RowId rid;
	u64 startTime, endTime;
	SubRecordBuilder compBuilder(m_tblDef, REC_REDUNDANT);
	SubRecord *subRec = compBuilder.createEmptySbById(Limits::PAGE_SIZE, "0");

	CPPUNIT_ASSERT(m_heap);
	int rows = 1000000;
	Record *rec = createRecord(0, 0130, "Kenneth Xie", 0130 << 31, 100000000, m_tblDef);


	// 生成数据
	session = m_db->getSessionManager()->allocSession("FLRHeapPerformanceTestCase::testTableScanPerf", m_conn);
	for (int i = 0 ; i < rows; ++i) {
		rid = m_heap->insert(session, rec, NULL);
	}
	m_db->getSessionManager()->freeSession(session);

	m_db->getPageBuffer()->flushAll(NULL);
	m_db->getPageBuffer()->disableScavenger();

	u64 prBefore = m_db->getPageBuffer()->getStatus().m_physicalReads;
	u64 pwBefore = m_db->getPageBuffer()->getStatus().m_physicalWrites;
	startTime = System::currentTimeMillis();
	int repeat = 10;
	for (int loop = 0; loop < repeat; loop++) {
		session = m_db->getSessionManager()->allocSession("FLRHeapPerformanceTestCase::testTableScanPerf", m_conn);
		DrsHeapScanHandle *scanhdl = m_heap->beginScan(session, SubrecExtractor::createInst(session, m_tblDef, subRec), None, NULL, false);
		int i = 0;
		while (m_heap->getNext(scanhdl, subRec)) {
			i++;
		}
		CPPUNIT_ASSERT(i == rows);
		m_heap->endScan(scanhdl);
		m_db->getSessionManager()->freeSession(session);
	}
	endTime = System::currentTimeMillis();
	u64 prAfter = m_db->getPageBuffer()->getStatus().m_physicalReads;
	u64 pwAfter = m_db->getPageBuffer()->getStatus().m_physicalWrites;
#if NTSE_PAGE_SIZE == 8192
	CPPUNIT_ASSERT(prAfter == prBefore);
	CPPUNIT_ASSERT(pwAfter == pwBefore);
#endif
	cout << "Speed of table scan of retrieving 1 attribute: " << (repeat * rows) / (endTime - startTime) * 1000 << endl;
}


/** 
 * insert的性能测试
 */
void FLRHeapPerformanceTestCase::testInsertPerf() {
	int rows = 1000000;
	Record *rec = createRecord(0, 130, "Kenneth Tse", 32768, 11992939, m_tblDef);
	Session *session = m_db->getSessionManager()->allocSession("FLRHeapPerformanceTestCase::testInsertPerf", m_conn);

	u64 startTime = System::currentTimeMillis();
	for (int i = 0; i < rows; ++i) {
		m_heap->insert(session, rec, NULL);
	}
	u64 endTime = System::currentTimeMillis();

	m_db->getSessionManager()->freeSession(session);
	freeRecord(rec);

	cout<<rows<<" inserts takes "<<endTime - startTime<<" ms."<<endl;
}





bool deleteRecord(RowId rid, Database *db, Connection *conn, DrsHeap *heap) {
	bool canDel;
	Session *session = db->getSessionManager()->allocSession("deleteRecord", conn);
	canDel = heap->del(session, rid);
	db->getSessionManager()->freeSession(session);
	return canDel;
}

Record* createRecord(u64 rowid, u64 userid, const char *username, u64 bankacc, u32 balance, TableDef *tableDef) {
	RecordBuilder rb(tableDef, rowid, REC_FIXLEN);
	rb.appendBigInt(userid);
	rb.appendChar(username);
	rb.appendBigInt(bankacc);
	rb.appendInt(balance);
	return rb.getRecord(tableDef->m_maxRecSize);
}

RowId insertRecord(uint number, Record **reco, Database *db, Connection *conn, DrsHeap *heap, TableDef *tableDef) {
	char name[50];
	RowId rid;
	sprintf(name, "Kenneth Tse Jr. %d \0", number);
	*reco = createRecord(0, number, name, number + ((u64)number << 32), (u32)(-1) - number, tableDef);
	CPPUNIT_ASSERT((*reco)->m_format == REC_FIXLEN);
	Connection *newconn = db->getConnection(false);
	Session *session = db->getSessionManager()->allocSession("insertRecord", newconn);
	EXCPT_OPER(rid = heap->insert(session, *reco, NULL));
	db->getSessionManager()->freeSession(session);
	db->freeConnection(newconn);
	return rid;
}

bool updateRecord(RowId rid, SubRecord *subRec, Database *db, Connection *conn, DrsHeap *heap) {
	bool success;
	Session *session = db->getSessionManager()->allocSession("updateRecord", conn);
	EXCPT_OPER(success = heap->update(session, rid, subRec));
	db->getSessionManager()->freeSession(session);
	return success;
}

bool updateRecord(RowId rid, Record *record, Database *db, Connection *conn, DrsHeap *heap) {
	bool success;
	Session *session = db->getSessionManager()->allocSession("updateRecord", conn);
	EXCPT_OPER(success = heap->update(session, rid, record));
	db->getSessionManager()->freeSession(session);
	return success;
}

bool readRecord(RowId rid, Record *record, Database *db, Connection *conn, DrsHeap *heap) {
	bool success;
	Session *session = db->getSessionManager()->allocSession("readRecord", conn);
	EXCPT_OPER(success = heap->getRecord(session, rid, record));
	db->getSessionManager()->freeSession(session);
	return success;
}

bool readSubRecord(RowId rid, SubRecord *subRec, Database *db, Connection *conn, DrsHeap *heap, const TableDef *tblDef) {
	bool success;
	Session *session = db->getSessionManager()->allocSession("readSubRecord", conn);
	SubrecExtractor *srExtractor = SubrecExtractor::createInst(session, tblDef, subRec);
	EXCPT_OPER(success = heap->getSubRecord(session, rid, srExtractor, subRec));
	db->getSessionManager()->freeSession(session);
	return success;
}

void backupPage(Session *session, DrsHeap *heap, u64 pageNum, byte *pageBuffer) {
	BufferPageHandle *bphdl = GET_PAGE(session, heap->getHeapFile(), PAGE_HEAP, pageNum, Shared, heap->getDBObjStats(), NULL);
	memcpy(pageBuffer, bphdl->getPage(), Limits::PAGE_SIZE);
	session->releasePage(&bphdl);
}

void restorePage(Session *session, DrsHeap *heap, u64 pageNum, byte *pageBuffer) {
	BufferPageHandle *bphdl = GET_PAGE(session, heap->getHeapFile(), PAGE_HEAP, pageNum, Exclusived, heap->getDBObjStats(), NULL);
	memcpy(bphdl->getPage(), pageBuffer, Limits::PAGE_SIZE);
	session->markDirty(bphdl);
	session->releasePage(&bphdl);
}

void backupHeapFile(File *file, const char *backupName, Buffer *buffer) {
	u64 errCode = 0;
	u32 checkSum;

	File bk(backupName);
	errCode = bk.remove();
	assert(File::getNtseError(errCode) == File::E_NO_ERROR || File::getNtseError(errCode) == File::E_NOT_EXIST);
	errCode = bk.create(true, false);
	assert(File::getNtseError(errCode) == File::E_NO_ERROR);

	byte *buf = (byte *)System::virtualAlloc(Limits::PAGE_SIZE);
	memset(buf, 0, Limits::PAGE_SIZE);
	BufferPageHdr *bphdr = (BufferPageHdr *)buf;

	int j = 0;
	u64 size = 0;
	u32 blockSize;
	file->getSize(&size);
	errCode = bk.setSize(size);
	assert(File::getNtseError(errCode) == File::E_NO_ERROR);
	for (; j < size; j += Limits::PAGE_SIZE) {
		blockSize = (u32)((j + Limits::PAGE_SIZE > size) ? (size - j) : Limits::PAGE_SIZE);
backupHeapFile_read_source:
		errCode = file->read(j, blockSize, buf);
		assert(File::getNtseError(errCode) == File::E_NO_ERROR);
		if (buffer) {
			checkSum = buffer->checksumPage(bphdr);
			if (checkSum != bphdr->m_checksum)
				goto backupHeapFile_read_source;
		}
		errCode = bk.write(j, blockSize, buf);
		assert(File::getNtseError(errCode) == File::E_NO_ERROR);
	}
	errCode = bk.close();
	assert(File::getNtseError(errCode) == File::E_NO_ERROR);

	bool exist;
	bk.isExist(&exist);
	assert(exist);

	System::virtualFree(buf);
}

void backupHeapFile(const char *origName, const char *backupName) {
	u64 errCode = 0;

	File orig(origName);
	orig.open(true);
	backupHeapFile(&orig, backupName);
	orig.close();
}

void backupTblDefFile(const char *origName, const char *backupName) {
	u64 errNo = File::copyFile(backupName, origName, true);
	assert((errNo == File::E_NO_ERROR));
}

void restoreHeapFile(const char *backupHeapFile, const char *origFile) {
	u64 errCode;
	File bk(backupHeapFile);
	File orig(origFile);
	errCode = orig.remove();
	assert(File::getNtseError(errCode) == File::E_NO_ERROR);
	errCode = bk.move(origFile);
	assert(File::getNtseError(errCode) == File::E_NO_ERROR);

}



int compareFile(File *file1, File *file2, int startPage, int *diffIdx) {
	u64 size1, size2;
	u64 errCode;
	uint readSize;
	file1->getSize(&size1);
	file2->getSize(&size2);
	if (size1 != size2) return -1;
	byte *buf1 = (byte *)System::virtualAlloc(Limits::PAGE_SIZE);
	byte *buf2 = (byte *)System::virtualAlloc(Limits::PAGE_SIZE);
	memset(buf1, 0, Limits::PAGE_SIZE);
	memset(buf2, 0, Limits::PAGE_SIZE);
	VLRHeapHeaderPageInfo *vh1 = (VLRHeapHeaderPageInfo *)buf1;
	VLRHeapHeaderPageInfo *vh2 = (VLRHeapHeaderPageInfo *)buf2;
	VLRHeapRecordPageInfo *vr1 = (VLRHeapRecordPageInfo *)buf1;
	VLRHeapRecordPageInfo *vr2 = (VLRHeapRecordPageInfo *)buf2;
	BitmapPage *bm1 = (BitmapPage *)buf1;
	BitmapPage *bm2 = (BitmapPage *)buf2;
	
	for (int i = startPage; i <= (size1 - 1) / Limits::PAGE_SIZE; ++i) {
		readSize = uint((((i + 1) * Limits::PAGE_SIZE) > size1) ? (size1 % Limits::PAGE_SIZE) : Limits::PAGE_SIZE);
		errCode = file1->read(i * Limits::PAGE_SIZE, readSize, buf1);
		assert(File::getNtseError(errCode) == File::E_NO_ERROR);
		errCode = file2->read(i * Limits::PAGE_SIZE, readSize, buf2);
		assert(File::getNtseError(errCode) == File::E_NO_ERROR);
		if (memcmp(buf1, buf2, readSize) != 0) {
			if (diffIdx) {
				int startIdx = *diffIdx;
				*diffIdx = 0;
				for (uint i = startIdx; i < readSize; ++i) {
					if (buf1[i] != buf2[i]) {
						*diffIdx = i;
						break;
					}
				}
			}
                        System::virtualFree(buf1); System::virtualFree(buf2);
			return i + 1;
		}
	}

	System::virtualFree(buf1); System::virtualFree(buf2);
	return 0;
}

void FLRHeapTestCase::testMTInsert() {
	//u64 rows = ((FixedLengthRecordHeap *)m_heap)->getRecPerPage() * (DrsHeap::HEAP_INCREASE_SIZE + 1);
	u64 rows = ((FixedLengthRecordHeap *)m_heap)->getRecPerPage() * (m_tblDef->m_incrSize + 1);
	svector<RowId> ridVec;
	int count[4];
	struct _data {
		svector<RowId> *m_svec;
		int count;
		int rows;
	} data[4];
	for (int i = 0; i < 4; ++i) {
		data[i].m_svec = &ridVec;
		data[i].count = 0;
		data[i].rows = (int)rows;
	}
	Frank f1(m_db, m_conn, m_tblDef, m_heap, "Insert thread 1", (void *)&data[0]);
	Frank f2(m_db, m_conn, m_tblDef, m_heap, "Insert thread 2", (void *)&data[1]);
	Frank f3(m_db, m_conn, m_tblDef, m_heap, "Insert thread 3", (void *)&data[2]);
	Julia j1(m_db, m_conn, m_tblDef, m_heap, "Get thread 1", (void *)&data[3]);

	f3.enableSyncPoint(SP_HEAP_FLR_BEFORE_EXTEND_HEAP);

	f1.start();
	f2.start();
	while (data[0].count < 1000 || data[1].count < 1000)
		Thread::msleep(5);
	//cout<<"data[0].count is "<<data[0].count<<endl;
	//cout<<"data[1].count is "<<data[1].count<<endl;
	j1.start();

	f1.enableSyncPoint(SP_HEAP_FLR_FINISH_INSERT);
	f2.enableSyncPoint(SP_HEAP_FLR_FINISH_INSERT);
	f1.joinSyncPoint(SP_HEAP_FLR_FINISH_INSERT);
	f2.joinSyncPoint(SP_HEAP_FLR_FINISH_INSERT);

	count[0] = data[0].count;
	count[1] = data[1].count;
	count[3] = data[3].count;
	//cout<<"count is "<<count[0]<<" "<<count[1]<<" "<<count[3]<<endl;

	f3.start();
	f3.joinSyncPoint(SP_HEAP_FLR_BEFORE_EXTEND_HEAP);


	//cout<<data[3].count<<endl;
	count[2] = data[2].count;

	f1.disableSyncPoint(SP_HEAP_FLR_FINISH_INSERT);
	f2.disableSyncPoint(SP_HEAP_FLR_FINISH_INSERT);
	f1.notifySyncPoint(SP_HEAP_FLR_FINISH_INSERT);
	f2.notifySyncPoint(SP_HEAP_FLR_FINISH_INSERT);

	Thread::msleep(200);


	CPPUNIT_ASSERT(count[0] + 1 == data[0].count);
	CPPUNIT_ASSERT(count[1] + 1 == data[1].count);
	CPPUNIT_ASSERT(count[3] != data[3].count);

	Thread::msleep(1000);
	CPPUNIT_ASSERT(count[2] == data[2].count);

	f3.disableSyncPoint(SP_HEAP_FLR_BEFORE_EXTEND_HEAP);
	f3.notifySyncPoint(SP_HEAP_FLR_BEFORE_EXTEND_HEAP);

	f1.join();
	f2.join();
	f3.join();
	j1.join();

	CPPUNIT_ASSERT(ridVec.size() == rows * 3);
	CPPUNIT_ASSERT(data[0].count == rows);
	CPPUNIT_ASSERT(data[1].count == rows);
	CPPUNIT_ASSERT(data[2].count == rows);
	CPPUNIT_ASSERT(data[3].count == rows);

}



/** 机器人类 */

/* Mooly线程打开关闭一个堆 */
void Mooly::run() {
	Session *session = m_db->getSessionManager()->allocSession("Mooly", m_conn);

	DrsHeap **heap = (DrsHeap **)m_data;

	try {
		*heap = DrsHeap::open(m_db, session, m_name, m_tblDef);
	} catch (NtseException &) {
		assert(*heap == NULL);
	}


	m_db->getSessionManager()->freeSession(session);
}

/* Frank线程插入记录 */
void Frank::run() {
	struct _data {
		svector<RowId> *m_svec;
		int count;
		int rows;
	} *pdata;
	pdata = (_data *)m_data;
	int &insertCount = pdata->count;
	int &rows = pdata->rows;
	svector<RowId> *ridVec = pdata->m_svec;
	u64 startTime, endTime;
	RowId rid;
	u64 pageNum;
	s16 slotNum;
	CPPUNIT_ASSERT(m_heap);
	int i = 0;
	pageNum = 0;
	int recPerPage = 0;
	Record *rec;
	// 计时
	startTime = System::currentTimeMillis();
	for (; i < 500;) {
		rid = insertRecord(++i, &rec, m_db, m_conn, m_heap, m_tblDef);
		CPPUNIT_ASSERT(rid);
		insertCount++;
		ridVec->push_back(rid);
		if (pageNum == 0) pageNum = RID_GET_PAGE(rid);
		if (pageNum != 0 && pageNum != RID_GET_PAGE(rid) && !recPerPage)
			recPerPage = i - 1;
		//delete rec;
		freeRecord(rec);
	}
	for (i = 500; i < rows;) { // 十万条记录顺序插入测试
		rid = insertRecord(++i, &rec, m_db, m_conn, m_heap, m_tblDef);
		CPPUNIT_ASSERT(rid);
		insertCount++;
		ridVec->push_back(rid);
		pageNum = RID_GET_PAGE(rid);
		slotNum = RID_GET_SLOT(rid);
		//cout<<"i is "<<i<<endl<<"pageNum is "<<pageNum<<endl<<"slotNum is "<<slotNum<<endl;
		freeRecord(rec);
	}
	// 计时
	endTime = System::currentTimeMillis();
	//cout << "One hundred thousand inserts take time "<<endTime - startTime<<" milliseconds."<<endl;
//cout << m_name<<" terminated."<<endl;
}

/* Julia线程读出记录 */
void Julia::run() {
	struct _data {
		svector<RowId> *m_svec;
		int count;
		int rows;
	} *pdata;
	pdata = (_data *)m_data;
	int &getCount = pdata->count;
	int &rows = pdata->rows;
	svector<RowId> *ridVec = pdata->m_svec;

	RowId rid;
	int size;
	int idx; 
	bool canRead;
	Record readOut;
	byte *data = (byte *)System::virtualAlloc(Limits::PAGE_SIZE);
	readOut.m_data = data;
	readOut.m_format = REC_FIXLEN;




	int j = rows / 100;
	for (int i = 0; i < rows; ++i) {
		size = ridVec->size();
		idx = (int)((float)size * rand() / RAND_MAX);
		if (idx == size) --idx;
		//cout<<"idx is "<<idx<<endl;
		CPPUNIT_ASSERT(0 <= idx && idx < size);
		rid = (*ridVec)[idx];
		canRead = readRecord(rid, &readOut, m_db, m_conn, m_heap);
		CPPUNIT_ASSERT(canRead);
		++getCount;
		if (i % j == 0)
			Thread::msleep(20);
	}


	System::virtualFree(data);
}

/* Gump线程插入制定条记录 */
void Gump::run() {
	u64 rid;
	Record *rec;

	for (int i = 0; i < *((int *)m_data); ++i) {
		rid = insertRecord(i, &rec, m_db, m_conn, m_heap, m_tblDef);
		freeRecord(rec);
	}
}

/* Depp 线程插入并锁定一条记录 */
void Depp::run() {
	u64 rid;
	Record *rec = createRecord(999, 999, "Shakespear", 999, 999, m_tblDef);
	RowLockHandle *rlh = NULL;

	Connection *conn = m_db->getConnection(false);
	Session *s = m_db->getSessionManager()->allocSession("Depp::run", conn);

	rid = m_heap->insert(s, rec, &rlh);

	s->unlockRow(&rlh);
	m_db->getSessionManager()->freeSession(s);
	m_db->freeConnection(conn);

	freeRecord(rec);
}

/* Susan线程读取记录 */
void Susan::run() {
	Record readOut;
	byte *data = (byte *)System::virtualAlloc(Limits::PAGE_SIZE);
	readOut.m_data = data;
	readOut.m_format = REC_FIXLEN;
	bool canRead;

	int pageNum = *((int *)m_data);
	int recPerPage = *((int *)m_data + 1);

	Connection *conn = m_db->getConnection(false);
	Session *s = m_db->getSessionManager()->allocSession("Susan::run", conn);
	for (int i = 0; i < recPerPage; ++i) {
		EXCPT_OPER(canRead = m_heap->getRecord(s, RID(pageNum, i), &readOut));
		if (canRead) {
			EXCPT_OPER(m_heap->del(s, RID(pageNum, i)));
		}
	}
	m_db->getSessionManager()->freeSession(s);
	m_db->freeConnection(conn);

	System::virtualFree(data);
}

/* Sergey线程进行一次表扫描 */
void Sergey::run() {
	//bool lockrow = *(bool *)m_data;
	bool lockrow = m_lockrow;
	RowLockHandle *rlh = NULL;
	Session *session = m_db->getSessionManager()->allocSession("Sergey", m_conn);
	u16 columns[] = {0, 1};
	SubRecord subRec(REC_REDUNDANT, sizeof(columns)/sizeof(columns[0]), columns, (byte *)session->getMemoryContext()->alloc(m_tblDef->m_maxRecSize), m_tblDef->m_maxRecSize);
	DrsHeapScanHandle *hdl = m_heap->beginScan(session, SubrecExtractor::createInst(session, m_tblDef, &subRec), !lockrow ? Shared : Exclusived, !lockrow ? NULL : &rlh, false);
	while (m_heap->getNext(hdl, &subRec)) {
		if (lockrow) {
			SYNCHERE(SP_HEAP_FLR_BEFORE_UNLOCKROW);
			if (m_delCnt) {
				m_heap->deleteCurrent(hdl);
				m_delCnt--;
			}
			session->unlockRow(&rlh);
		}
	}
	m_heap->endScan(hdl);
	m_db->getSessionManager()->freeSession(session);
}

/* Michael线程锁住一条记录一段时间 */
void Michael::run() {
	Connection *conn = m_db->getConnection(true);
	Session *session = m_db->getSessionManager()->allocSession("Michael::run", conn);

	RowLockHandle *rlh = LOCK_ROW(session, m_tid, m_rid, m_lmode);
	Thread::msleep(3000);
	session->unlockRow(&rlh);

	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(conn);
}

/* 返回随机数 */
static int randomInt(int max) {
	//int ranInt = (int)(max * rand() / RAND_MAX);
	int ranInt = rand() % max;
	return ranInt;
}
