#include "TestMHeapRecord.h"
#include "misc/TableDef.h"
#include "misc/RecordHelper.h"
#include "misc/MemCtx.h"
#include "util/Stream.h"

#define TNT_STU_NAME "name"
#define TNT_STU_SNO "sno"
#define TNT_STU_AGE "age"
#define TNT_STU_SEX "sex"
#define TNT_STU_CLASS "class"
#define TNT_STU_GPA "gpa"
#define TNT_STU_GRADE "grade"

const char* MHeapRecordTestCase::getName() {
	return "TNT heap record test";
}

const char* MHeapRecordTestCase::getDescription() {
	return "Test various operations of tnt heap record which is located in tnt heap page";
}

bool MHeapRecordTestCase::isBig() {
	return false;
}

void MHeapRecordTestCase::setUp() {
	TableDefBuilder tb(1, "Olympic", "student");
	tb.addColumnS(TNT_STU_NAME, CT_VARCHAR, 30, false, false, COLL_LATIN1);
	tb.addColumn(TNT_STU_SNO, CT_INT);
	tb.addColumn(TNT_STU_AGE, CT_SMALLINT);
	tb.addColumnS(TNT_STU_SEX, CT_CHAR, 2, false, true, COLL_LATIN1);
	tb.addColumn(TNT_STU_CLASS, CT_MEDIUMINT);
	tb.addColumn(TNT_STU_GPA, CT_FLOAT);
	PrType prtype;
	prtype.setUnsigned();
	tb.addColumnN(TNT_STU_GRADE, CT_BIGINT, prtype);
	m_tableDef = tb.getTableDef();

	m_rec1 = createStudentRec(201, "163", 1501, 16, "M", 8, 98);
	m_rec2 = createStudentRec(202, "netease", 1511, 19, "F", 6, 59);

	m_heapRec1 = new MHeapRec(500005/*txnId*/, 1002/*rollBackId*/, 15/*vTblIndex*/, m_rec1, 1/*del*/);
	m_heapRec2 = new MHeapRec(501234/*txnId*/, 1018/*rollBackId*/, 16/*vTblIndex*/, m_rec2, 0/*del*/);
}

void MHeapRecordTestCase::tearDown() {
	delete m_tableDef;
	delete m_heapRec1;
	delete m_heapRec2;
	delete[] m_rec1->m_data;
	delete[] m_rec2->m_data;
	delete m_rec1;
	delete m_rec2;
}

Record* MHeapRecordTestCase::createStudentRec(RowId rid, const char *name, u32 stuNo, u16 age, const char *sex, u32 clsNo, u64 grade) {
	RecordBuilder rb(m_tableDef, rid, REC_VARLEN);
	rb.appendVarchar(name);
	rb.appendInt(stuNo);
	rb.appendSmallInt(age);
	rb.appendChar(sex);
	rb.appendMediumInt(clsNo);
	rb.appendNull();
	rb.appendBigInt(grade);
	
	return rb.getRecord();
}

void MHeapRecordTestCase::compareHeapRec(MHeapRec *heapRec1, MHeapRec *heapRec2) {
	CPPUNIT_ASSERT(heapRec1->m_size == heapRec2->m_size);
	CPPUNIT_ASSERT(heapRec1->m_txnId == heapRec2->m_txnId);
	CPPUNIT_ASSERT(heapRec1->m_rollBackId == heapRec2->m_rollBackId);
	CPPUNIT_ASSERT(heapRec1->m_vTableIndex == heapRec2->m_vTableIndex);
	CPPUNIT_ASSERT(heapRec1->m_del == heapRec2->m_del);

	CPPUNIT_ASSERT(heapRec1->m_rec.m_format == heapRec2->m_rec.m_format);
	CPPUNIT_ASSERT(heapRec1->m_rec.m_rowId == heapRec2->m_rec.m_rowId);
	CPPUNIT_ASSERT(heapRec1->m_rec.m_size == heapRec2->m_rec.m_size);
	CPPUNIT_ASSERT(memcmp(heapRec1->m_rec.m_data, heapRec2->m_rec.m_data, heapRec2->m_rec.m_size) == 0);
}

void MHeapRecordTestCase::testHeapRecSerialize() {
	u64 txnId = 1002;
	RowId rollBackId = 5004;
	u8 vTblIndex = 8;
	Record *rec = NULL;
	u8 del = 1;

	u16 size = m_heapRec1->getSerializeSize();
	byte *data = new byte[size];
	Stream s1(data, size);
	m_heapRec1->serialize(&s1);

	CPPUNIT_ASSERT(s1.getSize() == size);

	Stream s2(data, size);
	MemoryContext ctx(Limits::PAGE_SIZE, 2);
	u64 sp = ctx.setSavepoint();
	MHeapRec *heapRec1 = MHeapRec::unSerialize(&s2, &ctx);

	CPPUNIT_ASSERT(size == heapRec1->m_size);
	compareHeapRec(m_heapRec1, heapRec1);

	ctx.resetToSavepoint(sp);
	delete rec;
	delete[] data;
}

void MHeapRecordTestCase::testGetFreeGrade() {
	u32 totalFreeSize = MRecordPage::TNTIM_PAGE_SIZE - sizeof(MRecordPage);
	u16 size = (u16)(totalFreeSize*0.05);
	u8 grade = MRecordPage::getFreeGrade(size);
	CPPUNIT_ASSERT(grade == 0);

	size = (u16)(totalFreeSize*0.25);
	grade = MRecordPage::getFreeGrade(size);
	CPPUNIT_ASSERT(grade == 1);

	size = (u16)(totalFreeSize*0.6);
	grade = MRecordPage::getFreeGrade(size);
	CPPUNIT_ASSERT(grade == 2);

	size = (u16)(totalFreeSize*0.8);
	grade = MRecordPage::getFreeGrade(size);
	CPPUNIT_ASSERT(grade == 3);
}

void MHeapRecordTestCase::appendRecord(MRecordPage *page, u32 total) {
	MemoryContext ctx(Limits::PAGE_SIZE, 2);
	u64 sp = ctx.setSavepoint();
	u64 sp1 = 0;
	u32 i = 0;
	s16** slots = (s16 **)ctx.alloc(sizeof(s16*)*total);
	

	u16 size1 = m_heapRec1->getSerializeSize();
	byte *data1 = (byte *)ctx.alloc(sizeof(byte)*size1);
	Stream s1(data1, size1);
	m_heapRec1->serialize(&s1);
	CPPUNIT_ASSERT(s1.getSize() == size1);

	u16 size2 = m_heapRec2->getSerializeSize();
	byte *data2 = (byte *)ctx.alloc(sizeof(byte)*size2);
	Stream s2(data2, size2);
	m_heapRec2->serialize(&s2);
	CPPUNIT_ASSERT(s2.getSize() == size2);

	for (i = 0; i < total; i++) {
		if ((i % 2) == 0) {
			slots[i] = page->appendRecord(data1, size1);
		} else {
			slots[i] = page->appendRecord(data2, size2);
		}
	}

	CPPUNIT_ASSERT(page->m_recordCnt == total);

	MHeapRec *heapRec = NULL;
	byte *rec = NULL;
	u16 size = 0;
	for (i = 0; i < total; i++) {
		sp1 = ctx.setSavepoint();
		CPPUNIT_ASSERT(page->getRecord(slots[i], &rec, &size));
		Stream s(rec, size);
		heapRec = MHeapRec::unSerialize(&s, &ctx);
		if ((i % 2) == 0) {
			CPPUNIT_ASSERT(size == size1);
			compareHeapRec(heapRec, m_heapRec1);
		} else {
			CPPUNIT_ASSERT(size == size2);
			compareHeapRec(heapRec, m_heapRec2);
		}
		ctx.resetToSavepoint(sp1);
	}

	ctx.resetToSavepoint(sp);
}

void MHeapRecordTestCase::testAppendRecord() {
	u32 total = 20;
	MRecordPage *page = (MRecordPage *)malloc(MRecordPage::TNTIM_PAGE_SIZE*sizeof(byte));
	page->init(MPT_MHEAP, m_tableDef->m_id);
	appendRecord(page, total);
	free(page);
}

void MHeapRecordTestCase::testUpdateRecord() {
	u32 i = 0;
	u32 total = 20;
	MRecordPage *page = (MRecordPage *)malloc(MRecordPage::TNTIM_PAGE_SIZE*sizeof(byte));
	page->init(MPT_MHEAP, m_tableDef->m_id);
	appendRecord(page, total);

	u16 freeSize = page->m_freeSize;
	u16 recordSize = page->m_recordCnt;

	//m_heapRec2的记录大于m_heapRec1
	MemoryContext ctx(Limits::PAGE_SIZE, 1);
	u64 sp = ctx.setSavepoint();
	u16 size1 = m_heapRec1->getSerializeSize();
	byte *data1 = (byte *)ctx.alloc(sizeof(byte)*size1);
	Stream s1(data1, size1);
	m_heapRec1->serialize(&s1);

	u16 size2 = m_heapRec2->getSerializeSize();
	byte *data2 = (byte *)ctx.alloc(sizeof(byte)*size2);
	Stream s2(data2, size2);
	m_heapRec2->serialize(&s2);

	s16 *ret = NULL;
	s16 *slot = (s16 *)((byte *)page + sizeof(MRecordPage));
	for (i = 0; i < total; i++) {
		//因为m_heapRec2的记录大于m_heapRec1，所以m_heapRec2更新为m_heapRec1时为原地更新
		if (i % 2 == 1) {
			ret = page->updateRecord(slot + i, data1, size1);
			CPPUNIT_ASSERT(ret == (slot + i));
		}
	}
	CPPUNIT_ASSERT(page->m_recordCnt == recordSize);

	MHeapRec *heapRec = NULL;
	byte *rec = NULL;
	u16 size = 0;
	u64 sp1 = 0;

	slot = (s16 *)((byte *)page + sizeof(MRecordPage));
	for (i = 0; i < total; i++) {
		sp1 = ctx.setSavepoint();
		CPPUNIT_ASSERT(page->getRecord(slot + i, &rec, &size));
		Stream s(rec, size);
		heapRec = MHeapRec::unSerialize(&s, &ctx);
		CPPUNIT_ASSERT(size == size1);
		compareHeapRec(heapRec, m_heapRec1);
		ctx.resetToSavepoint(sp1);
	}

	//处理增长更新
	slot = (s16 *)((byte *)page + sizeof(MRecordPage));
	for (i = 0; i < total; i++) {
		//因为m_heapRec2的记录大于m_heapRec1,所以需要处理增长更新,slot位置会发生变更
		if (i % 2 == 0) {
			ret = page->updateRecord(slot + i, data2, size2);
			CPPUNIT_ASSERT(ret != (slot + i));
		}
	}

	u16 rec1Count = 0, rec2Count = 0; 
	for (i = 0; (byte *)(slot + i) < (byte *)page + page->m_lastSlotOffset; i++) {
		if (*(slot + i) == -1) {
			continue;
		}
		sp1 = ctx.setSavepoint();
		CPPUNIT_ASSERT(page->getRecord(slot + i, &rec, &size));
		Stream s(rec, size);
		heapRec = MHeapRec::unSerialize(&s, &ctx);
		if (heapRec->m_rec.m_rowId == m_heapRec1->m_rec.m_rowId) {
			CPPUNIT_ASSERT(size == size1);
			compareHeapRec(heapRec, m_heapRec1);
			rec1Count++;
		} else {
			CPPUNIT_ASSERT(size == size2);
			compareHeapRec(heapRec, m_heapRec2);
			rec2Count++;
		}
		ctx.resetToSavepoint(sp1);
	}
	CPPUNIT_ASSERT(rec1Count == rec2Count);
	CPPUNIT_ASSERT(rec1Count + rec2Count == total);
	CPPUNIT_ASSERT(page->m_freeSize == freeSize);
	CPPUNIT_ASSERT(page->m_recordCnt == recordSize);

	free(page);
}

void MHeapRecordTestCase::testRemoveRecord() {
	u32 i = 0;
	u32 total = 20;
	MRecordPage *page = (MRecordPage *)malloc(MRecordPage::TNTIM_PAGE_SIZE*sizeof(byte));
	page->init(MPT_MHEAP, m_tableDef->m_id);
	appendRecord(page, total);
	s16 *slot = (s16 *)((byte *)page + sizeof(MRecordPage));
	for (i = 0; i < total; i++) {
		if (i % 2 == 0) {
			page->removeRecord(slot + i);
		}
	}
	CPPUNIT_ASSERT(page->m_recordCnt == (total/2));

	u64 sp = 0;
	MemoryContext ctx(Limits::PAGE_SIZE, 1);
	MHeapRec *heapRec = NULL;
	byte *rec = NULL;
	u16 size = 0;
	for (i = 0; i < (total/2); i++) {
		sp = ctx.setSavepoint();
		CPPUNIT_ASSERT(page->getLastRecord(&slot, &rec, &size));
		Stream s(rec, size);
		heapRec = MHeapRec::unSerialize(&s, &ctx);
		CPPUNIT_ASSERT(heapRec->getSerializeSize() == size);
		compareHeapRec(heapRec, m_heapRec2);
		page->removeRecord(slot);
		ctx.resetToSavepoint(sp);
	}

	CPPUNIT_ASSERT(!page->getLastRecord(&slot, &rec, &size));

	free(page);
}

void MHeapRecordTestCase::testDefrag() {
	u32 total = 10;
	if (MRecordPage::TNTIM_PAGE_SIZE == 8192) {
		total = 100;
	} else if (MRecordPage::TNTIM_PAGE_SIZE == 4096) {
		total = 50;
	}
	testDefrag(total);
	testDefrag(total + 1);

	size_t pageSize = MRecordPage::TNTIM_PAGE_SIZE*sizeof(byte);
	MRecordPage *page = (MRecordPage *)malloc(pageSize);
	memset(page, 0, pageSize);
	page->init(MPT_MHEAP, m_tableDef->m_id);
	appendRecord(page, total);
	MRecordPage *page1 = (MRecordPage *)malloc(pageSize);
	memcpy(page1, page, pageSize);
	page->defrag(NULL);
	CPPUNIT_ASSERT(!memcmp(page, page1, pageSize));
	free(page1);
	free(page);
}

void MHeapRecordTestCase::testDefrag(u32 total) {
	u32 i = 0;
	MRecordPage *page = (MRecordPage *)malloc(MRecordPage::TNTIM_PAGE_SIZE*sizeof(byte));
	page->init(MPT_MHEAP, m_tableDef->m_id);
	appendRecord(page, total);
	u16 freeSize = page->m_freeSize;
	u16 totalRemoveSize = 0;
	s16 *slot = (s16 *)((byte *)page + sizeof(MRecordPage));
	for (i = 0; i < total; i++) {
		if (i % 2 == 0) {
			page->removeRecord(slot + i);
			totalRemoveSize += m_heapRec1->getSerializeSize() + sizeof(s16);
		}
	}
	CPPUNIT_ASSERT(page->m_recordCnt == (total/2));

	page->defrag(NULL);
	CPPUNIT_ASSERT(page->m_freeSize == (freeSize + totalRemoveSize));
	CPPUNIT_ASSERT(page->m_freeSize == (page->m_lastRecordOffset - page->m_lastSlotOffset));
	CPPUNIT_ASSERT(sizeof(MRecordPage) + sizeof(s16)*page->m_recordCnt == page->m_lastSlotOffset);
	u64 sp = 0;
	MemoryContext ctx(Limits::PAGE_SIZE, 1);
	MHeapRec *heapRec = NULL;
	byte *rec = NULL;
	u16 size = 0;
	for (i = 0; i < page->m_recordCnt; i++) {
		sp = ctx.setSavepoint();
		CPPUNIT_ASSERT(page->getRecord(slot + i, &rec, &size));
		Stream s(rec, size);
		heapRec = MHeapRec::unSerialize(&s, &ctx);
		compareHeapRec(heapRec, m_heapRec2);
		ctx.resetToSavepoint(sp);
	}

	free(page);
}