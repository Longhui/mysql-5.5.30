
/**
* 堆稳定性测试
*
* @author 谢可(xieke@corp.netease.com, ken@163.org)
*/


#include "TestSbHeap.h"
#include "Random.h"
#include "misc/RecordHelper.h"
#include "CountTable.h"
#include "BlogTable.h"
#include "DbConfigs.h"
#include "util/File.h"
#include "misc/Trace.h"

#ifndef WIN32
#include <sys/select.h>
#endif


char g_string[Limits::PAGE_SIZE+1];

const int DEFAULT_RUN_MINUTE = 60;
const int INPUT_TIMEOUT = 10;

using namespace ntse;

#define PAUSE_INTERVAL 500


ActionConfig *g_ac = NULL;		/** 全局ActionConfig的指针 */
ThreadAction g_ta = TA_MAX;		/** ta是全局标志，若ta为TA_PAUSE则暂停，为TA_STOP则停止 */



/**
 * Count表，更新ID和COUNT字段，ID为内存堆ID，COUNT为随机。
 */
class CountHeapOp : public HeapOp {
public:
	CountHeapOp(Database *db, MemHeap **memhp, double dataBufRatio);
	virtual ~CountHeapOp();
	virtual u16 *getUpdCols();
	virtual u16 getUpdColNum();
	virtual SubRecord * createSubRec(u64 ID = 0);
	virtual void updateSubRec(SubRecord *sr);
	virtual MemHeapRid getMemHeapRid(SubRecord *subRecord);
	virtual bool compareSubRecord(Session *session, MemHeapRecord *mhRec, SubRecord *subRec);
	virtual Record *createRecord(MemHeapRid mhRid);
protected:
};

class BlogHeapOp : public HeapOp {
public:
	BlogHeapOp(Database *db, MemHeap **memhp, double dataBufRatio);
	virtual ~BlogHeapOp();
	virtual u16 *getUpdCols();
	virtual u16 getUpdColNum();
	virtual SubRecord * createSubRec(u64 ID = 0);
	virtual void updateSubRec(SubRecord *sr);
	virtual MemHeapRid getMemHeapRid(SubRecord *subRecord);
	virtual bool compareSubRecord(Session *session, MemHeapRecord *mhRec, SubRecord *subRec);
	virtual Record *createRecord(MemHeapRid mhRid);
protected:
	uint m_avgRecSize;
	uint m_avgTitleSize;
	uint m_avgPermalinkSize;
	uint m_maxTitleSize;
	uint m_maxPermalinkSize;
private:
	const char * getTitleStr();
	const char * getPermalinkStr();
};

ActionConfig::ActionConfig(const uint *taProp, const uint *msaProp) {
	init(taProp, msaProp);
}

void ActionConfig::init(const uint *taProp, const uint *msaProp) {
	for (int i = 0; i < TA_MAX; ++i) {
		m_taProportion[i] = taProp[i];
	}
	for (int i = 0; i < MSA_MAX; ++i) {
		m_msaProportion[i] = msaProp[i];
	}
}

ThreadAction ActionConfig::getRandAction() {
	uint totalProp = 0;
	switch (g_ta) {
		case TA_PAUSE:
			return TA_PAUSE;
		case TA_STOP:
			return TA_STOP;
		default:
			for (int i = 0; i < TA_MAX; ++i) totalProp += m_taProportion[i];
			int actionInt = RandomGen::nextInt(0, (int)totalProp);
			assert(actionInt >= 0);
			for (int i = 0; i < TA_MAX; ++i) {
				actionInt -= m_taProportion[i];
				if (actionInt < 0) {
					return (ThreadAction)i;
				}
			}
			assert(false); // 应该不会出错，不过如果实时改内存的话就说不定了。
	}
}

MScanAction ActionConfig::getRandScanAction() {
	uint totalProp = 0;
	for (int i = 0; i < MSA_MAX; ++i) totalProp += m_msaProportion[i];
	int actionInt = RandomGen::nextInt(0, (int)totalProp);
	assert(actionInt >= 0);
	for (int i = 0; i < TA_MAX; ++i) {
		actionInt -= m_msaProportion[i];
		if (actionInt < 0) {
			return (MScanAction)i;
		}
	}
	assert(false); // 应该不会出错，不过如果实时改内存的话就说不定了。
}


ActionOperator::ActionOperator(Database *db, HeapOp *heapOp, MemHeap *memHeap, Reporter *reporter) : Thread("ActionOperator"){
	m_memHeap = memHeap;
	m_heapOp = heapOp;
	m_db = db;
	m_reporter = reporter;
	for (int i = 0; i < TA_MAX; ++i)
		m_taCnt[i] = 0;
	for (int i = 0; i < MSA_MAX; ++i)
		m_msaCnt[i] = 0;
}

ActionOperator::~ActionOperator() {
}


void ActionOperator::run() {
	ThreadAction ta;
	Connection *conn = NULL;
	int opPerConn = 0;
	while (true) {
		if (opPerConn == 0) {
			assert(!conn);	/* 应该为空 */
			opPerConn = RandomGen::nextInt(1, MAX_OP_PER_CONN + 1);
			conn = m_db->getConnection(false);
		}
		assert(conn);
		ta = g_ac->getRandAction();
		switch (ta) {
			case TA_STOP:
				goto HeapOperator_run_end;
			case TA_PAUSE:
				Thread::msleep(PAUSE_INTERVAL);
				break;
			case TA_GET:
				opGet(conn);
				break;
			case TA_INSERT:
				opIns(conn);
				break;
			case TA_UPDATE:
				opUpd(conn);
				break;
			case TA_DELETE:
				opDel(conn);
				break;
			case TA_RSCANTBL:
				opRTS(conn);
				break;
			case TA_MSCANTBL:
				opMTS(conn);
				break;
			case TA_VERIFY:
				opVry(conn);
				break;
			case TA_SAMPLE:
				opSmp(conn);
			default:
				break;
		}
		if(--opPerConn == 0) {
			m_db->freeConnection(conn);
			conn = NULL;
		}

	}
HeapOperator_run_end:
	assert(conn);
	m_db->freeConnection(conn);

	m_reporter->countActions(this);
}

void ActionOperator::opGet(Connection *conn) {
	RowId rid = m_memHeap->getRandRowId();
	if (rid == INVALID_ROW_ID) return;
	SubRecord *sr = m_heapOp->createSubRec();
	RowLockHandle *rowLock = NULL;
	Session *session = m_db->getSessionManager()->allocSession("ActionOperator::opGet", conn);
	bool exist = m_heapOp->getSubRecord(session, rid, sr, Shared, &rowLock);
	if (exist) {
		MemHeapRid mhRid = m_heapOp->getMemHeapRid(sr);
		MemHeapRecord* mhRec = m_memHeap->recordAt(session, mhRid);
		assert(mhRec);
		NTSE_ASSERT(m_heapOp->compareSubRecord(session, mhRec, sr));
		session->unlockRow(&rowLock);
	}
	assert(!rowLock);
	m_db->getSessionManager()->freeSession(session);
	freeSubRecord(sr);
	++m_taCnt[TA_GET];
}

void ActionOperator::opIns(Connection *conn) {
	MemHeapRid mhRid = m_memHeap->reserveRecord();
	if (mhRid == MemHeapRecord::INVALID_ID) return;
	Record *rec = m_heapOp->createRecord(mhRid);
	assert(rec->m_format == REC_REDUNDANT);
	RowLockHandle *rowLock = NULL;
	Session *session = m_db->getSessionManager()->allocSession("ActionOperator::opIns", conn);
	RowId rid = m_heapOp->insert(session, rec, &rowLock);	/* 这里的rec是REDUDANT格式，内部转换 */
	assert(rowLock && rowLock->getLockMode() != None);
	NTSE_ASSERT(m_memHeap->insertAt(session, mhRid, rid, rec->m_data));
	/* 验证 *
	SubRecord *sr = m_heapOp->createSubRec();
	bool exist = m_heapOp->getSubRecord(session, rid, sr, None, NULL);
	assert(exist);
	mhRid = m_heapOp->getMemHeapRid(sr);
	MemHeapRecord* mhRec = m_memHeap->recordAt(session, mhRid);
	assert(mhRec);
	NTSE_ASSERT(m_heapOp->compareSubRecord(session, mhRec, sr));
	freeSubRecord(sr);
	* end验证 */
	session->unlockRow(&rowLock);
	m_db->getSessionManager()->freeSession(session);
	freeRecord(rec);
	++m_taCnt[TA_INSERT];
}


void ActionOperator::opDel(Connection *conn) {
	RowLockHandle *rowLock = NULL;
	Session *session = m_db->getSessionManager()->allocSession("ActionOperator::opDel", conn);
	MemHeapRecord *mhRec = m_memHeap->getRandRecord(session, &rowLock, Exclusived);
	if (!mhRec) {
		m_db->getSessionManager()->freeSession(session);
		return;
	}
	assert(rowLock);
	RowId rid = mhRec->getRowId();
	NTSE_ASSERT(m_heapOp->del(session, rid));
	MemHeapRid mhrid = mhRec->getId();
	NTSE_ASSERT(m_memHeap->deleteRecord(session, mhrid));
	/* 验证 *
	SubRecord *sr = m_heapOp->createSubRec();
	bool exist = m_heapOp->getSubRecord(session, rid, sr, None, NULL);
	assert(!exist);
	freeSubRecord(sr);
	* end验证 */
	session->unlockRow(&rowLock);
	m_db->getSessionManager()->freeSession(session);
	++m_taCnt[TA_DELETE];
}

void ActionOperator::opUpd(Connection *conn) {
	RowLockHandle *rowLock = NULL;
	Session *session = m_db->getSessionManager()->allocSession("ActionOperator::opUpd", conn);
	MemHeapRecord *mhRec = m_memHeap->getRandRecord(session, &rowLock, Exclusived);
	if (!mhRec) {
		m_db->getSessionManager()->freeSession(session);
		return;
	}
	assert(rowLock);
	RowId rid = mhRec->getRowId();
	assert(rid != INVALID_ROW_ID);
	switch(RandomGen::nextInt(0,2)) {
		case 0: // SubRecord更新
			{
				SubRecord *updSr = m_heapOp->createSubRec((u64)mhRec->getId());
				NTSE_ASSERT(m_heapOp->update(session, rid, updSr));
				mhRec->update(session, m_heapOp->getUpdColNum(), m_heapOp->getUpdCols(), updSr->m_data);
				freeSubRecord(updSr);
			}
			break;
		case 1: // Record更新
			{
				Record *updRec = m_heapOp->createRecord((u64)mhRec->getId());
				NTSE_ASSERT(m_heapOp->update(session, rid, updRec));
				mhRec->update(session, updRec->m_data);
				freeRecord(updRec);
			}
			break;
		default:
			assert(false);
	}
	/* 验证 *
	SubRecord *sr = m_heapOp->createSubRec();
	NTSE_ASSERT(m_heapOp->getSubRecord(session, rid, sr, None, NULL));
	MemHeapRid mhrid = m_heapOp->getMemHeapRid(sr);
	mhRec = m_memHeap->recordAt(session, mhrid, NULL, None);
	assert(mhRec);
	NTSE_ASSERT(mhRec->compare(session, m_heapOp->getUpdColNum(), m_heapOp->getUpdCols(), sr->m_data));
	freeSubRecord(sr);
	* end验证 */
	session->unlockRow(&rowLock);
	m_db->getSessionManager()->freeSession(session);
	++m_taCnt[TA_UPDATE];
}

void ActionOperator::opRTS(Connection *conn) {
	RowLockHandle *rowLock = NULL;
	SubRecord *subRec = m_heapOp->createSubRec();
	Session *session = m_db->getSessionManager()->allocSession("ActionOperator::opRTS", conn);
	DrsHeapScanHandle *scanHdl;
	scanHdl = m_heapOp->beginScan(session, Shared, &rowLock);
	while (m_heapOp->getNext(scanHdl, subRec)) {
		assert(rowLock && rowLock->getLockMode() == Shared);
		MemHeapRid mhRid = m_heapOp->getMemHeapRid(subRec);
		MemHeapRecord* mhRec = m_memHeap->recordAt(session, mhRid);
		assert(mhRec);
		NTSE_ASSERT(m_heapOp->compareSubRecord(session, mhRec, subRec));
		session->unlockRow(&rowLock);
	}
	m_heapOp->endScan(scanHdl);
	m_db->getSessionManager()->freeSession(session);
	freeSubRecord(subRec);
	++m_taCnt[TA_RSCANTBL];
}

void ActionOperator::opMTS(Connection *conn) {
	RowLockHandle *rowLock = NULL;
	SubRecord *subRec = m_heapOp->createSubRec();
	Session *session = m_db->getSessionManager()->allocSession("ActionOperator::opRTS", conn);
	DrsHeapScanHandle *scanHdl;
	scanHdl = m_heapOp->beginScan(session, Exclusived, &rowLock, true); // returnLinkSrc为true是为了变长堆修改扫描
	while (m_heapOp->getNext(scanHdl, subRec)) {
		assert(rowLock && rowLock->getLockMode() == Exclusived);
		MScanAction msa = g_ac->getRandScanAction();
		MemHeapRid mhRid = m_heapOp->getMemHeapRid(subRec);
		MemHeapRecord* mhRec = m_memHeap->recordAt(session, mhRid);
		assert(mhRec);
		switch (msa) {
			case MSA_GET:
				NTSE_ASSERT(m_heapOp->compareSubRecord(session, mhRec, subRec));
				++m_msaCnt[MSA_GET];
				break;
			case MSA_DELETE:
				m_heapOp->deleteCurrent(scanHdl);
				m_memHeap->deleteRecord(session, mhRid);
				++m_msaCnt[MSA_DELETE];
				break;
			case MSA_UPDATE:
				m_heapOp->updateSubRec(subRec);
				m_heapOp->updateCurrent(scanHdl, subRec);
				mhRec->update(session, m_heapOp->getUpdColNum(), m_heapOp->getUpdCols(), subRec->m_data);
				assert(m_heapOp->compareSubRecord(session, mhRec, subRec));
				++m_msaCnt[MSA_UPDATE];
				break;
		}
		session->unlockRow(&rowLock);
	}
	m_heapOp->endScan(scanHdl);
	m_db->getSessionManager()->freeSession(session);
	freeSubRecord(subRec);
	++m_taCnt[TA_MSCANTBL];
}

void ActionOperator::opVry(Connection *conn) {
	RowLockHandle *rowLock = NULL;
	SubRecord *subRec = m_heapOp->createSubRec();
	Session *session = m_db->getSessionManager()->allocSession("ActionOperator::opVry", conn);
	for (uint i = 0; i < m_memHeap->getMaxRecCount(); ++i) {
		MemHeapRid mhRid = (MemHeapRid)i;
		MemHeapRecord *mhRec = m_memHeap->recordAt(session, mhRid, &rowLock, Shared);
		if (!mhRec) continue;
		CPPUNIT_ASSERT(rowLock);
		RowId rid = mhRec->getRowId();
		bool exist = m_heapOp->getSubRecord(session, rid, subRec, None, NULL);
		CPPUNIT_ASSERT(exist);
		CPPUNIT_ASSERT(m_heapOp->compareSubRecord(session, mhRec, subRec));
		session->unlockRow(&rowLock);
		++m_taCnt[TA_GET];
	}
	m_db->getSessionManager()->freeSession(session);
	freeSubRecord(subRec);
	++m_taCnt[TA_VERIFY];
}

void ActionOperator::opSmp(Connection *conn) {
	const HeapStatusEx& exStatus = m_heapOp->getStatusEx();
	cout << "Links count is " << exStatus.m_numLinks << endl;
	cout << "Rec count is " << exStatus.m_numRows << endl;
	cout << "Space used percent is " << exStatus.m_pctUsed * 100 << endl;
	++m_taCnt[TA_SAMPLE];
}

CountHeapOp::CountHeapOp(Database *db, MemHeap **memhp, double dataBufRatio) : HeapOp(db) {
	Connection *conn = m_db->getConnection(false);
	Session *session = m_db->getSessionManager()->allocSession("CountHeapOp::CountHeapOp", conn);
	try {
		File oldTable(TABLE_NAME_COUNT);
		oldTable.remove();
		DrsHeap::create(m_db, TABLE_NAME_COUNT, CountTable::getTableDef(false));
		//m_heap = DrsHeap::open(m_db, session, TABLE_NAME_COUNT);
		m_heap = DrsHeap::open(m_db, session, TABLE_NAME_COUNT, CountTable::getTableDef(false));
	} catch (NtseException &) {
		assert(false);
	}
	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(conn);

	unsigned maxRecCnt = (unsigned)(m_db->getConfig()->m_pageBufSize * Limits::PAGE_SIZE * dataBufRatio / CountTable::getRecordSize());
        //cout << "maxRecCnt is " << maxRecCnt << endl;
	*memhp = new MemHeap(maxRecCnt, m_heap->getTableDef());
}

CountHeapOp::~CountHeapOp() {
	try {
		DrsHeap::drop(TABLE_NAME_COUNT);
	} catch (NtseException &) {
		assert(false);
	}
}

u16 * CountHeapOp::getUpdCols() {
	static u16 updCols[] = {COUNT_ID_CNO, COUNT_COUNT_CNO};
	return updCols;
}

u16 CountHeapOp::getUpdColNum() {
	static u16 updCols[] = {COUNT_ID_CNO, COUNT_COUNT_CNO};
	return sizeof(updCols) / sizeof(updCols[0]);
}


SubRecord * CountHeapOp::createSubRec(u64 ID) {
	SubRecordBuilder srb(m_heap->getTableDef(), REC_REDUNDANT);
	int count = RandomGen::nextInt();
	SubRecord *sr = srb.createSubRecordByName(COUNT_ID" "COUNT_COUNT, &ID, &count);
	return sr;
}

void CountHeapOp::updateSubRec(ntse::SubRecord *sr) {
	assert(sr && sr->m_data);
	int count = RandomGen::nextInt();
	RedRecord::writeNumber(m_heap->getTableDef(), COUNT_COUNT_CNO, sr->m_data, count);
}


bool HeapOp::getSubRecord(Session *session, RowId rid, SubRecord *subRec, LockMode lockMode, RowLockHandle **rlh) {
	return m_heap->getSubRecord(session, rid, SubrecExtractor::createInst(session, m_heap->getTableDef(), subRec), subRec, lockMode, rlh);
}

MemHeapRid CountHeapOp::getMemHeapRid(ntse::SubRecord *subRecord) {
	u64 ID = RedRecord::readBigInt(m_heap->getTableDef(), subRecord->m_data, COUNT_ID_CNO);
	return (MemHeapRid)ID;
}

bool CountHeapOp::compareSubRecord(ntse::Session *session, MemHeapRecord *mhRec, ntse::SubRecord *subRec) {
	return mhRec->compare(session, getUpdColNum(), getUpdCols(), subRec->m_data);
}

Record * CountHeapOp::createRecord(MemHeapRid mhRid) {
	Record *rec = CountTable::createRecord((u64)mhRid, RandomGen::nextInt());
	assert(rec->m_format == REC_REDUNDANT);
	return rec;
}


void HeapOp::close() {
	Connection *conn = m_db->getConnection(false);
	Session *session = m_db->getSessionManager()->allocSession("HeapOp::~HeapOp", conn);
	m_heap->close(session, true);
	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(conn);
	delete m_heap;
}


RowId HeapOp::insert(Session *session, Record *record, RowLockHandle **rlh) {
	assert(record->m_format == REC_REDUNDANT);
	RowId rid;
	HeapVersion hv = DrsHeap::getVersionFromTableDef(m_heap->getTableDef());
	if (hv == HEAP_VERSION_FLR) {
		record->m_format = REC_FIXLEN; // 暂时改变
		rid = m_heap->insert(session, record, rlh);
		assert((*rlh)->getLockMode() != None);
		record->m_format = REC_REDUNDANT;
	} else {
		byte buf[Limits::PAGE_SIZE];
		Record vlrRec(0, REC_VARLEN, buf, sizeof(buf));
		assert(hv == HEAP_VERSION_VLR);
		RecordOper::convertRecordRV(m_heap->getTableDef(), record, &vlrRec);
		rid = m_heap->insert(session, &vlrRec, rlh);
		assert((*rlh)->getLockMode() != None);
	}
	return rid;
}

bool HeapOp::del(ntse::Session *session, ntse::RowId rid) {
	bool success = m_heap->del(session, rid);
	return success;
}

bool HeapOp::update(Session *session, RowId rid, SubRecord *sr) {
	bool success = m_heap->update(session, rid, sr);
	return success;
}

bool HeapOp::update(Session *session, RowId rid, Record *rec) {
	assert(rec->m_format == REC_REDUNDANT);
	bool success;
	HeapVersion hv = DrsHeap::getVersionFromTableDef(m_heap->getTableDef());
	if (hv == HEAP_VERSION_FLR) {
		rec->m_format = REC_FIXLEN; // 暂时改变
		success = m_heap->update(session, rid, rec);
		rec->m_format = REC_REDUNDANT;
	} else {
		assert(hv == HEAP_VERSION_VLR);
		byte buf[Limits::PAGE_SIZE];
		Record vlrRec;
		vlrRec.m_data = buf;
		vlrRec.m_format = REC_VARLEN;
		vlrRec.m_rowId = 0;
		vlrRec.m_size = sizeof(buf);
		RecordOper::convertRecordRV(m_heap->getTableDef(), rec, &vlrRec);
		success = m_heap->update(session, rid, &vlrRec);
	}
	return success;
}

DrsHeapScanHandle* HeapOp::beginScan(Session *session, LockMode lockMd, RowLockHandle **rlh, bool returnLinkSrc) {
	SubrecExtractor *extractor = SubrecExtractor::createInst(session->getMemoryContext(), m_heap->getTableDef(), getUpdColNum(), getUpdCols(), m_heap->getTableDef()->m_recFormat, REC_REDUNDANT);
	return m_heap->beginScan(session, extractor, lockMd, rlh, returnLinkSrc);
}

bool HeapOp::getNext(ntse::DrsHeapScanHandle *scanHdl, ntse::SubRecord *subRec) {
	return m_heap->getNext(scanHdl, subRec);
}

void HeapOp::endScan(DrsHeapScanHandle *scanHdl) {
	m_heap->endScan(scanHdl);
}

void HeapOp::deleteCurrent(DrsHeapScanHandle *scanHdl) {
	m_heap->deleteCurrent(scanHdl);
}

void HeapOp::updateCurrent(ntse::DrsHeapScanHandle *scanHdl, const ntse::SubRecord *subRecord) {
	m_heap->updateCurrent(scanHdl, subRecord);
}

const HeapStatus& HeapOp::getStatus() {
	return m_heap->getStatus();
}

const HeapStatusEx& HeapOp::getStatusEx() {
	uint maxSample = 1024;
	//maxSample = (maxSample > 4096) ? 4096 : maxSample;
	//maxSample = (maxSample < 1024) ? 1024 : maxSample;
	Connection *conn = m_db->getConnection(false);
	Session *session = m_db->getSessionManager()->allocSession(__FUNC__, conn);
	m_heap->updateExtendStatus(session, maxSample);
	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(conn);
	return m_heap->getStatusEx();
}

/*** Blog Heap ***/

BlogHeapOp::BlogHeapOp(Database *db, MemHeap **memhp, double dataBufRatio) : HeapOp(db) {
	Connection *conn = m_db->getConnection(false);
	Session *session = m_db->getSessionManager()->allocSession("BlogHeapOp::BlogHeapOp", conn);
	try {
		File oldTable(TABLE_NAME_BLOG);
		oldTable.remove();
		DrsHeap::create(m_db, TABLE_NAME_BLOG, BlogTable::getTableDef(false));
		m_heap = DrsHeap::open(m_db, session, TABLE_NAME_BLOG, CountTable::getTableDef(false));
	} catch (NtseException &) {
		assert(false);
	}
	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(conn);

	m_avgTitleSize = 128;
	m_avgPermalinkSize = 30;
	m_avgRecSize = 8 + 4 + 8 + 0 + 11 + 11 + 0 + (40 + 128); // 42 + 30 + 128 = 200
	m_maxTitleSize = Limits::PAGE_SIZE / 2;
	m_maxPermalinkSize = 255;

	unsigned maxRecCnt = (unsigned)(m_db->getConfig()->m_pageBufSize * Limits::PAGE_SIZE * dataBufRatio / m_avgRecSize);
	*memhp = new MemHeap(maxRecCnt, m_heap->getTableDef());
}

//CountHeapOp::~CountHeapOp() {
BlogHeapOp::~BlogHeapOp() {
	try {
		DrsHeap::drop(TABLE_NAME_BLOG);
	} catch (NtseException &) {
		assert(false);
	}
}

u16 * BlogHeapOp::getUpdCols() {
	static u16 updCols[] = {BLOG_ID_CNO, BLOG_TITLE_CNO, BLOG_PERMALINK_CNO};
	return updCols;
}


u16 BlogHeapOp::getUpdColNum() {
	static u16 updCols[] = {BLOG_ID_CNO, BLOG_TITLE_CNO, BLOG_PERMALINK_CNO};
	return sizeof(updCols) / sizeof(updCols[0]);
}

const char * BlogHeapOp::getTitleStr() {
	int titleSize = RandomGen::randNorm(m_avgTitleSize, 0, m_maxTitleSize);
	return &(g_string[Limits::PAGE_SIZE - titleSize]);
}

const char * BlogHeapOp::getPermalinkStr() {
	int permalinkSize = RandomGen::randNorm(m_avgPermalinkSize, 0, m_maxPermalinkSize);
	return &(g_string[Limits::PAGE_SIZE - permalinkSize]);
}

SubRecord * BlogHeapOp::createSubRec(u64 ID) {
	SubRecordBuilder srb(m_heap->getTableDef(), REC_REDUNDANT);
	const char *title = getTitleStr();
	const char *permalink = getPermalinkStr();
	SubRecord *sr = srb.createSubRecordByName(BLOG_ID" "BLOG_TITLE" "BLOG_PERMALINK, &ID, title, permalink);
	return sr;
}

void BlogHeapOp::updateSubRec(ntse::SubRecord *sr) {
	assert(sr && sr->m_data);
	const char *title = getTitleStr();
	const char *permalink = getPermalinkStr();
	RedRecord::writeVarchar(m_heap->getTableDef(), sr->m_data, BLOG_TITLE_CNO, title);
	RedRecord::writeVarchar(m_heap->getTableDef(), sr->m_data, BLOG_PERMALINK_CNO, permalink);
}

MemHeapRid BlogHeapOp::getMemHeapRid(ntse::SubRecord *subRecord) {
	u64 ID = RedRecord::readBigInt(m_heap->getTableDef(), subRecord->m_data, BLOG_ID_CNO);
	return (MemHeapRid)ID;
}

bool BlogHeapOp::compareSubRecord(ntse::Session *session, MemHeapRecord *mhRec, ntse::SubRecord *subRec) {
	return mhRec->compare(session, getUpdColNum(), getUpdCols(), subRec->m_data);
}

Record * BlogHeapOp::createRecord(MemHeapRid mhRid) {
	const char *title = getTitleStr();
	const char *permalink = getPermalinkStr();
	RecordBuilder rb(m_heap->getTableDef(), 0, REC_REDUNDANT);
	rb.appendBigInt((u64)mhRid); // BLOG_ID
	rb.appendInt(0)->appendBigInt(0);
	rb.appendVarchar(title); // BLOG_TITLE
	rb.appendNull()->appendNull();
	rb.appendVarchar(permalink);
	Record *rec = rb.getRecord();
	assert(rec->m_format == REC_REDUNDANT);
	return rec;
}




/**
 * 观察memHeap堆的空闲情况，控制比例，及时调整config指针
 */
void ConfigWatcher::watch(MemHeap *memHeap, ActionConfig **beWatched) {
	unsigned totalCnt = memHeap->getMaxRecCount();
	unsigned usedCnt = memHeap->getUsedRecCount();
	int pct = usedCnt * 100 / totalCnt;
	if (pct < m_downPct) {
		if (*beWatched != m_config->wupConf())
			*beWatched = m_config->insConf();
	} else if (pct > m_upPct) {
		*beWatched = m_config->delConf();
	}
}


HeapStabilityTest::~HeapStabilityTest() {
	delete m_resLogger;
	// 关闭内存堆
	delete m_memHeap;
	// 关闭操作堆
	m_heapOp->close();
	delete m_heapOp;
	// 关闭数据库
	m_db->close();
	Database::drop(m_cfg->m_basedir);
	delete m_cfg;
	delete m_db;
	//delete m_warmUp;
}


/**
 * 创建数据库和内存堆
 */
void HeapStabilityTest::setup() {
	m_cfg = new Config(CommonDbConfig::getSmall()); // 稳定性测试必须要用小堆
	if (m_cfg->m_maxSessions < m_cfg->m_internalSessions + m_threadCnt) {
		m_cfg->m_maxSessions = m_cfg->m_internalSessions + m_threadCnt;
	}
	Database::drop(m_cfg->m_basedir);
	File dir(m_cfg->m_basedir);
	dir.mkdir();
	NTSE_ASSERT(m_db = Database::open(m_cfg, true));

	/* 创建操作堆并且初始化内存堆 */
	if (strcmp(m_tblName, TABLE_NAME_COUNT) == 0) {
		m_heapOp = new CountHeapOp(m_db, &m_memHeap, m_dataBufRatio);
	} else if (strcmp(m_tblName, TABLE_NAME_BLOG) == 0) {
		m_heapOp = new BlogHeapOp(m_db, &m_memHeap, m_dataBufRatio);
	}
	assert(m_memHeap);
	m_resLogger = new ResLogger(m_db, 1800, "[Heap]DbResource.txt");
}



void HeapStabilityTest::configInsCfg(uint *taProp, uint *msaProp) {
	taProp[TA_GET] = 6;
	taProp[TA_INSERT] = 3;
	taProp[TA_DELETE] = 1;
}

void HeapStabilityTest::configDelCfg(uint *taProp, uint *msaProp) {
	taProp[TA_GET] = 6;
	taProp[TA_INSERT] = 1;
	taProp[TA_DELETE] = 3;
}

void HeapStabilityTest::run() {
	setup();

	ActionOperator **acOper = new ActionOperator*[m_threadCnt];
	for(int i = 0; i < m_threadCnt; ++i) {
		acOper[i] = new ActionOperator(m_db, m_heapOp, m_memHeap, m_reporter);
	}

	g_ac = m_cfgWatcher->getWarmupConf();

	for(int i = 0; i < m_threadCnt; ++i) {
		acOper[i]->start();
	}

	while (true) {
		m_cfgWatcher->watch(m_memHeap, &g_ac);
		Thread::msleep(m_cfgWatcher->getInterval());
		if (g_ta == TA_STOP)
			break;
	}

	for(int i = 0; i < m_threadCnt; ++i) {
		acOper[i]->join();
		delete acOper[i];
	}

	// 打印统计
	m_reporter->report();

	delete [] acOper;
}


void Reporter::report() {
	cout << "Heap stability test report:" << endl;
	cout << "heap get operation " << m_taCnt[TA_GET] << " times," << endl;
	cout << "heap insert operation " << m_taCnt[TA_INSERT] << " times," << endl;
	cout << "heap delete operation " << m_taCnt[TA_DELETE] << " times," << endl;
	cout << "heap update operation " << m_taCnt[TA_UPDATE] << " times," << endl;
	cout << "heap read-only table scan operation " << m_taCnt[TA_RSCANTBL] << " times," <<endl;
	cout << "heap modify table scan operation " << m_taCnt[TA_MSCANTBL] << " times," <<endl;
	cout << "     get current    " << m_msaCnt[MSA_GET] << " times," << endl;
	cout << "     delete current " << m_msaCnt[MSA_DELETE] << " times," << endl;
	cout << "     update current " << m_msaCnt[MSA_UPDATE] << " times," << endl;
	cout << "reverse verify " << m_taCnt[TA_VERIFY] << " times, " << endl;
	cout << "sample " << m_taCnt[TA_SAMPLE] << " times, " << endl;
	cout << "That is all." << endl;
}

void Reporter::countActions(ActionOperator *aOp) {
	LOCK(&m_lock);
	for (int i = 0; i < TA_MAX; ++i) {
		m_taCnt[i] += aOp->m_taCnt[i];
	}
	for (int i = 0; i < MSA_MAX; ++i) {
		m_msaCnt[i] += aOp->m_msaCnt[i];
	}
	UNLOCK(&m_lock);
}





void HeapSbTestConfig::setProp(ActionConfig *cfg, ThreadAction ta, uint prop) {
	assert(ta < TA_MAX);
	cfg->m_taProportion[ta] = prop;
}

void HeapSbTestConfig::setProp(ActionConfig *cfg, uint *props) {
	for (int i = 0; i < TA_MAX; ++i) {
		cfg->m_taProportion[i] = props[i];
	}
}

void HeapSbTestConfig::setMsaProp(ActionConfig *cfg, MScanAction ma, uint prop) {
	assert(ma < MSA_MAX);
	cfg->m_msaProportion[ma] = prop;
}


/*** 延时获取测试数据 ***/
#if 0
int waitGetInput() {
	int minute;
	/*
	cout << "How many minutes would you like to run this test?" << endl;
	cin >> minute;
	cout << "This test will run for " << minute << " minutes." << endl;
	*/

	return 0;
}
#endif


int readIntFromStdin(int defaultVal, int timeoutSec) {
	int value;
#ifdef WIN32
	cin >> value;
#else
	fd_set fds;
	timeval tv;
	FD_ZERO(&fds);
	FD_SET(fileno(stdin), &fds);
	tv.tv_sec = timeoutSec;
	tv.tv_usec = 0;
	int input = select(fileno(stdin) + 1, &fds, NULL, NULL, &tv);
	if (input >= 0 && FD_ISSET(fileno(stdin), &fds)) {
		cin >> value;
	} else
		value = defaultVal;
#endif
	return value;
}


/*** 测试用例 ***/

void HeapStabilityTestCase::setUp() {
	memset(g_string, 'a', Limits::PAGE_SIZE);
	g_string[Limits::PAGE_SIZE] = '\0';
}

void HeapStabilityTestCase::tearDown() {
}

void HeapStabilityTestCase::testInsDelUpd() {
	Reporter reporter;
	HeapSbTestConfig config;
	config.setWUpProp(TA_INSERT, 1);
	config.setInsProp(TA_GET, 15);
	config.setInsProp(TA_INSERT, 9);
	config.setInsProp(TA_DELETE, 3);
	config.setInsProp(TA_UPDATE, 3);
	config.setDelProp(TA_GET, 15);
	config.setDelProp(TA_INSERT, 9);
	config.setDelProp(TA_DELETE, 3);
	config.setDelProp(TA_UPDATE, 3);
	ConfigWatcher watcher(20, 80, &config);
	HeapStabilityTest idTest(TABLE_NAME_COUNT, 10, &watcher,&reporter);

	idTest.start();
	Thread::msleep(1000 * 60 * 2);
	g_ta = TA_STOP;
	idTest.join();
}



void HeapStabilityTestCase::testFLRHeapCpuConsume() {
	int minute;
	cout << endl << "How many minutes would you like to run this test?" << endl;
	minute = readIntFromStdin(DEFAULT_RUN_MINUTE, INPUT_TIMEOUT);
	cout << "This test will run for " << minute << " minutes." << endl;
	Reporter reporter;
	HeapSbTestConfig config;
	/* TA prop setting */
	/* warmup */
	config.setWUpProp(TA_INSERT, 1);
	/* insert config */
	config.setInsProp(TA_GET, 6000000);
	config.setInsProp(TA_INSERT, 3000000);
	config.setInsProp(TA_DELETE, 1000000);
	config.setInsProp(TA_UPDATE, 1000000);
	config.setInsProp(TA_RSCANTBL, 20);
	config.setInsProp(TA_MSCANTBL, 20);
	config.setInsProp(TA_VERIFY, 1);
	config.setInsProp(TA_SAMPLE, 1);
	/* delete config */
	config.setDelProp(TA_GET, 6000000);
	config.setDelProp(TA_INSERT, 1000000);
	config.setDelProp(TA_DELETE, 3000000);
	config.setDelProp(TA_UPDATE, 1000000);
	config.setDelProp(TA_RSCANTBL, 20);
	config.setDelProp(TA_MSCANTBL, 20);
	config.setDelProp(TA_VERIFY, 1);
	config.setDelProp(TA_SAMPLE, 1);
	/* MSA prop setting */
	config.setInsMsaProp(MSA_GET, 30);
	config.setInsMsaProp(MSA_DELETE, 1);
	config.setInsMsaProp(MSA_UPDATE, 3);
	config.setDelMsaProp(MSA_GET, 30);
	config.setDelMsaProp(MSA_DELETE, 2);
	config.setDelMsaProp(MSA_UPDATE, 3);
	ConfigWatcher watcher(20, 80, &config);
	HeapStabilityTest idTest(TABLE_NAME_COUNT, 500, &watcher, &reporter, 0.7);

	idTest.start();

	u64 startTime = System::currentTimeMillis();
	u64 endTime;
	while (true) {
		endTime = startTime + 1000 * 60 * minute;
		if (System::currentTimeMillis() >= endTime)
			break;
		Thread::msleep(5000);
	}
	g_ta = TA_STOP;
	idTest.join();
}


void HeapStabilityTestCase::testFLRHeapIoConsume() {
	int minute;
	cout << endl << "How many minutes would you like to run this test?" << endl;
	minute = readIntFromStdin(DEFAULT_RUN_MINUTE, INPUT_TIMEOUT);
	cout << "This test will run for " << minute << " minutes." << endl;
	Reporter reporter;
	HeapSbTestConfig config;
	/* TA prop setting */
	/* warmup */
	config.setWUpProp(TA_INSERT, 1);
	/* insert config */
	config.setInsProp(TA_GET, 6000000);
	config.setInsProp(TA_INSERT, 3000000);
	config.setInsProp(TA_DELETE, 1000000);
	config.setInsProp(TA_UPDATE, 1000000);
	config.setInsProp(TA_RSCANTBL, 20);
	config.setInsProp(TA_MSCANTBL, 20);
	config.setInsProp(TA_VERIFY, 1);
	config.setInsProp(TA_SAMPLE, 1);
	/* delete config */
	config.setDelProp(TA_GET, 6000000);
	config.setDelProp(TA_INSERT, 1000000);
	config.setDelProp(TA_DELETE, 3000000);
	config.setDelProp(TA_UPDATE, 1000000);
	config.setDelProp(TA_RSCANTBL, 20);
	config.setDelProp(TA_MSCANTBL, 20);
	config.setDelProp(TA_VERIFY, 1);
	config.setDelProp(TA_SAMPLE, 1);
	/* MSA prop setting */
	config.setInsMsaProp(MSA_GET, 30);
	config.setInsMsaProp(MSA_DELETE, 1);
	config.setInsMsaProp(MSA_UPDATE, 3);
	config.setDelMsaProp(MSA_GET, 30);
	config.setDelMsaProp(MSA_DELETE, 2);
	config.setDelMsaProp(MSA_UPDATE, 3);
	ConfigWatcher watcher(20, 80, &config);
	HeapStabilityTest idTest(TABLE_NAME_COUNT, 500, &watcher, &reporter, 10);

	idTest.start();
	//Thread::msleep(1000 * 60 * minute);
	u64 startTime = System::currentTimeMillis();
	u64 endTime;
	while (true) {
		endTime = startTime + 1000 * 60 * minute;
		if (System::currentTimeMillis() >= endTime)
			break;
		Thread::msleep(5000);
	}
	g_ta = TA_STOP;
	idTest.join();
}




void HeapStabilityTestCase::testVLRHeapIoConsume() {
	int minute;
	cout << endl << "How many minutes would you like to run this test?" << endl;
	minute = readIntFromStdin(DEFAULT_RUN_MINUTE, INPUT_TIMEOUT);
	cout << "This test will run for " << minute << " minutes." << endl;
	Reporter reporter;
	HeapSbTestConfig config;
	/* TA prop setting */
	/* warmup */
	config.setWUpProp(TA_INSERT, 1);
	/* insert config */
	config.setInsProp(TA_GET, 600000);
	config.setInsProp(TA_INSERT, 300000);
	config.setInsProp(TA_DELETE, 100000);
	config.setInsProp(TA_UPDATE, 100000);
	config.setInsProp(TA_RSCANTBL, 20);
	config.setInsProp(TA_MSCANTBL, 20);
	config.setInsProp(TA_VERIFY, 1);
	config.setInsProp(TA_SAMPLE, 1);
	/* delete config */
	config.setDelProp(TA_GET, 600000);
	config.setDelProp(TA_INSERT, 100000);
	config.setDelProp(TA_DELETE, 300000);
	config.setDelProp(TA_UPDATE, 100000);
	config.setDelProp(TA_RSCANTBL, 20);
	config.setDelProp(TA_MSCANTBL, 20);
	config.setDelProp(TA_VERIFY, 1);
	config.setDelProp(TA_SAMPLE, 1);
	/* MSA prop setting */
	config.setInsMsaProp(MSA_GET, 30);
	config.setInsMsaProp(MSA_DELETE, 1);
	config.setInsMsaProp(MSA_UPDATE, 3);
	config.setDelMsaProp(MSA_GET, 30);
	config.setDelMsaProp(MSA_DELETE, 2);
	config.setDelMsaProp(MSA_UPDATE, 3);
	ConfigWatcher watcher(20, 80, &config);
	HeapStabilityTest idTest(TABLE_NAME_BLOG, 500, &watcher, &reporter, 10);

	idTest.start();
	//Thread::msleep(1000 * 60 * minute);
	u64 startTime = System::currentTimeMillis();
	u64 endTime;
	while (true) {
		endTime = startTime + 1000 * 60 * minute;
		if (System::currentTimeMillis() >= endTime)
			break;
		Thread::msleep(5000);
	}
	g_ta = TA_STOP;
	idTest.join();
}


void HeapStabilityTestCase::testVLRHeapCpuConsume() {
	//Tracer::init();
	//ts.hp = true;
	int minute;
	cout << endl << "How many minutes would you like to run this test?" << endl;
	minute = readIntFromStdin(DEFAULT_RUN_MINUTE, INPUT_TIMEOUT);
	cout << "This test will run for " << minute << " minutes." << endl;
	Reporter reporter;
	HeapSbTestConfig config;
	/* TA prop setting */
	/* warmup */
	config.setWUpProp(TA_INSERT, 1);
	/* insert config */
	config.setInsProp(TA_GET, 600000);
	config.setInsProp(TA_INSERT, 300000);
	config.setInsProp(TA_DELETE, 100000);
	config.setInsProp(TA_UPDATE, 100000);
	config.setInsProp(TA_RSCANTBL, 20);
	config.setInsProp(TA_MSCANTBL, 20);
	config.setInsProp(TA_VERIFY, 1);
	config.setInsProp(TA_SAMPLE, 1);
	/* delete config */
	config.setDelProp(TA_GET, 600000);
	config.setDelProp(TA_INSERT, 100000);
	config.setDelProp(TA_DELETE, 300000);
	config.setDelProp(TA_UPDATE, 100000);
	config.setDelProp(TA_RSCANTBL, 20);
	config.setDelProp(TA_MSCANTBL, 20);
	config.setDelProp(TA_VERIFY, 1);
	config.setDelProp(TA_SAMPLE, 1);
	/* MSA prop setting */
	config.setInsMsaProp(MSA_GET, 30);
	config.setInsMsaProp(MSA_DELETE, 1);
	config.setInsMsaProp(MSA_UPDATE, 3);
	config.setDelMsaProp(MSA_GET, 30);
	config.setDelMsaProp(MSA_DELETE, 2);
	config.setDelMsaProp(MSA_UPDATE, 3);
	ConfigWatcher watcher(20, 80, &config);
	HeapStabilityTest idTest(TABLE_NAME_BLOG, 500, &watcher, &reporter, 0.7);

	idTest.start();
	//Thread::msleep(1000 * 60 * minute);
	u64 startTime = System::currentTimeMillis();
	u64 endTime;
	while (true) {
		endTime = startTime + 1000 * 60 * minute;
		if (System::currentTimeMillis() >= endTime)
			break;
		Thread::msleep(5000);
	}
	g_ta = TA_STOP;
	idTest.join();
}

