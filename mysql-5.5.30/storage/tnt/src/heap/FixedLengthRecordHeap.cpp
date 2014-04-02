/**
 * 定长记录堆
 *
 * @author 谢可(ken@163.org)
 */

#include "heap/FixedLengthRecordHeap.h"
#include "api/Database.h"
#include "misc/Buffer.h"
#include "util/File.h"
#include "util/Stream.h"
#include "misc/Record.h"
#include "misc/Session.h"
#include "misc/Syslog.h"
#include "misc/LockManager.h"
#include "misc/Global.h"
#include "misc/TableDef.h"
#include "misc/Trace.h"
#include "misc/Verify.h"
#include <cassert>
#include <set>
#include "misc/Profile.h"

#ifdef TNT_ENGINE
#include "trx/TNTTransaction.h"
#endif

#ifdef NTSE_UNIT_TEST //测试用
#include "util/Thread.h"
#endif

using namespace ntse;

/**
 * Txn日志格式
 * insert log: RowId(u64) Flags(u8) ( New FreePageListHeader(u64) ) RecordLen(u16) RecordData(byte*)
 * delete log: RowId(u64) Flags(u8) ( Old FreePageListHeader(u64) )
 * update log: RowId(u64) isSubRecord(bool) m_numCols(u16) m_columns(u16*m_numCols) m_size(u16) m_data
 */

/**
 * system log shortcut
 */
#define LOG_DEBUG(fmt,...)   this->m_db->getSyslog()->log(EL_DEBUG, fmt, ##__VA_ARGS__)
#define LOG_LOG(fmt,...)   this->m_db->getSyslog()->log(EL_LOG, fmt, ##__VA_ARGS__)
#define LOG_WARN(fmt,...)   this->m_db->getSyslog()->log(EL_WARN, fmt, ##__VA_ARGS__)
#define LOG_ERR(fmt,...)   this->m_db->getSyslog()->log(EL_ERROR, fmt, ##__VA_ARGS__)
#define LOG_PANIC(fmt,...)   this->m_db->getSyslog()->log(EL_PANIC, fmt, ##__VA_ARGS__)

enum FLRHEAP_SAMPLE_FIELDS{
	FSF_NUMRECS,
	FSF_MAX,
};

/**
 * 定长堆构造函数
 *
 * @param db 数据库指针
 * @param heapFile 对应的堆文件指针
 * @param headerPage 堆头页
 * @param dbObjStats 数据对象状态
 * @throw NtseException 解析表定义失败
 */
FixedLengthRecordHeap::FixedLengthRecordHeap(Database *db, const TableDef *tableDef, File *heapFile, BufferPageHdr *headerPage, DBObjStats* dbObjStats) throw(NtseException):
	DrsHeap(db, tableDef, heapFile, headerPage, dbObjStats) {
	assert(db && heapFile && headerPage);
	ftrace(ts.hp, tout << heapFile << (HeapHeaderPageInfo *)headerPage;);
	m_version = HEAP_VERSION_FLR;
	m_dboStats = dbObjStats;

	m_slotLength = m_tableDef->m_maxRecSize + SLOT_FLAG_LENGTH;
	m_recPerPage = (Limits::PAGE_SIZE - OFFSET_PAGE_RECORD) / m_slotLength;
	if (m_recPerPage > Limits::MAX_SLOT)
		m_recPerPage = Limits::MAX_SLOT;
	m_freePageRecLimit = (uint)((float)m_recPerPage / (float)FREE_PAGE_RATIO);
	if (m_freePageRecLimit == 0)
		m_freePageRecLimit = 1;
}

/**
 * @see DrsHeap::getRecord
 */
bool FixedLengthRecordHeap::getRecord(Session *session, RowId rowId, Record *record,
	LockMode lockMode, RowLockHandle **rlh, bool duringRedo) {
	PROFILE(PI_FixedLengthRecordHeap_getRecord);

	assert(session && record);
	assert(record->m_format == REC_FIXLEN);
	assert((lockMode == None && rlh == NULL) || (lockMode != None && rlh != NULL));

	u64 pageNum = RID_GET_PAGE(rowId);
	s16 slotNum = RID_GET_SLOT(rowId);
	if (!duringRedo && pageNum > m_maxUsedPageNum)
		return false;
	if (slotNum >= (s16)m_recPerPage || !pageNum)
		return false;

	if (rlh) {
		*rlh = LOCK_ROW(session, m_tableDef->m_id, rowId, lockMode);
	}

	BufferPageHandle *pageHdl = GET_PAGE(session, m_heapFile, PAGE_HEAP, pageNum, Shared, m_dboStats, NULL);
	assert(NULL != pageHdl);
	SYNCHERE(SP_HEAP_FLR_GET_GOT_PAGE);

	bool exist = readRecord((FLRHeapRecordPageInfo *)pageHdl->getPage(), slotNum, record);
	session->releasePage(&pageHdl);
	
	if (exist) {
		record->m_rowId = rowId;
		/* 更新统计信息 */
		++m_status.m_rowsReadRecord;
	} else {
		if (rlh) session->unlockRow(rlh);
	}

	return exist;
}

/**
 * @see DrsHeap::getSubRecord
 */
bool FixedLengthRecordHeap::getSubRecord(Session *session, RowId rowId, SubrecExtractor *extractor, SubRecord *subRecord, LockMode lockMode, RowLockHandle **rlh) {
	PROFILE(PI_FixedLengthRecordHeap_getSubRecord);

	assert(session && subRecord);
	assert(subRecord->m_format == REC_REDUNDANT);
	assert((lockMode == None && rlh == NULL) || (lockMode != None && rlh != NULL));

	u64 pageNum = RID_GET_PAGE(rowId);
	u16 slotNum = RID_GET_SLOT(rowId);
	if (pageNum > m_maxUsedPageNum || slotNum >= m_recPerPage || !pageNum) {
		return false;
	}

	if (rlh) {
		*rlh = LOCK_ROW(session, m_tableDef->m_id, rowId, lockMode);
	}

	BufferPageHandle *pageHdl = GET_PAGE(session, m_heapFile, PAGE_HEAP, pageNum, Shared, m_dboStats, NULL);
	SYNCHERE(SP_HEAP_FLR_GET_GOT_PAGE);

	bool exist = readSubRecord((FLRHeapRecordPageInfo *)pageHdl->getPage(), slotNum, extractor, subRecord);
	session->releasePage(&pageHdl);
	if (exist) {
		subRecord->m_rowId = rowId;
		/* 更新统计信息 */
		++m_status.m_rowsReadSubRec;
	} else {
		if (rlh) session->unlockRow(rlh);
	}

	return exist;
}

/**
 * @see DrsHeap::insert
 */
RowId FixedLengthRecordHeap::insert(Session *session, const Record *record, RowLockHandle **rlh) throw(NtseException) {
	PROFILE(PI_FixedLengthRecordHeap_insert);

	assert(session && record);
	assert(record->m_format == REC_FIXLEN);

	u64 pageNum = 0, newListHeader;
	BufferPageHandle *freePageHdl;
	FLRHeapRecordPageInfo *freePage;
	BufferPageHandle *headerPageHdl;
	FLRHeapHeaderPageInfo *headerPage;
	s16 slotNum, slotNumAfterRelock;
	bool rowLocked = false;
	u64 rowId;
#ifdef TNT_ENGINE
	RowId tntLockRid = INVALID_ROW_ID;
#endif

insert_start:
	headerPageHdl = NULL;
	headerPage = NULL;
	freePageHdl = findFreePage(session, &pageNum);
	assert(freePageHdl);
	freePage = (FLRHeapRecordPageInfo *)freePageHdl->getPage();
	verify_ex(vs.hp, verifyPage(freePage));
#ifdef NTSE_TRACE
	bool inListBefore = freePage->m_inFreePageList;
#endif

insert_get_slot_info:
	/* 取得一个空闲槽号 */
	slotNum = freePage->m_firstFreeSlot;
	assert(-1 != slotNum); // 对于findFreePage应有此成立
	assert(slotNum >= 0 && slotNum < (s16)m_recPerPage);
	rowId = RID(pageNum, (u16)slotNum);

insert_check_page:
	if (freePage->m_recordNum == m_recPerPage - 1 && freePage->m_inFreePageList){ //仅剩一个空闲记录槽
		SYNCHERE(SP_HEAP_FLR_INSERT_BEFORE_REVERSE_LOCK_HEADER_PAGE);
		headerPageHdl = lockHeaderPage(session, Exclusived);
		headerPage = (FLRHeapHeaderPageInfo *)headerPageHdl->getPage();
		if (headerPage->m_firstFreePageNum != pageNum) { //现在的页面不是空闲页面链表首页
			unlockHeaderPage(session, &headerPageHdl);
			session->releasePage(&freePageHdl);
			if (rowLocked) {
				assert(*rlh);
				session->unlockRow(rlh);
				rowLocked = false;
			}
			goto insert_start;
		}
	}

	/* 如果需要，锁定行锁 */
	if (rlh && !rowLocked) {
		SYNCHERE(SP_HEAP_FLR_INSERT_BEFORE_TRY_LOCK_ROW);
		*rlh = TRY_LOCK_ROW(session, m_tableDef->m_id, rowId, Exclusived);
		if (!*rlh) { // TRY LOCK不成功
			// 释放已经获得的首页资源
			SYNCHERE(SP_HEAP_FLR_INSERT_TRY_LOCK_ROW_FAILED);
			if (headerPage) {
				unlockHeaderPage(session, &headerPageHdl);
				headerPage = NULL;
			}
			session->unlockPage(&freePageHdl);
			SYNCHERE(SP_HEAP_FLR_INSERT_UNLOCK_PAGE_AFTER_TRY_LOCK_ROW_FAILED);
			// 行锁定
			*rlh = LOCK_ROW(session, m_tableDef->m_id, rowId, Exclusived);
			// 然后锁定页，并进行验证
			LOCK_PAGE_HANDLE(session, freePageHdl, Exclusived);
			// 因为是保持pin时unlockPage，所以freePage页面句柄没变动
#ifdef NTSE_TRACE
			inListBefore = freePage->m_inFreePageList;
#endif
			slotNumAfterRelock = freePage->m_firstFreeSlot;
			if (slotNumAfterRelock == slotNum) { // slot依旧可用，这很好
				rowLocked = true;
				goto insert_check_page;
			}
			if (slotNumAfterRelock == -1) { //没有空闲slot啦
				session->unlockRow(rlh);
				session->releasePage(&freePageHdl);
				SYNCHERE(SP_HEAP_FLR_INSERT_No_Free_Slot_After_Relock);
				goto insert_start;
			} else { //slot不可用，但是页面并不空
				session->unlockRow(rlh);
				goto insert_get_slot_info;
			}
		} else { // TRY LOCK成功
			rowLocked = true;
		}
#ifdef TNT_ENGINE
		if (rowId != tntLockRid) {
			tnt::TNTTransaction *trx = session->getTrans();
			if (trx != NULL && m_tableDef->isTNTTable()) {
				if (tntLockRid != INVALID_ROW_ID) {
					trx->unlockRow(TL_X, tntLockRid, m_tableDef->m_id);
				}

				if (!trx->tryLockRow(TL_X, rowId, m_tableDef->m_id)) {
					session->unlockRow(rlh);
					if (headerPage != NULL) {
						unlockHeaderPage(session, &headerPageHdl);
						headerPage = NULL;
					}
					session->releasePage(&freePageHdl);
					//trx锁没加上，此时不马上restart，而是等持有该锁的事务结束
					//等到持有该行的事务锁后，重新寻找空闲slot进行check，
					//如果slot变更，需要释放原持有的事务锁
					try {
						trx->lockRow(TL_X, rowId, m_tableDef->m_id);
					} catch (NtseException &e) {
						// 抛错时已释放所有资源
						throw e;
					}
					tntLockRid = rowId;
					//trx->unlockRow(TL_X, rowId, m_tableDef->m_id);
					rowLocked = false;
					goto insert_start;
				}
			}
		}
#endif
	}

	/* 更新空闲槽链表头指针，写记录 */
	freePage->m_firstFreeSlot = getSlot(freePage, slotNum)->u.m_nextFreeSlot;
	writeRecord(freePage, slotNum, record);
	freePage->m_recordNum++;
	assert(freePage->m_recordNum > 0);
	
#ifdef TNT_ENGINE
	tnt::TNTTransaction *trx = session->getTrans();
	if (trx != NULL && m_tableDef->isTNTTable()) {
		assert(INVALID_ROW_ID != rowId);
		writeInsertTNTLog(session, m_tableDef->m_id, trx->getTrxId(), trx->getTrxLastLsn(), rowId);
		//assert(INVALID_LSN != lsn);
	}
#endif

	if (freePage->m_inFreePageList && freePage->m_recordNum == m_recPerPage) {
		// 将当前页从空闲页链表头中删除
		// 这种情况下我们已经持有了堆首页（在刚才的判断分支中得到的）
		assert(headerPage);
		headerPage->m_firstFreePageNum = freePage->m_nextFreePageNum;
		newListHeader = freePage->m_nextFreePageNum;
		freePage->m_nextFreePageNum = 0;
		freePage->m_inFreePageList = false;

#ifndef NTSE_VERIFY_EX
		u64 lsn = writeInsertLog(session, rowId, record, true, newListHeader);
#else
		u64 lsn = writeInsertLog(session, rowId, record, true, newListHeader, freePage->m_bph.m_lsn, headerPage->m_hhpi.m_bph.m_lsn);
#endif
		freePage->m_bph.m_lsn = lsn;
		headerPage->m_hhpi.m_bph.m_lsn = lsn;

		session->markDirty(headerPageHdl);
		unlockHeaderPage(session, &headerPageHdl);
	} else {// 不需要将页面从空闲页链表中删除
#ifndef NTSE_VERIFY_EX
		u64 lsn = writeInsertLog(session, rowId, record, false, 0);
#else
		u64 lsn = writeInsertLog(session, rowId, record, false, 0, freePage->m_bph.m_lsn, 0);
#endif
		freePage->m_bph.m_lsn = lsn;
	}

	session->markDirty(freePageHdl);
	verify_ex(vs.hp, verifyPage(freePage));
	assert(freePage->m_firstFreeSlot >= -1 && freePage->m_firstFreeSlot < (s16)m_recPerPage);
#ifdef NTSE_TRACE
	bool inListAfter = freePage->m_inFreePageList;
#endif
	session->releasePage(&freePageHdl);
	SYNCHERE(SP_HEAP_FLR_FINISH_INSERT);
	ftrace(ts.hp, tout << session << record << rid(rowId) << inListBefore << inListAfter);

	m_dboStats->countIt(DBOBJ_ITEM_INSERT);

	return rowId;
}

/**
 * 写INSERT日志
 * @param session              会话
 * @param rid                  插入的记录RowId
 * @param record               插入的记录
 * @param headerPageModified   首页是否修改
 * @param newListHeader        新的空闲页面链表首指针
 * @return                     日志lsn
 */
#ifndef NTSE_VERIFY_EX
u64 FixedLengthRecordHeap::writeInsertLog(Session *session, RowId rid, const Record *record, bool headerPageModified, u64 newListHeader) {
#else
u64 FixedLengthRecordHeap::writeInsertLog(Session *session, RowId rid, const Record *record, bool headerPageModified, u64 newListHeader, u64 oldLSN, u64 hdOldLSN) {
#endif
	byte logData[Limits::PAGE_SIZE];
	Stream s(logData, sizeof(logData));
	try {
#ifdef NTSE_VERIFY_EX
		s.write(oldLSN)->write(hdOldLSN);
#endif
		s.write(rid);						// rowId
		s.write(headerPageModified);		// headerPageModified, 堆头页是否被改变
		if (headerPageModified) s.write(newListHeader); // 依据flags写入headerList
		s.write(record->m_size)->write(record->m_data, record->m_size); // data长度和data
	} catch (NtseException &) {
		assert(false);	// 这里不可能出现异常
	}
	return session->writeLog(LOG_HEAP_INSERT, m_tableDef->m_id, logData, s.getSize());
}

/**
 * 解析Insert日志
 * @param log          日志内容
 * @param logSize      日志大小
 * @param rid          insert记录的RowId
 * @param record OUT   分析日志后赋值给该记录，记录内存已经足够
 * @param headerPageModified   首页是否修改
 * @param newListHeader        新的空闲页面链表首指针
 */
#ifndef NTSE_VERIFY_EX
void FixedLengthRecordHeap::parseInsertLog(const byte *log, uint logSize, RowId *rid, Record *record, bool *headerPageModified, u64 *newListHeader) {
#else
void FixedLengthRecordHeap::parseInsertLog(const byte *log, uint logSize, RowId *rid, Record *record, bool *headerPageModified, u64 *newListHeader, u64 *oldLSN, u64 *hdOldLSN) {
#endif
	assert(log && rid && record && headerPageModified && newListHeader && logSize > 0);
	Stream s((byte *)log, logSize);
	/* 读取数据 */
#ifdef NTSE_VERIFY_EX
	s.read(oldLSN)->read(hdOldLSN);
#endif
	s.read(rid)->read(headerPageModified);
	if (*headerPageModified) s.read(newListHeader);
	s.read(&record->m_size);
	memcpy(record->m_data, log + s.getSize(), record->m_size);
}

void FixedLengthRecordHeap::getRecordFromInsertlog(LogEntry *log, Record *outRec) {
	assert(outRec);
	assert(outRec->m_format == REC_FIXLEN);
	bool headerPageModified;
	u64 newListHeader;
#ifdef NTSE_VERIFY_EX
	u64 oldLSN, hdOldLSN;
#endif
	Stream s(log->m_data, log->m_size);
#ifdef NTSE_VERIFY_EX
	s.read(&oldLSN)->read(&hdOldLSN);
#endif
	s.read(&outRec->m_rowId);
	s.read(&headerPageModified);
	if (headerPageModified) s.read(&newListHeader);
	s.read(&outRec->m_size);
	assert(outRec->m_size <= Limits::MAX_REC_SIZE);
	outRec->m_data = log->m_data + s.getSize();
}


/**
 * 寻找一个含有空闲空间的页面
 * @pre 调用者不能锁定堆头页
 * @post 空闲页已经被用互斥锁锁定
 * @param session 会话对象
 * @param pageNum OUT，存放页面号
 * @return 找到的空闲页面句柄
 */
BufferPageHandle* FixedLengthRecordHeap::findFreePage(Session *session, u64 *pageNum) {
	assert(session && pageNum);
	ftrace(ts.hp, tout << session << this;);
	BufferPageHandle *headerPageHdl;
	FLRHeapHeaderPageInfo *headerPage;
	BufferPageHandle *freePageHdl;
	FLRHeapRecordPageInfo *freePage;
	u64 firstFreePageNum;

findFreePage_begin:
	/* 取得首页共享锁 */
	headerPageHdl = lockHeaderPage(session, Shared);
	assert(headerPageHdl);
	SYNCHERE(SP_HEAP_FLR_FINDFREEPAGE_1ST_LOCK_HEADER_PAGE);
	headerPage = (FLRHeapHeaderPageInfo *)headerPageHdl->getPage();
	/* 获得一个空闲页面地址后释放首页 */
	firstFreePageNum = headerPage->m_firstFreePageNum;
	unlockHeaderPage(session, &headerPageHdl);
	headerPage = NULL;

findFreePage_checkPage:
	if (0 != firstFreePageNum) { // 空闲页面不为0，说明有空闲空间
		*pageNum = firstFreePageNum;
		/* 锁住这个页 */
		SYNCHERE(SP_HEAP_FLR_INSERT_BEFORE_LOCK_A_USEFULL_PAGE);
		freePageHdl = GET_PAGE(session, m_heapFile, PAGE_HEAP, firstFreePageNum, Exclusived, m_dboStats, NULL);
		freePage = (FLRHeapRecordPageInfo *)freePageHdl->getPage();
		assert(NULL != freePage);
		verify_ex(vs.hp, verifyPage(freePage));

		if (-1 == freePage->m_firstFreeSlot) { // 没有空闲槽了，重新来过
			session->releasePage(&freePageHdl);
			goto findFreePage_begin;
		}

		if (firstFreePageNum > m_maxUsedPageNum) {
			headerPageHdl = lockHeaderPage(session, Exclusived);
			headerPage = (FLRHeapHeaderPageInfo *)headerPageHdl->getPage();
			if (headerPage->m_hhpi.m_maxUsed < firstFreePageNum) {
				headerPage->m_hhpi.m_maxUsed = firstFreePageNum;
				m_maxUsedPageNum = firstFreePageNum;
				session->markDirty(headerPageHdl);
			}
			unlockHeaderPage(session, &headerPageHdl);
		}

	} else {// 没有空闲页面
		SYNCHERE(SP_HEAP_FLR_FINDFREEPAGE_WANT_TO_EXTEND);
		headerPageHdl = lockHeaderPage(session, Exclusived);
		headerPage = (FLRHeapHeaderPageInfo *)headerPageHdl->getPage();
		firstFreePageNum = headerPage->m_firstFreePageNum;

		if (0 != firstFreePageNum) {// 这说明其他线程创建了空闲页面
			unlockHeaderPage(session, &headerPageHdl);
			goto findFreePage_checkPage;
		} else {
			/* 我们获得了首页，但是没有空闲页面了 */
			*pageNum = headerPage->m_hhpi.m_maxUsed = m_maxUsedPageNum = m_maxPageNum + 1;
			SYNCHERE(SP_HEAP_FLR_BEFORE_EXTEND_HEAP);
			extendHeapFile(session, (HeapHeaderPageInfo *)headerPage);
			freePageHdl = GET_PAGE(session, m_heapFile, PAGE_HEAP, *pageNum, Exclusived, m_dboStats, NULL);
			assert(NULL != freePageHdl);

			session->markDirty(headerPageHdl);
			unlockHeaderPage(session, &headerPageHdl);
		}
	}//else of if (0 != firstFreePageNum)
	nftrace(ts.hp, tout << *pageNum;);
	return freePageHdl;
}

/**
 * 扩展堆之后续工作
 * @param extendSize    扩展大小
 */
void FixedLengthRecordHeap::afterExtendHeap(uint extendSize) {
	/* 更新首页信息 */
	((FLRHeapHeaderPageInfo *)m_headerPage)->m_firstFreePageNum = m_maxPageNum - extendSize + 1;
}


/**
 * @see DrsHeap::initExtendedNewPages
 */
void FixedLengthRecordHeap::initExtendedNewPages(Session *session, uint size) {
	for (uint i = 0; i < size; ++i) {
		u64 pageNum = m_maxPageNum + i + 1; // maxPageNum还没有改变
		BufferPageHandle *pageHdl = NEW_PAGE(session, m_heapFile, PAGE_HEAP, pageNum, Exclusived, m_dboStats);
		FLRHeapRecordPageInfo *page = (FLRHeapRecordPageInfo *)pageHdl->getPage();
		memset(page, 0, Limits::PAGE_SIZE);
		page->m_bph.m_lsn = 0;
		page->m_bph.m_checksum = BufferPageHdr::CHECKSUM_NO;
		page->m_inFreePageList = true;
		page->m_recordNum = 0;
		page->m_firstFreeSlot	= 0;
		/* nextFreePageNum依次指向下一页 */
		if (i < size - 1)
			page->m_nextFreePageNum = pageNum + 1;
		else
			page->m_nextFreePageNum = 0;
		/* 初始化每一个slot */
		for (uint j = 0; j < m_recPerPage; j++) {
			FLRSlotInfo *slot = getSlot(page, (s16)j);
			slot->m_free = true;
			if (j < m_recPerPage - 1)
				slot->u.m_nextFreeSlot = (s16)(j + 1);
			else
				slot->u.m_nextFreeSlot = -1;
		}
		verify_ex(vs.hp, verifyPage(page));		
		session->markDirty(pageHdl);
		session->releasePage(&pageHdl);
	}
	m_buffer->batchWrite(session, m_heapFile, PAGE_HEAP, m_maxPageNum + 1, m_maxPageNum + size);
}

/**
 * @see DrsHeap::del
 */
bool FixedLengthRecordHeap::del(Session *session, RowId rowId) {
	PROFILE(PI_FixedLengthRecordHeap_del);

	assert(session);
	BufferPageHandle *headerPageHdl;
	u64 oldListHeader, pageNum = RID_GET_PAGE(rowId);
	s16 slotNum = s16(RID_GET_SLOT(rowId));
	if (pageNum > m_maxUsedPageNum || slotNum >= (s16)m_recPerPage)
		return false;

	BufferPageHandle *pageHdl = GET_PAGE(session, m_heapFile, PAGE_HEAP, pageNum, Exclusived, m_dboStats, NULL);
	FLRHeapRecordPageInfo *page = (FLRHeapRecordPageInfo *)pageHdl->getPage();
#ifdef NTSE_TRACE
	bool inListBefore = page->m_inFreePageList;
#endif
	verify_ex(vs.hp, verifyPage(page));
	assert(NULL != page);

	bool exist = deleteRecord(page, slotNum);

	if (exist) {
		if (!page->m_inFreePageList && (m_recPerPage - page->m_recordNum >= m_freePageRecLimit)) {
			/* 将页面加入到空闲页链表头 */
			headerPageHdl = lockHeaderPage(session, Exclusived);
			assert(NULL != headerPageHdl);
			FLRHeapHeaderPageInfo *headerPage = (FLRHeapHeaderPageInfo *)headerPageHdl->getPage();
			oldListHeader = headerPage->m_firstFreePageNum;
			assert(oldListHeader != pageNum);
			page->m_nextFreePageNum = oldListHeader;
			headerPage->m_firstFreePageNum = pageNum;
			page->m_inFreePageList = true;

#ifndef NTSE_VERIFY_EX
			u64 lsn = writeDeleteLog(session, rowId, true, oldListHeader);
#else
			u64 lsn = writeDeleteLog(session, rowId, true, oldListHeader, page->m_bph.m_lsn, headerPage->m_hhpi.m_bph.m_lsn);
#endif

			headerPage->m_hhpi.m_bph.m_lsn = lsn;
			page->m_bph.m_lsn = lsn;
			session->markDirty(headerPageHdl);
			unlockHeaderPage(session, &headerPageHdl);
		} else {
#ifndef NTSE_VERIFY_EX
			u64 lsn = writeDeleteLog(session, rowId, false, 0);
#else
			u64 lsn = writeDeleteLog(session, rowId, false, 0, page->m_bph.m_lsn, 0);
#endif
			page->m_bph.m_lsn = lsn;
		}
		session->markDirty(pageHdl);
		
		m_dboStats->countIt(DBOBJ_ITEM_DELETE);
	}
	verify_ex(vs.hp, verifyPage(page));
#ifdef NTSE_TRACE
	bool inListAfter = page->m_inFreePageList;
#endif
	session->releasePage(&pageHdl);
	ftrace(ts.hp, tout << session << rid(rowId) << exist << inListBefore << inListAfter);

	return exist;
}

/**
 *写DELETE日志
 * @param session              会话
 * @param rid                  删除记录的RowId
 * @param headerPageModified   首页是否修改
 * @param oldListHeader        如果首页被修改，那么老的空闲页面链表首页
 * @return                     日志lsn
 */
#ifndef NTSE_VERIFY_EX
u64 FixedLengthRecordHeap::writeDeleteLog(Session *session, RowId rid, bool headerPageModified, u64 oldListHeader) {
	byte logData[32];
#else
u64 FixedLengthRecordHeap::writeDeleteLog(Session *session, RowId rid, bool headerPageModified, u64 oldListHeader, u64 oldLSN, u64 hdOldLSN) {
	byte logData[48];
#endif
	Stream s(logData, sizeof(logData));
	try {
		// rowId
#ifdef NTSE_VERIFY_EX
		s.write(oldLSN)->write(hdOldLSN);
#endif
		s.write(rid);
		// headerPageModified, 堆头页是否被改变
		s.write(headerPageModified);
		if (headerPageModified) s.write(oldListHeader);
	} catch (NtseException &) {
		assert(false);	// 这里不可能出现异常
	}
	return session->writeLog(LOG_HEAP_DELETE, m_tableDef->m_id, logData, s.getSize());
}

/**
 * 读DELETE日志
 * @param log          日志内容
 * @param logSize      日志大小
 * @param rid          insert记录的RowId
 * @param headerPageModified   首页是否修改
 * @param oldListHeader        如果首页被修改，那么老的空闲页面链表首页
 */
#ifndef NTSE_VERIFY_EX
void FixedLengthRecordHeap::parseDeleteLog(const byte *log, uint logSize, RowId *rid, bool *headerPageModified, u64 *oldListHeader) {
#else
void FixedLengthRecordHeap::parseDeleteLog(const byte *log, uint logSize, RowId *rid, bool *headerPageModified, u64 *oldListHeader, u64 *oldLSN, u64* hdOldLSN) {
#endif
	Stream s((byte *)log, logSize);

	/* 读取数据 */
#ifdef NTSE_VERIFY_EX
	s.read(oldLSN)->read(hdOldLSN);
#endif
	s.read(rid);
	s.read(headerPageModified);
	if (*headerPageModified) s.read(oldListHeader);
}

/**
 * @see DrsHeap::update
 */
bool FixedLengthRecordHeap::update(Session *session, RowId rowId, const SubRecord *subRecord) {
	PROFILE(PI_FixedLengthRecordHeap_update_SubRecord);
	assert(session && rowId && subRecord);
	assert(subRecord->m_format == REC_REDUNDANT);
	ftrace(ts.hp, tout << session << rid(rowId) << subRecord;);
	u64 pageNum = RID_GET_PAGE(rowId);
	s16 slotNum = RID_GET_SLOT(rowId);
	if (pageNum > m_maxUsedPageNum || slotNum >= (s16)m_recPerPage || !pageNum)
		return false;

	BufferPageHandle *pageHdl = GET_PAGE(session, m_heapFile, PAGE_HEAP, pageNum, Exclusived, m_dboStats, NULL);
	FLRHeapRecordPageInfo *page = (FLRHeapRecordPageInfo *)pageHdl->getPage();
	verify_ex(vs.hp, verifyPage(page));
	bool exist = writeSubRecord(page, slotNum, subRecord);
	if (exist) {
#ifndef NTSE_VERIFY_EX
		u64 lsn = writeUpdateLog(session, rowId);
#else
		u64 lsn = writeUpdateLog(session, rowId, page->m_bph.m_lsn);
#endif
		page->m_bph.m_lsn = lsn;
		session->markDirty(pageHdl);
		/* 更新统计信息 */
		++m_status.m_rowsUpdateSubRec;
	}
	session->releasePage(&pageHdl);
	nftrace(ts.hp, tout << exist;);

	return exist;
}

/**
 * @see DrsHeap::update
 */
bool FixedLengthRecordHeap::update(Session *session, RowId rowId, const Record *record) {
	PROFILE(PI_FixedLengthRecordHeap_update_Record);

	assert(session && rowId && record);
	assert(record->m_format == REC_FIXLEN);
	ftrace(ts.hp, tout << session << rid(rowId) << record;);
	u64 pageNum = RID_GET_PAGE(rowId);
	s16 slotNum = RID_GET_SLOT(rowId);
	if (pageNum > m_maxUsedPageNum || slotNum >= (s16)m_recPerPage || !pageNum)
		return false;
	BufferPageHandle *pageHdl = GET_PAGE(session, m_heapFile, PAGE_HEAP, pageNum, Exclusived, m_dboStats, NULL);
	FLRHeapRecordPageInfo *page = (FLRHeapRecordPageInfo *)pageHdl->getPage();
	verify_ex(vs.hp, verifyPage(page));
	bool exist = !getSlot(page, slotNum)->m_free;
	if (exist) {
		writeRecord(page, slotNum, record);
#ifndef NTSE_VERIFY_EX
		u64 lsn = writeUpdateLog(session, rowId);
#else
		u64 lsn = writeUpdateLog(session, rowId, page->m_bph.m_lsn);
#endif
		page->m_bph.m_lsn = lsn;
		session->markDirty(pageHdl);
		/* 更新统计信息 */
		++m_status.m_rowsUpdateRecord;
	}
	session->releasePage(&pageHdl);
	nftrace(ts.hp, tout << exist);

	return exist;
}

/**
 * 写update日志
 * @param session 会话
 * @param rowId 更新的RowId
 * @return 日志的lsn
 */
#ifndef NTSE_VERIFY_EX
u64 FixedLengthRecordHeap::writeUpdateLog(Session *session, RowId rowId) {
#else
u64 FixedLengthRecordHeap::writeUpdateLog(Session *session, RowId rowId, u64 oldLSN) {
#endif
	assert(session);
	byte logData[20];
	Stream s(logData, sizeof(logData));
	try {
#ifdef NTSE_VERIFY_EX
		s.write(oldLSN);
#endif
		s.write(rowId);						// rowId
	} catch (NtseException &) {
		assert(false);	// 这里不可能出现异常
	}
	return session->writeLog(LOG_HEAP_UPDATE, m_tableDef->m_id, logData, s.getSize());
}

/**
 *读Update日志
 * @param log          日志内容
 * @param logSize      日志大小
 * @param rid          insert记录的RowId
 */
#ifndef NTSE_VERIFY_EX
void FixedLengthRecordHeap::parseUpdateLog(const byte *log, uint logSize, RowId *rid) {
#else
void FixedLengthRecordHeap::parseUpdateLog(const byte *log, uint logSize, RowId *rid, u64 *oldLSN) {
#endif
	assert(log && rid && logSize > 0);
	Stream s((byte *)log, logSize);

	/* 读取数据 */
#ifdef NTSE_VERIFY_EX
	s.read(oldLSN);
#endif
	s.read(rid);
}

/**
 * 初始化一块内存作为定长记录堆的首页
 * @param headerPage  首页内存
 * @param tableDef  表定义
 * @throw NtseException  表定义有错、写出界等
 */
void FixedLengthRecordHeap::initHeader(BufferPageHdr *headerPage) throw(NtseException) {
	assert(headerPage);
	FLRHeapHeaderPageInfo *headerInfo = (FLRHeapHeaderPageInfo *)headerPage;
	/* 日志由上层记录，且为幂等操作，不需要记录LSN */
	headerInfo->m_hhpi.m_bph.m_lsn = 0;
	headerInfo->m_hhpi.m_bph.m_checksum = BufferPageHdr::CHECKSUM_NO;
	headerInfo->m_hhpi.m_version = HEAP_VERSION_FLR;
	headerInfo->m_hhpi.m_pageNum = 0;
	headerInfo->m_hhpi.m_maxUsed = 0;
	headerInfo->m_firstFreePageNum = 0;
}

/* 记录操作 */


/**
 * 将一条完整的记录写入到页面的特定记录槽中
 * 不用处理空闲槽链表问题，因为写未必是会引起链表变化的
 *
 * @param page  页面
 * @param slotNum  记录槽号
 * @param record 需要写入的记录。
 */
void FixedLengthRecordHeap::writeRecord(FLRHeapRecordPageInfo *page, s16 slotNum, const Record *record) {
	assert(m_slotLength - SLOT_FLAG_LENGTH == record->m_size);
	FLRSlotInfo *slot = getSlot(page, slotNum);
	memcpy(&slot->u.m_data, record->m_data, record->m_size);
	slot->m_free = false;
}

/**
 * 读取一条记录
 * @param page  页面
 * @param slotNum  记录槽号
 * @param record 需要读取数据的记录。
 * @return 成功读取返回true，记录槽为空返回false
 */
bool FixedLengthRecordHeap::readRecord(FLRHeapRecordPageInfo *page, s16 slotNum, Record *record) {
	assert(record  && record->m_data );
	FLRSlotInfo *slot = getSlot(page, slotNum);
	if (slot->m_free)
		return false;
	memcpy(record->m_data, &slot->u.m_data, m_slotLength - SLOT_FLAG_LENGTH);
	record->m_size = m_slotLength - SLOT_FLAG_LENGTH;
	return true;
}

/**
 * 读取一条记录的某些属性
 * @param page  页面
 * @param slotNum  记录槽号
 * @param extractor 子记录提取器
 * @param subRecord 需要读取的属性集合
 * @return 成功读取返回true，记录槽为空返回false
 */
bool FixedLengthRecordHeap::readSubRecord(FLRHeapRecordPageInfo* page, s16 slotNum, SubrecExtractor *extractor, SubRecord *subRecord) {
    assert(page && slotNum >= 0 && (uint)slotNum < m_recPerPage && subRecord && subRecord->m_data);
	FLRSlotInfo* slot = getSlot(page, slotNum);
	if (slot->m_free)
		return false;
	Record record(INVALID_ROW_ID, REC_FIXLEN, slot->u.m_data, m_slotLength - SLOT_FLAG_LENGTH);
	extractor->extract(&record, subRecord);
	return true;
}

/**
 * 写入某些记录的属性值进行更新
 * @param page  页面
 * @param slotNum  记录槽号
 * @param subRecord 需要写入的记录。
 * @return 成功写入返回true，记录槽空闲返回false
 */
bool FixedLengthRecordHeap::writeSubRecord(FLRHeapRecordPageInfo* page, s16 slotNum, const SubRecord *subRecord) {
	assert(page && slotNum >= 0 && (uint)slotNum < m_recPerPage && subRecord && subRecord->m_data);
	if (getSlot(page, slotNum)->m_free)
		return false;
	Record record;
	constructRecord(page, slotNum, &record);
	RecordOper::updateRecordFR(m_tableDef, &record, subRecord);
	return true;
}


/**
 * 删除一条记录，需要处理页面空闲槽链表问题。
 * @param page  记录页面
 * @param slotNum  记录所在槽号
 * @return  记录槽为空返回false，成功删除返回true
 */
bool FixedLengthRecordHeap::deleteRecord(FLRHeapRecordPageInfo *page, s16 slotNum) {
	assert(page && slotNum >= 0 && m_recPerPage > (uint)slotNum);
	FLRSlotInfo *slot = getSlot(page, slotNum);
	if (slot->m_free)
		return false; // 空槽，无法删除
	slot->u.m_nextFreeSlot = page->m_firstFreeSlot;
	slot->m_free = true;
	page->m_firstFreeSlot = slotNum;
	--page->m_recordNum;
	return true;
}

/**
 * 构造一个定长堆记录
 *
 * @param page 数据面
 * @param slotNum 记录槽号
 * @param record 记录
 */
void FixedLengthRecordHeap::constructRecord(FLRHeapRecordPageInfo *page, s16 slotNum, Record *record) {
	assert(page && record && slotNum >= 0 && (uint)slotNum < m_recPerPage);
	record->m_format = REC_FIXLEN;
	record->m_data = (byte *)(&(getSlot(page, slotNum)->u.m_data));
	record->m_size = this->m_slotLength - SLOT_FLAG_LENGTH;
}

/**
 * @see DrsHeap::beginScan
 */
DrsHeapScanHandle* FixedLengthRecordHeap::beginScan(Session *session, SubrecExtractor *extractor, LockMode lockMode, RowLockHandle **rlh, bool returnLinkSrc) {
	PROFILE(PI_FixedLengthRecordHeap_beginScan);
	assert(session);
	ftrace(ts.hp, tout << session << extractor << lockMode << rlh << returnLinkSrc);

	UNREFERENCED_PARAMETER(returnLinkSrc);
	DrsHeapScanHandle *scanHdl = new DrsHeapScanHandle(this, session, extractor, lockMode, rlh);

	m_dboStats->countIt(DBOBJ_SCAN);
	return scanHdl;
}

/**
 * @see DrsHeap::getNext
 */
bool FixedLengthRecordHeap::getNext(DrsHeapScanHandle *scanHandle, SubRecord *subRec) {
	PROFILE(PI_FixedLengthRecordHeap_getNext);

	assert(scanHandle && subRec && subRec->m_data);
	assert(subRec->m_format == REC_REDUNDANT);

	u64 rowId = scanHandle->getNextPos();
	u64 pageNum= RID_GET_PAGE(rowId);
	u16 slotNum= RID_GET_SLOT(rowId);
	Session *session = scanHandle->getSession();

	BufferPageHandle *pageHdl = scanHandle->getPage();
	/*
	 getPage结果有三种可能
	 1.NULL，扫描刚刚启动
	 2.非NULL，扫描到一页正中
	 3.非NULL，这页其实已经扫描完
	 */
	FLRHeapRecordPageInfo *page;
	FLRSlotInfo *slot = NULL;

	/* 找到下一个可用的记录 */
	for (;;) {
		/* 进入这个循环，必须没有页面锁。可能持有页面pin */
beginScan_scan_start:
        /* 跳过第一页，当记录槽号超出，去下一个页面。 */
        if (slotNum >= m_recPerPage) {
			assert(0 != pageNum);
			/* 对于上述情况1,3的处理 */
            pageNum++;
			if (pageHdl) {
				/* 对于3的处理 */
				session->unpinPage(&pageHdl);
				assert(!pageHdl);
			}
			scanHandle->setPage(NULL);
            slotNum = 0;
        }
        /* 从此开始新搜索位置 */

		if (pageNum > m_maxUsedPageNum) { // 页面号过大，意味已经扫描完所有记录页面
            return false;
		}

		if (pageHdl) {
			LOCK_PAGE_HANDLE(session, pageHdl, Shared);
			page = (FLRHeapRecordPageInfo *)pageHdl->getPage();
		} else {
			pageHdl = GET_PAGE(session, m_heapFile, PAGE_HEAP, pageNum, Shared, m_dboStats, NULL);
			page = (FLRHeapRecordPageInfo *)pageHdl->getPage();
			verify_ex(vs.hp, verifyPage(page));
			scanHandle->setPage(pageHdl);
		}

		/* 找一条非空记录槽 */
		while ((slotNum < m_recPerPage) && (slot = getSlot(page, slotNum))->m_free)
			slotNum++;

		/* 找到一条记录没有？ */
		if (slotNum >= m_recPerPage) {
			/* 如果到达页尾，进行下一页扫描 */
			session->unlockPage(&pageHdl);
			/* 如果这里的pageHdl是本次循环中getPage得到的，那么不为NULL
			   如果这里的pageHdl是LOCK_PAGE(用参数BufferPageHandle），也不为NULL
		     */
			continue;
		}
		/* 找到一个非空的记录槽 */
		break; // 此时我们拥有页面锁
	}

	rowId = RID(pageNum, slotNum);
    /* 尝试取得行锁 */
	RowLockHandle *rowLockHdl = NULL;
	if (scanHandle->getLockMode() != None) {
		rowLockHdl = TRY_LOCK_ROW(session, m_tableDef->m_id, rowId, scanHandle->getLockMode());
		if (!rowLockHdl) { // trylock不成功
			/* 释放页面，然后先获取行锁，然后获取页面，读取页面记录，释放页面返回
			 * 加锁顺序为行锁->页面锁时没有逆向，所以不会死锁
			 */
			session->unlockPage(&pageHdl);
			SYNCHERE(SP_HEAP_FLR_AFTER_TRYLOCK_UNLOCKPAGE);
			/* 取得行锁，不超时 */
			rowLockHdl = LOCK_ROW(session, m_tableDef->m_id, rowId, scanHandle->getLockMode());
			LOCK_PAGE_HANDLE(session, pageHdl, Shared);
			/* 考虑这里有页面被修改的可能，只要slot不空即可 */
			if (slot->m_free) {
				session->unlockPage(&pageHdl);
				session->unlockRow(&rowLockHdl);
				goto beginScan_scan_start;
			}
		}
	}
    /* 行锁和页面锁都已经取得，读取SubRecord，释放页面返回 */
    Record record(rowId, REC_FIXLEN, slot->u.m_data, m_slotLength - SLOT_FLAG_LENGTH);
	scanHandle->getExtractor()->extract(&record, subRec);
	/* 设置RID的行锁句柄 */
	if (scanHandle->getLockMode() != None)
		scanHandle->setRowLockHandle(rowLockHdl);

	session->unlockPage(&pageHdl);
	scanHandle->setNextPos(RID(pageNum, (slotNum + 1)));

	/* 更新统计信息 */
	m_dboStats->countIt(DBOBJ_SCAN_ITEM);
	++m_status.m_rowsReadSubRec;

	return true;
}

/**
 * @see DrsHeap::updateCurrent
 */
void FixedLengthRecordHeap::updateCurrent(DrsHeapScanHandle *scanHandle, const SubRecord *subRecord) {
	PROFILE(PI_FixedLengthRecordHeap_updateCurrent);

	assert(scanHandle && subRecord && subRecord->m_data);
#ifndef NTSE_UNIT_TEST
	assert(scanHandle->getSession()->isRowLocked(m_tableDef->m_id, scanHandle->getRowLockHandle()->getRid(), Exclusived));
#endif
	ftrace(ts.hp, tout << scanHandle->getSession() << rid(scanHandle->getRowLockHandle()->getRid()) << subRecord);

	update(scanHandle->getSession(), scanHandle->getRowLockHandle()->getRid(), subRecord);
}

/**
* @see DrsHeap::updateCurrent
*/
void FixedLengthRecordHeap::updateCurrent(DrsHeapScanHandle *scanHandle, const Record *rcdDirectCopy) {
	PROFILE(PI_FixedLengthRecordHeap_updateCurrent);

	assert(scanHandle && rcdDirectCopy && rcdDirectCopy->m_data);
#ifndef NTSE_UNIT_TEST
	assert(scanHandle->getSession()->isRowLocked(m_tableDef->m_id, scanHandle->getRowLockHandle()->getRid(), Exclusived));
#endif
	ftrace(ts.hp, tout << scanHandle->getSession() << rid(scanHandle->getRowLockHandle()->getRid()) << rcdDirectCopy);

	update(scanHandle->getSession(), scanHandle->getRowLockHandle()->getRid(), rcdDirectCopy);
}

/**
 * @see DrsHeap::deleteCurrent
 */
void FixedLengthRecordHeap::deleteCurrent(DrsHeapScanHandle *scanHandle) {
	PROFILE(PI_FixedLengthRecordHeap_deleteCurrent);
	assert(scanHandle);
#ifndef NTSE_UNIT_TEST
	assert(scanHandle->getSession()->isRowLocked(m_tableDef->m_id, scanHandle->getRowLockHandle()->getRid(), Exclusived));
#endif
	ftrace(ts.hp, tout << scanHandle->getSession() << rid(scanHandle->getRowLockHandle()->getRid()));
	del(scanHandle->getSession(), scanHandle->getRowLockHandle()->getRid());
}

/**
 * @see DrsHeap::endScan
 */
void FixedLengthRecordHeap::endScan(DrsHeapScanHandle *scanHandle) {
	PROFILE(PI_FixedLengthRecordHeap_endScan);

	assert(scanHandle);
	ftrace(ts.hp, tout << scanHandle->getSession() << rid(scanHandle->getNextPos()));
	delete scanHandle;
}

/**
 * @see DrsHeap::storePosAndInfo
 */
void FixedLengthRecordHeap::storePosAndInfo(DrsHeapScanHandle *scanHandle) {
	scanHandle->m_prevNextPos = scanHandle->m_nextPos;
}


/**
 * @see DrsHeap::restorePosAndInfo
 */
void FixedLengthRecordHeap::restorePosAndInfo(DrsHeapScanHandle *scanHandle) {
	scanHandle->m_nextPos = scanHandle->m_prevNextPos;
}


/**
 * @see DrsHeap::redoInsert
 *
 */
RowId FixedLengthRecordHeap::redoInsert(Session *session, u64 lsn, const byte *log, uint size, Record *record) {
	assert(session && log && size > 0 && record);
	assert(record->m_format == REC_FIXLEN);
	bool needRedo = false;
	bool headerPageModified;
	u64 rowId, pageNum, newListHeader = 0;
	BufferPageHandle *pageHdl;
	FLRHeapRecordPageInfo *page;
	BufferPageHandle *headerPageHdl;
#ifdef NTSE_VERIFY_EX
	u64 oldLSN = 0, hdOldLSN = 0;
#endif

	/* 读取数据 */
#ifndef NTSE_VERIFY_EX
	parseInsertLog(log, size, &rowId, record, &headerPageModified, &newListHeader);
#else
	parseInsertLog(log, size, &rowId, record, &headerPageModified, &newListHeader, &oldLSN, &hdOldLSN);
#endif
	/* 完成构造record */
	assert(NULL != record);
	record->m_rowId = rowId;
	record->m_format = REC_FIXLEN;

	pageNum = RID_GET_PAGE(rowId);
	if (pageNum > m_maxPageNum) {
		needRedo = true;
		headerPageHdl = lockHeaderPage(session, Exclusived);
		do {
			extendHeapFile(session, (HeapHeaderPageInfo *)headerPageHdl->getPage());
		} while (pageNum > m_maxPageNum);
		session->markDirty(headerPageHdl);
		unlockHeaderPage(session, &headerPageHdl);

		pageHdl = GET_PAGE(session, m_heapFile, PAGE_HEAP, pageNum, Exclusived, m_dboStats, NULL);
		page = (FLRHeapRecordPageInfo *)pageHdl->getPage();
	} else {
		pageHdl = GET_PAGE(session, m_heapFile, PAGE_HEAP, pageNum, Exclusived, m_dboStats, NULL);
		page = (FLRHeapRecordPageInfo *)pageHdl->getPage();
		if (page->m_bph.m_lsn < lsn)
			needRedo = true;
	}

	if (needRedo) {
#ifdef NTSE_VERIFY_EX
		assert(page->m_bph.m_lsn == oldLSN);
#endif
		s16 slotNum = RID_GET_SLOT(rowId);
		nftrace(ts.recv, tout << "Write into slot: " << slotNum);
		assert(getSlot(page, slotNum)->m_free); // 必定是空槽，不可能有其他
		/* 先处理空闲槽链表 */
		page->m_firstFreeSlot = getSlot(page, slotNum)->u.m_nextFreeSlot;
		writeRecord(page, slotNum, record);
		page->m_recordNum++;
		page->m_bph.m_lsn = lsn;
		if (headerPageModified) {
			assert(page->m_inFreePageList);
			page->m_nextFreePageNum = 0; // 插入时如果改动了首页，那必然是因为记录页被从链表中删除
			page->m_inFreePageList = false;
		}
		session->markDirty(pageHdl);
	}

	session->releasePage(&pageHdl);


	if (pageNum > m_maxUsedPageNum) {
		headerPageHdl = lockHeaderPage(session, Exclusived);
		assert(m_maxUsedPageNum == ((FLRHeapHeaderPageInfo *)headerPageHdl->getPage())->m_hhpi.m_maxUsed);
		m_maxUsedPageNum = pageNum;
		((FLRHeapHeaderPageInfo *)headerPageHdl->getPage())->m_hhpi.m_maxUsed = pageNum;
		session->markDirty(headerPageHdl);
		unlockHeaderPage(session, &headerPageHdl);
		assert(m_maxUsedPageNum <= m_maxPageNum);
	}

	if (headerPageModified) { // insert操作改变了首页
		headerPageHdl = lockHeaderPage(session, Exclusived);
		if (headerPageHdl->getPage()->m_lsn < lsn) { // 需要重写headerPage.
#ifdef NTSE_VERIFY_EX
			assert(headerPageHdl->getPage()->m_lsn == hdOldLSN);
#endif
			((FLRHeapHeaderPageInfo *)headerPageHdl->getPage())->m_firstFreePageNum = newListHeader;
			headerPageHdl->getPage()->m_lsn = lsn;
			session->markDirty(headerPageHdl); /* 必须首先markDirty首页，这样才不会丢失信息。 */
		}
		unlockHeaderPage(session, &headerPageHdl);
	}
	ftrace(ts.recv, tout << session << lsn << log << size << record << headerPageModified);
	return rowId;
}


/**
 * @see DrsHeap::redoUpdate
 */
void FixedLengthRecordHeap::redoUpdate(Session *session, u64 lsn, const byte *log, uint size, const SubRecord *update) {
	ftrace(ts.recv, tout << session << lsn << log << size << update);
	assert(session && log && update);
	assert(update->m_format == REC_REDUNDANT);
	u64 rowId, pageNum;
	s16 slotNum;
	BufferPageHandle *pageHdl;
	FLRHeapRecordPageInfo *page;

#ifndef NTSE_VERIFY_EX
	parseUpdateLog(log, size, &rowId);
#else
	u64 oldLSN;
	parseUpdateLog(log, size, &rowId, &oldLSN);
#endif
	nftrace(ts.recv, tout << "rid: " << rowId);

	pageNum = RID_GET_PAGE(rowId);
	slotNum = RID_GET_SLOT(rowId);

	pageHdl = GET_PAGE(session, m_heapFile, PAGE_HEAP, pageNum, Exclusived, m_dboStats, NULL);
	page = (FLRHeapRecordPageInfo *)pageHdl->getPage();

	if (page->m_bph.m_lsn >= lsn) {
		session->releasePage(&pageHdl);
		return; // 无需redo
	}
#ifdef NTSE_VERIFY_EX
	assert(page->m_bph.m_lsn == oldLSN);
#endif
	nftrace(ts.recv, tout << "Update slot: " + slotNum);
	writeSubRecord(page, slotNum, update);

	page->m_bph.m_lsn = lsn;
	session->markDirty(pageHdl);
	session->releasePage(&pageHdl);

}

/**
 * @see DrsHeap::redoDelete
 */
void FixedLengthRecordHeap::redoDelete(Session *session, u64 lsn, const byte *log, uint size) {
	assert(session && log && size > 0);
	bool headerPageModified;
	s16 slotNum;
	u64 rowId, pageNum, oldListHeader = 0;
	BufferPageHandle *pageHdl;
	FLRHeapRecordPageInfo *page = NULL;
	BufferPageHandle *headerPageHdl = NULL;
	FLRHeapHeaderPageInfo *headerPage = NULL;
#ifdef NTSE_VERIFY_EX
	u64 oldLSN = 0, hdOldLSN = 0;
#endif

#ifndef NTSE_VERIFY_EX
	parseDeleteLog(log, size, &rowId, &headerPageModified, &oldListHeader);
#else
	parseDeleteLog(log, size, &rowId, &headerPageModified, &oldListHeader, &oldLSN, &hdOldLSN);
#endif

	pageNum = RID_GET_PAGE(rowId);
	slotNum = RID_GET_SLOT(rowId);

	pageHdl = GET_PAGE(session, m_heapFile, PAGE_HEAP, pageNum, Exclusived, m_dboStats, NULL);
	page = (FLRHeapRecordPageInfo *)pageHdl->getPage();

	if (page->m_bph.m_lsn < lsn) { // 需要redo
#ifdef NTSE_VERIFY_EX
		assert(page->m_bph.m_lsn == oldLSN);
#endif
		assert(!getSlot(page, slotNum)->m_free); // 记录槽一定非空
		deleteRecord(page, slotNum);
		page->m_bph.m_lsn = lsn;
		if (headerPageModified) {
			assert(!page->m_inFreePageList);
			page->m_nextFreePageNum = oldListHeader;
			page->m_inFreePageList = true;
		}
		session->markDirty(pageHdl);
	}
	session->releasePage(&pageHdl);

	if (headerPageModified) { // 首页被改变
		headerPageHdl = lockHeaderPage(session, Exclusived);
		headerPage = (FLRHeapHeaderPageInfo *)headerPageHdl->getPage();
		if (headerPage->m_hhpi.m_bph.m_lsn < lsn) { // 须要redo
#ifdef NTSE_VERIFY_EX
			assert(headerPage->m_hhpi.m_bph.m_lsn == hdOldLSN);
#endif
			assert(headerPage->m_firstFreePageNum != pageNum);
			headerPage->m_firstFreePageNum = pageNum;
			headerPage->m_hhpi.m_bph.m_lsn = lsn;
			session->markDirty(headerPageHdl);
		}
		unlockHeaderPage(session, &headerPageHdl);
	}
	ftrace(ts.recv, tout << session << lsn << log << size << rowId << headerPageModified);
}

/**
 * @see DrsHeap::isPageEmpty
 */
bool FixedLengthRecordHeap::isPageEmpty(Session *session, u64 pageNum) {
	assert(session);
	if (pageNum == 0) return false; // 首页被认为是不空的

	bool empty;
	BufferPageHandle * pageHdl = GET_PAGE(session, m_heapFile, PAGE_HEAP, pageNum, Shared, m_dboStats, NULL);
	FLRHeapRecordPageInfo *page = (FLRHeapRecordPageInfo *)pageHdl->getPage();
	empty = (page->m_recordNum == 0);
	session->releasePage(&pageHdl);
	return empty;
}

/**
 * 验证连续的空闲槽的个数和m_recordNum一致 
 * @param page  记录页面
 */
bool FixedLengthRecordHeap::verifyPage(ntse::FLRHeapRecordPageInfo *page) {
	FLRSlotInfo *slot;
	s16 slotNum = page->m_firstFreeSlot;
	assert(slotNum >= -1 && slotNum < (s16)m_recPerPage);
	for (u16 i = 0; i < m_recPerPage - page->m_recordNum; ++i) {
		slot = this->getSlot(page, slotNum);
		assert(slot->m_free);
		slotNum = slot->u.m_nextFreeSlot;
	}
	assert(slotNum == -1);
	return true;
}




/**
 * 选择指定数目的页面，填入输出数组。
 * @param outPages OUT    选择的页面数
 * @param wantNum         希望选择的页面数
 * @param min             最小页面
 * @param regionSize      划分的选区大小
 */
void FixedLengthRecordHeap::selectPage(u64 *outPages, int wantNum, u64 min, u64 regionSize) {
	assert(regionSize >= (u64)wantNum);
	set<u64> pnset;
	while (pnset.size() != (uint)wantNum) {
		u64 pn = min + (System::random() % regionSize);
		if (pnset.count(pn))
			continue;
		else {
			pnset.insert(pn);
		}
	}
	int idx = 0;
	for (set<u64>::iterator it = pnset.begin(); it != pnset.end(); ++it) {
                assert(*it >= min && *it < min + regionSize);
		outPages[idx++] = *it;
	}
	assert(idx == wantNum);
	pnset.clear();
        for (int i = 0; i < wantNum; ++i) {
            for (int j = 0; j < i; ++j) {
                assert(outPages[i] != outPages[j]);
            }
        }
}

/**
 * 页面是否可以采样
 * @param pageNum     页面号
 * @return            可采样返回true
 */
bool FixedLengthRecordHeap::isSamplable(u64 pageNum) {
	return (pageNum > 0 && pageNum <= m_maxUsedPageNum);
}

/**
 * 对一个内存中的定长表页进行采样
 * @param session         会话
 * @param page            目标采样页
 * @return                返回一个样本，内存从session的MemoryContext中分配
 */
Sample * FixedLengthRecordHeap::sampleBufferPage(Session *session, BufferPageHdr *page) {
	FLRHeapRecordPageInfo *fpage = (FLRHeapRecordPageInfo *)page;
	Sample *sample = Sample::create(session, FSF_MAX);//new Sample(FSF_MAX);
	(*sample)[FSF_NUMRECS] = fpage->m_recordNum;
	return sample;
}


/**
 * 更新扩展信息
 * @param session          会话
 * @param maxSamplePages   预期采样页
 */
void FixedLengthRecordHeap::updateExtendStatus(Session *session, uint maxSamplePages) {
	if (m_maxUsedPageNum < metaDataPgCnt()) {
		m_statusEx.m_numLinks = m_statusEx.m_numRows = 0;
		m_statusEx.m_numCmprsRows = 0;
		m_statusEx.m_pctUsed = .0;
		m_statusEx.m_cmprsRatio = 1.0;
		return;
	}

	McSavepoint mcSave(session->getMemoryContext());
	
	SampleResult *result;
	if (maxSamplePages > 2048)
		result = SampleAnalyse::sampleAnalyse(session, this, maxSamplePages, 30, true, 0.382, 16); // 样本较大时，相似度要求可以高一些
	else
		result = SampleAnalyse::sampleAnalyse(session, this, maxSamplePages, 50, true, 0.618, 8);  // 样本较小时，相似度要求放松一些

	m_statusEx.m_numLinks = 0;
	m_statusEx.m_numRows = (u64)(result->m_fieldCalc[FSF_NUMRECS].m_average * (m_maxUsedPageNum - metaDataPgCnt() + 1));
	m_statusEx.m_pctUsed = result->m_fieldCalc[FSF_NUMRECS].m_average / (double)m_recPerPage;
	m_statusEx.m_numCmprsRows = 0;
	m_statusEx.m_cmprsRatio = 1.0;

	delete result;
}


#ifdef NTSE_VERIFY_EX
/**
* @see DrsHeap::redoFinish
*/
void FixedLengthRecordHeap::redoFinish(Session *session) {
#ifdef NTSE_VERIFY_EX
	verifyFreePageList(session);
#endif
	DrsHeap::redoFinish(session);
}

/**
 * 验证空闲页面链表
 * @param session  会话
 */
void FixedLengthRecordHeap::verifyFreePageList(Session *session) {
	/* 验证页面空闲链表 */
	BufferPageHandle *headerPageHdl = lockHeaderPage(session, Exclusived);
	FLRHeapHeaderPageInfo *headerPage = (FLRHeapHeaderPageInfo *)headerPageHdl->getPage();
	u64 nextfree = headerPage->m_firstFreePageNum;
	unlockHeaderPage(session, &headerPageHdl);
	while (nextfree) {
		BufferPageHandle *pageHdl = GET_PAGE(session, m_heapFile, PAGE_HEAP, nextfree, Exclusived, m_dboStats, NULL);
		FLRHeapRecordPageInfo *page = (FLRHeapRecordPageInfo *)pageHdl->getPage();
		assert(page->m_inFreePageList);
		assert(page->m_recordNum < m_recPerPage && page->m_firstFreeSlot != -1);
		nextfree = page->m_nextFreePageNum;
		session->releasePage(&pageHdl);
	}
}
#endif
