/**
 * 变长记录堆
 *
 * @author 谢可(ken@163.org)
 */

#include "heap/VariableLengthRecordHeap.h"
#include "misc/Session.h"
#include "api/Database.h"
#include "misc/Buffer.h"
#include "util/File.h"
#include "util/Sync.h"
#include "util/Stream.h"
#include "misc/TableDef.h"
#include "misc/Trace.h"
#include <set>
#include "misc/Profile.h"

#include "misc/RecordHelper.h"

#ifdef TNT_ENGINE
#include "trx/TNTTransaction.h"
#endif

#ifdef NTSE_UNIT_TEST
#include "util/Thread.h"
#endif

using namespace ntse;

namespace ntse {

/* 变长堆样本域 */
enum VLRHEAP_SAMPLE_FIELDS {
	VSF_NUMRECS,
	VSF_NUMLINKS,
	VSF_FREEBYTES,
	VSF_NUM_CPRSRECS,
	VSF_UNCPRSSIZE,
	VSF_CPRS_SIZE,

	VSF_MAX,
};

/** 变长堆扫描实现需要的特别数据放在这里 */
struct ScanHandleAddInfo {
	bool m_returnLinkSrc;           /** 是否安装源扫描 */
	u64 m_nextBmpNum;               /** 下一个位图页面号 */
};

/**
 * 构造函数
 *
 * @param db                   数据库指针
 * @param session              会话
 * @param heapFile             对应的堆文件指针
 * @param headerPage           堆头页
 * @param dbObjStats           数据对象状态
 * @throw NtseException        解析表定义失败
 */
VariableLengthRecordHeap::VariableLengthRecordHeap(Database *db, Session *session, const TableDef *tableDef, File *heapFile,
												   BufferPageHdr *headerPage, DBObjStats *dbObjStats) throw(NtseException):
	DrsHeap(db, tableDef, heapFile, headerPage, dbObjStats), m_posLock("VLRHeap::posLock", __FILE__, __LINE__) {
	assert(db && session && heapFile && headerPage);
	ftrace(ts.hp, tout << session << heapFile << (HeapHeaderPageInfo *)headerPage;);
	m_version = HEAP_VERSION_VLR;

	m_dboStats = dbObjStats;

	/* 变长记录堆专有数据 */
	VLRHeapHeaderPageInfo *hdPage = (VLRHeapHeaderPageInfo *)headerPage;
	m_centralBmpNum = hdPage->m_centralBmpNum;
	assert(m_centralBmpNum > 0);
	/* 计算地方位图数目和最后位图页号 */
	if (m_maxPageNum > m_centralBmpNum) {
		m_bitmapNum = (m_maxPageNum - m_centralBmpNum - 1) / (BitmapPage::CAPACITY + 1) + 1;
		m_lastBmpNum = m_maxPageNum - (m_maxPageNum - m_centralBmpNum -1) % (BitmapPage::CAPACITY + 1);
	} else {
		m_bitmapNum = 0;
		m_lastBmpNum = 0; // 还不存在，不过不要紧
	}
	
	m_pctFree = m_tableDef->m_pctFree;
	m_reserveSize = (uint)(m_pctFree * (Limits::PAGE_SIZE - sizeof(VLRHeapRecordPageInfo)) / 100); //计算保留空间大小

	m_centralBitmapHdr = (BufferPageHdr **)(new byte[sizeof(BufferPageHdr *) * m_centralBmpNum]);
	/* 读取中央位图页面句柄，pin在内存中 */
	for (int i = 0; i < (int)m_centralBmpNum; ++i) {
		m_centralBitmapHdr[i] = m_buffer->getPage(session, m_heapFile, PAGE_HEAP, i + 1, Exclusived, m_dboStats, NULL);
		m_buffer->unlockPage(session->getId(), m_centralBitmapHdr[i], Exclusived);
		assert(m_centralBitmapHdr[i]->m_checksum == BufferPageHdr::CHECKSUM_DISABLED);
	}
	m_cbm.m_pageHdrArray = m_centralBitmapHdr;
	m_cbm.m_vlrHeap = this;

	for (int i = 0; i < 4; ++i) {
		m_position[i].m_bitmapIdx = 0;
		m_position[i].m_pageIdx = 0;
	}

	/* 打开即将cleanClose置为false，和定长记录堆不同，open并非完全只读操作 */
	if (!hdPage->m_cleanClosed)
		redoCentralBitmap(session);
	hdPage->m_cleanClosed = false;
	m_buffer->markDirty(session, m_headerPage);
	m_buffer->writePage(session, m_headerPage);
}

/** @see DrsHeap::close */
void VariableLengthRecordHeap::close(Session *session, bool flushDirty) {
	assert(session);
	ftrace(ts.hp, tout << session << flushDirty << this);
	if (!m_heapFile) return;
	if (!flushDirty) {
		for (int i = 0; i < (int)m_centralBmpNum; ++i) {
			m_buffer->unpinPage(m_centralBitmapHdr[i]);
		}
		m_buffer->unpinPage(m_headerPage);
		m_buffer->freePages(session, m_heapFile, false);
	} else {
		// 将所有脏数据刷出去
		for (int i = 0; i < (int)m_centralBmpNum; ++i) {
			m_buffer->lockPage(session->getId(), m_centralBitmapHdr[i], Exclusived, true);
			m_buffer->markDirty(session, m_centralBitmapHdr[i]);
			m_buffer->releasePage(session, m_centralBitmapHdr[i], Exclusived);
		}
		if (((HeapHeaderPageInfo *)m_headerPage)->m_maxUsed != m_maxUsedPageNum) {
			m_buffer->lockPage(session->getId(), m_headerPage, Exclusived, false);
			((HeapHeaderPageInfo *)m_headerPage)->m_maxUsed = m_maxUsedPageNum;
			m_buffer->markDirty(session, m_headerPage);
			m_buffer->unlockPage(session->getId(), m_headerPage, Exclusived);
		}
		m_buffer->unpinPage(m_headerPage);
		m_buffer->freePages(session, m_heapFile, true);
		// 最后设置安全关闭标志
		m_headerPage = m_db->getPageBuffer()->getPage(session, m_heapFile, PAGE_HEAP, 0, Exclusived, m_dboStats);
		((VLRHeapHeaderPageInfo *)m_headerPage)->m_cleanClosed = true;
		m_buffer->markDirty(session, m_headerPage);
		m_buffer->releasePage(session, m_headerPage, Exclusived);
		m_buffer->freePages(session, m_heapFile, true);
	}

	delete [] m_centralBitmapHdr;
	u64 errCode;
	errCode = m_heapFile->close();
	if (File::E_NO_ERROR != File::getNtseError(errCode)) {
		m_db->getSyslog()->fopPanic(errCode, "Closing heap file %s failed.", m_heapFile->getPath());
	}
	delete m_heapFile;
	delete m_dboStats;
	m_dboStats = 0;
	m_heapFile = NULL;
}

#ifdef NTSE_UNIT_TEST
/**
 * 同步元数据页
 * @param session    会话
 */
void VariableLengthRecordHeap::syncMataPages(Session *session) {
	if (!m_heapFile) return;
	for (int i = 0; i < (int)m_centralBmpNum; ++i) {
		m_buffer->lockPage(session->getId(), m_centralBitmapHdr[i], Exclusived, true);
		m_buffer->markDirty(session, m_centralBitmapHdr[i]);
		m_buffer->unlockPage(session->getId(), m_centralBitmapHdr[i], Exclusived);
	}
	DrsHeap::syncMataPages(session);
}

/**
 * 获取中央位图的Buffer页
 * @param idx   中央位图号
 * @return      中央位图Buffer页
 */
BufferPageHdr * VariableLengthRecordHeap::getCBMPage(int idx) {
	return m_centralBitmapHdr[idx];
}

#endif

/**
 * 初始化变长记录堆
 * @param headerPage             堆首页
 * @param tableDef               表定义
 * @param additionalPages OUT    中央位图的页面
 * @param additionalPageNum OUT  中央位图的页面数
 * @throw NtseException          表定义有错、写出界等
 */
void VariableLengthRecordHeap::initHeader(BufferPageHdr *headerPage, BufferPageHdr **additionalPages, uint *additionalPageNum) throw(NtseException) {
	assert(headerPage && additionalPages && additionalPageNum);
	assert(NULL == *additionalPages && 0 == *additionalPageNum);
	VLRHeapHeaderPageInfo *headerInfo = (VLRHeapHeaderPageInfo *)headerPage;
	headerInfo->m_hhpi.m_bph.m_lsn = 0;
	headerInfo->m_hhpi.m_bph.m_checksum = BufferPageHdr::CHECKSUM_NO;
	headerInfo->m_hhpi.m_version = HEAP_VERSION_VLR;
	headerInfo->m_centralBmpNum = DEFAULT_CBITMAP_SIZE;
	headerInfo->m_hhpi.m_pageNum = 0 + headerInfo->m_centralBmpNum; // 有几个中央位图页也要算上
	headerInfo->m_hhpi.m_maxUsed = headerInfo->m_hhpi.m_pageNum;
	headerInfo->m_cleanClosed = true; // 初次创建

	*additionalPages = (BufferPageHdr *)System::virtualAlloc(Limits::PAGE_SIZE * headerInfo->m_centralBmpNum);
	*additionalPageNum = headerInfo->m_centralBmpNum;
	memset(*additionalPages, 0x8, Limits::PAGE_SIZE * headerInfo->m_centralBmpNum); // 位图页全部置00001000，表示一个bitmap页面辖下有满足11的空间。
	// 将中央位图页的checksum设置为CHECKSUM_DISABLED
	for (int i = 0; i < headerInfo->m_centralBmpNum; ++i) {
		((BufferPageHdr *)((byte *)(*additionalPages) + Limits::PAGE_SIZE * i))->m_checksum = BufferPageHdr::CHECKSUM_DISABLED;
		((BufferPageHdr *)((byte *)(*additionalPages) + Limits::PAGE_SIZE * i))->m_lsn = 0;
	}
}

/**
 * @see DrsHeap::initExtendedNewPages
 */
void VariableLengthRecordHeap::initExtendedNewPages(Session *session, uint size) {
	for (uint i = 0; i < size; ++i) {
		u64 pageNum = m_maxPageNum + i + 1; // maxPageNum还没有改变
		BufferPageHandle *pageHdl = NEW_PAGE(session, m_heapFile, PAGE_HEAP, pageNum, Exclusived, m_dboStats);
		memset(pageHdl->getPage(), 0, Limits::PAGE_SIZE);
		if (pageIsBitmap(pageNum)) {
			BitmapPage *bmp = (BitmapPage *)pageHdl->getPage();
			bmp->init();
		} else {
			VLRHeapRecordPageInfo *recPage = (VLRHeapRecordPageInfo *)pageHdl->getPage();
			recPage->m_bph.m_lsn = 0;
			recPage->m_bph.m_checksum = BufferPageHdr::CHECKSUM_NO;
			recPage->m_recordNum = 0;
			recPage->m_freeSpaceSize = Limits::PAGE_SIZE - sizeof(VLRHeapRecordPageInfo);
			recPage->m_lastHeaderPos = 0;  // 0表示没有最后一个记录头
			recPage->m_freeSpaceTailPos = Limits::PAGE_SIZE;
			recPage->m_pageBitmapFlag = 3; // 二进制11
		}
		session->markDirty(pageHdl);
		session->releasePage(&pageHdl);
	}
	m_buffer->batchWrite(session, m_heapFile, PAGE_HEAP, m_maxPageNum + 1, m_maxPageNum + size);
}

/**
 * 扩展堆的收尾工作，修改中央位图
 *
 * @param extendSize  堆扩展的大小
 */
void VariableLengthRecordHeap::afterExtendHeap(uint extendSize) {
	assert(extendSize > 0);
	/* 更新中央位图信息 */
	if (!pageIsBitmap(m_maxPageNum - extendSize + 1)) {
		// 扩容的第一页不是Bitmap，说明前一个Bitmap增加了11类型的页面
		m_cbm.setBitmap((int)(m_bitmapNum - 1), m_cbm[(int)(m_bitmapNum - 1)] | 0x8);
	}
	if (pageIsBitmap(m_maxPageNum)) {
		// 扩容的最后一个页是bitmap，它辖下不含任何页面。修改使之不会被检索到。
		m_cbm.setBitmap((int)m_bitmapNum, 0);
	}
	m_bitmapNum = (m_maxPageNum - m_centralBmpNum - 1) / (BitmapPage::CAPACITY + 1) + 1;
	m_lastBmpNum = m_maxPageNum - (m_maxPageNum - m_centralBmpNum -1) % (BitmapPage::CAPACITY + 1);
}


/**
 * @see DrsHeap::setPctFree
 */
void VariableLengthRecordHeap::setPctFree(Session *session, u8 pctFree) throw(NtseException) {
	assert(session);
	assert(pctFree < FLAG_01_LIMIT * 100);
	ftrace(ts.hp, tout << session << this << pctFree);
	m_pctFree = pctFree;
	m_reserveSize = (uint)(m_pctFree * (Limits::PAGE_SIZE - sizeof(VLRHeapRecordPageInfo)) / 100);
}



/**
 * 寻找一个含有空闲空间的页面
 * @param session       会话
 * @param spaceRequest  请求的空间大小，最后判断页面是否适用时，是按大小来进行。
 * @param pageNum OUT   存放页面号
 * @param lockMode      锁模式，默认是排他锁。
 * @param lastFalse     上一次请求lockMode为None请求失败的页面号，这样避免反复将无用的m_position值返回给调用者
 * @return              找到的空闲页面，如果不锁，返回NULL
 * @post                找到新的可用页面后会更新m_position数据，但是如果lockMode为None，则不更新（因为无法确认确实是可用页面）
 */
BufferPageHandle* VariableLengthRecordHeap::findFreePage(Session *session, u16 spaceRequest, u64 *pageNum, LockMode lockMode, u64 lastFalse) {
	assert(session && pageNum);
	ftrace(ts.hp, tout << session << spaceRequest << lockMode << lastFalse);
	int lastBmpIdx, bmpIdx;
	int lastPageIdx, pageIdx;
	BufferPageHandle *freePageHdl, *bmpHdl, *headerPageHdl;
	VLRHeapRecordPageInfo *freePage;
	u16 spaceWithReverse = (u16)(spaceRequest + m_reserveSize);
	if (spaceWithReverse > FLAG10_SPACE_LIMIT)
		spaceWithReverse = FLAG10_SPACE_LIMIT - 1;
	u8 spaceFlag = spaceResquestToFlag(spaceWithReverse);
	assert(spaceRequest <= spaceWithReverse);
	u8 resultFlag;
	u64 maxPageNum;
	bool newPos = false;

	RWLOCK(&m_posLock, Shared);
	lastBmpIdx = m_position[spaceFlag].m_bitmapIdx;
	lastPageIdx = m_position[spaceFlag].m_pageIdx;
	RWUNLOCK(&m_posLock, Shared);

	*pageNum = getPageNumByIdx(lastBmpIdx, lastPageIdx);
	bmpIdx = lastBmpIdx;
	pageIdx = lastPageIdx;
	if (lastFalse == *pageNum) goto findFreePage_search_bitmap;
	if (*pageNum > m_maxPageNum) goto findFreePage_search_central_bmp;
findFreePage_check_page_usability:
	if (lockMode == None) {
		if (newPos) {
			if (*pageNum > m_maxUsedPageNum) {
				RWLOCK(&m_posLock, Exclusived);
				if (*pageNum > m_maxUsedPageNum)
					m_maxUsedPageNum = *pageNum;
				RWUNLOCK(&m_posLock, Exclusived);
			}
		}
		nftrace(ts.hp, tout << *pageNum);
		return NULL;
	}
	freePageHdl = GET_PAGE(session, m_heapFile, PAGE_HEAP, *pageNum, lockMode, m_dboStats, NULL);
	freePage = (VLRHeapRecordPageInfo *)freePageHdl->getPage();
	verify_ex(vs.hp, freePage->verifyPage());
	if (freePage->m_freeSpaceSize <= spaceWithReverse) { // 有空间保留检查
		session->releasePage(&freePageHdl);
		freePage = NULL;
		goto findFreePage_search_bitmap; // 从*pageNum开始检查
	}
	if (newPos) {
		if (0 < (resultFlag = getRecordPageFlag(0, freePage->m_freeSpaceSize, false))) { //至少必须是01才用修改上次指针
			// 找到一个更高空闲级别页面时，分配低空闲级别页面也用这一页面
			// 也就是我们不是用best fit
			RWLOCK(&m_posLock, Exclusived);
			for (int i = spaceFlag; i <= resultFlag; ++i) {
				m_position[i].m_bitmapIdx = bmpIdx;
				m_position[i].m_pageIdx = pageIdx;
			}
			if (*pageNum > m_maxUsedPageNum)
				m_maxUsedPageNum = *pageNum;
			RWUNLOCK(&m_posLock, Exclusived);
		}
	}
	nftrace(ts.hp, tout << *pageNum);
	return freePageHdl;
findFreePage_search_bitmap:
	bmpHdl = getBitmap(session, bmpIdx, Shared);
	maxPageNum = m_maxPageNum;
	pageIdx = ((BitmapPage *)bmpHdl->getPage())->findFreeInBMP(spaceFlag,
		(uint)(((u64)bmpIdx == (maxPageNum - m_centralBmpNum - 1) / (BitmapPage::CAPACITY + 1)) ? ((maxPageNum - m_centralBmpNum -1) % (BitmapPage::CAPACITY + 1)) : BitmapPage::CAPACITY),
		pageIdx);
	session->releasePage(&bmpHdl);
	if (-1 == pageIdx) goto findFreePage_search_central_bmp;
	*pageNum = getPageNumByIdx(bmpIdx, pageIdx);
	newPos = true;
	goto findFreePage_check_page_usability;
findFreePage_search_central_bmp:
	maxPageNum = m_maxPageNum;
	bmpIdx = m_cbm.findFirstFitBitmap(spaceFlag, bmpIdx);
	if (-1 != bmpIdx) {
		pageIdx = 0;
		goto findFreePage_search_bitmap;
	}
	SYNCHERE(SP_HEAP_VLR_FINDFREEPAGE_BEFORE_EXTEND_HEAP);
	headerPageHdl = lockHeaderPage(session, Exclusived);
	if (maxPageNum != m_maxPageNum) { // 已经有其他线程扩展了堆，无需再次扩展
		unlockHeaderPage(session, &headerPageHdl);
		bmpIdx = lastBmpIdx;
		goto findFreePage_search_central_bmp;
	}
	// 扩展堆文件
	*pageNum = maxPageNum + 1;
findFreePage_exxtend_heapfile:
	u16 extendedPages = extendHeapFile(session, (HeapHeaderPageInfo *)headerPageHdl->getPage());
	session->markDirty(headerPageHdl);
	if (pageIsBitmap(*pageNum)) {
		*pageNum += 1;
		if (extendedPages == 1) {
			goto findFreePage_exxtend_heapfile;
		}
	}
	unlockHeaderPage(session, &headerPageHdl);
	bmpIdx = bmpNum2Idx(getBmpNum(*pageNum));
	pageIdx = (int)(*pageNum - getBmpNum(*pageNum) - 1);
	newPos = true;
	goto findFreePage_check_page_usability;

}


/**
 * 锁定一个位图页面
 * @param session      资源会话
 * @param groupNum     位图页面所在组号，0开始编号，也就是在中央位图中的下标
 * @param lockMode     锁模式
 * @return             页面handle
 */
BufferPageHandle* VariableLengthRecordHeap::getBitmap(Session *session, int groupNum, LockMode lockMode) {
	assert(session && groupNum >= 0);
	return GET_PAGE(session, m_heapFile, PAGE_HEAP, (groupNum * (BitmapPage::CAPACITY + 1)) + m_centralBmpNum + 1, lockMode, m_dboStats, NULL);
}

/**
 * 锁定两个位图页面
 * @param session          会话
 * @param bmpPgN1,bmpPgN2  页面号1和2
 * @param bmpHdl1,bmpHdl2  页面句柄
 * @param lockMode         锁模式
 */
void VariableLengthRecordHeap::getTwoBitmap(Session *session, u64 bmpPgN1, u64 bmpPgN2, BufferPageHandle **bmpHdl1, BufferPageHandle **bmpHdl2, LockMode lockMode) {
	assert(bmpPgN1 != bmpPgN2);
	if (bmpPgN1 < bmpPgN2) {
		*bmpHdl1 = GET_PAGE(session, m_heapFile, PAGE_HEAP, bmpPgN1, lockMode, m_dboStats, NULL);
		*bmpHdl2 = GET_PAGE(session, m_heapFile, PAGE_HEAP, bmpPgN2, lockMode, m_dboStats, NULL);
	} else {
		*bmpHdl2 = GET_PAGE(session, m_heapFile, PAGE_HEAP, bmpPgN2, lockMode, m_dboStats, NULL);
		*bmpHdl1 = GET_PAGE(session, m_heapFile, PAGE_HEAP, bmpPgN1, lockMode, m_dboStats, NULL);
	}
}

/**
 * 获取记录
 * @param session                   会话
 * @param rowId                     记录RowId
 * @param dest OUT                  记录传出
 * @param destIsRecord              dest参数是否是Record, false表示是SubRecord
 * @param extractor                 记录提取子
 * @param lockMode                  页面锁模式
 * @param rlh                       行锁
 * @param duringRedo                是否redo过程中
 * @return                          成功读取返回true，记录不存在返回false
 */
bool VariableLengthRecordHeap::doGet(Session *session, RowId rowId, void *dest,
	bool destIsRecord, SubrecExtractor *extractor, LockMode lockMode, RowLockHandle **rlh, bool duringRedo) {
	assert(session && dest);
	bool gotTarget;
	u64 pageNum = RID_GET_PAGE(rowId);
	uint slotNum = RID_GET_SLOT(rowId);
	if ((pageNum < m_centralBmpNum + 2) || (!duringRedo && pageNum > m_maxPageNum) || pageIsBitmap(pageNum))
		return false;

	if (rlh) {
		*rlh = LOCK_ROW(session, m_tableDef->m_id, rowId, lockMode);
	}

doGet_start_get_record_page:
	BufferPageHandle *pageHdl = GET_PAGE(session, m_heapFile, PAGE_HEAP, pageNum, Shared, m_dboStats, NULL);
	verify_ex(vs.hp, ((VLRHeapRecordPageInfo *)pageHdl->getPage())->verifyPage());
	SYNCHERE(SP_HEAP_VLR_GET_AFTER_GET_PAGE);

	VLRHeapRecordPageInfo *recPage = (VLRHeapRecordPageInfo *)pageHdl->getPage();
	VLRHeader *recHdr;
	recHdr = recPage->getVLRHeader(slotNum);

	if (!recHdr || recHdr->m_ifEmpty || recHdr->m_ifTarget) {
		session->releasePage(&pageHdl);
		if (rlh) session->unlockRow(rlh);
		return false;
	}
	if (recHdr->m_ifLink) { // 链接记录
		u64 tagRid = RID_READ(recPage->getRecordData(recHdr));
		session->releasePage(&pageHdl);
		SYNCHERE(SP_HEAP_VLR_DOGET_AFTER_RELEASE_SOURCE_PAGE);
		gotTarget = doGetTarget(session, tagRid, rowId, dest, destIsRecord, extractor);
		if (!gotTarget) {
			// 重新get，因为source肯定有变化。
			goto doGet_start_get_record_page;
		}
	} else {
		if (destIsRecord) {
			recPage->readRecord(recHdr, (Record *)dest);
			((Record *)dest)->m_rowId = rowId;
			assert(((Record *)dest)->m_size <= m_tableDef->m_maxRecSize);
			/* 更新统计信息 */
			++m_status.m_rowsReadRecord;
		} else {
			recPage->readSubRecord(recHdr, extractor, (SubRecord *)dest);
			((SubRecord *)dest)->m_rowId = rowId;
			/* 更新统计信息 */
			++m_status.m_rowsReadSubRec;
		}
		session->releasePage(&pageHdl);
	}
	assert(destIsRecord || ((SubRecord *)dest)->m_rowId == rowId);
	assert(!destIsRecord || (((Record *)dest)->m_size <= m_tableDef->m_maxRecSize && ((Record *)dest)->m_rowId == rowId));

	return true;
}

// 记录操作

/**
 * 执行get操作的实体，因为重载的两个get只有读取操作不同，其他完全相同。
 * @param session          会话
 * @param rowId            目标RowId
 * @param dest OUT         目标数据填入区
 * @param destIsRecord     true表示dest是Record *，否则是一个SubRecord *
 * @param extractor        子记录提取器，只在destIsRecord为false时指定
 * @return                 成功读取返回true，有任何记录不存在等失败返回false
 */
bool VariableLengthRecordHeap::doGetTarget(Session *session, RowId rowId, RowId srcRid, void *dest,	bool destIsRecord, SubrecExtractor *extractor) {
	assert(session && dest);
	u64 pageNum = RID_GET_PAGE(rowId);
	uint slotNum = RID_GET_SLOT(rowId);
	assert(!((pageNum < m_centralBmpNum + 2) || (pageNum > m_maxPageNum) || pageIsBitmap(pageNum)));

	BufferPageHandle *pageHdl = GET_PAGE(session, m_heapFile, PAGE_HEAP, pageNum, Shared, m_dboStats, NULL);
	verify_ex(vs.hp, ((VLRHeapRecordPageInfo *)pageHdl->getPage())->verifyPage());
	SYNCHERE(SP_HEAP_VLR_GET_AFTER_GET_PAGE);

	VLRHeapRecordPageInfo *recPage = (VLRHeapRecordPageInfo *)pageHdl->getPage();
	VLRHeader *recHdr;
	recHdr = recPage->getVLRHeader(slotNum);

	if (!recHdr || recHdr->m_ifEmpty || !recHdr->m_ifTarget || RID_READ((byte *)recPage + recHdr->m_offset) != srcRid) {
		// 不是Target或者不是源的Target
		session->releasePage(&pageHdl);
		return false;
	}
	if (destIsRecord) {
		recPage->readRecord(recHdr, (Record *)dest);
		((Record *)dest)->m_rowId = srcRid;
		assert(((Record *)dest)->m_size <= m_tableDef->m_maxRecSize);
		++m_status.m_rowsReadRecord;
	} else {
		recPage->readSubRecord(recHdr, extractor, (SubRecord *)dest);
		((SubRecord *)dest)->m_rowId = srcRid;
		++m_status.m_rowsReadSubRec;
	}
	session->releasePage(&pageHdl);

	assert(destIsRecord || ((SubRecord *)dest)->m_rowId == srcRid);
	assert(!destIsRecord || (((Record *)dest)->m_size <= m_tableDef->m_maxRecSize && ((Record *)dest)->m_rowId == srcRid));
	return true;
}


/**
 * 提取一个Target
 * @param session          资源会话
 * @param rowId            目标RowId
 * @param srcRid           源RowId
 * @param outPut OUT       提取记录内容
 * @param extractor        字记录提取器
 * @return                 成功读取返回true，有任何记录不存在等失败返回false
 */
void VariableLengthRecordHeap::extractTarget(Session *session, RowId rowId, RowId srcRid, SubRecord *output, SubrecExtractor *extractor) {
	assert(session && output);
	u64 pageNum = RID_GET_PAGE(rowId);
	uint slotNum = RID_GET_SLOT(rowId);
	assert(!((pageNum < m_centralBmpNum + 2) || (pageNum > m_maxPageNum) || pageIsBitmap(pageNum)));

	BufferPageHandle *pageHdl = GET_PAGE(session, m_heapFile, PAGE_HEAP, pageNum, Shared, m_dboStats, NULL);
	verify_ex(vs.hp, ((VLRHeapRecordPageInfo *)pageHdl->getPage())->verifyPage());
	SYNCHERE(SP_HEAP_VLR_GET_AFTER_GET_PAGE);

	VLRHeapRecordPageInfo *recPage = (VLRHeapRecordPageInfo *)pageHdl->getPage();
	VLRHeader *recHdr;
	recHdr = recPage->getVLRHeader(slotNum);

	assert(!(!recHdr || recHdr->m_ifEmpty || !recHdr->m_ifTarget || RID_READ((byte *)recPage + recHdr->m_offset) != srcRid));

	Record record;
	recPage->constructRecord(recHdr, &record);
	extractor->extract(&record, output);

	output->m_rowId = srcRid;
	++m_status.m_rowsReadSubRec;

	session->releasePage(&pageHdl);
}




/**
 * @see DrsHeap::getRecord
 */
bool VariableLengthRecordHeap::getRecord(Session *session, RowId rowId, Record *record,
	LockMode lockMode, RowLockHandle **rlh, bool duringRedo) {
	PROFILE(PI_VariableLengthRecordHeap_getRecord);

	assert(session && record);
	assert((lockMode == None && rlh == NULL) || (lockMode != None && rlh != NULL));
	assert(record->m_format == REC_VARLEN || record->m_format == REC_COMPRESSED);
	return doGet(session, rowId, record, true, NULL, lockMode, rlh, duringRedo);
}

/**
 * @see DrsHeap::getSubRecord
 */
bool VariableLengthRecordHeap::getSubRecord(Session *session, RowId rowId,
	SubrecExtractor *extractor, SubRecord *subRecord, LockMode lockMode, RowLockHandle **rlh) {
	PROFILE(PI_VariableLengthRecordHeap_getSubRecord);

	assert(session && subRecord);
	assert((lockMode == None && rlh == NULL) || (lockMode != None && rlh != NULL));
	assert(subRecord->m_format == REC_REDUNDANT);
	return doGet(session, rowId, subRecord, false, extractor, lockMode, rlh, false);
}

/**
 * @see DrsHeap::insert
 */
RowId VariableLengthRecordHeap::insert(Session *session, const Record *record, RowLockHandle **rlh)  throw(NtseException){
	PROFILE(PI_VariableLengthRecordHeap_insert);

	assert(session && record && record->m_data);
	assert(record->m_format == REC_VARLEN || record->m_format == REC_COMPRESSED);
	assert(record->m_size <= m_tableDef->m_maxRecSize);
	ftrace(ts.hp, tout << session << record << rlh);
	u64 pageNum = 0;
	u64 lsn;
	BufferPageHandle *freePageHdl, *bmpHdl;
	VLRHeapRecordPageInfo *freePage;
	u8 newFlag;
	RowId rowId;
	uint slotNum;
	bool rowLocked = false;
#ifdef TNT_ENGINE
	RowId tntLockRid = INVALID_ROW_ID;
#endif

	// 上层必须保证dataSize不会超过最大值
	assert(record->m_size <= MAX_VLR_LENGTH);
	uint dataSize = ((record->m_size < LINK_SIZE) ? LINK_SIZE : record->m_size) + sizeof(VLRHeader);
insert_find_free_page:
	freePageHdl = findFreePage(session, (u16)dataSize, &pageNum); // 异常直接抛出。
	freePage = (VLRHeapRecordPageInfo *)freePageHdl->getPage();
	verify_ex(vs.hp, freePage->verifyPage());
insert_get_free_slot:
	slotNum = freePage->getEmptySlot();
	rowId = RID(pageNum, slotNum);

	// 如果需要，锁定行锁
	if (rlh && !rowLocked) {
		SYNCHERE(SP_HEAP_VLR_INSERT_BEFORE_TRY_LOCK_ROW);
		*rlh = TRY_LOCK_ROW(session, m_tableDef->m_id, rowId, Exclusived);
		if (!*rlh) { // TRY LOCK不成功
			SYNCHERE(SP_HEAP_VLR_INSERT_TRY_LOCK_ROW_FAILED);
			session->unlockPage(&freePageHdl);
			SYNCHERE(SP_HEAP_VLR_INSERT_UNLOCK_PAGE_AFTER_TRY_LOCK_ROW_FAILED);
			*rlh = LOCK_ROW(session, m_tableDef->m_id, rowId, Exclusived);
			LOCK_PAGE_HANDLE(session, freePageHdl, Exclusived);
			verify_ex(vs.hp, freePage->verifyPage());
			VLRHeader *vlrHdr = freePage->getVLRHeader(slotNum);
			if (!vlrHdr || vlrHdr->m_ifEmpty) { // 该记录槽还是空的
				// 验证空间足够
				if (!freePage->spaceIsEnough(slotNum, (u16)dataSize)) {
					// 空间已经不够了
					session->unlockRow(rlh);
					session->releasePage(&freePageHdl);
					SYNCHERE(SP_HEAP_VLR_INSERT_SLOT_FREE_BUT_NO_SPACE);
					goto insert_find_free_page;
				}
			} else { // 这个记录槽已经非空了
				// 首先释放行锁
				session->unlockRow(rlh);
				// 看页面的空间够不够
				if (freePage->m_freeSpaceSize >= dataSize ||
					((freePage->m_freeSpaceSize >= dataSize - sizeof(VLRHeader)) &&
					(freePage->m_lastHeaderPos - sizeof(VLRHeapRecordPageInfo) > (freePage->m_recordNum -1) * sizeof(VLRHeader))) ) //最后一行表明记录头区有空闲
				{
					assert(!rowLocked);
					SYNCHERE(SP_HEAP_VLR_INSERT_BEFORE_REFIND_FREE_SLOT);
					goto insert_get_free_slot;
				}
				// 页面空间不够了，直接释放
				session->releasePage(&freePageHdl);
				goto insert_find_free_page;
			}
		} else {
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
					session->releasePage(&freePageHdl);
					try {
						trx->lockRow(TL_X, rowId, m_tableDef->m_id);
					} catch (NtseException &e) {
						// 抛错时已释放所有资源
						throw e;
					}
					tntLockRid = rowId;
					//trx->unlockRow(TL_X, rowId, m_tableDef->m_id);
					rowLocked = false;
					goto insert_find_free_page;
				}
			}
		}
#endif
	}


#ifdef TNT_ENGINE   //如果是小型大对象插入，这里不需要记录InsertTNTLog日志，只需要Lob日志
	tnt::TNTTransaction *trx = session->getTrans();
	if (trx != NULL && m_tableDef->isTNTTable()) {
		assert(trx != NULL);
		if (!TableDef::tableIdIsVirtualLob(m_tableDef->m_id)) { //此处需要判断不是小型大对象的插入，如果是，不需要写TNT日志
			assert(INVALID_ROW_ID != rowId);
			writeInsertTNTLog(session, m_tableDef->m_id, trx->getTrxId(), trx->getTrxLastLsn(), rowId);
			//assert(INVALID_LSN != lsn);
		} else {//如果是小型大对象插入，则需要写一条大对象插入日志
			//小型大对象rowId就是LobId
			LobStorage::writeTNTInsertLob(session,trx->getTrxId(), TableDef::getNormalTableId(m_tableDef->m_id), trx->getTrxLastLsn(), rowId);
		}
	}
#endif

	freePage->insertIntoSlot(record, (u16)slotNum);


	newFlag = getRecordPageFlag(freePage->m_pageBitmapFlag, freePage->m_freeSpaceSize, false);
	if (newFlag != freePage->m_pageBitmapFlag) { // 需要修改bitmap
		bmpHdl = GET_PAGE(session, m_heapFile, PAGE_HEAP, getBmpNum(pageNum), Exclusived, m_dboStats, NULL);
#ifndef NTSE_VERIFY_EX
		lsn = writeInsertLog(session, rowId, record, true);
#else
		lsn = writeInsertLog(session, rowId, record, true, freePage->m_bph.m_lsn);
#endif
		updateBitmap(session, pageNum, newFlag, lsn, bmpHdl);
		freePage->m_pageBitmapFlag = newFlag;
	} else { // 不需要修改bitmap
#ifndef NTSE_VERIFY_EX
		lsn = writeInsertLog(session, rowId, record, false);
#else
		lsn = writeInsertLog(session, rowId, record, false, freePage->m_bph.m_lsn);
#endif
	}
	assert(!session->isLogging() || lsn > freePage->m_bph.m_lsn);
	freePage->m_bph.m_lsn = lsn;
	assert(freePage->m_lastHeaderPos >= sizeof(VLRHeapRecordPageInfo) || freePage->m_lastHeaderPos == 0);
	assert(!(freePage->m_lastHeaderPos == sizeof(VLRHeapRecordPageInfo) && freePage->m_recordNum != 1));
	session->markDirty(freePageHdl);
	session->releasePage(&freePageHdl);

	nftrace(ts.hp, tout << "rid of new record: " << rid(rowId));

	m_dboStats->countIt(DBOBJ_ITEM_INSERT);

	return rowId;
}

/**
 * 记录insert日志
 *
 * @param session          会话
 * @param rid              操作记录的RowId
 * @param record           insert插入的记录
 * @param bitmapModified   是否修改了对应的位图页
 * @return                 日志的lsn
 */
#ifndef NTSE_VERIFY_EX
u64 VariableLengthRecordHeap::writeInsertLog(Session *session, RowId rid, const Record *record, bool bitmapModified) {
#else
u64 VariableLengthRecordHeap::writeInsertLog(Session *session, RowId rid, const Record *record, bool bitmapModified, u64 oldLSN) {
#endif
	assert(session && record);
	byte logData[Limits::PAGE_SIZE];
	Stream s(logData, sizeof(logData));
	try {
#ifdef NTSE_VERIFY_EX
		s.write((u64)0)->write(oldLSN);
#endif
		s.write(rid);
		s.write(bitmapModified);
		s.write(record->m_format == REC_COMPRESSED);
		s.write(record->m_size)->write(record->m_data, record->m_size);
	} catch (NtseException &) {
		assert(false);
	}
	return session->writeLog(LOG_HEAP_INSERT, m_tableDef->m_id, logData, s.getSize());
}

/**
 * 解析INSERT日志
 * @param log  日志内容
 * @param logSize  日志大小
 * @param rowId  insert操作记录的RowId
 * @param bitmapModified  操作是否改变了位图页
 * @param record OUT  记录解析出的长度和数据放到这里
 */
#ifndef NTSE_VERIFY_EX
void VariableLengthRecordHeap::parseInsertLog(const byte *log, uint logSize, RowId *rowId, bool *bitmapModified, Record *record) {
#else
void VariableLengthRecordHeap::parseInsertLog(const byte *log, uint logSize, RowId *rowId, bool *bitmapModified, Record *record, u64 *oldLSN) {
#endif
	assert(log && rowId && bitmapModified && record);
	Stream s((byte *)log, logSize);
#ifdef NTSE_VERIFY_EX
	s.read(oldLSN)->read(oldLSN);
#endif
	s.read(rowId);
	s.read(bitmapModified);
	bool isRealCompressed = false;
	s.read(&isRealCompressed);
	record->m_format = isRealCompressed ? REC_COMPRESSED : REC_VARLEN;
	s.read(&record->m_size);
	assert(s.getSize() + record->m_size == logSize);
	memcpy(record->m_data, log + s.getSize(), record->m_size);
}

/**
 * @see DrsHeap::getRecordFromInsertlog
 */
void VariableLengthRecordHeap::getRecordFromInsertlog(LogEntry *log, Record *outRec) {
	assert(outRec->m_format == REC_VARLEN || outRec->m_format == REC_COMPRESSED);
	Stream s(log->m_data, log->m_size);
	bool bitmapModified;
#ifdef NTSE_VERIFY_EX
	u64 oldLSN;
	s.read(&oldLSN)->read(&oldLSN);
#endif
	s.read(&outRec->m_rowId);
	s.read(&bitmapModified);
	bool isRealCompressed = false;
	s.read(&isRealCompressed);
	outRec->m_format = isRealCompressed ? REC_COMPRESSED : REC_VARLEN;
	s.read(&outRec->m_size);
	assert(s.getSize() + outRec->m_size == log->m_size);
	outRec->m_data = log->m_data + s.getSize();
}

/**
 * 根据空闲空间freeSpaceSize给出对应的spaceFlag
 * @param oldFlag               旧的页面标志
 * @param freeSpaceSize         空闲空间大小
 * @param freeSpaceIncrease     空闲空间是增加还是减少
 * @return                      记录页面位图
 */
u8 VariableLengthRecordHeap::getRecordPageFlag(u8 oldFlag, u16 freeSpaceSize, bool freeSpaceIncrease) {
	if (freeSpaceIncrease) { // 空闲空间增加，比如delete操作
		if (freeSpaceSize > FLAG11_SPACE_DOWN_LIMIT || (oldFlag == 0x3 && freeSpaceSize > FLAG10_SPACE_LIMIT))
			return 0x3;
		if (freeSpaceSize > FLAG10_SPACE_DOWN_LIMIT || (oldFlag == 0x2 && freeSpaceSize > FLAG01_SPACE_LIMIT))
			return 0x2;
		if (freeSpaceSize > FLAG01_SPACE_DOWN_LIMIT || (oldFlag == 0x1 && freeSpaceSize > FLAG00_SPACE_LIMIT))
			return 0x1;
		return 0x0;
	}
	// 空闲空间减少，比如insert操作
	if (freeSpaceSize > FLAG10_SPACE_LIMIT)
		return 0x3; // 11
	if (freeSpaceSize > FLAG01_SPACE_LIMIT)
		return 0x2; // 10
	if (freeSpaceSize > FLAG00_SPACE_LIMIT)
		return 0x1; // 01
	return 0x0; // 00
}

/**
 * 根据空闲空间需求得到对应的flag要求。
 * @param spaceRequest          空间需求大小，单位byte
 * @return                      空闲空间标志位, 11,10,01之一
 */
u8 VariableLengthRecordHeap::spaceResquestToFlag(uint spaceRequest) {
	assert(spaceRequest > 0 && spaceRequest <= FLAG10_SPACE_LIMIT);
	if (spaceRequest > FLAG01_SPACE_LIMIT)
		return 0x3; // 11
	if (spaceRequest > FLAG00_SPACE_LIMIT)
		return 0x2; // 10
	return 0x1; // 10
}

/**
 * 操作后更新位图
 * @param session              会话
 * @param recPageNum           操作对应的记录页面号，也就是需要更新的位图页的辖下页面。
 * @param newFlag              记录页面所对应的新位图标志
 * @param lsn                  上次操作的lsn，为0表示不更新lsn域。
 * @param bmpHdl               位图的页面句柄
 */
void VariableLengthRecordHeap::updateBitmap(Session *session, u64 recPageNum, u8 newFlag, u64 lsn, BufferPageHandle* bmpHdl) {
	assert(session && bmpHdl);
	BufferPageHandle *headerPageHdl = NULL;
	BitmapPage *bmp;
	u8 newBmFlag; // 位图页在中央位图中的flag标志
	u64 bmPageNum = getBmpNum(recPageNum);

	ftrace(ts.hp, tout << session << recPageNum << newFlag << (BitmapPage *)bmpHdl->getPage() << bmPageNum);

	bmp = (BitmapPage *)bmpHdl->getPage();
	if (bmPageNum == m_lastBmpNum) { // 最后一个位图页，需要锁住首页锁以防止修改位图时堆扩容
		SYNCHERE(SP_HEAP_VLR_UPDATEBITMAP_BEFORE_LOCK_HEADER_PAGE);
		headerPageHdl = lockHeaderPage(session, Exclusived);
		if (bmPageNum == m_lastBmpNum) { // 再次确认
			if (bmp->setBits((int)(recPageNum - bmPageNum - 1), newFlag, &newBmFlag, (uint)(m_maxPageNum - m_lastBmpNum))) {
				// 需要修改中央位图。
				m_cbm.setBitmap((int)((bmPageNum - 1 - m_centralBmpNum) / (BitmapPage::CAPACITY + 1)), newBmFlag);
			}
		} else { // bitmap已经不是最后一页，不用担心再次扩容
			session->unlockPage(&headerPageHdl);
			goto updateBitmap_setBits_safely;
		}
		session->unlockPage(&headerPageHdl);
	} else { // 并非最后一个位图页，不用担心扩容操作产生影响
updateBitmap_setBits_safely:
		if (bmp->setBits((int)(recPageNum - bmPageNum - 1), newFlag, &newBmFlag, BitmapPage::CAPACITY)) {
			m_cbm.setBitmap((int)((bmPageNum - 1 - m_centralBmpNum) / (BitmapPage::CAPACITY + 1)), newBmFlag);
		}
	}
	if (lsn) bmp->u.m_header.m_bph.m_lsn = lsn;
	session->markDirty(bmpHdl);
	session->releasePage(&bmpHdl);
}


/**
 * 操作后更新位图
 * @param session                      会话
 * @param lsn                          上次操作的lsn，为0表示不更新lsn域。
 * @param recPageNum1, recPageNum2     操作对应的记录页面号，也就是需要更新的位图页的辖下页面
 * @param newFlag1, newFlag2           记录页面所对应的新位图标志
 * @param bmpHdl                       位图页句柄
 */
void VariableLengthRecordHeap::updateSameBitmap(Session *session, u64 lsn, u64 recPageNum1, u8 newFlag1, u64 recPageNum2, u8 newFlag2, BufferPageHandle* bmpHdl) {
	BufferPageHandle *headerPageHdl = NULL;
	BitmapPage *bmp;
	u8 newBmFlag; // 位图页在中央位图中的flag标志
	u64 bmPageNum = getBmpNum(recPageNum1);
	assert(recPageNum2 && bmPageNum == getBmpNum(recPageNum2));

	int idx1 = (int)(recPageNum1 - bmPageNum - 1);
	int idx2 = (int)(recPageNum2 - bmPageNum - 1);

	bmp = (BitmapPage *)bmpHdl->getPage();

	ftrace(ts.hp, tout << session << recPageNum1 << newFlag1 << recPageNum2 << newFlag2 <<  bmPageNum << bmp);

	if (bmPageNum == m_lastBmpNum) { // 最后一个位图页，需要锁住首页锁以防止修改位图时堆扩容
		SYNCHERE(SP_HEAP_VLR_UPDATESAMEBMP_BEFORE_LOCK_HEADER_PAGE);
		headerPageHdl = lockHeaderPage(session, Exclusived);
		if (bmPageNum == m_lastBmpNum) { // 再次确认
			if (bmp->setBits(&newBmFlag, (uint)(m_maxPageNum - m_lastBmpNum), idx1, newFlag1, idx2, newFlag2)) {
				// 需要修改中央位图。
				m_cbm.setBitmap((int)((bmPageNum - 1 - m_centralBmpNum) / (BitmapPage::CAPACITY + 1)), newBmFlag);
			}
		} else { // bitmap已经不是最后一页，不用担心再次扩容
			session->unlockPage(&headerPageHdl);
			goto updateBitmap_setBits_safely;
		}
		session->unlockPage(&headerPageHdl);
	} else { // 并非最后一个位图页，不用担心扩容操作产生影响
updateBitmap_setBits_safely:
		if (bmp->setBits(&newBmFlag, BitmapPage::CAPACITY, idx1, newFlag1, idx2, newFlag2)) {
			m_cbm.setBitmap((int)((bmPageNum - 1 - m_centralBmpNum) / (BitmapPage::CAPACITY + 1)), newBmFlag);
		}
	}
	if (lsn) bmp->u.m_header.m_bph.m_lsn = lsn;
	session->markDirty(bmpHdl);
	session->releasePage(&bmpHdl);
}

/**
 * @see DrsHeap::del
 */
bool VariableLengthRecordHeap::del(Session *session, RowId rowId) {
	PROFILE(PI_VariableLengthRecordHeap_del);

	assert(session);
	ftrace(ts.hp, tout << session << rowId);

	u64 pageNum = RID_GET_PAGE(rowId);
	uint slotNum = RID_GET_SLOT(rowId);
	if (pageIsBitmap(pageNum) || pageNum > m_maxUsedPageNum) return false;
	BufferPageHandle *recPageHdl, *bmpHdl, *tagBmpHdl;
	VLRHeapRecordPageInfo *recPage;
	VLRHeader *recHdr;
	u8 newFlag;
	u64 lsn;

	recPageHdl = GET_PAGE(session, m_heapFile, PAGE_HEAP, pageNum, Exclusived, m_dboStats, NULL);
	SYNCHERE(SP_HEAP_VLR_DEL_LOCKED_THE_PAGE);
	recPage = (VLRHeapRecordPageInfo *)recPageHdl->getPage();
	verify_ex(vs.hp, recPage->verifyPage());
	recHdr = recPage->getVLRHeader(slotNum);
	if (!recHdr || recHdr->m_ifEmpty || recHdr->m_ifTarget) { // 不可以为空，不可以为link target。
		session->releasePage(&recPageHdl);
		return false;
	}
	if (!recHdr->m_ifLink) { // 并非link，不涉及多页面
		recPage->deleteSlot(slotNum);
		newFlag = getRecordPageFlag(recPage->m_pageBitmapFlag, recPage->m_freeSpaceSize, true);
		if (newFlag != recPage->m_pageBitmapFlag) {
			bmpHdl = GET_PAGE(session, m_heapFile, PAGE_HEAP, getBmpNum(pageNum), Exclusived, m_dboStats, NULL);
#ifndef NTSE_VERIFY_EX
			lsn = writeDeleteLog(session, rowId, true, 0, false);
#else
			lsn = writeDeleteLog(session, rowId, true, 0, false, recPage->m_bph.m_lsn, 0);
#endif
			updateBitmap(session, pageNum, newFlag, lsn, bmpHdl);
			recPage->m_pageBitmapFlag = newFlag;
		} else {
#ifndef NTSE_VERIFY_EX
			lsn = writeDeleteLog(session, rowId, false, 0, false);
#else
			lsn = writeDeleteLog(session, rowId, false, 0, false, recPage->m_bph.m_lsn, 0);
#endif
		}
		assert(!session->isLogging() || lsn > recPage->m_bph.m_lsn);
		recPage->m_bph.m_lsn = lsn;
	} else { // 多页面操作
		assert(!recHdr->m_ifTarget);
		BufferPageHandle *tagPageHdl;
		VLRHeapRecordPageInfo *tagPage;
		VLRHeader *tagRecHdr;
		u8 newTagFlag;
		u64 tagRowId = RID_READ(recPage->getRecordData(recHdr));
		u64 tagPageNum = RID_GET_PAGE(tagRowId);
		uint tagSlotNum = RID_GET_SLOT(tagRowId);

		lockSecondPage(session, pageNum, tagPageNum, &recPageHdl, &tagPageHdl);
		tagPage = (VLRHeapRecordPageInfo *)tagPageHdl->getPage();
		verify_ex(vs.hp, recPage->verifyPage() && tagPage->verifyPage());
		tagRecHdr = tagPage->getVLRHeader(tagSlotNum);
		// 上层通过行锁或表锁保证记录不会被修改
		assert(tagRecHdr && !tagRecHdr->m_ifEmpty && tagRecHdr->m_ifTarget && RID_READ(tagPage->getRecordData(tagRecHdr) - LINK_SIZE) == rowId);
		recPage->deleteSlot(slotNum);
		tagPage->deleteSlot(tagSlotNum);
		newFlag = getRecordPageFlag(recPage->m_pageBitmapFlag ,recPage->m_freeSpaceSize, true);
		newTagFlag = getRecordPageFlag(tagPage->m_pageBitmapFlag, tagPage->m_freeSpaceSize, true);

		if (newFlag == recPage->m_pageBitmapFlag) {
			if (newTagFlag == tagPage->m_pageBitmapFlag) {
#ifndef NTSE_VERIFY_EX
				lsn = writeDeleteLog(session, rowId, false,	tagRowId, false);
#else
				lsn = writeDeleteLog(session, rowId, false,	tagRowId, false, recPage->m_bph.m_lsn, tagPage->m_bph.m_lsn);
#endif
			} else {
				tagBmpHdl = GET_PAGE(session, m_heapFile, PAGE_HEAP, getBmpNum(tagPageNum), Exclusived, m_dboStats, NULL);
#ifndef NTSE_VERIFY_EX
				lsn = writeDeleteLog(session, rowId, false,	tagRowId, true);
#else
				lsn = writeDeleteLog(session, rowId, false,	tagRowId, true, recPage->m_bph.m_lsn, tagPage->m_bph.m_lsn);
#endif
				updateBitmap(session, tagPageNum, newTagFlag, lsn, tagBmpHdl);
				tagPage->m_pageBitmapFlag = newTagFlag;
			}
		} else {
			if (newTagFlag == tagPage->m_pageBitmapFlag) {
				bmpHdl = GET_PAGE(session, m_heapFile, PAGE_HEAP, getBmpNum(pageNum), Exclusived, m_dboStats, NULL);
#ifndef NTSE_VERIFY_EX
				lsn = writeDeleteLog(session, rowId, true, tagRowId, false);
#else
				lsn = writeDeleteLog(session, rowId, true, tagRowId, false, recPage->m_bph.m_lsn, tagPage->m_bph.m_lsn);
#endif
				updateBitmap(session, pageNum, newFlag, lsn, bmpHdl);
			} else {
				if (getBmpNum(pageNum) == getBmpNum(tagPageNum)) {
					bmpHdl = GET_PAGE(session, m_heapFile, PAGE_HEAP, getBmpNum(pageNum), Exclusived, m_dboStats, NULL);
#ifndef NTSE_VERIFY_EX
					lsn = writeDeleteLog(session, rowId, true, tagRowId, true);
#else
					lsn = writeDeleteLog(session, rowId, true, tagRowId, true, recPage->m_bph.m_lsn, tagPage->m_bph.m_lsn);
#endif
					updateSameBitmap(session, lsn, pageNum, newFlag, tagPageNum, newTagFlag, bmpHdl);
				} else {
					getTwoBitmap(session, getBmpNum(pageNum), getBmpNum(tagPageNum), &bmpHdl, &tagBmpHdl);
#ifndef NTSE_VERIFY_EX
					lsn = writeDeleteLog(session, rowId, true, tagRowId, true);
#else
					lsn = writeDeleteLog(session, rowId, true, tagRowId, true, recPage->m_bph.m_lsn, tagPage->m_bph.m_lsn);
#endif
					updateBitmap(session, pageNum, newFlag, lsn, bmpHdl);
					updateBitmap(session, tagPageNum, newTagFlag, lsn, tagBmpHdl);
				}
				tagPage->m_pageBitmapFlag = newTagFlag;
			}
			recPage->m_pageBitmapFlag = newFlag;
		}
		assert(!session->isLogging() || (lsn > tagPage->m_bph.m_lsn && lsn > recPage->m_bph.m_lsn));
		tagPage->m_bph.m_lsn = recPage->m_bph.m_lsn = lsn;
		session->markDirty(tagPageHdl);
		verify_ex(vs.hp, tagPage->verifyPage());
		session->releasePage(&tagPageHdl);
	}
	session->markDirty(recPageHdl);
	verify_ex(vs.hp, recPage->verifyPage());
	session->releasePage(&recPageHdl);

	m_dboStats->countIt(DBOBJ_ITEM_DELETE);

	return true;
}

/**
 * 记录del日志
 *
 * @param session              会话
 * @param rid                  操作记录的RowId
 * @param bitmapModified       是否修改了对应的位图页
 * @param tagRid               如果所删除的记录是个link，那么这里记录了target的RowId
 * @param tagBitmapModified    target对应的位图是否修改
 * @return                     日志的lsn
 */
#ifndef NTSE_VERIFY_EX
u64 VariableLengthRecordHeap::writeDeleteLog(Session *session, RowId rid, bool bitmapModified, RowId tagRid, bool tagBitmapModified) {
#else
u64 VariableLengthRecordHeap::writeDeleteLog(Session *session, RowId rid, bool bitmapModified, RowId tagRid, bool tagBitmapModified, u64 oldSrcLSN, u64 oldTagLSN) {
	ftrace(ts.hp, tout << session << rid << bitmapModified << tagRid << tagBitmapModified);
#endif
	/* 当tagRid为0时说明不涉及多个页面的操作，最后一个不用记录。 */
	assert(session);
#ifndef NTSE_VERIFY_EX
	byte logData[32]; // 够用
#else
	byte logData[48];
#endif
	Stream s(logData, sizeof(logData));
	try {
#ifdef NTSE_VERIFY_EX
		s.write(oldSrcLSN);
		s.write(oldTagLSN);
#endif
		s.write(rid);
		s.write(bitmapModified);
		s.write(tagRid);
		if (tagRid) {
			s.write(tagBitmapModified);
		}
	} catch (NtseException &) {
		assert(false);
	}
	return session->writeLog(LOG_HEAP_DELETE, m_tableDef->m_id, logData, s.getSize());
}

/**
 * 解析DELETE日志
 * @param log                   日志内容
 * @param logSize               日志大小
 * @param rowId                 insert操作记录的RowId
 * @param bitmapModified        操作是否改变了位图页
 * @param tagRid                如果有target页面，则target的RowId
 * @param tagBitmapModified     是否修改了target页面
 */
#ifndef NTSE_VERIFY_EX
void VariableLengthRecordHeap::parseDeleteLog(const byte *log, uint logSize, RowId *rowId, bool *bitmapModified, RowId *tagRid, bool *tagBitmapModified) {
#else
void VariableLengthRecordHeap::parseDeleteLog(const byte *log, uint logSize, RowId *rowId, bool *bitmapModified, RowId *tagRid, bool *tagBitmapModified, u64 *oldSrcLSN, u64 *oldTagLSN) {
#endif
	assert(log && rowId && bitmapModified && tagRid && tagBitmapModified);
	Stream s((byte *)log, logSize);
#ifdef NTSE_VERIFY_EX
	s.read(oldSrcLSN);
	s.read(oldTagLSN);
#endif
	s.read(rowId);
	s.read(bitmapModified);
	s.read(tagRid);
	if (*tagRid) s.read(tagBitmapModified);
}


/**
 * @see DrsHeap::update
 */
bool VariableLengthRecordHeap::update(Session *session, RowId rowId, const SubRecord *subRecord) {
	PROFILE(PI_VariableLengthRecordHeap_update_SubRecord);

	assert(session && subRecord && subRecord->m_data);
	assert(subRecord->m_format == REC_REDUNDANT);
#ifndef NTSE_UNIT_TEST
	//assert(0 == strcmp(m_tableDef->m_name, "smallLob") || session->isRowLocked(m_tableDef->m_id, rowId, Exclusived));
#endif
	ftrace(ts.hp, tout << session << rid(rowId) << subRecord);
	bool success = doUpdate(session, rowId, subRecord, NULL);

	/* 更新统计信息 */
	if (success) {
		++m_status.m_rowsUpdateSubRec;
	}

	return success;
}

/**
 * @see DrsHeap::update
 */
bool VariableLengthRecordHeap::update(Session *session, RowId rowId, const Record *record) {
	PROFILE(PI_VariableLengthRecordHeap_update_Record);

	assert(session && record && record->m_data);
	assert(record->m_format == REC_VARLEN || record->m_format == REC_COMPRESSED);
	assert(record->m_size <= m_tableDef->m_maxRecSize);
	ftrace(ts.hp, tout << session << rid(rowId) << record);
	bool success = doUpdate(session, rowId, NULL, record);

	/* 更新统计信息 */
	if (success) {
		++m_status.m_rowsUpdateRecord;
	}

	return success;
}

/**
 * 实际进行update操作的函数
 * @post                 如果传入的是SubRecord，那么会在内部组装出一个Record对象，log的时候必须完整记录Record对象，以免丢失。
 * @param session        会话
 * @param rowId          RowId
 * @param subRecord      更新记录属性子集
 * @param record         更新记录
 * @return               更新成功完成返回true
 */
bool VariableLengthRecordHeap::doUpdate(Session *session, RowId rowId, const SubRecord *subRecord, const Record *record) {
	ftrace(ts.hp, tout << session << rowId << subRecord << record);

	assert(subRecord || record);
	BufferPageHandle *srcPageHdl, *oldTagPageHdl, *newTagPageHdl;
	BufferPageHandle *srcBmpHdl, *oldBmpHdl, *newBmpHdl;
	VLRHeapRecordPageInfo *srcPage, *oldTagPage, *newTagPage;
	VLRHeader *srcHdr, *oldTagHdr;
	u16 requestSpace;
	u8 srcFlag, oldTagFlag, newTagFlag;
	u64 srcPageNum, oldTagPgN, newTagPgN, lsn = 0;
	u64 oldTagRowId, newTagRowId;
	uint srcSlot, oldTagSlot, newTagSlot;
	Record oldRec, newRec;

	srcPageNum = RID_GET_PAGE(rowId);
	srcSlot = RID_GET_SLOT(rowId);
	if (pageIsBitmap(srcPageNum) || srcPageNum > m_maxUsedPageNum)
		return false;
	bool reLockTwoPages, reLockThreePages, newRecReady = false;
	bool sizeInc;
	u64 lastFalsePgN = 0;
	byte data[Limits::PAGE_SIZE - sizeof(VLRHeapRecordPageInfo)]; // 最大长度
	newRec.m_format = oldRec.m_format = REC_VARLEN;
	if (record) {
		newRec.m_size = record->m_size;
		newRec.m_data = record->m_data;
		newRec.m_format = record->m_format;
		newRecReady = true;
	} else {
		newRec.m_data = data;
		newRec.m_size = Limits::PAGE_SIZE - sizeof(VLRHeapRecordPageInfo);
	}
	// 取得目标页，并且check
	srcPageHdl = GET_PAGE(session, m_heapFile, PAGE_HEAP, srcPageNum, Exclusived, m_dboStats, NULL);
	srcPage = (VLRHeapRecordPageInfo *)srcPageHdl->getPage();
	verify_ex(vs.hp, srcPage->verifyPage());
	srcHdr = srcPage->getVLRHeader(srcSlot);
	if (!srcHdr || srcHdr->m_ifEmpty || srcHdr->m_ifTarget) {
		session->releasePage(&srcPageHdl);
		return false;
	}
	if (!srcHdr->m_ifLink) { // 目标记录不是一个link
		assert(!srcHdr->m_ifTarget);
		oldRec.m_data = srcPage->getRecordData(srcHdr);
		oldRec.m_size = srcHdr->m_size;
		oldRec.m_format = srcHdr->m_ifCompressed > 0 ? REC_COMPRESSED : REC_VARLEN;
		// 构造新记录，先不考虑直接本地更新优化
		if (!newRecReady) {
			if (m_cprsRcdExtrator != NULL) {
				RecordOper::updateRcdWithDic(session->getMemoryContext(), m_tableDef, m_cprsRcdExtrator, &oldRec, subRecord, &newRec);
			} else {
				newRec.m_size = RecordOper::getUpdateSizeVR(m_tableDef, &oldRec, subRecord);
				RecordOper::updateRecordVR(m_tableDef, &oldRec, subRecord, newRec.m_data);
			}
			assert(newRec.m_size <= m_tableDef->m_maxRecSize);
			newRecReady = true;
		}
		if ((int)newRec.m_size <= srcPage->m_freeSpaceSize + ((srcHdr->m_size < LINK_SIZE) ? LINK_SIZE : srcHdr->m_size)) { // 可以尝试defrag先
			sizeInc = (((s16)srcHdr->m_size - (s16)newRec.m_size) > 0);
			srcPage->updateLocalRecord(&newRec, srcSlot);
			srcFlag = getRecordPageFlag(srcPage->m_pageBitmapFlag, srcPage->m_freeSpaceSize, sizeInc);
			goto update_only_source_page_need_update;
		}
		// 本页无法更新，需要寻找新的页面
		requestSpace = (u16)(newRec.m_size + sizeof(VLRHeader) + LINK_SIZE);
update_find_free_page_to_insert_target:
		findFreePage(session, requestSpace, &newTagPgN, None, lastFalsePgN); // 仅取得页面号，不加锁
		if (newTagPgN == srcPageNum) {
			lastFalsePgN = srcPageNum;
			goto update_find_free_page_to_insert_target;
		}
		reLockTwoPages = lockSecondPage(session, srcPageNum, newTagPgN, &srcPageHdl, &newTagPageHdl);
		if (reLockTwoPages) {
			// 锁定前曾经放过锁，看下源页现在是不是有足够的空间了
			// 由于记录一定被锁定，不需要验证记录是否被他人更新
			assert(srcHdr && !srcHdr->m_ifEmpty && !srcHdr->m_ifTarget);
			assert(!srcHdr->m_ifLink);
			if (srcPage->m_freeSpaceSize >= newRec.m_size - ((srcHdr->m_size < LINK_SIZE) ? LINK_SIZE : srcHdr->m_size)) { // 空间突然又够了
				session->releasePage(&newTagPageHdl);
				srcPage->updateLocalRecord(&newRec, srcSlot);
				srcFlag = getRecordPageFlag(srcPage->m_pageBitmapFlag, srcPage->m_freeSpaceSize, false);
				goto update_only_source_page_need_update;
			}
		}
		// 检查拿到手的新页面
		newTagPage = (VLRHeapRecordPageInfo *)newTagPageHdl->getPage();
		verify_ex(vs.hp, newTagPage->verifyPage());
		if (newTagPage->m_freeSpaceSize < newRec.m_size + sizeof(VLRHeader) + LINK_SIZE) { // 页面空间已经不够，重新申请
			session->releasePage(&newTagPageHdl);
			lastFalsePgN = newTagPgN;
			goto update_find_free_page_to_insert_target;
		}
		// 两个页面已经到手，更新之。
		goto update_source_link_to_new_target;
	} else { // 目标记录是一个link
		oldTagRowId = RID_READ(srcPage->getRecordData(srcHdr));
		oldTagPgN = RID_GET_PAGE(oldTagRowId);
		oldTagSlot = RID_GET_SLOT(oldTagRowId);
		// 取得target页面
		assert(srcPageNum != oldTagPgN);
		reLockTwoPages = lockSecondPage(session, srcPageNum, oldTagPgN, &srcPageHdl, &oldTagPageHdl);
		verify_ex(vs.hp, !reLockTwoPages || srcPage->verifyPage());
#ifdef NTSE_UNIT_TEST
		if (reLockTwoPages) { // 加锁过程中曾经放过锁，重新获取原页面
			assert(srcHdr && !srcHdr->m_ifEmpty && !srcHdr->m_ifTarget);
			assert(srcHdr->m_ifLink);
			assert(RID_READ(srcPage->getRecordData(srcHdr)) == oldTagRowId);
		}
#endif
		// 至此取得了source和target两个页面
		oldTagPage = (VLRHeapRecordPageInfo *)oldTagPageHdl->getPage();
		verify_ex(vs.hp, oldTagPage->verifyPage());
		oldTagHdr = oldTagPage->getVLRHeader(oldTagSlot);
		assert(oldTagHdr->m_ifTarget);
		oldRec.m_size = oldTagHdr->m_size - LINK_SIZE;
		oldRec.m_data = oldTagPage->getRecordData(oldTagHdr);
		oldRec.m_format = oldTagHdr->m_ifCompressed > 0 ? REC_COMPRESSED : REC_VARLEN;
		if (!newRecReady) { // 构造新记录，不考虑直接本地更新优化
			if (m_cprsRcdExtrator != NULL) {
				RecordOper::updateRcdWithDic(session->getMemoryContext(), m_tableDef, m_cprsRcdExtrator, &oldRec, subRecord, &newRec);
			} else {
				newRec.m_size = RecordOper::getUpdateSizeVR(m_tableDef, &oldRec, subRecord);
				RecordOper::updateRecordVR(m_tableDef, &oldRec, subRecord, newRec.m_data);
			}
			assert(newRec.m_size <= m_tableDef->m_maxRecSize);
			newRecReady = true;
		}

		// 尝试source页面内更新
		if (srcPage->m_freeSpaceSize + LINK_SIZE >= (int)newRec.m_size) {
			goto update_local_update_in_source_page;
		}
		// 尝试old target页面内更新
		if ((uint)oldTagPage->m_freeSpaceSize + oldTagHdr->m_size >= LINK_SIZE + newRec.m_size) {
			goto update_local_update_in_old_target_page;
		}
		// 无法在source和old target页面之间解决问题，空间都不够。
		requestSpace = (u16)(newRec.m_size + sizeof(VLRHeader) + LINK_SIZE);
update_look_for_third_page:
		findFreePage(session, requestSpace, &newTagPgN, None, lastFalsePgN);
		if (newTagPgN == oldTagPgN || newTagPgN == srcPageNum) {
			lastFalsePgN = newTagPgN;
			goto update_look_for_third_page;
		}
		// 同时取得三个页面
		if (srcPageNum < oldTagPgN) {
			if (oldTagPgN < newTagPgN) { // 最简单情况
				goto update_new_target_page_number_is_biggest;
			} else {
				if (srcPageNum < newTagPgN) { // 说明srcPageNum < newTagPgN < oldTagPgN
					reLockTwoPages = lockSecondPage(session, oldTagPgN, newTagPgN, &oldTagPageHdl, &newTagPageHdl);
					if (reLockTwoPages) { // 需要重新验证old target，因为source page没有释放，所以old target slot合法性不会出问题，看看空间够不够
						verify_ex(vs.hp, oldTagPage->verifyPage());
						if ((uint)oldTagPage->m_freeSpaceSize + oldTagHdr->m_size >= LINK_SIZE + newRec.m_size) { // 空间够
							session->releasePage(&newTagPageHdl);
							goto update_local_update_in_old_target_page;
						}
					}
					newTagPage = (VLRHeapRecordPageInfo *)newTagPageHdl->getPage();
					verify_ex(vs.hp, newTagPage->verifyPage());
					if (newTagPage->m_freeSpaceSize < newRec.m_size + LINK_SIZE + sizeof(VLRHeader)) {
						session->releasePage(&newTagPageHdl);
						lastFalsePgN = newTagPgN;
						goto update_look_for_third_page;
					}
					goto update_have_three_pages_validated;
				} else { // 说明newTagPgN < srcPageNum < oldTagPgN，最坏的情况
					SYNCHERE(SP_HEAP_VLR_BEFORE_GET_MIN_NEWPAGE);
					reLockThreePages = lockThirdMinPage(session, oldTagPgN, srcPageNum, newTagPgN, &oldTagPageHdl, &srcPageHdl, &newTagPageHdl);
					if (reLockThreePages) {
						verify_ex(vs.hp, oldTagPage->verifyPage() && srcPage->verifyPage());
						goto update_thoroughly_validate_three_pages;
					}
					else {
						newTagPage = (VLRHeapRecordPageInfo *)newTagPageHdl->getPage();
						verify_ex(vs.hp, newTagPage->verifyPage());
						if (newTagPage->m_freeSpaceSize < newRec.m_size + LINK_SIZE + sizeof(VLRHeader)) {
							session->releasePage(&newTagPageHdl);
							lastFalsePgN = newTagPgN;
							goto update_look_for_third_page;
						}
						goto update_have_three_pages_validated;
					}
				}
			}
		} else { // 说明 oldTagPgN < srcPageNum
			if (srcPageNum < newTagPgN) { // 最简单情况
				goto update_new_target_page_number_is_biggest;
			} else { // oldTagPgN < srcPageNum && newTagPgN < srcPageNum
				if (oldTagPgN < newTagPgN) { // oldTagPgN < newTagPgN < srcPageNum
					reLockTwoPages = lockSecondPage(session, srcPageNum, newTagPgN, &srcPageHdl, &newTagPageHdl);
					if (reLockTwoPages) {  // 需要验证首页，old target页并未放开，所以记录不可能被更新。只需要看页面空间是否足够
						verify_ex(vs.hp, oldTagPage->verifyPage());
						if (srcPage->m_freeSpaceSize + LINK_SIZE >= (int)newRec.m_size) {
							session->releasePage(&newTagPageHdl);
							goto update_local_update_in_source_page;
						}
					}
					newTagPage = (VLRHeapRecordPageInfo *)newTagPageHdl->getPage();
					verify_ex(vs.hp, newTagPage->verifyPage());
					if (newTagPage->m_freeSpaceSize < newRec.m_size + LINK_SIZE + sizeof(VLRHeader)) {
						session->releasePage(&newTagPageHdl);
						lastFalsePgN = newTagPgN;
						goto update_look_for_third_page;
					}
					goto update_have_three_pages_validated;
				} else { // newTagPgN < oldTagPgN < srcPageNum
					reLockThreePages = lockThirdMinPage(session, srcPageNum, oldTagPgN, newTagPgN, &srcPageHdl, &oldTagPageHdl, &newTagPageHdl);
					if (reLockThreePages) {
						verify_ex(vs.hp, oldTagPage->verifyPage() && srcPage->verifyPage());
						goto update_thoroughly_validate_three_pages;
					} else {
						newTagPage = (VLRHeapRecordPageInfo *)newTagPageHdl->getPage();
						verify_ex(vs.hp, newTagPage->verifyPage());
						if (newTagPage->m_freeSpaceSize < newRec.m_size + LINK_SIZE + sizeof(VLRHeader)) {
							session->releasePage(&newTagPageHdl);
							lastFalsePgN = newTagPgN;
							goto update_look_for_third_page;
						}
						goto update_have_three_pages_validated;
					}
				}
			}
		}
		// 同时取得了三个页面
	} // if (!srcHdr->m_ifLink)

	// 记录原来不是个链接记录，并且更新后在原页面放得下
update_only_source_page_need_update:
	/* @pre  已经更新过页面，取得了新的srcFlag */
	/* 这种情况下日志只须记录SubRecord即可保证redo */
	if (srcFlag != srcPage->m_pageBitmapFlag) {
		srcBmpHdl = GET_PAGE(session, m_heapFile, PAGE_HEAP, getBmpNum(srcPageNum), Exclusived, m_dboStats, NULL);
#ifndef NTSE_VERIFY_EX
		lsn = writeUpdateLog(session, rowId, NULL, true, false, false, false, 0, false, 0, false);
#else
		lsn = writeUpdateLog(session, rowId, NULL, true, false, false, false, 0, false, 0, false, srcPage->m_bph.m_lsn, 0, 0);
#endif
		updateBitmap(session, srcPageNum, srcFlag, lsn, srcBmpHdl);
		srcPage->m_pageBitmapFlag = srcFlag;
	} else {
#ifndef NTSE_VERIFY_EX
		lsn = writeUpdateLog(session, rowId, NULL, false, false, false, false, 0, false, 0, false);
#else
		lsn = writeUpdateLog(session, rowId, NULL, false, false, false, false, 0, false, 0, false, srcPage->m_bph.m_lsn, 0, 0);
#endif
	}
	assert(!session->isLogging() || lsn > srcPage->m_bph.m_lsn);
	srcPage->m_bph.m_lsn = lsn;
	session->markDirty(srcPageHdl);
	verify_ex(vs.hp, srcPage->verifyPage());
	session->releasePage(&srcPageHdl);
	return true;

	// 记录原来不是个链接记录，更新后在原页面放不下了，放到新页面
update_source_link_to_new_target:
	/* @pre  已经取得两个页面，并且验证过可用性 */
	/* 这种情况下必须记录Record才能保证redo */
	newTagSlot = newTagPage->insertRecord(&newRec, rowId);
	newTagRowId = RID(newTagPgN, newTagSlot);
	newTagFlag = getRecordPageFlag(newTagPage->m_pageBitmapFlag, newTagPage->m_freeSpaceSize, false);
	srcPage->linklizeSlot(newTagRowId, srcSlot);
	srcFlag = getRecordPageFlag(srcPage->m_pageBitmapFlag, srcPage->m_freeSpaceSize, true);
	// 更新位图记录日志
	if (srcFlag == srcPage->m_pageBitmapFlag) {
		if (newTagFlag == newTagPage->m_pageBitmapFlag) {
#ifndef NTSE_VERIFY_EX
			lsn = writeUpdateLog(session, rowId, &newRec, false, true, false, false, newTagRowId, false, 0, false);
#else
			lsn = writeUpdateLog(session, rowId, &newRec, false, true, false, false, newTagRowId, false, 0, false, srcPage->m_bph.m_lsn, 0, newTagPage->m_bph.m_lsn);
#endif
		} else {
			newBmpHdl = GET_PAGE(session, m_heapFile, PAGE_HEAP, getBmpNum(newTagPgN), Exclusived, m_dboStats, NULL);
#ifndef NTSE_VERIFY_EX
			lsn = writeUpdateLog(session, rowId, &newRec, false, true, false, false, newTagRowId, true, 0, false);
#else
			lsn = writeUpdateLog(session, rowId, &newRec, false, true, false, false, newTagRowId, true, 0, false, srcPage->m_bph.m_lsn, 0, newTagPage->m_bph.m_lsn);
#endif
			updateBitmap(session, newTagPgN, newTagFlag, lsn, newBmpHdl);
			newTagPage->m_pageBitmapFlag = newTagFlag;
		}
	} else {
		if (newTagFlag == newTagPage->m_pageBitmapFlag) {
			srcBmpHdl = GET_PAGE(session, m_heapFile, PAGE_HEAP, getBmpNum(srcPageNum), Exclusived, m_dboStats, NULL);
#ifndef NTSE_VERIFY_EX
			lsn = writeUpdateLog(session, rowId, &newRec, true, true, false, false, newTagRowId, false, 0, false);
#else
			lsn = writeUpdateLog(session, rowId, &newRec, true, true, false, false, newTagRowId, false, 0, false, srcPage->m_bph.m_lsn, 0, newTagPage->m_bph.m_lsn);
#endif
			updateBitmap(session, srcPageNum, srcFlag, lsn, srcBmpHdl);
		} else {
			if (getBmpNum(srcPageNum) == getBmpNum(newTagPgN)) {
				srcBmpHdl = GET_PAGE(session, m_heapFile, PAGE_HEAP, getBmpNum(srcPageNum), Exclusived, m_dboStats, NULL);
#ifndef NTSE_VERIFY_EX
				lsn = writeUpdateLog(session, rowId, &newRec, true, true, false, false, newTagRowId, true, 0, false);
#else
				lsn = writeUpdateLog(session, rowId, &newRec, true, true, false, false, newTagRowId, true, 0, false, srcPage->m_bph.m_lsn, 0, newTagPage->m_bph.m_lsn);
#endif
				updateSameBitmap(session, lsn, srcPageNum, srcFlag, newTagPgN, newTagFlag, srcBmpHdl);
			} else {
				getTwoBitmap(session, getBmpNum(srcPageNum), getBmpNum(newTagPgN), &srcBmpHdl, &newBmpHdl);
#ifndef NTSE_VERIFY_EX
				lsn = writeUpdateLog(session, rowId, &newRec, true, true, false, false, newTagRowId, true, 0, false);
#else
				lsn = writeUpdateLog(session, rowId, &newRec, true, true, false, false, newTagRowId, true, 0, false, srcPage->m_bph.m_lsn, 0, newTagPage->m_bph.m_lsn);
#endif
				updateBitmap(session, newTagPgN, newTagFlag, lsn, newBmpHdl);
				updateBitmap(session, srcPageNum, srcFlag, lsn, srcBmpHdl);
			}
			newTagPage->m_pageBitmapFlag = newTagFlag;
		}
		srcPage->m_pageBitmapFlag = srcFlag;
	}
	assert(!session->isLogging() || (lsn > srcPage->m_bph.m_lsn && lsn > newTagPage->m_bph.m_lsn));
	srcPage->m_bph.m_lsn = newTagPage->m_bph.m_lsn = lsn;
	session->markDirty(newTagPageHdl);
	session->markDirty(srcPageHdl);
	verify_ex(vs.hp, newTagPage->verifyPage());
	session->releasePage(&newTagPageHdl);
	verify_ex(vs.hp, srcPage->verifyPage());
	session->releasePage(&srcPageHdl);
	return true;

	// 记录原来是一个链接，更新后，在原页面可以存储下，取消链接
update_local_update_in_source_page:
	/* @pre 已经有source页面和old target页面，newRec准备好，且source页面空间足够更新记录使用 */
	/* 这种情况下必须记录Record才能保证redo */
	srcPage->updateLocalRecord(&newRec, srcSlot);
	oldTagPage->deleteSlot(oldTagSlot);
	srcFlag = getRecordPageFlag(srcPage->m_pageBitmapFlag, srcPage->m_freeSpaceSize, false);
	oldTagFlag = getRecordPageFlag(oldTagPage->m_pageBitmapFlag, oldTagPage->m_freeSpaceSize, true);
	if (srcFlag == srcPage->m_pageBitmapFlag) {
		if (oldTagFlag == oldTagPage->m_pageBitmapFlag) {
#ifndef NTSE_VERIFY_EX
			lsn = writeUpdateLog(session, rowId, &newRec, false, false, true, false, 0, false, oldTagRowId, false);
#else
			lsn = writeUpdateLog(session, rowId, &newRec, false, false, true, false, 0, false, oldTagRowId, false, srcPage->m_bph.m_lsn, oldTagPage->m_bph.m_lsn, 0);
#endif
		} else {
			oldBmpHdl = GET_PAGE(session, m_heapFile, PAGE_HEAP, getBmpNum(oldTagPgN), Exclusived, m_dboStats, NULL);
#ifndef NTSE_VERIFY_EX
			lsn = writeUpdateLog(session, rowId, &newRec, false, false, true, false, 0, false, oldTagRowId, true);
#else
			lsn = writeUpdateLog(session, rowId, &newRec, false, false, true, false, 0, false, oldTagRowId, true, srcPage->m_bph.m_lsn, oldTagPage->m_bph.m_lsn, 0);
#endif
			updateBitmap(session, oldTagPgN, oldTagFlag, lsn, oldBmpHdl);
			oldTagPage->m_pageBitmapFlag = oldTagFlag;
		}
	} else {
		if (oldTagFlag == oldTagPage->m_pageBitmapFlag) {
			srcBmpHdl = GET_PAGE(session, m_heapFile, PAGE_HEAP, getBmpNum(srcPageNum), Exclusived, m_dboStats, NULL);
#ifndef NTSE_VERIFY_EX
			lsn = writeUpdateLog(session, rowId, &newRec, true, false, true, false, 0, false, oldTagRowId, false);
#else
			lsn = writeUpdateLog(session, rowId, &newRec, true, false, true, false, 0, false, oldTagRowId, false, srcPage->m_bph.m_lsn, oldTagPage->m_bph.m_lsn, 0);
#endif
			updateBitmap(session, srcPageNum, srcFlag, lsn, srcBmpHdl);
		} else {
			if (getBmpNum(srcPageNum) == getBmpNum(oldTagPgN)) {
				srcBmpHdl = GET_PAGE(session, m_heapFile, PAGE_HEAP, getBmpNum(srcPageNum), Exclusived, m_dboStats, NULL);
#ifndef NTSE_VERIFY_EX
				lsn = writeUpdateLog(session, rowId, &newRec, true, false, true, false, 0, false, oldTagRowId, true);
#else
				lsn = writeUpdateLog(session, rowId, &newRec, true, false, true, false, 0, false, oldTagRowId, true, srcPage->m_bph.m_lsn, oldTagPage->m_bph.m_lsn, 0);
#endif
				updateSameBitmap(session, lsn, srcPageNum, srcFlag, oldTagPgN, oldTagFlag, srcBmpHdl);
			} else {
				getTwoBitmap(session, getBmpNum(srcPageNum), getBmpNum(oldTagPgN), &srcBmpHdl, &oldBmpHdl);
#ifndef NTSE_VERIFY_EX
				lsn = writeUpdateLog(session, rowId, &newRec, true, false, true, false, 0, false, oldTagRowId, true);
#else
				lsn = writeUpdateLog(session, rowId, &newRec, true, false, true, false, 0, false, oldTagRowId, true, srcPage->m_bph.m_lsn, oldTagPage->m_bph.m_lsn, 0);
#endif
				updateBitmap(session, oldTagPgN, oldTagFlag, lsn, oldBmpHdl);
				updateBitmap(session, srcPageNum, srcFlag, lsn, srcBmpHdl);
			}
			oldTagPage->m_pageBitmapFlag = oldTagFlag;
		}
		srcPage->m_pageBitmapFlag = srcFlag;
	}
	assert(!session->isLogging() || (lsn > oldTagPage->m_bph.m_lsn && lsn > srcPage->m_bph.m_lsn));
	oldTagPage->m_bph.m_lsn = srcPage->m_bph.m_lsn = lsn;
	session->markDirty(oldTagPageHdl);
	session->markDirty(srcPageHdl);
	verify_ex(vs.hp, oldTagPage->verifyPage());
	session->releasePage(&oldTagPageHdl);
	verify_ex(vs.hp, srcPage->verifyPage());
	session->releasePage(&srcPageHdl);
	return true;

	// 记录原来是一个链接，更新后还是存储在原链接目标页
update_local_update_in_old_target_page:
	/* @pre  已经有source页面和old target页面，newRec准备好，且old target页面空间足够更新记录使用 */
	/* 这种情况下日志只须记录SubRecord即可保证redo */
	sizeInc = (((s16)oldTagHdr->m_size - LINK_SIZE - (s16)newRec.m_size) > 0);
	oldTagPage->updateLocalRecord(&newRec, oldTagSlot);
	oldTagFlag = getRecordPageFlag(oldTagPage->m_pageBitmapFlag, oldTagPage->m_freeSpaceSize, sizeInc);
	if (oldTagFlag == oldTagPage->m_pageBitmapFlag) {
#ifndef NTSE_VERIFY_EX
		lsn = writeUpdateLog(session, rowId, NULL, false, false, true, true, 0, false, oldTagRowId, false);
#else
		lsn = writeUpdateLog(session, rowId, NULL, false, false, true, true, 0, false, oldTagRowId, false, srcPage->m_bph.m_lsn, oldTagPage->m_bph.m_lsn, 0);
#endif
	} else {
		oldBmpHdl = GET_PAGE(session, m_heapFile, PAGE_HEAP, getBmpNum(oldTagPgN), Exclusived, m_dboStats, NULL);
#ifndef NTSE_VERIFY_EX
		lsn = writeUpdateLog(session, rowId, NULL, false, false, true, true, 0, false, oldTagRowId, true);
#else
		lsn = writeUpdateLog(session, rowId, NULL, false, false, true, true, 0, false, oldTagRowId, true, srcPage->m_bph.m_lsn, oldTagPage->m_bph.m_lsn, 0);
#endif
		updateBitmap(session, oldTagPgN, oldTagFlag, lsn, oldBmpHdl);
		oldTagPage->m_pageBitmapFlag = oldTagFlag;
	}
	assert(!session->isLogging() || (lsn > srcPage->m_bph.m_lsn && lsn > oldTagPage->m_bph.m_lsn));
	srcPage->m_bph.m_lsn = oldTagPage->m_bph.m_lsn = lsn;
	session->markDirty(oldTagPageHdl);
	session->markDirty(srcPageHdl);
	verify_ex(vs.hp, oldTagPage->verifyPage());
	session->releasePage(&oldTagPageHdl);
	verify_ex(vs.hp, srcPage->verifyPage());
	session->releasePage(&srcPageHdl);
	return true;

	// 记录原来是一个链接，更新后要存储在另一个页，且该页页号比原页和原链接目标页都大
update_new_target_page_number_is_biggest:
	SYNCHERE(SP_HEAP_VLR_BEFORE_GET_MAX_NEWPAGE);
	newTagPageHdl = GET_PAGE(session, m_heapFile, PAGE_HEAP, newTagPgN, Exclusived, m_dboStats, NULL);
	newTagPage = (VLRHeapRecordPageInfo *)newTagPageHdl->getPage();
	if (newTagPage->m_freeSpaceSize < newRec.m_size + LINK_SIZE + sizeof(VLRHeader)) {
		session->releasePage(&newTagPageHdl);
		lastFalsePgN = newTagPgN;
		goto update_look_for_third_page;
	}
	goto update_have_three_pages_validated;
update_thoroughly_validate_three_pages:
	/* @pre 我们有三个页面的handle，但是其他都不可靠，需要验证 */
	// 验证source
	assert(srcHdr && !srcHdr->m_ifEmpty && !srcHdr->m_ifTarget);
	assert(srcHdr->m_ifLink);
	assert(RID_READ(srcPage->getRecordData(srcHdr)) == oldTagRowId);
	if (srcPage->m_freeSpaceSize + LINK_SIZE >= (int)newRec.m_size) {
		session->releasePage(&newTagPageHdl);
		goto update_local_update_in_source_page;
	}
	// 验证old target
	if ((uint)oldTagPage->m_freeSpaceSize + oldTagHdr->m_size >= newRec.m_size + LINK_SIZE) {
		session->releasePage(&newTagPageHdl);
		goto update_local_update_in_old_target_page;
	}
	// 验证new target page
	newTagPage = (VLRHeapRecordPageInfo *)newTagPageHdl->getPage();
	if (newTagPage->m_freeSpaceSize < newRec.m_size + LINK_SIZE + sizeof(VLRHeader)) {
		session->releasePage(&newTagPageHdl);
		lastFalsePgN = newTagPgN;
		goto update_look_for_third_page;
	}
update_have_three_pages_validated:
	/* 最坏情况，source和old target页面都无法放下记录，必须放到new target页面中*/
	/* @pre 所有信息均已经验证过，可靠可用 */
	/* 这种情况下必须记录完整Record才能保证redo */
	oldTagPage->deleteSlot(oldTagSlot);
	newTagSlot = newTagPage->insertRecord(&newRec, rowId);
	newTagRowId = RID(newTagPgN, newTagSlot);
	RID_WRITE(newTagRowId, srcPage->getRecordData(srcHdr));
	/* 源页不会有空间变化 */
	oldTagFlag = getRecordPageFlag(oldTagPage->m_pageBitmapFlag, oldTagPage->m_freeSpaceSize, true);
	newTagFlag = getRecordPageFlag(newTagPage->m_pageBitmapFlag, newTagPage->m_freeSpaceSize, false);
	if (oldTagFlag == oldTagPage->m_pageBitmapFlag) {
		if (newTagFlag == newTagPage->m_pageBitmapFlag) {
#ifndef NTSE_VERIFY_EX
			lsn = writeUpdateLog(session, rowId, &newRec, false, true, true, false, newTagRowId, false, oldTagRowId, false);
#else
			lsn = writeUpdateLog(session, rowId, &newRec, false, true, true, false, newTagRowId, false, oldTagRowId, false, srcPage->m_bph.m_lsn, oldTagPage->m_bph.m_lsn, newTagPage->m_bph.m_lsn);
#endif
		} else {
			newBmpHdl = GET_PAGE(session, m_heapFile, PAGE_HEAP, getBmpNum(newTagPgN), Exclusived, m_dboStats, NULL);
#ifndef NTSE_VERIFY_EX
			lsn = writeUpdateLog(session, rowId, &newRec, false, true, true, false, newTagRowId, true, oldTagRowId, false);
#else
			lsn = writeUpdateLog(session, rowId, &newRec, false, true, true, false, newTagRowId, true, oldTagRowId, false, srcPage->m_bph.m_lsn, oldTagPage->m_bph.m_lsn, newTagPage->m_bph.m_lsn);
#endif
			updateBitmap(session, newTagPgN, newTagFlag, lsn, newBmpHdl);
			newTagPage->m_pageBitmapFlag = newTagFlag;
		}
	} else {
		if (newTagFlag == newTagPage->m_pageBitmapFlag) {
			oldBmpHdl = GET_PAGE(session, m_heapFile, PAGE_HEAP, getBmpNum(oldTagPgN), Exclusived, m_dboStats, NULL);
#ifndef NTSE_VERIFY_EX
			lsn = writeUpdateLog(session, rowId, &newRec, false, true, true, false, newTagRowId, false, oldTagRowId, true);
#else
			lsn = writeUpdateLog(session, rowId, &newRec, false, true, true, false, newTagRowId, false, oldTagRowId, true, srcPage->m_bph.m_lsn, oldTagPage->m_bph.m_lsn, newTagPage->m_bph.m_lsn);
#endif
			updateBitmap(session, oldTagPgN, oldTagFlag, lsn, oldBmpHdl);
		} else {
			if (getBmpNum(newTagPgN) == getBmpNum(oldTagPgN)) {
				oldBmpHdl = GET_PAGE(session, m_heapFile, PAGE_HEAP, getBmpNum(oldTagPgN), Exclusived, m_dboStats, NULL);
#ifndef NTSE_VERIFY_EX
				lsn = writeUpdateLog(session, rowId, &newRec, false, true, true, false, newTagRowId, true, oldTagRowId, true);
#else
				lsn = writeUpdateLog(session, rowId, &newRec, false, true, true, false, newTagRowId, true, oldTagRowId, true, srcPage->m_bph.m_lsn, oldTagPage->m_bph.m_lsn, newTagPage->m_bph.m_lsn);
#endif
				updateSameBitmap(session, lsn, newTagPgN, newTagFlag, oldTagPgN, oldTagFlag, oldBmpHdl);
			} else {
				getTwoBitmap(session, getBmpNum(newTagPgN), getBmpNum(oldTagPgN), &newBmpHdl, &oldBmpHdl);
#ifndef NTSE_VERIFY_EX
				lsn = writeUpdateLog(session, rowId, &newRec, false, true, true, false, newTagRowId, true, oldTagRowId, true);
#else
				lsn = writeUpdateLog(session, rowId, &newRec, false, true, true, false, newTagRowId, true, oldTagRowId, true, srcPage->m_bph.m_lsn, oldTagPage->m_bph.m_lsn, newTagPage->m_bph.m_lsn);
#endif
				updateBitmap(session, newTagPgN, newTagFlag, lsn, newBmpHdl);
				updateBitmap(session, oldTagPgN, oldTagFlag, lsn, oldBmpHdl);
			}
			newTagPage->m_pageBitmapFlag = newTagFlag;
		}
		oldTagPage->m_pageBitmapFlag = oldTagFlag;
	}
	assert(!session->isLogging() || (lsn > srcPage->m_bph.m_lsn && lsn > oldTagPage->m_bph.m_lsn && lsn > newTagPage->m_bph.m_lsn));
	newTagPage->m_bph.m_lsn = oldTagPage->m_bph.m_lsn = srcPage->m_bph.m_lsn = lsn;
	session->markDirty(srcPageHdl);
	session->markDirty(oldTagPageHdl);
	session->markDirty(newTagPageHdl);
	verify_ex(vs.hp, srcPage->verifyPage());
	session->releasePage(&srcPageHdl);
	verify_ex(vs.hp, oldTagPage->verifyPage());
	session->releasePage(&oldTagPageHdl);
	verify_ex(vs.hp, newTagPage->verifyPage());
	session->releasePage(&newTagPageHdl);
	return true;
}

/**
 * 记录update日志
 *
 * @param session                 会话
 * @param rid                     操作记录的RowId
 * @param record                  更新的record（如果必要的话，可能是NULL值）
 * @param bitmapModified          是否修改了对应的位图页
 * @param hasNewTarget            是否有新的target页面
 * @param hasOldTarget            被update的记录是否有target
 * @param updateInOldTag          是否更新在老target页面内
 * @param newTagRowId             如果有new target，那么它的RowId
 * @param newTagBmpModified       如果有new target，那么是否修改了对应的位图页
 * @param oldTagRowId             如果有old target，那么RowId
 * @param oldTagBmpModified       如果有old target，那么是否修改了对应的位图页
 * @return                        日志的lsn
 */
#ifndef NTSE_VERIFY_EX
u64 VariableLengthRecordHeap::writeUpdateLog(Session *session, RowId rid, const Record *record,
										bool bitmapModified, bool hasNewTarget, bool hasOldTarget, bool updateInOldTag,
										u64 newTagRowId, bool newTagBmpModified, u64 oldTagRowId, bool oldTagBmpModified) {
#else
u64 VariableLengthRecordHeap::writeUpdateLog(Session *session, RowId rid, const Record *record,
											 bool bitmapModified, bool hasNewTarget, bool hasOldTarget, bool updateInOldTag,
											 u64 newTagRowId, bool newTagBmpModified, u64 oldTagRowId, bool oldTagBmpModified,
											 u64 srcOldLSN, u64 oldTagOldLSN, u64 newTagOldLSN) {
	ftrace(ts.hp, tout << session << rid << record << "bitmapModified:" << bitmapModified << "hasNewTarget:" << hasNewTarget
		<< "hasOldTarget:" << hasOldTarget << "updateInOldTag:" << updateInOldTag << "newTagRowId:" << newTagRowId
		<< "newTagBmpModified:" << newTagBmpModified << "oldTagRowId:" << oldTagRowId << "oldTagBmpModified:" << oldTagBmpModified);
#endif
	assert(session);
	byte logDate[Limits::PAGE_SIZE];
	Stream s(logDate, sizeof(logDate));
	try {
#ifdef NTSE_VERIFY_EX
		s.write(srcOldLSN)->write(oldTagOldLSN)->write(newTagOldLSN);
#endif
		s.write(rid);
		// 记录首页修改
		s.write(bitmapModified)->write(hasNewTarget)->write(hasOldTarget)->write(updateInOldTag);
		if (hasNewTarget) s.write(newTagRowId)->write(newTagBmpModified);
		if (hasOldTarget) s.write(oldTagRowId)->write(oldTagBmpModified);
		if (record) {
			s.write(true); //记录record
			s.write(record->m_format == REC_COMPRESSED);//记录是否是压缩格式
			s.write(record->m_size)->write(record->m_data, record->m_size);
		} else {
			s.write(false); //不记录record
		}
	} catch (NtseException &) {
		assert(false);
	}
	return session->writeLog(LOG_HEAP_UPDATE, m_tableDef->m_id, logDate, s.getSize());
}

/**
 * 解析UPDATE日志
 * @param log                     日志
 * @param logSize                 日志大小
 * @param RowId OUT               操作记录的RowId
 * @param bitmapModified OUT      是否修改了对应的位图页
 * @param hasNewTarget OUT        是否有新的target页面
 * @param hasOldTarget OUT        被update的记录是否有target
 * @param updateInOldTag OUT      是否更新在老target页面内
 * @param newTagRowId OUT         如果有new target，那么它的RowId
 * @param newTagBmpModified OUT   如果有new target，那么是否修改了对应的位图页
 * @param oldTagRowId OUT         如果有old target，那么RowId
 * @param oldTagBmpModified OUT   如果有old target，那么是否修改了对应的位图页
 * @param hasRecord               有完整记录
 * @param record OUT              如果存在，传出log中记录的Record内容
 */
#ifndef NTSE_VERIFY_EX
void VariableLengthRecordHeap::parseUpdateLog(const byte *log, uint logSize, RowId *rowId, bool *bitmapModified,
											  bool *hasNewTarget, bool *hasOldTarget, bool *updateInOldTag,
											  u64 *newTagRowId, bool *newTagBmpModified, u64 *oldTagRowId, bool *oldTagBmpModified,
											  bool *hasRecord, Record *record) {
#else
void VariableLengthRecordHeap::parseUpdateLog(const byte *log, uint logSize, RowId *rowId, bool *bitmapModified,
											  bool *hasNewTarget, bool *hasOldTarget, bool *updateInOldTag,
											  u64 *newTagRowId, bool *newTagBmpModified, u64 *oldTagRowId, bool *oldTagBmpModified,
											  bool *hasRecord, Record *record,
											  u64 *srcOldLSN, u64 *oldTagOldLSN, u64 *newTagOldLSN) {
#endif
	Stream s((byte *)log, logSize);
#ifdef NTSE_VERIFY_EX
	s.read(srcOldLSN)->read(oldTagOldLSN)->read(newTagOldLSN);
#endif
	s.read(rowId);
	s.read(bitmapModified)->read(hasNewTarget)->read(hasOldTarget)->read(updateInOldTag);
	if (*hasNewTarget) s.read(newTagRowId)->read(newTagBmpModified);
	if (*hasOldTarget) s.read(oldTagRowId)->read(oldTagBmpModified);
	s.read(hasRecord);
	if (*hasRecord) {
		bool isRealCompressed = false;
		s.read(&isRealCompressed);
		record->m_format = isRealCompressed ? REC_COMPRESSED : REC_VARLEN;
		s.read(&record->m_size);
		record->m_data = (byte *)log + s.getSize();
	}
}


/**
 * 在已锁定页面的情况下锁定另外一个页面
 * @pre                         第一页必须已经锁定
 * @post                        因为可能会放锁再锁定，所以必须相应验证首页的数据是否依然有效，
 *                              比如首页的记录页面，本函数不负责这些。
 * @param session               会话
 * @param firstPageNum          已经被锁定的页面的pageId
 * @param secondPageNum         想要锁定的第二页面的pageId
 * @param firstPageHdl OUT      第一页面的句柄指针的指针
 * @param secondPageHdl OUT     第二页面的句柄指针（传入时应该是未定值，锁定后传出的是有效句柄）
 * @param lockMode              欲加锁模式
 * @return                      锁定过程中如果第一页面曾经放锁，返回true；没有放锁直接锁定成功，返回false;
 */
bool VariableLengthRecordHeap::lockSecondPage(Session *session, u64 firstPageNum, u64 secondPageNum, BufferPageHandle **firstPageHdl,
											  BufferPageHandle **secondPageHdl, LockMode lockMode) {
	assert(secondPageNum != firstPageNum);
	assert(session && firstPageHdl && secondPageHdl);
	if (secondPageNum > firstPageNum) { // 顺序加锁
		*secondPageHdl = GET_PAGE(session, m_heapFile, PAGE_HEAP, secondPageNum, lockMode, m_dboStats, NULL);
		return false;
	}
	//首先尝试try lock
	SYNCHERE(SP_HEAP_VLR_LOCKSECONDPAGE_BEFORE_TRYLOCK);
	*secondPageHdl = TRY_GET_PAGE(session, m_heapFile, PAGE_HEAP, secondPageNum, lockMode, m_dboStats);
	if (*secondPageHdl) { //锁定成功
		return false;
	}
	ftrace(ts.hp, tout << "try lock failed");
	session->unlockPage(firstPageHdl);
	SYNCHERE(SP_HEAP_VLR_LOCKSECONDPAGE_AFTER_RELEASE_FIRST);
	*secondPageHdl = GET_PAGE(session, m_heapFile, PAGE_HEAP, secondPageNum, lockMode, m_dboStats, NULL);
	LOCK_PAGE_HANDLE(session, *firstPageHdl, lockMode);
	return true;
}

/**
 * 锁定页号最小的页面
 *
 * @pre                    页面次序已经排好，fPgNum > sPgNum > tPgNum
 * @post                   三个页面按顺序锁定，原来的页面指针可能需要验证
 * @param session          资源会话
 * @param fPgNum           页号最大的第一页面
 * @param sPgNum           页号第二大的第二页面
 * @param tPgNum           第三页面，页号最小
 * @param fPageHdl OUT     第一个页面的句柄，已经取得
 * @param sPageHdl OUT     第二个页面的句柄，已经取得
 * @param tPageHdl OUT     第三个页面的句柄，尚未取得
 * @return                 有释放锁操作返回true，否则返回false
 */
bool VariableLengthRecordHeap::lockThirdMinPage(Session *session, u64 fPgNum, u64 sPgNum, u64 tPgNum, BufferPageHandle **fPageHdl, BufferPageHandle **sPageHdl, BufferPageHandle **tPageHdl) {
	assert(session && fPageHdl && sPageHdl && tPageHdl);
	assert(tPgNum < fPgNum && tPgNum < sPgNum);
	UNREFERENCED_PARAMETER(sPgNum);
	UNREFERENCED_PARAMETER(fPgNum);
	SYNCHERE(SP_HEAP_VLR_LOCK3RDPAGE_BEFORE_TRYLOCK);
	*tPageHdl = TRY_GET_PAGE(session, m_heapFile, PAGE_HEAP, tPgNum, Exclusived, m_dboStats);
	if (!*tPageHdl) {
		ftrace(ts.hp, tout << "try lock failed");
		session->unlockPage(fPageHdl);
		session->unlockPage(sPageHdl);
		SYNCHERE(SP_HEAP_VLR_LOCKTHIRDPAGE_AFTER_RELEASE);
		*tPageHdl = GET_PAGE(session, m_heapFile, PAGE_HEAP, tPgNum, Exclusived, m_dboStats, NULL);
		LOCK_PAGE_HANDLE(session, *sPageHdl, Exclusived);
		LOCK_PAGE_HANDLE(session, *fPageHdl, Exclusived);
		return true;
	}
	return false;
}


/* 表扫描 */

/**
 * @see DrsHeap::beginScan
 */
DrsHeapScanHandle* VariableLengthRecordHeap::beginScan(Session *session, SubrecExtractor *extractor, LockMode lockMode, RowLockHandle **rlh, bool returnLinkSrc) {
	PROFILE(PI_VariableLengthRecordHeap_beginScan);

    assert(session);
	ftrace(ts.hp, tout << session << extractor << lockMode << rlh << returnLinkSrc);

	ScanHandleAddInfo *info = new ScanHandleAddInfo;
	info->m_returnLinkSrc = returnLinkSrc;
	info->m_nextBmpNum = m_centralBmpNum + 1;

	DrsHeapScanHandle *scanHdl = new DrsHeapScanHandle(this, session, extractor, lockMode, rlh, info);
	m_dboStats->countIt(DBOBJ_SCAN);
	return scanHdl;
}

/**
 * @see DrsHeap::getNext
 */
bool VariableLengthRecordHeap::getNext(DrsHeapScanHandle *scanHandle, SubRecord *subRec) {
	PROFILE(PI_VariableLengthRecordHeap_getNext);

	/* 对于link两种扫描方式，一是跳过link只扫target，二是跳过target，当碰到link时去取得target
	   第一种方法快，但是扫描时如果有update而update到后向的页面中，则记录会被多次扫描；
	   第二种方法安全，但是扫描速度慢，因为会时不时去获取link所指向的target页面。
	   用那种方法由创建DrsHeapScanHandle时的returnLinkSr参数决定
	*/
	assert(scanHandle && subRec);
	assert(subRec->m_format == REC_REDUNDANT);
	RowId rowId = scanHandle->getNextPos();
	bool& returnLinkSrc = ((ScanHandleAddInfo *)scanHandle->getOtherInfo())->m_returnLinkSrc;
	u64& nextBmpNum = ((ScanHandleAddInfo *)scanHandle->getOtherInfo())->m_nextBmpNum;
	u64 pageNum = RID_GET_PAGE(rowId);//, dataPgN;
	u16 slotNum = RID_GET_SLOT(rowId); // 从下一个slot开始扫
	RowId sourceRid, targetRid, posRid;
	Session *session = scanHandle->getSession();
	BufferPageHandle *pageHdl = scanHandle->getPage(); // 可能是NULL
	BufferPageHandle *targetPageHdl = NULL;
	VLRHeapRecordPageInfo *targetPg;
	VLRHeapRecordPageInfo *page;
	VLRHeader *recHdr;
	RowLockHandle *rowLockHdl = NULL;

	// 找到下一个可用页面
	for (;;) {
		if(pageIsBitmap(pageNum, nextBmpNum)) {
			assert(pageNum > m_centralBmpNum);
			pageNum++;
			slotNum = 0;
			continue;
		}
		if (pageNum > m_maxUsedPageNum) return false;

		if (pageHdl) {
			LOCK_PAGE_HANDLE(session, pageHdl,Shared);
		} else {
			pageHdl = GET_PAGE(session, m_heapFile, PAGE_HEAP, pageNum, Shared, m_dboStats, NULL);
			scanHandle->setPage(pageHdl);
		}
		page = (VLRHeapRecordPageInfo *)pageHdl->getPage();

getNext_get_header:
		recHdr = page->getVLRHeader(slotNum);
		if (!recHdr) { // 该页没有其他号了
			assert(pageHdl);
			session->releasePage(&pageHdl);
			scanHandle->setPage(NULL);
			pageNum++;
			slotNum = 0;
			continue;
		}
		if (recHdr->m_ifEmpty) {
			slotNum++;
			goto getNext_get_header;
		}
		if ((returnLinkSrc && recHdr->m_ifTarget) || (!returnLinkSrc && recHdr->m_ifLink)) {
			slotNum++;
			goto getNext_get_header;
		}
		posRid = RID(pageNum, slotNum);
		// 设置sourceID和lastPos
		if (recHdr->m_ifTarget) {// 肯定returnLinkSrc是false
			assert(!returnLinkSrc);
			sourceRid = RID_READ((byte *)page + recHdr->m_offset);
		} else {
			sourceRid = posRid;
		}
		scanHandle->setNextPos(RID(pageNum, (slotNum + 1))); // 下一次位置后移
		// 尝试取得行锁
		if (scanHandle->getLockMode() != None) {
			SYNCHERE(SP_HEAP_VLR_GETNEXT_BEFORE_LOCKROW);
			rowLockHdl = TRY_LOCK_ROW(session, m_tableDef->m_id, sourceRid, scanHandle->getLockMode());
			SYNCHERE(SP_HEAP_VLR_GETNEXT_AFTER_LOCKROW);
			if (!rowLockHdl) {
				session->unlockPage(&pageHdl);
				SYNCHERE(SP_HEAP_VLR_GETNEXT_UNLOCKPAGE_TO_GET_ROWLOCK);
				assert(pageHdl);
				rowLockHdl = LOCK_ROW(session, m_tableDef->m_id, sourceRid, scanHandle->getLockMode());
				LOCK_PAGE_HANDLE(session, pageHdl, Shared);
				// 验证
				recHdr = page->getVLRHeader(slotNum);
				if (!recHdr || recHdr->m_ifEmpty
					|| (returnLinkSrc && recHdr->m_ifTarget) || (!returnLinkSrc && recHdr->m_ifLink)) {
					// 当前记录被删除，或变成不是想要的链接类型时，跳过当前记录
					slotNum++;
					session->unlockRow(&rowLockHdl);
					goto getNext_get_header;
				}
				if ((recHdr->m_ifTarget && RID_READ((byte *)page + recHdr->m_offset) != sourceRid) // 依然是个链接目的，但链接的源已经变了
					|| (sourceRid != posRid && !recHdr->m_ifTarget)) { // 当前位置以前是个链接目的，现在已经不是了
					session->unlockRow(&rowLockHdl);
					goto getNext_get_header;
				}
			}
		} //取得了行锁
		if (returnLinkSrc && recHdr->m_ifLink) { // 按source读取，并且记录头是个link source
			targetRid = RID_READ(page->getRecordData(recHdr));
			if (rowLockHdl) {
				session->unlockPage(&pageHdl);
				SYNCHERE(SP_HEAP_VLR_GETNEXT_BEFORE_EXTRACTTARGET);
				extractTarget(session, targetRid, sourceRid, subRec, scanHandle->getExtractor());
			} else {
				// 未加行锁
				SYNCHERE(SP_HEAP_VLR_GETNEXT_BEFORE_LOCK_SECOND_PAGE);
				if (lockSecondPage(session, pageNum, RID_GET_PAGE(targetRid), &pageHdl, &targetPageHdl, Shared)) {
					if (!recHdr || recHdr->m_ifEmpty || !recHdr->m_ifLink
						|| RID_READ(page->getRecordData(recHdr)) != targetRid) {
							// 记录已经失效，我们继续取下一条
							session->releasePage(&targetPageHdl);
							goto getNext_get_header; // slotNum不+1，因为可能扫描方式是按target
					}
				}
				session->unlockPage(&pageHdl);
				targetPg = (VLRHeapRecordPageInfo *)targetPageHdl->getPage();
				VLRHeader *tgrHdr = targetPg->getVLRHeader(RID_GET_SLOT(targetRid));
				Record record;
				targetPg->constructRecord(tgrHdr, &record);
				scanHandle->getExtractor()->extract(&record, subRec);
				session->releasePage(&targetPageHdl);
				subRec->m_rowId = sourceRid;
			}
		} else { // 本页读取
			Record record;
			page->constructRecord(recHdr, &record);
			scanHandle->getExtractor()->extract(&record, subRec);
			session->unlockPage(&pageHdl);
			subRec->m_rowId = sourceRid;
		}
		if (scanHandle->getLockMode() != None) {
			assert(rowLockHdl);
			scanHandle->setRowLockHandle(rowLockHdl);
			assert(session->isRowLocked(m_tableDef->m_id, sourceRid, scanHandle->getLockMode()));
		}
		assert(RID_GET_PAGE(subRec->m_rowId) >= m_centralBmpNum + 2);
		assert(scanHandle->getLockMode() == None || (sourceRid == scanHandle->getRowLockHandle()->getRid()));

		m_dboStats->countIt(DBOBJ_SCAN_ITEM);
		++m_status.m_rowsReadSubRec;

		return true;
	}
}

/**
 * @see DrsHeap::updateCurrent
 */
void VariableLengthRecordHeap::updateCurrent(DrsHeapScanHandle *scanHandle, const SubRecord *subRecord) {
	PROFILE(PI_VariableLengthRecordHeap_updateCurrent);

	assert(scanHandle && subRecord && scanHandle->getRowLockHandle() != NULL);
#ifndef NTSE_UNIT_TEST
	assert(scanHandle->getSession()->isRowLocked(m_tableDef->m_id, scanHandle->getRowLockHandle()->getRid(), Exclusived));
#endif
	ftrace(ts.hp, tout << scanHandle->getSession() << scanHandle->getLockMode() << rid(scanHandle->getRowLockHandle()->getRid()));
	update(scanHandle->getSession(), scanHandle->getRowLockHandle()->getRid(), subRecord);
}

/**
 * @see DrsHeap::updateCurrent
 */
void VariableLengthRecordHeap::updateCurrent(DrsHeapScanHandle *scanHandle, const Record *rcdDirectCopy) {
	PROFILE(PI_VariableLengthRecordHeap_updateCurrent);

	assert(scanHandle && scanHandle->getRowLockHandle() != NULL);
#ifndef NTSE_UNIT_TEST
	assert(scanHandle->getSession()->isRowLocked(m_tableDef->m_id, scanHandle->getRowLockHandle()->getRid(), Exclusived));
#endif
	ftrace(ts.hp, tout << scanHandle->getSession() << scanHandle->getLockMode() << rid(scanHandle->getRowLockHandle()->getRid()));
	update(scanHandle->getSession(), scanHandle->getRowLockHandle()->getRid(), rcdDirectCopy);
}

/**
 * @see DrsHeap::deleteCurrent
 */
void VariableLengthRecordHeap::deleteCurrent(DrsHeapScanHandle *scanHandle) {
	PROFILE(PI_VariableLengthRecordHeap_deleteCurrent);

	assert(scanHandle && scanHandle->getRowLockHandle() != NULL);
#ifndef NTSE_UNIT_TEST
	assert(scanHandle->getSession()->isRowLocked(m_tableDef->m_id, scanHandle->getRowLockHandle()->getRid(), Exclusived));
#endif
	ftrace(ts.hp, tout << scanHandle->getSession() << scanHandle->getLockMode() << rid(scanHandle->getRowLockHandle()->getRid()));
	del(scanHandle->getSession(), scanHandle->getRowLockHandle()->getRid());
}

/**
 * @see DrsHeap::endScan
 */
void VariableLengthRecordHeap::endScan(DrsHeapScanHandle *scanHandle) {
	PROFILE(PI_VariableLengthRecordHeap_endScan);

	assert(scanHandle);
	ftrace(ts.hp, tout << scanHandle->getSession());
	delete (ScanHandleAddInfo *)scanHandle->getOtherInfo();
	delete scanHandle;
}


/**
 * @see DrsHeap::storePosAndInfo
 */
void VariableLengthRecordHeap::storePosAndInfo(DrsHeapScanHandle *scanHandle) {
	scanHandle->m_prevNextPos = scanHandle->m_nextPos;
	ScanHandleAddInfo *info = (ScanHandleAddInfo *)scanHandle->m_info;
	scanHandle->m_prevNextBmpNumForVarHeap = info->m_nextBmpNum;
}


/**
 * @see DrsHeap::restorePosAndInfo
 */
void VariableLengthRecordHeap::restorePosAndInfo(DrsHeapScanHandle *scanHandle) {
	scanHandle->m_nextPos = scanHandle->m_prevNextPos;
	ScanHandleAddInfo *info = (ScanHandleAddInfo *)scanHandle->m_info;
	info->m_nextBmpNum = scanHandle->m_prevNextBmpNumForVarHeap;
}


/*** redo函数 ***/
/**
 * @see DrsHeap::redoInsert
 */
RowId VariableLengthRecordHeap::redoInsert(Session *session, u64 logLSN, const byte *log, uint size, Record *record) {
	assert(session && log && record);
	assert(record->m_format == REC_VARLEN || record->m_format == REC_COMPRESSED);
	u16 slotNum;
	u64 pageNum;
	bool bmModified;
	u8 newFlag;
	BufferPageHandle *pageHdl, *headerPageHdl;
	VLRHeapRecordPageInfo *page;

#ifndef NTSE_VERIFY_EX
	parseInsertLog(log, size, &record->m_rowId, &bmModified, record);
#else
	u64 oldLSN;
	parseInsertLog(log, size, &record->m_rowId, &bmModified, record, &oldLSN);
#endif

	ftrace(ts.recv, tout << session << logLSN << log << size << record);
	nftrace(ts.recv, tout << "bmModified: " << bmModified);
	
	pageNum = RID_GET_PAGE(record->m_rowId);
	slotNum = RID_GET_SLOT(record->m_rowId);

	while (pageNum > m_maxPageNum) {
		headerPageHdl = lockHeaderPage(session, Exclusived);
		extendHeapFile(session, (HeapHeaderPageInfo *)headerPageHdl->getPage());
		session->markDirty(headerPageHdl);
		unlockHeaderPage(session, &headerPageHdl);
	}

	pageHdl = GET_PAGE(session, m_heapFile, PAGE_HEAP, pageNum, Exclusived, m_dboStats, NULL);
	page = (VLRHeapRecordPageInfo *)pageHdl->getPage();
	verify_ex(vs.hpheavy, heavyVerify(page));
	nftrace(ts.recv, tout << "pageLSN: " << page->m_bph.m_lsn);
	if (page->m_bph.m_lsn < logLSN) {
#ifdef NTSE_VERIFY_EX
		assert(page->m_bph.m_lsn == oldLSN);
#endif
		nftrace(ts.recv, tout << "Insert into slot: " << slotNum);
		page->insertIntoSlot(record, slotNum);
		newFlag = getRecordPageFlag(page->m_pageBitmapFlag, page->m_freeSpaceSize, false);
		page->m_pageBitmapFlag = newFlag;
		page->m_bph.m_lsn = logLSN;
		session->markDirty(pageHdl);
		if (pageNum > m_maxUsedPageNum) {
			nftrace(ts.recv, tout << "m_maxUsedPageNum: " << m_maxUsedPageNum << "->" << pageNum);
			m_maxUsedPageNum = pageNum;
		}
	}

	if (bmModified) {
		redoVerifyBitmap(session, pageNum, page->m_pageBitmapFlag, logLSN);
	}
	verify_ex(vs.hpheavy, heavyVerify(page));
	session->releasePage(&pageHdl);

	return record->m_rowId;
}

/**
 * @see DrsHeap::redoUpdate
 */
void VariableLengthRecordHeap::redoUpdate(Session *session, u64 logLSN, const byte *log, uint size, const SubRecord *update) {
	assert(update->m_format == REC_REDUNDANT);
	u64 srcRid, oldTagRid, newTagRid, srcPgN, oldTagPgN, newTagPgN;
#ifdef NTSE_VERIFY_EX
	u64 srcOldLSN = 0, oldTagOldLSN = 0, newTagOldLSN = 0;
#endif
	u16 srcSlot, oldTagSlot, newTagSlot;
	bool bitmapModified, hasOldTarget, hasNewTarget, updateInOldTag, oldTagBmpModified = false, newTagBmpModified = false;
	bool hasRecord;
	bool soSameBmp = false, snSameBmp = false, onSameBmp = false;
	u8 srcFlag, oldTagFlag, newTagFlag;
	BufferPageHandle *srcPageHdl, *oldTagPageHdl, *newTagPageHdl, *headerPageHdl;
	VLRHeapRecordPageInfo *srcPage, *oldTagPage, *newTagPage;
	VLRHeader *srcHdr, *oldTagHdr;
	Record rec;
#ifdef NTSE_TRACE
	rec.m_format = REC_VARLEN;
	rec.m_rowId = 0;
	rec.m_size = 0;
	rec.m_data = NULL;
#endif
	byte data[Limits::PAGE_SIZE];

	newTagRid = 0;
	newTagPgN = oldTagPgN = 0;
	newTagSlot = oldTagSlot = 0;
	srcFlag = oldTagFlag = newTagFlag = 0;

#ifndef NTSE_VERIFY_EX
	parseUpdateLog(log, size, &srcRid, &bitmapModified, &hasNewTarget, &hasOldTarget, &updateInOldTag,
		&newTagRid, &newTagBmpModified, &oldTagRid, &oldTagBmpModified, &hasRecord, &rec);
#else
	parseUpdateLog(log, size, &srcRid, &bitmapModified, &hasNewTarget, &hasOldTarget, &updateInOldTag,
		&newTagRid, &newTagBmpModified, &oldTagRid, &oldTagBmpModified, &hasRecord, &rec,
		&srcOldLSN, &oldTagOldLSN, &newTagOldLSN);
#endif
	ftrace(ts.recv, tout << session << logLSN << log << size << update);
	nftrace(ts.recv, tout << "srcRid:" << rid(srcRid) << ",bitmapModified:" << bitmapModified << ",hasNewTarget:"
		<< hasNewTarget << ",hasOldTarget:" << hasOldTarget << ",updateInOldTag:" << updateInOldTag
		<< ",newTagRid:" << rid(newTagRid) << ",newTagBmpModified:" << newTagBmpModified
		<< ",oldTagRid:" << rid(oldTagRid) << ",oldTagBmpModified:" << oldTagBmpModified
		<< ",hasRecord:" << hasRecord << ",rec:" << &rec);

	srcPgN = RID_GET_PAGE(srcRid);
	srcSlot = RID_GET_SLOT(srcRid);
	if (hasNewTarget) {
		newTagPgN = RID_GET_PAGE(newTagRid);
		newTagSlot = RID_GET_SLOT(newTagRid);
		while (newTagPgN > m_maxPageNum) {
			headerPageHdl = lockHeaderPage(session, Exclusived);
			extendHeapFile(session, (HeapHeaderPageInfo *)headerPageHdl->getPage());
			session->markDirty(headerPageHdl);
			unlockHeaderPage(session, &headerPageHdl);
		}
		if (newTagPgN > m_maxUsedPageNum) {
			nftrace(ts.recv, tout << "m_maxUsedPageNum: " << m_maxUsedPageNum << "->" << newTagPgN);
			m_maxUsedPageNum = newTagPgN;
		}
		if (getBmpNum(srcPgN) == getBmpNum(newTagPgN)) {
			snSameBmp = true;
		}
	}
	if (hasOldTarget) {
		oldTagPgN = RID_GET_PAGE(oldTagRid);
		oldTagSlot = RID_GET_SLOT(oldTagRid);
		if (getBmpNum(srcPgN) == getBmpNum(oldTagPgN)) {
			soSameBmp = true;
		}
		if (hasNewTarget && getBmpNum(newTagPgN) == getBmpNum(oldTagPgN)) {
			onSameBmp = true;
		}
	}

	if (!hasRecord) {
		rec.m_format = REC_VARLEN;
		rec.m_data = data;
		rec.m_size = Limits::PAGE_SIZE;
	}

	// 先处理source page
	srcPageHdl = GET_PAGE(session, m_heapFile, PAGE_HEAP, srcPgN, Exclusived, m_dboStats, NULL);
	srcPage = (VLRHeapRecordPageInfo *)srcPageHdl->getPage();
	verify_ex(vs.hpheavy, heavyVerify(srcPage));
	nftrace(ts.recv, tout << "Src pageLSN: " << srcPage->m_bph.m_lsn);
	if (srcPage->m_bph.m_lsn < logLSN) {
#ifdef NTSE_VERIFY_EX
		assert(srcPage->m_bph.m_lsn == srcOldLSN);
#endif
		srcHdr = srcPage->getVLRHeader(srcSlot);
		if (hasOldTarget) { // 记录曾是link，
			assert(srcHdr->m_ifLink);
			if (hasNewTarget) { // 有新目标
				assert(hasRecord);
				RID_WRITE(newTagRid, srcPage->getRecordData(srcHdr));
			} else if (!updateInOldTag) {
				assert(hasRecord);
				srcPage->updateLocalRecord(&rec, srcSlot);
				srcFlag = getRecordPageFlag(srcPage->m_pageBitmapFlag, srcPage->m_freeSpaceSize, false);
				srcPage->m_pageBitmapFlag = srcFlag;
			} else if (updateInOldTag) { // 更新到本地，数据未取出
				/* do nothing. */
			}
		} else { // 记录不是link
			assert(!hasOldTarget && !srcHdr->m_ifLink);
			int diffsize = (srcHdr->m_size > LINK_SIZE) ? srcHdr->m_size : LINK_SIZE;
			if (hasNewTarget) { // 原来本地记录更新到别处
				assert(hasRecord);
				srcPage->linklizeSlot(newTagRid, srcSlot);
				diffsize -= LINK_SIZE;
			} else { // 源页本地更新
				assert(!hasRecord);
				Record tmpRec;
				srcPage->constructRecord(srcHdr, &tmpRec);
				if (m_cprsRcdExtrator != NULL) {
					RecordOper::updateRcdWithDic(session->getMemoryContext(), m_tableDef, m_cprsRcdExtrator, &tmpRec, update, &rec);
				} else {
					assert(tmpRec.m_format == REC_VARLEN);
					rec.m_size = RecordOper::getUpdateSizeVR(m_tableDef, &tmpRec, update);
					RecordOper::updateRecordVR(m_tableDef, &tmpRec, update, rec.m_data);
				}
				srcPage->updateLocalRecord(&rec, srcSlot);
				diffsize -= (srcHdr->m_size > LINK_SIZE) ? srcHdr->m_size : LINK_SIZE;
			}
			srcFlag = getRecordPageFlag(srcPage->m_pageBitmapFlag, srcPage->m_freeSpaceSize, diffsize > 0);
			srcPage->m_pageBitmapFlag = srcFlag;
		}
		srcPage->m_bph.m_lsn = logLSN;
		session->markDirty(srcPageHdl);
	}
	if (bitmapModified && !(snSameBmp && newTagBmpModified) && !(soSameBmp && oldTagBmpModified)) {
		redoVerifyBitmap(session, srcPgN, srcPage->m_pageBitmapFlag, logLSN);
	}
	verify_ex(vs.hpheavy, heavyVerify(srcPage));
	session->releasePage(&srcPageHdl);

	// 处理old target页面
	if (hasOldTarget) {
		oldTagPageHdl = GET_PAGE(session, m_heapFile, PAGE_HEAP, oldTagPgN, Exclusived, m_dboStats, NULL);
		oldTagPage = (VLRHeapRecordPageInfo *)oldTagPageHdl->getPage();
		verify_ex(vs.hpheavy, heavyVerify(oldTagPage));
		nftrace(ts.recv, tout << "Old target pageLSN: " << oldTagPage->m_bph.m_lsn);
		if (oldTagPage->m_bph.m_lsn < logLSN) {
#ifdef NTSE_VERIFY_EX
			assert(oldTagPage->m_bph.m_lsn == oldTagOldLSN);
#endif
			oldTagHdr = oldTagPage->getVLRHeader(oldTagSlot);
			assert(oldTagHdr && oldTagHdr->m_ifTarget);
			int sizediff = oldTagHdr->m_size; // 肯定大于LINK_SIZE
			if (updateInOldTag) {
				assert(!hasRecord);
				Record tmpRec;
				oldTagPage->constructRecord(oldTagHdr, &tmpRec);
				if (m_cprsRcdExtrator != NULL) {
					RecordOper::updateRcdWithDic(session->getMemoryContext(), m_tableDef, m_cprsRcdExtrator, &tmpRec, update, &rec);
				} else {
					assert(tmpRec.m_format == REC_VARLEN);
					rec.m_size = RecordOper::getUpdateSizeVR(m_tableDef, &tmpRec, update);
					RecordOper::updateRecordVR(m_tableDef, &tmpRec, update, rec.m_data);
				}
				oldTagPage->updateLocalRecord(&rec, oldTagSlot);
				sizediff -= oldTagHdr->m_size;
			} else {
				assert(hasRecord);
				oldTagPage->deleteSlot(oldTagSlot);
			}
			oldTagFlag = getRecordPageFlag(oldTagPage->m_pageBitmapFlag, oldTagPage->m_freeSpaceSize, (sizediff > 0));
			oldTagPage->m_pageBitmapFlag = oldTagFlag;
			oldTagPage->m_bph.m_lsn = logLSN;
			session->markDirty(oldTagPageHdl);
		}
		if (oldTagBmpModified && !(soSameBmp && bitmapModified) && !(onSameBmp && newTagBmpModified)) {
			redoVerifyBitmap(session, oldTagPgN, oldTagPage->m_pageBitmapFlag, logLSN);
		}
		verify_ex(vs.hpheavy, heavyVerify(oldTagPage));
		session->releasePage(&oldTagPageHdl);
	}

	// 处理new target页面
	if (hasNewTarget) {
		newTagPageHdl = GET_PAGE(session, m_heapFile, PAGE_HEAP, newTagPgN, Exclusived, m_dboStats, NULL);
		newTagPage = (VLRHeapRecordPageInfo *)newTagPageHdl->getPage();
		verify_ex(vs.hpheavy, heavyVerify(newTagPage));
		nftrace(ts.recv, tout << "New target pageLSN: " << newTagPage->m_bph.m_lsn);
		if (newTagPage->m_bph.m_lsn < logLSN) {
#ifdef NTSE_VERIFY_EX
			assert(newTagPage->m_bph.m_lsn == newTagOldLSN);
#endif
			assert(hasRecord);
			newTagPage->insertIntoSlot(&rec, newTagSlot, srcRid);
			newTagFlag = getRecordPageFlag(newTagPage->m_pageBitmapFlag, newTagPage->m_freeSpaceSize, false);
			newTagPage->m_pageBitmapFlag = newTagFlag;
			newTagPage->m_bph.m_lsn = logLSN;
			session->markDirty(newTagPageHdl);
		}
		if (newTagBmpModified && !(snSameBmp && bitmapModified) && !(onSameBmp && oldTagBmpModified)) {
			redoVerifyBitmap(session, newTagPgN, newTagPage->m_pageBitmapFlag, logLSN);
		}
		verify_ex(vs.hpheavy, heavyVerify(newTagPage));
		session->releasePage(&newTagPageHdl);
	}

	if (soSameBmp && bitmapModified && oldTagBmpModified) {
		redoVerifySameBitmap(session, logLSN, srcPgN, srcFlag, oldTagPgN, oldTagFlag);
	}
	if (snSameBmp && bitmapModified && newTagBmpModified) {
		redoVerifySameBitmap(session, logLSN, srcPgN, srcFlag, newTagPgN, newTagFlag);
	}
	if (onSameBmp && oldTagBmpModified && newTagBmpModified) {
		redoVerifySameBitmap(session, logLSN, oldTagPgN, oldTagFlag, newTagPgN, newTagFlag);
	}
}

/**
 * 检验一个记录页面对应的Bitmap页面是否需要redo，如果需要，完成redo。
 *
 * @param session  会话
 * @param logLSN  日志的LSN
 * @param pageNum  页面号
 * @param pageFlag  页面标志
 */
void VariableLengthRecordHeap::redoVerifyBitmap(Session *session, u64 pageNum, u8 pageFlag, u64 logLSN) {
	nftrace(ts.recv, tout << session << pageNum << pageFlag << logLSN);
	u8 nullFlag;
	u64 bmpNum = getBmpNum(pageNum);
	BufferPageHandle *bmpHdl = GET_PAGE(session, m_heapFile, PAGE_HEAP, bmpNum, Exclusived, m_dboStats, NULL);
	BitmapPage *bmp = (BitmapPage *)bmpHdl->getPage();
	if (bmp->u.m_header.m_bph.m_lsn < logLSN) {
		bmp->setBits((int)(pageNum - bmpNum - 1), pageFlag, &nullFlag, (bmpNum == m_lastBmpNum) ? (uint)(m_maxPageNum - bmpNum) : BitmapPage::CAPACITY);
		bmp->u.m_header.m_bph.m_lsn = logLSN;
		session->markDirty(bmpHdl);
	}
	session->releasePage(&bmpHdl);
}


/**
 * 检验两个对应同一位图的页面的位图需要不需要重整。
 *
 * @param session  会话
 * @param logLSN  日志的LSN
 * @param pageNum1,pageNum2  两个页面号
 * @param pageFlag1,pageFlag2  两个页面的标志
 */
void VariableLengthRecordHeap::redoVerifySameBitmap(Session *session, u64 logLSN, u64 pageNum1, u8 pageFlag1, u64 pageNum2, u8 pageFlag2) {
	ftrace(ts.recv, tout << session << logLSN << pageNum1 << pageFlag1 << pageNum2 << pageFlag2);
	u8 nullFlag;
	u64 bmpNum = getBmpNum(pageNum1);
	assert(pageNum2 && bmpNum == getBmpNum(pageNum2));
	int idx1 = (int)(pageNum1 - bmpNum - 1);
	int idx2 = (int)(pageNum2 - bmpNum - 1);
	BufferPageHandle *bmpHdl = GET_PAGE(session, m_heapFile, PAGE_HEAP, bmpNum, Exclusived, m_dboStats, NULL);
	BitmapPage *bmp = (BitmapPage *)bmpHdl->getPage();
	if (bmp->u.m_header.m_bph.m_lsn < logLSN) {
		bmp->setBits(&nullFlag, (bmpNum == m_lastBmpNum) ? (uint)(m_maxPageNum - bmpNum) : BitmapPage::CAPACITY, idx1, pageFlag1, idx2, pageFlag2);
		bmp->u.m_header.m_bph.m_lsn = logLSN;
		session->markDirty(bmpHdl);
	}
	session->releasePage(&bmpHdl);
}


/**
 * @see DrsHeap::redoDelete
 */
void VariableLengthRecordHeap::redoDelete(Session *session, u64 logLSN, const byte *log, uint size) {
	u64 srcRid, tagRid, srcPgN, tagPgN;
	u16 srcSlot, tagSlot;
	bool bitmapModified, tagBitmapModified = false;
	u8 srcFlag, tagFlag;
	BufferPageHandle *srcPageHdl, *tagPageHdl;
	VLRHeapRecordPageInfo *srcPage, *tagPage;
	VLRHeader *srcHdr;
	bool sameBitmap = false;
#ifdef NTSE_VERIFY_EX
	u64 oldSrcLSN = 0, oldTagLSN = 0;
#endif

	tagPgN = 0;
	tagSlot = 0;
	srcFlag = tagFlag = 0;

#ifndef NTSE_VERIFY_EX
	parseDeleteLog(log, size, &srcRid, &bitmapModified, &tagRid, &tagBitmapModified);
#else
	parseDeleteLog(log, size, &srcRid, &bitmapModified, &tagRid, &tagBitmapModified, &oldSrcLSN, &oldTagLSN);
#endif
	ftrace(ts.recv, tout << session << logLSN << log << size);
	nftrace(ts.recv, tout << "srcRid:" << rid(srcRid) << ",bitmapModified:" << bitmapModified
		<< ",tagRid:" << rid(tagRid) << ",tagBitmapModified:" << tagBitmapModified);

	srcPgN = RID_GET_PAGE(srcRid);
	srcSlot = RID_GET_SLOT(srcRid);
	if (tagRid) {
 		tagPgN = RID_GET_PAGE(tagRid);
		tagSlot = RID_GET_SLOT(tagRid);
		if (tagBitmapModified && bitmapModified)
			sameBitmap = (getBmpNum(srcPgN) == getBmpNum(tagPgN));
	}

	srcPageHdl = GET_PAGE(session, m_heapFile, PAGE_HEAP, srcPgN, Exclusived, m_dboStats, NULL);
	srcPage = (VLRHeapRecordPageInfo *)srcPageHdl->getPage();

	verify_ex(vs.hpheavy, heavyVerify(srcPage));
	nftrace(ts.recv, tout << "Src pageLSN: " << srcPage->m_bph.m_lsn);
	if (srcPage->m_bph.m_lsn < logLSN) {
#ifdef NTSE_VERIFY_EX
		assert(srcPage->m_bph.m_lsn == oldSrcLSN);
#endif
		nftrace(ts.recv, tout << "Delete slot: " << rid(srcRid));
		srcHdr = srcPage->getVLRHeader(srcSlot);
		assert(srcHdr && !srcHdr->m_ifEmpty && !srcHdr->m_ifTarget);
		assert((srcHdr->m_ifLink && tagRid && tagRid == RID_READ(srcPage->getRecordData(srcHdr))) || (!srcHdr->m_ifLink && !tagRid));
		srcPage->deleteSlot(srcSlot);
		srcFlag = getRecordPageFlag(srcPage->m_pageBitmapFlag, srcPage->m_freeSpaceSize, true);
		srcPage->m_pageBitmapFlag = srcFlag;
		srcPage->m_bph.m_lsn = logLSN;
		session->markDirty(srcPageHdl);
	}
	if (bitmapModified && !sameBitmap) {
		redoVerifyBitmap(session, srcPgN, srcPage->m_pageBitmapFlag, logLSN);
	}
	verify_ex(vs.hpheavy, heavyVerify(srcPage));

	session->releasePage(&srcPageHdl);

	if (tagRid) {
		tagPageHdl = GET_PAGE(session, m_heapFile, PAGE_HEAP, tagPgN, Exclusived, m_dboStats, NULL);
		tagPage = (VLRHeapRecordPageInfo *)tagPageHdl->getPage();
		verify_ex(vs.hpheavy, heavyVerify(tagPage));
		nftrace(ts.recv, tout << "Target pageLSN: " << srcPage->m_bph.m_lsn);
		if (tagPage->m_bph.m_lsn < logLSN) {
#ifdef NTSE_VERIFY_EX
			assert(tagPage->m_bph.m_lsn == oldTagLSN);
#endif
			nftrace(ts.recv, tout << "Delete slot: " << rid(tagRid));
			tagPage->deleteSlot(tagSlot);
			tagFlag = getRecordPageFlag(tagPage->m_pageBitmapFlag, tagPage->m_freeSpaceSize, true);
			tagPage->m_pageBitmapFlag = tagFlag;
			tagPage->m_bph.m_lsn = logLSN;
			session->markDirty(tagPageHdl);
		}
		if (tagBitmapModified && !sameBitmap) {
			redoVerifyBitmap(session, tagPgN, tagPage->m_pageBitmapFlag, logLSN);
		}
		verify_ex(vs.hpheavy, heavyVerify(tagPage));
		session->releasePage(&tagPageHdl);
	}

	if (sameBitmap) {
		redoVerifySameBitmap(session, logLSN, srcPgN, srcFlag, tagPgN, tagFlag);
	}
}

/**
 * 遍历所有Bitmap页面，重构中央位图。
 *
 * @pre 所有其他redo都已经完成
 * @param session 会话对象
 */
void VariableLengthRecordHeap::redoCentralBitmap(Session *session) {
	BitmapPage *bmp;
	u16 page11Num;
	u8 flag;
	for (uint i = 0; i < m_bitmapNum; ++i) {
		BufferPageHandle *handle = GET_PAGE(session, m_heapFile, PAGE_HEAP, (i * (BitmapPage::CAPACITY + 1)) + m_centralBmpNum + 1, Shared, m_dboStats, NULL);
		bmp = (BitmapPage *)handle->getPage();
		page11Num = (u16)(((i == m_bitmapNum - 1) ? m_maxPageNum - m_lastBmpNum : BitmapPage::CAPACITY) - bmp->u.m_header.m_pageCount[0] - bmp->u.m_header.m_pageCount[1] - bmp->u.m_header.m_pageCount[2]);
		flag = (page11Num ? 8 : 0) + (bmp->u.m_header.m_pageCount[2] ? 4 : 0) + (bmp->u.m_header.m_pageCount[1] ? 2 : 0) + (bmp->u.m_header.m_pageCount[0] ? 1 : 0);
		m_cbm.setBitmap(i, flag);
		assert(m_cbm[i] == flag);
		session->releasePage(&handle);
	}
}

/**
 * @see DrsHeap::redoFinish
 */
void VariableLengthRecordHeap::redoFinish(Session *session) {
	ftrace(ts.recv, tout << session);
	redoCentralBitmap(session);
	DrsHeap::redoFinish(session);
}

/**
 * @see DrsHeap::isPageEmpty
 */
bool VariableLengthRecordHeap::isPageEmpty(Session *session, u64 pageNum) {
	if (pageNum <= m_centralBmpNum) return false; // 首页和中央位图页认为是非空
	if (pageIsBitmap(pageNum)) return true; // 位图页认为是空

	bool empty;
	BufferPageHandle *pageHdl = GET_PAGE(session, m_heapFile, PAGE_HEAP, pageNum, Shared, m_dboStats, NULL);
	VLRHeapRecordPageInfo *page = (VLRHeapRecordPageInfo *)pageHdl->getPage();
	empty = (page->m_recordNum == 0);
	session->releasePage(&pageHdl);
	return empty;
}

/* 记录页面操作 */

/**
 * 读取一条记录
 *
 * @pre               记录头不为空，而且记录并非是个link source（可以是个link target），record的m_data空间已经由上层分配好。
 * @post              record的m_size和m_data被设置，其他未动。
 * @param recHdr      记录头
 * @param record OUT  需要读取数据的记录。
 */
void VLRHeapRecordPageInfo::readRecord(VLRHeader *recHdr, Record *record) {
	assert(recHdr && !recHdr->m_ifEmpty && !recHdr->m_ifLink);
	record->m_size = recHdr->m_size - ((recHdr->m_ifTarget) ? LINK_SIZE : 0);
	record->m_format = recHdr->m_ifCompressed > 0 ? REC_COMPRESSED : REC_VARLEN;
	memcpy(record->m_data, getRecordData(recHdr), record->m_size);
}

/**
 * 读取一条记录的某些属性
 *
 * @pre                       记录头不为空，而且记录并非是个link source（可以是个link target）
 * @post                      record的m_size和m_data被设置，其他未动。
 * @param recHdr              记录头
 * @param extractor           子记录提取器
 * @param subRecord OUT       需要读取的属性集合
 */
void VLRHeapRecordPageInfo::readSubRecord(VLRHeader *recHdr, SubrecExtractor *extractor, SubRecord *subRecord) {
	Record record;
	constructRecord(recHdr, &record);
	extractor->extract(&record, subRecord);
}

/**
 * 迅速构造一个只读的Record，不拷贝任何数据
 * @param recHdr        记录头
 * @param record OUT    记录指针
 */
void VLRHeapRecordPageInfo::constructRecord(VLRHeader *recHdr, Record *record) {
	assert(recHdr && !recHdr->m_ifEmpty && !recHdr->m_ifLink);
	record->m_format = recHdr->m_ifCompressed > 0 ? REC_COMPRESSED : REC_VARLEN;
	record->m_rowId = INVALID_ROW_ID;
	record->m_size = recHdr->m_size - ((recHdr->m_ifTarget) ? LINK_SIZE : 0);
	record->m_data = getRecordData(recHdr);
}




/**
 * 位图设置函数
 * @param idx              下标
 * @param flag             页面标志，2个bit的
 * @param bitSet OUT       更新后本位图在中央位图重的位标志
 * @param mapSize          一个bitmap中所表示的大小
 * @return                 当前位图的中央位图标志是否修改
 */
bool BitmapPage::setBits(int idx, u8 flag, u8 *bitSet, uint mapSize) {
	assert((flag & 3) == flag);
	u.m_header.m_pageCount[3] = (u16)(mapSize - u.m_header.m_pageCount[0] - u.m_header.m_pageCount[1] - u.m_header.m_pageCount[2]);
	bool havepage[4];
	for (int i = 0; i < 4; ++i)
		havepage[i] = u.m_header.m_pageCount[i] ? true : false;
	--u.m_header.m_pageCount[(*this)[idx]];
	++u.m_header.m_pageCount[flag];
	m_map[idx >> 2] = (byte)((m_map[idx >> 2] & ~(byte)(3 << ((idx % 4) << 1)))
		| (flag << ((idx % 4) << 1)));
	*bitSet = (u.m_header.m_pageCount[0] ? 1 : 0) + (u.m_header.m_pageCount[1] ? 2 : 0)
		+ (u.m_header.m_pageCount[2] ? 4 : 0) + (u.m_header.m_pageCount[3] ? 8 : 0);
	return havepage[0] ^ (u.m_header.m_pageCount[0] ? true : false)
		|| havepage[1] ^ (u.m_header.m_pageCount[1] ? true : false)
		|| havepage[2] ^ (u.m_header.m_pageCount[2] ? true : false)
		|| havepage[3] ^ (u.m_header.m_pageCount[3] ? true : false);
}

/**
 * 位图设置函数，同时设置两个页面的flag标志
 * @param idx1,idx2      索引
 * @param flag1,flag2    页面标志，2个bit的
 * @param bitSet OUT     更新后本位图在中央位图重的位标志
 * @param mapSize        一个bitmap中所表示的大小
 * @return               当前位图的中央位图标志是否修改
 */
bool BitmapPage::setBits(u8 *bitSet, uint mapSize, int idx1, u8 flag1, int idx2, u8 flag2) {
	assert((flag1 & 3) == flag1 && (flag2 & 3) == flag2);
	u.m_header.m_pageCount[3] = (u16)(mapSize - u.m_header.m_pageCount[0] - u.m_header.m_pageCount[1] - u.m_header.m_pageCount[2]);
	bool havepage[4];
	for (int i = 0; i < 4; ++i)
		havepage[i] = u.m_header.m_pageCount[i] ? true : false;
	--u.m_header.m_pageCount[(*this)[idx1]];
	++u.m_header.m_pageCount[flag1];
	--u.m_header.m_pageCount[(*this)[idx2]];
	++u.m_header.m_pageCount[flag2];
	m_map[idx1 >> 2] = (byte)((m_map[idx1 >> 2] & ~(byte)(3 << ((idx1 % 4) << 1)))
		| (flag1 << ((idx1 % 4) << 1)));
	m_map[idx2 >> 2] = (byte)((m_map[idx2 >> 2] & ~(byte)(3 << ((idx2 % 4) << 1)))
		| (flag2 << ((idx2 % 4) << 1)));
	*bitSet = (u.m_header.m_pageCount[0] ? 1 : 0) + (u.m_header.m_pageCount[1] ? 2 : 0)
		+ (u.m_header.m_pageCount[2] ? 4 : 0) + (u.m_header.m_pageCount[3] ? 8 : 0);
	return havepage[0] ^ (u.m_header.m_pageCount[0] ? true : false)
		|| havepage[1] ^ (u.m_header.m_pageCount[1] ? true : false)
		|| havepage[2] ^ (u.m_header.m_pageCount[2] ? true : false)
		|| havepage[3] ^ (u.m_header.m_pageCount[3] ? true : false);
}

/**
 * 初始化位图页
 */
void BitmapPage::init() {
	u.m_header.m_bph.m_lsn = 0;
	u.m_header.m_bph.m_checksum = BufferPageHdr::CHECKSUM_NO;
	memset(m_map, 0xFF, sizeof(m_map)); // map中所有bit全部置1。
	memset(u.m_header.m_pageCount, 0, sizeof(u.m_header.m_pageCount));
}


/**
 * 在位图中查找一个合适的含有空闲空间的页面，first fit
 * @param spaceFlag        01,10,11之一，00不可能
 * @param bitmapSize       该页管理的页面个数。
 * @param startPos         查找的起始位置
 * @return                 找到的组内pageNum，页号从0开始（0表示bitmap页面后第一页），-1表示未找到
 */
int BitmapPage::findFreeInBMP(u8 spaceFlag, uint bitmapSize, int startPos) {
	assert((byte *)this->m_map - (byte *)this == MAP_OFFSET);
	assert(spaceFlag == (spaceFlag & 0x3));
	if (0 == bitmapSize) return -1;
	int pos = startPos & (~31); // 起始下标, 保证整除32 (32*2 = 64bits)
	bool rewindSearch = (pos != 0); // startPos不是从0下标开始向后扫，所以如果没扫到，还要再把前面的扫一下。
	int endPos = bitmapSize; // 搜索起始中止位置。
	int unqualified = 0;
	static const u64 searchPat01 = 0x5555555555555555ULL; // 01扩展到8个byte
	static const u64 searchPat10 = 0xAAAAAAAAAAAAAAAAULL; // 10扩展到8个byte
	// 不会使用到 static const u64 searchPat11 = 0xFFFFFFFFFFFFFFFFULL; // 11扩展到8个byte
	u64 mapll;
	for (int i = 0; i < spaceFlag; ++i) {
		unqualified += u.m_header.m_pageCount[i];
	}
	if (unqualified >= (int)bitmapSize) return -1;
findFreeInBmp_search_backword:
	switch (spaceFlag) {
	case 0x1:
		for(; pos < endPos ; pos += 32) {
			mapll = *(u64 *)((m_map) + (pos >> 2));
			if ((mapll & searchPat01) || (mapll & searchPat10)) {
				goto validate_position_findFreeInBMP2;
			}
		}
		break;
	case 0x2:
		for(; pos < endPos ; pos += 32) {
			mapll = *(u64 *)((m_map) + (pos >> 2));
			if (mapll & searchPat10) {
				goto validate_position_findFreeInBMP2;
			}
		}
		break;
	case 0x3:
		for(; pos < endPos ; pos += 32) {
			mapll = *(u64 *)((m_map) + (pos >> 2));
			for (int i = 0; i < 32; ++i) {
				if (((mapll >> (i << 1)) & 0x3) == 0x3) {
					pos += i;
					goto page_found_findFreeInBMP2;
				}
			}
		}
		break;
	}
	if (rewindSearch) { // 折回从头查找
		SYNCHERE(SP_HEAP_VLR_FINDFREEINBMP_BEFORE_REWINDSEARCH);
findFreeInBMP_rewind:
		rewindSearch = false;
		endPos = startPos;
		pos = 0;
		goto findFreeInBmp_search_backword;
	} else {
		assert(false);
		return -1; // 搜完未找到。
	}
validate_position_findFreeInBMP2:
	for (int i = 0; i < 32; ++i) {
		if (((mapll >> (i << 1)) & 0x3) >= spaceFlag) {
			pos += i;
			break;
		}
	}
page_found_findFreeInBMP2:
	if (pos >= (int)bitmapSize) {
		if (rewindSearch) goto findFreeInBMP_rewind;
		else return -1;
	}
	else return pos;
}


/**
 * 找到第一个可能含有满足spaceFlag空间页面的位图页，查找不可以超过m_bitmapNum的位置。
 * @param spaceFlag  空间条件, 01, 10, 11之一，对应的位图必须满足xx1x, x1xx, 1xxx，
 *                   当然，空间多的可以满足小的空间要求，first fit而不是best fit，
 *					 查找到所需的页面的话，会更改相应的指针。
 * @param startPos   查找起点
 * @return           成功返回位图索引，失败返回-1，没有满足条件的位图页存在。
 */
int CentralBitmap::findFirstFitBitmap(u8 spaceFlag, int startPos) {
	assert((spaceFlag & 0x3) == spaceFlag && spaceFlag);
	if (m_vlrHeap->m_bitmapNum <= 0) return -1;
	int bitmapNum = (int)m_vlrHeap->m_bitmapNum;
	int endPos = bitmapNum;
	assert(endPos < (int)(Limits::PAGE_SIZE - MAP_OFFSET) * (int)m_vlrHeap->m_centralBmpNum);
	int pos = startPos & (~7);
	bool rewindSearch = (pos != 0);
	u64 searchPat; // 查找时每8个字节查找，速度比一个字节一个字节找要快
	searchPat = (0xF ^ ((1 << spaceFlag) - 1)) * 0x0101010101010101LL;
	int resultPos = -1;
	u64 *content;
findFirstFitBitmap_search:
	for(; pos < endPos; pos += 8) {
		content = (u64 *)((byte *)m_pageHdrArray[pos / (Limits::PAGE_SIZE - MAP_OFFSET)] + MAP_OFFSET + (pos % (Limits::PAGE_SIZE - MAP_OFFSET)));
		if (*content & searchPat) {
			for (int i = 0; i < 8; ++i) {
				if (*((u8 *)content + i) & (u8)(searchPat & 0xF)) {
					resultPos = pos + i;
					goto search_end2;
				}
			}
		}
	}
	if (rewindSearch) {
findFirstFitBitmap_rewind:
		rewindSearch = false;
		pos = 0;
		endPos = startPos;
		goto findFirstFitBitmap_search;
	}
	return -1;
search_end2:
	if (resultPos >= bitmapNum) { // 查找越界
		if (rewindSearch) goto findFirstFitBitmap_rewind;
		else return -1;
	}
	return resultPos;
}


/**
 * 设置中央位图中下标为idx的位图信息，相应改变lastBmpxx系列指针。
 * @param idx          下标
 * @param bitmapFlag   位图页标志，是4个bit的信息。
 */
void CentralBitmap::setBitmap(int idx, u8 bitmapFlag) {
	assert((bitmapFlag & 0xF) == bitmapFlag); // bitmapFlag只有前四位
	((u8 *)(m_pageHdrArray[idx / (Limits::PAGE_SIZE - MAP_OFFSET)]) + MAP_OFFSET)[idx % (Limits::PAGE_SIZE - MAP_OFFSET)] = bitmapFlag;
}


/**
 * 获得记录槽头信息
 * @param slotNum      记录槽号
 * @return             记录槽头指针，如果槽号有错，则为NULL，（为空槽时返回一个VLRHeader指针）
 */
VLRHeader* VLRHeapRecordPageInfo::getVLRHeader(uint slotNum) {
	if (!m_recordNum || this->m_lastHeaderPos - sizeof(VLRHeapRecordPageInfo) < slotNum * sizeof(VLRHeader))
		return NULL;
	return (VLRHeader *)(((byte *)this + sizeof(VLRHeapRecordPageInfo)) + sizeof(VLRHeader) * slotNum);
}




/**
 * 获得记录数据
 * @param vlrHeader    变长记录头
 * @return             记录数据指针
 */
byte* VLRHeapRecordPageInfo::getRecordData(VLRHeader *vlrHeader) {
	assert(!vlrHeader->m_ifEmpty);// && !vlrHeader->m_ifLink);
	int dataOff = (vlrHeader->m_ifTarget) ? LINK_SIZE : 0;
	return (byte *)this + vlrHeader->m_offset + dataOff;
}

/**
 * 整理记录页面，将空闲空间连续起来。
 */
void VLRHeapRecordPageInfo::defrag() {
	ftrace(ts.hp, );
	VLRHeader *vlrHdr, *tmpHdr;
	byte buf[Limits::PAGE_SIZE];

	memset(buf, 0, Limits::PAGE_SIZE);  // 这里为了比较方便，实际上不需要的。

	VLRHeapRecordPageInfo *tmpPage = (VLRHeapRecordPageInfo *)buf;
	tmpPage->m_freeSpaceTailPos = Limits::PAGE_SIZE;
	vlrHdr = (VLRHeader *)((byte *)this + sizeof(VLRHeapRecordPageInfo));
	tmpHdr = (VLRHeader *)(buf + sizeof(VLRHeapRecordPageInfo));
	for(uint i = 0; i <= (this->m_lastHeaderPos - sizeof(VLRHeapRecordPageInfo)) / sizeof(VLRHeader); ++i){
		(tmpHdr + i)->m_ifCompressed = (vlrHdr + i)->m_ifCompressed;
		(tmpHdr + i)->m_ifEmpty = (vlrHdr + i)->m_ifEmpty;
		if ((tmpHdr + i)->m_ifEmpty) continue;
		(tmpHdr + i)->m_ifLink = (vlrHdr + i)->m_ifLink;
		(tmpHdr + i)->m_ifTarget = (vlrHdr + i)->m_ifTarget;
		if ((vlrHdr + i)->m_offset == 0) {
			(tmpHdr + i)->m_offset = 0;
			continue;
		}
		(tmpHdr + i)->m_size = (vlrHdr + i)->m_size;
		tmpPage->m_freeSpaceTailPos = tmpPage->m_freeSpaceTailPos - (((tmpHdr + i)->m_size < LINK_SIZE)? LINK_SIZE : (tmpHdr + i)->m_size);
		memcpy(buf + tmpPage->m_freeSpaceTailPos, (byte *)this + (vlrHdr + i)->m_offset, (tmpHdr + i)->m_size);
		(tmpHdr + i)->m_offset = tmpPage->m_freeSpaceTailPos;
	}
	memcpy((byte *)this + sizeof(VLRHeapRecordPageInfo), buf + sizeof(VLRHeapRecordPageInfo), Limits::PAGE_SIZE - sizeof(VLRHeapRecordPageInfo));
	m_freeSpaceTailPos = tmpPage->m_freeSpaceTailPos;
}

/**
 * 插入一条记录到页面，改变页面的记录计数，剩余空间统计，有可能重整页面，但是不会更改m_pageBitmapFlag
 *
 * @pre               调用之前应该保证空间已经足够，本函数不做越界检查
 * @param record      要插入的记录
 * @param srcRid      如果要插入的是个链接的target，那么这里记录rid表示源的RowId，如果srcRid为0，表示不是一个target
 * @return            插入记录的记录槽号
 */
uint VLRHeapRecordPageInfo::insertRecord(const Record *record, RowId srcRid) {
	assert(NULL != record);
	VLRHeader *vlrHdr;
	uint slotNum;
	uint dataSize = record->m_size + ( srcRid ? LINK_SIZE : 0);
	if (dataSize < LINK_SIZE) dataSize = LINK_SIZE; // dataSize不算记录头的大小

	if (!m_recordNum) {
		vlrHdr = (VLRHeader *)((byte *)this + sizeof(VLRHeapRecordPageInfo));
		m_lastHeaderPos = sizeof(VLRHeapRecordPageInfo);
		slotNum = 0;
		m_freeSpaceSize -= sizeof(VLRHeader); // 记录头区会变大
	} else if (m_lastHeaderPos - sizeof(VLRHeapRecordPageInfo) == (m_recordNum -1) * sizeof(VLRHeader)) {
		// 说明记录头区没有空洞。
		if (m_freeSpaceTailPos - (m_lastHeaderPos + sizeof(VLRHeader)) < dataSize + sizeof(VLRHeader)) {
			// 连续空间不够，需要重整。
			defrag();
		}
		m_lastHeaderPos += sizeof(VLRHeader);
		vlrHdr = (VLRHeader *)((byte *)this + m_lastHeaderPos);
		slotNum = m_recordNum;
		m_freeSpaceSize -= sizeof(VLRHeader); //记录头区会变大
	} else {
		// 说明记录头区有空洞，记录头插入到空洞中。
		vlrHdr = (VLRHeader *)((byte *)this + sizeof(VLRHeapRecordPageInfo));
		for (slotNum = 0; !(vlrHdr + slotNum)->m_ifEmpty; ++slotNum) ;
		vlrHdr += slotNum;
		if (m_freeSpaceTailPos - (m_lastHeaderPos + sizeof(VLRHeader)) < dataSize) {
			defrag();
		}
		assert((byte *)vlrHdr < (byte *)this + m_lastHeaderPos);
	}

	// 拷贝数据
	m_freeSpaceTailPos = (u16)(m_freeSpaceTailPos - dataSize);
	vlrHdr->m_offset = m_freeSpaceTailPos;
	assert(record->m_format == REC_COMPRESSED || record->m_format == REC_VARLEN);
	vlrHdr->m_ifCompressed = record->m_format == REC_COMPRESSED ? 1 : 0;
	if (srcRid) {
		RID_WRITE(srcRid, (byte *)this + m_freeSpaceTailPos);
		assert(RID_READ((byte *)this + m_freeSpaceTailPos) == srcRid);
		memcpy((byte *)this + m_freeSpaceTailPos + LINK_SIZE, record->m_data, record->m_size);
		vlrHdr->m_size = record->m_size + LINK_SIZE;
		vlrHdr->m_ifTarget = 1;
	} else {
		memcpy((byte *)this + m_freeSpaceTailPos, record->m_data, record->m_size);
		vlrHdr->m_size = record->m_size; // 可能不到8字节，但是占用空间至少8字节，由dataSize保证。
		vlrHdr->m_ifTarget = 0;
	}
	vlrHdr->m_ifEmpty = 0;
	vlrHdr->m_ifLink = 0;

	/* 更新统计信息，m_freeSpaceTailPos和m_lastHeaderPos在前面已经更新过。
	 * m_freeSpaceSize在头区扩展的时候也已经改过，只需要算记录区占用的空间
     */
	++m_recordNum;
	m_freeSpaceSize = (u16)(m_freeSpaceSize - dataSize);
	return slotNum;
}

/**
 *  返回页面中第一个可用的slotNum
 * @return  可用的slot号
 * @past  可能这个slot不可用，因为空间不够的关系，本函数不做验证
 *
 */
uint VLRHeapRecordPageInfo::getEmptySlot() {
	assert(m_lastHeaderPos == 0 || m_lastHeaderPos >= sizeof(VLRHeapRecordPageInfo));
	uint slotNum;
	if (!m_recordNum) {
		return 0;
	}
	if (m_lastHeaderPos - sizeof(VLRHeapRecordPageInfo) == (m_recordNum -1) * sizeof(VLRHeader)) {
		// 说明记录头区没有空洞，可用的slot号在最后
		return m_recordNum;
	}
	assert(m_lastHeaderPos - sizeof(VLRHeapRecordPageInfo) > (m_recordNum - 1) * sizeof(VLRHeader));
	// 说明记录头区有空洞，记录头插入到空洞中。
	VLRHeader *vlrHdr = (VLRHeader *)((byte *)this + sizeof(VLRHeapRecordPageInfo));
	for (slotNum = 0; !(vlrHdr + slotNum)->m_ifEmpty; ++slotNum) ;
	assert(slotNum * sizeof(VLRHeader) < m_lastHeaderPos - sizeof(VLRHeapRecordPageInfo));
	return slotNum;
}

/**
 *  插入一条记录到指定记录槽
 *
 * @pre               空间足够，slotNum槽是空槽
 * @post              空间可能被重整
 * @param record      记录
 * @param slotNum     目的槽号，必须是空槽
 * @param srcRid      如果插入的是一个记录的target，那么这是source的RowId
 */
void VLRHeapRecordPageInfo::insertIntoSlot(const Record *record, u16 slotNum, RowId srcRid) {
	assert(record && record->m_data);
	assert(!getVLRHeader(slotNum) || getVLRHeader(slotNum)->m_ifEmpty);
	verify_ex(vs.hp, verifyPage());
	uint dataSize = (record->m_size < LINK_SIZE) ? LINK_SIZE : record->m_size;
	uint linkSize = srcRid ? LINK_SIZE : 0;
	VLRHeader *hdr = (VLRHeader *)((byte *)this + sizeof(VLRHeapRecordPageInfo) + slotNum * sizeof(VLRHeader));
	if (sizeof(VLRHeapRecordPageInfo) + slotNum * sizeof(VLRHeader) < m_lastHeaderPos) { // 记录头插到空隙中
		assert(m_freeSpaceSize >= (dataSize + linkSize));
		if (m_lastHeaderPos + sizeof(VLRHeader) + dataSize + linkSize > m_freeSpaceTailPos) {
			defrag();
		}
	} else { // 记录头放在头区尾部
		if (sizeof(VLRHeapRecordPageInfo) + slotNum * sizeof(VLRHeader) + sizeof(VLRHeader) + dataSize + linkSize > m_freeSpaceTailPos) {
			defrag();
		}
		if (slotNum == 0) {
			assert(!m_lastHeaderPos);
			m_lastHeaderPos = sizeof(VLRHeapRecordPageInfo);
			m_freeSpaceSize -= sizeof(VLRHeader);
		} else {
			if (!m_lastHeaderPos) {
				assert(!m_recordNum);
				m_lastHeaderPos = sizeof(VLRHeapRecordPageInfo) - sizeof(VLRHeader); // 方便计算
			}
			int slotHeaderSpaceUsed = sizeof(VLRHeapRecordPageInfo) + slotNum * sizeof(VLRHeader) - m_lastHeaderPos;
			for (u16 headerSpace = sizeof(VLRHeader); headerSpace < slotHeaderSpaceUsed; headerSpace += sizeof(VLRHeader)) {
				((VLRHeader *)((byte *)this + m_lastHeaderPos + headerSpace))->m_ifEmpty = 1;
			}
			m_lastHeaderPos = (u16)(m_lastHeaderPos + slotHeaderSpaceUsed);
			assert(m_freeSpaceSize >= dataSize + linkSize + slotHeaderSpaceUsed);
			m_freeSpaceSize = (u16)(m_freeSpaceSize - slotHeaderSpaceUsed);
		}
	}
	m_freeSpaceTailPos = (u16)(m_freeSpaceTailPos - dataSize - linkSize);
	assert(m_freeSpaceTailPos >= m_lastHeaderPos + sizeof(VLRHeader));
	hdr->m_ifEmpty = 0;
	hdr->m_ifLink = 0;
	hdr->m_offset = m_freeSpaceTailPos;
	assert(record->m_format == REC_COMPRESSED || record->m_format == REC_VARLEN);
	hdr->m_ifCompressed = record->m_format == REC_COMPRESSED ? 1 : 0;
	if (srcRid) {
		hdr->m_ifTarget = 1;
		assert(record->m_size > LINK_SIZE);
		hdr->m_size = LINK_SIZE + record->m_size;
		RID_WRITE(srcRid, (byte *)this + m_freeSpaceTailPos);
		memcpy((byte *)this + m_freeSpaceTailPos + LINK_SIZE, record->m_data, record->m_size);
		m_freeSpaceSize = (u16)(m_freeSpaceSize - record->m_size - LINK_SIZE);
	} else {
		hdr->m_ifTarget = 0;
		hdr->m_size = record->m_size;
		memcpy((byte *)this + m_freeSpaceTailPos, record->m_data, record->m_size);
		m_freeSpaceSize = (u16)(m_freeSpaceSize - dataSize);
	}
	++m_recordNum;
	verify_ex(vs.hp, verifyPage());
}

/**
 * 插入记录前空间是否足够
 * @param slotNum                预期插入的槽号
 * @param dataSizeWithHeader     包含记录头的记录大小
 * @return                       空间足够返回true
 */
bool VLRHeapRecordPageInfo::spaceIsEnough(uint slotNum, u16 dataSizeWithHeader) {
	assert(!getVLRHeader(slotNum) || getVLRHeader(slotNum)->m_ifEmpty);
	if (!m_recordNum) return true;

	uint slotPos = slotNum * sizeof(VLRHeader) + sizeof(VLRHeapRecordPageInfo);
	if (slotPos > m_lastHeaderPos) {
		return m_freeSpaceSize >= dataSizeWithHeader - sizeof(VLRHeader) + slotPos - m_lastHeaderPos;
	} else {
		return m_freeSpaceSize >= dataSizeWithHeader - sizeof(VLRHeader);
	}
}

#ifdef NTSE_VERIFY_EX
bool VLRHeapRecordPageInfo::verifyPage() {
	static const u16 headerSize = sizeof(VLRHeapRecordPageInfo);
	uint spaceUsed = 0;
	assert(
		(m_lastHeaderPos == 0 && m_recordNum == 0) ||
		(m_lastHeaderPos == headerSize && m_recordNum == 1) ||
		(m_lastHeaderPos > headerSize && m_recordNum <= (m_lastHeaderPos - headerSize) / sizeof(VLRHeader) + 1)
		);

	if (m_lastHeaderPos >= headerSize) {
		int nonEmpty = 0;
		VLRHeader *tmpHdr = (VLRHeader *)((byte *)this + headerSize);
		for (int i = 0; i <= (int)((m_lastHeaderPos - headerSize) / sizeof(VLRHeader)); ++i) {
			if (!(tmpHdr + i)->m_ifEmpty) {
				++nonEmpty;
				spaceUsed += ((tmpHdr + i)->m_size > LINK_SIZE) ? (tmpHdr + i)->m_size : LINK_SIZE;
			}
		}
		assert(m_recordNum == nonEmpty);
		assert(m_freeSpaceSize == Limits::PAGE_SIZE - m_lastHeaderPos - sizeof(VLRHeader) - spaceUsed);
	} else {
		assert(m_freeSpaceSize == Limits::PAGE_SIZE - headerSize);
	}
	return true;
}
#endif
/**
 * 删除一条记录（或者链接），清空记录槽，相应修改需要变动的页面统计数据
 * @param slotNum     记录槽号
 * @return            成功删除返回true，记录不存在返回false;
 */
bool VLRHeapRecordPageInfo::deleteSlot(uint slotNum) {
	verify_ex(vs.hp, verifyPage());
	VLRHeader *vlrHdr = getVLRHeader(slotNum);
	if (!vlrHdr || vlrHdr->m_ifEmpty) return false;
	vlrHdr->m_ifEmpty = 1;
	int i = 0; // 用来存放记录头区变小的数量
	if (m_lastHeaderPos == sizeof(VLRHeapRecordPageInfo) + slotNum * sizeof(VLRHeader)) { // 只有当记录头是最后一个记录头的时候才回向扫描
		for (; i <= (int)slotNum; ++i) { // 依次向前剔除空头
			if ((vlrHdr - i)->m_ifEmpty) {
				m_lastHeaderPos -= sizeof(VLRHeader);
			} else break;
		}
	}
	if (vlrHdr->m_offset == m_freeSpaceTailPos) {
		m_freeSpaceTailPos = m_freeSpaceTailPos + ((vlrHdr->m_size < LINK_SIZE) ? LINK_SIZE : vlrHdr->m_size); // 对于记录区的空洞不遍历记录头无法判断，而且因为总空闲空间有记录，这里不做处理。
	}
	if (--m_recordNum) {
		m_freeSpaceSize = (u16)(m_freeSpaceSize + ((vlrHdr->m_size < LINK_SIZE) ? LINK_SIZE : vlrHdr->m_size) + i * sizeof(VLRHeader));
	} else {
		// 是最后一条记录删除
		m_freeSpaceSize = Limits::PAGE_SIZE - sizeof(VLRHeapRecordPageInfo); // 可能会节省一次defrag
		m_lastHeaderPos = 0;
		m_freeSpaceTailPos = Limits::PAGE_SIZE;
	}
	verify_ex(vs.hp, verifyPage());
	return true;
}


/**
 * 将一个Slot做成link。
 *
 * @pre                slotNum对应的必须是个记录槽，非空，并且不能是target
 * @post               空间基数和指针都已经修改，但是页面位图并未修改。
 * @param tagRid       指向的目标RowId
 * @param slotNum      操作的slotNum
 * @return             操作成功返回true，条件不满足返回false
 */
bool VLRHeapRecordPageInfo::linklizeSlot(RowId tagRid, uint slotNum) {
	VLRHeader *vlrHdr = getVLRHeader(slotNum);
	assert(!(!vlrHdr || vlrHdr->m_ifEmpty || vlrHdr->m_ifTarget || vlrHdr->m_ifLink));
	vlrHdr->m_ifLink = 1;
	RID_WRITE(tagRid, (byte *)this + vlrHdr->m_offset);
	this->m_freeSpaceSize += (vlrHdr->m_size < LINK_SIZE) ? 0 : (vlrHdr->m_size - LINK_SIZE);
	vlrHdr->m_size = LINK_SIZE;
	return true;
}


/**
 * 在本页面内update一条记录
 * @pre                空间足够，不会有空间不足问题；记录存在，不会有传进空记录槽号的问题
 * @post               新的纪录内容被插到页面取代老记录，页面可能重整，记录头位置不变
 *                     如果被修改记录是个target，那么修改后的记录也是个target
 * @param record       记录指针，包含数据和长度信息
 * @param slotNum      更新的记录槽
 */
void VLRHeapRecordPageInfo::updateLocalRecord(Record *record, uint slotNum) {
	VLRHeader *oldHdr;
	RowId targetSrc = 0;
	oldHdr = (VLRHeader *)((byte *)this + sizeof(VLRHeapRecordPageInfo) + slotNum * sizeof(VLRHeader));
	if (oldHdr->m_ifLink) oldHdr->m_ifLink = 0; // 如果是link，就将之删掉
	// 调整页内空闲空间大小
	if (oldHdr->m_ifTarget) {
		targetSrc = RID_READ(getRecordData(oldHdr) - LINK_SIZE);
		m_freeSpaceSize = (u16)(m_freeSpaceSize - (LINK_SIZE + record->m_size - oldHdr->m_size));
	} else
		m_freeSpaceSize = (u16)(m_freeSpaceSize - ((record->m_size < LINK_SIZE) ? LINK_SIZE : record->m_size ) + (u16)((oldHdr->m_size < LINK_SIZE) ? LINK_SIZE : oldHdr->m_size));

	// 若记录更新后没有变大，存储在原地
	if ((record->m_size + (oldHdr->m_ifTarget ? LINK_SIZE : 0)) <= ((oldHdr->m_size < LINK_SIZE) ? LINK_SIZE : oldHdr->m_size)) { // 可以原地更新
		memcpy((byte *)this + oldHdr->m_offset + (oldHdr->m_ifTarget ? LINK_SIZE : 0), record->m_data, record->m_size);
		oldHdr->m_size = (u16)(record->m_size + (oldHdr->m_ifTarget ? LINK_SIZE : 0));
		assert(record->m_format == REC_COMPRESSED || record->m_format == REC_VARLEN);
		oldHdr->m_ifCompressed = record->m_format == REC_COMPRESSED ? 1 : 0;
		return;
	}
	if (m_freeSpaceTailPos == oldHdr->m_offset) {
		m_freeSpaceTailPos = m_freeSpaceTailPos + ((oldHdr->m_size < LINK_SIZE) ? LINK_SIZE : oldHdr->m_size);
	}
	// 如果记录在页头部的连续空闲空间中放不下则先重整页内碎片
	if (m_freeSpaceTailPos - m_lastHeaderPos - sizeof(VLRHeader) <
		(oldHdr->m_ifTarget ? (record->m_size + LINK_SIZE) : ((record->m_size < LINK_SIZE) ? LINK_SIZE : record->m_size))) {
		oldHdr->m_offset = 0;
		defrag();
	}
	if (oldHdr->m_ifTarget) {
		m_freeSpaceTailPos = (u16)(m_freeSpaceTailPos - (record->m_size + LINK_SIZE));
		RID_WRITE(targetSrc, (byte *)this + m_freeSpaceTailPos);
		memcpy((byte *)this + m_freeSpaceTailPos + LINK_SIZE, record->m_data, record->m_size);
		oldHdr->m_size = (u16)(LINK_SIZE + record->m_size);
	} else {
		m_freeSpaceTailPos = (u16)(m_freeSpaceTailPos - ((record->m_size < LINK_SIZE) ? LINK_SIZE : record->m_size));
		memcpy((byte *)this + m_freeSpaceTailPos, record->m_data, record->m_size);
		oldHdr->m_size = record->m_size;
	}
	oldHdr->m_offset = m_freeSpaceTailPos;
	assert(record->m_format == REC_COMPRESSED || record->m_format == REC_VARLEN);
	oldHdr->m_ifCompressed = record->m_format == REC_COMPRESSED ? 1 : 0;
}


#ifdef NTSE_UNIT_TEST
u8 VariableLengthRecordHeap::getPageFlag(Session *session, u64 pageNum) {
	BufferPageHandle *bph = GET_PAGE(session, m_heapFile, PAGE_HEAP, pageNum, Shared, m_dboStats, NULL);
	u8 flag = ((VLRHeapRecordPageInfo *)(bph->getPage()))->m_pageBitmapFlag;
	session->releasePage(&bph);
	return flag;
}

u16 VariableLengthRecordHeap::getRecordOffset(Session *session, RowId rid) {
	BufferPageHandle *pageHdl = GET_PAGE(session, m_heapFile, PAGE_HEAP, RID_GET_PAGE(rid), Shared, m_dboStats, NULL);
	VLRHeapRecordPageInfo *page = (VLRHeapRecordPageInfo *)pageHdl->getPage();
	VLRHeader *hdr = page->getVLRHeader(RID_GET_SLOT(rid));
	u16 offset;
	if (!hdr || hdr->m_ifEmpty) offset = 0;
	else offset = hdr->m_offset;
	session->releasePage(&pageHdl);
	return offset;
}

RowId VariableLengthRecordHeap::getTargetRowId(Session *session, RowId rid) {
	RowId linkrid;
	BufferPageHandle *pageHdl = GET_PAGE(session, m_heapFile, PAGE_HEAP, RID_GET_PAGE(rid), Shared, m_dboStats, NULL);
	VLRHeapRecordPageInfo *page = (VLRHeapRecordPageInfo *)pageHdl->getPage();
	u16 slotNum = RID_GET_SLOT(rid);
	VLRHeader *hdr = page->getVLRHeader(slotNum);
	if (!hdr || hdr->m_ifEmpty) {
		session->releasePage(&pageHdl);
		return 0;
	}
	if (hdr->m_ifLink) {
		linkrid = RID_READ(page->getRecordData(hdr));
	} else if (hdr->m_ifTarget) {
		linkrid = RID_READ(page->getRecordData(hdr) - LINK_SIZE);
	} else {
		linkrid = rid;
	}
	session->releasePage(&pageHdl);
	return linkrid;
}

u16 VariableLengthRecordHeap::getPageFreeSpace(Session *session, u64 pageNum) {
	BufferPageHandle *pageHdl = GET_PAGE(session, m_heapFile, PAGE_HEAP, pageNum, Shared, m_dboStats, NULL);
	VLRHeapRecordPageInfo *page = (VLRHeapRecordPageInfo *)pageHdl->getPage();
	u16 freeSpace = page->m_freeSpaceSize;
	session->releasePage(&pageHdl);
	return freeSpace;
}

#endif


/**
 * 判断一个页是否可以采样
 * @param pageNum     页面号
 * @return            可采样返回true
 */
bool VariableLengthRecordHeap::isSamplable(u64 pageNum) {
	return (pageNum > m_centralBmpNum && !isPageBmp(pageNum) && pageNum <= m_maxUsedPageNum);
}

/**
 * @see DrsHeap::unsamplablePagesBetween
 */
uint VariableLengthRecordHeap::unsamplablePagesBetween(u64 downPgn, u64 regionSize) {
	u64 upPgn = downPgn + regionSize - 1;
	uint bmpNum2down = (uint)((downPgn - metaDataPgCnt()) / (BitmapPage::CAPACITY + 1) + 1);
	uint bmpNum2up = (uint)((upPgn - metaDataPgCnt()) / (BitmapPage::CAPACITY + 1) + 1);
	return bmpNum2up - bmpNum2down + (isPageBmp(downPgn) ? 1 : 0);
}

/**
 * @see DrsHeap::sampleBufferPage
 */
Sample *VariableLengthRecordHeap::sampleBufferPage(Session *session, BufferPageHdr *page) {
	VLRHeapRecordPageInfo *vpage = (VLRHeapRecordPageInfo *)page;
	Sample *sample = Sample::create(session, VSF_MAX);//new Sample(VSF_MAX);
	for (int offset = sizeof(VLRHeapRecordPageInfo); offset <= vpage->m_lastHeaderPos; offset += sizeof(VLRHeader)) {
		VLRHeader *header = (VLRHeader *)((byte *)vpage + offset);
		if (!header->m_ifEmpty) {
			if (header->m_ifLink)
				(*sample)[VSF_NUMLINKS]++;
			else {
				(*sample)[VSF_NUMRECS]++;
				if (header->m_ifCompressed) {
					(*sample)[VSF_NUM_CPRSRECS]++;
					//取得记录内容并计算压缩比
					McSavepoint msp(session->getMemoryContext());
					byte *data = (byte *)session->getMemoryContext()->alloc(m_tableDef->m_maxRecSize);
					Record cprsRcd(INVALID_ROW_ID, REC_COMPRESSED, data, m_tableDef->m_maxRecSize);
					vpage->readRecord(header, &cprsRcd);
					(*sample)[VSF_CPRS_SIZE] += cprsRcd.m_size;
					assert(m_cprsRcdExtrator != NULL);
					(*sample)[VSF_UNCPRSSIZE] += (int)m_cprsRcdExtrator->calcRcdDecompressSize(&cprsRcd);
				}
			}
		}
	}
	(*sample)[VSF_FREEBYTES] = vpage->m_freeSpaceSize;
	return sample;
}

/**
 * @see DrsHeap::selectPage
 */
void VariableLengthRecordHeap::selectPage(u64 *outPages, int wantNum, u64 min, u64 regionSize) {
	assert(regionSize >= (u64)wantNum);
	set<u64> pnset;
	while (pnset.size() != (uint)wantNum) {
		u64 pn = min + (System::random() % regionSize);
		if (pnset.count(pn) || isPageBmp(pn))
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
 * @see DrsHeap::updateExtendStatus
 */
void VariableLengthRecordHeap::updateExtendStatus(Session *session, uint maxSamplePages) {
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

	u64 bmPageCnt = (m_maxUsedPageNum - metaDataPgCnt() + 1 - 1) / (BitmapPage::CAPACITY + 1) + 1;
	u64 recPageCnt = (m_maxUsedPageNum - metaDataPgCnt() + 1) - bmPageCnt;

	m_statusEx.m_numLinks = (u64)(result->m_fieldCalc[VSF_NUMLINKS].m_average * recPageCnt);
	m_statusEx.m_numRows = (u64)(result->m_fieldCalc[VSF_NUMRECS].m_average * recPageCnt);
	m_statusEx.m_pctUsed = 1. - (result->m_fieldCalc[VSF_FREEBYTES].m_average / (double)(Limits::PAGE_SIZE - sizeof(VLRHeapRecordPageInfo)));
	m_statusEx.m_numCmprsRows = (u64)(result->m_fieldCalc[VSF_NUM_CPRSRECS].m_average * recPageCnt);
	if (result->m_fieldCalc[VSF_UNCPRSSIZE].m_average == 0)
		m_statusEx.m_cmprsRatio = 1.0;
	else
		m_statusEx.m_cmprsRatio = (result->m_fieldCalc[VSF_CPRS_SIZE].m_average / (double)(result->m_fieldCalc[VSF_UNCPRSSIZE].m_average));

	delete result;
}

#ifdef NTSE_VERIFY_EX
void testAndSet(void *start, int count) {
	assert(count > 0);
	for (int i = 0; i < count; ++i) {
		assert(!*((byte *)start + i));
		*((byte *)start + i) = 1;
	}
}

/**
 * 对一个记录页面做耗时的完整性检验
 * @param page    页面
 * @return        无误返回true
 */
bool VariableLengthRecordHeap::heavyVerify(VLRHeapRecordPageInfo *page) {
	byte usedmap[Limits::PAGE_SIZE];
	memset(usedmap, 0, Limits::PAGE_SIZE);
	testAndSet(usedmap, sizeof(VLRHeapRecordPageInfo));

	static const u16 headerSize = sizeof(VLRHeapRecordPageInfo);

	assert(
		(page->m_lastHeaderPos == 0 && page->m_recordNum == 0) ||
		(page->m_lastHeaderPos == headerSize && page->m_recordNum == 1) ||
		(page->m_lastHeaderPos > headerSize && page->m_recordNum <= (page->m_lastHeaderPos - headerSize) / sizeof(VLRHeader) + 1)
		);

	uint spaceUsed = 0;
	if (page->m_lastHeaderPos >= headerSize) {
		int nonEmpty = 0;
		VLRHeader *tmpHdr = (VLRHeader *)((byte *)page + headerSize);
		for (int i = 0; i <= (int)((page->m_lastHeaderPos - headerSize) / sizeof(VLRHeader)); ++i) {
			testAndSet(usedmap + headerSize + i * sizeof(VLRHeader), sizeof(VLRHeader));
			if (!(tmpHdr + i)->m_ifEmpty) {
				++nonEmpty;
				uint recSize = ((tmpHdr + i)->m_size > LINK_SIZE) ? (tmpHdr + i)->m_size : LINK_SIZE;
				spaceUsed += recSize;
				testAndSet(usedmap + (tmpHdr+i)->m_offset, recSize);
			}
		}
		assert(page->m_recordNum == nonEmpty);
		assert(page->m_freeSpaceSize == Limits::PAGE_SIZE - page->m_lastHeaderPos - sizeof(VLRHeader) - spaceUsed);
	} else {
		assert(page->m_freeSpaceSize == Limits::PAGE_SIZE - headerSize);
	}

	return true;
}


#endif

} // namespace ntse {
