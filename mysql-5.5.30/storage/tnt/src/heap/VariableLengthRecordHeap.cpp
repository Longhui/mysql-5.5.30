/**
 * �䳤��¼��
 *
 * @author л��(ken@163.org)
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

/* �䳤�������� */
enum VLRHEAP_SAMPLE_FIELDS {
	VSF_NUMRECS,
	VSF_NUMLINKS,
	VSF_FREEBYTES,
	VSF_NUM_CPRSRECS,
	VSF_UNCPRSSIZE,
	VSF_CPRS_SIZE,

	VSF_MAX,
};

/** �䳤��ɨ��ʵ����Ҫ���ر����ݷ������� */
struct ScanHandleAddInfo {
	bool m_returnLinkSrc;           /** �Ƿ�װԴɨ�� */
	u64 m_nextBmpNum;               /** ��һ��λͼҳ��� */
};

/**
 * ���캯��
 *
 * @param db                   ���ݿ�ָ��
 * @param session              �Ự
 * @param heapFile             ��Ӧ�Ķ��ļ�ָ��
 * @param headerPage           ��ͷҳ
 * @param dbObjStats           ���ݶ���״̬
 * @throw NtseException        ��������ʧ��
 */
VariableLengthRecordHeap::VariableLengthRecordHeap(Database *db, Session *session, const TableDef *tableDef, File *heapFile,
												   BufferPageHdr *headerPage, DBObjStats *dbObjStats) throw(NtseException):
	DrsHeap(db, tableDef, heapFile, headerPage, dbObjStats), m_posLock("VLRHeap::posLock", __FILE__, __LINE__) {
	assert(db && session && heapFile && headerPage);
	ftrace(ts.hp, tout << session << heapFile << (HeapHeaderPageInfo *)headerPage;);
	m_version = HEAP_VERSION_VLR;

	m_dboStats = dbObjStats;

	/* �䳤��¼��ר������ */
	VLRHeapHeaderPageInfo *hdPage = (VLRHeapHeaderPageInfo *)headerPage;
	m_centralBmpNum = hdPage->m_centralBmpNum;
	assert(m_centralBmpNum > 0);
	/* ����ط�λͼ��Ŀ�����λͼҳ�� */
	if (m_maxPageNum > m_centralBmpNum) {
		m_bitmapNum = (m_maxPageNum - m_centralBmpNum - 1) / (BitmapPage::CAPACITY + 1) + 1;
		m_lastBmpNum = m_maxPageNum - (m_maxPageNum - m_centralBmpNum -1) % (BitmapPage::CAPACITY + 1);
	} else {
		m_bitmapNum = 0;
		m_lastBmpNum = 0; // �������ڣ�������Ҫ��
	}
	
	m_pctFree = m_tableDef->m_pctFree;
	m_reserveSize = (uint)(m_pctFree * (Limits::PAGE_SIZE - sizeof(VLRHeapRecordPageInfo)) / 100); //���㱣���ռ��С

	m_centralBitmapHdr = (BufferPageHdr **)(new byte[sizeof(BufferPageHdr *) * m_centralBmpNum]);
	/* ��ȡ����λͼҳ������pin���ڴ��� */
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

	/* �򿪼���cleanClose��Ϊfalse���Ͷ�����¼�Ѳ�ͬ��open������ȫֻ������ */
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
		// ������������ˢ��ȥ
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
		// ������ð�ȫ�رձ�־
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
 * ͬ��Ԫ����ҳ
 * @param session    �Ự
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
 * ��ȡ����λͼ��Bufferҳ
 * @param idx   ����λͼ��
 * @return      ����λͼBufferҳ
 */
BufferPageHdr * VariableLengthRecordHeap::getCBMPage(int idx) {
	return m_centralBitmapHdr[idx];
}

#endif

/**
 * ��ʼ���䳤��¼��
 * @param headerPage             ����ҳ
 * @param tableDef               ����
 * @param additionalPages OUT    ����λͼ��ҳ��
 * @param additionalPageNum OUT  ����λͼ��ҳ����
 * @throw NtseException          �����д�д�����
 */
void VariableLengthRecordHeap::initHeader(BufferPageHdr *headerPage, BufferPageHdr **additionalPages, uint *additionalPageNum) throw(NtseException) {
	assert(headerPage && additionalPages && additionalPageNum);
	assert(NULL == *additionalPages && 0 == *additionalPageNum);
	VLRHeapHeaderPageInfo *headerInfo = (VLRHeapHeaderPageInfo *)headerPage;
	headerInfo->m_hhpi.m_bph.m_lsn = 0;
	headerInfo->m_hhpi.m_bph.m_checksum = BufferPageHdr::CHECKSUM_NO;
	headerInfo->m_hhpi.m_version = HEAP_VERSION_VLR;
	headerInfo->m_centralBmpNum = DEFAULT_CBITMAP_SIZE;
	headerInfo->m_hhpi.m_pageNum = 0 + headerInfo->m_centralBmpNum; // �м�������λͼҳҲҪ����
	headerInfo->m_hhpi.m_maxUsed = headerInfo->m_hhpi.m_pageNum;
	headerInfo->m_cleanClosed = true; // ���δ���

	*additionalPages = (BufferPageHdr *)System::virtualAlloc(Limits::PAGE_SIZE * headerInfo->m_centralBmpNum);
	*additionalPageNum = headerInfo->m_centralBmpNum;
	memset(*additionalPages, 0x8, Limits::PAGE_SIZE * headerInfo->m_centralBmpNum); // λͼҳȫ����00001000����ʾһ��bitmapҳ��Ͻ��������11�Ŀռ䡣
	// ������λͼҳ��checksum����ΪCHECKSUM_DISABLED
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
		u64 pageNum = m_maxPageNum + i + 1; // maxPageNum��û�иı�
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
			recPage->m_lastHeaderPos = 0;  // 0��ʾû�����һ����¼ͷ
			recPage->m_freeSpaceTailPos = Limits::PAGE_SIZE;
			recPage->m_pageBitmapFlag = 3; // ������11
		}
		session->markDirty(pageHdl);
		session->releasePage(&pageHdl);
	}
	m_buffer->batchWrite(session, m_heapFile, PAGE_HEAP, m_maxPageNum + 1, m_maxPageNum + size);
}

/**
 * ��չ�ѵ���β�������޸�����λͼ
 *
 * @param extendSize  ����չ�Ĵ�С
 */
void VariableLengthRecordHeap::afterExtendHeap(uint extendSize) {
	assert(extendSize > 0);
	/* ��������λͼ��Ϣ */
	if (!pageIsBitmap(m_maxPageNum - extendSize + 1)) {
		// ���ݵĵ�һҳ����Bitmap��˵��ǰһ��Bitmap������11���͵�ҳ��
		m_cbm.setBitmap((int)(m_bitmapNum - 1), m_cbm[(int)(m_bitmapNum - 1)] | 0x8);
	}
	if (pageIsBitmap(m_maxPageNum)) {
		// ���ݵ����һ��ҳ��bitmap����Ͻ�²����κ�ҳ�档�޸�ʹ֮���ᱻ��������
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
 * Ѱ��һ�����п��пռ��ҳ��
 * @param session       �Ự
 * @param spaceRequest  ����Ŀռ��С������ж�ҳ���Ƿ�����ʱ���ǰ���С�����С�
 * @param pageNum OUT   ���ҳ���
 * @param lockMode      ��ģʽ��Ĭ������������
 * @param lastFalse     ��һ������lockModeΪNone����ʧ�ܵ�ҳ��ţ��������ⷴ�������õ�m_positionֵ���ظ�������
 * @return              �ҵ��Ŀ���ҳ�棬�������������NULL
 * @post                �ҵ��µĿ���ҳ�������m_position���ݣ��������lockModeΪNone���򲻸��£���Ϊ�޷�ȷ��ȷʵ�ǿ���ҳ�棩
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
	if (freePage->m_freeSpaceSize <= spaceWithReverse) { // �пռ䱣�����
		session->releasePage(&freePageHdl);
		freePage = NULL;
		goto findFreePage_search_bitmap; // ��*pageNum��ʼ���
	}
	if (newPos) {
		if (0 < (resultFlag = getRecordPageFlag(0, freePage->m_freeSpaceSize, false))) { //���ٱ�����01�����޸��ϴ�ָ��
			// �ҵ�һ�����߿��м���ҳ��ʱ������Ϳ��м���ҳ��Ҳ����һҳ��
			// Ҳ�������ǲ�����best fit
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
	if (maxPageNum != m_maxPageNum) { // �Ѿ��������߳���չ�˶ѣ������ٴ���չ
		unlockHeaderPage(session, &headerPageHdl);
		bmpIdx = lastBmpIdx;
		goto findFreePage_search_central_bmp;
	}
	// ��չ���ļ�
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
 * ����һ��λͼҳ��
 * @param session      ��Դ�Ự
 * @param groupNum     λͼҳ��������ţ�0��ʼ��ţ�Ҳ����������λͼ�е��±�
 * @param lockMode     ��ģʽ
 * @return             ҳ��handle
 */
BufferPageHandle* VariableLengthRecordHeap::getBitmap(Session *session, int groupNum, LockMode lockMode) {
	assert(session && groupNum >= 0);
	return GET_PAGE(session, m_heapFile, PAGE_HEAP, (groupNum * (BitmapPage::CAPACITY + 1)) + m_centralBmpNum + 1, lockMode, m_dboStats, NULL);
}

/**
 * ��������λͼҳ��
 * @param session          �Ự
 * @param bmpPgN1,bmpPgN2  ҳ���1��2
 * @param bmpHdl1,bmpHdl2  ҳ����
 * @param lockMode         ��ģʽ
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
 * ��ȡ��¼
 * @param session                   �Ự
 * @param rowId                     ��¼RowId
 * @param dest OUT                  ��¼����
 * @param destIsRecord              dest�����Ƿ���Record, false��ʾ��SubRecord
 * @param extractor                 ��¼��ȡ��
 * @param lockMode                  ҳ����ģʽ
 * @param rlh                       ����
 * @param duringRedo                �Ƿ�redo������
 * @return                          �ɹ���ȡ����true����¼�����ڷ���false
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
	if (recHdr->m_ifLink) { // ���Ӽ�¼
		u64 tagRid = RID_READ(recPage->getRecordData(recHdr));
		session->releasePage(&pageHdl);
		SYNCHERE(SP_HEAP_VLR_DOGET_AFTER_RELEASE_SOURCE_PAGE);
		gotTarget = doGetTarget(session, tagRid, rowId, dest, destIsRecord, extractor);
		if (!gotTarget) {
			// ����get����Ϊsource�϶��б仯��
			goto doGet_start_get_record_page;
		}
	} else {
		if (destIsRecord) {
			recPage->readRecord(recHdr, (Record *)dest);
			((Record *)dest)->m_rowId = rowId;
			assert(((Record *)dest)->m_size <= m_tableDef->m_maxRecSize);
			/* ����ͳ����Ϣ */
			++m_status.m_rowsReadRecord;
		} else {
			recPage->readSubRecord(recHdr, extractor, (SubRecord *)dest);
			((SubRecord *)dest)->m_rowId = rowId;
			/* ����ͳ����Ϣ */
			++m_status.m_rowsReadSubRec;
		}
		session->releasePage(&pageHdl);
	}
	assert(destIsRecord || ((SubRecord *)dest)->m_rowId == rowId);
	assert(!destIsRecord || (((Record *)dest)->m_size <= m_tableDef->m_maxRecSize && ((Record *)dest)->m_rowId == rowId));

	return true;
}

// ��¼����

/**
 * ִ��get������ʵ�壬��Ϊ���ص�����getֻ�ж�ȡ������ͬ��������ȫ��ͬ��
 * @param session          �Ự
 * @param rowId            Ŀ��RowId
 * @param dest OUT         Ŀ������������
 * @param destIsRecord     true��ʾdest��Record *��������һ��SubRecord *
 * @param extractor        �Ӽ�¼��ȡ����ֻ��destIsRecordΪfalseʱָ��
 * @return                 �ɹ���ȡ����true�����κμ�¼�����ڵ�ʧ�ܷ���false
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
		// ����Target���߲���Դ��Target
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
 * ��ȡһ��Target
 * @param session          ��Դ�Ự
 * @param rowId            Ŀ��RowId
 * @param srcRid           ԴRowId
 * @param outPut OUT       ��ȡ��¼����
 * @param extractor        �ּ�¼��ȡ��
 * @return                 �ɹ���ȡ����true�����κμ�¼�����ڵ�ʧ�ܷ���false
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

	// �ϲ���뱣֤dataSize���ᳬ�����ֵ
	assert(record->m_size <= MAX_VLR_LENGTH);
	uint dataSize = ((record->m_size < LINK_SIZE) ? LINK_SIZE : record->m_size) + sizeof(VLRHeader);
insert_find_free_page:
	freePageHdl = findFreePage(session, (u16)dataSize, &pageNum); // �쳣ֱ���׳���
	freePage = (VLRHeapRecordPageInfo *)freePageHdl->getPage();
	verify_ex(vs.hp, freePage->verifyPage());
insert_get_free_slot:
	slotNum = freePage->getEmptySlot();
	rowId = RID(pageNum, slotNum);

	// �����Ҫ����������
	if (rlh && !rowLocked) {
		SYNCHERE(SP_HEAP_VLR_INSERT_BEFORE_TRY_LOCK_ROW);
		*rlh = TRY_LOCK_ROW(session, m_tableDef->m_id, rowId, Exclusived);
		if (!*rlh) { // TRY LOCK���ɹ�
			SYNCHERE(SP_HEAP_VLR_INSERT_TRY_LOCK_ROW_FAILED);
			session->unlockPage(&freePageHdl);
			SYNCHERE(SP_HEAP_VLR_INSERT_UNLOCK_PAGE_AFTER_TRY_LOCK_ROW_FAILED);
			*rlh = LOCK_ROW(session, m_tableDef->m_id, rowId, Exclusived);
			LOCK_PAGE_HANDLE(session, freePageHdl, Exclusived);
			verify_ex(vs.hp, freePage->verifyPage());
			VLRHeader *vlrHdr = freePage->getVLRHeader(slotNum);
			if (!vlrHdr || vlrHdr->m_ifEmpty) { // �ü�¼�ۻ��ǿյ�
				// ��֤�ռ��㹻
				if (!freePage->spaceIsEnough(slotNum, (u16)dataSize)) {
					// �ռ��Ѿ�������
					session->unlockRow(rlh);
					session->releasePage(&freePageHdl);
					SYNCHERE(SP_HEAP_VLR_INSERT_SLOT_FREE_BUT_NO_SPACE);
					goto insert_find_free_page;
				}
			} else { // �����¼���Ѿ��ǿ���
				// �����ͷ�����
				session->unlockRow(rlh);
				// ��ҳ��Ŀռ乻����
				if (freePage->m_freeSpaceSize >= dataSize ||
					((freePage->m_freeSpaceSize >= dataSize - sizeof(VLRHeader)) &&
					(freePage->m_lastHeaderPos - sizeof(VLRHeapRecordPageInfo) > (freePage->m_recordNum -1) * sizeof(VLRHeader))) ) //���һ�б�����¼ͷ���п���
				{
					assert(!rowLocked);
					SYNCHERE(SP_HEAP_VLR_INSERT_BEFORE_REFIND_FREE_SLOT);
					goto insert_get_free_slot;
				}
				// ҳ��ռ䲻���ˣ�ֱ���ͷ�
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
						// �״�ʱ���ͷ�������Դ
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


#ifdef TNT_ENGINE   //�����С�ʹ������룬���ﲻ��Ҫ��¼InsertTNTLog��־��ֻ��ҪLob��־
	tnt::TNTTransaction *trx = session->getTrans();
	if (trx != NULL && m_tableDef->isTNTTable()) {
		assert(trx != NULL);
		if (!TableDef::tableIdIsVirtualLob(m_tableDef->m_id)) { //�˴���Ҫ�жϲ���С�ʹ����Ĳ��룬����ǣ�����ҪдTNT��־
			assert(INVALID_ROW_ID != rowId);
			writeInsertTNTLog(session, m_tableDef->m_id, trx->getTrxId(), trx->getTrxLastLsn(), rowId);
			//assert(INVALID_LSN != lsn);
		} else {//�����С�ʹ������룬����Ҫдһ������������־
			//С�ʹ����rowId����LobId
			LobStorage::writeTNTInsertLob(session,trx->getTrxId(), TableDef::getNormalTableId(m_tableDef->m_id), trx->getTrxLastLsn(), rowId);
		}
	}
#endif

	freePage->insertIntoSlot(record, (u16)slotNum);


	newFlag = getRecordPageFlag(freePage->m_pageBitmapFlag, freePage->m_freeSpaceSize, false);
	if (newFlag != freePage->m_pageBitmapFlag) { // ��Ҫ�޸�bitmap
		bmpHdl = GET_PAGE(session, m_heapFile, PAGE_HEAP, getBmpNum(pageNum), Exclusived, m_dboStats, NULL);
#ifndef NTSE_VERIFY_EX
		lsn = writeInsertLog(session, rowId, record, true);
#else
		lsn = writeInsertLog(session, rowId, record, true, freePage->m_bph.m_lsn);
#endif
		updateBitmap(session, pageNum, newFlag, lsn, bmpHdl);
		freePage->m_pageBitmapFlag = newFlag;
	} else { // ����Ҫ�޸�bitmap
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
 * ��¼insert��־
 *
 * @param session          �Ự
 * @param rid              ������¼��RowId
 * @param record           insert����ļ�¼
 * @param bitmapModified   �Ƿ��޸��˶�Ӧ��λͼҳ
 * @return                 ��־��lsn
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
 * ����INSERT��־
 * @param log  ��־����
 * @param logSize  ��־��С
 * @param rowId  insert������¼��RowId
 * @param bitmapModified  �����Ƿ�ı���λͼҳ
 * @param record OUT  ��¼�������ĳ��Ⱥ����ݷŵ�����
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
 * ���ݿ��пռ�freeSpaceSize������Ӧ��spaceFlag
 * @param oldFlag               �ɵ�ҳ���־
 * @param freeSpaceSize         ���пռ��С
 * @param freeSpaceIncrease     ���пռ������ӻ��Ǽ���
 * @return                      ��¼ҳ��λͼ
 */
u8 VariableLengthRecordHeap::getRecordPageFlag(u8 oldFlag, u16 freeSpaceSize, bool freeSpaceIncrease) {
	if (freeSpaceIncrease) { // ���пռ����ӣ�����delete����
		if (freeSpaceSize > FLAG11_SPACE_DOWN_LIMIT || (oldFlag == 0x3 && freeSpaceSize > FLAG10_SPACE_LIMIT))
			return 0x3;
		if (freeSpaceSize > FLAG10_SPACE_DOWN_LIMIT || (oldFlag == 0x2 && freeSpaceSize > FLAG01_SPACE_LIMIT))
			return 0x2;
		if (freeSpaceSize > FLAG01_SPACE_DOWN_LIMIT || (oldFlag == 0x1 && freeSpaceSize > FLAG00_SPACE_LIMIT))
			return 0x1;
		return 0x0;
	}
	// ���пռ���٣�����insert����
	if (freeSpaceSize > FLAG10_SPACE_LIMIT)
		return 0x3; // 11
	if (freeSpaceSize > FLAG01_SPACE_LIMIT)
		return 0x2; // 10
	if (freeSpaceSize > FLAG00_SPACE_LIMIT)
		return 0x1; // 01
	return 0x0; // 00
}

/**
 * ���ݿ��пռ�����õ���Ӧ��flagҪ��
 * @param spaceRequest          �ռ������С����λbyte
 * @return                      ���пռ��־λ, 11,10,01֮һ
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
 * ���������λͼ
 * @param session              �Ự
 * @param recPageNum           ������Ӧ�ļ�¼ҳ��ţ�Ҳ������Ҫ���µ�λͼҳ��Ͻ��ҳ�档
 * @param newFlag              ��¼ҳ������Ӧ����λͼ��־
 * @param lsn                  �ϴβ�����lsn��Ϊ0��ʾ������lsn��
 * @param bmpHdl               λͼ��ҳ����
 */
void VariableLengthRecordHeap::updateBitmap(Session *session, u64 recPageNum, u8 newFlag, u64 lsn, BufferPageHandle* bmpHdl) {
	assert(session && bmpHdl);
	BufferPageHandle *headerPageHdl = NULL;
	BitmapPage *bmp;
	u8 newBmFlag; // λͼҳ������λͼ�е�flag��־
	u64 bmPageNum = getBmpNum(recPageNum);

	ftrace(ts.hp, tout << session << recPageNum << newFlag << (BitmapPage *)bmpHdl->getPage() << bmPageNum);

	bmp = (BitmapPage *)bmpHdl->getPage();
	if (bmPageNum == m_lastBmpNum) { // ���һ��λͼҳ����Ҫ��ס��ҳ���Է�ֹ�޸�λͼʱ������
		SYNCHERE(SP_HEAP_VLR_UPDATEBITMAP_BEFORE_LOCK_HEADER_PAGE);
		headerPageHdl = lockHeaderPage(session, Exclusived);
		if (bmPageNum == m_lastBmpNum) { // �ٴ�ȷ��
			if (bmp->setBits((int)(recPageNum - bmPageNum - 1), newFlag, &newBmFlag, (uint)(m_maxPageNum - m_lastBmpNum))) {
				// ��Ҫ�޸�����λͼ��
				m_cbm.setBitmap((int)((bmPageNum - 1 - m_centralBmpNum) / (BitmapPage::CAPACITY + 1)), newBmFlag);
			}
		} else { // bitmap�Ѿ��������һҳ�����õ����ٴ�����
			session->unlockPage(&headerPageHdl);
			goto updateBitmap_setBits_safely;
		}
		session->unlockPage(&headerPageHdl);
	} else { // �������һ��λͼҳ�����õ������ݲ�������Ӱ��
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
 * ���������λͼ
 * @param session                      �Ự
 * @param lsn                          �ϴβ�����lsn��Ϊ0��ʾ������lsn��
 * @param recPageNum1, recPageNum2     ������Ӧ�ļ�¼ҳ��ţ�Ҳ������Ҫ���µ�λͼҳ��Ͻ��ҳ��
 * @param newFlag1, newFlag2           ��¼ҳ������Ӧ����λͼ��־
 * @param bmpHdl                       λͼҳ���
 */
void VariableLengthRecordHeap::updateSameBitmap(Session *session, u64 lsn, u64 recPageNum1, u8 newFlag1, u64 recPageNum2, u8 newFlag2, BufferPageHandle* bmpHdl) {
	BufferPageHandle *headerPageHdl = NULL;
	BitmapPage *bmp;
	u8 newBmFlag; // λͼҳ������λͼ�е�flag��־
	u64 bmPageNum = getBmpNum(recPageNum1);
	assert(recPageNum2 && bmPageNum == getBmpNum(recPageNum2));

	int idx1 = (int)(recPageNum1 - bmPageNum - 1);
	int idx2 = (int)(recPageNum2 - bmPageNum - 1);

	bmp = (BitmapPage *)bmpHdl->getPage();

	ftrace(ts.hp, tout << session << recPageNum1 << newFlag1 << recPageNum2 << newFlag2 <<  bmPageNum << bmp);

	if (bmPageNum == m_lastBmpNum) { // ���һ��λͼҳ����Ҫ��ס��ҳ���Է�ֹ�޸�λͼʱ������
		SYNCHERE(SP_HEAP_VLR_UPDATESAMEBMP_BEFORE_LOCK_HEADER_PAGE);
		headerPageHdl = lockHeaderPage(session, Exclusived);
		if (bmPageNum == m_lastBmpNum) { // �ٴ�ȷ��
			if (bmp->setBits(&newBmFlag, (uint)(m_maxPageNum - m_lastBmpNum), idx1, newFlag1, idx2, newFlag2)) {
				// ��Ҫ�޸�����λͼ��
				m_cbm.setBitmap((int)((bmPageNum - 1 - m_centralBmpNum) / (BitmapPage::CAPACITY + 1)), newBmFlag);
			}
		} else { // bitmap�Ѿ��������һҳ�����õ����ٴ�����
			session->unlockPage(&headerPageHdl);
			goto updateBitmap_setBits_safely;
		}
		session->unlockPage(&headerPageHdl);
	} else { // �������һ��λͼҳ�����õ������ݲ�������Ӱ��
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
	if (!recHdr || recHdr->m_ifEmpty || recHdr->m_ifTarget) { // ������Ϊ�գ�������Ϊlink target��
		session->releasePage(&recPageHdl);
		return false;
	}
	if (!recHdr->m_ifLink) { // ����link�����漰��ҳ��
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
	} else { // ��ҳ�����
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
		// �ϲ�ͨ�������������֤��¼���ᱻ�޸�
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
 * ��¼del��־
 *
 * @param session              �Ự
 * @param rid                  ������¼��RowId
 * @param bitmapModified       �Ƿ��޸��˶�Ӧ��λͼҳ
 * @param tagRid               �����ɾ���ļ�¼�Ǹ�link����ô�����¼��target��RowId
 * @param tagBitmapModified    target��Ӧ��λͼ�Ƿ��޸�
 * @return                     ��־��lsn
 */
#ifndef NTSE_VERIFY_EX
u64 VariableLengthRecordHeap::writeDeleteLog(Session *session, RowId rid, bool bitmapModified, RowId tagRid, bool tagBitmapModified) {
#else
u64 VariableLengthRecordHeap::writeDeleteLog(Session *session, RowId rid, bool bitmapModified, RowId tagRid, bool tagBitmapModified, u64 oldSrcLSN, u64 oldTagLSN) {
	ftrace(ts.hp, tout << session << rid << bitmapModified << tagRid << tagBitmapModified);
#endif
	/* ��tagRidΪ0ʱ˵�����漰���ҳ��Ĳ��������һ�����ü�¼�� */
	assert(session);
#ifndef NTSE_VERIFY_EX
	byte logData[32]; // ����
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
 * ����DELETE��־
 * @param log                   ��־����
 * @param logSize               ��־��С
 * @param rowId                 insert������¼��RowId
 * @param bitmapModified        �����Ƿ�ı���λͼҳ
 * @param tagRid                �����targetҳ�棬��target��RowId
 * @param tagBitmapModified     �Ƿ��޸���targetҳ��
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

	/* ����ͳ����Ϣ */
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

	/* ����ͳ����Ϣ */
	if (success) {
		++m_status.m_rowsUpdateRecord;
	}

	return success;
}

/**
 * ʵ�ʽ���update�����ĺ���
 * @post                 ����������SubRecord����ô�����ڲ���װ��һ��Record����log��ʱ�����������¼Record�������ⶪʧ��
 * @param session        �Ự
 * @param rowId          RowId
 * @param subRecord      ���¼�¼�����Ӽ�
 * @param record         ���¼�¼
 * @return               ���³ɹ���ɷ���true
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
	byte data[Limits::PAGE_SIZE - sizeof(VLRHeapRecordPageInfo)]; // ��󳤶�
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
	// ȡ��Ŀ��ҳ������check
	srcPageHdl = GET_PAGE(session, m_heapFile, PAGE_HEAP, srcPageNum, Exclusived, m_dboStats, NULL);
	srcPage = (VLRHeapRecordPageInfo *)srcPageHdl->getPage();
	verify_ex(vs.hp, srcPage->verifyPage());
	srcHdr = srcPage->getVLRHeader(srcSlot);
	if (!srcHdr || srcHdr->m_ifEmpty || srcHdr->m_ifTarget) {
		session->releasePage(&srcPageHdl);
		return false;
	}
	if (!srcHdr->m_ifLink) { // Ŀ���¼����һ��link
		assert(!srcHdr->m_ifTarget);
		oldRec.m_data = srcPage->getRecordData(srcHdr);
		oldRec.m_size = srcHdr->m_size;
		oldRec.m_format = srcHdr->m_ifCompressed > 0 ? REC_COMPRESSED : REC_VARLEN;
		// �����¼�¼���Ȳ�����ֱ�ӱ��ظ����Ż�
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
		if ((int)newRec.m_size <= srcPage->m_freeSpaceSize + ((srcHdr->m_size < LINK_SIZE) ? LINK_SIZE : srcHdr->m_size)) { // ���Գ���defrag��
			sizeInc = (((s16)srcHdr->m_size - (s16)newRec.m_size) > 0);
			srcPage->updateLocalRecord(&newRec, srcSlot);
			srcFlag = getRecordPageFlag(srcPage->m_pageBitmapFlag, srcPage->m_freeSpaceSize, sizeInc);
			goto update_only_source_page_need_update;
		}
		// ��ҳ�޷����£���ҪѰ���µ�ҳ��
		requestSpace = (u16)(newRec.m_size + sizeof(VLRHeader) + LINK_SIZE);
update_find_free_page_to_insert_target:
		findFreePage(session, requestSpace, &newTagPgN, None, lastFalsePgN); // ��ȡ��ҳ��ţ�������
		if (newTagPgN == srcPageNum) {
			lastFalsePgN = srcPageNum;
			goto update_find_free_page_to_insert_target;
		}
		reLockTwoPages = lockSecondPage(session, srcPageNum, newTagPgN, &srcPageHdl, &newTagPageHdl);
		if (reLockTwoPages) {
			// ����ǰ�����Ź���������Դҳ�����ǲ������㹻�Ŀռ���
			// ���ڼ�¼һ��������������Ҫ��֤��¼�Ƿ����˸���
			assert(srcHdr && !srcHdr->m_ifEmpty && !srcHdr->m_ifTarget);
			assert(!srcHdr->m_ifLink);
			if (srcPage->m_freeSpaceSize >= newRec.m_size - ((srcHdr->m_size < LINK_SIZE) ? LINK_SIZE : srcHdr->m_size)) { // �ռ�ͻȻ�ֹ���
				session->releasePage(&newTagPageHdl);
				srcPage->updateLocalRecord(&newRec, srcSlot);
				srcFlag = getRecordPageFlag(srcPage->m_pageBitmapFlag, srcPage->m_freeSpaceSize, false);
				goto update_only_source_page_need_update;
			}
		}
		// ����õ��ֵ���ҳ��
		newTagPage = (VLRHeapRecordPageInfo *)newTagPageHdl->getPage();
		verify_ex(vs.hp, newTagPage->verifyPage());
		if (newTagPage->m_freeSpaceSize < newRec.m_size + sizeof(VLRHeader) + LINK_SIZE) { // ҳ��ռ��Ѿ���������������
			session->releasePage(&newTagPageHdl);
			lastFalsePgN = newTagPgN;
			goto update_find_free_page_to_insert_target;
		}
		// ����ҳ���Ѿ����֣�����֮��
		goto update_source_link_to_new_target;
	} else { // Ŀ���¼��һ��link
		oldTagRowId = RID_READ(srcPage->getRecordData(srcHdr));
		oldTagPgN = RID_GET_PAGE(oldTagRowId);
		oldTagSlot = RID_GET_SLOT(oldTagRowId);
		// ȡ��targetҳ��
		assert(srcPageNum != oldTagPgN);
		reLockTwoPages = lockSecondPage(session, srcPageNum, oldTagPgN, &srcPageHdl, &oldTagPageHdl);
		verify_ex(vs.hp, !reLockTwoPages || srcPage->verifyPage());
#ifdef NTSE_UNIT_TEST
		if (reLockTwoPages) { // ���������������Ź��������»�ȡԭҳ��
			assert(srcHdr && !srcHdr->m_ifEmpty && !srcHdr->m_ifTarget);
			assert(srcHdr->m_ifLink);
			assert(RID_READ(srcPage->getRecordData(srcHdr)) == oldTagRowId);
		}
#endif
		// ����ȡ����source��target����ҳ��
		oldTagPage = (VLRHeapRecordPageInfo *)oldTagPageHdl->getPage();
		verify_ex(vs.hp, oldTagPage->verifyPage());
		oldTagHdr = oldTagPage->getVLRHeader(oldTagSlot);
		assert(oldTagHdr->m_ifTarget);
		oldRec.m_size = oldTagHdr->m_size - LINK_SIZE;
		oldRec.m_data = oldTagPage->getRecordData(oldTagHdr);
		oldRec.m_format = oldTagHdr->m_ifCompressed > 0 ? REC_COMPRESSED : REC_VARLEN;
		if (!newRecReady) { // �����¼�¼��������ֱ�ӱ��ظ����Ż�
			if (m_cprsRcdExtrator != NULL) {
				RecordOper::updateRcdWithDic(session->getMemoryContext(), m_tableDef, m_cprsRcdExtrator, &oldRec, subRecord, &newRec);
			} else {
				newRec.m_size = RecordOper::getUpdateSizeVR(m_tableDef, &oldRec, subRecord);
				RecordOper::updateRecordVR(m_tableDef, &oldRec, subRecord, newRec.m_data);
			}
			assert(newRec.m_size <= m_tableDef->m_maxRecSize);
			newRecReady = true;
		}

		// ����sourceҳ���ڸ���
		if (srcPage->m_freeSpaceSize + LINK_SIZE >= (int)newRec.m_size) {
			goto update_local_update_in_source_page;
		}
		// ����old targetҳ���ڸ���
		if ((uint)oldTagPage->m_freeSpaceSize + oldTagHdr->m_size >= LINK_SIZE + newRec.m_size) {
			goto update_local_update_in_old_target_page;
		}
		// �޷���source��old targetҳ��֮�������⣬�ռ䶼������
		requestSpace = (u16)(newRec.m_size + sizeof(VLRHeader) + LINK_SIZE);
update_look_for_third_page:
		findFreePage(session, requestSpace, &newTagPgN, None, lastFalsePgN);
		if (newTagPgN == oldTagPgN || newTagPgN == srcPageNum) {
			lastFalsePgN = newTagPgN;
			goto update_look_for_third_page;
		}
		// ͬʱȡ������ҳ��
		if (srcPageNum < oldTagPgN) {
			if (oldTagPgN < newTagPgN) { // ������
				goto update_new_target_page_number_is_biggest;
			} else {
				if (srcPageNum < newTagPgN) { // ˵��srcPageNum < newTagPgN < oldTagPgN
					reLockTwoPages = lockSecondPage(session, oldTagPgN, newTagPgN, &oldTagPageHdl, &newTagPageHdl);
					if (reLockTwoPages) { // ��Ҫ������֤old target����Ϊsource pageû���ͷţ�����old target slot�Ϸ��Բ�������⣬�����ռ乻����
						verify_ex(vs.hp, oldTagPage->verifyPage());
						if ((uint)oldTagPage->m_freeSpaceSize + oldTagHdr->m_size >= LINK_SIZE + newRec.m_size) { // �ռ乻
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
				} else { // ˵��newTagPgN < srcPageNum < oldTagPgN��������
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
		} else { // ˵�� oldTagPgN < srcPageNum
			if (srcPageNum < newTagPgN) { // ������
				goto update_new_target_page_number_is_biggest;
			} else { // oldTagPgN < srcPageNum && newTagPgN < srcPageNum
				if (oldTagPgN < newTagPgN) { // oldTagPgN < newTagPgN < srcPageNum
					reLockTwoPages = lockSecondPage(session, srcPageNum, newTagPgN, &srcPageHdl, &newTagPageHdl);
					if (reLockTwoPages) {  // ��Ҫ��֤��ҳ��old targetҳ��δ�ſ������Լ�¼�����ܱ����¡�ֻ��Ҫ��ҳ��ռ��Ƿ��㹻
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
		// ͬʱȡ��������ҳ��
	} // if (!srcHdr->m_ifLink)

	// ��¼ԭ�����Ǹ����Ӽ�¼�����Ҹ��º���ԭҳ��ŵ���
update_only_source_page_need_update:
	/* @pre  �Ѿ����¹�ҳ�棬ȡ�����µ�srcFlag */
	/* �����������־ֻ���¼SubRecord���ɱ�֤redo */
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

	// ��¼ԭ�����Ǹ����Ӽ�¼�����º���ԭҳ��Ų����ˣ��ŵ���ҳ��
update_source_link_to_new_target:
	/* @pre  �Ѿ�ȡ������ҳ�棬������֤�������� */
	/* ��������±����¼Record���ܱ�֤redo */
	newTagSlot = newTagPage->insertRecord(&newRec, rowId);
	newTagRowId = RID(newTagPgN, newTagSlot);
	newTagFlag = getRecordPageFlag(newTagPage->m_pageBitmapFlag, newTagPage->m_freeSpaceSize, false);
	srcPage->linklizeSlot(newTagRowId, srcSlot);
	srcFlag = getRecordPageFlag(srcPage->m_pageBitmapFlag, srcPage->m_freeSpaceSize, true);
	// ����λͼ��¼��־
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

	// ��¼ԭ����һ�����ӣ����º���ԭҳ����Դ洢�£�ȡ������
update_local_update_in_source_page:
	/* @pre �Ѿ���sourceҳ���old targetҳ�棬newRec׼���ã���sourceҳ��ռ��㹻���¼�¼ʹ�� */
	/* ��������±����¼Record���ܱ�֤redo */
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

	// ��¼ԭ����һ�����ӣ����º��Ǵ洢��ԭ����Ŀ��ҳ
update_local_update_in_old_target_page:
	/* @pre  �Ѿ���sourceҳ���old targetҳ�棬newRec׼���ã���old targetҳ��ռ��㹻���¼�¼ʹ�� */
	/* �����������־ֻ���¼SubRecord���ɱ�֤redo */
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

	// ��¼ԭ����һ�����ӣ����º�Ҫ�洢����һ��ҳ���Ҹ�ҳҳ�ű�ԭҳ��ԭ����Ŀ��ҳ����
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
	/* @pre ����������ҳ���handle���������������ɿ�����Ҫ��֤ */
	// ��֤source
	assert(srcHdr && !srcHdr->m_ifEmpty && !srcHdr->m_ifTarget);
	assert(srcHdr->m_ifLink);
	assert(RID_READ(srcPage->getRecordData(srcHdr)) == oldTagRowId);
	if (srcPage->m_freeSpaceSize + LINK_SIZE >= (int)newRec.m_size) {
		session->releasePage(&newTagPageHdl);
		goto update_local_update_in_source_page;
	}
	// ��֤old target
	if ((uint)oldTagPage->m_freeSpaceSize + oldTagHdr->m_size >= newRec.m_size + LINK_SIZE) {
		session->releasePage(&newTagPageHdl);
		goto update_local_update_in_old_target_page;
	}
	// ��֤new target page
	newTagPage = (VLRHeapRecordPageInfo *)newTagPageHdl->getPage();
	if (newTagPage->m_freeSpaceSize < newRec.m_size + LINK_SIZE + sizeof(VLRHeader)) {
		session->releasePage(&newTagPageHdl);
		lastFalsePgN = newTagPgN;
		goto update_look_for_third_page;
	}
update_have_three_pages_validated:
	/* ������source��old targetҳ�涼�޷����¼�¼������ŵ�new targetҳ����*/
	/* @pre ������Ϣ���Ѿ���֤�����ɿ����� */
	/* ��������±����¼����Record���ܱ�֤redo */
	oldTagPage->deleteSlot(oldTagSlot);
	newTagSlot = newTagPage->insertRecord(&newRec, rowId);
	newTagRowId = RID(newTagPgN, newTagSlot);
	RID_WRITE(newTagRowId, srcPage->getRecordData(srcHdr));
	/* Դҳ�����пռ�仯 */
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
 * ��¼update��־
 *
 * @param session                 �Ự
 * @param rid                     ������¼��RowId
 * @param record                  ���µ�record�������Ҫ�Ļ���������NULLֵ��
 * @param bitmapModified          �Ƿ��޸��˶�Ӧ��λͼҳ
 * @param hasNewTarget            �Ƿ����µ�targetҳ��
 * @param hasOldTarget            ��update�ļ�¼�Ƿ���target
 * @param updateInOldTag          �Ƿ��������targetҳ����
 * @param newTagRowId             �����new target����ô����RowId
 * @param newTagBmpModified       �����new target����ô�Ƿ��޸��˶�Ӧ��λͼҳ
 * @param oldTagRowId             �����old target����ôRowId
 * @param oldTagBmpModified       �����old target����ô�Ƿ��޸��˶�Ӧ��λͼҳ
 * @return                        ��־��lsn
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
		// ��¼��ҳ�޸�
		s.write(bitmapModified)->write(hasNewTarget)->write(hasOldTarget)->write(updateInOldTag);
		if (hasNewTarget) s.write(newTagRowId)->write(newTagBmpModified);
		if (hasOldTarget) s.write(oldTagRowId)->write(oldTagBmpModified);
		if (record) {
			s.write(true); //��¼record
			s.write(record->m_format == REC_COMPRESSED);//��¼�Ƿ���ѹ����ʽ
			s.write(record->m_size)->write(record->m_data, record->m_size);
		} else {
			s.write(false); //����¼record
		}
	} catch (NtseException &) {
		assert(false);
	}
	return session->writeLog(LOG_HEAP_UPDATE, m_tableDef->m_id, logDate, s.getSize());
}

/**
 * ����UPDATE��־
 * @param log                     ��־
 * @param logSize                 ��־��С
 * @param RowId OUT               ������¼��RowId
 * @param bitmapModified OUT      �Ƿ��޸��˶�Ӧ��λͼҳ
 * @param hasNewTarget OUT        �Ƿ����µ�targetҳ��
 * @param hasOldTarget OUT        ��update�ļ�¼�Ƿ���target
 * @param updateInOldTag OUT      �Ƿ��������targetҳ����
 * @param newTagRowId OUT         �����new target����ô����RowId
 * @param newTagBmpModified OUT   �����new target����ô�Ƿ��޸��˶�Ӧ��λͼҳ
 * @param oldTagRowId OUT         �����old target����ôRowId
 * @param oldTagBmpModified OUT   �����old target����ô�Ƿ��޸��˶�Ӧ��λͼҳ
 * @param hasRecord               ��������¼
 * @param record OUT              ������ڣ�����log�м�¼��Record����
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
 * ��������ҳ����������������һ��ҳ��
 * @pre                         ��һҳ�����Ѿ�����
 * @post                        ��Ϊ���ܻ���������������Ա�����Ӧ��֤��ҳ�������Ƿ���Ȼ��Ч��
 *                              ������ҳ�ļ�¼ҳ�棬��������������Щ��
 * @param session               �Ự
 * @param firstPageNum          �Ѿ���������ҳ���pageId
 * @param secondPageNum         ��Ҫ�����ĵڶ�ҳ���pageId
 * @param firstPageHdl OUT      ��һҳ��ľ��ָ���ָ��
 * @param secondPageHdl OUT     �ڶ�ҳ��ľ��ָ�루����ʱӦ����δ��ֵ�������󴫳�������Ч�����
 * @param lockMode              ������ģʽ
 * @return                      ���������������һҳ����������������true��û�з���ֱ�������ɹ�������false;
 */
bool VariableLengthRecordHeap::lockSecondPage(Session *session, u64 firstPageNum, u64 secondPageNum, BufferPageHandle **firstPageHdl,
											  BufferPageHandle **secondPageHdl, LockMode lockMode) {
	assert(secondPageNum != firstPageNum);
	assert(session && firstPageHdl && secondPageHdl);
	if (secondPageNum > firstPageNum) { // ˳�����
		*secondPageHdl = GET_PAGE(session, m_heapFile, PAGE_HEAP, secondPageNum, lockMode, m_dboStats, NULL);
		return false;
	}
	//���ȳ���try lock
	SYNCHERE(SP_HEAP_VLR_LOCKSECONDPAGE_BEFORE_TRYLOCK);
	*secondPageHdl = TRY_GET_PAGE(session, m_heapFile, PAGE_HEAP, secondPageNum, lockMode, m_dboStats);
	if (*secondPageHdl) { //�����ɹ�
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
 * ����ҳ����С��ҳ��
 *
 * @pre                    ҳ������Ѿ��źã�fPgNum > sPgNum > tPgNum
 * @post                   ����ҳ�水˳��������ԭ����ҳ��ָ�������Ҫ��֤
 * @param session          ��Դ�Ự
 * @param fPgNum           ҳ�����ĵ�һҳ��
 * @param sPgNum           ҳ�ŵڶ���ĵڶ�ҳ��
 * @param tPgNum           ����ҳ�棬ҳ����С
 * @param fPageHdl OUT     ��һ��ҳ��ľ�����Ѿ�ȡ��
 * @param sPageHdl OUT     �ڶ���ҳ��ľ�����Ѿ�ȡ��
 * @param tPageHdl OUT     ������ҳ��ľ������δȡ��
 * @return                 ���ͷ�����������true�����򷵻�false
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


/* ��ɨ�� */

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

	/* ����link����ɨ�跽ʽ��һ������linkֻɨtarget����������target��������linkʱȥȡ��target
	   ��һ�ַ����죬����ɨ��ʱ�����update��update�������ҳ���У����¼�ᱻ���ɨ�裻
	   �ڶ��ַ�����ȫ������ɨ���ٶ�������Ϊ��ʱ��ʱȥ��ȡlink��ָ���targetҳ�档
	   �����ַ����ɴ���DrsHeapScanHandleʱ��returnLinkSr��������
	*/
	assert(scanHandle && subRec);
	assert(subRec->m_format == REC_REDUNDANT);
	RowId rowId = scanHandle->getNextPos();
	bool& returnLinkSrc = ((ScanHandleAddInfo *)scanHandle->getOtherInfo())->m_returnLinkSrc;
	u64& nextBmpNum = ((ScanHandleAddInfo *)scanHandle->getOtherInfo())->m_nextBmpNum;
	u64 pageNum = RID_GET_PAGE(rowId);//, dataPgN;
	u16 slotNum = RID_GET_SLOT(rowId); // ����һ��slot��ʼɨ
	RowId sourceRid, targetRid, posRid;
	Session *session = scanHandle->getSession();
	BufferPageHandle *pageHdl = scanHandle->getPage(); // ������NULL
	BufferPageHandle *targetPageHdl = NULL;
	VLRHeapRecordPageInfo *targetPg;
	VLRHeapRecordPageInfo *page;
	VLRHeader *recHdr;
	RowLockHandle *rowLockHdl = NULL;

	// �ҵ���һ������ҳ��
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
		if (!recHdr) { // ��ҳû����������
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
		// ����sourceID��lastPos
		if (recHdr->m_ifTarget) {// �϶�returnLinkSrc��false
			assert(!returnLinkSrc);
			sourceRid = RID_READ((byte *)page + recHdr->m_offset);
		} else {
			sourceRid = posRid;
		}
		scanHandle->setNextPos(RID(pageNum, (slotNum + 1))); // ��һ��λ�ú���
		// ����ȡ������
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
				// ��֤
				recHdr = page->getVLRHeader(slotNum);
				if (!recHdr || recHdr->m_ifEmpty
					|| (returnLinkSrc && recHdr->m_ifTarget) || (!returnLinkSrc && recHdr->m_ifLink)) {
					// ��ǰ��¼��ɾ�������ɲ�����Ҫ����������ʱ��������ǰ��¼
					slotNum++;
					session->unlockRow(&rowLockHdl);
					goto getNext_get_header;
				}
				if ((recHdr->m_ifTarget && RID_READ((byte *)page + recHdr->m_offset) != sourceRid) // ��Ȼ�Ǹ�����Ŀ�ģ������ӵ�Դ�Ѿ�����
					|| (sourceRid != posRid && !recHdr->m_ifTarget)) { // ��ǰλ����ǰ�Ǹ�����Ŀ�ģ������Ѿ�������
					session->unlockRow(&rowLockHdl);
					goto getNext_get_header;
				}
			}
		} //ȡ��������
		if (returnLinkSrc && recHdr->m_ifLink) { // ��source��ȡ�����Ҽ�¼ͷ�Ǹ�link source
			targetRid = RID_READ(page->getRecordData(recHdr));
			if (rowLockHdl) {
				session->unlockPage(&pageHdl);
				SYNCHERE(SP_HEAP_VLR_GETNEXT_BEFORE_EXTRACTTARGET);
				extractTarget(session, targetRid, sourceRid, subRec, scanHandle->getExtractor());
			} else {
				// δ������
				SYNCHERE(SP_HEAP_VLR_GETNEXT_BEFORE_LOCK_SECOND_PAGE);
				if (lockSecondPage(session, pageNum, RID_GET_PAGE(targetRid), &pageHdl, &targetPageHdl, Shared)) {
					if (!recHdr || recHdr->m_ifEmpty || !recHdr->m_ifLink
						|| RID_READ(page->getRecordData(recHdr)) != targetRid) {
							// ��¼�Ѿ�ʧЧ�����Ǽ���ȡ��һ��
							session->releasePage(&targetPageHdl);
							goto getNext_get_header; // slotNum��+1����Ϊ����ɨ�跽ʽ�ǰ�target
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
		} else { // ��ҳ��ȡ
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


/*** redo���� ***/
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

	// �ȴ���source page
	srcPageHdl = GET_PAGE(session, m_heapFile, PAGE_HEAP, srcPgN, Exclusived, m_dboStats, NULL);
	srcPage = (VLRHeapRecordPageInfo *)srcPageHdl->getPage();
	verify_ex(vs.hpheavy, heavyVerify(srcPage));
	nftrace(ts.recv, tout << "Src pageLSN: " << srcPage->m_bph.m_lsn);
	if (srcPage->m_bph.m_lsn < logLSN) {
#ifdef NTSE_VERIFY_EX
		assert(srcPage->m_bph.m_lsn == srcOldLSN);
#endif
		srcHdr = srcPage->getVLRHeader(srcSlot);
		if (hasOldTarget) { // ��¼����link��
			assert(srcHdr->m_ifLink);
			if (hasNewTarget) { // ����Ŀ��
				assert(hasRecord);
				RID_WRITE(newTagRid, srcPage->getRecordData(srcHdr));
			} else if (!updateInOldTag) {
				assert(hasRecord);
				srcPage->updateLocalRecord(&rec, srcSlot);
				srcFlag = getRecordPageFlag(srcPage->m_pageBitmapFlag, srcPage->m_freeSpaceSize, false);
				srcPage->m_pageBitmapFlag = srcFlag;
			} else if (updateInOldTag) { // ���µ����أ�����δȡ��
				/* do nothing. */
			}
		} else { // ��¼����link
			assert(!hasOldTarget && !srcHdr->m_ifLink);
			int diffsize = (srcHdr->m_size > LINK_SIZE) ? srcHdr->m_size : LINK_SIZE;
			if (hasNewTarget) { // ԭ�����ؼ�¼���µ���
				assert(hasRecord);
				srcPage->linklizeSlot(newTagRid, srcSlot);
				diffsize -= LINK_SIZE;
			} else { // Դҳ���ظ���
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

	// ����old targetҳ��
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
			int sizediff = oldTagHdr->m_size; // �϶�����LINK_SIZE
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

	// ����new targetҳ��
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
 * ����һ����¼ҳ���Ӧ��Bitmapҳ���Ƿ���Ҫredo�������Ҫ�����redo��
 *
 * @param session  �Ự
 * @param logLSN  ��־��LSN
 * @param pageNum  ҳ���
 * @param pageFlag  ҳ���־
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
 * ����������Ӧͬһλͼ��ҳ���λͼ��Ҫ����Ҫ������
 *
 * @param session  �Ự
 * @param logLSN  ��־��LSN
 * @param pageNum1,pageNum2  ����ҳ���
 * @param pageFlag1,pageFlag2  ����ҳ��ı�־
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
 * ��������Bitmapҳ�棬�ع�����λͼ��
 *
 * @pre ��������redo���Ѿ����
 * @param session �Ự����
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
	if (pageNum <= m_centralBmpNum) return false; // ��ҳ������λͼҳ��Ϊ�Ƿǿ�
	if (pageIsBitmap(pageNum)) return true; // λͼҳ��Ϊ�ǿ�

	bool empty;
	BufferPageHandle *pageHdl = GET_PAGE(session, m_heapFile, PAGE_HEAP, pageNum, Shared, m_dboStats, NULL);
	VLRHeapRecordPageInfo *page = (VLRHeapRecordPageInfo *)pageHdl->getPage();
	empty = (page->m_recordNum == 0);
	session->releasePage(&pageHdl);
	return empty;
}

/* ��¼ҳ����� */

/**
 * ��ȡһ����¼
 *
 * @pre               ��¼ͷ��Ϊ�գ����Ҽ�¼�����Ǹ�link source�������Ǹ�link target����record��m_data�ռ��Ѿ����ϲ����á�
 * @post              record��m_size��m_data�����ã�����δ����
 * @param recHdr      ��¼ͷ
 * @param record OUT  ��Ҫ��ȡ���ݵļ�¼��
 */
void VLRHeapRecordPageInfo::readRecord(VLRHeader *recHdr, Record *record) {
	assert(recHdr && !recHdr->m_ifEmpty && !recHdr->m_ifLink);
	record->m_size = recHdr->m_size - ((recHdr->m_ifTarget) ? LINK_SIZE : 0);
	record->m_format = recHdr->m_ifCompressed > 0 ? REC_COMPRESSED : REC_VARLEN;
	memcpy(record->m_data, getRecordData(recHdr), record->m_size);
}

/**
 * ��ȡһ����¼��ĳЩ����
 *
 * @pre                       ��¼ͷ��Ϊ�գ����Ҽ�¼�����Ǹ�link source�������Ǹ�link target��
 * @post                      record��m_size��m_data�����ã�����δ����
 * @param recHdr              ��¼ͷ
 * @param extractor           �Ӽ�¼��ȡ��
 * @param subRecord OUT       ��Ҫ��ȡ�����Լ���
 */
void VLRHeapRecordPageInfo::readSubRecord(VLRHeader *recHdr, SubrecExtractor *extractor, SubRecord *subRecord) {
	Record record;
	constructRecord(recHdr, &record);
	extractor->extract(&record, subRecord);
}

/**
 * Ѹ�ٹ���һ��ֻ����Record���������κ�����
 * @param recHdr        ��¼ͷ
 * @param record OUT    ��¼ָ��
 */
void VLRHeapRecordPageInfo::constructRecord(VLRHeader *recHdr, Record *record) {
	assert(recHdr && !recHdr->m_ifEmpty && !recHdr->m_ifLink);
	record->m_format = recHdr->m_ifCompressed > 0 ? REC_COMPRESSED : REC_VARLEN;
	record->m_rowId = INVALID_ROW_ID;
	record->m_size = recHdr->m_size - ((recHdr->m_ifTarget) ? LINK_SIZE : 0);
	record->m_data = getRecordData(recHdr);
}




/**
 * λͼ���ú���
 * @param idx              �±�
 * @param flag             ҳ���־��2��bit��
 * @param bitSet OUT       ���º�λͼ������λͼ�ص�λ��־
 * @param mapSize          һ��bitmap������ʾ�Ĵ�С
 * @return                 ��ǰλͼ������λͼ��־�Ƿ��޸�
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
 * λͼ���ú�����ͬʱ��������ҳ���flag��־
 * @param idx1,idx2      ����
 * @param flag1,flag2    ҳ���־��2��bit��
 * @param bitSet OUT     ���º�λͼ������λͼ�ص�λ��־
 * @param mapSize        һ��bitmap������ʾ�Ĵ�С
 * @return               ��ǰλͼ������λͼ��־�Ƿ��޸�
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
 * ��ʼ��λͼҳ
 */
void BitmapPage::init() {
	u.m_header.m_bph.m_lsn = 0;
	u.m_header.m_bph.m_checksum = BufferPageHdr::CHECKSUM_NO;
	memset(m_map, 0xFF, sizeof(m_map)); // map������bitȫ����1��
	memset(u.m_header.m_pageCount, 0, sizeof(u.m_header.m_pageCount));
}


/**
 * ��λͼ�в���һ�����ʵĺ��п��пռ��ҳ�棬first fit
 * @param spaceFlag        01,10,11֮һ��00������
 * @param bitmapSize       ��ҳ�����ҳ�������
 * @param startPos         ���ҵ���ʼλ��
 * @return                 �ҵ�������pageNum��ҳ�Ŵ�0��ʼ��0��ʾbitmapҳ����һҳ����-1��ʾδ�ҵ�
 */
int BitmapPage::findFreeInBMP(u8 spaceFlag, uint bitmapSize, int startPos) {
	assert((byte *)this->m_map - (byte *)this == MAP_OFFSET);
	assert(spaceFlag == (spaceFlag & 0x3));
	if (0 == bitmapSize) return -1;
	int pos = startPos & (~31); // ��ʼ�±�, ��֤����32 (32*2 = 64bits)
	bool rewindSearch = (pos != 0); // startPos���Ǵ�0�±꿪ʼ���ɨ���������ûɨ������Ҫ�ٰ�ǰ���ɨһ�¡�
	int endPos = bitmapSize; // ������ʼ��ֹλ�á�
	int unqualified = 0;
	static const u64 searchPat01 = 0x5555555555555555ULL; // 01��չ��8��byte
	static const u64 searchPat10 = 0xAAAAAAAAAAAAAAAAULL; // 10��չ��8��byte
	// ����ʹ�õ� static const u64 searchPat11 = 0xFFFFFFFFFFFFFFFFULL; // 11��չ��8��byte
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
	if (rewindSearch) { // �ۻش�ͷ����
		SYNCHERE(SP_HEAP_VLR_FINDFREEINBMP_BEFORE_REWINDSEARCH);
findFreeInBMP_rewind:
		rewindSearch = false;
		endPos = startPos;
		pos = 0;
		goto findFreeInBmp_search_backword;
	} else {
		assert(false);
		return -1; // ����δ�ҵ���
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
 * �ҵ���һ�����ܺ�������spaceFlag�ռ�ҳ���λͼҳ�����Ҳ����Գ���m_bitmapNum��λ�á�
 * @param spaceFlag  �ռ�����, 01, 10, 11֮һ����Ӧ��λͼ��������xx1x, x1xx, 1xxx��
 *                   ��Ȼ���ռ��Ŀ�������С�Ŀռ�Ҫ��first fit������best fit��
 *					 ���ҵ������ҳ��Ļ����������Ӧ��ָ�롣
 * @param startPos   �������
 * @return           �ɹ�����λͼ������ʧ�ܷ���-1��û������������λͼҳ���ڡ�
 */
int CentralBitmap::findFirstFitBitmap(u8 spaceFlag, int startPos) {
	assert((spaceFlag & 0x3) == spaceFlag && spaceFlag);
	if (m_vlrHeap->m_bitmapNum <= 0) return -1;
	int bitmapNum = (int)m_vlrHeap->m_bitmapNum;
	int endPos = bitmapNum;
	assert(endPos < (int)(Limits::PAGE_SIZE - MAP_OFFSET) * (int)m_vlrHeap->m_centralBmpNum);
	int pos = startPos & (~7);
	bool rewindSearch = (pos != 0);
	u64 searchPat; // ����ʱÿ8���ֽڲ��ң��ٶȱ�һ���ֽ�һ���ֽ���Ҫ��
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
	if (resultPos >= bitmapNum) { // ����Խ��
		if (rewindSearch) goto findFirstFitBitmap_rewind;
		else return -1;
	}
	return resultPos;
}


/**
 * ��������λͼ���±�Ϊidx��λͼ��Ϣ����Ӧ�ı�lastBmpxxϵ��ָ�롣
 * @param idx          �±�
 * @param bitmapFlag   λͼҳ��־����4��bit����Ϣ��
 */
void CentralBitmap::setBitmap(int idx, u8 bitmapFlag) {
	assert((bitmapFlag & 0xF) == bitmapFlag); // bitmapFlagֻ��ǰ��λ
	((u8 *)(m_pageHdrArray[idx / (Limits::PAGE_SIZE - MAP_OFFSET)]) + MAP_OFFSET)[idx % (Limits::PAGE_SIZE - MAP_OFFSET)] = bitmapFlag;
}


/**
 * ��ü�¼��ͷ��Ϣ
 * @param slotNum      ��¼�ۺ�
 * @return             ��¼��ͷָ�룬����ۺ��д���ΪNULL����Ϊ�ղ�ʱ����һ��VLRHeaderָ�룩
 */
VLRHeader* VLRHeapRecordPageInfo::getVLRHeader(uint slotNum) {
	if (!m_recordNum || this->m_lastHeaderPos - sizeof(VLRHeapRecordPageInfo) < slotNum * sizeof(VLRHeader))
		return NULL;
	return (VLRHeader *)(((byte *)this + sizeof(VLRHeapRecordPageInfo)) + sizeof(VLRHeader) * slotNum);
}




/**
 * ��ü�¼����
 * @param vlrHeader    �䳤��¼ͷ
 * @return             ��¼����ָ��
 */
byte* VLRHeapRecordPageInfo::getRecordData(VLRHeader *vlrHeader) {
	assert(!vlrHeader->m_ifEmpty);// && !vlrHeader->m_ifLink);
	int dataOff = (vlrHeader->m_ifTarget) ? LINK_SIZE : 0;
	return (byte *)this + vlrHeader->m_offset + dataOff;
}

/**
 * �����¼ҳ�棬�����пռ�����������
 */
void VLRHeapRecordPageInfo::defrag() {
	ftrace(ts.hp, );
	VLRHeader *vlrHdr, *tmpHdr;
	byte buf[Limits::PAGE_SIZE];

	memset(buf, 0, Limits::PAGE_SIZE);  // ����Ϊ�˱ȽϷ��㣬ʵ���ϲ���Ҫ�ġ�

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
 * ����һ����¼��ҳ�棬�ı�ҳ��ļ�¼������ʣ��ռ�ͳ�ƣ��п�������ҳ�棬���ǲ������m_pageBitmapFlag
 *
 * @pre               ����֮ǰӦ�ñ�֤�ռ��Ѿ��㹻������������Խ����
 * @param record      Ҫ����ļ�¼
 * @param srcRid      ���Ҫ������Ǹ����ӵ�target����ô�����¼rid��ʾԴ��RowId�����srcRidΪ0����ʾ����һ��target
 * @return            �����¼�ļ�¼�ۺ�
 */
uint VLRHeapRecordPageInfo::insertRecord(const Record *record, RowId srcRid) {
	assert(NULL != record);
	VLRHeader *vlrHdr;
	uint slotNum;
	uint dataSize = record->m_size + ( srcRid ? LINK_SIZE : 0);
	if (dataSize < LINK_SIZE) dataSize = LINK_SIZE; // dataSize�����¼ͷ�Ĵ�С

	if (!m_recordNum) {
		vlrHdr = (VLRHeader *)((byte *)this + sizeof(VLRHeapRecordPageInfo));
		m_lastHeaderPos = sizeof(VLRHeapRecordPageInfo);
		slotNum = 0;
		m_freeSpaceSize -= sizeof(VLRHeader); // ��¼ͷ������
	} else if (m_lastHeaderPos - sizeof(VLRHeapRecordPageInfo) == (m_recordNum -1) * sizeof(VLRHeader)) {
		// ˵����¼ͷ��û�пն���
		if (m_freeSpaceTailPos - (m_lastHeaderPos + sizeof(VLRHeader)) < dataSize + sizeof(VLRHeader)) {
			// �����ռ䲻������Ҫ������
			defrag();
		}
		m_lastHeaderPos += sizeof(VLRHeader);
		vlrHdr = (VLRHeader *)((byte *)this + m_lastHeaderPos);
		slotNum = m_recordNum;
		m_freeSpaceSize -= sizeof(VLRHeader); //��¼ͷ������
	} else {
		// ˵����¼ͷ���пն�����¼ͷ���뵽�ն��С�
		vlrHdr = (VLRHeader *)((byte *)this + sizeof(VLRHeapRecordPageInfo));
		for (slotNum = 0; !(vlrHdr + slotNum)->m_ifEmpty; ++slotNum) ;
		vlrHdr += slotNum;
		if (m_freeSpaceTailPos - (m_lastHeaderPos + sizeof(VLRHeader)) < dataSize) {
			defrag();
		}
		assert((byte *)vlrHdr < (byte *)this + m_lastHeaderPos);
	}

	// ��������
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
		vlrHdr->m_size = record->m_size; // ���ܲ���8�ֽڣ�����ռ�ÿռ�����8�ֽڣ���dataSize��֤��
		vlrHdr->m_ifTarget = 0;
	}
	vlrHdr->m_ifEmpty = 0;
	vlrHdr->m_ifLink = 0;

	/* ����ͳ����Ϣ��m_freeSpaceTailPos��m_lastHeaderPos��ǰ���Ѿ����¹���
	 * m_freeSpaceSize��ͷ����չ��ʱ��Ҳ�Ѿ��Ĺ���ֻ��Ҫ���¼��ռ�õĿռ�
     */
	++m_recordNum;
	m_freeSpaceSize = (u16)(m_freeSpaceSize - dataSize);
	return slotNum;
}

/**
 *  ����ҳ���е�һ�����õ�slotNum
 * @return  ���õ�slot��
 * @past  �������slot�����ã���Ϊ�ռ䲻���Ĺ�ϵ��������������֤
 *
 */
uint VLRHeapRecordPageInfo::getEmptySlot() {
	assert(m_lastHeaderPos == 0 || m_lastHeaderPos >= sizeof(VLRHeapRecordPageInfo));
	uint slotNum;
	if (!m_recordNum) {
		return 0;
	}
	if (m_lastHeaderPos - sizeof(VLRHeapRecordPageInfo) == (m_recordNum -1) * sizeof(VLRHeader)) {
		// ˵����¼ͷ��û�пն������õ�slot�������
		return m_recordNum;
	}
	assert(m_lastHeaderPos - sizeof(VLRHeapRecordPageInfo) > (m_recordNum - 1) * sizeof(VLRHeader));
	// ˵����¼ͷ���пն�����¼ͷ���뵽�ն��С�
	VLRHeader *vlrHdr = (VLRHeader *)((byte *)this + sizeof(VLRHeapRecordPageInfo));
	for (slotNum = 0; !(vlrHdr + slotNum)->m_ifEmpty; ++slotNum) ;
	assert(slotNum * sizeof(VLRHeader) < m_lastHeaderPos - sizeof(VLRHeapRecordPageInfo));
	return slotNum;
}

/**
 *  ����һ����¼��ָ����¼��
 *
 * @pre               �ռ��㹻��slotNum���ǿղ�
 * @post              �ռ���ܱ�����
 * @param record      ��¼
 * @param slotNum     Ŀ�Ĳۺţ������ǿղ�
 * @param srcRid      ����������һ����¼��target����ô����source��RowId
 */
void VLRHeapRecordPageInfo::insertIntoSlot(const Record *record, u16 slotNum, RowId srcRid) {
	assert(record && record->m_data);
	assert(!getVLRHeader(slotNum) || getVLRHeader(slotNum)->m_ifEmpty);
	verify_ex(vs.hp, verifyPage());
	uint dataSize = (record->m_size < LINK_SIZE) ? LINK_SIZE : record->m_size;
	uint linkSize = srcRid ? LINK_SIZE : 0;
	VLRHeader *hdr = (VLRHeader *)((byte *)this + sizeof(VLRHeapRecordPageInfo) + slotNum * sizeof(VLRHeader));
	if (sizeof(VLRHeapRecordPageInfo) + slotNum * sizeof(VLRHeader) < m_lastHeaderPos) { // ��¼ͷ�嵽��϶��
		assert(m_freeSpaceSize >= (dataSize + linkSize));
		if (m_lastHeaderPos + sizeof(VLRHeader) + dataSize + linkSize > m_freeSpaceTailPos) {
			defrag();
		}
	} else { // ��¼ͷ����ͷ��β��
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
				m_lastHeaderPos = sizeof(VLRHeapRecordPageInfo) - sizeof(VLRHeader); // �������
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
 * �����¼ǰ�ռ��Ƿ��㹻
 * @param slotNum                Ԥ�ڲ���Ĳۺ�
 * @param dataSizeWithHeader     ������¼ͷ�ļ�¼��С
 * @return                       �ռ��㹻����true
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
 * ɾ��һ����¼���������ӣ�����ռ�¼�ۣ���Ӧ�޸���Ҫ�䶯��ҳ��ͳ������
 * @param slotNum     ��¼�ۺ�
 * @return            �ɹ�ɾ������true����¼�����ڷ���false;
 */
bool VLRHeapRecordPageInfo::deleteSlot(uint slotNum) {
	verify_ex(vs.hp, verifyPage());
	VLRHeader *vlrHdr = getVLRHeader(slotNum);
	if (!vlrHdr || vlrHdr->m_ifEmpty) return false;
	vlrHdr->m_ifEmpty = 1;
	int i = 0; // ������ż�¼ͷ����С������
	if (m_lastHeaderPos == sizeof(VLRHeapRecordPageInfo) + slotNum * sizeof(VLRHeader)) { // ֻ�е���¼ͷ�����һ����¼ͷ��ʱ��Ż���ɨ��
		for (; i <= (int)slotNum; ++i) { // ������ǰ�޳���ͷ
			if ((vlrHdr - i)->m_ifEmpty) {
				m_lastHeaderPos -= sizeof(VLRHeader);
			} else break;
		}
	}
	if (vlrHdr->m_offset == m_freeSpaceTailPos) {
		m_freeSpaceTailPos = m_freeSpaceTailPos + ((vlrHdr->m_size < LINK_SIZE) ? LINK_SIZE : vlrHdr->m_size); // ���ڼ�¼���Ŀն���������¼ͷ�޷��жϣ�������Ϊ�ܿ��пռ��м�¼�����ﲻ������
	}
	if (--m_recordNum) {
		m_freeSpaceSize = (u16)(m_freeSpaceSize + ((vlrHdr->m_size < LINK_SIZE) ? LINK_SIZE : vlrHdr->m_size) + i * sizeof(VLRHeader));
	} else {
		// �����һ����¼ɾ��
		m_freeSpaceSize = Limits::PAGE_SIZE - sizeof(VLRHeapRecordPageInfo); // ���ܻ��ʡһ��defrag
		m_lastHeaderPos = 0;
		m_freeSpaceTailPos = Limits::PAGE_SIZE;
	}
	verify_ex(vs.hp, verifyPage());
	return true;
}


/**
 * ��һ��Slot����link��
 *
 * @pre                slotNum��Ӧ�ı����Ǹ���¼�ۣ��ǿգ����Ҳ�����target
 * @post               �ռ������ָ�붼�Ѿ��޸ģ�����ҳ��λͼ��δ�޸ġ�
 * @param tagRid       ָ���Ŀ��RowId
 * @param slotNum      ������slotNum
 * @return             �����ɹ�����true�����������㷵��false
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
 * �ڱ�ҳ����updateһ����¼
 * @pre                �ռ��㹻�������пռ䲻�����⣻��¼���ڣ������д����ռ�¼�ۺŵ�����
 * @post               �µļ�¼���ݱ��嵽ҳ��ȡ���ϼ�¼��ҳ�������������¼ͷλ�ò���
 *                     ������޸ļ�¼�Ǹ�target����ô�޸ĺ�ļ�¼Ҳ�Ǹ�target
 * @param record       ��¼ָ�룬�������ݺͳ�����Ϣ
 * @param slotNum      ���µļ�¼��
 */
void VLRHeapRecordPageInfo::updateLocalRecord(Record *record, uint slotNum) {
	VLRHeader *oldHdr;
	RowId targetSrc = 0;
	oldHdr = (VLRHeader *)((byte *)this + sizeof(VLRHeapRecordPageInfo) + slotNum * sizeof(VLRHeader));
	if (oldHdr->m_ifLink) oldHdr->m_ifLink = 0; // �����link���ͽ�֮ɾ��
	// ����ҳ�ڿ��пռ��С
	if (oldHdr->m_ifTarget) {
		targetSrc = RID_READ(getRecordData(oldHdr) - LINK_SIZE);
		m_freeSpaceSize = (u16)(m_freeSpaceSize - (LINK_SIZE + record->m_size - oldHdr->m_size));
	} else
		m_freeSpaceSize = (u16)(m_freeSpaceSize - ((record->m_size < LINK_SIZE) ? LINK_SIZE : record->m_size ) + (u16)((oldHdr->m_size < LINK_SIZE) ? LINK_SIZE : oldHdr->m_size));

	// ����¼���º�û�б�󣬴洢��ԭ��
	if ((record->m_size + (oldHdr->m_ifTarget ? LINK_SIZE : 0)) <= ((oldHdr->m_size < LINK_SIZE) ? LINK_SIZE : oldHdr->m_size)) { // ����ԭ�ظ���
		memcpy((byte *)this + oldHdr->m_offset + (oldHdr->m_ifTarget ? LINK_SIZE : 0), record->m_data, record->m_size);
		oldHdr->m_size = (u16)(record->m_size + (oldHdr->m_ifTarget ? LINK_SIZE : 0));
		assert(record->m_format == REC_COMPRESSED || record->m_format == REC_VARLEN);
		oldHdr->m_ifCompressed = record->m_format == REC_COMPRESSED ? 1 : 0;
		return;
	}
	if (m_freeSpaceTailPos == oldHdr->m_offset) {
		m_freeSpaceTailPos = m_freeSpaceTailPos + ((oldHdr->m_size < LINK_SIZE) ? LINK_SIZE : oldHdr->m_size);
	}
	// �����¼��ҳͷ�����������пռ��зŲ�����������ҳ����Ƭ
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
 * �ж�һ��ҳ�Ƿ���Բ���
 * @param pageNum     ҳ���
 * @return            �ɲ�������true
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
					//ȡ�ü�¼���ݲ�����ѹ����
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
		result = SampleAnalyse::sampleAnalyse(session, this, maxSamplePages, 30, true, 0.382, 16); // �����ϴ�ʱ�����ƶ�Ҫ����Ը�һЩ
	else
		result = SampleAnalyse::sampleAnalyse(session, this, maxSamplePages, 50, true, 0.618, 8);  // ������Сʱ�����ƶ�Ҫ�����һЩ

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
 * ��һ����¼ҳ������ʱ�������Լ���
 * @param page    ҳ��
 * @return        ���󷵻�true
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
