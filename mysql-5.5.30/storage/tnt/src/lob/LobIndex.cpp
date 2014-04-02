/**
 * ���ʹ�����Ŀ¼�ļ�����
 *
 * @author zx(zhangxiao@corp.netease.com, zx@163.org)
 */
#include "lob/Lob.h"
#include "lob/BigLob.h"
#include "lob/LobIndex.h"
#include "util/File.h"
#include "misc/Buffer.h"
#include "misc/TableDef.h"
#include "api/Database.h"
#include "misc/Session.h"
#include "misc/Syslog.h"
#include "util/Stream.h"
#include "util/Thread.h"
#include <cassert>
#include <set>

namespace ntse {

/** 
 * ���캯��
 *
 * @param db �������ݿ�
 * @param file Ŀ¼�ļ�
 * @param liHeaderPage ͷҳ
 * @param dbObjStats ͳ�ƶ���
 */
LobIndex::LobIndex(Database *db, File *file, LIFileHeaderPageInfo *liHeaderPage, DBObjStats *dbObjStats) {
	m_db = db;
	m_buffer = db->getPageBuffer();
	m_file = file;
	m_headerPage = liHeaderPage;
	m_tableId = liHeaderPage->m_tableId;
	m_maxUsedPage = 0;
	m_dboStats = dbObjStats;
}

/** 
 * ��������
 */
LobIndex::~LobIndex() {
	delete m_file;
}

/**
 * �رմ����Ŀ¼
 *
 * @param session �Ự
 * @param flushDirty �Ƿ�д��������
 */
void LobIndex::close(Session *session, bool flushDirty) {
	u64 errCode;
	if (!m_file) return;
	m_buffer->unpinPage((BufferPageHdr *)m_headerPage);
	m_buffer->freePages(session, m_file, flushDirty);
	errCode = m_file->close();
	if (File::E_NO_ERROR != errCode) {
		m_db->getSyslog()->fopPanic(errCode, "Closing blob index file %s failed.", m_file->getPath());
	}
	delete m_file;
	m_file = NULL;
	delete m_dboStats;
	m_dboStats = NULL;
}


/**
 * ����Ŀ¼�ļ�
 *
 * @param path ����·��
 * @param db ���ݿ����
 * @param tableDef ����
 * @throw �ļ��޷�������IO����ȵ�
 */
void LobIndex::create(const char *path, Database *db, const TableDef *tableDef) throw(NtseException) {
	u64 errCode;
	// ����Ŀ¼�ļ�
	File *indexFile = new File(path);
	errCode = indexFile->create(true, false);
	if (File::E_NO_ERROR != errCode) {
		delete indexFile;
		NTSE_THROW(errCode, "Cannot create lob index file %s", path);
	}
	byte *page = (byte*)System::virtualAlloc(Limits::PAGE_SIZE);
	memset(page, 0, Limits::PAGE_SIZE);
	LIFileHeaderPageInfo *headerPage = (LIFileHeaderPageInfo *)(page);
	headerPage->m_bph.m_lsn = 0;
	headerPage->m_bph.m_checksum = BufferPageHdr::CHECKSUM_NO;
	headerPage->m_fileLen = 1;
	headerPage->m_firstFreePageNum = 0;
	headerPage->m_blobFileLen = Database::getBestIncrSize(tableDef, 0);
	headerPage->m_blobFileTail = 0;
	headerPage->m_blobFileFreeLen = 0;
	headerPage->m_blobFileFreeBlock = 0;
	headerPage->m_tableId = tableDef->m_id;
	if (File::E_NO_ERROR != (errCode = indexFile->setSize(Limits::PAGE_SIZE)))
		db->getSyslog()->fopPanic(errCode, "Setting lob index file '%s' size failed!", path);
	if (File::E_NO_ERROR != (errCode = indexFile->write(0, Limits::PAGE_SIZE, headerPage)))
		db->getSyslog()->fopPanic(errCode, "Cannot write header page for lob index file %s", path);

	System::virtualFree(page);
	indexFile->close();
	delete indexFile;
}

/**
 * redoCreate����
 *
 * @param db ���ݿ������
 * @param tableDef ����
 * @param path �ļ�·��
 * @param tid ��ID
 */
void LobIndex::redoCreate(Database *db, const TableDef *tableDef, const char *path, u16 tid) {
	UNREFERENCED_PARAMETER(tid);
	assert(tableDef->m_id == tid);
	
	u64 errCode;
	u64 fileSize;

	// ��redoĿ¼�ļ�
	File lobIndexFile(path);
	bool exist;
	lobIndexFile.isExist(&exist);

	if (!exist) {
		LobIndex::create(path, db, tableDef);
		return;
	}

	errCode = lobIndexFile.open(true);
	if (File::E_NO_ERROR != errCode) {
		NTSE_THROW(errCode, "Cannot open lob index file %s", path);
	}
	lobIndexFile.getSize(&fileSize);
	lobIndexFile.close();
	if (fileSize < LOB_INDEX_FILE_EXTEND * Limits::PAGE_SIZE) { // ����˵����û�������¼��redoһ�º���
		lobIndexFile.remove();
		LobIndex::create(path, db, tableDef);
	}
}

/**
 * ��Ŀ¼�ļ�
 *
 * @param path ����·��
 * @param db ���ݿ����
 * @param session �Ự
 * @throw �ļ��޷�������IO����ȵ�
 * @return LobIndex����
 */
LobIndex* LobIndex::open(const char *path, Database *db, Session *session) throw(NtseException) {
	u64 errCode;
	// �򿪶��ļ�
	File *indexFile = new File(path);

	errCode = indexFile->open(db->getConfig()->m_directIo);
	if (File::E_NO_ERROR != errCode) {
		delete indexFile;
		NTSE_THROW(errCode, "Cannot open lob index file %s", path);
	}

	DBObjStats *dbObjStats = new DBObjStats(DBO_LlobDir);
	// ȡ����ҳ��Ϣ
	LIFileHeaderPageInfo *headerPage;
	headerPage = (LIFileHeaderPageInfo *)db->getPageBuffer()->getPage(session, indexFile, PAGE_LOB_INDEX, 0, Shared, dbObjStats);
	assert(NULL != headerPage);
	LobIndex *lobIn = NULL;
	lobIn = new LobIndex(db, indexFile, headerPage, dbObjStats);

	// ֻ���ͷҳ���������ͷ�pin��ʼ�ս�Ŀ¼��ҳpin�ڻ�����
	db->getPageBuffer()->unlockPage(session->getId(), (BufferPageHdr *)headerPage, Shared);
	return lobIn;
}

/** ���ñ�ID
 * @param session �Ự
 * @param tableId ��ID
 */
void LobIndex::setTableId(Session *session, u16 tableId) {
	m_tableId = tableId;
	BufferPageHandle *headerPageHdl = lockHeaderPage(session, Exclusived);
	LIFileHeaderPageInfo *headerPage = (LIFileHeaderPageInfo *)headerPageHdl->getPage();
	headerPage->m_tableId = tableId;
	m_buffer->markDirty(session, headerPageHdl->getPage());
	m_buffer->writePage(session, headerPageHdl->getPage());
	unlockHeaderPage(session, headerPageHdl);
}

/**
 * ��ס��ҳ���ҷ���
 *
 * @param session �Ự����
 * @param lockMode ��ģʽ
 * @return ҳ����
 */
BufferPageHandle* LobIndex::lockHeaderPage(Session *session, LockMode lockMode) {
	return LOCK_PAGE(session, (BufferPageHdr *)m_headerPage, m_file, 0, lockMode);
}

/**
 * �ͷ���ҳ
 *
 * @param session �Ự����
 * @param pageHdl OUT ҳ����
 */
void LobIndex::unlockHeaderPage(Session *session, BufferPageHandle *pageHdl) {
	session->unlockPage(&pageHdl);
}


/**
 * ���ڴ��з���һ��������ҳ���ڴ棬��ʼ��ÿ��ҳ���ҳ���ϵ�ÿ����¼�ۣ����ҽ�����ҳ�洮������
 *
 * @param size ��С
 * @param indexFileLen �����ļ�����
 * @return �ڴ�ҳ���׵�ַ
 */
byte* LobIndex::createNewPagesInMem(uint size, u32 indexFileLen) {
	byte *pages = (byte *)System::virtualAlloc(Limits::PAGE_SIZE * size);
	if (!pages)
		m_db->getSyslog()->log(EL_PANIC, "Unable to alloc %d bytes", size * Limits::PAGE_SIZE);
	memset(pages, 0, Limits::PAGE_SIZE * size);
	/* ��ʼ������ҳ��ռ� */
	LIFilePageInfo *page = (LIFilePageInfo *)pages;
	for (uint i = 0; i < size; i++) {
		page->m_bph.m_lsn = 0;
		page->m_bph.m_checksum = BufferPageHdr::CHECKSUM_NO;
		page->m_inFreePageList = true;
		page->m_freeSlotNum = MAX_SLOT_PER_PAGE;
		page->m_firstFreeSlot = 0;

		/* nextFreePageNum����ָ����һҳ */
		if (i < size - 1)
			page->m_nextFreePageNum = indexFileLen + i + 1;
		else
			page->m_nextFreePageNum = 0;
		/* ��ʼ��ÿһ��slot */
		for (uint j = 0; j < MAX_SLOT_PER_PAGE; j++) {
			LiFileSlotInfo *slot = getSlot(page, (s16)j);
			slot->m_free = true;
			if (j < MAX_SLOT_PER_PAGE - 1)
				slot->u.m_nextFreeSlot = (s16)(j + 1);
			else
				slot->u.m_nextFreeSlot = -1;
		}
		page = (LIFilePageInfo *)((byte *)page + Limits::PAGE_SIZE);
	}
	return pages;
}

/**
 * ��չĿ¼�ļ�
 *
 * @param session �Ự
 * @param headerPage ��ҳ
 * @param extendSize ����ĳ���
 * @param indexFileLen Ŀ¼�ļ���ǰ����
 */
void LobIndex::extendIndexFile(Session *session, LIFileHeaderPageInfo *headerPage, uint extendSize, u32 indexFileLen) {
	u64 errCode;
	if (File::E_NO_ERROR != (errCode = m_file->setSize((u64)(indexFileLen + extendSize) * (u64)Limits::PAGE_SIZE)))
		m_db->getSyslog()->fopPanic(errCode, "Extending lob index file '%s' size failed!",m_file->getPath());

	for (uint i = 0; i < extendSize; ++i) {
		u64 pageNum = indexFileLen + i; // maxPageNum��û�иı�
		BufferPageHandle *pageHdl = NEW_PAGE(session, m_file, PAGE_LOB_INDEX, pageNum, Exclusived, m_dboStats);
		LIFilePageInfo *page = (LIFilePageInfo *)pageHdl->getPage();
		memset(page, 0, Limits::PAGE_SIZE);
		page->m_bph.m_lsn = 0;
		page->m_bph.m_checksum = BufferPageHdr::CHECKSUM_NO;
		page->m_inFreePageList = true;
		page->m_freeSlotNum = MAX_SLOT_PER_PAGE;
		page->m_firstFreeSlot = 0;

		/* nextFreePageNum����ָ����һҳ */
		if (i < extendSize - 1)
			page->m_nextFreePageNum = indexFileLen + i + 1;
		else
			page->m_nextFreePageNum = 0;
		/* ��ʼ��ÿһ��slot */
		for (uint j = 0; j < MAX_SLOT_PER_PAGE; j++) {
			LiFileSlotInfo *slot = getSlot(page, (s16)j);
			slot->m_free = true;
			if (j < MAX_SLOT_PER_PAGE - 1)
				slot->u.m_nextFreeSlot = (s16)(j + 1);
			else
				slot->u.m_nextFreeSlot = -1;
		}
		session->markDirty(pageHdl);
		session->releasePage(&pageHdl);
	}
	m_buffer->batchWrite(session, m_file, PAGE_LOB_INDEX, indexFileLen, indexFileLen + extendSize - 1);	
	headerPage->m_firstFreePageNum = headerPage->m_fileLen;
	headerPage->m_fileLen += extendSize;
}
/**
 * Ѱ��һ�����п��пռ��Ŀ¼ҳ��
 *
 * @post ���ص�ҳ��������Ǽ�X��,�������п��в۵�
 * @param session �Ự����
 * @param pageNum OUT�����ҳ���
 * @return �ҵ��Ŀ���ҳ��
 */
BufferPageHandle* LobIndex::getFreePage(Session *session, u64 *pageNum) {
	BufferPageHandle *headerPageHdl;
	LIFileHeaderPageInfo *headerPage;
	BufferPageHandle *freePageHdl;
	LIFilePageInfo *freePage;
	u64 firstFreePageNum;

findFreePage_begin:
	// ȡ����ҳ������
	headerPageHdl = lockHeaderPage(session, Shared);
	headerPage = (LIFileHeaderPageInfo *)headerPageHdl->getPage();
	/* ���һ������ҳ���ַ���ͷ���ҳ */
	firstFreePageNum = headerPage->m_firstFreePageNum;
	unlockHeaderPage(session, headerPageHdl);
	headerPage = NULL;

	SYNCHERE(SP_LOB_BIG_GET_FREE_PAGE);

findFreePage_checkPage:
	if (0 != firstFreePageNum) { // ����ҳ�治Ϊ0��˵���п��пռ�
		*pageNum = firstFreePageNum;
		// ��ס���ҳ
		freePageHdl = GET_PAGE(session, m_file, PAGE_LOB_INDEX, firstFreePageNum, Exclusived, m_dboStats, NULL);
		freePage = (LIFilePageInfo *)freePageHdl->getPage();
		assert(NULL != freePage);
		if (-1 == freePage->m_firstFreeSlot) { // û�п��в��ˣ���������
			session->releasePage(&freePageHdl);
			goto findFreePage_begin;
		}
	} else {// û�п���ҳ��
		SYNCHERE(SP_LOB_BIG_NO_FREE_PAGE);
		headerPageHdl = lockHeaderPage(session, Exclusived);
		headerPage = (LIFileHeaderPageInfo *)headerPageHdl->getPage();
		firstFreePageNum = headerPage->m_firstFreePageNum;
		if (0 != firstFreePageNum) {// ��˵�������̴߳����˿���ҳ��
			unlockHeaderPage(session, headerPageHdl);
			goto findFreePage_checkPage;
		}
		// ����û�п���ҳ����
		*pageNum = headerPage->m_fileLen;
		extendIndexFile(session, (LIFileHeaderPageInfo *)headerPage, LOB_INDEX_FILE_EXTEND, headerPage->m_fileLen);
		freePageHdl = GET_PAGE(session, m_file, PAGE_LOB_INDEX, *pageNum, Exclusived, m_dboStats, NULL);
		assert(NULL != freePageHdl);
		session->markDirty(headerPageHdl);
		unlockHeaderPage(session, headerPageHdl);
	}
	SYNCHERE(SP_LOB_BIG_GET_FREE_PAGE_FINISH);
	return freePageHdl;
}

/**
 * �õ���Ӧ��Ŀ¼��
 *
 * @param pageRecord ���ڵ�ҳ
 * @param slotNum  slot��
 * @return Ŀ¼��
 */
LiFileSlotInfo* LobIndex::getSlot(LIFilePageInfo *pageRecord, u16 slotNum){
	return (LiFileSlotInfo *)((byte *)pageRecord + LobIndex::OFFSET_PAGE_RECORD + slotNum * LobIndex::INDEX_SLOT_LENGTH);
}

/**
 * �õ�һ������Ŀ¼��
 * @param session �Ự
 * @param reFreePageHdl out ���صĿ���ҳ���
 * @param isHeaderPageLock out ��ҳ�Ƿ���ס
 * @param reHeaderPageHdl out ��ҳ�ľ��
 * @param newFreeHeader out �µĿ���ҳͷ
 * @param pageId ��Ҫд����в۵Ĵ�������ʼҳ��
 * @return LobId
 */
LobId LobIndex::getFreeSlot(Session *session, BufferPageHandle **reFreePageHdl, bool *isHeaderPageLock, BufferPageHandle **reHeaderPageHdl, u32 *newFreeHeader, u32 pageId) {
	u64 pageNum = 0;
	LobId lid = 0;
	BufferPageHandle *freePageHdl;
	LIFilePageInfo *freePage;
	BufferPageHandle *headerPageHdl;
	LIFileHeaderPageInfo *headerPage;

redo_start:
	headerPage = NULL;
	headerPageHdl = NULL;
	freePageHdl = getFreePage(session, &pageNum);
	// ����õ��Ŀ���ҳ�϶��Ǻ��п��в۵�
	freePage = (LIFilePageInfo *)freePageHdl->getPage();
	SYNCHERE(SP_LOB_BIG_NOT_FIRST_FREE_PAGE);
	if (freePage->m_inFreePageList && freePage->m_freeSlotNum == 1) {
		headerPageHdl = lockHeaderPage(session, Exclusived);
		headerPage = (LIFileHeaderPageInfo *)headerPageHdl->getPage();
		if (pageNum != headerPage->m_firstFreePageNum) {
			unlockHeaderPage(session, headerPageHdl);
			session->releasePage(&freePageHdl);
			goto redo_start;
		}
	}
	// �����ж��Ƿ��п���slot
	assert (freePage->m_inFreePageList);
	s16	firstFrSlot = freePage->m_firstFreeSlot;
	LiFileSlotInfo *freeSlotInfo = getSlot(freePage,firstFrSlot);
	freePage->m_freeSlotNum--;
	// �ж��Ƿ�ֻ��һ�����в�
	if (freePage->m_freeSlotNum >= 1) {
		// �޸�ҳ�ڿ�������
		freePage->m_firstFreeSlot = freeSlotInfo->u.m_nextFreeSlot;
		lid = LID(pageNum, firstFrSlot);
		*isHeaderPageLock = false;
		*reHeaderPageHdl = NULL;
	} else { // ���һ�����в�
		assert(freePage->m_freeSlotNum == 0);
		freePage->m_firstFreeSlot = -1;
		freePage->m_inFreePageList = false;
		// ��������ҳ����
		headerPage->m_firstFreePageNum = freePage->m_nextFreePageNum;
		*newFreeHeader = freePage->m_nextFreePageNum;
		freePage->m_nextFreePageNum = 0;
		lid = LID(pageNum, firstFrSlot);
		*isHeaderPageLock = true;
		*reHeaderPageHdl = headerPageHdl;
	}
	// �޸Ŀ��в�����
	freeSlotInfo->m_free = false;
	freeSlotInfo->u.m_pageId = pageId;

	*reFreePageHdl = freePageHdl;
	return LID_MAKE_BIGLOB_BIT(lid);
}

/**
 * �õ���Ӧĸ���ID
 *
 * @return ��ID
 */
u16 LobIndex::getTableID() {
	return m_tableId;
}

/**
 * �ж�Ŀ¼�ļ�ĳ��ҳ��ȷ�ԣ���Ҫ��֤������в�����Ϳ��в�������Ϣ
 *
 * @param session �Ự����
 * @param pid Ŀ¼�ļ�ҳ��
 */
//void LobIndex::verifyPage(Session *session, u32 pid) {
//	BufferPageHandle *liFilePageHdl = GET_PAGE(session, m_file, PAGE_LOB_INDEX, pid, Exclusived, m_dboStats, NULL);
//	LIFilePageInfo *liFilePage = (LIFilePageInfo *)liFilePageHdl->getPage();
//	u16 freeSlot = liFilePage->m_freeSlotNum;
//	if (freeSlot > 0) {
//		assert(liFilePage->m_inFreePageList);
//		// ��֤���в�list
//		s16 firstFreeSlot = liFilePage->m_firstFreeSlot;
//		LiFileSlotInfo *slotInfo = getSlot(liFilePage, firstFreeSlot);
//		bool isFree = slotInfo->m_free;
//		assert(isFree);
//		s16 nextFreeSlot = slotInfo->u.m_nextFreeSlot;
//		uint j = 1;
//		while (nextFreeSlot != -1) {
//			j++;
//			slotInfo = getSlot(liFilePage, nextFreeSlot);
//			isFree = slotInfo->m_free;
//			assert(isFree);
//			nextFreeSlot = slotInfo->u.m_nextFreeSlot;
//		}
//		assert(j == freeSlot);
//	} else {
//		assert(!liFilePage->m_inFreePageList);
//		assert(liFilePage->m_firstFreeSlot == -1);
//	}
//	session->releasePage(&liFilePageHdl);
//}

/**
 * ��ʼһ�β���ͳ�ƹ���
 *
 * @param session �Ự
 * @param maxSampleNum ����������
 * @param fastSample �Ƿ��ǿ��ٲ������Ǳ�ʾ�ڴ�buffer����Ϊ�������ʾ����������Ϊ��
 * @return ��ʼ���õĲ������
 */
SampleHandle* LobIndex::beginSample(Session *session, uint maxSampleNum, bool fastSample) {
	LobIndexSampleHandle *handle = new LobIndexSampleHandle(session, maxSampleNum, fastSample);
	if(fastSample) {
		// ��Ҫ���ڴ��в���,�Ȳ���Ŀ¼�ļ�
		handle->m_bufScanHdl = m_buffer->beginScan(session->getId(), m_file);
	} else {
		// Ȼ���ڴ����ϲ���
		// �ȵõ�Ŀ¼�ļ���С
		
		// ������Ҫ���Ǹճ�ʼ����ҳ��,���Բ����ǴӺ��濪ʼ��ǰ���õ�����ʹ�õ�ҳ�档
		handle->m_maxPage = m_maxUsedPage;
		handle->m_minPage = metaDataPgCnt();

		// ���ļ����峤�ȣ�Ȼ����������ķ�����С�ͷ����ĸ���
		if (handle->m_maxSample > (handle->m_maxPage - handle->m_minPage + 1))
			handle->m_maxSample = (uint)(handle->m_maxPage - handle->m_minPage + 1);
		
		if (maxSampleNum > handle->m_maxSample) 
			maxSampleNum = handle->m_maxSample;
		/** ��Ϊ16��������������ΪĿ¼�ļ�һ����չ16��ҳ�� */
		handle->m_blockNum = 16 > maxSampleNum ? maxSampleNum : 16;   
		handle->m_regionSize = (handle->m_maxPage + 1 - handle->m_minPage) / handle->m_blockNum;
		
		// ���ҳ��̫�٣������Է�������ȫ������
		if (handle->m_regionSize <= 1) { 
			handle->m_blockNum = 1;
			handle->m_regionSize = handle->m_maxPage + 1 - handle->m_minPage;
		}
		handle->m_curBlock = 0;
		handle->m_blockSize = handle->m_maxSample / handle->m_blockNum;
		handle->m_curIdxInBlock = 0;
	}
	return handle;
}


/** 
 * �õ�����ʹ��ҳ
 *
 * @param session �Ự����
 * @return ����ʹ��ҳ��ҳ��
 */
u32 LobIndex::getMaxUsedPage(Session *session) {
	BufferPageHandle *headerPageHdl = lockHeaderPage(session, Shared);
	LIFileHeaderPageInfo *headerPage = (LIFileHeaderPageInfo *)headerPageHdl->getPage();
	u32 fileLen = headerPage->m_fileLen;
	if (fileLen == 1) {
		unlockHeaderPage(session, headerPageHdl);
		return 0;
	}
	unlockHeaderPage(session, headerPageHdl);
	u32 pid = fileLen - 1;
	while(true) {
		BufferPageHandle *liFilePageHdl = GET_PAGE(session, m_file, PAGE_LOB_INDEX, pid, Shared, m_dboStats, NULL);
		LIFilePageInfo *liFilePage = (LIFilePageInfo *)liFilePageHdl->getPage();
		u32 freeSlotNum = liFilePage->m_freeSlotNum;
		session->releasePage(&liFilePageHdl);
		if (freeSlotNum < MAX_SLOT_PER_PAGE) {
			break;	
		}
		pid--;
	}
	m_maxUsedPage = pid;
	return pid;
}

/**
 * ȡ�ò�������һ��ҳ��
 *
 * @param handle �������
 * @return ����
 */
Sample* LobIndex::sampleNext(SampleHandle *handle) {
	LobIndexSampleHandle *shdl = (LobIndexSampleHandle *)handle;
	if (handle->m_fastSample) {
		if (shdl->m_maxSample > 0) {
sampleNext_bufferPage_getNext:
			const Bcb *bcb = m_buffer->getNext(shdl->m_bufScanHdl);
			if (!bcb)
				return NULL;
			u64 pageId = bcb->m_pageKey.m_pageId;
			if (pageId > shdl->m_maxPage || pageId < shdl->m_minPage)
				goto sampleNext_bufferPage_getNext; 
			BufferPageHdr *page = bcb->m_page;
			--shdl->m_maxSample;
			Sample *sample = sampleBufferPage(handle->m_session, page);
			sample->m_ID = bcb->m_pageKey.m_pageId;
			return sample;
		}
		return NULL;
	} else if (shdl->m_blockPages) {
		if (shdl->m_curIdxInBlock == shdl->m_blockSize - 1) { // ��blockɨ�����
			if (shdl->m_curBlock == shdl->m_blockNum - 1) { // �������һ��block
				return NULL;
			}
			shdl->m_curBlock++;
			shdl->m_curIdxInBlock = 0;
			u64 regionSize = (shdl->m_curBlock == shdl->m_blockNum - 1) ?
				(shdl->m_regionSize + (shdl->m_maxPage + 1 - shdl->m_minPage) % shdl->m_regionSize)
				: shdl->m_regionSize;  // ���һ��region������ҳ���������
			uint unsamplable = unsamplablePagesBetween(shdl->m_minPage + shdl->m_regionSize * shdl->m_curBlock, regionSize);
			assert(unsamplable < shdl->m_blockSize);
			shdl->m_curIdxInBlock += unsamplable;
			selectPage(shdl->m_blockPages + unsamplable, shdl->m_blockSize - unsamplable,
				shdl->m_minPage + shdl->m_regionSize * shdl->m_curBlock, regionSize);
			return sample(shdl->m_blockPages[unsamplable], shdl->m_session);
		}
		shdl->m_curIdxInBlock++;
		return sample(shdl->m_blockPages[shdl->m_curIdxInBlock], shdl->m_session);
	} else {
		// ��һ��sampleNext
		assert(shdl->m_curBlock == 0 && shdl->m_curIdxInBlock == 0);
		shdl->m_blockPages = new u64[shdl->m_blockSize];
		uint unsamplable = unsamplablePagesBetween(shdl->m_minPage, shdl->m_regionSize);
		assert(unsamplable < shdl->m_blockSize);
		shdl->m_curIdxInBlock += unsamplable;
		selectPage(shdl->m_blockPages + unsamplable, shdl->m_blockSize - unsamplable, shdl->m_minPage, shdl->m_regionSize);
		return sample(shdl->m_blockPages[unsamplable], shdl->m_session);
	}
}


/** 
 * �����������ͷ���Դ 
 *
 * @param handle �������
 */
void LobIndex::endSample(SampleHandle *handle) {
	LobIndexSampleHandle *shdl = (LobIndexSampleHandle *)handle;
	if (handle->m_fastSample) {
		assert(!shdl->m_blockPages);
		m_buffer->endScan(shdl->m_bufScanHdl);
	} else {
		assert(!shdl->m_bufScanHdl);
		if (shdl->m_blockPages)
			delete [] shdl->m_blockPages;
		
	}
	delete shdl;
}


/** 
 * �õ�һ��ҳ���ʹ�õ�Ŀ¼����� 
 *
 * @param session �Ự
 * @param page ����ҳ
 * @return һ������
 */
Sample* LobIndex::sampleBufferPage(Session *session, BufferPageHdr *page) {
	LIFilePageInfo *liPage = (LIFilePageInfo *)page;
	Sample *sample = Sample::create(session, SAMPLE_FIELDS_NUM);
	(*sample)[SAMPLE_FIELDS_NUM - 1] = MAX_SLOT_PER_PAGE - liPage->m_freeSlotNum;
	return sample;
}


/**
 * ��һ�����������ѡ���ҵ�N��ҳ�棬Ȼ����з���
 *
 * @param outPages out ���飬��ѡ����ҳ��
 * @param wantNum ��Ҫ��ѡ���ҳ��
 * @param min ��С��ҳ��
 * @param regionSize ѡ������Ĵ�С
 */
void LobIndex::selectPage(u64 *outPages, uint wantNum, u64 min, u64 regionSize) {
	assert(regionSize >= wantNum);
	set<u64> pnset;
	while (pnset.size() != (uint)wantNum) {
		u64 pn = min + (System::random() % regionSize);
		pnset.insert(pn);
	}
	uint idx = 0;
	for (set<u64>::iterator it = pnset.begin(); it != pnset.end(); ++it) {
		assert(*it >= min && *it < min + regionSize);
		outPages[idx++] = *it;
	}
	assert(idx == wantNum);
	pnset.clear();
}

/**
 * �õ�һ��sample
 *
 * @param pageId ҳ��
 * @param session �Ự����
 * @return һ������
 */
Sample* LobIndex::sample(u64 pageId, Session *session) {
	BufferPageHandle *pageHdl = GET_PAGE(session, m_file, PAGE_LOB_INDEX, pageId, Shared, m_dboStats, NULL);
	Sample *sample = sampleBufferPage(session, pageHdl->getPage());
	session->releasePage(&pageHdl);

	sample->m_ID = (SampleID)pageId;
	return sample;
}

/** 
 * Ѱ�Ҳ��ɲ�����ҳ�� 
 *
 * @param minPage ��Сҳ��
 * @param regionSize �����С
 * @return ҳ��ID
 */
uint LobIndex::unsamplablePagesBetween(u64 minPage, u64 regionSize) {
	UNREFERENCED_PARAMETER(minPage);
	UNREFERENCED_PARAMETER(regionSize);
	return 0;
}
}
