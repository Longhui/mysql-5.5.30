/**
 * 大型大对象的目录文件管理
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
 * 构造函数
 *
 * @param db 所属数据库
 * @param file 目录文件
 * @param liHeaderPage 头页
 * @param dbObjStats 统计对象
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
 * 析构函数
 */
LobIndex::~LobIndex() {
	delete m_file;
}

/**
 * 关闭大对象目录
 *
 * @param session 会话
 * @param flushDirty 是否写出脏数据
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
 * 创建目录文件
 *
 * @param path 完整路径
 * @param db 数据库对象
 * @param tableDef 表定义
 * @throw 文件无法创建，IO错误等等
 */
void LobIndex::create(const char *path, Database *db, const TableDef *tableDef) throw(NtseException) {
	u64 errCode;
	// 创建目录文件
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
 * redoCreate操作
 *
 * @param db 数据库对象句柄
 * @param tableDef 表定义
 * @param path 文件路径
 * @param tid 表ID
 */
void LobIndex::redoCreate(Database *db, const TableDef *tableDef, const char *path, u16 tid) {
	UNREFERENCED_PARAMETER(tid);
	assert(tableDef->m_id == tid);
	
	u64 errCode;
	u64 fileSize;

	// 先redo目录文件
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
	if (fileSize < LOB_INDEX_FILE_EXTEND * Limits::PAGE_SIZE) { // 成立说明还没插入过记录，redo一下好了
		lobIndexFile.remove();
		LobIndex::create(path, db, tableDef);
	}
}

/**
 * 打开目录文件
 *
 * @param path 完整路径
 * @param db 数据库对象
 * @param session 会话
 * @throw 文件无法创建，IO错误等等
 * @return LobIndex对象
 */
LobIndex* LobIndex::open(const char *path, Database *db, Session *session) throw(NtseException) {
	u64 errCode;
	// 打开堆文件
	File *indexFile = new File(path);

	errCode = indexFile->open(db->getConfig()->m_directIo);
	if (File::E_NO_ERROR != errCode) {
		delete indexFile;
		NTSE_THROW(errCode, "Cannot open lob index file %s", path);
	}

	DBObjStats *dbObjStats = new DBObjStats(DBO_LlobDir);
	// 取得首页信息
	LIFileHeaderPageInfo *headerPage;
	headerPage = (LIFileHeaderPageInfo *)db->getPageBuffer()->getPage(session, indexFile, PAGE_LOB_INDEX, 0, Shared, dbObjStats);
	assert(NULL != headerPage);
	LobIndex *lobIn = NULL;
	lobIn = new LobIndex(db, indexFile, headerPage, dbObjStats);

	// 只解除头页的锁，不释放pin，始终将目录首页pin在缓存中
	db->getPageBuffer()->unlockPage(session->getId(), (BufferPageHdr *)headerPage, Shared);
	return lobIn;
}

/** 设置表ID
 * @param session 会话
 * @param tableId 表ID
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
 * 锁住首页并且返回
 *
 * @param session 会话对象
 * @param lockMode 锁模式
 * @return 页面句柄
 */
BufferPageHandle* LobIndex::lockHeaderPage(Session *session, LockMode lockMode) {
	return LOCK_PAGE(session, (BufferPageHdr *)m_headerPage, m_file, 0, lockMode);
}

/**
 * 释放首页
 *
 * @param session 会话对象
 * @param pageHdl OUT 页面句柄
 */
void LobIndex::unlockHeaderPage(Session *session, BufferPageHandle *pageHdl) {
	session->unlockPage(&pageHdl);
}


/**
 * 在内存中分配一块连续的页面内存，初始化每个页面和页面上的每个记录槽，并且将各个页面串接起来
 *
 * @param size 大小
 * @param indexFileLen 索引文件长度
 * @return 内存页面首地址
 */
byte* LobIndex::createNewPagesInMem(uint size, u32 indexFileLen) {
	byte *pages = (byte *)System::virtualAlloc(Limits::PAGE_SIZE * size);
	if (!pages)
		m_db->getSyslog()->log(EL_PANIC, "Unable to alloc %d bytes", size * Limits::PAGE_SIZE);
	memset(pages, 0, Limits::PAGE_SIZE * size);
	/* 初始化所有页面空间 */
	LIFilePageInfo *page = (LIFilePageInfo *)pages;
	for (uint i = 0; i < size; i++) {
		page->m_bph.m_lsn = 0;
		page->m_bph.m_checksum = BufferPageHdr::CHECKSUM_NO;
		page->m_inFreePageList = true;
		page->m_freeSlotNum = MAX_SLOT_PER_PAGE;
		page->m_firstFreeSlot = 0;

		/* nextFreePageNum依次指向下一页 */
		if (i < size - 1)
			page->m_nextFreePageNum = indexFileLen + i + 1;
		else
			page->m_nextFreePageNum = 0;
		/* 初始化每一个slot */
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
 * 扩展目录文件
 *
 * @param session 会话
 * @param headerPage 首页
 * @param extendSize 分配的长度
 * @param indexFileLen 目录文件当前长度
 */
void LobIndex::extendIndexFile(Session *session, LIFileHeaderPageInfo *headerPage, uint extendSize, u32 indexFileLen) {
	u64 errCode;
	if (File::E_NO_ERROR != (errCode = m_file->setSize((u64)(indexFileLen + extendSize) * (u64)Limits::PAGE_SIZE)))
		m_db->getSyslog()->fopPanic(errCode, "Extending lob index file '%s' size failed!",m_file->getPath());

	for (uint i = 0; i < extendSize; ++i) {
		u64 pageNum = indexFileLen + i; // maxPageNum还没有改变
		BufferPageHandle *pageHdl = NEW_PAGE(session, m_file, PAGE_LOB_INDEX, pageNum, Exclusived, m_dboStats);
		LIFilePageInfo *page = (LIFilePageInfo *)pageHdl->getPage();
		memset(page, 0, Limits::PAGE_SIZE);
		page->m_bph.m_lsn = 0;
		page->m_bph.m_checksum = BufferPageHdr::CHECKSUM_NO;
		page->m_inFreePageList = true;
		page->m_freeSlotNum = MAX_SLOT_PER_PAGE;
		page->m_firstFreeSlot = 0;

		/* nextFreePageNum依次指向下一页 */
		if (i < extendSize - 1)
			page->m_nextFreePageNum = indexFileLen + i + 1;
		else
			page->m_nextFreePageNum = 0;
		/* 初始化每一个slot */
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
 * 寻找一个含有空闲空间的目录页面
 *
 * @post 返回的页句柄必须是加X锁,并且是有空闲槽的
 * @param session 会话对象
 * @param pageNum OUT，存放页面号
 * @return 找到的空闲页面
 */
BufferPageHandle* LobIndex::getFreePage(Session *session, u64 *pageNum) {
	BufferPageHandle *headerPageHdl;
	LIFileHeaderPageInfo *headerPage;
	BufferPageHandle *freePageHdl;
	LIFilePageInfo *freePage;
	u64 firstFreePageNum;

findFreePage_begin:
	// 取得首页共享锁
	headerPageHdl = lockHeaderPage(session, Shared);
	headerPage = (LIFileHeaderPageInfo *)headerPageHdl->getPage();
	/* 获得一个空闲页面地址后释放首页 */
	firstFreePageNum = headerPage->m_firstFreePageNum;
	unlockHeaderPage(session, headerPageHdl);
	headerPage = NULL;

	SYNCHERE(SP_LOB_BIG_GET_FREE_PAGE);

findFreePage_checkPage:
	if (0 != firstFreePageNum) { // 空闲页面不为0，说明有空闲空间
		*pageNum = firstFreePageNum;
		// 锁住这个页
		freePageHdl = GET_PAGE(session, m_file, PAGE_LOB_INDEX, firstFreePageNum, Exclusived, m_dboStats, NULL);
		freePage = (LIFilePageInfo *)freePageHdl->getPage();
		assert(NULL != freePage);
		if (-1 == freePage->m_firstFreeSlot) { // 没有空闲槽了，重新来过
			session->releasePage(&freePageHdl);
			goto findFreePage_begin;
		}
	} else {// 没有空闲页面
		SYNCHERE(SP_LOB_BIG_NO_FREE_PAGE);
		headerPageHdl = lockHeaderPage(session, Exclusived);
		headerPage = (LIFileHeaderPageInfo *)headerPageHdl->getPage();
		firstFreePageNum = headerPage->m_firstFreePageNum;
		if (0 != firstFreePageNum) {// 这说明其他线程创建了空闲页面
			unlockHeaderPage(session, headerPageHdl);
			goto findFreePage_checkPage;
		}
		// 但是没有空闲页面了
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
 * 得到对应的目录项
 *
 * @param pageRecord 所在的页
 * @param slotNum  slot号
 * @return 目录项
 */
LiFileSlotInfo* LobIndex::getSlot(LIFilePageInfo *pageRecord, u16 slotNum){
	return (LiFileSlotInfo *)((byte *)pageRecord + LobIndex::OFFSET_PAGE_RECORD + slotNum * LobIndex::INDEX_SLOT_LENGTH);
}

/**
 * 得到一个空闲目录项
 * @param session 会话
 * @param reFreePageHdl out 返回的空闲页句柄
 * @param isHeaderPageLock out 首页是否锁住
 * @param reHeaderPageHdl out 首页的句柄
 * @param newFreeHeader out 新的空闲页头
 * @param pageId 需要写入空闲槽的大对象的起始页号
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
	// 下面得到的空闲页肯定是含有空闲槽的
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
	// 首先判断是否有空闲slot
	assert (freePage->m_inFreePageList);
	s16	firstFrSlot = freePage->m_firstFreeSlot;
	LiFileSlotInfo *freeSlotInfo = getSlot(freePage,firstFrSlot);
	freePage->m_freeSlotNum--;
	// 判断是否只有一个空闲槽
	if (freePage->m_freeSlotNum >= 1) {
		// 修改页内空闲链表
		freePage->m_firstFreeSlot = freeSlotInfo->u.m_nextFreeSlot;
		lid = LID(pageNum, firstFrSlot);
		*isHeaderPageLock = false;
		*reHeaderPageHdl = NULL;
	} else { // 最后一个空闲槽
		assert(freePage->m_freeSlotNum == 0);
		freePage->m_firstFreeSlot = -1;
		freePage->m_inFreePageList = false;
		// 调整空闲页链表
		headerPage->m_firstFreePageNum = freePage->m_nextFreePageNum;
		*newFreeHeader = freePage->m_nextFreePageNum;
		freePage->m_nextFreePageNum = 0;
		lid = LID(pageNum, firstFrSlot);
		*isHeaderPageLock = true;
		*reHeaderPageHdl = headerPageHdl;
	}
	// 修改空闲槽内容
	freeSlotInfo->m_free = false;
	freeSlotInfo->u.m_pageId = pageId;

	*reFreePageHdl = freePageHdl;
	return LID_MAKE_BIGLOB_BIT(lid);
}

/**
 * 得到对应母表的ID
 *
 * @return 表ID
 */
u16 LobIndex::getTableID() {
	return m_tableId;
}

/**
 * 判断目录文件某个页正确性，主要验证里面空闲槽链表和空闲槽数等信息
 *
 * @param session 会话对象
 * @param pid 目录文件页号
 */
//void LobIndex::verifyPage(Session *session, u32 pid) {
//	BufferPageHandle *liFilePageHdl = GET_PAGE(session, m_file, PAGE_LOB_INDEX, pid, Exclusived, m_dboStats, NULL);
//	LIFilePageInfo *liFilePage = (LIFilePageInfo *)liFilePageHdl->getPage();
//	u16 freeSlot = liFilePage->m_freeSlotNum;
//	if (freeSlot > 0) {
//		assert(liFilePage->m_inFreePageList);
//		// 验证空闲槽list
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
 * 开始一次采样统计过程
 *
 * @param session 会话
 * @param maxSampleNum 最大采样个数
 * @param fastSample 是否是快速采样：是表示内存buffer采样为主，否表示外存随机采样为主
 * @return 初始化好的采样句柄
 */
SampleHandle* LobIndex::beginSample(Session *session, uint maxSampleNum, bool fastSample) {
	LobIndexSampleHandle *handle = new LobIndexSampleHandle(session, maxSampleNum, fastSample);
	if(fastSample) {
		// 主要在内存中采样,先采样目录文件
		handle->m_bufScanHdl = m_buffer->beginScan(session->getId(), m_file);
	} else {
		// 然后在磁盘上采样
		// 先得到目录文件大小
		
		// 这里需要考虑刚初始化的页面,所以采样是从后面开始往前，得到真正使用的页面。
		handle->m_maxPage = m_maxUsedPage;
		handle->m_minPage = metaDataPgCnt();

		// 看文件具体长度，然后决定采样的分区大小和分区的个数
		if (handle->m_maxSample > (handle->m_maxPage - handle->m_minPage + 1))
			handle->m_maxSample = (uint)(handle->m_maxPage - handle->m_minPage + 1);
		
		if (maxSampleNum > handle->m_maxSample) 
			maxSampleNum = handle->m_maxSample;
		/** 分为16个区来采样，因为目录文件一次扩展16个页面 */
		handle->m_blockNum = 16 > maxSampleNum ? maxSampleNum : 16;   
		handle->m_regionSize = (handle->m_maxPage + 1 - handle->m_minPage) / handle->m_blockNum;
		
		// 如果页面太少，不足以分区，则全部采样
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
 * 得到最大的使用页
 *
 * @param session 会话对象
 * @return 最大的使用页的页号
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
 * 取得采样的下一个页面
 *
 * @param handle 采样句柄
 * @return 样本
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
		if (shdl->m_curIdxInBlock == shdl->m_blockSize - 1) { // 本block扫描完毕
			if (shdl->m_curBlock == shdl->m_blockNum - 1) { // 这是最后一个block
				return NULL;
			}
			shdl->m_curBlock++;
			shdl->m_curIdxInBlock = 0;
			u64 regionSize = (shdl->m_curBlock == shdl->m_blockNum - 1) ?
				(shdl->m_regionSize + (shdl->m_maxPage + 1 - shdl->m_minPage) % shdl->m_regionSize)
				: shdl->m_regionSize;  // 最后一个region把所有页面包含进来
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
		// 第一次sampleNext
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
 * 结束采样，释放资源 
 *
 * @param handle 采样句柄
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
 * 得到一个页面的使用的目录项个数 
 *
 * @param session 会话
 * @param page 缓存页
 * @return 一个采样
 */
Sample* LobIndex::sampleBufferPage(Session *session, BufferPageHdr *page) {
	LIFilePageInfo *liPage = (LIFilePageInfo *)page;
	Sample *sample = Sample::create(session, SAMPLE_FIELDS_NUM);
	(*sample)[SAMPLE_FIELDS_NUM - 1] = MAX_SLOT_PER_PAGE - liPage->m_freeSlotNum;
	return sample;
}


/**
 * 在一个分区里随机选择，找到N个页面，然后进行分析
 *
 * @param outPages out 数组，放选择后的页面
 * @param wantNum 需要的选择的页数
 * @param min 最小的页号
 * @param regionSize 选择分区的大小
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
 * 得到一个sample
 *
 * @param pageId 页号
 * @param session 会话对象
 * @return 一个采样
 */
Sample* LobIndex::sample(u64 pageId, Session *session) {
	BufferPageHandle *pageHdl = GET_PAGE(session, m_file, PAGE_LOB_INDEX, pageId, Shared, m_dboStats, NULL);
	Sample *sample = sampleBufferPage(session, pageHdl->getPage());
	session->releasePage(&pageHdl);

	sample->m_ID = (SampleID)pageId;
	return sample;
}

/** 
 * 寻找不可采样的页面 
 *
 * @param minPage 最小页号
 * @param regionSize 区域大小
 * @return 页面ID
 */
uint LobIndex::unsamplablePagesBetween(u64 minPage, u64 regionSize) {
	UNREFERENCED_PARAMETER(minPage);
	UNREFERENCED_PARAMETER(regionSize);
	return 0;
}
}
