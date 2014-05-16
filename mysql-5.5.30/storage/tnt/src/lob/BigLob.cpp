 /**
  * 大型大对象管理
  *
  * @author zx(zhangxiao@corp.netease.com, zx@163.org)
  */

#include "lob/BigLob.h"
#include "util/File.h"
#include "util/Thread.h"
#include "misc/Buffer.h"
#include "api/Database.h"
#include "misc/Session.h"
#include "misc/Syslog.h"
#include "misc/TableDef.h"
#include "util/Stream.h"
#include <cassert>
#include "misc/Verify.h"
#include "iostream"

#ifdef TNT_ENGINE
#include "trx/TNTTransaction.h"
#endif

using namespace std;

namespace ntse {
/**
 * Txn日志格式
 * insert log: LobId(u64) PageId(u32) Flags(u8) ( New FreePageListHeader(u32) ) isCompress(u8) (Org_len(u32)) LobLen(u32) LobData(byte*)
 * delete log: LobId(u64) PageId(u32) Flags(u8) ( Old FreePageListHeader(u32) )
 * update log: LobId(u64) PageID(u32) isNewPageID (u8) (new PageID) isNewFreeBlock(u8) (newFreeBlockPageID(u32) newFreeBlockLen(u32)) LobLen(u32)
 * move log  : LobId(u64) PageID(u32)  newPageID(u32) LobLen(u32) LobData(byte*)
 *
 */


/**
 * 创建大对象文件
 *
 * @param db 数据库对象
 * @param tableDef 所属表定义
 * @param blobPath 大对象文件完整路径
 * @param indexPath 目录文件完整路径
 * @throw 文件无法创建，IO错误等等
 */
void BigLobStorage::create(Database *db, const TableDef *tableDef, const char *basePath)  throw(NtseException) {
	string blobPath = string(basePath) + Limits::NAME_LOBD_EXT;
	string indexPath = string(basePath) + Limits::NAME_LOBI_EXT;

	//先创建大对象文件
	createLobFile(db, tableDef, blobPath.c_str());
	//创建目录文件
	try {
		LobIndex::create(indexPath.c_str(), db, tableDef);
	} catch(NtseException &e) {
		//处理已经生成的大对象文件
		File *blobFile = new File(blobPath.c_str());
		blobFile->remove();
		delete blobFile;
		throw e;
	}
}

/** 
 * 创建大型大对象文件
 *
 * @param db 数据库对象
 * @param tableDef 所属表定义
 * @param blobPath 大对象文件完整路径
 * @throw 文件无法创建，IO错误等
 */
void BigLobStorage::createLobFile(Database *db, const TableDef *tableDef, const char *blobPath) throw(NtseException){
	u64 errCode;
	// 创建大对象文件
	File *blobFile = new File(blobPath);

	errCode = blobFile->create(true, false);
	if (File::E_NO_ERROR != errCode) {
		delete blobFile;
		NTSE_THROW(errCode, "Cannot create lob file %s", blobPath);
	}

	// 这里首先初始化一个块，大小为Database::getBestIncrSize(tableDef, 0)个页
	size_t incrSize = Database::getBestIncrSize(tableDef, 0);
	byte *newMem = (byte *) System::virtualAlloc(Limits::PAGE_SIZE * incrSize);
	if (!newMem)
		db->getSyslog()->log(EL_PANIC, "Unable to alloc %d bytes", Limits::PAGE_SIZE * incrSize);
	memset(newMem, 0, Limits::PAGE_SIZE * incrSize);

	errCode = blobFile->setSize((u64)Limits::PAGE_SIZE * incrSize);
	if (File::E_NO_ERROR != errCode)
		db->getSyslog()->fopPanic(errCode, "Setting lob file '%s' size failed!", blobPath);

	errCode = blobFile->write(0, Limits::PAGE_SIZE * incrSize, newMem);
	if (File::E_NO_ERROR != errCode)
		db->getSyslog()->fopPanic(errCode, "Cannot write header pages for lob file %s", blobPath);

	System::virtualFree(newMem);
	blobFile->close();
	delete blobFile;
}

/** 
 * 构造函数
 *
 * @param db 数据库对象
 * @param file 大对象文件 
 * @param lobi 大对象目录实例
 * @param dbObjStats 数据对象状态
 */
BigLobStorage::BigLobStorage(Database *db, File *file, LobIndex *lobi, DBObjStats *dbObjStats) {
	m_db = db;
	m_buffer = db->getPageBuffer();
	m_file = file;
	m_lobi = lobi;
	memset(&m_status, 0, sizeof(BLobStatus));
	memset(&m_statusEx, 0, sizeof(BLobStatusEx));
	m_lobAverageLen = 0;
	m_sampleRatio = .0;
	m_dboStats = dbObjStats;
	m_status.m_dboStats = dbObjStats;
}

/** 
 * 析构函数
 */
BigLobStorage::~BigLobStorage() {
	delete m_file;
	delete m_lobi;
}

/**
 * 打开大对象文件
 *
 * @param db 数据库对象
 * @param session 会话对象
 * @param lobPath 大对象文件路径
 * @param indexPath 目录文件路径
 * @throw 文件无法创建，IO错误等等
 * @return BigLob对象
 */
BigLobStorage* BigLobStorage::open(Database *db, Session *session, const char *basePath) throw(NtseException) {
	u64 errCode;
	string lobPath = string(basePath) +  Limits::NAME_LOBD_EXT;
	string indexPath = string(basePath) + Limits::NAME_LOBI_EXT;
	// 打开大对象文件
	File *blobfile = new File(lobPath.c_str());

	errCode = blobfile->open(db->getConfig()->m_directIo);
	if (File::E_NO_ERROR != errCode) {
		delete blobfile;
		NTSE_THROW(errCode, "Cannot open lob file %s", lobPath.c_str());
	}

	// 生成LobIndex对象
	LobIndex *lobdex = NULL;
	try {
		lobdex = LobIndex::open(indexPath.c_str(), db, session);
	} catch (NtseException &e) {
		blobfile->close();
		delete blobfile;
		throw e;
	}
    BigLobStorage *blob = new BigLobStorage(db, blobfile, lobdex, new DBObjStats(DBO_LlobDat));
	return blob;
}

/**
 * 对大对象文件进行扩张
 *
 * @param session 会话
 * @param fileLen 原来文件长度
 * @param size 要扩张的页数
 */
void BigLobStorage::extendNewBlock(Session *session, u32 fileLen, u16 size) {
	u64 errCode;
	if (File::E_NO_ERROR != (errCode = m_file->setSize((u64)Limits::PAGE_SIZE * (u64)(fileLen + size))))
		m_db->getSyslog()->fopPanic(errCode, "Extending lob file file '%s' size failed!",m_file->getPath());

	for (uint i = 0; i < size; ++i) {
		u64 pageNum = fileLen + i; // maxPageNum还没有改变
		BufferPageHandle *pageHdl = NEW_PAGE(session, m_file, PAGE_LOB_HEAP, pageNum, Exclusived, m_dboStats);
		LobBlockFirstPage *page = (LobBlockFirstPage *)pageHdl->getPage();
		memset(page, 0, Limits::PAGE_SIZE);
		page->m_isFree = 1;
		page->m_isFirstPage = true;
		page->m_len = Limits::PAGE_SIZE;	
		session->markDirty(pageHdl);
		session->releasePage(&pageHdl);
	}
	m_buffer->batchWrite(session, m_file, PAGE_LOB_HEAP, fileLen, fileLen + size - 1);
}

/**
 * 得到插入大对象时候的得到起始ID，这里先得到首页X锁，然后得到块首页锁，然后释放首页X锁
 *
 * @post 得到块首页的X锁
 * @param tableDef 表定义
 * @param pageNum 大对象需要占有的页数
 * @param session 会话对象
 * @param pageId out 大对象的起始PageId
 * @return  大对象首页的句柄
 */
BufferPageHandle* BigLobStorage::getStartPageIDAndPage(const TableDef *tableDef, u32 pageNum, Session *session, u32 *pageId) {
	BufferPageHandle *headerPageHdl;
    LIFileHeaderPageInfo *headerPage ;
	BufferPageHandle *blockHeaderPageHdl;
	u32 fileTail, fileLen;

	headerPageHdl = m_lobi->lockHeaderPage(session, Exclusived);
	headerPage = (LIFileHeaderPageInfo *)headerPageHdl->getPage();
    fileTail = headerPage->m_blobFileTail;
	*pageId = fileTail;
	fileLen = headerPage->m_blobFileLen;

	// 判断是否需要扩展大对象文件
	// 假如当前文件尾部<大对象长度
	extendLobFile(session, tableDef, fileTail, pageNum, fileLen, headerPage);
	headerPage->m_blobFileTail += pageNum;
	// 先newPage,这个页是将要写入的大对象的块首页
	blockHeaderPageHdl = NEW_PAGE(session, m_file, PAGE_LOB_HEAP, fileTail, Exclusived, m_dboStats);
	// 释放首页
	session->markDirty(headerPageHdl);
	m_lobi->unlockHeaderPage(session, headerPageHdl);
	return blockHeaderPageHdl;
}

/**
 * 插入大对象
 *
 * @param session 会话对象
 * @param tableDef 表定义
 * @param lob 大对象内容
 * @param size 大对象的长度
 * @param orgSize 大对象的原来长度
 * @return 大对象ID
 */
LobId BigLobStorage::insert(Session *session, const TableDef *tableDef, const byte *lob, u32 size, u32 orgSize) {
	ftrace(ts.lob, tout << session << tableDef << lob << size << orgSize);
	BufferPageHandle *headerPageHdl, *bloFirstPageHdl;
	BufferPageHandle *liFilePageHdl;
	LIFileHeaderPageInfo *liFileHeaderPage;
	LIFilePageInfo *liFilePage;
	u64 lsn;
	bool isHeaderPageLock;
    LobId lid, lobId;
	u32 pageId;
	u32 newHeaderList;
	uint blockLen = getBlockSize(size);
	u32 pageNum = getPageNum(blockLen);

	// 得到块首页和起始PageId
	bloFirstPageHdl = getStartPageIDAndPage(tableDef, pageNum, session, &pageId);
	SYNCHERE(SP_LOB_BIG_INSERT_LOG_REVERSE_1);
	// 得到空闲目录项
	lid = m_lobi->getFreeSlot(session, &liFilePageHdl, &isHeaderPageLock, &headerPageHdl, &newHeaderList, pageId);
	// verify目录文件页是否正确
	lobId = lid;
	MemoryContext *mc = session->getMemoryContext();
	u64 savePoint = mc->setSavepoint();

#ifdef TNT_ENGINE
	tnt::TNTTransaction *trx = session->getTrans();
	if(trx != NULL)
		LobStorage::writeTNTInsertLob(session,trx->getTrxId(),tableDef->m_id,trx->getTrxLastLsn(),lobId);
#endif

	if (isHeaderPageLock) {// 说明在getFreeSlot中，首页是锁住的
		// 写日志
		lsn = logInsert(session, lobId, pageId, true, newHeaderList, m_lobi->getTableID(), orgSize, size, lob, mc);
		// 释放首页
        liFileHeaderPage = (LIFileHeaderPageInfo *)headerPageHdl->getPage();
        liFileHeaderPage->m_bph.m_lsn = lsn;
		session->markDirty(headerPageHdl);
		m_lobi->unlockHeaderPage(session, headerPageHdl);
	} else {
		// 写日志

		lsn = logInsert(session, lobId, pageId, false, 0, m_lobi->getTableID(), orgSize, size, lob, mc);
	}
	mc->resetToSavepoint(savePoint);

	SYNCHERE(SP_LOB_BIG_INSERT_LOG_REVERSE_2);

	// 修改目录页LSN，其余修改已经在getFreeSlot完成
	liFilePage = (LIFilePageInfo *)liFilePageHdl->getPage();
	liFilePage->m_bph.m_lsn = lsn;
	// 释放目录页
	session->markDirty(liFilePageHdl);
	session->releasePage(&liFilePageHdl);
	// 这里backup测试问题，主要验证m_isFirstPage
	//vecode(vs.lob, m_lobi->verifyPage(session, LID_GET_PAGE(lid)));

	uint haveCopyLen;
	byte *bufFirstPage;
	// 开始写数据
	LobBlockFirstPage *bloFirstPage= (LobBlockFirstPage *)bloFirstPageHdl->getPage();
	memset(bloFirstPage, 0, Limits::PAGE_SIZE);
	bloFirstPage->m_bph.m_lsn = lsn;
	bloFirstPage->m_bph.m_checksum = BufferPageHdr::CHECKSUM_NO;
	bloFirstPage->m_isFirstPage = true;
	bloFirstPage->m_isFree = false;
	bloFirstPage->m_len = blockLen;
	bloFirstPage->m_lid = lobId;
	bloFirstPage->m_srcLen = orgSize;
	haveCopyLen = Limits::PAGE_SIZE - OFFSET_BLOCK_FIRST;
	bufFirstPage = (byte *)bloFirstPage + OFFSET_BLOCK_FIRST;
	if (pageNum == 1) {// 只有一个页
		// copy首页数据
		memcpy(bufFirstPage, lob, size);
	} else {
		memcpy(bufFirstPage, lob, haveCopyLen);
		// copy其他数据页
		writeLob(session, pageId, pageNum, haveCopyLen, lsn, lob, size);
	}

	// 最后释放块首页锁
	session->markDirty(bloFirstPageHdl);
	session->releasePage(&bloFirstPageHdl);
	
	// 统计信息
	m_dboStats->countIt(DBOBJ_ITEM_INSERT);
	return lobId;
}

/**
 * 把大型大对象的长度转换成应该写入的块长度（包括页头）
 *
 * @param size 大对象字节流的长度
 * @return 写入时候的总长度（块长度）
 */
uint BigLobStorage::getBlockSize(uint size) {
	uint offset = OFFSET_BLOCK_FIRST;

	if (size <= Limits::PAGE_SIZE - offset) // 长度只到一页
		return size + offset;

	uint remainSize = size - (Limits::PAGE_SIZE - offset);
	uint pageNum = (remainSize + Limits::PAGE_SIZE - OFFSET_BLOCK_OTHER - 1) / (Limits::PAGE_SIZE - OFFSET_BLOCK_OTHER);
	// 最后一页的长度
	uint lastPageLen = remainSize - (pageNum - 1) * (Limits::PAGE_SIZE - OFFSET_BLOCK_OTHER) + OFFSET_BLOCK_OTHER;

	return pageNum * Limits::PAGE_SIZE + lastPageLen;
}

/**
 * 块的长度转换到LOB长度，这里块的长度指占物理空间长度，包括页头等
 * LOB长度指内容的实际长度
 *
 * @param size 块长度
 * @return 大对象内容长度
 */
uint BigLobStorage::getLobSize(uint size) {
	// 假如小于一页
	if (size <= Limits::PAGE_SIZE)
		return size - OFFSET_BLOCK_FIRST;

	uint pageNum = (size + Limits:: PAGE_SIZE - 1) / Limits::PAGE_SIZE;
	return size - OFFSET_BLOCK_FIRST  -  (pageNum - 1) * OFFSET_BLOCK_OTHER;
}

/**
 * 读取大对象
 *
 * @param session 会话对象
 * @param mc 内存上下文
 * @param lobId 大对象ID
 * @param size out 大对象长度
 * @param org_size out 大对象非压缩的长度
 * @return 大对象内容
 */
byte* BigLobStorage::read(Session *session, MemoryContext *mc, LobId lobId, uint *size, uint *org_size) {
	ftrace(ts.lob, tout << session << lobId);
	BufferPageHandle *liFilePageHdl;
	LIFilePageInfo *liFilePage;
	u32 pageID;
	u8 isFree;
	bool isFind;
	byte *dataStream;
	u32 pid = LID_GET_PAGE(lobId);
	u16 slotNum = LID_GET_SLOT(lobId);
check_page:
	liFilePageHdl = GET_PAGE(session, m_lobi->m_file, PAGE_LOB_INDEX, pid, Shared, m_dboStats, NULL);
	liFilePage = (LIFilePageInfo*)liFilePageHdl->getPage();
	LiFileSlotInfo *slotInfo = m_lobi->getSlot(liFilePage, slotNum);
	isFree = slotInfo->m_free;
	if(isFree) {		//如果未找到大对象则返回
		session->releasePage(&liFilePageHdl);	
		*size = 0;
		return NULL;
	}
	// 目录项必须是not free
	assert(!isFree);
	pageID = slotInfo->u.m_pageId;
	session->releasePage(&liFilePageHdl);

	// 同步点
	SYNCHERE(SP_LOB_BIG_READ_DEFRAG_READ);
	isFind = readBlockAndCheck(session, mc, pageID, lobId, &dataStream, size, org_size);
	if (isFind) {
		m_dboStats->countIt(DBOBJ_ITEM_READ);
		return dataStream;
	}
	goto check_page;
}

/**
 * 读取一个大对象内容，并判断时候是否被移动过。如果没有移动过则返回大对象内容
 *
 * @param session会话对象
 * @param mc 内存分配上下文
 * @param pid 大对象的起始页号
 * @param lid 大对象ID
 * @param dataSteam out 大对象内容
 * @param size out 大对象大小
 * @param orgSize out 假如不压缩的情况下，大对象的大小
 * @return 是否已经读取到大对象
 */
bool BigLobStorage::readBlockAndCheck(Session *session, MemoryContext *mc, u32 pid, LobId lid, byte **dataSteam, uint *size, uint *orgSize) {
	BufferPageHandle *blockHeaderPageHdl;
	LobBlockFirstPage  *blockHeaderPage;
	LobId llid;
	bool isFree;
	bool isFirstPage;

	blockHeaderPageHdl = GET_PAGE(session, m_file, PAGE_LOB_HEAP, pid, Shared, m_dboStats, NULL);
	blockHeaderPage = (LobBlockFirstPage *)blockHeaderPageHdl->getPage();
    isFree = blockHeaderPage->m_isFree;
	// assert(!isFree);
    isFirstPage = blockHeaderPage->m_isFirstPage;
    llid = blockHeaderPage->m_lid;
	// 判断IsFree、ReferenceID和IsFirstPage，假如为Isfree为false
	// 并且ReferenceID=BlobID, IsFirstPage=1则读取块的首页，得到块的长度等信息
	if (!isFree && isFirstPage && lid == llid) {
		u32 blockLen = blockHeaderPage->m_len;
		// 分配内存
		uint lobSize = getLobSize(blockLen);
		*dataSteam = (byte *)mc->alloc(lobSize);
		*orgSize = blockHeaderPage->m_srcLen;
		*size = getLobContent(session, *dataSteam, blockHeaderPage, pid);
		assert(*size == lobSize);
		// 最后释放块的首页锁
        session->releasePage(&blockHeaderPageHdl);
        return true;
	}
	*size = 0;
	*orgSize = 0;
	session->releasePage(&blockHeaderPageHdl);
	return false;
}

/**
 * 得到大对象的内容
 *
 * @param session会话对象
 * @param data out 大对象数据
 * @param blockHeaderPage 大对象块首页
 * @param pid 起始页号
 * @return 大对象长度
 */
uint BigLobStorage::getLobContent(Session *session, byte *data, LobBlockFirstPage *blockHeaderPage, u32 pid) {
	byte *firstPageAddr;
	uint firstOffset;
	uint copyLen;
	uint size;
	BufferPageHandle  *blockOtherPageHdl;
	LobBlockOtherPage  *blockOtherPage;
	u32 blockLen = blockHeaderPage->m_len;
	// 大对象占有的页数
	u32 pageNum = getPageNum(blockLen);

	firstPageAddr = (byte *)(blockHeaderPage) + OFFSET_BLOCK_FIRST;
	copyLen = Limits::PAGE_SIZE - OFFSET_BLOCK_FIRST;
	firstOffset = OFFSET_BLOCK_FIRST;
	// 假如只有一页数据
	if (pageNum == 1) {
		// copy首页数据
		memcpy(data, firstPageAddr, blockLen - firstOffset);
		size = blockLen - firstOffset;
	} else {
		// copy首页数据
		memcpy(data, firstPageAddr, copyLen);
		// copy其他页数据
		uint i = 1;
		while (i < pageNum) {
			blockOtherPageHdl = GET_PAGE(session, m_file, PAGE_LOB_HEAP, pid + i, Shared, m_dboStats, NULL);
			blockOtherPage = (LobBlockOtherPage *)blockOtherPageHdl->getPage();
			byte *otherPageData = (byte *)(blockOtherPage) + OFFSET_BLOCK_OTHER;
			// 假如到最后一个页的数据
			if (i == pageNum - 1) {
				// 最后一页数据长度
				uint lastLen =  blockLen - (pageNum - 1) * Limits::PAGE_SIZE  - OFFSET_BLOCK_OTHER;
				memcpy(data + copyLen, otherPageData, lastLen);
				copyLen += lastLen;
			} else {
				memcpy(data + copyLen, otherPageData, (Limits::PAGE_SIZE - OFFSET_BLOCK_OTHER));
				copyLen += Limits::PAGE_SIZE - OFFSET_BLOCK_OTHER;
			}
			// unlock 这些数据页
			session->releasePage(&blockOtherPageHdl);
			i++;
		}
		size = copyLen;
	}
	return size;
}

/**
 * 删除大对象
 *
 * @param session会话对象
 * @param LobId 大对象ID
 */
void BigLobStorage::del(Session *session, LobId lobId) {
	ftrace(ts.lob, tout << session << lobId);
	BufferPageHandle *liFilePageHdl;
	LIFilePageInfo *liFilePage;
	u32 pageID;
	bool isFind;
	u32 pid = LID_GET_PAGE(lobId);
	u16 slotNum = LID_GET_SLOT(lobId);
check_page:
	liFilePageHdl = GET_PAGE(session, m_lobi->m_file, PAGE_LOB_INDEX, pid, Shared, m_dboStats, NULL);
	liFilePage = (LIFilePageInfo *)liFilePageHdl->getPage();
	LiFileSlotInfo *slotInfo = m_lobi->getSlot(liFilePage, slotNum);
	pageID = slotInfo->u.m_pageId;
	assert(!slotInfo->m_free);
	session->releasePage(&liFilePageHdl);
	SYNCHERE(SP_LOB_BIG_DEL_DEFRAG_DEL);
	isFind = delBlockAndCheck(session, pageID, lobId, m_lobi->getTableID());
	SYNCHERE(SP_LOB_BIG_OTHER_PUT_FREE_SLOT);
	if (isFind) {
		m_dboStats->countIt(DBOBJ_ITEM_DELETE);
		return;
	}
	goto check_page;
}

#ifdef TNT_ENGINE
/**
 * 删除大对象  用于crash恢复
 *
 * @param session会话对象
 * @param LobId 大对象ID
 */
void BigLobStorage::delAtCrash(Session *session, LobId lobId) {
	ftrace(ts.lob, tout << session << lobId);
	BufferPageHandle *liFilePageHdl;
	LIFilePageInfo *liFilePage;
	u32 pageID;
	bool isFind;
	u32 pid = LID_GET_PAGE(lobId);
	u16 slotNum = LID_GET_SLOT(lobId);
check_page:
	liFilePageHdl = GET_PAGE(session, m_lobi->m_file, PAGE_LOB_INDEX, pid, Shared, m_dboStats, NULL);
	liFilePage = (LIFilePageInfo *)liFilePageHdl->getPage();
	LiFileSlotInfo *slotInfo = m_lobi->getSlot(liFilePage, slotNum);
	pageID = slotInfo->u.m_pageId;
	if(slotInfo->m_free) {		//如果未找到大对象则返回
		session->releasePage(&liFilePageHdl);	
		return;
	}
	session->releasePage(&liFilePageHdl);
	SYNCHERE(SP_LOB_BIG_DEL_DEFRAG_DEL);
	isFind = delBlockAndCheck(session, pageID, lobId, m_lobi->getTableID());
	SYNCHERE(SP_LOB_BIG_OTHER_PUT_FREE_SLOT);
	if (isFind) {
		m_dboStats->countIt(DBOBJ_ITEM_DELETE);
		return;
	}
	goto check_page;
}
#endif

/**
 * 删除一个大对象，并判断时候是否被移动过。如果没有移动过则删除
 *
 * @param session会话对象
 * @param pid 大对象的起始页号
 * @param lid 大对象ID
 * @param tid 表ID
 * @return 是否已经成功删除大对象
 */
bool BigLobStorage::delBlockAndCheck(Session *session, u32 pid, LobId lid, u16 tid) {
	ftrace(ts.lob, tout << lid << pid);
	BufferPageHandle *blockHeaderPageHdl, *liFilePageHdl;
	LobBlockFirstPage *blockHeaderPage;
	LIFilePageInfo *liFilePage;
	LobId llid;
	bool isFree;
	bool isFirstPage;

	blockHeaderPageHdl = GET_PAGE(session, m_file, PAGE_LOB_HEAP, pid, Exclusived, m_dboStats, NULL);
	blockHeaderPage = (LobBlockFirstPage *)blockHeaderPageHdl->getPage();
	isFree = blockHeaderPage->m_isFree;
	isFirstPage = blockHeaderPage->m_isFirstPage;
	llid = blockHeaderPage->m_lid;
	// 判断IsFree、ReferenceID和IsFirstPage，假如为Isfree为false
	// 并且ReferenceID=BlobID, IsFirstPage=1则读取块的首页，得到块的长度等信息
	if (!isFree && isFirstPage && lid == llid) {
		blockHeaderPage->m_isFree = true;
		// 去读取对应的目录项页,并修改
		u32 indexPid = LID_GET_PAGE(lid);
		u16 slotNum = LID_GET_SLOT(lid);
		u64 newlsn = 0;
		BufferPageHandle *headPageHdl;
		liFilePageHdl = GET_PAGE(session, m_lobi->m_file, PAGE_LOB_INDEX, indexPid, Exclusived, m_dboStats, NULL);
		liFilePage = (LIFilePageInfo *)liFilePageHdl->getPage();
		LiFileSlotInfo *slotInfo = m_lobi->getSlot(liFilePage, slotNum);
		slotInfo->m_free = true;
		// 假如该目录页是第一次有空闲槽,则需要加入空闲页链表
		if (liFilePage->m_inFreePageList == false) {
			// 得到首页,设置空闲页链表
			headPageHdl = m_lobi->lockHeaderPage(session, Exclusived);
			LIFileHeaderPageInfo *headPage = (LIFileHeaderPageInfo *)headPageHdl->getPage();
			u32 oldListHeader = headPage->m_firstFreePageNum;
			liFilePage ->m_nextFreePageNum = oldListHeader;
			headPage->m_firstFreePageNum = indexPid;
			newlsn = logDelete(session, lid, pid, true, oldListHeader, tid);
			headPage->m_bph.m_lsn = newlsn;
			liFilePage->m_inFreePageList = true;
			liFilePage->m_freeSlotNum = 1;
			slotInfo->u.m_nextFreeSlot = -1;
		} else {// 修改页内空闲链表
			newlsn = logDelete(session, lid, pid, false, 0, tid);
			headPageHdl = m_lobi->lockHeaderPage(session, Exclusived);
			slotInfo->u.m_nextFreeSlot = liFilePage->m_firstFreeSlot;
			liFilePage->m_freeSlotNum += 1;
		}
		// 统计信息
		LIFileHeaderPageInfo *headPage = (LIFileHeaderPageInfo *)headPageHdl->getPage();
		headPage->m_blobFileFreeBlock += 1;
		headPage->m_blobFileFreeLen += getPageNum(blockHeaderPage->m_len);
		session->markDirty(headPageHdl);
		m_lobi->unlockHeaderPage(session, headPageHdl);

		liFilePage->m_firstFreeSlot = slotNum;
		liFilePage->m_bph.m_lsn = newlsn;
		session->markDirty(liFilePageHdl);
		session->releasePage(&liFilePageHdl);

		blockHeaderPage->m_bph.m_lsn = newlsn;
		session->markDirty(blockHeaderPageHdl);
		session->releasePage(&blockHeaderPageHdl);
		//vecode(vs.lob, m_lobi->verifyPage(session, LID_GET_PAGE(lid)));
		return true;
	}
	session->releasePage(&blockHeaderPageHdl);
	return false;
}

/**
 * 更新大对象
 *
 * @param session会话对象
 * @param tableDef 表定义
 * @param lobId 大对象ID
 * @param lob  大对象新的内容
 * @param size 大对象长度
 * @param orgSize 压缩前的大对象大小
 */
void BigLobStorage::update(Session *session, const TableDef *tableDef, LobId lobId, const byte *lob, uint size, uint orgSize) {
	ftrace(ts.lob, tout << session << tableDef << lobId << lob << size << orgSize);
	BufferPageHandle *liFilePageHdl;
	LIFilePageInfo *liFilePage;
	u32 pageID;
	bool isFind;
	u32 pid = LID_GET_PAGE(lobId);
	u16 slotNum = LID_GET_SLOT(lobId);

	while (1) {
		liFilePageHdl = GET_PAGE(session, m_lobi->m_file, PAGE_LOB_INDEX, pid, Shared, m_dboStats, NULL);
		liFilePage = (LIFilePageInfo *)liFilePageHdl->getPage();
		LiFileSlotInfo *slotInfo = m_lobi->getSlot(liFilePage,slotNum);
		pageID = slotInfo->u.m_pageId;
		session->releasePage(&liFilePageHdl);
		SYNCHERE(SP_LOB_BIG_DEL_DEFRAG_UPDATE);
		isFind = updateBlockAndCheck(session, tableDef, pageID, lobId, lob, size, orgSize);
		if (isFind) {
			m_dboStats->countIt(DBOBJ_ITEM_UPDATE);
			return;
		}
	}
}

/**
 * 写大对象到页
 *
 * @pre 写入首页已经完成，这里完成其他页
 * @param session 会话对象
 * @param pid 开始的页号
 * @param pageNum 大对象除了首页剩下的页数
 * @param offset 已经在首页写的数据长度
 * @param lsn 写入的lsn
 * @param lob 大对象内容
 * @param size 大对象长度
 */
void BigLobStorage::writeLob(Session *session, u32 pid, uint pageNum, uint offset, u64 lsn, const byte *lob, uint size) {
	uint copyLen;
	copyLen = offset;
	for(uint i = 1; i < pageNum; i++) {
		BufferPageHandle *bloOtherPageHdl = NEW_PAGE(session, m_file, PAGE_LOB_HEAP, pid + i, Exclusived, m_dboStats);
		LobBlockOtherPage *lobOtherPage =(LobBlockOtherPage *)bloOtherPageHdl->getPage();
		memset(lobOtherPage, 0, Limits::PAGE_SIZE);
		lobOtherPage->m_bph.m_lsn = lsn;
		lobOtherPage->m_isFirstPage = 0;
		if(i == pageNum - 1) {// 是最后一页
			memcpy((byte *)(lobOtherPage) + OFFSET_BLOCK_OTHER, lob + copyLen, size - copyLen);
		} else {
			memcpy((byte *)(lobOtherPage) + OFFSET_BLOCK_OTHER, lob + copyLen, Limits::PAGE_SIZE - OFFSET_BLOCK_OTHER);
			copyLen += Limits::PAGE_SIZE - OFFSET_BLOCK_OTHER;
		}
		session->markDirty(bloOtherPageHdl);
		session->releasePage(&bloOtherPageHdl);
	}
}

/**
 * 更新一个大对象，先判断时候是否被移动过。如果没有移动过则更新大对象
 *
 * @param session会话对象
 * @param tableDef 表定义
 * @param pid 大对象的起始页号
 * @param lid 大对象ID
 * @param lob 大对象内容
 * @param size 大对象长度
 * @param orgSize 大对象压缩前长度
 * @return 是否已经成功更新对象
 */
bool BigLobStorage::updateBlockAndCheck(Session *session, const TableDef *tableDef, u32 pid, LobId lid, const byte *lob, uint size, uint orgSize) {
	ftrace(ts.lob, tout << lid << pid);
	BufferPageHandle *blockHeaderPageHdl;
	LobBlockFirstPage *blockHeaderPage;
	LobId llid;
	bool isFree;
	bool isFirstPage;
	u32 preBlockLen;
	u32 newBlockLen;
	u64 lsn = 0;

	blockHeaderPageHdl = GET_PAGE(session, m_file, PAGE_LOB_HEAP, pid, Exclusived, m_dboStats, NULL);
	blockHeaderPage = (LobBlockFirstPage *)blockHeaderPageHdl->getPage();
	isFree = blockHeaderPage->m_isFree;
	isFirstPage = blockHeaderPage->m_isFirstPage;
	llid = blockHeaderPage->m_lid;
	// 判断IsFree、ReferenceID和IsFirstPage，假如为Isfree为false
	// 并且ReferenceID=BlobID, IsFirstPage=1则读取块的首页，得到块的长度等信息
	if (!isFree && isFirstPage && lid == llid) {
		LobId newLid = lid;
		uint haveCopyLen = Limits::PAGE_SIZE - OFFSET_BLOCK_FIRST;
		uint offset = OFFSET_BLOCK_FIRST;

		// 先读取原大对象长度
		preBlockLen = blockHeaderPage->m_len;
		newBlockLen = getBlockSize(size);
		u32 prePageNum = getPageNum(preBlockLen);
		u32 newPageNum = getPageNum(newBlockLen);
		// 假如大对象占有的页数没变或者小于
		if (prePageNum >= newPageNum) {
			// 假如大对象占有的页数没变
			if (prePageNum == newPageNum) {
				lsn = logUpdate(session, newLid, pid, false, 0, false, 0, 0, size, m_lobi->getTableID(), orgSize);
			} else {
				// 计算出空闲块
				int newFreeBlockStartPageID = pid + newPageNum;
				// 写日志
				lsn = logUpdate(session, newLid, pid, false, 0, true, newFreeBlockStartPageID, prePageNum - newPageNum, size, m_lobi->getTableID(), orgSize);
				BufferPageHandle *newBlockHeaderPageHdl = NEW_PAGE(session, m_file, PAGE_LOB_HEAP, newFreeBlockStartPageID, Exclusived, m_dboStats);
				LobBlockFirstPage *newBlockHeaderPage = (LobBlockFirstPage *)newBlockHeaderPageHdl->getPage();
				createNewFreeBlock(session, newBlockHeaderPageHdl, newBlockHeaderPage, lsn, prePageNum - newPageNum);
			}
			blockHeaderPage->m_srcLen = orgSize;

			// 修改数据
			blockHeaderPage->m_bph.m_lsn = lsn;
			blockHeaderPage->m_lid = newLid;
			blockHeaderPage->m_len = newBlockLen;
			if (newPageNum == 1) {// 只有一个页
				// copy首页数据
				memcpy((byte *)(blockHeaderPage) + offset, lob, size);
			} else {
				memcpy((byte *)(blockHeaderPage) + offset, lob, haveCopyLen);
				// copy其他数据页
				writeLob(session, pid, newPageNum, haveCopyLen, lsn, lob, size);
			}
			// 最后释放块首页
			session->markDirty(blockHeaderPageHdl);
			session->releasePage(&blockHeaderPageHdl);

			// 增加统计信息
			if (prePageNum > newPageNum) {
				BufferPageHandle *headPageHdl = m_lobi->lockHeaderPage(session, Exclusived);
				LIFileHeaderPageInfo *headPage = (LIFileHeaderPageInfo *)headPageHdl->getPage();
				headPage->m_blobFileFreeBlock += 1;
				headPage->m_blobFileFreeLen += (prePageNum - newPageNum);
				session->markDirty(headPageHdl);
				m_lobi->unlockHeaderPage(session, headPageHdl);
			}
			//vecode(vs.lob, verifyLobPage(session, pid));
			return true;
		}

		// 大对象变大了
		if (preBlockLen < newBlockLen) {
			BufferPageHandle  *headPageHdl = m_lobi->lockHeaderPage(session, Exclusived);
			LIFileHeaderPageInfo *headPage = (LIFileHeaderPageInfo *)headPageHdl->getPage();
			uint fileTail = headPage->m_blobFileTail;
			// 这里分四种情况
			// 1、修改大对象在文件末尾
			if (pid + prePageNum == fileTail) {
				u32 newfileTail = newPageNum - prePageNum + fileTail;
				u32 fileLen =  headPage->m_blobFileLen;
				// 判断是否需要扩展大对象文件
				// 假如当前文件尾部<大对象长度
				extendLobFile(session, tableDef, newfileTail, 0, fileLen, headPage);
				headPage->m_blobFileTail = newfileTail;
				session->markDirty(headPageHdl);
				m_lobi->unlockHeaderPage(session, headPageHdl);
				// 写日志
				lsn = logUpdate(session, newLid, pid, false, 0, false, 0, 0, size, m_lobi->getTableID(), orgSize);
				// 修改首页数据
				blockHeaderPage->m_srcLen = orgSize;
				blockHeaderPage->m_bph.m_lsn = lsn;
				blockHeaderPage->m_lid = newLid;
				blockHeaderPage->m_len = newBlockLen;
				// 开始写数据，这里的数据肯定是超过一页的长度
				memcpy((byte *)(blockHeaderPage) + offset, lob, haveCopyLen);
				// copy其他数据页
				writeLob(session, pid, newPageNum, haveCopyLen, lsn, lob, size);

				// 最后释放块首页
				session->markDirty(blockHeaderPageHdl);
				session->releasePage(&blockHeaderPageHdl);
				//vecode(vs.lob, verifyLobPage(session, pid));
				return true;
			}

			// m_lobi->unlockHeaderPage(session, headPageHdl);
			// 2、假如不是最后一块，但PageId+新的块长>文件末尾,这种情况可能和碎片整理一样
			// 于插入流程锁顺序相反容易发生死锁，但这里在判断后面块是否到了文件末尾时候都
			// 是先去锁首页判断，所以不会先去锁正在插入的新块首页，所以不形成死锁。
			// 这里改成直接移动到文件末尾
			if (pid + newPageNum > fileTail) {
				m_lobi->unlockHeaderPage(session, headPageHdl);
				assert(pid + prePageNum < fileTail);
				goto moveToTail;
			}

			// 3、假如不是最后一块，但PageId+新的块长<文件末尾,则探寻后面时候有空闲块，
			// 有足够长的时候将写入
			// 4、如果没有足够长的空闲块将把新数据写到文件末尾
			if (pid + newPageNum <= fileTail) {
				u32 newFreeBlockLen;
				u8 isFind = isNextFreeBlockEnough(session, pid, prePageNum, newPageNum - prePageNum, &newFreeBlockLen);
				// 大对象放在原处
				if (isFind) {
					if (isFind == 1) {// 新的空闲块产生
						// 写日志
						lsn = logUpdate(session, lid, pid, false, 0, true, pid + newPageNum, newFreeBlockLen, size, m_lobi->getTableID(), orgSize);
						BufferPageHandle *newFreeBlockHeaderPageHdl = NEW_PAGE(session, m_file, PAGE_LOB_HEAP, pid + newPageNum, Exclusived, m_dboStats);
						LobBlockFirstPage *newFreeBlockHeaderPage =(LobBlockFirstPage *)newFreeBlockHeaderPageHdl->getPage();
						// 生成新的空闲块
						createNewFreeBlock(session, newFreeBlockHeaderPageHdl, newFreeBlockHeaderPage, lsn, newFreeBlockLen);
					}
					if (isFind == 2) {// 没有新的空闲块产生，那几个连续空闲块刚好等于扩展的长度
						// 写日志
						lsn = logUpdate(session, newLid, pid, false, 0 ,false, 0, 0, size, m_lobi->getTableID(), orgSize);
					}

					// 开始写数据，这里的数据肯定是超过一页的长度
					blockHeaderPage->m_bph.m_lsn = lsn;
					blockHeaderPage->m_len = newBlockLen;
					blockHeaderPage->m_lid = newLid;
					blockHeaderPage->m_srcLen = orgSize;
					memcpy((byte *)(blockHeaderPage) + offset, lob, haveCopyLen);
					// copy其他数据页
					writeLob(session, pid, newPageNum, haveCopyLen, lsn, lob, size);
					// 最后释放块首页
					session->markDirty(blockHeaderPageHdl);
					session->releasePage(&blockHeaderPageHdl);

					// 统计数据
					// BufferPageHandle *headPageHdl = m_lobi->lockHeaderPage(session, Exclusived);
					LIFileHeaderPageInfo *headPage = (LIFileHeaderPageInfo *)headPageHdl->getPage();
					headPage->m_blobFileFreeBlock += 1;
					headPage->m_blobFileFreeLen -= (newPageNum - prePageNum);
					session->markDirty(headPageHdl);
					m_lobi->unlockHeaderPage(session, headPageHdl);

					//vecode(vs.lob, verifyLobPage(session, pid));
					return true;
				}
				// 新数据放到文件末尾
				m_lobi->unlockHeaderPage(session, headPageHdl);
				goto moveToTail;
				// m_lobi->unlockHeaderPage(session, headPageHdl);   不会执行到这里！
			}
			// m_lobi->unlockHeaderPage(session, headPageHdl);   不会执行到这里！
moveToTail:
			u32 newPid;
			BufferPageHandle *newBlockFirstPageHdl = getStartPageIDAndPage(tableDef, newPageNum, session, &newPid);
			// 因为要修改对应的起始PageID,所以要去修改对应目录页
			u32 indexPid = LID_GET_PAGE(lid);
			BufferPageHandle *liFilePageHdl = GET_PAGE(session, m_lobi->m_file, PAGE_LOB_INDEX, indexPid, Exclusived, m_dboStats, NULL);
			LIFilePageInfo *liFilePage = (LIFilePageInfo *)liFilePageHdl->getPage();
			// 写日志
			lsn = logUpdate(session, newLid, pid, true, newPid, true, pid, prePageNum, size, m_lobi->getTableID(), orgSize);
			liFilePage->m_bph.m_lsn = lsn;
			u16 slotNum = LID_GET_SLOT(lid);
			LiFileSlotInfo *slotInfo=m_lobi->getSlot(liFilePage,slotNum);
			slotInfo->u.m_pageId = newPid;
			// 释放目录页
			session->markDirty(liFilePageHdl);
			session->releasePage(&liFilePageHdl);
			// 开始写数据，这里的数据肯定是超过一页的长度
			LobBlockFirstPage *newDataBlockHeaderPage = (LobBlockFirstPage *)newBlockFirstPageHdl->getPage();
			memset(newDataBlockHeaderPage, 0, Limits::PAGE_SIZE);
			newDataBlockHeaderPage->m_bph.m_lsn = lsn;
			newDataBlockHeaderPage->m_len = newBlockLen;
			newDataBlockHeaderPage->m_isFirstPage = true;
			newDataBlockHeaderPage->m_lid = newLid;
			newDataBlockHeaderPage->m_isFree = false;
			newDataBlockHeaderPage->m_srcLen = orgSize;
			memcpy((byte *)(newDataBlockHeaderPage) + offset, lob, haveCopyLen);
			// copy其他数据页
			writeLob(session, newPid, newPageNum, haveCopyLen, lsn, lob, size);
			// 释放新块首页
			session->markDirty(newBlockFirstPageHdl);
			session->releasePage(&newBlockFirstPageHdl);
			// 最后释放旧块首页
			blockHeaderPage->m_bph.m_lsn = lsn;
			blockHeaderPage->m_isFree = true;
			session->markDirty(blockHeaderPageHdl);
			session->releasePage(&blockHeaderPageHdl);

			// 统计数据
			headPageHdl = m_lobi->lockHeaderPage(session, Exclusived);
			headPage = (LIFileHeaderPageInfo *)headPageHdl->getPage();
			headPage->m_blobFileFreeBlock += 1;
			headPage->m_blobFileFreeLen += prePageNum;
			session->markDirty(headPageHdl);
			m_lobi->unlockHeaderPage(session, headPageHdl);

			// vecode(vs.lob, m_lobi->verifyPage(session, LID_GET_PAGE(lid)));
			//vecode(vs.lob, verifyLobPage(session, pid));
			//vecode(vs.lob, verifyLobPage(session, newPid));
			m_status.m_moveUpdate += 1;
			return true;
		}
	} else {
		session->releasePage(&blockHeaderPageHdl);
		return false;
	}
	return true;
}

/**
 * 产生新的空闲大对象
 *
 * @param session 会话对象
 * @param blockHeaderPageHdl 块首页的句柄
 * @param lobFirstPage 块的首页
 * @param lsn LSN
 * @param freePageNum 空闲快的页数
 */
void BigLobStorage::createNewFreeBlock(Session *session, BufferPageHandle *blockHeaderPageHdl, LobBlockFirstPage *lobFirstPage, u64 lsn, u32 freePageNum) {
	memset(lobFirstPage, 0, Limits::PAGE_SIZE);
	lobFirstPage->m_bph.m_lsn = lsn;
	lobFirstPage->m_isFree = true;
	lobFirstPage->m_isFirstPage = true;
	lobFirstPage->m_len = freePageNum * Limits::PAGE_SIZE;
	session->markDirty(blockHeaderPageHdl);
	session->releasePage(&blockHeaderPageHdl);
}


/**
 * 判断一个块后面的连续空闲空闲是否大于一定长度，这个在大对象更新时候用
 *
 * @param session 会话对象
 * @param pid 大对象的起始PageId
 * @param pageNum 大对象占有的页数
 * @param needLen 更新变长的长度需要的页数
 * @param newBlockLen out 新产生的空闲块的页数
 * @return 这里返回三种情况：0表示没找到后面足够大的空闲空间
 *         1表示找到新空闲块产生，但产生新的空闲块
 *         2表示找到足够空闲块，不产生新空闲块
 */
u8 BigLobStorage::isNextFreeBlockEnough(Session *session, u32 pid , u32 pageNum, u32 needLen, u32 *newBlockLen) {
	u32 i = 0;
	uint nextBlockStartPageID = pid + pageNum ;
	BufferPageHandle *freeBlockHeaderPageHdl = TRY_GET_PAGE(session, m_file, PAGE_LOB_HEAP, nextBlockStartPageID, Shared, m_dboStats);
	if (freeBlockHeaderPageHdl == NULL) return 0;

	LobBlockFirstPage *freeBlockHeaderPage = (LobBlockFirstPage *)freeBlockHeaderPageHdl->getPage();
	if (!freeBlockHeaderPage->m_isFree) {
		session->releasePage(&freeBlockHeaderPageHdl);
		return 0;
	}
	u32  blockLen = freeBlockHeaderPage->m_len;
	session->releasePage(&freeBlockHeaderPageHdl);
	u32  bloPages = getPageNum(blockLen);
	i += bloPages;
	nextBlockStartPageID += bloPages;
	// 下面依次读取后面块，得到是否是空闲，并判断是否满足需要长度
	while (i < needLen)  {
		freeBlockHeaderPageHdl = GET_PAGE(session, m_file, PAGE_LOB_HEAP, nextBlockStartPageID, Shared, m_dboStats, NULL);
		freeBlockHeaderPage = (LobBlockFirstPage *)freeBlockHeaderPageHdl->getPage();
		if (freeBlockHeaderPage->m_isFree) {
			blockLen = freeBlockHeaderPage->m_len;
			bloPages = getPageNum(blockLen);
			i += bloPages;
			nextBlockStartPageID += bloPages;
			session->releasePage(&freeBlockHeaderPageHdl);
		} else {
			session->releasePage(&freeBlockHeaderPageHdl);
			break;
		}
	}
	// 说明后面连续空闲空间是够的
	if (i == needLen)
		return 2;
	else if (i > needLen) {
		*newBlockLen = i - needLen;
		return 1;
	}
	return 0;
}


/**
 * 写INSERT日志
 * 这里因为需要把目录文件和大对象文件的修改放在一起,所以需要记录两个文件的变化情况
 *
 * @param session 会话对象
 * @param lid 大对象ID
 * @param pid 大对象的起始页
 * @param headerPageModified 空闲链表是否改变
 * @param newListHeader 新的空闲页链表头
 * @param tid 表ID
 * @param orgLen 压缩前长度
 * @param lobSize 大对象长度
 * @param lobData 大对象内容
 * @param mc 内存上下文
 * @return lsn
 */
u64 BigLobStorage::logInsert(Session *session, LobId lid, u32 pid, bool headerPageModified, u32 newListHeader, u16 tid, u32 orgLen, u32 lobSize,const byte* lobData, MemoryContext *mc) {
	// 先分配内存
	byte *logData = (byte *)mc->alloc(32 + lobSize);
	Stream s(logData, 32 + lobSize);
	try {
		// LobId
		s.write(lid);
		// PageId
		s.write(pid);
		// flags, 目录文件头页是否被改变
		s.write(headerPageModified);
		if (headerPageModified) s.write(newListHeader);
		s.write(orgLen);
		// data长度和data
		s.write(lobSize)->write(lobData, lobSize);
	} catch (NtseException &) {
		assert(false);	// 这里不可能出现异常
	}
	return session->writeLog(LOG_LOB_INSERT, tid, logData, s.getSize());

}

/**
 * 写DELETE日志
 * 这里因为需要把目录文件和大对象文件的修改放在一起,所以需要记录两个文件的变化情况
 *
 * @param session 会话对象
 * @param lid 大对象ID
 * @param pid 大对象的起始页
 * @param headerPageModified 空闲链表是否改变
 * @param oldListHeader 老的空闲页链表头
 * @param tid 表ID
 * @return lsn
 */
u64 BigLobStorage::logDelete(Session *session, LobId lid, u32 pid, bool headerPageModified, u32 oldListHeader, u16 tid) {
	byte logData[32];
	Stream s(logData, sizeof(logData));
	try {
		// LobId
		s.write(lid);
		// PageId
		s.write(pid);
		// flags, 目录文件头页是否被改变
		s.write(headerPageModified);
		if (headerPageModified) s.write(oldListHeader);
	} catch (NtseException &) {
		assert(false);	// 这里不可能出现异常
	}
	return session->writeLog(LOG_LOB_DELETE, tid, logData, s.getSize());
}

/**
 * 写UPDATE日志
 * 这里因为需要把目录文件和大对象文件的修改放在一起,所以需要记录两个文件的变化情况
 *
 * @param session 会话对象
 * @param lid 大对象ID
 * @param pid 大对象的原来起始页号
 * @param isNewPageId 是否大对象的起始页有变化
 * @param newPageId 更新后大对象的起始页号
 * @param isNewFreeBlock 是否产生新的空闲块
 * @param freeBlockPageId 新的空闲块的起始页号
 * @param freeBlockLen 新的空闲块占有的页数
 * @param lobSize 更新后大对象的长度
 * @param tid 表ID
 * @param org_size 大对象压缩前长度
 * @return lsn
 */
u64 BigLobStorage::logUpdate(Session *session, LobId lid, u32 pid, bool isNewPageId, u32 newPageId, bool isNewFreeBlock, u32 freeBlockPageId, u32 freeBlockLen, u32 lobSize, u16 tid, u32 org_size) {
	byte logData[36];
	Stream s(logData, sizeof(logData));
	try {
		// LobId
		s.write(lid);
		// PageId
		s.write(pid);
		// flags, 起始页号是否被改变
		s.write(isNewPageId);
		if (isNewPageId) s.write(newPageId);
		// flags,是否产生新的空闲块
		s.write(isNewFreeBlock);
		if (isNewFreeBlock) {
			s.write(freeBlockPageId);
			s.write(freeBlockLen);
		}
		s.write(lobSize);
		s.write(org_size);
	} catch (NtseException &) {
		assert(false);	// 这里不可能出现异常
	}
	return session->writeLog(LOG_LOB_UPDATE, tid, logData, s.getSize());
}

/**
 * 删除大对象文件和目录文件
 *
 * @param lobPath 大对象文件路径
 * @param indexPath 目录文件路径
 * @throw 文件无法创建，IO错误等
 */
void BigLobStorage::drop(const char *basePath) throw(NtseException) {
	u64 errCode;
	string blobPath =  string(basePath) + Limits::NAME_LOBD_EXT;
	string indexPath = string(basePath) + Limits::NAME_LOBI_EXT;
	File indexFile(indexPath.c_str());
	errCode = indexFile.remove();
	if (File::getNtseError(errCode) == File::E_NOT_EXIST)
		goto Delete_BigLob_File;
	if (File::E_NO_ERROR != errCode) {
		NTSE_THROW(errCode, "Cannot drop lob index file for path %s", indexPath.c_str());
	}
Delete_BigLob_File:
	File lobFile(blobPath.c_str());
	errCode = lobFile.remove();
	if (File::getNtseError(errCode) == File::E_NOT_EXIST)
		return;
	if (File::E_NO_ERROR != errCode) {
		NTSE_THROW(errCode, "Cannot drop lob file for path %s", blobPath.c_str());
	}
}

/**
 * 关闭大对象存储
 *
 * @param session 会话
 * @param flushDirty 是否写出脏数据
 */
void BigLobStorage::close(Session *session, bool flushDirty) {
	u64 errCode;
	if (!m_file) return;
	m_buffer->freePages(session, m_file, flushDirty);
	if (File::E_NO_ERROR != (errCode = m_file->close())) {
		m_db->getSyslog()->fopPanic(errCode, "Closing blob heap file %s failed.", m_file->getPath());
	}
	delete m_file;
	m_file = NULL;
	// 然后关闭目录文件
	m_lobi->close(session, flushDirty);
	delete m_lobi;
	m_lobi = NULL;
	delete m_dboStats;
	m_dboStats = NULL;
}

/** 
 * 重做大对象创建
 *
 * @param db 数据库
 * @param tableDef 表定义
 * @param lobPath 大对象路径
 * @param indexPath 索引路径
 * @param tid 表ID
 * @throw 文件无法创建，IO错误等
 */
void BigLobStorage::redoCreate(Database *db, const TableDef *tableDef, const char *basePath, u16 tid) throw(NtseException) {
	string blobPath =  string(basePath) + Limits::NAME_LOBD_EXT;
	string indexPath = string(basePath) + Limits::NAME_LOBI_EXT;

	// 先redoCreate 目录文件
	LobIndex::redoCreate(db, tableDef, indexPath.c_str(), tid);
	u64 errCode;
	u64 fileSize;

	// 在redo大对象文件
	File lobFile(blobPath.c_str());
	bool lobExist;
	lobFile.isExist(&lobExist);

	if (!lobExist) {
		BigLobStorage::createLobFile(db, tableDef, blobPath.c_str());
		return;
	}
	if (File::E_NO_ERROR != (errCode = lobFile.open(true))) {
		NTSE_THROW(errCode, "Cannot open lob file %s", blobPath.c_str());
	}
	lobFile.getSize(&fileSize);
	if (fileSize <= Database::getBestIncrSize(tableDef, 0) * Limits::PAGE_SIZE) { // 成立说明还没插入过记录，redo一下好了
		lobFile.close();
		lobFile.remove();
		BigLobStorage::createLobFile(db, tableDef, blobPath.c_str());
	} else {
		lobFile.close();
	}
}

/**
 * 故障恢复时REDO大型大对象插入操作
 *
 * @param session 会话对象
 * @param tableDef 表定义
 * @param lsn 日志LSN
 * @param log 记录插入操作日志内容
 * @param size 日志大小
 * @return 大对象ID
 */
LobId BigLobStorage::redoInsert(Session *session, const TableDef *tableDef, u64 lsn, const byte *log, uint size) {
	/* LobId(u64) PageId(u32) Flags(u8) ( New FreePageListHeader(u32) ) Org_len(u32) LobLen(u32) LobData(byte*) */
	bool needRedo = false;
	bool flags = false, isHeaderPageModified = false;
	u32 dataLen = 0, orgLen = 0;
	LobId lobId = 0;
	u32 pid = 0, pageNum = 0, newListHeader = 0, pageId = 0;
	BufferPageHandle *pageHdl;
	LIFilePageInfo *page;
	BufferPageHandle *headerPageHdl;
	LIFileHeaderPageInfo *headerPage;
	BufferPageHandle* lobFirstHeaderPageHdl;
	Stream s((byte *)log, size);

	// 读取数据
	s.read(&lobId)->read(&pageId)->read(&flags);
	if (flags) 
		s.read(&newListHeader);
	s.read(&orgLen);
	s.read(&dataLen);

    // 先判断要不要重做,这里先判断目录文件页是不是需要redo
    pid = LID_GET_PAGE(lobId);
	if (pid >= m_lobi->m_headerPage->m_fileLen) {//说明需要扩张目录文件
		needRedo = true;
		headerPageHdl = m_lobi->lockHeaderPage(session, Exclusived);
		headerPage = (LIFileHeaderPageInfo *)headerPageHdl->getPage();
		m_lobi->extendIndexFile(session, headerPage, LOB_INDEX_FILE_EXTEND, headerPage->m_fileLen);
		session->markDirty(headerPageHdl);
		m_lobi->unlockHeaderPage(session, headerPageHdl);

		pageHdl = GET_PAGE(session, m_lobi->m_file, PAGE_LOB_INDEX, pid, Exclusived, m_dboStats, NULL);
		page = (LIFilePageInfo *)pageHdl->getPage();
	} else {
		pageHdl = GET_PAGE(session, m_lobi->m_file, PAGE_LOB_INDEX, pid, Exclusived, m_dboStats, NULL);
		page = (LIFilePageInfo *)pageHdl->getPage();
		if (page->m_bph.m_lsn < lsn)
			needRedo = true;
	}

	if (needRedo) {
		s16 slotNum = LID_GET_SLOT(lobId);
		/* 先处理空闲槽链表 */
		LiFileSlotInfo *liFileSlot = m_lobi->getSlot(page, slotNum);
		page->m_firstFreeSlot = liFileSlot->u.m_nextFreeSlot;
		page->m_freeSlotNum--;
		page->m_bph.m_lsn = lsn;

		// 插入时如果改动了首页，那必然是因为目录页被从空闲链表中删除
		if (flags) {
			page->m_nextFreePageNum = 0;
			page->m_inFreePageList = 0;
		}
		// 修改目录项
		liFileSlot->m_free = false;
		liFileSlot->u.m_pageId = pageId;
		session->markDirty(pageHdl);
	}
	session->releasePage(&pageHdl);

	// insert操作改变了首页
	if (flags) {
		headerPageHdl = m_lobi->lockHeaderPage(session, Exclusived);
		headerPage = (LIFileHeaderPageInfo *)headerPageHdl->getPage();
		if (headerPage->m_bph.m_lsn < lsn) { // 需要重写headerPage.
			headerPage->m_firstFreePageNum = newListHeader;
			headerPage->m_bph.m_lsn = lsn;
			session->markDirty(headerPageHdl);
		}
		m_lobi->unlockHeaderPage(session, headerPageHdl);
	}

	// 再判断大对象文件的数据页是否要redo
	// 因为各个大对象插入过程中写日志的顺序和它们的起始页号大小顺序可能是不一致，所以整理重新的调用
	// getStartPageIDAndPage还会得到不相同的起始页号
	uint blockLen = getBlockSize(dataLen);
	pageNum = getPageNum(blockLen);
	headerPageHdl = m_lobi->lockHeaderPage(session, Exclusived);
	headerPage = (LIFileHeaderPageInfo *)headerPageHdl->getPage();
	u32 fileLen = headerPage->m_blobFileLen;
	if (pageNum + pageId > fileLen) isHeaderPageModified = true;
	// 假如当前文件尾部<大对象长度
	extendLobFile(session, tableDef, pageId, pageNum, fileLen, headerPage);
	if (pageNum + pageId > headerPage->m_blobFileTail) {
		headerPage->m_blobFileTail = pageNum + pageId;
		isHeaderPageModified = true;
	}
	if (isHeaderPageModified)
		session->markDirty(headerPageHdl);
	m_lobi->unlockHeaderPage(session, headerPageHdl);
	uint offset = OFFSET_BLOCK_FIRST;
	uint copyLen = Limits::PAGE_SIZE - OFFSET_BLOCK_FIRST;

	lobFirstHeaderPageHdl = GET_PAGE(session, m_file, PAGE_LOB_HEAP, pageId, Exclusived, m_dboStats, NULL);
	LobBlockFirstPage *lobFirstHeaderPage = (LobBlockFirstPage *)lobFirstHeaderPageHdl->getPage();
	if (lobFirstHeaderPage->m_bph.m_lsn < lsn) {// 需要redo
		lobFirstHeaderPage->m_isFree = false;
		lobFirstHeaderPage->m_bph.m_lsn = lsn;
		lobFirstHeaderPage->m_isFirstPage = true;
		lobFirstHeaderPage->m_lid = lobId;
		lobFirstHeaderPage->m_len= blockLen;
		lobFirstHeaderPage->m_srcLen = orgLen;
		// 写入块首页其他数据
		if (pageNum == 1) {// 只有一个页
			// copy首页数据
			memcpy(((byte *)lobFirstHeaderPage) + offset, log + s.getSize(), dataLen);
		} else {
			memcpy(((byte *)lobFirstHeaderPage) + offset, log + s.getSize(), copyLen);
		}
		// 这里和insert不一样，可以直接释放，因为redo时候是单线程的
		session->markDirty(lobFirstHeaderPageHdl);
	}
	session->releasePage(&lobFirstHeaderPageHdl);
	// 处理块其他页的数据
	if (pageNum > 1) {
		const byte *lobContent = log + s.getSize();
		redoWriteOtherPages(session, lsn, lobContent, copyLen, pageNum, pageId, dataLen);
	}
	//vecode(vs.lob, verifyLobPage(session, pageId));
	return lobId;
}

/**
 * 在redo各种操作,进行写页面操作,主要除了首页后的其他页
 *
 * @param session 会话对象
 * @param lsn 日志LSN
 * @param lobContent 大对象字节流
 * @param offset 写入的偏移
 * @param pageNum 页数
 * @param pageId 块的启始页
 * @param dataLen 数据长度
 */
void BigLobStorage::redoWriteOtherPages(Session *session, u64 lsn, const byte *lobContent, uint offset, u32 pageNum, u32 pageId, u32 dataLen) {
	for(uint i = 1; i < pageNum; i++) {
		BufferPageHandle *bloOtherPageHdl = GET_PAGE(session, m_file, PAGE_LOB_HEAP, pageId + i, Exclusived, m_dboStats, NULL);
		LobBlockOtherPage *lobOtherPage =(LobBlockOtherPage *)bloOtherPageHdl->getPage();
		if (lobOtherPage->m_bph.m_lsn < lsn) {
			memset(lobOtherPage, 0, Limits::PAGE_SIZE);
			lobOtherPage->m_bph.m_lsn = lsn;
			lobOtherPage->m_isFirstPage = 0;
			if(i == pageNum - 1) {// 是最后一页
				memcpy((byte *)(lobOtherPage) + OFFSET_BLOCK_OTHER, lobContent + offset, dataLen - offset);
			} else {
				memcpy((byte *)(lobOtherPage) + OFFSET_BLOCK_OTHER, lobContent + offset, Limits::PAGE_SIZE - OFFSET_BLOCK_OTHER);
			}
			session->markDirty(bloOtherPageHdl);
		}
		session->releasePage(&bloOtherPageHdl);
		if (i < pageNum - 1) offset += Limits::PAGE_SIZE - OFFSET_BLOCK_OTHER;
	}
}

/**
 * 故障恢复时REDO大型大对象删除操作
 *
 * @param session 会话对象
 * @param lsn 日志LSN
 * @param log 记录插入操作日志内容
 * @param size 日志大小
 */
void BigLobStorage::redoDelete(Session *session, u64 lsn, const byte *log, uint size) {
	/* delete log: LobId(u64) PageId(u32) Flags(u8) (Old FreePageListHeader(u32) ) */
	bool flags;
	s16 slotNum;
	u64 lobid;
	u32 pageNum, pid, oldListHeader = 0;
	BufferPageHandle *pageHdl;
	LIFilePageInfo *page;
	BufferPageHandle *headerPageHdl;
	LIFileHeaderPageInfo *headerPage;
	BufferPageHandle* lobFirstHeaderPageHdl;
	Stream s((byte *)log, size);

	s.read(&lobid);
	s.read(&pid);
	s.read(&flags);
	if (flags)
		s.read(&oldListHeader);
	pageNum = LID_GET_PAGE(lobid);
	slotNum = LID_GET_SLOT(lobid);

	pageHdl = GET_PAGE(session, m_lobi->m_file, PAGE_LOB_INDEX, pageNum, Exclusived, m_dboStats, NULL);
	page = (LIFilePageInfo *)pageHdl->getPage();

	// 先判断目录文件页是否要redo
	if (page->m_bph.m_lsn < lsn) { // 需要redo
		assert(!m_lobi->getSlot(page, slotNum)->m_free); // 记录槽一定非空
		LiFileSlotInfo *liFileSlot = m_lobi->getSlot(page, slotNum);
		liFileSlot->m_free = true;
		liFileSlot->u.m_nextFreeSlot = page->m_firstFreeSlot;
		page->m_firstFreeSlot = slotNum;
		page->m_inFreePageList = true;
		page->m_bph.m_lsn = lsn;
		page->m_freeSlotNum++;
		if (flags) page->m_nextFreePageNum = oldListHeader;
		session->markDirty(pageHdl);
	}
	session->releasePage(&pageHdl);

	if (flags) { // 首页被改变
		headerPageHdl = m_lobi->lockHeaderPage(session, Exclusived);
		headerPage = (LIFileHeaderPageInfo *)headerPageHdl->getPage();
		if (headerPage->m_bph.m_lsn < lsn) { // 需要redo
			assert(headerPage->m_firstFreePageNum != pageNum);
			headerPage->m_firstFreePageNum = pageNum;
			headerPage->m_bph.m_lsn = lsn;
			session->markDirty(headerPageHdl);
		}
		m_lobi->unlockHeaderPage(session, headerPageHdl);
	}

	// 再判断大对象文件是否要redo
	lobFirstHeaderPageHdl = GET_PAGE(session, m_file, PAGE_LOB_HEAP, pid, Exclusived, m_dboStats, NULL);
    LobBlockFirstPage *lobFirstHeaderPage = (LobBlockFirstPage *)lobFirstHeaderPageHdl->getPage();
	if (lobFirstHeaderPage->m_bph.m_lsn < lsn) {// 需要redo
		lobFirstHeaderPage->m_isFree = true;
		lobFirstHeaderPage->m_bph.m_lsn = lsn;
		session->markDirty(lobFirstHeaderPageHdl);
	}
	session->releasePage(&lobFirstHeaderPageHdl);
}


/**
 * 判断大对象块是否超过文件长度，超过时候扩展文件
 * @param session  会话
 * @param tableDef 表定义
 * @param pageId  大对象的起始页号
 * @param pageNum 大对象占有页数
 * @param fileLen 大对象文件的长度
 * @param headerPage 控制页（首页）
 */
void BigLobStorage::extendLobFile(Session *session, const TableDef *tableDef, u32 pageId, u32 pageNum, u32 fileLen, LIFileHeaderPageInfo *headerPage) {
	while (pageId + pageNum > fileLen) {
		u16 incrSize = Database::getBestIncrSize(tableDef, fileLen * Limits::PAGE_SIZE);
		extendNewBlock(session, fileLen, incrSize);
		headerPage->m_blobFileLen += incrSize;
		fileLen = headerPage->m_blobFileLen;
	}
}
/**
 * 故障恢复时REDO大型大对象更新操作
 *
 * @param session 会话对象
 * @param tableDef 表定义
 * @param lid 大对象ID
 * @param lsn 日志LSN
 * @param log 记录更新操作日志内容
 * @param logSize 日志大小
 * @param lob 更新后大对象内容（在预更新日志中）
 * @param lobSize 更新后大对象长度
 */
void BigLobStorage::redoUpdate(Session *session, const TableDef *tableDef, LobId lid, u64 lsn, const byte *log, uint logSize, const byte *lob, uint lobSize) {
	/* update log: LobId(u64) PageID(u32) isNewPageID (u8) (new PageID) isNewFreeBlock(u8) (newFreeBlockPageID(u32) newFreeBlockLen(u32)) LobLen(u32) org_len(u32) */
	bool isNewPageID, isNewFreeBlock, isHeaderPageModified = false;
	s16 slotNum;
	u64 lobid;
	u32 lobOldPid = 0, lobNewPid = 0, indexPid= 0, newFreeBlockPid = 0, newFreeBlockLen = 0;
	u32 lobLen = 0, org_len = 0;
	BufferPageHandle *lobFirstHeaderPageHdl, *headerPageHdl;
	LobBlockFirstPage *lobFirstHeaderPage;
	LIFileHeaderPageInfo *headerPage;
	Stream s((byte *)log, logSize);

	s.read(&lobid);
	s.read(&lobOldPid);
	s.read(&isNewPageID);
	if (isNewPageID) s.read(&lobNewPid);
	s.read(&isNewFreeBlock);
	if (isNewFreeBlock) {
		s.read(&newFreeBlockPid);
		s.read(&newFreeBlockLen);
	}
	s.read(&lobLen);
	s.read(&org_len);

	u32 blockLen = getBlockSize(lobSize);
	u32 pageNum = getPageNum(blockLen);
	indexPid = LID_GET_PAGE(lobid);
	slotNum = LID_GET_SLOT(lobid);
	headerPageHdl = m_lobi->lockHeaderPage(session, Exclusived);
	headerPage = (LIFileHeaderPageInfo *)headerPageHdl->getPage();
	u32 fileLen = headerPage->m_blobFileLen;
	uint offset = OFFSET_BLOCK_FIRST;
	uint copyLen = Limits::PAGE_SIZE - OFFSET_BLOCK_FIRST;

	// 下面条件成立，说明没有被移动到文件末尾，故分下面四种情况：
	// 1、大对象变小
	// 2、大对象正在文件末尾最后一块
	// 3、大对象大小占有的页数没变
	// 4、后面有足够的连续的空闲空间
	if (!isNewPageID) {
		if (lobOldPid + pageNum > fileLen) isHeaderPageModified = true;
		extendLobFile(session, tableDef, lobOldPid, pageNum, fileLen, headerPage);

		if (!isNewFreeBlock) {// 上面2和3以及4的部分情况
			// 当情况是2和3的时候要判断下fileTail
			u32 fileTail = headerPage->m_blobFileTail;
			if (fileTail < lobOldPid + pageNum) {
				isHeaderPageModified = true;
				headerPage->m_blobFileTail = lobOldPid + pageNum;
			}
		} else {// 上面1和4的部分情况
			// 修改新产生的空闲块的首页数据
			BufferPageHandle *newFreeBlockHeadrPageHdl = GET_PAGE(session, m_file, PAGE_LOB_HEAP, newFreeBlockPid, Exclusived, m_dboStats, NULL);
			LobBlockFirstPage *newFreeBlockHeadrPage = (LobBlockFirstPage *)newFreeBlockHeadrPageHdl->getPage();
			// 需要redo这个页
			if (newFreeBlockHeadrPage->m_bph.m_lsn < lsn) {
				createNewFreeBlock(session, newFreeBlockHeadrPageHdl, newFreeBlockHeadrPage, lsn, newFreeBlockLen);
			} else {
				session->releasePage(&newFreeBlockHeadrPageHdl);
			}
		}
		// 更新大对象数据
		lobFirstHeaderPageHdl = GET_PAGE(session, m_file, PAGE_LOB_HEAP, lobOldPid, Exclusived, m_dboStats, NULL);
		lobFirstHeaderPage = (LobBlockFirstPage *)lobFirstHeaderPageHdl->getPage();
		// 需要redo这个页
		if (lobFirstHeaderPage->m_bph.m_lsn < lsn) {
			lobFirstHeaderPage->m_bph.m_lsn = lsn;
			lobFirstHeaderPage->m_len = blockLen;
			lobFirstHeaderPage->m_lid = lid;
			lobFirstHeaderPage->m_srcLen = org_len;

			// 更新其他数据
			if (pageNum == 1) {// 只有一个页
				// copy首页数据
				memcpy(((byte *)lobFirstHeaderPage) + offset, lob, lobSize);
			} else {
				memcpy(((byte *)lobFirstHeaderPage) + offset, lob, copyLen);
			}
			// 这里和insert不一样，可以直接释放，因为redo时候是单线程的
			session->markDirty(lobFirstHeaderPageHdl);
			session->releasePage(&lobFirstHeaderPageHdl);
		} else {
			session->releasePage(&lobFirstHeaderPageHdl);
		}
		// 处理块其他页的数据
		if (pageNum > 1) {
			redoWriteOtherPages(session, lsn, lob, copyLen, pageNum, lobOldPid, lobSize);
		}
	} else {// 大对象被移动到文件末尾
		BufferPageHandle *pageHdl;
		LIFilePageInfo *page;
		assert(isNewFreeBlock);// 一定产生新的空闲块
		if (lobNewPid + pageNum > fileLen) isHeaderPageModified = true;
		extendLobFile(session, tableDef, lobNewPid, pageNum, fileLen, headerPage);

		u32 fileTail = headerPage->m_blobFileTail;
		if (fileTail < lobNewPid + pageNum) {
			isHeaderPageModified = true;
			headerPage->m_blobFileTail = lobNewPid + pageNum;
		}
		lobFirstHeaderPageHdl = GET_PAGE(session, m_file, PAGE_LOB_HEAP, lobNewPid, Exclusived, m_dboStats, NULL);
		lobFirstHeaderPage = (LobBlockFirstPage *)lobFirstHeaderPageHdl->getPage();
		// 这里数据页肯定大于一页
		if (lobFirstHeaderPage->m_bph.m_lsn < lsn) {// 需要redo这个页
			lobFirstHeaderPage->m_bph.m_lsn = lsn;
			lobFirstHeaderPage->m_len = blockLen;
			lobFirstHeaderPage->m_isFirstPage = true;
			lobFirstHeaderPage->m_isFree = false;
			lobFirstHeaderPage->m_lid = lid;
			lobFirstHeaderPage->m_srcLen = org_len;
			// 更新其他数据
			memcpy(((byte *)lobFirstHeaderPage) + offset, lob, copyLen);
			// 这里和insert不一样，可以直接释放，因为redo时候是单线程的
			session->markDirty(lobFirstHeaderPageHdl);
		}
		session->releasePage(&lobFirstHeaderPageHdl);
		// 更新其他页
		redoWriteOtherPages(session, lsn, lob, copyLen, pageNum, lobNewPid, lobSize);

		// 修改空闲块首页数据
		BufferPageHandle *newFreeBlockHeadrPageHdl = GET_PAGE(session, m_file, PAGE_LOB_HEAP, lobOldPid, Exclusived, m_dboStats, NULL);
		LobBlockFirstPage *newFreeBlockHeadrPage = (LobBlockFirstPage *)newFreeBlockHeadrPageHdl->getPage();
		if (newFreeBlockHeadrPage->m_bph.m_lsn < lsn) {// 需要redo这个页
			createNewFreeBlock(session, newFreeBlockHeadrPageHdl, newFreeBlockHeadrPage, lsn, newFreeBlockLen);
		} else {
			session->releasePage(&newFreeBlockHeadrPageHdl);
		}
		// 修改对应的目录页，这里目录页肯定有变化
		pageHdl = GET_PAGE(session, m_lobi->m_file, PAGE_LOB_INDEX, indexPid, Exclusived, m_dboStats, NULL);
		page = (LIFilePageInfo *)pageHdl->getPage();
		if (page->m_bph.m_lsn < lsn) {// 需要redo
			page->m_bph.m_lsn = lsn;
			LiFileSlotInfo *slotInfo = m_lobi->getSlot(page, slotNum);
			slotInfo->u.m_pageId = lobNewPid;
			slotInfo->m_free = false;
			session->markDirty(pageHdl);
		}
		session->releasePage(&pageHdl);
	}
	// 释放首页
	if (isHeaderPageModified) session->markDirty(headerPageHdl);
	m_lobi->unlockHeaderPage(session, headerPageHdl);
}

/** 修改表的ID
 * @param session 会话
 * @param tableId 新的表ID
 */
void BigLobStorage::setTableId(Session *session, u16 tableId) {
	m_lobi->setTableId(session, tableId);
}

/**
 * 在线碎片整理
 *
 * @param session 会话
 * @param tableDef 表定义
 */
void BigLobStorage::defrag(Session *session, const TableDef *tableDef) {
	bool flag;
	u32 notFreePid, freePid, notFreeBlockLen, nextBlockPid, nextPid;
	BufferPageHandle *freeBlockPageHdl, *notFreeBlockPageHdl, *nextFreePageHdl;
	BufferPageHandle *nextBlockPageHdl = NULL;
	MemoryContext *mc = session->getMemoryContext();

	// 先读取下文件的长度，为以后判断是否到了文件末尾做依据
	u32 fileTail = getBlobFileTail(session);

	// 得到第一个空闲块和非空闲块
	freeBlockPageHdl = getFirstFreeBlockAndNextNotFreeBlock(session, &notFreeBlockPageHdl, &freePid, &notFreePid, &notFreeBlockLen, fileTail);
	if (freeBlockPageHdl == NULL)
		return;

	flag = true;
	u32 pageNum = getPageNum(notFreeBlockLen);
	nextPid = notFreePid;

	// 下面开始进行整理
	while (nextPid + pageNum < fileTail) {
		// 假如下面找到的块是非空闲块
		if (flag) {
			// 移动前准备工作，即判断是否会自我覆盖，假如自我覆盖，就放到文件末尾
			// 移动目的地空闲不够，需要移动到末尾
			if (nextPid - freePid < pageNum) {
				 putLobToTail(session, tableDef, nextPid, notFreeBlockPageHdl, mc);
			} else {// 移动大对象
				 moveLob(session, freePid, nextPid, freeBlockPageHdl, notFreeBlockPageHdl, mc);
				// 得到下个块，即新块后的下一个空闲块地址
				nextFreePageHdl = NEW_PAGE(session, m_file, PAGE_LOB_HEAP, freePid + pageNum, Exclusived, m_dboStats);
				// 然后释放刚才新块的首页
				session->markDirty(freeBlockPageHdl);
				session->releasePage(&freeBlockPageHdl);
				freePid = freePid + pageNum;
				freeBlockPageHdl = nextFreePageHdl;
			}
			// 读取下个块
			nextBlockPid = nextPid + pageNum;
			nextBlockPageHdl = GET_PAGE(session, m_file, PAGE_LOB_HEAP, nextBlockPid, Exclusived, m_dboStats, NULL);
			LobBlockFirstPage *lobfirstPage = (LobBlockFirstPage *)nextBlockPageHdl->getPage();
			notFreeBlockLen = lobfirstPage->m_len;
			pageNum = getPageNum(notFreeBlockLen);
			// 下面假如下个块是非空闲块
			if (!lobfirstPage->m_isFree) {
				notFreeBlockLen = lobfirstPage->m_len;
				notFreeBlockPageHdl = nextBlockPageHdl;
				flag = true;
			} else {
				// 释放假如是空闲块的首页
				session->releasePage(&nextBlockPageHdl);
				flag = false;
			}
			nextPid = nextBlockPid;
		} else {
			// 读取下个块。
			nextBlockPageHdl = GET_PAGE(session, m_file, PAGE_LOB_HEAP, nextPid + pageNum, Exclusived, m_dboStats, NULL);
			LobBlockFirstPage *lobfirstPage = (LobBlockFirstPage *) nextBlockPageHdl->getPage();
			notFreeBlockLen = lobfirstPage->m_len;
			nextPid = nextPid + pageNum;
			if (!lobfirstPage->m_isFree) {
				notFreeBlockPageHdl = nextBlockPageHdl;
				flag = true;
			} else {
				session->releasePage(&nextBlockPageHdl);
				flag = false;
			}
			pageNum = getPageNum(notFreeBlockLen);
		}
		// 假如到了前一次读取的文件末尾，则再读取一次
		if (nextPid + pageNum >= fileTail) {
			fileTail = getBlobFileTail(session);
			// 这里判断是否到了文件末尾并进行处理
			if (nextPid + pageNum == fileTail) {// 假如是最后一个块
				// 重新判断是否到了文件末尾
				// 先对目录文件首页加锁。
				BufferPageHandle *liFileHeaderPageHdl = m_lobi->lockHeaderPage(session, Exclusived);
				LIFileHeaderPageInfo *liFileHeaderPage=(LIFileHeaderPageInfo *)liFileHeaderPageHdl->getPage();
				u32 newFileTail = liFileHeaderPage->m_blobFileTail;
				if (nextPid + pageNum == newFileTail) {// 说明没有新的大对象插入
					if (flag) {// 最后一块是非空闲块
						moveLob(session, freePid, nextPid, freeBlockPageHdl, nextBlockPageHdl, mc);
						liFileHeaderPage->m_blobFileTail = freePid + pageNum;
					} else
						liFileHeaderPage->m_blobFileTail = freePid;
					session->markDirty(liFileHeaderPageHdl);
					session->markDirty(freeBlockPageHdl);
					session->releasePage(&freeBlockPageHdl);
				}
				m_lobi->unlockHeaderPage(session, liFileHeaderPageHdl);
				break;
			}
		}
	}

	SYNCHERE(SP_LOB_BIG_READ_DEFRAG_DEFRAG);
}

/**
 * 先查找第一个空闲块和空闲块后的第一个非空闲块
 *
 * @post 得到第一个空闲块的首页写锁
 * @post 得到第一个非空闲块的首页读锁
 *
 * @param session 会话对象
 * @param firstNotFreeBlock out 在空闲块后面的第一个非空闲块的起始页的句柄
 * @param firstFreePid out 第一个空闲块的起始页号
 * @param firstNotFreePid out 第一个非空闲块的起始页号
 * @param notFreeBlockLen out 第一个非空闲块的长度
 * @param fileTail 当前的大对象文件尾部
 * @return 第一个空闲块的起始页的句柄
 */
BufferPageHandle* BigLobStorage::getFirstFreeBlockAndNextNotFreeBlock(Session *session, BufferPageHandle **firstNotFreeBlock, u32 *firstFreePid, u32 *firstNotFreePid, u32 *notFreeBlockLen, u32 fileTail) {
	BufferPageHandle *freeBlockFirstPageHdl;
	u32 nextPid = 0;
	u32 blockLen, pageNum;
	BufferPageHandle *pageHdl = GET_PAGE(session, m_file, PAGE_LOB_HEAP, 0, Exclusived, m_dboStats, NULL);
	LobBlockFirstPage *lobFirstPage = (LobBlockFirstPage *)pageHdl->getPage();
	// 先找到第一个空闲块
	*firstFreePid = 0;
	while (lobFirstPage->m_isFree == false) {
	    blockLen = lobFirstPage->m_len;
		session->releasePage(&pageHdl);
		pageNum = getPageNum(blockLen);
		nextPid += pageNum;
		if (nextPid >= fileTail)
			return NULL;
		pageHdl = GET_PAGE(session, m_file, PAGE_LOB_HEAP, nextPid, Exclusived, m_dboStats, NULL);
		lobFirstPage = (LobBlockFirstPage *)pageHdl->getPage();
		*firstFreePid = nextPid;
	}
	freeBlockFirstPageHdl = pageHdl;
	blockLen = lobFirstPage->m_len;
	pageNum = getPageNum(blockLen);
	nextPid += pageNum;
	if (nextPid >= fileTail) {
		session->releasePage(&freeBlockFirstPageHdl);
		return NULL;
	}
	pageHdl = GET_PAGE(session, m_file, PAGE_LOB_HEAP, nextPid, Exclusived, m_dboStats, NULL);
	lobFirstPage = (LobBlockFirstPage *)pageHdl->getPage();
	*firstNotFreePid = nextPid;
	*notFreeBlockLen = lobFirstPage->m_len;
	*firstNotFreeBlock = pageHdl;
	// 寻找空闲块后面的第一个非空闲块
	while (lobFirstPage->m_isFree == true) {
		blockLen = lobFirstPage->m_len;
		session->releasePage(&pageHdl);
		pageNum = getPageNum(blockLen);
		nextPid += pageNum;
		if (nextPid >= fileTail) {
			session->releasePage(&freeBlockFirstPageHdl);
			return NULL;
		}
		pageHdl = GET_PAGE(session, m_file, PAGE_LOB_HEAP, nextPid, Exclusived, m_dboStats, NULL);
		lobFirstPage = (LobBlockFirstPage *)pageHdl->getPage();
		*notFreeBlockLen = lobFirstPage->m_len;
		*firstNotFreePid = nextPid;
		*firstNotFreeBlock = pageHdl;
	}
	*firstNotFreeBlock = pageHdl;
	return freeBlockFirstPageHdl;
}

/**
 * 得到大对象文件已经写入长度，即fileTail
 *
 * @param session 会话对象
 * @return fileTail
 */
u32 BigLobStorage::getBlobFileTail(Session *session) {
	BufferPageHandle *headerPageHdl = m_lobi->lockHeaderPage(session, Shared);
	u32 fileTail = ((LIFileHeaderPageInfo *)headerPageHdl->getPage())->m_blobFileTail;
	m_lobi->unlockHeaderPage(session, headerPageHdl);
	return fileTail;
}

/**
 * 把大对象移动到文件末尾
 *
 * @post 得到下个块的首页读锁
 * @post 释放原来块的首页锁
 *
 * @param session 会话对象
 * @param tableDef 表定义
 * @param pid 碎片整理过程中，需要移动的非空闲块的起始页号
 * @param notFreeBlockFirstPageHdl  需要移动的非空闲块的起始页句柄
 * @param mc 内存上下文
 */
void BigLobStorage::putLobToTail(Session *session, const TableDef *tableDef, u32 pid, BufferPageHandle *notFreeBlockFirstPageHdl, MemoryContext *mc) {
	u32 indexPid, newPid;
	u16 slotNum;
	u64 lsn = 0;
	// 准备LOB内容，为写日志
	LobBlockFirstPage *lobFirstPage = (LobBlockFirstPage *)notFreeBlockFirstPageHdl->getPage();
	LobId lid = lobFirstPage->m_lid;

	u32 org_len = lobFirstPage->m_srcLen;
	u32 offset = Limits::PAGE_SIZE - OFFSET_BLOCK_FIRST;
	u32 lobLen = lobFirstPage->m_len;
	u64 savePoint = mc->setSavepoint();
	byte *data = (byte *)mc->alloc(getLobSize(lobLen));
	u32 pageNum = getPageNum(lobLen);
	uint lobSize = getLobContent(session, data, lobFirstPage, pid);
	assert(lobSize == getLobSize(lobLen));
	BufferPageHandle *newBlockFirstPageHdl = getStartPageIDAndPage(tableDef, pageNum, session, &newPid);

	// 因为要修改对应的起始PageID,所以要去修改对应目录页
	indexPid = LID_GET_PAGE(lid);
	BufferPageHandle *liFilePageHdl = GET_PAGE(session, m_lobi->m_file, PAGE_LOB_INDEX, indexPid, Exclusived, m_dboStats, NULL);
	LIFilePageInfo *liFilePage = (LIFilePageInfo *)liFilePageHdl->getPage();

	// 写日志
	lsn = logMove(session, lid, pid, newPid, org_len, lobSize, data, m_lobi->getTableID(), mc);

	liFilePage->m_bph.m_lsn = lsn;
	slotNum = LID_GET_SLOT(lid);
	LiFileSlotInfo *slotInfo=m_lobi->getSlot(liFilePage,slotNum);
	slotInfo->u.m_pageId = newPid;
	// 释放目录页
	session->markDirty(liFilePageHdl);
	session->releasePage(&liFilePageHdl);

	// 开始写数据，这里的数据主要是首页copy,然后修改lsn
	LobBlockFirstPage *newDataBlockHeaderPage = (LobBlockFirstPage *)newBlockFirstPageHdl->getPage();
	memcpy(newDataBlockHeaderPage, lobFirstPage, Limits::PAGE_SIZE);
	newDataBlockHeaderPage->m_bph.m_lsn = lsn;
	// 假如大于一页，则写其他页数据
	if (pageNum >1) {
		writeLob(session, newPid, pageNum, offset, lsn, data, lobSize);
	}
	// 重设MemoryContext
	mc->resetToSavepoint(savePoint);
	// 释放新块首页
	session->markDirty(newBlockFirstPageHdl);
	session->releasePage(&newBlockFirstPageHdl);
	// 最后释放旧块首页
	lobFirstPage->m_bph.m_lsn = lsn;
	lobFirstPage->m_isFree = true;
	session->markDirty(notFreeBlockFirstPageHdl);
	session->releasePage(&notFreeBlockFirstPageHdl);
}


/**
 * 把大对象往前移动，做碎片整理
 * @post 移动后新块的首页锁还保持着
 * @post 得到下个块的首页读锁
 * @post 释放原来块的首页锁
 * @param session 会话对象
 * @param freePid 移动目的地的起始页号
 * @param notFreePid 碎片整理过程中，需要移动的非空闲块的起始页号
 * @param freeBlockPageHdl  移动目的地（空闲块）的起始页句柄
 * @param notFreeBlockFirstPageHdl  需要移动的非空闲块的起始页句柄
 * @param mc 内存上下文
 */
void BigLobStorage::moveLob(Session *session, u32 freePid, u32 notFreePid, BufferPageHandle *freeBlockPageHdl, BufferPageHandle *notFreeBlockFirstPageHdl, MemoryContext *mc) {
	u32 org_len = 0;
	u32 indexPid, pageNum;
	u16 slotNum;
	u64 lsn = 0;
	u32 offset = 0;

	// 得到大对象内容，为写日志准备
	LobBlockFirstPage *lobFirstPage = (LobBlockFirstPage *)notFreeBlockFirstPageHdl->getPage();
	LobBlockFirstPage *freeFirstPage = (LobBlockFirstPage *)freeBlockPageHdl->getPage();
	LobId lid = lobFirstPage->m_lid;
	org_len = lobFirstPage->m_srcLen;
	offset = Limits::PAGE_SIZE - OFFSET_BLOCK_FIRST;

	u32 lobLen = lobFirstPage->m_len;
	u64 savePoint = mc->setSavepoint();
	byte *data = (byte *)mc->alloc(getLobSize(lobLen));
	pageNum = getPageNum(lobLen);
	uint lobSize = getLobContent(session, data, lobFirstPage, notFreePid);
	assert(lobSize == getLobSize(lobLen));
	pageNum = getPageNum(lobLen);

	// 因为要修改对应的起始PageID,所以要去修改对应目录页
	indexPid = LID_GET_PAGE(lid);
	BufferPageHandle *liFilePageHdl = GET_PAGE(session, m_lobi->m_file, PAGE_LOB_INDEX, indexPid, Exclusived, m_dboStats, NULL);
	LIFilePageInfo *liFilePage = (LIFilePageInfo *)liFilePageHdl->getPage();

	// 写日志
	lsn = logMove(session, lid, notFreePid, freePid, org_len, lobSize, data, m_lobi->getTableID(), mc);
	liFilePage->m_bph.m_lsn = lsn;
	slotNum = LID_GET_SLOT(lid);
	LiFileSlotInfo *slotInfo=m_lobi->getSlot(liFilePage,slotNum);
	slotInfo->u.m_pageId = freePid;
	// 释放目录页
	session->markDirty(liFilePageHdl);
	session->releasePage(&liFilePageHdl);

	// 开始写数据，这里的数据主要是整页copy,然后修改lsn;
	memcpy(freeFirstPage, lobFirstPage, Limits::PAGE_SIZE);
	freeFirstPage->m_bph.m_lsn = lsn;
	// 假如大于一页，则copy其他页数据
	if (pageNum >1) {
		writeLob(session, freePid, pageNum, offset, lsn, data, lobSize);
	}
	mc->resetToSavepoint(savePoint);
	// 最后释放旧块首页
	lobFirstPage->m_bph.m_lsn = lsn;
	lobFirstPage->m_isFree = true;
	session->markDirty(notFreeBlockFirstPageHdl);
	session->releasePage(&notFreeBlockFirstPageHdl);
}

/**
 * 碎片整理过程中，移动大对象的时候的日志
 *
 * @param session 会话对象
 * @param lid 大对象ID
 * @param oldPid 移动前的起始页号
 * @param newPid 移动前后的起始页号
 * @param org_len 未压缩的长度
 * @param lobLen 大对象长度
 * @param data 数据内容
 * @param tid 母表ID
 * @param mc 内存上下文
 * @return lsn
 */
u64 BigLobStorage::logMove(Session *session, LobId lid, u32 oldPid, u32 newPid, u32 org_len, u32 lobLen, byte *data, u16 tid, MemoryContext *mc) {
	byte *logData =(byte *)mc->alloc(32 + lobLen);
	Stream s(logData, 32 + lobLen);
	try {
		// LobId
		s.write(lid);
		// PageId
		s.write(oldPid);
		// flags, 起始页号是否被改变
		s.write(newPid);
		s.write(org_len);
		s.write(lobLen);
		s.write(data, lobLen);
	} catch (NtseException &) {
		assert(false);	// 这里不可能出现异常
	}
	return session->writeLog(LOG_LOB_MOVE, tid, logData, s.getSize());
}


/**
 * 根据块长度等到块占有页数
 *
 * @param lobLen 块长度
 * @return 占有页数
 */
uint BigLobStorage::getPageNum(uint lobLen) {
	return (lobLen + Limits::PAGE_SIZE - 1) / Limits::PAGE_SIZE;
}

/**
 * 故障恢复时REDO大型大对象插入操作
 *
 * @param session 会话对象
 * @param tableDef 表定义
 * @param lsn 日志LSN
 * @param log 记录插入操作日志内容
 * @param logSize 日志大小
 */
void BigLobStorage::redoMove(Session *session, const TableDef *tableDef, u64 lsn, const byte *log, uint logSize) {
	/* move log: LobId(u64) PageID(u32) newPageID(u32) LobLen(u32) LobData(byte*) */
	s16 slotNum;
	LobId lobid;
	u32 lobOldPid = 0, lobNewPid = 0, indexPid= 0;
	u32 org_len = 0, lobLen = 0;
	BufferPageHandle *lobFirstHeaderPageHdl;
	LobBlockFirstPage *lobFirstHeaderPage;
	Stream s((byte *)log, logSize);

	s.read(&lobid);
	s.read(&lobOldPid);
	s.read(&lobNewPid);
	s.read(&org_len);
	s.read(&lobLen);
	uint lobBlockSize = getBlockSize(lobLen);
	u32 pageNum = getPageNum(lobBlockSize);
	indexPid = LID_GET_PAGE(lobid);
	slotNum = LID_GET_SLOT(lobid);
	u32 offset = OFFSET_BLOCK_FIRST;
	u32 haveCopyLen = Limits::PAGE_SIZE - OFFSET_BLOCK_FIRST;

	if (lobNewPid > lobOldPid) {// 说明大对象被移动到文件末尾
		bool isHeaderPageModified = false;
		BufferPageHandle *headerPageHdl = m_lobi->lockHeaderPage(session, Exclusived);
		LIFileHeaderPageInfo *headerPage = (LIFileHeaderPageInfo *)headerPageHdl->getPage();
		u32 fileLen = headerPage->m_blobFileLen;
		if (lobNewPid + pageNum > fileLen) isHeaderPageModified = true;
		extendLobFile(session, tableDef, lobNewPid, pageNum, fileLen, headerPage);
		u32 fileTail = headerPage->m_blobFileTail;
		if (fileTail < lobNewPid + pageNum) {
			isHeaderPageModified = true;
			headerPage->m_blobFileTail = lobNewPid + pageNum;
		}

		if (isHeaderPageModified) session->markDirty(headerPageHdl);
		m_lobi->unlockHeaderPage(session, headerPageHdl);
	}

	lobFirstHeaderPageHdl = GET_PAGE(session, m_file, PAGE_LOB_HEAP, lobNewPid, Exclusived, m_dboStats, NULL);
	lobFirstHeaderPage = (LobBlockFirstPage *)lobFirstHeaderPageHdl->getPage();
	if (lobFirstHeaderPage->m_bph.m_lsn < lsn) {// 需要redo这个页
		lobFirstHeaderPage->m_bph.m_lsn = lsn;
		lobFirstHeaderPage->m_len = lobBlockSize;
		lobFirstHeaderPage->m_isFirstPage = true;
		lobFirstHeaderPage->m_isFree = false;
		lobFirstHeaderPage->m_lid = lobid;
		lobFirstHeaderPage->m_srcLen = org_len;
		if (pageNum > 1)
		// 更新其他数据
			memcpy((byte *)lobFirstHeaderPage + offset, log + s.getSize(), haveCopyLen);
		else
			memcpy((byte *)lobFirstHeaderPage + offset, log + s.getSize(), lobLen);
		// 这里和insert不一样，可以直接释放，因为redo时候是单线程的
		session->markDirty(lobFirstHeaderPageHdl);
	}
	session->releasePage(&lobFirstHeaderPageHdl);

	// 更新其他页
	const byte *lobContent = log + s.getSize();
	if (pageNum > 1){
		redoWriteOtherPages(session, lsn, lobContent, haveCopyLen, pageNum, lobNewPid, lobLen);
	}
	// 修改空闲块首页数据
	BufferPageHandle *newFreeBlockHeadrPageHdl = GET_PAGE(session, m_file, PAGE_LOB_HEAP, lobOldPid, Exclusived, m_dboStats, NULL);
	LobBlockFirstPage *newFreeBlockHeadrPage = (LobBlockFirstPage *)newFreeBlockHeadrPageHdl->getPage();
	if (newFreeBlockHeadrPage->m_bph.m_lsn < lsn) {// 需要redo这个页
		createNewFreeBlock(session, newFreeBlockHeadrPageHdl, newFreeBlockHeadrPage, lsn, pageNum);
	} else {
		session->releasePage(&newFreeBlockHeadrPageHdl);
	}
	// 修改对应的目录页，这里目录页肯定有变化
	BufferPageHandle *pageHdl = GET_PAGE(session, m_lobi->m_file, PAGE_LOB_INDEX, indexPid, Exclusived, m_dboStats, NULL);
	LIFilePageInfo *page = (LIFilePageInfo *)pageHdl->getPage();
	if (page->m_bph.m_lsn < lsn) {//需要redo
		page->m_bph.m_lsn = lsn;
		LiFileSlotInfo *slotInfo = m_lobi->getSlot(page, slotNum);
		slotInfo->u.m_pageId = lobNewPid;
		assert(!slotInfo->m_free);
		session->markDirty(pageHdl);
	}
	session->releasePage(&pageHdl);
}

/**
 * 得到目录文件和大对象文件句柄
 *
 * @param files in/out 该模块的所有File指针数组， 空间调用者分配
 * @param pageTypes in/out File对应的页类型
 * @param numFile files数组和pageTypes数组长度
 * @return File对象个数
 */
int BigLobStorage::getFiles(File** files, PageType* pageTypes, int numFile) {
    UNREFERENCED_PARAMETER(numFile);
	assert(numFile >= 2);
	// 必须先返回索引文件
	// 否则备份时, 头页中的文件长度不对
	files[0] = m_lobi->getFile();
	pageTypes[0] = PAGE_LOB_INDEX;

	files[1] = m_file;
	pageTypes[1] = PAGE_LOB_HEAP;
	return 2;
}

/**
 * 刷出脏数据
 *
 * @param session 会话
 */
void BigLobStorage::flush(Session *session) {
	m_buffer->flushDirtyPages(session, m_lobi->m_file);
	m_buffer->flushDirtyPages(session, m_file);
}

/**
 * 验证大对象页，这里主要验证isFirstPage是否正确
 * @param session, 会话对象
 * @param pid 数据页ID
 */
//void BigLobStorage::verifyLobPage(Session *session, u32 pid) {
//	BufferPageHandle *lobBlockHeadrPageHdl = GET_PAGE(session, m_file, PAGE_LOB_HEAP, pid, Shared, m_dboStats, NULL);
//	LobBlockFirstPage *firstPage = (LobBlockFirstPage *)lobBlockHeadrPageHdl->getPage();
//	u32 blockLen = firstPage->m_len;
//	// 大对象占有的页数
//	uint pageNum = getPageNum(blockLen);
//	for(uint j = 1; j < pageNum -1; j++) {
//		BufferPageHandle *lobBlockkPageHdl = GET_PAGE(session, m_file, PAGE_LOB_HEAP, pid, Shared, m_dboStats, NULL);
//		LobBlockOtherPage *otherPage = (LobBlockOtherPage *)lobBlockkPageHdl->getPage();
//		assert(otherPage->m_isFirsPage);
//		session->releasePage(&lobBlockkPageHdl);
//	}
//	session->releasePage(&lobBlockHeadrPageHdl);
//}

/**
 * 得到统计信息
 *
 * @return 统计信息
 */
const BLobStatus& BigLobStorage::getStatus() {
	// TODO 不加锁取数据可能不太安全，但这里出于性能考虑不能加锁
	// 需要考虑在内存中记录这两个值
	m_status.m_datLength = m_lobi->m_headerPage->m_blobFileTail * Limits::PAGE_SIZE;
	m_status.m_idxLength = m_lobi->m_headerPage->m_fileLen * Limits::PAGE_SIZE;
	return m_status;
}

/**
 * 得到扩展统计信息
 * 
 * @return 统计信息
 */
const BLobStatusEx& BigLobStorage::getStatusEx() {
	return m_statusEx;
}

/**
 * 更新扩展统计信息
 *
 * @param session 会话
 * @param maxSamplePages 最多采样这么多个页面
 */
void BigLobStorage::updateExtendStatus(Session *session, uint maxSamplePages) {
	McSavepoint mcSave(session->getMemoryContext());
	
	// 增加一把锁，防止并发
	Mutex mutex("BigLobStorage::sample", __FILE__, __LINE__);
	MutexGuard sampleGuard(&mutex, __FILE__, __LINE__);
	// 先统计目录文件
	u32 maxUsedPage = m_lobi->getMaxUsedPage(session);
	u32 minPage = (u32)m_lobi->metaDataPgCnt();
	if (maxUsedPage <= minPage - 1) {
		m_statusEx.m_freePages = m_statusEx.m_numLobs = 0;
		m_statusEx.m_pctUsed = .0;
		return;
	}

	SampleResult *result;
	if (maxSamplePages > 2048)
		// 样本较大时，相似度要求可以高一些
		result = SampleAnalyse::sampleAnalyse(session, m_lobi, maxSamplePages, 30, true, 0.382, 16); 
	else
		// 样本较小时，相似度要求放松一些
		result = SampleAnalyse::sampleAnalyse(session, m_lobi, maxSamplePages, 50, true, 0.618, 8);  
	m_statusEx.m_numLobs = (u64)(result->m_fieldCalc[0].m_average * (maxUsedPage - minPage + 1));
	// 假如大对象比较少，采样出来是0，
	if (m_statusEx.m_numLobs == 0) {
		m_statusEx.m_freePages = m_statusEx.m_numLobs = 0;
		m_statusEx.m_pctUsed = .0;
		return;
	}
	
	// 得到大对象的平均长度
	u32 fileTail = getBlobFileTail(session);
	m_lobAverageLen = (u32) (fileTail * Limits::PAGE_SIZE / m_statusEx.m_numLobs);
	
	delete result;

	SampleResult *blobResult;

	// 再采样大对象文件
	if (maxSamplePages > 2048)
		// 样本较大时，相似度要求可以高一些
		blobResult = SampleAnalyse::sampleAnalyse(session, this, maxSamplePages, 30, true, 0.382, 16); 
	else
		// 样本较小时，相似度要求放松一些
		blobResult = SampleAnalyse::sampleAnalyse(session, this, maxSamplePages, 50, true, 0.618, 8);  
	m_statusEx.m_freePages = (u64)(blobResult->m_fieldCalc[0].m_average * m_sampleRatio);
	m_statusEx.m_pctUsed  = blobResult->m_fieldCalc[1].m_average /
		(blobResult->m_fieldCalc[2].m_average * Limits::PAGE_SIZE);			
	delete blobResult;
}


/** 
 * 开始采样准备工作 
 *
 * @param session 会话
 * @param maxSampleNum 最大采样个数
 * @param fastSample 快速采用
 * @return 采样句柄
 */
SampleHandle* BigLobStorage::beginSample(Session *session, uint maxSampleNum, bool fastSample) {
	BigLobSampleHandle *handle = new BigLobSampleHandle(session, maxSampleNum, fastSample);
	if(fastSample) {
		// 主要在内存中采样,先采样目录文件
		handle->m_bufScanHdl = m_buffer->beginScan(session->getId(), m_file);
	} else {
		// 然后在磁盘上采样
		// 得到采样的范围
		handle->m_maxPage = getBlobFileTail(session);
		handle->m_minPage = metaDataPgCnt();

		// 看文件具体长度，然后决定采样的分区大小和分区的个数
		if (handle->m_maxSample > (handle->m_maxPage - handle->m_minPage + 1))
			handle->m_maxSample = (uint)(handle->m_maxPage - handle->m_minPage + 1);

		if (maxSampleNum > handle->m_maxSample) 
			maxSampleNum = handle->m_maxSample;
		/** 分为16个区来采样 */
		handle->m_blockNum = 16 > maxSampleNum ? maxSampleNum : 16; 
		handle->m_regionSize = (handle->m_maxPage + 1 - handle->m_minPage) / handle->m_blockNum;

		// 如果页面太少，不足以分区，则全部采样
		if (handle->m_regionSize <= 1) { 
			handle->m_blockNum = 1;
			handle->m_regionSize = handle->m_maxPage + 1 - handle->m_minPage;
		}
		handle->m_curBlock = 0;
		handle->m_blockSize = getRegionSize(m_lobAverageLen);
		m_sampleRatio = (double)handle->m_regionSize / handle->m_blockSize;
		if ((u64)handle->m_blockSize > handle->m_regionSize) {
			handle->m_blockSize = (int)handle->m_regionSize;
			handle->m_blockNum = (int)((handle->m_maxPage + 1 - handle->m_minPage) / handle->m_regionSize);
			m_sampleRatio = (double)handle->m_regionSize / handle->m_blockSize;
		}
	}
	return handle;
}


/**
 * 进行下一个采样
 *
 * @param handle 采样句柄
 * @return 一个采样
 */
Sample* BigLobStorage::sampleNext(SampleHandle *handle) {
	BigLobSampleHandle *shdl = (BigLobSampleHandle *)handle;
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
			Sample *sample = sampleBufferPage(page, handle);
			sample->m_ID = bcb->m_pageKey.m_pageId;
			return sample;
		}
		return NULL;
	}

	u32 pid = 0;
	uint maxSample = shdl->m_maxSample;
sampleNext_diskPage_getNext:
	if (maxSample > 0 && shdl->m_curBlock < shdl->m_blockNum) {
		// 最后一个全取
		u64 regionSize = (shdl->m_curBlock == shdl->m_blockNum - 1) ?
			(shdl->m_blockSize + (shdl->m_maxPage + 1 - shdl->m_minPage) % shdl->m_regionSize)
			: shdl->m_blockSize;
		BufferPageHandle *page = unsamplablePagesBetween(handle->m_session, shdl->m_minPage + shdl->m_curBlock * shdl->m_regionSize, regionSize, &pid);
		if (page == NULL)  {
			shdl->m_curBlock++;
			maxSample--;
			goto sampleNext_diskPage_getNext;
		}
		Sample *sample = sampleRegionPage(page, handle, pid);
		// 记录下上次采样的空闲空间
		shdl->m_lastFreePages = (*sample)[0];
		shdl->m_curBlock++;
		return sample;
	}
	return NULL;
}


/** 
 * 采样结束，释放资源 
 *
 * @param handle 采样句柄
 */
void BigLobStorage::endSample(SampleHandle *handle) {
	BigLobSampleHandle *shdl = (BigLobSampleHandle *)handle;
	if (handle->m_fastSample) {
		m_buffer->endScan(shdl->m_bufScanHdl);
	} else {
		assert(!shdl->m_bufScanHdl);
	}
	delete shdl;
}


/** 
 * 采样一个区间的页面, 得到一个采样
 *
 * @param page 页对象
 * @param handle 采样句柄
 * @param pid 页号
 * @return 一个采样
 */
Sample* BigLobStorage::sampleRegionPage(BufferPageHandle *page, SampleHandle *handle, u32 pid){
	int freePage = 0;
	int usedLen = 0;
	int usedPage = 0;
	u32 pageNum = 0;
	BufferPageHandle *firstPageHdl = page;
	BigLobSampleHandle *shdl = (BigLobSampleHandle *)handle;
	LobBlockFirstPage *liFirstPage;

	// page肯定是首页
	while (pid < (shdl->m_minPage + shdl->m_curBlock * shdl->m_regionSize + shdl->m_blockSize - 1)) {
		liFirstPage = (LobBlockFirstPage *)firstPageHdl->getPage();
		pageNum = (liFirstPage->m_len + Limits::PAGE_SIZE - 1) / Limits::PAGE_SIZE;
		if (liFirstPage->m_isFree) {
			freePage += pageNum;
		} else {
			usedLen += liFirstPage->m_len; 
			usedPage += (liFirstPage->m_len + Limits::PAGE_SIZE - 1) / Limits::PAGE_SIZE;
		}
		pid += pageNum;
		if (pid >= shdl->m_maxPage)
			break;
		handle->m_session->releasePage(&firstPageHdl);
		firstPageHdl = GET_PAGE(handle->m_session, m_file, PAGE_LOB_HEAP, pid, Shared, m_dboStats, NULL);
	}
	handle->m_session->releasePage(&firstPageHdl);
	Sample *sample = Sample::create(handle->m_session, SAMPLE_FIELDS_NUM);
	(*sample)[SAMPLE_FIELDS_NUM - 3] = freePage;
	(*sample)[SAMPLE_FIELDS_NUM - 2] = usedLen;
	(*sample)[SAMPLE_FIELDS_NUM - 1] = usedPage;
	sample->m_ID = pid;
	return sample;
}


/** 
 * 采样一个buffer的页面, 得到一个采样
 *
 * @param page 页对象
 * @param handle 采样句柄
 * @return 一个采样
 */
Sample* BigLobStorage::sampleBufferPage(BufferPageHdr *page, SampleHandle *handle){
	LobBlockOtherPage *liOtherPage = (LobBlockOtherPage *)page;
	int freePage = 0;
	int usedLen = 0;
	if (liOtherPage->m_isFirstPage) {
		LobBlockFirstPage *liFirstPage = (LobBlockFirstPage *)page;
		if (liFirstPage->m_isFree) {
			freePage = (liFirstPage->m_len + Limits::PAGE_SIZE - 1) / Limits::PAGE_SIZE;
		} else {
			usedLen = liFirstPage->m_len; 
		}
	} 

	Sample *sample = Sample::create(handle->m_session, SAMPLE_FIELDS_NUM);
	(*sample)[SAMPLE_FIELDS_NUM - 2] = freePage;
	(*sample)[SAMPLE_FIELDS_NUM - 1] = usedLen;
	return sample;
}


/** 
 * 根据大对象的估计的平均长度，得到采样的区块大小 
 *
 * @param averageLen 大对象的平均长度
 * @return 采样的区块长度
 */
u32 BigLobStorage::getRegionSize(u32 averageLen) {
	u32 minLobSize = 8 * 1024;
	u32 minRegionSize = 1024 * 1024;
	u32 lobSize = minLobSize;
	u32 regionSize = minRegionSize;
	while (averageLen > lobSize) {
		regionSize <<= 1;
		lobSize <<= 1;
	}
	return regionSize / Limits::PAGE_SIZE;
}


/** 
 * 寻找不可采样的页面,这里就是找到block内第一个大对象的首页 
 * @param session 会话对象
 * @param minPage  该区的起始页号
 * @param regionSize 区的长度
 * @param startPid out,该区采样的起始页号
 * @return 页句柄
 */
BufferPageHandle* BigLobStorage::unsamplablePagesBetween(Session *session, u64 minPage, u64 regionSize, u32 *startPid) {
	u32 pid = (u32)minPage;
	BufferPageHandle *pageHdl = GET_PAGE(session, m_file, PAGE_LOB_HEAP, pid, Shared, m_dboStats, NULL);
	LobBlockOtherPage *otherPage = (LobBlockOtherPage *)pageHdl->getPage();
	while (!otherPage->m_isFirstPage ) {
		session->releasePage(&pageHdl);
		// 假如出区域了
		if (++pid > minPage + regionSize) {
			return NULL;
		}
		pageHdl = GET_PAGE(session, m_file, PAGE_LOB_HEAP, pid, Shared, m_dboStats, NULL);
		otherPage = (LobBlockOtherPage *)pageHdl->getPage();
	}
	*startPid = pid;
	return pageHdl;
}

}

