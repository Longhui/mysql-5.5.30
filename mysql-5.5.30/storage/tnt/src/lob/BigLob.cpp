 /**
  * ���ʹ�������
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
 * Txn��־��ʽ
 * insert log: LobId(u64) PageId(u32) Flags(u8) ( New FreePageListHeader(u32) ) isCompress(u8) (Org_len(u32)) LobLen(u32) LobData(byte*)
 * delete log: LobId(u64) PageId(u32) Flags(u8) ( Old FreePageListHeader(u32) )
 * update log: LobId(u64) PageID(u32) isNewPageID (u8) (new PageID) isNewFreeBlock(u8) (newFreeBlockPageID(u32) newFreeBlockLen(u32)) LobLen(u32)
 * move log  : LobId(u64) PageID(u32)  newPageID(u32) LobLen(u32) LobData(byte*)
 *
 */


/**
 * ����������ļ�
 *
 * @param db ���ݿ����
 * @param tableDef ��������
 * @param blobPath ������ļ�����·��
 * @param indexPath Ŀ¼�ļ�����·��
 * @throw �ļ��޷�������IO����ȵ�
 */
void BigLobStorage::create(Database *db, const TableDef *tableDef, const char *basePath)  throw(NtseException) {
	string blobPath = string(basePath) + Limits::NAME_LOBD_EXT;
	string indexPath = string(basePath) + Limits::NAME_LOBI_EXT;

	//�ȴ���������ļ�
	createLobFile(db, tableDef, blobPath.c_str());
	//����Ŀ¼�ļ�
	try {
		LobIndex::create(indexPath.c_str(), db, tableDef);
	} catch(NtseException &e) {
		//�����Ѿ����ɵĴ�����ļ�
		File *blobFile = new File(blobPath.c_str());
		blobFile->remove();
		delete blobFile;
		throw e;
	}
}

/** 
 * �������ʹ�����ļ�
 *
 * @param db ���ݿ����
 * @param tableDef ��������
 * @param blobPath ������ļ�����·��
 * @throw �ļ��޷�������IO�����
 */
void BigLobStorage::createLobFile(Database *db, const TableDef *tableDef, const char *blobPath) throw(NtseException){
	u64 errCode;
	// ����������ļ�
	File *blobFile = new File(blobPath);

	errCode = blobFile->create(true, false);
	if (File::E_NO_ERROR != errCode) {
		delete blobFile;
		NTSE_THROW(errCode, "Cannot create lob file %s", blobPath);
	}

	// �������ȳ�ʼ��һ���飬��СΪDatabase::getBestIncrSize(tableDef, 0)��ҳ
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
 * ���캯��
 *
 * @param db ���ݿ����
 * @param file ������ļ� 
 * @param lobi �����Ŀ¼ʵ��
 * @param dbObjStats ���ݶ���״̬
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
 * ��������
 */
BigLobStorage::~BigLobStorage() {
	delete m_file;
	delete m_lobi;
}

/**
 * �򿪴�����ļ�
 *
 * @param db ���ݿ����
 * @param session �Ự����
 * @param lobPath ������ļ�·��
 * @param indexPath Ŀ¼�ļ�·��
 * @throw �ļ��޷�������IO����ȵ�
 * @return BigLob����
 */
BigLobStorage* BigLobStorage::open(Database *db, Session *session, const char *basePath) throw(NtseException) {
	u64 errCode;
	string lobPath = string(basePath) +  Limits::NAME_LOBD_EXT;
	string indexPath = string(basePath) + Limits::NAME_LOBI_EXT;
	// �򿪴�����ļ�
	File *blobfile = new File(lobPath.c_str());

	errCode = blobfile->open(db->getConfig()->m_directIo);
	if (File::E_NO_ERROR != errCode) {
		delete blobfile;
		NTSE_THROW(errCode, "Cannot open lob file %s", lobPath.c_str());
	}

	// ����LobIndex����
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
 * �Դ�����ļ���������
 *
 * @param session �Ự
 * @param fileLen ԭ���ļ�����
 * @param size Ҫ���ŵ�ҳ��
 */
void BigLobStorage::extendNewBlock(Session *session, u32 fileLen, u16 size) {
	u64 errCode;
	if (File::E_NO_ERROR != (errCode = m_file->setSize((u64)Limits::PAGE_SIZE * (u64)(fileLen + size))))
		m_db->getSyslog()->fopPanic(errCode, "Extending lob file file '%s' size failed!",m_file->getPath());

	for (uint i = 0; i < size; ++i) {
		u64 pageNum = fileLen + i; // maxPageNum��û�иı�
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
 * �õ���������ʱ��ĵõ���ʼID�������ȵõ���ҳX����Ȼ��õ�����ҳ����Ȼ���ͷ���ҳX��
 *
 * @post �õ�����ҳ��X��
 * @param tableDef ����
 * @param pageNum �������Ҫռ�е�ҳ��
 * @param session �Ự����
 * @param pageId out ��������ʼPageId
 * @return  �������ҳ�ľ��
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

	// �ж��Ƿ���Ҫ��չ������ļ�
	// ���統ǰ�ļ�β��<����󳤶�
	extendLobFile(session, tableDef, fileTail, pageNum, fileLen, headerPage);
	headerPage->m_blobFileTail += pageNum;
	// ��newPage,���ҳ�ǽ�Ҫд��Ĵ����Ŀ���ҳ
	blockHeaderPageHdl = NEW_PAGE(session, m_file, PAGE_LOB_HEAP, fileTail, Exclusived, m_dboStats);
	// �ͷ���ҳ
	session->markDirty(headerPageHdl);
	m_lobi->unlockHeaderPage(session, headerPageHdl);
	return blockHeaderPageHdl;
}

/**
 * ��������
 *
 * @param session �Ự����
 * @param tableDef ����
 * @param lob ���������
 * @param size �����ĳ���
 * @param orgSize ������ԭ������
 * @return �����ID
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

	// �õ�����ҳ����ʼPageId
	bloFirstPageHdl = getStartPageIDAndPage(tableDef, pageNum, session, &pageId);
	SYNCHERE(SP_LOB_BIG_INSERT_LOG_REVERSE_1);
	// �õ�����Ŀ¼��
	lid = m_lobi->getFreeSlot(session, &liFilePageHdl, &isHeaderPageLock, &headerPageHdl, &newHeaderList, pageId);
	// verifyĿ¼�ļ�ҳ�Ƿ���ȷ
	lobId = lid;
	MemoryContext *mc = session->getMemoryContext();
	u64 savePoint = mc->setSavepoint();

#ifdef TNT_ENGINE
	tnt::TNTTransaction *trx = session->getTrans();
	if(trx != NULL)
		LobStorage::writeTNTInsertLob(session,trx->getTrxId(),tableDef->m_id,trx->getTrxLastLsn(),lobId);
#endif

	if (isHeaderPageLock) {// ˵����getFreeSlot�У���ҳ����ס��
		// д��־
		lsn = logInsert(session, lobId, pageId, true, newHeaderList, m_lobi->getTableID(), orgSize, size, lob, mc);
		// �ͷ���ҳ
        liFileHeaderPage = (LIFileHeaderPageInfo *)headerPageHdl->getPage();
        liFileHeaderPage->m_bph.m_lsn = lsn;
		session->markDirty(headerPageHdl);
		m_lobi->unlockHeaderPage(session, headerPageHdl);
	} else {
		// д��־

		lsn = logInsert(session, lobId, pageId, false, 0, m_lobi->getTableID(), orgSize, size, lob, mc);
	}
	mc->resetToSavepoint(savePoint);

	SYNCHERE(SP_LOB_BIG_INSERT_LOG_REVERSE_2);

	// �޸�Ŀ¼ҳLSN�������޸��Ѿ���getFreeSlot���
	liFilePage = (LIFilePageInfo *)liFilePageHdl->getPage();
	liFilePage->m_bph.m_lsn = lsn;
	// �ͷ�Ŀ¼ҳ
	session->markDirty(liFilePageHdl);
	session->releasePage(&liFilePageHdl);
	// ����backup�������⣬��Ҫ��֤m_isFirstPage
	//vecode(vs.lob, m_lobi->verifyPage(session, LID_GET_PAGE(lid)));

	uint haveCopyLen;
	byte *bufFirstPage;
	// ��ʼд����
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
	if (pageNum == 1) {// ֻ��һ��ҳ
		// copy��ҳ����
		memcpy(bufFirstPage, lob, size);
	} else {
		memcpy(bufFirstPage, lob, haveCopyLen);
		// copy��������ҳ
		writeLob(session, pageId, pageNum, haveCopyLen, lsn, lob, size);
	}

	// ����ͷſ���ҳ��
	session->markDirty(bloFirstPageHdl);
	session->releasePage(&bloFirstPageHdl);
	
	// ͳ����Ϣ
	m_dboStats->countIt(DBOBJ_ITEM_INSERT);
	return lobId;
}

/**
 * �Ѵ��ʹ����ĳ���ת����Ӧ��д��Ŀ鳤�ȣ�����ҳͷ��
 *
 * @param size ������ֽ����ĳ���
 * @return д��ʱ����ܳ��ȣ��鳤�ȣ�
 */
uint BigLobStorage::getBlockSize(uint size) {
	uint offset = OFFSET_BLOCK_FIRST;

	if (size <= Limits::PAGE_SIZE - offset) // ����ֻ��һҳ
		return size + offset;

	uint remainSize = size - (Limits::PAGE_SIZE - offset);
	uint pageNum = (remainSize + Limits::PAGE_SIZE - OFFSET_BLOCK_OTHER - 1) / (Limits::PAGE_SIZE - OFFSET_BLOCK_OTHER);
	// ���һҳ�ĳ���
	uint lastPageLen = remainSize - (pageNum - 1) * (Limits::PAGE_SIZE - OFFSET_BLOCK_OTHER) + OFFSET_BLOCK_OTHER;

	return pageNum * Limits::PAGE_SIZE + lastPageLen;
}

/**
 * ��ĳ���ת����LOB���ȣ������ĳ���ָռ����ռ䳤�ȣ�����ҳͷ��
 * LOB����ָ���ݵ�ʵ�ʳ���
 *
 * @param size �鳤��
 * @return ��������ݳ���
 */
uint BigLobStorage::getLobSize(uint size) {
	// ����С��һҳ
	if (size <= Limits::PAGE_SIZE)
		return size - OFFSET_BLOCK_FIRST;

	uint pageNum = (size + Limits:: PAGE_SIZE - 1) / Limits::PAGE_SIZE;
	return size - OFFSET_BLOCK_FIRST  -  (pageNum - 1) * OFFSET_BLOCK_OTHER;
}

/**
 * ��ȡ�����
 *
 * @param session �Ự����
 * @param mc �ڴ�������
 * @param lobId �����ID
 * @param size out ����󳤶�
 * @param org_size out ������ѹ���ĳ���
 * @return ���������
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
	if(isFree) {		//���δ�ҵ�������򷵻�
		session->releasePage(&liFilePageHdl);	
		*size = 0;
		return NULL;
	}
	// Ŀ¼�������not free
	assert(!isFree);
	pageID = slotInfo->u.m_pageId;
	session->releasePage(&liFilePageHdl);

	// ͬ����
	SYNCHERE(SP_LOB_BIG_READ_DEFRAG_READ);
	isFind = readBlockAndCheck(session, mc, pageID, lobId, &dataStream, size, org_size);
	if (isFind) {
		m_dboStats->countIt(DBOBJ_ITEM_READ);
		return dataStream;
	}
	goto check_page;
}

/**
 * ��ȡһ����������ݣ����ж�ʱ���Ƿ��ƶ��������û���ƶ����򷵻ش��������
 *
 * @param session�Ự����
 * @param mc �ڴ����������
 * @param pid ��������ʼҳ��
 * @param lid �����ID
 * @param dataSteam out ���������
 * @param size out ������С
 * @param orgSize out ���粻ѹ��������£������Ĵ�С
 * @return �Ƿ��Ѿ���ȡ�������
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
	// �ж�IsFree��ReferenceID��IsFirstPage������ΪIsfreeΪfalse
	// ����ReferenceID=BlobID, IsFirstPage=1���ȡ�����ҳ���õ���ĳ��ȵ���Ϣ
	if (!isFree && isFirstPage && lid == llid) {
		u32 blockLen = blockHeaderPage->m_len;
		// �����ڴ�
		uint lobSize = getLobSize(blockLen);
		*dataSteam = (byte *)mc->alloc(lobSize);
		*orgSize = blockHeaderPage->m_srcLen;
		*size = getLobContent(session, *dataSteam, blockHeaderPage, pid);
		assert(*size == lobSize);
		// ����ͷſ����ҳ��
        session->releasePage(&blockHeaderPageHdl);
        return true;
	}
	*size = 0;
	*orgSize = 0;
	session->releasePage(&blockHeaderPageHdl);
	return false;
}

/**
 * �õ�����������
 *
 * @param session�Ự����
 * @param data out ���������
 * @param blockHeaderPage ��������ҳ
 * @param pid ��ʼҳ��
 * @return ����󳤶�
 */
uint BigLobStorage::getLobContent(Session *session, byte *data, LobBlockFirstPage *blockHeaderPage, u32 pid) {
	byte *firstPageAddr;
	uint firstOffset;
	uint copyLen;
	uint size;
	BufferPageHandle  *blockOtherPageHdl;
	LobBlockOtherPage  *blockOtherPage;
	u32 blockLen = blockHeaderPage->m_len;
	// �����ռ�е�ҳ��
	u32 pageNum = getPageNum(blockLen);

	firstPageAddr = (byte *)(blockHeaderPage) + OFFSET_BLOCK_FIRST;
	copyLen = Limits::PAGE_SIZE - OFFSET_BLOCK_FIRST;
	firstOffset = OFFSET_BLOCK_FIRST;
	// ����ֻ��һҳ����
	if (pageNum == 1) {
		// copy��ҳ����
		memcpy(data, firstPageAddr, blockLen - firstOffset);
		size = blockLen - firstOffset;
	} else {
		// copy��ҳ����
		memcpy(data, firstPageAddr, copyLen);
		// copy����ҳ����
		uint i = 1;
		while (i < pageNum) {
			blockOtherPageHdl = GET_PAGE(session, m_file, PAGE_LOB_HEAP, pid + i, Shared, m_dboStats, NULL);
			blockOtherPage = (LobBlockOtherPage *)blockOtherPageHdl->getPage();
			byte *otherPageData = (byte *)(blockOtherPage) + OFFSET_BLOCK_OTHER;
			// ���絽���һ��ҳ������
			if (i == pageNum - 1) {
				// ���һҳ���ݳ���
				uint lastLen =  blockLen - (pageNum - 1) * Limits::PAGE_SIZE  - OFFSET_BLOCK_OTHER;
				memcpy(data + copyLen, otherPageData, lastLen);
				copyLen += lastLen;
			} else {
				memcpy(data + copyLen, otherPageData, (Limits::PAGE_SIZE - OFFSET_BLOCK_OTHER));
				copyLen += Limits::PAGE_SIZE - OFFSET_BLOCK_OTHER;
			}
			// unlock ��Щ����ҳ
			session->releasePage(&blockOtherPageHdl);
			i++;
		}
		size = copyLen;
	}
	return size;
}

/**
 * ɾ�������
 *
 * @param session�Ự����
 * @param LobId �����ID
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
 * ɾ�������  ����crash�ָ�
 *
 * @param session�Ự����
 * @param LobId �����ID
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
	if(slotInfo->m_free) {		//���δ�ҵ�������򷵻�
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
 * ɾ��һ������󣬲��ж�ʱ���Ƿ��ƶ��������û���ƶ�����ɾ��
 *
 * @param session�Ự����
 * @param pid ��������ʼҳ��
 * @param lid �����ID
 * @param tid ��ID
 * @return �Ƿ��Ѿ��ɹ�ɾ�������
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
	// �ж�IsFree��ReferenceID��IsFirstPage������ΪIsfreeΪfalse
	// ����ReferenceID=BlobID, IsFirstPage=1���ȡ�����ҳ���õ���ĳ��ȵ���Ϣ
	if (!isFree && isFirstPage && lid == llid) {
		blockHeaderPage->m_isFree = true;
		// ȥ��ȡ��Ӧ��Ŀ¼��ҳ,���޸�
		u32 indexPid = LID_GET_PAGE(lid);
		u16 slotNum = LID_GET_SLOT(lid);
		u64 newlsn = 0;
		BufferPageHandle *headPageHdl;
		liFilePageHdl = GET_PAGE(session, m_lobi->m_file, PAGE_LOB_INDEX, indexPid, Exclusived, m_dboStats, NULL);
		liFilePage = (LIFilePageInfo *)liFilePageHdl->getPage();
		LiFileSlotInfo *slotInfo = m_lobi->getSlot(liFilePage, slotNum);
		slotInfo->m_free = true;
		// �����Ŀ¼ҳ�ǵ�һ���п��в�,����Ҫ�������ҳ����
		if (liFilePage->m_inFreePageList == false) {
			// �õ���ҳ,���ÿ���ҳ����
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
		} else {// �޸�ҳ�ڿ�������
			newlsn = logDelete(session, lid, pid, false, 0, tid);
			headPageHdl = m_lobi->lockHeaderPage(session, Exclusived);
			slotInfo->u.m_nextFreeSlot = liFilePage->m_firstFreeSlot;
			liFilePage->m_freeSlotNum += 1;
		}
		// ͳ����Ϣ
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
 * ���´����
 *
 * @param session�Ự����
 * @param tableDef ����
 * @param lobId �����ID
 * @param lob  ������µ�����
 * @param size ����󳤶�
 * @param orgSize ѹ��ǰ�Ĵ�����С
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
 * д�����ҳ
 *
 * @pre д����ҳ�Ѿ���ɣ������������ҳ
 * @param session �Ự����
 * @param pid ��ʼ��ҳ��
 * @param pageNum ����������ҳʣ�µ�ҳ��
 * @param offset �Ѿ�����ҳд�����ݳ���
 * @param lsn д���lsn
 * @param lob ���������
 * @param size ����󳤶�
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
		if(i == pageNum - 1) {// �����һҳ
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
 * ����һ����������ж�ʱ���Ƿ��ƶ��������û���ƶ�������´����
 *
 * @param session�Ự����
 * @param tableDef ����
 * @param pid ��������ʼҳ��
 * @param lid �����ID
 * @param lob ���������
 * @param size ����󳤶�
 * @param orgSize �����ѹ��ǰ����
 * @return �Ƿ��Ѿ��ɹ����¶���
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
	// �ж�IsFree��ReferenceID��IsFirstPage������ΪIsfreeΪfalse
	// ����ReferenceID=BlobID, IsFirstPage=1���ȡ�����ҳ���õ���ĳ��ȵ���Ϣ
	if (!isFree && isFirstPage && lid == llid) {
		LobId newLid = lid;
		uint haveCopyLen = Limits::PAGE_SIZE - OFFSET_BLOCK_FIRST;
		uint offset = OFFSET_BLOCK_FIRST;

		// �ȶ�ȡԭ����󳤶�
		preBlockLen = blockHeaderPage->m_len;
		newBlockLen = getBlockSize(size);
		u32 prePageNum = getPageNum(preBlockLen);
		u32 newPageNum = getPageNum(newBlockLen);
		// ��������ռ�е�ҳ��û�����С��
		if (prePageNum >= newPageNum) {
			// ��������ռ�е�ҳ��û��
			if (prePageNum == newPageNum) {
				lsn = logUpdate(session, newLid, pid, false, 0, false, 0, 0, size, m_lobi->getTableID(), orgSize);
			} else {
				// ��������п�
				int newFreeBlockStartPageID = pid + newPageNum;
				// д��־
				lsn = logUpdate(session, newLid, pid, false, 0, true, newFreeBlockStartPageID, prePageNum - newPageNum, size, m_lobi->getTableID(), orgSize);
				BufferPageHandle *newBlockHeaderPageHdl = NEW_PAGE(session, m_file, PAGE_LOB_HEAP, newFreeBlockStartPageID, Exclusived, m_dboStats);
				LobBlockFirstPage *newBlockHeaderPage = (LobBlockFirstPage *)newBlockHeaderPageHdl->getPage();
				createNewFreeBlock(session, newBlockHeaderPageHdl, newBlockHeaderPage, lsn, prePageNum - newPageNum);
			}
			blockHeaderPage->m_srcLen = orgSize;

			// �޸�����
			blockHeaderPage->m_bph.m_lsn = lsn;
			blockHeaderPage->m_lid = newLid;
			blockHeaderPage->m_len = newBlockLen;
			if (newPageNum == 1) {// ֻ��һ��ҳ
				// copy��ҳ����
				memcpy((byte *)(blockHeaderPage) + offset, lob, size);
			} else {
				memcpy((byte *)(blockHeaderPage) + offset, lob, haveCopyLen);
				// copy��������ҳ
				writeLob(session, pid, newPageNum, haveCopyLen, lsn, lob, size);
			}
			// ����ͷſ���ҳ
			session->markDirty(blockHeaderPageHdl);
			session->releasePage(&blockHeaderPageHdl);

			// ����ͳ����Ϣ
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

		// ���������
		if (preBlockLen < newBlockLen) {
			BufferPageHandle  *headPageHdl = m_lobi->lockHeaderPage(session, Exclusived);
			LIFileHeaderPageInfo *headPage = (LIFileHeaderPageInfo *)headPageHdl->getPage();
			uint fileTail = headPage->m_blobFileTail;
			// ������������
			// 1���޸Ĵ�������ļ�ĩβ
			if (pid + prePageNum == fileTail) {
				u32 newfileTail = newPageNum - prePageNum + fileTail;
				u32 fileLen =  headPage->m_blobFileLen;
				// �ж��Ƿ���Ҫ��չ������ļ�
				// ���統ǰ�ļ�β��<����󳤶�
				extendLobFile(session, tableDef, newfileTail, 0, fileLen, headPage);
				headPage->m_blobFileTail = newfileTail;
				session->markDirty(headPageHdl);
				m_lobi->unlockHeaderPage(session, headPageHdl);
				// д��־
				lsn = logUpdate(session, newLid, pid, false, 0, false, 0, 0, size, m_lobi->getTableID(), orgSize);
				// �޸���ҳ����
				blockHeaderPage->m_srcLen = orgSize;
				blockHeaderPage->m_bph.m_lsn = lsn;
				blockHeaderPage->m_lid = newLid;
				blockHeaderPage->m_len = newBlockLen;
				// ��ʼд���ݣ���������ݿ϶��ǳ���һҳ�ĳ���
				memcpy((byte *)(blockHeaderPage) + offset, lob, haveCopyLen);
				// copy��������ҳ
				writeLob(session, pid, newPageNum, haveCopyLen, lsn, lob, size);

				// ����ͷſ���ҳ
				session->markDirty(blockHeaderPageHdl);
				session->releasePage(&blockHeaderPageHdl);
				//vecode(vs.lob, verifyLobPage(session, pid));
				return true;
			}

			// m_lobi->unlockHeaderPage(session, headPageHdl);
			// 2�����粻�����һ�飬��PageId+�µĿ鳤>�ļ�ĩβ,����������ܺ���Ƭ����һ��
			// �ڲ���������˳���෴���׷������������������жϺ�����Ƿ����ļ�ĩβʱ��
			// ����ȥ����ҳ�жϣ����Բ�����ȥ�����ڲ�����¿���ҳ�����Բ��γ�������
			// ����ĳ�ֱ���ƶ����ļ�ĩβ
			if (pid + newPageNum > fileTail) {
				m_lobi->unlockHeaderPage(session, headPageHdl);
				assert(pid + prePageNum < fileTail);
				goto moveToTail;
			}

			// 3�����粻�����һ�飬��PageId+�µĿ鳤<�ļ�ĩβ,��̽Ѱ����ʱ���п��п飬
			// ���㹻����ʱ��д��
			// 4�����û���㹻���Ŀ��п齫��������д���ļ�ĩβ
			if (pid + newPageNum <= fileTail) {
				u32 newFreeBlockLen;
				u8 isFind = isNextFreeBlockEnough(session, pid, prePageNum, newPageNum - prePageNum, &newFreeBlockLen);
				// ��������ԭ��
				if (isFind) {
					if (isFind == 1) {// �µĿ��п����
						// д��־
						lsn = logUpdate(session, lid, pid, false, 0, true, pid + newPageNum, newFreeBlockLen, size, m_lobi->getTableID(), orgSize);
						BufferPageHandle *newFreeBlockHeaderPageHdl = NEW_PAGE(session, m_file, PAGE_LOB_HEAP, pid + newPageNum, Exclusived, m_dboStats);
						LobBlockFirstPage *newFreeBlockHeaderPage =(LobBlockFirstPage *)newFreeBlockHeaderPageHdl->getPage();
						// �����µĿ��п�
						createNewFreeBlock(session, newFreeBlockHeaderPageHdl, newFreeBlockHeaderPage, lsn, newFreeBlockLen);
					}
					if (isFind == 2) {// û���µĿ��п�������Ǽ����������п�պõ�����չ�ĳ���
						// д��־
						lsn = logUpdate(session, newLid, pid, false, 0 ,false, 0, 0, size, m_lobi->getTableID(), orgSize);
					}

					// ��ʼд���ݣ���������ݿ϶��ǳ���һҳ�ĳ���
					blockHeaderPage->m_bph.m_lsn = lsn;
					blockHeaderPage->m_len = newBlockLen;
					blockHeaderPage->m_lid = newLid;
					blockHeaderPage->m_srcLen = orgSize;
					memcpy((byte *)(blockHeaderPage) + offset, lob, haveCopyLen);
					// copy��������ҳ
					writeLob(session, pid, newPageNum, haveCopyLen, lsn, lob, size);
					// ����ͷſ���ҳ
					session->markDirty(blockHeaderPageHdl);
					session->releasePage(&blockHeaderPageHdl);

					// ͳ������
					// BufferPageHandle *headPageHdl = m_lobi->lockHeaderPage(session, Exclusived);
					LIFileHeaderPageInfo *headPage = (LIFileHeaderPageInfo *)headPageHdl->getPage();
					headPage->m_blobFileFreeBlock += 1;
					headPage->m_blobFileFreeLen -= (newPageNum - prePageNum);
					session->markDirty(headPageHdl);
					m_lobi->unlockHeaderPage(session, headPageHdl);

					//vecode(vs.lob, verifyLobPage(session, pid));
					return true;
				}
				// �����ݷŵ��ļ�ĩβ
				m_lobi->unlockHeaderPage(session, headPageHdl);
				goto moveToTail;
				// m_lobi->unlockHeaderPage(session, headPageHdl);   ����ִ�е����
			}
			// m_lobi->unlockHeaderPage(session, headPageHdl);   ����ִ�е����
moveToTail:
			u32 newPid;
			BufferPageHandle *newBlockFirstPageHdl = getStartPageIDAndPage(tableDef, newPageNum, session, &newPid);
			// ��ΪҪ�޸Ķ�Ӧ����ʼPageID,����Ҫȥ�޸Ķ�ӦĿ¼ҳ
			u32 indexPid = LID_GET_PAGE(lid);
			BufferPageHandle *liFilePageHdl = GET_PAGE(session, m_lobi->m_file, PAGE_LOB_INDEX, indexPid, Exclusived, m_dboStats, NULL);
			LIFilePageInfo *liFilePage = (LIFilePageInfo *)liFilePageHdl->getPage();
			// д��־
			lsn = logUpdate(session, newLid, pid, true, newPid, true, pid, prePageNum, size, m_lobi->getTableID(), orgSize);
			liFilePage->m_bph.m_lsn = lsn;
			u16 slotNum = LID_GET_SLOT(lid);
			LiFileSlotInfo *slotInfo=m_lobi->getSlot(liFilePage,slotNum);
			slotInfo->u.m_pageId = newPid;
			// �ͷ�Ŀ¼ҳ
			session->markDirty(liFilePageHdl);
			session->releasePage(&liFilePageHdl);
			// ��ʼд���ݣ���������ݿ϶��ǳ���һҳ�ĳ���
			LobBlockFirstPage *newDataBlockHeaderPage = (LobBlockFirstPage *)newBlockFirstPageHdl->getPage();
			memset(newDataBlockHeaderPage, 0, Limits::PAGE_SIZE);
			newDataBlockHeaderPage->m_bph.m_lsn = lsn;
			newDataBlockHeaderPage->m_len = newBlockLen;
			newDataBlockHeaderPage->m_isFirstPage = true;
			newDataBlockHeaderPage->m_lid = newLid;
			newDataBlockHeaderPage->m_isFree = false;
			newDataBlockHeaderPage->m_srcLen = orgSize;
			memcpy((byte *)(newDataBlockHeaderPage) + offset, lob, haveCopyLen);
			// copy��������ҳ
			writeLob(session, newPid, newPageNum, haveCopyLen, lsn, lob, size);
			// �ͷ��¿���ҳ
			session->markDirty(newBlockFirstPageHdl);
			session->releasePage(&newBlockFirstPageHdl);
			// ����ͷžɿ���ҳ
			blockHeaderPage->m_bph.m_lsn = lsn;
			blockHeaderPage->m_isFree = true;
			session->markDirty(blockHeaderPageHdl);
			session->releasePage(&blockHeaderPageHdl);

			// ͳ������
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
 * �����µĿ��д����
 *
 * @param session �Ự����
 * @param blockHeaderPageHdl ����ҳ�ľ��
 * @param lobFirstPage �����ҳ
 * @param lsn LSN
 * @param freePageNum ���п��ҳ��
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
 * �ж�һ���������������п����Ƿ����һ�����ȣ�����ڴ�������ʱ����
 *
 * @param session �Ự����
 * @param pid ��������ʼPageId
 * @param pageNum �����ռ�е�ҳ��
 * @param needLen ���±䳤�ĳ�����Ҫ��ҳ��
 * @param newBlockLen out �²����Ŀ��п��ҳ��
 * @return ���ﷵ�����������0��ʾû�ҵ������㹻��Ŀ��пռ�
 *         1��ʾ�ҵ��¿��п�������������µĿ��п�
 *         2��ʾ�ҵ��㹻���п飬�������¿��п�
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
	// �������ζ�ȡ����飬�õ��Ƿ��ǿ��У����ж��Ƿ�������Ҫ����
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
	// ˵�������������пռ��ǹ���
	if (i == needLen)
		return 2;
	else if (i > needLen) {
		*newBlockLen = i - needLen;
		return 1;
	}
	return 0;
}


/**
 * дINSERT��־
 * ������Ϊ��Ҫ��Ŀ¼�ļ��ʹ�����ļ����޸ķ���һ��,������Ҫ��¼�����ļ��ı仯���
 *
 * @param session �Ự����
 * @param lid �����ID
 * @param pid ��������ʼҳ
 * @param headerPageModified ���������Ƿ�ı�
 * @param newListHeader �µĿ���ҳ����ͷ
 * @param tid ��ID
 * @param orgLen ѹ��ǰ����
 * @param lobSize ����󳤶�
 * @param lobData ���������
 * @param mc �ڴ�������
 * @return lsn
 */
u64 BigLobStorage::logInsert(Session *session, LobId lid, u32 pid, bool headerPageModified, u32 newListHeader, u16 tid, u32 orgLen, u32 lobSize,const byte* lobData, MemoryContext *mc) {
	// �ȷ����ڴ�
	byte *logData = (byte *)mc->alloc(32 + lobSize);
	Stream s(logData, 32 + lobSize);
	try {
		// LobId
		s.write(lid);
		// PageId
		s.write(pid);
		// flags, Ŀ¼�ļ�ͷҳ�Ƿ񱻸ı�
		s.write(headerPageModified);
		if (headerPageModified) s.write(newListHeader);
		s.write(orgLen);
		// data���Ⱥ�data
		s.write(lobSize)->write(lobData, lobSize);
	} catch (NtseException &) {
		assert(false);	// ���ﲻ���ܳ����쳣
	}
	return session->writeLog(LOG_LOB_INSERT, tid, logData, s.getSize());

}

/**
 * дDELETE��־
 * ������Ϊ��Ҫ��Ŀ¼�ļ��ʹ�����ļ����޸ķ���һ��,������Ҫ��¼�����ļ��ı仯���
 *
 * @param session �Ự����
 * @param lid �����ID
 * @param pid ��������ʼҳ
 * @param headerPageModified ���������Ƿ�ı�
 * @param oldListHeader �ϵĿ���ҳ����ͷ
 * @param tid ��ID
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
		// flags, Ŀ¼�ļ�ͷҳ�Ƿ񱻸ı�
		s.write(headerPageModified);
		if (headerPageModified) s.write(oldListHeader);
	} catch (NtseException &) {
		assert(false);	// ���ﲻ���ܳ����쳣
	}
	return session->writeLog(LOG_LOB_DELETE, tid, logData, s.getSize());
}

/**
 * дUPDATE��־
 * ������Ϊ��Ҫ��Ŀ¼�ļ��ʹ�����ļ����޸ķ���һ��,������Ҫ��¼�����ļ��ı仯���
 *
 * @param session �Ự����
 * @param lid �����ID
 * @param pid ������ԭ����ʼҳ��
 * @param isNewPageId �Ƿ��������ʼҳ�б仯
 * @param newPageId ���º��������ʼҳ��
 * @param isNewFreeBlock �Ƿ�����µĿ��п�
 * @param freeBlockPageId �µĿ��п����ʼҳ��
 * @param freeBlockLen �µĿ��п�ռ�е�ҳ��
 * @param lobSize ���º�����ĳ���
 * @param tid ��ID
 * @param org_size �����ѹ��ǰ����
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
		// flags, ��ʼҳ���Ƿ񱻸ı�
		s.write(isNewPageId);
		if (isNewPageId) s.write(newPageId);
		// flags,�Ƿ�����µĿ��п�
		s.write(isNewFreeBlock);
		if (isNewFreeBlock) {
			s.write(freeBlockPageId);
			s.write(freeBlockLen);
		}
		s.write(lobSize);
		s.write(org_size);
	} catch (NtseException &) {
		assert(false);	// ���ﲻ���ܳ����쳣
	}
	return session->writeLog(LOG_LOB_UPDATE, tid, logData, s.getSize());
}

/**
 * ɾ��������ļ���Ŀ¼�ļ�
 *
 * @param lobPath ������ļ�·��
 * @param indexPath Ŀ¼�ļ�·��
 * @throw �ļ��޷�������IO�����
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
 * �رմ����洢
 *
 * @param session �Ự
 * @param flushDirty �Ƿ�д��������
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
	// Ȼ��ر�Ŀ¼�ļ�
	m_lobi->close(session, flushDirty);
	delete m_lobi;
	m_lobi = NULL;
	delete m_dboStats;
	m_dboStats = NULL;
}

/** 
 * ��������󴴽�
 *
 * @param db ���ݿ�
 * @param tableDef ����
 * @param lobPath �����·��
 * @param indexPath ����·��
 * @param tid ��ID
 * @throw �ļ��޷�������IO�����
 */
void BigLobStorage::redoCreate(Database *db, const TableDef *tableDef, const char *basePath, u16 tid) throw(NtseException) {
	string blobPath =  string(basePath) + Limits::NAME_LOBD_EXT;
	string indexPath = string(basePath) + Limits::NAME_LOBI_EXT;

	// ��redoCreate Ŀ¼�ļ�
	LobIndex::redoCreate(db, tableDef, indexPath.c_str(), tid);
	u64 errCode;
	u64 fileSize;

	// ��redo������ļ�
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
	if (fileSize <= Database::getBestIncrSize(tableDef, 0) * Limits::PAGE_SIZE) { // ����˵����û�������¼��redoһ�º���
		lobFile.close();
		lobFile.remove();
		BigLobStorage::createLobFile(db, tableDef, blobPath.c_str());
	} else {
		lobFile.close();
	}
}

/**
 * ���ϻָ�ʱREDO���ʹ����������
 *
 * @param session �Ự����
 * @param tableDef ����
 * @param lsn ��־LSN
 * @param log ��¼���������־����
 * @param size ��־��С
 * @return �����ID
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

	// ��ȡ����
	s.read(&lobId)->read(&pageId)->read(&flags);
	if (flags) 
		s.read(&newListHeader);
	s.read(&orgLen);
	s.read(&dataLen);

    // ���ж�Ҫ��Ҫ����,�������ж�Ŀ¼�ļ�ҳ�ǲ�����Ҫredo
    pid = LID_GET_PAGE(lobId);
	if (pid >= m_lobi->m_headerPage->m_fileLen) {//˵����Ҫ����Ŀ¼�ļ�
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
		/* �ȴ�����в����� */
		LiFileSlotInfo *liFileSlot = m_lobi->getSlot(page, slotNum);
		page->m_firstFreeSlot = liFileSlot->u.m_nextFreeSlot;
		page->m_freeSlotNum--;
		page->m_bph.m_lsn = lsn;

		// ����ʱ����Ķ�����ҳ���Ǳ�Ȼ����ΪĿ¼ҳ���ӿ���������ɾ��
		if (flags) {
			page->m_nextFreePageNum = 0;
			page->m_inFreePageList = 0;
		}
		// �޸�Ŀ¼��
		liFileSlot->m_free = false;
		liFileSlot->u.m_pageId = pageId;
		session->markDirty(pageHdl);
	}
	session->releasePage(&pageHdl);

	// insert�����ı�����ҳ
	if (flags) {
		headerPageHdl = m_lobi->lockHeaderPage(session, Exclusived);
		headerPage = (LIFileHeaderPageInfo *)headerPageHdl->getPage();
		if (headerPage->m_bph.m_lsn < lsn) { // ��Ҫ��дheaderPage.
			headerPage->m_firstFreePageNum = newListHeader;
			headerPage->m_bph.m_lsn = lsn;
			session->markDirty(headerPageHdl);
		}
		m_lobi->unlockHeaderPage(session, headerPageHdl);
	}

	// ���жϴ�����ļ�������ҳ�Ƿ�Ҫredo
	// ��Ϊ�����������������д��־��˳������ǵ���ʼҳ�Ŵ�С˳������ǲ�һ�£������������µĵ���
	// getStartPageIDAndPage����õ�����ͬ����ʼҳ��
	uint blockLen = getBlockSize(dataLen);
	pageNum = getPageNum(blockLen);
	headerPageHdl = m_lobi->lockHeaderPage(session, Exclusived);
	headerPage = (LIFileHeaderPageInfo *)headerPageHdl->getPage();
	u32 fileLen = headerPage->m_blobFileLen;
	if (pageNum + pageId > fileLen) isHeaderPageModified = true;
	// ���統ǰ�ļ�β��<����󳤶�
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
	if (lobFirstHeaderPage->m_bph.m_lsn < lsn) {// ��Ҫredo
		lobFirstHeaderPage->m_isFree = false;
		lobFirstHeaderPage->m_bph.m_lsn = lsn;
		lobFirstHeaderPage->m_isFirstPage = true;
		lobFirstHeaderPage->m_lid = lobId;
		lobFirstHeaderPage->m_len= blockLen;
		lobFirstHeaderPage->m_srcLen = orgLen;
		// д�����ҳ��������
		if (pageNum == 1) {// ֻ��һ��ҳ
			// copy��ҳ����
			memcpy(((byte *)lobFirstHeaderPage) + offset, log + s.getSize(), dataLen);
		} else {
			memcpy(((byte *)lobFirstHeaderPage) + offset, log + s.getSize(), copyLen);
		}
		// �����insert��һ��������ֱ���ͷţ���Ϊredoʱ���ǵ��̵߳�
		session->markDirty(lobFirstHeaderPageHdl);
	}
	session->releasePage(&lobFirstHeaderPageHdl);
	// ���������ҳ������
	if (pageNum > 1) {
		const byte *lobContent = log + s.getSize();
		redoWriteOtherPages(session, lsn, lobContent, copyLen, pageNum, pageId, dataLen);
	}
	//vecode(vs.lob, verifyLobPage(session, pageId));
	return lobId;
}

/**
 * ��redo���ֲ���,����дҳ�����,��Ҫ������ҳ�������ҳ
 *
 * @param session �Ự����
 * @param lsn ��־LSN
 * @param lobContent ������ֽ���
 * @param offset д���ƫ��
 * @param pageNum ҳ��
 * @param pageId �����ʼҳ
 * @param dataLen ���ݳ���
 */
void BigLobStorage::redoWriteOtherPages(Session *session, u64 lsn, const byte *lobContent, uint offset, u32 pageNum, u32 pageId, u32 dataLen) {
	for(uint i = 1; i < pageNum; i++) {
		BufferPageHandle *bloOtherPageHdl = GET_PAGE(session, m_file, PAGE_LOB_HEAP, pageId + i, Exclusived, m_dboStats, NULL);
		LobBlockOtherPage *lobOtherPage =(LobBlockOtherPage *)bloOtherPageHdl->getPage();
		if (lobOtherPage->m_bph.m_lsn < lsn) {
			memset(lobOtherPage, 0, Limits::PAGE_SIZE);
			lobOtherPage->m_bph.m_lsn = lsn;
			lobOtherPage->m_isFirstPage = 0;
			if(i == pageNum - 1) {// �����һҳ
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
 * ���ϻָ�ʱREDO���ʹ����ɾ������
 *
 * @param session �Ự����
 * @param lsn ��־LSN
 * @param log ��¼���������־����
 * @param size ��־��С
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

	// ���ж�Ŀ¼�ļ�ҳ�Ƿ�Ҫredo
	if (page->m_bph.m_lsn < lsn) { // ��Ҫredo
		assert(!m_lobi->getSlot(page, slotNum)->m_free); // ��¼��һ���ǿ�
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

	if (flags) { // ��ҳ���ı�
		headerPageHdl = m_lobi->lockHeaderPage(session, Exclusived);
		headerPage = (LIFileHeaderPageInfo *)headerPageHdl->getPage();
		if (headerPage->m_bph.m_lsn < lsn) { // ��Ҫredo
			assert(headerPage->m_firstFreePageNum != pageNum);
			headerPage->m_firstFreePageNum = pageNum;
			headerPage->m_bph.m_lsn = lsn;
			session->markDirty(headerPageHdl);
		}
		m_lobi->unlockHeaderPage(session, headerPageHdl);
	}

	// ���жϴ�����ļ��Ƿ�Ҫredo
	lobFirstHeaderPageHdl = GET_PAGE(session, m_file, PAGE_LOB_HEAP, pid, Exclusived, m_dboStats, NULL);
    LobBlockFirstPage *lobFirstHeaderPage = (LobBlockFirstPage *)lobFirstHeaderPageHdl->getPage();
	if (lobFirstHeaderPage->m_bph.m_lsn < lsn) {// ��Ҫredo
		lobFirstHeaderPage->m_isFree = true;
		lobFirstHeaderPage->m_bph.m_lsn = lsn;
		session->markDirty(lobFirstHeaderPageHdl);
	}
	session->releasePage(&lobFirstHeaderPageHdl);
}


/**
 * �жϴ������Ƿ񳬹��ļ����ȣ�����ʱ����չ�ļ�
 * @param session  �Ự
 * @param tableDef ����
 * @param pageId  ��������ʼҳ��
 * @param pageNum �����ռ��ҳ��
 * @param fileLen ������ļ��ĳ���
 * @param headerPage ����ҳ����ҳ��
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
 * ���ϻָ�ʱREDO���ʹ������²���
 *
 * @param session �Ự����
 * @param tableDef ����
 * @param lid �����ID
 * @param lsn ��־LSN
 * @param log ��¼���²�����־����
 * @param logSize ��־��С
 * @param lob ���º��������ݣ���Ԥ������־�У�
 * @param lobSize ���º����󳤶�
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

	// ��������������˵��û�б��ƶ����ļ�ĩβ���ʷ��������������
	// 1��������С
	// 2������������ļ�ĩβ���һ��
	// 3��������Сռ�е�ҳ��û��
	// 4���������㹻�������Ŀ��пռ�
	if (!isNewPageID) {
		if (lobOldPid + pageNum > fileLen) isHeaderPageModified = true;
		extendLobFile(session, tableDef, lobOldPid, pageNum, fileLen, headerPage);

		if (!isNewFreeBlock) {// ����2��3�Լ�4�Ĳ������
			// �������2��3��ʱ��Ҫ�ж���fileTail
			u32 fileTail = headerPage->m_blobFileTail;
			if (fileTail < lobOldPid + pageNum) {
				isHeaderPageModified = true;
				headerPage->m_blobFileTail = lobOldPid + pageNum;
			}
		} else {// ����1��4�Ĳ������
			// �޸��²����Ŀ��п����ҳ����
			BufferPageHandle *newFreeBlockHeadrPageHdl = GET_PAGE(session, m_file, PAGE_LOB_HEAP, newFreeBlockPid, Exclusived, m_dboStats, NULL);
			LobBlockFirstPage *newFreeBlockHeadrPage = (LobBlockFirstPage *)newFreeBlockHeadrPageHdl->getPage();
			// ��Ҫredo���ҳ
			if (newFreeBlockHeadrPage->m_bph.m_lsn < lsn) {
				createNewFreeBlock(session, newFreeBlockHeadrPageHdl, newFreeBlockHeadrPage, lsn, newFreeBlockLen);
			} else {
				session->releasePage(&newFreeBlockHeadrPageHdl);
			}
		}
		// ���´��������
		lobFirstHeaderPageHdl = GET_PAGE(session, m_file, PAGE_LOB_HEAP, lobOldPid, Exclusived, m_dboStats, NULL);
		lobFirstHeaderPage = (LobBlockFirstPage *)lobFirstHeaderPageHdl->getPage();
		// ��Ҫredo���ҳ
		if (lobFirstHeaderPage->m_bph.m_lsn < lsn) {
			lobFirstHeaderPage->m_bph.m_lsn = lsn;
			lobFirstHeaderPage->m_len = blockLen;
			lobFirstHeaderPage->m_lid = lid;
			lobFirstHeaderPage->m_srcLen = org_len;

			// ������������
			if (pageNum == 1) {// ֻ��һ��ҳ
				// copy��ҳ����
				memcpy(((byte *)lobFirstHeaderPage) + offset, lob, lobSize);
			} else {
				memcpy(((byte *)lobFirstHeaderPage) + offset, lob, copyLen);
			}
			// �����insert��һ��������ֱ���ͷţ���Ϊredoʱ���ǵ��̵߳�
			session->markDirty(lobFirstHeaderPageHdl);
			session->releasePage(&lobFirstHeaderPageHdl);
		} else {
			session->releasePage(&lobFirstHeaderPageHdl);
		}
		// ���������ҳ������
		if (pageNum > 1) {
			redoWriteOtherPages(session, lsn, lob, copyLen, pageNum, lobOldPid, lobSize);
		}
	} else {// ������ƶ����ļ�ĩβ
		BufferPageHandle *pageHdl;
		LIFilePageInfo *page;
		assert(isNewFreeBlock);// һ�������µĿ��п�
		if (lobNewPid + pageNum > fileLen) isHeaderPageModified = true;
		extendLobFile(session, tableDef, lobNewPid, pageNum, fileLen, headerPage);

		u32 fileTail = headerPage->m_blobFileTail;
		if (fileTail < lobNewPid + pageNum) {
			isHeaderPageModified = true;
			headerPage->m_blobFileTail = lobNewPid + pageNum;
		}
		lobFirstHeaderPageHdl = GET_PAGE(session, m_file, PAGE_LOB_HEAP, lobNewPid, Exclusived, m_dboStats, NULL);
		lobFirstHeaderPage = (LobBlockFirstPage *)lobFirstHeaderPageHdl->getPage();
		// ��������ҳ�϶�����һҳ
		if (lobFirstHeaderPage->m_bph.m_lsn < lsn) {// ��Ҫredo���ҳ
			lobFirstHeaderPage->m_bph.m_lsn = lsn;
			lobFirstHeaderPage->m_len = blockLen;
			lobFirstHeaderPage->m_isFirstPage = true;
			lobFirstHeaderPage->m_isFree = false;
			lobFirstHeaderPage->m_lid = lid;
			lobFirstHeaderPage->m_srcLen = org_len;
			// ������������
			memcpy(((byte *)lobFirstHeaderPage) + offset, lob, copyLen);
			// �����insert��һ��������ֱ���ͷţ���Ϊredoʱ���ǵ��̵߳�
			session->markDirty(lobFirstHeaderPageHdl);
		}
		session->releasePage(&lobFirstHeaderPageHdl);
		// ��������ҳ
		redoWriteOtherPages(session, lsn, lob, copyLen, pageNum, lobNewPid, lobSize);

		// �޸Ŀ��п���ҳ����
		BufferPageHandle *newFreeBlockHeadrPageHdl = GET_PAGE(session, m_file, PAGE_LOB_HEAP, lobOldPid, Exclusived, m_dboStats, NULL);
		LobBlockFirstPage *newFreeBlockHeadrPage = (LobBlockFirstPage *)newFreeBlockHeadrPageHdl->getPage();
		if (newFreeBlockHeadrPage->m_bph.m_lsn < lsn) {// ��Ҫredo���ҳ
			createNewFreeBlock(session, newFreeBlockHeadrPageHdl, newFreeBlockHeadrPage, lsn, newFreeBlockLen);
		} else {
			session->releasePage(&newFreeBlockHeadrPageHdl);
		}
		// �޸Ķ�Ӧ��Ŀ¼ҳ������Ŀ¼ҳ�϶��б仯
		pageHdl = GET_PAGE(session, m_lobi->m_file, PAGE_LOB_INDEX, indexPid, Exclusived, m_dboStats, NULL);
		page = (LIFilePageInfo *)pageHdl->getPage();
		if (page->m_bph.m_lsn < lsn) {// ��Ҫredo
			page->m_bph.m_lsn = lsn;
			LiFileSlotInfo *slotInfo = m_lobi->getSlot(page, slotNum);
			slotInfo->u.m_pageId = lobNewPid;
			slotInfo->m_free = false;
			session->markDirty(pageHdl);
		}
		session->releasePage(&pageHdl);
	}
	// �ͷ���ҳ
	if (isHeaderPageModified) session->markDirty(headerPageHdl);
	m_lobi->unlockHeaderPage(session, headerPageHdl);
}

/** �޸ı��ID
 * @param session �Ự
 * @param tableId �µı�ID
 */
void BigLobStorage::setTableId(Session *session, u16 tableId) {
	m_lobi->setTableId(session, tableId);
}

/**
 * ������Ƭ����
 *
 * @param session �Ự
 * @param tableDef ����
 */
void BigLobStorage::defrag(Session *session, const TableDef *tableDef) {
	bool flag;
	u32 notFreePid, freePid, notFreeBlockLen, nextBlockPid, nextPid;
	BufferPageHandle *freeBlockPageHdl, *notFreeBlockPageHdl, *nextFreePageHdl;
	BufferPageHandle *nextBlockPageHdl = NULL;
	MemoryContext *mc = session->getMemoryContext();

	// �ȶ�ȡ���ļ��ĳ��ȣ�Ϊ�Ժ��ж��Ƿ����ļ�ĩβ������
	u32 fileTail = getBlobFileTail(session);

	// �õ���һ�����п�ͷǿ��п�
	freeBlockPageHdl = getFirstFreeBlockAndNextNotFreeBlock(session, &notFreeBlockPageHdl, &freePid, &notFreePid, &notFreeBlockLen, fileTail);
	if (freeBlockPageHdl == NULL)
		return;

	flag = true;
	u32 pageNum = getPageNum(notFreeBlockLen);
	nextPid = notFreePid;

	// ���濪ʼ��������
	while (nextPid + pageNum < fileTail) {
		// ���������ҵ��Ŀ��Ƿǿ��п�
		if (flag) {
			// �ƶ�ǰ׼�����������ж��Ƿ�����Ҹ��ǣ��������Ҹ��ǣ��ͷŵ��ļ�ĩβ
			// �ƶ�Ŀ�ĵؿ��в�������Ҫ�ƶ���ĩβ
			if (nextPid - freePid < pageNum) {
				 putLobToTail(session, tableDef, nextPid, notFreeBlockPageHdl, mc);
			} else {// �ƶ������
				 moveLob(session, freePid, nextPid, freeBlockPageHdl, notFreeBlockPageHdl, mc);
				// �õ��¸��飬���¿�����һ�����п��ַ
				nextFreePageHdl = NEW_PAGE(session, m_file, PAGE_LOB_HEAP, freePid + pageNum, Exclusived, m_dboStats);
				// Ȼ���ͷŸղ��¿����ҳ
				session->markDirty(freeBlockPageHdl);
				session->releasePage(&freeBlockPageHdl);
				freePid = freePid + pageNum;
				freeBlockPageHdl = nextFreePageHdl;
			}
			// ��ȡ�¸���
			nextBlockPid = nextPid + pageNum;
			nextBlockPageHdl = GET_PAGE(session, m_file, PAGE_LOB_HEAP, nextBlockPid, Exclusived, m_dboStats, NULL);
			LobBlockFirstPage *lobfirstPage = (LobBlockFirstPage *)nextBlockPageHdl->getPage();
			notFreeBlockLen = lobfirstPage->m_len;
			pageNum = getPageNum(notFreeBlockLen);
			// ��������¸����Ƿǿ��п�
			if (!lobfirstPage->m_isFree) {
				notFreeBlockLen = lobfirstPage->m_len;
				notFreeBlockPageHdl = nextBlockPageHdl;
				flag = true;
			} else {
				// �ͷż����ǿ��п����ҳ
				session->releasePage(&nextBlockPageHdl);
				flag = false;
			}
			nextPid = nextBlockPid;
		} else {
			// ��ȡ�¸��顣
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
		// ���絽��ǰһ�ζ�ȡ���ļ�ĩβ�����ٶ�ȡһ��
		if (nextPid + pageNum >= fileTail) {
			fileTail = getBlobFileTail(session);
			// �����ж��Ƿ����ļ�ĩβ�����д���
			if (nextPid + pageNum == fileTail) {// ���������һ����
				// �����ж��Ƿ����ļ�ĩβ
				// �ȶ�Ŀ¼�ļ���ҳ������
				BufferPageHandle *liFileHeaderPageHdl = m_lobi->lockHeaderPage(session, Exclusived);
				LIFileHeaderPageInfo *liFileHeaderPage=(LIFileHeaderPageInfo *)liFileHeaderPageHdl->getPage();
				u32 newFileTail = liFileHeaderPage->m_blobFileTail;
				if (nextPid + pageNum == newFileTail) {// ˵��û���µĴ�������
					if (flag) {// ���һ���Ƿǿ��п�
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
 * �Ȳ��ҵ�һ�����п�Ϳ��п��ĵ�һ���ǿ��п�
 *
 * @post �õ���һ�����п����ҳд��
 * @post �õ���һ���ǿ��п����ҳ����
 *
 * @param session �Ự����
 * @param firstNotFreeBlock out �ڿ��п����ĵ�һ���ǿ��п����ʼҳ�ľ��
 * @param firstFreePid out ��һ�����п����ʼҳ��
 * @param firstNotFreePid out ��һ���ǿ��п����ʼҳ��
 * @param notFreeBlockLen out ��һ���ǿ��п�ĳ���
 * @param fileTail ��ǰ�Ĵ�����ļ�β��
 * @return ��һ�����п����ʼҳ�ľ��
 */
BufferPageHandle* BigLobStorage::getFirstFreeBlockAndNextNotFreeBlock(Session *session, BufferPageHandle **firstNotFreeBlock, u32 *firstFreePid, u32 *firstNotFreePid, u32 *notFreeBlockLen, u32 fileTail) {
	BufferPageHandle *freeBlockFirstPageHdl;
	u32 nextPid = 0;
	u32 blockLen, pageNum;
	BufferPageHandle *pageHdl = GET_PAGE(session, m_file, PAGE_LOB_HEAP, 0, Exclusived, m_dboStats, NULL);
	LobBlockFirstPage *lobFirstPage = (LobBlockFirstPage *)pageHdl->getPage();
	// ���ҵ���һ�����п�
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
	// Ѱ�ҿ��п����ĵ�һ���ǿ��п�
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
 * �õ�������ļ��Ѿ�д�볤�ȣ���fileTail
 *
 * @param session �Ự����
 * @return fileTail
 */
u32 BigLobStorage::getBlobFileTail(Session *session) {
	BufferPageHandle *headerPageHdl = m_lobi->lockHeaderPage(session, Shared);
	u32 fileTail = ((LIFileHeaderPageInfo *)headerPageHdl->getPage())->m_blobFileTail;
	m_lobi->unlockHeaderPage(session, headerPageHdl);
	return fileTail;
}

/**
 * �Ѵ�����ƶ����ļ�ĩβ
 *
 * @post �õ��¸������ҳ����
 * @post �ͷ�ԭ�������ҳ��
 *
 * @param session �Ự����
 * @param tableDef ����
 * @param pid ��Ƭ��������У���Ҫ�ƶ��ķǿ��п����ʼҳ��
 * @param notFreeBlockFirstPageHdl  ��Ҫ�ƶ��ķǿ��п����ʼҳ���
 * @param mc �ڴ�������
 */
void BigLobStorage::putLobToTail(Session *session, const TableDef *tableDef, u32 pid, BufferPageHandle *notFreeBlockFirstPageHdl, MemoryContext *mc) {
	u32 indexPid, newPid;
	u16 slotNum;
	u64 lsn = 0;
	// ׼��LOB���ݣ�Ϊд��־
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

	// ��ΪҪ�޸Ķ�Ӧ����ʼPageID,����Ҫȥ�޸Ķ�ӦĿ¼ҳ
	indexPid = LID_GET_PAGE(lid);
	BufferPageHandle *liFilePageHdl = GET_PAGE(session, m_lobi->m_file, PAGE_LOB_INDEX, indexPid, Exclusived, m_dboStats, NULL);
	LIFilePageInfo *liFilePage = (LIFilePageInfo *)liFilePageHdl->getPage();

	// д��־
	lsn = logMove(session, lid, pid, newPid, org_len, lobSize, data, m_lobi->getTableID(), mc);

	liFilePage->m_bph.m_lsn = lsn;
	slotNum = LID_GET_SLOT(lid);
	LiFileSlotInfo *slotInfo=m_lobi->getSlot(liFilePage,slotNum);
	slotInfo->u.m_pageId = newPid;
	// �ͷ�Ŀ¼ҳ
	session->markDirty(liFilePageHdl);
	session->releasePage(&liFilePageHdl);

	// ��ʼд���ݣ������������Ҫ����ҳcopy,Ȼ���޸�lsn
	LobBlockFirstPage *newDataBlockHeaderPage = (LobBlockFirstPage *)newBlockFirstPageHdl->getPage();
	memcpy(newDataBlockHeaderPage, lobFirstPage, Limits::PAGE_SIZE);
	newDataBlockHeaderPage->m_bph.m_lsn = lsn;
	// �������һҳ����д����ҳ����
	if (pageNum >1) {
		writeLob(session, newPid, pageNum, offset, lsn, data, lobSize);
	}
	// ����MemoryContext
	mc->resetToSavepoint(savePoint);
	// �ͷ��¿���ҳ
	session->markDirty(newBlockFirstPageHdl);
	session->releasePage(&newBlockFirstPageHdl);
	// ����ͷžɿ���ҳ
	lobFirstPage->m_bph.m_lsn = lsn;
	lobFirstPage->m_isFree = true;
	session->markDirty(notFreeBlockFirstPageHdl);
	session->releasePage(&notFreeBlockFirstPageHdl);
}


/**
 * �Ѵ������ǰ�ƶ�������Ƭ����
 * @post �ƶ����¿����ҳ����������
 * @post �õ��¸������ҳ����
 * @post �ͷ�ԭ�������ҳ��
 * @param session �Ự����
 * @param freePid �ƶ�Ŀ�ĵص���ʼҳ��
 * @param notFreePid ��Ƭ��������У���Ҫ�ƶ��ķǿ��п����ʼҳ��
 * @param freeBlockPageHdl  �ƶ�Ŀ�ĵأ����п飩����ʼҳ���
 * @param notFreeBlockFirstPageHdl  ��Ҫ�ƶ��ķǿ��п����ʼҳ���
 * @param mc �ڴ�������
 */
void BigLobStorage::moveLob(Session *session, u32 freePid, u32 notFreePid, BufferPageHandle *freeBlockPageHdl, BufferPageHandle *notFreeBlockFirstPageHdl, MemoryContext *mc) {
	u32 org_len = 0;
	u32 indexPid, pageNum;
	u16 slotNum;
	u64 lsn = 0;
	u32 offset = 0;

	// �õ���������ݣ�Ϊд��־׼��
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

	// ��ΪҪ�޸Ķ�Ӧ����ʼPageID,����Ҫȥ�޸Ķ�ӦĿ¼ҳ
	indexPid = LID_GET_PAGE(lid);
	BufferPageHandle *liFilePageHdl = GET_PAGE(session, m_lobi->m_file, PAGE_LOB_INDEX, indexPid, Exclusived, m_dboStats, NULL);
	LIFilePageInfo *liFilePage = (LIFilePageInfo *)liFilePageHdl->getPage();

	// д��־
	lsn = logMove(session, lid, notFreePid, freePid, org_len, lobSize, data, m_lobi->getTableID(), mc);
	liFilePage->m_bph.m_lsn = lsn;
	slotNum = LID_GET_SLOT(lid);
	LiFileSlotInfo *slotInfo=m_lobi->getSlot(liFilePage,slotNum);
	slotInfo->u.m_pageId = freePid;
	// �ͷ�Ŀ¼ҳ
	session->markDirty(liFilePageHdl);
	session->releasePage(&liFilePageHdl);

	// ��ʼд���ݣ������������Ҫ����ҳcopy,Ȼ���޸�lsn;
	memcpy(freeFirstPage, lobFirstPage, Limits::PAGE_SIZE);
	freeFirstPage->m_bph.m_lsn = lsn;
	// �������һҳ����copy����ҳ����
	if (pageNum >1) {
		writeLob(session, freePid, pageNum, offset, lsn, data, lobSize);
	}
	mc->resetToSavepoint(savePoint);
	// ����ͷžɿ���ҳ
	lobFirstPage->m_bph.m_lsn = lsn;
	lobFirstPage->m_isFree = true;
	session->markDirty(notFreeBlockFirstPageHdl);
	session->releasePage(&notFreeBlockFirstPageHdl);
}

/**
 * ��Ƭ��������У��ƶ�������ʱ�����־
 *
 * @param session �Ự����
 * @param lid �����ID
 * @param oldPid �ƶ�ǰ����ʼҳ��
 * @param newPid �ƶ�ǰ�����ʼҳ��
 * @param org_len δѹ���ĳ���
 * @param lobLen ����󳤶�
 * @param data ��������
 * @param tid ĸ��ID
 * @param mc �ڴ�������
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
		// flags, ��ʼҳ���Ƿ񱻸ı�
		s.write(newPid);
		s.write(org_len);
		s.write(lobLen);
		s.write(data, lobLen);
	} catch (NtseException &) {
		assert(false);	// ���ﲻ���ܳ����쳣
	}
	return session->writeLog(LOG_LOB_MOVE, tid, logData, s.getSize());
}


/**
 * ���ݿ鳤�ȵȵ���ռ��ҳ��
 *
 * @param lobLen �鳤��
 * @return ռ��ҳ��
 */
uint BigLobStorage::getPageNum(uint lobLen) {
	return (lobLen + Limits::PAGE_SIZE - 1) / Limits::PAGE_SIZE;
}

/**
 * ���ϻָ�ʱREDO���ʹ����������
 *
 * @param session �Ự����
 * @param tableDef ����
 * @param lsn ��־LSN
 * @param log ��¼���������־����
 * @param logSize ��־��С
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

	if (lobNewPid > lobOldPid) {// ˵��������ƶ����ļ�ĩβ
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
	if (lobFirstHeaderPage->m_bph.m_lsn < lsn) {// ��Ҫredo���ҳ
		lobFirstHeaderPage->m_bph.m_lsn = lsn;
		lobFirstHeaderPage->m_len = lobBlockSize;
		lobFirstHeaderPage->m_isFirstPage = true;
		lobFirstHeaderPage->m_isFree = false;
		lobFirstHeaderPage->m_lid = lobid;
		lobFirstHeaderPage->m_srcLen = org_len;
		if (pageNum > 1)
		// ������������
			memcpy((byte *)lobFirstHeaderPage + offset, log + s.getSize(), haveCopyLen);
		else
			memcpy((byte *)lobFirstHeaderPage + offset, log + s.getSize(), lobLen);
		// �����insert��һ��������ֱ���ͷţ���Ϊredoʱ���ǵ��̵߳�
		session->markDirty(lobFirstHeaderPageHdl);
	}
	session->releasePage(&lobFirstHeaderPageHdl);

	// ��������ҳ
	const byte *lobContent = log + s.getSize();
	if (pageNum > 1){
		redoWriteOtherPages(session, lsn, lobContent, haveCopyLen, pageNum, lobNewPid, lobLen);
	}
	// �޸Ŀ��п���ҳ����
	BufferPageHandle *newFreeBlockHeadrPageHdl = GET_PAGE(session, m_file, PAGE_LOB_HEAP, lobOldPid, Exclusived, m_dboStats, NULL);
	LobBlockFirstPage *newFreeBlockHeadrPage = (LobBlockFirstPage *)newFreeBlockHeadrPageHdl->getPage();
	if (newFreeBlockHeadrPage->m_bph.m_lsn < lsn) {// ��Ҫredo���ҳ
		createNewFreeBlock(session, newFreeBlockHeadrPageHdl, newFreeBlockHeadrPage, lsn, pageNum);
	} else {
		session->releasePage(&newFreeBlockHeadrPageHdl);
	}
	// �޸Ķ�Ӧ��Ŀ¼ҳ������Ŀ¼ҳ�϶��б仯
	BufferPageHandle *pageHdl = GET_PAGE(session, m_lobi->m_file, PAGE_LOB_INDEX, indexPid, Exclusived, m_dboStats, NULL);
	LIFilePageInfo *page = (LIFilePageInfo *)pageHdl->getPage();
	if (page->m_bph.m_lsn < lsn) {//��Ҫredo
		page->m_bph.m_lsn = lsn;
		LiFileSlotInfo *slotInfo = m_lobi->getSlot(page, slotNum);
		slotInfo->u.m_pageId = lobNewPid;
		assert(!slotInfo->m_free);
		session->markDirty(pageHdl);
	}
	session->releasePage(&pageHdl);
}

/**
 * �õ�Ŀ¼�ļ��ʹ�����ļ����
 *
 * @param files in/out ��ģ�������Fileָ�����飬 �ռ�����߷���
 * @param pageTypes in/out File��Ӧ��ҳ����
 * @param numFile files�����pageTypes���鳤��
 * @return File�������
 */
int BigLobStorage::getFiles(File** files, PageType* pageTypes, int numFile) {
    UNREFERENCED_PARAMETER(numFile);
	assert(numFile >= 2);
	// �����ȷ��������ļ�
	// ���򱸷�ʱ, ͷҳ�е��ļ����Ȳ���
	files[0] = m_lobi->getFile();
	pageTypes[0] = PAGE_LOB_INDEX;

	files[1] = m_file;
	pageTypes[1] = PAGE_LOB_HEAP;
	return 2;
}

/**
 * ˢ��������
 *
 * @param session �Ự
 */
void BigLobStorage::flush(Session *session) {
	m_buffer->flushDirtyPages(session, m_lobi->m_file);
	m_buffer->flushDirtyPages(session, m_file);
}

/**
 * ��֤�����ҳ��������Ҫ��֤isFirstPage�Ƿ���ȷ
 * @param session, �Ự����
 * @param pid ����ҳID
 */
//void BigLobStorage::verifyLobPage(Session *session, u32 pid) {
//	BufferPageHandle *lobBlockHeadrPageHdl = GET_PAGE(session, m_file, PAGE_LOB_HEAP, pid, Shared, m_dboStats, NULL);
//	LobBlockFirstPage *firstPage = (LobBlockFirstPage *)lobBlockHeadrPageHdl->getPage();
//	u32 blockLen = firstPage->m_len;
//	// �����ռ�е�ҳ��
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
 * �õ�ͳ����Ϣ
 *
 * @return ͳ����Ϣ
 */
const BLobStatus& BigLobStorage::getStatus() {
	// TODO ������ȡ���ݿ��ܲ�̫��ȫ��������������ܿ��ǲ��ܼ���
	// ��Ҫ�������ڴ��м�¼������ֵ
	m_status.m_datLength = m_lobi->m_headerPage->m_blobFileTail * Limits::PAGE_SIZE;
	m_status.m_idxLength = m_lobi->m_headerPage->m_fileLen * Limits::PAGE_SIZE;
	return m_status;
}

/**
 * �õ���չͳ����Ϣ
 * 
 * @return ͳ����Ϣ
 */
const BLobStatusEx& BigLobStorage::getStatusEx() {
	return m_statusEx;
}

/**
 * ������չͳ����Ϣ
 *
 * @param session �Ự
 * @param maxSamplePages ��������ô���ҳ��
 */
void BigLobStorage::updateExtendStatus(Session *session, uint maxSamplePages) {
	McSavepoint mcSave(session->getMemoryContext());
	
	// ����һ��������ֹ����
	Mutex mutex("BigLobStorage::sample", __FILE__, __LINE__);
	MutexGuard sampleGuard(&mutex, __FILE__, __LINE__);
	// ��ͳ��Ŀ¼�ļ�
	u32 maxUsedPage = m_lobi->getMaxUsedPage(session);
	u32 minPage = (u32)m_lobi->metaDataPgCnt();
	if (maxUsedPage <= minPage - 1) {
		m_statusEx.m_freePages = m_statusEx.m_numLobs = 0;
		m_statusEx.m_pctUsed = .0;
		return;
	}

	SampleResult *result;
	if (maxSamplePages > 2048)
		// �����ϴ�ʱ�����ƶ�Ҫ����Ը�һЩ
		result = SampleAnalyse::sampleAnalyse(session, m_lobi, maxSamplePages, 30, true, 0.382, 16); 
	else
		// ������Сʱ�����ƶ�Ҫ�����һЩ
		result = SampleAnalyse::sampleAnalyse(session, m_lobi, maxSamplePages, 50, true, 0.618, 8);  
	m_statusEx.m_numLobs = (u64)(result->m_fieldCalc[0].m_average * (maxUsedPage - minPage + 1));
	// ��������Ƚ��٣�����������0��
	if (m_statusEx.m_numLobs == 0) {
		m_statusEx.m_freePages = m_statusEx.m_numLobs = 0;
		m_statusEx.m_pctUsed = .0;
		return;
	}
	
	// �õ�������ƽ������
	u32 fileTail = getBlobFileTail(session);
	m_lobAverageLen = (u32) (fileTail * Limits::PAGE_SIZE / m_statusEx.m_numLobs);
	
	delete result;

	SampleResult *blobResult;

	// �ٲ���������ļ�
	if (maxSamplePages > 2048)
		// �����ϴ�ʱ�����ƶ�Ҫ����Ը�һЩ
		blobResult = SampleAnalyse::sampleAnalyse(session, this, maxSamplePages, 30, true, 0.382, 16); 
	else
		// ������Сʱ�����ƶ�Ҫ�����һЩ
		blobResult = SampleAnalyse::sampleAnalyse(session, this, maxSamplePages, 50, true, 0.618, 8);  
	m_statusEx.m_freePages = (u64)(blobResult->m_fieldCalc[0].m_average * m_sampleRatio);
	m_statusEx.m_pctUsed  = blobResult->m_fieldCalc[1].m_average /
		(blobResult->m_fieldCalc[2].m_average * Limits::PAGE_SIZE);			
	delete blobResult;
}


/** 
 * ��ʼ����׼������ 
 *
 * @param session �Ự
 * @param maxSampleNum ����������
 * @param fastSample ���ٲ���
 * @return �������
 */
SampleHandle* BigLobStorage::beginSample(Session *session, uint maxSampleNum, bool fastSample) {
	BigLobSampleHandle *handle = new BigLobSampleHandle(session, maxSampleNum, fastSample);
	if(fastSample) {
		// ��Ҫ���ڴ��в���,�Ȳ���Ŀ¼�ļ�
		handle->m_bufScanHdl = m_buffer->beginScan(session->getId(), m_file);
	} else {
		// Ȼ���ڴ����ϲ���
		// �õ������ķ�Χ
		handle->m_maxPage = getBlobFileTail(session);
		handle->m_minPage = metaDataPgCnt();

		// ���ļ����峤�ȣ�Ȼ����������ķ�����С�ͷ����ĸ���
		if (handle->m_maxSample > (handle->m_maxPage - handle->m_minPage + 1))
			handle->m_maxSample = (uint)(handle->m_maxPage - handle->m_minPage + 1);

		if (maxSampleNum > handle->m_maxSample) 
			maxSampleNum = handle->m_maxSample;
		/** ��Ϊ16���������� */
		handle->m_blockNum = 16 > maxSampleNum ? maxSampleNum : 16; 
		handle->m_regionSize = (handle->m_maxPage + 1 - handle->m_minPage) / handle->m_blockNum;

		// ���ҳ��̫�٣������Է�������ȫ������
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
 * ������һ������
 *
 * @param handle �������
 * @return һ������
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
		// ���һ��ȫȡ
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
		// ��¼���ϴβ����Ŀ��пռ�
		shdl->m_lastFreePages = (*sample)[0];
		shdl->m_curBlock++;
		return sample;
	}
	return NULL;
}


/** 
 * �����������ͷ���Դ 
 *
 * @param handle �������
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
 * ����һ�������ҳ��, �õ�һ������
 *
 * @param page ҳ����
 * @param handle �������
 * @param pid ҳ��
 * @return һ������
 */
Sample* BigLobStorage::sampleRegionPage(BufferPageHandle *page, SampleHandle *handle, u32 pid){
	int freePage = 0;
	int usedLen = 0;
	int usedPage = 0;
	u32 pageNum = 0;
	BufferPageHandle *firstPageHdl = page;
	BigLobSampleHandle *shdl = (BigLobSampleHandle *)handle;
	LobBlockFirstPage *liFirstPage;

	// page�϶�����ҳ
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
 * ����һ��buffer��ҳ��, �õ�һ������
 *
 * @param page ҳ����
 * @param handle �������
 * @return һ������
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
 * ���ݴ����Ĺ��Ƶ�ƽ�����ȣ��õ������������С 
 *
 * @param averageLen ������ƽ������
 * @return ���������鳤��
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
 * Ѱ�Ҳ��ɲ�����ҳ��,��������ҵ�block�ڵ�һ����������ҳ 
 * @param session �Ự����
 * @param minPage  ��������ʼҳ��
 * @param regionSize ���ĳ���
 * @param startPid out,������������ʼҳ��
 * @return ҳ���
 */
BufferPageHandle* BigLobStorage::unsamplablePagesBetween(Session *session, u64 minPage, u64 regionSize, u32 *startPid) {
	u32 pid = (u32)minPage;
	BufferPageHandle *pageHdl = GET_PAGE(session, m_file, PAGE_LOB_HEAP, pid, Shared, m_dboStats, NULL);
	LobBlockOtherPage *otherPage = (LobBlockOtherPage *)pageHdl->getPage();
	while (!otherPage->m_isFirstPage ) {
		session->releasePage(&pageHdl);
		// �����������
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

