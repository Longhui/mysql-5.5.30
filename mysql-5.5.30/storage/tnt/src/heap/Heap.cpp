/**
 * Heap.cpp
 *
 * @author 谢可(ken@163.org)
 */

#include "util/File.h"
#include "misc/Buffer.h"
#include "api/Database.h"
#include "misc/Session.h"
#include "misc/Syslog.h"
#include "util/Stream.h"
#include "heap/Heap.h"
#include "heap/FixedLengthRecordHeap.h"
#include "heap/VariableLengthRecordHeap.h"
#include "misc/TableDef.h"
#include "misc/Trace.h"
#include "compress/RowCompress.h"
#include <cassert>

#ifdef TNT_ENGINE
#include "trx/TNTTransaction.h"
#endif

using namespace ntse;

namespace ntse {


/**
 * 堆构造函数
 *
 * @param db 数据库对象
 * @param heapFile 堆文件
 * @param headerPage 堆首页
 * @param dboStats 数据对象状态
 */
DrsHeap::DrsHeap(Database *db, const TableDef *tableDef, File *heapFile, BufferPageHdr *headerPage, DBObjStats *dboStats) {
	assert(db);
	assert(heapFile);
	assert(headerPage);
	ftrace(ts.hp, tout << (HeapHeaderPageInfo *)headerPage << heapFile;);
	m_db = db;
	m_tableDef = tableDef;
	m_buffer = db->getPageBuffer();
	m_heapFile = heapFile;
	m_headerPage = headerPage;
	m_maxPageNum = ((HeapHeaderPageInfo *)headerPage)->m_pageNum;
	m_maxUsedPageNum = ((HeapHeaderPageInfo *)headerPage)->m_maxUsed;
	m_dboStats = dboStats;
	memset(&m_status, 0, sizeof(HeapStatus));
	m_status.m_dboStats = dboStats;
	m_cprsRcdExtrator = NULL;
}

DrsHeap::~DrsHeap() {
	assert(!m_heapFile);
}

/**
 * 根据表定义获得堆版本
 *
 * @param tableDef
 * @return HeapVersion枚举类型
 */
HeapVersion DrsHeap::getVersionFromTableDef(const TableDef *tableDef) {
	assert(tableDef);
	switch (tableDef->m_recFormat) {
	case REC_FIXLEN:
		return HEAP_VERSION_FLR;
	case REC_VARLEN:
	case REC_COMPRESSED:
		return HEAP_VERSION_VLR;
	default:
		return HEAP_VERSION_VLR;
	}
}

/**
 * 创建一个堆并初始化
 *
 * @param db 数据库
 * @param path 堆文件完整路径
 * @param tableDef 表定义
 * @throw 文件无法创建，IO错误等等
 */
void DrsHeap::create(Database *db, const char *path, const TableDef *tableDef) throw(NtseException) {
	ftrace(ts.hp, tout << path;);
	u64 errCode;
	/* 创建堆文件 */
	File *heapFile = new File(path);
	errCode = heapFile->create(false, false);
	if (File::E_NO_ERROR != File::getNtseError(errCode)) {
		delete heapFile;
		NTSE_THROW(errCode, "Cannot create heap file for table %s", tableDef->m_name);
	}

	BufferPageHdr *headerPage = (BufferPageHdr*)System::virtualAlloc(Limits::PAGE_SIZE);
	NTSE_ASSERT(headerPage);

	memset(headerPage, 0, Limits::PAGE_SIZE);

	HeapVersion heapVer = DrsHeap::getVersionFromTableDef(tableDef);
	BufferPageHdr *additionalPages = NULL;
	uint additionalPageNum = 0;

	try {
		switch (heapVer) {
			case HEAP_VERSION_FLR:
				FixedLengthRecordHeap::initHeader(headerPage);
				break;
			case HEAP_VERSION_VLR:
				VariableLengthRecordHeap::initHeader(headerPage, &additionalPages, &additionalPageNum);
				break;
		}
	} catch (NtseException &e) {
		System::virtualFree(headerPage);
		throw e;
	}

	errCode = heapFile->setSize(Limits::PAGE_SIZE * (1 + additionalPageNum));
	if (File::E_NO_ERROR != File::getNtseError(errCode))
		db->getSyslog()->fopPanic(errCode, "Setting table '%s' file size failed, disk full?", tableDef->m_name);

	errCode = heapFile->write(0, Limits::PAGE_SIZE, headerPage);
	if (File::E_NO_ERROR != File::getNtseError(errCode))
		db->getSyslog()->fopPanic(errCode, "Cannot write header page for table %s", tableDef->m_name);

	if (additionalPageNum > 0) {
		errCode = heapFile->write(Limits::PAGE_SIZE, Limits::PAGE_SIZE * additionalPageNum, additionalPages);
		if (File::E_NO_ERROR != File::getNtseError(errCode))
			db->getSyslog()->fopPanic(errCode, "Cannot write aditional header pages for table %s", tableDef->m_name);
	}

	System::virtualFree(headerPage);
	if (additionalPages)
		System::virtualFree(additionalPages);
	heapFile->close();
	delete heapFile;
}

/**
 * 打开一个DRS堆文件，并分析出表定义信息
 *
 * @param db 数据库对象
 * @param session 会话
 * @param path 堆文件路径
 * @return 堆对象
 * @throw NtseException 文件找不到，文件格式不正确等等
 */
DrsHeap* DrsHeap::open(Database *db, Session *session, const char *path, const TableDef *tableDef) throw(NtseException) {
	assert(db && session && path);
	ftrace(ts.hp, tout << path;);
	u64 errCode;
	/* 打开堆文件 */
	File *heapFile = new File(path);
	assert(heapFile);
	errCode = heapFile->open(db->getConfig()->m_directIo);
	if (File::E_NO_ERROR != File::getNtseError(errCode)) {
		delete heapFile;
		NTSE_THROW(errCode, "Cannot open heap file %s", path);
	}

	DBObjStats *dbObjStats = new DBObjStats(DBO_Heap);

	BufferPageHdr *headerPage; // 取得通用首页信息，定长堆和变长堆是一样的。
	headerPage = db->getPageBuffer()->getPage(session, heapFile, PAGE_HEAP, 0, Exclusived, dbObjStats);
	assert(headerPage);
	SYNCHERE(SP_HEAP_AFTER_GET_HEADER_PAGE);

	HeapVersion heapVersion = (HeapVersion)((HeapHeaderPageInfo*)headerPage)->m_version;
	DrsHeap *heap = NULL;
	try {
		switch (heapVersion) {
		case HEAP_VERSION_FLR:
			heap = new FixedLengthRecordHeap(db, tableDef, heapFile, headerPage, dbObjStats);
			break;
		case HEAP_VERSION_VLR:
			heap = new VariableLengthRecordHeap(db, session, tableDef, heapFile, headerPage, dbObjStats);
			break;
		}
	} catch (NtseException &e) {
		db->getPageBuffer()->releasePage(session, headerPage, Exclusived);
		heapFile->close();
		delete heapFile;
		throw e;
	}

	db->getPageBuffer()->unlockPage(session->getId(), headerPage, Exclusived); 	// 只解除堆头页的锁，不释放pin，始终将堆头页pin在缓存中
	assert(heap);
	return heap;
}

/**
 * 锁住首页并且返回
 *
 * @param session 会话对象
 * @param lockMode 锁模式
 * @return 页面句柄
 */
BufferPageHandle* DrsHeap::lockHeaderPage(Session *session, LockMode lockMode) {
	assert(session);
	return LOCK_PAGE(session, m_headerPage, m_heapFile, 0, lockMode);
}

/**
 * 释放首页锁，保持pin
 *
 * @param session 会话对象
 * @param handle 首页句柄
 */
void DrsHeap::unlockHeaderPage(Session *session, BufferPageHandle **handle) {
	assert(session && handle);
	session->unlockPage(handle);
}



/**
 * 扩展堆文件，扩展策略统一服从Database类调度
 *
 * @param session			会话
 * @param headerPage		堆头页
 * @return 返回扩展了多少个页面
 */
u16 DrsHeap::extendHeapFile(Session *session, HeapHeaderPageInfo *headerPage) {
	ftrace(ts.hp || ts.recv, tout << session << headerPage; );
	assert(headerPage);
	/* 扩展文件 */
	u64 fileSize;
	u64 errCode = m_heapFile->getSize(&fileSize);
	if (File::E_NO_ERROR != File::getNtseError(errCode)) {
		m_db->getSyslog()->fopPanic(errCode, "Get heap file size of table %s failed", m_tableDef->m_name);
	}
	u16 extendSize = Database::getBestIncrSize(m_tableDef, (m_maxPageNum + 1) * Limits::PAGE_SIZE);
	errCode = m_heapFile->setSize(((m_maxPageNum + 1) + extendSize) * Limits::PAGE_SIZE);
	if (File::E_NO_ERROR != File::getNtseError(errCode)) {
		m_db->getSyslog()->fopPanic(errCode, "Extend heap file of table %s failed", m_tableDef->m_name);
	}

	initExtendedNewPages(session, extendSize);

	// 在每次自动扩展之后，唤醒Scavenger线程，Flush LRU链表尾部的部分脏页
	//m_buffer->signalScavenger();

	m_maxPageNum += extendSize;
	((HeapHeaderPageInfo *)headerPage)->m_pageNum = m_maxPageNum;
	/* 收尾工作 */
	afterExtendHeap(extendSize);

	return extendSize;
}


/**
 * 故障恢复时REDO创建表操作
 *
 * @param db 数据库对象
 * @param session 会话
 * @param path 堆文件路径
 * @param tableDef 表定义
 * @throw NtseException 文件操作出错
 */
void DrsHeap::redoCreate(Database *db, Session *session, const char *path, const TableDef *tableDef) throw(NtseException) {
	assert(db && session && path && tableDef);
	u64 errCode;
	u64 fileSize;
	UNREFERENCED_PARAMETER(session);

	File file(path);
	bool exist;
	file.isExist(&exist);
	if (!exist) {
		DrsHeap::create(db, path, tableDef);
		return;
	}
	errCode = file.open(true);
	if (File::E_NO_ERROR != File::getNtseError(errCode)) {
		NTSE_THROW(errCode, "Cannot open heap file %s", path);
	}
	file.getSize(&fileSize);
	if (fileSize <= 3 * Limits::PAGE_SIZE) { // 说明还没插入过记录，redo一下好了
		file.close();
		file.remove();
		DrsHeap::create(db, path, tableDef);
	} else {
		file.close();
	}
}

/**
 * 删除堆文件
 *
 * @param path 堆文件的完整路径，包括后缀
 * @throw NtseException 文件操作出错
 */
void DrsHeap::drop(const char *path) throw(NtseException) {
	ftrace(ts.hp, tout << path;);
	u64 errCode;
	File file(path);
	errCode = file.remove();
	if (File::E_NOT_EXIST == File::getNtseError(errCode))
		return;
	if (File::E_NO_ERROR != File::getNtseError(errCode)) {
		NTSE_THROW(errCode, "Cannot drop heap file for path %s", path);
	}
}

/** 关闭堆
 *
 * @param session  会话
 * @param flushDirty  是否刷出脏页
 */
void DrsHeap::close(Session *session, bool flushDirty) {
	assert(session);
	ftrace(ts.hp, tout << this;);
	u64 errCode;
	if (!m_heapFile)
		return;
	if (((HeapHeaderPageInfo *)m_headerPage)->m_maxUsed != m_maxUsedPageNum) {
		m_buffer->lockPage(session->getId(), m_headerPage, Exclusived, false);
		((HeapHeaderPageInfo *)m_headerPage)->m_maxUsed = m_maxUsedPageNum;
		m_buffer->markDirty(session, m_headerPage);
		m_buffer->unlockPage(session->getId(), m_headerPage, Exclusived);
	}
	m_buffer->unpinPage(m_headerPage);
	m_buffer->freePages(session, m_heapFile, flushDirty);
	errCode = m_heapFile->close();
	if (File::E_NO_ERROR != File::getNtseError(errCode)) {
		m_db->getSyslog()->fopPanic(errCode, "Closing heap file %s failed.", m_heapFile->getPath());
	}
	delete m_heapFile;
	if (m_dboStats)
		delete m_dboStats;
	m_heapFile = NULL;
}

/**
 * 刷出脏数据
 *
 * @pre 表中的写操作已经被禁用
 * @param session 会话
 */
void DrsHeap::flush(Session *session) {
	assert(session);
	if (((HeapHeaderPageInfo *)m_headerPage)->m_maxUsed != m_maxUsedPageNum) {
		m_buffer->lockPage(session->getId(), m_headerPage, Exclusived, false);
		((HeapHeaderPageInfo *)m_headerPage)->m_maxUsed = m_maxUsedPageNum;
		m_buffer->markDirty(session, m_headerPage);
		m_buffer->unlockPage(session->getId(), m_headerPage, Exclusived);
	}
	m_buffer->flushDirtyPages(session, m_heapFile);
}

/**
 * 获得堆模块的相关文件指针
 *
 * @param files IN/OUT  该模块的所有File指针数组，空间调用者分配
 * @param pageTypes File对象对应的页面类型，对于堆来说是PAGE_HEAP
 * @param numFile files数组长度
 * @return 该模块File对象个数
 */
int DrsHeap::getFiles(File **files, PageType *pageTypes, int numFile) {
	UNREFERENCED_PARAMETER(numFile);
	assert(files && pageTypes);
	assert(m_heapFile);
	int fileCount = 1;
	assert(numFile >= fileCount);
	files[0] = m_heapFile;
	pageTypes[0] = PAGE_HEAP;
	return fileCount;
}

/**
 * 故障恢复的最后，如果一个堆曾经redo过，那么调用这个函数来完成最后的工作
 *
 * @param session 会话
 */
void DrsHeap::redoFinish(Session *session) {
	ftrace(ts.recv, tout << session);
	assert(session);
	assert(m_maxPageNum >= m_maxUsedPageNum);

	nftrace(ts.recv, tout << "m_maxPageNum: " << m_maxPageNum << ", Old m_maxUsedPageNum: " << m_maxUsedPageNum);
	// 重新计算m_maxUsedPageNum
	for (u64 i = m_maxPageNum; i > 1; --i) {
		if (!isPageEmpty(session, i)) {
			m_maxUsedPageNum = i;
			break;
		}
	}
	nftrace(ts.recv, tout << "New m_maxUsedPageNum: " << m_maxUsedPageNum);
}

#ifdef NTSE_UNIT_TEST
void DrsHeap::syncMataPages(Session *session) {
	if (!m_heapFile)
		return;
	if (((HeapHeaderPageInfo *)m_headerPage)->m_maxUsed != m_maxUsedPageNum) {
		m_buffer->lockPage(session->getId(), m_headerPage, Exclusived, false);
		((HeapHeaderPageInfo *)m_headerPage)->m_maxUsed = m_maxUsedPageNum;
		m_buffer->markDirty(session, m_headerPage);
		m_buffer->unlockPage(session->getId(), m_headerPage, Exclusived);
	}
}


/**
 * 打印信息，调试用
 */
void DrsHeap::printInfo() {
	u64 size;
	m_heapFile->getSize(&size);

	printf("\n********************Heap \"%s\" Info***********************************\n", m_tableDef->m_name);
	printf("*    heap file is %s , size is "I64FORMAT"u.\n", m_heapFile->getPath(), size);
	printf("*    page size is %d , total "I64FORMAT"u page(s) .\n", Limits::PAGE_SIZE, m_maxPageNum + 1);
	printf("*    max page number is "I64FORMAT"u .\n", m_maxPageNum);
	printf("*    max record length is %d .\n", m_tableDef->m_maxRecSize);
	printOtherInfo();
	printf("***************************************************************************\n");
}

#endif

/**
 * 得到堆中已经使用的数据大小
 *
 * @return 堆已经使用的数据大小
 */
u64 DrsHeap::getUsedSize() {
	assert(m_heapFile);
	return Limits::PAGE_SIZE * m_maxUsedPageNum;
}

#ifdef TNT_ENGINE
/** 写insert的log日志
 * @param session 操作会话
 * @param txnId 插入操作所属的事务id
 * @param preLsn 同一事务前一个日志的lsn
 * @param rid 插入记录的rowid
 * return 日志的lsn
 */
LsnType DrsHeap::writeInsertTNTLog(Session *session, u16 tableId, TrxId txnId, LsnType preLsn, RowId rid) {
	LsnType lsn = 0;
	MemoryContext *ctx = session->getMemoryContext();
	McSavepoint msp(ctx);
	size_t size = sizeof(txnId) + sizeof(preLsn) + sizeof(rid);
	byte *buf = (byte *)ctx->alloc(size);
	Stream s(buf, size);
	s.write(txnId);
	s.write(preLsn);
	s.writeRid(rid);
	lsn = session->getTrans()->writeTNTLog(TNT_UNDO_I_LOG, tableId, buf, s.getSize());
	return lsn;
}

/** 解析insert的log日志
 * @param log 需要解析的log日志
 * @param rid out 需要插入记录的rowid
 * @param preLsn out 同一事务前一个日志的lsn
 * @param txnId out 插入操作所属的事务id
 */
void DrsHeap::parseInsertTNTLog(const LogEntry *log, TrxId *txnId, LsnType *preLsn, RowId *rid) {
	Stream s(log->m_data, log->m_size);
	s.read(txnId);
	s.read(preLsn);
	s.readRid(rid);
}
#endif

/**
 * 开始一次表扫描
 *
 * @param heap              堆对象
 * @param session           会话对象
 * @param extractor         子记录提取器
 * @param lockMode          对返回的记录要加的锁，可能为None
 * @param pRowLockHdl OUT   输出参数，存储行锁句柄，在lockMode为None时为NULL
 * @param returnLinkSrc    （仅对于变长堆有意义）对于链接记录，true表示在扫描到源时返回记录，false表示在扫描在目的时返回记录
 * @return 堆扫描句柄
 */
DrsHeapScanHandle::DrsHeapScanHandle(DrsHeap *heap, Session *session, SubrecExtractor *extractor, LockMode lockMode, RowLockHandle **pRowLockHdl, void *info) {
	assert(heap && session);
	ftrace(ts.hp, tout << heap << session << extractor << lockMode;);
	m_heap = heap;
	m_lockMode = lockMode;
	m_session = session;
	m_nextPos = RID(heap->metaDataPgCnt(), 0);
	m_pageHdl = NULL;
	m_pRowLockHandle = pRowLockHdl;
	assert((lockMode == None && pRowLockHdl == NULL) || (lockMode != None && pRowLockHdl != NULL));
	m_info = info;
	m_extractor = extractor;
	m_scanPagesCount = 0;
	m_prevNextPos = INVALID_ROW_ID;
	m_prevNextBmpNumForVarHeap = 0;
}

/** 析构函数 */
DrsHeapScanHandle::~DrsHeapScanHandle() {
	if (m_pageHdl) {
		m_session->unpinPage(&m_pageHdl);
	}
}

/**
 * 从insert日志中临时构建Record
 * @param log          日志项
 * @param outRec OUT   传出的记录
 */
void DrsHeap::getRecordFromInsertlog(LogEntry *log, Record *outRec) {
	assert(outRec->m_format == REC_FIXLEN || outRec->m_format == REC_VARLEN || outRec->m_format == REC_COMPRESSED);
	if (outRec->m_format == REC_FIXLEN) {
		FixedLengthRecordHeap::getRecordFromInsertlog(log, outRec);
	} else {
		VariableLengthRecordHeap::getRecordFromInsertlog(log, outRec);
	}
}

/**
 * 从insert日志中获取RowId
 * 这是parseInsertLog的一个快速简化版本
 * @param inslog         insert日志项
 * @return               insert记录的RowId
 */
RowId DrsHeap::getRowIdFromInsLog(const ntse::LogEntry *inslog) {
	assert(inslog->m_logType == LOG_HEAP_INSERT);
	u64 rid;
	Stream s(inslog->m_data, sizeof(u64) * 4); // 4 is not exact

#ifdef NTSE_VERIFY_EX
	u64 oldLSN, hdOldLSN;
	s.read(&oldLSN)->read(&hdOldLSN);
#endif
	s.read(&rid);

	return rid;
}

#ifdef NTSE_UNIT_TEST
u64 DrsHeap::getPageLSN( Session *session, u64 pageNum, DBObjStats *dbObjStats ) {
	BufferPageHandle *bphdl = GET_PAGE(session, m_heapFile, PAGE_HEAP, pageNum, Shared, dbObjStats, NULL);
	u64 lsn = bphdl->getPage()->m_lsn;
	session->releasePage(&bphdl);
	return lsn;
}
#endif



SampleHandle* DrsHeap::beginSample(Session *session, uint maxSampleNum, bool fastSample) {
	assert(maxSampleNum > 0);
	HeapSampleHandle *handle = new HeapSampleHandle(session, maxSampleNum, fastSample);
	if (fastSample) { // 缓存区采样
		handle->m_bufScanHdl = m_buffer->beginScan(session->getId(), m_heapFile);
		return handle;
	} 
	// 磁盘区采样
	handle->m_minPage = metaDataPgCnt(); /* 首页不可采样，meta data页不可采样 */
	handle->m_maxPage = m_maxUsedPageNum; /* 采样最大可到最大可用页 */
	if (handle->m_maxSample > (handle->m_maxPage - handle->m_minPage + 1))
		handle->m_maxSample = (uint)(handle->m_maxPage - handle->m_minPage + 1);
	handle->m_blockNum = 8 > maxSampleNum ? maxSampleNum : 8;   /** 分为8个区来采样 **/
	handle->m_regionSize = (handle->m_maxPage + 1 - handle->m_minPage) / handle->m_blockNum;
	if (handle->m_regionSize <= 1) { // 如果页面太少，不足以分区，则全部采样
		handle->m_blockNum = 1;
		handle->m_regionSize = handle->m_maxPage + 1 - handle->m_minPage;
	}
	handle->m_curBlock = 0;
	handle->m_blockSize = handle->m_maxSample / handle->m_blockNum;
	handle->m_curIdxInBlock = 0;
	return handle;
}

Sample* DrsHeap::sampleNext(SampleHandle *handle) {
	HeapSampleHandle *shdl = (HeapSampleHandle *)handle;
	if (handle->m_fastSample) {
		if (shdl->m_maxSample > 0) {
sampleNext_bufferPage_getNext:
			const Bcb *bcb = m_buffer->getNext(shdl->m_bufScanHdl);
			if (!bcb)
				return NULL;
			if (!isSamplable(bcb->m_pageKey.m_pageId))
				goto sampleNext_bufferPage_getNext; 
			BufferPageHdr *page = bcb->m_page;
			--shdl->m_maxSample;
			Sample *sample = sampleBufferPage(shdl->m_session, page);
			sample->m_ID = bcb->m_pageKey.m_pageId; // 需要在这里设置ID
			return sample;
		} else {
			return NULL;
		}
	} else {
		if (shdl->m_blockPages) {
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
				assert(unsamplable + shdl->m_blockSize <= regionSize);
				uint lessSample = (uint)(((int)(regionSize - unsamplable) >= shdl->m_blockSize) ?
					0 : shdl->m_blockSize - (regionSize - unsamplable));
				shdl->m_curIdxInBlock += lessSample;
				selectPage(shdl->m_blockPages + lessSample, shdl->m_blockSize - lessSample,
					shdl->m_minPage + shdl->m_regionSize * shdl->m_curBlock, regionSize);
				return sample(shdl->m_session, shdl->m_blockPages[lessSample]);
			} else {
				shdl->m_curIdxInBlock++;
				return sample(shdl->m_session, shdl->m_blockPages[shdl->m_curIdxInBlock]);
			}
		} else {
			// 第一次sampleNext
			assert(shdl->m_curBlock == 0 && shdl->m_curIdxInBlock == 0);
			uint unsamplable = unsamplablePagesBetween(shdl->m_minPage, shdl->m_regionSize);
			uint lessSample = ((int)(shdl->m_regionSize - unsamplable) >= shdl->m_blockSize) ?
				0 : (uint)(shdl->m_blockSize - (shdl->m_regionSize - unsamplable));
			shdl->m_blockPages = new u64[shdl->m_blockSize];
			shdl->m_curIdxInBlock += lessSample;
			selectPage(shdl->m_blockPages + lessSample, shdl->m_blockSize - lessSample, shdl->m_minPage, shdl->m_regionSize);
			return sample(shdl->m_session, shdl->m_blockPages[lessSample]);
		}
	}

}

void DrsHeap::endSample(SampleHandle *handle) {
	HeapSampleHandle *shdl = (HeapSampleHandle *)handle;
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

Sample* DrsHeap::sample(Session *session, u64 pageNum) {
	BufferPageHandle *pageHdl = GET_PAGE(session, m_heapFile, PAGE_HEAP, pageNum, Shared, m_dboStats, NULL);
	Sample *sample = sampleBufferPage(session, pageHdl->getPage());
	session->releasePage(&pageHdl);
	sample->m_ID = (SampleID)pageNum;

	return sample;
}

/**
 * 获得堆数据对象统计信息 
 * @return 数据对象状态
 */
DBObjStats* DrsHeap::getDBObjStats() {
	m_dboStats->m_statArr[DBOBJ_ITEM_READ] = m_status.m_rowsReadRecord + m_status.m_rowsReadSubRec;
	m_dboStats->m_statArr[DBOBJ_ITEM_UPDATE] = m_status.m_rowsUpdateRecord + m_status.m_rowsUpdateSubRec;
	return m_dboStats;
}
} // namespace ntse {
