/**
 * Heap.cpp
 *
 * @author л��(ken@163.org)
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
 * �ѹ��캯��
 *
 * @param db ���ݿ����
 * @param heapFile ���ļ�
 * @param headerPage ����ҳ
 * @param dboStats ���ݶ���״̬
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
 * ���ݱ����öѰ汾
 *
 * @param tableDef
 * @return HeapVersionö������
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
 * ����һ���Ѳ���ʼ��
 *
 * @param db ���ݿ�
 * @param path ���ļ�����·��
 * @param tableDef ����
 * @throw �ļ��޷�������IO����ȵ�
 */
void DrsHeap::create(Database *db, const char *path, const TableDef *tableDef) throw(NtseException) {
	ftrace(ts.hp, tout << path;);
	u64 errCode;
	/* �������ļ� */
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
 * ��һ��DRS���ļ�����������������Ϣ
 *
 * @param db ���ݿ����
 * @param session �Ự
 * @param path ���ļ�·��
 * @return �Ѷ���
 * @throw NtseException �ļ��Ҳ������ļ���ʽ����ȷ�ȵ�
 */
DrsHeap* DrsHeap::open(Database *db, Session *session, const char *path, const TableDef *tableDef) throw(NtseException) {
	assert(db && session && path);
	ftrace(ts.hp, tout << path;);
	u64 errCode;
	/* �򿪶��ļ� */
	File *heapFile = new File(path);
	assert(heapFile);
	errCode = heapFile->open(db->getConfig()->m_directIo);
	if (File::E_NO_ERROR != File::getNtseError(errCode)) {
		delete heapFile;
		NTSE_THROW(errCode, "Cannot open heap file %s", path);
	}

	DBObjStats *dbObjStats = new DBObjStats(DBO_Heap);

	BufferPageHdr *headerPage; // ȡ��ͨ����ҳ��Ϣ�������Ѻͱ䳤����һ���ġ�
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

	db->getPageBuffer()->unlockPage(session->getId(), headerPage, Exclusived); 	// ֻ�����ͷҳ���������ͷ�pin��ʼ�ս���ͷҳpin�ڻ�����
	assert(heap);
	return heap;
}

/**
 * ��ס��ҳ���ҷ���
 *
 * @param session �Ự����
 * @param lockMode ��ģʽ
 * @return ҳ����
 */
BufferPageHandle* DrsHeap::lockHeaderPage(Session *session, LockMode lockMode) {
	assert(session);
	return LOCK_PAGE(session, m_headerPage, m_heapFile, 0, lockMode);
}

/**
 * �ͷ���ҳ��������pin
 *
 * @param session �Ự����
 * @param handle ��ҳ���
 */
void DrsHeap::unlockHeaderPage(Session *session, BufferPageHandle **handle) {
	assert(session && handle);
	session->unlockPage(handle);
}



/**
 * ��չ���ļ�����չ����ͳһ����Database�����
 *
 * @param session			�Ự
 * @param headerPage		��ͷҳ
 * @return ������չ�˶��ٸ�ҳ��
 */
u16 DrsHeap::extendHeapFile(Session *session, HeapHeaderPageInfo *headerPage) {
	ftrace(ts.hp || ts.recv, tout << session << headerPage; );
	assert(headerPage);
	/* ��չ�ļ� */
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

	// ��ÿ���Զ���չ֮�󣬻���Scavenger�̣߳�Flush LRU����β���Ĳ�����ҳ
	//m_buffer->signalScavenger();

	m_maxPageNum += extendSize;
	((HeapHeaderPageInfo *)headerPage)->m_pageNum = m_maxPageNum;
	/* ��β���� */
	afterExtendHeap(extendSize);

	return extendSize;
}


/**
 * ���ϻָ�ʱREDO���������
 *
 * @param db ���ݿ����
 * @param session �Ự
 * @param path ���ļ�·��
 * @param tableDef ����
 * @throw NtseException �ļ���������
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
	if (fileSize <= 3 * Limits::PAGE_SIZE) { // ˵����û�������¼��redoһ�º���
		file.close();
		file.remove();
		DrsHeap::create(db, path, tableDef);
	} else {
		file.close();
	}
}

/**
 * ɾ�����ļ�
 *
 * @param path ���ļ�������·����������׺
 * @throw NtseException �ļ���������
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

/** �رն�
 *
 * @param session  �Ự
 * @param flushDirty  �Ƿ�ˢ����ҳ
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
 * ˢ��������
 *
 * @pre ���е�д�����Ѿ�������
 * @param session �Ự
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
 * ��ö�ģ�������ļ�ָ��
 *
 * @param files IN/OUT  ��ģ�������Fileָ�����飬�ռ�����߷���
 * @param pageTypes File�����Ӧ��ҳ�����ͣ����ڶ���˵��PAGE_HEAP
 * @param numFile files���鳤��
 * @return ��ģ��File�������
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
 * ���ϻָ���������һ��������redo������ô�������������������Ĺ���
 *
 * @param session �Ự
 */
void DrsHeap::redoFinish(Session *session) {
	ftrace(ts.recv, tout << session);
	assert(session);
	assert(m_maxPageNum >= m_maxUsedPageNum);

	nftrace(ts.recv, tout << "m_maxPageNum: " << m_maxPageNum << ", Old m_maxUsedPageNum: " << m_maxUsedPageNum);
	// ���¼���m_maxUsedPageNum
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
 * ��ӡ��Ϣ��������
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
 * �õ������Ѿ�ʹ�õ����ݴ�С
 *
 * @return ���Ѿ�ʹ�õ����ݴ�С
 */
u64 DrsHeap::getUsedSize() {
	assert(m_heapFile);
	return Limits::PAGE_SIZE * m_maxUsedPageNum;
}

#ifdef TNT_ENGINE
/** дinsert��log��־
 * @param session �����Ự
 * @param txnId �����������������id
 * @param preLsn ͬһ����ǰһ����־��lsn
 * @param rid �����¼��rowid
 * return ��־��lsn
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

/** ����insert��log��־
 * @param log ��Ҫ������log��־
 * @param rid out ��Ҫ�����¼��rowid
 * @param preLsn out ͬһ����ǰһ����־��lsn
 * @param txnId out �����������������id
 */
void DrsHeap::parseInsertTNTLog(const LogEntry *log, TrxId *txnId, LsnType *preLsn, RowId *rid) {
	Stream s(log->m_data, log->m_size);
	s.read(txnId);
	s.read(preLsn);
	s.readRid(rid);
}
#endif

/**
 * ��ʼһ�α�ɨ��
 *
 * @param heap              �Ѷ���
 * @param session           �Ự����
 * @param extractor         �Ӽ�¼��ȡ��
 * @param lockMode          �Է��صļ�¼Ҫ�ӵ���������ΪNone
 * @param pRowLockHdl OUT   ����������洢�����������lockModeΪNoneʱΪNULL
 * @param returnLinkSrc    �������ڱ䳤�������壩�������Ӽ�¼��true��ʾ��ɨ�赽Դʱ���ؼ�¼��false��ʾ��ɨ����Ŀ��ʱ���ؼ�¼
 * @return ��ɨ����
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

/** �������� */
DrsHeapScanHandle::~DrsHeapScanHandle() {
	if (m_pageHdl) {
		m_session->unpinPage(&m_pageHdl);
	}
}

/**
 * ��insert��־����ʱ����Record
 * @param log          ��־��
 * @param outRec OUT   �����ļ�¼
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
 * ��insert��־�л�ȡRowId
 * ����parseInsertLog��һ�����ټ򻯰汾
 * @param inslog         insert��־��
 * @return               insert��¼��RowId
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
	if (fastSample) { // ����������
		handle->m_bufScanHdl = m_buffer->beginScan(session->getId(), m_heapFile);
		return handle;
	} 
	// ����������
	handle->m_minPage = metaDataPgCnt(); /* ��ҳ���ɲ�����meta dataҳ���ɲ��� */
	handle->m_maxPage = m_maxUsedPageNum; /* �������ɵ�������ҳ */
	if (handle->m_maxSample > (handle->m_maxPage - handle->m_minPage + 1))
		handle->m_maxSample = (uint)(handle->m_maxPage - handle->m_minPage + 1);
	handle->m_blockNum = 8 > maxSampleNum ? maxSampleNum : 8;   /** ��Ϊ8���������� **/
	handle->m_regionSize = (handle->m_maxPage + 1 - handle->m_minPage) / handle->m_blockNum;
	if (handle->m_regionSize <= 1) { // ���ҳ��̫�٣������Է�������ȫ������
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
			sample->m_ID = bcb->m_pageKey.m_pageId; // ��Ҫ����������ID
			return sample;
		} else {
			return NULL;
		}
	} else {
		if (shdl->m_blockPages) {
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
			// ��һ��sampleNext
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
 * ��ö����ݶ���ͳ����Ϣ 
 * @return ���ݶ���״̬
 */
DBObjStats* DrsHeap::getDBObjStats() {
	m_dboStats->m_statArr[DBOBJ_ITEM_READ] = m_status.m_rowsReadRecord + m_status.m_rowsReadSubRec;
	m_dboStats->m_statArr[DBOBJ_ITEM_UPDATE] = m_status.m_rowsUpdateRecord + m_status.m_rowsUpdateSubRec;
	return m_dboStats;
}
} // namespace ntse {
