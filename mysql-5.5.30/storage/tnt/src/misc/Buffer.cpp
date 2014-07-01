/**
 * ҳ�滺�����
 *
 * @author ��Դ(wangyuan@corp.netease.com, wy@163.org)
 */

#include <vector>
#include <algorithm>
#include "misc/Buffer.h"
#include "util/Thread.h"
#include "util/File.h"
#include "misc/Syslog.h"
#include "misc/Txnlog.h"
#include "misc/Trace.h"
#include "misc/Session.h"
#include "api/Database.h"
#include "misc/Profile.h"

namespace ntse {
/** Ԥ������ */
struct PrefetchRequest {
	Mutex	m_lock;		/** ���ڴ���Ԥ������ʱ������һ���� */
	u16		m_userId;	/** ����Ԥ�����û�ID��ҳ�汻����û����� */
	File	*m_file;	/** ���ĸ��ļ���Ԥ����ΪNULL��ʾΪ����Ԥ��������� */
	std::vector<Bcb *>	m_pagesToRead;	/** ҪԤ����ҳ��һ������ͬһ�ļ���ҳ�����ͣ�����ҳ��Ϊ������ */

	PrefetchRequest(): m_lock("PrefetchRequest::lock", __FILE__, __LINE__) {
		m_file = NULL;
	}
};


/** Ԥ���߳� */
class Prefetcher: public BgTask {
public:
	Prefetcher(Database *db, Buffer *buffer, uint queueSize, Syslog *syslog);
	~Prefetcher();
	void runIt();
	void canclePrefetch(File *file);
	PrefetchRequest* preparePrefetch(u16 userId, File *file);
	void commitPrefetch(PrefetchRequest *request);
	void canclePrefetch(PrefetchRequest *request);

private:
	void prefetch(PrefetchRequest *request);

private:
	Mutex	m_lock;				/** �����������ʵ��� */
	Buffer	*m_buffer;			/** ҳ�滺�� */
	uint	m_maxRequest;		/** Ԥ��������д�С */
	PrefetchRequest	*m_queue;	/** Ԥ��������� */
	Syslog	*m_syslog;			/** ϵͳ��־ */
};

/** ����ҳд�������̵ĺ�̨�߳� 
 * ���̵߳���ҪĿ�Ĳ�����Ҫ��ʱˢд��ҳ������Ϊ�˱�֤�󲿷�ʱ�̻���LRU����β���㹻�ķ���ҳ�������滻
 * ���绺��LRUβ�Ѿ����㹻�ķ���ҳ���ˣ�������̲߳�Ӧ���ٶ�ˢд��ҳ�����ⲻ��Ҫ��IO
 */
class Scavenger: public BgTask {
public:
#ifdef TNT_ENGINE
	Scavenger(Database *db, Buffer *buffer, uint interval, double maxCleanPagesRatio, uint maxScavengerPages);
#else
	Scavenger(Database *db, Buffer *buffer, uint interval, double maxPagesRatio, double multiplier);
#endif
	~Scavenger();
	void runIt();
	double getMaxPagesRatio();
	void setMaxPagesRatio(double maxPagesRatio);
	double getMultiplier();
	void setMultiplier(double multiplier);
	uint getMaxScavengerPages();
	void setMaxScavengerPages(uint maxScavengerPages);
	u64 getRealScavengeCnt();
	void disable();
	void enable();

private:
	Buffer	*m_buffer;			/** ҳ�滺�� */
	double	m_maxPagesRatio;	/** һ������ʱ���д������ҳռ�����С֮�� */
	double	m_multiplier;		/** �Ŵ�ϵ�� */
	u64		m_pageCreates;		/** ҳ�滺���½�ҳ����� */
	bool	m_enabled;			/** �Ƿ����ú�̨ˢ��ҳ���� */
	AioArray m_aioArray;		/** ������߳�aio���� */
	u64		m_realScavengeCnt;  /** ���������ˢҳ��Ĵ��� */
	bool	m_useAio;			/** ������Ƿ�ʹ��AIO */

#ifdef TNT_ENGINE
	double	m_maxCleanPagesRatio;	/** һ���������ά��LRU����β��CLEAN Pages�ĳ���ռLRU�ܳ��ȵı��� */
	uint	m_maxCleanPagesLen;		/** һ���������ά��LRU����β��CLEAN Pages�ĳ��ȣ����ݱ���������� */	
	uint	m_maxScavengerPages;	/** һ���������д������ҳ�������ɸ���I/O�������� */
#endif

};

const double Buffer::OLD_GEN_RATIO = 0.3;
const double Buffer::SCAVENGER_DEFAULT_MAX_PAGES_RATIO = 0.1;
const double Buffer::SCAVENGER_DEFAULT_SEARCH_LENGTH_MULTIPLIER = 2.0;
const double Buffer::DEFAULT_PREFETCH_RATIO = 0.5;
const double Buffer::PREFETCH_RIGHT_ORDER_RATIO = 0.5;

#ifdef TNT_ENGINE
const double Buffer::SCAVENGER_DEFAULT_CLEAN_PAGES_LEN = 0.1;
#endif

BatchIoBufPool Buffer::m_batchIOBufPool(BatchIoBufPool::DFL_INSTANCE_NUM, Buffer::BATCH_IO_SIZE / BatchIoBufPool::DFL_PAGE_SIZE, 
										BatchIoBufPool::DFL_PAGE_SIZE);

/**
 * ����һ��ҳ�滺��
 *
 * @param db �������ݿ⣬�ڲ���ʱ����ΪNULL
 * @param numPages �����С
 * @param pagePool �����ڴ�ҳ��
 * @param syslog ϵͳ��־
 * @param txnlog ������־
 */
Buffer::Buffer(Database *db, uint numPages, PagePool *pagePool, Syslog *syslog, Txnlog *txnlog): PagePoolUser(numPages, pagePool),
	m_lock("Buffer::lock", __FILE__, __LINE__) {
	ftrace(ts.buf, tout << db << numPages << pagePool << syslog << txnlog);
	m_db = db;
	m_syslog = syslog;
	m_txnlog = txnlog;
	m_oldGenRatio = OLD_GEN_RATIO;
	m_checksumed = true;
	m_correlatedRefPeriod = CORRELATED_REF_PERIOD;
	m_prefetcher = new Prefetcher(db, this, PREFETCH_QUEUE_SIZE, m_syslog);
	m_prefetcher->start();
#ifdef TNT_ENGINE
	uint maxFlushPageLen = 200;		// For Unit Test
	if (db != NULL) {
		maxFlushPageLen = db->getConfig()->m_maxFlushPagesInScavenger;
	}
	m_scavenger = new Scavenger(db, this, SCAVENGER_DEFAULT_INTERVAL, SCAVENGER_DEFAULT_CLEAN_PAGES_LEN, maxFlushPageLen);
#else
	m_scavenger = new Scavenger(db, this, SCAVENGER_DEFAULT_INTERVAL, SCAVENGER_DEFAULT_MAX_PAGES_RATIO, SCAVENGER_DEFAULT_SEARCH_LENGTH_MULTIPLIER);
#endif
	m_scavenger->start();
	m_prefetchSize = DEFAULT_PREFETCH_SIZE;
	m_prefetchPages = m_prefetchSize / pagePool->getPageSize();
	m_prefetchRatio = DEFAULT_PREFETCH_RATIO;
	m_correlatedRefPeriod = (u64)(numPages * m_oldGenRatio * 0.5);
	m_readSeq = 1;
	// ����Ԥ��ʱ��һ���г������ٱ�����ҳ���Ѿ��ڻ�����ʱ
	// �����ȡ����ҳ�������˳���������ҳ��
	// ����ļ�����������˳���ȡ��������100MB/s�������ȡһ��
	// ҳ���ʱ����3ms
	int batchPages = BATCH_IO_SIZE / pagePool->getPageSize();
	int seqReadSpeed = 100 * 1024 * 1024;
	double randomReadTime = 0.003;
	double seqReadTime = randomReadTime + (double)BATCH_IO_SIZE / seqReadSpeed;
	m_skipPrefetchRatio = 1 - seqReadTime / batchPages / randomReadTime;
	memset(&m_status, 0, sizeof(BufferStatus));
	memset(&m_flushStatus, 0, sizeof(BufferFlushStatus));
}

/**
 * ���������������ͷŻ���ռ�õ�ҳ�棬��Ϊ��һ����ֻ��ϵͳֹͣʱ�ŵ��ã�
 * �����ڴ����ڴ�ҳ��ͳһ�ͷ�
 */
Buffer::~Buffer() {
	ftrace(ts.buf, );
	m_prefetcher->stop();
	m_prefetcher->join();
	delete m_prefetcher;
	m_scavenger->stop();
	m_scavenger->join();
	delete m_scavenger;
}

/**
 * @see PagePoolUser::freeSomePages
 */
uint Buffer::freeSomePages(u16 userId, uint numPages) {
	ftrace(ts.buf, tout << userId << numPages);
	uint count = PagePoolUser::freeSomeFreePages(userId, numPages);
	if (count >= numPages)
		return count;

	std::vector<KeyPagePair> freePages;
	RWLOCK(&m_lock, Shared);
	getFreePagesFromList(&freePages, &m_oldGen, numPages - count);
	if (freePages.size() + count < numPages)
		getFreePagesFromList(&freePages, &m_newGen, numPages - count - freePages.size());
	RWUNLOCK(&m_lock, Shared);

	for (size_t i = 0; i < freePages.size(); i++) {
		if (m_pool->lockPageIfType(userId, freePages[i].m_page, Exclusived, freePages[i].m_pageKey.m_pageType,
			UNSAFE_LOCK_TIMEOUT, __FILE__, __LINE__)) {
			Bcb *bcb = (Bcb *)m_pool->getInfo(freePages[i].m_page);
			// �Ի���������ҳ��֮�󣬶Ը�ҳΨһ���ܽ��еĲ�������unPin
			// ����ڼ�BcbԪ��Ϣ���ж�pinCountΪ0֮�󣬿��԰�ȫ�Ľ�Bcb
			// Ԫ��Ϣ���ͷŵ�
			LOCK(&bcb->m_metaLock);
			if (bcb->m_dirty || bcb->m_pinCount > 0) {
				UNLOCK(&bcb->m_metaLock);
				m_pool->unlockPage(userId, freePages[i].m_page, Exclusived);
				continue;
			}
			UNLOCK(&bcb->m_metaLock);
			freeBlock(userId, bcb);
			count++;
		}
	}
	return count;
}

/**
 * ��ȡһ��ҳ��pin��������ҳ
 *
 * @param session �Ự��������ΪNULL
 * @param file �ļ�
 * @param pageType ҳ������
 * @param pageId ҳ��
 * @param lockMode ��ģʽ
 * @param dbObjStats ���ݶ���״̬
 * @guess Ҫ��ȡ��ҳ���п����������ַ
 *
 * @return ҳ���׵�ַ
 */
BufferPageHdr* Buffer::getPage(Session *session, File *file, PageType pageType, u64 pageId,
							   LockMode lockMode, DBObjStats* dbObjStats, BufferPageHdr *guess) {
	ftrace(ts.buf, tout << session << file << pageType << pageId << lockMode << dbObjStats << guess);

	assert(lockMode == Shared || lockMode == Exclusived);

	// ���ָ����guess�����ȿ���ָ����ҳ���Ƿ�Ϊ����ҳ�棬
	// ������ֱ�ӷ��ظ�ҳ�棬������������������
	if (guess && m_pool->lockPageIfType(session->getId(), guess, lockMode, pageType, UNSAFE_LOCK_TIMEOUT, __FILE__, __LINE__)) {
		Bcb *bcb = (Bcb *)m_pool->getInfo(guess);
		if (bcb->m_pageKey.m_file == file && bcb->m_pageKey.m_pageId == pageId) {
			pinBlock(session->getId(), bcb, false);
#ifdef NTSE_VERIFY_EX
			syncChecksum(bcb, lockMode);
#endif
			m_status.m_logicalReads++;
			if (session)
				session->incOpStat(OPS_LOG_READ);
			dbObjStats->countIt(DBOBJ_LOG_READ);
			if (bcb->m_dbObjStats != dbObjStats)
				replaceDBObjStats(bcb, dbObjStats);
			return guess;
		}
		m_pool->unlockPage(session->getId(), guess, lockMode);
	}
	return doGetPage(session, file, pageType, pageId, lockMode, true, dbObjStats);
}

/**
 * ����һ����ҳ��pin��������ҳ
 *
 * @param session �Ự������ΪNULL
 * @param file �ļ�
 * @param pageType ҳ������
 * @param pageId ҳ��
 * @param lockMode ��ģʽ
 * @param dbObjStats ���ݶ���״̬
 *
 * @return ҳ���׵�ַ
 */
BufferPageHdr* Buffer::newPage(Session *session, File *file, PageType pageType, u64 pageId,
							   LockMode lockMode, DBObjStats* dbObjStats) {
	ftrace(ts.buf, tout << session << file << pageType << pageId << lockMode <<dbObjStats);
	return doGetPage(session, file, pageType, pageId, lockMode, false, dbObjStats);
}

/**
 * ��ȡһ��ҳ��pin��������ҳ
 *
 * @param session �Ự��������ΪNULL
 * @param file �ļ�
 * @param pageType ҳ������
 * @param pageId ҳ��
 * @param lockMode ��ģʽ
 * @param readFromFile ��Ҫ��ȡ��ҳ���ڻ�����ʱ���Ƿ�Ҫ���ļ��ж�ȡҳ������
 * @param dbObjStats ���ݶ���״̬
 *
 * @return ҳ���׵�ַ
 */
BufferPageHdr* Buffer::doGetPage( Session *session, File *file, PageType pageType, u64 pageId, LockMode lockMode, bool readFromFile, DBObjStats *dbObjStats ) {
	Bcb *bcb = NULL;

	assert(lockMode == Shared || lockMode == Exclusived);

	m_status.m_logicalReads++;
	m_status.m_pendingAllocs.increment();
	session->incOpStat(OPS_LOG_READ);
	dbObjStats->countIt(DBOBJ_LOG_READ);

	LockMode mode = Shared;
_START:
	RWLOCK(&m_lock, mode);
	PageKey pageKey(file, pageType, pageId);
	// ������ϣ��
	bcb = m_pageHash.get(&pageKey);
	if (bcb) {
		// ���ҵ��������ڳ���ȫ�ֽṹ��ʱ���Զ�ҳ������������ɹ�
		// ���ͷ�ȫ�ֽṹ������ͼ����������Ƿ�Ϊ��Ҫ��ҳ��
		// ��������һ��ҳʱ���ܻ�ȴ��Ƚϳ�ʱ�䣨ҳ��������IO����
		// ��������ͷ�ȫ�ֽṹ���Ǳ�Ҫ�ġ�
		// ���ȳ��Լ����ڲ�������ͻʱ�����Ż����ܡ�
		BufferPageHdr *page = bcb->m_page;
		if (!m_pool->trylockPage(session->getId(), page, lockMode, __FILE__, __LINE__)) {
			RWUNLOCK(&m_lock, mode);
			if (!m_pool->lockPageIfType(session->getId(), page, lockMode, pageType, UNSAFE_LOCK_TIMEOUT, __FILE__, __LINE__)) {
				goto _START;
			}
			bcb = (Bcb *)m_pool->getInfo(page);
			if (bcb->m_pageKey.m_file != file || bcb->m_pageKey.m_pageId != pageId) {
				m_pool->unlockPage(session->getId(), page, lockMode);
				m_status.m_unsafeLockFails++;
				goto _START;
			}
		} else
			RWUNLOCK(&m_lock, mode);
		assert(bcb->m_pageKey.m_pageType == pageType);	// ҳ�����Ͳ����ܷ����仯
		pinBlock(session->getId(), bcb, true);
#ifdef NTSE_VERIFY_EX
		syncChecksum(bcb, lockMode);
#endif
		if (bcb->m_dbObjStats != dbObjStats)
			replaceDBObjStats(bcb, dbObjStats);
		m_status.m_pendingAllocs.decrement();
		return page;
	} else {
		// ���Ҳ����������һ���飬������Ҫȷ������ȫ�ֽṹ������
		if (mode == Shared) {
			mode = Exclusived;
			RWUNLOCK(&m_lock, Shared);
			goto _START;
		}

		bcb = tryAllocBlock(session->getId(), file, pageType, pageId);
		if (bcb) {
			RWUNLOCK(&m_lock, Exclusived);
			assert(!bcb->m_dbObjStats);
			LOCK(&bcb->m_metaLock);
			bcb->m_dbObjStats = dbObjStats;
			UNLOCK(&bcb->m_metaLock);
			postAlloc(session, bcb, readFromFile, lockMode);
#ifdef NTSE_VERIFY_EX
			syncChecksum(bcb, lockMode);
#endif
			m_status.m_pendingAllocs.decrement();
			return bcb->m_page;
		}
		assert(m_lock.isLocked(Exclusived));
		RWUNLOCK(&m_lock, Exclusived);
		Thread::msleep(GET_PAGE_SLEEP);
		goto _START;
	}
}

/**
 * ��ͼ��ȡһ��ҳ��pin����������ҳ
 *
 * @param session �Ự
 * @param file �ļ�
 * @param pageType ҳ������
 * @param pageId ҳ��
 * @param lockMode ��ģʽ
 * @param dbObjStats ���ݶ���״̬
 *
 * @return ҳ���׵�ַ
 */
BufferPageHdr* Buffer::tryGetPage(Session *session, File *file, PageType pageType, u64 pageId,
								  LockMode lockMode, DBObjStats* dbObjStats) {
	ftrace(ts.buf, tout << session << file << pageType << pageId << lockMode);
	Bcb *bcb = NULL;

	m_status.m_logicalReads++;
	session->incOpStat(OPS_LOG_READ);
	dbObjStats->countIt(DBOBJ_LOG_READ);

	LockMode mode = Shared;
_START:
	RWLOCK(&m_lock, mode);
	PageKey pageKey(file, pageType, pageId);
	// ������ϣ��
	bcb = m_pageHash.get(&pageKey);
	if (bcb) {
		if (m_pool->trylockPage(session->getId(), bcb->m_page, lockMode, __FILE__, __LINE__)) {
			RWUNLOCK(&m_lock, mode);
			pinBlock(session->getId(), bcb, true);
#ifdef NTSE_VERIFY_EX
			syncChecksum(bcb, lockMode);
#endif
			if (bcb->m_dbObjStats != dbObjStats)
				replaceDBObjStats(bcb, dbObjStats);
			return bcb->m_page;
		} else {
			RWUNLOCK(&m_lock, mode);
			return NULL;
		}
	} else {
		// ���Ҳ����������һ���飬������Ҫȷ������ȫ�ֽṹ������
		if (mode == Shared) {
			mode = Exclusived;
			RWUNLOCK(&m_lock, Shared);
			goto _START;
		}

		bcb = tryAllocBlock(session->getId(), file, pageType, pageId);
		if (bcb) {
			RWUNLOCK(&m_lock, Exclusived);
			assert(!bcb->m_dbObjStats);
			bcb->m_dbObjStats = dbObjStats;
			postAlloc(session, bcb, true, lockMode);
#ifdef NTSE_VERIFY_EX
			syncChecksum(bcb, lockMode);
#endif
			return bcb->m_page;
		}
		RWUNLOCK(&m_lock, Exclusived);
		return NULL;
	}
}
/**
 * �ж�ҳ���Ƿ��Ѿ��ڻ�����
 * 
 * @param session �Ự
 * @param file �ļ�
 * @param pageType ҳ������
 * @param pageId ҳ��
 *
 * @return ҳ�ڻ����з���true�����򷵻�false
 */
bool Buffer::hasPage(File *file, PageType pageType, u64 pageId) {
	RWLockGuard(&m_lock, Shared, __FILE__, __LINE__);
	PageKey pageKey(file, pageType, pageId);
	return m_pageHash.get(&pageKey) != NULL;
}

/**
 * �������IO�����
 * @return
 */
BatchIoBufPool* Buffer::getBatchIoBufferPool() {
	return &m_batchIOBufPool;
}

/**
 * �ͷ�һ��ҳ�������ͷŸ�ҳ������pin
 *
 * @param session �Ự
 * @param page ҳ���ַ
 * @param lockMode ��ģʽ
 */
void Buffer::releasePage(Session *session, BufferPageHdr *page, LockMode lockMode) {
	ftrace(ts.buf, tout << session << (Bcb *)m_pool->getInfo(page) << lockMode);
	unpinPage(page);
	unlockPage(session->getId(), page, lockMode);
}

/**
 * ������������ϵͳ���ȳ��Բ����������������������ɹ���
 * ���ͷ����еĹ�������Ȼ��ӻ�������
 * @pre �����߳���pin
 * @pre �����߶�ָ��ҳ�ӵ��Ƕ���
 *
 * @param session ������������ĻỰ
 * @param page ҳ���ַ
 */
void Buffer::upgradePageLock(Session *session, BufferPageHdr *page) {
	ftrace(ts.buf, tout << session << (Bcb *)m_pool->getInfo(page));
	assert(((Bcb *)m_pool->getInfo(page))->m_pinCount > 0);
	assert(m_pool->isPageLocked(page, Shared));
	if (m_pool->tryUpgradePageLock(session->getId(), page, __FILE__, __LINE__))
		return;
	unlockPage(session->getId(), page, Shared);
	lockPage(session->getId(), page, Exclusived, false);
}

/**
 * �ͷ�һ��ҳ�ϵ�pin
 * ָ��ҳ����û�б�����
 *
 * @param page ҳ���ַ
 */
void Buffer::unpinPage(BufferPageHdr *page) {
	ftrace(ts.buf, tout << (Bcb *)m_pool->getInfo(page));
	Bcb *bcb = (Bcb *)m_pool->getInfo(page);
	unpinBlock(bcb);
}

/**
 * ����һ��ҳ��Ϊ��ҳ
 * @pre ҳ���Ѿ����û���������
 *
 * @param session �Ự������ΪNULL
 * @param page ҳ���ַ
 */
void Buffer::markDirty(Session *session, BufferPageHdr *page) {
	ftrace(ts.buf, tout << session << (Bcb *)m_pool->getInfo(page));
	assert(m_pool->isPageLocked(page, Exclusived));
	Bcb *bcb = (Bcb *)m_pool->getInfo(page);

#ifdef NTSE_VERIFY_EX
	bcb->m_dirtyMarked = true;
#endif
	if (bcb->m_dirty == false)
		m_status.m_curDirtyPages.increment();
	bcb->m_dirty = true;
	m_status.m_logicalWrites++;
	session->incOpStat(OPS_LOG_WRITE);
	bcb->m_dbObjStats->countIt(DBOBJ_LOG_WRITE);
}

/**
 * �鿴һ��ҳ���Ƿ�Ϊ��ҳ
 * @pre ҳ���Ѿ�������
 *
 * @param page
 * @return ҳ���Ƿ�Ϊ��ҳ
 */
bool Buffer::isDirty(BufferPageHdr *page) {
	assert(m_pool->isPageLocked(page, Shared) || m_pool->isPageLocked(page, Exclusived));

	Bcb *bcb = (Bcb *)m_pool->getInfo(page);
	return bcb->m_dirty;
}

/**
 * ��һ��ҳд�����ļ��У�ͬʱ������־λ
 * @pre ҳ���Ѿ�������
 *
 * @param session �Ự������ΪNULL
 * @param page ҳ���ַ
 */
void Buffer::writePage(Session *session, BufferPageHdr *page) {
	ftrace(ts.buf, tout << session << (Bcb *)m_pool->getInfo(page));
	assert(m_pool->isPageLocked(page, Shared) || m_pool->isPageLocked(page, Exclusived));

	Bcb *bcb = (Bcb *)m_pool->getInfo(page);
	LOCK(&bcb->m_metaLock);
	if(bcb->m_isWriting) {
		UNLOCK(&bcb->m_metaLock);
		return;
	}
	bcb->m_isWriting = true;
	UNLOCK(&bcb->m_metaLock);

	m_status.m_pendingWrites.increment();
		
	if (m_txnlog && page->m_lsn != INVALID_LSN)	// WAL
		m_txnlog->flush(page->m_lsn, FS_SINGLE_WRITE);

	updateChecksum(bcb->m_page);

	u64 before = System::currentTimeMillis();
	u64 code = bcb->m_pageKey.m_file->write(
		bcb->m_pageKey.m_pageId * m_pool->getPageSize(),
		m_pool->getPageSize(),
		bcb->m_page);
	m_status.m_writeTime += System::currentTimeMillis() - before;
	if (code != File::E_NO_ERROR) {
		m_syslog->fopPanic(code, "Write page"I64FORMAT"u of file %s failed",
			bcb->m_pageKey.m_pageId,
			bcb->m_pageKey.m_file->getPath());
	}
	LOCK(&bcb->m_metaLock);
	bcb->m_isWriting = false;
	if (bcb->m_dirty == true)
		m_status.m_curDirtyPages.decrement();
	bcb->m_dirty = false;	
	UNLOCK(&bcb->m_metaLock);
	m_status.m_physicalWrites++;
	if (session)
		session->incOpStat(OPS_PHY_WRITE);
	bcb->m_dbObjStats->countIt(DBOBJ_PHY_WRITE);
	m_status.m_pendingWrites.decrement();
}


/** ����д��ҳ�棬����д���ո���չ������ҳ��
 *
 * @param session �Ự
 * @param file �����ļ�
 * @param pageType ҳ������
 * @param minPid Ҫд������Сҳ�ţ�����
 * @param maxPid Ҫд�������ҳ�ţ�����
 */
void Buffer::batchWrite(Session *session, File *file, PageType pageType, u64 minPid, u64 maxPid) {
	assert(minPid <= maxPid);
	ftrace(ts.buf, tout << session << pageType << file << minPid << maxPid);

	uint batchPages = BATCH_IO_SIZE / Limits::PAGE_SIZE;
	s64 batchStart = -1;
	Bcb *bcb;
	std::vector<Bcb *> bcbs;

	RWLOCK(&m_lock, Shared);
	for (u64 pid = minPid; pid <= maxPid; pid++) {
		bool succ = false;
		bool otherIsWriting = false;
		PageKey pageKey(file, pageType, pid);
		bcb = m_pageHash.get(&pageKey);
		if (bcb) {
			// ���ڱ�����ֻ����д���ո���չ������ҳ�棬��Щҳ�治�ᱻ����Scavenger֮���Ӧ��ʹ�ã�
			// ���tryLockͨ���ܳɹ�
			succ = m_pool->trylockPage(session->getId(), bcb->m_page, Shared, __FILE__, __LINE__);
			if (succ) {
				LOCK(&bcb->m_metaLock);
				if(bcb->m_isWriting) {
					UNLOCK(&bcb->m_metaLock);
					otherIsWriting = true;
					succ = false;
					m_pool->unlockPage(session->getId(), bcb->m_page, Shared);
				}else {
					bcb->m_isWriting = true;
					UNLOCK(&bcb->m_metaLock);
				}

				if(!otherIsWriting) {
					if (batchStart < 0)
						batchStart = pid;
					bcbs.push_back(bcb);
				}
			} else {
				SYNCHERE(SP_BUF_BATCH_WRITE_LOCK_FAIL);
			}
		}
		if (!succ || bcbs.size() == batchPages) {
			if (bcbs.size()) {
				assert((u64)batchStart >= minPid && (u64)batchStart <= maxPid);

				RWUNLOCK(&m_lock, Shared);

				batchWritePages(session, file, batchStart, &bcbs);
				batchStart = -1;
				m_status.m_extendWrites += bcbs.size();
				bcbs.clear();

				RWLOCK(&m_lock, Shared);
			}
			if (bcb && !otherIsWriting) {	// �ڻ����У����Ӳ�����, ����ҳ�治�����ڱ������߳�д
				RWUNLOCK(&m_lock, Shared);
				BufferPageHdr *page = getPage(session, file, pageType, pid, Shared, bcb->m_dbObjStats, bcb->m_page);
				writePage(session, page);
				m_status.m_extendWrites++;
				releasePage(session, page, Shared);
				RWLOCK(&m_lock, Shared);
			}
		}
	}
	RWUNLOCK(&m_lock, Shared);
	if (bcbs.size())
		batchWritePages(session, file, batchStart, &bcbs);
	m_status.m_extendWrites += bcbs.size();
}

/** ����ҳ��У���
 * @param page ҳ��
 */
void Buffer::updateChecksum(BufferPageHdr *page) {
	if (page->m_checksum != BufferPageHdr::CHECKSUM_DISABLED) {
		if (m_checksumed)
			page->m_checksum = checksumPage(page);
		else
			page->m_checksum = BufferPageHdr::CHECKSUM_NO;
	}
}

/** ����д��ҳ������
 * @param session �Ự
 * @param file �ļ�
 * @param startPid Ҫд������ʼҳ��
 * @param bcbs Ҫд����ҳ��Ŀ��ƿ�
 */
void Buffer::batchWritePages(Session *session, File *file, u64 startPid, std::vector<Bcb *>* bcbs) {
	IoBuffer *ioBuffer = (IoBuffer *)m_batchIOBufPool.getInst();
	byte *buf = ioBuffer->getBuffer();
	assert(buf);

	if (ioBuffer->getSize() < bcbs->size() * Limits::PAGE_SIZE) {
		m_syslog->log(EL_PANIC, "Batch IO buffer size is two small, required is :%d, real size is %d.", 
		bcbs->size() * Limits::PAGE_SIZE, ioBuffer->getSize());
	}

	// ����д������������У��ͣ�ͳ�����pageLsn
	u64 maxLsn = 0;
	for (size_t i = 0; i < bcbs->size(); i++) {
		Bcb *bcb = bcbs->operator[](i);
		if (m_txnlog && bcb->m_page->m_lsn != INVALID_LSN && bcb->m_page->m_lsn > maxLsn)
			maxLsn = bcb->m_page->m_lsn;
		updateChecksum(bcb->m_page);
		memcpy(buf + i * Limits::PAGE_SIZE, bcb->m_page, Limits::PAGE_SIZE);
	}
	// WAL
	if (maxLsn)
		m_txnlog->flush(maxLsn, FS_BATCH_WRITE);

	// д����
	m_status.m_pendingWrites.increment();

	u64 before = System::currentTimeMillis();
	u64 code = file->write(
		startPid * m_pool->getPageSize(),
		m_pool->getPageSize() * bcbs->size(),
		buf);
	m_status.m_writeTime += System::currentTimeMillis() - before;
	if (code != File::E_NO_ERROR) {
		m_syslog->fopPanic(code, "Batch write page"I64FORMAT"u to page"I64FORMAT"u of file %s failed",
			startPid, startPid + bcbs->size() - 1,
			file->getPath());
	}

	m_status.m_physicalWrites++;
	if (session)
		session->incOpStat(OPS_PHY_WRITE);
	m_status.m_pendingWrites.decrement();

	// �������ݿ����ͳ����Ϣ�����ҳ��dirty��־���ͷ�д��������ҳ��
	for (size_t i = 0; i < bcbs->size(); i++) {
		Bcb *bcb = bcbs->operator[](i);
		
		bcb->m_dbObjStats->countIt(DBOBJ_PHY_WRITE);
		LOCK(&bcb->m_metaLock);
		if (bcb->m_dirty == true)
			m_status.m_curDirtyPages.decrement();
		bcb->m_dirty = false;
		bcb->m_isWriting = false;
		UNLOCK(&bcb->m_metaLock);
		m_pool->unlockPage(session->getId(), bcb->m_page, Shared);
	}

	m_batchIOBufPool.reclaimInst(ioBuffer);
}

/**
 * �ͷ�ҳ�滺����ָ���ļ����������ҳ
 * @pre Ҫ�ͷŵ�ҳ�治�ܱ�pinס��Ҳ���ܱ�����
 *
 * @param session �Ự������ΪNULL
 * @param file �ļ�
 * @param writeDirty �Ƿ�д����ҳ
 */
void Buffer::freePages(Session *session, File *file, bool writeDirty) {
	ftrace(ts.buf, tout << session << file << writeDirty);
	if (writeDirty) {
		try {
			std::vector<KeyPagePair> dirtyPages;
			getDirtyPagesOfFile(&dirtyPages, file, (u64)-1, (u64)-1, false);
			doFlushDirtyPages(session, file, &dirtyPages, true, true);
		} catch (NtseException &) {assert(false);}
	}
	freePages(session, file, 0, NULL);
}

/**
 * �ͷ�ҳ�滺����Ϊָ���������������ҳ
 * @pre Ҫ�ͷŵ�ҳ�治�ܱ�pinס��Ҳ���ܱ�����
 *
 * @param session �Ự
 * @param file �ļ�
 * @param indexId ����ID
 * @param callback �����ж�һ��ҳ�Ƿ���ָ������ʹ�õ�ҳ�Ļص�����
 *   ��ΪNULL���ͷ�Ϊָ���ļ����������ҳ
 */
void Buffer::freePages(Session *session, File *file, uint indexId, bool (*callback)(BufferPageHdr *page, PageId pageId, uint indexId)) {
	ftrace(ts.buf, tout << session << file << indexId << callback);
	// ȡ��������ļ���Ԥ������ֹ�ͷŹ������ֶ�����ļ�����ҳ��
	m_prefetcher->canclePrefetch(file);

	Array<BufferPageHdr *> toFreePages;
	Array<PageType > toFreeTypes;

	// ����ˢд��ҳ�̵߳�Ӱ�죬��Щҳ��������ڱ��������������ϱ��ͷŵ���
	// ������Ҫɨ���Σ�ֱ��ȷ��û��ָ���ļ���ҳ���ڻ����вſ�
	while (true) {
		bool lockFail = false;
		toFreePages.clear();
		toFreeTypes.clear();

		RWLOCK(&m_lock, Shared);
		FilePageList *toFreeList = m_filePageHash.get(file);
		if (!toFreeList) {
			RWUNLOCK(&m_lock, Shared);
			break;
		}
		DLink<Bcb *> *curr;
		for (curr = toFreeList->getBcbList()->getHeader()->getNext();
			curr != toFreeList->getBcbList()->getHeader(); curr = curr->getNext()) {
			Bcb *bcb = curr->get();
			assert(bcb->m_pageKey.m_file == file);
			if (callback == NULL || callback(bcb->m_page, bcb->m_pageKey.m_pageId, indexId)) {
				toFreePages.push(bcb->m_page);
				toFreeTypes.push(bcb->m_pageKey.m_pageType);
			}
		}
		RWUNLOCK(&m_lock, Shared);
		if (toFreePages.getSize() == 0)
			break;

		for (size_t i = 0; i < toFreePages.getSize(); i++) {
			if (m_pool->lockPageIfType(session->getId(), toFreePages[i], Exclusived, toFreeTypes[i], 0, __FILE__, __LINE__)) {
				Bcb *bcb = (Bcb *)m_pool->getInfo(toFreePages[i]);
				if (bcb->m_pageKey.m_file == file && (callback == NULL || callback(bcb->m_page, bcb->m_pageKey.m_pageId, indexId))) {
					freeBlock(session->getId(), bcb);
				} else
					m_pool->unlockPage(session->getId(), toFreePages[i], Exclusived);
			} else
				lockFail = true;
		}
		if (!lockFail)
			break;
		Thread::msleep(100);
	}

_RESTART:
	RWLOCK(&m_lock, Exclusived);
	FilePageList *pageList = m_filePageHash.get(file);
	if (pageList) {
		if (pageList->getPendingFlushCount() > 0) {
			// �ļ����ڵȴ���sync, �ȴ�һ��ʱ��֮������
			RWUNLOCK(&m_lock, Exclusived);
			Thread::msleep(100);
			goto _RESTART;
		}
		if (pageList->getBcbList()->getSize() == 0) {
			m_filePageHash.remove(file);
		} else {
			assert(callback);
		}
	}
	RWUNLOCK(&m_lock, Exclusived);
}

/**
 * д��������ҳ
 *
 * @param session �Ự������ΪNULL
 * @param targetTime ���������ָ����ʱ���֮ǰ��ɣ���λ��
 * @throw NtseException ������ȡ�����쳣��ΪNTSE_EC_CANCELED
 */
void Buffer::flushAll(Session *session) throw(NtseException) {
	ftrace(ts.buf, tout << session);
	flushDirtyPages(session, NULL, (u64)-1, (u64)-1, false);
}



/**
 * д��ָ����������ҳ
 *
 * @param session �Ự������ΪNULL
 * @param file ֻд��ָ���ļ�����ҳ
 * @param searchLength Ҫɨ���LRU������
 * @param maxPages Ҫд������ҳ������Ϊ(u64)-1��д��������ҳ
 * @param targetTime ���������ָ����ʱ���֮ǰ��ɣ���λ��
 * @param ignoreCancel �Ƿ���Բ�����ȡ����Ϣ����Ϊtrue���򱾺��������׳��쳣
 * @throw NtseException ������ȡ�����쳣��ΪNTSE_EC_CANCELED��ֻ��ignoreCancelΪfalse�ſ����׳��쳣
 */
void Buffer::flushDirtyPages(Session *session, File *file, u64 searchLength, u64 maxPages, bool ignoreCancel) throw(NtseException) {
	ftrace(ts.buf, tout << session << file << searchLength << maxPages);
	if (maxPages != (u64)-1 && m_allPages.getSize() == 0)
		return;

	bool flushAllPages = maxPages == (u64)-1;

	std::vector<KeyPagePair> dirtyPages;
	// Ҫ��֤д��������ҳ��������ס������ҳ
	// ˢд��ҳ�߳̿��������Ӳ���������ҳ
	bool skipLocked = !flushAllPages;

	// �ռ�����LRU�����ʱ��
	uint before = System::fastTime();

	RWLOCK(&m_lock, Shared);
	if (!file) {
		getDirtyPagesFromList(&dirtyPages, &m_oldGen, file, searchLength, maxPages, skipLocked);
		if (searchLength > m_oldGen.getSize() && dirtyPages.size() < maxPages)
			getDirtyPagesFromList(&dirtyPages, &m_newGen, file, searchLength - m_oldGen.getSize(), maxPages, skipLocked);
	} else {
		getDirtyPagesOfFile(&dirtyPages, file, searchLength, maxPages, skipLocked);
	}
	RWUNLOCK(&m_lock, Shared);

	// ��ȫ��flushʱ��Ҫͳ������lru�����ʱ��
	if (searchLength == (u64)-1) {
		m_status.m_flushAllBufferLockTime += (System::fastTime() - before);
		m_status.m_flushAllBufferLockCount++;
	}

	doFlushDirtyPages(session, file, &dirtyPages, ignoreCancel, flushAllPages);
}

/**
 * д��ָ������ҳ
 *
 * @param session �Ự������ΪNULL
 * @param file ֻд��ָ���ļ�����ҳ
 * @param dirtyPagesVec ��ҳ������
 * @param targetTime ���������ָ����ʱ���֮ǰ��ɣ���λ��
 * @param ignoreCancel �Ƿ���Բ�����ȡ����Ϣ����Ϊtrue���򱾺��������׳��쳣
 * @throw NtseException ������ȡ�����쳣��ΪNTSE_EC_CANCELED��ֻ��ignoreCancelΪfalse�ſ����׳��쳣
 */
void Buffer::doFlushDirtyPages(Session *session, File *file, vector<KeyPagePair> *dirtyPagesVec, 
							 bool ignoreCancel, bool flushAllPages) throw(NtseException) {
	vector<KeyPagePair> &dirtyPages(*dirtyPagesVec);
	uint batchSize = 16;
	size_t remain = dirtyPages.size(), done = 0, oldDone = 0, skipped = 0;
	u64 time = 0;

	// Ҫ��֤д��������ҳ��������ס������ҳ
	// ˢд��ҳ�߳̿��������Ӳ���������ҳ
	int timeout = flushAllPages ? -1 : UNSAFE_LOCK_TIMEOUT;
	// �Ƿ�ˢ��������
	bool flushWorld = file == NULL && flushAllPages;

	// ���ļ���ҳ�������д��ÿ����ҳ��������Ϊ���������
	u16 userId = session? session->getId(): 0;
	std::sort(dirtyPages.begin(), dirtyPages.end());

	u64 totalSleep =0;

	if(flushWorld)
#ifdef NTSE_UNIT_TEST
		batchSize = 200;
#else
		batchSize = getRecommandFlushSize();
#endif
	u64 batchWriteTime = System::currentTimeMillis();

	for (size_t i = 0; i < dirtyPages.size(); i++) {
		if (!ignoreCancel && session && session->isCanceled())
			NTSE_THROW(NTSE_EC_CANCELED, "flushDirtyPages has been canceled.");

		if (m_pool->lockPageIfType(userId, dirtyPages[i].m_page, Shared, dirtyPages[i].m_pageKey.m_pageType, timeout, __FILE__, __LINE__)) {
			Bcb *bcb = (Bcb *)m_pool->getInfo(dirtyPages[i].m_page);
			if (bcb->m_dirty) {
				u64 before = System::currentTimeMillis();
				writePage(session, bcb->m_page);
				time += System::currentTimeMillis() - before;

				if (flushAllPages)
					m_status.m_flushWrites++;
				else
					m_status.m_scavengerWrites++;
			} else
				skipped++;
			m_pool->unlockPage(userId, dirtyPages[i].m_page, Shared);
			remain--;
			done++;

			if (flushWorld
				&& (done - oldDone) >= batchSize	// д��һ����
				&& remain > 0		// ʣ�µĻ���
				&& m_db->getStat() != DB_CLOSING) {
				// ���м���ʱд��һ��ҳ�棬����sleepһ���Է�ֹ����Ӱ��ϵͳ����
				u64 now = System::currentTimeMillis();
				int sleepMillis = 1000 - (int)(now - batchWriteTime);
				if(sleepMillis > 0) {
					Thread::msleep(sleepMillis);
					batchWriteTime = now + sleepMillis;
				} else
					batchWriteTime = now;
				oldDone = done;
#ifdef NTSE_UNIT_TEST
				batchSize = 200;
#else
				batchSize = getRecommandFlushSize();
#endif
			}
		}
	}

	set<File *> fileSet; // ��ҳ��Ӧ���ļ�����
	if (file) {
		fileSet.insert(file);
	} else {
		for (size_t i = 0; i < dirtyPages.size(); i++) {
			if (fileSet.find(dirtyPages[i].m_pageKey.m_file) != fileSet.end())
				fileSet.insert(dirtyPages[i].m_pageKey.m_file);
		}
	}

	syncFiles(fileSet);

	if (flushAllPages) {
		m_syslog->log(EL_LOG, "Flush %s: dirty pages: %d, write time: "I64FORMAT"u, sleep: "I64FORMAT"u ms, skipped: %d.", 
			file? file->getPath(): "all", (uint)dirtyPages.size(), time, totalSleep, (int)skipped);
	}
}

/**
 * sync��directIo��ʽ�򿪵��ļ�
 *
 * @param fileSet ��Ҫsync���ļ�����
 */
void Buffer::syncFiles(set<File *> fileSet) {
	// ��Է�directIo�����򿪵��ļ��� �����ļ�ϵͳsync����
	// ����sync֮ǰ�������������ü����������ļ�����ɾ��
	for (set<File *>::iterator iter = fileSet.begin(); iter != fileSet.end(); ++iter) {
		File *file = *iter;
		RWLOCK(&m_lock, Exclusived);
		FilePageList *pageList = m_filePageHash.get(file);
		if (!pageList || file->isDirectIo()) {
			RWUNLOCK(&m_lock, Exclusived);
			continue;
		}
		pageList->incPendingFlushCount();
		RWUNLOCK(&m_lock, Exclusived);
		file->sync();
		RWLOCK(&m_lock, Exclusived);
		pageList = m_filePageHash.get(file);
		assert(pageList);
		pageList->decPendingFlushCount();
		RWUNLOCK(&m_lock, Exclusived);
	}
}

/**
 * ��ȡ�ļ���������ҳ��
 * @pre ȫ����������
 *
 * @param dirtyPages OUT�����������������ҳ��Ϣ
 * @param file ����Ȥ��ҳ���Ӧ���ļ�
 * @param searchLength ����list�������ô���ҳ��
 * @param maxPages ȡ�������ô�����ҳ
 * @param skipLocked �Ƿ�������������ҳ��
 */
void Buffer::getDirtyPagesOfFile(vector<KeyPagePair> *dirtyPages, File *file, u64 searchLength, u64 maxPages, bool skipLocked) {
	assert(file);
	
	RWLOCK(&m_lock, Shared);
	FilePageList *pageList = m_filePageHash.get(file);
	if (pageList) {
		assert(pageList->getFile() == file);
		DList<Bcb *> *list = pageList->getBcbList();
		DLink<Bcb *> *curr;
		for (curr = list->getHeader()->getNext(); curr != list->getHeader() && searchLength > 0; curr = curr->getNext()) {
			searchLength--;
			Bcb *bcb = curr->get();
			assert(bcb->m_pageKey.m_file == file);
			if (!bcb->m_dirty) {
				continue;
			}
			//�������ҳ
			if (skipLocked && m_pool->isPageLocked(bcb->m_page, Exclusived))
				continue;
			dirtyPages->push_back(KeyPagePair(bcb->m_pageKey, bcb->m_page));
			if (dirtyPages->size() >= maxPages)
				break;
		}
	}
	RWUNLOCK(&m_lock, Shared);
}


#ifndef WIN32
/**
 * ���첽IOд��������ҳ
 *
 * @param session �Ự������ΪNULL
 * @param targetTime ���������ָ����ʱ���֮ǰ��ɣ���λ��
 * @param array	�첽IO�������
 * @throw NtseException ������ȡ�����쳣��ΪNTSE_EC_CANCELED
 */
void Buffer::flushAllUseAio(Session *session, AioArray *array) throw(NtseException) {
	ftrace(ts.buf, tout << session);
	flushDirtyPagesUseAio(session, NULL, (u64)-1, (u64)-1, false, array);
}



/**
 * ���첽IOд��ָ����������ҳ
 *
 * @param session �Ự������ΪNULL
 * @param file ֻд��ָ���ļ�����ҳ
 * @param searchLength Ҫɨ���LRU������
 * @param maxPages Ҫд������ҳ������Ϊ(u64)-1��д��������ҳ
 * @param targetTime ���������ָ����ʱ���֮ǰ��ɣ���λ��
 * @param ignoreCancel �Ƿ���Բ�����ȡ����Ϣ����Ϊtrue���򱾺��������׳��쳣
 * @throw NtseException ������ȡ�����쳣��ΪNTSE_EC_CANCELED��ֻ��ignoreCancelΪfalse�ſ����׳��쳣
 */
void Buffer::flushDirtyPagesUseAio(Session *session, File *file, u64 searchLength, u64 maxPages, bool ignoreCancel, AioArray* array) throw(NtseException) {
	ftrace(ts.buf, tout << session << file << searchLength << maxPages);
	if (maxPages != (u64)-1 && m_allPages.getSize() == 0)
		return;

	bool flushAllPages = maxPages == (u64)-1;
	bool flushWorld = file == NULL && flushAllPages;
	uint batchSize = 0;
	bool scavengerFlush = !flushWorld;
	
	std::vector<KeyPagePair> dirtyPages;

	int timeout;
	bool skipLocked;
	if (flushAllPages) {
		// Ҫ��֤д��������ҳ��������ס������ҳ
		timeout = -1;
		skipLocked = false;
	} else {
		// ˢд��ҳ�߳̿��������Ӳ���������ҳ
		timeout = UNSAFE_LOCK_TIMEOUT;
		skipLocked = true;
	}

	// �ռ�����LRU�����ʱ��
	uint before = System::fastTime();

	RWLOCK(&m_lock, Shared);
	getDirtyPagesFromList(&dirtyPages, &m_oldGen, file, searchLength, maxPages, skipLocked);
	if (searchLength > m_oldGen.getSize() && dirtyPages.size() < maxPages)
		getDirtyPagesFromList(&dirtyPages, &m_newGen, file, searchLength - m_oldGen.getSize(), maxPages, skipLocked);
	RWUNLOCK(&m_lock, Shared);

	// ��ȫ��flushʱ��Ҫͳ������lru�����ʱ��
	if (searchLength == (u64)-1) {
		m_status.m_flushAllBufferLockTime += (System::fastTime() - before);
		m_status.m_flushAllBufferLockCount++;
	}

	size_t remain = dirtyPages.size(), done = 0, oldDone = 0, skipped = 0;
	u64 time = 0;

	// ���ļ���ҳ�������д��ÿ����ҳ��������Ϊ���������
	u16 userId = session? session->getId(): 0;
	std::sort(dirtyPages.begin(), dirtyPages.end());

	u64 totalSleep =0;
	if(flushWorld)
		batchSize = getRecommandFlushSize();
	u64 batchWriteTime = System::currentTimeMillis();
	
	for (size_t i = 0; i < dirtyPages.size(); i++) {
		if (!ignoreCancel && session && session->isCanceled())
			NTSE_THROW(NTSE_EC_CANCELED, "flushDirtyPages has been canceled.");
		timeout = 0;
	
		// ����첽io��������,�ȴ������첽IO���
		if (array->getReservedSlotNum() == AioArray::AIO_BATCH_SIZE) {
			waitWritePageUseAioComplete(session, array, false, scavengerFlush);
		}
__Retry:
		// ���Լ�ҳ��latch����ʱ������ͬ���첽IO
		if (m_pool->lockPageIfType(userId, dirtyPages[i].m_page, Shared, dirtyPages[i].m_pageKey.m_pageType, timeout, __FILE__, __LINE__)) {
			Bcb *bcb = (Bcb *)m_pool->getInfo(dirtyPages[i].m_page);
			if (bcb->m_dirty) {
				u64 before = System::currentTimeMillis();	
			
				if(!writePageUseAio(session, bcb->m_page, array)) {
					m_pool->unlockPage(userId, dirtyPages[i].m_page, Shared);
					continue;
				}
				
				time += System::currentTimeMillis() - before;

				if (flushAllPages)
					m_status.m_flushWrites++;
				else
					m_status.m_scavengerWrites++;
				// ͳ����ʵд����ҳ��
				done++;
			} else {
				skipped++;
				m_pool->unlockPage(userId, dirtyPages[i].m_page, Shared);
			}
			remain--;
 
			//������Ҫ��ʱ���ߣ���֤ϵͳ�������ȶ�
			if (flushWorld
				&& (done - oldDone) >= batchSize	// д��һ����
				&& remain > 0		// ʣ�µĻ���
				&& m_db->getStat() != DB_CLOSING) {
					// ���ȵȴ������첽IO��������������
					waitWritePageUseAioComplete(session, array, true, scavengerFlush);
					// ��֤ÿ����ҳ��ʽд��
					u64 now = System::currentTimeMillis();
					int sleepMillis = 1000 - (int)(now - batchWriteTime);
					if(sleepMillis > 0) {
						Thread::msleep(sleepMillis);
						batchWriteTime = now + sleepMillis;
					} else
						batchWriteTime = now;
					oldDone = done;
					batchSize = getRecommandFlushSize();
				//	loopIter++;
			}

		} else if (timeout == 0){
			// �������trylatchʧ����ͬ��֮ǰ���е��첽IO�����ͷ�����latch����ֹlatch����
			waitWritePageUseAioComplete(session, array, true, scavengerFlush);
			NTSE_ASSERT(array->getReservedSlotNum() == 0);
			// ��ʱ�Ѿ��ͷ�����ҳ���latch������´�ȥȡҳ��latch����try
			timeout = flushAllPages? -1: UNSAFE_LOCK_TIMEOUT;

			goto __Retry;
		}
	}
	// �첽IO��ʱҪ�ȴ�ȫ��IO���󷵻ز����ͷ�ҳ��latch
	waitWritePageUseAioComplete(session, array, true, scavengerFlush);
	NTSE_ASSERT(array->getReservedSlotNum() == 0);
		
	if (flushAllPages) {
		m_syslog->log(EL_LOG, "Flush %s: dirty pages: %d, write time: "I64FORMAT"u, sleep: "I64FORMAT"u ms, skipped: %d.", 
			file? file->getPath(): "all", (uint)dirtyPages.size(), time, totalSleep, (int)skipped);
	}
}

/**
 * ��һ��ҳ���첽IO��ʽд�����ļ���
 * @pre ҳ���Ѿ�������
 *
 * @param session �Ự������ΪNULL
 * @param page ҳ���ַ
 * @param array �첽IO����
 */
bool Buffer::writePageUseAio(Session *session, BufferPageHdr *page, AioArray *array) {
	ftrace(ts.buf, tout << session << (Bcb *)m_pool->getInfo(page));
	assert(m_pool->isPageLocked(page, Shared) || m_pool->isPageLocked(page, Exclusived));

	Bcb *bcb = (Bcb *)m_pool->getInfo(page);

	m_status.m_pendingWrites.increment();

	LOCK(&bcb->m_metaLock);
	if (bcb->m_isWriting) {
		UNLOCK(&bcb->m_metaLock);	
		return false;
	}
	bcb->m_isWriting = true;
	UNLOCK(&bcb->m_metaLock);	
	assert(array->getReservedSlotNum() < AioArray::AIO_BATCH_SIZE);
	AioSlot *slot = array->aioReserveSlot(AIO_WRITE, bcb->m_pageKey.m_file, bcb->m_page, 
		bcb->m_pageKey.m_pageId * m_pool->getPageSize(),m_pool->getPageSize(), bcb);

	
	if (m_txnlog && page->m_lsn != INVALID_LSN)	// WAL
		m_txnlog->flush(page->m_lsn, FS_SINGLE_WRITE);

	updateChecksum(bcb->m_page);

	u64 before = System::currentTimeMillis();
	
	u64 errCode = array->aioDispatch(slot);
	m_status.m_writeTime += System::currentTimeMillis() - before;
	if (errCode != File::E_NO_ERROR) {
		m_syslog->fopPanic(errCode, "AIO Write Failed");
	}

	m_status.m_physicalWrites++;
	if (session)
		session->incOpStat(OPS_PHY_WRITE);
	bcb->m_dbObjStats->countIt(DBOBJ_PHY_WRITE);

	m_status.m_pendingWrites.decrement();
	return true;
}


void Buffer::waitWritePageUseAioComplete(Session *session, AioArray* array, bool waitAll, bool freePage) {
	u32 numIoComplete = 0;
// 	u64 code1 = array->aioDispatchGroup(array->getReservedSlotNum());
// 	if (code1 != File::E_NO_ERROR)
// 		m_syslog->fopPanic(code1, "AIO Write failed");
	u32 minRequestNumber = waitAll? array->getReservedSlotNum(): 1;
/*	u32 minRequestNumber = array->getReservedSlotNum();*/
	u64 code = array->aioWaitFinish(minRequestNumber, &numIoComplete);
	if (code == File::E_NO_ERROR) {
		if (waitAll)
			NTSE_ASSERT(numIoComplete == array->getReservedSlotNum());
		for (u32 i = 0; i < numIoComplete; i++) {
			AioSlot *slot = array->getSlotFromEvent(i);
			void    *page = array->getDataFromEvent(i);
			Bcb		*bcb = (Bcb *)m_pool->getInfo(page);
			NTSE_ASSERT(slot->m_buffer == page);
			NTSE_ASSERT(slot->m_data == bcb);
			LOCK(&bcb->m_metaLock);
			bcb->m_isWriting = false;
			if (bcb->m_dirty == true)
				m_status.m_curDirtyPages.decrement();
			bcb->m_dirty = false;
			UNLOCK(&bcb->m_metaLock);
			array->aioFreeSlot(slot);
			u16 userId = session? session->getId(): 0;
/*			// �����������̣߳��ǳ��Խ�ҳ��Ż�Buffer��FreeList��
			// TODO:���Ż������б����в��ȶ����ݲ�����
			if (freePage) {
				if (m_pool->tryUpgradePageLock(userId, page, __FILE__, __LINE__)) {
					LOCK(&bcb->m_metaLock);
					if (bcb->m_dirty || bcb->m_pinCount > 0){
						UNLOCK(&bcb->m_metaLock);
						m_pool->unlockPage(userId, page, Exclusived);
						continue;
					}
					UNLOCK(&bcb->m_metaLock);
					// ��Xlatch������ҳ��ֻ��unpin����˴˴��ͷ�ҳ���ǰ�ȫ��
					freeBlock(userId, bcb);
				} else {
					m_pool->unlockPage(userId, page, Shared);
				}
			} else {
				m_pool->unlockPage(userId, page, Shared);
			}
*/
			m_pool->unlockPage(userId, page, Shared);

		}
	} else {
		m_syslog->fopPanic(code, "AIO Write failed");
	}
}
#endif

/**
 * ��ȡ��һ�������Ҫˢ��ҳ������
 */
uint Buffer::getRecommandFlushSize() {
	uint pagesNeedFlush = 0;
	u64 curNtseLogSize = m_db->getTxnlog()->getStatus().m_ntseLogSize;

	uint  curDirtyPages = m_status.m_curDirtyPages.get();
	uint systemIoCapacity = m_db->getConfig()->m_systemIoCapacity;
	uint flushLoop = m_db->getConfig()->m_flushAdjustLoop;
	// 1. �õ�֮ǰһ��ʱ���ˢ��ҳ������ƽ��ֵ����־��������ƽ��ֵ
	if (m_flushStatus.m_iter > flushLoop) {
		// ˢ��ҳƽ������
		m_flushStatus.m_avgPageRate = (m_flushStatus.m_loopWritePageCnt / flushLoop + 
			m_flushStatus.m_avgPageRate) / 2;
		m_flushStatus.m_loopWritePageCnt = 0;
		
		// ntse��־����ƽ������
		m_flushStatus.m_avgNtseLogRate = (curNtseLogSize - m_flushStatus.m_prevLoopNtseLogSize) / flushLoop;
		m_flushStatus.m_prevLoopNtseLogSize = m_db->getTxnlog()->getStatus().m_ntseLogSize;

		// ��ҳ������ƽ������
		m_flushStatus.m_avgDirtyPagesRate = (curDirtyPages - m_flushStatus.m_prevLoopDirtyPages) / flushLoop;
		m_flushStatus.m_prevLoopDirtyPages = curDirtyPages;
		
		m_flushStatus.m_iter = 0;
	}

	// 2. ͳ���ڴ�buffer��ҳ����
	uint pctDirtyPages = 0;
	uint bufferSize = m_db->getConfig()->m_pageBufSize;

	uint curDirtyPagePct = (curDirtyPages * 100) / bufferSize;
	uint maxDirtyPagePct = m_db->getConfig()->m_maxDirtyPagePct;
	uint maxDirtyPagePctLwm = m_db->getConfig()->m_maxDirtyPagePctLwm;
	
	if (maxDirtyPagePctLwm == 0) {
		// ���δ������ҳ���ʵ�ˮλ��
		if (curDirtyPagePct > maxDirtyPagePct)
			pctDirtyPages = 100;
	} else if (curDirtyPagePct > maxDirtyPagePctLwm){
		pctDirtyPages = curDirtyPagePct * 100 / maxDirtyPagePct;
	}
	pagesNeedFlush = (uint)(systemIoCapacity * (double)(pctDirtyPages) / 100.0);

	// 3. ��ȡ��ǰ����ҳ������
	int deltaSecDirtyPages = curDirtyPages - m_flushStatus.m_prevSecDirtyPages;
	deltaSecDirtyPages = (deltaSecDirtyPages + m_flushStatus.m_avgDirtyPagesRate) / 2;

	m_flushStatus.m_prevSecDirtyPages = curDirtyPages;
	//	����������Ӧˢҳ�����������2����õ�Ӧˢҳ����ȡ�ϴ�ֵ
	int modifyFlushPages = m_flushStatus.m_prevSecFlushPageCnt + deltaSecDirtyPages;
	if (modifyFlushPages < 0)
		modifyFlushPages = 0;
	pagesNeedFlush = max(pagesNeedFlush, (uint)modifyFlushPages);


	// 4. ȡˢ��ҳƽ�����ʺ��ڴ���ҳ�ȵ�ƽ��ֵ���������ϵͳIO�����Ľ�Сֵ
	
	pagesNeedFlush = (pagesNeedFlush + m_flushStatus.m_avgPageRate) / 2;
	if (pagesNeedFlush > systemIoCapacity)
		pagesNeedFlush = systemIoCapacity;

	// 5. Ϊ��֤�ƽ����㣬����������Ҫ��1/10��ϵͳIOˢ��ҳ
	uint minimunFlushPages = systemIoCapacity / 10;
	if (pagesNeedFlush < minimunFlushPages)
		pagesNeedFlush = minimunFlushPages;

	m_flushStatus.m_prevSecFlushPageCnt = pagesNeedFlush;
	m_flushStatus.m_loopWritePageCnt += pagesNeedFlush;
	m_flushStatus.m_iter++;
	
	

	m_syslog->log(EL_LOG, "flush dirty pages: %d", pagesNeedFlush);
	return pagesNeedFlush;
}

/**
 * ���ü���ˢ��ҳ��ͳ����Ϣ
 */
void Buffer::resetFlushStatus() {
	memset(&m_flushStatus, 0, sizeof(BufferFlushStatus));
}

/**
 * ��ָ��ҳ���б���ȡ��������������ҳ
 * @pre ȫ����������
 *
 * @param dirtyPages OUT�����������������ҳ��Ϣ
 * @param list ҳ���б�
 * @param file ����Ȥ��ҳ���Ӧ���ļ�����ΪNULL�򲻿����ļ�
 * @param searchLength ����list�������ô���ҳ��
 * @param maxPages ȡ�������ô�����ҳ
 * @param skipLocked �Ƿ�������������ҳ��
 */
void Buffer::getDirtyPagesFromList(std::vector<KeyPagePair> *dirtyPages, DList<Bcb *> *list, File *file, u64 searchLength, u64 maxPages, bool skipLocked) {
	assert(m_lock.isLocked(Shared));
	DLink<Bcb *> *curr;
	for (curr = list->getHeader()->getNext();
		curr != list->getHeader() && searchLength > 0; curr = curr->getNext()) {
		searchLength--;
		Bcb *bcb = curr->get();
		if (bcb->m_dirty && (file == NULL || bcb->m_pageKey.m_file == file)) {
			if (skipLocked && m_pool->isPageLocked(bcb->m_page, Exclusived))
				continue;
			dirtyPages->push_back(KeyPagePair(bcb->m_pageKey, bcb->m_page));
			if (dirtyPages->size() >= maxPages)
				return;
		}
	}
}

/**
 * ��ָ��ҳ���б���ȡ������ҳ����δ��pin�ҷ����ҳ
 * @pre ȫ����������
 *
 * @param freePages OUT����ſ���ҳ
 * @param list ҳ���б�
 * @param maxPages ȡ�������ô���ҳ
 */
void Buffer::getFreePagesFromList(std::vector<KeyPagePair> *freePages, DList<Bcb *> *list, u64 maxPages) {
	assert(m_lock.isLocked(Shared));
	DLink<Bcb *> *curr;
	for (curr = list->getHeader()->getNext();
		curr != list->getHeader(); curr = curr->getNext()) {
		Bcb *bcb = curr->get();
		// ������ʱȫ�ֽṹ���Ѿ�������������Bcb�����ǰ�ȫ��
		// ����û�м�BcbԪ�����������ܵõ�����Ϣ�������µģ�
		// ����Ҫ�ͷ�֮ǰ��������жϣ��������
		if (!bcb->m_dirty && bcb->m_pinCount == 0) {
			freePages->push_back(KeyPagePair(bcb->m_pageKey, bcb->m_page));
			if (freePages->size() >= maxPages)
				return;
		}
	}
}

/**
 * ҳ�滺���Ƿ�ʹ��У��͸�����֤ҳ�����ݵ�һ���ԡ�
 * ҳ�滺��Ĭ��ʹ��У��ͣ���ʹ��У���ʱ���������ģ����д��ҳ��ʱ
 * ���Զ�����ҳ�����ݼ���У���д���������У�ͬʱ�ڴӴ��̶���ҳ��
 * ʱ��ʹ�ü���������ҳ�������Ƿ���ȷ��
 *
 * @return �Ƿ�ʹ��У���
 */
bool Buffer::isChecksumed() {
	return m_checksumed;
}

/**
 * ����ҳ�滺���Ƿ�ʹ��У���
 *
 * @param checksumed �Ƿ�ʹ�ü����
 */
void Buffer::setChecksumed(bool checksumed) {
	ftrace(ts.buf, tout << checksumed);
	m_checksumed = checksumed;
}

/**
 * ����Ԥ������С����λΪ�ֽ���
 *
 * @return Ԥ������С
 */
uint Buffer::getPrefetchSize() {
	return m_prefetchSize;
}

/**
 * ����Ԥ������С
 *
 * @param prefetchSize Ԥ������С����ҳ��С֮�̱���Ϊ2����������
 */
void Buffer::setPrefetchSize(uint prefetchSize) {
	ftrace(ts.buf, tout << prefetchSize);
	uint n = prefetchSize / m_pool->getPageSize();
	while (n >= 2) {
		if ((n % 2 ) != 0)
			return;
		n /= 2;
	}
	m_prefetchSize = prefetchSize;
	m_prefetchPages = m_prefetchSize / m_pool->getPageSize();
}

/**
 * ���غ�̨д��ҳ�̴߳���ʱ����
 *
 * @return ��̨д��ҳ�̴߳���ʱ��������λ����
 */
uint Buffer::getScavengerInterval() {
	return m_scavenger->getInterval();
}

/**
 * ���ú�̨д��ҳ�̴߳���ʱ����
 *
 * @param interval ��̨д��ҳ�̴߳���ʱ��������λ����
 */
void Buffer::setScavengerInterval(uint interval) {
	ftrace(ts.buf, tout << interval);
	m_scavenger->setInterval(interval);
}

/**
 * ���غ�̨д��ҳ�߳�ÿ�δ���ʱд������ҳռ�����С��������
 *
 * @return ��̨д��ҳ�߳�ÿ�δ���ʱд������ҳռ�����С��������
 */
double Buffer::getScavengerMaxPagesRatio() {
	return  m_scavenger->getMaxPagesRatio();
}

/**
 * ���ú�̨д��ҳ�߳�ÿ�δ���ʱд������ҳռ�����С��������
 *
 * @param ��̨д��ҳ�߳�ÿ�δ���ʱд������ҳռ�����С����������������[0, 1]֮��
 */
void Buffer::setScavengerMaxPagesRatio(double maxPagesRatio) {
	ftrace(ts.buf, tout << maxPagesRatio);
	m_scavenger->setMaxPagesRatio(maxPagesRatio);
}

/**
 * ���غ�̨д��ҳ�߳�ÿ�δ���ʱɨ���ҳ�������ϴδ������������ж���ҳ�����ı���
 *
 * @return ��̨д��ҳ�߳�ÿ�δ���ʱɨ���ҳ�������ϴδ������������ж���ҳ�����ı���
 */
double Buffer::getScavengerMultiplier() {
	return m_scavenger->getMultiplier();
}

/**
 * ���ú�̨д��ҳ�߳�ÿ�δ���ʱɨ���ҳ�������ϴδ������������ж���ҳ�����ı���
 *
 * @param ��̨д��ҳ�߳�ÿ�δ���ʱɨ���ҳ�������ϴδ������������ж���ҳ�����ı����������ֵӦ��>1
 */
void Buffer::setScavengerMultiplier(double multiplier) {
	ftrace(ts.buf, tout << multiplier);
	m_scavenger->setMultiplier(multiplier);
}

/**
 * ���غ�̨д��ҳ�̣߳�ÿ��д�������ҳ������
 *
 */
uint Buffer::getMaxScavengerPages() {
	return m_scavenger->getMaxScavengerPages();
}

/**
 * ���ú�̨д��ҳ�����̣߳�ÿ��д����ҳ���������
 *
 */
void Buffer::setMaxScavengerPages(uint maxScavengerPages) {
	m_scavenger->setMaxScavengerPages(maxScavengerPages);
}

/**
 * ���ش�������Ԥ����ֵ��������ļ�ͷ˵��
 *
 * @return ��������Ԥ����ֵ
 */
double Buffer::getPrefetchRatio() {
	return m_prefetchRatio;
}

/**
 * ���ô�������Ԥ����ֵ��������ļ�ͷ˵��
 *
 * @param ��������Ԥ����ֵ
 */
void Buffer::setPrefetchRatio(double prefetchRatio) {
	ftrace(ts.buf, tout << prefetchRatio);
	m_prefetchRatio = prefetchRatio;
}

/**
 * ����ҳ�滺�����չͳ����Ϣ
 */
void Buffer::updateExtendStatus() {
	PROFILE(PI_Buffer_updateExtendStatus);

	memset(&m_status.m_statusEx, 0, sizeof(BufferStatusEx));

	RWLOCK(&m_lock, Shared);
	DLink<Bcb *> *curr;
	for (curr = m_allPages.getHeader()->getNext();
		curr != m_allPages.getHeader(); curr = curr->getNext()) {
		Bcb *bcb = curr->get();
		if (bcb->m_dirty)
			m_status.m_statusEx.m_dirtyPages++;
		if (bcb->m_pinCount > 0)
			m_status.m_statusEx.m_pinnedPages++;
		if (m_pool->isPageLocked(bcb->m_page, Shared))
			m_status.m_statusEx.m_rlockedPages++;
		else if (m_pool->isPageLocked(bcb->m_page, Exclusived))
			m_status.m_statusEx.m_wlockedPages++;
	}
	size_t maxHashConflict;
	m_pageHash.getConflictStatus(&m_status.m_statusEx.m_avgHashConflict, &maxHashConflict);
	m_status.m_statusEx.m_maxHashConflict = maxHashConflict;
	RWUNLOCK(&m_lock, Shared);
}

/**
 * ���ҳ�滺������״̬
 * ע: ��Щ����״̬û�н���ͬ�������ܲ���ȷ
 * ע: �����������Զ�������չͳ����Ϣ����Ҫ����updateExtendStatus������
 *
 * @return ҳ�滺������״̬
 */
const BufferStatus& Buffer::getStatus() {
	m_status.m_globalLockUsage = m_lock.getUsage();
	m_status.m_realScavengeCnt = m_scavenger->getRealScavengeCnt();
	return m_status;
}

/** ��ӡҳ�滺������״̬
 * ע: �����������Զ�������չͳ����Ϣ����Ҫ����updateExtendStatus������
 * @param out �����
 */
void Buffer::printStatus(ostream& out) const {
	out << "== page buffer pool ===========================================================" << endl;
	out << "page size: " << m_pool->getPageSize() << ", target size: " << getTargetSize();
	out << ", current size: " << getCurrentSize() << endl;
	out << "logical reads: " << m_status.m_logicalReads << ", physical reads: " << m_status.m_physicalReads << endl;
	out << "logical writes: " << m_status.m_logicalWrites << ", physical writes: " << m_status.m_physicalWrites << endl;
	out << "read time: " << m_status.m_readTime << ", write time: " << m_status.m_writeTime << endl;
	out << "pending reads: " << m_status.m_pendingReads.get() << "pending writes: " << m_status.m_pendingWrites.get() << endl;
	out << "scavenger writes: " << m_status.m_scavengerWrites << ", flush writes: " << m_status.m_flushWrites << endl;
	out << "prefetches: " << m_status.m_prefetches << ", batch prefetches: " << m_status.m_batchPrefetch;
	out << ", non-batch prefetches: " << m_status.m_nonbatchPrefetch << ", prefetch pages: " << m_status.m_prefetchPages << endl;
	out << "page creates: " << m_status.m_pageCreates << endl;
	out << "alloc block fails: " << m_status.m_allocBlockFail << endl;
	out << "replace searches: " << m_status.m_replaceSearches << ", replace search length: " << m_status.m_replaceSearchLen << endl;
	out << "dirty pages: " << m_status.m_statusEx.m_dirtyPages << ", pinned pages: " << m_status.m_statusEx.m_pinnedPages << endl;
	out << "Shared locked pages: " << m_status.m_statusEx.m_rlockedPages << ", Exclusived locked pages: " << m_status.m_statusEx.m_wlockedPages << endl;
	out << "average length of conflict lists of page hash: " << m_status.m_statusEx.m_avgHashConflict << endl;
	out << "max length of conflict lists of page hash: " << m_status.m_statusEx.m_maxHashConflict << endl;
	out << "=== global lock ===" << endl;
	m_lock.getUsage()->print(out);
}

/** ҳ�滺��ɨ���� */
struct BufScanHandle {
	PoolScanHandle	*m_poolScan;	/** �ڴ�ҳ��ɨ���� */
	File			*m_file;		/** ֻ���ر��ļ��Ļ���ҳ����ΪNULL��������ҳ */

	BufScanHandle(PoolScanHandle *poolScan, File *file) {
		m_poolScan = poolScan;
		m_file = file;
	}
};

/**
 * ��ʼ���������е�ҳ�棬ע�����ڱ���ʱ����������ȫ�ֽṹ������������ܲ�̫��ȷ
 *
 * @param userId �û�ID
 * @param file ֻ��������ָ���ļ��Ļ���ҳ����ΪNULL�򷵻����л���ҳ
 * @return ɨ����
 */
BufScanHandle* Buffer::beginScan(u16 userId, File *file) {
	return new BufScanHandle(m_pool->beginScan(this, userId), file);
}

/**
 * �õ���һ������ҳ
 * @post ���ص�ҳ�Ѿ��ù������������ϴη��ص�ҳ���ϵ����Ѿ����ͷ�
 *
 * @param h ɨ����
 * @return ������ƿ飬��û���򷵻�NULL
 */
const Bcb* Buffer::getNext(BufScanHandle *h) {
	while (true) {
		void *p = m_pool->getNext(h->m_poolScan);
		if (!p)
			return NULL;
		Bcb *bcb = (Bcb *)m_pool->getInfo(p);
		if (!h->m_file || h->m_file == bcb->m_pageKey.m_file)
			return bcb;
	}
}

/** �ͷŵ�ǰҳ
 *
 * @param h ɨ����
 */
void Buffer::releaseCurrent(BufScanHandle *h) {
	m_pool->releaseCurrent(h->m_poolScan);
}

/**
 * ����ɨ�裬����ǰҳ�治ΪNULL���Զ�����
 * @post ɨ���������Ѿ���delete
 *
 * @param h ɨ����
 */
void Buffer::endScan(BufScanHandle *h) {
	m_pool->endScan(h->m_poolScan);
	delete h;
}

/**
 * ���Է���һ���顣
 * @pre �������Ѿ�����ȫ�ֽṹ������
 * @post �ɹ�ʱ���ص�ҳ�Ѿ����û���������
 *
 * @param userId �û�ID
 * @param file �ļ�
 * @param pageType ҳ������
 * @param pageId ҳ��
 * @return �ɹ�ʱ���ؿ��ƿ飬ʧ�ܷ���NULL
 */
Bcb* Buffer::tryAllocBlock(u16 userId, File *file, PageType pageType, u64 pageId) {
	ftrace(ts.buf, tout << userId << file << pageType << pageId);
	assert(m_lock.isLocked(Exclusived));

	size_t poolEntry = m_bcbPool.alloc();
	Bcb *bcb = &m_bcbPool[poolEntry];

	void *page = allocPage(userId, pageType, bcb);
	if (page) {
		bcb->m_page = (BufferPageHdr *)page;
		bcb->m_poolEntry = poolEntry;
	} else {
		m_bcbPool.free(poolEntry);
		bcb = NULL;
	}
	if (!bcb)
		bcb = replaceInList(userId, &m_oldGen);
	if (!bcb)
		bcb = replaceInList(userId, &m_newGen);
	if (bcb) {
		assert(m_pool->isPageLocked(bcb->m_page, Exclusived));
		memset(bcb->m_page, 0, m_pool->getPageSize());

		m_pool->setInfoAndType(bcb->m_page, bcb, pageType);

		bcb->init(file, pageType, pageId);
		bcb->m_lruSeq = m_readSeq;

		m_oldGen.addLast(&bcb->m_lruLink);
		m_allPages.addFirst(&bcb->m_link);
		m_pageHash.put(bcb);
		// �ļ���Ӧ������ҳ������һ��
		FilePageList *pageList = m_filePageHash.get(file);
		if (!pageList) {
			pageList = &m_filePageListPool[m_filePageListPool.alloc()];
			pageList->setFile(file);
			m_filePageHash.put(pageList);
		}
		pageList->add(bcb);

		m_status.m_pageCreates++;
	} else {
		m_status.m_allocBlockFail++;
	}
	assert(!bcb || m_pool->isPageLocked(bcb->m_page, Exclusived));
	return bcb;
}

/**
 * ���䵽һ��Bcb֮����ж�ȡ���ݵȲ���
 * @pre Bcb��Ӧ��ҳ���Ѿ��û���������
 *
 * @param session �Ự������ΪNULL
 * @param readFromFile �Ƿ��ȡҳ������
 * @param lockMode ��ģʽ
 */
void Buffer::postAlloc(Session *session, Bcb *bcb, bool readFromFile, LockMode lockMode) {
	ftrace(ts.buf, tout << session << bcb << readFromFile << lockMode);
	assert(m_pool->isPageLocked(bcb->m_page, Exclusived));
	if (readFromFile)
		readBlock(session, bcb, false);
	else
		bcb->m_readSeq = ++m_readSeq;
	pinBlock(session->getId(), bcb, true);
	if (lockMode == Shared) {
		// ����ҳ���Ѿ���pinס�����õ�����ʧ
		assert(bcb->m_pinCount > 0);
		m_pool->unlockPage(session->getId(), bcb->m_page, Exclusived);
		m_pool->lockPage(session->getId(), bcb->m_page, Shared, __FILE__, __LINE__);
	}
	assert(m_pool->isPageLocked(bcb->m_page, lockMode));
}

/**
 * ����ָ���Ŀ��ƿ������滻����ĳ��pin����Ϊ0�ҷ����ҳ��
 * @pre �Ѿ�����ȫ�ֽṹ������
 * @post ���滻�ɹ������滻��ҳ�Ѿ����˻������������Ѿ�������ȫ�ֽṹ��ɾ��
 *
 * @param userId �û�ID
 * @param list ���ƿ�����
 * @return �滻�ɹ����ر��滻ҳ�Ŀ��ƿ飬�Ҳ��������滻��ҳ����NULL
 */
Bcb* Buffer::replaceInList(u16 userId, DList<Bcb *> *list) {
	assert(m_lock.isLocked(Exclusived));

	u64		searchLen = 0;
	bool	alreadySignaled = false;

	m_status.m_replaceSearches++;

	DLink<Bcb *> *e;
	for (e = list->getHeader()->getNext(); e != list->getHeader(); e = e->getNext()) {
		
		searchLen++;
		// ��ǰsearch length����200������Scavenger�߳�
		if (searchLen > SCAVENGER_SIGNALED_AFTER_LRU_SEARCH_LEN && !alreadySignaled) {
			alreadySignaled = true;
		}

		m_status.m_replaceSearchLen++;
		Bcb *bcb = e->get();
		if (bcb->m_pinCount == 0 && !bcb->m_dirty && m_pool->trylockPage(userId, bcb->m_page, Exclusived, __FILE__, __LINE__)) {
			LOCK(&bcb->m_metaLock);
			if (bcb->m_pinCount == 0 && !bcb->m_dirty) {
				bcb->m_link.unLink();
				bcb->m_lruLink.unLink();
				bcb->m_fileLink.unLink();
				m_pageHash.remove(&bcb->m_pageKey);
				if (bcb->m_dbObjStats && bcb->m_dbObjStats->m_bufInternal)
					delete bcb->m_dbObjStats;
				bcb->m_dbObjStats = NULL;
				UNLOCK(&bcb->m_metaLock);
				return bcb;
			} else {
				UNLOCK(&bcb->m_metaLock);
				m_pool->unlockPage(userId, bcb->m_page, Exclusived);
			}
		}
	}
	return NULL;
}

/**
 * ��һ��ҳ��ӻ�����ɾ��
 * @pre ҳ���Ѿ��û���������
 * @pre ҳ�治�ܱ�pinס
 *
 * @param userId �û�ID
 * @param bcb ҳ����ƿ�
 */
void Buffer::freeBlock(u16 userId, Bcb *bcb) {
	ftrace(ts.buf, tout << userId << bcb);
	assert(m_pool->isPageLocked(bcb->m_page, Exclusived));
	assert(bcb->m_pinCount == 0);

	LOCK(&bcb->m_metaLock);
	if (bcb->m_dbObjStats && bcb->m_dbObjStats->m_bufInternal)
		delete bcb->m_dbObjStats;
	bcb->m_dbObjStats = NULL;
	UNLOCK(&bcb->m_metaLock);

	RWLOCK(&m_lock, Exclusived);
	bcb->m_lruLink.unLink();
	bcb->m_link.unLink();
	bcb->m_fileLink.unLink();
	m_pageHash.remove(&bcb->m_pageKey);
	freePage(userId, bcb->m_page);
	bcb->m_page = NULL;
	m_bcbPool.free(bcb->m_poolEntry);
	RWUNLOCK(&m_lock, Exclusived);
}

/**
 * ��ȡһ��ҳ������
 * @pre ҳ���Ѿ����û���������
 *
 * @param session �Ự��������ΪNULL
 * @param bcb ���ƿ�
 * @param prefetch �Ƿ���Ԥ��
 */
void Buffer::readBlock(Session *session, Bcb *bcb, bool prefetch) {
	ftrace(ts.buf, tout << session << bcb << prefetch);
	assert(m_pool->isPageLocked(bcb->m_page, Exclusived));

	m_status.m_pendingReads.increment();
	u64 before = System::currentTimeMillis();

	u64 errCode = bcb->m_pageKey.m_file->read(bcb->m_pageKey.m_pageId * m_pool->getPageSize(),
		m_pool->getPageSize(), bcb->m_page);

	m_status.m_pendingReads.decrement();
	m_status.m_readTime += System::currentTimeMillis() - before;

	if (errCode != File::E_NO_ERROR)
		m_syslog->fopPanic(errCode, "Fail to read page "I64FORMAT"u of file %s.",
		bcb->m_pageKey.m_pageId, bcb->m_pageKey.m_file->getPath());
	if (m_checksumed && bcb->m_page->m_checksum != BufferPageHdr::CHECKSUM_NO
		&& bcb->m_page->m_checksum != BufferPageHdr::CHECKSUM_DISABLED) {
		// У���ΪCHECKSUM_NO��ʾ�ϴ�д��ʱû��ʹ��У���
		// У���ΪCHECKSUM_DISABLED��ʾ��ʹ��У���
		u32 checksum = checksumPage(bcb->m_page);
		if (bcb->m_page->m_checksum != checksum)
			m_syslog->log(EL_PANIC, "Page "I64FORMAT"u of file %s has invalid checksum value",
				bcb->m_pageKey.m_pageId, bcb->m_pageKey.m_file->getPath());
	}
	m_status.m_physicalReads++;
	session->incOpStat(OPS_PHY_READ);
	bcb->m_dbObjStats->countIt(DBOBJ_PHY_READ);

	if (!prefetch) {
		bcb->m_readSeq = ++m_readSeq;
		checkPrefetch(session->getId(), bcb);
	}
}

/**
 * ����Ƿ���Ҫ����Ԥ������Ҫʱ����Ԥ������
 *
 * @param userId �û�ID
 * @param bcb �ոն����ҳ
 */
void Buffer::checkPrefetch(u16 userId, Bcb *bcb) {
	ftrace(ts.buf, tout << userId << bcb);
	uint prefetchPages = m_prefetchPages;		// ��ֹ������ִ�й�����m_prefetchPages���޸ģ��Ȼ���֮
	u64 mod = bcb->m_pageKey.m_pageId % prefetchPages;
	if (mod == prefetchPages - PREFETCH_AHEAD - 1) {
		u64 areaStart, areaEnd;
		areaEnd = bcb->m_pageKey.m_pageId;
		areaStart = areaEnd / prefetchPages * prefetchPages;

		// ���ȼ��һ�¿���ҪԤ���������Ƿ����
		u64 fileSize = 0;
		bcb->m_pageKey.m_file->getSize(&fileSize);
		if ((areaStart + prefetchPages * 2) * m_pool->getPageSize() > fileSize)
			return;

		// ͳ�Ƶ�ǰԤ�������ж���ҳ���ڻ����У��Ҷ�ȡ˳���Ƿ���Ԥ��˳�����һ��
		RWLOCK(&m_lock, Shared);
		uint inBufferCnt = 0;
		uint rightOrderCnt = 0;
		u64 lastReadSeq = 0;
		for (u64 pageId = areaStart; pageId <= areaEnd; pageId++) {
			PageKey key(bcb->m_pageKey.m_file, bcb->m_pageKey.m_pageType, pageId);
			Bcb *abcb = m_pageHash.get(&key);
			if (abcb) {
				inBufferCnt++;
				if (abcb->m_readSeq > lastReadSeq) {
					rightOrderCnt++;
					lastReadSeq = abcb->m_readSeq;
				}
			}
		}
		RWUNLOCK(&m_lock, Shared);

		if (inBufferCnt >= (prefetchPages - PREFETCH_AHEAD) * m_prefetchRatio
			&& rightOrderCnt >= inBufferCnt * PREFETCH_RIGHT_ORDER_RATIO) {
			// ׼������Ԥ������Ϊ���ڻ����е�ҳ�����ÿ��ƿ鲢��������Щ��
			areaStart += prefetchPages;
			areaEnd = areaStart + prefetchPages - 1;

			PrefetchRequest *request = m_prefetcher->preparePrefetch(userId, bcb->m_pageKey.m_file);
			if (!request)
				return;

			bool allocBlockFailed = false;
			for (u64 pageId = areaStart; pageId <= areaEnd; pageId++) {
				// һ����˵ҪԤ��������ͨ�����ڻ����У�ֱ�Ӽӻ�����
				RWLOCK(&m_lock, Exclusived);
				PageKey key(bcb->m_pageKey.m_file, bcb->m_pageKey.m_pageType, pageId);
				if (m_pageHash.get(&key)) {
					RWUNLOCK(&m_lock, Exclusived);
					continue;
				}

				Bcb *abcb = tryAllocBlock(userId, bcb->m_pageKey.m_file, bcb->m_pageKey.m_pageType, pageId);
				if (!abcb) {
					allocBlockFailed = true;
					RWUNLOCK(&m_lock, Exclusived);
					break;
				}
				abcb->m_dbObjStats = new DBObjStats(DBO_Unknown, true);
				pinBlock(userId, abcb, false);
				request->m_pagesToRead.push_back(abcb);
				RWUNLOCK(&m_lock, Exclusived);
			}

			if (allocBlockFailed || request->m_pagesToRead.size() == 0) {
				canclePrefetch(userId, &request->m_pagesToRead);
				m_prefetcher->canclePrefetch(request);
			} else {
				m_prefetcher->commitPrefetch(request);
				m_status.m_prefetches++;
			}
		}
	}
}

/**
 * Ԥ��ָ����ҳ��
 * @pre pagesToRead������ҳ���Ѿ��û�������������pin
 * @post pagesToRead������ҳ���Ѿ����������pin
 *
 * @param session �Ự������ΪNULL
 * @param userId ����Ԥ�����û�ID
 * @param pagesToRead ҪԤ����ҳ��
 */
void Buffer::prefetchPages(Session *session, u16 userId, std::vector<Bcb *> *pagesToRead) {
	ftrace(ts.buf, tout << session << userId << pagesToRead);
	byte *p = new byte[BATCH_IO_SIZE + Limits::PAGE_SIZE];
	byte *buf = p + Limits::PAGE_SIZE - ((size_t)p) % Limits::PAGE_SIZE;
	uint batchPages = BATCH_IO_SIZE / Limits::PAGE_SIZE;

	File *file = pagesToRead->front()->m_pageKey.m_file;
	u64 startPage = pagesToRead->front()->m_pageKey.m_pageId;
	u64 endPage = pagesToRead->back()->m_pageKey.m_pageId;
	size_t currIdx = 0, nextBatchIdx = 0;

	for (u64 pageId = startPage; pageId <= endPage; pageId += batchPages) {
		if (pageId + batchPages > endPage + 1)
			batchPages = (uint)(endPage + 1 - pageId);

		// ͳ����Ҫ��ȡ��ҳ����
		uint notInBufPages = 0;
		size_t idx;
		for (idx = currIdx; idx < pagesToRead->size(); idx++) {
			if (pagesToRead->at(idx)->m_pageKey.m_pageId >= pageId + batchPages)
				break;
			notInBufPages++;
		}
		nextBatchIdx = idx;

		try {
			if (notInBufPages > batchPages * (1 - m_skipPrefetchRatio)) {
				nftrace(ts.buf, tout << "batch read.";);
				m_status.m_batchPrefetch++;

				// ���ڻ����е�ҳ�滹�Ƚ϶࣬������ȡ���������
				m_status.m_pendingReads.increment();
				u64 before = System::currentTimeMillis();

				u64 code = file->read(pageId * m_pool->getPageSize(),
					(u32)(batchPages * m_pool->getPageSize()), buf);

				m_status.m_readTime += System::currentTimeMillis() - before;
				m_status.m_pendingReads.decrement();
				m_status.m_physicalReads++;
				if (session)
					session->incOpStat(OPS_PHY_READ);
				
				if (code != File::E_NO_ERROR)
					NTSE_THROW(code, "Failed to read page"I64FORMAT"u to "I64FORMAT"u of file %s",
						pageId, pageId + batchPages - 1, file->getPath());
				assert(currIdx < nextBatchIdx);
				// ��������Ƶ�������ȡ�ĵ�һ��ҳ��ͷ�ϣ����ܲ���ȷ���������ҳ���з����ڲ�ͬ�����ݿ����
				// ������Щ��������ȫ�����ݣ���Щ����ĳ������������
				pagesToRead->at(currIdx)->m_dbObjStats->countIt(DBOBJ_PHY_READ);
				while (currIdx < nextBatchIdx) {
					Bcb *bcb = pagesToRead->at(currIdx);
					memcpy(bcb->m_page, buf + (bcb->m_pageKey.m_pageId - pageId) * m_pool->getPageSize(),
						m_pool->getPageSize());
					if (m_checksumed && bcb->m_page->m_checksum != BufferPageHdr::CHECKSUM_NO
						&& bcb->m_page->m_checksum != BufferPageHdr::CHECKSUM_DISABLED) {
						u32 checksum = checksumPage(bcb->m_page);
						if (bcb->m_page->m_checksum != checksum) {
							NTSE_THROW(NTSE_EC_PAGE_DAMAGE, "Page "I64FORMAT"u of file %s has invalid checksum value",
								bcb->m_pageKey.m_pageId, bcb->m_pageKey.m_file->getPath());
						}
					}
					m_status.m_prefetchPages++;
					unpinBlock(bcb);
					m_pool->unlockPage(userId, bcb->m_page, Exclusived);
					currIdx++;
				}
			} else {
				nftrace(ts.buf, tout << "read page one by one.";);
				m_status.m_nonbatchPrefetch++;
				// ���󲿷�ҳ���Ѿ��ڻ������ˣ�ֻ��ȡ��Щ���ڻ����е�ҳ
				while(currIdx < nextBatchIdx) {
					Bcb *bcb = pagesToRead->at(currIdx);
					readBlock(session, bcb, true);
					m_status.m_prefetchPages++;
					unpinBlock(bcb);
					m_pool->unlockPage(userId, bcb->m_page, Exclusived);
					currIdx++;
				}
			}
		} catch (NtseException &e) {
			nftrace(ts.buf, tout << "error in prefetch: " << e.getMessage(););
			// ����Ԥ��ʱ�Ĵ���
			m_syslog->log(EL_DEBUG, "Error in prefetch: %s", e.getMessage());
			while(currIdx < pagesToRead->size()) {
				Bcb *bcb = pagesToRead->at(currIdx);
				unpinBlock(bcb);
				freeBlock(userId, bcb);
				currIdx++;
			}
			break;
		}
	}

	delete []p;
}

/**
 * ȡ��Ԥ��
 *
 * @param userId �û�ID
 * @param pagesToRead ҪԤ����ҳ��
 */
void Buffer::canclePrefetch(u16 userId, std::vector<Bcb *> *pagesToRead) {
	ftrace(ts.buf, tout << userId << pagesToRead);
	for (size_t i = 0; i < pagesToRead->size(); i++) {
		unpinBlock(pagesToRead->at(i));
		freeBlock(userId, pagesToRead->at(i));
	}
}

/**
 * ����һ��ҳ���pin��������Ҫʱ����LRU����
 * @pre û�������ṹ��
 *
 * @param userId �û�ID
 * @param bcb ���ƿ�
 * @param touch �Ƿ��Ƕ�ҳ�����Ч����
 */
void Buffer::pinBlock(u16 userId, Bcb *bcb, bool touch) {
	ftrace(ts.buf, tout << userId << bcb << touch);
	assert(m_pool->isPageLocked(bcb->m_page, Shared) || m_pool->isPageLocked(bcb->m_page, Exclusived));

	LOCK(&bcb->m_metaLock);
	bcb->m_pinCount++;
	UNLOCK(&bcb->m_metaLock);

	if (touch)
		touchBlock(userId, bcb);
}

/**
 * �ͷ�һ��ҳ�ϵ�pin
 * ָ��ҳ����û�б�����
 *
 * @param bcb ҳ����ƿ�
 */
void Buffer::unpinBlock(Bcb *bcb) {
	ftrace(ts.buf, tout << bcb);
	LOCK(&bcb->m_metaLock);
	assert(bcb->m_pinCount > 0);
	bcb->m_pinCount--;
	UNLOCK(&bcb->m_metaLock);
}

/**
 * ����һ��ҳ��ʱ���ñ���������Ҫʱ��¼ҳ�����ʱ�䲢����LRU
 *
 * @param userId �û�ID
 * @param bcb ���ƿ�
 */
void Buffer::touchBlock(u16 userId, Bcb *bcb) {
	ftrace(ts.buf, tout << userId << bcb);
	bool firstTouch = bcb->m_readSeq == 0;
	if (firstTouch) {	// Ԥ��������û�з��ʹ���ҳ��
		bcb->m_readSeq = ++m_readSeq;
		checkPrefetch(userId, bcb);
	}
	if (firstTouch || m_readSeq - bcb->m_lruSeq >= m_correlatedRefPeriod) {
		// ������Է��ʣ�����LRU�������ԭ�����Ϸִ����������·ִ�
		// �������Ԥ��������ҳ�汻��һ�η��ʣ������²��뵽�Ϸִ�ͷ
		LOCK(&bcb->m_metaLock);
		bcb->m_lruSeq = m_readSeq;
		UNLOCK(&bcb->m_metaLock);

		RWLOCK(&m_lock, Exclusived);

		if (firstTouch) {
			bcb->m_lruLink.unLink();
			m_oldGen.addLast(&bcb->m_lruLink);
			m_status.m_firstTouch++;
		} else {
			DList<Bcb *> *list = bcb->m_lruLink.getList();
			if (list == &m_newGen)
				list->moveToLast(&bcb->m_lruLink);
			else {
				bcb->m_lruLink.unLink();
				m_newGen.addLast(&bcb->m_lruLink);
			}
			// �����Ϸִ�ռ�õı�����Ϊ���ٵ��������������Ϸִ���ʵ�ʴ�С
			// ������֮����OLD_GEN_SHIFT��ҳ�����ڵ�ƫ��
			uint targetOldGenSize = (uint)((m_newGen.getSize() + m_oldGen.getSize()) * m_oldGenRatio);
			if (m_oldGen.getSize() >= targetOldGenSize + OLD_GEN_SHIFT) {
				while (m_oldGen.getSize() > targetOldGenSize) {
					DLink<Bcb *> *link = m_oldGen.removeLast();
					m_newGen.addFirst(link);
				}
			} else if (m_oldGen.getSize() + OLD_GEN_SHIFT <= targetOldGenSize) {
				while (m_oldGen.getSize() < targetOldGenSize) {
					DLink<Bcb *> *link = m_newGen.removeFirst();
					m_oldGen.addLast(link);
				}
			}
			m_status.m_laterTouch++;
		}

		RWUNLOCK(&m_lock, Exclusived);
	}
}


/** ���ú�̨ˢ��ҳ�߳� */
void Buffer::disableScavenger() {
	ftrace(ts.buf, );
	m_scavenger->disable();
}

/** ���ú�̨ˢ��ҳ�߳� */
void Buffer::enableScavenger() {
	ftrace(ts.buf, );
	m_scavenger->enable();
}


/**
 * ������ҳ����У��͡���У��͵���Ҫ��;�Ǽ��ҳ�汻����д��ʱ���µ�
 * ���ݲ�һ���ԣ������Ƿ�ֹ��Ϊ�����ݽ��еĴ۸ġ���˹�ע�������ܶ���
 * ��α��Ŀ����ԡ�
 *
 * @param page ����ҳ
 * @return �����
 */
u32 Buffer::checksumPage(BufferPageHdr *page) {
	ftrace(ts.buf, tout << page);
	u64 v = 16777619;
	for (u64 *p = (u64 *)((char *)page + sizeof(BufferPageHdr));
		(char *)p + sizeof(u64) < (char *)page + m_pool->getPageSize(); p++)
		v ^= *p;
	u32 checksum = (u32)(v >> 32) ^ (u32)(v & 0xFFFFFFFF);
	if (checksum == BufferPageHdr::CHECKSUM_DISABLED || checksum == BufferPageHdr::CHECKSUM_NO)
		checksum = 1;
	return checksum;
}

/**
 * �ڿ�����չ��֤����ʱ��ͬ��ҳ��������checksum��ʹ�ý������Լ��
 * checksum��ȷ��
 * @pre bcb��Ӧ��ҳ���Ѿ�����ָ����������
 *
 * @param bcb ҳ����ƿ�
 */
void Buffer::syncChecksum(Bcb *bcb, LockMode lockMode) {
#ifdef NTSE_VERIFY_EX
	assert(m_pool->isPageLocked(bcb->m_page, lockMode));
	if (bcb->m_page->m_checksum == BufferPageHdr::CHECKSUM_DISABLED)
		return;
	bcb->m_dirtyMarked = false;
	if (!vs.buf) {
		if (lockMode == Exclusived)
			bcb->m_checksumValid = false;
		return;
	}
	// �Ӷ���ʱ�п����ж���˶�������checksum��ֻӰ��һ������
	// ��Ӱ����ȷ�ԣ�ԭ������Щ�˼��������checksumһ����һ����
	if (!bcb->m_checksumValid) {
		u32 checksum = checksumPage(bcb->m_page);
		LOCK(&bcb->m_metaLock);
		if (!bcb->m_checksumValid) {
			bcb->m_page->m_checksum = checksum;
			bcb->m_checksumValid = true;
		}
		UNLOCK(&bcb->m_metaLock);
	} else {
		assert(checksumPage(bcb->m_page) == bcb->m_page->m_checksum);
	}
#else
	UNREFERENCED_PARAMETER(bcb);
	UNREFERENCED_PARAMETER(lockMode);
#endif
}

/**
 * �ڿ�����չ��֤����ʱ������checksum��֤ҳ����û�д���ı��޸ġ�
 * @pre ҳ���Ѿ�����ָ������ģʽ����
 *
 * @param page ҳ��
 * @param lockMode ��ҳ�����ӵ���
 */
void Buffer::verifyChecksum(BufferPageHdr *page, LockMode lockMode) {
#ifdef NTSE_VERIFY_EX
	assert(m_pool->isPageLocked(page, lockMode));
	if (page->m_checksum == BufferPageHdr::CHECKSUM_DISABLED)
		return;
	Bcb *bcb = (Bcb *)m_pool->getInfo(page);
	if (vs.buf) {
		if (bcb->m_checksumValid) {
			if (lockMode == Shared || !bcb->m_dirtyMarked) {
				// �Ӷ����������д������û�е���markDirtyʱ��ҳ������
				// ��Ӧ�ñ��޸�
				NTSE_ASSERT(checksumPage(page) == page->m_checksum);
			} else if (bcb->m_dirtyMarked) {
				assert(lockMode == Exclusived);
				page->m_checksum = checksumPage(page);
			}
		}
	} else if (lockMode == Exclusived)
		bcb->m_checksumValid = false;
#else
	UNREFERENCED_PARAMETER(page);
	UNREFERENCED_PARAMETER(lockMode);
#endif
}

/** �滻ҳ���ͳ�ƶ���
 * @param bcb ҳ����ƿ�
 * @param dbObjStats ���ݿ����ͳ�ƽṹ
 */
void Buffer::replaceDBObjStats(Bcb *bcb, DBObjStats *dbObjStats) {
	assert(!dbObjStats->m_bufInternal);
	LOCK(&bcb->m_metaLock);
	if (bcb->m_dbObjStats != dbObjStats) {
		if (bcb->m_dbObjStats->m_bufInternal) {
			dbObjStats->merge(bcb->m_dbObjStats);
			delete bcb->m_dbObjStats;
		}
		bcb->m_dbObjStats = dbObjStats;
	}
	UNLOCK(&bcb->m_metaLock);
}

/**
 * ����Scavenger�߳�
 */
void Buffer::signalScavenger() {
	m_scavenger->signal();
}

/**
 * ���캯��
 *
 * @param db �������ݿ⣬����ΪNULL
 * @param buffer ҳ�滺��
 * @param queueSize Ԥ�����д�С
 * @param syslog ϵͳ��־
 */
Prefetcher::Prefetcher(Database *db, Buffer *buffer, uint queueSize, Syslog *syslog): BgTask(db, "Buffer::Prefetcher", 1000, true, -1, true),
	m_lock("Prefetcher::lock", __FILE__, __LINE__) {
	m_buffer = buffer;
	m_syslog = syslog;
	m_maxRequest = queueSize;
	m_queue = new PrefetchRequest[m_maxRequest];
	for (uint i = 0; i < m_maxRequest; i++)
		m_queue[i].m_file = NULL;
}

/**
 * ��������
 */
Prefetcher::~Prefetcher() {
	delete[] m_queue;
}

/**
 * Ԥ�������������������Ҫ�����Ԥ������������֮
 */
void Prefetcher::runIt() {
	ftrace(ts.buf, );
	PrefetchRequest *request = NULL;
	LOCK(&m_lock);
	for (uint i = 0; i < m_maxRequest; i++) {
		if (TRYLOCK(&m_queue[i].m_lock)) {
			if (m_queue[i].m_file) {
				request = &m_queue[i];
				break;
			} else {
				UNLOCK(&m_queue[i].m_lock);
			}
		}
	}
	UNLOCK(&m_lock);
	if (request)
		prefetch(request);
}

/**
 * ֪ͨԤ�������߳�ȡ����ָ���ļ���Ԥ��
 * @post ��ָ���ļ��Ѿ����е�Ԥ���Ѿ���ɣ�δ�����Ԥ������ȡ��
 *
 * @param file �ļ�
 */
void Prefetcher::canclePrefetch(File *file) {
	ftrace(ts.buf, tout << file);
	assert(file);

	while (true) {
		bool inProgress = false;
		LOCK(&m_lock);
		for (uint i = 0; i < m_maxRequest; i++) {
			if (TRYLOCK(&m_queue[i].m_lock)) {
				if (m_queue[i].m_file == file) {
					m_buffer->canclePrefetch(m_queue[i].m_userId, &m_queue[i].m_pagesToRead);
					m_queue[i].m_pagesToRead.clear();
					m_queue[i].m_file = NULL;
				}
				UNLOCK(&m_queue[i].m_lock);
			} else
				inProgress = true;
		}
		UNLOCK(&m_lock);
		if (!inProgress)
			break;
		Thread::msleep(100);
	}
}

/**
 * ׼������Ԥ��������Ԥ��������
 * @post ���ص�Ԥ���������Ѿ�������
 *
 * @param userId �û�ID
 * @param file ��Ҫ������ļ�Ԥ��
 * @return ����ɹ�����Ԥ�������ʧ�ܷ���NULL
 */
PrefetchRequest* Prefetcher::preparePrefetch(u16 userId, File *file) {
	ftrace(ts.buf, tout << userId << file);
	PrefetchRequest *ret = NULL;
	LOCK(&m_lock);
	uint i;
	for (i = 0; i < m_maxRequest; i++) {
		if (TRYLOCK(&m_queue[i].m_lock)) {
			if (m_queue[i].m_file == NULL) {
				m_queue[i].m_userId = userId;
				m_queue[i].m_file = file;
				ret = m_queue + i;
				break;
			}
			UNLOCK(&m_queue[i].m_lock);
		}
	}
	UNLOCK(&m_lock);

	assert(!ret || ret->m_pagesToRead.empty());
	return ret;
}

/**
 * �ύԤ������
 * @pre request����prepareRequest����
 *
 * @param request Ԥ������
 */
void Prefetcher::commitPrefetch(PrefetchRequest *request) {
	ftrace(ts.buf, tout << &request->m_pagesToRead);
	assert(request->m_lock.isLocked());
	assert(request->m_pagesToRead.size());
	assert(request->m_file);
	UNLOCK(&request->m_lock);
	signal();
}

/**
 * ȡ��Ԥ������
 * @pre request����prepareRequest����
 *
 * @param request Ԥ������
 */
void Prefetcher::canclePrefetch(PrefetchRequest *request) {
	ftrace(ts.buf, tout << &request->m_pagesToRead);
	assert(request->m_lock.isLocked());
	assert(request->m_file);
	request->m_file = NULL;
	request->m_pagesToRead.clear();
	UNLOCK(&request->m_lock);
}

/**
 * ����Ԥ��
 * @pre Ҫ�����Ԥ����������Ѿ�������
 * @post Ԥ����������ѽ���
 *
 * @request Ԥ������
 */
void Prefetcher::prefetch(PrefetchRequest *request) {
	ftrace(ts.buf, tout << &request->m_pagesToRead);

	m_buffer->prefetchPages(m_session, request->m_userId, &request->m_pagesToRead);

	request->m_pagesToRead.clear();
	request->m_file = NULL;
	UNLOCK(&request->m_lock);
}

#ifdef TNT_ENGINE
/**
 * ���캯��
 *
 * @param db ���ݿ⡣����ΪNULL
 * @param buffer ҳ�滺��
 * @param interval ��������
 * @param maxCleanPagesRatio LRU����β��Clean Pagesռ����LRU�����ȵı���
 * @param maxScavengerPages	 һ���������д������ҳ����
 */
Scavenger::Scavenger(Database *db, Buffer *buffer, uint interval, double maxCleanPagesRatio, uint maxScavengerPages):
	BgTask(db, "Buffer::Scavenger", interval, true, -1, true) {
	m_buffer = buffer;
	m_maxCleanPagesRatio = maxCleanPagesRatio;
	m_enabled = true;

	// ����m_maxCleanPageRatio������m_maxCleanPagesLen
	uint maxCleanPagesLen = (uint)(buffer->getTargetSize() * m_maxCleanPagesRatio);
	
	if (maxCleanPagesLen < Buffer::SCAVENGER_MIN_SEARCH_LEGNTH) {
		maxCleanPagesLen = Buffer::SCAVENGER_MIN_SEARCH_LEGNTH;
	}

	m_maxCleanPagesLen = maxCleanPagesLen;
	m_maxScavengerPages= maxScavengerPages;
	m_realScavengeCnt = 0;
#ifdef NTSE_UNIT_TEST
	m_useAio = true;
#else
	m_useAio = m_db->getConfig()->m_aio;
#endif

#ifndef WIN32
	if (m_useAio) {
		u64 errCode = m_aioArray.aioInit();
		if (File::E_NO_ERROR != errCode) {
			m_db->getSyslog()->fopPanic(errCode, "System AIO Init Error");
		}
	}
#endif
}

/**
 * ˢд��ҳ������
 */
void Scavenger::runIt() {
	ftrace(ts.buf, );
	if (!m_enabled) {
		nftrace(ts.buf, tout << "Scavenger has been disabled.";);
		return;
	}
	uint numFreePages = m_buffer->getNumFreePages() + m_buffer->getPool()->getNumFreePages();
	// ֻҪ��ǰ��Ծ�ȷ�Ŀ���ҳ�������������Ҫ�����ҳ�������Ͳ���Ҫ������ҳ�����QA38615
 	if (m_maxCleanPagesLen < numFreePages)
 		return;


	m_realScavengeCnt++;
	/** Scavengerÿ��Flush��Dirty Pages�������ޣ��ɸ���ϵͳI/O������������Ĭ��Ϊÿ��200 */
#ifndef WIN32
	if (m_useAio)
		m_buffer->flushDirtyPagesUseAio(m_session, NULL, m_maxCleanPagesLen, m_maxScavengerPages, true, &m_aioArray);
	else
#endif
		m_buffer->flushDirtyPages(m_session, NULL, m_maxCleanPagesLen, m_maxScavengerPages, true);
}

#else
/**
 * ���캯��
 *
 * @param db ���ݿ⡣����ΪNULL
 * @param buffer ҳ�滺��
 * @param interval ��������
 * @param maxPagesRatio ÿ��д����ҳ�������������һ����
 * @param multiplier ÿ��д����ҳ���ϴ����������������¼���ҳ��ı���
 */
Scavenger::Scavenger(Database *db, Buffer *buffer, uint interval, double maxPagesRatio, double multiplier):
	BgTask(db, "Buffer::Scavenger", interval, true, -1, true) {
	m_buffer = buffer;
	m_maxPagesRatio = maxPagesRatio;
	m_multiplier = multiplier;
	m_pageCreates = 0;
	m_enabled = true;
}

/**
 * ˢд��ҳ������
 */
void Scavenger::runIt() {
	ftrace(ts.buf, );
	if (!m_enabled) {
		nftrace(ts.buf, tout << "Scavenger has been disabled.";);
		return;
	}
	u64 pageCreates = m_buffer->getStatus().m_pageCreates;
	u64 diff = pageCreates - m_pageCreates;
	m_pageCreates = pageCreates;
	diff += m_buffer->getStatus().m_pendingAllocs.get();

	u64 searchLength = (u64)(diff * m_multiplier);
	if (searchLength < Buffer::SCAVENGER_MIN_SEARCH_LEGNTH)
		searchLength = Buffer::SCAVENGER_MIN_SEARCH_LEGNTH;
	uint numFreePages = m_buffer->getNumFreePages() + m_buffer->getPool()->getNumFreePages();
	// ֻҪ��ǰ��Ծ�ȷ�Ŀ���ҳ�������������Ҫ�����ҳ�������Ͳ���Ҫ������ҳ�����QA38615
	if (searchLength < numFreePages)
		return;
	u64 maxPages = (u64)(m_buffer->getCurrentSize() * m_maxPagesRatio);
	if (maxPages < Buffer::SCAVENGER_MIN_PAGES)
		maxPages = Buffer::SCAVENGER_MIN_PAGES;

	m_buffer->flushDirtyPages(m_session, NULL, searchLength, maxPages);
}

#endif

/**
 * ��������
 *
 */
Scavenger::~Scavenger() {
#ifndef WIN32
	if (m_useAio) {
		u64 errCode = m_aioArray.aioDeInit();
		if (File::E_NO_ERROR != errCode) {
			m_db->getSyslog()->fopPanic(errCode, "System AIO DeInit Error");
		}
	}
#endif
}

/**
 * ��ȡһ������ʱ���д������ҳռ�����С֮��
 *
 * @return һ������ʱ���д������ҳռ�����С֮��
 */
double Scavenger::getMaxPagesRatio() {
	return m_maxPagesRatio;
}

/**
 * ����һ������ʱ���д������ҳռ�����С֮��
 *
 * @param maxPagesRatio һ������ʱ���д������ҳռ�����С֮�ȣ�������[0,1]֮��
 */
void Scavenger::setMaxPagesRatio(double maxPagesRatio) {
	ftrace(ts.buf, tout << m_maxPagesRatio);
	if (maxPagesRatio < 0 || maxPagesRatio > 1.0)
		return;
	m_maxPagesRatio = maxPagesRatio;
}

/**
 * ��ȡ�Ŵ�ϵ��(��ҳ��˵��)
 *
 * @return �Ŵ�ϵ��
 */
double Scavenger::getMultiplier() {
	return m_multiplier;
}

/**
 * ���÷Ŵ�ϵ��
 *
 * @param multiplier �Ŵ�ϵ��������>=0
 */
void Scavenger::setMultiplier(double multiplier) {
	ftrace(ts.buf, tout << multiplier);
	if (multiplier < 0)
		return;
	m_multiplier = multiplier;
}

/**
 * ����ÿ��д�������ҳ����
 *
 */
uint Scavenger::getMaxScavengerPages() {
	return m_maxScavengerPages;
}

/**
 * ����ÿ��д�������ҳ����
 *
 */
void Scavenger::setMaxScavengerPages(uint maxScavengerPages) {
	m_maxScavengerPages = maxScavengerPages;
}

/**
 * ������������������Ĵ���
 *
 */
u64 Scavenger::getRealScavengeCnt() {
	return m_realScavengeCnt;
}

/** ��ֹ��̨д��ҳ�߳� */
void Scavenger::disable() {
	ftrace(ts.buf, );
	m_enabled = false;
}

/** ���ú�̨д��ҳ�߳� */
void Scavenger::enable() {
	ftrace(ts.buf, );
	m_enabled = true;
}

///////////////////////////////////////////////////

/** ���쳤��Ϊָ���ֽ�����IoBuffer */
IoBuffer::IoBuffer(uint pageCnt, uint pageSize) 
: m_pageSize(pageSize), m_size(pageCnt * pageSize) {
	m_data = (byte *)System::virtualAlloc(m_size);
}

IoBuffer::~IoBuffer() {
	if (m_data) {
		System::virtualFree(m_data);
	}
}

/** ��ȡ�����ַ������ */
byte* IoBuffer::getBuffer() {
	assert(m_data);		
	return m_data;
}

/** ���泤�� */
size_t IoBuffer::getSize() const {
	return m_size;
}

/** ҳ�� */
uint IoBuffer::getPageCnt() const {
	return m_size / m_pageSize;
}

BatchIoBufPool::BatchIoBufPool(uint instNum, uint pageCntPerInst, uint pageSize) : Pool((uint)-1, false) {
	LOCK(&m_lock);
	for (uint i = 0; i < instNum; i++) {
		IoBuffer *inst = new IoBuffer(pageCntPerInst, pageSize);
		add(inst);
	}
	UNLOCK(&m_lock);
}

}
