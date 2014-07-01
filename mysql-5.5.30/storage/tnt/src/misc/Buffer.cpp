/**
 * 页面缓存管理
 *
 * @author 汪源(wangyuan@corp.netease.com, wy@163.org)
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
/** 预读请求 */
struct PrefetchRequest {
	Mutex	m_lock;		/** 正在处理预读请求时锁定这一对象 */
	u16		m_userId;	/** 请求预读的用户ID，页面被这个用户锁定 */
	File	*m_file;	/** 从哪个文件中预读，为NULL表示为自由预读请求对象 */
	std::vector<Bcb *>	m_pagesToRead;	/** 要预读的页，一定属于同一文件和页面类型，并且页号为递增序 */

	PrefetchRequest(): m_lock("PrefetchRequest::lock", __FILE__, __LINE__) {
		m_file = NULL;
	}
};


/** 预读线程 */
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
	Mutex	m_lock;				/** 保护并发访问的锁 */
	Buffer	*m_buffer;			/** 页面缓存 */
	uint	m_maxRequest;		/** 预读请求队列大小 */
	PrefetchRequest	*m_queue;	/** 预读请求队列 */
	Syslog	*m_syslog;			/** 系统日志 */
};

/** 将脏页写出到磁盘的后台线程 
 * 该线程的主要目的并不是要定时刷写脏页，而是为了保证大部分时刻缓存LRU链表尾有足够的非脏页面用于替换
 * 假如缓存LRU尾已经有足够的非脏页面了，清道夫线程不应该再多刷写脏页，避免不必要的IO
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
	Buffer	*m_buffer;			/** 页面缓存 */
	double	m_maxPagesRatio;	/** 一次清理时最多写出的脏页占缓存大小之比 */
	double	m_multiplier;		/** 放大系数 */
	u64		m_pageCreates;		/** 页面缓存新建页面次数 */
	bool	m_enabled;			/** 是否启用后台刷脏页功能 */
	AioArray m_aioArray;		/** 清道夫线程aio队列 */
	u64		m_realScavengeCnt;  /** 清道夫真正刷页面的次数 */
	bool	m_useAio;			/** 清道夫是否使用AIO */

#ifdef TNT_ENGINE
	double	m_maxCleanPagesRatio;	/** 一次清理，最多维护LRU链表尾部CLEAN Pages的长度占LRU总长度的比例 */
	uint	m_maxCleanPagesLen;		/** 一次清理，最多维护LRU链表尾部CLEAN Pages的长度，根据比例计算得来 */	
	uint	m_maxScavengerPages;	/** 一次清理，最多写出的脏页数量，可根据I/O能力调整 */
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
 * 创建一个页面缓存
 *
 * @param db 所属数据库，在测试时可以为NULL
 * @param numPages 缓存大小
 * @param pagePool 所属内存页池
 * @param syslog 系统日志
 * @param txnlog 事务日志
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
	// 计算预读时，一批中超过多少比例的页面已经在缓存中时
	// 逐个读取各个页面而不是顺序读出所有页面
	// 这里的计算假设服务器顺序读取的性能是100MB/s，随机读取一个
	// 页面的时间是3ms
	int batchPages = BATCH_IO_SIZE / pagePool->getPageSize();
	int seqReadSpeed = 100 * 1024 * 1024;
	double randomReadTime = 0.003;
	double seqReadTime = randomReadTime + (double)BATCH_IO_SIZE / seqReadSpeed;
	m_skipPrefetchRatio = 1 - seqReadTime / batchPages / randomReadTime;
	memset(&m_status, 0, sizeof(BufferStatus));
	memset(&m_flushStatus, 0, sizeof(BufferFlushStatus));
}

/**
 * 析构函数。不会释放缓存占用的页面，因为这一函数只在系统停止时才调用，
 * 所有内存由内存页池统一释放
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
			// 以互斥锁锁定页面之后，对该页唯一可能进行的操作就是unPin
			// 因此在加Bcb元信息锁判断pinCount为0之后，可以安全的将Bcb
			// 元信息锁释放掉
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
 * 获取一个页，pin和锁定该页
 *
 * @param session 会话，不允许为NULL
 * @param file 文件
 * @param pageType 页面类型
 * @param pageId 页号
 * @param lockMode 锁模式
 * @param dbObjStats 数据对象状态
 * @guess 要读取的页很有可能是这个地址
 *
 * @return 页的首地址
 */
BufferPageHdr* Buffer::getPage(Session *session, File *file, PageType pageType, u64 pageId,
							   LockMode lockMode, DBObjStats* dbObjStats, BufferPageHdr *guess) {
	ftrace(ts.buf, tout << session << file << pageType << pageId << lockMode << dbObjStats << guess);

	assert(lockMode == Shared || lockMode == Exclusived);

	// 如果指定了guess，首先看看指定的页面是否为所需页面，
	// 若是则直接返回该页面，若不是则走正常流程
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
 * 分配一个新页，pin和锁定该页
 *
 * @param session 会话，可能为NULL
 * @param file 文件
 * @param pageType 页面类型
 * @param pageId 页号
 * @param lockMode 锁模式
 * @param dbObjStats 数据对象状态
 *
 * @return 页的首地址
 */
BufferPageHdr* Buffer::newPage(Session *session, File *file, PageType pageType, u64 pageId,
							   LockMode lockMode, DBObjStats* dbObjStats) {
	ftrace(ts.buf, tout << session << file << pageType << pageId << lockMode <<dbObjStats);
	return doGetPage(session, file, pageType, pageId, lockMode, false, dbObjStats);
}

/**
 * 获取一个页，pin和锁定该页
 *
 * @param session 会话，不允许为NULL
 * @param file 文件
 * @param pageType 页面类型
 * @param pageId 页号
 * @param lockMode 锁模式
 * @param readFromFile 当要获取的页不在缓存中时，是否要从文件中读取页面数据
 * @param dbObjStats 数据对象状态
 *
 * @return 页的首地址
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
	// 搜索哈希表
	bcb = m_pageHash.get(&pageKey);
	if (bcb) {
		// 若找到，首先在持有全局结构锁时尝试对页面加锁，若不成功
		// 则释放全局结构锁后试图锁定并检查是否为需要的页。
		// 由于锁定一个页时可能会等待比较长时间（页面正进行IO），
		// 因此先行释放全局结构锁是必要的。
		// 首先尝试加锁在不发生冲突时可以优化性能。
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
		assert(bcb->m_pageKey.m_pageType == pageType);	// 页面类型不可能发生变化
		pinBlock(session->getId(), bcb, true);
#ifdef NTSE_VERIFY_EX
		syncChecksum(bcb, lockMode);
#endif
		if (bcb->m_dbObjStats != dbObjStats)
			replaceDBObjStats(bcb, dbObjStats);
		m_status.m_pendingAllocs.decrement();
		return page;
	} else {
		// 若找不到，则分配一个块，但首先要确保加了全局结构互斥锁
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
 * 试图获取一个页，pin并且锁定该页
 *
 * @param session 会话
 * @param file 文件
 * @param pageType 页面类型
 * @param pageId 页号
 * @param lockMode 锁模式
 * @param dbObjStats 数据对象状态
 *
 * @return 页的首地址
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
	// 搜索哈希表
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
		// 若找不到，则分配一个块，但首先要确保加了全局结构互斥锁
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
 * 判断页面是否已经在缓冲中
 * 
 * @param session 会话
 * @param file 文件
 * @param pageType 页面类型
 * @param pageId 页号
 *
 * @return 页在缓冲中返回true，否则返回false
 */
bool Buffer::hasPage(File *file, PageType pageType, u64 pageId) {
	RWLockGuard(&m_lock, Shared, __FILE__, __LINE__);
	PageKey pageKey(file, pageType, pageId);
	return m_pageHash.get(&pageKey) != NULL;
}

/**
 * 获得批量IO缓存池
 * @return
 */
BatchIoBufPool* Buffer::getBatchIoBufferPool() {
	return &m_batchIOBufPool;
}

/**
 * 释放一个页，包括释放该页的锁和pin
 *
 * @param session 会话
 * @param page 页面地址
 * @param lockMode 锁模式
 */
void Buffer::releasePage(Session *session, BufferPageHdr *page, LockMode lockMode) {
	ftrace(ts.buf, tout << session << (Bcb *)m_pool->getInfo(page) << lockMode);
	unpinPage(page);
	unlockPage(session->getId(), page, lockMode);
}

/**
 * 进行锁升级。系统将先尝试不放锁进行锁升级，若不成功，
 * 则释放现有的共享锁，然后加互斥锁。
 * @pre 调用者持有pin
 * @pre 调用者对指定页加的是读锁
 *
 * @param session 发出加锁请求的会话
 * @param page 页面地址
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
 * 释放一个页上的pin
 * 指定页可能没有被锁定
 *
 * @param page 页面地址
 */
void Buffer::unpinPage(BufferPageHdr *page) {
	ftrace(ts.buf, tout << (Bcb *)m_pool->getInfo(page));
	Bcb *bcb = (Bcb *)m_pool->getInfo(page);
	unpinBlock(bcb);
}

/**
 * 设置一个页面为脏页
 * @pre 页面已经被用互斥锁锁定
 *
 * @param session 会话，允许为NULL
 * @param page 页面地址
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
 * 查看一个页面是否为脏页
 * @pre 页面已经被锁定
 *
 * @param page
 * @return 页面是否为脏页
 */
bool Buffer::isDirty(BufferPageHdr *page) {
	assert(m_pool->isPageLocked(page, Shared) || m_pool->isPageLocked(page, Exclusived));

	Bcb *bcb = (Bcb *)m_pool->getInfo(page);
	return bcb->m_dirty;
}

/**
 * 将一个页写出到文件中，同时清除脏标志位
 * @pre 页面已经被锁定
 *
 * @param session 会话，可能为NULL
 * @param page 页面地址
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


/** 批量写出页面，用于写出刚刚扩展出来的页面
 *
 * @param session 会话
 * @param file 所属文件
 * @param pageType 页面类型
 * @param minPid 要写出的最小页号，包括
 * @param maxPid 要写出的最大页号，包括
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
			// 由于本函数只用于写出刚刚扩展出来的页面，这些页面不会被除了Scavenger之外的应用使用，
			// 因此tryLock通常能成功
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
			if (bcb && !otherIsWriting) {	// 在缓存中，但加不上锁, 并且页面不是正在被其他线程写
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

/** 更新页面校验和
 * @param page 页面
 */
void Buffer::updateChecksum(BufferPageHdr *page) {
	if (page->m_checksum != BufferPageHdr::CHECKSUM_DISABLED) {
		if (m_checksumed)
			page->m_checksum = checksumPage(page);
		else
			page->m_checksum = BufferPageHdr::CHECKSUM_NO;
	}
}

/** 批量写出页面数据
 * @param session 会话
 * @param file 文件
 * @param startPid 要写出的起始页号
 * @param bcbs 要写出的页面的控制块
 */
void Buffer::batchWritePages(Session *session, File *file, u64 startPid, std::vector<Bcb *>* bcbs) {
	IoBuffer *ioBuffer = (IoBuffer *)m_batchIOBufPool.getInst();
	byte *buf = ioBuffer->getBuffer();
	assert(buf);

	if (ioBuffer->getSize() < bcbs->size() * Limits::PAGE_SIZE) {
		m_syslog->log(EL_PANIC, "Batch IO buffer size is two small, required is :%d, real size is %d.", 
		bcbs->size() * Limits::PAGE_SIZE, ioBuffer->getSize());
	}

	// 加上写操作锁，更新校验和，统计最大pageLsn
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

	// 写数据
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

	// 更新数据库对象级统计信息，清除页面dirty标志，释放写操作锁和页数
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
 * 释放页面缓存中指定文件缓存的所有页
 * @pre 要释放的页面不能被pin住，也不能被锁定
 *
 * @param session 会话，可以为NULL
 * @param file 文件
 * @param writeDirty 是否写出脏页
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
 * 释放页面缓存中为指定索引缓存的所有页
 * @pre 要释放的页面不能被pin住，也不能被锁定
 *
 * @param session 会话
 * @param file 文件
 * @param indexId 索引ID
 * @param callback 用于判断一个页是否是指定索引使用的页的回调函数
 *   若为NULL则释放为指定文件缓存的所有页
 */
void Buffer::freePages(Session *session, File *file, uint indexId, bool (*callback)(BufferPageHdr *page, PageId pageId, uint indexId)) {
	ftrace(ts.buf, tout << session << file << indexId << callback);
	// 取消对这个文件的预读，防止释放过程中又读入该文件的新页面
	m_prefetcher->canclePrefetch(file);

	Array<BufferPageHdr *> toFreePages;
	Array<PageType > toFreeTypes;

	// 由于刷写脏页线程的影响，有些页面可能由于被锁定而不能马上被释放掉，
	// 这里需要扫描多次，直到确认没有指定文件的页面在缓存中才可
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
			// 文件正在等待被sync, 等待一段时间之后重试
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
 * 写出所有脏页
 *
 * @param session 会话，可能为NULL
 * @param targetTime 建议操作在指定的时间点之前完成，单位秒
 * @throw NtseException 操作被取消，异常码为NTSE_EC_CANCELED
 */
void Buffer::flushAll(Session *session) throw(NtseException) {
	ftrace(ts.buf, tout << session);
	flushDirtyPages(session, NULL, (u64)-1, (u64)-1, false);
}



/**
 * 写出指定数量的脏页
 *
 * @param session 会话，可能为NULL
 * @param file 只写出指定文件的脏页
 * @param searchLength 要扫描的LRU链表长度
 * @param maxPages 要写出的脏页数，若为(u64)-1则写出所有脏页
 * @param targetTime 建议操作在指定的时间点之前完成，单位秒
 * @param ignoreCancel 是否忽略操作被取消信息，若为true，则本函数不会抛出异常
 * @throw NtseException 操作被取消，异常码为NTSE_EC_CANCELED，只在ignoreCancel为false才可能抛出异常
 */
void Buffer::flushDirtyPages(Session *session, File *file, u64 searchLength, u64 maxPages, bool ignoreCancel) throw(NtseException) {
	ftrace(ts.buf, tout << session << file << searchLength << maxPages);
	if (maxPages != (u64)-1 && m_allPages.getSize() == 0)
		return;

	bool flushAllPages = maxPages == (u64)-1;

	std::vector<KeyPagePair> dirtyPages;
	// 要保证写出所有脏页，必须锁住所有脏页
	// 刷写脏页线程可以跳过加不上锁的脏页
	bool skipLocked = !flushAllPages;

	// 收集锁定LRU链表的时间
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

	// 做全量flush时需要统计锁定lru链表的时间
	if (searchLength == (u64)-1) {
		m_status.m_flushAllBufferLockTime += (System::fastTime() - before);
		m_status.m_flushAllBufferLockCount++;
	}

	doFlushDirtyPages(session, file, &dirtyPages, ignoreCancel, flushAllPages);
}

/**
 * 写出指定的脏页
 *
 * @param session 会话，可能为NULL
 * @param file 只写出指定文件的脏页
 * @param dirtyPagesVec 脏页面数组
 * @param targetTime 建议操作在指定的时间点之前完成，单位秒
 * @param ignoreCancel 是否忽略操作被取消信息，若为true，则本函数不会抛出异常
 * @throw NtseException 操作被取消，异常码为NTSE_EC_CANCELED，只在ignoreCancel为false才可能抛出异常
 */
void Buffer::doFlushDirtyPages(Session *session, File *file, vector<KeyPagePair> *dirtyPagesVec, 
							 bool ignoreCancel, bool flushAllPages) throw(NtseException) {
	vector<KeyPagePair> &dirtyPages(*dirtyPagesVec);
	uint batchSize = 16;
	size_t remain = dirtyPages.size(), done = 0, oldDone = 0, skipped = 0;
	u64 time = 0;

	// 要保证写出所有脏页，必须锁住所有脏页
	// 刷写脏页线程可以跳过加不上锁的脏页
	int timeout = flushAllPages ? -1 : UNSAFE_LOCK_TIMEOUT;
	// 是否刷整个缓存
	bool flushWorld = file == NULL && flushAllPages;

	// 按文件和页号排序后，写出每个脏页。排序是为了提高性能
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
				&& (done - oldDone) >= batchSize	// 写完一批了
				&& remain > 0		// 剩下的还有
				&& m_db->getStat() != DB_CLOSING) {
				// 进行检查点时写完一批页面，考虑sleep一下以防止过度影响系统性能
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

	set<File *> fileSet; // 脏页对应的文件集合
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
 * sync非directIo方式打开的文件
 *
 * @param fileSet 需要sync的文件集合
 */
void Buffer::syncFiles(set<File *> fileSet) {
	// 针对非directIo防护打开的文件， 调用文件系统sync方法
	// 调用sync之前，必须增加引用计数，避免文件对象被删除
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
 * 获取文件的所有脏页面
 * @pre 全局锁被锁定
 *
 * @param dirtyPages OUT，存放满足条件的脏页信息
 * @param file 感兴趣的页面对应的文件
 * @param searchLength 搜索list中最多这么多个页面
 * @param maxPages 取出最多这么多个脏页
 * @param skipLocked 是否跳过被锁定的页面
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
			//如果是脏页
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
 * 用异步IO写出所有脏页
 *
 * @param session 会话，可能为NULL
 * @param targetTime 建议操作在指定的时间点之前完成，单位秒
 * @param array	异步IO请求队列
 * @throw NtseException 操作被取消，异常码为NTSE_EC_CANCELED
 */
void Buffer::flushAllUseAio(Session *session, AioArray *array) throw(NtseException) {
	ftrace(ts.buf, tout << session);
	flushDirtyPagesUseAio(session, NULL, (u64)-1, (u64)-1, false, array);
}



/**
 * 用异步IO写出指定数量的脏页
 *
 * @param session 会话，可能为NULL
 * @param file 只写出指定文件的脏页
 * @param searchLength 要扫描的LRU链表长度
 * @param maxPages 要写出的脏页数，若为(u64)-1则写出所有脏页
 * @param targetTime 建议操作在指定的时间点之前完成，单位秒
 * @param ignoreCancel 是否忽略操作被取消信息，若为true，则本函数不会抛出异常
 * @throw NtseException 操作被取消，异常码为NTSE_EC_CANCELED，只在ignoreCancel为false才可能抛出异常
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
		// 要保证写出所有脏页，必须锁住所有脏页
		timeout = -1;
		skipLocked = false;
	} else {
		// 刷写脏页线程可以跳过加不上锁的脏页
		timeout = UNSAFE_LOCK_TIMEOUT;
		skipLocked = true;
	}

	// 收集锁定LRU链表的时间
	uint before = System::fastTime();

	RWLOCK(&m_lock, Shared);
	getDirtyPagesFromList(&dirtyPages, &m_oldGen, file, searchLength, maxPages, skipLocked);
	if (searchLength > m_oldGen.getSize() && dirtyPages.size() < maxPages)
		getDirtyPagesFromList(&dirtyPages, &m_newGen, file, searchLength - m_oldGen.getSize(), maxPages, skipLocked);
	RWUNLOCK(&m_lock, Shared);

	// 做全量flush时需要统计锁定lru链表的时间
	if (searchLength == (u64)-1) {
		m_status.m_flushAllBufferLockTime += (System::fastTime() - before);
		m_status.m_flushAllBufferLockCount++;
	}

	size_t remain = dirtyPages.size(), done = 0, oldDone = 0, skipped = 0;
	u64 time = 0;

	// 按文件和页号排序后，写出每个脏页。排序是为了提高性能
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
	
		// 如果异步io队列已满,等待部分异步IO完成
		if (array->getReservedSlotNum() == AioArray::AIO_BATCH_SIZE) {
			waitWritePageUseAioComplete(session, array, false, scavengerFlush);
		}
__Retry:
		// 尝试加页面latch，超时则立即同步异步IO
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
				// 统计真实写出的页面
				done++;
			} else {
				skipped++;
				m_pool->unlockPage(userId, dirtyPages[i].m_page, Shared);
			}
			remain--;
 
			//检查点需要定时休眠，保证系统吞吐量稳定
			if (flushWorld
				&& (done - oldDone) >= batchSize	// 写完一批了
				&& remain > 0		// 剩下的还有
				&& m_db->getStat() != DB_CLOSING) {
					// 首先等待所有异步IO队列里的请求完成
					waitWritePageUseAioComplete(session, array, true, scavengerFlush);
					// 保证每秒脏页流式写出
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
			// 如果发现trylatch失败则同步之前所有的异步IO请求，释放所有latch，防止latch死锁
			waitWritePageUseAioComplete(session, array, true, scavengerFlush);
			NTSE_ASSERT(array->getReservedSlotNum() == 0);
			// 此时已经释放所有页面的latch，因此下次去取页面latch不用try
			timeout = flushAllPages? -1: UNSAFE_LOCK_TIMEOUT;

			goto __Retry;
		}
	}
	// 异步IO此时要等待全部IO请求返回并且释放页面latch
	waitWritePageUseAioComplete(session, array, true, scavengerFlush);
	NTSE_ASSERT(array->getReservedSlotNum() == 0);
		
	if (flushAllPages) {
		m_syslog->log(EL_LOG, "Flush %s: dirty pages: %d, write time: "I64FORMAT"u, sleep: "I64FORMAT"u ms, skipped: %d.", 
			file? file->getPath(): "all", (uint)dirtyPages.size(), time, totalSleep, (int)skipped);
	}
}

/**
 * 将一个页用异步IO方式写出到文件中
 * @pre 页面已经被锁定
 *
 * @param session 会话，可能为NULL
 * @param page 页面地址
 * @param array 异步IO队列
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
/*			// 如果是清道夫线程，那尝试将页面放回Buffer的FreeList中
			// TODO:此优化测试中表现尚不稳定，暂不开放
			if (freePage) {
				if (m_pool->tryUpgradePageLock(userId, page, __FILE__, __LINE__)) {
					LOCK(&bcb->m_metaLock);
					if (bcb->m_dirty || bcb->m_pinCount > 0){
						UNLOCK(&bcb->m_metaLock);
						m_pool->unlockPage(userId, page, Exclusived);
						continue;
					}
					UNLOCK(&bcb->m_metaLock);
					// 被Xlatch锁定的页面只能unpin，因此此处释放页面是安全的
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
 * 获取下一秒检查点需要刷的页面数量
 */
uint Buffer::getRecommandFlushSize() {
	uint pagesNeedFlush = 0;
	u64 curNtseLogSize = m_db->getTxnlog()->getStatus().m_ntseLogSize;

	uint  curDirtyPages = m_status.m_curDirtyPages.get();
	uint systemIoCapacity = m_db->getConfig()->m_systemIoCapacity;
	uint flushLoop = m_db->getConfig()->m_flushAdjustLoop;
	// 1. 得到之前一段时间的刷脏页的速率平均值和日志产生速率平均值
	if (m_flushStatus.m_iter > flushLoop) {
		// 刷脏页平均速率
		m_flushStatus.m_avgPageRate = (m_flushStatus.m_loopWritePageCnt / flushLoop + 
			m_flushStatus.m_avgPageRate) / 2;
		m_flushStatus.m_loopWritePageCnt = 0;
		
		// ntse日志产生平均速率
		m_flushStatus.m_avgNtseLogRate = (curNtseLogSize - m_flushStatus.m_prevLoopNtseLogSize) / flushLoop;
		m_flushStatus.m_prevLoopNtseLogSize = m_db->getTxnlog()->getStatus().m_ntseLogSize;

		// 脏页产生的平均速率
		m_flushStatus.m_avgDirtyPagesRate = (curDirtyPages - m_flushStatus.m_prevLoopDirtyPages) / flushLoop;
		m_flushStatus.m_prevLoopDirtyPages = curDirtyPages;
		
		m_flushStatus.m_iter = 0;
	}

	// 2. 统计内存buffer脏页比率
	uint pctDirtyPages = 0;
	uint bufferSize = m_db->getConfig()->m_pageBufSize;

	uint curDirtyPagePct = (curDirtyPages * 100) / bufferSize;
	uint maxDirtyPagePct = m_db->getConfig()->m_maxDirtyPagePct;
	uint maxDirtyPagePctLwm = m_db->getConfig()->m_maxDirtyPagePctLwm;
	
	if (maxDirtyPagePctLwm == 0) {
		// 如果未设置脏页比率低水位线
		if (curDirtyPagePct > maxDirtyPagePct)
			pctDirtyPages = 100;
	} else if (curDirtyPagePct > maxDirtyPagePctLwm){
		pctDirtyPages = curDirtyPagePct * 100 / maxDirtyPagePct;
	}
	pagesNeedFlush = (uint)(systemIoCapacity * (double)(pctDirtyPages) / 100.0);

	// 3. 获取当前秒脏页产生量
	int deltaSecDirtyPages = curDirtyPages - m_flushStatus.m_prevSecDirtyPages;
	deltaSecDirtyPages = (deltaSecDirtyPages + m_flushStatus.m_avgDirtyPagesRate) / 2;

	m_flushStatus.m_prevSecDirtyPages = curDirtyPages;
	//	计算修正后应刷页面量，并与第2步求得的应刷页面量取较大值
	int modifyFlushPages = m_flushStatus.m_prevSecFlushPageCnt + deltaSecDirtyPages;
	if (modifyFlushPages < 0)
		modifyFlushPages = 0;
	pagesNeedFlush = max(pagesNeedFlush, (uint)modifyFlushPages);


	// 4. 取刷脏页平均速率和内存脏页比的平均值，并求得与系统IO能力的较小值
	
	pagesNeedFlush = (pagesNeedFlush + m_flushStatus.m_avgPageRate) / 2;
	if (pagesNeedFlush > systemIoCapacity)
		pagesNeedFlush = systemIoCapacity;

	// 5. 为保证推进检查点，所以至少需要用1/10的系统IO刷脏页
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
 * 重置检查点刷脏页的统计信息
 */
void Buffer::resetFlushStatus() {
	memset(&m_flushStatus, 0, sizeof(BufferFlushStatus));
}

/**
 * 从指定页面列表中取出满足条件的脏页
 * @pre 全局锁被锁定
 *
 * @param dirtyPages OUT，存放满足条件的脏页信息
 * @param list 页面列表
 * @param file 感兴趣的页面对应的文件，若为NULL则不考虑文件
 * @param searchLength 搜索list中最多这么多个页面
 * @param maxPages 取出最多这么多个脏页
 * @param skipLocked 是否跳过被锁定的页面
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
 * 从指定页面列表中取出空闲页，即未被pin且非脏的页
 * @pre 全局锁被锁定
 *
 * @param freePages OUT，存放空闲页
 * @param list 页面列表
 * @param maxPages 取出最多这么多个页
 */
void Buffer::getFreePagesFromList(std::vector<KeyPagePair> *freePages, DList<Bcb *> *list, u64 maxPages) {
	assert(m_lock.isLocked(Shared));
	DLink<Bcb *> *curr;
	for (curr = list->getHeader()->getNext();
		curr != list->getHeader(); curr = curr->getNext()) {
		Bcb *bcb = curr->get();
		// 由于这时全局结构锁已经被锁定，引用Bcb对象是安全的
		// 由于没有加Bcb元数据锁，可能得到的信息不是最新的，
		// 真正要释放之前会加锁再判断，不会出错
		if (!bcb->m_dirty && bcb->m_pinCount == 0) {
			freePages->push_back(KeyPagePair(bcb->m_pageKey, bcb->m_page));
			if (freePages->size() >= maxPages)
				return;
		}
	}
}

/**
 * 页面缓存是否使用校验和各来验证页面数据的一致性。
 * 页面缓存默认使用校验和，当使用校验和时，缓存管理模块在写出页面时
 * 将自动根据页面内容计算校验和写出到磁盘中，同时在从磁盘读入页面
 * 时将使用检验和来检测页面数据是否正确。
 *
 * @return 是否使用校验和
 */
bool Buffer::isChecksumed() {
	return m_checksumed;
}

/**
 * 设置页面缓存是否使用校验和
 *
 * @param checksumed 是否使用检验和
 */
void Buffer::setChecksumed(bool checksumed) {
	ftrace(ts.buf, tout << checksumed);
	m_checksumed = checksumed;
}

/**
 * 返回预读区大小，单位为字节数
 *
 * @return 预读区大小
 */
uint Buffer::getPrefetchSize() {
	return m_prefetchSize;
}

/**
 * 设置预读区大小
 *
 * @param prefetchSize 预读区大小，与页大小之商必须为2的整数次幂
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
 * 返回后台写脏页线程处理时间间隔
 *
 * @return 后台写脏页线程处理时间间隔，单位毫秒
 */
uint Buffer::getScavengerInterval() {
	return m_scavenger->getInterval();
}

/**
 * 设置后台写脏页线程处理时间间隔
 *
 * @param interval 后台写脏页线程处理时间间隔，单位毫秒
 */
void Buffer::setScavengerInterval(uint interval) {
	ftrace(ts.buf, tout << interval);
	m_scavenger->setInterval(interval);
}

/**
 * 返回后台写脏页线程每次处理时写出的脏页占缓存大小的最大比例
 *
 * @return 后台写脏页线程每次处理时写出的脏页占缓存大小的最大比例
 */
double Buffer::getScavengerMaxPagesRatio() {
	return  m_scavenger->getMaxPagesRatio();
}

/**
 * 设置后台写脏页线程每次处理时写出的脏页占缓存大小的最大比例
 *
 * @param 后台写脏页线程每次处理时写出的脏页占缓存大小的最大比例，必须在[0, 1]之间
 */
void Buffer::setScavengerMaxPagesRatio(double maxPagesRatio) {
	ftrace(ts.buf, tout << maxPagesRatio);
	m_scavenger->setMaxPagesRatio(maxPagesRatio);
}

/**
 * 返回后台写脏页线程每次处理时扫描的页面数与上次处理以来缓存中读入页面数的比例
 *
 * @return 后台写脏页线程每次处理时扫描的页面数与上次处理以来缓存中读入页面数的比例
 */
double Buffer::getScavengerMultiplier() {
	return m_scavenger->getMultiplier();
}

/**
 * 设置后台写脏页线程每次处理时扫描的页面数与上次处理以来缓存中读入页面数的比例
 *
 * @param 后台写脏页线程每次处理时扫描的页面数与上次处理以来缓存中读入页面数的比例，合理的值应该>1
 */
void Buffer::setScavengerMultiplier(double multiplier) {
	ftrace(ts.buf, tout << multiplier);
	m_scavenger->setMultiplier(multiplier);
}

/**
 * 返回后台写脏页线程，每次写的最多脏页的数量
 *
 */
uint Buffer::getMaxScavengerPages() {
	return m_scavenger->getMaxScavengerPages();
}

/**
 * 设置后台写脏页处理线程，每次写的脏页的最多数量
 *
 */
void Buffer::setMaxScavengerPages(uint maxScavengerPages) {
	m_scavenger->setMaxScavengerPages(maxScavengerPages);
}

/**
 * 返回触发线性预读阈值，含义见文件头说明
 *
 * @return 触发线性预读阈值
 */
double Buffer::getPrefetchRatio() {
	return m_prefetchRatio;
}

/**
 * 设置触发线性预读阈值，含义见文件头说明
 *
 * @param 触发线性预读阈值
 */
void Buffer::setPrefetchRatio(double prefetchRatio) {
	ftrace(ts.buf, tout << prefetchRatio);
	m_prefetchRatio = prefetchRatio;
}

/**
 * 更新页面缓存的扩展统计信息
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
 * 获得页面缓存运行状态
 * 注: 这些运行状态没有进行同步，可能不精确
 * 注: 本函数不会自动更新扩展统计信息，需要调用updateExtendStatus来更新
 *
 * @return 页面缓存运行状态
 */
const BufferStatus& Buffer::getStatus() {
	m_status.m_globalLockUsage = m_lock.getUsage();
	m_status.m_realScavengeCnt = m_scavenger->getRealScavengeCnt();
	return m_status;
}

/** 打印页面缓存运行状态
 * 注: 本函数不会自动更新扩展统计信息，需要调用updateExtendStatus来更新
 * @param out 输出流
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

/** 页面缓存扫描句柄 */
struct BufScanHandle {
	PoolScanHandle	*m_poolScan;	/** 内存页池扫描句柄 */
	File			*m_file;		/** 只返回本文件的缓存页，若为NULL返回所有页 */

	BufScanHandle(PoolScanHandle *poolScan, File *file) {
		m_poolScan = poolScan;
		m_file = file;
	}
};

/**
 * 开始遍历缓存中的页面，注意由于遍历时不锁定缓存全局结构，遍历结果可能不太精确
 *
 * @param userId 用户ID
 * @param file 只返回属于指定文件的缓存页，若为NULL则返回所有缓存页
 * @return 扫描句柄
 */
BufScanHandle* Buffer::beginScan(u16 userId, File *file) {
	return new BufScanHandle(m_pool->beginScan(this, userId), file);
}

/**
 * 得到下一个缓存页
 * @post 返回的页已经用共享锁锁定，上次返回的页面上的锁已经被释放
 *
 * @param h 扫描句柄
 * @return 缓存控制块，若没有则返回NULL
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

/** 释放当前页
 *
 * @param h 扫描句柄
 */
void Buffer::releaseCurrent(BufScanHandle *h) {
	m_pool->releaseCurrent(h->m_poolScan);
}

/**
 * 结束扫描，若当前页面不为NULL则自动放锁
 * @post 扫描句柄对象已经被delete
 *
 * @param h 扫描句柄
 */
void Buffer::endScan(BufScanHandle *h) {
	m_pool->endScan(h->m_poolScan);
	delete h;
}

/**
 * 尝试分配一个块。
 * @pre 调用者已经锁定全局结构互斥锁
 * @post 成功时返回的页已经被用互斥锁锁定
 *
 * @param userId 用户ID
 * @param file 文件
 * @param pageType 页面类型
 * @param pageId 页号
 * @return 成功时返回控制块，失败返回NULL
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
		// 文件对应的所有页面链在一起
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
 * 分配到一个Bcb之后进行读取数据等操作
 * @pre Bcb对应的页面已经用互斥锁锁定
 *
 * @param session 会话，不能为NULL
 * @param readFromFile 是否读取页面数据
 * @param lockMode 锁模式
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
		// 由于页面已经被pin住，不用担心消失
		assert(bcb->m_pinCount > 0);
		m_pool->unlockPage(session->getId(), bcb->m_page, Exclusived);
		m_pool->lockPage(session->getId(), bcb->m_page, Shared, __FILE__, __LINE__);
	}
	assert(m_pool->isPageLocked(bcb->m_page, lockMode));
}

/**
 * 搜索指定的控制块链表，替换其中某个pin次数为0且非脏的页面
 * @pre 已经加了全局结构互斥锁
 * @post 若替换成功，则被替换的页已经加了互斥锁，并且已经从所有全局结构中删除
 *
 * @param userId 用户ID
 * @param list 控制块链表
 * @return 替换成功返回被替换页的控制块，找不到可以替换的页返回NULL
 */
Bcb* Buffer::replaceInList(u16 userId, DList<Bcb *> *list) {
	assert(m_lock.isLocked(Exclusived));

	u64		searchLen = 0;
	bool	alreadySignaled = false;

	m_status.m_replaceSearches++;

	DLink<Bcb *> *e;
	for (e = list->getHeader()->getNext(); e != list->getHeader(); e = e->getNext()) {
		
		searchLen++;
		// 当前search length超过200，唤醒Scavenger线程
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
 * 将一个页面从缓存中删除
 * @pre 页面已经用互斥锁锁定
 * @pre 页面不能被pin住
 *
 * @param userId 用户ID
 * @param bcb 页面控制块
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
 * 读取一个页面数据
 * @pre 页面已经被用互斥锁锁定
 *
 * @param session 会话，不允许为NULL
 * @param bcb 控制块
 * @param prefetch 是否是预读
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
		// 校验和为CHECKSUM_NO表示上次写入时没有使用校验和
		// 校验和为CHECKSUM_DISABLED表示不使用校验和
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
 * 检查是否需要进行预读，需要时发出预读请求
 *
 * @param userId 用户ID
 * @param bcb 刚刚读入的页
 */
void Buffer::checkPrefetch(u16 userId, Bcb *bcb) {
	ftrace(ts.buf, tout << userId << bcb);
	uint prefetchPages = m_prefetchPages;		// 防止在以下执行过程中m_prefetchPages被修改，先缓存之
	u64 mod = bcb->m_pageKey.m_pageId % prefetchPages;
	if (mod == prefetchPages - PREFETCH_AHEAD - 1) {
		u64 areaStart, areaEnd;
		areaEnd = bcb->m_pageKey.m_pageId;
		areaStart = areaEnd / prefetchPages * prefetchPages;

		// 首先检查一下可能要预读的区域是否存在
		u64 fileSize = 0;
		bcb->m_pageKey.m_file->getSize(&fileSize);
		if ((areaStart + prefetchPages * 2) * m_pool->getPageSize() > fileSize)
			return;

		// 统计当前预读区中有多少页面在缓存中，且读取顺序是否与预读顺序大致一致
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
			// 准备发出预读请求，为不在缓存中的页面分配好控制块并且锁定这些块
			areaStart += prefetchPages;
			areaEnd = areaStart + prefetchPages - 1;

			PrefetchRequest *request = m_prefetcher->preparePrefetch(userId, bcb->m_pageKey.m_file);
			if (!request)
				return;

			bool allocBlockFailed = false;
			for (u64 pageId = areaStart; pageId <= areaEnd; pageId++) {
				// 一般来说要预读的数据通常不在缓存中，直接加互斥锁
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
 * 预读指定的页面
 * @pre pagesToRead中所有页面已经用互斥锁锁定并且pin
 * @post pagesToRead中所有页面已经解除锁定和pin
 *
 * @param session 会话，可以为NULL
 * @param userId 请求预读的用户ID
 * @param pagesToRead 要预读的页面
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

		// 统计需要读取的页面数
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

				// 不在缓存中的页面还比较多，批量读取以提高性能
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
				// 将物理读计到本批读取的第一个页面头上，可能不精确，如果本批页面中分属于不同的数据库对象，
				// 比如有些属于索引全局数据，有些属于某个索引的数据
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
				// 绝大部分页面已经在缓存中了，只读取那些不在缓存中的页
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
			// 忽略预读时的错误
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
 * 取消预读
 *
 * @param userId 用户ID
 * @param pagesToRead 要预读的页面
 */
void Buffer::canclePrefetch(u16 userId, std::vector<Bcb *> *pagesToRead) {
	ftrace(ts.buf, tout << userId << pagesToRead);
	for (size_t i = 0; i < pagesToRead->size(); i++) {
		unpinBlock(pagesToRead->at(i));
		freeBlock(userId, pagesToRead->at(i));
	}
}

/**
 * 增加一个页面的pin次数，必要时调整LRU链表
 * @pre 没有锁定结构锁
 *
 * @param userId 用户ID
 * @param bcb 控制块
 * @param touch 是否是对页面的有效访问
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
 * 释放一个页上的pin
 * 指定页可能没有被锁定
 *
 * @param bcb 页面控制块
 */
void Buffer::unpinBlock(Bcb *bcb) {
	ftrace(ts.buf, tout << bcb);
	LOCK(&bcb->m_metaLock);
	assert(bcb->m_pinCount > 0);
	bcb->m_pinCount--;
	UNLOCK(&bcb->m_metaLock);
}

/**
 * 访问一个页面时调用本函数，必要时记录页面访问时间并调整LRU
 *
 * @param userId 用户ID
 * @param bcb 控制块
 */
void Buffer::touchBlock(u16 userId, Bcb *bcb) {
	ftrace(ts.buf, tout << userId << bcb);
	bool firstTouch = bcb->m_readSeq == 0;
	if (firstTouch) {	// 预读进来还没有访问过的页面
		bcb->m_readSeq = ++m_readSeq;
		checkPrefetch(userId, bcb);
	}
	if (firstTouch || m_readSeq - bcb->m_lruSeq >= m_correlatedRefPeriod) {
		// 非相关性访问，调整LRU链表，如果原来在老分代则提升到新分代
		// 若如果是预读进来的页面被第一次访问，则重新插入到老分代头
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
			// 调整老分代占用的比例，为减少调整次数，允许老分代的实际大小
			// 与配置之间有OLD_GEN_SHIFT个页面以内的偏差
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


/** 禁用后台刷脏页线程 */
void Buffer::disableScavenger() {
	ftrace(ts.buf, );
	m_scavenger->disable();
}

/** 启用后台刷脏页线程 */
void Buffer::enableScavenger() {
	ftrace(ts.buf, );
	m_scavenger->enable();
}


/**
 * 给缓存页计算校验和。本校验和的主要用途是检测页面被部分写入时导致的
 * 数据不一致性，而不是防止人为对数据进行的篡改。因此关注的是性能而非
 * 被伪造的可能性。
 *
 * @param page 缓存页
 * @return 检验和
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
 * 在开启扩展验证功能时，同步页面数据与checksum，使得将来可以检测
 * checksum正确性
 * @pre bcb对应的页面已经被用指定的锁锁定
 *
 * @param bcb 页面控制块
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
	// 加读锁时有可能有多个人都来计算checksum，只影响一点性能
	// 不影响正确性，原因是这些人计算出来的checksum一定是一样的
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
 * 在开启扩展验证功能时，根据checksum验证页面有没有错误的被修改。
 * @pre 页面已经被用指定的锁模式锁定
 *
 * @param page 页面
 * @param lockMode 对页面所加的锁
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
				// 加读锁，或加了写锁，但没有调用markDirty时，页面数据
				// 不应该被修改
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

/** 替换页面的统计对象
 * @param bcb 页面控制块
 * @param dbObjStats 数据库对象级统计结构
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
 * 唤醒Scavenger线程
 */
void Buffer::signalScavenger() {
	m_scavenger->signal();
}

/**
 * 构造函数
 *
 * @param db 所属数据库，可以为NULL
 * @param buffer 页面缓存
 * @param queueSize 预读队列大小
 * @param syslog 系统日志
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
 * 析构函数
 */
Prefetcher::~Prefetcher() {
	delete[] m_queue;
}

/**
 * 预读处理函数。检查有无需要处理的预读请求，有则处理之
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
 * 通知预读处理线程取消对指定文件的预读
 * @post 对指定文件已经进行的预读已经完成，未处理的预读请求被取消
 *
 * @param file 文件
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
 * 准备进行预读。分配预读请求项
 * @post 返回的预读请求项已经被锁定
 *
 * @param userId 用户ID
 * @param file 将要从这个文件预读
 * @return 分配成功返回预读请求项，失败返回NULL
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
 * 提交预读请求
 * @pre request调用prepareRequest分配
 *
 * @param request 预读请求
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
 * 取消预读请求
 * @pre request调用prepareRequest分配
 *
 * @param request 预读请求
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
 * 进行预读
 * @pre 要处理的预读请求对象已经被锁定
 * @post 预读请求对象已解锁
 *
 * @request 预读请求
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
 * 构造函数
 *
 * @param db 数据库。可能为NULL
 * @param buffer 页面缓存
 * @param interval 启动周期
 * @param maxCleanPagesRatio LRU链表尾部Clean Pages占整个LRU链表长度的比例
 * @param maxScavengerPages	 一次清理，最多写出的脏页数量
 */
Scavenger::Scavenger(Database *db, Buffer *buffer, uint interval, double maxCleanPagesRatio, uint maxScavengerPages):
	BgTask(db, "Buffer::Scavenger", interval, true, -1, true) {
	m_buffer = buffer;
	m_maxCleanPagesRatio = maxCleanPagesRatio;
	m_enabled = true;

	// 根据m_maxCleanPageRatio，计算m_maxCleanPagesLen
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
 * 刷写脏页处理函数
 */
void Scavenger::runIt() {
	ftrace(ts.buf, );
	if (!m_enabled) {
		nftrace(ts.buf, tout << "Scavenger has been disabled.";);
		return;
	}
	uint numFreePages = m_buffer->getNumFreePages() + m_buffer->getPool()->getNumFreePages();
	// 只要当前相对精确的空闲页可以满足大致需要请求的页面数，就不需要清理脏页。详见QA38615
 	if (m_maxCleanPagesLen < numFreePages)
 		return;


	m_realScavengeCnt++;
	/** Scavenger每次Flush的Dirty Pages数量上限，可根据系统I/O的能力调整，默认为每次200 */
#ifndef WIN32
	if (m_useAio)
		m_buffer->flushDirtyPagesUseAio(m_session, NULL, m_maxCleanPagesLen, m_maxScavengerPages, true, &m_aioArray);
	else
#endif
		m_buffer->flushDirtyPages(m_session, NULL, m_maxCleanPagesLen, m_maxScavengerPages, true);
}

#else
/**
 * 构造函数
 *
 * @param db 数据库。可能为NULL
 * @param buffer 页面缓存
 * @param interval 启动周期
 * @param maxPagesRatio 每次写的脏页不超过缓存的这一比例
 * @param multiplier 每次写的脏页与上次运行以来缓存中新加入页面的比例
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
 * 刷写脏页处理函数
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
	// 只要当前相对精确的空闲页可以满足大致需要请求的页面数，就不需要清理脏页。详见QA38615
	if (searchLength < numFreePages)
		return;
	u64 maxPages = (u64)(m_buffer->getCurrentSize() * m_maxPagesRatio);
	if (maxPages < Buffer::SCAVENGER_MIN_PAGES)
		maxPages = Buffer::SCAVENGER_MIN_PAGES;

	m_buffer->flushDirtyPages(m_session, NULL, searchLength, maxPages);
}

#endif

/**
 * 析构函数
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
 * 获取一次清理时最多写出的脏页占缓存大小之比
 *
 * @return 一次清理时最多写出的脏页占缓存大小之比
 */
double Scavenger::getMaxPagesRatio() {
	return m_maxPagesRatio;
}

/**
 * 设置一次清理时最多写出的脏页占缓存大小之比
 *
 * @param maxPagesRatio 一次清理时最多写出的脏页占缓存大小之比，必须在[0,1]之间
 */
void Scavenger::setMaxPagesRatio(double maxPagesRatio) {
	ftrace(ts.buf, tout << m_maxPagesRatio);
	if (maxPagesRatio < 0 || maxPagesRatio > 1.0)
		return;
	m_maxPagesRatio = maxPagesRatio;
}

/**
 * 获取放大系数(见页首说明)
 *
 * @return 放大系数
 */
double Scavenger::getMultiplier() {
	return m_multiplier;
}

/**
 * 设置放大系数
 *
 * @param multiplier 放大系数，必须>=0
 */
void Scavenger::setMultiplier(double multiplier) {
	ftrace(ts.buf, tout << multiplier);
	if (multiplier < 0)
		return;
	m_multiplier = multiplier;
}

/**
 * 返回每次写的最大脏页数量
 *
 */
uint Scavenger::getMaxScavengerPages() {
	return m_maxScavengerPages;
}

/**
 * 设置每次写的最大脏页数量
 *
 */
void Scavenger::setMaxScavengerPages(uint maxScavengerPages) {
	m_maxScavengerPages = maxScavengerPages;
}

/**
 * 返回清道夫真正工作的次数
 *
 */
u64 Scavenger::getRealScavengeCnt() {
	return m_realScavengeCnt;
}

/** 禁止后台写脏页线程 */
void Scavenger::disable() {
	ftrace(ts.buf, );
	m_enabled = false;
}

/** 启用后台写脏页线程 */
void Scavenger::enable() {
	ftrace(ts.buf, );
	m_enabled = true;
}

///////////////////////////////////////////////////

/** 构造长度为指定字节数的IoBuffer */
IoBuffer::IoBuffer(uint pageCnt, uint pageSize) 
: m_pageSize(pageSize), m_size(pageCnt * pageSize) {
	m_data = (byte *)System::virtualAlloc(m_size);
}

IoBuffer::~IoBuffer() {
	if (m_data) {
		System::virtualFree(m_data);
	}
}

/** 获取缓存地址并加锁 */
byte* IoBuffer::getBuffer() {
	assert(m_data);		
	return m_data;
}

/** 缓存长度 */
size_t IoBuffer::getSize() const {
	return m_size;
}

/** 页数 */
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
