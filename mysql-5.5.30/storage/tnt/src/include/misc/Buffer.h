/**
 * 页面缓存管理
 *
 * @author 汪源(wangyuan@corp.netease.com, wy@163.org)
 */

#ifndef _NTSE_BUFFER_H_
#define _NTSE_BUFFER_H_

#include <iostream>
#include "misc/Global.h"
#include "util/PagePool.h"
#include "util/System.h"
#include "util/Hash.h"
#include "misc/Trace.h"
#include "misc/Verify.h"
#include "misc/ResourcePool.h"
#include <vector>
#include <set>

using namespace std;

#ifdef NTSE_UNIT_TEST
class BufferTestCase;
class BufferBigTest;
#endif

namespace ntse {

#pragma pack(1)
/** 缓存页结构 */
struct BufferPageHdr {
	u64	m_lsn;		/** 该页上最后一次修改操作对应的日志LSN */
	u32	m_checksum;	/** 页校验和，用于检验页内数据是否被破坏，为0表示不使用检验和 */
	/** 表示对页面不使用checksum，系统在任何时候都不会去更新或验证其
	 * checksum，目前只用于变长堆的中央位图页。
	 */
	static const u32 CHECKSUM_DISABLED = 0xFFFFFFFF;
	/** 表示页面没有计算校验和，但系统在需要的时候会更新其检验和。
	 */
	static const u32 CHECKSUM_NO = 0;
};
#pragma pack()

class File;
/** 一个缓存页的唯一标识 */
struct PageKey {
	File		*m_file;		/** 页所属文件 */
	PageType	m_pageType;		/** 页面类型 */
	u64			m_pageId;		/** 页号 */

	/** 提供默认构造函数，为了能在一些模板容器类中使用 */
	PageKey() {
	}

	/** 构造一个PageKey对象
	 *
	 * @param file 文件
	 * @param pageType 页面类型
	 * @param pageId 页号
	 */
	PageKey(File *file, PageType pageType, u64 pageId) {
		m_file = file;
		m_pageType = pageType;
		m_pageId = pageId;
	}
};

struct DBObjStats;

/** 缓存控制块Buffer control block，每个缓存页的控制信息
 * 注意每个BCB中不需要包含页面锁，页面锁定功能由内存页池模块提供
 */
struct Bcb {
	Mutex		m_metaLock;		/** 用于保证对本控制块信息的并发访问 */
	PageKey		m_pageKey;		/** 唯一标识 */
	bool		m_dirty;		/** 是否是脏页 */
	bool		m_isWriting;	/** 是否正在被写出*/
	uint		m_pinCount;		/** 被pin的次数 */
	BufferPageHdr	*m_page;	/** 页面数据 */
	size_t		m_poolEntry;	/** 在缓存控制块池中的入口项 */
	DLink<Bcb *>	m_lruLink;	/** 在LRU链表中的入口项 */
	DLink<Bcb *>	m_link;		/** 在所有页面链表中的入口项 */
	DLink<Bcb *>	m_fileLink;	/** 在文件页面列表的入口项 */
	u64			m_readSeq;		/** 页面被读入的次序，预读的时候不设置，只在应用真正访问时才设置。
								 * 页面读入次序信息在预读检测时使用
								 */
	u64			m_lruSeq;		/** 上次调整LRU链表位置时的次序 */

	DBObjStats *m_dbObjStats;	/** 数据对象统计结构 */
#ifdef NTSE_VERIFY_EX
	bool		m_checksumValid;/** 检验和是否与数据一致 */
	bool		m_dirtyMarked;	/** 在加/放锁之间是否调用了markDirty */
#endif

	/** 无参构造函数仅被对象池用于初始化新分配的对象 */
	Bcb(): m_metaLock("Bcb::metaLock", __FILE__, __LINE__){
			// 不带参数的构造函数仅被对象池用于初始化新分配的对象
			init(NULL, PAGE_EMPTY, INVALID_PAGE_ID);
	}
	/**
	 * 构造一个缓存控制块
	 *
	 * @param file 对应页面所属的文件
	 * @param pageType 对应页面的类型
	 * @param pageId 对应页面的页号
	 */
	void init(File *file, PageType pageType, u64 pageId) {
		m_pageKey.m_file = file;
		m_pageKey.m_pageType = pageType;
		m_pageKey.m_pageId = pageId;
		m_dirty = false;
		m_isWriting = false;
		m_pinCount = 0;
		m_link.set(this);
		m_lruLink.set(this);
		m_fileLink.set(this);
		m_readSeq = 0;
#ifdef NTSE_VERIFY_EX
		m_checksumValid = false;
		m_dirtyMarked = false;
#endif
		m_dbObjStats = NULL;
	}
};

/** 根据缓存控制块计算哈希值的函数对象 */
class BcbHasher {
public:
	/**
	 * 计算哈希值，BCB的哈希值由对应页所属文件和页号确定
	 *
	 * @param bcb 缓存控制块
	 * @return 哈希值
	 */
	inline unsigned int operator()(const Bcb *bcb) const {
		return (unsigned int)((long)(u64)bcb->m_pageKey.m_file ^ (long)(bcb->m_pageKey.m_pageId));
	}
};

/** 根据缓存页标识计算哈希值的函数对象 */
class PageKeyHasher {
public:
	/**
	 * 计算哈希值
	 *
	 * @param pageKey 缓存页标识
	 * @return 哈希值
	 */
	inline unsigned int operator()(const PageKey *pageKey) const {
		return (unsigned int)((long)(u64)pageKey->m_file ^ (long)(pageKey->m_pageId));
	}
};

/** 比较缓存控制块对应的页面是否与指定页面标识为同一页面的函数对象 */
class BcbKeyEqualer {
public:
	/**
	 * 判断缓存页标识和缓存控制块是否对应同一缓存页
	 *
	 * @param pageKey 缓存页标识
	 * @param bcb 缓存控制块
	 * @return 缓存页标识和缓存控制块是否对应同一缓存页
	 */
	inline bool operator()(const PageKey *pageKey, const Bcb *bcb) const {
		return bcb->m_pageKey.m_file == pageKey->m_file && bcb->m_pageKey.m_pageId == pageKey->m_pageId;
	}
};

#pragma pack(4)
/** 包含页面标识和页面数据的二元组 */
struct KeyPagePair {
	PageKey			m_pageKey;	/** 页面标识 */
	BufferPageHdr	*m_page;	/** 页面数据 */

	/**
	 * 构造函数
	 *
	 * @param pageKey 页面标识
	 * @param page 页面数据
	 */
	KeyPagePair(const PageKey &pageKey, BufferPageHdr *page):
		m_pageKey(pageKey.m_file, pageKey.m_pageType, pageKey.m_pageId) {
		m_page = page;
	}

	/**
	 * 比较大小
	 *
	 * @param another 另一个KeyPagePair对象
	 * @return 我是否小于another
	 */
	bool operator < (const KeyPagePair &another) const {
		if (m_pageKey.m_file == another.m_pageKey.m_file)
			return m_pageKey.m_pageId < another.m_pageKey.m_pageId;
		else
			return m_pageKey.m_file < another.m_pageKey.m_file;
	}
};
#pragma pack()

/** 页面缓存扩展使用状态，扩展使用状态不是实时统计，而是按需计算，
 * 计算时具有一定的开销。
 */
struct BufferStatusEx {
	u64		m_dirtyPages;			/** 脏页数 */
	u64		m_pinnedPages;			/** pin住的页面个数 */
	u64		m_rlockedPages;			/** 用共享锁锁住的页面个数 */
	u64		m_wlockedPages;			/** 用互斥锁锁住的页面个数 */
	double	m_avgHashConflict;		/** 页面哈希表平均冲突链表长度 */
	u64		m_maxHashConflict;		/** 页面哈希表最大冲突链表长度 */
};

/** 页面缓存使用状态 */
struct BufferStatus {
	u64 		m_logicalReads;		/** 逻辑读操作次数 */
	u64 		m_physicalReads;	/** 物理读操作次数，包括预读的 */
	u64 		m_logicalWrites;	/** 逻辑写操作次数 */
	u64 		m_physicalWrites;	/** 物理写操作次数，包括scavengerWrites/flushWrites */
	u64 		m_scavengerWrites;	/** 后台刷脏页线程写的页面数 */
	u64 		m_flushWrites;		/** 刷写所有脏页（检查点）时写出的脏页数 */
	u64			m_extendWrites; /** 堆扩展时写出的脏页数目 */
	u64 		m_prefetches;		/** 预读次数 */
	u64			m_batchPrefetch;	/** 批量预读次数 */
	u64			m_nonbatchPrefetch;	/** 非批量预读次数 */
	u64			m_prefetchPages;	/** 预读读入的页面数 */
	u64			m_pageCreates;		/** 新建页面数，包括读入页面或创建页面 */
	u64			m_allocBlockFail;	/** 分配页面失败，该值过高可能表示清道夫工作不够积极 */
	u64			m_replaceSearches;	/** 页面替换搜索LRU链表次数 */
	u64			m_replaceSearchLen;	/** 页面替换搜索LRU链表总长度 */
	u64			m_unsafeLockFails;	/** 不安全的加锁，即lockPageIfType失败次数 */
	u64         m_firstTouch;       /** 当页面刚加载进buffer引起的lru链表改变 */
	u64         m_laterTouch;       /** 当页面在过m_correlatedRefPeriod次后引起的lru链表改变 */
	u64			m_flushAllBufferLockCount; /** 全量flush遍历buffer收集脏页的计数，包括检查点和realclose表 */
	u64			m_flushAllBufferLockTime;  /** 全量flush遍历buffer收集脏页的总时间 */
	Atomic<long>	m_pendingReads;	/** 正在进行中的读操作个数 */
	Atomic<long>	m_pendingWrites;/** 正在进行中的写操作个数 */
	Atomic<long>	m_pendingAllocs;/** 正在进行中的分配页面的线程数 */
	u64			m_readTime;			/** 读数据所用时间，单位毫秒 */
	u64			m_writeTime;		/** 写数据所用时间，单位毫秒 */
	BufferStatusEx	m_statusEx;		/** 扩展统计信息 */
	const RWLockUsage	*m_globalLockUsage;	/** 全局锁使用统计信息 */
	u64			m_realScavengeCnt;	 /** 清道夫真正刷页面的次数 */
	Atomic<long>	m_curDirtyPages;/** 系统中脏页面数 */
};



struct BufferFlushStatus {
	uint		m_iter;					/** 系统检查点刷脏页周期 */
	u64			m_prevLoopNtseLogSize;	/** 上一个循环时ntselog大小 */
	u64			m_avgNtseLogRate;		/** 平均的产生NTSE日志的速率 */
	int			m_avgDirtyPagesRate;	/** 平均的产生脏页的速率 */
	uint		m_loopWritePageCnt;		/** 当前循环中刷脏页的数目 */
	uint		m_avgPageRate;			/** 本循环中的平均刷脏页速率 */
	uint		m_prevLoopDirtyPages;	/** 前一个循环时系统脏页量 */
	uint		m_prevSecDirtyPages;	/** 前一秒的系统脏页量*/
	uint		m_prevSecFlushPageCnt;	/** 前一秒刷脏页的量 */
};


///////////////////////////////////////////////

typedef class ResourceUser IoBufferUser;

/** 管理一段VirtualAlloc出来的内存空间 */
class IoBuffer : public Resource {
public:
	/** 构造长度为指定字节数的IoBuffer */
	IoBuffer(uint pageCnt, uint pageSize);
	virtual ~IoBuffer();
	/** 获取缓存地址并加锁 */
	byte* getBuffer();
	/** 缓存长度 */
	size_t getSize() const;
	/** 页数 */
	uint getPageCnt() const;

private:
	uint   m_pageSize; /** 缓存页面大小 */
	byte  *m_data;     /** 内存区 */
	size_t m_size;     /** 缓存大小 */
};

class BatchIoBufPool : public Pool {
public:
	static const uint DFL_INSTANCE_NUM = 4;
#ifdef NTSE_UNIT_TEST
	static const uint DFL_PAGE_CNT = 8;
#else
	static const uint DFL_PAGE_CNT = 128;
#endif
	static const uint DFL_PAGE_SIZE = Limits::PAGE_SIZE;

public:
	BatchIoBufPool(uint instNum = DFL_INSTANCE_NUM, uint pageCntPerInst = DFL_PAGE_CNT, 
		uint pageSize = DFL_PAGE_SIZE);
	~BatchIoBufPool() {}
};

///////////////////////////////////////////////

/** 描述文件对应的页面链表 */
class FilePageList {
public:
	FilePageList() :m_file(NULL), m_pendingFlushCount(0) {}

	inline void add(Bcb *bcb) {
		assert(bcb->m_pageKey.m_file == m_file);
		m_bcbList.addLast(&bcb->m_fileLink);
	}

	inline File* getFile() const {
		return m_file;
	}

	inline void setFile(File *file) {
		m_file = file;
	}

	inline DList<Bcb *>* getBcbList() {
		return &m_bcbList;
	}

	inline int getPendingFlushCount() {
		return m_pendingFlushCount;
	}

	void incPendingFlushCount() {
		++m_pendingFlushCount;
	}
	
	void decPendingFlushCount() {
		--m_pendingFlushCount;
	}
private:
	DList<Bcb *> m_bcbList;		/** 控制块链表 */
	File *m_file;				/** 文件指针*/
	int m_pendingFlushCount;
};


/** File以及FilePageList的哈希函数 */
class FileHasher {
public:
	/**
	 * 计算File哈希值
	 *
	 * @param file
	 * @return 哈希值
	 */
	inline unsigned int operator()(const File *file) const {
		return (unsigned int)reinterpret_cast<long>(file);
	}

	/**
	 * 计算File哈希值
	 *
	 * @param file
	 * @return 哈希值
	 */
	inline unsigned int operator()(const FilePageList *pageList) const {
		return (unsigned int)reinterpret_cast<long>(pageList->getFile());
	}
};


class FilePageListEqualer {
public:
	/**
	 * 判断页面链表是否属于文件
	 *
	 * @param file 文件指针
	 * @param pageList 页面链表
	 * @return 页面链表是否属于文件
	 */
	inline bool operator()(const File *file, const FilePageList *pageList) const {
		return pageList->getFile() == file;
	}
};

struct BufScanHandle;
class Scavenger;
class Prefetcher;
class File;
class AioArray;
class Syslog;
class Session;
class Txnlog;
class Database;
struct DBObjStats;
/** 页面缓存 */
class Buffer: public PagePoolUser {
public:
	Buffer(Database *db, uint numPages, PagePool *pagePool, Syslog *syslog, Txnlog *txnlog);
	~Buffer();
	virtual uint freeSomePages(u16 userId, uint numPages);
	BufferPageHdr* getPage(Session *session, File *file, PageType pageType, u64 pageId, LockMode lockMode, DBObjStats* dbObjStats, BufferPageHdr *guess = NULL);
	BufferPageHdr* newPage(Session *session, File *file, PageType pageType, u64 pageId, LockMode lockMode, DBObjStats* dbObjStats);
	BufferPageHdr* tryGetPage(Session *session, File *file, PageType pageType, u64 pageId, LockMode lockMode, DBObjStats* dbObjStats);
	void releasePage(Session *session, BufferPageHdr *page, LockMode lockMode);

	/**
	 * 锁定一个页
	 *
	 * @param userId 用户ID
	 * @param page 页面地址
	 * @param lockMode 锁模式
	 * @param touch 是否记录访问时间并考虑调整LRU
	 */
	inline void lockPage(u16 userId, BufferPageHdr *page, LockMode lockMode, bool touch) {
		ftrace(ts.buf, tout << userId << (Bcb *)m_pool->getInfo(page) << lockMode << touch);
		m_pool->lockPage(userId, page, lockMode, __FILE__, __LINE__);
#ifdef NTSE_VERIFY_EX
		syncChecksum((Bcb *)m_pool->getInfo(page), lockMode);
#endif
		if (touch)
			touchBlock(userId, (Bcb *)m_pool->getInfo(page));
	}

	/**
	 * 释放一个页上的锁，但不释放pin
	 *
	 * @param userId 用户ID
	 * @param page 页面地址
	 * @param lockMode 锁模式
	 */
	inline void unlockPage(u16 userId, BufferPageHdr *page, LockMode lockMode) {
		ftrace(ts.buf, tout << userId << (Bcb *)m_pool->getInfo(page) << lockMode);
#ifdef NTSE_VERIFY_EX
		verifyChecksum(page, lockMode);
#endif
		m_pool->unlockPage(userId, page, lockMode);
	}

	void upgradePageLock(Session *session, BufferPageHdr *page);
	void unpinPage(BufferPageHdr *page);
	void markDirty(Session *session, BufferPageHdr *page);
	bool isDirty(BufferPageHdr *page);
	void writePage(Session *session, BufferPageHdr *page);
	void batchWrite(Session *session, File *file, PageType pageType, u64 minPid, u64 maxPid);
	void freePages(Session *session, File *file, bool writeDirty);
	void freePages(Session *session, File *file, uint indexId, bool (*fn)(BufferPageHdr *page, PageId pageId, uint indexId));
	void flushAll(Session *session = NULL) throw(NtseException);
#ifndef WIN32
	bool writePageUseAio(Session *session, BufferPageHdr *page, AioArray *array);
	void waitWritePageUseAioComplete(Session *session, AioArray* array, bool waitAll, bool freePage);
	void flushAllUseAio(Session *session, AioArray *array) throw(NtseException);
	void flushDirtyPagesUseAio(Session *session, File *file, u64 searchLength = (u64)-1,
		u64 maxPages = (u64)-1, bool ignoreCancel = true, AioArray *array = NULL) throw(NtseException);
#endif
	void flushDirtyPages(Session *session, File *file, u64 searchLength = (u64)-1,
		u64 maxPages = (u64)-1, bool ignoreCancel = true) throw(NtseException);
	void prefetchPages(Session *session, u16 userId, std::vector<Bcb *> *pagesToRead);
	void canclePrefetch(u16 userId, std::vector<Bcb *> *pagesToRead);
	u32 checksumPage(BufferPageHdr *page);
	bool hasPage(File *file, PageType pageType, u64 pageId);
	static BatchIoBufPool* getBatchIoBufferPool();

	void disableScavenger();
	void enableScavenger();

	/////////////////////////////////////////////////////////////////////////
	// 这些是配置参数                                             //
	/////////////////////////////////////////////////////////////////////////
	bool isChecksumed();
	void setChecksumed(bool checksumed);
	uint getPrefetchSize();
	void setPrefetchSize(uint prefetchSize);
	double getPrefetchRatio();
	void setPrefetchRatio(double prefetchRatio);
	uint getScavengerInterval();
	void setScavengerInterval(uint interval);
	double getScavengerMaxPagesRatio();
	void setScavengerMaxPagesRatio(double maxPagesRatio);
	double getScavengerMultiplier();
	void setScavengerMultiplier(double multiplier);
	uint getMaxScavengerPages();
	void setMaxScavengerPages(uint maxScavengerPages);
	/////////////////////////////////////////////////////////////////////////
	// 这些是运行状态                                             //
	/////////////////////////////////////////////////////////////////////////
	void updateExtendStatus();
	uint getRecommandFlushSize();
	void resetFlushStatus();
	const BufferStatus& getStatus();
	void printStatus(ostream &out) const;
	BufScanHandle* beginScan(u16 userId, File *file);
	const Bcb* getNext(BufScanHandle *h);
	void releaseCurrent(BufScanHandle *h);
	void endScan(BufScanHandle *h);

	void signalScavenger();


private:
	BufferPageHdr* doGetPage(Session *session, File *file, PageType pageType, u64 pageId, LockMode lockMode, bool readFromFile, DBObjStats *dbObjStats);
	Bcb* tryAllocBlock(u16 userId, File *file, PageType pageType, u64 pageId);
	void postAlloc(Session *session, Bcb *bcb, bool readFromFile, LockMode lockMode);
	Bcb* replaceInList(u16 userId, DList<Bcb *> *list);
	void freeBlock(u16 userId, Bcb *bcb);
	void readBlock(Session *session, Bcb *bcb, bool prefetch);
	void pinBlock(u16 userId, Bcb *bcb, bool touch);
	void unpinBlock(Bcb *bcb);
	void touchBlock(u16 userId, Bcb *bcb);
	void checkPrefetch(u16 userId, Bcb *bcb);
	void getDirtyPagesFromList(std::vector<KeyPagePair> *dirtyPages, DList<Bcb *> *list, File *file, u64 searchLength, u64 maxPages, bool skipLocked);
	void getFreePagesFromList(std::vector<KeyPagePair> *freePages, DList<Bcb *> *list, u64 maxPages);
	void getDirtyPagesOfFile(vector<KeyPagePair> *dirtyPages, File *file, u64 searchLength, u64 maxPages, bool skipLocked);
	void syncChecksum(Bcb *bcb, LockMode lockMode);
	void verifyChecksum(BufferPageHdr *page, LockMode lockMode);
	void updateChecksum(BufferPageHdr *page);
	void batchWritePages(Session *session, File *file, u64 startPid, std::vector<Bcb *>* bcbs);
	void replaceDBObjStats(Bcb *bcb, DBObjStats *dbObjStats);
	void doFlushDirtyPages(Session *session, File *file, vector<KeyPagePair> *dirtyPagesVec,
		bool ignoreCancel, bool flushAllPages) throw (NtseException);

	void syncFiles(set<File *> fileSet);
public:
	/** 老分代占缓存大小的比例 */
	static const double OLD_GEN_RATIO;
	/** 允许老分代的实际大小与目标大小之间有以下误差 */
	static const uint OLD_GEN_SHIFT = 20;
	/** 时间间隔小于这一阈值的访问被认为是相关性访问，单位为秒 */
	static const uint CORRELATED_REF_PERIOD = 10;
	/** 脏页刷写线程默认运行时间间隔，单位为毫秒 */
	static const uint SCAVENGER_DEFAULT_INTERVAL = 1000;
	/** 脏页刷写线程默认写的脏页数不超过缓存大小的这一比例 */
	static const double SCAVENGER_DEFAULT_MAX_PAGES_RATIO;
	/** 脏页刷写线程一次写的脏页数的最小值 */
	static const uint SCAVENGER_MIN_PAGES = 10;
	/** 脏页刷写线程默认扫描的LRU链表长度与上次运行以来缓存中新建页的比例 */
	static const double SCAVENGER_DEFAULT_SEARCH_LENGTH_MULTIPLIER;
	/** 脏页刷写线程一次扫描的LRU链表长度最小值 */
	static const uint SCAVENGER_MIN_SEARCH_LEGNTH = 100;
#ifdef TNT_ENGINE
	/** 脏页刷写线程尝试维护的LRU链表尾部CLEAN PAGES占整个LRU链表的长度(百分比) */
	static const double SCAVENGER_DEFAULT_CLEAN_PAGES_LEN;
	static const uint SCAVENGER_SIGNALED_AFTER_LRU_SEARCH_LEN = 2000;
#endif
	/** 预读区默认大小 */
	static const uint DEFAULT_PREFETCH_SIZE = 4 * 1024 * 1024;
	/** 当预读区中这么多的页在缓存中的启动预读 */
	static const double DEFAULT_PREFETCH_RATIO;
	/** 当预读区中这么多的页读入顺序与预读顺序一致时进行预读 */
	static const double PREFETCH_RIGHT_ORDER_RATIO;
	/** 预读请求队列大小 */
	static const uint PREFETCH_QUEUE_SIZE = 4;
	/** 当进行可能导致死锁的页面锁定时的超时时间，单位毫秒 */
	static const uint UNSAFE_LOCK_TIMEOUT = 10;
	/** 获取页面冲突时的睡眠时间，单位毫秒 */
	static const uint GET_PAGE_SLEEP = 10;
	/** 预读时的提前量 */
	static const uint PREFETCH_AHEAD = 128;
	/** 批量IO操作的数据量 */
	static const uint BATCH_IO_SIZE = 1024 * 1024;

private:
	/** 保存页面标识到BCB的哈希表 */
	DynHash<PageKey *, Bcb *, BcbHasher, PageKeyHasher, BcbKeyEqualer>	m_pageHash;
	/** 保存文件到文件对应页面的哈希表 */
	DynHash<File *, FilePageList *, FileHasher, FileHasher, FilePageListEqualer> m_filePageHash;
	Database		*m_db;			/** 所属数据库 */
	Syslog			*m_syslog;		/** 系统日志 */
	Txnlog			*m_txnlog;		/** 事务日志 */
	RWLock			m_lock;			/** 用于保护哈希表、LRU链表等全局结构的锁 */
	ObjectPool<Bcb>	m_bcbPool;		/** 缓存控制块池 */
	ObjectPool<FilePageList> m_filePageListPool;	/** 文件页面链表对象池 */
	u64				m_correlatedRefPeriod;	/** 读入次序小于这一阈值的访问被认为是相关性访问，不调整LRU链表 */
	DList<Bcb *>	m_oldGen;		/** 老分代LRU链表，最近访问的页面靠近链表尾 */
	DList<Bcb *>	m_newGen;		/** 新分代LRU链表，最近访问的页面靠近链表尾 */
	double	m_oldGenRatio;			/** 老分代比例 */
	DList<Bcb *>	m_allPages;		/** 所有页面链表 */
	Prefetcher		*m_prefetcher;	/** 处理预读的线程 */
	Scavenger		*m_scavenger;	/** 后台写脏页线程 */
	u64				m_readSeq;		/** 页面读取次序 */
	BufferStatus	m_status;		/** 运行状态 */
	BufferFlushStatus m_flushStatus;/** 刷脏页相关状态统计 */
	bool			m_checksumed;	/** 是否计算检验和 */
	uint			m_prefetchSize;	/** 预读区大小 */
	uint			m_prefetchPages;/** 预计区页数 */
	double			m_prefetchRatio;/** 进行线性预读的配置 */
	double			m_skipPrefetchRatio;/** 超过这一比例的页面已经在缓存中时，预读时跳跃性的预读各个不在缓存中的页 */
	static BatchIoBufPool  m_batchIOBufPool;/** 批量IO缓存池 */

#ifdef NTSE_UNIT_TEST
public:
	friend class ::BufferTestCase;
	friend class ::BufferBigTest;
#endif
};

}

#endif
