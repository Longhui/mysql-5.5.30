/**
 * 内存页池管理
 *
 * @author 汪源(wangyuan@corp.netease.com, wy@163.org)
 */

#ifndef _NTSE_PAGEPOOL_H_
#define _NTSE_PAGEPOOL_H_

#include "misc/Global.h"
#include "util/Sync.h"
#include "util/DList.h"
#include "misc/Trace.h"
#include "util/Thread.h"

namespace ntse {

/** 页类型 */
enum PageType {
	PAGE_EMPTY = 0,		/** 未使用的内存页 */
	PAGE_MMS_PAGE,		/** MMS中用于存储记录内容的页 */
	PAGE_MMS_UPDATE,	/** MMS中用于存储更新信息的页 */
	PAGE_MMS_MISC,		/** MMS中的其它内容，如映射表占用的页等 */
	PAGE_HEAP,			/** 堆页 */
	PAGE_INDEX,			/** 索引页 */
	PAGE_TXNLOG,		/** 日志页 */
	PAGE_LOB_INDEX,     /**目录文件页*/
	PAGE_LOB_HEAP,      /**大对象文件页*/
	PAGE_COMMON_POOL,   /** 通用内存池页面 */
	PAGE_MEM_HEAP,      /** TNTIM内存堆页 */
	PAGE_HASH_INDEX,    /** TNTIM哈希索引页*/
	PAGE_SCAN,          /** Scan页面 */
	PAGE_DUMP,          /** Dump页面 */
	PAGE_MEM_INDEX,     /** TNTIM内存索引页 */
	PAGE_MAX,
};

Tracer& operator << (Tracer &tracer, PageType pageType);
extern const char* getPageTypeStr(PageType type);

/**
 * 使用内存页池的用户。
 * 
 * 内存页池的主要功能是维护一个内存页池目前使用了多少内存页，
 * 同时提供空闲页面的维护功能。
 * 
 * 最终的页面分配是由内存页池完成的，但并不是每次内存页分配
 * 释放操作都会操作内存池。当一个内存页池的当前大小不超过其目标
 * 大小时，内存页池用户会将被释放的页面记录在该用户对应的一个
 * 空闲页链表中，并不会立即释放到内存池中。这样设计的目的是防止
 * freeSomePages函数被频繁的调用。另一方面，如果一个内存页池用户
 * 中确实有大量的空闲页面，则这些页面理论上可以为其它内存页池
 * 用户使用，提高性能。因此最终使用的方法是内存页池还保持一个
 * 空闲页的最大数量，如果空闲页太多，则即使当前大小不超过目标
 * 大小，也将被释放的页返回给内存池。
 */
class PagePool;
class PagePoolUser {
protected:
	PagePoolUser(uint targetSize, PagePool *pool);
	virtual ~PagePoolUser() {}

public:
	/**
	 * 内存页池管理器调用这一接口要求该使用统一内存页池的某用户释放
	 * 一些内存页，为其它用户腾出空间。内存页池管理器只有在该用户的
	 * 当前大小大于其目标大小时才会调用这一方法。
	 * 
	 * @param userId 用户ID
	 * @param numPages 期望释放的页面数
	 * @return 实际释放的页面数
	 */
	virtual uint freeSomePages(u16 userId, uint numPages) = 0;
	uint freeSomeFreePages(u16 userId, uint numPages);
	uint getCurrentSize(bool includeFreePages = true) const;
	uint getNumFreePages() const;
	uint getTargetSize() const;
	bool setTargetSize(uint newSize);
	void* allocPage(u16 userId, PageType pageType, void *info, int timeoutMs = 0);
	void* allocPageForce(u16 userId, PageType pageType, void *info);
	void freePage(u16 userId, void *page);
#ifdef TNT_ENGINE
	void putToFreeList(u16 userId, void *page);
	void preAlloc(u16 userId, PageType pageType, uint targetSize);
#endif
	PagePool* getPool();
	
public:
	static const uint CACHE_SIZE = 100;	/** 缓存这么多的空闲页 */

protected:
	PagePool	*m_pool;	/** 所属内存页池 */
	uint	m_targetSize;	/** 以页为单位的目标大小 */
	uint	m_currentSize;	/** 以页为单位的当前大小，包含缓存的空闲页 */

private:
	bool verify();

	Mutex	m_mutex;		/** 保护并发的锁 */
	void	*m_freeList;	/** 空闲页链表头 */
	uint	m_numFreePages;	/** 空闲页数量 */
	bool	m_wantMore;		/** 有无更多的内存需求 */
#ifdef TNT_ENGINE
	uint    m_cacheSize;    /** 最多缓存这么多的空闲页 */
#endif

friend class PagePool;
};

/** 内存页池中各页的描述信息 */
struct PageDesc {
	u32			m_magic;	/** 魔数，检测结构内容被非法修改 */
	LURWLock	m_lock;		/** 保护这个页的锁 */
	void		*m_info;	/** 用户自定义信息 */
	PageType	m_type;		/** 页类型 */
	PagePoolUser	*m_user;/** 使用这个页的用户 */

	static const u32	MAGIC_NUMBER = 0x39785624;

	PageDesc(u16 maxUserId): m_lock(maxUserId, "PageDesc::lock", __FILE__, __LINE__) {
		m_info = NULL;
		m_magic = MAGIC_NUMBER;
		m_type = PAGE_EMPTY;
	}
};

struct PoolScanHandle;
class PoolRebalancer;
/** 内存页池
 * 内存页池模块的主要功能是页面的分配与释放，并提供对页面的读写锁功能。
 * 内存页池管理的内存页大小必须为2的整数次幂，且内存页池分配的页面地址
 * 已经按系统磁盘块大小对齐，可用于DIRECT_IO的文件读写。
 *
 * 注:
 * 如果某种类型的页面会使用lockPageIfType来锁定页面，则分配和释放这类
 * 页面时必须使用互斥锁锁定页面，或者进行额外的同步控制。否则可能产生
 * 以下的错误操作序列(以下T1,T2表示两个线程):
 *
 * 设页面p当前为T类型，
 * T1: 调用lockPageIfType(p, Shared, T, ...)
 *     由于p当前为T类型，锁定成功
 * T2: 调用freePage(p)释放p，释放后p变成类型为PAGE_EMPTY。
 * 这样，T1锁定的p，但p的类型后来又变成PAGE_EMPTY
 */
class PagePool {
public:
	PagePool(u16 maxUserId, uint pageSize);
	virtual ~PagePool();
	void registerUser(PagePoolUser *user);
	void init() throw(NtseException);
	uint getPageSize();
	uint getSize();
	uint getNumFreePages();
	bool isFull();
	void preDelete();
	void requestMore(PagePoolUser *user);
	bool isRebalanceEnabled() const;
	void setRebalanceEnabled(bool enabled);
	/**
	 * 锁定一个内存页
	 *
	 * @param userId 用户ID
	 * @param page 内存页
	 * @param lockMode 锁模式
	 * @param file 发出加锁操作的代码文件名
	 * @param line 发出加锁操作的代码行号
	 */
	inline void lockPage(u16 userId, void *page, LockMode lockMode, const char *file, uint line) {
		ftrace(ts.pool, tout << userId << page << lockMode << file << line);
		PageDesc *desc = pageToDesc(page);
		desc->m_lock.lock(userId, lockMode, file, line);
	}

	/**
	 * 尝试锁定一个内存页
	 *
	 * @param userId 用户ID
	 * @param page 内存页
	 * @param lockMode 锁模式
	 * @param file 发出加锁操作的代码文件名
	 * @param line 发出加锁操作的代码行号
	 * @return 成功与否
	 */
	bool trylockPage(u16 userId, void *page, LockMode lockMode, const char *file, uint line) {
		ftrace(ts.pool, tout << userId << page << lockMode << file << line);
		PageDesc *desc = pageToDesc(page);
		assert(desc->m_type != PAGE_EMPTY);
		return desc->m_lock.tryLock(userId, lockMode, file, line);
	}

	bool tryUpgradePageLock(u16 userId, void *page, const char *file, uint line);
	bool lockPageIfType(u16 userId, void *page, LockMode lockMode, PageType pageType, int timeoutMs, const char *file, uint line);

	/**
	 * 解除对一个内存页的锁定
	 *
	 * @param userId 用户ID
	 * @param page 内存页
	 * @param lockMode 锁模式
	 */
	inline void unlockPage(u16 userId, void *page, LockMode lockMode) {
		ftrace(ts.pool, tout << userId << page << lockMode);
		PageDesc *desc = pageToDesc(page);
		desc->m_lock.unlock(userId, lockMode);
	}

	bool isPageLocked(void *page, LockMode lockMode);
	LockMode getPageLockMode(void *page) const;
	
	/**
	 * 得到某内存页对应的用户自定义数据
	 * @pre 必须为使用中的页面
	 *
	 * @param page 内存页
	 * @return 用户自定义数据
	 */
	inline void* getInfo(void *page) {
		PageDesc *desc = pageToDesc(page);
		assert(desc->m_type != PAGE_EMPTY);
		return desc->m_info;
	}

	PageType getType(void *page);
	void setInfoAndType(void *page, void *info, PageType pageType);

#ifdef NTSE_UNIT_TEST 
	void setType(void *page, PageType pageType);
#endif
	/**
	 * 给定一页内地址，得到其所在内存页的页首地址
	 *
	 * @param addr 页内地址
	 * @return 页首地址
	 */
	inline void* alignPage(void *addr) {
		long long i = (long long)addr;
		i = (i >> m_psPower) << m_psPower;
#ifndef NTSE_MEM_CHECK
		assert((((char *)i - (char *)m_pages) >> m_psPower) < (int)m_size);
#endif
		return (void *)i;
	}

#ifdef NTSE_UNIT_TEST
	PoolRebalancer* getRebalancer() {
		return m_rebalancer;
	}
#endif

	PoolScanHandle* beginScan(PagePoolUser *user, u16 userId);
	void* getNext(PoolScanHandle *h);
	void releaseCurrent(PoolScanHandle *h);
	void endScan(PoolScanHandle *h);

private:
	void* allocPage(PagePoolUser *poolUser, u16 userId, PageType pageType, void *info);
	void freePage(u16 userId, void *page);
	inline PageDesc* pageToDesc(void *page) const {
		assert(((size_t)page) % m_pageSize == 0);
#ifdef NTSE_MEM_CHECK
		PageDesc *pageDesc = (PageDesc *)((char *)page + m_pageSize);
		assert(pageDesc->m_magic == PageDesc::MAGIC_NUMBER);
		return pageDesc;
#else
		assert(page >= m_pages && page <= m_pagesGuard);
		size_t pageNo = ((char *)page - (char *)m_pages) >> m_psPower;
		assert(pageNo < m_size);
		return m_pageDescs + pageNo;
#endif
	}
	bool canChangeTargetSize(PagePoolUser *poolUser, uint newSize);
	void rebalance();

private:
	Mutex	m_mutex;				/** 保护并发操作的锁 */
	size_t	m_pageSize;				/** 页大小 */
	size_t	m_psPower;				/** 页大小是2的多少次幂 */
	size_t	m_size;					/** 以页面数表示的总大小 */
	void	*m_freeList;			/** 指向第一个未使用的页 */
	uint	m_numFreePages;			/** 空闲页数量 */
	bool	m_inited;				/** 是否已经初始化过了 */
#ifdef NTSE_MEM_CHECK
	void	**m_pages;				/** 页数组，页描述符附在页面数据之后 */
#else
	PageDesc	*m_pageDescs;		/** 页描述数组 */
	void	*m_pages;				/** 页数组 */
	void	*m_pagesGuard;			/** 页面地址的上界 */
	void	*m_pagesMemStart;		/** 页数组所分配的内存的起始地址 */
#endif
	u16		m_maxUserId;			/** 加锁时使用的用户ID的最大值 */
	DList<PagePoolUser *>	m_users;/** 使用这一内存页池的用户 */
	PoolRebalancer	*m_rebalancer;	/** 协调各用户占用空间的后台线程 */
friend class PagePoolUser;
friend class PoolRebalancer;
};

/** 负责协调各内存页池用户之间内存占用的后台线程 */
class PoolRebalancer: public Task {
public:
	PoolRebalancer(PagePool *pagePool): Task("PoolRebalancer", 1000) {
		m_pagePool = pagePool;
	}

	void run() {
		m_pagePool->rebalance();
	}

private:
	PagePool	*m_pagePool;
};
}

#endif

