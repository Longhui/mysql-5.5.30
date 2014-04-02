/**
 * 内存池管理
 *
 * @author 汪源(wangyuan@corp.netease.com, wy@163.org)
 */

#include <new>
#include "util/PagePool.h"
#include "util/System.h"
#include "misc/Verify.h"

namespace ntse {

extern const char* getPageTypeStr(PageType type) {
	switch (type) {
	case PAGE_EMPTY:
		return "PAGE_EMPTY";
	case PAGE_MMS_PAGE:
		return "PAGE_MMS_PAGE";
	case PAGE_MMS_UPDATE:
		return "PAGE_MMS_UPDATE";
	case PAGE_MMS_MISC:
		return "PAGE_MMS_MISC";
	case PAGE_HEAP:
		return "PAGE_HEAP";
	case PAGE_INDEX:
		return "PAGE_INDEX";
	case PAGE_TXNLOG:
		return "PAGE_TXNLOG";
	case PAGE_LOB_INDEX:
		return "PAGE_LOB_INDEX";
	case PAGE_LOB_HEAP:
		return "PAGE_LOB_HEAP";
	default:
		assert(type == PAGE_MAX);
		return "PAGE_MAX";
	}
}

/******************************************************************************
 * PagePoolUser                                                               *
 *****************************************************************************/

/**
 * 构造函数
 *
 * @param targetSize 目标大小，单位为页数
 * @param pool 所属内存页池
 */
PagePoolUser::PagePoolUser(uint targetSize, PagePool *pool): m_mutex("PagePoolUser::mutex", __FILE__, __LINE__) {
	ftrace(ts.pool, tout << targetSize << pool);
	m_targetSize = targetSize;
	m_currentSize = 0;
	m_pool = pool;
	m_freeList = NULL;
	m_numFreePages  = 0;
	m_wantMore = false;
#ifdef TNT_ENGINE
	m_cacheSize = CACHE_SIZE;
#endif
}

/**
 * 得到该用户占用的当前大小
 *
 * @param 是否包含被缓存的空闲页
 * @return 当前大小，单位为页
 */
uint PagePoolUser::getCurrentSize(bool includeFreePages) const {
	if (includeFreePages)
		return m_currentSize;
	else {
		uint currentSize = m_currentSize;
		uint numFrees = m_numFreePages;
		return currentSize >= numFrees? currentSize - numFrees: 0;
	}
}

/**
 * 得到该用户缓存的空闲页个数
 *
 * @return 该用户缓存的空闲页个数
 */
uint PagePoolUser::getNumFreePages() const {
	return m_numFreePages;
}

/**
 * 得到该用户的目标大小
 *
 * @return 目标大小，单位为页
 */
uint PagePoolUser::getTargetSize() const {
	return m_targetSize;
}

/** 
 * 设置该用户的目标大小
 *
 * @param newSize 新的目标大小，以页为单位
 * @return 是否设置成功
 */
bool PagePoolUser::setTargetSize(uint newSize) {
	ftrace(ts.pool, tout << newSize);
	SYNC(&m_mutex, 
		if (newSize > m_targetSize && !m_pool->canChangeTargetSize(this, newSize)) {
			nftrace(ts.pool, tout << "failed.");
			return false;
		}
		m_targetSize = newSize;
		nftrace(ts.pool, tout << "succeed.");
		return true;
	);
}

/**
 * 申请一个内存页。
 * @post 若申请成功，则新申请的页已经加上Exclusived锁
 *
 * @param userId 用户ID
 * @param pageType 页面类型
 * @param info 用户自定义数据
 * @param timeoutMs 当内存页池当前大小小于目标大小时，不能马上分配到页面时的超时时间。
 *   -1表示不超时，0表示马上超时，正数为用毫秒数指定的超时时间。默认为0
 * @return 成功返回内存页，失败返回NULL
 */
void* PagePoolUser::allocPage(u16 userId, PageType pageType, void *info, int timeoutMs) {
	ftrace(ts.pool, tout << userId << pageType << info);
	void *page = NULL;
	u64 before = 0;
_retry:
	// TODO 此处加锁时间较长，是否可能优化?
	LOCK(&m_mutex);
	if (m_freeList) {
		page = m_freeList;
		m_freeList = *((void **)m_freeList);
		assert(m_pool->getType(page) == PAGE_EMPTY);
		m_pool->lockPage(userId, page, Exclusived, __FILE__, __LINE__);
		m_pool->setInfoAndType(page, info, pageType);
		m_numFreePages--;
	} else {
		page = m_pool->allocPage(this, userId, pageType, info);
		if (page)
			m_currentSize++;
	}
	UNLOCK(&m_mutex);
	if (page) {
		assert(m_pool->isPageLocked(page, Exclusived));
		assert(m_pool->getType(page) == pageType);
		assert(m_pool->getInfo(page) == info);
	} else if (m_currentSize < m_targetSize) {
		m_pool->requestMore(this);
		if (timeoutMs != 0) {
			if (timeoutMs > 0 && !before)
				before = System::currentTimeMillis();
			Thread::msleep(10);
			if (timeoutMs < 0 || (System::currentTimeMillis() - before) < (u64)timeoutMs)
				goto _retry;
		}
	}
	verify_ex(vs.pool, verify());
	return page;
}

/**
 * 强制分配一个页，不超时，也不管当前大小是否已经超出目标大小
 *
 * @param userId 用户ID
 * @param pageType 页面类型
 * @param info 用户自定义数据
 * @return 新分配的页面
 */
void* PagePoolUser::allocPageForce(u16 userId, PageType pageType, void *info) {
	while (true) {
		void *p = allocPage(userId, pageType, info);
		if (p)
			return p;
		m_pool->requestMore(this);
		Thread::msleep(100);
	}
}

/** 
 * 释放一个内存页。
 * @pre 已经对页面加互斥锁
 *
 * @param userId 用户ID
 * @param page 内存页
 */
void PagePoolUser::freePage(u16 userId, void *page) {
	ftrace(ts.pool, tout << userId << page);
	assert(m_pool->isPageLocked(page, Exclusived));
	// TODO 此处加锁时间较长，是否可能优化?
	LOCK(&m_mutex);
#ifdef TNT_ENGINE
	if (m_currentSize <= m_targetSize && m_numFreePages < m_cacheSize) {
#else
	if (m_currentSize <= m_targetSize && m_numFreePages < CACHE_SIZE) {
#endif
		m_pool->setInfoAndType(page, NULL, PAGE_EMPTY);
		// 必须先将页类型设为EMPTY再解锁，否则一个将要被释放的内存页
		// 可能会被lockPageIfType锁住
		m_pool->unlockPage(userId, page, Exclusived);
		*((void **)page) = m_freeList;
		m_freeList = page;
		m_numFreePages++;
	} else {
		m_pool->freePage(userId, page);
		m_currentSize--;
	}
	UNLOCK(&m_mutex);
	verify_ex(vs.pool, verify());
}

#ifdef TNT_ENGINE
/** 将page放入freelist中，不采用freePage的原因是如果currentSize大于targetSize，
 * freePage会将页面返回给pool。现在需要的场景是alloc一个page后，将它返回到freeList中
 * 为了最大可能的下次alloc page no force。
 * @param userId 用户ID
 * @param page 内存页
 */
void PagePoolUser::putToFreeList(u16 userId, void *page) {
	ftrace(ts.pool, tout << userId << page);
	assert(m_pool->isPageLocked(page, Exclusived));
	LOCK(&m_mutex);
	m_pool->setInfoAndType(page, NULL, PAGE_EMPTY);
	// 必须先将页类型设为EMPTY再解锁，否则一个将要被释放的内存页
	// 可能会被lockPageIfType锁住
	m_pool->unlockPage(userId, page, Exclusived);
	*((void **)page) = m_freeList;
	m_freeList = page;
	m_numFreePages++;
	UNLOCK(&m_mutex);
	verify_ex(vs.pool, verify());
}

/** PagePoolUser预分配一定数量的页面
 * @pre preAlloc目前只规定在系统初始化使用，所以requestMore也是在持有锁的情况下完成，目的是减少lock次数
 * @param userId 用户ID
 * @param pageType 页面类型
 * @param targetSize 目标页面数
 */
void PagePoolUser::preAlloc(u16 userId, PageType pageType, uint targetSize) {
	ftrace(ts.pool, tout << userId << pageType);
	assert(m_pool->m_inited);
	void *page = NULL;
	LOCK(&m_mutex);
	for (uint i = 0; i < targetSize; i++) {
_retry:
		page = m_pool->allocPage(this, userId, pageType, NULL);
		if (page) {
			m_currentSize++;
			assert(m_pool->isPageLocked(page, Exclusived));
			assert(m_pool->getType(page) == pageType);
			m_pool->setInfoAndType(page, NULL, PAGE_EMPTY);
			// 必须先将页类型设为EMPTY再解锁，否则一个将要被释放的内存页
			// 可能会被lockPageIfType锁住
			m_pool->unlockPage(userId, page, Exclusived);
			*((void **)page) = m_freeList;
			m_freeList = page;
			m_numFreePages++;
		}
		if (!page) {
			m_pool->requestMore(this);
			Thread::msleep(10);
			goto _retry;
		}
	}
	m_cacheSize = targetSize;
	UNLOCK(&m_mutex);
	verify_ex(vs.pool, verify());
	return;
}
#endif

/** 
 * 得到所用的内存页池
 *
 * @return 所用的内存页池
 */
PagePool* PagePoolUser::getPool() {
	return m_pool;
}

/**
 * 释放一些空闲页
 *
 * @param userId 用户ID
 * @param numPages 期望释放的页面数
 * @return 成功释放的空闲页个数
 */
uint PagePoolUser::freeSomeFreePages(u16 userId, uint numPages) {
	ftrace(ts.pool, tout << userId << numPages);
	uint count = 0;
	SYNC(&m_mutex, 
		while (count < numPages && m_freeList) {
			void *page = m_freeList;
			m_freeList = *((void **)m_freeList);
			m_pool->lockPage(userId, page, Exclusived, __FILE__, __LINE__);
			m_pool->freePage(userId, page);
			
			count++;
			m_numFreePages--;
		}
	);
	verify_ex(vs.pool, verify());
	nftrace(ts.pool, tout << count << " pages freed.");
	return count;
}

/**
 * 验证一致性
 *
 * @param 是否一致
 */
bool PagePoolUser::verify() {
	// m_numFreePages与空闲页链表是否一致
	uint n = 0;
	void *page = m_freeList;
	SYNC(&m_mutex,
		while (page) {
			n++;
			page = *((void **)page);
		}
	);
	return n == m_numFreePages;
}

/******************************************************************************
 * PagePool                                                                   *
 *****************************************************************************/

/**
 * 创建一个内存页池（还没有分配内存）
 *
 * @param maxUserId 加锁时使用的用户ID的最大值
 * @param pageSize 页大小，必须是2的整数次幂
 */
PagePool::PagePool(u16 maxUserId, uint pageSize): m_mutex("PagePool::mutex", __FILE__, __LINE__) {
	ftrace(ts.pool, tout << maxUserId << pageSize);
	uint n = pageSize;
	m_psPower = 0;
	while (n >= 2) {
		assert((n % 2) == 0);
		n /= 2;
		m_psPower++;
	}

	m_pageSize = pageSize;
	m_size = 0;
	m_inited = false;
#ifdef NTSE_MEM_CHECK
	m_pages = NULL;
#else
	m_pageDescs = NULL;
	m_pages = NULL;
	m_pagesGuard = NULL;
	m_pagesMemStart = NULL;
#endif
	m_maxUserId = maxUserId;
	m_numFreePages = 0;
}

PagePool::~PagePool() {
	ftrace(ts.pool, );
	if (m_inited) {
		if (m_rebalancer) {
			m_rebalancer->stop();
			m_rebalancer->join();
			delete m_rebalancer;
		}
#ifdef NTSE_MEM_CHECK
		for (uint i = 0; i < m_size; i++) {
			PageDesc *desc = (PageDesc *)((char *)m_pages[i] + m_pageSize);
			desc->~PageDesc();
#ifdef WIN32
			System::virtualFree(m_pages[i]);
#else
			free(m_pages[i]);
#endif
		}
		delete []m_pages;
		m_pages = NULL;
#else
		if (m_pagesMemStart != NULL)
			System::virtualFree(m_pagesMemStart);
		if (m_pageDescs != NULL) {
			for (uint i = 0; i < m_size; i++) {
				(m_pageDescs + i)->~PageDesc();
			}
			free(m_pageDescs);
		}
		m_pageDescs = NULL;
		m_pages = NULL;
		m_pagesGuard = NULL;
		m_pagesMemStart = NULL;
#endif
	}
	DLink<PagePoolUser *> *e, *next;
	for (e = m_users.getHeader()->getNext(), next = e->getNext(); e != m_users.getHeader(); 
		e = next, next = next->getNext()) {
			delete e;
	}
}

/**
 * 注册一个使用内存页池的用户
 * @pre init函数还没有被调用
 * @pre 指定的用户以前没有注册过
 *
 * @param user 使用内存页池的用户，必须实现freeSomePages函数。
 */
void PagePool::registerUser(PagePoolUser *user) {
	ftrace(ts.pool, tout << user);
	assert(!m_inited);
	for (DLink<PagePoolUser *> *e = m_users.getHeader()->getNext(); e != m_users.getHeader(); e = e->getNext())
		assert(e->get() != user);
	m_size += user->getTargetSize();
	m_users.addLast(new DLink<PagePoolUser *>(user));
}

/**
 * 初始化内存页池。内存页池的总大小为各注册用户目标大小之和
 * @pre 所有内存页池的用户都已经调用registerUser函数注册
 * @pre 还没有调用本函数初始化过
 *
 * @throw NtseException 内存空间不足
 */
void PagePool::init() throw(NtseException) {
	ftrace(ts.pool, );
	assert(!m_inited);
	m_inited = true;
#ifdef NTSE_MEM_CHECK
	// 分配内存
	m_pages = new void *[m_size];
	for (uint i = 0; i < m_size; i++) {
#ifdef WIN32
		m_pages[i] = System::virtualAlloc(m_pageSize + sizeof(PageDesc));
		assert(((size_t)m_pages[i]) % m_pageSize == 0);
#else
		m_pages[i] = memalign(m_pageSize, m_pageSize + sizeof(PageDesc));
#endif
		if (!m_pages[i])
			NTSE_THROW(NTSE_EC_OUT_OF_MEM, "Failed to initialize page pool.");
	}
	// 初始化
	for (uint i = 0; i < m_size - 1; i++) {
		*((void **)(m_pages[i])) = m_pages[i + 1];
	}
	*((void **)(m_pages[m_size - 1])) = NULL;
	for (uint i = 0; i < m_size; i++) {
		void *p = (char *)m_pages[i] + m_pageSize;
		new (p)PageDesc(m_maxUserId);
	}
	m_freeList = m_pages[0];
#else
	// 分配页面占用的内存时多分配一些，使得各页内存地址能被m_pageSize整除
	size_t allocSize = (m_size + 1) * m_pageSize;
	m_pagesMemStart = System::virtualAlloc(allocSize);
	if (!m_pagesMemStart)
		NTSE_THROW(NTSE_EC_OUT_OF_MEM, "Failed to initialize page pool.");
	m_pages = (char *)m_pagesMemStart + m_pageSize - ((size_t)m_pagesMemStart) % m_pageSize;
	m_pagesGuard = (char *)m_pages + m_size * m_pageSize;
	m_pageDescs = (PageDesc *)malloc(m_size * sizeof(PageDesc));
	if (!m_pageDescs) {
		System::virtualFree(m_pagesMemStart);
		NTSE_THROW(NTSE_EC_OUT_OF_MEM, "Failed to initialize page pool.");
	}
	for (uint i = 0; i < m_size; i++) {
		new (m_pageDescs + i)PageDesc(m_maxUserId);
	}

	// 初始化空闲页链表
	char *p = (char *)m_pages;
	for (uint i = 0; i < m_size - 1; i++) {
		*((void **)p) = p + m_pageSize;
		m_pageDescs[i].m_type = PAGE_EMPTY;
		m_pageDescs[i].m_info = NULL;
		p += m_pageSize;
	}
	*((void **)p) = NULL;
	m_pageDescs[m_size - 1].m_type = PAGE_EMPTY;
	m_pageDescs[m_size - 1].m_info = NULL;
	m_freeList = m_pages;
#endif

	m_numFreePages = m_size;

	m_rebalancer = new PoolRebalancer(this);
	m_rebalancer->start();
}

/**
 * 得到内存页池的页大小
 *
 * @return 页大小
 */
uint PagePool::getPageSize() {
	return m_pageSize;
}

/**
 * 得到内存页池的大小
 * @pre 已经调用init函数进行了初始化
 * 
 * @return 内存页池大小，以页为单位
 */
uint PagePool::getSize() {
	return m_size;
}

/**
 * 得到内存页池当前空闲页面大小
 * @return 当前空闲页面数
 */
uint PagePool::getNumFreePages() {
	return m_numFreePages;
}

/**
 * 尝试进行锁升级
 * @pre 调用者加的是读锁
 *
 * @param userId 用户ID
 * @param 内存页
 * @param file 发出加锁操作的代码文件名
 * @param line 发出加锁操作的代码行号
 * @return 成功与否
 */
bool PagePool::tryUpgradePageLock(u16 userId, void *page, const char *file, uint line) {
	ftrace(ts.pool, tout << userId << page << file << line);
	PageDesc *desc = pageToDesc(page);
	assert(desc->m_type != PAGE_EMPTY);
	assert(desc->m_lock.isLocked(Shared));
	return desc->m_lock.tryUpgrade(userId, file, line);
}

/**
 * 如果所给地址为指定类型的内存页则锁定该页
 * @pre page必须为一个正确的内存页地址
 *
 * @param userId 用户ID
 * @param page 内存页
 * @param lockMode 锁模式
 * @param pageType 页面类型
 * @param timeoutMs 超时时间，若为0则马上超时，若为<0则不超时
 * @param file 发出加锁操作的代码文件名
 * @param line 发出加锁操作的代码行号
 * @return 如果所给地址为指定类型的内存页，则返回true，否则返回false
 */
bool PagePool::lockPageIfType(u16 userId, void *page, LockMode lockMode, PageType pageType, int timeoutMs, const char *file, uint line) {
	ftrace(ts.pool, tout << userId << page << lockMode << pageType << timeoutMs << file << line);
	assert(pageType != PAGE_EMPTY);
	PageDesc *desc = pageToDesc(page);
	bool succ = false;
	// 先判断一下页面类型并进行带超时的加锁，判断页面类型之后
	// 页面类型还有可能发生变化，因此不能强制加锁
	int thisTimeout = timeoutMs < 0? 100: timeoutMs;
	while (true) {
		if (desc->m_type != pageType)
			return false;
		succ = desc->m_lock.timedLock(userId, lockMode, thisTimeout, file, line);	
		if (desc->m_type != pageType) {
			if (succ)
				desc->m_lock.unlock(userId, lockMode);
			return false;
		}
		if (timeoutMs >= 0)
			return succ;
		if (succ)
			return true;
	}
}

/** 
 * 判断一个页是否被锁定
 * 
 * @param page 内存页
 * @param lockMode 锁模式
 */
bool PagePool::isPageLocked(void *page, LockMode lockMode) {
	PageDesc *desc = pageToDesc(page);
	return desc->m_lock.isLocked(lockMode);
}

/**
 * 获得当前页面加锁的模式
 * @param page 内存页
 * @return 锁模式
 */
LockMode PagePool::getPageLockMode(void *page) const {
	PageDesc *desc = pageToDesc(page);
	return desc->m_lock.getLockMode();
}

/**
 * 得到某内存页对应的页面类型
 *
 * @param page 内存页
 * @return 页面类型
 */
PageType PagePool::getType(void *page) {
	PageDesc *desc = pageToDesc(page);
	return desc->m_type;
}

/**
 * 设置某内存页对应的页面类型和用户自定义数据
 *
 * @param page 内存页
 * @param info 用户自定义数据
 * @param pageType 页面类型
 */
void PagePool::setInfoAndType(void *page, void *info, PageType pageType) {
	ftrace(ts.pool, tout << page << info << pageType);
	assert(pageType >= PAGE_EMPTY && pageType < PAGE_MAX);
	PageDesc *desc = pageToDesc(page);
	desc->m_info = info;
	desc->m_type = pageType;
}

#ifdef NTSE_UNIT_TEST
void PagePool::setType(void *page, PageType pageType) {
	ftrace(ts.pool, tout << page << info << pageType);
	assert(pageType >= PAGE_EMPTY && pageType < PAGE_MAX);
	PageDesc *desc = pageToDesc(page);
	desc->m_type = pageType;
}
#endif

/**
 * 申请一个内存页。
 * @post 新申请的页已经加Exclusived锁
 *
 * @param poolUser 申请分配这一内存页的用户
 * @param userId 用户ID，这指加锁的用户ID，与poolUser没有关系
 * @param pageType 页类型
 * @param info 用户自定义数据
 * @param lockMode 锁模式，可以为None
 * @return 成功返回内存页，失败返回NULL
 */
void* PagePool::allocPage(PagePoolUser *poolUser, u16 userId, PageType pageType, void *info) {
	ftrace(ts.pool, tout << poolUser << userId << pageType << info);
	assert(pageType > PAGE_EMPTY && pageType < PAGE_MAX);
	LOCK(&m_mutex);
	void *page;
	if (m_freeList) {
		page = m_freeList;
		m_freeList = *((void **)m_freeList);
		PageDesc *desc = pageToDesc(page);
		desc->m_lock.lock(userId, Exclusived, __FILE__, __LINE__);
		desc->m_info = info;
		desc->m_type = pageType;
		desc->m_user = poolUser;

		--m_numFreePages;
	} else {
		page = NULL;
	}
	UNLOCK(&m_mutex);
	return page;
}

/** 
 * 释放一个内存页。
 * @pre 已经对页面加上互斥锁
 *
 * @param userId 用户ID
 * @param page 内存页
 */
void PagePool::freePage(u16 userId, void *page) {
	ftrace(ts.pool, tout << userId << page);
	assert(isPageLocked(page, Exclusived));
	
	LOCK(&m_mutex);

	PageDesc *desc = pageToDesc(page);

	*((void **)page) = m_freeList;
	m_freeList = page;
	desc->m_info = NULL;
	desc->m_type = PAGE_EMPTY;
	desc->m_user = NULL;
	desc->m_lock.unlock(userId, Exclusived);

	++m_numFreePages;

	UNLOCK(&m_mutex);
}

/**
 * 判断能否将某内存页池用户的大小修改为指定大小。
 * 判断原则是修改之后各用户的总大小不能超过当初初始化时的大小
 *
 * @param poolUser 内存页池用户
 * @param newSize 新大小
 * @return 能否将某内存页池用户的大小修改为指定大小
 */
bool PagePool::canChangeTargetSize(PagePoolUser *poolUser, uint newSize) {
	LOCK(&m_mutex);
	uint size = 0;
	for (DLink<PagePoolUser *> *e = m_users.getHeader()->getNext(); e != m_users.getHeader(); e = e->getNext()) {
		if (e->get() != poolUser)
			size += e->get()->getTargetSize();
	}
	size += newSize;
	UNLOCK(&m_mutex);
	return size <= m_size;
}

/**
 * 获得内存页池是否已满
 *
 * @return 内存页池是否已满
 */
bool PagePool::isFull() {
	return m_freeList == NULL;
}

/**
 * 准备销毁内存页池。在销毁使用该内存页池的用户之前必须调用此函数
 *
 */
void PagePool::preDelete() {
	if (!m_inited) {
		return;
	}
	if (m_rebalancer) {
		m_rebalancer->stop();
		m_rebalancer->join();
	}	
}

/**
 * 通知内存页池管理器该用户需要更多的页面
 *
 * @param user 内存页池用户
 */
void PagePool::requestMore(PagePoolUser *user) {
	user->m_wantMore = true;
	if (!m_rebalancer->isPaused())
		m_rebalancer->signal();
}

/** 返回是否启用在使用同一内存页池中不同用户之间动态调配内存功能
 * @return 是否启用
 */
bool PagePool::isRebalanceEnabled() const {
	return m_rebalancer->isPaused();
}

/** 设置是否启用在使用同一内存页池中不同用户之间动态调配内存功能
 * @return 启用还是禁用
 */
void PagePool::setRebalanceEnabled(bool enabled) {
	if (enabled)
		m_rebalancer->resume();
	else
		m_rebalancer->pause();
}

/** 调整各用户占用内存大小，调节各方需求。本函数只有PoolRebalancer后台线程会
 * 调用，不存在并发。
 */
void PagePool::rebalance() {
	ftrace(ts.pool, );
	// 检查调整需求，需要调整的充要条件是有某个用户设置了wantMore标志位，
	// 并且该用户内部已经没有空闲页了。而不考虑用户当前大小是否超过其
	// 目标大小
	for (DLink<PagePoolUser *> *e = m_users.getHeader()->getNext(); e != m_users.getHeader(); e = e->getNext()) {
		PagePoolUser *user = e->get();
		if (user->m_numFreePages == 0 && user->m_wantMore) {
			int requirement = user->getTargetSize() - user->getCurrentSize();
			if (requirement > (int)user->getCurrentSize())
				requirement = (int)user->getCurrentSize();
			if (requirement > 20)
				requirement = 20;
			if (requirement < 5)
				requirement = 5;
			nftrace(ts.pool, tout << "Make room for " << user);
			// 进行两轮调整，第一轮优先只考虑占用空间超过目标大小的用户
			// 第二轮也考虑也考虑占用空间不超过目标大小的用户
			for (int run = 0; run < 2; run++) {
				for (DLink<PagePoolUser *> *e2 = m_users.getHeader()->getNext(); e2 != m_users.getHeader(); e2 = e2->getNext()) {
					PagePoolUser *user2 = e2->get();
					if (user2 == user)
						continue;
					if (run == 0 && user2->getCurrentSize() <= user2->getTargetSize())
						continue;
					int shouldFree = user2->getCurrentSize() - user2->getTargetSize();
					if (shouldFree < 5)
						shouldFree = 5;
					if (shouldFree > 20)
						shouldFree = 20;
					if (shouldFree > requirement)
						shouldFree = requirement;
					user2->freeSomePages(0, shouldFree);
				}
			}
			user->m_wantMore = false;
		}
	}
}

/** 内存页池扫描句柄 */
struct PoolScanHandle {
	PagePoolUser	*m_user;	/** 只扫描此用户，或NULL */
	u16				m_userId;	/** 用户ID */
	void			*m_page;	/** 当前页面 */
	size_t			m_curPos;	/** 当前位置 */

	PoolScanHandle(PagePoolUser *user, u16 userId) {
		m_userId = userId;
		m_user = user;
		m_curPos = 0;
		m_page = NULL;
	}
};

/**
 * 开始扫描内存页池
 *
 * @param user 若为NULL则返回所有使用中的内存页，否则只返回指定用户使用的内存页
 * @param userId 用户ID
 * @return 扫描句柄
 */
PoolScanHandle* PagePool::beginScan(PagePoolUser *user, u16 userId) {
	return new PoolScanHandle(user, userId);
}

/**
 * 得到下一个页
 * @post 返回的页已经用共享锁锁定，上次返回的页面上的锁已经被释放
 *
 * @param h 扫描句柄
 * @return 页面数据，若没有下一个页则返回NULL
 */
void* PagePool::getNext(PoolScanHandle *h) {
	if (h->m_page) {
		unlockPage(h->m_userId, h->m_page, Shared);
		h->m_page = NULL;
	}
	while (h->m_curPos < m_size) {
		void *page;
#ifdef NTSE_MEM_CHECK
		page = m_pages[h->m_curPos];
#else
		page = ((char *)m_pages) + m_pageSize * h->m_curPos;
#endif
		PageDesc *desc = pageToDesc(page);
		PageType type = desc->m_type;
		if (type == PAGE_EMPTY || (h->m_user && desc->m_user != h->m_user)) {
			h->m_curPos++;
			continue;
		}
		if (!lockPageIfType(h->m_userId, page, Shared, type, 100, __FILE__, __LINE__))
			continue;

		if (h->m_user && desc->m_user != h->m_user) {
			unlockPage(h->m_userId, page, Shared);
			h->m_curPos++;
			continue;
		}

		h->m_curPos++;
		h->m_page = page;
		return page;
	}
	return NULL;
}

/** 释放当前页
 *
 * @param h 扫描句柄
 */
void PagePool::releaseCurrent(PoolScanHandle *h) {
	assert(h->m_page);
	unlockPage(h->m_userId, h->m_page, Shared);
	h->m_page = NULL;
}

/**
 * 结束扫描，若当前页面不为NULL则自动放锁
 * @post 扫描句柄对象已经被delete
 *
 * @param h 扫描句柄
 */
void PagePool::endScan(PoolScanHandle *h) {
	if (h->m_page) {
		unlockPage(h->m_userId, h->m_page, Shared);
		h->m_page = NULL;
	}
	delete h;
}

}
