/**
 * �ڴ�ع���
 *
 * @author ��Դ(wangyuan@corp.netease.com, wy@163.org)
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
 * ���캯��
 *
 * @param targetSize Ŀ���С����λΪҳ��
 * @param pool �����ڴ�ҳ��
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
 * �õ����û�ռ�õĵ�ǰ��С
 *
 * @param �Ƿ����������Ŀ���ҳ
 * @return ��ǰ��С����λΪҳ
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
 * �õ����û�����Ŀ���ҳ����
 *
 * @return ���û�����Ŀ���ҳ����
 */
uint PagePoolUser::getNumFreePages() const {
	return m_numFreePages;
}

/**
 * �õ����û���Ŀ���С
 *
 * @return Ŀ���С����λΪҳ
 */
uint PagePoolUser::getTargetSize() const {
	return m_targetSize;
}

/** 
 * ���ø��û���Ŀ���С
 *
 * @param newSize �µ�Ŀ���С����ҳΪ��λ
 * @return �Ƿ����óɹ�
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
 * ����һ���ڴ�ҳ��
 * @post ������ɹ������������ҳ�Ѿ�����Exclusived��
 *
 * @param userId �û�ID
 * @param pageType ҳ������
 * @param info �û��Զ�������
 * @param timeoutMs ���ڴ�ҳ�ص�ǰ��СС��Ŀ���Сʱ���������Ϸ��䵽ҳ��ʱ�ĳ�ʱʱ�䡣
 *   -1��ʾ����ʱ��0��ʾ���ϳ�ʱ������Ϊ�ú�����ָ���ĳ�ʱʱ�䡣Ĭ��Ϊ0
 * @return �ɹ������ڴ�ҳ��ʧ�ܷ���NULL
 */
void* PagePoolUser::allocPage(u16 userId, PageType pageType, void *info, int timeoutMs) {
	ftrace(ts.pool, tout << userId << pageType << info);
	void *page = NULL;
	u64 before = 0;
_retry:
	// TODO �˴�����ʱ��ϳ����Ƿ�����Ż�?
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
 * ǿ�Ʒ���һ��ҳ������ʱ��Ҳ���ܵ�ǰ��С�Ƿ��Ѿ�����Ŀ���С
 *
 * @param userId �û�ID
 * @param pageType ҳ������
 * @param info �û��Զ�������
 * @return �·����ҳ��
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
 * �ͷ�һ���ڴ�ҳ��
 * @pre �Ѿ���ҳ��ӻ�����
 *
 * @param userId �û�ID
 * @param page �ڴ�ҳ
 */
void PagePoolUser::freePage(u16 userId, void *page) {
	ftrace(ts.pool, tout << userId << page);
	assert(m_pool->isPageLocked(page, Exclusived));
	// TODO �˴�����ʱ��ϳ����Ƿ�����Ż�?
	LOCK(&m_mutex);
#ifdef TNT_ENGINE
	if (m_currentSize <= m_targetSize && m_numFreePages < m_cacheSize) {
#else
	if (m_currentSize <= m_targetSize && m_numFreePages < CACHE_SIZE) {
#endif
		m_pool->setInfoAndType(page, NULL, PAGE_EMPTY);
		// �����Ƚ�ҳ������ΪEMPTY�ٽ���������һ����Ҫ���ͷŵ��ڴ�ҳ
		// ���ܻᱻlockPageIfType��ס
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
/** ��page����freelist�У�������freePage��ԭ�������currentSize����targetSize��
 * freePage�Ὣҳ�淵�ظ�pool��������Ҫ�ĳ�����allocһ��page�󣬽������ص�freeList��
 * Ϊ�������ܵ��´�alloc page no force��
 * @param userId �û�ID
 * @param page �ڴ�ҳ
 */
void PagePoolUser::putToFreeList(u16 userId, void *page) {
	ftrace(ts.pool, tout << userId << page);
	assert(m_pool->isPageLocked(page, Exclusived));
	LOCK(&m_mutex);
	m_pool->setInfoAndType(page, NULL, PAGE_EMPTY);
	// �����Ƚ�ҳ������ΪEMPTY�ٽ���������һ����Ҫ���ͷŵ��ڴ�ҳ
	// ���ܻᱻlockPageIfType��ס
	m_pool->unlockPage(userId, page, Exclusived);
	*((void **)page) = m_freeList;
	m_freeList = page;
	m_numFreePages++;
	UNLOCK(&m_mutex);
	verify_ex(vs.pool, verify());
}

/** PagePoolUserԤ����һ��������ҳ��
 * @pre preAllocĿǰֻ�涨��ϵͳ��ʼ��ʹ�ã�����requestMoreҲ���ڳ��������������ɣ�Ŀ���Ǽ���lock����
 * @param userId �û�ID
 * @param pageType ҳ������
 * @param targetSize Ŀ��ҳ����
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
			// �����Ƚ�ҳ������ΪEMPTY�ٽ���������һ����Ҫ���ͷŵ��ڴ�ҳ
			// ���ܻᱻlockPageIfType��ס
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
 * �õ����õ��ڴ�ҳ��
 *
 * @return ���õ��ڴ�ҳ��
 */
PagePool* PagePoolUser::getPool() {
	return m_pool;
}

/**
 * �ͷ�һЩ����ҳ
 *
 * @param userId �û�ID
 * @param numPages �����ͷŵ�ҳ����
 * @return �ɹ��ͷŵĿ���ҳ����
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
 * ��֤һ����
 *
 * @param �Ƿ�һ��
 */
bool PagePoolUser::verify() {
	// m_numFreePages�����ҳ�����Ƿ�һ��
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
 * ����һ���ڴ�ҳ�أ���û�з����ڴ棩
 *
 * @param maxUserId ����ʱʹ�õ��û�ID�����ֵ
 * @param pageSize ҳ��С��������2����������
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
 * ע��һ��ʹ���ڴ�ҳ�ص��û�
 * @pre init������û�б�����
 * @pre ָ�����û���ǰû��ע���
 *
 * @param user ʹ���ڴ�ҳ�ص��û�������ʵ��freeSomePages������
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
 * ��ʼ���ڴ�ҳ�ء��ڴ�ҳ�ص��ܴ�СΪ��ע���û�Ŀ���С֮��
 * @pre �����ڴ�ҳ�ص��û����Ѿ�����registerUser����ע��
 * @pre ��û�е��ñ�������ʼ����
 *
 * @throw NtseException �ڴ�ռ䲻��
 */
void PagePool::init() throw(NtseException) {
	ftrace(ts.pool, );
	assert(!m_inited);
	m_inited = true;
#ifdef NTSE_MEM_CHECK
	// �����ڴ�
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
	// ��ʼ��
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
	// ����ҳ��ռ�õ��ڴ�ʱ�����һЩ��ʹ�ø�ҳ�ڴ��ַ�ܱ�m_pageSize����
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

	// ��ʼ������ҳ����
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
 * �õ��ڴ�ҳ�ص�ҳ��С
 *
 * @return ҳ��С
 */
uint PagePool::getPageSize() {
	return m_pageSize;
}

/**
 * �õ��ڴ�ҳ�صĴ�С
 * @pre �Ѿ�����init���������˳�ʼ��
 * 
 * @return �ڴ�ҳ�ش�С����ҳΪ��λ
 */
uint PagePool::getSize() {
	return m_size;
}

/**
 * �õ��ڴ�ҳ�ص�ǰ����ҳ���С
 * @return ��ǰ����ҳ����
 */
uint PagePool::getNumFreePages() {
	return m_numFreePages;
}

/**
 * ���Խ���������
 * @pre �����߼ӵ��Ƕ���
 *
 * @param userId �û�ID
 * @param �ڴ�ҳ
 * @param file �������������Ĵ����ļ���
 * @param line �������������Ĵ����к�
 * @return �ɹ����
 */
bool PagePool::tryUpgradePageLock(u16 userId, void *page, const char *file, uint line) {
	ftrace(ts.pool, tout << userId << page << file << line);
	PageDesc *desc = pageToDesc(page);
	assert(desc->m_type != PAGE_EMPTY);
	assert(desc->m_lock.isLocked(Shared));
	return desc->m_lock.tryUpgrade(userId, file, line);
}

/**
 * ���������ַΪָ�����͵��ڴ�ҳ��������ҳ
 * @pre page����Ϊһ����ȷ���ڴ�ҳ��ַ
 *
 * @param userId �û�ID
 * @param page �ڴ�ҳ
 * @param lockMode ��ģʽ
 * @param pageType ҳ������
 * @param timeoutMs ��ʱʱ�䣬��Ϊ0�����ϳ�ʱ����Ϊ<0�򲻳�ʱ
 * @param file �������������Ĵ����ļ���
 * @param line �������������Ĵ����к�
 * @return ���������ַΪָ�����͵��ڴ�ҳ���򷵻�true�����򷵻�false
 */
bool PagePool::lockPageIfType(u16 userId, void *page, LockMode lockMode, PageType pageType, int timeoutMs, const char *file, uint line) {
	ftrace(ts.pool, tout << userId << page << lockMode << pageType << timeoutMs << file << line);
	assert(pageType != PAGE_EMPTY);
	PageDesc *desc = pageToDesc(page);
	bool succ = false;
	// ���ж�һ��ҳ�����Ͳ����д���ʱ�ļ������ж�ҳ������֮��
	// ҳ�����ͻ��п��ܷ����仯����˲���ǿ�Ƽ���
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
 * �ж�һ��ҳ�Ƿ�����
 * 
 * @param page �ڴ�ҳ
 * @param lockMode ��ģʽ
 */
bool PagePool::isPageLocked(void *page, LockMode lockMode) {
	PageDesc *desc = pageToDesc(page);
	return desc->m_lock.isLocked(lockMode);
}

/**
 * ��õ�ǰҳ�������ģʽ
 * @param page �ڴ�ҳ
 * @return ��ģʽ
 */
LockMode PagePool::getPageLockMode(void *page) const {
	PageDesc *desc = pageToDesc(page);
	return desc->m_lock.getLockMode();
}

/**
 * �õ�ĳ�ڴ�ҳ��Ӧ��ҳ������
 *
 * @param page �ڴ�ҳ
 * @return ҳ������
 */
PageType PagePool::getType(void *page) {
	PageDesc *desc = pageToDesc(page);
	return desc->m_type;
}

/**
 * ����ĳ�ڴ�ҳ��Ӧ��ҳ�����ͺ��û��Զ�������
 *
 * @param page �ڴ�ҳ
 * @param info �û��Զ�������
 * @param pageType ҳ������
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
 * ����һ���ڴ�ҳ��
 * @post �������ҳ�Ѿ���Exclusived��
 *
 * @param poolUser ���������һ�ڴ�ҳ���û�
 * @param userId �û�ID����ָ�������û�ID����poolUserû�й�ϵ
 * @param pageType ҳ����
 * @param info �û��Զ�������
 * @param lockMode ��ģʽ������ΪNone
 * @return �ɹ������ڴ�ҳ��ʧ�ܷ���NULL
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
 * �ͷ�һ���ڴ�ҳ��
 * @pre �Ѿ���ҳ����ϻ�����
 *
 * @param userId �û�ID
 * @param page �ڴ�ҳ
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
 * �ж��ܷ�ĳ�ڴ�ҳ���û��Ĵ�С�޸�Ϊָ����С��
 * �ж�ԭ�����޸�֮����û����ܴ�С���ܳ���������ʼ��ʱ�Ĵ�С
 *
 * @param poolUser �ڴ�ҳ���û�
 * @param newSize �´�С
 * @return �ܷ�ĳ�ڴ�ҳ���û��Ĵ�С�޸�Ϊָ����С
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
 * ����ڴ�ҳ���Ƿ�����
 *
 * @return �ڴ�ҳ���Ƿ�����
 */
bool PagePool::isFull() {
	return m_freeList == NULL;
}

/**
 * ׼�������ڴ�ҳ�ء�������ʹ�ø��ڴ�ҳ�ص��û�֮ǰ������ô˺���
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
 * ֪ͨ�ڴ�ҳ�ع��������û���Ҫ�����ҳ��
 *
 * @param user �ڴ�ҳ���û�
 */
void PagePool::requestMore(PagePoolUser *user) {
	user->m_wantMore = true;
	if (!m_rebalancer->isPaused())
		m_rebalancer->signal();
}

/** �����Ƿ�������ʹ��ͬһ�ڴ�ҳ���в�ͬ�û�֮�䶯̬�����ڴ湦��
 * @return �Ƿ�����
 */
bool PagePool::isRebalanceEnabled() const {
	return m_rebalancer->isPaused();
}

/** �����Ƿ�������ʹ��ͬһ�ڴ�ҳ���в�ͬ�û�֮�䶯̬�����ڴ湦��
 * @return ���û��ǽ���
 */
void PagePool::setRebalanceEnabled(bool enabled) {
	if (enabled)
		m_rebalancer->resume();
	else
		m_rebalancer->pause();
}

/** �������û�ռ���ڴ��С�����ڸ������󡣱�����ֻ��PoolRebalancer��̨�̻߳�
 * ���ã������ڲ�����
 */
void PagePool::rebalance() {
	ftrace(ts.pool, );
	// ������������Ҫ�����ĳ�Ҫ��������ĳ���û�������wantMore��־λ��
	// ���Ҹ��û��ڲ��Ѿ�û�п���ҳ�ˡ����������û���ǰ��С�Ƿ񳬹���
	// Ŀ���С
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
			// �������ֵ�������һ������ֻ����ռ�ÿռ䳬��Ŀ���С���û�
			// �ڶ���Ҳ����Ҳ����ռ�ÿռ䲻����Ŀ���С���û�
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

/** �ڴ�ҳ��ɨ���� */
struct PoolScanHandle {
	PagePoolUser	*m_user;	/** ֻɨ����û�����NULL */
	u16				m_userId;	/** �û�ID */
	void			*m_page;	/** ��ǰҳ�� */
	size_t			m_curPos;	/** ��ǰλ�� */

	PoolScanHandle(PagePoolUser *user, u16 userId) {
		m_userId = userId;
		m_user = user;
		m_curPos = 0;
		m_page = NULL;
	}
};

/**
 * ��ʼɨ���ڴ�ҳ��
 *
 * @param user ��ΪNULL�򷵻�����ʹ���е��ڴ�ҳ������ֻ����ָ���û�ʹ�õ��ڴ�ҳ
 * @param userId �û�ID
 * @return ɨ����
 */
PoolScanHandle* PagePool::beginScan(PagePoolUser *user, u16 userId) {
	return new PoolScanHandle(user, userId);
}

/**
 * �õ���һ��ҳ
 * @post ���ص�ҳ�Ѿ��ù������������ϴη��ص�ҳ���ϵ����Ѿ����ͷ�
 *
 * @param h ɨ����
 * @return ҳ�����ݣ���û����һ��ҳ�򷵻�NULL
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

/** �ͷŵ�ǰҳ
 *
 * @param h ɨ����
 */
void PagePool::releaseCurrent(PoolScanHandle *h) {
	assert(h->m_page);
	unlockPage(h->m_userId, h->m_page, Shared);
	h->m_page = NULL;
}

/**
 * ����ɨ�裬����ǰҳ�治ΪNULL���Զ�����
 * @post ɨ���������Ѿ���delete
 *
 * @param h ɨ����
 */
void PagePool::endScan(PoolScanHandle *h) {
	if (h->m_page) {
		unlockPage(h->m_userId, h->m_page, Shared);
		h->m_page = NULL;
	}
	delete h;
}

}
