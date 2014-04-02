/**
 * �ڴ�ҳ�ع���
 *
 * @author ��Դ(wangyuan@corp.netease.com, wy@163.org)
 */

#ifndef _NTSE_PAGEPOOL_H_
#define _NTSE_PAGEPOOL_H_

#include "misc/Global.h"
#include "util/Sync.h"
#include "util/DList.h"
#include "misc/Trace.h"
#include "util/Thread.h"

namespace ntse {

/** ҳ���� */
enum PageType {
	PAGE_EMPTY = 0,		/** δʹ�õ��ڴ�ҳ */
	PAGE_MMS_PAGE,		/** MMS�����ڴ洢��¼���ݵ�ҳ */
	PAGE_MMS_UPDATE,	/** MMS�����ڴ洢������Ϣ��ҳ */
	PAGE_MMS_MISC,		/** MMS�е��������ݣ���ӳ���ռ�õ�ҳ�� */
	PAGE_HEAP,			/** ��ҳ */
	PAGE_INDEX,			/** ����ҳ */
	PAGE_TXNLOG,		/** ��־ҳ */
	PAGE_LOB_INDEX,     /**Ŀ¼�ļ�ҳ*/
	PAGE_LOB_HEAP,      /**������ļ�ҳ*/
	PAGE_COMMON_POOL,   /** ͨ���ڴ��ҳ�� */
	PAGE_MEM_HEAP,      /** TNTIM�ڴ��ҳ */
	PAGE_HASH_INDEX,    /** TNTIM��ϣ����ҳ*/
	PAGE_SCAN,          /** Scanҳ�� */
	PAGE_DUMP,          /** Dumpҳ�� */
	PAGE_MEM_INDEX,     /** TNTIM�ڴ�����ҳ */
	PAGE_MAX,
};

Tracer& operator << (Tracer &tracer, PageType pageType);
extern const char* getPageTypeStr(PageType type);

/**
 * ʹ���ڴ�ҳ�ص��û���
 * 
 * �ڴ�ҳ�ص���Ҫ������ά��һ���ڴ�ҳ��Ŀǰʹ���˶����ڴ�ҳ��
 * ͬʱ�ṩ����ҳ���ά�����ܡ�
 * 
 * ���յ�ҳ����������ڴ�ҳ����ɵģ���������ÿ���ڴ�ҳ����
 * �ͷŲ�����������ڴ�ء���һ���ڴ�ҳ�صĵ�ǰ��С��������Ŀ��
 * ��Сʱ���ڴ�ҳ���û��Ὣ���ͷŵ�ҳ���¼�ڸ��û���Ӧ��һ��
 * ����ҳ�����У������������ͷŵ��ڴ���С�������Ƶ�Ŀ���Ƿ�ֹ
 * freeSomePages������Ƶ���ĵ��á���һ���棬���һ���ڴ�ҳ���û�
 * ��ȷʵ�д����Ŀ���ҳ�棬����Щҳ�������Ͽ���Ϊ�����ڴ�ҳ��
 * �û�ʹ�ã�������ܡ��������ʹ�õķ������ڴ�ҳ�ػ�����һ��
 * ����ҳ������������������ҳ̫�࣬��ʹ��ǰ��С������Ŀ��
 * ��С��Ҳ�����ͷŵ�ҳ���ظ��ڴ�ء�
 */
class PagePool;
class PagePoolUser {
protected:
	PagePoolUser(uint targetSize, PagePool *pool);
	virtual ~PagePoolUser() {}

public:
	/**
	 * �ڴ�ҳ�ع�����������һ�ӿ�Ҫ���ʹ��ͳһ�ڴ�ҳ�ص�ĳ�û��ͷ�
	 * һЩ�ڴ�ҳ��Ϊ�����û��ڳ��ռ䡣�ڴ�ҳ�ع�����ֻ���ڸ��û���
	 * ��ǰ��С������Ŀ���Сʱ�Ż������һ������
	 * 
	 * @param userId �û�ID
	 * @param numPages �����ͷŵ�ҳ����
	 * @return ʵ���ͷŵ�ҳ����
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
	static const uint CACHE_SIZE = 100;	/** ������ô��Ŀ���ҳ */

protected:
	PagePool	*m_pool;	/** �����ڴ�ҳ�� */
	uint	m_targetSize;	/** ��ҳΪ��λ��Ŀ���С */
	uint	m_currentSize;	/** ��ҳΪ��λ�ĵ�ǰ��С����������Ŀ���ҳ */

private:
	bool verify();

	Mutex	m_mutex;		/** ������������ */
	void	*m_freeList;	/** ����ҳ����ͷ */
	uint	m_numFreePages;	/** ����ҳ���� */
	bool	m_wantMore;		/** ���޸�����ڴ����� */
#ifdef TNT_ENGINE
	uint    m_cacheSize;    /** ��໺����ô��Ŀ���ҳ */
#endif

friend class PagePool;
};

/** �ڴ�ҳ���и�ҳ��������Ϣ */
struct PageDesc {
	u32			m_magic;	/** ħ�������ṹ���ݱ��Ƿ��޸� */
	LURWLock	m_lock;		/** �������ҳ���� */
	void		*m_info;	/** �û��Զ�����Ϣ */
	PageType	m_type;		/** ҳ���� */
	PagePoolUser	*m_user;/** ʹ�����ҳ���û� */

	static const u32	MAGIC_NUMBER = 0x39785624;

	PageDesc(u16 maxUserId): m_lock(maxUserId, "PageDesc::lock", __FILE__, __LINE__) {
		m_info = NULL;
		m_magic = MAGIC_NUMBER;
		m_type = PAGE_EMPTY;
	}
};

struct PoolScanHandle;
class PoolRebalancer;
/** �ڴ�ҳ��
 * �ڴ�ҳ��ģ�����Ҫ������ҳ��ķ������ͷţ����ṩ��ҳ��Ķ�д�����ܡ�
 * �ڴ�ҳ�ع�����ڴ�ҳ��С����Ϊ2���������ݣ����ڴ�ҳ�ط����ҳ���ַ
 * �Ѿ���ϵͳ���̿��С���룬������DIRECT_IO���ļ���д��
 *
 * ע:
 * ���ĳ�����͵�ҳ���ʹ��lockPageIfType������ҳ�棬�������ͷ�����
 * ҳ��ʱ����ʹ�û���������ҳ�棬���߽��ж����ͬ�����ơ�������ܲ���
 * ���µĴ����������(����T1,T2��ʾ�����߳�):
 *
 * ��ҳ��p��ǰΪT���ͣ�
 * T1: ����lockPageIfType(p, Shared, T, ...)
 *     ����p��ǰΪT���ͣ������ɹ�
 * T2: ����freePage(p)�ͷ�p���ͷź�p�������ΪPAGE_EMPTY��
 * ������T1������p����p�����ͺ����ֱ��PAGE_EMPTY
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
	 * ����һ���ڴ�ҳ
	 *
	 * @param userId �û�ID
	 * @param page �ڴ�ҳ
	 * @param lockMode ��ģʽ
	 * @param file �������������Ĵ����ļ���
	 * @param line �������������Ĵ����к�
	 */
	inline void lockPage(u16 userId, void *page, LockMode lockMode, const char *file, uint line) {
		ftrace(ts.pool, tout << userId << page << lockMode << file << line);
		PageDesc *desc = pageToDesc(page);
		desc->m_lock.lock(userId, lockMode, file, line);
	}

	/**
	 * ��������һ���ڴ�ҳ
	 *
	 * @param userId �û�ID
	 * @param page �ڴ�ҳ
	 * @param lockMode ��ģʽ
	 * @param file �������������Ĵ����ļ���
	 * @param line �������������Ĵ����к�
	 * @return �ɹ����
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
	 * �����һ���ڴ�ҳ������
	 *
	 * @param userId �û�ID
	 * @param page �ڴ�ҳ
	 * @param lockMode ��ģʽ
	 */
	inline void unlockPage(u16 userId, void *page, LockMode lockMode) {
		ftrace(ts.pool, tout << userId << page << lockMode);
		PageDesc *desc = pageToDesc(page);
		desc->m_lock.unlock(userId, lockMode);
	}

	bool isPageLocked(void *page, LockMode lockMode);
	LockMode getPageLockMode(void *page) const;
	
	/**
	 * �õ�ĳ�ڴ�ҳ��Ӧ���û��Զ�������
	 * @pre ����Ϊʹ���е�ҳ��
	 *
	 * @param page �ڴ�ҳ
	 * @return �û��Զ�������
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
	 * ����һҳ�ڵ�ַ���õ��������ڴ�ҳ��ҳ�׵�ַ
	 *
	 * @param addr ҳ�ڵ�ַ
	 * @return ҳ�׵�ַ
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
	Mutex	m_mutex;				/** ���������������� */
	size_t	m_pageSize;				/** ҳ��С */
	size_t	m_psPower;				/** ҳ��С��2�Ķ��ٴ��� */
	size_t	m_size;					/** ��ҳ������ʾ���ܴ�С */
	void	*m_freeList;			/** ָ���һ��δʹ�õ�ҳ */
	uint	m_numFreePages;			/** ����ҳ���� */
	bool	m_inited;				/** �Ƿ��Ѿ���ʼ������ */
#ifdef NTSE_MEM_CHECK
	void	**m_pages;				/** ҳ���飬ҳ����������ҳ������֮�� */
#else
	PageDesc	*m_pageDescs;		/** ҳ�������� */
	void	*m_pages;				/** ҳ���� */
	void	*m_pagesGuard;			/** ҳ���ַ���Ͻ� */
	void	*m_pagesMemStart;		/** ҳ������������ڴ����ʼ��ַ */
#endif
	u16		m_maxUserId;			/** ����ʱʹ�õ��û�ID�����ֵ */
	DList<PagePoolUser *>	m_users;/** ʹ����һ�ڴ�ҳ�ص��û� */
	PoolRebalancer	*m_rebalancer;	/** Э�����û�ռ�ÿռ�ĺ�̨�߳� */
friend class PagePoolUser;
friend class PoolRebalancer;
};

/** ����Э�����ڴ�ҳ���û�֮���ڴ�ռ�õĺ�̨�߳� */
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

