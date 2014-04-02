/**
* TNTIM内存页面管理
*
* @author 李伟钊(liweizhao@corp.netease.com)
*/
#ifndef _TNTIM_PAGE_MANAGER_H_ 
#define _TNTIM_PAGE_MANAGER_H_

#include "util/PagePool.h"
#include "misc/Global.h"

using namespace ntse;

namespace tnt {

/** TNTIM 内存页面句柄，不允许实例化! */
class TNTIMPage {
public:
	static const uint TNTIM_PAGE_SIZE = ntse::Limits::PAGE_SIZE; //TNT内存页面大小

protected:
	TNTIMPage() {}
	~TNTIMPage() {}
};

#define LATCH_TNTIM_PAGE_IF_TYPE(session, pageManager, page, lockMode, pageType, timeoutMs) \
	(pageManager)->latchPageIfType(session->getId(), page, lockMode, pageType, timeoutMs, __FILE__, __LINE__)
#define LATCH_TNTIM_PAGE(session, pageManager, page, lockMode) \
	(pageManager)->latchPage(session->getId(), page, lockMode, __FILE__, __LINE__)
#define TRY_LATCH_TNTIM_PAGE(session, pageManager, page, lockMode) \
	(pageManager)->tryLatchPage(session->getId(), page, lockMode, __FILE__, __LINE__)
#define UNLATCH_TNTIM_PAGE(session, pageManager, page, lockMode) \
	(pageManager)->unlatchPage(session->getId(), page, lockMode)
#define TRY_UPGRADE_LATCH_TNTIM_PAGE(session, pageManager, page) \
	(pageManager)->tryUpgradePageLatch(session->getId(), page, __FILE__, __LINE__)

/** TNTIM内存页面管理 */
class TNTIMPageManager : public PagePoolUser {
private:
	//空闲链表结点
	struct FreeListLink {
		TNTIMPage *m_next;
	};

public:
	TNTIMPageManager(uint targetSize, uint totalBorrowSize, PagePool *pool);
	virtual ~TNTIMPageManager();

	TNTIMPage* getPage(u16 userId, PageType pageType, bool force = false);
	void releasePage(u16 userId, TNTIMPage* page);
	virtual uint freeSomePages(u16 userId, uint numPages);
	
	/**
	* 如果所给地址为指定类型的内存页则锁定该页
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
	inline bool latchPageIfType(u16 userId, TNTIMPage *page, LockMode lockMode, PageType pageType, 
		int timeoutMs, const char *file, uint line) {
			assert(PAGE_MEM_HEAP <= pageType && PAGE_MEM_INDEX >= pageType);
			return m_pool->lockPageIfType(userId, page, lockMode, pageType, timeoutMs, file, line);
	}

	/**
	 * 判断一个页面是否用指定的锁模式锁定
	 * @param page 内存页
	 * @param lockMode 锁模式
	 * @return 
	 */
	inline bool isPageLatched(TNTIMPage *page, LockMode lockMode) {
		return m_pool->isPageLocked(page, lockMode);
	}
	
	/**
	 * 获得当前页面加latch的模式
	 * @param page 内存页
	 * @return 锁模式
	 */
	inline LockMode getPageLatchMode(TNTIMPage *page) const {
		return m_pool->getPageLockMode(page);
	}

	/**
	* 锁定一个内存页
	*
	* @param userId 用户ID
	* @param page 内存页
	* @param lockMode 锁模式
	* @param file 发出加锁操作的代码文件名
	* @param line 发出加锁操作的代码行号
	*/
	inline void latchPage(u16 userId, TNTIMPage *page, LockMode lockMode, const char *file, uint line) {
		m_pool->lockPage(userId, page, lockMode, file, line);
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
	inline bool tryLatchPage(u16 userId, TNTIMPage *page, LockMode lockMode, const char *file, uint line) {
		return m_pool->trylockPage(userId, page, lockMode, file, line);
	}

	/**
	* 解除对一个内存页的锁定
	*
	* @param userId 用户ID
	* @param page 内存页
	* @param lockMode 锁模式
	*/
	inline void unlatchPage(u16 userId, TNTIMPage *page, LockMode lockMode) {
		m_pool->unlockPage(userId, page, lockMode);
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
	inline bool tryUpgradePageLatch(u16 userId, TNTIMPage *page, const char *file, uint line) {
		return m_pool->tryUpgradePageLock(userId, page, file, line);
	}

	/**
	* 获取TNTIM针对大型事务的最大页面数
	*
	* @return 最大页面数
	*/
	inline uint getMaxSizeForBigTrx() {
		return m_maxSizeForBigTrx;
	}
	
	/**
	* 获取TNTIM针对小型事务的最大页面数
	*
	* @return 最大页面数
	*/
	inline uint getMaxSizeForSmallTrx() {
		return m_maxSizeForSmallTrx;
	}


	/**
	* 检查TNTIM大小是否达到借页面的上限
	*
	* @param bigTrx 是否针对大事务
	* @return 达到上限返回true，否则返回false
	*/
	inline bool reachedUpperBound(bool bigTrx) {
		u32 maxTntimSize = bigTrx? m_maxSizeForBigTrx: m_maxSizeForSmallTrx;
		return m_currentSize >= maxTntimSize;
	}
private:
	//void addToPageFreeList(TNTIMPage *page);
	//TNTIMPage* getPageFromFreeList(u16 userId, PageType pageType, bool needLock = true);

//private:
	//Mutex        m_lock;         /* 保护全局结构的互斥锁 */
	//TNTIMPage	 *m_freePageList;/* 空闲页面链表 */
	uint			m_maxSizeForBigTrx;							 //针对大型事务，页面数上限
	uint			m_maxSizeForSmallTrx;						 //针对小型事务，页面数上限

	static const uint TNTIM_UPPBOUND_BIGTRX = 10;				 //针对大型事务，能够借用的NTSE PAGE BUFFER大小比例的上限
	static const uint TNTIM_UPPBOUND_SMALLTRX = 30;				 //针对小型事务，能够借用的NTSE PAGE BUFFER大小比例的上限
};

}

#endif