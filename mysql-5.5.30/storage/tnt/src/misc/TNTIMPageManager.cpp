/**
* TNTIM内存页面管理
*
* @author 李伟钊(liweizhao@corp.netease.com)
*/
#include "misc/TNTIMPageManager.h"

namespace tnt {

TNTIMPageManager::TNTIMPageManager(uint targetSize, uint totalBorrowSize, PagePool *pool) : PagePoolUser(targetSize, pool)/*, 
	m_lock("meta lock of TNTIM pages manager", __FILE__, __LINE__)*/ {
	//m_freePageList = NULL;
	m_maxSizeForBigTrx = targetSize + totalBorrowSize * TNTIM_UPPBOUND_BIGTRX;
	m_maxSizeForSmallTrx = targetSize + totalBorrowSize * TNTIM_UPPBOUND_SMALLTRX;
}
TNTIMPageManager::~TNTIMPageManager() {
}

/**
 * 获得一个空闲内存页
 * @post 若申请成功，则新申请的页已经加上Exclusived锁
 * @param userId 用户ID
 * @param pageType 页面类型
 * @param force 是否即使超出目标大小也强制分配
 */
TNTIMPage* TNTIMPageManager::getPage(u16 userId, PageType pageType, bool force/* = false*/) {
	assert(PAGE_MEM_HEAP <= pageType && PAGE_MEM_INDEX >= pageType);
	TNTIMPage *page = NULL;
	//TNTIMPage *page = getPageFromFreeList(userId, pageType);
	//if (!page) {
	if (force) {
		page = (TNTIMPage *)allocPageForce(userId, pageType, NULL);
	} else {
		page = (TNTIMPage *)allocPage(userId, pageType, NULL);
	}
	//}
	return page;
}

/**
 * 释放一个内存页
 * @param page 内存页
 */
void TNTIMPageManager::releasePage(u16 userId, TNTIMPage* page) {
	//assert(isPageLatched(page, Exclusived));

	//MutexGuard mutexGuard(&m_lock, __FILE__, __LINE__);
	//addToPageFreeList(page);
	//unlatchPage(userId, page, Exclusived);
	freePage(userId, page);
}

/**
 * 实现PagePoolUser::freeSomePages接口
 */
uint TNTIMPageManager::freeSomePages(u16 userId, uint numPages) {
	UNREFERENCED_PARAMETER(userId);
	UNREFERENCED_PARAMETER(numPages);

	/*uint count = PagePoolUser::freeSomeFreePages(userId, numPages);
	if (count >= numPages)
		return count;
	LOCK(&m_lock);
	while (count < numPages) {
		TNTIMPage* page = getPageFromFreeList(userId, PAGE_EMPTY, false);
		if (!page)
			break;
		PagePoolUser::freePage(userId, page);
		count++;
	}
	UNLOCK(&m_lock);
	return count;*/
	return 0;
}

/**
 * 将一个页面添加到空闲页面链表
 * @param page 内存页
 */
/*void TNTIMPageManager::addToPageFreeList(TNTIMPage *page) {
	m_pool->setInfoAndType(page, NULL, PAGE_EMPTY);
	((FreeListLink *)page)->m_next = m_freePageList;
	m_freePageList = page;
}*/

/**
* 获得一个空闲内存页
* @post 若申请成功，则新申请的页已经加上Exclusived锁
* @param userId 用户ID
* @param pageType 页面类型
* @param force 是否即使超出目标大小也强制分配
*/
//TNTIMPage* TNTIMPageManager::getPageFromFreeList(u16 userId, PageType pageType, bool needLock /*=true*/) {
/*	assert(PAGE_MEM_HEAP == pageType || PAGE_MEM_INDEX == pageType);
	if (needLock)
		LOCK(&m_lock);
	assert(m_lock.isLocked());
	TNTIMPage *rtn = NULL;
	if (m_freePageList) {//空闲链表非空
		rtn = m_freePageList;
		m_freePageList = ((FreeListLink *)m_freePageList)->m_next;
	}
	if (rtn) {
		latchPage(userId, rtn, Exclusived,__FILE__, __LINE__);//一定成功
		m_pool->setInfoAndType(rtn, NULL, pageType);
	}	
	if (needLock)
		UNLOCK(&m_lock);
	return rtn;
}*/
}