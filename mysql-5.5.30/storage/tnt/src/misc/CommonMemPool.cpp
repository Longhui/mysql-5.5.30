/**
 * 通用内存池
 *
 * @author 李伟钊(liweizhao@corp.netease.com)
 */
#include "misc/CommonMemPool.h"

namespace ntse {

uint CommonMemPool::freeSomePages(u16 userId, uint numPages) {
	UNREFERENCED_PARAMETER(userId);
	UNREFERENCED_PARAMETER(numPages);
	return 0;
}

void* CommonMemPool::getPage(bool force) {
	void *page = NULL;
	if (force) {
		page = allocPageForce(VIRTUAL_USER_ID_COM_POOL, PAGE_COMMON_POOL, NULL);
	} else {
		page = allocPage(VIRTUAL_USER_ID_COM_POOL, PAGE_COMMON_POOL, NULL);
	}
	//if (page)
	//	m_pool->unlockPage(0, page, Exclusived);
	return page;
}

void CommonMemPool::releasePage(void *page) {
	//MutexGuard mutexGuard(&m_lock, __FILE__, __LINE__);
	//m_pool->lockPage(0, page, Exclusived, __FILE__, __LINE__);
	assert(m_pool->isPageLocked(page, Exclusived));
	freePage(0, page);
}

}