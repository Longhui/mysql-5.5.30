/**
* TNTIM�ڴ�ҳ�����
*
* @author ��ΰ��(liweizhao@corp.netease.com)
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
 * ���һ�������ڴ�ҳ
 * @post ������ɹ������������ҳ�Ѿ�����Exclusived��
 * @param userId �û�ID
 * @param pageType ҳ������
 * @param force �Ƿ�ʹ����Ŀ���СҲǿ�Ʒ���
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
 * �ͷ�һ���ڴ�ҳ
 * @param page �ڴ�ҳ
 */
void TNTIMPageManager::releasePage(u16 userId, TNTIMPage* page) {
	//assert(isPageLatched(page, Exclusived));

	//MutexGuard mutexGuard(&m_lock, __FILE__, __LINE__);
	//addToPageFreeList(page);
	//unlatchPage(userId, page, Exclusived);
	freePage(userId, page);
}

/**
 * ʵ��PagePoolUser::freeSomePages�ӿ�
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
 * ��һ��ҳ����ӵ�����ҳ������
 * @param page �ڴ�ҳ
 */
/*void TNTIMPageManager::addToPageFreeList(TNTIMPage *page) {
	m_pool->setInfoAndType(page, NULL, PAGE_EMPTY);
	((FreeListLink *)page)->m_next = m_freePageList;
	m_freePageList = page;
}*/

/**
* ���һ�������ڴ�ҳ
* @post ������ɹ������������ҳ�Ѿ�����Exclusived��
* @param userId �û�ID
* @param pageType ҳ������
* @param force �Ƿ�ʹ����Ŀ���СҲǿ�Ʒ���
*/
//TNTIMPage* TNTIMPageManager::getPageFromFreeList(u16 userId, PageType pageType, bool needLock /*=true*/) {
/*	assert(PAGE_MEM_HEAP == pageType || PAGE_MEM_INDEX == pageType);
	if (needLock)
		LOCK(&m_lock);
	assert(m_lock.isLocked());
	TNTIMPage *rtn = NULL;
	if (m_freePageList) {//��������ǿ�
		rtn = m_freePageList;
		m_freePageList = ((FreeListLink *)m_freePageList)->m_next;
	}
	if (rtn) {
		latchPage(userId, rtn, Exclusived,__FILE__, __LINE__);//һ���ɹ�
		m_pool->setInfoAndType(rtn, NULL, pageType);
	}	
	if (needLock)
		UNLOCK(&m_lock);
	return rtn;
}*/
}