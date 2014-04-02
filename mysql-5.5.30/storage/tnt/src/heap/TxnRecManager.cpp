/**内存堆中事务数组的管理
 * author 忻丁峰 xindingfeng@corp.netease.com
 */
#include "heap/TxnRecManager.h"

namespace tnt {
void TxnRecPage::init() {
	m_prev = m_next = NULL;
	m_maxTxnId = 0;
	m_minTxnId = MAX_U64;
	m_recCnt = 0;
}

TxnRecManager::TxnRecManager(TNTIMPageManager *pageManager): m_lock("TxnRecManager", __FILE__, __LINE__)
{
	m_pageManager = pageManager;
	m_head = m_tail = m_curPage = NULL;
	m_pageSize = 0;
	m_recPerPage = (TxnRecPage::TNTIM_PAGE_SIZE - sizeof(TxnRecPage))/sizeof(TxnRec);
}

TxnRecManager::~TxnRecManager(void)
{
}

void TxnRecManager::init(Session *session) {
	allocPage(session, FIRST_ALLOC_PAGE_SIZE);
	m_curPage = m_head;
}

/** 存放一个事务记录，如果空间不够，则分配新的页
 * @param rec 需要存在的事务记录
 */
TxnRec* TxnRecManager::push(Session *session, TxnRec *rec) {
	TxnRec *ret = NULL;
	TxnRecPage *page = m_curPage; //脏读
	latchPage(session, page, Exclusived);

	//如果页面不能再容纳别的记录
	while (page->m_recCnt == m_recPerPage) {
		unLatchPage(session, page, Exclusived);

		m_lock.lock(Exclusived, __FILE__, __LINE__);
		//重判断，因为此时页面分配可能已经完成了
		if (page == m_curPage) {
			if (m_curPage == m_tail) {
				while(!allocPage(session, 1)) {
					//TODO 在页面不够的情况下需要触发整理内存线程
				}
			}
			m_curPage = m_curPage->m_next;
			assert(m_curPage != NULL);
		}
		page = m_curPage;
		m_lock.unlock(Exclusived);
		latchPage(session, page, Exclusived);
	}
	
	ret = (TxnRec *)((byte *)page + sizeof(TxnRecPage) + m_curPage->m_recCnt*sizeof(TxnRec));
	memcpy(ret, rec, sizeof(TxnRec));
	page->m_recCnt++;
	if (rec->m_txnId > page->m_maxTxnId) {
		page->m_maxTxnId = rec->m_txnId;
	}
	if (rec->m_txnId < page->m_minTxnId) {
		page->m_minTxnId = rec->m_txnId;
	}
	unLatchPage(session, page, Exclusived);

	return ret;
}

/** 整理事务数组，将小于minReadView的页清空放入链表尾部
 * @param minReadView 需要回收事务数组的最大事务号
 */
void TxnRecManager::defrag(Session *session, TrxId minReadView, HashIndexOperPolicy *policy) {
	u32 i = 0;
	TxnRec *txnRec = NULL;
	TxnRecPage *defragPage = NULL;
	TxnRecPage *page = NULL;
	TxnRecPage *nextPage = NULL;
	m_lock.lock(Exclusived, __FILE__, __LINE__);
	for (page = m_head; page != m_curPage; page = nextPage) {
		nextPage = page->m_next;
		latchPage(session, page, Shared);
		if (page->m_maxTxnId < minReadView) {
			if (m_head == page) {
				m_head = page->m_next;
			} else {
				NTSE_ASSERT(page->m_prev != NULL);
				page->m_prev->m_next = page->m_next;
			}
			NTSE_ASSERT(page->m_next != NULL);
			page->m_next->m_prev = page->m_prev;

			if (defragPage == NULL) {
				page->m_next = NULL;
				defragPage = page;
			} else {
				page->m_next = defragPage->m_next;
				defragPage->m_next = page;
			}
		}
		unLatchPage(session, page, Shared);
	}
	m_lock.unlock(Exclusived);

	while (defragPage != NULL) {
		page = defragPage;
		defragPage = page->m_next;
		//遍历该页的所有的事务记录，根据ROWID去hash索引删除相应的索引项
		txnRec = (TxnRec *)((byte *)page + sizeof(TxnRecPage));
		for (i = 0; i < page->m_recCnt; i++, txnRec++) {
			NTSE_ASSERT(policy->remove(txnRec->m_rowId));
		}
		latchPage(session, page, Exclusived);
		page->init();
		unLatchPage(session, page, Exclusived);

		m_lock.lock(Exclusived, __FILE__, __LINE__);
		//只是将页面移至尾部，所以不存在页面数目减少问题，故m_pageSize不变
		m_tail->m_next = page;
		page->m_prev = m_tail;
		m_tail = page;
		m_lock.unlock(Exclusived);
	}
}

/** 申请新的空闲页
 * @param incrSize 需要申请的页数
 */
bool TxnRecManager::allocPage(Session *session, u16 incrSize) {
	//assert(m_lock.isLocked(Exclusived));在init初始化分配空间的时候，不需要加锁
	u16 destSize = m_pageSize + incrSize;
	TxnRecPage *prevPage = NULL;
	TxnRecPage *page = NULL;

	if (m_tail != NULL) {
		prevPage = m_tail;
	}

	for (u16 i = m_pageSize; i < destSize; i++) {
		page = (TxnRecPage *)m_pageManager->getPage(session->getId(), PAGE_MEM_HEAP);
		if (!page) {
			m_tail = prevPage;
			m_lock.unlock(Exclusived);
			return false;
		}

		page->init();
		unLatchPage(session, page, Exclusived);
		if (i == 0) {
			m_head = page;
		} else {
			prevPage->m_next = page;
			page->m_prev = prevPage;
		}

		if (i == destSize -1) {
			m_tail = page;
			break;
		}
		prevPage = page;
	}
	m_pageSize = destSize;
	return true;
}

/** 释放空闲页面
 * @param 期待释放的页面数
 * return 真正释放的空闲页面数
 */
u16 TxnRecManager::freeSomePage(Session *session, u16 size) {
	u16 freePageSize = 0;
	TxnRecPage *prevPage = NULL;
	m_lock.lock(Exclusived, __FILE__, __LINE__);
	for(TxnRecPage *page = m_tail; page != m_curPage && freePageSize < size; page = prevPage) {
		latchPage(session, page, Exclusived);
		prevPage = page->m_prev;
		m_pageManager->releasePage(session->getId(), page);
		prevPage->m_next = NULL;
		freePageSize++;
	}
	m_tail = prevPage;
	m_lock.unlock(Exclusived);
	m_pageSize -= freePageSize;

	return freePageSize;
}
}
