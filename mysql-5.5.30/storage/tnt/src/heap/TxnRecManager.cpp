/**�ڴ������������Ĺ���
 * author �ö��� xindingfeng@corp.netease.com
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

/** ���һ�������¼������ռ䲻����������µ�ҳ
 * @param rec ��Ҫ���ڵ������¼
 */
TxnRec* TxnRecManager::push(Session *session, TxnRec *rec) {
	TxnRec *ret = NULL;
	TxnRecPage *page = m_curPage; //���
	latchPage(session, page, Exclusived);

	//���ҳ�治�������ɱ�ļ�¼
	while (page->m_recCnt == m_recPerPage) {
		unLatchPage(session, page, Exclusived);

		m_lock.lock(Exclusived, __FILE__, __LINE__);
		//���жϣ���Ϊ��ʱҳ���������Ѿ������
		if (page == m_curPage) {
			if (m_curPage == m_tail) {
				while(!allocPage(session, 1)) {
					//TODO ��ҳ�治�����������Ҫ���������ڴ��߳�
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

/** �����������飬��С��minReadView��ҳ��շ�������β��
 * @param minReadView ��Ҫ���������������������
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
		//������ҳ�����е������¼������ROWIDȥhash����ɾ����Ӧ��������
		txnRec = (TxnRec *)((byte *)page + sizeof(TxnRecPage));
		for (i = 0; i < page->m_recCnt; i++, txnRec++) {
			NTSE_ASSERT(policy->remove(txnRec->m_rowId));
		}
		latchPage(session, page, Exclusived);
		page->init();
		unLatchPage(session, page, Exclusived);

		m_lock.lock(Exclusived, __FILE__, __LINE__);
		//ֻ�ǽ�ҳ������β�������Բ�����ҳ����Ŀ�������⣬��m_pageSize����
		m_tail->m_next = page;
		page->m_prev = m_tail;
		m_tail = page;
		m_lock.unlock(Exclusived);
	}
}

/** �����µĿ���ҳ
 * @param incrSize ��Ҫ�����ҳ��
 */
bool TxnRecManager::allocPage(Session *session, u16 incrSize) {
	//assert(m_lock.isLocked(Exclusived));��init��ʼ������ռ��ʱ�򣬲���Ҫ����
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

/** �ͷſ���ҳ��
 * @param �ڴ��ͷŵ�ҳ����
 * return �����ͷŵĿ���ҳ����
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
