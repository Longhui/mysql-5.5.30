/**
 * MMSҳ�ѹ���
 *
 * @author �۷�(shaofeng@corp.netease.com, sf@163.org)
 */

#include "mms/Mms.h"
#include "mms/MmsHeap.h"
#include "mms/MmsPage.h"

namespace ntse {

/************************************************************************/
/*					    ���ϻ���ҳ��                                    */
/************************************************************************/

/** 
 * ���캯��
 *
 * @param mms MMSȫ�ֶ���
 * @param rpClass ������ҳ����� 
 * @param freqHeap ���Ƶ��ҳ��
 */
MmsOldestHeap::MmsOldestHeap(Mms *mms, MmsRPClass *rpClass, MmsFreqHeap *freqHeap) {
	UNREFERENCED_PARAMETER(mms);
	m_rpClass = rpClass;
	m_freqHeap = freqHeap;
	m_freqIdx = (u32)-1;
	m_currSize = 0;
	assert(m_heapArray.getSize() == m_currSize);
};

/**
 * ҳ����ҳ�����ƣ��������ϻ���ҳ�Ѻ���СƵ��ҳ�ѣ�
 *
 * @param session �Ự
 * @param pos ҳ�ǰλ��
 * @param pageLocked ����ҳ�Ƿ����
 * @param lockedRecPage �Ѽ�����ҳ
 * @param topHeapChanged �����Ƿ����
 */
void MmsOldestHeap::moveDown(Session *session, u32 pos, bool pageLocked, MmsRecPage *lockedRecPage, bool topHeapChanged) {
	assert (pos < m_currSize);

	OldestHeapItem *heapItem = &m_heapArray[pos];
	MmsRecPage *recPage = heapItem->m_page;
	bool freqHeapChangeNeeded;
	bool topPageLocked;

	if (pageLocked && pos == 0) {
		freqHeapChangeNeeded = true;
		topPageLocked = true;
	} else {
		freqHeapChangeNeeded = false;
		topPageLocked = false;
	}

	// ��ȡҳ����Сʱ���,ע�⣺�������������Ҫ������Сʱ���
	if (pageLocked)
		MmsPageOper::getRecPageTs(recPage, &heapItem->m_tsMin, NULL, NULL);

	// ����
	u32 left = leftChild(pos);
	while (left < m_currSize) {
		if (m_heapArray[pos].m_tsMin > m_heapArray[left].m_tsMin) {
			if (left + 1 < m_currSize && m_heapArray[left].m_tsMin > m_heapArray[left + 1].m_tsMin) {
				swap(pos, left + 1);
				pos = left + 1;
				topPageLocked = false;
			} else {
				swap(pos, left);
				pos = left;
				topPageLocked = false;
			}
		} else 
			break;
		left = leftChild(pos);
	}

	assert(m_freqIdx != (u32)-1);

	// ����Ƶ��ҳ��
	if (topHeapChanged || freqHeapChangeNeeded)
		m_freqHeap->moveDown(session, m_freqIdx, topPageLocked, lockedRecPage);
}

/**
 * ��ҳ�Ѳ���һ����¼ҳ���ڲ�Լ�����ջ���ҳ���ܵ��øú���
 * @pre ��¼ҳ�Ѽ���
 *
 * @param recPage ������ļ�¼ҳ
 */
void MmsOldestHeap::insert(MmsRecPage *recPage) {
	m_rpClass->m_status.m_pageInserts++;

	OldestHeapItem oldestItem;

	oldestItem.m_page = recPage;
	MmsPageOper::getRecPageTs(recPage, &oldestItem.m_tsMin, NULL, NULL);
	if (!m_heapArray.push(oldestItem))
		NTSE_ASSERT(false);
	recPage->m_oldestHeapIdx = m_currSize;
	if (m_currSize == 0) {
		assert(m_freqIdx == (u32)-1);
		m_freqHeap->insert(this);
	} 
	m_currSize++;
	assert(m_heapArray.getSize() == m_currSize);
}

/**
 * ��ҳ��ɾ��һ��ҳ��
 *
 * @apram session �Ự
 * @param pos ҳ�ǰλ��
 * @param lockedRecPage �Ѽ�����ҳ
 */
void MmsOldestHeap::del(Session *session, u32 pos, MmsRecPage *lockedRecPage) {
	m_rpClass->m_status.m_pageDeletes++;

	m_heapArray[pos].m_page->m_oldestHeapIdx = (u32) -1;
	if (pos == m_currSize - 1) {
		m_heapArray.pop();
		if (--m_currSize == 0) {
			// ����СƵ��ҳ��ɾ��
			m_freqHeap->del(m_freqIdx);
		}
	} else {
		m_heapArray[pos] = m_heapArray[--m_currSize];
		m_heapArray[pos].m_page->m_oldestHeapIdx = pos;
		m_heapArray.pop();
		if (pos)
			moveDown(session, pos, false, lockedRecPage);
		else
			moveDown(session, pos, false, lockedRecPage, true);
	}
	assert(m_heapArray.getSize() == m_currSize);
}

/**
 * ҳ�������ڵ㻥��
 *
 * @param pos1 �ڵ�λ��1
 * @param pos2 �ڵ�λ��2
 */
void MmsOldestHeap::swap(const int &pos1, const int &pos2) {
	OldestHeapItem tmp = m_heapArray[pos1];
	m_heapArray[pos1] = m_heapArray[pos2];
	m_heapArray[pos2] = tmp;
	// ע�⣺���ﲻ��֤�Ƿ��ҳ������Ϊm_oldestHeapIdxֻ��ͬʱ��һ���߳�ʹ�ã�������ҳ������
	// �ڲ�Լ����Ϊ��ֹ���²�����������ҳ������ݲ��滻/ɾ��֮ǰ�����ȱ���ɾ��ҳ������ص�ҳ��
	m_heapArray[pos1].m_page->m_oldestHeapIdx = pos1;
	m_heapArray[pos2].m_page->m_oldestHeapIdx = pos2;
}

/** 
 * ��ȡ��¼ҳ
 *
 * @param idx ������
 * @return ��¼ҳ
 */
MmsRecPage* MmsOldestHeap::getPage(size_t idx) {
	if (!m_currSize) return NULL;

	return m_heapArray[idx].m_page;
}

/** 
 * �õ��Ѿ�ռ�õĸ���ҳ����
 *
 * @return �Ѿ�ռ�õĸ���ҳ����
 */
u64 MmsOldestHeap::getPagesOccupied() {
	assert (m_currSize == m_heapArray.getSize());
	return m_heapArray.getSize();
}

/************************************************************************/
/*					    ��ͷ���Ƶ�ʻ���ҳ��                            */
/************************************************************************/

/** 
 * ���캯��
 * 
 * @param mms MMSȫ�ֶ���
 * @param mmsTable MMS�����
 */
MmsFreqHeap::MmsFreqHeap(Mms *mms, MmsTable *mmsTable) { 
	UNREFERENCED_PARAMETER(mms);
	m_mmsTable = mmsTable;
	m_currSize = 0;
	assert(m_heapArray.getSize() == m_currSize);
}

/**
 * ҳ����ҳ������(�������ĵ�ǰ������)
 *
 * @param session �Ự
 * @param pos ҳ�ǰλ��
 * @param pageLocked ҳ����صļ�¼ҳ�Ƿ��Ѽ�ҳ��
 * @param lockedRecPage �Ѽ�����ҳ
 */
void MmsFreqHeap::moveDown(Session *session, u32 pos, bool pageLocked, MmsRecPage *lockedRecPage) {
	FreqHeapItem *freqItem = &m_heapArray[pos];
	MmsRecPage *recPage = freqItem->m_oldestHeap->m_heapArray[0].m_page;

	// �ع�ҳ��
	if (!pageLocked && (lockedRecPage != recPage)) {
		MMS_LOCK_REC_PAGE(session, m_mmsTable->m_mms, recPage);
		freqItem->m_page = recPage;
		MmsPageOper::getRecPageTs(freqItem->m_page, &freqItem->m_tsMin, &freqItem->m_tsMax, &freqItem->m_numRecs);
		MmsPageOper::unlockRecPage(session, m_mmsTable->m_mms, recPage);
	} else {
		freqItem->m_page = recPage;
		MmsPageOper::getRecPageTs(freqItem->m_page, &freqItem->m_tsMin, &freqItem->m_tsMax, &freqItem->m_numRecs);
	}

	// ����
	doMoveDown(pos);
}

/**
 * ҳ������
 *
 * @param pos ҳ�ǰλ��
 */
void MmsFreqHeap::doMoveDown(u32 pos) {
	u32 left = leftChild(pos);
	while (left < m_currSize) {
		if (itemCmp(pos, left)) {
			if (left + 1 < m_currSize && itemCmp(left, left + 1)) {
				swap(pos, left + 1);
				pos = left + 1;
			} else {
				swap(pos, left);
				pos = left;
			}
		} else 
			break;
		left = leftChild(pos);
	}
}

/**
 * ��ҳ�Ѳ���һ����¼ҳ
 *
 * @param oldestItem �������ҳ��
 */
void MmsFreqHeap::insert(MmsOldestHeap *oldestHeap) {
	FreqHeapItem freqItem;

	freqItem.m_oldestHeap = oldestHeap;
	freqItem.m_page = oldestHeap->m_heapArray[0].m_page;
	MmsPageOper::getRecPageTs(freqItem.m_page, &freqItem.m_tsMin, &freqItem.m_tsMax, &freqItem.m_numRecs);
	if (!m_heapArray.push(freqItem))
		NTSE_ASSERT(false);
	oldestHeap->m_freqIdx = m_currSize++;
	assert(m_heapArray.getSize() == m_currSize);
}

/**
 * ��ҳ��ɾ��һ��ҳ��
 *
 * @param pos ҳ�ǰλ��
 */
void MmsFreqHeap::del(u32 pos) {
	m_heapArray[pos].m_oldestHeap->m_freqIdx = (u32) -1;
	if (pos == m_currSize - 1) {		
		m_heapArray.pop();
		m_currSize--;
	} else {
		m_heapArray[pos] = m_heapArray[--m_currSize];
		m_heapArray[pos].m_oldestHeap->m_freqIdx = pos;
		m_heapArray.pop();
		// ע�⣺���ﲻ�ܵ���moveDown���������������������ͬ�����ҳͬʱ���������
		doMoveDown(pos);
	}
	assert(m_heapArray.getSize() == m_currSize);
}

/** 
 * ��ȡ��¼ҳ
 *
 * @param idx ������
 * @return ��¼ҳ
 */
MmsRecPage* MmsFreqHeap::getPage(int idx) {
	if (!m_currSize) return NULL;

	return m_heapArray[idx].m_page;
}

/**
 * ҳ�������ڵ㻥��
 *
 * @param pos1 �ڵ�λ��1
 * @param pos2 �ڵ�λ��2
 */
void MmsFreqHeap::swap(const u32 &pos1, const u32 &pos2) {
	FreqHeapItem tmp = m_heapArray[pos1];
	m_heapArray[pos1] = m_heapArray[pos2];
	m_heapArray[pos2] = tmp;
	
	m_heapArray[pos1].m_oldestHeap->m_freqIdx = pos1;
	m_heapArray[pos2].m_oldestHeap->m_freqIdx = pos2;
}

/**
 * �Ƚ�����ҳ��,�������FPage����
 *
 * @param pos1 λ��1
 * @param pos2 λ��2
 * @return ���pos1 > pos2������1; ���򷵻�0
 */
bool MmsFreqHeap::itemCmp(u32 pos1, u32 pos2) {
	FreqHeapItem *item1 = &m_heapArray[pos1];
	FreqHeapItem *item2 = &m_heapArray[pos2];
	u32	currTs = System::fastTime();
	float fPage1, fPage2;

	//fPage1 = MmsPageOper::computeFPage(currTs, item1->m_tsMin, item1->m_tsMax, item1->m_numRecs);
	//fPage2 = MmsPageOper::computeFPage(currTs, item2->m_tsMin, item2->m_tsMax, item2->m_numRecs);
	fPage1 = MmsPageOper::computeFPage(currTs, item1->m_tsMin, item1->m_tsMax);
	fPage2 = MmsPageOper::computeFPage(currTs, item2->m_tsMin, item2->m_tsMax);

	return fPage1 > fPage2;
}

}
