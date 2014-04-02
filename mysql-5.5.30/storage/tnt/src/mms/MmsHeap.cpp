/**
 * MMS页堆管理
 *
 * @author 邵峰(shaofeng@corp.netease.com, sf@163.org)
 */

#include "mms/Mms.h"
#include "mms/MmsHeap.h"
#include "mms/MmsPage.h"

namespace ntse {

/************************************************************************/
/*					    最老缓存页堆                                    */
/************************************************************************/

/** 
 * 构造函数
 *
 * @param mms MMS全局对象
 * @param rpClass 所属的页类对象 
 * @param freqHeap 最低频率页堆
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
 * 页堆中页项下移（包括最老缓存页堆和最小频繁页堆）
 *
 * @param session 会话
 * @param pos 页项当前位置
 * @param pageLocked 调整页是否加锁
 * @param lockedRecPage 已加锁的页
 * @param topHeapChanged 顶堆是否更改
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

	// 获取页内最小时间戳,注意：不加锁情况不需要调整最小时间戳
	if (pageLocked)
		MmsPageOper::getRecPageTs(recPage, &heapItem->m_tsMin, NULL, NULL);

	// 下移
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

	// 调整频繁页堆
	if (topHeapChanged || freqHeapChangeNeeded)
		m_freqHeap->moveDown(session, m_freqIdx, topPageLocked, lockedRecPage);
}

/**
 * 向页堆插入一个记录页。内部约定：空缓存页不能调用该函数
 * @pre 记录页已加锁
 *
 * @param recPage 待插入的记录页
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
 * 在页堆删除一个页项
 *
 * @apram session 会话
 * @param pos 页项当前位置
 * @param lockedRecPage 已加锁的页
 */
void MmsOldestHeap::del(Session *session, u32 pos, MmsRecPage *lockedRecPage) {
	m_rpClass->m_status.m_pageDeletes++;

	m_heapArray[pos].m_page->m_oldestHeapIdx = (u32) -1;
	if (pos == m_currSize - 1) {
		m_heapArray.pop();
		if (--m_currSize == 0) {
			// 从最小频繁页堆删除
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
 * 页堆中两节点互换
 *
 * @param pos1 节点位置1
 * @param pos2 节点位置2
 */
void MmsOldestHeap::swap(const int &pos1, const int &pos2) {
	OldestHeapItem tmp = m_heapArray[pos1];
	m_heapArray[pos1] = m_heapArray[pos2];
	m_heapArray[pos2] = tmp;
	// 注意：这里不保证是否加页锁，因为m_oldestHeapIdx只能同时有一个线程使用，不必由页锁保护
	// 内部约定：为防止以下操作不出错，在页清空内容并替换/删除之前，首先必须删除页堆中相关的页项
	m_heapArray[pos1].m_page->m_oldestHeapIdx = pos1;
	m_heapArray[pos2].m_page->m_oldestHeapIdx = pos2;
}

/** 
 * 获取记录页
 *
 * @param idx 索引号
 * @return 记录页
 */
MmsRecPage* MmsOldestHeap::getPage(size_t idx) {
	if (!m_currSize) return NULL;

	return m_heapArray[idx].m_page;
}

/** 
 * 得到已经占用的更新页个数
 *
 * @return 已经占用的更新页个数
 */
u64 MmsOldestHeap::getPagesOccupied() {
	assert (m_currSize == m_heapArray.getSize());
	return m_heapArray.getSize();
}

/************************************************************************/
/*					    最低访问频率缓存页堆                            */
/************************************************************************/

/** 
 * 构造函数
 * 
 * @param mms MMS全局对象
 * @param mmsTable MMS表对象
 */
MmsFreqHeap::MmsFreqHeap(Mms *mms, MmsTable *mmsTable) { 
	UNREFERENCED_PARAMETER(mms);
	m_mmsTable = mmsTable;
	m_currSize = 0;
	assert(m_heapArray.getSize() == m_currSize);
}

/**
 * 页堆中页项下移(包括更改当前项内容)
 *
 * @param session 会话
 * @param pos 页项当前位置
 * @param pageLocked 页项相关的记录页是否已加页锁
 * @param lockedRecPage 已加锁的页
 */
void MmsFreqHeap::moveDown(Session *session, u32 pos, bool pageLocked, MmsRecPage *lockedRecPage) {
	FreqHeapItem *freqItem = &m_heapArray[pos];
	MmsRecPage *recPage = freqItem->m_oldestHeap->m_heapArray[0].m_page;

	// 重构页项
	if (!pageLocked && (lockedRecPage != recPage)) {
		MMS_LOCK_REC_PAGE(session, m_mmsTable->m_mms, recPage);
		freqItem->m_page = recPage;
		MmsPageOper::getRecPageTs(freqItem->m_page, &freqItem->m_tsMin, &freqItem->m_tsMax, &freqItem->m_numRecs);
		MmsPageOper::unlockRecPage(session, m_mmsTable->m_mms, recPage);
	} else {
		freqItem->m_page = recPage;
		MmsPageOper::getRecPageTs(freqItem->m_page, &freqItem->m_tsMin, &freqItem->m_tsMax, &freqItem->m_numRecs);
	}

	// 下移
	doMoveDown(pos);
}

/**
 * 页项下移
 *
 * @param pos 页项当前位置
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
 * 向页堆插入一个记录页
 *
 * @param oldestItem 待插入的页项
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
 * 在页堆删除一个页项
 *
 * @param pos 页项当前位置
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
		// 注意：这里不能调用moveDown函数，否则会引起两个不同级别的页同时加锁情况！
		doMoveDown(pos);
	}
	assert(m_heapArray.getSize() == m_currSize);
}

/** 
 * 获取记录页
 *
 * @param idx 索引项
 * @return 记录页
 */
MmsRecPage* MmsFreqHeap::getPage(int idx) {
	if (!m_currSize) return NULL;

	return m_heapArray[idx].m_page;
}

/**
 * 页堆中两节点互换
 *
 * @param pos1 节点位置1
 * @param pos2 节点位置2
 */
void MmsFreqHeap::swap(const u32 &pos1, const u32 &pos2) {
	FreqHeapItem tmp = m_heapArray[pos1];
	m_heapArray[pos1] = m_heapArray[pos2];
	m_heapArray[pos2] = tmp;
	
	m_heapArray[pos1].m_oldestHeap->m_freqIdx = pos1;
	m_heapArray[pos2].m_oldestHeap->m_freqIdx = pos2;
}

/**
 * 比较两个页项,　须采用FPage方法
 *
 * @param pos1 位置1
 * @param pos2 位置2
 * @return 如果pos1 > pos2，返回1; 否则返回0
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
