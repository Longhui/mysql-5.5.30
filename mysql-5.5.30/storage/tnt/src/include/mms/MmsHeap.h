/**
 * MMSҳ��
 *
 * @author �۷�(shaofeng@corp.netease.com, sf@163.org)
 */

#ifndef _NTSE_MMS_HEAP_H_
#define _NTSE_MMS_HEAP_H_

namespace ntse {

class MmsOldestHeap;
struct MmsRecPage;

/** ����ҳ���*/
struct OldestHeapItem {
	MmsRecPage		*m_page;		/**	��¼ҳ��ַ */
	u32				m_tsMin;		/** ҳ����Сʱ��� */
};

/** ��Ƶ��ҳ���*/
struct FreqHeapItem {
	MmsOldestHeap	*m_oldestHeap;	/** ����ҳ�ѵ�ַ */
	MmsRecPage		*m_page;		/**	��¼ҳ��ַ */
	u32				m_tsMin;		/** ҳ����Сʱ��� */
	u32				m_tsMax;		/** ҳ����Сʱ��� */
	u8				m_numRecs;		/** ҳ����Ч��¼����� */	
};

/** ���ϻ���ҳ�� */
class MmsOldestHeap {
public:
	MmsOldestHeap(Mms *mms, MmsRPClass *rpClass, MmsFreqHeap *freqHeap);
	void moveDown(Session *session, u32 pos, bool pageLocked, MmsRecPage *lockedRecPage, bool topHeapChanged = false);
	void insert(MmsRecPage *recPage); 
	void del(Session *session, u32 pos, MmsRecPage *lockedRecPage);
	u64 getPagesOccupied();
	MmsRecPage* getPage(size_t idx);

	/** 
	* ��ȡ��¼ҳ����
	*
	* @return ��¼ҳ����
	*/
	Array<OldestHeapItem>* getHeapArray() {
		return &m_heapArray;
	}

private:
	void swap(const int &pos1, const int &pos2);

	/**
	* ҳ���и�ҳ��λ��
	*
	* @param pos ��ǰҳ��λ��
	* @return ��ҳ��λ��
	*/
	inline u32 parent(u32 pos) {
		assert (pos);
		return (pos - 1) / 2;
	}

	/**
	* ҳ����LeftChildҳ��λ��
	*
	* @param pos ��ǰҳ��λ��
	* @return LeftChildҳ��λ��
	*/
	inline u32 leftChild(u32 pos) {
		return pos * 2 + 1;
	}

	/**
	* ҳ����RightChildҳ��λ��
	*
	* @param pos ��ǰҳ��λ��
	* @return RightChildҳ��λ��
	*/
	inline u32 rightChild(u32 pos) {
		return pos * 2 + 2;
	}

private:
	MmsRPClass				*m_rpClass;			/** ������ҳ������� */
	MmsFreqHeap				*m_freqHeap;		/** ������Ƶ��ҳ�� */
	u32						m_freqIdx;			/** �Ѷ�ҳ��Ƶ��ҳ���е������� */
	Array<OldestHeapItem>	m_heapArray;		/** ����ҳ�����飬�ö�̬����ʵ�� */
	u32						m_currSize;			/** ҳ�����鵱ǰ��С */

	friend class MmsFreqHeap;
};

/** ��ͷ���Ƶ��ҳ�� */
class MmsFreqHeap {
public:
	MmsFreqHeap(Mms *mms, MmsTable *mmsTable);
	MmsRecPage* getPage(int idx);
	void insert(MmsOldestHeap *oldestHeap);
	void del(u32 pos);

private:
	void swap(const u32 &pos1, const u32 &pos2);
	void doMoveDown(u32 pos);
	void moveDown(Session *session, u32 pos, bool pageLocked, MmsRecPage *lockedRecPage);
	bool itemCmp(u32 pos1, u32 pos2);

	/**
	* ҳ���и�ҳ��λ��
	*
	* @param pos ��ǰҳ��λ��
	* @return ��ҳ��λ��
	*/
	inline u32 parent(u32 pos) {
		assert(pos);
		return (pos - 1) / 2;
	}

	/**
	* ҳ����LeftChildҳ��λ��
	*
	* @param pos ��ǰҳ��λ��
	* @return LeftChildҳ��λ��
	*/
	inline u32 leftChild(u32 pos) {
		return pos * 2 + 1;
	}

	/**
	* ҳ����RightChildҳ��λ��
	*
	* @param pos ��ǰҳ��λ��
	* @return RightChildҳ��λ��
	*/
	inline u32 rightChild(u32 pos) {
		return pos * 2 + 2;
	}

private:
	MmsTable			*m_mmsTable;		/** ������MMS�� */
	Array<FreqHeapItem>	m_heapArray;		/** ҳ�����飬�ö�̬����ʵ�� */
	u32					m_currSize;			/** ��ǰ�����С */

	friend class MmsOldestHeap;
};

}

#endif
