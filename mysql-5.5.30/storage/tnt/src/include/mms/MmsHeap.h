/**
 * MMS页堆
 *
 * @author 邵峰(shaofeng@corp.netease.com, sf@163.org)
 */

#ifndef _NTSE_MMS_HEAP_H_
#define _NTSE_MMS_HEAP_H_

namespace ntse {

class MmsOldestHeap;
struct MmsRecPage;

/** 最老页堆项　*/
struct OldestHeapItem {
	MmsRecPage		*m_page;		/**	记录页地址 */
	u32				m_tsMin;		/** 页内最小时间戳 */
};

/** 最频繁页堆项　*/
struct FreqHeapItem {
	MmsOldestHeap	*m_oldestHeap;	/** 最老页堆地址 */
	MmsRecPage		*m_page;		/**	记录页地址 */
	u32				m_tsMin;		/** 页内最小时间戳 */
	u32				m_tsMax;		/** 页内最小时间戳 */
	u8				m_numRecs;		/** 页内有效记录项个数 */	
};

/** 最老缓存页堆 */
class MmsOldestHeap {
public:
	MmsOldestHeap(Mms *mms, MmsRPClass *rpClass, MmsFreqHeap *freqHeap);
	void moveDown(Session *session, u32 pos, bool pageLocked, MmsRecPage *lockedRecPage, bool topHeapChanged = false);
	void insert(MmsRecPage *recPage); 
	void del(Session *session, u32 pos, MmsRecPage *lockedRecPage);
	u64 getPagesOccupied();
	MmsRecPage* getPage(size_t idx);

	/** 
	* 获取记录页数组
	*
	* @return 记录页数组
	*/
	Array<OldestHeapItem>* getHeapArray() {
		return &m_heapArray;
	}

private:
	void swap(const int &pos1, const int &pos2);

	/**
	* 页堆中父页项位置
	*
	* @param pos 当前页项位置
	* @return 父页项位置
	*/
	inline u32 parent(u32 pos) {
		assert (pos);
		return (pos - 1) / 2;
	}

	/**
	* 页堆中LeftChild页项位置
	*
	* @param pos 当前页项位置
	* @return LeftChild页项位置
	*/
	inline u32 leftChild(u32 pos) {
		return pos * 2 + 1;
	}

	/**
	* 页堆中RightChild页项位置
	*
	* @param pos 当前页项位置
	* @return RightChild页项位置
	*/
	inline u32 rightChild(u32 pos) {
		return pos * 2 + 2;
	}

private:
	MmsRPClass				*m_rpClass;			/** 所属的页级别对象 */
	MmsFreqHeap				*m_freqHeap;		/** 所属的频繁页堆 */
	u32						m_freqIdx;			/** 堆顶页在频繁页堆中的索引号 */
	Array<OldestHeapItem>	m_heapArray;		/** 最老页堆数组，用动态数组实现 */
	u32						m_currSize;			/** 页堆数组当前大小 */

	friend class MmsFreqHeap;
};

/** 最低访问频率页堆 */
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
	* 页堆中父页项位置
	*
	* @param pos 当前页项位置
	* @return 父页项位置
	*/
	inline u32 parent(u32 pos) {
		assert(pos);
		return (pos - 1) / 2;
	}

	/**
	* 页堆中LeftChild页项位置
	*
	* @param pos 当前页项位置
	* @return LeftChild页项位置
	*/
	inline u32 leftChild(u32 pos) {
		return pos * 2 + 1;
	}

	/**
	* 页堆中RightChild页堆位置
	*
	* @param pos 当前页项位置
	* @return RightChild页项位置
	*/
	inline u32 rightChild(u32 pos) {
		return pos * 2 + 2;
	}

private:
	MmsTable			*m_mmsTable;		/** 所属的MMS表 */
	Array<FreqHeapItem>	m_heapArray;		/** 页堆数组，用动态数组实现 */
	u32					m_currSize;			/** 当前数组大小 */

	friend class MmsOldestHeap;
};

}

#endif
