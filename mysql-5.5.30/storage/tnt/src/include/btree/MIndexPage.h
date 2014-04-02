/**
* 内存索引页面相关
*
* @author 李伟钊(liweizhao@corp.netease.com)
*/
#ifndef _TNT_MINDEX_PAGE_H_
#define _TNT_MINDEX_PAGE_H_

#include "btree/IndexPage.h"
#include "btree/MIndexKey.h"
#include "misc/TNTIMPageManager.h"
#include "misc/DoubleChecker.h"
#include "trx/TNTTransaction.h"

using namespace ntse;

namespace tnt {

/** 内存索引键位置 */
class MkeyLocation {
public:
	MkeyLocation() 
		: m_sOffset((u16)-1), m_eOffset((u16)-1), m_keyNo((u16)-1) {
	}
	MkeyLocation(u16 keyNo, u16 sOffset, u16 eOffset) 
		: m_sOffset(sOffset), m_eOffset(eOffset), m_keyNo(keyNo) {
	} 
	MkeyLocation(const MkeyLocation& keyInfo) 
		: m_sOffset(keyInfo.m_sOffset), m_eOffset(keyInfo.m_eOffset), m_keyNo(keyInfo.m_keyNo) { 
	}

public:
	u16	m_sOffset;	/** 当前键值起始偏移量 */
	u16	m_eOffset;	/** 当前键值结束偏移量 */
	u16	m_keyNo;	/** 当前键值在页面中排第几项 */	
};

/** 内存索引查找定位结果 */
struct MIndexSearchResult {
	s32   m_cmpResult;         /** 查找键值和返回的键值的比较大小 */
	MkeyLocation m_keyLocation;/** 查找到的键值位置 */
};

/**
 * 内存索引页面
 */
#pragma pack(1)
class MIndexPage : public TNTIMPage {
public:
	static const u32 MINDEX_PAGE_SIZE;       /** 内存索引页面大小 */
	static const u16 MINDEX_DIR_SIZE;        /** 内存索引页面项目录大小 */
	static const u16 MINDEX_PAGE_HEADER_SIZE;/** 内存索引页面头部信息大小 */

public:
	void initPage(u32 pageMark, IndexPageType pageType, u8 pageLevel = 0, 
		MIndexPageHdl prevPage = MIDX_PAGE_NONE, MIndexPageHdl nextPage = MIDX_PAGE_NONE);
	s32 binarySearch(const SubRecord *searchkey, SubRecord *outKey, u16 *keyNoInPage, 
		const SearchFlag *flag, KeyComparator *comparator);
	MIndexPageHdl findChildPage(const SubRecord *searchkey, SubRecord *tmpKey, const SearchFlag* flag, 
		KeyComparator *comparator);
	void resetChildPage(const SubRecord *searchkey, SubRecord *tmpKey, KeyComparator *comparator, 
		MIndexPageHdl childpageHdl);
	bool findKeyInLeafPage(const SubRecord *searchkey, SubRecord *tmpKey, const SearchFlag *searchFlag, 
		KeyComparator *comparator, MIndexSearchResult *searchResult);
	bool locateInsertKeyInLeafPage(const SubRecord *searchkey, SubRecord *tmpKey, 
		KeyComparator *comparator, MkeyLocation *keyLocation);

	void split(MIndexPageHdl newPage);
	void merge(MIndexPageHdl pageMergeFrom);
	void redistribute(MIndexPageHdl rightPage);

	u64 bulkPhyReclaim(const ReadView *readView, SubRecord *assistKey, const DoubleChecker *doubleChecker);
	bool deleteIndexKey(const SubRecord *key, SubRecord *tmpKey, KeyComparator *comparator);
	IndexInsertResult addIndexKey(const SubRecord *key, SubRecord *tmpKey, KeyComparator *comparator, 
		MIndexPageHdl pageHdl = MIDX_PAGE_NONE, const MkeyLocation *keyLocation = NULL);
	IndexInsertResult appendIndexKey(const SubRecord *key, MIndexPageHdl pageHdl = MIDX_PAGE_NONE);
	void getHighKey(SubRecord *foundKey);
	bool getNextKey(const MkeyLocation *keyLoc, MkeyLocation *nextKeyLoc, SubRecord *foundKey);
	bool getPrevKey(const MkeyLocation *keyLoc, MkeyLocation *prevKeyLoc, SubRecord *foundKey);
	void verifyPage(const TableDef *tableDef, const IndexDef *idxDef, bool output = false);
	void setLeafPageHighKey();
	bool findPageAssosiateKey(MIndexPageHdl childPage, SubRecord *outKey) const;
	bool isMyChildPage(const MIndexPageHdl page, SubRecord *assistKey, KeyComparator *comparator);
	void printPage(const IndexDef *idxDef);
	/**
	 * 获得high-key总长度(包含扩展信息长度)
	 * @return 
	 */
	inline u16 getHighKeyLen() const {
		assert(isPageLeaf() || m_keyCount > 0);

		return isPageLeaf() ? (u16)(getKeyDataStart() - ((byte *)this + MINDEX_PAGE_HEADER_SIZE)) : 
			(u16)(getKeyDataEnd() - getKeyStartByNo(m_keyCount - 1));
	}

	/**
	 * 获得页面内索引项个数
	 * @return 页面内索引项个数
	 */
	inline u16 getKeyCount() const {
		return m_keyCount;
	}

	/**
	 * 获得指定索引项的数据长度
	 * @param no 索引项在页面中的序号
	 * @return 数据长度
	 */
	inline u16 getKeyLenByNo(u16 no) {
		assert(no < m_keyCount);
		return (u16)(((no == m_keyCount - 1) ? getKeyDataEnd() : getKeyStartByNo(no + 1)) - getKeyStartByNo(no));
	}

	inline void getKeyLocationByNo(u16 no, MkeyLocation *loc) const {
		assert(no < m_keyCount);
		loc->m_keyNo = no;
		loc->m_sOffset = getKeySOffsetByNo(no);
		loc->m_eOffset = getKeyEOffsetByNo(no);
	} 

	/**
	 * 根据序号获取索引项的键值(不拷贝)
	 * @param no 索引项在页面中的序号
	 * @param key OUT 输出键值信息，直接引用原数据
	 */
	inline void getKeyByNo (u16 no, SubRecord *key) const {
		assert(no < m_keyCount);
		byte *keyStart = getKeyStartByNo(no);
		byte *keyEnd = (no == m_keyCount - 1) ? getKeyDataEnd() : getKeyStartByNo(no + 1);
		key->m_data = keyStart;
		assert(keyEnd >= keyStart + MIndexKeyOper::getKeyExtLength(isPageLeaf()));
		key->m_size = keyEnd - keyStart - MIndexKeyOper::getKeyExtLength(isPageLeaf());
		key->m_rowId = MIndexKeyOper::readRowId(key);
	} 

	/**
	 * 根据位置获取索引项的键值(不拷贝)
	 * @param location 索引项在页面中的位置
	 * @param key OUT  输出键值信息，直接引用原数据
	 */
	inline void getKeyByLocation(const MkeyLocation *location, SubRecord *key) const {
		assert(location->m_keyNo < m_keyCount);
		assert(location->m_sOffset == getKeySOffsetByNo(location->m_keyNo));
		assert(location->m_eOffset == getKeyEOffsetByNo(location->m_keyNo));

		key->m_data = (byte*)this + location->m_sOffset;
		key->m_size = location->m_eOffset - location->m_sOffset - 
			MIndexKeyOper::getKeyExtLength(isPageLeaf());
		key->m_rowId = MIndexKeyOper::readRowId(key);
	}

	/**
	 * 根据扫描方向获得页面第一项或最后一项
	 * @param forward 是否是前向扫描
	 * @param key OUT 保存键值
	 * @param location INOUT 如果输入不为NULL，输出索引项的位置
	 * @return 
	 */
	inline void getExtremeKey(bool forward, SubRecord *key, MkeyLocation *location = NULL) {
		forward ? getFirstKey(key, location) : getLastKey(key, location);
	}

	/**
	 * 根据扫描方向获得页面第一项或最后一项(在叶子页面中反向扫描跳过Infinit Key)，用于反向扫描叶节点数据
	 * @param forward 是否是前向扫描
	 * @param key OUT 保存键值
	 * @param location INOUT 如果输入不为NULL，输出索引项的位置
	 * @return 
	 */
	inline void getExtremeKeyAtLeaf(bool forward, SubRecord *key, MkeyLocation *location = NULL) {
		forward ? getFirstKey(key, location) : getLastValidKey(key, location);
	}


	/**
	 * 获得页面第一项
	 * @param firstKey OUT
 	 * @param location INOUT 如果输入不为NULL，输出索引项的位置
	 * @return 
	 */
	inline void getFirstKey(SubRecord *firstKey, MkeyLocation *location = NULL) {
		assert(m_keyCount > 0);
		assert(KEY_NATURAL == firstKey->m_format);

		u16 sOffset = getKeySOffsetByNo(0);
		u16 eOffset = getKeyEOffsetByNo(0);
		firstKey->m_data = (byte*)this + sOffset;
		firstKey->m_size = eOffset - sOffset - MIndexKeyOper::getKeyExtLength(isPageLeaf());
		firstKey->m_rowId = MIndexKeyOper::readRowId(firstKey);

		if (NULL != location) {
			location->m_keyNo = 0;
			location->m_sOffset = sOffset;
			location->m_eOffset = eOffset;
		}
	}

	/**
	 * 获得页面最后一项
	 * @param lastKey OUT
 	 * @param location INOUT 如果输入不为NULL，输出索引项的位置
	 * @return 
	 */
	inline void getLastKey(SubRecord *lastKey, MkeyLocation *location = NULL) {
		assert(m_keyCount > 0);
		assert(KEY_NATURAL == lastKey->m_format);

		u16 lastKeyNo = m_keyCount - 1;
		u16 sOffset = getKeySOffsetByNo(lastKeyNo);
		u16 eOffset = getKeyEOffsetByNo(lastKeyNo);
		lastKey->m_data = (byte*)this + sOffset;
		lastKey->m_size = eOffset - sOffset - MIndexKeyOper::getKeyExtLength(isPageLeaf());
		lastKey->m_rowId = MIndexKeyOper::readRowId(lastKey);

		if (NULL != location) {
			location->m_keyNo = lastKeyNo;
			location->m_sOffset = sOffset;
			location->m_eOffset = eOffset;
		}
	}

	/**
	 * 获得页面最后一项valid项, 如果页中只有一项Infinit Key，那就返回Infinit Key
	 * @param lastKey OUT
 	 * @param location INOUT 如果输入不为NULL，输出索引项的位置
	 * @return 
	 */
	inline void getLastValidKey(SubRecord *lastKey, MkeyLocation *location = NULL) {
		assert(m_keyCount > 0);
		assert(KEY_NATURAL == lastKey->m_format);
		
		u16 lastKeyNo = m_keyCount - 1;
		u16 sOffset = getKeySOffsetByNo(lastKeyNo);
		u16 eOffset = getKeyEOffsetByNo(lastKeyNo);
		lastKey->m_data = (byte*)this + sOffset;
		lastKey->m_size = eOffset - sOffset - MIndexKeyOper::getKeyExtLength(isPageLeaf());
		lastKey->m_rowId = MIndexKeyOper::readRowId(lastKey);
		if(unlikely(MIndexKeyOper::isInfiniteKey(lastKey))) {
			//当叶子节点是最右节点，并且此叶节点中除了InifinitKey还有Valid项
			if(m_keyCount > 1) {
				lastKeyNo = m_keyCount - 2;
				sOffset = getKeySOffsetByNo(lastKeyNo);
				eOffset = getKeyEOffsetByNo(lastKeyNo);
				lastKey->m_data = (byte*)this + sOffset;
				lastKey->m_size = eOffset - sOffset - MIndexKeyOper::getKeyExtLength(isPageLeaf());
				lastKey->m_rowId = MIndexKeyOper::readRowId(lastKey);
			}
		}

		if (NULL != location) {
			location->m_keyNo = lastKeyNo;
			location->m_sOffset = sOffset;
			location->m_eOffset = eOffset;
		}
	}


	/**
	 * 设置左链接页面地址
	 * @param rightPage
	 * @return 
	 */
	inline void setPrevPage(MIndexPageHdl prevPage) {
		m_prevPage = prevPage;
	}

	/**
	 * 设置右链接页面地址
	 * @param rightPage
	 * @return 
	 */
	inline void setNextPage(MIndexPageHdl nextPage) {
		m_nextPage = nextPage;
	}

	/**
	 * 获得页面类型
	 * @return 
	 */
	inline IndexPageType getPageType() const {
		return (IndexPageType)m_pageType;
	}

	/**
	 * 设置页面类型
	 * @param pageType
	 */
	inline void setPageType(IndexPageType pageType) {
		m_pageType = (u8)pageType;
	}

	/**
	 * 获得页面层数
	 * @return 
	 */
	inline u8 getPageLevel() const {
		return m_pageLevel;
	}
	
	/**
	 * 增加页面层数
	 */
	inline void incrPageLevel() {
		m_pageLevel++;
	}

	/**
	 * 降低页面层数
	 */
	inline void decPageLevel() {
		m_pageLevel--;
	}
	
	/** 判断某个索引页面是否是叶结点 */
	inline bool isPageLeaf() const {
		return (m_pageType & LEAF_PAGE) || (m_pageType & ROOT_AND_LEAF);
	}
	/** 判断某个索引页面是否是根结点 */
	inline bool isPageRoot() {
		return (m_pageType & ROOT_PAGE) || (m_pageType & ROOT_AND_LEAF);
	}

	/** 判断一个页面是否为空 */
	inline bool isPageEmpty() {
		return 0 == m_keyCount;
	}

	/**
	 * 获得更新页面的最大事务ID号
	 * @return 
	 */
	inline u64 getMaxTrxId() const {
		assert(isPageLeaf());
		return m_maxTrxId;
	}

	/**
	 * 设置更新页面的最大事务ID号
	 * @param maxTrxId 
	 */
	inline void setMaxTrxId(u64 maxTrxId) {
		assert(isPageLeaf());
		if (maxTrxId > m_maxTrxId) {
			m_maxTrxId = maxTrxId;
		}
	}

	/**
	 * 获得页面更新时间戳
	 * @return 
	 */
	inline u64 getTimeStamp() {
		return m_timeStamp;
	}

	/**
	 * 递增页面更新时间戳
	 * @return 
	 */
	inline void incrTimeStamp() {
		++m_timeStamp;
	}

	/**
	 * 获得页面的键值数据总长度(叶页面不包含额外的high-key的长度)
	 * @return 
	 */
	inline u16 getKeyDataTotalLen() {
		return m_keyCount > 0 ? (getKeyEOffsetByNo(m_keyCount - 1) - getKeySOffsetByNo(0)) : 0;
	}

	/**
	 * 获得指定索引项的数据起始偏移量
	 * @param dirNo
	 * @return 
	 */
	inline u16 getKeySOffsetByNo(u16 dirNo) const {
		assert(dirNo < m_keyCount);
		return ((u16 *)((byte *)this + m_dirStart))[dirNo];
	}

	/**
	 * 获得指定索引项的数据结束偏移量
	 * @param dirNo
	 * @return 
	 */
	inline u16 getKeyEOffsetByNo(u16 dirNo) const {
		assert(dirNo < m_keyCount);
		return (dirNo == m_keyCount - 1) ? (m_dirStart - m_freeSpace) : getKeySOffsetByNo(dirNo + 1);
	}

	/**
	 * 获得页面中指定索引项的起始地址
	 * @param dirNo 要获取的索引项在页面中的序号
	 * @return 
	 */
	inline byte *getKeyStartByNo(u16 dirNo) const {
		assert(dirNo < m_keyCount);
		u16 offset = ((u16 *)((byte *)this + m_dirStart))[dirNo];
		return (byte *)this + offset;
	}

	/**
	 * 获得页面键值数据起始地址
	 * @return 
	 */
	inline byte *getKeyDataStart() const {
		return (0 == m_keyCount) ? getKeyDataEnd() : getKeyStartByNo(0);
	}

	/** 
	 * 索引页面数据区域的结束偏移量
	 */
	inline byte* getKeyDataEnd() const {
		return (byte *)this + m_dirStart - m_freeSpace;
	}

	
	/**
	 * 通过索引ID构造索引页面标识
	 * @param indexId
	 * @return 
	 */
	inline static u32 formPageMark(u16 indexId) {
		return u32(indexId);
	}

	/**
	 * 获得索引ID
	 * @param mark
	 * @return 
	 */
	inline static u16 getIndexId(u32 mark) {
		return (u16)mark;
	}

	/**
	 * 移动页面键值
	 * @param dst 目标页面
	 * @param src 源页面 
	 */
	inline static void movePageKeys(MIndexPageHdl dst, const MIndexPageHdl src) {
		memcpy((byte *)dst + MINDEX_PAGE_HEADER_SIZE, (byte *)src + MINDEX_PAGE_HEADER_SIZE, 
			MINDEX_PAGE_SIZE - MINDEX_PAGE_HEADER_SIZE);
		dst->m_keyCount = src->m_keyCount;
		dst->m_freeSpace = src->m_freeSpace;
		dst->m_dirStart = src->m_dirStart;

		memset((byte *)src + MINDEX_PAGE_HEADER_SIZE, 0, MINDEX_PAGE_SIZE - MINDEX_PAGE_HEADER_SIZE);
		src->m_keyCount = 0;
		src->m_freeSpace = (u16)MINDEX_PAGE_SIZE - MINDEX_PAGE_HEADER_SIZE;
		src->m_dirStart = (u16)MINDEX_PAGE_SIZE;
	}

	/** 封装移动数据，使用memmove */
	inline static void shiftDataByMOVE(void *dest, void *src, size_t size) {
		memmove(dest, src, size);
	}

	/**
	 * 计算存储完整键值信息以及项目录需要的空间大小，包括RowId，子页面句柄(非叶页面)等
	 * @param key        键值记录
	 * @param isLeafPage 是否是叶页面的键值
	 * @return 需要的存储空间大小
	 */
	inline static u16 calcSpaceNeeded(const SubRecord *key, bool isLeafPage) {
		assert(KEY_NATURAL == key->m_format);
		return MIndexKeyOper::calcKeyTotalLen((u16)key->m_size, isLeafPage) + MINDEX_DIR_SIZE;
	}

	/**
	 * 是否需要继续获取逻辑下一项
	 * 详细说明参考IndexPage::isNeedFetchNext
	 *
	 * @param result
	 * @param forward
	 * @return 
	 */
	inline static bool isNeedFetchNext(s32 result, const SearchFlag *flag) {
		return (result == 0) || ((result > 0) == flag->isForward());
	}

	/**
	 * 跟页面high-key比较大小
	 * @pre 必须已经持有页面latch
	 * @param key 查找键
	 * @param assistKey 用于比较的辅助键值 
	 * @param comparator 键值比较器
	 * @param flag 查找标志
	 * @return 小于返回-1，大于返回1，等于返回0
	 */
	inline int compareWithHighKey(const SubRecord *key, SubRecord *assistKey, 
		KeyComparator *comparator, const SearchFlag *flag) {
			if (likely(MIndexKeyOper::isKeyValid(key) || MIndexKeyOper::isInfiniteKey(key))) {
				getHighKey(assistKey);
				return MIndexKeyOper::compareKey(key, assistKey, flag, comparator);
			} else {
				return -1;
			}
	}

	/**
	 * 获得当前页面的前一页面
	 * @return 
	 */
	inline MIndexPageHdl getPrevPage() const {
		return m_prevPage;
	}


	/**
	 * 获得当前页面的下一页面
	 * @return 
	 */
	inline MIndexPageHdl getNextPage() const {
		return m_nextPage;
	}

	/**
	 * 获得页面空闲空间大小
	 * @return 
	 */
	inline u16 getFreeSpace() const {
		return m_freeSpace;
	}

	/**
	 * 获得页面已经使用的空间大小
	 * @return 
	 */
	inline u16 getDataSpaceUsed() const {
		return (u16)MINDEX_PAGE_SIZE - MINDEX_PAGE_HEADER_SIZE - m_freeSpace;
	}

	/**
     * 页面是否下溢(空闲空间超过页面的一半认为是下溢)
     * @return 
     */
	inline bool isUnderFlow() const {
		return m_freeSpace > (MIndexPage::MINDEX_PAGE_SIZE - MIndexPage::MINDEX_PAGE_HEADER_SIZE) / 2;
	}

	/**
	 * 判断是否能和另一页面合并
	 * @param page
	 * @return 
	 */
	inline bool canMergeWith(MIndexPageHdl page) const {
		uint dataSize = MIndexPage::MINDEX_PAGE_SIZE - MIndexPage::MINDEX_PAGE_HEADER_SIZE - 
			page->m_freeSpace;
		return m_freeSpace >= dataSize;
	}

	inline void markOverflow(bool overflow) {
		m_pageType = (overflow ? (m_pageType | OVERFLOW_PAGE) : (m_pageType & ~OVERFLOW_PAGE)); 
	}

	inline bool isOverflow() const {
		return (m_pageType & OVERFLOW_PAGE) != 0;
	}

private:
	MIndexPage() {}
	~MIndexPage() {}

	void addDir(u16 newDirNo, u16 dataOffset);
	void adjustPageDataOffset(u16 dirNo, s32 deltaOffset);
	void adjustPageDataOffset(u16 firstAjustDirNo, u16 lastAjustDirNo, s32 deltaOffset);
	u16 calcSplitPoint(u16 splitSize);

private:
	u8             m_pageType;   /* 页面类型 */
	u8             m_pageLevel;  /* 页面层数 */
	u16            m_freeSpace;  /* 空闲空间 */
	u16            m_keyCount;	 /* 页面项数 */
	u16            m_dirStart;	 /* 项目录起始位置 */
	MIndexPageHdl  m_prevPage;   /* 前驱页面 */
	MIndexPageHdl  m_nextPage;   /* 后继页面 */
	u32            m_pageMark;   /* 索引页面标识 */
	u64            m_maxTrxId;	 /* 页面最大事务ID号 */
	u64            m_timeStamp;	 /* 伪更新时间戳 */
};
#pragma pack()

}

#endif