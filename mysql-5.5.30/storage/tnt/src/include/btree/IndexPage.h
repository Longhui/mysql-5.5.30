/**
* NTSE B+树索引页面管理类
* 
* author: naturally (naturally@163.org)
*/

#ifndef _NTSE_INDEX_PAGE_H_
#define _NTSE_INDEX_PAGE_H_

#include "misc/Global.h"
#include "misc/Buffer.h"
#include "IndexLog.h"
#include "IndexKey.h"
#include "IndexCommon.h"

using namespace std;

namespace ntse {

class SubRecord;
class KeyComparator;
class TableDef;
class IndexLog;
class Session;

#define Page BufferPageHdr

static const uint INDEX_MARK = 0x78848373;					/** 索引文件唯一标识符 */

static const u64 INDEX_PAGE_SIZE = Limits::PAGE_SIZE;		/** 定义索引页面大小宏 */
static const u16 INDEX_MAX_NUM = Limits::MAX_INDEX_NUM;		/** 定义一个索引文件包含的最大索引个数 */

enum IndexInsertResult {
	INSERT_SUCCESS,			/** 插入成功 */
	NEED_SPLIT				/** 页面需要分裂，当前无法插入 */
};


struct KeyInfo {			/** 用来表示某个键值在索引页面当中的位置信息 */
public:
	KeyInfo() {	m_sOffset = m_eOffset = m_miniPageNo = m_keyCount = 0; m_result = 0; }
	KeyInfo(KeyInfo *keyInfo) { m_sOffset = keyInfo->m_sOffset; m_eOffset = keyInfo->m_eOffset; m_miniPageNo = keyInfo->m_miniPageNo; m_keyCount = keyInfo->m_keyCount; m_result = keyInfo->m_result; }

	u16	m_sOffset;			/** 当前键值起始偏移量 */	
	u16	m_eOffset;			/** 当前键值结束偏移量 */
	u16	m_miniPageNo;		/** 当前键值在第几个MiniPage */	
	u16 m_keyCount;			/** 键值在所在MiniPage中排第几项或者在整个页面当中排第几项，根据具体应用确定 */
	s32 m_result;			/** 导致查找键值的搜索键和查找到的键值的比较结果 */
};

class IndicePage : public Page {
public:
	void updateInfo(Session *session, IndexLog *logger, PageId pageId, u16 offset, u16 size, byte *newValue);
};

class IndexPage : public IndicePage {
private:
	/** 该类用来直接指向某个页面，不需要也不应该通过构造函数创建 */
	IndexPage() {}
	~IndexPage() {}

public:
	// 正常操作
	void initPage(Session *session, IndexLog *logger, bool clearData, bool logContent, PageId pageId, u32 pageMark, IndexPageType pageType, u8 pageLevel = 0, PageId prevPage = INVALID_PAGE_ID, PageId nextPage = INVALID_PAGE_ID);
	void clearData(u16 offset = sizeof(IndexPage), u16 size = Limits::PAGE_SIZE - sizeof(IndexPage));
	void setSMOBit(Session *session, PageId pageId, IndexLog *logger);
	void clearSMOBit(Session *session, PageId pageId, IndexLog *logger);
	
	bool findKeyInPage(const SubRecord *key, SearchFlag *flag, KeyComparator *comparator, SubRecord *foundKey, KeyInfo *keyInfo);
	bool findKeyInMiniPage(const SubRecord *key, KeyInfo *keyInfo, SearchFlag *flag, KeyComparator *comparator, SubRecord *foundKey);
	void findKeyInPageTwo(const SubRecord *key, KeyComparator *comparator, SubRecord **prev, SubRecord **next, KeyInfo *keyInfo);

	IndexInsertResult addIndexKey(Session *session, IndexLog *logger, PageId pageid, const SubRecord *insertKey, const SubRecord *cpInsertKey, SubRecord *idxKey1, SubRecord *idxKey2, KeyComparator *comparator, KeyInfo *keyInfo);
	IndexInsertResult appendIndexKey(Session *session, IndexLog *logger, PageId pageId, const SubRecord *appendKey, const SubRecord *lastKey);
	bool deleteIndexKey(Session *session, IndexLog *logger, PageId pageId, SubRecord *lastKey, const SubRecord *findKey, SubRecord *nextKey, KeyInfo keyInfo, bool siblingGot = false, u16 prevKeySOffset = 0, u16 nextKeyEOffset = INDEX_PAGE_SIZE);
	void splitPage(Session *session, IndexLog *logger, IndexPage *newPage, PageId pageId, PageId newPageId, u8 splitFactor, u16 insertPos, u16 reserveSize, SubRecord *splitKey);
	void mergePage(Session *session, IndexLog *logger, IndexPage *leftPage, PageId pageId, PageId leftPageId);
	//void updateSpecifiedPageId(Session *session, IndexLog *logger, PageId pageId, PageId newPageId, KeyInfo updateKeyInfo);
	bool prejudgeIsNeedMerged(const SubRecord *deleteKey, SubRecord *prevKey, SubRecord *nextKey, KeyInfo keyInfo, u16 *prevKeySOffset, u16 *nextKeyEOffset);

	void getExtremeKey(SubRecord *key, bool forward, KeyInfo *keyInfo);
	u16 getNextKey(const SubRecord *lastKey, u16 offset, bool spanMiniPage, u16 *miniPageNo, SubRecord *foundKey);
	u16 getPrevKey(u16 offset, bool spanMiniPage, u16 *miniPageNo, SubRecord *foundKey);
	void getFirstKey(SubRecord *key, KeyInfo *keyInfo);
	void getLastKey(SubRecord *key, KeyInfo *keyInfo);
	bool findSpecifiedPageId(PageId findPageId);
	bool isKeyInPageMiddle(KeyInfo *keyInfo);

	void updateNextPage(Session *session, IndexLog *logger, PageId pageId, PageId newNextPage);
	void updatePrevPage(Session *session, IndexLog *logger, PageId pageId, PageId newPrevPage);
	bool mergeTwoMiniPage(Session *session, IndexLog *logger, PageId pageId, u16 miniPageNo, SubRecord *prevKey, SubRecord *nextKey);

	uint calcUncompressedSpace(const TableDef *tableDef, const IndexDef *indeDef, SubRecord *key);
	void fetchKeyByOffset(u16 offset, SubRecord **key1, SubRecord **key2);
	u16 getNextKeyPageId(u16 offset, PageId *pageId);
	bool traversalAndVerify(const TableDef *tableDef, const IndexDef *indexDef, SubRecord *key1, SubRecord *key2, SubRecord *pkey);
	bool isDataClean();

	// 恢复相关
	void redoDMLUpdate(IDXLogType type, u16 offset, u16 miniPageNo, u16 oldSize, byte *newValue, u16 newSize);
	void redoSMOMergeOrig(PageId prevPageId, byte *moveData, u16 dataSize, byte *moveDir, u16 dirSize, u64 smoLSN);
	void redoSMOSplitOrig(PageId newPageId, u16 dataSize, u16 dirSize, u16 oldSKLen, u8 mpLeftCount, u8 mpMoveCount, u64 smoLSN);
	void redoSMOMergeRel();
	void redoSMOSplitRel(byte *moveData, u16 dataSize, byte *moveDir, u16 dirSize, byte *newSplitKey, u16 newSKLen, u8 mpLeftCount, u8 mpMoveCount, u64 smoLSN);
	void redoPageUpdate(u16 offset, byte *newValue, u16 valueLen, bool clearPageFirst);
	void redoPageAddMP(const byte *keyValue, u16 dataSize, u16 miniPageNo);
	void redoPageDeleteMP(u16 miniPageNo);
	void redoPageSplitMP(u16 offset, u16 oldSize, byte *newValue, u16 newSize, u16 leftItems, u16 miniPageNo);
	void redoPageMergeMP(u16 offset, u16 oldSize, byte *newValue, u16 newSize, u16 miniPageNo);

	void undoDMLUpdate(IDXLogType type, u16 offset, u16 miniPageNo, byte *oldValue, u16 oldSize, u16 newSize);
	void undoSMOMergeOrig(PageId leftPageId, u16 dataSize, u16 dirSize);
	void undoSMOSplitOrig(PageId nextPageId, byte *moveData, u16 dataSize, byte *moveDir, u16 dirSize, byte *oldSplitKey, u16 oldSKLen, u16 newSKLen, u8 mpLeftCount, u8 mpMoveCount);
	void undoSMOMergeRel(PageId prevPageId, byte *moveData, u16 dataSize, byte *moveDir, u16 dirSize);
	void undoSMOSplitRel();
	void undoPageUpdate(u16 offset, byte *oldValue, u16 valueLen);
	void undoPageAddMP(u16 dataSize, u16 miniPageNo);
	void undoPageDeleteMP(const byte *keyValue, u16 dataSize, u16 miniPageNo);
	void undoPageSplitMP(u16 offset, byte *oldValue, u16 oldSize, u16 newSize, u16 miniPageNo);
	void undoPageMergeMP(u16 offset, byte *oldValue, u16 oldSize, u16 newSize, u16 originalMPKeyCounts, u16 miniPageNo);

	static u32 formPageMark(u16 indexId, PageId pageId = INVALID_PAGE_ID);
	static u16 getIndexId(u32 mark);

private:
	void adjustMiniPageDataOffset(u16 startMiniPageNo, s32 deltaOffset);

	u16 findMiniPageInPage(const SubRecord *key, SearchFlag *flag, KeyComparator *comparator, SubRecord *record, s32 *result);

	void findKeyInMiniPageTwo(const SubRecord *key, KeyInfo *keyInfo, KeyComparator *comparator, SubRecord **prev, SubRecord **next);

	void addMiniPage(Session *session, IndexLog *logger, PageId pageId, const SubRecord *insertKey, u16 newMiniPageNo);
	void addMiniPageDir(u16 newMiniPageNo, u16 dataStart, u16 itemCounts);
	u16 addFirstKeyInMiniPage(const SubRecord *insertKey, u16 dataStart);
	void deleteMiniPage(Session *session, IndexLog *logger, PageId pageid, u16 miniPageNo);
	void deleteMiniPageIn(u16 miniPageNo, u16 miniPageStart, u16 miniPageEnd);
	void deleteMiniPageDir(u16 miniPageNo);

	IndexInsertResult splitMiniPageFull(Session *session, IndexLog *logger, PageId pageid, SubRecord *idxKey1, SubRecord *idxKey2, u16 miniPageNo);
	IndexInsertResult splitMiniPageReal(Session *session, IndexLog *logger, PageId pageId, const SubRecord *next, u16 sOffset, u16 eOffset, u16 miniPageNo, u16 splitItem);

	u16 getSplitOffset(u16 splitFactor, u16 insertPos, u16 reserveSize, SubRecord *splitKey, u16 *miniPageNo, u8 *mpLeftCount);
	u16 getMPsItemCounts(u16 miniPageNo);

	s32 compareKey(const SubRecord *key, const SubRecord *indexKey, SearchFlag *flag, KeyComparator *comparator) const;

public:
	/** 改变页面的SMOLSN为加一 */
	inline void setSMOLSN() {
		m_smoLSN++;
	}

	/** 判断某个索引页面是否是叶结点 */
	inline bool isPageLeaf() {
		return (m_pageType & LEAF_PAGE) || (m_pageType & ROOT_AND_LEAF);
	}

	/** 判断某个索引页面是否是根结点 */
	inline bool isPageRoot() {
		return (m_pageType & ROOT_PAGE) || (m_pageType & ROOT_AND_LEAF);
	}

	/** 判断某个索引页面是否正在SMO */
	inline bool isPageSMO() {
		return (m_pageType & BEING_SMO) != 0;
	}

	/** 判断一个页面是否为空 */
	inline bool isPageEmpty() {
		assert((m_pageCount == 0) == (m_miniPageNum == 0));
		return m_pageCount == 0;
	}

	/** 返回指定页面能否用来被合并 */
	inline bool canPageBeMerged(u16 mergeSize) {
		return INDEX_PAGE_SIZE - m_freeSpace + mergeSize <= INDEXPAGE_DATA_MAX_MERGE_SIZE || mergeSize == 0;
	}

	/** 返回指定页面是否需要进行合并 */
	inline bool isNeedMerged() {
		return ((INDEX_PAGE_SIZE - m_freeSpace <= INDEXPAGE_DATA_MIN_SIZE && m_nChance == 0) || isPageEmpty());
	}

	/** 判断某个页面是否满足初始插入条件 */
	inline bool isPageStillFree() {
		return m_freeSpace > INDEX_PAGE_SIZE - INDEXPAGE_INIT_MAX_USAGE;
	}

	/** 索引页面数据区域的结束偏移量 */
	inline u16 getDataEndOffset() {
		return m_dirStart - m_freeSpace;
	}

	/** 得到索引页面使用了的长度，包括项目录 */
	inline u16 getUsedSpace() {
		return INDEX_PAGE_SIZE - m_freeSpace - sizeof(IndexPage);
	};

private:
	/** 取得指定MiniPage目录 */
	inline byte* getMiniPageDirStart(u16 miniPageNo) {
		return (byte*)this + m_dirStart + miniPageNo * MINI_PAGE_DIR_SIZE;
	}

	/** 取得指定MiniPage数据的起始位置 */
	inline byte* getMiniPageDataStart(u16 miniPageNo) {
		return (byte*)this + *((u16*)getMiniPageDirStart(miniPageNo));
	}

	/** 设置指定MiniPage数据的起始位置 */
	inline void setMiniPageDataStart(u16 miniPageNo, u16 offset) {
		*((u16*)getMiniPageDirStart(miniPageNo)) = offset;
	}

	/** 修改指定MiniPage数据的起始位置 */
	inline void adjustMiniPageDataStart(u16 miniPageNo, s32 increment) {
		u16 *start = (u16*)getMiniPageDirStart(miniPageNo);
		*start = (u16)(*start + increment);
	}

	/** 取得指定MiniPage数据的起始偏移 */
	inline u16 getMiniPageDataOffset(u16 miniPageNo) {
		return *((u16*)getMiniPageDirStart(miniPageNo));
	}

	/** 取得指定MiniPage内表示键值个数的起始偏移 */
	inline byte* getMiniPageItemStart(u16 miniPageNo) {
		return (byte*)this + this->m_dirStart + (miniPageNo) * MINI_PAGE_DIR_SIZE + 2;
	}

	/** 取得指定MiniPage内索引键的个数 */
	inline byte getMiniPageItemCount(u16 miniPageNo) {
		return *getMiniPageItemStart(miniPageNo);
	}

	/** 增加指定MiniPage索引键个数1个 */
	inline void incMiniPageItemCount(u16 miniPageNo) {
		(*getMiniPageItemStart(miniPageNo))++;
	}

	/** 减少指定MiniPage索引键个数1个 */
	inline void decMiniPageItemCount(u16 miniPageNo) {
		(*getMiniPageItemStart(miniPageNo))--;
	}

	/** 设定指定MiniPage索引键个数 */
	inline void setMiniPageItemCount(u16 miniPageNo, u16 itemCount) {
		(*getMiniPageItemStart(miniPageNo)) = (u8)itemCount;
	}

	/** 判断页面是否还有所需的空间 */
	inline bool isPageEnough(u16 spaceNeeded) {
		return m_freeSpace >= spaceNeeded;
	}

	/** 更新本页插入偏移百分比数据，调用条件是在插入前更新! */
	inline void updateInsertOffsetRatio(u16 offset) {
		// 目前新插入项权重因子取1
		u16 newOffsetRatio = (u16)((offset - sizeof(IndexPage)) * 100.0 / (getDataEndOffset() - sizeof(IndexPage)) * 100);
		m_insertOffsetRatio = (u16)((m_insertOffsetRatio * m_pageCount + newOffsetRatio * 1) / (m_pageCount + 1));
	}

	/** 封装移动数据，使用memmove */
	inline void shiftDataByMOVE(void *dest, void *src, size_t size) {
		memmove(dest, src, size);
	}

	/** 封装移动数据，使用memcpy */
	inline void shiftDataByCPY(void *dest, void *src, size_t size) {
		memcpy(dest, src, size);
	}

	/** 封装移动项目录，使用memmove */
	inline void shiftDirByMOVE(void *dest, void *src, size_t size) {
		memmove(dest, src, size);
	}

	/** 封装移动项目录，使用memcpy */
	inline void shiftDirByCPY(void *dest, void *src, size_t size) {
		memcpy(dest, src, size);
	}

	/** 判断正常使用页面MiniPage是否需要分裂，MiniPage必定不为0 */
	inline bool isMiniPageFull(u16 miniPageNo) {
		return getMiniPageItemCount(miniPageNo) >= MINI_PAGE_MAX_ITEMS;
	}

	/** 判断创建索引初始页面MiniPage是否需要分裂，MiniPage可能为0 */
	inline bool isInitMiniPageFull(u16 miniPageNo) {
		return getMiniPageItemCount(miniPageNo) >= MINI_PAGE_MAX_INIT_ITEMS;
	}

	/** 根据和指定项比较的结果，判断当前是否需要取下一项 */
	inline bool isNeedFetchNext(s32 result, bool forward) {
		/************************************************************************/
		/* 对返回项位置调整做如下说明：
		当equalAllowable，要定位的应该是相等的项，此时result有三种返回：1/0/-1
		当forward，对于result=0/1，要继续取next；对于result=-1，当前项即可用
		当!forward，对于result=0/-1,要继续取prev，对于result=1，当前项即可用
		当!equalAllowable，则需完全根据forward以及includekey来判断
		注意，当!equalAllowable，result不可能是0
		forward && include (>=/=)	result=-1，当前项可用；对于result=1，需要取next
		forward && !include	(>)		result=-1，当前项可用；对于result=1，需要取next
		!forward && include	(<=)	result=1，当前项可用；对于result=-1，需要取prev
		!forward && !include (<)	result=1，当前项可用；对于result=-1，需要取prev
		*/

		/*	===>
		return (equalAllowable && forward && result >= 0 ||
		equalAllowable && !forward && result <= 0 ||
		!equalAllowable && forward && result > 0 ||
		!equalAllowable && !forward && result < 0);
		实际上includeKey已经在compare的时候起完作用了
		*/

		/*	===>
		return (result == 0 ||
		equalAllowable && ((result > 0) == forward) ||
		!equalAllowable && ((result > 0) == forward));
		*/
		/************************************************************************/

		return (result == 0 || (result > 0) == forward);
	}

public:
	PageId	m_prevPage;			/** 该页面的前驱页面 */
	PageId	m_nextPage;			/** 该页面的后继页面 */
	u64		m_smoLSN;			/** 保存SMO操作结束之后的LSN */
	u32		m_pageMark;			/** 特定索引页面标志，为了保证唯一性和有效性，只保存索引内部的ID编号 */
	u16		m_freeSpace;		/** 页面空闲空间 */
	u16		m_pageCount;		/** 页面总项数 */
	u16		m_dirStart;			/** 项目录起始位置 */
	u16		m_miniPageNum;		/** 页面包含MiniPage数目 */
	u16		m_insertOffsetRatio;/** 本页面插入数据的平均偏移量，按照0.00～100.00的范围乘100保存，数据从0～10000 */
	u16		m_splitedMPNo;		/** 表示最近的一次MiniPage分裂的页面号，可以不需要保证持久化，崩溃恢复过程也可以忽略 */
	u8		m_pageType;			/** 索引页面类型 */
	u8		m_pageLevel;		/** 索引页面层数 */
	u8		m_nChance;			/** 表示当前页面在多少次删除之后允许进行合并，为0表示随时可以合并 */

public:
	static const u16 MINI_PAGE_DIR_SIZE = 3;									/** MiniPage项目录大小——前两个字节表示MiniPage起始偏移，第三个字节表示该MiniPage有多少项 */
	static const u16 INDEXPAGE_DATA_MAX_MERGE_SIZE = INDEX_PAGE_SIZE / 4 * 3;	/** 允许页面合并之后的最大填充值，超过则不选择合并 */
	static const u16 INDEXPAGE_DATA_MIN_SIZE = INDEX_PAGE_SIZE / 3;				/** 索引页面最低填充率，如果低于该阀值，需要合并 */

	static const u8 MINI_PAGE_MAX_ITEMS = 15;								/** MiniPage页面最多索引键个数 */
	static const u8 MINI_PAGE_MAX_INIT_ITEMS = 12;							/** MiniPage页面最多初始索引键个数 */
	static const u16 INDEXPAGE_INIT_MAX_USAGE = INDEX_PAGE_SIZE / 16 * 15;	/** 索引页面起始填充率 */

	static const uint INDEXPAGE_MAX_SPLIT_FACTOR = 9500;					/** 索引页面最大分裂系数 */
	static const uint INDEXPAGE_MIN_SPLIT_FACTOR = 500;						/** 索引页面最小分裂系数 */
};


/** 索引文件头页管理类，所有调用都需要对页面加锁进行并发控制
 */
class IndexHeaderPage : public IndicePage {
public:
	IndexHeaderPage(u32 indexMark = INDEX_MARK);
	~IndexHeaderPage() {}

	void discardIndexChangeHeaderPage(u8 indexNo, u32 minFreeByteMap);
	void updatePageUsedStatus(Session *session, IndexLog *logger, u8 indexNo, u64 dataPages, u64 freePages);
	u8 getAvailableIndexId() const;
	u8 getAvailableIndexIdAndMark(Session *session, IndexLog *logger);

public:
	u32		m_indexMark;							/** NTSE索引特殊标记 */
	u32		m_minFreeBitMap;						/** 最小空闲位图所在Byte相对文件头的偏移量 */
	u32		m_curFreeBitMap;						/** 当前顺序使用最小空闲位图所在Byte相对文件头的偏移量 */
	u32		m_indexNum;								/** 索引文件包含多少个索引 */
	u64		m_dataOffset;							/** 索引数据区域相对索引文件头的偏移量 */
	PageId	m_rootPageIds[INDEX_MAX_NUM];			/** 各个索引根页面相对文件头的偏移量，不存在的索引置0 */
	PageId	m_freeListPageIds[INDEX_MAX_NUM];		/** 各个索引空闲页面链表头相对文件头的偏移量，不存在的置0 */
	u64		m_indexDataPages[INDEX_MAX_NUM];		/** 各个索引所占用总数据页数 */
	u64		m_indexFreePages[INDEX_MAX_NUM];		/** 各个索引数据页当中空闲页数 */
	u32		m_maxBMPageIds[INDEX_MAX_NUM];			/** 表示各个索引所用位图偏移最大值 */
	u8		m_indexIds[INDEX_MAX_NUM];				/** 各个索引对应的ID序列号，不存在的置0 */
	u8		m_indexUniqueId;						/** 表示的唯一标识ID序列号，该值表示目前分配的序列号 */
};

class IndexFileBitMapPage : public IndicePage {
};

static const u16 INDEXPAGE_DATA_START_OFFSET = sizeof(IndexPage);		/** 索引页面数据区域的起始偏移量 */

}

#endif
