/**
* NTSE B+������ҳ�������
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

static const uint INDEX_MARK = 0x78848373;					/** �����ļ�Ψһ��ʶ�� */

static const u64 INDEX_PAGE_SIZE = Limits::PAGE_SIZE;		/** ��������ҳ���С�� */
static const u16 INDEX_MAX_NUM = Limits::MAX_INDEX_NUM;		/** ����һ�������ļ������������������ */

enum IndexInsertResult {
	INSERT_SUCCESS,			/** ����ɹ� */
	NEED_SPLIT				/** ҳ����Ҫ���ѣ���ǰ�޷����� */
};


struct KeyInfo {			/** ������ʾĳ����ֵ������ҳ�浱�е�λ����Ϣ */
public:
	KeyInfo() {	m_sOffset = m_eOffset = m_miniPageNo = m_keyCount = 0; m_result = 0; }
	KeyInfo(KeyInfo *keyInfo) { m_sOffset = keyInfo->m_sOffset; m_eOffset = keyInfo->m_eOffset; m_miniPageNo = keyInfo->m_miniPageNo; m_keyCount = keyInfo->m_keyCount; m_result = keyInfo->m_result; }

	u16	m_sOffset;			/** ��ǰ��ֵ��ʼƫ���� */	
	u16	m_eOffset;			/** ��ǰ��ֵ����ƫ���� */
	u16	m_miniPageNo;		/** ��ǰ��ֵ�ڵڼ���MiniPage */	
	u16 m_keyCount;			/** ��ֵ������MiniPage���ŵڼ������������ҳ�浱���ŵڼ�����ݾ���Ӧ��ȷ�� */
	s32 m_result;			/** ���²��Ҽ�ֵ���������Ͳ��ҵ��ļ�ֵ�ıȽϽ�� */
};

class IndicePage : public Page {
public:
	void updateInfo(Session *session, IndexLog *logger, PageId pageId, u16 offset, u16 size, byte *newValue);
};

class IndexPage : public IndicePage {
private:
	/** ��������ֱ��ָ��ĳ��ҳ�棬����ҪҲ��Ӧ��ͨ�����캯������ */
	IndexPage() {}
	~IndexPage() {}

public:
	// ��������
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

	// �ָ����
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
	/** �ı�ҳ���SMOLSNΪ��һ */
	inline void setSMOLSN() {
		m_smoLSN++;
	}

	/** �ж�ĳ������ҳ���Ƿ���Ҷ��� */
	inline bool isPageLeaf() {
		return (m_pageType & LEAF_PAGE) || (m_pageType & ROOT_AND_LEAF);
	}

	/** �ж�ĳ������ҳ���Ƿ��Ǹ���� */
	inline bool isPageRoot() {
		return (m_pageType & ROOT_PAGE) || (m_pageType & ROOT_AND_LEAF);
	}

	/** �ж�ĳ������ҳ���Ƿ�����SMO */
	inline bool isPageSMO() {
		return (m_pageType & BEING_SMO) != 0;
	}

	/** �ж�һ��ҳ���Ƿ�Ϊ�� */
	inline bool isPageEmpty() {
		assert((m_pageCount == 0) == (m_miniPageNum == 0));
		return m_pageCount == 0;
	}

	/** ����ָ��ҳ���ܷ��������ϲ� */
	inline bool canPageBeMerged(u16 mergeSize) {
		return INDEX_PAGE_SIZE - m_freeSpace + mergeSize <= INDEXPAGE_DATA_MAX_MERGE_SIZE || mergeSize == 0;
	}

	/** ����ָ��ҳ���Ƿ���Ҫ���кϲ� */
	inline bool isNeedMerged() {
		return ((INDEX_PAGE_SIZE - m_freeSpace <= INDEXPAGE_DATA_MIN_SIZE && m_nChance == 0) || isPageEmpty());
	}

	/** �ж�ĳ��ҳ���Ƿ������ʼ�������� */
	inline bool isPageStillFree() {
		return m_freeSpace > INDEX_PAGE_SIZE - INDEXPAGE_INIT_MAX_USAGE;
	}

	/** ����ҳ����������Ľ���ƫ���� */
	inline u16 getDataEndOffset() {
		return m_dirStart - m_freeSpace;
	}

	/** �õ�����ҳ��ʹ���˵ĳ��ȣ�������Ŀ¼ */
	inline u16 getUsedSpace() {
		return INDEX_PAGE_SIZE - m_freeSpace - sizeof(IndexPage);
	};

private:
	/** ȡ��ָ��MiniPageĿ¼ */
	inline byte* getMiniPageDirStart(u16 miniPageNo) {
		return (byte*)this + m_dirStart + miniPageNo * MINI_PAGE_DIR_SIZE;
	}

	/** ȡ��ָ��MiniPage���ݵ���ʼλ�� */
	inline byte* getMiniPageDataStart(u16 miniPageNo) {
		return (byte*)this + *((u16*)getMiniPageDirStart(miniPageNo));
	}

	/** ����ָ��MiniPage���ݵ���ʼλ�� */
	inline void setMiniPageDataStart(u16 miniPageNo, u16 offset) {
		*((u16*)getMiniPageDirStart(miniPageNo)) = offset;
	}

	/** �޸�ָ��MiniPage���ݵ���ʼλ�� */
	inline void adjustMiniPageDataStart(u16 miniPageNo, s32 increment) {
		u16 *start = (u16*)getMiniPageDirStart(miniPageNo);
		*start = (u16)(*start + increment);
	}

	/** ȡ��ָ��MiniPage���ݵ���ʼƫ�� */
	inline u16 getMiniPageDataOffset(u16 miniPageNo) {
		return *((u16*)getMiniPageDirStart(miniPageNo));
	}

	/** ȡ��ָ��MiniPage�ڱ�ʾ��ֵ��������ʼƫ�� */
	inline byte* getMiniPageItemStart(u16 miniPageNo) {
		return (byte*)this + this->m_dirStart + (miniPageNo) * MINI_PAGE_DIR_SIZE + 2;
	}

	/** ȡ��ָ��MiniPage���������ĸ��� */
	inline byte getMiniPageItemCount(u16 miniPageNo) {
		return *getMiniPageItemStart(miniPageNo);
	}

	/** ����ָ��MiniPage����������1�� */
	inline void incMiniPageItemCount(u16 miniPageNo) {
		(*getMiniPageItemStart(miniPageNo))++;
	}

	/** ����ָ��MiniPage����������1�� */
	inline void decMiniPageItemCount(u16 miniPageNo) {
		(*getMiniPageItemStart(miniPageNo))--;
	}

	/** �趨ָ��MiniPage���������� */
	inline void setMiniPageItemCount(u16 miniPageNo, u16 itemCount) {
		(*getMiniPageItemStart(miniPageNo)) = (u8)itemCount;
	}

	/** �ж�ҳ���Ƿ�������Ŀռ� */
	inline bool isPageEnough(u16 spaceNeeded) {
		return m_freeSpace >= spaceNeeded;
	}

	/** ���±�ҳ����ƫ�ưٷֱ����ݣ������������ڲ���ǰ����! */
	inline void updateInsertOffsetRatio(u16 offset) {
		// Ŀǰ�²�����Ȩ������ȡ1
		u16 newOffsetRatio = (u16)((offset - sizeof(IndexPage)) * 100.0 / (getDataEndOffset() - sizeof(IndexPage)) * 100);
		m_insertOffsetRatio = (u16)((m_insertOffsetRatio * m_pageCount + newOffsetRatio * 1) / (m_pageCount + 1));
	}

	/** ��װ�ƶ����ݣ�ʹ��memmove */
	inline void shiftDataByMOVE(void *dest, void *src, size_t size) {
		memmove(dest, src, size);
	}

	/** ��װ�ƶ����ݣ�ʹ��memcpy */
	inline void shiftDataByCPY(void *dest, void *src, size_t size) {
		memcpy(dest, src, size);
	}

	/** ��װ�ƶ���Ŀ¼��ʹ��memmove */
	inline void shiftDirByMOVE(void *dest, void *src, size_t size) {
		memmove(dest, src, size);
	}

	/** ��װ�ƶ���Ŀ¼��ʹ��memcpy */
	inline void shiftDirByCPY(void *dest, void *src, size_t size) {
		memcpy(dest, src, size);
	}

	/** �ж�����ʹ��ҳ��MiniPage�Ƿ���Ҫ���ѣ�MiniPage�ض���Ϊ0 */
	inline bool isMiniPageFull(u16 miniPageNo) {
		return getMiniPageItemCount(miniPageNo) >= MINI_PAGE_MAX_ITEMS;
	}

	/** �жϴ���������ʼҳ��MiniPage�Ƿ���Ҫ���ѣ�MiniPage����Ϊ0 */
	inline bool isInitMiniPageFull(u16 miniPageNo) {
		return getMiniPageItemCount(miniPageNo) >= MINI_PAGE_MAX_INIT_ITEMS;
	}

	/** ���ݺ�ָ����ȽϵĽ�����жϵ�ǰ�Ƿ���Ҫȡ��һ�� */
	inline bool isNeedFetchNext(s32 result, bool forward) {
		/************************************************************************/
		/* �Է�����λ�õ���������˵����
		��equalAllowable��Ҫ��λ��Ӧ������ȵ����ʱresult�����ַ��أ�1/0/-1
		��forward������result=0/1��Ҫ����ȡnext������result=-1����ǰ�����
		��!forward������result=0/-1,Ҫ����ȡprev������result=1����ǰ�����
		��!equalAllowable��������ȫ����forward�Լ�includekey���ж�
		ע�⣬��!equalAllowable��result��������0
		forward && include (>=/=)	result=-1����ǰ����ã�����result=1����Ҫȡnext
		forward && !include	(>)		result=-1����ǰ����ã�����result=1����Ҫȡnext
		!forward && include	(<=)	result=1����ǰ����ã�����result=-1����Ҫȡprev
		!forward && !include (<)	result=1����ǰ����ã�����result=-1����Ҫȡprev
		*/

		/*	===>
		return (equalAllowable && forward && result >= 0 ||
		equalAllowable && !forward && result <= 0 ||
		!equalAllowable && forward && result > 0 ||
		!equalAllowable && !forward && result < 0);
		ʵ����includeKey�Ѿ���compare��ʱ������������
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
	PageId	m_prevPage;			/** ��ҳ���ǰ��ҳ�� */
	PageId	m_nextPage;			/** ��ҳ��ĺ��ҳ�� */
	u64		m_smoLSN;			/** ����SMO��������֮���LSN */
	u32		m_pageMark;			/** �ض�����ҳ���־��Ϊ�˱�֤Ψһ�Ժ���Ч�ԣ�ֻ���������ڲ���ID��� */
	u16		m_freeSpace;		/** ҳ����пռ� */
	u16		m_pageCount;		/** ҳ�������� */
	u16		m_dirStart;			/** ��Ŀ¼��ʼλ�� */
	u16		m_miniPageNum;		/** ҳ�����MiniPage��Ŀ */
	u16		m_insertOffsetRatio;/** ��ҳ��������ݵ�ƽ��ƫ����������0.00��100.00�ķ�Χ��100���棬���ݴ�0��10000 */
	u16		m_splitedMPNo;		/** ��ʾ�����һ��MiniPage���ѵ�ҳ��ţ����Բ���Ҫ��֤�־û��������ָ�����Ҳ���Ժ��� */
	u8		m_pageType;			/** ����ҳ������ */
	u8		m_pageLevel;		/** ����ҳ����� */
	u8		m_nChance;			/** ��ʾ��ǰҳ���ڶ��ٴ�ɾ��֮��������кϲ���Ϊ0��ʾ��ʱ���Ժϲ� */

public:
	static const u16 MINI_PAGE_DIR_SIZE = 3;									/** MiniPage��Ŀ¼��С����ǰ�����ֽڱ�ʾMiniPage��ʼƫ�ƣ��������ֽڱ�ʾ��MiniPage�ж����� */
	static const u16 INDEXPAGE_DATA_MAX_MERGE_SIZE = INDEX_PAGE_SIZE / 4 * 3;	/** ����ҳ��ϲ�֮���������ֵ��������ѡ��ϲ� */
	static const u16 INDEXPAGE_DATA_MIN_SIZE = INDEX_PAGE_SIZE / 3;				/** ����ҳ���������ʣ�������ڸ÷�ֵ����Ҫ�ϲ� */

	static const u8 MINI_PAGE_MAX_ITEMS = 15;								/** MiniPageҳ��������������� */
	static const u8 MINI_PAGE_MAX_INIT_ITEMS = 12;							/** MiniPageҳ������ʼ���������� */
	static const u16 INDEXPAGE_INIT_MAX_USAGE = INDEX_PAGE_SIZE / 16 * 15;	/** ����ҳ����ʼ����� */

	static const uint INDEXPAGE_MAX_SPLIT_FACTOR = 9500;					/** ����ҳ��������ϵ�� */
	static const uint INDEXPAGE_MIN_SPLIT_FACTOR = 500;						/** ����ҳ����С����ϵ�� */
};


/** �����ļ�ͷҳ�����࣬���е��ö���Ҫ��ҳ��������в�������
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
	u32		m_indexMark;							/** NTSE���������� */
	u32		m_minFreeBitMap;						/** ��С����λͼ����Byte����ļ�ͷ��ƫ���� */
	u32		m_curFreeBitMap;						/** ��ǰ˳��ʹ����С����λͼ����Byte����ļ�ͷ��ƫ���� */
	u32		m_indexNum;								/** �����ļ��������ٸ����� */
	u64		m_dataOffset;							/** ��������������������ļ�ͷ��ƫ���� */
	PageId	m_rootPageIds[INDEX_MAX_NUM];			/** ����������ҳ������ļ�ͷ��ƫ�����������ڵ�������0 */
	PageId	m_freeListPageIds[INDEX_MAX_NUM];		/** ������������ҳ������ͷ����ļ�ͷ��ƫ�����������ڵ���0 */
	u64		m_indexDataPages[INDEX_MAX_NUM];		/** ����������ռ��������ҳ�� */
	u64		m_indexFreePages[INDEX_MAX_NUM];		/** ������������ҳ���п���ҳ�� */
	u32		m_maxBMPageIds[INDEX_MAX_NUM];			/** ��ʾ������������λͼƫ�����ֵ */
	u8		m_indexIds[INDEX_MAX_NUM];				/** ����������Ӧ��ID���кţ������ڵ���0 */
	u8		m_indexUniqueId;						/** ��ʾ��Ψһ��ʶID���кţ���ֵ��ʾĿǰ��������к� */
};

class IndexFileBitMapPage : public IndicePage {
};

static const u16 INDEXPAGE_DATA_START_OFFSET = sizeof(IndexPage);		/** ����ҳ�������������ʼƫ���� */

}

#endif
