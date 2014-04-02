/**
* �ڴ�����ҳ�����
*
* @author ��ΰ��(liweizhao@corp.netease.com)
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

/** �ڴ�������λ�� */
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
	u16	m_sOffset;	/** ��ǰ��ֵ��ʼƫ���� */
	u16	m_eOffset;	/** ��ǰ��ֵ����ƫ���� */
	u16	m_keyNo;	/** ��ǰ��ֵ��ҳ�����ŵڼ��� */	
};

/** �ڴ��������Ҷ�λ��� */
struct MIndexSearchResult {
	s32   m_cmpResult;         /** ���Ҽ�ֵ�ͷ��صļ�ֵ�ıȽϴ�С */
	MkeyLocation m_keyLocation;/** ���ҵ��ļ�ֵλ�� */
};

/**
 * �ڴ�����ҳ��
 */
#pragma pack(1)
class MIndexPage : public TNTIMPage {
public:
	static const u32 MINDEX_PAGE_SIZE;       /** �ڴ�����ҳ���С */
	static const u16 MINDEX_DIR_SIZE;        /** �ڴ�����ҳ����Ŀ¼��С */
	static const u16 MINDEX_PAGE_HEADER_SIZE;/** �ڴ�����ҳ��ͷ����Ϣ��С */

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
	 * ���high-key�ܳ���(������չ��Ϣ����)
	 * @return 
	 */
	inline u16 getHighKeyLen() const {
		assert(isPageLeaf() || m_keyCount > 0);

		return isPageLeaf() ? (u16)(getKeyDataStart() - ((byte *)this + MINDEX_PAGE_HEADER_SIZE)) : 
			(u16)(getKeyDataEnd() - getKeyStartByNo(m_keyCount - 1));
	}

	/**
	 * ���ҳ�������������
	 * @return ҳ�������������
	 */
	inline u16 getKeyCount() const {
		return m_keyCount;
	}

	/**
	 * ���ָ������������ݳ���
	 * @param no ��������ҳ���е����
	 * @return ���ݳ���
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
	 * ������Ż�ȡ������ļ�ֵ(������)
	 * @param no ��������ҳ���е����
	 * @param key OUT �����ֵ��Ϣ��ֱ������ԭ����
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
	 * ����λ�û�ȡ������ļ�ֵ(������)
	 * @param location ��������ҳ���е�λ��
	 * @param key OUT  �����ֵ��Ϣ��ֱ������ԭ����
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
	 * ����ɨ�跽����ҳ���һ������һ��
	 * @param forward �Ƿ���ǰ��ɨ��
	 * @param key OUT �����ֵ
	 * @param location INOUT ������벻ΪNULL������������λ��
	 * @return 
	 */
	inline void getExtremeKey(bool forward, SubRecord *key, MkeyLocation *location = NULL) {
		forward ? getFirstKey(key, location) : getLastKey(key, location);
	}

	/**
	 * ����ɨ�跽����ҳ���һ������һ��(��Ҷ��ҳ���з���ɨ������Infinit Key)�����ڷ���ɨ��Ҷ�ڵ�����
	 * @param forward �Ƿ���ǰ��ɨ��
	 * @param key OUT �����ֵ
	 * @param location INOUT ������벻ΪNULL������������λ��
	 * @return 
	 */
	inline void getExtremeKeyAtLeaf(bool forward, SubRecord *key, MkeyLocation *location = NULL) {
		forward ? getFirstKey(key, location) : getLastValidKey(key, location);
	}


	/**
	 * ���ҳ���һ��
	 * @param firstKey OUT
 	 * @param location INOUT ������벻ΪNULL������������λ��
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
	 * ���ҳ�����һ��
	 * @param lastKey OUT
 	 * @param location INOUT ������벻ΪNULL������������λ��
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
	 * ���ҳ�����һ��valid��, ���ҳ��ֻ��һ��Infinit Key���Ǿͷ���Infinit Key
	 * @param lastKey OUT
 	 * @param location INOUT ������벻ΪNULL������������λ��
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
			//��Ҷ�ӽڵ������ҽڵ㣬���Ҵ�Ҷ�ڵ��г���InifinitKey����Valid��
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
	 * ����������ҳ���ַ
	 * @param rightPage
	 * @return 
	 */
	inline void setPrevPage(MIndexPageHdl prevPage) {
		m_prevPage = prevPage;
	}

	/**
	 * ����������ҳ���ַ
	 * @param rightPage
	 * @return 
	 */
	inline void setNextPage(MIndexPageHdl nextPage) {
		m_nextPage = nextPage;
	}

	/**
	 * ���ҳ������
	 * @return 
	 */
	inline IndexPageType getPageType() const {
		return (IndexPageType)m_pageType;
	}

	/**
	 * ����ҳ������
	 * @param pageType
	 */
	inline void setPageType(IndexPageType pageType) {
		m_pageType = (u8)pageType;
	}

	/**
	 * ���ҳ�����
	 * @return 
	 */
	inline u8 getPageLevel() const {
		return m_pageLevel;
	}
	
	/**
	 * ����ҳ�����
	 */
	inline void incrPageLevel() {
		m_pageLevel++;
	}

	/**
	 * ����ҳ�����
	 */
	inline void decPageLevel() {
		m_pageLevel--;
	}
	
	/** �ж�ĳ������ҳ���Ƿ���Ҷ��� */
	inline bool isPageLeaf() const {
		return (m_pageType & LEAF_PAGE) || (m_pageType & ROOT_AND_LEAF);
	}
	/** �ж�ĳ������ҳ���Ƿ��Ǹ���� */
	inline bool isPageRoot() {
		return (m_pageType & ROOT_PAGE) || (m_pageType & ROOT_AND_LEAF);
	}

	/** �ж�һ��ҳ���Ƿ�Ϊ�� */
	inline bool isPageEmpty() {
		return 0 == m_keyCount;
	}

	/**
	 * ��ø���ҳ����������ID��
	 * @return 
	 */
	inline u64 getMaxTrxId() const {
		assert(isPageLeaf());
		return m_maxTrxId;
	}

	/**
	 * ���ø���ҳ����������ID��
	 * @param maxTrxId 
	 */
	inline void setMaxTrxId(u64 maxTrxId) {
		assert(isPageLeaf());
		if (maxTrxId > m_maxTrxId) {
			m_maxTrxId = maxTrxId;
		}
	}

	/**
	 * ���ҳ�����ʱ���
	 * @return 
	 */
	inline u64 getTimeStamp() {
		return m_timeStamp;
	}

	/**
	 * ����ҳ�����ʱ���
	 * @return 
	 */
	inline void incrTimeStamp() {
		++m_timeStamp;
	}

	/**
	 * ���ҳ��ļ�ֵ�����ܳ���(Ҷҳ�治���������high-key�ĳ���)
	 * @return 
	 */
	inline u16 getKeyDataTotalLen() {
		return m_keyCount > 0 ? (getKeyEOffsetByNo(m_keyCount - 1) - getKeySOffsetByNo(0)) : 0;
	}

	/**
	 * ���ָ���������������ʼƫ����
	 * @param dirNo
	 * @return 
	 */
	inline u16 getKeySOffsetByNo(u16 dirNo) const {
		assert(dirNo < m_keyCount);
		return ((u16 *)((byte *)this + m_dirStart))[dirNo];
	}

	/**
	 * ���ָ������������ݽ���ƫ����
	 * @param dirNo
	 * @return 
	 */
	inline u16 getKeyEOffsetByNo(u16 dirNo) const {
		assert(dirNo < m_keyCount);
		return (dirNo == m_keyCount - 1) ? (m_dirStart - m_freeSpace) : getKeySOffsetByNo(dirNo + 1);
	}

	/**
	 * ���ҳ����ָ�����������ʼ��ַ
	 * @param dirNo Ҫ��ȡ����������ҳ���е����
	 * @return 
	 */
	inline byte *getKeyStartByNo(u16 dirNo) const {
		assert(dirNo < m_keyCount);
		u16 offset = ((u16 *)((byte *)this + m_dirStart))[dirNo];
		return (byte *)this + offset;
	}

	/**
	 * ���ҳ���ֵ������ʼ��ַ
	 * @return 
	 */
	inline byte *getKeyDataStart() const {
		return (0 == m_keyCount) ? getKeyDataEnd() : getKeyStartByNo(0);
	}

	/** 
	 * ����ҳ����������Ľ���ƫ����
	 */
	inline byte* getKeyDataEnd() const {
		return (byte *)this + m_dirStart - m_freeSpace;
	}

	
	/**
	 * ͨ������ID��������ҳ���ʶ
	 * @param indexId
	 * @return 
	 */
	inline static u32 formPageMark(u16 indexId) {
		return u32(indexId);
	}

	/**
	 * �������ID
	 * @param mark
	 * @return 
	 */
	inline static u16 getIndexId(u32 mark) {
		return (u16)mark;
	}

	/**
	 * �ƶ�ҳ���ֵ
	 * @param dst Ŀ��ҳ��
	 * @param src Դҳ�� 
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

	/** ��װ�ƶ����ݣ�ʹ��memmove */
	inline static void shiftDataByMOVE(void *dest, void *src, size_t size) {
		memmove(dest, src, size);
	}

	/**
	 * ����洢������ֵ��Ϣ�Լ���Ŀ¼��Ҫ�Ŀռ��С������RowId����ҳ����(��Ҷҳ��)��
	 * @param key        ��ֵ��¼
	 * @param isLeafPage �Ƿ���Ҷҳ��ļ�ֵ
	 * @return ��Ҫ�Ĵ洢�ռ��С
	 */
	inline static u16 calcSpaceNeeded(const SubRecord *key, bool isLeafPage) {
		assert(KEY_NATURAL == key->m_format);
		return MIndexKeyOper::calcKeyTotalLen((u16)key->m_size, isLeafPage) + MINDEX_DIR_SIZE;
	}

	/**
	 * �Ƿ���Ҫ������ȡ�߼���һ��
	 * ��ϸ˵���ο�IndexPage::isNeedFetchNext
	 *
	 * @param result
	 * @param forward
	 * @return 
	 */
	inline static bool isNeedFetchNext(s32 result, const SearchFlag *flag) {
		return (result == 0) || ((result > 0) == flag->isForward());
	}

	/**
	 * ��ҳ��high-key�Ƚϴ�С
	 * @pre �����Ѿ�����ҳ��latch
	 * @param key ���Ҽ�
	 * @param assistKey ���ڱȽϵĸ�����ֵ 
	 * @param comparator ��ֵ�Ƚ���
	 * @param flag ���ұ�־
	 * @return С�ڷ���-1�����ڷ���1�����ڷ���0
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
	 * ��õ�ǰҳ���ǰһҳ��
	 * @return 
	 */
	inline MIndexPageHdl getPrevPage() const {
		return m_prevPage;
	}


	/**
	 * ��õ�ǰҳ�����һҳ��
	 * @return 
	 */
	inline MIndexPageHdl getNextPage() const {
		return m_nextPage;
	}

	/**
	 * ���ҳ����пռ��С
	 * @return 
	 */
	inline u16 getFreeSpace() const {
		return m_freeSpace;
	}

	/**
	 * ���ҳ���Ѿ�ʹ�õĿռ��С
	 * @return 
	 */
	inline u16 getDataSpaceUsed() const {
		return (u16)MINDEX_PAGE_SIZE - MINDEX_PAGE_HEADER_SIZE - m_freeSpace;
	}

	/**
     * ҳ���Ƿ�����(���пռ䳬��ҳ���һ����Ϊ������)
     * @return 
     */
	inline bool isUnderFlow() const {
		return m_freeSpace > (MIndexPage::MINDEX_PAGE_SIZE - MIndexPage::MINDEX_PAGE_HEADER_SIZE) / 2;
	}

	/**
	 * �ж��Ƿ��ܺ���һҳ��ϲ�
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
	u8             m_pageType;   /* ҳ������ */
	u8             m_pageLevel;  /* ҳ����� */
	u16            m_freeSpace;  /* ���пռ� */
	u16            m_keyCount;	 /* ҳ������ */
	u16            m_dirStart;	 /* ��Ŀ¼��ʼλ�� */
	MIndexPageHdl  m_prevPage;   /* ǰ��ҳ�� */
	MIndexPageHdl  m_nextPage;   /* ���ҳ�� */
	u32            m_pageMark;   /* ����ҳ���ʶ */
	u64            m_maxTrxId;	 /* ҳ���������ID�� */
	u64            m_timeStamp;	 /* α����ʱ��� */
};
#pragma pack()

}

#endif