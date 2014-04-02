/**
 * NTSE B+������ҳ�������
 *
 * author: naturally (naturally@163.org)
 */

// IndexPage.cpp: implementation of the IndexPage class.
//
////////////////////////////////////////////////////////////////////

#include "misc/Session.h"
#include "btree/IndexPage.h"
#include "btree/IndexKey.h"
#include "btree/IndexLog.h"
#include "btree/IndexBPTree.h"
#include "btree/IndexBPTreesManager.h"
#include "misc/Trace.h"
#include "misc/Verify.h"
#include "misc/Record.h"
#include <algorithm>

namespace ntse {


/**
 * IndexPage��ʵ������ҳ�����
 *
 * ����ҳ��洢�������£�
 * ----------------------------------------------------------------------
 * |ҳ��ͷ����Ϣ��ҳ�����ͣ�ʣ��ռ䣬ǰ�����ҳ�����Ϣ��				|
 * +--------------------------------------------------------------------+
 * |ҳ��������|MiniPage								|MiniPage			|
 * +--------------------------------------------------------------------+
 * |						���ɸ�MiniPage								|
 * +--------------------------------------------------------------------+
 * |MiniPage		|��������											|
 * +--------------------------------------------------------------------+
 * |��Ŀ¼��|	|	|	|	|	|	|	|	|	|	|	|	|	|	|	|
 * ----------------------------------------------------------------------
 *
 * ҳ��洢��ʽ��������ͼ��ʾ����Ҫ��Ϊ�Ĳ��֣�
 * 1. ҳ��ͷ����Ϣ���֣����������ҳ�泣�������Ϣ���ɲμ�IndexPage����ı���
 * 2. ҳ��������������Ĵ��ҳ�������������ֵ����
 * 3. ��Ŀ¼��������Ĵ�Ÿ�����Ŀ¼��ÿ����Ŀ¼ռMINI_PAGE_DIR_SIZE���ֽ�
 * 4. ҳ��������򣬿������������µ�������Ϣ�Ŀռ�
 *
 * ҳ���ڵ�����ͳһ����ǰ׺ѹ������ʽ�����洢��Ϊ�˱���ҳ�������ַ����ң�
 * ��ҳ����������ѹ�������ɸ���1�����߶�������MINI_PAGE_MAX_ITEMS�������ݿ����߼��ϵ�һ�����ݣ�����һ��MiniPage������
 * ��Ŀ¼�����þ���������ʾһ��MiniPage��������ݣ�MiniPage��ʼ��������2���ֽڣ��Ͱ����ļ�¼������1���ֽڣ�
 * ��Ŀ¼���մ�С�����˳���ʾҳ���ڸ���MiniPage����Ϣ���������ݵ�ʱ������ͨ����Ŀ¼�Ķ��ֲ��ң�
 * ��λ��ĳ��MiniPage��Ȼ������MiniPage����˳�����
 * ע�⣺Ϊ��ʹ���ֲ��ҷ���Ч���ܾ����ܸߣ���ĳ����ֵ������ǰ����ֵ�޷�����ǰ׺ѹ����ʱ��
 * �ü�ֵ��洢����һ����ͬ��MiniPage����
 *
 * ���ݴ洢��ҳ��ͷ����洢����Ŀ¼�洢��ҳ��β��ǰ�洢���������������ݼ����غϵ��¿ռ䲻��ʱ����ʾҳ��Ϊ��
*/

SearchFlag SearchFlag::DEFAULT_FLAG;

/*
 * ��ʼ��ҳ�汾��
 *
 * @param session	�Ự���
 * @param logger	��־��¼��
 * @param clearData	�Ƿ�����ҳ������
 * @param logContent����ҳ��ԭ���������ݵ�������Ƿ���Ҫ��¼ԭ�����ݣ����ڻָ�
 * @param pageId	ҳ��ID��
 * @param pageMark	����ҳ��Ψһ��ʶ
 * @param pageType	��ǰҳ�������
 * @param pageLevel	��ǰҳ���ڲ�����Ĭ��Ϊ0
 * @param prevPage	��ǰҳ�߼�ǰ��ҳ�棬Ĭ��ΪNULL
 * @param nextPage	��ǰҳ�߼����ҳ�棬Ĭ��ΪNULL
 */
void IndexPage::initPage(Session *session, IndexLog *logger, bool clearData, bool logContent, PageId pageId, u32 pageMark, IndexPageType pageType, u8 pageLevel, PageId prevPage, PageId nextPage) {
	byte newPage[INDEX_PAGE_SIZE];
	IndexPage *page = (IndexPage*)newPage;
	memset((byte*)page, 0, INDEX_PAGE_SIZE);
	page->m_pageMark = pageMark;
	page->m_pageType = (u8)pageType;
	page->m_pageLevel = pageLevel;
	page->m_freeSpace = INDEX_PAGE_SIZE - INDEXPAGE_DATA_START_OFFSET;
	page->m_pageCount = 0;
	page->m_dirStart = INDEX_PAGE_SIZE;
	page->m_prevPage = prevPage;
	page->m_nextPage = nextPage;
	page->m_miniPageNum = 0;
	page->m_nChance = 0;
	page->m_insertOffsetRatio = 5000;
	page->m_smoLSN = 0;
	page->m_splitedMPNo = (u16)-1;
	u16 realAffectedSize = clearData ? INDEX_PAGE_SIZE - sizeof(Page) : sizeof(IndexPage) - sizeof(Page);
	u16 logSize = logContent ? INDEX_PAGE_SIZE - sizeof(Page) : sizeof(IndexPage) - sizeof(Page);
	m_lsn = logger->logPageUpdate(session, IDX_LOG_UPDATE_PAGE, pageId, sizeof(Page), (byte*)page + sizeof(Page), (byte*)this + sizeof(Page), logSize, m_lsn, clearData);
	memcpy((byte*)this + sizeof(Page), (byte*)page + sizeof(Page), realAffectedSize);
}


/** ���ҳ��������������ݣ�����¼��־
 * @param offset	�������ݵ���ʼƫ��
 * @param size		�������ݵ��ܳ���
 */
void IndexPage::clearData(u16 offset /*= sizeof(IndexPage)*/, u16 size/* = Limits::PAGE_SIZE - sizeof(IndexPage)*/ ) {
	memset((byte*)this + offset, 0, size);
}


/**
 * ���ñ�ҳ���״̬Ϊ���ڽ���SMO����
 * ǰ����ʹ���߶�ҳ�����X-Latch
 * @param session	�Ự���
 * @param pageId	ҳ��ID
 * @param logger	��־��¼��
 */
void IndexPage::setSMOBit(Session *session, PageId pageId, IndexLog *logger) {
	u8 oldType = m_pageType;
	u8 newType = (u8)(oldType | BEING_SMO);
#ifdef NTSE_TRACE
	u64 oldLSN = m_lsn;
#endif
	m_lsn = logger->logPageUpdate(session, IDX_LOG_UPDATE_PAGE, pageId, OFFSET(IndexPage, m_pageType), &newType, &oldType, 1, m_lsn);
	m_pageType |= BEING_SMO;
	ftrace(ts.idx, tout << "SetSMOBit oldLSN: " << pageId << oldLSN << " newLSN: " << m_lsn << " smoLSN: " << m_lsn);
}


/**
 * �����ҳ���SMO״̬λ
 * @param session	�Ự���
 * @param pageId	ҳ��ID
 * @param logger	��־��¼��
 */
void IndexPage::clearSMOBit(Session *session, PageId pageId, IndexLog *logger) {
	assert((m_pageType & BEING_SMO) != 0);
	u8 oldType = m_pageType;
	u8 newType = (u8)(oldType & ~BEING_SMO);
#ifdef NTSE_TRACE
	u64 oldLSN = m_lsn;
#endif
	m_lsn = logger->logPageUpdate(session, IDX_LOG_UPDATE_PAGE, pageId, OFFSET(IndexPage, m_pageType), &newType, &oldType, 1, m_lsn);
	m_pageType &= ~BEING_SMO;
	ftrace(ts.idx, tout << "ClearSMOBit oldLSN: " << pageId << oldLSN << " newLSN: " << m_lsn);
}



/**
 * ����ҳ��ĺ���ҳ��ID
 * @post ����SMO������m_smoLSN�ᱻ����
 *
 * @param session		�Ự���
 * @param logger		��־��¼��
 * @param pageId		ҳ��ID
 * @param newNextPage	�µĺ��ҳ��
 */
void IndexPage::updateNextPage(Session *session, IndexLog *logger, PageId pageId, PageId newNextPage) {
#ifdef NTSE_TRACE
	u64 oldLSN = m_lsn;;
#endif
	m_lsn = logger->logPageUpdate(session, IDX_LOG_UPDATE_PAGE, pageId, OFFSET(IndexPage, m_nextPage), (byte*)&newNextPage, (byte*)&m_nextPage, sizeof(PageId), m_lsn);
	m_nextPage = newNextPage;
	m_smoLSN = m_lsn;
	ftrace(ts.idx, tout << " Update next page: oldLSN: " << oldLSN << " newLSN: " << m_lsn << " smoLSN: " << m_lsn);
}



/**
 * ����ҳ��ĺ���ҳ��ID
 * @post ����SMO������m_smoLSN�ᱻ����
 *
 * @param session		�Ự���
 * @param logger		��־��¼��
 * @param pageId		ҳ��ID
 * @param newPrevPage	�µ�ǰ��ҳ��
 */
void IndexPage::updatePrevPage(Session *session, IndexLog *logger, PageId pageId, PageId newPrevPage) {
#ifdef NTSE_TRACE
	u64 oldLSN = m_lsn;
#endif
	m_lsn = logger->logPageUpdate(session, IDX_LOG_UPDATE_PAGE, pageId, OFFSET(IndexPage, m_prevPage), (byte*)&newPrevPage, (byte*)&m_prevPage, sizeof(PageId), m_lsn);
	m_prevPage = newPrevPage;
	m_smoLSN = m_lsn;
	ftrace(ts.idx, tout << " Update prev page: oldLSN: " << oldLSN << " newLSN: " << m_lsn << " smoLSN: " << m_lsn);
}




/**
 * ��λ��ҳ���к��ʵ�mini pageĿ¼��
 *
 * @param key			Ҫ��λ�ļ�ֵ
 * @param flag			���ұ��
 * @param comparator	������ֵ�Ƚ���
 * @param tmpRec		��Ҫ���ڱ�����ʱ��ֵ�ļ�¼�����Ƿ���֮��ü�ֵ����������
 * @param result		OUT	���Ҽ�ֵ>Ŀ¼���1��<����-1�����ڷ���0
 * @return ���ض�λ����mini page��Ŀ¼�ţ����ҳ��Ϊ�գ�����-1
 */
u16 IndexPage::findMiniPageInPage(const SubRecord *key, SearchFlag *flag, KeyComparator *comparator, SubRecord *tmpRec, s32 *result) {
	s32 start = 0;
	s32 end = m_miniPageNum - 1;
	byte *origData = tmpRec->m_data;
	u16 mpNo;

	while (end > start) {
		u16 mid = (u16)(end - (end - start) / 2);
		IndexKey *indexKey = (IndexKey*)((byte*)this + getMiniPageDataOffset(mid));
		indexKey->fakeExtractMPFirstKey(tmpRec, !isPageLeaf());
		s32 midresult = compareKey(key, tmpRec, flag, comparator);
		if (midresult > 0)
			start = mid;
		else if (midresult < 0)
			end = mid - 1;
		else {
			*result = 0;
			mpNo = mid;
			goto Got;
		}
	}

	if (start != 0) {
		*result = 1;
	} else {
		IndexKey *indexKey = (IndexKey*)((byte*)this + getMiniPageDataOffset((u16)start));
		indexKey->fakeExtractMPFirstKey(tmpRec, !isPageLeaf());
		*result = compareKey(key, tmpRec, flag, comparator);
	}

	mpNo = (u16)start;

Got:
	// ������Ҫ������ָ���滻����
	tmpRec->m_data = origData;
	return mpNo;
}


/**
 * ��ָ����Mini Page����Ѱ��ָ���ļ�¼������ȡ�õļ�¼���ݣ���¼��ƫ����
 *
 * ����涨�����м�ֵ�������߼���Ӧ�ø���flag���а�����forward/includeKey/equalAllowable���������ڱ���������
 *			Ҳ����˵����fetchNext�����ص����������������forward������һ��ȡ�������getNext/getPrev��
 * @param findkey		ҪѰ�ҵļ�¼��ֵ
 * @param keyInfo		IN	������ǲ��Ҽ�ֵ�͸�MP���е�һ���ֵ��λ�úͱȽ���Ϣ
 *						OUT	�����ҵ���ֵ��λ����Ϣ�������ҵ��ļ�ֵ��Ҫ�ҵļ�ֵ��С��ϵ�޷�����ȷ��
 * @param comparator	������ֵ�Ƚ���
 * @param foundKey			OUT	���ڱ����ҵ��ļ�¼
 * @return ��ʾ�ϲ��Ƿ���Ҫ����forwardȡ�߼��ϵ���һ��
 */
bool IndexPage::findKeyInMiniPage(const SubRecord *findkey, KeyInfo *keyInfo, SearchFlag *flag, KeyComparator *comparator, SubRecord *foundKey) {
	assert(keyInfo->m_miniPageNo < m_miniPageNum);

	bool forward = flag->isForward();
	bool equalAllowable = flag->isEqualAllowed();

	byte *origData = NULL;
	u16 MPCount = getMiniPageItemCount(keyInfo->m_miniPageNo);
	u16 count = 0;
	s32 res;
	u16 pos = getMiniPageDataOffset(keyInfo->m_miniPageNo);
	u16 lastpos = pos;
	u16 end = (keyInfo->m_miniPageNo < m_miniPageNum - 1) ? getMiniPageDataOffset(keyInfo->m_miniPageNo + 1) : getDataEndOffset();
	bool isPageIdExist = !isPageLeaf();
	while (pos < end) {
		lastpos = pos;
		IndexKey *indexKey = (IndexKey*)((byte*)this + lastpos);

		if (count == 0) {	// ��һ����ҳ�治ֹһ����Բ�����memcpy������ֱ�ӽ�ѹ����
			if (MPCount == 1)
				pos = pos + indexKey->extractKey(foundKey->m_data, foundKey, isPageIdExist);
			else {
				origData = foundKey->m_data;
				pos = pos + indexKey->fakeExtractMPFirstKey(foundKey, isPageIdExist);
			}
		} else if (count == 1) {	// �ڶ���滻������ָ�룬��ʼ���н�ѹ�Ƚ�
			byte *lastData = foundKey->m_data;
			foundKey->m_data = origData;
			pos = pos + indexKey->extractKey(lastData, foundKey, isPageIdExist);
		} else {	// ֮��ÿ�Ҫ��ѹ
			pos = pos + indexKey->extractKey(foundKey->m_data, foundKey, isPageIdExist);
		}

		++count;
		res = compareKey(findkey, foundKey, flag, comparator);
		if ((res == 0 && equalAllowable) || res < 0) {	// �ҵ���Ȼ��߸�������ʾ���ҳɹ�
			if (count == 1 && MPCount != 1) {
				foundKey->m_data = origData;
				indexKey->extractKey(NULL, foundKey, isPageIdExist);
			}
			goto Finish;
		}
	}

	// ��Mini Page����������������Ƚϴ�
	res = 1;

Finish:
	keyInfo->m_sOffset = lastpos;
	keyInfo->m_eOffset = pos;
	keyInfo->m_result = res;
	keyInfo->m_keyCount = count;
	
	return isNeedFetchNext(res, forward);
}



/**
 * �ڸ�ҳ�浱�в���ָ����
 * @pre	����ҳ��Ӧ�ñ�֤��Ϊ��
 * @param key			ҪѰ�ҵļ�¼��ֵ
 * @param flag			���ұ��
 * @param comparator	������ֵ�Ƚ���
 * @param foundKey		OUT	���ڱ����ҵ��ļ�¼
 * @param keyInfo		IN/OUT	�����ҵ���¼��λ����Ϣ
 * @return ��ʾ�ϲ��Ƿ���Ҫ����forwardȡ�߼��ϵ���һ��
 */
bool IndexPage::findKeyInPage(const SubRecord *key, SearchFlag *flag, KeyComparator *comparator, SubRecord *foundKey, KeyInfo *keyInfo) {
	assert(m_miniPageNum != 0);
	bool forward = flag->isForward();

	if (!IndexKey::isKeyValid(key)) {	// �Ƚϼ�ֵΪ��
		getExtremeKey(foundKey, forward, keyInfo);
		return false;
	}

	u16 MPNo = keyInfo->m_miniPageNo = findMiniPageInPage(key, flag, comparator, foundKey, &(keyInfo->m_result));
	assert(keyInfo->m_result >= 0 || (MPNo == 0 && MPNo < m_miniPageNum));

	bool fetchNext = findKeyInMiniPage(key, keyInfo, flag, comparator, foundKey);

	if (MPNo != 0)
		keyInfo->m_keyCount = keyInfo->m_keyCount + getMPsItemCounts(MPNo);
	if (keyInfo->m_result > 0 && m_pageLevel != 0) {	// ��Ҷ�����Ҷ�λ����
		assert(keyInfo->m_eOffset == getDataEndOffset() || keyInfo->m_eOffset == getMiniPageDataOffset(MPNo + 1));
		if (keyInfo->m_eOffset != getDataEndOffset()) {
			++keyInfo->m_miniPageNo;
			keyInfo->m_sOffset = keyInfo->m_eOffset;
			keyInfo->m_eOffset = getNextKey(NULL, keyInfo->m_eOffset, false, &keyInfo->m_miniPageNo, foundKey);
			keyInfo->m_result = -1;
			++keyInfo->m_keyCount;
			return false;
		}
	}

	return fetchNext;
}


/**
 * ���ݷ���õ���ǰҳ��ĵ�һ��������һ��
 * @param key		���ص�key��ֵ
 * @param forward	true��ʾ���ص�һ�false�������һ��
 * @keyInfo			out �����ҵ������Ϣ
 */
void IndexPage::getExtremeKey(SubRecord *key, bool forward, KeyInfo *keyInfo) {
	if (forward) {
		getFirstKey(key, keyInfo);
		keyInfo->m_keyCount = 1;
	} else {
		getLastKey(key, keyInfo);
		keyInfo->m_keyCount = m_pageCount;
	}
	keyInfo->m_result = 0;
}


/**
 * ȡ��ָ��ƫ�Ƶĺ�һ��
 *
 * @param lastKey		ǰһ����ֵ������ƴ�յ�ǰ��ֵ
 * @param offset		Ҫ��ȡ��ֵ����ʼƫ����
 * @param spanMiniPage	�Ƿ��Զ���MiniPageȡ��
 * @param miniPageNo	in/out	��ǰ���ڵ�MiniPage�ţ�����ôλ�ȡ��¼��MiniPage����ֵ���޸�
 * @param foundKey		in/out	�����������ļ�¼
 * @param key		in/out	�����������ļ�¼
 * @return ��һ����ֵ����ʼƫ����������������ԽMiniPage����ָ��MiniPageȡ����������ҳ��ȡ�������ش����offset
 *			�����ǰȡ��ʧ�ܣ�record->m_size����Ϊ0
 */
u16 IndexPage::getNextKey(const SubRecord *lastKey, u16 offset, bool spanMiniPage, u16 *miniPageNo, SubRecord *foundKey) {
	assert(lastKey == NULL || lastKey->m_format == KEY_COMPRESS);
	assert(*miniPageNo < m_miniPageNum);
	assert(offset < INDEX_PAGE_SIZE);

	byte *prefixData = (lastKey == NULL ? NULL : lastKey->m_data);

	foundKey->m_size = 0;
	// �жϸ�ҳ���Ƿ��ȡ����
	if (offset >= getDataEndOffset())
		return offset;

	// �жϵ�ǰ�Ƿ�λ��һ���µ�MiniPage
	if (*miniPageNo < m_miniPageNum - 1 && offset >= getMiniPageDataOffset(*miniPageNo + 1)) {
		if (spanMiniPage)
			*miniPageNo += 1;
		else
			return offset;
	}

	IndexKey *indexKey = (IndexKey*)((byte*)this + offset);
	return offset + indexKey->extractKey(prefixData, foundKey, !isPageLeaf());
}



/**
 * ȡ��ָ��ƫ�Ƶ�ǰһ������ǰminiPage�Ѿ��þ�������ָ����ȡǰһ��MiniPage���������ҳ����꣬���ؿ�
 *
 * @param offset		��һ����¼��ƫ�ƣ�ȡ���Ǹ�ƫ�Ƶ�ǰһ����¼
 * @param spanMiniPage	�Ƿ��Զ���MiniPageȡ��
 * @param miniPageNo	INOUT ��ǰ��ȡ��ֵ���ڵ�MiniPage��
 * @param foundKey		INOUT ����ǰһ��ֵ�ļ�¼
 * @param key		INOUT ����ǰһ��ֵ�ļ�¼
 * @return ǰһ����ֵ����ʼƫ����������������ԽMiniPage����ָ��MiniPageȡ����������ҳ��ȡ�������ش����offset
 *			�����ǰȡ��ʧ�ܣ�record->m_size����Ϊ0
 */
u16 IndexPage::getPrevKey(u16 offset, bool spanMiniPage, u16 *miniPageNo, SubRecord *foundKey) {
	// TODO: Ѱ�����Ч�ķ�ʽ...
	assert(*miniPageNo < m_miniPageNum);
	assert(offset < INDEX_PAGE_SIZE);

	foundKey->m_size = 0;
	// �����жϵ�ǰҳ�Ƿ��þ�
	if (offset <= INDEXPAGE_DATA_START_OFFSET)
		return offset;

	// ����ж��Ƿ��þ��˵�ǰMini Page������ǣ�ȡǰһ��Mini Page
	if (offset <= getMiniPageDataOffset(*miniPageNo)) {
		if (!spanMiniPage)
			return offset;
		assert(*miniPageNo > 0);
		*miniPageNo -= 1;
		assert(offset >= getMiniPageDataOffset(*miniPageNo));
	}

	// ��ʼ��ָ��MiniPage���ж�λǰһ����ҽ���϶��ڵ�ǰMini Page����
	u16 start, newoff;
	start = newoff = getMiniPageDataOffset(*miniPageNo);
	while (newoff < offset) {
		start = newoff;
		IndexKey *indexKey = (IndexKey*)((byte*)this + start);
		newoff = newoff + indexKey->extractKey(foundKey->m_data, foundKey, !isPageLeaf());
	}

	assert(newoff == offset);
	return start;
}


/**
 * ��ȡҳ��ĵ�һ��
 *
 * @param key		OUT	����ȡ����������
 * @param keyInfo		OUT	�����ֵλ����Ϣ
 */
void IndexPage::getFirstKey(SubRecord *key, KeyInfo *keyInfo) {
	keyInfo->m_miniPageNo = 0;
	keyInfo->m_sOffset = INDEXPAGE_DATA_START_OFFSET;
	keyInfo->m_eOffset = getNextKey(key, INDEXPAGE_DATA_START_OFFSET, true, &(keyInfo->m_miniPageNo), key);
}


/**
 * ��ȡҳ��ĵ�һ��
 *
 * @param key		OUT	����ȡ����������
 * @param keyInfo		OUT	�����ֵλ����Ϣ
 */
void IndexPage::getLastKey(SubRecord *key, KeyInfo *keyInfo) {
	keyInfo->m_miniPageNo = m_miniPageNum - 1;
	keyInfo->m_eOffset = getDataEndOffset();
	keyInfo->m_sOffset = getPrevKey(keyInfo->m_eOffset, true, &(keyInfo->m_miniPageNo), key);
}


/** 
 * �ж�ĳ����ֵ�ǲ���һ����ҳ�淶Χ�ڣ������жϻ�ο���ֵ�ȽϽ��
 * @pre ���ҳ���keyInfo����͵�ǰҳ��İ汾һ�£�ҳ�治�ñ��޸Ĺ�
 * @param keyInfo ��ֵ��Ϣ
 */
bool IndexPage::isKeyInPageMiddle(KeyInfo *keyInfo) {
	return !((keyInfo->m_sOffset <= INDEXPAGE_DATA_START_OFFSET && keyInfo->m_result < 0) ||
		(keyInfo->m_eOffset >= getDataEndOffset() && keyInfo->m_result > 0));
}


/**
 * ���ݲ��ҷ���������Ƚ�������ֵ�Ĵ�С
 * @param key			Ҫ���ҵļ�ֵ
 * @param indexKey		�����϶�ȡ�ļ�ֵ
 * @param flag			�Ƚϱ��
 * @param comparator	������ֵ�Ƚ���
 * @return ����key��indexKey�Ĺ�ϵ��key���ڵ���С��indexKey���ֱ���1/0/-1��ʾ
 */
s32 IndexPage::compareKey(const SubRecord *key, const SubRecord *indexKey, SearchFlag *flag, KeyComparator *comparator) const {
	bool includeKey = flag->isIncludingKey();
	bool forward = flag->isForward();
	bool equalAllowable = flag->isEqualAllowed();

	s32 result = comparator->compareKey(key, indexKey);

	/**************************************************************************************/
	/* ������		includeKey	forward		compare result: 1	0/0	(equalAllowable)	-1
		===================================================================================
		>=				1			1					|	1	-1/0					-1
		=				1			1					|	1	-1/0					-1
		>				0			1					|	1	1/0						-1
		<				0			0					|	1	-1/0					-1
		<=				1			0					|	1	1/0						-1
		===================================================================================
		���������ȣ����ڱȽϽ��Ϊ0��ֱ�ӷ���0
	*/
	/**************************************************************************************/
	return result != 0 ? result : equalAllowable ? 0 : (includeKey ^ forward) ? 1 : -1;
}


/**
 * Ԥ�ж�һ��ҳ��ɾ����ָ����ֵ֮���Ƿ���ҪSMO����ҪԤ�е��������Ҷҳ��
 * @param deleteKey			Ҫɾ���ļ�ֵ
 * @param prevKey			out ����ɾ����ֵ��ǰ����û��Ҫ��֤m_size = 0
 * @param nextKey			out ����ɾ����ֵ�ĺ�����û��Ҫ��֤m_size = 0
 * @param keyInfo			Ҫɾ����ֵ�ļ�ֵ��Ϣ
 * @param prevKeySOffset	out ����ǰһ��ֵ����ʼƫ���������û�У�����keyInfo->m_eOffset
 * @param nextKeyEOffset	out ���غ�һ��ֵ�Ľ���ƫ���������û�У�����keyInfo->m_sOffset
 * @return true��ʾ��Ҫ�ϲ���false��ʾ����Ҫ�ϲ�
 */
bool IndexPage::prejudgeIsNeedMerged(const SubRecord *deleteKey, SubRecord *prevKey, SubRecord *nextKey, KeyInfo keyInfo, u16 *prevKeySOffset, u16 *nextKeyEOffset) {
	assert(isPageLeaf());
	*prevKeySOffset = getPrevKey(keyInfo.m_sOffset, false, &(keyInfo.m_miniPageNo), prevKey);
	*nextKeyEOffset = getNextKey(deleteKey, keyInfo.m_eOffset, false, &(keyInfo.m_miniPageNo), nextKey);

	// ���������ɾ������ֵ֮��ҳ��ʣ���������
	if (!IndexKey::isKeyValid(nextKey)) {
		// ��Ҫ�жϵ�ǰMiniPage�ǲ���ͬʱ��Ҫ����ɾ��
		u16 deleteMPDirSize = getMiniPageItemCount(keyInfo.m_miniPageNo) == 1 ? MINI_PAGE_DIR_SIZE : 0;
		return (INDEX_PAGE_SIZE - m_freeSpace - (keyInfo.m_eOffset - keyInfo.m_sOffset) - deleteMPDirSize <= INDEXPAGE_DATA_MIN_SIZE && m_nChance <= 1) || m_pageCount == 1;	// ֻʣһ��ɾ�������Ҫ�ϲ�
	}

	assert(*nextKeyEOffset > keyInfo.m_eOffset);
	u16 needSpace = IndexKey::computeSpace(prevKey, nextKey, false);
	return INDEX_PAGE_SIZE - m_freeSpace - (*nextKeyEOffset - keyInfo.m_sOffset - needSpace) <= INDEXPAGE_DATA_MIN_SIZE && m_nChance <= 1;
}



/**
 * ɾ��ҳ����ָ����һ��
 *
 * @param session		�Ự���
 * @param logger		��־��¼��
 * @param pageId		ҳ��ID
 * @param prevKey		��������ɾ����ǰһ��ļ�¼�ռ�
 * @param deleteKey		Ҫɾ����������Ѿ�ȡ����
 * @param nextKey		��������ɾ�����һ��ļ�¼�ռ�
 * @param keyInfo		Ҫɾ������ҳ�浱��λ����Ϣ
 * @param siblingGot	��ʾɾ����ֵ��ǰ�����Ƿ��Ѿ���ȡ����Ĭ��Ϊfalse
 * @param prevKeySOffsetǰ��ļ�ֵ��ʼ��Ϣ�����siblingGotΪ�棬��ֵ��Ҫ���룬Ĭ��Ϊ0
 * @param nextKeyEOffset����ļ�ֵ������Ϣ�����siblingGot���𣬸�ֵ��Ҫ���룬Ĭ��ΪINDEX_PAGE_SIZE
 * @return ��ǰҳ���Ƿ���ҪSMO��true/false
 */
bool IndexPage::deleteIndexKey(Session *session, IndexLog *logger, PageId pageId, SubRecord *prevKey, const SubRecord *deleteKey, SubRecord *nextKey, KeyInfo keyInfo, bool siblingGot, u16 prevKeySOffset, u16 nextKeyEOffset) {
	assert(m_pageCount > 0);

	if (getMiniPageItemCount(keyInfo.m_miniPageNo) == 1) {
		// ����MiniPageֻ��һ�ɾ������MiniPage
		deleteMiniPage(session, logger, pageId, keyInfo.m_miniPageNo);
		return isNeedMerged();
	}

	u16 sOffset = keyInfo.m_sOffset, eOffset = keyInfo.m_eOffset;
	u16 nextEOffset = 0, prevSOffset = 0;
	// ����ȡ��ǰ����ļ�ֵ��miniPageNoʼ�ն�����
	if (!siblingGot) {
		prevSOffset = getPrevKey(keyInfo.m_sOffset, false, &(keyInfo.m_miniPageNo), prevKey);
		nextEOffset = getNextKey(deleteKey, eOffset, false, &(keyInfo.m_miniPageNo), nextKey);
	} else {
		prevSOffset = prevKeySOffset;
		nextEOffset = nextKeyEOffset;
	}

	byte oldStatus[INDEX_PAGE_SIZE];
	memcpy(oldStatus, (byte*)this + sOffset, nextEOffset - sOffset);

	u16 deltaSize = 0;
	u16 moveSize = getDataEndOffset() - nextEOffset;
	u16 nextKeyNewSpace;
	if (IndexKey::isKeyValid(nextKey)) {	// ���ں����ǰ�����ѹ������
		// �������¼�ʹ����ѹ���ʽ��ͣ�Ҳ����ʹ�ø���Ŀռ䣬ɾ����ֵ�Ŀռ�϶��㹻���
		byte *start = (byte*)this + sOffset;
		IndexKey *indexKey = (IndexKey*)start;
		nextKeyNewSpace = (u16)(indexKey->compressAndSaveKey(prevKey, nextKey, !isPageLeaf()) - start);
		// �ƶ������ռ�
		assert(start + nextKeyNewSpace <= (byte*)this + nextEOffset);
		shiftDataByMOVE(start + nextKeyNewSpace, (byte*)this + nextEOffset, moveSize);
		// ����ռ�仯
		assert(nextEOffset >= sOffset + nextKeyNewSpace);
		deltaSize = (nextEOffset - sOffset) - nextKeyNewSpace;
	} else {	// �����ں��ֱ��ɾ����ǰ��
		nextKeyNewSpace = 0;
		shiftDataByMOVE((byte*)this + sOffset, (byte*)this + eOffset, moveSize);
		deltaSize = eOffset - sOffset;
	}

#ifdef NTSE_TRACE
	u64 oldLSN = m_lsn;
#endif
	m_lsn = logger->logDMLUpdate(session, IDX_LOG_DELETE, pageId, sOffset, keyInfo.m_miniPageNo, oldStatus, nextEOffset - sOffset, (byte*)this + sOffset, nextKeyNewSpace, m_lsn);
	ftrace(ts.idx, tout << session << ": Delete key: " << pageId << rid(deleteKey->m_rowId) << (isPageLeaf() ? 0 : IndexKey::getPageId(deleteKey)) << " oldLSN: " << oldLSN << " newLSN: " << m_lsn;);

	assert(deltaSize > 0);
	// ����MiniPageĿ¼�Լ�ҳ�������Ϣ
	adjustMiniPageDataOffset(keyInfo.m_miniPageNo + 1,  0 - deltaSize);
	m_freeSpace = m_freeSpace + deltaSize;
	decMiniPageItemCount(keyInfo.m_miniPageNo);
	m_pageCount--;
	m_nChance > 0 ? --m_nChance : m_nChance;
	memset((byte*)this + getDataEndOffset(), 0, deltaSize);

	return isNeedMerged();
}



/**
 * ɾ��ָ����MiniPage
 * @param session		�Ự���
 * @param logger		��־��¼��
 * @param pageId		ҳ��ID
 * @param miniPageNo	ָ����MiniPage��
 * @attention ����֮��ҳ��m_freeSpace��m_dirStart�Ѿ�����
 */
void IndexPage::deleteMiniPage(Session *session, IndexLog *logger, PageId pageId, u16 miniPageNo) {
	assert(miniPageNo < m_miniPageNum);
	u16 miniPageEnd = (miniPageNo < m_miniPageNum - 1) ? getMiniPageDataOffset(miniPageNo + 1) : getDataEndOffset();
	u16 miniPageStart = getMiniPageDataOffset(miniPageNo);

	// ��¼ɾ��MiniPage�߼���־
#ifdef NTSE_TRACE
	u64 oldLSN = m_lsn;
#endif
	m_lsn = logger->logPageDeleteMP(session, IDX_LOG_DELETE_MP, pageId, (byte*)this + miniPageStart, miniPageEnd - miniPageStart, miniPageNo, m_lsn);
	ftrace(ts.idx, tout << "deleteMP: pageId: " << pageId << " MPNo: " << miniPageNo << " oldLSN: " << oldLSN << " newLSN: " << m_lsn);

	deleteMiniPageIn(miniPageNo, miniPageStart, miniPageEnd);
}


/**
 * ɾ��MiniPage��ʵ��������������¼��־
 * @param miniPageNo	Ҫɾ��MP��
 * @param miniPageStart	Ҫɾ��MP��ʼƫ��
 * @param miniPageEnd	Ҫɾ��MP����ƫ��
 */
void IndexPage::deleteMiniPageIn(u16 miniPageNo, u16 miniPageStart, u16 miniPageEnd) {
	ftrace(ts.idx, tout << miniPageNo << miniPageStart << miniPageEnd);

	assert(miniPageNo < m_miniPageNum);
	u16 deleteMiniPageSize =  miniPageEnd - miniPageStart;
	u16 dataLastPoint = getDataEndOffset();
	assert(deleteMiniPageSize > 0);
	assert(dataLastPoint >= miniPageEnd);

	// �ƶ�ҳ������
	shiftDataByMOVE((byte*)this + miniPageStart, (byte*)this + miniPageEnd, dataLastPoint - miniPageEnd);
	// ɾ��MiniPageĿ¼
	deleteMiniPageDir(miniPageNo);
	// ���º���MiniPageĿ¼
	adjustMiniPageDataOffset(miniPageNo, 0 - deleteMiniPageSize);

	// ���ҳ����Ϣ
	m_freeSpace = m_freeSpace + deleteMiniPageSize;
	m_pageCount--;
	m_nChance > 0 ? --m_nChance : m_nChance;

	memset((byte*)this + getDataEndOffset(), 0, deleteMiniPageSize);
}



/**
 * ɾ��ָ��MiniPageĿ¼
 *
 * @param miniPageNo	��Ŀ¼��
 */
void IndexPage::deleteMiniPageDir(u16 miniPageNo) {
	ftrace(ts.idx, tout << miniPageNo);
	assert(miniPageNo < m_miniPageNum);

	if (miniPageNo != 0)
		shiftDirByMOVE(getMiniPageDirStart(1), getMiniPageDirStart(0), MINI_PAGE_DIR_SIZE * miniPageNo);
	memset((byte*)this + m_dirStart, 0, MINI_PAGE_DIR_SIZE);

	m_freeSpace += MINI_PAGE_DIR_SIZE;
	m_dirStart += MINI_PAGE_DIR_SIZE;
	--m_miniPageNum;
}



/**
 * ����һ��MiniPageĿ¼��ʾ��ƫ��������ĳ��Ŀ¼��ʼ��ҳ�����һ��Ŀ¼����
 * �����ƫ�������ڲ�����ʼ�����ӷ�
 *
 * @param startMiniPageNo	Ҫ��������ʼMiniPageĿ¼
 * @param deltaOffset		Ҫ������ƫ����
 */
void IndexPage::adjustMiniPageDataOffset(u16 startMiniPageNo, s32 deltaOffset) {
	if (m_miniPageNum < startMiniPageNo + 1)
		return;

	for (u16 no = startMiniPageNo; no < m_miniPageNum; no++)
		adjustMiniPageDataStart(no, deltaOffset);
}



/**
 * ��ָ����ҳ��ϲ�����ҳ�棬ָ��ҳ���߼�˳���ڱ�ҳ��֮ǰ������ϲ�ҳ��ֻ��Ҫ�򵥵Ľ�ǰ��ҳ���MiniPageȫ���Ƶ���ǰҳ����ǰ�棬�����ϲ��Ż�
 *
 * @pre	������Ӧ�����ȱ�֤����ҳ����Ժϲ�����ռ����
 * @post ����SMO������m_smoLSN�ᱻ����
 * @param session		�Ự���
 * @param logger		��־��¼��
 * @param leftPage		Ҫ�ϲ���ҳ��
 * @param pageId		��ǰҳID
 * @param leftPageId	Ҫ�ϲ�ҳ��ID
 */
void IndexPage::mergePage(Session *session, IndexLog *logger, IndexPage *leftPage, PageId pageId, PageId leftPageId) {
	ftrace(ts.idx, tout << this << pageId << leftPage << leftPageId);
	assert(m_pageMark == leftPage->m_pageMark);
	assert((leftPage->m_pageType & m_pageType) != 0);
	assert(leftPage->m_pageLevel == m_pageLevel);

	u16 newStart = leftPage->getDataEndOffset();
	u16	dataSize = getDataEndOffset() - INDEXPAGE_DATA_START_OFFSET;
	u16 prevDataSize = leftPage->getDataEndOffset() - INDEXPAGE_DATA_START_OFFSET;
	u16 prevDirSize = INDEX_PAGE_SIZE - leftPage->m_dirStart;
	assert(m_freeSpace >= prevDataSize + prevDirSize);

	// ��¼SMO�ϲ��߼���־
	u64 lsn = logger->logSMOMerge(session, IDX_LOG_SMO_MERGE, pageId, leftPageId, leftPage->m_prevPage, (byte*)leftPage + INDEXPAGE_DATA_START_OFFSET, prevDataSize, (byte*)leftPage + leftPage->m_dirStart, prevDirSize, this->m_lsn, leftPage->m_lsn);
	m_lsn = lsn;
	leftPage->m_lsn = lsn;
	leftPage->m_smoLSN = lsn;

	// ��������ҳ������
	shiftDataByMOVE((byte*)this + newStart, (byte*)this + INDEXPAGE_DATA_START_OFFSET, dataSize);
	shiftDataByCPY((byte*)this + INDEXPAGE_DATA_START_OFFSET, (byte*)leftPage + INDEXPAGE_DATA_START_OFFSET, prevDataSize);
	shiftDirByCPY((byte*)this + m_dirStart - prevDirSize, (byte*)leftPage + leftPage->m_dirStart, prevDirSize);

	// �޸�ҳ��ͷ��Ϣ
	m_freeSpace -= prevDataSize + prevDirSize;
	m_pageCount = m_pageCount + leftPage->m_pageCount;
	m_miniPageNum = m_miniPageNum + leftPage->m_miniPageNum;
	m_dirStart = m_dirStart - prevDirSize;
	m_prevPage = leftPage->m_prevPage;
	m_smoLSN = m_lsn;
	assert(m_dirStart >= getDataEndOffset());

	adjustMiniPageDataOffset(leftPage->m_miniPageNum, prevDataSize);

	memset((byte*)leftPage + INDEXPAGE_DATA_START_OFFSET, 0, INDEX_PAGE_SIZE - INDEXPAGE_DATA_START_OFFSET);
}


/**
 * �ڸ�ҳ�浱�в���ָ����
 *
 * @param key			ҪѰ�ҵļ�¼��ֵ
 * @param comparator	������ֵ�Ƚ���
 * @param prev			OUT	���ڱ���Ѱ�Ҽ�¼��ǰ����û����m_size=0(prev<key)
 * @param next			OUT	���ڱ���Ѱ�Ҽ�¼�ĺ�̣�û����m_size=0(next>=key)
 * @param keyInfo		IN/OUT	����next��λ����Ϣ
 */
void IndexPage::findKeyInPageTwo(const SubRecord *key, KeyComparator *comparator, SubRecord **prev, SubRecord **next, KeyInfo *keyInfo) {
	assert(key != NULL);

	if (m_miniPageNum == 0) {	// ҳ��Ϊ��
		keyInfo->m_miniPageNo = 0;
		keyInfo->m_eOffset = keyInfo->m_sOffset = INDEXPAGE_DATA_START_OFFSET;
		keyInfo->m_result = 1;
		return;
	}

	s32 result;
	keyInfo->m_miniPageNo = findMiniPageInPage(key, &SearchFlag::DEFAULT_FLAG, comparator, *next, &result);
	assert(result >= 0 || keyInfo->m_miniPageNo == 0);

	findKeyInMiniPageTwo(key, keyInfo, comparator, prev, next);
	return;
}



/**
 * ��ָ����Mini Page����Ѱ��ָ���ļ�¼������ȡ�õļ�¼���ݣ���¼��ƫ����
 *
 * @param key			ҪѰ�ҵļ�¼��ֵ
 * @param keyInfo		IN/OUT	����next��λ����Ϣ
 * @param comparator	������ֵ�Ƚ���
 * @param prev			OUT	���ڱ���Ѱ�Ҽ�¼��ǰ����û����m_size=0(prev<key)
 * @param next			OUT	���ڱ���Ѱ�Ҽ�¼�ĺ�̣�û����m_size=0(next>=key)
 */
void IndexPage::findKeyInMiniPageTwo(const SubRecord *key, KeyInfo *keyInfo, KeyComparator *comparator, SubRecord **prev, SubRecord **next) {
	assert(keyInfo->m_miniPageNo < m_miniPageNum);

	(*prev)->m_size = (*next)->m_size = 0;

	s32 res;
	u16 count = 0;
	u16 pos = getMiniPageDataOffset(keyInfo->m_miniPageNo);
	u16 lastpos = pos;
	u16 end = (keyInfo->m_miniPageNo < m_miniPageNum - 1) ? getMiniPageDataOffset(keyInfo->m_miniPageNo + 1) : getDataEndOffset();
	while (pos < end) {
		IndexKey::swapKey(prev, next);

		lastpos = pos;
		IndexKey *indexKey = (IndexKey*)((byte*)this + lastpos);
		pos = pos + indexKey->extractKey((*prev)->m_data, *next, !isPageLeaf());
		res = compareKey(key, *next, &SearchFlag::DEFAULT_FLAG, comparator);
		if (res < 0)	// �ҵ���Ȼ��߸�������ʾ���ҳɹ�
			goto Finish;

		count++;
	}

	// ��Mini Page����������������Ƚϴ󣬲����ں���
	res = 1;
	IndexKey::swapKey(prev, next);
	(*next)->m_size = 0;
	lastpos = pos;

Finish:
	keyInfo->m_sOffset = lastpos;
	keyInfo->m_eOffset = pos;
	keyInfo->m_keyCount = count;
	keyInfo->m_result = res;

	assert(res != 0);
}




/**
 * ��ҳ�����������
 * @pre �����insertKeyӦ�úͱȽ����ȽϺ���ƥ�䣬�����������Ż����ң����insertKey�Ѿ���ѹ����ʽ��cpInsertKey����ΪNULL
 *
 * @param session		�Ự���
 * @param logger		��־��¼��
 * @param pageId		ҳ��ID
 * @param insertKey		Ҫ����ļ�ֵ
 * @param cpInsertKey	ѹ����ʽ�Ĳ����ֵ�����insertKey����ѹ����ʽ�ģ���ֵΪNULL
 * @param idxKey1		�䵱��ʱ�����ļ�ֵ����ʾ�������ǰ��
 * @param idxKey2		�䵱��ʱ�����ļ�ֵ����ʾ������ĺ���
 * @param comparator	������ֵ�Ƚ���
 * @param keyInfo		IN/OUT ����֮�����keyInfo->m_sOffset�����Ƿ���Ҫ���Ҳ����ֵλ��,
						���ز����ֵλ����Ϣ�������Ƿ����ɹ���keyInfo->m_sOffset���뱻����
 * @return ��ֵ����Ľ�����������INSERT_SUCCESS����ʾ����ɹ�������NEED_SPLIT��ʾ��Ҫ����ҳ��û�иı�
 */
IndexInsertResult IndexPage::addIndexKey(Session *session, IndexLog *logger, PageId pageId, const SubRecord *insertKey, const SubRecord *cpInsertKey, SubRecord *idxKey1, SubRecord *idxKey2, KeyComparator *comparator, KeyInfo *keyInfo) {
	vecode(vs.idx, this->traversalAndVerify(NULL, NULL, NULL, NULL, NULL));

	if (keyInfo->m_sOffset == 0) {	// �����Ϊ0��ʾ��ǰ��ֵ�Ѿ����ҹ���
		idxKey1->m_size = idxKey2->m_size = 0;
		findKeyInPageTwo(insertKey, comparator, &idxKey1, &idxKey2, keyInfo);
	}
	u16 sOffset = keyInfo->m_sOffset, eOffset = keyInfo->m_eOffset, miniPageNo = keyInfo->m_miniPageNo;
	u16 lowerCount = keyInfo->m_keyCount;

	u16 commonPrefix1 = 0, commonPrefix2 = 0;
	u16 newMiniPageNo = (u16)-1;
	u16 offset1 = (u16)-1;
	bool needCheck = false;
	bool addNewMiniPage = false;
	bool metMPEnd = !IndexKey::isKeyValid(idxKey2);
	u16 nextMPNo = miniPageNo + 1;

	if (eOffset == sOffset) {
		needCheck = true;
		if (nextMPNo < m_miniPageNum) {
			// �ٽ�״���������жϲ������ĸ�MiniPage����ǰ;
			assert(!IndexKey::isKeyValid(idxKey2) && keyInfo->m_result == 1);
			offset1 = getNextKey(idxKey2, eOffset, true, &nextMPNo, idxKey2);
		}
	}

	const SubRecord *compressKey = cpInsertKey == NULL ? insertKey : cpInsertKey;	// ʹ��ѹ����ʽ����ռ�Ͳ���

	{	// �þ���β��������Ż�
		commonPrefix1 = IndexKey::computePrefix(compressKey, idxKey1);
		commonPrefix2 = IndexKey::computePrefix(compressKey, idxKey2);

		// ���ݺ�ǰ�����ǰ׺ѹ�������������������ѹ����ʽ
		if (commonPrefix1 == 0 && commonPrefix2 == 0) {
			addNewMiniPage = true;
			if (IndexKey::isKeyValid(idxKey1) && !metMPEnd) {	// ��Ҫ���ѵ�ǰMiniPage���������ٶ��Դ����µ�MiniPage
				if (splitMiniPageReal(session, logger, pageId, idxKey2, sOffset, eOffset, miniPageNo, lowerCount) == NEED_SPLIT)
					return NEED_SPLIT;
				newMiniPageNo = nextMPNo;
			} else
				newMiniPageNo = (metMPEnd && m_miniPageNum != 0) ? nextMPNo : miniPageNo;	// ȷ�������MiniPage����ţ�ע���ҳ������
		} else if (needCheck) {
			if (commonPrefix1 < commonPrefix2 && !isMiniPageFull(nextMPNo)) {
				// ���ͺ�һ��MiniPageѹ��Ч�����ã����Һ�һ��MP������ѡ���������һ��
				eOffset = offset1;
				miniPageNo = nextMPNo;
				idxKey1->m_size = 0;
				commonPrefix1 = 0;
			} else if (commonPrefix1 == 0) {	// ��ǰ���ѹ�����Һ�һ��MP������ѡ�񵥶�����һ��MP
				addNewMiniPage = true;
				newMiniPageNo = nextMPNo;
			} else {	// ��ǰ�����ѹ����Ҫ���ڵ�ǰMP��������Ч
				idxKey2->m_size = 0;
			}
		}
	}

	if (addNewMiniPage) {	// ����ļ�ֵ�޷�ѹ����������MiniPage
		u16 needSpace = IndexKey::computeSpace(compressKey, (u16)0, !isPageLeaf()) + MINI_PAGE_DIR_SIZE;
		if (!isPageEnough(needSpace))
			return NEED_SPLIT;

		addMiniPage(session, logger, pageId, compressKey, newMiniPageNo);
		keyInfo->m_miniPageNo = newMiniPageNo;
		keyInfo->m_eOffset = keyInfo->m_sOffset + needSpace - MINI_PAGE_DIR_SIZE;

		vecode(vs.idx, this->traversalAndVerify(NULL, NULL, NULL, NULL, NULL));

		return INSERT_SUCCESS;
	}

	if (isMiniPageFull(miniPageNo)) {		// ��ǰMiniPage���������Է���
		if (splitMiniPageFull(session, logger, pageId, idxKey1, idxKey2, miniPageNo) == NEED_SPLIT)
			return NEED_SPLIT;

		vecode(vs.idx, this->traversalAndVerify(NULL, NULL, NULL, NULL, NULL));

		keyInfo->m_sOffset = 0;
		return addIndexKey(session, logger, pageId, insertKey, cpInsertKey, idxKey1, idxKey2, comparator, keyInfo);	// ����ֻ���ܱ�����һ�Σ������ܳ������޵ݹ�����
	}

	// ��������ռ䣬����eOffset��sOffset�ֱ��ʾ����Ľ�������ʼλ�ã����������ڣ��������
	bool isPageIdExist = !isPageLeaf();
	u16 needSpace = IndexKey::computeSpace(compressKey, commonPrefix1, isPageIdExist);
	keyInfo->m_eOffset = keyInfo->m_sOffset + needSpace;
	if (IndexKey::isKeyValid(idxKey2))
		needSpace = needSpace + IndexKey::computeSpace(idxKey2, commonPrefix2, isPageIdExist);
	assert(needSpace > (eOffset - sOffset));
	needSpace -= (eOffset - sOffset);
	if (m_freeSpace < needSpace)	// �ռ䲻����ҪSMO
		return NEED_SPLIT;

	// ��֤���Բ���֮�����ȸ��²���ƫ�ưٷֱ�
	if (m_pageCount != 0)
		updateInsertOffsetRatio(sOffset);
	byte oldStatus[INDEX_PAGE_SIZE];
	memcpy(oldStatus, (byte*)this + sOffset, eOffset - sOffset);
	// �ƶ������ڳ��ռ����
	byte *start = (byte*)this + sOffset;
	shiftDataByMOVE(start + needSpace, start, getDataEndOffset() - sOffset);

	// ��������
	IndexKey *indexKey = (IndexKey*)start;
	indexKey = (IndexKey*)indexKey->compressAndSaveKey(compressKey, commonPrefix1, isPageIdExist);
	if (IndexKey::isKeyValid(idxKey2))
		indexKey->compressAndSaveKey(idxKey2, commonPrefix2, isPageIdExist);

	// ��¼�߼�������־
#ifdef NTSE_TRACE
	u64 oldLSN = m_lsn;
#endif
	m_lsn = logger->logDMLUpdate(session, IDX_LOG_INSERT, pageId, sOffset, miniPageNo, oldStatus, eOffset - sOffset, start, needSpace + (eOffset - sOffset), m_lsn);
	ftrace(ts.idx, tout << session->getId() << ": " << "Insert key: " << pageId << rid(compressKey->m_rowId) << (isPageLeaf() ? 0 : IndexKey::getPageId(compressKey)) << " oldLSN: " << oldLSN << " newLSN: " << m_lsn);

	m_freeSpace = m_freeSpace - needSpace;
	++m_pageCount;
	incMiniPageItemCount(miniPageNo);
	adjustMiniPageDataOffset(miniPageNo + 1, needSpace);

	vecode(vs.idx, this->traversalAndVerify(NULL, NULL, NULL, NULL, NULL));

	return INSERT_SUCCESS;
}


/**
 * ��ʼ����ҳ�沢����ָ��ҳ�棬���ѽ����ʹ�ñ�ҳ��ĺ���ҳ������ݴ�ŵ�ָ����ҳ�浱��
 * ǰ����ʹ���߳��б�ҳ�����ҳ���X-Latch
 *
 * ����ķ�����Ҫ�������ܷ��ѵķ�ʽ�����ܷ���ϵ������ϲ�ָ���˾Ͱ����ϲ�ָ���ģ�
 * ����Ͱ���������ҳ������һ��ƽ��ƫ��������������ϵ��ֱ�Ӿ���ҳ����ѵ����ʼλ��
 * ��λ�ڷ��ѵ�֮������ݽ��ᱻ�ƶ����ұߵ�ҳ��
 *
 * @post �ر�ע�⣬newPage������ᱻ��ʼ�����ⲿ�κγ�ʼ�����ᱻ����
 * @post ����SMO������m_smoLSN�ᱻ����
 * @param session		�Ự���
 * @param logger		��־��¼��
 * @param newPage		������ѵ���ҳ��
 * @param pageId		��ҳ���ID��
 * @param newPageId		��ҳ��ID��
 * @param splitFactor	ָ���ķ���ϵ���������ϵ������0�������������������ܷ���ϵ��
 * @param insertPos		����֮������
 * @param reserveSize	����֮������֮����뱣֤����˶�Ŀ��пռ�
 * @param splitKey		���ѱ���ʹ�õ���ʱ��ֵ
 */
void IndexPage::splitPage(Session *session, IndexLog *logger, IndexPage *newPage, PageId pageId, PageId newPageId, u8 splitFactor, u16 insertPos, u16 reserveSize, SubRecord *splitKey) {
	ftrace(ts.idx, tout << this << pageId << newPageId << newPageId << splitFactor << insertPos << reserveSize);
	assert(newPage != NULL);
	assert(m_pageCount > 1);

	// ��ʼ����ҳ��
	newPage->initPage(session, logger, true, false, newPageId, m_pageMark, (IndexPageType)m_pageType, m_pageLevel, pageId, m_nextPage);

	// ������ʵķ��ѵ�
	u16 splitMPNo;
	u8 mpLeftCount;
	u16 splitOffset = getSplitOffset((splitFactor == 0 ? m_insertOffsetRatio : splitFactor * 100), insertPos, reserveSize, splitKey, &splitMPNo, &mpLeftCount);

	// ������������Ϣ
	u8 mpMoveCount = getMiniPageItemCount(splitMPNo) - mpLeftCount;
	u16 leftMPNum = splitMPNo + (mpLeftCount > 0 ? 1 : 0);
	u16 moveMPNum = m_miniPageNum - splitMPNo - (mpMoveCount > 0 ? 0 : 1);
	u16 splitKeyCLen = ((IndexKey*)((byte*)this + splitOffset))->skipAKey();
	u16 moveData = splitOffset + splitKeyCLen;
	u16 dataSize = getDataEndOffset() - moveData;
	u16 moveDirSize = moveMPNum * MINI_PAGE_DIR_SIZE;

	byte oldKey[INDEX_PAGE_SIZE];
	memcpy(&oldKey, (IndexKey*)((byte*)this + splitOffset), splitKeyCLen);

	// �ƶ����ݺ���Ŀ¼
	IndexKey *indexKey = (IndexKey*)((byte*)newPage + INDEXPAGE_DATA_START_OFFSET);
	u16 offset = (u16)(indexKey->compressAndSaveKey(splitKey, (u16)0, !isPageLeaf()) - ((byte*)newPage + INDEXPAGE_DATA_START_OFFSET));
	shiftDataByCPY((byte*)newPage + INDEXPAGE_DATA_START_OFFSET + offset, (byte*)this + moveData, dataSize);
	newPage->m_dirStart = INDEX_PAGE_SIZE - MINI_PAGE_DIR_SIZE * moveMPNum;
	shiftDirByCPY((byte*)newPage + newPage->m_dirStart, (byte*)this + newPage->m_dirStart, MINI_PAGE_DIR_SIZE * moveMPNum);

	// ������ҳ�����Ŀ¼
	newPage->m_miniPageNum = moveMPNum;
	newPage->setMiniPageDataStart(0, INDEXPAGE_DATA_START_OFFSET);
	if (newPage->m_miniPageNum > 1) {
		s32 diff = offset - splitKeyCLen - (splitOffset - INDEXPAGE_DATA_START_OFFSET);
		newPage->adjustMiniPageDataOffset(1, diff);
	}

	// ��¼SMO�߼���־
	u64 lsn = logger->logSMOSplit(session, IDX_LOG_SMO_SPLIT, pageId, newPageId, m_nextPage, (byte*)this + moveData, dataSize, oldKey, splitKeyCLen, (byte*)newPage + INDEXPAGE_DATA_START_OFFSET, offset, (byte*)newPage + newPage->m_dirStart, moveDirSize, mpLeftCount, mpMoveCount, m_lsn, newPage->m_lsn);
	m_lsn = lsn;
	newPage->m_lsn = lsn;

	// ����ԭʼҳ�����ݺ���Ŀ¼
	memset((byte*)this + splitOffset, 0, getDataEndOffset() - splitOffset);
	u16 leftDirOffset = INDEX_PAGE_SIZE - MINI_PAGE_DIR_SIZE * leftMPNum;
	shiftDirByMOVE((byte*)this + leftDirOffset, (byte*)this + m_dirStart, INDEX_PAGE_SIZE - leftDirOffset);
	memset((byte*)this + m_dirStart, 0, leftDirOffset - m_dirStart);
	m_dirStart = leftDirOffset;

	// ��������ҳ���������
	if (mpLeftCount > 0)
		setMiniPageItemCount(leftMPNum - 1, mpLeftCount);
	if (mpMoveCount > 0)
		newPage->setMiniPageItemCount(0, mpMoveCount);
	m_miniPageNum = leftMPNum;
	u16 leftCounts = getMPsItemCounts(m_miniPageNum);

	newPage->m_pageCount = m_pageCount - leftCounts;
	newPage->m_freeSpace = newPage->m_dirStart - (offset + dataSize) - INDEXPAGE_DATA_START_OFFSET;
	newPage->m_nChance = 2;

	m_insertOffsetRatio = newPage->m_insertOffsetRatio = 5000;
	m_smoLSN = newPage->m_smoLSN = m_lsn;
	m_pageCount = leftCounts;
	m_freeSpace = m_dirStart - splitOffset;
	m_nextPage = newPageId;

	vecode(vs.idx, this->traversalAndVerify(NULL, NULL, NULL, NULL, NULL));
	vecode(vs.idx, newPage->traversalAndVerify(NULL, NULL, NULL, NULL, NULL));
}


/**
 * ����ҳ�濪ʼ��ĳ��MiniPage֮ǰ��������(������ָ����MP)������MiniPageĿ¼����Ϣ����
 * @param Ҫͳ�Ƶ����miniPageNo��
 * @return ҳ���������������
 */
u16 IndexPage::getMPsItemCounts(u16 miniPageNo) {
	assert(m_miniPageNum >= miniPageNo);
	u16 keyCounts = 0;

	for (u16 no = 0; no < miniPageNo; no++)
		keyCounts = keyCounts + getMiniPageItemCount(no);

	return keyCounts;
}


/**
 * ���ݲ�������ҳ����ĸ�ƫ�Ƶ㿪ʼ����
 * @param splitFactor	����ϵ��
 * @param insertPos		���·��ѵĲ������ݲ����
 * @param reserveSize	�����󼴽������������
 * @param splitKey		������ʱ��ֵ������ѵ��һ����¼������
 * @param miniPageNo	out ���ѵ����ڵ�MiniPage��
 * @param mpLeftCount	out ���ѵ�MiniPage���ڱ�ҳ�������
 * @return ���ѵ��ҳ��ƫ��
 */
u16 IndexPage::getSplitOffset(u16 splitFactor, u16 insertPos, u16 reserveSize, SubRecord *splitKey, u16 *miniPageNo, u8 *mpLeftCount) {
	u16 splitOffset = 0;
	// ������·��ѵ�
	if (splitFactor > IndexPage::INDEXPAGE_MAX_SPLIT_FACTOR)
		splitFactor = IndexPage::INDEXPAGE_MAX_SPLIT_FACTOR;
	if (splitFactor < IndexPage::INDEXPAGE_MIN_SPLIT_FACTOR)
		splitFactor = IndexPage::INDEXPAGE_MIN_SPLIT_FACTOR;
	s16 overRecord = 0;
	u16 expectOffset = (u16)((m_dirStart - INDEXPAGE_DATA_START_OFFSET) * (splitFactor / 100.0) / 100) + INDEXPAGE_DATA_START_OFFSET;
	if (expectOffset >= getDataEndOffset())	// ����Ҫ��֤ԭҳ��϶����������ƶ�����ҳ��
		expectOffset = getDataEndOffset() - 1;

	// �����ϸ�ļ���ʣ��ռ䣬��Ŀ¼�ռ䱻�����ǣ���˿��ܳ��ּ��㲻����ȷ�����
	// ��յ�Ӧ�������Ƽ�ֵ��������֤������ֿռ䲻�������
	// ʹ��expectOffset��overRecord���Ͽ�����ʵ���ѵ�λ�ã�
	// expectOffset��ʾ��λ�ÿ��ܴ��κ�һ����¼��㵽�����յ㣬
	// overRecord���-1����ʾ��������¼��ͷ��ʼ���ѣ�
	// overRecord���1����ʾ��β��ʼ���ѣ������0����ʾӦ������
	// ͬʱ������Σ�����Ҫ�ƶ�1���ҳ��
	u16 indexPageInitFree = INDEX_PAGE_SIZE - INDEXPAGE_INIT_MAX_USAGE;
	if (expectOffset > insertPos && m_dirStart - expectOffset < reserveSize + indexPageInitFree) {	// ��������Ҫ���ƶ�����ҳ�棬����������Ҫ�Ѽ�ֵ���뵽��ҳ��
		if (m_dirStart - reserveSize - indexPageInitFree < insertPos) {	// ��������Ҫ�ƶ�����ҳ�棬ֻ�ƶ������֮������ݵ���ҳ�棬��֤�����֮ǰ������������reserveSize
			assert(insertPos - INDEXPAGE_DATA_START_OFFSET > reserveSize);
			// ���ѵ���ǲ����
			expectOffset = insertPos;
			overRecord = 0;
		} else {
			// �ƶ�m_dirStart - reserveSize - INDEXPAGE_INIT_MAX_USAGE������ݵ���ҳ�棬ͬʱ����㻹������ԭ��ҳ�棬������ԭ��ҳ�����
			expectOffset = m_dirStart - reserveSize - indexPageInitFree;
			overRecord = -1;
			// ����֮��insertPos���ܴ��ڵ���expectOffset, ��ʱ����뵽��ҳ�棬overRecord�Ծ���-1�Ŷ�
		}
	} else if (expectOffset <= insertPos && expectOffset - INDEXPAGE_DATA_START_OFFSET + m_freeSpace < reserveSize + indexPageInitFree) {	// ��������Ҫ���ƶ�һЩ�����ܵ��²����ھ�ҳ��
		if (reserveSize + indexPageInitFree - m_freeSpace + INDEXPAGE_DATA_START_OFFSET < insertPos) {
			// ����㻹�ǿ����ƶ�����ҳ�棬�ƶ��ĵ�����reserveSize + INDEXPAGE_INIT_MAX_USAGE - m_freeSpace + INDEXPAGE_DATA_START_OFFSET;
			expectOffset = reserveSize + indexPageInitFree - m_freeSpace + INDEXPAGE_DATA_START_OFFSET;
			overRecord = 1;
		} else {
			// ����ĵ�Ҫ������ԭʼҳ�棬���ѵ�Ӳ�����һ����¼��ʼ��ͬʱ��Ҫ��֤ԭʼҳ���㹻����
			assert(m_dirStart - insertPos + 1 > reserveSize);
			overRecord = 1;
			expectOffset = insertPos + 1;
		}
	}
	assert(expectOffset < getDataEndOffset());

	// ����MiniPage������ѵ����ڵ�ҳ
	u16 splitMPNo = 0;
	for (; splitMPNo < m_miniPageNum - 1; ++splitMPNo) {
		if (getMiniPageDataOffset(splitMPNo + 1) > expectOffset)
			break;
	}
	// ˳������õ��������ѵļ�ֵ��ʼλ�ò��õ���ֵ
	u8 leftMPCount = 0;
	u16 start = getMiniPageDataOffset(splitMPNo);
	u16 end = (splitMPNo == m_miniPageNum - 1) ? getDataEndOffset() : getMiniPageDataOffset(splitMPNo + 1);
	// ���ȱ�����������ԽexpectOffset�ļ�ֵ
	while (start < end) {
		splitOffset = start;
		IndexKey *indexKey = (IndexKey*)((byte*)this + start);
		start = start + indexKey->extractKey(splitKey->m_data, splitKey, !isPageLeaf());
		if (start >= expectOffset)	// �����>=��ʾ���expectOffset�պ�ʱ������ֵ�����ӵ㣬�������ں�һ����ֵ
			break;
		++leftMPCount;	// leftMPCountֻ���㵽����Ҫ��overRecord�������ļ�ֵ����
	}

	// ���ȸ���splitOffset����overRead������splitOffset��ʾ���ǿ�expectOffset��¼����ʼλ�ã�start��ʾ���ǽ���λ��
	if (splitOffset > insertPos) {	// Ԥ������ԭ��ҳ�����
		if (m_dirStart - splitOffset < reserveSize + indexPageInitFree)
			overRecord = -1;
	} else {	// ��������ҳ��
		if (splitOffset - INDEXPAGE_DATA_START_OFFSET + m_freeSpace < reserveSize + indexPageInitFree)
			overRecord = 1;
		if (splitOffset == insertPos && m_dirStart - start < reserveSize + indexPageInitFree)	// ��һ���ٽ�״̬���жϲ���������ĸ�ҳ��ȽϺ���
			overRecord = 0;
	}

	// �ٸ���overRecord������
	if (overRecord == 1 && start < getDataEndOffset()) {	// ȡ��һ��
		if (start == end) {	// ȡ��һ��MiniPage�����Ҳ�������ҳ��β
			leftMPCount = 0;
			splitMPNo++;
		} else
			leftMPCount++;
		splitOffset = start;
		IndexKey *indexKey = (IndexKey*)((byte*)this + start);
		indexKey->extractKey(splitKey->m_data, splitKey, !isPageLeaf());
	}

	// TODO:ȷ������ռ��㹻������ĳЩ����ö��Կ��ܻ�ʧ�ܣ����ǲ�Ӧ��Ӱ�������ȷ��
	assert(splitOffset < getDataEndOffset());
	assert((insertPos < splitOffset && m_dirStart - splitOffset >= reserveSize + indexPageInitFree) ||
			(insertPos >= splitOffset && splitOffset - INDEXPAGE_DATA_START_OFFSET + m_freeSpace >= reserveSize + indexPageInitFree));

	*miniPageNo = splitMPNo;
	*mpLeftCount = leftMPCount;

	return splitOffset;
}



/**
 * ��Ҫ����ļ�ֵ��Ϊ������MiniPage���룬�����µ�MiniPage
 *
 * @pre	�����߱��뱣֤ҳ��ռ��㹻
 * @param session		�Ự���
 * @param logger		��־��¼��
 * @param pageId		ҳ��ID
 * @param insertKey		Ҫ�����������ֵ
 * @param newMiniPageNo	���ɵ���MiniPage�ı��
 */
void IndexPage::addMiniPage(Session *session, IndexLog *logger, PageId pageId, const SubRecord *insertKey, u16 newMiniPageNo) {
	assert(newMiniPageNo <= m_miniPageNum);

	u16 start = newMiniPageNo == 0 ? INDEXPAGE_DATA_START_OFFSET :
		newMiniPageNo < m_miniPageNum ? getMiniPageDataOffset(newMiniPageNo) :
		getDataEndOffset();

	// ������Ŀ¼��Ϣ
	addMiniPageDir(newMiniPageNo, start, 1);
	// �����ֵ
	u16 needSpace = addFirstKeyInMiniPage(insertKey, start);
	assert(m_freeSpace >= needSpace);

	// ��¼����MiniPage�߼���־
#ifdef NTSE_TRACE
	u64 oldLSN = m_lsn;
#endif
	m_lsn = logger->logPageAddMP(session, IDX_LOG_ADD_MP, pageId, (byte*)this + start, needSpace, newMiniPageNo, m_lsn);
	ftrace(ts.idx, tout << "AddMP: pageId: " << pageId << " MPNo: " << newMiniPageNo << " oldLSN: " << oldLSN << " newLSN: " << m_lsn);

	// ����Ӱ�쵽����Ŀ¼��Ϣ��ҳ����Ϣ
	if (m_pageCount != 0)
		updateInsertOffsetRatio(start);
	adjustMiniPageDataOffset(newMiniPageNo + 1, needSpace);
	m_freeSpace = m_freeSpace - needSpace;
	m_pageCount++;
}


/**
 * ��ҳ��������һ��MiniPage��Ŀ¼��Ϣ
 *
 * @post ҳ��m_dirStart/m_miniPageNum/m_freeSpace���޸ģ�ͬʱMiniPage��Ŀ¼���ݱ���λ
 * @param newMiniPageNo	�µ�MiniPage��
 * @param dataStart		��Ŀ¼ָ�����ݵ�ַ��ʼƫ��
 * @param itemCounts	MiniPage��������
 */
void IndexPage::addMiniPageDir(u16 newMiniPageNo, u16 dataStart, u16 itemCounts) {
	ftrace(ts.idx, tout << newMiniPageNo << dataStart << itemCounts);
	assert(itemCounts != 0);
	assert(newMiniPageNo <= m_miniPageNum);
	// �����µ���Ŀ¼
	byte *miniPageStart = (byte*)this + m_dirStart;
	byte *miniPageNewStart = miniPageStart - MINI_PAGE_DIR_SIZE;
	shiftDataByMOVE(miniPageNewStart, miniPageStart, MINI_PAGE_DIR_SIZE * (newMiniPageNo));
	// �޸�������Ŀ¼����Ϣ
	m_dirStart -= MINI_PAGE_DIR_SIZE;
	m_miniPageNum++;
	m_freeSpace -= MINI_PAGE_DIR_SIZE;
	// ��ʼ����Ŀ¼
	setMiniPageDataStart(newMiniPageNo, dataStart);
	setMiniPageItemCount(newMiniPageNo, itemCounts);
}


/**
 * ��ָ����Ŀ¼�����һ����ֵ
 *
 * @param insertKey	����ļ�ֵ
 * @param dataStart	�������ݵ����
 * @return �����ֵռ�õĿռ�
 */
u16 IndexPage::addFirstKeyInMiniPage(const SubRecord *insertKey, u16 dataStart) {
	bool isPageIdExists = !isPageLeaf();
	byte *pos = (byte*)this + dataStart;

	u16 needSpace = IndexKey::computeSpace(NULL, insertKey, isPageIdExists);
	u16 moveSize = getDataEndOffset() - dataStart;

	shiftDataByMOVE(pos + needSpace, pos, moveSize);
	IndexKey *indexKey = (IndexKey*)pos;
	NTSE_ASSERT(indexKey->compressAndSaveKey(NULL, insertKey, isPageIdExists) < (byte*)this + INDEX_PAGE_SIZE);

	return needSpace;
}


/**
 * ����ָ��MiniPage
 *
 * @pre	����MiniPage����������ﵽ������Ʋŵ��ø÷��Ѻ���
 * @param session		�Ự���
 * @param logger		��־��¼��
 * @param pageId		ҳ��ID
 * @param idxKey1		��ʱ��ֵ
 * @param idxKey2		��ʱ��ֵ
 * @param miniPageNo	Ҫ���ѵ�MiniPage��
 * @return ���ط��ѳɹ����߷��Ѻ�ռ䲻����Ҫ��������ҳ�棺INSERT_SUCCESS/NEED_SPLIT
 */
IndexInsertResult IndexPage::splitMiniPageFull(Session *session, IndexLog *logger, PageId pageId, SubRecord *idxKey1, SubRecord *idxKey2, u16 miniPageNo) {
	ftrace(ts.idx, tout << pageId << miniPageNo);
	assert(getMiniPageItemCount(miniPageNo) >= 2);

	// ��Ҫ����MiniPage������ҳ�棬��ҳ���м������
	u8 leftItem;
	if (m_insertOffsetRatio >= 80)
		leftItem = MINI_PAGE_MAX_INIT_ITEMS;
	else if (m_insertOffsetRatio <= 20)
		leftItem = 2;
	else
		leftItem = getMiniPageItemCount(miniPageNo) / 2;
	u16 offset1 = getMiniPageDataOffset(miniPageNo);
	for (u16 i = 0; i < leftItem; i++)
		offset1 = getNextKey(idxKey1, offset1, false, &miniPageNo, idxKey1);
	u16 offset2 = getNextKey(idxKey1, offset1, false, &miniPageNo, idxKey2);

	return splitMiniPageReal(session, logger, pageId, idxKey2, offset1, offset2, miniPageNo, leftItem);
}




/**
 * ��������MP���ѵĺ���
 *
 * @����ǰ�ϲ�Ӧ�ü����Ҫ���ѵ����һ����ֵ�Լ����ƫ����
 * @param session		�Ự���
 * @param logger		��־��¼��
 * @param pageId		ҳ��ID
 * @param next			���ѵ����һ����ֵ
 * @param sOffset		��ֵ����ʼƫ����
 * @param eOffset		��ֵ�Ľ���ƫ����
 * @param miniPageNo	Ҫ���ѵ�MP��
 * @param leftItem		��������MP����ļ�ֵ����
 * @return ���ط��ѳɹ�INSERT_SUCCESS���߷���ʧ��NEED_SPLIT
 */
IndexInsertResult IndexPage::splitMiniPageReal(Session *session, IndexLog *logger, PageId pageId, const SubRecord *next, u16 sOffset, u16 eOffset, u16 miniPageNo, u16 leftItem) {
	bool isPageIdExist = !isPageLeaf();
	u16 needSpace = IndexKey::computeSpace(NULL, next, isPageIdExist) - (eOffset - sOffset);
	if (needSpace + MINI_PAGE_DIR_SIZE > m_freeSpace)	// ʣ��ռ䲻������һ��MiniPage
		return NEED_SPLIT;

	u16 oldValueLen = eOffset - sOffset;
	byte *oldValue = (byte*)alloca(oldValueLen);
	shiftDataByCPY(oldValue, (byte*)this + sOffset, oldValueLen);

	// ���������µ�MiniPage
	addMiniPageDir(miniPageNo + 1, sOffset, getMiniPageItemCount(miniPageNo) - leftItem);

	// �޸�����MiniPage��Ϣ
	setMiniPageItemCount(miniPageNo, leftItem);
	adjustMiniPageDataOffset(miniPageNo + 2, needSpace);

	// �ƶ��������ݣ���ѹ������ѵ��ֵ
	byte *pos = (byte*)this + eOffset;
	shiftDataByMOVE((byte*)(pos + needSpace), pos, getDataEndOffset() - eOffset);

	IndexKey *indexKey = (IndexKey*)((byte*)this + sOffset);
	indexKey->compressAndSaveKey(next, (u16)0, isPageIdExist);

	assert(m_freeSpace >= needSpace);
	m_freeSpace = m_freeSpace - needSpace;
	m_splitedMPNo = miniPageNo;

	// ��¼MP�����߼���־
#ifdef NTSE_TRACE
	u64 oldLSN = m_lsn;
#endif
	m_lsn = logger->logSplitMP(session, IDX_LOG_SPLIT_MP, pageId, sOffset, oldValue, oldValueLen, (byte*)this + sOffset, needSpace + oldValueLen, leftItem, miniPageNo, m_lsn);
	ftrace(ts.idx, tout << "SplitMP: pageId: " << pageId << " MPNo: " << miniPageNo << " oldLSN: " << oldLSN << " newLSN: " << m_lsn);

	return INSERT_SUCCESS;
}


/** �ϲ�ָ����MiniPage�����ĺ�һ��MiniPage
 * @param session		�Ự���
 * @param logger		��־��¼��
 * @param pageId		ҳ��ID
 * @param miniPageNo	Ҫ�ϲ�����ʼMP��
 * @param prevKey		��������ϲ�֮ǰ��ǰһ��MP�����һ����ֵ
 * @param nextKey		��������ϲ�֮ǰ����һ��MP�ĵ�һ����ֵ
 * @return true��ʾ�ܳɹ��ϲ���false��ʾ�޷��ϲ�
 */
bool IndexPage::mergeTwoMiniPage( Session *session, IndexLog *logger, PageId pageId, u16 miniPageNo, SubRecord *prevKey, SubRecord *nextKey ) {
	u16 nextMPNo = miniPageNo + 1;
	u16 originalMPKeyCounts = getMiniPageItemCount(miniPageNo);
	u16 newMPItemCounts = originalMPKeyCounts + getMiniPageItemCount(nextMPNo);
	if (m_miniPageNum <= miniPageNo + 1 || newMPItemCounts > MINI_PAGE_MAX_ITEMS)
		// ��֤miniPageNoָ������ȷ�ԣ����벻Խ�磬���ںϲ��ĺ���
		// ָ��MiniPage�ϲ�֮�󣬱����������ܳ����涨�������
		return false;

	u16 nextMPOffset = getMiniPageDataOffset(nextMPNo);
	IndexKey *indexKey = (IndexKey*)((byte*)this + nextMPOffset);
	u16 nextKeyLen = indexKey->extractKey(NULL, nextKey, !isPageLeaf());
	getPrevKey(nextMPOffset, false, &miniPageNo, prevKey);

	u16 compressedSize = IndexKey::computeSpace(prevKey, nextKey, !isPageLeaf());
	assert(compressedSize < nextKeyLen);
	u16 savedSpace = nextKeyLen - compressedSize;

	// ��¼Ҫ�ϲ���ֵԭʼ��Ϣ
	byte* oldValue = (byte*)alloca(nextKeyLen);
	shiftDataByCPY(oldValue, (byte*)this + nextMPOffset, nextKeyLen);

	// ִ�кϲ�
	byte *end = indexKey->compressAndSaveKey(prevKey, nextKey, !isPageLeaf());
	shiftDataByMOVE(end, (byte*)this + nextMPOffset + nextKeyLen, getDataEndOffset() - nextMPOffset - nextKeyLen);
	clearData(getDataEndOffset() - savedSpace, m_dirStart - (getDataEndOffset() - savedSpace));
	// ��Ŀ¼�ϲ�����
	setMiniPageItemCount(miniPageNo, newMPItemCounts);
	deleteMiniPageDir(nextMPNo);
	adjustMiniPageDataOffset(miniPageNo + 1, 0 - savedSpace);

	m_freeSpace += savedSpace;
	m_splitedMPNo = (u16)-1;

	// ��¼��־
#ifdef NTSE_TRACE
	u64 oldLSN = m_lsn;
#endif
	m_lsn = logger->logMergeMP(session, IDX_LOG_MERGE_MP, pageId, nextMPOffset, (byte*)this + nextMPOffset, compressedSize, oldValue, nextKeyLen, miniPageNo, originalMPKeyCounts, m_lsn);
	ftrace(ts.idx, tout << "MergeMP: pageId: " << pageId << " MPNo: " << miniPageNo << " oldLSN: " << oldLSN << " newLSN: " << m_lsn);

	return true;
}

///**
// * ����ָ�����pageId��Ϣ
// * @param session	�Ự���
// * @param logger	��־��¼��
// * @param pageId	��ǰҳ��PageId
// * @param newPageId	�µ�ҳ��ID��Ϣ
// * @param keyInfo	Ҫ���¼�ֵ��λ����Ϣ
// */
//void IndexPage::updateSpecifiedPageId(Session *session, IndexLog *logger, PageId pageId, PageId newPageId, KeyInfo updateKeyInfo) {
//	assert(updateKeyInfo.m_eOffset < getDataEndOffset() && updateKeyInfo.m_miniPageNo < m_miniPageNum);
//	IndexKey *indexKey = (IndexKey*)((byte*)this + updateKeyInfo.m_sOffset);
//	u16 keyLen = updateKeyInfo.m_eOffset - updateKeyInfo.m_sOffset;
//	PageId oldPageId = indexKey->getCurPageId(keyLen);
//	u16 offset = indexKey->updatePageId(keyLen, newPageId);
//#ifdef NTSE_TRACE
//	u64 oldLSN = m_lsn;
//#endif
//	m_lsn = logger->logPageUpdate(session, IDX_LOG_UPDATE_PAGE, pageId, updateKeyInfo.m_sOffset + offset, (byte*)&newPageId, (byte*)&oldPageId, PID_BYTES, m_lsn);
//	nftrace(ts.idx, tout << "UpdatePageId: Page: " << pageId << " updatedId: " << newPageId << " Offset: " << updateKeyInfo.m_sOffset << " oldLSN: " << oldLSN << " newLSN: " << m_lsn);;
//}


/**
 * ��ҳ�����ҵ�ָ����PageId
 * @param findPageId	Ҫ���ҵ�pageId
 * @return ����ҵ�������true�����򷵻�false
 */
bool IndexPage::findSpecifiedPageId(PageId findPageId) {
	u16 offset = INDEXPAGE_DATA_START_OFFSET;
	PageId gotPageId = 0;
	while (gotPageId != INVALID_PAGE_ID) {
		offset = offset + this->getNextKeyPageId(offset, &gotPageId);
		if (gotPageId == findPageId)
			return true;
	}

	return false;
}


/**
 * �õ�ָ��λ�ÿ�ʼ��������ֵ��PageId��Ϣ
 *
 * @param offset	ȡ��׺����ʼƫ��
 * @param pageId	IN/OUT	ȡ�õ�PageId�������ǰ��ҳ��β��������INVALID_PAGE_ID
 * @return ��һ��Key�����ƫ��
 */
u16 IndexPage::getNextKeyPageId(u16 offset, PageId *pageId) {
	assert(offset >= INDEXPAGE_DATA_START_OFFSET);
	assert(m_pageLevel > 0);

	if (offset >= getDataEndOffset()) {
		*pageId = INVALID_PAGE_ID;
		return offset;
	}

	IndexKey *indexKey = (IndexKey*)((byte*)this + offset);
	return indexKey->extractNextPageId(pageId);
}


/**
 * ��ָ������뵽ҳ���β�����������������ģ����ڴ�������ʱʹ��
 *
 * @pre appendKey������lastKey��ֱ�Ӻ�����lastKeyΪNULL����ʾ��ҳ������һ��
 *
 * @param session	�Ự���
 * @param logger	��־��¼��
 * @param pageId	ҳ��ID
 * @param appendKey	Ҫ��ӵ���
 * @param lastKey	ҳ�浱ǰ���һ��
 * @return ��ֵ����Ľ�����������INSERT_SUCCESS����ʾ����ɹ�������NEED_SPLIT��ʾ��Ҫ����ҳ��û�иı�
 */
IndexInsertResult IndexPage::appendIndexKey(Session *session, IndexLog *logger, PageId pageId, const SubRecord *appendKey, const SubRecord *lastKey) {
	u16 miniPageNo = m_miniPageNum - 1;
	const SubRecord *prevKey = lastKey;
	bool isPageIdExists = !isPageLeaf();

	u16 commonPrefixLen = lastKey != NULL ? IndexKey::computePrefix(lastKey, appendKey) : 0;

	if (m_miniPageNum == 0 || isInitMiniPageFull(miniPageNo) || commonPrefixLen == 0) {
		// ��ǰ��MiniPage�ѳ�������ʼ�����ƻ���ҳ��Ϊ�գ���Ҫʹ���µ�MiniPage
		// �����ж�ҳ��ռ��Ƿ��㹻
		if (!isPageEnough(INDEX_PAGE_SIZE - INDEXPAGE_INIT_MAX_USAGE + MINI_PAGE_DIR_SIZE + IndexKey::computeSpace(NULL, appendKey, isPageIdExists)))
			return NEED_SPLIT;

		addMiniPage(session, logger, pageId, appendKey, m_miniPageNum);

		return INSERT_SUCCESS;
	}

	// �ж�ҳ��ռ��Ƿ��㹻
	u16 needSpace = IndexKey::computeSpace(appendKey, commonPrefixLen, isPageIdExists);
	if (!isPageEnough(INDEX_PAGE_SIZE - INDEXPAGE_INIT_MAX_USAGE + needSpace))	// �ռ䲻������Ҫ����
		return NEED_SPLIT;

	// �����ֵ
	u16 start = getDataEndOffset();
	IndexKey *indexKey = (IndexKey*)((byte*)this + start);
	indexKey->compressAndSaveKey(prevKey, appendKey, isPageIdExists);

	// ��¼append��־
	m_lsn = logger->logDMLUpdate(session, IDX_LOG_APPEND, pageId, start, miniPageNo, (byte*)this + start, 0, (byte*)this + start, needSpace, m_lsn);

	// �޸�ҳ�������Ϣ
	m_freeSpace = m_freeSpace - needSpace;
	m_pageCount++;
	incMiniPageItemCount(miniPageNo);

	return INSERT_SUCCESS;
}



/**
 * ����ҳ���޸Ĳ�����ֻ�漰��������޸ģ����漰ҳ�����MiniPage�ķ���ɾ���������ɵȶ���
 * �����redoֻ��Ҫ�򵥵��ƶ�ҳ�����ݣ����Ʋ���ֵ�����״̬����
 * @param type			��־����
 * @param offset		ҳ���޸���ʼƫ��
 * @param miniPageNo	�����漰��MiniPage
 * @param oldSize		ǰ�����ݵĳ���
 * @param newValue		֮����ָ��ƫ�ƴ�������
 * @param newSize		ǰ�����ݵĳ���
 */
void IndexPage::redoDMLUpdate(IDXLogType type, u16 offset, u16 miniPageNo, u16 oldSize, byte *newValue, u16 newSize) {
	vecode(vs.idx, traversalAndVerify(NULL, NULL, NULL, NULL, NULL));

	if (type == IDX_LOG_INSERT && m_pageCount != 0)
		updateInsertOffsetRatio(offset);

	s32 moveSize = getDataEndOffset() - offset - oldSize;
	assert(moveSize >= 0);
	shiftDataByMOVE((byte*)this + offset + newSize, (byte*)this + offset + oldSize, moveSize);
	memcpy((byte*)this + offset, newValue, newSize);
	// �޸�ҳ�������Ϣ
	assert(m_freeSpace + oldSize >= newSize);
	m_freeSpace = m_freeSpace - (newSize - oldSize);
	if (type == IDX_LOG_DELETE) {
		if (newSize < oldSize)	// ���ٵ���������
			memset((byte*)this + offset + newSize + moveSize, 0, oldSize - newSize);
		decMiniPageItemCount(miniPageNo);
		m_pageCount--;
		m_nChance > 0 ? --m_nChance : m_nChance;
	} else {	// IDX_INSERT/IDX_APPEND
		incMiniPageItemCount(miniPageNo);
		m_pageCount++;
	}

	adjustMiniPageDataOffset(miniPageNo + 1, newSize - oldSize);

	vecode(vs.idx, if (type != IDX_LOG_APPEND)
		traversalAndVerify(NULL, NULL, NULL, NULL, NULL)
	);
}


/**
 * redo����SMO�ϲ�����
 * @param prevPageId	�ϲ���ҳ���ǰһ��ҳ���ID
 * @param moveData		��ҳ���ƶ�����������
 * @param dataSize		��ҳ���ƶ������ݳ���
 * @param moveDir		��ҳ���ƶ���������Ŀ¼
 * @param dirSize		��ҳ���ƶ���Ŀ¼���ݳ���
 * @param smoLSN		ҳ��SMO֮���LSN
 */
void IndexPage::redoSMOMergeOrig(PageId prevPageId, byte *moveData, u16 dataSize, byte *moveDir, u16 dirSize, u64 smoLSN) {
	vecode(vs.idx, traversalAndVerify(NULL, NULL, NULL, NULL, NULL));

	u16 localDataSize = getDataEndOffset() - INDEXPAGE_DATA_START_OFFSET;
	shiftDataByMOVE((byte*)this + INDEXPAGE_DATA_START_OFFSET + dataSize, (byte*)this + INDEXPAGE_DATA_START_OFFSET, localDataSize);
	shiftDataByCPY((byte*)this + INDEXPAGE_DATA_START_OFFSET, moveData, dataSize);
	shiftDirByCPY((byte*)this + m_dirStart - dirSize, moveDir, dirSize);

	assert(m_freeSpace > dataSize + dirSize);
	// �޸�ҳ��ͷ��Ϣ
	m_freeSpace -= dataSize + dirSize;
	m_miniPageNum = m_miniPageNum + (dirSize / MINI_PAGE_DIR_SIZE);
	m_dirStart = m_dirStart - dirSize;
	m_prevPage = prevPageId;
	m_pageCount = getMPsItemCounts(m_miniPageNum);
	m_smoLSN = smoLSN;
	assert(m_dirStart >= getDataEndOffset());

	adjustMiniPageDataOffset(dirSize / MINI_PAGE_DIR_SIZE, dataSize);

	vecode(vs.idx, traversalAndVerify(NULL, NULL, NULL, NULL, NULL));
}


/**
 * redo����SMO���Ѳ���(ԭҳ��MiniPage��Ŀ����һ�����)
 * @param nextPageId	�ϲ���ҳ�����һ��ҳ���ID
 * @param dataSize		��ҳ���ƶ������ݳ���
 * @param dirSize		��ҳ���ƶ���Ŀ¼���ݳ���
 * @param oldSKLen		���Ѽ�ֵԭʼ����
 * @param mpLeftCount	���ѵ�MPʣ��ԭʼҳ���������Ϊ0˵����MP����
 * @param mpLeftCount	���ѵ�MP�ƶ�����ҳ���������Ϊ0˵����MP����
 * @param smoLSN		ҳ��SMO֮���LSN
 */
void IndexPage::redoSMOSplitOrig(PageId newPageId, u16 dataSize, u16 dirSize, u16 oldSKLen, u8 mpLeftCount, u8 mpMoveCount, u64 smoLSN) {
	assert(getDataEndOffset() - INDEXPAGE_DATA_START_OFFSET > dataSize + oldSKLen);
	assert(INDEX_PAGE_SIZE - m_dirStart >= dirSize);

	bool splitMiddle = (mpLeftCount != 0 && mpMoveCount != 0);
	u16 movePoint = getDataEndOffset() - dataSize - oldSKLen;
	u16 leftDirSize = INDEX_PAGE_SIZE - m_dirStart - dirSize + (splitMiddle ? MINI_PAGE_DIR_SIZE : 0);
	shiftDirByMOVE((byte*)this + INDEX_PAGE_SIZE - leftDirSize, (byte*)this + m_dirStart, leftDirSize);
	memset((byte*)this + movePoint, 0, dataSize + oldSKLen);
	memset((byte*)this + m_dirStart, 0, INDEX_PAGE_SIZE - m_dirStart - leftDirSize);

	m_miniPageNum = leftDirSize / MINI_PAGE_DIR_SIZE;
	m_dirStart = m_dirStart + dirSize - (splitMiddle ? MINI_PAGE_DIR_SIZE : 0);
	m_freeSpace = m_freeSpace + dataSize + dirSize - (splitMiddle ? MINI_PAGE_DIR_SIZE : 0) + oldSKLen;
	if (splitMiddle)
		setMiniPageItemCount(m_miniPageNum - 1, mpLeftCount);
	m_pageCount = getMPsItemCounts(m_miniPageNum);
	m_nextPage = newPageId;
	m_insertOffsetRatio = 5000;
	m_smoLSN = smoLSN;
	assert(movePoint + m_freeSpace == m_dirStart);

	vecode(vs.idx, traversalAndVerify(NULL, NULL, NULL, NULL, NULL));
}



/**
 * ����SMO�ϲ������ϲ�ҳ��Ĳ���
 */
void IndexPage::redoSMOMergeRel() {
	memset((byte*)this + INDEXPAGE_DATA_START_OFFSET, 0, INDEX_PAGE_SIZE - INDEXPAGE_DATA_START_OFFSET);
}


/**
 * ����SMO���Ѳ�����ҳ��Ļָ�
 * @param moveData		�ƶ�����������
 * @param dataSize		�ƶ������ݳ���
 * @param moveDir		�ƶ�����Ŀ¼����
 * @param dirSize		�ƶ�����Ŀ¼����
 * @param newSplitKey	���ѵ��ֵ��ѹ�������
 * @param newSKLen		���ѵ��ֵ��ѹ�󳤶�
 * @param mpLeftCount	����MPʣ��ԭҳ�������
 * @param mpMoveCount	����MP�ƶ�����ҳ�������
 * @param smoLSN		ҳ��SMO֮���LSN
 */
void IndexPage::redoSMOSplitRel(byte *moveData, u16 dataSize, byte *moveDir, u16 dirSize, byte *newSplitKey, u16 newSKLen, u8 mpLeftCount, u8 mpMoveCount, u64 smoLSN) {
	shiftDataByCPY((byte*)this + INDEXPAGE_DATA_START_OFFSET, newSplitKey, newSKLen);
	shiftDataByCPY((byte*)this + INDEXPAGE_DATA_START_OFFSET + newSKLen, moveData, dataSize);
	shiftDirByCPY((byte*)this + INDEX_PAGE_SIZE - dirSize, moveDir, dirSize);
	m_dirStart = INDEX_PAGE_SIZE - dirSize;
	m_freeSpace = INDEX_PAGE_SIZE - INDEXPAGE_DATA_START_OFFSET - dataSize - dirSize - newSKLen;
	m_miniPageNum = dirSize / MINI_PAGE_DIR_SIZE;
	if (mpLeftCount != 0 && mpMoveCount != 0)
		setMiniPageItemCount(0, mpMoveCount);
	m_pageCount = getMPsItemCounts(m_miniPageNum);
	m_insertOffsetRatio = 5000;
	m_nChance = 2;
	m_smoLSN = smoLSN;

	vecode(vs.idx, traversalAndVerify(NULL, NULL, NULL, NULL, NULL));
}


/**
 * redoҳ����²���
 * @param offset			��������ҳ��ƫ��
 * @param newValue			���µ�������
 * @param valueLen			�������ݳ���
 * @param clearPageFirst	��־����ǰ�Ƿ�Ҫ������
 */
void IndexPage::redoPageUpdate(u16 offset, byte *newValue, u16 valueLen, bool clearPageFirst) {
	if (clearPageFirst)
		memset((byte*)this + sizeof(Page), 0, INDEX_PAGE_SIZE - sizeof(Page));
	shiftDataByCPY((byte*)this + offset, newValue, valueLen);
}



/**
 * ����ҳ������MiniPage����
 * @param keyValue		MiniPage������ֵ����
 * @param dataSize		��ֵ�ĳ���
 * @param miniPageNo	MiniPageҳ���
 */
void IndexPage::redoPageAddMP(const byte *keyValue, u16 dataSize, u16 miniPageNo) {
	assert(miniPageNo <= m_miniPageNum);
	assert(m_freeSpace >= dataSize + MINI_PAGE_DIR_SIZE);
	u16 offset = (miniPageNo == m_miniPageNum) ? getDataEndOffset() : getMiniPageDataOffset(miniPageNo);
	if (m_pageCount != 0)
		updateInsertOffsetRatio(offset);
	addMiniPageDir(miniPageNo, offset, 1);
	shiftDataByMOVE((byte*)this + offset + dataSize, (byte*)this + offset, getDataEndOffset() - offset);
	shiftDataByCPY((byte*)this + offset, (byte*)keyValue, dataSize);
	m_freeSpace = m_freeSpace - dataSize;
	m_pageCount++;
	adjustMiniPageDataOffset(miniPageNo + 1, dataSize);

	vecode(vs.idx, traversalAndVerify(NULL, NULL, NULL, NULL, NULL));
}


/**
 * ����ҳ��ɾ��MiniPage����
 * @param miniPageNo	Ҫɾ����MIniPageNo��
 */
void IndexPage::redoPageDeleteMP(u16 miniPageNo) {
	u16 miniPageEnd = (miniPageNo < m_miniPageNum - 1) ? getMiniPageDataOffset(miniPageNo + 1) : getDataEndOffset();
	u16 miniPageStart = getMiniPageDataOffset(miniPageNo);
	deleteMiniPageIn(miniPageNo, miniPageStart, miniPageEnd);

	vecode(vs.idx, traversalAndVerify(NULL, NULL, NULL, NULL, NULL));
}



/**
 * ����ҳ��ĳ��MiniPage�ķ��Ѳ���
 * @param offset			���ѵ��ҳ��ƫ��
 * @param oldSize			�����ݳ���
 * @param newValue			����������
 * @param newSize			�����ݳ���
 * @param leftItems			ԭMiniPage���µ�����
 * @param miniPageNo		���ѵ�MiniPage���
 */
void IndexPage::redoPageSplitMP(u16 offset, u16 oldSize, byte *newValue, u16 newSize, u16 leftItems, u16 miniPageNo) {
	// ��Ŀ¼
	u16 splitItems = getMiniPageItemCount(miniPageNo) - leftItems;
	addMiniPageDir(miniPageNo + 1, offset, splitItems);
	adjustMiniPageDataOffset(miniPageNo + 2, newSize - oldSize);
	setMiniPageItemCount(miniPageNo, leftItems);
	// ����
	s32 moveSize = getDataEndOffset() - offset - oldSize;
	assert(moveSize > 0);
	shiftDataByMOVE((byte*)this + offset + newSize, (byte*)this + offset + oldSize, moveSize);
	shiftDataByCPY((byte*)this + offset, newValue, newSize);
	assert(m_freeSpace + oldSize >= newSize);
	m_freeSpace = m_freeSpace - (newSize - oldSize);
	// m_splitedMPNo = miniPageNo;	// Ӧ�ò���Ҫ

	vecode(vs.idx, traversalAndVerify(NULL, NULL, NULL, NULL, NULL));
}

/**
 * ����ҳ��ĳ��MiniPage�ĺϲ�����
 * @param offset		�ϲ����ҳ��ƫ��
 * @param oldSize		�����ݳ���
 * @param newValue		����������
 * @param newSize		�����ݳ���
 * @param miniPageNo	�ϲ������MiniPage���
 */
void IndexPage::redoPageMergeMP( u16 offset, u16 oldSize, byte *newValue, u16 newSize, u16 miniPageNo ) {
	assert(newSize <= oldSize);
	assert(m_miniPageNum > miniPageNo + 1);
	// ��Ŀ¼
	u16 mergeLeftKeys = getMiniPageItemCount(miniPageNo);
	u16 mergeRightKeys = getMiniPageItemCount(miniPageNo + 1);
	setMiniPageItemCount(miniPageNo, mergeLeftKeys + mergeRightKeys);
	adjustMiniPageDataOffset(miniPageNo + 2, newSize - oldSize);
	deleteMiniPageDir(miniPageNo + 1);
	// ����
	s32 moveSize = getDataEndOffset() - offset - oldSize;
	assert(moveSize > 0);
	shiftDataByMOVE((byte*)this + offset + newSize, (byte*)this + offset + oldSize, moveSize);
	shiftDataByMOVE((byte*)this + offset, newValue, newSize);
	m_freeSpace += oldSize - newSize;

	vecode(vs.idx, traversalAndVerify(NULL, NULL, NULL, NULL, NULL));
}


/**
 * undo���롢ɾ�������²���
 * @param type			��־����
 * @param offset		ҳ���޸���ʼƫ��
 * @param miniPageNo	�����漰��MiniPage
 * @param oldValue		֮ǰ��ָ��ƫ�ƴ�������
 * @param oldSize		ǰ�����ݵĳ���
 * @param newSize		ǰ�����ݵĳ���
 */
void IndexPage::undoDMLUpdate(IDXLogType type, u16 offset, u16 miniPageNo, byte *oldValue, u16 oldSize, u16 newSize) {
	s32 moveSize = getDataEndOffset() - offset - newSize;
	assert(moveSize >= 0);
	shiftDataByMOVE((byte*)this + offset + oldSize, (byte*)this + offset + newSize, moveSize);
	memcpy((byte*)this + offset, oldValue, oldSize);
	// �޸�ҳ�������Ϣ
	assert(m_freeSpace + newSize >= oldSize);
	m_freeSpace = m_freeSpace - (oldSize - newSize);
	if (type == IDX_LOG_DELETE) {
		incMiniPageItemCount(miniPageNo);
		m_pageCount++;
		m_nChance > 0 ? ++m_nChance : m_nChance;
	} else {	// IDX_INSERT/IDX_APPEND
		if (newSize > oldSize)	// ���ٵ���������
			memset((byte*)this + offset + oldSize + moveSize, 0, newSize - oldSize);

		decMiniPageItemCount(miniPageNo);
		m_pageCount--;
	}
	adjustMiniPageDataOffset(miniPageNo + 1, oldSize - newSize);
}


/**
 * undo����SMO�ϲ�����ԭʼҳ����޸�
 * @param leftPageId	�ϲ���ҳ��ID
 * @param dataSize		��ҳ���ƶ������ݳ���
 * @param dirSize		��ҳ���ƶ���Ŀ¼���ݳ���
 */
void IndexPage::undoSMOMergeOrig(PageId leftPageId, u16 dataSize, u16 dirSize) {
	// undoԭҳ�����
	u16 origDataSize = getDataEndOffset() - dataSize;
	byte *dataStart = (byte*)this + INDEXPAGE_DATA_START_OFFSET + dataSize;
	shiftDataByMOVE((byte*)this + INDEXPAGE_DATA_START_OFFSET, dataStart, origDataSize);
	memset((byte*)this + origDataSize, 0, dataSize);
	memset((byte*)this + m_dirStart, 0, dirSize);
	m_miniPageNum = (INDEX_PAGE_SIZE - m_dirStart - dirSize) / MINI_PAGE_DIR_SIZE;
	m_freeSpace = m_freeSpace + dataSize + dirSize;
	m_dirStart = m_dirStart + dirSize;
	adjustMiniPageDataOffset(0, (0 - dataSize));
	m_pageCount = getMPsItemCounts(m_miniPageNum);
	m_prevPage = leftPageId;
	assert(m_freeSpace < INDEX_PAGE_SIZE);

	vecode(vs.idx, traversalAndVerify(NULL, NULL, NULL, NULL, NULL));
}


/**
 * undo����SMO���Ѳ���ԭʼҳ����޸�
 * @param nextPageId	����ǰ��ҳ��ĺ��ҳ��
 * @param moveData		��ҳ���ƶ�����������
 * @param dataSize		��ҳ���ƶ������ݳ���
 * @param moveDir		��ҳ���ƶ���������Ŀ¼
 * @param dirSize		��ҳ���ƶ���Ŀ¼���ݳ���
 * @param oldSplitKey	���Ѽ�ֵԭʼ����
 * @param oldSKLen		���Ѽ�ֵԭʼ����
 * @param newSKLen		���Ѽ�ֵ�³���
 * @param mpLeftCount	����MP����ԭʼҳ������
 * @param mpMoveCount	����MP�ƶ���ҳ������
 */
void IndexPage::undoSMOSplitOrig(PageId nextPageId, byte *moveData, u16 dataSize, byte *moveDir, u16 dirSize, byte *oldSplitKey, u16 oldSKLen, u16 newSKLen, u8 mpLeftCount, u8 mpMoveCount) {
	assert(m_freeSpace > dataSize + dirSize);
	u16 splitOffset = getDataEndOffset();
	byte *appendStart = (byte*)this + splitOffset;

	shiftDataByCPY(appendStart, oldSplitKey, oldSKLen);
	appendStart += oldSKLen;
	shiftDataByCPY(appendStart, moveData, dataSize);
	bool splitMiddle = mpLeftCount != 0 && mpMoveCount != 0;
	if (splitMiddle)
		setMiniPageItemCount(m_miniPageNum - 1, mpLeftCount + mpMoveCount);
	u16 reduceDirSize = dirSize - (splitMiddle ? MINI_PAGE_DIR_SIZE : 0);
	shiftDirByMOVE((byte*)this + m_dirStart - reduceDirSize, (byte*)this + m_dirStart, INDEX_PAGE_SIZE - m_dirStart);
	shiftDirByCPY((byte*)this + INDEX_PAGE_SIZE - reduceDirSize, moveDir + (splitMiddle ? MINI_PAGE_DIR_SIZE : 0), reduceDirSize);
	m_nextPage = nextPageId;
	m_dirStart = m_dirStart - reduceDirSize;
	m_freeSpace = m_freeSpace - dataSize - oldSKLen - reduceDirSize;
	m_miniPageNum = (INDEX_PAGE_SIZE - m_dirStart) / MINI_PAGE_DIR_SIZE;
	m_pageCount = getMPsItemCounts(m_miniPageNum);
	m_insertOffsetRatio = (u16)((splitOffset - INDEXPAGE_DATA_START_OFFSET) * 100.0 / (getDataEndOffset() - INDEXPAGE_DATA_START_OFFSET) * 100);
	u16 diff = splitOffset - INDEXPAGE_DATA_START_OFFSET + oldSKLen - newSKLen;
	adjustMiniPageDataOffset(m_miniPageNum - reduceDirSize / MINI_PAGE_DIR_SIZE, diff);

	vecode(vs.idx, traversalAndVerify(NULL, NULL, NULL, NULL, NULL));
}


/**
 * undo����SMO�ϲ��������ҳ��Ĳ���
 * @param prevPageId	��ҳ��ϲ���ǰһ��ҳ���ҳ��ID
 * @param moveData		�ƶ�����������
 * @param dataSize		��ҳ������ݳ���
 * @param moveDir		�ƶ�����Ŀ¼����
 * @param dirSize		��Ŀ¼����
 */
void IndexPage::undoSMOMergeRel(PageId prevPageId, byte *moveData, u16 dataSize, byte *moveDir, u16 dirSize) {
	assert(m_freeSpace > dataSize + dirSize);
	shiftDataByCPY((byte*)this + INDEXPAGE_DATA_START_OFFSET, moveData, dataSize);
	shiftDataByCPY((byte*)this + INDEX_PAGE_SIZE - dirSize, moveDir, dirSize);
	m_dirStart = INDEX_PAGE_SIZE - dirSize;
	m_miniPageNum = dirSize / MINI_PAGE_DIR_SIZE;
	m_pageCount = getMPsItemCounts(m_miniPageNum);
	m_freeSpace = INDEX_PAGE_SIZE - INDEXPAGE_DATA_START_OFFSET - dataSize - dirSize;
	m_prevPage = prevPageId;

	vecode(vs.idx, traversalAndVerify(NULL, NULL, NULL, NULL, NULL));
}

/**
 * undoSMO�������ҳ��Ĳ���
 */
void IndexPage::undoSMOSplitRel() {
	m_miniPageNum = m_pageCount = 0;
	m_freeSpace = INDEX_PAGE_SIZE - INDEXPAGE_DATA_START_OFFSET;
	m_dirStart = INDEX_PAGE_SIZE;
	memset((byte*)this, 0, INDEX_PAGE_SIZE - INDEXPAGE_DATA_START_OFFSET);
}


/**
 * undoҳ����²���
 * @param offset	��������ҳ��ƫ��
 * @param oldValue	���µľ�����
 * @param valueLen	�������ݳ���
 */
void IndexPage::undoPageUpdate(u16 offset, byte *oldValue, u16 valueLen) {
	shiftDataByCPY((byte*)this + offset, oldValue, valueLen);
}


/**
 * undoҳ������MiniPage����
 * @param dataSize		MiniPageҳ�����ݳ���
 * @param miniPageNo	MiniPageҳ���
 */
void IndexPage::undoPageAddMP(u16 dataSize, u16 miniPageNo) {
	UNREFERENCED_PARAMETER(dataSize);
	assert(miniPageNo <= m_miniPageNum);
	u16 miniPageStart = getMiniPageDataOffset(miniPageNo);
	u16 miniPageEnd = (miniPageNo == m_miniPageNum - 1) ? getDataEndOffset() : getMiniPageDataOffset(miniPageNo + 1);
	assert(miniPageEnd - miniPageStart == dataSize);
	deleteMiniPageIn(miniPageNo, miniPageStart, miniPageEnd);

	vecode(vs.idx, traversalAndVerify(NULL, NULL, NULL, NULL, NULL));
}


/**
 * undoҳ��ɾ��MiniPage����
 * @param keyValue		MIniPage���еļ�ֵ
 * @param dataSize		��ֵ�ĳ���
 * @param miniPageNo	Ҫɾ����MIniPageNo��
 */
void IndexPage::undoPageDeleteMP(const byte *keyValue, u16 dataSize, u16 miniPageNo) {
	u16 offset = (miniPageNo == m_miniPageNum) ? getDataEndOffset() : getMiniPageDataOffset(miniPageNo);
	addMiniPageDir(miniPageNo, offset, 1);
	shiftDataByMOVE((byte*)this + offset + dataSize, (byte*)this + offset, getDataEndOffset() - offset);
	shiftDataByCPY((byte*)this + offset, (byte*)keyValue, dataSize);
	assert(m_freeSpace > dataSize);
	m_freeSpace = m_freeSpace - dataSize;
	m_pageCount++;
	m_nChance > 0 ? ++m_nChance : m_nChance;
	adjustMiniPageDataOffset(miniPageNo + 1, dataSize);

	vecode(vs.idx, traversalAndVerify(NULL, NULL, NULL, NULL, NULL));
}


/**
 * undoҳ��ĳ��MiniPage�ķ��Ѳ���
 * @param offset			���ѵ��ҳ��ƫ��
 * @param oldValue			����ǰ��ֵǰ׺
 * @param oldSize			��ֵǰ׺����
 * @param newSize			��ֵǰ׺����
 * @param miniPageNo		���ѵ�MiniPage���
 */
void IndexPage::undoPageSplitMP(u16 offset, byte *oldValue, u16 oldSize, u16 newSize, u16 miniPageNo) {
	assert(oldSize <= newSize);	// ����ǰ׺�ͺ�׺���ܳ���Ҫ�����ֽڱ�ʾ���������ѹ֮���׺ֻҪһ���ֽڣ���ʱ�������1���ֽڣ����Ǽ�ֵ����ʵ���ϻ��ܶ�
	// ��Ŀ¼undo
	u16 newMPItems = getMiniPageItemCount(miniPageNo + 1);
	setMiniPageItemCount(miniPageNo, getMiniPageItemCount(miniPageNo) + newMPItems);
	adjustMiniPageDataOffset(miniPageNo + 2, oldSize - newSize);
	deleteMiniPageDir(miniPageNo + 1);

	// ����undo
	s32 moveSize = getDataEndOffset() - offset - newSize;
	assert(moveSize > 0);
	shiftDataByMOVE((byte*)this + offset + oldSize, (byte*)this + offset + newSize, moveSize);
	memcpy((byte*)this + offset, oldValue, oldSize);
	// �޸�ҳ�������Ϣ
	assert(m_freeSpace + newSize >= oldSize);
	m_freeSpace = m_freeSpace - (oldSize - newSize);
	//m_splitedMPNo = (u16)-1;	// ����Ҫ

	if (newSize > oldSize)
		memset((byte*)this + getDataEndOffset(), 0, newSize - oldSize);

	vecode(vs.idx, traversalAndVerify(NULL, NULL, NULL, NULL, NULL));
}


/**
 * undoĳ��ҳ���MP�ϲ�����
 * @param offset		�ϲ����ҳ��ƫ��
 * @param oldValue		�ϲ�ǰ���������ݣ�δѹ����ʽ
 * @param oldSize		�ϲ�ǰ�����ݳ���
 * @param newSize		�ϲ�������ݳ��ȣ�ѹ��֮��
 * @param originalMPKeyCounts	�ϲ�ǰ���MP����������
 * @param miniPageNo	�ϲ����MP�ı��
 */
void IndexPage::undoPageMergeMP( u16 offset, byte *oldValue, u16 oldSize, u16 newSize, u16 originalMPKeyCounts, u16 miniPageNo ) {
	assert(newSize <= oldSize);
	// ��Ŀ¼undo
	u16 rightMPKeyCounts = getMiniPageItemCount(miniPageNo) - originalMPKeyCounts;
	addMiniPageDir(miniPageNo + 1, offset, rightMPKeyCounts);
	adjustMiniPageDataOffset(miniPageNo + 2, oldSize - newSize);
	setMiniPageItemCount(miniPageNo, originalMPKeyCounts);
	// ����undo
	s32 moveSize = getDataEndOffset() - offset - newSize;
	assert(moveSize > 0);
	shiftDataByMOVE((byte*)this + offset + oldSize, (byte*)this + offset + newSize, moveSize);
	shiftDataByCPY((byte*)this + offset, oldValue, oldSize);
	assert(m_freeSpace + newSize >= oldSize);
	m_freeSpace = m_freeSpace - (oldSize - newSize);

	vecode(vs.idx, traversalAndVerify(NULL, NULL, NULL, NULL, NULL));
}


/**
 * ����ָ����ƫ�ƣ���ȡ��ƫ�ƴ�����ļ�ֵ
 * @param offset	ָ��ƫ��
 * @param key1		out �ҵ��ļ�ֵ������key1����
 * @Param key2		��ʱ��ֵ
 */
void IndexPage::fetchKeyByOffset(u16 offset, SubRecord **key1, SubRecord **key2) {
	u16 pos = INDEXPAGE_DATA_START_OFFSET;
	assert(offset >= pos);

	while (pos <= offset) {
		IndexKey::swapKey(key1, key2);
		IndexKey *indexKey = (IndexKey*)((byte*)this + pos);
		u16 used = indexKey->extractKey((*key2)->m_data, *key1, !isPageLeaf());
		pos = pos + used;
	}

	assert(pos <= getDataEndOffset());
}


/**
 * ���������ڲ�ID��ţ��Լ�ҳ��ID�������һ��ҳ��Ψһ��ʶ��
 * ���ڵļ��㷽ʽΪֻʹ��indexId���һ����־��
 * ʹ������ͬһ��������ҳ���־һ����һ���ģ�����һ��ҳ��Ψһ��Ӧһ������
 * @param indexId	�����ڲ�ID
 * @param pageId	�����ΪINVALID_ROW_ID����ʾָ��ĳ��ҳ��
 * @return ҳ���־
 */
u32 IndexPage::formPageMark(u16 indexId, PageId pageId) {
	UNREFERENCED_PARAMETER(pageId);
	return u32(indexId);
}

/**
 * ����ָ���ı�־���ó�����������ID��
 * @param mark	ҳ���־
 * @return ����ID
 */
u16 IndexPage::getIndexId(u32 mark) {
	return (u16)mark;
}

/**
 * ��������ҳ�棬���ҳ��������ȷ��
 *
 * @param	tableDef	����ҳ����������
 * @param	indexDef	��������
 * @param	key1		��ʱ��ֵ
 * @param	key2		��ʱ��ֵ
 * @param	pkey		pad��ʽ��ֵ
 * @return true��ʾҳ����ȷ������ҳ�����
 */
bool IndexPage::traversalAndVerify(const TableDef *tableDef, const IndexDef *indexDef, SubRecord *key1, SubRecord *key2, SubRecord *pkey) {
	assert(isPageEmpty() || m_pageCount == getMPsItemCounts(m_miniPageNum));
	assert(m_dirStart - m_freeSpace >= INDEXPAGE_DATA_START_OFFSET);
	assert((u16)(INDEX_PAGE_SIZE - m_dirStart) == MINI_PAGE_DIR_SIZE * m_miniPageNum);

	bool compareKey = (key1 != NULL && key2 != NULL && pkey != NULL);

	u16 miniPageNum = 0;
	u16 itemCounts = 0;

	if (isPageEmpty()) {
		assert(m_freeSpace == INDEX_PAGE_SIZE - INDEXPAGE_DATA_START_OFFSET);
		return true;
	}

	SubRecord **prevKey = NULL;
	SubRecord **nextKey = NULL;
	if (compareKey) {
		key1->m_size = key2->m_size = 0;
		prevKey = &key1;
		nextKey = &key2;
	}
	uint offset = INDEXPAGE_DATA_START_OFFSET;
	uint end = getDataEndOffset();
	while (offset < end) {
		IndexKey *indexKey = (IndexKey*)((byte*)this + offset);
		if (*((byte*)indexKey) == 0) {	// ��֤MiniPageĿ¼��ȷ��
			assert_always(getMiniPageItemCount(miniPageNum) <= MINI_PAGE_MAX_ITEMS);
			assert_always(offset == getMiniPageDataOffset(miniPageNum));
			if (offset != INDEXPAGE_DATA_START_OFFSET) {
				assert_always(itemCounts == getMiniPageItemCount(miniPageNum - 1));
				itemCounts = 0;
			}
			miniPageNum++;
		}

		if (compareKey) {
			IndexKey::swapKey(prevKey, nextKey);
			offset = offset + indexKey->extractKey((*prevKey)->m_data, (*nextKey), !isPageLeaf());
			if (IndexKey::isKeyValid(*prevKey) && IndexKey::isKeyValid(*nextKey)) {
				RecordOper::convertKeyCP(tableDef, indexDef, *nextKey, pkey);
				s32 result = RecordOper::compareKeyPC(tableDef, pkey, *prevKey, indexDef);
				assert_always(result >= 0);
				UNREFERENCED_PARAMETER(result);
			}
		} else {
			offset = offset + indexKey->skipAKey();
		}
		itemCounts++;
	}

	// �����пռ䶼����
	byte *start = (byte*)this + m_dirStart - m_freeSpace;
	byte *finish = (byte*)this + m_dirStart;
	while (start < finish) {
		assert(*start == 0x00);
		start++;
	}

	assert_always(itemCounts == getMiniPageItemCount(miniPageNum - 1));
	assert_always(offset == end);
	assert_always(miniPageNum == m_miniPageNum);

	return true;
}

/**
 * ����ҳ���ѹ��ǰҳ�����ݳ���
 * @param tableDef	����
 * @param indexDef  ��������
 * @param key		���ڶ�ȡÿ����ֵ���Ӽ�¼
 * @return ѹ��ǰҳ�����ݳ���
 */
uint IndexPage::calcUncompressedSpace(const TableDef *tableDef, const IndexDef *indexDef, SubRecord *key) {
	assert(key->m_format == KEY_COMPRESS);
	assert(m_pageLevel == 0);
	u16 pos = INDEXPAGE_DATA_START_OFFSET;
	u16 end = getDataEndOffset();
	uint initLen = 0;
	while (pos < end) {
		IndexKey *indexKey = (IndexKey*)((byte*)this + pos);
		pos = pos + indexKey->extractKey(key->m_data, key, false);
		initLen += (RecordOper::getKeySizeCN(tableDef, indexDef, key) + RID_BYTES);
	}

	if (initLen == 0) {	// ������Ϊ�յ����
		assert(this->isPageRoot() && this->isPageEmpty());
		initLen = 1;
	}

	return initLen;
}


/** ��֤ҳ������������0
 * @return true��֤�ɹ���falseʧ��
 */
bool IndexPage::isDataClean() {
	uint steps = (Limits::PAGE_SIZE - sizeof(IndexPage)) / sizeof(u64);
	for (uint i = 0; i < steps; i++) {
		u64 value = *(u64*)((byte*)this + sizeof(IndexPage) + i * sizeof(u64));
		if (value != 0)
			return false;
	}

	uint leftDatas = (Limits::PAGE_SIZE - sizeof(IndexPage)) % sizeof(u64);
	for (uint i = 0; i < leftDatas; i++) {
		if (*(byte*)(this + Limits::PAGE_SIZE - i - 1) != 0x00)
			return false;
	}

	return true;
}


/**
 * �ļ�ͷҳ�湹�캯��
 * @param indexMark	�ļ���ʶ��
 */
IndexHeaderPage::IndexHeaderPage(u32 indexMark) {
	memset((void*)this, 0, sizeof(IndexHeaderPage));
	m_checksum = BufferPageHdr::CHECKSUM_NO;
	m_indexMark = indexMark;
	m_minFreeBitMap = m_curFreeBitMap = IndicePageManager::HEADER_PAGE_NUM * INDEX_PAGE_SIZE + sizeof(Page);
	m_indexUniqueId = (u8)1;
	m_dataOffset = INDEX_PAGE_SIZE * (IndicePageManager::BITMAP_PAGE_NUM + IndicePageManager::HEADER_PAGE_NUM);
}


/**
 * �����������ҳ��Ĳ�����Ϣ
 *
 * @param session	�Ự���
 * @param logger	��־��¼��
 * @param pageId	��ҳ���ID
 * @param offset	Ҫ���µ���Ϣ��ҳ��ƫ����
 * @param size		������Ϣ����
 * @param newValue	������Ϣ
 */
void IndicePage::updateInfo(Session *session, IndexLog *logger, PageId pageId, u16 offset, u16 size, byte *newValue) {
	byte *oldValue = (byte*)this + offset;
	u64 newLSN = logger->logPageUpdate(session, IDX_LOG_UPDATE_PAGE, pageId, offset, newValue, oldValue, size, m_lsn);
	memcpy(oldValue, newValue, size);
	m_lsn = newLSN;
}



/**
 * ��������ʱ��ͷҳ����޸�
 * @pre ���Ӧ�ö�ͷҳ�����X���������������������Ҫȷ��ָ��������������Ϣȷʵ����
 * @param indexNo			Ҫ������������Ϣ���
 * @param minFreeByteMap	����֮����С����λͼλƫ��
 */
void IndexHeaderPage::discardIndexChangeHeaderPage(u8 indexNo, u32 minFreeByteMap) {
	u8 indexNum = (u8)m_indexNum;
	for (u8 i = indexNo + 1; i <= indexNum - 1; i++) {
		m_indexIds[i - 1] = m_indexIds[i];
		m_rootPageIds[i - 1] = m_rootPageIds[i];
		m_freeListPageIds[i - 1] = m_freeListPageIds[i];
		m_maxBMPageIds[i - 1] = m_maxBMPageIds[i];
		m_indexDataPages[i - 1] = m_indexDataPages[i];
		m_indexFreePages[i - 1] = m_indexFreePages[i];
	}

	if (indexNo < indexNum) {
		--m_indexNum;
		m_indexIds[m_indexNum] = 0;
		m_rootPageIds[m_indexNum] = 0;
		m_freeListPageIds[m_indexNum] = 0;
		m_maxBMPageIds[m_indexNum] = 0;
		m_indexDataPages[m_indexNum] = 0;
		m_indexFreePages[m_indexNum] = 0;
	}

	if (m_curFreeBitMap > minFreeByteMap)
		m_curFreeBitMap = minFreeByteMap;
}



/**
 * ����ָ������ҳ��ʹ��ͳ����Ϣ
 * @pre ����ͷҳ����˻�����
 * @param session		�Ự���
 * @param logger		��־��¼��
 * @param indexNo		Ҫ������Ϣ���������
 * @param dataPages		���µ�����ʹ��ҳ�������������(u64)-1��ʾ��ǰ����Ҫ����
 * @param freePages		���µ���������ҳ�������������(u64)-1��ʾ��ǰ����Ҫ����
 */
void IndexHeaderPage::updatePageUsedStatus(Session *session, IndexLog *logger, u8 indexNo, u64 dataPages, u64 freePages) {
	assert(freePages != (u64)-1 && dataPages >= freePages);
	if (dataPages != (u64)-1)
		updateInfo(session, logger, 0, OFFSETOFARRAY(IndexHeaderPage, m_indexDataPages, sizeof(dataPages), indexNo), sizeof(dataPages), (byte*)&dataPages);
	updateInfo(session, logger, 0, OFFSETOFARRAY(IndexHeaderPage, m_indexFreePages, sizeof(freePages), indexNo), sizeof(freePages), (byte*)&freePages);
}

/** ���ص�ǰ�������������õ�����ID��Ҳ���Ƿ���m_uniqueIndexId
 * @pre ����ͷҳ����˻�����
 * @return ���õ�����ID
 */
u8 IndexHeaderPage::getAvailableIndexId() const {
	return m_indexUniqueId;
}

/** ���ص�ǰ�������������õ�����ID�����ұ�ʶ��ID�����á��ڲ�ʵ�ֽ���������һ��ID������m_uniqueIndexId��
 * @pre ����ͷҳ����˻�����
 * @post ��Ϊʹ�õ�id�����Ѿ��޸ģ�������Ҫ�����������޸ģ�����m_indexNumӦ�ü�1
 * @param session	�Ự���
 * @param logger	��־��¼��
 * @return ���õ�����ID
 */
u8 IndexHeaderPage::getAvailableIndexIdAndMark( Session *session, IndexLog *logger ) {
	u8 indexId = m_indexUniqueId;

	// ������һ�����õ�����ID����Ҫ����Ѿ�ʹ�õ�ID������
	u8 ids[Limits::MAX_INDEX_NUM];
	ids[0] = indexId;
	memcpy(ids + sizeof(ids[0]), m_indexIds, sizeof(ids[0]) * m_indexNum);	// ȷ��m_indexNumӦ��ֻ�����Ѿ������˵�ID����������
	std::sort(ids, ids + m_indexNum + 1);

	u8 uniqueId = 0;
	if (ids[m_indexNum] < 255) {
		uniqueId = ids[m_indexNum] + 1;
	} else if (ids[0] > 1) {
		uniqueId = ids[0] - 1;
	} else {
		uniqueId = ids[0] + 1;
		for (uint i = 1; i < m_indexNum && ids[i] == uniqueId; i++, uniqueId++);
	}
	//u8 uniqueId = (m_indexNum == 0 ? m_indexUniqueId + 1 : (indexId < 255 ? indexId + 1 : 1));
	// ��ѭ����Ҫ�Ǵ�������uniqueId==1��m_indexNum!=0������������������ִ�������ѭ��������Ӱ��������ȷ�ԣ�Ҳ������Ч������
	//for (uint i = 0; i < m_indexNum && ids[i] == uniqueId; i++, uniqueId++) ;

	updateInfo(session, logger, 0, OFFSET(IndexHeaderPage, m_indexUniqueId), sizeof(u8), (byte*)&uniqueId);
	updateInfo(session, logger, 0, OFFSETOFARRAY(IndexHeaderPage, m_indexIds, 1, m_indexNum), 1, &indexId);

	return indexId;
}

}
