/**
* �ڴ�����ҳ�����
*
* @author ��ΰ��(liweizhao@corp.netease.com)
*/
#include "btree/MIndexPage.h"
#include <iomanip>

namespace tnt {

const u32 MIndexPage::MINDEX_PAGE_SIZE = Limits::PAGE_SIZE;
const u16 MIndexPage::MINDEX_DIR_SIZE = sizeof(u16);
const u16 MIndexPage::MINDEX_PAGE_HEADER_SIZE = sizeof(MIndexPage);

/**
 * ��ʼ��ҳ��
 * @param pageMark
 * @param pageType
 * @param pageLevel
 * @param prevPage
 * @param nextPage
 * @return 
 */
void MIndexPage::initPage(u32 pageMark, IndexPageType pageType, u8 pageLevel, MIndexPageHdl prevPage, 
						  MIndexPageHdl nextPage) {
	m_pageMark = pageMark;
	m_pageType = (u8)pageType;
	m_pageLevel = pageLevel;
	m_freeSpace = (u16)(MINDEX_PAGE_SIZE - MINDEX_PAGE_HEADER_SIZE);
	m_keyCount = 0;
	m_dirStart = (u16)MINDEX_PAGE_SIZE;
	m_prevPage = prevPage;
	m_nextPage = nextPage;
	m_timeStamp = 0;
	m_maxTrxId = 0;
}

/**
 * ��ҳ���������ֲ���
 * @param searchkey
 * @param outKey
 * @param keyNoInPage
 * @param flag
 * @param comparator
 * @return 
 */
s32 MIndexPage::binarySearch(const SubRecord *searchkey, SubRecord *outKey, u16 *keyNoInPage, 
							 const SearchFlag *flag, KeyComparator *comparator) {
	s32 low = 0;
	s32 high = m_keyCount - 1;
	s32 middle = 0;
	while (low < high) {
		middle = (low + high) / 2;
		getKeyByNo((u16)middle, outKey);
		s32 result = MIndexKeyOper::compareKey(searchkey, outKey, flag, comparator);
		if (result < 0) {
			high = middle;
		} else if (result > 0) {
			low = middle + 1;
		} else {
			*keyNoInPage = (u16)middle;
			return 0;
		}
	}//while
	if (low >= (s32)m_keyCount)
		low = m_keyCount - 1;
	*keyNoInPage = (u16)low;
	getKeyByNo((u16)low, outKey);
	return MIndexKeyOper::compareKey(searchkey, outKey, flag, comparator);
}

/**
 * �ڷ�Ҷҳ���и���������������ҳ��
 * @pre ҳ���Ƿ�Ҷҳ��
 * @pre ������һ����ҳ��ļ�ֵ��Χ��
 * @param searchkey
 * @param tmpKey
 * @param searchFlag
 * @param comparator
 * @return 
 */
MIndexPageHdl MIndexPage::findChildPage(const SubRecord *searchkey, SubRecord *tmpKey, 
										const SearchFlag* flag, KeyComparator *comparator) {
	assert(!isPageLeaf());
	assert(m_keyCount > 0);

	if (likely(MIndexKeyOper::isKeyValid(searchkey))) {
		u16 keyNoInPage;
		s32 result = binarySearch(searchkey, tmpKey, &keyNoInPage, flag, comparator);
		NTSE_ASSERT(result <= 0);
	} else {
		getExtremeKey(flag->isForward(), tmpKey);
	}
	return MIndexKeyOper::readPageHdl(tmpKey);
}

/**
 * 
 * @param searchkey
 * @param tmpKey
 * @param comparator
 * @param childpageHdl
 * @return 
 */
void MIndexPage::resetChildPage(const SubRecord *searchkey, SubRecord *tmpKey, KeyComparator *comparator, 
								MIndexPageHdl childpageHdl) {
	assert(!isPageLeaf());
	assert(m_keyCount > 0);

	u16 keyNoInPage;
	s32 result = binarySearch(searchkey, tmpKey, &keyNoInPage, &SearchFlag::DEFAULT_FLAG, comparator);
	NTSE_ASSERT(0 == result);
	MIndexKeyOper::writePageHdl(tmpKey, childpageHdl);
}

/**
 * ��Ҷҳ���в���������
 * @param searchkey    ���Ҽ�
 * @param tmpKey       �ⲿ�������ʱ��ֵ
 * @param searchFlag   ���ұ�־
 * @param comparator   ��ֵ�Ƚ���
 * @param sr OUT ������ҽ��
 * @return �Ƿ���Ҫ��ȡ�߼��ϵ���һ��
 */
bool MIndexPage::findKeyInLeafPage(const SubRecord *searchkey, SubRecord *tmpKey, 
								   const SearchFlag *searchFlag, KeyComparator *comparator, 
								   MIndexSearchResult *searchResult) {
	assert(isPageLeaf());
	assert(m_keyCount > 0);

	if (likely(MIndexKeyOper::isKeyValid(searchkey))) {
		u16 keyNoInPage = (u16)-1;
		searchResult->m_cmpResult = binarySearch(searchkey, tmpKey, &keyNoInPage, searchFlag, comparator);
		searchResult->m_keyLocation.m_keyNo = keyNoInPage;
		searchResult->m_keyLocation.m_sOffset = getKeySOffsetByNo(keyNoInPage);
		searchResult->m_keyLocation.m_eOffset = getKeyEOffsetByNo(keyNoInPage);
		return isNeedFetchNext(searchResult->m_cmpResult, searchFlag);
	} else {// �Ƚϼ�ֵΪ��
		getExtremeKeyAtLeaf(searchFlag->isForward(), tmpKey, &searchResult->m_keyLocation);
		return false;
	}
}

/**
 * ��Ҷҳ���ж�λ��������������λ��
 * @param searchkey
 * @param tmpKey
 * @param comparator
 * @param keyLocation
 * @return ҳ�����Ƿ��Ѿ����ڼ�ֵ��RowId����ȵ�������
 */
bool MIndexPage::locateInsertKeyInLeafPage(const SubRecord *searchkey, SubRecord *tmpKey, 
										   KeyComparator *comparator, MkeyLocation *keyLocation) {
	assert(INVALID_ROW_ID != searchkey->m_rowId);
	
	if (likely(m_keyCount > 0)) {
		u16 keyNoInPage = (u16)-1;
		s32 result = binarySearch(searchkey, tmpKey, &keyNoInPage, &SearchFlag::DEFAULT_FLAG, comparator);
		if (result <= 0) {	
			keyLocation->m_keyNo = keyNoInPage;
			keyLocation->m_sOffset = getKeySOffsetByNo(keyNoInPage);
		} else {
			keyLocation->m_keyNo = keyNoInPage + 1;
			keyLocation->m_sOffset = getKeyEOffsetByNo(keyNoInPage);
		}
		keyLocation->m_eOffset = keyLocation->m_sOffset	+ 
			MIndexKeyOper::calcKeyTotalLen((u16)searchkey->m_size, isPageLeaf());
		return 0 == result;
	} else {
		keyLocation->m_keyNo = 0;
		keyLocation->m_sOffset = m_dirStart - m_freeSpace;
		keyLocation->m_eOffset = keyLocation->m_sOffset	+ 
			MIndexKeyOper::calcKeyTotalLen((u16)searchkey->m_size, isPageLeaf());
		return false;
	}
}

/**
 * ��ҳ����ɾ��ָ���ļ�ֵ
 * @param key    Ҫɾ���ļ�ֵ
 * @param tmpKey ������
 * @param comparator �Ƚ���
 * @return Ҫɾ���ļ�ֵ�Ƿ���ҳ���б��ҵ�
 */
bool MIndexPage::deleteIndexKey(const SubRecord *key, SubRecord *tmpKey, KeyComparator *comparator) {
	assert(KEY_NATURAL == key->m_format);

	if (likely(m_keyCount > 0)) {
		u16 keyNoInPage = (u16)-1;
		s32 result = binarySearch(key, tmpKey, &keyNoInPage, &SearchFlag::DEFAULT_FLAG, comparator);
		if (0 == result) {
			assert(key->m_rowId == tmpKey->m_rowId);
			if (1 == m_keyCount) {
				assert(0 == keyNoInPage);
				u16 keyLen = getKeyLenByNo(keyNoInPage);
				m_keyCount = 0;
				m_dirStart = MINDEX_PAGE_SIZE;
				m_freeSpace += keyLen + MINDEX_DIR_SIZE;
			} else {
				u16 keyLen = getKeyLenByNo(keyNoInPage);

				//�ƶ���ֵ����
				byte *keyStart = getKeyStartByNo(keyNoInPage);
				u16 dataLen = m_dirStart - m_freeSpace - getKeySOffsetByNo(keyNoInPage) - keyLen;
				shiftDataByMOVE(keyStart, keyStart + keyLen, dataLen);

				//�ƶ���Ŀ¼������ƫ����
				adjustPageDataOffset(keyNoInPage, 0 - keyLen);
				byte *dirStart = (byte *)this + m_dirStart;
				shiftDataByMOVE(dirStart + MINDEX_DIR_SIZE, dirStart, keyNoInPage * MINDEX_DIR_SIZE);

				//����ҳͷ��Ϣ
				m_keyCount--;
				m_dirStart += MINDEX_DIR_SIZE;
				m_freeSpace += keyLen + MINDEX_DIR_SIZE;
				
				//����ҳ��ʱ���
				incrTimeStamp();
			}
			return true;
		}
	}
	return false;
}

/**
 * ��ҳ���в���������
 * @param key
 * @param tmpKey
 * @param searchFlag
 * @param comparator
 * @param childPageHdl
 * @param keyLocation
 * @return 
 */
IndexInsertResult MIndexPage::addIndexKey(const SubRecord *key, SubRecord *tmpKey, KeyComparator *comparator, 
										  MIndexPageHdl childPageHdl, const MkeyLocation *keyLocation) {
	assert(KEY_NATURAL == key->m_format);

	uint needLen = calcSpaceNeeded(key, isPageLeaf());
	if (m_freeSpace < needLen) {
		return NEED_SPLIT;
	} else {
		MkeyLocation location;
		//��ҳ���ڲ��Ҳ���λ��
		if (!keyLocation) {
			NTSE_ASSERT(!locateInsertKeyInLeafPage(key, tmpKey, comparator, &location));
			keyLocation = &location;
		}
		
		byte *keyStart = (byte*)this + keyLocation->m_sOffset;

		//��ҳ���ֵ���ݺ�벿��������
		u16 keyLength = MIndexKeyOper::calcKeyTotalLen((u16)key->m_size, isPageLeaf());
		shiftDataByMOVE(keyStart + keyLength, keyStart, getKeyDataEnd() - keyStart);

		//���������ļ�ֵ��Ϣ��������ֵ�Լ���չ��Ϣ
		memcpy(keyStart, key->m_data, keyLength);

		if (MIDX_PAGE_NONE != childPageHdl) {
			assert(!isPageLeaf());
			InnerPageKeyExt * keyExt = (InnerPageKeyExt *)(keyStart + key->m_size);
			keyExt->m_pageHdl = childPageHdl;
		}

		//�޸�ͷ����Ϣ
		m_freeSpace -= keyLength;
		++m_keyCount;

		//����µ���Ŀ¼
		addDir(keyLocation->m_keyNo, keyLocation->m_sOffset);

		//������Ŀ¼�еļ�ֵƫ����
		adjustPageDataOffset(keyLocation->m_keyNo + 1, keyLength);

		//����ҳ��ʱ���
		incrTimeStamp();

		return INSERT_SUCCESS;
	}
}

/**
 * appendһ�������ҳ����
 * @pre ���÷����뱣֤��ֵ��������
 * @param key ��������
 * @param pageHdl ����Ƿ�Ҷҳ�棬��Ϊ���������Ӧ����ҳ����
 * @return 
 */
IndexInsertResult MIndexPage::appendIndexKey(const SubRecord *key, MIndexPageHdl pageHdl) {
	assert(KEY_NATURAL == key->m_format);
	uint needLen = calcSpaceNeeded(key, isPageLeaf());
	if (m_freeSpace < needLen) {
		/*return NEED_SPLIT;*/
		assert(false);
		return NEED_SPLIT;
	} else {
		u16 newDirNo = m_keyCount;
		byte *keyStart = (byte*) this + m_dirStart - m_freeSpace;

		//����µ���Ŀ¼
		addDir(newDirNo, m_dirStart - m_freeSpace);

		//���������ļ�ֵ��Ϣ��������ֵ�Լ���չ��Ϣ
		u16 keyLength = MIndexKeyOper::calcKeyTotalLen((u16)key->m_size, isPageLeaf());
		memcpy(keyStart, key->m_data, keyLength);

		if (MIDX_PAGE_NONE != pageHdl) {
// 			assert(!isPageLeaf());
// 			InnerPageKeyExt * keyExt = (InnerPageKeyExt *)(keyStart + key->m_size);
// 			keyExt->m_pageHdl = pageHdl;
			assert(false);
		}

		//�޸�ͷ����Ϣ
		m_freeSpace -= keyLength;
		++m_keyCount;

		//����ҳ��ʱ���
		incrTimeStamp();

		return INSERT_SUCCESS;
	}
}

/**
 * �ö��ַ�����Ŀ¼��ȷ�����ѵ�
 * @return 
 */
u16 MIndexPage::calcSplitPoint(u16 splitSize) {
	assert(m_keyCount > 1);
	u16 low = 0;
	u16 high = m_keyCount - 1;

	while (low < high) {
		u16 middle = (low + high) / 2;
		u16 offset = getKeySOffsetByNo(middle);
		if (offset > splitSize) {
			high = middle;
		} else if (offset < splitSize) {
			low = middle + 1;
		} else {
			return middle;
		}
	}
	if (low >= m_keyCount)
		low = m_keyCount - 1;
	return low;
}

/**
 * ����ҳ��
 * @param newPage INOUT ��ҳ��
 */
void MIndexPage::split(MIndexPageHdl newPage) {
	assert(m_keyCount > 1);

	const u16 splitSize = MINDEX_PAGE_HEADER_SIZE + 
		(MINDEX_PAGE_SIZE - MINDEX_PAGE_HEADER_SIZE - m_keyCount * MINDEX_DIR_SIZE) / 2;
	const u16 splitPoint = calcSplitPoint(splitSize);
	const u16 oldHighKeyLen = getHighKeyLen();
	
	//������ҳ��high-key����Ҷ���high-key����ҳ��������key�����Բ���Ҫ��һ��
	byte *newPageCopyTo = (byte*)newPage + MINDEX_PAGE_HEADER_SIZE;
	if (isPageLeaf()) {
		memcpy(newPageCopyTo, (byte*)this + MINDEX_PAGE_HEADER_SIZE, oldHighKeyLen);
		newPageCopyTo += oldHighKeyLen;
		newPage->m_freeSpace -= oldHighKeyLen;
	}

	//������ֵ���ݵ���ҳ��
	byte *keyCopyStart = getKeyStartByNo(splitPoint + 1);
	const byte *keyCopyEnd = getKeyDataEnd();
	const u16 keyCopyLen = (u16)(keyCopyEnd - keyCopyStart);
	memcpy(newPageCopyTo, keyCopyStart, keyCopyLen);
	memset(keyCopyStart, 0, keyCopyLen);

	//������Ŀ¼����ҳ��
	adjustPageDataOffset(splitPoint + 1, (byte *)this + MINDEX_PAGE_HEADER_SIZE - keyCopyStart + 
		(isPageLeaf() ? oldHighKeyLen : 0));
	const u16 dirSplitOffset = m_dirStart + MINDEX_DIR_SIZE * (splitPoint + 1);
	const byte *dirSplitStart = (byte *)this + dirSplitOffset;
	const u16 dirCopyLen = MINDEX_PAGE_SIZE - dirSplitOffset;
	byte * newPageDirStart = (byte *)newPage + dirSplitOffset;
	memcpy(newPageDirStart, dirSplitStart, dirCopyLen);
	newPage->m_dirStart = dirSplitOffset;
	
	//������ҳ��ҳͷ��Ϣ
	assert(m_keyCount > splitPoint);
	newPage->m_keyCount = m_keyCount - splitPoint - 1;
	newPage->m_freeSpace -= keyCopyLen + dirCopyLen;

	//�ƶ���ҳ�����Ŀ¼
	const s32 moveSize = MINDEX_DIR_SIZE * (splitPoint + 1);
	byte * dirStart = (byte *)this + m_dirStart;
	shiftDataByMOVE(dirStart + dirCopyLen, dirStart, moveSize);
	m_dirStart += dirCopyLen;
	
	//������ҳ��ҳͷ��Ϣ
	m_keyCount = splitPoint + 1;
	m_freeSpace += keyCopyLen + dirCopyLen;

	//�������þ�ҳ���high-key, ���ƶ���ҳ��ļ�ֵ����
	if (isPageLeaf()) {
		const u16 oldHighKeyLen = getKeySOffsetByNo(0) - MINDEX_PAGE_HEADER_SIZE;
		byte *newHighKeyStart = getKeyStartByNo(splitPoint);
		const u16 newHighKeyLen = getKeyLenByNo(splitPoint);
		const s16 shiftSize = newHighKeyLen - oldHighKeyLen;//�ɼ�ֵ���ݵ��ƶ�ƫ����
		const uint shiftLen = getKeyEOffsetByNo(splitPoint) - getKeySOffsetByNo(0);//Ҫ�ƶ����ݳ���
		byte *pageDataStart = getKeyDataStart();//�ɼ�ֵ������ʼ��ַ�� ������high-key		
		shiftDataByMOVE(pageDataStart + shiftSize, pageDataStart, shiftLen);
		adjustPageDataOffset(0, splitPoint, shiftSize);
		memcpy((byte *)this + MINDEX_PAGE_HEADER_SIZE, newHighKeyStart + shiftSize, newHighKeyLen);//������high-key
		m_freeSpace -= shiftSize;
	}

	if (newPage->isPageLeaf()) {
		//��ҳ����������ID����ԭҳ����������ID
		newPage->setMaxTrxId(m_maxTrxId);
	}
}

/**
 * ����һҳ��ϲ�����ҳ��
 * @pre ��ҳ����пռ�һ���㹻������ҳ�������Լ���Ŀ¼
 * @param pageMergeFrom Ҫ�ϲ���ҳ��
 */
void MIndexPage::merge(MIndexPageHdl pageMergeFrom) {
	byte *copyDst = getKeyDataEnd();
	byte *copySrc = pageMergeFrom->getKeyDataStart();

	u16 keyDataLen = getKeyDataTotalLen();
	u16 keyDataLen2 = pageMergeFrom->getKeyDataTotalLen();

	s16 shiftSize = 0;
	if (isPageLeaf()) {
		SubRecord highKey(KEY_NATURAL, 0, NULL, NULL, 0);
		SubRecord highKey2(KEY_NATURAL, 0, NULL, NULL, 0);
		getHighKey(&highKey);
		pageMergeFrom->getHighKey(&highKey2);

		shiftSize = (s16)highKey2.m_size - (s16)highKey.m_size;
		byte *keyDataStart = getKeyDataStart();
		shiftDataByMOVE(keyDataStart + shiftSize, keyDataStart, keyDataLen);
		copyDst += shiftSize;
		if (m_keyCount > 0) {
			adjustPageDataOffset(0, m_keyCount - 1, shiftSize);
		}
		m_freeSpace -= shiftSize;

		//reset high-key
		memcpy((byte*)this + MINDEX_PAGE_HEADER_SIZE, highKey2.m_data, 
			MIndexKeyOper::calcKeyTotalLen((u16)highKey2.m_size, true));
	}
	memcpy(copyDst, copySrc, keyDataLen2);
	
	assert(copyDst + keyDataLen2 < (byte *)this + m_dirStart);

	byte *dirStart = (byte*)this + m_dirStart;
	u16 dirShiftSize = MINDEX_DIR_SIZE * pageMergeFrom->getKeyCount();
	byte *newDirStart = dirStart - dirShiftSize;
	
	shiftDataByMOVE(newDirStart, dirStart, m_keyCount * MINDEX_DIR_SIZE);
	memcpy(newDirStart + m_keyCount * MINDEX_DIR_SIZE, (byte*)pageMergeFrom + pageMergeFrom->m_dirStart, 
		dirShiftSize);

	m_freeSpace -= keyDataLen2 + dirShiftSize;
	m_dirStart -= dirShiftSize;
	m_keyCount += pageMergeFrom->getKeyCount();

	if (pageMergeFrom->getKeyCount() > 0) {
		adjustPageDataOffset(m_keyCount - pageMergeFrom->getKeyCount(), m_keyCount - 1, 
			keyDataLen);
	}

	if (isPageLeaf()) {
		//�ϲ����ҳ����������IDӦ������Ϊ����ҳ�����������ID�ϴ���Ǹ�
		setMaxTrxId(max(m_maxTrxId, pageMergeFrom->getMaxTrxId()));
	}
}

/**
 * ����һ��ҳ�����·ֲ���ֵ
 * @param rightPage
 * @return 
 */
void MIndexPage::redistribute(MIndexPageHdl rightPage) {
	u16 keyDataLen = getKeyDataTotalLen();
	u16 keyDataLen2 = rightPage->getKeyDataTotalLen();
	u16 splitSize = (keyDataLen + keyDataLen2) / 2;

	if (keyDataLen < keyDataLen2) {//����ҳ���ƶ�һ���ֵ���ҳ��
		u16 splitPoint = rightPage->calcSplitPoint(rightPage->getKeyEOffsetByNo(rightPage->getKeyCount()- 1) - splitSize);
		assert(splitPoint < rightPage->getKeyCount());
		
		u16 moveKeyCount = splitPoint + 1;
		u16 dirCopySize = moveKeyCount * MINDEX_DIR_SIZE;
		s32 dirDelta = keyDataLen + (isPageLeaf() ? getHighKeyLen() - rightPage->getHighKeyLen() : 0);

		u16 copySize = rightPage->getKeyEOffsetByNo(splitPoint) - rightPage->getKeySOffsetByNo(0);
		byte *copyFrom = rightPage->getKeyDataStart();
		byte *copyTo = getKeyDataEnd();

		if (unlikely(m_freeSpace < 2 * (copySize + dirCopySize))) {
			return;
		}

		memcpy(copyTo, copyFrom, copySize);
		byte *dirStart = (byte*)this + m_dirStart;
		shiftDataByMOVE(dirStart - dirCopySize,  dirStart, m_keyCount * MINDEX_DIR_SIZE);
		memcpy(dirStart - dirCopySize + m_keyCount * MINDEX_DIR_SIZE, (byte*)rightPage + rightPage->m_dirStart, 
			dirCopySize);
		m_keyCount += moveKeyCount;
		m_freeSpace -= copySize + dirCopySize;
		m_dirStart -= dirCopySize;
		adjustPageDataOffset(m_keyCount - moveKeyCount, m_keyCount - 1, dirDelta);

		shiftDataByMOVE(copyFrom, copyFrom + copySize, keyDataLen2 - copySize);
		rightPage->adjustPageDataOffset(0, rightPage->getKeyCount() - 1, 0 - copySize);
		rightPage->m_keyCount -= moveKeyCount;
		rightPage->m_freeSpace += copySize + dirCopySize;
		rightPage->m_dirStart += dirCopySize;
	} else {//����ҳ���ƶ�һ���ֵ���ҳ��
		u16 splitPoint = calcSplitPoint(getKeySOffsetByNo(0) + splitSize);
		assert(splitPoint < m_keyCount);
		u16 reserveCount = splitPoint + 1;
		if (reserveCount == m_keyCount) {
			return;
		}
		u16 moveCount = m_keyCount - reserveCount;
		u16 dirCopySize = moveCount * MINDEX_DIR_SIZE;
		s16 dirDelta = MINDEX_PAGE_HEADER_SIZE - getKeySOffsetByNo(reserveCount) + 
			(rightPage->isPageLeaf() ? rightPage->getHighKeyLen() : 0);

		u16 copySize = getKeyEOffsetByNo(m_keyCount - 1) - getKeySOffsetByNo(reserveCount);
		byte *copyFrom = getKeyStartByNo(reserveCount);
		byte *copyTo = rightPage->getKeyDataStart();
		
		if (unlikely(rightPage->getFreeSpace() < 2 * (copySize + dirCopySize))) {
			return;
		}
		
		shiftDataByMOVE(copyTo + copySize, copyTo, keyDataLen2);
		memcpy(copyTo, copyFrom, copySize);
		memset(copyFrom, 0, copySize);
		//adjustPageDataOffset(0, copySize);

		byte *dirCopyFrom = (byte *)this + m_dirStart + reserveCount * MINDEX_DIR_SIZE;
		byte *dirStart = (byte*)this + m_dirStart;
		byte *dirStart2 = (byte*)rightPage + rightPage->m_dirStart;
		memcpy(dirStart2 - dirCopySize, dirCopyFrom, dirCopySize);
		shiftDataByMOVE(dirStart + dirCopySize, dirStart, reserveCount * MINDEX_DIR_SIZE);
		
		m_dirStart += dirCopySize;
		m_freeSpace += copySize + dirCopySize;
		m_keyCount -= moveCount;
		rightPage->m_dirStart -= dirCopySize;
		rightPage->m_freeSpace -= copySize + dirCopySize;
		rightPage->m_keyCount += moveCount;

		rightPage->adjustPageDataOffset(0, moveCount - 1, dirDelta);
		rightPage->adjustPageDataOffset(moveCount, rightPage->m_keyCount - 1, (s32)copySize);
	}


	if (isPageLeaf()) {
		//�޸�ҳ���������ID
		if (getMaxTrxId() > rightPage->getMaxTrxId()) {
			rightPage->setMaxTrxId(getMaxTrxId());
		} else {
			setMaxTrxId(rightPage->getMaxTrxId());
		}

		//������ҳ���high-key
		u16 lastKeyLen = getKeyLenByNo(m_keyCount - 1);
		s16 highKeyDelta = lastKeyLen - getHighKeyLen();
		byte *keyStart = getKeyDataStart();
		keyDataLen = getKeyDataTotalLen();
		shiftDataByMOVE(keyStart + highKeyDelta, keyStart, keyDataLen);
		adjustPageDataOffset(0, highKeyDelta);
		memcpy((byte*)this + MINDEX_PAGE_HEADER_SIZE, getKeyStartByNo(m_keyCount - 1), lastKeyLen);
		m_freeSpace -= highKeyDelta;
	}
}

/**
 * ��������ҳ��������
 * @param readView  purge��ͼ
 * @param assistKey ������ֵ
 * @param doubleChecker 
 * @return ���յ���������
 */
u64 MIndexPage::bulkPhyReclaim(const ReadView *readView, SubRecord *assistKey, 
							   const DoubleChecker *doubleChecker) {
    assert(isPageLeaf());
	if (m_keyCount > 0) {
		u64 purgeKeyCount = 0;
 		
		u16 *keys2Del = (u16 *)alloca(m_keyCount * sizeof(u16));
		u16 delCount = 0;
		//�ȶ�λ����������������
		for (u16 i = 0; i < m_keyCount; i++) {
			getKeyByNo(i, assistKey);
			if (MIndexKeyOper::isInfiniteKey(assistKey))
				continue;

			RowIdVersion version = MIndexKeyOper::readIdxVersion(assistKey);
			if (!doubleChecker->isRowIdValid(assistKey->m_rowId, version)) {
				//���ͨ��RowId���ڴ�����Ҳ�����¼�������ҵ���¼����RowId�汾���ԣ�
				//˵�����������Ӧ��ԭ��¼�Ѿ����ڴ��purge��ȥ����ʱɾ���ڴ��������
				keys2Del[delCount++] = i;
			}
		}

		if (delCount > 0) {
			u16 segKeyLen = 0;
			u16 segCount = 0;
			for(int i = delCount - 1; i >= 0 ; i--) {
				u16 keyNoInPage = keys2Del[i];
				u16 keyLen = getKeyLenByNo(keyNoInPage);
				segKeyLen += keyLen;
				segCount++;

				
				if (segCount == m_keyCount) {
					assert(0 == keyNoInPage);
					m_keyCount = 0;
					m_dirStart = MINDEX_PAGE_SIZE;
					m_freeSpace += segKeyLen + MINDEX_DIR_SIZE * segCount;
				} else {
					if (i > 0 &&  keyNoInPage -1 == keys2Del[i - 1] ) {
						continue;
					} else {
						// �����ƶ�����
						byte *keyStart = getKeyStartByNo(keyNoInPage);
						u16 dataLen = m_dirStart - m_freeSpace - getKeySOffsetByNo(keyNoInPage) - segKeyLen;
						shiftDataByMOVE(keyStart, keyStart + segKeyLen, dataLen);

						// �����ƶ���Ŀ¼
						adjustPageDataOffset(keyNoInPage, 0 - segKeyLen);
						byte *dirStart = (byte *)this + m_dirStart;
						shiftDataByMOVE(dirStart + MINDEX_DIR_SIZE * segCount , dirStart, keyNoInPage * MINDEX_DIR_SIZE);

						// ����ҳͷ��Ϣ
						m_keyCount -= segCount;
						m_dirStart += MINDEX_DIR_SIZE * segCount;
						m_freeSpace += segKeyLen + MINDEX_DIR_SIZE * segCount;

						segCount = 0;
						segKeyLen = 0;
					} 
				}
			}
			purgeKeyCount = delCount;
			assert(m_dirStart + MINDEX_DIR_SIZE * m_keyCount == MINDEX_PAGE_SIZE);
		}
		
		incrTimeStamp();
		return purgeKeyCount;
	}
	return 0;
}

/**
 *	���һ����Ŀ¼
 *	@param newDirNo ����Ŀ¼��
 *	@param dataOffset ��Ŀ¼���ݣ�����ֵ��ʼλ��ƫ����
 */
void MIndexPage::addDir(u16 newDirNo, u16 dataOffset) {
	byte *dirStartAddr = (byte*)this + m_dirStart;
	byte *newDirStartAddr = dirStartAddr - MINDEX_DIR_SIZE;
	//��ǰ�ƶ�ǰ�벿����Ŀ¼
	shiftDataByMOVE(newDirStartAddr, dirStartAddr, newDirNo * MINDEX_DIR_SIZE);
	//��ʼ������Ŀ¼����
	((u16 *)newDirStartAddr)[newDirNo] = dataOffset;
	//�޸�ҳͷ��Ϣ
	m_freeSpace -= MINDEX_DIR_SIZE;
	m_dirStart -= MINDEX_DIR_SIZE;
	assert(dataOffset == ((u16 *)((byte *)this + m_dirStart))[newDirNo]);
}


/**
 * ������Ŀ¼�еļ�ֵ��ʼλ��ƫ����
 * @param firstAjustDirNo ��ʼ��Ŀ¼��
 * @param deltaOffset     Ҫ������ƫ����deltaֵ
 */
void MIndexPage::adjustPageDataOffset(u16 firstAjustDirNo, s32 deltaOffset) {
	assert(firstAjustDirNo <= m_keyCount);
	adjustPageDataOffset(firstAjustDirNo, m_keyCount - 1, deltaOffset);
}

/**
 * ������Ŀ¼�еļ�ֵ��ʼλ��ƫ����
 * @param firstAjustDirNo ��ʼ��Ŀ¼��
 * @param deltaOffset     Ҫ������ƫ����deltaֵ
 */
void MIndexPage::adjustPageDataOffset(u16 firstAjustDirNo, u16 lastAjustDirNo, s32 deltaOffset) {
	//assert(firstAjustDirNo <= m_keyCount && lastAjustDirNo < m_keyCount);
	if (likely(deltaOffset != 0)) {
		u16 *dirStartAddr = (u16*)((byte*)this + m_dirStart);
		for (uint i = firstAjustDirNo; i <= lastAjustDirNo; i++) {		
			dirStartAddr[i] = (u16)(dirStartAddr[i] + deltaOffset);
		}
	}
}

/**
 * ���ҳ���high-key
 * @pre �����Ѿ�����ҳ��latch
 * @param key OUT �洢ҳ���high-key��ֱ������ҳ�����ݲ�����
 */
void MIndexPage::getHighKey(SubRecord *highKey) {
	assert(KEY_NATURAL == highKey->m_format);
	if (!isPageLeaf()) {
		assert(m_keyCount > 0);
		//��Ҷҳ���hight-keyΪ��ҳ�����ļ�ֵ
		getLastKey(highKey);
	} else {
		//Ҷҳ���hight-key��һ���Ǳ�ҳ�����ļ�ֵ,����ҳͷ֮���λ��
		byte *hightKeyStart = (byte *)this + MINDEX_PAGE_HEADER_SIZE;
		highKey->m_data = hightKeyStart;
		highKey->m_size = getKeyDataStart() - hightKeyStart - LeafPageKeyExt::PAGE_KEY_EXT_LEN;
		highKey->m_rowId = MIndexKeyOper::readRowId(highKey);
	}
}

/**
 * ���ָ�����������һ������
 * @param keyLocation ��һ������λ��
 * @param nextkeyLoc OUT ��һ������λ��
 * @param foundKey OUT ��һ��ֵ
 * @return �Ƿ������һ������
 */
bool MIndexPage::getNextKey(const MkeyLocation *keyLocation, MkeyLocation *nextkeyLoc, 
							SubRecord *foundKey) {
	assert(KEY_NATURAL == foundKey->m_format);
	assert(keyLocation->m_keyNo < m_keyCount);
	assert(keyLocation->m_sOffset == getKeySOffsetByNo(keyLocation->m_keyNo));
	assert(keyLocation->m_eOffset == getKeyEOffsetByNo(keyLocation->m_keyNo));

	if (likely(keyLocation->m_keyNo < m_keyCount - 1)) {
		u16 no = keyLocation->m_keyNo + 1;
		nextkeyLoc->m_keyNo = no;
		u16 keyLen = getKeyLenByNo(no);
		u16 sOffset = keyLocation->m_eOffset;
		u16 eOffset = sOffset + keyLen;
		nextkeyLoc->m_sOffset = sOffset;
		nextkeyLoc->m_eOffset = eOffset;

		assert(nextkeyLoc->m_sOffset == getKeySOffsetByNo(nextkeyLoc->m_keyNo));
		assert(nextkeyLoc->m_eOffset == getKeyEOffsetByNo(nextkeyLoc->m_keyNo));

		foundKey->m_data = (byte *)this + sOffset;
		foundKey->m_size = keyLen - MIndexKeyOper::getKeyExtLength(isPageLeaf());
		foundKey->m_rowId = MIndexKeyOper::readRowId(foundKey);
		return !MIndexKeyOper::isInfiniteKey(foundKey);
	} else {
		return false;
	}
}

/**
 * ���ָ���������ǰһ������
 * @param keyLoc ��ǰ������λ��
 * @param prevKeyLoc ǰһ������λ��
 * @param foundKey OUT ǰһ��ֵ
 * @return �Ƿ����ǰһ������
 */
bool MIndexPage::getPrevKey(const MkeyLocation *keyLoc, MkeyLocation *prevKeyLoc, SubRecord *foundKey) {
	assert(keyLoc->m_keyNo < m_keyCount);
	assert(keyLoc->m_sOffset == getKeySOffsetByNo(keyLoc->m_keyNo));
	assert(keyLoc->m_eOffset == getKeyEOffsetByNo(keyLoc->m_keyNo));

	if (likely(keyLoc->m_keyNo > 0)) {
		u16 no = keyLoc->m_keyNo - 1;
		prevKeyLoc->m_keyNo = no;
		u16 keyLen = getKeyLenByNo(no);
		u16 eOffset = keyLoc->m_sOffset;
		u16 sOffset = eOffset - keyLen;
		prevKeyLoc->m_eOffset = eOffset;
		prevKeyLoc->m_sOffset = sOffset;

		assert(prevKeyLoc->m_sOffset == getKeySOffsetByNo(prevKeyLoc->m_keyNo));
		assert(prevKeyLoc->m_eOffset == getKeyEOffsetByNo(prevKeyLoc->m_keyNo));
		
		foundKey->m_data = (byte *)this + sOffset;
		foundKey->m_size = keyLen - MIndexKeyOper::getKeyExtLength(isPageLeaf());
		foundKey->m_rowId = MIndexKeyOper::readRowId(foundKey);
		assert(!MIndexKeyOper::isInfiniteKey(foundKey));
		return true;
	} else {
		return false;
	}
}

/**
 * ��ҳ������ֵ��Ϊҳ���high-key
 * @pre ҳ����û��high-key
 * @param indexDef
 * @return 
 */
void MIndexPage::setLeafPageHighKey() {
	assert(m_keyCount > 0);
	assert(isPageLeaf());

	SubRecord lastKey(KEY_NATURAL, 0, NULL, NULL, 0);
	getLastKey(&lastKey);
	u16 totalLen = MIndexKeyOper::calcKeyTotalLen((u16)lastKey.m_size, true);
	void *data = alloca(totalLen);
	memcpy(data, lastKey.m_data, totalLen);

	assert(m_freeSpace >= totalLen);
	byte *keyData = getKeyDataStart();
	u16 keyDataLen = getKeyDataTotalLen();
	shiftDataByMOVE(keyData + totalLen, keyData, keyDataLen);
	adjustPageDataOffset(0, m_keyCount - 1, totalLen);

	memcpy((byte*)this + MINDEX_PAGE_HEADER_SIZE, data, totalLen);
	m_freeSpace -= totalLen;
}

/**
 * ������ҳ���ڱ�ҳ���ж�Ӧ�ļ�ֵ
 * @param childPage
 * @param outKey
 * @return 
 */
bool MIndexPage::findPageAssosiateKey(MIndexPageHdl childPage, SubRecord *outKey) const {
	assert(!isPageLeaf());

	u16 keyCount = getKeyCount();
	for (u16 i = 0; i < keyCount; i++) {
		getKeyByNo(i, outKey);
		if (MIndexKeyOper::readPageHdl(outKey) == childPage)
			return true;
	}
	return false;
}

/**
 * �ж�һ��ҳ���Ƿ��Ǳ�ҳ�����ҳ��
 * @param page
 * @param assistKey
 * @param comparator
 * @return 
 */
bool MIndexPage::isMyChildPage(const MIndexPageHdl page, SubRecord *assistKey, KeyComparator *comparator) {
	assert(MIDX_PAGE_NONE != page);

	SubRecord highKey(KEY_NATURAL, assistKey->m_numCols, assistKey->m_columns, NULL, 0);
	page->getHighKey(&highKey);

	u16 keyNo;
	int cmp = binarySearch(&highKey, assistKey, &keyNo, &SearchFlag::DEFAULT_FLAG, comparator);
	return 0 == cmp && page == MIndexKeyOper::readPageHdl(assistKey);
}

/**
 * ��֤ҳ������
 * @param tableDef ������������
 * @param idxDef   ��������
 */
void MIndexPage::verifyPage(const TableDef *tableDef, const IndexDef *idxDef, bool output/* = false*/) {
	NTSE_ASSERT((int)(MINDEX_PAGE_SIZE - m_dirStart) == (int)MINDEX_DIR_SIZE * m_keyCount);

	KeyComparator comparator(tableDef, idxDef);
	comparator.setComparator(RecordOper::compareKeyNN);

	if (output) {
		std::cout << "\n====================================" << std::endl;
		std::cout << "===page:" << this << std::endl;
	}

	if (m_keyCount > 0) {
		u16 end1 = m_dirStart - m_freeSpace;
		u16 end2 = getKeyEOffsetByNo(m_keyCount - 1);
		assert(end1 == end2);

		NTSE_ASSERT(!isPageLeaf() || getKeySOffsetByNo(0) != MINDEX_PAGE_HEADER_SIZE);

		SubRecord highkey(KEY_NATURAL, idxDef->m_numCols, idxDef->m_columns, NULL, 0);
		getHighKey(&highkey);
		if (output) {
			std::cout << "high key: " << highkey.m_rowId << "###    ";
		}
	}
	
	SubRecord key(KEY_NATURAL, idxDef->m_numCols, idxDef->m_columns, NULL, 0);
	SubRecord nextKey(KEY_NATURAL, idxDef->m_numCols, idxDef->m_columns, NULL, 0);
	for (u16 i = 0; i < m_keyCount; i++) {
		getKeyByNo(i, &key);
		if (i == 0) {
			getKeyByNo(m_keyCount - 1, &nextKey);
			if (output) {
				std::cout << key.m_rowId << " - ";
				std::cout << nextKey.m_rowId << "|" << std::endl;
			}
		}
		if (i != m_keyCount - 1) {
			getKeyByNo(i + 1, &nextKey);
			NTSE_ASSERT(MIndexKeyOper::compareKey(&key, &nextKey, &SearchFlag::DEFAULT_FLAG, 
				&comparator) <= 0);
		}
		if (output) {
			std::cout << key.m_rowId;
			if (!isPageLeaf()) {
				InnerPageKeyExt *ext = (InnerPageKeyExt*)(key.m_data + key.m_size);
				std::cout << "(" << ext->m_pageHdl << ")";
			}
			std::cout << " ";
		}
	}
	if (output) {
		std::cout << std::endl;
	}
}


/**
 * ��ӡҳ������
 * @param idxDef   ��������
 */
void MIndexPage::printPage(const IndexDef *idxDef) {
	NTSE_ASSERT((int)(MINDEX_PAGE_SIZE - m_dirStart) == (int)MINDEX_DIR_SIZE * m_keyCount);

	std::cout << "PageHandle:" << this << endl;
	if (m_keyCount > 0) {
		u16 end1 = m_dirStart - m_freeSpace;
		u16 end2 = getKeyEOffsetByNo(m_keyCount - 1);
		assert(end1 == end2);

		NTSE_ASSERT(!isPageLeaf() || getKeySOffsetByNo(0) != MINDEX_PAGE_HEADER_SIZE);

		SubRecord highkey(KEY_NATURAL, idxDef->m_numCols, idxDef->m_columns, NULL, 0);
		getHighKey(&highkey);
		std::cout << std::hex <<"high key: " << highkey.m_rowId << "### ";
		for(uint i = 0; i < highkey.m_size; i++) {
			std::cout << setw(2) << setfill('0') << (int)highkey.m_data[i];
		}
		std::cout << std::endl;
	}
	
	SubRecord key(KEY_NATURAL, idxDef->m_numCols, idxDef->m_columns, NULL, 0);
	for (u16 i = 0; i < m_keyCount; i++) {
		getKeyByNo(i, &key);
		
		std::cout << "No."<<i<<":"<<"rowid-"<<key.m_rowId<<" || data:";
		for(uint i = 0; i < key.m_size; i++) {
			std::cout << setw(2) << setfill('0') << (int)key.m_data[i];
		}
		std::cout << " || size:"<<key.m_size;
		if (!isPageLeaf()) {
			InnerPageKeyExt *ext = (InnerPageKeyExt*)(key.m_data + key.m_size);
			MIndexPageHdl tmpPage = ((InnerPageKeyExt*)MIndexKeyOper::getKeyExtStart(&key))->m_pageHdl;
			assert(tmpPage == ext->m_pageHdl);
			std::cout << "(" << ext->m_pageHdl << ")";
		}
		std::cout << " " << std::endl;
		
	}
	std::cout << std::oct << std::endl;
}

}
