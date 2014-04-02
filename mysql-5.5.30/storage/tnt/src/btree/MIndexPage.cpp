/**
* 内存索引页面相关
*
* @author 李伟钊(liweizhao@corp.netease.com)
*/
#include "btree/MIndexPage.h"
#include <iomanip>

namespace tnt {

const u32 MIndexPage::MINDEX_PAGE_SIZE = Limits::PAGE_SIZE;
const u16 MIndexPage::MINDEX_DIR_SIZE = sizeof(u16);
const u16 MIndexPage::MINDEX_PAGE_HEADER_SIZE = sizeof(MIndexPage);

/**
 * 初始化页面
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
 * 在页面内做二分查找
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
 * 在非叶页面中根据搜索键查找子页面
 * @pre 页面是非叶页面
 * @pre 搜索键一定在页面的键值范围内
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
 * 在叶页面中查找索引键
 * @param searchkey    查找键
 * @param tmpKey       外部分配的临时键值
 * @param searchFlag   查找标志
 * @param comparator   键值比较器
 * @param sr OUT 输出查找结果
 * @return 是否需要获取逻辑上的下一项
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
	} else {// 比较键值为空
		getExtremeKeyAtLeaf(searchFlag->isForward(), tmpKey, &searchResult->m_keyLocation);
		return false;
	}
}

/**
 * 在叶页面中定位待插入索引键的位置
 * @param searchkey
 * @param tmpKey
 * @param comparator
 * @param keyLocation
 * @return 页面中是否已经存在键值和RowId都相等的索引项
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
 * 在页面中删除指定的键值
 * @param key    要删除的键值
 * @param tmpKey 辅助键
 * @param comparator 比较器
 * @return 要删除的键值是否在页面中被找到
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

				//移动键值数据
				byte *keyStart = getKeyStartByNo(keyNoInPage);
				u16 dataLen = m_dirStart - m_freeSpace - getKeySOffsetByNo(keyNoInPage) - keyLen;
				shiftDataByMOVE(keyStart, keyStart + keyLen, dataLen);

				//移动项目录并调整偏移量
				adjustPageDataOffset(keyNoInPage, 0 - keyLen);
				byte *dirStart = (byte *)this + m_dirStart;
				shiftDataByMOVE(dirStart + MINDEX_DIR_SIZE, dirStart, keyNoInPage * MINDEX_DIR_SIZE);

				//更新页头信息
				m_keyCount--;
				m_dirStart += MINDEX_DIR_SIZE;
				m_freeSpace += keyLen + MINDEX_DIR_SIZE;
				
				//更新页面时间戳
				incrTimeStamp();
			}
			return true;
		}
	}
	return false;
}

/**
 * 在页面中插入索引项
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
		//在页面内查找插入位置
		if (!keyLocation) {
			NTSE_ASSERT(!locateInsertKeyInLeafPage(key, tmpKey, comparator, &location));
			keyLocation = &location;
		}
		
		byte *keyStart = (byte*)this + keyLocation->m_sOffset;

		//将页面键值数据后半部分往后移
		u16 keyLength = MIndexKeyOper::calcKeyTotalLen((u16)key->m_size, isPageLeaf());
		shiftDataByMOVE(keyStart + keyLength, keyStart, getKeyDataEnd() - keyStart);

		//拷贝完整的键值信息，包括键值以及扩展信息
		memcpy(keyStart, key->m_data, keyLength);

		if (MIDX_PAGE_NONE != childPageHdl) {
			assert(!isPageLeaf());
			InnerPageKeyExt * keyExt = (InnerPageKeyExt *)(keyStart + key->m_size);
			keyExt->m_pageHdl = childPageHdl;
		}

		//修改头部信息
		m_freeSpace -= keyLength;
		++m_keyCount;

		//添加新的项目录
		addDir(keyLocation->m_keyNo, keyLocation->m_sOffset);

		//调整项目录中的键值偏移量
		adjustPageDataOffset(keyLocation->m_keyNo + 1, keyLength);

		//更新页面时间戳
		incrTimeStamp();

		return INSERT_SUCCESS;
	}
}

/**
 * append一个索引项到页面中
 * @pre 调用方必须保证键值的有序性
 * @param key 新索引项
 * @param pageHdl 如果是非叶页面，则为新索引项对应的子页面句柄
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

		//添加新的项目录
		addDir(newDirNo, m_dirStart - m_freeSpace);

		//拷贝完整的键值信息，包括键值以及扩展信息
		u16 keyLength = MIndexKeyOper::calcKeyTotalLen((u16)key->m_size, isPageLeaf());
		memcpy(keyStart, key->m_data, keyLength);

		if (MIDX_PAGE_NONE != pageHdl) {
// 			assert(!isPageLeaf());
// 			InnerPageKeyExt * keyExt = (InnerPageKeyExt *)(keyStart + key->m_size);
// 			keyExt->m_pageHdl = pageHdl;
			assert(false);
		}

		//修改头部信息
		m_freeSpace -= keyLength;
		++m_keyCount;

		//更新页面时间戳
		incrTimeStamp();

		return INSERT_SUCCESS;
	}
}

/**
 * 用二分法在项目录中确定分裂点
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
 * 分裂页面
 * @param newPage INOUT 新页面
 */
void MIndexPage::split(MIndexPageHdl newPage) {
	assert(m_keyCount > 1);

	const u16 splitSize = MINDEX_PAGE_HEADER_SIZE + 
		(MINDEX_PAGE_SIZE - MINDEX_PAGE_HEADER_SIZE - m_keyCount * MINDEX_DIR_SIZE) / 2;
	const u16 splitPoint = calcSplitPoint(splitSize);
	const u16 oldHighKeyLen = getHighKeyLen();
	
	//设置新页面high-key，非叶结点high-key等于页面内最大的key，所以不需要这一步
	byte *newPageCopyTo = (byte*)newPage + MINDEX_PAGE_HEADER_SIZE;
	if (isPageLeaf()) {
		memcpy(newPageCopyTo, (byte*)this + MINDEX_PAGE_HEADER_SIZE, oldHighKeyLen);
		newPageCopyTo += oldHighKeyLen;
		newPage->m_freeSpace -= oldHighKeyLen;
	}

	//拷贝键值数据到新页面
	byte *keyCopyStart = getKeyStartByNo(splitPoint + 1);
	const byte *keyCopyEnd = getKeyDataEnd();
	const u16 keyCopyLen = (u16)(keyCopyEnd - keyCopyStart);
	memcpy(newPageCopyTo, keyCopyStart, keyCopyLen);
	memset(keyCopyStart, 0, keyCopyLen);

	//拷贝项目录到新页面
	adjustPageDataOffset(splitPoint + 1, (byte *)this + MINDEX_PAGE_HEADER_SIZE - keyCopyStart + 
		(isPageLeaf() ? oldHighKeyLen : 0));
	const u16 dirSplitOffset = m_dirStart + MINDEX_DIR_SIZE * (splitPoint + 1);
	const byte *dirSplitStart = (byte *)this + dirSplitOffset;
	const u16 dirCopyLen = MINDEX_PAGE_SIZE - dirSplitOffset;
	byte * newPageDirStart = (byte *)newPage + dirSplitOffset;
	memcpy(newPageDirStart, dirSplitStart, dirCopyLen);
	newPage->m_dirStart = dirSplitOffset;
	
	//调整新页面页头信息
	assert(m_keyCount > splitPoint);
	newPage->m_keyCount = m_keyCount - splitPoint - 1;
	newPage->m_freeSpace -= keyCopyLen + dirCopyLen;

	//移动旧页面的项目录
	const s32 moveSize = MINDEX_DIR_SIZE * (splitPoint + 1);
	byte * dirStart = (byte *)this + m_dirStart;
	shiftDataByMOVE(dirStart + dirCopyLen, dirStart, moveSize);
	m_dirStart += dirCopyLen;
	
	//调整旧页面页头信息
	m_keyCount = splitPoint + 1;
	m_freeSpace += keyCopyLen + dirCopyLen;

	//重新设置旧页面的high-key, 并移动旧页面的键值数据
	if (isPageLeaf()) {
		const u16 oldHighKeyLen = getKeySOffsetByNo(0) - MINDEX_PAGE_HEADER_SIZE;
		byte *newHighKeyStart = getKeyStartByNo(splitPoint);
		const u16 newHighKeyLen = getKeyLenByNo(splitPoint);
		const s16 shiftSize = newHighKeyLen - oldHighKeyLen;//旧键值数据的移动偏移量
		const uint shiftLen = getKeyEOffsetByNo(splitPoint) - getKeySOffsetByNo(0);//要移动数据长度
		byte *pageDataStart = getKeyDataStart();//旧键值数据起始地址， 不包括high-key		
		shiftDataByMOVE(pageDataStart + shiftSize, pageDataStart, shiftLen);
		adjustPageDataOffset(0, splitPoint, shiftSize);
		memcpy((byte *)this + MINDEX_PAGE_HEADER_SIZE, newHighKeyStart + shiftSize, newHighKeyLen);//设置新high-key
		m_freeSpace -= shiftSize;
	}

	if (newPage->isPageLeaf()) {
		//新页面的最大事务ID等于原页面的最大事务ID
		newPage->setMaxTrxId(m_maxTrxId);
	}
}

/**
 * 将另一页面合并到本页面
 * @pre 本页面空闲空间一定足够容纳新页面数据以及项目录
 * @param pageMergeFrom 要合并的页面
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
		//合并后的页面的最大事务ID应该设置为两个页面中最大事务ID较大的那个
		setMaxTrxId(max(m_maxTrxId, pageMergeFrom->getMaxTrxId()));
	}
}

/**
 * 和另一个页面重新分布键值
 * @param rightPage
 * @return 
 */
void MIndexPage::redistribute(MIndexPageHdl rightPage) {
	u16 keyDataLen = getKeyDataTotalLen();
	u16 keyDataLen2 = rightPage->getKeyDataTotalLen();
	u16 splitSize = (keyDataLen + keyDataLen2) / 2;

	if (keyDataLen < keyDataLen2) {//从右页面移动一部分到左页面
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
	} else {//从左页面移动一部分到右页面
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
		//修改页面最大事务ID
		if (getMaxTrxId() > rightPage->getMaxTrxId()) {
			rightPage->setMaxTrxId(getMaxTrxId());
		} else {
			setMaxTrxId(rightPage->getMaxTrxId());
		}

		//更新左页面的high-key
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
 * 批量清理页面索引项
 * @param readView  purge视图
 * @param assistKey 辅助键值
 * @param doubleChecker 
 * @return 回收的索引项数
 */
u64 MIndexPage::bulkPhyReclaim(const ReadView *readView, SubRecord *assistKey, 
							   const DoubleChecker *doubleChecker) {
    assert(isPageLeaf());
	if (m_keyCount > 0) {
		u64 purgeKeyCount = 0;
 		
		u16 *keys2Del = (u16 *)alloca(m_keyCount * sizeof(u16));
		u16 delCount = 0;
		//先定位满足条件的索引项
		for (u16 i = 0; i < m_keyCount; i++) {
			getKeyByNo(i, assistKey);
			if (MIndexKeyOper::isInfiniteKey(assistKey))
				continue;

			RowIdVersion version = MIndexKeyOper::readIdxVersion(assistKey);
			if (!doubleChecker->isRowIdValid(assistKey->m_rowId, version)) {
				//如果通过RowId在内存堆中找不到记录，或者找到记录但是RowId版本不对，
				//说明此索引项对应的原记录已经从内存堆purge出去，此时删除内存索引项即可
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
						// 批量移动数据
						byte *keyStart = getKeyStartByNo(keyNoInPage);
						u16 dataLen = m_dirStart - m_freeSpace - getKeySOffsetByNo(keyNoInPage) - segKeyLen;
						shiftDataByMOVE(keyStart, keyStart + segKeyLen, dataLen);

						// 批量移动项目录
						adjustPageDataOffset(keyNoInPage, 0 - segKeyLen);
						byte *dirStart = (byte *)this + m_dirStart;
						shiftDataByMOVE(dirStart + MINDEX_DIR_SIZE * segCount , dirStart, keyNoInPage * MINDEX_DIR_SIZE);

						// 更新页头信息
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
 *	添加一个项目录
 *	@param newDirNo 新项目录号
 *	@param dataOffset 项目录数据，即键值起始位置偏移量
 */
void MIndexPage::addDir(u16 newDirNo, u16 dataOffset) {
	byte *dirStartAddr = (byte*)this + m_dirStart;
	byte *newDirStartAddr = dirStartAddr - MINDEX_DIR_SIZE;
	//往前移动前半部分项目录
	shiftDataByMOVE(newDirStartAddr, dirStartAddr, newDirNo * MINDEX_DIR_SIZE);
	//初始化新项目录数据
	((u16 *)newDirStartAddr)[newDirNo] = dataOffset;
	//修改页头信息
	m_freeSpace -= MINDEX_DIR_SIZE;
	m_dirStart -= MINDEX_DIR_SIZE;
	assert(dataOffset == ((u16 *)((byte *)this + m_dirStart))[newDirNo]);
}


/**
 * 调整项目录中的键值起始位置偏移量
 * @param firstAjustDirNo 起始项目录号
 * @param deltaOffset     要调整的偏移量delta值
 */
void MIndexPage::adjustPageDataOffset(u16 firstAjustDirNo, s32 deltaOffset) {
	assert(firstAjustDirNo <= m_keyCount);
	adjustPageDataOffset(firstAjustDirNo, m_keyCount - 1, deltaOffset);
}

/**
 * 调整项目录中的键值起始位置偏移量
 * @param firstAjustDirNo 起始项目录号
 * @param deltaOffset     要调整的偏移量delta值
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
 * 获得页面的high-key
 * @pre 必须已经持有页面latch
 * @param key OUT 存储页面的high-key，直接引用页面数据不拷贝
 */
void MIndexPage::getHighKey(SubRecord *highKey) {
	assert(KEY_NATURAL == highKey->m_format);
	if (!isPageLeaf()) {
		assert(m_keyCount > 0);
		//非叶页面的hight-key为本页面最大的键值
		getLastKey(highKey);
	} else {
		//叶页面的hight-key不一定是本页面最大的键值,存在页头之后的位置
		byte *hightKeyStart = (byte *)this + MINDEX_PAGE_HEADER_SIZE;
		highKey->m_data = hightKeyStart;
		highKey->m_size = getKeyDataStart() - hightKeyStart - LeafPageKeyExt::PAGE_KEY_EXT_LEN;
		highKey->m_rowId = MIndexKeyOper::readRowId(highKey);
	}
}

/**
 * 获得指定索引项的下一索引项
 * @param keyLocation 上一索引项位置
 * @param nextkeyLoc OUT 下一索引项位置
 * @param foundKey OUT 下一键值
 * @return 是否存在下一索引项
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
 * 获得指定索引项的前一索引项
 * @param keyLoc 当前索引项位置
 * @param prevKeyLoc 前一索引项位置
 * @param foundKey OUT 前一键值
 * @return 是否存在前一索引项
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
 * 将页面最大键值设为页面的high-key
 * @pre 页面上没有high-key
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
 * 查找子页面在本页面中对应的键值
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
 * 判断一个页面是否是本页面的子页面
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
 * 验证页面数据
 * @param tableDef 索引所属表定义
 * @param idxDef   索引定义
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
 * 打印页面数据
 * @param idxDef   索引定义
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
