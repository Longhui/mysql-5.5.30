/**
 * NTSE B+树索引页面管理类
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
 * IndexPage类实现索引页面管理
 *
 * 具体页面存储策略如下：
 * ----------------------------------------------------------------------
 * |页面头部信息（页面类型，剩余空间，前驱后继页面等信息）				|
 * +--------------------------------------------------------------------+
 * |页面数据区|MiniPage								|MiniPage			|
 * +--------------------------------------------------------------------+
 * |						若干个MiniPage								|
 * +--------------------------------------------------------------------+
 * |MiniPage		|空闲区域											|
 * +--------------------------------------------------------------------+
 * |项目录区|	|	|	|	|	|	|	|	|	|	|	|	|	|	|	|
 * ----------------------------------------------------------------------
 *
 * 页面存储格式大致如上图所示，主要分为四部分，
 * 1. 页面头部信息部分，具体包括了页面常用相关信息，可参见IndexPage定义的变量
 * 2. 页面数据区，有序的存放页面包含的索引键值数据
 * 3. 项目录区，有序的存放各个项目录，每个项目录占MINI_PAGE_DIR_SIZE个字节
 * 4. 页面空闲区域，可以用来插入新的数据信息的空间
 *
 * 页面内的数据统一采用前缀压缩的形式连续存储，为了便于页面做二分法查找，
 * 将页面内连续可压缩的若干个（1个或者多个，最多MINI_PAGE_MAX_ITEMS个）数据看作逻辑上的一组数据，当作一个MiniPage来管理
 * 项目录的作用就是用来表示一个MiniPage的相关内容：MiniPage起始便宜量（2个字节）和包含的记录条数（1个字节）
 * 项目录按照从小到大的顺序表示页面内各个MiniPage的信息，查找数据的时候首先通过项目录的二分查找，
 * 定位到某个MiniPage，然后再在MiniPage当中顺序查找
 * 注意：为了使二分查找法的效率能尽可能高，当某个键值和它的前驱键值无法进行前缀压缩的时候，
 * 该键值会存储在另一个不同的MiniPage当中
 *
 * 数据存储从页面头往后存储，项目录存储从页面尾向前存储，当两个部分内容即将重合导致空间不够时，表示页面为满
*/

SearchFlag SearchFlag::DEFAULT_FLAG;

/*
 * 初始化页面本身
 *
 * @param session	会话句柄
 * @param logger	日志记录器
 * @param clearData	是否清零页面数据
 * @param logContent对于页面原来存在数据的情况，是否需要记录原有数据，便于恢复
 * @param pageId	页面ID号
 * @param pageMark	索引页面唯一标识
 * @param pageType	当前页面的类型
 * @param pageLevel	当前页所在层数，默认为0
 * @param prevPage	当前页逻辑前驱页面，默认为NULL
 * @param nextPage	当前页逻辑后继页面，默认为NULL
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


/** 清空页面的数据区域内容，不记录日志
 * @param offset	清理数据的起始偏移
 * @param size		清理数据的总长度
 */
void IndexPage::clearData(u16 offset /*= sizeof(IndexPage)*/, u16 size/* = Limits::PAGE_SIZE - sizeof(IndexPage)*/ ) {
	memset((byte*)this + offset, 0, size);
}


/**
 * 设置本页面的状态为正在进行SMO操作
 * 前提是使用者对页面加了X-Latch
 * @param session	会话句柄
 * @param pageId	页面ID
 * @param logger	日志记录器
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
 * 清除本页面的SMO状态位
 * @param session	会话句柄
 * @param pageId	页面ID
 * @param logger	日志记录器
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
 * 更新页面的后续页面ID
 * @post 属于SMO操作，m_smoLSN会被重设
 *
 * @param session		会话句柄
 * @param logger		日志记录器
 * @param pageId		页面ID
 * @param newNextPage	新的后继页面
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
 * 更新页面的后续页面ID
 * @post 属于SMO操作，m_smoLSN会被重设
 *
 * @param session		会话句柄
 * @param logger		日志记录器
 * @param pageId		页面ID
 * @param newPrevPage	新的前驱页面
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
 * 定位到页面中合适的mini page目录项
 *
 * @param key			要定位的键值
 * @param flag			查找标记
 * @param comparator	索引键值比较器
 * @param tmpRec		主要用于保存临时键值的记录，但是返回之后该键值不保存内容
 * @param result		OUT	查找键值>目录项返回1，<返回-1，等于返回0
 * @return 返回定位到的mini page项目录号，如果页面为空，返回-1
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
	// 这里需要把数据指针替换回来
	tmpRec->m_data = origData;
	return mpNo;
}


/**
 * 在指定的Mini Page当中寻找指定的记录，返回取得的记录内容，记录的偏移量
 *
 * 这里规定：所有键值调整的逻辑都应该根据flag当中包含的forward/includeKey/equalAllowable三个变量在本函数处理
 *			也就是说对于fetchNext，返回的项可以无条件根据forward进行下一步取项操作（getNext/getPrev）
 * @param findkey		要寻找的记录键值
 * @param keyInfo		IN	保存的是查找键值和该MP当中第一项键值的位置和比较信息
 *						OUT	保存找到键值的位置信息，这里找到的键值和要找的键值大小关系无法事先确定
 * @param comparator	索引键值比较器
 * @param foundKey			OUT	用于保存找到的记录
 * @return 表示上层是否还需要根据forward取逻辑上的下一项
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

		if (count == 0) {	// 第一项：如果页面不止一项，可以不进行memcpy；否则，直接解压出来
			if (MPCount == 1)
				pos = pos + indexKey->extractKey(foundKey->m_data, foundKey, isPageIdExist);
			else {
				origData = foundKey->m_data;
				pos = pos + indexKey->fakeExtractMPFirstKey(foundKey, isPageIdExist);
			}
		} else if (count == 1) {	// 第二项：替换回数据指针，开始进行解压比较
			byte *lastData = foundKey->m_data;
			foundKey->m_data = origData;
			pos = pos + indexKey->extractKey(lastData, foundKey, isPageIdExist);
		} else {	// 之后每项都要解压
			pos = pos + indexKey->extractKey(foundKey->m_data, foundKey, isPageIdExist);
		}

		++count;
		res = compareKey(findkey, foundKey, flag, comparator);
		if ((res == 0 && equalAllowable) || res < 0) {	// 找到相等或者更大的项，表示查找成功
			if (count == 1 && MPCount != 1) {
				foundKey->m_data = origData;
				indexKey->extractKey(NULL, foundKey, isPageIdExist);
			}
			goto Finish;
		}
	}

	// 该Mini Page遍历结束，查找项比较大
	res = 1;

Finish:
	keyInfo->m_sOffset = lastpos;
	keyInfo->m_eOffset = pos;
	keyInfo->m_result = res;
	keyInfo->m_keyCount = count;
	
	return isNeedFetchNext(res, forward);
}



/**
 * 在该页面当中查找指定项
 * @pre	查找页面应该保证不为空
 * @param key			要寻找的记录键值
 * @param flag			查找标记
 * @param comparator	索引键值比较器
 * @param foundKey		OUT	用于保存找到的记录
 * @param keyInfo		IN/OUT	保存找到记录的位置信息
 * @return 表示上层是否还需要根据forward取逻辑上的下一项
 */
bool IndexPage::findKeyInPage(const SubRecord *key, SearchFlag *flag, KeyComparator *comparator, SubRecord *foundKey, KeyInfo *keyInfo) {
	assert(m_miniPageNum != 0);
	bool forward = flag->isForward();

	if (!IndexKey::isKeyValid(key)) {	// 比较键值为空
		getExtremeKey(foundKey, forward, keyInfo);
		return false;
	}

	u16 MPNo = keyInfo->m_miniPageNo = findMiniPageInPage(key, flag, comparator, foundKey, &(keyInfo->m_result));
	assert(keyInfo->m_result >= 0 || (MPNo == 0 && MPNo < m_miniPageNum));

	bool fetchNext = findKeyInMiniPage(key, keyInfo, flag, comparator, foundKey);

	if (MPNo != 0)
		keyInfo->m_keyCount = keyInfo->m_keyCount + getMPsItemCounts(MPNo);
	if (keyInfo->m_result > 0 && m_pageLevel != 0) {	// 非叶结点查找定位修正
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
 * 根据方向得到当前页面的第一项或者最后一项
 * @param key		返回的key键值
 * @param forward	true表示返回第一项，false返回最后一项
 * @keyInfo			out 返回找到项的信息
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
 * 取得指定偏移的后一项
 *
 * @param lastKey		前一个键值，用于拼凑当前键值
 * @param offset		要获取键值的起始偏移量
 * @param spanMiniPage	是否自动跨MiniPage取项
 * @param miniPageNo	in/out	当前所在的MiniPage号，如果该次获取记录跨MiniPage，该值被修改
 * @param foundKey		in/out	用来保存结果的记录
 * @param key		in/out	用来保存结果的记录
 * @return 下一个键值的起始偏移量，如果不允许跨越MiniPage并且指定MiniPage取尽或者整个页面取尽，返回传入的offset
 *			如果当前取项失败，record->m_size设置为0
 */
u16 IndexPage::getNextKey(const SubRecord *lastKey, u16 offset, bool spanMiniPage, u16 *miniPageNo, SubRecord *foundKey) {
	assert(lastKey == NULL || lastKey->m_format == KEY_COMPRESS);
	assert(*miniPageNo < m_miniPageNum);
	assert(offset < INDEX_PAGE_SIZE);

	byte *prefixData = (lastKey == NULL ? NULL : lastKey->m_data);

	foundKey->m_size = 0;
	// 判断该页面是否读取结束
	if (offset >= getDataEndOffset())
		return offset;

	// 判断当前是否定位到一个新的MiniPage
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
 * 取得指定偏移的前一项，如果当前miniPage已经用尽，根据指定读取前一个MiniPage，如果整个页面读完，返回空
 *
 * @param offset		上一条记录的偏移，取得是该偏移的前一条记录
 * @param spanMiniPage	是否自动跨MiniPage取项
 * @param miniPageNo	INOUT 当前读取键值所在的MiniPage号
 * @param foundKey		INOUT 保存前一键值的记录
 * @param key		INOUT 保存前一键值的记录
 * @return 前一个键值的起始偏移量，如果不允许跨越MiniPage并且指定MiniPage取尽或者整个页面取尽，返回传入的offset
 *			如果当前取项失败，record->m_size设置为0
 */
u16 IndexPage::getPrevKey(u16 offset, bool spanMiniPage, u16 *miniPageNo, SubRecord *foundKey) {
	// TODO: 寻求更高效的方式...
	assert(*miniPageNo < m_miniPageNum);
	assert(offset < INDEX_PAGE_SIZE);

	foundKey->m_size = 0;
	// 首先判断当前页是否用尽
	if (offset <= INDEXPAGE_DATA_START_OFFSET)
		return offset;

	// 其次判断是否用尽了当前Mini Page，如果是，取前一个Mini Page
	if (offset <= getMiniPageDataOffset(*miniPageNo)) {
		if (!spanMiniPage)
			return offset;
		assert(*miniPageNo > 0);
		*miniPageNo -= 1;
		assert(offset >= getMiniPageDataOffset(*miniPageNo));
	}

	// 开始在指定MiniPage当中定位前一项，而且结果肯定在当前Mini Page当中
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
 * 获取页面的第一项
 *
 * @param key		OUT	保存取到的项内容
 * @param keyInfo		OUT	保存键值位置信息
 */
void IndexPage::getFirstKey(SubRecord *key, KeyInfo *keyInfo) {
	keyInfo->m_miniPageNo = 0;
	keyInfo->m_sOffset = INDEXPAGE_DATA_START_OFFSET;
	keyInfo->m_eOffset = getNextKey(key, INDEXPAGE_DATA_START_OFFSET, true, &(keyInfo->m_miniPageNo), key);
}


/**
 * 获取页面的第一项
 *
 * @param key		OUT	保存取到的项内容
 * @param keyInfo		OUT	保存键值位置信息
 */
void IndexPage::getLastKey(SubRecord *key, KeyInfo *keyInfo) {
	keyInfo->m_miniPageNo = m_miniPageNum - 1;
	keyInfo->m_eOffset = getDataEndOffset();
	keyInfo->m_sOffset = getPrevKey(keyInfo->m_eOffset, true, &(keyInfo->m_miniPageNo), key);
}


/** 
 * 判断某个键值是不是一定在页面范围内，这里判断会参考键值比较结果
 * @pre 查找出的keyInfo必须和当前页面的版本一致，页面不该被修改过
 * @param keyInfo 键值信息
 */
bool IndexPage::isKeyInPageMiddle(KeyInfo *keyInfo) {
	return !((keyInfo->m_sOffset <= INDEXPAGE_DATA_START_OFFSET && keyInfo->m_result < 0) ||
		(keyInfo->m_eOffset >= getDataEndOffset() && keyInfo->m_result > 0));
}


/**
 * 根据查找方向和条件比较两个键值的大小
 * @param key			要查找的键值
 * @param indexKey		索引上读取的键值
 * @param flag			比较标记
 * @param comparator	索引键值比较器
 * @return 返回key和indexKey的关系，key大于等于小于indexKey，分别用1/0/-1表示
 */
s32 IndexPage::compareKey(const SubRecord *key, const SubRecord *indexKey, SearchFlag *flag, KeyComparator *comparator) const {
	bool includeKey = flag->isIncludingKey();
	bool forward = flag->isForward();
	bool equalAllowable = flag->isEqualAllowed();

	s32 result = comparator->compareKey(key, indexKey);

	/**************************************************************************************/
	/* 操作符		includeKey	forward		compare result: 1	0/0	(equalAllowable)	-1
		===================================================================================
		>=				1			1					|	1	-1/0					-1
		=				1			1					|	1	-1/0					-1
		>				0			1					|	1	1/0						-1
		<				0			0					|	1	-1/0					-1
		<=				1			0					|	1	1/0						-1
		===================================================================================
		如果允许相等，对于比较结果为0，直接返回0
	*/
	/**************************************************************************************/
	return result != 0 ? result : equalAllowable ? 0 : (includeKey ^ forward) ? 1 : -1;
}


/**
 * 预判断一个页面删除了指定键值之后是否需要SMO，需要预判的情况都是叶页面
 * @param deleteKey			要删除的键值
 * @param prevKey			out 保存删除键值的前项，如果没有要保证m_size = 0
 * @param nextKey			out 保存删除键值的后项，如果没有要保证m_size = 0
 * @param keyInfo			要删除键值的键值信息
 * @param prevKeySOffset	out 返回前一键值的起始偏移量，如果没有，返回keyInfo->m_eOffset
 * @param nextKeyEOffset	out 返回后一键值的结束偏移量，如果没有，返回keyInfo->m_sOffset
 * @return true表示需要合并，false表示不需要合并
 */
bool IndexPage::prejudgeIsNeedMerged(const SubRecord *deleteKey, SubRecord *prevKey, SubRecord *nextKey, KeyInfo keyInfo, u16 *prevKeySOffset, u16 *nextKeyEOffset) {
	assert(isPageLeaf());
	*prevKeySOffset = getPrevKey(keyInfo.m_sOffset, false, &(keyInfo.m_miniPageNo), prevKey);
	*nextKeyEOffset = getNextKey(deleteKey, keyInfo.m_eOffset, false, &(keyInfo.m_miniPageNo), nextKey);

	// 分情况计算删除本键值之后页面剩余的数据量
	if (!IndexKey::isKeyValid(nextKey)) {
		// 需要判断当前MiniPage是不是同时需要整个删除
		u16 deleteMPDirSize = getMiniPageItemCount(keyInfo.m_miniPageNo) == 1 ? MINI_PAGE_DIR_SIZE : 0;
		return (INDEX_PAGE_SIZE - m_freeSpace - (keyInfo.m_eOffset - keyInfo.m_sOffset) - deleteMPDirSize <= INDEXPAGE_DATA_MIN_SIZE && m_nChance <= 1) || m_pageCount == 1;	// 只剩一项删除后必须要合并
	}

	assert(*nextKeyEOffset > keyInfo.m_eOffset);
	u16 needSpace = IndexKey::computeSpace(prevKey, nextKey, false);
	return INDEX_PAGE_SIZE - m_freeSpace - (*nextKeyEOffset - keyInfo.m_sOffset - needSpace) <= INDEXPAGE_DATA_MIN_SIZE && m_nChance <= 1;
}



/**
 * 删除页面内指定的一项
 *
 * @param session		会话句柄
 * @param logger		日志记录器
 * @param pageId		页面ID
 * @param prevKey		用来保存删除项前一项的记录空间
 * @param deleteKey		要删除的项（必须已经取过）
 * @param nextKey		用来保存删除项后一项的记录空间
 * @param keyInfo		要删除项在页面当中位置信息
 * @param siblingGot	表示删除键值的前后向是否已经读取过，默认为false
 * @param prevKeySOffset前项的键值起始信息，如果siblingGot为真，该值需要传入，默认为0
 * @param nextKeyEOffset后项的键值结束信息，如果siblingGot威震，该值需要传入，默认为INDEX_PAGE_SIZE
 * @return 当前页面是否需要SMO，true/false
 */
bool IndexPage::deleteIndexKey(Session *session, IndexLog *logger, PageId pageId, SubRecord *prevKey, const SubRecord *deleteKey, SubRecord *nextKey, KeyInfo keyInfo, bool siblingGot, u16 prevKeySOffset, u16 nextKeyEOffset) {
	assert(m_pageCount > 0);

	if (getMiniPageItemCount(keyInfo.m_miniPageNo) == 1) {
		// 所在MiniPage只有一项，删除整个MiniPage
		deleteMiniPage(session, logger, pageId, keyInfo.m_miniPageNo);
		return isNeedMerged();
	}

	u16 sOffset = keyInfo.m_sOffset, eOffset = keyInfo.m_eOffset;
	u16 nextEOffset = 0, prevSOffset = 0;
	// 首先取得前后项的键值，miniPageNo始终都不变
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
	if (IndexKey::isKeyValid(nextKey)) {	// 存在后项，跟前项进行压缩保存
		// 这个情况下即使后项压缩率降低，也不会使用更多的空间，删除键值的空间肯定足够填充
		byte *start = (byte*)this + sOffset;
		IndexKey *indexKey = (IndexKey*)start;
		nextKeyNewSpace = (u16)(indexKey->compressAndSaveKey(prevKey, nextKey, !isPageLeaf()) - start);
		// 移动后续空间
		assert(start + nextKeyNewSpace <= (byte*)this + nextEOffset);
		shiftDataByMOVE(start + nextKeyNewSpace, (byte*)this + nextEOffset, moveSize);
		// 计算空间变化
		assert(nextEOffset >= sOffset + nextKeyNewSpace);
		deltaSize = (nextEOffset - sOffset) - nextKeyNewSpace;
	} else {	// 不存在后项，直接删除当前项
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
	// 调整MiniPage目录以及页面相关信息
	adjustMiniPageDataOffset(keyInfo.m_miniPageNo + 1,  0 - deltaSize);
	m_freeSpace = m_freeSpace + deltaSize;
	decMiniPageItemCount(keyInfo.m_miniPageNo);
	m_pageCount--;
	m_nChance > 0 ? --m_nChance : m_nChance;
	memset((byte*)this + getDataEndOffset(), 0, deltaSize);

	return isNeedMerged();
}



/**
 * 删除指定的MiniPage
 * @param session		会话句柄
 * @param logger		日志记录器
 * @param pageId		页面ID
 * @param miniPageNo	指定的MiniPage号
 * @attention 返回之后页面m_freeSpace和m_dirStart已经调整
 */
void IndexPage::deleteMiniPage(Session *session, IndexLog *logger, PageId pageId, u16 miniPageNo) {
	assert(miniPageNo < m_miniPageNum);
	u16 miniPageEnd = (miniPageNo < m_miniPageNum - 1) ? getMiniPageDataOffset(miniPageNo + 1) : getDataEndOffset();
	u16 miniPageStart = getMiniPageDataOffset(miniPageNo);

	// 记录删除MiniPage逻辑日志
#ifdef NTSE_TRACE
	u64 oldLSN = m_lsn;
#endif
	m_lsn = logger->logPageDeleteMP(session, IDX_LOG_DELETE_MP, pageId, (byte*)this + miniPageStart, miniPageEnd - miniPageStart, miniPageNo, m_lsn);
	ftrace(ts.idx, tout << "deleteMP: pageId: " << pageId << " MPNo: " << miniPageNo << " oldLSN: " << oldLSN << " newLSN: " << m_lsn);

	deleteMiniPageIn(miniPageNo, miniPageStart, miniPageEnd);
}


/**
 * 删除MiniPage真实操作，操作不记录日志
 * @param miniPageNo	要删除MP号
 * @param miniPageStart	要删除MP起始偏移
 * @param miniPageEnd	要删除MP结束偏移
 */
void IndexPage::deleteMiniPageIn(u16 miniPageNo, u16 miniPageStart, u16 miniPageEnd) {
	ftrace(ts.idx, tout << miniPageNo << miniPageStart << miniPageEnd);

	assert(miniPageNo < m_miniPageNum);
	u16 deleteMiniPageSize =  miniPageEnd - miniPageStart;
	u16 dataLastPoint = getDataEndOffset();
	assert(deleteMiniPageSize > 0);
	assert(dataLastPoint >= miniPageEnd);

	// 移动页面数据
	shiftDataByMOVE((byte*)this + miniPageStart, (byte*)this + miniPageEnd, dataLastPoint - miniPageEnd);
	// 删除MiniPage目录
	deleteMiniPageDir(miniPageNo);
	// 更新后续MiniPage目录
	adjustMiniPageDataOffset(miniPageNo, 0 - deleteMiniPageSize);

	// 标记页面信息
	m_freeSpace = m_freeSpace + deleteMiniPageSize;
	m_pageCount--;
	m_nChance > 0 ? --m_nChance : m_nChance;

	memset((byte*)this + getDataEndOffset(), 0, deleteMiniPageSize);
}



/**
 * 删除指定MiniPage目录
 *
 * @param miniPageNo	项目录号
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
 * 调整一段MiniPage目录表示的偏移量，从某个目录开始到页面最后一个目录结束
 * 传入的偏移量在内部调整始终做加法
 *
 * @param startMiniPageNo	要调整的起始MiniPage目录
 * @param deltaOffset		要调整的偏移量
 */
void IndexPage::adjustMiniPageDataOffset(u16 startMiniPageNo, s32 deltaOffset) {
	if (m_miniPageNum < startMiniPageNo + 1)
		return;

	for (u16 no = startMiniPageNo; no < m_miniPageNum; no++)
		adjustMiniPageDataStart(no, deltaOffset);
}



/**
 * 将指定的页面合并到本页面，指定页面逻辑顺序在本页面之前。这里合并页面只需要简单的将前面页面的MiniPage全部移到当前页面最前面，不做合并优化
 *
 * @pre	调用者应该首先保证两个页面可以合并不会空间溢出
 * @post 属于SMO操作，m_smoLSN会被重设
 * @param session		会话句柄
 * @param logger		日志记录器
 * @param leftPage		要合并的页面
 * @param pageId		当前页ID
 * @param leftPageId	要合并页面ID
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

	// 记录SMO合并逻辑日志
	u64 lsn = logger->logSMOMerge(session, IDX_LOG_SMO_MERGE, pageId, leftPageId, leftPage->m_prevPage, (byte*)leftPage + INDEXPAGE_DATA_START_OFFSET, prevDataSize, (byte*)leftPage + leftPage->m_dirStart, prevDirSize, this->m_lsn, leftPage->m_lsn);
	m_lsn = lsn;
	leftPage->m_lsn = lsn;
	leftPage->m_smoLSN = lsn;

	// 拷贝调整页面内容
	shiftDataByMOVE((byte*)this + newStart, (byte*)this + INDEXPAGE_DATA_START_OFFSET, dataSize);
	shiftDataByCPY((byte*)this + INDEXPAGE_DATA_START_OFFSET, (byte*)leftPage + INDEXPAGE_DATA_START_OFFSET, prevDataSize);
	shiftDirByCPY((byte*)this + m_dirStart - prevDirSize, (byte*)leftPage + leftPage->m_dirStart, prevDirSize);

	// 修改页面头信息
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
 * 在该页面当中查找指定项
 *
 * @param key			要寻找的记录键值
 * @param comparator	索引键值比较器
 * @param prev			OUT	用于保存寻找记录的前驱，没有则m_size=0(prev<key)
 * @param next			OUT	用于保存寻找记录的后继，没有则m_size=0(next>=key)
 * @param keyInfo		IN/OUT	保存next的位置信息
 */
void IndexPage::findKeyInPageTwo(const SubRecord *key, KeyComparator *comparator, SubRecord **prev, SubRecord **next, KeyInfo *keyInfo) {
	assert(key != NULL);

	if (m_miniPageNum == 0) {	// 页面为空
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
 * 在指定的Mini Page当中寻找指定的记录，返回取得的记录内容，记录的偏移量
 *
 * @param key			要寻找的记录键值
 * @param keyInfo		IN/OUT	保存next的位置信息
 * @param comparator	索引键值比较器
 * @param prev			OUT	用于保存寻找记录的前驱，没有则m_size=0(prev<key)
 * @param next			OUT	用于保存寻找记录的后继，没有则m_size=0(next>=key)
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
		if (res < 0)	// 找到相等或者更大的项，表示查找成功
			goto Finish;

		count++;
	}

	// 该Mini Page遍历结束，查找项比较大，不存在后项
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
 * 在页面添加索引项
 * @pre 传入的insertKey应该和比较器比较函数匹配，可以用于最优化查找，如果insertKey已经是压缩格式，cpInsertKey可以为NULL
 *
 * @param session		会话句柄
 * @param logger		日志记录器
 * @param pageId		页面ID
 * @param insertKey		要插入的键值
 * @param cpInsertKey	压缩格式的插入键值，如果insertKey就是压缩格式的，该值为NULL
 * @param idxKey1		充当临时变量的键值，表示插入项的前项
 * @param idxKey2		充当临时变量的键值，表示插入项的后项
 * @param comparator	索引键值比较器
 * @param keyInfo		IN/OUT 传入之后根据keyInfo->m_sOffset决定是否需要查找插入键值位置,
						返回插入键值位置信息，无论是否插入成功，keyInfo->m_sOffset必须被设置
 * @return 键值插入的结果，如果返回INSERT_SUCCESS，表示插入成功；返回NEED_SPLIT表示需要分裂页面没有改变
 */
IndexInsertResult IndexPage::addIndexKey(Session *session, IndexLog *logger, PageId pageId, const SubRecord *insertKey, const SubRecord *cpInsertKey, SubRecord *idxKey1, SubRecord *idxKey2, KeyComparator *comparator, KeyInfo *keyInfo) {
	vecode(vs.idx, this->traversalAndVerify(NULL, NULL, NULL, NULL, NULL));

	if (keyInfo->m_sOffset == 0) {	// 如果不为0表示当前键值已经查找过了
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
			// 临界状况，尝试判断插入在哪个MiniPage更有前途
			assert(!IndexKey::isKeyValid(idxKey2) && keyInfo->m_result == 1);
			offset1 = getNextKey(idxKey2, eOffset, true, &nextMPNo, idxKey2);
		}
	}

	const SubRecord *compressKey = cpInsertKey == NULL ? insertKey : cpInsertKey;	// 使用压缩格式计算空间和插入

	{	// 裁决如何插入能最优化
		commonPrefix1 = IndexKey::computePrefix(compressKey, idxKey1);
		commonPrefix2 = IndexKey::computePrefix(compressKey, idxKey2);

		// 根据和前后项的前缀压缩情况，决定采用哪种压缩方式
		if (commonPrefix1 == 0 && commonPrefix2 == 0) {
			addNewMiniPage = true;
			if (IndexKey::isKeyValid(idxKey1) && !metMPEnd) {	// 需要分裂当前MiniPage，插入项再独自创建新的MiniPage
				if (splitMiniPageReal(session, logger, pageId, idxKey2, sOffset, eOffset, miniPageNo, lowerCount) == NEED_SPLIT)
					return NEED_SPLIT;
				newMiniPageNo = nextMPNo;
			} else
				newMiniPageNo = (metMPEnd && m_miniPageNum != 0) ? nextMPNo : miniPageNo;	// 确认新添加MiniPage的序号，注意空页面的情况
		} else if (needCheck) {
			if (commonPrefix1 < commonPrefix2 && !isMiniPageFull(nextMPNo)) {
				// 当和后一个MiniPage压缩效果更好，并且后一个MP不满，选择插入在下一个
				eOffset = offset1;
				miniPageNo = nextMPNo;
				idxKey1->m_size = 0;
				commonPrefix1 = 0;
			} else if (commonPrefix1 == 0) {	// 与前项不可压缩，且后一个MP已满，选择单独生成一个MP
				addNewMiniPage = true;
				newMiniPageNo = nextMPNo;
			} else {	// 和前项可以压缩，要插在当前MP，后项无效
				idxKey2->m_size = 0;
			}
		}
	}

	if (addNewMiniPage) {	// 插入的键值无法压缩，建立新MiniPage
		u16 needSpace = IndexKey::computeSpace(compressKey, (u16)0, !isPageLeaf()) + MINI_PAGE_DIR_SIZE;
		if (!isPageEnough(needSpace))
			return NEED_SPLIT;

		addMiniPage(session, logger, pageId, compressKey, newMiniPageNo);
		keyInfo->m_miniPageNo = newMiniPageNo;
		keyInfo->m_eOffset = keyInfo->m_sOffset + needSpace - MINI_PAGE_DIR_SIZE;

		vecode(vs.idx, this->traversalAndVerify(NULL, NULL, NULL, NULL, NULL));

		return INSERT_SUCCESS;
	}

	if (isMiniPageFull(miniPageNo)) {		// 当前MiniPage已满，尝试分裂
		if (splitMiniPageFull(session, logger, pageId, idxKey1, idxKey2, miniPageNo) == NEED_SPLIT)
			return NEED_SPLIT;

		vecode(vs.idx, this->traversalAndVerify(NULL, NULL, NULL, NULL, NULL));

		keyInfo->m_sOffset = 0;
		return addIndexKey(session, logger, pageId, insertKey, cpInsertKey, idxKey1, idxKey2, comparator, keyInfo);	// 这里只可能被调用一次，不可能出现无限递归的情况
	}

	// 计算所需空间，至此eOffset和sOffset分别表示后项的结束和起始位置，如果后项不存在，两项相等
	bool isPageIdExist = !isPageLeaf();
	u16 needSpace = IndexKey::computeSpace(compressKey, commonPrefix1, isPageIdExist);
	keyInfo->m_eOffset = keyInfo->m_sOffset + needSpace;
	if (IndexKey::isKeyValid(idxKey2))
		needSpace = needSpace + IndexKey::computeSpace(idxKey2, commonPrefix2, isPageIdExist);
	assert(needSpace > (eOffset - sOffset));
	needSpace -= (eOffset - sOffset);
	if (m_freeSpace < needSpace)	// 空间不够需要SMO
		return NEED_SPLIT;

	// 保证可以插入之后首先更新插入偏移百分比
	if (m_pageCount != 0)
		updateInsertOffsetRatio(sOffset);
	byte oldStatus[INDEX_PAGE_SIZE];
	memcpy(oldStatus, (byte*)this + sOffset, eOffset - sOffset);
	// 移动数据腾出空间插入
	byte *start = (byte*)this + sOffset;
	shiftDataByMOVE(start + needSpace, start, getDataEndOffset() - sOffset);

	// 插入数据
	IndexKey *indexKey = (IndexKey*)start;
	indexKey = (IndexKey*)indexKey->compressAndSaveKey(compressKey, commonPrefix1, isPageIdExist);
	if (IndexKey::isKeyValid(idxKey2))
		indexKey->compressAndSaveKey(idxKey2, commonPrefix2, isPageIdExist);

	// 记录逻辑插入日志
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
 * 初始化新页面并分裂指定页面，分裂结果将使得本页面的后半个页面的内容存放到指定新页面当中
 * 前提是使用者持有本页面和新页面的X-Latch
 *
 * 这里的分裂主要采用智能分裂的方式，智能分裂系数如果上层指定了就按照上层指定的，
 * 否则就按照索引该页面插入的一个平均偏移量来决定，该系数直接决定页面分裂点的起始位置
 * 而位于分裂点之后的数据将会被移动到右边的页面
 *
 * @post 特别注意，newPage在这里会被初始化，外部任何初始化都会被覆盖
 * @post 属于SMO操作，m_smoLSN会被重设
 * @param session		会话句柄
 * @param logger		日志记录器
 * @param newPage		参与分裂的新页面
 * @param pageId		本页面的ID号
 * @param newPageId		新页面ID号
 * @param splitFactor	指定的分裂系数，如果该系数等于0，则采用索引定义的智能分裂系数
 * @param insertPos		分裂之后插入点
 * @param reserveSize	分裂之后插入点之后必须保证有如此多的空闲空间
 * @param splitKey		分裂遍历使用的临时键值
 */
void IndexPage::splitPage(Session *session, IndexLog *logger, IndexPage *newPage, PageId pageId, PageId newPageId, u8 splitFactor, u16 insertPos, u16 reserveSize, SubRecord *splitKey) {
	ftrace(ts.idx, tout << this << pageId << newPageId << newPageId << splitFactor << insertPos << reserveSize);
	assert(newPage != NULL);
	assert(m_pageCount > 1);

	// 初始化新页面
	newPage->initPage(session, logger, true, false, newPageId, m_pageMark, (IndexPageType)m_pageType, m_pageLevel, pageId, m_nextPage);

	// 计算合适的分裂点
	u16 splitMPNo;
	u8 mpLeftCount;
	u16 splitOffset = getSplitOffset((splitFactor == 0 ? m_insertOffsetRatio : splitFactor * 100), insertPos, reserveSize, splitKey, &splitMPNo, &mpLeftCount);

	// 计算分裂相关信息
	u8 mpMoveCount = getMiniPageItemCount(splitMPNo) - mpLeftCount;
	u16 leftMPNum = splitMPNo + (mpLeftCount > 0 ? 1 : 0);
	u16 moveMPNum = m_miniPageNum - splitMPNo - (mpMoveCount > 0 ? 0 : 1);
	u16 splitKeyCLen = ((IndexKey*)((byte*)this + splitOffset))->skipAKey();
	u16 moveData = splitOffset + splitKeyCLen;
	u16 dataSize = getDataEndOffset() - moveData;
	u16 moveDirSize = moveMPNum * MINI_PAGE_DIR_SIZE;

	byte oldKey[INDEX_PAGE_SIZE];
	memcpy(&oldKey, (IndexKey*)((byte*)this + splitOffset), splitKeyCLen);

	// 移动数据和项目录
	IndexKey *indexKey = (IndexKey*)((byte*)newPage + INDEXPAGE_DATA_START_OFFSET);
	u16 offset = (u16)(indexKey->compressAndSaveKey(splitKey, (u16)0, !isPageLeaf()) - ((byte*)newPage + INDEXPAGE_DATA_START_OFFSET));
	shiftDataByCPY((byte*)newPage + INDEXPAGE_DATA_START_OFFSET + offset, (byte*)this + moveData, dataSize);
	newPage->m_dirStart = INDEX_PAGE_SIZE - MINI_PAGE_DIR_SIZE * moveMPNum;
	shiftDirByCPY((byte*)newPage + newPage->m_dirStart, (byte*)this + newPage->m_dirStart, MINI_PAGE_DIR_SIZE * moveMPNum);

	// 调整新页面的项目录
	newPage->m_miniPageNum = moveMPNum;
	newPage->setMiniPageDataStart(0, INDEXPAGE_DATA_START_OFFSET);
	if (newPage->m_miniPageNum > 1) {
		s32 diff = offset - splitKeyCLen - (splitOffset - INDEXPAGE_DATA_START_OFFSET);
		newPage->adjustMiniPageDataOffset(1, diff);
	}

	// 记录SMO逻辑日志
	u64 lsn = logger->logSMOSplit(session, IDX_LOG_SMO_SPLIT, pageId, newPageId, m_nextPage, (byte*)this + moveData, dataSize, oldKey, splitKeyCLen, (byte*)newPage + INDEXPAGE_DATA_START_OFFSET, offset, (byte*)newPage + newPage->m_dirStart, moveDirSize, mpLeftCount, mpMoveCount, m_lsn, newPage->m_lsn);
	m_lsn = lsn;
	newPage->m_lsn = lsn;

	// 清理原始页面内容和项目录
	memset((byte*)this + splitOffset, 0, getDataEndOffset() - splitOffset);
	u16 leftDirOffset = INDEX_PAGE_SIZE - MINI_PAGE_DIR_SIZE * leftMPNum;
	shiftDirByMOVE((byte*)this + leftDirOffset, (byte*)this + m_dirStart, INDEX_PAGE_SIZE - leftDirOffset);
	memset((byte*)this + m_dirStart, 0, leftDirOffset - m_dirStart);
	m_dirStart = leftDirOffset;

	// 修正两个页面相关数据
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
 * 计算页面开始到某个MiniPage之前的总项数(不包括指定的MP)，根据MiniPage目录的信息计算
 * @param 要统计的最大miniPageNo号
 * @return 页面包含索引键个数
 */
u16 IndexPage::getMPsItemCounts(u16 miniPageNo) {
	assert(m_miniPageNum >= miniPageNo);
	u16 keyCounts = 0;

	for (u16 no = 0; no < miniPageNo; no++)
		keyCounts = keyCounts + getMiniPageItemCount(no);

	return keyCounts;
}


/**
 * 根据参数决定页面从哪个偏移点开始分裂
 * @param splitFactor	分裂系数
 * @param insertPos		导致分裂的插入数据插入点
 * @param reserveSize	插入点后即将插入的数据数
 * @param splitKey		用于临时键值保存分裂点第一条记录的内容
 * @param miniPageNo	out 分裂点所在的MiniPage号
 * @param mpLeftCount	out 分裂的MiniPage留在本页面的项数
 * @return 分裂点的页内偏移
 */
u16 IndexPage::getSplitOffset(u16 splitFactor, u16 insertPos, u16 reserveSize, SubRecord *splitKey, u16 *miniPageNo, u8 *mpLeftCount) {
	u16 splitOffset = 0;
	// 计算大致分裂点
	if (splitFactor > IndexPage::INDEXPAGE_MAX_SPLIT_FACTOR)
		splitFactor = IndexPage::INDEXPAGE_MAX_SPLIT_FACTOR;
	if (splitFactor < IndexPage::INDEXPAGE_MIN_SPLIT_FACTOR)
		splitFactor = IndexPage::INDEXPAGE_MIN_SPLIT_FACTOR;
	s16 overRecord = 0;
	u16 expectOffset = (u16)((m_dirStart - INDEXPAGE_DATA_START_OFFSET) * (splitFactor / 100.0) / 100) + INDEXPAGE_DATA_START_OFFSET;
	if (expectOffset >= getDataEndOffset())	// 这里要保证原页面肯定会有数据移动到新页面
		expectOffset = getDataEndOffset() - 1;

	// 更加严格的计算剩余空间，项目录空间被不考虑，因此可能出现计算不够精确的情况
	// 最保险的应该是限制键值长度来保证不会出现空间不够的情况
	// 使用expectOffset和overRecord联合控制真实分裂点位置，
	// expectOffset表示的位置可能从任何一条记录起点到它的终点，
	// overRecord如果-1，表示从这条记录的头开始分裂，
	// overRecord如果1，表示从尾开始分裂，如果是0，表示应该正好
	// 同时无论如何，至少要移动1项到新页面
	u16 indexPageInitFree = INDEX_PAGE_SIZE - INDEXPAGE_INIT_MAX_USAGE;
	if (expectOffset > insertPos && m_dirStart - expectOffset < reserveSize + indexPageInitFree) {	// 调整，需要多移动到新页面，甚至可能需要把键值插入到新页面
		if (m_dirStart - reserveSize - indexPageInitFree < insertPos) {	// 如果插入点要移动到新页面，只移动插入点之后的数据到新页面，保证插入点之前的数据量大于reserveSize
			assert(insertPos - INDEXPAGE_DATA_START_OFFSET > reserveSize);
			// 分裂点就是插入点
			expectOffset = insertPos;
			overRecord = 0;
		} else {
			// 移动m_dirStart - reserveSize - INDEXPAGE_INIT_MAX_USAGE这段数据到新页面，同时插入点还是留在原来页面，数据在原来页面插入
			expectOffset = m_dirStart - reserveSize - indexPageInitFree;
			overRecord = -1;
			// 调整之后insertPos可能大于等于expectOffset, 这时候插入到新页面，overRecord仍旧是-1才对
		}
	} else if (expectOffset <= insertPos && expectOffset - INDEXPAGE_DATA_START_OFFSET + m_freeSpace < reserveSize + indexPageInitFree) {	// 调整，需要少移动一些，可能导致插入在旧页面
		if (reserveSize + indexPageInitFree - m_freeSpace + INDEXPAGE_DATA_START_OFFSET < insertPos) {
			// 插入点还是可以移动到新页面，移动的点扩大到reserveSize + INDEXPAGE_INIT_MAX_USAGE - m_freeSpace + INDEXPAGE_DATA_START_OFFSET;
			expectOffset = reserveSize + indexPageInitFree - m_freeSpace + INDEXPAGE_DATA_START_OFFSET;
			overRecord = 1;
		} else {
			// 插入的点要保留在原始页面，分裂点从插入点后一条记录开始，同时需要保证原始页面足够插入
			assert(m_dirStart - insertPos + 1 > reserveSize);
			overRecord = 1;
			expectOffset = insertPos + 1;
		}
	}
	assert(expectOffset < getDataEndOffset());

	// 遍历MiniPage到达分裂点所在的页
	u16 splitMPNo = 0;
	for (; splitMPNo < m_miniPageNum - 1; ++splitMPNo) {
		if (getMiniPageDataOffset(splitMPNo + 1) > expectOffset)
			break;
	}
	// 顺序遍历得到真正分裂的键值起始位置并得到键值
	u8 leftMPCount = 0;
	u16 start = getMiniPageDataOffset(splitMPNo);
	u16 end = (splitMPNo == m_miniPageNum - 1) ? getDataEndOffset() : getMiniPageDataOffset(splitMPNo + 1);
	// 首先遍历到那条跨越expectOffset的键值
	while (start < end) {
		splitOffset = start;
		IndexKey *indexKey = (IndexKey*)((byte*)this + start);
		start = start + indexKey->extractKey(splitKey->m_data, splitKey, !isPageLeaf());
		if (start >= expectOffset)	// 这里的>=表示如果expectOffset刚好时两个键值的连接点，它归属于后一个键值
			break;
		++leftMPCount;	// leftMPCount只计算到不需要靠overRecord来调整的键值那项
	}

	// 首先根据splitOffset调整overRead，现在splitOffset表示的是跨expectOffset记录的起始位置，start表示的是结束位置
	if (splitOffset > insertPos) {	// 预定项在原有页面插入
		if (m_dirStart - splitOffset < reserveSize + indexPageInitFree)
			overRecord = -1;
	} else {	// 插入在新页面
		if (splitOffset - INDEXPAGE_DATA_START_OFFSET + m_freeSpace < reserveSize + indexPageInitFree)
			overRecord = 1;
		if (splitOffset == insertPos && m_dirStart - start < reserveSize + indexPageInitFree)	// 是一个临界状态，判断插入项到底在哪个页面比较合适
			overRecord = 0;
	}

	// 再根据overRecord做调整
	if (overRecord == 1 && start < getDataEndOffset()) {	// 取下一项
		if (start == end) {	// 取下一个MiniPage，并且不会碰到页面尾
			leftMPCount = 0;
			splitMPNo++;
		} else
			leftMPCount++;
		splitOffset = start;
		IndexKey *indexKey = (IndexKey*)((byte*)this + start);
		indexKey->extractKey(splitKey->m_data, splitKey, !isPageLeaf());
	}

	// TODO:确保插入空间足够，可能某些情况该断言可能会失败，但是不应该影响分裂正确性
	assert(splitOffset < getDataEndOffset());
	assert((insertPos < splitOffset && m_dirStart - splitOffset >= reserveSize + indexPageInitFree) ||
			(insertPos >= splitOffset && splitOffset - INDEXPAGE_DATA_START_OFFSET + m_freeSpace >= reserveSize + indexPageInitFree));

	*miniPageNo = splitMPNo;
	*mpLeftCount = leftMPCount;

	return splitOffset;
}



/**
 * 将要插入的键值作为单独的MiniPage插入，生成新的MiniPage
 *
 * @pre	调用者必须保证页面空间足够
 * @param session		会话句柄
 * @param logger		日志记录器
 * @param pageId		页面ID
 * @param insertKey		要插入的索引键值
 * @param newMiniPageNo	生成的新MiniPage的编号
 */
void IndexPage::addMiniPage(Session *session, IndexLog *logger, PageId pageId, const SubRecord *insertKey, u16 newMiniPageNo) {
	assert(newMiniPageNo <= m_miniPageNum);

	u16 start = newMiniPageNo == 0 ? INDEXPAGE_DATA_START_OFFSET :
		newMiniPageNo < m_miniPageNum ? getMiniPageDataOffset(newMiniPageNo) :
		getDataEndOffset();

	// 增加项目录信息
	addMiniPageDir(newMiniPageNo, start, 1);
	// 插入键值
	u16 needSpace = addFirstKeyInMiniPage(insertKey, start);
	assert(m_freeSpace >= needSpace);

	// 记录增加MiniPage逻辑日志
#ifdef NTSE_TRACE
	u64 oldLSN = m_lsn;
#endif
	m_lsn = logger->logPageAddMP(session, IDX_LOG_ADD_MP, pageId, (byte*)this + start, needSpace, newMiniPageNo, m_lsn);
	ftrace(ts.idx, tout << "AddMP: pageId: " << pageId << " MPNo: " << newMiniPageNo << " oldLSN: " << oldLSN << " newLSN: " << m_lsn);

	// 调整影响到的项目录信息和页面信息
	if (m_pageCount != 0)
		updateInsertOffsetRatio(start);
	adjustMiniPageDataOffset(newMiniPageNo + 1, needSpace);
	m_freeSpace = m_freeSpace - needSpace;
	m_pageCount++;
}


/**
 * 在页面内增加一个MiniPage项目录信息
 *
 * @post 页面m_dirStart/m_miniPageNum/m_freeSpace被修改，同时MiniPage项目录内容被置位
 * @param newMiniPageNo	新的MiniPage号
 * @param dataStart		项目录指向数据地址起始偏移
 * @param itemCounts	MiniPage包含项数
 */
void IndexPage::addMiniPageDir(u16 newMiniPageNo, u16 dataStart, u16 itemCounts) {
	ftrace(ts.idx, tout << newMiniPageNo << dataStart << itemCounts);
	assert(itemCounts != 0);
	assert(newMiniPageNo <= m_miniPageNum);
	// 插入新的项目录
	byte *miniPageStart = (byte*)this + m_dirStart;
	byte *miniPageNewStart = miniPageStart - MINI_PAGE_DIR_SIZE;
	shiftDataByMOVE(miniPageNewStart, miniPageStart, MINI_PAGE_DIR_SIZE * (newMiniPageNo));
	// 修改增加项目录后信息
	m_dirStart -= MINI_PAGE_DIR_SIZE;
	m_miniPageNum++;
	m_freeSpace -= MINI_PAGE_DIR_SIZE;
	// 初始化项目录
	setMiniPageDataStart(newMiniPageNo, dataStart);
	setMiniPageItemCount(newMiniPageNo, itemCounts);
}


/**
 * 在指定项目录插入第一个键值
 *
 * @param insertKey	插入的键值
 * @param dataStart	插入数据的起点
 * @return 插入键值占用的空间
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
 * 分裂指定MiniPage
 *
 * @pre	必须MiniPage包含索引项达到最大限制才调用该分裂函数
 * @param session		会话句柄
 * @param logger		日志记录器
 * @param pageId		页面ID
 * @param idxKey1		临时键值
 * @param idxKey2		临时键值
 * @param miniPageNo	要分裂的MiniPage号
 * @return 返回分裂成功或者分裂后空间不足需要分裂整个页面：INSERT_SUCCESS/NEED_SPLIT
 */
IndexInsertResult IndexPage::splitMiniPageFull(Session *session, IndexLog *logger, PageId pageId, SubRecord *idxKey1, SubRecord *idxKey2, u16 miniPageNo) {
	ftrace(ts.idx, tout << pageId << miniPageNo);
	assert(getMiniPageItemCount(miniPageNo) >= 2);

	// 需要分裂MiniPage，遍历页面，从页面中间项分裂
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
 * 真正进行MP分裂的函数
 *
 * @调用前上层应该计算好要分裂点的下一个键值以及相关偏移量
 * @param session		会话句柄
 * @param logger		日志记录器
 * @param pageId		页面ID
 * @param next			分裂点的下一个键值
 * @param sOffset		键值的起始偏移量
 * @param eOffset		键值的结束偏移量
 * @param miniPageNo	要分裂的MP号
 * @param leftItem		分裂留下MP保存的键值个数
 * @return 返回分裂成功INSERT_SUCCESS或者分裂失败NEED_SPLIT
 */
IndexInsertResult IndexPage::splitMiniPageReal(Session *session, IndexLog *logger, PageId pageId, const SubRecord *next, u16 sOffset, u16 eOffset, u16 miniPageNo, u16 leftItem) {
	bool isPageIdExist = !isPageLeaf();
	u16 needSpace = IndexKey::computeSpace(NULL, next, isPageIdExist) - (eOffset - sOffset);
	if (needSpace + MINI_PAGE_DIR_SIZE > m_freeSpace)	// 剩余空间不够分裂一个MiniPage
		return NEED_SPLIT;

	u16 oldValueLen = eOffset - sOffset;
	byte *oldValue = (byte*)alloca(oldValueLen);
	shiftDataByCPY(oldValue, (byte*)this + sOffset, oldValueLen);

	// 分裂生成新的MiniPage
	addMiniPageDir(miniPageNo + 1, sOffset, getMiniPageItemCount(miniPageNo) - leftItem);

	// 修改其他MiniPage信息
	setMiniPageItemCount(miniPageNo, leftItem);
	adjustMiniPageDataOffset(miniPageNo + 2, needSpace);

	// 移动后续数据，解压插入分裂点键值
	byte *pos = (byte*)this + eOffset;
	shiftDataByMOVE((byte*)(pos + needSpace), pos, getDataEndOffset() - eOffset);

	IndexKey *indexKey = (IndexKey*)((byte*)this + sOffset);
	indexKey->compressAndSaveKey(next, (u16)0, isPageIdExist);

	assert(m_freeSpace >= needSpace);
	m_freeSpace = m_freeSpace - needSpace;
	m_splitedMPNo = miniPageNo;

	// 记录MP分裂逻辑日志
#ifdef NTSE_TRACE
	u64 oldLSN = m_lsn;
#endif
	m_lsn = logger->logSplitMP(session, IDX_LOG_SPLIT_MP, pageId, sOffset, oldValue, oldValueLen, (byte*)this + sOffset, needSpace + oldValueLen, leftItem, miniPageNo, m_lsn);
	ftrace(ts.idx, tout << "SplitMP: pageId: " << pageId << " MPNo: " << miniPageNo << " oldLSN: " << oldLSN << " newLSN: " << m_lsn);

	return INSERT_SUCCESS;
}


/** 合并指定的MiniPage和它的后一个MiniPage
 * @param session		会话句柄
 * @param logger		日志记录器
 * @param pageId		页面ID
 * @param miniPageNo	要合并的起始MP号
 * @param prevKey		用来保存合并之前，前一个MP的最后一个键值
 * @param nextKey		用来保存合并之前，后一个MP的第一个键值
 * @return true表示能成功合并，false表示无法合并
 */
bool IndexPage::mergeTwoMiniPage( Session *session, IndexLog *logger, PageId pageId, u16 miniPageNo, SubRecord *prevKey, SubRecord *nextKey ) {
	u16 nextMPNo = miniPageNo + 1;
	u16 originalMPKeyCounts = getMiniPageItemCount(miniPageNo);
	u16 newMPItemCounts = originalMPKeyCounts + getMiniPageItemCount(nextMPNo);
	if (m_miniPageNum <= miniPageNo + 1 || newMPItemCounts > MINI_PAGE_MAX_ITEMS)
		// 保证miniPageNo指定的正确性，必须不越界，存在合并的后项
		// 指定MiniPage合并之后，必须项数不能超过规定最大上限
		return false;

	u16 nextMPOffset = getMiniPageDataOffset(nextMPNo);
	IndexKey *indexKey = (IndexKey*)((byte*)this + nextMPOffset);
	u16 nextKeyLen = indexKey->extractKey(NULL, nextKey, !isPageLeaf());
	getPrevKey(nextMPOffset, false, &miniPageNo, prevKey);

	u16 compressedSize = IndexKey::computeSpace(prevKey, nextKey, !isPageLeaf());
	assert(compressedSize < nextKeyLen);
	u16 savedSpace = nextKeyLen - compressedSize;

	// 记录要合并键值原始信息
	byte* oldValue = (byte*)alloca(nextKeyLen);
	shiftDataByCPY(oldValue, (byte*)this + nextMPOffset, nextKeyLen);

	// 执行合并
	byte *end = indexKey->compressAndSaveKey(prevKey, nextKey, !isPageLeaf());
	shiftDataByMOVE(end, (byte*)this + nextMPOffset + nextKeyLen, getDataEndOffset() - nextMPOffset - nextKeyLen);
	clearData(getDataEndOffset() - savedSpace, m_dirStart - (getDataEndOffset() - savedSpace));
	// 项目录合并调整
	setMiniPageItemCount(miniPageNo, newMPItemCounts);
	deleteMiniPageDir(nextMPNo);
	adjustMiniPageDataOffset(miniPageNo + 1, 0 - savedSpace);

	m_freeSpace += savedSpace;
	m_splitedMPNo = (u16)-1;

	// 记录日志
#ifdef NTSE_TRACE
	u64 oldLSN = m_lsn;
#endif
	m_lsn = logger->logMergeMP(session, IDX_LOG_MERGE_MP, pageId, nextMPOffset, (byte*)this + nextMPOffset, compressedSize, oldValue, nextKeyLen, miniPageNo, originalMPKeyCounts, m_lsn);
	ftrace(ts.idx, tout << "MergeMP: pageId: " << pageId << " MPNo: " << miniPageNo << " oldLSN: " << oldLSN << " newLSN: " << m_lsn);

	return true;
}

///**
// * 更新指定项的pageId信息
// * @param session	会话句柄
// * @param logger	日志记录器
// * @param pageId	当前页面PageId
// * @param newPageId	新的页面ID信息
// * @param keyInfo	要更新键值的位置信息
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
 * 在页面内找到指定的PageId
 * @param findPageId	要查找的pageId
 * @return 如果找到，返回true，否则返回false
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
 * 得到指定位置开始的索引键值的PageId信息
 *
 * @param offset	取后缀的起始偏移
 * @param pageId	IN/OUT	取得的PageId，如果当前到页面尾部，返回INVALID_PAGE_ID
 * @return 下一个Key的相对偏移
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
 * 将指定项插入到页面的尾部，插入必须是有序的，用于创建索引时使用
 *
 * @pre appendKey必须是lastKey的直接后项，如果lastKey为NULL，表示空页面插入第一项
 *
 * @param session	会话句柄
 * @param logger	日志记录器
 * @param pageId	页面ID
 * @param appendKey	要添加的项
 * @param lastKey	页面当前最后一项
 * @return 键值插入的结果，如果返回INSERT_SUCCESS，表示插入成功；返回NEED_SPLIT表示需要分裂页面没有改变
 */
IndexInsertResult IndexPage::appendIndexKey(Session *session, IndexLog *logger, PageId pageId, const SubRecord *appendKey, const SubRecord *lastKey) {
	u16 miniPageNo = m_miniPageNum - 1;
	const SubRecord *prevKey = lastKey;
	bool isPageIdExists = !isPageLeaf();

	u16 commonPrefixLen = lastKey != NULL ? IndexKey::computePrefix(lastKey, appendKey) : 0;

	if (m_miniPageNum == 0 || isInitMiniPageFull(miniPageNo) || commonPrefixLen == 0) {
		// 当前的MiniPage已超过最大初始化限制或者页面为空，需要使用新的MiniPage
		// 首先判断页面空间是否足够
		if (!isPageEnough(INDEX_PAGE_SIZE - INDEXPAGE_INIT_MAX_USAGE + MINI_PAGE_DIR_SIZE + IndexKey::computeSpace(NULL, appendKey, isPageIdExists)))
			return NEED_SPLIT;

		addMiniPage(session, logger, pageId, appendKey, m_miniPageNum);

		return INSERT_SUCCESS;
	}

	// 判断页面空间是否足够
	u16 needSpace = IndexKey::computeSpace(appendKey, commonPrefixLen, isPageIdExists);
	if (!isPageEnough(INDEX_PAGE_SIZE - INDEXPAGE_INIT_MAX_USAGE + needSpace))	// 空间不够，需要分裂
		return NEED_SPLIT;

	// 插入键值
	u16 start = getDataEndOffset();
	IndexKey *indexKey = (IndexKey*)((byte*)this + start);
	indexKey->compressAndSaveKey(prevKey, appendKey, isPageIdExists);

	// 记录append日志
	m_lsn = logger->logDMLUpdate(session, IDX_LOG_APPEND, pageId, start, miniPageNo, (byte*)this + start, 0, (byte*)this + start, needSpace, m_lsn);

	// 修改页面相关信息
	m_freeSpace = m_freeSpace - needSpace;
	m_pageCount++;
	incMiniPageItemCount(miniPageNo);

	return INSERT_SUCCESS;
}



/**
 * 重做页面修改操作，只涉及最基本的修改，不涉及页面分裂MiniPage的分裂删除和新生成等动作
 * 这里的redo只需要简单的移动页面内容，复制插入值的完成状态即可
 * @param type			日志类型
 * @param offset		页面修改起始偏移
 * @param miniPageNo	操作涉及的MiniPage
 * @param oldSize		前述内容的长度
 * @param newValue		之后在指定偏移处的内容
 * @param newSize		前述内容的长度
 */
void IndexPage::redoDMLUpdate(IDXLogType type, u16 offset, u16 miniPageNo, u16 oldSize, byte *newValue, u16 newSize) {
	vecode(vs.idx, traversalAndVerify(NULL, NULL, NULL, NULL, NULL));

	if (type == IDX_LOG_INSERT && m_pageCount != 0)
		updateInsertOffsetRatio(offset);

	s32 moveSize = getDataEndOffset() - offset - oldSize;
	assert(moveSize >= 0);
	shiftDataByMOVE((byte*)this + offset + newSize, (byte*)this + offset + oldSize, moveSize);
	memcpy((byte*)this + offset, newValue, newSize);
	// 修改页面相关信息
	assert(m_freeSpace + oldSize >= newSize);
	m_freeSpace = m_freeSpace - (newSize - oldSize);
	if (type == IDX_LOG_DELETE) {
		if (newSize < oldSize)	// 减少的数据清零
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
 * redo索引SMO合并操作
 * @param prevPageId	合并左页面的前一个页面的ID
 * @param moveData		左页面移动过来的数据
 * @param dataSize		左页面移动的数据长度
 * @param moveDir		左页面移动过来的项目录
 * @param dirSize		左页面移动项目录内容长度
 * @param smoLSN		页面SMO之后的LSN
 */
void IndexPage::redoSMOMergeOrig(PageId prevPageId, byte *moveData, u16 dataSize, byte *moveDir, u16 dirSize, u64 smoLSN) {
	vecode(vs.idx, traversalAndVerify(NULL, NULL, NULL, NULL, NULL));

	u16 localDataSize = getDataEndOffset() - INDEXPAGE_DATA_START_OFFSET;
	shiftDataByMOVE((byte*)this + INDEXPAGE_DATA_START_OFFSET + dataSize, (byte*)this + INDEXPAGE_DATA_START_OFFSET, localDataSize);
	shiftDataByCPY((byte*)this + INDEXPAGE_DATA_START_OFFSET, moveData, dataSize);
	shiftDirByCPY((byte*)this + m_dirStart - dirSize, moveDir, dirSize);

	assert(m_freeSpace > dataSize + dirSize);
	// 修改页面头信息
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
 * redo索引SMO分裂操作(原页面MiniPage数目大于一的情况)
 * @param nextPageId	合并左页面的下一个页面的ID
 * @param dataSize		左页面移动的数据长度
 * @param dirSize		左页面移动项目录内容长度
 * @param oldSKLen		分裂键值原始长度
 * @param mpLeftCount	分裂的MP剩在原始页面的项数，为0说明整MP分裂
 * @param mpLeftCount	分裂的MP移动到新页面的项数，为0说明整MP分裂
 * @param smoLSN		页面SMO之后的LSN
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
 * 重做SMO合并操作合并页面的操作
 */
void IndexPage::redoSMOMergeRel() {
	memset((byte*)this + INDEXPAGE_DATA_START_OFFSET, 0, INDEX_PAGE_SIZE - INDEXPAGE_DATA_START_OFFSET);
}


/**
 * 重做SMO分裂操作新页面的恢复
 * @param moveData		移动的数据内容
 * @param dataSize		移动的数据长度
 * @param moveDir		移动的项目录内容
 * @param dirSize		移动的项目录长度
 * @param newSplitKey	分裂点键值解压后的内容
 * @param newSKLen		分裂点键值解压后长度
 * @param mpLeftCount	分裂MP剩在原页面的项数
 * @param mpMoveCount	分裂MP移动到新页面的项数
 * @param smoLSN		页面SMO之后的LSN
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
 * redo页面更新操作
 * @param offset			更新内容页内偏移
 * @param newValue			更新的新内容
 * @param valueLen			更新内容长度
 * @param clearPageFirst	标志重做前是否要先清零
 */
void IndexPage::redoPageUpdate(u16 offset, byte *newValue, u16 valueLen, bool clearPageFirst) {
	if (clearPageFirst)
		memset((byte*)this + sizeof(Page), 0, INDEX_PAGE_SIZE - sizeof(Page));
	shiftDataByCPY((byte*)this + offset, newValue, valueLen);
}



/**
 * 重做页面增加MiniPage操作
 * @param keyValue		MiniPage包含键值内容
 * @param dataSize		键值的长度
 * @param miniPageNo	MiniPage页面号
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
 * 重做页面删除MiniPage操作
 * @param miniPageNo	要删除的MIniPageNo号
 */
void IndexPage::redoPageDeleteMP(u16 miniPageNo) {
	u16 miniPageEnd = (miniPageNo < m_miniPageNum - 1) ? getMiniPageDataOffset(miniPageNo + 1) : getDataEndOffset();
	u16 miniPageStart = getMiniPageDataOffset(miniPageNo);
	deleteMiniPageIn(miniPageNo, miniPageStart, miniPageEnd);

	vecode(vs.idx, traversalAndVerify(NULL, NULL, NULL, NULL, NULL));
}



/**
 * 重做页面某个MiniPage的分裂操作
 * @param offset			分裂点的页内偏移
 * @param oldSize			旧数据长度
 * @param newValue			新数据内容
 * @param newSize			新数据长度
 * @param leftItems			原MiniPage留下的项数
 * @param miniPageNo		分裂的MiniPage编号
 */
void IndexPage::redoPageSplitMP(u16 offset, u16 oldSize, byte *newValue, u16 newSize, u16 leftItems, u16 miniPageNo) {
	// 项目录
	u16 splitItems = getMiniPageItemCount(miniPageNo) - leftItems;
	addMiniPageDir(miniPageNo + 1, offset, splitItems);
	adjustMiniPageDataOffset(miniPageNo + 2, newSize - oldSize);
	setMiniPageItemCount(miniPageNo, leftItems);
	// 数据
	s32 moveSize = getDataEndOffset() - offset - oldSize;
	assert(moveSize > 0);
	shiftDataByMOVE((byte*)this + offset + newSize, (byte*)this + offset + oldSize, moveSize);
	shiftDataByCPY((byte*)this + offset, newValue, newSize);
	assert(m_freeSpace + oldSize >= newSize);
	m_freeSpace = m_freeSpace - (newSize - oldSize);
	// m_splitedMPNo = miniPageNo;	// 应该不必要

	vecode(vs.idx, traversalAndVerify(NULL, NULL, NULL, NULL, NULL));
}

/**
 * 重做页面某个MiniPage的合并操作
 * @param offset		合并点的页内偏移
 * @param oldSize		旧数据长度
 * @param newValue		新数据内容
 * @param newSize		新数据长度
 * @param miniPageNo	合并的左边MiniPage编号
 */
void IndexPage::redoPageMergeMP( u16 offset, u16 oldSize, byte *newValue, u16 newSize, u16 miniPageNo ) {
	assert(newSize <= oldSize);
	assert(m_miniPageNum > miniPageNo + 1);
	// 项目录
	u16 mergeLeftKeys = getMiniPageItemCount(miniPageNo);
	u16 mergeRightKeys = getMiniPageItemCount(miniPageNo + 1);
	setMiniPageItemCount(miniPageNo, mergeLeftKeys + mergeRightKeys);
	adjustMiniPageDataOffset(miniPageNo + 2, newSize - oldSize);
	deleteMiniPageDir(miniPageNo + 1);
	// 数据
	s32 moveSize = getDataEndOffset() - offset - oldSize;
	assert(moveSize > 0);
	shiftDataByMOVE((byte*)this + offset + newSize, (byte*)this + offset + oldSize, moveSize);
	shiftDataByMOVE((byte*)this + offset, newValue, newSize);
	m_freeSpace += oldSize - newSize;

	vecode(vs.idx, traversalAndVerify(NULL, NULL, NULL, NULL, NULL));
}


/**
 * undo插入、删除、更新操作
 * @param type			日志类型
 * @param offset		页面修改起始偏移
 * @param miniPageNo	操作涉及的MiniPage
 * @param oldValue		之前在指定偏移处的内容
 * @param oldSize		前述内容的长度
 * @param newSize		前述内容的长度
 */
void IndexPage::undoDMLUpdate(IDXLogType type, u16 offset, u16 miniPageNo, byte *oldValue, u16 oldSize, u16 newSize) {
	s32 moveSize = getDataEndOffset() - offset - newSize;
	assert(moveSize >= 0);
	shiftDataByMOVE((byte*)this + offset + oldSize, (byte*)this + offset + newSize, moveSize);
	memcpy((byte*)this + offset, oldValue, oldSize);
	// 修改页面相关信息
	assert(m_freeSpace + newSize >= oldSize);
	m_freeSpace = m_freeSpace - (oldSize - newSize);
	if (type == IDX_LOG_DELETE) {
		incMiniPageItemCount(miniPageNo);
		m_pageCount++;
		m_nChance > 0 ? ++m_nChance : m_nChance;
	} else {	// IDX_INSERT/IDX_APPEND
		if (newSize > oldSize)	// 减少的数据清零
			memset((byte*)this + offset + oldSize + moveSize, 0, newSize - oldSize);

		decMiniPageItemCount(miniPageNo);
		m_pageCount--;
	}
	adjustMiniPageDataOffset(miniPageNo + 1, oldSize - newSize);
}


/**
 * undo索引SMO合并操作原始页面的修改
 * @param leftPageId	合并左页面ID
 * @param dataSize		左页面移动的数据长度
 * @param dirSize		左页面移动项目录内容长度
 */
void IndexPage::undoSMOMergeOrig(PageId leftPageId, u16 dataSize, u16 dirSize) {
	// undo原页面操作
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
 * undo索引SMO分裂操作原始页面的修改
 * @param nextPageId	分裂前该页面的后继页面
 * @param moveData		左页面移动过来的数据
 * @param dataSize		左页面移动的数据长度
 * @param moveDir		左页面移动过来的项目录
 * @param dirSize		左页面移动项目录内容长度
 * @param oldSplitKey	分裂键值原始内容
 * @param oldSKLen		分裂键值原始长度
 * @param newSKLen		分裂键值新长度
 * @param mpLeftCount	分裂MP留在原始页面项数
 * @param mpMoveCount	分裂MP移动到页面项数
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
 * undo索引SMO合并当中相关页面的操作
 * @param prevPageId	该页面合并到前一个页面的页面ID
 * @param moveData		移动的数据内容
 * @param dataSize		该页面的数据长度
 * @param moveDir		移动的项目录内容
 * @param dirSize		项目录长度
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
 * undoSMO分裂相关页面的操作
 */
void IndexPage::undoSMOSplitRel() {
	m_miniPageNum = m_pageCount = 0;
	m_freeSpace = INDEX_PAGE_SIZE - INDEXPAGE_DATA_START_OFFSET;
	m_dirStart = INDEX_PAGE_SIZE;
	memset((byte*)this, 0, INDEX_PAGE_SIZE - INDEXPAGE_DATA_START_OFFSET);
}


/**
 * undo页面更新操作
 * @param offset	更新内容页内偏移
 * @param oldValue	更新的旧内容
 * @param valueLen	更新内容长度
 */
void IndexPage::undoPageUpdate(u16 offset, byte *oldValue, u16 valueLen) {
	shiftDataByCPY((byte*)this + offset, oldValue, valueLen);
}


/**
 * undo页面增加MiniPage操作
 * @param dataSize		MiniPage页面数据长度
 * @param miniPageNo	MiniPage页面号
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
 * undo页面删除MiniPage操作
 * @param keyValue		MIniPage当中的键值
 * @param dataSize		键值的长度
 * @param miniPageNo	要删除的MIniPageNo号
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
 * undo页面某个MiniPage的分裂操作
 * @param offset			分裂点的页内偏移
 * @param oldValue			分裂前键值前缀
 * @param oldSize			键值前缀长度
 * @param newSize			键值前缀长度
 * @param miniPageNo		分裂的MiniPage编号
 */
void IndexPage::undoPageSplitMP(u16 offset, byte *oldValue, u16 oldSize, u16 newSize, u16 miniPageNo) {
	assert(oldSize <= newSize);	// 对于前缀和后缀都很长，要两个字节表示的情况，解压之后后缀只要一个字节，这时候会少用1个字节，但是键值内容实际上会多很多
	// 项目录undo
	u16 newMPItems = getMiniPageItemCount(miniPageNo + 1);
	setMiniPageItemCount(miniPageNo, getMiniPageItemCount(miniPageNo) + newMPItems);
	adjustMiniPageDataOffset(miniPageNo + 2, oldSize - newSize);
	deleteMiniPageDir(miniPageNo + 1);

	// 数据undo
	s32 moveSize = getDataEndOffset() - offset - newSize;
	assert(moveSize > 0);
	shiftDataByMOVE((byte*)this + offset + oldSize, (byte*)this + offset + newSize, moveSize);
	memcpy((byte*)this + offset, oldValue, oldSize);
	// 修改页面相关信息
	assert(m_freeSpace + newSize >= oldSize);
	m_freeSpace = m_freeSpace - (oldSize - newSize);
	//m_splitedMPNo = (u16)-1;	// 不必要

	if (newSize > oldSize)
		memset((byte*)this + getDataEndOffset(), 0, newSize - oldSize);

	vecode(vs.idx, traversalAndVerify(NULL, NULL, NULL, NULL, NULL));
}


/**
 * undo某个页面的MP合并操作
 * @param offset		合并点的页内偏移
 * @param oldValue		合并前的数据内容，未压缩格式
 * @param oldSize		合并前的数据长度
 * @param newSize		合并后的数据长度，压缩之后
 * @param originalMPKeyCounts	合并前左边MP包含的项数
 * @param miniPageNo	合并左边MP的编号
 */
void IndexPage::undoPageMergeMP( u16 offset, byte *oldValue, u16 oldSize, u16 newSize, u16 originalMPKeyCounts, u16 miniPageNo ) {
	assert(newSize <= oldSize);
	// 项目录undo
	u16 rightMPKeyCounts = getMiniPageItemCount(miniPageNo) - originalMPKeyCounts;
	addMiniPageDir(miniPageNo + 1, offset, rightMPKeyCounts);
	adjustMiniPageDataOffset(miniPageNo + 2, oldSize - newSize);
	setMiniPageItemCount(miniPageNo, originalMPKeyCounts);
	// 数据undo
	s32 moveSize = getDataEndOffset() - offset - newSize;
	assert(moveSize > 0);
	shiftDataByMOVE((byte*)this + offset + oldSize, (byte*)this + offset + newSize, moveSize);
	shiftDataByCPY((byte*)this + offset, oldValue, oldSize);
	assert(m_freeSpace + newSize >= oldSize);
	m_freeSpace = m_freeSpace - (oldSize - newSize);

	vecode(vs.idx, traversalAndVerify(NULL, NULL, NULL, NULL, NULL));
}


/**
 * 根据指定的偏移，获取该偏移处保存的键值
 * @param offset	指定偏移
 * @param key1		out 找到的键值保存在key1当中
 * @Param key2		临时键值
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
 * 根据索引内部ID序号，以及页面ID，计算出一个页面唯一标识符
 * 现在的计算方式为只使用indexId组成一个标志，
 * 使得属于同一个索引得页面标志一定是一样的，并且一个页面唯一对应一个索引
 * @param indexId	索引内部ID
 * @param pageId	如果不为INVALID_ROW_ID，表示指定某个页面
 * @return 页面标志
 */
u32 IndexPage::formPageMark(u16 indexId, PageId pageId) {
	UNREFERENCED_PARAMETER(pageId);
	return u32(indexId);
}

/**
 * 根据指定的标志，得出所属索引的ID号
 * @param mark	页面标志
 * @return 索引ID
 */
u16 IndexPage::getIndexId(u32 mark) {
	return (u16)mark;
}

/**
 * 遍历索引页面，检查页面数据正确性
 *
 * @param	tableDef	索引页面所属表定义
 * @param	indexDef	索引定义
 * @param	key1		临时键值
 * @param	key2		临时键值
 * @param	pkey		pad格式键值
 * @return true表示页面正确，否则页面出错
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
		if (*((byte*)indexKey) == 0) {	// 验证MiniPage目录正确性
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

	// 检查空闲空间都清零
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
 * 计算页面的压缩前页面数据长度
 * @param tableDef	表定义
 * @param indexDef  索引定义
 * @param key		用于读取每个键值的子记录
 * @return 压缩前页面数据长度
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

	if (initLen == 0) {	// 索引树为空的情况
		assert(this->isPageRoot() && this->isPageEmpty());
		initLen = 1;
	}

	return initLen;
}


/** 验证页面数据区域都是0
 * @return true验证成功，false失败
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
 * 文件头页面构造函数
 * @param indexMark	文件标识符
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
 * 更新索引相关页面的部分信息
 *
 * @param session	会话句柄
 * @param logger	日志记录器
 * @param pageId	本页面的ID
 * @param offset	要更新的信息在页面偏移量
 * @param size		更新信息长度
 * @param newValue	更新信息
 */
void IndicePage::updateInfo(Session *session, IndexLog *logger, PageId pageId, u16 offset, u16 size, byte *newValue) {
	byte *oldValue = (byte*)this + offset;
	u64 newLSN = logger->logPageUpdate(session, IDX_LOG_UPDATE_PAGE, pageId, offset, newValue, oldValue, size, m_lsn);
	memcpy(oldValue, newValue, size);
	m_lsn = newLSN;
}



/**
 * 丢弃索引时对头页面的修改
 * @pre 外层应该对头页面加了X锁，并且最后负责放锁。外层要确认指定丢弃的索引信息确实存在
 * @param indexNo			要丢弃的索引信息序号
 * @param minFreeByteMap	丢弃之后最小索引位图位偏移
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
 * 更新指定索引页面使用统计信息
 * @pre 必须头页面加了互斥锁
 * @param session		会话句柄
 * @param logger		日志记录器
 * @param indexNo		要更新信息的索引编号
 * @param dataPages		更新的索引使用页面总数，如果是(u64)-1表示当前不需要更新
 * @param freePages		更新的索引空闲页面总数，如果是(u64)-1表示当前不需要更新
 */
void IndexHeaderPage::updatePageUsedStatus(Session *session, IndexLog *logger, u8 indexNo, u64 dataPages, u64 freePages) {
	assert(freePages != (u64)-1 && dataPages >= freePages);
	if (dataPages != (u64)-1)
		updateInfo(session, logger, 0, OFFSETOFARRAY(IndexHeaderPage, m_indexDataPages, sizeof(dataPages), indexNo), sizeof(dataPages), (byte*)&dataPages);
	updateInfo(session, logger, 0, OFFSETOFARRAY(IndexHeaderPage, m_indexFreePages, sizeof(freePages), indexNo), sizeof(freePages), (byte*)&freePages);
}

/** 返回当前创建索引所能用的索引ID，也就是返回m_uniqueIndexId
 * @pre 必须头页面加了互斥锁
 * @return 可用的索引ID
 */
u8 IndexHeaderPage::getAvailableIndexId() const {
	return m_indexUniqueId;
}

/** 返回当前创建索引所能用的索引ID，并且标识该ID不可用。内部实现将会计算出下一个ID保存在m_uniqueIndexId中
 * @pre 必须头页面加了互斥锁
 * @post 因为使用的id数组已经修改，后续需要对其他数组修改，并且m_indexNum应该加1
 * @param session	会话句柄
 * @param logger	日志记录器
 * @return 可用的索引ID
 */
u8 IndexHeaderPage::getAvailableIndexIdAndMark( Session *session, IndexLog *logger ) {
	u8 indexId = m_indexUniqueId;

	// 计算下一个可用的索引ID，需要结合已经使用的ID来计算
	u8 ids[Limits::MAX_INDEX_NUM];
	ids[0] = indexId;
	memcpy(ids + sizeof(ids[0]), m_indexIds, sizeof(ids[0]) * m_indexNum);	// 确保m_indexNum应该只包含已经分配了的ID的索引个数
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
	// 该循环主要是处理上面uniqueId==1且m_indexNum!=0的情况，但是其他情况执行了这个循环并不会影响结果的正确性，也不会有效率问题
	//for (uint i = 0; i < m_indexNum && ids[i] == uniqueId; i++, uniqueId++) ;

	updateInfo(session, logger, 0, OFFSET(IndexHeaderPage, m_indexUniqueId), sizeof(u8), (byte*)&uniqueId);
	updateInfo(session, logger, 0, OFFSETOFARRAY(IndexHeaderPage, m_indexIds, 1, m_indexNum), 1, &indexId);

	return indexId;
}

}
