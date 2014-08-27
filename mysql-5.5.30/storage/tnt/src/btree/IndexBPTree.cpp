/**
 * NTSE B+树索引实现类
 *
 * author: naturally(naturally@163.org), liweizhao(liweizhao@corp.netease.com)
 */

// IndexBPTree.cpp: implementation of the DrsBPTreeIndex/DrsIndexRangeScanHandle class.
//
////////////////////////////////////////////////////////////////////

#include "api/Database.h"
#include "btree/IndexBPTree.h"
#include "btree/IndexBPTreesManager.h"
#include "btree/IndexPage.h"
#include "btree/IndexKey.h"
#include "btree/IndexLog.h"
#include "util/PagePool.h"
#include "util/Stream.h"
#include "util/Sync.h"
#include "util/File.h"
#include "misc/Buffer.h"
#include "misc/Record.h"
#include "misc/Session.h"
#include "misc/Trace.h"
#include "misc/Verify.h"
#include "misc/IndiceLockManager.h"
#include "api/Table.h"
#include "util/Array.h"
#include <algorithm>
#include "misc/Profile.h"

#ifdef NTSE_UNIT_TEST
#include "util/Thread.h"
#endif

#ifdef TNT_ENGINE
#include "trx/TNTTransaction.h"
#include "trx/TLock.h"
#endif

using namespace std;

namespace ntse {


uint IndexSampleHandle::DEFAULT_MIN_SAMPLE_RANGE_PAGES = 32;
uint IndexSampleHandle::DEFAULT_SAMPLE_RANGES = 8;

DrsIndexScanHandleInfoExt::DrsIndexScanHandleInfoExt(
	Session *session, const TableDef *tableDef, const IndexDef *indexDef, LockMode lockMode, 
	LockMode latchMode, bool isFastComparable, CompareKey comparator, TLockMode trxLockMode) 
	: DrsIndexScanHandleInfo(session, tableDef, indexDef, lockMode, 
	latchMode, isFastComparable, comparator) {
		m_readKey = IndexKey::allocSubRecord(session->getMemoryContext(), indexDef, KEY_COMPRESS);
		m_readPageId = INVALID_PAGE_ID;
		m_readPageLSN = 0;
		m_readPageSMOLSN = 0;
		m_readPageHandle = NULL;
		m_findKeyBufSize = 0;
		m_forceSearchPage = false;
		m_trxLockMode = trxLockMode;
}

/**
 * 移动外存索引游标到读到的下一条记录
 */
void DrsIndexScanHandleInfoExt::moveCursor() {
	checkFormat();
	IndexKey::swapKey(&m_findKey, &m_readKey);

	if (m_pageHandle != m_readPageHandle && m_pageHandle != NULL) {
		//游标移到下一页面了，此时释放上一页面的pin
		m_session->unpinPage(&m_pageHandle);
		
// 		m_pageId = m_readPageId;
// 		m_pageHandle = m_readPageHandle;
// 		m_pageLSN = m_readPageLSN;
// 		m_pageSMOLSN = m_readPageSMOLSN;
	}
	m_pageId = m_readPageId;
	m_pageHandle = m_readPageHandle;
	m_pageLSN = m_readPageLSN;
	m_pageSMOLSN = m_readPageSMOLSN;
	assert(m_pageHandle == m_readPageHandle);
	assert(m_pageLSN == m_readPageLSN);
	assert(m_pageSMOLSN == m_readPageSMOLSN);
	m_keyInfo = m_readKeyInfo;
	m_backupRangeFirst = m_rangeFirst;
}

/**
 * 移动外存索引游标到指定的记录
 * @param key 指定的记录
 */
void DrsIndexScanHandleInfoExt::moveCursorTo(const SubRecord *key) {
	assert(KEY_PAD == key->m_format);
	checkFormat();
	m_findKey->m_size = m_indexDef->m_maxKeySize;
	RecordOper::convertKeyPC(m_tableDef, m_indexDef, key, m_findKey);

	//如果第一次定位页面失败，下次一定还是search path，必须把pin放掉
	if(m_pageHandle != m_readPageHandle)
		m_session->unpinPage(&m_readPageHandle);

	m_includeKey = false;
	m_forceSearchPage = true;
}

/**
 * 检查查找键的格式是否为压缩格式，不是则转化为压缩格式
 */
void DrsIndexScanHandleInfoExt::checkFormat() {
	if (unlikely(m_findKey->m_format == KEY_PAD)) {
		m_findKey->m_columns = m_key0->m_columns;
		m_findKey->m_numCols = m_key0->m_numCols;
		assert(m_key0 != NULL && m_key0->m_format == KEY_COMPRESS);
		IndexKey::swapKey(&(m_findKey), &(m_key0));
		if (m_isFastComparable)
			m_comparator->setComparator(RecordOper::compareKeyCC);
	}
	assert(KEY_COMPRESS == m_findKey->m_format);
}

// Implementation of DrsIndex class
//
//////////////////////////////////////////////////////////////////////


/**
 * 构造函数
 * @param indice		对应的indice类对象
 * @param tableDef		对应的表定义
 * @param indexDef		索引定义，需要本地备份
 * @param indexId		该索引在索引文件当中的ID
 * @param rootPageId	根页面ID
 */
DrsBPTreeIndex::DrsBPTreeIndex(DrsIndice *indice, const TableDef *tableDef, const IndexDef *indexDef, u8 indexId, u64 rootPageId) {
	ftrace(ts.idx, tout << indice << indexDef << indexId << rootPageId);

	m_indice = indice;
	m_tableDef = tableDef;
	m_indexId = indexId;
	m_rootPageId = rootPageId;
	m_indexDef = new IndexDef(indexDef);
	m_dboStats = new DBObjStats(DBO_Index);
	m_dboStats->m_idxName = m_indexDef->m_name;
	m_doneSMOTxnId = INVALID_TXN_ID;
	memset(&m_indexStatus, 0, sizeof(m_indexStatus));
	memset(&m_indexStatusEx, 0, sizeof(m_indexStatusEx));
	m_indexStatus.m_dboStats = m_dboStats;
}


/**
 * 析构函数
 */
DrsBPTreeIndex::~DrsBPTreeIndex() {
	ftrace(ts.idx, );

	delete m_dboStats;
	delete m_indexDef;
}



/**
 * 获得某个索引的ID号
 * @return 索引ID
 */
u8 DrsBPTreeIndex::getIndexId() {
	return m_indexId;
}


/**
 * 得到索引定义
 * @return 索引定义
 */
const IndexDef* DrsBPTreeIndex::getIndexDef() {
	return m_indexDef;
}

/**
 * 估计索引键值在[min,max]之间的记录数
 * @pre min和max应该是所以上顺序从小到大的两个键值
 *
 * @param session 会话
 * @param minKey 下限，若为NULL则表示不指定下限，否则一定是PAD格式得键值
 * @param includeKeyMin	下限模式，true是>=/false是>
 * @param maxKey 上限，若为NULL则表示不指定上限，否则一定是PAD格式得键值
 * @param includeKeyMax	上限模式，true是<=/false是<
 * @return 在[min,max]之间的记录数的估计值
 */
u64 DrsBPTreeIndex::recordsInRange(Session *session, const SubRecord *minKey, bool includeKeyMin, const SubRecord *maxKey, bool includeKeyMax) {
	PROFILE(PI_DrsIndex_recordsInRange);

	MemoryContext *memoryContext = session->getMemoryContext();
	bool singleroot = false;
	PageId minLeafPageId = INVALID_PAGE_ID, maxLeafPageId = INVALID_PAGE_ID;
	struct IndexSearchTraceInfo	traceMin[MAX_INDEX_LEVEL], traceMax[MAX_INDEX_LEVEL];
	u16 minLeafPageCount = 0, maxLeafPageCount = 0;
	u16 minLeafKeyCount = 0, maxLeafKeyCount = 0;
	SearchFlag minFlag(true, includeKeyMin, false);
	SearchFlag maxFlag(false, includeKeyMax, false);
	bool minFetchNext = false, maxFetchNext = false;
	KeyInfo keyInfo;

	void *memory = memoryContext->alloc(sizeof(DrsIndexScanHandleInfo));
	DrsIndexScanHandleInfo *scanInfo = new (memory) DrsIndexScanHandleInfo(session, m_tableDef, m_indexDef, None, Shared, false, RecordOper::compareKeyPC);
	scanInfo->m_key0 = IndexKey::allocSubRecord(memoryContext, m_indexDef, KEY_COMPRESS);
	scanInfo->m_cKey1 = IndexKey::allocSubRecord(memoryContext, m_indexDef, KEY_COMPRESS);

	// 查找小键值信息
	estimateKeySearch(minKey, scanInfo, &minFlag, &singleroot, &minFetchNext, &minLeafKeyCount, &minLeafPageCount);
	memcpy(traceMin, scanInfo->m_traceInfo, sizeof(struct IndexSearchTraceInfo) * MAX_INDEX_LEVEL);
	minLeafPageId = scanInfo->m_pageId;

	memset(scanInfo->m_traceInfo, 0, sizeof(struct IndexSearchTraceInfo) * MAX_INDEX_LEVEL);
	scanInfo->m_pageId = INVALID_PAGE_ID;

	// 再搜索大键值信息
	estimateKeySearch(maxKey, scanInfo, &maxFlag, &singleroot, &maxFetchNext, &maxLeafKeyCount, &maxLeafPageCount);
	memcpy(traceMax, scanInfo->m_traceInfo, sizeof(struct IndexSearchTraceInfo) * MAX_INDEX_LEVEL);
	maxLeafPageId = scanInfo->m_pageId;

	// 估算键值数
	s16 level = MAX_INDEX_LEVEL;
	u64 rows = 1;
	bool diverged = false;    //标识是否分开在不同页面上
	bool diverged_lot = false;	//标识是否分开在不同页面，并且不在相邻页面上
	while (true) {
		--level;
		if (level < 0)
			break;

		bool minStart = (traceMax[level].m_pageId != 0), maxStart = (traceMin[level].m_pageId != 0);
		if (minStart ^ maxStart)
			// 表示搜索过程中索引层数已经发生变化，返回估算值
			return 10;
		else if (!minStart)
			continue;

		if (traceMin[level].m_pageId == traceMax[level].m_pageId) {
			if (traceMin[level].pi.ei.m_keyCount != traceMax[level].pi.ei.m_keyCount) {	// 当前层开始分开了
				if (traceMin[level].pi.ei.m_keyCount < traceMax[level].pi.ei.m_keyCount) {	// 正确情况
					diverged = true;
					rows = max(traceMax[level].pi.ei.m_keyCount - traceMin[level].pi.ei.m_keyCount, 1);
					if(rows > 1) {
						diverged_lot = true;
					}
				} else {	// 两次定位之间可能出现索引树被修改，定位不准确，返回估计值
					return 10;
				}
			}
			continue;
		} else if (diverged && !diverged_lot){	// 表示路径已经不同，但是是在相邻的页面上
			if (traceMin[level].pi.ei.m_keyCount < traceMin[level].pi.ei.m_pageCount 
				|| traceMax[level].pi.ei.m_keyCount > 1) {
					diverged_lot = true;
					rows = 0;
					if (traceMin[level].pi.ei.m_keyCount < traceMin[level].pi.ei.m_pageCount) {
						rows += traceMin[level].pi.ei.m_pageCount - traceMin[level].pi.ei.m_keyCount;
					}
					if (traceMax[level].pi.ei.m_keyCount > 1) {
						rows += traceMax[level].pi.ei.m_keyCount - 1;
					}
			}
		} else if (diverged_lot) {		//路径不同，且不是在相邻页面上
			rows *= (traceMin[level].pi.ei.m_pageCount + traceMax[level].pi.ei.m_pageCount) / 2;
		}
	}

	// 计算叶节点一层
	if (diverged || minLeafPageId != maxLeafPageId) {
		rows *= (minLeafPageCount + maxLeafPageCount) / 2;
	} else if (singleroot || !diverged) {
		rows = maxLeafKeyCount - minLeafKeyCount;
		if (minFetchNext)
			rows--;
		if (!maxFetchNext)
			rows++;
	} else {
		rows = 10;
	}

	// 处理0的情况，解决Innodb估算当中提及的问题：
	/* The MySQL optimizer seems to believe an estimate of 0 rows is
	always accurate and may return the result 'Empty set' based on that.
	The accuracy is not guaranteed, and even if it were, for a locking
	read we should anyway perform the search to set the next-key lock.
	Add 1 to the value to make sure MySQL does not make the assumption! */
	if (rows == 0)
		++rows;

	return rows;
}

/**
 * 生成用于插入时扫描的句柄信息
 * @param session
 * @param key
 * @return 
 */
DrsIndexScanHandleInfo* DrsBPTreeIndex::prepareInsertScanInfo(Session *session, const SubRecord *key) {
	assert(KEY_COMPRESS == key->m_format || KEY_PAD == key->m_format);
	//生成一个扫描信息句柄
	MemoryContext *memoryContext = session->getMemoryContext();
	void *memory = memoryContext->alloc(sizeof(DrsIndexScanHandleInfo));
	DrsIndexScanHandleInfo *scanInfo = new (memory) DrsIndexScanHandleInfo(session, m_tableDef, 
		m_indexDef, None, Exclusived, (key->m_format == KEY_COMPRESS), 
		(key->m_format == KEY_COMPRESS ? RecordOper::compareKeyCC : RecordOper::compareKeyPC));

	scanInfo->m_findKey = IndexKey::allocSubRecord(memoryContext, key, m_indexDef);
	scanInfo->m_cKey1 = IndexKey::allocSubRecord(memoryContext, m_indexDef, KEY_COMPRESS);
	scanInfo->m_cKey2 = IndexKey::allocSubRecord(memoryContext, m_indexDef, KEY_COMPRESS);
	scanInfo->m_cKey3 = IndexKey::allocSubRecord(memoryContext, m_indexDef, KEY_COMPRESS);
	scanInfo->m_key0 = NULL;
	if (!scanInfo->m_isFastComparable) {	// 当无法快速比较，需要创建压缩键值待真正插入使用
		assert(key->m_format == KEY_PAD);
		scanInfo->m_key0 = IndexKey::convertKeyPC(memoryContext, key, m_tableDef, m_indexDef);
	}
	return scanInfo;
}


/**
 * 插入一个索引项
 *
 * @param session 会话句柄
 * @param key 索引键，格式一定是REC_REDUNDANT或者KEY_COMPRESS的
 * @param duplicateKey out 返回当前插入是否会产生唯一约束违背，如果违背，新键值不会被插入
 * @param checkDuplicate	指定是否需要检查唯一性，在回退操作的时候不需要进行唯一性验证，应该肯定可以回退，默认必须要检查
 * @return true表示插入成功，并且不违反唯一性约束，false表示插入不成功，可能由于违反唯一性约束，可能由于死锁导致
 */
bool DrsBPTreeIndex::insert(Session *session, const SubRecord *key, bool *duplicateKey, bool checkDuplicate) {
	assert(key != NULL);
	assert(key->m_format == KEY_PAD || key->m_format == KEY_COMPRESS);

	ftrace(ts.idx, tout << session << key << rid(key->m_rowId););

	McSavepoint mcs(session->getMemoryContext());
	DrsIndexScanHandleInfo *scanInfo = prepareInsertScanInfo(session, key);

	return insertIndexEntry(scanInfo, duplicateKey, checkDuplicate);
}

#ifdef TNT_ENGINE
/**
 * 查找唯一性索引是否存在重复键值
 * @param session
 * @param key
 * @param info
 * @return 
 */
bool DrsBPTreeIndex::checkDuplicate(Session *session, const SubRecord *key, DrsIndexScanHandleInfo **info) {
	assert(m_indexDef->m_unique);
	bool duplicateKey = false;
	IndexPage *page = NULL;

_findLeafLevel:
	u64 savePoint = session->getMemoryContext()->setSavepoint();

	DrsIndexScanHandleInfo *scanInfo = prepareInsertScanInfo(session, key);
	// 定位要插入的页面并对页面加锁
	u64 token = session->getToken();
	scanInfo->m_pageHandle = findSpecifiedLevel(scanInfo, &SearchFlag::DEFAULT_FLAG, 0, IDX_SEARCH);
	if (!scanInfo->m_pageHandle) {// 这里表示加SMO锁遇到死锁
		goto _dealWithDeadLock;
	}

	SYNCHERE(SP_IDX_RESEARCH_PARENT_IN_SMO);

	IDX_SWITCH_AND_GOTO(lockIdxObjectHoldingLatch(session, scanInfo->m_pageId, &(scanInfo->m_pageHandle), 
		FOR_INSERT, &DrsBPTreeIndex::judgerForDMLLocatePage, (void*)scanInfo, NULL), _restart, _dealWithDeadLock);

	page = (IndexPage*)scanInfo->m_pageHandle->getPage();

	// 查找插入键值在页面中的位置
	scanInfo->m_keyInfo.m_sOffset = 0;
	scanInfo->m_cKey1->m_size = 0;
	scanInfo->m_cKey2->m_size = 0;
	page->findKeyInPageTwo(scanInfo->m_findKey, scanInfo->m_comparator, &(scanInfo->m_cKey1), &(scanInfo->m_cKey2), 
		&(scanInfo->m_keyInfo));

	// 检查唯一性约束是否有冲突
	IDX_SWITCH_AND_GOTO(insertCheckSame(scanInfo, &duplicateKey), _restart, _dealWithDeadLock);

	if (!duplicateKey && NULL != info) {
		//不存在重复键值，保存当前LSN，释放页面Latch，保留pin
		scanInfo->m_pageLSN = scanInfo->m_pageHandle->getPage()->m_lsn;
		scanInfo->m_pageSMOLSN = page->m_smoLSN;		
		session->unlockPage(&scanInfo->m_pageHandle);
		assert(scanInfo->m_pageHandle->isPinned());
		assert(None == scanInfo->m_pageHandle->getLockMode());
		*info = scanInfo;
	} else {
		//存在重复键值，释放页面Latch以及pin
		session->releasePage(&scanInfo->m_pageHandle);
	}
	return duplicateKey;

_restart:
	idxUnlockObject(session, scanInfo->m_pageId, token);// 重启的话，需要释放已经持有的页面锁
	session->getMemoryContext()->resetToSavepoint(savePoint);
	goto _findLeafLevel;

_dealWithDeadLock:
	session->unlockIdxObjects(token);//死锁时需要释放所有的锁对象
	session->getMemoryContext()->resetToSavepoint(savePoint);
	statisticDL();
	goto _findLeafLevel;
}

#endif

/**
 * 在已经定位过的叶页面上插入键值
 * @param info 扫描句柄信息
 * @return true 一定成功
 */
bool DrsBPTreeIndex::insertGotPage(DrsIndexScanHandleInfo *scanInfo) {
	Session *session = scanInfo->m_session;
	u64 token = session->getToken();

	assert(NULL != scanInfo->m_pageHandle);
	assert(scanInfo->m_pageHandle->isPinned());//一定持有pin，但是不持有页面latch

	McSavepoint mcs(session->getMemoryContext());
	BufferPageHandle *pageHandle = scanInfo->m_pageHandle;
	LOCK_PAGE_HANDLE(session, pageHandle, scanInfo->m_latchMode);
	IndexPage *page = (IndexPage*)pageHandle->getPage();

	//判断页面LSN, 如果LSN没有发生变化，则可以直接在页面中插入键值
	if (page->m_lsn == scanInfo->m_pageLSN) {
		// 首先要保证该页面还是本索引的叶页面，并且页面有效，并且页面不是在SMO状态
		if (!page->isPageLeaf() || page->m_pageType == FREE || checkSMOBit(session, &pageHandle) != IDX_SUCCESS) {
			if (pageHandle != NULL)
				session->releasePage(&pageHandle);
			goto _insertBySearchFromRoot;
		}
		assert(IndexPage::getIndexId(page->m_pageMark) == m_indexId);

		IndexLog *logger = ((DrsBPTreeIndice*)m_indice)->getLogger();
		u64 beforeAddLSN = page->m_lsn;
		if (page->addIndexKey(session, logger, scanInfo->m_pageId, scanInfo->m_findKey, scanInfo->m_key0, 
			scanInfo->m_cKey1, scanInfo->m_cKey2, scanInfo->m_comparator, &(scanInfo->m_keyInfo)) == NEED_SPLIT) {
				if (beforeAddLSN != page->m_lsn) {
					// 插入操作有可能修改了页面(对于MiniPage已满，分裂了MiniPage成功，但是后续操作发现空间不够)
					// 这个情况需要标记当前页面为脏
					session->markDirty(scanInfo->m_pageHandle);
				}
				if (!insertSMOPrelockPages(session, &scanInfo->m_pageHandle)) {
					//发生死锁，需要释放所有的锁对象
					session->unlockIdxObjects(token);
					statisticDL();
					goto _insertBySearchFromRoot;
				}
				insertSMO(scanInfo);
				page = (IndexPage*)scanInfo->m_pageHandle->getPage();

				scanInfo->m_keyInfo.m_sOffset = 0;
				NTSE_ASSERT(page->addIndexKey(session, logger, scanInfo->m_pageId, scanInfo->m_findKey, 
					scanInfo->m_key0, scanInfo->m_cKey1, scanInfo->m_cKey2, scanInfo->m_comparator, 
					&(scanInfo->m_keyInfo)) == INSERT_SUCCESS);
		}
		session->markDirty(scanInfo->m_pageHandle);
		session->releasePage(&scanInfo->m_pageHandle);
		return true;
	}
	session->releasePage(&pageHandle);

_insertBySearchFromRoot:
	//从根节点开始查找插入位置
	loopInsertIfDeadLock(scanInfo);

	return true;
}

/**
 * 插入键值，不需要检查是否有键值重复
 * @param session 会话
 * @param key 待插入的键值
 */
void DrsBPTreeIndex::insertNoCheckDuplicate(Session *session, const SubRecord *key) {
	DrsIndexScanHandleInfo *scanInfo = prepareInsertScanInfo(session, key);	
	loopInsertIfDeadLock(scanInfo);
}

/**
 * 插入键值，如果死锁则重试
 * @param scanInfo
 */
void DrsBPTreeIndex::loopInsertIfDeadLock(DrsIndexScanHandleInfo *scanInfo) {
	assert(scanInfo && scanInfo->m_findKey);

	Session *session = scanInfo->m_session;
	bool duplicateKey = false;
	u64 savePoint = session->getMemoryContext()->setSavepoint();
	u64 token = session->getToken();

	while (!insertIndexEntry(scanInfo, &duplicateKey, true)) {
		//处理死锁
		NTSE_ASSERT(!duplicateKey);
		session->unlockIdxObjects(token);//需要释放所有的锁对象
		statisticDL();
		session->getMemoryContext()->resetToSavepoint(savePoint);
		token = session->getToken();
	}
	session->getMemoryContext()->resetToSavepoint(savePoint);
}


/**
 * 删除一个索引项
 *
 * @pre	调用该函数前应保证要删除的键值确实存在在索引当中，如果这里找不到，说明索引结构出错
 * @param session 会话句柄
 * @param key 索引键，格式一定是REC_REDUNDANT或者KEY_COMPRESS的
 */
bool DrsBPTreeIndex::del(Session *session, const SubRecord *key) {
	assert(key != NULL);
	assert(key->m_format == KEY_PAD || key->m_format == KEY_COMPRESS);

	ftrace(ts.idx, tout << session << key << rid(key->m_rowId););

	MemoryContext *memoryContext = session->getMemoryContext();
	u64 savePoint = memoryContext->setSavepoint();
	// 首先生成一个扫描信息句柄
	void *memory = memoryContext->alloc(sizeof(DrsIndexScanHandleInfo));
	DrsIndexScanHandleInfo *scanInfo = new (memory) DrsIndexScanHandleInfo(session, m_tableDef, m_indexDef, None, Exclusived, (key->m_format == KEY_COMPRESS), (key->m_format == KEY_COMPRESS ? RecordOper::compareKeyCC : RecordOper::compareKeyPC));

	scanInfo->m_findKey = IndexKey::allocSubRecord(memoryContext, key, m_indexDef);
	scanInfo->m_key0 = IndexKey::allocSubRecord(memoryContext, m_indexDef, KEY_PAD);
	scanInfo->m_cKey1 = IndexKey::allocSubRecord(memoryContext, m_indexDef, KEY_COMPRESS);
	scanInfo->m_cKey2 = IndexKey::allocSubRecord(memoryContext, m_indexDef, KEY_COMPRESS);
	scanInfo->m_cKey3 = IndexKey::allocSubRecord(memoryContext, m_indexDef, KEY_COMPRESS);

	bool result;
	if(!fetchUnique(scanInfo, &DrsBPTreeIndex::deleteIndexEntry, &result)) {	
		cout << "TNT: Table:" << m_tableDef->m_name << " Index: "
			<< m_indexDef->m_name << " could not delete key" << endl;
	}

	memoryContext->resetToSavepoint(savePoint);

	return result;
}


/**
 * 根据唯一性条件搜索索引
 *
 * @param session		会话句柄
 * @param key			条件，格式一定是KEY_PAD的，对于单纯使用键值无法唯一区分的索引，需要标志m_rowId为INVALID_ROW_ID
 * @param lockMode		对返回的记录要加的锁
 * @param rowId			OUT	命中记录时返回记录RID
 * @param subRecord		IN/OUT	输入指定要读取的属性，输出为读取的属性内容，可能为NULL，此时只返回记录RID
 * @param rlh			IN/OUT	如果LockMode=None该值为NULL，否则该值要返回加锁的句柄
 * @param extractor		若不为NULL则用此子记录提取器提取要读取的属性存储于subRecord，为NULL表示只返回RID
 * @return 找到记录时返回true，找不到返回false
 */
bool DrsBPTreeIndex::getByUniqueKey(Session *session, const SubRecord *key, LockMode lockMode, RowId *rowId, 
									SubRecord *subRecord, RowLockHandle **rlh, SubToSubExtractor *extractor) {
	PROFILE(PI_DrsIndex_getByUniqueKey);

	assert(key != NULL && key->m_format == KEY_PAD);
	assert(subRecord == NULL || subRecord->m_format == KEY_PAD);
	assert(!((lockMode != None) ^ (rlh != NULL)));

	ftrace(ts.idx, tout << session << key << rid(key->m_rowId) << lockMode;);

	m_indexStatus.m_dboStats->countIt(DBOBJ_SCAN);
	MemoryContext *memoryContext = session->getMemoryContext();
	u64 savePoint = memoryContext->setSavepoint();
	// 首先生成一个扫描信息句柄
	bool isFastComparable = RecordOper::isFastCCComparable(m_tableDef, m_indexDef, key->m_numCols, key->m_columns);
	void *memory = memoryContext->alloc(sizeof(DrsIndexScanHandleInfo));
	DrsIndexScanHandleInfo *scanInfo = new (memory) DrsIndexScanHandleInfo(session, m_tableDef, m_indexDef, 
		lockMode, ((lockMode == Exclusived ? Exclusived : Shared)), isFastComparable, 
		isFastComparable ? RecordOper::compareKeyCC : RecordOper::compareKeyPC);
	scanInfo->m_rowHandle = rlh;
	scanInfo->m_cKey1 = IndexKey::allocSubRecord(memoryContext, m_indexDef, KEY_COMPRESS);
	if (isFastComparable) {
		scanInfo->m_findKey = IndexKey::allocSubRecord(memoryContext, key->m_numCols, key->m_columns, KEY_COMPRESS, m_indexDef->m_maxKeySize);
		RecordOper::convertKeyPC(m_tableDef, m_indexDef, key, scanInfo->m_findKey);
	} else {
		scanInfo->m_findKey = IndexKey::allocSubRecord(memoryContext, key, m_indexDef);
	}

	if (!fetchUnique(scanInfo, NULL, NULL)) {
		memoryContext->resetToSavepoint(savePoint);
		return false;
	}

	*rowId = scanInfo->m_findKey->m_rowId;
	if (subRecord != NULL) {
		assert(extractor != NULL);
		extractor->extract(scanInfo->m_findKey, subRecord);
	}

	memoryContext->resetToSavepoint(savePoint);

	// 扫描成功后修改统计信息
	m_indexStatus.m_dboStats->countIt(DBOBJ_SCAN_ITEM);

	return true;
}


/**
 * 开始索引扫描
 *
 * @param session		会话句柄
 * @param key			搜索键，可能为NULL，若为NULL，则在forward为true时应定位到索引第一项，forward为false时定位到最后一项，若不为NULL，格式一定是KEY_PAD的
 * @param forward		是否为前向搜索
 * @param includeKey	是否包含key本身（>=/<=条件时为true，>/<条件时为false）
 * @param lockMode		对返回的记录要加的锁
 * @param rlh			IN/OUT	如果LockMode=None该值为NULL，否则该值要返回加锁的句柄
 * @param extractor		若不为NULL则在getNext时用此子记录提取器提取要读取的属性存储于subRecord，为NULL表示只返回RID
 * @return	构造好的扫描句柄
 */
IndexScanHandle* DrsBPTreeIndex::beginScan(Session *session, const SubRecord *key, bool forward, bool includeKey, LockMode lockMode, RowLockHandle **rlh, SubToSubExtractor *extractor) {
	PROFILE(PI_DrsIndex_beginScan);

	assert(!((lockMode != None) ^ (rlh != NULL)));
	assert(key == NULL || key->m_format == KEY_PAD);

	// 增加统计信息
	m_indexStatus.m_dboStats->countIt(DBOBJ_SCAN);
	m_indexStatus.m_backwardScans = m_indexStatus.m_backwardScans + (forward ? 0 : 1);

	MemoryContext *memoryContext = session->getMemoryContext();
	void *memory = memoryContext->alloc(sizeof(DrsIndexRangeScanHandle));
	DrsIndexRangeScanHandle *scanHandler = new (memory) DrsIndexRangeScanHandle();

	bool isFastComparable = (key == NULL) ? RecordOper::isFastCCComparable(m_tableDef, m_indexDef, m_indexDef->m_numCols, m_indexDef->m_columns) : 
		RecordOper::isFastCCComparable(m_tableDef, m_indexDef, key->m_numCols, key->m_columns);
	memory = memoryContext->alloc(sizeof(DrsIndexScanHandleInfo));
	DrsIndexScanHandleInfo *scanInfo = new (memory) DrsIndexScanHandleInfo(session, m_tableDef, m_indexDef, lockMode, ((lockMode == Exclusived ? Exclusived : Shared)), isFastComparable, isFastComparable ? RecordOper::compareKeyCC : RecordOper::compareKeyPC);

	scanInfo->m_includeKey = includeKey;
	scanInfo->m_forward = forward;
	scanInfo->m_rowHandle = rlh;
	scanInfo->m_uniqueScan = false;
	scanInfo->m_extractor = extractor;

	if (key != NULL) {
		if (isFastComparable) {
			scanInfo->m_findKey = IndexKey::allocSubRecord(memoryContext, key->m_numCols, key->m_columns, KEY_COMPRESS, m_indexDef->m_maxKeySize);
			RecordOper::convertKeyPC(m_tableDef, m_indexDef, key, scanInfo->m_findKey);
		} else {
			scanInfo->m_findKey = IndexKey::allocSubRecord(memoryContext, key, m_indexDef);
		}
	} else {
		scanInfo->m_findKey = IndexKey::allocSubRecord(memoryContext, m_indexDef, KEY_PAD);
		scanInfo->m_findKey->m_size = 0;	// 表示没有查找键值
	}

	scanInfo->m_key0 = IndexKey::allocSubRecord(memoryContext, m_indexDef, KEY_COMPRESS);
	scanInfo->m_cKey1 = IndexKey::allocSubRecord(memoryContext, m_indexDef, KEY_COMPRESS);
	if (lockMode == Exclusived) {
		scanInfo->m_cKey2 = IndexKey::allocSubRecord(memoryContext, m_indexDef, KEY_COMPRESS);
		scanInfo->m_cKey3 = IndexKey::allocSubRecord(memoryContext, m_indexDef, KEY_COMPRESS);
	}

	scanHandler->init(scanInfo);

	return scanHandler;
}


/**
 * 得到下一个搜索结果（当forward为false时实际是前一项）
 *
 * @param scanHandle	扫描句柄
 * @param key			out 用来保存查找到键值的SubRecord。如果为NULL，表示只需要取得RowId
 * @return 是否有下一个结果
 */
bool DrsBPTreeIndex::getNext(IndexScanHandle *scanHandle, SubRecord *key) {
	PROFILE(PI_DrsIndex_getNext);

	assert(scanHandle != NULL);

	DrsIndexRangeScanHandle *rangeHandle = (DrsIndexRangeScanHandle*)scanHandle;
	rangeHandle->setSubRecord(key);
	DrsIndexScanHandleInfo *scanInfo = rangeHandle->getScanInfo();
	assert(scanInfo->m_hasNext);	// 该断言在目前成立，如果以后引入索引游标反向使用，则不能使用这个断言

	if (!fetchNext(scanInfo))
		return false;

	scanInfo->m_rangeFirst = false;
	assert(rangeHandle->getScanInfo()->m_lockMode == None || rangeHandle->getScanInfo()->m_rowHandle != NULL);
	rangeHandle->setRowId(scanInfo->m_findKey->m_rowId);
	rangeHandle->saveKey(scanInfo->m_findKey);

	// 扫描成功后修改统计信息
	m_indexStatus.m_dboStats->countIt(DBOBJ_SCAN_ITEM);
	if (!scanInfo->m_forward)
		++m_indexStatus.m_rowsBScanned;

	return true;
}

/**
 * 删除当前扫描的记录
 *
 * @param scanHandle 扫描句柄
 * @return true表示删除成功，false表示出现死锁
 */
bool DrsBPTreeIndex::deleteCurrent(IndexScanHandle *scanHandle) {
	PROFILE(PI_DrsIndex_deleteCurrent);

	DrsIndexScanHandleInfo *scanInfo = ((DrsIndexRangeScanHandle*)scanHandle)->getScanInfo();
	if (scanInfo->m_pageHandle == NULL) {	// 这里不能保证一定已经持有页面的句柄
		researchForDelete(scanInfo);
		return deleteIndexEntry(scanInfo, true);
	}

	return deleteIndexEntry(scanInfo, false);
}


/**
 * 结束索引扫描
 *
 * @param scanHandle 扫描句柄
 */
void DrsBPTreeIndex::endScan(IndexScanHandle *scanHandle) {
	PROFILE(PI_DrsIndex_endScan);

	DrsIndexRangeScanHandle *handle = (DrsIndexRangeScanHandle*)scanHandle;
	DrsIndexScanHandleInfo *scanInfo = handle->getScanInfo();
	if (scanInfo->m_pageHandle != NULL)
		scanInfo->m_session->unpinPage(&(scanInfo->m_pageHandle));
}

#ifdef TNT_ENGINE

/**
 * 开始索引扫描(改进版本)
 * @param session
 * @param key
 * @param forward
 * @param includeKey
 * @param lockMode
 * @param rlh
 * @param extractor
 * @return 
 */
IndexScanHandle* DrsBPTreeIndex::beginScanSecond(Session *session, const SubRecord *key, bool forward, 
												 bool includeKey, LockMode lockMode, RowLockHandle **rlh, 
												 SubToSubExtractor *extractor, TLockMode trxLockMode) {
	PROFILE(PI_DrsIndex_beginScan);

	assert(!((lockMode != None) ^ (rlh != NULL)));
	assert(key == NULL || key->m_format == KEY_PAD);

	// 增加统计信息
	m_indexStatus.m_dboStats->countIt(DBOBJ_SCAN);
	m_indexStatus.m_backwardScans = m_indexStatus.m_backwardScans + (forward ? 0 : 1);

	MemoryContext *memoryContext = session->getMemoryContext();
	void *memory = memoryContext->alloc(sizeof(DrsIndexRangeScanHandle));
	DrsIndexRangeScanHandle *scanHandler = new (memory) DrsIndexRangeScanHandle();

	bool isFastComparable = (key == NULL) ? RecordOper::isFastCCComparable(m_tableDef, m_indexDef, 
		m_indexDef->m_numCols, m_indexDef->m_columns) : 
		RecordOper::isFastCCComparable(m_tableDef, m_indexDef, key->m_numCols, key->m_columns);
	memory = memoryContext->alloc(sizeof(DrsIndexScanHandleInfoExt));
	DrsIndexScanHandleInfoExt *scanInfo = new (memory) DrsIndexScanHandleInfoExt(session, m_tableDef, 
		m_indexDef, lockMode, ((lockMode == Exclusived ? Exclusived : Shared)), isFastComparable, 
		isFastComparable ? RecordOper::compareKeyCC : RecordOper::compareKeyPC, trxLockMode);

	scanInfo->m_includeKey = includeKey;
	scanInfo->m_forward = forward;
	scanInfo->m_rowHandle = rlh;
	scanInfo->m_uniqueScan = false;
	scanInfo->m_extractor = extractor;
	scanInfo->m_tableDef = m_tableDef;
	scanInfo->m_backupRangeFirst = true;

	if (key != NULL) {
		assert(IndexKey::isKeyValid(key));
		if (isFastComparable) {
			scanInfo->m_findKey = IndexKey::allocSubRecord(memoryContext, key->m_numCols, 
				key->m_columns, KEY_COMPRESS, m_indexDef->m_maxKeySize);
			scanInfo->m_findKeyBufSize = scanInfo->m_findKey->m_size;
			RecordOper::convertKeyPC(m_tableDef, m_indexDef, key, scanInfo->m_findKey);
		} else {
			scanInfo->m_findKey = IndexKey::allocSubRecord(memoryContext, key, m_indexDef);
			scanInfo->m_findKeyBufSize = scanInfo->m_findKey->m_size;
		}
	} else {
		scanInfo->m_findKey = IndexKey::allocSubRecord(memoryContext, m_indexDef, KEY_PAD);
		scanInfo->m_findKeyBufSize = scanInfo->m_findKey->m_size;
		scanInfo->m_findKey->m_size = 0;	// 表示没有查找键值
	}
	assert(scanInfo->m_findKeyBufSize > 0);

	scanInfo->m_key0 = IndexKey::allocSubRecord(memoryContext, m_indexDef, KEY_COMPRESS);
	scanInfo->m_cKey1 = IndexKey::allocSubRecord(memoryContext, m_indexDef, KEY_COMPRESS);
	if (lockMode == Exclusived) {
		scanInfo->m_cKey2 = IndexKey::allocSubRecord(memoryContext, m_indexDef, KEY_COMPRESS);
		scanInfo->m_cKey3 = IndexKey::allocSubRecord(memoryContext, m_indexDef, KEY_COMPRESS);
	}

	scanHandler->init(scanInfo);

	return scanHandler;
}

/**
 * 获取下一项(改进版本)
 * @param scanHandle 范围扫描句柄
 * @return 是否存在下一项
 */
bool DrsBPTreeIndex::getNextSecond(IndexScanHandle *scanHandle) throw(NtseException) {
	PROFILE(PI_DrsIndex_getNext);

	assert(scanHandle != NULL);

	DrsIndexRangeScanHandle *rangeHandle = (DrsIndexRangeScanHandle*)scanHandle;
	DrsIndexScanHandleInfoExt *scanInfo = (DrsIndexScanHandleInfoExt*)rangeHandle->getScanInfo();
	assert(scanInfo->m_hasNext);	// 该断言在目前成立，如果以后引入索引游标反向使用，则不能使用这个断言

	if (!fetchNextSecond(scanInfo))
		return false;

	scanInfo->m_rangeFirst = false;
	assert(rangeHandle->getScanInfo()->m_lockMode == None || rangeHandle->getScanInfo()->m_rowHandle != NULL);
	rangeHandle->setRowId(scanInfo->m_readKey->m_rowId);
	rangeHandle->saveKey(scanInfo->m_readKey);

	// 扫描成功后修改统计信息
	m_indexStatus.m_dboStats->countIt(DBOBJ_SCAN_ITEM);
	if (!scanInfo->m_forward)
		++m_indexStatus.m_rowsBScanned;

	return true;
}

/**
 * 结束范围扫描
 * @param scanHandle 范围扫描句柄
 */
void DrsBPTreeIndex::endScanSecond(IndexScanHandle *scanHandle) {
	PROFILE(PI_DrsIndex_endScan);

	DrsIndexRangeScanHandle *handle = (DrsIndexRangeScanHandle*)scanHandle;
	DrsIndexScanHandleInfoExt *scanInfo = (DrsIndexScanHandleInfoExt*)handle->getScanInfo();
	/*
	// Unique扫描的时候可能最后一次getNext正好跨页，并且跨页后的第一项不符合条件
	// 此时endScan时持有两个页面的pin，都需要释放
	*/
	if(scanInfo->m_pageHandle != NULL && scanInfo->m_pageHandle != scanInfo->m_readPageHandle) {
		scanInfo->m_session->unpinPage(&(scanInfo->m_pageHandle));
	}

	if (scanInfo->m_readPageHandle != NULL) {
		scanInfo->m_session->unpinPage(&(scanInfo->m_readPageHandle));
	}
}

/**
 * 改进的查找下一项的真正实现，只读取并且不更新游标
 * @param info
 * @return 
 */
bool DrsBPTreeIndex::fetchNextSecond(DrsIndexScanHandleInfoExt *info) throw(NtseException) {
FetchRestart:
	Session *session = info->m_session;
	bool needFetchNext = false;
	s32 result = -1;

	info->m_readPageId = info->m_pageId;
	info->m_readPageHandle = info->m_pageHandle;
	info->m_readPageLSN = info->m_pageLSN;
	info->m_readPageSMOLSN = info->m_pageSMOLSN;
	info->m_readKeyInfo = info->m_keyInfo;
	info->m_rangeFirst = info->m_backupRangeFirst;

	if (!locateLeafPageAndFindKeySecond(info, &needFetchNext, &result, info->m_forceSearchPage))
		goto FetchFail;

	// 保存当前LSN
	info->m_forceSearchPage = false;
	info->m_readPageLSN = info->m_readPageHandle->getPage()->m_lsn;
	info->m_readPageSMOLSN = ((IndexPage*)info->m_readPageHandle->getPage())->m_smoLSN;

	if (needFetchNext) {
		IDX_SWITCH_AND_GOTO(readNextKey(info), FetchRestart, FetchFail);
	} else {
		// 在持有页面latch的情况下，加NTSE行锁
		IDXResult ret = tryTrxLockHoldingLatch(info,  &info->m_readPageHandle, info->m_cKey1);
		if(ret == IDX_RESTART) {
			//如果重启，或者游标页面是NULL，此时read页面已被释放，或者游标页面和read页面相同，也已经被释放
			info->m_pageHandle = NULL;
			info->m_readPageHandle = NULL;
			goto FetchRestart;
		} else if(ret == IDX_FAIL)
			goto FetchFail;

		//释放页面Latch返回成功
		info->m_session->unlockPage(&(info->m_readPageHandle));
	}

	IndexKey::swapKey(&(info->m_readKey), &(info->m_cKey1));
	return true;

FetchFail:
	info->m_pageId = info->m_readPageHandle->getPageId();
	info->m_pageHandle = info->m_readPageHandle;
	session->unlockPage(&(info->m_readPageHandle));
	return false;
}

/**
 * 读取当前定位到的索引项的下一项
 * @param info
 * @return 
 */
IDXResult DrsBPTreeIndex::readNextKey(DrsIndexScanHandleInfoExt *info) throw(NtseException) {
	PageHandle *handle = info->m_readPageHandle;
	PageHandle *nextHandle = NULL;
	IndexPage *page = (IndexPage*)handle->getPage();
	PageId nextPageId = INVALID_PAGE_ID;
	bool spanPage = false;
	bool forward = info->m_forward;
	u16 offset;

	SubRecord *key = info->m_cKey1;
	SubRecord *lastRecord = (info->m_findKey->m_format == KEY_COMPRESS ? info->m_findKey : 
		info->m_cKey1);	// 如果findKey格式不是压缩的,说明可能经过重定位或者是第一次查找,无论如何这时候可能的前一个键值都只会是m_cKey1
	KeyInfo *keyInfo = &(info->m_readKeyInfo);

	if (forward) {
		offset = page->getNextKey(lastRecord, keyInfo->m_eOffset, true, &(info->m_readKeyInfo.m_miniPageNo), key);
		if (!IndexKey::isKeyValid(key)) {
			nextPageId = page->m_nextPage;
			spanPage = true;
		}
		info->m_readKeyInfo.m_sOffset = keyInfo->m_eOffset;
		info->m_readKeyInfo.m_eOffset = offset;
	} else { 
		offset = page->getPrevKey(keyInfo->m_sOffset, true, &(info->m_readKeyInfo.m_miniPageNo), key);
		if (!IndexKey::isKeyValid(key)) {
			nextPageId = page->m_prevPage;
			spanPage = true;
		}
		info->m_readKeyInfo.m_eOffset = keyInfo->m_sOffset;
		info->m_readKeyInfo.m_sOffset = offset;
	}

	if (!spanPage) {
		// 在持有页面latch的情况下，加NTSE行锁
		IDX_SWITCH_AND_GOTO(tryTrxLockHoldingLatch(info, &info->m_readPageHandle, key), 
			SpanPageRestart, SpanPageFail);

	} else {
		// 需要跨页面查找
		if (nextPageId == INVALID_PAGE_ID) {	// 扫描到达索引边界
			return IDX_FAIL;
		}

		Session *session = info->m_session;
		PageId pageId = info->m_readPageId;
		File *file = ((DrsBPTreeIndice*)m_indice)->getFileDesc();
		LockMode latchMode = info->m_latchMode;
		u64 pageLSN = page->m_lsn;

		session->releasePage(&handle);

		SYNCHERE(SP_IDX_WANT_TO_GET_PAGE1);

		nextHandle = GET_PAGE(session, file, PAGE_INDEX, nextPageId, latchMode, m_dboStats, NULL);
		//由于跨页面，必然不会影响到游标页面，所以此处restart不需要重置游标页面
		IDX_SWITCH_AND_GOTO(checkSMOBit(session, &nextHandle), SpanPageRestart, SpanPageFail);
		IDX_SWITCH_AND_GOTO(latchHoldingLatch(session, pageId, nextPageId, &nextHandle, &handle, latchMode), 
			SpanPageRestart, SpanPageFail);
		page = (IndexPage*)handle->getPage();

		if (pageLSN != page->m_lsn) {
			session->releasePage(&handle);
			session->releasePage(&nextHandle);
			goto SpanPageRestart;
		}

		page = (IndexPage*)nextHandle->getPage();
		page->getExtremeKey(key, forward, &info->m_readKeyInfo);
		assert(IndexKey::isKeyValid(key));	// 一定能取到索引项

		try {
			// 在持有页面latch的情况下，加NTSE行锁以及TNT事务锁
			IDX_SWITCH_AND_GOTO(tryTrxLockHoldingTwoLatch(info, &nextHandle, &handle, key), SpanPageRestart, SpanPageFail);
		} catch (NtseException &e) {
		//	session->releasePage(&handle);
			throw e;
		}

		info->m_readPageId = nextPageId;
		info->m_readPageHandle = nextHandle;
		info->m_readPageLSN = nextHandle->getPage()->m_lsn;
		info->m_readPageSMOLSN = ((IndexPage *)nextHandle->getPage())->m_smoLSN;

		// 释放下一页面的Latch, 不释放Pin
		session->unlockPage(&nextHandle);
	}

	if (!spanPage)
		info->m_session->unlockPage(&handle);
	else {
		if(info->m_pageId != handle->getPageId()) {
			// 如果跨页前的页面不是游标页，那只能才此处释放LATCH和PIN
			info->m_session->releasePage(&handle);
		} else {
			// 由于游标页被realease过，可能被写出又重新读入，因此要更新游标页指针
			info->m_pageHandle = handle;
			info->m_session->unlockPage(&handle);
		}	
	}
	
	return IDX_SUCCESS;

SpanPageRestart:
	//此处需要放掉所有的latch和pin
	if(spanPage && handle != NULL) {
		info->m_session->releasePage(&handle);
	} 
	info->m_pageHandle = NULL;
	info->m_readPageHandle = NULL;

	return IDX_RESTART;

SpanPageFail:
	info->m_pageHandle = NULL;
	assert(false);
	return IDX_FAIL;	
}

/**
 * 在持有页面latch的情况下，尝试加事务级行锁
 *
 * @pre 要加事务锁的记录所在的索引页面的latch已经加上
 * @param info 外存索引扫描句柄信息
 * @param pageHdl 要加事务锁的记录所在的索引页面
 * @param key 要加事务锁的记录
 * @throw NtseException 锁表空间不足等
 * @return 三阶段加锁成功返回IDX_SUCCESS，此时持有页面latch，NTSE行锁以及事务锁；
 *         加锁失败返回IDX_RESTART，由上层重启整个查找过程，此时仍持有页面latch
 */
IDXResult DrsBPTreeIndex::tryTrxLockHoldingLatch(DrsIndexScanHandleInfoExt *info, PageHandle **pageHdl,
												 const SubRecord *key) throw(NtseException) {
	Session *session = info->m_session;
	RowId keyRowId = key->m_rowId;// 运行到这里，下一项的键值肯定保存在record指向的m_cKey1当中

	if (info->m_lockMode != None) {
		// 检查NTSE行锁是否已经加上
		assert(info->m_lockMode == Shared);
		if (!session->isRowLocked(m_tableDef->m_id, keyRowId, info->m_lockMode)) {
			return lockHoldingLatch(session, keyRowId, info->m_lockMode, 
				pageHdl, info->m_rowHandle, true, info);
		}
	}
	return IDX_SUCCESS;	
}




/**
 * 当lockHoldingLatch加锁成功，但是页面LSN改变的时候，可以用该函数来确定加锁键值是不是还在当前页面,(用于TNT)
 * @pre info结构当中idxKey1是本次加锁的键值对象，m_findKey是本次最初要定位的键值，m_pageHandle是表示加了锁的页面信息句柄
 * @post idxKey1里面会保存重新搜索的键值的信息，如果返回false，info里面的keyInfo信息都作废，否则，keyInfo表示当前新的找到键值的信息
 * @param info		扫描信息句柄
 * @param inRange	out 要查找的键值是否在当前页面范围内
 * @return true表示要加锁的键值还在当前页面，false表示不在
 */
bool DrsBPTreeIndex::researchScanKeyInPageSecond(DrsIndexScanHandleInfo *info, bool *inRange) {
	// 首先保证页面有效性，无效或者空页面，肯定找不到
	DrsIndexScanHandleInfoExt *infoExt = (DrsIndexScanHandleInfoExt*)info; 
	IndexPage *page = (IndexPage*)infoExt->m_readPageHandle->getPage();
	if (!page->isPageLeaf() || page->isPageSMO() || page->isPageEmpty())
		return false;

	SubRecord *gotKey = infoExt->m_cKey1;
	RowId origRowId = gotKey->m_rowId;
	KeyInfo *keyInfo = &infoExt->m_readKeyInfo;

	makeFindKeyPADIfNecessary(infoExt);
	SearchFlag flag(infoExt->m_forward, infoExt->m_includeKey, (infoExt->m_uniqueScan || !infoExt->m_rangeFirst));
	bool fetchNext = page->findKeyInPage(infoExt->m_findKey, &flag, infoExt->m_comparator, gotKey, keyInfo);
	assert(IndexKey::isKeyValid(gotKey));

	// 唯一查询必须相等才是有效，否则可能刚好是被其他线程更新，但是位置和rowId没有发生改变
	if (infoExt->m_uniqueScan)
		return (*inRange = (keyInfo->m_result == 0 && gotKey->m_rowId == origRowId));

	// 根据查找结果判断是否应该取左右的下一项
	if (fetchNext) {
		if (infoExt->m_forward) {
			keyInfo->m_sOffset = keyInfo->m_eOffset;
			keyInfo->m_eOffset = page->getNextKey(gotKey, keyInfo->m_eOffset, true, &keyInfo->m_miniPageNo, gotKey);
			keyInfo->m_result = -1;
		} else {
			keyInfo->m_eOffset = keyInfo->m_sOffset;
			keyInfo->m_sOffset = page->getPrevKey(keyInfo->m_sOffset, true, &keyInfo->m_miniPageNo, gotKey);
			keyInfo->m_result = 1;
		}
	}

	// 范围查询，定位到查找范围，然后根据结果调整之后，如果rowId一致，说明现在定位的项是有效正确的；
	// 如果rowId无法一致肯定不准确，可能是原来定位的项被删除或者更新，或者范围之内又插入其他项，
	// 总之此时必须重新查找
	*inRange = page->isKeyInPageMiddle(keyInfo);
	return (*inRange && IndexKey::isKeyValid(gotKey) && gotKey->m_rowId == origRowId);
}



/**
 * 估计索引键值在[min,max]之间的记录数
 * @pre min和max应该是所以上顺序从小到大的两个键值
 *
 * @param session 会话
 * @param minKey 下限，若为NULL则表示不指定下限，否则一定是PAD格式得键值
 * @param includeKeyMin	下限模式，true是>=/false是>
 * @param maxKey 上限，若为NULL则表示不指定上限，否则一定是PAD格式得键值
 * @param includeKeyMax	上限模式，true是<=/false是<
 * @return 在[min,max]之间的记录数的估计值
 */
u64 DrsBPTreeIndex::recordsInRangeSecond(Session *session, const SubRecord *minKey, bool includeKeyMin, const SubRecord *maxKey, bool includeKeyMax) {
	PROFILE(PI_DrsIndex_recordsInRange);

	MemoryContext *memoryContext = session->getMemoryContext();
	bool singleroot = false;
	PageId minLeafPageId = INVALID_PAGE_ID, maxLeafPageId = INVALID_PAGE_ID;
	struct IndexSearchTraceInfo	traceMin[MAX_INDEX_LEVEL], traceMax[MAX_INDEX_LEVEL];
	u16 minLeafPageCount = 0, maxLeafPageCount = 0;
	u16 minLeafKeyCount = 0, maxLeafKeyCount = 0;
	SearchFlag minFlag(true, includeKeyMin, false);
	SearchFlag maxFlag(false, includeKeyMax, false);
	bool minFetchNext = false, maxFetchNext = false;
	KeyInfo keyInfo;

	void *memory = memoryContext->alloc(sizeof(DrsIndexScanHandleInfo));
	DrsIndexScanHandleInfo *scanInfo = new (memory) DrsIndexScanHandleInfo(session, m_tableDef, m_indexDef, None, Shared, false, RecordOper::compareKeyPC);
	scanInfo->m_key0 = IndexKey::allocSubRecord(memoryContext, m_indexDef, KEY_COMPRESS);
	scanInfo->m_cKey1 = IndexKey::allocSubRecord(memoryContext, m_indexDef, KEY_COMPRESS);

	// 查找小键值信息
	estimateKeySearch(minKey, scanInfo, &minFlag, &singleroot, &minFetchNext, &minLeafKeyCount, &minLeafPageCount);
	memcpy(traceMin, scanInfo->m_traceInfo, sizeof(struct IndexSearchTraceInfo) * MAX_INDEX_LEVEL);
	minLeafPageId = scanInfo->m_pageId;

	memset(scanInfo->m_traceInfo, 0, sizeof(struct IndexSearchTraceInfo) * MAX_INDEX_LEVEL);
	scanInfo->m_pageId = INVALID_PAGE_ID;

	// 再搜索大键值信息
	estimateKeySearch(maxKey, scanInfo, &maxFlag, &singleroot, &maxFetchNext, &maxLeafKeyCount, &maxLeafPageCount);
	memcpy(traceMax, scanInfo->m_traceInfo, sizeof(struct IndexSearchTraceInfo) * MAX_INDEX_LEVEL);
	maxLeafPageId = scanInfo->m_pageId;

	// 估算键值数
	s16 level = MAX_INDEX_LEVEL;
	u64 rows = 1;
	bool diverged = false;    //标识是否分开在不同页面上
	bool diverged_lot = false;	//标识是否分开在不同页面，并且不在相邻页面上
	while (true) {
		--level;
		if (level < 0)
			break;

		bool minStart = (traceMax[level].m_pageId != 0), maxStart = (traceMin[level].m_pageId != 0);
		if (minStart ^ maxStart)
			// 表示搜索过程中索引层数已经发生变化，返回估算值
			return 10;
		else if (!minStart)
			continue;

		if (traceMin[level].m_pageId == traceMax[level].m_pageId) {
			if (traceMin[level].pi.ei.m_keyCount != traceMax[level].pi.ei.m_keyCount) {	// 当前层开始分开了
				if (traceMin[level].pi.ei.m_keyCount < traceMax[level].pi.ei.m_keyCount) {	// 正确情况
					diverged = true;
					rows = max(traceMax[level].pi.ei.m_keyCount - traceMin[level].pi.ei.m_keyCount, 1);
					if(rows > 1) {
						diverged_lot = true;
					}
				} else {	// 两次定位之间可能出现索引树被修改，定位不准确，返回估计值
					return 10;
				}
			}
			continue;
		} else if (diverged && !diverged_lot){	// 表示路径已经不同，但是是在相邻的页面上
			if (traceMin[level].pi.ei.m_keyCount < traceMin[level].pi.ei.m_pageCount 
				|| traceMax[level].pi.ei.m_keyCount > 1) {
					diverged_lot = true;
					rows = 0;
					if (traceMin[level].pi.ei.m_keyCount < traceMin[level].pi.ei.m_pageCount) {
						rows += traceMin[level].pi.ei.m_pageCount - traceMin[level].pi.ei.m_keyCount;
					}
					if (traceMax[level].pi.ei.m_keyCount > 1) {
						rows += traceMax[level].pi.ei.m_keyCount - 1;
					}
			}
		} else if (diverged_lot) {		//路径不同，且不是在相邻页面上
			rows *= (traceMin[level].pi.ei.m_pageCount + traceMax[level].pi.ei.m_pageCount) / 2;
		}
	}

	// 计算叶节点一层
	if (diverged || minLeafPageId != maxLeafPageId) {
		rows *= (minLeafPageCount + maxLeafPageCount) / 2;
	} else if (singleroot || !diverged) {
		rows = maxLeafKeyCount - minLeafKeyCount;
		if (minFetchNext)
			rows--;
		if (!maxFetchNext)
			rows++;
	} else {
		rows = 10;
	}

	// 处理0的情况，解决Innodb估算当中提及的问题：
	/* The MySQL optimizer seems to believe an estimate of 0 rows is
	always accurate and may return the result 'Empty set' based on that.
	The accuracy is not guaranteed, and even if it were, for a locking
	read we should anyway perform the search to set the next-key lock.
	Add 1 to the value to make sure MySQL does not make the assumption! */
	if (rows == 0)
		++rows;

	return rows;
}

/**
 * 在持有页面latch的情况下，尝试加事务级行锁
 *
 * @pre 要加事务锁的记录所在的索引页面的latch已经加上
 * @param info 外存索引扫描句柄信息
 * @param pageHdl 要加事务锁的记录所在的索引页面
 * @param pageHdl2 游标所在前页面
 * @param key 要加事务锁的记录
 * @throw NtseException 锁表空间不足等
 * @return 三阶段加锁成功返回IDX_SUCCESS，此时持有页面latch，NTSE行锁以及事务锁；
 *         加锁失败返回IDX_RESTART，由上层重启整个查找过程，此时仍持有页面latch
 */
IDXResult DrsBPTreeIndex::tryTrxLockHoldingTwoLatch(DrsIndexScanHandleInfoExt *info, PageHandle **pageHdl, PageHandle **pageHdl2,
												 const SubRecord *key) throw(NtseException) {
	Session *session = info->m_session;
	RowId keyRowId = key->m_rowId;// 运行到这里，下一项的键值肯定保存在record指向的m_cKey1当中

	if (info->m_lockMode != None) {
		// 检查NTSE行锁是否已经加上
		assert(info->m_lockMode == Shared);
		if (!session->isRowLocked(m_tableDef->m_id, keyRowId, info->m_lockMode)) {
			return lockHoldingTwoLatch(session, keyRowId, info->m_lockMode, 
				pageHdl, pageHdl2, info->m_rowHandle, true, info);
		}
	}
	return IDX_SUCCESS;	
}


/**
 * 在持有两个页面Latch的情况下，加页面某个键值的行锁，如果加锁成功，既持有锁也持有Latch（跨页的时候使用）
 *
 * @param session			会话句柄
 * @param rowId				要加锁键值的rowId
 * @param lockMode			加锁模式
 * @param pageHandle		INOUT	页面句柄
 * @param pageHdl2			INOUT   游标所在前页面
 * @param rowHandle			OUT		行锁句柄
 * @param mustReleaseLatch	决定在restart的时候是不是可以不放页面的pin
 * @param info				扫描句柄，用于在页面LSN改变的时候，判断查找项是不是依然在该页面，默认值为NULL，如果为NULL，就不做这项判断
 * @return 返回成功IDX_SUCCESS，失败IDX_FAIL，需要重新开始IDX_RESTART
 */
IDXResult DrsBPTreeIndex::lockHoldingTwoLatch(Session *session, RowId rowId, LockMode lockMode, PageHandle **pageHandle, PageHandle **pageHandle2, RowLockHandle **rowHandle, bool mustReleaseLatch, DrsIndexScanHandleInfo *info) {
	UNREFERENCED_PARAMETER(info);
	UNREFERENCED_PARAMETER(mustReleaseLatch);

	assert(lockMode != None && rowHandle != NULL);

	u16 tableId = ((DrsBPTreeIndice*)m_indice)->getTableId();
	if ((*rowHandle = TRY_LOCK_ROW(session, tableId, rowId, lockMode)) != NULL)
		return IDX_SUCCESS;

	// 否则需要先释放本页面的Latch再无条件加锁

	//如果是tnt，不管如何，只要需要重启扫描，就必须释放这个页面,
	session->releasePage(pageHandle);
	session->releasePage(pageHandle2);

	*rowHandle = LOCK_ROW(session, tableId, rowId, lockMode);

	++m_indexStatus.m_numRLRestarts;	// 统计信息

	session->unlockRow(rowHandle);

		
	return IDX_RESTART;
}

#endif

/////////////////////////////////////////////////////////////////////////////////////
// Search

/**
 * 取扫描的下一项
 * @param info	扫描信息
 * @return 是否还存在下一项
 */
bool DrsBPTreeIndex::fetchNext(DrsIndexScanHandleInfo *info) {
	Session *session = info->m_session;
	bool needFetchNext;
	s32 result;

FetchStart:
	if (!locateLeafPageAndFindKey(info, &needFetchNext, &result))
		goto FetchFail;

	if (needFetchNext) {
		IDX_SWITCH_AND_GOTO(shiftToNextKey(info), FetchStart, FetchFail);

		if (!info->m_hasNext) {	// 不存在下一项
			goto FetchFail;
		}
	}

	// 运行到这里，下一项的键值肯定保存在record指向的m_idxKey1当中
	if (info->m_lockMode != None) {
		IDX_SWITCH_AND_GOTO(lockHoldingLatch(session, info->m_cKey1->m_rowId, info->m_lockMode, 
			&info->m_pageHandle, info->m_rowHandle, false, info), FetchStart, FetchFail);
		if (info->m_lockMode == Exclusived) {
			nftrace(ts.idx, tout << info->m_session->getId() << " Lock " << 
				rid(info->m_cKey1->m_rowId) << " for dml in page: " << info->m_pageId;);
		}
	}

	// 保存当前LSN，释放页面Latch返回成功
	info->m_pageLSN = info->m_pageHandle->getPage()->m_lsn;
	info->m_pageSMOLSN = ((IndexPage*)info->m_pageHandle->getPage())->m_smoLSN;
	session->unlockPage(&(info->m_pageHandle));

	// 这里要判断做记录的调换，保证除了第一次的查找之外，所有查找使用的SubRecord都能完整保存索引键值
	if (info->m_findKey->m_format == KEY_PAD) {
		info->m_findKey->m_columns = info->m_key0->m_columns;
		info->m_findKey->m_numCols = info->m_key0->m_numCols;
		assert(info->m_key0 != NULL && info->m_key0->m_format == KEY_COMPRESS);
		IndexKey::swapKey(&(info->m_findKey), &(info->m_key0));
		if (info->m_isFastComparable)
			info->m_comparator->setComparator(RecordOper::compareKeyCC);
	}

	// 替换两个SubRecord，始终保持找到的结果存储在findKey
	IndexKey::swapKey(&(info->m_findKey), &(info->m_cKey1));

	return true;

FetchFail:
	session->unlockPage(&(info->m_pageHandle));
	return false;
}



/**
 * 根据扫描信息，取得指定项
 * @pre 比较函数和查找键值格式已经确定
 * @post 不持有任何页面资源
 *
 * @param info			扫描信息句柄
 * @param scanCallBack	扫描回调函数，如果存在，说明当前为删除操作
 * @param cbResult		out 回调函数的返回值
 * @return 是否取到指定项
 * @attention 由于不需要进行后续取项操作，该函数返回之后不持有任何页面资源
 */
bool DrsBPTreeIndex::fetchUnique(DrsIndexScanHandleInfo *info, IndexScanCallBackFN scanCallBack, bool *cbResult) {
	assert(scanCallBack == NULL || cbResult != NULL);

	Session *session = info->m_session;
	bool needFetchNext;
	s32 result;

FetchStart:
	if (!locateLeafPageAndFindKey(info, &needFetchNext, &result)) {
		if (scanCallBack != NULL) {
			cout << "TNT: Table:" << m_tableDef->m_name << " Index: "
				<< m_indexDef->m_name << " May be Broken, Need Reconstruct" << endl;
		}
		goto FetchFail;
	}

	if (result == 1 && info->m_everEqual && info->m_findKey->m_rowId == INVALID_ROW_ID) {	// 这时候可能查找的键值在下一个叶页面
		// 如果取项成功，比较新键值和查找键值的大小
		IDX_SWITCH_AND_GOTO(shiftToNextKey(info), FetchStart, FetchFail);
		if (info->m_hasNext)
			result = info->m_comparator->compareKey(info->m_findKey, info->m_cKey1);
	}

	if (result != 0) {
		if (scanCallBack != NULL) {
			cout << "TNT: Table:" << m_tableDef->m_name << " Index: "
				<< m_indexDef->m_name << " May be Broken, Need Reconstruct" << endl;
		}	
		goto FetchFail;
	}

	if (info->m_lockMode != None) {
		IDX_SWITCH_AND_GOTO(lockHoldingLatch(session, info->m_cKey1->m_rowId, info->m_lockMode, 
			&info->m_pageHandle, info->m_rowHandle, true, info), FetchStart, FetchFail);
		if (info->m_lockMode == Exclusived) {
			nftrace(ts.idx, tout << "Lock " << rid(info->m_cKey1->m_rowId) << " for dml";);
		}
	}

	// 替换两个SubRecord，始终保持找到的结果存储在findKey
	info->m_findKey->m_format = KEY_COMPRESS;
	IndexKey::swapKey(&(info->m_findKey), &(info->m_cKey1));

	if (scanCallBack != NULL) {
		*cbResult = (this->*scanCallBack)(info, true);
		return true;
	}

	session->releasePage(&(info->m_pageHandle));
	return true;

FetchFail:
	session->releasePage(&(info->m_pageHandle));
	return false;
}


/**
 * 如果下一个页面存在更新指定下一个页面的前项指针
 * @pre 使用者应该保证下一个页面可以直接加锁
 * @param session			会话句柄
 * @param file				索引文件句柄
 * @param logger			日志记录器
 * @param nextPageId		下一个页面ID
 * @param prevLinkPageId	要更新的前项指针
 */
void DrsBPTreeIndex::updateNextPagePrevLink(Session *session, File *file, IndexLog *logger, PageId nextPageId, PageId prevLinkPageId) {
	ftrace(ts.idx, tout << session << nextPageId << prevLinkPageId;);

	if (nextPageId == INVALID_ROW_ID)
		return;

	PageHandle *nextHandle = GET_PAGE(session, file, PAGE_INDEX, nextPageId, Exclusived, m_dboStats, NULL);
	((IndexPage*)(nextHandle->getPage()))->updatePrevPage(session, logger, nextPageId, prevLinkPageId);
	session->markDirty(nextHandle);
	session->releasePage(&nextHandle);
}


/**
 * 如果前一个页面存在更新指定前一个页面的后项指针
 * @pre 使用者应该保证下一个页面可以直接加锁
 * @param session			会话句柄
 * @param logger			日志记录器
 * @param file				索引文件句柄
 * @param nextPageId		前一个页面ID
 * @param prevLinkPageId	要更新的后项指针
 */
void DrsBPTreeIndex::updatePrevPageNextLink(Session *session, File *file, IndexLog *logger, PageId prevPageId, PageId nextLinkPageId) {
	ftrace(ts.idx, tout << session << prevPageId << nextLinkPageId;);

	if (prevPageId == INVALID_ROW_ID)
		return;

	PageHandle *prevHandle = GET_PAGE(session, file, PAGE_INDEX, prevPageId, Exclusived, m_dboStats, NULL);
	((IndexPage*)(prevHandle->getPage()))->updateNextPage(session, logger, prevPageId, nextLinkPageId);
	session->markDirty(prevHandle);
	session->releasePage(&prevHandle);
}



////////////////////////////////////////////////////////////////////////////////
// Delete


/**
 * 在删除过程中，由于定位页面修改对删除项做的重定位操作
 * @post 新定位的删除项信息都在info结构当中保存
 * @param info		扫描信息句柄
 * return 定位到的page信息
 */
IndexPage* DrsBPTreeIndex::researchForDelete(DrsIndexScanHandleInfo *info) {
	bool needFetchNext;
	s32 result;
	// 首先处理用何种键值查找以及设置比较函数
	NTSE_ASSERT(locateLeafPageAndFindKey(info, &needFetchNext, &result));	// 不应该有死锁
	assert(result == 0);
	IndexPage *page = (IndexPage*)info->m_pageHandle->getPage();
	assert(page->m_pageCount != 0);
	// 保证要删除的项在m_findKey当中
	saveFoundKeyToFindKey(info);

	assert(info->m_findKey->m_format == KEY_COMPRESS);

	return page;
}


/**
 * 根据扫描信息删除当前定位的项，当前页面Latch情况调用者需要传入表示，如果没有Latch，默认持有页面的Pin
 * 要删除的项保存在info->m_findKey当中，格式是KEY_COMPRESS或者KEY_REDUNDANT
 * 删除结束之后info->m_findKey保持KEY_COMPRESS格式
 *
 * 删除之前需要对页面进行加锁，保证不会被后续操作修改，这里需要注意的是删除操作本身加锁不应该出现
 * 加锁失败的情况，因为无论是delete还是update，在进行删除操作加锁之前，本session不会持有该索引上其他页面的页面锁
 * 因此删除操作对页面加锁最终应该总是能够成功
 *
 * @post 无论进入函数之前是否持有页面锁资源以及本删除操作是否成功，结束之后都会放锁放pin
 * @param info		扫描信息句柄
 * @param hasLatch	是否持有删除项所在页面Latch，如果没有Latch，则页面页一定已经pin住
 * @return true表示成功，false表示加锁过程出现死锁
 */
bool DrsBPTreeIndex::deleteIndexEntry(DrsIndexScanHandleInfo *info, bool hasLatch) {
	ftrace(ts.idx, tout << info->m_session << rid(info->m_findKey->m_rowId) << info->m_latchMode << hasLatch << info->m_lockMode << info->m_pageId;);
	assert(info->m_rowHandle == NULL || info->m_session->isRowLocked(m_tableDef->m_id, (*info->m_rowHandle)->getRid(), Exclusived));

	Session *session = info->m_session;
	assert(info->m_pageHandle != NULL);
	IndexPage *page = (IndexPage*)info->m_pageHandle->getPage();	// 因为持有pin，可以先获取

	// 处理有pin没有Latch的情况
	if (!hasLatch) {
		assert(info->m_pageHandle->isPinned());
		LOCK_PAGE_HANDLE(session, info->m_pageHandle, Exclusived);
		if (info->m_pageLSN != page->m_lsn) {	// 页面有改变，重定位要删除的项
			session->releasePage(&info->m_pageHandle);
			page = researchForDelete(info);
		}
	}

	// 首先需要对页面加锁
	PageId pageId = info->m_pageId;
	IDXResult result;
	u64 locateLSN = page->m_lsn;
	nftrace(ts.irl, tout << session->getId() << " del lock page " << pageId);
	while ((result = lockIdxObjectHoldingLatch(session, pageId, &info->m_pageHandle, FOR_DELETE, &DrsBPTreeIndex::judgerForDMLLocatePage, (void*)info, NULL)) == IDX_RESTART) {
		nftrace(ts.irl, tout << session->getId() << " need restart");
		// 加锁肯定不会失败，但是可能需要重定位
		page = researchForDelete(info);
		locateLSN = page->m_lsn;
		pageId = info->m_pageId;
	}
	assert(result == IDX_SUCCESS);
	page = (IndexPage*)info->m_pageHandle->getPage();
	pageId = info->m_pageId;
	// 加锁过程可能会造成页面被其他线程修改，这个时候要判断LSN，决定是不是要重新定位数据在页面的位置
	if (page->m_lsn != locateLSN) {
		// TODO: 这里并不是只要LSN改变就都还需要判断一次
		makeFindKeyPADIfNecessary(info);
		page->findKeyInPage(info->m_findKey, &SearchFlag::DEFAULT_FLAG, info->m_comparator, info->m_cKey1, &(info->m_keyInfo));
		assert(info->m_keyInfo.m_result == 0);
		saveFoundKeyToFindKey(info);
	}

	/**
	 * 需要预先加SMO锁
	 * 如果等到删除结束之后再加SMO锁发现死锁返回失败，这个时候可能会导致有访问操作访问到空页面
	 * 这里的解决方法是预先估算删除之后剩余空间大小，如果小于某个阀值，就预先加SMO锁，如果死锁不需要回退任何操作
	 */
	bool smo = false;
	u16 prevKeySOffset, nextKeyEOffset;	// 这两个变量分别表示删除键值前驱的起始偏移和后继偏移
	if (page->prejudgeIsNeedMerged(info->m_findKey, info->m_cKey1, info->m_cKey2, info->m_keyInfo, &prevKeySOffset, &nextKeyEOffset)) {
		smo = true;
		// 加上所有必要的锁资源，这里可能会多加
		if (!deleteSMOPrelockPages(session, &info->m_pageHandle))
			return false;
	}

	bool postSMOJudge = page->deleteIndexKey(session, ((DrsBPTreeIndice*)m_indice)->getLogger(), pageId, info->m_cKey1, info->m_findKey, info->m_cKey2, info->m_keyInfo, true, prevKeySOffset, nextKeyEOffset);
	//vecode(vs.idx, 
	//{
	//	MemoryContext *memoryContext = info->m_session->getMemoryContext();
	//	u64 savePoint = memoryContext->setSavepoint();
	//	SubRecord *key1 = IndexKey::allocSubRecord(memoryContext, m_indexDef, KEY_COMPRESS);
	//	SubRecord *key2 = IndexKey::allocSubRecord(memoryContext, m_indexDef, KEY_COMPRESS);
	//	SubRecord *pkey = IndexKey::allocSubRecord(memoryContext, m_indexDef, KEY_PAD);
	//	NTSE_ASSERT(page->traversalAndVerify(m_tableDef, key1, key2, pkey));
	//	memoryContext->resetToSavepoint(savePoint);
	//});
	session->markDirty(info->m_pageHandle);
	NTSE_ASSERT(postSMOJudge == smo);
	if (postSMOJudge)
		deleteSMO(info);

	//vecode(vs.idx, 
	//{
	//	MemoryContext *memoryContext = info->m_session->getMemoryContext();
	//	u64 savePoint = memoryContext->setSavepoint();
	//	SubRecord *key1 = IndexKey::allocSubRecord(memoryContext, m_indexDef, KEY_COMPRESS);
	//	SubRecord *key2 = IndexKey::allocSubRecord(memoryContext, m_indexDef, KEY_COMPRESS);
	//	SubRecord *pkey = IndexKey::allocSubRecord(memoryContext, m_indexDef, KEY_PAD);
	//	NTSE_ASSERT(((IndexPage*)(info->m_pageHandle->getPage()))->traversalAndVerify(m_tableDef, key1, key2, pkey));
	//	memoryContext->resetToSavepoint(savePoint);
	//});

	session->markDirty(info->m_pageHandle);

	//{
	//	session->releasePage(&info->m_pageHandle);
	//	MemoryContext *memoryContext = info->m_session->getMemoryContext();
	//	SubRecord *key1 = IndexKey::allocSubRecord(memoryContext, m_indexDef, KEY_COMPRESS);
	//	SubRecord *key2 = IndexKey::allocSubRecord(memoryContext, m_indexDef, KEY_COMPRESS);
	//	SubRecord *pkey = IndexKey::allocSubRecord(memoryContext, m_indexDef, KEY_PAD);
	//	NTSE_ASSERT(verify(info->m_session, key1, key2, pkey, false));
	//	info->m_pageHandle = GET_PAGE(session, ((DrsBPTreeIndice*)m_indice)->getFileDesc(), PAGE_INDEX, info->m_pageId, Exclusived, m_dboStats, NULL);
	//}

	if (hasLatch)
		session->releasePage(&info->m_pageHandle);
	else
		session->unlockPage(&info->m_pageHandle);

	return true;
}


/**
 * 对信息句柄当中的页面进行合并操作，寻找页面的左右结点进行合并，如果空间都不够少则停止合并，从叶页面开始往索引树高层进行
 *
 * 这个过程不会对索引页面加锁，具体原因可以参见insertSMO函数说明部分
 *
 * @pre	该操作必然已经被上层串行，整个过程索引不会被修改，因此不需要判断页面SMO位
 *		同时，保存在info->m_findKey内的键值是KEY_COMPRESS格式的
 *
 * @param info			扫描信息句柄
 * @attention 执行之前持有要合并页面的Latch，返回之后仍持有该Latch资源，即使该页面已经改变
 */
void DrsBPTreeIndex::deleteSMO(DrsIndexScanHandleInfo *info) {
	PROFILE(PI_DrsIndex_deleteSMO);

	PageHandle *sonHandle = info->m_pageHandle;
	IndexPage *sonPage = (IndexPage*)sonHandle->getPage();
	Session *session = info->m_session;
	File *file = ((DrsBPTreeIndice*)m_indice)->getFileDesc();
	PageId sonPageId = info->m_pageId;
	PageId mergedLeafPageId = sonPageId;
	IndexPage *mergedLeafPage = sonPage;
	uint level = 0;
	IndicePageManager *pagesManager = ((DrsBPTreeIndice*)m_indice)->getPagesManager();
	bool isFastComparable = info->m_isFastComparable;
	IndexLog *logger = ((DrsBPTreeIndice*)m_indice)->getLogger();

	u64 savePoint = session->getMemoryContext()->setSavepoint();

	SubRecord *padKey = IndexKey::allocSubRecord(session->getMemoryContext(), m_indexDef, KEY_PAD);
	SubRecord *backup = NULL;

	ftrace(ts.idx, tout << info << session);

	bool researched = false;
	for (;;) {
		if (sonPage->isPageRoot()) {	// 根结点无法再合并
			if (sonPage->isPageEmpty()) {
				nftrace(ts.idx, tout << "Root became empty";);
				if (sonPage->m_pageLevel != 0) {
					// 说明索引被删空，层数需要修改，根页面类型也要修改
					u8 newLevel = 0;
					sonPage->updateInfo(session, logger, sonPageId, OFFSET(IndexPage, m_pageLevel), sizeof(u8), (byte*)&newLevel);
					u8 type = (u8)ROOT_AND_LEAF;
					sonPage->updateInfo(session, logger, sonPageId, OFFSET(IndexPage, m_pageType), sizeof(u8), (byte*)&type);
					session->markDirty(sonHandle);
				}
			}
			session->releasePage(&sonHandle);
			goto SMOSuccess;
		}
		// 1.定位父结点，首先释放子结点的Latch
		u16 sonUsedSpace = sonPage->getUsedSpace();
		bool pageEmpty = sonPage->isPageEmpty();
		sonPage->setSMOBit(session, sonPageId, logger);
		session->markDirty(sonHandle);
		session->releasePage(&sonHandle);

		// 设置比较键值压缩格式以及比较函数
		if (!isFastComparable) {
			backup = info->m_findKey;
			RecordOper::convertKeyCP(m_tableDef, m_indexDef, info->m_findKey, padKey);
			info->m_findKey = padKey;

			info->m_comparator->setComparator(RecordOper::compareKeyPC);
		} else {
			info->m_comparator->setComparator(RecordOper::compareKeyCC);
		}

		PageId parentPageId;
		PageHandle *parentHandle;
		researched = searchParentInSMO(info, level, researched, sonPageId, &parentPageId, &parentHandle);
		IndexPage *parentPage = (IndexPage*)parentHandle->getPage();

		// 2.确定可以合并的左右兄弟结点
		KeyInfo parentKeyInfo, siblingKeyInfo;
		IDXResult latchResult;
		SubRecord *sonParentKey = info->m_cKey1;
		parentPage->findKeyInPage(info->m_findKey, &SearchFlag::DEFAULT_FLAG, info->m_comparator, sonParentKey, &parentKeyInfo);
		NTSE_ASSERT(parentKeyInfo.m_result <= 0 || parentKeyInfo.m_miniPageNo == parentPage->m_miniPageNum - 1);	// 定位的项就是当前页面的父页面项
		NTSE_ASSERT(PID_READ((byte*)parentPage + parentKeyInfo.m_eOffset - PID_BYTES) == sonPageId);

		// 还原findKey信息
		if (!isFastComparable)
			info->m_findKey = backup;

		PageHandle *leftHandle = NULL, *rightHandle = NULL;
		IndexPage *leftPage = NULL, *rightPage = NULL;
		PageId rightPageId = INVALID_PAGE_ID, leftPageId = INVALID_PAGE_ID;

		u16 miniPageNoNext = parentKeyInfo.m_miniPageNo;
		u16 miniPageNoPrev = parentKeyInfo.m_miniPageNo;
		u16 offset;
		offset = parentPage->getNextKey(sonParentKey, parentKeyInfo.m_eOffset, true, &miniPageNoNext, info->m_cKey2);
		if (IndexKey::isKeyValid(info->m_cKey2)) {		// 存在右项
			rightPageId = IndexKey::getPageId(info->m_cKey2);
			latchResult = latchHoldingLatch(session, rightPageId, parentPageId, &parentHandle, &rightHandle, Exclusived);
			assert(latchResult != IDX_RESTART);

			rightPage = (IndexPage*)rightHandle->getPage();
			siblingKeyInfo.m_sOffset = parentKeyInfo.m_eOffset;
			siblingKeyInfo.m_eOffset = offset;
			siblingKeyInfo.m_miniPageNo = miniPageNoNext;
			if (rightPage->canPageBeMerged(sonUsedSpace)) {	// 确定可以取右页面进行合并
				rightPage->setSMOBit(session, rightPageId, logger);
				leftPageId = sonPageId;
			} else {
				session->releasePage(&rightHandle);
				rightPage = NULL;
			}
		}

		if (rightHandle == NULL) {
			offset = parentPage->getPrevKey(parentKeyInfo.m_sOffset, true, &miniPageNoPrev, info->m_cKey2);
			if (IndexKey::isKeyValid(info->m_cKey2)) {
				leftPageId = IndexKey::getPageId(info->m_cKey2);

				latchResult = latchHoldingLatch(session, leftPageId, parentPageId, &parentHandle, &leftHandle, Exclusived);
				assert(latchResult != IDX_RESTART);

				assert(leftHandle != NULL);
				leftPage = (IndexPage*)leftHandle->getPage();
				siblingKeyInfo.m_sOffset = offset;
				siblingKeyInfo.m_eOffset = parentKeyInfo.m_sOffset;
				siblingKeyInfo.m_miniPageNo = miniPageNoPrev;
				if (leftPage->canPageBeMerged(sonUsedSpace)) {	// 确定可以取左页面进行合并
					// 这里不需要加页面锁，因为会合并到删除键值的页面，已经有了锁
					leftPage->setSMOBit(session, leftPageId, logger);
					rightPageId = sonPageId;
				} else {
					session->releasePage(&leftHandle);
					leftPage = NULL;
				}
			}
		}

		if (leftHandle == NULL && rightHandle == NULL) {	// 该页面无法进行合并
			nftrace(ts.idx, tout << "No page to merge, level: " << level;);
			// 清原始页面SMO位
			latchResult = latchHoldingLatch(session, sonPageId, parentPageId, &parentHandle, &sonHandle, Exclusived);
			assert(latchResult != IDX_RESTART);
			sonPage = (IndexPage*)sonHandle->getPage();
			//PageId prevSonPageId = sonPage->m_prevPage;
			if (pageEmpty) {
				session->unlockPage(&sonHandle);	// 避免死锁，先放锁
				PageId sonLeftPageId = sonPage->m_prevPage, sonRightPageId = sonPage->m_nextPage;
				updatePrevPageNextLink(session, file, logger, sonLeftPageId, sonRightPageId);
				updateNextPagePrevLink(session, file, logger, sonRightPageId, sonLeftPageId);
				nftrace(ts.idx, tout << "Sibling pages change linker: " << sonLeftPageId << " " << sonRightPageId);
				LOCK_PAGE_HANDLE(session, sonHandle, Exclusived);
			}
			sonPage->clearSMOBit(session, sonPageId, logger);
			if (pageEmpty) {
				if (sonPage->isPageLeaf())
					NTSE_ASSERT(idxUnlockObject(session, sonPageId));
				pagesManager->freePage(logger, session, sonPageId, sonPage);
				nftrace(ts.idx, tout << "PageId: " << sonPageId << " is freed");
			}
			session->markDirty(sonHandle);
			session->releasePage(&sonHandle);

			if (pageEmpty) {	// 当前页面为空，且没有左右页面可以进行合并，删除父页面对应项
				// 即使要删除的项是页面的最后一项，这一项也不需要被特意保留，而去删除前一项
				NTSE_ASSERT(parentPage->m_nextPage == INVALID_PAGE_ID || parentKeyInfo.m_eOffset < parentPage->getDataEndOffset() || parentPage->m_pageCount == 1);
				parentPage->deleteIndexKey(session, logger, parentPageId, info->m_cKey2, sonParentKey, info->m_cKey3, parentKeyInfo);
				session->markDirty(parentHandle);

				if (parentPage->isPageEmpty()) {	// 判断父页面是否要合并
					sonHandle = parentHandle;
					sonPage = parentPage;
					sonPageId = parentPageId;
					level++;
					continue;
				}
			}

			session->releasePage(&parentHandle);
			goto SMOSuccess;
		}

		session->releasePage(&parentHandle);
		assert(rightPageId != INVALID_PAGE_ID && leftPageId != INVALID_PAGE_ID);
		if (leftHandle == NULL) {
			latchResult = latchHoldingLatch(session, sonPageId, rightPageId, &rightHandle, &leftHandle, Exclusived);
			assert(latchResult != IDX_RESTART);
			leftPage = (IndexPage*)leftHandle->getPage();
		} else {
			latchResult = latchHoldingLatch(session, sonPageId, leftPageId, &leftHandle, &rightHandle, Exclusived);
			assert(latchResult != IDX_RESTART);
			rightPage = (IndexPage*)rightHandle->getPage();
		}

		// 3.将左页面合并到右页面，修改左右指针关系
		rightPage->mergePage(session, logger, leftPage, rightPageId, leftPageId);
		nftrace(ts.idx, tout << session << " Merge page: " << leftPageId << " and " << rightPageId);
		session->markDirty(leftHandle);
		session->markDirty(rightHandle);
		// 增加统计计数
		++m_indexStatus.m_numMerge;

		if (rightPage->isPageLeaf())	// 需要记录叶页面合并的页面最后加Latch
			mergedLeafPageId = rightPageId;
		session->releasePage(&rightHandle);
		rightPage = NULL;

		PageId leftLeftId = leftPage->m_prevPage;
		session->markDirty(leftHandle);
		if (leftLeftId != INVALID_PAGE_ID) {	// 修改合并中左页面的左页面的右指针
			session->unlockPage(&leftHandle);
			updatePrevPageNextLink(session, file, logger, leftLeftId, rightPageId);
			nftrace(ts.idx, tout << "Update prev page's next link: " << leftLeftId << " " << rightPageId);
			LOCK_PAGE_HANDLE(session, leftHandle, Exclusived);
		}

		// 4.删除父页面对应的项
		latchResult = latchHoldingLatch(session, parentPageId, leftPageId, &leftHandle, &parentHandle, Exclusived);
		parentPage = (IndexPage*)parentHandle->getPage();
		assert(latchResult != IDX_RESTART);
		if (sonPageId == leftPageId)	// 和右页面合并
			parentPage->deleteIndexKey(session, logger, parentPageId, info->m_cKey2, sonParentKey, info->m_cKey3, parentKeyInfo);
		else	// 和左页面合并
			parentPage->deleteIndexKey(session, logger, parentPageId, sonParentKey, info->m_cKey2, info->m_cKey3, siblingKeyInfo);
		session->markDirty(parentHandle);

		// 5.清除合并右页面SMO位，释放左页面，释放的页面不清零
		// 如果回收页面是叶页面并且该页面加过页面锁，需要首先释放页面锁
		if (leftPage->isPageLeaf()) {
			if (leftPageId == sonPageId)
				idxUnlockObject(session, leftPageId);
		}
		pagesManager->freePage(logger, session, leftPageId, leftPage);
		session->markDirty(leftHandle);
		session->releasePage(&leftHandle);
		leftPage = NULL;

		latchResult = latchHoldingLatch(session, rightPageId, parentPageId, &parentHandle, &rightHandle, Exclusived);
		assert(latchResult != IDX_RESTART);
		rightPage = (IndexPage*)rightHandle->getPage();
		rightPage->clearSMOBit(session, rightPageId, logger);
		session->markDirty(rightHandle);
		session->releasePage(&rightHandle);

		// 6.按照现在的逻辑，运行到这里，父节点不可能为空
		NTSE_ASSERT(!parentPage->isPageEmpty());
		//// 6.判断父页面是否需要合并
		//if (parentPage->isPageEmpty()) {
		//	sonHandle = parentHandle;
		//	sonPage = parentPage;
		//	sonPageId = parentPageId;
		//	level++;
		//	continue;
		//}

		session->releasePage(&parentHandle);
		break;
	}

SMOSuccess:
	// 要加回删除键值所在的页面的Latch
	info->m_pageId = mergedLeafPageId;
	info->m_pageHandle = GET_PAGE(session, file, PAGE_INDEX, mergedLeafPageId, Exclusived, m_dboStats, mergedLeafPage);

	session->getMemoryContext()->resetToSavepoint(savePoint);
}



//////////////////////////////////////////////////////////////////////////////////////


/**
 * 在SMO过程中，定位当前层的父节点，可能需要重新搜索路径
 * @param info			扫描信息句柄
 * @param level			父节点所在的层数
 * @param researched	在本次SMO过程中，是否进行过重定位
 * @param sonPageId		当前子页面的ID
 * @param parentPageId	out 返回父页面的pageId
 * @param parentHandle	out 返回父页面的handle
 * @return 本次定位过程是否出现了重定位
 */
bool DrsBPTreeIndex::searchParentInSMO(DrsIndexScanHandleInfo *info, uint level, bool researched, PageId sonPageId, PageId *parentPageId, PageHandle **parentHandle) {
	Session *session = info->m_session;
	File *file = ((DrsBPTreeIndice*)m_indice)->getFileDesc();

	*parentPageId = info->m_traceInfo[level - info->m_lastSearchPathLevel].m_pageId;
	*parentHandle = GET_PAGE(session, file, PAGE_INDEX, *parentPageId, Exclusived, m_dboStats, NULL);
	IndexPage *parentPage = (IndexPage*)(*parentHandle)->getPage();
	if (!researched && !parentPage->findSpecifiedPageId(sonPageId)) {
		// LSN不等，肯定说明变化，如果相等，还需要确认能不能找到
		// 父页面可能有变动，需要重新定位，由于SMO串行，该情况只可能出现一次
		// 父页面可能已经被其他线程删光回收甚至重利用
		nftrace(ts.idx, tout << session->getId() << " Key path changed during smo";);
		session->releasePage(parentHandle);

		if (info->m_findKey->m_format == KEY_PAD)
			info->m_comparator->setComparator(RecordOper::compareKeyPC);

		PageHandle *leafPageHandle = findSpecifiedLevel(info, &SearchFlag::DEFAULT_FLAG, level, IDX_SEARCH);
		assert(leafPageHandle != NULL);	// 这里不可能出现SMO检查导致的死锁，因此页面肯定可以找得到
		session->releasePage(&leafPageHandle);

		info->m_comparator->setComparator(info->m_isFastComparable ? RecordOper::compareKeyCC : RecordOper::compareKeyPC);

		*parentPageId = info->m_traceInfo[0].m_pageId;
		*parentHandle = GET_PAGE(session, file, PAGE_INDEX, *parentPageId, Exclusived, m_dboStats, NULL);

		return true;
	}

	return false;
}


/**
 * 进行插入SMO之前需要预先加索引SMO锁，和可能修改的叶页面锁
 * @pre 插入页面锁已经加成功
 * @param session		会话句柄
 * @param insertHandle	in/out 插入的页面的句柄
 * @post 加锁都成功不会释放任何资源，加锁失败会释放页面Latch资源
 * @return 加锁都成功返回true，有死锁返回false
 */
bool DrsBPTreeIndex::insertSMOPrelockPages(Session *session, PageHandle **insertHandle) {
	SYNCHERE(SP_IDX_TO_LOCK_SMO);

	IndexPage *page = (IndexPage*)(*insertHandle)->getPage();
	// 加索引SMO锁，执行SMO，然后重新插入
	nftrace(ts.irl, tout << session->getId() << " ins need smo lock " << m_indexId);
	IDXResult result = lockIdxObjectHoldingLatch(session, m_indexId, insertHandle);
	assert(result != IDX_RESTART);
	if (result == IDX_FAIL)
		return false;

	// 因为SMO要修改叶页面连接，预加插入页面的下一个页面的锁
	PageId nextPageId = page->m_nextPage;
	if (nextPageId != INVALID_PAGE_ID) {
		nftrace(ts.irl, tout << session->getId() << " ins need smo lock " << nextPageId);
		IDXResult result = lockIdxObjectHoldingLatch(session, nextPageId, insertHandle);
		assert(result != IDX_RESTART);
		if (result == IDX_FAIL)
			return false;
	}

	return true;
}

/**
 * 进行删除SMO之前需要预先加索引SMO锁，和可能修改的叶页面锁
 * @pre 删除页面锁已经加成功
 * @post 加锁都成功不会释放任何资源，加锁失败会释放页面Latch资源
 * @param session		会话句柄
 * @param insertHandle	in/out 删除的页面的句柄
 * @return 加锁都成功返回true，有死锁返回false
 */
bool DrsBPTreeIndex::deleteSMOPrelockPages(Session *session, PageHandle **deleteHandle) {
	SYNCHERE(SP_IDX_TO_LOCK_SMO);

	IndexPage *page = (IndexPage*)(*deleteHandle)->getPage();
	IDXResult result = lockIdxObjectHoldingLatch(session, m_indexId, deleteHandle);
	nftrace(ts.irl, tout << session->getId() << " del need smo and lock " << m_indexId);
	if (result == IDX_FAIL)	{// 死锁
		nftrace(ts.irl, tout << session->getId() << " smo lock dl");
		return false;
	}
	assert(result == IDX_SUCCESS);	// 加了页面锁没人会修改数据

	// 再继续加左右两个页面的页面锁
	PageId prevPageId = page->m_prevPage, nextPageId = page->m_nextPage;
	if (prevPageId != INVALID_PAGE_ID) {
		result = lockIdxObjectHoldingLatch(session, prevPageId, deleteHandle);
		nftrace(ts.irl, tout << session->getId() << " del need smo and lock " << prevPageId);
		if (result == IDX_FAIL)	{// 死锁
			nftrace(ts.irl, tout << session->getId() << " smo lock dl");
			return false;
		}
		assert(result == IDX_SUCCESS);	// 加了页面锁没人会修改数据

		// 继续锁前一个页面
		session->unlockPage(deleteHandle);
		PageHandle *ppHandle = GET_PAGE(session, ((DrsBPTreeIndice*)m_indice)->getFileDesc(), PAGE_INDEX, prevPageId, Shared, m_dboStats, NULL);
		PageId prevPrevPageId = ((IndexPage*)ppHandle->getPage())->m_prevPage;
		session->releasePage(&ppHandle);
		LOCK_PAGE_HANDLE(session, *deleteHandle, Exclusived);
		if (prevPrevPageId != INVALID_PAGE_ID) {
			result = lockIdxObjectHoldingLatch(session, prevPrevPageId, deleteHandle);
			nftrace(ts.irl, tout << session->getId() << " del need smo and lock " << prevPrevPageId);
			if (result == IDX_FAIL)	{// 死锁
				nftrace(ts.irl, tout << session->getId() << " smo lock dl");
				return false;
			}
			assert(result == IDX_SUCCESS);	// 加了页面锁没人会修改数据
		}
	}
	if (nextPageId != INVALID_PAGE_ID) {
		result = lockIdxObjectHoldingLatch(session, nextPageId, deleteHandle);
		nftrace(ts.irl, tout << session->getId() << " del need smo and lock " << nextPageId);
		if (result == IDX_FAIL)	{// 死锁
			nftrace(ts.irl, tout << session->getId() << " smo lock dl");
			return false;
		}
		assert(result == IDX_SUCCESS);	// 加了页面锁没人会修改数据
	}

	return true;
}

/**
 * 插入指定索引键值
 * @pre 使用者必须设置好插入键值格式REC_REDUNDANT以及比较函数compareRC
 *
 * 在执行插入之前，需要对页面进行加锁，此时的操作如果是单纯insert出发的，那么肯定不会有死锁，
 * 如果本次插入是update出发的，那么可能出现死锁
 *
 * @param info				插入信息句柄
 * @param duplicateKey		out true表示插入会导致唯一索引有相同键值，false表示不会
 * @param checkDuplicate	指定是否需要检测唯一性，如果是恢复操作不需要检查唯一性，默认为true，只有恢复回退删除操作才能设false
 * @return true表示插入成功并且不违反唯一性约束，false表示违反唯一性约束或者有死锁导致
 */
bool DrsBPTreeIndex::insertIndexEntry(DrsIndexScanHandleInfo *info, bool *duplicateKey, bool checkDuplicate) {
	ftrace(ts.idx, tout << info << checkDuplicate);

	IndexLog *logger = ((DrsBPTreeIndice*)m_indice)->getLogger();
	Session *session = info->m_session;
	IndexPage *page;
	*duplicateKey = false;
	u64 token, beforeAddLSN;

InsertStart:
	// 定位要插入的页面并对页面加锁
	if ((info->m_pageHandle = findSpecifiedLevel(info, &SearchFlag::DEFAULT_FLAG, 0, IDX_SEARCH)) == NULL)
		goto InsertFail;	// 这里表示加SMO锁遇到死锁

	SYNCHERE(SP_IDX_RESEARCH_PARENT_IN_SMO);

	nftrace(ts.irl, tout << session->getId() << " ins need lock page " << info->m_pageId);
	token = session->getToken();
	IDX_SWITCH_AND_GOTO(lockIdxObjectHoldingLatch(session, info->m_pageId, &(info->m_pageHandle), FOR_INSERT, &DrsBPTreeIndex::judgerForDMLLocatePage, (void*)info, NULL), InsertStart, InsertFail);
	nftrace(ts.irl, tout << session->getId() << " ins lock " << info->m_pageId << " succ");
	page = (IndexPage*)info->m_pageHandle->getPage();

	// 查找插入键值在页面中的位置
	info->m_keyInfo.m_sOffset = 0;
	info->m_cKey1->m_size = info->m_cKey2->m_size = 0;
	page->findKeyInPageTwo(info->m_findKey, info->m_comparator, &(info->m_cKey1), &(info->m_cKey2), &(info->m_keyInfo));

	// 检查唯一性约束如果发现有冲突，直接返回
	// 注:只有NTSE才需要在这里进行唯一性检查，TNT不需要
	if (NULL == session->getTrans() && m_indexDef->m_unique && checkDuplicate) {
		IDXResult result = insertCheckSame(info, duplicateKey);
		if (result == IDX_RESTART) {	// 重启的话，需要释放已经持有的页面锁
			idxUnlockObject(session, info->m_pageId, token);
			goto InsertStart;
		} else if (result == IDX_FAIL)
			goto InsertFail;
		else {
			if (*duplicateKey) {
				session->releasePage(&info->m_pageHandle);
				return false;
			}
		}
	}

	SYNCHERE(SP_IDX_WAIT_FOR_INSERT);

	// 插入操作有可能修改了页面(对于MiniPage已满，分裂了MiniPage成功，但是后续操作发现空间不够)
	// 这个情况需要标记当前页面为脏
	beforeAddLSN = page->m_lsn;
	if (page->addIndexKey(session, logger, info->m_pageId, info->m_findKey, info->m_key0, info->m_cKey1, info->m_cKey2, info->m_comparator, &(info->m_keyInfo)) == NEED_SPLIT) {
		if (beforeAddLSN != page->m_lsn)
			session->markDirty(info->m_pageHandle);

		if (!checkDuplicate) {
			NTSE_ASSERT(beforeAddLSN == page->m_lsn);
			// 不需要检查唯一性表明是回退操作
			// 页面改变是由于MP分裂，合并之前分裂的MP，保证页面状态一致
			NTSE_ASSERT(page->m_splitedMPNo != (u16)-1);
			NTSE_ASSERT(page->mergeTwoMiniPage(session, logger, info->m_pageId, page->m_splitedMPNo, info->m_cKey1, info->m_cKey2));
		} else {
			// 这个时候需要SMO，预先加锁
			if (!insertSMOPrelockPages(session, &info->m_pageHandle))
				goto InsertFail;
			insertSMO(info);
			page = (IndexPage*)info->m_pageHandle->getPage();
		}

		info->m_keyInfo.m_sOffset = 0;
		NTSE_ASSERT(page->addIndexKey(session, logger, info->m_pageId, info->m_findKey, info->m_key0, info->m_cKey1, info->m_cKey2, info->m_comparator, &(info->m_keyInfo)) == INSERT_SUCCESS);
	}

	session->markDirty(info->m_pageHandle);
	session->releasePage(&info->m_pageHandle);

	return true;

InsertFail:
	nftrace(ts.irl, tout << session->getId() << " dl goto fail");
	return false;
}



/**
 * 将索引树分裂使得键值可以插入，这里操作基于关键假设是对索引树的修改是串行的
 * 整个过程不会更改比较器指针
 *
 * 操作过程当中，会对叶页面这一层新生成的页面加锁，保证后续不会随意被修改
 * 但是不需要对相邻兄弟节点加锁，虽然它们被修改了链接指针，因为：
 * 修改链接指针的操作，只有在SMO操作才可能发生，而SMO操作会被索引的SMO修改锁串行化
 * 因此在本事务修改提交前，不可能有别的事务来修改页面的链接指针，只可能修改页面的数据
 * 而数据的修改，不会影响链接指针修改的undo操作
 *
 * @pre	上层负责对memoryContext进行保存点控制，这里只负责分配
 * @param info			插入信息句柄
 * @attention 进入之前info内的pageHandle加了X锁，返回之后pageHandle仍应表示要插入的页面，且还是持有X锁
 */
void DrsBPTreeIndex::insertSMO(DrsIndexScanHandleInfo *info) {
	PROFILE(PI_DrsIndex_insertSMO);

	Session *session = info->m_session;
	File *file = ((DrsBPTreeIndice*)m_indice)->getFileDesc();
	PageHandle *sonHandle = info->m_pageHandle;
	IndexPage *sonPage = (IndexPage*)sonHandle->getPage();
	PageId sonPageId = info->m_pageId;
	uint level = 0;
	PageId smoPageIds[MAX_INDEX_LEVEL * 3];
	PageHandle *smoPageHandle[MAX_INDEX_LEVEL * 3];
	MemoryContext *memoryContext = session->getMemoryContext();
	u16 smoPageNum = 0;
	u8 indexId = m_indexId;
	bool isFastComparable = info->m_isFastComparable;

	IndicePageManager *pagesManager = ((DrsBPTreeIndice*)m_indice)->getPagesManager();	// 由于上层已经有并发，这里可以直接获取应用
	IndexLog *logger = ((DrsBPTreeIndice*)m_indice)->getLogger();

	PageId newPageId = INVALID_PAGE_ID;
	PageHandle *newHandle = NULL;
	IndexPage *newPage = NULL;

	KeyInfo keyInfo;
	keyInfo.m_sOffset = info->m_keyInfo.m_sOffset;
	bool rootSplited = false;
	bool researched = false;
	PageId realSonPageId = INVALID_PAGE_ID;

	// 以下几个变量重新分配的原因是，如果使用info当中的临时键值，可能会导致误用，详见QA49342
	SubRecord *maxSonPageKey1 = IndexKey::allocSubRecord(memoryContext, m_indexDef, KEY_COMPRESS);
	SubRecord *maxSonPageKey2 = IndexKey::allocSubRecord(memoryContext, m_indexDef, KEY_COMPRESS);
	SubRecord *parentKey = IndexKey::allocSubRecord(memoryContext, m_indexDef, KEY_COMPRESS);
	SubRecord *splitKey = IndexKey::allocSubRecord(memoryContext, m_indexDef, KEY_COMPRESS);

	// 对应maxRecord的非压缩格式，不需要则不使用
	SubRecord *padKey1 = isFastComparable ? NULL : IndexKey::allocSubRecord(memoryContext, m_indexDef, KEY_PAD);
	SubRecord *padKey2 = isFastComparable ? NULL : IndexKey::allocSubRecord(memoryContext, m_indexDef, KEY_PAD);

	SubRecord *cKey1 = IndexKey::allocSubRecord(memoryContext, m_indexDef, KEY_COMPRESS);
	SubRecord *cKey2 = IndexKey::allocSubRecord(memoryContext, m_indexDef, KEY_COMPRESS);

	KeyComparator *comparator = info->m_comparator;
	comparator->setComparator(isFastComparable ? RecordOper::compareKeyCC : RecordOper::compareKeyPC);

	ftrace(ts.idx, tout << info << session);

	assert(sonPage->isPageLeaf());

	for (;;) {
		IndexInsertResult insertResult;
		u16 insertedCount = 0;
		u16 insertPos = keyInfo.m_sOffset;
		u16 reservedSize = (u16)(info->m_findKey->m_format == KEY_COMPRESS ? info->m_findKey->m_size : info->m_key0->m_size);

		// 1.非叶结点，尝试插入两个子页面的最大项
		if (!sonPage->isPageLeaf()) {
			assert(IndexKey::isKeyValid(maxSonPageKey1) && IndexKey::isKeyValid(maxSonPageKey2));
			// 首先删除原来的项，肯定能成功
			if (!rootSplited)
				sonPage->deleteIndexKey(session, logger, sonPageId, cKey1, parentKey, cKey2, keyInfo);

			// 先插入大的，保证两个项会插入在同一个页面，后面处理简单
			keyInfo.m_sOffset = 0;
			if (isFastComparable)
				insertResult = sonPage->addIndexKey(session, logger, sonPageId, maxSonPageKey2, NULL, cKey1, cKey2, info->m_comparator, &keyInfo);
			else
				insertResult = sonPage->addIndexKey(session, logger, sonPageId, padKey2, maxSonPageKey2, cKey1, cKey2, info->m_comparator, &keyInfo);
			session->markDirty(sonHandle);

			if (insertResult != NEED_SPLIT) {
				insertedCount++;
				keyInfo.m_sOffset = 0;
				if (isFastComparable)
					insertResult = sonPage->addIndexKey(session, logger, sonPageId, maxSonPageKey1, NULL, cKey1, cKey2, info->m_comparator, &keyInfo);
				else
					insertResult = sonPage->addIndexKey(session, logger, sonPageId, padKey1, maxSonPageKey1, cKey1, cKey2, info->m_comparator, &keyInfo);
				session->markDirty(sonHandle);

				if (insertResult != NEED_SPLIT) {
					sonPage->setSMOBit(session, sonPageId, logger);
					smoPageHandle[smoPageNum] = sonHandle;
					smoPageIds[smoPageNum++] = sonPageId;
					session->unlockPage(&sonHandle);
					break;	// 成功SMO结束
				} else {
					reservedSize = (u16)(maxSonPageKey1->m_size);
				}
			} else {
				reservedSize = (u16)(maxSonPageKey1->m_size + maxSonPageKey2->m_size);
			}
		}

		// 2.分裂页面，不同页面采取不同的分裂
		if (sonPage->isPageRoot()) {
			// 分配两个新页面，根页面始终保持不变
			PageId leftPageId, rightPageId;
			PageHandle *leftHandle, *rightHandle;
			IndexPage *leftPage, *rightPage;

			leftHandle = pagesManager->allocPage(logger, session, indexId, &leftPageId);
			rightHandle = pagesManager->allocPage(logger, session, indexId, &rightPageId);
			leftPage = (IndexPage*)leftHandle->getPage();
			rightPage = (IndexPage*)rightHandle->getPage();

			leftPage->updateInfo(session, logger, leftPageId, 0, INDEX_PAGE_SIZE, (byte*)sonPage);

			sonPage->initPage(session, logger, true, true, sonPageId, sonPage->m_pageMark, ROOT_PAGE, sonPage->m_pageLevel + 1);
			sonPage->setSMOBit(session, sonPageId, logger);
			sonPage->setSMOLSN();
			session->markDirty(sonHandle);
			session->releasePage(&sonHandle);

			u8 newType = (u8)(leftPage->m_pageLevel == 0 ? LEAF_PAGE : NON_LEAF_PAGE);
			leftPage->updateInfo(session, logger, leftPageId, OFFSET(IndexPage, m_pageType), 1, (byte*)&newType);
			leftPage->splitPage(session, logger, rightPage, leftPageId, rightPageId, m_indexDef->m_splitFactor == IndexDef::SMART_SPLIT_FACTOR ? 0 : m_indexDef->m_splitFactor, insertPos, reservedSize, splitKey);
			leftPage->setSMOBit(session, leftPageId, logger);
			rightPage->setSMOBit(session, rightPageId, logger);

			nftrace(ts.idx, tout << session << " Root has split, from " << sonPageId << " to " << leftPageId << " and " << rightPageId << " root page smolsn:" << sonPage->m_smoLSN;);
			// 如果是叶页面，需要对新页面加页面锁，防止操作结束被修改
			if (leftPage->isPageLeaf()) {
				nftrace(ts.irl, tout << session->getId() << " ins smo lock newpage " << leftPageId << " smolsn: " << leftPage->m_smoLSN);
				NTSE_ASSERT(lockIdxObjectHoldingLatch(session, leftPageId, &leftHandle) == IDX_SUCCESS);
				nftrace(ts.irl, tout << session->getId() << " ins smo lock newpage " << rightPageId << " smolsn: " << rightPage->m_smoLSN);
				NTSE_ASSERT(lockIdxObjectHoldingLatch(session, rightPageId, &rightHandle) == IDX_SUCCESS);
			}

			session->markDirty(leftHandle);
			session->markDirty(rightHandle);

			info->m_traceInfo[level].m_pageId = sonPageId;
			info->m_traceInfo[level].pi.si.m_pageLSN = sonPage->m_lsn;
			info->m_traceInfo[level].pi.si.m_miniPageNo = 0;

			sonHandle = leftHandle;
			sonPage = (IndexPage*)sonHandle->getPage();
			sonPageId = leftPageId;
			newHandle = rightHandle;
			newPage = (IndexPage*)newHandle->getPage();
			newPageId = rightPageId;

			rootSplited = true;
			researched = true;
		} else {
			newHandle = pagesManager->allocPage(logger, session, indexId, &newPageId);
			newPage = (IndexPage*)newHandle->getPage();

			// 如果是叶页面，需要对新页面加页面锁，防止操作结束被修改
			if (sonPage->isPageLeaf()) {
				nftrace(ts.irl, tout << session->getId() << " ins smo lock newpage " << newPageId);
				NTSE_ASSERT(lockIdxObjectHoldingLatch(session, newPageId, &newHandle) == IDX_SUCCESS);
			}

			sonPage->splitPage(session, logger, newPage, sonPageId, newPageId, m_indexDef->m_splitFactor == IndexDef::SMART_SPLIT_FACTOR ? 0 : m_indexDef->m_splitFactor, insertPos, reservedSize, splitKey);
			newPage->setSMOBit(session, newPageId, logger);
			sonPage->setSMOBit(session, sonPageId, logger);
			session->markDirty(sonHandle);
			session->markDirty(newHandle);
		}

		++m_indexStatus.m_numSplit;

		// 3.非叶结点，再次尝试插入和更新，必定成功
		if (!sonPage->isPageLeaf()) {
			KeyInfo insertKeyInfo;
			IndexPage *insertPage;
			PageId insertPageId;
			if (insertPos < sonPage->getDataEndOffset()) {
				insertPageId = sonPageId;
				insertPage = sonPage;
			} else {
				insertPageId = newPageId;
				insertPage = newPage;
			}

			if (insertedCount == 0) {
				insertKeyInfo.m_sOffset = 0;
				if (isFastComparable)
					NTSE_ASSERT(insertPage->addIndexKey(session, logger, insertPageId, maxSonPageKey2, NULL, cKey1, cKey2, info->m_comparator, &insertKeyInfo) == INSERT_SUCCESS);
				else
					NTSE_ASSERT(insertPage->addIndexKey(session, logger, insertPageId, padKey2, maxSonPageKey2, cKey1, cKey2, info->m_comparator, &insertKeyInfo) == INSERT_SUCCESS);
				session->markDirty(sonHandle);
			}
			insertKeyInfo.m_sOffset = 0;
			if (isFastComparable)
				NTSE_ASSERT(insertPage->addIndexKey(session, logger, insertPageId, maxSonPageKey1, NULL, cKey1, cKey2, info->m_comparator, &insertKeyInfo) == INSERT_SUCCESS);
			else
				NTSE_ASSERT(insertPage->addIndexKey(session, logger, insertPageId, padKey1, maxSonPageKey1, cKey1, cKey2, info->m_comparator, &insertKeyInfo) == INSERT_SUCCESS);
			session->markDirty(sonHandle);
		} else {
			// 叶结点需要记录插入项对应的页面
			if (info->m_keyInfo.m_sOffset < sonPage->getDataEndOffset())	//! 必须是小于，否则结构出错
				realSonPageId = sonPageId;
			else
				realSonPageId = newPageId;
		}

		// 4.生成当前层两个页面的最大键值准备插入父结点
		{
			KeyInfo kInfo;
			newPage->getLastKey(maxSonPageKey2, &kInfo);
			sonPage->getLastKey(maxSonPageKey1, &kInfo);
			NTSE_ASSERT(!sonPage->isPageLeaf() || maxSonPageKey1->m_rowId != maxSonPageKey2->m_rowId);	// 非叶节点无法保证rowId一定不相同

			// 加入pageId信息，标志data结束位
			IndexKey::appendPageId(maxSonPageKey2, newPageId);
			IndexKey::appendPageId(maxSonPageKey1, sonPageId);

			// 如果无法快速比较，取得非压缩格式键值
			if (!info->m_isFastComparable) {
				padKey1 = IndexKey::convertKeyCP(maxSonPageKey1, padKey1, m_tableDef, m_indexDef, true);
				padKey2 = IndexKey::convertKeyCP(maxSonPageKey2, padKey2, m_tableDef, m_indexDef, true);
			}
		}

		PageId siblingId = newPage->m_nextPage;

		smoPageHandle[smoPageNum] = sonHandle;
		smoPageIds[smoPageNum++] = sonPageId;
		smoPageHandle[smoPageNum] = newHandle;
		smoPageIds[smoPageNum++] = newPageId;

		session->unlockPage(&newHandle);	// 采用只释放锁的方式，后面清除SMO Bit的时候再放pin
		session->unlockPage(&sonHandle);

		// 5. 更新新页面右页面的左指针
		if (siblingId != INVALID_PAGE_ID) {
			updateNextPagePrevLink(session, file, logger, siblingId, newPageId);
			nftrace(ts.idx, tout << "Update new page's next page link: " << newPageId << " " << siblingId);
		}

		// 6. 定位父结点，需要判断记录的父节点信息是否正确，如果不正确，需要重新搜索，重新搜索只会出现一次
		PageId oldSonPageId = sonPageId;
		researched = searchParentInSMO(info, level, researched, oldSonPageId, &sonPageId, &sonHandle);
		sonPage = (IndexPage*)sonHandle->getPage();

		// 7.除非根结点分裂过，否则比较父页面对应项和insertRecord的大小
		if (!rootSplited) {
			SubRecord *findKey = isFastComparable ? maxSonPageKey1 : padKey1;
			sonPage->findKeyInPage(findKey, &SearchFlag::DEFAULT_FLAG, info->m_comparator, parentKey, &keyInfo);
			NTSE_ASSERT(keyInfo.m_result <= 0 || keyInfo.m_eOffset == sonPage->getDataEndOffset());
			assert(sonPage->findSpecifiedPageId(oldSonPageId));
			NTSE_ASSERT(IndexKey::getPageId(parentKey) == oldSonPageId);

			int result;
			if (info->m_isFastComparable)
				result = RecordOper::compareKeyCC(m_tableDef, maxSonPageKey2, parentKey, m_indexDef);
			else
				result = RecordOper::compareKeyPC(m_tableDef, padKey2, parentKey, m_indexDef);
			if (result < 0 || ((result == 0) && (maxSonPageKey2->m_rowId < parentKey->m_rowId))) {	// 将parentKey拷贝到maxSonPageKey2进行后续插入
				nftrace(ts.idx, tout << "Parent original key is larger, replace new max";);
				IndexKey::copyKey(maxSonPageKey2, parentKey, true);
				IndexKey::appendPageId(maxSonPageKey2, newPageId);
				if (!info->m_isFastComparable)
					padKey2 = IndexKey::convertKeyCP(parentKey, padKey2, m_tableDef, m_indexDef, true);
			}
		}

		level++;
	}

	SYNCHERE(SP_IDX_BEFORE_CLEAR_SMO_BIT);

	// 写入页面最新LSN，清除SMO位，标记页面为脏，释放pin
	for (u16 i = 0; i < smoPageNum; i++) {
		LOCK_PAGE_HANDLE(session, smoPageHandle[i], Exclusived);
		IndexPage *page = (IndexPage*)smoPageHandle[i]->getPage();
		assert(page != NULL);
		page->clearSMOBit(session, smoPageIds[i], logger);
		session->markDirty(smoPageHandle[i]);
		session->releasePage(&smoPageHandle[i]);
	}

	// 将真正需要插入的页面保存到info
	info->m_pageId = realSonPageId;
	info->m_pageHandle = GET_PAGE(session, file, PAGE_INDEX, realSonPageId, Exclusived, m_dboStats, NULL);
	info->m_comparator->setComparator(info->m_isFastComparable ? RecordOper::compareKeyCC : RecordOper::compareKeyPC);

	return;
}



/**
 * 对于插入操作，检查键值的唯一性
 *
 * @pre 调用之前，info需要保存插入键值在页面的哪个MiniPage，以及具体的起始偏移
 * 同时info->m_idxKey1表示插入键值的前驱，info->m_idxKey2表示后继，如果没有前驱或者后继，键值的m_size=0
 * @post 不应该改变info的keyInfo以及m_idxKey1和m_idxKey2的内容
 * @param info		插入信息句柄
 * @param hasSame	out 返回是否包含了相同值
 * @return 返回加锁之后的状态，如果一直返回IDX_SUCCESS，不一致返回IDX_RESTART，有死锁返回IDX_FAIL
 */
IDXResult DrsBPTreeIndex::insertCheckSame(DrsIndexScanHandleInfo *info, bool *hasSame) {
	IndexPage *page = (IndexPage*)info->m_pageHandle->getPage();
	PageId nextPageId = INVALID_PAGE_ID;
	SubRecord *findKey = info->m_findKey;
	SubRecord *prev = info->m_cKey1, *next = info->m_cKey2;
	bool spanPage = false;
	bool spanNext = false;
	KeyInfo keyInfo(info->m_keyInfo);

	info->m_cKey3->m_size = 0;
	if (!IndexKey::isKeyValid(prev)) {	// 要判断是需要跨页面还是只需要取前一个MiniPage数据
		NTSE_ASSERT(keyInfo.m_sOffset <= INDEXPAGE_DATA_START_OFFSET);
		spanPage = true;
		nextPageId = page->m_prevPage;
		prev = info->m_cKey3;
		// 否则只需要跨MiniPage，这是不可能的
	} 

	if (!IndexKey::isKeyValid(next)) {	// 要判断是需要跨页面还是只需要取后一个MiniPage数据
		if (keyInfo.m_eOffset < page->getDataEndOffset()) {
			// 只需要跨MiniPage
			u16 mpNo = keyInfo.m_miniPageNo;
			next = info->m_cKey3;
			page->getNextKey(NULL, keyInfo.m_eOffset, true, &mpNo, next);
		} else {
			spanPage = true;
			spanNext = true;
			nextPageId = page->m_nextPage;
			next = info->m_cKey3;
		}
	}

	// 假设一个页面里面至少包含两个索引项
	// 需要并且可能的话要跨页面取项
	if (spanPage && nextPageId != INVALID_PAGE_ID) {
		Session *session = info->m_session;
		PageId pageId = info->m_pageId;
		PageHandle *nextHandle;

		// 获得新页面
		IDX_SWITCH_AND_GOTO(latchHoldingLatch(session, nextPageId, pageId, &info->m_pageHandle, &nextHandle, Shared), CheckUniqueRestart, CheckUniqueFail);
		// 释放老页面的锁，防止后续操作引入latch的死锁，这个时候不用担心原页面被修改
		session->unlockPage(&info->m_pageHandle);
		IDX_SWITCH_AND_GOTO(checkSMOBit(session, &nextHandle), CheckRestartUnpin, CheckFailUnpin);
		// 这里需要保证对next-key所在页面加锁，保证没有可能会回退的修改，保证键值的唯一性判断正确
		// 可能回退的修改：某更新操作删除了当前插入键值的next-key，但是插入操作不判断就插入的话，update操作无法回退
		u64 token = session->getToken();
		nftrace(ts.irl, tout << session->getId() << " ins check dk lock " << nextPageId << " at least hold " << pageId);
		IDX_SWITCH_AND_GOTO(lockIdxObjectHoldingLatch(session, nextPageId, &nextHandle, FOR_INSERT, NULL/*&DrsBPTreeIndex::judgerForInsertCheckSame*/, (void*)&pageId, (void*)&spanNext), CheckRestartUnpin, CheckFailUnpin);

		page = (IndexPage*)nextHandle->getPage();

		KeyInfo ki;
		SubRecord *key = (spanNext ? next : prev);
		page->getExtremeKey(key, spanNext, &ki);

		session->releasePage(&nextHandle);
		idxUnlockObject(session, nextPageId, token);

		LOCK_PAGE_HANDLE(session, info->m_pageHandle, Exclusived);
	}

	// 比较是否有相同键值出现
	if (info->m_isFastComparable) {
		*hasSame = (IndexKey::isKeyValueEqual(findKey, prev) || IndexKey::isKeyValueEqual(findKey, next));
	} else {
		*hasSame = ((IndexKey::isKeyValid(prev) && RecordOper::compareKeyPC(m_tableDef, findKey, prev, m_indexDef) == 0) ||
			(IndexKey::isKeyValid(next) && RecordOper::compareKeyPC(m_tableDef, findKey, next, m_indexDef) == 0));
	}

	return IDX_SUCCESS;

CheckUniqueRestart:
	return IDX_RESTART;
CheckUniqueFail:
	return IDX_FAIL;
CheckFailUnpin:	// 这里都需要释放掉pin，本来只有放锁还持有pin
	info->m_session->unpinPage(&info->m_pageHandle);
	return IDX_FAIL;
CheckRestartUnpin:
	info->m_session->unpinPage(&info->m_pageHandle);
	return IDX_RESTART;
}


//////////////////////////////////////////////////////////////////////////////////////



/**
 * 找到指定层数的索引页面
 * @pre 调用者需要设置好比较函数
 * @post 返回之后traceInfo数组当中更新保存的是从根结点到指定层之间的结点信息,指定层到叶结点的结点信息不变
 * 这里成功返回之后scanInfo当中的m_pageId以及m_traceInfo会被设置。如果返回NULL，表示出现死锁，本操作需要重新开始
 *
 * @param scanInfo	IN/OUT	扫描信息
 * @param flag		查找标记
 * @param level		要定位到的层数
 * @param findType	当前的定位是查找过程的定位还是预估算代价的查找定位
 * @return 指定层数符合条件的页面
 */
PageHandle* DrsBPTreeIndex::findSpecifiedLevel(DrsIndexScanHandleInfo *scanInfo, SearchFlag *flag, uint level, FindType findType) {
	SubRecord *findKey = scanInfo->m_findKey;
	SubRecord *foundKey = scanInfo->m_cKey1;
	Session *session = scanInfo->m_session;
	File *file = ((DrsBPTreeIndice*)m_indice)->getFileDesc();
	PageId parentPageId;
	uint curLevel = MAX_INDEX_LEVEL + 1;
	uint diffLevel = MAX_INDEX_LEVEL + 1;

FindLevelStart:
	// 得到根页面并检查页面SMO状态
	parentPageId = m_rootPageId;
	PageHandle *parentHandle = GET_PAGE(session, file, PAGE_INDEX, parentPageId, Shared, m_dboStats, NULL);
	IndexPage *page = (IndexPage*)parentHandle->getPage();
	IDX_SWITCH_AND_GOTO(checkSMOBit(session, &parentHandle), FindLevelStart, Fail);

	curLevel = page->m_pageLevel;
	diffLevel = curLevel - level;
	assert(level <= curLevel);
	while (curLevel > level) {
		assert(page->m_pageCount != 0 && page->m_miniPageNum != 0);

		KeyInfo keyInfo;
		page->findKeyInPage(findKey, flag, scanInfo->m_comparator, foundKey, &keyInfo);	// 这里定位的项应该可以直接使用
		assert(keyInfo.m_result <= 0 || keyInfo.m_miniPageNo == page->m_miniPageNum - 1);
		scanInfo->m_everEqual = (keyInfo.m_result == 0 || scanInfo->m_everEqual);

		// 加子页面Latch，释放父页面Latch
		PageId childPageId = IndexKey::getPageId(foundKey);
		PageHandle *childHandle = NULL;
		IDX_SWITCH_AND_GOTO(latchHoldingLatch(session, childPageId, parentPageId, &parentHandle, &childHandle, Shared), FindLevelStart, Fail);

		// 第0项存储的是要定位层父结点页面
		scanInfo->m_traceInfo[--diffLevel].m_pageId = parentPageId;
		if (findType == IDX_SEARCH) {
			scanInfo->m_traceInfo[diffLevel].pi.si.m_miniPageNo = keyInfo.m_miniPageNo;
			scanInfo->m_traceInfo[diffLevel].pi.si.m_pageLSN = parentHandle->getPage()->m_lsn;
		} else {
			assert(findType == IDX_ESTIMATE);
			scanInfo->m_traceInfo[diffLevel].pi.ei.m_keyCount = keyInfo.m_keyCount;
			scanInfo->m_traceInfo[diffLevel].pi.ei.m_pageCount = page->m_pageCount;
		}

		session->releasePage(&parentHandle);

		// 检查子页面的SMO状态位
		IDX_SWITCH_AND_GOTO(checkSMOBit(session, &childHandle), FindLevelStart, Fail);

		parentPageId = childPageId;
		parentHandle = childHandle;
		page = (IndexPage*)childHandle->getPage();
		curLevel--;
	}

	assert(diffLevel == 0);
	scanInfo->m_lastSearchPathLevel = (u16)level;

	// 判断是否要升级Latch
	if (scanInfo->m_latchMode == Exclusived) {
		IDX_SWITCH_AND_GOTO(upgradeLatch(session, &parentHandle), FindLevelStart, Fail);
	}
	SYNCHERE(SP_IDX_WAIT_TO_FIND_SPECIAL_LEVEL_PAGE);
	// 记录下叶页面PageId
	scanInfo->m_pageId = parentPageId;
	return parentHandle;

Fail:
	return NULL;
}


/**
 * 找到指定层数的索引页面
 * @pre 调用者需要设置好比较函数
 * @post 返回之后traceInfo数组当中更新保存的是从根结点到指定层之间的结点信息,指定层到叶结点的结点信息不变
 * 这里成功返回之后scanInfo当中的m_pageId以及m_traceInfo会被设置。如果返回NULL，表示出现死锁，本操作需要重新开始
 *
 * @param scanInfo	IN/OUT	扫描信息
 * @param flag		查找标记
 * @param level		要定位到的层数
 * @param findType	当前的定位是查找过程的定位还是预估算代价的查找定位
 * @return 指定层数符合条件的页面
 */
PageHandle* DrsBPTreeIndex::findSpecifiedLevelSecond(DrsIndexScanHandleInfoExt *scanInfo, SearchFlag *flag, uint level, FindType findType) {
	SubRecord *findKey = scanInfo->m_findKey;
	SubRecord *foundKey = scanInfo->m_cKey1;
	Session *session = scanInfo->m_session;
	File *file = ((DrsBPTreeIndice*)m_indice)->getFileDesc();
	PageId parentPageId;
	uint curLevel = MAX_INDEX_LEVEL + 1;
	uint diffLevel = MAX_INDEX_LEVEL + 1;

FindLevelStart:
	// 得到根页面并检查页面SMO状态
	parentPageId = m_rootPageId;
	PageHandle *parentHandle = GET_PAGE(session, file, PAGE_INDEX, parentPageId, Shared, m_dboStats, NULL);
	IndexPage *page = (IndexPage*)parentHandle->getPage();
	IDX_SWITCH_AND_GOTO(checkSMOBit(session, &parentHandle), FindLevelStart, Fail);

	curLevel = page->m_pageLevel;
	diffLevel = curLevel - level;
	assert(level <= curLevel);
	while (curLevel > level) {
		assert(page->m_pageCount != 0 && page->m_miniPageNum != 0);

		KeyInfo keyInfo;
		page->findKeyInPage(findKey, flag, scanInfo->m_comparator, foundKey, &keyInfo);	// 这里定位的项应该可以直接使用
		assert(keyInfo.m_result <= 0 || keyInfo.m_miniPageNo == page->m_miniPageNum - 1);
		scanInfo->m_everEqual = (keyInfo.m_result == 0 || scanInfo->m_everEqual);

		// 加子页面Latch，释放父页面Latch
		PageId childPageId = IndexKey::getPageId(foundKey);
		PageHandle *childHandle = NULL;
		IDX_SWITCH_AND_GOTO(latchHoldingLatch(session, childPageId, parentPageId, &parentHandle, &childHandle, Shared), FindLevelStart, Fail);

		// 第0项存储的是要定位层父结点页面
		scanInfo->m_traceInfo[--diffLevel].m_pageId = parentPageId;
		if (findType == IDX_SEARCH) {
			scanInfo->m_traceInfo[diffLevel].pi.si.m_miniPageNo = keyInfo.m_miniPageNo;
			scanInfo->m_traceInfo[diffLevel].pi.si.m_pageLSN = parentHandle->getPage()->m_lsn;
		} else {
			assert(findType == IDX_ESTIMATE);
			scanInfo->m_traceInfo[diffLevel].pi.ei.m_keyCount = keyInfo.m_keyCount;
			scanInfo->m_traceInfo[diffLevel].pi.ei.m_pageCount = page->m_pageCount;
		}

		session->releasePage(&parentHandle);

		// 检查子页面的SMO状态位
		IDX_SWITCH_AND_GOTO(checkSMOBit(session, &childHandle), FindLevelStart, Fail);

		parentPageId = childPageId;
		parentHandle = childHandle;
		page = (IndexPage*)childHandle->getPage();
		curLevel--;
	}

	assert(diffLevel == 0);

	// 判断是否要升级Latch
	if (scanInfo->m_latchMode == Exclusived) {
		IDX_SWITCH_AND_GOTO(upgradeLatch(session, &parentHandle), FindLevelStart, Fail);
	}

	// 记录下叶页面PageId
	scanInfo->m_readPageId = parentPageId;
	return parentHandle;

Fail:
	return NULL;
}
/**
 * 检查扫描句柄中保存的叶页面信息是否有效
 * @param info
 * @param flag
 * @param needFetchNext
 * @return 页面信息有效返回IDX_SUCCESS, 页面无效返回IDX_RESTART，索引树为空返回IDX_FAIL
 */
IDXResult DrsBPTreeIndex::checkHandleLeafPage(DrsIndexScanHandleInfo *info, SearchFlag *flag, 
											  bool *needFetchNext, bool forceSearchPage) {
	if (NULL != info->m_pageHandle) {
		Session *session = info->m_session;
		SubRecord *record = info->m_cKey1;
		KeyInfo *keyInfo = &(info->m_keyInfo);

		assert(info->m_pageHandle->isPinned());
		// 在这个情况下，扫描句柄应该持有当前页面的pin，而没有Latch
		PageHandle *pageHandle = info->m_pageHandle;
		LOCK_PAGE_HANDLE(session, pageHandle, info->m_latchMode);
		IndexPage *page = (IndexPage*)pageHandle->getPage();

		// 其次判断页面LSN
		if (page->m_lsn != info->m_pageLSN || forceSearchPage) {
			// 首先要保证该页面还是本索引的叶页面，并且页面有效，并且页面不是在SMO状态
			if (!page->isPageLeaf() || page->m_pageType == FREE || checkSMOBit(session, &pageHandle) != IDX_SUCCESS) {
				if (pageHandle != NULL)
					session->releasePage(&pageHandle);
				return IDX_RESTART;
			}

			if (page->isPageEmpty()) {	// 索引树此时为空
				NTSE_ASSERT(page->isPageRoot());
				return IDX_FAIL;
			}

			assert(IndexPage::getIndexId(page->m_pageMark) == m_indexId);
			// 先在页面内部做查找，无论查找的键值存在不存在，只要该页面没有发生过SMO，
			// 或者即使SMO过，但是查找到的键值在页面的中部，就算该查找项有效
			makeFindKeyPADIfNecessary(info);
			*needFetchNext = page->findKeyInPage(info->m_findKey, flag, info->m_comparator, record, keyInfo);
			if (!(page->m_smoLSN == info->m_pageSMOLSN || (keyInfo->m_result <= 0 && keyInfo->m_sOffset != INDEXPAGE_DATA_START_OFFSET))) {
				// 本页面找不到，必须重新开始找，首先放锁
				session->releasePage(&pageHandle);
				return IDX_RESTART;
			}
		}
		return IDX_SUCCESS;
	}
	return IDX_RESTART;
}



/**
 * 检查扫描句柄中保存的叶页面信息是否有效
 * @param info
 * @param flag
 * @param needFetchNext
 * @return 页面信息有效返回IDX_SUCCESS, 页面无效返回IDX_RESTART，索引树为空返回IDX_FAIL
 */
IDXResult DrsBPTreeIndex::checkHandleLeafPageSecond(DrsIndexScanHandleInfoExt *info, SearchFlag *flag, 
											  bool *needFetchNext, bool forceSearchPage) {
	if (NULL != info->m_pageHandle) {
		Session *session = info->m_session;
		SubRecord *record = info->m_cKey1;
		KeyInfo *keyInfo = &(info->m_readKeyInfo);

		assert(info->m_pageHandle->isPinned());
		// 在这个情况下，扫描句柄应该持有当前页面的pin，而没有Latch
		LOCK_PAGE_HANDLE(session, info->m_pageHandle, info->m_latchMode);
		IndexPage *page = (IndexPage*)info->m_pageHandle->getPage();

		// 其次判断页面LSN
		if (page->m_lsn != info->m_pageLSN || forceSearchPage) {
			// 首先要保证该页面还是本索引的叶页面，并且页面有效，并且页面不是在SMO状态
			if (!page->isPageLeaf() || page->m_pageType == FREE || checkSMOBit(session, &(info->m_pageHandle)) != IDX_SUCCESS) {
				if (info->m_pageHandle != NULL)
					session->releasePage(&(info->m_pageHandle));
				return IDX_RESTART;
			}

			if (page->isPageEmpty()) {	// 索引树此时为空
				NTSE_ASSERT(page->isPageRoot());
				return IDX_FAIL;
			}

			assert(IndexPage::getIndexId(page->m_pageMark) == m_indexId);
			// 先在页面内部做查找，无论查找的键值存在不存在，只要该页面没有发生过SMO，
			// 或者即使SMO过，但是查找到的键值在页面的中部，就算该查找项有效
			makeFindKeyPADIfNecessary(info);
			*needFetchNext = page->findKeyInPage(info->m_findKey, flag, info->m_comparator, record, keyInfo);
			if (!(page->m_smoLSN == info->m_pageSMOLSN || (keyInfo->m_result <= 0 && keyInfo->m_sOffset != INDEXPAGE_DATA_START_OFFSET))) {
				// 本页面找不到，必须重新开始找，首先放锁
				session->releasePage(&(info->m_pageHandle));
				return IDX_RESTART;
			}
		}
		return IDX_SUCCESS;
	}
	return IDX_RESTART;
}
/**
 * 根据索引扫描句柄信息，定位相关叶页面以及相关键值位置
 * @pre	对于传入查找键值info->m_findKey，可能是REC_REDUNDANT/KEY_PAD/KEY_COMPRESS，当是KEY_COMPRESS的时候，需要注意如果
 *		无法进行快速比较，需要将压缩键值解压进行比较
 * @post 一般搜索到的键值保存在info->m_idxkey1，对于fetchNext的调用，如果页面没有改变，键值保存在info->m_findkey
 *
 * @param info			索引扫描信息句柄
 * @param needFetchNext	OUT	需要继续取逻辑后项
 * @param result		OUT	要查找键值和定位到的键值的大小关系
 * @return 定位成功或者失败，失败表示索引为空
 */
bool DrsBPTreeIndex::locateLeafPageAndFindKey(DrsIndexScanHandleInfo *info, bool *needFetchNext, 
											  s32 *result, bool forceSearchPage) {
	bool fetchNext = true;
	SearchFlag flag(info->m_forward, info->m_includeKey, (info->m_uniqueScan || !info->m_rangeFirst));
	IndexPage *page = NULL;

	KeyInfo *keyInfo = &(info->m_keyInfo);
	PageHandle *pageHandle = info->m_pageHandle;

	SYNCHERE(SP_IDX_WAIT_FOR_GET_NEXT);

	IDXResult rst = checkHandleLeafPage(info, &flag, &fetchNext, forceSearchPage);
	if (IDX_SUCCESS == rst) {
		goto Succeed;
	} else if (IDX_FAIL == rst) {
		goto Failed;
	}

	//需要从根页面开始定位叶页面
	// 保证查找键值m_findKey格式是正确的
	makeFindKeyPADIfNecessary(info);
	// 根据指定键值，从根结点向下搜索
	NTSE_ASSERT((pageHandle = findSpecifiedLevel(info, &flag, 0, IDX_SEARCH)) != NULL);
	if (((IndexPage*)(pageHandle->getPage()))->isPageEmpty()) {// 索引树为空
		goto Failed;
	}

	info->m_pageHandle = pageHandle;
	page = (IndexPage*)pageHandle->getPage();
	fetchNext = page->findKeyInPage(info->m_findKey, &flag, info->m_comparator, info->m_cKey1, keyInfo);

Succeed:
	//叶页面定位成功
	*needFetchNext = fetchNext;
	*result = keyInfo->m_result;
	return true;

Failed:
	//查找失败，索引树为空
	nftrace(ts.idx, tout << "Tree is empty";);
	info->m_pageHandle = pageHandle;
	*needFetchNext = false;
	*result = 1;
	return false;
}


/**
 * 根据索引扫描句柄信息，定位相关叶页面以及相关键值位置
 * @pre	对于传入查找键值info->m_findKey，可能是REC_REDUNDANT/KEY_PAD/KEY_COMPRESS，当是KEY_COMPRESS的时候，需要注意如果
 *		无法进行快速比较，需要将压缩键值解压进行比较
 * @post 一般搜索到的键值保存在info->m_idxkey1，对于fetchNext的调用，如果页面没有改变，键值保存在info->m_findkey
 *
 * @param info			索引扫描信息句柄
 * @param needFetchNext	OUT	需要继续取逻辑后项
 * @param result		OUT	要查找键值和定位到的键值的大小关系
 * @return 定位成功或者失败，失败表示索引为空
 */
bool DrsBPTreeIndex::locateLeafPageAndFindKeySecond(DrsIndexScanHandleInfoExt *info, bool *needFetchNext, 
											  s32 *result, bool forceSearchPage) {
	bool fetchNext = true;
	SearchFlag flag(info->m_forward, info->m_includeKey, (info->m_uniqueScan || !info->m_rangeFirst));
	IndexPage *page = NULL;

	KeyInfo *keyInfo = &(info->m_readKeyInfo);

	SYNCHERE(SP_IDX_WAIT_FOR_GET_NEXT);

	IDXResult rst = checkHandleLeafPageSecond(info, &flag, &fetchNext, forceSearchPage);
	if (IDX_SUCCESS == rst) {
		goto Succeed;
	} else if (IDX_FAIL == rst) {
		goto Failed;
	}

	//需要从根页面开始定位叶页面
	// 保证查找键值m_findKey格式是正确的
	makeFindKeyPADIfNecessary(info);
	// 根据指定键值，从根结点向下搜索
	NTSE_ASSERT((info->m_readPageHandle = findSpecifiedLevelSecond(info, &flag, 0, IDX_SEARCH)) != NULL);
	if (((IndexPage*)(info->m_readPageHandle->getPage()))->isPageEmpty()) {// 索引树为空
		goto Failed;
	}

	page = (IndexPage*)info->m_readPageHandle->getPage();
	fetchNext = page->findKeyInPage(info->m_findKey, &flag, info->m_comparator, info->m_cKey1, keyInfo);

Succeed:
	//叶页面定位成功
	*needFetchNext = fetchNext;
	*result = keyInfo->m_result;
	return true;

Failed:
	//查找失败，索引树为空
	nftrace(ts.idx, tout << "Tree is empty";);
	*needFetchNext = false;
	*result = 1;
	return false;
}
/**
 * 开始查找之前确定查找键值格式正确
 * @pre 如果需要换，交换对象是info->m_findKey和info->m_idxKey0，格式分别是KEY_COMPRESS和KEY_PAD
 * @post info->m_findKey格式是KEY_PAD，同时比较函数也被设置过
 * @param	info	扫描信息句柄
 * @return  交换了Key0和findKey则返回true， 否则返回false
 */
bool DrsBPTreeIndex::makeFindKeyPADIfNecessary(DrsIndexScanHandleInfo *info) {
	if (!info->m_isFastComparable && info->m_findKey->m_format == KEY_COMPRESS && IndexKey::isKeyValid(info->m_findKey)) {
		assert(info->m_key0 != NULL && info->m_key0->m_format == KEY_PAD);
		info->m_key0->m_size = m_indexDef->m_maxKeySize + RID_BYTES + PID_BYTES + 1;
		RecordOper::convertKeyCP(m_tableDef, m_indexDef, info->m_findKey, info->m_key0);
		info->m_comparator->setComparator(RecordOper::compareKeyPC);
		IndexKey::swapKey(&(info->m_key0), &(info->m_findKey));
		return true;
	}
	return false;
}


/**
 * 重定位结束之后，如果需要，把找到的键值存储到findKey当中，同时保证findKey的格式是KEY_COMPRESS,而idxKey0是KEY_PAD
 * @post info->m_findKey格式是KEY_PAD，同时比较函数也被设置过
 * @param info	扫描句柄信息
 */
void DrsBPTreeIndex::saveFoundKeyToFindKey(DrsIndexScanHandleInfo *info) {
	if (info->m_findKey->m_format == KEY_PAD) {
		assert(info->m_key0 != NULL && info->m_key0->m_format == KEY_COMPRESS);
		IndexKey::swapKey(&(info->m_findKey), &(info->m_key0));
	}
	IndexKey::swapKey(&info->m_findKey, &info->m_cKey1);
}


/**
 * 从当前info信息定位的项根据需要获取逻辑下一项
 * 在本函数中需要处理取下一项需要跨页面的情况
 *
 * @post 对于返回，IDX_SUCCESS表示持有返回项页面的Latch和Pin，其他情况不持有任何资源
 *			返回值保存在info->m_idxKey1当中
 *
 * @param info	扫描句柄信息
 * @return	返回取项成功IDX_SUCCESS、需要重新开始IDX_RESTART，不可能是IDX_FAIL
 */
IDXResult DrsBPTreeIndex::shiftToNextKey(DrsIndexScanHandleInfo *info) {
	PageHandle *handle = info->m_pageHandle;
	IndexPage *page = (IndexPage*)handle->getPage();
	PageId nextPageId = INVALID_PAGE_ID;
	bool spanPage = false;
	bool forward = info->m_forward;
	u16 offset;
	SubRecord *key = info->m_cKey1;
	SubRecord *lastRecord = (info->m_findKey->m_format == KEY_COMPRESS ? info->m_findKey : info->m_cKey1);	// 如果findKey格式不是压缩的,说明可能经过重定位或者是第一次查找,无论如何这时候可能的前一个键值都只会是idxKey1
	KeyInfo *keyInfo = &(info->m_keyInfo);

	if (forward) {
		offset = page->getNextKey(lastRecord, keyInfo->m_eOffset, true, &(info->m_keyInfo.m_miniPageNo), key);
		if (!IndexKey::isKeyValid(key)) {
			nextPageId = page->m_nextPage;
			spanPage = true;
		}
		keyInfo->m_sOffset = keyInfo->m_eOffset;
		keyInfo->m_eOffset = offset;
	} else {
		offset = page->getPrevKey(keyInfo->m_sOffset, true, &(info->m_keyInfo.m_miniPageNo), key);
		if (!IndexKey::isKeyValid(key)) {
			nextPageId = page->m_prevPage;
			spanPage = true;
		}
		keyInfo->m_eOffset = keyInfo->m_sOffset;
		keyInfo->m_sOffset = offset;
	}

	if (!spanPage)
		return IDX_SUCCESS;

	// 需要跨页面查找
	if (nextPageId == INVALID_PAGE_ID) {	// 扫描到达索引边界，返回成功
		info->m_hasNext = false;
		return IDX_SUCCESS;
	}

	Session *session = info->m_session;
	PageId pageId = info->m_pageId;
	PageHandle *nextHandle;
	File *file = ((DrsBPTreeIndice*)m_indice)->getFileDesc();
	LockMode latchMode = info->m_latchMode;
	u64 pageLSN = page->m_lsn;

	session->releasePage(&handle);

	SYNCHERE(SP_IDX_WANT_TO_GET_PAGE1);

	nextHandle = GET_PAGE(session, file, PAGE_INDEX, nextPageId, latchMode, m_dboStats, NULL);
	IDX_SWITCH_AND_GOTO(checkSMOBit(session, &nextHandle), SpanPageRestart, SpanPageFail);
	IDX_SWITCH_AND_GOTO(latchHoldingLatch(session, pageId, nextPageId, &nextHandle, &handle, latchMode), 
		SpanPageRestart, SpanPageFail);
	page = (IndexPage*)handle->getPage();

	if (pageLSN != page->m_lsn) {
		session->releasePage(&handle);
		session->releasePage(&nextHandle);
		goto SpanPageRestart;
	}

	page = (IndexPage*)nextHandle->getPage();
	page->getExtremeKey(key, forward, keyInfo);

	assert(IndexKey::isKeyValid(key));	// 一定能取到索引项
	// 释放原有页面的Latch和Pin
	session->releasePage(&handle);
	info->m_pageHandle = nextHandle;
	info->m_pageId = nextPageId;
	return IDX_SUCCESS;

SpanPageRestart:
	info->m_pageHandle = NULL;
	return IDX_RESTART;
SpanPageFail:
	assert(false);
	return IDX_FAIL;
}


/**
 * 当lockHoldingLatch加锁成功，但是页面LSN改变的时候，可以用该函数来确定加锁键值是不是还在当前页面
 * @pre info结构当中idxKey1是本次加锁的键值对象，m_findKey是本次最初要定位的键值，m_pageHandle是表示加了锁的页面信息句柄
 * @post idxKey1里面会保存重新搜索的键值的信息，如果返回false，info里面的keyInfo信息都作废，否则，keyInfo表示当前新的找到键值的信息
 * @param info		扫描信息句柄
 * @param inRange	out 要查找的键值是否在当前页面范围内
 * @return true表示要加锁的键值还在当前页面，false表示不在
 */
bool DrsBPTreeIndex::researchScanKeyInPage(DrsIndexScanHandleInfo *info, bool *inRange) {
	// 首先保证页面有效性，无效或者空页面，肯定找不到
	IndexPage *page = (IndexPage*)info->m_pageHandle->getPage();
	if (!page->isPageLeaf() || page->isPageSMO() || page->isPageEmpty())
		return false;

	SubRecord *gotKey = info->m_cKey1;
	RowId origRowId = gotKey->m_rowId;
	KeyInfo *keyInfo = &info->m_keyInfo;

	makeFindKeyPADIfNecessary(info);
	SearchFlag flag(info->m_forward, info->m_includeKey, (info->m_uniqueScan || !info->m_rangeFirst));
	bool fetchNext = page->findKeyInPage(info->m_findKey, &flag, info->m_comparator, gotKey, keyInfo);
	assert(IndexKey::isKeyValid(gotKey));

	// 唯一查询必须相等才是有效，否则可能刚好是被其他线程更新，但是位置和rowId没有发生改变
	if (info->m_uniqueScan)
		return (*inRange = (keyInfo->m_result == 0 && gotKey->m_rowId == origRowId));

	// 根据查找结果判断是否应该取左右的下一项
	if (fetchNext) {
		if (info->m_forward) {
			keyInfo->m_sOffset = keyInfo->m_eOffset;
			keyInfo->m_eOffset = page->getNextKey(gotKey, keyInfo->m_eOffset, true, &keyInfo->m_miniPageNo, gotKey);
			keyInfo->m_result = -1;
		} else {
			keyInfo->m_eOffset = keyInfo->m_sOffset;
			keyInfo->m_sOffset = page->getPrevKey(keyInfo->m_sOffset, true, &keyInfo->m_miniPageNo, gotKey);
			keyInfo->m_result = 1;
		}
	}

	// 范围查询，定位到查找范围，然后根据结果调整之后，如果rowId一致，说明现在定位的项是有效正确的；
	// 如果rowId无法一致肯定不准确，可能是原来定位的项被删除或者更新，或者范围之内又插入其他项，
	// 总之此时必须重新查找
	*inRange = page->isKeyInPageMiddle(keyInfo);
	return (*inRange && IndexKey::isKeyValid(gotKey) && gotKey->m_rowId == origRowId);
}


/**
 * 在持有一个页面Latch的情况下，加页面某个键值的行锁，如果加锁成功，既持有锁也持有Latch
 *
 * @param session			会话句柄
 * @param rowId				要加锁键值的rowId
 * @param lockMode			加锁模式
 * @param pageHandle		INOUT	页面句柄
 * @param rowHandle			OUT		行锁句柄
 * @param mustReleaseLatch	决定在restart的时候是不是可以不放页面的pin
 * @param info				扫描句柄，用于在页面LSN改变的时候，判断查找项是不是依然在该页面，默认值为NULL，如果为NULL，就不做这项判断
 * @return 返回成功IDX_SUCCESS，失败IDX_FAIL，需要重新开始IDX_RESTART
 */
IDXResult DrsBPTreeIndex::lockHoldingLatch(Session *session, RowId rowId, LockMode lockMode, PageHandle **pageHandle, RowLockHandle **rowHandle, bool mustReleaseLatch, DrsIndexScanHandleInfo *info) {
	assert(lockMode != None && rowHandle != NULL);

	u16 tableId = ((DrsBPTreeIndice*)m_indice)->getTableId();
	if ((*rowHandle = TRY_LOCK_ROW(session, tableId, rowId, lockMode)) != NULL)
		return IDX_SUCCESS;

	// 否则需要先释放本页面的Latch再无条件加锁
	LockMode curLatchMode = (*pageHandle)->getLockMode();
	IndexPage *page = (IndexPage*)(*pageHandle)->getPage();
	u64 oldLSN = page->m_lsn;
	session->unlockPage(pageHandle);

	SYNCHERE(SP_IDX_WAIT_TO_LOCK);

	*rowHandle = LOCK_ROW(session, tableId, rowId, lockMode);

	LOCK_PAGE_HANDLE(session, *pageHandle, curLatchMode);

	if (page->m_lsn == oldLSN)	// 本系统中似乎可以assert(page->m_lsn != pageLSN)，因为加锁加不上的记录肯定被其他线程操作过
		return IDX_SUCCESS;

	// 判断要查找的项是不是还在当前页面当中,如果是,不需要restart,只需要重新定位到查找项的位置
	bool inRange = false;
#ifdef TNT_ENGINE
	if(session->getTrans() != NULL) {
		if (info != NULL && researchScanKeyInPageSecond(info, &inRange))
			return IDX_SUCCESS;
	} else {
		if (info != NULL && researchScanKeyInPage(info, &inRange))
			return IDX_SUCCESS;
	}
#endif
	++m_indexStatus.m_numRLRestarts;	// 统计信息

	session->unlockRow(rowHandle);

#ifdef TNT_ENGINE
	if(session->getTrans() == NULL) {		//如果是ntse
		if (!mustReleaseLatch && inRange)	// 如果查找到的键值还在该页面，可以只释放lock不放pin，重新在该页面查找就行
			session->unlockPage(pageHandle);
		else{
			session->releasePage(pageHandle);	
		}
	} else {								//如果是tnt，不管如何，只要需要重启扫描，就必须释放这个页面
		session->releasePage(pageHandle);
	}
#endif
	return IDX_RESTART;
}


/**
 * 在持有一个页面Latch的情况下，获取另一个页面的Latch，两个模式相同
 *
 * 在此规定加Latch必须满足一定的顺序，即持有PageId小的页面的Latch可以直接获得PageId大的页面的Latch，
 * 否则需要等待，任何其他地方对索引页面加Latch的流程不得与这里发生冲突
 * 返回IDX_SUCCESS表示curPage和newPage都持有Latch和pin，
 * 返回IDX_RESTART或者IDX_FAIL表示两个页面都没持有Latch和pin
 *
 * @param session	会话句柄
 * @param newPageId	新页面的ID
 * @param curPageId 当前持有Latch页面ID
 * @param curHandle	当前页面句柄
 * @param newHandle	新页面句柄	INOUT 返回值为加了Latch的新页面
 * @param latchMode	加Latch的模式
 * @return 加Latch成功IDX_SUCCESS，需要重新开始IDX_RESTART
 */
IDXResult DrsBPTreeIndex::latchHoldingLatch(Session *session, PageId newPageId, PageId curPageId, PageHandle **curHandle, PageHandle **newHandle, LockMode latchMode) {
	assert(curPageId != newPageId);
	assert(newPageId != INVALID_PAGE_ID);

	File *file = ((DrsBPTreeIndice*)m_indice)->getFileDesc();

	if (curPageId < newPageId) {
		// 直接加Latch
		*newHandle = GET_PAGE(session, file, PAGE_INDEX, newPageId, latchMode, m_dboStats, NULL);
		return IDX_SUCCESS;
	} else {
		SYNCHERE(SP_IDX_WANT_TO_GET_PAGE2);
		// 首先尝试直接加Latch
		*newHandle = TRY_GET_PAGE(session, file, PAGE_INDEX, newPageId, latchMode, m_dboStats);
		if (*newHandle != NULL) {
			return IDX_SUCCESS;
		} else {
			++m_indexStatus.m_numLatchesConflicts; // 统计信息
			// 先释放本页面Latch再加新页面Latch，由于必须要加回本页面Latch判断LSN，因此只释放latch持有pin
			LockMode curLatchMode = (*curHandle)->getLockMode();
			u64 curLSN = (*curHandle)->getPage()->m_lsn;
			session->unlockPage(curHandle);

			SYNCHERE(SP_IDX_WANT_TO_GET_PAGE3);

			*newHandle = GET_PAGE(session, file, PAGE_INDEX, newPageId, latchMode, m_dboStats, NULL);

			SYNCHERE(SP_IDX_WANT_TO_GET_PAGE4);

			LOCK_PAGE_HANDLE(session, *curHandle, curLatchMode);

			if (curLSN == (*curHandle)->getPage()->m_lsn)
				return IDX_SUCCESS;
			else {
				++m_indexStatus.m_numLatchesConflicts;

				session->releasePage(newHandle);	// 先释放ID小的页面
				session->releasePage(curHandle);
				*newHandle = *curHandle = NULL;
				return IDX_RESTART;
			}
		}
	}
}


/**
 * 升级指定页面的Latch
 *
 * @param session	会话句柄
 * @param handle	in/out 要升级的页面句柄，成功返回则为加了X-Latch的句柄，否则为NULL
 * @return 升级成功IDX_SUCCESS，需要重新开始IDX_RESTART
 */
IDXResult DrsBPTreeIndex::upgradeLatch(Session *session, PageHandle **handle) {
	assert((*handle)->getLockMode() == Shared);

	Page *page = (*handle)->getPage();
	u64 curLSN = page->m_lsn;

	UPGRADE_PAGELOCK(session, *handle);
	assert((*handle)->getLockMode() == Exclusived);

	if (page->m_lsn == curLSN)
		return IDX_SUCCESS;
	else {
		session->releasePage(handle);
		return IDX_RESTART;
	}
}


/**
 * 根据表ID和加锁页面对象ID拼凑出一个唯一的对象ID供锁表加锁
 * @param objectId 对象ID
 * @return 和表ID拼凑过的真实加锁对象ID
 */
u64 DrsBPTreeIndex::getRealObjectId(u64 objectId) {
	return ((u64)m_tableDef->m_id << ((sizeof(u64) - sizeof(m_tableDef->m_id)) * 8)) | objectId;
}

/**
 * 尝试加某个对象锁
 * @param session	会话句柄
 * @param objectId	加锁对象
 * @return 加锁成功true，否则false
 */
bool DrsBPTreeIndex::idxTryLockObject(Session *session, u64 objectId) {
	u64 lockId = getRealObjectId(objectId);
	return session->tryLockIdxObject(lockId);
}

/**
 * 加某个对象锁
 * @param session	会话句柄
 * @param objectId	加锁对象
 * @return 加锁成功true，否则false
 */
bool DrsBPTreeIndex::idxLockObject(Session *session, u64 objectId) {
	u64 lockId = getRealObjectId(objectId);
	return session->lockIdxObject(lockId);
}

/**
 * 尝试释放某个对象锁
 * @param session	会话句柄
 * @param objectId	放锁对象
 * @param token		释放的锁的token必须大于当前token
 * @return 放锁成功true，否则false
 */
bool DrsBPTreeIndex::idxUnlockObject(Session *session, u64 objectId, u64 token) {
	u64 lockId = getRealObjectId(objectId);
	return session->unlockIdxObject(lockId, token);
}


/**
 * 用来在插入删除操作定位到页面加页面锁等锁而页面发生改变之后的重判断函数
 *
 * @param page		加锁页面对象
 * @param scaninfo	扫描句柄
 * @param nullparam	无用参数
 * @return 返回true表示当前加锁页面有效成功，否则需要重定位
 */
bool DrsBPTreeIndex::judgerForDMLLocatePage(IndexPage *page, void *scaninfo, void *nullparam) {
	DrsIndexScanHandleInfo *info = (DrsIndexScanHandleInfo*)scaninfo;
	UNREFERENCED_PARAMETER(nullparam);
	// 在页面经过SMO的情况下，根据键值判断当前页面是不是还是有效
	// 这里可以加上锁，表示其他事务的修改操作一定都结束或者回退了，不会出现只修改到一半的状态
	if (page->isPageLeaf()) {
		assert(info != NULL);
		KeyInfo *keyInfo = &(info->m_keyInfo);
		// 如果此处在makeFindKeyPadIfNecessary中交换了key0和findkey，那么后续需要再交换回来
		bool needSwapKey = makeFindKeyPADIfNecessary(info);
		page->findKeyInPage(info->m_findKey, &SearchFlag::DEFAULT_FLAG, info->m_comparator, info->m_cKey1, keyInfo);
		if (needSwapKey) {
			assert(info->m_key0 != NULL && info->m_key0->m_format == KEY_COMPRESS);
			IndexKey::swapKey(&(info->m_findKey), &(info->m_key0));
		}

		if (keyInfo->m_result == 0 || (keyInfo->m_result < 0 && keyInfo->m_sOffset != INDEXPAGE_DATA_START_OFFSET)
			|| (keyInfo->m_result > 0  && keyInfo->m_eOffset != page->getDataEndOffset()))
			return true;
	}

	return false;
}


/**
 * 在持有页面Latch的情况下，对某个索引对象加页锁
 * @param session		会话句柄
 * @param objectId		加锁对象的ID
 * @param pageHandle	in/out 加锁页面句柄
 * @param intention		加锁的意图，用于插入还是删除还是其他
 * @param judger		重判断函数对象
 * @param param1		传给重判断函数的参数1
 * @param param2		传给重判断函数的参数2
 * @return 加锁成功，返回IDX_SUCCESS，页面LSN被改变要重定位返回IDX_RESTART，加锁失败有死锁返回IDX_FAIL
 */
IDXResult DrsBPTreeIndex::lockIdxObjectHoldingLatch(Session *session, u64 objectId, PageHandle **pageHandle, LockIdxObjIntention intention, lockIdxObjRejudge judger, void *param1, void *param2) {
	if (idxTryLockObject(session, objectId))
		return IDX_SUCCESS;

	// 否则要先释放Latch再加锁避免死锁
	u64 oldSMOLSN = ((IndexPage*)(*pageHandle)->getPage())->m_smoLSN;
	LockMode lockMode = (*pageHandle)->getLockMode();
	session->unlockPage(pageHandle);

	SYNCHERE(SP_IDX_WAIT_FOR_PAGE_LOCK);

	u64 token = session->getToken();
	if (!idxLockObject(session, objectId)) {
		// 存在死锁
		session->unpinPage(pageHandle);
		return IDX_FAIL;
	}

	LOCK_PAGE_HANDLE(session, *pageHandle, lockMode);
	// 在这里的逻辑是只要页面没有经过分裂，页面内容还是可信的
	if (((IndexPage*)(*pageHandle)->getPage())->m_smoLSN == oldSMOLSN)
		return IDX_SUCCESS;
	// 或者经过判断函数判断
	if (judger != NULL && (this->*judger)((IndexPage*)(*pageHandle)->getPage(), param1, param2))
		return IDX_SUCCESS;

	if (intention == FOR_INSERT)
		++m_indexStatus.m_numILRestartsForI;
	else if (intention == FOR_DELETE)
		++m_indexStatus.m_numILRestartsForD;
	nftrace(ts.irl, tout << session->getId() << " lock " << objectId << " restart, oldSMOLSN: " << oldSMOLSN << " newSMOLsn: " << ((IndexPage*)(*pageHandle)->getPage())->m_smoLSN);

	// 否则页面已经被更改，需要重定位，放锁返回
	idxUnlockObject(session, objectId, token);
	session->releasePage(pageHandle);
	return IDX_RESTART;
}


/**
 * 检查指定页面的SMO位是否设置
 *
 * @param session		会话句柄
 * @param pageHandle	in/out 页面句柄
 * @return 返回检查成功、需要重新开始或者出现死锁返回失败
 */
IDXResult DrsBPTreeIndex::checkSMOBit(Session *session, PageHandle **pageHandle) {
	IndexPage *page = (IndexPage*)(*pageHandle)->getPage();
	if (page->isPageSMO()) {
		u64 lockId = getRealObjectId(m_indexId);
		if (session->isLocked(lockId))
			return IDX_SUCCESS;

		session->unlockPage(pageHandle);
		// 加SMO锁，失败释放pin返回IDX_FAIL，成功放锁放pin返回IDX_RESTART
		SYNCHERE(SP_IDX_WAIT_FOR_SMO_BIT);

		u64 token = session->getToken();
		if (!idxLockObject(session, m_indexId)) {
			session->unpinPage(pageHandle);
			return IDX_FAIL;
		}

		idxUnlockObject(session, m_indexId, token);
		session->unpinPage(pageHandle);
		return IDX_RESTART;
	}

	return IDX_SUCCESS;
}


/**
 * 获取索引的基本统计信息
 * @return 索引基本统计信息
 */
const IndexStatus& DrsBPTreeIndex::getStatus() {
	IndicePageManager *pageManager = ((DrsBPTreeIndice*)m_indice)->getPagesManager();
	pageManager->updateNewStatus(m_indexId, &m_indexStatus);
	return m_indexStatus;
}

/**
 * 获得索引数据对象统计信息 
 * @return 数据对象状态
 */
DBObjStats* DrsBPTreeIndex::getDBObjStats() {
	return m_dboStats;
}

/**
 * 更新索引的扩展统计信息
 * @param session			会话句柄
 * @param maxSamplePages	最大采样页面数
 */
void DrsBPTreeIndex::updateExtendStatus(Session *session, uint maxSamplePages) {
	McSavepoint mcSave(session->getMemoryContext());
	SampleResult *result = SampleAnalyse::sampleAnalyse(session, this, (int)maxSamplePages, 0);
	m_indexStatusEx.m_pctUsed = result->m_fieldCalc[0].m_average * 1.0 / INDEX_PAGE_SIZE;
	assert(m_indexStatusEx.m_pctUsed <= 1.0);
	m_indexStatusEx.m_compressRatio = result->m_fieldCalc[2].m_average * 1.0 / result->m_fieldCalc[1].m_average;
	delete result;
}

/**
 * 获取索引的扩展统计信息（只是返回updateExtendStatus计算好的信息，不重新采样统计）
 * @return 索引的扩展统计信息
 */
const IndexStatusEx& DrsBPTreeIndex::getStatusEx() {
	return m_indexStatusEx;
}



///////////////////////////////////////////////////////////////////////////////////


/**
 * 验证本索引结构的大致正确性
 * @pre 保证索引树此时不会被修改
 * @param session	会话句柄
 * @param key1		用于保存比较键值的变量1
 * @param key2		用于保存比较键值的变量2
 * @param pkey		pad格式的键值
 * @param fullCheck	是否执行严格的键值大小检查
 * @return 返回验证是否成功
 */
bool DrsBPTreeIndex::verify(Session *session, SubRecord *key1, SubRecord *key2, SubRecord *pkey, bool fullCheck) {
	PageHandle *path[MAX_INDEX_LEVEL];
	IndexPage *pages[MAX_INDEX_LEVEL];
	PageId pageIds[MAX_INDEX_LEVEL];
	IndexPage *page;
	u16 offsets[MAX_INDEX_LEVEL];
	s32 level = 0;
	u16 totalLevel;

	// 首先遍历整个索引将相关页面所在的块释放
	path[level] = GET_PAGE(session, ((DrsBPTreeIndice*)m_indice)->getFileDesc(), PAGE_INDEX, m_rootPageId, Shared, m_dboStats, NULL);
	offsets[level] = INDEXPAGE_DATA_START_OFFSET;
	pageIds[level] = m_rootPageId;
	pages[level] = (IndexPage*)path[level]->getPage();

	IndexPage *rootPage = (IndexPage*)path[0]->getPage();
	totalLevel = rootPage->m_pageLevel;
	assert(rootPage->isPageRoot());

	while (true) {
		if (level < 0)	// 所有页面处理完毕
			break;

		page = (IndexPage*)path[level]->getPage();
		assert(rootPage->m_pageLevel == totalLevel && page->m_pageLevel + level == totalLevel);
		assert(page->isPageRoot() || page->m_pageCount != 0);
		assert(page->m_pageCount == 0 || page->m_miniPageNum != 0);
		u16 offset = offsets[level];
		PageId pageId;

		if (page->m_pageLevel == 0) {	// 检查叶页面的正确性回朔
			if (fullCheck)
				page->traversalAndVerify(m_tableDef, m_indexDef, key1, key2, pkey);
			else
				page->traversalAndVerify(m_tableDef, m_indexDef, NULL, NULL, NULL);
			assert(page->isPageLeaf());
			session->releasePage(&path[level]);
			level--;
			continue;
		}

		if (offset == page->getDataEndOffset()) {	// 某个非叶页面遍历结束
			// 检查页面本身的正确性
			if (fullCheck)
				page->traversalAndVerify(m_tableDef, m_indexDef, key1, key2, pkey);
			else
				page->traversalAndVerify(m_tableDef, m_indexDef, NULL, NULL, NULL);
			session->releasePage(&path[level]);
			level--;
		} else {	// 获取下一项
			// 首先得到下一项键值内容
			page->fetchKeyByOffset(offset, &key1, &key2);

			offset = page->getNextKeyPageId(offset, &pageId);
			offsets[level] = offsets[level] + offset;

			level++;
			path[level] = GET_PAGE(session, ((DrsBPTreeIndice*)m_indice)->getFileDesc(), PAGE_INDEX, pageId, Shared, m_dboStats, NULL);	// 不可能有死锁
			offsets[level] = INDEXPAGE_DATA_START_OFFSET;
			pageIds[level] = pageId;
			pages[level] = (IndexPage*)path[level]->getPage();

			// 获得叶页面的最大项
			IndexPage *indexPage = (IndexPage*)path[level]->getPage();
			assert(indexPage->m_pageLevel == page->m_pageLevel - 1);
			KeyInfo keyInfo;
			indexPage->getLastKey(key2, &keyInfo);

			RecordOper::convertKeyCP(m_tableDef, m_indexDef, key1, pkey);

			// 确认key1 >= key2
			if (offsets[level - 1] < page->getDataEndOffset()) {	// 每层页面的最后一项可以不满足
				assert(IndexKey::isKeyValid(key1) && IndexKey::isKeyValid(key2));
				s32 result = RecordOper::compareKeyPC(m_tableDef, pkey, key2, m_indexDef);
				assert(result >= 0);
				UNREFERENCED_PARAMETER(result);
			}
		}
	}

	assert(level <= 0);

	return true;
}


/**
 * 给定一个键值，得到键值查找路径信息和所在叶页面信息，用于查询范围估算
 * @param key			要查找的键值
 * @param info			查询信息对象
 * @param flag			查找符号
 * @param singleRoot	out 返回是否索引只有一个根页面
 * @param fetchNext		out 返回查找键值相对于当前结果是否要取下一项
 * @param leafKeyCount	out 查找键值在叶页面当中的第几项
 * @param leafPageCount	out 查询简直所在叶页面共有多少项
 */
void DrsBPTreeIndex::estimateKeySearch(const SubRecord *key, DrsIndexScanHandleInfo *info, SearchFlag *flag, 
									   bool *singleRoot, bool *fetchNext, u16 *leafKeyCount, u16 *leafPageCount) {
	info->m_isFastComparable = (key == NULL) ? 
		RecordOper::isFastCCComparable(m_tableDef, m_indexDef, m_indexDef->m_numCols, m_indexDef->m_columns) : 
		RecordOper::isFastCCComparable(m_tableDef, m_indexDef, key->m_numCols, key->m_columns);

	MemoryContext *memoryContext = info->m_session->getMemoryContext();

	if (info->m_isFastComparable && key != NULL) {	// 这个情况下，可以将键值转换成C格式的再比较，提高性能
		info->m_findKey = IndexKey::allocSubRecord(memoryContext, key->m_numCols, key->m_columns, KEY_COMPRESS, m_indexDef->m_maxKeySize);
		RecordOper::convertKeyPC(m_tableDef, m_indexDef, key, info->m_findKey);
		info->m_comparator->setComparator(RecordOper::compareKeyCC);
	} else if (key != NULL) {
		assert(key->m_format == KEY_PAD);
		info->m_findKey = IndexKey::allocSubRecord(memoryContext, key, m_indexDef);
		info->m_findKey->m_rowId = INVALID_PAGE_ID;
		info->m_comparator->setComparator(RecordOper::compareKeyPC);
	} else {
		assert(key == NULL);
		info->m_findKey = IndexKey::allocSubRecord(memoryContext, m_indexDef, KEY_PAD);
		info->m_findKey->m_size = 0;	// 表示没有查找键值
	}

	PageHandle *handle = findSpecifiedLevel(info, flag, 0, IDX_ESTIMATE);
	assert(handle != NULL);	// 这里不应该出现死锁
	*leafPageCount = ((IndexPage*)(handle->getPage()))->m_pageCount;
	*singleRoot = ((IndexPage*)(handle->getPage()))->isPageRoot();

	// 计算叶页面定位的键值偏移情况
	IndexPage *page = (IndexPage*)(handle->getPage());
	if (page->isPageEmpty()) {
		*leafKeyCount = 0;
		*fetchNext = false;
	} else {
		KeyInfo keyInfo;
		*fetchNext = page->findKeyInPage(info->m_findKey, flag, info->m_comparator, info->m_cKey1, &keyInfo);
		*leafKeyCount = keyInfo.m_keyCount;
	}

	info->m_session->releasePage(&handle);
}


/**
 * 在DML修改操作之前增加统计信息
 * @param op	新的操作
 * @param times	增加的统计次数
 */
void DrsBPTreeIndex::statisticOp(IDXOperation op, int times) {
	switch (op) {
	case IDX_INSERT:
		m_dboStats->countIt(DBOBJ_ITEM_INSERT, times);
		break;
	case IDX_DELETE:
		m_dboStats->countIt(DBOBJ_ITEM_DELETE, times);
		break;
	case IDX_UPDATE:
		m_dboStats->countIt(DBOBJ_ITEM_UPDATE, times);
		break;
	}
}


/**
 * 开始一次采样统计过程
 * @param session			会话句柄
 * @param wantSampleNum		需要采样的页面量
 * @param fastSample		是否是快速采样：是表示内存buffer采样为主，否表示外存随机采样为主
 * @return 初始化好的采样句柄
 */
SampleHandle* DrsBPTreeIndex::beginSample(Session *session, uint wantSampleNum, bool fastSample) {
	ftrace(ts.idx, tout << session << wantSampleNum << fastSample);

	IndexSampleHandle *sampleHandle = new IndexSampleHandle(session, wantSampleNum, fastSample);
	if (fastSample) {	// 表示主要在buffer随机采样
		Buffer *buffer = ((DrsBPTreeIndice*)m_indice)->getDatabase()->getPageBuffer();
		File *file = ((DrsBPTreeIndice*)m_indice)->getFileDesc();
		sampleHandle->m_bufferHandle = buffer->beginScan(session->getId(), file);
	}

	sampleHandle->m_key = IndexKey::allocSubRecord(session->getMemoryContext(), m_indexDef, KEY_COMPRESS);
	return sampleHandle;
}

/**
 * 取得采样的下一个页面
 * @param handle	采样句柄
 * @return 样本
 */
Sample* DrsBPTreeIndex::sampleNext(SampleHandle *handle) {
	IndexSampleHandle *idxHandle = (IndexSampleHandle*)handle;

	if (idxHandle->m_fastSample) {	// 进行buffer为主的采样
		Database *db = ((DrsBPTreeIndice*)m_indice)->getDatabase();
		Buffer *buffer = db->getPageBuffer();
		Bcb *bcb = NULL;
		while (true) {
			bcb = (Bcb*)buffer->getNext(idxHandle->m_bufferHandle);
			if (bcb == NULL)
				return NULL;
			if (isPageSamplable(bcb->m_page, bcb->m_pageKey.m_pageId))
				break;
		}
		return sample(idxHandle->m_session, bcb->m_page, m_tableDef, idxHandle->m_key);
	}

	// 进行磁盘为主的采样
	while (true) {
		if (idxHandle->m_eofIdx) {	// 采样扫描结束
			NTSE_ASSERT(idxHandle->m_runs != 0);
			return NULL;
		}

		PageHandle *pageHandle;
		Session *session = idxHandle->m_session;
		if (idxHandle->m_curRangePages == 0 || idxHandle->m_curRangePages > idxHandle->m_rangeSamplePages) {
			pageHandle = searchSampleStart(idxHandle);
		} else {
			pageHandle = GET_PAGE(session, ((DrsBPTreeIndice*)m_indice)->getFileDesc(), PAGE_INDEX, idxHandle->m_curPageId, Shared, m_dboStats, NULL);
		}

		// 当前情况是可以在本范围继续取下一个页面
		assert(idxHandle->m_curRangePages <= idxHandle->m_rangeSamplePages);
		assert(pageHandle != NULL);
		NTSE_ASSERT(idxHandle->m_runs != 0 || idxHandle->m_curPageId != INVALID_PAGE_ID);
		++idxHandle->m_curRangePages;
		PageId curPageId = idxHandle->m_curPageId;
		IndexPage *page = (IndexPage*)pageHandle->getPage();
		Sample *spl = isPageSamplable(page, curPageId) ? sample(idxHandle->m_session, page, m_tableDef, idxHandle->m_key) : NULL;
		idxHandle->m_curPageId = page->m_nextPage;

		session->releasePage(&pageHandle);

		if (idxHandle->m_curPageId == INVALID_PAGE_ID)
			idxHandle->m_eofIdx = true;
		if (spl == NULL) {	// 当前采样失败，重新开始一次采样定位
			idxHandle->m_curRangePages = 0;
			idxHandle->m_curPageId = INVALID_PAGE_ID;
			if (idxHandle->m_runs == 0)	// 处理第一次采样，还没得到页面的情况，因为索引采样至少能采样根页面，不应该返回空
				idxHandle->m_eofIdx = false;
			continue;
		}

		spl->m_ID = curPageId;
		++idxHandle->m_runs;
		return spl;
	}
}

/**
 * 结束一次采样
 * @param handle	采样句柄
 */
void DrsBPTreeIndex::endSample(SampleHandle *handle) {
	ftrace(ts.idx, );

	IndexSampleHandle *idxHandle = (IndexSampleHandle*)handle;
	Database *db = ((DrsBPTreeIndice*)m_indice)->getDatabase();
	if (idxHandle->m_fastSample) {
		Buffer *buffer = db->getPageBuffer();
		buffer->endScan(idxHandle->m_bufferHandle);
	}

	delete handle;
}

/**
 * 进行对指定页面的采样
 * @param session	会话句柄
 * @param page		页面
 * @param tableDef	表定义
 * @param key		读取键值使用
 * @return 采样结果，如果为NULL，表示该页无法继续再采样(页面为空，无法继续取得区间的下一个页面)
 */
Sample* DrsBPTreeIndex::sample(Session *session, Page *page, const TableDef *tableDef, SubRecord *key) {
	IndexPage *indexPage = (IndexPage*)page;
	Sample *sample = Sample::create(session, IndexStatusEx::m_fieldNum);
	(*sample)[0] = INDEX_PAGE_SIZE - indexPage->m_freeSpace;
	(*sample)[1] = indexPage->calcUncompressedSpace(tableDef, m_indexDef, key);
	(*sample)[2] = indexPage->getDataEndOffset() - INDEXPAGE_DATA_START_OFFSET;

	return sample;
}

/**
 * 搜索一次采样范围的起始位置
 * @param idxHandle	索引采样句柄
 * @return 返回范围采样的起始页面句柄
 */
PageHandle* DrsBPTreeIndex::searchSampleStart(IndexSampleHandle *idxHandle) {
	Session *session = idxHandle->m_session;
	File *file = ((DrsBPTreeIndice*)m_indice)->getFileDesc();
	PageId parentPageId;
	u64 skippedPages = idxHandle->m_runs * (idxHandle->m_rangeSamplePages + idxHandle->m_skipPages);
	u64 branchPages = idxHandle->m_estimateLeafPages;
	u64 skipBase = 0, curBase = 1;
	assert(idxHandle->m_runs == 0 || branchPages != 0);
	if (idxHandle->m_runs == 0) {
		memset(idxHandle->m_levelItems, 0, sizeof(idxHandle->m_levelItems[0]) * MAX_INDEX_LEVEL);
		branchPages = 0;
	}

FindLevelStart:
	// 得到根页面并检查页面SMO状态
	parentPageId = m_rootPageId;
	PageHandle *parentHandle = GET_PAGE(session, file, PAGE_INDEX, parentPageId, Shared, m_dboStats, NULL);
	IndexPage *page = (IndexPage*)parentHandle->getPage();
	u16 rootLevel = page->m_pageLevel;
	IDX_SWITCH_AND_GOTO(checkSMOBit(session, &parentHandle), FindLevelStart, Fail);
	// 根据索引树高度,决定每层扫描可以随机跳过多少项

	while (page->m_pageLevel > 0) {
		assert(page->m_pageCount != 0 && page->m_miniPageNum != 0);
		// 确定当前层应该取哪一项继续定位
		PageId childPageId = INVALID_PAGE_ID;
		u16 pos = INDEXPAGE_DATA_START_OFFSET;
		uint randSkip = rootLevel > 3 ? System::random() % 2 : System::random() % 3;
		curBase *= idxHandle->m_levelItems[page->m_pageLevel] == 0 ? page->m_pageCount : idxHandle->m_levelItems[page->m_pageLevel];

		if (idxHandle->m_runs == 0) {
			idxHandle->m_levelItems[page->m_pageLevel] = page->m_pageCount;
			idxHandle->m_estimateLeafPages *= page->m_pageCount;
		}

		uint i = 0;
		if (idxHandle->m_runs != 0 || idxHandle->m_estimateLeafPages > idxHandle->m_wantNum) {
			for (; i < page->m_pageCount; i++) {
				pos = pos + page->getNextKeyPageId(pos, &childPageId);
				u64 curSkipPages = branchPages * (skipBase + i) / curBase;
				if (idxHandle->m_runs == 0 || curSkipPages > skippedPages) {
					// 如果是第一次，只需要定位到第0项，如果非第一次，看估计的比例决定是否继续取下一项
					// 在此基础上，允许再随机跳过几项
					if (randSkip > 0)
						--randSkip;
					else
						break;
				}
			}
		} else {
			page->getNextKeyPageId(pos, &childPageId);
		}

		skipBase = (skipBase + i) * idxHandle->m_levelItems[page->m_pageLevel] == 0 ? page->m_pageCount : idxHandle->m_levelItems[page->m_pageLevel];

		// 加子页面Latch，释放父页面Latch
		PageHandle *childHandle = NULL;
		IDX_SWITCH_AND_GOTO(latchHoldingLatch(session, childPageId, parentPageId, &parentHandle, &childHandle, Shared), FindLevelStart, Fail);
		session->releasePage(&parentHandle);

		// 检查子页面的SMO状态位
		IDX_SWITCH_AND_GOTO(checkSMOBit(session, &childHandle), FindLevelStart, Fail);

		parentPageId = childPageId;
		parentHandle = childHandle;
		page = (IndexPage*)childHandle->getPage();
	}

	if (idxHandle->m_runs == 0) {	// 需要更新部分采样信息
		u64 estimateLeafPages = idxHandle->m_estimateLeafPages;
		if (idxHandle->m_maxSample >= estimateLeafPages || idxHandle->m_wantNum >= estimateLeafPages) {	// 此时应该采样所有页面
			idxHandle->m_rangeSamplePages = estimateLeafPages;
			idxHandle->m_skipPages = 0;
		} else if (estimateLeafPages < idxHandle->m_rangeSamplePages * 2) {	// 估计页面数不足，可以一次采样
			idxHandle->m_skipPages = 0;
			idxHandle->m_rangeSamplePages = idxHandle->m_maxSample;
		} else if (estimateLeafPages > idxHandle->m_maxSample * 4) {	// 采样页面远小于总页面数，增大skip页面数
			assert(estimateLeafPages / (idxHandle->m_maxSample / idxHandle->m_rangeSamplePages + 1) > idxHandle->m_rangeSamplePages);
			idxHandle->m_skipPages = estimateLeafPages / (idxHandle->m_maxSample / idxHandle->m_rangeSamplePages + 1) - idxHandle->m_rangeSamplePages;
		}
	}

	idxHandle->m_curPageId = parentPageId;
	idxHandle->m_curRangePages = 1;
	return parentHandle;

Fail:
	NTSE_ASSERT(false);
	return NULL;
}

/**
 * 判断一个页面是否是可采样的页面
 * @param page		采样页面
 * @param pageId	页面ID
 * @return 返回页面是否可以被采样
 */
bool DrsBPTreeIndex::isPageSamplable(Page *page, PageId pageId) {
	IndexPage *indexPage = (IndexPage*)page;
	vecode(vs.idx, {
		if (IndexPage::getIndexId(indexPage->m_pageMark) != m_indexId) {
			nftrace(ts.idx, tout << "sample a page not belonging to this index");
		}
		if (!indexPage->isPageLeaf()) {
			nftrace(ts.idx, tout << "sample a page who is not leaf");
		}
	});
	return (pageId >= IndicePageManager::NON_DATA_PAGE_NUM && IndexPage::getIndexId(indexPage->m_pageMark) == m_indexId && indexPage->isPageLeaf());
}

/**
 * 增加一次死锁操作统计
 */
void DrsBPTreeIndex::statisticDL() {
	m_indexStatus.m_numDeadLockRestarts.increment();
}

/** 将原有索引定义替换为新的索引定义
 * @param splitFactor	分裂系数
 */
void DrsBPTreeIndex::setSplitFactor( s8 splitFactor ) {
	m_indexDef->m_splitFactor = splitFactor;
}

/**
 * 扫描句柄信息结构初始化构造函数
 */
DrsIndexScanHandleInfo::DrsIndexScanHandleInfo(Session *session, const TableDef *tableDef, const IndexDef *indexDef, LockMode lockMode, LockMode latchMode, bool isFastComparable, CompareKey comparator) {
	m_session = session;
	m_indexDef = indexDef;
	m_findKey = NULL;
	m_pageId = INVALID_PAGE_ID;
	m_pageHandle = NULL;
	m_rowHandle = NULL;
	m_pageLSN = m_pageSMOLSN = 0;
	m_lockMode = lockMode;
	m_latchMode = latchMode;
	m_key0 = m_cKey1 = m_cKey2 = m_cKey3 = NULL;
	void *memory = (KeyComparator*)m_session->getMemoryContext()->alloc(sizeof(KeyComparator));
	m_comparator = new (memory) KeyComparator(tableDef, indexDef);
	m_comparator->setComparator(comparator);
	m_forward = true;
	m_includeKey = true;
	m_hasNext = true;
	m_uniqueScan = true;
	m_rangeFirst = true;
	m_everEqual = false;
	m_isFastComparable = isFastComparable;
	m_lastSearchPathLevel = 0;
	memset(m_traceInfo, 0, sizeof(struct IndexSearchTraceInfo) * MAX_INDEX_LEVEL);
}


/**
 * 采样句柄初始化构造函数
 */
IndexSampleHandle::IndexSampleHandle(Session *session, int wantNum, bool fastSample) : SampleHandle(session, wantNum, fastSample) {
	m_wantNum = wantNum;
	m_curRangePages = 0;
	m_estimateLeafPages = 1;
	m_skipPages = m_rangeSamplePages = (uint)wantNum > DEFAULT_MIN_SAMPLE_RANGE_PAGES ? DEFAULT_MIN_SAMPLE_RANGE_PAGES : max(wantNum / DEFAULT_SAMPLE_RANGES, (uint)1);
	m_curPageId = INVALID_PAGE_ID;
	m_eofIdx = false;
	m_bufferHandle = NULL;
	m_runs = 0;
	m_key = NULL;
	memset(m_levelItems, 0, sizeof(m_levelItems[0]) * MAX_INDEX_LEVEL);
}




/**
 * 根据索引扫描句柄信息，定位最右叶页面取出最大键值
 * 传入传出的查找键值foundKey，是KEY_COMPRESS， foundKey在外部构造
 *
 * @param session			
 * @param foundKey	 OUT	最大键
 * @return 成功或者失败，失败表示索引为空
 */
bool DrsBPTreeIndex::locateLastLeafPageAndFindMaxKey(Session *session, SubRecord *foundKey) {
	//找到叶子层
	uint level = 0; 
	//SubRecord *foundKey = NULL;
	PageId parentPageId;	
	uint curLevel = MAX_INDEX_LEVEL + 1;
	uint diffLevel = MAX_INDEX_LEVEL + 1;
	File *file = ((DrsBPTreeIndice*)m_indice)->getFileDesc();
	SearchFlag flag(false, true, true);
	KeyInfo keyInfo;
	SubRecord *key = IndexKey::allocSubRecord(session->getMemoryContext(), m_indexDef, KEY_COMPRESS);

	parentPageId = m_rootPageId;
	PageHandle *parentHandle = GET_PAGE(session, file, PAGE_INDEX, parentPageId, Shared, m_dboStats, NULL);
	IndexPage *page = (IndexPage*)parentHandle->getPage();
		
	curLevel = page->m_pageLevel;
	diffLevel = curLevel - level;

	assert(level <= curLevel);

	while (curLevel > level) {
		assert(page->m_pageCount != 0 && page->m_miniPageNum != 0);
		page->getExtremeKey(key, false, &keyInfo);

		//加子页面latch，释放父页面latch
		PageId childPageId = IndexKey::getPageId(key);
		PageHandle *childHandle = NULL;
		latchHoldingLatch(session, childPageId, parentPageId, &parentHandle, &childHandle, Shared);	
		session->releasePage(&parentHandle);
		parentPageId = childPageId;
		parentHandle = childHandle;
		page = (IndexPage*)childHandle->getPage();
		
		curLevel--;
	}
	assert(curLevel == level);
	//如果页面为空，则返回false,否则取页内最大键值
	if(page->m_miniPageNum > 0)
		goto Succeed;
	else	
		goto Failed;

Succeed:
	page->getExtremeKey(key, false, &keyInfo);
	RecordOper::extractSubRecordCRNoLobColumn(m_tableDef, m_indexDef, key, foundKey);
	foundKey->m_rowId = key->m_rowId;
	session->releasePage(&parentHandle);
	return true;

Failed:
	session->releasePage(&parentHandle);
	return false; 	
}

}

