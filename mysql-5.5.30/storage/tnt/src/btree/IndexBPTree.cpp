/**
 * NTSE B+������ʵ����
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
 * �ƶ���������α굽��������һ����¼
 */
void DrsIndexScanHandleInfoExt::moveCursor() {
	checkFormat();
	IndexKey::swapKey(&m_findKey, &m_readKey);

	if (m_pageHandle != m_readPageHandle && m_pageHandle != NULL) {
		//�α��Ƶ���һҳ���ˣ���ʱ�ͷ���һҳ���pin
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
 * �ƶ���������α굽ָ���ļ�¼
 * @param key ָ���ļ�¼
 */
void DrsIndexScanHandleInfoExt::moveCursorTo(const SubRecord *key) {
	assert(KEY_PAD == key->m_format);
	checkFormat();
	m_findKey->m_size = m_indexDef->m_maxKeySize;
	RecordOper::convertKeyPC(m_tableDef, m_indexDef, key, m_findKey);

	//�����һ�ζ�λҳ��ʧ�ܣ��´�һ������search path�������pin�ŵ�
	if(m_pageHandle != m_readPageHandle)
		m_session->unpinPage(&m_readPageHandle);

	m_includeKey = false;
	m_forceSearchPage = true;
}

/**
 * �����Ҽ��ĸ�ʽ�Ƿ�Ϊѹ����ʽ��������ת��Ϊѹ����ʽ
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
 * ���캯��
 * @param indice		��Ӧ��indice�����
 * @param tableDef		��Ӧ�ı���
 * @param indexDef		�������壬��Ҫ���ر���
 * @param indexId		�������������ļ����е�ID
 * @param rootPageId	��ҳ��ID
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
 * ��������
 */
DrsBPTreeIndex::~DrsBPTreeIndex() {
	ftrace(ts.idx, );

	delete m_dboStats;
	delete m_indexDef;
}



/**
 * ���ĳ��������ID��
 * @return ����ID
 */
u8 DrsBPTreeIndex::getIndexId() {
	return m_indexId;
}


/**
 * �õ���������
 * @return ��������
 */
const IndexDef* DrsBPTreeIndex::getIndexDef() {
	return m_indexDef;
}

/**
 * ����������ֵ��[min,max]֮��ļ�¼��
 * @pre min��maxӦ����������˳���С�����������ֵ
 *
 * @param session �Ự
 * @param minKey ���ޣ���ΪNULL���ʾ��ָ�����ޣ�����һ����PAD��ʽ�ü�ֵ
 * @param includeKeyMin	����ģʽ��true��>=/false��>
 * @param maxKey ���ޣ���ΪNULL���ʾ��ָ�����ޣ�����һ����PAD��ʽ�ü�ֵ
 * @param includeKeyMax	����ģʽ��true��<=/false��<
 * @return ��[min,max]֮��ļ�¼���Ĺ���ֵ
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

	// ����С��ֵ��Ϣ
	estimateKeySearch(minKey, scanInfo, &minFlag, &singleroot, &minFetchNext, &minLeafKeyCount, &minLeafPageCount);
	memcpy(traceMin, scanInfo->m_traceInfo, sizeof(struct IndexSearchTraceInfo) * MAX_INDEX_LEVEL);
	minLeafPageId = scanInfo->m_pageId;

	memset(scanInfo->m_traceInfo, 0, sizeof(struct IndexSearchTraceInfo) * MAX_INDEX_LEVEL);
	scanInfo->m_pageId = INVALID_PAGE_ID;

	// ���������ֵ��Ϣ
	estimateKeySearch(maxKey, scanInfo, &maxFlag, &singleroot, &maxFetchNext, &maxLeafKeyCount, &maxLeafPageCount);
	memcpy(traceMax, scanInfo->m_traceInfo, sizeof(struct IndexSearchTraceInfo) * MAX_INDEX_LEVEL);
	maxLeafPageId = scanInfo->m_pageId;

	// �����ֵ��
	s16 level = MAX_INDEX_LEVEL;
	u64 rows = 1;
	bool diverged = false;    //��ʶ�Ƿ�ֿ��ڲ�ͬҳ����
	bool diverged_lot = false;	//��ʶ�Ƿ�ֿ��ڲ�ͬҳ�棬���Ҳ�������ҳ����
	while (true) {
		--level;
		if (level < 0)
			break;

		bool minStart = (traceMax[level].m_pageId != 0), maxStart = (traceMin[level].m_pageId != 0);
		if (minStart ^ maxStart)
			// ��ʾ�������������������Ѿ������仯�����ع���ֵ
			return 10;
		else if (!minStart)
			continue;

		if (traceMin[level].m_pageId == traceMax[level].m_pageId) {
			if (traceMin[level].pi.ei.m_keyCount != traceMax[level].pi.ei.m_keyCount) {	// ��ǰ�㿪ʼ�ֿ���
				if (traceMin[level].pi.ei.m_keyCount < traceMax[level].pi.ei.m_keyCount) {	// ��ȷ���
					diverged = true;
					rows = max(traceMax[level].pi.ei.m_keyCount - traceMin[level].pi.ei.m_keyCount, 1);
					if(rows > 1) {
						diverged_lot = true;
					}
				} else {	// ���ζ�λ֮����ܳ������������޸ģ���λ��׼ȷ�����ع���ֵ
					return 10;
				}
			}
			continue;
		} else if (diverged && !diverged_lot){	// ��ʾ·���Ѿ���ͬ�������������ڵ�ҳ����
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
		} else if (diverged_lot) {		//·����ͬ���Ҳ���������ҳ����
			rows *= (traceMin[level].pi.ei.m_pageCount + traceMax[level].pi.ei.m_pageCount) / 2;
		}
	}

	// ����Ҷ�ڵ�һ��
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

	// ����0����������Innodb���㵱���ἰ�����⣺
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
 * �������ڲ���ʱɨ��ľ����Ϣ
 * @param session
 * @param key
 * @return 
 */
DrsIndexScanHandleInfo* DrsBPTreeIndex::prepareInsertScanInfo(Session *session, const SubRecord *key) {
	assert(KEY_COMPRESS == key->m_format || KEY_PAD == key->m_format);
	//����һ��ɨ����Ϣ���
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
	if (!scanInfo->m_isFastComparable) {	// ���޷����ٱȽϣ���Ҫ����ѹ����ֵ����������ʹ��
		assert(key->m_format == KEY_PAD);
		scanInfo->m_key0 = IndexKey::convertKeyPC(memoryContext, key, m_tableDef, m_indexDef);
	}
	return scanInfo;
}


/**
 * ����һ��������
 *
 * @param session �Ự���
 * @param key ����������ʽһ����REC_REDUNDANT����KEY_COMPRESS��
 * @param duplicateKey out ���ص�ǰ�����Ƿ�����ΨһԼ��Υ�������Υ�����¼�ֵ���ᱻ����
 * @param checkDuplicate	ָ���Ƿ���Ҫ���Ψһ�ԣ��ڻ��˲�����ʱ����Ҫ����Ψһ����֤��Ӧ�ÿ϶����Ի��ˣ�Ĭ�ϱ���Ҫ���
 * @return true��ʾ����ɹ������Ҳ�Υ��Ψһ��Լ����false��ʾ���벻�ɹ�����������Υ��Ψһ��Լ��������������������
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
 * ����Ψһ�������Ƿ�����ظ���ֵ
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
	// ��λҪ�����ҳ�沢��ҳ�����
	u64 token = session->getToken();
	scanInfo->m_pageHandle = findSpecifiedLevel(scanInfo, &SearchFlag::DEFAULT_FLAG, 0, IDX_SEARCH);
	if (!scanInfo->m_pageHandle) {// �����ʾ��SMO����������
		goto _dealWithDeadLock;
	}

	SYNCHERE(SP_IDX_RESEARCH_PARENT_IN_SMO);

	IDX_SWITCH_AND_GOTO(lockIdxObjectHoldingLatch(session, scanInfo->m_pageId, &(scanInfo->m_pageHandle), 
		FOR_INSERT, &DrsBPTreeIndex::judgerForDMLLocatePage, (void*)scanInfo, NULL), _restart, _dealWithDeadLock);

	page = (IndexPage*)scanInfo->m_pageHandle->getPage();

	// ���Ҳ����ֵ��ҳ���е�λ��
	scanInfo->m_keyInfo.m_sOffset = 0;
	scanInfo->m_cKey1->m_size = 0;
	scanInfo->m_cKey2->m_size = 0;
	page->findKeyInPageTwo(scanInfo->m_findKey, scanInfo->m_comparator, &(scanInfo->m_cKey1), &(scanInfo->m_cKey2), 
		&(scanInfo->m_keyInfo));

	// ���Ψһ��Լ���Ƿ��г�ͻ
	IDX_SWITCH_AND_GOTO(insertCheckSame(scanInfo, &duplicateKey), _restart, _dealWithDeadLock);

	if (!duplicateKey && NULL != info) {
		//�������ظ���ֵ�����浱ǰLSN���ͷ�ҳ��Latch������pin
		scanInfo->m_pageLSN = scanInfo->m_pageHandle->getPage()->m_lsn;
		scanInfo->m_pageSMOLSN = page->m_smoLSN;		
		session->unlockPage(&scanInfo->m_pageHandle);
		assert(scanInfo->m_pageHandle->isPinned());
		assert(None == scanInfo->m_pageHandle->getLockMode());
		*info = scanInfo;
	} else {
		//�����ظ���ֵ���ͷ�ҳ��Latch�Լ�pin
		session->releasePage(&scanInfo->m_pageHandle);
	}
	return duplicateKey;

_restart:
	idxUnlockObject(session, scanInfo->m_pageId, token);// �����Ļ�����Ҫ�ͷ��Ѿ����е�ҳ����
	session->getMemoryContext()->resetToSavepoint(savePoint);
	goto _findLeafLevel;

_dealWithDeadLock:
	session->unlockIdxObjects(token);//����ʱ��Ҫ�ͷ����е�������
	session->getMemoryContext()->resetToSavepoint(savePoint);
	statisticDL();
	goto _findLeafLevel;
}

#endif

/**
 * ���Ѿ���λ����Ҷҳ���ϲ����ֵ
 * @param info ɨ������Ϣ
 * @return true һ���ɹ�
 */
bool DrsBPTreeIndex::insertGotPage(DrsIndexScanHandleInfo *scanInfo) {
	Session *session = scanInfo->m_session;
	u64 token = session->getToken();

	assert(NULL != scanInfo->m_pageHandle);
	assert(scanInfo->m_pageHandle->isPinned());//һ������pin�����ǲ�����ҳ��latch

	McSavepoint mcs(session->getMemoryContext());
	BufferPageHandle *pageHandle = scanInfo->m_pageHandle;
	LOCK_PAGE_HANDLE(session, pageHandle, scanInfo->m_latchMode);
	IndexPage *page = (IndexPage*)pageHandle->getPage();

	//�ж�ҳ��LSN, ���LSNû�з����仯�������ֱ����ҳ���в����ֵ
	if (page->m_lsn == scanInfo->m_pageLSN) {
		// ����Ҫ��֤��ҳ�滹�Ǳ�������Ҷҳ�棬����ҳ����Ч������ҳ�治����SMO״̬
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
					// ��������п����޸���ҳ��(����MiniPage������������MiniPage�ɹ������Ǻ����������ֿռ䲻��)
					// ��������Ҫ��ǵ�ǰҳ��Ϊ��
					session->markDirty(scanInfo->m_pageHandle);
				}
				if (!insertSMOPrelockPages(session, &scanInfo->m_pageHandle)) {
					//������������Ҫ�ͷ����е�������
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
	//�Ӹ��ڵ㿪ʼ���Ҳ���λ��
	loopInsertIfDeadLock(scanInfo);

	return true;
}

/**
 * �����ֵ������Ҫ����Ƿ��м�ֵ�ظ�
 * @param session �Ự
 * @param key ������ļ�ֵ
 */
void DrsBPTreeIndex::insertNoCheckDuplicate(Session *session, const SubRecord *key) {
	DrsIndexScanHandleInfo *scanInfo = prepareInsertScanInfo(session, key);	
	loopInsertIfDeadLock(scanInfo);
}

/**
 * �����ֵ���������������
 * @param scanInfo
 */
void DrsBPTreeIndex::loopInsertIfDeadLock(DrsIndexScanHandleInfo *scanInfo) {
	assert(scanInfo && scanInfo->m_findKey);

	Session *session = scanInfo->m_session;
	bool duplicateKey = false;
	u64 savePoint = session->getMemoryContext()->setSavepoint();
	u64 token = session->getToken();

	while (!insertIndexEntry(scanInfo, &duplicateKey, true)) {
		//��������
		NTSE_ASSERT(!duplicateKey);
		session->unlockIdxObjects(token);//��Ҫ�ͷ����е�������
		statisticDL();
		session->getMemoryContext()->resetToSavepoint(savePoint);
		token = session->getToken();
	}
	session->getMemoryContext()->resetToSavepoint(savePoint);
}


/**
 * ɾ��һ��������
 *
 * @pre	���øú���ǰӦ��֤Ҫɾ���ļ�ֵȷʵ�������������У���������Ҳ�����˵�������ṹ����
 * @param session �Ự���
 * @param key ����������ʽһ����REC_REDUNDANT����KEY_COMPRESS��
 */
bool DrsBPTreeIndex::del(Session *session, const SubRecord *key) {
	assert(key != NULL);
	assert(key->m_format == KEY_PAD || key->m_format == KEY_COMPRESS);

	ftrace(ts.idx, tout << session << key << rid(key->m_rowId););

	MemoryContext *memoryContext = session->getMemoryContext();
	u64 savePoint = memoryContext->setSavepoint();
	// ��������һ��ɨ����Ϣ���
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
 * ����Ψһ��������������
 *
 * @param session		�Ự���
 * @param key			��������ʽһ����KEY_PAD�ģ����ڵ���ʹ�ü�ֵ�޷�Ψһ���ֵ���������Ҫ��־m_rowIdΪINVALID_ROW_ID
 * @param lockMode		�Է��صļ�¼Ҫ�ӵ���
 * @param rowId			OUT	���м�¼ʱ���ؼ�¼RID
 * @param subRecord		IN/OUT	����ָ��Ҫ��ȡ�����ԣ����Ϊ��ȡ���������ݣ�����ΪNULL����ʱֻ���ؼ�¼RID
 * @param rlh			IN/OUT	���LockMode=None��ֵΪNULL�������ֵҪ���ؼ����ľ��
 * @param extractor		����ΪNULL���ô��Ӽ�¼��ȡ����ȡҪ��ȡ�����Դ洢��subRecord��ΪNULL��ʾֻ����RID
 * @return �ҵ���¼ʱ����true���Ҳ�������false
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
	// ��������һ��ɨ����Ϣ���
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

	// ɨ��ɹ����޸�ͳ����Ϣ
	m_indexStatus.m_dboStats->countIt(DBOBJ_SCAN_ITEM);

	return true;
}


/**
 * ��ʼ����ɨ��
 *
 * @param session		�Ự���
 * @param key			������������ΪNULL����ΪNULL������forwardΪtrueʱӦ��λ��������һ�forwardΪfalseʱ��λ�����һ�����ΪNULL����ʽһ����KEY_PAD��
 * @param forward		�Ƿ�Ϊǰ������
 * @param includeKey	�Ƿ����key����>=/<=����ʱΪtrue��>/<����ʱΪfalse��
 * @param lockMode		�Է��صļ�¼Ҫ�ӵ���
 * @param rlh			IN/OUT	���LockMode=None��ֵΪNULL�������ֵҪ���ؼ����ľ��
 * @param extractor		����ΪNULL����getNextʱ�ô��Ӽ�¼��ȡ����ȡҪ��ȡ�����Դ洢��subRecord��ΪNULL��ʾֻ����RID
 * @return	����õ�ɨ����
 */
IndexScanHandle* DrsBPTreeIndex::beginScan(Session *session, const SubRecord *key, bool forward, bool includeKey, LockMode lockMode, RowLockHandle **rlh, SubToSubExtractor *extractor) {
	PROFILE(PI_DrsIndex_beginScan);

	assert(!((lockMode != None) ^ (rlh != NULL)));
	assert(key == NULL || key->m_format == KEY_PAD);

	// ����ͳ����Ϣ
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
		scanInfo->m_findKey->m_size = 0;	// ��ʾû�в��Ҽ�ֵ
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
 * �õ���һ�������������forwardΪfalseʱʵ����ǰһ�
 *
 * @param scanHandle	ɨ����
 * @param key			out ����������ҵ���ֵ��SubRecord�����ΪNULL����ʾֻ��Ҫȡ��RowId
 * @return �Ƿ�����һ�����
 */
bool DrsBPTreeIndex::getNext(IndexScanHandle *scanHandle, SubRecord *key) {
	PROFILE(PI_DrsIndex_getNext);

	assert(scanHandle != NULL);

	DrsIndexRangeScanHandle *rangeHandle = (DrsIndexRangeScanHandle*)scanHandle;
	rangeHandle->setSubRecord(key);
	DrsIndexScanHandleInfo *scanInfo = rangeHandle->getScanInfo();
	assert(scanInfo->m_hasNext);	// �ö�����Ŀǰ����������Ժ����������α귴��ʹ�ã�����ʹ���������

	if (!fetchNext(scanInfo))
		return false;

	scanInfo->m_rangeFirst = false;
	assert(rangeHandle->getScanInfo()->m_lockMode == None || rangeHandle->getScanInfo()->m_rowHandle != NULL);
	rangeHandle->setRowId(scanInfo->m_findKey->m_rowId);
	rangeHandle->saveKey(scanInfo->m_findKey);

	// ɨ��ɹ����޸�ͳ����Ϣ
	m_indexStatus.m_dboStats->countIt(DBOBJ_SCAN_ITEM);
	if (!scanInfo->m_forward)
		++m_indexStatus.m_rowsBScanned;

	return true;
}

/**
 * ɾ����ǰɨ��ļ�¼
 *
 * @param scanHandle ɨ����
 * @return true��ʾɾ���ɹ���false��ʾ��������
 */
bool DrsBPTreeIndex::deleteCurrent(IndexScanHandle *scanHandle) {
	PROFILE(PI_DrsIndex_deleteCurrent);

	DrsIndexScanHandleInfo *scanInfo = ((DrsIndexRangeScanHandle*)scanHandle)->getScanInfo();
	if (scanInfo->m_pageHandle == NULL) {	// ���ﲻ�ܱ�֤һ���Ѿ�����ҳ��ľ��
		researchForDelete(scanInfo);
		return deleteIndexEntry(scanInfo, true);
	}

	return deleteIndexEntry(scanInfo, false);
}


/**
 * ��������ɨ��
 *
 * @param scanHandle ɨ����
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
 * ��ʼ����ɨ��(�Ľ��汾)
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

	// ����ͳ����Ϣ
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
		scanInfo->m_findKey->m_size = 0;	// ��ʾû�в��Ҽ�ֵ
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
 * ��ȡ��һ��(�Ľ��汾)
 * @param scanHandle ��Χɨ����
 * @return �Ƿ������һ��
 */
bool DrsBPTreeIndex::getNextSecond(IndexScanHandle *scanHandle) throw(NtseException) {
	PROFILE(PI_DrsIndex_getNext);

	assert(scanHandle != NULL);

	DrsIndexRangeScanHandle *rangeHandle = (DrsIndexRangeScanHandle*)scanHandle;
	DrsIndexScanHandleInfoExt *scanInfo = (DrsIndexScanHandleInfoExt*)rangeHandle->getScanInfo();
	assert(scanInfo->m_hasNext);	// �ö�����Ŀǰ����������Ժ����������α귴��ʹ�ã�����ʹ���������

	if (!fetchNextSecond(scanInfo))
		return false;

	scanInfo->m_rangeFirst = false;
	assert(rangeHandle->getScanInfo()->m_lockMode == None || rangeHandle->getScanInfo()->m_rowHandle != NULL);
	rangeHandle->setRowId(scanInfo->m_readKey->m_rowId);
	rangeHandle->saveKey(scanInfo->m_readKey);

	// ɨ��ɹ����޸�ͳ����Ϣ
	m_indexStatus.m_dboStats->countIt(DBOBJ_SCAN_ITEM);
	if (!scanInfo->m_forward)
		++m_indexStatus.m_rowsBScanned;

	return true;
}

/**
 * ������Χɨ��
 * @param scanHandle ��Χɨ����
 */
void DrsBPTreeIndex::endScanSecond(IndexScanHandle *scanHandle) {
	PROFILE(PI_DrsIndex_endScan);

	DrsIndexRangeScanHandle *handle = (DrsIndexRangeScanHandle*)scanHandle;
	DrsIndexScanHandleInfoExt *scanInfo = (DrsIndexScanHandleInfoExt*)handle->getScanInfo();
	/*
	// Uniqueɨ���ʱ��������һ��getNext���ÿ�ҳ�����ҿ�ҳ��ĵ�һ���������
	// ��ʱendScanʱ��������ҳ���pin������Ҫ�ͷ�
	*/
	if(scanInfo->m_pageHandle != NULL && scanInfo->m_pageHandle != scanInfo->m_readPageHandle) {
		scanInfo->m_session->unpinPage(&(scanInfo->m_pageHandle));
	}

	if (scanInfo->m_readPageHandle != NULL) {
		scanInfo->m_session->unpinPage(&(scanInfo->m_readPageHandle));
	}
}

/**
 * �Ľ��Ĳ�����һ�������ʵ�֣�ֻ��ȡ���Ҳ������α�
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

	// ���浱ǰLSN
	info->m_forceSearchPage = false;
	info->m_readPageLSN = info->m_readPageHandle->getPage()->m_lsn;
	info->m_readPageSMOLSN = ((IndexPage*)info->m_readPageHandle->getPage())->m_smoLSN;

	if (needFetchNext) {
		IDX_SWITCH_AND_GOTO(readNextKey(info), FetchRestart, FetchFail);
	} else {
		// �ڳ���ҳ��latch������£���NTSE����
		IDXResult ret = tryTrxLockHoldingLatch(info,  &info->m_readPageHandle, info->m_cKey1);
		if(ret == IDX_RESTART) {
			//��������������α�ҳ����NULL����ʱreadҳ���ѱ��ͷţ������α�ҳ���readҳ����ͬ��Ҳ�Ѿ����ͷ�
			info->m_pageHandle = NULL;
			info->m_readPageHandle = NULL;
			goto FetchRestart;
		} else if(ret == IDX_FAIL)
			goto FetchFail;

		//�ͷ�ҳ��Latch���سɹ�
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
 * ��ȡ��ǰ��λ�������������һ��
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
		info->m_cKey1);	// ���findKey��ʽ����ѹ����,˵�����ܾ����ض�λ�����ǵ�һ�β���,���������ʱ����ܵ�ǰһ����ֵ��ֻ����m_cKey1
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
		// �ڳ���ҳ��latch������£���NTSE����
		IDX_SWITCH_AND_GOTO(tryTrxLockHoldingLatch(info, &info->m_readPageHandle, key), 
			SpanPageRestart, SpanPageFail);

	} else {
		// ��Ҫ��ҳ�����
		if (nextPageId == INVALID_PAGE_ID) {	// ɨ�赽�������߽�
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
		//���ڿ�ҳ�棬��Ȼ����Ӱ�쵽�α�ҳ�棬���Դ˴�restart����Ҫ�����α�ҳ��
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
		assert(IndexKey::isKeyValid(key));	// һ����ȡ��������

		try {
			// �ڳ���ҳ��latch������£���NTSE�����Լ�TNT������
			IDX_SWITCH_AND_GOTO(tryTrxLockHoldingTwoLatch(info, &nextHandle, &handle, key), SpanPageRestart, SpanPageFail);
		} catch (NtseException &e) {
		//	session->releasePage(&handle);
			throw e;
		}

		info->m_readPageId = nextPageId;
		info->m_readPageHandle = nextHandle;
		info->m_readPageLSN = nextHandle->getPage()->m_lsn;
		info->m_readPageSMOLSN = ((IndexPage *)nextHandle->getPage())->m_smoLSN;

		// �ͷ���һҳ���Latch, ���ͷ�Pin
		session->unlockPage(&nextHandle);
	}

	if (!spanPage)
		info->m_session->unlockPage(&handle);
	else {
		if(info->m_pageId != handle->getPageId()) {
			// �����ҳǰ��ҳ�治���α�ҳ����ֻ�ܲŴ˴��ͷ�LATCH��PIN
			info->m_session->releasePage(&handle);
		} else {
			// �����α�ҳ��realease�������ܱ�д�������¶��룬���Ҫ�����α�ҳָ��
			info->m_pageHandle = handle;
			info->m_session->unlockPage(&handle);
		}	
	}
	
	return IDX_SUCCESS;

SpanPageRestart:
	//�˴���Ҫ�ŵ����е�latch��pin
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
 * �ڳ���ҳ��latch������£����Լ���������
 *
 * @pre Ҫ���������ļ�¼���ڵ�����ҳ���latch�Ѿ�����
 * @param info �������ɨ������Ϣ
 * @param pageHdl Ҫ���������ļ�¼���ڵ�����ҳ��
 * @param key Ҫ���������ļ�¼
 * @throw NtseException ����ռ䲻���
 * @return ���׶μ����ɹ�����IDX_SUCCESS����ʱ����ҳ��latch��NTSE�����Լ���������
 *         ����ʧ�ܷ���IDX_RESTART�����ϲ������������ҹ��̣���ʱ�Գ���ҳ��latch
 */
IDXResult DrsBPTreeIndex::tryTrxLockHoldingLatch(DrsIndexScanHandleInfoExt *info, PageHandle **pageHdl,
												 const SubRecord *key) throw(NtseException) {
	Session *session = info->m_session;
	RowId keyRowId = key->m_rowId;// ���е������һ��ļ�ֵ�϶�������recordָ���m_cKey1����

	if (info->m_lockMode != None) {
		// ���NTSE�����Ƿ��Ѿ�����
		assert(info->m_lockMode == Shared);
		if (!session->isRowLocked(m_tableDef->m_id, keyRowId, info->m_lockMode)) {
			return lockHoldingLatch(session, keyRowId, info->m_lockMode, 
				pageHdl, info->m_rowHandle, true, info);
		}
	}
	return IDX_SUCCESS;	
}




/**
 * ��lockHoldingLatch�����ɹ�������ҳ��LSN�ı��ʱ�򣬿����øú�����ȷ��������ֵ�ǲ��ǻ��ڵ�ǰҳ��,(����TNT)
 * @pre info�ṹ����idxKey1�Ǳ��μ����ļ�ֵ����m_findKey�Ǳ������Ҫ��λ�ļ�ֵ��m_pageHandle�Ǳ�ʾ��������ҳ����Ϣ���
 * @post idxKey1����ᱣ�����������ļ�ֵ����Ϣ���������false��info�����keyInfo��Ϣ�����ϣ�����keyInfo��ʾ��ǰ�µ��ҵ���ֵ����Ϣ
 * @param info		ɨ����Ϣ���
 * @param inRange	out Ҫ���ҵļ�ֵ�Ƿ��ڵ�ǰҳ�淶Χ��
 * @return true��ʾҪ�����ļ�ֵ���ڵ�ǰҳ�棬false��ʾ����
 */
bool DrsBPTreeIndex::researchScanKeyInPageSecond(DrsIndexScanHandleInfo *info, bool *inRange) {
	// ���ȱ�֤ҳ����Ч�ԣ���Ч���߿�ҳ�棬�϶��Ҳ���
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

	// Ψһ��ѯ������Ȳ�����Ч��������ܸպ��Ǳ������̸߳��£�����λ�ú�rowIdû�з����ı�
	if (infoExt->m_uniqueScan)
		return (*inRange = (keyInfo->m_result == 0 && gotKey->m_rowId == origRowId));

	// ���ݲ��ҽ���ж��Ƿ�Ӧ��ȡ���ҵ���һ��
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

	// ��Χ��ѯ����λ�����ҷ�Χ��Ȼ����ݽ������֮�����rowIdһ�£�˵�����ڶ�λ��������Ч��ȷ�ģ�
	// ���rowId�޷�һ�¿϶���׼ȷ��������ԭ����λ���ɾ�����߸��£����߷�Χ֮���ֲ��������
	// ��֮��ʱ�������²���
	*inRange = page->isKeyInPageMiddle(keyInfo);
	return (*inRange && IndexKey::isKeyValid(gotKey) && gotKey->m_rowId == origRowId);
}



/**
 * ����������ֵ��[min,max]֮��ļ�¼��
 * @pre min��maxӦ����������˳���С�����������ֵ
 *
 * @param session �Ự
 * @param minKey ���ޣ���ΪNULL���ʾ��ָ�����ޣ�����һ����PAD��ʽ�ü�ֵ
 * @param includeKeyMin	����ģʽ��true��>=/false��>
 * @param maxKey ���ޣ���ΪNULL���ʾ��ָ�����ޣ�����һ����PAD��ʽ�ü�ֵ
 * @param includeKeyMax	����ģʽ��true��<=/false��<
 * @return ��[min,max]֮��ļ�¼���Ĺ���ֵ
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

	// ����С��ֵ��Ϣ
	estimateKeySearch(minKey, scanInfo, &minFlag, &singleroot, &minFetchNext, &minLeafKeyCount, &minLeafPageCount);
	memcpy(traceMin, scanInfo->m_traceInfo, sizeof(struct IndexSearchTraceInfo) * MAX_INDEX_LEVEL);
	minLeafPageId = scanInfo->m_pageId;

	memset(scanInfo->m_traceInfo, 0, sizeof(struct IndexSearchTraceInfo) * MAX_INDEX_LEVEL);
	scanInfo->m_pageId = INVALID_PAGE_ID;

	// ���������ֵ��Ϣ
	estimateKeySearch(maxKey, scanInfo, &maxFlag, &singleroot, &maxFetchNext, &maxLeafKeyCount, &maxLeafPageCount);
	memcpy(traceMax, scanInfo->m_traceInfo, sizeof(struct IndexSearchTraceInfo) * MAX_INDEX_LEVEL);
	maxLeafPageId = scanInfo->m_pageId;

	// �����ֵ��
	s16 level = MAX_INDEX_LEVEL;
	u64 rows = 1;
	bool diverged = false;    //��ʶ�Ƿ�ֿ��ڲ�ͬҳ����
	bool diverged_lot = false;	//��ʶ�Ƿ�ֿ��ڲ�ͬҳ�棬���Ҳ�������ҳ����
	while (true) {
		--level;
		if (level < 0)
			break;

		bool minStart = (traceMax[level].m_pageId != 0), maxStart = (traceMin[level].m_pageId != 0);
		if (minStart ^ maxStart)
			// ��ʾ�������������������Ѿ������仯�����ع���ֵ
			return 10;
		else if (!minStart)
			continue;

		if (traceMin[level].m_pageId == traceMax[level].m_pageId) {
			if (traceMin[level].pi.ei.m_keyCount != traceMax[level].pi.ei.m_keyCount) {	// ��ǰ�㿪ʼ�ֿ���
				if (traceMin[level].pi.ei.m_keyCount < traceMax[level].pi.ei.m_keyCount) {	// ��ȷ���
					diverged = true;
					rows = max(traceMax[level].pi.ei.m_keyCount - traceMin[level].pi.ei.m_keyCount, 1);
					if(rows > 1) {
						diverged_lot = true;
					}
				} else {	// ���ζ�λ֮����ܳ������������޸ģ���λ��׼ȷ�����ع���ֵ
					return 10;
				}
			}
			continue;
		} else if (diverged && !diverged_lot){	// ��ʾ·���Ѿ���ͬ�������������ڵ�ҳ����
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
		} else if (diverged_lot) {		//·����ͬ���Ҳ���������ҳ����
			rows *= (traceMin[level].pi.ei.m_pageCount + traceMax[level].pi.ei.m_pageCount) / 2;
		}
	}

	// ����Ҷ�ڵ�һ��
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

	// ����0����������Innodb���㵱���ἰ�����⣺
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
 * �ڳ���ҳ��latch������£����Լ���������
 *
 * @pre Ҫ���������ļ�¼���ڵ�����ҳ���latch�Ѿ�����
 * @param info �������ɨ������Ϣ
 * @param pageHdl Ҫ���������ļ�¼���ڵ�����ҳ��
 * @param pageHdl2 �α�����ǰҳ��
 * @param key Ҫ���������ļ�¼
 * @throw NtseException ����ռ䲻���
 * @return ���׶μ����ɹ�����IDX_SUCCESS����ʱ����ҳ��latch��NTSE�����Լ���������
 *         ����ʧ�ܷ���IDX_RESTART�����ϲ������������ҹ��̣���ʱ�Գ���ҳ��latch
 */
IDXResult DrsBPTreeIndex::tryTrxLockHoldingTwoLatch(DrsIndexScanHandleInfoExt *info, PageHandle **pageHdl, PageHandle **pageHdl2,
												 const SubRecord *key) throw(NtseException) {
	Session *session = info->m_session;
	RowId keyRowId = key->m_rowId;// ���е������һ��ļ�ֵ�϶�������recordָ���m_cKey1����

	if (info->m_lockMode != None) {
		// ���NTSE�����Ƿ��Ѿ�����
		assert(info->m_lockMode == Shared);
		if (!session->isRowLocked(m_tableDef->m_id, keyRowId, info->m_lockMode)) {
			return lockHoldingTwoLatch(session, keyRowId, info->m_lockMode, 
				pageHdl, pageHdl2, info->m_rowHandle, true, info);
		}
	}
	return IDX_SUCCESS;	
}


/**
 * �ڳ�������ҳ��Latch������£���ҳ��ĳ����ֵ����������������ɹ����ȳ�����Ҳ����Latch����ҳ��ʱ��ʹ�ã�
 *
 * @param session			�Ự���
 * @param rowId				Ҫ������ֵ��rowId
 * @param lockMode			����ģʽ
 * @param pageHandle		INOUT	ҳ����
 * @param pageHdl2			INOUT   �α�����ǰҳ��
 * @param rowHandle			OUT		�������
 * @param mustReleaseLatch	������restart��ʱ���ǲ��ǿ��Բ���ҳ���pin
 * @param info				ɨ������������ҳ��LSN�ı��ʱ���жϲ������ǲ�����Ȼ�ڸ�ҳ�棬Ĭ��ֵΪNULL�����ΪNULL���Ͳ��������ж�
 * @return ���سɹ�IDX_SUCCESS��ʧ��IDX_FAIL����Ҫ���¿�ʼIDX_RESTART
 */
IDXResult DrsBPTreeIndex::lockHoldingTwoLatch(Session *session, RowId rowId, LockMode lockMode, PageHandle **pageHandle, PageHandle **pageHandle2, RowLockHandle **rowHandle, bool mustReleaseLatch, DrsIndexScanHandleInfo *info) {
	UNREFERENCED_PARAMETER(info);
	UNREFERENCED_PARAMETER(mustReleaseLatch);

	assert(lockMode != None && rowHandle != NULL);

	u16 tableId = ((DrsBPTreeIndice*)m_indice)->getTableId();
	if ((*rowHandle = TRY_LOCK_ROW(session, tableId, rowId, lockMode)) != NULL)
		return IDX_SUCCESS;

	// ������Ҫ���ͷű�ҳ���Latch������������

	//�����tnt��������Σ�ֻҪ��Ҫ����ɨ�裬�ͱ����ͷ����ҳ��,
	session->releasePage(pageHandle);
	session->releasePage(pageHandle2);

	*rowHandle = LOCK_ROW(session, tableId, rowId, lockMode);

	++m_indexStatus.m_numRLRestarts;	// ͳ����Ϣ

	session->unlockRow(rowHandle);

		
	return IDX_RESTART;
}

#endif

/////////////////////////////////////////////////////////////////////////////////////
// Search

/**
 * ȡɨ�����һ��
 * @param info	ɨ����Ϣ
 * @return �Ƿ񻹴�����һ��
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

		if (!info->m_hasNext) {	// ��������һ��
			goto FetchFail;
		}
	}

	// ���е������һ��ļ�ֵ�϶�������recordָ���m_idxKey1����
	if (info->m_lockMode != None) {
		IDX_SWITCH_AND_GOTO(lockHoldingLatch(session, info->m_cKey1->m_rowId, info->m_lockMode, 
			&info->m_pageHandle, info->m_rowHandle, false, info), FetchStart, FetchFail);
		if (info->m_lockMode == Exclusived) {
			nftrace(ts.idx, tout << info->m_session->getId() << " Lock " << 
				rid(info->m_cKey1->m_rowId) << " for dml in page: " << info->m_pageId;);
		}
	}

	// ���浱ǰLSN���ͷ�ҳ��Latch���سɹ�
	info->m_pageLSN = info->m_pageHandle->getPage()->m_lsn;
	info->m_pageSMOLSN = ((IndexPage*)info->m_pageHandle->getPage())->m_smoLSN;
	session->unlockPage(&(info->m_pageHandle));

	// ����Ҫ�ж�����¼�ĵ�������֤���˵�һ�εĲ���֮�⣬���в���ʹ�õ�SubRecord������������������ֵ
	if (info->m_findKey->m_format == KEY_PAD) {
		info->m_findKey->m_columns = info->m_key0->m_columns;
		info->m_findKey->m_numCols = info->m_key0->m_numCols;
		assert(info->m_key0 != NULL && info->m_key0->m_format == KEY_COMPRESS);
		IndexKey::swapKey(&(info->m_findKey), &(info->m_key0));
		if (info->m_isFastComparable)
			info->m_comparator->setComparator(RecordOper::compareKeyCC);
	}

	// �滻����SubRecord��ʼ�ձ����ҵ��Ľ���洢��findKey
	IndexKey::swapKey(&(info->m_findKey), &(info->m_cKey1));

	return true;

FetchFail:
	session->unlockPage(&(info->m_pageHandle));
	return false;
}



/**
 * ����ɨ����Ϣ��ȡ��ָ����
 * @pre �ȽϺ����Ͳ��Ҽ�ֵ��ʽ�Ѿ�ȷ��
 * @post �������κ�ҳ����Դ
 *
 * @param info			ɨ����Ϣ���
 * @param scanCallBack	ɨ��ص�������������ڣ�˵����ǰΪɾ������
 * @param cbResult		out �ص������ķ���ֵ
 * @return �Ƿ�ȡ��ָ����
 * @attention ���ڲ���Ҫ���к���ȡ��������ú�������֮�󲻳����κ�ҳ����Դ
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

	if (result == 1 && info->m_everEqual && info->m_findKey->m_rowId == INVALID_ROW_ID) {	// ��ʱ����ܲ��ҵļ�ֵ����һ��Ҷҳ��
		// ���ȡ��ɹ����Ƚ��¼�ֵ�Ͳ��Ҽ�ֵ�Ĵ�С
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

	// �滻����SubRecord��ʼ�ձ����ҵ��Ľ���洢��findKey
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
 * �����һ��ҳ����ڸ���ָ����һ��ҳ���ǰ��ָ��
 * @pre ʹ����Ӧ�ñ�֤��һ��ҳ�����ֱ�Ӽ���
 * @param session			�Ự���
 * @param file				�����ļ����
 * @param logger			��־��¼��
 * @param nextPageId		��һ��ҳ��ID
 * @param prevLinkPageId	Ҫ���µ�ǰ��ָ��
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
 * ���ǰһ��ҳ����ڸ���ָ��ǰһ��ҳ��ĺ���ָ��
 * @pre ʹ����Ӧ�ñ�֤��һ��ҳ�����ֱ�Ӽ���
 * @param session			�Ự���
 * @param logger			��־��¼��
 * @param file				�����ļ����
 * @param nextPageId		ǰһ��ҳ��ID
 * @param prevLinkPageId	Ҫ���µĺ���ָ��
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
 * ��ɾ�������У����ڶ�λҳ���޸Ķ�ɾ���������ض�λ����
 * @post �¶�λ��ɾ������Ϣ����info�ṹ���б���
 * @param info		ɨ����Ϣ���
 * return ��λ����page��Ϣ
 */
IndexPage* DrsBPTreeIndex::researchForDelete(DrsIndexScanHandleInfo *info) {
	bool needFetchNext;
	s32 result;
	// ���ȴ����ú��ּ�ֵ�����Լ����ñȽϺ���
	NTSE_ASSERT(locateLeafPageAndFindKey(info, &needFetchNext, &result));	// ��Ӧ��������
	assert(result == 0);
	IndexPage *page = (IndexPage*)info->m_pageHandle->getPage();
	assert(page->m_pageCount != 0);
	// ��֤Ҫɾ��������m_findKey����
	saveFoundKeyToFindKey(info);

	assert(info->m_findKey->m_format == KEY_COMPRESS);

	return page;
}


/**
 * ����ɨ����Ϣɾ����ǰ��λ�����ǰҳ��Latch�����������Ҫ�����ʾ�����û��Latch��Ĭ�ϳ���ҳ���Pin
 * Ҫɾ���������info->m_findKey���У���ʽ��KEY_COMPRESS����KEY_REDUNDANT
 * ɾ������֮��info->m_findKey����KEY_COMPRESS��ʽ
 *
 * ɾ��֮ǰ��Ҫ��ҳ����м�������֤���ᱻ���������޸ģ�������Ҫע�����ɾ���������������Ӧ�ó���
 * ����ʧ�ܵ��������Ϊ������delete����update���ڽ���ɾ����������֮ǰ����session������и�����������ҳ���ҳ����
 * ���ɾ��������ҳ���������Ӧ�������ܹ��ɹ�
 *
 * @post ���۽��뺯��֮ǰ�Ƿ����ҳ������Դ�Լ���ɾ�������Ƿ�ɹ�������֮�󶼻������pin
 * @param info		ɨ����Ϣ���
 * @param hasLatch	�Ƿ����ɾ��������ҳ��Latch�����û��Latch����ҳ��ҳһ���Ѿ�pinס
 * @return true��ʾ�ɹ���false��ʾ�������̳�������
 */
bool DrsBPTreeIndex::deleteIndexEntry(DrsIndexScanHandleInfo *info, bool hasLatch) {
	ftrace(ts.idx, tout << info->m_session << rid(info->m_findKey->m_rowId) << info->m_latchMode << hasLatch << info->m_lockMode << info->m_pageId;);
	assert(info->m_rowHandle == NULL || info->m_session->isRowLocked(m_tableDef->m_id, (*info->m_rowHandle)->getRid(), Exclusived));

	Session *session = info->m_session;
	assert(info->m_pageHandle != NULL);
	IndexPage *page = (IndexPage*)info->m_pageHandle->getPage();	// ��Ϊ����pin�������Ȼ�ȡ

	// ������pinû��Latch�����
	if (!hasLatch) {
		assert(info->m_pageHandle->isPinned());
		LOCK_PAGE_HANDLE(session, info->m_pageHandle, Exclusived);
		if (info->m_pageLSN != page->m_lsn) {	// ҳ���иı䣬�ض�λҪɾ������
			session->releasePage(&info->m_pageHandle);
			page = researchForDelete(info);
		}
	}

	// ������Ҫ��ҳ�����
	PageId pageId = info->m_pageId;
	IDXResult result;
	u64 locateLSN = page->m_lsn;
	nftrace(ts.irl, tout << session->getId() << " del lock page " << pageId);
	while ((result = lockIdxObjectHoldingLatch(session, pageId, &info->m_pageHandle, FOR_DELETE, &DrsBPTreeIndex::judgerForDMLLocatePage, (void*)info, NULL)) == IDX_RESTART) {
		nftrace(ts.irl, tout << session->getId() << " need restart");
		// �����϶�����ʧ�ܣ����ǿ�����Ҫ�ض�λ
		page = researchForDelete(info);
		locateLSN = page->m_lsn;
		pageId = info->m_pageId;
	}
	assert(result == IDX_SUCCESS);
	page = (IndexPage*)info->m_pageHandle->getPage();
	pageId = info->m_pageId;
	// �������̿��ܻ����ҳ�汻�����߳��޸ģ����ʱ��Ҫ�ж�LSN�������ǲ���Ҫ���¶�λ������ҳ���λ��
	if (page->m_lsn != locateLSN) {
		// TODO: ���ﲢ����ֻҪLSN�ı�Ͷ�����Ҫ�ж�һ��
		makeFindKeyPADIfNecessary(info);
		page->findKeyInPage(info->m_findKey, &SearchFlag::DEFAULT_FLAG, info->m_comparator, info->m_cKey1, &(info->m_keyInfo));
		assert(info->m_keyInfo.m_result == 0);
		saveFoundKeyToFindKey(info);
	}

	/**
	 * ��ҪԤ�ȼ�SMO��
	 * ����ȵ�ɾ������֮���ټ�SMO��������������ʧ�ܣ����ʱ����ܻᵼ���з��ʲ������ʵ���ҳ��
	 * ����Ľ��������Ԥ�ȹ���ɾ��֮��ʣ��ռ��С�����С��ĳ����ֵ����Ԥ�ȼ�SMO���������������Ҫ�����κβ���
	 */
	bool smo = false;
	u16 prevKeySOffset, nextKeyEOffset;	// �����������ֱ��ʾɾ����ֵǰ������ʼƫ�ƺͺ��ƫ��
	if (page->prejudgeIsNeedMerged(info->m_findKey, info->m_cKey1, info->m_cKey2, info->m_keyInfo, &prevKeySOffset, &nextKeyEOffset)) {
		smo = true;
		// �������б�Ҫ������Դ��������ܻ���
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
 * ����Ϣ������е�ҳ����кϲ�������Ѱ��ҳ������ҽ����кϲ�������ռ䶼��������ֹͣ�ϲ�����Ҷҳ�濪ʼ���������߲����
 *
 * ������̲��������ҳ�����������ԭ����Բμ�insertSMO����˵������
 *
 * @pre	�ò�����Ȼ�Ѿ����ϲ㴮�У����������������ᱻ�޸ģ���˲���Ҫ�ж�ҳ��SMOλ
 *		ͬʱ��������info->m_findKey�ڵļ�ֵ��KEY_COMPRESS��ʽ��
 *
 * @param info			ɨ����Ϣ���
 * @attention ִ��֮ǰ����Ҫ�ϲ�ҳ���Latch������֮���Գ��и�Latch��Դ����ʹ��ҳ���Ѿ��ı�
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
		if (sonPage->isPageRoot()) {	// ������޷��ٺϲ�
			if (sonPage->isPageEmpty()) {
				nftrace(ts.idx, tout << "Root became empty";);
				if (sonPage->m_pageLevel != 0) {
					// ˵��������ɾ�գ�������Ҫ�޸ģ���ҳ������ҲҪ�޸�
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
		// 1.��λ����㣬�����ͷ��ӽ���Latch
		u16 sonUsedSpace = sonPage->getUsedSpace();
		bool pageEmpty = sonPage->isPageEmpty();
		sonPage->setSMOBit(session, sonPageId, logger);
		session->markDirty(sonHandle);
		session->releasePage(&sonHandle);

		// ���ñȽϼ�ֵѹ����ʽ�Լ��ȽϺ���
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

		// 2.ȷ�����Ժϲ��������ֵܽ��
		KeyInfo parentKeyInfo, siblingKeyInfo;
		IDXResult latchResult;
		SubRecord *sonParentKey = info->m_cKey1;
		parentPage->findKeyInPage(info->m_findKey, &SearchFlag::DEFAULT_FLAG, info->m_comparator, sonParentKey, &parentKeyInfo);
		NTSE_ASSERT(parentKeyInfo.m_result <= 0 || parentKeyInfo.m_miniPageNo == parentPage->m_miniPageNum - 1);	// ��λ������ǵ�ǰҳ��ĸ�ҳ����
		NTSE_ASSERT(PID_READ((byte*)parentPage + parentKeyInfo.m_eOffset - PID_BYTES) == sonPageId);

		// ��ԭfindKey��Ϣ
		if (!isFastComparable)
			info->m_findKey = backup;

		PageHandle *leftHandle = NULL, *rightHandle = NULL;
		IndexPage *leftPage = NULL, *rightPage = NULL;
		PageId rightPageId = INVALID_PAGE_ID, leftPageId = INVALID_PAGE_ID;

		u16 miniPageNoNext = parentKeyInfo.m_miniPageNo;
		u16 miniPageNoPrev = parentKeyInfo.m_miniPageNo;
		u16 offset;
		offset = parentPage->getNextKey(sonParentKey, parentKeyInfo.m_eOffset, true, &miniPageNoNext, info->m_cKey2);
		if (IndexKey::isKeyValid(info->m_cKey2)) {		// ��������
			rightPageId = IndexKey::getPageId(info->m_cKey2);
			latchResult = latchHoldingLatch(session, rightPageId, parentPageId, &parentHandle, &rightHandle, Exclusived);
			assert(latchResult != IDX_RESTART);

			rightPage = (IndexPage*)rightHandle->getPage();
			siblingKeyInfo.m_sOffset = parentKeyInfo.m_eOffset;
			siblingKeyInfo.m_eOffset = offset;
			siblingKeyInfo.m_miniPageNo = miniPageNoNext;
			if (rightPage->canPageBeMerged(sonUsedSpace)) {	// ȷ������ȡ��ҳ����кϲ�
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
				if (leftPage->canPageBeMerged(sonUsedSpace)) {	// ȷ������ȡ��ҳ����кϲ�
					// ���ﲻ��Ҫ��ҳ��������Ϊ��ϲ���ɾ����ֵ��ҳ�棬�Ѿ�������
					leftPage->setSMOBit(session, leftPageId, logger);
					rightPageId = sonPageId;
				} else {
					session->releasePage(&leftHandle);
					leftPage = NULL;
				}
			}
		}

		if (leftHandle == NULL && rightHandle == NULL) {	// ��ҳ���޷����кϲ�
			nftrace(ts.idx, tout << "No page to merge, level: " << level;);
			// ��ԭʼҳ��SMOλ
			latchResult = latchHoldingLatch(session, sonPageId, parentPageId, &parentHandle, &sonHandle, Exclusived);
			assert(latchResult != IDX_RESTART);
			sonPage = (IndexPage*)sonHandle->getPage();
			//PageId prevSonPageId = sonPage->m_prevPage;
			if (pageEmpty) {
				session->unlockPage(&sonHandle);	// �����������ȷ���
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

			if (pageEmpty) {	// ��ǰҳ��Ϊ�գ���û������ҳ����Խ��кϲ���ɾ����ҳ���Ӧ��
				// ��ʹҪɾ��������ҳ������һ���һ��Ҳ����Ҫ�����Ᵽ������ȥɾ��ǰһ��
				NTSE_ASSERT(parentPage->m_nextPage == INVALID_PAGE_ID || parentKeyInfo.m_eOffset < parentPage->getDataEndOffset() || parentPage->m_pageCount == 1);
				parentPage->deleteIndexKey(session, logger, parentPageId, info->m_cKey2, sonParentKey, info->m_cKey3, parentKeyInfo);
				session->markDirty(parentHandle);

				if (parentPage->isPageEmpty()) {	// �жϸ�ҳ���Ƿ�Ҫ�ϲ�
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

		// 3.����ҳ��ϲ�����ҳ�棬�޸�����ָ���ϵ
		rightPage->mergePage(session, logger, leftPage, rightPageId, leftPageId);
		nftrace(ts.idx, tout << session << " Merge page: " << leftPageId << " and " << rightPageId);
		session->markDirty(leftHandle);
		session->markDirty(rightHandle);
		// ����ͳ�Ƽ���
		++m_indexStatus.m_numMerge;

		if (rightPage->isPageLeaf())	// ��Ҫ��¼Ҷҳ��ϲ���ҳ������Latch
			mergedLeafPageId = rightPageId;
		session->releasePage(&rightHandle);
		rightPage = NULL;

		PageId leftLeftId = leftPage->m_prevPage;
		session->markDirty(leftHandle);
		if (leftLeftId != INVALID_PAGE_ID) {	// �޸ĺϲ�����ҳ�����ҳ�����ָ��
			session->unlockPage(&leftHandle);
			updatePrevPageNextLink(session, file, logger, leftLeftId, rightPageId);
			nftrace(ts.idx, tout << "Update prev page's next link: " << leftLeftId << " " << rightPageId);
			LOCK_PAGE_HANDLE(session, leftHandle, Exclusived);
		}

		// 4.ɾ����ҳ���Ӧ����
		latchResult = latchHoldingLatch(session, parentPageId, leftPageId, &leftHandle, &parentHandle, Exclusived);
		parentPage = (IndexPage*)parentHandle->getPage();
		assert(latchResult != IDX_RESTART);
		if (sonPageId == leftPageId)	// ����ҳ��ϲ�
			parentPage->deleteIndexKey(session, logger, parentPageId, info->m_cKey2, sonParentKey, info->m_cKey3, parentKeyInfo);
		else	// ����ҳ��ϲ�
			parentPage->deleteIndexKey(session, logger, parentPageId, sonParentKey, info->m_cKey2, info->m_cKey3, siblingKeyInfo);
		session->markDirty(parentHandle);

		// 5.����ϲ���ҳ��SMOλ���ͷ���ҳ�棬�ͷŵ�ҳ�治����
		// �������ҳ����Ҷҳ�沢�Ҹ�ҳ��ӹ�ҳ��������Ҫ�����ͷ�ҳ����
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

		// 6.�������ڵ��߼������е�������ڵ㲻����Ϊ��
		NTSE_ASSERT(!parentPage->isPageEmpty());
		//// 6.�жϸ�ҳ���Ƿ���Ҫ�ϲ�
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
	// Ҫ�ӻ�ɾ����ֵ���ڵ�ҳ���Latch
	info->m_pageId = mergedLeafPageId;
	info->m_pageHandle = GET_PAGE(session, file, PAGE_INDEX, mergedLeafPageId, Exclusived, m_dboStats, mergedLeafPage);

	session->getMemoryContext()->resetToSavepoint(savePoint);
}



//////////////////////////////////////////////////////////////////////////////////////


/**
 * ��SMO�����У���λ��ǰ��ĸ��ڵ㣬������Ҫ��������·��
 * @param info			ɨ����Ϣ���
 * @param level			���ڵ����ڵĲ���
 * @param researched	�ڱ���SMO�����У��Ƿ���й��ض�λ
 * @param sonPageId		��ǰ��ҳ���ID
 * @param parentPageId	out ���ظ�ҳ���pageId
 * @param parentHandle	out ���ظ�ҳ���handle
 * @return ���ζ�λ�����Ƿ�������ض�λ
 */
bool DrsBPTreeIndex::searchParentInSMO(DrsIndexScanHandleInfo *info, uint level, bool researched, PageId sonPageId, PageId *parentPageId, PageHandle **parentHandle) {
	Session *session = info->m_session;
	File *file = ((DrsBPTreeIndice*)m_indice)->getFileDesc();

	*parentPageId = info->m_traceInfo[level - info->m_lastSearchPathLevel].m_pageId;
	*parentHandle = GET_PAGE(session, file, PAGE_INDEX, *parentPageId, Exclusived, m_dboStats, NULL);
	IndexPage *parentPage = (IndexPage*)(*parentHandle)->getPage();
	if (!researched && !parentPage->findSpecifiedPageId(sonPageId)) {
		// LSN���ȣ��϶�˵���仯�������ȣ�����Ҫȷ���ܲ����ҵ�
		// ��ҳ������б䶯����Ҫ���¶�λ������SMO���У������ֻ���ܳ���һ��
		// ��ҳ������Ѿ��������߳�ɾ���������������
		nftrace(ts.idx, tout << session->getId() << " Key path changed during smo";);
		session->releasePage(parentHandle);

		if (info->m_findKey->m_format == KEY_PAD)
			info->m_comparator->setComparator(RecordOper::compareKeyPC);

		PageHandle *leafPageHandle = findSpecifiedLevel(info, &SearchFlag::DEFAULT_FLAG, level, IDX_SEARCH);
		assert(leafPageHandle != NULL);	// ���ﲻ���ܳ���SMO��鵼�µ����������ҳ��϶������ҵõ�
		session->releasePage(&leafPageHandle);

		info->m_comparator->setComparator(info->m_isFastComparable ? RecordOper::compareKeyCC : RecordOper::compareKeyPC);

		*parentPageId = info->m_traceInfo[0].m_pageId;
		*parentHandle = GET_PAGE(session, file, PAGE_INDEX, *parentPageId, Exclusived, m_dboStats, NULL);

		return true;
	}

	return false;
}


/**
 * ���в���SMO֮ǰ��ҪԤ�ȼ�����SMO�����Ϳ����޸ĵ�Ҷҳ����
 * @pre ����ҳ�����Ѿ��ӳɹ�
 * @param session		�Ự���
 * @param insertHandle	in/out �����ҳ��ľ��
 * @post �������ɹ������ͷ��κ���Դ������ʧ�ܻ��ͷ�ҳ��Latch��Դ
 * @return �������ɹ�����true������������false
 */
bool DrsBPTreeIndex::insertSMOPrelockPages(Session *session, PageHandle **insertHandle) {
	SYNCHERE(SP_IDX_TO_LOCK_SMO);

	IndexPage *page = (IndexPage*)(*insertHandle)->getPage();
	// ������SMO����ִ��SMO��Ȼ�����²���
	nftrace(ts.irl, tout << session->getId() << " ins need smo lock " << m_indexId);
	IDXResult result = lockIdxObjectHoldingLatch(session, m_indexId, insertHandle);
	assert(result != IDX_RESTART);
	if (result == IDX_FAIL)
		return false;

	// ��ΪSMOҪ�޸�Ҷҳ�����ӣ�Ԥ�Ӳ���ҳ�����һ��ҳ�����
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
 * ����ɾ��SMO֮ǰ��ҪԤ�ȼ�����SMO�����Ϳ����޸ĵ�Ҷҳ����
 * @pre ɾ��ҳ�����Ѿ��ӳɹ�
 * @post �������ɹ������ͷ��κ���Դ������ʧ�ܻ��ͷ�ҳ��Latch��Դ
 * @param session		�Ự���
 * @param insertHandle	in/out ɾ����ҳ��ľ��
 * @return �������ɹ�����true������������false
 */
bool DrsBPTreeIndex::deleteSMOPrelockPages(Session *session, PageHandle **deleteHandle) {
	SYNCHERE(SP_IDX_TO_LOCK_SMO);

	IndexPage *page = (IndexPage*)(*deleteHandle)->getPage();
	IDXResult result = lockIdxObjectHoldingLatch(session, m_indexId, deleteHandle);
	nftrace(ts.irl, tout << session->getId() << " del need smo and lock " << m_indexId);
	if (result == IDX_FAIL)	{// ����
		nftrace(ts.irl, tout << session->getId() << " smo lock dl");
		return false;
	}
	assert(result == IDX_SUCCESS);	// ����ҳ����û�˻��޸�����

	// �ټ�������������ҳ���ҳ����
	PageId prevPageId = page->m_prevPage, nextPageId = page->m_nextPage;
	if (prevPageId != INVALID_PAGE_ID) {
		result = lockIdxObjectHoldingLatch(session, prevPageId, deleteHandle);
		nftrace(ts.irl, tout << session->getId() << " del need smo and lock " << prevPageId);
		if (result == IDX_FAIL)	{// ����
			nftrace(ts.irl, tout << session->getId() << " smo lock dl");
			return false;
		}
		assert(result == IDX_SUCCESS);	// ����ҳ����û�˻��޸�����

		// ������ǰһ��ҳ��
		session->unlockPage(deleteHandle);
		PageHandle *ppHandle = GET_PAGE(session, ((DrsBPTreeIndice*)m_indice)->getFileDesc(), PAGE_INDEX, prevPageId, Shared, m_dboStats, NULL);
		PageId prevPrevPageId = ((IndexPage*)ppHandle->getPage())->m_prevPage;
		session->releasePage(&ppHandle);
		LOCK_PAGE_HANDLE(session, *deleteHandle, Exclusived);
		if (prevPrevPageId != INVALID_PAGE_ID) {
			result = lockIdxObjectHoldingLatch(session, prevPrevPageId, deleteHandle);
			nftrace(ts.irl, tout << session->getId() << " del need smo and lock " << prevPrevPageId);
			if (result == IDX_FAIL)	{// ����
				nftrace(ts.irl, tout << session->getId() << " smo lock dl");
				return false;
			}
			assert(result == IDX_SUCCESS);	// ����ҳ����û�˻��޸�����
		}
	}
	if (nextPageId != INVALID_PAGE_ID) {
		result = lockIdxObjectHoldingLatch(session, nextPageId, deleteHandle);
		nftrace(ts.irl, tout << session->getId() << " del need smo and lock " << nextPageId);
		if (result == IDX_FAIL)	{// ����
			nftrace(ts.irl, tout << session->getId() << " smo lock dl");
			return false;
		}
		assert(result == IDX_SUCCESS);	// ����ҳ����û�˻��޸�����
	}

	return true;
}

/**
 * ����ָ��������ֵ
 * @pre ʹ���߱������úò����ֵ��ʽREC_REDUNDANT�Լ��ȽϺ���compareRC
 *
 * ��ִ�в���֮ǰ����Ҫ��ҳ����м�������ʱ�Ĳ�������ǵ���insert�����ģ���ô�϶�������������
 * ������β�����update�����ģ���ô���ܳ�������
 *
 * @param info				������Ϣ���
 * @param duplicateKey		out true��ʾ����ᵼ��Ψһ��������ͬ��ֵ��false��ʾ����
 * @param checkDuplicate	ָ���Ƿ���Ҫ���Ψһ�ԣ�����ǻָ���������Ҫ���Ψһ�ԣ�Ĭ��Ϊtrue��ֻ�лָ�����ɾ������������false
 * @return true��ʾ����ɹ����Ҳ�Υ��Ψһ��Լ����false��ʾΥ��Ψһ��Լ����������������
 */
bool DrsBPTreeIndex::insertIndexEntry(DrsIndexScanHandleInfo *info, bool *duplicateKey, bool checkDuplicate) {
	ftrace(ts.idx, tout << info << checkDuplicate);

	IndexLog *logger = ((DrsBPTreeIndice*)m_indice)->getLogger();
	Session *session = info->m_session;
	IndexPage *page;
	*duplicateKey = false;
	u64 token, beforeAddLSN;

InsertStart:
	// ��λҪ�����ҳ�沢��ҳ�����
	if ((info->m_pageHandle = findSpecifiedLevel(info, &SearchFlag::DEFAULT_FLAG, 0, IDX_SEARCH)) == NULL)
		goto InsertFail;	// �����ʾ��SMO����������

	SYNCHERE(SP_IDX_RESEARCH_PARENT_IN_SMO);

	nftrace(ts.irl, tout << session->getId() << " ins need lock page " << info->m_pageId);
	token = session->getToken();
	IDX_SWITCH_AND_GOTO(lockIdxObjectHoldingLatch(session, info->m_pageId, &(info->m_pageHandle), FOR_INSERT, &DrsBPTreeIndex::judgerForDMLLocatePage, (void*)info, NULL), InsertStart, InsertFail);
	nftrace(ts.irl, tout << session->getId() << " ins lock " << info->m_pageId << " succ");
	page = (IndexPage*)info->m_pageHandle->getPage();

	// ���Ҳ����ֵ��ҳ���е�λ��
	info->m_keyInfo.m_sOffset = 0;
	info->m_cKey1->m_size = info->m_cKey2->m_size = 0;
	page->findKeyInPageTwo(info->m_findKey, info->m_comparator, &(info->m_cKey1), &(info->m_cKey2), &(info->m_keyInfo));

	// ���Ψһ��Լ����������г�ͻ��ֱ�ӷ���
	// ע:ֻ��NTSE����Ҫ���������Ψһ�Լ�飬TNT����Ҫ
	if (NULL == session->getTrans() && m_indexDef->m_unique && checkDuplicate) {
		IDXResult result = insertCheckSame(info, duplicateKey);
		if (result == IDX_RESTART) {	// �����Ļ�����Ҫ�ͷ��Ѿ����е�ҳ����
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

	// ��������п����޸���ҳ��(����MiniPage������������MiniPage�ɹ������Ǻ����������ֿռ䲻��)
	// ��������Ҫ��ǵ�ǰҳ��Ϊ��
	beforeAddLSN = page->m_lsn;
	if (page->addIndexKey(session, logger, info->m_pageId, info->m_findKey, info->m_key0, info->m_cKey1, info->m_cKey2, info->m_comparator, &(info->m_keyInfo)) == NEED_SPLIT) {
		if (beforeAddLSN != page->m_lsn)
			session->markDirty(info->m_pageHandle);

		if (!checkDuplicate) {
			NTSE_ASSERT(beforeAddLSN == page->m_lsn);
			// ����Ҫ���Ψһ�Ա����ǻ��˲���
			// ҳ��ı�������MP���ѣ��ϲ�֮ǰ���ѵ�MP����֤ҳ��״̬һ��
			NTSE_ASSERT(page->m_splitedMPNo != (u16)-1);
			NTSE_ASSERT(page->mergeTwoMiniPage(session, logger, info->m_pageId, page->m_splitedMPNo, info->m_cKey1, info->m_cKey2));
		} else {
			// ���ʱ����ҪSMO��Ԥ�ȼ���
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
 * ������������ʹ�ü�ֵ���Բ��룬����������ڹؼ������Ƕ����������޸��Ǵ��е�
 * �������̲�����ıȽ���ָ��
 *
 * �������̵��У����Ҷҳ����һ�������ɵ�ҳ���������֤�����������ⱻ�޸�
 * ���ǲ���Ҫ�������ֵܽڵ��������Ȼ���Ǳ��޸�������ָ�룬��Ϊ��
 * �޸�����ָ��Ĳ�����ֻ����SMO�����ſ��ܷ�������SMO�����ᱻ������SMO�޸������л�
 * ����ڱ������޸��ύǰ���������б���������޸�ҳ�������ָ�룬ֻ�����޸�ҳ�������
 * �����ݵ��޸ģ�����Ӱ������ָ���޸ĵ�undo����
 *
 * @pre	�ϲ㸺���memoryContext���б������ƣ�����ֻ�������
 * @param info			������Ϣ���
 * @attention ����֮ǰinfo�ڵ�pageHandle����X��������֮��pageHandle��Ӧ��ʾҪ�����ҳ�棬�һ��ǳ���X��
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

	IndicePageManager *pagesManager = ((DrsBPTreeIndice*)m_indice)->getPagesManager();	// �����ϲ��Ѿ��в������������ֱ�ӻ�ȡӦ��
	IndexLog *logger = ((DrsBPTreeIndice*)m_indice)->getLogger();

	PageId newPageId = INVALID_PAGE_ID;
	PageHandle *newHandle = NULL;
	IndexPage *newPage = NULL;

	KeyInfo keyInfo;
	keyInfo.m_sOffset = info->m_keyInfo.m_sOffset;
	bool rootSplited = false;
	bool researched = false;
	PageId realSonPageId = INVALID_PAGE_ID;

	// ���¼����������·����ԭ���ǣ����ʹ��info���е���ʱ��ֵ�����ܻᵼ�����ã����QA49342
	SubRecord *maxSonPageKey1 = IndexKey::allocSubRecord(memoryContext, m_indexDef, KEY_COMPRESS);
	SubRecord *maxSonPageKey2 = IndexKey::allocSubRecord(memoryContext, m_indexDef, KEY_COMPRESS);
	SubRecord *parentKey = IndexKey::allocSubRecord(memoryContext, m_indexDef, KEY_COMPRESS);
	SubRecord *splitKey = IndexKey::allocSubRecord(memoryContext, m_indexDef, KEY_COMPRESS);

	// ��ӦmaxRecord�ķ�ѹ����ʽ������Ҫ��ʹ��
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

		// 1.��Ҷ��㣬���Բ���������ҳ��������
		if (!sonPage->isPageLeaf()) {
			assert(IndexKey::isKeyValid(maxSonPageKey1) && IndexKey::isKeyValid(maxSonPageKey2));
			// ����ɾ��ԭ������϶��ܳɹ�
			if (!rootSplited)
				sonPage->deleteIndexKey(session, logger, sonPageId, cKey1, parentKey, cKey2, keyInfo);

			// �Ȳ����ģ���֤������������ͬһ��ҳ�棬���洦���
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
					break;	// �ɹ�SMO����
				} else {
					reservedSize = (u16)(maxSonPageKey1->m_size);
				}
			} else {
				reservedSize = (u16)(maxSonPageKey1->m_size + maxSonPageKey2->m_size);
			}
		}

		// 2.����ҳ�棬��ͬҳ���ȡ��ͬ�ķ���
		if (sonPage->isPageRoot()) {
			// ����������ҳ�棬��ҳ��ʼ�ձ��ֲ���
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
			// �����Ҷҳ�棬��Ҫ����ҳ���ҳ��������ֹ�����������޸�
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

			// �����Ҷҳ�棬��Ҫ����ҳ���ҳ��������ֹ�����������޸�
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

		// 3.��Ҷ��㣬�ٴγ��Բ���͸��£��ض��ɹ�
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
			// Ҷ�����Ҫ��¼�������Ӧ��ҳ��
			if (info->m_keyInfo.m_sOffset < sonPage->getDataEndOffset())	//! ������С�ڣ�����ṹ����
				realSonPageId = sonPageId;
			else
				realSonPageId = newPageId;
		}

		// 4.���ɵ�ǰ������ҳ�������ֵ׼�����븸���
		{
			KeyInfo kInfo;
			newPage->getLastKey(maxSonPageKey2, &kInfo);
			sonPage->getLastKey(maxSonPageKey1, &kInfo);
			NTSE_ASSERT(!sonPage->isPageLeaf() || maxSonPageKey1->m_rowId != maxSonPageKey2->m_rowId);	// ��Ҷ�ڵ��޷���֤rowIdһ������ͬ

			// ����pageId��Ϣ����־data����λ
			IndexKey::appendPageId(maxSonPageKey2, newPageId);
			IndexKey::appendPageId(maxSonPageKey1, sonPageId);

			// ����޷����ٱȽϣ�ȡ�÷�ѹ����ʽ��ֵ
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

		session->unlockPage(&newHandle);	// ����ֻ�ͷ����ķ�ʽ���������SMO Bit��ʱ���ٷ�pin
		session->unlockPage(&sonHandle);

		// 5. ������ҳ����ҳ�����ָ��
		if (siblingId != INVALID_PAGE_ID) {
			updateNextPagePrevLink(session, file, logger, siblingId, newPageId);
			nftrace(ts.idx, tout << "Update new page's next page link: " << newPageId << " " << siblingId);
		}

		// 6. ��λ����㣬��Ҫ�жϼ�¼�ĸ��ڵ���Ϣ�Ƿ���ȷ���������ȷ����Ҫ������������������ֻ�����һ��
		PageId oldSonPageId = sonPageId;
		researched = searchParentInSMO(info, level, researched, oldSonPageId, &sonPageId, &sonHandle);
		sonPage = (IndexPage*)sonHandle->getPage();

		// 7.���Ǹ������ѹ�������Ƚϸ�ҳ���Ӧ���insertRecord�Ĵ�С
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
			if (result < 0 || ((result == 0) && (maxSonPageKey2->m_rowId < parentKey->m_rowId))) {	// ��parentKey������maxSonPageKey2���к�������
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

	// д��ҳ������LSN�����SMOλ�����ҳ��Ϊ�࣬�ͷ�pin
	for (u16 i = 0; i < smoPageNum; i++) {
		LOCK_PAGE_HANDLE(session, smoPageHandle[i], Exclusived);
		IndexPage *page = (IndexPage*)smoPageHandle[i]->getPage();
		assert(page != NULL);
		page->clearSMOBit(session, smoPageIds[i], logger);
		session->markDirty(smoPageHandle[i]);
		session->releasePage(&smoPageHandle[i]);
	}

	// ��������Ҫ�����ҳ�汣�浽info
	info->m_pageId = realSonPageId;
	info->m_pageHandle = GET_PAGE(session, file, PAGE_INDEX, realSonPageId, Exclusived, m_dboStats, NULL);
	info->m_comparator->setComparator(info->m_isFastComparable ? RecordOper::compareKeyCC : RecordOper::compareKeyPC);

	return;
}



/**
 * ���ڲ������������ֵ��Ψһ��
 *
 * @pre ����֮ǰ��info��Ҫ��������ֵ��ҳ����ĸ�MiniPage���Լ��������ʼƫ��
 * ͬʱinfo->m_idxKey1��ʾ�����ֵ��ǰ����info->m_idxKey2��ʾ��̣����û��ǰ�����ߺ�̣���ֵ��m_size=0
 * @post ��Ӧ�øı�info��keyInfo�Լ�m_idxKey1��m_idxKey2������
 * @param info		������Ϣ���
 * @param hasSame	out �����Ƿ��������ֵͬ
 * @return ���ؼ���֮���״̬�����һֱ����IDX_SUCCESS����һ�·���IDX_RESTART������������IDX_FAIL
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
	if (!IndexKey::isKeyValid(prev)) {	// Ҫ�ж�����Ҫ��ҳ�滹��ֻ��Ҫȡǰһ��MiniPage����
		NTSE_ASSERT(keyInfo.m_sOffset <= INDEXPAGE_DATA_START_OFFSET);
		spanPage = true;
		nextPageId = page->m_prevPage;
		prev = info->m_cKey3;
		// ����ֻ��Ҫ��MiniPage�����ǲ����ܵ�
	} 

	if (!IndexKey::isKeyValid(next)) {	// Ҫ�ж�����Ҫ��ҳ�滹��ֻ��Ҫȡ��һ��MiniPage����
		if (keyInfo.m_eOffset < page->getDataEndOffset()) {
			// ֻ��Ҫ��MiniPage
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

	// ����һ��ҳ���������ٰ�������������
	// ��Ҫ���ҿ��ܵĻ�Ҫ��ҳ��ȡ��
	if (spanPage && nextPageId != INVALID_PAGE_ID) {
		Session *session = info->m_session;
		PageId pageId = info->m_pageId;
		PageHandle *nextHandle;

		// �����ҳ��
		IDX_SWITCH_AND_GOTO(latchHoldingLatch(session, nextPageId, pageId, &info->m_pageHandle, &nextHandle, Shared), CheckUniqueRestart, CheckUniqueFail);
		// �ͷ���ҳ���������ֹ������������latch�����������ʱ���õ���ԭҳ�汻�޸�
		session->unlockPage(&info->m_pageHandle);
		IDX_SWITCH_AND_GOTO(checkSMOBit(session, &nextHandle), CheckRestartUnpin, CheckFailUnpin);
		// ������Ҫ��֤��next-key����ҳ���������֤û�п��ܻ���˵��޸ģ���֤��ֵ��Ψһ���ж���ȷ
		// ���ܻ��˵��޸ģ�ĳ���²���ɾ���˵�ǰ�����ֵ��next-key�����ǲ���������жϾͲ���Ļ���update�����޷�����
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

	// �Ƚ��Ƿ�����ͬ��ֵ����
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
CheckFailUnpin:	// ���ﶼ��Ҫ�ͷŵ�pin������ֻ�з���������pin
	info->m_session->unpinPage(&info->m_pageHandle);
	return IDX_FAIL;
CheckRestartUnpin:
	info->m_session->unpinPage(&info->m_pageHandle);
	return IDX_RESTART;
}


//////////////////////////////////////////////////////////////////////////////////////



/**
 * �ҵ�ָ������������ҳ��
 * @pre ��������Ҫ���úñȽϺ���
 * @post ����֮��traceInfo���鵱�и��±�����ǴӸ���㵽ָ����֮��Ľ����Ϣ,ָ���㵽Ҷ���Ľ����Ϣ����
 * ����ɹ�����֮��scanInfo���е�m_pageId�Լ�m_traceInfo�ᱻ���á��������NULL����ʾ������������������Ҫ���¿�ʼ
 *
 * @param scanInfo	IN/OUT	ɨ����Ϣ
 * @param flag		���ұ��
 * @param level		Ҫ��λ���Ĳ���
 * @param findType	��ǰ�Ķ�λ�ǲ��ҹ��̵Ķ�λ����Ԥ������۵Ĳ��Ҷ�λ
 * @return ָ����������������ҳ��
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
	// �õ���ҳ�沢���ҳ��SMO״̬
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
		page->findKeyInPage(findKey, flag, scanInfo->m_comparator, foundKey, &keyInfo);	// ���ﶨλ����Ӧ�ÿ���ֱ��ʹ��
		assert(keyInfo.m_result <= 0 || keyInfo.m_miniPageNo == page->m_miniPageNum - 1);
		scanInfo->m_everEqual = (keyInfo.m_result == 0 || scanInfo->m_everEqual);

		// ����ҳ��Latch���ͷŸ�ҳ��Latch
		PageId childPageId = IndexKey::getPageId(foundKey);
		PageHandle *childHandle = NULL;
		IDX_SWITCH_AND_GOTO(latchHoldingLatch(session, childPageId, parentPageId, &parentHandle, &childHandle, Shared), FindLevelStart, Fail);

		// ��0��洢����Ҫ��λ�㸸���ҳ��
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

		// �����ҳ���SMO״̬λ
		IDX_SWITCH_AND_GOTO(checkSMOBit(session, &childHandle), FindLevelStart, Fail);

		parentPageId = childPageId;
		parentHandle = childHandle;
		page = (IndexPage*)childHandle->getPage();
		curLevel--;
	}

	assert(diffLevel == 0);
	scanInfo->m_lastSearchPathLevel = (u16)level;

	// �ж��Ƿ�Ҫ����Latch
	if (scanInfo->m_latchMode == Exclusived) {
		IDX_SWITCH_AND_GOTO(upgradeLatch(session, &parentHandle), FindLevelStart, Fail);
	}
	SYNCHERE(SP_IDX_WAIT_TO_FIND_SPECIAL_LEVEL_PAGE);
	// ��¼��Ҷҳ��PageId
	scanInfo->m_pageId = parentPageId;
	return parentHandle;

Fail:
	return NULL;
}


/**
 * �ҵ�ָ������������ҳ��
 * @pre ��������Ҫ���úñȽϺ���
 * @post ����֮��traceInfo���鵱�и��±�����ǴӸ���㵽ָ����֮��Ľ����Ϣ,ָ���㵽Ҷ���Ľ����Ϣ����
 * ����ɹ�����֮��scanInfo���е�m_pageId�Լ�m_traceInfo�ᱻ���á��������NULL����ʾ������������������Ҫ���¿�ʼ
 *
 * @param scanInfo	IN/OUT	ɨ����Ϣ
 * @param flag		���ұ��
 * @param level		Ҫ��λ���Ĳ���
 * @param findType	��ǰ�Ķ�λ�ǲ��ҹ��̵Ķ�λ����Ԥ������۵Ĳ��Ҷ�λ
 * @return ָ����������������ҳ��
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
	// �õ���ҳ�沢���ҳ��SMO״̬
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
		page->findKeyInPage(findKey, flag, scanInfo->m_comparator, foundKey, &keyInfo);	// ���ﶨλ����Ӧ�ÿ���ֱ��ʹ��
		assert(keyInfo.m_result <= 0 || keyInfo.m_miniPageNo == page->m_miniPageNum - 1);
		scanInfo->m_everEqual = (keyInfo.m_result == 0 || scanInfo->m_everEqual);

		// ����ҳ��Latch���ͷŸ�ҳ��Latch
		PageId childPageId = IndexKey::getPageId(foundKey);
		PageHandle *childHandle = NULL;
		IDX_SWITCH_AND_GOTO(latchHoldingLatch(session, childPageId, parentPageId, &parentHandle, &childHandle, Shared), FindLevelStart, Fail);

		// ��0��洢����Ҫ��λ�㸸���ҳ��
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

		// �����ҳ���SMO״̬λ
		IDX_SWITCH_AND_GOTO(checkSMOBit(session, &childHandle), FindLevelStart, Fail);

		parentPageId = childPageId;
		parentHandle = childHandle;
		page = (IndexPage*)childHandle->getPage();
		curLevel--;
	}

	assert(diffLevel == 0);

	// �ж��Ƿ�Ҫ����Latch
	if (scanInfo->m_latchMode == Exclusived) {
		IDX_SWITCH_AND_GOTO(upgradeLatch(session, &parentHandle), FindLevelStart, Fail);
	}

	// ��¼��Ҷҳ��PageId
	scanInfo->m_readPageId = parentPageId;
	return parentHandle;

Fail:
	return NULL;
}
/**
 * ���ɨ�����б����Ҷҳ����Ϣ�Ƿ���Ч
 * @param info
 * @param flag
 * @param needFetchNext
 * @return ҳ����Ϣ��Ч����IDX_SUCCESS, ҳ����Ч����IDX_RESTART��������Ϊ�շ���IDX_FAIL
 */
IDXResult DrsBPTreeIndex::checkHandleLeafPage(DrsIndexScanHandleInfo *info, SearchFlag *flag, 
											  bool *needFetchNext, bool forceSearchPage) {
	if (NULL != info->m_pageHandle) {
		Session *session = info->m_session;
		SubRecord *record = info->m_cKey1;
		KeyInfo *keyInfo = &(info->m_keyInfo);

		assert(info->m_pageHandle->isPinned());
		// ���������£�ɨ����Ӧ�ó��е�ǰҳ���pin����û��Latch
		PageHandle *pageHandle = info->m_pageHandle;
		LOCK_PAGE_HANDLE(session, pageHandle, info->m_latchMode);
		IndexPage *page = (IndexPage*)pageHandle->getPage();

		// ����ж�ҳ��LSN
		if (page->m_lsn != info->m_pageLSN || forceSearchPage) {
			// ����Ҫ��֤��ҳ�滹�Ǳ�������Ҷҳ�棬����ҳ����Ч������ҳ�治����SMO״̬
			if (!page->isPageLeaf() || page->m_pageType == FREE || checkSMOBit(session, &pageHandle) != IDX_SUCCESS) {
				if (pageHandle != NULL)
					session->releasePage(&pageHandle);
				return IDX_RESTART;
			}

			if (page->isPageEmpty()) {	// ��������ʱΪ��
				NTSE_ASSERT(page->isPageRoot());
				return IDX_FAIL;
			}

			assert(IndexPage::getIndexId(page->m_pageMark) == m_indexId);
			// ����ҳ���ڲ������ң����۲��ҵļ�ֵ���ڲ����ڣ�ֻҪ��ҳ��û�з�����SMO��
			// ���߼�ʹSMO�������ǲ��ҵ��ļ�ֵ��ҳ����в�������ò�������Ч
			makeFindKeyPADIfNecessary(info);
			*needFetchNext = page->findKeyInPage(info->m_findKey, flag, info->m_comparator, record, keyInfo);
			if (!(page->m_smoLSN == info->m_pageSMOLSN || (keyInfo->m_result <= 0 && keyInfo->m_sOffset != INDEXPAGE_DATA_START_OFFSET))) {
				// ��ҳ���Ҳ������������¿�ʼ�ң����ȷ���
				session->releasePage(&pageHandle);
				return IDX_RESTART;
			}
		}
		return IDX_SUCCESS;
	}
	return IDX_RESTART;
}



/**
 * ���ɨ�����б����Ҷҳ����Ϣ�Ƿ���Ч
 * @param info
 * @param flag
 * @param needFetchNext
 * @return ҳ����Ϣ��Ч����IDX_SUCCESS, ҳ����Ч����IDX_RESTART��������Ϊ�շ���IDX_FAIL
 */
IDXResult DrsBPTreeIndex::checkHandleLeafPageSecond(DrsIndexScanHandleInfoExt *info, SearchFlag *flag, 
											  bool *needFetchNext, bool forceSearchPage) {
	if (NULL != info->m_pageHandle) {
		Session *session = info->m_session;
		SubRecord *record = info->m_cKey1;
		KeyInfo *keyInfo = &(info->m_readKeyInfo);

		assert(info->m_pageHandle->isPinned());
		// ���������£�ɨ����Ӧ�ó��е�ǰҳ���pin����û��Latch
		LOCK_PAGE_HANDLE(session, info->m_pageHandle, info->m_latchMode);
		IndexPage *page = (IndexPage*)info->m_pageHandle->getPage();

		// ����ж�ҳ��LSN
		if (page->m_lsn != info->m_pageLSN || forceSearchPage) {
			// ����Ҫ��֤��ҳ�滹�Ǳ�������Ҷҳ�棬����ҳ����Ч������ҳ�治����SMO״̬
			if (!page->isPageLeaf() || page->m_pageType == FREE || checkSMOBit(session, &(info->m_pageHandle)) != IDX_SUCCESS) {
				if (info->m_pageHandle != NULL)
					session->releasePage(&(info->m_pageHandle));
				return IDX_RESTART;
			}

			if (page->isPageEmpty()) {	// ��������ʱΪ��
				NTSE_ASSERT(page->isPageRoot());
				return IDX_FAIL;
			}

			assert(IndexPage::getIndexId(page->m_pageMark) == m_indexId);
			// ����ҳ���ڲ������ң����۲��ҵļ�ֵ���ڲ����ڣ�ֻҪ��ҳ��û�з�����SMO��
			// ���߼�ʹSMO�������ǲ��ҵ��ļ�ֵ��ҳ����в�������ò�������Ч
			makeFindKeyPADIfNecessary(info);
			*needFetchNext = page->findKeyInPage(info->m_findKey, flag, info->m_comparator, record, keyInfo);
			if (!(page->m_smoLSN == info->m_pageSMOLSN || (keyInfo->m_result <= 0 && keyInfo->m_sOffset != INDEXPAGE_DATA_START_OFFSET))) {
				// ��ҳ���Ҳ������������¿�ʼ�ң����ȷ���
				session->releasePage(&(info->m_pageHandle));
				return IDX_RESTART;
			}
		}
		return IDX_SUCCESS;
	}
	return IDX_RESTART;
}
/**
 * ��������ɨ������Ϣ����λ���Ҷҳ���Լ���ؼ�ֵλ��
 * @pre	���ڴ�����Ҽ�ֵinfo->m_findKey��������REC_REDUNDANT/KEY_PAD/KEY_COMPRESS������KEY_COMPRESS��ʱ����Ҫע�����
 *		�޷����п��ٱȽϣ���Ҫ��ѹ����ֵ��ѹ���бȽ�
 * @post һ���������ļ�ֵ������info->m_idxkey1������fetchNext�ĵ��ã����ҳ��û�иı䣬��ֵ������info->m_findkey
 *
 * @param info			����ɨ����Ϣ���
 * @param needFetchNext	OUT	��Ҫ����ȡ�߼�����
 * @param result		OUT	Ҫ���Ҽ�ֵ�Ͷ�λ���ļ�ֵ�Ĵ�С��ϵ
 * @return ��λ�ɹ�����ʧ�ܣ�ʧ�ܱ�ʾ����Ϊ��
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

	//��Ҫ�Ӹ�ҳ�濪ʼ��λҶҳ��
	// ��֤���Ҽ�ֵm_findKey��ʽ����ȷ��
	makeFindKeyPADIfNecessary(info);
	// ����ָ����ֵ���Ӹ������������
	NTSE_ASSERT((pageHandle = findSpecifiedLevel(info, &flag, 0, IDX_SEARCH)) != NULL);
	if (((IndexPage*)(pageHandle->getPage()))->isPageEmpty()) {// ������Ϊ��
		goto Failed;
	}

	info->m_pageHandle = pageHandle;
	page = (IndexPage*)pageHandle->getPage();
	fetchNext = page->findKeyInPage(info->m_findKey, &flag, info->m_comparator, info->m_cKey1, keyInfo);

Succeed:
	//Ҷҳ�涨λ�ɹ�
	*needFetchNext = fetchNext;
	*result = keyInfo->m_result;
	return true;

Failed:
	//����ʧ�ܣ�������Ϊ��
	nftrace(ts.idx, tout << "Tree is empty";);
	info->m_pageHandle = pageHandle;
	*needFetchNext = false;
	*result = 1;
	return false;
}


/**
 * ��������ɨ������Ϣ����λ���Ҷҳ���Լ���ؼ�ֵλ��
 * @pre	���ڴ�����Ҽ�ֵinfo->m_findKey��������REC_REDUNDANT/KEY_PAD/KEY_COMPRESS������KEY_COMPRESS��ʱ����Ҫע�����
 *		�޷����п��ٱȽϣ���Ҫ��ѹ����ֵ��ѹ���бȽ�
 * @post һ���������ļ�ֵ������info->m_idxkey1������fetchNext�ĵ��ã����ҳ��û�иı䣬��ֵ������info->m_findkey
 *
 * @param info			����ɨ����Ϣ���
 * @param needFetchNext	OUT	��Ҫ����ȡ�߼�����
 * @param result		OUT	Ҫ���Ҽ�ֵ�Ͷ�λ���ļ�ֵ�Ĵ�С��ϵ
 * @return ��λ�ɹ�����ʧ�ܣ�ʧ�ܱ�ʾ����Ϊ��
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

	//��Ҫ�Ӹ�ҳ�濪ʼ��λҶҳ��
	// ��֤���Ҽ�ֵm_findKey��ʽ����ȷ��
	makeFindKeyPADIfNecessary(info);
	// ����ָ����ֵ���Ӹ������������
	NTSE_ASSERT((info->m_readPageHandle = findSpecifiedLevelSecond(info, &flag, 0, IDX_SEARCH)) != NULL);
	if (((IndexPage*)(info->m_readPageHandle->getPage()))->isPageEmpty()) {// ������Ϊ��
		goto Failed;
	}

	page = (IndexPage*)info->m_readPageHandle->getPage();
	fetchNext = page->findKeyInPage(info->m_findKey, &flag, info->m_comparator, info->m_cKey1, keyInfo);

Succeed:
	//Ҷҳ�涨λ�ɹ�
	*needFetchNext = fetchNext;
	*result = keyInfo->m_result;
	return true;

Failed:
	//����ʧ�ܣ�������Ϊ��
	nftrace(ts.idx, tout << "Tree is empty";);
	*needFetchNext = false;
	*result = 1;
	return false;
}
/**
 * ��ʼ����֮ǰȷ�����Ҽ�ֵ��ʽ��ȷ
 * @pre �����Ҫ��������������info->m_findKey��info->m_idxKey0����ʽ�ֱ���KEY_COMPRESS��KEY_PAD
 * @post info->m_findKey��ʽ��KEY_PAD��ͬʱ�ȽϺ���Ҳ�����ù�
 * @param	info	ɨ����Ϣ���
 * @return  ������Key0��findKey�򷵻�true�� ���򷵻�false
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
 * �ض�λ����֮�������Ҫ�����ҵ��ļ�ֵ�洢��findKey���У�ͬʱ��֤findKey�ĸ�ʽ��KEY_COMPRESS,��idxKey0��KEY_PAD
 * @post info->m_findKey��ʽ��KEY_PAD��ͬʱ�ȽϺ���Ҳ�����ù�
 * @param info	ɨ������Ϣ
 */
void DrsBPTreeIndex::saveFoundKeyToFindKey(DrsIndexScanHandleInfo *info) {
	if (info->m_findKey->m_format == KEY_PAD) {
		assert(info->m_key0 != NULL && info->m_key0->m_format == KEY_COMPRESS);
		IndexKey::swapKey(&(info->m_findKey), &(info->m_key0));
	}
	IndexKey::swapKey(&info->m_findKey, &info->m_cKey1);
}


/**
 * �ӵ�ǰinfo��Ϣ��λ���������Ҫ��ȡ�߼���һ��
 * �ڱ���������Ҫ����ȡ��һ����Ҫ��ҳ������
 *
 * @post ���ڷ��أ�IDX_SUCCESS��ʾ���з�����ҳ���Latch��Pin����������������κ���Դ
 *			����ֵ������info->m_idxKey1����
 *
 * @param info	ɨ������Ϣ
 * @return	����ȡ��ɹ�IDX_SUCCESS����Ҫ���¿�ʼIDX_RESTART����������IDX_FAIL
 */
IDXResult DrsBPTreeIndex::shiftToNextKey(DrsIndexScanHandleInfo *info) {
	PageHandle *handle = info->m_pageHandle;
	IndexPage *page = (IndexPage*)handle->getPage();
	PageId nextPageId = INVALID_PAGE_ID;
	bool spanPage = false;
	bool forward = info->m_forward;
	u16 offset;
	SubRecord *key = info->m_cKey1;
	SubRecord *lastRecord = (info->m_findKey->m_format == KEY_COMPRESS ? info->m_findKey : info->m_cKey1);	// ���findKey��ʽ����ѹ����,˵�����ܾ����ض�λ�����ǵ�һ�β���,���������ʱ����ܵ�ǰһ����ֵ��ֻ����idxKey1
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

	// ��Ҫ��ҳ�����
	if (nextPageId == INVALID_PAGE_ID) {	// ɨ�赽�������߽磬���سɹ�
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

	assert(IndexKey::isKeyValid(key));	// һ����ȡ��������
	// �ͷ�ԭ��ҳ���Latch��Pin
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
 * ��lockHoldingLatch�����ɹ�������ҳ��LSN�ı��ʱ�򣬿����øú�����ȷ��������ֵ�ǲ��ǻ��ڵ�ǰҳ��
 * @pre info�ṹ����idxKey1�Ǳ��μ����ļ�ֵ����m_findKey�Ǳ������Ҫ��λ�ļ�ֵ��m_pageHandle�Ǳ�ʾ��������ҳ����Ϣ���
 * @post idxKey1����ᱣ�����������ļ�ֵ����Ϣ���������false��info�����keyInfo��Ϣ�����ϣ�����keyInfo��ʾ��ǰ�µ��ҵ���ֵ����Ϣ
 * @param info		ɨ����Ϣ���
 * @param inRange	out Ҫ���ҵļ�ֵ�Ƿ��ڵ�ǰҳ�淶Χ��
 * @return true��ʾҪ�����ļ�ֵ���ڵ�ǰҳ�棬false��ʾ����
 */
bool DrsBPTreeIndex::researchScanKeyInPage(DrsIndexScanHandleInfo *info, bool *inRange) {
	// ���ȱ�֤ҳ����Ч�ԣ���Ч���߿�ҳ�棬�϶��Ҳ���
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

	// Ψһ��ѯ������Ȳ�����Ч��������ܸպ��Ǳ������̸߳��£�����λ�ú�rowIdû�з����ı�
	if (info->m_uniqueScan)
		return (*inRange = (keyInfo->m_result == 0 && gotKey->m_rowId == origRowId));

	// ���ݲ��ҽ���ж��Ƿ�Ӧ��ȡ���ҵ���һ��
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

	// ��Χ��ѯ����λ�����ҷ�Χ��Ȼ����ݽ������֮�����rowIdһ�£�˵�����ڶ�λ��������Ч��ȷ�ģ�
	// ���rowId�޷�һ�¿϶���׼ȷ��������ԭ����λ���ɾ�����߸��£����߷�Χ֮���ֲ��������
	// ��֮��ʱ�������²���
	*inRange = page->isKeyInPageMiddle(keyInfo);
	return (*inRange && IndexKey::isKeyValid(gotKey) && gotKey->m_rowId == origRowId);
}


/**
 * �ڳ���һ��ҳ��Latch������£���ҳ��ĳ����ֵ����������������ɹ����ȳ�����Ҳ����Latch
 *
 * @param session			�Ự���
 * @param rowId				Ҫ������ֵ��rowId
 * @param lockMode			����ģʽ
 * @param pageHandle		INOUT	ҳ����
 * @param rowHandle			OUT		�������
 * @param mustReleaseLatch	������restart��ʱ���ǲ��ǿ��Բ���ҳ���pin
 * @param info				ɨ������������ҳ��LSN�ı��ʱ���жϲ������ǲ�����Ȼ�ڸ�ҳ�棬Ĭ��ֵΪNULL�����ΪNULL���Ͳ��������ж�
 * @return ���سɹ�IDX_SUCCESS��ʧ��IDX_FAIL����Ҫ���¿�ʼIDX_RESTART
 */
IDXResult DrsBPTreeIndex::lockHoldingLatch(Session *session, RowId rowId, LockMode lockMode, PageHandle **pageHandle, RowLockHandle **rowHandle, bool mustReleaseLatch, DrsIndexScanHandleInfo *info) {
	assert(lockMode != None && rowHandle != NULL);

	u16 tableId = ((DrsBPTreeIndice*)m_indice)->getTableId();
	if ((*rowHandle = TRY_LOCK_ROW(session, tableId, rowId, lockMode)) != NULL)
		return IDX_SUCCESS;

	// ������Ҫ���ͷű�ҳ���Latch������������
	LockMode curLatchMode = (*pageHandle)->getLockMode();
	IndexPage *page = (IndexPage*)(*pageHandle)->getPage();
	u64 oldLSN = page->m_lsn;
	session->unlockPage(pageHandle);

	SYNCHERE(SP_IDX_WAIT_TO_LOCK);

	*rowHandle = LOCK_ROW(session, tableId, rowId, lockMode);

	LOCK_PAGE_HANDLE(session, *pageHandle, curLatchMode);

	if (page->m_lsn == oldLSN)	// ��ϵͳ���ƺ�����assert(page->m_lsn != pageLSN)����Ϊ�����Ӳ��ϵļ�¼�϶��������̲߳�����
		return IDX_SUCCESS;

	// �ж�Ҫ���ҵ����ǲ��ǻ��ڵ�ǰҳ�浱��,�����,����Ҫrestart,ֻ��Ҫ���¶�λ���������λ��
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
	++m_indexStatus.m_numRLRestarts;	// ͳ����Ϣ

	session->unlockRow(rowHandle);

#ifdef TNT_ENGINE
	if(session->getTrans() == NULL) {		//�����ntse
		if (!mustReleaseLatch && inRange)	// ������ҵ��ļ�ֵ���ڸ�ҳ�棬����ֻ�ͷ�lock����pin�������ڸ�ҳ����Ҿ���
			session->unlockPage(pageHandle);
		else{
			session->releasePage(pageHandle);	
		}
	} else {								//�����tnt��������Σ�ֻҪ��Ҫ����ɨ�裬�ͱ����ͷ����ҳ��
		session->releasePage(pageHandle);
	}
#endif
	return IDX_RESTART;
}


/**
 * �ڳ���һ��ҳ��Latch������£���ȡ��һ��ҳ���Latch������ģʽ��ͬ
 *
 * �ڴ˹涨��Latch��������һ����˳�򣬼�����PageIdС��ҳ���Latch����ֱ�ӻ��PageId���ҳ���Latch��
 * ������Ҫ�ȴ����κ������ط�������ҳ���Latch�����̲��������﷢����ͻ
 * ����IDX_SUCCESS��ʾcurPage��newPage������Latch��pin��
 * ����IDX_RESTART����IDX_FAIL��ʾ����ҳ�涼û����Latch��pin
 *
 * @param session	�Ự���
 * @param newPageId	��ҳ���ID
 * @param curPageId ��ǰ����Latchҳ��ID
 * @param curHandle	��ǰҳ����
 * @param newHandle	��ҳ����	INOUT ����ֵΪ����Latch����ҳ��
 * @param latchMode	��Latch��ģʽ
 * @return ��Latch�ɹ�IDX_SUCCESS����Ҫ���¿�ʼIDX_RESTART
 */
IDXResult DrsBPTreeIndex::latchHoldingLatch(Session *session, PageId newPageId, PageId curPageId, PageHandle **curHandle, PageHandle **newHandle, LockMode latchMode) {
	assert(curPageId != newPageId);
	assert(newPageId != INVALID_PAGE_ID);

	File *file = ((DrsBPTreeIndice*)m_indice)->getFileDesc();

	if (curPageId < newPageId) {
		// ֱ�Ӽ�Latch
		*newHandle = GET_PAGE(session, file, PAGE_INDEX, newPageId, latchMode, m_dboStats, NULL);
		return IDX_SUCCESS;
	} else {
		SYNCHERE(SP_IDX_WANT_TO_GET_PAGE2);
		// ���ȳ���ֱ�Ӽ�Latch
		*newHandle = TRY_GET_PAGE(session, file, PAGE_INDEX, newPageId, latchMode, m_dboStats);
		if (*newHandle != NULL) {
			return IDX_SUCCESS;
		} else {
			++m_indexStatus.m_numLatchesConflicts; // ͳ����Ϣ
			// ���ͷű�ҳ��Latch�ټ���ҳ��Latch�����ڱ���Ҫ�ӻر�ҳ��Latch�ж�LSN�����ֻ�ͷ�latch����pin
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

				session->releasePage(newHandle);	// ���ͷ�IDС��ҳ��
				session->releasePage(curHandle);
				*newHandle = *curHandle = NULL;
				return IDX_RESTART;
			}
		}
	}
}


/**
 * ����ָ��ҳ���Latch
 *
 * @param session	�Ự���
 * @param handle	in/out Ҫ������ҳ�������ɹ�������Ϊ����X-Latch�ľ��������ΪNULL
 * @return �����ɹ�IDX_SUCCESS����Ҫ���¿�ʼIDX_RESTART
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
 * ���ݱ�ID�ͼ���ҳ�����IDƴ�ճ�һ��Ψһ�Ķ���ID���������
 * @param objectId ����ID
 * @return �ͱ�IDƴ�չ�����ʵ��������ID
 */
u64 DrsBPTreeIndex::getRealObjectId(u64 objectId) {
	return ((u64)m_tableDef->m_id << ((sizeof(u64) - sizeof(m_tableDef->m_id)) * 8)) | objectId;
}

/**
 * ���Լ�ĳ��������
 * @param session	�Ự���
 * @param objectId	��������
 * @return �����ɹ�true������false
 */
bool DrsBPTreeIndex::idxTryLockObject(Session *session, u64 objectId) {
	u64 lockId = getRealObjectId(objectId);
	return session->tryLockIdxObject(lockId);
}

/**
 * ��ĳ��������
 * @param session	�Ự���
 * @param objectId	��������
 * @return �����ɹ�true������false
 */
bool DrsBPTreeIndex::idxLockObject(Session *session, u64 objectId) {
	u64 lockId = getRealObjectId(objectId);
	return session->lockIdxObject(lockId);
}

/**
 * �����ͷ�ĳ��������
 * @param session	�Ự���
 * @param objectId	��������
 * @param token		�ͷŵ�����token������ڵ�ǰtoken
 * @return �����ɹ�true������false
 */
bool DrsBPTreeIndex::idxUnlockObject(Session *session, u64 objectId, u64 token) {
	u64 lockId = getRealObjectId(objectId);
	return session->unlockIdxObject(lockId, token);
}


/**
 * �����ڲ���ɾ��������λ��ҳ���ҳ����������ҳ�淢���ı�֮������жϺ���
 *
 * @param page		����ҳ�����
 * @param scaninfo	ɨ����
 * @param nullparam	���ò���
 * @return ����true��ʾ��ǰ����ҳ����Ч�ɹ���������Ҫ�ض�λ
 */
bool DrsBPTreeIndex::judgerForDMLLocatePage(IndexPage *page, void *scaninfo, void *nullparam) {
	DrsIndexScanHandleInfo *info = (DrsIndexScanHandleInfo*)scaninfo;
	UNREFERENCED_PARAMETER(nullparam);
	// ��ҳ�澭��SMO������£����ݼ�ֵ�жϵ�ǰҳ���ǲ��ǻ�����Ч
	// ������Լ���������ʾ����������޸Ĳ���һ�����������߻����ˣ��������ֻ�޸ĵ�һ���״̬
	if (page->isPageLeaf()) {
		assert(info != NULL);
		KeyInfo *keyInfo = &(info->m_keyInfo);
		// ����˴���makeFindKeyPadIfNecessary�н�����key0��findkey����ô������Ҫ�ٽ�������
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
 * �ڳ���ҳ��Latch������£���ĳ�����������ҳ��
 * @param session		�Ự���
 * @param objectId		���������ID
 * @param pageHandle	in/out ����ҳ����
 * @param intention		��������ͼ�����ڲ��뻹��ɾ����������
 * @param judger		���жϺ�������
 * @param param1		�������жϺ����Ĳ���1
 * @param param2		�������жϺ����Ĳ���2
 * @return �����ɹ�������IDX_SUCCESS��ҳ��LSN���ı�Ҫ�ض�λ����IDX_RESTART������ʧ������������IDX_FAIL
 */
IDXResult DrsBPTreeIndex::lockIdxObjectHoldingLatch(Session *session, u64 objectId, PageHandle **pageHandle, LockIdxObjIntention intention, lockIdxObjRejudge judger, void *param1, void *param2) {
	if (idxTryLockObject(session, objectId))
		return IDX_SUCCESS;

	// ����Ҫ���ͷ�Latch�ټ�����������
	u64 oldSMOLSN = ((IndexPage*)(*pageHandle)->getPage())->m_smoLSN;
	LockMode lockMode = (*pageHandle)->getLockMode();
	session->unlockPage(pageHandle);

	SYNCHERE(SP_IDX_WAIT_FOR_PAGE_LOCK);

	u64 token = session->getToken();
	if (!idxLockObject(session, objectId)) {
		// ��������
		session->unpinPage(pageHandle);
		return IDX_FAIL;
	}

	LOCK_PAGE_HANDLE(session, *pageHandle, lockMode);
	// ��������߼���ֻҪҳ��û�о������ѣ�ҳ�����ݻ��ǿ��ŵ�
	if (((IndexPage*)(*pageHandle)->getPage())->m_smoLSN == oldSMOLSN)
		return IDX_SUCCESS;
	// ���߾����жϺ����ж�
	if (judger != NULL && (this->*judger)((IndexPage*)(*pageHandle)->getPage(), param1, param2))
		return IDX_SUCCESS;

	if (intention == FOR_INSERT)
		++m_indexStatus.m_numILRestartsForI;
	else if (intention == FOR_DELETE)
		++m_indexStatus.m_numILRestartsForD;
	nftrace(ts.irl, tout << session->getId() << " lock " << objectId << " restart, oldSMOLSN: " << oldSMOLSN << " newSMOLsn: " << ((IndexPage*)(*pageHandle)->getPage())->m_smoLSN);

	// ����ҳ���Ѿ������ģ���Ҫ�ض�λ����������
	idxUnlockObject(session, objectId, token);
	session->releasePage(pageHandle);
	return IDX_RESTART;
}


/**
 * ���ָ��ҳ���SMOλ�Ƿ�����
 *
 * @param session		�Ự���
 * @param pageHandle	in/out ҳ����
 * @return ���ؼ��ɹ�����Ҫ���¿�ʼ���߳�����������ʧ��
 */
IDXResult DrsBPTreeIndex::checkSMOBit(Session *session, PageHandle **pageHandle) {
	IndexPage *page = (IndexPage*)(*pageHandle)->getPage();
	if (page->isPageSMO()) {
		u64 lockId = getRealObjectId(m_indexId);
		if (session->isLocked(lockId))
			return IDX_SUCCESS;

		session->unlockPage(pageHandle);
		// ��SMO����ʧ���ͷ�pin����IDX_FAIL���ɹ�������pin����IDX_RESTART
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
 * ��ȡ�����Ļ���ͳ����Ϣ
 * @return ��������ͳ����Ϣ
 */
const IndexStatus& DrsBPTreeIndex::getStatus() {
	IndicePageManager *pageManager = ((DrsBPTreeIndice*)m_indice)->getPagesManager();
	pageManager->updateNewStatus(m_indexId, &m_indexStatus);
	return m_indexStatus;
}

/**
 * ����������ݶ���ͳ����Ϣ 
 * @return ���ݶ���״̬
 */
DBObjStats* DrsBPTreeIndex::getDBObjStats() {
	return m_dboStats;
}

/**
 * ������������չͳ����Ϣ
 * @param session			�Ự���
 * @param maxSamplePages	������ҳ����
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
 * ��ȡ��������չͳ����Ϣ��ֻ�Ƿ���updateExtendStatus����õ���Ϣ�������²���ͳ�ƣ�
 * @return ��������չͳ����Ϣ
 */
const IndexStatusEx& DrsBPTreeIndex::getStatusEx() {
	return m_indexStatusEx;
}



///////////////////////////////////////////////////////////////////////////////////


/**
 * ��֤�������ṹ�Ĵ�����ȷ��
 * @pre ��֤��������ʱ���ᱻ�޸�
 * @param session	�Ự���
 * @param key1		���ڱ���Ƚϼ�ֵ�ı���1
 * @param key2		���ڱ���Ƚϼ�ֵ�ı���2
 * @param pkey		pad��ʽ�ļ�ֵ
 * @param fullCheck	�Ƿ�ִ���ϸ�ļ�ֵ��С���
 * @return ������֤�Ƿ�ɹ�
 */
bool DrsBPTreeIndex::verify(Session *session, SubRecord *key1, SubRecord *key2, SubRecord *pkey, bool fullCheck) {
	PageHandle *path[MAX_INDEX_LEVEL];
	IndexPage *pages[MAX_INDEX_LEVEL];
	PageId pageIds[MAX_INDEX_LEVEL];
	IndexPage *page;
	u16 offsets[MAX_INDEX_LEVEL];
	s32 level = 0;
	u16 totalLevel;

	// ���ȱ����������������ҳ�����ڵĿ��ͷ�
	path[level] = GET_PAGE(session, ((DrsBPTreeIndice*)m_indice)->getFileDesc(), PAGE_INDEX, m_rootPageId, Shared, m_dboStats, NULL);
	offsets[level] = INDEXPAGE_DATA_START_OFFSET;
	pageIds[level] = m_rootPageId;
	pages[level] = (IndexPage*)path[level]->getPage();

	IndexPage *rootPage = (IndexPage*)path[0]->getPage();
	totalLevel = rootPage->m_pageLevel;
	assert(rootPage->isPageRoot());

	while (true) {
		if (level < 0)	// ����ҳ�洦�����
			break;

		page = (IndexPage*)path[level]->getPage();
		assert(rootPage->m_pageLevel == totalLevel && page->m_pageLevel + level == totalLevel);
		assert(page->isPageRoot() || page->m_pageCount != 0);
		assert(page->m_pageCount == 0 || page->m_miniPageNum != 0);
		u16 offset = offsets[level];
		PageId pageId;

		if (page->m_pageLevel == 0) {	// ���Ҷҳ�����ȷ�Ի�˷
			if (fullCheck)
				page->traversalAndVerify(m_tableDef, m_indexDef, key1, key2, pkey);
			else
				page->traversalAndVerify(m_tableDef, m_indexDef, NULL, NULL, NULL);
			assert(page->isPageLeaf());
			session->releasePage(&path[level]);
			level--;
			continue;
		}

		if (offset == page->getDataEndOffset()) {	// ĳ����Ҷҳ���������
			// ���ҳ�汾�����ȷ��
			if (fullCheck)
				page->traversalAndVerify(m_tableDef, m_indexDef, key1, key2, pkey);
			else
				page->traversalAndVerify(m_tableDef, m_indexDef, NULL, NULL, NULL);
			session->releasePage(&path[level]);
			level--;
		} else {	// ��ȡ��һ��
			// ���ȵõ���һ���ֵ����
			page->fetchKeyByOffset(offset, &key1, &key2);

			offset = page->getNextKeyPageId(offset, &pageId);
			offsets[level] = offsets[level] + offset;

			level++;
			path[level] = GET_PAGE(session, ((DrsBPTreeIndice*)m_indice)->getFileDesc(), PAGE_INDEX, pageId, Shared, m_dboStats, NULL);	// ������������
			offsets[level] = INDEXPAGE_DATA_START_OFFSET;
			pageIds[level] = pageId;
			pages[level] = (IndexPage*)path[level]->getPage();

			// ���Ҷҳ��������
			IndexPage *indexPage = (IndexPage*)path[level]->getPage();
			assert(indexPage->m_pageLevel == page->m_pageLevel - 1);
			KeyInfo keyInfo;
			indexPage->getLastKey(key2, &keyInfo);

			RecordOper::convertKeyCP(m_tableDef, m_indexDef, key1, pkey);

			// ȷ��key1 >= key2
			if (offsets[level - 1] < page->getDataEndOffset()) {	// ÿ��ҳ������һ����Բ�����
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
 * ����һ����ֵ���õ���ֵ����·����Ϣ������Ҷҳ����Ϣ�����ڲ�ѯ��Χ����
 * @param key			Ҫ���ҵļ�ֵ
 * @param info			��ѯ��Ϣ����
 * @param flag			���ҷ���
 * @param singleRoot	out �����Ƿ�����ֻ��һ����ҳ��
 * @param fetchNext		out ���ز��Ҽ�ֵ����ڵ�ǰ����Ƿ�Ҫȡ��һ��
 * @param leafKeyCount	out ���Ҽ�ֵ��Ҷҳ�浱�еĵڼ���
 * @param leafPageCount	out ��ѯ��ֱ����Ҷҳ�湲�ж�����
 */
void DrsBPTreeIndex::estimateKeySearch(const SubRecord *key, DrsIndexScanHandleInfo *info, SearchFlag *flag, 
									   bool *singleRoot, bool *fetchNext, u16 *leafKeyCount, u16 *leafPageCount) {
	info->m_isFastComparable = (key == NULL) ? 
		RecordOper::isFastCCComparable(m_tableDef, m_indexDef, m_indexDef->m_numCols, m_indexDef->m_columns) : 
		RecordOper::isFastCCComparable(m_tableDef, m_indexDef, key->m_numCols, key->m_columns);

	MemoryContext *memoryContext = info->m_session->getMemoryContext();

	if (info->m_isFastComparable && key != NULL) {	// �������£����Խ���ֵת����C��ʽ���ٱȽϣ��������
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
		info->m_findKey->m_size = 0;	// ��ʾû�в��Ҽ�ֵ
	}

	PageHandle *handle = findSpecifiedLevel(info, flag, 0, IDX_ESTIMATE);
	assert(handle != NULL);	// ���ﲻӦ�ó�������
	*leafPageCount = ((IndexPage*)(handle->getPage()))->m_pageCount;
	*singleRoot = ((IndexPage*)(handle->getPage()))->isPageRoot();

	// ����Ҷҳ�涨λ�ļ�ֵƫ�����
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
 * ��DML�޸Ĳ���֮ǰ����ͳ����Ϣ
 * @param op	�µĲ���
 * @param times	���ӵ�ͳ�ƴ���
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
 * ��ʼһ�β���ͳ�ƹ���
 * @param session			�Ự���
 * @param wantSampleNum		��Ҫ������ҳ����
 * @param fastSample		�Ƿ��ǿ��ٲ������Ǳ�ʾ�ڴ�buffer����Ϊ�������ʾ����������Ϊ��
 * @return ��ʼ���õĲ������
 */
SampleHandle* DrsBPTreeIndex::beginSample(Session *session, uint wantSampleNum, bool fastSample) {
	ftrace(ts.idx, tout << session << wantSampleNum << fastSample);

	IndexSampleHandle *sampleHandle = new IndexSampleHandle(session, wantSampleNum, fastSample);
	if (fastSample) {	// ��ʾ��Ҫ��buffer�������
		Buffer *buffer = ((DrsBPTreeIndice*)m_indice)->getDatabase()->getPageBuffer();
		File *file = ((DrsBPTreeIndice*)m_indice)->getFileDesc();
		sampleHandle->m_bufferHandle = buffer->beginScan(session->getId(), file);
	}

	sampleHandle->m_key = IndexKey::allocSubRecord(session->getMemoryContext(), m_indexDef, KEY_COMPRESS);
	return sampleHandle;
}

/**
 * ȡ�ò�������һ��ҳ��
 * @param handle	�������
 * @return ����
 */
Sample* DrsBPTreeIndex::sampleNext(SampleHandle *handle) {
	IndexSampleHandle *idxHandle = (IndexSampleHandle*)handle;

	if (idxHandle->m_fastSample) {	// ����bufferΪ���Ĳ���
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

	// ���д���Ϊ���Ĳ���
	while (true) {
		if (idxHandle->m_eofIdx) {	// ����ɨ�����
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

		// ��ǰ����ǿ����ڱ���Χ����ȡ��һ��ҳ��
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
		if (spl == NULL) {	// ��ǰ����ʧ�ܣ����¿�ʼһ�β�����λ
			idxHandle->m_curRangePages = 0;
			idxHandle->m_curPageId = INVALID_PAGE_ID;
			if (idxHandle->m_runs == 0)	// �����һ�β�������û�õ�ҳ����������Ϊ�������������ܲ�����ҳ�棬��Ӧ�÷��ؿ�
				idxHandle->m_eofIdx = false;
			continue;
		}

		spl->m_ID = curPageId;
		++idxHandle->m_runs;
		return spl;
	}
}

/**
 * ����һ�β���
 * @param handle	�������
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
 * ���ж�ָ��ҳ��Ĳ���
 * @param session	�Ự���
 * @param page		ҳ��
 * @param tableDef	����
 * @param key		��ȡ��ֵʹ��
 * @return ������������ΪNULL����ʾ��ҳ�޷������ٲ���(ҳ��Ϊ�գ��޷�����ȡ���������һ��ҳ��)
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
 * ����һ�β�����Χ����ʼλ��
 * @param idxHandle	�����������
 * @return ���ط�Χ��������ʼҳ����
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
	// �õ���ҳ�沢���ҳ��SMO״̬
	parentPageId = m_rootPageId;
	PageHandle *parentHandle = GET_PAGE(session, file, PAGE_INDEX, parentPageId, Shared, m_dboStats, NULL);
	IndexPage *page = (IndexPage*)parentHandle->getPage();
	u16 rootLevel = page->m_pageLevel;
	IDX_SWITCH_AND_GOTO(checkSMOBit(session, &parentHandle), FindLevelStart, Fail);
	// �����������߶�,����ÿ��ɨ������������������

	while (page->m_pageLevel > 0) {
		assert(page->m_pageCount != 0 && page->m_miniPageNum != 0);
		// ȷ����ǰ��Ӧ��ȡ��һ�������λ
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
					// ����ǵ�һ�Σ�ֻ��Ҫ��λ����0�����ǵ�һ�Σ������Ƶı��������Ƿ����ȡ��һ��
					// �ڴ˻����ϣ������������������
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

		// ����ҳ��Latch���ͷŸ�ҳ��Latch
		PageHandle *childHandle = NULL;
		IDX_SWITCH_AND_GOTO(latchHoldingLatch(session, childPageId, parentPageId, &parentHandle, &childHandle, Shared), FindLevelStart, Fail);
		session->releasePage(&parentHandle);

		// �����ҳ���SMO״̬λ
		IDX_SWITCH_AND_GOTO(checkSMOBit(session, &childHandle), FindLevelStart, Fail);

		parentPageId = childPageId;
		parentHandle = childHandle;
		page = (IndexPage*)childHandle->getPage();
	}

	if (idxHandle->m_runs == 0) {	// ��Ҫ���²��ֲ�����Ϣ
		u64 estimateLeafPages = idxHandle->m_estimateLeafPages;
		if (idxHandle->m_maxSample >= estimateLeafPages || idxHandle->m_wantNum >= estimateLeafPages) {	// ��ʱӦ�ò�������ҳ��
			idxHandle->m_rangeSamplePages = estimateLeafPages;
			idxHandle->m_skipPages = 0;
		} else if (estimateLeafPages < idxHandle->m_rangeSamplePages * 2) {	// ����ҳ�������㣬����һ�β���
			idxHandle->m_skipPages = 0;
			idxHandle->m_rangeSamplePages = idxHandle->m_maxSample;
		} else if (estimateLeafPages > idxHandle->m_maxSample * 4) {	// ����ҳ��ԶС����ҳ����������skipҳ����
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
 * �ж�һ��ҳ���Ƿ��ǿɲ�����ҳ��
 * @param page		����ҳ��
 * @param pageId	ҳ��ID
 * @return ����ҳ���Ƿ���Ա�����
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
 * ����һ����������ͳ��
 */
void DrsBPTreeIndex::statisticDL() {
	m_indexStatus.m_numDeadLockRestarts.increment();
}

/** ��ԭ�����������滻Ϊ�µ���������
 * @param splitFactor	����ϵ��
 */
void DrsBPTreeIndex::setSplitFactor( s8 splitFactor ) {
	m_indexDef->m_splitFactor = splitFactor;
}

/**
 * ɨ������Ϣ�ṹ��ʼ�����캯��
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
 * ���������ʼ�����캯��
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
 * ��������ɨ������Ϣ����λ����Ҷҳ��ȡ������ֵ
 * ���봫���Ĳ��Ҽ�ֵfoundKey����KEY_COMPRESS�� foundKey���ⲿ����
 *
 * @param session			
 * @param foundKey	 OUT	����
 * @return �ɹ�����ʧ�ܣ�ʧ�ܱ�ʾ����Ϊ��
 */
bool DrsBPTreeIndex::locateLastLeafPageAndFindMaxKey(Session *session, SubRecord *foundKey) {
	//�ҵ�Ҷ�Ӳ�
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

		//����ҳ��latch���ͷŸ�ҳ��latch
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
	//���ҳ��Ϊ�գ��򷵻�false,����ȡҳ������ֵ
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

