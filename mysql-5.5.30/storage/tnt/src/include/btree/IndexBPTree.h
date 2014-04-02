/**
* NTSE B+������ʵ����
* 
* author: naturally (naturally@163.org), liweizhao(liweizhao@corp.netease.com)
*/

#ifndef _NTSE_INDEX_BPTREE_H_
#define _NTSE_INDEX_BPTREE_H_

#include "Index.h"
#include "IndexPage.h"
#include "IndexKey.h"
#include "util/Sync.h"
#include "misc/Record.h"
#include "string.h"

#ifdef TNT_ENGINE
#include "trx/TLock.h"
#endif

namespace ntse {

class IndexSampleHandle;
class IndexHeaderPage;
class Database;
class Buffer;
class IndexDef;
class DrsHeap;
class File;
class Connection;
class Session;
class BufferPageHandle;
class RowLockHandle;
class DrsIndexScanHandleInfo;
class DrsBPTreeIndex;
struct Mutex;
struct BufferPageHdr;
struct BufScanHandle;

#define Page BufferPageHdr
#define PageHandle BufferPageHandle

typedef bool (DrsBPTreeIndex::*IndexScanCallBackFN)(DrsIndexScanHandleInfo *info, bool hasLatch);		/** ��ѯ�ص�����ָ�붨�� */

typedef bool (DrsBPTreeIndex::*lockIdxObjRejudge)(IndexPage *page, void *param1, void *param2);	/** �ڼ�����������ʱ��ʹ�õ����жϺ���ָ�붨�� */

static const uint MAX_INDEX_LEVEL = Limits::MAX_BTREE_LEVEL;	/** ���������� */

enum LockIdxObjIntention {
	FOR_INSERT = 0,				/** ��ʾ�������ɲ����������� */
	FOR_DELETE,					/** ��ʾ��������ɾ����������� */
	FOR_OTHERS					/** ��ʾ��������������������� */
};

struct IndexSearchTraceInfo {
public:
	u64	m_pageId;					/** ����ҳ���ID */
	union pageInfo {
		struct searchInfo {
			u64	m_pageLSN;			/** ����ҳ��ĵ�ǰLSN */
			u16 m_miniPageNo;		/** ���Ҷ�λ�����ڵ�MiniPage�� */
		} si;
		struct estimateInfo {
			u16 m_pageCount;		/** ��ǰҳ��������� */
			u16 m_keyCount;			/** ���Ҽ�ֵ�ڵ�ǰҳ������� */
		} ei;
	} pi;
};

class DrsIndexScanHandleInfo {
public:
	DrsIndexScanHandleInfo(Session *session, const TableDef *tableDef, const IndexDef *indexDef, 
		LockMode lockMode, LockMode latchMode, bool isFastComparable, CompareKey comparator);

public:
	// ���¼�����Ϣ��ÿ��ȡ����߿�ҳ�����ʱ����Ҫ��ʱ����
	PageId				m_pageId;								/** ��ǰɨ�赽ҳ���ID */	
	u64					m_pageLSN;								/** ��ǰɨ�赽ҳ���LSN */	
	u64					m_pageSMOLSN;							/** ��ǰɨ�赽ҳ�����һ��SMO������LSN */
	BufferPageHandle	*m_pageHandle;							/** ��ǰɨ�赽��ҳ�� */	
	RowLockHandle		**m_rowHandle;							/** ��ǰɨ������е������ */
	struct KeyInfo		m_keyInfo;								/** ��ǰɨ�赽��ֵ��Ϣ */

	Session				*m_session;								/** �Ự��� */
	KeyComparator		*m_comparator;							/** �����ȽϺ��� */
	SubRecord			*m_findKey;								/** ÿ��ɨ��Ҫ��λ��ֵ */
	SubRecord			*m_key0;								/** ����RangeScan��һ�ζ�λ֮���滻m_findKey���������������m_findKey��ͬʹ�� */
	SubRecord			*m_cKey1;								/** ��ǰɨ�赽�ļ�¼������ɨ����ʱ��ֵʹ�� 
																	������ʹ�øñ�����ʱ����Ҫע��һ��ִ������findSpecifiedLevel�ĺ�����
																	�ü�ֵ�����ݻᱻ�޸�
																	*/
	SubRecord			*m_cKey2;								/** ���ڵ���ʱ��ֵʹ�� */
	SubRecord			*m_cKey3;								/** ���ڵ���ʱ��ֵʹ�� */
	const IndexDef		*m_indexDef;							/** ��������ָ�� */
	SubToSubExtractor	*m_extractor;							/** ��Ҫ��������������ʱ������ȡ���Ե���ȡ�� */
	LockMode			m_lockMode;								/** ��ǰɨ�����ģʽ */	
	LockMode			m_latchMode;							/** ��Ҫ��ҳ���Latch��ģʽ */
	bool				m_forward;								/** ɨ�跽�� */	
	bool				m_includeKey;							/** ɨ���ֵ��Χ���Ƿ�Ϊ>=,<= */
	bool				m_hasNext;								/** ��ʾ��ǰ�Ƿ�ɨ�赽ĳ��������� */
	bool				m_isFastComparable;						/** ��ʾ��ɨ���ܷ�ʹ�ÿ��ٱȽ� */
	bool				m_uniqueScan;							/** ��ʾɨ����Ψһ��ֵɨ�� */
	bool				m_rangeFirst;							/** ����Ƿ�Χɨ�裬true��ʾ��ǰ�ǵ�һ�ζ�λ��false��ʾ��������getNext���� */
	bool				m_everEqual;							/** ������searchpath�����о����ķ�Ҷ�ڵ����Ƿ���������� */
	u16					m_lastSearchPathLevel;					/** �������һ��searchPath����Ĳ�Σ���m_traceInfo��أ�����SearchParentInSMO*/
	struct IndexSearchTraceInfo	m_traceInfo[MAX_INDEX_LEVEL];	/** �����������̵��о�����ҳ����Ϣ */
};


/** ʵ��ʹ�õ�ɨ���� */
class DrsIndexRangeScanHandle : public IndexScanHandle {
public:
	DrsIndexRangeScanHandle() : m_rowId(INVALID_ROW_ID), m_record(NULL) {
	}
	~DrsIndexRangeScanHandle() {}

	/**
	 * ��ʼ��ɨ����
	 * @param scanInfo	ɨ����Ϣ�ṹָ��
	 */
	void init(DrsIndexScanHandleInfo *scanInfo) {
		m_scanInfo = scanInfo;
	}

	/**
	 * ���ñ�����Ҽ�ֵ��SubRecord
	 *
	 * @param subRecord	ɨ��õ���������ֵ��ΪNULL����ʾֻҪȡrowId
	 */
	inline void setSubRecord(SubRecord *subRecord) {
		m_record = subRecord;
	}
	/**
	 * ���rowId
	 *
	 * @return ����rowId
	 */
	inline RowId getRowId() const {
		return m_rowId;
	}

	/**
	 * ����rowId
	 * @param rowId	ɨ��õ���rowId
	 */
	inline void setRowId(RowId rowId) {
		m_rowId = rowId;
	}

	/**
	 * ����ɨ����Ϣ
	 * @param scanInfo ɨ����Ϣ
	 */
	void setScanInfo(DrsIndexScanHandleInfo *scanInfo) {
		m_scanInfo = scanInfo;
	}

	/**
	 * ���ɨ����Ϣ
	 * @return ɨ����Ϣ
	 */
	DrsIndexScanHandleInfo* getScanInfo() {
		return m_scanInfo;
	}

	/**
	 * �����ҵ��ļ�ֵ�����m_recordΪNULL����ʾ����Ҫ����
	 *
	 * @param key		ɨ���ҵ��ļ�ֵ
	 */
	void saveKey(const SubRecord *key) {
		if (m_record == NULL)
			return;

		assert(key->m_format == KEY_COMPRESS);
		assert(m_scanInfo->m_extractor);
		m_scanInfo->m_extractor->extract(key, m_record);
	}

private:
	RowId	   m_rowId;	    /** ��ǰɨ�赽���RowId */
	SubRecord  *m_record;	/** ��ǰ��ȡ��������¼���ݣ��˿ռ��ϲ����/�ͷţ�����ΪNULL */
	DrsIndexScanHandleInfo *m_scanInfo;	/** ɨ����Ϣ�ṹ */
};

/** ����TNT����չɨ������Ϣ */
class DrsIndexScanHandleInfoExt : public DrsIndexScanHandleInfo {
public:
	DrsIndexScanHandleInfoExt(Session *session, const TableDef *tableDef, const IndexDef *indexDef, 
		LockMode lockMode, LockMode latchMode, bool isFastComparable, CompareKey comparator, 
		TLockMode trxLockMode);
	~DrsIndexScanHandleInfoExt() {}
	void moveCursor();
	void moveCursorTo(const SubRecord *key);

protected:
	void checkFormat();

public:
	const TableDef      *m_tableDef;        /** ���� */
	PageId				m_readPageId;		/** ��ǰɨ�赽ҳ���ID */	
	u64					m_readPageLSN;		/** ��ǰɨ�赽ҳ���LSN */	
	u64					m_readPageSMOLSN;	/** ��ǰɨ�赽ҳ�����һ��SMO������LSN */
	BufferPageHandle	*m_readPageHandle;	/** ��ǰɨ�赽��ҳ�� */	
	struct KeyInfo		m_readKeyInfo;		/** ��ǰɨ�赽��ֵ��Ϣ */
	SubRecord           *m_readKey;         /** ��ȡ������һ����¼ */
	uint                m_findKeyBufSize;   /** ��������Ҽ��Ļ����С */
	bool                m_forceSearchPage;  /** �Ƿ�ʱ�����ͬҲǿ����Ҷҳ���в��� */
	TLockMode           m_trxLockMode;      /** ������ģʽ */
	bool				m_backupRangeFirst;	/** ����rangeFirst�������α�ֵ��������RangeScan */
};

///////////////////////////////////////


class DrsBPTreeIndex : public DrsIndex {
public:
	DrsBPTreeIndex(DrsIndice *indice, const TableDef *tableDef, const IndexDef *indexDef, u8 indexId, u64 rootPageId);

	~DrsBPTreeIndex();

	// ����ӿ�
	bool insert(Session *session, const SubRecord *key, bool *duplicateKey, bool checkDuplicate = true);
	bool insertGotPage(DrsIndexScanHandleInfo *info);
	void insertNoCheckDuplicate(Session *session, const SubRecord *key);

	bool del(Session *session, const SubRecord *key);
	bool getByUniqueKey(Session *session, const SubRecord *key, LockMode lockMode, RowId *rowId, 
		SubRecord *subRecord, RowLockHandle **rlh, SubToSubExtractor *extractor);
	IndexScanHandle* beginScan(Session *session, const SubRecord *key, bool forward, 
		bool includeKey, LockMode lockMode, RowLockHandle **rlh, SubToSubExtractor *extractor);
	bool getNext(IndexScanHandle *scanHandle, SubRecord *key);
	bool deleteCurrent(IndexScanHandle *scanHandle);
	void endScan(IndexScanHandle *scanHandle);
	u64 recordsInRange(Session *session, const SubRecord *min, bool includeKeyMin, 
		const SubRecord *max, bool includeKeyMax);

#ifdef TNT_ENGINE
	IndexScanHandle* beginScanSecond(Session *session, const SubRecord *key, bool forward, 
		bool includeKey, LockMode lockMode, RowLockHandle **rlh, SubToSubExtractor *extractor, 
		TLockMode trxLockMode);
	bool getNextSecond(IndexScanHandle *scanHandle) throw(NtseException);
	void endScanSecond(IndexScanHandle *scanHandle);

	bool checkDuplicate(Session *session, const SubRecord *key, DrsIndexScanHandleInfo **info);

	u64 recordsInRangeSecond(Session *session, const SubRecord *min, bool includeKeyMin, 
		const SubRecord *max, bool includeKeyMax);
#endif

	const IndexStatus& getStatus();
	void updateExtendStatus(Session *session, uint maxSamplePages);
	const IndexStatusEx& getStatusEx();
	void setSplitFactor(s8 splitFactor);

	// �����ӿ�
	SampleHandle *beginSample(Session *session, uint wantSampleNum, bool fastSample);
	Sample* sampleNext(SampleHandle *handle);
	void endSample(SampleHandle *handle);
	Sample* sample(Session *session, Page *page, const TableDef *tableDef, SubRecord *key);
	PageHandle* searchSampleStart(IndexSampleHandle *idxHandle);
	bool isPageSamplable(Page *page, PageId pageId);

	void statisticOp(IDXOperation op, int times);
	void statisticDL();

	u8 getIndexId();
	const IndexDef* getIndexDef();

	bool verify(Session *session, SubRecord *key1, SubRecord *key2, SubRecord *pkey, bool fullCheck);

	DBObjStats* getDBObjStats();

	bool locateLastLeafPageAndFindMaxKey(Session *session, SubRecord *foundKey);

private:
	// ����������غ���
	bool fetchNext(DrsIndexScanHandleInfo *info);
	bool fetchUnique(DrsIndexScanHandleInfo *info, IndexScanCallBackFN scanCallBack, bool *cbResult);
	PageHandle* findSpecifiedLevel(DrsIndexScanHandleInfo *scanInfo, SearchFlag *flag, uint level, FindType findType);
	bool locateLeafPageAndFindKey(DrsIndexScanHandleInfo *info, bool *needFetchNext, s32 *result, 
		bool forceSearchPage = false);
	IDXResult checkHandleLeafPage(DrsIndexScanHandleInfo *info, SearchFlag *flag, bool *needFetchNext, 
		bool forceSearchPage);
	IDXResult shiftToNextKey(DrsIndexScanHandleInfo *info);
	bool makeFindKeyPADIfNecessary(DrsIndexScanHandleInfo *info);
	void saveFoundKeyToFindKey(DrsIndexScanHandleInfo *info);
	void estimateKeySearch(const SubRecord *key, DrsIndexScanHandleInfo *info, SearchFlag *flag, bool *singleRoot, bool *fetchNext, u16 *leafKeyCount, u16 *leafPageCount);

#ifdef TNT_ENGINE
	bool fetchNextSecond(DrsIndexScanHandleInfoExt *info) throw(NtseException);
	PageHandle* findSpecifiedLevelSecond(DrsIndexScanHandleInfoExt *scanInfo, SearchFlag *flag, uint level, FindType findType);
	bool locateLeafPageAndFindKeySecond(DrsIndexScanHandleInfoExt *info, bool *needFetchNext, s32 *result, 
		bool forceSearchPage = false);
	IDXResult checkHandleLeafPageSecond(DrsIndexScanHandleInfoExt *info, SearchFlag *flag, bool *needFetchNext, 
		bool forceSearchPage);
	IDXResult readNextKey(DrsIndexScanHandleInfoExt *info) throw(NtseException);
	IDXResult tryTrxLockHoldingLatch(DrsIndexScanHandleInfoExt *info, PageHandle **pageHdl, 
		const SubRecord *key) throw(NtseException);
	IDXResult tryTrxLockHoldingTwoLatch(DrsIndexScanHandleInfoExt *info, PageHandle **pageHdl, PageHandle **pageHdl2,
		const SubRecord *key) throw(NtseException);
	bool researchScanKeyInPageSecond(DrsIndexScanHandleInfo *info, bool *inRange);
	IDXResult lockHoldingTwoLatch(Session *session, RowId rowId, LockMode lockMode, PageHandle **pageHandle, PageHandle **pageHandle2, RowLockHandle **rowHandle, bool mustReleaseLatch, DrsIndexScanHandleInfo *info = NULL);
#endif

	// ����������غ���
	DrsIndexScanHandleInfo* prepareInsertScanInfo(Session *session, const SubRecord *key);
	bool insertIndexEntry(DrsIndexScanHandleInfo *info, bool *duplicateKey, bool checkDuplicate = true);
	void insertSMO(DrsIndexScanHandleInfo *info);
	bool insertSMOPrelockPages(Session *session, PageHandle **insertHandle);
	IDXResult insertCheckSame(DrsIndexScanHandleInfo *info, bool *hasSame);
	void loopInsertIfDeadLock(DrsIndexScanHandleInfo *scanInfo);

	// ����ɾ����غ���
	bool deleteIndexEntry(DrsIndexScanHandleInfo *info, bool hasLatch);
	void deleteSMO(DrsIndexScanHandleInfo *info);
	bool deleteSMOPrelockPages(Session *session, PageHandle **deleteHandle);
	IndexPage *researchForDelete(DrsIndexScanHandleInfo *info);

	// �ṹ�޸���غ���
	bool searchParentInSMO(DrsIndexScanHandleInfo *info, uint level, bool researched, PageId sonPageId, PageId *parentPageId, PageHandle **parentHandle);
	void updateNextPagePrevLink(Session *session, File *file, IndexLog *logger, PageId nextPageId, PageId prevLinkPageId);
	void updatePrevPageNextLink(Session *session, File *file, IndexLog *logger, PageId prevPageId, PageId nextLinkPageId);

	// ��������
	IDXResult lockHoldingLatch(Session *session, RowId rowId, LockMode lockMode, PageHandle **pageHandle, RowLockHandle **rowHandle, bool mustReleaseLatch, DrsIndexScanHandleInfo *info = NULL);
	bool researchScanKeyInPage(DrsIndexScanHandleInfo *info, bool *inRange);
	IDXResult latchHoldingLatch(Session *session, PageId newPageId, PageId curPageId, PageHandle **curPageHandle, PageHandle **newPageHandle, LockMode latchMode);
	IDXResult upgradeLatch(Session *session, PageHandle **pageHandle);
	IDXResult checkSMOBit(Session *session, PageHandle **pageHandle);
	IDXResult lockIdxObjectHoldingLatch(Session *session, u64 objectId, PageHandle **pageHandle, LockIdxObjIntention intention = FOR_OTHERS, lockIdxObjRejudge judger = NULL, void *param1 = NULL, void *param2 = NULL);

	bool judgerForDMLLocatePage(IndexPage *page, void *info, void *nullparam);
	//bool judgerForInsertCheckSame(IndexPage *page, void *neighbourPageId, void *spanNext);

	// ����ҳ����ͳһ�����ӿ�
	u64 getRealObjectId(u64 objectId);
	bool idxTryLockObject(Session *session, u64 objectId);
	bool idxLockObject(Session *session, u64 objectId);
	bool idxUnlockObject(Session *session, u64 objectId, u64 token = 0);

private:
	DrsIndice		*m_indice;			/** ָ�������ļ��ܹ���ָ�� */
	IndexDef		*m_indexDef;		/** �������� */
	const TableDef	*m_tableDef;		/** ��Ӧ���� */
	PageId			m_rootPageId;		/** ������ҳ���ID */
	u64				m_doneSMOTxnId;		/** ��SMO����������������� */
	u8				m_indexId;			/** ��ǰ����ΨһID */

	IndexStatus		m_indexStatus;		/** ��ǰ����ͳ����Ϣ */
	IndexStatusEx	m_indexStatusEx;	/** ��ǰ������չͳ����Ϣ */
	DBObjStats 		*m_dboStats;		/** �������ݶ���״̬ */
};


class IndexSampleHandle : public SampleHandle {
public:
	IndexSampleHandle(Session *session, int wantNum, bool fastSample);

private:
	PageId m_curPageId;					/** ��ǰ��λҳ���PageId **/
	u64 m_estimateLeafPages;			/** ���Ƶ�Ҷҳ��ĸ��� */
	u64 m_rangeSamplePages;				/** ��λ��ĳ��Ҷҳ��֮��Χɨ���ҳ���� */
	u64 m_skipPages;					/** ����һ�η�Χ����֮����Ҫ������ҳ���� */
	u64 m_curRangePages;				/** ĳ�η�Χɨ���Ѿ�ʹ�õ�ҳ������������m_rangeSamplePages����Ҫ�¶�λһ����Χ */
	u64 m_wantNum;						/** ָ����Ҫ������ҳ���� */
	uint m_runs;						/** Ӧ�ý��еķ�Χ������������ֵ��һ������ֵ������Ҫ׼ȷ */
	BufScanHandle* m_bufferHandle;		/** ����ɨ���� */
	SubRecord *m_key;					/** ���������������ʱ���ȡ��¼������ */
	u16 m_levelItems[MAX_INDEX_LEVEL];	/** ��¼��Ҷ�ڵ��ҳ�����������ڹ�����Ҷҳ������ȷ����һ��range����� */
	bool m_eofIdx;						/** ������������Ҷҳ������Ҷ� */

	static uint DEFAULT_MIN_SAMPLE_RANGE_PAGES;	/** Ĭ�ϵ�����������Χҳ���� */
	static uint DEFAULT_SAMPLE_RANGES;			/** Ĭ�ϲ����ķ�Χ���� */

	friend class DrsBPTreeIndex;
};


}


#endif