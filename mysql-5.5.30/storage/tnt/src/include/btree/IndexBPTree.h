/**
* NTSE B+树索引实现类
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

typedef bool (DrsBPTreeIndex::*IndexScanCallBackFN)(DrsIndexScanHandleInfo *info, bool hasLatch);		/** 查询回调函数指针定义 */

typedef bool (DrsBPTreeIndex::*lockIdxObjRejudge)(IndexPage *page, void *param1, void *param2);	/** 在加索引对象锁时候使用的重判断函数指针定义 */

static const uint MAX_INDEX_LEVEL = Limits::MAX_BTREE_LEVEL;	/** 索引最大层数 */

enum LockIdxObjIntention {
	FOR_INSERT = 0,				/** 表示加锁是由插入操作引起的 */
	FOR_DELETE,					/** 表示加锁是由删除操作引起的 */
	FOR_OTHERS					/** 表示加锁是由其它操作引起的 */
};

struct IndexSearchTraceInfo {
public:
	u64	m_pageId;					/** 查找页面的ID */
	union pageInfo {
		struct searchInfo {
			u64	m_pageLSN;			/** 查找页面的当前LSN */
			u16 m_miniPageNo;		/** 查找定位项所在的MiniPage号 */
		} si;
		struct estimateInfo {
			u16 m_pageCount;		/** 当前页面的总项数 */
			u16 m_keyCount;			/** 查找键值在当前页面的项数 */
		} ei;
	} pi;
};

class DrsIndexScanHandleInfo {
public:
	DrsIndexScanHandleInfo(Session *session, const TableDef *tableDef, const IndexDef *indexDef, 
		LockMode lockMode, LockMode latchMode, bool isFastComparable, CompareKey comparator);

public:
	// 以下几个信息在每次取项或者跨页面操作时都需要及时保存
	PageId				m_pageId;								/** 当前扫描到页面的ID */	
	u64					m_pageLSN;								/** 当前扫描到页面的LSN */	
	u64					m_pageSMOLSN;							/** 当前扫描到页面的上一次SMO操作的LSN */
	BufferPageHandle	*m_pageHandle;							/** 当前扫描到的页面 */	
	RowLockHandle		**m_rowHandle;							/** 当前扫描加锁行的锁句柄 */
	struct KeyInfo		m_keyInfo;								/** 当前扫描到键值信息 */

	Session				*m_session;								/** 会话句柄 */
	KeyComparator		*m_comparator;							/** 索引比较函数 */
	SubRecord			*m_findKey;								/** 每次扫描要定位键值 */
	SubRecord			*m_key0;								/** 用在RangeScan第一次定位之后替换m_findKey，其他操作会配合m_findKey共同使用 */
	SubRecord			*m_cKey1;								/** 当前扫描到的记录，兼作扫描临时键值使用 
																	程序党中使用该变量的时候需要注意一旦执行类似findSpecifiedLevel的函数，
																	该键值的内容会被修改
																	*/
	SubRecord			*m_cKey2;								/** 用于当临时键值使用 */
	SubRecord			*m_cKey3;								/** 用于当临时键值使用 */
	const IndexDef		*m_indexDef;							/** 索引定义指针 */
	SubToSubExtractor	*m_extractor;							/** 需要从索引返回属性时用于提取属性的提取器 */
	LockMode			m_lockMode;								/** 当前扫描加锁模式 */	
	LockMode			m_latchMode;							/** 需要对页面加Latch的模式 */
	bool				m_forward;								/** 扫描方向 */	
	bool				m_includeKey;							/** 扫描键值范围，是否为>=,<= */
	bool				m_hasNext;								/** 表示当前是否扫描到某个方向结束 */
	bool				m_isFastComparable;						/** 表示该扫描能否使用快速比较 */
	bool				m_uniqueScan;							/** 表示扫描是唯一键值扫描 */
	bool				m_rangeFirst;							/** 如果是范围扫描，true表示当前是第一次定位，false表示至少两次getNext操作 */
	bool				m_everEqual;							/** 表明在searchpath过程中经过的非叶节点项是否遇到相等项 */
	u16					m_lastSearchPathLevel;					/** 保存最后一次searchPath到达的层次，和m_traceInfo相关，用于SearchParentInSMO*/
	struct IndexSearchTraceInfo	m_traceInfo[MAX_INDEX_LEVEL];	/** 保存搜索过程当中经历的页面信息 */
};


/** 实际使用的扫描句柄 */
class DrsIndexRangeScanHandle : public IndexScanHandle {
public:
	DrsIndexRangeScanHandle() : m_rowId(INVALID_ROW_ID), m_record(NULL) {
	}
	~DrsIndexRangeScanHandle() {}

	/**
	 * 初始化扫描句柄
	 * @param scanInfo	扫描信息结构指针
	 */
	void init(DrsIndexScanHandleInfo *scanInfo) {
		m_scanInfo = scanInfo;
	}

	/**
	 * 设置保存查找键值的SubRecord
	 *
	 * @param subRecord	扫描得到的索引键值，为NULL，表示只要取rowId
	 */
	inline void setSubRecord(SubRecord *subRecord) {
		m_record = subRecord;
	}
	/**
	 * 获得rowId
	 *
	 * @return 返回rowId
	 */
	inline RowId getRowId() const {
		return m_rowId;
	}

	/**
	 * 设置rowId
	 * @param rowId	扫描得到的rowId
	 */
	inline void setRowId(RowId rowId) {
		m_rowId = rowId;
	}

	/**
	 * 设置扫描信息
	 * @param scanInfo 扫描信息
	 */
	void setScanInfo(DrsIndexScanHandleInfo *scanInfo) {
		m_scanInfo = scanInfo;
	}

	/**
	 * 获得扫描信息
	 * @return 扫描信息
	 */
	DrsIndexScanHandleInfo* getScanInfo() {
		return m_scanInfo;
	}

	/**
	 * 保存找到的键值，如果m_record为NULL，表示不需要保存
	 *
	 * @param key		扫描找到的键值
	 */
	void saveKey(const SubRecord *key) {
		if (m_record == NULL)
			return;

		assert(key->m_format == KEY_COMPRESS);
		assert(m_scanInfo->m_extractor);
		m_scanInfo->m_extractor->extract(key, m_record);
	}

private:
	RowId	   m_rowId;	    /** 当前扫描到项的RowId */
	SubRecord  *m_record;	/** 当前读取的索引记录内容，此空间上层分配/释放，可以为NULL */
	DrsIndexScanHandleInfo *m_scanInfo;	/** 扫描信息结构 */
};

/** 用于TNT的扩展扫描句柄信息 */
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
	const TableDef      *m_tableDef;        /** 表定义 */
	PageId				m_readPageId;		/** 当前扫描到页面的ID */	
	u64					m_readPageLSN;		/** 当前扫描到页面的LSN */	
	u64					m_readPageSMOLSN;	/** 当前扫描到页面的上一次SMO操作的LSN */
	BufferPageHandle	*m_readPageHandle;	/** 当前扫描到的页面 */	
	struct KeyInfo		m_readKeyInfo;		/** 当前扫描到键值信息 */
	SubRecord           *m_readKey;         /** 读取到的下一条记录 */
	uint                m_findKeyBufSize;   /** 分配给查找键的缓存大小 */
	bool                m_forceSearchPage;  /** 是否时间戳相同也强制在叶页面中查找 */
	TLockMode           m_trxLockMode;      /** 事务锁模式 */
	bool				m_backupRangeFirst;	/** 保存rangeFirst变量的游标值用于索引RangeScan */
};

///////////////////////////////////////


class DrsBPTreeIndex : public DrsIndex {
public:
	DrsBPTreeIndex(DrsIndice *indice, const TableDef *tableDef, const IndexDef *indexDef, u8 indexId, u64 rootPageId);

	~DrsBPTreeIndex();

	// 对外接口
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

	// 采样接口
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
	// 索引遍历相关函数
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

	// 索引插入相关函数
	DrsIndexScanHandleInfo* prepareInsertScanInfo(Session *session, const SubRecord *key);
	bool insertIndexEntry(DrsIndexScanHandleInfo *info, bool *duplicateKey, bool checkDuplicate = true);
	void insertSMO(DrsIndexScanHandleInfo *info);
	bool insertSMOPrelockPages(Session *session, PageHandle **insertHandle);
	IDXResult insertCheckSame(DrsIndexScanHandleInfo *info, bool *hasSame);
	void loopInsertIfDeadLock(DrsIndexScanHandleInfo *scanInfo);

	// 索引删除相关函数
	bool deleteIndexEntry(DrsIndexScanHandleInfo *info, bool hasLatch);
	void deleteSMO(DrsIndexScanHandleInfo *info);
	bool deleteSMOPrelockPages(Session *session, PageHandle **deleteHandle);
	IndexPage *researchForDelete(DrsIndexScanHandleInfo *info);

	// 结构修改相关函数
	bool searchParentInSMO(DrsIndexScanHandleInfo *info, uint level, bool researched, PageId sonPageId, PageId *parentPageId, PageHandle **parentHandle);
	void updateNextPagePrevLink(Session *session, File *file, IndexLog *logger, PageId nextPageId, PageId prevLinkPageId);
	void updatePrevPageNextLink(Session *session, File *file, IndexLog *logger, PageId prevPageId, PageId nextLinkPageId);

	// 其他函数
	IDXResult lockHoldingLatch(Session *session, RowId rowId, LockMode lockMode, PageHandle **pageHandle, RowLockHandle **rowHandle, bool mustReleaseLatch, DrsIndexScanHandleInfo *info = NULL);
	bool researchScanKeyInPage(DrsIndexScanHandleInfo *info, bool *inRange);
	IDXResult latchHoldingLatch(Session *session, PageId newPageId, PageId curPageId, PageHandle **curPageHandle, PageHandle **newPageHandle, LockMode latchMode);
	IDXResult upgradeLatch(Session *session, PageHandle **pageHandle);
	IDXResult checkSMOBit(Session *session, PageHandle **pageHandle);
	IDXResult lockIdxObjectHoldingLatch(Session *session, u64 objectId, PageHandle **pageHandle, LockIdxObjIntention intention = FOR_OTHERS, lockIdxObjRejudge judger = NULL, void *param1 = NULL, void *param2 = NULL);

	bool judgerForDMLLocatePage(IndexPage *page, void *info, void *nullparam);
	//bool judgerForInsertCheckSame(IndexPage *page, void *neighbourPageId, void *spanNext);

	// 索引页面锁统一加锁接口
	u64 getRealObjectId(u64 objectId);
	bool idxTryLockObject(Session *session, u64 objectId);
	bool idxLockObject(Session *session, u64 objectId);
	bool idxUnlockObject(Session *session, u64 objectId, u64 token = 0);

private:
	DrsIndice		*m_indice;			/** 指向索引文件总管理指针 */
	IndexDef		*m_indexDef;		/** 索引定义 */
	const TableDef	*m_tableDef;		/** 对应表定义 */
	PageId			m_rootPageId;		/** 索引根页面的ID */
	u64				m_doneSMOTxnId;		/** 对SMO互斥锁加锁的事务号 */
	u8				m_indexId;			/** 当前索引唯一ID */

	IndexStatus		m_indexStatus;		/** 当前索引统计信息 */
	IndexStatusEx	m_indexStatusEx;	/** 当前索引扩展统计信息 */
	DBObjStats 		*m_dboStats;		/** 索引数据对象状态 */
};


class IndexSampleHandle : public SampleHandle {
public:
	IndexSampleHandle(Session *session, int wantNum, bool fastSample);

private:
	PageId m_curPageId;					/** 当前定位页面的PageId **/
	u64 m_estimateLeafPages;			/** 估计的叶页面的个数 */
	u64 m_rangeSamplePages;				/** 定位到某个叶页面之后范围扫描的页面数 */
	u64 m_skipPages;					/** 进行一次范围采样之后需要跳过的页面数 */
	u64 m_curRangePages;				/** 某次范围扫描已经使用的页面数，当超过m_rangeSamplePages，需要新定位一个范围 */
	u64 m_wantNum;						/** 指定需要采样的页面数 */
	uint m_runs;						/** 应该进行的范围采样次数，该值是一个估算值，不需要准确 */
	BufScanHandle* m_bufferHandle;		/** 缓存扫描句柄 */
	SubRecord *m_key;					/** 分配出来供采样的时候获取记录内容用 */
	u16 m_levelItems[MAX_INDEX_LEVEL];	/** 记录非叶节点的页面项数，用于估计总叶页面数，确定下一次range的起点 */
	bool m_eofIdx;						/** 采样到达索引叶页面的最右端 */

	static uint DEFAULT_MIN_SAMPLE_RANGE_PAGES;	/** 默认的连续采样范围页面数 */
	static uint DEFAULT_SAMPLE_RANGES;			/** 默认采样的范围个数 */

	friend class DrsBPTreeIndex;
};


}


#endif