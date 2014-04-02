/**
* BLink树实现相关
*
* @author 李伟钊(liweizhao@corp.netease.com)
*/
#ifndef _TNT_INDEX_BLINK_TREE_H_
#define _TNT_INDEX_BLINK_TREE_H_

#include "misc/MemCtx.h"
#include "btree/MIndex.h"
#include "btree/MIndexKey.h"
#include "btree/MIndexPage.h"
#include "misc/TNTIMPageManager.h"
#include "misc/DoubleChecker.h"

using namespace ntse;

namespace tnt {

/** 遍历模式 */
enum TraversalMode {
	READMODE,    /** 只读模式 */
	UPDATEMODE,  /** 更新模式 */
};

/** 内存索引扫描信息 */
class MIndexScanInfo {
public:
	MIndexScanInfo(Session *session, TNTTransaction *trx, const TableDef *tableDef, 
		const IndexDef *indexDef, LockMode latchMode, CompareKey comparator);
	~MIndexScanInfo() {}


public:
	Session			  *m_session;	  /** 会话句柄 */
	TNTTransaction      *m_trx;       /** 事务 */
	const IndexDef	  *m_indexDef;	  /** 索引定义指针 */

	TraversalMode     m_traversalMode;/** 页面遍历模式 */
	LockMode          m_latchMode;    /** 页面加latch模式 */
	MIndexPageHdl     m_currentPage;  /** 当前扫描到的页面 */
	u64               m_pageTimeStamp;/** 当前扫描到的页面的时间戳 */
	MkeyLocation      m_keyLocation;  /** 当前定位到的索引项在页面中的位置 */
	RowLockHandle	  **m_rowHandle;  /** 当前扫描中加的NTSE底层表行锁句柄 */
	
	KeyComparator	  *m_comparator;  /** 索引比较函数 */
	SubRecord		  *m_searchKey;	  /** 每次扫描要定位键值, 一定为KEY_NATURAL格式 */
	SubToSubExtractor *m_extractor;	  /** 需要从索引返回属性时用于提取属性的提取器 */
	
	SubRecord         *m_assistKey;   /** 用于比较过程当辅助键使用，一定为KEY_NATURAL格式 */
	
	bool			  m_forward;	  /** 扫描方向 */
	bool			  m_includeKey;	  /** 扫描键值范围，是否为>=,<= */
	bool			  m_uniqueScan;	  /** 表示扫描是唯一键值扫描 */

	u64               m_fetchCount;   /** 获取过的索引项计数 */
	bool              m_rangeFirst;   /** 是否是第一次查找 */

	bool			  m_hasNext;	  /** 表示当前是否扫描到某个方向结束 */
	SearchFlag        m_searchFlag;   /** 查找标志 */
};

/** 扩展的扫描句柄信息 */
class MIndexScanInfoExt : public MIndexScanInfo {
public:
	MIndexScanInfoExt(Session *session, TNTTransaction *trx, const TableDef *tableDef, 
		const IndexDef *indexDef, LockMode latchMode, CompareKey comparator, 
		LockMode ntseRlMode, TLockMode trxLockMode);
	~MIndexScanInfoExt() {}

	void checkFormat();
	void moveCursor();
	void moveCursorTo(const SubRecord *key);

public:
	MIndexPageHdl     m_readPage;         /** 当前扫描到的页面 */
	u64               m_readPageTimeStamp;/** 当前扫描到的页面的时间戳 */
	MkeyLocation      m_readKeyLocation;  /** 当前定位到的索引项在页面中的位置 */
	SubRecord         *m_readKey;         /** 存放读到的下一条索引项 */
	const TableDef    *m_tableDef;        /** 表定义 */
	uint              m_searchKeyBufSize; /** 分配给查找键的缓存大小 */
	bool              m_forceSearchPage;  /** 是否时间戳相同也强制在叶页面中查找 */
	LockMode          m_ntseRlMode;       /** 需要加的NTSE底层表行锁模式 */
	TLockMode         m_trxLockMode;      /** 事务锁模式 */
	bool              m_tmpRangeFirst;   /** 用于临时保存rangeFirst变量 */
};

/** 内存索引扫描句柄 */
class MIndexRangeScanHandle : public MIndexScanHandle {
public:
	MIndexRangeScanHandle() {
	}
	~MIndexRangeScanHandle() {}

	/**
	* 设置扫描信息
	* @param scanInfo 扫描信息
	*/
	void setScanInfo(MIndexScanInfo *scanInfo) {
		m_scanInfo = scanInfo;
	}

	/**
	* 获得扫描信息
	* @return 扫描信息
	*/
	MIndexScanInfo* getScanInfo() {
		return m_scanInfo;
	}

	/**
	 * 获得当前扫描到的索引项的RowId
	 * @return 
	 */
	RowId getRowId() const {
		return m_rowId;
	}

	/**
	 * 保存找到的键值，如果m_record为NULL，表示不需要保存
	 *
	 * @param key		扫描找到的键值
	 */
	void saveKey(const SubRecord *key) {
		assert(key->m_format == KEY_NATURAL);

		m_rowId = key->m_rowId;
	
		m_keyMVInfo.m_delBit = MIndexKeyOper::readDelBit(key);
		m_keyMVInfo.m_version = MIndexKeyOper::readIdxVersion(key);
		m_keyMVInfo.m_ntseReturned = false;

		MIndexScanInfoExt* scanInfoExt = (MIndexScanInfoExt*)m_scanInfo;
		if(scanInfoExt->m_trxLockMode != TL_NO)	//如果加锁模式不是TL_NO则是当前读
			m_keyMVInfo.m_visable = true;
		else {
			ReadView *readView = scanInfoExt->m_trx->getReadView();
			m_keyMVInfo.m_visable = scanInfoExt->m_readPage->getMaxTrxId() < readView->getUpTrxId();
		}
	}

	/**
	 * 获得当前扫描到的索引项的可见性信息
	 * @return 
	 */
	const KeyMVInfo& getMVInfo() const {
		return m_keyMVInfo;
	}

	inline void unlatchLastRow() {
		RowLockHandle **rlh = m_scanInfo->m_rowHandle;
		if (rlh && *rlh) {
			m_scanInfo->m_session->unlockRow(rlh);
		}
	}

private:
	MIndexScanInfo *m_scanInfo;	/** 扫描信息结构 */
	RowId          m_rowId;     /** 当前扫描到的索引项的RowId */
	KeyMVInfo      m_keyMVInfo; /** 当前扫描到的索引项的可见性信息 */
};



//////////////////////////////////////////////

/**
 * BLink树实现
 * 参考论文 “Concurrency control and recovery for balanced B-link trees”, 
 * The VLDB Journal (2005) 14: 257–277, Ibrahim Jaluta, Seppo Sippu, Eljas Soisalon-Soininen
 */
class BLinkTree : public MIndex {
public:
	BLinkTree(Session *session, MIndice *mIndice, TNTIMPageManager *pageManager, 
		const DoubleChecker *doubleChecker, TableDef **tableDef, 
		const IndexDef *indexDef, u8 indexId);
	virtual ~BLinkTree();
	bool init(Session *session, bool waitSuccess);
	void close(Session *session);

	/////////////////////////////////////////////////////////////////////////
	// 索引更新
	/////////////////////////////////////////////////////////////////////////
	void insert(Session *session, const SubRecord *key, RowIdVersion version);
	bool delByMark(Session *session, const SubRecord *key, RowIdVersion *version);
	u64 purge(Session *session, const ReadView *readView);
	void reclaimIndex(Session *session, u32 hwm, u32 lwm);

	void delRec(Session *session, const SubRecord *key, RowIdVersion *version);
	bool undoDelByMark(Session *session, const SubRecord *key, RowIdVersion version);

	/////////////////////////////////////////////////////////////////////////
	// 索引扫描
	/////////////////////////////////////////////////////////////////////////
// 	bool getByUniqueKey(Session *session, const SubRecord *key, RowId *rowId, 
// 		SubRecord *subRecord, SubToSubExtractor *extractor);

	bool checkDuplicate(Session *session, const SubRecord *key);

#ifdef TNT_INDEX_OPTIMIZE
	MIndexScanHandle* beginScanFast(Session *session, const SubRecord *key, bool forward, bool includeKey);
	bool getNextFast(MIndexScanHandle *scanHandle);
	void endScanFast(MIndexScanHandle *scanHandle);
#endif

	MIndexScanHandle* beginScan(Session *session, const SubRecord *key, bool forward, 
		bool includeKey, LockMode ntseRlMode, RowLockHandle **rowHdl, TLockMode trxLockMode);
	bool getNext(MIndexScanHandle *scanHandle) throw(NtseException);
	void endScan(MIndexScanHandle *scanHandle);

	u8 getIndexId() const;
	u8 getHeight() const;

	u64 recordsInRange(Session *session, const SubRecord *min, bool includeKeyMin, 
		const SubRecord *max, bool includeKeyMax);
	SampleHandle *beginSample(Session *session, uint wantSampleNum, bool fastSample);
	Sample * sampleNext(SampleHandle *handle);
	void endSample(SampleHandle *handle);
	DBObjStats* getDBObjStats();

#ifdef NTSE_UNIT_TEST
public:
#else
private:
#endif
	void realeaseLevel(Session *session, MIndexPageHdl firstPage);
	void initRootPage(Session *session);
	void releasePage(Session *session, MIndexPageHdl page, bool isToFreeList = false, bool clean = true);



	/////////////////////////////////////////////////////////////////////////
	// 页面和键值查找相关
	/////////////////////////////////////////////////////////////////////////
	MIndexScanInfo *generateScanInfo(Session *session, LockMode latchMode);
	MIndexPageHdl readModeLocateLeafPage(MIndexScanInfo *scanInfo);
	MIndexPageHdl updateModeLocateLeafPage(MIndexScanInfo *scanInfo);
	MIndexPageHdl updateModeTraverse(MIndexScanInfo *scanInfo, u8 level);
	MIndexPageHdl locateLeafPage(MIndexScanInfo *scanInfo);
	MIndexPageHdl getRootPage() const;

	bool fetchUnique(MIndexScanInfo *scanInfo, const SubRecord *key);
	bool loopForDupKey(MIndexScanInfo *scanInfo, 
		MkeyLocation *keyLocation, bool fetchNext);
	bool findDupKey(MIndexScanInfo *scanInfo);
	IDXResult searchAtLeafLevel(MIndexScanInfo *scanInfo, MkeyLocation *keyLocation, 
		const SearchFlag *searchFlag = &SearchFlag::DEFAULT_FLAG);
	IDXResult searchAtLeafLevelSecond(MIndexScanInfo *scanInfo, MkeyLocation *keyLocation, 
		const SearchFlag *searchFlag);
	bool saveScanInfo(MIndexScanInfo *scanInfo, MIndexSearchResult *sr);
//	bool judgeIfNeedRestart(MIndexScanInfo *info) const;
	bool checkHandlePage(MIndexScanInfoExt *info, bool forceSearchPage) const;
	IDXResult tryTrxLockHoldingLatch(MIndexScanInfoExt *info, MIndexPageHdl pageHdl, 
		const SubRecord *key) throw(NtseException);
	
	IDXResult shiftToNextKey(MIndexScanInfo *info, MkeyLocation *keyPos, 
		const SearchFlag *searchFlag, bool onlyRead = false);
	IDXResult shiftToForwardKey(MIndexScanInfo *info, MkeyLocation *keyPos, bool onlyRead = false);
	IDXResult shiftToBackwardKey(MIndexScanInfo *info, MkeyLocation *keyPos, bool onlyRead = false);
	IDXResult shiftToForwardPage(MIndexScanInfo *info, MIndexPageHdl currentPage, 
		MIndexPageHdl *nextPageHdl);
	IDXResult shiftToBackwardPage(MIndexScanInfo *info, MIndexPageHdl currentPage, 
		MIndexPageHdl *prevPageHdl);

	MIndexPageHdl switchToCoverPage(MIndexScanInfo *scanInfo, 
		MIndexPageHdl leftPage, MIndexPageHdl rightPage);

	MIndexPageHdl switchToCoverPageForLinkChild(MIndexScanInfo *scanInfo, 
		MIndexPageHdl leftPage, MIndexPageHdl rightPage, const SubRecord *includeKey);
	/////////////////////////////////////////////////////////////////////////
	// PURGE相关
	/////////////////////////////////////////////////////////////////////////
	uint repairLevel(MIndexScanInfo *scanInfo, MIndexPageHdl parentPage, u32 maxReclaimPages);
	void repairTree(MIndexScanInfo *scanInfo, u32 numReclaimPages);

	/////////////////////////////////////////////////////////////////////////
	// SMO操作相关
	/////////////////////////////////////////////////////////////////////////
	MIndexPageHdl linkChild(MIndexScanInfo *scanInfo, MIndexPageHdl parentPage, 
		MIndexPageHdl leftChildPage, MIndexPageHdl rightChildPage);
	void unlinkChild(MIndexScanInfo *scanInfo, MIndexPageHdl parentPage, MIndexPageHdl leftPage, 
		MIndexPageHdl rightPage, const SubRecord *leftPageKey, const SubRecord *rightPageKey);
	MIndexPageHdl splitPage(MIndexScanInfo *scanInfo, MIndexPageHdl pageToSplit, 
		MIndexPageHdl *newPageHdl, bool isForLinkChild = false, SubRecord *highKeyOfChildPageToBeLinked = NULL);
	MIndexPageHdl mergePage(MIndexScanInfo *info, MIndexPageHdl pageMergeTo, 
		MIndexPageHdl pageMergeFrom);
	bool addKeyOnLeafPage(MIndexScanInfo *scanInfo, SubRecord *addKey, 
		const MkeyLocation *keyLocation = NULL, MIndexPageHdl splitNewPage = NULL);

	MIndexPageHdl increaseTreeHeight(MIndexScanInfo *info);
	MIndexPageHdl decreaseTreeHeight(MIndexScanInfo *info, MIndexPageHdl childPage);
	MIndexPageHdl mergeOrRedistribute(MIndexScanInfo *scanInfo, MIndexPageHdl *parentPage, 
		MIndexPageHdl leftPage, MIndexPageHdl rightPage, SubRecord *leftPageKey = NULL, 
		SubRecord *rightPageKey = NULL);
	bool needRedistribute(MIndexPageHdl leftPage, MIndexPageHdl rightPage) const;
	MIndexPageHdl redistributeTwoPage(MIndexScanInfo *scanInfo, MIndexPageHdl *parentPage, 
		MIndexPageHdl leftPage, MIndexPageHdl rightPage);
	MIndexPageHdl repairRightMostChild(MIndexScanInfo *scanInfo, MIndexPageHdl *parentPage, 
		MIndexPageHdl childPage);
	MIndexPageHdl repairNotRightMostChild(MIndexScanInfo *scanInfo, MIndexPageHdl *parentPage, 
		MIndexPageHdl childPage);
	MIndexPageHdl repairPageOverflow(MIndexScanInfo *scanInfo, MIndexPageHdl *parentPage, 
		MIndexPageHdl overflowChildPage);
	MIndexPageHdl repairPageUnderFlow(MIndexScanInfo *scanInfo, MIndexPageHdl *parentPage, 
		MIndexPageHdl childPage);

	/////////////////////////////////////////////////////////////////////////
	// 统计信息相关
	/////////////////////////////////////////////////////////////////////////
	const MIndexStatus& getStatus();
		
	void setTableDef(TableDef **tableDef) {
		m_tableDef = tableDef;
	}

	void setIndexDef(const IndexDef *indexDef) {
		m_indexDef = indexDef;
	}

	/**
	 * 获取新的空闲页面
	 * @param session 会话
	 * @param force   是否即使超出目标大小也强制分配
	 * @return 空闲页面句柄
	 */
	inline MIndexPageHdl getFreeNewPage(Session *session, bool isWaitForFreePage, bool force = false) {
		MIndexPageHdl page = NULL;
		//FIXME: 当页面分配不出，不能拿着latch死等，需要重启算法
		if ((page = (MIndexPageHdl)m_pageManager->getPage(session->getId(), PAGE_MEM_INDEX, force)) == NULL) {
			session->getTNTDb()->freeMem(__FILE__, __LINE__);
			Thread::yield();
			if (!force) {
#ifndef NTSE_UNIT_TEST
				session->getTNTDb()->getTNTSyslog()->log(EL_LOG, "BLinkTree getFreeNewPage must alloc page force");
#endif
				if(!isWaitForFreePage)
				//此处不能死等，立即返回，在外层死等，重启算法
					return MIDX_PAGE_NONE;
				else
					page = (MIndexPageHdl)m_pageManager->getPage(session->getId(), PAGE_MEM_INDEX, true);
			}
		}
		NTSE_ASSERT(page);

		//统计信息
		m_indexStatus.m_numAllocPage.increment();

		return page;
	}

	/**
	 * 升级页面latch模式
	 * @param session 会话
	 * @param pageToLatch 内存索引页面句柄
	 * @return 是否升级成功
	 */
	bool upgradeLatch(Session *session, MIndexPageHdl pageToLatch);

	/**
	 * 如果页面为内存索引页面则加指定的latch
	 * @param session 会话
	 * @param page    页面句柄
	 * @param latchMode latch模式
	 * @return 页面类型发生变更则返回false，仍是内存索引页面则在加latch成功后返回true
	 */
	inline bool latchPageIfType(Session *session, MIndexPageHdl page, LockMode latchMode, 
		int timeoutMs = -1, const char *file = __FILE__, uint line = __LINE__) {
			return m_pageManager->latchPageIfType(session->getId(), page, latchMode, PAGE_MEM_INDEX,
				timeoutMs, file, line);
	}

	/**
	 * 对页面加latch
	 * @param session   会话
	 * @param page      内存索引页面句柄
	 * @param latchMode latch模式
	 */
	inline void latchPage(Session *session, MIndexPageHdl page, LockMode latchMode, 
		const char *file = __FILE__, uint line = __LINE__) {
			m_pageManager->latchPage(session->getId(), page, latchMode, file, line);
	}

	/**
	 * 尝试对页面加latch，不成功马上返回
	 * @param session   会话
	 * @param page      内存索引页面句柄
	 * @param latchMode latch模式
	 * @return 是否加latch成功
	 */
	inline bool tryLatchPage(Session *session, MIndexPageHdl page, LockMode latchMode, 
		const char *file = __FILE__, uint line = __LINE__) {
			return m_pageManager->tryLatchPage(session->getId(), page, latchMode, file, line);
	}

	/**
	 * 释放页面latch
	 * @param session   会话
	 * @param page      内存索引页面句柄
	 * @param latchMode latch模式
	 */
	inline void unlatchPage(Session *session, MIndexPageHdl page, LockMode latchMode) {
			return m_pageManager->unlatchPage(session->getId(), page, latchMode);
	}

	/**
	 * 尝试升级页面latch模式为Exclusive模式
	 * @param session 会话
	 * @param page    内存页面句柄
	 * @return 是否升级成功
	 */
	inline bool tryUpgradePage(Session *session, MIndexPageHdl page, const char *file = __FILE__, 
		uint line = __LINE__) {
			return m_pageManager->tryUpgradePageLatch(session->getId(), page, file, line);
	}

	/**
	 * 获得页面已经加的latch类型
	 * @param page
	 * @return 
	 */
	inline LockMode getPageLatchMode(MIndexPageHdl page) const {
		assert(MIDX_PAGE_NONE != page);
		return m_pageManager->getPageLatchMode(page);
	}

#ifdef NTSE_UNIT_TEST
	inline void setPageType(MIndexPageHdl page, PageType pageType) {
		m_pageManager->getPool()->setType(page, pageType);
	}
#endif

private:
	MIndice			 *m_mIndice;			/** 指向索引文件总管理指针 */
	TNTIMPageManager *m_pageManager;        /** TNTIM内存页面管理器 */
	TableDef		**m_tableDef;		    /** 对应表定义 */
	const IndexDef	 *m_indexDef;		    /** 索引定义 */
	u8				 m_indexId;			    /** 当前索引唯一ID */
	MIndexPage       *m_rootPage;		    /** 内存索引根页面 */
	const DoubleChecker *m_doubleChecker;   /** RowId校验器 */

	MIndexStatus		m_indexStatus;		/** 当前索引统计信息 */
	//IndexStatusEx	m_indexStatusEx;		/** 当前索引扩展统计信息 */
	//DBObjStats 		*m_dboStats;		/** 索引数据对象状态 */
};

#ifdef TNT_VERIFY_MINDEX
#define VERIFY_TNT_MINDEX_PAGE(page) \
	page->verifyPage(m_tableDef, m_indexDef)
#else
#define VERIFY_TNT_MINDEX_PAGE(page)
#endif

}

#endif
