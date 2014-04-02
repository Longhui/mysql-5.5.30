/**
* BLink��ʵ�����
*
* @author ��ΰ��(liweizhao@corp.netease.com)
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

/** ����ģʽ */
enum TraversalMode {
	READMODE,    /** ֻ��ģʽ */
	UPDATEMODE,  /** ����ģʽ */
};

/** �ڴ�����ɨ����Ϣ */
class MIndexScanInfo {
public:
	MIndexScanInfo(Session *session, TNTTransaction *trx, const TableDef *tableDef, 
		const IndexDef *indexDef, LockMode latchMode, CompareKey comparator);
	~MIndexScanInfo() {}


public:
	Session			  *m_session;	  /** �Ự��� */
	TNTTransaction      *m_trx;       /** ���� */
	const IndexDef	  *m_indexDef;	  /** ��������ָ�� */

	TraversalMode     m_traversalMode;/** ҳ�����ģʽ */
	LockMode          m_latchMode;    /** ҳ���latchģʽ */
	MIndexPageHdl     m_currentPage;  /** ��ǰɨ�赽��ҳ�� */
	u64               m_pageTimeStamp;/** ��ǰɨ�赽��ҳ���ʱ��� */
	MkeyLocation      m_keyLocation;  /** ��ǰ��λ������������ҳ���е�λ�� */
	RowLockHandle	  **m_rowHandle;  /** ��ǰɨ���мӵ�NTSE�ײ��������� */
	
	KeyComparator	  *m_comparator;  /** �����ȽϺ��� */
	SubRecord		  *m_searchKey;	  /** ÿ��ɨ��Ҫ��λ��ֵ, һ��ΪKEY_NATURAL��ʽ */
	SubToSubExtractor *m_extractor;	  /** ��Ҫ��������������ʱ������ȡ���Ե���ȡ�� */
	
	SubRecord         *m_assistKey;   /** ���ڱȽϹ��̵�������ʹ�ã�һ��ΪKEY_NATURAL��ʽ */
	
	bool			  m_forward;	  /** ɨ�跽�� */
	bool			  m_includeKey;	  /** ɨ���ֵ��Χ���Ƿ�Ϊ>=,<= */
	bool			  m_uniqueScan;	  /** ��ʾɨ����Ψһ��ֵɨ�� */

	u64               m_fetchCount;   /** ��ȡ������������� */
	bool              m_rangeFirst;   /** �Ƿ��ǵ�һ�β��� */

	bool			  m_hasNext;	  /** ��ʾ��ǰ�Ƿ�ɨ�赽ĳ��������� */
	SearchFlag        m_searchFlag;   /** ���ұ�־ */
};

/** ��չ��ɨ������Ϣ */
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
	MIndexPageHdl     m_readPage;         /** ��ǰɨ�赽��ҳ�� */
	u64               m_readPageTimeStamp;/** ��ǰɨ�赽��ҳ���ʱ��� */
	MkeyLocation      m_readKeyLocation;  /** ��ǰ��λ������������ҳ���е�λ�� */
	SubRecord         *m_readKey;         /** ��Ŷ�������һ�������� */
	const TableDef    *m_tableDef;        /** ���� */
	uint              m_searchKeyBufSize; /** ��������Ҽ��Ļ����С */
	bool              m_forceSearchPage;  /** �Ƿ�ʱ�����ͬҲǿ����Ҷҳ���в��� */
	LockMode          m_ntseRlMode;       /** ��Ҫ�ӵ�NTSE�ײ������ģʽ */
	TLockMode         m_trxLockMode;      /** ������ģʽ */
	bool              m_tmpRangeFirst;   /** ������ʱ����rangeFirst���� */
};

/** �ڴ�����ɨ���� */
class MIndexRangeScanHandle : public MIndexScanHandle {
public:
	MIndexRangeScanHandle() {
	}
	~MIndexRangeScanHandle() {}

	/**
	* ����ɨ����Ϣ
	* @param scanInfo ɨ����Ϣ
	*/
	void setScanInfo(MIndexScanInfo *scanInfo) {
		m_scanInfo = scanInfo;
	}

	/**
	* ���ɨ����Ϣ
	* @return ɨ����Ϣ
	*/
	MIndexScanInfo* getScanInfo() {
		return m_scanInfo;
	}

	/**
	 * ��õ�ǰɨ�赽���������RowId
	 * @return 
	 */
	RowId getRowId() const {
		return m_rowId;
	}

	/**
	 * �����ҵ��ļ�ֵ�����m_recordΪNULL����ʾ����Ҫ����
	 *
	 * @param key		ɨ���ҵ��ļ�ֵ
	 */
	void saveKey(const SubRecord *key) {
		assert(key->m_format == KEY_NATURAL);

		m_rowId = key->m_rowId;
	
		m_keyMVInfo.m_delBit = MIndexKeyOper::readDelBit(key);
		m_keyMVInfo.m_version = MIndexKeyOper::readIdxVersion(key);
		m_keyMVInfo.m_ntseReturned = false;

		MIndexScanInfoExt* scanInfoExt = (MIndexScanInfoExt*)m_scanInfo;
		if(scanInfoExt->m_trxLockMode != TL_NO)	//�������ģʽ����TL_NO���ǵ�ǰ��
			m_keyMVInfo.m_visable = true;
		else {
			ReadView *readView = scanInfoExt->m_trx->getReadView();
			m_keyMVInfo.m_visable = scanInfoExt->m_readPage->getMaxTrxId() < readView->getUpTrxId();
		}
	}

	/**
	 * ��õ�ǰɨ�赽��������Ŀɼ�����Ϣ
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
	MIndexScanInfo *m_scanInfo;	/** ɨ����Ϣ�ṹ */
	RowId          m_rowId;     /** ��ǰɨ�赽���������RowId */
	KeyMVInfo      m_keyMVInfo; /** ��ǰɨ�赽��������Ŀɼ�����Ϣ */
};



//////////////////////////////////////////////

/**
 * BLink��ʵ��
 * �ο����� ��Concurrency control and recovery for balanced B-link trees��, 
 * The VLDB Journal (2005) 14: 257�C277, Ibrahim Jaluta, Seppo Sippu, Eljas Soisalon-Soininen
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
	// ��������
	/////////////////////////////////////////////////////////////////////////
	void insert(Session *session, const SubRecord *key, RowIdVersion version);
	bool delByMark(Session *session, const SubRecord *key, RowIdVersion *version);
	u64 purge(Session *session, const ReadView *readView);
	void reclaimIndex(Session *session, u32 hwm, u32 lwm);

	void delRec(Session *session, const SubRecord *key, RowIdVersion *version);
	bool undoDelByMark(Session *session, const SubRecord *key, RowIdVersion version);

	/////////////////////////////////////////////////////////////////////////
	// ����ɨ��
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
	// ҳ��ͼ�ֵ�������
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
	// PURGE���
	/////////////////////////////////////////////////////////////////////////
	uint repairLevel(MIndexScanInfo *scanInfo, MIndexPageHdl parentPage, u32 maxReclaimPages);
	void repairTree(MIndexScanInfo *scanInfo, u32 numReclaimPages);

	/////////////////////////////////////////////////////////////////////////
	// SMO�������
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
	// ͳ����Ϣ���
	/////////////////////////////////////////////////////////////////////////
	const MIndexStatus& getStatus();
		
	void setTableDef(TableDef **tableDef) {
		m_tableDef = tableDef;
	}

	void setIndexDef(const IndexDef *indexDef) {
		m_indexDef = indexDef;
	}

	/**
	 * ��ȡ�µĿ���ҳ��
	 * @param session �Ự
	 * @param force   �Ƿ�ʹ����Ŀ���СҲǿ�Ʒ���
	 * @return ����ҳ����
	 */
	inline MIndexPageHdl getFreeNewPage(Session *session, bool isWaitForFreePage, bool force = false) {
		MIndexPageHdl page = NULL;
		//FIXME: ��ҳ����䲻������������latch���ȣ���Ҫ�����㷨
		if ((page = (MIndexPageHdl)m_pageManager->getPage(session->getId(), PAGE_MEM_INDEX, force)) == NULL) {
			session->getTNTDb()->freeMem(__FILE__, __LINE__);
			Thread::yield();
			if (!force) {
#ifndef NTSE_UNIT_TEST
				session->getTNTDb()->getTNTSyslog()->log(EL_LOG, "BLinkTree getFreeNewPage must alloc page force");
#endif
				if(!isWaitForFreePage)
				//�˴��������ȣ��������أ���������ȣ������㷨
					return MIDX_PAGE_NONE;
				else
					page = (MIndexPageHdl)m_pageManager->getPage(session->getId(), PAGE_MEM_INDEX, true);
			}
		}
		NTSE_ASSERT(page);

		//ͳ����Ϣ
		m_indexStatus.m_numAllocPage.increment();

		return page;
	}

	/**
	 * ����ҳ��latchģʽ
	 * @param session �Ự
	 * @param pageToLatch �ڴ�����ҳ����
	 * @return �Ƿ������ɹ�
	 */
	bool upgradeLatch(Session *session, MIndexPageHdl pageToLatch);

	/**
	 * ���ҳ��Ϊ�ڴ�����ҳ�����ָ����latch
	 * @param session �Ự
	 * @param page    ҳ����
	 * @param latchMode latchģʽ
	 * @return ҳ�����ͷ�������򷵻�false�������ڴ�����ҳ�����ڼ�latch�ɹ��󷵻�true
	 */
	inline bool latchPageIfType(Session *session, MIndexPageHdl page, LockMode latchMode, 
		int timeoutMs = -1, const char *file = __FILE__, uint line = __LINE__) {
			return m_pageManager->latchPageIfType(session->getId(), page, latchMode, PAGE_MEM_INDEX,
				timeoutMs, file, line);
	}

	/**
	 * ��ҳ���latch
	 * @param session   �Ự
	 * @param page      �ڴ�����ҳ����
	 * @param latchMode latchģʽ
	 */
	inline void latchPage(Session *session, MIndexPageHdl page, LockMode latchMode, 
		const char *file = __FILE__, uint line = __LINE__) {
			m_pageManager->latchPage(session->getId(), page, latchMode, file, line);
	}

	/**
	 * ���Զ�ҳ���latch�����ɹ����Ϸ���
	 * @param session   �Ự
	 * @param page      �ڴ�����ҳ����
	 * @param latchMode latchģʽ
	 * @return �Ƿ��latch�ɹ�
	 */
	inline bool tryLatchPage(Session *session, MIndexPageHdl page, LockMode latchMode, 
		const char *file = __FILE__, uint line = __LINE__) {
			return m_pageManager->tryLatchPage(session->getId(), page, latchMode, file, line);
	}

	/**
	 * �ͷ�ҳ��latch
	 * @param session   �Ự
	 * @param page      �ڴ�����ҳ����
	 * @param latchMode latchģʽ
	 */
	inline void unlatchPage(Session *session, MIndexPageHdl page, LockMode latchMode) {
			return m_pageManager->unlatchPage(session->getId(), page, latchMode);
	}

	/**
	 * ��������ҳ��latchģʽΪExclusiveģʽ
	 * @param session �Ự
	 * @param page    �ڴ�ҳ����
	 * @return �Ƿ������ɹ�
	 */
	inline bool tryUpgradePage(Session *session, MIndexPageHdl page, const char *file = __FILE__, 
		uint line = __LINE__) {
			return m_pageManager->tryUpgradePageLatch(session->getId(), page, file, line);
	}

	/**
	 * ���ҳ���Ѿ��ӵ�latch����
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
	MIndice			 *m_mIndice;			/** ָ�������ļ��ܹ���ָ�� */
	TNTIMPageManager *m_pageManager;        /** TNTIM�ڴ�ҳ������� */
	TableDef		**m_tableDef;		    /** ��Ӧ���� */
	const IndexDef	 *m_indexDef;		    /** �������� */
	u8				 m_indexId;			    /** ��ǰ����ΨһID */
	MIndexPage       *m_rootPage;		    /** �ڴ�������ҳ�� */
	const DoubleChecker *m_doubleChecker;   /** RowIdУ���� */

	MIndexStatus		m_indexStatus;		/** ��ǰ����ͳ����Ϣ */
	//IndexStatusEx	m_indexStatusEx;		/** ��ǰ������չͳ����Ϣ */
	//DBObjStats 		*m_dboStats;		/** �������ݶ���״̬ */
};

#ifdef TNT_VERIFY_MINDEX
#define VERIFY_TNT_MINDEX_PAGE(page) \
	page->verifyPage(m_tableDef, m_indexDef)
#else
#define VERIFY_TNT_MINDEX_PAGE(page)
#endif

}

#endif
