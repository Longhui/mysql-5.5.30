/**
* 内外存索引接口封装
*
* @author 李伟钊(liweizhao@corp.netease.com)
*/
#ifndef _TNT_INDEX_H_
#define _TNT_INDEX_H_

#include "misc/Global.h"
#include "btree/Index.h"
#include "btree/MIndex.h"
#include "api/TNTDatabase.h"

using namespace ntse;

namespace ntse {
	class DrsIndexRangeScanHandle;
}

namespace tnt {

//尝试对索引键加唯一性键值锁
#define TRY_LOCK_UNIQUE(session, ukLockManager, key) \
	session->tryLockUniqueKey(ukLockManager, key, __FILE__, __LINE__)
//对索引键加唯一性键值锁
#define LOCK_UNIQUE(session, ukLockManager, key) \
	session->lockUniqueKey(ukLockManager, key, __FILE__, __LINE__)

class TNTDatabase;
class TNTIndex;
class MIndexRangeScanHandle;

/** TNT索引扫描状态 */
enum TNTIndexScanState {
	ON_DRS_INDEX, /** 扫描处于外存索引 */
	ON_MEM_INDEX, /** 扫描处于内存索引 */
	ON_NONE,      /** 扫描还未开始 */
};

struct TNTIndexStatus {
//	DBObjStats	*m_dboStats;	/** 所有数据库对象共有的统计信息 */
	u64	m_numScans;				/** 扫描次数 */
	u64	m_rowsScanned;			/** 扫描过的项数 */
	u64	m_backwardScans;		/** 反向扫描次数 */
	u64	m_rowsBScanned;			/** 反向扫描过的项数 */
	u64 m_numDrsReturn;			/** NTSE索引返回的项数 */
	u64 m_numMIdxReturn;		/** TNT索引返回的项数 */
	u64 m_numRLRestarts;		/** 加行锁出现冲突的次数 */
};

/** TNT索引扫描句柄信息 */
class TNTIdxScanHandleInfo {
public:
	TNTIdxScanHandleInfo(Session *session, const TableDef *tableDef, const IndexDef *indexDef, const SubRecord *searchKey, 
		SubRecord *redKey, SubToSubExtractor *extractor) {
		m_session  = session;
		m_tableDef = tableDef;
		m_indexDef = indexDef;
		m_scanState = ON_NONE;
		m_extractor = extractor; 
		m_searchKey = searchKey;
		m_currentKey = redKey;
		m_curKeyBufSize = redKey->m_size;
		m_pDIdxRlh = NULL;
		m_dIdxRlh = NULL;
		m_pMIdxRlh = NULL;
		m_mIdxRlh = NULL;
		m_rangeFirst= true;
		m_hasNext = true;
		m_trxLockMode = TL_NO;
	}

public:
	Session             *m_session;     /** 会话 */
	const TableDef      *m_tableDef;    /** 表定义 */
	const IndexDef		*m_indexDef;	/** 索引定义 */
	TNTIndexScanState   m_scanState;    /** 扫描状态 */
	SubToSubExtractor   *m_extractor;   /** 子记录提取器 */
	const SubRecord     *m_searchKey;   /** 查找键 */
	SubRecord           *m_currentKey;  /** 当前扫描游标指向的索引键 */
	uint                m_curKeyBufSize;/** 外部分配的索引键数据缓存的大小 */
	RowLockHandle       **m_pDIdxRlh;   /** 指向内存索引扫描中加的NTSE底层行锁句柄的指针 */
	RowLockHandle	    *m_dIdxRlh;     /** 内存索引扫描中加的NTSE底层行锁句柄 */
	RowLockHandle       **m_pMIdxRlh;   /** 指向外存索引扫描中加的NTSE底层行锁句柄的指针 */
	RowLockHandle	    *m_mIdxRlh;     /** 外存索引扫描中加的NTSE底层行锁句柄 */
	bool                m_rangeFirst;   /** 是否是第一次扫描 */
	bool                m_hasNext;      /** 是否还有下一项 */
	bool                m_isUnique;     /** 是否是索引唯一性扫描 */
	bool                m_isForward;    /** 是否是正向扫描 */
	TLockMode           m_trxLockMode;
};

/** TNT索引扫描句柄 */
class TNTIdxScanHandle {
	friend class TNTIndex;
public:
	TNTIdxScanHandle(DrsIndexRangeScanHandle *drsIdxScanHdl, MIndexRangeScanHandle *memIdxScanHdl) 
		: m_drsIdxScanHdl(drsIdxScanHdl), m_memIdxScanHdl(memIdxScanHdl), m_scanInfo(NULL) {
	}
	~TNTIdxScanHandle() {}

	void saveMemKey();
	void saveDrsKey();

	/**
	 * 设置扫描句柄信息
	 * @param scanInfo
	 */
	inline void setScanInfo(TNTIdxScanHandleInfo *scanInfo) {
		m_scanInfo = scanInfo;
	}

	/**
	 * 获取扫描句柄信息
	 * @return 
	 */
	inline TNTIdxScanHandleInfo* getScanInfo() const {
		return m_scanInfo;
	}

	/**
	 * 获取外存索引扫描句柄
	 * @return 
	 */
	inline DrsIndexRangeScanHandle * getDrsIndexScanHdl() const {
		return m_drsIdxScanHdl;
	}

	/**
	 * 获取内存索引扫描句柄
	 * @return 
	 */
	inline MIndexRangeScanHandle * getMemIdxScanHdl() const {
		return m_memIdxScanHdl;
	}

	/**
	 * 获取当前读取到的索引键的版本信息
	 * @return 
	 */
	inline const KeyMVInfo& getMVInfo() const {
		return m_keyMVInfo;
	}

	/**
	 * 获取当前读取的索引键的RowId
	 * @return 
	 */
	inline RowId getRowId() const {
		return m_scanInfo->m_currentKey->m_rowId;
	}

	/**
	 * 获得当前游标的位置，在内存索引还是在外存索引
	 * @return 
	 */
	inline TNTIndexScanState getScanState() const {
		return m_scanInfo->m_scanState;
	}

	inline void unlatchNtseRowBoth() {
		unlatchNtseRowDrs();
		unlatchNTseRowMem();
	}

	inline void unlatchNtseRowDrs() {
		if (m_scanInfo->m_dIdxRlh) {
			m_scanInfo->m_session->unlockRow(&m_scanInfo->m_dIdxRlh);
		}
	}

	inline void unlatchNTseRowMem() {
		if (m_scanInfo->m_mIdxRlh) {
			m_scanInfo->m_session->unlockRow(&m_scanInfo->m_mIdxRlh);
		}
	}

	bool retMemDrsKeyEqual();
	bool retDrsIndexKey();
	bool retMemIndexKey();
	void moveMemDrsIndexKey();
	void moveMemIndexKey();
	void moveDrsIndexKey();

private:
	bool isOutofRange(const SubRecord *key);

	/**
	 * 保存索引项可见性等信息
	 * @param mvInfo 可见性信息 
	 */
	inline void saveMVInfo(const KeyMVInfo &mvInfo) {
		m_keyMVInfo = mvInfo;
	}

	/**
	 * 保存索引项可见性等信息
	 * @param isViable 根据页面信息是否可直接判断可见性
	 * @param delBit   删除标志位
	 * @param version  索引项RowId版本
	 */
	inline void saveMVInfo(bool isViable = false, bool delBit = 0, RowIdVersion version = 0, bool ntseReturned = false) {
		m_keyMVInfo.m_visable = isViable;
		m_keyMVInfo.m_delBit = delBit;
		m_keyMVInfo.m_version = version;
		m_keyMVInfo.m_ntseReturned = ntseReturned;
	}

private:
	DrsIndexRangeScanHandle *m_drsIdxScanHdl;  /** 外存索引范围扫描句柄 */
	MIndexRangeScanHandle   *m_memIdxScanHdl;  /** 内存索引范围扫描句柄 */
	TNTIdxScanHandleInfo    *m_scanInfo;       /** 扫描信息 */
	KeyMVInfo               m_keyMVInfo;       /** 当前读取到的索引键的版本信息 */
};

/** TNT索引管理 */
class TNTIndice {
public:
	TNTIndice(TNTDatabase *db, TableDef **tableDef, LobStorage *lobStorage, DrsIndice *drsIndice, MIndice *memIndice);
	~TNTIndice();

	// 索引操作接口
	static void create(const char *path, const TableDef *tableDef) throw(NtseException);
	static void drop(const char *path) throw(NtseException);

	static TNTIndice* open(TNTDatabase *db, Session *session, TableDef **tableDef, LobStorage *lobStorage,
		DrsIndice *drsIndice, const DoubleChecker *doubleChecker);
	void close(Session *session, bool closeMIndice = true);
	void reOpen(TableDef **tableDef, LobStorage *lobStorage, DrsIndice *drsIndice);

	bool lockAllUniqueKey(Session *session, const Record *record, uint *dupIndex);
	bool lockUpdateUniqueKey(Session *session, const Record *record, u16 updateUniques, 
		const u16 *updateUniquesNo, uint *dupIndex);

	bool checkDuplicate(Session *session, const Record *record, u16 uniquesNum, 
		const u16 * uniquesNo, uint *dupIndex, DrsIndexScanHandleInfo **drsScanHdlInfo = NULL);

	void deleteIndexEntries(Session *session, const Record *record, RowIdVersion version);
	bool updateIndexEntries(Session *session, const SubRecord *before, SubRecord *after, bool isFirtsRound, RowIdVersion version);


	void undoFirstUpdateOrDeleteIndexEntries(Session *session, const Record *record);
	bool undoSecondUpdateIndexEntries(Session *session, const SubRecord *before, SubRecord *after);
	void undoSecondDeleteIndexEntries(Session *session, const Record *record);

	void dropPhaseOne(Session *session, uint idxNo);
	void dropPhaseTwo(Session *session, uint idxNo);
	void createIndexPhaseOne(Session *session, const IndexDef *indexDef, const TableDef *tblDef, 
		DrsHeap *heap) throw(NtseException);
	TNTIndex* createIndexPhaseTwo(Session *session, const IndexDef *def, uint idxNo);
	

	inline uint getIndexNum() const {
		return m_memIndice->getIndexNum();
	}

	inline uint getUniqueIndexNum() const {
		return m_drsIndice->getUniqueIndexNum();
	}

	inline DrsIndex *getDrsIndex(uint indexNo) const {
		return m_drsIndice->getIndex(indexNo);
	}

	inline MIndex *geMemIndex(uint indexNo) const {
		return m_memIndice->getIndex(indexNo);
	}

	inline DrsIndice* getDrsIndice() const {
		return m_drsIndice;
	}

	inline MIndice* getMemIndice() const {
		return  m_memIndice;
	}

	inline TNTIndex* getTntIndex(u8 indexNo) const {
		assert(indexNo < getIndexNum());
		return m_tntIndex[indexNo];
	}

	/**
	 * 根据要更新的后项，计算当前有哪些索引需要更新
	 * @param memoryContext		内存分配上下文，用来保存更新的索引序号数组，调用者注意空间的使用和释放
	 * @param update			更新操作的后项信息，更新字段有序排列
	 * @param updateNum			out 返回有多少个索引需要被更新
	 * @param updateIndices		out 指向memoryContext分配的空间，返回需要更新的索引序号，该序号和索引内部有序序列号一致
	 * @param updateUniques		out 返回多少唯一索引需要被更新
	 */
	inline void getUpdateIndices( MemoryContext *memoryContext, const SubRecord *update, u16 *updateNum, 
		u16 **updateIndices, u16 *updateUniques) {
			m_drsIndice->getUpdateIndices(memoryContext, update, updateNum, 
				updateIndices, updateUniques);
	}
	
private:
	/**
	 * 计算键值的校验和
	 * @param uniqueKey
	 * @return 
	 */
	inline u64 calcCheckSum(const SubRecord *uniqueKey) const {
		assert(KEY_NATURAL == uniqueKey->m_format);
		return checksum64(uniqueKey->m_data, uniqueKey->m_size);
	}

private:
	TNTDatabase			 *m_db;				/** 数据库 */
	TableDef			 **m_tableDef;		/** 所属表的定义 */
	LobStorage			 *m_lobStorage;		/** 所属表的大对象存储 */
	DrsIndice            *m_drsIndice;	    /** 各个外存索引 */
	MIndice              *m_memIndice;      /** 各个内存索引 */
	TNTIndex             **m_tntIndex;      /** TNT索引(内外存索引组合) */

	friend class TNTTblMntAlterIndex;
};

/** TNT索引 */
class TNTIndex {
public:
	TNTIndex(TNTDatabase *db, TableDef **tableDef, const IndexDef *indexDef, 
		TNTIndice *tntIndice, DrsIndex *drsIndex, MIndex *memIndex);
	~TNTIndex();

#ifdef TNT_INDEX_OPTIMIZE
	TNTIdxScanHandle* beginScanFast(Session *session, const SubRecord *key, bool forward, 
		bool includeKey, SubToSubExtractor *extractor = NULL);
	bool getNextFast(TNTIdxScanHandle *scanHandle);
	void endScanFast(TNTIdxScanHandle *scanHandle);
#endif

	TNTIdxScanHandle* beginScan(Session *session, const SubRecord *key, SubRecord *redKey, 
		bool unique, bool forward, bool includeKey, TLockMode trxLockMode, 
		SubToSubExtractor *extractor = NULL);
	bool getNext(TNTIdxScanHandle *scanHandle) throw(NtseException);
	void endScan(TNTIdxScanHandle *scanHandle);
	
	//暂时不提供唯一性扫描接口，外层需要进行唯一性扫描时统一用范围扫描的接口
	//bool getByUniqueKey(Session *session, MTransaction *trx, const SubRecord *key, RowId *rowId, 
	//	SubRecord *subRecord, SubToSubExtractor *extractor, bool *isVisable);

	u64 purge(Session *session, const ReadView *readView);

	void reclaimIndex(Session *session, u32 hwm, u32 lwm);

	inline DrsIndex* getDrsIndex() const {
		return m_drsIndex;
	}

	inline MIndex* getMIndex() const {
		return m_memIndex;
	}

	inline void setTableDef(TableDef **tableDef) {
		m_tableDef = tableDef;
	}

	/////////////////////////////////////////////////////////////////////////
	// 统计信息相关
	/////////////////////////////////////////////////////////////////////////
	const TNTIndexStatus& getStatus();

private:
	int compareKeys(const SubRecord *key1, const SubRecord *key2);

private:
	TableDef	   **m_tableDef;  /** 所属表定义 */
	const IndexDef *m_indexDef;  /** 所属索引定义 */
	TNTIndice      *m_tntIndice; /** TNT索引管理 */
	DrsIndex       *m_drsIndex;  /** 外存索引 */
	MIndex         *m_memIndex;  /** 内存索引 */
	TNTIndexStatus	   m_indexStatus;	/** 索引统计讯息 */
};

}

#endif