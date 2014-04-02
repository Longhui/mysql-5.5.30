/**
* 内存索引接口
*
* @author 李伟钊(liweizhao@corp.netease.com)
*/
#ifndef _TNT_M_INDEX_
#define _TNT_M_INDEX_

#include "api/TNTDatabase.h"
#include "btree/IndexCommon.h"
#include "misc/Session.h"
#include "misc/TableDef.h"
#include "misc/Record.h"
#include "misc/DoubleChecker.h"

using namespace ntse;

namespace tnt {

class TNTDatabase;
class MIndex;


/** 索引基本统计信息（不一定精确） */
struct MIndexStatus {
//	DBObjStats	*m_dboStats;	/** 所有数据库对象共有的统计信息 */
	u64	m_dataLength;			/** 索引总占用的数据大小，单位字节数 */
	u64 m_freeLength;			/** 总占用空间中空闲的数据大小，单位字节数 */
	u64	m_numInsert;			/** 记录插入次数 */
	u64	m_numDelete;			/** 记录删除次数 */
 	u64	m_numScans;				/** 扫描次数 */
	u64	m_rowsScanned;			/** 扫描过的项数 */
	u64	m_backwardScans;		/** 反向扫描次数 */
	u64	m_rowsBScanned;			/** 反向扫描过的项数 */
	u64	m_numSplit;				/** 页面分裂次数 */
	u64	m_numMerge;				/** 页面合并次数 */
	u64 m_numRedistribute;		/** 页面重分配次数*/
	u64 m_numRestarts;		/** 重启的次数 */
	u64 m_numLatchesConflicts;	/** 加Latch出现冲突的次数 */
	u64 m_numRepairUnderflow; /** 修复下溢页面的次数 */
	u64 m_numRepairOverflow;	/** 修复上溢页面的次数 */
	u64 m_numIncreaseTreeHeight;/** 增加树高度操作的次数 */
	u64 m_numDecreaseTreeHeight;/** 降低树高度操作的次数 */
	Atomic<long> m_numAllocPage;			/** 申请页面总页面数 */
	Atomic<long> m_numFreePage;			/** 释放页面页面数 */
};


/** 索引项可见性信息 */
class KeyMVInfo {
public:
	bool m_visable; /** 是否可通过索引页面直接判断可见行，外存索引项的该项恒为false */
	u8   m_delBit;  /** 索引项删除标志位，外存索引项的该项恒为0 */
	RowIdVersion m_version; /** 索引项RowId版本，外存索引项的该项恒为0 */
	bool m_ntseReturned;	/** 索引项是从NTSE的索引中返回该项为true，否则从TNT返回则为false*/
};

/** DRS索引扫描句柄 */
class MIndexScanHandle {
public:
	/**
	 * 返回当前行的RID
	 *
	 * @return 当前行的RID
	 */
	virtual RowId getRowId() const = 0;
	virtual ~MIndexScanHandle() {}
};

/**
* 内存索引管理抽象接口
*/
class MIndice {
public:
	virtual ~MIndice() {}
	// 索引操作接口
	static MIndice* open(TNTDatabase *db, Session *session, uint indexNum, TableDef **tableDef, LobStorage *lobStorage, 
		const DoubleChecker *doubleChecker);
	virtual bool init(Session *session) = 0;
	virtual void close(Session *session) = 0;

	virtual void deleteIndexEntries(Session *session, const Record *record, RowIdVersion version) = 0;
	virtual bool updateIndexEntries(Session *session, const SubRecord *before, SubRecord *after) = 0;
	virtual bool insertIndexEntries(Session *session, SubRecord *after, RowIdVersion version) = 0;

	virtual void undoFirstUpdateOrDeleteIndexEntries(Session *session, const Record *record) = 0;
	virtual bool undoSecondUpdateIndexEntries(Session *session, const SubRecord *before, SubRecord *after) = 0;
	virtual void undoSecondDeleteIndexEntries(Session *session, const Record *record) = 0;

	virtual MIndex* createIndex(Session *session, const IndexDef *def, u8 indexId) = 0;
	virtual void dropIndex(Session *session, uint idxNo) = 0;

	virtual void setTableDef(TableDef **tableDef) = 0;
	virtual uint getIndexNum() const = 0;
	virtual MIndex* getIndex(uint index) const = 0;
	virtual u64 getMemUsed(bool includeMeta = true) = 0;
	virtual void setLobStorage(LobStorage *lobStorage) = 0;

	virtual void setDoubleChecker(const DoubleChecker *doubleChecker) = 0;
};

/**
 * 内存索引抽象接口
 */
class MIndex : public IndexBase {
public:
	virtual ~MIndex() {}

	static MIndex* open(TNTDatabase *db, Session *session, MIndice *mIndice, TableDef **tableDef, 
		const IndexDef *indexDef, u8 indexId, const DoubleChecker *doubleChecker);
	virtual bool init(Session *session, bool waitSuccess) = 0;
	virtual void close(Session *session) = 0;

	virtual void insert(Session *session, const SubRecord *key, RowIdVersion version) = 0;
	virtual bool delByMark(Session *session, const SubRecord *key, RowIdVersion *version) = 0;

	virtual void delRec(Session *session, const SubRecord *key, RowIdVersion *version) = 0;
	virtual bool undoDelByMark(Session *session, const SubRecord *key, RowIdVersion version) = 0;

// 	virtual bool getByUniqueKey(Session *session, const SubRecord *key, RowId *rowId, 
// 		SubRecord *subRecord, SubToSubExtractor *extractor) = 0;

#ifdef TNT_INDEX_OPTIMIZE
	virtual MIndexScanHandle* beginScanFast(Session *session, const SubRecord *key, 
		bool forward, bool includeKey) = 0;
	virtual bool getNextFast(MIndexScanHandle *scanHandle) = 0;
	virtual void endScanFast(MIndexScanHandle *scanHandle) = 0;
#endif
	virtual void setTableDef(TableDef **tableDef) = 0;
	virtual void setIndexDef(const IndexDef *indexDef) = 0;
	virtual u8 getIndexId() const = 0;
	virtual MIndexScanHandle* beginScan(Session *session, const SubRecord *key, bool forward, 
		bool includeKey, LockMode ntseRlMode, RowLockHandle **rowHdl, TLockMode trxLockMode) = 0;
	virtual bool getNext(MIndexScanHandle *scanHandle) throw(NtseException) = 0;
	virtual void endScan(MIndexScanHandle *scanHandle) = 0;

	virtual u64 recordsInRange(Session *session, const SubRecord *min, bool includeKeyMin, 
		const SubRecord *max, bool includeKeyMax) = 0;

	virtual u64 purge(Session *session, const ReadView *readView) = 0;
	virtual void reclaimIndex(Session *session, u32 hwm, u32 lwm) = 0;
	virtual bool checkDuplicate(Session *session, const SubRecord *key) = 0;

	virtual const MIndexStatus& getStatus() = 0;
};

}

#endif