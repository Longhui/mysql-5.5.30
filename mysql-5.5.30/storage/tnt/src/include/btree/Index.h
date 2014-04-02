/**
* 索引管理接口
*
* @author 汪源(wangyuan@corp.netease.com, wy@163.org)
*/

#ifndef _NTSE_INDEX_H_
#define _NTSE_INDEX_H_

#include "btree/IndexCommon.h"
#include "misc/Global.h"
#include "util/Sync.h"
#include "util/PagePool.h"
#include "misc/Buffer.h"
#include "misc/Sample.h"
#include "misc/Session.h"

#ifdef TNT_ENGINE
#include "trx/TLock.h"
#endif

namespace ntse {

class Session;
class Database;
class TableDef;
class IndexDef;
class DrsHeap;
class Record;
class SubRecord;
class File;
class DrsIndex;
class MemoryContext;
class IndexScanHandle;
class RowLockHandle;
class OrderedIndices;
class DrsIndexScanHandleInfo;

/** 索引基本统计信息（不一定精确） */
struct IndexStatus {
	DBObjStats	*m_dboStats;	/** 所有数据库对象共有的统计信息 */
	u64	m_dataLength;			/** 索引总占用的数据大小，单位字节数 */
	u64 m_freeLength;			/** 总占用空间中空闲的数据大小，单位字节数 */
	u64	m_numInsert;			/** 记录插入次数 */
	u64	m_numUpdate;			/** 记录更新次数 */
	u64	m_numDelete;			/** 记录删除次数 */
	u64	m_numScans;				/** 扫描次数 */
	u64	m_rowsScanned;			/** 扫描过的项数 */
	u64	m_backwardScans;		/** 反向扫描次数 */
	u64	m_rowsBScanned;			/** 反向扫描过的项数 */
	u64	m_numSplit;				/** 页面分裂次数 */
	u64	m_numMerge;				/** 页面合并次数 */
	u64 m_numRLRestarts;		/** 加行锁出现冲突的次数 */
	u64 m_numILRestartsForI;	/** 加索引页面锁出现冲突的次数，冲突是由插入操作引起 */
	u64 m_numILRestartsForD;	/** 加索引页面锁出现冲突的次数，冲突是由删除操作引起 */
	u64 m_numLatchesConflicts;	/** 加Latch出现冲突的次数 */
	Atomic<long> m_numDeadLockRestarts;	/** DML过程当中出现死锁，但是需要restartDML修改的统计次数 */
};

/** 索引扩展统计信息（通过采样获得，不精确） */
struct IndexStatusEx {
	double	m_pctUsed;			/** 页面利用率 */
	double	m_compressRatio;	/** 压缩比（压缩后/压缩前大小之比） */
	static const int m_fieldNum = 3;/** 本统计信息的域数 */
};

/** DRS索引扫描句柄 */
class IndexScanHandle {
public:
	/**
	 * 返回当前行的RID
	 *
	 * @return 当前行的RID
	 */
	virtual RowId getRowId() const = 0;
	virtual ~IndexScanHandle() {}
};

/** 管理属于一个表的所有DRS索引 */
class DrsIndice {
public:
	virtual ~DrsIndice() {};

	// 索引操作接口
	static void create(const char *path, const TableDef *tableDef) throw(NtseException);
	static DrsIndice* open(Database *db, Session *session, const char *path, const TableDef *tableDef, LobStorage *lobStorage) throw(NtseException);
	static void drop(const char *path) throw(NtseException);

	virtual void close(Session *session, bool flushDirty) = 0;
	virtual void flush(Session *session) = 0;

	virtual bool insertIndexEntries(Session *session, const Record *record, uint *dupIndex) = 0;
#ifdef TNT_ENGINE
	virtual void insertIndexNoDupEntries(Session *session, const Record *record) = 0;
#endif
	virtual void deleteIndexEntries(Session *session, const Record *record, IndexScanHandle* scanHandle) = 0;
	virtual bool updateIndexEntries(Session *session, const SubRecord *before, SubRecord *after, bool updateLob, uint *dupIndex) = 0;
	virtual void dropPhaseOne(Session *session, uint idxNo) = 0;
	virtual void dropPhaseTwo(Session *session, uint idxNo) = 0;
	virtual void createIndexPhaseOne(Session *session, const IndexDef *indexDef, const TableDef *tblDef, 
		DrsHeap *heap) throw(NtseException) = 0;
	virtual DrsIndex* createIndexPhaseTwo(const IndexDef *def) = 0;

	virtual DrsIndex* getIndex(uint index) = 0;
	virtual int getFiles(File** files, PageType* pageTypes, int numFile) = 0;
	virtual uint getIndexNum() const = 0;
	virtual uint getUniqueIndexNum() const = 0;
	virtual u64 getDataLength(bool includeMeta = true) = 0;
	virtual int getIndexNo(const BufferPageHdr *page, u64 pageId) = 0;
	virtual const OrderedIndices* getOrderedIndices() const { return NULL; };
	virtual LobStorage* getLobStorage() = 0;

	// 恢复接口
	// redo
	static void redoCreate(const char *path, const TableDef *tableDef) throw(NtseException);
	virtual void redoCreateIndexEnd(const byte *log, uint size) = 0;		// 索引创建结束，需要同步内存和文件当中的数据状态
	virtual s32 redoDropIndex(Session *session, u64 lsn, const byte *log, uint size) = 0;			// 丢弃一个指定索引，日志记录索引号 

	virtual void redoDML(Session *session, u64 lsn, const byte *log, uint size) = 0;
	virtual void redoSMO(Session *session, u64 lsn, const byte *log, uint size) = 0;		// 处理页面合并和分裂
	virtual void redoPageSet(Session *session, u64 lsn, const byte *log, uint size) = 0;	// 包括SMO位设置以及页面初始化、位图页和头页面使用记录
	virtual bool isIdxDMLSucceed(const byte *log, uint size) = 0;
	virtual u8 getLastUpdatedIdxNo(const byte *log, uint size) = 0;

	// undo
	virtual void undoDML(Session *session, u64 lsn, const byte *log, uint size, bool logCPST) = 0;
	virtual void undoSMO(Session *session, u64 lsn, const byte *log, uint size, bool logCPST) = 0;
	virtual void undoPageSet(Session *session, u64 lsn, const byte *log, uint size, bool logCPST) = 0;	// 包括SMO位设置以及页面初始化
	virtual void undoCreateIndex(Session *session, u64 lsn, const byte *log, uint size, bool logCPST) = 0;

	// redo补偿日志
	virtual void redoCpstDML(Session *session, u64 lsn, const byte *log, uint size) = 0;
	virtual void redoCpstCreateIndex(Session *session, u64 lsn, const byte *log, uint size) = 0;
	virtual void redoCpstSMO(Session *session, u64 lsn, const byte *log, uint size) = 0;		// 处理页面合并和分裂
	virtual void redoCpstPageSet(Session *session, u64 lsn, const byte *log, uint size) = 0;	// 包括SMO位设置以及页面初始化、位图页和头页面

	virtual DBObjStats* getDBObjStats() = 0;

	// 补充恢复接口
	virtual void recvInsertIndexEntries(Session *session, const Record *record, s16 lastDoneIdxNo, u64 beginLSN) = 0;
	virtual void recvDeleteIndexEntries(Session *session, const Record *record, s16 lastDoneIdxNo, u64 beginLSN) = 0;
	virtual void recvUpdateIndexEntries(Session *session, const SubRecord *before, const SubRecord *after, 
		s16 lastDoneIdxNo, u64 beginLSN, bool isUpdateLob) = 0;
	virtual u8 recvCompleteHalfUpdate(Session *session, const SubRecord *record, s16 lastDoneIdxNo, 
		u16 updateIdxNum, u16 *updateIndices, bool isUpdateLob) = 0;

	virtual void logDMLDoneInRecv(Session *session, u64 beginLSN, bool succ = false) = 0;
	virtual void getUpdateIndices(MemoryContext *memoryContext, const SubRecord *update, u16 *updateNum, 
		u16 **updateIndices, u16 *updateUniques) = 0;

	static const uint IDX_MAX_KEY_LEN = Limits::PAGE_SIZE / 3;
	static const uint IDX_MAX_KEY_PART_LEN = 767;
};

class SubToSubExtractor;
/** 一个DRS索引 */
class DrsIndex : public Analysable, public IndexBase {
public:
	virtual bool insert(Session *session, const SubRecord *key, bool *duplicateKey, bool checkDuplicate = true) = 0;
	virtual bool insertGotPage(DrsIndexScanHandleInfo *info) = 0;
	virtual void insertNoCheckDuplicate(Session *session, const SubRecord *key) = 0;

	virtual bool del(Session *session, const SubRecord *key) = 0;
	virtual bool getByUniqueKey(Session *session, const SubRecord *key, LockMode lockMode, RowId *rowId, 
		SubRecord *subRecord, RowLockHandle **rlh, SubToSubExtractor *extractor) = 0;
	virtual IndexScanHandle* beginScan(Session *session, const SubRecord *key, bool forward, bool includeKey, 
		LockMode lockMode, RowLockHandle **rlh, SubToSubExtractor *extractor) = 0;
	virtual bool getNext(IndexScanHandle *scanHandle, SubRecord *key) = 0;
	virtual bool deleteCurrent(IndexScanHandle *scanHandle) = 0;
	virtual void endScan(IndexScanHandle *scanHandle) = 0;

#ifdef TNT_ENGINE
	virtual IndexScanHandle* beginScanSecond(Session *session, const SubRecord *key, bool forward, 
		bool includeKey, LockMode lockMode, RowLockHandle **rlh, SubToSubExtractor *extractor, 
		TLockMode trxLockMode) = 0;
	virtual bool getNextSecond(IndexScanHandle *scanHandle) throw(NtseException) = 0;
	virtual void endScanSecond(IndexScanHandle *scanHandle) = 0;

	virtual bool checkDuplicate(Session *session, const SubRecord *key, DrsIndexScanHandleInfo **info) = 0;
//	virtual void setTableDef(const TableDef* tableDef) = 0;
	virtual bool locateLastLeafPageAndFindMaxKey(Session *session, SubRecord *foundKey) = 0;
	virtual u64 recordsInRangeSecond(Session *session, const SubRecord *min, bool includeKeyMin, const SubRecord *max,
		bool includeKeyMax) = 0;
#endif
	virtual u8 getIndexId() = 0;
	virtual u64 recordsInRange(Session *session, const SubRecord *min, bool includeKeyMin, const SubRecord *max,
		bool includeKeyMax) = 0;
	virtual const IndexStatus& getStatus() = 0;
	virtual void updateExtendStatus(Session *session, uint maxSamplePages) = 0;
	virtual const IndexStatusEx& getStatusEx() = 0;
	virtual void setSplitFactor(s8 splitFactor) = 0;
};


}

#endif