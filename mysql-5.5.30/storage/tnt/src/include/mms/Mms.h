/**
 * MMS记录缓存
 *
 * @author 汪源(wangyuan@corp.netease.com, wy@163.org)
 */

#ifndef _NTSE_MMS_H_
#define _NTSE_MMS_H_

#include <vector>
#include <iostream>
#include "misc/Global.h"
#include "misc/Session.h"
#include "util/Array.h"
#include "util/DList.h"
#include "util/PagePool.h"
#include "util/Portable.h"
#include "util/Thread.h"
#include "heap/Heap.h"

using namespace std;

namespace ntse {

class Table;
class SubRecord;
class Record;
struct ColList;

class Mms;
class MmsTable;
class MmsRPClass;
class MmsFreqHeap;
class MmsOldestHeap;
class MmsPageOper;
class MmsRidMap;
class MmsFlushTimer;
class MmsReplacer;

struct MmsRecord;
struct MmsRecPage;
struct MmsRecPair;

/** 8字节对齐 */
#define MMS_ALIGN_8(size)	((size + 7) & (~7))
/** MMS缓存记录页头长度 */
#define MMS_REC_PAGE_HEAD_SIZE MMS_ALIGN_8(sizeof(MmsRecPage))
/** 最大缓存记录长度 */
#define MMS_MAX_RECORD_SIZE (Limits::PAGE_SIZE - MMS_REC_PAGE_HEAD_SIZE)
/** 页级别增长因子 */
#define MMS_RPC_GROWTH_FACTOR	1.25

/** 全局MMS使用状态 */
struct MmsStatus {
	u64		m_recordQueries;	/** MMS记录查询次数	*/			
	u64		m_recordQueryHits;	/** MMS记录查询命中次数 */
	u64		m_recordInserts;	/** MMS记录插入次数	*/			
	u64		m_recordDeletes;	/** MMS记录被删除次数 */	
	u64		m_recordUpdates;	/** MMS记录被更新次数 */
	u64		m_recordVictims;    /** MMS记录被替换次数 */
	u64		m_pageVictims;		/** MMS缓存页被替换次数 */	
	u64		m_occupiedPages;	/** MMS占用的内存页个数，包括哈希表占用的页面和缓存的空闲页 */
	u64		m_recordPages;		/** 占用的记录页数 */
};

/** MMS表使用状态 */
struct MmsTableStatus {
	u64		m_records;			/** MMS表中缓存记录个数	*/
	u64		m_recordPages;		/** MMS表占用的记录页个数 */
	u64		m_recordQueries;	/** MMS表记录查询次数 */
	u64		m_recordQueryHits;	/** MMS表记录查询命中次数 */
	u64		m_recordInserts;	/** MMS表记录插入次数 */ 
	u64		m_recordDeletes;	/** MMS表记录删除次数 */
	u64		m_recordUpdates;	/** MMS表记录更新次数 */
	u64		m_recordVictims;	/** MMS表记录被替换次数 */
	u64		m_pageVictims;		/** MMS表缓存页被替换次数 */
	u64		m_replaceFailsWhenPut; /** MMS表插入时替换失败次数 */
	u64		m_updateMerges;		/** 缓存更新合并次数 */
	u64		m_dirtyRecords;		/** MMS表中脏记录个数 */
};

/** MMS页类使用状态 */
struct MmsRPClassStatus {
	u64		m_recordPages;		/** 表页级别占用的缓存页个数 */
	u64		m_freePages;		/** 表页级别中有空闲槽的页个数 */
	u64		m_records;			/**  表页级别中缓存记录个数 */
	u64		m_recordInserts;	/** 表页级别中记录插入次数 */
	u64		m_recordDeletes;	/** 表页级别中记录被删除次数 */
	u64		m_recordUpdates;	/** 表页级别中记录被更新次数 */
	u64		m_recordVictims;	/** 表页级别中记录被替换次数 */
	u64		m_pageInserts;		/** 表页级别中缓存页插入次数 */
	u64		m_pageDeletes;		/** 表页级别中缓存页删除次数 */
	u64		m_pageVictims;		/** 表页级别中缓存页被替换次数 */						
};

/** BINLOG使用 */
/**
 * mms刷新更新缓存的时候调用的写binlog回调函数
 * @param session		会话
 * @param dirtyRecord	mms更新的记录内容，包含了记录的主键以及更新的属性内容，一定是REC_REDUNDANT格式的
 * @param data			额外信息参数
 */
typedef void (*MMSBinlogWriter)(Session *session, SubRecord *dirtyRecord, void *data);
struct MMSBinlogCallBack {
	MMSBinlogCallBack(MMSBinlogWriter writer, void *data) : m_writer(writer), m_data(data) {}
	MMSBinlogWriter m_writer;      /** 回调函数指针定义 */
	void			*m_data;       /** 允许上层传入一个额外的数据指针作为回调函数的参数 */
};



/** MMS记录缓存页类 */
class MmsRPClass {
public:
	const MmsRPClassStatus& getStatus();
	u16 getMinRecSize();
	u16 getMaxRecSize();

private:
	MmsRPClass(MmsTable *mmsTable, u16 slotSize);
	~MmsRPClass();
	MmsRecPage* getRecPageFromFreelist();
	void delRecPageFromFreelist(MmsRecPage *recPage);
	void addRecPageToFreeList(MmsRecPage *recPage);
	void freeAllPages(Session *session);

private:
	MmsTable			*m_mmsTable;	/** 所属的MMS表 */
	u16					m_slotSize;		/** 页类中缓存槽大小 */
	u8					m_numSlots;		/** 每页中记录槽个数 */
	DList<MmsRecPage *> m_freeList;		/** 空闲记录页双向链表 */
	MmsOldestHeap		*m_oldestHeap;	/** 最老缓存页堆 */
	MmsRPClassStatus    m_status;		/** 使用状态统计 */

	friend class Mms;
	friend class MmsTable;
	friend class MmsPageOper;
	friend class MmsOldestHeap;
};

class Database;
/** MMS全局类 */
class Mms : public PagePoolUser {
public:
	Mms(Database *db, uint targetSize, PagePool *pagePool, bool needRedo = true, float replaceRatio = 0.01, int intervalReplacer = 60, float minFPageRatio = 0.01, float maxFPageRatio = 100.0);	
	void close();
	void stopReplacer();
	void registerMmsTable(Session *session, MmsTable *mmsTable);
	void unregisterMmsTable(Session *session, MmsTable *mmsTable);
	void setMaxNrDirtyRecs(int threshold);
	int getMaxNrDirtyRecs();
	float getPageReplaceRatio();
	bool setPageReplaceRatio(float ratio);
	virtual uint freeSomePages(u16 userId, uint numPages);
	void endRedo();
	const MmsStatus& getStatus();
	void printStatus(ostream &out);
	void computeDeltaQueries(Session *session);

#ifdef NTSE_UNIT_TEST
	void lockMmsTable(u16 userId);
	void unlockMmsTable(u16 userId);
	void lockRecPage(Session *session);
	void unlockRecPage(Session *session);
	void pinRecPage();
	void unpinRecPage();
	void mmsTestGetPage(MmsRecPage *recPage);
	void mmsTestGetTable(MmsTable *mmsTable);
	void mmsTestSetTask(MmsRPClass *rpClass, int taskCount);
	void runReplacerForce();
#endif

private:
	void* allocMmsPage(Session *session, bool external);
	void freeMmsPage(Session *session, MmsTable *mmsTable, void *page);
	int getReplacedPageNum();

private:
	Database	*m_db;							/** 数据库对象 */
	Connection	*m_fspConn;						/** 仅用于freeSomePages的数据库连接 */
	u8			m_nrClasses;					/** 类级别个数 */
	u16			*m_pageClassSize;				/** 类级别页大小 */
	u16			*m_pageClassNrSlots;			/** 类级别槽个数 */
	u16			*m_size2class;					/** 级别映射表 */
	MmsReplacer *m_replacer;					/** 页替换线程实例 */
	PagePool			*m_pagePool;			/** 所属页池 */
	bool				m_duringRedo;			/** 是否正在REDO */
	LURWLock			m_mmsLock;				/** Mms全局锁 */
	DList<MmsTable *>	m_mmsTableList;			/** MmsTable双向链表 */
	Atomic<long>		m_numPagesInUse;		/** MMS使用的内存页个数 */
	int					m_maxNrDirtyRecs;		/** 临时区最大脏记录数 */
	float				m_pgRpcRatio;			/** 页替换概率，[0, 1]之间 */
	float				m_replaceRatio;			/** 替换率 */									
	MmsStatus			m_status;				/** 使用状态统计 */
	u64					m_preRecordQueries;		/** 先前记录查询个数 */
	float				m_minFPageRatio;		/** 最小FPage调整因子 */
	float				m_maxFPageRatio;		/** 最大FPage调整因子 */

#ifdef NTSE_UNIT_TEST
	int				m_taskCount;				/** 测试任务个数 */
	int				m_taskNum;					/** 当前测试任务编号 */
	MmsOldestHeap	*m_taskBottomHeap;			/** 测试任务涉及的底页堆 */
	MmsFreqHeap		*m_taskTopHeap;				/** 测试任务涉及的顶页堆 */
	MmsTable		*m_taskTable;				/** 测试任务涉及的MMS表 */
	MmsRecPage		*m_taskPage;				/** 测试任务涉及的记录页 */
#endif
	
	friend class MmsTable;
	friend class MmsRPClass;
	friend class MmsPageOper;
	friend class MmsOldestHeap;
	friend class MmsFreqHeap;
	friend class MmsRidMap;
	friend class MmsReplacer;
};

/** 一个表对应的MMS系统 */
class MmsTable {
public:
	MmsTable(Mms *mms, Database *db, DrsHeap *drsHeap, const TableDef *tableDef, bool cacheUpdate, uint updateCacheTime, int partitionNr = 31);
	void close(Session *session, bool flushDirty);
	uint getUpdateCacheTime();
	void setUpdateCacheTime(uint updateCacheTime);
	MmsRecord* getByRid(Session *session, RowId rid, bool touch, RowLockHandle **rlh, LockMode lockMode);
	MmsRecord* putIfNotExist(Session *session, const Record *record);
	void unpinRecord(Session *session, const MmsRecord *record);
	bool canUpdate(MmsRecord *record, const SubRecord *subRecord, u16 *recSize);
	bool canUpdate(MmsRecord *record, u16 newSize);
	void update(Session *session, MmsRecord *record, const SubRecord *subRecord, u16 recSize, Record *newCprsRcd = NULL);
	void flushAndDel(Session *session, MmsRecord *record);
	void del(Session *session, MmsRecord *record);
	void flush(Session *session, bool force, bool ignoreCancel = true) throw(NtseException);
	bool getSubRecord(Session *session, RowId rid, SubrecExtractor *extractor, SubRecord *subRec, bool touch, bool ifDirty, u32 readMask);
	void getSubRecord(const MmsRecord *mmsRecord, SubrecExtractor *extractor, SubRecord *subRecord);
	void getRecord(const MmsRecord *mmsRecord, Record *rec, bool copyIt = true);
	bool isDirty(const MmsRecord *mmsRecord);
	void redoUpdate(Session *session, const byte *log, uint size);
	MmsRPClass** getMmsRPClass(int *nrRPClasses);
	const MmsTableStatus& getStatus();
	void flushUpdateLog(Session *session);
	const LURWLock& getTableLock();
	void setMaxRecordCount(u64 recordSize = (u64)-1);
	void setMapPartitions(int ridNrZone);
	void setCacheUpdate(bool cacheUpdate);
	uint getPartitionNumber();
	void getRidHashConflictStatus(uint partition, double *avgConflictLen, size_t *maxConflictLen);
	void setBinlogCallback(MMSBinlogCallBack *mmsCallback);
	void setCprsRecordExtrator(CmprssRecordExtractor *cprsRcdExtractor);

// 单元测试使用
#ifdef NTSE_UNIT_TEST
	void lockMmsTable(u16 userId);
	void unlockMmsTable(u16 userId);
	void flushMmsLog(Session *session);
	void disableAutoFlushLog();
	void evictCurrPage(Session *session);
	void pinCurrPage();
	void unpinCurrPage();
	void disableCurrPage();
	void lockCurrPage(Session *session);
	void unlockCurrPage(Session *session);
	void delCurrRecord();
	void mmsTableTestGetPage(MmsRecPage *recPage);
	void setRpClass(MmsRPClass *rpClass);
	void setMmsTableInRpClass(MmsTable *mmsTable);
	void runFlushTimerForce();
#endif

public:
	static const u64 MAX_UPDATE_CACHE_COLUMNS = 32;
	
private:
	void getSubRecord(Session *session, const MmsRecord *mmsRecord, SubRecord *subRecord);
	MmsRecPage* allocMmsRecord(Session *session, MmsRPClass *rpClass, int ridNr, bool *locked);
	void delRecord(Session *session, MmsRecPage *recPage, MmsRecord *mmsRecord);
	void evictMmsPage(Session *session, MmsRecPage *victimPage);
	void sortAndFlush(Session *session, bool force, bool ignoreCancel, MmsTable *table, std::vector<MmsRecPair> *tmpRecArray) throw(NtseException);
	void writeDataToDrs(Session *session, MmsRecPage *recPage, MmsRecord *mmsRecord, bool force);
	MmsRecord* put(Session *session, const Record *record, u32 dirtyBitmap, int ridNr, bool *tryAgain);
	void doFlush(Session *session, bool force, bool ignoreCancel, bool tblLocked) throw(NtseException);
	bool doTouch(Session *session, MmsRecPage *recPage, MmsRecord *mmsRecord);
	void doMmsLog(Session *session, MemoryContext *mc, const SubRecord *subRecord);
	void writeMmsUpdateLog(Session *session, MmsRecord *mmsRecord, u32 updBitmap);
	void writeCachedBinlog(Session *session, const ColList &updCachedCols, MmsRecord *mmsRecord);
	void writeMmsUpdateLog(Session *session, const SubRecord *subRecord);
	void writeMmsUpdateLog(Session *session, MmsRecord *mmsRecord, const SubRecord *subRecord, u32 updBitmap);
	void flushLog(Session *session, bool force = false);

	int dirtyBm2cols(u32 dirtyBitmap, u16 *cols);
	int updBm2cols(u32 updBitmap, u16 *cols);
	bool cols2bitmap(const SubRecord *subRecord, u32 *updBitmap, u32 *dirtyBitmap);
	int mergeCols(const SubRecord *subRecord, u32 updBitmap, u16 *cols, int numCols);
	int mergeCols(u16 *src1Cols, int num1Cols, u16 *src2Cols, int num2Cols, u16 *dstCols);
	
	inline int getRidPartition(const RowId rid) {
		return (int)(rid % m_ridNrZone);
	}

	RecFormat getMmsRecFormat(const TableDef *tableDef, const MmsRecord *mmsRecord);

private:
	Mms					*m_mms;								/** 所属的记录缓存管理者 */
	Database			*m_db;								/** 所属的数据库 */
	DrsHeap				*m_drsHeap;							/** 所属的堆 */
	const TableDef            *m_tableDef;
	LURWLock			m_mmsTblLock;						/** MMS表读写锁 */
	LURWLock			**m_ridLocks;						/** RID分区锁 */
	MmsFreqHeap			*m_freqHeap;						/** 最低频繁访问页堆 */
	MmsRPClass			**m_rpClasses;						/** 页类指针数组 */
	MmsRidMap			**m_ridMaps;						/** RID分区映射表 */
	bool				m_cacheUpdate;						/** 是否缓存更新 */
	MmsFlushTimer		*m_flushTimer;						/** 更新间隔刷新定时器 */
	MMSBinlogCallBack   *m_binlogCallback;					/** BINLOG回调 */
	u8					m_updateCacheNumCols;				/** 更新缓存字段数 */				
	u16					*m_updateCacheCols;					/** 更新缓存支持字段列表 */
	byte				*m_updateBitmapOffsets;				/** 更新缓存位图偏移列表 */
	Array<MmsRecPage *> *m_recPageArray;					/** 记录页指针数组 */
	MmsTableStatus		m_status;							/** 使用状态统计 */
	Atomic<int>			m_inLogFlush;						/** 是否在刷写日志 */
	Atomic<long>		m_numDirtyRecords;					/** MMS中脏记录个数 */ 
	u64					m_maxRecordCount;					/** MMS表中最多允许存放的记录个数 */
	int					m_ridNrZone;						/** RID映射分区数 */
	Atomic<int>			m_existPin;							/** 表存在PIN */
	u64					m_preRecordQueries;					/** 先前的记录查询数 */
	float				m_deltaRecordQueries;				/** 记录查询数差值率 */

	CmprssRecordExtractor *m_cprsRcdExtractor;                 /** 压缩记录提取器 */

// 单元测试使用
#ifdef NTSE_UNIT_TEST
	bool				m_autoFlushLog;
	MmsRecPage			*m_testCurrPage;
	MmsRecord			*m_testCurrRecord;
	MmsRPClass			*m_testCurrRPClass;
#endif

	friend class Mms;	
	friend class MmsRPClass;
	friend class MmsFreqHeap;
	friend class MmsOldestHeap;
	friend class MmsRidMap;
	friend class MmsFlushTimer;
	friend class MmsPageOper;
};

/** 　更新日志刷写后台线程　*/
class MmsFlushTimer : public BgTask {
public:
	MmsFlushTimer(Database *db, MmsTable *mmsTable, uint interval);
	void runIt();

private:
	MmsTable			*m_mmsTable;						/** 所属的MMS表 */
};

/** 　页替换后台线程　*/
class MmsReplacer : public BgTask {
public:
	MmsReplacer(Mms *mms, Database *db, uint interval);
	void runIt();

private:
	Mms					*m_mms;								/** 所属MMS	*/
	Database			*m_db;		/** 数据库 */
};

#ifdef NTSE_UNIT_TEST

#define MMS_TEST_GET_PAGE(mms, page) (mms)->mmsTestGetPage((page))
#define MMS_TEST_GET_TABLE(mms, tbl) (mms)->mmsTestGetTable((tbl))
#define MMS_TEST_SET_TASK(mms, rpclass, cnt) (mms)->mmsTestSetTask((rpclass), (cnt))
#define MMSTABLE_TEST_GET_PAGE(mmsTable, page) (mmsTable)->mmsTableTestGetPage((page))

#else

#define MMS_TEST_GET_PAGE(mms, page)
#define MMS_TEST_GET_TABLE(mms, tbl)
#define MMS_TEST_SET_TASK(mms, rpclass, cnt)
#define MMSTABLE_TEST_GET_PAGE(mmsTable, page)

#endif

}
#endif


