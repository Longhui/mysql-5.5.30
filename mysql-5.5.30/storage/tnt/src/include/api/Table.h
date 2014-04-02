/**
 * 表管理接口
 *
 * @author 汪源(wangyuan@corp.netease.com, wy@163.org)
 */

#ifndef _NTSE_TABLE_H_
#define _NTSE_TABLE_H_

#include "api/Database.h"
#include "misc/Global.h"
#include "misc/Record.h"
#include "util/Array.h"
#include "lob/Lob.h"
#include "mms/Mms.h"
#include "misc/TableDef.h"
#include "misc/Session.h"
#include "util/Thread.h"
#include "rec/Records.h"


#ifdef NTSE_UNIT_TEST
class TableTestCase;
#endif

#ifdef TNT_ENGINE
namespace tnt {
	class TNTTable;
}
#endif

namespace ntse {

/** 修改信息，表示UPDATE/DELETE语句共同的操作特征 */
struct TSModInfo {
public:
	TSModInfo(Session *session, const TableDef *tableDef, const ColList &updCols, const ColList &readCols);
	void prepareForUpdate(const SubRecord *redSr);
	
public:
	ColList			m_missCols;			/** 更新之前需要补齐的属性(目前即更新涉及的扫描时未读取的索引属性) */
	bool			m_updIndex;			/** 是否更新索引 */
	SubRecord		*m_indexPreImage;	/** UPDATE/DELETE时更新索引的前像部分记录，用于存储记录的内存由外部指定 */
};

class IndexScanHandle;
class DrsHeapScanHandle;
class Session;
class SubRecord;
class RowLockHandle;
struct MmsRecord;
class DrsIndex;
class Table;
class TblScan {
private:
	TblScan();

public:
	ScanType getType() const {
		return m_type;
	}
	RowId getCurrentRid() const {
		return m_redRow->m_rowId;
	}
	SubToSubExtractor* getIdxExtractor() const {
		return m_idxExtractor;
	}
	bool getCoverageIndex() const {
		return m_coverageIndex;
	}
	SubRecord* getIdxKey() const {
		return m_idxKey;
	}
	SubRecord* getMysqlRow() const {
		return m_mysqlRow;
	}
	SubRecord* getRedRow() const {
		return m_redRow;
	}
	Records::BulkOperation* getRecInfo() const {
		return m_recInfo;
	}
	ColList getReadCols() const {
		return m_readCols;
	}

	IndexScanHandle* getDrsScanHandle() const {
		return m_indexScan;
	}

	void setCurrentData(byte *redData) {
		m_redRow->m_data = redData;
	}	
	void setUpdateColumns(u16 numCols, u16 *columns);
	void setPurgeUpdateColumns(u16 numCols, u16 *columns);
	bool isUpdateColumnsSet() const { return m_updInfo != NULL; }
	static StatType getTblStatTypeForScanType(ScanType scanType);
	static StatType getRowStatTypeForScanType(ScanType scanType);
	bool checkEofOnGetNext();

private:
	void prepareForUpdate(const byte *mysqlUpdate, const byte *oldRow, bool isUpdateEngineFormat) throw(NtseException);
	void prepareForDelete();
	void initDelInfo();
	void determineRowLockPolicy();

	Session		*m_session;			/** 会话 */
	ScanType	m_type;				/** 扫描类型 */
	OpType		m_opType;			/** 操作类型 */
	Table		*m_table;			/** 表 */
	TableDef	*m_tableDef;		/** 表定义 */
	ColList		m_readCols;			/** 上层指定要读取的属性 */
	bool		m_bof;				/** 是否没开始扫描 */
	bool		m_eof;				/** 是否已经扫描到末尾 */

	LockMode	m_rowLock;			/** 记录锁模式 */
	RowLockHandle	**m_pRlh;		/** 指各行锁句柄的指针 */
	RowLockHandle	*m_rlh;			/** 行锁句柄 */
	
	bool		m_tblLock;			/** 为true表示是扫描过程自动加表锁，为false表示调用者负责加表锁 */
	StatType	m_scanRowsType;		/** 更新哪种扫描行数 */
	SubRecord	*m_mysqlRow;		/** 当前行，REC_MYSQL格式 */
	SubRecord	*m_redRow;			/** 当前行，REC_REDUNDANT格式 */
	TSModInfo	*m_delInfo;			/** DELETE所用的更新信息 */
	TSModInfo	*m_updInfo;			/** UPDATE所用的更新信息 */

	bool		m_singleFetch;		/** 是否为指定唯一性索引等值条件的获取单条记录操作 */
	IndexDef	*m_pkey;			/** 主键 */
	const SubRecord	*m_indexKey;	/** 搜索键 */
	IndexScanHandle	*m_indexScan;	/** 索引扫描句柄 */
	IndexDef	*m_indexDef;		/** 用于扫描的索引定义 */
	DrsIndex	*m_index;			/** 用于扫描的索引 */
	bool		m_coverageIndex;	/** 索引中是否包含所有所属属性 */
	SubRecord	*m_idxKey;			/** 用于从索引中读取属性 */
	SubToSubExtractor	*m_idxExtractor;	/** 从索引中提取子记录的提取器 */

	Records::BulkOperation	*m_recInfo;
	
friend class Table;
friend Tracer& operator << (Tracer& tracer, const TblScan *scan);
};


/** INSERT ... ON DUPLICATE KEY UPDATE或REPLACE操作序列 */
#ifdef TNT_ENGINE
template <typename T>
#endif
class IUSequence {
public:
	void getDupRow(byte *buf) {
		memcpy(buf, m_mysqlRow, m_tableDef->m_maxRecSize);
	}
#ifdef TNT_ENGINE
	T getScanHandle() {
#else
	TblScan *getScanHandle() {
#endif
		return m_scan;
	}

	uint getDupIndex() {
		return m_dupIndex;
	}

	byte *getMysqlRow() {
		return m_mysqlRow;
	}

private:
	TableDef	*m_tableDef;	/** 表定义 */
	byte		*m_mysqlRow;	/** 导致冲突的记录 */
	uint		m_dupIndex;		/** 导致冲突的索引 */
#ifdef TNT_ENGINE
	T           m_scan;
#else
	TblScan		*m_scan;		/** 扫描句柄 */
#endif
friend class Table;
#ifdef TNT_ENGINE
friend class tnt::TNTTable;
#endif
};

/** 索引扫描条件 */
struct IndexScanCond {
	IndexScanCond(u16 idx, const SubRecord *key, bool forward, 
		bool includeKey, bool singleFetch) {
		m_idx = idx;
		m_key = key;
		m_forward = forward;
		m_includeKey = includeKey;
		m_singleFetch = singleFetch;
	}

	u16				m_idx;			/** 索引编号 */
	const SubRecord *m_key;			/** 扫描起始键值 */
	bool			m_forward;		/** 是否为前向扫描 */
	bool			m_includeKey;	/** 是否包含=m_key的记录 */
	bool			m_singleFetch;	/** 是否为唯一性等值扫描 */
};

class DrsHeap;
class Database;
class DrsIndice;
class MmsTable;
class Session;
class LobStorage;
class MemoryContext;
struct LogEntry;
struct PreUpdateLog;
struct Stream;


/** 表管理 */
class Table {
public:
	///////////////////////////////////////////////////////////////////////////
	// DDL操作接口 ///////////////////////////////////////////////////////////
	///////////////////////////////////////////////////////////////////////////
	static void create(Database *db, Session *session, const char *path, TableDef *tableDef) throw(NtseException);
	static void drop(Database *db, const char *path) throw(NtseException);
	static void drop(const char *baseDir, const char *path) throw(NtseException);
	static Table* open(Database *db, Session *session, const char *path, bool hasCprsDict = false) throw(NtseException);
	static void checkRename(Database *db, const char *oldPath, const char *newPath, bool hasLob, bool hasCprsDict = false) throw(NtseException);
	static void rename(Database *db, Session *session, const char *oldPath, const char *newPath, bool hasLob, bool redo, bool hasCprsDict = false);
	void close(Session *session, bool flushDirty, bool closeComponents = false);
	void truncate(Session *session, bool tblLock = true, bool *newHasDic = NULL, bool isTruncateOper = true) throw(NtseException);
	void addIndex(Session *session, u16 numIndice, const IndexDef **indexDefs) throw(NtseException);

	// TNT引擎的需要，将NTSE Table的addIndex方法，修改为两阶段流程
	void addIndexPhaseOne(Session *session, u16 numIndice, const IndexDef **indexDefs) throw (NtseException);
	void addIndexPhaseTwo(Session *session, u16 numIndice, const IndexDef **indexDefs) throw(NtseException);

	// TNT引擎的需要，将NTSE Table的dropIndex方法，修改为三阶段流程
	void dropIndexPhaseOne(Session *session, uint numIndice, ILMode oldLock) throw(NtseException);
	void dropIndexPhaseTwo(Session *session, uint numIndice, ILMode oldLock) throw(NtseException);
	void dropIndexFlushPhaseOne(Session *session) throw(NtseException);
	void dropIndexFlushPhaseTwo(Session *session, ILMode oldLock) throw(NtseException);


	void dropIndex(Session *session, uint idx) throw(NtseException);
	void optimize(Session *session, bool keepDict, bool *newHasDic, bool *cancelFlag) throw(NtseException);
	void verify(Session *session) throw(NtseException);
	void flush(Session *session);
	void flushComponent(Session *session, bool flushHeap, bool flushIndice, bool flushMms, bool flushLob);
	static void getSchemaTableFromPath(const char *path, char **schemaName, char **tableName);
	void setOnlineAddingIndice(Session *session, u16 numIndice, const IndexDef **indice);
	void resetOnlineAddingIndice(Session *session);
	void alterTableArgument(Session *session, const char *name, const char *valueStr, int timeout, bool inLockTables = false) throw (NtseException);
	void defragLobs(Session *session) throw (NtseException);

	void alterUseMms(Session *session, bool useMms, int timeout, bool redo = false) throw(NtseException);
	void alterCacheUpdate(Session *session, bool cachUpdate, int timeout, bool redo = false) throw(NtseException);
	void doAlterCacheUpdate(Session *session, bool cachUpdate, int timeout, bool redo = false) throw(NtseException);
	void alterCacheUpdateTime(Session *session, uint interval, int timeout, bool redo = false) throw(NtseException);
	void alterCachedColumns(Session *session, const char *valueStr, int timeout) throw(NtseException);
	void alterCachedColumns(Session *session, u16 *cols, u16 numCols, bool enable, int timeout, bool redo = false) throw(NtseException);
	void alterCompressLobs(Session *session, bool compressLobs, int timeout, bool redo = false) throw(NtseException);
	void alterHeapPctFree(Session *session, u8 pctFree, int timeout, bool redo = false) throw(NtseException);
	void alterSplitFactors(Session *session, const char *valueStr, int timeout) throw(NtseException);
	void alterSplitFactors(Session *session, u8 indexNo, s8 splitFactor, int timeout, bool redo = false) throw(NtseException);
	void alterIncrSize(Session *session, u16 incrSize, int timeout, bool redo = false) throw(NtseException);
	void alterCompressRows(Session *session, bool compressRows, int timeout, bool redo = false) throw(NtseException);
	void alterHeapFixLen(Session *session, bool fixlen, int timeout) throw(NtseException);
	void alterColGrpDef(Session *session, const char *valueStr, int timeout) throw(NtseException);
	void alterColGrpDef(Session *session, Array<ColGroupDef *> *colGrpDef, int timeout, bool redo = false) throw(NtseException);
	void alterDictionaryArg(Session *session, int cmdType, uint newValue, int timeout, bool redo = false) throw(NtseException);
#ifdef TNT_ENGINE
	void alterTableStatus(Session *session, TableStatus tableStatus, int timeout, bool inLockTables, bool redo = false) throw(NtseException);
#endif
	void checkPermission(Session *session, bool redo) throw(NtseException);
	static void checkPermissionReal(bool trxConn, TableStatus tblStatus, const char *path) throw(NtseException);

	bool hasOnlineIndex(string *idxInfo);
	bool isDoneOnlineIndex(const IndexDef **indexDefs, u16 numIndice);
	bool isOnlineIndexOrder(const IndexDef **indexDefs, u16 numIndice);
	void dualFlushAndBump(Session *session, int timeout) throw (NtseException);
	///////////////////////////////////////////////////////////////////////////
	// DML操作接口 ///////////////////////////////////////////////////////////
	///////////////////////////////////////////////////////////////////////////
	TblScan* tableScan(Session *session, OpType opType,
		u16 numReadCols, u16 *readCols, bool tblLock = true, MemoryContext *lobCtx = NULL) throw(NtseException);
	TblScan* indexScan(Session *session, OpType opType, const IndexScanCond *cond,
		u16 numReadCols, u16 *readCols, bool tblLock = true, MemoryContext *lobCtx = NULL) throw(NtseException);
	TblScan* positionScan(Session *session, OpType opType,
		u16 numReadCols, u16 *readCols, bool tblLock = true, MemoryContext *lobCtx = NULL) throw(NtseException);
	bool getNext(TblScan *scan, byte *mysqlRow, RowId rid = INVALID_ROW_ID, bool needConvertToUppMysqlFormat = false);
	bool updateCurrent(TblScan *scan, const byte *update, bool isUpdateEngineFormat = true, uint *dupIndex = NULL, const byte *oldRow = NULL, void *cbParam = NULL) throw(NtseException);
	void deleteCurrent(TblScan *scan, void *cbParam = NULL);
	void endScan(TblScan *scan);

	RowId insert(Session *session, const byte *record, bool isRecordEngineFormat, uint *dupIndex, bool tblLock = true, void *cbParam = NULL) throw(NtseException);
#ifdef TNT_ENGINE
	RowId insert(Session *session, const byte *record, uint *dupIndex, RowLockHandle **rlh) throw(NtseException);
	void deleteLob(Session* session,  LobId lobId);
	void deleteLobAllowDupDel(Session* session,  LobId lobId);
	void tntLockMeta(Session *session, ILMode mode, int timeoutMs, const char *file, uint line) throw(NtseException);
	void tntUnlockMeta(Session *session, ILMode mode);

	IUSequence<TblScan *>* insertForDupUpdate(Session *session, const byte *record, bool tblLock = true, void *cbParam = NULL) throw(NtseException);
	bool updateDuplicate(IUSequence<TblScan *> *iuSeq, byte *update, u16 numUpdateCols, u16 *updateCols, uint *dupIndex = 0, void *cbParam = NULL) throw(NtseException);
	void deleteDuplicate(IUSequence<TblScan *> *iuSeq, void *cbParam = NULL);
	void freeIUSequenceDirect(IUSequence<TblScan *> *iduSeq);
#else
	IUSequence* insertForDupUpdate(Session *session, const byte *record, bool tblLock = true, void *cbParam = NULL) throw(NtseException);
	bool updateDuplicate(IUSequence *iuSeq, byte *update, u16 numUpdateCols, u16 *updateCols, uint *dupIndex = 0, void *cbParam = NULL) throw(NtseException);
	void deleteDuplicate(IUSequence *iuSeq, void *cbParam = NULL);
	void freeIUSequenceDirect(IUSequence *iduSeq);
#endif

	///////////////////////////////////////////////////////////////////////////
	// 表级锁接口 ///////////////////////////////////////////////////////////
	///////////////////////////////////////////////////////////////////////////
	void lockMeta(Session *session, ILMode mode, int timeoutMs, const char *file, uint line) throw(NtseException);
	void upgradeMetaLock(Session *session, ILMode oldMode, ILMode newMode, int timeoutMs, const char *file, uint line) throw(NtseException);
	void downgradeMetaLock(Session *session, ILMode oldMode, ILMode newMode, const char *file, uint line);
	void unlockMeta(Session *session, ILMode mode);
	ILMode getMetaLock(Session *session) const;
	const IntentionLockUsage* getMetaLockUsage() const;

	void lock(Session *session, ILMode mode, int timeoutMs, const char *file, uint line) throw(NtseException);
	void upgradeLock(Session *session, ILMode oldMode, ILMode newMode, int timeoutMs, const char *file, uint line) throw(NtseException);
	void downgradeLock(Session *session, ILMode oldMode, ILMode newMode, const char *file, uint line);
	void unlock(Session *session, ILMode mode);
	ILMode getLock(Session *session) const;
	const IntentionLockUsage* getLockUsage() const;

	///////////////////////////////////////////////////////////////////////////
	// 恢复接口 /////////////////////////////////////////////////////////////
	///////////////////////////////////////////////////////////////////////////
	static void redoTrunate(Database *db, Session *session, const LogEntry *log, const char *path, bool *newHasDict)  throw(NtseException);
	void redoDropIndex(Session *session, u64 lsn, const LogEntry *log);
	PreDeleteLog* parsePreDeleteLog(Session *session, const LogEntry *log);
#ifdef TNT_ENGINE		//tnt中关于删除大对象的日志
	LobId parsePreDeleteLobLog(Session *session, const LogEntry *log);
	void writePreDeleteLobLog(Session *session, LobId lobId);
#endif
	RowId getRidFromPreDeleteLog(const LogEntry *log);
	static char* parseCreateLog(const LogEntry *log, TableDef *tableDef);
	IndexDef* parseAddIndexLog(const LogEntry *log);
	void redoFlush(Session *session);
	void redoAlterTableArg(Session *session, const LogEntry *log) throw (NtseException);
	static void redoAlterIndice(Session *session, const LogEntry *log, Database *db, const char *tablepath) throw (NtseException);
	static void redoAlterColumn(Session *session, const LogEntry *log, Database *db, const char *tablepath, bool *newHasDict);
	void redoMoveLobs(Session *session, LsnType lsn, const LogEntry *log) throw (NtseException);
	static void redoCreateDictionary(Session *session, Database *db, const char *tablePath);

	///////////////////////////////////////////////////////////////////////////
	// 统计信息 ///////////////////////////////////////////////////////
	///////////////////////////////////////////////////////////////////////////
	void refreshRows(Session *session);
	u64 getRows();
	u64 getDataLength(Session *session);
	u64 getIndexLength(Session *session, bool includeMeta = true);
	Array<DBObjStats*>* getDBObjStats();
	u64 getNumRowLocks(LockMode mode) {
		return m_rlockCnts[mode];  
	}

	void writeTableDef();
	///////////////////////////////////////////////////////////////////////////
	// 记录压缩接口 ///////////////////////////////////////////////////////////
	///////////////////////////////////////////////////////////////////////////
	/**
	* 是否含有记录压缩字典
	*/
	inline bool hasCompressDict() {
		return m_hasCprsDict;
	}
	inline const RCDictionary * getCompressDict() {
		assert(m_records);
		return hasCompressDict() ? m_records->getDictionary() : NULL; 
	}
	void createDictionary(Session *session, ILMode *metaLockMode, ILMode *dataLockMode) throw(NtseException);

	///////////////////////////////////////////////////////////////////////////
	// 得到组成表的各个部分 ////////////////////////////////////////////////
	///////////////////////////////////////////////////////////////////////////	
	/** 得到表定义
	 * @param assertSafe 是否检查在加了表元数据锁后的安全调用
	 * @param session 若assertSafe为true则给出会话，否则可以是NULL
	 * @return 表定义
	 */
	TableDef* getTableDef(bool assertSafe = false, Session *session = NULL) const {
		if (assertSafe) {
			UNREFERENCED_PARAMETER(session);
			assert(getMetaLock(session) != IL_NO || m_db->getStat() == DB_RECOVERING);
		}

		return m_tableDef;
	}

#ifdef TNT_ENGINE
	TableDef **getTableDefAddr() {
		return &m_tableDef;
	}
#endif

	void setTableDef(TableDef *tableDef) {
		m_tableDef = tableDef;
		m_tableDefWithAddingIndice = tableDef;
		m_records->setTableDef(tableDef);
	}
	/** 得到表的记录管理器
	 * @return 表的记录管理器
	 */
	Records* getRecords() const {
		return m_records;
	}
	/** 得到表对应的MMS系统
	 * @return 表对应的MMS系统
	 */
	MmsTable* getMmsTable() const {
		return m_records->getMms();
	}
	/** 得到用于存储表中记录的堆对象
	 * @return 堆对象
	 */
	DrsHeap* getHeap() const {
		return m_records->getHeap();
	}
	/** 得到用于存储表中索引数据的索引对象
	 * @return 索引对象
	 */
	DrsIndice* getIndice() const {
		return m_indice;
	}
	/** 得到用于存储表中大对象内容的大对象存储对象
	 * @return 大对象存储对象
	 */
	LobStorage* getLobStorage() const {
		return m_records->getLobStorage();
	}
	/** 得到表文件路径
	 * @return 表文件路径
	 */
	const char* getPath() const {
		return m_path;
	}
	
	/** 得到表的元数据锁
	*	@return 表元数据锁
	*/
	IntentionLock* getMetaLockInst() {
		return &m_metaLock;
	}

	/** 设置当前表的临时表属性
	 * @param mysqlTmpTable	true表示是mysql的临时表，false表示不是
	 */
	void setMysqlTmpTable(bool mysqlTmpTable) {
		m_mysqlTmpTable = mysqlTmpTable;
	}
	
	/** 得到表的openTableLink 入口项
	*	@return 链表入口项
	 */
	DLink<Table *> &getOpenTablesLink()  {
		return m_openTablesLink;
	}

private:
	Table(Database *db, const char *path, Records *records, TableDef *tableDef, bool hasCprsDict);
	TblScan* beginScan(Session *session, ScanType type, OpType opType,
		u16 numReadCols, u16 *readCols, bool tblLock);
	bool isCoverageIndex(const TableDef *table, const IndexDef *index, const ColList &cols);

	/** 释放最近一条记录占用的资源
	 *
	 * @param scan 扫描句柄
	 * @param retainLobs 是否保留大对象内容
	 */
	void releaseLastRow(TblScan *scan, bool retainLobs) {
		if (scan->m_rlh)
			scan->m_session->unlockRow(&scan->m_rlh);
		if (scan->m_recInfo)
			scan->m_recInfo->releaseLastRow(retainLobs);
	}
	TblScan* allocScan(Session *session, u16 numCols, u16 *columns, OpType opType, bool tblLock);
	void stopUnderlyingScan(TblScan *scan);
	void verifyRecord(Session *session, RowId rid, const SubRecord *expected) throw(NtseException);
	void verifyRecord(Session *session, const Record *expected);
	void verifyDeleted(Session *session, RowId rid, const SubRecord *indexAttrs);
	bool checkLock(Session *session, OpType opType);
	void lockTable(Session *session, OpType opType) throw(NtseException);
	void unlockTable(Session *session, OpType opType);
	
	// 日志相关
	void writePreDeleteLog(Session *session, RowId rid, const byte *row, const SubRecord *indexPreImage);
	void writeAddIndexLog(Session *session, const IndexDef *indexDef);
	u64 writeTruncateLog(Session *session, const TableDef *tableDef, bool hasDict, bool isTruncateOper);
	static void parseTruncateLog(const LogEntry *log, TableDef** tableDefOut, bool *hasDict, bool *isTruncateOper);

	u64 writeAlterIndiceLog(Session *session, const TableDef *newTableDef, const char* relativeIdxPath);
	static void parseAlterIndiceLog(const LogEntry *log, TableDef **tableDef, char** relativeIdxPath);
	u64 writeAlterColumnLog(Session *session, const char* tmpTablePath, bool hasLob, bool hasDict);
	static void parseAlterColumnLog(const LogEntry *log, char** tmpTablePath, bool* hasLob, bool *hasDict);

	u64 writeAlterTableArgLog(Session *session, u16 tableId, int cmdType, uint newValue);
	u64 writeAlterTableArgLog(Session *session, u16 tableId, int cmdType, u16 *cols, u16 numCols, bool enable);
	u64 writeAlterTableArgLog(Session *session, u16 tableId, int cmdType, u8 indexNo, s8 splitFactor);
	u64 writeAlterTableArgLog(Session *session, u16 tableId, int cmdType, Array<ColGroupDef *> *colGrpDefArr);

	u64 writeCreateDictLog(Session *session);


	void replaceComponents(const Table *another);
	void setTableId(Session *session, u16 tableId);

	//修改表定义相关
	void preAlter(Session *session, int cmdType, uint newValue, int timeout) throw (NtseException);
	void createTmpCompressDictFile(const char *dicFullPath, const RCDictionary *tmpDict) throw (NtseException);
	RCDictionary *extractCompressDict(Session *session) throw (NtseException); 

	void shareFlush(Session *session);
	void dualFlush(Session *session, int timeout) throw(NtseException) ;

	static void binlogMmsUpdate(Session *session, SubRecord *dirtyRecord, void *data);
private:
	Database	*m_db;			/** 所属数据库 */
	Records		*m_records;		/** 记录管理器 */
	TableDef	*m_tableDef;	/** 表定义 */
	TableDef	*m_tableDefWithAddingIndice;	/** 包括正在创建的索引的表定义，若与m_tableDef相同则表示不在创建索引过程中 */
	DrsIndice	*m_indice;		/** DRS索引 */
	const char	*m_path;		/** 表文件路径 */
	IntentionLock	m_metaLock;	/** 元数据锁 */
	IntentionLock	m_tblLock;	/** 表级锁 */
	s64			m_estimateRows;	/** 估计有多少条记录 */
	u64			m_rlockCnts[Exclusived + 1];	/** 各类行锁的加锁次数 */
	struct MMSBinlogCallBack *m_mmsCallback;	/** MMS记录binlog所需要的回调对象 */
	bool		m_mysqlTmpTable;				/** 是否是Mysql上层指定的临时表 */
	bool        m_hasCprsDict;                  /** 是否有全局字典文件 */
	DLink<Table *>	m_openTablesLink;		/** 在LRU链表中的入口项 */

	friend class TblScan;
	friend class TableOnlineMaintain;
	friend class TblMntAlterIndex;
	friend class TblMntAlterColumn;
#ifdef NTSE_UNIT_TEST
public:
	friend class ::TableTestCase;
#endif
};

/** 析构函数中自动释放表锁 */
struct TblLockGuard {
	TblLockGuard() {
		m_session = NULL;
	}
	~TblLockGuard() {
		if (!m_session)
			return;
		if (m_table->getLock(m_session) != IL_NO)
			m_table->unlock(m_session, m_table->getLock(m_session));
		if (m_table->getMetaLock(m_session) != IL_NO)
			m_table->unlockMeta(m_session, m_table->getMetaLock(m_session));
		m_session = NULL;
		m_table = NULL;
	}
	void attach(Session *session, Table *table) {
		m_session = session;
		m_table = table;
	}
	void detach() {
		m_session = NULL;
		m_table = NULL;
	}

private:
	Session	*m_session;
	Table	*m_table;
};

}

#endif

