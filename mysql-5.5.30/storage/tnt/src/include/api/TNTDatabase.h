/**
* TNT Database模块。
*
* @author 何登成
*/
#ifndef _TNTDATABASE_H_
#define _TNTDATABASE_H_

#include "trx/TNTTransaction.h"
#include "api/Database.h"
#include "misc/TNTControlFile.h"
#include "misc/TNTIMPageManager.h"
#include "misc/CommonMemPool.h"
#include "heap/VersionPool.h"

namespace tnt {
class TNTControlFile;
class PurgeTask;
class DumpTask;
struct TNTTableInfo;
class TNTTableBase;
struct TNTConfig;
class TNTTable;

#define TNT_AUTO_CREATE			(1 << 0)
#define TNT_UPGRADE_FROM_NTSE	(1 << 1)

struct TNTGlobalStatus {
	uint		m_pageBufSize;		/** TNT Buffer的当前空间大小 */
	uint		m_freeBufSize;		/** TNT Buffer当前空闲页面数量 */

	uint		m_purgeCnt;			/** TNT Purge的次数 */
	uint		m_numPurgeSkipTable;/** TNT Purge跳过的表的数量*/
	uint		m_purgeMaxTime;		/** TNT Purge最长使用的时间*/
	uint		m_purgeAvgTime;		/** TNT Purge平均使用的时间*/
	uint		m_purgeLastTime;	/** TNT 上一次Purge使用的时间*/
	char		m_purgeMaxBeginTime[30];/** TNT Purge 最长一次的开始时间*/

	uint		m_defragHashCnt;	/** TNT DefragHash的次数*/
	uint		m_numDefragHashSkipTable; /** TNT Defrag Hash 跳过的表数量*/

	uint		m_dumpCnt;			/** TNT Dump的次数*/

	u64			m_openTableCnt;		/** TNT openTable的次数*/
	u64			m_closeTableCnt;	/** TNT closeTable的次数*/

	u64			m_freeMemCallCnt;	/** TNT 内存不够时调用freeMem的次数 */

	u64			m_switchVersionPoolCnt;   /** TNT 切换版本池次数 */
	char		m_switchVerPoolLastTime[30]; /** TNT 上次切换版本池的时间 */

	u64			m_reclaimVersionPoolCnt;  /** TNT 版本池回收次数 */
	char		m_reclaimVerPoolLastBeginTime[30]; /** TNT 上次版本池回收开始时间 */
	char		m_reclaimVerPoolLastEndTime[30]; /** TNT 上次版本池回收结束时间 */
	
	VerpoolStatus m_verpoolStatus;	  /** 版本池状态 */

	TNTGlobalStatus() {
	}
};

struct TNTStatus {
	uint		m_purgeCnt;			/** TNT Purge的次数 */
	uint		m_numPurgeSkipTable;/** TNT Purge跳过的表的数量*/
	uint		m_purgeMaxTime;		/** TNT Purge最长消耗的时间 */
	uint		m_purgeTotalTime;	/** TNT Purge总共消耗的时间 */
	uint		m_purgeLastTime;	/** TNT 上次Purge消耗的时间 */
	uint		m_purgeMaxBeginTime;/** TNT 最长的一次purge的开始时间*/

	uint		m_defragHashCnt;	/** TNT DefragHash的次数*/
	uint		m_numDefragHashSkipTable; /** TNT Defrag Hash 跳过的表数量*/

	uint		m_dumpCnt;			/** TNT Dump的次数*/

	u64			m_openTableCnt;		/** TNT openTable的次数*/
	u64			m_closeTableCnt;	/** TNT closeTable的次数*/

	u64			m_freeMemCallCnt;	/** TNT 内存不够调用freeMem的次数*/

	u64			m_switchVersionPoolCnt;   /** TNT 切换版本池次数 */
	u32			m_switchVerPoolLastTime;  /** TNT 上次切换版本池时间 */
	u64			m_reclaimVersionPoolCnt;  /** TNT 版本池回收次数 */
	u32			m_reclaimVerPoolLastBeginTime;	 /** TNT 上次版本池回收开始时间 */
	u32			m_reclaimVerPoolLastEndTime;	 /** TNT 上次版本池回收结束时间 */


	TNTStatus() {
		m_purgeCnt = 0;
		m_numPurgeSkipTable = 0;
		m_purgeMaxTime = 0;
		m_purgeTotalTime = 0;
		m_purgeLastTime = 0;
		m_purgeMaxBeginTime = 0;

		m_defragHashCnt = 0;
		m_numDefragHashSkipTable = 0;
		m_dumpCnt = 0;
		m_openTableCnt = 0;
		m_closeTableCnt = 0;
		
		m_freeMemCallCnt = 0;
		m_switchVersionPoolCnt = 0; 
		m_switchVerPoolLastTime = 0;
		m_reclaimVersionPoolCnt = 0; 
		m_reclaimVerPoolLastBeginTime = 0;
		m_reclaimVerPoolLastEndTime = 0;
	}
};



enum TNTStat {
	TNTIM_NOOP,
	TNTIM_PURGEING,
	TNTIM_DUMPING,
	TNTIM_DEFRAGING
};

enum PurgeTarget {
	PT_NONE,
	PT_DEFRAG,
	PT_PURGEPHASE1,
	PT_PURGEPHASE2,
};

enum RedoType {
	RT_COMMIT,
	RT_PREPARE
}; 

struct PurgeAndDumpProcess {
public: 
	PurgeAndDumpProcess() {
		m_purgeTrxId = INVALID_TRX_ID;
	}
	u16				m_tabPurged;		// purge操作已成功purge的表数量
	TNTTable		*m_curTab;		    // purge操作当前正在purge的表
	u32				m_begin;			// purge操作开始时间
	u32				m_end;				// purge操作结束时间
	TrxId           m_purgeTrxId;		// purge事务号
	TNTTransaction	*m_purgeTrx;		// purge操作所属事务
	Connection		*m_conn;			// purge操作的connection
	Session			*m_session;			// purge操作的session
	TrxId           m_minTrxId;         // purge操作的最小readView

	LsnType			m_dumpLsn;			// dumppoint lsn，dump恢复起点
	bool			m_dumpSuccess;		// dump是否成功（是否是purge了所有的表）
};



/*struct DumpRecover {
	uint	m_purgeStatus;		// crash时，purge操作所处于的状态; 0, 1, 2, 3, 4
	uint	m_preparedTrxs;		// redo完成之后，处于prepare状态的事务数量
	uint	m_activeTrxs;		// redo完成之后，活跃的事务数量
	TrxId	m_maxTrxId;			// 恢复过程中，记录最大的事务ID
};*/

struct RecoverProcess {
	TNTTransaction		*m_trx;
	DList<LogEntry *>	*m_logs;
	Session				*m_session;
};

struct ReclaimLobProcess {
	Session				*m_session;
	Connection		    *m_conn;
	TrxId			m_minReadView;
	u32				m_begin;			// reclaim lob操作开始时间
	u32				m_end;				// reclaim lob操作结束时间
};

struct DefragHashIndexProcess {
public:
	Connection		*m_conn;			// defragHashIndex操作的connection
	Session			*m_session;			// defragHashIndex操作的session
	u32				m_begin;			// defragHashIndex操作开始时间
	u32				m_end;				// defragHashIndex操作结束时间
	TNTTransaction	*m_trx;				// defragHashIndex操作所属事务
};

struct SwitchVerPoolProcess {
public:
	Connection		*m_conn;			// SwitchVerPoolProcess操作的connection
	Session			*m_session;			// SwitchVerPoolProcess操作的session
};

/** 备份任务及处理状态 */
struct TNTBackupProcess {
	TNTBackupProcess(BackupProcess *ntseBackupProc, TNTDatabase *tntDb) {
		m_ntseBackupProc = ntseBackupProc;
		m_tntDb = tntDb;
		m_stat = BS_INIT;
	}

	BackupProcess  *m_ntseBackupProc;
	TNTDatabase    *m_tntDb;
	BackupStat		m_stat;
};

// TNT引擎database，全局唯一
// 封装了TNT引擎所有的操作
class TNTDatabase {
public:
	// 析构函数
	~TNTDatabase();
	// database 初始化/打开/关闭等基本操作
	static TNTDatabase* open(TNTConfig *config, u32 flag, int recover) throw(NtseException);
	static void create(TNTConfig *config) throw(NtseException);
	static void upgradeNtse2Tnt(TNTConfig *config) throw(NtseException);
	static void drop(const char *dir, const char *logDir);
	void close(bool flushLog = true, bool flushData = true);
	
	// TNT Table 创建/打开/查找/关闭等操作
	// TNT DDL操作，需要提交当前事务，然后新开事务进行DDL
	void createTable(Session *session, const char *path, TableDef *tableDef) 
		throw(NtseException);
	void dropTable(Session *session, const char *path, int timeoutSec = -1) throw(NtseException);
	TNTTable* openTable(Session *session, const char *path, bool needDDLLock = true) throw(NtseException);
	TNTTable* pinTableIfOpened(Session *session, const char *path, int timeoutMs);
	void closeTable(Session *session, TNTTable *table);
	void truncateTable(Session *session, TNTTable *table, bool isTruncateOper = true) throw(NtseException);
	bool realCloseTableIfNeed(Session *session, Table *table) throw(NtseException);
	void renameTable(Session *session, const char *oldPath, 
		const char *newPath, int timeoutSec = -1) throw(NtseException);
	void addIndex(Session *session, TNTTable *table, u16 numIndice, 
		const IndexDef **indexDefs) throw(NtseException);
	void dropIndex(Session *session, TNTTable *table, uint idx) throw(NtseException);
	void alterTableArgument(Session *session, TNTTable *table, 
		const char *name, const char *valueStr, int timeout, bool inLockTables) throw(NtseException);
	void alterCtrlFileHasCprsDic(Session *session, u16 tblId, bool hasCprsDic);
	// NTSE在此函数中实现部分online操作
	void stopBgCustomThd();
	void cancelBgCustomThd(uint connId) throw(NtseException);
	void doOptimize(Session *session, const char *path, bool keepDict, bool *cancelFlag) throw(NtseException);
	void optimizeTable(Session *session, TNTTable *table, bool keepDict = false, bool waitForFinish = true) throw(NtseException);
	// 更新表的flush lsn；TNT有两个日志文件，因此需要特殊处理
	void bumpFlushLsn(Session *session, u16 tableId, bool flushed = true);
	void createCompressDic(Session *session, TNTTable *table) throw(NtseException);
	
	// TNT 检查点
	void checkPoint(Session *session, uint targetTime = 0) throw(NtseException);
	void setCheckpointEnabled(bool enabled);

	// TNT Version Pool
	void initSwitchVerPool(SwitchVerPoolProcess *proc);
	void delInitSwitchVerPool(SwitchVerPoolProcess *proc);
	void switchActiveVerPoolIfNeeded();

	void freeMem(const char *file = NULL, uint line = 0);
	
	// TNT 备份操作，由于NTSE的备份已由NTSE Database实现，因此此处只需要实现TNTIM Dump即可
	/*static void restore(const char *backupDir, const char *dumpDir, 
		const char *dir) throw(NtseException);*/
	bool dumpJudgeNeed();
	void doDump() throw(NtseException);
	void finishingDumpAndLock() throw(NtseException);
	void doneDump();
	
	// TNT dump & purge
	void initPurgeAndDump(PurgeAndDumpProcess *purgeAndDumpProcess, bool *needDump, bool log) throw(NtseException);
	void deInitDump(PurgeAndDumpProcess *purgeAndDumpProcess);
	void deInitPurgeAndDump(PurgeAndDumpProcess *purgeAndDumpProcess, bool needDump);
	void beginDumpTntim(PurgeAndDumpProcess *purgeAndDumpProcess);
	void finishDumpTntim(PurgeAndDumpProcess *dumpProcess);
	void purgeAndDumpTntim(PurgeTarget purgeTarget, bool skipSmallTables = false, bool needDump = false, bool log = true) throw(NtseException);

	bool purgeJudgeNeed();
	
	// 回收内存索引 
	void reclaimMemIndex() throw(NtseException); 

	// purge单表操作主函数入口，给定表，对表上的索引进行purge，释放空间
	void purgeTntimTable(TNTTable *table, TrxId purgeTrxId) throw(NtseException);
	// TNTIM purge操作，提供给mysql用户以命令，第一阶段暂不实现
	void doPurge() throw(NtseException);
	void finishingPurgeAndLock() throw(NtseException);
	void donePurge();

	bool masterJudgeNeed();
	uint defragHashIndex(bool skipSmallTable) throw(NtseException);
	// 关闭部分应用计数为0的表
	void closeOpenTablesIfNeed();

	TNTBackupProcess* initBackup(const char *backupDir) throw(NtseException);
	void doBackup(TNTBackupProcess *backupProcess) throw(NtseException);
	void finishingBackupAndLock(TNTBackupProcess *backupProcess) throw(NtseException);
	void doneBackup(TNTBackupProcess *backupProcess);
	static void restore(const char *backupDir, const char *baseDir, const char *logDir) throw(NtseException);
	
	//void flushMHeap(Session *session, TNTTable *table);
	// database 当前状态查询，必要的话，需要封装NTSE Database类的get方法
	Database* getNtseDb() const {return m_db;}
	TNTConfig* getConfig() const {return m_tntConfig;}
	DbStat getStat() const {return m_dbStat;}

	TblLobHashMap* getPurgeInsertLobHash() const {return m_purgeInsertLobs;}
	
	void reportStatus(TNTGlobalStatus *status);
	void printStatus(ostream &out);
	
	CommonMemPool* getCommonMemPool() const {return m_commonMemPool;}
	TNTIMPageManager* getTNTIMPageManager() const {return m_TNTIMpageManager;}
	VersionPool* getVersionPool() const {return m_vPool;}
	TNTTrxSys* getTransSys() const {return m_tranSys;}
	Txnlog   * getTNTLog() const {return m_tntTxnLog;}
	Syslog* getTNTSyslog() const {return m_tntSyslog;}
	TNTControlFile *getTNTControlFile() const {return m_tntCtrlFile;}
	TNTConfig *getTNTConfig() const {return m_tntConfig;}
	bool undoTrxByLog(Session *session, LsnType beginLsn, LsnType endLsn);
	bool undoTrxByLog(Session *session, DList<LogEntry *> *logs);
	
	void reclaimLob() throw(NtseException);

	void getAllTableBaseIds(Session *session, vector<u16> *ids, bool skipSmallTable, bool mheap, u64 limit, uint* numSkipTables = NULL);

	void killHangTrx();
#ifdef NTSE_UNIT_TEST
	Hash<u16, TNTTableInfo *>* getIdToTablesHash() {return m_idToTables;}
#endif
	void setOpenTableCnt(u32 tableOpenCnt);
	
	inline UKLockManager* getUniqueKeyLock(){
		return m_uniqueKeyLockMgr;
	}

#ifndef NTSE_UNIT_TEST
private:
#endif

	TNTDatabase(TNTConfig *config);
	void openVersionPool(bool create) throw (NtseException);
	void closeVersionPool();
	void init();
	void recover(int recover) throw(NtseException);
	void verify(int recover);
	
	bool isTntShouldRecover();
	void waitBgTasks(bool all);
	void closeOpenTables(bool closeNtse = false);
	void closeTNTTableBases();
	u16  getNtseTableId();

	void realCloseTable(Session *session, TNTTableInfo *tableInfo, TableInfo *ntseTblInfo, bool flushDirty, bool closeTnt, bool closeNtse);
	void waitRealClosed(Session *session, const char *path, int timeoutSec = -1) throw(NtseException);

	void acquireATSLock(Session *session, ILMode mode, int timeoutMs, const char *file, uint line) throw(NtseException);
	void releaseATSLock(Session *session, ILMode mode);
	ILMode getATSLock(Session *session) const;
	void acquireDDLLock(Session *session, ILMode mode, const char *file, uint line) throw(NtseException);
	void releaseDDLLock(Session *session, ILMode mode);

	// ddl recover
	// 所有的ddl recover操作，由NTSE database类提供；TNTIM内存，不需要支持任何ddl recover
	
	// purge 
	void initRedoPurge(PurgeAndDumpProcess &purgeProcess, const RecoverProcess *proc);
	void setRedoPurgeReadView(PurgeAndDumpProcess &purgeProcess, TrxId minTrxId);
	void purgePhaseOne(Session *session, TNTTable *table);
	void purgeWaitTrx(TrxId purgeTrxId) throw(NtseException);
	void purgePhaseTwo(Session *session, TNTTable *table) throw(NtseException);
	void purgeTntimHeap(TNTTable *table);
	void purgeTntimIndice(TNTTable *table);
	void purgeCollectStats();
	void purgeMergePages() throw(NtseException);
	void redoPurgeTntim(TrxId trxId, bool commit) throw (NtseException);
	void purgeTNTTable(Session *session, TNTTable *table, PurgeTarget purgeTarget);

	static void parsePurgeBeginLog(const LogEntry *log, TrxId *txnId, LsnType *preLsn/*, TrxId *minReadView*/);
	LsnType writePurgeBeginLog(Session *session, TrxId trxId, LsnType preLsn/*, TrxId minReadView*/);
	LsnType writePurgeEndLog(Session *session, TrxId trxId, LsnType preLsn);
	void getPurgeMinTrxIdPerTable(Session *session, LogEntry *log, TrxId *minReadView);
	void purgeCompleteBeforeDDL(Session *session, TNTTable *table) throw (NtseException);
	
	// dump process
	static inline string generateDumpSNPath(const char *dumpBaseDir, uint dumpSN) {
		// m_dumpBaseDir + m_dumpSN
		string dumpSNPath(dumpBaseDir);
		char	ch[10];
		int ret = System::snprintf_mine(ch, sizeof(ch) / sizeof(char), "%d", dumpSN);
		NTSE_ASSERT(ret > 0);
		dumpSNPath.append(NTSE_PATH_SEP).append(ch).append(NTSE_PATH_SEP);
		return dumpSNPath;
	}

	inline string generateDumpFilePath(const char *dumpBaseDir, uint dumpSN, u16 tableId) {
		// m_dumpBaseDir + m_dumpSN + table_id.dump，序列号+表名.dump
		string dumpFilePath(dumpBaseDir);
		char sn_str[10];
		int ret = System::snprintf_mine(sn_str, sizeof(sn_str) / sizeof(char), "%d", dumpSN);
		NTSE_ASSERT(ret > 0);
		char tableId_str[10];
		ret = System::snprintf_mine(tableId_str, sizeof(tableId_str) / sizeof(char), "%d", tableId);
		NTSE_ASSERT(ret > 0);
		dumpFilePath.append(NTSE_PATH_SEP).append(sn_str).append(NTSE_PATH_SEP).append(tableId_str).append(Limits::NAME_DUMP_EXT);
		return dumpFilePath;
	}
	void dumpTableHeap(PurgeAndDumpProcess *purgeAndDumpProcess) throw(NtseException);

	void initDefragHashIndex(DefragHashIndexProcess *defragHashIndexProcess) throw(NtseException);
	void deInitDefragHashIndex(DefragHashIndexProcess *defragHashIndexProcess);

	//Reclaim Lob
	void initReclaimLob(ReclaimLobProcess *reclaimLobProcess) throw(NtseException);
	void deInitReclaimLob(ReclaimLobProcess *reclaimLobProcess);
	
	// dump recover
	//void initDumpRecover();
	void readDump() throw(NtseException);
	void getNextPage() throw(NtseException);

	bool hasTableId(const LogEntry *log);
	bool judgeLogEntryValid(u64 lsn, const LogEntry *log);
	
	void dumpRecoverApplyLog() throw(NtseException);
	// 验证是否有需要删除的内存表(Ntse恢复过程中，ControlFile删除的表)
	bool dumpRecoverVerifyHeaps();
	// Tntim heap恢复完成之后(包括redo/undo),根据表定义，build内存索引
	void dumpRecoverBuildIndice() throw(NtseException);
	void dumpDoneRecover();

	void dumpEndRecover(bool succ);

	static LogEntry *copyLog(const LogEntry *log, MemoryContext *ctx);
	// 根据不同日志类型，进行redo
	void recoverBeginTrx(TrxId trxId, LsnType lsn, u8 versionPoolId);
	void recoverCommitTrx(TrxId trxId);
	void recoverPrepareTrx(TrxId trxId, const LogEntry *log);
	void recoverBeginRollBackTrx(TrxId trxId, const LogEntry *log);
	void recoverRollBackTrx(TrxId trxId);
	void recoverPartialBeginRollBack(TrxId trxId, const LogEntry *log);
	void recoverPartialEndRollBack(TrxId trxId, const LogEntry *log);

	void releaseRecoverProcess(RecoverProcess *proc);
	void makeupCommitTransaction(TrxId trxId, bool log);

	void recoverRedoPrepareTrxs(bool enableLog, bool free);
	void recoverRedoTrx(RecoverProcess *proc, RedoType redoType);
	void recoverUndoTrxs(TrxState target, TrxId purgeTrxId);

	void addRedoLogToTrans(TrxId trxId, const LogEntry* log);
	void updateTrxLastLsn(TrxId trxId, LsnType lsn);

	TNTTable* getTNTTable(Session *session, u16 tableId) throw(NtseException);

	bool isNeedRedoLog(const LogEntry *log);
	void redoLogEntry(Session *session, const LogEntry *log, RedoType redoType);
	//用于crash recover和log recover
	LsnType undoLogEntry(Session *session, TNTTable * table, const LogEntry *log, const bool crash);

	void addTransactionLobId(LsnType logLsn, TrxId rollbackTrxId, const LogEntry* log);

	void redoReclaimLob();

	void addLobIdToPurgeHash(TrxId purgeTrxId, const LogEntry* log);
	void removePurgeHashLobIds();

	// crash recovery结束，将DumpRecover中处于prepare状态的事务copy到TransactionSys中
	void copyPreparedTrxsToTransSys();
	//void releaseDumpRecover();

	static TrxId parseTrxId(const LogEntry *log);
	static LsnType parsePreLsn(const LogEntry *log);

	//获取回滚事务表中最早开始rollback的lsn
	LsnType getFirstUnfinishedRollbackLsn();

	void createBackupFile(const char *path, byte *buf, size_t bufSize) throw(NtseException);
	void backupTNTCtrlAndDumpFile(TNTBackupProcess *backupProcess) throw(NtseException);
	static byte *readFile(const char *path, u64 *bufSize) throw(NtseException);
	void releaseBackupProcess(TNTBackupProcess *tntBackupProcess);
	
	void parseTntRedoLogForDebug(Syslog *syslog, const LogEntry *logEntry, LsnType lsn);
	void parseSubRecord(SubRecord *updateSub, TableDef *tableDef);

	

private:
	typedef Hash<char *, TNTTableInfo *, StrNoCaseHasher, StrNoCaseEqualer> PathHash;
	static const int MAX_OPEN_TABLES;	/** 系统最多支持同时打开这么多个表 */
	static const int MASTER_INTERVAL = 7;   /** master线程的间隔时间，单位为s */
	static const int DEFRAG_HASH_INTERVAL = 1;	/** defrag hash 线程的间隔时间，单位为s*/
	static const int CLOSE_OPEN_TABLE_INTERVAL = 10; /** 关闭引用为0的表的线程的间隔时间*/
	static const int RECLAIM_MINDEX_INTERVAL = 5;	/** 回收内存索引页面线程的间隔时间，单位为s*/
#ifndef NTSE_UNIT_TEST
	static const int MAX_DEFRAG_NUM = 50;    /** 一次最多整理几个页面 */
#else
	static const int MAX_DEFRAG_NUM = 5;
#endif

    static const int TABLE_PURGE_THRESHOLD        = 5;  /** purge 中tntim几个页面以下的表跳过不做*/
    static const int TABLE_DEFRAG_HASH_INDEX_THRESHOLD = 1000; /** defrag hash index时小于该项则跳表*/
	
	Database 					*m_db;			// ntse database;
	// 存储已open表的hash表，hash表内存放的是TNT Table实例
	// 注意点：m_db实例中，有存储NTSE Table实例的hash表
	Hash<u16, TNTTableInfo *>	*m_idToTables;	// table id hash
	PathHash					*m_pathToTables;
	
	PagePool                    *m_commonPool;
	PagePool					*m_pagePool;	// TNT引擎的page pool
	TNTConfig					*m_tntConfig;
	DbStat						m_dbStat;
	bool						m_closed;		// 当前数据库是否已经关闭
	bool						m_unRecovered;
	
	// TNT controlfile，保存属于TNT特有的控制信息
	TNTControlFile				*m_tntCtrlFile;	// TNT controlfile
	ControlFile					*m_ntseCtrlFile;
	Syslog						*m_tntSyslog;
	// TNT redo日志文件，用于写入TNT redo日志
	Txnlog						*m_tntTxnLog;
	
	// TNT引擎crash recovery需要的结构
	//DumpRecover					*m_dumpRecover;
	
	// TNT引擎需要定时触发的任务，包括Dump与Purge
	// 任务运行的过程中，TNTStatus变量将会被设置为相应状态
	Mutex						*m_taskLock;	// mutex control tntim dump
	Task                        *m_masterTask;
	Task						*m_purgeAndDumpTask; //tntim purge,defrag,dump
	Task						*m_defragHashTask;	// tntim defrag hash index
	Task                        *m_closeOpenTablesTask; //close tnt tables
	Task						*m_reclaimMemIndexTask;	// tntim reclaim memory index page
	TNTStat						m_tntState;	// 
public:
	TNTStatus					m_tntStatus; //tnt统计信息
private:
	// 将TNT事务全局变量放在TNTDatabase中
	// 在Database初始化时创建
	// crash recovery，根据日志中事务的提交状态，重构m_tranSys
	TNTTrxSys				    *m_tranSys;		// transaction sys
	
	// 记录TNT Table基本信息的链表,以及Hash表
	// 无论表是否real close，base信息不会发生变化
	DList<TNTTableBase *>		m_tntTabBases;
	Hash<u16, TNTTableBase *>	*m_idToTabBase;

	// 版本池
	VersionPool					*m_vPool;
	UKLockManager				*m_uniqueKeyLockMgr;

	CommonMemPool               *m_commonMemPool;   /** 通用内存池 */
	TNTIMPageManager            *m_TNTIMpageManager;/** TNTIM内存池 */

	Hash<TrxId, RecoverProcess *>	*m_activeTrxProcs;	/** 恢复过程中活跃事务 */

	Hash<TrxId, RecoverProcess *>	*m_rollbackTrx; //记录在恢复时正在回滚的事务
	TblLobHashMap                   *m_purgeInsertLobs; //记录在purge过程中被插入的lobId集合

	Atomic<int>						m_backingUp;		/** 正在备份 */

	friend class ntse::Database;
};

}

#endif