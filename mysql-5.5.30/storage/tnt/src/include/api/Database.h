/**
 * 数据库及数据字典
 *
 * @author 汪源(wangyuan@corp.netease.com, wy@163.org)
 */

#ifndef _NTSE_DATABASE_H_
#define _NTSE_DATABASE_H_

#include <set>
#include <string>
#include <vector>
#include <string>
#include "util/Portable.h"
#include "misc/Global.h"
#include "util/System.h"
#include "util/DList.h"
#include "util/Sync.h"
#include "util/Hash.h"
#include "misc/Txnlog.h"
#include "misc/Syslog.h"
#include "misc/Session.h"
#include "misc/Config.h"
#include "misc/Buffer.h"
#include "mms/Mms.h"
#include "misc/LockManager.h"
#include "misc/Callbacks.h"
#include "misc/BgCustomThread.h"

using namespace std;

#ifdef NTSE_KEYVALUE_SERVER

struct st_table;
typedef struct st_table TABLE;

#endif

namespace ntse {

/** 运行状态 */
struct Status {
public:
	u64	m_realOpenTableCnt;		/** 真正 OpenTable的次数*/
	u64	m_realCloseTableCnt;	/** 真正 CloseTable的次数*/
	u64 m_pageBufSize;		/** 当前页面缓存大小 */
	OpStat	m_opStat;			/** 连接私有运行状态的汇总 */
	BufferStatus	m_bufStat;	/** 页面缓存全局统计信息 */
	MmsStatus		m_mmsStat;	/** MMS全局统计信息 */
	LogStatus		m_logStat;	/** 日志模块统计信息 */
	u64		m_bufPendingReads;	/** 页面缓存正在进行中的读操作个数 */
	u64		m_bufPendingWrites;	/** 页面缓存正在进行中的写操作个数 */
	u64		m_realDirtyPages;	/** 页面缓存中脏页个数 */
	LockTableStatus m_rowlockStat; /** 行锁冲突状态统计 */
	u64		m_checkpointCnt;	   /** 检查点次数 */
	Status(): m_opStat(NULL) {
		m_realOpenTableCnt = 0;
		m_realCloseTableCnt = 0;
	}
};

/** 备份操作执行状态 */
enum BackupStat {
	BS_INIT,	/** 初始状态 */
	BS_FAILED,	/** 失败了 */
	BS_COPYED,	/** 执行了doBackup成功，还没有加锁 */
	BS_LOCKED,	/** 执行了finishingBackupAndLock成功 */
};

class Session;
class LogBackuper;
/** 备份任务及处理状态 */
struct BackupProcess {
	BackupProcess(Database *db, const char* backupDir, uint bufPages);
	~BackupProcess();

	Database	*m_db;		/** 所属数据库 */
	char	*m_backupDir;	/** 备份目录 */
	uint	m_bufPages;		/** 缓存大小 */
	byte*	m_buffer;		/** 缓存空间 */
	Session	*m_session;		/** 备份会话 */
	Connection	*m_conn;		/** 备份连接 */
	LogBackuper	*m_logBackuper;	/** 日志备份者 */
	LsnType	m_tailLsn;		/** 备份结束时的LSN */
	BackupStat	m_stat;		/** 执行状态 */
	bool	m_ckptEnabled;	/** 备份之前的检查点状态 */
	int		m_onlineLsnToken; /** 用于处理onlineLsn */
	map<File *, u64> m_files;  /** 已备份的文件集  */
	vector<Table *> m_tables;	  /** 已备份的表 */
};

class Table;
/** 表信息 */
struct TableInfo {
public:
	Table	*m_table;	/** 表对象 */
	char	*m_path;	/** 表文件路径 */
	uint	m_refCnt;	/** 引用计数 */

public:
	TableInfo(Table *table, const char *path) {
		m_table = table;
		m_path = System::strdup(path);
		m_refCnt = 1;
	}

	~TableInfo() {
		delete []m_path;
		m_table = NULL;
		m_path = NULL;
	}
};

/** 页面缓存分布项，表示某表的某类对象在页面缓存中占用的页面数 */
struct BufUsage {
	u16			m_tblId;		/** 表ID */
	const char	*m_tblSchema;	/** 表所属schema */
	const char	*m_tblName;		/** 表名 */
	DBObjType	m_type;			/** 对象类型 */
	const char	*m_idxName;		/** 仅在类型为BU_Index时指定索引名，否则为"" */
	u64			m_numPages;		/** 在页面缓存中占用的页面数 */

	BufUsage(u16 tblId, const char *tblSchema, const char *tblName, DBObjType type, const char *idxName) {
		m_tblId = tblId;
		m_tblSchema = System::strdup(tblSchema);
		m_tblName = System::strdup(tblName);
		m_type = type;
		m_idxName = System::strdup(idxName);
		m_numPages = 0;
	}

	~BufUsage() {
		delete [] m_tblSchema;
		delete [] m_tblName;
		delete [] m_idxName;
	}

	static const char *getTypeStr(DBObjType type) {
		switch (type) {
		case DBO_Heap:
			return "Heap";
		case DBO_LlobDir:
			return "LlobDir";
		case DBO_LlobDat:
			return "LlobDat";
		case DBO_Slob:
			return "Slob";
		case DBO_Indice:
			return "Indice";
		case DBO_Index:
			return "Index";
		case DBO_Unknown:
			return "Unknown";
		default:
			assert(false);
			return "Unknown";
		}
	}
};

/** 数据库状态 */
enum DbStat {
	DB_STARTING,		/** 正在启动 */
	DB_MOUNTED,			/** 已经加载 */
	DB_RECOVERING,		/** 正在进行恢复 */
	DB_RUNNING,			/** 正常运行状态 */
	DB_CLOSING,			/** 正在关闭 */
};

class File;
class Table;
class Txnlog;
class Buffer;
class LockManager;
class IndicesLockManager;
class Syslog;
class PagePool;
class SessionManager;
class TableDef;
class Mms;
class ControlFile;
class Transaction;
class IndexDef;
class DrsIndice;
class BgCustomThreadManager;

/** 数据库 */
class Database {
public:
	// TNT引擎特殊需求函数
	static void preOpen(TNTDatabase *db, int recover, bool pkeRequiredForDnU, bool *bRecovered) throw(NtseException);
	void postOpen(int recover, bool bRecovered);
	TableInfo* getTableInfo(u16 tableId);
	TableInfo* getTableInfo(const char * path);

	~Database();
	static Database* open(Config *config, bool autoCreate, int recover = 0, bool pkeRequiredForDnU = false) throw(NtseException);
	static void create(Config *config) throw(NtseException);
#ifdef TNT_ENGINE
	static void drop(const char *dir, const char *logDir = NULL);
#else
	static void drop(const char *dir);
#endif
	void close(bool flushLog = true, bool flushData = true);

	void createTable(Session *session, const char *path, TableDef *tableDef) throw(NtseException);
	void dropTable(Session *session, const char *path, int timeoutSec = -1) throw(NtseException);
	void dropTableSafe(Session *session, const char *path, int timeoutSec = -1) throw(NtseException);
	Table* openTable(Session *session, const char *path) throw(NtseException);
	Table* pinTableIfOpened(Session *session, const char *path, int timeoutMs = -1);
	void closeTable(Session *session, Table *table);
	void truncateTable(Session *session, Table *table, bool tblLock = true, bool isTruncateOper = true) throw(NtseException);
	void renameTable(Session *session, const char *oldPath, const char *newPath, int timeoutSec = -1) throw(NtseException);
	void renameTableSafe(Session *session, const char *oldPath, const char *newPath, int timeoutSec) throw(NtseException);
	void addIndex(Session *session, Table *table, u16 numIndice, const IndexDef **indexDefs) throw(NtseException);
	void dropIndex(Session *session, Table *table, uint idx) throw(NtseException);
	void alterTableArgument(Session *session, Table *table, const char *name, const char *valueStr, int timeout, bool inLockTables = false) throw(NtseException);
	void alterCtrlFileHasCprsDic(Session *session, u16 tblId, bool hasCprsDic);
	void optimizeTable(Session *session, Table *table, bool keepDict = false, bool waitForFinish = true) throw(NtseException);
	void doOptimize(Session *session, const char *path, bool keepDict, bool *cancelFlag) throw(NtseException);
	void cancelBgCustomThd(uint connId) throw(NtseException);
	inline BgCustomThreadManager *getBgCustomThreadManager() {
		return m_bgCustomThdsManager;
	}
	void bumpFlushLsn(Session *session, u16 tableId, bool flushed = true);
	void bumpTntAndNtseFlushLsn(Session *session, u16 tableId, bool flushed = true);
	void createCompressDic(Session *session, Table *table) throw(NtseException);

	DbStat getStat() const;
	Config* getConfig() const;
	Txnlog* getTxnlog() const;
	Buffer* getPageBuffer() const;
	LockManager* getRowLockManager() const;
	IndicesLockManager* getIndicesLockManager() const;
	Syslog* getSyslog() const;
	SessionManager* getSessionManager() const;
	Mms* getMms() const;
	ControlFile* getControlFile() const;
	IntentionLock* getAtsLockInst() const {return m_atsLock;}
	IntentionLock* getDdlLockInst() const {return m_ddlLock;}

	void reportStatus(Status *status);
	Array<BufUsage *>* getBufferDistr(Session *session);
	void printStatus(ostream &out);
	uint getPendingIOOperations() const;
	u64 getNumIOOperations() const;

	Connection* getConnection(bool internal, const char *name = NULL);
	void freeConnection(Connection *conn);

	void checkpoint(Session *session, AioArray *array = NULL) throw(NtseException);
	void setCheckpointEnabled(bool enabled);
	void setCheckpointInterval(uint sec);
	void doCheckpoint();


	BackupProcess* initBackup(const char *backupDir) throw(NtseException);
	void doBackup(BackupProcess *backupProcess) throw(NtseException);
	void finishingBackupAndLock(BackupProcess *backupProcess) throw(NtseException);
	void doneBackup(BackupProcess *backupProcess);
#ifdef TNT_ENGINE
	static void restore(const char *backupDir, const char *dataDir, const char *logDir = NULL) throw(NtseException);
#else
	static void restore(const char *backupDir, const char *dir) throw(NtseException);
#endif
	void registerBgTask(BgTask *task);
	void unregisterBgTask(BgTask *task);

	NTSECallbackManager* getNTSECallbackManager();
	void registerNTSECallback(CallbackType type, NTSECallbackFN *cbfn);
	bool unregisterNTSECallback(CallbackType type, NTSECallbackFN *cbfn);
	bool isPkeRequiredForUnD() const;

	static bool isFileOfNtse(const char *name);
	static u16 getBestIncrSize(const TableDef *tableDef, u64 fileSize);

	Table* getTable(u16 tableId) throw(NtseException);
	void waitRealClosed(Session *session, const char *path, int timeoutSec = -1) throw(NtseException);
	void realCloseTable(Session *session, TableInfo *tableInfo, bool flushDirty);
	bool realCloseTableIfNeed(Session *session, Table *table) throw(NtseException);

	void closeOpenTables(bool flushData);

	TableInfo* getTableInfoById(u16 tableId);

	void backupCtrlFile(BackupProcess *backupProcess) throw(NtseException);
	void backupTable(BackupProcess *backupProcess, u16 tableId) throw(NtseException);
	void backupTxnlog(BackupProcess *backupProcess, bool firstPass) throw(NtseException);
	void rebackupExtendedFiles(BackupProcess *backupProcess);
	vector<string> getOpenedTablePaths(Session *session);
#ifdef TNT_ENGINE
	vector<Table *> getOpenTables(Session *session);
#endif
	DList<Table*>*	getOpenTableLRU() const;

private:
	Database(Config *config);
	void init();
	bool fileBackup(const char *dir) throw(NtseException);
	void recover() throw(NtseException);
	void redoCreateTable(Session *session, const LogEntry *log) throw(NtseException);
	void redoDropTable(Session *session, const LogEntry *log) throw(NtseException);
	void redoTruncate(Session *session, const LogEntry *log) throw(NtseException);
	void redoDropIndex(Session *session, LsnType lsn, const LogEntry *log);
	void redoRenameTable(Session *session, const LogEntry *log) throw(NtseException);
	void redoAlterTableArg(Session *session, const LogEntry *log) throw(NtseException);
	void redoAlterIndice(Session *session, const LogEntry *log) throw(NtseException);
	void redoAlterColumn(Session *session, const LogEntry *log);
	void redoCreateCompressDict(Session *session, const LogEntry *log);
	void redoMoveLobs(Session *session, LsnType lsn, const LogEntry *log) throw(NtseException);
	void redoBumpFlushLsn(Session *session, const LogEntry *log);
	void startTransaction(const LogEntry *log) throw(NtseException);
	void endTransaction(const LogEntry *log);
	void acquireATSLock(Session *session, ILMode mode, int timeoutMs, const char *file, uint line) throw(NtseException);
	void releaseATSLock(Session *session, ILMode mode);
	ILMode getATSLock(Session *session) const;
	void acquireDDLLock(Session *session, ILMode mode, const char *file, uint line) throw(NtseException);
	void releaseDDLLock(Session *session, ILMode mode);
	ILMode getDDLLock(Session *session) const;
	u64 doBackupFile(BackupProcess *backupProcess, File *src, PageType pageType, u16 tableId, DBObjStats *dbObjStats, DrsIndice *indice, u64 existLen) throw(NtseException);
	void backupFile(BackupProcess *backupProcess, File *src, PageType pageType, u16 tableId, DBObjStats *dbObjStats, DrsIndice *indice = NULL) throw(NtseException);
	void backupFile(BackupProcess *backupProcess, u16 tableId, const char *suffix) throw(NtseException);
	static void restoreTxnlog(ControlFile *ctrlFile, const char *basedir
		, const char *backupPath) throw(NtseException);
	static void restoreTable(ControlFile *ctrlFile, u16 tabId, const char *backupDir
		, const char *dir) throw(NtseException);
	TableInfo* doOpenTable(Session *session, const char *path) throw(NtseException);
	// void closeOpenTables(bool flushData);
	u64 writeCreateLog(Session *session, const char *path, const TableDef *tableDef);
	char* parseCreateLog(const LogEntry *log, TableDef *tableDef);
	u64 writeDropLog(Database *db, u16 tableId, const char *path);
	char* parseDropLog(const LogEntry *log);
	u64 writeRenameLog(Session *session, u16 tableId, const char *oldPath, const char *newPath, bool hasLob, bool hasCprsDic);
	void parseRenameLog(const LogEntry *log, char **oldPath, char **newPath, bool *hasLob, bool *hasCprsDic);
	u64 writeBumpFlushLsnLog(Session *session, u16 tableId, u64 flushLsn, bool flushed = true);
	u64 parseBumpFlushLsnLog(const LogEntry *log, bool *flushed);
	bool canSkipLog(u64 lsn, const LogEntry *log);
	void waitBgTasks(bool all);
	void verify();
	void flushMmsUpdateLogs();
#ifdef TNT_ENGINE
	void checkPermission(Session *session, const char *path) throw(NtseException);
#endif

private:
	typedef Hash<char *, TableInfo *, StrNoCaseHasher, StrNoCaseEqualer> PathHash;
	static const int MAX_OPEN_TABLES;		/** 系统最多支持同时打开这么多个表 */

	Hash<u16, TableInfo *>		*m_idToTables;		/** 活跃的表ID到表对象的映射表 */
	PathHash	*m_pathToTables;	/** 活跃的表的路径到表对象的映射表，大小写不敏感 */
	bool		m_closed;			/** 是否已经被关闭 */
	bool		m_unRecovered;		/** 是否为未恢复状态，即用强制不恢复模式打开的数据库 */
	bool		m_pkeRequiredForDnU;/** 数据库进行UPDATE/DELETE操作的时候是否需要PKE */
	Config		*m_config;			/** 配置参数 */
	Status		m_status;			/** 运行状态 */
	Syslog		*m_syslog;			/** 系统日志 */
	ControlFile	*m_ctrlFile;		/** 控制文件 */
	Txnlog		*m_txnlog;			/** 事务日志 */
	//tnt中，database的pagePool为null，所有的poolUser都注册于TNTDatabase的pagepool中
	PagePool	*m_pagePool;		/** 内存页池 */
	Buffer		*m_buffer;			/** 页面缓存 */
	LockManager	*m_rowLockMgr;		/** 行锁管理器 */
	IndicesLockManager *m_idxLockMgr;/** 索引页面锁管理器 */
	SessionManager	*m_sessionManager;	/** 会话管理器 */
	Mms			*m_mms;				/** MMS系统 */
	IntentionLock	*m_atsLock;		/** 保持活跃表集合的锁 */
	IntentionLock	*m_ddlLock;		/** 进行DDL操作时要加的锁。如果需要同时加m_ddlLock与m_openLock，
									 * 加m_ddlLock必须在m_openLock之前以防止死锁，这一顺序是由
									 * 在线备份过程决定的。
									 */
	Mutex		*m_chkptLock;		/** 进行检查点时要加的锁 */
	Task		*m_chkptTask;		/** 定期进行检查点的任务 */
	Hash<u16, Transaction *>	*m_activeTransactions;	/** 恢复过程中活跃事务 */
	DbStat		m_stat;				/** 数据库状态 */
	Atomic<int>	m_backingUp;		/** 正在备份 */
	Mutex		*m_bgtLock;			/** 后台线程锁 */
	vector<BgTask *>	*m_bgTasks;		/** 后台线程 */

	NTSECallbackManager	*m_cbManager;	/** 行操作回调管理器 */
	BgCustomThreadManager *m_bgCustomThdsManager;/** 转入后台执行的用户线程管理器 */

	DList<Table*>		  *m_openTablesLink;		/** 被打开的Table链表*/
};

/**
 * 唯一性键值锁锁表
 */
class UKLockManager {
public:
	UKLockManager(uint maxLocks) : m_maxLocks(maxLocks) {
		assert(maxLocks > 0);
		m_lockTable = new LockManager(maxLocks);
	}

	virtual ~UKLockManager() {
		delete m_lockTable;
	}

	inline bool tryLock(u16 threadId, u64 key) {
		return m_lockTable->tryLock(threadId, key, Exclusived);
	}

	inline bool lock(u16 threadId, u64 key) {
		return m_lockTable->lock(threadId, key, Exclusived);
	}

	inline void unlock(u64 key) {
		m_lockTable->unlock(key, Exclusived);
	}

protected:
	uint                  m_maxLocks;
	LockManager           *m_lockTable;
};

}

#endif

