/**
 * ���ݿ⼰�����ֵ�
 *
 * @author ��Դ(wangyuan@corp.netease.com, wy@163.org)
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

/** ����״̬ */
struct Status {
public:
	u64	m_realOpenTableCnt;		/** ���� OpenTable�Ĵ���*/
	u64	m_realCloseTableCnt;	/** ���� CloseTable�Ĵ���*/
	u64 m_pageBufSize;		/** ��ǰҳ�滺���С */
	OpStat	m_opStat;			/** ����˽������״̬�Ļ��� */
	BufferStatus	m_bufStat;	/** ҳ�滺��ȫ��ͳ����Ϣ */
	MmsStatus		m_mmsStat;	/** MMSȫ��ͳ����Ϣ */
	LogStatus		m_logStat;	/** ��־ģ��ͳ����Ϣ */
	u64		m_bufPendingReads;	/** ҳ�滺�����ڽ����еĶ��������� */
	u64		m_bufPendingWrites;	/** ҳ�滺�����ڽ����е�д�������� */
	u64		m_realDirtyPages;	/** ҳ�滺������ҳ���� */
	LockTableStatus m_rowlockStat; /** ������ͻ״̬ͳ�� */
	u64		m_checkpointCnt;	   /** ������� */
	Status(): m_opStat(NULL) {
		m_realOpenTableCnt = 0;
		m_realCloseTableCnt = 0;
	}
};

/** ���ݲ���ִ��״̬ */
enum BackupStat {
	BS_INIT,	/** ��ʼ״̬ */
	BS_FAILED,	/** ʧ���� */
	BS_COPYED,	/** ִ����doBackup�ɹ�����û�м��� */
	BS_LOCKED,	/** ִ����finishingBackupAndLock�ɹ� */
};

class Session;
class LogBackuper;
/** �������񼰴���״̬ */
struct BackupProcess {
	BackupProcess(Database *db, const char* backupDir, uint bufPages);
	~BackupProcess();

	Database	*m_db;		/** �������ݿ� */
	char	*m_backupDir;	/** ����Ŀ¼ */
	uint	m_bufPages;		/** �����С */
	byte*	m_buffer;		/** ����ռ� */
	Session	*m_session;		/** ���ݻỰ */
	Connection	*m_conn;		/** �������� */
	LogBackuper	*m_logBackuper;	/** ��־������ */
	LsnType	m_tailLsn;		/** ���ݽ���ʱ��LSN */
	BackupStat	m_stat;		/** ִ��״̬ */
	bool	m_ckptEnabled;	/** ����֮ǰ�ļ���״̬ */
	int		m_onlineLsnToken; /** ���ڴ���onlineLsn */
	map<File *, u64> m_files;  /** �ѱ��ݵ��ļ���  */
	vector<Table *> m_tables;	  /** �ѱ��ݵı� */
};

class Table;
/** ����Ϣ */
struct TableInfo {
public:
	Table	*m_table;	/** ����� */
	char	*m_path;	/** ���ļ�·�� */
	uint	m_refCnt;	/** ���ü��� */

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

/** ҳ�滺��ֲ����ʾĳ���ĳ�������ҳ�滺����ռ�õ�ҳ���� */
struct BufUsage {
	u16			m_tblId;		/** ��ID */
	const char	*m_tblSchema;	/** ������schema */
	const char	*m_tblName;		/** ���� */
	DBObjType	m_type;			/** �������� */
	const char	*m_idxName;		/** ��������ΪBU_Indexʱָ��������������Ϊ"" */
	u64			m_numPages;		/** ��ҳ�滺����ռ�õ�ҳ���� */

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

/** ���ݿ�״̬ */
enum DbStat {
	DB_STARTING,		/** �������� */
	DB_MOUNTED,			/** �Ѿ����� */
	DB_RECOVERING,		/** ���ڽ��лָ� */
	DB_RUNNING,			/** ��������״̬ */
	DB_CLOSING,			/** ���ڹر� */
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

/** ���ݿ� */
class Database {
public:
	// TNT��������������
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
	static const int MAX_OPEN_TABLES;		/** ϵͳ���֧��ͬʱ����ô����� */

	Hash<u16, TableInfo *>		*m_idToTables;		/** ��Ծ�ı�ID��������ӳ��� */
	PathHash	*m_pathToTables;	/** ��Ծ�ı��·����������ӳ�����Сд������ */
	bool		m_closed;			/** �Ƿ��Ѿ����ر� */
	bool		m_unRecovered;		/** �Ƿ�Ϊδ�ָ�״̬������ǿ�Ʋ��ָ�ģʽ�򿪵����ݿ� */
	bool		m_pkeRequiredForDnU;/** ���ݿ����UPDATE/DELETE������ʱ���Ƿ���ҪPKE */
	Config		*m_config;			/** ���ò��� */
	Status		m_status;			/** ����״̬ */
	Syslog		*m_syslog;			/** ϵͳ��־ */
	ControlFile	*m_ctrlFile;		/** �����ļ� */
	Txnlog		*m_txnlog;			/** ������־ */
	//tnt�У�database��pagePoolΪnull�����е�poolUser��ע����TNTDatabase��pagepool��
	PagePool	*m_pagePool;		/** �ڴ�ҳ�� */
	Buffer		*m_buffer;			/** ҳ�滺�� */
	LockManager	*m_rowLockMgr;		/** ���������� */
	IndicesLockManager *m_idxLockMgr;/** ����ҳ���������� */
	SessionManager	*m_sessionManager;	/** �Ự������ */
	Mms			*m_mms;				/** MMSϵͳ */
	IntentionLock	*m_atsLock;		/** ���ֻ�Ծ���ϵ��� */
	IntentionLock	*m_ddlLock;		/** ����DDL����ʱҪ�ӵ����������Ҫͬʱ��m_ddlLock��m_openLock��
									 * ��m_ddlLock������m_openLock֮ǰ�Է�ֹ��������һ˳������
									 * ���߱��ݹ��̾����ġ�
									 */
	Mutex		*m_chkptLock;		/** ���м���ʱҪ�ӵ��� */
	Task		*m_chkptTask;		/** ���ڽ��м�������� */
	Hash<u16, Transaction *>	*m_activeTransactions;	/** �ָ������л�Ծ���� */
	DbStat		m_stat;				/** ���ݿ�״̬ */
	Atomic<int>	m_backingUp;		/** ���ڱ��� */
	Mutex		*m_bgtLock;			/** ��̨�߳��� */
	vector<BgTask *>	*m_bgTasks;		/** ��̨�߳� */

	NTSECallbackManager	*m_cbManager;	/** �в����ص������� */
	BgCustomThreadManager *m_bgCustomThdsManager;/** ת���ִ̨�е��û��̹߳����� */

	DList<Table*>		  *m_openTablesLink;		/** ���򿪵�Table����*/
};

/**
 * Ψһ�Լ�ֵ������
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

