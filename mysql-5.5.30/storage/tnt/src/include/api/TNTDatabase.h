/**
* TNT Databaseģ�顣
*
* @author �εǳ�
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
	uint		m_pageBufSize;		/** TNT Buffer�ĵ�ǰ�ռ��С */
	uint		m_freeBufSize;		/** TNT Buffer��ǰ����ҳ������ */

	uint		m_purgeCnt;			/** TNT Purge�Ĵ��� */
	uint		m_numPurgeSkipTable;/** TNT Purge�����ı������*/
	uint		m_purgeMaxTime;		/** TNT Purge�ʹ�õ�ʱ��*/
	uint		m_purgeAvgTime;		/** TNT Purgeƽ��ʹ�õ�ʱ��*/
	uint		m_purgeLastTime;	/** TNT ��һ��Purgeʹ�õ�ʱ��*/
	char		m_purgeMaxBeginTime[30];/** TNT Purge �һ�εĿ�ʼʱ��*/

	uint		m_defragHashCnt;	/** TNT DefragHash�Ĵ���*/
	uint		m_numDefragHashSkipTable; /** TNT Defrag Hash �����ı�����*/

	uint		m_dumpCnt;			/** TNT Dump�Ĵ���*/

	u64			m_openTableCnt;		/** TNT openTable�Ĵ���*/
	u64			m_closeTableCnt;	/** TNT closeTable�Ĵ���*/

	u64			m_freeMemCallCnt;	/** TNT �ڴ治��ʱ����freeMem�Ĵ��� */

	u64			m_switchVersionPoolCnt;   /** TNT �л��汾�ش��� */
	char		m_switchVerPoolLastTime[30]; /** TNT �ϴ��л��汾�ص�ʱ�� */

	u64			m_reclaimVersionPoolCnt;  /** TNT �汾�ػ��մ��� */
	char		m_reclaimVerPoolLastBeginTime[30]; /** TNT �ϴΰ汾�ػ��տ�ʼʱ�� */
	char		m_reclaimVerPoolLastEndTime[30]; /** TNT �ϴΰ汾�ػ��ս���ʱ�� */
	
	VerpoolStatus m_verpoolStatus;	  /** �汾��״̬ */

	TNTGlobalStatus() {
	}
};

struct TNTStatus {
	uint		m_purgeCnt;			/** TNT Purge�Ĵ��� */
	uint		m_numPurgeSkipTable;/** TNT Purge�����ı������*/
	uint		m_purgeMaxTime;		/** TNT Purge����ĵ�ʱ�� */
	uint		m_purgeTotalTime;	/** TNT Purge�ܹ����ĵ�ʱ�� */
	uint		m_purgeLastTime;	/** TNT �ϴ�Purge���ĵ�ʱ�� */
	uint		m_purgeMaxBeginTime;/** TNT ���һ��purge�Ŀ�ʼʱ��*/

	uint		m_defragHashCnt;	/** TNT DefragHash�Ĵ���*/
	uint		m_numDefragHashSkipTable; /** TNT Defrag Hash �����ı�����*/

	uint		m_dumpCnt;			/** TNT Dump�Ĵ���*/

	u64			m_openTableCnt;		/** TNT openTable�Ĵ���*/
	u64			m_closeTableCnt;	/** TNT closeTable�Ĵ���*/

	u64			m_freeMemCallCnt;	/** TNT �ڴ治������freeMem�Ĵ���*/

	u64			m_switchVersionPoolCnt;   /** TNT �л��汾�ش��� */
	u32			m_switchVerPoolLastTime;  /** TNT �ϴ��л��汾��ʱ�� */
	u64			m_reclaimVersionPoolCnt;  /** TNT �汾�ػ��մ��� */
	u32			m_reclaimVerPoolLastBeginTime;	 /** TNT �ϴΰ汾�ػ��տ�ʼʱ�� */
	u32			m_reclaimVerPoolLastEndTime;	 /** TNT �ϴΰ汾�ػ��ս���ʱ�� */


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
	u16				m_tabPurged;		// purge�����ѳɹ�purge�ı�����
	TNTTable		*m_curTab;		    // purge������ǰ����purge�ı�
	u32				m_begin;			// purge������ʼʱ��
	u32				m_end;				// purge��������ʱ��
	TrxId           m_purgeTrxId;		// purge�����
	TNTTransaction	*m_purgeTrx;		// purge������������
	Connection		*m_conn;			// purge������connection
	Session			*m_session;			// purge������session
	TrxId           m_minTrxId;         // purge��������СreadView

	LsnType			m_dumpLsn;			// dumppoint lsn��dump�ָ����
	bool			m_dumpSuccess;		// dump�Ƿ�ɹ����Ƿ���purge�����еı�
};



/*struct DumpRecover {
	uint	m_purgeStatus;		// crashʱ��purge���������ڵ�״̬; 0, 1, 2, 3, 4
	uint	m_preparedTrxs;		// redo���֮�󣬴���prepare״̬����������
	uint	m_activeTrxs;		// redo���֮�󣬻�Ծ����������
	TrxId	m_maxTrxId;			// �ָ������У���¼��������ID
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
	u32				m_begin;			// reclaim lob������ʼʱ��
	u32				m_end;				// reclaim lob��������ʱ��
};

struct DefragHashIndexProcess {
public:
	Connection		*m_conn;			// defragHashIndex������connection
	Session			*m_session;			// defragHashIndex������session
	u32				m_begin;			// defragHashIndex������ʼʱ��
	u32				m_end;				// defragHashIndex��������ʱ��
	TNTTransaction	*m_trx;				// defragHashIndex������������
};

struct SwitchVerPoolProcess {
public:
	Connection		*m_conn;			// SwitchVerPoolProcess������connection
	Session			*m_session;			// SwitchVerPoolProcess������session
};

/** �������񼰴���״̬ */
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

// TNT����database��ȫ��Ψһ
// ��װ��TNT�������еĲ���
class TNTDatabase {
public:
	// ��������
	~TNTDatabase();
	// database ��ʼ��/��/�رյȻ�������
	static TNTDatabase* open(TNTConfig *config, u32 flag, int recover) throw(NtseException);
	static void create(TNTConfig *config) throw(NtseException);
	static void upgradeNtse2Tnt(TNTConfig *config) throw(NtseException);
	static void drop(const char *dir, const char *logDir);
	void close(bool flushLog = true, bool flushData = true);
	
	// TNT Table ����/��/����/�رյȲ���
	// TNT DDL��������Ҫ�ύ��ǰ����Ȼ���¿��������DDL
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
	// NTSE�ڴ˺�����ʵ�ֲ���online����
	void stopBgCustomThd();
	void cancelBgCustomThd(uint connId) throw(NtseException);
	void doOptimize(Session *session, const char *path, bool keepDict, bool *cancelFlag) throw(NtseException);
	void optimizeTable(Session *session, TNTTable *table, bool keepDict = false, bool waitForFinish = true) throw(NtseException);
	// ���±��flush lsn��TNT��������־�ļ��������Ҫ���⴦��
	void bumpFlushLsn(Session *session, u16 tableId, bool flushed = true);
	void createCompressDic(Session *session, TNTTable *table) throw(NtseException);
	
	// TNT ����
	void checkPoint(Session *session, uint targetTime = 0) throw(NtseException);
	void setCheckpointEnabled(bool enabled);

	// TNT Version Pool
	void initSwitchVerPool(SwitchVerPoolProcess *proc);
	void delInitSwitchVerPool(SwitchVerPoolProcess *proc);
	void switchActiveVerPoolIfNeeded();

	void freeMem(const char *file = NULL, uint line = 0);
	
	// TNT ���ݲ���������NTSE�ı�������NTSE Databaseʵ�֣���˴˴�ֻ��Ҫʵ��TNTIM Dump����
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
	
	// �����ڴ����� 
	void reclaimMemIndex() throw(NtseException); 

	// purge���������������ڣ��������Ա��ϵ���������purge���ͷſռ�
	void purgeTntimTable(TNTTable *table, TrxId purgeTrxId) throw(NtseException);
	// TNTIM purge�������ṩ��mysql�û��������һ�׶��ݲ�ʵ��
	void doPurge() throw(NtseException);
	void finishingPurgeAndLock() throw(NtseException);
	void donePurge();

	bool masterJudgeNeed();
	uint defragHashIndex(bool skipSmallTable) throw(NtseException);
	// �رղ���Ӧ�ü���Ϊ0�ı�
	void closeOpenTablesIfNeed();

	TNTBackupProcess* initBackup(const char *backupDir) throw(NtseException);
	void doBackup(TNTBackupProcess *backupProcess) throw(NtseException);
	void finishingBackupAndLock(TNTBackupProcess *backupProcess) throw(NtseException);
	void doneBackup(TNTBackupProcess *backupProcess);
	static void restore(const char *backupDir, const char *baseDir, const char *logDir) throw(NtseException);
	
	//void flushMHeap(Session *session, TNTTable *table);
	// database ��ǰ״̬��ѯ����Ҫ�Ļ�����Ҫ��װNTSE Database���get����
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
	// ���е�ddl recover��������NTSE database���ṩ��TNTIM�ڴ棬����Ҫ֧���κ�ddl recover
	
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
		// m_dumpBaseDir + m_dumpSN + table_id.dump�����к�+����.dump
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
	// ��֤�Ƿ�����Ҫɾ�����ڴ��(Ntse�ָ������У�ControlFileɾ���ı�)
	bool dumpRecoverVerifyHeaps();
	// Tntim heap�ָ����֮��(����redo/undo),���ݱ��壬build�ڴ�����
	void dumpRecoverBuildIndice() throw(NtseException);
	void dumpDoneRecover();

	void dumpEndRecover(bool succ);

	static LogEntry *copyLog(const LogEntry *log, MemoryContext *ctx);
	// ���ݲ�ͬ��־���ͣ�����redo
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
	//����crash recover��log recover
	LsnType undoLogEntry(Session *session, TNTTable * table, const LogEntry *log, const bool crash);

	void addTransactionLobId(LsnType logLsn, TrxId rollbackTrxId, const LogEntry* log);

	void redoReclaimLob();

	void addLobIdToPurgeHash(TrxId purgeTrxId, const LogEntry* log);
	void removePurgeHashLobIds();

	// crash recovery��������DumpRecover�д���prepare״̬������copy��TransactionSys��
	void copyPreparedTrxsToTransSys();
	//void releaseDumpRecover();

	static TrxId parseTrxId(const LogEntry *log);
	static LsnType parsePreLsn(const LogEntry *log);

	//��ȡ�ع�����������翪ʼrollback��lsn
	LsnType getFirstUnfinishedRollbackLsn();

	void createBackupFile(const char *path, byte *buf, size_t bufSize) throw(NtseException);
	void backupTNTCtrlAndDumpFile(TNTBackupProcess *backupProcess) throw(NtseException);
	static byte *readFile(const char *path, u64 *bufSize) throw(NtseException);
	void releaseBackupProcess(TNTBackupProcess *tntBackupProcess);
	
	void parseTntRedoLogForDebug(Syslog *syslog, const LogEntry *logEntry, LsnType lsn);
	void parseSubRecord(SubRecord *updateSub, TableDef *tableDef);

	

private:
	typedef Hash<char *, TNTTableInfo *, StrNoCaseHasher, StrNoCaseEqualer> PathHash;
	static const int MAX_OPEN_TABLES;	/** ϵͳ���֧��ͬʱ����ô����� */
	static const int MASTER_INTERVAL = 7;   /** master�̵߳ļ��ʱ�䣬��λΪs */
	static const int DEFRAG_HASH_INTERVAL = 1;	/** defrag hash �̵߳ļ��ʱ�䣬��λΪs*/
	static const int CLOSE_OPEN_TABLE_INTERVAL = 10; /** �ر�����Ϊ0�ı���̵߳ļ��ʱ��*/
	static const int RECLAIM_MINDEX_INTERVAL = 5;	/** �����ڴ�����ҳ���̵߳ļ��ʱ�䣬��λΪs*/
#ifndef NTSE_UNIT_TEST
	static const int MAX_DEFRAG_NUM = 50;    /** һ�����������ҳ�� */
#else
	static const int MAX_DEFRAG_NUM = 5;
#endif

    static const int TABLE_PURGE_THRESHOLD        = 5;  /** purge ��tntim����ҳ�����µı���������*/
    static const int TABLE_DEFRAG_HASH_INDEX_THRESHOLD = 1000; /** defrag hash indexʱС�ڸ���������*/
	
	Database 					*m_db;			// ntse database;
	// �洢��open���hash��hash���ڴ�ŵ���TNT Tableʵ��
	// ע��㣺m_dbʵ���У��д洢NTSE Tableʵ����hash��
	Hash<u16, TNTTableInfo *>	*m_idToTables;	// table id hash
	PathHash					*m_pathToTables;
	
	PagePool                    *m_commonPool;
	PagePool					*m_pagePool;	// TNT�����page pool
	TNTConfig					*m_tntConfig;
	DbStat						m_dbStat;
	bool						m_closed;		// ��ǰ���ݿ��Ƿ��Ѿ��ر�
	bool						m_unRecovered;
	
	// TNT controlfile����������TNT���еĿ�����Ϣ
	TNTControlFile				*m_tntCtrlFile;	// TNT controlfile
	ControlFile					*m_ntseCtrlFile;
	Syslog						*m_tntSyslog;
	// TNT redo��־�ļ�������д��TNT redo��־
	Txnlog						*m_tntTxnLog;
	
	// TNT����crash recovery��Ҫ�Ľṹ
	//DumpRecover					*m_dumpRecover;
	
	// TNT������Ҫ��ʱ���������񣬰���Dump��Purge
	// �������еĹ����У�TNTStatus�������ᱻ����Ϊ��Ӧ״̬
	Mutex						*m_taskLock;	// mutex control tntim dump
	Task                        *m_masterTask;
	Task						*m_purgeAndDumpTask; //tntim purge,defrag,dump
	Task						*m_defragHashTask;	// tntim defrag hash index
	Task                        *m_closeOpenTablesTask; //close tnt tables
	Task						*m_reclaimMemIndexTask;	// tntim reclaim memory index page
	TNTStat						m_tntState;	// 
public:
	TNTStatus					m_tntStatus; //tntͳ����Ϣ
private:
	// ��TNT����ȫ�ֱ�������TNTDatabase��
	// ��Database��ʼ��ʱ����
	// crash recovery��������־��������ύ״̬���ع�m_tranSys
	TNTTrxSys				    *m_tranSys;		// transaction sys
	
	// ��¼TNT Table������Ϣ������,�Լ�Hash��
	// ���۱��Ƿ�real close��base��Ϣ���ᷢ���仯
	DList<TNTTableBase *>		m_tntTabBases;
	Hash<u16, TNTTableBase *>	*m_idToTabBase;

	// �汾��
	VersionPool					*m_vPool;
	UKLockManager				*m_uniqueKeyLockMgr;

	CommonMemPool               *m_commonMemPool;   /** ͨ���ڴ�� */
	TNTIMPageManager            *m_TNTIMpageManager;/** TNTIM�ڴ�� */

	Hash<TrxId, RecoverProcess *>	*m_activeTrxProcs;	/** �ָ������л�Ծ���� */

	Hash<TrxId, RecoverProcess *>	*m_rollbackTrx; //��¼�ڻָ�ʱ���ڻع�������
	TblLobHashMap                   *m_purgeInsertLobs; //��¼��purge�����б������lobId����

	Atomic<int>						m_backingUp;		/** ���ڱ��� */

	friend class ntse::Database;
};

}

#endif