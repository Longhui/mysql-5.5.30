/**
* TNT引擎，Database模块。
*
* @author 何登成
*/

#include "api/TNTDatabase.h"
#include "api/TNTTable.h"
#include "api/Table.h"
#include "util/File.h"
#include "util/SmartPtr.h"
#include "misc/Txnlog.h"
#include "misc/ControlFile.h"
#include "misc/TableDef.h"
#include "rec/MRecords.h"
#include "misc/OptimizeThread.h"

using namespace ntse;
namespace tnt {
const int TNTDatabase::MAX_OPEN_TABLES = ntse::TableDef::MAX_NORMAL_TABLEID;

class TNTTableHasher {
public:
	inline unsigned int operator()(const TNTTable *table) const {
		return table->getNtseTable()->getTableDef()->m_id;
	}
};

template<typename T1, typename T2>
class TNTTableEqualer {
public:
	inline bool operator()(const T1 &v1, const T2 &v2) const {
		return equals(v1, v2);
	}

private:
	static bool equals(const u16 &tableId, const TNTTable* table) {
		return tableId == table->getNtseTable()->getTableDef()->m_id;
	}
};

typedef DynHash<u16, TNTTable*, TNTTableHasher, Hasher<u16>, TNTTableEqualer<u16, TNTTable*> > TNTTableHash;
/** TNT后台线程 */
class TNTBgTask: public Task {
public:
	/**
	 * 构造函数
	 *
	 * @param db 数据库
	 * @param interval 检查点时间间隔，单位秒
	 */
	TNTBgTask(TNTDatabase *db, const char *name, uint interval)
		: Task(name, interval) {
		m_db = db;
	}

	virtual void runIt() = 0;

	void run()	{
		runIt();
	}
protected:
	TNTDatabase   *m_db;
};

/** 定期进行purge，defrag和dump的后台线程 */
class PurgeAndDumpTask: public TNTBgTask {
public:
	/**
	 * 构造函数
	 *
	 * @param db 数据库
	 * @param interval 检查点时间间隔，单位秒
	 */
	PurgeAndDumpTask(TNTDatabase *db, uint interval)
		: TNTBgTask(db, "TNTDatabase::PurgeAndDumpTask", min(interval, (uint)PURGE_AND_DUMP_TASK_INTERVAL) * 1000) {
		m_lastRunTime = System::fastTime();
		m_runInterval = interval;
		m_purgeCnt = 0;
	}

	virtual void runIt() {
#ifndef NTSE_UNIT_TEST
		u32 now = System::fastTime();
		if ((now < m_lastRunTime + m_runInterval) && !m_db->purgeJudgeNeed()) {
			return;
		}
		//	检测配置参数，是否需要dump
		m_totalPurgePerDump = m_db->getTNTConfig()->m_dumpInterval;
		bool skipTables = m_purgeCnt % PURGES_PER_TOTAL_PURGE;
		bool needDump = (m_totalPurgePerDump == 0)? false: (!(m_purgeCnt % (PURGES_PER_TOTAL_PURGE * m_totalPurgePerDump)));
		try {
			m_db->purgeAndDumpTntim(PT_PURGEPHASE2, skipTables, needDump, true);
		} catch (NtseException &e) {
			m_db->getTNTSyslog()->log(EL_WARN, "PurgeAndDumpTask Error: %s", e.getMessage());
		}
		m_lastRunTime = now;
		m_purgeCnt++;
#endif
	}

private:
	u32			m_runInterval;			/** 后台线程时间间隔，单位为秒 */
	u32			m_lastRunTime;			/** 上一次完成后台线程的时间, 单位秒 */
	static const int PURGE_AND_DUMP_TASK_INTERVAL =	3; /** purge Task多久起来一次 */
	static const int PURGES_PER_TOTAL_PURGE       = 1;  /** PurgeAndDump Task 中几次purge做一次totalPurge*/
	int			m_totalPurgePerDump;	/** PurgeAndDump Task 中几次totalPurge做一次Dump*/
	u64			m_purgeCnt;				/** 数据库启动后进行purge的计数 */
};


/** 定期进行检查等各项任务的后台线程 */
class Master: public TNTBgTask {
public:
	/**
	 * 构造函数
	 *
	 * @param db 数据库
	 * @param interval 检查点时间间隔，单位秒
	 */
	Master(TNTDatabase *db, uint interval)
		: TNTBgTask(db, "TNTDatabase::Master", interval*1000) {
		m_lastSwitchOpTime = System::fastTime();
		m_lastKillHangTrxTime = m_lastSwitchOpTime;
		
		// Master线程的启动周期，不使用传入的internal参数
		m_switchOpInterval = 10;
		m_killHangTrxInterval = 10;
	}

	virtual void runIt() {
#ifndef NTSE_UNIT_TEST
		u32 now = System::fastTime();

		// 切换版本池与ReclaimLob，m_operInterval时间内启动一次检测
		if ((now >= m_lastSwitchOpTime + m_switchOpInterval) || m_db->masterJudgeNeed()) {
			try {
				m_db->switchActiveVerPoolIfNeeded();
				m_db->reclaimLob();

				m_lastSwitchOpTime = now;
			} catch (NtseException &e) {
				m_db->getTNTSyslog()->log(EL_WARN, "Master Thread Switch Version Pool Error: %s", e.getMessage());
			}
		}

		// kill hang trx
		if ((now >= m_lastKillHangTrxTime + m_killHangTrxInterval) || m_db->masterJudgeNeed()) {
			m_db->killHangTrx();
			m_lastKillHangTrxTime = now;
		}
#endif
	}

private:
	u32         m_switchOpInterval;		/** master线程，切换version pool的时间间隔，单位为秒 */
	u32			m_lastSwitchOpTime;		/** Master线程上一次尝试切换VersionPool的时间, 单位秒 */
	u32			m_killHangTrxInterval;		/** master线程，kill hang trx的时间间隔，单位为秒 */
	u32			m_lastKillHangTrxTime;		/** Master线程上一次尝试kill hang trx的时间, 单位秒 */
};


class DefragHashTask: public TNTBgTask {
public:
	/**
	 * 构造函数
	 *
	 * @param db 数据库
	 * @param interval 检查点时间间隔，单位秒
	 */
	DefragHashTask(TNTDatabase *db, uint interval)
		: TNTBgTask(db, "TNTDatabase::Master", interval*1000) {
		m_lastDefragOpTime = System::fastTime();		
		m_defragOpInterval = 1;
		m_defragHashCnt = 0;
	}

	virtual void runIt() {
#ifndef NTSE_UNIT_TEST
		u32 now = System::fastTime();
		// defrag hash index
		if (now >= m_lastDefragOpTime + m_defragOpInterval) {
			try {
				bool skipSmallTable = ((m_defragHashCnt != 0) && (0 == m_defragHashCnt % DEFRAG_HASH_INDEX_PER_TOTAL))? false: true;
				m_db->defragHashIndex(skipSmallTable);
				m_defragHashCnt++;
				m_lastDefragOpTime = now;
			} catch (NtseException &e) {
				m_db->getTNTSyslog()->log(EL_WARN, "Defrag HashIndex Error: %s", e.getMessage());
			}
		}
#endif
	}
private:
	u32			m_defragOpInterval;		/** master线程，defrage hash index的时间间隔，单位为秒 */
	u32			m_lastDefragOpTime;		/** Master线程上一次尝试Defrag HashIndex的时间, 单位秒 */
	u64			m_defragHashCnt;		/** 自数据库启动以来Master线程做defrag hash index的次数 */

	static const int DEFRAG_HASH_INDEX_PER_TOTAL = 5; /** 每隔一定的skill small table后需要做一次全量defrag hashindex*/
};


/** 定期关闭引用为0的TNT表的后台线程 */
class CloseOpenTableTask: public TNTBgTask {
public:
	/**
	 * 构造函数
	 *
	 * @param db 数据库
	 * @param interval 检查点时间间隔，单位秒
	 */
	CloseOpenTableTask(TNTDatabase *db, uint interval)
		: TNTBgTask(db, "TNTDatabase::CloseOpenTableTask", interval * 1000) {
		m_lastRunTime = System::fastTime();
		m_runInterval = interval;
	}

	virtual void runIt() {
#ifndef NTSE_UNIT_TEST
		u32 now = System::fastTime();
		if ((now < m_lastRunTime + m_runInterval)) {
			return;
		}
		
		try {
			m_db->closeOpenTablesIfNeed();
		} catch (NtseException &e) {
			m_db->getTNTSyslog()->log(EL_WARN, "CloseOpenTable Error: %s", e.getMessage());
		}
		m_lastRunTime = now;
#endif
	}

private:
	u32			m_runInterval;			/** 后台线程时间间隔，单位为秒 */
	u32			m_lastRunTime;			/** 上一次完成后台线程的时间, 单位秒 */
};


/** 定期回收内存索引页面的后台线程 */
class ReclaimMemIndexTask: public TNTBgTask {
public:
	/**
	 * 构造函数
	 *
	 * @param db 数据库
	 * @param interval 检查点时间间隔，单位秒
	 */
	ReclaimMemIndexTask(TNTDatabase *db, uint interval)
		: TNTBgTask(db, "TNTDatabase:ReclaimMemIndexTask", interval * 1000) {
		m_lastRunTime = System::fastTime();
		m_runInterval = interval;
	}

	virtual void runIt() {
#ifndef NTSE_UNIT_TEST
		u32 now = System::fastTime();
		if ((now < m_lastRunTime + m_runInterval)) {
			return;
		}
		
		try {
			m_db->reclaimMemIndex();
		} catch (NtseException &e) {
			m_db->getTNTSyslog()->log(EL_WARN, "ReclaimMemIndexTask Error: %s", e.getMessage());
		}
		m_lastRunTime = now;
#endif
	}

private:
	u32			m_runInterval;			/** 后台线程时间间隔，单位为秒 */
	u32			m_lastRunTime;			/** 上一次完成后台线程的时间, 单位秒 */
};

/**
 * 构造一个TNT Database对象
 *
 * @param config TNT数据库配置
 *   内存使用约定：直接引用至数据库关闭
 */
TNTDatabase::TNTDatabase(TNTConfig *config) {
	init();
	m_idToTables	= new Hash<u16, TNTTableInfo *>(MAX_OPEN_TABLES);
	m_pathToTables	= new PathHash(MAX_OPEN_TABLES);
	m_idToTabBase	= new Hash<u16, TNTTableBase *>(MAX_OPEN_TABLES);

	m_tntConfig = config;
	m_tntState = TNTIM_NOOP;
	m_taskLock = new Mutex("TNT Database::dump & purge Lock", __FILE__, __LINE__);
}

TNTDatabase::~TNTDatabase() {

}

/** 初始化TNT Database对象状态 */
void TNTDatabase::init() {
	m_db			= NULL;
	m_idToTables	= NULL;
	m_pathToTables	= NULL;
	m_commonPool    = NULL;
	m_pagePool		= NULL;
	m_tntConfig		= NULL;
	m_tntCtrlFile	= NULL;
	m_ntseCtrlFile	= NULL;
	m_tntSyslog		= NULL;
	m_tntTxnLog		= NULL;
	//m_dumpRecover	= NULL;
	m_taskLock		= NULL;
	m_purgeAndDumpTask = NULL;
	m_defragHashTask = NULL;
	m_masterTask    = NULL;
	m_closeOpenTablesTask = NULL;
	m_reclaimMemIndexTask = NULL;
	m_tranSys		= NULL;
	m_idToTabBase	= NULL;
	m_vPool			= NULL;
	m_TNTIMpageManager	= NULL;
	m_commonMemPool = NULL;
	m_uniqueKeyLockMgr = NULL;
	m_unRecovered	= false;
	m_closed        = false;
}


/**
 * 打开一个数据库
 *
 * @param config		TNT引擎配置参数
 *   内存使用约定：		直接引用至数据库关闭
 * @param autoCreate	数据库不存在时是否自动创建
 * @param recover		是否恢复，0表示根据数据库是否安全关闭需要自动决定是否恢复，1表示强制恢复，-1表示强制不恢复
 * @throw NtseException 操作失败
 */
TNTDatabase* TNTDatabase::open(TNTConfig *config, u32 flag, int recover) throw(NtseException) {	
	// 1.判断控制文件是否存在，若控制文件存在，则认为数据库已经创建，这时
	// 打开数据库。否则认为数据库不存在，这时创建数据库
	// TNT，NTSE对应的basedir是一样的，同一目录
	string basedir(config->m_tntBasedir);
	
	string ntsePath = basedir + NTSE_PATH_SEP + Limits::NAME_CTRL_FILE;
	string tntPath = basedir + NTSE_PATH_SEP + Limits::NAME_TNT_CTRL_FILE;
	string newNtsePath = ntsePath + Limits::NAME_CTRL_SWAP_FILE_EXT;
	string newTntPath = tntPath + Limits::NAME_CTRL_SWAP_FILE_EXT;
	
	bool tntCreate = false;

	// 如果控制文件不存在但是存在控制文件的交换文件，则将交换文件重命名为控制文件
	if (!File::isExist(ntsePath.c_str()) && File::isExist(newNtsePath.c_str())) {
		u64 code = File(newNtsePath.c_str()).move(ntsePath.c_str());
		if (code != File::E_NO_ERROR)
			NTSE_THROW(NTSE_EC_FILE_FAIL, "Unable to move %s to %s.", newNtsePath.c_str(), ntsePath.c_str());
	}

	if (!File::isExist(tntPath.c_str()) && File::isExist(newTntPath.c_str())) {
		u64 code = File(newTntPath.c_str()).move(tntPath.c_str());
		if (code != File::E_NO_ERROR)
			NTSE_THROW(NTSE_EC_FILE_FAIL, "Unable to move %s to %s.", newTntPath.c_str(), tntPath.c_str());
	}

	// Ntse control file与tnt control file均不存在
	if (!File::isExist(ntsePath.c_str()) && !File::isExist(tntPath.c_str())) {
		if (flag & TNT_AUTO_CREATE) {
			tntCreate = true;
			create(config);
		} else
			NTSE_THROW(NTSE_EC_FILE_NOT_EXIST, "Both ctrl files %s and %s not exist.", ntsePath.c_str(), tntPath.c_str());
	} else if (!File::isExist(tntPath.c_str())) {
		if (flag & TNT_UPGRADE_FROM_NTSE) {
			tntCreate = true;
			upgradeNtse2Tnt(config);
		} else // 只有tnt control file不存在
			NTSE_THROW(NTSE_EC_FILE_NOT_EXIST, "Tnt ctrl file %s not exists.", tntPath.c_str());
	} else if (!File::isExist(ntsePath.c_str()))
		// 只有ntse control file不存在
		NTSE_THROW(NTSE_EC_FILE_NOT_EXIST, "Ntse ctrl file %s not exists.", ntsePath.c_str());

	// 2. Tnt controlfile与ntse controlfile均存在，选择打开数据库
	TNTDatabase *db = new TNTDatabase(config);
	db->m_dbStat	= DB_STARTING;
	// 创建tnt引擎的系统日志文件
	tntPath	 = basedir + NTSE_PATH_SEP + Limits::NAME_TNT_SYSLOG;
	db->m_tntSyslog = new Syslog(tntPath.c_str(), config->m_tntLogLevel, false, config->m_ntseConfig.m_printToStdout);
	tntPath = basedir + NTSE_PATH_SEP + Limits::NAME_TNT_CTRL_FILE;
	db->m_tntCtrlFile = TNTControlFile::open(tntPath.c_str(), db->m_tntSyslog);

	/*db->m_tntTxnLog = NULL;
	do {
		try {
			// 打开TNT引擎的redo日志文件
			db->m_tntTxnLog = Txnlog::open(db, config->m_tntBasedir, Limits::NAME_TNT_TXNLOG, db->m_tntCtrlFile->getNumTxnlogs(), config->m_tntLogfileSize, config->m_tntLogBufSize);
		} catch(NtseException &e) {
			if (e.getErrorCode() == NTSE_EC_MISSING_LOGFILE) { // 日志文件已经被删除
				db->m_tntSyslog->log(EL_WARN, "Missing TNT transaction log files! Try to recreate");
				// 假定数据库状态一致，重新创建日志文件
				LsnType dumpLsn = Txnlog::recreate(config->m_tntBasedir, Limits::NAME_TNT_TXNLOG, db->m_tntCtrlFile->getNumTxnlogs(), config->m_tntLogfileSize, db->m_tntCtrlFile->getDumpLsn());
				db->m_tntSyslog->log(EL_LOG, "TNT Transaction log files created, set dumppoint LSN "
					"from "I64FORMAT"u to "I64FORMAT"u.", db->m_tntCtrlFile->getDumpLsn(), dumpLsn);
				db->m_tntCtrlFile->setDumpLsn(dumpLsn);
			} 
			else {
				db->m_tntSyslog->log(EL_ERROR, e.getMessage());
				db->close();
				delete db;
				throw e;
			}
		}
	} while (!db->m_tntTxnLog);*/

	//初始化commonPool
	db->m_commonPool = new PagePool(config->m_ntseConfig.m_maxSessions + 1, Limits::PAGE_SIZE);
	// 注册通用内存池管理模块
	db->m_commonMemPool = new CommonMemPool(config->m_ntseConfig.m_commonPoolSize, db->m_commonPool);
	db->m_commonPool->registerUser(db->m_commonMemPool);
	db->m_commonPool->init();

	// 3. 初始化TNTDatabase的pagePool，但pagePool的init在Database的Database::preOpen初始化
	db->m_pagePool = new PagePool(config->m_ntseConfig.m_maxSessions + 1, Limits::PAGE_SIZE);
	// 注册TNTIM内存管理模块
	db->m_TNTIMpageManager = new TNTIMPageManager(config->m_tntBufSize, config->m_ntseConfig.m_pageBufSize, db->m_pagePool);
	db->m_pagePool->registerUser(db->m_TNTIMpageManager);

	//4 调用Ntse Database接口，打开ntse数据库
	bool bRecovered = false;
	try {
		// 开始阶段，不恢复数据库，恢复流程统一由TNTDatabase->recover函数控制
		Database::preOpen(db, recover, false, &bRecovered);
		db->m_TNTIMpageManager->preAlloc(0, PAGE_MEM_HEAP, config->m_tntBufSize);
		db->m_db->getSessionManager()->setTNTDb(db);
	} catch (NtseException &e) {
		db->m_tntSyslog->log(EL_ERROR, e.getMessage());
		db->close();
		delete db;
		throw e;
	}
	
	db->m_ntseCtrlFile = db->m_db->getControlFile();

	// 初始化唯一性锁表
	db->m_uniqueKeyLockMgr = new UKLockManager(db->getNtseDb()->getConfig()->m_maxSessions);

	try {
		//db->m_pagePool->init();

		//FIXME: 最大的活跃事务数需要配置参数
		db->m_tranSys = new TNTTrxSys(db, config->m_maxTrxCnt, config->m_trxFlushMode, config->m_txnLockTimeout);
		db->m_tranSys->setActiveVersionPoolId(db->m_tntCtrlFile->getActvieVerPool());
		db->m_tranSys->setMaxTrxIdIfGreater(db->m_tntCtrlFile->getMaxTrxId());

		db->openVersionPool(tntCreate);
		db->m_dbStat = DB_MOUNTED;
		db->waitBgTasks(false);
		
		// 5. 进行crash recover，无论是否需要，都会调用recover函数
		db->recover(recover);

		db->m_tranSys->markHangPrepareTrxAfterRecover();
	} catch (NtseException &e) {
		db->m_tntSyslog->log(EL_ERROR, e.getMessage());
		db->close(true, false);
		delete db;
		throw e;
	}
	
	// 6. crash recover完成，做完后续操作，open过程完毕
	db->m_db->postOpen(recover, bRecovered);
	
	db->m_closed = false;
	db->m_unRecovered = (recover == -1);
	//if (!db->m_unRecovered) {
	db->m_purgeAndDumpTask = new PurgeAndDumpTask(db, config->m_purgeInterval);
	db->m_purgeAndDumpTask->start();
	
	db->m_defragHashTask = new DefragHashTask(db, DEFRAG_HASH_INTERVAL);
	db->m_defragHashTask->start();

	db->m_masterTask = new Master(db, MASTER_INTERVAL);
	db->m_masterTask->start();

	db->m_closeOpenTablesTask = new CloseOpenTableTask(db, CLOSE_OPEN_TABLE_INTERVAL);
	db->m_closeOpenTablesTask->start();

	db->m_reclaimMemIndexTask = new ReclaimMemIndexTask(db, RECLAIM_MINDEX_INTERVAL);
	db->m_reclaimMemIndexTask->start();

	db->m_dbStat = DB_RUNNING;

	// 验证TNT内存，NTSE验证已完成
	db->verify(recover);

	return db;
}

void TNTDatabase::openVersionPool(bool create) throw (NtseException) {
	// 初始化version pool
	// 若为新建数据库，则创建版本池；否则直接打开版本池即可
	Connection *conn = m_db->getConnection(true, __FUNC__);
	Session *session = m_db->getSessionManager()->allocSession(__FUNC__, conn);
	if (create) {
		VersionPool::create(m_db, session, ".", m_tntConfig->m_verpoolCnt);
	}

	try {
		m_vPool = VersionPool::open(m_db, session, ".", m_tntConfig->m_verpoolCnt);
		assert(m_vPool != NULL);
	} catch (NtseException &e) {
		m_db->getSessionManager()->freeSession(session);
		m_db->freeConnection(conn);
		throw e;
	}
	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(conn);
}

void TNTDatabase::closeVersionPool() {
	Connection *conn = m_db->getConnection(true, __FUNC__);
	Session *session = m_db->getSessionManager()->allocSession(__FUNC__, conn);
	m_vPool->close(session);
	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(conn);
}

/**
 * 创建TNT数据库
 *
 * @param config 数据库配置
 * @throw NtseException 文件已经存在，无权限等导致操作失败
 */
void TNTDatabase::create(TNTConfig *config) throw(NtseException) {
	string basedir(config->m_tntBasedir);

	// 1. 创建TNT系统日志文件
	string tntPath = basedir + NTSE_PATH_SEP + Limits::NAME_TNT_SYSLOG;
	Syslog syslog(tntPath.c_str(), EL_LOG, true, true);

	// 2. 创建NTSE数据库
	try {
		Database::create(&config->m_ntseConfig);
	} catch (NtseException &e) {
		syslog.log(EL_ERROR, e.getMessage());
		throw e;
	}

	// 3. 创建TNT引擎控制文件
	tntPath = basedir + NTSE_PATH_SEP + Limits::NAME_TNT_CTRL_FILE;
	TNTControlFile::create(tntPath.c_str(), config, &syslog);

	// 4. 创建TNT引擎redo日志文件
	//Txnlog::create(config->m_tntBasedir, Limits::NAME_TNT_TXNLOG, config->m_tntLogfileSize);
}

/**
 * 升级ntse数据库为tnt数据库
 *
 * @param config 数据库配置
 * @throw NtseException 文件已经存在，无权限等导致操作失败
 */
void TNTDatabase::upgradeNtse2Tnt(TNTConfig *config) throw(NtseException) {
	Config *ntseConfig = &config->m_ntseConfig;
	string basedir(config->m_tntBasedir);
	
	Syslog syslog(NULL, EL_LOG, true, true);
	//如果ntse_ctrl未升级成功，那么ControlFile::open会throw exception
	string ntsePath(config->m_ntseConfig.m_basedir);
	ntsePath.append(NTSE_PATH_SEP).append(Limits::NAME_CTRL_FILE);
	ControlFile *ctrlFile = ControlFile::open(ntsePath.c_str(), &syslog);
	
	// 1. 创建TNT系统日志文件
	string tntPath = basedir + NTSE_PATH_SEP + Limits::NAME_TNT_SYSLOG;
	Syslog tntSyslog(tntPath.c_str(), EL_LOG, true, true);

	// 2. 创建TNT引擎控制文件
	tntPath = basedir + NTSE_PATH_SEP + Limits::NAME_TNT_CTRL_FILE;
	TNTControlFile::create(tntPath.c_str(), config, &tntSyslog);

	TNTControlFile *tntCtrlFile = NULL;
	try {
		tntCtrlFile = TNTControlFile::open(tntPath.c_str(), &tntSyslog);
	} catch (NtseException &e) {
		ctrlFile->close();
		delete ctrlFile;
		throw e;
	}

	tntCtrlFile->setDumpLsn(ctrlFile->getCheckpointLSN());

	try {
		Txnlog::drop(ntseConfig->m_logdir, Limits::NAME_TXNLOG, ctrlFile->getNumTxnlogs());
		Txnlog::recreate(ntseConfig->m_logdir, Limits::NAME_TXNLOG, ctrlFile->getNumTxnlogs(), ntseConfig->m_logFileSize, tntCtrlFile->getDumpLsn());
	} catch (NtseException &e) {
		tntCtrlFile->close();
		delete tntCtrlFile;

		ctrlFile->close();
		delete ctrlFile;
		throw e;
	}

	tntCtrlFile->close();
	delete tntCtrlFile;

	ctrlFile->close();
	delete ctrlFile;
}

/**
 * 删除TNT数据库
 *
 * @param dir 数据库文件所在目录
 */
void TNTDatabase::drop(const char *dir, const char *logDir)
{
	Syslog log(NULL, EL_LOG, true, true);
	string basedir(dir);

	string tntPath = basedir + NTSE_PATH_SEP + Limits::NAME_TNT_CTRL_FILE;

	try {
		Database::drop(dir, logDir);
		TNTControlFile *cf = TNTControlFile::open(tntPath.c_str(), &log);
		//Txnlog::drop(dir, Limits::NAME_TNT_TXNLOG, cf->getNumTxnlogs());
		VersionPool::drop(dir, cf->getVersionPoolCnt());
		cf->close();
		delete cf;
	} catch (NtseException &) {
	}

	File ctrlfile(tntPath.c_str());
	ctrlfile.remove();

	tntPath = basedir + NTSE_PATH_SEP + Limits::NAME_TNT_SYSLOG;
	File syslog(tntPath.c_str());
	syslog.remove();
}

/**
 * 停止用户后台线程
 *
 */
void TNTDatabase::stopBgCustomThd() {
	if (m_db && m_db->getBgCustomThreadManager()) {
		m_db->getBgCustomThreadManager()->stopAll();
		m_db->getBgCustomThreadManager()->join();
	}
}

/**
 * 关闭TNT数据库
 *
 * @param flushLog 是否刷出日志
 * @param flushData 是否刷出数据
 */
void TNTDatabase::close(bool flushLog, bool flushData)
{
	if (m_closed) {
		return;
	}
	
	DbStat prevStat = m_dbStat;
	m_dbStat = DB_CLOSING;
	
	// 停止用户后台线程
	stopBgCustomThd();

	// 停止purge、defrag、dump线程
	if (m_purgeAndDumpTask != NULL) {
		m_purgeAndDumpTask->stop();
		m_purgeAndDumpTask->join();
		delete m_purgeAndDumpTask;
	}

	// 停止defrag hash 线程
	if (m_defragHashTask != NULL) {
		m_defragHashTask->stop();
		m_defragHashTask->join();
		delete m_defragHashTask;
	}

	// 停止master
	if (m_masterTask != NULL) {
		m_masterTask->stop();
		m_masterTask->join();
		delete m_masterTask;
	}

	// 停止closeOpenTables 线程
	if (m_closeOpenTablesTask != NULL) {
		m_closeOpenTablesTask->stop();
		m_closeOpenTablesTask->join();
		delete m_closeOpenTablesTask;
	}

	// 停止reclaim mindex pages 线程
	if (m_reclaimMemIndexTask != NULL) {
		m_reclaimMemIndexTask->stop();
		m_reclaimMemIndexTask->join();
		delete m_reclaimMemIndexTask;
	}

	if (prevStat == DB_RUNNING) {
		// 若需要，purge内存heap
		if (m_tntConfig->m_purgeBeforeClose) {
			try {
				if( !m_tntConfig->m_dumpBeforeClose) {
					purgeAndDumpTntim(PT_PURGEPHASE1, false, false, true);
				} else {
					purgeAndDumpTntim(PT_PURGEPHASE2, false, true, true);
				}
			} catch (NtseException &e) {
				m_tntSyslog->log(EL_ERROR, e.getMessage());
			}
		}
	}

	if (prevStat != DB_STARTING) {
		// 关闭打开的内存表
		closeOpenTables();
		closeTNTTableBases();
	}

	// 关闭version pool
	if (m_vPool) {
		closeVersionPool();
	}

	// 关闭唯一性锁表
	if (m_uniqueKeyLockMgr)
		delete m_uniqueKeyLockMgr;

	delete m_idToTables;
	delete m_pathToTables;
	delete m_idToTabBase;
	delete m_taskLock;
	delete m_tranSys;
	delete m_vPool;

	// 关闭commonPool
	if (m_commonPool) {
		m_commonPool->preDelete();
	}

	// 关闭page pool
	if (m_pagePool) {
		m_pagePool->preDelete();
	}

	// 最后关闭NTSE Database
	if (m_db != NULL) {
		m_db->close(flushLog, flushData);
		delete m_db;
		m_db = NULL;
	}
	m_tntTxnLog = NULL;
	delete m_commonMemPool;
	delete m_commonPool;
	m_commonPool = NULL;

	delete m_TNTIMpageManager;
	delete m_pagePool;
	m_pagePool = NULL;

	// 关闭日志系统
	/*if (m_tntTxnLog) 
		m_tntTxnLog->close(flushLog);*/

	// 关闭控制文件系统
	if (m_tntCtrlFile) {
		m_tntCtrlFile->close(flushData);
		delete m_tntCtrlFile;
	}
	delete m_tntSyslog;

	init();
	m_closed = true;
}

TrxId TNTDatabase::parseTrxId(const LogEntry *log) {
	TrxId trxId;
	Stream s(log->m_data, log->m_size);
	s.read(&trxId);
	return trxId;
}

LsnType TNTDatabase::parsePreLsn(const LogEntry *log) {
	LsnType preLsn = INVALID_LSN;
	Stream s(log->m_data, log->m_size);
	s.skip(sizeof(TrxId));
	s.read(&preLsn);
	return preLsn;
}

/**
* 对TNT数据库进行crash recovery
*
* @param recover	是否需要真正进行恢复
*/
void TNTDatabase::recover(int recover) throw(NtseException)
{
	LsnType		dumppointLsn;
	bool		beNeedRecover;
	
	assert(m_dbStat == DB_MOUNTED);
	// 1. 设置数据库当前状态并初始化dump结构
	m_dbStat = DB_RECOVERING;
	//initDumpRecover();

	// 2. 内存恢复起点之前的数据已全部purge至外存，此处不需要readDump
/*	try {
		readDump();
	} catch (NtseException &e) {
		m_tntSyslog->log(EL_ERROR, e.getMessage());
		//releaseDumpRecover();
		throw e;
	}
*/

	// 3. 判断是否需要进recover
	if (recover == 1)
		beNeedRecover = true;
	else if (recover == -1)
		beNeedRecover = false;
	else {
		beNeedRecover = !m_tntCtrlFile->getCleanClosed();
	}

	if (!beNeedRecover) {
		// 3.1. 上次正常关闭，不需要进行恢复
		try {
			dumpRecoverBuildIndice();
		} catch (NtseException &e) {
			m_tntSyslog->log(EL_ERROR, e.getMessage());
			//releaseDumpRecover();
			return;
		}
		//releaseDumpRecover();
		return;
	}

	// 4. TNT数据库需要进行crash recover
	dumppointLsn = m_tntCtrlFile->getDumpLsn();
	m_tntSyslog->log(EL_LOG, "Start database recover from LSN: "I64FORMAT"d.", dumppointLsn);
	
	// 4.1. NTSE进行recover，可以通过创建线程的形式并行执行

	// 4.2. TNT进行redo阶段recover
	m_activeTrxProcs = new Hash<TrxId, RecoverProcess *>(m_db->getConfig()->m_maxSessions * 2);
	m_purgeInsertLobs = new TblLobHashMap();
	m_rollbackTrx = new Hash<TrxId, RecoverProcess *>(m_db->getConfig()->m_maxSessions * 2);
	LogScanHandle *logScan = m_tntTxnLog->beginScan(dumppointLsn, m_tntTxnLog->tailLsn());
	int		oldPercent	= 0;
	int		oldTime		= System::fastTime();
	u64		lsnRange	= m_tntTxnLog->tailLsn() - dumppointLsn + 1;
	//记录最后一个可能未完成purge的事务id
	TrxId   purgeTrxId = INVALID_TRX_ID;
	//记录最后一次未完成purge的purgephase1 的lsn
	LsnType purgePhaseOneLsn = INVALID_LSN;
	
	//创建打印恢复日志的syslog(需要打印恢复日志时)
	//Syslog *redoSyslog = new Syslog("dump_tnt_redo.log", EL_DEBUG, true, false);
	while (m_tntTxnLog->getNext(logScan)) {
		const LogEntry	*logEntry	= logScan->logEntry();
		//如果log不是tnt日志，则在recover中可以直接跳过
		if (!logEntry->isTNTLog()) {
			continue;
		}
		LsnType     lsn			= logScan->curLsn();
		TrxId		trxId		= parseTrxId(logEntry);
		LogType		logType		= logEntry->m_logType;
		//u16		tableId		= logEntry->m_tableId;
		((LogEntry *)logEntry)->m_lsn = lsn;

		// 最多每10秒打印一次TNT恢复进度信息
		int percent = (int)((lsn - dumppointLsn) * 100 / lsnRange);
		if (percent > oldPercent && (System::fastTime() - oldTime) > 10) {
			m_tntSyslog->log(EL_LOG, "Done replay of %d%% logs.", percent);
			oldPercent = percent;
			oldTime = System::fastTime();
		}
		
		// 判断当前表，是否可以不用恢复
		if (!judgeLogEntryValid(lsn, logEntry))
			continue;
		
		//FOR DEBUG ：进行tnt恢复过程中的日志打印，内容可见baseDir的dump_tnt_redo.log文件(需要打印恢复日志时)
		//parseTntRedoLogForDebug(redoSyslog, logEntry, lsn);

		try {
			// 处理不同类型的logType
			// 1.1 事务相关日志
			if (logType == TNT_BEGIN_TRANS)	{
				TrxId trxId1;
				u8 versionPoolId;
				TNTTransaction::parseBeginTrxLog(logEntry, &trxId1, &versionPoolId);
				assert(trxId == trxId1);
				m_tranSys->setMaxTrxIdIfGreater(trxId);
				recoverBeginTrx(trxId, lsn, versionPoolId);
			} else if (logType == TNT_COMMIT_TRANS) {
				if (purgeTrxId == trxId) {
					redoPurgeTntim(purgeTrxId, true);
					purgeTrxId = INVALID_TRX_ID;
				} else {
					recoverCommitTrx(trxId);
				}
				continue;//事务提交无需更新事务的lastLsn
			} else if (logType == TNT_PREPARE_TRANS) {
				recoverPrepareTrx(trxId, logEntry);
			} else if (logType == TNT_BEGIN_ROLLBACK_TRANS) {
				recoverBeginRollBackTrx(trxId, logEntry);
			} else if (logType == TNT_END_ROLLBACK_TRANS)	{
				recoverRollBackTrx(trxId);
				continue;//事务回滚无需更新事务的lastLsn
			} else if (logType == TNT_PARTIAL_BEGIN_ROLLBACK) {
				recoverPartialBeginRollBack(trxId, logEntry);
			} else if (logType == TNT_PARTIAL_END_ROLLBACK) {
				recoverPartialEndRollBack(trxId, logEntry);
			} else if (logType == TNT_BEGIN_PURGE_LOG) {
				//因为recover是从上一次的dump开始点恢复，而且由于purge后必然会触发dump，所以crash recover最多只会碰到一次purge
				assert(purgeTrxId == INVALID_TRX_ID);
				purgeTrxId = trxId;
				addRedoLogToTrans(trxId, logEntry);
			} else if( logType == TNT_END_PURGE_LOG || logType == TNT_PURGE_BEGIN_FIR_PHASE 
				|| logType == TNT_PURGE_BEGIN_SEC_PHASE || logType == TNT_PURGE_END_HEAP) {
				//purge redo不需要关心TNT_BEGIN_PURGE_LOG和TNT_END_PURGE_LOG
				if (purgeTrxId == INVALID_TRX_ID) {
					continue;
				}
				// 1.3 purge相关日志
				//获取最后一次purge的第一阶段开始LSN
				if (logType == TNT_PURGE_BEGIN_FIR_PHASE && purgePhaseOneLsn == INVALID_LSN) {
					purgePhaseOneLsn = lsn;
				} else if (logType == TNT_PURGE_BEGIN_SEC_PHASE && purgePhaseOneLsn != INVALID_LSN) {
					purgePhaseOneLsn = INVALID_LSN;
				}
				assert(purgeTrxId == trxId);
				addRedoLogToTrans(trxId, logEntry);
			} else if (logType == TNT_UNDO_I_LOG) {
				addRedoLogToTrans(trxId, logEntry);
			} else if (logType == TNT_UNDO_LOB_LOG) {
				addRedoLogToTrans(trxId, logEntry);
			} else if (logType == TNT_U_I_LOG || logType == TNT_U_U_LOG || logType == TNT_D_I_LOG || logType == TNT_D_U_LOG) { 
				// 1.4 用户dml日志
				addRedoLogToTrans(trxId, logEntry);
			}
			updateTrxLastLsn(trxId, lsn);
		} catch(NtseException &e) {
			m_tntSyslog->log(EL_ERROR, e.getMessage());
			m_tntTxnLog->endScan(logScan);
			//releaseDumpRecover();
			throw e;
		}
	}
	m_tntTxnLog->endScan(logScan);

	//从最早开始rollback的未结束事务的lsn和未完成purge的较早的LSN开始，重新再扫描一次
  	LsnType beginRollbackLsn = getFirstUnfinishedRollbackLsn();
	LsnType beginScanLsn = min(beginRollbackLsn, purgePhaseOneLsn);
	if (beginScanLsn != INVALID_LSN) {
		LogScanHandle *logScanAgain = m_tntTxnLog->beginScan(beginScanLsn, m_tntTxnLog->tailLsn());
		while (m_tntTxnLog->getNext(logScanAgain)) {
			const LogEntry	*logEntry	= logScanAgain->logEntry();
			//如果log不是tnt日志，则在recover中可以直接跳过
			if (!logEntry->isTNTLog()) {
				continue;
			}
			LsnType		lsn			= logScanAgain->curLsn();
			TrxId		trxId		= parseTrxId(logEntry);
			//LogType		logType		= logEntry->m_logType;
			//u16			tableId		= logEntry->m_tableId;
			if (logEntry->m_logType == TNT_UNDO_LOB_LOG){
				if (lsn > beginRollbackLsn)
					addTransactionLobId(lsn, trxId, logEntry);
				if (lsn > purgePhaseOneLsn)
					addLobIdToPurgeHash(purgeTrxId, logEntry);
			}
		}
		m_tntTxnLog->endScan(logScanAgain);
	}
	try {
		recoverRedoPrepareTrxs(true, false);
		// 重做版本池回收
		redoReclaimLob();
		// 4.3 TNT进行undo阶段recover
		// TODO 4.3.1. TNT undo必须等待NTSE recover完成，
		//但目前由于tnt的recover是等ntse的recover完成后再做，所以暂时不完成此函数。以后如果是并行recover，需要完成此函数
		// waitNTSECrashRecoverFinish();
		recoverUndoTrxs(TRX_ACTIVE, purgeTrxId);
	} catch (NtseException &e) {
		m_tntSyslog->log(EL_ERROR, e.getMessage());
		//releaseDumpRecover();
		throw e;
	}
		
	try {
		// 4.4  purge的redo
		if (purgeTrxId != INVALID_TRX_ID) {
			redoPurgeTntim(purgeTrxId, false);
		}

		// 4.5. 为内存所有Heap创建index
		dumpRecoverBuildIndice();
		//releaseDumpRecover();
	} catch (NtseException &e) {
		m_tntSyslog->log(EL_ERROR, e.getMessage());
		//releaseDumpRecover();
		throw e;
	}

	assert(m_activeTrxProcs->getSize() == 0);
	delete m_activeTrxProcs;
	m_activeTrxProcs = NULL;

	delete m_purgeInsertLobs;			//恢复结束删除purge的LobId Hash
	m_purgeInsertLobs = NULL;

	assert(m_rollbackTrx->getSize() == 0);	//恢复结束，没有正在回滚的事务，删除回滚事务表，
	delete m_rollbackTrx;
	m_rollbackTrx = NULL;

	// 恢复结束，首先关闭内存TNTTable，保留TNTTableBase
	closeOpenTables(true);
	// 然后关闭NTSE Table，并且需要flush data
	//m_db->closeOpenTables(true);

	if (m_tntConfig->m_purgeAfterRecover) {
		purgeAndDumpTntim(PT_PURGEPHASE2, false, true, false);
	}
	
	// 关闭打印恢复日志的临时log(需要打印恢复日志时)
	//delete redoSyslog;
}

/** 验证数据库正确性
 * @pre 恢复已经完成
 */
void TNTDatabase::verify(int recover) {
	UNREFERENCED_PARAMETER(recover);
}

/** 初始化切换版本池
 * @param proc 切换版本池process结构
 */
void TNTDatabase::initSwitchVerPool(SwitchVerPoolProcess *proc) {
	Connection *conn = m_db->getConnection(true, "switchActiveVerPoolIfNeeded");
	Session *session = m_db->getSessionManager()->allocSession("switchActiveVerPoolIfNeeded", conn);
	proc->m_conn = conn;
	proc->m_session = session;
}

/** 释放切换版本池process结构中的资源
 * @param proc 切换版本池process结构
 */
void TNTDatabase::delInitSwitchVerPool(SwitchVerPoolProcess *proc) {
	Connection *conn = proc->m_conn;
	Session *session = proc->m_session;
	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(conn);
}

/**
* 如果当前version pool的size超过预定义上限，则切换；此函数由后台线程调用，切换动作是单线程的
* @pre1	 注意，切换版本池时，需要持有两个mutex，分别是trx_sys::mutex, TNTControlFile::mutex
*		 持有的顺序是，先持有trx_sys::mutex，再持有TNTControlFile::mutex
*
* @pre2	 切换Version Pool时，需要有两个保证
*		 1. 所有的新事务无法产生
*		 2. 所有的已分配事务，必须也已分配到对应的version pool
*
*
* @param tntVerPoolSize TNT定义的单个version pool的最大size
* @param curVerPoolSize	当前事务的version pool size
*
*/
void TNTDatabase::switchActiveVerPoolIfNeeded() {
	SwitchVerPoolProcess switchVerPoolProcess;

	// 初始化切换版本池结构
	initSwitchVerPool(&switchVerPoolProcess);
	// 获取TrxSys的mutex，并读取系统中最大的TrxId
	m_tranSys->lockTrxSysMutex();
	TrxId maxTrxId = m_tranSys->getMaxTrxId();
	
	// 然后获取TNTControlFile自己的mutex，获取ActiveVerPoolNum
	m_tntCtrlFile->lockCFMutex();
	u8 activeVerPool	= m_tntCtrlFile->getActiveVerPoolForSwitch();
	u8 verPoolCnt		= m_tntCtrlFile->getVersionPoolCnt();
	Session *session = switchVerPoolProcess.m_session;
	// 检查active version pool是否超过预定义上限
	if (m_vPool->getDataLen(session, activeVerPool) < m_tntConfig->m_verpoolFileSize) {
		m_tntCtrlFile->unlockCFMutex();
		m_tranSys->unlockTrxSysMutex();
		delInitSwitchVerPool(&switchVerPoolProcess);
		return;
	}

	// 记录版本池切换的时间
	m_tntStatus.m_switchVerPoolLastTime = System::fastTime();

	u8 newActiveId = (activeVerPool == (verPoolCnt - 1)) ? 0 : (activeVerPool + 1);
	
	// 进行真正的切换操作
	if (!m_tntCtrlFile->switchActiveVerPool(newActiveId, maxTrxId)) {
		m_tntSyslog->log(EL_LOG, "version pool %d is used, switch version pool fail", newActiveId);
	} else {
		m_vPool->setVersionPoolHasLobBit(newActiveId, false);
		m_tranSys->setActiveVersionPoolId(newActiveId, false);
		m_tntStatus.m_switchVersionPoolCnt++;
	}
	
	// 释放资源
	m_tntCtrlFile->unlockCFMutex();
	m_tranSys->unlockTrxSysMutex();
	delInitSwitchVerPool(&switchVerPoolProcess);
}

/**	Dump操作开始，获取dump serial number
*	@dumpProcess	dump操作数据结构
*/
void TNTDatabase::beginDumpTntim(PurgeAndDumpProcess *purgeAndDumpProcess) {
	UNREFERENCED_PARAMETER(purgeAndDumpProcess);
	// 读取TNT ControlFile的dumpSN，然后赋值给dump
//	purgeAndDumpProcess->m_dumpSN = m_tntCtrlFile->getDumpSN() + 1;
}

/**	Dump操作结束，将dump信息写入TNT ControlFile；由于dump是后台单线程处理，可以不加ControlFile Mutex
*	@dumpProcess	dump操作数据结构
*/
void TNTDatabase::finishDumpTntim(PurgeAndDumpProcess *purgeAndDumpProcess) {
	// 如果purge发生跳表，本次dump失效
	if (! purgeAndDumpProcess->m_dumpSuccess)
		return;
	
	m_tntStatus.m_dumpCnt++;

	// 获取系统此时的最大事务号作为恢复用的起始max_trxId
	TrxId maxTrxId = m_tranSys->getMaxDumpTrxId();

	m_tntCtrlFile->lockCFMutex();

	// 设置TNTControlFile中的Dump相关属性
	//m_tntCtrlFile->setDumpBegin(purgeAndDumpProcess->m_begin);
	//m_tntCtrlFile->setDumpEnd(purgeAndDumpProcess->m_end);

	m_tntCtrlFile->setDumpLsn(purgeAndDumpProcess->m_dumpLsn);
	
	m_tntCtrlFile->setMaxTrxId(maxTrxId);

	m_tntCtrlFile->updateFile();

	m_tntCtrlFile->unlockCFMutex();

}

/** 判断log日志是否含有tableId
 * @param log 日志
 * return 如果该日志项含有有效的tableId返回true，否则返回false
 */
bool TNTDatabase::hasTableId(const LogEntry *log) {
	LogType logType = log->m_logType;
	if (logType == TNT_BEGIN_TRANS || logType == TNT_BEGIN_ROLLBACK_TRANS || logType == TNT_END_ROLLBACK_TRANS
		|| logType == TNT_PREPARE_TRANS || logType == TNT_COMMIT_TRANS || logType == TNT_BEGIN_PURGE_LOG 
		|| logType == TNT_END_PURGE_LOG
		|| logType == TNT_PARTIAL_BEGIN_ROLLBACK || logType == TNT_PARTIAL_END_ROLLBACK) {
		return false;
	} else {
		return true;
	}
}

/** 判断tableId是否需要做恢复
* @param tableId 表id
* return 如果是有效日志，返回true，否则返回false
*/
bool TNTDatabase::judgeLogEntryValid(u64 lsn, const LogEntry *log) {
	if (!hasTableId(log)) {
		return true;
	}
	u16 tableId = log->m_tableId;

	//判断是否是小型大对象（变长堆数据），ID虽然不是normal，但是在TNT中的大对象也需要恢复
	assert(TableDef::tableIdIsNormal(tableId));

	if (m_db->getControlFile()->getTablePath(tableId).empty()) {
		return false;
	}

	if (m_db->getControlFile()->getTntFlushLsn(tableId) > lsn) {
		nftrace(ts.recv, tout << "Skip log " << lsn << " because tntFlushLsn " << m_db->getControlFile()->getTntFlushLsn(tableId)
			<< " of table " << tableId << " is ahead of it");
		return false;
	}
	
	return true;
}

/** copy log
 * @param log 待拷贝log
 * @param ctx 分配内存上下文
 * return 拷贝完log
 */
LogEntry *TNTDatabase::copyLog(const LogEntry *log, MemoryContext *ctx) {
	LogEntry *ret = (LogEntry *)ctx->alloc(sizeof(LogEntry));
	memcpy(ret, log, sizeof(LogEntry));
	ret->m_data = (byte *)ctx->alloc(ret->m_size);
	memcpy(ret->m_data, log->m_data, log->m_size);
	return ret;
}

/** recover阶段，开启一个事务
* @trxId		事务ID
*/
void TNTDatabase::recoverBeginTrx(TrxId trxId, LsnType lsn, u8 versionPoolId) {
	assert(trxId != INVALID_TRX_ID && lsn != INVALID_LSN && versionPoolId != INVALID_VERSION_POOL_INDEX);
	Connection *conn = m_db->getConnection(false);
	Session *session = m_db->getSessionManager()->allocSession("recover transaction", conn);
	MemoryContext *ctx = session->getMemoryContext();
	RecoverProcess *proc = (RecoverProcess *)ctx->alloc(sizeof(RecoverProcess));
	proc->m_session = session;
	proc->m_trx = m_tranSys->allocTrx(trxId);
	proc->m_trx->setVersionPoolId(versionPoolId);
	proc->m_trx->disableLogging();
	proc->m_trx->startTrxIfNotStarted(conn);
	proc->m_session->setTrans(proc->m_trx);
	proc->m_trx->setTrxBeginLsn(lsn);
	proc->m_trx->setReadOnly(false);
	proc->m_logs = new (ctx->alloc(sizeof(DList<LogEntry *>))) DList<LogEntry *>();
	m_activeTrxProcs->put(trxId, proc);
}

/** recover阶段，提交一个事务
* @trxId		事务ID
*/
void TNTDatabase::recoverCommitTrx(TrxId trxId) {
	RecoverProcess *proc = m_activeTrxProcs->get(trxId);
	//处理TNT_END_RECLAIM_LOBPTR日志中，已经将该事务commit
	if (!proc) {
		return;
	}
	TNTTransaction *trx = proc->m_trx;
	recoverRedoTrx(proc, RT_COMMIT);
	m_activeTrxProcs->remove(trx->getTrxId());
	proc->m_trx->commitTrx(CS_RECOVER);
	releaseRecoverProcess(proc);
	m_tranSys->freeTrx(trx);
}

/** recover阶段，prepare一个事务
* @trxId		事务ID
*/
void TNTDatabase::recoverPrepareTrx(TrxId trxId, const LogEntry *log) {
	RecoverProcess *proc = m_activeTrxProcs->get(trxId);
	if (!proc) {
		return;
	}
	TrxId trxId1;
	LsnType preLsn;
	XID   xid;
	TNTTransaction::parsePrepareTrxLog(log, &trxId1, &preLsn, &xid);
	assert(trxId1 == trxId);
	proc->m_trx->setXId(xid);
	proc->m_trx->prepareForMysql();
}

/** recover阶段，开始回滚一个事务
* @trxId		事务ID
* @lsn			begin rollback日志
*/
void TNTDatabase::recoverBeginRollBackTrx(TrxId trxId, const LogEntry *log) {
	RecoverProcess *proc = m_activeTrxProcs->get(trxId);
	if (!proc) {
		return;
	}
	m_rollbackTrx->put(trxId, proc); //将事务加入回滚事务表
	TNTTransaction *trx = m_activeTrxProcs->get(trxId)->m_trx;
	trx->initRollbackInsertLobHash();//初始化事务回滚LobId哈希
	trx->setTrxBeginRollbackLsn(log->m_lsn);
	trx->setTrxState(TRX_ACTIVE);
}

/** recover阶段，回滚一个事务
* @trxId		事务ID
*/
void TNTDatabase::recoverRollBackTrx(TrxId trxId) {
	RecoverProcess *proc = m_activeTrxProcs->get(trxId);
	if (!proc) {
		return;
	}
	TNTTransaction *trx = proc->m_trx;
	if(trx->getRollbackInsertLobHash() != NULL)
		trx->releaseRollbackInsertLobHash();	//析构事务回滚时记录大对象重用信息的hash
	m_activeTrxProcs->remove(trx->getTrxId());
	proc->m_trx->rollbackForRecover(proc->m_session, NULL);
	//assert(m_rollbackTrx->get(trxId) != NULL);
	m_rollbackTrx->remove(trxId);//从回滚事务表中删除该事务
	releaseRecoverProcess(proc);
	m_tranSys->freeTrx(trx);
}

/** recover阶段，partial rollback begin处理
 * @param trxId 事务id
 * @param log   partial begin rollback日志
 */
void TNTDatabase::recoverPartialBeginRollBack(TrxId trxId, const LogEntry *log) {
	RecoverProcess *proc = m_activeTrxProcs->get(trxId);
	if (!proc) {
		return;
	}

	m_rollbackTrx->put(trxId, proc); //将事务加入回滚事务表
	m_activeTrxProcs->get(trxId)->m_trx->initRollbackInsertLobHash();//初始化事务回滚LobId哈希
	m_activeTrxProcs->get(trxId)->m_trx->setTrxBeginRollbackLsn(log->m_lsn);
}

/** recover阶段，partial rollback的end处理
 * @param trxId 事务id
 */
void TNTDatabase::recoverPartialEndRollBack(TrxId trxId, const LogEntry *log) {
	RecoverProcess *proc = m_activeTrxProcs->get(trxId);
	if (!proc) {
		return;
	}
	TNTTransaction *trx = proc->m_trx;
	trx->releaseRollbackInsertLobHash();	//析构事务回滚时记录大对象重用信息的hash
	LsnType preLsn = parsePreLsn(log);
	DLink<LogEntry *> *logEntry = NULL;
	while ((logEntry = proc->m_logs->removeLast()) != NULL) {
		if (preLsn != INVALID_LSN && logEntry->get()->m_lsn <= preLsn) {
			proc->m_logs->addLast(logEntry);
			break;
		}
	}
	if(m_rollbackTrx->get(trxId) != NULL)
		m_rollbackTrx->remove(trxId);
}

void TNTDatabase::releaseRecoverProcess(RecoverProcess *proc) {
	Session *session = proc->m_session;
	Connection *conn = session->getConnection();
	session->setTrans(NULL);
	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(conn);
}

/** crash recover过程中构造该事务commit的剩余步骤
 * @param trxId 事务id
 */
void TNTDatabase::makeupCommitTransaction(TrxId trxId, bool log) {
	RecoverProcess *proc = m_activeTrxProcs->get(trxId);
	if (!proc) {
		assert(false);
		return;
	}

	if (log) {
		proc->m_trx->enableLogging();
	}

	TNTTransaction *trx = proc->m_trx;
	m_activeTrxProcs->remove(trx->getTrxId());
	trx->commitTrx(CS_RECOVER);
	releaseRecoverProcess(proc);
	m_tranSys->freeTrx(trx);
}

/** redo当前存活事务中状态为target的所有事务
 * @param target redo事务的目标状态
 * @param free  标识是否需要将trx回收
 */
void TNTDatabase::recoverRedoPrepareTrxs(bool enableLog, bool free) {
	RecoverProcess *proc = NULL;
	TrxState state;
	TNTTransaction *trx = NULL;
	size_t size = m_activeTrxProcs->getSize();
	RecoverProcess **procs = new RecoverProcess*[size];
	m_activeTrxProcs->values(procs);
	for (uint i = 0; i < size; i++) {
		proc = procs[i];
		trx = proc->m_trx;
		state = trx->getTrxState();
		if (state != TRX_PREPARED) {
			continue;
		}
		recoverRedoTrx(proc, RT_PREPARE);
		m_activeTrxProcs->remove(trx->getTrxId());
		m_rollbackTrx->remove(trx->getTrxId()); //从回滚事务表中删除

		/*对prepare事务需要将事务的连接置成NULL，这个connection会在接下来free，
		  等恢复完成上层回滚会为这个事务分配新的connection*/
		trx->setConnection(NULL);
		releaseRecoverProcess(proc);
		if (enableLog) {
			trx->enableLogging();
		}

		if (free) {
			m_tranSys->freeTrx(trx);
		}
	}
	delete[] procs;
	procs = NULL;
}

/** 根据事务的日志做redo操作
 * @param proc 事务恢复流程的数据结构，内包括该事务所有需要redo的log
 */
void TNTDatabase::recoverRedoTrx(RecoverProcess *proc, RedoType redoType) {
	Session *session = proc->m_session;
	DLink<LogEntry *> *log = NULL;
	while ((log = proc->m_logs->removeFirst()) != NULL) {
		redoLogEntry(session, log->get(), redoType);
	}
}

/** crash recover过程中undo指定事务状态的所有事务
 * @param target 需要undo事务的事务状态
 */
void TNTDatabase::recoverUndoTrxs(TrxState target, TrxId purgeTrxId) {
	RecoverProcess *proc = NULL;
	Connection *conn = NULL;
	TrxState state;
	TNTTransaction *trx = NULL;
	size_t size = m_activeTrxProcs->getSize();
	if (size == 0) {
		return;
	}
	RecoverProcess **procs = new RecoverProcess*[size];
	m_activeTrxProcs->values(procs);
	for (uint i = 0; i < size; i++) {
		proc = procs[i];
		trx = proc->m_trx;
		if (trx->getTrxId() == purgeTrxId) {
			continue;
		}
		conn = proc->m_session->getConnection();
		state = trx->getTrxState();
		if (state != target) {
			continue;
		}
		trx->enableLogging();
		m_activeTrxProcs->remove(trx->getTrxId());
		m_rollbackTrx->remove(trx->getTrxId()); //从回滚事务表中删除
		trx->rollbackForRecover(proc->m_session, proc->m_logs);
		releaseRecoverProcess(proc);
		m_tranSys->freeTrx(trx);
	}
	delete[] procs;
	procs = NULL;
}

/** recover阶段，将属于同一事务的redo日志串联起来
* @trxId		事务ID
* @logEntry		redo日志
*/
void TNTDatabase::addRedoLogToTrans(TrxId trxId, const LogEntry* log) {
	RecoverProcess *proc = m_activeTrxProcs->get(trxId);
	//该事务的startTransaction位于dumpPointLsn之间，但在dump开始时，该事务肯定已提交，所以该事务的影响肯定被dump到外存了
	if (!proc) {
		return;
	}
	MemoryContext *ctx = proc->m_session->getMemoryContext();
	LogEntry *logEntry = copyLog(log, ctx);
	DLink<LogEntry *> *logLink = new (ctx->alloc(sizeof(DLink<LogEntry *>))) DLink<LogEntry *>(logEntry);
	proc->m_logs->addLast(logLink);
}

/** 更新事务的lastlsn
 * @param trxId 需要更新的事务id
 * @param lsn   设置lastLsn的值
 */
void TNTDatabase::updateTrxLastLsn(TrxId trxId, LsnType lsn) {
	RecoverProcess *proc = m_activeTrxProcs->get(trxId);
	if (!proc) {
		return;
	}
	proc->m_trx->setTrxLastLsn(lsn);
}

/** 根据tableId获取tnt表，目前只用于recover流程
 * @param session 会话
 * @param tableId 表id
 */
TNTTable* TNTDatabase::getTNTTable(Session *session, u16 tableId) throw(NtseException) {
	NTSE_ASSERT(DB_RECOVERING == m_dbStat);
	string pathStr = m_ntseCtrlFile->getTablePath(tableId);
	if (pathStr.empty()) {
		return NULL;
	}

	char *path = (char *)pathStr.c_str();
	// 首先，打开NTSE Table
	Table *ntseTable = NULL;
	try {
		ntseTable = m_db->getTable(tableId);
	} catch (NtseException &e) {
		throw e;
	}
	
	// NTSE open成功，内部不放锁
	// open TNT table
	TNTTableInfo* tableInfo = m_pathToTables->get(path);
	if (!tableInfo) {
		TNTTableBase *tableBase = NULL;
		TNTTable *table = NULL;
		tableBase = m_idToTabBase->get(tableId);
		if (tableBase == NULL) {
			tableBase = new TNTTableBase();
			// 将tableBase存入hash表
			m_idToTabBase->put(tableId, tableBase);
			// 将tableBase存入双向链表
			m_tntTabBases.addLast(&tableBase->m_tableBaseLink);
		}
		table = TNTTable::open(this, session, ntseTable, tableBase);

		tableInfo = new TNTTableInfo(table, path);
		assert(!m_idToTables->get(tableId));
		m_idToTables->put(tableId, tableInfo);
		m_pathToTables->put(tableInfo->m_path, tableInfo);
	} else {
		tableInfo->m_table->setNtseTable(ntseTable);
	}

	return tableInfo->m_table;
}


/** 根据log执行undo操作
 * @param session 会话
 * @param log     事务日志记录
 * @param crash   用于指明是crash的undo还是log的undo
 */
LsnType TNTDatabase::undoLogEntry(Session *session, TNTTable * table, const LogEntry *log, const bool crash) {
	LsnType preLsn = INVALID_LSN;
	switch (log->m_logType) {
		case TNT_BEGIN_TRANS:
			return preLsn;
		case TNT_PREPARE_TRANS:
		case TNT_BEGIN_ROLLBACK_TRANS:
		case TNT_END_ROLLBACK_TRANS:
		case TNT_PARTIAL_BEGIN_ROLLBACK:
		case TNT_PARTIAL_END_ROLLBACK:
			preLsn = parsePreLsn(log);
			return preLsn;
	}

	//该表有可能被drop，则直接越过该表
	if (table == NULL) {
		preLsn = parsePreLsn(log);
		return preLsn;
	}
	//如果此时为非事务表，做redo也无妨，因为purge第二阶段写日志后会flush外存
	assert(table->getNtseTable()->getTableDef()->getTableStatus() != TS_CHANGING
		&& table->getNtseTable()->getTableDef()->getTableStatus() != TS_SYSTEM);
	switch (log->m_logType) {
		case TNT_UNDO_I_LOG:
			//普通字段的回滚
			preLsn = table->undoInsert(session, log, crash);
			goto _Finish;
		case TNT_UNDO_LOB_LOG:
			//大对象回滚
			preLsn = table->undoInsertLob(session,log);
			goto _Finish;
		default:
			//如果是crash的recover，不需要考虑update和delete的undo
			if (crash) {
				preLsn = parsePreLsn(log);
				goto _Finish;
			}
	}

	switch (log->m_logType) {
		case TNT_U_I_LOG:
			preLsn = table->undoFirUpdate(session, log, crash);
			break;
		case TNT_U_U_LOG:
			preLsn = table->undoSecUpdate(session, log, crash);
			break;
		case TNT_D_I_LOG:
			preLsn = table->undoFirRemove(session, log, crash);
			break;
		case TNT_D_U_LOG:
			preLsn = table->undoSecRemove(session, log, crash);
			break;
		default:
			assert(false);
	}

_Finish:

	return preLsn;
}

/** 根据log从endLsn到beginLsn做同一个事务的undo，用于log recover
 * @param session  会话
 * @param beginLsn 日志的起始lsn
 * @param endLsn 日志的结束lsn，事务由endLsn的log决定
 */
bool TNTDatabase::undoTrxByLog(Session *session, LsnType beginLsn, LsnType endLsn) {
	assert(endLsn != INVALID_LSN);
	TNTTableHash openTables;
	LogScanHandle *logScan = m_tntTxnLog->beginScan(endLsn, m_tntTxnLog->tailLsn());
	//log回滚时，相应的table都已经被加锁
	while (endLsn != INVALID_LSN && ((beginLsn == INVALID_LSN) || (beginLsn < endLsn))) {
		m_tntTxnLog->resetLogScanHandle(logScan, endLsn, m_tntTxnLog->tailLsn());
		m_tntTxnLog->getNext(logScan);
		assert(logScan != NULL);
		const LogEntry *logEntry = logScan->logEntry();
		assert(logScan->curLsn() == endLsn);
		bool lockByMe = false;
		TNTTable *table = NULL;
		u16 tableId = logEntry->m_tableId;
		if (likely(TableDef::tableIdIsNormal(tableId))) {
			if ((table = openTables.get(tableId)) == NULL) {
				string pathStr = m_ntseCtrlFile->getTablePath(tableId);
				//pathStr.empty()说明该表已经被drop
				if (!pathStr.empty()) {
Restart:
					try {
						table = openTable(session, pathStr.c_str());
						assert(table != NULL);
						openTables.put(table);
					} catch (NtseException &e) {
						if (e.getErrorCode() == ntse::NTSE_EC_FILE_NOT_EXIST) {
							m_tntSyslog->log(EL_WARN, "rollback open table error %s", e.getMessage());
							continue;
						} else if (e.getErrorCode() == ntse::NTSE_EC_LOCK_TIMEOUT)
							goto Restart;
						else
							m_tntSyslog->log(EL_PANIC, "open table error %s", e.getMessage());
					}
				}
			}
			if (NULL != table && table->getMetaLock(session) == IL_NO) {
				table->lockMeta(session, IL_S, -1, __FILE__, __LINE__);
				lockByMe = true;
			}
		} else {
			assert(TableDef::INVALID_TABLEID == tableId);
		}

		endLsn = undoLogEntry(session, table, logEntry, false);

		if (lockByMe) {
			table->unlockMeta(session, IL_S);
		}
	}
	m_tntTxnLog->endScan(logScan);

	size_t size = openTables.getSize();
	for (u16 i = 0; i < size; i++) {
		closeTable(session, openTables.getAt(i));
	}
	return true;
}

/** 根据logs做同一个事物的undo，用于crash recover
 * @param session 会话
 * @param logs  日志链表
 */
bool TNTDatabase::undoTrxByLog(Session *session, DList<LogEntry *> *logs) {
	if (logs == NULL) {
		return true;
	}

	DLink<LogEntry *> *log = NULL;
	while ((log = logs->removeLast()) != NULL) {
		LogEntry *logEntry = log->get();
		TNTTable *table = NULL;
		if (TableDef::tableIdIsNormal(logEntry->m_tableId)) {
			table = getTNTTable(session, logEntry->m_tableId);
		} else {
			assert(TableDef::INVALID_TABLEID == logEntry->m_tableId);
		}
		undoLogEntry(session, table, logEntry, true);
	}
	return true;
}

/** 判断该log遇到commit日志时是否需要做redo
 * @param log 事务日志记录
 * return 需要做redo返回true，否则返回false
 */
bool TNTDatabase::isNeedRedoLog(const LogEntry *log) {
	if (!hasTableId(log)) {
		return false;
	}

	assert(log->m_logType != TNT_PURGE_BEGIN_FIR_PHASE);
	assert(log->m_logType != TNT_PURGE_BEGIN_SEC_PHASE);
	assert(log->m_logType != TNT_PURGE_END_HEAP);

	return true;
}

/** 根据log执行redo操作
 * @param session 会话
 * @param log     日志记录
 */
void TNTDatabase::redoLogEntry(Session *session, const LogEntry *log, RedoType redoType) {
	if (!isNeedRedoLog(log)) {
		return;
	}

	TNTTable *table = getTNTTable(session, log->m_tableId);
	//该表如果不存在，则会在judgeLogEntryValid被过滤
	assert(table != NULL);
	if (redoType == RT_PREPARE) {
		session->getTrans()->lockTable(TL_IX, log->m_tableId);
	}

	switch (log->m_logType) {
		case TNT_UNDO_I_LOG:
			table->redoInsert(session, log, redoType);
			break;
		case TNT_U_I_LOG:
			table->redoFirUpdate(session, log, redoType);
			break;
		case TNT_U_U_LOG:
			table->redoSecUpdate(session, log, redoType);
			break;
		case TNT_D_I_LOG:
			table->redoFirRemove(session, log, redoType);
			break;
		case TNT_D_U_LOG:
			table->redoSecRemove(session, log, redoType);
			break;
		default:
			;
	}
}

/** recover阶段，对所有除了log本身事务外正在回滚的事务插入lobId
* @logEntry		TNT_UNDO_LOB_LOG日志
*/
void TNTDatabase::addTransactionLobId(LsnType logLsn, TrxId rollbackTrxId, const LogEntry* log){
	size_t rollbackHashSize = m_rollbackTrx->getSize();
	assert(rollbackHashSize > 0);
	TrxId  *key = new TrxId[rollbackHashSize];
	RecoverProcess  **value = new RecoverProcess*[rollbackHashSize];
	m_rollbackTrx->elements(key, value);
	for(size_t rollbackTrxCount = 0;rollbackTrxCount<rollbackHashSize; rollbackTrxCount++){
		if(key[rollbackTrxCount] != rollbackTrxId && logLsn > value[rollbackTrxCount]->m_trx->getBeginRollbackLsn()){
			Session *session = value[rollbackTrxCount]->m_session;
			MemoryContext *ctx = session->getMemoryContext();
			u16 tableId = log->m_tableId;
			TrxId trxId;
			u64	  preLsn;
			LobId lobId;
			LobStorage::parseTNTInsertLob(log, &trxId, &preLsn, &lobId);
			NTSE_ASSERT(value[rollbackTrxCount]->m_trx->getRollbackInsertLobHash()->put(new (ctx->alloc(sizeof(TblLob))) TblLob(tableId, lobId)));
		}
	}
	delete[] key;
	delete[] value;
}


/** recover阶段，记录所有在purgephase1之后插入的lobId
* @logEntry		TNT_UNDO_LOB_LOG日志
*/
void TNTDatabase::addLobIdToPurgeHash(TrxId purgeTrxId, const LogEntry* log) {
	Session *session = m_activeTrxProcs->get(purgeTrxId)->m_session;
	MemoryContext *ctx = session->getMemoryContext();
	u16 tableId = log->m_tableId;
	TrxId trxId;
	u64   preLsn;
	LobId lobId;
	LobStorage::parseTNTInsertLob(log, &trxId, &preLsn, &lobId);
	NTSE_ASSERT(m_purgeInsertLobs->put(new (ctx->alloc(sizeof(TblLob))) TblLob(tableId, lobId)));
}


/**	新建表
* @session	建表session
* @path		表名
* @tableDef	表定义
*/
void TNTDatabase::createTable(Session *session, const char *path, TableDef *tableDef) throw(NtseException) {
	// 直接调用NTSE::Database->createTable即可
	// DDL Lock的获取，需要提升到此函数之中
	acquireDDLLock(session, IL_X, __FILE__, __LINE__);

	try {
		m_db->createTable(session, path, tableDef);
	} catch (NtseException &e) {
		releaseDDLLock(session, IL_X);
		throw e;
	}

	// 创建TNTTableBase信息
	// 此处直接new空间，后续需要修改

	// 修改hash，链表结构，通过DDL Lock保护
	//tableBase = new TNTTableBase(path);

	// 将tableBase存入hash表
	//m_pathToTabBase->put((char *)tableBase->m_path, tableBase);
	// 将tableBase存入双向链表
	//m_tntTabBases.addLast(&tableBase->m_tableBaseLink);

	// DDL Lock的释放
	releaseDDLLock(session, IL_X);
}

/**	删除表
* @session		删表session
* @path			表名
* @timeoutSec	超时时间，单位秒，为<0表示不超时，0表示马上超时，>0为超时时间
* @throw Exception	指定的表不存在等，超时等
*/
void TNTDatabase::dropTable(Session *session, const char *path, int timeoutSec) throw(NtseException) {
	// 获取DDL Lock
	acquireDDLLock(session, IL_X, __FILE__, __LINE__);

	// 检查表是否存在
	// 此处通过DDL X Lock来保证没有并发的其他DDL，直接读取表元数据，是否可以？
	u16 tableId = m_ntseCtrlFile->getTableId(path);
	if (tableId == TableDef::INVALID_TABLEID) {
		releaseDDLLock(session, IL_X);
		
		NTSE_THROW(NTSE_EC_FILE_NOT_EXIST, "No such table '%s'", path);
	}

	string pathStr = m_ntseCtrlFile->getTablePath(tableId);
	// 获取ATS Lock
	acquireATSLock(session, IL_S, -1, __FILE__, __LINE__);
	try {
		// 等待TNT Table，包括NTSE Table真正被关闭
		waitRealClosed(session, path, timeoutSec);
	} catch (NtseException &e) {
		releaseATSLock(session, IL_S);
		releaseDDLLock(session, IL_X);
		throw e;
	}

	// 先将NTSE Table删除，注意：
	// 1. 此时DDL Lock，ATS Lock已经获取
	// 2. 与Database->dropTable函数中KV实现冲突
	try {
		m_db->dropTableSafe(session, path, timeoutSec);
	} catch (NtseException &e) {
		releaseATSLock(session, IL_S);
		acquireATSLock(session, IL_X, -1, __FILE__, __LINE__);
		// 无论NTSE是否Drop成功，都要删除TNT内存中的TableBase，否则就会导致内存泄漏
		TNTTableBase *tableBase = m_idToTabBase->get(tableId);
		if (tableBase != NULL) {
			tableBase->close(session, false);
			m_idToTabBase->remove(tableId);
			tableBase->m_tableBaseLink.unLink();
			delete tableBase;
			tableBase = NULL;
		}

		releaseATSLock(session, IL_X);
		releaseDDLLock(session, IL_X);
		throw e;
	}

	// 将tableBase从hash表，双向链表中删除
	// 由于涉及修改TNTTableBase的hash表，因此需要释放ATS S锁，加X锁
	// 同时，由于一直持有DDL X锁，当前表不会被open(open加DDL IS锁)
	releaseATSLock(session, IL_S);
	acquireATSLock(session, IL_X, -1, __FILE__, __LINE__);

	TNTTableBase	*tableBase = m_idToTabBase->get(tableId);
	if (tableBase != NULL) {
		tableBase->close(session, false);
		m_idToTabBase->remove(tableId);
		tableBase->m_tableBaseLink.unLink();
		delete tableBase;
		tableBase = NULL;
	}

	// dropTable成功，DDL Lock，ATS Lock的释放
	releaseATSLock(session, IL_X);
	releaseDDLLock(session, IL_X);
}

/** 若Table已打开，增加其引用计数
* @session		操作Session
* @path			表名
* @timeoutMs	锁超时时间
* @return		TNTTable实例
*/
TNTTable* TNTDatabase::pinTableIfOpened(Session *session, const char *path, int timeoutMs) {
	ftrace(ts.ddl || ts.dml, tout << path);
	assert(!m_closed && !m_unRecovered);

	try {
		acquireATSLock(session, IL_X, timeoutMs, __FILE__, __LINE__);
	} catch (NtseException &) {
		return NULL;
	}
	
	TNTTableInfo *tableInfo = m_pathToTables->get((char *)path);
	TableInfo *ntseTblInfo = m_db->getTableInfo((char *)path);
	if (tableInfo) {
		NTSE_ASSERT(ntseTblInfo != NULL);
		// 统计信息
		m_tntStatus.m_openTableCnt++;
		tableInfo->m_refCnt++;
		ntseTblInfo->m_refCnt++;

		// 如果refcnt从0变成1，移动LRU链表的位置
		if (ntseTblInfo->m_refCnt == 0) {
			m_db->getOpenTableLRU()->moveToLast(&ntseTblInfo->m_table->getOpenTablesLink());
		}

		releaseATSLock(session, IL_X);
		return tableInfo->m_table;
	}
	releaseATSLock(session, IL_X);
	return NULL;
}


/**	打开TNT Table
* @session	打开表session
* @path		表名
*/
TNTTable* TNTDatabase::openTable(Session *session, const char *path, bool needDDLLock/*= true*/) throw(NtseException) {	
	// 获取DDL Lock，防止打开表过程中表被RENAME或DROP

	// TRUNCATE与open是可以并发执行的
	// NTSE：TRUNCATE中的表，在open过程中，会堵在Database::openTable方法的lockMeta调用下，因为TRUNCATE加了Meta X锁
	// TNT： TRUNCATE中的表，在open过程中，会堵在Database::openTable方法的acquireATSLock调用下，因为TRUNCATE在修改TNTTableBase结构时，会获取ATS X锁
	
	if (needDDLLock) {
		acquireDDLLock(session, IL_IS, __FILE__, __LINE__);
	}
	
	// TODO：此处设置timeout为-1，后续需要改进

	try {
		acquireATSLock(session, IL_X, m_tntConfig->m_ntseConfig.m_tlTimeout * 1000, __FILE__, __LINE__);
	} catch (NtseException &e) {
		if (needDDLLock) {
			releaseDDLLock(session, IL_IS);
		}
		throw e;
	}

	// 首先，打开NTSE Table
	Table *ntseTable = NULL;
	try {
		ntseTable = m_db->openTable(session, path);
	} catch (NtseException &e) {
		releaseATSLock(session, IL_X);
		if (needDDLLock) {
			releaseDDLLock(session, IL_IS);
		}
		throw e;
	}

	ntseTable->tntLockMeta(session, IL_S, -1, __FILE__, __LINE__);
	if (TS_SYSTEM == ntseTable->getTableDef()->getTableStatus()) {
		ntseTable->tntUnlockMeta(session, IL_S);
		m_db->closeTable(session, ntseTable);
		releaseATSLock(session, IL_X);
		if (needDDLLock) {
			releaseDDLLock(session, IL_IS);
		}
		NTSE_THROW(NTSE_EC_OPEN_SYS_TBL, "Can't Open System Table(%s)", path);
	}
	ntseTable->tntUnlockMeta(session, IL_S);

	u16	tableId = m_ntseCtrlFile->getTableId(path);
	string pathStr = m_ntseCtrlFile->getTablePath(tableId);	// 大小写与传入的path可能不同
	assert(!pathStr.empty());
	
	// NTSE open成功，内部不放锁
	// open TNT table
	TNTTableInfo* tableInfo = m_pathToTables->get((char *)path);
	if (!tableInfo) {
		TNTTableBase	*tableBase;
		TNTTable *table = NULL;

		tableBase = m_idToTabBase->get(tableId);
		if (tableBase == NULL) {
			tableBase = new TNTTableBase();
			// 将tableBase存入hash表
			m_idToTabBase->put(tableId, tableBase);
			// 将tableBase存入双向链表
			m_tntTabBases.addLast(&tableBase->m_tableBaseLink);
		}
		try {
			table = TNTTable::open(this, session, ntseTable, tableBase);
		} catch (NtseException &e) {
			// 如果tableBase是未开状态，则需要从个链表中摘除
			if (!tableBase->m_opened) {
				m_idToTabBase->remove(tableId);
				tableBase->m_tableBaseLink.unLink();
				delete tableBase;
			}

			m_db->closeTable(session, ntseTable);
			releaseATSLock(session, IL_X);
			if (needDDLLock) {
				releaseDDLLock(session, IL_IS);
			}
			throw e;
		}
		

		tableInfo = new TNTTableInfo(table, pathStr.c_str());
		assert(!m_idToTables->get(tableId));
		m_idToTables->put(tableId, tableInfo);
		m_pathToTables->put(tableInfo->m_path, tableInfo);

	} else {
		tableInfo->m_table->setNtseTable(ntseTable);
		tableInfo->m_refCnt++;
	}

	// 统计信息
	m_tntStatus.m_openTableCnt++;

	// 释放DDL Lock，ATS Lock
	releaseATSLock(session, IL_X);
	if (needDDLLock) {
		releaseDDLLock(session, IL_IS);
	}

	return tableInfo->m_table;
}

/**	关闭表
* @session	关闭表session
* @table	已打开的表
*/
void TNTDatabase::closeTable(Session *session, TNTTable *table) {	
	ftrace(ts.ddl, tout << session << table->getNtseTable()->getPath());
	assert(!m_closed && !m_unRecovered);
	
	Table	*ntseTable = NULL;
	u16		tableId;
	// 获取ATS Lock
	acquireATSLock(session, IL_X, -1, __FILE__, __LINE__);

	// 读取TNTTableInfo 注意：
	ntseTable = table->getNtseTable();
	ILMode mode = table->getNtseTable()->getMetaLock(session);
	if (mode == IL_NO) {
		table->lockMeta(session, IL_S, -1, __FILE__, __LINE__);
	} else {
		assert(mode == IL_S || mode == IL_X);
	}
	tableId = ntseTable->getTableDef()->m_id;
	TNTTableInfo *tableInfo = m_idToTables->get(tableId);
	TableInfo *ntseTblInfo = m_db->getTableInfo(tableId);
	if (mode == IL_NO) {
		table->unlockMeta(session, IL_S);
	}

	assert(tableInfo);
	assert(tableInfo->m_table == table);
	assert(ntseTblInfo);
	assert(ntseTblInfo->m_table == ntseTable);

	tableInfo->m_refCnt--;
	ntseTblInfo->m_refCnt--;
	// 如果refcnt为0，移动LRU链表的位置
	if (ntseTblInfo->m_refCnt == 0) {
		m_db->getOpenTableLRU()->moveToFirst(&ntseTblInfo->m_table->getOpenTablesLink());
	}
	
	assert(tableInfo->m_refCnt <= ntseTblInfo->m_refCnt);

	// 统计信息
	m_tntStatus.m_closeTableCnt++;

	// 释放ATS Lock
	releaseATSLock(session, IL_X);
}

/**	如果引用为0关闭表
* @session	关闭表session
* @table	已打开的表
* @return	是否成功关闭表
*/
bool TNTDatabase::realCloseTableIfNeed(Session *session, Table *ntseTable) throw(NtseException) {
	ftrace(ts.ddl, tout << session << ntseTable->getPath());
	assert(!m_closed && !m_unRecovered);
	// 此时必然已持有ATS排他锁
	assert(getATSLock(session) == IL_X);

	// 由于此处只有waitRealClose 和 closeOpenTableIfNeed，因此必然之前没有加meta锁
	assert(IL_NO == ntseTable->getMetaLock(session));
	ntseTable->tntLockMeta(session, IL_S, -1, __FILE__, __LINE__);

	TableId tableId = ntseTable->getTableDef()->m_id;
	TNTTableInfo *tntTableInfo = m_idToTables->get(tableId);
	TableInfo *ntseTblInfo = m_db->getTableInfo(tableId);

	ntseTable->tntUnlockMeta(session, IL_S);
	

	assert(ntseTblInfo);
	assert(ntseTblInfo->m_table == ntseTable);

	bool closeTnt = false, closeNtse = false;
	if (tntTableInfo)
		closeTnt = tntTableInfo->m_refCnt == 0 ? true : false;
	closeNtse= ntseTblInfo->m_refCnt == 0 ? true : false;
	bool success = false;
	if (closeNtse) {
		// 当NTSE表的refCnt为0时，tnt表的refCnt也一定为0
		assert(closeTnt || !tntTableInfo);
		ntseTable->tntLockMeta(session, IL_X, -1, __FILE__, __LINE__);
		realCloseTable(session, tntTableInfo, ntseTblInfo, true, closeTnt, closeNtse);	
		success = true;
	}

	return success;
}

/**	truncate表
* @并发控制说明：
* 1. truncate操作，由ha_tnt的delete_all_rows方法调用，truncate语句，需要open_table/external_lock 
	 由于当前表上的其他ddl操作会调用waitRealClose关闭表，因此其他ddl操作不能并发进行
* 2. 在ha_tnt::delete_all_rows方法中，会将meta s锁升级为meta x锁
	 保证当前表上的所有的dml操作无法并发执行
* 3. 后台线程操作(Purge/Dump/Defrag)，直接调用TNTDatabase层面的openTable方法打开表，
	 加DDL IS Lock，ATS X Lock，MetaLock S Lock，因此不会与truncate中的DDL IX冲突，但是会与
	 MetaLock X Lock冲突。如此一来，有以下问题：
	 3.1 Open Table先开始，会OpenTable成功，后续操作此表，需要加MetaLock后才能访问
	 3.2 Open Table后开始，会等待在Truncate的MetaLock上，直至Truncate返回后，OpenTable方能成功
* 4. truncate操作本身加DDL IX锁
	 阻止并发的备份操作，备份需要加DDL S锁
* 5. 在tnt table进行truncate时，加ATS X锁
	 因为需要修改TableBase结构，而此结构，是通过ATS锁保护的
** @并发控制说明

* @session			truncate表session
* @table			已打开的表
* @isTruncateOper	
*/
void TNTDatabase::truncateTable(Session *session, TNTTable *table, bool isTruncateOper) throw(NtseException) {
	Table			*ntseTable	= table->getNtseTable();
	//TNTTransaction	*trx		= session->getTrans();
	u16				tableId;
	// 获取DDL Lock
	acquireDDLLock(session, IL_IX, __FILE__, __LINE__);

	tableId = ntseTable->getTableDef(true, session)->m_id;

	try {
		// 由于其中涉及到修改TNTTableBase结构，因此需要ATS X锁保护
		acquireATSLock(session, IL_X, m_tntConfig->m_ntseConfig.m_tlTimeout * 1000, __FILE__, __LINE__);
		// truncate NTSE table
		//虽然Table::truncate是通过建立新的空表来实现，但是其Id和tableName都没有改变
		bool newHasDict = false;
		ntseTable->truncate(session, false, &newHasDict, isTruncateOper);

		//修改控制文件中是否含有全局字典的信息
		alterCtrlFileHasCprsDic(session, tableId, newHasDict);
	} catch (NtseException &e) {
		releaseDDLLock(session, IL_IX);
		throw e;
	}

	// Sync Point
	// openTable操作与Truncate操作并发的同步点，openTable可以看到Truncate的中间过程
	SYNCHERE(SP_DB_TRUNCATE_CURRENT_OPEN);

	// truncate TNT table
	table->truncate(session);
	
	releaseATSLock(session, IL_X);
	releaseDDLLock(session, IL_IX);
}

/**	rename表
* @session			rename表session
* @oldPath			旧表名
* @newPath			新表名
* @timeoutSec			
*/
void TNTDatabase::renameTable(Session *session, const char *oldPath, 
				 const char *newPath, int timeoutSec) throw(NtseException) {
	// rename操作，不需要将TNTTable从hash表中摘除，因为close会完成此操作
	// 但是，rename需要修改TNTTableBase结构，以及对应的hash表
	
	// 获取DDL Lock
	acquireDDLLock(session, IL_X, __FILE__, __LINE__);
	// 获取ATS Lock
	acquireATSLock(session, IL_S, -1, __FILE__, __LINE__);

	// 等待TNT Table，包括NTSE Table真正被关闭
	try {
		waitRealClosed(session,oldPath, timeoutSec);
	} catch (NtseException &e) {
		releaseATSLock(session, IL_S);
		releaseDDLLock(session, IL_X);
		throw e;
	}	

	// 进行NTSE层面的rename操作，NTSE 根据Limits::EXTS[]数组定义的顺序，Rename表的所有文件：
	//	NAME_IDX_EXT：			索引文件
	//	NAME_HEAP_EXT：			堆文件
	//	NAME_TBLDEF_EXT：		TableDef文件
	//	NAME_SOBH_EXT：			小型大对象文件
	//	NAME_SOBH_TBLDEF_EXT：	
	//	NAME_LOBI_EXT：
	//	NAME_LOBD_EXT：
	//	NAME_GLBL_DIC_EXT：
	try {
		m_db->renameTableSafe(session, oldPath, newPath, timeoutSec);
	} catch (NtseException &e) {
		releaseATSLock(session, IL_S);
		releaseDDLLock(session, IL_X);
		throw e;
	}

	// rename完成之后，修改TNTTableBase结构
	// 由于TNTTable已经被close，因此不需要处理

	// 由于涉及修改TNTTableBase的hash表，因此需要释放ATS S锁，加X锁
	// 同时，由于一直持有DDL X锁，当前表不会被open(open加DDL IS锁)
	releaseATSLock(session, IL_S);
	/*acquireATSLock(session, IL_X, -1, __FILE__, __LINE__);

	TNTTableBase *tableBase = m_pathToTabBase->get((char *)oldPath);
	if (tableBase != NULL) {
		// TNTTableBase结构存在，证明表曾经被open过

		// 首先将TableBase从Hash表中摘除
		NTSE_ASSERT(m_pathToTabBase->remove((char *)oldPath));
		
		// 然后重命名TableBase
		tableBase->rename(newPath);
		
		// 最后将tableBase存入hash表
		m_pathToTabBase->put(tableBase->m_path, tableBase);

		// TNTTableBase的链表不需要处理
	}

	// 释放DDL Lock & ATS Lock
	releaseATSLock(session, IL_X);*/
	releaseDDLLock(session, IL_X);
}

/**	表上增加索引
* @session			操作session
* @table			已打开的表
* @numIndice		索引序号
* @indexDef			index的定义	
*/
void TNTDatabase::addIndex(Session *session, TNTTable *table, u16 numIndice, 
			  const IndexDef **indexDefs) throw(NtseException) {
	// 1. 获取DDL Lock
	acquireDDLLock(session, IL_IX, __FILE__, __LINE__);

	try {
		table->addIndex(session, indexDefs, numIndice);
	} catch (NtseException &e) {
		releaseDDLLock(session, IL_IX);
		throw e;
	}

	releaseDDLLock(session, IL_IX);
}

/**	表上删除索引
* @session			操作session
* @table			已打开的表
* @idx				删除索引序号	
*/
void TNTDatabase::dropIndex(Session *session, TNTTable *table, uint idx) throw(NtseException) {
	// 1. 获取DDL Lock
	acquireDDLLock(session, IL_IX, __FILE__, __LINE__);

	try {
		table->dropIndex(session, idx);
	} catch (NtseException &e) {
		releaseDDLLock(session, IL_IX);
		throw e;
	}
	
	releaseDDLLock(session, IL_IX);
}

/**	修改表参数
* @session			操作session
* @table			已打开的表
* @name				参数名
* @valueStr			参数值
* @timeout			过期时间
* @param inLockTables  操作是否处于Lock Tables 语句之中
*/
void TNTDatabase::alterTableArgument(Session *session, TNTTable *table, 
						const char *name, const char *valueStr, int timeout, bool inLockTables) throw(NtseException) {
	// 功能完全交由NTSE Database 完成
	// 但是所有的并发控制，需要在TNT中完成
	// 1. 获取DDL Lock
	acquireDDLLock(session, IL_IX, __FILE__, __LINE__);

	try {
		table->alterTableArgument(session, name, valueStr, timeout, inLockTables);
	} catch (NtseException &e) {
		releaseDDLLock(session, IL_IX);
		throw e;
	}

	releaseDDLLock(session, IL_IX);
}

/**	
* @session			操作session
* @tblId			表ID
* @hasCprsDic			
*/
void TNTDatabase::alterCtrlFileHasCprsDic(Session *session, u16 tblId, bool hasCprsDic) {
	UNREFERENCED_PARAMETER(session);
	UNREFERENCED_PARAMETER(tblId);
	UNREFERENCED_PARAMETER(hasCprsDic);
	// 完全交由NTSE Database完成
	/*
	try {
		m_db->alterCtrlFileHasCprsDic(session, tblId, hasCprsDic);
	} catch (NtseException &e)
	{
		throw e;
	}
	*/
}

/**
 * 优化表数据实际操作
 *
 * @param session 会话
 * @param path 表路径，相对于basedir，大小写不敏感
 * @param keepDict 是否指定了保留旧字典文件(如果有的话)
 * @param cancelFlag 操作取消标志
 * @throw NtseException 加表锁超时或文件操作异常，在发生异常时，本函数保证原表数据不会被破坏
 */
void TNTDatabase::doOptimize(Session *session, const char *path, bool keepDict, 
						  bool *cancelFlag) throw(NtseException) {
	acquireDDLLock(session, IL_IX, __FILE__, __LINE__);
	TNTTable *table = NULL;
	TNTTransaction *trx = session->getTrans();
	TNTTransaction tempTrx;

	//如果表没有打开，则先打开表
	try {
		if (!trx) {
			//为了兼容非事务连接持有事务open table
			session->setTrans(&tempTrx);
		}
		table = openTable(session, path, false);
		if (!trx) {
			session->setTrans(NULL);
		}
	} catch (NtseException &e) {
		releaseDDLLock(session, IL_IX);
		throw e;
	}
	assert((NULL != table));

	bool metaLock = false;
	try {
		table->lockMeta(session, IL_U, m_db->getConfig()->m_tlTimeout * 1000, __FILE__, __LINE__);
		metaLock = true;

		bool newHasDict = false;
		table->optimize(session, keepDict, &newHasDict, cancelFlag);

		//更新控制文件
		m_db->alterCtrlFileHasCprsDic(session, table->getNtseTable()->getTableDef()->m_id, newHasDict);
	} catch (NtseException &e) {
		if (metaLock) {
			table->unlockMeta(session, IL_U);
		}
		closeTable(session, table);
		releaseDDLLock(session, IL_IX);
		throw e;
	}

	table->unlockMeta(session, IL_U);
	closeTable(session, table);
	releaseDDLLock(session, IL_IX);
}

/**
 * 取消后台正在运行的用户线程
 * @param thdId 数据库连接ID
 * @return 
 */
void TNTDatabase::cancelBgCustomThd(uint connId) throw(NtseException) {
	m_db->cancelBgCustomThd(connId);
}

/**	实现部分online操作
* @session			操作session
* @table			已打开的表
* @keepDict			是否保留原有压缩字典			
*/
void TNTDatabase::optimizeTable(Session *session, TNTTable *table, bool keepDict /*=false*/, bool waitForFinish /*=true*/) throw(NtseException) {
	// Optimize完全交由NTSE实现，因此先将Session中的事务移除
	UNREFERENCED_PARAMETER(session);
	BgCustomThreadManager *bgCustomThdsManager = m_db->getBgCustomThreadManager();
	if (waitForFinish) {
		OptimizeThread thd(this, bgCustomThdsManager, table, keepDict, false, session->getConnection()->isTrx());
		thd.start();
		thd.join();
	} else {
		//BgCustomThread对象由后台线程管理器负责回收
		BgCustomThread *thd = new OptimizeThread(this, bgCustomThdsManager, table, keepDict, true, session->getConnection()->isTrx());
		bgCustomThdsManager->startBgThd(thd);
	}
}

/**	等待表真正被关闭，这一函数用在需要对表关闭之后才能进行的DROP/RENAME等操作，这是由于
* 即使应用已经关闭了表，表仍然会被备份、检查点、Dump、Purge等任务暂时pin住没有真正关闭
* @session			操作session
* @path				关闭表的路径
* @timeoutSec		等待超时			
*/
void TNTDatabase::waitRealClosed(Session *session, const char *path, int timeoutSec) throw(NtseException) {
	u32 before = System::fastTime();
	bool first = true;
	bool realClose = false;
	// 最长等待2倍的timeoutSec时间
	while (true) {
		TNTTableInfo *tntTableInfo = m_pathToTables->get((char *)path);
		TableInfo	 *ntseTableInfo= m_db->getTableInfo((char *)path);
		
		if (!tntTableInfo && !ntseTableInfo) {
			// TNTTableInfo与NTSE TableInfo均不存在，表已被Real Close
			break;
		}

		if (first)
			m_tntSyslog->log(EL_WARN, "Table '%s'(tntref = %d && ntseref = %d) is used, wait and retry.", path, 
			!tntTableInfo? 0: tntTableInfo->m_refCnt, !ntseTableInfo? 0: ntseTableInfo->m_refCnt);

		if (timeoutSec == 0 || (timeoutSec > 0 && (u32)timeoutSec < (System::fastTime() - before))) {
			NTSE_THROW(NTSE_EC_LOCK_TIMEOUT, "waitRealClosed timeout");
		}
		
		releaseATSLock(session, IL_S);

		// 尝试获取ATS排他锁去主动关闭refCnt为0的表
		acquireATSLock(session, IL_X, -1, __FILE__, __LINE__);
		ntseTableInfo= m_db->getTableInfo((char *)path);
		if (ntseTableInfo) {	
			try {
				realClose = realCloseTableIfNeed(session, ntseTableInfo->m_table);
			} catch (NtseException &e) {
				// 关表抛错，一般是由于刷脏数据时发生I/O异常
				releaseATSLock(session, IL_X);
				m_db->getSyslog()->log(EL_PANIC, "CloseOpenTable Error: %s", e.getMessage());
			}
		}
		releaseATSLock(session, IL_X);
		if (!realClose)
			Thread::msleep(1000);
		first = false;
		acquireATSLock(session, IL_S, -1, __FILE__, __LINE__);
	}
}

/** 真正关闭一张表，并将表从Database的hash表中删除
*	@session		操作session
*	@tableInfo		TNTTableInfo结构
*	@ntseTblInfo	NTSE TableInfo结构
*	@flushDirty
*	@closeTnt		是否关闭TNTTable
*	@closeNtse		是否关闭Ntse Table
*/
void TNTDatabase::realCloseTable(Session *session, TNTTableInfo *tableInfo, TableInfo *ntseTblInfo, bool flushDirty, bool closeTnt, bool closeNtse) {
	u16		tableId;

	tableId = ntseTblInfo->m_table->getTableDef()->m_id;

	// 首先关闭TNT Table
	if (closeTnt) {
		m_pathToTables->remove(tableInfo->m_path);
		m_idToTables->remove(tableId);

		// 一期，实现close表时，不清空TNT内存
		tableInfo->m_table->close(session, false, false);
		
		if (tableInfo->m_table->getMRecords()->getHashIndex()->getSize() == 0) {
			TNTTableBase *tableBase = m_idToTabBase->remove(tableId);
			tableBase->close(session, false);
			tableBase->m_tableBaseLink.unLink();

			delete tableBase;
			tableBase = NULL;
		}

		delete tableInfo->m_table;
		tableInfo->m_table = NULL;
		delete tableInfo;
	}

	// 然后关闭NTSE Table
	if (closeNtse) {
		// NTSE realCloseTable会同时将 Table Meta Lock remove
		m_db->realCloseTable(session, ntseTblInfo, flushDirty);
	}
}


/** 关闭部分引用计数为0的表
*	@session		操作session
*/
void TNTDatabase::closeOpenTablesIfNeed() {
	Connection *conn = m_db->getConnection(false, "tnt:closeOpenTables");
	Session *session = m_db->getSessionManager()->allocSession("tnt::closeOpenTables", conn);
	DLink<Table *> *curr = NULL;
	bool listEnd = false;
	while(true) {
	// 获取ATS Lock
		acquireATSLock(session, IL_X, -1, __FILE__, __LINE__);
		int numTablesNeedClose = m_db->getOpenTableLRU()->getSize() - m_tntConfig->m_openTableCnt;
		if (numTablesNeedClose <= 0) {
			releaseATSLock(session, IL_X);
			break;
		}
		for (curr = m_db->getOpenTableLRU()->getHeader()->getNext();; curr = curr->getNext()) {
			// 判断是否扫描完整条链表
			if (curr == m_db->getOpenTableLRU()->getHeader()) {
					listEnd = true;
					break;
			}
			Table *table = curr->get();
			try {
				if (realCloseTableIfNeed(session, table))
					break;
			} catch (NtseException &e) {
				// 关表发生异常，说明在flush脏数据时发生I/O错误
				releaseATSLock(session, IL_X);
				m_db->getSyslog()->log(EL_PANIC, "CloseOpenTable Error: %s", e.getMessage());
			}
		}
		releaseATSLock(session, IL_X);
		// 如果链表扫描完毕，结束任务
		if (listEnd)
			break;

	}
	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(conn);
}


/** 根据TableBase获取所有的表Id，purge需要根据条件跳过小表来得到需要操作的表的集合
*	@ids						表Id集合
*	@skipSmallTables			是否跳过小表
*	@mheap					    true为根据内存堆页面数；false为根据hashIndex的大小
*   @limit                      判断是否跳表的标准值
*   @numSkipTables				[In/Out]跳过的表数目
*/
void TNTDatabase::getAllTableBaseIds(Session *session, vector<u16> *ids, bool skipSmallTables, bool mheap, u64 limit, uint* numSkipTables) {
	DLink<TNTTableBase *>	*tblBaseHeader = NULL;
	DLink<TNTTableBase *>	*tblBaseCur	   = NULL;
	TNTTableBase			*tableBase	   = NULL;
	bool					conditionSkip  = false;
	acquireATSLock(session, IL_S, -1, __FILE__, __LINE__);
	tblBaseHeader	= m_tntTabBases.getHeader();
	tblBaseCur		= tblBaseHeader;
	while ((tblBaseCur = tblBaseCur->getNext()) != tblBaseHeader) {
		tableBase = tblBaseCur->get();
		if (mheap) {
			conditionSkip = tableBase->m_records->getMHeapStat().m_total <= limit;
		} else {
			conditionSkip = tableBase->m_records->getHashIndex()->getSize(false) <= limit;
		}
		if (TableDef::INVALID_TABLEID != tableBase->m_tableId) {
			if(!skipSmallTables || !conditionSkip) {
				ids->push_back(tableBase->m_tableId);
			} else if (skipSmallTables) {
				assert(numSkipTables != NULL);
				*numSkipTables += 1;
			}
		}

	}
	releaseATSLock(session, IL_S);
}

/** 对指定表的内存堆记录flush至外存
 * @param session 会话
 * @param table 需要flush的table
 */
/*void TNTDatabase::flushMHeap(Session *session, TNTTable *table) {
	McSavepoint msp(session->getMemoryContext());
	u16 tableId = table->getNtseTable()->getTableDef()->m_id;
	TNTTransaction *prevTrx = session->getTrans();
	assert(prevTrx != NULL);
	assert(prevTrx->isTableLocked(tableId, TL_S) || prevTrx->isTableLocked(tableId, TL_X));
	TNTTransaction *trx = m_tranSys->allocTrx();
	trx->startTrxIfNotStarted(session->getConnection(), true);
	//因为此时已经加了S或者X的表锁，所以此时作用于该表的事务已经全部提交，
	//此时trx的assign readview必定能看到所有的最新记录
	trx->trxAssignReadView();
	trx->getReadView()->setUpTrxId(trx->getReadView()->getLowTrxId());
	session->setTrans(trx);

	writePurgeBeginLog(session, trx->getTrxId(), trx->getTrxLastLsn());
	try {
		purgeTNTTable(session, table, PT_PURGEPHASE2);
	} catch (NtseException &e) {
		trx->rollbackTrx(RBS_INNER, session);
		session->setTrans(prevTrx);
		m_tranSys->freeTrx(trx);
		throw e;
	}
	writePurgeEndLog(session, trx->getTrxId(), trx->getTrxLastLsn());

	assert(table->getMRecSize() == 0);

	trx->commitTrx(CS_PURGE);
	session->setTrans(prevTrx);
	m_tranSys->freeTrx(trx);
}*/

/** 对指定的TNTTable进行purge 
*	@param session	会话session
*	@param table 需要purge的table
*   @param purgeTarget purge的目标
*	@param minReadView purge的minReadView
*/
void TNTDatabase::purgeTNTTable(Session *session, TNTTable *table, PurgeTarget purgeTarget) {
	TNTTransaction *trx = session->getTrans();
	assert(trx != NULL);
	TrxId minReadView = trx->getReadView()->getUpTrxId();
	m_tntSyslog->log(EL_LOG, "begin purge Table id = %d, purgeReadView = %llu", table->getNtseTable()->getTableDef()->m_id, minReadView);
	LsnType lsn = table->writePurgePhase1(session, trx->getTrxId(), trx->getTrxLastLsn(), minReadView);
	purgePhaseOne(session, table);
						
	if (purgeTarget >= PT_PURGEPHASE2) {
		table->writePurgePhase2(session, trx->getTrxId(), trx->getTrxLastLsn());
		//删去等待活跃事务的流程，因为现在purge有ntse行锁保护，具体参见#104906->#7
		/*if (likely(m_dbStat == DB_RUNNING)) {
			purgeWaitTrx(trx->getTrxId());
		}*/
		SYNCHERE(SP_TNTDB_PURGE_PHASE2_BEFORE);
		purgePhaseTwo(session, table);
		lsn = table->writePurgeTableEnd(session, trx->getTrxId(), trx->getTrxLastLsn());
	}

}

/** TNT回收内存索引页面操作入口
*
*/
void TNTDatabase::reclaimMemIndex() throw(NtseException){
	vector<u16>				allIds;
	// 没有内存表直接退出
	if (m_tntTabBases.getSize() == 0)
		return;

	// 分配连接、会话
	Connection *conn = m_db->getConnection(true, "tnt:reclaimMemIndex");
	Session *session = m_db->getSessionManager()->allocSession("tnt::reclaimMemIndex", conn);
	TNTTransaction *trx = m_tranSys->allocTrx();
	trx->startTrxIfNotStarted(conn, true);
	session->setTrans(trx);

	// 获取所有的内存表
	uint numSkipTables;
	getAllTableBaseIds(session, &allIds, false, true, 0, &numSkipTables);
	
	uint tableCount = allIds.size();
	TNTTable *table = NULL;
	for (uint i = 0; i < tableCount; i++) {
		u16 tableId = allIds[i];
_ReStart:
		string path = m_ntseCtrlFile->getTablePath(tableId);
		if (path.empty()) {
			continue;
		}

		try {
			table = openTable(session, path.c_str());
		} catch (NtseException &e) {
			UNREFERENCED_PARAMETER(e);
			continue;
		}
		
		table->lockMeta(session, IL_S, -1, __FILE__, __LINE__);
		// 主要是为了解决通过tableId获取path，然后用path去open table。
		// 中间如果存在rename tableName1 to tableName2，然后create tableName1，此时tableName1对应的tableId已经不是当初的那个tableId了
		if (table->getNtseTable()->getTableDef()->m_id != tableId) {
			table->unlockMeta(session, IL_S);
			closeTable(session, table);
			goto _ReStart;
		}
		// 真正的索引回收操作
		table->reclaimMemIndex(session);

	
		table->unlockMeta(session, IL_S);
		closeTable(session, table);
	}

	trx->commitTrx(CS_INNER);
	m_tranSys->freeTrx(trx);
	trx = NULL;

	session->setTrans(NULL);
	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(conn);
}


/** PurgeAndDump全库操作主函数入口，TNT引擎其他模块调用
*	@neededPurgePhase	需要进行多少阶段的purge
*	@skipSmallTables	是否跳过小表
*	@log				是否要记日志
*/
void TNTDatabase::purgeAndDumpTntim(PurgeTarget purgeTarget, bool skipSmallTables, bool needDump, bool log) throw(NtseException) {
	TNTTable                *table         = NULL;
	Session                 *session       = NULL;
	TNTTransaction          *trx           = NULL;
	PurgeAndDumpProcess		purgeAndDumpProcess;
	vector<u16>				allIds;
	int						defragTarget   = MAX_DEFRAG_NUM;
	int						totalDefragPageNum = 0;
	
	//如果是包含dump操作，必须打开tableBase中所有的表
	if(needDump)
		assert(!skipSmallTables);

	// 1. 如果没有内存表则直接退出
	if (m_tntTabBases.getSize() == 0 && !needDump) {
		return;
	}

	// 2. 初始化purgeAndDump结构
	initPurgeAndDump(&purgeAndDumpProcess, &needDump, log);

	session = purgeAndDumpProcess.m_session;
	trx = purgeAndDumpProcess.m_purgeTrx;

	// 4. 写Purge开始日志
	if (purgeTarget >= PT_PURGEPHASE1)
		writePurgeBeginLog(session, trx->getTrxId(), trx->getTrxLastLsn());

	// 5. 遍历m_tntTabBases链表，purge其中的表
	uint numPurgeSkipTables	= 0;
	getAllTableBaseIds(session, &allIds, skipSmallTables, true, TABLE_PURGE_THRESHOLD, &numPurgeSkipTables);
	m_tntStatus.m_numPurgeSkipTable += numPurgeSkipTables;
	uint i = 0;
	u16 tableId = 0;
	uint tableCount = allIds.size();
	m_tntSyslog->log(EL_LOG, "==> begin PurgeAndDump TNTIM. Need purgeAndDump table count = %d", tableCount);
	if(needDump)
		m_tntSyslog->log(EL_LOG, "==> begin Dump TNTIM. DumpLSN = %llu",purgeAndDumpProcess.m_dumpLsn);
	for (i = 0; i < tableCount; i++) {
		tableId = allIds[i];
_ReStart:
		string path = m_ntseCtrlFile->getTablePath(tableId);
		if (path.empty()) {
			continue;
		}

		try {
			table = openTable(session, path.c_str());
		} catch (NtseException) {
			// 由于跳表，此次dump失败
			purgeAndDumpProcess.m_dumpSuccess = false;
			m_tntStatus.m_numPurgeSkipTable++;
			continue;
		}
		
		try {
			table->lockMeta(session, IL_S, 1000, __FILE__, __LINE__);
		} catch (NtseException) {
			closeTable(session, table);
			purgeAndDumpProcess.m_dumpSuccess = false;
			m_tntStatus.m_numPurgeSkipTable++;
			continue;
		}

		try {
			//主要是为了解决通过tableId获取path，然后用path去open table。
			//中间如果存在rename tableName1 to tableName2，然后create tableName1，此时tableName1对应的tableId已经不是当初的那个tableId了
			if (table->getNtseTable()->getTableDef()->m_id != tableId) {
				table->unlockMeta(session, IL_S);
				closeTable(session, table);
				goto _ReStart;
			}
			
			purgeAndDumpProcess.m_curTab = table;
#ifdef NTSE_UNIT_TEST
			//如果不需要purge那么直接跳到defrag/dump阶段
			if (purgeTarget == PT_DEFRAG)
				goto DEFRAG_TNT_IM;
			if (purgeTarget ==  PT_NONE)
				goto DUMP_TNT_IM;
#else
			assert(purgeTarget >= PT_PURGEPHASE1);
#endif
			//修改lockTable为tryLockTable，因为addIndex时，metaLock为U锁，table Lock为S锁，
			//如果此时purge，meta Lock为S锁，然后等待table的IX锁，这时addIndex如果需要将U锁升级为X锁，此时将产生死锁
			if (!trx->tryLockTable(TL_IX, tableId)) {
				// 由于跳表，此次dump失败
				purgeAndDumpProcess.m_dumpSuccess = false;
				m_tntStatus.m_numPurgeSkipTable++;
				goto DUMP_TNT_IM;
			}
			
			//绑定session和purge事务
			purgeAndDumpProcess.m_session->setTrans(purgeAndDumpProcess.m_purgeTrx);
			purgeAndDumpProcess.m_purgeTrx->trxAssignPurgeReadView();
			purgeAndDumpProcess.m_minTrxId = purgeAndDumpProcess.m_purgeTrx->getReadView()->getUpTrxId();

			purgeTNTTable(session, table, purgeTarget);
						
			if (purgeTarget >= PT_PURGEPHASE2) {
				purgeAndDumpProcess.m_tabPurged++;
			}
			
			// 必须在释放表锁前关闭readView，否则会因释放表锁重置事务ctx，导致readView内存被提前释放
			m_tranSys->closeReadViewForMysql(trx);
			
			trx->unlockTable(TL_IX, tableId);

			//Defrag TNT内存堆
#ifdef NTSE_UNIT_TEST
DEFRAG_TNT_IM:
#endif
			if(defragTarget > 0) {
				int defragPageNum = table->getMRecords()->freeSomePage(session, defragTarget);
				assert (defragPageNum <= defragTarget);
				totalDefragPageNum += defragPageNum;
				defragTarget -= defragPageNum;
			}
		} catch (NtseException &e) {
			if (tableId != TableDef::INVALID_TABLEID && trx->isTableLocked(tableId, TL_IX)) {
				trx->unlockTable(TL_IX, tableId);
			}

			if (table->getMetaLock(session) == IL_S) {
				table->unlockMeta(session, IL_S);
			}
			
			closeTable(session, table);
			deInitPurgeAndDump(&purgeAndDumpProcess, needDump);
			throw e;
		}
DUMP_TNT_IM:
		table->unlockMeta(session, IL_S);
		closeTable(session, table);
	}


	purgeAndDumpProcess.m_end = System::fastTime();
	u32 purgeLastTime = purgeAndDumpProcess.m_end - purgeAndDumpProcess.m_begin;

	// 6. 写purge完成日志
	if (purgeTarget >= PT_PURGEPHASE1)
		writePurgeEndLog(session, trx->getTrxId(), trx->getTrxLastLsn());
	
	// 7. 修改控制文件，记录dump信息，同时删除前一个dump备份
	if (needDump) 
		finishDumpTntim(&purgeAndDumpProcess);

	m_tntSyslog->log(EL_LOG, "==> End PurgeAndDump TNTIM && time = %d s", purgeLastTime);

	// 收集统计信息
	m_tntStatus.m_purgeLastTime = purgeLastTime;
	m_tntStatus.m_purgeTotalTime += purgeLastTime;
	if (purgeLastTime >= m_tntStatus.m_purgeMaxTime) {
		m_tntStatus.m_purgeMaxTime = purgeLastTime;
		m_tntStatus.m_purgeMaxBeginTime = purgeAndDumpProcess.m_begin;
	}

	// 8. 释放purgeAndDump结构体
	deInitPurgeAndDump(&purgeAndDumpProcess, needDump);

	
}

/** redo purge操作
 * @param log purge的最后一个日志
 */
void TNTDatabase::redoPurgeTntim(TrxId trxId, bool commit) throw (NtseException) {
	RecoverProcess *proc = m_activeTrxProcs->get(trxId);
	assert(proc != NULL);

	LogEntry *log = NULL;
	LogType logType = LOG_MAX;
	string		 path;
	TNTTable     *table     = NULL;
	Session      *session   = proc->m_session;
	TNTTransaction  *trx    = proc->m_trx;
	TrxId           minReadView = 0;
	LsnType         preLsn = 0;
	PurgeAndDumpProcess    purgeAndDumpProcess;
	while (!proc->m_logs->isEmpty()) {
		log = proc->m_logs->removeFirst()->get();
		assert(log != NULL);
		logType = log->m_logType;
		if (logType == TNT_BEGIN_PURGE_LOG) {
			parsePurgeBeginLog(log, &purgeAndDumpProcess.m_purgeTrxId, &preLsn);
			assert(purgeAndDumpProcess.m_purgeTrxId == trx->getTrxId());
			initRedoPurge(purgeAndDumpProcess, proc);
			continue;
		} else if (logType == TNT_PURGE_BEGIN_FIR_PHASE) { 
			getPurgeMinTrxIdPerTable(purgeAndDumpProcess.m_session, log, &minReadView);
			setRedoPurgeReadView(purgeAndDumpProcess, minReadView);
			continue;
		} else if (logType != TNT_PURGE_END_HEAP) {
			continue;
		} 

		assert(TNT_PURGE_END_HEAP == logType);
		try {
			table = getTNTTable(purgeAndDumpProcess.m_session, log->m_tableId);
			assert(table != NULL);
			purgePhaseTwo(session, table);
			m_tranSys->closeReadViewForMysql(purgeAndDumpProcess.m_purgeTrx);
		} catch (NtseException &e) {
			UNREFERENCED_PARAMETER(e);
			continue;
		}
	}

	if (commit) {
		makeupCommitTransaction(trxId, false);
		return;
	}

	assert(log != NULL && logType != LOG_MAX);
	proc->m_trx->enableLogging();
	if (logType == TNT_PURGE_BEGIN_FIR_PHASE || logType == TNT_PURGE_BEGIN_SEC_PHASE) {
		if (logType == TNT_PURGE_BEGIN_FIR_PHASE) {
			TNTTable::parsePurgePhase1(log, &trxId, &preLsn, &minReadView);
			setRedoPurgeReadView(purgeAndDumpProcess, minReadView);
		} else {
			TNTTable::parsePurgePhase2(log, &trxId, &preLsn);
		}

		try {
			table = getTNTTable(session, log->m_tableId);
			assert(table != NULL);
			purgeAndDumpProcess.m_curTab = table;
			if (log->m_logType == TNT_PURGE_BEGIN_FIR_PHASE) {
				purgePhaseOne(session, table);
				table->writePurgePhase2(session, trx->getTrxId(), trx->getTrxLastLsn());
			}
			purgePhaseTwo(session, table);
			m_tranSys->closeReadViewForMysql(purgeAndDumpProcess.m_purgeTrx);
			table->writePurgeTableEnd(session, trx->getTrxId(), trx->getTrxLastLsn());
		} catch (NtseException &e) {
			throw e;
		}
		purgeAndDumpProcess.m_tabPurged++;
		
		// 6. 写purge完成日志
		purgeAndDumpProcess.m_end = System::fastTime();
		writePurgeEndLog(session, trxId, trx->getTrxLastLsn());
	} else if (logType == TNT_PURGE_END_HEAP || logType == TNT_BEGIN_PURGE_LOG) {
		writePurgeEndLog(session, trxId, trx->getTrxLastLsn());
	} else {
		assert(logType == TNT_END_PURGE_LOG);
	}
	makeupCommitTransaction(trxId, true);
}

void TNTDatabase::getPurgeMinTrxIdPerTable(Session *session, LogEntry *log, TrxId *minReadView) {
	assert(log->m_logType == TNT_PURGE_BEGIN_FIR_PHASE);
	TrxId purgeTrxId = INVALID_TRX_ID;
	LsnType preFirstPurgeLsn = INVALID_LSN;
	TNTTable *table = getTNTTable(session, log->m_tableId);
	table->parsePurgePhase1(log, &purgeTrxId, &preFirstPurgeLsn, minReadView);
}


void TNTDatabase::initRedoPurge(PurgeAndDumpProcess &purgeAndDumpProcess, const RecoverProcess *proc) {
	purgeAndDumpProcess.m_begin = System::fastTime();
	purgeAndDumpProcess.m_purgeTrx = proc->m_trx;
	purgeAndDumpProcess.m_session = proc->m_session;
	purgeAndDumpProcess.m_conn = proc->m_session->getConnection();
}
/**	初始化PurgeAndDumpProcess结构
*	外部用户可以通过命令，执行此函数
*	@needDump[in/out]如果dump创建文件夹失败，则传出为false
*/
void TNTDatabase::initPurgeAndDump(PurgeAndDumpProcess *purgeAndDumpProcess, bool *needDump, bool log) throw(NtseException){
	Connection *conn = m_db->getConnection(false, "tnt:purgeAndDump");
	Session *session = m_db->getSessionManager()->allocSession("tnt::purgeAndDump", conn);

	purgeAndDumpProcess->m_begin = System::fastTime();
	purgeAndDumpProcess->m_purgeTrx = m_tranSys->allocTrx();
	if (!log) {
		purgeAndDumpProcess->m_purgeTrx->disableLogging();
	}
	purgeAndDumpProcess->m_purgeTrx->startTrxIfNotStarted(conn, true);
	
	purgeAndDumpProcess->m_conn	= conn;
	purgeAndDumpProcess->m_session = session;
	purgeAndDumpProcess->m_session->setTrans(purgeAndDumpProcess->m_purgeTrx);

	m_tntStatus.m_purgeCnt++;

	if (*needDump) {
		//为purge分配一个临时readView，只用于在没有其他readView时正确获取DumpLsn
		purgeAndDumpProcess->m_purgeTrx->trxAssignReadView();
		//获取dumpLSN
		LsnType logTailLsn = m_tntTxnLog->tailLsn();
		LsnType minTrxLsn = m_tranSys->getMinStartLsnFromReadViewList();
		if (minTrxLsn != INVALID_LSN) {
			purgeAndDumpProcess->m_dumpLsn = minTrxLsn;
		} else {
			purgeAndDumpProcess->m_dumpLsn = logTailLsn;
		}
		//关闭purge事务的临时readView
		m_tranSys->closeReadViewForMysql(purgeAndDumpProcess->m_purgeTrx);
		
		purgeAndDumpProcess->m_dumpSuccess = true;
	}
}
/**	释放PurgeAndDumpProcess结构中的dump相关元素
*
*/
void TNTDatabase::deInitDump(PurgeAndDumpProcess *purgeAndDumpProcess) {
	UNREFERENCED_PARAMETER(purgeAndDumpProcess);
	//需要回收超额的日志
	m_tntTxnLog->reclaimOverflowSpace();
}

/**	释放PurgeAndDumpProcess结构
*
*/
void TNTDatabase::deInitPurgeAndDump(PurgeAndDumpProcess *purgeAndDumpProcess, bool needDump) {
	// 释放purge结构
	purgeAndDumpProcess->m_purgeTrx->commitTrx(CS_PURGE);
	m_tranSys->freeTrx(purgeAndDumpProcess->m_purgeTrx);
	purgeAndDumpProcess->m_purgeTrx = NULL;

	//释放dump结构
	if(needDump) {
		deInitDump(purgeAndDumpProcess);
	}

	//释放session和连接
	Connection *conn = purgeAndDumpProcess->m_conn;
	Session *session = purgeAndDumpProcess->m_session;
	session->setTrans(NULL);
	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(conn);
}

void TNTDatabase::parsePurgeBeginLog(const LogEntry *log, TrxId *txnId, LsnType *preLsn/*, TrxId *minReadView*/) {
	Stream s(log->m_data, log->m_size);
	s.read(txnId);
	s.read(preLsn);
}

LsnType TNTDatabase::writePurgeBeginLog(Session *session, TrxId trxId, LsnType preLsn/*, TrxId minReadView*/) {
	MemoryContext *ctx = session->getMemoryContext();
	McSavepoint msp(ctx);
	size_t size = sizeof(trxId) + sizeof(preLsn);
	byte *buf = (byte *)ctx->alloc(size);
	Stream s(buf, size);
	s.write(trxId);
	s.write(preLsn);
	return session->getTrans()->writeTNTLog(TNT_BEGIN_PURGE_LOG, TableDef::INVALID_TABLEID, buf, s.getSize());
}

LsnType TNTDatabase::writePurgeEndLog(Session *session, TrxId trxId, LsnType preLsn) {
	MemoryContext *ctx = session->getMemoryContext();
	McSavepoint msp(ctx);
	size_t size = sizeof(trxId) + sizeof(preLsn);
	byte *buf = (byte *)ctx->alloc(size);
	Stream s(buf, size);
	s.write(trxId);
	s.write(preLsn);
	return session->getTrans()->writeTNTLog(TNT_END_PURGE_LOG, TableDef::INVALID_TABLEID, buf, s.getSize());
}

/**	Purge操作
*	
*
*/
void TNTDatabase::doPurge() throw(NtseException) {

}

/**
*
*
*/
void TNTDatabase::finishingPurgeAndLock() throw(NtseException) {

}

/**
*
*
*/
void TNTDatabase::donePurge() {

}

bool TNTDatabase::purgeJudgeNeed() {
#ifdef NTSE_UNIT_TEST
	return false;
#else
	assert(m_tntConfig->m_purgeThreshold < 100 && m_tntConfig->m_purgeThreshold > 0);

	//m_tntBufSize为页面数
	uint freePageLimit = ((100 - m_tntConfig->m_purgeThreshold)*m_tntConfig->m_tntBufSize)/100;
	//当pagePool借了页面之后，targetSize可能会小于当前的实际size
	uint currentFreePage = 0;
	if(m_TNTIMpageManager->getTargetSize() < m_TNTIMpageManager->getCurrentSize(false))
		currentFreePage = 0;
	else
		currentFreePage = m_TNTIMpageManager->getTargetSize() - m_TNTIMpageManager->getCurrentSize(false);

	/*
	FILE *fp;
	fp = fopen("purge_judge_need.txt", "a+");

	fprintf(fp, "\n");
	fprintf(fp, "Purge Threshold: %d\n", m_tntConfig->m_purgeThreshold);
	fprintf(fp, "TNT Buffer Size: %d\n", m_tntConfig->m_tntBufSize);
	fprintf(fp, "TNT Target Size: %d\n", m_TNTIMpageManager->getTargetSize());
	fprintf(fp, "TNT Current Size: %d\n", m_TNTIMpageManager->getCurrentSize(false));
	fprintf(fp, "Free page Limit: %d\n", freePageLimit);
	fprintf(fp, "Current Free Page: %d\n", currentFreePage);

	fclose(fp);
	*/

	if (currentFreePage <= freePageLimit) {
		return true;
	} else {
		return false;
	}
#endif
}


void TNTDatabase::setRedoPurgeReadView(PurgeAndDumpProcess &purgeAndDumpProcess, TrxId minTrxId) {
	purgeAndDumpProcess.m_purgeTrx->trxAssignReadView();
	ReadView *readView = purgeAndDumpProcess.m_purgeTrx->getReadView();
	purgeAndDumpProcess.m_minTrxId = minTrxId;
	readView->setLowTrxId(minTrxId);
	readView->setUpTrxId(minTrxId);
}

/**	purge操作第一阶段，一期TNT中，由底层完成purge table完整操作，Database层面无法感知
*
*
*/
void TNTDatabase::purgePhaseOne(Session *session, TNTTable *table) {
	table->purgePhase1(session, session->getTrans());
}

/**	purge操作第一阶段结束，第二阶段开始前，需要等待当前表上的全表扫描，取到下一条记录
*	一期TNT，此函数由TNT Mrecords层面调用
*/
void TNTDatabase::purgeWaitTrx(TrxId purgeTrxId) throw(NtseException) {
	std::vector<TrxId> trxIds;
	m_tranSys->getActiveTrxIds(&trxIds);
	u32 size = trxIds.size();
	assert(size > 0);//这是因为purge的事务在当时肯定是活跃的
	if (size == 1) {
		return;
	}
	Thread::msleep(1*1000);
	for (uint i = 0; i < size; i++) {
		while (trxIds[i] != purgeTrxId && m_tranSys->isTrxActive(trxIds[i])) {
			Thread::msleep(500);
			SYNCHERE(SP_TNTDB_PURGEWAITTRX_WAIT);
		}
	}
}

/**	purge操作第二阶段，一期TNT中，由底层完成purge table完整操作，Database层面无法感知
*
*
*/
void TNTDatabase::purgePhaseTwo(Session *session, TNTTable *table) throw(NtseException) {
	u32 beginTime = System::fastTime();
	u64 ret = table->purgePhase2(session, session->getTrans());
	m_tntSyslog->log(EL_LOG, "purge table(%s) Phase2 and erase "I64FORMAT"u record from memHeap and time is %d", 
		table->getNtseTable()->getPath(), ret, System::fastTime() - beginTime);
}

/**	purge操作第二阶段，删除heap记录
*
*
*/
void TNTDatabase::purgeTntimHeap(TNTTable *table) {
	UNREFERENCED_PARAMETER(table);
}

/**	purge操作第二阶段，删除Indice记录
*
*
*/
void TNTDatabase::purgeTntimIndice(TNTTable *table) {
	UNREFERENCED_PARAMETER(table);
}


/**	Dump函数，命令提供给外层调用
*
*/
void TNTDatabase::doDump() throw(NtseException) {

}

/**	Dump函数，命令提供给外层调用
*
*/
void TNTDatabase::finishingDumpAndLock() throw(NtseException) {

}

/**	Dump函数，命令提供给外层调用
*
*/
void TNTDatabase::doneDump() {

}

/*********************************************************/
/* Dump操作内部函数                                      */
/*********************************************************/

/**	判断当前是否需要dump
*	定期触发的dump，可能并不需要
*/
bool TNTDatabase::dumpJudgeNeed() {
#ifdef NTSE_UNIT_TEST
	return false;
#else
	// 一期TNT实现中，默认为true，后续根据实际情况，调整此函数
	u64 delta = m_tntTxnLog->tailLsn() - m_tntCtrlFile->getDumpLsn();
	u64 limit = m_tntConfig->m_dumponRedoSize;//m_dumponRedoSize以bytes为单位
	if (delta >= limit) {
		return true;
	} else {
		return false;
	}
#endif
}

/**	Dump一张TNT内存表
*	@firstTable	是否为第一个dump的表，此时需要创建dump目录
*/
void TNTDatabase::dumpTableHeap(PurgeAndDumpProcess *purgeAndDumpProcess) throw(NtseException) {
	UNREFERENCED_PARAMETER(purgeAndDumpProcess);
/*	TNTTable *table	= purgeAndDumpProcess->m_curTab;
	TableDef *tableDef = table->getNtseTable()->getTableDef();

	u64	errCode, offset = 0;

	// 1. 打开准备dump的表对应的文件
	string dumpFile	= generateDumpFilePath(purgeAndDumpProcess->m_dumpPath, purgeAndDumpProcess->m_dumpSN, tableDef->m_id);
	File *file = new File(dumpFile.c_str());
	if ((errCode = file->create(false, false)) != File::E_NO_ERROR) {
		delete file;
		NTSE_THROW(errCode, "Can not create file %s", dumpFile.c_str());
	}

	// 2. 调用MRecords接口，备份数据
	try {
		table->getMRecords()->dump(purgeAndDumpProcess->m_session, purgeAndDumpProcess->m_dumpTrx->getReadView(), file, &offset);
	} catch (NtseException &e) {
		file->close();
		delete file;
		throw e;
	}

	file->close();
	delete file;
*/
}


bool TNTDatabase::masterJudgeNeed() {
#ifdef NTSE_UNIT_TEST
	return false;
#else
	return false;
#endif
}

void TNTDatabase::freeMem(const char *file, uint line) {
	UNREFERENCED_PARAMETER(line);
	if (unlikely(file != NULL)) {
		m_tntStatus.m_freeMemCallCnt++;
	}
}

/** 初始化整理hash索引项操作
 * @param 整理hash索引过程句柄
 */
void TNTDatabase::initDefragHashIndex(DefragHashIndexProcess *defragHashIndexProcess) throw(NtseException){
	Connection *conn = m_db->getConnection(true, "tnt:defragHashIndex");
	Session *session = m_db->getSessionManager()->allocSession("tnt::defragHashIndex", conn);

	defragHashIndexProcess->m_begin = System::fastTime();

	defragHashIndexProcess->m_trx = m_tranSys->allocTrx();
	defragHashIndexProcess->m_trx->startTrxIfNotStarted(conn, true);
	//必须需要设置readview，为了findMinReadViewInActiveTrxs找到minReadView
	defragHashIndexProcess->m_trx->trxAssignReadView();

	defragHashIndexProcess->m_conn	= conn;
	defragHashIndexProcess->m_session = session;
	defragHashIndexProcess->m_session->setTrans(defragHashIndexProcess->m_trx);
}

/** 释放整理hash索引项所占用的资源
 * @param 整理hash索引过程句柄
 */
void TNTDatabase::deInitDefragHashIndex(DefragHashIndexProcess *defragHashIndexProcess) {
	defragHashIndexProcess->m_trx->commitTrx(CS_DEFRAG_HASHINDEX);
	m_tranSys->freeTrx(defragHashIndexProcess->m_trx);
	defragHashIndexProcess->m_trx = NULL;

	Connection *conn = defragHashIndexProcess->m_conn;
	Session *session = defragHashIndexProcess->m_session;
	session->setTrans(NULL);
	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(conn);
}

//回收hash索引
uint TNTDatabase::defragHashIndex(bool skipSmallTable) throw(NtseException) {
	u32						total = 0;
	vector<u16>				allIds;
	TNTTable                *table = NULL;
	DefragHashIndexProcess  defragHashIndexProcess;

	// 1.脏读，如果没有内存表则直接退出
	if (m_tntTabBases.getSize() == 0) {
		return 0;
	}
	
	// Defrag HashIndex统计信息
	m_tntStatus.m_defragHashCnt++;

	// 2.初始化defrag hash index结构
	initDefragHashIndex(&defragHashIndexProcess);

	TrxId minReadView = m_tranSys->findMinReadViewInActiveTrxs();
	assert(minReadView != INVALID_TRX_ID);
	Session *session = defragHashIndexProcess.m_session;
	// 获取到minReadView后可释放readView
	m_tranSys->closeReadViewForMysql(session->getTrans());
	// 3. 遍历TNT table，并进行defrag Index
	uint numDefragSkipTables = 0;
	getAllTableBaseIds(session, &allIds, skipSmallTable, false, TABLE_DEFRAG_HASH_INDEX_THRESHOLD, &numDefragSkipTables);
	m_tntStatus.m_numDefragHashSkipTable += numDefragSkipTables;

	uint i = 0;
	uint tableCount = allIds.size();
	m_tntSyslog->log(EL_DEBUG, "==> Begin Defrag HashIndex. Need defrag hashindex table count = %d", tableCount);
	for (i = 0; i < tableCount; i++) {
		u16 tableId = allIds[i];
_ReStart:
		string path = m_ntseCtrlFile->getTablePath(tableId);
		if (path.empty()) {
			continue;
		}

		try {
			table = openTable(session, path.c_str());
		} catch (NtseException) {
			continue;
		}

		try {
			table->lockMeta(session, IL_S, 1000, __FILE__, __LINE__);
		} catch (NtseException) {
			closeTable(session, table);
			continue;
		}
		//主要是为了解决通过tableId获取path，然后用path去open table。
		//中间如果存在rename tableName1 to tableName2，然后create tableName1，此时tableName1对应的tableId已经不是当初的那个tableId了
		if (table->getNtseTable()->getTableDef()->m_id != tableId) {
			table->unlockMeta(session, IL_S);
			closeTable(session, table);
			goto _ReStart;
		}

		total += table->getMRecords()->defragHashIndex(session, minReadView, m_tntConfig->m_defragHashIndexMaxTraverseCnt);
		table->unlockMeta(session, IL_S);
		closeTable(session, table);
	}

	// 4 设置defrag hashIndex结束时间
	defragHashIndexProcess.m_end = System::fastTime();

	m_tntSyslog->log(EL_DEBUG, "defrag hashIndex the total is %d and time is %d s", total, defragHashIndexProcess.m_end - defragHashIndexProcess.m_begin);

	// 6. defrag hashIndex结束，释放资源
	deInitDefragHashIndex(&defragHashIndexProcess);
	return total;
}

// 初始化reclaimLob
void TNTDatabase::initReclaimLob(ReclaimLobProcess *reclaimLobProcess) throw(NtseException){
	Connection *conn = m_db->getConnection(false, "tnt:reclaimLob");
	Session *session = m_db->getSessionManager()->allocSession("tnt::reclaimLob", conn);
	reclaimLobProcess->m_begin = System::fastTime();
	
	// 临时分配一个事务用于获取此时系统的minReadview
	TNTTransaction *trx	 = m_tranSys->allocTrx();
	trx->startTrxIfNotStarted(conn, true);
	trx->trxAssignReadView();
	reclaimLobProcess->m_minReadView = m_tranSys->findMinReadViewInActiveTrxs();
	assert(reclaimLobProcess->m_minReadView != INVALID_TRX_ID);
	trx->commitTrx(CS_RECLAIMLOB);
	m_tranSys->freeTrx(trx);

	reclaimLobProcess->m_conn	= conn;
	reclaimLobProcess->m_session = session;
}

// 析构reclaimLob
void TNTDatabase::deInitReclaimLob(ReclaimLobProcess *reclaimLobProcess) {
	Connection *conn = reclaimLobProcess->m_conn;
	Session *session = reclaimLobProcess->m_session;
	
	m_db->getSessionManager()->freeSession(session);
	reclaimLobProcess->m_session = NULL;
	m_db->freeConnection(conn);
	reclaimLobProcess->m_conn = NULL;
}

/** 回收版本池中第poolIndex数据表中的大对象
 * @param poolIndex 版本池中的表序号
 */
void TNTDatabase::reclaimLob() throw (NtseException){
	u8 poolIdx = 0;
	u8 poolCnt = 0;
	m_tntCtrlFile->lockCFMutex();
	poolIdx = m_tntCtrlFile->getReclaimedVerPool();
	poolCnt = m_tntCtrlFile->getVersionPoolCnt();
	if (poolIdx == INVALID_VERSION_POOL_INDEX || poolIdx == poolCnt - 1) {
		poolIdx = 0;
	} else {
		poolIdx++;
	}

	//如果该本版本池表还处于活跃状态，则不能回收
	if (m_tntCtrlFile->getVerPoolStatus(poolIdx) == VP_ACTIVE) {
		m_tntCtrlFile->unlockCFMutex();
		return;
	}
	
	m_tntCtrlFile->unlockCFMutex();

	ReclaimLobProcess reclaimLobProcess;
	//初始化回收大对象,如果分配事务失败，该步骤会抛出异常
	initReclaimLob(&reclaimLobProcess);

	m_tntCtrlFile->lockCFMutex();
	assert(m_tntCtrlFile->getVerPoolStatus(poolIdx) == VP_USED);
	//只有minReadView不小于该版本池的maxTrxId时才需要回收
	if (m_tntCtrlFile->getVerPoolMaxTrxId(poolIdx) >= reclaimLobProcess.m_minReadView) {
		m_tntCtrlFile->unlockCFMutex();
		deInitReclaimLob(&reclaimLobProcess);
		return;
	}
	m_tntSyslog->log(EL_LOG, "==> Begin Reclaim Lob");

	m_tntStatus.m_reclaimVersionPoolCnt++;
	// 记录版本池回收开始时间
	m_tntStatus.m_reclaimVerPoolLastBeginTime = System::fastTime();

	//再次检测控制文件并设置新状态
	//FIXME: 目前只考虑单线程reclaim
	assert(m_tntCtrlFile->getVerPoolStatus(poolIdx) == VP_USED);
	m_tntCtrlFile->writeBeginReclaimPool(poolIdx);
	m_tntCtrlFile->unlockCFMutex();

	reclaimLobProcess.m_begin = System::fastTime();
	//开始回收版本池
	m_vPool->defrag(reclaimLobProcess.m_session, poolIdx);
	reclaimLobProcess.m_end = System::fastTime();


	//设置tnt ctrl file状态
	m_tntCtrlFile->lockCFMutex();
	m_tntCtrlFile->writeLastReclaimedPool(poolIdx);
	m_tntCtrlFile->unlockCFMutex();

	m_tntSyslog->log(EL_LOG, "==> End Reclaim Lob and time is %d s", reclaimLobProcess.m_end - reclaimLobProcess.m_begin);
	//释放回收大对象结构
	deInitReclaimLob(&reclaimLobProcess);
	// 记录版本池回收时间
	m_tntStatus.m_reclaimVerPoolLastEndTime = System::fastTime();
}

/** redo回收版本池大对象
 * @param reclaimLobTrxId 回收版本池大对象的事务id
 */
void TNTDatabase::redoReclaimLob() {	
	// 从控制文件读取上次回收的版本池
	m_tntCtrlFile->lockCFMutex();
	uint poolCnt = m_tntCtrlFile->getVersionPoolCnt();
	u8 poolIdx = m_tntCtrlFile->getReclaimedVerPool();
	// 得到可能需要回收的版本池号
	poolIdx = (poolIdx == (poolCnt - 1)) ? 0 : (poolIdx + 1);

	VerpoolStat vpStat = m_tntCtrlFile->getVerPoolStatus(poolIdx);
	m_tntCtrlFile->unlockCFMutex();
	// 如果崩溃时正在做回收,直接truncate即可
	if(vpStat == VP_RECLAIMING) {	
		// 不分配事务，不回收大对象，只对版本池表做truncate操作
		Connection *conn = m_db->getConnection(true, __FUNC__);
		Session *session = m_db->getSessionManager()->allocSession(__FUNC__, conn);
		m_vPool->defrag(session, poolIdx, true);	
		m_db->getSessionManager()->freeSession(session);
		m_db->freeConnection(conn);

		// 修改控制文件
		m_tntCtrlFile->lockCFMutex();
		m_tntCtrlFile->writeLastReclaimedPool(poolIdx);
		m_tntCtrlFile->unlockCFMutex();
	}
}

/**	判断TNT数据库是否需要做恢复
*	@return true：需要恢复；false：不需要恢复
*
*/
bool TNTDatabase::isTntShouldRecover() {
	return true;
}

/**	等待后台线程
*	@all	需要等待所有后台线程初始化完毕
*	
*/
void TNTDatabase::waitBgTasks(bool all) {
	UNREFERENCED_PARAMETER(all);
}

/**	真正关闭TNT内存基础表
*
*
*/
void TNTDatabase::closeTNTTableBases() {
	DLink<TNTTableBase *>	*tblBaseCur	  = NULL;
	TNTTableBase			*tableBase	  = NULL;
	Connection				*conn		  = m_db->getConnection(true, __FUNC__);
	Session					*session	  = m_db->getSessionManager()->allocSession(__FUNC__, conn);

	// 并发控制，此处需要加上ATS Lock
	acquireATSLock(session, IL_X, -1, __FILE__, __LINE__);

	assert(m_pathToTables->getSize() == 0 && m_idToTables->getSize() == 0);

	// 遍历TNT内存表并dump
	while ((tblBaseCur = m_tntTabBases.removeLast()) != NULL) {
		tableBase = tblBaseCur->get();
		// 释放当前节点
		m_idToTabBase->remove(tableBase->m_tableId);
		tableBase->close(session, false);
		
		delete tableBase;
		tableBase = NULL;
	}

	// 关闭完成之后，TableBase链表，Hash表均为空
	assert(m_idToTabBase->getSize() == 0);
	assert(m_tntTabBases.getSize() == 0);

	releaseATSLock(session, IL_X);
	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(conn);
}

/**	关闭所有打开的内存表
*
*/
void TNTDatabase::closeOpenTables(bool closeNtse) {
	Connection *conn = m_db->getConnection(true, __FUNC__);
	Session *session = m_db->getSessionManager()->allocSession(__FUNC__, conn);

	acquireATSLock(session, IL_X, -1, __FILE__, __LINE__);

	// 将TNT的TableInfo移动到数组中，便于遍历
	size_t numTables = m_idToTables->getSize();
	TNTTableInfo **tables = new TNTTableInfo*[numTables];
	m_idToTables->values(tables);

	for (size_t i = 0; i < numTables; i++) {
		Table *ntseTable = tables[i]->m_table->getNtseTable();
		tables[i]->m_table->lockMeta(session, IL_X, -1, __FILE__, __LINE__);

		u16 tableId = ntseTable->getTableDef()->m_id;
		TableInfo *ntseTableInfo = m_db->getTableInfoById(tableId);

		realCloseTable(session, tables[i], ntseTableInfo, true, true, closeNtse);

		if (!closeNtse) {
			TNTTable::unlockMetaWithoutInst(session, ntseTable, IL_X);
		}
	}

	delete [] tables;

	releaseATSLock(session, IL_X);

	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(conn);	
}

/**
*
*
*/
u16  TNTDatabase::getNtseTableId() {
	return 1;
}

/** 加活跃表集合锁
 * @param session 会话
 * @param mode 模式，只能为IL_S或IL_X
 * @param timeoutMs <0表示不超时，0在不能立即得到锁时马上超时，>0为毫秒为单位的超时时间
 * @param file 代码文件
 * @param line 代码行号
 * @throw NtseException 加锁超时，如果指定timeousMs为<0不会抛出异常
 */
void TNTDatabase::acquireATSLock(Session *session, ILMode mode, int timeoutMs, const char *file, uint line) throw(NtseException) {
	assert(mode == IL_S || mode == IL_X);
	ftrace(ts.ddl || ts.dml, tout << session << mode << timeoutMs);

	IntentionLock *atsLock = m_db->getAtsLockInst();
	if (!atsLock->lock(session->getId(), mode, timeoutMs, file, line)) {
		nftrace(ts.ddl | ts.dml, tout << "Lock failed");
		NTSE_THROW(NTSE_EC_LOCK_TIMEOUT, "Unable to acquire ATS lock.");
	}
	session->addLock(atsLock);
}

/** 释放活跃表集合锁
 * @param session 会话
 * @param mode 模式，只能为IL_S或IL_X
 */
void TNTDatabase::releaseATSLock(Session *session, ILMode mode) {
	assert(mode == IL_S || mode == IL_X);
	ftrace(ts.ddl || ts.dml, tout << session << mode);

	IntentionLock *atsLock = m_db->getAtsLockInst();
	atsLock->unlock(session->getId(), mode);
	session->removeLock(atsLock);
}


/** 得到指定会话所加的活跃表集合锁模式
 * @param session 会话
 * @return 指定会话所加的活跃表集合锁模式，可能是IL_NO、IL_S或IL_X
 */
ILMode TNTDatabase::getATSLock(Session *session) const {
	IntentionLock	*atsLock = m_db->getAtsLockInst();
	return atsLock->getLock(session->getId());
}

/** 加DDL锁。CREATE/DROP/RENAME操作时加X锁，TRUNCATE/ADD INDEX等修改表结构的操作时需要加
 * IX锁，在线备份时需要加S锁，从而防止在备份期间进行任何DDL操作
 *
 * @param session 会话
 * @param mode 锁模式
 * @throw NtseException 加锁超时，超时时间由数据库配置中的m_tlTimeout指定
 */
void TNTDatabase::acquireDDLLock(Session *session, ILMode mode, const char *file, uint line) throw(NtseException) {
	ftrace(ts.ddl || ts.dml, tout << session << mode);

	IntentionLock *ddlLock = m_db->getDdlLockInst();
	if (!ddlLock->lock(session->getId(), mode, m_tntConfig->m_ntseConfig.m_tlTimeout * 1000, file, line))
		NTSE_THROW(NTSE_EC_LOCK_TIMEOUT, "Unable to acquire DDL lock in %s mode.", IntentionLock::getLockStr(mode));
	session->addLock(ddlLock);
}

/**
 * 释放DDL锁
 *
 * @param session 会话
 * @param mode 锁模式
 */
void TNTDatabase::releaseDDLLock(Session *session, ILMode mode) {
	ftrace(ts.ddl || ts.dml, tout << session << mode);

	IntentionLock *ddlLock = m_db->getDdlLockInst();
	ddlLock->unlock(session->getId(), mode);
	session->removeLock(ddlLock);
}

void TNTDatabase::dumpRecoverApplyLog() throw(NtseException) {

}

// 验证是否有需要删除的内存表(Ntse恢复过程中，ControlFile删除的表)
bool TNTDatabase::dumpRecoverVerifyHeaps() {
	return true;
}

// Tntim heap恢复完成之后(包括redo/undo),根据表定义，build内存索引
void TNTDatabase::dumpRecoverBuildIndice() throw(NtseException) {
	vector<u16> allIds; 
	Connection *conn = m_db->getConnection(false, "tnt:rebuildIndex");
	Session *session = m_db->getSessionManager()->allocSession("tnt::rebuildIndex", conn);
	//新分配一个事务
	TNTTransaction *trx = m_tranSys->allocTrx();
	trx->startTrxIfNotStarted(conn);
	session->setTrans(trx);
	//遍历获取所有表
	getAllTableBaseIds(session, &allIds, false, false, 0);
	uint tableCount = allIds.size();
	for (uint i = 0; i < tableCount; i++) {
		u16 tableId = allIds[i];

		string path = m_ntseCtrlFile->getTablePath(tableId);
		if(path.empty()) {
			continue;
		}

		TNTTable *table = getTNTTable(session, tableId);
		table->buildMemIndexs(session, table->getIndice()->getMemIndice());
	}
	trx->commitTrx(CS_INNER);
	m_tranSys->freeTrx(trx);
	session->setTrans(NULL);
	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(conn);
}

void TNTDatabase::dumpDoneRecover() {

}

void TNTDatabase::dumpEndRecover(bool succ) {
	UNREFERENCED_PARAMETER(succ);
}

// crash recovery结束，将DumpRecover中处于prepare状态的事务copy到TransactionSys中
void TNTDatabase::copyPreparedTrxsToTransSys() {

}

/*void TNTDatabase::releaseDumpRecover() {
	delete m_dumpRecover;
	m_dumpRecover = NULL;
}

void TNTDatabase::initDumpRecover() {
	m_dumpRecover = new DumpRecover();
}*/

void TNTDatabase::readDump() throw(NtseException) {
	uint dumpSN = 0;// m_tntCtrlFile->getDumpSN();
	if (dumpSN == 0) {
		return;
	}

	string dumpDir = generateDumpSNPath(m_tntConfig->m_dumpdir, dumpSN);
	File dir(dumpDir.c_str());
	bool exist = true;
	u64 errCode = dir.isExist(&exist);
	assert(File::getNtseError(errCode) == File::E_NO_ERROR);
	if (!exist) {
		NTSE_THROW(NTSE_EC_FILE_NOT_EXIST, "Can not open dump dir %s", dumpDir.c_str());
	}

	Connection *conn = m_db->getConnection(true);
	Session *session = m_db->getSessionManager()->allocSession("TNTDatabase::readDump", conn);
	TNTTransaction *trx = m_tranSys->allocTrx();
	trx->disableLogging();
	trx->startTrxIfNotStarted(conn);
	session->setTrans(trx);

	u16 numTables = m_db->getControlFile()->getNumTables();
	u16 *tableIds = new u16[numTables];
	m_db->getControlFile()->listAllTables(tableIds, numTables);
	for (u16 i = 0; i < numTables; i++) {
		string dumpTablePath = generateDumpFilePath(m_tntConfig->m_dumpdir, dumpSN, tableIds[i]);
		AutoPtr<File> file(new File(dumpTablePath.c_str()));
		u64 offset = 0;
		errCode = file->open(false);
		if (File::getNtseError(errCode) == File::E_NOT_EXIST) {
			continue;
		} else if (File::getNtseError(errCode) != File::E_NO_ERROR) {
			file->close();
			trx->commitTrx(CS_RECOVER);
			m_tranSys->freeTrx(trx);
			delete [] tableIds;
			m_db->getSessionManager()->freeSession(session);
			m_db->freeConnection(conn);
			NTSE_THROW(errCode, "Can not open file %s", dumpTablePath.c_str());
		}
		
		string tablePath = m_db->getControlFile()->getTablePath(tableIds[i]);
		TNTTable *table = NULL;
		try {
			table = openTable(session, tablePath.c_str());
		} catch (NtseException &) {
			file->close();
			continue;
		}
		table->getMRecords()->readDump(session, file, &offset);
		//TODO 可以考虑不关闭，这样做tnt redo时就没必要再打开了
		closeTable(session, table);
		file->close();
	}

	trx->commitTrx(CS_RECOVER);
	m_tranSys->freeTrx(trx);
 
	delete [] tableIds;
	session->setTrans(NULL);
	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(conn);
}

void TNTDatabase::getNextPage() throw(NtseException) {

}

void TNTDatabase::purgeCompleteBeforeDDL(Session *session, TNTTable *table) throw (NtseException) {
	UNREFERENCED_PARAMETER(session);
	UNREFERENCED_PARAMETER(table);
	return;
}

/**
 * 获取回滚事务表中开始回滚最早的事务的begin rollback回滚lsn
 *
 * @return  最早开始回滚的lsn
 */
LsnType TNTDatabase::getFirstUnfinishedRollbackLsn(){
	size_t rollbackHashSize = m_rollbackTrx->getSize();
	if(rollbackHashSize == 0)
		return INVALID_LSN;
	TrxId  *key = new TrxId[rollbackHashSize];
	RecoverProcess  **value = new RecoverProcess*[rollbackHashSize];
	LsnType firstRollbackLsn = INVALID_LSN;
	m_rollbackTrx->elements(key, value);
	for(size_t rollbackTrxCount = 0; rollbackTrxCount < rollbackHashSize; rollbackTrxCount++){
		if(firstRollbackLsn > value[rollbackTrxCount]->m_trx->getBeginRollbackLsn()){
			firstRollbackLsn = value[rollbackTrxCount]->m_trx->getBeginRollbackLsn();
		}
	}
	delete[] key;
	delete[] value;
	return firstRollbackLsn;
}

/**
 * 汇报运行状态信息
 *
 * @param status 输出参数
 */
void TNTDatabase::reportStatus(TNTGlobalStatus *status) {
	assert(!m_closed);

	status->m_pageBufSize	= m_TNTIMpageManager->getCurrentSize(true);
	status->m_freeBufSize	= m_TNTIMpageManager->getCurrentSize(true) - m_TNTIMpageManager->getCurrentSize(false);

	status->m_purgeCnt = m_tntStatus.m_purgeCnt;
	status->m_numPurgeSkipTable = m_tntStatus.m_numPurgeSkipTable;
	status->m_purgeMaxTime = m_tntStatus.m_purgeMaxTime;
	status->m_purgeAvgTime = m_tntStatus.m_purgeTotalTime / (m_tntStatus.m_purgeCnt + 1);
	status->m_purgeLastTime = m_tntStatus.m_purgeLastTime;

	// 如果没有做过purge，则不打印时间
	memset(status->m_purgeMaxBeginTime, 0, 30);
	time_t purgeMaxBeginTime = (time_t)(m_tntStatus.m_purgeMaxBeginTime);
	if (status->m_purgeCnt != 0)
		System::formatTime(status->m_purgeMaxBeginTime, sizeof(status->m_purgeMaxBeginTime), &purgeMaxBeginTime);
	else
		strcpy(status->m_purgeMaxBeginTime, "NULL");

	status->m_defragHashCnt = m_tntStatus.m_defragHashCnt;
	status->m_numDefragHashSkipTable = m_tntStatus.m_numDefragHashSkipTable;
	status->m_dumpCnt = m_tntStatus.m_dumpCnt;
	status->m_openTableCnt = m_tntStatus.m_openTableCnt;		
	status->m_closeTableCnt = m_tntStatus.m_closeTableCnt;	
	status->m_freeMemCallCnt = m_tntStatus.m_freeMemCallCnt;
	status->m_switchVersionPoolCnt = m_tntStatus.m_switchVersionPoolCnt;
	status->m_reclaimVersionPoolCnt = m_tntStatus.m_reclaimVersionPoolCnt;
	status->m_verpoolStatus = m_vPool->getStatus();

	// 如果没有做过版本池切换，则不打印时间
	memset(status->m_switchVerPoolLastTime, 0, 30);
	time_t lastSwitchVerPoolTime = (time_t)(m_tntStatus.m_switchVerPoolLastTime);
	if (lastSwitchVerPoolTime != 0)
		System::formatTime(status->m_switchVerPoolLastTime, sizeof(status->m_switchVerPoolLastTime), &lastSwitchVerPoolTime);
	else
		strcpy(status->m_switchVerPoolLastTime, "NULL");

	// 如果没有做过版本池回收,则不打印开始时间
	memset(status->m_reclaimVerPoolLastBeginTime, 0, 30);
	time_t lastReclaimVerPoolBeginTime = (time_t)(m_tntStatus.m_reclaimVerPoolLastBeginTime);
	if (lastReclaimVerPoolBeginTime != 0)
		System::formatTime(status->m_reclaimVerPoolLastBeginTime, sizeof(status->m_reclaimVerPoolLastBeginTime), &lastReclaimVerPoolBeginTime);
	else
		strcpy(status->m_reclaimVerPoolLastBeginTime, "NULL");

	// 如果版本池回收正在进行中，则不打印End时间戳
	memset(status->m_reclaimVerPoolLastEndTime, 0, 30);
	time_t lastReclaimVerPoolEndTime = (time_t)(m_tntStatus.m_reclaimVerPoolLastEndTime);
	if(lastReclaimVerPoolEndTime >= lastReclaimVerPoolBeginTime && lastReclaimVerPoolEndTime != 0)
		System::formatTime(status->m_reclaimVerPoolLastEndTime, sizeof(status->m_reclaimVerPoolLastEndTime), &lastReclaimVerPoolBeginTime);
	else
		strcpy(status->m_reclaimVerPoolLastEndTime, "NULL");
}

/** 释放TNTBackupProcess中结构，结束事务
 * @param tntBackupProcess 备份句柄
 */
void TNTDatabase::releaseBackupProcess(TNTBackupProcess *tntBackupProcess) {
	BackupProcess *ntseBackupProcess = tntBackupProcess->m_ntseBackupProc;
	TNTTransaction *trx = ntseBackupProcess->m_session->getTrans();
	if (trx != NULL) {
		trx->commitTrx(CS_BACKUP);
		m_tranSys->freeTrx(trx);
	}
	delete ntseBackupProcess;
	delete tntBackupProcess;
}

/** kill recover后hang的prepare事务 */
void TNTDatabase::killHangTrx() {
	m_tranSys->killHangTrx();
}

/** 从path对应的文件中读取所有的数据
 * @param path 需要读取文件的路径
 * @param bufSize out path对应文件总共有多少byte数据
 * return path对应文件的数据
 */
byte *TNTDatabase::readFile(const char *path, u64 *bufSize) throw(NtseException) {
	File file(path);
	u64 errNo = file.open(false);
	if (errNo != File::E_NO_ERROR)
		NTSE_THROW(errNo, "open config file failed");
	file.getSize(bufSize);
	byte *buf = new byte[(u32)*bufSize];
	errNo = file.read(0, (u32)*bufSize, buf);
	if (errNo != File::E_NO_ERROR) {
		delete [] buf;
		file.close();
		NTSE_THROW(errNo, "read config file failed");
	}
	file.close();
	return buf;
}

/** 创建文件，并将buf中的所有数据写入刚创建的文件
 * @param path 需要创建并写入的文件路径
 * @param buf 需要写入的数据
 * @param bufSize 需要写入的数据量
 */
void TNTDatabase::createBackupFile(const char *path, byte *buf, size_t bufSize) throw(NtseException) {
	File backupFile(path);
	backupFile.create(false, false);
	u64 errNo = backupFile.setSize(bufSize);
	if (errNo != File::E_NO_ERROR) {
		backupFile.close();
		NTSE_THROW(errNo, "set size of backup file %s failed", path);
	}
	errNo = backupFile.write(0, bufSize, buf);
	if (errNo != File::E_NO_ERROR) {
		backupFile.close();
		NTSE_THROW(errNo, "write backup file %s failed", path);
	}
	backupFile.close();
}

/** 备份tnt ctrl和dump文件
 * @param backupProcess 备份句柄
 */
void TNTDatabase::backupTNTCtrlAndDumpFile(TNTBackupProcess *backupProcess) throw(NtseException) {
	BackupProcess *ntseBackupProcess = backupProcess->m_ntseBackupProc;
	//因为dump的table是用meta lock s保护，所以dump和backup还是没有做到互斥，只能通过最后的ctrl mutex去保护
	m_tntCtrlFile->lockCFMutex();
	//uint dumpSN = m_tntCtrlFile->getDumpSN();
	//dumpSN为0表示现还未进行dump
	/*if (dumpSN != 0) {
		string dumpPath = generateDumpSNPath(m_tntConfig->m_dumpdir, dumpSN);
		string dumpBackupPath = generateDumpSNPath(ntseBackupProcess->m_backupDir, dumpSN);
		u64 error = File::copyDir(dumpBackupPath.c_str(), dumpPath.c_str(), true);
		if (error != File::E_NO_ERROR) {
			m_tntCtrlFile->unlockCFMutex();
			NTSE_THROW(error, "copy dump file error reason: %s", File::explainErrno(error));
		}
	}*/

	u32 cfSize;
	byte *cfData = m_tntCtrlFile->dupContent(&cfSize);
	AutoPtr<byte> ap(cfData, true);
	string cfPath = string(ntseBackupProcess->m_backupDir) + NTSE_PATH_SEP + Limits::NAME_TNT_CTRL_FILE;
	try {
		createBackupFile(cfPath.c_str(), cfData, cfSize);
	} catch (NtseException &e) {
		m_tntCtrlFile->unlockCFMutex();
		throw e;
	}
	
	m_tntCtrlFile->unlockCFMutex();
}

/**
 * 初始化备份操作
 *
 * @param backupDir 备份到这个目录
 * @throw NtseException 无法创建备份文件，加DDL锁超时，已经有备份操作在进行中等
 */
TNTBackupProcess* TNTDatabase::initBackup(const char *backupDir) throw(NtseException) {
	assert(!m_closed  && !m_unRecovered);
	ftrace(ts.ddl, tout << backupDir);

	// 检查是否已经有一个备份操作在执行
	if (!m_backingUp.compareAndSwap(0, 1))
		NTSE_THROW(NTSE_EC_NOT_SUPPORT, "Another backup process already exist.");

	// 检测目的目录是否存在且是空目录
	bool exist = false;
	File(backupDir).isExist(&exist);
	if (!exist) {
		m_backingUp.set(0);
		NTSE_THROW(NTSE_EC_FILE_NOT_EXIST, "Backup directory %s isn't exists", backupDir);
	}
	bool isEmptyDir = false;
	File(backupDir).isEmptyDirectory(&isEmptyDir);
	if (!isEmptyDir) {
		m_backingUp.set(0);
		NTSE_THROW(NTSE_EC_FILE_NOT_EXIST, "Backup directory %s isn't empty", backupDir);
	}

	BackupProcess *ntseBackupProcess = new BackupProcess(this->m_db, backupDir, 256);
	TNTBackupProcess *backupProcess = new TNTBackupProcess(ntseBackupProcess, this);
	
	try {
		//开启事务
		TNTTransaction *trx = m_tranSys->allocTrx();
		trx->disableLogging();
		trx->startTrxIfNotStarted(ntseBackupProcess->m_session->getConnection(), true);
		ntseBackupProcess->m_session->setTrans(trx);
		// 禁用DDL语句
		acquireDDLLock(ntseBackupProcess->m_session, IL_S, __FILE__, __LINE__);
	} catch (NtseException &e) {
		releaseBackupProcess(backupProcess);
		m_backingUp.set(0);
		throw e;
	}

	// 防止备份期间产生的日志被回收
	LsnType minOnlineLsn = min(m_tntCtrlFile->getDumpLsn(), m_db->getControlFile()->getCheckpointLSN());
	ntseBackupProcess->m_onlineLsnToken = m_tntTxnLog->setOnlineLsn(minOnlineLsn);
	m_tntSyslog->log(EL_LOG, "Begin backup to %s", backupDir);

	return backupProcess;
}

/**
 * 进行备份
 * @pre 必须是BS_INIT状态
 * @post 若成功则为BS_COPYED状态，若失败为BS_FAILED状态
 *
 * @param backupProcess 备份操作
 * @throw NtseException 写备份文件失败
 */
void TNTDatabase::doBackup(TNTBackupProcess *backupProcess) throw(NtseException) {
	BackupProcess *ntseBackupProc = backupProcess->m_ntseBackupProc;
	Session *session = ntseBackupProc->m_session;
	assert(m_db->getDdlLockInst()->isLocked(session->getId(), IL_S));
	assert(backupProcess->m_stat == BS_INIT);
	ftrace(ts.ddl, tout << backupProcess);

	backupProcess->m_stat = BS_FAILED;

	// 备份控制文件。备份控制文件必须在备份表数据之前，防止在备份期间完成了检查点操作导致恢复起始点后置
	m_tntSyslog->log(EL_LOG, "Backup control file...");
	LsnType logStartLsn = min(m_tntCtrlFile->getDumpLsn(), m_db->getControlFile()->getCheckpointLSN()); // 日志备份的起始LSN
	ntseBackupProc->m_logBackuper = new LogBackuper(m_tntTxnLog, logStartLsn);
	m_db->backupCtrlFile(ntseBackupProc);

	// 获取表信息
	u16 numTables = m_db->getControlFile()->getNumTables();
	u16 *tableIds = new u16[numTables];
	AutoPtr<u16> apTableIds(tableIds, true);
	m_db->getControlFile()->listAllTables(tableIds, numTables);
	// 备份每张表的数据
	m_tntSyslog->log(EL_LOG, "Backup tables, numTables = %d", numTables);
	
	// 为底层保证并发，这里暂时将session中的事务置为NULL
	TNTTransaction *trx = session->getTrans();
	session->setTrans(NULL);
	try {
		for (u16 i = 0; i < numTables; ++i) {
			m_db->backupTable(ntseBackupProc, tableIds[i]);
		}
	} catch (NtseException &e) {
		session->setTrans(trx);
		throw e;
	}
	session->setTrans(trx);

	// 然后备份从最近一次checkpoint开始后的日志。
	m_tntSyslog->log(EL_LOG, "Backup log, startLSN "I64FORMAT"u ...", logStartLsn);
	m_db->backupTxnlog(ntseBackupProc, true);
	// 备份ntse和tnt数据库配置文件
	m_tntSyslog->log(EL_LOG, "Backup database config...");
	size_t cfgSize;
	byte *cfg = m_tntConfig->write(&cfgSize);
	string cfgPath = string(ntseBackupProc->m_backupDir) + NTSE_PATH_SEP + Limits::NAME_TNTCONFIG_BACKUP;
	try {
		createBackupFile(cfgPath.c_str(), cfg, cfgSize);
	} catch (NtseException &e) {
		delete []cfg;
		m_tntSyslog->log(EL_ERROR, "createBackupFile error msg: %s", e.getMessage());
		throw e;
	}
	delete []cfg;

	backupProcess->m_stat = BS_COPYED;
}

/**
 * 结束备份并且锁定数据库为只读状态
 * @pre 必须为BS_COPYED状态
 *
 * @param backupProcess 备份操作
 * @throw NtseException 备份日志时写文件失败
 */
void TNTDatabase::finishingBackupAndLock(TNTBackupProcess *backupProcess) throw(NtseException) {
	assert(backupProcess->m_stat == BS_COPYED);
	ftrace(ts.ddl, tout << backupProcess);

	BackupProcess *ntseBackupProc = backupProcess->m_ntseBackupProc;
	Session *session = ntseBackupProc->m_session;

	backupProcess->m_stat = BS_FAILED;

	m_tntSyslog->log(EL_LOG, "Lock database...");
	// 为保证备份数据与binlog相一致，需要在备份即将结束时禁止对数据库的所有
	// 操作，并且刷出MMS更新缓存日志，这一过程如下实现:
	// 1. 得到当前打开的表中启用MMS更新缓存的表列表
	// 2. 对上述每张表，pin住该表防止其被删除或关闭，然后刷MMS更新缓存日志
	// 3. 锁定openLock防止表被打开或关闭
	// 4. 遍历所有表，禁止对表的写操作
	// 5. 若为启用MMS更新缓存的表，再次刷出MMS更新缓存日志，由于刚刷过一次
	//    这次通常比较快，这样可以减少写操作被禁用的时间

	// 第一次获取启用MMS更新缓存的表列表
	vector<string> allPaths;

	acquireATSLock(session, IL_S, -1, __FILE__, __LINE__);
	allPaths = m_db->getOpenedTablePaths(session);
	releaseATSLock(session, IL_S);

	// 第一次刷MMS更新缓存，小型大对象MMS不会启用更新缓存因此不需要刷
	// 底层操作，为保证并发，session中暂时取消事务
	TNTTransaction *trx = session->getTrans();
	session->setTrans(NULL);
	for (size_t i = 0; i < allPaths.size(); i++) {
		Table *table = m_db->pinTableIfOpened(session, allPaths[i].c_str(), -1);
		// 由于开始备份时已经禁止了所有DDL操作，因此这里不需要加表元数据锁
		assert(m_db->getDdlLockInst()->isLocked(session->getId(), IL_S));
		if (table) {
			TableDef *tableDef = table->getTableDef();
			if (tableDef->m_useMms && tableDef->m_cacheUpdate) {
				table->getMmsTable()->flushUpdateLog(session);
			}
			m_db->closeTable(session, table);
		}
	}
	session->setTrans(trx);

	// 锁定每张表禁止写操作，再次刷MMS更新缓存
	acquireATSLock(session, IL_S, -1, __FILE__, __LINE__);
	vector<Table *> tables = m_db->getOpenTables(session);
	for (size_t i = 0; i < tables.size(); i++) {
		//Table *table = m_db->pinTableIfOpened(session, allPaths[i].c_str(), -1);
		Table *table = tables[i];
		assert(table != NULL);
		//此时不会有meta lock的X锁，因为只有alterTableArgument会需要meta lock的X和IX
		//而alterTableArgument需要ddl IX锁，我们进行到此步，ddl已经是S锁了
		table->tntLockMeta(session, IL_S, -1, __FILE__, __LINE__);
		TableDef *tableDef = table->getTableDef();
		//目前认为系统表的写操作是由ddl，用户表的写操作或者修改表定义引起
		//那么此时已经禁止了所有的ddl，用户表的表锁加了S锁导致用户表不会有写操作
		//用户表的meta锁加了S锁导致不会修改表定义操作，故认为此时系统表不会有写操作了
		if (TS_TRX == tableDef->getTableStatus()) {
			//在持有meta lock加table lock，不会造成死锁。此时ddl已经被禁止，只有dml有效，所有的dml都是加meta lock s锁的
_ReStart:
			try {
				session->getTrans()->lockTable(TL_S, tableDef->m_id);
			} catch (NtseException &e) {
				m_tntSyslog->log(EL_WARN, "backup lock table error: %s", e.getMessage());
				goto _ReStart;
			}
		} else if (TS_NON_TRX == tableDef->getTableStatus()) {
			table->lock(session, IL_S, -1, __FILE__, __LINE__);
		}
		
		if (tableDef->m_useMms && tableDef->m_cacheUpdate) {
			table->getMmsTable()->flushUpdateLog(session);
		}
		//m_db->closeTable(session, table);
	}

	backupProcess->m_stat = BS_LOCKED;

	m_tntSyslog->log(EL_LOG, "Backup files that become larger ...");
	m_db->rebackupExtendedFiles(ntseBackupProc);
	m_tntSyslog->log(EL_LOG, "Backup addtional log...");
	// 备份这期间产生的少量NTSE日志，保证备份的NTSE数据与binlog一致，异常直接抛出
	m_db->backupTxnlog(ntseBackupProc, false);
	ntseBackupProc->m_tailLsn = m_tntTxnLog->tailLsn();
	m_tntSyslog->log(EL_LOG, "Finish backup, lsn: "I64FORMAT"u", ntseBackupProc->m_tailLsn);

	backupTNTCtrlAndDumpFile(backupProcess);
}

/**
 * 结束备份
 * @pre 必须为BS_LOCKED或BS_FAILED状态
 * @post backupProcess对象已经被销毁
 *
 * @param backupProcess 备份操作
 */
void TNTDatabase::doneBackup(TNTBackupProcess *backupProcess) {
	BackupProcess *ntseBackupProc = backupProcess->m_ntseBackupProc;
	Session *session = ntseBackupProc->m_session;
	assert(backupProcess->m_stat == BS_LOCKED || backupProcess->m_stat == BS_FAILED);
	ftrace(ts.ddl, tout << backupProcess);

	if (backupProcess->m_stat == BS_LOCKED) {
		// 恢复写操作
		//vector<string> paths = m_db->getOpenedTablePaths(session);
		vector<Table *> tables = m_db->getOpenTables(session);
		for (size_t i = 0; i < tables.size(); i++) {
			//Table *table = m_db->pinTableIfOpened(session, paths[i].c_str(), -1);
			Table *table = tables[i];
			assert(table != NULL);
			if (TS_NON_TRX == table->getTableDef()->getTableStatus()) {
				table->unlock(session, IL_S);
			}
			table->tntUnlockMeta(session, IL_S);
			//m_db->closeTable(session, table);
		}
		releaseATSLock(session, IL_S);
	}
	// 暂时取消session的事务，保证底层的并发逻辑
	TNTTransaction *trx= session->getTrans();
	session->setTrans(NULL);
	for (size_t i = 0; i < ntseBackupProc->m_tables.size(); ++i)
		m_db->closeTable(session, ntseBackupProc->m_tables[i]);
	session->setTrans(trx);
	m_tntTxnLog->clearOnlineLsn(ntseBackupProc->m_onlineLsnToken);
	releaseDDLLock(session, IL_S);
	m_backingUp.set(0);

	releaseBackupProcess(backupProcess);
	m_tntSyslog->log(EL_LOG, "Finish doneBackup");
}

/**
 * 从备份文件恢复数据库
 *
 * @param backupDir 备份所在目录
 * @param basedir 数据文件恢复到这个目录
 * @param logdir  redo日志恢复到这个目录
 * @throw NtseException 读备份文件失败，写数据失败等
 */
void TNTDatabase::restore(const char *backupDir, const char *baseDir, const char *logDir) throw(NtseException) {
	Database::restore(backupDir, baseDir, logDir);
	Syslog log(NULL, EL_LOG, true, true);
	string destDir(baseDir);
	string srcDir(backupDir);

	// 拷贝控制文件
	string ctfPath = destDir + NTSE_PATH_SEP + Limits::NAME_TNT_CTRL_FILE;
	string ctfBackPath = srcDir + NTSE_PATH_SEP + Limits::NAME_TNT_CTRL_FILE;
	File backCtrlFile(ctfBackPath.c_str());
	u64 errNo = backCtrlFile.move(ctfPath.c_str(), true);
	if (errNo != File::E_NO_ERROR)
		NTSE_THROW(errNo, "copy %s to %s failed", ctfBackPath.c_str(), ctfPath.c_str());

	//恢复
	u64 bufSize = 0;
	string tntCfgPath = srcDir + NTSE_PATH_SEP + Limits::NAME_TNTCONFIG_BACKUP;
	byte *buf = readFile(tntCfgPath.c_str(), &bufSize);
	AutoPtr<TNTConfig> tntConfig(TNTConfig::read(buf, (size_t)bufSize));
	delete [] buf;
	tntConfig->setTntBasedir(baseDir);
	tntConfig->setTntDumpdir(baseDir);
	tntConfig->setNtseBasedir(baseDir);
	tntConfig->setNtseTmpdir(baseDir);
	tntConfig->setTxnLogdir(logDir);
	
	TNTDatabase *db = TNTDatabase::open(tntConfig, false, 1);
	db->close(true, true);
	delete db;
}

/**
 * 设置数据库中open table cache 大小
 *
 * @param logEntry 单条日志
 */
void TNTDatabase::setOpenTableCnt(u32 tableOpenCnt) {
	// 修改32位的变量，不需要加锁保护
	m_tntConfig->setOpenTableCnt(tableOpenCnt);
}


/**
 * 恢复过程中解析TNT的redo日志，并输出到dump_tnt_log 文件中,用于调试
 *
 * @param logEntry 单条日志
 */
void TNTDatabase::parseTntRedoLogForDebug(Syslog *syslog, const LogEntry *logEntry, LsnType lsn) {
	TrxId		trxId = parseTrxId(logEntry);
	LogType		logType	= logEntry->m_logType;
	u16			tableId		= logEntry->m_tableId;
	Connection *redoConn = m_db->getConnection(false);
	Session *redoSession = m_db->getSessionManager()->allocSession("print redo log", redoConn);
	MemoryContext *redoCtx = redoSession->getMemoryContext();

	if(logType == TNT_BEGIN_TRANS) {
		syslog->log(EL_DEBUG, "TNT_BEGIN_TRANS, TRXID = %llu, CURLSN = %llu", trxId, lsn);
	} else if(logType == TNT_COMMIT_TRANS) {
		syslog->log(EL_DEBUG, "TNT_COMMIT_TRANS, TRXID =  %llu, CURLSN = %llu", trxId, lsn);
	} else if(logType == TNT_PREPARE_TRANS) {
		TrxId tid = 0;
		LsnType preLsn = 0;
		XID xid;
		TNTTransaction::parsePrepareTrxLog(logEntry, &tid, &preLsn, &xid);
		u64 tmp;
		memcpy(&tmp, xid.data + 8 + sizeof(unsigned long), sizeof(tmp));
		syslog->log(EL_DEBUG, "TNT_PREPARE_TRANS, TRXID =  %llu, XID = %llu, CURLSN = %llu", trxId, tmp, lsn);
	} else if(logType == TNT_BEGIN_ROLLBACK_TRANS) {
		syslog->log(EL_DEBUG, "TNT_BEGIN_ROLLBACK_TRANS, TRXID =  %llu, CURLSN = %llu", trxId, lsn);
	} else if(logType == TNT_END_ROLLBACK_TRANS) {
		syslog->log(EL_DEBUG, "TNT_END_ROLLBACK_TRANS, TRXID =  %llu, CURLSN = %llu", trxId, lsn);
	} else if(logType == TNT_PARTIAL_BEGIN_ROLLBACK) {
		syslog->log(EL_DEBUG, "TNT_PARTIAL_BEGIN_ROLLBACK, TRXID =  %llu, CURLSN = %llu", trxId, lsn);
	} else if(logType == TNT_PARTIAL_END_ROLLBACK) {
		syslog->log(EL_DEBUG,  "TNT_PARTIAL_END_ROLLBACK, TRXID =  %llu, CURLSN = %llu", trxId, lsn);
	} else if(logType == TNT_BEGIN_PURGE_LOG) {
		//TODO: 解析日志
		TrxId    tid = 0;
		LsnType	 preLsn = 0;
		parsePurgeBeginLog(logEntry, &tid, &preLsn);
		syslog->log(EL_DEBUG, "TNT_BEGIN_PURGE_LOG, TRXID =  %llu, CURLSN = %llu", trxId, lsn);
	} else if (logType == TNT_END_PURGE_LOG){
		syslog->log(EL_DEBUG,  "TNT_END_PURGE_LOG, TRXID =  %llu, CURLSN = %llu", trxId, lsn);
	} else if(logType == TNT_PURGE_BEGIN_FIR_PHASE) {
		TrxId    tid = 0;
		TrxId	 minReadView = 0;
		LsnType	 preLsn = 0;
		TNTTable::parsePurgePhase1(logEntry, &tid, &preLsn, &minReadView);
		syslog->log(EL_DEBUG, "TNT_PURGE_BEGIN_FIR_PHASE, TRXID =  %llu, CURLSN = %llu, MinReadView = %llu, TableId = %hd", trxId, lsn, minReadView, tableId);
	} else if(logType == TNT_PURGE_BEGIN_SEC_PHASE) {
		syslog->log(EL_DEBUG, "TNT_PURGE_BEGIN_SEC_PHASE, TRXID =  %llu, CURLSN = %llu, TableId = %hd", trxId, lsn, tableId);
	} else if(logType == TNT_PURGE_END_HEAP) {
		syslog->log(EL_DEBUG, "TNT_PURGE_END_HEAP, TRXID =  %llu, CURLSN = %llu, , TableId = %hd", trxId, lsn, tableId);
	} else if(logType == TNT_UNDO_I_LOG){
		//TODO: 解析日志
		TrxId tid;
		LsnType preLsn;
		RowId rid;
		DrsHeap::parseInsertTNTLog(logEntry, &tid, &preLsn, &rid);
		assert(tid == trxId);
		syslog->log(EL_DEBUG, "TNT_UNDO_I_LOG, TRXID =  %llu , RowId = %llu, CURLSN = %llu, TableId = %hd", trxId, rid, lsn, tableId);
	} else if(logType == TNT_UNDO_LOB_LOG) {
		//TODO: 解析日志
		TrxId tid;
		LsnType preLsn;
		LobId lobId;
		LobStorage::parseTNTInsertLob(logEntry, &tid, &preLsn, &lobId);
		assert(tid == trxId);
		syslog->log(EL_DEBUG, "TNT_UNDO_LOB_LOG, TRXID =  %llu , LobId = %llu, CURLSN = %llu, TableId = %hd", trxId, lobId, lsn, tableId);
	} else if(logType == TNT_U_I_LOG) {
		//TODO: 解析日志
		TrxId tid;
		LsnType preLsn;
		RowId rid, rollBackId;
		u8 tableIndex;
		SubRecord *updateSub = NULL;
		TNTTable *table = getTNTTable(redoSession, logEntry->m_tableId);
		table->getMRecords()->parseFirUpdateLog(logEntry, &tid, &preLsn, &rid, &rollBackId, &tableIndex, &updateSub, redoCtx);
		syslog->log(EL_DEBUG, "TNT_U_I_LOG, TRXID =  %llu , RowId = %llu, CURLSN = %llu, TableId = %hd", trxId, rid, lsn, tableId);
		//解析更新项
		RecordOper::parseAndPrintRedSubRecord(syslog, table->getNtseTable()->getTableDef(), updateSub);	
	} else if(logType == TNT_U_U_LOG) {
		//TODO: 解析日志
		TrxId tid;
		LsnType preLsn;
		RowId rid, rollBackId;
		u8 tableIndex;
		SubRecord *updateSub = NULL;
		TNTTable *table = getTNTTable(redoSession, logEntry->m_tableId);
		table->getMRecords()->parseSecUpdateLog(logEntry, &tid, &preLsn, &rid, &rollBackId, &tableIndex, &updateSub, redoCtx);
		syslog->log(EL_DEBUG, "TNT_U_U_LOG, TRXID =  %llu , RowId = %llu, CURLSN = %llu, TableId = %hd", trxId, rid, lsn, tableId);
		//解析更新项
		RecordOper::parseAndPrintRedSubRecord(syslog, table->getNtseTable()->getTableDef(), updateSub);
	} else if(logType == TNT_D_I_LOG) {
		//TODO: 解析日志
		TrxId tid;
		LsnType preLsn;
		RowId rid, rollBackId;
		u8 tableIndex;
		TNTTable *table = getTNTTable(redoSession, logEntry->m_tableId);
		table->getMRecords()->parseFirRemoveLog(logEntry, &tid, &preLsn, &rid, &rollBackId, &tableIndex);
		syslog->log(EL_DEBUG, "TNT_D_I_LOG, TRXID =  %llu, RowId = %llu, CURLSN = %llu, TableId = %hd", trxId, rid, lsn, tableId);
	} else if(logType == TNT_D_U_LOG) {
		//TODO: 解析日志
		TrxId tid;
		LsnType preLsn;
		RowId rid, rollBackId;
		u8 tableIndex;
		TNTTable *table = getTNTTable(redoSession, logEntry->m_tableId);
		table->getMRecords()->parseSecRemoveLog(logEntry, &tid, &preLsn, &rid, &rollBackId, &tableIndex);
		syslog->log(EL_DEBUG, "TNT_D_U_LOG, TRXID =  %llu, RowId = %llu, CURLSN = %llu, TableId = %hd", trxId, rid, lsn, tableId);
	}
	m_db->getSessionManager()->freeSession(redoSession);
	m_db->freeConnection(redoConn);
}
}
