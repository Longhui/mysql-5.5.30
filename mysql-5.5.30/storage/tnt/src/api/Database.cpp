/**
 * 数据库及数据字典
 *
 * @author 汪源(wangyuan@corp.netease.com, wy@163.org)
 */

#ifdef NTSE_KEYVALUE_SERVER
#include "keyvalue/KeyValueServer.h"
#endif

#include "api/Database.h"
#include "misc/Callbacks.h"
#include "misc/Syslog.h"
#include "misc/Buffer.h"
#include "misc/LockManager.h"
#include "misc/IndiceLockManager.h"
#include "util/File.h"
#include "util/SmartPtr.h"
#include "api/Table.h"
#include <string>
#include <sstream>
#include <algorithm>
#include "misc/ControlFile.h"
#include "misc/Session.h"
#include "mms/Mms.h"
#include "util/Stream.h"
#include "btree/Index.h"
#include "util/Array.h"
#include "misc/MemCtx.h"
#include "misc/Global.h"
#include "misc/ParFileParser.h"
#include "misc/OptimizeThread.h"

#ifdef NTSE_KEYVALUE_SERVER
#include "mysql_priv.h"
#endif

#ifdef TNT_ENGINE
#include "api/TNTDatabase.h"
#endif

using namespace std;

namespace ntse {

const int Database::MAX_OPEN_TABLES = TableDef::MAX_NORMAL_TABLEID;

static string basename(const char *absPath);
static string dirname(const char *absPath);
static string makeBackupPath(const string& backupDir, const string& filename, u16 tableId);

/** 定期进行检查点的后台线程 */
class Checkpointer: public BgTask {
public:
	/**
	 * 构造函数
	 *
	 * @param db 数据库
	 * @param interval 检查点时间间隔，单位秒
	 */
	Checkpointer(Database *db)
		: BgTask(db, "Database::Checkpointer", (uint)5 * 1000, true) {
		m_instant = false;
		m_lastNtseLogSize = m_db->getTxnlog()->getStatus().m_ntseLogSize;
		m_useAio = m_db->getConfig()->m_aio;
#ifndef WIN32
		if(m_useAio) {
			u64 errCode = m_aioArray.aioInit();
			if (File::E_NO_ERROR != errCode) {
				m_db->getSyslog()->fopPanic(errCode, "System AIO Init Error");
			}
		}
#endif
	}

	~Checkpointer() {
#ifndef WIN32
		if(m_db->getConfig()->m_aio) {
			u64 errCode = m_aioArray.aioDeInit();
			if (File::E_NO_ERROR != errCode) {
				m_db->getSyslog()->fopPanic(errCode, "System AIO DeInit Error");
			}
		}
#endif
	}

	virtual void runIt() {
#ifndef NTSE_UNIT_TEST
		// 如果系统没有产生NTSE日志，那么就不需要做检查点
		if (m_db->getTxnlog()->getStatus().m_ntseLogSize - m_lastNtseLogSize == 0) {
			// 重置统计信息
			m_db->getPageBuffer()->resetFlushStatus();	

			// 如果当前日志已超过检查点LSN 一半的logCapacity的大小，则强制推进一次检查点
			LsnType logCapacity = m_db->getConfig()->m_logFileCntHwm * m_db->getConfig()->m_logFileSize;
			if(m_db->getTxnlog()->tailLsn() - m_db->getControlFile()->getCheckpointLSN() < logCapacity / 2)
			return;
		}
		m_instant = false;
		m_lastNtseLogSize = m_db->getTxnlog()->getStatus().m_ntseLogSize;

		try {
			m_db->checkpoint(m_session, &m_aioArray);
		} catch (NtseException &e) {
			m_db->getSyslog()->log(EL_WARN, "Checkpoint skipped or canceled: %s", e.getMessage());
		}
#endif
	}

	/** 取消检查点操作，只在系统关闭时调用 */
	void cancel() {
		m_session->setCanceled(true);
	}

	/** 设置检查点操作周期 */
	void setInterval(u32 sec) {
		m_interval = sec;
	}

	/** 设置是否立即执行一次检查点操作标志位 */
	void setInstant(bool ins) {
		m_instant = ins;
	}
private:
	bool		m_instant;			/** 是否立即执行*/
	u64			m_lastNtseLogSize;		/** 上一次完成检查点时ntse的log写入量 */
	bool		m_useAio;			/** 是否使用Aio */
	AioArray	m_aioArray;			/** 检查点Aio队列 */
};

///////////////////////////////////////////////////////////////////////////////
// 数据库 ////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////////


/**
 * 打开一个数据库
 *
 * @param config 配置参数
 *   内存使用约定：直接引用至数据库关闭
 * @param autoCreate 数据库不存在时是否自动创建
 * @param recover 是否恢复，0表示根据数据库是否安全关闭需要自动决定是否恢复，1表示强制恢复，-1表示强制不恢复
 * @param pkeRequiredForDnU	在UPDATE和DELETE操作的时候是否需要得到PKE
 * @throw NtseException 操作失败
 */
Database* Database::open(Config *config, bool autoCreate, int recover, bool pkeRequiredForDnU /*= false*/) throw(NtseException) {
	ftrace(ts.ddl, tout << config << autoCreate << recover);
	assert(recover >= -1 && recover <= 1);
	assert(config->m_logFileCntHwm > 1);
	// 判断控制文件是否存在，若控制文件存在，则认为数据库已经创建，这时
	// 打开数据库。否则认为数据库不存在，这时创建数据库
	string basedir(config->m_basedir);
	string path = basedir + NTSE_PATH_SEP + Limits::NAME_CTRL_FILE;

	if (!File::isExist(path.c_str())) {
		if (autoCreate)
			create(config);
		else
			NTSE_THROW(NTSE_EC_FILE_NOT_EXIST, "Ctrl file %s not exists.", path.c_str());
	}

	Database *db = new Database(config);
	db->m_stat = DB_STARTING;
	bool shouldRecover;

	path = basedir + NTSE_PATH_SEP + Limits::NAME_SYSLOG;
	db->m_syslog = new Syslog(path.c_str(), config->m_logLevel, false, config->m_printToStdout);

	db->m_cbManager = new NTSECallbackManager();
	db->m_pkeRequiredForDnU = pkeRequiredForDnU;

	try {
		path = basedir + NTSE_PATH_SEP + Limits::NAME_CTRL_FILE;
		db->m_ctrlFile = ControlFile::open(path.c_str(), db->m_syslog);

		db->m_txnlog = 0;
		do {
			try {
				db->m_txnlog = Txnlog::open(db, config->m_basedir, Limits::NAME_TXNLOG, db->m_ctrlFile->getNumTxnlogs(),
					config->m_logFileSize, config->m_logBufSize);
			} catch(NtseException &e) {
				if (e.getErrorCode() == NTSE_EC_MISSING_LOGFILE) { // 日志文件已经被删除
					db->getSyslog()->log(EL_WARN, "Missing transaction log files! Try to recreate.");
					// 假定数据库状态一致，重新创建日志文件
					LsnType ckptLsn = Txnlog::recreate(db->getConfig()->m_basedir
								, Limits::NAME_TXNLOG, db->getControlFile()->getNumTxnlogs()
								, config->m_logFileSize, db->getControlFile()->getCheckpointLSN());
					db->getSyslog()->log(EL_LOG, "Transaction log files created, set checkpoint LSN "
						"from "I64FORMAT"u to"I64FORMAT"u.", db->getControlFile()->getCheckpointLSN(), ckptLsn);
					db->getControlFile()->setCheckpointLSN(ckptLsn);
				} else {
					throw e;
				}
			}
		} while(!db->m_txnlog);

		db->m_pagePool = new PagePool(config->m_maxSessions + 1, Limits::PAGE_SIZE);
		db->m_buffer = new Buffer(db, config->m_pageBufSize, db->m_pagePool, db->m_syslog, db->m_txnlog);
		db->m_pagePool->registerUser(db->m_buffer);
		db->m_rowLockMgr = new LockManager(config->m_maxSessions * 2);
		db->m_idxLockMgr = new IndicesLockManager(config->m_maxSessions + 1, config->m_maxSessions * 5 * 4);
		db->m_sessionManager = new SessionManager(db, config->m_maxSessions, config->m_internalSessions);

		if (recover == 1)
			shouldRecover = true;
		else if (recover == -1)
			shouldRecover = false;
		else
			shouldRecover = !db->m_ctrlFile->isCleanClosed();

		db->m_mms = new Mms(db, config->m_mmsSize, db->m_pagePool, shouldRecover);
		db->m_pagePool->registerUser(db->m_mms);
		db->m_pagePool->init();

		db->m_stat = DB_MOUNTED;
		db->waitBgTasks(false);

		if (shouldRecover) {
			if (db->m_config->m_backupBeforeRecover) {
				stringstream targetDir;
				targetDir << basedir << "/../ntse-recover-auto-bak-" << db->m_ctrlFile->getCheckpointLSN();
				db->fileBackup(targetDir.str().c_str());
			}
			db->recover();
		}
	} catch (NtseException &e) {
		db->m_syslog->log(EL_ERROR, e.getMessage());
		db->close(true, false);
		delete db;
		throw e;
	}

	db->m_unRecovered = recover == -1;
	if (!db->m_unRecovered) {
		db->m_chkptTask = new Checkpointer(db);
		db->m_chkptTask->start();
	}

	db->m_bgCustomThdsManager = new BgCustomThreadManager(db);
	db->m_bgCustomThdsManager->start();

	db->m_closed = false;
	db->m_stat = DB_RUNNING;
	db->waitBgTasks(true);

	if (shouldRecover && db->m_config->m_verifyAfterRecover)
		db->verify();
	
	return db;
}

/** 判断文件是否需要拷贝
 * @param path 文件路径
 * @param name 文件名
 * @param isDir 是否为目录
 * @return 是否需要拷贝
 */
static bool dbBackupFilter(const char *path, const char *name, bool isDir) {
	UNREFERENCED_PARAMETER(path);
	if (isDir)
		return true;
	if (Database::isFileOfNtse(name))
		return true;
	string sName(name);
	if (sName.rfind(".frm") == sName.size() - strlen(".frm"))
		return true;
	return false;
}

/** 判断指定名称的文件是否是NTSE使用的文件
 * @param name 文件名
 * @return 指定名称的文件是否是NTSE使用的文件
 */
bool Database::isFileOfNtse(const char *name) {
	string sName(name);
	if (sName.rfind(Limits::NAME_HEAP_EXT) == sName.size() - strlen(Limits::NAME_HEAP_EXT))
		return true;
	if (sName.rfind(Limits::NAME_TBLDEF_EXT) == sName.size() - strlen(Limits::NAME_TBLDEF_EXT))
		return true;
	if (sName.rfind(Limits::NAME_IDX_EXT) == sName.size() - strlen(Limits::NAME_IDX_EXT))
		return true;
	if (sName.rfind(Limits::NAME_LOBD_EXT) == sName.size() - strlen(Limits::NAME_LOBD_EXT))
		return true;
	if (sName.rfind(Limits::NAME_LOBI_EXT) == sName.size() - strlen(Limits::NAME_LOBI_EXT))
		return true;
	if (sName.rfind(Limits::NAME_SOBH_EXT) == sName.size() - strlen(Limits::NAME_SOBH_EXT))
		return true;
	if (sName.rfind(Limits::NAME_SOBH_TBLDEF_EXT) == sName.size() - strlen(Limits::NAME_SOBH_TBLDEF_EXT))
		return true;
	if (sName.rfind(Limits::NAME_GLBL_DIC_EXT) == sName.size() - strlen(Limits::NAME_GLBL_DIC_EXT))
		return true;
	if (sName.rfind(Limits::NAME_TEMP_GLBL_DIC_EXT) == sName.size() - strlen(Limits::NAME_TEMP_GLBL_DIC_EXT))
		return true;
	if (sName.rfind(Limits::NAME_TMP_IDX_EXT) == sName.size() - strlen(Limits::NAME_TMP_IDX_EXT))
		return true;
	if (sName.compare(Limits::NAME_CTRL_FILE) == 0)
		return true;
	if (sName.compare(Limits::NAME_SYSLOG) == 0)
		return true;
	if (sName.find(Limits::NAME_TXNLOG) == 0)
		return true;
	if (sName.find(Limits::TEMP_FILE_PREFIX) == 0)
		return true;
	if (sName.find(Limits::NAME_TEMP_TABLE) != string::npos)
		return true;
	return false;
}

/** 进行文件系统备份。若目标目录已经存在时不备份
 * @param dir 备份目标目录
 * @return 是否进行了备份
 * @throw NtseException 文件操作失败
 */
bool Database::fileBackup(const char *dir) throw(NtseException) {
	assert(m_stat == DB_MOUNTED);
	m_syslog->log(EL_LOG, "Prepare to backup database to directory %s", dir);
	if (File::isExist(dir)) {
		m_syslog->log(EL_LOG, "Target directory already exists, skip backup");
		return false;
	}
	// 备份数据库目录下的所有文件，而不能根据控制文件只备份NTSE表数据文件，
	// 因为这样就无法备份建表过程中创建的文件
	// 作为一个用于方便调试的功能，多备份一些数据也可以接受
	u64 code = File::copyDir(dir, m_config->m_basedir, false, dbBackupFilter);
	if (code != File::E_NO_ERROR)
		NTSE_THROW(code, "Backup failed.");
	return true;
}

/**
 * 创建数据库
 *
 * @param config 数据库配置
 * @throw NtseException 文件已经存在，无权限等导致操作失败
 */
void Database::create(Config *config) throw(NtseException) {
	ftrace(ts.ddl, tout << config);
	string basedir(config->m_basedir);

	string path = basedir + "/" + Limits::NAME_SYSLOG;
	Syslog syslog(path.c_str(), EL_LOG, true, true);

	path = basedir + "/" + Limits::NAME_CTRL_FILE;
	
#ifdef TNT_ENGINE
	ControlFile::create(path.c_str(), &syslog, config->m_logFileCntHwm);
	Txnlog::create(config->m_logdir, Limits::NAME_TXNLOG, config->m_logFileSize, config->m_logFileCntHwm);
#else
	ControlFile::create(path.c_str(), &syslog);
	Txnlog::create(config->m_basedir, Limits::NAME_TXNLOG, config->m_logFileSize);
#endif
}

/**
 * 删除数据库
 *
 * @param dir 数据库文件所在目录
 */
#ifdef TNT_ENGINE
void Database::drop(const char *dir, const char *logDir) {
#else
void Database::drop(const char *dir) {
#endif
	ftrace(ts.ddl, tout << dir);
	string basedir(dir);

	string path = basedir + NTSE_PATH_SEP + Limits::NAME_CTRL_FILE;
	try {
		Syslog log(NULL, EL_LOG, true, true);
		ControlFile *cf = ControlFile::open(path.c_str(), &log);
#ifdef TNT_ENGINE
		if (logDir != NULL) {
			Txnlog::drop(logDir, Limits::NAME_TXNLOG, cf->getNumTxnlogs());
		} else {
			logDir = ".";
#endif
			Txnlog::drop(dir, Limits::NAME_TXNLOG, cf->getNumTxnlogs());
#ifdef TNT_ENGINE
		}
#endif
		cf->close();
		delete cf;
	} catch (NtseException &) {
	}
	File ctrlFile(path.c_str());
	ctrlFile.remove();

	path = basedir + NTSE_PATH_SEP + Limits::NAME_SYSLOG;
	File syslog(path.c_str());
	syslog.remove();
}

/**
 * 关闭数据库
 *
 * @param flushLog 是否刷出日志
 * @param flushData 是否刷出数据
 */
void Database::close(bool flushLog, bool flushData) {
	ftrace(ts.ddl, tout << flushLog << flushData);
	if (m_closed)
		return;
	DbStat prevStat = m_stat;
	m_stat = DB_CLOSING;

	if (m_bgCustomThdsManager) {
		m_bgCustomThdsManager->stopAll();
		m_bgCustomThdsManager->join();
		delete m_bgCustomThdsManager;
		m_bgCustomThdsManager = NULL;
	}

	// 在关闭表之前关闭检查点和MMS替换后台线程，防止这两类线程操作过程中表被关闭
	if (m_chkptTask) {
		((Checkpointer *)m_chkptTask)->cancel();
		m_chkptTask->stop();
		m_chkptTask->join();
		delete m_chkptTask;
	}
	if (m_mms)
		m_mms->stopReplacer();

	// 关闭打开的表
	if (prevStat != DB_STARTING)
		closeOpenTables(flushData);

	// 关闭MMS
	if (m_mms)
		m_mms->close();

	// 关闭日志系统
	LsnType tailLsn = INVALID_LSN;
	if (m_txnlog) {
		if (flushLog)
			tailLsn = m_txnlog->tailLsn();
		m_txnlog->close(flushLog);
	}

	// 关闭控制文件系统
	if (m_ctrlFile) {
		if (flushData && flushLog && tailLsn != INVALID_LSN)
			m_ctrlFile->setCheckpointLSN(tailLsn);
		m_ctrlFile->close(flushData, flushData);
		delete m_ctrlFile;
	}

	// 在数据库没有成功打开时，以下指针可能为NULL，但C++
	// 保证delete空指针不进行任何操作，因此不用判断
	if (m_pagePool)
		m_pagePool->preDelete();
	delete m_mms;
	delete m_buffer;
	delete m_pagePool;
	delete m_rowLockMgr;
	delete m_idxLockMgr;
	delete m_sessionManager;
	delete m_syslog;
	delete m_idToTables;
	delete m_pathToTables;
	delete m_atsLock;
	delete m_ddlLock;
	delete m_chkptLock;
	delete m_bgtLock;
	delete m_bgTasks;
	delete m_cbManager;
	delete m_openTablesLink;
	init();
	m_closed = true;
}

/**
* 指定tableId，得到对应的TableInfo
* 目前，此函数仅在恢复过程中调用
* @param	tableId 表Id
* @return	TableInfo实例
*/
TableInfo* Database::getTableInfoById(u16 tableId) {

	TableInfo *tableInfo = m_idToTables->get(tableId);

	assert(tableInfo != NULL);

	return tableInfo;
}

/**
 * 关闭打开中的表
 *
 * @param flushData 是否写出数据
 */
void Database::closeOpenTables(bool flushData) {
	Connection *conn = getConnection(true, __FUNC__);
	Session *session = m_sessionManager->allocSession(__FUNC__, conn);

	acquireATSLock(session, IL_X, -1, __FILE__, __LINE__);

	size_t numTables = m_idToTables->getSize();
	TableInfo **tables = new TableInfo*[numTables];
	m_idToTables->values(tables);

	for (size_t i = 0; i < numTables; i++) {
		tables[i]->m_table->lockMeta(session, IL_X, -1, __FILE__, __LINE__);
		realCloseTable(session, tables[i], flushData);
	}

	delete [] tables;

	releaseATSLock(session, IL_X);

	m_sessionManager->freeSession(session);
	freeConnection(conn);
}

/**
 * 构造一个Database对象
 *
 * @param config 数据库配置
 *   内存使用约定：直接引用至数据库关闭
 */
Database::Database(Config *config) {
	init();
	m_idToTables = new Hash<u16, TableInfo *>(MAX_OPEN_TABLES);
	m_pathToTables = new PathHash(MAX_OPEN_TABLES);
	m_config = config;
	m_atsLock = new IntentionLock(config->m_maxSessions, "Database::atsLock", __FILE__, __LINE__);
	m_ddlLock = new IntentionLock(config->m_maxSessions, "Database::ddlLock", __FILE__, __LINE__);
	m_chkptLock = new Mutex("Database::chkptLock", __FILE__, __LINE__);
	m_bgtLock = new Mutex("Database::bgtLock", __FILE__, __LINE__);
	m_bgTasks = new vector<BgTask *>();
	m_openTablesLink = new DList<Table*>();
}

/**
 * 析构一个Database对象
 * @pre 必须先调用close关闭数据库
 */
Database::~Database() {

}

/** 初始化对象状态 */
void Database::init() {
	memset(this, 0, sizeof(*this));
}

/** 获得数据库状态
 * @return 数据库状态
 */
DbStat Database::getStat() const {
	return m_stat;
}

/**
 * 获得数据库配置
 *
 * @return 数据库配置
 */
Config* Database::getConfig() const {
	assert(!m_closed);
	return m_config;
}

/**
 * 获得OpenTable LRU 链表
 *
 * @return OpenTableLRU
 */
DList<Table*>*	Database::getOpenTableLRU() const{
	return m_openTablesLink;
}
/**
 * 创建表。建表时会加DDL X锁，使得建表只能串行执行
 * @pre tableDef->m_id为TableDef::INVALID_TABLEID
 * @post tableDef->m_id为新建表的ID
 *
 * @param session 会话对象
 * @param path 表文件路径(相对于basedir），不包含后缀
 * @param tableDef 表定义，其中表的ID还没有生成
 * @throw NtseException 表ID已经用完，指定的表已经存在等，DDL操作被禁止等
 */
void Database::createTable(Session *session, const char *path, TableDef *tableDef) throw(NtseException) {
	ftrace(ts.ddl, tout << session << path << tableDef);
	assert(!m_closed && m_stat != DB_CLOSING && !m_unRecovered);
	assert(tableDef->m_id == TableDef::INVALID_TABLEID);

	tableDef->check();

	acquireDDLLock(session, IL_X, __FILE__, __LINE__);

	if (m_ctrlFile->getTableId(path) != TableDef::INVALID_TABLEID) {
		releaseDDLLock(session, IL_X);
		NTSE_THROW(NTSE_EC_FILE_EXIST, "Table %s already exists", path);
	}

	try {
		// NTSETNT-316 由于allocTableId()函数也会抛出Exception
		// 因此需要将这个操作同样用try保护起来，否则会导致DDL锁泄漏
		tableDef->m_id = m_ctrlFile->allocTableId();	

		Table::create(this, session, path, tableDef);
	} catch (NtseException &e) {
		releaseDDLLock(session, IL_X);
		throw e;
	}
	// TODO: 可能会遗留下垃圾文件

	// 建表日志必须在创建表文件之后，更新控制文件之前。若在创建表文件之前写日志，
	// 则必须使用一个事务，否则恢复时不知道建表操作是否成功；若在更新控制文件之后
	// 再写日志，则可能出现建表已经成功，但没有对应日志的情况，违反了WAL原则。
	//
	// 建表时只写一条LOG_CREATE_TABLE日志，建表是否成功取决于控制文件有没有更新，
	// 如果表文件已经创建但控制文件没有更新则认为建表不成功，若建表日志没有写出，
	// 则恢复时不会去删除这些垃圾表文件，没有问题。
	m_txnlog->flush(writeCreateLog(session, path, tableDef));
	SYNCHERE(SP_DB_CREATE_TABLE_AFTER_LOG);
	m_ctrlFile->createTable(path, tableDef->m_id, tableDef->hasLob(), false);

	releaseDDLLock(session, IL_X);
}

/**
 * 写创建表日志
 *
 * @param session 会话
 * @param path 表文件路径
 * @param tableDef 表定义
 * @return 建表日志LSN
 */
u64 Database::writeCreateLog(Session *session, const char *path, const TableDef *tableDef) {
	u64 lsn = 0;
	u32 size = 0;
	byte *tmpBuffer = NULL;
	byte *buf = NULL;
	tableDef->write(&tmpBuffer, &size);
	McSavepoint msp(session->getMemoryContext());
	buf = (byte*)session->getMemoryContext()->alloc(size + Limits::PAGE_SIZE);
	Stream s(buf, size + Limits::PAGE_SIZE);
	s.write(path);
	s.write(size);
	s.write(tmpBuffer, size);
	
	lsn = session->writeLog(LOG_CREATE_TABLE, tableDef->m_id, buf, s.getSize());

	if (tmpBuffer != NULL) {
		delete[] tmpBuffer;
		tmpBuffer = NULL;
	}

	return lsn;
}

/**
 * 解析创建表日志
 *
 * @param log LOG_CREATE_TABLE日志内容
 * @param tableDef OUT，表定义
 * @return 表文件路径
 */
char* Database::parseCreateLog(const LogEntry *log, TableDef *tableDef) {
	assert(log->m_logType == LOG_CREATE_TABLE);
	Stream s(log->m_data, log->m_size);
	char *path;
	u32 size = 0;
	s.readString(&path);
	s.read(&size);
	tableDef->read(s.currPtr(), size);
	assert((size_t)(s.currPtr() - log->m_data + size) == log->m_size);
	return path;
}

/**
 * 删除表。这个函数只能用于删除在数据字典中存在的表，若指定的表在
 * 数据字典中不存在则本函数不进行任何操作。
 *
 * @pre 当前线程不能打开要删除的表，如果有其它线程打开了要删除的表，
 *   则本函数会等待表被最终被关闭
 *
 * @param session 会话
 * @param path 表文件路径，不包含后缀，大小写不敏感
 * @param timeoutSec 超时时间，单位秒，为<0表示不超时，0表示马上超时，>0为超时时间
 * @throw NtseException 指定的表不存在等，超时等
 */
void Database::dropTable(Session *session, const char *path, int timeoutSec) throw(NtseException) {
	ftrace(ts.ddl, tout << path);
	assert(!m_closed && m_stat != DB_CLOSING && !m_unRecovered);

	acquireDDLLock(session, IL_X, __FILE__, __LINE__);

	// 检查表是否存在，必须在加了DDL锁之后进行
	u16 tableId = m_ctrlFile->getTableId(path);
	if (tableId == TableDef::INVALID_TABLEID) {
		releaseDDLLock(session, IL_X);
		return;
	}
	string pathStr = m_ctrlFile->getTablePath(tableId);
#ifdef NTSE_KEYVALUE_SERVER
	// 通知keyvalue，该表正在被关闭
	/** 等待现有的所有对关闭表请求结束之后，关闭表 */
	KeyValueServer::closeDDLTable(this, session, path);
#endif

	acquireATSLock(session, IL_S, -1, __FILE__, __LINE__);
	try {
		waitRealClosed(session, pathStr.c_str(), timeoutSec);
		checkPermission(session, pathStr.c_str());
	} catch (NtseException &e) {
		releaseATSLock(session, IL_S);
		releaseDDLLock(session, IL_X);
		throw e;
	}
#ifdef TNT_ENGINE
	if (session->getTrans() == NULL) {
#endif
	assert(getATSLock(session) == IL_S);
#ifdef TNT_ENGINE
	}
#endif

	// 在进行所有更新之前写日志并强制写出日志，一但日志写出，删除表操作必须强制完成，
	// 这是因为删除表操作是不可逆转的，无法恢复删除到一半的表。
	u64 lsn = writeDropLog(this, tableId, path);
	m_txnlog->flush(lsn);

	m_ctrlFile->dropTable(tableId);
	try {
		Table::drop(this, path);
	} catch (NtseException &e) {
		releaseATSLock(session, IL_S);
		releaseDDLLock(session, IL_X);
		throw e;
	}

	releaseATSLock(session, IL_S);
#ifdef NTSE_KEYVALUE_SERVER
	KeyValueServer::resetDDLTable();
#endif
	releaseDDLLock(session, IL_X);
}

/**
 * 删除表。这个函数只能用于删除在数据字典中存在的表，若指定的表在
 * 数据字典中不存在则本函数不进行任何操作。
 *
 * @pre 当前线程不能打开要删除的表，如果有其它线程打开了要删除的表，
 *   则本函数会等待表被最终被关闭
 *
 * @param session 会话
 * @param path 表文件路径，不包含后缀，大小写不敏感
 * @param timeoutSec 超时时间，单位秒，为<0表示不超时，0表示马上超时，>0为超时时间
 * @throw NtseException 指定的表不存在等，超时等
 */
void Database::dropTableSafe(Session *session, const char *path, int timeoutSec) throw(NtseException) {
	ftrace(ts.ddl, tout << path);
	assert(!m_closed && m_stat != DB_CLOSING && !m_unRecovered);
	assert(getDDLLock(session) == IL_X);
	assert(getATSLock(session) == IL_S);

	// 检查表是否存在，必须在加了DDL锁之后进行
	u16 tableId = m_ctrlFile->getTableId(path);
	if (tableId == TableDef::INVALID_TABLEID) {
		return;
	}
	string pathStr = m_ctrlFile->getTablePath(tableId);
#ifdef NTSE_KEYVALUE_SERVER
	// 通知keyvalue，该表正在被关闭
	/** 等待现有的所有对关闭表请求结束之后，关闭表 */
	KeyValueServer::closeDDLTable(this, session, path);
#endif

	try {
		waitRealClosed(session, pathStr.c_str(), timeoutSec);
		checkPermission(session, pathStr.c_str());
	} catch (NtseException &e) {
		throw e;
	}

	// 在进行所有更新之前写日志并强制写出日志，一但日志写出，删除表操作必须强制完成，
	// 这是因为删除表操作是不可逆转的，无法恢复删除到一半的表。
	u64 lsn = writeDropLog(this, tableId, path);
	m_txnlog->flush(lsn);

	m_ctrlFile->dropTable(tableId);
	try {
		Table::drop(this, path);
	} catch (NtseException &e) {
		throw e;
	}

#ifdef NTSE_KEYVALUE_SERVER
	KeyValueServer::resetDDLTable();
#endif
}


/** 重做CREATE TABLE操作
 * @pre 控制文件还没有更新，真的需要重做
 *
 * @param session 会话
 * @param log LOG_CREATE_TABLE日志
 * @throw NtseException 文件操作失败
 */
void Database::redoCreateTable(Session *session, const LogEntry *log)  throw(NtseException) {
	ftrace(ts.recv, tout << session << log);
	assert(log->m_logType == LOG_CREATE_TABLE);
	assert(m_ctrlFile->getTablePath(log->m_tableId).empty());

	TableDef *tableDef = new TableDef();
	char *path = parseCreateLog(log, tableDef);
	// 建表时是在表数据文件已经创建之后再写日志，因此如果日志已经写出，表一定已经建好，只是需要更新控制文件
	Table *t = Table::open(this, session, path, false);
	assert(*tableDef == *(t->getTableDef(true, session)));
	t->close(session, true, true);
	delete t;
	m_ctrlFile->createTable(path, log->m_tableId, tableDef->hasLob(), false);
	delete []path;
	delete tableDef;
}

/**
 * 重做DROP TABLE操作
 *
 * @param session 会话
 * @param log LOG_DROP_TABLE日志
 * @throw NtseException 文件操作失败
 */
void Database::redoDropTable(Session *session, const LogEntry *log) throw(NtseException) {
	ftrace(ts.recv, tout << session << log);
	assert(log->m_logType == LOG_DROP_TABLE);
	assert(!m_ctrlFile->getTablePath(log->m_tableId).empty());

	TableInfo *tableInfo = m_idToTables->get(log->m_tableId);
	if (tableInfo)
		realCloseTable(session, tableInfo, false);

	m_ctrlFile->dropTable(log->m_tableId);
	char *path = parseDropLog(log);
	Table::drop(this, path);
	delete [] path;
}

/**
 * 重做TRUNCATE操作
 *
 * @param session 会话
 * @param log LOG_TRUNCATE日志
 * @throw NtseException 文件操作失败等
 */
void Database::redoTruncate(ntse::Session *session, const ntse::LogEntry *log) throw(NtseException) {
	ftrace(ts.recv, tout << session << log);
	assert(log->m_logType == LOG_TRUNCATE);
	assert(!m_ctrlFile->getTablePath(log->m_tableId).empty());
	assert(!m_idToTables->get(log->m_tableId));

	string path = m_ctrlFile->getTablePath(log->m_tableId);

	bool newHasDict = false;
	try {
		Table::redoTrunate(this, session, log, path.c_str(), &newHasDict);
	} catch (NtseException &e) {
		throw e;
	}

	//修改控制文件中是否含有全局字典的信息
	alterCtrlFileHasCprsDic(session, log->m_tableId, newHasDict);
}

/**
 * 写DROP TABLE日志
 *
 * @param db 数据库
 * @param tableId 表ID
 * @param path 表文件路径(相对于basedir)，不包含后缀
 * @return 日志LSN
 */
u64 Database::writeDropLog(Database *db, u16 tableId, const char *path) {
	byte buf[Limits::PAGE_SIZE];
	Stream s(buf, sizeof(buf));
	s.write(path);
	return db->getTxnlog()->log(0, LOG_DROP_TABLE, tableId, buf, s.getSize());
}

/**
 * 解析DROP TABLE日志
 *
 * @param log LOG_DROP_TABLE日志
 * @return 表文件路径(相对于basedir)，不包含后缀
 */
char* Database::parseDropLog(const LogEntry *log) {
	assert(log->m_logType == LOG_DROP_TABLE);
	Stream s(log->m_data, log->m_size);
	char *path;
	s.readString(&path);
	return path;
}

/**
 * 打开表，如果指定的表已经打开，则增加引用计数
 *
 * @param session 会话
 * @param path 表文件路径(相对于basedir），不包含后缀，大小写不敏感
 * @return 表对象
 * @throw NtseException 指定的表不存在等
 */
Table* Database::openTable(Session *session, const char *path) throw(NtseException) {
	ftrace(ts.ddl, tout << session << path);
	assert(!m_closed && m_stat != DB_CLOSING && !m_unRecovered);

	bool lockedByMe = false;
	if (getDDLLock(session) == IL_NO) {
		// 如果是备份操作则已经加了锁，因此要判断是否已经加锁
		acquireDDLLock(session, IL_IS, __FILE__, __LINE__);	// 防止打开表过程中表被RENAME或DROP
		lockedByMe = true;
	} else {
#ifdef TNT_ENGINE
		if (session->getTrans() != NULL) {
			assert(getDDLLock(session) == IL_IS || getDDLLock(session) == IL_S || getDDLLock(session) == IL_IX);
		}
		else {
#endif
			assert(IntentionLock::isHigher(IL_IS, getDDLLock(session)));
#ifdef TNT_ENGINE
		}
#endif
	}

	try {
		acquireATSLock(session, IL_X, m_config->m_tlTimeout * 1000, __FILE__, __LINE__);
	} catch (NtseException &e) {
		if (lockedByMe) {
			releaseDDLLock(session, IL_IS);
		}
		throw e;
	}

	TableInfo *tableInfo = m_pathToTables->get((char *)path);
	bool hasCprsDic = false;
	if (!tableInfo) {
		u16 tableId = m_ctrlFile->getTableId(path);
		if (tableId == TableDef::INVALID_TABLEID) {
			releaseATSLock(session, IL_X);
			if (lockedByMe)
				releaseDDLLock(session, IL_IS);
			NTSE_THROW(NTSE_EC_FILE_NOT_EXIST, "No such table '%s'", path);
		}
		hasCprsDic = m_ctrlFile->hasCprsDict(tableId);
		string pathStr = m_ctrlFile->getTablePath(tableId);	// 大小写与传入的path可能不同
		assert(!pathStr.empty());

		Table *table = NULL;
		try {
			table = Table::open(this, session, pathStr.c_str(), hasCprsDic);
		} catch (NtseException &e) {
			releaseATSLock(session, IL_X);
			if (lockedByMe)
				releaseDDLLock(session, IL_IS);
			throw e;
		}
#ifdef TNT_ENGINE
		table->tntLockMeta(session, IL_S, -1, __FILE__, __LINE__);
#else
		table->lockMeta(session, IL_S, -1, __FILE__, __LINE__);
#endif
		if (table->getTableDef(true, session)->m_id != tableId) {
			u16 tableId2 = table->getTableDef(true, session)->m_id;
			table->close(session, false);
			delete table;
			releaseATSLock(session, IL_X);
			if (lockedByMe)
				releaseDDLLock(session, IL_IS);
			NTSE_THROW(NTSE_EC_GENERIC, "Inconsistent table id, in table: %d, in control file: %d",
				tableId2, tableId);
		}

		tableInfo = new TableInfo(table, pathStr.c_str());
		assert(!m_idToTables->get(table->getTableDef(true, session)->m_id));
		m_idToTables->put(table->getTableDef(true, session)->m_id, tableInfo);
		m_pathToTables->put(tableInfo->m_path, tableInfo);
#ifdef TNT_ENGINE
		table->tntUnlockMeta(session, IL_S);
#else
		table->unlockMeta(session, IL_S);
#endif

		// 链入open tables link	
		m_openTablesLink->addLast(&tableInfo->m_table->getOpenTablesLink());	
		// 统计信息
		m_status.m_realOpenTableCnt++;
	} else {
		tableInfo->m_refCnt++;
		// 如果refCnt从0提升为1，那么调整链表中的位置
		if (tableInfo->m_refCnt == 1) {
			m_openTablesLink->moveToLast(&tableInfo->m_table->getOpenTablesLink());
		}
	}
	releaseATSLock(session, IL_X);
	if (lockedByMe)
		releaseDDLLock(session, IL_IS);
	return tableInfo->m_table;
}

/**
 * 关闭表。若表还有其它引用则只减引用计数。若引用计数减到0，则系统会
 * 自动刷写出表中的脏数据。
 * @pre 已经释放了表锁
 * @post 若无其它引用，则table对象已经被销毁不能再使用
 *
 * @param session 会话
 * @param table 要关闭的表
 */
void Database::closeTable(Session *session, Table *table) {
	ftrace(ts.ddl, tout << session << table->getPath());
	assert(!m_closed && !m_unRecovered);
	assert(table->getLock(session) == IL_NO);

	acquireATSLock(session, IL_X, -1, __FILE__, __LINE__);
#ifdef TNT_ENGINE
	table->tntLockMeta(session, IL_S, -1, __FILE__, __LINE__);
#else
	table->lockMeta(session, IL_S, -1, __FILE__, __LINE__);
#endif
	TableInfo *tableInfo = m_idToTables->get(table->getTableDef(true, session)->m_id);
#ifdef TNT_ENGINE
	table->tntUnlockMeta(session, IL_S);
#else
	table->unlockMeta(session, IL_S);
#endif

	assert(tableInfo);
	assert(tableInfo->m_table == table);

	tableInfo->m_refCnt--;
	
	// 如果refCnt为0，移动链表中的位置
	if (tableInfo->m_refCnt == 0) {
		m_openTablesLink->moveToFirst(&tableInfo->m_table->getOpenTablesLink());
	}

	releaseATSLock(session, IL_X);
}

/**	如果引用为0关闭表
* @session	关闭表session
* @table	已打开的表
* @return	是否成功关闭表
*/
bool Database::realCloseTableIfNeed(Session *session, Table *table) throw(NtseException) {
	ftrace(ts.ddl, tout << session << table->getPath());
	assert(!m_closed && !m_unRecovered);
	assert(table->getLock(session) == IL_NO);
#ifndef NTSE_UNIT_TEST
	assert(getATSLock(session) == IL_X);
#endif
#ifdef TNT_ENGINE
	table->tntLockMeta(session, IL_S, -1, __FILE__, __LINE__);
#else
	table->lockMeta(session, IL_S, -1, __FILE__, __LINE__);
#endif
	TableInfo *tableInfo = m_idToTables->get(table->getTableDef(true, session)->m_id);
#ifdef TNT_ENGINE
	table->tntUnlockMeta(session, IL_S);
#else
	table->unlockMeta(session, IL_S);
#endif

	assert(tableInfo);
	assert(tableInfo->m_table == table);
	bool success = false;
	if (tableInfo->m_refCnt == 0) {
#ifdef TNT_ENGINE
		table->tntLockMeta(session, IL_X, -1, __FILE__, __LINE__);
#else
		table->lockMeta(session, IL_X, -1, __FILE__, __LINE__);
#endif
		realCloseTable(session, tableInfo, true);
		success = true;
	}
	
	return success;
}


/**
 * TRUNCATE表
 *
 * @param session        会话
 * @param table          要TRUNCATE的表
 * @param tblLock        是否加表锁
 * @param isTruncateOper 是否是truncate操作
 * @throw NtseException  加表锁超时
 */
void Database::truncateTable(Session *session, Table *table, bool tblLock, bool isTruncateOper) throw(NtseException) {
	ftrace(ts.ddl, tout << session << table->getPath());
	assert(!m_closed && m_stat != DB_CLOSING && !m_unRecovered);

	acquireDDLLock(session, IL_IX, __FILE__, __LINE__);
#ifdef NTSE_KEYVALUE_SERVER
	// 通知keyvalue，该表正在被truncate
	/** 等待现有的所有对关闭表请求结束之后，关闭表 */
	bool existInKeyvalue = KeyValueServer::closeDDLTable(this, session, table->getPath(), false);
	if(existInKeyvalue) {
		TableInfo *tableInfo = m_idToTables->get(table->getTableDef(true, session)->m_id);
		assert(tableInfo);
		assert(tableInfo->m_table == table);

		tableInfo->m_refCnt--;
	}
#endif

	try {
		// TODO 异常情况怎么处理

		//虽然Table::truncate是通过建立新的空表来实现，但是其Id和tableName都没有改变
		bool newHasDict = false;
		table->truncate(session, tblLock, &newHasDict, isTruncateOper);

		//修改控制文件中是否含有全局字典的信息
		alterCtrlFileHasCprsDic(session, table->getTableDef()->m_id, newHasDict);
	} catch (NtseException &e) {
		releaseDDLLock(session, IL_IX);
		throw e;
	}
#ifdef NTSE_KEYVALUE_SERVER
	KeyValueServer::resetDDLTable();
#endif
	releaseDDLLock(session, IL_IX);
}

/**
 * 重命名表
 * @pre 当前线程不能打开要重命名的表，如果有其它线程打开了该表，
 *   则本函数会等待表被最终被关闭
 *
 * @param session 会话
 * @param oldPath 表原来存储路径，大小写不敏感
 * @param newPath 表现在存储路径
 * @param timeoutSec 超时时间，单位秒，为<0表示不超时，0表示马上超时，>0为超时时间
 * @throw NtseException 文件操作失败，原表不存在，目的表已经存在，超时等
 */
void Database::renameTable(Session *session, const char *oldPath, const char *newPath, int timeoutSec) throw(NtseException) {
	ftrace(ts.ddl, tout << session << oldPath << newPath);
	assert(!m_closed && m_stat != DB_CLOSING && !m_unRecovered);

	acquireDDLLock(session, IL_X, __FILE__, __LINE__);

	if (m_ctrlFile->getTableId(newPath) != TableDef::INVALID_TABLEID) {
		releaseDDLLock(session, IL_X);
		NTSE_THROW(NTSE_EC_FILE_EXIST, "Can not rename to %s because it is an existing table.", newPath);
	}
	u16 tableId = m_ctrlFile->getTableId(oldPath);
	if (tableId == TableDef::INVALID_TABLEID) {
		releaseDDLLock(session, IL_X);
		NTSE_THROW(NTSE_EC_FILE_NOT_EXIST, "Table %s doesn't exist", oldPath);
	}
	string oldPathStr = m_ctrlFile->getTablePath(tableId);
	oldPath = oldPathStr.c_str();

#ifdef NTSE_KEYVALUE_SERVER
	// 通知keyvalue，该表正在被关闭
	/** 等待现有的所有对关闭表请求结束之后，关闭表 */
	KeyValueServer::closeDDLTable(this, session, oldPath);
#endif

	acquireATSLock(session, IL_S, -1, __FILE__, __LINE__);
	try {
		waitRealClosed(session, oldPath, timeoutSec);
		checkPermission(session, oldPath);
	} catch (NtseException &e) {
		releaseATSLock(session, IL_S);
		releaseDDLLock(session, IL_X);
		throw e;
	}
	assert(getATSLock(session) == IL_S);

	// 如果通过检查，系统将保证重命名一定成功（若不成功则崩溃进行恢复）
	bool hasLob = m_ctrlFile->hasLob(tableId);
	bool hasCprsDic = m_ctrlFile->hasCprsDict(tableId);
	try {
		Table::checkRename(this, oldPath, newPath, hasLob, hasCprsDic);
	} catch (NtseException &e) {
		releaseATSLock(session, IL_S);
		releaseDDLLock(session, IL_X);
		throw e;
	}

	bumpFlushLsn(session, tableId);

	m_txnlog->flush(writeRenameLog(session, tableId, oldPath, newPath, hasLob, hasCprsDic));	// WAL
	Table::rename(this, session, oldPath, newPath, hasLob, false, hasCprsDic);
	m_ctrlFile->renameTable(oldPath, newPath);
	bumpFlushLsn(session, tableId);

	releaseATSLock(session, IL_S);
#ifdef NTSE_KEYVALUE_SERVER
	KeyValueServer::resetDDLTable();
#endif
	releaseDDLLock(session, IL_X);
}


/**
 * 重命名表
 * @pre 当前线程不能打开要重命名的表，如果有其它线程打开了该表，
 *   则本函数会等待表被最终被关闭
 *
 * @param session 会话
 * @param oldPath 表原来存储路径，大小写不敏感
 * @param newPath 表现在存储路径
 * @param timeoutSec 超时时间，单位秒，为<0表示不超时，0表示马上超时，>0为超时时间
 * @throw NtseException 文件操作失败，原表不存在，目的表已经存在，超时等
 */
void Database::renameTableSafe(Session *session, const char *oldPath, const char *newPath, int timeoutSec) throw(NtseException) {
	ftrace(ts.ddl, tout << session << oldPath << newPath);
	assert(!m_closed && m_stat != DB_CLOSING && !m_unRecovered);

	if (m_ctrlFile->getTableId(newPath) != TableDef::INVALID_TABLEID) {
		NTSE_THROW(NTSE_EC_FILE_EXIST, "Can not rename to %s because it is an existing table.", newPath);
	}
	u16 tableId = m_ctrlFile->getTableId(oldPath);
	if (tableId == TableDef::INVALID_TABLEID) {
		NTSE_THROW(NTSE_EC_FILE_NOT_EXIST, "Table %s doesn't exist", oldPath);
	}
	string oldPathStr = m_ctrlFile->getTablePath(tableId);
	oldPath = oldPathStr.c_str();

#ifdef NTSE_KEYVALUE_SERVER
	// 通知keyvalue，该表正在被关闭
	/** 等待现有的所有对关闭表请求结束之后，关闭表 */
	KeyValueServer::closeDDLTable(this, session, oldPath);
#endif

	try {
		waitRealClosed(session, oldPath, timeoutSec);
		checkPermission(session, oldPath);
	} catch (NtseException &e) {
		throw e;
	}
	assert(getATSLock(session) == IL_S);

	// 如果通过检查，系统将保证重命名一定成功（若不成功则崩溃进行恢复）
	bool hasLob = m_ctrlFile->hasLob(tableId);
	bool hasCprsDic = m_ctrlFile->hasCprsDict(tableId);
	try {
		Table::checkRename(this, oldPath, newPath, hasLob, hasCprsDic);
	} catch (NtseException &e) {
		throw e;
	}

	bumpFlushLsn(session, tableId);

	m_txnlog->flush(writeRenameLog(session, tableId, oldPath, newPath, hasLob, hasCprsDic));	// WAL
	Table::rename(this, session, oldPath, newPath, hasLob, false, hasCprsDic);
	m_ctrlFile->renameTable(oldPath, newPath);
	bumpFlushLsn(session, tableId);

#ifdef NTSE_KEYVALUE_SERVER
	KeyValueServer::resetDDLTable();
#endif
}

/**
 * 写重命名日志
 *
 * @param session 会话
 * @param tableId 表ID
 * @param oldPath 原路径(相对于basedir)，不包含后缀名
 * @param newPath 新路径(相对于basedir)，不包含后缀名
 * @param hasLob 是否包含大对象
 * @return 日志LSN
 */
u64 Database::writeRenameLog(Session *session, u16 tableId, const char *oldPath, const char *newPath, bool hasLob, bool hasCprsDic) {
	size_t size = strlen(oldPath) + strlen(newPath) + sizeof(u32) * 2 + 2 + 1 + 1;
	byte *buf = (byte *)alloca(size);
	Stream s(buf, size);
	s.write(hasLob);
	s.write(hasCprsDic);
	s.write(oldPath);
	s.write(newPath);
	return session->writeLog(LOG_RENAME_TABLE, tableId, buf, (uint)s.getSize());
}

/**
 * 解析重命名日志
 *
 * @param log LOG_RENAME_TABLE日志
 * @param oldPath OUT，原路径
 * @param newPath OUT，新路径
 * @param hasLob OUT，表是否包含大对象
 * @param hasCprsDict OUT, 表是否含有压缩字典
 */
void Database::parseRenameLog(const LogEntry *log, char **oldPath, char **newPath, bool *hasLob, bool *hasCprsDict) {
	assert(log->m_logType == LOG_RENAME_TABLE);
	Stream s(log->m_data, log->m_size);
	s.read(hasLob);
	s.read(hasCprsDict);
	s.readString(oldPath);
	s.readString(newPath);
	assert(s.getSize() == log->m_size);
}

/**
 * 修改表参数。
 *
 * @param session		会话。
 * @param table		操作的表。
 * @param name			修改的对象名。
 * @param valueStr		修改的值。
 * @param timeout		表锁超时时间，单位秒
 * @param inLockTables  操作是否处于Lock Tables 语句之中
 * @throw NtseException	修改操作失败。
 */
void Database::alterTableArgument(Session *session, Table *table, const char *name, const char *valueStr, int timeout, bool inLockTables) throw (NtseException) {
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
 * 修改表是否含有全局压缩字典文件
 * @param session    会话
 * @param path       表路径
 * @param hasCprsDict 是否含有全局压缩字典文件
 */
void Database::alterCtrlFileHasCprsDic(Session *session, u16 tableId, bool hasCprsDict) {
	bool ddlLockedByMe = false;
	if (getDDLLock(session) == IL_NO) {
		acquireDDLLock(session, IL_IS, __FILE__, __LINE__);
		ddlLockedByMe = true;
	} else {
		assert(IntentionLock::isHigher(IL_IS, getDDLLock(session)));
	}
	
	m_ctrlFile->alterHasCprsDic(tableId, hasCprsDict);
	assert(hasCprsDict == m_ctrlFile->hasCprsDict(tableId));

	if (ddlLockedByMe)
		releaseDDLLock(session, IL_IS);
}

/**
 * 优化表数据
 *
 * @param session 会话
 * @param table 要优化的表
 * @param keepDict 是否指定了保留旧字典文件(如果有的话)
 * @throw NtseException 加表锁超时或文件操作异常，在发生异常时，本函数保证原表数据不会被破坏
 */
void Database::optimizeTable(Session *session, Table *table, bool keepDict /*=false*/, bool waitForFinish /*=true*/) throw(NtseException) {
	UNREFERENCED_PARAMETER(session);
	if (waitForFinish) {
		OptimizeThread thd(this, m_bgCustomThdsManager, table, keepDict, false);
		thd.start();
		thd.join();
	} else {
		//BgCustomThread对象由后台线程管理器负责回收
		BgCustomThread *thd = new OptimizeThread(this, m_bgCustomThdsManager, table, keepDict, true);
		m_bgCustomThdsManager->startBgThd(thd);
	}
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
void Database::doOptimize(Session *session, const char *path, bool keepDict, 
						  bool *cancelFlag) throw(NtseException) {
	acquireDDLLock(session, IL_IX, __FILE__, __LINE__);
	Table *table = NULL;

	//如果表没有打开，则先打开表
	try {
		table = openTable(session, path);
	} catch (NtseException &e) {
		releaseDDLLock(session, IL_IX);
		throw e;
	}
	assert((NULL != table));

	bool metaLock = false;
	try {
		table->lockMeta(session, IL_U, m_config->m_tlTimeout * 1000, __FILE__, __LINE__);
		metaLock = true;

		bool newHasDict = false;
		table->optimize(session, keepDict, &newHasDict, cancelFlag);

		//更新控制文件
		alterCtrlFileHasCprsDic(session, table->getTableDef()->m_id, newHasDict);
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
void Database::cancelBgCustomThd(uint connId) throw(NtseException) {
	m_bgCustomThdsManager->cancelThd(connId);
}

/**
 * 创建索引
 *
 * @param session 会话对象
 * @param table 操作的表
 * @param numIndice 要增加的索引个数，为online为false只能为1
 * @param indexDefs 新索引定义
 * @throw NtseException 加表锁超时，索引无法支持，增加唯一性索引时有重复键值等
 */
void Database::addIndex(Session *session, Table *table, u16 numIndice, const IndexDef **indexDefs) throw(NtseException) {
	assert(numIndice > 0);
	acquireDDLLock(session, IL_IX, __FILE__, __LINE__);
	try {
		table->addIndex(session, numIndice, indexDefs);
	} catch (NtseException &e) {
		releaseDDLLock(session, IL_IX);
		throw e;
	}
	releaseDDLLock(session, IL_IX);
}

/**
 * 创建压缩全局字典
 * @pre 表已经被打开
 * @param session 会话
 * @param table 要创建压缩字典的表
 * @throw NtseException 表没有被定义为压缩表，表已经含有全局字典，采样出错等
 */
void Database::createCompressDic(Session *session, Table *table) throw(NtseException) {
	acquireDDLLock(session, IL_IX, __FILE__, __LINE__);

	ILMode metaLockMode = IL_NO, dataLockMode = IL_NO;
	try {
		table->lockMeta(session, IL_U, 10000, __FILE__, __LINE__);
		metaLockMode = table->getMetaLock(session);

		TableDef *tableDef = table->getTableDef();
		if (!tableDef->m_isCompressedTbl)
			NTSE_THROW(NTSE_EC_GENERIC, "Table %s is not define as a compressed table", tableDef->m_name);
		if ((m_ctrlFile->hasCprsDict(tableDef->m_id)) && (!ParFileParser::isPartitionByPhyicTabName(tableDef->m_name))) {
			NTSE_THROW(NTSE_EC_GENERIC, "Table %s already had a global dictionary.", tableDef->m_name);
		} else if (!(m_ctrlFile->hasCprsDict(tableDef->m_id))) {
			table->createDictionary(session, &metaLockMode, &dataLockMode);
		}

	} catch (NtseException &e) {
		if (dataLockMode != IL_NO)
			table->unlock(session, dataLockMode);
		if (metaLockMode != IL_NO)
			table->unlockMeta(session, metaLockMode);
		releaseDDLLock(session, IL_IX);
		//重抛出异常
		throw e;
	}	
	if (dataLockMode != IL_NO)
		table->unlock(session, dataLockMode);
	if (metaLockMode != IL_NO)
		table->unlockMeta(session, metaLockMode);
	releaseDDLLock(session, IL_IX);
}

/**
 * 删除索引
 *
 * @param session 会话对象
 * @param table 操作的表
 * @param idx 要删除的是第几个索引
 * @throw NtseException 加表锁超时
 */
void Database::dropIndex(Session *session, Table *table, uint idx) throw(NtseException) {
	acquireDDLLock(session, IL_IX, __FILE__, __LINE__);
	try {
		table->dropIndex(session, idx);
	} catch (NtseException &e) {
		releaseDDLLock(session, IL_IX);
		throw e;
	}
	releaseDDLLock(session, IL_IX);
}

/** 更新表的flushLsn为当前日志尾
 * @param session 会话
 * @param tableId 表ID
 * @param flushed 表数据是否已经写出
 */
void Database::bumpFlushLsn(Session *session, u16 tableId, bool flushed) {
	// 先记录当前日志尾，再写LOG_BUMP_FLUSH_LSN日志，使得在恢复时LOG_BUMP_FLUSH_LSN日志本身
	// 不被跳过
	u64 tailLsn = m_txnlog->tailLsn();
	m_txnlog->flush(writeBumpFlushLsnLog(session, tableId, tailLsn, flushed));
	m_cbManager->callback(TABLE_ALTER, NULL, NULL, NULL, NULL);
	m_ctrlFile->bumpFlushLsn(tableId, tailLsn);
}

/** 更新表的flushLsn, tntFlushLsn为当前日志尾
 * @param session 会话
 * @param tableId 表ID
 * @param flushed 表数据是否已经写出
 */
void Database::bumpTntAndNtseFlushLsn(Session *session, u16 tableId, bool flushed) {
	// 先记录当前日志尾，再写LOG_BUMP_FLUSH_LSN日志，使得在恢复时LOG_BUMP_FLUSH_LSN日志本身
	// 不被跳过
	u64 tailLsn = m_txnlog->tailLsn();
	u64 lsn = writeBumpFlushLsnLog(session, tableId, tailLsn, flushed);
	m_txnlog->flush(lsn);
	m_ctrlFile->bumpTntAndNtseFlushLsn(tableId, tailLsn, tailLsn);
}

/** 重做设置flushLsn操作
 * @param session 会话
 * @param log LOG_BUMP_FLUSH_LSN日志
 */
void Database::redoBumpFlushLsn(Session *session, const LogEntry *log) {
	assert(log->m_logType == LOG_BUMP_FLUSHLSN);

	bool flushed;
	u64 flushLsn = parseBumpFlushLsnLog(log, &flushed);

	TableInfo *tableInfo = m_idToTables->get(log->m_tableId);
	if (tableInfo) {
		u64 tailLsn1 = m_txnlog->tailLsn();
		realCloseTable(session, tableInfo, flushed);
		u64 tailLsn2 = m_txnlog->tailLsn();
		assert_always(!flushed || tailLsn2 == tailLsn1);
	}

	if (m_ctrlFile->getFlushLsn(log->m_tableId) < flushLsn)
		m_ctrlFile->bumpFlushLsn(log->m_tableId, flushLsn);
	else {
		assert(m_ctrlFile->getFlushLsn(log->m_tableId) == flushLsn);
	}
}

/** 写LOG_BUMP_FLUSH_LSN日志
 * @param session 会话
 * @param tableId 表ID
 * @param flushLsn 新的flushLsn
 * @param flushed 表中的数据是否已经写出
 * @return 刚写的LOG_BUMP_FLUSH_LSN日志的LSN
 */
u64 Database::writeBumpFlushLsnLog(Session *session, u16 tableId, u64 flushLsn, bool flushed) {
	byte buf[sizeof(u64) + sizeof(bool)];
	Stream s(buf, sizeof(buf));
	s.write(flushLsn);
	s.write(flushed);
	return session->writeLog(LOG_BUMP_FLUSHLSN, tableId, buf, s.getSize());
}

/** 解析LOG_BUMP_FLUSH_LSN日志
 * @param log LOG_BUMP_FLUSH_LSN日志内容
 * @param flushed OUT，表中的数据是否已经写出
 * @return flushLsn
 */
u64 Database::parseBumpFlushLsnLog(const LogEntry *log, bool *flushed) {
	assert(log->m_logType == LOG_BUMP_FLUSHLSN);
	Stream s(log->m_data, log->m_size);
	u64 flushLsn;
	s.read(&flushLsn);
	s.read(flushed);
	return flushLsn;
}

/**
 * 返回日志管理器
 *
 * @return 日志管理器
 */
Txnlog* Database::getTxnlog() const {
	assert(!m_closed);
	return m_txnlog;
}

/**
 * 返回页面缓存管理器
 *
 * @return 页面缓存管理器
 */
Buffer* Database::getPageBuffer() const {
	assert(!m_closed);
	return m_buffer;
}

/**
 * 返回行锁管理器
 *
 * @return 行锁管理器
 */
LockManager* Database::getRowLockManager() const {
	assert(!m_closed);
	return m_rowLockMgr;
}

/**
 * 返回索引页锁管理器
 *
 * @return 索引页锁管理器
 */
IndicesLockManager* Database::getIndicesLockManager() const {
	assert(!m_closed);
	return m_idxLockMgr;
}

/**
 * 返回系统日志管理器
 *
 * @return 系统日志管理器
 */
Syslog* Database::getSyslog() const {
	assert(!m_closed);
	return m_syslog;
}

/**
 * 返回会话管理器
 *
 * @return 会话管理器
 */
SessionManager* Database::getSessionManager() const {
	assert(!m_closed);
	return m_sessionManager;
}

/**
 * 返回MMS系统
 *
 * @return MMS系统
 */
Mms* Database::getMms() const {
	assert(!m_closed);
	return m_mms;
}

/**
 * 汇报运行状态信息
 *
 * @param status 输出参数
 */
void Database::reportStatus(Status *status) {
	assert(!m_closed);
	m_status.m_pageBufSize = m_buffer->getCurrentSize();
	m_buffer->updateExtendStatus();
	m_status.m_bufStat = m_buffer->getStatus();
	m_status.m_bufPendingReads = m_buffer->getStatus().m_pendingReads.get();
	m_status.m_realDirtyPages = m_buffer->getStatus().m_curDirtyPages.get();
	m_status.m_mmsStat = m_mms->getStatus();
	m_status.m_logStat = m_txnlog->getStatus();
	m_status.m_rowlockStat = m_rowLockMgr->getStatus();
	*status = m_status;
}

/** 返回正在待处理的IO操作个数
 * @return 待处理的IO操作个数
 */
uint Database::getPendingIOOperations() const {
	assert(!m_closed);
	return m_buffer->getStatus().m_pendingReads.get() + m_buffer->getStatus().m_pendingWrites.get();
}

/** 返回已经进行的IO操作个数
 * @return 已经进行的IO操作个数
 */
u64 Database::getNumIOOperations() const {
	assert(!m_closed);
	return m_buffer->getStatus().m_physicalReads + m_buffer->getStatus().m_physicalWrites;
}

/** 打印数据库状态
 * @param out 输出流
 */
void Database::printStatus(ostream &out) {
	assert(!m_closed);
	MutexUsage::printAll(out);
	RWLockUsage::printAll(out);
	m_buffer->updateExtendStatus();
	m_buffer->printStatus(out);
	m_rowLockMgr->printStatus(out);
	m_mms->printStatus(out);
}

/** 唯一确定一个页面缓存分布项的键 */
struct BuKey {
	u16			m_tblId;	/** 表ID */
	DBObjType		m_type;		/** 对象类型 */
	const char	*m_idxName;	/** 仅在类型为BU_Index时指定索引名，否则为NULL */

	BuKey(u16 tblId, DBObjType type, const char *idxName) {
		assert(idxName);
		m_tblId = tblId;
		m_type = type;
		m_idxName = idxName;
	}
};

/** 计算BuKey对象哈希值的函数对象 */
class BkHasher {
public:
	unsigned int operator ()(const BuKey &key) const {
		unsigned int h = m_strHasher.operator()(key.m_idxName);
		return h + key.m_tblId + key.m_type;
	}

private:
	Hasher<const char *>	m_strHasher;
};

/** 计算BufUsage对象哈希值的函数对象 */
class BuHasher {
public:
	unsigned int operator ()(const BufUsage *usage) const {
		BuKey key(usage->m_tblId, usage->m_type, usage->m_idxName);
		return m_keyHasher.operator()(key);
	}
private:
	BkHasher	m_keyHasher;
};

/** 比如BuKey与BufUsagec对象是否相等的函数对象 */
class BkBuEqualer {
public:
	bool operator()(const BuKey &key, const BufUsage *usage) const {
		return !strcmp(key.m_idxName, usage->m_idxName)
			&& key.m_tblId == usage->m_tblId && key.m_type == usage->m_type;
	}
};

/**
 * 统计页面缓存分布信息
 * 注：正在进行在线模式修改操作的表的信息可能由于无法加锁则被忽略
 *
 * @param session 会话
 * @return 页面缓存分布信息
 */
Array<BufUsage *>* Database::getBufferDistr(Session *session) {
	assert(!m_closed && m_stat != DB_CLOSING && !m_unRecovered);

	DynHash<BuKey, BufUsage *, BuHasher, BkHasher, BkBuEqualer> usageHash;

	BufScanHandle *h = m_buffer->beginScan(session->getId(), NULL);
	const Bcb *bcb;
	while ((bcb = m_buffer->getNext(h)) != NULL) {
		McSavepoint mcSave(session->getMemoryContext());
		char *tblPath;
		const char *ext;
		// 从完整的文件路径中去掉basedir前缀和后缀名，提取出表路径
		const char *path = bcb->m_pageKey.m_file->getPath();
		string prefix(string(m_config->m_basedir) + NTSE_PATH_SEP);
		assert(!strncmp(path, prefix.c_str(), prefix.size()));
		ext = strrchr(path, '.');
		size_t pathLen = ext - path - prefix.size();
		tblPath = (char *)session->getMemoryContext()->alloc(pathLen + 1);
		memcpy(tblPath, path + prefix.size(), pathLen);
		tblPath[pathLen] = '\0';

		const char *idxName = "";
		DBObjType type = DBO_Unknown;
		u16 tblId = TableDef::INVALID_TABLEID;
		const char *schemaName = "";
		const char *tableName = "";

		Table *tblInfo = pinTableIfOpened(session, tblPath, 100);
		if (tblInfo) {
			// 在表被打开的过程中，表中的数据页可能在Buffer中存在，但pinTableIfOpened会
			// 返回NULL
			try {
				tblInfo->lockMeta(session, IL_S, 0, __FILE__, __LINE__);

				TableDef *tblDef = tblInfo->getTableDef(true, session);
				schemaName = tblDef->m_schemaName;
				tableName = tblDef->m_name;
				tblId = tblDef->m_id;

				if (!strcmp(ext, Limits::NAME_HEAP_EXT))
					type = DBO_Heap;
				else if (!strcmp(ext, Limits::NAME_LOBD_EXT))
					type = DBO_LlobDat;
				else if (!strcmp(ext, Limits::NAME_LOBI_EXT))
					type = DBO_LlobDir;
				else if (!strcmp(ext, Limits::NAME_SOBH_EXT))
					type = DBO_Slob;
				else if (!strcmp(ext, Limits::NAME_TMP_IDX_EXT)) {
					type = DBO_Unknown;
				} else {
					assert(!strcmp(ext, Limits::NAME_IDX_EXT));
					int idx = tblInfo->getIndice()->getIndexNo(bcb->m_page, bcb->m_pageKey.m_pageId);
					if (idx >= 0) {
						type = DBO_Index;
						// 正在建索引的时候返回的idx可能表示正在建的那个索引，这时还没有更新TableDef
						if (idx < tblDef->m_numIndice)
							idxName = tblDef->m_indice[idx]->m_name;
						else
							idxName = "Creating";
					} else
						type = DBO_Indice;
				}
			} catch (NtseException &) {
			}
		}
		m_buffer->releaseCurrent(h);

		BufUsage *usage = usageHash.get(BuKey(tblId, type, idxName));
		if (!usage) {
			usage = new BufUsage(tblId, schemaName, tableName, type, idxName);
			usageHash.put(usage);
		}
		usage->m_numPages++;

		if (tblInfo) {
			if (tblInfo->getMetaLock(session) != IL_NO)
				tblInfo->unlockMeta(session, tblInfo->getMetaLock(session));
			closeTable(session, tblInfo);
		}
	}
	m_buffer->endScan(h);

	Array<BufUsage *> *distr = new Array<BufUsage *>();
	for (size_t i = 0; i < usageHash.getSize(); i++) {
		distr->push(usageHash.getAt(i));
	}
	return distr;
}

/**
 * 创建一个连接（从连接池中获取）
 *
 * @param internal 是否为内部连接，内部连接指的为了执行与用户请求没有
 *   直接关系的操作（如检查点、MMS刷更新缓存等）而获取的连接
 * @param name 连接名称
 * @return 数据库连接
 */
Connection* Database::getConnection(bool internal, const char *name) {
	ftrace(ts.dml || ts.ddl, tout << internal);
	assert(!m_closed);
	return m_sessionManager->getConnection(internal, &m_config->m_localConfigs, &m_status.m_opStat, name);
}

/**
 * 释放一个连接（到连接池）
 * @pre 要释放的连接conn为使用中的连接
 *
 * @param 数据库连接
 */
void Database::freeConnection(Connection *conn) {
	ftrace(ts.dml || ts.ddl, tout << conn);
	assert(!m_closed);
	m_sessionManager->freeConnection(conn);
}

/**
 * 设置是否允许进行检查点。如果设置不允许进行检查点且当前正在进行检查点，
 * 则本函数会等待当前检查点操作完成后再返回，同时也不允许直接调用checkpoint
 * 函数进行检查点操作，保证函数返回后不可能有进行中的检查点操作存在。
 *
 * @param enabled 是否定期进行检查点
 */
void Database::setCheckpointEnabled(bool enabled) {
	ftrace(ts.ddl || ts.dml, tout << enabled);
	assert(!m_closed);
	if (enabled)
		m_chkptTask->resume();
	else if (m_chkptTask) {
		m_chkptTask->pause(true);
	}
}

/**
 * 设置新的检查点执行周期
 *
 * @param sec 新指定的检查点周期，单位秒
 */
void Database::setCheckpointInterval(uint sec) {
	m_chkptTask->setInterval(min(sec, (uint)10) * 1000);
	((Checkpointer *)m_chkptTask)->setInterval((u32)sec);
}

/**
 * 立即执行一次检查点操作
 *
 */
void Database::doCheckpoint() {
	((Checkpointer *)m_chkptTask)->setInstant(true);
	m_chkptTask->signal();
}

/**
 * 进行一次检查点
 *
 * @param session 会话
 * @param targetTime 建议检查点在指定的时间点之前完成，单位秒
 * @throw NtseException 检查点被禁用，或者操作被取消
 */
void Database::checkpoint(Session *session, AioArray *array) throw(NtseException) {
	ftrace(ts.ddl || ts.dml, tout << session);
	assert(!m_closed && !m_unRecovered);

	m_status.m_checkpointCnt++;

	if (m_chkptTask->isPaused())
		NTSE_THROW(NTSE_EC_GENERIC, "Checkpointing has been disabled.");

	MutexGuard guard(m_chkptLock, __FILE__, __LINE__);


	// 计算恢复起始点，若有活跃事务则为活跃事务最小的起始LSN，
	// 否则为日志尾
	LsnType redoStartLsn = m_txnlog->tailLsn();
	assert(redoStartLsn != INVALID_LSN);

	LsnType minTxnStartLsn = m_sessionManager->getMinTxnStartLsn();
	if (minTxnStartLsn != INVALID_LSN) {
		assert(minTxnStartLsn <= m_txnlog->lastLsn());
		redoStartLsn = minTxnStartLsn;
	}

	LsnType lastLsn = m_txnlog->lastLsn();
	if (lastLsn != INVALID_LSN)
		m_txnlog->flush(lastLsn, FS_CHECK_POINT);

	m_syslog->log(EL_LOG, "Checkpoint begins, set redo start lsn to "I64FORMAT"d.", redoStartLsn);

	vector<string> pathes = getOpenedTablePaths(session);

	// 刷MMS，期间新建的表的日志一定在恢复起始日志之后，没有问题
	for (size_t i = 0; i < pathes.size(); i++) {
		Table *tableInfo = pinTableIfOpened(session, pathes[i].c_str());
		if (tableInfo) {
			tableInfo->lockMeta(session, IL_S, -1, __FILE__, __LINE__);
			if (tableInfo->getTableDef(true, session)->m_useMms) {
				m_syslog->log(EL_LOG, "Flush dirty records in MMS of table '%s'.", pathes[i].c_str());

				u64 thisDirtyMmsRecs = tableInfo->getMmsTable()->getStatus().m_dirtyRecords;
				m_syslog->log(EL_LOG, "MMS Dirty Records For Table %s: "I64FORMAT"u", tableInfo->getPath(), thisDirtyMmsRecs);

				try {
					tableInfo->getMmsTable()->flush(session, false, false);
				} catch (NtseException &e) {
					assert(e.getErrorCode() == NTSE_EC_CANCELED);
					tableInfo->unlockMeta(session, IL_S);
					closeTable(session, tableInfo);
					throw e;
				}

				if (tableInfo->getLobStorage()) {
					thisDirtyMmsRecs = tableInfo->getLobStorage()->getSLMmsTable()->getStatus().m_dirtyRecords;
					m_syslog->log(EL_LOG, "Lob MMS Dirty Records For Table %s: "I64FORMAT"u", tableInfo->getPath(), thisDirtyMmsRecs);

					try {
						tableInfo->getLobStorage()->getSLMmsTable()->flush(session, false, false);
					} catch (NtseException &e) {
						assert(e.getErrorCode() == NTSE_EC_CANCELED);
						tableInfo->unlockMeta(session, IL_S);
						closeTable(session, tableInfo);
						throw e;
					}
				}
			}
			tableInfo->unlockMeta(session, IL_S);
			closeTable(session, tableInfo);
		}
	}

	m_syslog->log(EL_LOG, "Flush dirty buffer pages.");
	// 异常直接抛出
#ifndef WIN32
	if(m_config->m_aio)
		m_buffer->flushAllUseAio(session, array);
	else
#endif
		m_buffer->flushAll(session);

	m_txnlog->setCheckpointLsn(redoStartLsn);

	m_syslog->log(EL_LOG, "Checkpoint finished.");
}

/**
 * 得到当前被打开的表的路径集合
 *
 * @param session 会话
 * @return 当前被打开的表的路径（相对于basedir）集合
 */
vector<string> Database::getOpenedTablePaths(Session *session) {
	assert(!m_closed);

	acquireATSLock(session, IL_S, -1, __FILE__, __LINE__);

	TableInfo **tables = new TableInfo*[m_idToTables->getSize()];
	m_idToTables->values(tables);

	vector<string> pathes;
	for (size_t i = 0; i < m_idToTables->getSize(); i++)
		pathes.push_back(tables[i]->m_path);

	delete []tables;

	releaseATSLock(session, IL_S);

	return pathes;
}

#ifdef TNT_ENGINE
/**
 * 得到当前被打开的表的集合
 *
 * @param session 会话
 * @return 当前被打开的表的（相对于basedir）集合
 */
vector<Table *> Database::getOpenTables(Session *session) {
	assert(!m_closed);

	acquireATSLock(session, IL_S, -1, __FILE__, __LINE__);

	TableInfo **tableInfos = new TableInfo*[m_idToTables->getSize()];
	m_idToTables->values(tableInfos);

	vector<Table *> tables;
	for (size_t i = 0; i < m_idToTables->getSize(); i++)
		tables.push_back(tableInfos[i]->m_table);

	delete []tableInfos;
	releaseATSLock(session, IL_S);

	return tables;
}
#endif

/**
 * 如果指定的表被打开则增加其引用计数
 *
 * @param session 会话
 * @param path 表路径，相对于basedir，大小写不敏感
 * @param 超时时间，，若为<0则不超时，若为0则马上超时，相当于tryLock，>0为毫秒为单位的超时时间
 * @return 若表被打开则返回表信息，否则返回NULL
 */
Table* Database::pinTableIfOpened(Session *session, const char *path, int timeoutMs) {
	ftrace(ts.ddl || ts.dml, tout << path);
	assert(!m_closed && !m_unRecovered);

	try {
		acquireATSLock(session, IL_X, timeoutMs, __FILE__, __LINE__);
	} catch (NtseException &) {
		return NULL;
	}

	TableInfo *tableInfo = m_pathToTables->get((char *)path);
	if (tableInfo) {
		tableInfo->m_refCnt++;
		// 如果refCnt从0提升为1，那么调整链表中的位置
		if (tableInfo->m_refCnt == 1) {
			m_openTablesLink->moveToLast(&tableInfo->m_table->getOpenTablesLink());
		}
		releaseATSLock(session, IL_X);
		return tableInfo->m_table;
	}
	releaseATSLock(session, IL_X);
	return NULL;
}

/**
 * 获得控制文件
 *
 * @return 控制文件
 */
ControlFile* Database::getControlFile() const {
	assert(!m_closed);
	return m_ctrlFile;
}

/**
 * 进行故障恢复
 *
 * @throw NtseException 一些DDL操作redo时会失败
 */
void Database::recover() throw(NtseException) {
	assert(m_stat == DB_MOUNTED);
	ftrace(ts.recv, );
	m_stat = DB_RECOVERING;
	u64 checkpointLsn = m_ctrlFile->getCheckpointLSN();

	m_syslog->log(EL_LOG, "Start database recovery from LSN: "I64FORMAT"d.", checkpointLsn);

	m_activeTransactions = new Hash<u16, Transaction *>(m_config->m_maxSessions * 2);

	// 回放日志
	LogScanHandle *logScan = m_txnlog->beginScan(checkpointLsn, m_txnlog->tailLsn());
	int oldPercent = 0;
	int oldTime = System::fastTime();
	u64 lsnRange = m_txnlog->tailLsn() - checkpointLsn+ 1;
	while (m_txnlog->getNext(logScan)) {
		LsnType lsn = logScan->curLsn();
		const LogEntry *logEntry = logScan->logEntry();
#ifdef TNT_ENGINE
		//如果是tnt日志，直接跳过
		if (logEntry->isTNTLog()) {
			continue;
		}
#endif
		LogType type = logEntry->m_logType;
		nftrace(ts.recv, tout << "Got log: " << lsn << "," << logEntry);

		// 最多每10秒打印一次恢复进度信息
		int percent = (int)((lsn - checkpointLsn) * 100 / lsnRange);
		if (percent > oldPercent && (System::fastTime() - oldTime) > 10) {
			m_syslog->log(EL_LOG, "Done replay of %d%% logs.", percent);
			oldPercent = percent;
			oldTime = System::fastTime();
		}
		
		if (canSkipLog(lsn, logEntry))
			continue;

		if (logEntry->m_txnId == 0) {	// 非事务操作
			Connection *conn = getConnection(true);
			Session *session = m_sessionManager->allocSession("Database::recover", conn);
			try {
				if (type == LOG_DROP_TABLE) {
					redoDropTable(session, logEntry);
				} else if (type == LOG_RENAME_TABLE) {
					redoRenameTable(session, logEntry);
				} else if (type == LOG_IDX_DROP_INDEX) {
					redoDropIndex(session, lsn, logEntry);
				} else if (type == LOG_TRUNCATE) {
					redoTruncate(session, logEntry);
				} else if (type == LOG_BUMP_FLUSHLSN) {
					redoBumpFlushLsn(session, logEntry);
				} else if (type == LOG_ALTER_TABLE_ARG) {
					redoAlterTableArg(session, logEntry);
				} else if (type == LOG_LOB_MOVE) {
					redoMoveLobs(session, lsn, logEntry);
				} else if (type == LOG_ALTER_INDICE){
					redoAlterIndice(session, logEntry);
				} else if (type == LOG_ALTER_COLUMN) {
					redoAlterColumn(session, logEntry);
				} else if (type == LOG_CREATE_DICTIONARY) {
					redoCreateCompressDict(session, logEntry);
				} else {
					assert(type == LOG_CREATE_TABLE);
					redoCreateTable(session, logEntry);
				}
			} catch (NtseException &e) {
				m_sessionManager->freeSession(session);
				freeConnection(conn);
				m_activeTransactions->clear();
				delete m_activeTransactions;
				m_txnlog->endScan(logScan);
				throw e;
			}
			m_sessionManager->freeSession(session);
			freeConnection(conn);
		} else if (type == LOG_TXN_START) {
			try {
				startTransaction(logEntry);
			} catch (NtseException &e) {
				m_activeTransactions->clear();
				delete m_activeTransactions;
				m_txnlog->endScan(logScan);
				throw e;
			}
		} else if (type == LOG_TXN_END) {
			endTransaction(logEntry);
		} else {
			Transaction *trans = m_activeTransactions->get(logEntry->m_txnId);
			assert(trans);
			trans->redo(lsn, logEntry);
		}
	}
	m_txnlog->endScan(logScan);

	m_syslog->log(EL_LOG, "Reconstruct some meta information of all tables.", (int)m_idToTables->getSize());
	// 非正常关闭时堆需要进行重建中央位图等工作
	u16 numTables = m_ctrlFile->getNumTables();
	u16 *tableIds = new u16[numTables];
	m_ctrlFile->listAllTables(tableIds, numTables);

	for (u16 i = 0; i < numTables; i++) {
		Connection *conn = getConnection(true);
		Session *session = m_sessionManager->allocSession("Database::recover", conn);

		string path = m_ctrlFile->getTablePath(tableIds[i]);
		Table *table = openTable(session, path.c_str());
		table->getHeap()->redoFinish(session);
		if (table->getLobStorage())
			table->getLobStorage()->getSLHeap()->redoFinish(session);
		closeTable(session, table);

		m_sessionManager->freeSession(session);
		freeConnection(conn);
	}

	delete [] tableIds;

	m_syslog->log(EL_LOG, "Replay log finished, complete %d active transactions(phase 1).", (int)m_activeTransactions->getSize());

	size_t numActiveTrans = m_activeTransactions->getSize();
	Transaction **txnArr = new Transaction *[numActiveTrans];
	m_activeTransactions->values(txnArr);
	for (u16 i = 0; i < numActiveTrans; i++) {
		bool callPhase2 = txnArr[i]->completePhase1(false, false);
		if (!callPhase2) {
			m_activeTransactions->remove(txnArr[i]->getId());
			delete txnArr[i];
		}
	}
	delete []txnArr;

	m_syslog->log(EL_LOG, "Complete %d active transactions(phase 2).", (int)m_activeTransactions->getSize());
	numActiveTrans = m_activeTransactions->getSize();
	txnArr = new Transaction *[numActiveTrans];
	m_activeTransactions->values(txnArr);
	for (u16 i = 0; i < numActiveTrans; i++) {
		txnArr[i]->completePhase2();
		delete txnArr[i];
	}
	delete []txnArr;

	m_activeTransactions->clear();
	delete m_activeTransactions;
	
	m_syslog->log(EL_LOG, "Flush dirty data of %d tables.", m_idToTables->getSize());
	closeOpenTables(true);
	m_ctrlFile->cleanUpTempFiles();

	m_mms->endRedo();

	m_syslog->log(EL_LOG, "Database recovery finished.");
}

/** 验证数据库正确性
 * @pre 恢复已经完成
 */
void Database::verify() {
	assert(m_stat == DB_RUNNING);
	
	u16 numTables = m_ctrlFile->getNumTables();
	u16 *tableIds = new u16[numTables];
	m_ctrlFile->listAllTables(tableIds, numTables);

	m_syslog->log(EL_LOG, "Verify database, %d tables", (int)numTables);

	for (u16 i = 0; i < numTables; i++) {
		Connection *conn = getConnection(true);
		Session *session = m_sessionManager->allocSession("Database::verify", conn);

		string path = m_ctrlFile->getTablePath(tableIds[i]);
		Table *table = openTable(session, path.c_str());
		try {
			table->verify(session);
		} catch (NtseException &e) {
			m_syslog->log(EL_PANIC, "%s", e.getMessage());
			assert_always(false);
		}
		closeTable(session, table);

		m_sessionManager->freeSession(session);
		freeConnection(conn);
	}

	delete [] tableIds;
}

#ifdef TNT_ENGINE
/** 检查该session是否具有操作这张表的权限
 * @注意点，这个函数只打开tableDef文件，也未加任何的lock，目前只用于drop和rename waitrealclose后调用
 *          因为这时table不会被别的连接打开，未加meta lock获取tabledef信息是安全的
 * @param 会话
 * @tableName 判断权限的表
 */
void Database::checkPermission(Session *session, const char *path) throw(NtseException) {
	string basePath = string(m_config->m_basedir) + NTSE_PATH_SEP + path;
	string tblDefPath = basePath + Limits::NAME_TBLDEF_EXT;
	TableDef *tableDef = TableDef::open(tblDefPath.c_str());
	TableStatus tblStatus = tableDef->getTableStatus();
	delete tableDef;

	bool trxConn = session->getConnection()->isTrx();
	Table::checkPermissionReal(trxConn, tblStatus, path);
	/*if (tblStatus == TS_CHANGING) {
		assert(false);
		NTSE_THROW(NTSE_EC_NOT_SUPPORT, "The table(%s) status is changing", path);
	} else if (tblStatus == TS_ONLINE_DDL) {
		NTSE_THROW(NTSE_EC_NOT_SUPPORT, "The table(%s) is online ddl operation", path);
	} else if (trxConn && tblStatus == TS_NON_TRX) {
		NTSE_THROW(NTSE_EC_NOT_SUPPORT, "Transactional Connection can't operate Non-Transaction Table(%s)", path);
	} else if (!trxConn && tblStatus == TS_TRX) {
		NTSE_THROW(NTSE_EC_NOT_SUPPORT, "Non-Transactional Connection can't operate Transaction Table(%s)", path);
	} else if (!trxConn && tblStatus == TS_SYSTEM) {
		assert(false);
		NTSE_THROW(NTSE_EC_NOT_SUPPORT, "Non-Transactional Connection can't operate System Table(%s)", path);
	}*/
}
#endif

/** 判断日志是否可以被跳过
 * @param lsn 日志LSN
 * @param log 日志
 * @return 日志是否可以被跳过
 */
bool Database::canSkipLog(u64 lsn, const LogEntry *log) {
	assert(log->m_tableId != TableDef::INVALID_TABLEID);
	u16 tableId = log->m_tableId;
	if (TableDef::tableIdIsVirtualLob(tableId))
		tableId = TableDef::getNormalTableId(tableId);

	// 检查表是否已经被删除，表如果被删除则所有操作都不需要重做，包括DROP TABLE操作
	if (m_ctrlFile->isDeleted(tableId)) {
		nftrace(ts.recv, tout << "Skip log " << lsn << "because table " << log->m_tableId << " has been dropped");
		return true;
	}
	if (log->m_logType == LOG_CREATE_TABLE) {
		// 由于更新控制文件是创建表的最后一步，因此若表ID在控制文件中已经存在则表示建表
		// 操作已经成功，恢复时可跳过
		if (!m_ctrlFile->getTablePath(tableId).empty()) {
			nftrace(ts.recv, tout << "Skip CREATE TABLE log " << lsn << "because table " << log->m_tableId << " already exists");
			return true;
		}
	} else {
		assert(!m_ctrlFile->getTablePath(tableId).empty());	// 表没有被删除，又不是建表日志，则表一定存在
		// 如果表flushLsn > lsn，则当前日志的修改已经持久化，跳过
		if (m_ctrlFile->getFlushLsn(tableId) > lsn) {
			nftrace(ts.recv, tout << "Skip log " << lsn << " because flushLsn " << m_ctrlFile->getFlushLsn(tableId)
				<< " of table " << tableId << " is ahead of it");
			return true;
		}

		if (log->m_txnId && log->m_logType != LOG_TXN_START) {
			Transaction *trans = m_activeTransactions->get(log->m_txnId);
			if (!trans) {
				// 这里有两种可能，一是因为事务开始日志被跳过，二是事务跨恢复起始点，
				// 进行检查点时设置恢复起始点为活跃事务起始LSN的最小值，因此若事务跨恢复
				// 起始点则该事务一定已经持久化，可以跳过
				nftrace(ts.recv, tout << "Skip log " << lsn << " because transaction is incomplete");
				return true;
			}
		}
	}
	return false;
}

/**
 * 根据ID获取表，若表没有被打开自动打开表。当前只会在恢复时调用这一函数
 *
 * @param session 会话
 * @param tableId 表ID
 * @throw NtseException 打开表失败
 * @return 表，若不存在返回NULL
 */
Table* Database::getTable(u16 tableId) throw (NtseException) {
	NTSE_ASSERT(m_stat == DB_RECOVERING);

	TableInfo *tableInfo = m_idToTables->get(tableId);
	Table *ret;
	if (!tableInfo) {
		string pathStr = m_ctrlFile->getTablePath(tableId);
		if (pathStr.empty())
			return NULL;
		const char* path = pathStr.c_str();
		Connection *conn = getConnection(true);
		Session *session = m_sessionManager->allocSession("Database::getTable", conn);
		try {
			ret = openTable(session, path);
		} catch (NtseException &e) {
			m_sessionManager->freeSession(session);
			freeConnection(conn);
			throw e;
		}
		m_sessionManager->freeSession(session);
		freeConnection(conn);
	} else
		ret = tableInfo->m_table;
	return ret;
}

/**
 * 执行真正的关闭表操作
 *
 * @post tableInfo被delete
 *
 * @param session 会话
 * @param tableInfo 表信息
 * @param flushDirty 是否刷出脏数据
 */
void Database::realCloseTable(Session *session, TableInfo *tableInfo, bool flushDirty) {
	ftrace(ts.ddl || ts.dml, tout << session << tableInfo->m_path << flushDirty);

/*#ifdef TNT_ENGINE
	if (session->getTrans() == NULL) {
#endif*/
		assert(tableInfo->m_table->getMetaLock(session) == IL_X || m_stat == DB_RECOVERING);
/*#ifdef TNT_ENGINE
	}
#endif*/

	u16 tableID = tableInfo->m_table->getTableDef(true, session)->m_id;
	m_idToTables->remove(tableID);
	m_pathToTables->remove(tableInfo->m_path);

#ifdef NTSE_KEYVALUE_SERVER
	/*
	 * ntsebinlog中keyvalue打开的表需要删除
	*/
	KeyValueServer::closeMysqlTable(tableID);
#endif

	// 将表从openTableLRU 链表中摘除
	tableInfo->m_table->getOpenTablesLink().unLink();

	tableInfo->m_table->close(session, flushDirty);

	m_status.m_realCloseTableCnt++;

	delete tableInfo->m_table;
	delete tableInfo;
}

/**
 * 等待表真正被关闭。这一函数用在需要对表关闭之后才能进行的DROP/RENAME等操作，这是由于
 * 即使应用已经关闭了表，表仍然会被备份、检查点等任务暂时pin住没有真正关闭
 *
 * @pre 已经加了ATS锁S锁
 * @post ATS锁仍然持有。本函数执行期间ATS锁可能被释放
 *
 * @param session 会话
 * @param path 表路径。大小写不敏感
 * @param timeoutSec 超时时间，单位秒，为<0表示不超时，0表示马上超时，>0为超时时间
 * @throw NtseException 超时了
 */
void Database::waitRealClosed(Session *session, const char *path, int timeoutSec) throw(NtseException) {
#ifdef TNT_ENGINE
	if (session->getTrans() == NULL) {
#endif
		assert(getATSLock(session) == IL_S);
#ifdef TNT_ENGINE
	}
#endif

	ftrace(ts.ddl || ts.dml, tout << path);

	u32 before = System::fastTime();
	bool first = true;
	while (true) {
		TableInfo *tableInfo = m_pathToTables->get((char *)path);
		if (!tableInfo)
			break;

		if (first)
			m_syslog->log(EL_WARN, "Table '%s' is used, wait and retry.", path);

		if (timeoutSec == 0 || (timeoutSec > 0 && (u32)timeoutSec < (System::fastTime() - before))) {
			nftrace(ts.ddl || ts.dml, tout << "Timeout");
			NTSE_THROW(NTSE_EC_LOCK_TIMEOUT, "waitRealClosed timeout");
		}
		releaseATSLock(session, IL_S);
		
		// 加ATS排他锁尝试关闭refCnt为0的表
		acquireATSLock(session, IL_X, -1, __FILE__, __LINE__);
		tableInfo = m_pathToTables->get((char *)path);
		if (tableInfo) {	
			try {
				realCloseTableIfNeed(session, tableInfo->m_table);
			} catch (NtseException &e) {
				// 关表抛错，一般是由于刷脏数据时发生I/O异常
				releaseATSLock(session, IL_X);
				getSyslog()->log(EL_PANIC, "CloseOpenTable Error: %s", e.getMessage());
			}
		}
		releaseATSLock(session, IL_X);

		Thread::msleep(1000);
		first = false;
		acquireATSLock(session, IL_S, -1, __FILE__, __LINE__);
	}
}

/**
 * 重做DROP INDEX操作
 *
 * @param session 会话
 * @param lsn LOG_DROP_INDEX日志LSN
 * @param log LOG_DROP_INDEX日志内容
 */
void Database::redoDropIndex(Session *session, LsnType lsn, const LogEntry *log) {
	ftrace(ts.recv, tout << session << lsn << log);
	assert(log->m_logType == LOG_IDX_DROP_INDEX);
	Table *table = getTable(log->m_tableId);
	table->redoDropIndex(session, lsn, log);
}

/**
 * 重做RENAME TABLE操作
 *
 * @param session 会话
 * @param log LOG_RENAME_TABLE日志
 * @throw NtseException 文件操作失败等
 */
void Database::redoRenameTable(Session *session, const LogEntry *log) throw(NtseException) {
	ftrace(ts.recv, tout << session << log);
	assert(log->m_logType == LOG_RENAME_TABLE);
	assert(!m_idToTables->get(log->m_tableId));

	char *oldPath = NULL, *newPath = NULL;
	bool hasLob;
	bool hasCprsDict;
	parseRenameLog(log, &oldPath, &newPath, &hasLob, &hasCprsDict);
	Table::rename(this, session, oldPath, newPath, hasLob, true, hasCprsDict);
	m_ctrlFile->renameTable(oldPath, newPath);

	delete []oldPath;
	delete []newPath;
}

/**
 * 重做表参数修改操作。
 *
 * @param session	会话。
 * @param log		LOG_ALTER_TABLE_ARG日志。
 * @throw	NtseException 自定义表参数修改操作失败等。
 */
void Database::redoAlterTableArg(Session *session, const LogEntry *log) throw(NtseException) {
	ftrace(ts.recv, tout << session << log);
	assert(LOG_ALTER_TABLE_ARG == log->m_logType);	
	Table *table = getTable(log->m_tableId);
	table->redoAlterTableArg(session, log);
}

/**
 * 重做表索引修改。
 *
 * @param session	会话。
 * @param log		LOG_ALTER_INDICE日志。
 * @throw			日志解析错误，无法打开堆文件时抛出异常
 */
void Database::redoAlterIndice(Session *session, const LogEntry *log) throw(NtseException) {
	ftrace(ts.recv, tout << session << log);
	assert(LOG_ALTER_INDICE == log->m_logType);
	assert(!m_idToTables->get(log->m_tableId));
	Table::redoAlterIndice(session, log, this, m_ctrlFile->getTablePath(log->m_tableId).c_str());
}

/**
* 重做表列修改。
*
* @param session	会话。
* @param log		LOG_ALTER_COLUMN日志。
*/
void Database::redoAlterColumn(ntse::Session *session, const ntse::LogEntry *log) {
	ftrace(ts.recv, tout << session << log);
	assert(LOG_ALTER_COLUMN == log->m_logType);
	assert(!m_idToTables->get(log->m_tableId));
	bool newHasDict = false;
	string tblPath = m_ctrlFile->getTablePath(log->m_tableId);
	Table::redoAlterColumn(session, log, this, tblPath.c_str(), &newHasDict);
	alterCtrlFileHasCprsDic(session, log->m_tableId, newHasDict);
}

/**
 * 重做创建压缩字典
 * 
 * @param session 会话
 * @param log     LOG_CREATE_DICTIONARY日志项
 */
void Database::redoCreateCompressDict(ntse::Session *session, const ntse::LogEntry *log) {
	ftrace(ts.recv, tout << session << log);
	assert(LOG_CREATE_DICTIONARY == log->m_logType);

	//修改控制文件中是否有字典标记为true
	string tablePath = m_ctrlFile->getTablePath(log->m_tableId);
	assert(!tablePath.empty());
	alterCtrlFileHasCprsDic(session, log->m_tableId, true);

	Table::redoCreateDictionary(session, this, tablePath.c_str());

	//如果表被打开，强制关闭之以在表被替换表组件之后不会出错
	TableInfo *tableInfo = m_idToTables->get(log->m_tableId);
	if (tableInfo)
		realCloseTable(session, tableInfo, false);
	assert(NULL == m_idToTables->get(log->m_tableId));
}

/**
 * 重做表大对象整理修改操作。
 *
 * @param session	会话。
 * @param lsn		日志号
 * @param log		LOG_LOB_MOVE日志。
 * @throw			操作失败。
 */
void Database::redoMoveLobs(Session *session, LsnType lsn,  const LogEntry *log) throw(NtseException) {
	ftrace(ts.recv, tout << session << log);
	assert(LOG_LOB_MOVE == log->m_logType);
	Table *table = getTable(log->m_tableId);
	try {
		table->redoMoveLobs(session, lsn, log);
	} catch (NtseException & e)	 {
		closeTable(session, table);
		throw e;
	}
	closeTable(session, table);
}

/**
 * 开始一个事务
 *
 * @param log LOG_TXN_START日志内容
 * @throw NtseException 得不到指定的会话
 */
void Database::startTransaction(const LogEntry *log) throw(NtseException) {
	ftrace(ts.recv, tout << log);
	assert(log->m_logType == LOG_TXN_START);
	assert(!m_activeTransactions->get(log->m_txnId));

	TxnType type = Session::parseTxnStartLog(log);

	Table *table = NULL;
	u16 tableId = log->m_tableId;
	if (TableDef::tableIdIsVirtualLob(tableId))
		tableId = TableDef::getNormalTableId(tableId);
	table = getTable(tableId);
	assert(table);
	Transaction *trans = Transaction::createTransaction(type, this, log->m_txnId, log->m_tableId, table);
	m_activeTransactions->put(log->m_txnId, trans);
}

/**
 * 结束一个事务。如果指定的事务不存在则不进行任何操作，如果指定的事务
 * 存在，而没有完成，本函数将根据事务类型及状态自动完成或回滚该事务。
 *
 * @param log LOG_TXN_END日志内容
 */
void Database::endTransaction(const LogEntry *log) {
	ftrace(ts.recv, tout << log);
	assert(log->m_logType == LOG_TXN_END);

	Transaction *trans = m_activeTransactions->remove(log->m_txnId);
	assert(trans);
	bool callPhase2 = trans->completePhase1(true, Session::parseTxnEndLog(log));
	NTSE_ASSERT(!callPhase2);
	delete trans;
}

/** 加活跃表集合锁
 * @param session 会话
 * @param mode 模式，只能为IL_S或IL_X
 * @param timeoutMs <0表示不超时，0在不能立即得到锁时马上超时，>0为毫秒为单位的超时时间
 * @param file 代码文件
 * @param line 代码行号
 * @throw NtseException 加锁超时，如果指定timeousMs为<0不会抛出异常
 */
void Database::acquireATSLock(Session *session, ILMode mode, int timeoutMs, const char *file, uint line) throw(NtseException) {
#ifdef TNT_ENGINE
	if (session->getTrans() != NULL) {
		UNREFERENCED_PARAMETER(mode);
		UNREFERENCED_PARAMETER(timeoutMs);
		UNREFERENCED_PARAMETER(file);
		UNREFERENCED_PARAMETER(line);
	}
	else {
#endif
		assert(mode == IL_S || mode == IL_X);
		ftrace(ts.ddl || ts.dml, tout << session << mode << timeoutMs);
		if (!m_atsLock->lock(session->getId(), mode, timeoutMs, file, line)) {
			nftrace(ts.ddl | ts.dml, tout << "Lock failed");
			NTSE_THROW(NTSE_EC_LOCK_TIMEOUT, "Unable to acquire ATS lock.");
		}
		session->addLock(m_atsLock);
#ifdef TNT_ENGINE
	}
#endif
	assert(mode == getATSLock(session));
}

/** 释放活跃表集合锁
 * @param session 会话
 * @param mode 模式，只能为IL_S或IL_X
 */
void Database::releaseATSLock(Session *session, ILMode mode) {
	assert(mode == getATSLock(session));
#ifdef TNT_ENGINE
	if (session->getTrans() != NULL) {
		UNREFERENCED_PARAMETER(mode);
	}
	else {
#endif
		assert(mode == IL_S || mode == IL_X);
		ftrace(ts.ddl || ts.dml, tout << session << mode);
		m_atsLock->unlock(session->getId(), mode);
		session->removeLock(m_atsLock);
#ifdef TNT_ENGINE
	}
#endif
}

/** 得到指定会话所加的活跃表集合锁模式
 * @param session 会话
 * @return 指定会话所加的活跃表集合锁模式，可能是IL_NO、IL_S或IL_X
 */
ILMode Database::getATSLock(Session *session) const {
	return m_atsLock->getLock(session->getId());
}

/** 加DDL锁。CREATE/DROP/RENAME操作时加X锁，TRUNCATE/ADD INDEX等修改表结构的操作时需要加
 * IX锁，在线备份时需要加S锁，从而防止在备份期间进行任何DDL操作
 *
 * @param session 会话
 * @param mode 锁模式
 * @throw NtseException 加锁超时，超时时间由数据库配置中的m_tlTimeout指定
 */
void Database::acquireDDLLock(Session *session, ILMode mode, const char *file, uint line) throw(NtseException) {
#ifdef TNT_ENGINE
	if (session->getTrans() != NULL) {
		UNREFERENCED_PARAMETER(mode);
		UNREFERENCED_PARAMETER(file);
		UNREFERENCED_PARAMETER(line);
	}
	else {
#endif
		ftrace(ts.ddl || ts.dml, tout << session << mode);
		if (!m_ddlLock->lock(session->getId(), mode, m_config->m_tlTimeout * 1000, file, line))
			NTSE_THROW(NTSE_EC_LOCK_TIMEOUT, "Unable to acquire DDL lock in %s mode.", IntentionLock::getLockStr(mode));
		session->addLock(m_ddlLock);
#ifdef TNT_ENGINE
	}
#endif	
	assert(getDDLLock(session) == mode);
}

/**
 * 释放DDL锁
 *
 * @param session 会话
 * @param mode 锁模式
 */
void Database::releaseDDLLock(Session *session, ILMode mode) {
	assert(getDDLLock(session) == mode);
#ifdef TNT_ENGINE
	if (session->getTrans() != NULL) {
		UNREFERENCED_PARAMETER(mode);
	}
	else {
#endif
		ftrace(ts.ddl || ts.dml, tout << session << mode);
		m_ddlLock->unlock(session->getId(), mode);
		session->removeLock(m_ddlLock);
#ifdef TNT_ENGINE
	}
#endif
}

/**
 * 得到指定会话加的DDL锁模式
 *
 * @param session 会话
 * @return 指定会话加的DDL锁模式
 */
ILMode Database::getDDLLock(Session *session) const {
	return m_ddlLock->getLock(session->getId());
}

///////////////////////////////////////////////////////////////////////////////
// 备份与恢复 ///////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////////

/**
 * 备份一个文件（备份已经存在时，拷贝多出的数据）
 * @pre 已经加了DDL共享锁
 * @param backupProcess 备份操作
 * @param tableId       源文件对应的表id
 * @param suffix        源文件后缀
 * @throw NtseException 拷贝文件时发生错误
 */
void Database::backupFile(BackupProcess *backupProcess, u16 tableId, const char *suffix) throw(NtseException) {
	string tmpFilePath = m_ctrlFile->getTablePath(tableId);
	if (ParFileParser::isPartitionByPhyicTabName(tmpFilePath.c_str())) {
		char logicTabName[Limits::MAX_NAME_LEN + 1] = {0};
		bool ret = ParFileParser::parseLogicTabName(tmpFilePath.c_str(), logicTabName, Limits::MAX_NAME_LEN + 1);
		assert(ret);
		UNREFERENCED_PARAMETER(ret);
		tmpFilePath = string(logicTabName);
	}

	string srcPath = string(m_config->m_basedir) + NTSE_PATH_SEP + tmpFilePath + suffix;
	string backupPath = makeBackupPath(backupProcess->m_backupDir
		, basename(srcPath.c_str()), tableId);
	u64 errNo = File::copyFile(backupPath.c_str(), srcPath.c_str(), true);
	if (errNo != File::E_NO_ERROR)
		NTSE_THROW(errNo, "copy %s to %s failed", srcPath.c_str(), backupPath.c_str());
}

/**
 * 备份一个文件
 * @pre 检查点被禁用，并且已经加了DDL共享锁
 *
 * @param backupProcess 备份操作
 * @param src 源文件
 * @param pageType 该文件的页类型
 * @param tableId 源文件对应的表id
 * @param dbObjStats 数据库数据对象状态结构指针
 * @param indice 只在备份索引文件时指定索引，其它情况为为NULL
 * @param existLen 备份文件已经有长度
 * @return 已经备份的文件长度
 * @throw NtseException IO异常
 */
u64 Database::doBackupFile(BackupProcess *backupProcess, File *src, PageType pageType, u16 tableId, DBObjStats *dbObjStats, DrsIndice *indice, u64 existLen) throw(NtseException) {
	assert(m_ddlLock->isLocked(backupProcess->m_session->getId(), IL_S));
	string backupPath = makeBackupPath(backupProcess->m_backupDir, basename(src->getPath()), tableId);
	File backupFile(backupPath.c_str());
	u64 srcSize = 0; // 源文件长度
	try {
		u64 errNo;
		if (existLen) { // 备份文件已经存在，打开
			if ((errNo = backupFile.open(false)) != File::E_NO_ERROR)
				NTSE_THROW(errNo, "Cannot open backup file %s", backupFile.getPath());
		} else { // 创建备份文件
			if ((errNo = backupFile.create(false, false)) != File::E_NO_ERROR)
				NTSE_THROW(errNo, "Cannot create backup file %s", backupFile.getPath());
		}

		if ((errNo = src->getSize(&srcSize)) != File::E_NO_ERROR)
			NTSE_THROW(errNo, "getSize of %s failed", src->getPath());
		SYNCHERE(SP_DB_BACKUPFILE_AFTER_GETSIZE);
		u64 backupSize = 0; // 备份文件长度 
		if ((errNo = backupFile.getSize(&backupSize)) != File::E_NO_ERROR)
			NTSE_THROW(errNo, "getSize of %s failed", backupFile.getPath());
		assert(srcSize > backupSize);
		assert(existLen == backupSize);
		if ((errNo = backupFile.setSize(srcSize)) != File::E_NO_ERROR)
			NTSE_THROW(errNo, "setSize of backup file %s failed", backupFile.getPath());
		
		uint numPages = (uint)(srcSize / Limits::PAGE_SIZE);
		byte *buffer = backupProcess->m_buffer;
		Session *session = backupProcess->m_session;
		for (uint pageNo = (uint)(backupSize / Limits::PAGE_SIZE); pageNo < numPages; pageNo += backupProcess->m_bufPages) {
			uint pageCnt = 0;
			for (; pageCnt < backupProcess->m_bufPages && pageNo + pageCnt < numPages; ++pageCnt) {
				u64 phyReadFromSession = session->getConnection()->getLocalStatus()->m_statArr[OPS_PHY_READ];
				BufferPageHandle *hdl = GET_PAGE(session, src, pageType, pageNo + pageCnt, Shared, dbObjStats, NULL);
				BufferPageHdr *curPage = (BufferPageHdr *)(buffer + pageCnt * Limits::PAGE_SIZE);
				memcpy(curPage, hdl->getPage(), Limits::PAGE_SIZE);
				if (indice) {
					int idx = indice->getIndexNo(curPage, pageNo + pageCnt);
					if (idx >= 0) {
						indice->getIndex(idx)->getDBObjStats()->countIt(DBOBJ_LOG_READ);
						dbObjStats->countIt(DBOBJ_LOG_READ, -1);
						if (session->getConnection()->getLocalStatus()->m_statArr[OPS_PHY_READ] != phyReadFromSession) {
							indice->getIndex(idx)->getDBObjStats()->countIt(DBOBJ_PHY_READ);
							dbObjStats->countIt(DBOBJ_PHY_READ, -1);
						}
					}
				}

				bool dirty = m_buffer->isDirty(hdl->getPage());
				backupProcess->m_session->releasePage(&hdl);
				if (dirty) // 脏页需要重新计算校验和
					curPage->m_checksum = m_buffer->checksumPage(curPage);
				assert(curPage->m_checksum == BufferPageHdr::CHECKSUM_NO
					|| curPage->m_checksum == BufferPageHdr::CHECKSUM_DISABLED
					|| curPage->m_checksum == m_buffer->checksumPage(curPage));
			}
			errNo = backupFile.write((u64)pageNo * (u64)Limits::PAGE_SIZE, pageCnt * Limits::PAGE_SIZE, buffer);
			if (errNo != File::E_NO_ERROR)
				NTSE_THROW(errNo, "write backup file %s failed", backupFile.getPath());
		}
	} catch(NtseException &e) {
		backupFile.close();
		throw e;
	}
	backupFile.close();
	return srcSize;
}
/**
 * 备份一个文件
 * @pre 检查点被禁用，并且已经加了DDL共享锁
 *
 * @param backupProcess 备份操作
 * @param src 源文件
 * @param pageType 该文件的页类型
 * @param tableId 源文件对应的表id
 * @param dbObjStats 数据库数据对象状态结构指针
 * @param indice 只在备份索引文件时指定索引，其它情况为为NULL
 * @throw NtseException IO异常
 */
void Database::backupFile( BackupProcess *backupProcess, File *src, PageType pageType, u16 tableId, DBObjStats *dbObjStats, DrsIndice *indice) throw(NtseException) {
	u64 len = doBackupFile(backupProcess, src, pageType, tableId, dbObjStats, indice, 0);
	backupProcess->m_files.insert(map<File *, u64>::value_type(src, len));	
}

/**
 * 备份控制文件
 */
void Database::backupCtrlFile(BackupProcess *backupProcess) throw(NtseException) {
	u32 cfSize;
	byte *cfData = m_ctrlFile->dupContent(&cfSize);
	AutoPtr<byte> ap(cfData, true);
	string cfPath = string(backupProcess->m_backupDir) + NTSE_PATH_SEP + Limits::NAME_CTRL_FILE;
	File cfBak(cfPath.c_str());
	u64 errNo = cfBak.create(false, false);
	if (errNo != File::E_NO_ERROR) {
		NTSE_THROW(errNo, "create backup file %s failed", cfBak.getPath());
	}
	errNo = cfBak.setSize(cfSize);
	if (errNo != File::E_NO_ERROR) {
		cfBak.close();
		NTSE_THROW(errNo, "set size of backup file %s failed", cfBak.getPath());
	}
	errNo = cfBak.write(0, cfSize, cfData);
	if (errNo != File::E_NO_ERROR) {
		cfBak.close();
		NTSE_THROW(errNo, "write backup file %s failed", cfBak.getPath());
	}
	cfBak.close();
}

/**
 * 备份一个数据表
 * @pre 检查点被禁用，并且已经加了DDL共享锁
 *
 * @param backupProcess 备份操作
 * @param tableId 待备份数据表ID
 * @throw NtseException 写备份文件失败
 */
void Database::backupTable(BackupProcess *backupProcess, u16 tableId) throw(NtseException) {
	assert(m_ddlLock->isLocked(backupProcess->m_session->getId(), IL_S));
	u64 errNo = 0;
	string pathStr = m_ctrlFile->getTablePath(tableId);
	const char *tablename = pathStr.c_str();
	m_syslog->log(EL_LOG, "Backup table %s ...", tablename);
	Table *table = openTable(backupProcess->m_session, tablename);
	try {
		const int maxFileCnt = 3;
		File **srcFiles = new File*[maxFileCnt];
		AutoPtr<File *> apSrcFiles(srcFiles, true);
		PageType *pageTypes = new PageType[maxFileCnt];
		AutoPtr<PageType> apPageTypes(pageTypes, true);

		// 备份堆文件
		SYNCHERE(SP_DB_BACKUP_BEFORE_HEAP);
		DrsHeap *heap = table->getHeap();
		int fileCnt = heap->getFiles(srcFiles, pageTypes, maxFileCnt);
		for (int i = 0; i < fileCnt; ++i)
			backupFile(backupProcess, srcFiles[i], pageTypes[i], tableId, heap->getDBObjStats());

		LobStorage *lobStore = table->getLobStorage();
		if (lobStore) { // 备份大对象数据
			string srcSLTblDefPath;
			string backupSLTblDefPath;
			int fileCnt = lobStore->getFiles(srcFiles, pageTypes, maxFileCnt);
			for (int i = 0; i < fileCnt; ++i) {
				switch (pageTypes[i]) {
					case PAGE_LOB_INDEX:
						backupFile(backupProcess, srcFiles[i], pageTypes[i], tableId, lobStore->getLLobDirStats());
						break;
					case PAGE_LOB_HEAP:
						backupFile(backupProcess, srcFiles[i], pageTypes[i], tableId, lobStore->getLLobDatStats());
						break;
					case PAGE_HEAP:
						backupFile(backupProcess, srcFiles[i], pageTypes[i], tableId, lobStore->getSLHeap()->getDBObjStats());

						srcSLTblDefPath = string(m_config->m_basedir) + NTSE_PATH_SEP + pathStr + Limits::NAME_SOBH_TBLDEF_EXT;
						backupSLTblDefPath = makeBackupPath(backupProcess->m_backupDir, basename(srcSLTblDefPath.c_str()), tableId);
						errNo = File::copyFile(backupSLTblDefPath.c_str(), srcSLTblDefPath.c_str(), true);
						if (errNo != File::E_NO_ERROR)
							NTSE_THROW(errNo, "copy %s to %s failed", srcSLTblDefPath.c_str(), backupSLTblDefPath.c_str());
						break;
					default:
						break;
				}
			}
		}
		DrsIndice *indice = table->getIndice();
		if (indice) { // 备份索引数据
			int fileCnt = indice->getFiles(srcFiles, pageTypes, maxFileCnt);
			for (int i = 0; i < fileCnt; ++i)
				backupFile(backupProcess, srcFiles[i], pageTypes[i], tableId, indice->getDBObjStats(), indice);
		}
		//备份压缩字典文件
		if (table->hasCompressDict()) {
			backupFile(backupProcess, tableId, Limits::NAME_GLBL_DIC_EXT);
		}

		string srcTblDefPath = string(m_config->m_basedir) + NTSE_PATH_SEP + pathStr + Limits::NAME_TBLDEF_EXT;
		string backupTblDefPath = makeBackupPath(backupProcess->m_backupDir, basename(srcTblDefPath.c_str()), tableId);
		errNo = File::copyFile(backupTblDefPath.c_str(), srcTblDefPath.c_str(), true);
		if (errNo != File::E_NO_ERROR)
			NTSE_THROW(errNo, "copy %s to %s failed", srcTblDefPath.c_str(), backupTblDefPath.c_str());

		// 备份mysql相关文件
#ifdef TNT_ENGINE
		//因为所有的ddl被禁止，所以meta lock可以不加
		if (TS_SYSTEM != table->getTableDef()->getTableStatus()) {
#endif
			// 备份mysql相关 frm文件
			backupFile(backupProcess, tableId, ".frm");
			//备份mysql分区相关的par文件
			if (ParFileParser::isPartitionByPhyicTabName(pathStr.c_str())) {
				backupFile(backupProcess, tableId, ".par");
			}
#ifdef TNT_ENGINE
		}
#endif
	} catch(NtseException &e) {
		closeTable(backupProcess->m_session, table);
		throw e;
	}
	backupProcess->m_tables.push_back(table);
}

/**
 * 备份日志
 * @pre 检查点被禁用，并且已经加了DDL互斥锁，若firstPass为false则已经禁止
 *
 *
 * @param backupProcess 备份操作
 * @param firstPass 是否第一趟
 * @throw NtseException 写备份文件失败
 */
void Database::backupTxnlog(BackupProcess *backupProcess, bool firstPass) throw(NtseException) {
	string backupFilename = string(backupProcess->m_backupDir) + NTSE_PATH_SEP + Limits::NAME_TXNLOG;
	File backupFile(backupFilename.c_str());
	u64 writtenSize = 0; // 已写入长度
	u64 fileSize = 0; // 日志文件长度
	u64 errNo = 0;
	try {
		if (firstPass) { // 第一趟创建文件
			if ((errNo = backupFile.create(false, false)) != File::E_NO_ERROR)
				NTSE_THROW(errNo, "Cannot create backup file %s", backupFile.getPath());
		} else { // 第二趟直接打开文件，并读取文件大小
			if ((errNo = backupFile.open(false)) != File::E_NO_ERROR)
				NTSE_THROW(errNo, "Cannot open backup file %s", backupFile.getPath());
			if ((errNo = backupFile.getSize(&writtenSize)) != File::E_NO_ERROR)
				NTSE_THROW(errNo, "getSize on backup file %s failed", backupFile.getPath());
			fileSize = writtenSize;
		}
		LogBackuper *logBackuper = backupProcess->m_logBackuper;
		u64 size = logBackuper->getSize();
		assert(size >= fileSize);
		fileSize = size;
		if ((errNo = backupFile.setSize(fileSize)) != File::E_NO_ERROR)
			NTSE_THROW(errNo, "setSize of backup file %s failed", backupFile.getPath());
		uint bufPages = backupProcess->m_bufPages * Limits::PAGE_SIZE / LogConfig::LOG_PAGE_SIZE;
		uint pageCnt = 0;
		while (0 != (pageCnt = logBackuper->getPages(backupProcess->m_buffer, bufPages, !firstPass))) {
			uint dataSize = pageCnt * LogConfig::LOG_PAGE_SIZE;
			if (writtenSize + dataSize > fileSize) { // 扩展文件
				if ((errNo = backupFile.setSize(writtenSize + dataSize)) != File::E_NO_ERROR)
					NTSE_THROW(errNo, "setSize of backup file %s failed", backupFile.getPath());
			}
			errNo = backupFile.write(writtenSize, dataSize, backupProcess->m_buffer);
			if (errNo != File::E_NO_ERROR)
				NTSE_THROW(errNo, "write backup file %s failed", backupFile.getPath());
			writtenSize += dataSize;
		}
	} catch(NtseException &e) {
		backupFile.close();
		throw e;
	}
	backupFile.close();
}

/**
 * 初始化备份操作
 *
 * @param backupDir 备份到这个目录
 * @throw NtseException 无法创建备份文件，加DDL锁超时，已经有备份操作在进行中等
 */
BackupProcess* Database::initBackup(const char *backupDir) throw(NtseException) {
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

	BackupProcess *backupProcess = new BackupProcess(this, backupDir, 256);
	// 禁用DDL语句
	try {
		acquireDDLLock(backupProcess->m_session, IL_S, __FILE__, __LINE__);
	} catch (NtseException &e) {
		delete backupProcess;
		m_backingUp.set(0);
		throw e;
	}

	// 防止备份期间产生的日志被回收
	backupProcess->m_onlineLsnToken = m_txnlog->setOnlineLsn(m_ctrlFile->getCheckpointLSN());
	m_syslog->log(EL_LOG, "Begin backup to %s", backupDir);
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
void Database::doBackup(BackupProcess *backupProcess) throw(NtseException) {
	assert(m_ddlLock->isLocked(backupProcess->m_session->getId(), IL_S));
	assert(backupProcess->m_stat == BS_INIT);
	ftrace(ts.ddl, tout << backupProcess);

	backupProcess->m_stat = BS_FAILED;

	// 备份控制文件。备份控制文件必须在备份表数据之前，防止在备份期间完成了检查点操作导致恢复起始点后置
	m_syslog->log(EL_LOG, "Backup control file...");
	LsnType logStartLsn = m_ctrlFile->getCheckpointLSN(); // 日志备份的起始LSN
	SYNCHERE(SP_DB_BEFORE_BACKUP_CTRLFILE);
	backupProcess->m_logBackuper = new LogBackuper(getTxnlog(), logStartLsn);
	backupCtrlFile(backupProcess);
		
	// 获取表信息
	u16 numTables = m_ctrlFile->getNumTables();
	u16 *tableIds = new u16[numTables];
	AutoPtr<u16> apTableIds(tableIds, true);
	m_ctrlFile->listAllTables(tableIds, numTables);
	// 备份每张表的数据
	for (u16 i = 0; i < numTables; ++i)
		backupTable(backupProcess, tableIds[i]);
	SYNCHERE(SP_DB_BEFORE_BACKUP_LOG);
	// 然后备份从最近一次checkpoint开始后的日志。
	m_syslog->log(EL_LOG, "Backup log, startLSN "I64FORMAT"u ...", logStartLsn);
	backupTxnlog(backupProcess, true);
	// 备份数据库配置
	m_syslog->log(EL_LOG, "Backup database config...");
	size_t cfgSize;
	byte *cfg = m_config->write(&cfgSize);
	string cfgPath = string(backupProcess->m_backupDir) + NTSE_PATH_SEP + "dbcfg";
	File cfgBak(cfgPath.c_str());
	cfgBak.create(false, false);
	u64 errNo = cfgBak.setSize(cfgSize);
	if (errNo != File::E_NO_ERROR) {
		delete []cfg;
		cfgBak.close();
		NTSE_THROW(errNo, "set size of backup file %s failed", cfgBak.getPath());
	}
	errNo = cfgBak.write(0, cfgSize, cfg);
	if (errNo != File::E_NO_ERROR) {
		delete []cfg;
		cfgBak.close();
		NTSE_THROW(errNo, "write backup file %s failed", cfgBak.getPath());
	}
	delete []cfg;
	cfgBak.close();
	
	backupProcess->m_stat = BS_COPYED;
}

/**
 * 结束备份并且锁定数据库为只读状态
 * @pre 必须为BS_COPYED状态
 *
 * @param backupProcess 备份操作
 * @throw NtseException 备份日志时写文件失败
 */
void Database::finishingBackupAndLock(BackupProcess *backupProcess) throw(NtseException) {
	assert(backupProcess->m_stat == BS_COPYED);
	ftrace(ts.ddl, tout << backupProcess);

	backupProcess->m_stat = BS_FAILED;

	m_syslog->log(EL_LOG, "Lock database...");
	// 为保证备份数据与binlog相一致，需要在备份即将结束时禁止对数据库的所有
	// 操作，并且刷出MMS更新缓存日志，这一过程如下实现:
	// 1. 得到当前打开的表中启用MMS更新缓存的表列表
	// 2. 对上述每张表，pin住该表防止其被删除或关闭，然后刷MMS更新缓存日志
	// 3. 锁定openLock防止表被打开或关闭
	// 4. 遍历所有表，禁止对表的写操作
	// 5. 若为启用MMS更新缓存的表，再次刷出MMS更新缓存日志，由于刚刷过一次
	//    这次通常比较快，这样可以减少写操作被禁用的时间

	// 第一次获取启用MMS更新缓存的表列表
	vector<string> cacheUpdateTables;

	acquireATSLock(backupProcess->m_session, IL_S, -1, __FILE__, __LINE__);

	TableInfo **tables = new TableInfo*[m_idToTables->getSize()];
	m_idToTables->values(tables);
	for (size_t i = 0; i < m_idToTables->getSize(); i++) {
		// 由于开始备份时已经禁止了所有DDL操作，因此这里不需要加表元数据锁
		TableDef *tableDef = tables[i]->m_table->getTableDef(false);
		if (tableDef->m_useMms && tableDef->m_cacheUpdate)
			cacheUpdateTables.push_back(tables[i]->m_path);
	}
	delete [] tables;

	releaseATSLock(backupProcess->m_session, IL_S);

	// 第一次刷MMS更新缓存，小型大对象MMS不会启用更新缓存因此不需要刷
	for (size_t i = 0; i < cacheUpdateTables.size(); i++) {
		Table *tableInfo = pinTableIfOpened(backupProcess->m_session, cacheUpdateTables[i].c_str());
		// 由于开始备份时已经禁止了所有DDL操作，因此这里不需要加表元数据锁
		assert(getDDLLock(backupProcess->m_session) == IL_S);
		if (tableInfo) {
			TableDef *tableDef = tableInfo->getTableDef();
			if (tableDef->m_useMms && tableDef->m_cacheUpdate) {
				tableInfo->getMmsTable()->flushUpdateLog(backupProcess->m_session);
			}
			closeTable(backupProcess->m_session, tableInfo);
		}
	}

	// 锁定每张表禁止写操作，再次刷MMS更新缓存
	acquireATSLock(backupProcess->m_session, IL_S, -1, __FILE__, __LINE__);
	tables = new TableInfo*[m_idToTables->getSize()];
	m_idToTables->values(tables);

	for (size_t i = 0; i < m_idToTables->getSize(); i++) {
		tables[i]->m_table->lockMeta(backupProcess->m_session, IL_S, -1, __FILE__, __LINE__);
		tables[i]->m_table->lock(backupProcess->m_session, IL_S, -1, __FILE__, __LINE__);
		TableDef *tableDef = tables[i]->m_table->getTableDef();
		if (tableDef->m_useMms && tableDef->m_cacheUpdate) {
			tables[i]->m_table->getMmsTable()->flushUpdateLog(backupProcess->m_session);
		}
	}
	delete [] tables;

	backupProcess->m_stat = BS_LOCKED;
	
	m_syslog->log(EL_LOG, "Backup files that become larger ...");
	rebackupExtendedFiles(backupProcess);
	m_syslog->log(EL_LOG, "Backup addtional log...");
	// 备份这期间产生的少量NTSE日志，保证备份的NTSE数据与binlog一致，异常直接抛出
	backupTxnlog(backupProcess, false);
	backupProcess->m_tailLsn = m_txnlog->tailLsn();
	m_syslog->log(EL_LOG, "Finish backup, lsn: "I64FORMAT"u", backupProcess->m_tailLsn);
}

/**
 * 重新备份变长的文件
 *	堆模块会在堆头页中记录堆文件的页数，堆恢复代码根据头页中记录的页数决定是否扩展堆。
 *  这种做法实际上假定了堆头页中的页数要大于文件的实际页数。
 *  为满足这个假设，本方法检查文件长度是否变长，如果变长则拷贝多出的额外页面到备份文件
 *  详见QA58235任务
 *
 * @pre 数据库只读
 * @param  backupProcess 备份操作
 */

void Database::rebackupExtendedFiles(BackupProcess *backupProcess) {
	
	assert(m_ddlLock->isLocked(backupProcess->m_session->getId(), IL_S));

	// 重新检查每张表的文件，看看文件有无变长
	for (u16 i = 0; i < backupProcess->m_tables.size(); ++i) {
		Table *table = backupProcess->m_tables[i];
		u16 tableId = table->getTableDef()->m_id;
		const int maxFileCnt = 3;
		File **srcFiles = new File*[maxFileCnt];
		AutoPtr<File *> apSrcFiles(srcFiles, true);
		PageType *pageTypes = new PageType[maxFileCnt];
		AutoPtr<PageType> apPageTypes(pageTypes, true);
		
		DrsHeap *heap = table->getHeap();
		int fileCnt = heap->getFiles(srcFiles, pageTypes, maxFileCnt);
		for (int i = 0; i < fileCnt; ++i) {
			map<File *, u64>::iterator iter = backupProcess->m_files.find(srcFiles[i]);
			assert(backupProcess->m_files.end() != iter);
			
			u64 size = 0, errNo;
			if ((errNo = srcFiles[i]->getSize(&size)) != File::E_NO_ERROR)
				NTSE_THROW(errNo, "getSize of %s failed", srcFiles[i]->getPath());
			if (size > iter->second) //  文件变长
				doBackupFile(backupProcess, srcFiles[i], pageTypes[i], tableId, heap->getDBObjStats(), NULL, iter->second);	
		}

		LobStorage *lobStore = table->getLobStorage();
		if (lobStore) {
			int fileCnt = lobStore->getFiles(srcFiles, pageTypes, maxFileCnt);
			for (int i = 0; i < fileCnt; ++i) {
				map<File *, u64>::iterator iter = backupProcess->m_files.find(srcFiles[i]);
				assert(backupProcess->m_files.end() != iter);

				u64 size = 0, errNo;
				if ((errNo = srcFiles[i]->getSize(&size)) != File::E_NO_ERROR)
					NTSE_THROW(errNo, "getSize of %s failed", srcFiles[i]->getPath());
				if (size <= iter->second)
					continue;

				switch (pageTypes[i]) {
				case PAGE_LOB_INDEX:
					doBackupFile(backupProcess, srcFiles[i], pageTypes[i], tableId, lobStore->getLLobDirStats(), NULL, iter->second);
					break;
				case PAGE_LOB_HEAP:
					doBackupFile(backupProcess, srcFiles[i], pageTypes[i], tableId, lobStore->getLLobDatStats(), NULL, iter->second);
					break;
				case PAGE_HEAP:
					doBackupFile(backupProcess, srcFiles[i], pageTypes[i], tableId, lobStore->getSLHeap()->getDBObjStats(), NULL, iter->second);
					break;
				default:
					break;
				}
			}
		}
		DrsIndice *indice = table->getIndice();
		if (indice) { // 备份索引数据
			int fileCnt = indice->getFiles(srcFiles, pageTypes, maxFileCnt);
			for (int i = 0; i < fileCnt; ++i) {
				map<File *, u64>::iterator iter = backupProcess->m_files.find(srcFiles[i]);
				assert(backupProcess->m_files.end() != iter);

				u64 size = 0, errNo;
				if ((errNo = srcFiles[i]->getSize(&size)) != File::E_NO_ERROR)
					NTSE_THROW(errNo, "getSize of %s failed", srcFiles[i]->getPath());
				if (size <= iter->second)
					continue;
				doBackupFile(backupProcess, srcFiles[i], pageTypes[i], tableId, indice->getDBObjStats(), indice, iter->second);
			}
		}
	}
}

/**
 * 结束备份
 * @pre 必须为BS_LOCKED或BS_FAILED状态
 * @post backupProcess对象已经被销毁
 *
 * @param backupProcess 备份操作
 */
void Database::doneBackup(BackupProcess *backupProcess) {
	ftrace(ts.ddl, tout << backupProcess);

	if (backupProcess->m_stat == BS_LOCKED) {
		// 恢复写操作
		TableInfo **tables = new TableInfo*[m_idToTables->getSize()];
		m_idToTables->values(tables);
		for (size_t i = 0; i < m_idToTables->getSize(); i++) {
			tables[i]->m_table->unlock(backupProcess->m_session, IL_S);
			tables[i]->m_table->unlockMeta(backupProcess->m_session, IL_S);
		}
		delete []tables;
		releaseATSLock(backupProcess->m_session, IL_S);
	}
	for (size_t i = 0; i < backupProcess->m_tables.size(); ++i)
		closeTable(backupProcess->m_session, backupProcess->m_tables[i]);
	m_txnlog->clearOnlineLsn(backupProcess->m_onlineLsnToken);
	releaseDDLLock(backupProcess->m_session, IL_S);
	m_backingUp.set(0);

	delete backupProcess;
}

/**
 * 从备份文件恢复数据库
 *
 * @param backupDir 备份所在目录
 * @param dir 恢复到这个目录
 * @throw NtseException 读备份文件失败，写数据失败等
 */
#ifdef TNT_ENGINE
void Database::restore(const char *backupDir, const char *dir, const char *logDir) throw(NtseException) {
#else
void Database::restore(const char *backupDir, const char *dir) throw(NtseException) {
#endif	
	printf("Restore from '%s' to '%s'\n", backupDir, dir);
	
	Syslog log(NULL, EL_LOG, true, true);
	ControlFile *ctrlFile = 0;
	string destDir(dir);
	string srcDir(backupDir);
	try {
		// 拷贝控制文件
		string ctfPath = destDir + NTSE_PATH_SEP + Limits::NAME_CTRL_FILE;
		string ctfBackPath = srcDir + NTSE_PATH_SEP + Limits::NAME_CTRL_FILE;
		u64 errNo = File::copyFile(ctfPath.c_str(), ctfBackPath.c_str(), true);
		if (errNo != File::E_NO_ERROR)
			NTSE_THROW(errNo, "copy %s to %s failed", ctfBackPath.c_str(), ctfPath.c_str());
		ctrlFile = ControlFile::open(ctfPath.c_str(), &log);
		u16 numTabs = ctrlFile->getNumTables();
		u16 *tabIds = new u16[numTabs];
		AutoPtr<u16> apTabIds(tabIds, true);
		ctrlFile->listAllTables(tabIds, numTabs);
		printf("%d tables to restore\n", numTabs);
		// 拷贝数据表
		for (u16 i = 0; i < numTabs; ++i)
			restoreTable(ctrlFile, tabIds[i], backupDir, dir);
		// 拷贝日志
#ifdef TNT_ENGINE
		if (logDir != NULL) {
			restoreTxnlog(ctrlFile, logDir, (srcDir + NTSE_PATH_SEP + Limits::NAME_TXNLOG).c_str());
		} else {
#endif
			restoreTxnlog(ctrlFile, dir, (srcDir + NTSE_PATH_SEP + Limits::NAME_TXNLOG).c_str());
#ifdef TNT_ENGINE
		}
#endif
	} catch(NtseException &e) {
		if (ctrlFile)
			ctrlFile->close();
		delete ctrlFile;
		throw e;
	}
	if (ctrlFile)
		ctrlFile->close();
	delete ctrlFile;

	// 恢复
#ifndef TNT_ENGINE
	string cfgPath = srcDir + NTSE_PATH_SEP + "dbcfg";
	File cfgFile(cfgPath.c_str());
	u64 errNo = cfgFile.open(false);
	if (errNo != File::E_NO_ERROR)
		NTSE_THROW(errNo, "open config file failed");
	u64 cfgSize;
	cfgFile.getSize(&cfgSize);
	byte *buf = new byte[(size_t)cfgSize];
	errNo = cfgFile.read(0, (u32)cfgSize, buf);
	if (errNo != File::E_NO_ERROR) {
		delete []buf;
		NTSE_THROW(errNo, "read config file failed");
	}
	cfgFile.close();

	Config *config = Config::read(buf, (size_t)cfgSize);
	delete []buf;
	config->setBasedir(dir);
	config->setTmpdir(dir);
	try {
		Database *db = Database::open(config, false, 1);
		db->close(true, true);
		delete db;
	} catch (NtseException &e) {
		delete config;
		throw e;
	}
	delete config;
#endif
}

/**
 * 恢复一张表
 * @param ctrlFile 数据库控制文件
 * @param tabId 表id
 * @param backupDir 备份目录
 * @param dir MySQL数据目录
 */
void Database::restoreTable(ControlFile *ctrlFile, u16 tabId , const char *backupDir, const char *dir) throw (NtseException) {
	printf("Restore table '%s'\n", ctrlFile->getTablePath(tabId).c_str());
	
	string basedir(dir);
	const char * suffixes[] = {Limits::NAME_HEAP_EXT, Limits::NAME_TBLDEF_EXT, ".frm", ".par", Limits::NAME_IDX_EXT
		, Limits::NAME_LOBD_EXT, Limits::NAME_LOBI_EXT, Limits::NAME_SOBH_EXT, Limits::NAME_SOBH_TBLDEF_EXT, Limits::NAME_GLBL_DIC_EXT};
	for (uint i = 0; i < sizeof(suffixes) / sizeof(const char *); ++i) {
		string pathStr = ctrlFile->getTablePath(tabId);
		if ((i == 2) && (ParFileParser::isPartitionByPhyicTabName(pathStr.c_str()))) {
			char logicTabName[Limits::MAX_NAME_LEN + 1] = {0};
			bool ret = ParFileParser::parseLogicTabName(pathStr.c_str(), logicTabName, Limits::MAX_NAME_LEN + 1);
			assert(ret);
			UNREFERENCED_PARAMETER(ret);
			pathStr = string(logicTabName);
		}
		if ((i == 3) && (ParFileParser::isPartitionByPhyicTabName(pathStr.c_str()))) {
			char logicTabName[Limits::MAX_NAME_LEN + 1] = {0};
			bool ret = ParFileParser::parseLogicTabName(pathStr.c_str(), logicTabName, Limits::MAX_NAME_LEN + 1);
			assert(ret);
			UNREFERENCED_PARAMETER(ret);
			pathStr = string(logicTabName);
		}
		string filename = basename(pathStr.c_str()) + suffixes[i];
		string backupPath = makeBackupPath(backupDir, filename, tabId);
		File file(backupPath.c_str());
		bool exist;
		file.isExist(&exist);
#ifdef TNT_ENGINE
		if (!exist && i <= 1) //tnt中由于系统表不存在frm，所以只能在这里进行放宽条件，只要求需要堆表和表定义
			NTSE_THROW(NTSE_EC_INVALID_BACKUP, "no heap or tabledef file for table %s", ctrlFile->getTablePath(tabId).c_str());
#else
		if (!exist && i <= 2)// 堆文件和frm文件不能不存在
			NTSE_THROW(NTSE_EC_INVALID_BACKUP, "no heap, tabledef or frm file for table %s", ctrlFile->getTablePath(tabId).c_str());
#endif
		if (!exist)
			continue;
		string dstPath = basedir + NTSE_PATH_SEP + pathStr + suffixes[i];
		File(dirname(dstPath.c_str()).c_str()).mkdir();
		u64 errNo = File::copyFile(dstPath.c_str(), backupPath.c_str(), true);
		if (errNo != File::E_NO_ERROR)
			NTSE_THROW(errNo, "copy %s to %s failed", backupPath.c_str(), dstPath.c_str());
	}
}

/**
 * 恢复日志文件
 * @param ctrlFile 数据库控制文件
 * @param char *basedir	MySQL数据目录
 * @param backupPath	备份文件全路径
 */
void Database::restoreTxnlog(ControlFile *ctrlFile, const char *basedir, const char *backupPath) throw(NtseException) {
	printf("Restore transaction logs");
#ifdef TNT_ENGINE
	File logDir(basedir);
	bool exist = true;
	u64 errCode = logDir.isExist(&exist);
	assert(errCode == File::E_NO_ERROR);
	if (!exist) 
		logDir.mkdir();
#endif

	File backupFile(backupPath);
	u64 errNo = 0;
	if ((errNo = backupFile.open(false)) != File::E_NO_ERROR)
		NTSE_THROW(errNo, "open backup file %s failed", backupFile.getPath());
	try {
		uint bufPages = 256;
		uint bufSize = bufPages * LogConfig::LOG_PAGE_SIZE;
		byte *buf = new byte[bufSize];
		AutoPtr<byte> apBuf(buf, true);
		u64 size = 0;
		if ((errNo = backupFile.getSize(&size)) != File::E_NO_ERROR)
			NTSE_THROW(errNo, "getSize of %s failed", backupFile.getPath());
		if (size % LogConfig::LOG_PAGE_SIZE != 0)
			NTSE_THROW(NTSE_EC_INVALID_BACKUP, "log file backup %s has invalid size", backupFile.getPath());
		LogRestorer logRestorer(ctrlFile, (string(basedir) + NTSE_PATH_SEP + Limits::NAME_TXNLOG).c_str());
		u64 offset = 0;
		while (offset < size) {
			// 由于这里的bufSize一定不会超过uint 范围，因此如下的类型强制转换是安全的
			uint readSize = (uint)min((size - offset), (u64)bufSize);
			if ((errNo = backupFile.read(offset,readSize, buf)) != File::E_NO_ERROR)
				NTSE_THROW(errNo, "read backup file %s failed", backupFile.getPath());
			logRestorer.sendPages(buf, readSize / LogConfig::LOG_PAGE_SIZE);
			offset += readSize;
		}
	} catch(NtseException &e) {
		backupFile.close();
		throw e;
	}
	backupFile.close();
}

/** 注册后台线程
 * @param task 后台线程
 */
void Database::registerBgTask(BgTask *task) {
	MutexGuard guard(m_bgtLock, __FILE__, __LINE__);
	assert(find(m_bgTasks->begin(), m_bgTasks->end(), task) == m_bgTasks->end());
	m_bgTasks->push_back(task);
}

/** 注销后台线程
 * @param task 后台线程
 */
void Database::unregisterBgTask(BgTask *task) {
	MutexGuard guard(m_bgtLock, __FILE__, __LINE__);
	vector<BgTask *>::iterator it = find(m_bgTasks->begin(), m_bgTasks->end(), task);
	assert(it != m_bgTasks->end());
	m_bgTasks->erase(it);
}

/** 等待后台线程完成初始化
 * @param all 为true表示等待所有后台线程完成初始化，false表示只等等那些在恢复过程中也要
 *   运行的线程完成初始化
 */
void Database::waitBgTasks(bool all) {
	MutexGuard guard(m_bgtLock, __FILE__, __LINE__);
	for (size_t i = 0; i < m_bgTasks->size(); i++) {
		while (true) {
			if (!all && !(*m_bgTasks)[i]->shouldRunInRecover())
				break;
			if ((*m_bgTasks)[i]->setUpFinished())
				break;
			Thread::msleep(10);
		}
	}
}

/**
 * 得到行操作回调管理器
 * @return 返回行操作管理器
 */
NTSECallbackManager* Database::getNTSECallbackManager() {
	return m_cbManager;
}

/**
 * 注册行操作回调函数
 * @param type	操作类型
 * @param cbfn	对应的回调函数
 */
void Database::registerNTSECallback( CallbackType type, NTSECallbackFN *cbfn ) {
	m_cbManager->registerCallback(type, cbfn);
}

/**
 * 注销行操作回调函数
 * @param type	操作类型
 * @param cbfn	对应的回调函数
 * @return true表示注销成功，false表示回调不存在
 */
bool Database::unregisterNTSECallback( CallbackType type, NTSECallbackFN *cbfn ) {
	flushMmsUpdateLogs();
	return m_cbManager->unregisterCallback(type, cbfn);
}

/** UPDATE/DELETE是否需要主键等价物的前像
 * 该值在数据库打开之后不可能改变
 *
 * @return UPDATE/DELETE是否需要主键等价物的前像
 */
bool Database::isPkeRequiredForUnD() const {
	return m_pkeRequiredForDnU;
}

/**
 * 刷写所有表的mms日志
 */
void Database::flushMmsUpdateLogs() {
	Connection *conn = getConnection(true, __FUNC__);
	Session *session = m_sessionManager->allocSession(__FUNC__, conn);

	acquireATSLock(session, IL_S, -1, __FILE__, __LINE__);

	size_t numTables = m_idToTables->getSize();
	TableInfo **tables = new TableInfo*[numTables];
	m_idToTables->values(tables);

	for (size_t i = 0; i < numTables; i++) {
		if (tables[i]->m_table->getTableDef()->m_cacheUpdate)
			tables[i]->m_table->getMmsTable()->flushUpdateLog(session);
	}

	delete [] tables;

	releaseATSLock(session, IL_S);

	m_sessionManager->freeSession(session);
	freeConnection(conn);
}

/**
 * 得到文件最佳扩展大小
 * 具体策略为：若当前数据库文件大小>=ntse_incr_size * 4，则扩展大小为ntse_incr_size;
 * 如果当前文件大小不足5个页面,扩展1个页面;否则扩展大小为当前数据库大小/4
 * @param tableDef	要扩展文件所属的表定义
 * @param fileSize	要扩展文件当前大小
 * @return 推荐的扩展大小
 */
u16 Database::getBestIncrSize( const TableDef *tableDef, u64 fileSize ) {
	u64 curPages = fileSize / Limits::PAGE_SIZE;
	if (curPages >= (u64)tableDef->m_incrSize * 4)
		return tableDef->m_incrSize;
	else if (curPages <= 4)
		return 1;
	else
		return (u16)(curPages / 4);
}

//////////////////////////////////////////////////////////////////////////
///////////////BackupProcess//////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////
BackupProcess::BackupProcess(Database *db, const char* backupDir, uint bufPages)
	: m_db(db), m_backupDir(System::strdup(backupDir)), m_bufPages(bufPages)
		, m_logBackuper(0), m_onlineLsnToken(-1) {
	m_buffer = (byte *)System::virtualAlloc(m_bufPages * Limits::PAGE_SIZE);
	if (!m_buffer)
		db->getSyslog()->log(EL_PANIC, "Unable to alloc %d bytes", m_bufPages * Limits::PAGE_SIZE);
	m_conn = db->getConnection(true, "Backup");
	m_session = db->getSessionManager()->allocSession("Backup", m_conn);
	m_stat = BS_INIT;
	m_ckptEnabled = false;
}

BackupProcess::~BackupProcess() {
	m_db->getSessionManager()->freeSession(m_session);
	m_db->freeConnection(m_conn);
	System::virtualFree(m_buffer);
	delete [] m_backupDir;
	delete m_logBackuper;
}

///////////////////////////////////////////////////////////////////////////////
// 辅助函数 ///////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////////
/** 获取文件名 */
string basename(const char *absPath) {
	string path(absPath);
	size_t slashPos = path.rfind(NTSE_PATH_SEP_CHAR);
	return (slashPos == string::npos) ? path : path.substr(slashPos + 1);
}
/** 获取目录 */
string dirname(const char *absPath) {
	string path(absPath);
	size_t slashPos = path.rfind(NTSE_PATH_SEP_CHAR);
	return (slashPos == string::npos) ? "." : path.substr(0, slashPos);
}
/**
 * 获取备份文件名
 * @param backupDir 备份目录
 * @param filename 文件名
 * @param tableId 文件所属的表id
 * @return 备份文件名
 */
string makeBackupPath(const string& backupDir, const string& filename, u16 tableId) {
	// 生成备份文件路径, 为了处理同名表，在路径最后加上一个tableid后缀
	stringstream ss;
	ss << backupDir << NTSE_PATH_SEP << filename << "." << tableId;
	return ss.str();
}

/** Add by TNT */
/**	NTSE Database打开，分为两个步骤，preOpen为步骤一
*	@config		数据库相关配置
*	@autoCreate	数据库不存在时，是否自动创建
*	@bRecovered	数据库打开过程中，是否经过恢复
*	@recover	数据库是否需要恢复
*	@pkeRequiredForDnU
*	@return		数据库打开成功，返回实例；否则返回Exception
*/
void Database::preOpen(TNTDatabase *tntDb, int recover, bool pkeRequiredForDnU, bool *bRecovered) throw(NtseException)
{
	ftrace(ts.ddl, tout << tntDb << recover << pkeRequiredForDnU);
	Config *config = &tntDb->getTNTConfig()->m_ntseConfig;
	assert(recover >= -1 && recover <= 1);
	assert(config->m_logFileCntHwm > 1);

	// 判断控制文件是否存在，若控制文件存在，则认为数据库已经创建，这时
	// 打开数据库。否则认为数据库不存在，这时创建数据库
	string basedir(config->m_basedir);
	string path = basedir + NTSE_PATH_SEP + Limits::NAME_CTRL_FILE;

	if (!File::isExist(path.c_str())) {
		NTSE_THROW(NTSE_EC_FILE_NOT_EXIST, "Ctrl file %s not exists.", path.c_str());
	}

	Database *db = new Database(config);
	tntDb->m_db = db;
	db->m_stat = DB_STARTING;
	bool shouldRecover;

	path = basedir + NTSE_PATH_SEP + Limits::NAME_SYSLOG;
	db->m_syslog = new Syslog(path.c_str(), config->m_logLevel, false, config->m_printToStdout);

	db->m_cbManager = new NTSECallbackManager();
	db->m_pkeRequiredForDnU = pkeRequiredForDnU;

	try {
		path = basedir + NTSE_PATH_SEP + Limits::NAME_CTRL_FILE;
		db->m_ctrlFile = ControlFile::open(path.c_str(), db->m_syslog);

		db->m_txnlog = NULL;
		do {
			try {
				db->m_txnlog = Txnlog::open(tntDb, config->m_logdir, Limits::NAME_TXNLOG, db->m_ctrlFile->getNumTxnlogs(),
					config->m_logFileSize, config->m_logBufSize);
				tntDb->m_tntTxnLog = db->m_txnlog;
			} catch(NtseException &e) {
				if (e.getErrorCode() == NTSE_EC_MISSING_LOGFILE) { // 日志文件已经被删除
					db->getSyslog()->log(EL_WARN, "Missing transaction log files! Try to recreate.");
					// 假定数据库状态一致，重新创建日志文件
					LsnType srcCkptLsn = db->getControlFile()->getCheckpointLSN();
					LsnType srcDumpLsn = tntDb->getTNTControlFile()->getDumpLsn();
					LsnType ckptLsn = Txnlog::recreate(db->getConfig()->m_logdir
						, Limits::NAME_TXNLOG, db->getControlFile()->getNumTxnlogs()
						, config->m_logFileSize, min(srcCkptLsn, srcDumpLsn));
					db->getSyslog()->log(EL_LOG, "Transaction log files created, set checkpoint LSN "
						"from "I64FORMAT"u to "I64FORMAT"u and set dump LSN from "I64FORMAT"u to "I64FORMAT"u.", srcCkptLsn, ckptLsn, srcDumpLsn, ckptLsn);
					db->getControlFile()->setCheckpointLSN(ckptLsn);
					tntDb->getTNTControlFile()->setDumpLsn(ckptLsn);
				} else {
					throw e;
				}
			}
		} while(!db->m_txnlog);

		//db->m_pagePool = new PagePool(config->m_maxSessions + 1, Limits::PAGE_SIZE);
		PagePool *pool = tntDb->m_pagePool;
		db->m_buffer = new Buffer(db, config->m_pageBufSize, pool, db->m_syslog, db->m_txnlog);
		pool->registerUser(db->m_buffer);
		db->m_rowLockMgr = new LockManager(config->m_maxSessions * 2);
		db->m_idxLockMgr = new IndicesLockManager(config->m_maxSessions + 1, config->m_maxSessions * 5 * 4);
		db->m_sessionManager = new SessionManager(db, config->m_maxSessions, config->m_internalSessions);

		if (recover == 1)
			shouldRecover = true;
		else if (recover == -1)
			shouldRecover = false;
		else
			shouldRecover = !db->m_ctrlFile->isCleanClosed();

		*bRecovered = shouldRecover;

		db->m_mms = new Mms(db, config->m_mmsSize, pool, shouldRecover);
		pool->registerUser(db->m_mms);
		//该pool由TNTDatabase Open进行new，但在此处init，而不返回给TNTDatabase进行init，
		//主要是为了不将Database preOpen再进行拆分
		pool->init();

		db->m_stat = DB_MOUNTED;
		db->waitBgTasks(false);

		if (shouldRecover) {
// 			if (db->m_config->m_backupBeforeRecover) {
// 				stringstream targetDir;
// 				targetDir << basedir << "/../ntse-recover-auto-bak-" << db->m_ctrlFile->getCheckpointLSN();
// 				db->fileBackup(targetDir.str().c_str());
// 			}
			db->recover();
		}
		else {
			db->m_stat = DB_RECOVERING;
		}
	} catch (NtseException &e) {
		db->m_syslog->log(EL_ERROR, e.getMessage());
		db->close(true, false);
		delete db;
		tntDb->m_db = NULL;
		throw e;
	}
}

/**	NTSE Database打开，分为两个步骤，preOpen为步骤二
*	@recover	数据库打开时，是否需要恢复
*	@bRecovered	数据库打开时，是否经历过恢复
*/

void Database::postOpen(int recover, bool bRecovered) {
	m_unRecovered = recover == -1;
	if (!m_unRecovered) {
		m_chkptTask = new Checkpointer(this);
		m_chkptTask->start();
	}

	m_bgCustomThdsManager = new BgCustomThreadManager(this);
	m_bgCustomThdsManager->start();

	m_closed = false;
	m_stat = DB_RUNNING;
	waitBgTasks(true);

	if (bRecovered && m_config->m_verifyAfterRecover)
		this->verify();
}

TableInfo* Database::getTableInfo(u16 tableId) {
	return m_idToTables->get(tableId);
}

TableInfo* Database::getTableInfo(const char * path) {
	return m_pathToTables->get((char *)path);
}

}
