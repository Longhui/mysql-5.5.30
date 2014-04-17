/**
 * ���ݿ⼰�����ֵ�
 *
 * @author ��Դ(wangyuan@corp.netease.com, wy@163.org)
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

/** ���ڽ��м���ĺ�̨�߳� */
class Checkpointer: public BgTask {
public:
	/**
	 * ���캯��
	 *
	 * @param db ���ݿ�
	 * @param interval ����ʱ��������λ��
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
		// ���ϵͳû�в���NTSE��־����ô�Ͳ���Ҫ������
		if (m_db->getTxnlog()->getStatus().m_ntseLogSize - m_lastNtseLogSize == 0) {
			// ����ͳ����Ϣ
			m_db->getPageBuffer()->resetFlushStatus();	

			// �����ǰ��־�ѳ�������LSN һ���logCapacity�Ĵ�С����ǿ���ƽ�һ�μ���
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

	/** ȡ�����������ֻ��ϵͳ�ر�ʱ���� */
	void cancel() {
		m_session->setCanceled(true);
	}

	/** ���ü���������� */
	void setInterval(u32 sec) {
		m_interval = sec;
	}

	/** �����Ƿ�����ִ��һ�μ��������־λ */
	void setInstant(bool ins) {
		m_instant = ins;
	}
private:
	bool		m_instant;			/** �Ƿ�����ִ��*/
	u64			m_lastNtseLogSize;		/** ��һ����ɼ���ʱntse��logд���� */
	bool		m_useAio;			/** �Ƿ�ʹ��Aio */
	AioArray	m_aioArray;			/** ����Aio���� */
};

///////////////////////////////////////////////////////////////////////////////
// ���ݿ� ////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////////


/**
 * ��һ�����ݿ�
 *
 * @param config ���ò���
 *   �ڴ�ʹ��Լ����ֱ�����������ݿ�ر�
 * @param autoCreate ���ݿⲻ����ʱ�Ƿ��Զ�����
 * @param recover �Ƿ�ָ���0��ʾ�������ݿ��Ƿ�ȫ�ر���Ҫ�Զ������Ƿ�ָ���1��ʾǿ�ƻָ���-1��ʾǿ�Ʋ��ָ�
 * @param pkeRequiredForDnU	��UPDATE��DELETE������ʱ���Ƿ���Ҫ�õ�PKE
 * @throw NtseException ����ʧ��
 */
Database* Database::open(Config *config, bool autoCreate, int recover, bool pkeRequiredForDnU /*= false*/) throw(NtseException) {
	ftrace(ts.ddl, tout << config << autoCreate << recover);
	assert(recover >= -1 && recover <= 1);
	assert(config->m_logFileCntHwm > 1);
	// �жϿ����ļ��Ƿ���ڣ��������ļ����ڣ�����Ϊ���ݿ��Ѿ���������ʱ
	// �����ݿ⡣������Ϊ���ݿⲻ���ڣ���ʱ�������ݿ�
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
				if (e.getErrorCode() == NTSE_EC_MISSING_LOGFILE) { // ��־�ļ��Ѿ���ɾ��
					db->getSyslog()->log(EL_WARN, "Missing transaction log files! Try to recreate.");
					// �ٶ����ݿ�״̬һ�£����´�����־�ļ�
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

/** �ж��ļ��Ƿ���Ҫ����
 * @param path �ļ�·��
 * @param name �ļ���
 * @param isDir �Ƿ�ΪĿ¼
 * @return �Ƿ���Ҫ����
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

/** �ж�ָ�����Ƶ��ļ��Ƿ���NTSEʹ�õ��ļ�
 * @param name �ļ���
 * @return ָ�����Ƶ��ļ��Ƿ���NTSEʹ�õ��ļ�
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

/** �����ļ�ϵͳ���ݡ���Ŀ��Ŀ¼�Ѿ�����ʱ������
 * @param dir ����Ŀ��Ŀ¼
 * @return �Ƿ�����˱���
 * @throw NtseException �ļ�����ʧ��
 */
bool Database::fileBackup(const char *dir) throw(NtseException) {
	assert(m_stat == DB_MOUNTED);
	m_syslog->log(EL_LOG, "Prepare to backup database to directory %s", dir);
	if (File::isExist(dir)) {
		m_syslog->log(EL_LOG, "Target directory already exists, skip backup");
		return false;
	}
	// �������ݿ�Ŀ¼�µ������ļ��������ܸ��ݿ����ļ�ֻ����NTSE�������ļ���
	// ��Ϊ�������޷����ݽ�������д������ļ�
	// ��Ϊһ�����ڷ�����ԵĹ��ܣ��౸��һЩ����Ҳ���Խ���
	u64 code = File::copyDir(dir, m_config->m_basedir, false, dbBackupFilter);
	if (code != File::E_NO_ERROR)
		NTSE_THROW(code, "Backup failed.");
	return true;
}

/**
 * �������ݿ�
 *
 * @param config ���ݿ�����
 * @throw NtseException �ļ��Ѿ����ڣ���Ȩ�޵ȵ��²���ʧ��
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
 * ɾ�����ݿ�
 *
 * @param dir ���ݿ��ļ�����Ŀ¼
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
 * �ر����ݿ�
 *
 * @param flushLog �Ƿ�ˢ����־
 * @param flushData �Ƿ�ˢ������
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

	// �ڹرձ�֮ǰ�رռ����MMS�滻��̨�̣߳���ֹ�������̲߳��������б��ر�
	if (m_chkptTask) {
		((Checkpointer *)m_chkptTask)->cancel();
		m_chkptTask->stop();
		m_chkptTask->join();
		delete m_chkptTask;
	}
	if (m_mms)
		m_mms->stopReplacer();

	// �رմ򿪵ı�
	if (prevStat != DB_STARTING)
		closeOpenTables(flushData);

	// �ر�MMS
	if (m_mms)
		m_mms->close();

	// �ر���־ϵͳ
	LsnType tailLsn = INVALID_LSN;
	if (m_txnlog) {
		if (flushLog)
			tailLsn = m_txnlog->tailLsn();
		m_txnlog->close(flushLog);
	}

	// �رտ����ļ�ϵͳ
	if (m_ctrlFile) {
		if (flushData && flushLog && tailLsn != INVALID_LSN)
			m_ctrlFile->setCheckpointLSN(tailLsn);
		m_ctrlFile->close(flushData, flushData);
		delete m_ctrlFile;
	}

	// �����ݿ�û�гɹ���ʱ������ָ�����ΪNULL����C++
	// ��֤delete��ָ�벻�����κβ�������˲����ж�
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
* ָ��tableId���õ���Ӧ��TableInfo
* Ŀǰ���˺������ڻָ������е���
* @param	tableId ��Id
* @return	TableInfoʵ��
*/
TableInfo* Database::getTableInfoById(u16 tableId) {

	TableInfo *tableInfo = m_idToTables->get(tableId);

	assert(tableInfo != NULL);

	return tableInfo;
}

/**
 * �رմ��еı�
 *
 * @param flushData �Ƿ�д������
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
 * ����һ��Database����
 *
 * @param config ���ݿ�����
 *   �ڴ�ʹ��Լ����ֱ�����������ݿ�ر�
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
 * ����һ��Database����
 * @pre �����ȵ���close�ر����ݿ�
 */
Database::~Database() {

}

/** ��ʼ������״̬ */
void Database::init() {
	memset(this, 0, sizeof(*this));
}

/** ������ݿ�״̬
 * @return ���ݿ�״̬
 */
DbStat Database::getStat() const {
	return m_stat;
}

/**
 * ������ݿ�����
 *
 * @return ���ݿ�����
 */
Config* Database::getConfig() const {
	assert(!m_closed);
	return m_config;
}

/**
 * ���OpenTable LRU ����
 *
 * @return OpenTableLRU
 */
DList<Table*>*	Database::getOpenTableLRU() const{
	return m_openTablesLink;
}
/**
 * ����������ʱ���DDL X����ʹ�ý���ֻ�ܴ���ִ��
 * @pre tableDef->m_idΪTableDef::INVALID_TABLEID
 * @post tableDef->m_idΪ�½����ID
 *
 * @param session �Ự����
 * @param path ���ļ�·��(�����basedir������������׺
 * @param tableDef ���壬���б��ID��û������
 * @throw NtseException ��ID�Ѿ����ָ꣬���ı��Ѿ����ڵȣ�DDL��������ֹ��
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
		// NTSETNT-316 ����allocTableId()����Ҳ���׳�Exception
		// �����Ҫ���������ͬ����try��������������ᵼ��DDL��й©
		tableDef->m_id = m_ctrlFile->allocTableId();	

		Table::create(this, session, path, tableDef);
	} catch (NtseException &e) {
		releaseDDLLock(session, IL_X);
		throw e;
	}
	// TODO: ���ܻ������������ļ�

	// ������־�����ڴ������ļ�֮�󣬸��¿����ļ�֮ǰ�����ڴ������ļ�֮ǰд��־��
	// �����ʹ��һ�����񣬷���ָ�ʱ��֪����������Ƿ�ɹ������ڸ��¿����ļ�֮��
	// ��д��־������ܳ��ֽ����Ѿ��ɹ�����û�ж�Ӧ��־�������Υ����WALԭ��
	//
	// ����ʱֻдһ��LOG_CREATE_TABLE��־�������Ƿ�ɹ�ȡ���ڿ����ļ���û�и��£�
	// ������ļ��Ѿ������������ļ�û�и�������Ϊ�����ɹ�����������־û��д����
	// ��ָ�ʱ����ȥɾ����Щ�������ļ���û�����⡣
	m_txnlog->flush(writeCreateLog(session, path, tableDef));
	SYNCHERE(SP_DB_CREATE_TABLE_AFTER_LOG);
	m_ctrlFile->createTable(path, tableDef->m_id, tableDef->hasLob(), false);

	releaseDDLLock(session, IL_X);
}

/**
 * д��������־
 *
 * @param session �Ự
 * @param path ���ļ�·��
 * @param tableDef ����
 * @return ������־LSN
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
 * ������������־
 *
 * @param log LOG_CREATE_TABLE��־����
 * @param tableDef OUT������
 * @return ���ļ�·��
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
 * ɾ�����������ֻ������ɾ���������ֵ��д��ڵı���ָ���ı���
 * �����ֵ��в������򱾺����������κβ�����
 *
 * @pre ��ǰ�̲߳��ܴ�Ҫɾ���ı�����������̴߳���Ҫɾ���ı�
 *   �򱾺�����ȴ������ձ��ر�
 *
 * @param session �Ự
 * @param path ���ļ�·������������׺����Сд������
 * @param timeoutSec ��ʱʱ�䣬��λ�룬Ϊ<0��ʾ����ʱ��0��ʾ���ϳ�ʱ��>0Ϊ��ʱʱ��
 * @throw NtseException ָ���ı����ڵȣ���ʱ��
 */
void Database::dropTable(Session *session, const char *path, int timeoutSec) throw(NtseException) {
	ftrace(ts.ddl, tout << path);
	assert(!m_closed && m_stat != DB_CLOSING && !m_unRecovered);

	acquireDDLLock(session, IL_X, __FILE__, __LINE__);

	// �����Ƿ���ڣ������ڼ���DDL��֮�����
	u16 tableId = m_ctrlFile->getTableId(path);
	if (tableId == TableDef::INVALID_TABLEID) {
		releaseDDLLock(session, IL_X);
		return;
	}
	string pathStr = m_ctrlFile->getTablePath(tableId);
#ifdef NTSE_KEYVALUE_SERVER
	// ֪ͨkeyvalue���ñ����ڱ��ر�
	/** �ȴ����е����жԹرձ��������֮�󣬹رձ� */
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

	// �ڽ������и���֮ǰд��־��ǿ��д����־��һ����־д����ɾ�����������ǿ����ɣ�
	// ������Ϊɾ��������ǲ�����ת�ģ��޷��ָ�ɾ����һ��ı�
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
 * ɾ�����������ֻ������ɾ���������ֵ��д��ڵı���ָ���ı���
 * �����ֵ��в������򱾺����������κβ�����
 *
 * @pre ��ǰ�̲߳��ܴ�Ҫɾ���ı�����������̴߳���Ҫɾ���ı�
 *   �򱾺�����ȴ������ձ��ر�
 *
 * @param session �Ự
 * @param path ���ļ�·������������׺����Сд������
 * @param timeoutSec ��ʱʱ�䣬��λ�룬Ϊ<0��ʾ����ʱ��0��ʾ���ϳ�ʱ��>0Ϊ��ʱʱ��
 * @throw NtseException ָ���ı����ڵȣ���ʱ��
 */
void Database::dropTableSafe(Session *session, const char *path, int timeoutSec) throw(NtseException) {
	ftrace(ts.ddl, tout << path);
	assert(!m_closed && m_stat != DB_CLOSING && !m_unRecovered);
	assert(getDDLLock(session) == IL_X);
	assert(getATSLock(session) == IL_S);

	// �����Ƿ���ڣ������ڼ���DDL��֮�����
	u16 tableId = m_ctrlFile->getTableId(path);
	if (tableId == TableDef::INVALID_TABLEID) {
		return;
	}
	string pathStr = m_ctrlFile->getTablePath(tableId);
#ifdef NTSE_KEYVALUE_SERVER
	// ֪ͨkeyvalue���ñ����ڱ��ر�
	/** �ȴ����е����жԹرձ��������֮�󣬹رձ� */
	KeyValueServer::closeDDLTable(this, session, path);
#endif

	try {
		waitRealClosed(session, pathStr.c_str(), timeoutSec);
		checkPermission(session, pathStr.c_str());
	} catch (NtseException &e) {
		throw e;
	}

	// �ڽ������и���֮ǰд��־��ǿ��д����־��һ����־д����ɾ�����������ǿ����ɣ�
	// ������Ϊɾ��������ǲ�����ת�ģ��޷��ָ�ɾ����һ��ı�
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


/** ����CREATE TABLE����
 * @pre �����ļ���û�и��£������Ҫ����
 *
 * @param session �Ự
 * @param log LOG_CREATE_TABLE��־
 * @throw NtseException �ļ�����ʧ��
 */
void Database::redoCreateTable(Session *session, const LogEntry *log)  throw(NtseException) {
	ftrace(ts.recv, tout << session << log);
	assert(log->m_logType == LOG_CREATE_TABLE);
	assert(m_ctrlFile->getTablePath(log->m_tableId).empty());

	TableDef *tableDef = new TableDef();
	char *path = parseCreateLog(log, tableDef);
	// ����ʱ���ڱ������ļ��Ѿ�����֮����д��־����������־�Ѿ�д������һ���Ѿ����ã�ֻ����Ҫ���¿����ļ�
	Table *t = Table::open(this, session, path, false);
	assert(*tableDef == *(t->getTableDef(true, session)));
	t->close(session, true, true);
	delete t;
	m_ctrlFile->createTable(path, log->m_tableId, tableDef->hasLob(), false);
	delete []path;
	delete tableDef;
}

/**
 * ����DROP TABLE����
 *
 * @param session �Ự
 * @param log LOG_DROP_TABLE��־
 * @throw NtseException �ļ�����ʧ��
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
 * ����TRUNCATE����
 *
 * @param session �Ự
 * @param log LOG_TRUNCATE��־
 * @throw NtseException �ļ�����ʧ�ܵ�
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

	//�޸Ŀ����ļ����Ƿ���ȫ���ֵ����Ϣ
	alterCtrlFileHasCprsDic(session, log->m_tableId, newHasDict);
}

/**
 * дDROP TABLE��־
 *
 * @param db ���ݿ�
 * @param tableId ��ID
 * @param path ���ļ�·��(�����basedir)����������׺
 * @return ��־LSN
 */
u64 Database::writeDropLog(Database *db, u16 tableId, const char *path) {
	byte buf[Limits::PAGE_SIZE];
	Stream s(buf, sizeof(buf));
	s.write(path);
	return db->getTxnlog()->log(0, LOG_DROP_TABLE, tableId, buf, s.getSize());
}

/**
 * ����DROP TABLE��־
 *
 * @param log LOG_DROP_TABLE��־
 * @return ���ļ�·��(�����basedir)����������׺
 */
char* Database::parseDropLog(const LogEntry *log) {
	assert(log->m_logType == LOG_DROP_TABLE);
	Stream s(log->m_data, log->m_size);
	char *path;
	s.readString(&path);
	return path;
}

/**
 * �򿪱����ָ���ı��Ѿ��򿪣����������ü���
 *
 * @param session �Ự
 * @param path ���ļ�·��(�����basedir������������׺����Сд������
 * @return �����
 * @throw NtseException ָ���ı����ڵ�
 */
Table* Database::openTable(Session *session, const char *path) throw(NtseException) {
	ftrace(ts.ddl, tout << session << path);
	assert(!m_closed && m_stat != DB_CLOSING && !m_unRecovered);

	bool lockedByMe = false;
	if (getDDLLock(session) == IL_NO) {
		// ����Ǳ��ݲ������Ѿ������������Ҫ�ж��Ƿ��Ѿ�����
		acquireDDLLock(session, IL_IS, __FILE__, __LINE__);	// ��ֹ�򿪱�����б�RENAME��DROP
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
		string pathStr = m_ctrlFile->getTablePath(tableId);	// ��Сд�봫���path���ܲ�ͬ
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

		// ����open tables link	
		m_openTablesLink->addLast(&tableInfo->m_table->getOpenTablesLink());	
		// ͳ����Ϣ
		m_status.m_realOpenTableCnt++;
	} else {
		tableInfo->m_refCnt++;
		// ���refCnt��0����Ϊ1����ô���������е�λ��
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
 * �رձ�����������������ֻ�����ü����������ü�������0����ϵͳ��
 * �Զ�ˢд�����е������ݡ�
 * @pre �Ѿ��ͷ��˱���
 * @post �����������ã���table�����Ѿ������ٲ�����ʹ��
 *
 * @param session �Ự
 * @param table Ҫ�رյı�
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
	
	// ���refCntΪ0���ƶ������е�λ��
	if (tableInfo->m_refCnt == 0) {
		m_openTablesLink->moveToFirst(&tableInfo->m_table->getOpenTablesLink());
	}

	releaseATSLock(session, IL_X);
}

/**	�������Ϊ0�رձ�
* @session	�رձ�session
* @table	�Ѵ򿪵ı�
* @return	�Ƿ�ɹ��رձ�
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
 * TRUNCATE��
 *
 * @param session        �Ự
 * @param table          ҪTRUNCATE�ı�
 * @param tblLock        �Ƿ�ӱ���
 * @param isTruncateOper �Ƿ���truncate����
 * @throw NtseException  �ӱ�����ʱ
 */
void Database::truncateTable(Session *session, Table *table, bool tblLock, bool isTruncateOper) throw(NtseException) {
	ftrace(ts.ddl, tout << session << table->getPath());
	assert(!m_closed && m_stat != DB_CLOSING && !m_unRecovered);

	acquireDDLLock(session, IL_IX, __FILE__, __LINE__);
#ifdef NTSE_KEYVALUE_SERVER
	// ֪ͨkeyvalue���ñ����ڱ�truncate
	/** �ȴ����е����жԹرձ��������֮�󣬹رձ� */
	bool existInKeyvalue = KeyValueServer::closeDDLTable(this, session, table->getPath(), false);
	if(existInKeyvalue) {
		TableInfo *tableInfo = m_idToTables->get(table->getTableDef(true, session)->m_id);
		assert(tableInfo);
		assert(tableInfo->m_table == table);

		tableInfo->m_refCnt--;
	}
#endif

	try {
		// TODO �쳣�����ô����

		//��ȻTable::truncate��ͨ�������µĿձ���ʵ�֣�������Id��tableName��û�иı�
		bool newHasDict = false;
		table->truncate(session, tblLock, &newHasDict, isTruncateOper);

		//�޸Ŀ����ļ����Ƿ���ȫ���ֵ����Ϣ
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
 * ��������
 * @pre ��ǰ�̲߳��ܴ�Ҫ�������ı�����������̴߳��˸ñ�
 *   �򱾺�����ȴ������ձ��ر�
 *
 * @param session �Ự
 * @param oldPath ��ԭ���洢·������Сд������
 * @param newPath �����ڴ洢·��
 * @param timeoutSec ��ʱʱ�䣬��λ�룬Ϊ<0��ʾ����ʱ��0��ʾ���ϳ�ʱ��>0Ϊ��ʱʱ��
 * @throw NtseException �ļ�����ʧ�ܣ�ԭ�����ڣ�Ŀ�ı��Ѿ����ڣ���ʱ��
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
	// ֪ͨkeyvalue���ñ����ڱ��ر�
	/** �ȴ����е����жԹرձ��������֮�󣬹رձ� */
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

	// ���ͨ����飬ϵͳ����֤������һ���ɹ��������ɹ���������лָ���
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
 * ��������
 * @pre ��ǰ�̲߳��ܴ�Ҫ�������ı�����������̴߳��˸ñ�
 *   �򱾺�����ȴ������ձ��ر�
 *
 * @param session �Ự
 * @param oldPath ��ԭ���洢·������Сд������
 * @param newPath �����ڴ洢·��
 * @param timeoutSec ��ʱʱ�䣬��λ�룬Ϊ<0��ʾ����ʱ��0��ʾ���ϳ�ʱ��>0Ϊ��ʱʱ��
 * @throw NtseException �ļ�����ʧ�ܣ�ԭ�����ڣ�Ŀ�ı��Ѿ����ڣ���ʱ��
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
	// ֪ͨkeyvalue���ñ����ڱ��ر�
	/** �ȴ����е����жԹرձ��������֮�󣬹رձ� */
	KeyValueServer::closeDDLTable(this, session, oldPath);
#endif

	try {
		waitRealClosed(session, oldPath, timeoutSec);
		checkPermission(session, oldPath);
	} catch (NtseException &e) {
		throw e;
	}
	assert(getATSLock(session) == IL_S);

	// ���ͨ����飬ϵͳ����֤������һ���ɹ��������ɹ���������лָ���
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
 * д��������־
 *
 * @param session �Ự
 * @param tableId ��ID
 * @param oldPath ԭ·��(�����basedir)����������׺��
 * @param newPath ��·��(�����basedir)����������׺��
 * @param hasLob �Ƿ���������
 * @return ��־LSN
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
 * ������������־
 *
 * @param log LOG_RENAME_TABLE��־
 * @param oldPath OUT��ԭ·��
 * @param newPath OUT����·��
 * @param hasLob OUT�����Ƿ���������
 * @param hasCprsDict OUT, ���Ƿ���ѹ���ֵ�
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
 * �޸ı������
 *
 * @param session		�Ự��
 * @param table		�����ı�
 * @param name			�޸ĵĶ�������
 * @param valueStr		�޸ĵ�ֵ��
 * @param timeout		������ʱʱ�䣬��λ��
 * @param inLockTables  �����Ƿ���Lock Tables ���֮��
 * @throw NtseException	�޸Ĳ���ʧ�ܡ�
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
 * �޸ı��Ƿ���ȫ��ѹ���ֵ��ļ�
 * @param session    �Ự
 * @param path       ��·��
 * @param hasCprsDict �Ƿ���ȫ��ѹ���ֵ��ļ�
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
 * �Ż�������
 *
 * @param session �Ự
 * @param table Ҫ�Ż��ı�
 * @param keepDict �Ƿ�ָ���˱������ֵ��ļ�(����еĻ�)
 * @throw NtseException �ӱ�����ʱ���ļ������쳣���ڷ����쳣ʱ����������֤ԭ�����ݲ��ᱻ�ƻ�
 */
void Database::optimizeTable(Session *session, Table *table, bool keepDict /*=false*/, bool waitForFinish /*=true*/) throw(NtseException) {
	UNREFERENCED_PARAMETER(session);
	if (waitForFinish) {
		OptimizeThread thd(this, m_bgCustomThdsManager, table, keepDict, false);
		thd.start();
		thd.join();
	} else {
		//BgCustomThread�����ɺ�̨�̹߳������������
		BgCustomThread *thd = new OptimizeThread(this, m_bgCustomThdsManager, table, keepDict, true);
		m_bgCustomThdsManager->startBgThd(thd);
	}
}

/**
 * �Ż�������ʵ�ʲ���
 *
 * @param session �Ự
 * @param path ��·���������basedir����Сд������
 * @param keepDict �Ƿ�ָ���˱������ֵ��ļ�(����еĻ�)
 * @param cancelFlag ����ȡ����־
 * @throw NtseException �ӱ�����ʱ���ļ������쳣���ڷ����쳣ʱ����������֤ԭ�����ݲ��ᱻ�ƻ�
 */
void Database::doOptimize(Session *session, const char *path, bool keepDict, 
						  bool *cancelFlag) throw(NtseException) {
	acquireDDLLock(session, IL_IX, __FILE__, __LINE__);
	Table *table = NULL;

	//�����û�д򿪣����ȴ򿪱�
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

		//���¿����ļ�
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
 * ȡ����̨�������е��û��߳�
 * @param thdId ���ݿ�����ID
 * @return 
 */
void Database::cancelBgCustomThd(uint connId) throw(NtseException) {
	m_bgCustomThdsManager->cancelThd(connId);
}

/**
 * ��������
 *
 * @param session �Ự����
 * @param table �����ı�
 * @param numIndice Ҫ���ӵ�����������ΪonlineΪfalseֻ��Ϊ1
 * @param indexDefs ����������
 * @throw NtseException �ӱ�����ʱ�������޷�֧�֣�����Ψһ������ʱ���ظ���ֵ��
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
 * ����ѹ��ȫ���ֵ�
 * @pre ���Ѿ�����
 * @param session �Ự
 * @param table Ҫ����ѹ���ֵ�ı�
 * @throw NtseException ��û�б�����Ϊѹ�������Ѿ�����ȫ���ֵ䣬���������
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
		//���׳��쳣
		throw e;
	}	
	if (dataLockMode != IL_NO)
		table->unlock(session, dataLockMode);
	if (metaLockMode != IL_NO)
		table->unlockMeta(session, metaLockMode);
	releaseDDLLock(session, IL_IX);
}

/**
 * ɾ������
 *
 * @param session �Ự����
 * @param table �����ı�
 * @param idx Ҫɾ�����ǵڼ�������
 * @throw NtseException �ӱ�����ʱ
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

/** ���±��flushLsnΪ��ǰ��־β
 * @param session �Ự
 * @param tableId ��ID
 * @param flushed �������Ƿ��Ѿ�д��
 */
void Database::bumpFlushLsn(Session *session, u16 tableId, bool flushed) {
	// �ȼ�¼��ǰ��־β����дLOG_BUMP_FLUSH_LSN��־��ʹ���ڻָ�ʱLOG_BUMP_FLUSH_LSN��־����
	// ��������
	u64 tailLsn = m_txnlog->tailLsn();
	m_txnlog->flush(writeBumpFlushLsnLog(session, tableId, tailLsn, flushed));
	m_cbManager->callback(TABLE_ALTER, NULL, NULL, NULL, NULL);
	m_ctrlFile->bumpFlushLsn(tableId, tailLsn);
}

/** ���±��flushLsn, tntFlushLsnΪ��ǰ��־β
 * @param session �Ự
 * @param tableId ��ID
 * @param flushed �������Ƿ��Ѿ�д��
 */
void Database::bumpTntAndNtseFlushLsn(Session *session, u16 tableId, bool flushed) {
	// �ȼ�¼��ǰ��־β����дLOG_BUMP_FLUSH_LSN��־��ʹ���ڻָ�ʱLOG_BUMP_FLUSH_LSN��־����
	// ��������
	u64 tailLsn = m_txnlog->tailLsn();
	u64 lsn = writeBumpFlushLsnLog(session, tableId, tailLsn, flushed);
	m_txnlog->flush(lsn);
	m_ctrlFile->bumpTntAndNtseFlushLsn(tableId, tailLsn, tailLsn);
}

/** ��������flushLsn����
 * @param session �Ự
 * @param log LOG_BUMP_FLUSH_LSN��־
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

/** дLOG_BUMP_FLUSH_LSN��־
 * @param session �Ự
 * @param tableId ��ID
 * @param flushLsn �µ�flushLsn
 * @param flushed ���е������Ƿ��Ѿ�д��
 * @return ��д��LOG_BUMP_FLUSH_LSN��־��LSN
 */
u64 Database::writeBumpFlushLsnLog(Session *session, u16 tableId, u64 flushLsn, bool flushed) {
	byte buf[sizeof(u64) + sizeof(bool)];
	Stream s(buf, sizeof(buf));
	s.write(flushLsn);
	s.write(flushed);
	return session->writeLog(LOG_BUMP_FLUSHLSN, tableId, buf, s.getSize());
}

/** ����LOG_BUMP_FLUSH_LSN��־
 * @param log LOG_BUMP_FLUSH_LSN��־����
 * @param flushed OUT�����е������Ƿ��Ѿ�д��
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
 * ������־������
 *
 * @return ��־������
 */
Txnlog* Database::getTxnlog() const {
	assert(!m_closed);
	return m_txnlog;
}

/**
 * ����ҳ�滺�������
 *
 * @return ҳ�滺�������
 */
Buffer* Database::getPageBuffer() const {
	assert(!m_closed);
	return m_buffer;
}

/**
 * ��������������
 *
 * @return ����������
 */
LockManager* Database::getRowLockManager() const {
	assert(!m_closed);
	return m_rowLockMgr;
}

/**
 * ��������ҳ��������
 *
 * @return ����ҳ��������
 */
IndicesLockManager* Database::getIndicesLockManager() const {
	assert(!m_closed);
	return m_idxLockMgr;
}

/**
 * ����ϵͳ��־������
 *
 * @return ϵͳ��־������
 */
Syslog* Database::getSyslog() const {
	assert(!m_closed);
	return m_syslog;
}

/**
 * ���ػỰ������
 *
 * @return �Ự������
 */
SessionManager* Database::getSessionManager() const {
	assert(!m_closed);
	return m_sessionManager;
}

/**
 * ����MMSϵͳ
 *
 * @return MMSϵͳ
 */
Mms* Database::getMms() const {
	assert(!m_closed);
	return m_mms;
}

/**
 * �㱨����״̬��Ϣ
 *
 * @param status �������
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

/** �������ڴ������IO��������
 * @return �������IO��������
 */
uint Database::getPendingIOOperations() const {
	assert(!m_closed);
	return m_buffer->getStatus().m_pendingReads.get() + m_buffer->getStatus().m_pendingWrites.get();
}

/** �����Ѿ����е�IO��������
 * @return �Ѿ����е�IO��������
 */
u64 Database::getNumIOOperations() const {
	assert(!m_closed);
	return m_buffer->getStatus().m_physicalReads + m_buffer->getStatus().m_physicalWrites;
}

/** ��ӡ���ݿ�״̬
 * @param out �����
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

/** Ψһȷ��һ��ҳ�滺��ֲ���ļ� */
struct BuKey {
	u16			m_tblId;	/** ��ID */
	DBObjType		m_type;		/** �������� */
	const char	*m_idxName;	/** ��������ΪBU_Indexʱָ��������������ΪNULL */

	BuKey(u16 tblId, DBObjType type, const char *idxName) {
		assert(idxName);
		m_tblId = tblId;
		m_type = type;
		m_idxName = idxName;
	}
};

/** ����BuKey�����ϣֵ�ĺ������� */
class BkHasher {
public:
	unsigned int operator ()(const BuKey &key) const {
		unsigned int h = m_strHasher.operator()(key.m_idxName);
		return h + key.m_tblId + key.m_type;
	}

private:
	Hasher<const char *>	m_strHasher;
};

/** ����BufUsage�����ϣֵ�ĺ������� */
class BuHasher {
public:
	unsigned int operator ()(const BufUsage *usage) const {
		BuKey key(usage->m_tblId, usage->m_type, usage->m_idxName);
		return m_keyHasher.operator()(key);
	}
private:
	BkHasher	m_keyHasher;
};

/** ����BuKey��BufUsagec�����Ƿ���ȵĺ������� */
class BkBuEqualer {
public:
	bool operator()(const BuKey &key, const BufUsage *usage) const {
		return !strcmp(key.m_idxName, usage->m_idxName)
			&& key.m_tblId == usage->m_tblId && key.m_type == usage->m_type;
	}
};

/**
 * ͳ��ҳ�滺��ֲ���Ϣ
 * ע�����ڽ�������ģʽ�޸Ĳ����ı����Ϣ���������޷������򱻺���
 *
 * @param session �Ự
 * @return ҳ�滺��ֲ���Ϣ
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
		// ���������ļ�·����ȥ��basedirǰ׺�ͺ�׺������ȡ����·��
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
			// �ڱ��򿪵Ĺ����У����е�����ҳ������Buffer�д��ڣ���pinTableIfOpened��
			// ����NULL
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
						// ���ڽ�������ʱ�򷵻ص�idx���ܱ�ʾ���ڽ����Ǹ���������ʱ��û�и���TableDef
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
 * ����һ�����ӣ������ӳ��л�ȡ��
 *
 * @param internal �Ƿ�Ϊ�ڲ����ӣ��ڲ�����ָ��Ϊ��ִ�����û�����û��
 *   ֱ�ӹ�ϵ�Ĳ���������㡢MMSˢ���»���ȣ�����ȡ������
 * @param name ��������
 * @return ���ݿ�����
 */
Connection* Database::getConnection(bool internal, const char *name) {
	ftrace(ts.dml || ts.ddl, tout << internal);
	assert(!m_closed);
	return m_sessionManager->getConnection(internal, &m_config->m_localConfigs, &m_status.m_opStat, name);
}

/**
 * �ͷ�һ�����ӣ������ӳأ�
 * @pre Ҫ�ͷŵ�����connΪʹ���е�����
 *
 * @param ���ݿ�����
 */
void Database::freeConnection(Connection *conn) {
	ftrace(ts.dml || ts.ddl, tout << conn);
	assert(!m_closed);
	m_sessionManager->freeConnection(conn);
}

/**
 * �����Ƿ�������м��㡣������ò�������м����ҵ�ǰ���ڽ��м��㣬
 * �򱾺�����ȴ���ǰ���������ɺ��ٷ��أ�ͬʱҲ������ֱ�ӵ���checkpoint
 * �������м����������֤�������غ󲻿����н����еļ���������ڡ�
 *
 * @param enabled �Ƿ��ڽ��м���
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
 * �����µļ���ִ������
 *
 * @param sec ��ָ���ļ������ڣ���λ��
 */
void Database::setCheckpointInterval(uint sec) {
	m_chkptTask->setInterval(min(sec, (uint)10) * 1000);
	((Checkpointer *)m_chkptTask)->setInterval((u32)sec);
}

/**
 * ����ִ��һ�μ������
 *
 */
void Database::doCheckpoint() {
	((Checkpointer *)m_chkptTask)->setInstant(true);
	m_chkptTask->signal();
}

/**
 * ����һ�μ���
 *
 * @param session �Ự
 * @param targetTime ���������ָ����ʱ���֮ǰ��ɣ���λ��
 * @throw NtseException ���㱻���ã����߲�����ȡ��
 */
void Database::checkpoint(Session *session, AioArray *array) throw(NtseException) {
	ftrace(ts.ddl || ts.dml, tout << session);
	assert(!m_closed && !m_unRecovered);

	m_status.m_checkpointCnt++;

	if (m_chkptTask->isPaused())
		NTSE_THROW(NTSE_EC_GENERIC, "Checkpointing has been disabled.");

	MutexGuard guard(m_chkptLock, __FILE__, __LINE__);


	// ����ָ���ʼ�㣬���л�Ծ������Ϊ��Ծ������С����ʼLSN��
	// ����Ϊ��־β
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

	// ˢMMS���ڼ��½��ı����־һ���ڻָ���ʼ��־֮��û������
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
	// �쳣ֱ���׳�
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
 * �õ���ǰ���򿪵ı��·������
 *
 * @param session �Ự
 * @return ��ǰ���򿪵ı��·���������basedir������
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
 * �õ���ǰ���򿪵ı�ļ���
 *
 * @param session �Ự
 * @return ��ǰ���򿪵ı�ģ������basedir������
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
 * ���ָ���ı��������������ü���
 *
 * @param session �Ự
 * @param path ��·���������basedir����Сд������
 * @param ��ʱʱ�䣬����Ϊ<0�򲻳�ʱ����Ϊ0�����ϳ�ʱ���൱��tryLock��>0Ϊ����Ϊ��λ�ĳ�ʱʱ��
 * @return �������򷵻ر���Ϣ�����򷵻�NULL
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
		// ���refCnt��0����Ϊ1����ô���������е�λ��
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
 * ��ÿ����ļ�
 *
 * @return �����ļ�
 */
ControlFile* Database::getControlFile() const {
	assert(!m_closed);
	return m_ctrlFile;
}

/**
 * ���й��ϻָ�
 *
 * @throw NtseException һЩDDL����redoʱ��ʧ��
 */
void Database::recover() throw(NtseException) {
	assert(m_stat == DB_MOUNTED);
	ftrace(ts.recv, );
	m_stat = DB_RECOVERING;
	u64 checkpointLsn = m_ctrlFile->getCheckpointLSN();

	m_syslog->log(EL_LOG, "Start database recovery from LSN: "I64FORMAT"d.", checkpointLsn);

	m_activeTransactions = new Hash<u16, Transaction *>(m_config->m_maxSessions * 2);

	// �ط���־
	LogScanHandle *logScan = m_txnlog->beginScan(checkpointLsn, m_txnlog->tailLsn());
	int oldPercent = 0;
	int oldTime = System::fastTime();
	u64 lsnRange = m_txnlog->tailLsn() - checkpointLsn+ 1;
	while (m_txnlog->getNext(logScan)) {
		LsnType lsn = logScan->curLsn();
		const LogEntry *logEntry = logScan->logEntry();
#ifdef TNT_ENGINE
		//�����tnt��־��ֱ������
		if (logEntry->isTNTLog()) {
			continue;
		}
#endif
		LogType type = logEntry->m_logType;
		nftrace(ts.recv, tout << "Got log: " << lsn << "," << logEntry);

		// ���ÿ10���ӡһ�λָ�������Ϣ
		int percent = (int)((lsn - checkpointLsn) * 100 / lsnRange);
		if (percent > oldPercent && (System::fastTime() - oldTime) > 10) {
			m_syslog->log(EL_LOG, "Done replay of %d%% logs.", percent);
			oldPercent = percent;
			oldTime = System::fastTime();
		}
		
		if (canSkipLog(lsn, logEntry))
			continue;

		if (logEntry->m_txnId == 0) {	// ���������
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
	// �������ر�ʱ����Ҫ�����ؽ�����λͼ�ȹ���
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

/** ��֤���ݿ���ȷ��
 * @pre �ָ��Ѿ����
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
/** ����session�Ƿ���в������ű��Ȩ��
 * @ע��㣬�������ֻ��tableDef�ļ���Ҳδ���κε�lock��Ŀǰֻ����drop��rename waitrealclose�����
 *          ��Ϊ��ʱtable���ᱻ������Ӵ򿪣�δ��meta lock��ȡtabledef��Ϣ�ǰ�ȫ��
 * @param �Ự
 * @tableName �ж�Ȩ�޵ı�
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

/** �ж���־�Ƿ���Ա�����
 * @param lsn ��־LSN
 * @param log ��־
 * @return ��־�Ƿ���Ա�����
 */
bool Database::canSkipLog(u64 lsn, const LogEntry *log) {
	assert(log->m_tableId != TableDef::INVALID_TABLEID);
	u16 tableId = log->m_tableId;
	if (TableDef::tableIdIsVirtualLob(tableId))
		tableId = TableDef::getNormalTableId(tableId);

	// �����Ƿ��Ѿ���ɾ�����������ɾ�������в���������Ҫ����������DROP TABLE����
	if (m_ctrlFile->isDeleted(tableId)) {
		nftrace(ts.recv, tout << "Skip log " << lsn << "because table " << log->m_tableId << " has been dropped");
		return true;
	}
	if (log->m_logType == LOG_CREATE_TABLE) {
		// ���ڸ��¿����ļ��Ǵ���������һ�����������ID�ڿ����ļ����Ѿ��������ʾ����
		// �����Ѿ��ɹ����ָ�ʱ������
		if (!m_ctrlFile->getTablePath(tableId).empty()) {
			nftrace(ts.recv, tout << "Skip CREATE TABLE log " << lsn << "because table " << log->m_tableId << " already exists");
			return true;
		}
	} else {
		assert(!m_ctrlFile->getTablePath(tableId).empty());	// ��û�б�ɾ�����ֲ��ǽ�����־�����һ������
		// �����flushLsn > lsn����ǰ��־���޸��Ѿ��־û�������
		if (m_ctrlFile->getFlushLsn(tableId) > lsn) {
			nftrace(ts.recv, tout << "Skip log " << lsn << " because flushLsn " << m_ctrlFile->getFlushLsn(tableId)
				<< " of table " << tableId << " is ahead of it");
			return true;
		}

		if (log->m_txnId && log->m_logType != LOG_TXN_START) {
			Transaction *trans = m_activeTransactions->get(log->m_txnId);
			if (!trans) {
				// ���������ֿ��ܣ�һ����Ϊ����ʼ��־�����������������ָ���ʼ�㣬
				// ���м���ʱ���ûָ���ʼ��Ϊ��Ծ������ʼLSN����Сֵ������������ָ�
				// ��ʼ���������һ���Ѿ��־û�����������
				nftrace(ts.recv, tout << "Skip log " << lsn << " because transaction is incomplete");
				return true;
			}
		}
	}
	return false;
}

/**
 * ����ID��ȡ������û�б����Զ��򿪱���ǰֻ���ڻָ�ʱ������һ����
 *
 * @param session �Ự
 * @param tableId ��ID
 * @throw NtseException �򿪱�ʧ��
 * @return ���������ڷ���NULL
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
 * ִ�������Ĺرձ����
 *
 * @post tableInfo��delete
 *
 * @param session �Ự
 * @param tableInfo ����Ϣ
 * @param flushDirty �Ƿ�ˢ��������
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
	 * ntsebinlog��keyvalue�򿪵ı���Ҫɾ��
	*/
	KeyValueServer::closeMysqlTable(tableID);
#endif

	// �����openTableLRU ������ժ��
	tableInfo->m_table->getOpenTablesLink().unLink();

	tableInfo->m_table->close(session, flushDirty);

	m_status.m_realCloseTableCnt++;

	delete tableInfo->m_table;
	delete tableInfo;
}

/**
 * �ȴ����������رա���һ����������Ҫ�Ա�ر�֮����ܽ��е�DROP/RENAME�Ȳ�������������
 * ��ʹӦ���Ѿ��ر��˱�����Ȼ�ᱻ���ݡ������������ʱpinסû�������ر�
 *
 * @pre �Ѿ�����ATS��S��
 * @post ATS����Ȼ���С�������ִ���ڼ�ATS�����ܱ��ͷ�
 *
 * @param session �Ự
 * @param path ��·������Сд������
 * @param timeoutSec ��ʱʱ�䣬��λ�룬Ϊ<0��ʾ����ʱ��0��ʾ���ϳ�ʱ��>0Ϊ��ʱʱ��
 * @throw NtseException ��ʱ��
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
		
		// ��ATS���������Թر�refCntΪ0�ı�
		acquireATSLock(session, IL_X, -1, __FILE__, __LINE__);
		tableInfo = m_pathToTables->get((char *)path);
		if (tableInfo) {	
			try {
				realCloseTableIfNeed(session, tableInfo->m_table);
			} catch (NtseException &e) {
				// �ر��״�һ��������ˢ������ʱ����I/O�쳣
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
 * ����DROP INDEX����
 *
 * @param session �Ự
 * @param lsn LOG_DROP_INDEX��־LSN
 * @param log LOG_DROP_INDEX��־����
 */
void Database::redoDropIndex(Session *session, LsnType lsn, const LogEntry *log) {
	ftrace(ts.recv, tout << session << lsn << log);
	assert(log->m_logType == LOG_IDX_DROP_INDEX);
	Table *table = getTable(log->m_tableId);
	table->redoDropIndex(session, lsn, log);
}

/**
 * ����RENAME TABLE����
 *
 * @param session �Ự
 * @param log LOG_RENAME_TABLE��־
 * @throw NtseException �ļ�����ʧ�ܵ�
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
 * ����������޸Ĳ�����
 *
 * @param session	�Ự��
 * @param log		LOG_ALTER_TABLE_ARG��־��
 * @throw	NtseException �Զ��������޸Ĳ���ʧ�ܵȡ�
 */
void Database::redoAlterTableArg(Session *session, const LogEntry *log) throw(NtseException) {
	ftrace(ts.recv, tout << session << log);
	assert(LOG_ALTER_TABLE_ARG == log->m_logType);	
	Table *table = getTable(log->m_tableId);
	table->redoAlterTableArg(session, log);
}

/**
 * �����������޸ġ�
 *
 * @param session	�Ự��
 * @param log		LOG_ALTER_INDICE��־��
 * @throw			��־���������޷��򿪶��ļ�ʱ�׳��쳣
 */
void Database::redoAlterIndice(Session *session, const LogEntry *log) throw(NtseException) {
	ftrace(ts.recv, tout << session << log);
	assert(LOG_ALTER_INDICE == log->m_logType);
	assert(!m_idToTables->get(log->m_tableId));
	Table::redoAlterIndice(session, log, this, m_ctrlFile->getTablePath(log->m_tableId).c_str());
}

/**
* ���������޸ġ�
*
* @param session	�Ự��
* @param log		LOG_ALTER_COLUMN��־��
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
 * ��������ѹ���ֵ�
 * 
 * @param session �Ự
 * @param log     LOG_CREATE_DICTIONARY��־��
 */
void Database::redoCreateCompressDict(ntse::Session *session, const ntse::LogEntry *log) {
	ftrace(ts.recv, tout << session << log);
	assert(LOG_CREATE_DICTIONARY == log->m_logType);

	//�޸Ŀ����ļ����Ƿ����ֵ���Ϊtrue
	string tablePath = m_ctrlFile->getTablePath(log->m_tableId);
	assert(!tablePath.empty());
	alterCtrlFileHasCprsDic(session, log->m_tableId, true);

	Table::redoCreateDictionary(session, this, tablePath.c_str());

	//������򿪣�ǿ�ƹر�֮���ڱ��滻�����֮�󲻻����
	TableInfo *tableInfo = m_idToTables->get(log->m_tableId);
	if (tableInfo)
		realCloseTable(session, tableInfo, false);
	assert(NULL == m_idToTables->get(log->m_tableId));
}

/**
 * ���������������޸Ĳ�����
 *
 * @param session	�Ự��
 * @param lsn		��־��
 * @param log		LOG_LOB_MOVE��־��
 * @throw			����ʧ�ܡ�
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
 * ��ʼһ������
 *
 * @param log LOG_TXN_START��־����
 * @throw NtseException �ò���ָ���ĻỰ
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
 * ����һ���������ָ�������񲻴����򲻽����κβ��������ָ��������
 * ���ڣ���û����ɣ��������������������ͼ�״̬�Զ���ɻ�ع�������
 *
 * @param log LOG_TXN_END��־����
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

/** �ӻ�Ծ������
 * @param session �Ự
 * @param mode ģʽ��ֻ��ΪIL_S��IL_X
 * @param timeoutMs <0��ʾ����ʱ��0�ڲ��������õ���ʱ���ϳ�ʱ��>0Ϊ����Ϊ��λ�ĳ�ʱʱ��
 * @param file �����ļ�
 * @param line �����к�
 * @throw NtseException ������ʱ�����ָ��timeousMsΪ<0�����׳��쳣
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

/** �ͷŻ�Ծ������
 * @param session �Ự
 * @param mode ģʽ��ֻ��ΪIL_S��IL_X
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

/** �õ�ָ���Ự���ӵĻ�Ծ������ģʽ
 * @param session �Ự
 * @return ָ���Ự���ӵĻ�Ծ������ģʽ��������IL_NO��IL_S��IL_X
 */
ILMode Database::getATSLock(Session *session) const {
	return m_atsLock->getLock(session->getId());
}

/** ��DDL����CREATE/DROP/RENAME����ʱ��X����TRUNCATE/ADD INDEX���޸ı�ṹ�Ĳ���ʱ��Ҫ��
 * IX�������߱���ʱ��Ҫ��S�����Ӷ���ֹ�ڱ����ڼ�����κ�DDL����
 *
 * @param session �Ự
 * @param mode ��ģʽ
 * @throw NtseException ������ʱ����ʱʱ�������ݿ������е�m_tlTimeoutָ��
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
 * �ͷ�DDL��
 *
 * @param session �Ự
 * @param mode ��ģʽ
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
 * �õ�ָ���Ự�ӵ�DDL��ģʽ
 *
 * @param session �Ự
 * @return ָ���Ự�ӵ�DDL��ģʽ
 */
ILMode Database::getDDLLock(Session *session) const {
	return m_ddlLock->getLock(session->getId());
}

///////////////////////////////////////////////////////////////////////////////
// ������ָ� ///////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////////

/**
 * ����һ���ļ��������Ѿ�����ʱ��������������ݣ�
 * @pre �Ѿ�����DDL������
 * @param backupProcess ���ݲ���
 * @param tableId       Դ�ļ���Ӧ�ı�id
 * @param suffix        Դ�ļ���׺
 * @throw NtseException �����ļ�ʱ��������
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
 * ����һ���ļ�
 * @pre ���㱻���ã������Ѿ�����DDL������
 *
 * @param backupProcess ���ݲ���
 * @param src Դ�ļ�
 * @param pageType ���ļ���ҳ����
 * @param tableId Դ�ļ���Ӧ�ı�id
 * @param dbObjStats ���ݿ����ݶ���״̬�ṹָ��
 * @param indice ֻ�ڱ��������ļ�ʱָ���������������ΪΪNULL
 * @param existLen �����ļ��Ѿ��г���
 * @return �Ѿ����ݵ��ļ�����
 * @throw NtseException IO�쳣
 */
u64 Database::doBackupFile(BackupProcess *backupProcess, File *src, PageType pageType, u16 tableId, DBObjStats *dbObjStats, DrsIndice *indice, u64 existLen) throw(NtseException) {
	assert(m_ddlLock->isLocked(backupProcess->m_session->getId(), IL_S));
	string backupPath = makeBackupPath(backupProcess->m_backupDir, basename(src->getPath()), tableId);
	File backupFile(backupPath.c_str());
	u64 srcSize = 0; // Դ�ļ�����
	try {
		u64 errNo;
		if (existLen) { // �����ļ��Ѿ����ڣ���
			if ((errNo = backupFile.open(false)) != File::E_NO_ERROR)
				NTSE_THROW(errNo, "Cannot open backup file %s", backupFile.getPath());
		} else { // ���������ļ�
			if ((errNo = backupFile.create(false, false)) != File::E_NO_ERROR)
				NTSE_THROW(errNo, "Cannot create backup file %s", backupFile.getPath());
		}

		if ((errNo = src->getSize(&srcSize)) != File::E_NO_ERROR)
			NTSE_THROW(errNo, "getSize of %s failed", src->getPath());
		SYNCHERE(SP_DB_BACKUPFILE_AFTER_GETSIZE);
		u64 backupSize = 0; // �����ļ����� 
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
				if (dirty) // ��ҳ��Ҫ���¼���У���
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
 * ����һ���ļ�
 * @pre ���㱻���ã������Ѿ�����DDL������
 *
 * @param backupProcess ���ݲ���
 * @param src Դ�ļ�
 * @param pageType ���ļ���ҳ����
 * @param tableId Դ�ļ���Ӧ�ı�id
 * @param dbObjStats ���ݿ����ݶ���״̬�ṹָ��
 * @param indice ֻ�ڱ��������ļ�ʱָ���������������ΪΪNULL
 * @throw NtseException IO�쳣
 */
void Database::backupFile( BackupProcess *backupProcess, File *src, PageType pageType, u16 tableId, DBObjStats *dbObjStats, DrsIndice *indice) throw(NtseException) {
	u64 len = doBackupFile(backupProcess, src, pageType, tableId, dbObjStats, indice, 0);
	backupProcess->m_files.insert(map<File *, u64>::value_type(src, len));	
}

/**
 * ���ݿ����ļ�
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
 * ����һ�����ݱ�
 * @pre ���㱻���ã������Ѿ�����DDL������
 *
 * @param backupProcess ���ݲ���
 * @param tableId ���������ݱ�ID
 * @throw NtseException д�����ļ�ʧ��
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

		// ���ݶ��ļ�
		SYNCHERE(SP_DB_BACKUP_BEFORE_HEAP);
		DrsHeap *heap = table->getHeap();
		int fileCnt = heap->getFiles(srcFiles, pageTypes, maxFileCnt);
		for (int i = 0; i < fileCnt; ++i)
			backupFile(backupProcess, srcFiles[i], pageTypes[i], tableId, heap->getDBObjStats());

		LobStorage *lobStore = table->getLobStorage();
		if (lobStore) { // ���ݴ��������
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
		if (indice) { // ������������
			int fileCnt = indice->getFiles(srcFiles, pageTypes, maxFileCnt);
			for (int i = 0; i < fileCnt; ++i)
				backupFile(backupProcess, srcFiles[i], pageTypes[i], tableId, indice->getDBObjStats(), indice);
		}
		//����ѹ���ֵ��ļ�
		if (table->hasCompressDict()) {
			backupFile(backupProcess, tableId, Limits::NAME_GLBL_DIC_EXT);
		}

		string srcTblDefPath = string(m_config->m_basedir) + NTSE_PATH_SEP + pathStr + Limits::NAME_TBLDEF_EXT;
		string backupTblDefPath = makeBackupPath(backupProcess->m_backupDir, basename(srcTblDefPath.c_str()), tableId);
		errNo = File::copyFile(backupTblDefPath.c_str(), srcTblDefPath.c_str(), true);
		if (errNo != File::E_NO_ERROR)
			NTSE_THROW(errNo, "copy %s to %s failed", srcTblDefPath.c_str(), backupTblDefPath.c_str());

		// ����mysql����ļ�
#ifdef TNT_ENGINE
		//��Ϊ���е�ddl����ֹ������meta lock���Բ���
		if (TS_SYSTEM != table->getTableDef()->getTableStatus()) {
#endif
			// ����mysql��� frm�ļ�
			backupFile(backupProcess, tableId, ".frm");
			//����mysql������ص�par�ļ�
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
 * ������־
 * @pre ���㱻���ã������Ѿ�����DDL����������firstPassΪfalse���Ѿ���ֹ
 *
 *
 * @param backupProcess ���ݲ���
 * @param firstPass �Ƿ��һ��
 * @throw NtseException д�����ļ�ʧ��
 */
void Database::backupTxnlog(BackupProcess *backupProcess, bool firstPass) throw(NtseException) {
	string backupFilename = string(backupProcess->m_backupDir) + NTSE_PATH_SEP + Limits::NAME_TXNLOG;
	File backupFile(backupFilename.c_str());
	u64 writtenSize = 0; // ��д�볤��
	u64 fileSize = 0; // ��־�ļ�����
	u64 errNo = 0;
	try {
		if (firstPass) { // ��һ�˴����ļ�
			if ((errNo = backupFile.create(false, false)) != File::E_NO_ERROR)
				NTSE_THROW(errNo, "Cannot create backup file %s", backupFile.getPath());
		} else { // �ڶ���ֱ�Ӵ��ļ�������ȡ�ļ���С
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
			if (writtenSize + dataSize > fileSize) { // ��չ�ļ�
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
 * ��ʼ�����ݲ���
 *
 * @param backupDir ���ݵ����Ŀ¼
 * @throw NtseException �޷����������ļ�����DDL����ʱ���Ѿ��б��ݲ����ڽ����е�
 */
BackupProcess* Database::initBackup(const char *backupDir) throw(NtseException) {
	assert(!m_closed  && !m_unRecovered);
	ftrace(ts.ddl, tout << backupDir);

	// ����Ƿ��Ѿ���һ�����ݲ�����ִ��
	if (!m_backingUp.compareAndSwap(0, 1))
		NTSE_THROW(NTSE_EC_NOT_SUPPORT, "Another backup process already exist.");

	// ���Ŀ��Ŀ¼�Ƿ�������ǿ�Ŀ¼
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
	// ����DDL���
	try {
		acquireDDLLock(backupProcess->m_session, IL_S, __FILE__, __LINE__);
	} catch (NtseException &e) {
		delete backupProcess;
		m_backingUp.set(0);
		throw e;
	}

	// ��ֹ�����ڼ��������־������
	backupProcess->m_onlineLsnToken = m_txnlog->setOnlineLsn(m_ctrlFile->getCheckpointLSN());
	m_syslog->log(EL_LOG, "Begin backup to %s", backupDir);
	return backupProcess;
}

/**
 * ���б���
 * @pre ������BS_INIT״̬
 * @post ���ɹ���ΪBS_COPYED״̬����ʧ��ΪBS_FAILED״̬
 *
 * @param backupProcess ���ݲ���
 * @throw NtseException д�����ļ�ʧ��
 */
void Database::doBackup(BackupProcess *backupProcess) throw(NtseException) {
	assert(m_ddlLock->isLocked(backupProcess->m_session->getId(), IL_S));
	assert(backupProcess->m_stat == BS_INIT);
	ftrace(ts.ddl, tout << backupProcess);

	backupProcess->m_stat = BS_FAILED;

	// ���ݿ����ļ������ݿ����ļ������ڱ��ݱ�����֮ǰ����ֹ�ڱ����ڼ�����˼���������»ָ���ʼ�����
	m_syslog->log(EL_LOG, "Backup control file...");
	LsnType logStartLsn = m_ctrlFile->getCheckpointLSN(); // ��־���ݵ���ʼLSN
	SYNCHERE(SP_DB_BEFORE_BACKUP_CTRLFILE);
	backupProcess->m_logBackuper = new LogBackuper(getTxnlog(), logStartLsn);
	backupCtrlFile(backupProcess);
		
	// ��ȡ����Ϣ
	u16 numTables = m_ctrlFile->getNumTables();
	u16 *tableIds = new u16[numTables];
	AutoPtr<u16> apTableIds(tableIds, true);
	m_ctrlFile->listAllTables(tableIds, numTables);
	// ����ÿ�ű������
	for (u16 i = 0; i < numTables; ++i)
		backupTable(backupProcess, tableIds[i]);
	SYNCHERE(SP_DB_BEFORE_BACKUP_LOG);
	// Ȼ�󱸷ݴ����һ��checkpoint��ʼ�����־��
	m_syslog->log(EL_LOG, "Backup log, startLSN "I64FORMAT"u ...", logStartLsn);
	backupTxnlog(backupProcess, true);
	// �������ݿ�����
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
 * �������ݲ����������ݿ�Ϊֻ��״̬
 * @pre ����ΪBS_COPYED״̬
 *
 * @param backupProcess ���ݲ���
 * @throw NtseException ������־ʱд�ļ�ʧ��
 */
void Database::finishingBackupAndLock(BackupProcess *backupProcess) throw(NtseException) {
	assert(backupProcess->m_stat == BS_COPYED);
	ftrace(ts.ddl, tout << backupProcess);

	backupProcess->m_stat = BS_FAILED;

	m_syslog->log(EL_LOG, "Lock database...");
	// Ϊ��֤����������binlog��һ�£���Ҫ�ڱ��ݼ�������ʱ��ֹ�����ݿ������
	// ����������ˢ��MMS���»�����־����һ��������ʵ��:
	// 1. �õ���ǰ�򿪵ı�������MMS���»���ı��б�
	// 2. ������ÿ�ű�pinס�ñ��ֹ�䱻ɾ����رգ�Ȼ��ˢMMS���»�����־
	// 3. ����openLock��ֹ���򿪻�ر�
	// 4. �������б���ֹ�Ա��д����
	// 5. ��Ϊ����MMS���»���ı��ٴ�ˢ��MMS���»�����־�����ڸ�ˢ��һ��
	//    ���ͨ���ȽϿ죬�������Լ���д���������õ�ʱ��

	// ��һ�λ�ȡ����MMS���»���ı��б�
	vector<string> cacheUpdateTables;

	acquireATSLock(backupProcess->m_session, IL_S, -1, __FILE__, __LINE__);

	TableInfo **tables = new TableInfo*[m_idToTables->getSize()];
	m_idToTables->values(tables);
	for (size_t i = 0; i < m_idToTables->getSize(); i++) {
		// ���ڿ�ʼ����ʱ�Ѿ���ֹ������DDL������������ﲻ��Ҫ�ӱ�Ԫ������
		TableDef *tableDef = tables[i]->m_table->getTableDef(false);
		if (tableDef->m_useMms && tableDef->m_cacheUpdate)
			cacheUpdateTables.push_back(tables[i]->m_path);
	}
	delete [] tables;

	releaseATSLock(backupProcess->m_session, IL_S);

	// ��һ��ˢMMS���»��棬С�ʹ����MMS�������ø��»�����˲���Ҫˢ
	for (size_t i = 0; i < cacheUpdateTables.size(); i++) {
		Table *tableInfo = pinTableIfOpened(backupProcess->m_session, cacheUpdateTables[i].c_str());
		// ���ڿ�ʼ����ʱ�Ѿ���ֹ������DDL������������ﲻ��Ҫ�ӱ�Ԫ������
		assert(getDDLLock(backupProcess->m_session) == IL_S);
		if (tableInfo) {
			TableDef *tableDef = tableInfo->getTableDef();
			if (tableDef->m_useMms && tableDef->m_cacheUpdate) {
				tableInfo->getMmsTable()->flushUpdateLog(backupProcess->m_session);
			}
			closeTable(backupProcess->m_session, tableInfo);
		}
	}

	// ����ÿ�ű��ֹд�������ٴ�ˢMMS���»���
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
	// �������ڼ����������NTSE��־����֤���ݵ�NTSE������binlogһ�£��쳣ֱ���׳�
	backupTxnlog(backupProcess, false);
	backupProcess->m_tailLsn = m_txnlog->tailLsn();
	m_syslog->log(EL_LOG, "Finish backup, lsn: "I64FORMAT"u", backupProcess->m_tailLsn);
}

/**
 * ���±��ݱ䳤���ļ�
 *	��ģ����ڶ�ͷҳ�м�¼���ļ���ҳ�����ѻָ��������ͷҳ�м�¼��ҳ�������Ƿ���չ�ѡ�
 *  ��������ʵ���ϼٶ��˶�ͷҳ�е�ҳ��Ҫ�����ļ���ʵ��ҳ����
 *  Ϊ����������裬����������ļ������Ƿ�䳤������䳤�򿽱�����Ķ���ҳ�浽�����ļ�
 *  ���QA58235����
 *
 * @pre ���ݿ�ֻ��
 * @param  backupProcess ���ݲ���
 */

void Database::rebackupExtendedFiles(BackupProcess *backupProcess) {
	
	assert(m_ddlLock->isLocked(backupProcess->m_session->getId(), IL_S));

	// ���¼��ÿ�ű���ļ��������ļ����ޱ䳤
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
			if (size > iter->second) //  �ļ��䳤
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
		if (indice) { // ������������
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
 * ��������
 * @pre ����ΪBS_LOCKED��BS_FAILED״̬
 * @post backupProcess�����Ѿ�������
 *
 * @param backupProcess ���ݲ���
 */
void Database::doneBackup(BackupProcess *backupProcess) {
	ftrace(ts.ddl, tout << backupProcess);

	if (backupProcess->m_stat == BS_LOCKED) {
		// �ָ�д����
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
 * �ӱ����ļ��ָ����ݿ�
 *
 * @param backupDir ��������Ŀ¼
 * @param dir �ָ������Ŀ¼
 * @throw NtseException �������ļ�ʧ�ܣ�д����ʧ�ܵ�
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
		// ���������ļ�
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
		// �������ݱ�
		for (u16 i = 0; i < numTabs; ++i)
			restoreTable(ctrlFile, tabIds[i], backupDir, dir);
		// ������־
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

	// �ָ�
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
 * �ָ�һ�ű�
 * @param ctrlFile ���ݿ�����ļ�
 * @param tabId ��id
 * @param backupDir ����Ŀ¼
 * @param dir MySQL����Ŀ¼
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
		if (!exist && i <= 1) //tnt������ϵͳ������frm������ֻ����������зſ�������ֻҪ����Ҫ�ѱ�ͱ���
			NTSE_THROW(NTSE_EC_INVALID_BACKUP, "no heap or tabledef file for table %s", ctrlFile->getTablePath(tabId).c_str());
#else
		if (!exist && i <= 2)// ���ļ���frm�ļ����ܲ�����
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
 * �ָ���־�ļ�
 * @param ctrlFile ���ݿ�����ļ�
 * @param char *basedir	MySQL����Ŀ¼
 * @param backupPath	�����ļ�ȫ·��
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
			// ���������bufSizeһ�����ᳬ��uint ��Χ��������µ�����ǿ��ת���ǰ�ȫ��
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

/** ע���̨�߳�
 * @param task ��̨�߳�
 */
void Database::registerBgTask(BgTask *task) {
	MutexGuard guard(m_bgtLock, __FILE__, __LINE__);
	assert(find(m_bgTasks->begin(), m_bgTasks->end(), task) == m_bgTasks->end());
	m_bgTasks->push_back(task);
}

/** ע����̨�߳�
 * @param task ��̨�߳�
 */
void Database::unregisterBgTask(BgTask *task) {
	MutexGuard guard(m_bgtLock, __FILE__, __LINE__);
	vector<BgTask *>::iterator it = find(m_bgTasks->begin(), m_bgTasks->end(), task);
	assert(it != m_bgTasks->end());
	m_bgTasks->erase(it);
}

/** �ȴ���̨�߳���ɳ�ʼ��
 * @param all Ϊtrue��ʾ�ȴ����к�̨�߳���ɳ�ʼ����false��ʾֻ�ȵ���Щ�ڻָ�������ҲҪ
 *   ���е��߳���ɳ�ʼ��
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
 * �õ��в����ص�������
 * @return �����в���������
 */
NTSECallbackManager* Database::getNTSECallbackManager() {
	return m_cbManager;
}

/**
 * ע���в����ص�����
 * @param type	��������
 * @param cbfn	��Ӧ�Ļص�����
 */
void Database::registerNTSECallback( CallbackType type, NTSECallbackFN *cbfn ) {
	m_cbManager->registerCallback(type, cbfn);
}

/**
 * ע���в����ص�����
 * @param type	��������
 * @param cbfn	��Ӧ�Ļص�����
 * @return true��ʾע���ɹ���false��ʾ�ص�������
 */
bool Database::unregisterNTSECallback( CallbackType type, NTSECallbackFN *cbfn ) {
	flushMmsUpdateLogs();
	return m_cbManager->unregisterCallback(type, cbfn);
}

/** UPDATE/DELETE�Ƿ���Ҫ�����ȼ����ǰ��
 * ��ֵ�����ݿ��֮�󲻿��ܸı�
 *
 * @return UPDATE/DELETE�Ƿ���Ҫ�����ȼ����ǰ��
 */
bool Database::isPkeRequiredForUnD() const {
	return m_pkeRequiredForDnU;
}

/**
 * ˢд���б��mms��־
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
 * �õ��ļ������չ��С
 * �������Ϊ������ǰ���ݿ��ļ���С>=ntse_incr_size * 4������չ��СΪntse_incr_size;
 * �����ǰ�ļ���С����5��ҳ��,��չ1��ҳ��;������չ��СΪ��ǰ���ݿ��С/4
 * @param tableDef	Ҫ��չ�ļ������ı���
 * @param fileSize	Ҫ��չ�ļ���ǰ��С
 * @return �Ƽ�����չ��С
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
// �������� ///////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////////
/** ��ȡ�ļ��� */
string basename(const char *absPath) {
	string path(absPath);
	size_t slashPos = path.rfind(NTSE_PATH_SEP_CHAR);
	return (slashPos == string::npos) ? path : path.substr(slashPos + 1);
}
/** ��ȡĿ¼ */
string dirname(const char *absPath) {
	string path(absPath);
	size_t slashPos = path.rfind(NTSE_PATH_SEP_CHAR);
	return (slashPos == string::npos) ? "." : path.substr(0, slashPos);
}
/**
 * ��ȡ�����ļ���
 * @param backupDir ����Ŀ¼
 * @param filename �ļ���
 * @param tableId �ļ������ı�id
 * @return �����ļ���
 */
string makeBackupPath(const string& backupDir, const string& filename, u16 tableId) {
	// ���ɱ����ļ�·��, Ϊ�˴���ͬ������·��������һ��tableid��׺
	stringstream ss;
	ss << backupDir << NTSE_PATH_SEP << filename << "." << tableId;
	return ss.str();
}

/** Add by TNT */
/**	NTSE Database�򿪣���Ϊ�������裬preOpenΪ����һ
*	@config		���ݿ��������
*	@autoCreate	���ݿⲻ����ʱ���Ƿ��Զ�����
*	@bRecovered	���ݿ�򿪹����У��Ƿ񾭹��ָ�
*	@recover	���ݿ��Ƿ���Ҫ�ָ�
*	@pkeRequiredForDnU
*	@return		���ݿ�򿪳ɹ�������ʵ�������򷵻�Exception
*/
void Database::preOpen(TNTDatabase *tntDb, int recover, bool pkeRequiredForDnU, bool *bRecovered) throw(NtseException)
{
	ftrace(ts.ddl, tout << tntDb << recover << pkeRequiredForDnU);
	Config *config = &tntDb->getTNTConfig()->m_ntseConfig;
	assert(recover >= -1 && recover <= 1);
	assert(config->m_logFileCntHwm > 1);

	// �жϿ����ļ��Ƿ���ڣ��������ļ����ڣ�����Ϊ���ݿ��Ѿ���������ʱ
	// �����ݿ⡣������Ϊ���ݿⲻ���ڣ���ʱ�������ݿ�
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
				if (e.getErrorCode() == NTSE_EC_MISSING_LOGFILE) { // ��־�ļ��Ѿ���ɾ��
					db->getSyslog()->log(EL_WARN, "Missing transaction log files! Try to recreate.");
					// �ٶ����ݿ�״̬һ�£����´�����־�ļ�
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
		//��pool��TNTDatabase Open����new�����ڴ˴�init���������ظ�TNTDatabase����init��
		//��Ҫ��Ϊ�˲���Database preOpen�ٽ��в��
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

/**	NTSE Database�򿪣���Ϊ�������裬preOpenΪ�����
*	@recover	���ݿ��ʱ���Ƿ���Ҫ�ָ�
*	@bRecovered	���ݿ��ʱ���Ƿ������ָ�
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
