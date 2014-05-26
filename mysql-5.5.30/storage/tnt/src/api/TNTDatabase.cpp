/**
* TNT���棬Databaseģ�顣
*
* @author �εǳ�
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
/** TNT��̨�߳� */
class TNTBgTask: public Task {
public:
	/**
	 * ���캯��
	 *
	 * @param db ���ݿ�
	 * @param interval ����ʱ��������λ��
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

/** ���ڽ���purge��defrag��dump�ĺ�̨�߳� */
class PurgeAndDumpTask: public TNTBgTask {
public:
	/**
	 * ���캯��
	 *
	 * @param db ���ݿ�
	 * @param interval ����ʱ��������λ��
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
		//	������ò������Ƿ���Ҫdump
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
	u32			m_runInterval;			/** ��̨�߳�ʱ��������λΪ�� */
	u32			m_lastRunTime;			/** ��һ����ɺ�̨�̵߳�ʱ��, ��λ�� */
	static const int PURGE_AND_DUMP_TASK_INTERVAL =	3; /** purge Task�������һ�� */
	static const int PURGES_PER_TOTAL_PURGE       = 1;  /** PurgeAndDump Task �м���purge��һ��totalPurge*/
	int			m_totalPurgePerDump;	/** PurgeAndDump Task �м���totalPurge��һ��Dump*/
	u64			m_purgeCnt;				/** ���ݿ����������purge�ļ��� */
};


/** ���ڽ��м��ȸ�������ĺ�̨�߳� */
class Master: public TNTBgTask {
public:
	/**
	 * ���캯��
	 *
	 * @param db ���ݿ�
	 * @param interval ����ʱ��������λ��
	 */
	Master(TNTDatabase *db, uint interval)
		: TNTBgTask(db, "TNTDatabase::Master", interval*1000) {
		m_lastSwitchOpTime = System::fastTime();
		m_lastKillHangTrxTime = m_lastSwitchOpTime;
		
		// Master�̵߳��������ڣ���ʹ�ô����internal����
		m_switchOpInterval = 10;
		m_killHangTrxInterval = 10;
	}

	virtual void runIt() {
#ifndef NTSE_UNIT_TEST
		u32 now = System::fastTime();

		// �л��汾����ReclaimLob��m_operIntervalʱ��������һ�μ��
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
	u32         m_switchOpInterval;		/** master�̣߳��л�version pool��ʱ��������λΪ�� */
	u32			m_lastSwitchOpTime;		/** Master�߳���һ�γ����л�VersionPool��ʱ��, ��λ�� */
	u32			m_killHangTrxInterval;		/** master�̣߳�kill hang trx��ʱ��������λΪ�� */
	u32			m_lastKillHangTrxTime;		/** Master�߳���һ�γ���kill hang trx��ʱ��, ��λ�� */
};


class DefragHashTask: public TNTBgTask {
public:
	/**
	 * ���캯��
	 *
	 * @param db ���ݿ�
	 * @param interval ����ʱ��������λ��
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
	u32			m_defragOpInterval;		/** master�̣߳�defrage hash index��ʱ��������λΪ�� */
	u32			m_lastDefragOpTime;		/** Master�߳���һ�γ���Defrag HashIndex��ʱ��, ��λ�� */
	u64			m_defragHashCnt;		/** �����ݿ���������Master�߳���defrag hash index�Ĵ��� */

	static const int DEFRAG_HASH_INDEX_PER_TOTAL = 5; /** ÿ��һ����skill small table����Ҫ��һ��ȫ��defrag hashindex*/
};


/** ���ڹر�����Ϊ0��TNT��ĺ�̨�߳� */
class CloseOpenTableTask: public TNTBgTask {
public:
	/**
	 * ���캯��
	 *
	 * @param db ���ݿ�
	 * @param interval ����ʱ��������λ��
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
	u32			m_runInterval;			/** ��̨�߳�ʱ��������λΪ�� */
	u32			m_lastRunTime;			/** ��һ����ɺ�̨�̵߳�ʱ��, ��λ�� */
};


/** ���ڻ����ڴ�����ҳ��ĺ�̨�߳� */
class ReclaimMemIndexTask: public TNTBgTask {
public:
	/**
	 * ���캯��
	 *
	 * @param db ���ݿ�
	 * @param interval ����ʱ��������λ��
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
	u32			m_runInterval;			/** ��̨�߳�ʱ��������λΪ�� */
	u32			m_lastRunTime;			/** ��һ����ɺ�̨�̵߳�ʱ��, ��λ�� */
};

/**
 * ����һ��TNT Database����
 *
 * @param config TNT���ݿ�����
 *   �ڴ�ʹ��Լ����ֱ�����������ݿ�ر�
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

/** ��ʼ��TNT Database����״̬ */
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
 * ��һ�����ݿ�
 *
 * @param config		TNT�������ò���
 *   �ڴ�ʹ��Լ����		ֱ�����������ݿ�ر�
 * @param autoCreate	���ݿⲻ����ʱ�Ƿ��Զ�����
 * @param recover		�Ƿ�ָ���0��ʾ�������ݿ��Ƿ�ȫ�ر���Ҫ�Զ������Ƿ�ָ���1��ʾǿ�ƻָ���-1��ʾǿ�Ʋ��ָ�
 * @throw NtseException ����ʧ��
 */
TNTDatabase* TNTDatabase::open(TNTConfig *config, u32 flag, int recover) throw(NtseException) {	
	// 1.�жϿ����ļ��Ƿ���ڣ��������ļ����ڣ�����Ϊ���ݿ��Ѿ���������ʱ
	// �����ݿ⡣������Ϊ���ݿⲻ���ڣ���ʱ�������ݿ�
	// TNT��NTSE��Ӧ��basedir��һ���ģ�ͬһĿ¼
	string basedir(config->m_tntBasedir);
	
	string ntsePath = basedir + NTSE_PATH_SEP + Limits::NAME_CTRL_FILE;
	string tntPath = basedir + NTSE_PATH_SEP + Limits::NAME_TNT_CTRL_FILE;
	string newNtsePath = ntsePath + Limits::NAME_CTRL_SWAP_FILE_EXT;
	string newTntPath = tntPath + Limits::NAME_CTRL_SWAP_FILE_EXT;
	
	bool tntCreate = false;

	// ��������ļ������ڵ��Ǵ��ڿ����ļ��Ľ����ļ����򽫽����ļ�������Ϊ�����ļ�
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

	// Ntse control file��tnt control file��������
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
		} else // ֻ��tnt control file������
			NTSE_THROW(NTSE_EC_FILE_NOT_EXIST, "Tnt ctrl file %s not exists.", tntPath.c_str());
	} else if (!File::isExist(ntsePath.c_str()))
		// ֻ��ntse control file������
		NTSE_THROW(NTSE_EC_FILE_NOT_EXIST, "Ntse ctrl file %s not exists.", ntsePath.c_str());

	// 2. Tnt controlfile��ntse controlfile�����ڣ�ѡ������ݿ�
	TNTDatabase *db = new TNTDatabase(config);
	db->m_dbStat	= DB_STARTING;
	// ����tnt�����ϵͳ��־�ļ�
	tntPath	 = basedir + NTSE_PATH_SEP + Limits::NAME_TNT_SYSLOG;
	db->m_tntSyslog = new Syslog(tntPath.c_str(), config->m_tntLogLevel, false, config->m_ntseConfig.m_printToStdout);
	tntPath = basedir + NTSE_PATH_SEP + Limits::NAME_TNT_CTRL_FILE;
	db->m_tntCtrlFile = TNTControlFile::open(tntPath.c_str(), db->m_tntSyslog);

	/*db->m_tntTxnLog = NULL;
	do {
		try {
			// ��TNT�����redo��־�ļ�
			db->m_tntTxnLog = Txnlog::open(db, config->m_tntBasedir, Limits::NAME_TNT_TXNLOG, db->m_tntCtrlFile->getNumTxnlogs(), config->m_tntLogfileSize, config->m_tntLogBufSize);
		} catch(NtseException &e) {
			if (e.getErrorCode() == NTSE_EC_MISSING_LOGFILE) { // ��־�ļ��Ѿ���ɾ��
				db->m_tntSyslog->log(EL_WARN, "Missing TNT transaction log files! Try to recreate");
				// �ٶ����ݿ�״̬һ�£����´�����־�ļ�
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

	//��ʼ��commonPool
	db->m_commonPool = new PagePool(config->m_ntseConfig.m_maxSessions + 1, Limits::PAGE_SIZE);
	// ע��ͨ���ڴ�ع���ģ��
	db->m_commonMemPool = new CommonMemPool(config->m_ntseConfig.m_commonPoolSize, db->m_commonPool);
	db->m_commonPool->registerUser(db->m_commonMemPool);
	db->m_commonPool->init();

	// 3. ��ʼ��TNTDatabase��pagePool����pagePool��init��Database��Database::preOpen��ʼ��
	db->m_pagePool = new PagePool(config->m_ntseConfig.m_maxSessions + 1, Limits::PAGE_SIZE);
	// ע��TNTIM�ڴ����ģ��
	db->m_TNTIMpageManager = new TNTIMPageManager(config->m_tntBufSize, config->m_ntseConfig.m_pageBufSize, db->m_pagePool);
	db->m_pagePool->registerUser(db->m_TNTIMpageManager);

	//4 ����Ntse Database�ӿڣ���ntse���ݿ�
	bool bRecovered = false;
	try {
		// ��ʼ�׶Σ����ָ����ݿ⣬�ָ�����ͳһ��TNTDatabase->recover��������
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

	// ��ʼ��Ψһ������
	db->m_uniqueKeyLockMgr = new UKLockManager(db->getNtseDb()->getConfig()->m_maxSessions);

	try {
		//db->m_pagePool->init();

		//FIXME: ���Ļ�Ծ��������Ҫ���ò���
		db->m_tranSys = new TNTTrxSys(db, config->m_maxTrxCnt, config->m_trxFlushMode, config->m_txnLockTimeout);
		db->m_tranSys->setActiveVersionPoolId(db->m_tntCtrlFile->getActvieVerPool());
		db->m_tranSys->setMaxTrxIdIfGreater(db->m_tntCtrlFile->getMaxTrxId());

		db->openVersionPool(tntCreate);
		db->m_dbStat = DB_MOUNTED;
		db->waitBgTasks(false);
		
		// 5. ����crash recover�������Ƿ���Ҫ���������recover����
		db->recover(recover);

		db->m_tranSys->markHangPrepareTrxAfterRecover();
	} catch (NtseException &e) {
		db->m_tntSyslog->log(EL_ERROR, e.getMessage());
		db->close(true, false);
		delete db;
		throw e;
	}
	
	// 6. crash recover��ɣ��������������open�������
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

	// ��֤TNT�ڴ棬NTSE��֤�����
	db->verify(recover);

	return db;
}

void TNTDatabase::openVersionPool(bool create) throw (NtseException) {
	// ��ʼ��version pool
	// ��Ϊ�½����ݿ⣬�򴴽��汾�أ�����ֱ�Ӵ򿪰汾�ؼ���
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
 * ����TNT���ݿ�
 *
 * @param config ���ݿ�����
 * @throw NtseException �ļ��Ѿ����ڣ���Ȩ�޵ȵ��²���ʧ��
 */
void TNTDatabase::create(TNTConfig *config) throw(NtseException) {
	string basedir(config->m_tntBasedir);

	// 1. ����TNTϵͳ��־�ļ�
	string tntPath = basedir + NTSE_PATH_SEP + Limits::NAME_TNT_SYSLOG;
	Syslog syslog(tntPath.c_str(), EL_LOG, true, true);

	// 2. ����NTSE���ݿ�
	try {
		Database::create(&config->m_ntseConfig);
	} catch (NtseException &e) {
		syslog.log(EL_ERROR, e.getMessage());
		throw e;
	}

	// 3. ����TNT��������ļ�
	tntPath = basedir + NTSE_PATH_SEP + Limits::NAME_TNT_CTRL_FILE;
	TNTControlFile::create(tntPath.c_str(), config, &syslog);

	// 4. ����TNT����redo��־�ļ�
	//Txnlog::create(config->m_tntBasedir, Limits::NAME_TNT_TXNLOG, config->m_tntLogfileSize);
}

/**
 * ����ntse���ݿ�Ϊtnt���ݿ�
 *
 * @param config ���ݿ�����
 * @throw NtseException �ļ��Ѿ����ڣ���Ȩ�޵ȵ��²���ʧ��
 */
void TNTDatabase::upgradeNtse2Tnt(TNTConfig *config) throw(NtseException) {
	Config *ntseConfig = &config->m_ntseConfig;
	string basedir(config->m_tntBasedir);
	
	Syslog syslog(NULL, EL_LOG, true, true);
	//���ntse_ctrlδ�����ɹ�����ôControlFile::open��throw exception
	string ntsePath(config->m_ntseConfig.m_basedir);
	ntsePath.append(NTSE_PATH_SEP).append(Limits::NAME_CTRL_FILE);
	ControlFile *ctrlFile = ControlFile::open(ntsePath.c_str(), &syslog);
	
	// 1. ����TNTϵͳ��־�ļ�
	string tntPath = basedir + NTSE_PATH_SEP + Limits::NAME_TNT_SYSLOG;
	Syslog tntSyslog(tntPath.c_str(), EL_LOG, true, true);

	// 2. ����TNT��������ļ�
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
 * ɾ��TNT���ݿ�
 *
 * @param dir ���ݿ��ļ�����Ŀ¼
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
 * ֹͣ�û���̨�߳�
 *
 */
void TNTDatabase::stopBgCustomThd() {
	if (m_db && m_db->getBgCustomThreadManager()) {
		m_db->getBgCustomThreadManager()->stopAll();
		m_db->getBgCustomThreadManager()->join();
	}
}

/**
 * �ر�TNT���ݿ�
 *
 * @param flushLog �Ƿ�ˢ����־
 * @param flushData �Ƿ�ˢ������
 */
void TNTDatabase::close(bool flushLog, bool flushData)
{
	if (m_closed) {
		return;
	}
	
	DbStat prevStat = m_dbStat;
	m_dbStat = DB_CLOSING;
	
	// ֹͣ�û���̨�߳�
	stopBgCustomThd();

	// ֹͣpurge��defrag��dump�߳�
	if (m_purgeAndDumpTask != NULL) {
		m_purgeAndDumpTask->stop();
		m_purgeAndDumpTask->join();
		delete m_purgeAndDumpTask;
	}

	// ֹͣdefrag hash �߳�
	if (m_defragHashTask != NULL) {
		m_defragHashTask->stop();
		m_defragHashTask->join();
		delete m_defragHashTask;
	}

	// ֹͣmaster
	if (m_masterTask != NULL) {
		m_masterTask->stop();
		m_masterTask->join();
		delete m_masterTask;
	}

	// ֹͣcloseOpenTables �߳�
	if (m_closeOpenTablesTask != NULL) {
		m_closeOpenTablesTask->stop();
		m_closeOpenTablesTask->join();
		delete m_closeOpenTablesTask;
	}

	// ֹͣreclaim mindex pages �߳�
	if (m_reclaimMemIndexTask != NULL) {
		m_reclaimMemIndexTask->stop();
		m_reclaimMemIndexTask->join();
		delete m_reclaimMemIndexTask;
	}

	if (prevStat == DB_RUNNING) {
		// ����Ҫ��purge�ڴ�heap
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
		// �رմ򿪵��ڴ��
		closeOpenTables();
		closeTNTTableBases();
	}

	// �ر�version pool
	if (m_vPool) {
		closeVersionPool();
	}

	// �ر�Ψһ������
	if (m_uniqueKeyLockMgr)
		delete m_uniqueKeyLockMgr;

	delete m_idToTables;
	delete m_pathToTables;
	delete m_idToTabBase;
	delete m_taskLock;
	delete m_tranSys;
	delete m_vPool;

	// �ر�commonPool
	if (m_commonPool) {
		m_commonPool->preDelete();
	}

	// �ر�page pool
	if (m_pagePool) {
		m_pagePool->preDelete();
	}

	// ���ر�NTSE Database
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

	// �ر���־ϵͳ
	/*if (m_tntTxnLog) 
		m_tntTxnLog->close(flushLog);*/

	// �رտ����ļ�ϵͳ
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
* ��TNT���ݿ����crash recovery
*
* @param recover	�Ƿ���Ҫ�������лָ�
*/
void TNTDatabase::recover(int recover) throw(NtseException)
{
	LsnType		dumppointLsn;
	bool		beNeedRecover;
	
	assert(m_dbStat == DB_MOUNTED);
	// 1. �������ݿ⵱ǰ״̬����ʼ��dump�ṹ
	m_dbStat = DB_RECOVERING;
	//initDumpRecover();

	// 2. �ڴ�ָ����֮ǰ��������ȫ��purge����棬�˴�����ҪreadDump
/*	try {
		readDump();
	} catch (NtseException &e) {
		m_tntSyslog->log(EL_ERROR, e.getMessage());
		//releaseDumpRecover();
		throw e;
	}
*/

	// 3. �ж��Ƿ���Ҫ��recover
	if (recover == 1)
		beNeedRecover = true;
	else if (recover == -1)
		beNeedRecover = false;
	else {
		beNeedRecover = !m_tntCtrlFile->getCleanClosed();
	}

	if (!beNeedRecover) {
		// 3.1. �ϴ������رգ�����Ҫ���лָ�
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

	// 4. TNT���ݿ���Ҫ����crash recover
	dumppointLsn = m_tntCtrlFile->getDumpLsn();
	m_tntSyslog->log(EL_LOG, "Start database recover from LSN: "I64FORMAT"d.", dumppointLsn);
	
	// 4.1. NTSE����recover������ͨ�������̵߳���ʽ����ִ��

	// 4.2. TNT����redo�׶�recover
	m_activeTrxProcs = new Hash<TrxId, RecoverProcess *>(m_db->getConfig()->m_maxSessions * 2);
	m_purgeInsertLobs = new TblLobHashMap();
	m_rollbackTrx = new Hash<TrxId, RecoverProcess *>(m_db->getConfig()->m_maxSessions * 2);
	LogScanHandle *logScan = m_tntTxnLog->beginScan(dumppointLsn, m_tntTxnLog->tailLsn());
	int		oldPercent	= 0;
	int		oldTime		= System::fastTime();
	u64		lsnRange	= m_tntTxnLog->tailLsn() - dumppointLsn + 1;
	//��¼���һ������δ���purge������id
	TrxId   purgeTrxId = INVALID_TRX_ID;
	//��¼���һ��δ���purge��purgephase1 ��lsn
	LsnType purgePhaseOneLsn = INVALID_LSN;
	
	//������ӡ�ָ���־��syslog(��Ҫ��ӡ�ָ���־ʱ)
	//Syslog *redoSyslog = new Syslog("dump_tnt_redo.log", EL_DEBUG, true, false);
	while (m_tntTxnLog->getNext(logScan)) {
		const LogEntry	*logEntry	= logScan->logEntry();
		//���log����tnt��־������recover�п���ֱ������
		if (!logEntry->isTNTLog()) {
			continue;
		}
		LsnType     lsn			= logScan->curLsn();
		TrxId		trxId		= parseTrxId(logEntry);
		LogType		logType		= logEntry->m_logType;
		//u16		tableId		= logEntry->m_tableId;
		((LogEntry *)logEntry)->m_lsn = lsn;

		// ���ÿ10���ӡһ��TNT�ָ�������Ϣ
		int percent = (int)((lsn - dumppointLsn) * 100 / lsnRange);
		if (percent > oldPercent && (System::fastTime() - oldTime) > 10) {
			m_tntSyslog->log(EL_LOG, "Done replay of %d%% logs.", percent);
			oldPercent = percent;
			oldTime = System::fastTime();
		}
		
		// �жϵ�ǰ���Ƿ���Բ��ûָ�
		if (!judgeLogEntryValid(lsn, logEntry))
			continue;
		
		//FOR DEBUG ������tnt�ָ������е���־��ӡ�����ݿɼ�baseDir��dump_tnt_redo.log�ļ�(��Ҫ��ӡ�ָ���־ʱ)
		//parseTntRedoLogForDebug(redoSyslog, logEntry, lsn);

		try {
			// ����ͬ���͵�logType
			// 1.1 ���������־
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
				continue;//�����ύ������������lastLsn
			} else if (logType == TNT_PREPARE_TRANS) {
				recoverPrepareTrx(trxId, logEntry);
			} else if (logType == TNT_BEGIN_ROLLBACK_TRANS) {
				recoverBeginRollBackTrx(trxId, logEntry);
			} else if (logType == TNT_END_ROLLBACK_TRANS)	{
				recoverRollBackTrx(trxId);
				continue;//����ع�������������lastLsn
			} else if (logType == TNT_PARTIAL_BEGIN_ROLLBACK) {
				recoverPartialBeginRollBack(trxId, logEntry);
			} else if (logType == TNT_PARTIAL_END_ROLLBACK) {
				recoverPartialEndRollBack(trxId, logEntry);
			} else if (logType == TNT_BEGIN_PURGE_LOG) {
				//��Ϊrecover�Ǵ���һ�ε�dump��ʼ��ָ�����������purge���Ȼ�ᴥ��dump������crash recover���ֻ������һ��purge
				assert(purgeTrxId == INVALID_TRX_ID);
				purgeTrxId = trxId;
				addRedoLogToTrans(trxId, logEntry);
			} else if( logType == TNT_END_PURGE_LOG || logType == TNT_PURGE_BEGIN_FIR_PHASE 
				|| logType == TNT_PURGE_BEGIN_SEC_PHASE || logType == TNT_PURGE_END_HEAP) {
				//purge redo����Ҫ����TNT_BEGIN_PURGE_LOG��TNT_END_PURGE_LOG
				if (purgeTrxId == INVALID_TRX_ID) {
					continue;
				}
				// 1.3 purge�����־
				//��ȡ���һ��purge�ĵ�һ�׶ο�ʼLSN
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
				// 1.4 �û�dml��־
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

	//�����翪ʼrollback��δ���������lsn��δ���purge�Ľ����LSN��ʼ��������ɨ��һ��
  	LsnType beginRollbackLsn = getFirstUnfinishedRollbackLsn();
	LsnType beginScanLsn = min(beginRollbackLsn, purgePhaseOneLsn);
	if (beginScanLsn != INVALID_LSN) {
		LogScanHandle *logScanAgain = m_tntTxnLog->beginScan(beginScanLsn, m_tntTxnLog->tailLsn());
		while (m_tntTxnLog->getNext(logScanAgain)) {
			const LogEntry	*logEntry	= logScanAgain->logEntry();
			//���log����tnt��־������recover�п���ֱ������
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
		// �����汾�ػ���
		redoReclaimLob();
		// 4.3 TNT����undo�׶�recover
		// TODO 4.3.1. TNT undo����ȴ�NTSE recover��ɣ�
		//��Ŀǰ����tnt��recover�ǵ�ntse��recover��ɺ�������������ʱ����ɴ˺������Ժ�����ǲ���recover����Ҫ��ɴ˺���
		// waitNTSECrashRecoverFinish();
		recoverUndoTrxs(TRX_ACTIVE, purgeTrxId);
	} catch (NtseException &e) {
		m_tntSyslog->log(EL_ERROR, e.getMessage());
		//releaseDumpRecover();
		throw e;
	}
		
	try {
		// 4.4  purge��redo
		if (purgeTrxId != INVALID_TRX_ID) {
			redoPurgeTntim(purgeTrxId, false);
		}

		// 4.5. Ϊ�ڴ�����Heap����index
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

	delete m_purgeInsertLobs;			//�ָ�����ɾ��purge��LobId Hash
	m_purgeInsertLobs = NULL;

	assert(m_rollbackTrx->getSize() == 0);	//�ָ�������û�����ڻع�������ɾ���ع������
	delete m_rollbackTrx;
	m_rollbackTrx = NULL;

	// �ָ����������ȹر��ڴ�TNTTable������TNTTableBase
	closeOpenTables(true);
	// Ȼ��ر�NTSE Table��������Ҫflush data
	//m_db->closeOpenTables(true);

	if (m_tntConfig->m_purgeAfterRecover) {
		purgeAndDumpTntim(PT_PURGEPHASE2, false, true, false);
	}
	
	// �رմ�ӡ�ָ���־����ʱlog(��Ҫ��ӡ�ָ���־ʱ)
	//delete redoSyslog;
}

/** ��֤���ݿ���ȷ��
 * @pre �ָ��Ѿ����
 */
void TNTDatabase::verify(int recover) {
	UNREFERENCED_PARAMETER(recover);
}

/** ��ʼ���л��汾��
 * @param proc �л��汾��process�ṹ
 */
void TNTDatabase::initSwitchVerPool(SwitchVerPoolProcess *proc) {
	Connection *conn = m_db->getConnection(true, "switchActiveVerPoolIfNeeded");
	Session *session = m_db->getSessionManager()->allocSession("switchActiveVerPoolIfNeeded", conn);
	proc->m_conn = conn;
	proc->m_session = session;
}

/** �ͷ��л��汾��process�ṹ�е���Դ
 * @param proc �л��汾��process�ṹ
 */
void TNTDatabase::delInitSwitchVerPool(SwitchVerPoolProcess *proc) {
	Connection *conn = proc->m_conn;
	Session *session = proc->m_session;
	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(conn);
}

/**
* �����ǰversion pool��size����Ԥ�������ޣ����л����˺����ɺ�̨�̵߳��ã��л������ǵ��̵߳�
* @pre1	 ע�⣬�л��汾��ʱ����Ҫ��������mutex���ֱ���trx_sys::mutex, TNTControlFile::mutex
*		 ���е�˳���ǣ��ȳ���trx_sys::mutex���ٳ���TNTControlFile::mutex
*
* @pre2	 �л�Version Poolʱ����Ҫ��������֤
*		 1. ���е��������޷�����
*		 2. ���е��ѷ������񣬱���Ҳ�ѷ��䵽��Ӧ��version pool
*
*
* @param tntVerPoolSize TNT����ĵ���version pool�����size
* @param curVerPoolSize	��ǰ�����version pool size
*
*/
void TNTDatabase::switchActiveVerPoolIfNeeded() {
	SwitchVerPoolProcess switchVerPoolProcess;

	// ��ʼ���л��汾�ؽṹ
	initSwitchVerPool(&switchVerPoolProcess);
	// ��ȡTrxSys��mutex������ȡϵͳ������TrxId
	m_tranSys->lockTrxSysMutex();
	TrxId maxTrxId = m_tranSys->getMaxTrxId();
	
	// Ȼ���ȡTNTControlFile�Լ���mutex����ȡActiveVerPoolNum
	m_tntCtrlFile->lockCFMutex();
	u8 activeVerPool	= m_tntCtrlFile->getActiveVerPoolForSwitch();
	u8 verPoolCnt		= m_tntCtrlFile->getVersionPoolCnt();
	Session *session = switchVerPoolProcess.m_session;
	// ���active version pool�Ƿ񳬹�Ԥ��������
	if (m_vPool->getDataLen(session, activeVerPool) < m_tntConfig->m_verpoolFileSize) {
		m_tntCtrlFile->unlockCFMutex();
		m_tranSys->unlockTrxSysMutex();
		delInitSwitchVerPool(&switchVerPoolProcess);
		return;
	}

	// ��¼�汾���л���ʱ��
	m_tntStatus.m_switchVerPoolLastTime = System::fastTime();

	u8 newActiveId = (activeVerPool == (verPoolCnt - 1)) ? 0 : (activeVerPool + 1);
	
	// �����������л�����
	if (!m_tntCtrlFile->switchActiveVerPool(newActiveId, maxTrxId)) {
		m_tntSyslog->log(EL_LOG, "version pool %d is used, switch version pool fail", newActiveId);
	} else {
		m_vPool->setVersionPoolHasLobBit(newActiveId, false);
		m_tranSys->setActiveVersionPoolId(newActiveId, false);
		m_tntStatus.m_switchVersionPoolCnt++;
	}
	
	// �ͷ���Դ
	m_tntCtrlFile->unlockCFMutex();
	m_tranSys->unlockTrxSysMutex();
	delInitSwitchVerPool(&switchVerPoolProcess);
}

/**	Dump������ʼ����ȡdump serial number
*	@dumpProcess	dump�������ݽṹ
*/
void TNTDatabase::beginDumpTntim(PurgeAndDumpProcess *purgeAndDumpProcess) {
	UNREFERENCED_PARAMETER(purgeAndDumpProcess);
	// ��ȡTNT ControlFile��dumpSN��Ȼ��ֵ��dump
//	purgeAndDumpProcess->m_dumpSN = m_tntCtrlFile->getDumpSN() + 1;
}

/**	Dump������������dump��Ϣд��TNT ControlFile������dump�Ǻ�̨���̴߳������Բ���ControlFile Mutex
*	@dumpProcess	dump�������ݽṹ
*/
void TNTDatabase::finishDumpTntim(PurgeAndDumpProcess *purgeAndDumpProcess) {
	// ���purge������������dumpʧЧ
	if (! purgeAndDumpProcess->m_dumpSuccess)
		return;
	
	m_tntStatus.m_dumpCnt++;

	// ��ȡϵͳ��ʱ������������Ϊ�ָ��õ���ʼmax_trxId
	TrxId maxTrxId = m_tranSys->getMaxDumpTrxId();

	m_tntCtrlFile->lockCFMutex();

	// ����TNTControlFile�е�Dump�������
	//m_tntCtrlFile->setDumpBegin(purgeAndDumpProcess->m_begin);
	//m_tntCtrlFile->setDumpEnd(purgeAndDumpProcess->m_end);

	m_tntCtrlFile->setDumpLsn(purgeAndDumpProcess->m_dumpLsn);
	
	m_tntCtrlFile->setMaxTrxId(maxTrxId);

	m_tntCtrlFile->updateFile();

	m_tntCtrlFile->unlockCFMutex();

}

/** �ж�log��־�Ƿ���tableId
 * @param log ��־
 * return �������־�����Ч��tableId����true�����򷵻�false
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

/** �ж�tableId�Ƿ���Ҫ���ָ�
* @param tableId ��id
* return �������Ч��־������true�����򷵻�false
*/
bool TNTDatabase::judgeLogEntryValid(u64 lsn, const LogEntry *log) {
	if (!hasTableId(log)) {
		return true;
	}
	u16 tableId = log->m_tableId;

	//�ж��Ƿ���С�ʹ���󣨱䳤�����ݣ���ID��Ȼ����normal��������TNT�еĴ����Ҳ��Ҫ�ָ�
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
 * @param log ������log
 * @param ctx �����ڴ�������
 * return ������log
 */
LogEntry *TNTDatabase::copyLog(const LogEntry *log, MemoryContext *ctx) {
	LogEntry *ret = (LogEntry *)ctx->alloc(sizeof(LogEntry));
	memcpy(ret, log, sizeof(LogEntry));
	ret->m_data = (byte *)ctx->alloc(ret->m_size);
	memcpy(ret->m_data, log->m_data, log->m_size);
	return ret;
}

/** recover�׶Σ�����һ������
* @trxId		����ID
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

/** recover�׶Σ��ύһ������
* @trxId		����ID
*/
void TNTDatabase::recoverCommitTrx(TrxId trxId) {
	RecoverProcess *proc = m_activeTrxProcs->get(trxId);
	//����TNT_END_RECLAIM_LOBPTR��־�У��Ѿ���������commit
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

/** recover�׶Σ�prepareһ������
* @trxId		����ID
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

/** recover�׶Σ���ʼ�ع�һ������
* @trxId		����ID
* @lsn			begin rollback��־
*/
void TNTDatabase::recoverBeginRollBackTrx(TrxId trxId, const LogEntry *log) {
	RecoverProcess *proc = m_activeTrxProcs->get(trxId);
	if (!proc) {
		return;
	}
	m_rollbackTrx->put(trxId, proc); //���������ع������
	TNTTransaction *trx = m_activeTrxProcs->get(trxId)->m_trx;
	trx->initRollbackInsertLobHash();//��ʼ������ع�LobId��ϣ
	trx->setTrxBeginRollbackLsn(log->m_lsn);
	trx->setTrxState(TRX_ACTIVE);
}

/** recover�׶Σ��ع�һ������
* @trxId		����ID
*/
void TNTDatabase::recoverRollBackTrx(TrxId trxId) {
	RecoverProcess *proc = m_activeTrxProcs->get(trxId);
	if (!proc) {
		return;
	}
	TNTTransaction *trx = proc->m_trx;
	if(trx->getRollbackInsertLobHash() != NULL)
		trx->releaseRollbackInsertLobHash();	//��������ع�ʱ��¼�����������Ϣ��hash
	m_activeTrxProcs->remove(trx->getTrxId());
	proc->m_trx->rollbackForRecover(proc->m_session, NULL);
	//assert(m_rollbackTrx->get(trxId) != NULL);
	m_rollbackTrx->remove(trxId);//�ӻع��������ɾ��������
	releaseRecoverProcess(proc);
	m_tranSys->freeTrx(trx);
}

/** recover�׶Σ�partial rollback begin����
 * @param trxId ����id
 * @param log   partial begin rollback��־
 */
void TNTDatabase::recoverPartialBeginRollBack(TrxId trxId, const LogEntry *log) {
	RecoverProcess *proc = m_activeTrxProcs->get(trxId);
	if (!proc) {
		return;
	}

	m_rollbackTrx->put(trxId, proc); //���������ع������
	m_activeTrxProcs->get(trxId)->m_trx->initRollbackInsertLobHash();//��ʼ������ع�LobId��ϣ
	m_activeTrxProcs->get(trxId)->m_trx->setTrxBeginRollbackLsn(log->m_lsn);
}

/** recover�׶Σ�partial rollback��end����
 * @param trxId ����id
 */
void TNTDatabase::recoverPartialEndRollBack(TrxId trxId, const LogEntry *log) {
	RecoverProcess *proc = m_activeTrxProcs->get(trxId);
	if (!proc) {
		return;
	}
	TNTTransaction *trx = proc->m_trx;
	trx->releaseRollbackInsertLobHash();	//��������ع�ʱ��¼�����������Ϣ��hash
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

/** crash recover�����й��������commit��ʣ�ಽ��
 * @param trxId ����id
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

/** redo��ǰ���������״̬Ϊtarget����������
 * @param target redo�����Ŀ��״̬
 * @param free  ��ʶ�Ƿ���Ҫ��trx����
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
		m_rollbackTrx->remove(trx->getTrxId()); //�ӻع��������ɾ��

		/*��prepare������Ҫ������������ó�NULL�����connection���ڽ�����free��
		  �Ȼָ�����ϲ�ع���Ϊ�����������µ�connection*/
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

/** �����������־��redo����
 * @param proc ����ָ����̵����ݽṹ���ڰ���������������Ҫredo��log
 */
void TNTDatabase::recoverRedoTrx(RecoverProcess *proc, RedoType redoType) {
	Session *session = proc->m_session;
	DLink<LogEntry *> *log = NULL;
	while ((log = proc->m_logs->removeFirst()) != NULL) {
		redoLogEntry(session, log->get(), redoType);
	}
}

/** crash recover������undoָ������״̬����������
 * @param target ��Ҫundo���������״̬
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
		m_rollbackTrx->remove(trx->getTrxId()); //�ӻع��������ɾ��
		trx->rollbackForRecover(proc->m_session, proc->m_logs);
		releaseRecoverProcess(proc);
		m_tranSys->freeTrx(trx);
	}
	delete[] procs;
	procs = NULL;
}

/** recover�׶Σ�������ͬһ�����redo��־��������
* @trxId		����ID
* @logEntry		redo��־
*/
void TNTDatabase::addRedoLogToTrans(TrxId trxId, const LogEntry* log) {
	RecoverProcess *proc = m_activeTrxProcs->get(trxId);
	//�������startTransactionλ��dumpPointLsn֮�䣬����dump��ʼʱ��������϶����ύ�����Ը������Ӱ��϶���dump�������
	if (!proc) {
		return;
	}
	MemoryContext *ctx = proc->m_session->getMemoryContext();
	LogEntry *logEntry = copyLog(log, ctx);
	DLink<LogEntry *> *logLink = new (ctx->alloc(sizeof(DLink<LogEntry *>))) DLink<LogEntry *>(logEntry);
	proc->m_logs->addLast(logLink);
}

/** ���������lastlsn
 * @param trxId ��Ҫ���µ�����id
 * @param lsn   ����lastLsn��ֵ
 */
void TNTDatabase::updateTrxLastLsn(TrxId trxId, LsnType lsn) {
	RecoverProcess *proc = m_activeTrxProcs->get(trxId);
	if (!proc) {
		return;
	}
	proc->m_trx->setTrxLastLsn(lsn);
}

/** ����tableId��ȡtnt��Ŀǰֻ����recover����
 * @param session �Ự
 * @param tableId ��id
 */
TNTTable* TNTDatabase::getTNTTable(Session *session, u16 tableId) throw(NtseException) {
	NTSE_ASSERT(DB_RECOVERING == m_dbStat);
	string pathStr = m_ntseCtrlFile->getTablePath(tableId);
	if (pathStr.empty()) {
		return NULL;
	}

	char *path = (char *)pathStr.c_str();
	// ���ȣ���NTSE Table
	Table *ntseTable = NULL;
	try {
		ntseTable = m_db->getTable(tableId);
	} catch (NtseException &e) {
		throw e;
	}
	
	// NTSE open�ɹ����ڲ�������
	// open TNT table
	TNTTableInfo* tableInfo = m_pathToTables->get(path);
	if (!tableInfo) {
		TNTTableBase *tableBase = NULL;
		TNTTable *table = NULL;
		tableBase = m_idToTabBase->get(tableId);
		if (tableBase == NULL) {
			tableBase = new TNTTableBase();
			// ��tableBase����hash��
			m_idToTabBase->put(tableId, tableBase);
			// ��tableBase����˫������
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


/** ����logִ��undo����
 * @param session �Ự
 * @param log     ������־��¼
 * @param crash   ����ָ����crash��undo����log��undo
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

	//�ñ��п��ܱ�drop����ֱ��Խ���ñ�
	if (table == NULL) {
		preLsn = parsePreLsn(log);
		return preLsn;
	}
	//�����ʱΪ���������redoҲ�޷�����Ϊpurge�ڶ��׶�д��־���flush���
	assert(table->getNtseTable()->getTableDef()->getTableStatus() != TS_CHANGING
		&& table->getNtseTable()->getTableDef()->getTableStatus() != TS_SYSTEM);
	switch (log->m_logType) {
		case TNT_UNDO_I_LOG:
			//��ͨ�ֶεĻع�
			preLsn = table->undoInsert(session, log, crash);
			goto _Finish;
		case TNT_UNDO_LOB_LOG:
			//�����ع�
			preLsn = table->undoInsertLob(session,log);
			goto _Finish;
		default:
			//�����crash��recover������Ҫ����update��delete��undo
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

/** ����log��endLsn��beginLsn��ͬһ�������undo������log recover
 * @param session  �Ự
 * @param beginLsn ��־����ʼlsn
 * @param endLsn ��־�Ľ���lsn��������endLsn��log����
 */
bool TNTDatabase::undoTrxByLog(Session *session, LsnType beginLsn, LsnType endLsn) {
	assert(endLsn != INVALID_LSN);
	TNTTableHash openTables;
	LogScanHandle *logScan = m_tntTxnLog->beginScan(endLsn, m_tntTxnLog->tailLsn());
	//log�ع�ʱ����Ӧ��table���Ѿ�������
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
				//pathStr.empty()˵���ñ��Ѿ���drop
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

/** ����logs��ͬһ�������undo������crash recover
 * @param session �Ự
 * @param logs  ��־����
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

/** �жϸ�log����commit��־ʱ�Ƿ���Ҫ��redo
 * @param log ������־��¼
 * return ��Ҫ��redo����true�����򷵻�false
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

/** ����logִ��redo����
 * @param session �Ự
 * @param log     ��־��¼
 */
void TNTDatabase::redoLogEntry(Session *session, const LogEntry *log, RedoType redoType) {
	if (!isNeedRedoLog(log)) {
		return;
	}

	TNTTable *table = getTNTTable(session, log->m_tableId);
	//�ñ���������ڣ������judgeLogEntryValid������
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

/** recover�׶Σ������г���log�������������ڻع����������lobId
* @logEntry		TNT_UNDO_LOB_LOG��־
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


/** recover�׶Σ���¼������purgephase1֮������lobId
* @logEntry		TNT_UNDO_LOB_LOG��־
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


/**	�½���
* @session	����session
* @path		����
* @tableDef	����
*/
void TNTDatabase::createTable(Session *session, const char *path, TableDef *tableDef) throw(NtseException) {
	// ֱ�ӵ���NTSE::Database->createTable����
	// DDL Lock�Ļ�ȡ����Ҫ�������˺���֮��
	acquireDDLLock(session, IL_X, __FILE__, __LINE__);

	try {
		m_db->createTable(session, path, tableDef);
	} catch (NtseException &e) {
		releaseDDLLock(session, IL_X);
		throw e;
	}

	// ����TNTTableBase��Ϣ
	// �˴�ֱ��new�ռ䣬������Ҫ�޸�

	// �޸�hash������ṹ��ͨ��DDL Lock����
	//tableBase = new TNTTableBase(path);

	// ��tableBase����hash��
	//m_pathToTabBase->put((char *)tableBase->m_path, tableBase);
	// ��tableBase����˫������
	//m_tntTabBases.addLast(&tableBase->m_tableBaseLink);

	// DDL Lock���ͷ�
	releaseDDLLock(session, IL_X);
}

/**	ɾ����
* @session		ɾ��session
* @path			����
* @timeoutSec	��ʱʱ�䣬��λ�룬Ϊ<0��ʾ����ʱ��0��ʾ���ϳ�ʱ��>0Ϊ��ʱʱ��
* @throw Exception	ָ���ı����ڵȣ���ʱ��
*/
void TNTDatabase::dropTable(Session *session, const char *path, int timeoutSec) throw(NtseException) {
	// ��ȡDDL Lock
	acquireDDLLock(session, IL_X, __FILE__, __LINE__);

	// �����Ƿ����
	// �˴�ͨ��DDL X Lock����֤û�в���������DDL��ֱ�Ӷ�ȡ��Ԫ���ݣ��Ƿ���ԣ�
	u16 tableId = m_ntseCtrlFile->getTableId(path);
	if (tableId == TableDef::INVALID_TABLEID) {
		releaseDDLLock(session, IL_X);
		
		NTSE_THROW(NTSE_EC_FILE_NOT_EXIST, "No such table '%s'", path);
	}

	string pathStr = m_ntseCtrlFile->getTablePath(tableId);
	// ��ȡATS Lock
	acquireATSLock(session, IL_S, -1, __FILE__, __LINE__);
	try {
		// �ȴ�TNT Table������NTSE Table�������ر�
		waitRealClosed(session, path, timeoutSec);
	} catch (NtseException &e) {
		releaseATSLock(session, IL_S);
		releaseDDLLock(session, IL_X);
		throw e;
	}

	// �Ƚ�NTSE Tableɾ����ע�⣺
	// 1. ��ʱDDL Lock��ATS Lock�Ѿ���ȡ
	// 2. ��Database->dropTable������KVʵ�ֳ�ͻ
	try {
		m_db->dropTableSafe(session, path, timeoutSec);
	} catch (NtseException &e) {
		releaseATSLock(session, IL_S);
		acquireATSLock(session, IL_X, -1, __FILE__, __LINE__);
		// ����NTSE�Ƿ�Drop�ɹ�����Ҫɾ��TNT�ڴ��е�TableBase������ͻᵼ���ڴ�й©
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

	// ��tableBase��hash��˫��������ɾ��
	// �����漰�޸�TNTTableBase��hash�������Ҫ�ͷ�ATS S������X��
	// ͬʱ������һֱ����DDL X������ǰ���ᱻopen(open��DDL IS��)
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

	// dropTable�ɹ���DDL Lock��ATS Lock���ͷ�
	releaseATSLock(session, IL_X);
	releaseDDLLock(session, IL_X);
}

/** ��Table�Ѵ򿪣����������ü���
* @session		����Session
* @path			����
* @timeoutMs	����ʱʱ��
* @return		TNTTableʵ��
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
		// ͳ����Ϣ
		m_tntStatus.m_openTableCnt++;
		tableInfo->m_refCnt++;
		ntseTblInfo->m_refCnt++;

		// ���refcnt��0���1���ƶ�LRU�����λ��
		if (ntseTblInfo->m_refCnt == 0) {
			m_db->getOpenTableLRU()->moveToLast(&ntseTblInfo->m_table->getOpenTablesLink());
		}

		releaseATSLock(session, IL_X);
		return tableInfo->m_table;
	}
	releaseATSLock(session, IL_X);
	return NULL;
}


/**	��TNT Table
* @session	�򿪱�session
* @path		����
*/
TNTTable* TNTDatabase::openTable(Session *session, const char *path, bool needDDLLock/*= true*/) throw(NtseException) {	
	// ��ȡDDL Lock����ֹ�򿪱�����б�RENAME��DROP

	// TRUNCATE��open�ǿ��Բ���ִ�е�
	// NTSE��TRUNCATE�еı���open�����У������Database::openTable������lockMeta�����£���ΪTRUNCATE����Meta X��
	// TNT�� TRUNCATE�еı���open�����У������Database::openTable������acquireATSLock�����£���ΪTRUNCATE���޸�TNTTableBase�ṹʱ�����ȡATS X��
	
	if (needDDLLock) {
		acquireDDLLock(session, IL_IS, __FILE__, __LINE__);
	}
	
	// TODO���˴�����timeoutΪ-1��������Ҫ�Ľ�

	try {
		acquireATSLock(session, IL_X, m_tntConfig->m_ntseConfig.m_tlTimeout * 1000, __FILE__, __LINE__);
	} catch (NtseException &e) {
		if (needDDLLock) {
			releaseDDLLock(session, IL_IS);
		}
		throw e;
	}

	// ���ȣ���NTSE Table
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
	string pathStr = m_ntseCtrlFile->getTablePath(tableId);	// ��Сд�봫���path���ܲ�ͬ
	assert(!pathStr.empty());
	
	// NTSE open�ɹ����ڲ�������
	// open TNT table
	TNTTableInfo* tableInfo = m_pathToTables->get((char *)path);
	if (!tableInfo) {
		TNTTableBase	*tableBase;
		TNTTable *table = NULL;

		tableBase = m_idToTabBase->get(tableId);
		if (tableBase == NULL) {
			tableBase = new TNTTableBase();
			// ��tableBase����hash��
			m_idToTabBase->put(tableId, tableBase);
			// ��tableBase����˫������
			m_tntTabBases.addLast(&tableBase->m_tableBaseLink);
		}
		try {
			table = TNTTable::open(this, session, ntseTable, tableBase);
		} catch (NtseException &e) {
			// ���tableBase��δ��״̬������Ҫ�Ӹ�������ժ��
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

	// ͳ����Ϣ
	m_tntStatus.m_openTableCnt++;

	// �ͷ�DDL Lock��ATS Lock
	releaseATSLock(session, IL_X);
	if (needDDLLock) {
		releaseDDLLock(session, IL_IS);
	}

	return tableInfo->m_table;
}

/**	�رձ�
* @session	�رձ�session
* @table	�Ѵ򿪵ı�
*/
void TNTDatabase::closeTable(Session *session, TNTTable *table) {	
	ftrace(ts.ddl, tout << session << table->getNtseTable()->getPath());
	assert(!m_closed && !m_unRecovered);
	
	Table	*ntseTable = NULL;
	u16		tableId;
	// ��ȡATS Lock
	acquireATSLock(session, IL_X, -1, __FILE__, __LINE__);

	// ��ȡTNTTableInfo ע�⣺
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
	// ���refcntΪ0���ƶ�LRU�����λ��
	if (ntseTblInfo->m_refCnt == 0) {
		m_db->getOpenTableLRU()->moveToFirst(&ntseTblInfo->m_table->getOpenTablesLink());
	}
	
	assert(tableInfo->m_refCnt <= ntseTblInfo->m_refCnt);

	// ͳ����Ϣ
	m_tntStatus.m_closeTableCnt++;

	// �ͷ�ATS Lock
	releaseATSLock(session, IL_X);
}

/**	�������Ϊ0�رձ�
* @session	�رձ�session
* @table	�Ѵ򿪵ı�
* @return	�Ƿ�ɹ��رձ�
*/
bool TNTDatabase::realCloseTableIfNeed(Session *session, Table *ntseTable) throw(NtseException) {
	ftrace(ts.ddl, tout << session << ntseTable->getPath());
	assert(!m_closed && !m_unRecovered);
	// ��ʱ��Ȼ�ѳ���ATS������
	assert(getATSLock(session) == IL_X);

	// ���ڴ˴�ֻ��waitRealClose �� closeOpenTableIfNeed����˱�Ȼ֮ǰû�м�meta��
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
		// ��NTSE���refCntΪ0ʱ��tnt���refCntҲһ��Ϊ0
		assert(closeTnt || !tntTableInfo);
		ntseTable->tntLockMeta(session, IL_X, -1, __FILE__, __LINE__);
		realCloseTable(session, tntTableInfo, ntseTblInfo, true, closeTnt, closeNtse);	
		success = true;
	}

	return success;
}

/**	truncate��
* @��������˵����
* 1. truncate��������ha_tnt��delete_all_rows�������ã�truncate��䣬��Ҫopen_table/external_lock 
	 ���ڵ�ǰ���ϵ�����ddl���������waitRealClose�رձ��������ddl�������ܲ�������
* 2. ��ha_tnt::delete_all_rows�����У��Ὣmeta s������Ϊmeta x��
	 ��֤��ǰ���ϵ����е�dml�����޷�����ִ��
* 3. ��̨�̲߳���(Purge/Dump/Defrag)��ֱ�ӵ���TNTDatabase�����openTable�����򿪱�
	 ��DDL IS Lock��ATS X Lock��MetaLock S Lock����˲�����truncate�е�DDL IX��ͻ�����ǻ���
	 MetaLock X Lock��ͻ�����һ�������������⣺
	 3.1 Open Table�ȿ�ʼ����OpenTable�ɹ������������˱���Ҫ��MetaLock����ܷ���
	 3.2 Open Table��ʼ����ȴ���Truncate��MetaLock�ϣ�ֱ��Truncate���غ�OpenTable���ܳɹ�
* 4. truncate���������DDL IX��
	 ��ֹ�����ı��ݲ�����������Ҫ��DDL S��
* 5. ��tnt table����truncateʱ����ATS X��
	 ��Ϊ��Ҫ�޸�TableBase�ṹ�����˽ṹ����ͨ��ATS��������
** @��������˵��

* @session			truncate��session
* @table			�Ѵ򿪵ı�
* @isTruncateOper	
*/
void TNTDatabase::truncateTable(Session *session, TNTTable *table, bool isTruncateOper) throw(NtseException) {
	Table			*ntseTable	= table->getNtseTable();
	//TNTTransaction	*trx		= session->getTrans();
	u16				tableId;
	// ��ȡDDL Lock
	acquireDDLLock(session, IL_IX, __FILE__, __LINE__);

	tableId = ntseTable->getTableDef(true, session)->m_id;

	try {
		// ���������漰���޸�TNTTableBase�ṹ�������ҪATS X������
		acquireATSLock(session, IL_X, m_tntConfig->m_ntseConfig.m_tlTimeout * 1000, __FILE__, __LINE__);
		// truncate NTSE table
		//��ȻTable::truncate��ͨ�������µĿձ���ʵ�֣�������Id��tableName��û�иı�
		bool newHasDict = false;
		ntseTable->truncate(session, false, &newHasDict, isTruncateOper);

		//�޸Ŀ����ļ����Ƿ���ȫ���ֵ����Ϣ
		alterCtrlFileHasCprsDic(session, tableId, newHasDict);
	} catch (NtseException &e) {
		releaseDDLLock(session, IL_IX);
		throw e;
	}

	// Sync Point
	// openTable������Truncate����������ͬ���㣬openTable���Կ���Truncate���м����
	SYNCHERE(SP_DB_TRUNCATE_CURRENT_OPEN);

	// truncate TNT table
	table->truncate(session);
	
	releaseATSLock(session, IL_X);
	releaseDDLLock(session, IL_IX);
}

/**	rename��
* @session			rename��session
* @oldPath			�ɱ���
* @newPath			�±���
* @timeoutSec			
*/
void TNTDatabase::renameTable(Session *session, const char *oldPath, 
				 const char *newPath, int timeoutSec) throw(NtseException) {
	// rename����������Ҫ��TNTTable��hash����ժ������Ϊclose����ɴ˲���
	// ���ǣ�rename��Ҫ�޸�TNTTableBase�ṹ���Լ���Ӧ��hash��
	
	// ��ȡDDL Lock
	acquireDDLLock(session, IL_X, __FILE__, __LINE__);
	// ��ȡATS Lock
	acquireATSLock(session, IL_S, -1, __FILE__, __LINE__);

	// �ȴ�TNT Table������NTSE Table�������ر�
	try {
		waitRealClosed(session,oldPath, timeoutSec);
	} catch (NtseException &e) {
		releaseATSLock(session, IL_S);
		releaseDDLLock(session, IL_X);
		throw e;
	}	

	// ����NTSE�����rename������NTSE ����Limits::EXTS[]���鶨���˳��Rename��������ļ���
	//	NAME_IDX_EXT��			�����ļ�
	//	NAME_HEAP_EXT��			���ļ�
	//	NAME_TBLDEF_EXT��		TableDef�ļ�
	//	NAME_SOBH_EXT��			С�ʹ�����ļ�
	//	NAME_SOBH_TBLDEF_EXT��	
	//	NAME_LOBI_EXT��
	//	NAME_LOBD_EXT��
	//	NAME_GLBL_DIC_EXT��
	try {
		m_db->renameTableSafe(session, oldPath, newPath, timeoutSec);
	} catch (NtseException &e) {
		releaseATSLock(session, IL_S);
		releaseDDLLock(session, IL_X);
		throw e;
	}

	// rename���֮���޸�TNTTableBase�ṹ
	// ����TNTTable�Ѿ���close����˲���Ҫ����

	// �����漰�޸�TNTTableBase��hash�������Ҫ�ͷ�ATS S������X��
	// ͬʱ������һֱ����DDL X������ǰ���ᱻopen(open��DDL IS��)
	releaseATSLock(session, IL_S);
	/*acquireATSLock(session, IL_X, -1, __FILE__, __LINE__);

	TNTTableBase *tableBase = m_pathToTabBase->get((char *)oldPath);
	if (tableBase != NULL) {
		// TNTTableBase�ṹ���ڣ�֤����������open��

		// ���Ƚ�TableBase��Hash����ժ��
		NTSE_ASSERT(m_pathToTabBase->remove((char *)oldPath));
		
		// Ȼ��������TableBase
		tableBase->rename(newPath);
		
		// ���tableBase����hash��
		m_pathToTabBase->put(tableBase->m_path, tableBase);

		// TNTTableBase��������Ҫ����
	}

	// �ͷ�DDL Lock & ATS Lock
	releaseATSLock(session, IL_X);*/
	releaseDDLLock(session, IL_X);
}

/**	������������
* @session			����session
* @table			�Ѵ򿪵ı�
* @numIndice		�������
* @indexDef			index�Ķ���	
*/
void TNTDatabase::addIndex(Session *session, TNTTable *table, u16 numIndice, 
			  const IndexDef **indexDefs) throw(NtseException) {
	// 1. ��ȡDDL Lock
	acquireDDLLock(session, IL_IX, __FILE__, __LINE__);

	try {
		table->addIndex(session, indexDefs, numIndice);
	} catch (NtseException &e) {
		releaseDDLLock(session, IL_IX);
		throw e;
	}

	releaseDDLLock(session, IL_IX);
}

/**	����ɾ������
* @session			����session
* @table			�Ѵ򿪵ı�
* @idx				ɾ���������	
*/
void TNTDatabase::dropIndex(Session *session, TNTTable *table, uint idx) throw(NtseException) {
	// 1. ��ȡDDL Lock
	acquireDDLLock(session, IL_IX, __FILE__, __LINE__);

	try {
		table->dropIndex(session, idx);
	} catch (NtseException &e) {
		releaseDDLLock(session, IL_IX);
		throw e;
	}
	
	releaseDDLLock(session, IL_IX);
}

/**	�޸ı����
* @session			����session
* @table			�Ѵ򿪵ı�
* @name				������
* @valueStr			����ֵ
* @timeout			����ʱ��
* @param inLockTables  �����Ƿ���Lock Tables ���֮��
*/
void TNTDatabase::alterTableArgument(Session *session, TNTTable *table, 
						const char *name, const char *valueStr, int timeout, bool inLockTables) throw(NtseException) {
	// ������ȫ����NTSE Database ���
	// �������еĲ������ƣ���Ҫ��TNT�����
	// 1. ��ȡDDL Lock
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
* @session			����session
* @tblId			��ID
* @hasCprsDic			
*/
void TNTDatabase::alterCtrlFileHasCprsDic(Session *session, u16 tblId, bool hasCprsDic) {
	UNREFERENCED_PARAMETER(session);
	UNREFERENCED_PARAMETER(tblId);
	UNREFERENCED_PARAMETER(hasCprsDic);
	// ��ȫ����NTSE Database���
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
 * �Ż�������ʵ�ʲ���
 *
 * @param session �Ự
 * @param path ��·���������basedir����Сд������
 * @param keepDict �Ƿ�ָ���˱������ֵ��ļ�(����еĻ�)
 * @param cancelFlag ����ȡ����־
 * @throw NtseException �ӱ�����ʱ���ļ������쳣���ڷ����쳣ʱ����������֤ԭ�����ݲ��ᱻ�ƻ�
 */
void TNTDatabase::doOptimize(Session *session, const char *path, bool keepDict, 
						  bool *cancelFlag) throw(NtseException) {
	acquireDDLLock(session, IL_IX, __FILE__, __LINE__);
	TNTTable *table = NULL;
	TNTTransaction *trx = session->getTrans();
	TNTTransaction tempTrx;

	//�����û�д򿪣����ȴ򿪱�
	try {
		if (!trx) {
			//Ϊ�˼��ݷ��������ӳ�������open table
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

		//���¿����ļ�
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
 * ȡ����̨�������е��û��߳�
 * @param thdId ���ݿ�����ID
 * @return 
 */
void TNTDatabase::cancelBgCustomThd(uint connId) throw(NtseException) {
	m_db->cancelBgCustomThd(connId);
}

/**	ʵ�ֲ���online����
* @session			����session
* @table			�Ѵ򿪵ı�
* @keepDict			�Ƿ���ԭ��ѹ���ֵ�			
*/
void TNTDatabase::optimizeTable(Session *session, TNTTable *table, bool keepDict /*=false*/, bool waitForFinish /*=true*/) throw(NtseException) {
	// Optimize��ȫ����NTSEʵ�֣�����Ƚ�Session�е������Ƴ�
	UNREFERENCED_PARAMETER(session);
	BgCustomThreadManager *bgCustomThdsManager = m_db->getBgCustomThreadManager();
	if (waitForFinish) {
		OptimizeThread thd(this, bgCustomThdsManager, table, keepDict, false, session->getConnection()->isTrx());
		thd.start();
		thd.join();
	} else {
		//BgCustomThread�����ɺ�̨�̹߳������������
		BgCustomThread *thd = new OptimizeThread(this, bgCustomThdsManager, table, keepDict, true, session->getConnection()->isTrx());
		bgCustomThdsManager->startBgThd(thd);
	}
}

/**	�ȴ����������رգ���һ����������Ҫ�Ա�ر�֮����ܽ��е�DROP/RENAME�Ȳ�������������
* ��ʹӦ���Ѿ��ر��˱�����Ȼ�ᱻ���ݡ����㡢Dump��Purge��������ʱpinסû�������ر�
* @session			����session
* @path				�رձ��·��
* @timeoutSec		�ȴ���ʱ			
*/
void TNTDatabase::waitRealClosed(Session *session, const char *path, int timeoutSec) throw(NtseException) {
	u32 before = System::fastTime();
	bool first = true;
	bool realClose = false;
	// ��ȴ�2����timeoutSecʱ��
	while (true) {
		TNTTableInfo *tntTableInfo = m_pathToTables->get((char *)path);
		TableInfo	 *ntseTableInfo= m_db->getTableInfo((char *)path);
		
		if (!tntTableInfo && !ntseTableInfo) {
			// TNTTableInfo��NTSE TableInfo�������ڣ����ѱ�Real Close
			break;
		}

		if (first)
			m_tntSyslog->log(EL_WARN, "Table '%s'(tntref = %d && ntseref = %d) is used, wait and retry.", path, 
			!tntTableInfo? 0: tntTableInfo->m_refCnt, !ntseTableInfo? 0: ntseTableInfo->m_refCnt);

		if (timeoutSec == 0 || (timeoutSec > 0 && (u32)timeoutSec < (System::fastTime() - before))) {
			NTSE_THROW(NTSE_EC_LOCK_TIMEOUT, "waitRealClosed timeout");
		}
		
		releaseATSLock(session, IL_S);

		// ���Ի�ȡATS������ȥ�����ر�refCntΪ0�ı�
		acquireATSLock(session, IL_X, -1, __FILE__, __LINE__);
		ntseTableInfo= m_db->getTableInfo((char *)path);
		if (ntseTableInfo) {	
			try {
				realClose = realCloseTableIfNeed(session, ntseTableInfo->m_table);
			} catch (NtseException &e) {
				// �ر��״�һ��������ˢ������ʱ����I/O�쳣
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

/** �����ر�һ�ű��������Database��hash����ɾ��
*	@session		����session
*	@tableInfo		TNTTableInfo�ṹ
*	@ntseTblInfo	NTSE TableInfo�ṹ
*	@flushDirty
*	@closeTnt		�Ƿ�ر�TNTTable
*	@closeNtse		�Ƿ�ر�Ntse Table
*/
void TNTDatabase::realCloseTable(Session *session, TNTTableInfo *tableInfo, TableInfo *ntseTblInfo, bool flushDirty, bool closeTnt, bool closeNtse) {
	u16		tableId;

	tableId = ntseTblInfo->m_table->getTableDef()->m_id;

	// ���ȹر�TNT Table
	if (closeTnt) {
		m_pathToTables->remove(tableInfo->m_path);
		m_idToTables->remove(tableId);

		// һ�ڣ�ʵ��close��ʱ�������TNT�ڴ�
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

	// Ȼ��ر�NTSE Table
	if (closeNtse) {
		// NTSE realCloseTable��ͬʱ�� Table Meta Lock remove
		m_db->realCloseTable(session, ntseTblInfo, flushDirty);
	}
}


/** �رղ������ü���Ϊ0�ı�
*	@session		����session
*/
void TNTDatabase::closeOpenTablesIfNeed() {
	Connection *conn = m_db->getConnection(false, "tnt:closeOpenTables");
	Session *session = m_db->getSessionManager()->allocSession("tnt::closeOpenTables", conn);
	DLink<Table *> *curr = NULL;
	bool listEnd = false;
	while(true) {
	// ��ȡATS Lock
		acquireATSLock(session, IL_X, -1, __FILE__, __LINE__);
		int numTablesNeedClose = m_db->getOpenTableLRU()->getSize() - m_tntConfig->m_openTableCnt;
		if (numTablesNeedClose <= 0) {
			releaseATSLock(session, IL_X);
			break;
		}
		for (curr = m_db->getOpenTableLRU()->getHeader()->getNext();; curr = curr->getNext()) {
			// �ж��Ƿ�ɨ������������
			if (curr == m_db->getOpenTableLRU()->getHeader()) {
					listEnd = true;
					break;
			}
			Table *table = curr->get();
			try {
				if (realCloseTableIfNeed(session, table))
					break;
			} catch (NtseException &e) {
				// �ر����쳣��˵����flush������ʱ����I/O����
				releaseATSLock(session, IL_X);
				m_db->getSyslog()->log(EL_PANIC, "CloseOpenTable Error: %s", e.getMessage());
			}
		}
		releaseATSLock(session, IL_X);
		// �������ɨ����ϣ���������
		if (listEnd)
			break;

	}
	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(conn);
}


/** ����TableBase��ȡ���еı�Id��purge��Ҫ������������С�����õ���Ҫ�����ı�ļ���
*	@ids						��Id����
*	@skipSmallTables			�Ƿ�����С��
*	@mheap					    trueΪ�����ڴ��ҳ������falseΪ����hashIndex�Ĵ�С
*   @limit                      �ж��Ƿ�����ı�׼ֵ
*   @numSkipTables				[In/Out]�����ı���Ŀ
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

/** ��ָ������ڴ�Ѽ�¼flush�����
 * @param session �Ự
 * @param table ��Ҫflush��table
 */
/*void TNTDatabase::flushMHeap(Session *session, TNTTable *table) {
	McSavepoint msp(session->getMemoryContext());
	u16 tableId = table->getNtseTable()->getTableDef()->m_id;
	TNTTransaction *prevTrx = session->getTrans();
	assert(prevTrx != NULL);
	assert(prevTrx->isTableLocked(tableId, TL_S) || prevTrx->isTableLocked(tableId, TL_X));
	TNTTransaction *trx = m_tranSys->allocTrx();
	trx->startTrxIfNotStarted(session->getConnection(), true);
	//��Ϊ��ʱ�Ѿ�����S����X�ı��������Դ�ʱ�����ڸñ�������Ѿ�ȫ���ύ��
	//��ʱtrx��assign readview�ض��ܿ������е����¼�¼
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

/** ��ָ����TNTTable����purge 
*	@param session	�Ựsession
*	@param table ��Ҫpurge��table
*   @param purgeTarget purge��Ŀ��
*	@param minReadView purge��minReadView
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
		//ɾȥ�ȴ���Ծ��������̣���Ϊ����purge��ntse��������������μ�#104906->#7
		/*if (likely(m_dbStat == DB_RUNNING)) {
			purgeWaitTrx(trx->getTrxId());
		}*/
		SYNCHERE(SP_TNTDB_PURGE_PHASE2_BEFORE);
		purgePhaseTwo(session, table);
		lsn = table->writePurgeTableEnd(session, trx->getTrxId(), trx->getTrxLastLsn());
	}

}

/** TNT�����ڴ�����ҳ��������
*
*/
void TNTDatabase::reclaimMemIndex() throw(NtseException){
	vector<u16>				allIds;
	// û���ڴ��ֱ���˳�
	if (m_tntTabBases.getSize() == 0)
		return;

	// �������ӡ��Ự
	Connection *conn = m_db->getConnection(true, "tnt:reclaimMemIndex");
	Session *session = m_db->getSessionManager()->allocSession("tnt::reclaimMemIndex", conn);
	TNTTransaction *trx = m_tranSys->allocTrx();
	trx->startTrxIfNotStarted(conn, true);
	session->setTrans(trx);

	// ��ȡ���е��ڴ��
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
		// ��Ҫ��Ϊ�˽��ͨ��tableId��ȡpath��Ȼ����pathȥopen table��
		// �м��������rename tableName1 to tableName2��Ȼ��create tableName1����ʱtableName1��Ӧ��tableId�Ѿ����ǵ������Ǹ�tableId��
		if (table->getNtseTable()->getTableDef()->m_id != tableId) {
			table->unlockMeta(session, IL_S);
			closeTable(session, table);
			goto _ReStart;
		}
		// �������������ղ���
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


/** PurgeAndDumpȫ�������������ڣ�TNT��������ģ�����
*	@neededPurgePhase	��Ҫ���ж��ٽ׶ε�purge
*	@skipSmallTables	�Ƿ�����С��
*	@log				�Ƿ�Ҫ����־
*/
void TNTDatabase::purgeAndDumpTntim(PurgeTarget purgeTarget, bool skipSmallTables, bool needDump, bool log) throw(NtseException) {
	TNTTable                *table         = NULL;
	Session                 *session       = NULL;
	TNTTransaction          *trx           = NULL;
	PurgeAndDumpProcess		purgeAndDumpProcess;
	vector<u16>				allIds;
	int						defragTarget   = MAX_DEFRAG_NUM;
	int						totalDefragPageNum = 0;
	
	//����ǰ���dump�����������tableBase�����еı�
	if(needDump)
		assert(!skipSmallTables);

	// 1. ���û���ڴ����ֱ���˳�
	if (m_tntTabBases.getSize() == 0 && !needDump) {
		return;
	}

	// 2. ��ʼ��purgeAndDump�ṹ
	initPurgeAndDump(&purgeAndDumpProcess, &needDump, log);

	session = purgeAndDumpProcess.m_session;
	trx = purgeAndDumpProcess.m_purgeTrx;

	// 4. дPurge��ʼ��־
	if (purgeTarget >= PT_PURGEPHASE1)
		writePurgeBeginLog(session, trx->getTrxId(), trx->getTrxLastLsn());

	// 5. ����m_tntTabBases����purge���еı�
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
			// ���������˴�dumpʧ��
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
			//��Ҫ��Ϊ�˽��ͨ��tableId��ȡpath��Ȼ����pathȥopen table��
			//�м��������rename tableName1 to tableName2��Ȼ��create tableName1����ʱtableName1��Ӧ��tableId�Ѿ����ǵ������Ǹ�tableId��
			if (table->getNtseTable()->getTableDef()->m_id != tableId) {
				table->unlockMeta(session, IL_S);
				closeTable(session, table);
				goto _ReStart;
			}
			
			purgeAndDumpProcess.m_curTab = table;
#ifdef NTSE_UNIT_TEST
			//�������Ҫpurge��ôֱ������defrag/dump�׶�
			if (purgeTarget == PT_DEFRAG)
				goto DEFRAG_TNT_IM;
			if (purgeTarget ==  PT_NONE)
				goto DUMP_TNT_IM;
#else
			assert(purgeTarget >= PT_PURGEPHASE1);
#endif
			//�޸�lockTableΪtryLockTable����ΪaddIndexʱ��metaLockΪU����table LockΪS����
			//�����ʱpurge��meta LockΪS����Ȼ��ȴ�table��IX������ʱaddIndex�����Ҫ��U������ΪX������ʱ����������
			if (!trx->tryLockTable(TL_IX, tableId)) {
				// ���������˴�dumpʧ��
				purgeAndDumpProcess.m_dumpSuccess = false;
				m_tntStatus.m_numPurgeSkipTable++;
				goto DUMP_TNT_IM;
			}
			
			//��session��purge����
			purgeAndDumpProcess.m_session->setTrans(purgeAndDumpProcess.m_purgeTrx);
			purgeAndDumpProcess.m_purgeTrx->trxAssignPurgeReadView();
			purgeAndDumpProcess.m_minTrxId = purgeAndDumpProcess.m_purgeTrx->getReadView()->getUpTrxId();

			purgeTNTTable(session, table, purgeTarget);
						
			if (purgeTarget >= PT_PURGEPHASE2) {
				purgeAndDumpProcess.m_tabPurged++;
			}
			
			// �������ͷű���ǰ�ر�readView����������ͷű�����������ctx������readView�ڴ汻��ǰ�ͷ�
			m_tranSys->closeReadViewForMysql(trx);
			
			trx->unlockTable(TL_IX, tableId);

			//Defrag TNT�ڴ��
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

	// 6. дpurge�����־
	if (purgeTarget >= PT_PURGEPHASE1)
		writePurgeEndLog(session, trx->getTrxId(), trx->getTrxLastLsn());
	
	// 7. �޸Ŀ����ļ�����¼dump��Ϣ��ͬʱɾ��ǰһ��dump����
	if (needDump) 
		finishDumpTntim(&purgeAndDumpProcess);

	m_tntSyslog->log(EL_LOG, "==> End PurgeAndDump TNTIM && time = %d s", purgeLastTime);

	// �ռ�ͳ����Ϣ
	m_tntStatus.m_purgeLastTime = purgeLastTime;
	m_tntStatus.m_purgeTotalTime += purgeLastTime;
	if (purgeLastTime >= m_tntStatus.m_purgeMaxTime) {
		m_tntStatus.m_purgeMaxTime = purgeLastTime;
		m_tntStatus.m_purgeMaxBeginTime = purgeAndDumpProcess.m_begin;
	}

	// 8. �ͷ�purgeAndDump�ṹ��
	deInitPurgeAndDump(&purgeAndDumpProcess, needDump);

	
}

/** redo purge����
 * @param log purge�����һ����־
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
		
		// 6. дpurge�����־
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
/**	��ʼ��PurgeAndDumpProcess�ṹ
*	�ⲿ�û�����ͨ�����ִ�д˺���
*	@needDump[in/out]���dump�����ļ���ʧ�ܣ��򴫳�Ϊfalse
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
		//Ϊpurge����һ����ʱreadView��ֻ������û������readViewʱ��ȷ��ȡDumpLsn
		purgeAndDumpProcess->m_purgeTrx->trxAssignReadView();
		//��ȡdumpLSN
		LsnType logTailLsn = m_tntTxnLog->tailLsn();
		LsnType minTrxLsn = m_tranSys->getMinStartLsnFromReadViewList();
		if (minTrxLsn != INVALID_LSN) {
			purgeAndDumpProcess->m_dumpLsn = minTrxLsn;
		} else {
			purgeAndDumpProcess->m_dumpLsn = logTailLsn;
		}
		//�ر�purge�������ʱreadView
		m_tranSys->closeReadViewForMysql(purgeAndDumpProcess->m_purgeTrx);
		
		purgeAndDumpProcess->m_dumpSuccess = true;
	}
}
/**	�ͷ�PurgeAndDumpProcess�ṹ�е�dump���Ԫ��
*
*/
void TNTDatabase::deInitDump(PurgeAndDumpProcess *purgeAndDumpProcess) {
	UNREFERENCED_PARAMETER(purgeAndDumpProcess);
	//��Ҫ���ճ������־
	m_tntTxnLog->reclaimOverflowSpace();
}

/**	�ͷ�PurgeAndDumpProcess�ṹ
*
*/
void TNTDatabase::deInitPurgeAndDump(PurgeAndDumpProcess *purgeAndDumpProcess, bool needDump) {
	// �ͷ�purge�ṹ
	purgeAndDumpProcess->m_purgeTrx->commitTrx(CS_PURGE);
	m_tranSys->freeTrx(purgeAndDumpProcess->m_purgeTrx);
	purgeAndDumpProcess->m_purgeTrx = NULL;

	//�ͷ�dump�ṹ
	if(needDump) {
		deInitDump(purgeAndDumpProcess);
	}

	//�ͷ�session������
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

/**	Purge����
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

	//m_tntBufSizeΪҳ����
	uint freePageLimit = ((100 - m_tntConfig->m_purgeThreshold)*m_tntConfig->m_tntBufSize)/100;
	//��pagePool����ҳ��֮��targetSize���ܻ�С�ڵ�ǰ��ʵ��size
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

/**	purge������һ�׶Σ�һ��TNT�У��ɵײ����purge table����������Database�����޷���֪
*
*
*/
void TNTDatabase::purgePhaseOne(Session *session, TNTTable *table) {
	table->purgePhase1(session, session->getTrans());
}

/**	purge������һ�׶ν������ڶ��׶ο�ʼǰ����Ҫ�ȴ���ǰ���ϵ�ȫ��ɨ�裬ȡ����һ����¼
*	һ��TNT���˺�����TNT Mrecords�������
*/
void TNTDatabase::purgeWaitTrx(TrxId purgeTrxId) throw(NtseException) {
	std::vector<TrxId> trxIds;
	m_tranSys->getActiveTrxIds(&trxIds);
	u32 size = trxIds.size();
	assert(size > 0);//������Ϊpurge�������ڵ�ʱ�϶��ǻ�Ծ��
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

/**	purge�����ڶ��׶Σ�һ��TNT�У��ɵײ����purge table����������Database�����޷���֪
*
*
*/
void TNTDatabase::purgePhaseTwo(Session *session, TNTTable *table) throw(NtseException) {
	u32 beginTime = System::fastTime();
	u64 ret = table->purgePhase2(session, session->getTrans());
	m_tntSyslog->log(EL_LOG, "purge table(%s) Phase2 and erase "I64FORMAT"u record from memHeap and time is %d", 
		table->getNtseTable()->getPath(), ret, System::fastTime() - beginTime);
}

/**	purge�����ڶ��׶Σ�ɾ��heap��¼
*
*
*/
void TNTDatabase::purgeTntimHeap(TNTTable *table) {
	UNREFERENCED_PARAMETER(table);
}

/**	purge�����ڶ��׶Σ�ɾ��Indice��¼
*
*
*/
void TNTDatabase::purgeTntimIndice(TNTTable *table) {
	UNREFERENCED_PARAMETER(table);
}


/**	Dump�����������ṩ��������
*
*/
void TNTDatabase::doDump() throw(NtseException) {

}

/**	Dump�����������ṩ��������
*
*/
void TNTDatabase::finishingDumpAndLock() throw(NtseException) {

}

/**	Dump�����������ṩ��������
*
*/
void TNTDatabase::doneDump() {

}

/*********************************************************/
/* Dump�����ڲ�����                                      */
/*********************************************************/

/**	�жϵ�ǰ�Ƿ���Ҫdump
*	���ڴ�����dump�����ܲ�����Ҫ
*/
bool TNTDatabase::dumpJudgeNeed() {
#ifdef NTSE_UNIT_TEST
	return false;
#else
	// һ��TNTʵ���У�Ĭ��Ϊtrue����������ʵ������������˺���
	u64 delta = m_tntTxnLog->tailLsn() - m_tntCtrlFile->getDumpLsn();
	u64 limit = m_tntConfig->m_dumponRedoSize;//m_dumponRedoSize��bytesΪ��λ
	if (delta >= limit) {
		return true;
	} else {
		return false;
	}
#endif
}

/**	Dumpһ��TNT�ڴ��
*	@firstTable	�Ƿ�Ϊ��һ��dump�ı���ʱ��Ҫ����dumpĿ¼
*/
void TNTDatabase::dumpTableHeap(PurgeAndDumpProcess *purgeAndDumpProcess) throw(NtseException) {
	UNREFERENCED_PARAMETER(purgeAndDumpProcess);
/*	TNTTable *table	= purgeAndDumpProcess->m_curTab;
	TableDef *tableDef = table->getNtseTable()->getTableDef();

	u64	errCode, offset = 0;

	// 1. ��׼��dump�ı��Ӧ���ļ�
	string dumpFile	= generateDumpFilePath(purgeAndDumpProcess->m_dumpPath, purgeAndDumpProcess->m_dumpSN, tableDef->m_id);
	File *file = new File(dumpFile.c_str());
	if ((errCode = file->create(false, false)) != File::E_NO_ERROR) {
		delete file;
		NTSE_THROW(errCode, "Can not create file %s", dumpFile.c_str());
	}

	// 2. ����MRecords�ӿڣ���������
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

/** ��ʼ������hash���������
 * @param ����hash�������̾��
 */
void TNTDatabase::initDefragHashIndex(DefragHashIndexProcess *defragHashIndexProcess) throw(NtseException){
	Connection *conn = m_db->getConnection(true, "tnt:defragHashIndex");
	Session *session = m_db->getSessionManager()->allocSession("tnt::defragHashIndex", conn);

	defragHashIndexProcess->m_begin = System::fastTime();

	defragHashIndexProcess->m_trx = m_tranSys->allocTrx();
	defragHashIndexProcess->m_trx->startTrxIfNotStarted(conn, true);
	//������Ҫ����readview��Ϊ��findMinReadViewInActiveTrxs�ҵ�minReadView
	defragHashIndexProcess->m_trx->trxAssignReadView();

	defragHashIndexProcess->m_conn	= conn;
	defragHashIndexProcess->m_session = session;
	defragHashIndexProcess->m_session->setTrans(defragHashIndexProcess->m_trx);
}

/** �ͷ�����hash��������ռ�õ���Դ
 * @param ����hash�������̾��
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

//����hash����
uint TNTDatabase::defragHashIndex(bool skipSmallTable) throw(NtseException) {
	u32						total = 0;
	vector<u16>				allIds;
	TNTTable                *table = NULL;
	DefragHashIndexProcess  defragHashIndexProcess;

	// 1.��������û���ڴ����ֱ���˳�
	if (m_tntTabBases.getSize() == 0) {
		return 0;
	}
	
	// Defrag HashIndexͳ����Ϣ
	m_tntStatus.m_defragHashCnt++;

	// 2.��ʼ��defrag hash index�ṹ
	initDefragHashIndex(&defragHashIndexProcess);

	TrxId minReadView = m_tranSys->findMinReadViewInActiveTrxs();
	assert(minReadView != INVALID_TRX_ID);
	Session *session = defragHashIndexProcess.m_session;
	// ��ȡ��minReadView����ͷ�readView
	m_tranSys->closeReadViewForMysql(session->getTrans());
	// 3. ����TNT table��������defrag Index
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
		//��Ҫ��Ϊ�˽��ͨ��tableId��ȡpath��Ȼ����pathȥopen table��
		//�м��������rename tableName1 to tableName2��Ȼ��create tableName1����ʱtableName1��Ӧ��tableId�Ѿ����ǵ������Ǹ�tableId��
		if (table->getNtseTable()->getTableDef()->m_id != tableId) {
			table->unlockMeta(session, IL_S);
			closeTable(session, table);
			goto _ReStart;
		}

		total += table->getMRecords()->defragHashIndex(session, minReadView, m_tntConfig->m_defragHashIndexMaxTraverseCnt);
		table->unlockMeta(session, IL_S);
		closeTable(session, table);
	}

	// 4 ����defrag hashIndex����ʱ��
	defragHashIndexProcess.m_end = System::fastTime();

	m_tntSyslog->log(EL_DEBUG, "defrag hashIndex the total is %d and time is %d s", total, defragHashIndexProcess.m_end - defragHashIndexProcess.m_begin);

	// 6. defrag hashIndex�������ͷ���Դ
	deInitDefragHashIndex(&defragHashIndexProcess);
	return total;
}

// ��ʼ��reclaimLob
void TNTDatabase::initReclaimLob(ReclaimLobProcess *reclaimLobProcess) throw(NtseException){
	Connection *conn = m_db->getConnection(false, "tnt:reclaimLob");
	Session *session = m_db->getSessionManager()->allocSession("tnt::reclaimLob", conn);
	reclaimLobProcess->m_begin = System::fastTime();
	
	// ��ʱ����һ���������ڻ�ȡ��ʱϵͳ��minReadview
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

// ����reclaimLob
void TNTDatabase::deInitReclaimLob(ReclaimLobProcess *reclaimLobProcess) {
	Connection *conn = reclaimLobProcess->m_conn;
	Session *session = reclaimLobProcess->m_session;
	
	m_db->getSessionManager()->freeSession(session);
	reclaimLobProcess->m_session = NULL;
	m_db->freeConnection(conn);
	reclaimLobProcess->m_conn = NULL;
}

/** ���հ汾���е�poolIndex���ݱ��еĴ����
 * @param poolIndex �汾���еı����
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

	//����ñ��汾�ر����ڻ�Ծ״̬�����ܻ���
	if (m_tntCtrlFile->getVerPoolStatus(poolIdx) == VP_ACTIVE) {
		m_tntCtrlFile->unlockCFMutex();
		return;
	}
	
	m_tntCtrlFile->unlockCFMutex();

	ReclaimLobProcess reclaimLobProcess;
	//��ʼ�����մ����,�����������ʧ�ܣ��ò�����׳��쳣
	initReclaimLob(&reclaimLobProcess);

	m_tntCtrlFile->lockCFMutex();
	assert(m_tntCtrlFile->getVerPoolStatus(poolIdx) == VP_USED);
	//ֻ��minReadView��С�ڸð汾�ص�maxTrxIdʱ����Ҫ����
	if (m_tntCtrlFile->getVerPoolMaxTrxId(poolIdx) >= reclaimLobProcess.m_minReadView) {
		m_tntCtrlFile->unlockCFMutex();
		deInitReclaimLob(&reclaimLobProcess);
		return;
	}
	m_tntSyslog->log(EL_LOG, "==> Begin Reclaim Lob");

	m_tntStatus.m_reclaimVersionPoolCnt++;
	// ��¼�汾�ػ��տ�ʼʱ��
	m_tntStatus.m_reclaimVerPoolLastBeginTime = System::fastTime();

	//�ٴμ������ļ���������״̬
	//FIXME: Ŀǰֻ���ǵ��߳�reclaim
	assert(m_tntCtrlFile->getVerPoolStatus(poolIdx) == VP_USED);
	m_tntCtrlFile->writeBeginReclaimPool(poolIdx);
	m_tntCtrlFile->unlockCFMutex();

	reclaimLobProcess.m_begin = System::fastTime();
	//��ʼ���հ汾��
	m_vPool->defrag(reclaimLobProcess.m_session, poolIdx);
	reclaimLobProcess.m_end = System::fastTime();


	//����tnt ctrl file״̬
	m_tntCtrlFile->lockCFMutex();
	m_tntCtrlFile->writeLastReclaimedPool(poolIdx);
	m_tntCtrlFile->unlockCFMutex();

	m_tntSyslog->log(EL_LOG, "==> End Reclaim Lob and time is %d s", reclaimLobProcess.m_end - reclaimLobProcess.m_begin);
	//�ͷŻ��մ����ṹ
	deInitReclaimLob(&reclaimLobProcess);
	// ��¼�汾�ػ���ʱ��
	m_tntStatus.m_reclaimVerPoolLastEndTime = System::fastTime();
}

/** redo���հ汾�ش����
 * @param reclaimLobTrxId ���հ汾�ش���������id
 */
void TNTDatabase::redoReclaimLob() {	
	// �ӿ����ļ���ȡ�ϴλ��յİ汾��
	m_tntCtrlFile->lockCFMutex();
	uint poolCnt = m_tntCtrlFile->getVersionPoolCnt();
	u8 poolIdx = m_tntCtrlFile->getReclaimedVerPool();
	// �õ�������Ҫ���յİ汾�غ�
	poolIdx = (poolIdx == (poolCnt - 1)) ? 0 : (poolIdx + 1);

	VerpoolStat vpStat = m_tntCtrlFile->getVerPoolStatus(poolIdx);
	m_tntCtrlFile->unlockCFMutex();
	// �������ʱ����������,ֱ��truncate����
	if(vpStat == VP_RECLAIMING) {	
		// ���������񣬲����մ����ֻ�԰汾�ر���truncate����
		Connection *conn = m_db->getConnection(true, __FUNC__);
		Session *session = m_db->getSessionManager()->allocSession(__FUNC__, conn);
		m_vPool->defrag(session, poolIdx, true);	
		m_db->getSessionManager()->freeSession(session);
		m_db->freeConnection(conn);

		// �޸Ŀ����ļ�
		m_tntCtrlFile->lockCFMutex();
		m_tntCtrlFile->writeLastReclaimedPool(poolIdx);
		m_tntCtrlFile->unlockCFMutex();
	}
}

/**	�ж�TNT���ݿ��Ƿ���Ҫ���ָ�
*	@return true����Ҫ�ָ���false������Ҫ�ָ�
*
*/
bool TNTDatabase::isTntShouldRecover() {
	return true;
}

/**	�ȴ���̨�߳�
*	@all	��Ҫ�ȴ����к�̨�̳߳�ʼ�����
*	
*/
void TNTDatabase::waitBgTasks(bool all) {
	UNREFERENCED_PARAMETER(all);
}

/**	�����ر�TNT�ڴ������
*
*
*/
void TNTDatabase::closeTNTTableBases() {
	DLink<TNTTableBase *>	*tblBaseCur	  = NULL;
	TNTTableBase			*tableBase	  = NULL;
	Connection				*conn		  = m_db->getConnection(true, __FUNC__);
	Session					*session	  = m_db->getSessionManager()->allocSession(__FUNC__, conn);

	// �������ƣ��˴���Ҫ����ATS Lock
	acquireATSLock(session, IL_X, -1, __FILE__, __LINE__);

	assert(m_pathToTables->getSize() == 0 && m_idToTables->getSize() == 0);

	// ����TNT�ڴ��dump
	while ((tblBaseCur = m_tntTabBases.removeLast()) != NULL) {
		tableBase = tblBaseCur->get();
		// �ͷŵ�ǰ�ڵ�
		m_idToTabBase->remove(tableBase->m_tableId);
		tableBase->close(session, false);
		
		delete tableBase;
		tableBase = NULL;
	}

	// �ر����֮��TableBase����Hash���Ϊ��
	assert(m_idToTabBase->getSize() == 0);
	assert(m_tntTabBases.getSize() == 0);

	releaseATSLock(session, IL_X);
	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(conn);
}

/**	�ر����д򿪵��ڴ��
*
*/
void TNTDatabase::closeOpenTables(bool closeNtse) {
	Connection *conn = m_db->getConnection(true, __FUNC__);
	Session *session = m_db->getSessionManager()->allocSession(__FUNC__, conn);

	acquireATSLock(session, IL_X, -1, __FILE__, __LINE__);

	// ��TNT��TableInfo�ƶ��������У����ڱ���
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

/** �ӻ�Ծ������
 * @param session �Ự
 * @param mode ģʽ��ֻ��ΪIL_S��IL_X
 * @param timeoutMs <0��ʾ����ʱ��0�ڲ��������õ���ʱ���ϳ�ʱ��>0Ϊ����Ϊ��λ�ĳ�ʱʱ��
 * @param file �����ļ�
 * @param line �����к�
 * @throw NtseException ������ʱ�����ָ��timeousMsΪ<0�����׳��쳣
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

/** �ͷŻ�Ծ������
 * @param session �Ự
 * @param mode ģʽ��ֻ��ΪIL_S��IL_X
 */
void TNTDatabase::releaseATSLock(Session *session, ILMode mode) {
	assert(mode == IL_S || mode == IL_X);
	ftrace(ts.ddl || ts.dml, tout << session << mode);

	IntentionLock *atsLock = m_db->getAtsLockInst();
	atsLock->unlock(session->getId(), mode);
	session->removeLock(atsLock);
}


/** �õ�ָ���Ự���ӵĻ�Ծ������ģʽ
 * @param session �Ự
 * @return ָ���Ự���ӵĻ�Ծ������ģʽ��������IL_NO��IL_S��IL_X
 */
ILMode TNTDatabase::getATSLock(Session *session) const {
	IntentionLock	*atsLock = m_db->getAtsLockInst();
	return atsLock->getLock(session->getId());
}

/** ��DDL����CREATE/DROP/RENAME����ʱ��X����TRUNCATE/ADD INDEX���޸ı�ṹ�Ĳ���ʱ��Ҫ��
 * IX�������߱���ʱ��Ҫ��S�����Ӷ���ֹ�ڱ����ڼ�����κ�DDL����
 *
 * @param session �Ự
 * @param mode ��ģʽ
 * @throw NtseException ������ʱ����ʱʱ�������ݿ������е�m_tlTimeoutָ��
 */
void TNTDatabase::acquireDDLLock(Session *session, ILMode mode, const char *file, uint line) throw(NtseException) {
	ftrace(ts.ddl || ts.dml, tout << session << mode);

	IntentionLock *ddlLock = m_db->getDdlLockInst();
	if (!ddlLock->lock(session->getId(), mode, m_tntConfig->m_ntseConfig.m_tlTimeout * 1000, file, line))
		NTSE_THROW(NTSE_EC_LOCK_TIMEOUT, "Unable to acquire DDL lock in %s mode.", IntentionLock::getLockStr(mode));
	session->addLock(ddlLock);
}

/**
 * �ͷ�DDL��
 *
 * @param session �Ự
 * @param mode ��ģʽ
 */
void TNTDatabase::releaseDDLLock(Session *session, ILMode mode) {
	ftrace(ts.ddl || ts.dml, tout << session << mode);

	IntentionLock *ddlLock = m_db->getDdlLockInst();
	ddlLock->unlock(session->getId(), mode);
	session->removeLock(ddlLock);
}

void TNTDatabase::dumpRecoverApplyLog() throw(NtseException) {

}

// ��֤�Ƿ�����Ҫɾ�����ڴ��(Ntse�ָ������У�ControlFileɾ���ı�)
bool TNTDatabase::dumpRecoverVerifyHeaps() {
	return true;
}

// Tntim heap�ָ����֮��(����redo/undo),���ݱ��壬build�ڴ�����
void TNTDatabase::dumpRecoverBuildIndice() throw(NtseException) {
	vector<u16> allIds; 
	Connection *conn = m_db->getConnection(false, "tnt:rebuildIndex");
	Session *session = m_db->getSessionManager()->allocSession("tnt::rebuildIndex", conn);
	//�·���һ������
	TNTTransaction *trx = m_tranSys->allocTrx();
	trx->startTrxIfNotStarted(conn);
	session->setTrans(trx);
	//������ȡ���б�
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

// crash recovery��������DumpRecover�д���prepare״̬������copy��TransactionSys��
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
		//TODO ���Կ��ǲ��رգ�������tnt redoʱ��û��Ҫ�ٴ���
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
 * ��ȡ�ع�������п�ʼ�ع�����������begin rollback�ع�lsn
 *
 * @return  ���翪ʼ�ع���lsn
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
 * �㱨����״̬��Ϣ
 *
 * @param status �������
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

	// ���û������purge���򲻴�ӡʱ��
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

	// ���û�������汾���л����򲻴�ӡʱ��
	memset(status->m_switchVerPoolLastTime, 0, 30);
	time_t lastSwitchVerPoolTime = (time_t)(m_tntStatus.m_switchVerPoolLastTime);
	if (lastSwitchVerPoolTime != 0)
		System::formatTime(status->m_switchVerPoolLastTime, sizeof(status->m_switchVerPoolLastTime), &lastSwitchVerPoolTime);
	else
		strcpy(status->m_switchVerPoolLastTime, "NULL");

	// ���û�������汾�ػ���,�򲻴�ӡ��ʼʱ��
	memset(status->m_reclaimVerPoolLastBeginTime, 0, 30);
	time_t lastReclaimVerPoolBeginTime = (time_t)(m_tntStatus.m_reclaimVerPoolLastBeginTime);
	if (lastReclaimVerPoolBeginTime != 0)
		System::formatTime(status->m_reclaimVerPoolLastBeginTime, sizeof(status->m_reclaimVerPoolLastBeginTime), &lastReclaimVerPoolBeginTime);
	else
		strcpy(status->m_reclaimVerPoolLastBeginTime, "NULL");

	// ����汾�ػ������ڽ����У��򲻴�ӡEndʱ���
	memset(status->m_reclaimVerPoolLastEndTime, 0, 30);
	time_t lastReclaimVerPoolEndTime = (time_t)(m_tntStatus.m_reclaimVerPoolLastEndTime);
	if(lastReclaimVerPoolEndTime >= lastReclaimVerPoolBeginTime && lastReclaimVerPoolEndTime != 0)
		System::formatTime(status->m_reclaimVerPoolLastEndTime, sizeof(status->m_reclaimVerPoolLastEndTime), &lastReclaimVerPoolBeginTime);
	else
		strcpy(status->m_reclaimVerPoolLastEndTime, "NULL");
}

/** �ͷ�TNTBackupProcess�нṹ����������
 * @param tntBackupProcess ���ݾ��
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

/** kill recover��hang��prepare���� */
void TNTDatabase::killHangTrx() {
	m_tranSys->killHangTrx();
}

/** ��path��Ӧ���ļ��ж�ȡ���е�����
 * @param path ��Ҫ��ȡ�ļ���·��
 * @param bufSize out path��Ӧ�ļ��ܹ��ж���byte����
 * return path��Ӧ�ļ�������
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

/** �����ļ�������buf�е���������д��մ������ļ�
 * @param path ��Ҫ������д����ļ�·��
 * @param buf ��Ҫд�������
 * @param bufSize ��Ҫд���������
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

/** ����tnt ctrl��dump�ļ�
 * @param backupProcess ���ݾ��
 */
void TNTDatabase::backupTNTCtrlAndDumpFile(TNTBackupProcess *backupProcess) throw(NtseException) {
	BackupProcess *ntseBackupProcess = backupProcess->m_ntseBackupProc;
	//��Ϊdump��table����meta lock s����������dump��backup����û���������⣬ֻ��ͨ������ctrl mutexȥ����
	m_tntCtrlFile->lockCFMutex();
	//uint dumpSN = m_tntCtrlFile->getDumpSN();
	//dumpSNΪ0��ʾ�ֻ�δ����dump
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
 * ��ʼ�����ݲ���
 *
 * @param backupDir ���ݵ����Ŀ¼
 * @throw NtseException �޷����������ļ�����DDL����ʱ���Ѿ��б��ݲ����ڽ����е�
 */
TNTBackupProcess* TNTDatabase::initBackup(const char *backupDir) throw(NtseException) {
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

	BackupProcess *ntseBackupProcess = new BackupProcess(this->m_db, backupDir, 256);
	TNTBackupProcess *backupProcess = new TNTBackupProcess(ntseBackupProcess, this);
	
	try {
		//��������
		TNTTransaction *trx = m_tranSys->allocTrx();
		trx->disableLogging();
		trx->startTrxIfNotStarted(ntseBackupProcess->m_session->getConnection(), true);
		ntseBackupProcess->m_session->setTrans(trx);
		// ����DDL���
		acquireDDLLock(ntseBackupProcess->m_session, IL_S, __FILE__, __LINE__);
	} catch (NtseException &e) {
		releaseBackupProcess(backupProcess);
		m_backingUp.set(0);
		throw e;
	}

	// ��ֹ�����ڼ��������־������
	LsnType minOnlineLsn = min(m_tntCtrlFile->getDumpLsn(), m_db->getControlFile()->getCheckpointLSN());
	ntseBackupProcess->m_onlineLsnToken = m_tntTxnLog->setOnlineLsn(minOnlineLsn);
	m_tntSyslog->log(EL_LOG, "Begin backup to %s", backupDir);

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
void TNTDatabase::doBackup(TNTBackupProcess *backupProcess) throw(NtseException) {
	BackupProcess *ntseBackupProc = backupProcess->m_ntseBackupProc;
	Session *session = ntseBackupProc->m_session;
	assert(m_db->getDdlLockInst()->isLocked(session->getId(), IL_S));
	assert(backupProcess->m_stat == BS_INIT);
	ftrace(ts.ddl, tout << backupProcess);

	backupProcess->m_stat = BS_FAILED;

	// ���ݿ����ļ������ݿ����ļ������ڱ��ݱ�����֮ǰ����ֹ�ڱ����ڼ�����˼���������»ָ���ʼ�����
	m_tntSyslog->log(EL_LOG, "Backup control file...");
	LsnType logStartLsn = min(m_tntCtrlFile->getDumpLsn(), m_db->getControlFile()->getCheckpointLSN()); // ��־���ݵ���ʼLSN
	ntseBackupProc->m_logBackuper = new LogBackuper(m_tntTxnLog, logStartLsn);
	m_db->backupCtrlFile(ntseBackupProc);

	// ��ȡ����Ϣ
	u16 numTables = m_db->getControlFile()->getNumTables();
	u16 *tableIds = new u16[numTables];
	AutoPtr<u16> apTableIds(tableIds, true);
	m_db->getControlFile()->listAllTables(tableIds, numTables);
	// ����ÿ�ű������
	m_tntSyslog->log(EL_LOG, "Backup tables, numTables = %d", numTables);
	
	// Ϊ�ײ㱣֤������������ʱ��session�е�������ΪNULL
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

	// Ȼ�󱸷ݴ����һ��checkpoint��ʼ�����־��
	m_tntSyslog->log(EL_LOG, "Backup log, startLSN "I64FORMAT"u ...", logStartLsn);
	m_db->backupTxnlog(ntseBackupProc, true);
	// ����ntse��tnt���ݿ������ļ�
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
 * �������ݲ����������ݿ�Ϊֻ��״̬
 * @pre ����ΪBS_COPYED״̬
 *
 * @param backupProcess ���ݲ���
 * @throw NtseException ������־ʱд�ļ�ʧ��
 */
void TNTDatabase::finishingBackupAndLock(TNTBackupProcess *backupProcess) throw(NtseException) {
	assert(backupProcess->m_stat == BS_COPYED);
	ftrace(ts.ddl, tout << backupProcess);

	BackupProcess *ntseBackupProc = backupProcess->m_ntseBackupProc;
	Session *session = ntseBackupProc->m_session;

	backupProcess->m_stat = BS_FAILED;

	m_tntSyslog->log(EL_LOG, "Lock database...");
	// Ϊ��֤����������binlog��һ�£���Ҫ�ڱ��ݼ�������ʱ��ֹ�����ݿ������
	// ����������ˢ��MMS���»�����־����һ��������ʵ��:
	// 1. �õ���ǰ�򿪵ı�������MMS���»���ı��б�
	// 2. ������ÿ�ű�pinס�ñ��ֹ�䱻ɾ����رգ�Ȼ��ˢMMS���»�����־
	// 3. ����openLock��ֹ���򿪻�ر�
	// 4. �������б���ֹ�Ա��д����
	// 5. ��Ϊ����MMS���»���ı��ٴ�ˢ��MMS���»�����־�����ڸ�ˢ��һ��
	//    ���ͨ���ȽϿ죬�������Լ���д���������õ�ʱ��

	// ��һ�λ�ȡ����MMS���»���ı��б�
	vector<string> allPaths;

	acquireATSLock(session, IL_S, -1, __FILE__, __LINE__);
	allPaths = m_db->getOpenedTablePaths(session);
	releaseATSLock(session, IL_S);

	// ��һ��ˢMMS���»��棬С�ʹ����MMS�������ø��»�����˲���Ҫˢ
	// �ײ������Ϊ��֤������session����ʱȡ������
	TNTTransaction *trx = session->getTrans();
	session->setTrans(NULL);
	for (size_t i = 0; i < allPaths.size(); i++) {
		Table *table = m_db->pinTableIfOpened(session, allPaths[i].c_str(), -1);
		// ���ڿ�ʼ����ʱ�Ѿ���ֹ������DDL������������ﲻ��Ҫ�ӱ�Ԫ������
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

	// ����ÿ�ű��ֹд�������ٴ�ˢMMS���»���
	acquireATSLock(session, IL_S, -1, __FILE__, __LINE__);
	vector<Table *> tables = m_db->getOpenTables(session);
	for (size_t i = 0; i < tables.size(); i++) {
		//Table *table = m_db->pinTableIfOpened(session, allPaths[i].c_str(), -1);
		Table *table = tables[i];
		assert(table != NULL);
		//��ʱ������meta lock��X������Ϊֻ��alterTableArgument����Ҫmeta lock��X��IX
		//��alterTableArgument��Ҫddl IX�������ǽ��е��˲���ddl�Ѿ���S����
		table->tntLockMeta(session, IL_S, -1, __FILE__, __LINE__);
		TableDef *tableDef = table->getTableDef();
		//Ŀǰ��Ϊϵͳ���д��������ddl���û����д���������޸ı�������
		//��ô��ʱ�Ѿ���ֹ�����е�ddl���û���ı�������S�������û�������д����
		//�û����meta������S�����²����޸ı������������Ϊ��ʱϵͳ������д������
		if (TS_TRX == tableDef->getTableStatus()) {
			//�ڳ���meta lock��table lock�����������������ʱddl�Ѿ�����ֹ��ֻ��dml��Ч�����е�dml���Ǽ�meta lock s����
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
	// �������ڼ����������NTSE��־����֤���ݵ�NTSE������binlogһ�£��쳣ֱ���׳�
	m_db->backupTxnlog(ntseBackupProc, false);
	ntseBackupProc->m_tailLsn = m_tntTxnLog->tailLsn();
	m_tntSyslog->log(EL_LOG, "Finish backup, lsn: "I64FORMAT"u", ntseBackupProc->m_tailLsn);

	backupTNTCtrlAndDumpFile(backupProcess);
}

/**
 * ��������
 * @pre ����ΪBS_LOCKED��BS_FAILED״̬
 * @post backupProcess�����Ѿ�������
 *
 * @param backupProcess ���ݲ���
 */
void TNTDatabase::doneBackup(TNTBackupProcess *backupProcess) {
	BackupProcess *ntseBackupProc = backupProcess->m_ntseBackupProc;
	Session *session = ntseBackupProc->m_session;
	assert(backupProcess->m_stat == BS_LOCKED || backupProcess->m_stat == BS_FAILED);
	ftrace(ts.ddl, tout << backupProcess);

	if (backupProcess->m_stat == BS_LOCKED) {
		// �ָ�д����
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
	// ��ʱȡ��session�����񣬱�֤�ײ�Ĳ����߼�
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
 * �ӱ����ļ��ָ����ݿ�
 *
 * @param backupDir ��������Ŀ¼
 * @param basedir �����ļ��ָ������Ŀ¼
 * @param logdir  redo��־�ָ������Ŀ¼
 * @throw NtseException �������ļ�ʧ�ܣ�д����ʧ�ܵ�
 */
void TNTDatabase::restore(const char *backupDir, const char *baseDir, const char *logDir) throw(NtseException) {
	Database::restore(backupDir, baseDir, logDir);
	Syslog log(NULL, EL_LOG, true, true);
	string destDir(baseDir);
	string srcDir(backupDir);

	// ���������ļ�
	string ctfPath = destDir + NTSE_PATH_SEP + Limits::NAME_TNT_CTRL_FILE;
	string ctfBackPath = srcDir + NTSE_PATH_SEP + Limits::NAME_TNT_CTRL_FILE;
	File backCtrlFile(ctfBackPath.c_str());
	u64 errNo = backCtrlFile.move(ctfPath.c_str(), true);
	if (errNo != File::E_NO_ERROR)
		NTSE_THROW(errNo, "copy %s to %s failed", ctfBackPath.c_str(), ctfPath.c_str());

	//�ָ�
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
 * �������ݿ���open table cache ��С
 *
 * @param logEntry ������־
 */
void TNTDatabase::setOpenTableCnt(u32 tableOpenCnt) {
	// �޸�32λ�ı���������Ҫ��������
	m_tntConfig->setOpenTableCnt(tableOpenCnt);
}


/**
 * �ָ������н���TNT��redo��־���������dump_tnt_log �ļ���,���ڵ���
 *
 * @param logEntry ������־
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
		//TODO: ������־
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
		//TODO: ������־
		TrxId tid;
		LsnType preLsn;
		RowId rid;
		DrsHeap::parseInsertTNTLog(logEntry, &tid, &preLsn, &rid);
		assert(tid == trxId);
		syslog->log(EL_DEBUG, "TNT_UNDO_I_LOG, TRXID =  %llu , RowId = %llu, CURLSN = %llu, TableId = %hd", trxId, rid, lsn, tableId);
	} else if(logType == TNT_UNDO_LOB_LOG) {
		//TODO: ������־
		TrxId tid;
		LsnType preLsn;
		LobId lobId;
		LobStorage::parseTNTInsertLob(logEntry, &tid, &preLsn, &lobId);
		assert(tid == trxId);
		syslog->log(EL_DEBUG, "TNT_UNDO_LOB_LOG, TRXID =  %llu , LobId = %llu, CURLSN = %llu, TableId = %hd", trxId, lobId, lsn, tableId);
	} else if(logType == TNT_U_I_LOG) {
		//TODO: ������־
		TrxId tid;
		LsnType preLsn;
		RowId rid, rollBackId;
		u8 tableIndex;
		SubRecord *updateSub = NULL;
		TNTTable *table = getTNTTable(redoSession, logEntry->m_tableId);
		table->getMRecords()->parseFirUpdateLog(logEntry, &tid, &preLsn, &rid, &rollBackId, &tableIndex, &updateSub, redoCtx);
		syslog->log(EL_DEBUG, "TNT_U_I_LOG, TRXID =  %llu , RowId = %llu, CURLSN = %llu, TableId = %hd", trxId, rid, lsn, tableId);
		//����������
		RecordOper::parseAndPrintRedSubRecord(syslog, table->getNtseTable()->getTableDef(), updateSub);	
	} else if(logType == TNT_U_U_LOG) {
		//TODO: ������־
		TrxId tid;
		LsnType preLsn;
		RowId rid, rollBackId;
		u8 tableIndex;
		SubRecord *updateSub = NULL;
		TNTTable *table = getTNTTable(redoSession, logEntry->m_tableId);
		table->getMRecords()->parseSecUpdateLog(logEntry, &tid, &preLsn, &rid, &rollBackId, &tableIndex, &updateSub, redoCtx);
		syslog->log(EL_DEBUG, "TNT_U_U_LOG, TRXID =  %llu , RowId = %llu, CURLSN = %llu, TableId = %hd", trxId, rid, lsn, tableId);
		//����������
		RecordOper::parseAndPrintRedSubRecord(syslog, table->getNtseTable()->getTableDef(), updateSub);
	} else if(logType == TNT_D_I_LOG) {
		//TODO: ������־
		TrxId tid;
		LsnType preLsn;
		RowId rid, rollBackId;
		u8 tableIndex;
		TNTTable *table = getTNTTable(redoSession, logEntry->m_tableId);
		table->getMRecords()->parseFirRemoveLog(logEntry, &tid, &preLsn, &rid, &rollBackId, &tableIndex);
		syslog->log(EL_DEBUG, "TNT_D_I_LOG, TRXID =  %llu, RowId = %llu, CURLSN = %llu, TableId = %hd", trxId, rid, lsn, tableId);
	} else if(logType == TNT_D_U_LOG) {
		//TODO: ������־
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
