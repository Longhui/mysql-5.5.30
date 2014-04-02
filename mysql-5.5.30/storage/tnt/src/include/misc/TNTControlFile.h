/**
* TNT ControlFile and Config。
*
* @author 何登成
*/
#ifndef _TNTCONTROLFILE_H_
#define _TNTCONTROLFILE_H_

#include "util/Hash.h"
#include "misc/Txnlog.h"
#include "misc/Config.h"
#include "util/File.h"
#include "trx/TNTTransaction.h"

using namespace ntse;

namespace tnt {

#ifndef NTSE_UNIT_TEST
	typedef ntse::File File;
#endif

struct DumpProcess;
struct TNTConfig;

struct TNTCFDumpHeader {
	// tntim dump相关
	u32			m_cleanClosed;		// 是否安全关闭
//	u16			m_dumpTables;		// 上次备份的表数量
//	u64			m_dumpBegin;		// 上次备份开始时间
//	u64			m_dumpEnd;			// 上次备份结束时间
	LsnType		m_dumpLSN;			// 备份开始的LSN
//	uint		m_dumpSN;			// dump序列号,尝试一次dump，此序列号++
};

enum VerpoolStat {
	VP_FREE = 0,
	VP_ACTIVE,
	VP_USED,
	VP_RECLAIMING
};

struct TNTVerpoolInfo {
	TrxId			m_minTrxId;		// version pool对应的最小事务id
	TrxId			m_maxTrxId;		// version pool对应的最大事务id
	VerpoolStat		m_stat;			// version pool当前状态
};

struct TNTCFVerpoolHeader {
	// 版本池相关
	u8			m_verpoolCnt;		// 版本池数量
	u8			m_activeVerpool;	// 活跃版本池
	u8			m_reclaimedVerpool;	// 上一次成功回收的版本池
	TNTVerpoolInfo	*m_vpInfo;		// 每个version pool的相关信息
};

class TNTControlFile {
public:
	static TNTControlFile* open(const char *path, Syslog *syslog) throw(NtseException);
	static void create(const char *path, TNTConfig *config, Syslog *syslog) throw(NtseException);
	void close(bool clean = true);
	// 系统相关

	// version pool相关
	uint getActvieVerPool();
	bool switchActiveVerPool(u8 newActiveId, TrxId currTrxId);

	void lockCFMutex();
	void unlockCFMutex();
	
	void writeBeginReclaimPool(u8 verPoolNum);
	void writeLastReclaimedPool(u8 verpoolNum);


	u8		getVersionPoolCnt() const {return m_verpoolHeader.m_verpoolCnt;}
	u8		getActiveVerPoolForSwitch() const {return m_verpoolHeader.m_activeVerpool;}
	u8      getReclaimedVerPool() const {return m_verpoolHeader.m_reclaimedVerpool;}
	TrxId   getVerPoolMaxTrxId(u8 poolIdx) const {return m_verpoolHeader.m_vpInfo[poolIdx].m_maxTrxId;}
	VerpoolStat getVerPoolStatus(u8 poolIdx) const {return m_verpoolHeader.m_vpInfo[poolIdx].m_stat;}
	LsnType	getDumpLsn();
	bool	getCleanClosed() const {return m_cleanClosed;}

	void	setDumpLsn(LsnType dumpLsn) {
		m_dumpHeader.m_dumpLSN = dumpLsn;
	}

	TrxId   getMaxTrxId() {return m_maxTrxId;}
	void    setMaxTrxId(TrxId maxTrxId) {m_maxTrxId = maxTrxId;}
	void    updateFile();
	byte *  dupContent(u32 *size);
private:
	TNTControlFile(File *file);
	void init();
	static u64 checksum(const byte *buf, size_t size);
	void check() throw(NtseException);
	void freeMem();
	string serialize();
	static void getLineWithCheck(stringstream &ss, bool eol, char delim, char *buf, size_t bufSize, const char *expected) throw(NtseException);

	static const int MAX_PATH_LENGTH = 1024;	/** 文件路径最大长度 */

private:
	TNTCFDumpHeader		m_dumpHeader;	// TNT控制文件中的dump信息
	TNTCFVerpoolHeader	m_verpoolHeader;// TNT控制文件中的version pool信息
	// 系统相关
	TrxId       m_maxTrxId;
	bool		m_cleanClosed;		// 上次数据库是否正常关闭
	bool		m_closed;			// 是否被关闭
	byte		*m_fileBuf;			// 内存中保存控制文件内容，方便计算校验和
	u32			m_bufSize;			// m_fileBuf的大小
	File		*m_file;			// 控制文件
	Mutex		m_lock;				// 保护并发的锁
	Syslog		*m_syslog;			// TNT系统日志
};

/**
* TNT引擎配置文件
* 包含NTSE配置文件
* 
*/
struct TNTConfig {
public:
	Config	m_ntseConfig;		// NTSE引擎的Config文件
	// TNT引擎特有的属性项
	char	*m_tntBasedir;		// TNT日志文件的路径
	char	*m_dumpdir;			// TNT dump文件路径
	uint    m_dumpReserveCnt;   // TNT dump目录最多文件夹保留个数
	uint	m_tntBufSize;		// TNT需要分配的内存大小：NTSE_PAGES
	uint	m_txnLockTimeout;	// 事务锁，等待超时时间设置：seconds
	uint	m_maxTrxCnt;		// 事务系统中总共的事务数目
	bool	m_backupBeforeRec;	//
	bool	m_verifyAfterRec;	
	ErrLevel m_tntLogLevel;		// 
	TrxFlushMode m_trxFlushMode;
	
	// TNT purge操作可设置属性
	uint	m_purgeThreshold;	// 内存利用率达到多少之后(0,100)，需要purge：TNT_Buf_Size的百分比
	uint	m_purgeEnough;		// Purge回收多少内存pages之后，可以认为purge成功：TNT_Buf_Size的百分比
	bool	m_autoPurge;		// TNT引擎是否允许进行auto purge，在系统空闲时
	uint	m_purgeInterval;	// 两次purge操作之间的时间间隔：Seconds
	bool	m_purgeBeforeClose;	// TNT引擎关闭前，是否需要进行最后一次purge；默认不需要
	bool    m_purgeAfterRecover;// recover后是否进行purge；默认需要

	// TNT dump操作可设置属性
	uint	m_dumpInterval;		// 两次dump操作之间的时间间隔：Seconds
	uint	m_dumponRedoSize;	// 上次dump以来，写入多少tnt redo日志，需要dump：bytes
	bool	m_autoDump;			// TNT引擎是否允许进行auto dump，在系统空闲时
	bool	m_dumpBeforeClose;	// TNT引擎关闭前，是否需要进行最后一次dump；默认需要

	// TNT version pool相关属性
	uint	m_verpoolFileSize;	// 版本池单个文件大小：bytes
	u8		m_verpoolCnt;		// 版本池数量

	u32     m_defragHashIndexMaxTraverseCnt; //defrag hash需要遍历的最大个数
	u32		m_openTableCnt;		// 维护打开表的数目

	u32		m_maxTrxRunTime;	// 事务最长执行时间
	u32		m_maxTrxLocks;		// 事务最多加锁数目
	u32		m_maxTrxCommonIdleTime;		// 普通状态事务最多idle时间
	u32		m_maxTrxPrepareIdleTime;	// prepare状态下事务最多idle的时间

	u32		m_reclaimMemIndexHwm;		// 内存索引回收高水位线
	u32		m_reclaimMemIndexLwm;		// 内存索引回收低水位线

public:
	/**
	 * 构造函数，设定各参数的默认值，适合单元测试时用
	 */
	TNTConfig() {
		m_tntBasedir		= System::strdup(".");
		m_dumpdir			= System::strdup(".");
		m_dumpReserveCnt    = 10;
		m_tntBufSize		= 1024;
		m_txnLockTimeout	= 5000;					// TNT 事务锁超时时间，5s

		m_maxTrxCnt		= 512;

		m_backupBeforeRec	= false;
		m_verifyAfterRec	= false;
		m_tntLogLevel		= EL_WARN;
		m_trxFlushMode      = TFM_FLUSH_SYNC;

		m_purgeThreshold	= 80;
		m_purgeEnough		= 50;
		m_autoPurge			= true;
		m_purgeInterval		= 10;
		m_purgeBeforeClose	= false;
		m_purgeAfterRecover = false;

		m_dumpInterval		= 1;
		m_dumponRedoSize	= 100 * 1024 * 1024;
		m_autoDump			= true;
		m_dumpBeforeClose	= false;

		m_verpoolFileSize	= 100 * 1024 * 1024;
		m_verpoolCnt		= 2;

		m_defragHashIndexMaxTraverseCnt = 32*1000;
		m_openTableCnt		= 128;

		m_maxTrxRunTime		= 3600;
		m_maxTrxLocks		= 200000;
		m_maxTrxCommonIdleTime	= 600;
		m_maxTrxPrepareIdleTime = 1200;

		m_reclaimMemIndexHwm = 1000;
		m_reclaimMemIndexLwm = 800;
	}
	/**
	 * 拷贝构造函数
	 */
	TNTConfig(const TNTConfig *cfg) {
		memcpy(this, cfg, sizeof(TNTConfig));
		memset(&m_ntseConfig, 0, sizeof(Config));
		m_tntBasedir = System::strdup(cfg->m_tntBasedir);
		m_dumpdir	 = System::strdup(cfg->m_dumpdir);
		m_ntseConfig = cfg->m_ntseConfig;
	}

	TNTConfig& operator = (const TNTConfig &cfg) {
		delete []m_tntBasedir;
		delete []m_dumpdir;
		memcpy(this, &cfg, sizeof(TNTConfig));
		memset(&m_ntseConfig, 0, sizeof(Config));
		m_tntBasedir = System::strdup(cfg.m_tntBasedir);
		m_dumpdir	 = System::strdup(cfg.m_dumpdir);
		m_ntseConfig = cfg.m_ntseConfig;
		return *this;
	}

	/** 析构函数 */
	~TNTConfig() {
		delete []m_tntBasedir;
		delete []m_dumpdir;
	}
	
	void setTntBasedir(const char *basedir) {
		delete []m_tntBasedir;
		m_tntBasedir = System::strdup(basedir);
	}

	void setTntDumpdir(const char *dumpdir) {
		delete []m_dumpdir;
		m_dumpdir = System::strdup(dumpdir);
	}

	void setTntDumpReserveCnt(uint dumpReserveCnt) {
		m_dumpReserveCnt = dumpReserveCnt;
	}

	void setNtseBasedir(const char *basedir) {
		m_ntseConfig.setBasedir(basedir);
	}

	void setNtseTmpdir(const char *tmpdir) {
		m_ntseConfig.setTmpdir(tmpdir);
	}

	void setTxnLogdir(const char *txnLogDir) {
		m_ntseConfig.setLogdir(txnLogDir);
	}

	void setOpenTableCnt(const u32 tableOpenCnt) {
		m_openTableCnt = tableOpenCnt;
	}

	void setDumpInterval(const u32 dumpInterval) {
		m_dumpInterval = dumpInterval;
	}

	/** 序列化数据库配置内容
	 * @param size OUT，序列化结果大小
	 * @return 序列化结果，使用new分配
	 */
	byte* write(size_t *size) {
		size_t maxSize = sizeof(*this) + strlen(m_tntBasedir) + strlen(m_ntseConfig.m_basedir) + strlen(m_ntseConfig.m_tmpdir) + strlen(m_ntseConfig.m_logdir) + 10;
		byte *r = new byte[maxSize];
		Stream s(r, maxSize);
		try {		
			// 序列化NTSE Config
			s.write(m_ntseConfig.m_basedir);
			s.write(m_ntseConfig.m_tmpdir);
			s.write(m_ntseConfig.m_logdir);
			s.write(m_ntseConfig.m_logFileSize);
			s.write(m_ntseConfig.m_logFileCntHwm);
			s.write(m_ntseConfig.m_logBufSize);
			s.write(m_ntseConfig.m_pageBufSize);
			s.write(m_ntseConfig.m_mmsSize);
			s.write(m_ntseConfig.m_commonPoolSize);
			s.write(m_ntseConfig.m_maxSessions);
			s.write(m_ntseConfig.m_internalSessions);
			s.write(m_ntseConfig.m_tlTimeout);
			s.write(m_ntseConfig.m_maxFlushPagesInScavenger);
			s.write(m_ntseConfig.m_directIo);
			s.write(m_ntseConfig.m_aio);
			s.write(m_ntseConfig.m_backupBeforeRecover);
			s.write(m_ntseConfig.m_verifyAfterRecover);
			s.write(m_ntseConfig.m_enableMmsCacheUpdate);
			s.write(m_ntseConfig.m_systemIoCapacity);
			s.write(m_ntseConfig.m_flushAdjustLoop);
			s.write(m_ntseConfig.m_maxDirtyPagePct);
			s.write(m_ntseConfig.m_maxDirtyPagePctLwm);
			s.write((int)m_ntseConfig.m_logLevel);
			s.write(m_ntseConfig.m_localConfigs.m_accurateTblScan);
			s.write(m_ntseConfig.m_localConfigs.m_smplStrategy);
			s.write(m_ntseConfig.m_localConfigs.m_tblSmplPct);
			s.write(m_ntseConfig.m_localConfigs.m_tblSmplWinSize);
			s.write(m_ntseConfig.m_localConfigs.m_tblSmplWinDetectTimes);
			s.write(m_ntseConfig.m_localConfigs.m_tblSmplWinMemLevel);			
			s.write(m_ntseConfig.m_localConfigs.m_smplTrieCte);
			s.write(m_ntseConfig.m_localConfigs.m_smplTrieBatchDelSize);
			// 序列化TNT Config
			s.write(m_tntBasedir);
			s.write(m_dumpdir);
			s.write(m_dumpReserveCnt);
			s.write(m_tntBufSize);
			s.write(m_txnLockTimeout);
			s.write(m_maxTrxCnt);
			s.write(m_backupBeforeRec);
			s.write(m_verifyAfterRec);
			s.write((int)m_tntLogLevel);
			s.write((int)m_trxFlushMode);
			s.write(m_purgeThreshold);
			s.write(m_purgeEnough);
			s.write(m_autoPurge);
			s.write(m_purgeInterval);
			s.write(m_purgeBeforeClose);
			s.write(m_purgeAfterRecover);
			s.write(m_dumpInterval);
			s.write(m_dumponRedoSize);
			s.write(m_autoDump);
			s.write(m_dumpBeforeClose);
			s.write(m_verpoolFileSize);
			s.write(m_verpoolCnt);
			s.write(m_defragHashIndexMaxTraverseCnt);
			s.write(m_openTableCnt);
			s.write(m_maxTrxRunTime);
			s.write(m_maxTrxLocks);
			s.write(m_maxTrxCommonIdleTime);
			s.write(m_maxTrxPrepareIdleTime);
			s.write(m_reclaimMemIndexHwm);
			s.write(m_reclaimMemIndexLwm);
		} catch (NtseException &) { assert(false); }

		*size = s.getSize();
		return r;
	}

	/** 反序列化数据库配置内容
	 * @param buf 数据库配置序列化结果
	 * @param size 数据库配置序列化结果大小
	 * @return 数据库配置，使用new分配
	 */
	static TNTConfig* read(const byte *buf, size_t size) {
		TNTConfig *r = new TNTConfig();
		Stream s((byte *)buf, size);
		try {
			// 设置NTSE Config
			char *str = s.readString();
			r->m_ntseConfig.setBasedir(str);
			delete []str;
			str = s.readString();
			r->m_ntseConfig.setTmpdir(str);
			delete []str;
			str = s.readString();
			r->m_ntseConfig.setLogdir(str);
			delete []str;
			s.read(&r->m_ntseConfig.m_logFileSize);
			s.read(&r->m_ntseConfig.m_logFileCntHwm);
			s.read(&r->m_ntseConfig.m_logBufSize);
			s.read(&r->m_ntseConfig.m_pageBufSize);
			s.read(&r->m_ntseConfig.m_mmsSize);
			s.read(&r->m_ntseConfig.m_commonPoolSize);
			s.read(&r->m_ntseConfig.m_maxSessions);
			s.read(&r->m_ntseConfig.m_internalSessions);
			s.read(&r->m_ntseConfig.m_tlTimeout);
			s.read(&r->m_ntseConfig.m_maxFlushPagesInScavenger);
			s.read(&r->m_ntseConfig.m_directIo);
			s.read(&r->m_ntseConfig.m_aio);
			s.read(&r->m_ntseConfig.m_backupBeforeRecover);
			s.read(&r->m_ntseConfig.m_verifyAfterRecover);
			s.read(&r->m_ntseConfig.m_enableMmsCacheUpdate);
			s.read(&r->m_ntseConfig.m_systemIoCapacity);
			s.read(&r->m_ntseConfig.m_flushAdjustLoop);
			s.read(&r->m_ntseConfig.m_maxDirtyPagePct);
			s.read(&r->m_ntseConfig.m_maxDirtyPagePctLwm);
			int logLevel;
			s.read(&logLevel);
			r->m_ntseConfig.m_logLevel = (ErrLevel)logLevel;
			s.read(&r->m_ntseConfig.m_localConfigs.m_accurateTblScan);
			str = s.readString();
			r->m_ntseConfig.m_localConfigs.setCompressSampleStrategy(str);
			delete []str;
			s.read(&r->m_ntseConfig.m_localConfigs.m_tblSmplPct);
			s.read(&r->m_ntseConfig.m_localConfigs.m_tblSmplWinSize);
			s.read(&r->m_ntseConfig.m_localConfigs.m_tblSmplWinDetectTimes);
			s.read(&r->m_ntseConfig.m_localConfigs.m_tblSmplWinMemLevel);
			s.read(&r->m_ntseConfig.m_localConfigs.m_smplTrieCte);
			s.read(&r->m_ntseConfig.m_localConfigs.m_smplTrieBatchDelSize);
			// 设置TNT Config
			str = s.readString();
			r->setTntBasedir(str);
			delete []str;
			str = s.readString();
			r->setTntDumpdir(str);
			delete []str;
			s.read(&r->m_dumpReserveCnt);
			s.read(&r->m_tntBufSize);
			s.read(&r->m_txnLockTimeout);
			s.read(&r->m_maxTrxCnt);
			s.read(&r->m_backupBeforeRec);
			s.read(&r->m_verifyAfterRec);
			s.read(&logLevel);
			r->m_tntLogLevel = (ErrLevel)logLevel;
			int trxFlushMode;
			s.read(&trxFlushMode);
			r->m_trxFlushMode = (TrxFlushMode)trxFlushMode;
			s.read(&r->m_purgeThreshold);
			s.read(&r->m_purgeEnough);
			s.read(&r->m_autoPurge);
			s.read(&r->m_purgeInterval);
			s.read(&r->m_purgeBeforeClose);
			s.read(&r->m_purgeAfterRecover);
			s.read(&r->m_dumpInterval);
			s.read(&r->m_dumponRedoSize);
			s.read(&r->m_autoDump);
			s.read(&r->m_dumpBeforeClose);
			s.read(&r->m_verpoolFileSize);
			s.read(&r->m_verpoolCnt);
			s.read(&r->m_defragHashIndexMaxTraverseCnt);
			s.read(&r->m_openTableCnt);
			s.read(&r->m_maxTrxRunTime);
			s.read(&r->m_maxTrxLocks);
			s.read(&r->m_maxTrxCommonIdleTime);
			s.read(&r->m_maxTrxPrepareIdleTime);
			s.read(&r->m_reclaimMemIndexHwm);
			s.read(&r->m_reclaimMemIndexLwm);
		} catch (NtseException &) { assert(false); }
		return r;
	}

	bool isEqual(TNTConfig *tntConfig) {
		if (strcmp(m_tntBasedir, tntConfig->m_tntBasedir) != 0) return false;
		if (strcmp(m_dumpdir, tntConfig->m_dumpdir) != 0) return false;
		if (m_dumpReserveCnt != tntConfig->m_dumpReserveCnt) return false;
		if (m_tntBufSize != tntConfig->m_tntBufSize) return false;
		if (m_txnLockTimeout != tntConfig->m_txnLockTimeout) return false;
		if (m_maxTrxCnt != tntConfig->m_maxTrxCnt) return false;
		if (m_backupBeforeRec != tntConfig->m_backupBeforeRec) return false;
		if (m_verifyAfterRec != tntConfig->m_verifyAfterRec) return false;
		if (m_tntLogLevel != tntConfig->m_tntLogLevel) return false;
		if (m_trxFlushMode != tntConfig->m_trxFlushMode) return false;
		if (m_purgeThreshold != tntConfig->m_purgeThreshold) return false;
		if (m_purgeEnough != tntConfig->m_purgeEnough) return false;
		if (m_autoPurge != tntConfig->m_autoPurge) return false;
		if (m_purgeInterval != tntConfig->m_purgeInterval) return false;
		if (m_purgeBeforeClose != tntConfig->m_purgeBeforeClose) return false;
		if (m_purgeAfterRecover != tntConfig->m_purgeAfterRecover) return false;
		if (m_dumpInterval != tntConfig->m_dumpInterval) return false;
		if (m_dumponRedoSize != tntConfig->m_dumponRedoSize) return false;
		if (m_autoDump != tntConfig->m_autoDump) return false;
		if (m_dumpBeforeClose != tntConfig->m_dumpBeforeClose) return false;
		if (m_verpoolFileSize != tntConfig->m_verpoolFileSize) return false;
		if (m_verpoolCnt != tntConfig->m_verpoolCnt) return false;
		if (m_defragHashIndexMaxTraverseCnt != tntConfig->m_defragHashIndexMaxTraverseCnt) return false;
		if (m_openTableCnt != tntConfig->m_openTableCnt) return false;
		if (m_maxTrxRunTime != tntConfig->m_maxTrxRunTime) return false;
		if (m_maxTrxLocks != tntConfig->m_maxTrxLocks) return false;
		if (m_maxTrxCommonIdleTime != tntConfig->m_maxTrxCommonIdleTime) return false;
		if (m_maxTrxPrepareIdleTime != tntConfig->m_maxTrxPrepareIdleTime) return false;
		if (m_reclaimMemIndexHwm != tntConfig->m_reclaimMemIndexHwm) return false;
		if (m_reclaimMemIndexLwm != tntConfig->m_reclaimMemIndexLwm) return false;

		return m_ntseConfig.isEqual(&tntConfig->m_ntseConfig);
	}
};
}

#endif