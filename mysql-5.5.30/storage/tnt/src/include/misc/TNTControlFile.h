/**
* TNT ControlFile and Config��
*
* @author �εǳ�
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
	// tntim dump���
	u32			m_cleanClosed;		// �Ƿ�ȫ�ر�
//	u16			m_dumpTables;		// �ϴα��ݵı�����
//	u64			m_dumpBegin;		// �ϴα��ݿ�ʼʱ��
//	u64			m_dumpEnd;			// �ϴα��ݽ���ʱ��
	LsnType		m_dumpLSN;			// ���ݿ�ʼ��LSN
//	uint		m_dumpSN;			// dump���к�,����һ��dump�������к�++
};

enum VerpoolStat {
	VP_FREE = 0,
	VP_ACTIVE,
	VP_USED,
	VP_RECLAIMING
};

struct TNTVerpoolInfo {
	TrxId			m_minTrxId;		// version pool��Ӧ����С����id
	TrxId			m_maxTrxId;		// version pool��Ӧ���������id
	VerpoolStat		m_stat;			// version pool��ǰ״̬
};

struct TNTCFVerpoolHeader {
	// �汾�����
	u8			m_verpoolCnt;		// �汾������
	u8			m_activeVerpool;	// ��Ծ�汾��
	u8			m_reclaimedVerpool;	// ��һ�γɹ����յİ汾��
	TNTVerpoolInfo	*m_vpInfo;		// ÿ��version pool�������Ϣ
};

class TNTControlFile {
public:
	static TNTControlFile* open(const char *path, Syslog *syslog) throw(NtseException);
	static void create(const char *path, TNTConfig *config, Syslog *syslog) throw(NtseException);
	void close(bool clean = true);
	// ϵͳ���

	// version pool���
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

	static const int MAX_PATH_LENGTH = 1024;	/** �ļ�·����󳤶� */

private:
	TNTCFDumpHeader		m_dumpHeader;	// TNT�����ļ��е�dump��Ϣ
	TNTCFVerpoolHeader	m_verpoolHeader;// TNT�����ļ��е�version pool��Ϣ
	// ϵͳ���
	TrxId       m_maxTrxId;
	bool		m_cleanClosed;		// �ϴ����ݿ��Ƿ������ر�
	bool		m_closed;			// �Ƿ񱻹ر�
	byte		*m_fileBuf;			// �ڴ��б�������ļ����ݣ��������У���
	u32			m_bufSize;			// m_fileBuf�Ĵ�С
	File		*m_file;			// �����ļ�
	Mutex		m_lock;				// ������������
	Syslog		*m_syslog;			// TNTϵͳ��־
};

/**
* TNT���������ļ�
* ����NTSE�����ļ�
* 
*/
struct TNTConfig {
public:
	Config	m_ntseConfig;		// NTSE�����Config�ļ�
	// TNT�������е�������
	char	*m_tntBasedir;		// TNT��־�ļ���·��
	char	*m_dumpdir;			// TNT dump�ļ�·��
	uint    m_dumpReserveCnt;   // TNT dumpĿ¼����ļ��б�������
	uint	m_tntBufSize;		// TNT��Ҫ������ڴ��С��NTSE_PAGES
	uint	m_txnLockTimeout;	// ���������ȴ���ʱʱ�����ã�seconds
	uint	m_maxTrxCnt;		// ����ϵͳ���ܹ���������Ŀ
	bool	m_backupBeforeRec;	//
	bool	m_verifyAfterRec;	
	ErrLevel m_tntLogLevel;		// 
	TrxFlushMode m_trxFlushMode;
	
	// TNT purge��������������
	uint	m_purgeThreshold;	// �ڴ������ʴﵽ����֮��(0,100)����Ҫpurge��TNT_Buf_Size�İٷֱ�
	uint	m_purgeEnough;		// Purge���ն����ڴ�pages֮�󣬿�����Ϊpurge�ɹ���TNT_Buf_Size�İٷֱ�
	bool	m_autoPurge;		// TNT�����Ƿ��������auto purge����ϵͳ����ʱ
	uint	m_purgeInterval;	// ����purge����֮���ʱ������Seconds
	bool	m_purgeBeforeClose;	// TNT����ر�ǰ���Ƿ���Ҫ�������һ��purge��Ĭ�ϲ���Ҫ
	bool    m_purgeAfterRecover;// recover���Ƿ����purge��Ĭ����Ҫ

	// TNT dump��������������
	uint	m_dumpInterval;		// ����dump����֮���ʱ������Seconds
	uint	m_dumponRedoSize;	// �ϴ�dump������д�����tnt redo��־����Ҫdump��bytes
	bool	m_autoDump;			// TNT�����Ƿ��������auto dump����ϵͳ����ʱ
	bool	m_dumpBeforeClose;	// TNT����ر�ǰ���Ƿ���Ҫ�������һ��dump��Ĭ����Ҫ

	// TNT version pool�������
	uint	m_verpoolFileSize;	// �汾�ص����ļ���С��bytes
	u8		m_verpoolCnt;		// �汾������

	u32     m_defragHashIndexMaxTraverseCnt; //defrag hash��Ҫ������������
	u32		m_openTableCnt;		// ά���򿪱����Ŀ

	u32		m_maxTrxRunTime;	// �����ִ��ʱ��
	u32		m_maxTrxLocks;		// ������������Ŀ
	u32		m_maxTrxCommonIdleTime;		// ��ͨ״̬�������idleʱ��
	u32		m_maxTrxPrepareIdleTime;	// prepare״̬���������idle��ʱ��

	u32		m_reclaimMemIndexHwm;		// �ڴ��������ո�ˮλ��
	u32		m_reclaimMemIndexLwm;		// �ڴ��������յ�ˮλ��

public:
	/**
	 * ���캯�����趨��������Ĭ��ֵ���ʺϵ�Ԫ����ʱ��
	 */
	TNTConfig() {
		m_tntBasedir		= System::strdup(".");
		m_dumpdir			= System::strdup(".");
		m_dumpReserveCnt    = 10;
		m_tntBufSize		= 1024;
		m_txnLockTimeout	= 5000;					// TNT ��������ʱʱ�䣬5s

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
	 * �������캯��
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

	/** �������� */
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

	/** ���л����ݿ���������
	 * @param size OUT�����л������С
	 * @return ���л������ʹ��new����
	 */
	byte* write(size_t *size) {
		size_t maxSize = sizeof(*this) + strlen(m_tntBasedir) + strlen(m_ntseConfig.m_basedir) + strlen(m_ntseConfig.m_tmpdir) + strlen(m_ntseConfig.m_logdir) + 10;
		byte *r = new byte[maxSize];
		Stream s(r, maxSize);
		try {		
			// ���л�NTSE Config
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
			// ���л�TNT Config
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

	/** �����л����ݿ���������
	 * @param buf ���ݿ��������л����
	 * @param size ���ݿ��������л������С
	 * @return ���ݿ����ã�ʹ��new����
	 */
	static TNTConfig* read(const byte *buf, size_t size) {
		TNTConfig *r = new TNTConfig();
		Stream s((byte *)buf, size);
		try {
			// ����NTSE Config
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
			// ����TNT Config
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