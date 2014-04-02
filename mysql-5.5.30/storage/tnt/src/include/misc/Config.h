/**
 * ���ݿ�����
 *
 * @author ��Դ(wangyuan@corp.netease.com, wy@163.org)
 */

#ifndef _NTSE_CONFIG_H_
#define _NTSE_CONFIG_H_

#include "misc/Global.h"
#include "misc/Syslog.h"
#include "util/Stream.h"

namespace ntse {

/** NTSE����˽������ */
struct LocalConfig {
public:
	/** Ĭ�Ϲ��캯�� */
	LocalConfig() {
		m_smplStrategy = System::strdup("sequence");
		m_accurateTblScan = true;
		m_tblSmplWinSize = 256 * 1024;
		m_tblSmplWinDetectTimes = 2;
		m_tblSmplWinMemLevel = 12;
		m_tblSmplPct = 100;
		m_smplTrieCte = 8;
		m_smplTrieBatchDelSize = 32 * 1024;
	}

	/**
	 * �������캯��
	 */
	LocalConfig(const LocalConfig &cfg) {
		memcpy(this, &cfg, sizeof(LocalConfig));
		m_smplStrategy = System::strdup(cfg.m_smplStrategy);
	}

	LocalConfig& operator = (const LocalConfig &cfg) {
		delete [] m_smplStrategy;
		memcpy(this, &cfg, sizeof(LocalConfig));
		m_smplStrategy = System::strdup(cfg.m_smplStrategy);
		return *this;
	}
		
	~LocalConfig() {
		delete [] m_smplStrategy;
	}

	void setCompressSampleStrategy(const char *strategy) {
		delete [] m_smplStrategy;
		m_smplStrategy = System::strdup(strategy);
	}

	bool isEqual(LocalConfig *localConfig) {
		if (strcmp(m_smplStrategy, localConfig->m_smplStrategy) != 0) return false;
		if (m_accurateTblScan != localConfig->m_accurateTblScan) return false;
		if (m_tblSmplWinSize != localConfig->m_tblSmplWinSize) return false;
		if (m_tblSmplWinDetectTimes != localConfig->m_tblSmplWinDetectTimes) return false;
		if (m_tblSmplWinMemLevel != localConfig->m_tblSmplWinMemLevel) return false;
		if (m_tblSmplPct != localConfig->m_tblSmplPct) return false;
		if (m_smplTrieCte != localConfig->m_smplTrieCte) return false;
		if (m_smplTrieBatchDelSize != localConfig->m_smplTrieBatchDelSize) return false;

		return true;
	}

public:
	/** ��ɨ��ʱ��֤�����ȷ�ԣ�����Ϊfalseɨ��ʱ������MMS�����Ҷ������Ӽ�¼
	 * ����������Ŀ��ʱ���ؼ�¼�����ܴ���������
	 */
	bool	m_accurateTblScan;
	char    *m_smplStrategy;        /** ѹ���������� */
	u8      m_tblSmplPct;           /** ѹ���������� */
	uint    m_tblSmplWinSize;       /** �����������ڴ�С����λ���ֽ� */
	u8      m_tblSmplWinDetectTimes;/** ��������������ƥ����̽�����,Ϊ0��ʾ������̽����� */
	u8      m_tblSmplWinMemLevel;   /** �����������ڹ�ϣ���ڴ�ʹ��ˮƽ */
	u8      m_smplTrieCte;          /** ����Trie������ϵ�� */
	uint    m_smplTrieBatchDelSize; /** ����Trie����ɾ��Ҷ�ӽڵ���Ŀ */
};

/** NTSEȫ�����ò��� */
struct Config {
public:
	char	*m_basedir;		/** NTSE����־�ļ�����־������Ŀ¼ */
	char	*m_tmpdir;		/** �洢��ʱ�ļ����罨��������ʱ����Ŀ¼ */
	char	*m_logdir;		/** NTSE��־�ļ�����Ŀ¼ */
	uint	m_logFileSize;	/** ������־�ļ���С����λ�ֽ� */
	uint	m_logFileCntHwm;/** ��־�ļ�������ˮ�� */
	uint	m_logBufSize;	/** ������־�����С����λ�ֽ� */
	uint	m_pageBufSize;	/** ҳ�滺���С����λҳ���� */
	uint	m_mmsSize;		/** MMS�����С����λҳ���� */
	uint    m_commonPoolSize;/** ͨ���ڴ�ش�С����λҳ���� */
	u16		m_maxSessions;	/** ��ಢ���Ự�� */
	u16		m_internalSessions;	/** �ڲ��Ự������ */
	uint	m_tlTimeout;	/** ������ʱʱ�䣬��λ�� */
	uint	m_maxFlushPagesInScavenger;	/** һ��Scavenger���������Flush����ҳ���� */
	bool	m_directIo;		/** �ļ������Ƿ�ʹ��directIo */
	bool	m_aio;			/** ��������������Ƿ����첽IO*/
	bool	m_backupBeforeRecover;	/** �ָ�֮ǰ�Ƿ񱸷����ݿ� */
	bool	m_verifyAfterRecover;	/** �ָ����Ƿ���֤������ȷ�� */
	bool	m_enableMmsCacheUpdate;	/** ����MMS������� */
	uint	m_systemIoCapacity;		/** ϵͳ����io���� */
	uint	m_flushAdjustLoop;		/** ˢ��ҳ�߳�ͳ��ƽ����ҳ�������� */
	uint	m_maxDirtyPagePct;		/** ϵͳbuffer�е������ҳ���ʵ�λ��% */
	uint	m_maxDirtyPagePctLwm;	/** ϵͳbuffer�е������ҳ���ʵ�ˮλ��ֵ,��λ��% */
	ErrLevel	m_logLevel;	/** �����־���� */
	bool		m_printToStdout;	/** ��־�Ƿ��������׼��� */
	LocalConfig	m_localConfigs;	/** ����Ϊÿ�����ӵ������õ����� */
public:
	/**
	 * ���캯�����趨��������Ĭ��ֵ���ʺϵ�Ԫ����ʱ��
	 */
	Config() {
		m_basedir = System::strdup(".");
		m_tmpdir = System::strdup(".");
		m_logdir = System::strdup(".");
		m_logFileSize = 1024 * 1024 * 8;
		m_logFileCntHwm = 3;
		m_logBufSize = 1024 * 1024;
		m_pageBufSize = 512;
		m_mmsSize = 512;
		m_commonPoolSize = 4096;
		m_internalSessions = 100;
		m_maxSessions = 255 + m_internalSessions;
		m_tlTimeout = 5;
		m_maxFlushPagesInScavenger = 200;
#ifdef NTSE_UNIT_TEST
		m_directIo = false;
		m_aio = false;
#else
		m_directIo = true;
		m_aio = true;
#endif
		m_backupBeforeRecover = false;
		m_verifyAfterRecover = false;
		m_enableMmsCacheUpdate = true;
		m_logLevel = EL_WARN;
		m_printToStdout = true;
		m_systemIoCapacity = 256;
		m_flushAdjustLoop = 30;
		m_maxDirtyPagePct = 75;
		m_maxDirtyPagePctLwm = 0;
	}
	/**
	 * �������캯��
	 */
	Config(const Config *cfg) {
		memcpy(this, cfg, sizeof(Config));
		memset(&m_localConfigs, 0, sizeof(LocalConfig));
		m_basedir = System::strdup(cfg->m_basedir);
		m_tmpdir = System::strdup(cfg->m_tmpdir);
		m_logdir = System::strdup(cfg->m_logdir);
		m_localConfigs = cfg->m_localConfigs;
	}

	Config& operator = (const Config &cfg) {
		delete [] m_basedir;
		delete [] m_tmpdir;
		delete [] m_logdir;
		memcpy(this, &cfg, sizeof(Config));
		memset(&m_localConfigs, 0, sizeof(LocalConfig));
		m_localConfigs.m_smplStrategy = NULL;
		m_basedir = System::strdup(cfg.m_basedir);
		m_tmpdir = System::strdup(cfg.m_tmpdir);
		m_logdir = System::strdup(cfg.m_logdir);
		m_localConfigs = cfg.m_localConfigs;
		return *this;
	}

	/** �������� */
	~Config() {
		delete []m_basedir;
		delete []m_tmpdir;
		delete []m_logdir;
	}

	void setBasedir(const char *basedir) {
		delete []m_basedir;
		m_basedir = System::strdup(basedir);
	}

	void setTmpdir(const char *tmpdir) {
		delete []m_tmpdir;
		m_tmpdir = System::strdup(tmpdir);
	}

	void setLogdir(const char *logdir) {
		delete []m_logdir;
		m_logdir = System::strdup(logdir);
	}

	/** ���л����ݿ���������
	 * @param size OUT�����л������С
	 * @return ���л������ʹ��new����
	 */
	byte* write(size_t *size) {
		size_t maxSize = sizeof(*this) + strlen(m_basedir) + strlen(m_tmpdir) + strlen(m_logdir) + 10;
		byte *r = new byte[maxSize];
		Stream s(r, maxSize);
		try {
			s.write(m_basedir);
			s.write(m_tmpdir);
			s.write(m_logdir);
			s.write(m_logFileSize);
			s.write(m_logFileCntHwm);
			s.write(m_logBufSize);
			s.write(m_pageBufSize);
			s.write(m_mmsSize);
			s.write(m_commonPoolSize);
			s.write(m_maxSessions);
			s.write(m_internalSessions);
			s.write(m_tlTimeout);
			s.write(m_maxFlushPagesInScavenger);
			s.write(m_directIo);
			s.write(m_aio);
			s.write(m_backupBeforeRecover);
			s.write(m_verifyAfterRecover);
			s.write(m_enableMmsCacheUpdate);
			s.write(m_systemIoCapacity);
			s.write(m_flushAdjustLoop);
			s.write(m_maxDirtyPagePct);
			s.write(m_maxDirtyPagePctLwm);
			s.write((int)m_logLevel);
			s.write(m_printToStdout);
			s.write(m_localConfigs.m_accurateTblScan);
			s.write(m_localConfigs.m_smplStrategy);
			s.write(m_localConfigs.m_tblSmplPct);
			s.write(m_localConfigs.m_tblSmplWinSize);
			s.write(m_localConfigs.m_tblSmplWinDetectTimes);
			s.write(m_localConfigs.m_tblSmplWinMemLevel);			
			s.write(m_localConfigs.m_smplTrieCte);
			s.write(m_localConfigs.m_smplTrieBatchDelSize);
		} catch (NtseException &) { assert(false); }

		*size = s.getSize();
		return r;
	}

	/** �����л����ݿ���������
	 * @param buf ���ݿ��������л����
	 * @param size ���ݿ��������л������С
	 * @return ���ݿ����ã�ʹ��new����
	 */
	static Config* read(const byte *buf, size_t size) {
		Config *r = new Config();
		Stream s((byte *)buf, size);
		try {
			char *str = s.readString();
			r->setBasedir(str);
			delete []str;
			str = s.readString();
			r->setTmpdir(str);
			delete []str;
			str = s.readString();
			r->setLogdir(str);
			delete []str;
			s.read(&r->m_logFileSize);
			s.read(&r->m_logFileCntHwm);
			s.read(&r->m_logBufSize);
			s.read(&r->m_pageBufSize);
			s.read(&r->m_mmsSize);
			s.read(&r->m_commonPoolSize);
			s.read(&r->m_maxSessions);
			s.read(&r->m_internalSessions);
			s.read(&r->m_tlTimeout);
			s.read(&r->m_maxFlushPagesInScavenger);
			s.read(&r->m_directIo);
			s.read(&r->m_aio);
			s.read(&r->m_backupBeforeRecover);
			s.read(&r->m_verifyAfterRecover);
			s.read(&r->m_enableMmsCacheUpdate);
			s.read(&r->m_systemIoCapacity);
			s.read(&r->m_flushAdjustLoop);
			s.read(&r->m_maxDirtyPagePct);
			s.read(&r->m_maxDirtyPagePctLwm);
			int logLevel;
			s.read(&logLevel);
			r->m_logLevel = (ErrLevel)logLevel;
			s.read(&r->m_printToStdout);
			s.read(&r->m_localConfigs.m_accurateTblScan);
			str = s.readString();
			r->m_localConfigs.setCompressSampleStrategy(str);
			delete []str;
			s.read(&r->m_localConfigs.m_tblSmplPct);
			s.read(&r->m_localConfigs.m_tblSmplWinSize);
			s.read(&r->m_localConfigs.m_tblSmplWinDetectTimes);
			s.read(&r->m_localConfigs.m_tblSmplWinMemLevel);
			s.read(&r->m_localConfigs.m_smplTrieCte);
			s.read(&r->m_localConfigs.m_smplTrieBatchDelSize);
		} catch (NtseException &) { assert(false); }
		return r;
	}

	bool isEqual(Config *config) {
		if (strcmp(m_basedir, config->m_basedir) != 0) return false;
		if (strcmp(m_tmpdir, config->m_tmpdir) != 0) return false;
		if (strcmp(m_logdir, config->m_logdir) != 0) return false;
		if (m_logFileSize != config->m_logFileSize) return false;
		if (m_logFileCntHwm != config->m_logFileCntHwm) return false;
		if (m_logBufSize != config->m_logBufSize) return false;
		if (m_pageBufSize != config->m_pageBufSize) return false;
		if (m_mmsSize != config->m_mmsSize) return false;
		if (m_commonPoolSize != config->m_commonPoolSize) return false;
		if (m_maxSessions != config->m_maxSessions) return false;
		if (m_internalSessions != config->m_internalSessions) return false;
		if (m_tlTimeout != config->m_tlTimeout) return false;
		if (m_maxFlushPagesInScavenger != config->m_maxFlushPagesInScavenger) return false;
		if (m_directIo != config->m_directIo) return false;
		if (m_aio != config->m_aio) return false;
		if (m_backupBeforeRecover != config->m_backupBeforeRecover) return false;
		if (m_verifyAfterRecover != config->m_verifyAfterRecover) return false;
		if (m_enableMmsCacheUpdate != config->m_enableMmsCacheUpdate) return false;
		if (m_logLevel != config->m_logLevel) return false;
		if (m_systemIoCapacity != config->m_systemIoCapacity) return false;
		if (m_flushAdjustLoop != config->m_flushAdjustLoop) return false;
		if (m_maxDirtyPagePct != config->m_maxDirtyPagePct) return false;
		if (m_maxDirtyPagePctLwm != config->m_maxDirtyPagePctLwm) return false;
		return m_localConfigs.isEqual(&config->m_localConfigs);
	}
};

}

#endif
