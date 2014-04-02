/**
 * 数据库配置
 *
 * @author 汪源(wangyuan@corp.netease.com, wy@163.org)
 */

#ifndef _NTSE_CONFIG_H_
#define _NTSE_CONFIG_H_

#include "misc/Global.h"
#include "misc/Syslog.h"
#include "util/Stream.h"

namespace ntse {

/** NTSE连接私有配置 */
struct LocalConfig {
public:
	/** 默认构造函数 */
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
	 * 拷贝构造函数
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
	/** 表扫描时保证结果精确性，若设为false扫描时将跳过MMS，并且对于链接记录
	 * 在遇到链接目标时返回记录，可能大幅提高性能
	 */
	bool	m_accurateTblScan;
	char    *m_smplStrategy;        /** 压缩采样方法 */
	u8      m_tblSmplPct;           /** 压缩采样比例 */
	uint    m_tblSmplWinSize;       /** 采样滑动窗口大小，单位：字节 */
	u8      m_tblSmplWinDetectTimes;/** 采样滑动窗口内匹配项探测次数,为0表示不限制探测次数 */
	u8      m_tblSmplWinMemLevel;   /** 采样滑动窗口哈希表内存使用水平 */
	u8      m_smplTrieCte;          /** 采样Trie树膨胀系数 */
	uint    m_smplTrieBatchDelSize; /** 采样Trie树批删除叶子节点数目 */
};

/** NTSE全局配置参数 */
struct Config {
public:
	char	*m_basedir;		/** NTSE的日志文件、日志等所在目录 */
	char	*m_tmpdir;		/** 存储临时文件（如建索引排序时）的目录 */
	char	*m_logdir;		/** NTSE日志文件所在目录 */
	uint	m_logFileSize;	/** 事务日志文件大小，单位字节 */
	uint	m_logFileCntHwm;/** 日志文件个数高水线 */
	uint	m_logBufSize;	/** 事务日志缓存大小，单位字节 */
	uint	m_pageBufSize;	/** 页面缓存大小，单位页面数 */
	uint	m_mmsSize;		/** MMS缓存大小，单位页面数 */
	uint    m_commonPoolSize;/** 通用内存池大小，单位页面数 */
	u16		m_maxSessions;	/** 最多并发会话数 */
	u16		m_internalSessions;	/** 内部会话数限制 */
	uint	m_tlTimeout;	/** 表锁超时时间，单位秒 */
	uint	m_maxFlushPagesInScavenger;	/** 一次Scavenger操作，最多Flush的脏页数量 */
	bool	m_directIo;		/** 文件操作是否使用directIo */
	bool	m_aio;			/** 检查点和清道夫操作是否用异步IO*/
	bool	m_backupBeforeRecover;	/** 恢复之前是否备份数据库 */
	bool	m_verifyAfterRecover;	/** 恢复后是否验证数据正确性 */
	bool	m_enableMmsCacheUpdate;	/** 启用MMS缓存更新 */
	uint	m_systemIoCapacity;		/** 系统最大的io能力 */
	uint	m_flushAdjustLoop;		/** 刷脏页线程统计平均脏页量的周期 */
	uint	m_maxDirtyPagePct;		/** 系统buffer中的最大脏页比率单位是% */
	uint	m_maxDirtyPagePctLwm;	/** 系统buffer中的最大脏页比率低水位线值,单位是% */
	ErrLevel	m_logLevel;	/** 输出日志级别 */
	bool		m_printToStdout;	/** 日志是否输出到标准输出 */
	LocalConfig	m_localConfigs;	/** 可以为每个连接单独设置的配置 */
public:
	/**
	 * 构造函数，设定各参数的默认值，适合单元测试时用
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
	 * 拷贝构造函数
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

	/** 析构函数 */
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

	/** 序列化数据库配置内容
	 * @param size OUT，序列化结果大小
	 * @return 序列化结果，使用new分配
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

	/** 反序列化数据库配置内容
	 * @param buf 数据库配置序列化结果
	 * @param size 数据库配置序列化结果大小
	 * @return 数据库配置，使用new分配
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
