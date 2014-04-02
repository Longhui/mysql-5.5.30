/**
 * �������ݿ������ļ�
 * 
 * @author ������(yulihua@corp.netease.com, ylh@163.org)
 */
#ifndef _NTSETEST_DB_CONFIG_H_
#define _NTSETEST_DB_CONFIG_H_

#include "api/Database.h"

#ifdef WIN32
#define PERF_SMALL_CONFIG
#endif

using namespace ntse;
/** ���ݿⳣ������ */
class CommonDbConfig {
public:
	/** ��ȡС�����ݿ����ò��� */
	static const Config* getSmall() {
		return &m_inst.m_small;
	}
	/** ��ȡ�������ݿ����ò��� */
	static const Config* getMedium() {
		return &m_inst.m_medium;
	}
	/** ���ݿ�Ŀ¼ */
	static const char *getBasedir() {
		return m_dbdir;
	}

private:
	CommonDbConfig() {
		delete [] m_small.m_basedir;
		delete [] m_small.m_tmpdir;
		m_small.m_basedir = System::strdup(m_dbdir);
		m_small.m_tmpdir = System::strdup(m_dbdir);
		delete [] m_medium.m_basedir;
		delete [] m_medium.m_tmpdir;
		m_medium.m_basedir = System::strdup(m_dbdir);
		m_medium.m_tmpdir = System::strdup(m_dbdir);

#ifdef PERF_SMALL_CONFIG
		//small
		m_small.m_logBufSize = 1 * 1024 * 1024;
		m_small.m_logFileSize = 10 * 1024 * 1024;
		m_small.m_pageBufSize = 2 * 1024 * 1024 / Limits::PAGE_SIZE;
		m_small.m_mmsSize = 2 * 1024 * 1024 / Limits::PAGE_SIZE;
		m_small.m_maxSessions = 512;
		// medium
		m_medium.m_logBufSize = 1 * 1024 * 1024;
		m_medium.m_logFileSize = 10 * 1024 * 1024;
		m_medium.m_pageBufSize = 20 * 1024 * 1024 / Limits::PAGE_SIZE;
		m_medium.m_mmsSize = 20 * 1024 * 1024 / Limits::PAGE_SIZE;
		m_medium.m_maxSessions = 512;
#else
		//small
		m_small.m_logBufSize = 1 * 1024 * 1024;
		m_small.m_logFileSize = 20 * 1024 * 1024;
		m_small.m_pageBufSize = 20 * 1024 * 1024 / Limits::PAGE_SIZE;
		m_small.m_mmsSize = 2 * 1024 * 1024 / Limits::PAGE_SIZE;
		m_small.m_maxSessions = 512;
		// medium
		m_medium.m_logBufSize = 10 * 1024 * 1024;
		m_medium.m_logFileSize = 128 * 1024 * 1024;
		m_medium.m_pageBufSize = 200 * 1024 * 1024  / Limits::PAGE_SIZE;
		m_medium.m_mmsSize = 200 * 1024 * 1024  / Limits::PAGE_SIZE;
		m_medium.m_maxSessions = 512;
#endif
	}

private:
	Config m_small;
	Config m_medium;
	static CommonDbConfig m_inst;
	static const char * m_dbdir;
};


#endif // _NTSETEST_DB_CONFIG_H_
