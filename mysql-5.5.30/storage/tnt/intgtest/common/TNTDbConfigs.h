/**
 * �������ݿ������ļ�TNT
 * 
 * @author �Լ�Զ(hzzhaojy@corp.netease.com)
 */
#ifndef _TNTTEST_DB_CONFIG_H_
#define _TNTTEST_DB_CONFIG_H_

#include "api/Database.h"
#include "misc/TNTControlFile.h"
#include "DbConfigs.h"

using namespace ntse;
using namespace tnt;

#ifdef WIN32
#define PERF_SMALL_CONFIG
#endif

class TNTCommonDbConfig{
public:
	/** ��ȡС�����ݿ����ò��� */
	static TNTConfig* getSmall() {
		return &m_inst.m_small;
	}
	/** ��ȡ�������ݿ����ò��� */
	static TNTConfig* getMedium() {
		return &m_inst.m_medium;
	}
	/** ���ݿ�Ŀ¼ */
	static const char * getBasedir() {
		return m_dbdir;
	}

private:
	TNTCommonDbConfig() {
	

#ifdef PERF_SMALL_CONFIG
		m_small.m_ntseConfig = *(CommonDbConfig::getSmall());
		if(!m_small.m_tntBasedir)
			delete []m_small.m_tntBasedir;
        m_small.m_tntBasedir= System::strdup(m_dbdir);
		if(!m_small.m_dumpdir)
			delete []m_small.m_dumpdir;
		m_small.m_dumpdir = System::strdup(m_dbdir);

		m_medium.m_ntseConfig = *(CommonDbConfig::getMedium());
		if(!m_medium.m_tntBasedir)
			delete []m_medium.m_tntBasedir;
        m_medium.m_tntBasedir= System::strdup(m_dbdir);
		if(!m_medium.m_dumpdir)
			delete []m_medium.m_dumpdir;
		m_medium.m_dumpdir = System::strdup(m_dbdir);
#else
        m_small.m_ntseConfig = *(CommonDbConfig::getSmall());
		m_medium.m_ntseConfig = *(CommonDbConfig::getMedium());
#endif

	}

private:
	TNTConfig m_small;
	TNTConfig m_medium;
	static TNTCommonDbConfig m_inst;
	static const char * m_dbdir;

};

#endif // _TNTTEST_DB_CONFIG_H_