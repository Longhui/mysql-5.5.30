/**
 * ��ִ̨�е��û��߳�
 *
 * @author ��ΰ��(liweizhao@corp.netease.com)
 */
#ifndef _NTSE_BG_CUSTOM_THREAD_H_
#define _NTSE_BG_CUSTOM_THREAD_H_

#include "misc/Global.h"
#include "misc/Session.h"
#include "util/Sync.h"
#include "util/Hash.h"
#include "util/Thread.h"
#include "util/DList.h"

namespace ntse {

class Database;
class Table;
class Thread;
class BgTask;
class BgCustomThreadManager;

/**
 * ��̨�û��߳�
 */
class BgCustomThread : public Thread, public DLink<BgCustomThread*> {
public:
	BgCustomThread(const char *name, Database *db, BgCustomThreadManager *bgThdManager, 
		Table *table, bool needUnregister, bool trxConn = false);
	virtual ~BgCustomThread();
	void run();
	
	/**
	 * ������ݿ�����ID
	 * @return 
	 */
	inline uint getConnId() {
		assert(m_conn);
		return m_conn->getId();
	}

	/**
	 * �����߳�ȡ����־
	 */
	inline void cancel() {
		m_cancelFlag = true;
	}

protected:
	/**
	 * ������һ����ʵ�ֺ�̨�߳�Ҫ���Ĳ���
	 * @throw NtseException 
	 */
	virtual void doBgWork() throw(NtseException) {}

protected:
	BgCustomThreadManager *m_bgThdManager; /** ������Optimize��̨�̵߳Ĺ����� */
	const char *m_path;     /** �����·�� */
	Database   *m_db;       /** ���������ݿ� */
	Connection *m_conn;     /** ���ݿ����� */
	Session    *m_session;  /** �Ự */
	bool       m_needUnregister;/** ��������ʱ�Ƿ���Ҫ�������ע���Լ� */
	bool       m_cancelFlag;/** �����Ƿ�ȡ�� */
};

/**
 * ��̨�û��̵߳Ĺ�����
 */
class BgCustomThreadManager : public BgTask {
public:
	static const uint DFL_RECLAIM_INTERVAL = 3600000;
public:
	BgCustomThreadManager(Database *db, uint interval = DFL_RECLAIM_INTERVAL);
	~BgCustomThreadManager();
	virtual void runIt();
	void startBgThd(BgCustomThread *thd);

	void registerThd(BgCustomThread *thd);
	void unRegisterThd(BgCustomThread *thd);

	void stopAll(bool forceStop = false);
	void cancelThd(uint connId) throw(NtseException);

protected:
	void reclaimThd();

protected:
	Mutex      m_mutex; /** �������������ȫ�Ļ����� */
	DList<BgCustomThread*> *m_runningThds;/** �������е�Optimize��̨�߳��б� */
	DList<BgCustomThread*> *m_zombieThds; /** �ɻ��յ�Optimize��̨�߳��б� */
};

}

#endif