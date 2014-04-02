/**
 * 后台执行的用户线程
 *
 * @author 李伟钊(liweizhao@corp.netease.com)
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
 * 后台用户线程
 */
class BgCustomThread : public Thread, public DLink<BgCustomThread*> {
public:
	BgCustomThread(const char *name, Database *db, BgCustomThreadManager *bgThdManager, 
		Table *table, bool needUnregister, bool trxConn = false);
	virtual ~BgCustomThread();
	void run();
	
	/**
	 * 获得数据库连接ID
	 * @return 
	 */
	inline uint getConnId() {
		assert(m_conn);
		return m_conn->getId();
	}

	/**
	 * 设置线程取消标志
	 */
	inline void cancel() {
		m_cancelFlag = true;
	}

protected:
	/**
	 * 重载这一函数实现后台线程要作的操作
	 * @throw NtseException 
	 */
	virtual void doBgWork() throw(NtseException) {}

protected:
	BgCustomThreadManager *m_bgThdManager; /** 所属的Optimize后台线程的管理器 */
	const char *m_path;     /** 表相对路径 */
	Database   *m_db;       /** 表所属数据库 */
	Connection *m_conn;     /** 数据库连接 */
	Session    *m_session;  /** 会话 */
	bool       m_needUnregister;/** 操作结束时是否需要向管理器注销自己 */
	bool       m_cancelFlag;/** 操作是否被取消 */
};

/**
 * 后台用户线程的管理器
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
	Mutex      m_mutex; /** 保护链表操作安全的互斥锁 */
	DList<BgCustomThread*> *m_runningThds;/** 正在运行的Optimize后台线程列表 */
	DList<BgCustomThread*> *m_zombieThds; /** 可回收的Optimize后台线程列表 */
};

}

#endif