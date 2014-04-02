/**
 * 后台执行Optimize操作的线程
 *
 * @author 李伟钊(liweizhao@corp.netease.com)
 */
#ifndef _NTSE_OPTIMIZE_THREAD_H_
#define _NTSE_OPTIMIZE_THREAD_H_

#include "api/Database.h"
#include "misc/BgCustomThread.h"
#include "api/TNTDatabase.h"
#include "api/TNTTable.h"

namespace ntse {

class Database;

class OptimizeThread : public BgCustomThread {
public:
	OptimizeThread(Database *db, BgCustomThreadManager *optThdManager, 
		Table *table, bool keepDict, bool needUnregister);
	OptimizeThread(TNTDatabase *db, BgCustomThreadManager *optThdManager, 
		TNTTable *table, bool keepDict, bool needUnregister, bool trxConn);
	~OptimizeThread();

protected:
	void doBgWork() throw(NtseException);

private:
	bool m_keepDict;  /** 是否保留字典 */
	TNTDatabase *m_tntDb;
};

}

#endif