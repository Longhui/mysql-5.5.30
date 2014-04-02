/**
 * 后台执行Optimize操作的线程
 *
 * @author 李伟钊(liweizhao@corp.netease.com)
 */
#include "misc/OptimizeThread.h"

namespace ntse {

/**
 * OptimizeThread构造函数
 * @param db 数据库
 * @param bgThdManager 后台用户线程管理器
 * @param table 操作的表
 * @param keepDict 是否保留字典
 * @param needUnregister 是否需要向用户线程管理器注销自己
 */
OptimizeThread::OptimizeThread(Database *db, BgCustomThreadManager *bgThdManager, 
							   Table *table, bool keepDict, bool needUnregister)
	: BgCustomThread("Optimize Thread", db, bgThdManager, table, needUnregister), m_keepDict(keepDict) {
		m_tntDb = NULL;
}

OptimizeThread::OptimizeThread(TNTDatabase *db, BgCustomThreadManager *bgThdManager, 
			   TNTTable *table, bool keepDict, bool needUnregister, bool trxConn)
	: BgCustomThread("Optimize Thread", db->getNtseDb(), bgThdManager, table->getNtseTable(), needUnregister, trxConn), m_keepDict(keepDict) {
	m_tntDb = db;
}

OptimizeThread::~OptimizeThread() {

}

/**
 * 重载BgCustomThread::doBgwork 
 */
void OptimizeThread::doBgWork() throw(NtseException) {
	if (!m_tntDb) {
		m_db->doOptimize(m_session, m_path, m_keepDict, &m_cancelFlag);
	} else {
		TNTTransaction *trx = NULL;
		if (m_session->getConnection()->isTrx()) {
			trx = m_tntDb->getTransSys()->allocTrx();
			trx->startTrxIfNotStarted(m_session->getConnection(), true);
			m_session->setTrans(trx);
		}

		try {
			m_tntDb->doOptimize(m_session, m_path, m_keepDict, &m_cancelFlag);
		} catch (NtseException &e) {
			if (trx != NULL) {
				trx->rollbackTrx(RBS_INNER, m_session);
				m_tntDb->getTransSys()->freeTrx(trx);
			}
			throw e;
		}

		if (trx != NULL) {
			trx->commitTrx(CS_INNER);
			m_tntDb->getTransSys()->freeTrx(trx);
		}
	}
}

}