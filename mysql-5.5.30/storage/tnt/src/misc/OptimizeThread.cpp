/**
 * ��ִ̨��Optimize�������߳�
 *
 * @author ��ΰ��(liweizhao@corp.netease.com)
 */
#include "misc/OptimizeThread.h"

namespace ntse {

/**
 * OptimizeThread���캯��
 * @param db ���ݿ�
 * @param bgThdManager ��̨�û��̹߳�����
 * @param table �����ı�
 * @param keepDict �Ƿ����ֵ�
 * @param needUnregister �Ƿ���Ҫ���û��̹߳�����ע���Լ�
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
 * ����BgCustomThread::doBgwork 
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