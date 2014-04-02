#include "api/TNTTblArgAlter.h"
#include "api/TNTDatabase.h"
#include "api/TNTTable.h"
#include "misc/ParFileParser.h"

/**
 * ���캯����
 *
 * @param db		���ݿ�
 * @param conn		���ݿ�����
 * @param parser	�������
 * @param timeout	����ʱʱ�䣬��λ�롣
 * @param inLockTables  �����Ƿ���Lock Tables ���֮��
 */
TNTTableArgAlterHelper::TNTTableArgAlterHelper(TNTDatabase *tntDb, Connection *conn, Parser *parser, int timeout, bool inLockTables):
TableArgAlterHelper(tntDb->getNtseDb(), conn, parser, timeout, inLockTables){
	assert(NULL != tntDb);
	m_tntDb = tntDb;
}

/** ����ִ���޸ı������������
 * @param session �Ự
 * @param parFiles ������·��
 * @param name  �޸Ĳ���������
 * @param value �޸Ĳ�����ֵ
 */
void TNTTableArgAlterHelper::alterTableArgumentReal(Session *session, vector<string> *parFiles, const char *name, const char *value) throw(NtseException) {
	TNTTable *table = NULL;
	bool isTrxConn = false;
	isTrxConn = session->getConnection()->isTrx();
	TNTTransaction *trx = m_tntDb->getTransSys()->allocTrx();

	try {
		for (uint i = 0; i < parFiles->size(); i++) {
			session->setTrans(trx);
			table = m_tntDb->openTable(session, parFiles->at(i).c_str());
			session->setTrans(NULL);
			//�Ƿ�����ִ���޸ı�
			if (isTrxConn) {
				m_tntDb->alterTableArgument(session, table, name, value, m_timeout, m_inLockTables);
			} else {
				m_db->alterTableArgument(session, table->getNtseTable(), name, value, m_timeout, m_inLockTables);
			}
			m_tntDb->closeTable(session, table);
			table = NULL;
		}
	} catch (NtseException &e) {
		if (table)
			m_tntDb->closeTable(session, table);
		if (trx) {
			m_tntDb->getTransSys()->freeTrx(trx);
		}
		session->setTrans(NULL);
		throw e;
	}
	m_tntDb->getTransSys()->freeTrx(trx);
	session->setTrans(NULL);
}
