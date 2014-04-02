#include "api/TNTTblArgAlter.h"
#include "api/TNTDatabase.h"
#include "api/TNTTable.h"
#include "misc/ParFileParser.h"

/**
 * 构造函数。
 *
 * @param db		数据库
 * @param conn		数据库连接
 * @param parser	命令解析
 * @param timeout	锁超时时间，单位秒。
 * @param inLockTables  操作是否处于Lock Tables 语句之中
 */
TNTTableArgAlterHelper::TNTTableArgAlterHelper(TNTDatabase *tntDb, Connection *conn, Parser *parser, int timeout, bool inLockTables):
TableArgAlterHelper(tntDb->getNtseDb(), conn, parser, timeout, inLockTables){
	assert(NULL != tntDb);
	m_tntDb = tntDb;
}

/** 真正执行修改表定义参数的命令
 * @param session 会话
 * @param parFiles 分区表路径
 * @param name  修改参数的名称
 * @param value 修改参数的值
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
			//非分区表执行修改表
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
