/** TNT Table��log��־
 * author �ö���xindingfeng@corp.netease.com
 */
#include "api/TNTTable.h"
#include "api/Table.h"

namespace tnt {
/** дpurge��һ�׶ο�ʼǰ����־
 * @param session �����Ự
 * @param txnId purge��������������id
 * @param preLsn ͬһ����ǰһ����־��lsn
 * @param minReadView txnIdС��minReadView�ļ�¼����purge�������ȥ
 * return ��־���к�
 */
LsnType TNTTable::writePurgePhase1(Session *session, TrxId txnId, LsnType preLsn, TrxId minReadView) {
	LsnType lsn = 0;
	MemoryContext *ctx = session->getMemoryContext();
	McSavepoint msp(ctx);
	size_t size = sizeof(txnId) + sizeof(preLsn) + sizeof(minReadView);
	byte *buf = (byte *)ctx->alloc(size);
	Stream s(buf, size);
	s.write(txnId);
	s.write(preLsn);
	s.write(minReadView);
	lsn = session->getTrans()->writeTNTLog(TNT_PURGE_BEGIN_FIR_PHASE, m_tab->getTableDef()->m_id, buf, s.getSize());
	return lsn;
}

/** ����purge��һ�׶ο�ʼǰ����־
 * @param log ��־����
 * @param txnId out purge��������������id
 * @param preLsn out ͬһ����ǰһ����־��lsn
 * @param minReadView out txnIdС��minReadView�ļ�¼����purge�������ȥ
 */
void TNTTable::parsePurgePhase1(const LogEntry *log, TrxId *txnId, LsnType *preLsn, TrxId *minReadView) {
	Stream s(log->m_data, log->m_size);
	s.read(txnId);
	s.read(preLsn);
	s.read(minReadView);
}

/** дpurge�ڶ��׶ο�ʼǰ����־
 * @param session �����Ự
 * @param txnId purge��������������id
 * @param preLsn ͬһ����ǰһ����־��lsn
 * @param minReadView txnIdС��minReadView�ļ�¼����purge�������ȡ
 * return ��־���к�
 */
LsnType TNTTable::writePurgePhase2(Session *session, TrxId txnId, LsnType preLsn) {
	LsnType lsn = 0;
	MemoryContext *ctx = session->getMemoryContext();
	McSavepoint msp(ctx);
	size_t size = sizeof(txnId) + sizeof(preLsn);
	byte *buf = (byte *)ctx->alloc(size);
	Stream s(buf, size);
	s.write(txnId);
	s.write(preLsn);
	lsn = session->getTrans()->writeTNTLog(TNT_PURGE_BEGIN_SEC_PHASE, m_tab->getTableDef()->m_id, buf, s.getSize());
	return lsn;
}

/** ����purge�ڶ��׶ο�ʼǰ����־
 * @param log ��־����
 * @param txnId out purge��������������id
 * @param preLsn out ͬһ����ǰһ����־��lsn
 * @param minReadView out xnIdС��minReadView�ļ�¼����purge�������ȥ
 */
void TNTTable::parsePurgePhase2(const LogEntry *log, TrxId *txnId, LsnType *preLsn) {
	Stream s(log->m_data, log->m_size);
	s.read(txnId);
	s.read(preLsn);
}

/** purge��������־
 * @param session �����Ự
 * @param txnId purge��������������id
 * @param preLsn ͬһ����ǰһ����־��lsn
 * return ��־�����к�
 */
LsnType TNTTable::writePurgeTableEnd(Session *session, TrxId txnId, LsnType preLsn) {
	m_purgeStatus = PS_NOOP;
	LsnType lsn = 0;
	MemoryContext *ctx = session->getMemoryContext();
	McSavepoint msp(ctx);
	size_t size = sizeof(txnId) + sizeof(preLsn);
	byte *buf = (byte *)ctx->alloc(size);
	Stream s(buf, size);
	s.write(txnId);
	s.write(preLsn);
	lsn = session->getTrans()->writeTNTLog(TNT_PURGE_END_HEAP, m_tab->getTableDef()->m_id, buf, s.getSize());
	return lsn;
}

/** ����purge��������־
 * @param log ��־����
 * @param txnId out purge��������������id
 * @param preLsn out ͬһ����ǰһ����־��lsn
 */
/*void TNTTable::parsePurgeTableEnd(const LogEntry *log, TrxId *txnId, LsnType *preLsn) {
	Stream s(log->m_data, log->m_size);
	s.read(txnId);
	s.read(preLsn);
}*/
}
