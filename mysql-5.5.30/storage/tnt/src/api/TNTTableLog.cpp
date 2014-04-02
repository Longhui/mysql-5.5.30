/** TNT Table的log日志
 * author 忻丁峰xindingfeng@corp.netease.com
 */
#include "api/TNTTable.h"
#include "api/Table.h"

namespace tnt {
/** 写purge第一阶段开始前的日志
 * @param session 操作会话
 * @param txnId purge操作所属的事务id
 * @param preLsn 同一事务前一个日志的lsn
 * @param minReadView txnId小于minReadView的记录可以purge到外存中去
 * return 日志序列号
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

/** 解析purge第一阶段开始前的日志
 * @param log 日志对象
 * @param txnId out purge操作所属的事务id
 * @param preLsn out 同一事务前一个日志的lsn
 * @param minReadView out txnId小于minReadView的记录可以purge到外存中去
 */
void TNTTable::parsePurgePhase1(const LogEntry *log, TrxId *txnId, LsnType *preLsn, TrxId *minReadView) {
	Stream s(log->m_data, log->m_size);
	s.read(txnId);
	s.read(preLsn);
	s.read(minReadView);
}

/** 写purge第二阶段开始前的日志
 * @param session 操作会话
 * @param txnId purge操作所属的事务id
 * @param preLsn 同一事务前一个日志的lsn
 * @param minReadView txnId小于minReadView的记录可以purge到外存中取
 * return 日志序列号
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

/** 解析purge第二阶段开始前的日志
 * @param log 日志对象
 * @param txnId out purge操作所属的事务id
 * @param preLsn out 同一事务前一个日志的lsn
 * @param minReadView out xnId小于minReadView的记录可以purge到外存中去
 */
void TNTTable::parsePurgePhase2(const LogEntry *log, TrxId *txnId, LsnType *preLsn) {
	Stream s(log->m_data, log->m_size);
	s.read(txnId);
	s.read(preLsn);
}

/** purge结束的日志
 * @param session 操作会话
 * @param txnId purge操作所属的事务id
 * @param preLsn 同一事务前一个日志的lsn
 * return 日志的序列号
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

/** 解析purge结束的日志
 * @param log 日志对象
 * @param txnId out purge操作所属的事务id
 * @param preLsn out 同一事务前一个日志的lsn
 */
/*void TNTTable::parsePurgeTableEnd(const LogEntry *log, TrxId *txnId, LsnType *preLsn) {
	Stream s(log->m_data, log->m_size);
	s.read(txnId);
	s.read(preLsn);
}*/
}
