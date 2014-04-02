/** 内存堆的log日志
 * author 忻丁峰xindingfeng@corp.netease.com
 */
#include "rec/MRecords.h"

namespace tnt {
LsnType MRecords::writeUpdateLog(Session *session, LogType logType, TrxId txnId, LsnType preLsn, RowId rid, RowId rollBackId, u8 tableIndex, SubRecord *update) {
	LsnType lsn = 0;
	MemoryContext *ctx = session->getMemoryContext();
	McSavepoint msp(ctx);
	size_t size = sizeof(txnId) + sizeof(preLsn) + sizeof(rid) + sizeof(rollBackId) + sizeof(tableIndex);
	size += RecordOper::getSubRecordSerializeSize(*m_tableDef, update, false);
	byte *buf = (byte *)ctx->alloc(size);
	Stream s(buf, size);
	s.write(txnId);
	s.write(preLsn);
	s.writeRid(rid);
	s.writeRid(rollBackId);
	s.write(tableIndex);
	RecordOper::serializeSubRecordMNR(&s, *m_tableDef, update, false);
	lsn = session->getTrans()->writeTNTLog(logType, (*m_tableDef)->m_id, buf, s.getSize());
	return lsn;
}

void MRecords::parseUpdateLog(const LogEntry *log, TrxId *txnId, LsnType *preLsn, RowId *rid, RowId *rollBackId, u8 *tableIndex, SubRecord **update, MemoryContext *ctx) {
	Stream s(log->m_data, log->m_size);
	s.read(txnId);
	s.read(preLsn);
	s.readRid(rid);
	s.readRid(rollBackId);
	s.read(tableIndex);
	*update = RecordOper::unserializeSubRecordMNR(&s, *m_tableDef, ctx);
}

LsnType MRecords::writeRemoveLog(Session *session, LogType logType, TrxId txnId, LsnType preLsn, RowId rid, RowId rollBackId, u8 tableIndex) {
	LsnType lsn = 0;
	MemoryContext *ctx = session->getMemoryContext();
	McSavepoint msp(ctx);
	size_t size = sizeof(txnId) + sizeof(preLsn) + sizeof(rid) + sizeof(rollBackId) + sizeof(tableIndex);
	byte *buf = (byte *)ctx->alloc(size);
	Stream s(buf, size);
	s.write(txnId);
	s.write(preLsn);
	s.writeRid(rid);
	s.writeRid(rollBackId);
	s.write(tableIndex);
	lsn = session->getTrans()->writeTNTLog(logType, (*m_tableDef)->m_id, buf, s.getSize());
	return lsn;
}

void MRecords::parseRemoveLog(const LogEntry *log, TrxId *txnId, LsnType *preLsn, RowId *rid, RowId *rollBackId, u8 *tableIndex) {
	Stream s(log->m_data, log->m_size);
	s.read(txnId);
	s.read(preLsn);
	s.readRid(rid);
	s.readRid(rollBackId);
	s.read(tableIndex);
}

/** 写首次更新的log日志
 * @param session 操作会话
 * @param txnId 更新操作所属的事务id
 * @param preLsn 同一事务前一个日志的lsn
 * @param rid 需要更新记录的rowid
 * @param rollBackId 回滚记录的rowId
 * @param tableIndex 版本池序号
 * @param update 更新后像
 * return 日志的lsn
 */
LsnType MRecords::writeFirUpdateLog(Session *session, TrxId txnId, LsnType preLsn, RowId rid, RowId rollBackId, u8 tableIndex, SubRecord *update) {
	return writeUpdateLog(session, TNT_U_I_LOG, txnId, preLsn, rid, rollBackId, tableIndex, update);
}

/** 解析首次更新的log日志
 * @param log 需要解析的log日志
 * @param txnId out 更新操作所属的事务id
 * @param preLsn out 同一事务前一个日志的lsn
 * @param rid out 需要更新记录的rowid
 * @param rollBackId out 回滚记录的rowId
 * @param tableIndex out 版本池序号
 * @param update out 更新后像
 * @param ctx 分配内存的上下文
 */
void MRecords::parseFirUpdateLog(const LogEntry *log, TrxId *txnId, LsnType *preLsn, RowId *rid, RowId *rollBackId, u8 *tableIndex, SubRecord **update, MemoryContext *ctx) {
	assert(log->m_logType == TNT_U_I_LOG);
	return parseUpdateLog(log, txnId, preLsn, rid, rollBackId, tableIndex, update, ctx);
}

/** 写非首次更新的log日志
 * @param session 操作会话
 * @param txnId 更新操作所属的事务id
 * @param preLsn 同一事务前一个日志的lsn
 * @param rid 需要更新记录的rowid
 * @param rollBackId 回滚记录的rowId
 * @param tableIndex 版本池序号
 * @param update 更新字段值
 * return 日志的lsn
 */
LsnType MRecords::writeSecUpdateLog(Session *session, TrxId txnId, LsnType preLsn, RowId rid, RowId rollBackId, u8 tableIndex, SubRecord *update) {
	return writeUpdateLog(session, TNT_U_U_LOG, txnId, preLsn, rid, rollBackId, tableIndex, update);
}

/** 解析非首次更新的log日志
 * @param log 需要解析的log日志
 * @param txnId out 更新操作所属的事务id
 * @param preLsn out 同一事务前一个日志的lsn
 * @param rid out 需要更新记录的rowid
 * @param rollBackId out 回滚记录的rowId
 * @param tableIndex out 版本池序号
 * @param update out 更新字段值
 * @param ctx 分配内存的上下文
 */
void MRecords::parseSecUpdateLog(const LogEntry *log, TrxId *txnId, LsnType *preLsn, RowId *rid, RowId *rollBackId, u8 *tableIndex, SubRecord **update, MemoryContext *ctx) {
	assert(log->m_logType == TNT_U_U_LOG);
	parseUpdateLog(log, txnId, preLsn, rid, rollBackId, tableIndex, update, ctx);
}

/** 写首次删除的log日志
 * @param session 操作会话
 * @param txnId 删除操作所属的事务id
 * @param preLsn 同一事务前一个日志的lsn
 * @param rid 需要更新记录的rowid
 * @param rollBackId 回滚记录的rowId
 * @param tableIndex 版本池序号
 * @param delRec 删除前像
 * return 日志的lsn
 */
LsnType MRecords::writeFirRemoveLog(Session *session, TrxId txnId, LsnType preLsn, RowId rid, RowId rollBackId, u8 tableIndex/*, Record *delRec*/) {
	return writeRemoveLog(session, TNT_D_I_LOG, txnId, preLsn, rid, rollBackId, tableIndex);
}

/** 解析首次删除的log日志
 * @param log 需要解析的log日志
 * @param txnId out 删除操作所属的事务id
 * @param preLsn out 同一事务前一个日志的lsn
 * @param rid out 需要删除记录的rowid
 * @param rollBackId out 回滚记录的rowId
 * @param tableIndex out 版本池序号
 * @param delRec out 删除前像
 * @param ctx 分配内存的上下文
 */
void MRecords::parseFirRemoveLog(const LogEntry *log, TrxId *txnId, LsnType *preLsn, RowId *rid, RowId *rollBackId, u8 *tableIndex/*, Record **delRec, MemoryContext *ctx*/) {
	assert(log->m_logType == TNT_D_I_LOG);
	parseRemoveLog(log, txnId, preLsn, rid, rollBackId, tableIndex);
}

/** 写非首次删除的log日志
 * @param session 操作会话
 * @param txnId 删除操作所属的事务id
 * @param preLsn 同一事务前一个日志的lsn
 * @param rid 需要更新记录的rowid
 * @param rollBackId 回滚记录的rowId
 * @param tableIndex 版本池序号
 * return 日志的lsn
 */
LsnType MRecords::writeSecRemoveLog(Session *session, TrxId txnId, LsnType preLsn, RowId rid, RowId rollBackId, u8 tableIndex) {
	return writeRemoveLog(session, TNT_D_U_LOG, txnId, preLsn, rid, rollBackId, tableIndex);
}

/** 解析非首次删除的log日志
 * @param log 需要解析的log日志
 * @param txnId out 删除操作所属的事务id
 * @param preLsn out 同一事务前一个日志的lsn
 * @param rid out 需要删除记录的rowid
 * @param rollBackId out 回滚记录的rowId
 * @param tableIndex out 版本池序号
 */
void MRecords::parseSecRemoveLog(const LogEntry *log, TrxId *txnId, LsnType *preLsn, RowId *rid, RowId *rollBackId, u8 *tableIndex) {
	assert(log->m_logType == TNT_D_U_LOG);
	parseRemoveLog(log, txnId, preLsn, rid, rollBackId, tableIndex);
}
}
