/** �ڴ�ѵ�log��־
 * author �ö���xindingfeng@corp.netease.com
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

/** д�״θ��µ�log��־
 * @param session �����Ự
 * @param txnId ���²�������������id
 * @param preLsn ͬһ����ǰһ����־��lsn
 * @param rid ��Ҫ���¼�¼��rowid
 * @param rollBackId �ع���¼��rowId
 * @param tableIndex �汾�����
 * @param update ���º���
 * return ��־��lsn
 */
LsnType MRecords::writeFirUpdateLog(Session *session, TrxId txnId, LsnType preLsn, RowId rid, RowId rollBackId, u8 tableIndex, SubRecord *update) {
	return writeUpdateLog(session, TNT_U_I_LOG, txnId, preLsn, rid, rollBackId, tableIndex, update);
}

/** �����״θ��µ�log��־
 * @param log ��Ҫ������log��־
 * @param txnId out ���²�������������id
 * @param preLsn out ͬһ����ǰһ����־��lsn
 * @param rid out ��Ҫ���¼�¼��rowid
 * @param rollBackId out �ع���¼��rowId
 * @param tableIndex out �汾�����
 * @param update out ���º���
 * @param ctx �����ڴ��������
 */
void MRecords::parseFirUpdateLog(const LogEntry *log, TrxId *txnId, LsnType *preLsn, RowId *rid, RowId *rollBackId, u8 *tableIndex, SubRecord **update, MemoryContext *ctx) {
	assert(log->m_logType == TNT_U_I_LOG);
	return parseUpdateLog(log, txnId, preLsn, rid, rollBackId, tableIndex, update, ctx);
}

/** д���״θ��µ�log��־
 * @param session �����Ự
 * @param txnId ���²�������������id
 * @param preLsn ͬһ����ǰһ����־��lsn
 * @param rid ��Ҫ���¼�¼��rowid
 * @param rollBackId �ع���¼��rowId
 * @param tableIndex �汾�����
 * @param update �����ֶ�ֵ
 * return ��־��lsn
 */
LsnType MRecords::writeSecUpdateLog(Session *session, TrxId txnId, LsnType preLsn, RowId rid, RowId rollBackId, u8 tableIndex, SubRecord *update) {
	return writeUpdateLog(session, TNT_U_U_LOG, txnId, preLsn, rid, rollBackId, tableIndex, update);
}

/** �������״θ��µ�log��־
 * @param log ��Ҫ������log��־
 * @param txnId out ���²�������������id
 * @param preLsn out ͬһ����ǰһ����־��lsn
 * @param rid out ��Ҫ���¼�¼��rowid
 * @param rollBackId out �ع���¼��rowId
 * @param tableIndex out �汾�����
 * @param update out �����ֶ�ֵ
 * @param ctx �����ڴ��������
 */
void MRecords::parseSecUpdateLog(const LogEntry *log, TrxId *txnId, LsnType *preLsn, RowId *rid, RowId *rollBackId, u8 *tableIndex, SubRecord **update, MemoryContext *ctx) {
	assert(log->m_logType == TNT_U_U_LOG);
	parseUpdateLog(log, txnId, preLsn, rid, rollBackId, tableIndex, update, ctx);
}

/** д�״�ɾ����log��־
 * @param session �����Ự
 * @param txnId ɾ����������������id
 * @param preLsn ͬһ����ǰһ����־��lsn
 * @param rid ��Ҫ���¼�¼��rowid
 * @param rollBackId �ع���¼��rowId
 * @param tableIndex �汾�����
 * @param delRec ɾ��ǰ��
 * return ��־��lsn
 */
LsnType MRecords::writeFirRemoveLog(Session *session, TrxId txnId, LsnType preLsn, RowId rid, RowId rollBackId, u8 tableIndex/*, Record *delRec*/) {
	return writeRemoveLog(session, TNT_D_I_LOG, txnId, preLsn, rid, rollBackId, tableIndex);
}

/** �����״�ɾ����log��־
 * @param log ��Ҫ������log��־
 * @param txnId out ɾ����������������id
 * @param preLsn out ͬһ����ǰһ����־��lsn
 * @param rid out ��Ҫɾ����¼��rowid
 * @param rollBackId out �ع���¼��rowId
 * @param tableIndex out �汾�����
 * @param delRec out ɾ��ǰ��
 * @param ctx �����ڴ��������
 */
void MRecords::parseFirRemoveLog(const LogEntry *log, TrxId *txnId, LsnType *preLsn, RowId *rid, RowId *rollBackId, u8 *tableIndex/*, Record **delRec, MemoryContext *ctx*/) {
	assert(log->m_logType == TNT_D_I_LOG);
	parseRemoveLog(log, txnId, preLsn, rid, rollBackId, tableIndex);
}

/** д���״�ɾ����log��־
 * @param session �����Ự
 * @param txnId ɾ����������������id
 * @param preLsn ͬһ����ǰһ����־��lsn
 * @param rid ��Ҫ���¼�¼��rowid
 * @param rollBackId �ع���¼��rowId
 * @param tableIndex �汾�����
 * return ��־��lsn
 */
LsnType MRecords::writeSecRemoveLog(Session *session, TrxId txnId, LsnType preLsn, RowId rid, RowId rollBackId, u8 tableIndex) {
	return writeRemoveLog(session, TNT_D_U_LOG, txnId, preLsn, rid, rollBackId, tableIndex);
}

/** �������״�ɾ����log��־
 * @param log ��Ҫ������log��־
 * @param txnId out ɾ����������������id
 * @param preLsn out ͬһ����ǰһ����־��lsn
 * @param rid out ��Ҫɾ����¼��rowid
 * @param rollBackId out �ع���¼��rowId
 * @param tableIndex out �汾�����
 */
void MRecords::parseSecRemoveLog(const LogEntry *log, TrxId *txnId, LsnType *preLsn, RowId *rid, RowId *rollBackId, u8 *tableIndex) {
	assert(log->m_logType == TNT_D_U_LOG);
	parseRemoveLog(log, txnId, preLsn, rid, rollBackId, tableIndex);
}
}
