/**
 * TNT�����������ģ�顣
 *
 * @author ��ΰ��(liweizhao@corp.netease.com)
 */

#include "api/TNTDatabase.h"
#include "trx/TNTTransaction.h"
#include "misc/TableDef.h"
#include "util/Stream.h"

using namespace ntse;

namespace tnt {

/**
 * ����һ����ͼ
 * @param createTrxId
 * @param trxIds
 * @param trxCnt
 */
ReadView::ReadView(TrxId createTrxId, TrxId *trxIds, uint trxCnt) 
	: m_createTrxId(createTrxId), m_trxCnt(trxCnt), m_trxIds(trxIds), m_minStartLsn(INVALID_LSN) {
		m_rvLink.set(this);
}

/**
 * ��������ID�ж�����ͼ���Ƿ�ɼ�
 * @param trxId ����ID
 * @return 
 */
bool ReadView::isVisible(TrxId trxId) {
	if (trxId == m_createTrxId) {
		return true;
	} else if (trxId < m_upTrxId) {
		return true;
	} else if (trxId >= m_lowTrxId) {
		return false;
	} else {
		for (int i = (int)m_trxCnt - 1; i >= 0; i--) {
			if (trxId < m_trxIds[i]) {
				return true;
			} else if (trxId == m_trxIds[i]) {
				return false;
			}
		}
		return true;
	}
}

void ReadView::print() {
	printf("readView createTrxId = "I64FORMAT"u, lowTrxId = "I64FORMAT"u, upTrxId = "I64FORMAT"u\n", m_createTrxId, m_lowTrxId, m_upTrxId);
	printf("Active trxCnt = %d: ", m_trxCnt);
	for (uint i = 0; i < m_trxCnt; i++) {
		printf("Active TrxId = "I64FORMAT"u, ", m_trxIds[i]);
	}
	printf("\n");
}

////////////////////////////////////////////////////////////////////////////////////////

/**
 * TNTTransactionĬ�Ϲ��캯��
 * @return 
 */
TNTTransaction::TNTTransaction() {
}

/**
 * �����ʼ��
 * @param trxSys
 * @param mtx
 * @param trxId
 * @param lockMode
 * @param poolId
 * @return 
 */
void TNTTransaction::init(TNTTrxSys *trxSys, Txnlog *tntLog, MemoryContext *ctx, 
						  TrxId trxId, size_t poolId) {
	m_tranSys = trxSys;
	m_opInfo = "";
	m_trxId = trxId;
	m_isolationLevel = TRX_ISO_REPEATABLE_READ;
	m_flushMode = trxSys->getTrxFlushMode();

	m_supportXA = true;
	m_trxState = TRX_NOT_START;
	m_thd = NULL;
	m_threadId = (uint)-1;
	m_processId = (uint)-1;
	m_memctx = ctx;
	m_lockOwner = new (ctx->alloc(sizeof(DldLockOwner)))DldLockOwner(trxId, ctx);
	m_sp = m_memctx->setSavepoint();

	//m_duplicates = ;
	m_trxActiveStat = 1;
	m_tablesInUse = 0;
	m_tablesLocked = 0;
	m_queryStr = NULL;
	m_queryLen = NULL;
	m_globalReadView = NULL;
	m_readView = NULL;

	m_binlogFileName = NULL;
	m_binlogPosition = (u64)-1;

	m_lastLSN = INVALID_LSN;
	m_lastStmtLsn = INVALID_LSN;
	m_realStartLsn = INVALID_LSN;
	m_beginLSN = INVALID_LSN;
	m_commitLsn = INVALID_LSN;

	m_versionPoolId = INVALID_VERSION_POOL_INDEX;
	m_tntLog = tntLog;
	m_logging = true;

	m_errState = 0;
	m_errInfo = NULL;
	m_trxIsRegistered = false;
	m_trxPoolId = poolId;
	//m_isPurge = false;
	m_magicNum = TRX_MAGIC_NUM;
	m_valid = true;
	memset(&m_xid, 0, sizeof(XID));
	m_trxLink.set(this);

	m_readOnly = true;
	m_lastRowLockStat = DLD_LOCK_SUCCESS;
	m_conn = NULL;

	m_inLockTables = false;
	m_beginTime = 0;
	m_waitingLock = false;
	m_waitStartTime = 0;
	m_redoCnt = 0;
	m_hangByRecover = false;
	m_hangTime = 0;

}

/**
 * ��������
 * @return 
 */
void TNTTransaction::destory() {
	assert(TRX_MAGIC_NUM == m_magicNum);
	if (NULL != m_lockOwner) {
		m_lockOwner->~DldLockOwner();
	}
	memset(this, 0, sizeof(TNTTransaction));
	m_magicNum = 11112222;
	m_valid = false;
	m_hangByRecover = false;
	m_hangTime = 0;
}

void TNTTransaction::reset() {
	m_realStartLsn = INVALID_LSN;
	m_beginLSN = INVALID_LSN;
	m_lastLSN = INVALID_LSN;
	m_lastStmtLsn = INVALID_LSN;
	m_commitLsn = INVALID_LSN;
	m_trxState = TRX_NOT_START;
	m_versionPoolId = INVALID_VERSION_POOL_INDEX;
	m_trxId = INVALID_TRX_ID;
	m_readOnly = true;
	m_memctx->resetToSavepoint(m_sp);
	m_logging = true;
	m_conn = NULL;
	m_inLockTables = false;
	m_beginTime = 0;
	m_waitingLock = false;
	m_waitStartTime = 0;
	m_redoCnt = 0;
	m_hangByRecover = false;
	m_hangTime = 0;
	m_thd = NULL;
}

/**
 * �������û�п�ʼ��ʼ����
 * @param conn ��ǰ����
 * @param inner �Ƿ����ڲ�����
 * @return 
 */
void TNTTransaction::startTrxIfNotStarted(Connection *conn, bool inner) {
	if (TRX_NOT_START == m_trxState) {
		m_beginTime = System::fastTime();
		m_tranSys->startTrx(this, inner);
		m_conn = conn;
	}
}

/**
 * �ж������Ƿ��Ѿ���ʼ
 * @return 
 */
bool TNTTransaction::isTrxStarted() {
	return TRX_NOT_START != m_trxState;
}

/**
 * ����prepare
 * @return 
 */
void TNTTransaction::prepareForMysql() {
	if (TRX_NOT_START == m_trxState)
		return;

	m_opInfo = "preparing";

	//�����ʱ����δstart����˵��������϶�ΪreadOnly��
	//����rollback�ǲ����κ��£���connection����ΪNULL
	startTrxIfNotStarted();

	m_tranSys->prepareTrxLow(this);

	m_opInfo = "";
}

/**
 * �����ύ
 * @throw NtseException
 * @return 
 */
bool TNTTransaction::commitTrx(CommitStat stat) {
	if (TRX_NOT_START == m_trxState)
		return true;

	m_opInfo = "committing";

	//�����ʱ����δstart����˵��������϶�ΪreadOnly��
	//����rollback�ǲ����κ��£���connection����ΪNULL
	startTrxIfNotStarted();

	m_tranSys->commitTrxLow(this, stat);

	m_opInfo = "";

	return true;
}

/**
 * MySQL�ϲ�������������ʱ���ٴ�ȷ��������ύ��������һ�ӿ�
 * @return 
 */
bool TNTTransaction::commitCompleteForMysql() {
	m_opInfo = "flushing log";

	if (m_commitLsn != INVALID_LSN)
		flushTNTLog(m_commitLsn, FS_COMMIT);

	m_opInfo = "";

	return true;
}

/**
 * �ع���������
 * @throw NtseException
 * @return 
 */
bool TNTTransaction::rollbackTrx(RollBackStat rollBackStat, Session *session/* = NULL*/) {
	if (TRX_NOT_START == m_trxState)
		return true;

	m_opInfo = "rollback";

	bool ret = m_tranSys->rollbackLow(this, session, rollBackStat, false);

	m_opInfo = "";

	return ret;
}

/**
 * �ع��������
 * @return 
 */
bool TNTTransaction::rollbackLastStmt(RollBackStat rollBackStat, Session *session/* = NULL*/) {
	if (TRX_NOT_START == m_trxState)
		return true;

	m_opInfo = "rollback of SQL statement";

	bool ret = m_tranSys->rollbackLow(this, session, rollBackStat, true);

	m_opInfo = "";

	return ret;
}

/**
 * �ع���������(ֻ���ָ�ʱʹ��)
 * @param session
 * @param logs
 * @return 
 */
bool TNTTransaction::rollbackForRecover(Session *session, DList<LogEntry *> *logs) {
	assert(TRX_NOT_START != m_trxState);

	m_opInfo = "rollback for recover";

	bool ret = m_tranSys->rollbackForRecoverLow(this, session, logs);

	m_opInfo = "";

	return ret;
}

/**
 * ��ǵ�ǰSQL������
 */
void  TNTTransaction::markSqlStatEnd() {
	if (m_trxState == TRX_NOT_START) {
		m_lastLSN = INVALID_LSN;
	}
	m_lastStmtLsn = m_lastLSN;
}

/**
 * ���������ȫ����������ͷ��
 * @param trxList
 */
void TNTTransaction::relatedWithTrxList(DList<TNTTransaction*> *trxList) {
	trxList->addFirst(&m_trxLink);
}

/**
 * �������ȫ������������ɾ��
 */
void TNTTransaction::excludeFromTrxList() {
	assert(m_trxLink.getList());
	m_trxLink.unLink();
}

/**
 * �жϰ汾�Ե�ǰ�����Ƿ�ɼ�
 * @param trxId Ҫ�жϵİ汾
 * @return 
 */
bool TNTTransaction::trxJudgeVisible(TrxId trxId) {
	if (NULL != m_readView) // һ���Զ�
		return m_readView->isVisible(trxId);
	return true;
}

/**
 * Ϊ�������һ����ͼ
 * @return 
 * @throw NtseException �ڴ治��
 */
ReadView* TNTTransaction::trxAssignReadView() throw(NtseException) {
	assert(m_trxState == TRX_ACTIVE);
	return m_tranSys->trxAssignReadView(this);
}

/**
 * ΪPurge�������һ����ͼ
 * @return 
 * @throw NtseException �ڴ治��
 */
ReadView* TNTTransaction::trxAssignPurgeReadView() throw(NtseException) {
	assert(m_trxState == TRX_ACTIVE);
	return m_tranSys->trxAssignPurgeReadView(this);
}

#ifdef NTSE_UNIT_TEST
ReadView* TNTTransaction::trxAssignReadView(TrxId *trxIds, uint trxCnt) throw(NtseException) {
	assert(m_trxState == TRX_ACTIVE);
	return m_tranSys->trxAssignReadView(this, trxIds, trxCnt);
}
#endif

/**
 * �ж�ָ�������Ƿ����
 * @param rowId ��¼id
 * @param tabId ��Ӧ�ı�id
 * @param bePrecise �Ƿ���Ҫ��ȷ�ж�
 * @return 
 */
bool TNTTransaction::pickRowLock(RowId rowId, TableId tabId, bool bePrecise) {
	TLockSys *lockSys = m_tranSys->getLockSys();
	return lockSys->pickRowLock(rowId, tabId, bePrecise);
}

/**
 * �ж�ĳ�������Ƿ��Ѿ�������ָ��ģʽ������
 * @param rowId
 * @param tabId
 * @param lockMode
 * @return 
 */
bool TNTTransaction::isRowLocked(RowId rowId, TableId tabId, TLockMode lockMode) {
	TLockSys *lockSys = m_tranSys->getLockSys();
	return lockSys->isRowLocked(m_lockOwner, lockMode, rowId, tabId);
}

/**
 * �ж�ĳ�������Ƿ��Ѿ�������ָ��ģʽ�ı���
 * @param tabId
 * @param lockMode
 * @return 
 */
bool TNTTransaction::isTableLocked(TableId tabId, TLockMode lockMode) {
	TLockSys *lockSys = m_tranSys->getLockSys();
	return lockSys->isTableLocked(m_lockOwner, lockMode, tabId);
}

/************************************************************************/
/**
* ��ӡ��ǰ������е�����������
*/
/************************************************************************/
void TNTTransaction::printTrxLocks() {
	uint nLocks = m_lockOwner->getHoldingList()->getSize();

	FILE * file = NULL;

	file = fopen("trxLocks.txt", "w+");

	assert(nLocks > 0);

	DLink<DldLock *> *next = m_lockOwner->getHoldingList()->getHeader();

	next = next->getNext();

	while (next != m_lockOwner->getHoldingList()->getHeader()) {
		DldLock * dldLock = next->get();
		fprintf(file, "%d %d %d \n", dldLock->getId(), dldLock->getLockMode(), dldLock->getStatus());

		next = next->getNext();
	}

	fclose(file);
}

/**
 * ���Լ����������ɹ����Ϸ���
 * @param lockMode
 * @param rowId
 * @param tabId
 * @return 
 */
bool TNTTransaction::tryLockRow(TLockMode lockMode, RowId rowId, TableId tabId) {
	// ��λ��Lock Tables�����֮�У��򲻼�����
	if (m_inLockTables) {
		return true;
	}

    TLockSys *lockSys = m_tranSys->getLockSys();
	assert(NULL != lockSys);

	m_lastRowLockStat = lockSys->tryLockRow(m_lockOwner, lockMode, rowId, tabId);

	// try lock ���״�ֻ���سɹ�����ʧ��
	try {
		return lockSys->checkResult(m_lastRowLockStat);
	} catch (NtseException &e) {
		UNREFERENCED_PARAMETER(e);
		return false;
	}
}

/**
 * ����������
 * �������ȴ�����ô�ȴ���ʱͨ��ȫ�ֱ�������
 * @param lockMode
 * @param rowId
 * @param tabId
 * @throws
 */
void TNTTransaction::lockRow(TLockMode lockMode, RowId rowId, TableId tabId) throw(NtseException) {
	// ��λ��Lock Tables�����֮�У��򲻼�����
	if (m_inLockTables) {
		return;
	}

    TLockSys *lockSys = m_tranSys->getLockSys();
	assert(NULL != lockSys);
	m_waitingLock = true;
	m_waitStartTime = System::fastTime();
	m_lastRowLockStat = lockSys->lockRow(m_lockOwner, lockMode, rowId, tabId);
	m_waitingLock = false;
	return lockSys->translateErr(m_lastRowLockStat);
}

/**
 * ���Լӱ��������ɹ����Ϸ���
 * @param lockMode
 * @param tabId
 * @return 
 */
bool TNTTransaction::tryLockTable(TLockMode lockMode, TableId tabId) {
    TLockSys *lockSys = m_tranSys->getLockSys();
	assert(NULL != lockSys);
	return lockSys->tryLockTable(m_lockOwner, lockMode, tabId);
}

/**
 * �����񼶱���
 * @param lockMode
 * @param tabId
 * @throws
 */
void TNTTransaction::lockTable(TLockMode lockMode, TableId tabId) throw(NtseException) {
    TLockSys *lockSys = m_tranSys->getLockSys();
	assert(NULL != lockSys);
	m_waitingLock = true;
	m_waitStartTime = System::fastTime();
	DldLockResult result = lockSys->lockTable(m_lockOwner, lockMode, tabId);
	m_waitingLock = false;
	return lockSys->translateErr(result);
}

/**
 * �ͷ�����
 * @param lockMode
 * @param rowId
 * @param tabId
 * @return
 */
bool TNTTransaction::unlockRow(TLockMode lockMode, RowId rowId, TableId tabId) {
	if (m_lastRowLockStat == DLD_LOCK_IMPLICITY) {
		m_lastRowLockStat = DLD_LOCK_SUCCESS;
		return true;
	}

	TLockSys *lockSys = m_tranSys->getLockSys();
	assert(NULL != lockSys);
	return lockSys->unlockRow(m_lockOwner, lockMode, rowId, tabId);
}

/**
 *  �ͷű���
 * @param lockMode
 * @param tabId
 * @return
 */
bool TNTTransaction::unlockTable(TLockMode lockMode, TableId tabId) {
	TLockSys *lockSys = m_tranSys->getLockSys();
	assert(NULL != lockSys);
	return lockSys->unlockTable(m_lockOwner, lockMode, tabId);
}

/**
 * ���Լ����������������ɹ����Ϸ���
 * @param lockMode
 * @param autoIncr
 * @return 
 */
bool TNTTransaction::tryLockAutoIncr(TLockMode lockMode, u64 autoIncr) {
	TLockSys *lockSys = m_tranSys->getLockSys();
	assert(NULL != lockSys);
	return lockSys->tryLockAutoIncr(m_lockOwner, lockMode, autoIncr);
}

/**
 * ������������
 * @param lockMode
 * @param autoIncr
 */
void TNTTransaction::lockAutoIncr(TLockMode lockMode, u64 autoIncr) throw(NtseException) {
	TLockSys *lockSys = m_tranSys->getLockSys();
	assert(NULL != lockSys);
	lockSys->lockAutoIncr(m_lockOwner, lockMode, autoIncr);
}

/**
 * �ͷ�����������
 * @param lockMode
 * @param autoIncr
 * @return 
 */
bool TNTTransaction::unlockAutoIncr(TLockMode lockMode, u64 autoIncr) {
	TLockSys *lockSys = m_tranSys->getLockSys();
	assert(NULL != lockSys);
	return lockSys->unlockAutoIncr(m_lockOwner, lockMode, autoIncr);
}

/**
 * �ͷ�����ͬһ���������Lock
 * �����ύ��ع�ʱʹ��
 */
void TNTTransaction::releaseLocks() {
	TLockSys *lockSys = m_tranSys->getLockSys();
	assert(NULL != lockSys);
	lockSys->releaseLockList(m_lockOwner);
}

/**
 * дTNT��־
 * @param logType ��־����
 * @param tableId ��ID
 * @param data ��־����
 * @param size ��־���ݴ�С
 * @return ��־LSN�����Ự����Ϊ��д��־�򷵻�INVALID_LSN
 */
LsnType TNTTransaction::writeTNTLog(LogType logType, u16 tableId, byte *data, size_t size) {
	assert(m_valid);
	if (!m_logging)
		return INVALID_LSN;
	assert_always(!TableDef::tableIdIsTemp(tableId));
	if (m_readOnly) {
		m_readOnly = false;
		LsnType lsn = writeBeginTrxLog();
		if (lsn != INVALID_LSN) {
			m_beginLSN = lsn;
			markSqlStatEnd();

			//���ڻ�δдbegin��־������д������־��preLsn��Ҫ���и�д
			Stream s(data, size);
			s.skip(sizeof(TrxId));
			s.write(lsn);
		}
	}

	if (logType > LOG_TNT_TRX_MAX) {
		assert(logType < LOG_TNT_MAX);
		m_redoCnt++;
	}
	
	m_lastLSN = m_tntLog->log(0, logType, tableId, data, size);
	return m_lastLSN;
}

/**
 * ˢдTNT������־
 * @param lsnType ��־LSN
 */
void TNTTransaction::flushTNTLog(LsnType lsn, FlushSource fs) {
	assert(m_valid);
	if (m_flushMode == TFM_NOFLUSH) {
		return;
	}
	m_tntLog->flush(lsn, fs);
}

/**
 * д��ʼ������־
 * @return 
 */
LsnType TNTTransaction::writeBeginTrxLog() {
	return doWritelog(TNT_BEGIN_TRANS, m_trxId, INVALID_LSN, m_versionPoolId);
}

/**
 * д��ʼ�ع�������־
 * @return 
 */
LsnType TNTTransaction::writeBeginRollBackTrxLog() {
	return doWritelog(TNT_BEGIN_ROLLBACK_TRANS, m_trxId, m_lastLSN);
}

/**
 * д�ع�������־
 * @return 
 */
LsnType TNTTransaction::writeEndRollBackTrxLog() {
	return doWritelog(TNT_END_ROLLBACK_TRANS, m_trxId, m_lastLSN);
}

/**
 * д�ύ������־
 * @return 
 */
LsnType TNTTransaction::writeCommitTrxLog() {
	return doWritelog(TNT_COMMIT_TRANS, m_trxId, m_lastLSN);
}

/**
 * д׼��������־
 * @return 
 */
LsnType TNTTransaction::writePrepareTrxLog() {
	assert(m_memctx);
	McSavepoint msp(m_memctx);

	size_t bufSize = sizeof(TrxId) + sizeof(LsnType) + sizeof(XID);
	byte *buf = (byte *)m_memctx->alloc(bufSize);
	Stream s(buf, bufSize);
	s.write(m_trxId);
	assert(m_lastLSN != INVALID_LSN);
	s.write(m_lastLSN);
	s.write((byte *)&m_xid, sizeof(XID));
	
	return writeTNTLog(TNT_PREPARE_TRANS, TableDef::INVALID_TABLEID, buf, s.getSize());
}

/** дpartial begin rollback ��־
 * @param rollBackLsn ��Ҫ�ع���lsn
 */
LsnType TNTTransaction::writePartialBeginRollBackTrxLog() {
	assert(m_lastLSN != INVALID_LSN);
	return doWritelog(TNT_PARTIAL_BEGIN_ROLLBACK, m_trxId, m_lastLSN);
}

/** дpartial end rollback��־*/
LsnType TNTTransaction::writePartialEndRollBackTrxLog() {
	assert(m_lastLSN != INVALID_LSN);
	return doWritelog(TNT_PARTIAL_END_ROLLBACK, m_trxId, m_lastLSN);
}

/**
 * ����begin������־
 * @param log ��־����
 * @param trxId ����id
 * @param versionPoolId �汾��id
 */
void TNTTransaction::parseBeginTrxLog(const LogEntry *log, TrxId *trxId, u8 *versionPoolId) {
	assert(log->m_logType == TNT_BEGIN_TRANS);
	Stream s(log->m_data, log->m_size);
	s.read(trxId);
	s.read(versionPoolId);
	assert(*versionPoolId != INVALID_VERSION_POOL_INDEX);
}

/**
 * ����׼��������־
 * @param log
 * @param trxId
 * @param preLsn
 * @param xid
 */
void TNTTransaction::parsePrepareTrxLog(const LogEntry *log, TrxId *trxId, LsnType *preLsn, XID *xid) {
	assert(log->m_logType == TNT_PREPARE_TRANS);
	Stream s(log->m_data, log->m_size);
	s.read(trxId);
	s.read(preLsn);
	s.readBytes((byte *)xid, sizeof(XID));
}

/** д������־
 * @param logType ��־����
 * @param trxId ����id
 * @param preLsn ͬһ����ǰһ����־��lsn
 * return ��־���к�
 */
LsnType TNTTransaction::doWritelog(LogType logType, TrxId trxId, LsnType preLsn, u8 versionPoolId) {
	assert(m_memctx);
	McSavepoint msp(m_memctx);

	size_t bufSize = sizeof(TrxId) + sizeof(LsnType);
	byte *buf = (byte *)m_memctx->alloc(bufSize);
	Stream s(buf, bufSize);
	s.write(trxId);
	if (preLsn != INVALID_LSN) {
		s.write(preLsn);
	}
	if (versionPoolId != INVALID_VERSION_POOL_INDEX) {
		s.write(versionPoolId);
	}

	return writeTNTLog(logType, TableDef::INVALID_TABLEID, buf, s.getSize());
}

/**
 * �ж��Ƿ���Ҫд������־
 * @return 
 */
bool TNTTransaction::isLogging() const {
	return m_logging;
}

/**
 * ���ò���Ҫд������־
 */
void TNTTransaction::disableLogging() {
	m_logging = false;
}

/**
 * ������Ҫд������־
 */
void TNTTransaction::enableLogging() {
	m_logging = true;
}

char *TNTTransaction::getTrxStateDesc(TrxState stat) {
	switch (stat) {
		case TRX_NOT_START:
			return "TRX_NOT_START";
		case TRX_ACTIVE:
			return "TRX_ACTIVE";
		case TRX_PREPARED:
			return "TRX_PREPARED";
		case TRX_COMMITTED_IN_MEMORY:
			return "TRX_COMMITTED_IN_MEMORY";
		default:
			return NULL;
	}
}


///////////////////////////////////////////////////////////////////////////////////////////////

/**
 * TNTTrxSys���캯��
 * @param maxTrxNum ���������Ծ������
 */
TNTTrxSys::TNTTrxSys(TNTDatabase *db, uint maxTrxNum, TrxFlushMode trxFlushMode, int lockTimeoutMs) 
	: m_transMutex("TNT Transaction System Mutex", __FILE__, __LINE__), 
	m_maxTrxNum(maxTrxNum), m_trxFlushMode(trxFlushMode), m_db(db), m_freeTrxPool(db->getCommonMemPool(), PAGE_COMMON_POOL) {
		// FIXME: ��ǰ�������ID����������
		m_maxTrxId = (TrxId)1;
		m_lockSys = new TLockSys(1024 * 1024, lockTimeoutMs);
		m_activeVerPoolId = 0;
		memset(&m_stat, 0, sizeof(TNTTrxSysStat));
		m_trxCnt = 0;
		m_totalLockCnt = 0;
		m_totalRedoCnt = 0;
		m_recoverLastTrxPos = NULL;
		m_hasHangTrx = false;
}

TNTTrxSys::~TNTTrxSys() {
	assert(m_activeTrxs.getSize() == 0);
	assert(m_activeInnerTrxs.getSize() == 0);
	assert(m_activeReadViews.getSize() == 0);
	assert(m_freeMemCtxList.getSize() <= m_maxTrxNum);

	while (m_freeMemCtxList.getSize() > 0) {
		DLink<MemoryContext*> *link = m_freeMemCtxList.removeLast();
		MemoryContext *memCtx = link->get();
		memCtx->reset();
		memCtx->~MemoryContext();
		delete [] (byte*)memCtx;
	}

	m_freeTrxPool.clear();

	delete m_lockSys;
}

/**
 * ����һ��������
 * @param trxId	������ID����crash recover�����У���ָ������ID
 * @return �ɹ����������
 * @throw NtseException �ڴ治��
 */
TNTTransaction* TNTTrxSys::allocTrx(TrxId trxId) throw(NtseException) {
	MutexGuard mutexGuard(&m_transMutex, __FILE__, __LINE__);
	return doAllocTrx(trxId);
}

/**	
 * ����һ��������
 * @return �ɹ����������
 * @throw NtseException �ڴ治��
 */
TNTTransaction* TNTTrxSys::allocTrx() throw(NtseException) {
	MutexGuard mutexGuard(&m_transMutex, __FILE__, __LINE__);
	return doAllocTrx(INVALID_TRX_ID);
}

/**
 * ����һ�����������ʵ��
 * @param trxId	������ID����crash recover�����У���ָ������ID
 * @return �ɹ����������
 * @throw NtseException �ڴ治��
 */
TNTTransaction* TNTTrxSys::doAllocTrx(TrxId trxId) throw(NtseException) {
	assert(m_transMutex.isLocked());

	if (m_activeTrxs.getSize() + m_activeInnerTrxs.getSize() < m_maxTrxNum) {
		// �����������з����������
		size_t poolId = m_freeTrxPool.alloc();
		TNTTransaction *trx = &m_freeTrxPool[poolId];
		MemoryContext *memCtx = NULL;
		if (m_freeMemCtxList.getSize() > 0) {// ���ڿ��е��ڴ����������
			memCtx = m_freeMemCtxList.removeLast()->get();
		} else {			
			byte *data = new byte[sizeof(MemoryContext) + sizeof(DLink<MemoryContext*>)];
			memCtx = new (data)MemoryContext(m_db->getCommonMemPool(), 1);
			// �ڴ���������Ķ���֮������Ŵ������������Щ�����������
			data += sizeof(MemoryContext);
			DLink<MemoryContext*> *link = new (data)DLink<MemoryContext*>(memCtx);
			UNREFERENCED_PARAMETER(link);
		}
		assert(NULL != m_db);
		trx->init(this, m_db->getTNTLog(), memCtx, trxId, poolId);
		return trx;
	} else {
		NTSE_THROW(NTSE_EC_TOO_MANY_TRX, "Too many active transactions currently!\n");
	}
}

/**
 * ����һ������
 * @param trx
 */
void TNTTrxSys::freeTrx(TNTTransaction *trx) {
	MutexGuard mutexGuard(&m_transMutex, __FILE__, __LINE__);
	
	if (NULL != trx->getGlobalReadView()) {// �ر���ͼ
		trx->getGlobalReadView()->excludeFromList();
		trx->setGlobalReadView(NULL);
	}
	if (trx->getTrxState() != TRX_NOT_START) {
		trx->excludeFromTrxList();
	}

	assert(trx && trx->m_valid);
	MemoryContext *memCtx = trx->getMemoryContext();
	m_freeTrxPool.free(trx->m_trxPoolId);
	trx->destory();
	trx = NULL;

	memCtx->reset();
	DLink<MemoryContext*> *link = (DLink<MemoryContext*>*)((byte*)memCtx + sizeof(MemoryContext));
	m_freeMemCtxList.addLast(link);
}

/**
 * ��ʼһ����δ��ʼ������
 * @param trx
 * @param versionPoolId
 * @return 
 */
bool TNTTrxSys::startTrx(TNTTransaction *trx, bool inner) {
	assert(trx);
	assert(trx->getTrxState() != TRX_ACTIVE);

	MutexGuard mutexGuard(&m_transMutex, __FILE__, __LINE__);
	bool ret = startTrxLow(trx);
	if (inner)
		trx->relatedWithTrxList(&m_activeInnerTrxs);
	else
		trx->relatedWithTrxList(&m_activeTrxs);
	return ret;
}

/**
 * ����prepare
 * @param trx
 */
void TNTTrxSys::prepareTrxLow(TNTTransaction *trx) {
	assert(trx != NULL);
	assert(trx->getTrxState() == TRX_ACTIVE);

	LOCK(&m_transMutex);
	LsnType lsn = INVALID_LSN;
	if (!trx->m_readOnly) {
		lsn = trx->writePrepareTrxLog();
	}

	assert(m_transMutex.isLocked());
	trx->m_trxState = TRX_PREPARED;

	UNLOCK(&m_transMutex);

	if (lsn != INVALID_LSN) {		
		trx->flushTNTLog(lsn, FS_PREPARE);
	}
}

/**
 * �ύ����
 * @param trx
 */
void TNTTrxSys::commitTrxLow(TNTTransaction *trx, CommitStat stat) {
	assert(trx != NULL);

	if (CS_NORMAL == stat) {
		m_stat.m_commit_normal++;
	} else if (CS_INNER == stat) {
		m_stat.m_commit_inner++;
	} 
	LOCK(&m_transMutex);

	// FIXME: ��������Ӧ�ó־û���д��汾�ص���Ϣ, ���ڰ汾�ر����ʹ����NTSE��
	// ����������Բ�����ʲô����
	LsnType lsn = INVALID_LSN;
	if (!trx->m_readOnly) {
		lsn = trx->writeCommitTrxLog();
	}

	// ��������״̬
	assert(TRX_ACTIVE == trx->getTrxState() || TRX_PREPARED == trx->getTrxState());
	assert(m_transMutex.isLocked());
	trx->setTrxState(TRX_COMMITTED_IN_MEMORY);

	if (CS_NORMAL == stat) {
		m_trxCnt++;
		sampleLockAndRedo(trx);
	}

	// �ͷ����е�������
	trx->releaseLocks();

	// �ر���ͼ
	if (NULL != trx->getGlobalReadView()) {
		trx->getGlobalReadView()->excludeFromList();
		trx->setGlobalReadView(NULL);
	}
	trx->setReadView(NULL);

	// ��ˢ������־
	if (lsn != INVALID_LSN) {
		// ������Ҫ�ȴ�IO��������������ȷ���
		UNLOCK(&m_transMutex);

		//�����֧��xa���񣬼�Ϊbegin��commit��û��prepare�����Դ�ʱӦ��flush log
		trx->flushTNTLog(lsn, FS_COMMIT);

		assert(trx->m_beginLSN != INVALID_LSN);
		trx->m_commitLsn = lsn;
		
		LOCK(&m_transMutex);
	}

	// ������ӻ�Ծ�����б����޳�
	trx->excludeFromTrxList();
	UNLOCK(&m_transMutex);

	if (CS_NORMAL == stat) {
		sampleExecuteTime(trx);
	}

	trx->reset();
}

/**
 * ����ع�
 * @param trx Ҫ�ع�������
 * @param session ʹ�õĻỰ�������ΪNULL����ʹ���������ϵͳ�ڲ��ĻỰ
 * @param partial true��ع�������䣬false��ع���������
 * @return �ع������Ƿ�ɹ�
 */
bool TNTTrxSys::rollbackLow(TNTTransaction *trx, Session *session, RollBackStat rollBackStat, bool partial) {
	assert(trx != NULL);
	assert(TRX_NOT_START != trx->getTrxState());
	assert(trx->isLogging() == true);

	bool ret = false;
	bool dummy = false;

	if (partial) {
		if (RBS_TIMEOUT == rollBackStat) {
			m_stat.m_partial_rollback_timeout++;
		} else if (RBS_DEADLOCK == rollBackStat) {
			m_stat.m_partial_rollback_deadlock++;
		} else if (RBS_NORMAL == rollBackStat) {
			m_stat.m_partial_rollback_normal++;
		}
	} else {
		if (RBS_TIMEOUT == rollBackStat) {
			m_stat.m_rollback_timeout++;
		} else if (RBS_DEADLOCK == rollBackStat) {
			m_stat.m_rollback_deadlock++;
		} else if (RBS_INNER == rollBackStat) {
			m_stat.m_rollback_inner++;
	    } else if (RBS_NORMAL == rollBackStat) {
			m_stat.m_rollback_normal++;
		} else if (RBS_DUPLICATE_KEY == rollBackStat) {
			m_stat.m_rollback_duplicate_key++;
		} else if (RBS_ROW_TOO_LONG == rollBackStat) {
			m_stat.m_rollback_row_too_long++;
		} else if (RBS_ABORT == rollBackStat) {
			m_stat.m_rollback_abort++;
		} else if (RBS_OUT_OF_MEMORY == rollBackStat) {
			m_stat.m_rollback_out_of_mem++;
		}

		////////ͳ����Ϣ//////////
		if (RBS_INNER != rollBackStat) {
			m_trxCnt++;
			sampleLockAndRedo(trx);
		}
		/////////////////////////////
	}
	
	if (trx->m_readOnly) {
		if (!partial) {
			LOCK(&m_transMutex);
			finishRollbackAll(false, trx);
			UNLOCK(&m_transMutex);
			if (RBS_INNER != rollBackStat) {
				sampleExecuteTime(trx);
			}
			trx->reset();
		}
		return true;
	}

	if (!session) {
		// ����ڲ�����Ự����δ���䣬���ȷ���֮
		NTSE_ASSERT(trx->m_conn != NULL);
		session = m_db->getNtseDb()->getSessionManager()->allocSession("RollBack Dummy Session", trx->m_conn);
		session->setTrans(trx);
		dummy = true;
	}

	LsnType rollLimit = INVALID_LSN;
	LsnType lsn = INVALID_LSN;
	// partial rollback ��ȫ�� rollbackһ�������ڰ汾�صĻع����в���ع������
	// ��������partial rollback�ᵼ�°汾�ز�����������������޷����գ������ᵼ�´���
	if (partial) {
		rollLimit = trx->m_lastStmtLsn;
		lsn = trx->writePartialBeginRollBackTrxLog();
	} else {
		lsn = trx->writeBeginRollBackTrxLog();//д����ʼ�ع���־
	}
	
	m_db->getVersionPool()->rollBack(session, trx->getVersionPoolId(), trx->getTrxId());
	ret = m_db->undoTrxByLog(session, rollLimit, trx->m_lastLSN);

	LOCK(&m_transMutex);
	if (!partial) {
		finishRollbackAll(ret, trx);
		if (RBS_INNER != rollBackStat) {
			sampleExecuteTime(trx);
		}
		trx->reset();
	} else {
		trx->setTrxLastLsn(rollLimit);
		lsn = trx->writePartialEndRollBackTrxLog();
		if (lsn != INVALID_LSN) {
			trx->flushTNTLog(lsn, FS_ROLLBACK);
		}
		// ���ع��ɹ�֮�󣬲��ܵ���markSqlStatEnd����
		// ֱ�ӱ�����һ�γɹ����õ�m_lastStatementLsn����
		// trx->markSqlStatEnd();
	}
	UNLOCK(&m_transMutex);

	if (dummy) {
		m_db->getNtseDb()->getSessionManager()->freeSession(session);
	}

	return ret;
}

/**
 * ����ع�(ֻ���ָ�ʱʹ��)
 * @param trx
 * @param session
 * @param logs
 * @return 
 */
bool TNTTrxSys::rollbackForRecoverLow(TNTTransaction *trx, Session *session, DList<LogEntry *> *logs) {
	assert(session != NULL);
	assert(trx != NULL);
	assert(TRX_NOT_START != trx->getTrxState());

	trx->startTrxIfNotStarted(session->getConnection());

	bool ret = m_db->undoTrxByLog(session, logs);
	if (logs != NULL) {
		m_db->getVersionPool()->rollBack(session, trx->getVersionPoolId(), trx->getTrxId());
	}

	LOCK(&m_transMutex);

	finishRollbackAll(ret, trx);
	trx->reset();
	m_stat.m_rollback_recover++;

	UNLOCK(&m_transMutex);
	return ret;
}

/**
 * ��ɻع����������Ҫ���Ĺ���
 * @param needLog
 * @param trx
 * @return 
 */
void TNTTrxSys::finishRollbackAll(bool needLog, TNTTransaction *trx) {
	assert(trx != NULL);
	assert(m_transMutex.isLocked());
	
	
	LsnType lsn = INVALID_LSN;
	// �˴���д��־�ٷ���
	if (needLog)
		lsn = trx->writeEndRollBackTrxLog();
	// �ر���ͼ
	if (NULL != trx->getGlobalReadView()) {
		trx->getGlobalReadView()->excludeFromList();
		trx->setGlobalReadView(NULL);
	}
	trx->setReadView(NULL);

	trx->releaseLocks();

	if (needLog) {
		if (lsn != INVALID_LSN) {
			assert(lsn == trx->getTrxLastLsn());
			UNLOCK(&m_transMutex);

			trx->flushTNTLog(lsn, FS_ROLLBACK);

			LOCK(&m_transMutex);
		}
	}
	//������ڻָ��е��ã������ع�lobHash��
	if(trx->getRollbackInsertLobHash() != NULL)
		trx->getRollbackInsertLobHash()->~TblLobHashMap();
	// ������ӻ�Ծ�����б����޳�
	trx->excludeFromTrxList();
}

/**
 * Ϊ�������һ����ͼ
 * @param trx
 * @return ���ط������ͼ
 * @throw NtseException �ڴ治��
 */
ReadView* TNTTrxSys::trxAssignReadView(TNTTransaction *trx) throw(NtseException) {
	assert(trx != NULL);
	assert(TRX_ACTIVE == trx->getTrxState());
	if (NULL == trx->getReadView()) {
		MutexGuard mutexGuard(&m_transMutex, __FILE__, __LINE__);
		if (NULL == trx->getReadView()) {
			ReadView *view = openReadViewNow(trx->getTrxId(), trx->getMemoryContext());
			assert(NULL != view);
 			trx->setReadView(view);
			trx->setGlobalReadView(view);
		}
	}
	return trx->getReadView();
}

/**
 * ΪPurge�������һ����ͼ
 * @param trx
 * @return ���ط������ͼ
 * @throw NtseException �ڴ治��
 */
ReadView* TNTTrxSys::trxAssignPurgeReadView(TNTTransaction *trx) throw(NtseException) {
	assert(trx != NULL);
	assert(TRX_ACTIVE == trx->getTrxState());
	if (NULL == trx->getReadView()) {
		MutexGuard mutexGuard(&m_transMutex, __FILE__, __LINE__);
		if (NULL == trx->getReadView()) {
			ReadView *view = openReadViewNow(trx->getTrxId(), trx->getMemoryContext());
			assert(NULL != view);
 			trx->setReadView(view);
			trx->setGlobalReadView(view);
			//��ȡ��ԾreadView�е���СUpTrxId��Ϊpurge�����upTrxId��lowTrxId
			TrxId miniActiveReadViewTrxId = INVALID_TRX_ID;
			Iterator<ReadView> it(&m_activeReadViews);
			ReadView *readView = NULL;
			while (it.hasNext()) {
				readView = it.next();
				if (readView != NULL && readView->getUpTrxId() < miniActiveReadViewTrxId)
					miniActiveReadViewTrxId = readView->getUpTrxId();
			}
			assert(miniActiveReadViewTrxId != INVALID_TRX_ID);
			view->setUpTrxId(miniActiveReadViewTrxId);
			view->setLowTrxId(miniActiveReadViewTrxId);
		}
	} 
	return trx->getReadView();
}

LsnType TNTTrxSys::getMinStartLsnFromReadViewList() {
	MutexGuard mutexGuard(&m_transMutex, __FILE__, __LINE__);
	LsnType minStartLsn = INVALID_LSN;
	Iterator<ReadView> it(&m_activeReadViews);
	ReadView *readView = NULL;
	while (it.hasNext()) {
		readView = it.next();
		if (readView != NULL && readView->getMinStartLsn() < minStartLsn)
			minStartLsn = readView->getMinStartLsn();
	}
	return minStartLsn;
}


#ifdef NTSE_UNIT_TEST
/**
 * ���õ�ǰ�������ID(ֻ����Ԫ����ʹ��)
 * @param maxTrxId
 * @return 
 */
void TNTTrxSys::setMaxTrxId(TrxId maxTrxId) {
	MutexGuard mutexGuard(&m_transMutex, __FILE__, __LINE__);
	m_maxTrxId = maxTrxId;
}

/**
 * Ϊ���������ͼ(ֻ����Ԫ����ʹ��)
 * @param trx
 * @param trxIds
 * @param trxCnt
 * @return 
 */
ReadView* TNTTrxSys::trxAssignReadView(TNTTransaction *trx, TrxId *trxIds, 
									   uint trxCnt) throw(NtseException) {
    assert(trx != NULL);
    assert(TRX_ACTIVE == trx->getTrxState());
    assert(NULL == trx->getReadView());
	assert(NULL != trxIds && trxCnt > 0);
	TrxId lastTrxId = trxIds[0];
	for (uint i = 1; i < trxCnt; i++) {
		assert(lastTrxId > trxIds[i]);
		lastTrxId = trxIds[i];
	}

	MutexGuard mutexGuard(&m_transMutex, __FILE__, __LINE__);

	ReadView *view = createReadView(trx->getTrxId(), trx->getMemoryContext());
	view->init(0, 0, trxIds, trxCnt);
	assert(NULL != view);
	view->setLowTrxId(m_maxTrxId);

	view->setUpTrxId(trxIds[trxCnt - 1]);

	view->relatedWithList(&m_activeReadViews);

	trx->setReadView(view);
	trx->setGlobalReadView(view);

	return trx->getReadView();
}
#endif

/**
 * �ر�������ͼ
 * @param trx
 */
void TNTTrxSys::closeReadViewForMysql(TNTTransaction *trx) {
	assert(trx != NULL);
	MutexGuard mutexGuard(&m_transMutex, __FILE__, __LINE__);

	ReadView *globalReadView = trx->getGlobalReadView();
	if (globalReadView != NULL) {
		globalReadView->excludeFromList();
		trx->setGlobalReadView(NULL);
	}
	trx->setReadView(NULL);
}

/**
 * ��������ʼ��һ����ͼ
 * @param trxId
 * @param memCtx
 * @return 
 */
ReadView* TNTTrxSys::openReadViewNow(TrxId trxId, MemoryContext *memCtx) throw(NtseException) {
	assert(m_transMutex.isLocked());

	ReadView *readView = createReadView(trxId, memCtx);
	assert(NULL != readView);
	readView->setLowTrxId(m_maxTrxId);
	LsnType minStartLsn = INVALID_LSN;
	uint n = 0;
	TNTTransaction* trx = NULL;
	Iterator<TNTTransaction> it(&m_activeTrxs);
	while (it.hasNext()) {
		trx = it.next();
		if (trx->getTrxId() != trxId 
			&& (trx->getTrxState() == TRX_ACTIVE
			|| trx->getTrxState() == TRX_PREPARED)) {
				readView->setNthTrxIdViewed(n, trx->getTrxId());
				++n;

				if (trx->getTrxRealStartLsn() < minStartLsn)
					minStartLsn = trx->getTrxRealStartLsn();
		}
	}
	readView->setMinStartLsn(minStartLsn);
	readView->setTrxCnt(n);
	readView->setUpTrxId(n > 0 ? readView->getNthTrxIdViewed(n - 1) : readView->getLowTrxId());
	readView->relatedWithList(&m_activeReadViews);

	return readView;
}

/**
 * ����һ����ͼ����
 * @param trxId
 * @param memCtx
 * @return 
 */
ReadView* TNTTrxSys::createReadView(TrxId trxId, MemoryContext *memCtx) throw(NtseException) {
	assert(m_transMutex.isLocked());

	uint activeTrxNum = m_activeTrxs.getSize();
	TrxId *trxIds = (TrxId *)memCtx->alloc(sizeof(TrxId) * activeTrxNum);
	void *data = memCtx->alloc(sizeof(ReadView));
	if (trxIds && data) {
		return new (data)ReadView(trxId, trxIds, activeTrxNum);
	} else {
		NTSE_THROW(NTSE_EC_OUT_OF_MEM, "There is not enough memory left for creating read view!\n");
	}
}

/**
 * ��ȡ��ǰ��Ծ�����д���TRX_PREPARED״̬�������XID�б�
 * @param list INOUT ���Ϊ����TRX_PREPARED״̬�������XID�б��ڴ��ɵ��÷�����ò�ͨ��lenָ������󳤶�
 * @param len list�������󳤶�
 * @return ����TRX_PREPARED״̬���������
 */
uint TNTTrxSys::getPreparedTrxForMysql(XID* list, uint len) {
	MutexGuard mutexGuard(&m_transMutex, __FILE__, __LINE__);
	
	uint n = 0;
	if (m_recoverLastTrxPos == m_activeTrxs.getHeader()) {
		return n;
	} else if (m_recoverLastTrxPos == NULL) {
		m_recoverLastTrxPos = m_activeTrxs.getHeader()->getNext();
	}

	while (n < len && m_recoverLastTrxPos != m_activeTrxs.getHeader()) {
		TNTTransaction *trx = m_recoverLastTrxPos->get();
		if (TRX_PREPARED == trx->getTrxState()) {
			list[n++] = *(trx->getTrxXID());
		}

		//��m_recoverLastTrxPos�Ƶ���һ��prepare������
		while ((m_recoverLastTrxPos = m_recoverLastTrxPos->getNext())
			!= m_activeTrxs.getHeader()) {
				if (TRX_PREPARED == m_recoverLastTrxPos->get()->getTrxState()) {
					break;
				}
		}
	}
	return n;
}

void TNTTrxSys::setMaxTrxIdIfGreater(TrxId curTrxId) {
	MutexGuard mutexGuard(&m_transMutex, __FILE__, __LINE__);
	if (curTrxId >= m_maxTrxId)
		m_maxTrxId = curTrxId + 1;
}

/**
 * ��ȡ��ǰ���л�Ծ��������С������ID(��defragHash��reclaimLobʹ��)
 * @return 
 */
TrxId TNTTrxSys::findMinReadViewInActiveTrxs() {
	MutexGuard mutexGuard(&m_transMutex, __FILE__, __LINE__);

	TrxId minReadView = INVALID_TRX_ID;
	Iterator<ReadView> it(&m_activeReadViews);
	ReadView *readView = NULL;
	while (it.hasNext()) {
		readView = it.next();
		if (readView != NULL && readView->getUpTrxId() < minReadView)
			minReadView = readView->getUpTrxId();
	}
	return minReadView;
}

TrxId TNTTrxSys::getMaxDumpTrxId() {
	MutexGuard mutexGuard(&m_transMutex, __FILE__, __LINE__);
	return  getNewTrxId();
}


/**
 * ����XID��ȡ��Ӧ������xid��Ӧ������һ������prepare״̬
 * @param xid
 * @return
 */
TNTTransaction* TNTTrxSys::getTrxByXID(XID* xid) {
	MutexGuard mutexGuard(&m_transMutex, __FILE__, __LINE__);

	if (NULL != xid) {
		TNTTransaction *trx = NULL;
		
		Iterator<TNTTransaction> it(&m_activeTrxs);
		while (it.hasNext()) {
			trx = it.next();
			const XID *trxXid = trx->getTrxXID();
			if (xid->gtrid_length == trxXid->gtrid_length
				&& xid->bqual_length == trxXid->bqual_length
				&& memcmp(xid->data, trxXid->data, 
					xid->gtrid_length + xid->bqual_length) == 0) {
						break;
			} else {
				trx = NULL;
			}
		}
		
		if (NULL != trx && trx->getTrxState() == TRX_PREPARED) {
			//�����������recover���prepare����
			if (trx->m_hangByRecover == true) {
				trx->m_hangByRecover = false;
				trx->m_hangTime = 0;
			}
			return trx;
		}
	}
	return NULL;
}

/** ��ʶ���е�hang prepare���� */
void TNTTrxSys::markHangPrepareTrxAfterRecover() {
	u32 now = System::fastTime();
	MutexGuard mutexGuard(&m_transMutex, __FILE__, __LINE__);

	TNTTransaction *trx = NULL;
	Iterator<TNTTransaction> it(&m_activeTrxs);
	while (it.hasNext()) {
		trx = it.next();
		if (TRX_PREPARED == trx->getTrxState()) {
			trx->m_hangByRecover = true;
			trx->m_hangTime = now;
			m_hasHangTrx = true;
		} else {
			NTSE_ASSERT(false);
		}
	}
}

/** kill recover��hang��prepare���� */
void TNTTrxSys::killHangTrx() {
	if (!m_hasHangTrx) {
		return;
	}

	u32 now = System::fastTime();
	bool hasHangTrx = false;
	m_transMutex.lock(__FILE__, __LINE__);
	TNTTransaction *trx = NULL;
	Iterator<TNTTransaction> it(&m_activeTrxs);
	while (it.hasNext() && !trx) {
		trx = it.next();
		if (trx->m_hangByRecover) {
			NTSE_ASSERT(TRX_PREPARED == trx->m_trxState);
			if (now - trx->m_hangTime >= m_db->getTNTConfig()->m_maxTrxPrepareIdleTime) {
				trx->m_hangTime = 0;
				trx->m_hangByRecover = false;
				//��xid�ÿգ������ɵ���getTrxByXID����null�����Ⲣ��
				memset(&trx->m_xid, 0, sizeof(XID));
			} else {
				//��hang������ʱ����Ҫrollback
				trx = NULL;
			}
			//����hang����
			hasHangTrx = true;
		} else {
			//trx������hang�������Կ϶�����Ҫrollback
			trx = NULL;
		}
	}
	m_transMutex.unlock();

	if (trx != NULL) {
		Connection *conn = m_db->getNtseDb()->getConnection(false, __FUNC__);
		Session *session = m_db->getNtseDb()->getSessionManager()->allocSession(__FUNC__, conn);
		trx->setConnection(conn);
		session->setTrans(trx);
		trx->rollbackTrx(RBS_ABORT, session);
		freeTrx(trx);
		m_db->getNtseDb()->getSessionManager()->freeSession(session);
		m_db->getNtseDb()->freeConnection(conn);
	}
	
	m_hasHangTrx = hasHangTrx;
}

/**
 * ��õ�ǰ��Ծ�����е���С��LSN
 * @return 
 */
LsnType TNTTrxSys::getMinTrxLsn() {
	MutexGuard mutexGuard(&m_transMutex, __FILE__, __LINE__);

	LsnType minTrxLsn = INVALID_LSN;
	TNTTransaction *trx = NULL;
	Iterator<TNTTransaction> itOuter(&m_activeTrxs);
	while (itOuter.hasNext()) {
		trx = itOuter.next();
		LsnType lsn = trx->m_beginLSN;
		if (lsn != INVALID_LSN && lsn < minTrxLsn)
			minTrxLsn = lsn;
	}

	Iterator<TNTTransaction> itInner(&m_activeInnerTrxs);
	while (itInner.hasNext()) {
		trx = itInner.next();
		LsnType lsn = trx->m_beginLSN;
		if (lsn != INVALID_LSN && lsn < minTrxLsn)
			minTrxLsn = lsn;
	}
	return minTrxLsn;
}

void TNTTrxSys::getActiveTrxIds(std::vector<TrxId> *activeTrxsArr) {
	MutexGuard mutexGuard(&m_transMutex, __FILE__, __LINE__);

	TNTTransaction *trx0 = NULL;
	Iterator<TNTTransaction> it(&m_activeTrxs);

	while (it.hasNext()) {
		trx0 = it.next();
		if (trx0->getTrxState() == TRX_ACTIVE 
			|| trx0->getTrxState() == TRX_PREPARED) {
				activeTrxsArr->push_back(trx0->getTrxId());
		}
	}
}

bool TNTTrxSys::isTrxActive(TrxId trxId) {
	MutexGuard mutexGuard(&m_transMutex, __FILE__, __LINE__);

	// ���������Ծ�����б�
	DLink<TNTTransaction*> *header = m_activeTrxs.getHeader();
	DLink<TNTTransaction*> *it = NULL;
	for (it = header->getPrev(); it != header; it = it->getPrev()) {
		TNTTransaction *trx0 = it->get();
		if (trxId < trx0->getTrxId()) {
			//���ڻ�Ծ�����б��ǰ�����ID�Ž������У����Բ������ٴ��ڸ�Ҫ���ҵ�����ID��ȵ�����
			return false;
		} else if (trxId == trx0->getTrxId() 
			&& (trx0->getTrxState() == TRX_ACTIVE || trx0->getTrxState() == TRX_PREPARED)) { 
				return true;
		}
	}

	return false;
}

DList<TNTTransaction *> *TNTTrxSys::getActiveTrxs() {
	assert(m_transMutex.isLocked());
	return &m_activeTrxs;
}

DList<TNTTransaction *> *TNTTrxSys::getActiveInnerTrxs() {
	assert(m_transMutex.isLocked());
	return &m_activeInnerTrxs;
}

/**
 * ��ʼ����
 * @return 
 */
bool TNTTrxSys::startTrxLow(TNTTransaction* trx) {
	assert(m_transMutex.isLocked());

	//purge����������һ������ţ�����ָ���ʱ�򣬶���û����ŵ�begin��־��Ҫ���⴦��
	//����purge������������ֻ�ܽ�Լһ������id�ͼ���һ�η���汾�أ�������۲��Ǻܴ󣬶���purge�����ܵ�Ҫ���Ǻܸ�
	/*if (trx->m_isPurge) {
		trx->m_trxId = INVALID_TRX_ID;
		trx->m_trxState = TRX_ACTIVE;
		trx->m_beginTime = System::fastTime();
		return true;
	}*/
	
	assert(trx->m_trxState != TRX_ACTIVE);

	if (trx->m_trxId == INVALID_TRX_ID) {
		trx->m_trxId = getNewTrxId();
		trx->m_lockOwner->m_txnId = trx->m_trxId;
	}
	trx->m_trxState = TRX_ACTIVE;
	trx->m_beginTime = System::fastTime();
	trx->m_trxActiveStat = 1;
	trx->m_insertLobs = NULL;

	if (INVALID_VERSION_POOL_INDEX == trx->m_versionPoolId) {
		trx->setVersionPoolId(assignVersionPool());
	}

	// ÿ�ο�ʼ�µ����񣬾���Ҫ����������־��Flush����
	trx->m_flushMode = m_trxFlushMode;

	trx->m_realStartLsn = trx->m_tntLog->tailLsn();

	return true;
}

/**
 * ����һ���汾��ID
 * @return 
 */
u8 TNTTrxSys::assignVersionPool() {
	assert(m_transMutex.isLocked());
	return (u8)m_activeVerPoolId;
}

/**
 * ��ȡһ���µ�����ID
 * @return 
 */
TrxId TNTTrxSys::getNewTrxId() {
	assert(m_transMutex.isLocked());
	TrxId id = m_maxTrxId++;
	return id;
}

/** �ɼ������lock��redo��Ϣ
 * @param trx ��Ҫ�ɼ����������
 */
void TNTTrxSys::sampleLockAndRedo(TNTTransaction *trx) {
	u32 lockCnt = trx->m_lockOwner->getHoldingList()->getSize();
	if (lockCnt > m_stat.m_maxLockCnt) {
		m_stat.m_maxLockCnt = lockCnt;
	}
	m_totalLockCnt += lockCnt;

	if (trx->m_redoCnt > m_stat.m_maxRedoCnt) {
		m_stat.m_maxRedoCnt = trx->m_redoCnt;
	}
	m_totalRedoCnt += trx->m_redoCnt;
}

/** �ɼ�����ִ��ʱ����Ϣ
 * @param trx ��Ҫ�ɼ����������
 */
void TNTTrxSys::sampleExecuteTime(TNTTransaction *trx) {
	assert(trx->m_beginTime > 0);
	u32 nowTime = System::fastTime() - trx->m_beginTime;
	if (nowTime > m_stat.m_maxTime) {
		m_stat.m_maxTime = nowTime;
	}
}
}