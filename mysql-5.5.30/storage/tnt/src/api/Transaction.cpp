/**
 * ����ģ��ʵ��
 *
 * @author ��Դ(wangyuan@corp.netease.com, wy@163.org)
 */

#include <string>
#include "api/Transaction.h"
#include "api/Database.h"
#include "api/Table.h"
#include "heap/Heap.h"
#include "btree/Index.h"
#include "misc/Session.h"
#include "mms/Mms.h"
#include "lob/Lob.h"
#include "misc/ControlFile.h"

/*
= NTSE����ָ�����˵��

����˵��NTSE������ָ����ƣ���ν����ָ�ָ�����������־ͨ�����ڱ�ʶ��ĳĳ����ҳ������ĳĳ���£������Ƕ�
���ݿ��е�ĳ���߼���������������ʶ��ĳһ��¼������ʲô���¡���֮��Եģ�MySQL��binlog��һ���߼��ָ����ƣ�
ֻҪ���ݿ�ײ�ͨ������ָ����Ʊ�֤�����ݵ������ԣ��߼��ָ�����ͨ��������һ����ײ��޹صķ�ʽ���ϲ�ʵ�֡�

���������˵��ֻ�漰����ָ���Ϊ�������ϵļ�࣬����ֻ��NTSE�Ļָ����ƣ���ȥ������֣���ʵ��ָ��������ָ�
���ơ�

== ����˵��

NTSE�Ļָ�����ֻ֧���������ָֻ�:
1. �����ϵͳ������Ļָ����ָ�ʱ�ĳ�ʼ״̬�Ǳ�����һ�̵�״̬��
2. ���ݱ��ݻָ�ʱ�����ڱ��ݵ����ݺ��뱸�������׵���־���лָ���

���ﲻ֧�ֽ��ʹ��ϵĻָ���Ҳ����˵����һ�����ϵ����ݿ�Ϊ������ͨ��Ӧ�ú���������־�ָ�������ָ����ʱ���
�ǲ��еģ����������������
1. NTSE�Ļָ����ƽ��޷�֧��ͨ���鵵��־�ķ�ʽʵ���������ݣ�
2. NTSE���ָ������޷�֧��ͨ��log-shipping�ķ�ʽʵ�����ӱ��ݣ�

��������Ϊ����������ǿ��Խ��ܵģ���������ԭ��
1. NTSE��Ҫ��ΪMySQL�Ĵ洢����ʹ�ã���ʱ�������ݺ����ӱ��ݶ�ͨ��MySQL�ṩ��binlog����ʵ���ˣ�
2. ��ʹNTSEҪ����ʵ�������������ܣ�ͨ���߼��ָ��������ϲ�Ϳ���ʵ�֣����߼��ָ���ʵ��ͨ�����򵥣����׳���

���ƻָ�ֻ֧���������ָֻ��ĺô��ǿ��ܼ򻯻ָ���ʵ�֣����籸����DDL�����Ǳ���ֹ������ڻָ�ʱ����Ҫ���ǣ�
������ʱ���ȷ����ID��д����־������ڻָ�ʱ��������־ʱ�������ļ�һ���Ѿ������³�Ҫ���ı��ID�Ѿ�����
��ȥ��״̬�� 

*/

using namespace std;

namespace ntse {

static LogRecord* copyLog(Session *session, LsnType lsn, const LogEntry *log);

/** ����DML������ʹ������ָ����߼��ָ����ϵķ������лָ���һ����¼�����Զ���������޸�
 * ��LOG_IDX_DMLDONE_IDXNO��־���ֳɶ�����֡����޸�Ψһ���������޸ķ�Ψһ������������IUD����
 * ��Ҫ�������ǻع�ȡ���ڶ�Ψһ���������޸��Ƿ���ɡ�
 * �ָ��߼�����:
 * 1. ���Ψһ����������û����ɣ����������UNDO�����ع�����
 * 2. ���Ψһ�����������Ѿ���ɣ���������UNDO������һ��ķ�Ψһ��������Ȼ���߼�REDOʣ�µ�����
 *    ���������ύ����
 *
 * ����а���Ψһ������U1, U2, ..., Um����Ψһ������I1, I2, ..., In������������²�������־����:
 * DML_BEGIN
 * DML_XXX+
 * DMLDONE_IDXNO(U1)
 * DML_XXX+
 * DMLDONE_IDXNO(U2)
 * ...
 * DML_XXX+
 * DMLDONE_IDXNO(Um)
 * DML_XXX+
 * DMLDONE_IDXNO(I1)
 * DML_XXX+
 * DMLDONE_IDXNO(I2)
 * ...
 * DML_XXX+
 * DMLDONE_IDXNO(In)
 * DML_END
 *
 * ����ڲ���Ψһ������������ʧ�ܣ����������UNDOʱ��������Ĳ�����־�����в�����LOG_IDX_DMLDONE_IDXNO
 * ��Ӧ�Ĳ�����־����������µĴ���򵥡�
 *
 * ����ڲ���ĳ��Ψһ������������ʧ�ܣ����Ի�û����ɴ���������Ƚ�������UNDO��Ȼ����
 * �����߼�REDO���߼�REDO��������־���������������²�������־����ͬ�ģ���˶�ĳ����Ψһ
 * ������Ik������������־Ƭ��
 * DML_XXX+
 * DMLDONE_IDXNO(Ik)
 * �ڻָ�����ܱ��
 * DML_XXX[n]
 * DML_XXX_CPST[n]
 * DML_XXX+
 * DMLDONE_IDXNO(Ik)
 * ������߼�REDO�������ֱ�������ָ����ֻ�����һ��
 * DML_XXX[n]
 * DML_XXX_CPST[n]
 * ��־Ƭ�ϣ���������־Ƭ�Ͽ��ܻ�����������
 *
 * ���κ�����£����۱����ָ��˶��ٴΣ�DMLDONE_IDXNO��־�ĸ����������Ѿ�������ɵ���������
 */
class IdxDml {
public:
	IdxDml(Table *table, Session *session, u64 beginLsn);
	virtual ~IdxDml();
	void redo(LsnType lsn, const LogEntry *log);
	bool physicalUndo();
	virtual void logicalRedo() = 0;

private:
	void appendLog(LsnType lsn, const LogEntry *log);
	void popLog();
	void clearLog();
	LogRecord* lastLog();
	void undoLog(LogEntry *log, LsnType lsn);

protected:
	Table				*m_table;	/** �����ı� */
	DrsIndice			*m_indice;	/** ���� */
	TableDef			*m_tableDef;/** ���� */
	Session				*m_session;	/** �Ự���� */
	u64					m_beginLsn;	/** LOG_IDX_DML_BEGIN��־��LSN */
	u16					m_tableId;	/** �����ı�ID */
	u16					m_idxRemain;/** δ������������� */
	u16					m_uniqueRemain;	/** δ�����Ψһ���������� */
	s16					m_lastIdx;	/** ���һ���Ѿ�������������� */
	DList<LogRecord *>	m_idxLogs;	/** ����������־ */
};

/** ����������� */
class IdxInsert: public IdxDml {
public:
	IdxInsert(Table *table, Session *session, u64 beginLsn, const Record *record);
	virtual void logicalRedo();

private:
	const Record	*m_record;	/** ����ļ�¼��ΪRED_REDUNDANT��ʽ */
};

/** �������²��� */
class IdxUpdate: public IdxDml {
public:
	IdxUpdate(Table *table, Session *session, u64 beginLsn, PreUpdateLog *puLog);
	virtual void logicalRedo();

private:
	const SubRecord	*m_before;	/** ��������ǰ�� */
	SubRecord		*m_update;	/** ���µ��������Ժ��� */
	PreUpdateLog	*m_puLog;	/** Ԥ������־ */
	u16				m_idxTotal;	/** �����ܹ����漰���ٸ����� */
	u16				m_updateIdxNo[Limits::MAX_INDEX_NUM];	/** ������Ҫ���µ�������� */
};

/** ����ɾ������ */
class IdxDelete: public IdxDml {
public:
	IdxDelete(Table *table, Session *session, u64 beginLsn, const SubRecord *before);
	virtual void logicalRedo();

private:
	const SubRecord	*m_before;	/** ��������ǰ�� */
};

/** �����¼����
 * ��������µ���־��������:
 *   ��ѡ�Ĵ���������־��������LOG_LOB_INSERT��LOG_HEAP_INSERT(�����)����
 *   LOG_HEAP_INSERT: ���������¼
 *   ������־����
 *
 *   ����ʱ
 *     LOG_HEAP_DELETE: ɾ�������¼
 *     ��ѡ��ɾ������󣬿�����LOG_LOB_DELETE��LOG_HEAP_DELETE(�����)����
 *   LOG_TXN_END
 */
class InsertTrans: public Transaction {
public:
	InsertTrans(Database *db, u16 id, Table *table): Transaction(db, id, table) {
		ftrace(ts.recv, tout << db << id << table->getPath());
		m_tableDef = m_table->getTableDef(true, m_session);
		m_record.m_rowId = INVALID_ROW_ID;

		//���ڶ���ѹ����������redoʱ���ܻ��ѹ����¼���н�ѹ�������Ա����ѹ����¼��ȡ����������
		m_cprsRcdExtractor = m_tableDef->m_isCompressedTbl ? table->getRecords()->getRowCompressMng() : NULL;

		m_record.m_format = m_tableDef->m_recFormat;
		m_record.m_data = (byte *)m_session->getMemoryContext()->alloc(m_tableDef->m_maxRecSize);
		m_record.m_size = m_tableDef->m_maxRecSize;
		m_session->startTransaction(TXN_INSERT, m_tableDef->m_id, false);

		u16 numLobCols = m_tableDef->getNumLobColumns();
		if (numLobCols) {
			m_lobIds = (LobId *)m_session->getMemoryContext()->alloc(sizeof(LobId) * numLobCols);
			memset(m_lobIds, 0, sizeof(LobId) * numLobCols);
		} else
			m_lobIds = NULL;
		if (m_tableDef->m_indexOnly)
			m_stat = STAT_HEAP_INSERTED;
		else if (m_tableDef->hasLob())
			m_stat = STAT_WAIT_LOB_INSERT;
		else
			m_stat = STAT_WAIT_HEAP_INSERT;
		m_idxFail = false;
		m_idxInsert = NULL;
		m_numLobs = 0;
	}

	virtual ~InsertTrans() {
		// �ڴ涼��MemoryContext�з��䣬����Ҫ�ͷ�
	}

	/** @see Transaction::redo */
	virtual void redo(LsnType lsn, const LogEntry *log) throw(NtseException) {
		ftrace(ts.recv, tout << lsn << log);
		if (m_record.m_rowId != INVALID_ROW_ID) {
			nftrace(ts.recv, tout << "rid: " << m_record.m_rowId);
		}
_start:
		if (m_stat == STAT_WAIT_LOB_INSERT) {	// �����������
			assert(m_numLobs < m_tableDef->getNumLobColumns());
			if (log->m_logType == LOG_LOB_INSERT) {	// ���ʹ����
				assert(log->m_tableId == m_tableId);
				m_lobIds[m_numLobs++] = m_table->getLobStorage()->redoBLInsert(m_session, lsn, log->m_data, (uint)log->m_size);
			} else if (log->m_logType == LOG_HEAP_INSERT) {	// С�ʹ�����Ѳ���
				if (log->m_tableId == TableDef::getVirtualLobTableId(m_tableId))	// С�ʹ����
					m_lobIds[m_numLobs++] = m_table->getLobStorage()->redoSLInsert(m_session, lsn, log->m_data, (uint)log->m_size);
				else {	// ĳЩ�����ΪNULLʱ���ܳ�����һ���
					assert(log->m_tableId == m_tableId);
					m_stat = STAT_WAIT_HEAP_INSERT;
					goto _start;
				}
			} else {	// �����������û�����ʱ���ع�
				assert(log->m_logType == LOG_LOB_DELETE || log->m_logType == LOG_HEAP_DELETE);
				m_stat = STAT_WAIT_LOB_DELETE;
				goto _start;
			}
			if (m_numLobs == m_tableDef->getNumLobColumns())
				m_stat = STAT_WAIT_HEAP_INSERT;
		} else if (m_stat == STAT_WAIT_HEAP_INSERT) {	// �������������ɣ���û�д����
			if (log->m_logType == LOG_HEAP_INSERT) {
				assert(log->m_tableId == m_tableId);
				m_table->getHeap()->redoInsert(m_session, lsn, log->m_data, (uint)log->m_size, &m_record);
				nftrace(ts.recv, tout << t_rec(m_tableDef, &m_record));
				if (m_tableDef->m_numIndice > 0)
					m_stat = STAT_HEAP_INSERTED;
				else
					m_stat = STAT_DONE;
			} else {	// ����������ɺ�Ѽ�¼û�в���ʱ���ع�
				assert(log->m_logType == LOG_LOB_DELETE || log->m_logType == LOG_HEAP_DELETE);
				m_stat = STAT_WAIT_LOB_DELETE;
				goto _start;
			}
		} else if (m_stat == STAT_HEAP_INSERTED) {// ���в���ող���
			if (log->m_logType == LOG_IDX_DML_BEGIN) {
				assert(log->m_tableId == m_tableId);
				m_redRec.m_format = REC_REDUNDANT;
				m_redRec.m_rowId = m_record.m_rowId;
				m_redRec.m_size = m_tableDef->m_maxRecSize;
				bool indexOnly = m_tableDef->m_indexOnly;
				if (!indexOnly) {
					if (m_record.m_format == REC_COMPRESSED) {//�����¼��ѹ���ģ�����ת��ΪREDUNDANT��ʽ��
						CompressOrderRecord dummyRcd;
						m_redRec.m_data = (byte *)m_session->getMemoryContext()->alloc(m_tableDef->m_maxRecSize);
						dummyRcd.m_size = m_tableDef->m_maxRecSize;
						dummyRcd.m_data = (byte *)m_session->getMemoryContext()->alloc(m_tableDef->m_maxRecSize);
						assert(NULL != m_redRec.m_data && NULL != dummyRcd.m_data);

						RecordOper::convRecordCompressedToCO(m_cprsRcdExtractor, &m_record, &dummyRcd);
						RecordOper::convRecordCOToRed(m_tableDef, &dummyRcd, &m_redRec);
					} else {
						if (m_record.m_format == REC_FIXLEN) {
							m_redRec.m_data = m_record.m_data;
						} else {
							m_redRec.m_data = (byte *)m_session->getMemoryContext()->alloc(m_tableDef->m_maxRecSize);
							RecordOper::convertRecordVR(m_tableDef, &m_record, &m_redRec);
						}
					}
				}
				m_idxInsert = new IdxInsert(m_table, m_session, lsn, &m_redRec);
				m_stat = STAT_IDX_DML;
			} else {
				// �ع�ʱɾ�����м�¼
				assert(log->m_logType == LOG_HEAP_DELETE);
				m_stat = STAT_WAIT_HEAP_DELETE;
				goto _start;
			}
		} else if (m_stat == STAT_WAIT_HEAP_DELETE) {	// �Ѿ�����Ҫ�ع�������ûɾ�����м�¼
			assert(log->m_logType == LOG_HEAP_DELETE);
			assert(log->m_tableId == m_tableId);
			m_table->getHeap()->redoDelete(m_session, lsn, log->m_data, (uint)log->m_size);
			if (m_numLobs > 0)
				m_stat = STAT_WAIT_LOB_DELETE;
			else
				m_stat = STAT_DONE;
		} else if (m_stat == STAT_IDX_DML) {	// ����DML����������
			if (log->m_logType == LOG_IDX_DML_END) {
				m_idxFail = !m_table->getIndice()->isIdxDMLSucceed(log->m_data, (uint)log->m_size);
				delete m_idxInsert;
				m_idxInsert = NULL;
				if (m_idxFail) {
					if (!m_tableDef->m_indexOnly)
						m_stat = STAT_WAIT_HEAP_DELETE;
					else
						m_stat = STAT_DONE;
				} else
					m_stat = STAT_DONE;
			} else
				m_idxInsert->redo(lsn, log);
		} else {
			assert(m_stat == STAT_WAIT_LOB_DELETE);
			assert(m_numLobs > 0);
			if (log->m_logType == LOG_LOB_DELETE) {	// ɾ�����ʹ����
				assert(log->m_tableId == m_tableId);
				m_table->getLobStorage()->redoBLDelete(m_session, lsn, log->m_data, (uint)log->m_size);
			} else {	// ɾ��С�ʹ����
				assert(log->m_logType == LOG_HEAP_DELETE);
				assert(log->m_tableId == TableDef::getVirtualLobTableId(m_tableId));
				m_table->getLobStorage()->redoSLDelete(m_session, m_lobIds[m_numLobs - 1], lsn, log->m_data, (uint)log->m_size);
			}
			m_numLobs--;
			if (m_numLobs == 0)
				m_stat = STAT_DONE;
		}
	}

	/** @see Transaction::completePhase1 */
	virtual bool completePhase1(bool logComplete, bool commit) {
		ftrace(ts.recv, tout << logComplete << commit);
		if (m_record.m_rowId != INVALID_ROW_ID) {
			nftrace(ts.recv, tout << "rid: " << m_record.m_rowId);
		}
		if (logComplete) {
			assert(m_stat == STAT_DONE || (m_stat == STAT_WAIT_LOB_INSERT && m_numLobs == 0)
				|| m_stat == STAT_WAIT_HEAP_INSERT);
			assert(!m_idxInsert);
			m_session->endTransaction(commit, false);
			return false;
		}
		if (m_stat == STAT_DONE) {
			assert(!m_idxInsert);
			m_session->endTransaction(!m_idxFail, true);
			return false;
		}
		if (m_stat == STAT_IDX_DML) {
			m_idxFail = !m_idxInsert->physicalUndo();
			if (m_idxFail) {
				delete m_idxInsert;
				m_idxInsert = NULL;
				if (!m_tableDef->m_indexOnly)
					m_stat = STAT_WAIT_HEAP_DELETE;
				else
					m_stat = STAT_DONE;
			} else
				return true;
		}
		if (m_stat == STAT_HEAP_INSERTED)
			m_stat = STAT_WAIT_HEAP_DELETE;
		if (m_stat == STAT_WAIT_HEAP_DELETE) {
			// ���еļ�¼�Ѿ����룬�ع�ʱɾ��֮
			m_table->getHeap()->del(m_session, m_record.m_rowId);
			m_stat = STAT_WAIT_LOB_DELETE;
		}
		if (m_stat == STAT_WAIT_LOB_INSERT || m_stat == STAT_WAIT_HEAP_INSERT)
			m_stat = STAT_WAIT_LOB_DELETE;
		if (m_stat == STAT_WAIT_LOB_DELETE) {
			// ɾ���������
			for (int i = m_numLobs - 1; i >= 0; i--)
				m_table->getLobStorage()->del(m_session, m_lobIds[i]);
		}
		m_session->endTransaction(false, true);
		return false;
	}

	/** @see Transaction::completePhase2 */
	virtual void completePhase2() {
		ftrace(ts.recv, );
		if (m_record.m_rowId != INVALID_ROW_ID) {
			nftrace(ts.recv, tout << "rid: " << m_record.m_rowId);
		}
		assert(m_idxInsert && !m_idxFail);
		m_idxInsert->logicalRedo();
		delete m_idxInsert;
		m_idxInsert = NULL;
		m_session->endTransaction(true, true);
	}

private:
	static const int STAT_WAIT_LOB_INSERT;
	static const int STAT_WAIT_HEAP_INSERT;
	static const int STAT_HEAP_INSERTED;
	static const int STAT_IDX_DML;
	static const int STAT_WAIT_HEAP_DELETE;
	static const int STAT_WAIT_LOB_DELETE;
	static const int STAT_DONE;

	int		m_stat;			/** ������е��ĸ����� */
	Record	m_record;		/** ����ļ�¼ */
	Record	m_redRec;		/** REC_REDUNDANT��ʽ�ļ�¼ */
	LobId	*m_lobIds;		/** ��������ID */
	u16		m_numLobs;		/** �Ѿ��ɹ�����Ĵ������� */
	IdxInsert	*m_idxInsert;	/** ����������� */
	bool	m_idxFail;		/** ���������Ƿ�ʧ�� */
	TableDef	*m_tableDef;/** ���� */
	CmprssRecordExtractor *m_cprsRcdExtractor;
};
const int InsertTrans::STAT_WAIT_LOB_INSERT = 1;	/** �ȴ�����������־�����а��������ʱ�ĳ�ʼ״̬ */
const int InsertTrans::STAT_WAIT_HEAP_INSERT = 2;	/** �ȴ��Ѳ�����־ */
const int InsertTrans::STAT_HEAP_INSERTED = 3;		/** ���м�¼�ող��� */
const int InsertTrans::STAT_IDX_DML = 4;			/** �������������� */
const int InsertTrans::STAT_WAIT_HEAP_DELETE= 5;	/** �ع�ʱ�ȴ�ɾ�����м�¼ */
const int InsertTrans::STAT_WAIT_LOB_DELETE = 6;	/** �ع�ʱ�ȴ�ɾ������� */
const int InsertTrans::STAT_DONE= 7;				/** ��� */

/**
 * �õ������µĵ�n�����������Ժ�
 *
 * @param nthUpdateLobs ָ���ǵڼ��������µĴ���󣬴�1��ʼ���
 * @return �ô��������Ժ�
 */
u16 PreUpdateLog::getNthUpdateLobsCno(u16 nthUpdateLobs) {
	assert(nthUpdateLobs <= m_numLobs);
	return m_lobCnos[nthUpdateLobs - 1];
}

/** ���¼�¼����
 * ��־����
 *   LOG_PRE_UPDATE
 *   ������־����
 *   ʧ��ʱ
 *     LOG_TXN_END
 *   �ɹ�ʱ
 *     ���´����
 *       LOG_LOB_UPDATE, LOG_HEAP_DELETE, LOG_LOB_INSERT, LOG_HEAP_UPDATE
 *     LOG_MMS_UPDATE | LOG_HEAP_UPDATE
 *     LOG_TXN_END
 */
class UpdateTrans: public Transaction {
public:
	UpdateTrans(Database *db, u16 id, Table *table): Transaction(db, id, table) {
		ftrace(ts.recv, tout << db << id << table->getPath());
		m_tableDef = m_table->getTableDef(true, m_session);
		m_session->startTransaction(TXN_UPDATE, m_tableDef->m_id, false);
		m_stat = STAT_START;
		m_update = NULL;
		m_isFail = false;
		m_currLob = 0;
		m_idxUpdate = NULL;
	}

	virtual ~UpdateTrans() {
		// �ڴ涼��MemoryContext�з��䣬����Ҫ�ͷ�
	}

	/** @see Transaction::redo */
	virtual void redo(LsnType lsn, const LogEntry *log) throw(NtseException) {
		ftrace(ts.recv, tout << lsn << log);
		if (m_update != NULL) {
			nftrace(ts.recv, tout << "rid: " << m_update->m_subRec->m_rowId);
		}

		if (m_stat == STAT_START) {
			assert(log->m_logType == LOG_PRE_UPDATE);
			m_update = m_session->parsePreUpdateLog(m_tableDef, log);
			nftrace(ts.recv, tout << t_srec(m_tableDef, m_update->m_subRec));
			assert(m_update->m_subRec->m_format == REC_REDUNDANT);
			m_currLob = 0;
			if (m_update->m_updateIndex) {
				m_stat = STAT_WAIT_IDX_DML;
			} else if (m_update->m_numLobs > 0) {
				m_stat = STAT_WAIT_LOB_UPDATE;
			} else
				m_stat = STAT_WAIT_REC_UPDATE;
		} else if (m_stat == STAT_WAIT_IDX_DML) {
			assert(log->m_logType == LOG_IDX_DML_BEGIN);
			assert(log->m_tableId == m_tableId);
			m_idxUpdate = new IdxUpdate(m_table, m_session, lsn, m_update);
			m_stat = STAT_IDX_DML;
		} else if (m_stat == STAT_IDX_DML) {
			if (log->m_logType == LOG_IDX_DML_END) {
				m_isFail = !m_table->getIndice()->isIdxDMLSucceed(log->m_data, (uint)log->m_size);
				delete m_idxUpdate;
				m_idxUpdate = NULL;
				if (m_isFail)
					m_stat = STAT_DONE;
				else if (m_tableDef->m_indexOnly)
					m_stat = STAT_DONE;
				else if (m_update->m_numLobs > 0)
					m_stat = STAT_WAIT_LOB_UPDATE;
				else
					m_stat = STAT_WAIT_REC_UPDATE;
			} else
				m_idxUpdate->redo(lsn, log);
		} else if (m_stat == STAT_WAIT_LOB_UPDATE) {
			assert(m_currLob < m_update->m_numLobs);
			if (log->m_logType == LOG_LOB_UPDATE || log->m_logType == LOG_HEAP_UPDATE || log->m_logType == LOG_MMS_UPDATE) {
				LobId newLid;
				if (log->m_logType == LOG_LOB_UPDATE) {	// ���´��ʹ����
					assert(log->m_tableId == m_tableId);
					newLid = m_update->m_lobIds[m_currLob];
					m_table->getLobStorage()->redoBLUpdate(m_session, m_update->m_lobIds[m_currLob], lsn, log->m_data,
						(uint)log->m_size, m_update->m_lobs[m_currLob], m_update->m_lobSizes[m_currLob], m_tableDef->m_compressLobs);
				} else if (log->m_logType == LOG_HEAP_UPDATE) {	// С�ʹ������º���С��
					newLid = m_table->getLobStorage()->redoSLUpdateHeap(m_session, m_update->m_lobIds[m_currLob],
						lsn, log->m_data, (uint)log->m_size, m_update->m_lobs[m_currLob],
						m_update->m_lobSizes[m_currLob], m_tableDef->m_compressLobs);
				} else {
					assert(log->m_logType == LOG_MMS_UPDATE);
					newLid = m_update->m_lobIds[m_currLob];
					m_table->getLobStorage()->redoSLUpdateMms(m_session, lsn, log->m_data,
						(uint)log->m_size);
				}
				if (newLid != m_update->m_lobIds[m_currLob]) {
					u16 lobCno = m_update->getNthUpdateLobsCno(m_currLob + 1);
					RecordOper::writeLobId(m_update->m_subRec->m_data, m_tableDef->m_columns[lobCno], newLid);
				}
			} else if (log->m_logType == LOG_HEAP_DELETE) {	// С�ʹ������º��ɴ��ʹ��������ΪNULL
				assert(log->m_tableId == TableDef::getVirtualLobTableId(m_tableId));
				m_table->getLobStorage()->redoSLDelete(m_session, m_update->m_lobIds[m_currLob],
					lsn, log->m_data, (uint)log->m_size);
				if (m_update->m_lobs[m_currLob])
					m_stat = STAT_WAIT_BL_INSERT;
				else {
					u16 lobCno = m_update->getNthUpdateLobsCno(m_currLob + 1);
					RecordOper::writeLobId(m_update->m_subRec->m_data, m_tableDef->m_columns[lobCno], INVALID_LOB_ID);
				}
			} else if (log->m_logType == LOG_HEAP_INSERT) {	// NULL���³�С�ʹ����
				assert(log->m_tableId == TableDef::getVirtualLobTableId(m_tableId));
				LobId newLid = m_table->getLobStorage()->redoSLInsert(m_session, lsn, log->m_data, (uint)log->m_size);
				u16 lobCno = m_update->getNthUpdateLobsCno(m_currLob + 1);
				RecordOper::writeLobId(m_update->m_subRec->m_data, m_tableDef->m_columns[lobCno], newLid);
			} else if (log->m_logType == LOG_LOB_INSERT) {	// NULL���³ɴ��ʹ����
				assert(log->m_tableId == m_tableId);
				LobId newLid = m_table->getLobStorage()->redoBLInsert(m_session, lsn, log->m_data, (uint)log->m_size);
				u16 lobCno = m_update->getNthUpdateLobsCno(m_currLob + 1);
				RecordOper::writeLobId(m_update->m_subRec->m_data, m_tableDef->m_columns[lobCno], newLid);
			} else {	// ���ʹ������³�NULL
				assert(log->m_logType == LOG_LOB_DELETE);
				assert(log->m_tableId == m_tableId);
				assert(!m_update->m_lobs[m_currLob]);
				m_table->getLobStorage()->redoBLDelete(m_session, lsn, log->m_data, (uint)log->m_size);
				u16 lobCno = m_update->getNthUpdateLobsCno(m_currLob + 1);
				RecordOper::writeLobId(m_update->m_subRec->m_data, m_tableDef->m_columns[lobCno], INVALID_LOB_ID);
			}
			if (m_stat != STAT_WAIT_BL_INSERT) {
				m_currLob++;
				if (m_currLob == m_update->m_numLobs)
					m_stat = STAT_WAIT_REC_UPDATE;
			}
		} else if (m_stat == STAT_WAIT_BL_INSERT) {
			assert(m_currLob < m_update->m_numLobs);
			assert(log->m_logType == LOG_LOB_INSERT);
			assert(log->m_tableId == m_tableId);
			LobId newId = m_table->getLobStorage()->redoBLInsert(m_session, lsn, log->m_data, (uint)log->m_size);
			u16 lobCno = m_update->getNthUpdateLobsCno(m_currLob + 1);
			RecordOper::writeLobId(m_update->m_subRec->m_data, m_tableDef->m_columns[lobCno], newId);
			m_currLob++;
			if (m_currLob == m_update->m_numLobs)
					m_stat = STAT_WAIT_REC_UPDATE;
		} else {
			assert(m_stat == STAT_WAIT_REC_UPDATE);
			assert(log->m_tableId == m_tableId);
			if (log->m_logType == LOG_MMS_UPDATE) {
				assert(m_tableDef->m_useMms);
				m_table->getMmsTable()->redoUpdate(m_session, log->m_data, (uint)log->m_size);
			} else {
				assert(log->m_logType == LOG_HEAP_UPDATE);
				m_table->getHeap()->redoUpdate(m_session, lsn, log->m_data, (uint)log->m_size, m_update->m_subRec);
				deleteMmsRecord(m_update->m_subRec->m_rowId);
			}
			m_stat = STAT_DONE;
		}
	}

	/** @see Transaction::completePhase1 */
	virtual bool completePhase1(bool logComplete, bool commit) {
		ftrace(ts.recv, tout << logComplete << commit);
		if (m_update != NULL) {
			nftrace(ts.recv, tout << "rid: " << m_update->m_subRec->m_rowId);
		}
		if (logComplete) {
			assert(m_stat == STAT_DONE || m_stat == STAT_START);
			assert(!m_idxUpdate);
			m_session->endTransaction(commit, false);
			return false;
		}
		if (m_stat < STAT_IDX_DML) {
			// ��ûREDO�������£���ʱ��ʹ�Ѿ�������Ԥ������־���������в�����δ��Ч
			// ����ҪREDO
			m_session->endTransaction(false);
			return false;
		}
		if (m_idxUpdate) {	// ������������һ��
			assert(m_stat == STAT_IDX_DML);
			m_isFail = !m_idxUpdate->physicalUndo();
			if (m_isFail) {
				delete m_idxUpdate;
				m_idxUpdate = NULL;
				m_stat = STAT_DONE;
			} else
				return true;
		}
		if (m_isFail) {
			m_session->endTransaction(false);
			return false;
		}

		logicalRedo();

		m_session->endTransaction(true);
		return false;
	}

	/** @see Transaction::completePhase2 */
	virtual void completePhase2() {
		ftrace(ts.recv, );
		assert(m_idxUpdate && !m_isFail && m_stat == STAT_IDX_DML);
		if (m_update != NULL) {
			nftrace(ts.recv, tout << "rid: " << m_update->m_subRec->m_rowId);
		}

		m_idxUpdate->logicalRedo();
		delete m_idxUpdate;
		m_idxUpdate = NULL;

		if (m_tableDef->m_indexOnly)
			m_stat = STAT_DONE;
		else {
			if (m_update->m_numLobs > 0)
				m_stat = STAT_WAIT_LOB_UPDATE;
			else
				m_stat = STAT_WAIT_REC_UPDATE;

			logicalRedo();
		}

		m_session->endTransaction(true, true);
	}

private:
	/** �����߼�REDO����֤UPDATE����ִ�гɹ� */
	void logicalRedo() {
		// �ȴ�����µ�һ��Ĵ����
		if (m_stat == STAT_WAIT_BL_INSERT) {
			LobId newId = m_table->getLobStorage()->insert(m_session, m_update->m_lobs[m_currLob],
				m_update->m_lobSizes[m_currLob], m_tableDef->m_compressLobs);
			assert(newId != INVALID_LOB_ID);
			u16 lobCno = m_update->getNthUpdateLobsCno(m_currLob + 1);
			RecordOper::writeLobId(m_update->m_subRec->m_data, m_tableDef->m_columns[lobCno], newId);
			m_currLob++;
			m_stat = STAT_WAIT_LOB_UPDATE;
		}
		if (m_stat == STAT_WAIT_LOB_UPDATE) {
			while (m_currLob < m_update->m_numLobs) {
				LobId oldId = m_update->m_lobIds[m_currLob];
				LobId newId = oldId;
				byte *newLob = m_update->m_lobs[m_currLob];
				if (oldId != INVALID_LOB_ID && newLob) {
					// ��NULL���³ɷ�NULL
					newId = m_table->getLobStorage()->update(m_session, oldId,
						newLob, m_update->m_lobSizes[m_currLob], m_tableDef->m_compressLobs);
					assert(newId != INVALID_LOB_ID);
				} else if (oldId != INVALID_LOB_ID && !newLob) {
					// ��NULL���³�NULL
					m_table->getLobStorage()->del(m_session, oldId);
					newId = INVALID_LOB_ID;
				} else if (oldId == INVALID_LOB_ID && newLob) {
					// NULL���³ɷ�NULL
					newId = m_table->getLobStorage()->insert(m_session, newLob,
						m_update->m_lobSizes[m_currLob], true);
					assert(newId != INVALID_LOB_ID);
				}
				
				u16 lobCno = m_update->getNthUpdateLobsCno(m_currLob + 1);
				RecordOper::writeLobId(m_update->m_subRec->m_data, m_tableDef->m_columns[lobCno], newId);
				RecordOper::writeLobSize(m_update->m_subRec->m_data, m_tableDef->m_columns[lobCno], 0);
				
				m_currLob++;
			}
			m_stat = STAT_WAIT_REC_UPDATE;
		}
		if (m_stat == STAT_WAIT_REC_UPDATE) {

			if (m_tableDef->m_useMms) {
				MmsRecord *mmsRec = m_table->getMmsTable()->getByRid(
					m_session, m_update->m_subRec->m_rowId, false, NULL, None);
				if (mmsRec) {
					u16 recSize;
					if (m_table->getMmsTable()->canUpdate(mmsRec, m_update->m_subRec, &recSize)) {
						m_table->getMmsTable()->update(m_session, mmsRec, m_update->m_subRec, recSize);
						return;
					} else {
						Session *s = m_db->getSessionManager()->allocSession("UpdateTrans::logicalRedo", m_session->getConnection());
						m_table->getMmsTable()->flushAndDel(s, mmsRec);
						m_db->getSessionManager()->freeSession(s);
					}
				}
			}

			NTSE_ASSERT(m_table->getHeap()->update(m_session, m_update->m_subRec->m_rowId,	m_update->m_subRec));
		}
	}

	void deleteMmsRecord(RowId rid) {
		ftrace(ts.recv, tout << rid);
		if (m_tableDef->m_useMms) {
			MmsRecord *mmsRec = m_table->getMmsTable()->getByRid(m_session, rid,
				false, NULL, None);
			if (mmsRec)
				m_table->getMmsTable()->del(m_session, mmsRec);
		}
	}

	static const int STAT_START;
	static const int STAT_WAIT_IDX_DML;
	static const int STAT_IDX_DML;
	static const int STAT_WAIT_LOB_UPDATE;
	static const int STAT_WAIT_BL_INSERT;
	static const int STAT_WAIT_REC_UPDATE;
	static const int STAT_DONE;

	int			m_stat;			/** ����ִ�е�ʲô״̬ */
	PreUpdateLog	*m_update;	/** ���²������� */
	bool		m_isFail;		/** ���������£���û�ɹ� */
	u16			m_currLob;		/** ��ǰ�ȴ�����Ĵ���� */
	IdxUpdate	*m_idxUpdate;	/** �������²��� */
	TableDef	*m_tableDef;	/** ���� */
};
const int UpdateTrans::STAT_START = 1;				/** ��ʼ״̬ */
const int UpdateTrans::STAT_WAIT_IDX_DML = 2;		/** �ȴ�����DML���� */
const int UpdateTrans::STAT_IDX_DML = 3;			/** ���ڴ����������� */
const int UpdateTrans::STAT_WAIT_LOB_UPDATE = 4;	/** ׼�����д������� */
const int UpdateTrans::STAT_WAIT_BL_INSERT = 5;		/** ��С�ʹ�������Ϊ���ʹ����ʱ��С����ɾ�����ȴ�������� */
const int UpdateTrans::STAT_WAIT_REC_UPDATE = 6;	/** �ȴ����¶ѻ�MMS�м�¼ */
const int UpdateTrans::STAT_DONE = 7;				/** ��� */

/** ɾ����¼����
 *  ��־����
 *    LOG_PRE_DELETE: Ԥɾ����־��������ɾ���ļ�¼RID
 *    ������־����
 *    ʧ��ʱ
 *      LOG_TXN_END
 *    (LOG_HEAP_DELETE | LOG_LOB_DELETE)*: ɾ�������
 *    LOG_HEAP_DELETE: ɾ�����м�¼
 *    LOG_TXN_END
 */
class DeleteTrans: public Transaction {
public:
	/**
	 * ����ɾ����¼�������
	 *
	 * @see Transaction::Transaction
	 */
	DeleteTrans(Database *db, u16 id, Table *table): Transaction(db, id, table) {
		ftrace(ts.recv, tout << db << id << table->getPath());
		m_tableDef = m_table->getTableDef(true, m_session);
		m_session->startTransaction(TXN_DELETE, m_tableDef->m_id, false);
		m_stat = STAT_START;
		m_log = NULL;
		m_idxDelete = NULL;
		m_isFail = true;
		m_currLob = 0;
	}

	/** ����ɾ����¼������� */
	virtual ~DeleteTrans() {
		// �ڴ涼��MemoryContext�з��䣬����Ҫ�ͷ�
	}

	/** @see Transaction::redo */
	virtual void redo(LsnType lsn, const LogEntry *log) throw(NtseException) {
		ftrace(ts.recv, tout << lsn << log);
		if (m_log != NULL) {
			nftrace(ts.recv, tout << "rid: " << m_log->m_rid);
		}

		if (m_stat == STAT_START) {
			assert(log->m_logType == LOG_PRE_DELETE);
			m_log = m_table->parsePreDeleteLog(m_session, log);
			if (m_tableDef->m_numIndice > 0)
				m_stat = STAT_WAIT_IDX_DML;
			else if (m_log->m_numLobs > 0)
				m_stat = STAT_WAIT_LOB_DELETE;
			else
				m_stat = STAT_WAIT_HEAP_DELETE;
		} else if (m_stat == STAT_WAIT_IDX_DML) {
			assert(log->m_logType == LOG_IDX_DML_BEGIN);
			assert(log->m_tableId == m_tableId);
			m_idxDelete = new IdxDelete(m_table, m_session, lsn, m_log->m_indexPreImage);
			m_stat = STAT_IDX_DML;
		} else if (m_stat == STAT_IDX_DML) {
			if (log->m_logType == LOG_IDX_DML_END) {
				m_isFail = !m_table->getIndice()->isIdxDMLSucceed(log->m_data, (uint)log->m_size);
				delete m_idxDelete;
				m_idxDelete = NULL;
				if (m_isFail)
					m_stat = STAT_DONE;
				else if (m_tableDef->m_indexOnly)
					m_stat = STAT_DONE;
				else if (m_log->m_numLobs > 0)
					m_stat = STAT_WAIT_LOB_DELETE;
				else
					m_stat = STAT_WAIT_HEAP_DELETE;
			} else
				m_idxDelete->redo(lsn, log);
		} else if (m_stat == STAT_WAIT_LOB_DELETE) {
			if (log->m_logType == LOG_HEAP_DELETE) {	// ɾ��С�ʹ����
				assert(log->m_tableId == TableDef::getVirtualLobTableId(m_tableId));
				m_table->getLobStorage()->redoSLDelete(m_session, m_log->m_lobIds[m_currLob], lsn, log->m_data, (uint)log->m_size);
			} else {	// ɾ�����ʹ����
				assert(log->m_logType == LOG_LOB_DELETE);
				assert(log->m_tableId == m_tableId);
				m_table->getLobStorage()->redoBLDelete(m_session, lsn, log->m_data, (uint)log->m_size);
			}
			m_currLob++;
			if (m_currLob == m_log->m_numLobs)
				m_stat = STAT_WAIT_HEAP_DELETE;
		} else {
			assert(m_stat == STAT_WAIT_HEAP_DELETE);
			assert(log->m_logType == LOG_HEAP_DELETE);
			assert(log->m_tableId == m_tableId);
			deleteMmsRecord(m_log->m_rid);
			m_table->getHeap()->redoDelete(m_session, lsn, log->m_data, (uint)log->m_size);
			m_stat = STAT_DONE;
		}
	}

	/** @see Transaction::completePhase1 */
	virtual bool completePhase1(bool logComplete, bool commit) {
		ftrace(ts.recv, tout << logComplete << commit);
		if (m_log != NULL) {
			nftrace(ts.recv, tout << "rid: " << m_log->m_rid);
		}
		if (logComplete) {
			assert(m_stat == STAT_DONE || m_stat == STAT_START || (!commit && m_stat == STAT_WAIT_IDX_DML));
			assert(!m_idxDelete);
			m_session->endTransaction(commit, false);
			return false;
		}
		if (m_stat == STAT_IDX_DML) {
			m_isFail = !m_idxDelete->physicalUndo();
			if (m_isFail) {
				delete m_idxDelete;
				m_idxDelete = NULL;
				m_stat = STAT_DONE;
			} else
				return true;
		}
		if (m_isFail) {
			m_session->endTransaction(false);
			return false;
		}

		logicalRedo();

		m_session->endTransaction(!m_isFail);
		return false;
	}

	/** @see Transaction::completePhase2 */
	void completePhase2() {
		ftrace(ts.recv, );
		assert(m_idxDelete && !m_isFail && m_stat == STAT_IDX_DML);
		if (m_log != NULL) {
			nftrace(ts.recv, tout << "rid: " << m_log->m_rid);
		}

		m_idxDelete->logicalRedo();
		delete m_idxDelete;
		m_idxDelete = NULL;

		if (m_tableDef->m_indexOnly)
			m_stat = STAT_DONE;
		else {
			if (m_log->m_numLobs > 0)
				m_stat = STAT_WAIT_LOB_DELETE;
			else
				m_stat = STAT_WAIT_HEAP_DELETE;

			logicalRedo();
		}

		m_session->endTransaction(true, true);
	}

private:
	void logicalRedo() {
		if (m_stat == STAT_WAIT_LOB_DELETE) {	// �����û��ȫɾ��
			while (m_currLob < m_log->m_numLobs) {
				m_table->getLobStorage()->del(m_session, m_log->m_lobIds[m_currLob]);
				m_currLob++;
			}
			m_stat = STAT_WAIT_HEAP_DELETE;
		}
		if (m_stat == STAT_WAIT_HEAP_DELETE) {
			deleteMmsRecord(m_log->m_rid);
			NTSE_ASSERT(m_table->getHeap()->del(m_session, m_log->m_rid));
		}
	}

	void deleteMmsRecord(RowId rid) {
		ftrace(ts.recv, tout << rid);
		if (m_tableDef->m_useMms) {
			MmsRecord *mmsRec = m_table->getMmsTable()->getByRid(m_session, rid,
				false, NULL, None);
			if (mmsRec)
				m_table->getMmsTable()->del(m_session, mmsRec);
		}
	}

	static const int STAT_START;
	static const int STAT_WAIT_IDX_DML;
	static const int STAT_IDX_DML;
	static const int STAT_WAIT_LOB_DELETE;
	static const int STAT_WAIT_HEAP_DELETE;
	static const int STAT_DONE;

	int			m_stat;		/** ����ִ��״̬ */
	PreDeleteLog	*m_log;	/** Ԥɾ����־ */
	IdxDelete	*m_idxDelete;	/** ������������ */
	bool		m_isFail;	/** ���������Ƿ�ʧ�� */
	u16			m_currLob;	/** �Ѿ�ɾ���Ĵ������� */
	TableDef	*m_tableDef;/** ���� */
};

const int DeleteTrans::STAT_START = 1;				/** ��ʼ״̬ */
const int DeleteTrans::STAT_WAIT_IDX_DML = 2;		/** �ȴ�����DML���� */
const int DeleteTrans::STAT_IDX_DML = 3;			/** ���ڴ����������� */
const int DeleteTrans::STAT_WAIT_LOB_DELETE = 4;	/** ׼��ɾ������� */
const int DeleteTrans::STAT_WAIT_HEAP_DELETE = 5;	/** ׼��ɾ�����м�¼ */
const int DeleteTrans::STAT_DONE = 6;				/** ��� */

/**
 * MMSˢ���¼����־
 *
 * ��־����
 *	LOG_MMS_UPDATE
 *  ...
 *  LOG_MMS_UPDATE
 */
class MmsFlushDirtyTrans: public Transaction {
public:
	/** ����һ��MMSˢ���¼����־���������
	 * @see Transaction::Transaction
	 */
	MmsFlushDirtyTrans(Database *db, u16 id, u16 tableId, Table *table): Transaction(db, id, table) {
		m_tableDef = m_table->getTableDef(true, m_session);
		m_session->startTransaction(TXN_MMS_FLUSH_DIRTY, m_tableDef->m_id, false);
		m_isSlob = TableDef::tableIdIsVirtualLob(tableId);
	}
	/** ����һ��MMSˢ���¼����־��������� */
	virtual ~MmsFlushDirtyTrans() {
	}

	/** @see Transaction::redo */
	virtual void redo(LsnType lsn, const LogEntry *log) throw(NtseException) {
		UNREFERENCED_PARAMETER(lsn);
		ftrace(ts.recv, tout << lsn << log);
		assert(log->m_logType == LOG_MMS_UPDATE);
		if (!m_isSlob) {
			m_table->getMmsTable()->redoUpdate(m_session, log->m_data, (uint)log->m_size);
		} else {
			m_table->getLobStorage()->getSLMmsTable()->redoUpdate(m_session, log->m_data, (uint)log->m_size);
		}
	}

	/** @see Transaction::completePhase1 */
	virtual bool completePhase1(bool logComplete, bool commit) {
		ftrace(ts.recv, tout << logComplete << commit);
		if (logComplete) {
			m_session->endTransaction(commit, false);
		} else {
			m_session->endTransaction(m_stat != STAT_START, true);
		}
		return false;
	}
private:
	static const int STAT_START;
	static const int STAT_WAIT_MMS_UPDATE;

	int		m_stat;			/** ����ǰִ��״̬ */
	bool	m_isSlob;		/** �Ƿ�С�ʹ�����mms��־ */
	TableDef	*m_tableDef;/** ���� */
};

const int MmsFlushDirtyTrans::STAT_START = 1;				/** ��ʼ״̬ */
const int MmsFlushDirtyTrans::STAT_WAIT_MMS_UPDATE = 2;		/** �ȴ������MMS_UPDATE */

/**
 * MMS���¼�¼�������������������һ�Ǹ�����ͨ���¼һ�Ǹ���С��
 * �����������¼
 *
 * ��־����
 *   LOG_PRE_UPDATE_HEAP
 *   LOG_HEAP_UPDATE
 */
class UpdateHeapTrans: public Transaction {
public:
	/** ����һ��MMS���¼�¼�����������
	 * @see Transaction::Transaction
	 */
	UpdateHeapTrans(Database *db, u16 id, u16 tableId, Table *table): Transaction(db, id, table) {
		ftrace(ts.recv, tout << db << id << table->getPath());
		m_tableDef = m_table->getTableDef(true, m_session);
		assert(m_tableDef->m_useMms);
		m_session->startTransaction(TXN_UPDATE_HEAP, tableId, false);
		m_tableId = tableId;
		m_stat = STAT_START;
		m_isSlob = TableDef::tableIdIsVirtualLob(m_tableId);
		m_update = NULL;
	}

	/** ����һ��MMS���¼�¼����������� */
	virtual ~UpdateHeapTrans() {
	}

	/** @see Transaction::redo */
	virtual void redo(LsnType lsn, const LogEntry *log) throw(NtseException) {
		ftrace(ts.recv, tout << lsn << log);
		if (m_update) {
			nftrace(ts.recv, tout << "rid: " << m_update->m_rowId << ", rec: " << t_srec(m_tableDef, m_update));
		}
		if (m_stat == STAT_START) {
			assert(log->m_logType == LOG_PRE_UPDATE_HEAP);
			assert(log->m_tableId == m_tableId);
			if (!m_isSlob)
				m_update = m_session->parsePreUpdateHeapLog(m_tableDef, log);
			else {
				const TableDef *tableDef = m_table->getLobStorage()->getSLVTableDef();
				m_update = m_session->parsePreUpdateHeapLog(tableDef, log);
			}
			m_stat = STAT_WAIT_HEAP_UPDATE;
		} else if (m_stat == STAT_WAIT_HEAP_UPDATE) {
			assert(log->m_logType == LOG_HEAP_UPDATE);
			assert(log->m_tableId == m_tableId);
			if (!m_isSlob) {
				removeMmsRecord(m_table->getMmsTable(), m_update->m_rowId);
				m_table->getHeap()->redoUpdate(m_session, lsn, log->m_data,
					(uint)log->m_size, m_update);
			} else {
				assert(m_tableDef->m_useMms);
				removeMmsRecord(m_table->getLobStorage()->getSLMmsTable(), m_update->m_rowId);
				m_table->getLobStorage()->getSLHeap()->redoUpdate(m_session, lsn,
					log->m_data, (uint)log->m_size, m_update);
			}
			m_stat = STAT_DONE;
		}
	}

	/** @see Transaction::completePhase1 */
	virtual bool completePhase1(bool logComplete, bool commit) {
		ftrace(ts.recv, tout << logComplete << commit);
		if (m_update) {
			nftrace(ts.recv, tout << "rid: " << m_update->m_rowId << ", rec: " << t_srec(m_tableDef, m_update));
		}
		if (logComplete) {
			assert(m_stat == STAT_DONE || m_stat == STAT_START);
			m_session->endTransaction(commit, false);
			return false;
		}
		if (m_stat == STAT_START) {
			m_session->endTransaction(false);
			return false;
		}
		if (m_stat == STAT_WAIT_HEAP_UPDATE) {
			if (!m_isSlob) {
				removeMmsRecord(m_table->getMmsTable(), m_update->m_rowId);
				m_table->getHeap()->update(m_session, m_update->m_rowId, m_update);
			} else {
				removeMmsRecord(m_table->getLobStorage()->getSLMmsTable(), m_update->m_rowId);
				m_table->getLobStorage()->getSLHeap()->update(m_session, m_update->m_rowId, m_update);
			}
		}
		m_session->endTransaction(true);
		return false;
	}

private:
	void removeMmsRecord(MmsTable *mmsTable, RowId rid) {
		ftrace(ts.recv, tout << mmsTable << rid);
		MmsRecord *mmsRec = mmsTable->getByRid(m_session, rid, false, NULL, None);
		if (mmsRec)
			mmsTable->del(m_session, mmsRec);
	}
	static const int STAT_START;
	static const int STAT_WAIT_HEAP_UPDATE;
	static const int STAT_DONE;

	int		m_stat;			/** ����ǰִ��״̬ */
	SubRecord	*m_update;	/** �������� */
	bool	m_isSlob;		/** �Ƿ�Ϊ����С�ʹ�����¼ */
	TableDef	*m_tableDef;/** ���� */
};
const int UpdateHeapTrans::STAT_START = 1;				/** ��ʼ״̬ */
const int UpdateHeapTrans::STAT_WAIT_HEAP_UPDATE = 2;	/** �Ѿ���ȡ��Ԥ������־������û�и��¶� */
const int UpdateHeapTrans::STAT_DONE = 3;				/** �Ѿ����¶� */

/**
 * ������������
 * ��־����: 
 * LOG_IDX_ADD_INDEX
 * LOG_IDX_CREATE_BEGIN
 * (LOG_IDX_DML|LOG_IDX_SET_PAGE)+
 * LOG_IDX_DROP_INDEX? (ֻ�ڽ���������Ψһ�Գ�ͻʧ��ʱ���ܴ���)
 * LOG_IDX_ADD_INDEX_CPST
 * LOG_IDX_CREATE_END
 */
class AddIndexTrans: public Transaction {
public:
	/** ����һ�����������������
	 * @see Transaction::Transaction
	 */
	AddIndexTrans(Database *db, u16 id, Table *table): Transaction(db, id, table) {
		ftrace(ts.recv, tout << db << id << table->getPath());
		m_session->startTransaction(TXN_ADD_INDEX, table->getTableDef(true, m_session)->m_id, false);
		m_stat = STAT_START;
		m_isFail = false;
		m_indexDef = NULL;
		m_createBeginLog = NULL;
	}

	/** ����һ����������������� */
	virtual ~AddIndexTrans() {
		delete m_indexDef;
	}

	/** @see Transaction::redo */
	virtual void redo(LsnType lsn, const LogEntry *log) throw(NtseException) {
		ftrace(ts.recv, tout << lsn << log);
		if (m_stat == STAT_START) {
			assert(log->m_logType == LOG_ADD_INDEX);
			m_indexDef = m_table->parseAddIndexLog(log);
			m_stat = STAT_WAIT_IDX_CREATE;
		} else if (m_stat == STAT_WAIT_IDX_CREATE) {
			assert(log->m_logType == LOG_IDX_CREATE_BEGIN);
			m_createBeginLog = copyLog(m_session, lsn, log);
			m_stat = STAT_ADD_INDEX;
		} else {
			assert(m_stat == STAT_ADD_INDEX);
			if (log->m_logType == LOG_IDX_DML) {
				m_table->getIndice()->redoDML(m_session, lsn, log->m_data, (uint)log->m_size);
			} else if (log->m_logType == LOG_IDX_SET_PAGE) {
				m_table->getIndice()->redoPageSet(m_session, lsn, log->m_data, (uint)log->m_size);
			} else if (log->m_logType == LOG_IDX_ADD_INDEX_CPST) {
				m_table->getIndice()->redoCpstCreateIndex(m_session, lsn, log->m_data, (uint)log->m_size);
				m_isFail = true;
				m_stat = STAT_DONE;
			} else if (log->m_logType == LOG_IDX_DROP_INDEX) {
				m_table->getIndice()->redoDropIndex(m_session, lsn, log->m_data, (uint)log->m_size);
				m_isFail = true;
			} else {
				assert(log->m_logType == LOG_IDX_CREATE_END);
				m_table->getIndice()->redoCreateIndexEnd(log->m_data, (uint)log->m_size);
				m_stat = STAT_DONE;
			}
		}
	}

	/** @see Transaction::completePhase1 */
	virtual bool completePhase1(bool logComplete, bool commit) {
		ftrace(ts.recv, tout << logComplete << commit);
		if (m_stat == STAT_ADD_INDEX) {
			m_table->getIndice()->undoCreateIndex(m_session, m_createBeginLog->m_lsn, m_createBeginLog->m_log.m_data,
				(uint)m_createBeginLog->m_log.m_size, true);
			m_isFail = true;
		}
		if (!m_isFail && m_stat == STAT_DONE) {
			m_table->redoFlush(m_session);
			// �ָ����ֻ����lsn > flushLsnʱ�Ż������һ����
			// ��ʱ���еı���Ҫô��û�����½���������Ҫô
			// ���һ�����������½����������������Ѿ�����������DDL����
			TableDef *tableDef = m_table->getTableDef(true, m_session);
			if (tableDef->m_numIndice == 0 || strcmp(tableDef->m_indice[tableDef->m_numIndice - 1]->m_name, m_indexDef->m_name)) {
				tableDef->addIndex(m_indexDef);
				m_table->writeTableDef();
			}
		}
		if (logComplete) {
			assert(m_stat == STAT_DONE || m_stat == STAT_START);
			m_session->endTransaction(commit, false);
			return false;
		}
		m_session->endTransaction(!m_isFail);
		return false;
	}

private:
	static const int STAT_START;
	static const int STAT_WAIT_IDX_CREATE;
	static const int STAT_ADD_INDEX;
	static const int STAT_DONE;

	int			m_stat;				/** ����ִ��״̬ */
	IndexDef	*m_indexDef;		/** �������� */
	LogRecord	*m_createBeginLog;	/** ��ʼ������־ */
	bool		m_isFail;			/** �Ƿ�ʧ�� */
};
const int AddIndexTrans::STAT_START = 1;		/** ��ʼ״̬ */
const int AddIndexTrans::STAT_WAIT_IDX_CREATE = 2;	/** �ȴ�����������־ */
const int AddIndexTrans::STAT_ADD_INDEX = 3;	/** �������������� */
const int AddIndexTrans::STAT_DONE = 4;			/** ��� */

//TNT�����¼������ֵ�����������������
#ifdef TNT_ENGINE
/** ������������
 * ��������µ���־��������:
 *   ��ѡ�Ĵ���������־��������LOG_LOB_INSERT��LOG_HEAP_INSERT(�����)����
 *   ������־����
 *
 *   ����ʱ
 *   
 *     ��ѡ��ɾ������󣬿�����LOG_LOB_DELETE��LOG_HEAP_DELETE(�����)����
 *   LOG_TXN_END
 */
class InsertLobTrans: public Transaction{
public:
	InsertLobTrans(Database *db, u16 id, Table *table): Transaction(db, id, table) {
		ftrace(ts.recv, tout << db << id << table->getPath());
		m_tableDef = m_table->getTableDef(true, m_session);
	
		m_session->startTransaction(TXN_LOB_INSERT, m_tableDef->m_id, false);
		m_stat = STAT_WAIT_LOB_INSERT;
		m_numLobs = 0;
	}

	virtual ~InsertLobTrans() {
		// �ڴ涼��MemoryContext�з��䣬����Ҫ�ͷ�
	}

	/** @see Transaction::redo */
	virtual void redo(LsnType lsn, const LogEntry *log) throw(NtseException) {
		ftrace(ts.recv, tout << lsn << log);
	
_start:
		if (m_stat == STAT_WAIT_LOB_INSERT) {	// �����������
			assert(m_numLobs < m_tableDef->getNumLobColumns());
			if (log->m_logType == LOG_LOB_INSERT) {	// ���ʹ����
				assert(log->m_tableId == m_tableId);
				m_lobId = m_table->getLobStorage()->redoBLInsert(m_session, lsn, log->m_data, (uint)log->m_size);
				m_numLobs++;
			} else if (log->m_logType == LOG_HEAP_INSERT) {	// С�ʹ�����Ѳ���
				if (log->m_tableId == TableDef::getVirtualLobTableId(m_tableId)){	// С�ʹ����
					m_lobId = m_table->getLobStorage()->redoSLInsert(m_session, lsn, log->m_data, (uint)log->m_size);
					m_numLobs++;
				}	else {	// ĳЩ�����ΪNULLʱ���ܳ�����һ���
					assert(log->m_tableId == m_tableId);
					m_stat = STAT_DONE;
					goto _start;
				}
			} else {	// �����������û�����ʱ���ع�
				assert(log->m_logType == LOG_LOB_DELETE || log->m_logType == LOG_HEAP_DELETE);
				m_stat = STAT_WAIT_LOB_DELETE;
				goto _start;
			}
			if (m_numLobs == 1)
				m_stat = STAT_DONE;
		}   else {
			assert(m_stat == STAT_WAIT_LOB_DELETE);
			assert(m_numLobs > 0);
			if (log->m_logType == LOG_LOB_DELETE) {	// ɾ�����ʹ����
				assert(log->m_tableId == m_tableId);
				m_table->getLobStorage()->redoBLDelete(m_session, lsn, log->m_data, (uint)log->m_size);
			} else {	// ɾ��С�ʹ����
				assert(log->m_logType == LOG_HEAP_DELETE);
				assert(log->m_tableId == TableDef::getVirtualLobTableId(m_tableId));
				m_table->getLobStorage()->redoSLDelete(m_session, m_lobId, lsn, log->m_data, (uint)log->m_size);
			}
			m_numLobs--;
			if (m_numLobs == 0)
				m_stat = STAT_DONE;
		}
	}

	/** @see Transaction::completePhase1 */
	virtual bool completePhase1(bool logComplete, bool commit) {
		ftrace(ts.recv, tout << logComplete << commit);
		if (logComplete) {
			assert(m_stat == STAT_DONE || (m_stat == STAT_WAIT_LOB_INSERT && m_numLobs == 0));
			m_session->endTransaction(commit, false);
			return false;
		}
		if (m_stat == STAT_DONE) {
			m_session->endTransaction(true, true);
			return false;
		}
		if (m_stat == STAT_WAIT_LOB_INSERT )
			m_stat = STAT_WAIT_LOB_DELETE;
		if (m_stat == STAT_WAIT_LOB_DELETE) {
			// ɾ���������
			if(m_numLobs == 1)
				m_table->getLobStorage()->del(m_session, m_lobId);
		}
		m_session->endTransaction(false, true);
		return false;
	}

	/** @see Transaction::completePhase2 */
	virtual void completePhase2() {
		ftrace(ts.recv, );
		m_session->endTransaction(true, true);
	}

private:
	static const int STAT_WAIT_LOB_INSERT;
	static const int STAT_WAIT_LOB_DELETE;
	static const int STAT_DONE;

	int		m_stat;			/** ������е��ĸ����� */
	LobId	m_lobId;			/** ����Ĵ����Id*/
	u16		m_numLobs;		/** �Ѿ��ɹ�����Ĵ������� */
	TableDef	*m_tableDef;/** ���� */
};
const int InsertLobTrans::STAT_WAIT_LOB_INSERT = 1;	/** �ȴ�����������־�����а��������ʱ�ĳ�ʼ״̬ */
const int InsertLobTrans::STAT_WAIT_LOB_DELETE = 2;	/** �ع�ʱ�ȴ�ɾ������� */
const int InsertLobTrans::STAT_DONE= 3;				/** ��� */


/** ɾ�����������
 *  ��־����
 *    LOG_PRE_DELETE: Ԥɾ����־��������ɾ���ļ�¼RID
 *    ������־����
 *    ʧ��ʱ
 *      LOG_TXN_END
 *    (LOG_HEAP_DELETE | LOG_LOB_DELETE)*: ɾ�������
 *    LOG_HEAP_DELETE: ɾ�����м�¼
 *    LOG_TXN_END
 */
class DeleteLobTrans: public Transaction {
public:
	/**
	 * ����ɾ����¼�������
	 *
	 * @see Transaction::Transaction
	 */
	DeleteLobTrans(Database *db, u16 id, Table *table): Transaction(db, id, table) {
		ftrace(ts.recv, tout << db << id << table->getPath());
		m_tableDef = m_table->getTableDef(true, m_session);
		m_session->startTransaction(TXN_DELETE, m_tableDef->m_id, false);
		m_stat = STAT_START;
		m_currLob = 0;
	}

	/** ����ɾ����¼������� */
	virtual ~DeleteLobTrans() {
		// �ڴ涼��MemoryContext�з��䣬����Ҫ�ͷ�
	}

	/** @see Transaction::redo */
	virtual void redo(LsnType lsn, const LogEntry *log) throw(NtseException) {
		ftrace(ts.recv, tout << lsn << log);

		if (m_stat == STAT_START) {
			assert(log->m_logType == LOG_PRE_LOB_DELETE);
			m_lobId= m_table->parsePreDeleteLobLog(m_session, log);
			m_stat = STAT_WAIT_LOB_DELETE;
			
		} else {
			assert(m_stat == STAT_WAIT_LOB_DELETE);
			if (log->m_logType == LOG_HEAP_DELETE) {	// ɾ��С�ʹ����
				assert(log->m_tableId == TableDef::getVirtualLobTableId(m_tableId));
				m_table->getLobStorage()->redoSLDelete(m_session, m_lobId, lsn, log->m_data, (uint)log->m_size);
			} else {	// ɾ�����ʹ����
				assert(log->m_logType == LOG_LOB_DELETE);
				assert(log->m_tableId == m_tableId);
				m_table->getLobStorage()->redoBLDelete(m_session, lsn, log->m_data, (uint)log->m_size);
			}
			m_currLob++;
			if (m_currLob == 1)
				m_stat = STAT_DONE;
		}
	}

	/** @see Transaction::completePhase1 */
	virtual bool completePhase1(bool logComplete, bool commit) {
		ftrace(ts.recv, tout << logComplete << commit);
		if (logComplete) {
		//	assert(m_stat == STAT_DONE || m_stat == STAT_START );  // TODO�� ������������⣬���ʹ�����ظ�ɾ��ʱ����дɾ����־����ʱ����trxEnd��־ʱ��m_stat == STAT_WAIT_LOB_DELETE
			m_session->endTransaction(commit, false);
			return false;
		}
		logicalRedo();
		m_session->endTransaction(true);
		return false;
	}

	/** @see Transaction::completePhase2 */
	void completePhase2() {
		ftrace(ts.recv, );
		m_stat = STAT_WAIT_LOB_DELETE;
		logicalRedo();
		m_session->endTransaction(true, true);
	}

private:
	void logicalRedo() {
		if (m_stat == STAT_WAIT_LOB_DELETE) {	// �����û��ȫɾ��
			while (m_currLob < 1) {
				//���tnt���ڻָ�ʱɾ�������ڵĴ��������Ҳ��Ҫ�ݴ�
				m_table->getLobStorage()->delAtCrash(m_session, m_lobId);
				m_currLob++;
			}
			m_stat = STAT_DONE;
		}
	}


	static const int STAT_START;
	static const int STAT_WAIT_LOB_DELETE;
	static const int STAT_DONE;

	int			m_stat;		/** ����ִ��״̬ */
	//PreDeleteLog	*m_log;	/** Ԥɾ����־ */
	LobId		m_lobId;	/** Ԥɾ�������ID */
	u16			m_currLob;	/** �Ѿ�ɾ���Ĵ������� */
	TableDef	*m_tableDef;/** ���� */
};
const int DeleteLobTrans::STAT_START = 1;				/** ��ʼ״̬ */
const int DeleteLobTrans::STAT_WAIT_LOB_DELETE = 2;	/** ׼��ɾ������� */
const int DeleteLobTrans::STAT_DONE = 3;				/** ��� */


#endif


/**
 * �����������ʹ�������������󣬼�����Ĵ�������
 *
 * @param type ��������
 * @param db ���ݿ����
 * @param id ����ID
 * @param tableId �����������ı��ID��������С�ʹ����������ID
 * @param table �����������ı�����ΪNULL
 * @throw NtseException �ò���ָ���ĻỰ
 * @return �������
 */
Transaction* Transaction::createTransaction(TxnType type, Database *db, u16 id, u16 tableId, Table *table) throw (NtseException) {
	switch(type) {
	case TXN_INSERT:
		return new InsertTrans(db, id, table);
	case TXN_UPDATE:
		return new UpdateTrans(db, id, table);
	case TXN_DELETE:
		return new DeleteTrans(db, id, table);
	case TXN_UPDATE_HEAP:
		return new UpdateHeapTrans(db, id, tableId, table);
	case TXN_MMS_FLUSH_DIRTY:
		return new MmsFlushDirtyTrans(db, id, tableId, table);
#ifdef TNT_ENGINE			//�˴�����������������
	case TXN_LOB_INSERT:
		return new InsertLobTrans(db,id,table);
	case TXN_LOB_DELETE:
		return new DeleteLobTrans(db,id,table);
#endif

	default:
		assert(type == TXN_ADD_INDEX);
		return new AddIndexTrans(db, id, table);
	}
}

/**
 * ����һ���������
 *
 * @param db ���ݿ����
 * @param id ����ID
 * @param table �����������ı�
 */
Transaction::Transaction(Database *db, u16 id, Table *table) {
	m_db = db;
	m_conn = db->getConnection(true);
	m_session = db->getSessionManager()->getSessionDirect("Recover", id, m_conn);
	if (!m_session)
		NTSE_THROW(NTSE_EC_TOO_MANY_SESSION, "Can not get session %d, do you change ntse_max_sessions?", id);
	m_table = table;
	if (m_table)
		m_tableId = table->getTableDef(true, m_session)->m_id;
}

/**
 * ����һ���������
 */
Transaction::~Transaction() {
	m_db->getSessionManager()->freeSession(m_session);
	m_db->freeConnection(m_conn);
}

/** �õ�����ID
 * @return ����ID
 */
u16 Transaction::getId() {
	return m_session->getId();
}

///////////////////////////////////////////////////////////////////////////////
// IdxDml /////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////////
/**
 * ����IdxDml����
 *
 * @param table �����ı�
 * @param session �Ự����
 */
IdxDml::IdxDml(Table *table, Session *session, u64 beginLsn) {
	m_table = table;
	m_session = session;
	m_beginLsn = beginLsn;
	m_tableDef = m_table->getTableDef(true, m_session);
	m_tableId = m_tableDef->m_id;
	m_indice = m_table->getIndice();
	m_idxRemain = m_tableDef->m_numIndice;
	m_uniqueRemain = m_tableDef->getNumIndice(true);
	m_lastIdx = -1;
}

/**
 * ����IdxDml����
 */
IdxDml::~IdxDml() {
}

/**
 * ��������DML����������д����־��������REDO��־�򲹳���־
 *
 * @param lsn ��־LSN
 * @param log ��־����
 */
void IdxDml::redo(LsnType lsn, const LogEntry *log) {
	ftrace(ts.recv, tout << lsn << log);
	assert(log->m_tableId == m_tableId);
	if (log->m_logType == LOG_IDX_DMLDONE_IDXNO) {
		m_lastIdx = (s16)m_indice->getLastUpdatedIdxNo(log->m_data, (uint)log->m_size);
		assert(m_lastIdx < m_tableDef->m_numIndice && m_idxRemain > 0);
		m_idxRemain--;
		if (m_uniqueRemain > 0) {
			m_uniqueRemain--;
			if (m_uniqueRemain == 0)
				clearLog();
		} else {
			clearLog();
		}
	} else if (log->m_logType < LOG_CPST_MIN) {	// REDO��־
		if (log->m_logType == LOG_IDX_DML) {
			m_table->getIndice()->redoDML(m_session, lsn, log->m_data, (uint)log->m_size);
		} else if (log->m_logType == LOG_IDX_SMO) {
			m_table->getIndice()->redoSMO(m_session, lsn, log->m_data, (uint)log->m_size);
		} else if (log->m_logType == LOG_IDX_DIU_DONE) {
		} else {
			assert(log->m_logType == LOG_IDX_SET_PAGE);
			m_table->getIndice()->redoPageSet(m_session, lsn, log->m_data, (uint)log->m_size);
		}
		appendLog(lsn, log);
	} else {	// ������־
		// ����UNDO������Խ��LOG_IDX_DIU_DONE��־�߽磬��˴�������־ʱ����Ҫ����LOG_IDX_DIU_DONE��־
		assert(lastLog()->m_lsn == log->m_cpstForLsn);	// ������־��˳��Ӧǡ����REDO��־�෴
		if (log->m_logType == LOG_IDX_DML_CPST) {
			assert(lastLog()->m_log.m_logType == LOG_IDX_DML);
			m_table->getIndice()->redoCpstDML(m_session, lsn, log->m_data, (uint)log->m_size);
			popLog();
		} else if (log->m_logType == LOG_IDX_SMO_CPST) {
			assert(lastLog()->m_log.m_logType == LOG_IDX_SMO);
			m_table->getIndice()->redoCpstSMO(m_session, lsn, log->m_data, (uint)log->m_size);
			popLog();
		} else {
			assert(log->m_logType == LOG_IDX_SET_PAGE_CPST);
			assert(lastLog()->m_log.m_logType == LOG_IDX_SET_PAGE);
			m_table->getIndice()->redoCpstPageSet(m_session, lsn, log->m_data, (uint)log->m_size);
			popLog();
		}
	}
}

/**
 * �������DML�������еĵ�һ�׶Σ�����ع�������һ���������־
 * @return �Ƿ���Ҫ�߼�REDO
 */
bool IdxDml::physicalUndo() {
	ftrace(ts.recv, );
	while (m_idxLogs.getSize()) {
		if (lastLog()->m_log.m_logType == LOG_IDX_DIU_DONE)
			break;
		undoLog(&lastLog()->m_log, lastLog()->m_lsn);
		popLog();
	}
	// ���Ψһ��������������ϲ����Ѿ���������һ����������UPDATE��Ψһ������ʱɾ���׶�����ɣ�������ҳ�����Ѿ��ͷţ�
	// ��ʱ���ܻع�����������߼�REDO��ʣ�µĲ�������
	bool succ = m_uniqueRemain == 0 && (m_lastIdx >= 0 || m_idxLogs.getSize() > 0);
	if (!succ) {
		m_indice->logDMLDoneInRecv(m_session, m_beginLsn);
	}
	return succ;
}

/** UNDOһ��������־
 * @param log ��־����
 * @param lsn ��־LSN
 */
void IdxDml::undoLog(LogEntry *log, LsnType lsn) {
	assert(log->m_tableId == m_tableId);
	if (log->m_logType == LOG_IDX_DML) {
		m_table->getIndice()->undoDML(m_session, lsn, log->m_data, (uint)log->m_size, true);
	} else if (log->m_logType == LOG_IDX_SMO) {
		m_table->getIndice()->undoSMO(m_session, lsn, log->m_data, (uint)log->m_size, true);
	} else {
		assert(log->m_logType == LOG_IDX_SET_PAGE);
		m_table->getIndice()->undoPageSet(m_session, lsn, log->m_data, (uint)log->m_size, true);
	}
}

/**
 * ����־���뵽�����С�������ʹ�ò����Ŀ����������걾��������������ͷ�
 *
 * @param lsn ��־LSN
 * @param log ��־����
 */
void IdxDml::appendLog(LsnType lsn, const LogEntry *log) {
	LogRecord *rec = copyLog(m_session, lsn, log);
	DLink<LogRecord *> *lnk = new (m_session->getMemoryContext()->alloc(sizeof(DLink<LogRecord *>)))DLink<LogRecord *>(rec);
	m_idxLogs.addLast(lnk);
}

/** ����־������ɾ�����һ����־ */
void IdxDml::popLog() {
	assert(m_idxLogs.getSize() > 0);
	m_idxLogs.removeLast();
}

/** �����־���� */
void IdxDml::clearLog() {
	while (m_idxLogs.getSize() > 0)
		m_idxLogs.removeLast();
}

/** �õ���־���������һ����־
 *
 * @return ���һ����־
 */
LogRecord* IdxDml::lastLog() {
	assert(m_idxLogs.getSize() > 0);
	return m_idxLogs.getHeader()->getPrev()->get();
}

/** ����һ�������������
 *
 * @param table �����ı�
 * @param session �Ự����
 * @param beginLsn LOG_IDX_DML_BEGIN��־��LSN
 * @param record ����ļ�¼��ΪREC_REDUNDANT��ʽ 
 */ 
IdxInsert::IdxInsert(Table *table, Session *session, u64 beginLsn, const Record *record): IdxDml(table, session, beginLsn) {
	assert(!record || record->m_format == REC_REDUNDANT);
	m_record = record;
}

/** @see IdxDml::logicalRedo */
void IdxInsert::logicalRedo() {
	assert(m_uniqueRemain == 0 && m_lastIdx >= 0);
	if (m_idxRemain == 0) {
		m_indice->logDMLDoneInRecv(m_session, m_beginLsn, true);
		return;
	}
	m_indice->recvInsertIndexEntries(m_session, m_record, (u8)m_lastIdx, m_beginLsn);
}

/** ����һ���������¶���
 *
 * @param table �����ı�
 * @param session �Ự����
 * @param beginLsn LOG_IDX_DML_BEGIN��־��LSN
 * @param before ��������ǰ��ΪREC_REDUNDANT��ʽ
 * @param update �������������Ժ���ΪREC_REDUNDANT��ʽ
 */ 
IdxUpdate::IdxUpdate(Table *table, Session *session, u64 beginLsn, PreUpdateLog	*puLog) 
	: IdxDml(table, session, beginLsn) {
	assert(puLog->m_indexPreImage->m_format == REC_REDUNDANT);
	assert(puLog->m_subRec->m_format == REC_REDUNDANT);
	m_before = puLog->m_indexPreImage;
	m_update = puLog->m_subRec;
	m_puLog = puLog;
	// ��������漰������������Ψһ����������
	u16 *updateIndiceNo;

	m_indice->getUpdateIndices(session->getMemoryContext(), puLog->m_subRec, &m_idxTotal, &updateIndiceNo, &m_uniqueRemain);
	assert(m_idxTotal <= Limits::MAX_INDEX_NUM);
	m_idxRemain = m_idxTotal;

	memcpy(&m_updateIdxNo, updateIndiceNo, sizeof(m_updateIdxNo[0]) * m_idxTotal);
}

/** @see IdxDml::logicalRedo */
void IdxUpdate::logicalRedo() {
	assert(m_uniqueRemain == 0 && (m_lastIdx >= 0 || m_idxLogs.getSize() > 0));

	// �޸ĺ����еĴ��ֶ���ΪREC_MYSQL��¼��ʽ
	for(u16 curLob = 0 ;curLob < m_puLog->m_numLobs; curLob++) {
		u16 lobCno = m_puLog->getNthUpdateLobsCno(curLob + 1);
		RecordOper::writeLob(m_update->m_data, m_tableDef->m_columns[lobCno], m_puLog->m_lobs[curLob]);
		RecordOper::writeLobSize(m_update->m_data, m_tableDef->m_columns[lobCno], m_puLog->m_lobSizes[curLob]);
	}

	if (m_idxLogs.getSize() > 0) {
		// �д���һ���UPDATE�������Ȳ�����INSERT����
		RecordOper::mergeSubRecordRR(m_tableDef, m_update, m_before);
		m_lastIdx = m_table->getIndice()->recvCompleteHalfUpdate(m_session, m_update, (u8)m_lastIdx, m_idxTotal, m_updateIdxNo, (bool)(m_puLog->m_numLobs));
		m_idxRemain--;
	}
	if (m_idxRemain == 0) {
		m_indice->logDMLDoneInRecv(m_session, m_beginLsn, true);
		return;
	}
	m_indice->recvUpdateIndexEntries(m_session, m_before, m_update, (u8)m_lastIdx, m_beginLsn, (bool)(m_puLog->m_numLobs));
}

/** ����һ������ɾ������
 *
 * @param table �����ı�
 * @param session �Ự����
 * @param beginLsn LOG_IDX_DML_BEGIN��־��LSN
 * @param before ��������ǰ��ΪREC_REDUNDANT��ʽ
 */ 
IdxDelete::IdxDelete(Table *table, Session *session, u64 beginLsn, const SubRecord *before) 
	: IdxDml(table, session, beginLsn) {
	assert(before->m_format == REC_REDUNDANT);
	m_before = before;
}

/** @see IdxDml::logicalRedo */
void IdxDelete::logicalRedo() {
	assert(m_uniqueRemain == 0 && m_lastIdx >= 0);
	if (m_idxRemain == 0) {
		m_indice->logDMLDoneInRecv(m_session, m_beginLsn, true);
		return;
	}
	Record rec(m_before->m_rowId, REC_REDUNDANT, m_before->m_data, m_tableDef->m_maxRecSize);
	m_indice->recvDeleteIndexEntries(m_session, &rec, (u8)m_lastIdx, m_beginLsn);
}

/**
 * ����һ����־
 *
 * @param session �Ự���������ݴӸûỰ��MemoryContext�з����ڴ�
 * @param lsn ��־LSN
 * @param log ��־����
 * @return ���ݵ���־��¼
 */
static LogRecord* copyLog(Session *session, LsnType lsn, const LogEntry *log) {
	LogRecord *rec = (LogRecord *)session->getMemoryContext()->alloc(sizeof(LogRecord));
	rec->m_lsn = lsn;
	rec->m_log = *log;
	rec->m_log.m_data = (byte *)session->getMemoryContext()->alloc(log->m_size);
	memcpy(rec->m_log.m_data, log->m_data, log->m_size);
	return rec;
}


}
