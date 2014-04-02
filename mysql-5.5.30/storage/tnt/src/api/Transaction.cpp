/**
 * 事务模块实现
 *
 * @author 汪源(wangyuan@corp.netease.com, wy@163.org)
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
= NTSE物理恢复机制说明

这里说明NTSE的物理恢复机制，所谓物理恢复指的是这里的日志通常用于标识对某某数据页进行了某某更新，而不是对
数据库中的某个逻辑对象，如以主键标识的某一记录进行了什么更新。与之相对的，MySQL的binlog是一种逻辑恢复机制，
只要数据库底层通过物理恢复机制保证了数据的完整性，逻辑恢复机制通常可以以一种与底层无关的方式在上层实现。

由于这里的说明只涉及物理恢复，为了文字上的简洁，以下只称NTSE的恢复机制，略去物理二字，但实现指的是物理恢复
机制。

== 总体说明

NTSE的恢复机制只支持以下两种恢复:
1. 软件或系统崩溃后的恢复，恢复时的初始状态是崩溃那一刻的状态；
2. 根据备份恢复时，基于备份的数据和与备份相配套的日志进行恢复；

这里不支持介质故障的恢复，也就是说，以一个较老的数据库为基础，通过应用后续事务日志恢复到任意指定的时间点
是不行的，这会带来两个后果：
1. NTSE的恢复机制将无法支持通过归档日志的方式实现增量备份；
2. NTSE将恢复机制无法支持通过log-shipping的方式实现主从备份；

但我们认为这两个后果是可以接受的，基于以下原因：
1. NTSE主要作为MySQL的存储引擎使用，这时增量备份和主从备份都通过MySQL提供的binlog机制实现了；
2. 即使NTSE要独立实现以上两个功能，通过逻辑恢复机制在上层就可以实现，而逻辑恢复的实现通常更简单，不易出错。

限制恢复只支持以上两种恢复的好处是可能简化恢复的实现，比如备份是DDL操作是被禁止的因此在恢复时不需要考虑，
创建表时是先分配表ID再写表日志，因此在恢复时处理建表日志时，控制文件一定已经被更新成要建的表的ID已经分配
出去的状态。 

*/

using namespace std;

namespace ntse {

static LogRecord* copyLog(Session *session, LsnType lsn, const LogEntry *log);

/** 索引DML操作，使用物理恢复与逻辑恢复相结合的方法进行恢复。一个记录操作对多个索引的修改
 * 被LOG_IDX_DMLDONE_IDXNO日志划分成多个部分。先修改唯一性索引再修改非唯一性索引，所有IUD操作
 * 是要重做还是回滚取决于对唯一性索引的修改是否完成。
 * 恢复逻辑如下:
 * 1. 如果唯一性索引操作没有完成，则进行物理UNDO，并回滚事务；
 * 2. 如果唯一性索引操作已经完成，则先物理UNDO操作到一半的非唯一性索引，然后逻辑REDO剩下的索引
 *    操作，并提交事务；
 *
 * 设表中包含唯一性索引U1, U2, ..., Um，非唯一性索引I1, I2, ..., In，则正常情况下产生的日志如下:
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
 * 如果在操作唯一性索引过程中失败，则进行物理UNDO时产生反序的补偿日志，其中不包含LOG_IDX_DMLDONE_IDXNO
 * 对应的补偿日志，这类情况下的处理简单。
 *
 * 如果在操作某非唯一性索引过程中失败，则会对还没有完成处理的索引先进行物理UNDO，然后再
 * 进行逻辑REDO，逻辑REDO产生的日志与正常操作流程下产生的日志是相同的，因此对某个非唯一
 * 性索引Ik操作产生的日志片断
 * DML_XXX+
 * DMLDONE_IDXNO(Ik)
 * 在恢复后可能变成
 * DML_XXX[n]
 * DML_XXX_CPST[n]
 * DML_XXX+
 * DMLDONE_IDXNO(Ik)
 * 如果在逻辑REDO过程中又崩溃，则恢复后又会增加一个
 * DML_XXX[n]
 * DML_XXX_CPST[n]
 * 日志片断，这样的日志片断可能会有任意多个。
 *
 * 在任何情况下，无论崩溃恢复了多少次，DMLDONE_IDXNO日志的个数都等于已经操作完成的索引数。
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
	Table				*m_table;	/** 操作的表 */
	DrsIndice			*m_indice;	/** 索引 */
	TableDef			*m_tableDef;/** 表定义 */
	Session				*m_session;	/** 会话对象 */
	u64					m_beginLsn;	/** LOG_IDX_DML_BEGIN日志的LSN */
	u16					m_tableId;	/** 操作的表ID */
	u16					m_idxRemain;/** 未处理的索引个数 */
	u16					m_uniqueRemain;	/** 未处理的唯一性索引个数 */
	s16					m_lastIdx;	/** 最后一个已经处理完的索引号 */
	DList<LogRecord *>	m_idxLogs;	/** 索引操作日志 */
};

/** 索引插入操作 */
class IdxInsert: public IdxDml {
public:
	IdxInsert(Table *table, Session *session, u64 beginLsn, const Record *record);
	virtual void logicalRedo();

private:
	const Record	*m_record;	/** 插入的记录，为RED_REDUNDANT格式 */
};

/** 索引更新操作 */
class IdxUpdate: public IdxDml {
public:
	IdxUpdate(Table *table, Session *session, u64 beginLsn, PreUpdateLog *puLog);
	virtual void logicalRedo();

private:
	const SubRecord	*m_before;	/** 索引属性前像 */
	SubRecord		*m_update;	/** 更新的索引属性后像 */
	PreUpdateLog	*m_puLog;	/** 预更新日志 */
	u16				m_idxTotal;	/** 更新总共会涉及多少个索引 */
	u16				m_updateIdxNo[Limits::MAX_INDEX_NUM];	/** 所有需要更新的索引序号 */
};

/** 索引删除操作 */
class IdxDelete: public IdxDml {
public:
	IdxDelete(Table *table, Session *session, u64 beginLsn, const SubRecord *before);
	virtual void logicalRedo();

private:
	const SubRecord	*m_before;	/** 索引属性前像 */
};

/** 插入记录事务
 * 正常情况下的日志序列如下:
 *   可选的大对象插入日志，可能是LOG_LOB_INSERT或LOG_HEAP_INSERT(虚拟表)类型
 *   LOG_HEAP_INSERT: 插入主表记录
 *   索引日志序列
 *
 *   出错时
 *     LOG_HEAP_DELETE: 删除主表记录
 *     可选的删除大对象，可能是LOG_LOB_DELETE或LOG_HEAP_DELETE(虚拟表)类型
 *   LOG_TXN_END
 */
class InsertTrans: public Transaction {
public:
	InsertTrans(Database *db, u16 id, Table *table): Transaction(db, id, table) {
		ftrace(ts.recv, tout << db << id << table->getPath());
		m_tableDef = m_table->getTableDef(true, m_session);
		m_record.m_rowId = INVALID_ROW_ID;

		//由于对于压缩表，后面在redo时可能会对压缩记录进行解压缩，所以必须把压缩记录提取器保存下来
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
		// 内存都在MemoryContext中分配，不需要释放
	}

	/** @see Transaction::redo */
	virtual void redo(LsnType lsn, const LogEntry *log) throw(NtseException) {
		ftrace(ts.recv, tout << lsn << log);
		if (m_record.m_rowId != INVALID_ROW_ID) {
			nftrace(ts.recv, tout << "rid: " << m_record.m_rowId);
		}
_start:
		if (m_stat == STAT_WAIT_LOB_INSERT) {	// 插入大对象过程
			assert(m_numLobs < m_tableDef->getNumLobColumns());
			if (log->m_logType == LOG_LOB_INSERT) {	// 大型大对象
				assert(log->m_tableId == m_tableId);
				m_lobIds[m_numLobs++] = m_table->getLobStorage()->redoBLInsert(m_session, lsn, log->m_data, (uint)log->m_size);
			} else if (log->m_logType == LOG_HEAP_INSERT) {	// 小型大对象或堆插入
				if (log->m_tableId == TableDef::getVirtualLobTableId(m_tableId))	// 小型大对象
					m_lobIds[m_numLobs++] = m_table->getLobStorage()->redoSLInsert(m_session, lsn, log->m_data, (uint)log->m_size);
				else {	// 某些大对象为NULL时可能出现这一情况
					assert(log->m_tableId == m_tableId);
					m_stat = STAT_WAIT_HEAP_INSERT;
					goto _start;
				}
			} else {	// 插入大对象过程没有完成时即回滚
				assert(log->m_logType == LOG_LOB_DELETE || log->m_logType == LOG_HEAP_DELETE);
				m_stat = STAT_WAIT_LOB_DELETE;
				goto _start;
			}
			if (m_numLobs == m_tableDef->getNumLobColumns())
				m_stat = STAT_WAIT_HEAP_INSERT;
		} else if (m_stat == STAT_WAIT_HEAP_INSERT) {	// 大对象插入过程完成，或没有大对象
			if (log->m_logType == LOG_HEAP_INSERT) {
				assert(log->m_tableId == m_tableId);
				m_table->getHeap()->redoInsert(m_session, lsn, log->m_data, (uint)log->m_size, &m_record);
				nftrace(ts.recv, tout << t_rec(m_tableDef, &m_record));
				if (m_tableDef->m_numIndice > 0)
					m_stat = STAT_HEAP_INSERTED;
				else
					m_stat = STAT_DONE;
			} else {	// 大对象插入完成后堆记录没有插入时即回滚
				assert(log->m_logType == LOG_LOB_DELETE || log->m_logType == LOG_HEAP_DELETE);
				m_stat = STAT_WAIT_LOB_DELETE;
				goto _start;
			}
		} else if (m_stat == STAT_HEAP_INSERTED) {// 堆中插入刚刚插入
			if (log->m_logType == LOG_IDX_DML_BEGIN) {
				assert(log->m_tableId == m_tableId);
				m_redRec.m_format = REC_REDUNDANT;
				m_redRec.m_rowId = m_record.m_rowId;
				m_redRec.m_size = m_tableDef->m_maxRecSize;
				bool indexOnly = m_tableDef->m_indexOnly;
				if (!indexOnly) {
					if (m_record.m_format == REC_COMPRESSED) {//如果记录是压缩的，则先转化为REDUNDANT格式的
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
				// 回滚时删除堆中记录
				assert(log->m_logType == LOG_HEAP_DELETE);
				m_stat = STAT_WAIT_HEAP_DELETE;
				goto _start;
			}
		} else if (m_stat == STAT_WAIT_HEAP_DELETE) {	// 已经决定要回滚，但还没删除堆中记录
			assert(log->m_logType == LOG_HEAP_DELETE);
			assert(log->m_tableId == m_tableId);
			m_table->getHeap()->redoDelete(m_session, lsn, log->m_data, (uint)log->m_size);
			if (m_numLobs > 0)
				m_stat = STAT_WAIT_LOB_DELETE;
			else
				m_stat = STAT_DONE;
		} else if (m_stat == STAT_IDX_DML) {	// 索引DML操作过程中
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
			if (log->m_logType == LOG_LOB_DELETE) {	// 删除大型大对象
				assert(log->m_tableId == m_tableId);
				m_table->getLobStorage()->redoBLDelete(m_session, lsn, log->m_data, (uint)log->m_size);
			} else {	// 删除小型大对象
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
			// 堆中的记录已经插入，回滚时删除之
			m_table->getHeap()->del(m_session, m_record.m_rowId);
			m_stat = STAT_WAIT_LOB_DELETE;
		}
		if (m_stat == STAT_WAIT_LOB_INSERT || m_stat == STAT_WAIT_HEAP_INSERT)
			m_stat = STAT_WAIT_LOB_DELETE;
		if (m_stat == STAT_WAIT_LOB_DELETE) {
			// 删除各大对象
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

	int		m_stat;			/** 事务进行到哪个步骤 */
	Record	m_record;		/** 插入的记录 */
	Record	m_redRec;		/** REC_REDUNDANT格式的记录 */
	LobId	*m_lobIds;		/** 各大对象的ID */
	u16		m_numLobs;		/** 已经成功插入的大对象个数 */
	IdxInsert	*m_idxInsert;	/** 索引插入操作 */
	bool	m_idxFail;		/** 索引操作是否失败 */
	TableDef	*m_tableDef;/** 表定义 */
	CmprssRecordExtractor *m_cprsRcdExtractor;
};
const int InsertTrans::STAT_WAIT_LOB_INSERT = 1;	/** 等待插入大对象日志，表中包含大对象时的初始状态 */
const int InsertTrans::STAT_WAIT_HEAP_INSERT = 2;	/** 等待堆插入日志 */
const int InsertTrans::STAT_HEAP_INSERTED = 3;		/** 堆中记录刚刚插入 */
const int InsertTrans::STAT_IDX_DML = 4;			/** 操作索引过程中 */
const int InsertTrans::STAT_WAIT_HEAP_DELETE= 5;	/** 回滚时等待删除堆中记录 */
const int InsertTrans::STAT_WAIT_LOB_DELETE = 6;	/** 回滚时等待删除大对象 */
const int InsertTrans::STAT_DONE= 7;				/** 完成 */

/**
 * 得到被更新的第n个大对象的属性号
 *
 * @param nthUpdateLobs 指定是第几个被更新的大对象，从1开始编号
 * @return 该大对象的属性号
 */
u16 PreUpdateLog::getNthUpdateLobsCno(u16 nthUpdateLobs) {
	assert(nthUpdateLobs <= m_numLobs);
	return m_lobCnos[nthUpdateLobs - 1];
}

/** 更新记录事务
 * 日志序列
 *   LOG_PRE_UPDATE
 *   索引日志序列
 *   失败时
 *     LOG_TXN_END
 *   成功时
 *     更新大对象
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
		// 内存都在MemoryContext中分配，不需要释放
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
				if (log->m_logType == LOG_LOB_UPDATE) {	// 更新大型大对象
					assert(log->m_tableId == m_tableId);
					newLid = m_update->m_lobIds[m_currLob];
					m_table->getLobStorage()->redoBLUpdate(m_session, m_update->m_lobIds[m_currLob], lsn, log->m_data,
						(uint)log->m_size, m_update->m_lobs[m_currLob], m_update->m_lobSizes[m_currLob], m_tableDef->m_compressLobs);
				} else if (log->m_logType == LOG_HEAP_UPDATE) {	// 小型大对象更新后还是小型
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
			} else if (log->m_logType == LOG_HEAP_DELETE) {	// 小型大对象更新后变成大型大对象或更新为NULL
				assert(log->m_tableId == TableDef::getVirtualLobTableId(m_tableId));
				m_table->getLobStorage()->redoSLDelete(m_session, m_update->m_lobIds[m_currLob],
					lsn, log->m_data, (uint)log->m_size);
				if (m_update->m_lobs[m_currLob])
					m_stat = STAT_WAIT_BL_INSERT;
				else {
					u16 lobCno = m_update->getNthUpdateLobsCno(m_currLob + 1);
					RecordOper::writeLobId(m_update->m_subRec->m_data, m_tableDef->m_columns[lobCno], INVALID_LOB_ID);
				}
			} else if (log->m_logType == LOG_HEAP_INSERT) {	// NULL更新成小型大对象
				assert(log->m_tableId == TableDef::getVirtualLobTableId(m_tableId));
				LobId newLid = m_table->getLobStorage()->redoSLInsert(m_session, lsn, log->m_data, (uint)log->m_size);
				u16 lobCno = m_update->getNthUpdateLobsCno(m_currLob + 1);
				RecordOper::writeLobId(m_update->m_subRec->m_data, m_tableDef->m_columns[lobCno], newLid);
			} else if (log->m_logType == LOG_LOB_INSERT) {	// NULL更新成大型大对象
				assert(log->m_tableId == m_tableId);
				LobId newLid = m_table->getLobStorage()->redoBLInsert(m_session, lsn, log->m_data, (uint)log->m_size);
				u16 lobCno = m_update->getNthUpdateLobsCno(m_currLob + 1);
				RecordOper::writeLobId(m_update->m_subRec->m_data, m_tableDef->m_columns[lobCno], newLid);
			} else {	// 大型大对象更新成NULL
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
			// 还没REDO索引更新，这时即使已经读到了预更新日志，由于所有操作都未生效
			// 不需要REDO
			m_session->endTransaction(false);
			return false;
		}
		if (m_idxUpdate) {	// 索引操作处理到一半
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
	/** 进行逻辑REDO，保证UPDATE操作执行成功 */
	void logicalRedo() {
		// 先处理更新到一半的大对象
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
					// 非NULL更新成非NULL
					newId = m_table->getLobStorage()->update(m_session, oldId,
						newLob, m_update->m_lobSizes[m_currLob], m_tableDef->m_compressLobs);
					assert(newId != INVALID_LOB_ID);
				} else if (oldId != INVALID_LOB_ID && !newLob) {
					// 非NULL更新成NULL
					m_table->getLobStorage()->del(m_session, oldId);
					newId = INVALID_LOB_ID;
				} else if (oldId == INVALID_LOB_ID && newLob) {
					// NULL更新成非NULL
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

	int			m_stat;			/** 操作执行到什么状态 */
	PreUpdateLog	*m_update;	/** 更新操作内容 */
	bool		m_isFail;		/** （索引更新）有没成功 */
	u16			m_currLob;		/** 当前等待处理的大对象 */
	IdxUpdate	*m_idxUpdate;	/** 索引更新操作 */
	TableDef	*m_tableDef;	/** 表定义 */
};
const int UpdateTrans::STAT_START = 1;				/** 初始状态 */
const int UpdateTrans::STAT_WAIT_IDX_DML = 2;		/** 等待索引DML操作 */
const int UpdateTrans::STAT_IDX_DML = 3;			/** 正在处理索引更新 */
const int UpdateTrans::STAT_WAIT_LOB_UPDATE = 4;	/** 准备进行大对象更新 */
const int UpdateTrans::STAT_WAIT_BL_INSERT = 5;		/** 将小型大对象更新为大型大对象时，小型已删除，等待插入大型 */
const int UpdateTrans::STAT_WAIT_REC_UPDATE = 6;	/** 等待更新堆或MMS中记录 */
const int UpdateTrans::STAT_DONE = 7;				/** 完成 */

/** 删除记录事务
 *  日志序列
 *    LOG_PRE_DELETE: 预删除日志包含将被删除的记录RID
 *    索引日志序列
 *    失败时
 *      LOG_TXN_END
 *    (LOG_HEAP_DELETE | LOG_LOB_DELETE)*: 删除大对象
 *    LOG_HEAP_DELETE: 删除堆中记录
 *    LOG_TXN_END
 */
class DeleteTrans: public Transaction {
public:
	/**
	 * 创建删除记录事务对象
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

	/** 销毁删除记录事务对象 */
	virtual ~DeleteTrans() {
		// 内存都在MemoryContext中分配，不需要释放
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
			if (log->m_logType == LOG_HEAP_DELETE) {	// 删除小型大对象
				assert(log->m_tableId == TableDef::getVirtualLobTableId(m_tableId));
				m_table->getLobStorage()->redoSLDelete(m_session, m_log->m_lobIds[m_currLob], lsn, log->m_data, (uint)log->m_size);
			} else {	// 删除大型大对象
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
		if (m_stat == STAT_WAIT_LOB_DELETE) {	// 大对象还没完全删除
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

	int			m_stat;		/** 事务执行状态 */
	PreDeleteLog	*m_log;	/** 预删除日志 */
	IdxDelete	*m_idxDelete;	/** 索引操作序列 */
	bool		m_isFail;	/** 索引操作是否失败 */
	u16			m_currLob;	/** 已经删除的大对象个数 */
	TableDef	*m_tableDef;/** 表定义 */
};

const int DeleteTrans::STAT_START = 1;				/** 初始状态 */
const int DeleteTrans::STAT_WAIT_IDX_DML = 2;		/** 等待索引DML操作 */
const int DeleteTrans::STAT_IDX_DML = 3;			/** 正在处理索引更新 */
const int DeleteTrans::STAT_WAIT_LOB_DELETE = 4;	/** 准备删除大对象 */
const int DeleteTrans::STAT_WAIT_HEAP_DELETE = 5;	/** 准备删除堆中记录 */
const int DeleteTrans::STAT_DONE = 6;				/** 完成 */

/**
 * MMS刷脏记录到日志
 *
 * 日志序列
 *	LOG_MMS_UPDATE
 *  ...
 *  LOG_MMS_UPDATE
 */
class MmsFlushDirtyTrans: public Transaction {
public:
	/** 创建一个MMS刷脏记录到日志的事务对象
	 * @see Transaction::Transaction
	 */
	MmsFlushDirtyTrans(Database *db, u16 id, u16 tableId, Table *table): Transaction(db, id, table) {
		m_tableDef = m_table->getTableDef(true, m_session);
		m_session->startTransaction(TXN_MMS_FLUSH_DIRTY, m_tableDef->m_id, false);
		m_isSlob = TableDef::tableIdIsVirtualLob(tableId);
	}
	/** 销毁一个MMS刷脏记录到日志的事务对象 */
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

	int		m_stat;			/** 事务当前执行状态 */
	bool	m_isSlob;		/** 是否小型大对象的mms日志 */
	TableDef	*m_tableDef;/** 表定义 */
};

const int MmsFlushDirtyTrans::STAT_START = 1;				/** 初始状态 */
const int MmsFlushDirtyTrans::STAT_WAIT_MMS_UPDATE = 2;		/** 等待更多的MMS_UPDATE */

/**
 * MMS更新记录到堆事务，有两种情况，一是更新普通表记录一是更新小型
 * 大对象虚拟表记录
 *
 * 日志序列
 *   LOG_PRE_UPDATE_HEAP
 *   LOG_HEAP_UPDATE
 */
class UpdateHeapTrans: public Transaction {
public:
	/** 创建一个MMS更新记录到堆事务对象
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

	/** 销毁一个MMS更新记录到堆事务对象 */
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

	int		m_stat;			/** 事务当前执行状态 */
	SubRecord	*m_update;	/** 更新内容 */
	bool	m_isSlob;		/** 是否为更新小型大对象记录 */
	TableDef	*m_tableDef;/** 表定义 */
};
const int UpdateHeapTrans::STAT_START = 1;				/** 初始状态 */
const int UpdateHeapTrans::STAT_WAIT_HEAP_UPDATE = 2;	/** 已经读取到预更新日志，但还没有更新堆 */
const int UpdateHeapTrans::STAT_DONE = 3;				/** 已经更新堆 */

/**
 * 创建索引事务
 * 日志序列: 
 * LOG_IDX_ADD_INDEX
 * LOG_IDX_CREATE_BEGIN
 * (LOG_IDX_DML|LOG_IDX_SET_PAGE)+
 * LOG_IDX_DROP_INDEX? (只在建索引由于唯一性冲突失败时可能存在)
 * LOG_IDX_ADD_INDEX_CPST
 * LOG_IDX_CREATE_END
 */
class AddIndexTrans: public Transaction {
public:
	/** 创建一个创建索引事务对象
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

	/** 销毁一个创建索引事务对象 */
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
			// 恢复框架只有在lsn > flushLsn时才会进行这一事务
			// 这时堆中的表定义要么还没包含新建的索引，要么
			// 最后一个索引即是新建的索引，不可能已经进行了其它DDL操作
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

	int			m_stat;				/** 事务执行状态 */
	IndexDef	*m_indexDef;		/** 索引定义 */
	LogRecord	*m_createBeginLog;	/** 开始创建日志 */
	bool		m_isFail;			/** 是否失败 */
};
const int AddIndexTrans::STAT_START = 1;		/** 初始状态 */
const int AddIndexTrans::STAT_WAIT_IDX_CREATE = 2;	/** 等待创建索引日志 */
const int AddIndexTrans::STAT_ADD_INDEX = 3;	/** 创建索引过程中 */
const int AddIndexTrans::STAT_DONE = 4;			/** 完成 */

//TNT事务新加入两种单独操作大对象的事务
#ifdef TNT_ENGINE
/** 插入大对象事务
 * 正常情况下的日志序列如下:
 *   可选的大对象插入日志，可能是LOG_LOB_INSERT或LOG_HEAP_INSERT(虚拟表)类型
 *   索引日志序列
 *
 *   出错时
 *   
 *     可选的删除大对象，可能是LOG_LOB_DELETE或LOG_HEAP_DELETE(虚拟表)类型
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
		// 内存都在MemoryContext中分配，不需要释放
	}

	/** @see Transaction::redo */
	virtual void redo(LsnType lsn, const LogEntry *log) throw(NtseException) {
		ftrace(ts.recv, tout << lsn << log);
	
_start:
		if (m_stat == STAT_WAIT_LOB_INSERT) {	// 插入大对象过程
			assert(m_numLobs < m_tableDef->getNumLobColumns());
			if (log->m_logType == LOG_LOB_INSERT) {	// 大型大对象
				assert(log->m_tableId == m_tableId);
				m_lobId = m_table->getLobStorage()->redoBLInsert(m_session, lsn, log->m_data, (uint)log->m_size);
				m_numLobs++;
			} else if (log->m_logType == LOG_HEAP_INSERT) {	// 小型大对象或堆插入
				if (log->m_tableId == TableDef::getVirtualLobTableId(m_tableId)){	// 小型大对象
					m_lobId = m_table->getLobStorage()->redoSLInsert(m_session, lsn, log->m_data, (uint)log->m_size);
					m_numLobs++;
				}	else {	// 某些大对象为NULL时可能出现这一情况
					assert(log->m_tableId == m_tableId);
					m_stat = STAT_DONE;
					goto _start;
				}
			} else {	// 插入大对象过程没有完成时即回滚
				assert(log->m_logType == LOG_LOB_DELETE || log->m_logType == LOG_HEAP_DELETE);
				m_stat = STAT_WAIT_LOB_DELETE;
				goto _start;
			}
			if (m_numLobs == 1)
				m_stat = STAT_DONE;
		}   else {
			assert(m_stat == STAT_WAIT_LOB_DELETE);
			assert(m_numLobs > 0);
			if (log->m_logType == LOG_LOB_DELETE) {	// 删除大型大对象
				assert(log->m_tableId == m_tableId);
				m_table->getLobStorage()->redoBLDelete(m_session, lsn, log->m_data, (uint)log->m_size);
			} else {	// 删除小型大对象
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
			// 删除各大对象
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

	int		m_stat;			/** 事务进行到哪个步骤 */
	LobId	m_lobId;			/** 插入的大对象Id*/
	u16		m_numLobs;		/** 已经成功插入的大对象个数 */
	TableDef	*m_tableDef;/** 表定义 */
};
const int InsertLobTrans::STAT_WAIT_LOB_INSERT = 1;	/** 等待插入大对象日志，表中包含大对象时的初始状态 */
const int InsertLobTrans::STAT_WAIT_LOB_DELETE = 2;	/** 回滚时等待删除大对象 */
const int InsertLobTrans::STAT_DONE= 3;				/** 完成 */


/** 删除大对象事务
 *  日志序列
 *    LOG_PRE_DELETE: 预删除日志包含将被删除的记录RID
 *    索引日志序列
 *    失败时
 *      LOG_TXN_END
 *    (LOG_HEAP_DELETE | LOG_LOB_DELETE)*: 删除大对象
 *    LOG_HEAP_DELETE: 删除堆中记录
 *    LOG_TXN_END
 */
class DeleteLobTrans: public Transaction {
public:
	/**
	 * 创建删除记录事务对象
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

	/** 销毁删除记录事务对象 */
	virtual ~DeleteLobTrans() {
		// 内存都在MemoryContext中分配，不需要释放
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
			if (log->m_logType == LOG_HEAP_DELETE) {	// 删除小型大对象
				assert(log->m_tableId == TableDef::getVirtualLobTableId(m_tableId));
				m_table->getLobStorage()->redoSLDelete(m_session, m_lobId, lsn, log->m_data, (uint)log->m_size);
			} else {	// 删除大型大对象
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
		//	assert(m_stat == STAT_DONE || m_stat == STAT_START );  // TODO： 这里可能有问题，大型大对象重复删除时不会写删除日志，此时读到trxEnd日志时，m_stat == STAT_WAIT_LOB_DELETE
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
		if (m_stat == STAT_WAIT_LOB_DELETE) {	// 大对象还没完全删除
			while (m_currLob < 1) {
				//针对tnt层在恢复时删除不存在的大对象，这里也需要容错
				m_table->getLobStorage()->delAtCrash(m_session, m_lobId);
				m_currLob++;
			}
			m_stat = STAT_DONE;
		}
	}


	static const int STAT_START;
	static const int STAT_WAIT_LOB_DELETE;
	static const int STAT_DONE;

	int			m_stat;		/** 事务执行状态 */
	//PreDeleteLog	*m_log;	/** 预删除日志 */
	LobId		m_lobId;	/** 预删除大对象ID */
	u16			m_currLob;	/** 已经删除的大对象个数 */
	TableDef	*m_tableDef;/** 表定义 */
};
const int DeleteLobTrans::STAT_START = 1;				/** 初始状态 */
const int DeleteLobTrans::STAT_WAIT_LOB_DELETE = 2;	/** 准备删除大对象 */
const int DeleteLobTrans::STAT_DONE = 3;				/** 完成 */


#endif


/**
 * 根据事务类型创建各类事务对象，即事务的创建工厂
 *
 * @param type 事务类型
 * @param db 数据库对象
 * @param id 事务ID
 * @param tableId 事务所操作的表的ID，可能是小型大对象虚拟表的ID
 * @param table 事务所操作的表，可能为NULL
 * @throw NtseException 得不到指定的会话
 * @return 事务对象
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
#ifdef TNT_ENGINE			//此处新增两个事务类型
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
 * 构造一个事务对象
 *
 * @param db 数据库对象
 * @param id 事务ID
 * @param table 事务所操作的表
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
 * 销毁一个事务对象
 */
Transaction::~Transaction() {
	m_db->getSessionManager()->freeSession(m_session);
	m_db->freeConnection(m_conn);
}

/** 得到事务ID
 * @return 事务ID
 */
u16 Transaction::getId() {
	return m_session->getId();
}

///////////////////////////////////////////////////////////////////////////////
// IdxDml /////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////////
/**
 * 构建IdxDml对象
 *
 * @param table 操作的表
 * @param session 会话对象
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
 * 销毁IdxDml对象
 */
IdxDml::~IdxDml() {
}

/**
 * 处理索引DML操作过程中写的日志，可能是REDO日志或补偿日志
 *
 * @param lsn 日志LSN
 * @param log 日志内容
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
	} else if (log->m_logType < LOG_CPST_MIN) {	// REDO日志
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
	} else {	// 补偿日志
		// 由于UNDO不可能越过LOG_IDX_DIU_DONE日志边界，因此处理补偿日志时不需要考虑LOG_IDX_DIU_DONE日志
		assert(lastLog()->m_lsn == log->m_cpstForLsn);	// 补偿日志的顺序应恰好与REDO日志相反
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
 * 完成索引DML操作序列的第一阶段，物理回滚操作到一半的索引日志
 * @return 是否需要逻辑REDO
 */
bool IdxDml::physicalUndo() {
	ftrace(ts.recv, );
	while (m_idxLogs.getSize()) {
		if (lastLog()->m_log.m_logType == LOG_IDX_DIU_DONE)
			break;
		undoLog(&lastLog()->m_log, lastLog()->m_lsn);
		popLog();
	}
	// 如果唯一性索引都处理完毕并且已经处理完了一个索引，若UPDATE非唯一性索引时删除阶段已完成，则索引页面锁已经释放，
	// 这时不能回滚，必须进行逻辑REDO把剩下的操作做完
	bool succ = m_uniqueRemain == 0 && (m_lastIdx >= 0 || m_idxLogs.getSize() > 0);
	if (!succ) {
		m_indice->logDMLDoneInRecv(m_session, m_beginLsn);
	}
	return succ;
}

/** UNDO一条索引日志
 * @param log 日志内容
 * @param lsn 日志LSN
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
 * 将日志加入到队列中。本函数使用参数的拷贝，调用完本函数后参数可以释放
 *
 * @param lsn 日志LSN
 * @param log 日志内容
 */
void IdxDml::appendLog(LsnType lsn, const LogEntry *log) {
	LogRecord *rec = copyLog(m_session, lsn, log);
	DLink<LogRecord *> *lnk = new (m_session->getMemoryContext()->alloc(sizeof(DLink<LogRecord *>)))DLink<LogRecord *>(rec);
	m_idxLogs.addLast(lnk);
}

/** 从日志队列中删除最后一条日志 */
void IdxDml::popLog() {
	assert(m_idxLogs.getSize() > 0);
	m_idxLogs.removeLast();
}

/** 清空日志队列 */
void IdxDml::clearLog() {
	while (m_idxLogs.getSize() > 0)
		m_idxLogs.removeLast();
}

/** 得到日志队列中最后一条日志
 *
 * @return 最后一条日志
 */
LogRecord* IdxDml::lastLog() {
	assert(m_idxLogs.getSize() > 0);
	return m_idxLogs.getHeader()->getPrev()->get();
}

/** 构造一个索引插入对象
 *
 * @param table 操作的表
 * @param session 会话对象
 * @param beginLsn LOG_IDX_DML_BEGIN日志的LSN
 * @param record 插入的记录，为REC_REDUNDANT格式 
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

/** 构造一个索引更新对象
 *
 * @param table 操作的表
 * @param session 会话对象
 * @param beginLsn LOG_IDX_DML_BEGIN日志的LSN
 * @param before 索引属性前像，为REC_REDUNDANT格式
 * @param update 被更新索引属性后像，为REC_REDUNDANT格式
 */ 
IdxUpdate::IdxUpdate(Table *table, Session *session, u64 beginLsn, PreUpdateLog	*puLog) 
	: IdxDml(table, session, beginLsn) {
	assert(puLog->m_indexPreImage->m_format == REC_REDUNDANT);
	assert(puLog->m_subRec->m_format == REC_REDUNDANT);
	m_before = puLog->m_indexPreImage;
	m_update = puLog->m_subRec;
	m_puLog = puLog;
	// 计算更新涉及的索引个数与唯一性索引个数
	u16 *updateIndiceNo;

	m_indice->getUpdateIndices(session->getMemoryContext(), puLog->m_subRec, &m_idxTotal, &updateIndiceNo, &m_uniqueRemain);
	assert(m_idxTotal <= Limits::MAX_INDEX_NUM);
	m_idxRemain = m_idxTotal;

	memcpy(&m_updateIdxNo, updateIndiceNo, sizeof(m_updateIdxNo[0]) * m_idxTotal);
}

/** @see IdxDml::logicalRedo */
void IdxUpdate::logicalRedo() {
	assert(m_uniqueRemain == 0 && (m_lastIdx >= 0 || m_idxLogs.getSize() > 0));

	// 修改后项中的大字段项为REC_MYSQL记录方式
	for(u16 curLob = 0 ;curLob < m_puLog->m_numLobs; curLob++) {
		u16 lobCno = m_puLog->getNthUpdateLobsCno(curLob + 1);
		RecordOper::writeLob(m_update->m_data, m_tableDef->m_columns[lobCno], m_puLog->m_lobs[curLob]);
		RecordOper::writeLobSize(m_update->m_data, m_tableDef->m_columns[lobCno], m_puLog->m_lobSizes[curLob]);
	}

	if (m_idxLogs.getSize() > 0) {
		// 有处理到一半的UPDATE操作，先补充完INSERT操作
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

/** 构造一个索引删除对象
 *
 * @param table 操作的表
 * @param session 会话对象
 * @param beginLsn LOG_IDX_DML_BEGIN日志的LSN
 * @param before 索引属性前像，为REC_REDUNDANT格式
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
 * 备份一条日志
 *
 * @param session 会话，备份数据从该会话的MemoryContext中分配内存
 * @param lsn 日志LSN
 * @param log 日志内容
 * @return 备份的日志记录
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
