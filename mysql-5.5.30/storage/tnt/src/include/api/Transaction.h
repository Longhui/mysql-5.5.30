/**
 * 事务
 * 事务在NTSE中指对单条记录更新原子性行为，只在故障恢复时使用。
 *
 * @author 汪源(wangyuan@corp.netease.com, wy@163.org)
 */

#ifndef _NTSE_TRANSACTION_H_
#define _NTSE_TRANSACTION_H_

#include "misc/Global.h"
#include "misc/Txnlog.h"
#include "lob/Lob.h"
#include "util/DList.h"

namespace ntse {

/** 事务类型 */
enum TxnType {
	TXN_NONE,			/** 不是事务 */
	TXN_INSERT,			/** 插入记录事务 */
	TXN_UPDATE,			/** 更新记录事务 */
	TXN_DELETE,			/** 删除记录事务 */
	TXN_UPDATE_HEAP,	/** MMS更新记录到堆时所用事务 */
	TXN_MMS_FLUSH_DIRTY,/** MMS写脏数据到日志事务 */
	TXN_ADD_INDEX,		/** 创建索引事务 */


#ifdef  TNT_ENGINE		
	TXN_LOB_INSERT,		/** 插入大对象事务*/
	TXN_LOB_DELETE,		/** 删除大对象事务*/
#endif
};

class SubRecord;
class TableDef;
/**
 * 预更新日志内容
 */
struct PreUpdateLog {
	SubRecord	*m_subRec;	/** 表示更新内容的部分记录，为REC_REDUNDANT格式 */
	SubRecord	*m_indexPreImage;	/** 涉及到索引更新时，更新所涉及索引中所有属性的前像 */
	u16		m_numLobs;		/** 更新涉及的大对象属性个数，不包含那些更新前后都为NULL的情况 */
	u16		*m_lobCnos;		/** 被更新大对象的属性号 */
	LobId	*m_lobIds;		/** 被更新大对象ID */
	uint	*m_lobSizes;	/** 被更新大对象大小 */
	byte	**m_lobs;		/** 被更新大对象内容，NULL表示更新为NULL */
	bool	m_updateIndex;	/** 是否更新索引 */

	u16 getNthUpdateLobsCno(u16 nthUpdateLobs);
};

/** 预删除日志内容 */
struct PreDeleteLog {
	RowId		m_rid;				/** 被删除记录的RID */
	SubRecord	*m_indexPreImage;	/** 表包含索引时索引属性的前像 */
	u16			m_numLobs;			/** 被删除大对象个数 */
	LobId		*m_lobIds;			/** 如果被删除的记录包含大对象，为各大对象ID（从后往前排列），否则为NULL */
};

/** 一条日志 */
struct LogRecord {
	LsnType		m_lsn;	/** LSN */
	LogEntry	m_log;	/** 日志内容 */
};

class Database;
class Table;
class Session;
class Connection;
/** 事务 */
class Transaction {
public:
	static Transaction* createTransaction(TxnType type, Database *db, u16 id, u16 tableId, Table *table) throw(NtseException);
	Transaction(Database *db, u16 id, Table *table);
	virtual ~Transaction();

	/**
	 * 重做日志
	 *
	 * @param lsn 日志序列号
	 * @param log 日志
	 * @throw NtseException 文件操作失败
	 */
	virtual void redo(LsnType lsn, const LogEntry *log) throw(NtseException) = 0;

	/**
	 * 完成事务，第一阶段，对DML操作进行物理的UNDO
	 *
	 * @param logComplete 事务日志是否完整
	 * @param commit 当事务日志完整时，事务是否提交
	 * @return 是否需要调用completePhase2
	 */
	virtual bool completePhase1(bool logComplete, bool commit) = 0;

	/** 完成事务，第二阶段，对DML操作进行逻辑的REDO */
	virtual void completePhase2() {}

	u16 getId();
	/**
	 * 获得事务所用会话
	 *
	 * @return 事务所用会话
	 */
	Session* getSession() {
		return m_session;
	}

protected:
	Database	*m_db;			/** 所操作的数据库 */
	Connection	*m_conn;		/** 数据库连接 */
	Session		*m_session;		/** 会话 */
	u16			m_tableId;		/** 操作的表ID */
	Table		*m_table;		/** 操作的表 */
};

}

#endif
