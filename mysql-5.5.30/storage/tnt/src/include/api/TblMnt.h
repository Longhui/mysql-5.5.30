/**
* 表在线维护功能
*
* @author 谢可(xieke@corp.netease.com, ken@163.org)
*/


#ifndef _NTSE_TABLE_MAINTAIN
#define _NTSE_TABLE_MAINTAIN

#include "misc/Global.h"
#include "misc/Txnlog.h"
#include "api/Transaction.h"
#include "misc/TableDef.h"
#include <vector>
#include <map>
#include <set>
#include <list>
#include <btree/Index.h>
#include "api/Table.h"
#include "misc/RecordHelper.h"

using namespace std;

namespace ntse {

class DrsIndice;

extern int duringAlterIdx;


/*** RID映射集合 ***/

/** 简单RID映射集，其他映射集合继承该类 */
class SimpleRidMapping {
	/* 父类是个空映射 */
public:
	SimpleRidMapping() {};
	virtual ~SimpleRidMapping() {};
	/**
	* 开始批量载入
	* 批量载入的时候，为了增加速度，会减少一些操作，比如不建立索引
	*/
	virtual void beginBatchLoad() {};

	/**
	* 结束批量载入
	*/
	virtual void endBatchLoad() {};

	/**
	* 查询rid->rid'映射
	* @param rid       需要查询的RowId
	* @return          返回映射对应的rid'，如果不存在则返回INVALID_ROW_ID
	*/
	virtual RowId getMapping(RowId origRid) { return origRid; }

	/**
	* 插入RID映射
	* 若origRid已经存在，则覆盖原来的映射。
	* @param origRid            原表中的rowid
	* @param mappingRid         对应的复制表中rowid
	*/
	virtual void insertMapping(RowId origRid, RowId mappingRid) {
		UNREFERENCED_PARAMETER(origRid);
		UNREFERENCED_PARAMETER(mappingRid);
	};

	/**
	 * 删除RID映射
	 * @param origRid   源RID
	 */
	virtual void deleteMapping(RowId origRid) {
		UNREFERENCED_PARAMETER(origRid);
	};

};

/**
 * 使用NTSE临时表做的rid映射
 */
class NtseRidMapping : public SimpleRidMapping {
public:
	NtseRidMapping(Database *db, Table *table);
	~NtseRidMapping();

	void init(Session *session) throw(NtseException);

	void beginBatchLoad();
	void endBatchLoad();
	RowId getMapping(RowId origRid);
	void insertMapping(RowId origRid, RowId mappingRid);
	void deleteMapping(RowId origRid);
	void startIter();
	RowId getNextOrig(RowId *mapped);
	void endIter();
	u64 getCount();
	RowId getOrig(RowId mapped);
#ifdef NTSE_UNIT_TEST
	ILMode getMapLockMode() {return m_map->getLock(m_session);}
	ILMode getMapMetaLockMode() {return m_map->getMetaLock(m_session);}
#endif

private:
	Database	*m_db;				/** 数据库 */
	Table		*m_table;			/** 源数据表 */
	Table		*m_map;				/** 映射表 */
	string		*m_mapPath;			/** 映射表名称 */
	Connection	*m_conn;			/** 连接 */
	Session		*m_session;			/** 会话 */
	SubRecord	*m_key, *m_subRec;	/** 子记录和搜索键 */
	Record		*m_redKey, *m_rec;	/** 记录和冗余记录 */
	TblScan		*m_scanHdl;			/** 表扫描句柄 */
};



/** 日志回放 */

/** 日志拷贝 */
struct LogCopy {
	LogEntry	m_logEntry;		/** 日志项 */
	LogCopy		*m_next;		/** next指针 */
};


/** 按事务id分组的日志 */
struct TxnLogList {
	u16			m_txnId;		/** 事务id，这里的事务id不可能冲突 */
	bool		m_valid;		/** 是否需要考虑的日志 */
	TxnType		m_type;			/** 事务类型 */
	RowId		m_rowId;		/** 操作记录的RowId */
	LogCopy		*m_first;		/** 事务中第一条log */
	LogCopy		*m_last;		/** 事务中最后一条log */
	LsnType     m_startLsn;     /** 事务起始日志的lsn */
};



/** 日志回放类 */
class LogReplay {
public:
	LogReplay(Table *table, Database *db, LsnType startLSN, LsnType endLSN, size_t logBufSize = 32 * 1024 * 1024);
	~LogReplay();
	/* 开始回放 */
	void start();
	/* 结束回放 */
	void end();
	/* 获取下一系列事务日志，没有则返回NULL */
	TxnLogList* getNextTxn(Session *session);
	/* 切换内存资源 */
	void switchMemResource();
	/** 获取未完成事务中最小的lsn */
	LsnType getMinUnfinishTxnLsn();
private:
	Table						*m_table;			/** 回放日志的表 */
	Database					*m_db;				/** 数据库 */
	LsnType						m_lsnStart;			/** 开始lsn */
	LsnType						m_lsnEnd;			/** 结束lsn，上层必须保证，没有transaction是跨越开始lsn和结束lsn的 */
	size_t						m_bufSize;			/** 用来拷贝日志的缓存大小 */
	LogScanHandle				*m_scanHdl;			/** 日志扫描句柄 */
	MemoryContext				*m_memCtx;			/** 内存分配 */
	MemoryContext				*m_memCtxBak;		/** 备用内存分配 */
	list<TxnLogList*>			*m_txnList;			/** TxnLogList数组，用于存放已经完成的事务 */
	list<TxnLogList*>			*m_bakList;			/** TxnLogList数组备份，用于存放已经完成的事务 */
	vector<TxnLogList *>		m_orderedTxn;		/** 排序好的事务日志 */
	map<u16, TxnLogList*>		*m_unfinished;		/** txnid到TxnlogList的映射 */
	map<u16, TxnLogList*>		*m_bakMap;			/** txnid到TxnlogList的映射backup */
	bool						m_logScaned;		/** 日志扫描是否完成 */
	uint						m_vecIdx;			/** 向量下标 */
#ifdef NTSE_UNIT_TEST
	uint		m_returnCnt;
	uint		m_shlNextCnt;
	uint		m_validStartCnt;
#endif
};

/** 在线维护操作类 */
class TableOnlineMaintain {
public:
	/**
	 * 在线维护操作类构造函数
	 * @param table 要操作的表
	 * @param cancelFlag 操作取消标志
	 */
	TableOnlineMaintain(Table *table, bool *cancelFlag = NULL) 
		: m_table(table), m_cancelFlag(cancelFlag) {}
	virtual ~TableOnlineMaintain() {
		m_table = NULL;
	}

	/**
	 * 表变换操作
	 * 根据给定的数据对表结构进行在线变换
	 * @param session	会话
	 * @return			操作成功返回true
	 * @throw			参数不合法，文件操作错误等等可恢复的错误
	 */
	virtual bool alterTable(Session *session) throw(NtseException) = 0;

#ifdef NTSE_UNIT_TEST
public:
#else
protected:
#endif
	void processLogToIndice(Session *session, LogReplay *replay, TableDef *tbdef, DrsIndice *indice, SimpleRidMapping *ridmap);
	void processLogToTable(Session *session, LogReplay *replay, RecordConvert *recconv, Table *destTb, NtseRidMapping *ridmap);
	u64 getLsnPoint(Session *session, bool lockTable, bool setOnlineLSN = false, int *onlineLsnHdl = NULL);
	TableDef *tempCopyTableDef(u16 numIndice, IndexDef **indice);
	void delTempTableDef(TableDef *tmpTbDef);
	inline bool isCancel() throw(NtseException) {
		return (m_cancelFlag != NULL && *m_cancelFlag);
	};

	virtual void reopenTblAndReplaceComponent(Session *session, const char *origTablePath, bool hasCprsDict = false);

	virtual void lockMeta(Session *session, ILMode mode, int timeoutMs, const char *file, uint line) throw(NtseException);
	virtual void upgradeMetaLock(Session *session, ILMode oldMode, ILMode newMode, int timeoutMs, const char *file, uint line) throw(NtseException);
	virtual void downgradeMetaLock(Session *session, ILMode oldMode, ILMode newMode, const char *file, uint line);
	virtual void unlockMeta(Session *session, ILMode mode);

	virtual void enableLogging(Session *session);
	virtual void disableLogging(Session *session);
	
	Table			*m_table;			/** 操作表对象 */
	bool            *m_cancelFlag;          /** 操作是否被取消 */
};

/** 在线索引维护类 */
class TblMntAlterIndex : public TableOnlineMaintain {
public:
	/** 构造一个在线索引维护类 */
	TblMntAlterIndex(Table *table, const u16 numAddIdx, const IndexDef **addIndice, 
		const u16 numDelIdx, const IndexDef **delIndice, bool *cancelFlag = NULL) 
		: TableOnlineMaintain(table, cancelFlag) {
			m_addIndice = addIndice;
			m_delIndice = delIndice;
			m_numAddIdx = numAddIdx;
			m_numDelIdx = numDelIdx;
	}
	bool alterTable(Session *session) throw(NtseException);

protected:
	bool isNewTbleHasDict();
	virtual void additionalAlterIndex(Session *session, TableDef *oldTblDef, TableDef **newTblDef, DrsIndice *drsIndice,
		const IndexDef **addIndice, u16 numAddIdx, bool *idxDeleted) {
		UNREFERENCED_PARAMETER(session);
		UNREFERENCED_PARAMETER(oldTblDef);
		UNREFERENCED_PARAMETER(newTblDef);
		UNREFERENCED_PARAMETER(drsIndice);
		UNREFERENCED_PARAMETER(addIndice);
		UNREFERENCED_PARAMETER(numAddIdx);
		UNREFERENCED_PARAMETER(idxDeleted);
		return;
	};
	
private:
	u16				m_numNewIndice;					/** 维护操作目标索引的个数 */
	IndexDef		**m_newIndice;					/** 新的索引定义数组 */
	u16				m_numAddIdx, m_numDelIdx;		/** 增加和删除的索引数目 */
	const IndexDef	**m_addIndice;
	const IndexDef  **m_delIndice;	
};

/** 临时表定义信息 */
struct TempTableDefInfo {
public:
	TempTableDefInfo() 	: m_indexNum(0), m_indexDef(NULL), m_newTbpkey(NULL), m_newTbUseMms(false), 
		m_indexUniqueMem(NULL), m_cacheUpdateCol(NULL) {
	}
	~TempTableDefInfo() {
		if (NULL != m_indexDef) {
			assert(m_indexNum > 0);
			for (u16 i = 0; i < m_indexNum; i++) {
				delete m_indexDef[i];
				m_indexDef[i] = NULL;
			}
			delete []m_indexDef;
			m_newTbpkey = NULL;
		}
	}

	u8       m_indexNum;       /** 索引数目 */
	IndexDef **m_indexDef;     /** 各个索引的定义 */
	IndexDef *m_newTbpkey;     /** 主键索引定义 */
	bool     m_newTbUseMms;    /** 是否使用MMS */
	int      *m_indexUniqueMem;/** 索引类型标记，0表示非唯一性索引，1表示唯一性索引，2表示主键索引 */
	bool     *m_cacheUpdateCol;/** 各个列是否开启 */
};

/** 在线维护列操作类 */
class TblMntAlterColumn : public TableOnlineMaintain {
public:
	TblMntAlterColumn(Table *table, Connection *conn, u16 addColNum, const AddColumnDef *addCol, 
		u16 delColNum, const ColumnDef **delCol, bool *cancelFlag = NULL, bool keepOldDict = false);
	virtual ~TblMntAlterColumn();

	virtual bool alterTable(Session *session) throw(NtseException);
	bool isNewTbleHasDict() const;

protected:
	void upgradeTblLock(Session *session);
	void upgradeTblMetaLock(Session *session);
	void createOrCopyDictForTmpTbl(Session *session, Table *origTb, Table *tmpTb, bool onlyCopy) throw(NtseException);
	RCDictionary* createNewDictionary(Session *session, Database *db, Table *table) throw (NtseException);
	
	virtual TableDef* preAlterTblDef(Session *session, TableDef *newTbdef, TempTableDefInfo *tempTableDefInfo);
	Table* createTempTable(Session *session, TableDef *newTbdef) throw(NtseException);

	void copyTable(Session *session, Table *tmpTb, NtseRidMapping *ridMap, TableDef *newTbdef) throw(NtseException);
	void copyRowsByScan(Session *session, Table *tmpTb, NtseRidMapping *ridMap) throw(NtseException);
	
	LsnType replayLogWithoutIndex(Session *session, Table *tmpTb, NtseRidMapping *ridMap, LsnType lsnStart, int *olLsnHdl);
	LsnType replayLogWithIndex(Session *session, Table *tmpTb, NtseRidMapping *ridMap, LsnType lsnStart, int *olLsnHdl);
	
	void replaceTableFile(bool tmpTbleHasLob, string &origTableFullPath, string &tmpTableFullPath);
	void rebuildIndex(Session *session, Table *tmpTb, TempTableDefInfo *tempInfo) throw(NtseException);
	void restoreTblSetting(Table *tmpTb, const TempTableDefInfo *tempInfo, TableDef *newTbdef);

	virtual void additionalAlterColumn(Session *session, NtseRidMapping *ridmap) {
		UNREFERENCED_PARAMETER(session);
		UNREFERENCED_PARAMETER(ridmap);
	};

	virtual void preLockTable() {};

	//void reopenTblAndReplaceComponent(Session *session, const char *origTablePath);

protected:
	Database            *m_db;                      /** 所属数据库 */
	RecordConvert       *m_convert;                 /** 记录转换器 */
	Connection			*m_conn;					/** 数据库连接 */
	u16					m_numAddCol;                /** 增加列的数量 */
	u16                 m_numDelCol;	            /** 删除列的数量 */
	const AddColumnDef	*m_addCols;					/** 增加列的列信息数组 */
	const ColumnDef		**m_delCols;				/** 删除列的列定义数组 */
	bool                m_keepOldDict;              /** 是否设置了保留旧压缩全局字典 */
	bool                m_newHasDict;               /** 新表是否含有压缩字典 */
};

/** 在线修改堆类型操作类 */
class TblMntAlterHeapType : public TblMntAlterColumn {
public:
	TblMntAlterHeapType(Table *table, Connection *conn, bool *cancelFlag = NULL);
	~TblMntAlterHeapType();

protected:
	TableDef* preAlterTblDef(Session *session, TableDef *newTbdef, TempTableDefInfo *tempTableDefInfo);
};

/** 在线Optimize操作类 */
class TblMntOptimizer : public TblMntAlterColumn {
public:
	TblMntOptimizer(Table *table, Connection *conn, bool *cancelFlag, bool keepOldDict);
	~TblMntOptimizer();
};

} // namespace ntse
#endif // #ifndef _NTSE_TABLE_MAINTAIN
