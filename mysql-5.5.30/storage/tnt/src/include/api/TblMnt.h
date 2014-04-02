/**
* ������ά������
*
* @author л��(xieke@corp.netease.com, ken@163.org)
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


/*** RIDӳ�伯�� ***/

/** ��RIDӳ�伯������ӳ�伯�ϼ̳и��� */
class SimpleRidMapping {
	/* �����Ǹ���ӳ�� */
public:
	SimpleRidMapping() {};
	virtual ~SimpleRidMapping() {};
	/**
	* ��ʼ��������
	* ���������ʱ��Ϊ�������ٶȣ������һЩ���������粻��������
	*/
	virtual void beginBatchLoad() {};

	/**
	* ������������
	*/
	virtual void endBatchLoad() {};

	/**
	* ��ѯrid->rid'ӳ��
	* @param rid       ��Ҫ��ѯ��RowId
	* @return          ����ӳ���Ӧ��rid'������������򷵻�INVALID_ROW_ID
	*/
	virtual RowId getMapping(RowId origRid) { return origRid; }

	/**
	* ����RIDӳ��
	* ��origRid�Ѿ����ڣ��򸲸�ԭ����ӳ�䡣
	* @param origRid            ԭ���е�rowid
	* @param mappingRid         ��Ӧ�ĸ��Ʊ���rowid
	*/
	virtual void insertMapping(RowId origRid, RowId mappingRid) {
		UNREFERENCED_PARAMETER(origRid);
		UNREFERENCED_PARAMETER(mappingRid);
	};

	/**
	 * ɾ��RIDӳ��
	 * @param origRid   ԴRID
	 */
	virtual void deleteMapping(RowId origRid) {
		UNREFERENCED_PARAMETER(origRid);
	};

};

/**
 * ʹ��NTSE��ʱ������ridӳ��
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
	Database	*m_db;				/** ���ݿ� */
	Table		*m_table;			/** Դ���ݱ� */
	Table		*m_map;				/** ӳ��� */
	string		*m_mapPath;			/** ӳ������� */
	Connection	*m_conn;			/** ���� */
	Session		*m_session;			/** �Ự */
	SubRecord	*m_key, *m_subRec;	/** �Ӽ�¼�������� */
	Record		*m_redKey, *m_rec;	/** ��¼�������¼ */
	TblScan		*m_scanHdl;			/** ��ɨ���� */
};



/** ��־�ط� */

/** ��־���� */
struct LogCopy {
	LogEntry	m_logEntry;		/** ��־�� */
	LogCopy		*m_next;		/** nextָ�� */
};


/** ������id�������־ */
struct TxnLogList {
	u16			m_txnId;		/** ����id�����������id�����ܳ�ͻ */
	bool		m_valid;		/** �Ƿ���Ҫ���ǵ���־ */
	TxnType		m_type;			/** �������� */
	RowId		m_rowId;		/** ������¼��RowId */
	LogCopy		*m_first;		/** �����е�һ��log */
	LogCopy		*m_last;		/** ���������һ��log */
	LsnType     m_startLsn;     /** ������ʼ��־��lsn */
};



/** ��־�ط��� */
class LogReplay {
public:
	LogReplay(Table *table, Database *db, LsnType startLSN, LsnType endLSN, size_t logBufSize = 32 * 1024 * 1024);
	~LogReplay();
	/* ��ʼ�ط� */
	void start();
	/* �����ط� */
	void end();
	/* ��ȡ��һϵ��������־��û���򷵻�NULL */
	TxnLogList* getNextTxn(Session *session);
	/* �л��ڴ���Դ */
	void switchMemResource();
	/** ��ȡδ�����������С��lsn */
	LsnType getMinUnfinishTxnLsn();
private:
	Table						*m_table;			/** �ط���־�ı� */
	Database					*m_db;				/** ���ݿ� */
	LsnType						m_lsnStart;			/** ��ʼlsn */
	LsnType						m_lsnEnd;			/** ����lsn���ϲ���뱣֤��û��transaction�ǿ�Խ��ʼlsn�ͽ���lsn�� */
	size_t						m_bufSize;			/** ����������־�Ļ����С */
	LogScanHandle				*m_scanHdl;			/** ��־ɨ���� */
	MemoryContext				*m_memCtx;			/** �ڴ���� */
	MemoryContext				*m_memCtxBak;		/** �����ڴ���� */
	list<TxnLogList*>			*m_txnList;			/** TxnLogList���飬���ڴ���Ѿ���ɵ����� */
	list<TxnLogList*>			*m_bakList;			/** TxnLogList���鱸�ݣ����ڴ���Ѿ���ɵ����� */
	vector<TxnLogList *>		m_orderedTxn;		/** ����õ�������־ */
	map<u16, TxnLogList*>		*m_unfinished;		/** txnid��TxnlogList��ӳ�� */
	map<u16, TxnLogList*>		*m_bakMap;			/** txnid��TxnlogList��ӳ��backup */
	bool						m_logScaned;		/** ��־ɨ���Ƿ���� */
	uint						m_vecIdx;			/** �����±� */
#ifdef NTSE_UNIT_TEST
	uint		m_returnCnt;
	uint		m_shlNextCnt;
	uint		m_validStartCnt;
#endif
};

/** ����ά�������� */
class TableOnlineMaintain {
public:
	/**
	 * ����ά�������๹�캯��
	 * @param table Ҫ�����ı�
	 * @param cancelFlag ����ȡ����־
	 */
	TableOnlineMaintain(Table *table, bool *cancelFlag = NULL) 
		: m_table(table), m_cancelFlag(cancelFlag) {}
	virtual ~TableOnlineMaintain() {
		m_table = NULL;
	}

	/**
	 * ��任����
	 * ���ݸ��������ݶԱ�ṹ�������߱任
	 * @param session	�Ự
	 * @return			�����ɹ�����true
	 * @throw			�������Ϸ����ļ���������ȵȿɻָ��Ĵ���
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
	
	Table			*m_table;			/** ��������� */
	bool            *m_cancelFlag;          /** �����Ƿ�ȡ�� */
};

/** ��������ά���� */
class TblMntAlterIndex : public TableOnlineMaintain {
public:
	/** ����һ����������ά���� */
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
	u16				m_numNewIndice;					/** ά������Ŀ�������ĸ��� */
	IndexDef		**m_newIndice;					/** �µ������������� */
	u16				m_numAddIdx, m_numDelIdx;		/** ���Ӻ�ɾ����������Ŀ */
	const IndexDef	**m_addIndice;
	const IndexDef  **m_delIndice;	
};

/** ��ʱ������Ϣ */
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

	u8       m_indexNum;       /** ������Ŀ */
	IndexDef **m_indexDef;     /** ���������Ķ��� */
	IndexDef *m_newTbpkey;     /** ������������ */
	bool     m_newTbUseMms;    /** �Ƿ�ʹ��MMS */
	int      *m_indexUniqueMem;/** �������ͱ�ǣ�0��ʾ��Ψһ��������1��ʾΨһ��������2��ʾ�������� */
	bool     *m_cacheUpdateCol;/** �������Ƿ��� */
};

/** ����ά���в����� */
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
	Database            *m_db;                      /** �������ݿ� */
	RecordConvert       *m_convert;                 /** ��¼ת���� */
	Connection			*m_conn;					/** ���ݿ����� */
	u16					m_numAddCol;                /** �����е����� */
	u16                 m_numDelCol;	            /** ɾ���е����� */
	const AddColumnDef	*m_addCols;					/** �����е�����Ϣ���� */
	const ColumnDef		**m_delCols;				/** ɾ���е��ж������� */
	bool                m_keepOldDict;              /** �Ƿ������˱�����ѹ��ȫ���ֵ� */
	bool                m_newHasDict;               /** �±��Ƿ���ѹ���ֵ� */
};

/** �����޸Ķ����Ͳ����� */
class TblMntAlterHeapType : public TblMntAlterColumn {
public:
	TblMntAlterHeapType(Table *table, Connection *conn, bool *cancelFlag = NULL);
	~TblMntAlterHeapType();

protected:
	TableDef* preAlterTblDef(Session *session, TableDef *newTbdef, TempTableDefInfo *tempTableDefInfo);
};

/** ����Optimize������ */
class TblMntOptimizer : public TblMntAlterColumn {
public:
	TblMntOptimizer(Table *table, Connection *conn, bool *cancelFlag, bool keepOldDict);
	~TblMntOptimizer();
};

} // namespace ntse
#endif // #ifndef _NTSE_TABLE_MAINTAIN
