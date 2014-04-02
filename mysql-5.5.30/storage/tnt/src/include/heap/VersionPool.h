/** 提供版本池功能
 * author 忻丁峰 xindingfeng@corp.netease.com
 */
#ifndef _TNT_VERSIONPOOL_H_
#define _TNT_VERSIONPOOL_H_

#include "api/Database.h"
#include "trx/TNTTransaction.h"
#include "misc/Session.h"
#include "misc/Global.h"
#include "util/Hash.h"
#include "heap/MHeapRecord.h"

using namespace ntse;
namespace tnt {
struct VersionTables {
	Table         *m_table;    //对应版本池表
	Table         *m_rollBack; //对应回滚表
	bool		   m_hasLob;   //版本池表中是否有需要回收的大对象
};

struct VerpoolStatus {

	u64			m_reclaimLobCnt;		  /** TNT 版本池回收大对象计数 */
	u64			m_relaimLobTime;		  /** TNT 版本池回收耗时 */

	VerpoolStatus() {
		m_reclaimLobCnt = 0;		  
		m_relaimLobTime = 0;		 
	}
};

enum HeapRecStat {
	VALID = 0,     //tnt中存在可见记录
	DELETED,       //tnt中存在可见记录，但该记录已经被删除
	NTSE_VISIBLE,  //tnt中不存在可见记录，但ntse对当前事务可见
	NTSE_UNVISIBLE //tnt中不存在可见记录，ntse对当前事务也不可见
};

typedef DynHash<TblLob*, TblLob*, TblLobHasher, TblLobHasher, TblLobEqualer<TblLob*, TblLob*> > TblLobHashMap;

typedef DynHash<TrxId, TrxId> TrxIdHashMap;

class VersionPoolPolicy {
public:
	virtual RowId insert(Session *session, u8 tblIndex, TableDef *recTblDef, RowId rollBackId, u8 vtableIndex, TrxId txnId, SubRecord *update, u8 delBit, TrxId pushTxnId) = 0;
	virtual void rollBack(Session *session, u8 tblIndex, TrxId txnId) = 0;
	virtual MHeapRec *getVersionRecord(Session *session, TableDef *recTblDef, MHeapRec *heapRec, ReadView *readView, Record *destRec, HeapRecStat *stat) = 0;
	virtual ~VersionPoolPolicy() {};
};

class VersionPool: public VersionPoolPolicy
{
public:
	~VersionPool(void);
	static VersionPool* open(Database *db, Session *session, const char *basePath, u8 count) throw (NtseException);
	static void create(Database *db, Session *session, const char *basePath, u8 count) throw (NtseException);
	void close(Session *session);
	static void drop(const char *basePath, u8 count);
	RowId insert(Session *session, u8 tblIndex, TableDef *recTblDef, RowId rollBackId, u8 vTableIndex, TrxId txnId, SubRecord *update, u8 delBit, TrxId pushTxnId);
	u64 getDataLen(Session *session, u8 tblIndex);
	MHeapRec *getRollBackHeapRec(Session *session, TableDef *recTblDef, u8 vTableIndex, Record *record, RowId rollBackId);
	void rollBack(Session *session, u8 tblIndex, TrxId txnId);
	void defrag(Session *session, u8 tblIndex, bool isRecovering = false);
	MHeapRec *getVersionRecord(Session *session, TableDef *recTblDef, MHeapRec *heapRec, ReadView *readView, Record *rec, HeapRecStat *stat);

	void setVersionPoolHasLobBit(uint activeVersionPoolId, bool hasLob);

	const VerpoolStatus& getStatus();

#ifndef NTSE_UNIT_TEST
private:
#endif

	VersionPool(Database *db, VersionTables *vTables, u8 count);
	static void createVersionTable(Database *db, Session *session, const char *basePath, u8 index) throw (NtseException);
	void readAllRollBackTxnId(Session *session, u8 tableIndex, TrxIdHashMap *allRollBack);
	void defragLob(Session *session, u8 tableIndex, TrxIdHashMap *allRollBack);

	static TableDef* createVersionTableDef(const char *schemaName, const char *tableName);
	static TableDef* createRollBackTableDef(const char *schemaName, const char *tableName);

	Database	   *m_db;
	VersionTables  *m_vTables;				//版本池中的表
	VerpoolStatus  m_status;				//版本池统计信息
	u8             m_count;					//版本池中表的个数
	u16            *m_columns;				//版本池表中所有列序号

	static const char *SCHEMA_NAME;
	static const char *VTABLE_NAME;
	static const char* VTABLE_COLNAME[];

	static const char *ROLLBACK_TABLE_NAME;
	static const char *ROLLBACK_TBL_COLNAME[];

	static const u16 VERSIONPOOL_SMALLDIFF_MAX = 4000;

	static const u8 SMALL_DIFF_FORMAT = 0;
	static const u8 BIG_DIFF_FORMAT = 1;

	static const size_t ROLLBACK_IDS_CAP = 1 << 20;
};
}

#endif