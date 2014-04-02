/**
* TNT扫描管理模块。
*
* @author 何登成
*/
#ifndef _TNTTBLSCAN_H_
#define _TNTTBLSCAN_H_

#include "rec/Records.h"
#include "trx/TNTTransaction.h"
#include "btree/TNTIndex.h"

using namespace ntse;

struct TSModInfo;

namespace tnt {

class TNTTable;

enum LastRecPlace {
	LAST_REC_NO_WHERE,
	LAST_REC_IN_REDROW,
	LAST_REC_IN_FULLROW,
	LAST_REC_IN_IDXROW
};

struct TNTOpInfo {
	TLockMode	m_selLockType;
	bool		m_sqlStatStart;
	bool		m_mysqlHasLocked;
	bool		m_mysqlOper;		// 当前操作类型：MySQL外部操作 = true；TNT内部操作 = false；
};

class TNTTblScan {
public:
	TNTTblScan();
	~TNTTblScan() {}

	ScanType 		getType() const {return m_scanType;}
	RowId 			getCurrentRid() const {return m_rowId;}
	TLockMode		getRowLockMode() const {return m_rowLockMode;}
	TNTTable* 		getTable() const {return m_tntTable;}
	OpType			getOpType() const {return m_opType;}
	uint			getDirection() const {return m_fetchDirection;}
	TNTTransaction*   getMTransaction() const {return m_trx;}
	Session*        getSession() const {return m_session;}
	const ColList*	getReadCols() const {return &m_readCols;}
	SubRecord*		getIdxKey() const {return m_idxKey;}
	SubRecord*		getMysqlRow() const {return m_mysqlRow;}
	SubRecord*		getRedRow() const {return m_redRow;}
	SubRecord*		getFullRow() const {return m_fullRow;}
	Record*			getRecord() const {return m_fullRec;}
	u64 			getRowPtr() const {return m_rowPtr;}
	bool			getPtrType() const {return m_ptrType;}
	RowIdVersion    getVersion() const {return m_version;}

	void setRowPtr(u64 ptr) {m_rowPtr = ptr;}
	void setPtrType(bool ptrType) {m_ptrType = ptrType;}
	void setCurrentRid(RowId rowId) {m_rowId = rowId;}
	void setVersion(RowIdVersion version) {m_version = version;}
	void setCurrentData(byte *redData) {m_redRow->m_data = redData;}

	void setUpdateSubRecord(u16 numCols, u16 *columns, bool needCreate = true);
	void setRowLockType(uint rowLockType);
	void setNtseTblScan(TblScan *tblScan);
	
	bool isSnapshotRead() const {return (m_rowLockMode == TL_NO ? true : false);}
	bool isUpdateSubRecordSet() const { return m_updateRed != NULL; }
	bool isLobUpdated() const {return m_lobUpdated; }
	static StatType getTblStatTypeForScanType(ScanType scanType);
	static StatType getRowStatTypeForScanType(ScanType scanType);
	bool checkEofOnGetNext();
	bool doubleCheckRecord();
	void readLobs();
	void releaseLastRow(bool bRetainLobs);
	void determineRowLockPolicy();
	void determineScanLockMode(TLockMode lockMode);
	
private:
	Session			*m_session;			// 扫描对应的session
	ScanType		m_scanType;			// 扫描类型
	OpType			m_opType;			// 操作类型，scan/dml?
	TNTTable		*m_tntTable;		// 扫描对应的TNT Table
	TableDef		*m_tableDef;		/** 表定义 */
	ColList			m_readCols;			/** 上层指定要读取的属性 */
	bool			m_bof;				// 是否为开始状态
	bool			m_eof;				// 是否到达了扫描结束
	bool			m_readLob;			// 是否需要读取大字段属性
	LastRecPlace	m_lastRecPlace;		// 上一条记录，在scan句柄中保存的位置
	uint			m_fetchDirection;	// 扫描方向
	MemoryContext	*m_lobCtx;			// scan用的大对象MemoryContext
	bool			m_externalLobCtx;	// 是否为外部指定Lob MemoryContext
	TNTOpInfo		*m_opInfo;			// 当前scan对应的操作类型

	SubRecord		*m_idxKey;			// 当前行对应的索引键值，KEY_PAD格式
	SubRecord		*m_mysqlRow;		// 当前行，REC_MYSQL格式，空间指向Mysql上层给定空间
	SubRecord		*m_redRow;			// 当前行，REC_REDUNDANT格式。空间可选，或者指向Mysql上层空间，或者自己分配，取决于表是否含有大对象 

	Record			*m_fullRec;			// 当前行，REC_REDUNDANT格式，完整行。与m_fullRow公用m_data。
	SubRecord		*m_fullRow;			// 当前行，REC_REDUNDANT格式，完整行。空间自己分配
	SubrecExtractor	*m_subExtractor;	// m_fullRow SubRecord提取器
	bool			m_fullRowFetched;	// scan阶段，是否已经读取记录全项

	SubRecord		*m_updateRed;		// update操作，指定更新的SubRecord,REC_REDUNDANT格式
	SubRecord		*m_updateMysql;		// update操作，REC_MYSQL格式
	bool			m_lobUpdated;		// update操作，是否更新表中的lob字段

	u64 			m_rowPtr;			// Update/Delete操作，当前行更新的页面
	bool			m_ptrType;			// 标识m_rowPtr的类型，如果为true说明rowPtr为指针，否则为trxId

	StatType		m_scanRowsType;		/** 更新哪种扫描行数 */
	u64				m_rowsFetched;		// 扫描一共返回多少行记录

	// NTSE row latch 信息
	RowLockHandle	**m_pRlh;
	RowLockHandle	*m_rlh;
	
	// 事务，加锁相关信息
	TNTTransaction	*m_trx;				// 扫描所属事务	
	TLockMode		m_rowLockMode;		// 扫描行锁模式，在store_lock，external_lock函数中设置
	TLockMode		m_tabLockMode;		// 表锁模式，通过行锁模式推出LOCK_S -> LOCK_IS; LOCK_X -> LOCK_IX
										
	RowId			m_rowId;			// 扫描当前行，RowId
	RowIdVersion	m_version;			// 内存索引扫描，需要记录索引记录上的version
	
	bool				m_singleFetch;	// 是否为unique scan
	IndexDef			*m_pkey;		/** 主键 */
	const SubRecord		*m_indexKey;	// 索引搜索键值
	TNTIdxScanHandle	*m_indexScan;	// 索引扫描
	IndexDef			*m_indexDef;	/** 用于扫描的索引定义 */
	TNTIndex			*m_index;		
	bool				m_coverageIndex;// 是否为索引覆盖扫描
	SubToSubExtractor	*m_idxExtractor;/** 从索引中提取子记录的提取器 */

	Records::BulkOperation	*m_recInfo;

	friend class TNTTable;
};
}
#endif