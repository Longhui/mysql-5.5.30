/**
* TNTɨ�����ģ�顣
*
* @author �εǳ�
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
	bool		m_mysqlOper;		// ��ǰ�������ͣ�MySQL�ⲿ���� = true��TNT�ڲ����� = false��
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
	Session			*m_session;			// ɨ���Ӧ��session
	ScanType		m_scanType;			// ɨ������
	OpType			m_opType;			// �������ͣ�scan/dml?
	TNTTable		*m_tntTable;		// ɨ���Ӧ��TNT Table
	TableDef		*m_tableDef;		/** ���� */
	ColList			m_readCols;			/** �ϲ�ָ��Ҫ��ȡ������ */
	bool			m_bof;				// �Ƿ�Ϊ��ʼ״̬
	bool			m_eof;				// �Ƿ񵽴���ɨ�����
	bool			m_readLob;			// �Ƿ���Ҫ��ȡ���ֶ�����
	LastRecPlace	m_lastRecPlace;		// ��һ����¼����scan����б����λ��
	uint			m_fetchDirection;	// ɨ�跽��
	MemoryContext	*m_lobCtx;			// scan�õĴ����MemoryContext
	bool			m_externalLobCtx;	// �Ƿ�Ϊ�ⲿָ��Lob MemoryContext
	TNTOpInfo		*m_opInfo;			// ��ǰscan��Ӧ�Ĳ�������

	SubRecord		*m_idxKey;			// ��ǰ�ж�Ӧ��������ֵ��KEY_PAD��ʽ
	SubRecord		*m_mysqlRow;		// ��ǰ�У�REC_MYSQL��ʽ���ռ�ָ��Mysql�ϲ�����ռ�
	SubRecord		*m_redRow;			// ��ǰ�У�REC_REDUNDANT��ʽ���ռ��ѡ������ָ��Mysql�ϲ�ռ䣬�����Լ����䣬ȡ���ڱ��Ƿ��д���� 

	Record			*m_fullRec;			// ��ǰ�У�REC_REDUNDANT��ʽ�������С���m_fullRow����m_data��
	SubRecord		*m_fullRow;			// ��ǰ�У�REC_REDUNDANT��ʽ�������С��ռ��Լ�����
	SubrecExtractor	*m_subExtractor;	// m_fullRow SubRecord��ȡ��
	bool			m_fullRowFetched;	// scan�׶Σ��Ƿ��Ѿ���ȡ��¼ȫ��

	SubRecord		*m_updateRed;		// update������ָ�����µ�SubRecord,REC_REDUNDANT��ʽ
	SubRecord		*m_updateMysql;		// update������REC_MYSQL��ʽ
	bool			m_lobUpdated;		// update�������Ƿ���±��е�lob�ֶ�

	u64 			m_rowPtr;			// Update/Delete��������ǰ�и��µ�ҳ��
	bool			m_ptrType;			// ��ʶm_rowPtr�����ͣ����Ϊtrue˵��rowPtrΪָ�룬����ΪtrxId

	StatType		m_scanRowsType;		/** ��������ɨ������ */
	u64				m_rowsFetched;		// ɨ��һ�����ض����м�¼

	// NTSE row latch ��Ϣ
	RowLockHandle	**m_pRlh;
	RowLockHandle	*m_rlh;
	
	// ���񣬼��������Ϣ
	TNTTransaction	*m_trx;				// ɨ����������	
	TLockMode		m_rowLockMode;		// ɨ������ģʽ����store_lock��external_lock����������
	TLockMode		m_tabLockMode;		// ����ģʽ��ͨ������ģʽ�Ƴ�LOCK_S -> LOCK_IS; LOCK_X -> LOCK_IX
										
	RowId			m_rowId;			// ɨ�赱ǰ�У�RowId
	RowIdVersion	m_version;			// �ڴ�����ɨ�裬��Ҫ��¼������¼�ϵ�version
	
	bool				m_singleFetch;	// �Ƿ�Ϊunique scan
	IndexDef			*m_pkey;		/** ���� */
	const SubRecord		*m_indexKey;	// ����������ֵ
	TNTIdxScanHandle	*m_indexScan;	// ����ɨ��
	IndexDef			*m_indexDef;	/** ����ɨ����������� */
	TNTIndex			*m_index;		
	bool				m_coverageIndex;// �Ƿ�Ϊ��������ɨ��
	SubToSubExtractor	*m_idxExtractor;/** ����������ȡ�Ӽ�¼����ȡ�� */

	Records::BulkOperation	*m_recInfo;

	friend class TNTTable;
};
}
#endif