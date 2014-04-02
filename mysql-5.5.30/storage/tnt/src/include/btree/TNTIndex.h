/**
* ����������ӿڷ�װ
*
* @author ��ΰ��(liweizhao@corp.netease.com)
*/
#ifndef _TNT_INDEX_H_
#define _TNT_INDEX_H_

#include "misc/Global.h"
#include "btree/Index.h"
#include "btree/MIndex.h"
#include "api/TNTDatabase.h"

using namespace ntse;

namespace ntse {
	class DrsIndexRangeScanHandle;
}

namespace tnt {

//���Զ���������Ψһ�Լ�ֵ��
#define TRY_LOCK_UNIQUE(session, ukLockManager, key) \
	session->tryLockUniqueKey(ukLockManager, key, __FILE__, __LINE__)
//����������Ψһ�Լ�ֵ��
#define LOCK_UNIQUE(session, ukLockManager, key) \
	session->lockUniqueKey(ukLockManager, key, __FILE__, __LINE__)

class TNTDatabase;
class TNTIndex;
class MIndexRangeScanHandle;

/** TNT����ɨ��״̬ */
enum TNTIndexScanState {
	ON_DRS_INDEX, /** ɨ�账��������� */
	ON_MEM_INDEX, /** ɨ�账���ڴ����� */
	ON_NONE,      /** ɨ�軹δ��ʼ */
};

struct TNTIndexStatus {
//	DBObjStats	*m_dboStats;	/** �������ݿ�����е�ͳ����Ϣ */
	u64	m_numScans;				/** ɨ����� */
	u64	m_rowsScanned;			/** ɨ��������� */
	u64	m_backwardScans;		/** ����ɨ����� */
	u64	m_rowsBScanned;			/** ����ɨ��������� */
	u64 m_numDrsReturn;			/** NTSE�������ص����� */
	u64 m_numMIdxReturn;		/** TNT�������ص����� */
	u64 m_numRLRestarts;		/** ���������ֳ�ͻ�Ĵ��� */
};

/** TNT����ɨ������Ϣ */
class TNTIdxScanHandleInfo {
public:
	TNTIdxScanHandleInfo(Session *session, const TableDef *tableDef, const IndexDef *indexDef, const SubRecord *searchKey, 
		SubRecord *redKey, SubToSubExtractor *extractor) {
		m_session  = session;
		m_tableDef = tableDef;
		m_indexDef = indexDef;
		m_scanState = ON_NONE;
		m_extractor = extractor; 
		m_searchKey = searchKey;
		m_currentKey = redKey;
		m_curKeyBufSize = redKey->m_size;
		m_pDIdxRlh = NULL;
		m_dIdxRlh = NULL;
		m_pMIdxRlh = NULL;
		m_mIdxRlh = NULL;
		m_rangeFirst= true;
		m_hasNext = true;
		m_trxLockMode = TL_NO;
	}

public:
	Session             *m_session;     /** �Ự */
	const TableDef      *m_tableDef;    /** ���� */
	const IndexDef		*m_indexDef;	/** �������� */
	TNTIndexScanState   m_scanState;    /** ɨ��״̬ */
	SubToSubExtractor   *m_extractor;   /** �Ӽ�¼��ȡ�� */
	const SubRecord     *m_searchKey;   /** ���Ҽ� */
	SubRecord           *m_currentKey;  /** ��ǰɨ���α�ָ��������� */
	uint                m_curKeyBufSize;/** �ⲿ��������������ݻ���Ĵ�С */
	RowLockHandle       **m_pDIdxRlh;   /** ָ���ڴ�����ɨ���мӵ�NTSE�ײ����������ָ�� */
	RowLockHandle	    *m_dIdxRlh;     /** �ڴ�����ɨ���мӵ�NTSE�ײ�������� */
	RowLockHandle       **m_pMIdxRlh;   /** ָ���������ɨ���мӵ�NTSE�ײ����������ָ�� */
	RowLockHandle	    *m_mIdxRlh;     /** �������ɨ���мӵ�NTSE�ײ�������� */
	bool                m_rangeFirst;   /** �Ƿ��ǵ�һ��ɨ�� */
	bool                m_hasNext;      /** �Ƿ�����һ�� */
	bool                m_isUnique;     /** �Ƿ�������Ψһ��ɨ�� */
	bool                m_isForward;    /** �Ƿ�������ɨ�� */
	TLockMode           m_trxLockMode;
};

/** TNT����ɨ���� */
class TNTIdxScanHandle {
	friend class TNTIndex;
public:
	TNTIdxScanHandle(DrsIndexRangeScanHandle *drsIdxScanHdl, MIndexRangeScanHandle *memIdxScanHdl) 
		: m_drsIdxScanHdl(drsIdxScanHdl), m_memIdxScanHdl(memIdxScanHdl), m_scanInfo(NULL) {
	}
	~TNTIdxScanHandle() {}

	void saveMemKey();
	void saveDrsKey();

	/**
	 * ����ɨ������Ϣ
	 * @param scanInfo
	 */
	inline void setScanInfo(TNTIdxScanHandleInfo *scanInfo) {
		m_scanInfo = scanInfo;
	}

	/**
	 * ��ȡɨ������Ϣ
	 * @return 
	 */
	inline TNTIdxScanHandleInfo* getScanInfo() const {
		return m_scanInfo;
	}

	/**
	 * ��ȡ�������ɨ����
	 * @return 
	 */
	inline DrsIndexRangeScanHandle * getDrsIndexScanHdl() const {
		return m_drsIdxScanHdl;
	}

	/**
	 * ��ȡ�ڴ�����ɨ����
	 * @return 
	 */
	inline MIndexRangeScanHandle * getMemIdxScanHdl() const {
		return m_memIdxScanHdl;
	}

	/**
	 * ��ȡ��ǰ��ȡ�����������İ汾��Ϣ
	 * @return 
	 */
	inline const KeyMVInfo& getMVInfo() const {
		return m_keyMVInfo;
	}

	/**
	 * ��ȡ��ǰ��ȡ����������RowId
	 * @return 
	 */
	inline RowId getRowId() const {
		return m_scanInfo->m_currentKey->m_rowId;
	}

	/**
	 * ��õ�ǰ�α��λ�ã����ڴ������������������
	 * @return 
	 */
	inline TNTIndexScanState getScanState() const {
		return m_scanInfo->m_scanState;
	}

	inline void unlatchNtseRowBoth() {
		unlatchNtseRowDrs();
		unlatchNTseRowMem();
	}

	inline void unlatchNtseRowDrs() {
		if (m_scanInfo->m_dIdxRlh) {
			m_scanInfo->m_session->unlockRow(&m_scanInfo->m_dIdxRlh);
		}
	}

	inline void unlatchNTseRowMem() {
		if (m_scanInfo->m_mIdxRlh) {
			m_scanInfo->m_session->unlockRow(&m_scanInfo->m_mIdxRlh);
		}
	}

	bool retMemDrsKeyEqual();
	bool retDrsIndexKey();
	bool retMemIndexKey();
	void moveMemDrsIndexKey();
	void moveMemIndexKey();
	void moveDrsIndexKey();

private:
	bool isOutofRange(const SubRecord *key);

	/**
	 * ����������ɼ��Ե���Ϣ
	 * @param mvInfo �ɼ�����Ϣ 
	 */
	inline void saveMVInfo(const KeyMVInfo &mvInfo) {
		m_keyMVInfo = mvInfo;
	}

	/**
	 * ����������ɼ��Ե���Ϣ
	 * @param isViable ����ҳ����Ϣ�Ƿ��ֱ���жϿɼ���
	 * @param delBit   ɾ����־λ
	 * @param version  ������RowId�汾
	 */
	inline void saveMVInfo(bool isViable = false, bool delBit = 0, RowIdVersion version = 0, bool ntseReturned = false) {
		m_keyMVInfo.m_visable = isViable;
		m_keyMVInfo.m_delBit = delBit;
		m_keyMVInfo.m_version = version;
		m_keyMVInfo.m_ntseReturned = ntseReturned;
	}

private:
	DrsIndexRangeScanHandle *m_drsIdxScanHdl;  /** ���������Χɨ���� */
	MIndexRangeScanHandle   *m_memIdxScanHdl;  /** �ڴ�������Χɨ���� */
	TNTIdxScanHandleInfo    *m_scanInfo;       /** ɨ����Ϣ */
	KeyMVInfo               m_keyMVInfo;       /** ��ǰ��ȡ�����������İ汾��Ϣ */
};

/** TNT�������� */
class TNTIndice {
public:
	TNTIndice(TNTDatabase *db, TableDef **tableDef, LobStorage *lobStorage, DrsIndice *drsIndice, MIndice *memIndice);
	~TNTIndice();

	// ���������ӿ�
	static void create(const char *path, const TableDef *tableDef) throw(NtseException);
	static void drop(const char *path) throw(NtseException);

	static TNTIndice* open(TNTDatabase *db, Session *session, TableDef **tableDef, LobStorage *lobStorage,
		DrsIndice *drsIndice, const DoubleChecker *doubleChecker);
	void close(Session *session, bool closeMIndice = true);
	void reOpen(TableDef **tableDef, LobStorage *lobStorage, DrsIndice *drsIndice);

	bool lockAllUniqueKey(Session *session, const Record *record, uint *dupIndex);
	bool lockUpdateUniqueKey(Session *session, const Record *record, u16 updateUniques, 
		const u16 *updateUniquesNo, uint *dupIndex);

	bool checkDuplicate(Session *session, const Record *record, u16 uniquesNum, 
		const u16 * uniquesNo, uint *dupIndex, DrsIndexScanHandleInfo **drsScanHdlInfo = NULL);

	void deleteIndexEntries(Session *session, const Record *record, RowIdVersion version);
	bool updateIndexEntries(Session *session, const SubRecord *before, SubRecord *after, bool isFirtsRound, RowIdVersion version);


	void undoFirstUpdateOrDeleteIndexEntries(Session *session, const Record *record);
	bool undoSecondUpdateIndexEntries(Session *session, const SubRecord *before, SubRecord *after);
	void undoSecondDeleteIndexEntries(Session *session, const Record *record);

	void dropPhaseOne(Session *session, uint idxNo);
	void dropPhaseTwo(Session *session, uint idxNo);
	void createIndexPhaseOne(Session *session, const IndexDef *indexDef, const TableDef *tblDef, 
		DrsHeap *heap) throw(NtseException);
	TNTIndex* createIndexPhaseTwo(Session *session, const IndexDef *def, uint idxNo);
	

	inline uint getIndexNum() const {
		return m_memIndice->getIndexNum();
	}

	inline uint getUniqueIndexNum() const {
		return m_drsIndice->getUniqueIndexNum();
	}

	inline DrsIndex *getDrsIndex(uint indexNo) const {
		return m_drsIndice->getIndex(indexNo);
	}

	inline MIndex *geMemIndex(uint indexNo) const {
		return m_memIndice->getIndex(indexNo);
	}

	inline DrsIndice* getDrsIndice() const {
		return m_drsIndice;
	}

	inline MIndice* getMemIndice() const {
		return  m_memIndice;
	}

	inline TNTIndex* getTntIndex(u8 indexNo) const {
		assert(indexNo < getIndexNum());
		return m_tntIndex[indexNo];
	}

	/**
	 * ����Ҫ���µĺ�����㵱ǰ����Щ������Ҫ����
	 * @param memoryContext		�ڴ���������ģ�����������µ�����������飬������ע��ռ��ʹ�ú��ͷ�
	 * @param update			���²����ĺ�����Ϣ�������ֶ���������
	 * @param updateNum			out �����ж��ٸ�������Ҫ������
	 * @param updateIndices		out ָ��memoryContext����Ŀռ䣬������Ҫ���µ�������ţ�����ź������ڲ��������к�һ��
	 * @param updateUniques		out ���ض���Ψһ������Ҫ������
	 */
	inline void getUpdateIndices( MemoryContext *memoryContext, const SubRecord *update, u16 *updateNum, 
		u16 **updateIndices, u16 *updateUniques) {
			m_drsIndice->getUpdateIndices(memoryContext, update, updateNum, 
				updateIndices, updateUniques);
	}
	
private:
	/**
	 * �����ֵ��У���
	 * @param uniqueKey
	 * @return 
	 */
	inline u64 calcCheckSum(const SubRecord *uniqueKey) const {
		assert(KEY_NATURAL == uniqueKey->m_format);
		return checksum64(uniqueKey->m_data, uniqueKey->m_size);
	}

private:
	TNTDatabase			 *m_db;				/** ���ݿ� */
	TableDef			 **m_tableDef;		/** ������Ķ��� */
	LobStorage			 *m_lobStorage;		/** ������Ĵ����洢 */
	DrsIndice            *m_drsIndice;	    /** ����������� */
	MIndice              *m_memIndice;      /** �����ڴ����� */
	TNTIndex             **m_tntIndex;      /** TNT����(������������) */

	friend class TNTTblMntAlterIndex;
};

/** TNT���� */
class TNTIndex {
public:
	TNTIndex(TNTDatabase *db, TableDef **tableDef, const IndexDef *indexDef, 
		TNTIndice *tntIndice, DrsIndex *drsIndex, MIndex *memIndex);
	~TNTIndex();

#ifdef TNT_INDEX_OPTIMIZE
	TNTIdxScanHandle* beginScanFast(Session *session, const SubRecord *key, bool forward, 
		bool includeKey, SubToSubExtractor *extractor = NULL);
	bool getNextFast(TNTIdxScanHandle *scanHandle);
	void endScanFast(TNTIdxScanHandle *scanHandle);
#endif

	TNTIdxScanHandle* beginScan(Session *session, const SubRecord *key, SubRecord *redKey, 
		bool unique, bool forward, bool includeKey, TLockMode trxLockMode, 
		SubToSubExtractor *extractor = NULL);
	bool getNext(TNTIdxScanHandle *scanHandle) throw(NtseException);
	void endScan(TNTIdxScanHandle *scanHandle);
	
	//��ʱ���ṩΨһ��ɨ��ӿڣ������Ҫ����Ψһ��ɨ��ʱͳһ�÷�Χɨ��Ľӿ�
	//bool getByUniqueKey(Session *session, MTransaction *trx, const SubRecord *key, RowId *rowId, 
	//	SubRecord *subRecord, SubToSubExtractor *extractor, bool *isVisable);

	u64 purge(Session *session, const ReadView *readView);

	void reclaimIndex(Session *session, u32 hwm, u32 lwm);

	inline DrsIndex* getDrsIndex() const {
		return m_drsIndex;
	}

	inline MIndex* getMIndex() const {
		return m_memIndex;
	}

	inline void setTableDef(TableDef **tableDef) {
		m_tableDef = tableDef;
	}

	/////////////////////////////////////////////////////////////////////////
	// ͳ����Ϣ���
	/////////////////////////////////////////////////////////////////////////
	const TNTIndexStatus& getStatus();

private:
	int compareKeys(const SubRecord *key1, const SubRecord *key2);

private:
	TableDef	   **m_tableDef;  /** �������� */
	const IndexDef *m_indexDef;  /** ������������ */
	TNTIndice      *m_tntIndice; /** TNT�������� */
	DrsIndex       *m_drsIndex;  /** ������� */
	MIndex         *m_memIndex;  /** �ڴ����� */
	TNTIndexStatus	   m_indexStatus;	/** ����ͳ��ѶϢ */
};

}

#endif