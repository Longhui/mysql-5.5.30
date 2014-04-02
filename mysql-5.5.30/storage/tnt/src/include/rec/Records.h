/**
 * ��¼�������ѡ������MMS���������¼�洢�йص������ϳ�һ�������
 *
 * @author ��Դ��wy@163.org, wangyuan@corp.netease.com��
 */

#ifndef _NTSE_RECORDS_H_
#define _NTSE_RECORDS_H_

#include "misc/Global.h"
#include "misc/TableDef.h"
#include "misc/Record.h"
#include "heap/Heap.h"
#include "lob/Lob.h"
#include "mms/Mms.h"
#include "compress/RowCompress.h"

namespace ntse {

/** ɨ������ */
enum ScanType {
	ST_TBL_SCAN,	/** ��ɨ�� */
	ST_IDX_SCAN,	/** ����ɨ�� */
	ST_POS_SCAN		/** ��λɨ��(ָ��RIDȡ��¼��ɨ��) */
};

Tracer& operator << (Tracer& tracer, ScanType type);
extern const char* getScanTypeStr(ScanType type);

/** �������� */
enum OpType {
	OP_READ,	/** ֻ������ */
	OP_UPDATE,	/** ���ܶ�ɨ���ĳЩ�н���UPDATE */
	OP_DELETE,	/** ���ܶ�ɨ���ĳЩ�н���DELETE */
	OP_WRITE,	/**  ���ܱ�ʾ�����������֮һ:
				 * 1. ��Ҫ����д��������������ȷ���ǽ�ҪUPDATE����DELETE
				 *    MySQL��REPLACE�ͷ���UPDATEʱ�����ܻ�UPDATE��DELETE���޷�ȷ��
				 * 2. INSERT����
				 */
};

Tracer& operator << (Tracer& tracer, OpType opType);
extern const char* getOpTypeStr(OpType type);

class Records;
/** UPDATE/DELETE���е��޸���Ϣ */
struct RSModInfo {
public:
	RSModInfo(Session *session, Records *records, const ColList &updCols, const ColList &readCols, const ColList &extraMissCols);
	void setRow(RowId rid, const byte *redSr);
	void readMissCols(MmsRecord *mmsRec);
	
public:
	Session			*m_session;			/** �Ự */
	Records			*m_records;			/** ������¼������ */
	const TableDef	*m_tableDef;		/** �������� */
	ColList			m_updLobCols;		/** �����漰�Ĵ�������� */
	ColList			m_myMissCols;		/** ��¼����ģ�鱾����Ҫ�Ķ�ȡʱû�ж�������֮ǰҪ��ȡ������ */
	ColList			m_extraMissCols;	/** �ⲿָ������Ҫ�Ķ�ȡʱû�ж�������֮ǰҪ��ȡ������ */
	ColList			m_allMissCols;		/** ���ж�ȡʱû�ж�������֮ǰҪ��ȡ������ */
	ColList			m_allRead;			/** ����ʱ����Ҫ��ȡ������ */
	SubRecord		*m_missSr;			/** UPDATE/DELETEʱ���ڲ���ɨ��ʱδ��ȡ�����ԡ�δ��ȡ�Ĵ���󲿷ּ�¼��
										 * ���ڴ洢��¼���ڴ����ڲ����䣬ֻ���ڴ���δ��ȡ������ʱ�ŷ���
										 */
	SubrecExtractor *m_missExtractor;	/** UPDATE/DELETEʱ���ڲ��뱻������������������Ե���ȡ����ֻ���ڴ���δ��ȡ������ʱ�ŷ��� */
	SubRecord		m_redRow;			/** Ҫ���µĵ�ǰ�У�REC_REDUNDANT��ʽ������m_allRead���� */
};

/** ������Ϣ����ʾUPDATE���Ĳ������� */
struct RSUpdateInfo: public RSModInfo {
public:
	RSUpdateInfo(Session *session, Records *records, const ColList &updCols, const ColList &readCols, const ColList &extraMissCols);
	void prepareForUpdate(RowId rid, const byte *updateMysql);
	
public:
	ColList		m_updateCols;	/** ���µ����� */
	SubRecord	m_updateMysql;	/** �������ݣ�ΪREC_MYSQL��ʽ */
	SubRecord	m_updateRed;	/** �������ݣ�ΪREC_REDUNDANT��ʽ����������� */
	bool		m_updLob;		/** �Ƿ���´���� */
	bool		m_updCached;	/** ֻ���������ø��»���ļ�¼ */
	bool		m_couldTooLong;	/** ��¼����֮���Ƿ���ܳ��� */
	u16			m_newRecSize;	/** ��¼����֮��Ĵ�С����Ϊ0���ʾδ֪ */
};

/** ��¼���� */
class Records {
public:
	/** ���������������������������� */
	class BulkOperation {
	public:
		void setUpdateInfo(const ColList &updCols, const ColList &extraMissCols);
		ColList getUpdateColumns() const;
		void setDeleteInfo(const ColList &extraMissCols);
		virtual void releaseLastRow(bool retainLobs);
		virtual void end();
		
		virtual void prepareForUpdate(const SubRecord *redSr, const SubRecord *mysqlSr, const byte *updateMysql) throw(NtseException);
		void preUpdateRecWithDic(const Record *oldRcd, const SubRecord *update);
		/**
		* update�޸ĵ���Ϣ�Ƿ��ǿ���ʹ�ø��»����
		* @return
		*/
		inline bool isMmsUptCached() {
			return m_mmsRec && m_updInfo->m_updCached;
		}
		bool tryMmsCachedUpdate();
		void updateRow();
		
		void prepareForDelete(const SubRecord *redSr);
		void deleteRow();
		RSUpdateInfo *getUpdInfo() const {
			return m_updInfo;
		}
	protected:
		BulkOperation(Session *session, Records *records, OpType opType, const ColList &readCols, LockMode lockMode, bool scan);
		virtual ~BulkOperation() {}
		virtual bool shouldCheckMmsBeforeUpdate() {return false;}
		virtual DrsHeapScanHandle* getHeapScan() {return NULL;}
		bool fillMms(RowId rid, LockMode lockMode, RowLockHandle **rlh);
	private:
		void prepareForMod(const SubRecord *redSr, RSModInfo *modInfo, OpType modType);
		void checkMmsForMod(RSModInfo *modInfo, OpType modType);
		
	protected:
		Session 		*m_session; 		/** ���ݿ�Ự */
		Records			*m_records;			/** ��¼���� */
		TableDef		*m_tableDef;		/** ���� */
		OpType			m_opType;			/** �������� */
		ColList 		m_initRead; 		/** ��ȡ������ */
		LockMode		m_rowLock;			/** ����ģʽ */
		MmsRecord		*m_mmsRec;			/** MMS��¼ */
		RSModInfo		*m_delInfo;			/** DELETE�������õ��޸���Ϣ */
		RSUpdateInfo	*m_updInfo;			/** UPDATE�������õ��޸���Ϣ */
		bool			m_shouldPutToMms;	/** �Ƿ�Ӧ�ý���¼���뵽MMS */
		Record			*m_heapRec; 		/** ���ڴӶ��ж�ȡ������¼��Ϊ��Ȼ��ʽ */
		Record          *m_tryUpdateRecCache; /** ����ѹ����¼�Ľ�����棬ΪREC_COMPRESSED��ʽ��REC_VARLEN��ʽ */
	};

	/** ������ȡ���� */
	class BulkFetch: public BulkOperation {
	public:
		BulkFetch(Session *session, Records *records, bool scan, OpType opType, const ColList &readCols, 
			MemoryContext *externalLobMc, LockMode lockMode);
		bool getNext(RowId rid, byte *mysqlRow, LockMode lockMode, RowLockHandle **rlh);

#ifdef TNT_ENGINE
		bool getFullRecbyRowId(RowId rid, SubRecord *fullRow, SubrecExtractor *recExtractor);
		void readLob(const SubRecord *redSr, SubRecord *mysqlSr);
#endif

		virtual void prepareForUpdate(const SubRecord *redSr, const SubRecord *mysqlSr, const byte *updateMysql) throw(NtseException);
		virtual void releaseLastRow(bool retainLobs);
		SubRecord* getMysqlRow() {
			return &m_mysqlRow;
		}
		SubRecord* getRedRow() {
			return &m_redRow;
		}
		void setMysqlRow(byte *row);
		void afterFetch();
		
	protected:
		virtual ~BulkFetch() {}
		bool checkMmsForNewer();
		void checkDrsForNewer();

	public:
		SubRecord	m_mysqlRow;			/** ��ǰ�����¼�����ڶ��������¼��ΪREC_MYSQL��ʽ������m_initRead���� */
		SubRecord	m_redRow; 			/** ��ǰɨ���¼�������ڲ�����ɨ�����ü�¼��ΪREC_REDUNDANT��ʽ������m_initRead���� */
		SubrecExtractor 	*m_srExtractor; /** �Ӷѻ�MMS����ȡ�Ӽ�¼����ȡ�� */
		bool		m_readLob;			/** �Ƿ�Ҫ��ȡ��������ԣ������m_mysqlRow/m_redRow�Ƿ�Ϊ��ͬ���� */
		MemoryContext	*m_lobMc;		/** ���ڴ洢�����صĴ�������ݵ��ڴ���������� */
		bool		m_externalLobMc;	/** �洢��������ݵ��ڴ�����������Ƿ����ⲿָ���� */
		u32			m_readMask;			/** ���ڶ�ȡMMSʱ���Ҫ��ȡ�������Ƿ�Ϊ�� */
	};
	
	/** ��ɨ�� */
	class Scan: public BulkFetch {
	public:
		Scan(Session *session, Records *records, OpType opType, const ColList &readCols, MemoryContext *externalLobMc, 
			LockMode lockMode);
		virtual ~Scan() {}
		bool getNext(byte *mysqlRow);
		virtual void end();
		/**
		 * ��õ�ǰɨ�����ҳ�����
		 * @return 
		 */
		inline u64 getCurScanPagesCount() const {
			return m_heapScan->getScanPageCount();
		}

		/**
		 * �����ڱ�ɨ������в���ȡ����������
		 * �����û�к�����������
		 */
		inline void setNotReadLob() {
			if (m_tableDef->hasLob(m_initRead.m_size, m_initRead.m_cols))
				m_readLob = false;
		}

#ifndef TNT_ENGINE
	private:
#endif
		virtual DrsHeapScanHandle* getHeapScan() {return m_heapScan;}
		
	private:
		DrsHeapScanHandle	*m_heapScan;/** ��ɨ������ֻ�ڶ�ɨ��ʱ�ŷ��� */
	friend class Records;
	};

	/** �������²���������֮ǰ����ȡ(�ϲ㸲������ɨ�貢����) */
	class BulkUpdate: public BulkOperation {
	public:
		BulkUpdate(Session *session, Records *records, OpType opType, const ColList &readCols);
		virtual ~BulkUpdate() {}
	private:
		virtual bool shouldCheckMmsBeforeUpdate() {return true;}
	};
	
public:
	Records(Database *db, DrsHeap *heap, TableDef *tableDef);
	static void create(Database *db, const char *path, const TableDef *tableDef) throw(NtseException);
	static void drop(const char *path) throw(NtseException);
	static Records* open(Database *db, Session *session, const char *path, TableDef *tableDef, bool hasCprsDic) throw(NtseException);
	void close(Session *session, bool flushDirty);
	void flush(Session *session, bool flushHeap, bool flushMms, bool flushLob);
	void setTableId(Session *session, u16 tableId);
	void alterUseMms(Session *session, bool useMms);
	void alterMmsCacheUpdate(bool cacheUpdate);
	void alterMmsUpdateCacheTime(u16 interval);
	void alterMmsCachedColumns(Session *session, u16 numCols, u16 *cols, bool cached);
	void alterPctFree(Session *session, u8 pctFree) throw(NtseException);

	Scan* beginScan(Session *session, OpType opType, const ColList &readCols, MemoryContext *externalLobMc,
		LockMode lockMode, RowLockHandle **rlh, bool returnLinkSrc);
	BulkFetch* beginBulkFetch(Session *session, OpType opType, const ColList &readCols, MemoryContext *externalLobMc, LockMode lockMode);
	BulkUpdate *beginBulkUpdate(Session *session, OpType opType, const ColList &readCols);
	bool couldNoRowLockReading(const ColList &readCols) const;
	void prepareForInsert(const Record *mysqlRec) throw(NtseException);
	Record* insert(Session *session, Record *mysqlRec, RowLockHandle **rlh) throw(NtseException);
	void undoInsert(Session *session, const Record *redRec);
	void verifyRecord(Session *session, RowId rid, const SubRecord *expected) throw(NtseException);
	void verifyDeleted(Session *session, RowId rid);
	bool getSubRecord(Session *session, RowId rid, SubRecord *redSr, SubrecExtractor *extractor = NULL);

	bool getRecord(Session *session, RowId rowId, Record *record, LockMode lockMode = None, RowLockHandle **rlh = NULL);

	u64 getDataLength();
	void getDBObjStats(Array<DBObjStats*>* stats);

	void setTableDef(TableDef *tableDef) {
		m_tableDef = tableDef;
	}
	void setMmsCallback(MMSBinlogCallBack *mmsCallback) {
		m_mmsCallback = mmsCallback;
		if (m_mms)
			m_mms->setBinlogCallback(mmsCallback);
	}
	DrsHeap* getHeap() const {
		return m_heap;
	}
	MmsTable* getMms() const {
		return m_mms;
	}
	LobStorage* getLobStorage() const {
		return m_lobStorage;
	}

	RowCompressMng* getRowCompressMng() {
		return m_rowCompressMng;
	}
	void closeRowCompressMng();
	//�Ƿ��Ѿ������˿��õ�ѹ���ֵ�
	inline bool hasValidDictionary() {
		return (m_rowCompressMng != NULL) && (m_rowCompressMng->getDictionary() != NULL);
	}
	inline RCDictionary *getDictionary() const {
		return (m_rowCompressMng == NULL) ? NULL : m_rowCompressMng->getDictionary();
	}
	void resetCompressComponent(const char *path) throw (NtseException);
	void createTmpDictFile(const char *dicFullPath, const RCDictionary *tmpDict) throw(NtseException);

private:
	void openMms(Session *session);
	void closeMms(Session *session, bool flushDirty);
	void insertLobs(Session *session, const Record *mysqlRec, Record *redRec);
	void deleteLobs(Session *session, const byte *redRow);
	void readLobs(Session *session, MemoryContext *ctx, const SubRecord *redSr, SubRecord *mysqlSr, bool intoMms);
	bool couldLobIdsChange(const SubRecord *old, const SubRecord *mysqlSr, SubRecord *redSr);
	void updateLobs(Session *session, const SubRecord *old, const SubRecord *mysqlSr, SubRecord *redSr);

	Database	*m_db;			/** ���ݿ� */
	TableDef	*m_tableDef;	/** ���� */
	DrsHeap		*m_heap;		/** �� */
	MmsTable	*m_mms;			/** MMS */
	LobStorage	*m_lobStorage;	/** �����洢 */
	MMSBinlogCallBack *m_mmsCallback;	/** MMSдbinlog�Ļص����� */
	RowCompressMng *m_rowCompressMng;   /** ��¼ѹ������, ֻ������Ϊѹ����ʱ������, ����ΪNULL */

friend class BulkOperation;
friend class Scan;
friend class BulkUpdate;
friend struct RSModInfo;
friend struct RSUpdateInfo;
};

}

#endif

