/**
 * 记录管理。将堆、大对象、MMS这三个与记录存储有关的组件组合成一个组件。
 *
 * @author 汪源（wy@163.org, wangyuan@corp.netease.com）
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

/** 扫描类型 */
enum ScanType {
	ST_TBL_SCAN,	/** 表扫描 */
	ST_IDX_SCAN,	/** 索引扫描 */
	ST_POS_SCAN		/** 定位扫描(指定RID取记录的扫描) */
};

Tracer& operator << (Tracer& tracer, ScanType type);
extern const char* getScanTypeStr(ScanType type);

/** 操作类型 */
enum OpType {
	OP_READ,	/** 只读操作 */
	OP_UPDATE,	/** 可能对扫描的某些行进行UPDATE */
	OP_DELETE,	/** 可能对扫描的某些行进行DELETE */
	OP_WRITE,	/**  可能表示以下两种情况之一:
				 * 1. 将要进行写操作，但还不能确定是将要UPDATE还是DELETE
				 *    MySQL的REPLACE和分区UPDATE时，可能会UPDATE或DELETE，无法确定
				 * 2. INSERT操作
				 */
};

Tracer& operator << (Tracer& tracer, OpType opType);
extern const char* getOpTypeStr(OpType type);

class Records;
/** UPDATE/DELETE共有的修改信息 */
struct RSModInfo {
public:
	RSModInfo(Session *session, Records *records, const ColList &updCols, const ColList &readCols, const ColList &extraMissCols);
	void setRow(RowId rid, const byte *redSr);
	void readMissCols(MmsRecord *mmsRec);
	
public:
	Session			*m_session;			/** 会话 */
	Records			*m_records;			/** 所属记录管理器 */
	const TableDef	*m_tableDef;		/** 所属表定义 */
	ColList			m_updLobCols;		/** 更新涉及的大对象属性 */
	ColList			m_myMissCols;		/** 记录管理模块本身需要的读取时没有读，更新之前要读取的属性 */
	ColList			m_extraMissCols;	/** 外部指定的需要的读取时没有读，更新之前要读取的属性 */
	ColList			m_allMissCols;		/** 所有读取时没有读，更新之前要读取的属性 */
	ColList			m_allRead;			/** 更新时所有要读取的属性 */
	SubRecord		*m_missSr;			/** UPDATE/DELETE时用于补齐扫描时未读取的属性、未读取的大对象部分记录，
										 * 用于存储记录的内存在内部分配，只有在存在未读取的属性时才分配
										 */
	SubrecExtractor *m_missExtractor;	/** UPDATE/DELETE时用于补齐被更新索引及大对象属性的提取器，只有在存在未读取的属性时才分配 */
	SubRecord		m_redRow;			/** 要更新的当前行，REC_REDUNDANT格式，包含m_allRead属性 */
};

/** 更新信息，表示UPDATE语句的操作特征 */
struct RSUpdateInfo: public RSModInfo {
public:
	RSUpdateInfo(Session *session, Records *records, const ColList &updCols, const ColList &readCols, const ColList &extraMissCols);
	void prepareForUpdate(RowId rid, const byte *updateMysql);
	
public:
	ColList		m_updateCols;	/** 更新的属性 */
	SubRecord	m_updateMysql;	/** 更新内容，为REC_MYSQL格式 */
	SubRecord	m_updateRed;	/** 更新内容，为REC_REDUNDANT格式，包含大对象 */
	bool		m_updLob;		/** 是否更新大对象 */
	bool		m_updCached;	/** 只更新了启用更新缓存的记录 */
	bool		m_couldTooLong;	/** 记录更新之后是否可能超长 */
	u16			m_newRecSize;	/** 记录更新之后的大小，若为0则表示未知 */
};

/** 记录管理 */
class Records {
public:
	/** 批量操作，各类批量操作的子类 */
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
		* update修改的信息是否是可以使用更新缓存的
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
		Session 		*m_session; 		/** 数据库会话 */
		Records			*m_records;			/** 记录管理 */
		TableDef		*m_tableDef;		/** 表定义 */
		OpType			m_opType;			/** 操作类型 */
		ColList 		m_initRead; 		/** 读取的属性 */
		LockMode		m_rowLock;			/** 行锁模式 */
		MmsRecord		*m_mmsRec;			/** MMS记录 */
		RSModInfo		*m_delInfo;			/** DELETE操作所用的修改信息 */
		RSUpdateInfo	*m_updInfo;			/** UPDATE操作所用的修改信息 */
		bool			m_shouldPutToMms;	/** 是否应该将记录插入到MMS */
		Record			*m_heapRec; 		/** 用于从堆中读取完整记录，为自然格式 */
		Record          *m_tryUpdateRecCache; /** 尝试压缩记录的结果缓存，为REC_COMPRESSED格式或REC_VARLEN格式 */
	};

	/** 批量读取操作 */
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
		SubRecord	m_mysqlRow;			/** 当前输出记录，用于对外输出记录，为REC_MYSQL格式，包含m_initRead属性 */
		SubRecord	m_redRow; 			/** 当前扫描记录，用于内部保存扫描所得记录，为REC_REDUNDANT格式，包含m_initRead属性 */
		SubrecExtractor 	*m_srExtractor; /** 从堆或MMS中提取子记录的提取器 */
		bool		m_readLob;			/** 是否要读取大对象属性，这决定m_mysqlRow/m_redRow是否为相同数据 */
		MemoryContext	*m_lobMc;		/** 用于存储所返回的大对象内容的内存分配上下文 */
		bool		m_externalLobMc;	/** 存储大对象内容的内存分配上下文是否是外部指定的 */
		u32			m_readMask;			/** 用于读取MMS时检查要读取的属性是否为脏 */
	};
	
	/** 堆扫描 */
	class Scan: public BulkFetch {
	public:
		Scan(Session *session, Records *records, OpType opType, const ColList &readCols, MemoryContext *externalLobMc, 
			LockMode lockMode);
		virtual ~Scan() {}
		bool getNext(byte *mysqlRow);
		virtual void end();
		/**
		 * 获得当前扫描过的页面计数
		 * @return 
		 */
		inline u64 getCurScanPagesCount() const {
			return m_heapScan->getScanPageCount();
		}

		/**
		 * 设置在表扫描过程中不读取大对象的内容
		 * 如果表没有含大对象则忽略
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
		DrsHeapScanHandle	*m_heapScan;/** 表扫描句柄，只在堆扫描时才分配 */
	friend class Records;
	};

	/** 批量更新操作，更新之前不读取(上层覆盖索引扫描并更新) */
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
	//是否已经创建了可用的压缩字典
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

	Database	*m_db;			/** 数据库 */
	TableDef	*m_tableDef;	/** 表定义 */
	DrsHeap		*m_heap;		/** 堆 */
	MmsTable	*m_mms;			/** MMS */
	LobStorage	*m_lobStorage;	/** 大对象存储 */
	MMSBinlogCallBack *m_mmsCallback;	/** MMS写binlog的回调对象 */
	RowCompressMng *m_rowCompressMng;   /** 记录压缩管理, 只当表定义为压缩表时有意义, 否则为NULL */

friend class BulkOperation;
friend class Scan;
friend class BulkUpdate;
friend struct RSModInfo;
friend struct RSUpdateInfo;
};

}

#endif

