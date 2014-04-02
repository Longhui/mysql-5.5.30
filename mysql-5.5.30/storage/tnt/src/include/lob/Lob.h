/**
 * 大对象管理
 *
 * @author 汪源(wangyuan@corp.netease.com, wy@163.org)
 */

#ifndef _NTSE_LOB_H_
#define _NTSE_LOB_H_

#include "misc/Global.h"
#include <string>
#include "util/PagePool.h"
#include "lob/Lzo.h"
//#include "misc/Trace.h"
#include "heap/Heap.h"


#ifdef NTSE_UNIT_TEST
class LobOperTestCase;
class LobDefragTestCase;
#endif

namespace ntse {

/** 小型大对象统计信息（不一定精确） */
typedef struct HeapStatus SLobStatus;

/** 小型大对象扩展统计信息（不精确） */
typedef struct HeapStatusEx SLobStatusEx;

/** 大型大对象统计信息（不一定精确） */
struct BLobStatus {
	DBObjStats	*m_dboStats;	/** 数据库对象的公有统计信息 */
	u64		m_idxLength;    	/** 目录大小 */
	u64		m_datLength;    	/** 数据大小 */
	u64		m_moveUpdate;   	/** 非本地更新，需要MOVE到末尾的更新次数 */
};

/** 大型大对象扩展统计信息（通过采样获得，不精确） */
struct BLobStatusEx {
	u64		m_numLobs;		/** 大对象个数 */
	u64		m_freePages;	/** 非尾部的空闲页面数 */
	double	m_pctUsed;		/** 页面利用率 */
};

/** 大对象统计信息（不一定精确）*/
struct LobStatus {
	SLobStatus	m_slobStatus;			/** 小型大对象统计信息 */
	BLobStatus	m_blobStatus;			/** 大型大对象统计信息 */
	u64			m_lobInsert;			/** 大对象插入次数，由于更新导致大对象类型发生变化时反映到
										 * 底层的小型大对象或大型大对象中为插入或删除操作，因此大
										 * 对象的插入操作不等于小型大对象与大型大对象插入操作之和，
										 * 必须单独统计
										 */
	u64			m_lobUpdate;			/** 大对象更新次数 */
	u64			m_lobDelete;			/** 大对象删除次数 */
	u64			m_usefulCompress;		/** 有效压缩次数，即压缩后比压缩前要小 */
	u64			m_uselessCompress;		/** 无效压缩次数，即压缩后反而变大了 */
	u64			m_preCompressSize;		/** 压缩前总大小，只统计指定要压缩的，包括大小小于压缩下限及无效压缩 */
	u64			m_postCompressSize;		/** 压缩后总大小，同上 */
};

/** 大对象扩展统计信息（不精确）*/
struct LobStatusEx {
	SLobStatusEx    m_slobStatus;    /** 小型大对象扩展统计信息 */
	BLobStatusEx    m_blobStatus;    /** 大型大对象扩展统计信息 */
};

/** 不正确的大对象ID */
#define INVALID_LOB_ID	((LobId)INVALID_ROW_ID)

/** 判断大对象是否是大型大对象 */
#define IS_BIG_LOB(lid) ((lid) >> 63) != 0
/** 设置大对象为大型大对象 */
#define LID_MAKE_BIGLOB_BIT(lid)     ((LobId)((lid) | (((u64)1) << 63)))

#define NEED_COMPRESS(isCompress, size)		((isCompress) && (size) >= MIN_COMPRESS_LEN)

/** 压缩时候根据输入长度得到输出长度, 该公式来至于lzo文档，是当遇到不可压缩时候，
	需要的最大空间，该公式不是精确的，但是建议用这个公式，能保证正确性 */
#define MAX_COMPRESSED_SIZE(uncompressed_size)    ((uncompressed_size) + (uncompressed_size) / 16 + 64 + 3)

/** 小型大对象中大对象数据的最大长度, 这里7是因为有1个字节的NullBitmap,
 * 第一个字段表示大对象压缩之前长度占用4个字节，第二个用于存储大对象数据
 * 的字段头还有两个字节表示占用的存储空间大小（可能压缩或未压缩）
 */
#define MAX_LOB_DATA_SIZE (Limits::MAX_REC_SIZE - 7)

class Database;
class MemoryContext;
class Session;
class File;
class SmallLobStorage;
class BigLobStorage;
class LobIndex;
class Buffer;
class SubRecord;
class DrsHeap;
class MmsTable;
/** 大对象存储 */
class LobStorage {
public:
	static void create(Database *db, const TableDef *tableDef, const char *path) throw(NtseException);
	static LobStorage* open(Database *db, Session *session, const TableDef *tableDef, const char *path, bool useMms = true) throw(NtseException);
	static void drop(const char *path) throw(NtseException);
	void close(Session *session, bool flushDirty);  
	void flush(Session *session);
	int getFiles(File** files, PageType* pageTypes, int numFile);
	void defrag(Session *session);
	void setTableId(Session *session, u16 tableId);
	
    //基本操作
	byte* get(Session *session, MemoryContext *mc, LobId lobId, uint *size, bool intoMms = true); 
	LobId insert(Session *session, const byte *lob, uint size, bool compress = true);
	void del(Session *session, LobId lobId);
#ifdef TNT_ENGINE
	void delAtCrash(Session *session, LobId lobId);
#endif
	LobId update(Session *session, LobId lobId, const byte *lob, uint size, bool compress);
	void setMmsTable(Session *session, bool useMms, bool flushDirty);
	/**
	 * 不精确判断更新之后lobId是否可能发生变化
	 * @param lobId 大对象Id
	 * @param lobSize 大对象大小
	 * @return 如果真正更新时lobId发生变化，肯定返回true，
	 * 如果真正更新时lobId没有发生变化，可能返回true也可能返回false（受压缩影响）
	 */
	bool couldLobIdChanged(LobId lobId, uint lobSize) {
		if (IS_BIG_LOB(lobId)) {
			return false;
		} else {
			return lobSize > m_maxSmallLobLen;
		}
	}

	//redo函数
	static void redoCreate(Database *db, Session *session, const TableDef *tableDef, const char *path,
		u16 tableId) throw(NtseException);
	LobId redoSLInsert(Session *session, u64 lsn, const byte *log, uint logSize);
	LobId redoBLInsert(Session *session, u64 lsn, const byte *log, uint logSize);

	LobId redoSLUpdateHeap(Session *session, LobId lobId, u64 lsn, const byte *log,
		uint size, const byte *lob, uint lobSize, bool compress);
	void redoSLUpdateMms(Session *session, u64 lsn, const byte *log, uint size);
	void redoBLUpdate(Session *session, LobId lobId, u64 lsn, const byte *log,
		uint logSize, const byte *lob, uint lobSize, bool compress);
	void redoSLDelete(Session *session, LobId lobId, u64 lsn, const byte *log,
		uint size);
	void redoBLDelete(Session *session, u64 lsn, const byte *log, uint size);
	void redoMove(Session *session, u64 lsn, const byte *log, uint logSize);

	byte *parseInsertLog(const LogEntry *log, LobId lid, size_t *origLen, MemoryContext *mc);

#ifdef TNT_ENGINE 
	//Log日志
	//Insert Lob
	static u64 writeTNTInsertLob(Session *session, TrxId txnId, u16 tableId, u64 preLsn, LobId lobId);
	static void parseTNTInsertLob(const LogEntry *log, TrxId *txnId, u64 *preLsn, LobId *lobId); 
#endif 
	~LobStorage ();
	
	DrsHeap* getSLHeap() const;
	MmsTable* getSLMmsTable() const;
	
	DBObjStats *getLLobDirStats();
	DBObjStats* getLLobDatStats();

	const LobStatus& getStatus();
	
	void updateExtendStatus(Session *session, uint maxSamplePages);
	const LobStatusEx& getStatusEx();
	void setTableDef(const TableDef *tableDef);
	const TableDef* getSLVTableDef();

private:
	LobStorage(Database *db, const TableDef *tableDef, SmallLobStorage *slob, BigLobStorage *blob, LzoOper *lzoOper);

	Database		*m_db;				/** 数据库 */
	const TableDef		*m_tableDef;		/** 所属表定义 */
	SmallLobStorage	*m_slob	;			/** 小型大对象 */
	u16				m_maxSmallLobLen;	/** 小型大对象的最大长度 */
	BigLobStorage	*m_blob;			/** 大型大对象 */
	LzoOper			*m_lzo;				/** LZO压缩对象 */
	LobStatus		m_status;			/** 大对象基本统计信息 */
	LobStatusEx		m_statusEx;			/** 大对象的扩展统计信息 */

	/** 是否要压缩的限制 */
	const static uint MIN_COMPRESS_LEN = 300;

#ifdef NTSE_UNIT_TEST
public:
	friend class ::LobOperTestCase;
	friend class ::LobDefragTestCase;
	File* getIndexFile();
	File* getBlobFile();
#endif

};

} // namespace ntse

#endif
