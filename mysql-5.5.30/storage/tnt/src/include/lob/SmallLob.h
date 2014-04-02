/**
 * 小型大对象管理
 *
 * @author zx(zhangxiao@corp.netease.com, zx@163.org)
 */
#ifndef _NTSE_SMALLLOB_H_
#define _NTSE_SMALLLOB_H_
#include "lob/Lob.h"
#include "misc/Session.h"

namespace ntse {

class Record;
class DrsHeap;
class MmsTable;
class SubRecord;
class Database;
class MemoryContext;
class Session;
class File;
class TableDef;
class LobIndex;
class Buffer;

/**小型大对象 */
class SmallLobStorage {
 
public:
	SmallLobStorage(Database *db, DrsHeap *heap, const char *path, MmsTable *mmstable, TableDef *vtableDef, bool useMms);
	~SmallLobStorage();
   
	static void create(Database *db, const TableDef *tableDef, u16 tableId, const char *basePath) throw(NtseException);
	static void drop(const char *basePath) throw(NtseException);
	static SmallLobStorage* open(Database *db, Session *session, const char *basePath, bool useMms) throw(NtseException);
	void close(Session *session, bool flushDirty);
	LobId insert(Session *session, const byte *stream, uint size, u32 orgLen);
	void update(Session *session, LobId lobId, const byte *lob, uint size, u32 orgLen);
	bool del(Session *session, LobId rid) throw(NtseException);
#ifdef TNT_ENGINE
	bool delAtCrash(Session *session, LobId rid) throw(NtseException);
#endif
	byte* read(Session *session, MemoryContext *mc, LobId rid, u32 *size, bool intoMms, u32 *orgLen);
	static void redoCreate(Database *db, Session *session, const char *basePath, u16 tableId) throw(NtseException);
	LobId redoInsert(Session *session, u64 lsn, const byte *log, uint logSize);
	void redoDelete(Session *session, LobId lobId, u64 lsn, const byte *log, uint size);
	void redoUpdateHeap(Session *session, LobId lobId, u64 lsn, const byte *log, uint size,
		const byte *lob, uint lobSize, uint org_size);
	void redoUpdateMms(Session *session, u64 lsn, const byte *log, uint size);
	void delInMms(Session *session, LobId lobId);
	int getFiles(File** files, PageType* pageTypes, int numFile);
	u16 getLenColumnBytes();
	void flush(Session *session);
	void setTableId(Session *session, u16 tableId);

	/**
	 * 返回用于存储小型大对象的堆对象
	 *
	 * @return 用于存储小型大对象的堆对象
	 */
	DrsHeap* getHeap() const {
		return m_heap;
	}

	/**
	 * 返回用于缓存小型大对象的MMS表对象
	 *
	 * @return 用于缓存小型大对象的MMS表对象
	 */
	MmsTable* getMmsTable() const {
		return m_mtable;
	}

	void setMmsTable(Session *session, bool useMms, bool flushDirty);
	
	/** 
	 * 得到基本统计信息
	 * 
	 * @return 基本统计信息
	 */
	const SLobStatus& getStatus() {
		return (SLobStatus &)m_heap->getStatus();
	}

	/** 
	 * 得到扩展统计信息
	 *
	 * @return 扩展统计信息
	 */
	const SLobStatusEx& getStatusEx() {
		return (SLobStatusEx &)m_heap->getStatusEx();
	}

	/** 
	 * 更新扩展信息
	 * 
	 * @param session 会话
	 * @param maxSamplePages 最大取样页个数
	 */
	void updateExtendStatus(Session *session, uint maxSamplePages) {
		m_heap->updateExtendStatus(session, maxSamplePages);
	}

	/** 
	 * 返回虚拟表定义
	 *
	 * @return 虚拟表定义
	 */
	const TableDef *getVTableDef() { 
		return m_vtableDef;
	}

private :
	static TableDef* createTableDef(u16 tableId, const char *tableName);
	Database *m_db;         /** 大对象所在的数据库 */
    DrsHeap *m_heap;        /** 大对象对应的变长堆 */
	MmsTable *m_mtable;     /** 对应的MMSTable */ 
	bool m_useMms;          /** 是否使用MMS */
	TableDef *m_vtableDef;  /** 虚拟表定义 */
	const char *m_path;

#ifdef NTSE_UNIT_TEST
	friend class ::LobOperTestCase;
#endif
};

}

#endif // _NTSE_SMALLLOB_H_
