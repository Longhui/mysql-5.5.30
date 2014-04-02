/**
 * С�ʹ�������
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

/**С�ʹ���� */
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
	 * �������ڴ洢С�ʹ����ĶѶ���
	 *
	 * @return ���ڴ洢С�ʹ����ĶѶ���
	 */
	DrsHeap* getHeap() const {
		return m_heap;
	}

	/**
	 * �������ڻ���С�ʹ�����MMS�����
	 *
	 * @return ���ڻ���С�ʹ�����MMS�����
	 */
	MmsTable* getMmsTable() const {
		return m_mtable;
	}

	void setMmsTable(Session *session, bool useMms, bool flushDirty);
	
	/** 
	 * �õ�����ͳ����Ϣ
	 * 
	 * @return ����ͳ����Ϣ
	 */
	const SLobStatus& getStatus() {
		return (SLobStatus &)m_heap->getStatus();
	}

	/** 
	 * �õ���չͳ����Ϣ
	 *
	 * @return ��չͳ����Ϣ
	 */
	const SLobStatusEx& getStatusEx() {
		return (SLobStatusEx &)m_heap->getStatusEx();
	}

	/** 
	 * ������չ��Ϣ
	 * 
	 * @param session �Ự
	 * @param maxSamplePages ���ȡ��ҳ����
	 */
	void updateExtendStatus(Session *session, uint maxSamplePages) {
		m_heap->updateExtendStatus(session, maxSamplePages);
	}

	/** 
	 * �����������
	 *
	 * @return �������
	 */
	const TableDef *getVTableDef() { 
		return m_vtableDef;
	}

private :
	static TableDef* createTableDef(u16 tableId, const char *tableName);
	Database *m_db;         /** ��������ڵ����ݿ� */
    DrsHeap *m_heap;        /** ������Ӧ�ı䳤�� */
	MmsTable *m_mtable;     /** ��Ӧ��MMSTable */ 
	bool m_useMms;          /** �Ƿ�ʹ��MMS */
	TableDef *m_vtableDef;  /** ������� */
	const char *m_path;

#ifdef NTSE_UNIT_TEST
	friend class ::LobOperTestCase;
#endif
};

}

#endif // _NTSE_SMALLLOB_H_
