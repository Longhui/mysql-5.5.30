/**
 * ��������
 *
 * @author ��Դ(wangyuan@corp.netease.com, wy@163.org)
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

/** С�ʹ����ͳ����Ϣ����һ����ȷ�� */
typedef struct HeapStatus SLobStatus;

/** С�ʹ������չͳ����Ϣ������ȷ�� */
typedef struct HeapStatusEx SLobStatusEx;

/** ���ʹ����ͳ����Ϣ����һ����ȷ�� */
struct BLobStatus {
	DBObjStats	*m_dboStats;	/** ���ݿ����Ĺ���ͳ����Ϣ */
	u64		m_idxLength;    	/** Ŀ¼��С */
	u64		m_datLength;    	/** ���ݴ�С */
	u64		m_moveUpdate;   	/** �Ǳ��ظ��£���ҪMOVE��ĩβ�ĸ��´��� */
};

/** ���ʹ������չͳ����Ϣ��ͨ��������ã�����ȷ�� */
struct BLobStatusEx {
	u64		m_numLobs;		/** �������� */
	u64		m_freePages;	/** ��β���Ŀ���ҳ���� */
	double	m_pctUsed;		/** ҳ�������� */
};

/** �����ͳ����Ϣ����һ����ȷ��*/
struct LobStatus {
	SLobStatus	m_slobStatus;			/** С�ʹ����ͳ����Ϣ */
	BLobStatus	m_blobStatus;			/** ���ʹ����ͳ����Ϣ */
	u64			m_lobInsert;			/** ����������������ڸ��µ��´�������ͷ����仯ʱ��ӳ��
										 * �ײ��С�ʹ�������ʹ������Ϊ�����ɾ����������˴�
										 * ����Ĳ������������С�ʹ��������ʹ����������֮�ͣ�
										 * ���뵥��ͳ��
										 */
	u64			m_lobUpdate;			/** �������´��� */
	u64			m_lobDelete;			/** �����ɾ������ */
	u64			m_usefulCompress;		/** ��Чѹ����������ѹ�����ѹ��ǰҪС */
	u64			m_uselessCompress;		/** ��Чѹ����������ѹ���󷴶������ */
	u64			m_preCompressSize;		/** ѹ��ǰ�ܴ�С��ֻͳ��ָ��Ҫѹ���ģ�������СС��ѹ�����޼���Чѹ�� */
	u64			m_postCompressSize;		/** ѹ�����ܴ�С��ͬ�� */
};

/** �������չͳ����Ϣ������ȷ��*/
struct LobStatusEx {
	SLobStatusEx    m_slobStatus;    /** С�ʹ������չͳ����Ϣ */
	BLobStatusEx    m_blobStatus;    /** ���ʹ������չͳ����Ϣ */
};

/** ����ȷ�Ĵ����ID */
#define INVALID_LOB_ID	((LobId)INVALID_ROW_ID)

/** �жϴ�����Ƿ��Ǵ��ʹ���� */
#define IS_BIG_LOB(lid) ((lid) >> 63) != 0
/** ���ô����Ϊ���ʹ���� */
#define LID_MAKE_BIGLOB_BIT(lid)     ((LobId)((lid) | (((u64)1) << 63)))

#define NEED_COMPRESS(isCompress, size)		((isCompress) && (size) >= MIN_COMPRESS_LEN)

/** ѹ��ʱ��������볤�ȵõ��������, �ù�ʽ������lzo�ĵ����ǵ���������ѹ��ʱ��
	��Ҫ�����ռ䣬�ù�ʽ���Ǿ�ȷ�ģ����ǽ����������ʽ���ܱ�֤��ȷ�� */
#define MAX_COMPRESSED_SIZE(uncompressed_size)    ((uncompressed_size) + (uncompressed_size) / 16 + 64 + 3)

/** С�ʹ�����д�������ݵ���󳤶�, ����7����Ϊ��1���ֽڵ�NullBitmap,
 * ��һ���ֶα�ʾ�����ѹ��֮ǰ����ռ��4���ֽڣ��ڶ������ڴ洢���������
 * ���ֶ�ͷ���������ֽڱ�ʾռ�õĴ洢�ռ��С������ѹ����δѹ����
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
/** �����洢 */
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
	
    //��������
	byte* get(Session *session, MemoryContext *mc, LobId lobId, uint *size, bool intoMms = true); 
	LobId insert(Session *session, const byte *lob, uint size, bool compress = true);
	void del(Session *session, LobId lobId);
#ifdef TNT_ENGINE
	void delAtCrash(Session *session, LobId lobId);
#endif
	LobId update(Session *session, LobId lobId, const byte *lob, uint size, bool compress);
	void setMmsTable(Session *session, bool useMms, bool flushDirty);
	/**
	 * ����ȷ�жϸ���֮��lobId�Ƿ���ܷ����仯
	 * @param lobId �����Id
	 * @param lobSize ������С
	 * @return �����������ʱlobId�����仯���϶�����true��
	 * �����������ʱlobIdû�з����仯�����ܷ���trueҲ���ܷ���false����ѹ��Ӱ�죩
	 */
	bool couldLobIdChanged(LobId lobId, uint lobSize) {
		if (IS_BIG_LOB(lobId)) {
			return false;
		} else {
			return lobSize > m_maxSmallLobLen;
		}
	}

	//redo����
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
	//Log��־
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

	Database		*m_db;				/** ���ݿ� */
	const TableDef		*m_tableDef;		/** �������� */
	SmallLobStorage	*m_slob	;			/** С�ʹ���� */
	u16				m_maxSmallLobLen;	/** С�ʹ�������󳤶� */
	BigLobStorage	*m_blob;			/** ���ʹ���� */
	LzoOper			*m_lzo;				/** LZOѹ������ */
	LobStatus		m_status;			/** ��������ͳ����Ϣ */
	LobStatusEx		m_statusEx;			/** ��������չͳ����Ϣ */

	/** �Ƿ�Ҫѹ�������� */
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
