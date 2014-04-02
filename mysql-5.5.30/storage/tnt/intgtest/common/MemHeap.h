#ifndef _NTSETEST_MEMHEAP_H_
#define _NTSETEST_MEMHEAP_H_

#include "misc/Global.h"
#include "misc/Record.h"
#include "misc/TableDef.h"
#include "util/Sync.h"
#include "misc/Session.h"
#include "misc/Syslog.h"

using namespace ntse;


/** MemHeap��¼Ψһ��ʶ */
typedef unsigned MemHeapRid;



class MemHeap;

/** �ڴ�����Ϣ */
struct ColumnDesc {
	u16		m_offset;		/** ������ƫ�� */
	u16		m_size;			/** �г��� */
	bool	m_needCksum;	/** �Ƿ���checksum */
};


/**
 * �ڴ�Ѽ�¼
 */
class MemHeapRecord {
	friend class MemHeap;
public:
	bool compare(Session *session, const byte *record) const;
	bool compare(Session *session, u16 numCols, u16 *columns, const byte *subRecord) const;
	bool compareColumn(Session *session, u16 cno, const byte *rec) const;
	bool compare(Session *session, const Record *rec) const;
	void update(Session *session, const byte *record);
	void update(Session *session, u16 numCols, u16 *columns, const byte *subRecord);
	void toRecord(Session *session, byte *record);
	RowId getRowId() const;
	MemHeapRid getId() const;

public:
	static const MemHeapRid INVALID_ID = (MemHeapRid) -1;	// �Ƿ���ʶ��

private:
	void setRowId(RowId rid);
	void updateColumn(u16 cno, const byte *rec);
	bool columnEq(u16 cno, const byte *rec) const;
	u64 checksumColumn(u16 cno, const byte *rec) const;

private:
	
	MemHeapRecord(const TableDef *tableDef, const ColumnDesc *colDesc, MemHeapRid id, byte *data);

private:
	u16		m_numCols;
	const ColumnDesc *m_colDescs;
	const TableDef *m_tableDef;
	byte	*m_data;
	RowId	m_rowId;
	MemHeapRid m_id;
};


/**
 * �ڴ��
 */
class MemHeap {
	/** �ڴ�Ѳ�λ */
	struct Slot {
		Slot() : m_mutex("MemHeap::slot::mutex", __FILE__, __LINE__) {}
		Mutex			m_mutex;		/** ������Slot */
		DLink<Slot *>	m_dlink;		/** ˫������ */
		bool			m_isFree;		/** �Ƿ���� */
		MemHeapRecord	*m_record;		/** ��¼ָ�� */
	};

public:
	MemHeap(unsigned maxRecCnt, const TableDef *tableDef
		, u16 numCksumCols = 0 , u16 *cksumCols = 0);
	~MemHeap();
	void setLogger(Syslog *logger);
	
	
	unsigned getMaxRecCount() const;
	unsigned getUsedRecCount();

	bool deleteRecord(Session *session, MemHeapRid id);
	MemHeapRecord* recordAt(Session *session, MemHeapRid id, RowLockHandle **rlh = 0, LockMode mode = None);
	MemHeapRecord* getRandRecord(Session *session, RowLockHandle **rlh, LockMode mode);
	RowId getRandRowId();
	MemHeapRid getRandId();

	MemHeapRid insert(Session *session, RowId rid, const byte *rec);

	MemHeapRid reserveRecord();
	void unreserve(MemHeapRid id);
	bool insertAt(Session *session, MemHeapRid id, RowId rid, const byte *rec);



private:
	MemHeapRid slotToId(const Slot * slot);
	bool getRandRecord(RowId *rid, MemHeapRid *id);

private:
	static const unsigned CKSUM_SIZE = sizeof(u64);	// У�鳤��
	static const unsigned SCAN_LEN = 100;			// ɨ�賤�ȣ����ڻ�ȡ�����¼

	TableDef	*m_tableDef;	/** ���� */
	unsigned	m_maxRecCnt;	/** ����¼�� */
	Slot		*m_slots;		/** ������ */
	byte		*m_data;		/** ��ż�¼���ڴ�ռ� */
	DList<Slot *> m_freeList;	/** ���в����� */
	Mutex		m_mutex;		/** ����freeList */
	u16			m_recSize;		/** ��¼���� */
	unsigned	m_numCols;		/** ���� */
	ColumnDesc	*m_colDescs;	/** �ж��� */
	Syslog		*m_logger;
};


#endif // _NTSETEST_MEMHEAP_H_

