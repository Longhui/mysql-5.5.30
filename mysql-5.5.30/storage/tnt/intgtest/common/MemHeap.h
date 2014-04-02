#ifndef _NTSETEST_MEMHEAP_H_
#define _NTSETEST_MEMHEAP_H_

#include "misc/Global.h"
#include "misc/Record.h"
#include "misc/TableDef.h"
#include "util/Sync.h"
#include "misc/Session.h"
#include "misc/Syslog.h"

using namespace ntse;


/** MemHeap记录唯一标识 */
typedef unsigned MemHeapRid;



class MemHeap;

/** 内存列信息 */
struct ColumnDesc {
	u16		m_offset;		/** 列数据偏移 */
	u16		m_size;			/** 列长度 */
	bool	m_needCksum;	/** 是否做checksum */
};


/**
 * 内存堆记录
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
	static const MemHeapRid INVALID_ID = (MemHeapRid) -1;	// 非法标识符

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
 * 内存堆
 */
class MemHeap {
	/** 内存堆槽位 */
	struct Slot {
		Slot() : m_mutex("MemHeap::slot::mutex", __FILE__, __LINE__) {}
		Mutex			m_mutex;		/** 包含本Slot */
		DLink<Slot *>	m_dlink;		/** 双向链表 */
		bool			m_isFree;		/** 是否空闲 */
		MemHeapRecord	*m_record;		/** 记录指针 */
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
	static const unsigned CKSUM_SIZE = sizeof(u64);	// 校验长度
	static const unsigned SCAN_LEN = 100;			// 扫描长度，用于获取随机记录

	TableDef	*m_tableDef;	/** 表定义 */
	unsigned	m_maxRecCnt;	/** 最大记录数 */
	Slot		*m_slots;		/** 槽数组 */
	byte		*m_data;		/** 存放记录的内存空间 */
	DList<Slot *> m_freeList;	/** 空闲槽链表 */
	Mutex		m_mutex;		/** 保护freeList */
	u16			m_recSize;		/** 记录长度 */
	unsigned	m_numCols;		/** 列数 */
	ColumnDesc	*m_colDescs;	/** 列定义 */
	Syslog		*m_logger;
};


#endif // _NTSETEST_MEMHEAP_H_

