#ifndef _TNT_MHEAPRECORD_H_
#define _TNT_MHEAPRECORD_H_

#include "misc/TNTIMPageManager.h"
#include "misc/Record.h"
#include "trx/TNTTransaction.h"
#include "heap/HashIndex.h"

namespace tnt {
/** 新页面空闲级别初始为11 */
#define FLAG_TNT_10_LIMIT 0.7	/** 页内空闲空间低于70%则修改空闲级别为10 */
#define FLAG_TNT_01_LIMIT 0.4	/** 页内空闲空间低于40%则修改空闲级别为01 */
#define FLAG_TNT_00_LIMIT 0.1	/** 页内空闲空间低于10%则修改空闲级别为00 */

#define INVALID_SLOT      -1
#define FLAG_MHEAPREC_DEL  1
//记录信息
struct MHeapRec {
	u16     m_size;       //内存堆记录大小
	u8      m_vTableIndex;//回滚记录所在的版本池序号
	u8      m_del;        //是否已经被删除
	Record  m_rec;        //记录
	TrxId	m_txnId;      //事务id
	RowId	m_rollBackId; //回滚版本记录的rowId
	
	MHeapRec() {}
	MHeapRec(TrxId txnId, RowId rollBackId, u8 vTableIndex, Record *rec, u8 del) {
		m_vTableIndex = vTableIndex;
		m_del = del;
		//在rec.m_data采用浅拷贝，因为data会被序列化到堆记录的page上去
		memcpy(&m_rec, rec, sizeof(m_rec));
		m_txnId = txnId;
		m_rollBackId = rollBackId;
		m_size = getSerializeSize();
	}

	u16 getSerializeSize();
	void serialize(Stream *s);
	static MHeapRec *unSerialize(Stream *s, MemoryContext *ctx, RowId rid = INVALID_ROW_ID, bool copyData = true);
	static RowId getRowId(Stream *s);
	static void writeRowId(Stream *s, RowId oldRid, RowId newRid);
};

#define GET_PAGE_HEADER(ptr)  (((u64)ptr) & (~((u64)TNTIMPage::TNTIM_PAGE_SIZE - 1)))
#define MAX_U16  0xFFFF

enum MPageType {
	MPT_MHEAP,
	MPT_DUMP,
	MPT_DUMP_COMPENSATE,
	MPT_DUMP_END
};

//记录页头信息
struct MRecordPage: public TNTIMPage {
	void init(MPageType pageType, u16 tableId);
	void defrag(HashIndexOperPolicy *hashIndexOperPolicy);
	void defragSlot(HashIndexOperPolicy *hashIndexOperPolicy);
	static u8 getFreeGrade(u16 size);
	bool getLastRecord(s16 **pSlot, byte **rec, u16 *size);
	s16* appendRecord(byte *record, u16 size);
	s16* updateRecord(s16 *slot, byte *record, u16 size);
	void removeRecord(s16 *slot);
	bool getRecord(s16 *slot, byte **rec, u16 *size);
	u16 getRecordSize(s16 *slot);
	bool verify(HashIndexOperPolicy *hashIndexOperPolicy);

	MPageType      m_pageType;  //page类型
	u16            m_tableId;   //table Id
	u16            m_freeSize;  //空闲空间大小
	u16            m_recordCnt; //页中记录数
	u16            m_lastSlotOffset;  //最后一个slot的offset
	u16            m_lastRecordOffset; //最后一条记录的offset
	MRecordPage    *m_prev;
	MRecordPage    *m_next;
};
}
#endif
