#ifndef _TNT_MHEAPRECORD_H_
#define _TNT_MHEAPRECORD_H_

#include "misc/TNTIMPageManager.h"
#include "misc/Record.h"
#include "trx/TNTTransaction.h"
#include "heap/HashIndex.h"

namespace tnt {
/** ��ҳ����м����ʼΪ11 */
#define FLAG_TNT_10_LIMIT 0.7	/** ҳ�ڿ��пռ����70%���޸Ŀ��м���Ϊ10 */
#define FLAG_TNT_01_LIMIT 0.4	/** ҳ�ڿ��пռ����40%���޸Ŀ��м���Ϊ01 */
#define FLAG_TNT_00_LIMIT 0.1	/** ҳ�ڿ��пռ����10%���޸Ŀ��м���Ϊ00 */

#define INVALID_SLOT      -1
#define FLAG_MHEAPREC_DEL  1
//��¼��Ϣ
struct MHeapRec {
	u16     m_size;       //�ڴ�Ѽ�¼��С
	u8      m_vTableIndex;//�ع���¼���ڵİ汾�����
	u8      m_del;        //�Ƿ��Ѿ���ɾ��
	Record  m_rec;        //��¼
	TrxId	m_txnId;      //����id
	RowId	m_rollBackId; //�ع��汾��¼��rowId
	
	MHeapRec() {}
	MHeapRec(TrxId txnId, RowId rollBackId, u8 vTableIndex, Record *rec, u8 del) {
		m_vTableIndex = vTableIndex;
		m_del = del;
		//��rec.m_data����ǳ��������Ϊdata�ᱻ���л����Ѽ�¼��page��ȥ
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

//��¼ҳͷ��Ϣ
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

	MPageType      m_pageType;  //page����
	u16            m_tableId;   //table Id
	u16            m_freeSize;  //���пռ��С
	u16            m_recordCnt; //ҳ�м�¼��
	u16            m_lastSlotOffset;  //���һ��slot��offset
	u16            m_lastRecordOffset; //���һ����¼��offset
	MRecordPage    *m_prev;
	MRecordPage    *m_next;
};
}
#endif
