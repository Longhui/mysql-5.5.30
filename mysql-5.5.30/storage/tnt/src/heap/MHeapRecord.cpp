/**����ڴ�Ѽ�¼�ĸ���ҳ����
 *author �ö��� xindingfeng@corp.netease.com
 */
#include "heap/MHeapRecord.h"
#include "util/Stream.h"
#include "misc/MemCtx.h"
#include "trx/TNTTransaction.h"

namespace tnt {
/** �ڴ�Ѽ�¼���л�����
 * return �ڴ�Ѽ�¼���л�����
 */
u16 MHeapRec::getSerializeSize() {
	return (u16)(sizeof(m_size) + sizeof(m_vTableIndex) + sizeof(m_del) + sizeof(s8) /*m_rec.m_format*/ + m_rec.m_size
		+ RID_BYTES/*m_rec.m_rowId*/ + sizeof(m_txnId) + RID_BYTES/*m_rollBackId*/);
}

/** �ڴ�Ѽ�¼���л�
 * @param s out ���л������������
 */
void MHeapRec::serialize(Stream *s) {
	assert(m_size == getSerializeSize());
	s->write(m_size);
	s->write(m_vTableIndex);
	s->write(m_del);
	s->write((s8)m_rec.m_format);
	s->writeRid(m_rec.m_rowId);
	assert(m_rec.m_size > 0);
	s->write(m_rec.m_data, m_rec.m_size);
	s->write(m_txnId);
	s->writeRid(m_rollBackId);
}

/** �ڴ�Ѽ�¼�����л�
 * @param s �����л��Ķ���������
 * @param ctx �����ģ���ҪΪ���ؼ�¼����ռ�
 * return �����к���ڴ�Ѽ�¼
 */
MHeapRec *MHeapRec::unSerialize(Stream *s, MemoryContext *ctx, RowId rid, bool copyData) {
	u64 sp = ctx->setSavepoint();
	MHeapRec *ret = (MHeapRec *)ctx->alloc(sizeof(MHeapRec));
	s8 format = 0;
	
	s->read(&ret->m_size);
	s->read(&ret->m_vTableIndex);
	s->read(&ret->m_del);
	s->read((s8*)&format);
	ret->m_rec.m_format = (RecFormat)format;
	s->readRid(&ret->m_rec.m_rowId);
	if (rid != INVALID_ROW_ID && ret->m_rec.m_rowId != rid) {
		ctx->resetToSavepoint(sp);
		return NULL;
	}
	
	ret->m_rec.m_size = ret->m_size -  sizeof(ret->m_size) - RID_BYTES/*ret->m_rec.m_rowId*/ - sizeof(ret->m_txnId)
		- RID_BYTES/*ret->m_rollBackId*/ - sizeof(ret->m_vTableIndex) - sizeof(s8)/*ret->m_rec.m_format*/ - sizeof(ret->m_del);

	assert(ret->m_rec.m_size > 0);
	if (copyData) {
		ret->m_rec.m_data = (byte *)ctx->alloc(ret->m_rec.m_size);
		s->readBytes(ret->m_rec.m_data, ret->m_rec.m_size);
	} else {
		ret->m_rec.m_data = s->currPtr();
		s->skip(ret->m_rec.m_size);
	}

	s->read(&ret->m_txnId);
	s->readRid(&ret->m_rollBackId);

	return ret;
}

RowId MHeapRec::getRowId(Stream *s) {
	RowId rid;
	s->skip(sizeof(u16) + sizeof(u8) + sizeof(u8) + sizeof(s8)); //skip heapRec��m_size, m_vTableIndex, m_del, m_rec.m_format
	s->readRid(&rid);
	return rid;
}

void MHeapRec::writeRowId(Stream *s, RowId oldRid, RowId newRid) {
	RowId rid;
	s->skip(sizeof(u16) + sizeof(u8) + sizeof(u8) + sizeof(s8));
	rid = RID_READ(s->currPtr());
	assert(oldRid == rid);
	s->writeRid(newRid);
}

//��ʼ��ҳ����Ϣ
void MRecordPage::init(MPageType pageType, u16 tableId) {
	m_pageType = pageType;
	m_tableId = tableId;
	m_prev = NULL;
	m_next = NULL;
	m_freeSize = TNTIM_PAGE_SIZE - sizeof(MRecordPage);
	m_recordCnt = 0;
	m_lastSlotOffset = sizeof(MRecordPage);
	m_lastRecordOffset = TNTIM_PAGE_SIZE;
}

// �����ڴ��¼ҳ��slot��ȥ������Ϊ-1��slot
void MRecordPage::defragSlot(HashIndexOperPolicy *hashIndexOperPolicy) {
	assert((m_lastSlotOffset - sizeof(MRecordPage))%sizeof(s16) == 0);
	byte *buf = NULL;
	u16 recSize = 0;
	RowId rid = 0;
	s16 *src = NULL;
	//s16 *dest = (s16 *)((byte *)this + m_lastSlotOffset);
	s16 *lastPtr = (s16 *)((byte *)this + m_lastSlotOffset - sizeof(s16));
	//��֤���һ��slot�Ǳ�����ʹ�õ�
	//��Ϊ���remove�����һ��slot����ı�m_lastSlotOffset
	//�������һ��slot���Բ�����ΪINVALID_SLOT
	assert(*lastPtr != INVALID_SLOT);
	/*while (*dest == INVALID_SLOT) {
		m_lastSlotOffset -= sizeof(s16);
		dest--;
	}*/
	assert((byte *)lastPtr >= (byte *)this + sizeof(MRecordPage));

	//�Կ�ʼ�ʼ����
	s16 *dest = (s16 *)((byte *)this + sizeof(MRecordPage));
	for (; dest < lastPtr; dest++) {
		//�����slot��Ч�������������һ����Чslot
		if (*dest != INVALID_SLOT) {
			continue;
		}
		//������Ч��slot�����
		assert(*dest == INVALID_SLOT);
		//����dest���һ����Ч��slot
		for (src = dest + 1; src < lastPtr && (*src == INVALID_SLOT); src++);
		
		if (INVALID_SLOT == *src) {
			//��ʱ��ζ��dest��������Чslot
			assert(src == lastPtr);
			break;
		}

		//����ҵ�dest��һ����Ч��slot
		if (hashIndexOperPolicy != NULL) {
			buf = (byte *)this + *src;
			recSize = *(u16 *)buf;
			Stream s(buf, recSize);
			rid = MHeapRec::getRowId(&s);
			hashIndexOperPolicy->update(rid, (u64)dest);
		}
		*dest = *src;
		*src = INVALID_SLOT;
	}

	//��ʱdestָ�������һ�����δ�ж����һ���Ƿ�Ϊinvalid��
	m_lastSlotOffset = (u16)((byte *)dest - (byte *)this);
	if (*dest != INVALID_SLOT) {
		m_lastSlotOffset += sizeof(s16);
	}
}

/**
 *�ڴ��ҳ��������Ҫ�������ڴ�ҳ��Ϊ�������������пռ����ڴ�ż�¼
 */
void MRecordPage::defrag(HashIndexOperPolicy *hashIndexOperPolicy) {
	assert(m_lastSlotOffset >= sizeof(MRecordPage));
	//�������û��slot����û��������ı�Ҫ
	if (m_lastSlotOffset == sizeof(MRecordPage)) {
		return;
	}
	//verify_ex(vs.mheap, verify(hashIndexOperPolicy));
	defragSlot(hashIndexOperPolicy);
	assert((m_lastSlotOffset - sizeof(MRecordPage))%sizeof(s16) == 0);

	//verify_ex(vs.mheap, verify(hashIndexOperPolicy));
	s16 *pSlot = (s16 *)((byte *)this + sizeof(MRecordPage));
	s16 *endSlot = (s16 *)((byte *)this + m_lastSlotOffset);
	u16 recSize = 0;
	byte *recEnd = (byte *)((byte *)this + TNTIM_PAGE_SIZE);
	while (pSlot < endSlot) {
	//while ((byte *)pSlot < ((byte *)this + m_lastSlotOffset)) {
		assert(*pSlot != INVALID_SLOT);
		byte *rec = (byte *)this + *pSlot;
		//recSize = ((MHeapRec *)rec)->m_size;
		recSize = *(u16 *)(rec);
		if (rec + recSize < recEnd) {
			memmove(recEnd - recSize, rec, recSize);
			*pSlot = (s16)(recEnd - recSize - (byte *)this);
		} else {
			assert(rec + recSize == recEnd);
		}
		recEnd -= recSize;
		pSlot++;
	}
	
	m_lastRecordOffset = (u16)(recEnd - (byte *)this);
	verify_ex(vs.mheap, verify(hashIndexOperPolicy));
}

/** ����size�Ĵ�С��ȡ���м���
 * @param size ʵ�ʿռ��С
 * return ���м���
 */
u8 MRecordPage::getFreeGrade(u16 size) {
	assert(size <= TNTIM_PAGE_SIZE - sizeof(MRecordPage));
	float ratio = (float)size/(MRecordPage::TNTIM_PAGE_SIZE - sizeof(MRecordPage));
	u8 index = 3;
	if (ratio < FLAG_TNT_00_LIMIT) {
		index = 0;
	} else if (ratio < FLAG_TNT_01_LIMIT) {
		index = 1;
	} else if (ratio < FLAG_TNT_10_LIMIT) {
		index = 2;
	}

	return index;
}

/** ��ȡ��¼ҳ���һ��slot��ַ����Ӧ�ļ�¼�����С
 * @param pSlot out ���һ��slot��ַ
 * @param rec out ���һ����¼��ַ
 * @param size out ���һ����¼��С
 * return �Ƿ�������һ����¼
 */
bool MRecordPage::getLastRecord(s16 **pSlot, byte **rec, u16 *size) {
	if (m_recordCnt == 0) {
		*pSlot = NULL;
		*rec = NULL;
		*size = 0;
		return false;
	}

	*pSlot = (s16 *)((byte *)this + m_lastSlotOffset - sizeof(s16));
	//����getLastRecord��ѭ�����ã����Բ��ܱ�֤���һ���϶�����Ч��
	while (*pSlot >= (s16 *)((byte *)this + sizeof(MRecordPage))) {
		if (**pSlot != INVALID_SLOT) {
			break;
		}
		(*pSlot)--;
	}

	*rec = (byte *)this + **pSlot;
	*size = *(u16 *)(*rec);
	return true;
}

/** ���¼ҳ����Ӽ�¼
 * @param record ��Ҫ��ӵļ�¼
 * @param size ��Ҫ��Ӽ�¼�Ĵ�С
 * return ��Ӽ�¼�Ƿ�ɹ�
 */
s16* MRecordPage::appendRecord(byte *record, u16 size) {
	u16 realSize = size + sizeof(s16);
	s16 *slot = NULL;
	if (m_freeSize < realSize) {
		return NULL;
	}

	if (realSize > m_lastRecordOffset - m_lastSlotOffset) {
		//defrag();
		return NULL;
	}

	byte *recBegin = (byte *)this + m_lastRecordOffset - size;
	memcpy(recBegin, record, size);
	m_lastRecordOffset -= size;
	slot = (s16 *)((byte *)this + m_lastSlotOffset);
	*slot = m_lastRecordOffset;

	m_lastSlotOffset += sizeof(s16);
	m_recordCnt++;
	m_freeSize -= realSize;
	return slot;
}

/** ����slot��ȡ���Ӧ��¼�ĳ���
 * @param slot slotָ��
 * @param ��Ӧ��¼����
 */
u16 MRecordPage::getRecordSize(s16 *slot) {
	u16 recordOffset = *slot;
	u16 recSize = *(u16 *)((byte *)this + recordOffset);
	return recSize;
}

/** ���¼�¼,���пռ�����ܹ����ɸ��º�ĳ���
 * @param slot ����slotָ��ļ�¼
 * @param record ���¼�¼�ĺ���
 * @param size ���º���ĳ���
 * return ���¼�¼���ڵ�slotָ��
 */
s16* MRecordPage::updateRecord(s16 *slot, byte *record, u16 size) {
	u16 recordOffset = *slot;
	u16 recSize = getRecordSize(slot);
	assert(m_freeSize >= size - recSize);
	//�������ǰ��ĳ��ȴ��ڸ��º�������ԭ�ظ���
	//������ڸ�ҳ��ɾȥ����ǰ����Ӹ��º���
	if (recSize >= size) {
		memcpy((byte *)this + recordOffset, record, size);
		m_freeSize += recSize - size;
		return slot;
	} else {
		removeRecord(slot);
		return appendRecord(record, size);
	}
}

/** ����slotɾ����Ӧ�ļ�¼
 * @param slot slotָ��
 */
void MRecordPage::removeRecord(s16 *slot) {
	m_freeSize += getRecordSize(slot) + sizeof(s16);
	*slot = INVALID_SLOT;
	m_recordCnt--;

	s16 *beginPtr = (s16 *)((byte *)this + sizeof(MRecordPage));
	s16 *lastPtr = (s16 *)((byte *)this + m_lastSlotOffset - sizeof(s16));
	//�����slot�պô������һ��slot������Ҫ����invalid slot����Чslot
	if (lastPtr == slot) {
		//����lastPtrǰһ����Ч��slot
		for (; lastPtr > beginPtr && *lastPtr == INVALID_SLOT; lastPtr--);
		//��slotǰ���е�slot��Ϊinvalid
		if (*lastPtr == INVALID_SLOT) {
			assert(lastPtr == beginPtr);
			m_lastRecordOffset = TNTIM_PAGE_SIZE;
			m_lastSlotOffset = sizeof(MRecordPage);
		} else {
			//��ʱlastPtrָ���slotΪ��Чֵ
			//byte *rec = (byte *)this + *lastPtr;
			m_lastRecordOffset = *lastPtr;
			m_lastSlotOffset = (u16)((byte *)lastPtr/*��Чslot��ʼ��ַ*/ - (byte *)this + sizeof(s16));
			assert(*lastPtr != INVALID_SLOT);
		}
	}
}

/** ����slot��ȡ��Ӧ��¼
 * @param slot slotָ��
 * @param rec out slot��Ӧ�ļ�¼
 * @param size out slot��Ӧ��¼�Ĵ�С
 * return ��ȡ�ɹ�����true�����򷵻�false
 */
bool MRecordPage::getRecord(s16 *slot, byte **rec, u16 *size) {
	s16 dataOffset = *slot;
	if (dataOffset == INVALID_SLOT) {
		return false;
	}
	*rec = (byte *)this + dataOffset;
	*size = *(u16 *)(*rec);
	return true;
}

bool MRecordPage::verify(HashIndexOperPolicy *hashIndexOperPolicy) {
	verify_ex(vs.mheap, MPT_MHEAP == m_pageType);
	//verify_ex(vs.mheap, TableDef::tableIdIsNormal(m_tableId));
	verify_ex(vs.mheap, m_freeSize <= (TNTIM_PAGE_SIZE - sizeof(MRecordPage)));
	verify_ex(vs.mheap, m_lastSlotOffset >= sizeof(MRecordPage));
	verify_ex(vs.mheap, m_lastSlotOffset <= TNTIM_PAGE_SIZE);
	verify_ex(vs.mheap, m_lastRecordOffset >= m_lastSlotOffset);
	verify_ex(vs.mheap, m_lastRecordOffset >= sizeof(MRecordPage));
	verify_ex(vs.mheap, m_lastRecordOffset <= TNTIM_PAGE_SIZE);
	
	MemoryContext ctx(TNTIM_PAGE_SIZE, 1);
	s16 *pSlot = (s16 *)((byte *)this + sizeof(MRecordPage));
	s16 *endSlot = (s16 *)((byte *)this + m_lastSlotOffset);

	u64 sp = 0;
	HashIndexEntry *indexEntry = NULL;
	MHeapRec *heapRec = NULL;
	byte *rec = NULL;
	u16 recSize = 0;
	u16 count = 0;
	for (; pSlot < endSlot; pSlot++) {
		if (*pSlot == INVALID_SLOT) {
			continue;
		}
		verify_ex(vs.mheap, *pSlot > 0);
		rec = (byte *)this + *pSlot;
		recSize = *(u16 *)(rec);
		sp = ctx.setSavepoint();
		Stream s(rec, recSize);
		heapRec = MHeapRec::unSerialize(&s, &ctx, INVALID_ROW_ID, false);
		count++;
		//printf("rowId = "I64FORMAT"u, slot = "I64FORMAT"u\n", heapRec->m_rec.m_rowId, (u64)rec);
		if (hashIndexOperPolicy == NULL) {
			continue;
		}

		indexEntry = hashIndexOperPolicy->get(heapRec->m_rec.m_rowId, &ctx);
		verify_ex(vs.mheap, indexEntry != NULL);
		verify_ex(vs.mheap, indexEntry->m_rowId == heapRec->m_rec.m_rowId);
		verify_ex(vs.mheap, indexEntry->m_value == (u64)pSlot);
		verify_ex(vs.mheap, indexEntry->m_type == HIT_MHEAPREC);
		ctx.resetToSavepoint(sp);
	}
	verify_ex(vs.mheap, count == m_recordCnt);
	return true;
}

}