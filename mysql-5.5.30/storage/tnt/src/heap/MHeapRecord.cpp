/**存放内存堆记录的各种页操作
 *author 忻丁峰 xindingfeng@corp.netease.com
 */
#include "heap/MHeapRecord.h"
#include "util/Stream.h"
#include "misc/MemCtx.h"
#include "trx/TNTTransaction.h"

namespace tnt {
/** 内存堆记录序列化长度
 * return 内存堆记录序列化长度
 */
u16 MHeapRec::getSerializeSize() {
	return (u16)(sizeof(m_size) + sizeof(m_vTableIndex) + sizeof(m_del) + sizeof(s8) /*m_rec.m_format*/ + m_rec.m_size
		+ RID_BYTES/*m_rec.m_rowId*/ + sizeof(m_txnId) + RID_BYTES/*m_rollBackId*/);
}

/** 内存堆记录序列化
 * @param s out 序列化后二进制数据
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

/** 内存堆记录反序列化
 * @param s 反序列化的二进制数据
 * @param ctx 上下文，主要为返回记录分配空间
 * return 反序列后的内存堆记录
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
	s->skip(sizeof(u16) + sizeof(u8) + sizeof(u8) + sizeof(s8)); //skip heapRec的m_size, m_vTableIndex, m_del, m_rec.m_format
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

//初始化页面信息
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

// 整理内存记录页的slot，去除其中为-1的slot
void MRecordPage::defragSlot(HashIndexOperPolicy *hashIndexOperPolicy) {
	assert((m_lastSlotOffset - sizeof(MRecordPage))%sizeof(s16) == 0);
	byte *buf = NULL;
	u16 recSize = 0;
	RowId rid = 0;
	s16 *src = NULL;
	//s16 *dest = (s16 *)((byte *)this + m_lastSlotOffset);
	s16 *lastPtr = (s16 *)((byte *)this + m_lastSlotOffset - sizeof(s16));
	//保证最后一个slot是被正常使用的
	//因为如果remove是最后一个slot，会改变m_lastSlotOffset
	//所以最后一个slot绝对不可能为INVALID_SLOT
	assert(*lastPtr != INVALID_SLOT);
	/*while (*dest == INVALID_SLOT) {
		m_lastSlotOffset -= sizeof(s16);
		dest--;
	}*/
	assert((byte *)lastPtr >= (byte *)this + sizeof(MRecordPage));

	//自开始项开始查找
	s16 *dest = (s16 *)((byte *)this + sizeof(MRecordPage));
	for (; dest < lastPtr; dest++) {
		//如果该slot有效，则继续查找下一个无效slot
		if (*dest != INVALID_SLOT) {
			continue;
		}
		//查找无效的slot被填充
		assert(*dest == INVALID_SLOT);
		//查找dest后第一个有效的slot
		for (src = dest + 1; src < lastPtr && (*src == INVALID_SLOT); src++);
		
		if (INVALID_SLOT == *src) {
			//此时意味着dest后再无有效slot
			assert(src == lastPtr);
			break;
		}

		//如果找到dest后一个有效的slot
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

	//此时dest指的是最后一项，但还未判断最后一项是否为invalid的
	m_lastSlotOffset = (u16)((byte *)dest - (byte *)this);
	if (*dest != INVALID_SLOT) {
		m_lastSlotOffset += sizeof(s16);
	}
}

/**
 *内存堆页的整理，主要是整理内存页，为了增加连续空闲空间用于存放记录
 */
void MRecordPage::defrag(HashIndexOperPolicy *hashIndexOperPolicy) {
	assert(m_lastSlotOffset >= sizeof(MRecordPage));
	//如果本身没有slot，就没存在整理的必要
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

/** 根据size的大小获取空闲级别
 * @param size 实际空间大小
 * return 空闲级别
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

/** 获取记录页最后一个slot地址，相应的记录及其大小
 * @param pSlot out 最后一个slot地址
 * @param rec out 最后一个记录地址
 * @param size out 最后一个记录大小
 * return 是否存在最后一个记录
 */
bool MRecordPage::getLastRecord(s16 **pSlot, byte **rec, u16 *size) {
	if (m_recordCnt == 0) {
		*pSlot = NULL;
		*rec = NULL;
		*size = 0;
		return false;
	}

	*pSlot = (s16 *)((byte *)this + m_lastSlotOffset - sizeof(s16));
	//由于getLastRecord是循环调用，所以不能保证最后一个肯定是有效的
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

/** 向记录页中添加记录
 * @param record 需要添加的记录
 * @param size 需要添加记录的大小
 * return 添加记录是否成功
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

/** 根据slot获取相对应记录的长度
 * @param slot slot指针
 * @param 对应记录长度
 */
u16 MRecordPage::getRecordSize(s16 *slot) {
	u16 recordOffset = *slot;
	u16 recSize = *(u16 *)((byte *)this + recordOffset);
	return recSize;
}

/** 更新记录,空闲空间必须能够容纳更新后的长度
 * @param slot 更新slot指向的记录
 * @param record 更新记录的后像
 * @param size 更新后像的长度
 * return 更新记录所在的slot指针
 */
s16* MRecordPage::updateRecord(s16 *slot, byte *record, u16 size) {
	u16 recordOffset = *slot;
	u16 recSize = getRecordSize(slot);
	assert(m_freeSize >= size - recSize);
	//如果更新前像的长度大于更新后像，则在原地更新
	//否则就在该页面删去更新前像，添加更新后像
	if (recSize >= size) {
		memcpy((byte *)this + recordOffset, record, size);
		m_freeSize += recSize - size;
		return slot;
	} else {
		removeRecord(slot);
		return appendRecord(record, size);
	}
}

/** 根据slot删除对应的记录
 * @param slot slot指针
 */
void MRecordPage::removeRecord(s16 *slot) {
	m_freeSize += getRecordSize(slot) + sizeof(s16);
	*slot = INVALID_SLOT;
	m_recordCnt--;

	s16 *beginPtr = (s16 *)((byte *)this + sizeof(MRecordPage));
	s16 *lastPtr = (s16 *)((byte *)this + m_lastSlotOffset - sizeof(s16));
	//如果该slot刚好处于最后一个slot，则需要回收invalid slot至有效slot
	if (lastPtr == slot) {
		//查找lastPtr前一个有效的slot
		for (; lastPtr > beginPtr && *lastPtr == INVALID_SLOT; lastPtr--);
		//该slot前所有的slot都为invalid
		if (*lastPtr == INVALID_SLOT) {
			assert(lastPtr == beginPtr);
			m_lastRecordOffset = TNTIM_PAGE_SIZE;
			m_lastSlotOffset = sizeof(MRecordPage);
		} else {
			//此时lastPtr指向的slot为有效值
			//byte *rec = (byte *)this + *lastPtr;
			m_lastRecordOffset = *lastPtr;
			m_lastSlotOffset = (u16)((byte *)lastPtr/*有效slot起始地址*/ - (byte *)this + sizeof(s16));
			assert(*lastPtr != INVALID_SLOT);
		}
	}
}

/** 根据slot获取相应记录
 * @param slot slot指针
 * @param rec out slot对应的记录
 * @param size out slot对应记录的大小
 * return 获取成功返回true，否则返回false
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