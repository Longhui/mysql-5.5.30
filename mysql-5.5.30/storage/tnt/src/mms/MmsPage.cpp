/**
 * MMS页管理
 *
 * @author 邵峰(shaofeng@corp.netease.com, sf@163.org)
 */

#include "mms/Mms.h"
#include "mms/MmsPage.h"
#include "misc/Record.h"

namespace ntse {

/**
 * 在页中清除一条缓存记录
 *
 * @param recPage 记录页
 * @param mmsRecord 记录项
 * @return 是否需要调整页堆
 */
bool MmsPageOper::clearRecordInPage(MmsRecPage *recPage, MmsRecord *mmsRecord) {
	bool heapChangeNeeded;

	// 是否需要调整页堆
	if (offset2pointer(recPage, recPage->m_lruHead) == (byte *)mmsRecord)
		heapChangeNeeded = true;
	else 
		heapChangeNeeded = false;

	// 从页内LRU删除记录
	delFromLruList(recPage, mmsRecord);

	// 减页内脏记录数
	setDirty(recPage, mmsRecord, false);

	// 清空记录项
	memset(mmsRecord, 0, recPage->m_slotSize);

	// 添加到页内空闲链表
	*((u8 *)mmsRecord) = recPage->m_freeList;
	recPage->m_freeList = pointer2offset(recPage, (byte *)mmsRecord);

	// 增加空闲记录槽个数
	recPage->m_numFreeSlots++;

	// 增加版本号
	recPage->m_version++;

	// 调整rpClass统计信息
	recPage->m_rpClass->m_status.m_records--;
	recPage->m_rpClass->m_status.m_recordDeletes++;

	return heapChangeNeeded;
}

/**
 * 获取空闲记录槽
 * @pre 调用者已加页锁，并且当前页存在空闲记录槽
 *
 * @param recPage 缓存记录页指针
 * @return 记录项指针
 */
MmsRecord* MmsPageOper::getFreeRecSlot(MmsRecPage *recPage) {
	assert(recPage->m_numFreeSlots > 0);

	byte *tmpByte = offset2pointer(recPage, recPage->m_freeList);

	recPage->m_freeList = *tmpByte;
	recPage->m_numFreeSlots--;
	recPage->m_version++;

	// 调整rpClass统计信息
	recPage->m_rpClass->m_status.m_records++;
	recPage->m_rpClass->m_status.m_recordInserts++;

	return (MmsRecord *)tmpByte;
}

/** 
 * 获取页内最大/最小时间戳和使用记录数
 *
 * @param recPage 缓存页地址
 * @param tsMin INOUT 最小时间戳
 * @param tsMax INOUT 最大时间戳, 输入可能为NULL
 * @param numRecs INOUT 页内有效记录个数, 输入可能为NULL
 */
void MmsPageOper::getRecPageTs(MmsRecPage *recPage, u32 *tsMin, u32 *tsMax, u8 *numRecs) {
	assert (recPage->m_lruHead);

	MmsRecord *mmsRecord;

	// 最老项记录缓存时间戳
	mmsRecord = (MmsRecord *)offset2pointer(recPage, recPage->m_lruHead);
	*tsMin = mmsRecord->m_timestamp;

	// 最新项记录缓存时间戳
	if (tsMax) {
		mmsRecord = (MmsRecord *)offset2pointer(recPage, recPage->m_lruTail);
		*tsMax = mmsRecord->m_timestamp;
	}

	// 记录槽个数
	if (numRecs) {
		*numRecs = recPage->m_rpClass->m_numSlots - recPage->m_numFreeSlots;
	}
}

/** 
 * 获取某个页内的所有脏记录项 (需要判断该页是否为记录页，以及是否属于当前级别)
 *
 * @param session 会话
 * @param mms 全局MMS对象
 * @param rpClass 页类
 * @param recPage 记录页
 * @param tmpRecArray 临时数组
 */
void MmsPageOper::getDirtyRecords(Session *session, Mms *mms, MmsRPClass *rpClass, MmsRecPage *recPage, std::vector<MmsRecPair> *tmpRecArray) {
	MMSTABLE_TEST_GET_PAGE(rpClass->m_mmsTable, recPage);
	SYNCHERE(SP_MMS_GET_DIRTY_REC_1ST);

	if (!MMS_LOCK_IF_PAGE_TYPE(session, mms, recPage, PAGE_MMS_PAGE)) {
		SYNCHERE(SP_MMS_GET_DIRTY_REC_1ST_END);
		return;
	}

	MMSTABLE_TEST_GET_PAGE(rpClass->m_mmsTable, recPage);
	SYNCHERE(SP_MMS_GET_DIRTY_REC_2ND);

	// 非缓存记录页或非该页类所属的记录页
	if (recPage->m_rpClass != rpClass || recPage->m_numPins.get() == -1) {
		SYNCHERE(SP_MMS_GET_DIRTY_REC_2ND_END);
		unlockRecPage(session, mms, recPage);
		return;
	}

	u8 offset = recPage->m_lruHead;
	MmsRecord *mmsRecord;
	byte *tmpByte;

	while (&recPage->m_lruHead != (tmpByte = offset2pointer(recPage, offset))) {
		mmsRecord = (MmsRecord *)tmpByte;
		assert(mmsRecord->m_valid);
		if (mmsRecord->m_dirty) {
			tmpRecArray->push_back(MmsRecPair(mmsRecord));
		}
		offset = mmsRecord->m_lruNext;
	}
	unlockRecPage(session, mms, recPage);
}

/** 
 * 初始化记录页
 *
 * @param rpClass 页类对象
 * @param recPage 记录页指针
 *
 */
void MmsPageOper::initRecPage(MmsRPClass *rpClass, MmsRecPage *recPage) {
	recPage->m_version = 1;
	recPage->m_numPins.set(0);
	recPage->m_rpClass = rpClass;
	new (&recPage->m_freeLink) DLink<MmsRecPage *>;
	recPage->m_freeLink.set(recPage);
	recPage->m_slotSize = rpClass->m_slotSize;
	recPage->m_numFreeSlots = rpClass->m_numSlots;
	recPage->m_oldestHeapIdx = (u32) -1;
	recPage->m_lruHead = 0;
	recPage->m_lruTail = 0;
	recPage->m_numDirtyRecs = 0;

	// 初始化空闲链表
	byte *tmpByte = (byte *)recPage + MMS_REC_PAGE_HEAD_SIZE;
	u8 offset = 1;

	recPage->m_freeList = offset++;

	for (int i = 1; i < recPage->m_numFreeSlots; i++) {
		*tmpByte = offset++;
		tmpByte += recPage->m_slotSize;
	}

	assert(((byte *)recPage + Limits::PAGE_SIZE - tmpByte) / recPage->m_slotSize == 1);

	*tmpByte = 0;

	assert(recPage->m_freeLink.getList() == NULL);
}

/**
 * 在缓存记录槽填写内容(除rowId)
 *
 * @param recPage 记录页
 * @param mmsRecord	记录项
 * @param record 记录对象
 */
void MmsPageOper::fillRecSlotEx(MmsRecPage *recPage, MmsRecord *mmsRecord, const Record *record) {
	// 填写记录头
	assert(record->m_size <= (u16)-1); /** 记录长度小于(u16)-1 */
	mmsRecord->m_size = (u16)record->m_size;
	mmsRecord->m_timestamp = System::fastTime();
	mmsRecord->m_updateBitmap = 0;
	mmsRecord->m_dirtyBitmap = 0;
	mmsRecord->m_valid = 1;
	mmsRecord->m_dirty = 0;
	mmsRecord->m_padding = 0;
	mmsRecord->m_compressed = record->m_format == REC_COMPRESSED ? 1 : 0;

	// 填写记录内容
	memcpy((byte *)mmsRecord + sizeof(MmsRecord), record->m_data, record->m_size);
	
	// 添加到页内LRU链表末尾
	addToLruList(recPage, mmsRecord);
}

/**
 * 访问一个记录
 *				 ------------------------------<------------------------------                                                      
 * 页内LRU构造	|   -------------	  -------------		 -------------        |
 *				--->|  lruHead  | --> |   lruNext |  --> |   lruNext |---------   
 *					------------- 	  -------------		 -------------              相差1字节
 *				----|  lruTail  | <-- |   lruPrev | <--  |   lruPrev |<--------   
 *			   |	-------------     -------------      -------------        |
 *			   --------------------------- > ---------------------------------- 
 *
 * @param recPage 记录页
 * @param mmsRecord 记录项
 * @return 最老缓存页堆是否需要调整
 */
bool MmsPageOper::touchRecord(MmsRecPage *recPage, MmsRecord *mmsRecord) {
	assert(mmsRecord->m_valid);

	bool heapChangeNeeded = false;
	byte *tmpByte;

	// 更改记录项时间戳
	u32 oldTimestamp = mmsRecord->m_timestamp;
	mmsRecord->m_timestamp = System::fastTime();

	// 判断当前记录项是否为LRU链表首项
	tmpByte = offset2pointer(recPage, recPage->m_lruHead);
	if (tmpByte == (byte *)mmsRecord)
		heapChangeNeeded = true;

	// 把记录从LRU链表删除
	delFromLruList(recPage, mmsRecord);

	// 添加到末尾
	addToLruList(recPage, mmsRecord);

	// 如果新LRU链头记录块时间戳Tnew与旧LRU链头记录块时间戳Told之间的差值小于2秒，则不需要调整页堆
	if (heapChangeNeeded) {
		tmpByte = offset2pointer(recPage, recPage->m_lruHead);
		if (((MmsRecord *)tmpByte)->m_timestamp - oldTimestamp < 1)
			heapChangeNeeded = false;
	}

	return heapChangeNeeded;
}

/**
 * 获取FPage值
 * FPage值计算公式为： FPage = (Fmin + Fmax) / 2
 * 其中 F 表示动态频率，计算方式为 F = 1 / (当前时间戳 － 记录时间戳）
 *      N 表示记录数
 *
 * @param tsCurr 当前时间戳
 * @param tsMin 最老记录项时间戳
 * @param tsMax 最新记录项时间戳
 * @return FPage值
 */
float MmsPageOper::computeFPage(const u32 tsCurr, const u32 tsMin, const u32 tsMax) {
	float freq;

	if (tsCurr <= tsMax)
		freq = 2;
	else 
		freq = 1 / (float)(tsCurr - tsMax);

	if (tsCurr <= tsMin)
		freq += 2;
	else
		freq += 1 / (float)(tsCurr - tsMin);

	return freq / 2;
}



/** 
 * 判断记录页是否为最低访问频率页
 *
 * @param recPage 记录页
 * @param tsMin INOUT 最老时间戳
 * @param tsMax INOUT 最新时间戳
 * @param numRecs INOUT 记录项数
 * @param assigned 赋值操作
 * @param victimTable 惩罚表
 * @param pgRatio 页替换率，[0, 1]之间起作用
 * @param ext 扩展版本，增强替换算法
 * @return 是否为最低访问频率页
 */
bool MmsPageOper::isMinFPage(MmsRecPage *recPage, u32 *tsMin, u32 *tsMax, u8 *numRecs, bool assigned, MmsTable *victimTable, float pgRatio, bool ext) {
	u32 tsCurr = System::fastTime();
	u32 pg_tsMin, pg_tsMax;
	u8 pg_numRecs;
	float fPage, oldFPage;
	MmsTable *mmsTable;
	float extRatio;

	getRecPageTs(recPage, &pg_tsMin, &pg_tsMax, &pg_numRecs);
	//fPage = computeFPage(tsCurr, pg_tsMin, pg_tsMax, pg_numRecs);
	fPage = computeFPage(tsCurr, pg_tsMin, pg_tsMax);
	mmsTable = recPage->m_rpClass->m_mmsTable;
	if (ext) {// FPage = FPage × { 查询比例 / 记录页占用比例 }的三次方根
		extRatio = mmsTable->m_deltaRecordQueries * (float)mmsTable->m_mms->m_status.m_occupiedPages / (float)mmsTable->m_status.m_recordPages;
		if (extRatio < mmsTable->m_mms->m_minFPageRatio) extRatio =  mmsTable->m_mms->m_minFPageRatio;
		if (extRatio > mmsTable->m_mms->m_maxFPageRatio) extRatio =  mmsTable->m_mms->m_maxFPageRatio;
		fPage = fPage * extRatio;
	}

	if (assigned) {
		*tsMin = pg_tsMin;
		*tsMax = pg_tsMax;
		*numRecs = pg_numRecs;
		return true;
	}

	assert(victimTable);
	//oldFPage = computeFPage(tsCurr, *tsMin, *tsMax, *numRecs);
	oldFPage = computeFPage(tsCurr, *tsMin, *tsMax);
	if (ext) {
		extRatio = victimTable->m_deltaRecordQueries * (float)victimTable->m_mms->m_status.m_occupiedPages / (float)victimTable->m_status.m_recordPages;
		if (extRatio < victimTable->m_mms->m_minFPageRatio) extRatio =  victimTable->m_mms->m_minFPageRatio;
		if (extRatio > victimTable->m_mms->m_maxFPageRatio) extRatio =  victimTable->m_mms->m_maxFPageRatio;
		oldFPage = oldFPage * extRatio;
	}

	// 如果记录项和记录页之间比较，需考虑记录页替换权重参数
	if (pgRatio >= 0 && pgRatio <= 1)
		fPage *= pgRatio;

	if (fPage < oldFPage) {
		*tsMin = pg_tsMin;
		*tsMax = pg_tsMax;
		*numRecs = pg_numRecs;
		return true;
	}

	return false;
}


}
