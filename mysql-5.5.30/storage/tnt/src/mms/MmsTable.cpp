/**
 * MMS模块实现
 *
 * 
 *
 * @author 邵峰(shaofeng@corp.netease.com, sf@163.org)
 */

#include <vector>
#include <algorithm>
#include "misc/Global.h"
#include "api/Database.h"
#include "api/Table.h"
#include "misc/Record.h"
#include "util/Stream.h"
#include "mms/Mms.h"
#include "mms/MmsPage.h"
#include "mms/MmsHeap.h"
#include "mms/MmsMap.h"
#include "misc/Trace.h"
#include "misc/Profile.h"

namespace ntse {

/************************************************************************/
/*                  　　　MMS表实现		　                              */
/************************************************************************/

class MmsRPClass;

/**
 * 构造一个MmsTable对象
 *
 * @param mms 所属全局MMS
 * @param db	 所属Database
 * @param drsHeap 所属堆
 * @param cacheUpdate 是否缓存更新
 * @param updateCacheTime 更新缓存时间，单位为秒
 * @param partitionNr RID分区个数
 */
MmsTable::MmsTable(Mms *mms, Database *db, DrsHeap *drsHeap, const TableDef *tableDef, bool cacheUpdate, uint updateCacheTime, int partitionNr):
	m_mmsTblLock(db->getConfig()->m_maxSessions + 1, "MmsTable::lock", __FILE__, __LINE__) {
	
	assert(tableDef->m_useMms);

	m_db = db;
	m_mms = mms;
	m_drsHeap = drsHeap;
	m_tableDef = tableDef;
	m_inLogFlush.set(0);
	m_numDirtyRecords.set(0);
	m_maxRecordCount = (u64) -1;
	m_existPin.set(0);
	m_preRecordQueries = 0;
	m_deltaRecordQueries = 1.0; // 最大值
	m_binlogCallback = NULL;
	m_cprsRcdExtractor = NULL;

	// 创建页级别
	m_rpClasses = new MmsRPClass *[m_mms->m_nrClasses];
	memset(m_rpClasses, 0, sizeof(MmsRPClass *) * m_mms->m_nrClasses);
	
	// 创建最低频率页堆
	m_freqHeap = new MmsFreqHeap(m_mms, this);

	// 创建临时页数组
	m_recPageArray = new Array<MmsRecPage *>();

	// 统计信息初始化
	memset(&m_status, 0, sizeof(MmsTableStatus));

	// 初始化MMS更新相关信息
	m_cacheUpdate = cacheUpdate;
	m_updateBitmapOffsets = new byte[tableDef->m_numCols];
	// 未使用，默认为255
	memset(m_updateBitmapOffsets, (byte)-1, tableDef->m_numCols * sizeof(byte));
	m_updateCacheNumCols = 0;

	if (cacheUpdate) {
		assert(tableDef->m_cacheUpdate);
		for (u16 i = 0; i < tableDef->m_numCols; i++) {
			ColumnDef *columnDef = tableDef->m_columns[i];
			if (columnDef->m_cacheUpdate)
				m_updateBitmapOffsets[i] = m_updateCacheNumCols++;
		}
		
		assert(m_updateCacheNumCols <= MAX_UPDATE_CACHE_COLUMNS);

		m_updateCacheCols = new u16[m_updateCacheNumCols];
		memset(m_updateCacheCols, 0, m_updateCacheNumCols * sizeof(u16));

		int j = 0;
		for (u16 i = 0; i < tableDef->m_numCols; i++) {
			ColumnDef *columnDef = tableDef->m_columns[i];
			if (columnDef->m_cacheUpdate)
				m_updateCacheCols[j++] = i;
		}
		assert(j == m_updateCacheNumCols);

		m_flushTimer = new MmsFlushTimer(db, this, updateCacheTime * 1000);
		m_flushTimer->start();
	} else {
		m_updateCacheCols = NULL;
		m_flushTimer = NULL;
	}

	setMapPartitions(partitionNr); // 设置映射分区

#ifdef NTSE_UNIT_TEST
	m_autoFlushLog = true;
	m_testCurrPage = NULL;
	m_testCurrRecord = NULL;
#endif
}

/**
 * 关闭一个表对应的MMS系统
 *
 * @param session	 会话对象
 * @param flushDirty 是否写出脏数据
 */
void MmsTable::close(Session *session, bool flushDirty) {
	ftrace(ts.mms, tout << session << flushDirty);

	while (m_existPin.get() > 0) // 等待表PIN为0
		Thread::msleep(50);
	// 删除更新信息
	if (m_flushTimer) {
		m_flushTimer->stop();
		m_flushTimer->join();
		delete m_flushTimer;
		m_flushTimer = NULL;
	}
	if (m_updateCacheCols) {
		delete [] m_updateCacheCols;
		m_updateCacheCols = NULL;
	}
	if (m_updateBitmapOffsets) {
		delete [] m_updateBitmapOffsets;
		m_updateBitmapOffsets = NULL;
	}

	MMS_RWLOCK(session->getId(), &m_mmsTblLock, Exclusived);

	// 刷写脏数据
	if (flushDirty)
		doFlush(session, true, true, true);

	// 删除映射表
	for (int i = 0; i < m_ridNrZone; i++) {
		delete m_ridLocks[i];
		m_ridLocks[i] = NULL;
		delete m_ridMaps[i];
		m_ridMaps[i] = NULL;
	}
	delete [] m_ridLocks;
	m_ridLocks = NULL;
	delete [] m_ridMaps;
	m_ridMaps = NULL;

	// 删除临时页数组
	delete m_recPageArray;
	m_recPageArray = NULL;

	// 删除页类 （包括页类中的所有记录页）
	for (int i = 0; i < m_mms->m_nrClasses; i++)
		if (m_rpClasses[i]) {
			m_rpClasses[i]->freeAllPages(session);
			delete m_rpClasses[i];
			m_rpClasses[i] = NULL;
		}
	delete [] m_rpClasses;
	m_rpClasses = NULL;

	// 删除最低频率页堆
	delete m_freqHeap;
	m_freqHeap = NULL;

	MmsPageOper::mmsRWUNLock(session->getId(), &m_mmsTblLock, Exclusived);
}

/** 
 * 获取更新缓存间隔刷新时间
 *
 * @return 间隔时间 单位为秒
 */
uint MmsTable::getUpdateCacheTime() {
	if (m_flushTimer)
		return m_flushTimer->getInterval() / 1000;
	return 0;
}

/** 
 * 设置更新缓存间隔刷新时间
 *
 * @param updateCacheTime 更新间隔刷新时间 间隔为秒
 */
void MmsTable::setUpdateCacheTime(uint updateCacheTime) {
	if (m_flushTimer) 
		m_flushTimer->setInterval(updateCacheTime * 1000);
}

/**
 * 根据RID获取MMS缓存记录
 * @post 返回的记录所在页已经被pin住
 *
 * @param session 会话对象
 * @param rid RID
 * @param touch 是否更新记录访问时间戳，进行表扫描时适合MRU而不是LRU替换，这时不更新时间戳
 * @param rlh INOUT，若不为NULL，则需要对返回的记录加锁，并用于存储行锁句柄，否则不加锁
 * @param lockMode 锁模式
 * 
 * @return 指向MMS记录的指针，找不到返回NULL
 */
MmsRecord* MmsTable::getByRid(Session *session, RowId rid, bool touch, RowLockHandle **rlh, LockMode lockMode) {
	PROFILE(PI_MmsTable_getByRid);

	ftrace(ts.mms, tout << session << rid << touch << rlh << lockMode);
	
	MmsRecord *mmsRecord;
	MmsRecPage *recPage;
	u16	pageVersion;
	int ridNr = getRidPartition(rid);
	bool lruChanged;
	LockMode recPageMode;

	m_mms->m_status.m_recordQueries++; // 非精确
	m_status.m_recordQueries++;

start:
	MMS_RWLOCK(session->getId(), m_ridLocks[ridNr], Shared);
	mmsRecord = m_ridMaps[ridNr]->get(rid);
	if (!mmsRecord) {
		MmsPageOper::mmsRWUNLock(session->getId(), m_ridLocks[ridNr], Shared);
		return NULL;
	}
	recPage = MmsPageOper::getRecPage(m_mms->m_pagePool, mmsRecord);
	lruChanged = false;
	recPageMode = Shared;
	if (touch) {
		byte *tmpByte = MmsPageOper::offset2pointer(recPage, recPage->m_lruHead);
		if ((tmpByte - (byte *)recPage < Limits::PAGE_SIZE)
			&& (System::fastTime() - ((MmsRecord *)tmpByte)->m_timestamp > 1)) {
				lruChanged = true;
				recPageMode = Exclusived;
		}
	}
#ifdef NTSE_UNIT_TEST
	m_testCurrPage = recPage;
#endif
	SYNCHERE(SP_MMS_RID_TRYLOCK);
	MMS_LOCK_REC_PAGE_EX(session, m_mms, recPage, recPageMode);
	MmsPageOper::mmsRWUNLock(session->getId(), m_ridLocks[ridNr], Shared);

#ifdef NTSE_UNIT_TEST
	m_testCurrPage = recPage;
#endif
	SYNCHERE(SP_MMS_RID_DISABLEPG);
	if (recPage->m_numPins.get() == -1) { // 该记录页不可用(正在被替换)
		MmsPageOper::unlockRecPage(session, m_mms, recPage, recPageMode);
		return NULL;
	}

	recPage->m_numPins.increment();

	// 是否已加行锁
	if (rlh) {
		assert (rid == RID_READ((byte *)mmsRecord->m_rid));
		SYNCHERE(SP_MMS_RID_LOCKROW);
		*rlh = TRY_LOCK_ROW(session, m_tableDef->m_id, rid, lockMode);
		if (NULL == *rlh) {
			// 尝试加记录锁失败，临时记录页版本号
			pageVersion = recPage->m_version;
			// 释放页锁
			MmsPageOper::unlockRecPage(session, m_mms, recPage, recPageMode);
			SYNCHERE(SP_MMS_RID_UNLOCKROW);
			// 加记录锁
			*rlh = LOCK_ROW(session, m_tableDef->m_id, rid, lockMode);
			recPageMode = Exclusived;
			// 加页锁
			MMS_LOCK_REC_PAGE_EX(session, m_mms, recPage, recPageMode);
			if (pageVersion != recPage->m_version 
				&& (!mmsRecord->m_valid || RID_READ(mmsRecord->m_rid) != rid)) {
					// 释放页上的锁和pin
					session->unlockRow(rlh);
					if (recPage->m_numPins.get() == 1 && recPage->m_numFreeSlots == recPage->m_rpClass->m_numSlots) { // 释放空页
						memset(recPage, 0, Limits::PAGE_SIZE); // 清空页
						m_mms->freeMmsPage(session, this, recPage);
					} else {
						recPage->m_numPins.decrement();
						MmsPageOper::unlockRecPage(session, m_mms, recPage, recPageMode);
					}
					goto start;
			}
		}
	}
	if (lruChanged && doTouch(session, recPage, mmsRecord)) // 访问缓存记录
		NTSE_ASSERT(false);  // 因为外部已经加行锁并且页加pin, 所以记录页不可能为空
	m_mms->m_status.m_recordQueryHits++;
	m_status.m_recordQueryHits++;
	assert(recPage->m_numPins.get() > 0);
	MmsPageOper::unlockRecPage(session, m_mms, recPage, recPageMode); // 释放记录页锁(页pin仍持有)
	return mmsRecord;
}

/** 
 * 根据RID搜索MMS并读取指定的属性
 * 注意：为提高性能，在判断是否为MMS脏记录时，该函数不加记录页锁！
 *
 * @param session 会话
 * @param rid 要搜索的记录RID
 * @param extractor 子记录提取器
 * @param subRec INOUT，要读取的部分记录，格式一定为REC_REDUNDANT
 *   若不需要读取部分记录，本函数保证传入的subRec内容不被修改
 * @param touch 是否更新记录访问时间戳
 * @param ifDirty 是否只在记录为脏时才读取部分记录
 * @param readMask 一个32位的位图表示要读取哪些属性，前31位与第1-31个属性一一对应，
 *   第32位总管剩下的所有属性，若ifDirty为true，则只有readMask中的属性在MMS中被修改
 *   时才需要读取部分记录
 * @return 是否命中MMS
 */
bool MmsTable::getSubRecord(Session *session, RowId rid, SubrecExtractor *extractor, SubRecord *subRec, bool touch, bool  ifDirty, u32 readMask) {
	PROFILE(PI_MmsTable_getSubRecord);

	assert(subRec->m_format == REC_REDUNDANT);
	UNREFERENCED_PARAMETER(session);

	MmsRecord *mmsRecord;
	MmsRecPage *recPage;
	int ridNr = getRidPartition(rid);
	bool lruChanged;
	LockMode recPageMode;

	m_mms->m_status.m_recordQueries++;
	m_status.m_recordQueries++;

start:
	MMS_RWLOCK(session->getId(), m_ridLocks[ridNr], Shared);
	mmsRecord = m_ridMaps[ridNr]->get(rid);// 查询RID映射表
	if (!mmsRecord) {
		MmsPageOper::mmsRWUNLock(session->getId(), m_ridLocks[ridNr], Shared);
		return false;
	}
	if (ifDirty && !(mmsRecord->m_dirtyBitmap & readMask)) {
		MmsPageOper::mmsRWUNLock(session->getId(), m_ridLocks[ridNr], Shared);
		goto end;
	}
	recPage = MmsPageOper::getRecPage(m_mms->m_pagePool, mmsRecord);
	lruChanged = false;
	recPageMode = Shared;
	if (touch) {
		byte *tmpByte = MmsPageOper::offset2pointer(recPage, recPage->m_lruHead);
		if ((tmpByte - (byte *)recPage < Limits::PAGE_SIZE)
			&& (System::fastTime() - ((MmsRecord *)tmpByte)->m_timestamp > 1)) {
				lruChanged = true;
				recPageMode = Exclusived;
		}
	}
	MMS_LOCK_REC_PAGE_EX(session, m_mms, recPage, recPageMode);
	MmsPageOper::mmsRWUNLock(session->getId(), m_ridLocks[ridNr], Shared);
#ifdef NTSE_UNIT_TEST
	m_testCurrPage = recPage;
#endif
	SYNCHERE(SP_MMS_SUBRECORD_DISABLEPG);
	if (recPage->m_numPins.get() == -1) { // 该记录页不可用(正在被替换)
		MmsPageOper::unlockRecPage(session, m_mms, recPage, recPageMode);
		return false;
	}
	recPage->m_numPins.increment();
	if (lruChanged && doTouch(session, recPage, mmsRecord))
		goto start; // 记录项已被其他线程删除，需要重新查询 
	getSubRecord(mmsRecord, extractor, subRec);
	recPage->m_numPins.decrement();
	MmsPageOper::unlockRecPage(session, m_mms, recPage, recPageMode);
end:
	m_mms->m_status.m_recordQueryHits++;
	m_status.m_recordQueryHits++;
	return true;
}

/** 
 * 访问记录项
 * @pre 已加记录锁
 *
 * @param session 会话
 * @param recPage 记录页
 * @param mmsRecord 记录项
 * @return 是否删除该页 （在touch过程中，该记录页内所有记录项被其他线程删除，导致空记录页，由该函数负责清空)
 */
bool MmsTable::doTouch(Session *session, MmsRecPage *recPage, MmsRecord *mmsRecord) {
	PROFILE(PI_MmsTable_doTouch);

	if (MmsPageOper::touchRecord(recPage, mmsRecord)) {	
#ifdef NTSE_UNIT_TEST
		m_testCurrPage = recPage;
		m_testCurrRecord = mmsRecord;
#endif
		SYNCHERE(SP_MMS_DOTOUCH_LOCK);
		if (MMS_TRYRWLOCK(session->getId(), &m_mmsTblLock, Exclusived)) {
			recPage->m_rpClass->m_oldestHeap->moveDown(session, recPage->m_oldestHeapIdx, true, recPage);
		} else { // TODO: 如果该处存在性能瓶颈，可以考虑在TRYLOCK MMS锁不成功时，不执行touch操作！
			MmsPageOper::unlockRecPage(session, m_mms, recPage);
			SYNCHERE(SP_MMS_DOTOUCH_UNLOCK);
			MMS_RWLOCK(session->getId(), &m_mmsTblLock, Exclusived);
			MMS_LOCK_REC_PAGE(session, m_mms, recPage);
			if (recPage->m_numFreeSlots != recPage->m_rpClass->m_numSlots) // 记录页不为空
				recPage->m_rpClass->m_oldestHeap->moveDown(session, recPage->m_oldestHeapIdx, true, recPage);
			else if (recPage->m_numPins.get() == 1) {
				MmsPageOper::mmsRWUNLock(session->getId(), &m_mmsTblLock, Exclusived);
				memset(recPage, 0, Limits::PAGE_SIZE);
				m_mms->freeMmsPage(session, this, recPage);
				return true;
			}
		}
		MmsPageOper::mmsRWUNLock(session->getId(), &m_mmsTblLock, Exclusived);
	}
	return false;
}

/**
 * 释放MMS记录所在页的pin
 *
 * @param session 会话
 * @param mmsRecord MMS记录
 */
void MmsTable::unpinRecord(Session *session, const MmsRecord *mmsRecord) {
	ftrace(ts.mms, tout << session << (void *)mmsRecord);
	UNREFERENCED_PARAMETER(session);
	MmsRecPage *recPage = MmsPageOper::getRecPage(m_mms->m_pagePool, const_cast<MmsRecord *>(mmsRecord));
	recPage->m_numPins.decrement();
}

/**
 * 插入一条记录在MMS缓存中
 * @pre  该记录不在MMS缓存中, 已加MMS表锁
 * @post 释放MMS表锁，记录所在页被pin住
 *
 * @param session 会话对象
 * @param record 要插入的记录
 * @param dirtyBitmap 脏位图
 * @param ridNr 分区锁索引
 * @param tryAgain INOUT 再次尝试
 * @return 记录项；插入不成功，返回NULL
 */
MmsRecord* MmsTable::put(Session *session, const Record *record, u32 dirtyBitmap, int ridNr, bool *tryAgain) {
	ftrace(ts.mms, tout << session << record << dirtyBitmap << ridNr << *tryAgain);
	assert(record);
	assert(record->m_format == REC_FIXLEN || record->m_format == REC_VARLEN || record->m_format == REC_COMPRESSED);
	assert(sizeof(MmsRecord) + record->m_size <= MMS_MAX_RECORD_SIZE);
	
	MmsRecord *mmsRecord;
	MmsRecPage *recPage;
	
	*tryAgain = false;
	if (!m_ridMaps[ridNr]->reserve(1, m_mms->m_duringRedo)) {
		MmsPageOper::mmsRWUNLock(session->getId(), m_ridLocks[ridNr], Exclusived);
		m_status.m_replaceFailsWhenPut++;
		return NULL;
	}

	MMS_RWLOCK(session->getId(), &m_mmsTblLock, Exclusived);

	// 获取页类型
	int classType = m_mms->m_size2class[sizeof(MmsRecord) + record->m_size];
	if (!m_rpClasses[classType])
		m_rpClasses[classType] = new MmsRPClass(this, m_mms->m_pageClassSize[classType]);

	// 从页类空闲记录页链表中获取空闲记录页 
	recPage = m_rpClasses[classType]->getRecPageFromFreelist();
	if (!recPage) {
		bool ridLocked = true;
		// 从全局MMS中获取空闲记录页用于设置记录项，记录页已加锁，并且已放入空闲页链表
		recPage = allocMmsRecord(session, m_rpClasses[classType], ridNr, &ridLocked);
		if (!recPage) {
			if (ridLocked)
				MmsPageOper::mmsRWUNLock(session->getId(), m_ridLocks[ridNr], Exclusived);
			MmsPageOper::mmsRWUNLock(session->getId(), &m_mmsTblLock, Exclusived);
			m_status.m_replaceFailsWhenPut++;
			return NULL;
		}
		if (!ridLocked) { // 重做
			*tryAgain = true;
			MmsPageOper::mmsRWUNLock(session->getId(), &m_mmsTblLock, Exclusived);
			recPage->m_numPins.decrement();
			MmsPageOper::unlockRecPage(session, m_mms, recPage);
			return NULL;
		}
	} else {
		MMS_LOCK_REC_PAGE(session, m_mms, recPage);
		recPage->m_numPins.increment();
	}
		
	mmsRecord = MmsPageOper::getFreeRecSlot(recPage);
	RID_WRITE(record->m_rowId, (byte *)mmsRecord->m_rid);
	// 在RID映射表中添加映射项
	m_ridMaps[ridNr]->put(mmsRecord);
	m_ridMaps[ridNr]->unreserve();
	MmsPageOper::mmsRWUNLock(session->getId(), m_ridLocks[ridNr], Exclusived); // 先释放RID分区锁
	MmsPageOper::fillRecSlotEx(recPage, mmsRecord, record);
	if (dirtyBitmap)
		MmsPageOper::setDirty(recPage, mmsRecord, true, dirtyBitmap);
	if (m_rpClasses[classType]->m_numSlots == recPage->m_numFreeSlots + 1)
		m_rpClasses[classType]->m_oldestHeap->insert(recPage);
	if (0 == recPage->m_numFreeSlots) { 
		// 注：这里不需要调整页堆
		// 如果满，则从空闲队列删除
		m_rpClasses[classType]->delRecPageFromFreelist(recPage);
	}

	m_status.m_recordInserts++;
	m_mms->m_status.m_recordInserts++;
	m_status.m_records++;
	MmsPageOper::mmsRWUNLock(session->getId(), &m_mmsTblLock, Exclusived);
	MmsPageOper::unlockRecPage(session, m_mms, recPage);
	
	return mmsRecord;
}

/**
 * 当指定的记录不在MMS缓存中时，插入一条记录在MMS缓存中。
 * @post 返回的记录所在页已经被pin住
 *
 * @param session 会话对象
 * @param record 要插入的记录
 * @return 若记录在MMS中已经存在，则返回已经存在的记录；否则返回新插入的记录；如果插入不成功，返回NULL
 */
MmsRecord* MmsTable::putIfNotExist(Session *session, const Record *record) {
	PROFILE(PI_MmsTable_putIfNotExist);

	ftrace(ts.mms, tout << session << record);
	assert(record->m_format == REC_FIXLEN || record->m_format == REC_VARLEN || record->m_format == REC_COMPRESSED);
	
	MmsRecord *mmsRecord;
	MmsRecPage *recPage;
	int ridNr = getRidPartition(record->m_rowId);
	int tryCount = 0;
	bool tryAgain = false;

start:
	// 加MMS表锁
	MMS_RWLOCK(session->getId(), m_ridLocks[ridNr], Exclusived);

	// 查询主键映射表
	if ((mmsRecord = m_ridMaps[ridNr]->get(record->m_rowId)) != NULL) {
		recPage = MmsPageOper::getRecPage(m_mms->m_pagePool, mmsRecord);
		MMS_LOCK_REC_PAGE(session, m_mms, recPage);
#ifdef NTSE_UNIT_TEST
		m_testCurrPage = recPage;
#endif
		SYNCHERE(SP_MMS_PUT_DISABLEPG);
		if (recPage->m_numPins.get() == -1) { // 该记录页不可用
			MmsPageOper::mmsRWUNLock(session->getId(), m_ridLocks[ridNr], Exclusived);
			MmsPageOper::unlockRecPage(session, m_mms, recPage);
			return NULL;
		}
		recPage->m_numPins.increment();
		MmsPageOper::mmsRWUNLock(session->getId(), m_ridLocks[ridNr], Exclusived);
		if (doTouch(session, recPage, mmsRecord))
			return NULL; // 记录项被删除
		MmsPageOper::unlockRecPage(session, m_mms, recPage);
		return mmsRecord;
	}
	mmsRecord = put(session, record, false, ridNr, &tryAgain);
	if (!mmsRecord && tryAgain && tryCount == 0) {
		tryCount++;
		goto start;
	}
	return mmsRecord;
}

/** 
 * 删除记录项
 * @pre 已加表锁和记录页锁
 *
 * @param session 会话
 * @param recPage 记录页
 * @param mmsRecord 记录项
 */
void MmsTable::delRecord(Session *session, MmsRecPage *recPage, MmsRecord *mmsRecord) {
	ftrace(ts.mms, tout << session << recPage << mmsRecord << RID_READ(mmsRecord->m_rid));

	// 在页内删除旧记录项(注：由外层模块加锁保证无其他线程读取该记录项内容)
	bool changedTimestamp = MmsPageOper::clearRecordInPage(recPage, mmsRecord);

	// 如果记录页从满变为空闲，则添加到空闲链表中
	if (1 == recPage->m_numFreeSlots)
		recPage->m_rpClass->addRecPageToFreeList(recPage);

	// 需要调整页堆
	if (changedTimestamp) {
		MmsRPClass *rpClass = recPage->m_rpClass;
		// 记录页为全空且无其他线程加pin
		if (recPage->m_numFreeSlots == rpClass->m_numSlots) {
			// 从页堆删除, 注意recNewPage页锁必须已经释放	
			rpClass->m_oldestHeap->del(session, recPage->m_oldestHeapIdx, recPage);
			// 从空闲链表删除
			rpClass->delRecPageFromFreelist(recPage);
			if (recPage->m_numPins.get() == 1) {
				// 清空页
				memset(recPage, 0, Limits::PAGE_SIZE);
				MmsPageOper::mmsRWUNLock(session->getId(), &m_mmsTblLock, Exclusived);
				// 把自由空闲页释放到MMS
				m_mms->freeMmsPage(session, this, recPage);
			} else {
				recPage->m_numPins.decrement();
				MmsPageOper::unlockRecPage(session, m_mms, recPage);
				MmsPageOper::mmsRWUNLock(session->getId(), &m_mmsTblLock, Exclusived);
			}
		} else { // 空记录页情况
			rpClass->m_oldestHeap->moveDown(session, recPage->m_oldestHeapIdx, true, recPage);
			recPage->m_numPins.decrement();
			MmsPageOper::unlockRecPage(session, m_mms, recPage);
			MmsPageOper::mmsRWUNLock(session->getId(), &m_mmsTblLock, Exclusived);
		}
		return;
	}
	MmsPageOper::mmsRWUNLock(session->getId(), &m_mmsTblLock, Exclusived);
	recPage->m_numPins.decrement();
	MmsPageOper::unlockRecPage(session, m_mms, recPage);
}

/** 
 * 能否更新MMS记录
 * @pre 调用者已经持有记录页pin
 *
 * @param mmsRecord MMS记录项
 * @param subRecord 更新的属性及属性值 REC_REDUNDANT
 * @param recSize OUT 更新后记录大小
 * @return 能否更新
 */
bool MmsTable::canUpdate(MmsRecord *mmsRecord, const SubRecord *subRecord, u16 *recSize) {
	Record oldRecord;

	MmsRecPage *recPage = MmsPageOper::getRecPage(m_mms->m_pagePool, mmsRecord);

	// 计算新记录项长度
	oldRecord.m_rowId = subRecord->m_rowId;
	oldRecord.m_size = mmsRecord->m_size;
	oldRecord.m_format = getMmsRecFormat(m_tableDef, mmsRecord);
	oldRecord.m_data = (byte *)mmsRecord + sizeof(MmsRecord);
	assert(oldRecord.m_format != REC_COMPRESSED);
	if (REC_FIXLEN == oldRecord.m_format) 
		*recSize = (u16)oldRecord.m_size;
	else
		*recSize = RecordOper::getUpdateSizeVR(m_tableDef, &oldRecord, subRecord);
	if (*recSize + sizeof(MmsRecord) > recPage->m_slotSize)
		return false;
	return true;
}

/** 能够更新MMS记录
 * @pre 调用者已经持有记录页pin
 *
 * @param mmsRecord MMS记录项
 * @param newSize 更新后记录大小
 * @return 能否更新
 */
bool MmsTable::canUpdate(MmsRecord *mmsRecord, u16 newSize) {
	MmsRecPage *recPage = MmsPageOper::getRecPage(m_mms->m_pagePool, mmsRecord);
	return (newSize + sizeof(MmsRecord)) <= recPage->m_slotSize;
}

/**
 * 更新一个MMS记录
 * @pre 调用者持有pin
 * @post pin被释放
 *
 * @param session 会话对象
 * @param mmsRecord MMS记录项
 * @param subRecord 更新的属性及属性值 REC_REDUNDANT
 * @param recSize	更新后记录大小
 * @param newCprsRcd INOUT 如果输入不为NULL，则输出为更新后的记录
 */
void MmsTable::update(Session *session, MmsRecord *mmsRecord, const SubRecord *subRecord, 
					  u16 recSize, Record *newCprsRcd){
	ftrace(ts.mms, tout << session << mmsRecord << subRecord << recSize);
	
	PROFILE(PI_MmsTable_update);

	Record oldRecord(INVALID_ROW_ID, m_tableDef->m_recFormat, NULL, 0);
	u32 updBitmap = 0;
	u32 dirtyBitmap = 0;
	bool notCached = cols2bitmap(subRecord, &updBitmap, &dirtyBitmap); // 计算本次更新位图
	MmsRecPage *recPage = MmsPageOper::getRecPage(m_mms->m_pagePool, mmsRecord);
	assert(recPage->m_numPins.get() > 0);
	assert (REC_REDUNDANT == subRecord->m_format);

	MMS_LOCK_REC_PAGE(session, m_mms, recPage);
	m_status.m_recordUpdates++;
	m_mms->m_status.m_recordUpdates++;
	recPage->m_rpClass->m_status.m_recordUpdates++;
	getRecord(mmsRecord, &oldRecord, false);

	if (m_cprsRcdExtractor != NULL) {
		MemoryContext *ctx = session->getMemoryContext();
		McSavepoint msp(ctx);
		if (newCprsRcd == NULL) {		
			//这里不采用本地更新压缩记录，因为有可能更新之后的压缩记录比原来更长，极端情况可能压缩之后比不压缩更长
			void *rcdData = ctx->alloc(sizeof(Record));
			uint bufSize = m_tableDef->m_maxRecSize << 1;
			byte *data = (byte *)ctx->alloc(bufSize);
			newCprsRcd = new (rcdData)Record(oldRecord.m_rowId, REC_VARLEN, data, bufSize);
			RecordOper::updateRcdWithDic(ctx, m_tableDef, m_cprsRcdExtractor, &oldRecord, subRecord, newCprsRcd);
		}
		assert(subRecord->m_rowId == newCprsRcd->m_rowId);
		assert(newCprsRcd->m_size + sizeof(MmsRecord) <= MmsPageOper::getRecPage(m_mms->m_pagePool, mmsRecord)->m_slotSize);
		memcpy(oldRecord.m_data, newCprsRcd->m_data, newCprsRcd->m_size);
		oldRecord.m_size = newCprsRcd->m_size;
		mmsRecord->m_compressed = (newCprsRcd->m_format == REC_COMPRESSED ? 1 : 0);
		recSize = (u16)newCprsRcd->m_size;
	} else {
		assert(REC_FIXLEN == oldRecord.m_format || REC_VARLEN == oldRecord.m_format);
		if (REC_FIXLEN == oldRecord.m_format)
			RecordOper::updateRecordFR(m_tableDef, &oldRecord, subRecord);
		else 
			RecordOper::updateRecordVRInPlace(m_tableDef, &oldRecord, subRecord, 
			recPage->m_slotSize - sizeof(MmsRecord));
	}

	mmsRecord->m_size = recSize;
	assert(mmsRecord->m_size <= m_tableDef->m_maxRecSize);
	// 设为脏记录，注：在更新中不重设时间戳
	MmsPageOper::setDirty(recPage, mmsRecord, true, mmsRecord->m_dirtyBitmap | dirtyBitmap);
	// 更新操作
	if (m_cacheUpdate) {
		if (notCached) {
			//先释放页锁后释放pin，期间页面不会被替换，
			//并且上层已经持有行锁，对mmsRecord的读写是安全的
			MmsPageOper::unlockRecPage(session, m_mms, recPage);
			writeMmsUpdateLog(session, mmsRecord, subRecord, mmsRecord->m_updateBitmap);	
			mmsRecord->m_updateBitmap = 0;
			recPage->m_numPins.decrement();
		} else {
			mmsRecord->m_updateBitmap |= updBitmap;
			recPage->m_numPins.decrement();
			MmsPageOper::unlockRecPage(session, m_mms, recPage);
			m_status.m_updateMerges++;
		}
	} else {
		recPage->m_numPins.decrement();
		MmsPageOper::unlockRecPage(session, m_mms, recPage);
		writeMmsUpdateLog(session, subRecord);
	}
}

/** 
 * 刷脏数据并删除记录项
 * @pre 持有pin
 * @post 释放pin
 * 
 * @param session 会话对象
 * @param mmsRecord 记录项
 */
void MmsTable::flushAndDel(Session *session, MmsRecord *mmsRecord) {
	ftrace(ts.mms, tout << session << mmsRecord << RID_READ(mmsRecord->m_rid));

	MmsRecPage *recPage = MmsPageOper::getRecPage(m_mms->m_pagePool, mmsRecord);
	int ridNr = getRidPartition(RID_READ(mmsRecord->m_rid));

	MMS_RWLOCK(session->getId(), m_ridLocks[ridNr], Exclusived);
	m_ridMaps[ridNr]->del(mmsRecord); // 在RID映射表中删除对应入口项
	MmsPageOper::mmsRWUNLock(session->getId(), m_ridLocks[ridNr], Exclusived);

	MMS_RWLOCK(session->getId(), &m_mmsTblLock, Exclusived);
	MMS_LOCK_REC_PAGE(session, m_mms, recPage);
	assert(recPage->m_numPins.get() > 0);
	m_status.m_records--;
	m_status.m_recordDeletes++;
	m_mms->m_status.m_recordDeletes++;
	if (mmsRecord->m_dirty)
		writeDataToDrs(session, recPage, mmsRecord,  true);
	delRecord(session, recPage, mmsRecord);
}

/**
 * 删除一个MMS记录
 * @pre 调用者已经持有了记录所在MMS页的pin
 * @post 记录所在MMS页的pin已经被释放
 *
 * @param session 会话
 * @param mmsRecord MMS记录
 */
void MmsTable::del(Session *session, MmsRecord *mmsRecord) {
	PROFILE(PI_MmsTable_del);

	ftrace(ts.mms, tout << session << mmsRecord << RID_READ(mmsRecord->m_rid));
	assert(mmsRecord);
	MmsRecPage *recPage = MmsPageOper::getRecPage(m_mms->m_pagePool, mmsRecord);
	int ridNr = getRidPartition(RID_READ(mmsRecord->m_rid));

	MMS_RWLOCK(session->getId(), m_ridLocks[ridNr], Exclusived);
	m_ridMaps[ridNr]->del(mmsRecord); // 在RID映射表中删除对应入口项
	MmsPageOper::mmsRWUNLock(session->getId(), m_ridLocks[ridNr], Exclusived);

	MMS_RWLOCK(session->getId(), &m_mmsTblLock, Exclusived);
	MMS_LOCK_REC_PAGE(session, m_mms, recPage);
	assert(recPage->m_numPins.get() > 0);
	m_status.m_records--;
	m_status.m_recordDeletes++;
	m_mms->m_status.m_recordDeletes++;
	delRecord(session, recPage, mmsRecord);
}

/**
 * 写出所有脏记录到DRS中（在检查点时调用）
 *
 * @param session 会话对象
 * @param force 强制刷脏数据
 * @param ignoreCancel 是否忽略操作取消请求
 * @throw NtseException 操作被取消，异常码为NTSE_EC_CANCELED
 */
void MmsTable::flush(Session *session, bool force, bool ignoreCancel) throw(NtseException) {
	ftrace(ts.mms, tout << session << ignoreCancel;);

	doFlush(session, force, ignoreCancel, false);
}

/**
 * 刷写脏记录
 *
 * @param session 会话对象
 * @param force 强制刷脏数据
 * @param ignoreCancel 是否忽略操作取消请求
 * @param tblLocked 是否已加MMS表锁
 * @throw NtseException 操作被取消，异常码为NTSE_EC_CANCELED
 */
void MmsTable::doFlush(Session *session, bool force, bool ignoreCancel, bool tblLocked) throw(NtseException) {
	Array<MmsRecPage *> tmpPageArray;
	std::vector<MmsRecPair> tmpRecArray;

	u64 totalDirties = m_numDirtyRecords.get() + 1;
	u64 doneDirties = 0;
	
	for (int i = 0;  i < m_mms->m_nrClasses; i++) {
		MmsRPClass *rpClass = m_rpClasses[i];
		if (rpClass) {// 对级别内的所有页执行刷写检查
			if (!tblLocked)
				MMS_RWLOCK(session->getId(), &m_mmsTblLock, Shared);
			// 获取级别内的所有记录页
			Array<OldestHeapItem> *heapArray = rpClass->m_oldestHeap->getHeapArray();
			size_t size;

			size = heapArray->getSize();
			for (uint iPage = 0; iPage < size; iPage++)
				if (!tmpPageArray.push((*heapArray)[iPage].m_page))
					NTSE_ASSERT(false);
			if (!tblLocked)
				MmsPageOper::mmsRWUNLock(session->getId(), &m_mmsTblLock, Shared);

			size = tmpPageArray.getSize();
			for (uint iPage = 0; iPage < size; iPage++) {
				MmsPageOper::getDirtyRecords(session, m_mms, rpClass, tmpPageArray[iPage], &tmpRecArray);
				if ((int)tmpRecArray.size() > m_mms->m_maxNrDirtyRecs) {
					double pct = (double)(doneDirties + tmpRecArray.size()) / totalDirties;
					if (pct > 1.0)
						pct = 1.0;
					doneDirties += tmpRecArray.size();

					// 异常时直接返回，没有内存和锁泄漏
					sortAndFlush(session, force, ignoreCancel, this, &tmpRecArray);
					tmpRecArray.clear();
				}
			}

			tmpPageArray.clear();
		}
	}

	if (tmpRecArray.size() > 0) {
		sortAndFlush(session, force, ignoreCancel, this, &tmpRecArray);
		tmpRecArray.clear();
	}
}

/**
 * 排序并刷新缓存记录项
 *
 * @param session 会话对象
 * @param force 强制刷脏数据
 * @param ignoreCancel 是否忽略操作取消请求
 * @param table 所属的MMS表
 * @param tmpRecArray 临时缓存
 * @throw NtseException 操作被取消，异常码为NTSE_EC_CANCELED
 */
void MmsTable::sortAndFlush(Session *session, bool force, bool ignoreCancel, MmsTable *table, std::vector<MmsRecPair> *tmpRecArray) throw(NtseException) {
	m_db->getSyslog()->log(EL_LOG, "sortAndFlush[size: %d]", (int)tmpRecArray->size()); 
	size_t remain = tmpRecArray->size();
	size_t done = 0, skip1 = 0, skip2 = 0, oldDone = 0;
	uint batchSize = m_db->getConfig()->m_systemIoCapacity * 200;
	std::sort(tmpRecArray->begin(), tmpRecArray->end());

	u64 prBefore = session->getConnection()->getLocalStatus()->m_statArr[OPS_PHY_READ], prAfter = 0;
	u64 prOrigin = prBefore;
	u64 before = System::currentTimeMillis(), now = 0, time = 0;
	u64 totalSleep = 0;
	for (uint iRec = 0; iRec < tmpRecArray->size(); iRec++) {
		if (!ignoreCancel && session->isCanceled())
			NTSE_THROW(NTSE_EC_CANCELED, "sortAndFlush has been canceled.");
		
		MmsRecPair recPair = (*tmpRecArray)[iRec];
		MmsRecord *mmsRecord = recPair.m_mmsRecord;
		MmsRecPage *recPage = MmsPageOper::getRecPage(m_mms->m_pagePool, mmsRecord);
		MMSTABLE_TEST_GET_PAGE(this, recPage);
		remain--;
		done++;
#ifdef NTSE_UNIT_TEST
		m_testCurrRPClass = recPage->m_rpClass;
#endif
		SYNCHERE(SP_MMS_SF_LOCK_PG);
		if (!MMS_LOCK_IF_PAGE_TYPE(session, m_mms, recPage, PAGE_MMS_PAGE)) {
			SYNCHERE(SP_MMS_SF_UNLOCK_PG);
			skip1++;
			continue;
		}
		if (table != recPage->m_rpClass->m_mmsTable ||					             // 不是所属表的
			RID_READ((byte *)mmsRecord->m_rid) != RID_READ((byte *)recPair.m_rid) || // RID不匹配
			!mmsRecord->m_dirty) {									                 // 非脏记录
			MmsPageOper::unlockRecPage(session, m_mms, recPage);
			skip2++;
			continue;	
		}

		writeDataToDrs(session, recPage, mmsRecord, force);
		MmsPageOper::unlockRecPage(session, m_mms, recPage);
		
		if ((done - oldDone) >= batchSize
			&& remain > 0
			&& m_db->getStat() != DB_CLOSING) {
			prAfter = session->getConnection()->getLocalStatus()->m_statArr[OPS_PHY_READ];
			if (prAfter > prBefore) {	// 防止在页面缓存够大时不必要的sleep
				prBefore = prAfter; 
				now = System::currentTimeMillis();
				int sleepMillis = 1000 - (int)(now - before);
				if(sleepMillis > 0) {
					Thread::msleep(sleepMillis);
					before = now + sleepMillis;
				} else
					before = now;

				now = System::currentTimeMillis();
				time += now - before;
				before = now;
			}
			oldDone = done;
		}
	}
	prAfter = session->getConnection()->getLocalStatus()->m_statArr[OPS_PHY_READ];
	m_db->getSyslog()->log(EL_LOG, "Sleep "I64FORMAT"u ms, avg service time: %d ms, skipped: %d/%d", totalSleep, (int)(done ? time / done: 0), (int)skip1, (int)skip2);
	m_db->getSyslog()->log(EL_LOG, "Number of reads caused by flushing %d dirty records: %d", (int)tmpRecArray->size(), (int)(prAfter - prOrigin));
}

/** 
 * 刷写更新日志
 *
 * @param session 会话
 * @param force 是否为强制刷新
 */
void MmsTable::flushLog(Session *session, bool force) {
	ftrace(ts.mms, tout << session << force);

	if (force) {
		assert(m_inLogFlush.get());	
	} else {
		if (m_mms->m_duringRedo) // REDO阶段，不执行间隔刷新
			return;
		if (!m_inLogFlush.compareAndSwap(0, 1))
			return;
	}

	Array<OldestHeapItem> *heapArray;
	size_t size;
	MmsRecPage *recPage;
	u8 offset;
	MmsRecord *mmsRecord;
	byte *tmpByte;

	SYNCHERE(SP_MMS_FL);
	for (int i = 0;  i < m_mms->m_nrClasses; i++) {
		MmsRPClass *rpClass = m_rpClasses[i];
		if (rpClass) {// 对级别内的所有页执行刷写检查
			MMS_RWLOCK(session->getId(), &m_mmsTblLock, Shared);
			// 获取级别内的所有记录页
			heapArray = rpClass->m_oldestHeap->getHeapArray();
			size = heapArray->getSize();
			for (uint iPage = 0; iPage < size; iPage++) {
				if ((*heapArray)[iPage].m_page->m_numDirtyRecs) // 如果记录页不存在脏记录项，则无日志更新
					if (!m_recPageArray->push((*heapArray)[iPage].m_page))
						NTSE_ASSERT(false); // 系统挂掉	
			}
			MmsPageOper::mmsRWUNLock(session->getId(), &m_mmsTblLock, Shared);
			size = m_recPageArray->getSize();
			for (uint iPage = 0; iPage < size; iPage++) {
				recPage = (*m_recPageArray)[iPage];
				MMSTABLE_TEST_GET_PAGE(this, recPage);
				SYNCHERE(SP_MMS_FL_LOCK_PG);
				if (!MMS_LOCK_IF_PAGE_TYPE(session, m_mms, recPage, PAGE_MMS_PAGE)) {
					SYNCHERE(SP_MMS_FL_UNLOCK_PG);
					continue;
				}
				// 非缓存记录页或非该页类所属的记录页
				if (recPage->m_rpClass != rpClass || recPage->m_numPins.get() == -1) {
					MmsPageOper::unlockRecPage(session, m_mms, recPage);
					continue;
				}
				offset = recPage->m_lruHead;
				while (&recPage->m_lruHead != (tmpByte = MmsPageOper::offset2pointer(recPage, offset))) {
					mmsRecord = (MmsRecord *)tmpByte;
					assert(mmsRecord->m_valid);
					if (mmsRecord->m_updateBitmap) {
						assert(mmsRecord->m_dirtyBitmap);
						writeMmsUpdateLog(session, mmsRecord, mmsRecord->m_updateBitmap);
						mmsRecord->m_updateBitmap = 0;
					}
					offset = mmsRecord->m_lruNext;
				}
				MmsPageOper::unlockRecPage(session, m_mms, recPage);
			}
			m_recPageArray->clear();
		}
	}
	assert(m_inLogFlush.get());
	m_inLogFlush.set(0);
}

/** 
 * 获取MMS表统计信息
 *
 * @return 统计结果
 */
const MmsTableStatus& MmsTable::getStatus() {
	m_status.m_dirtyRecords = (u64)m_numDirtyRecords.get();
	return m_status;	
}

/** 
 * 获取MMS表锁
 *
 * @return MMS表锁
 */
const LURWLock& MmsTable::getTableLock() {
	return m_mmsTblLock;
}

/** 
 * 获取分区个数
 *
 * return 分区个数
 */
uint MmsTable::getPartitionNumber() {
	return m_ridNrZone;
}

/** 
 * 获取RID哈希冲突状态
 *
 * @param partition RID哈希表分区号
 * @param avgConflictLen OUT，冲突链表平均长度
 * @param maxConflictLen OUT，冲突链表最大长度
 */
void MmsTable::getRidHashConflictStatus(uint partition, double *avgConflictLen, size_t *maxConflictLen) {
	assert(partition < (uint)m_ridNrZone);
	m_ridMaps[partition]->getConflictStatus(avgConflictLen, maxConflictLen);
}

/** 提取子记录内容
 *
 * @param mmsRecord MMS记录项
 * @param extractor 子记录提取器
 * @param subRecord INOUT 子记录
 */
void MmsTable::getSubRecord(const MmsRecord *mmsRecord, SubrecExtractor *extractor, SubRecord *subRecord) {
	PROFILE(PI_MmsTable_getSubRecord);

	ftrace(ts.mms, tout << mmsRecord << extractor << subRecord);

	Record rec(RID_READ((byte *)mmsRecord->m_rid), getMmsRecFormat(m_tableDef, mmsRecord), (byte *)mmsRecord + sizeof(MmsRecord), 
		mmsRecord->m_size);
	extractor->extract(&rec, subRecord);
}

/**
 * 获取子记录内容
 *
 * @param session 会话
 * @param mmsRecord MMS记录项
 * @param subRecord INOUT 子记录，一定为REC_REDUNDANT格式
 */
void MmsTable::getSubRecord(Session *session, const MmsRecord *mmsRecord, SubRecord *subRecord) {
	assert(REC_REDUNDANT == subRecord->m_format);
	PROFILE(PI_MmsTable_getSubRecord);

	ftrace(ts.mms, tout << mmsRecord << subRecord);

	Record record(RID_READ((byte *)mmsRecord->m_rid), getMmsRecFormat(m_tableDef, mmsRecord), (byte *)mmsRecord + sizeof(MmsRecord), mmsRecord->m_size);
	
	if (record.m_format == REC_COMPRESSED) {//如果记录是压缩格式的
		assert(m_cprsRcdExtractor != NULL);//此时字典一定可用的，不然不可能以压缩格式存在MMS中
		RecordOper::extractSubRecordCompressedR(session->getMemoryContext(), m_cprsRcdExtractor, m_tableDef, &record, subRecord);
	} else {
		if (REC_FIXLEN == record.m_format)
			RecordOper::extractSubRecordFR(m_tableDef, &record, subRecord);
		else {
			assert(REC_VARLEN == record.m_format);
			RecordOper::extractSubRecordVR(m_tableDef, &record, subRecord);
		}
	}
}

/** 
 * 获取记录内容
 *
 * @param mmsRecord MMS记录项
 * @param rec INOUT 记录
 * @param copyIt 是拷贝记录内容还是直接返回mmsRecord中的记录内容
 */
void MmsTable::getRecord(const MmsRecord *mmsRecord, Record *rec, bool copyIt) {
	PROFILE(PI_MmsTable_getRecord);

	ftrace(ts.mms, tout << mmsRecord);

	rec->m_format = getMmsRecFormat(m_tableDef, mmsRecord);
	rec->m_rowId = RID_READ((byte *)mmsRecord->m_rid);
	rec->m_size = mmsRecord->m_size; 
	if (copyIt)
		memcpy(rec->m_data, (byte *)mmsRecord + sizeof(MmsRecord), rec->m_size);
	else {
		assert(!rec->m_data);
		rec->m_data = (byte *)mmsRecord + sizeof(MmsRecord);
	}
}

/**
 * 返回MMS记录是否为脏
 *
 * @param mmsRecord MMS记录项
 * @return 是否为脏
 */
bool MmsTable::isDirty(const MmsRecord *mmsRecord) {
	return mmsRecord->m_dirty != 0;
}

/** 
 * 替换记录页
 *
 * @param session 会话对象，可为NULL
 * @param victimPage 被替换的记录页
 */
void MmsTable::evictMmsPage(Session *session, MmsRecPage *victimPage) {
	PROFILE(PI_MmsTable_evictMmsPage);

	ftrace(ts.mms, tout << session << victimPage);

	m_mms->m_status.m_pageVictims++;
	m_status.m_pageVictims++;
	victimPage->m_rpClass->m_status.m_pageVictims++;

	// 刷写脏记录
	MmsRecord *currRecord = MmsPageOper::getLruRecord(victimPage);
	int numRecords = 0;
	while (1) {
		numRecords++;
		if (currRecord->m_dirty)
			writeDataToDrs(session, victimPage, currRecord,true);
		if (!currRecord->m_lruNext)
			break; // 遍历结束
		currRecord = (MmsRecord *)MmsPageOper::offset2pointer(victimPage, currRecord->m_lruNext);
	}

	// 调整统计信息
	m_status.m_records -= numRecords;
	victimPage->m_rpClass->m_status.m_records -= numRecords;
	victimPage->m_rpClass->m_status.m_recordDeletes += numRecords;
	victimPage->m_rpClass->m_status.m_recordVictims += numRecords;
	m_status.m_recordDeletes += numRecords;
	m_status.m_recordVictims += numRecords;
	m_mms->m_status.m_recordVictims += numRecords;
	victimPage->m_rpClass->m_mmsTable->m_status.m_recordPages--;

	// 标记该页不可使用
	victimPage->m_numPins.set(-1);

	victimPage->m_rpClass->delRecPageFromFreelist(victimPage);
	victimPage->m_rpClass->m_oldestHeap->del(session, victimPage->m_oldestHeapIdx, victimPage);

	// 删除记录项
	currRecord = MmsPageOper::getLruRecord(victimPage);
	while (1) {
		int ridNr = getRidPartition(RID_READ(currRecord->m_rid));		
		if (!MMS_TRYRWLOCK(session->getId(), m_ridLocks[ridNr], Exclusived)) {
			MmsPageOper::mmsRWUNLock(session->getId(), &m_mmsTblLock, Exclusived);
			MmsPageOper::unlockRecPage(session, m_mms, victimPage);
			MMS_RWLOCK(session->getId(), m_ridLocks[ridNr], Exclusived);
			MMS_RWLOCK(session->getId(), &m_mmsTblLock, Exclusived);
			MMS_LOCK_REC_PAGE(session, m_mms, victimPage);
		}
		m_ridMaps[ridNr]->del(currRecord);
		MmsPageOper::mmsRWUNLock(session->getId(), m_ridLocks[ridNr], Exclusived);
		if (!currRecord->m_lruNext)
			break; // 遍历结束
		currRecord = (MmsRecord *)MmsPageOper::offset2pointer(victimPage, currRecord->m_lruNext);
	}

	// 清空记录页
	memset(victimPage, 0, Limits::PAGE_SIZE);
}

/**
 * 写脏数据到DRS
 *
 * @param session 会话对象
 * @param recPage 记录页
 * @param mmsRecord 记录项
 * @param force 强制刷到DRS中
 */
void MmsTable::writeDataToDrs(Session *session, MmsRecPage *recPage, MmsRecord *mmsRecord, bool force) {
	ftrace(ts.mms, tout << session << recPage << mmsRecord << force);

	MemoryContext *mc = session->getMemoryContext();
	McSavepoint memSave(mc);
	
	u16 cols[Limits::MAX_COL_NUM];
	u16 numCols = (u16)dirtyBm2cols(mmsRecord->m_dirtyBitmap, cols);

	Record record(RID_READ((byte *)mmsRecord->m_rid), getMmsRecFormat(m_tableDef, mmsRecord), 
		(byte *)mmsRecord + sizeof(MmsRecord), mmsRecord->m_size);

	SubRecord subRec(REC_REDUNDANT, numCols, cols, (byte *)mc->alloc(m_tableDef->m_maxRecSize), m_tableDef->m_maxRecSize, record.m_rowId);
	
	if (record.m_format == REC_FIXLEN) {
		RecordOper::extractSubRecordFR(m_tableDef, &record, &subRec);
	} else {
		assert(record.m_format == REC_VARLEN || record.m_format == REC_COMPRESSED);
		if (record.m_format == REC_VARLEN) {		
			RecordOper::extractSubRecordVR(m_tableDef, &record, &subRec);
		} else {
			RecordOper::extractSubRecordCompressedR(session->getMemoryContext(), m_cprsRcdExtractor, m_tableDef, &record, &subRec);
		}
	}

	if (!force) {
		File* srcFiles[1];
		PageType pageTypes[1];
		m_drsHeap->getFiles(srcFiles, pageTypes, sizeof(pageTypes) / sizeof(pageTypes[0]));
		assert(pageTypes[0] == PAGE_HEAP);
		if (!m_db->getPageBuffer()->hasPage(srcFiles[0], pageTypes[0], RID_GET_PAGE(record.m_rowId))) {
			nftrace(ts.mms, tout << "Write mms update log, but not write to DRS: " << &subRec);
			// buffer失配，此时只写日志不写堆, 记录保持为脏
			session->startTransaction(TXN_MMS_FLUSH_DIRTY, m_tableDef->m_id);
			writeMmsUpdateLog(session, &subRec);
			session->endTransaction(true);
			return;
		}
	}

	nftrace(ts.mms, tout << "Write dirty record to DRS: " << &subRec);
	// 使用一个事务将脏记录更新到堆中
	session->startTransaction(TXN_UPDATE_HEAP, m_tableDef->m_id);
	session->writePreUpdateHeapLog(m_tableDef, &subRec);
	session->setTxnDurableLsn(session->getLastLsn());
	if (!m_mms->m_duringRedo)
		NTSE_ASSERT(m_drsHeap->update(session, record.m_rowId, &record));
	else
		NTSE_ASSERT(m_drsHeap->update(session, record.m_rowId, &subRec));
	session->endTransaction(true);

	if (m_binlogCallback != NULL && mmsRecord->m_updateBitmap) {
		u16 *updCachedCols = (u16 *)mc->alloc(sizeof(u16) * 32);
		u16 numUpdCachedCols = (u16)updBm2cols(mmsRecord->m_updateBitmap, updCachedCols);
		writeCachedBinlog(session, ColList(numUpdCachedCols, updCachedCols), mmsRecord);
	}
	
	MmsPageOper::setDirty(recPage, mmsRecord, false);
}

/**
 * 执行写MMS日志操作 (注意：调用者负责清理MemoryContext)
 *
 * @param session 会话对象
 * @param mc 内存上下文
 * @param subRecord 子记录 变长格式
 */
void MmsTable::doMmsLog(Session *session, MemoryContext *mc, const SubRecord *subRecord) {
	ftrace(ts.mms, tout << session << mc << subRecord);
	assert(REC_VARLEN == subRecord->m_format);
	size_t size = sizeof(RowId) + sizeof(u16) * (1 + subRecord->m_numCols) + sizeof(uint) + subRecord->m_size;
	byte *logData = (byte *)mc->alloc(size);
	Stream s(logData, size);

	try {
		// 写rid
		s.write(subRecord->m_rowId);
		// 写属性长度
		s.write((u16)subRecord->m_numCols);
		// 写属性列表
		s.write((const byte *)subRecord->m_columns, sizeof(u16) * subRecord->m_numCols);
		// 写子记录长度
		s.write(subRecord->m_size);
		// 写子记录内容
		s.write(subRecord->m_data, subRecord->m_size);
	} catch(NtseException &) {
		NTSE_ASSERT(false);
	}

	session->writeLog(LOG_MMS_UPDATE, m_tableDef->m_id, logData, size);
}

/** 
 * 写MMS更新日志 (在间隔刷新阶段使用）
 * 
 * @param session 会话对象
 * @param mmsRecord 记录项
 * @param updBitmap 更新位图
 */
void MmsTable::writeMmsUpdateLog(Session *session, MmsRecord *mmsRecord, u32 updBitmap) {
	assert(updBitmap);
	
	MemoryContext *mc = session->getMemoryContext();
	McSavepoint msp(mc);
	
	u16 *cols = (u16 *)mc->alloc(sizeof(u16) * 32);
	u16 numCols = (u16)updBm2cols(updBitmap, cols);
	u16 dataSize = m_tableDef->m_maxRecSize;
	RowId rid = RID_READ((byte *)mmsRecord->m_rid);
	
	session->startTransaction(TXN_UPDATE, m_tableDef->m_id);

	SubRecord subRecord(REC_REDUNDANT, numCols, cols, (byte *)mc->alloc(dataSize), dataSize, rid);
	getSubRecord(session, mmsRecord, &subRecord);

	size_t preUpdateLogSize;
	byte *preUpdateLog = NULL;
	try {
		preUpdateLog = session->constructPreUpdateLog(m_tableDef, NULL, &subRecord, false, NULL, &preUpdateLogSize);
	} catch (NtseException &) {assert(false);}
		
	session->writePreUpdateLog(m_tableDef, preUpdateLog, preUpdateLogSize);
	session->setTxnDurableLsn(session->getLastLsn());

	SubRecord subVarRecord(REC_VARLEN, numCols, cols, (byte *)mc->alloc(dataSize), dataSize, subRecord.m_rowId);
	RecordOper::convertSubRecordRV(m_tableDef, &subRecord, &subVarRecord);
	
	doMmsLog(session, mc, &subVarRecord);

	session->endTransaction(true);

	if (NULL != m_binlogCallback)
		writeCachedBinlog(session, ColList(numCols, cols), mmsRecord);
}

/**
 * 写MMS缓存更新binlog
 *
 * @param session 会话
 * @param updCachedCols 被缓存更新的属性列表
 * @param mmsRecord MMS记录内容
 */
void MmsTable::writeCachedBinlog(Session *session, const ColList &updCachedCols, MmsRecord *mmsRecord) {
	MemoryContext *mc = session->getMemoryContext();
	McSavepoint msp(mc);
	
	ColList binlogCols = updCachedCols.merge(mc, ColList(m_tableDef->m_pkey->m_numCols, m_tableDef->m_pkey->m_columns));
	Record record(RID_READ((byte *)mmsRecord->m_rid), getMmsRecFormat(m_tableDef, mmsRecord), (byte *)mmsRecord + sizeof(MmsRecord), mmsRecord->m_size);
	SubRecord binlogSubRecord(REC_REDUNDANT, binlogCols.m_size, binlogCols.m_cols, (byte *)mc->alloc(m_tableDef->m_maxRecSize), m_tableDef->m_maxRecSize, record.m_rowId);
	if (m_tableDef->m_recFormat == REC_FIXLEN)
		RecordOper::extractSubRecordFR(m_tableDef, &record, &binlogSubRecord);
	else {
		assert(record.m_format == REC_VARLEN || record.m_format == REC_COMPRESSED);
		if (record.m_format == REC_COMPRESSED) {
			assert(m_cprsRcdExtractor);
			RecordOper::extractSubRecordCompressedR(session->getMemoryContext(), m_cprsRcdExtractor, m_tableDef, &record, &binlogSubRecord);
		} else {
			RecordOper::extractSubRecordVR(m_tableDef, &record, &binlogSubRecord);
		}
	}
	m_binlogCallback->m_writer(session, &binlogSubRecord, m_binlogCallback->m_data);
}

/** 
 * 写MMS更新日志 (在MmsTable::update中不启用updateCache时使用)
 * 
 * @param session 会话对象
 * @param subRecord 子记录 REC_REDUNDANT
 */
void MmsTable::writeMmsUpdateLog(Session *session, const SubRecord *subRecord) {
	assert(subRecord->m_format == REC_REDUNDANT);
	MemoryContext *mc = session->getMemoryContext();
	McSavepoint msp(mc);
	
	SubRecord subVarRecord(REC_VARLEN, subRecord->m_numCols, subRecord->m_columns, (byte *)mc->alloc(subRecord->m_size),
		subRecord->m_size, subRecord->m_rowId);
	RecordOper::convertSubRecordRV(m_tableDef, subRecord, &subVarRecord);
	
	doMmsLog(session, mc, &subVarRecord);
}

/** 
 * 写MMS更新日志 (在MmsTable::update中启用updateCache时使用)
 *
 * @param session 会话对象
 * @param mmsRecord 记录项
 * @param subRecord 子记录 REC_REDUNDANT
 * @param updBitmap 更新位图
 */
void MmsTable::writeMmsUpdateLog(Session *session, MmsRecord *mmsRecord, const SubRecord *subRecord, u32 updBitmap) {
	assert(REC_REDUNDANT == subRecord->m_format);

	if (!updBitmap) {
		writeMmsUpdateLog(session, subRecord);
		return;
	}
	
	MemoryContext *mc = session->getMemoryContext();
	McSavepoint msp(mc);
	
	// 合并所有更新属性字段
	int maxNumCols = subRecord->m_numCols + 32;  // 最大可能长度为子记录字段加32
	u16 *cols = (u16 *)mc->alloc(sizeof(u16) * maxNumCols);
	u16 numCols = (u16)mergeCols(subRecord, updBitmap, cols, maxNumCols);
	u16 size = m_tableDef->m_maxRecSize;
	RowId rid = RID_READ((byte *)mmsRecord->m_rid);
	
	SubRecord subNewRecord(REC_REDUNDANT, numCols, cols, (byte *)mc->alloc(size), size, rid);
	getSubRecord(session, mmsRecord, &subNewRecord);

	SubRecord subVarRecord(REC_VARLEN, numCols, cols, (byte *)mc->alloc(size), size, subNewRecord.m_rowId);
	RecordOper::convertSubRecordRV(m_tableDef, &subNewRecord, &subVarRecord);

	doMmsLog(session, mc, &subVarRecord);

	if (NULL != m_binlogCallback) {
		assert(updBitmap);
		u16 *updCachedCols = (u16 *)mc->alloc(sizeof(u16) * 32);
		u16 numUpdCachedCols = (u16)updBm2cols(updBitmap, updCachedCols);
		writeCachedBinlog(session, ColList(numUpdCachedCols, updCachedCols), mmsRecord);
	}
}

/** 
 * 根据脏记录位图，构造属性列
 * 
 * @param dirtyBitmap 脏记录位图
 * @param cols INOUT 属性列
 * @return 属性列个数
 */
int MmsTable::dirtyBm2cols(u32 dirtyBitmap, u16 *cols) {
	int numCols = 0;
	u32 tmpBitmap = dirtyBitmap;
	u16 i = 0, j = 0;

	// 前31个字段
	for (i = 0; i < 31; i++) {
		if ((tmpBitmap & 1) == 1) {
			cols[j++] = i;
			numCols++;
		}
		tmpBitmap >>= 1;
	}

	if ((tmpBitmap & 1) == 1)
		numCols += m_tableDef->m_numCols - 31;
	for (; j < numCols; j++)
		cols[j] = i++;

	return numCols;
}

/** 
 * 根据更新位图, 构造属性列
 * 
 * @param updBitmap 更新位图
 * @param cols INOUT 属性列
 * @return 属性列个数
 */
int MmsTable::updBm2cols(u32 updBitmap, u16 *cols) {
	int numCols = 0;

	for (int i = 0; i < 32; i++) {
		if ((updBitmap & 1) == 1) 
			cols[numCols++] = m_updateCacheCols[i];
		updBitmap >>= 1;
	}
	assert(numCols <= m_updateCacheNumCols);
	return numCols;
}

/** 
 * 根据更新属性列，构造当前更新位图和脏记录位图
 *
 * @param subRecord 子记录
 * @param updBitmap	INOUT 更新位图
 * @param dirtyBitmap INOUT 脏记录位图
 * @return 是否存在非更新缓存字段
 */
bool MmsTable::cols2bitmap(const SubRecord *subRecord, u32 *updBitmap, u32 *dirtyBitmap) {
	u16 *cols = subRecord->m_columns;
	uint numCols = subRecord->m_numCols;
	bool notCached = false;
	byte offset;

	*updBitmap = 0;
	*dirtyBitmap = 0;

	for (uint i = 0; i < numCols; i++) {
		// 计算脏记录位图
		if (cols[i] < 31)
			*dirtyBitmap |= ((u32)1 << cols[i]);
		else
			*dirtyBitmap |= ((u32)1 << 31);
		// 计算更新位图
		offset = m_updateBitmapOffsets[cols[i]];
		if (offset != (byte)-1) {
			assert (offset < 32);
			*updBitmap |= ((u32)1 << offset);
		} else 
			notCached = true;
	}
	return notCached;
}

/** 
 * 合并更新字段
 * 
 * @param subRecord 子记录
 * @param updBitmap 更新位图
 * @param cols 属性列
 * @param numCols 属性列最大长度
 * @return 属性列长度
 */
int MmsTable::mergeCols(const SubRecord *subRecord, u32 updBitmap, u16 *cols, int numCols) {
	int i = 0;
	int j = 0; 
	int k = 0;
	int srNumCols = subRecord->m_numCols;

	while (i < srNumCols && j < 32) {
		if ((updBitmap & 1) == 0) {
			updBitmap >>= 1;
			j++;
		} else if (subRecord->m_columns[i] > m_updateCacheCols[j]) {
			cols[k++] = m_updateCacheCols[j++];
			updBitmap >>= 1;
		} else if (subRecord->m_columns[i] < m_updateCacheCols[j]) {
			cols[k++] = subRecord->m_columns[i++];
		} else {
			cols[k++] = subRecord->m_columns[i++];
			j++;
			updBitmap >>= 1;
		}
	}

	while (i < srNumCols)
		cols[k++] = subRecord->m_columns[i++];

	for (; j < 32; j++) {
		if ((updBitmap & 1) == 1)
			cols[k++] = m_updateCacheCols[j];
		updBitmap >>= 1;
	}

	assert (k <= numCols);
	UNREFERENCED_PARAMETER(numCols);

	return k;
}

/** 
 * 合并两列
 *
 * @param src1Cols 一列
 * @param num1Cols 一列长度
 * @param src2Cols 二列
 * @param num2Cols 二列长度
 * @param dstCols 合并列
 * @return 合并列长度
 */
int MmsTable::mergeCols(u16 *src1Cols, int num1Cols, u16 *src2Cols, int num2Cols, u16 *dstCols) {
	int i = 0, k = 0;
	int numDst = 0;

	while(i < num1Cols && k < num2Cols) {
		if (src1Cols[i] < src2Cols[k])
			dstCols[numDst++] = src1Cols[i++];
		else
			dstCols[numDst++] = src2Cols[k++];
	}

	for (; i < num1Cols; i++)
		dstCols[numDst++] = src1Cols[i++];
	
	for (; k < num2Cols; k++)
		dstCols[numDst++] = src2Cols[k++];
	
	return numDst;
}

/**
 * 获得mms记录的格式
 * @param tableDef 表定义
 * @param mmsRecord mms记录
 * @return mms记录的格式
 */
inline RecFormat MmsTable::getMmsRecFormat(const TableDef *tableDef, const MmsRecord *mmsRecord) {
	if (mmsRecord->m_compressed > 0) {
		return REC_COMPRESSED;
	} else {
		return tableDef->m_recFormat == REC_FIXLEN ? REC_FIXLEN : REC_VARLEN;
	}
}

/** 
 * 根据MmsUpdate日志进行REDO恢复
 * 
 * @param session 会话对象
 * @param log 日志内容
 * @param size 日志大小
 */
void MmsTable::redoUpdate(Session *session, const byte *log, uint size) {
	ftrace(ts.recv, tout << session << (void *)log << size);

	Stream s((byte *)log, size);
	MemoryContext *mc = session->getMemoryContext();
	u64 sp = mc->setSavepoint();
	RowId rid;
	SubRecord subRecord;
	MmsRecord *mmsRecord = NULL;
	Record	record;
	bool needInsert = true;
	u32 dirtyBitmap = 0;

	// 读取日志项，重构子记录
	subRecord.m_format = REC_VARLEN;          // 日志记录为变长格式
	s.read(&rid)->read(&subRecord.m_numCols);
	subRecord.m_rowId = rid;
	subRecord.m_columns = (u16 *)mc->calloc(sizeof(u16) * subRecord.m_numCols);
	s.readBytes((byte *)subRecord.m_columns, sizeof(u16) * subRecord.m_numCols);
	s.read(&subRecord.m_size);
	subRecord.m_data = (byte *)mc->calloc(subRecord.m_size);
	s.readBytes(subRecord.m_data, subRecord.m_size);
	nftrace(ts.recv, tout << "rid: " << rid << ", rec: " << &subRecord);

	// 转成冗余格式
	SubRecord varSubRecord;
		
	varSubRecord.m_rowId = subRecord.m_rowId;
	varSubRecord.m_numCols = subRecord.m_numCols;
	varSubRecord.m_columns = subRecord.m_columns;
	varSubRecord.m_format = subRecord.m_format;
	varSubRecord.m_data = subRecord.m_data;
	varSubRecord.m_size = subRecord.m_size;

	subRecord.m_format = REC_REDUNDANT;
	subRecord.m_size = m_tableDef->m_maxRecSize;
	subRecord.m_data = (byte *)mc->calloc(subRecord.m_size);
	RecordOper::convertSubRecordVR(m_tableDef, &varSubRecord, &subRecord);

	// 获取原记录
	record.m_rowId = rid;
	record.m_format = m_tableDef->m_recFormat;

	mmsRecord = getByRid(session, rid, false, NULL, None);
	
	if (mmsRecord) {
		nftrace(ts.recv, tout << "already exist in mms");
		if (REC_FIXLEN == record.m_format) { // 定长记录，在原缓存记录项基础上修改
			assert(!mmsRecord->m_compressed);
			MMS_RWLOCK(session->getId(), &m_mmsTblLock, Exclusived);
			record.m_size = mmsRecord->m_size;
			record.m_data = (byte *)mmsRecord + sizeof(MmsRecord);
			needInsert = false;
		} else { // 变长记录或压缩记录，先删除，再添加
			record.m_size = mmsRecord->m_size;
			record.m_data = (byte *)mc->calloc(record.m_size);
			record.m_format = mmsRecord->m_compressed > 0 ? REC_COMPRESSED : REC_VARLEN;
			memcpy(record.m_data, (byte *)mmsRecord + sizeof(MmsRecord), record.m_size);
			dirtyBitmap = mmsRecord->m_dirtyBitmap;
			del(session, mmsRecord);
		}
	} else {
		// 恢复阶段，DRS堆内容不可读，构造全零记录
		record.m_size = m_tableDef->m_maxRecSize;
		record.m_data = (byte *)mc->calloc(record.m_size);
		//构造的全零记录一定是定长格式或变长格式
		RecordOper::initEmptyRecord(&record, m_tableDef, rid, m_tableDef->m_recFormat == REC_FIXLEN ? REC_FIXLEN : REC_VARLEN);
	}
	nftrace(ts.recv, tout << "Old dirtyBitmap: " << dirtyBitmap);

	assert(record.m_format != REC_COMPRESSED);
	if (REC_FIXLEN == m_tableDef->m_recFormat) {
		RecordOper::updateRecordFR(m_tableDef, &record, &subRecord);
	} else {
		uint updSize;
		updSize = RecordOper::getUpdateSizeVR(m_tableDef, &record, &subRecord);
		if (updSize <= record.m_size)
			RecordOper::updateRecordVRInPlace(m_tableDef, &record, &subRecord, record.m_size);
		else {
			byte *buf = (byte *)mc->calloc(updSize);
			RecordOper::updateRecordVR(m_tableDef, &record, &subRecord, buf);
			record.m_data = buf;
			record.m_size = updSize;
		}
	}

	// 生成更新位图
	u32 currDirtyBitmap, currUpdBitmap;

	cols2bitmap(&subRecord, &currUpdBitmap, &currDirtyBitmap);

	// MMS Recover
	nftrace(ts.recv, tout << "needInsert: " << needInsert);
	if (needInsert) {
		// 加MMS表锁
		int ridNr = getRidPartition(record.m_rowId);
		bool tryAgain;
		MMS_RWLOCK(session->getId(), m_ridLocks[ridNr], Exclusived);
		mmsRecord = put(session, &record, currDirtyBitmap | dirtyBitmap, ridNr, &tryAgain);
	} else {
		mmsRecord->m_dirtyBitmap |= currDirtyBitmap;
		MmsPageOper::mmsRWUNLock(session->getId(), &m_mmsTblLock, Exclusived);
	}
	nftrace(ts.recv, tout << "New dirtyBitmap: " << mmsRecord->m_dirtyBitmap);

	// 释放pin
	unpinRecord(session, mmsRecord);

	mc->resetToSavepoint(sp);
}

/** 
 * 获取所有类级别
 *
 * @param nrRPClass OUT MmsRPClass数组长度
 * @return MmsRPClass数组指针
 */
MmsRPClass** MmsTable::getMmsRPClass(int *nrRPClasses) {
	*nrRPClasses = m_mms->m_nrClasses;
	return m_rpClasses;
}

/** 
 * 强制刷写更新日志
 *
 * @param session 会话
 */
void MmsTable::flushUpdateLog(Session *session) {
	while (!m_inLogFlush.compareAndSwap(0, 1))
		Thread::msleep(100);
	flushLog(session, true);
}

/************************************************************************/
/*                  　　　MMS全局类实现                                 */
/************************************************************************/

/** 
 * 构造函数
 *
 * @param db 数据库对象
 * @param targetSize 目标大小，单位为页数
 * @param pagePool 所属内存页池
 * @param needRedo 需要执行REDO
 * @param replaceRatio 替换率（当已有页面数/最多页面数超过替换率时，触发页替换)
 * @param intervalReplacer 替换线程间隔执行时间(秒为单位)
 */
Mms::Mms(Database *db, uint targetSize, PagePool *pagePool, bool needRedo, float replaceRatio, int intervalReplacer, float minFPageRatio, float maxFPageRatio) : PagePoolUser(targetSize, pagePool),
	m_mmsLock(db->getConfig()->m_maxSessions + 1, "Mms::lock", __FILE__, __LINE__) {
	// 初始化普通变量
	m_db = db;
	m_fspConn = NULL;
	m_pagePool = pagePool;
	m_numPagesInUse.set(0);
	m_duringRedo = needRedo;
	m_maxNrDirtyRecs = 512 * 1024; // 默认512K
	m_replaceRatio = replaceRatio;
	m_pgRpcRatio = 1;  // 默认为1
	m_preRecordQueries = 0;
	m_minFPageRatio = minFPageRatio;
	m_maxFPageRatio = maxFPageRatio;

	// 初始化统计信息
	memset(&m_status, 0, sizeof(MmsStatus));

	// 初始化类级别相关信息
	u16 size = Limits::PAGE_SIZE / (1 << 8);
	float fSize;
	float factor = MMS_RPC_GROWTH_FACTOR;
	Array<u16> tmpArray;
	u16 oldNrSlots = Limits::PAGE_SIZE;
	u16 nrSlots;

	while (size <= MMS_MAX_RECORD_SIZE) {
		nrSlots = Limits::PAGE_SIZE / size;
		if (nrSlots >= oldNrSlots) break;
		oldNrSlots = nrSlots;
		tmpArray.push(size);
		// 计算下一个size
		fSize = (float)size * factor;
		size = MMS_ALIGN_8((u16)fSize);
	}

	for (nrSlots = oldNrSlots; nrSlots > 0; nrSlots--) {
		if ((Limits::PAGE_SIZE - MMS_REC_PAGE_HEAD_SIZE) % nrSlots)
			size = (Limits::PAGE_SIZE - MMS_REC_PAGE_HEAD_SIZE) / nrSlots + 1;
		else
			size = (Limits::PAGE_SIZE - MMS_REC_PAGE_HEAD_SIZE) / nrSlots;
		tmpArray.push(size);
	}

	assert (tmpArray.getSize() <= (u8)-1);
	m_nrClasses = (u8)tmpArray.getSize();
	m_pageClassSize = new u16[m_nrClasses];
	m_pageClassNrSlots = new u16[m_nrClasses];

	for (int i = 0; i < m_nrClasses; i++) {
		m_pageClassSize[i] = tmpArray[i];
		m_pageClassNrSlots[i] = MMS_MAX_RECORD_SIZE / m_pageClassSize[i];
	}

	m_size2class = new u16[MMS_MAX_RECORD_SIZE];
	for (u16 i = 0, j = 0; i < MMS_MAX_RECORD_SIZE; i++) {
		if (i > m_pageClassSize[j]) j++;
		m_size2class[i] = j;
	}

	// 初始化临时区变量
	// m_recPageArray = new Array<MmsRecPage *>();
	// m_dirtyRecArray = new std::vector<MmsRecPair>();

	m_replacer = new MmsReplacer(this, m_db, intervalReplacer * 1000);
	m_replacer->start();
}

/** 关闭后台替换线程 */
void Mms::stopReplacer() {
	if (m_replacer) {
		m_replacer->stop();
		m_replacer->join();
		delete m_replacer;
		m_replacer = NULL;
	}
}

/** 
 * 关闭全局MMS，并释放资源
 */
void Mms::close() {
	stopReplacer();
	MMS_RWLOCK(0, &m_mmsLock, Exclusived);

	delete [] m_size2class;
	m_size2class = NULL;

	delete [] m_pageClassNrSlots;
	m_pageClassNrSlots = NULL;

	delete [] m_pageClassSize;
	m_pageClassSize = NULL;

	// 删除链表的所有元素
	DLink<MmsTable *> *curr;
	while(!m_mmsTableList.isEmpty()) {
		curr = m_mmsTableList.removeLast();
		delete curr;
	}

	if (m_fspConn) {
		m_db->freeConnection(m_fspConn);
	}

	MmsPageOper::mmsRWUNLock(0, &m_mmsLock, Exclusived);
}

/**
 * 向MMS全局对象注册一个MmsTable对象
 *
 * @param session 会话
 * @param mmsTable MmsTable对象
 */
void Mms::registerMmsTable(Session *session, MmsTable *mmsTable) {
	assert(mmsTable);

	MMS_RWLOCK(session->getId(), &m_mmsLock, Exclusived); 
	m_mmsTableList.addLast(new DLink<MmsTable *>(mmsTable));
	MmsPageOper::mmsRWUNLock(session->getId(), &m_mmsLock, Exclusived);
}

/** 
 * 计算MMS表的DELTA查询数
 *
 * @param session 会话
 */
void Mms::computeDeltaQueries(Session *session) {
	MmsTable *currTable;
	u64 tmpRecordQueries;
	u64 deltaQueries;
	u64 deltaTableQueries;

	MMS_RWLOCK(session->getId(), &m_mmsLock, Exclusived);
	tmpRecordQueries = m_status.m_recordQueries;
	deltaQueries = tmpRecordQueries - m_preRecordQueries;
	m_preRecordQueries = tmpRecordQueries;

	uint size = m_mmsTableList.getSize();
	DLink<MmsTable *> *item = m_mmsTableList.getHeader()->getNext();
	for (uint i = 0; i < size; i++, item = item->getNext()) {
		currTable = item->get();
		tmpRecordQueries = currTable->m_status.m_recordQueries;
		deltaTableQueries = tmpRecordQueries - currTable->m_preRecordQueries;
		if (deltaTableQueries <= 0) deltaTableQueries = 1;
		currTable->m_deltaRecordQueries = (float)deltaTableQueries / (float)deltaQueries; 
		currTable->m_preRecordQueries = tmpRecordQueries;
	}
	MmsPageOper::mmsRWUNLock(session->getId(), &m_mmsLock, Exclusived);
}

/**
 * 在MMS全局注销一个MmsTable对象
 *
 * @param session 会话
 * @param mmsTable MmsTable对象
 */
void Mms::unregisterMmsTable(Session *session, MmsTable *mmsTable) {
	assert(mmsTable);

	DLink<MmsTable *> *curr;
	MMS_RWLOCK(session->getId(), &m_mmsLock, Exclusived);
	for (curr = m_mmsTableList.getHeader()->getNext();
		curr != m_mmsTableList.getHeader(); curr = curr->getNext()) {
		if (mmsTable == curr->get()) {
			curr->unLink();
			delete curr;
			break;
		}
	}
	MmsPageOper::mmsRWUNLock(session->getId(), &m_mmsLock, Exclusived);
}

/** 
 * 设置临时区最大脏记录个数
 *
 * @param threshold 脏记录个数
 */
void Mms::setMaxNrDirtyRecs(int threshold) {
	m_maxNrDirtyRecs = threshold;
}

/** 
 * 获取当前临时区最大脏记录个数
 *
 * @return 临时区最大脏记录个数
 */
int Mms::getMaxNrDirtyRecs() {
	return m_maxNrDirtyRecs;
}

/** 
 * 获取页替换率参数值
 *
 * @return 页替换率参数值
 */
float Mms::getPageReplaceRatio() {
	return m_pgRpcRatio;
}

/** 
 * 设置页替换率参数值，参数值必须在[0, 1]之间
 *
 * @param ratio 参数值
 * @return 设置是否成功
 */
bool Mms::setPageReplaceRatio(float ratio) {
	if (ratio < 0 || ratio > 1) return false;
	m_pgRpcRatio = ratio;
	return true;
}

/**
 * 在MMS全局释放页
 * 注：MMS不保证每次调用该函数都释放numPages个缓存页
 *
 * @param userId 关联的用户ID
 * @param numPages 待释放的页个数
 * @return 实际释放的页数
 */
uint Mms::freeSomePages(u16 userId, uint numPages) {
	ftrace(ts.mms, tout << userId << numPages);

	uint count = PagePoolUser::freeSomeFreePages(userId, numPages);
	if (count >= numPages) return count;

	MMS_RWLOCK(userId, &m_mmsLock, Exclusived);
	if (!m_fspConn) {
		m_fspConn = m_db->getConnection(true, __FUNC__);
	}
	MmsPageOper::mmsRWUNLock(userId, &m_mmsLock, Exclusived);

	Session *session = m_db->getSessionManager()->allocSession(__FUNC__, m_fspConn, 1000);
	if (!session) {
		return count;
	}
	while (count < numPages) {
		void *page = allocMmsPage(session, true);
		if (!page) break;
		freePage(session->getId(), page);
		++count;
	}
	m_db->getSessionManager()->freeSession(session);
	return count;
}

/** 获取MMS全局统计信息
 *
 * @return 统计结果
 */
const MmsStatus& Mms::getStatus() {
	m_status.m_recordPages = (u64)m_numPagesInUse.get();
	m_status.m_occupiedPages = getCurrentSize();
	return m_status;
}

/** 打印MMS全局状态
 * @param out 输出流
 */
void Mms::printStatus(ostream &out) {
	out << "== mms status =================================================================" << endl;
	out << "mms_size: " << m_numPagesInUse.get() << endl;
	out << "mms_queries: " << m_status.m_recordQueries << endl;
	out << "mms_query_hits: " << m_status.m_recordQueryHits << endl;
	out << "mms_inserts: " << m_status.m_recordInserts << endl;
	out << "mms_deletes: " << m_status.m_recordDeletes << endl;
	out << "mms_updates: " << m_status.m_recordUpdates << endl;
	out << "mms_rec_replaces: " << m_status.m_recordVictims << endl;
	out << "mms_page_replaces: " << m_status.m_pageVictims << endl;
}

/** 
 * 结束REDO
 */
void Mms::endRedo() {
	assert(m_duringRedo);
	
	m_duringRedo = false;
}

/** 
 * 分配缓存页
 * @pre 不允许当前线程加任何记录页锁
 * 
 * @param session 会话对象
 * @param external 外部调用（非内部替换线程调用)
 * @return 空闲页, 可能为NULL
 */
void* Mms::allocMmsPage(Session *session, bool external){
	bool needAssigned;
	u32 ts_min, ts_max;
	u8	numRecs = 1;
	MmsTable *victimTable = NULL;
	MmsRecPage *victimPage = NULL;
	MmsRecPage *recPage;

	SYNCHERE(SP_MMS_ALLOC_PAGE);

	if (!external) { // 首先从PagePool申请页
		if (m_duringRedo)
			recPage = (MmsRecPage *)allocPageForce(session->getId(), PAGE_MMS_PAGE, NULL);
		else
			recPage = (MmsRecPage *)allocPage(session->getId(), PAGE_MMS_PAGE, NULL, 50); // 最长等待50毫秒
		if (recPage)
			return recPage;
	}

	if (m_duringRedo) { // 在恢复阶段
		if (external)
			return NULL;    // 页池申请引起的页替换,不执行
		NTSE_ASSERT(false);
	}
	needAssigned = true;
	MMS_RWLOCK(session->getId(), &m_mmsLock, Shared);

	DLink<MmsTable *> *item = m_mmsTableList.getHeader()->getNext();
	uint size = m_mmsTableList.getSize();
	MmsTable *currTable;

	for (uint i = 0; i < size; i++, item = item->getNext()) {
		currTable = item->get();
		MMS_TEST_GET_TABLE(this, currTable);
		SYNCHERE(SP_MMS_AMP_GET_TABLE);
		// 尝试加MMS锁
		if (!MMS_TRYRWLOCK(session->getId(), &currTable->m_mmsTblLock, Exclusived)) {
			SYNCHERE(SP_MMS_AMP_GET_TABLE_END);
			continue;
		}

		assert(currTable->m_freqHeap); // 防止出现MMS表已被close情况
		recPage = currTable->m_freqHeap->getPage(0);

		if (!recPage) {
			MmsPageOper::mmsRWUNLock(session->getId(), &currTable->m_mmsTblLock, Exclusived);
			continue;
		}

		MMS_TEST_GET_PAGE(this, recPage);
		SYNCHERE(SP_MMS_AMP_GET_PAGE);

		if (!MMS_TRYLOCK_REC_PAGE(session, this, recPage)) {
			MmsPageOper::mmsRWUNLock(session->getId(), &currTable->m_mmsTblLock, Exclusived);
			SYNCHERE(SP_MMS_AMP_GET_PAGE_END);
			continue;
		}

		MMS_TEST_GET_PAGE(this, recPage);
		SYNCHERE(SP_MMS_AMP_PIN_PAGE);

		if (recPage->m_numPins.get() != 0) {
			MmsPageOper::unlockRecPage(session, this, recPage);
			MmsPageOper::mmsRWUNLock(session->getId(), &currTable->m_mmsTblLock, Exclusived);
			SYNCHERE(SP_MMS_AMP_PIN_PAGE_END);
			continue;
		}

		if (MmsPageOper::isMinFPage(recPage, &ts_min, &ts_max, &numRecs, needAssigned, victimTable)) {
			if (victimTable) {
				MmsPageOper::unlockRecPage(session, this, victimPage);
				MmsPageOper::mmsRWUNLock(session->getId(), &victimTable->m_mmsTblLock, Exclusived);
			}
			//　更改惩罚项
			victimTable = currTable;
			victimPage = recPage;
		} else {
			MmsPageOper::unlockRecPage(session, this, recPage);
			MmsPageOper::mmsRWUNLock(session->getId(), &currTable->m_mmsTblLock, Exclusived);
		}

		needAssigned = false;
	}
	MmsPageOper::mmsRWUNLock(session->getId(), &m_mmsLock, Shared);

	// 找不到惩罚项
	if (!victimTable) return NULL;

	SYNCHERE(SP_MMS_ALLOC_PAGE_END);

	// 缓存页替换
	victimTable->m_existPin.increment();
	victimTable->evictMmsPage(session, victimPage);
	victimTable->m_existPin.decrement();
	MmsPageOper::mmsRWUNLock(session->getId(), &victimTable->m_mmsTblLock, Exclusived);
	m_numPagesInUse.decrement();
	return (void *)victimPage;
}

/** 
 * 分配记录项
 *
 * @param session 会话对象
 * @param rpClass 所属RPClass
 * @param ridNr 已加RID分区锁所属分区
 * @param locked INOUT 是否加着RID分区锁
 * @return 记录项，分配不成功返回为NULL
 */
MmsRecPage* MmsTable::allocMmsRecord(Session *session, MmsRPClass *rpClass, int ridNr, bool *locked){
	MmsRecPage *recPage;
	MmsRecord *mmsRecord;
	
	*locked = true;
	if (m_mms->m_duringRedo)
		recPage = (MmsRecPage *) m_mms->allocPageForce(session->getId(), PAGE_MMS_PAGE, NULL);
	else if (m_status.m_records < m_maxRecordCount)  
		recPage = (MmsRecPage *) m_mms->allocPage(session->getId(), PAGE_MMS_PAGE, NULL, 0); // 最长等待50毫秒
	else // 注：当MMS表已有记录数超过阈值时，停止申请额外记录页
		recPage = NULL;
	if (recPage) {
		m_mms->m_numPagesInUse.increment();
		MmsPageOper::initRecPage(rpClass, recPage);
		recPage->m_numPins.increment();
		rpClass->addRecPageToFreeList(recPage);
		m_status.m_recordPages++;
		return recPage;
	}

	if (m_mms->m_duringRedo)
		 NTSE_ASSERT(false); // 在恢复阶段，不可能出现替换情况

	MMS_TEST_SET_TASK(m_mms, rpClass, 1);
	SYNCHERE(SP_MMS_RANDOM_REPLACE);

	recPage = rpClass->m_oldestHeap->getPage(0);

	MMSTABLE_TEST_GET_PAGE(this, recPage);
	SYNCHERE(SP_MMS_GET_PIN_WHEN_REPLACEMENT);

	// 检查当前表类型最老页堆堆顶项, 注:recPage可能为NULL
	if (!recPage) {
		MmsPageOper::mmsRWUNLock(session->getId(), m_ridLocks[ridNr], Exclusived);
		MmsPageOper::mmsRWUNLock(session->getId(), &m_mmsTblLock, Exclusived);
		recPage = (MmsRecPage *)m_mms->allocMmsPage(session, false);
		if (recPage) {
			m_mms->m_numPagesInUse.increment();
			MmsPageOper::initRecPage(rpClass, recPage);
			recPage->m_numPins.increment();
			MMS_RWLOCK(session->getId(), &m_mmsTblLock, Exclusived);
			rpClass->addRecPageToFreeList(recPage);
			m_status.m_recordPages++;
		} else
			MMS_RWLOCK(session->getId(), &m_mmsTblLock, Exclusived);
		*locked = false;
		return recPage;
	} // return NULL; // 该级别无记录页
	if (!MMS_TRYLOCK_REC_PAGE(session, m_mms, recPage)) return NULL; // 加锁不成功
	if (recPage->m_numPins.get()) { // 该页存在pin
		MmsPageOper::unlockRecPage(session, m_mms, recPage);
		SYNCHERE(SP_MMS_GET_PIN_WHEN_REPLACEMENT_END);
		return NULL;
	}
	assert(!recPage->m_numFreeSlots);
	mmsRecord = MmsPageOper::getLruRecord(recPage);

	*locked = false;
	MmsPageOper::mmsRWUNLock(session->getId(), m_ridLocks[ridNr], Exclusived);
	if (mmsRecord->m_dirty)
		writeDataToDrs(session, recPage, mmsRecord, true);

	// 获取RID区间锁
	int ridCurrNr = getRidPartition(RID_READ(mmsRecord->m_rid));
	if (!MMS_RWTIMEDLOCK(session->getId(), m_ridLocks[ridCurrNr], Exclusived, 0)) {
		MmsPageOper::unlockRecPage(session, m_mms, recPage);
		return NULL;
	}

	m_mms->m_status.m_recordVictims++;
	m_status.m_recordVictims++;
	recPage->m_rpClass->m_status.m_recordVictims++;

	m_ridMaps[ridCurrNr]->del(mmsRecord);
	MmsPageOper::mmsRWUNLock(session->getId(), m_ridLocks[ridCurrNr], Exclusived);

	m_status.m_records--;
	m_status.m_recordDeletes++;
	m_mms->m_status.m_recordDeletes++;

	// 从在页内删除记录项
	MmsPageOper::clearRecordInPage(recPage, mmsRecord);

	if (rpClass->m_numSlots == recPage->m_numFreeSlots) { // 惩罚页为空
		// 把记录页从页堆删除
		assert (recPage->m_numPins.get() == 0);
		rpClass->m_oldestHeap->del(session, recPage->m_oldestHeapIdx, recPage);
	} else {
		// 调整页堆
		rpClass->m_oldestHeap->moveDown(session, recPage->m_oldestHeapIdx, true, recPage);
	}

	recPage->m_numPins.increment();
	assert(recPage->m_rpClass == rpClass);
	if (!recPage->m_freeLink.getList())
		rpClass->addRecPageToFreeList(recPage);
	return recPage;
}

/** 
 * 获取待替换的记录页个数
 *
 * @return 待替换记录页个数
 */
int Mms::getReplacedPageNum() {
	int pagesCurr = getCurrentSize();
	int	pagesLimit = (int) ((float)getTargetSize() * (1 - m_replaceRatio));

	return pagesCurr > pagesLimit ? pagesCurr - pagesLimit : 0;
}

/** 
 * 释放一个MMS页
 *
 * @param session 会话
 * @param mmsTable 被释放页所属的表
 * @param page 被释放的MMS页
 */
void Mms::freeMmsPage(Session *session, MmsTable *mmsTable, void *page) {
	m_numPagesInUse.decrement();
	freePage(session->getId(), page);
	mmsTable->m_status.m_recordPages--;
}

/** 
 * 设置MMS表缓存的最多记录个数
 *
 * @param maxRecordCount 最多记录个数
 */
void MmsTable::setMaxRecordCount(u64 maxRecordCount) {
	m_maxRecordCount = maxRecordCount;
}

/** 
 * 设置MMS表缓存更新
 *
 * @param cacheUpdate 是否缓存更新
 */
void MmsTable::setCacheUpdate(bool cacheUpdate) {
	m_cacheUpdate = cacheUpdate;
}

/** 
 * 设置映射分区个数
 *
 * @param ridNrZone RID映射表分区数
 */
void MmsTable::setMapPartitions(int ridNrZone) {
	m_ridNrZone = ridNrZone;

	m_ridMaps = new MmsRidMap* [m_ridNrZone];
	m_ridLocks = new LURWLock* [m_ridNrZone];
	for (int i = 0; i < m_ridNrZone; i++) {
		m_ridMaps[i] = new MmsRidMap(m_mms, this);
		m_ridLocks[i] = new LURWLock(m_db->getConfig()->m_maxSessions + 1, "MmsTable::RidLock", __FILE__, __LINE__);
	}
}

/**
 * 设置MMS写binlog的回调对象
 *
 * @param mmsCallback	回调对象
 */
void MmsTable::setBinlogCallback(MMSBinlogCallBack *mmsCallback) {
	m_binlogCallback = mmsCallback;
}

/**
 * 设置压缩记录提取器
 *
 * @param 
 */
void MmsTable::setCprsRecordExtrator(CmprssRecordExtractor *cprsRcdExtractor) {
	m_cprsRcdExtractor = cprsRcdExtractor;
}

/************************************************************************/
/*					页级别类实现                                        */
/************************************************************************/

/**
* 获取统计信息
*
* @return  统计结果
*/
const MmsRPClassStatus& MmsRPClass::getStatus() {
	m_status.m_recordPages = m_oldestHeap->getPagesOccupied();
	return m_status;
}

/** 
* 返回本缓存页类存储的记录的最小大小
* 注: 对于变长记录表，由于更新可能会导致实际存储的记录最小大小小于本大小
*
* @return 本缓存页类所存储的记录的最小大小(包含)
*/
u16 MmsRPClass::getMinRecSize() {
	assert(m_mmsTable);
	int nrClasses = m_mmsTable->m_mms->m_nrClasses;
	int idx = 0;

	for (; idx < nrClasses; idx++) {
		if (m_mmsTable->m_rpClasses[idx] == this)
			break;
	}
	assert(idx < nrClasses);
	if (idx)
		return m_mmsTable->m_mms->m_pageClassSize[idx - 1] + 1;		
	else
		return 1;
}

/** 
* 返回本缓存页类存储的记录的最大大小
*
* @return 本缓存页类所存储的记录的最大大小(包含)
*/
u16 MmsRPClass::getMaxRecSize() {
	return m_slotSize;
}

/** 
* 释放本缓存页类存储的所有页面
*
* @param session
*/
void MmsRPClass::freeAllPages(Session *session) {
	size_t rlClassesPageCnt = m_oldestHeap->getHeapArray()->getSize();
	for (size_t pageCnt = 0; pageCnt < rlClassesPageCnt; pageCnt++) {
		MmsRecPage *recPage = m_oldestHeap->getPage(pageCnt);
		MMS_LOCK_REC_PAGE(session, m_mmsTable->m_mms, recPage);
		m_mmsTable->m_mms->freeMmsPage(session, m_mmsTable, recPage);
	}
}

/**
 * MmsRPClass类构造函数
 *
 * @param mmsTable MMS表
 * @param slotSize 缓存槽大小
 */
MmsRPClass::MmsRPClass(MmsTable *mmsTable, u16 slotSize) {
	m_mmsTable = mmsTable;
	m_slotSize = slotSize;
	m_numSlots = (u8)(MMS_MAX_RECORD_SIZE / m_slotSize);
	m_oldestHeap = new MmsOldestHeap(mmsTable->m_mms, this, mmsTable->m_freqHeap);

	// 统计信息初始化
	memset(&m_status, 0, sizeof(MmsRPClassStatus));
}

/**
 * 析构函数
 */
MmsRPClass::~MmsRPClass() {
	delete m_oldestHeap;
}

/**
 * 获取空闲缓存记录页
 * 
 * @return 缓存记录页
 */
MmsRecPage* MmsRPClass::getRecPageFromFreelist() {
	if (m_freeList.isEmpty()) 
		return NULL;
	return m_freeList.getHeader()->getNext()->get();
}

/**
 * 如果记录页在空闲链表中，则删除该页
 * 
 * @param recPage 待删除的记录页
 */
void MmsRPClass::delRecPageFromFreelist(MmsRecPage *recPage) {
	assert(recPage->m_rpClass == this);

	if (recPage->m_freeLink.getList()) {
		recPage->m_freeLink.unLink();
		m_status.m_freePages--;
	}
}

/**
 * 向空闲页链表添加一页
 * @pre 记录页不属于空闲链表
 * 
 * @param recPage 待添加的记录页
 */
void MmsRPClass::addRecPageToFreeList(MmsRecPage *recPage) {
	assert(!recPage->m_freeLink.getList());
	assert(recPage->m_rpClass == this);

	m_freeList.getHeader()->addBefore(&recPage->m_freeLink);
	m_status.m_freePages++;
}

/************************************************************************/
/*					更新日志刷写后台线程实现                            */
/************************************************************************/

/** 
 * MmsFlushTimer构造函数
 * 
 * @param db 数据库
 * @param mmsTable	所属的MMS表
 * @param interval	间隔时间
 */
MmsFlushTimer::MmsFlushTimer(Database *db, MmsTable *mmsTable, uint interval) : BgTask(db, "Mms Flush Timer", interval, false, 1000) {
	m_mmsTable = mmsTable;
} 

/** 
 * 运行
 */
void MmsFlushTimer::runIt() {
#ifdef NTSE_UNIT_TEST
	if (!m_mmsTable->m_autoFlushLog)
		return;
#endif
	m_mmsTable->flushLog(m_session);
}

/************************************************************************/
/*					页替换后台线程实现		                            */
/************************************************************************/

/** 
 * 构造函数
 * 
 * @param mms 全局MMS
 * @param db 数据库
 * @param interval	间隔时间
 */
MmsReplacer::MmsReplacer(Mms *mms, Database *db, uint interval) : BgTask(db, "Mms Replacer", interval, false, 1000) {
	m_mms = mms;
	m_db = db;
} 

/** 
 * 运行
 */
void MmsReplacer::runIt() {
	void *page;
	int pageCountNeeded = m_mms->getReplacedPageNum();
	m_db->getSyslog()->log(EL_DEBUG, "Plans to replace %d pages.", pageCountNeeded);

	m_mms->computeDeltaQueries(m_session);
	while (pageCountNeeded) {
		page = m_mms->allocMmsPage(m_session, true);
		if (!page)
			break;
		m_mms->freePage(m_session->getId(), page); // 释放记录页
		pageCountNeeded--;
	}
	m_db->getSyslog()->log(EL_DEBUG, "%d pages was skipped.", pageCountNeeded);
}

}

