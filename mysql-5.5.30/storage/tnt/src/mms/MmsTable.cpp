/**
 * MMSģ��ʵ��
 *
 * 
 *
 * @author �۷�(shaofeng@corp.netease.com, sf@163.org)
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
/*                  ������MMS��ʵ��		��                              */
/************************************************************************/

class MmsRPClass;

/**
 * ����һ��MmsTable����
 *
 * @param mms ����ȫ��MMS
 * @param db	 ����Database
 * @param drsHeap ������
 * @param cacheUpdate �Ƿ񻺴����
 * @param updateCacheTime ���»���ʱ�䣬��λΪ��
 * @param partitionNr RID��������
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
	m_deltaRecordQueries = 1.0; // ���ֵ
	m_binlogCallback = NULL;
	m_cprsRcdExtractor = NULL;

	// ����ҳ����
	m_rpClasses = new MmsRPClass *[m_mms->m_nrClasses];
	memset(m_rpClasses, 0, sizeof(MmsRPClass *) * m_mms->m_nrClasses);
	
	// �������Ƶ��ҳ��
	m_freqHeap = new MmsFreqHeap(m_mms, this);

	// ������ʱҳ����
	m_recPageArray = new Array<MmsRecPage *>();

	// ͳ����Ϣ��ʼ��
	memset(&m_status, 0, sizeof(MmsTableStatus));

	// ��ʼ��MMS���������Ϣ
	m_cacheUpdate = cacheUpdate;
	m_updateBitmapOffsets = new byte[tableDef->m_numCols];
	// δʹ�ã�Ĭ��Ϊ255
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

	setMapPartitions(partitionNr); // ����ӳ�����

#ifdef NTSE_UNIT_TEST
	m_autoFlushLog = true;
	m_testCurrPage = NULL;
	m_testCurrRecord = NULL;
#endif
}

/**
 * �ر�һ�����Ӧ��MMSϵͳ
 *
 * @param session	 �Ự����
 * @param flushDirty �Ƿ�д��������
 */
void MmsTable::close(Session *session, bool flushDirty) {
	ftrace(ts.mms, tout << session << flushDirty);

	while (m_existPin.get() > 0) // �ȴ���PINΪ0
		Thread::msleep(50);
	// ɾ��������Ϣ
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

	// ˢд������
	if (flushDirty)
		doFlush(session, true, true, true);

	// ɾ��ӳ���
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

	// ɾ����ʱҳ����
	delete m_recPageArray;
	m_recPageArray = NULL;

	// ɾ��ҳ�� ������ҳ���е����м�¼ҳ��
	for (int i = 0; i < m_mms->m_nrClasses; i++)
		if (m_rpClasses[i]) {
			m_rpClasses[i]->freeAllPages(session);
			delete m_rpClasses[i];
			m_rpClasses[i] = NULL;
		}
	delete [] m_rpClasses;
	m_rpClasses = NULL;

	// ɾ�����Ƶ��ҳ��
	delete m_freqHeap;
	m_freqHeap = NULL;

	MmsPageOper::mmsRWUNLock(session->getId(), &m_mmsTblLock, Exclusived);
}

/** 
 * ��ȡ���»�����ˢ��ʱ��
 *
 * @return ���ʱ�� ��λΪ��
 */
uint MmsTable::getUpdateCacheTime() {
	if (m_flushTimer)
		return m_flushTimer->getInterval() / 1000;
	return 0;
}

/** 
 * ���ø��»�����ˢ��ʱ��
 *
 * @param updateCacheTime ���¼��ˢ��ʱ�� ���Ϊ��
 */
void MmsTable::setUpdateCacheTime(uint updateCacheTime) {
	if (m_flushTimer) 
		m_flushTimer->setInterval(updateCacheTime * 1000);
}

/**
 * ����RID��ȡMMS�����¼
 * @post ���صļ�¼����ҳ�Ѿ���pinס
 *
 * @param session �Ự����
 * @param rid RID
 * @param touch �Ƿ���¼�¼����ʱ��������б�ɨ��ʱ�ʺ�MRU������LRU�滻����ʱ������ʱ���
 * @param rlh INOUT������ΪNULL������Ҫ�Է��صļ�¼�����������ڴ洢������������򲻼���
 * @param lockMode ��ģʽ
 * 
 * @return ָ��MMS��¼��ָ�룬�Ҳ�������NULL
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

	m_mms->m_status.m_recordQueries++; // �Ǿ�ȷ
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
	if (recPage->m_numPins.get() == -1) { // �ü�¼ҳ������(���ڱ��滻)
		MmsPageOper::unlockRecPage(session, m_mms, recPage, recPageMode);
		return NULL;
	}

	recPage->m_numPins.increment();

	// �Ƿ��Ѽ�����
	if (rlh) {
		assert (rid == RID_READ((byte *)mmsRecord->m_rid));
		SYNCHERE(SP_MMS_RID_LOCKROW);
		*rlh = TRY_LOCK_ROW(session, m_tableDef->m_id, rid, lockMode);
		if (NULL == *rlh) {
			// ���ԼӼ�¼��ʧ�ܣ���ʱ��¼ҳ�汾��
			pageVersion = recPage->m_version;
			// �ͷ�ҳ��
			MmsPageOper::unlockRecPage(session, m_mms, recPage, recPageMode);
			SYNCHERE(SP_MMS_RID_UNLOCKROW);
			// �Ӽ�¼��
			*rlh = LOCK_ROW(session, m_tableDef->m_id, rid, lockMode);
			recPageMode = Exclusived;
			// ��ҳ��
			MMS_LOCK_REC_PAGE_EX(session, m_mms, recPage, recPageMode);
			if (pageVersion != recPage->m_version 
				&& (!mmsRecord->m_valid || RID_READ(mmsRecord->m_rid) != rid)) {
					// �ͷ�ҳ�ϵ�����pin
					session->unlockRow(rlh);
					if (recPage->m_numPins.get() == 1 && recPage->m_numFreeSlots == recPage->m_rpClass->m_numSlots) { // �ͷſ�ҳ
						memset(recPage, 0, Limits::PAGE_SIZE); // ���ҳ
						m_mms->freeMmsPage(session, this, recPage);
					} else {
						recPage->m_numPins.decrement();
						MmsPageOper::unlockRecPage(session, m_mms, recPage, recPageMode);
					}
					goto start;
			}
		}
	}
	if (lruChanged && doTouch(session, recPage, mmsRecord)) // ���ʻ����¼
		NTSE_ASSERT(false);  // ��Ϊ�ⲿ�Ѿ�����������ҳ��pin, ���Լ�¼ҳ������Ϊ��
	m_mms->m_status.m_recordQueryHits++;
	m_status.m_recordQueryHits++;
	assert(recPage->m_numPins.get() > 0);
	MmsPageOper::unlockRecPage(session, m_mms, recPage, recPageMode); // �ͷż�¼ҳ��(ҳpin�Գ���)
	return mmsRecord;
}

/** 
 * ����RID����MMS����ȡָ��������
 * ע�⣺Ϊ������ܣ����ж��Ƿ�ΪMMS���¼ʱ���ú������Ӽ�¼ҳ����
 *
 * @param session �Ự
 * @param rid Ҫ�����ļ�¼RID
 * @param extractor �Ӽ�¼��ȡ��
 * @param subRec INOUT��Ҫ��ȡ�Ĳ��ּ�¼����ʽһ��ΪREC_REDUNDANT
 *   ������Ҫ��ȡ���ּ�¼����������֤�����subRec���ݲ����޸�
 * @param touch �Ƿ���¼�¼����ʱ���
 * @param ifDirty �Ƿ�ֻ�ڼ�¼Ϊ��ʱ�Ŷ�ȡ���ּ�¼
 * @param readMask һ��32λ��λͼ��ʾҪ��ȡ��Щ���ԣ�ǰ31λ���1-31������һһ��Ӧ��
 *   ��32λ�ܹ�ʣ�µ��������ԣ���ifDirtyΪtrue����ֻ��readMask�е�������MMS�б��޸�
 *   ʱ����Ҫ��ȡ���ּ�¼
 * @return �Ƿ�����MMS
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
	mmsRecord = m_ridMaps[ridNr]->get(rid);// ��ѯRIDӳ���
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
	if (recPage->m_numPins.get() == -1) { // �ü�¼ҳ������(���ڱ��滻)
		MmsPageOper::unlockRecPage(session, m_mms, recPage, recPageMode);
		return false;
	}
	recPage->m_numPins.increment();
	if (lruChanged && doTouch(session, recPage, mmsRecord))
		goto start; // ��¼���ѱ������߳�ɾ������Ҫ���²�ѯ 
	getSubRecord(mmsRecord, extractor, subRec);
	recPage->m_numPins.decrement();
	MmsPageOper::unlockRecPage(session, m_mms, recPage, recPageMode);
end:
	m_mms->m_status.m_recordQueryHits++;
	m_status.m_recordQueryHits++;
	return true;
}

/** 
 * ���ʼ�¼��
 * @pre �ѼӼ�¼��
 *
 * @param session �Ự
 * @param recPage ��¼ҳ
 * @param mmsRecord ��¼��
 * @return �Ƿ�ɾ����ҳ ����touch�����У��ü�¼ҳ�����м�¼������߳�ɾ�������¿ռ�¼ҳ���ɸú����������)
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
		} else { // TODO: ����ô���������ƿ�������Կ�����TRYLOCK MMS�����ɹ�ʱ����ִ��touch������
			MmsPageOper::unlockRecPage(session, m_mms, recPage);
			SYNCHERE(SP_MMS_DOTOUCH_UNLOCK);
			MMS_RWLOCK(session->getId(), &m_mmsTblLock, Exclusived);
			MMS_LOCK_REC_PAGE(session, m_mms, recPage);
			if (recPage->m_numFreeSlots != recPage->m_rpClass->m_numSlots) // ��¼ҳ��Ϊ��
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
 * �ͷ�MMS��¼����ҳ��pin
 *
 * @param session �Ự
 * @param mmsRecord MMS��¼
 */
void MmsTable::unpinRecord(Session *session, const MmsRecord *mmsRecord) {
	ftrace(ts.mms, tout << session << (void *)mmsRecord);
	UNREFERENCED_PARAMETER(session);
	MmsRecPage *recPage = MmsPageOper::getRecPage(m_mms->m_pagePool, const_cast<MmsRecord *>(mmsRecord));
	recPage->m_numPins.decrement();
}

/**
 * ����һ����¼��MMS������
 * @pre  �ü�¼����MMS������, �Ѽ�MMS����
 * @post �ͷ�MMS��������¼����ҳ��pinס
 *
 * @param session �Ự����
 * @param record Ҫ����ļ�¼
 * @param dirtyBitmap ��λͼ
 * @param ridNr ����������
 * @param tryAgain INOUT �ٴγ���
 * @return ��¼����벻�ɹ�������NULL
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

	// ��ȡҳ����
	int classType = m_mms->m_size2class[sizeof(MmsRecord) + record->m_size];
	if (!m_rpClasses[classType])
		m_rpClasses[classType] = new MmsRPClass(this, m_mms->m_pageClassSize[classType]);

	// ��ҳ����м�¼ҳ�����л�ȡ���м�¼ҳ 
	recPage = m_rpClasses[classType]->getRecPageFromFreelist();
	if (!recPage) {
		bool ridLocked = true;
		// ��ȫ��MMS�л�ȡ���м�¼ҳ�������ü�¼���¼ҳ�Ѽ����������ѷ������ҳ����
		recPage = allocMmsRecord(session, m_rpClasses[classType], ridNr, &ridLocked);
		if (!recPage) {
			if (ridLocked)
				MmsPageOper::mmsRWUNLock(session->getId(), m_ridLocks[ridNr], Exclusived);
			MmsPageOper::mmsRWUNLock(session->getId(), &m_mmsTblLock, Exclusived);
			m_status.m_replaceFailsWhenPut++;
			return NULL;
		}
		if (!ridLocked) { // ����
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
	// ��RIDӳ��������ӳ����
	m_ridMaps[ridNr]->put(mmsRecord);
	m_ridMaps[ridNr]->unreserve();
	MmsPageOper::mmsRWUNLock(session->getId(), m_ridLocks[ridNr], Exclusived); // ���ͷ�RID������
	MmsPageOper::fillRecSlotEx(recPage, mmsRecord, record);
	if (dirtyBitmap)
		MmsPageOper::setDirty(recPage, mmsRecord, true, dirtyBitmap);
	if (m_rpClasses[classType]->m_numSlots == recPage->m_numFreeSlots + 1)
		m_rpClasses[classType]->m_oldestHeap->insert(recPage);
	if (0 == recPage->m_numFreeSlots) { 
		// ע�����ﲻ��Ҫ����ҳ��
		// ���������ӿ��ж���ɾ��
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
 * ��ָ���ļ�¼����MMS������ʱ������һ����¼��MMS�����С�
 * @post ���صļ�¼����ҳ�Ѿ���pinס
 *
 * @param session �Ự����
 * @param record Ҫ����ļ�¼
 * @return ����¼��MMS���Ѿ����ڣ��򷵻��Ѿ����ڵļ�¼�����򷵻��²���ļ�¼��������벻�ɹ�������NULL
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
	// ��MMS����
	MMS_RWLOCK(session->getId(), m_ridLocks[ridNr], Exclusived);

	// ��ѯ����ӳ���
	if ((mmsRecord = m_ridMaps[ridNr]->get(record->m_rowId)) != NULL) {
		recPage = MmsPageOper::getRecPage(m_mms->m_pagePool, mmsRecord);
		MMS_LOCK_REC_PAGE(session, m_mms, recPage);
#ifdef NTSE_UNIT_TEST
		m_testCurrPage = recPage;
#endif
		SYNCHERE(SP_MMS_PUT_DISABLEPG);
		if (recPage->m_numPins.get() == -1) { // �ü�¼ҳ������
			MmsPageOper::mmsRWUNLock(session->getId(), m_ridLocks[ridNr], Exclusived);
			MmsPageOper::unlockRecPage(session, m_mms, recPage);
			return NULL;
		}
		recPage->m_numPins.increment();
		MmsPageOper::mmsRWUNLock(session->getId(), m_ridLocks[ridNr], Exclusived);
		if (doTouch(session, recPage, mmsRecord))
			return NULL; // ��¼�ɾ��
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
 * ɾ����¼��
 * @pre �Ѽӱ����ͼ�¼ҳ��
 *
 * @param session �Ự
 * @param recPage ��¼ҳ
 * @param mmsRecord ��¼��
 */
void MmsTable::delRecord(Session *session, MmsRecPage *recPage, MmsRecord *mmsRecord) {
	ftrace(ts.mms, tout << session << recPage << mmsRecord << RID_READ(mmsRecord->m_rid));

	// ��ҳ��ɾ���ɼ�¼��(ע�������ģ�������֤�������̶߳�ȡ�ü�¼������)
	bool changedTimestamp = MmsPageOper::clearRecordInPage(recPage, mmsRecord);

	// �����¼ҳ������Ϊ���У�����ӵ�����������
	if (1 == recPage->m_numFreeSlots)
		recPage->m_rpClass->addRecPageToFreeList(recPage);

	// ��Ҫ����ҳ��
	if (changedTimestamp) {
		MmsRPClass *rpClass = recPage->m_rpClass;
		// ��¼ҳΪȫ�����������̼߳�pin
		if (recPage->m_numFreeSlots == rpClass->m_numSlots) {
			// ��ҳ��ɾ��, ע��recNewPageҳ�������Ѿ��ͷ�	
			rpClass->m_oldestHeap->del(session, recPage->m_oldestHeapIdx, recPage);
			// �ӿ�������ɾ��
			rpClass->delRecPageFromFreelist(recPage);
			if (recPage->m_numPins.get() == 1) {
				// ���ҳ
				memset(recPage, 0, Limits::PAGE_SIZE);
				MmsPageOper::mmsRWUNLock(session->getId(), &m_mmsTblLock, Exclusived);
				// �����ɿ���ҳ�ͷŵ�MMS
				m_mms->freeMmsPage(session, this, recPage);
			} else {
				recPage->m_numPins.decrement();
				MmsPageOper::unlockRecPage(session, m_mms, recPage);
				MmsPageOper::mmsRWUNLock(session->getId(), &m_mmsTblLock, Exclusived);
			}
		} else { // �ռ�¼ҳ���
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
 * �ܷ����MMS��¼
 * @pre �������Ѿ����м�¼ҳpin
 *
 * @param mmsRecord MMS��¼��
 * @param subRecord ���µ����Լ�����ֵ REC_REDUNDANT
 * @param recSize OUT ���º��¼��С
 * @return �ܷ����
 */
bool MmsTable::canUpdate(MmsRecord *mmsRecord, const SubRecord *subRecord, u16 *recSize) {
	Record oldRecord;

	MmsRecPage *recPage = MmsPageOper::getRecPage(m_mms->m_pagePool, mmsRecord);

	// �����¼�¼���
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

/** �ܹ�����MMS��¼
 * @pre �������Ѿ����м�¼ҳpin
 *
 * @param mmsRecord MMS��¼��
 * @param newSize ���º��¼��С
 * @return �ܷ����
 */
bool MmsTable::canUpdate(MmsRecord *mmsRecord, u16 newSize) {
	MmsRecPage *recPage = MmsPageOper::getRecPage(m_mms->m_pagePool, mmsRecord);
	return (newSize + sizeof(MmsRecord)) <= recPage->m_slotSize;
}

/**
 * ����һ��MMS��¼
 * @pre �����߳���pin
 * @post pin���ͷ�
 *
 * @param session �Ự����
 * @param mmsRecord MMS��¼��
 * @param subRecord ���µ����Լ�����ֵ REC_REDUNDANT
 * @param recSize	���º��¼��С
 * @param newCprsRcd INOUT ������벻ΪNULL�������Ϊ���º�ļ�¼
 */
void MmsTable::update(Session *session, MmsRecord *mmsRecord, const SubRecord *subRecord, 
					  u16 recSize, Record *newCprsRcd){
	ftrace(ts.mms, tout << session << mmsRecord << subRecord << recSize);
	
	PROFILE(PI_MmsTable_update);

	Record oldRecord(INVALID_ROW_ID, m_tableDef->m_recFormat, NULL, 0);
	u32 updBitmap = 0;
	u32 dirtyBitmap = 0;
	bool notCached = cols2bitmap(subRecord, &updBitmap, &dirtyBitmap); // ���㱾�θ���λͼ
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
			//���ﲻ���ñ��ظ���ѹ����¼����Ϊ�п��ܸ���֮���ѹ����¼��ԭ�������������������ѹ��֮��Ȳ�ѹ������
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
	// ��Ϊ���¼��ע���ڸ����в�����ʱ���
	MmsPageOper::setDirty(recPage, mmsRecord, true, mmsRecord->m_dirtyBitmap | dirtyBitmap);
	// ���²���
	if (m_cacheUpdate) {
		if (notCached) {
			//���ͷ�ҳ�����ͷ�pin���ڼ�ҳ�治�ᱻ�滻��
			//�����ϲ��Ѿ�������������mmsRecord�Ķ�д�ǰ�ȫ��
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
 * ˢ�����ݲ�ɾ����¼��
 * @pre ����pin
 * @post �ͷ�pin
 * 
 * @param session �Ự����
 * @param mmsRecord ��¼��
 */
void MmsTable::flushAndDel(Session *session, MmsRecord *mmsRecord) {
	ftrace(ts.mms, tout << session << mmsRecord << RID_READ(mmsRecord->m_rid));

	MmsRecPage *recPage = MmsPageOper::getRecPage(m_mms->m_pagePool, mmsRecord);
	int ridNr = getRidPartition(RID_READ(mmsRecord->m_rid));

	MMS_RWLOCK(session->getId(), m_ridLocks[ridNr], Exclusived);
	m_ridMaps[ridNr]->del(mmsRecord); // ��RIDӳ�����ɾ����Ӧ�����
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
 * ɾ��һ��MMS��¼
 * @pre �������Ѿ������˼�¼����MMSҳ��pin
 * @post ��¼����MMSҳ��pin�Ѿ����ͷ�
 *
 * @param session �Ự
 * @param mmsRecord MMS��¼
 */
void MmsTable::del(Session *session, MmsRecord *mmsRecord) {
	PROFILE(PI_MmsTable_del);

	ftrace(ts.mms, tout << session << mmsRecord << RID_READ(mmsRecord->m_rid));
	assert(mmsRecord);
	MmsRecPage *recPage = MmsPageOper::getRecPage(m_mms->m_pagePool, mmsRecord);
	int ridNr = getRidPartition(RID_READ(mmsRecord->m_rid));

	MMS_RWLOCK(session->getId(), m_ridLocks[ridNr], Exclusived);
	m_ridMaps[ridNr]->del(mmsRecord); // ��RIDӳ�����ɾ����Ӧ�����
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
 * д���������¼��DRS�У��ڼ���ʱ���ã�
 *
 * @param session �Ự����
 * @param force ǿ��ˢ������
 * @param ignoreCancel �Ƿ���Բ���ȡ������
 * @throw NtseException ������ȡ�����쳣��ΪNTSE_EC_CANCELED
 */
void MmsTable::flush(Session *session, bool force, bool ignoreCancel) throw(NtseException) {
	ftrace(ts.mms, tout << session << ignoreCancel;);

	doFlush(session, force, ignoreCancel, false);
}

/**
 * ˢд���¼
 *
 * @param session �Ự����
 * @param force ǿ��ˢ������
 * @param ignoreCancel �Ƿ���Բ���ȡ������
 * @param tblLocked �Ƿ��Ѽ�MMS����
 * @throw NtseException ������ȡ�����쳣��ΪNTSE_EC_CANCELED
 */
void MmsTable::doFlush(Session *session, bool force, bool ignoreCancel, bool tblLocked) throw(NtseException) {
	Array<MmsRecPage *> tmpPageArray;
	std::vector<MmsRecPair> tmpRecArray;

	u64 totalDirties = m_numDirtyRecords.get() + 1;
	u64 doneDirties = 0;
	
	for (int i = 0;  i < m_mms->m_nrClasses; i++) {
		MmsRPClass *rpClass = m_rpClasses[i];
		if (rpClass) {// �Լ����ڵ�����ҳִ��ˢд���
			if (!tblLocked)
				MMS_RWLOCK(session->getId(), &m_mmsTblLock, Shared);
			// ��ȡ�����ڵ����м�¼ҳ
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

					// �쳣ʱֱ�ӷ��أ�û���ڴ����й©
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
 * ����ˢ�»����¼��
 *
 * @param session �Ự����
 * @param force ǿ��ˢ������
 * @param ignoreCancel �Ƿ���Բ���ȡ������
 * @param table ������MMS��
 * @param tmpRecArray ��ʱ����
 * @throw NtseException ������ȡ�����쳣��ΪNTSE_EC_CANCELED
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
		if (table != recPage->m_rpClass->m_mmsTable ||					             // �����������
			RID_READ((byte *)mmsRecord->m_rid) != RID_READ((byte *)recPair.m_rid) || // RID��ƥ��
			!mmsRecord->m_dirty) {									                 // �����¼
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
			if (prAfter > prBefore) {	// ��ֹ��ҳ�滺�湻��ʱ����Ҫ��sleep
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
 * ˢд������־
 *
 * @param session �Ự
 * @param force �Ƿ�Ϊǿ��ˢ��
 */
void MmsTable::flushLog(Session *session, bool force) {
	ftrace(ts.mms, tout << session << force);

	if (force) {
		assert(m_inLogFlush.get());	
	} else {
		if (m_mms->m_duringRedo) // REDO�׶Σ���ִ�м��ˢ��
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
		if (rpClass) {// �Լ����ڵ�����ҳִ��ˢд���
			MMS_RWLOCK(session->getId(), &m_mmsTblLock, Shared);
			// ��ȡ�����ڵ����м�¼ҳ
			heapArray = rpClass->m_oldestHeap->getHeapArray();
			size = heapArray->getSize();
			for (uint iPage = 0; iPage < size; iPage++) {
				if ((*heapArray)[iPage].m_page->m_numDirtyRecs) // �����¼ҳ���������¼�������־����
					if (!m_recPageArray->push((*heapArray)[iPage].m_page))
						NTSE_ASSERT(false); // ϵͳ�ҵ�	
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
				// �ǻ����¼ҳ��Ǹ�ҳ�������ļ�¼ҳ
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
 * ��ȡMMS��ͳ����Ϣ
 *
 * @return ͳ�ƽ��
 */
const MmsTableStatus& MmsTable::getStatus() {
	m_status.m_dirtyRecords = (u64)m_numDirtyRecords.get();
	return m_status;	
}

/** 
 * ��ȡMMS����
 *
 * @return MMS����
 */
const LURWLock& MmsTable::getTableLock() {
	return m_mmsTblLock;
}

/** 
 * ��ȡ��������
 *
 * return ��������
 */
uint MmsTable::getPartitionNumber() {
	return m_ridNrZone;
}

/** 
 * ��ȡRID��ϣ��ͻ״̬
 *
 * @param partition RID��ϣ�������
 * @param avgConflictLen OUT����ͻ����ƽ������
 * @param maxConflictLen OUT����ͻ������󳤶�
 */
void MmsTable::getRidHashConflictStatus(uint partition, double *avgConflictLen, size_t *maxConflictLen) {
	assert(partition < (uint)m_ridNrZone);
	m_ridMaps[partition]->getConflictStatus(avgConflictLen, maxConflictLen);
}

/** ��ȡ�Ӽ�¼����
 *
 * @param mmsRecord MMS��¼��
 * @param extractor �Ӽ�¼��ȡ��
 * @param subRecord INOUT �Ӽ�¼
 */
void MmsTable::getSubRecord(const MmsRecord *mmsRecord, SubrecExtractor *extractor, SubRecord *subRecord) {
	PROFILE(PI_MmsTable_getSubRecord);

	ftrace(ts.mms, tout << mmsRecord << extractor << subRecord);

	Record rec(RID_READ((byte *)mmsRecord->m_rid), getMmsRecFormat(m_tableDef, mmsRecord), (byte *)mmsRecord + sizeof(MmsRecord), 
		mmsRecord->m_size);
	extractor->extract(&rec, subRecord);
}

/**
 * ��ȡ�Ӽ�¼����
 *
 * @param session �Ự
 * @param mmsRecord MMS��¼��
 * @param subRecord INOUT �Ӽ�¼��һ��ΪREC_REDUNDANT��ʽ
 */
void MmsTable::getSubRecord(Session *session, const MmsRecord *mmsRecord, SubRecord *subRecord) {
	assert(REC_REDUNDANT == subRecord->m_format);
	PROFILE(PI_MmsTable_getSubRecord);

	ftrace(ts.mms, tout << mmsRecord << subRecord);

	Record record(RID_READ((byte *)mmsRecord->m_rid), getMmsRecFormat(m_tableDef, mmsRecord), (byte *)mmsRecord + sizeof(MmsRecord), mmsRecord->m_size);
	
	if (record.m_format == REC_COMPRESSED) {//�����¼��ѹ����ʽ��
		assert(m_cprsRcdExtractor != NULL);//��ʱ�ֵ�һ�����õģ���Ȼ��������ѹ����ʽ����MMS��
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
 * ��ȡ��¼����
 *
 * @param mmsRecord MMS��¼��
 * @param rec INOUT ��¼
 * @param copyIt �ǿ�����¼���ݻ���ֱ�ӷ���mmsRecord�еļ�¼����
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
 * ����MMS��¼�Ƿ�Ϊ��
 *
 * @param mmsRecord MMS��¼��
 * @return �Ƿ�Ϊ��
 */
bool MmsTable::isDirty(const MmsRecord *mmsRecord) {
	return mmsRecord->m_dirty != 0;
}

/** 
 * �滻��¼ҳ
 *
 * @param session �Ự���󣬿�ΪNULL
 * @param victimPage ���滻�ļ�¼ҳ
 */
void MmsTable::evictMmsPage(Session *session, MmsRecPage *victimPage) {
	PROFILE(PI_MmsTable_evictMmsPage);

	ftrace(ts.mms, tout << session << victimPage);

	m_mms->m_status.m_pageVictims++;
	m_status.m_pageVictims++;
	victimPage->m_rpClass->m_status.m_pageVictims++;

	// ˢд���¼
	MmsRecord *currRecord = MmsPageOper::getLruRecord(victimPage);
	int numRecords = 0;
	while (1) {
		numRecords++;
		if (currRecord->m_dirty)
			writeDataToDrs(session, victimPage, currRecord,true);
		if (!currRecord->m_lruNext)
			break; // ��������
		currRecord = (MmsRecord *)MmsPageOper::offset2pointer(victimPage, currRecord->m_lruNext);
	}

	// ����ͳ����Ϣ
	m_status.m_records -= numRecords;
	victimPage->m_rpClass->m_status.m_records -= numRecords;
	victimPage->m_rpClass->m_status.m_recordDeletes += numRecords;
	victimPage->m_rpClass->m_status.m_recordVictims += numRecords;
	m_status.m_recordDeletes += numRecords;
	m_status.m_recordVictims += numRecords;
	m_mms->m_status.m_recordVictims += numRecords;
	victimPage->m_rpClass->m_mmsTable->m_status.m_recordPages--;

	// ��Ǹ�ҳ����ʹ��
	victimPage->m_numPins.set(-1);

	victimPage->m_rpClass->delRecPageFromFreelist(victimPage);
	victimPage->m_rpClass->m_oldestHeap->del(session, victimPage->m_oldestHeapIdx, victimPage);

	// ɾ����¼��
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
			break; // ��������
		currRecord = (MmsRecord *)MmsPageOper::offset2pointer(victimPage, currRecord->m_lruNext);
	}

	// ��ռ�¼ҳ
	memset(victimPage, 0, Limits::PAGE_SIZE);
}

/**
 * д�����ݵ�DRS
 *
 * @param session �Ự����
 * @param recPage ��¼ҳ
 * @param mmsRecord ��¼��
 * @param force ǿ��ˢ��DRS��
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
			// bufferʧ�䣬��ʱֻд��־��д��, ��¼����Ϊ��
			session->startTransaction(TXN_MMS_FLUSH_DIRTY, m_tableDef->m_id);
			writeMmsUpdateLog(session, &subRec);
			session->endTransaction(true);
			return;
		}
	}

	nftrace(ts.mms, tout << "Write dirty record to DRS: " << &subRec);
	// ʹ��һ���������¼���µ�����
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
 * ִ��дMMS��־���� (ע�⣺�����߸�������MemoryContext)
 *
 * @param session �Ự����
 * @param mc �ڴ�������
 * @param subRecord �Ӽ�¼ �䳤��ʽ
 */
void MmsTable::doMmsLog(Session *session, MemoryContext *mc, const SubRecord *subRecord) {
	ftrace(ts.mms, tout << session << mc << subRecord);
	assert(REC_VARLEN == subRecord->m_format);
	size_t size = sizeof(RowId) + sizeof(u16) * (1 + subRecord->m_numCols) + sizeof(uint) + subRecord->m_size;
	byte *logData = (byte *)mc->alloc(size);
	Stream s(logData, size);

	try {
		// дrid
		s.write(subRecord->m_rowId);
		// д���Գ���
		s.write((u16)subRecord->m_numCols);
		// д�����б�
		s.write((const byte *)subRecord->m_columns, sizeof(u16) * subRecord->m_numCols);
		// д�Ӽ�¼����
		s.write(subRecord->m_size);
		// д�Ӽ�¼����
		s.write(subRecord->m_data, subRecord->m_size);
	} catch(NtseException &) {
		NTSE_ASSERT(false);
	}

	session->writeLog(LOG_MMS_UPDATE, m_tableDef->m_id, logData, size);
}

/** 
 * дMMS������־ (�ڼ��ˢ�½׶�ʹ�ã�
 * 
 * @param session �Ự����
 * @param mmsRecord ��¼��
 * @param updBitmap ����λͼ
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
 * дMMS�������binlog
 *
 * @param session �Ự
 * @param updCachedCols ��������µ������б�
 * @param mmsRecord MMS��¼����
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
 * дMMS������־ (��MmsTable::update�в�����updateCacheʱʹ��)
 * 
 * @param session �Ự����
 * @param subRecord �Ӽ�¼ REC_REDUNDANT
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
 * дMMS������־ (��MmsTable::update������updateCacheʱʹ��)
 *
 * @param session �Ự����
 * @param mmsRecord ��¼��
 * @param subRecord �Ӽ�¼ REC_REDUNDANT
 * @param updBitmap ����λͼ
 */
void MmsTable::writeMmsUpdateLog(Session *session, MmsRecord *mmsRecord, const SubRecord *subRecord, u32 updBitmap) {
	assert(REC_REDUNDANT == subRecord->m_format);

	if (!updBitmap) {
		writeMmsUpdateLog(session, subRecord);
		return;
	}
	
	MemoryContext *mc = session->getMemoryContext();
	McSavepoint msp(mc);
	
	// �ϲ����и��������ֶ�
	int maxNumCols = subRecord->m_numCols + 32;  // �����ܳ���Ϊ�Ӽ�¼�ֶμ�32
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
 * �������¼λͼ������������
 * 
 * @param dirtyBitmap ���¼λͼ
 * @param cols INOUT ������
 * @return �����и���
 */
int MmsTable::dirtyBm2cols(u32 dirtyBitmap, u16 *cols) {
	int numCols = 0;
	u32 tmpBitmap = dirtyBitmap;
	u16 i = 0, j = 0;

	// ǰ31���ֶ�
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
 * ���ݸ���λͼ, ����������
 * 
 * @param updBitmap ����λͼ
 * @param cols INOUT ������
 * @return �����и���
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
 * ���ݸ��������У����쵱ǰ����λͼ�����¼λͼ
 *
 * @param subRecord �Ӽ�¼
 * @param updBitmap	INOUT ����λͼ
 * @param dirtyBitmap INOUT ���¼λͼ
 * @return �Ƿ���ڷǸ��»����ֶ�
 */
bool MmsTable::cols2bitmap(const SubRecord *subRecord, u32 *updBitmap, u32 *dirtyBitmap) {
	u16 *cols = subRecord->m_columns;
	uint numCols = subRecord->m_numCols;
	bool notCached = false;
	byte offset;

	*updBitmap = 0;
	*dirtyBitmap = 0;

	for (uint i = 0; i < numCols; i++) {
		// �������¼λͼ
		if (cols[i] < 31)
			*dirtyBitmap |= ((u32)1 << cols[i]);
		else
			*dirtyBitmap |= ((u32)1 << 31);
		// �������λͼ
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
 * �ϲ������ֶ�
 * 
 * @param subRecord �Ӽ�¼
 * @param updBitmap ����λͼ
 * @param cols ������
 * @param numCols ��������󳤶�
 * @return �����г���
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
 * �ϲ�����
 *
 * @param src1Cols һ��
 * @param num1Cols һ�г���
 * @param src2Cols ����
 * @param num2Cols ���г���
 * @param dstCols �ϲ���
 * @return �ϲ��г���
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
 * ���mms��¼�ĸ�ʽ
 * @param tableDef ����
 * @param mmsRecord mms��¼
 * @return mms��¼�ĸ�ʽ
 */
inline RecFormat MmsTable::getMmsRecFormat(const TableDef *tableDef, const MmsRecord *mmsRecord) {
	if (mmsRecord->m_compressed > 0) {
		return REC_COMPRESSED;
	} else {
		return tableDef->m_recFormat == REC_FIXLEN ? REC_FIXLEN : REC_VARLEN;
	}
}

/** 
 * ����MmsUpdate��־����REDO�ָ�
 * 
 * @param session �Ự����
 * @param log ��־����
 * @param size ��־��С
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

	// ��ȡ��־��ع��Ӽ�¼
	subRecord.m_format = REC_VARLEN;          // ��־��¼Ϊ�䳤��ʽ
	s.read(&rid)->read(&subRecord.m_numCols);
	subRecord.m_rowId = rid;
	subRecord.m_columns = (u16 *)mc->calloc(sizeof(u16) * subRecord.m_numCols);
	s.readBytes((byte *)subRecord.m_columns, sizeof(u16) * subRecord.m_numCols);
	s.read(&subRecord.m_size);
	subRecord.m_data = (byte *)mc->calloc(subRecord.m_size);
	s.readBytes(subRecord.m_data, subRecord.m_size);
	nftrace(ts.recv, tout << "rid: " << rid << ", rec: " << &subRecord);

	// ת�������ʽ
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

	// ��ȡԭ��¼
	record.m_rowId = rid;
	record.m_format = m_tableDef->m_recFormat;

	mmsRecord = getByRid(session, rid, false, NULL, None);
	
	if (mmsRecord) {
		nftrace(ts.recv, tout << "already exist in mms");
		if (REC_FIXLEN == record.m_format) { // ������¼����ԭ�����¼��������޸�
			assert(!mmsRecord->m_compressed);
			MMS_RWLOCK(session->getId(), &m_mmsTblLock, Exclusived);
			record.m_size = mmsRecord->m_size;
			record.m_data = (byte *)mmsRecord + sizeof(MmsRecord);
			needInsert = false;
		} else { // �䳤��¼��ѹ����¼����ɾ���������
			record.m_size = mmsRecord->m_size;
			record.m_data = (byte *)mc->calloc(record.m_size);
			record.m_format = mmsRecord->m_compressed > 0 ? REC_COMPRESSED : REC_VARLEN;
			memcpy(record.m_data, (byte *)mmsRecord + sizeof(MmsRecord), record.m_size);
			dirtyBitmap = mmsRecord->m_dirtyBitmap;
			del(session, mmsRecord);
		}
	} else {
		// �ָ��׶Σ�DRS�����ݲ��ɶ�������ȫ���¼
		record.m_size = m_tableDef->m_maxRecSize;
		record.m_data = (byte *)mc->calloc(record.m_size);
		//�����ȫ���¼һ���Ƕ�����ʽ��䳤��ʽ
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

	// ���ɸ���λͼ
	u32 currDirtyBitmap, currUpdBitmap;

	cols2bitmap(&subRecord, &currUpdBitmap, &currDirtyBitmap);

	// MMS Recover
	nftrace(ts.recv, tout << "needInsert: " << needInsert);
	if (needInsert) {
		// ��MMS����
		int ridNr = getRidPartition(record.m_rowId);
		bool tryAgain;
		MMS_RWLOCK(session->getId(), m_ridLocks[ridNr], Exclusived);
		mmsRecord = put(session, &record, currDirtyBitmap | dirtyBitmap, ridNr, &tryAgain);
	} else {
		mmsRecord->m_dirtyBitmap |= currDirtyBitmap;
		MmsPageOper::mmsRWUNLock(session->getId(), &m_mmsTblLock, Exclusived);
	}
	nftrace(ts.recv, tout << "New dirtyBitmap: " << mmsRecord->m_dirtyBitmap);

	// �ͷ�pin
	unpinRecord(session, mmsRecord);

	mc->resetToSavepoint(sp);
}

/** 
 * ��ȡ�����༶��
 *
 * @param nrRPClass OUT MmsRPClass���鳤��
 * @return MmsRPClass����ָ��
 */
MmsRPClass** MmsTable::getMmsRPClass(int *nrRPClasses) {
	*nrRPClasses = m_mms->m_nrClasses;
	return m_rpClasses;
}

/** 
 * ǿ��ˢд������־
 *
 * @param session �Ự
 */
void MmsTable::flushUpdateLog(Session *session) {
	while (!m_inLogFlush.compareAndSwap(0, 1))
		Thread::msleep(100);
	flushLog(session, true);
}

/************************************************************************/
/*                  ������MMSȫ����ʵ��                                 */
/************************************************************************/

/** 
 * ���캯��
 *
 * @param db ���ݿ����
 * @param targetSize Ŀ���С����λΪҳ��
 * @param pagePool �����ڴ�ҳ��
 * @param needRedo ��Ҫִ��REDO
 * @param replaceRatio �滻�ʣ�������ҳ����/���ҳ���������滻��ʱ������ҳ�滻)
 * @param intervalReplacer �滻�̼߳��ִ��ʱ��(��Ϊ��λ)
 */
Mms::Mms(Database *db, uint targetSize, PagePool *pagePool, bool needRedo, float replaceRatio, int intervalReplacer, float minFPageRatio, float maxFPageRatio) : PagePoolUser(targetSize, pagePool),
	m_mmsLock(db->getConfig()->m_maxSessions + 1, "Mms::lock", __FILE__, __LINE__) {
	// ��ʼ����ͨ����
	m_db = db;
	m_fspConn = NULL;
	m_pagePool = pagePool;
	m_numPagesInUse.set(0);
	m_duringRedo = needRedo;
	m_maxNrDirtyRecs = 512 * 1024; // Ĭ��512K
	m_replaceRatio = replaceRatio;
	m_pgRpcRatio = 1;  // Ĭ��Ϊ1
	m_preRecordQueries = 0;
	m_minFPageRatio = minFPageRatio;
	m_maxFPageRatio = maxFPageRatio;

	// ��ʼ��ͳ����Ϣ
	memset(&m_status, 0, sizeof(MmsStatus));

	// ��ʼ���༶�������Ϣ
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
		// ������һ��size
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

	// ��ʼ����ʱ������
	// m_recPageArray = new Array<MmsRecPage *>();
	// m_dirtyRecArray = new std::vector<MmsRecPair>();

	m_replacer = new MmsReplacer(this, m_db, intervalReplacer * 1000);
	m_replacer->start();
}

/** �رպ�̨�滻�߳� */
void Mms::stopReplacer() {
	if (m_replacer) {
		m_replacer->stop();
		m_replacer->join();
		delete m_replacer;
		m_replacer = NULL;
	}
}

/** 
 * �ر�ȫ��MMS�����ͷ���Դ
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

	// ɾ�����������Ԫ��
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
 * ��MMSȫ�ֶ���ע��һ��MmsTable����
 *
 * @param session �Ự
 * @param mmsTable MmsTable����
 */
void Mms::registerMmsTable(Session *session, MmsTable *mmsTable) {
	assert(mmsTable);

	MMS_RWLOCK(session->getId(), &m_mmsLock, Exclusived); 
	m_mmsTableList.addLast(new DLink<MmsTable *>(mmsTable));
	MmsPageOper::mmsRWUNLock(session->getId(), &m_mmsLock, Exclusived);
}

/** 
 * ����MMS���DELTA��ѯ��
 *
 * @param session �Ự
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
 * ��MMSȫ��ע��һ��MmsTable����
 *
 * @param session �Ự
 * @param mmsTable MmsTable����
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
 * ������ʱ��������¼����
 *
 * @param threshold ���¼����
 */
void Mms::setMaxNrDirtyRecs(int threshold) {
	m_maxNrDirtyRecs = threshold;
}

/** 
 * ��ȡ��ǰ��ʱ��������¼����
 *
 * @return ��ʱ��������¼����
 */
int Mms::getMaxNrDirtyRecs() {
	return m_maxNrDirtyRecs;
}

/** 
 * ��ȡҳ�滻�ʲ���ֵ
 *
 * @return ҳ�滻�ʲ���ֵ
 */
float Mms::getPageReplaceRatio() {
	return m_pgRpcRatio;
}

/** 
 * ����ҳ�滻�ʲ���ֵ������ֵ������[0, 1]֮��
 *
 * @param ratio ����ֵ
 * @return �����Ƿ�ɹ�
 */
bool Mms::setPageReplaceRatio(float ratio) {
	if (ratio < 0 || ratio > 1) return false;
	m_pgRpcRatio = ratio;
	return true;
}

/**
 * ��MMSȫ���ͷ�ҳ
 * ע��MMS����֤ÿ�ε��øú������ͷ�numPages������ҳ
 *
 * @param userId �������û�ID
 * @param numPages ���ͷŵ�ҳ����
 * @return ʵ���ͷŵ�ҳ��
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

/** ��ȡMMSȫ��ͳ����Ϣ
 *
 * @return ͳ�ƽ��
 */
const MmsStatus& Mms::getStatus() {
	m_status.m_recordPages = (u64)m_numPagesInUse.get();
	m_status.m_occupiedPages = getCurrentSize();
	return m_status;
}

/** ��ӡMMSȫ��״̬
 * @param out �����
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
 * ����REDO
 */
void Mms::endRedo() {
	assert(m_duringRedo);
	
	m_duringRedo = false;
}

/** 
 * ���仺��ҳ
 * @pre ������ǰ�̼߳��κμ�¼ҳ��
 * 
 * @param session �Ự����
 * @param external �ⲿ���ã����ڲ��滻�̵߳���)
 * @return ����ҳ, ����ΪNULL
 */
void* Mms::allocMmsPage(Session *session, bool external){
	bool needAssigned;
	u32 ts_min, ts_max;
	u8	numRecs = 1;
	MmsTable *victimTable = NULL;
	MmsRecPage *victimPage = NULL;
	MmsRecPage *recPage;

	SYNCHERE(SP_MMS_ALLOC_PAGE);

	if (!external) { // ���ȴ�PagePool����ҳ
		if (m_duringRedo)
			recPage = (MmsRecPage *)allocPageForce(session->getId(), PAGE_MMS_PAGE, NULL);
		else
			recPage = (MmsRecPage *)allocPage(session->getId(), PAGE_MMS_PAGE, NULL, 50); // ��ȴ�50����
		if (recPage)
			return recPage;
	}

	if (m_duringRedo) { // �ڻָ��׶�
		if (external)
			return NULL;    // ҳ�����������ҳ�滻,��ִ��
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
		// ���Լ�MMS��
		if (!MMS_TRYRWLOCK(session->getId(), &currTable->m_mmsTblLock, Exclusived)) {
			SYNCHERE(SP_MMS_AMP_GET_TABLE_END);
			continue;
		}

		assert(currTable->m_freqHeap); // ��ֹ����MMS���ѱ�close���
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
			//�����ĳͷ���
			victimTable = currTable;
			victimPage = recPage;
		} else {
			MmsPageOper::unlockRecPage(session, this, recPage);
			MmsPageOper::mmsRWUNLock(session->getId(), &currTable->m_mmsTblLock, Exclusived);
		}

		needAssigned = false;
	}
	MmsPageOper::mmsRWUNLock(session->getId(), &m_mmsLock, Shared);

	// �Ҳ����ͷ���
	if (!victimTable) return NULL;

	SYNCHERE(SP_MMS_ALLOC_PAGE_END);

	// ����ҳ�滻
	victimTable->m_existPin.increment();
	victimTable->evictMmsPage(session, victimPage);
	victimTable->m_existPin.decrement();
	MmsPageOper::mmsRWUNLock(session->getId(), &victimTable->m_mmsTblLock, Exclusived);
	m_numPagesInUse.decrement();
	return (void *)victimPage;
}

/** 
 * �����¼��
 *
 * @param session �Ự����
 * @param rpClass ����RPClass
 * @param ridNr �Ѽ�RID��������������
 * @param locked INOUT �Ƿ����RID������
 * @return ��¼����䲻�ɹ�����ΪNULL
 */
MmsRecPage* MmsTable::allocMmsRecord(Session *session, MmsRPClass *rpClass, int ridNr, bool *locked){
	MmsRecPage *recPage;
	MmsRecord *mmsRecord;
	
	*locked = true;
	if (m_mms->m_duringRedo)
		recPage = (MmsRecPage *) m_mms->allocPageForce(session->getId(), PAGE_MMS_PAGE, NULL);
	else if (m_status.m_records < m_maxRecordCount)  
		recPage = (MmsRecPage *) m_mms->allocPage(session->getId(), PAGE_MMS_PAGE, NULL, 0); // ��ȴ�50����
	else // ע����MMS�����м�¼��������ֵʱ��ֹͣ��������¼ҳ
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
		 NTSE_ASSERT(false); // �ڻָ��׶Σ������ܳ����滻���

	MMS_TEST_SET_TASK(m_mms, rpClass, 1);
	SYNCHERE(SP_MMS_RANDOM_REPLACE);

	recPage = rpClass->m_oldestHeap->getPage(0);

	MMSTABLE_TEST_GET_PAGE(this, recPage);
	SYNCHERE(SP_MMS_GET_PIN_WHEN_REPLACEMENT);

	// ��鵱ǰ����������ҳ�ѶѶ���, ע:recPage����ΪNULL
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
	} // return NULL; // �ü����޼�¼ҳ
	if (!MMS_TRYLOCK_REC_PAGE(session, m_mms, recPage)) return NULL; // �������ɹ�
	if (recPage->m_numPins.get()) { // ��ҳ����pin
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

	// ��ȡRID������
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

	// ����ҳ��ɾ����¼��
	MmsPageOper::clearRecordInPage(recPage, mmsRecord);

	if (rpClass->m_numSlots == recPage->m_numFreeSlots) { // �ͷ�ҳΪ��
		// �Ѽ�¼ҳ��ҳ��ɾ��
		assert (recPage->m_numPins.get() == 0);
		rpClass->m_oldestHeap->del(session, recPage->m_oldestHeapIdx, recPage);
	} else {
		// ����ҳ��
		rpClass->m_oldestHeap->moveDown(session, recPage->m_oldestHeapIdx, true, recPage);
	}

	recPage->m_numPins.increment();
	assert(recPage->m_rpClass == rpClass);
	if (!recPage->m_freeLink.getList())
		rpClass->addRecPageToFreeList(recPage);
	return recPage;
}

/** 
 * ��ȡ���滻�ļ�¼ҳ����
 *
 * @return ���滻��¼ҳ����
 */
int Mms::getReplacedPageNum() {
	int pagesCurr = getCurrentSize();
	int	pagesLimit = (int) ((float)getTargetSize() * (1 - m_replaceRatio));

	return pagesCurr > pagesLimit ? pagesCurr - pagesLimit : 0;
}

/** 
 * �ͷ�һ��MMSҳ
 *
 * @param session �Ự
 * @param mmsTable ���ͷ�ҳ�����ı�
 * @param page ���ͷŵ�MMSҳ
 */
void Mms::freeMmsPage(Session *session, MmsTable *mmsTable, void *page) {
	m_numPagesInUse.decrement();
	freePage(session->getId(), page);
	mmsTable->m_status.m_recordPages--;
}

/** 
 * ����MMS���������¼����
 *
 * @param maxRecordCount ����¼����
 */
void MmsTable::setMaxRecordCount(u64 maxRecordCount) {
	m_maxRecordCount = maxRecordCount;
}

/** 
 * ����MMS�������
 *
 * @param cacheUpdate �Ƿ񻺴����
 */
void MmsTable::setCacheUpdate(bool cacheUpdate) {
	m_cacheUpdate = cacheUpdate;
}

/** 
 * ����ӳ���������
 *
 * @param ridNrZone RIDӳ��������
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
 * ����MMSдbinlog�Ļص�����
 *
 * @param mmsCallback	�ص�����
 */
void MmsTable::setBinlogCallback(MMSBinlogCallBack *mmsCallback) {
	m_binlogCallback = mmsCallback;
}

/**
 * ����ѹ����¼��ȡ��
 *
 * @param 
 */
void MmsTable::setCprsRecordExtrator(CmprssRecordExtractor *cprsRcdExtractor) {
	m_cprsRcdExtractor = cprsRcdExtractor;
}

/************************************************************************/
/*					ҳ������ʵ��                                        */
/************************************************************************/

/**
* ��ȡͳ����Ϣ
*
* @return  ͳ�ƽ��
*/
const MmsRPClassStatus& MmsRPClass::getStatus() {
	m_status.m_recordPages = m_oldestHeap->getPagesOccupied();
	return m_status;
}

/** 
* ���ر�����ҳ��洢�ļ�¼����С��С
* ע: ���ڱ䳤��¼�����ڸ��¿��ܻᵼ��ʵ�ʴ洢�ļ�¼��С��СС�ڱ���С
*
* @return ������ҳ�����洢�ļ�¼����С��С(����)
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
* ���ر�����ҳ��洢�ļ�¼������С
*
* @return ������ҳ�����洢�ļ�¼������С(����)
*/
u16 MmsRPClass::getMaxRecSize() {
	return m_slotSize;
}

/** 
* �ͷű�����ҳ��洢������ҳ��
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
 * MmsRPClass�๹�캯��
 *
 * @param mmsTable MMS��
 * @param slotSize ����۴�С
 */
MmsRPClass::MmsRPClass(MmsTable *mmsTable, u16 slotSize) {
	m_mmsTable = mmsTable;
	m_slotSize = slotSize;
	m_numSlots = (u8)(MMS_MAX_RECORD_SIZE / m_slotSize);
	m_oldestHeap = new MmsOldestHeap(mmsTable->m_mms, this, mmsTable->m_freqHeap);

	// ͳ����Ϣ��ʼ��
	memset(&m_status, 0, sizeof(MmsRPClassStatus));
}

/**
 * ��������
 */
MmsRPClass::~MmsRPClass() {
	delete m_oldestHeap;
}

/**
 * ��ȡ���л����¼ҳ
 * 
 * @return �����¼ҳ
 */
MmsRecPage* MmsRPClass::getRecPageFromFreelist() {
	if (m_freeList.isEmpty()) 
		return NULL;
	return m_freeList.getHeader()->getNext()->get();
}

/**
 * �����¼ҳ�ڿ��������У���ɾ����ҳ
 * 
 * @param recPage ��ɾ���ļ�¼ҳ
 */
void MmsRPClass::delRecPageFromFreelist(MmsRecPage *recPage) {
	assert(recPage->m_rpClass == this);

	if (recPage->m_freeLink.getList()) {
		recPage->m_freeLink.unLink();
		m_status.m_freePages--;
	}
}

/**
 * �����ҳ�������һҳ
 * @pre ��¼ҳ�����ڿ�������
 * 
 * @param recPage ����ӵļ�¼ҳ
 */
void MmsRPClass::addRecPageToFreeList(MmsRecPage *recPage) {
	assert(!recPage->m_freeLink.getList());
	assert(recPage->m_rpClass == this);

	m_freeList.getHeader()->addBefore(&recPage->m_freeLink);
	m_status.m_freePages++;
}

/************************************************************************/
/*					������־ˢд��̨�߳�ʵ��                            */
/************************************************************************/

/** 
 * MmsFlushTimer���캯��
 * 
 * @param db ���ݿ�
 * @param mmsTable	������MMS��
 * @param interval	���ʱ��
 */
MmsFlushTimer::MmsFlushTimer(Database *db, MmsTable *mmsTable, uint interval) : BgTask(db, "Mms Flush Timer", interval, false, 1000) {
	m_mmsTable = mmsTable;
} 

/** 
 * ����
 */
void MmsFlushTimer::runIt() {
#ifdef NTSE_UNIT_TEST
	if (!m_mmsTable->m_autoFlushLog)
		return;
#endif
	m_mmsTable->flushLog(m_session);
}

/************************************************************************/
/*					ҳ�滻��̨�߳�ʵ��		                            */
/************************************************************************/

/** 
 * ���캯��
 * 
 * @param mms ȫ��MMS
 * @param db ���ݿ�
 * @param interval	���ʱ��
 */
MmsReplacer::MmsReplacer(Mms *mms, Database *db, uint interval) : BgTask(db, "Mms Replacer", interval, false, 1000) {
	m_mms = mms;
	m_db = db;
} 

/** 
 * ����
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
		m_mms->freePage(m_session->getId(), page); // �ͷż�¼ҳ
		pageCountNeeded--;
	}
	m_db->getSyslog()->log(EL_DEBUG, "%d pages was skipped.", pageCountNeeded);
}

}

