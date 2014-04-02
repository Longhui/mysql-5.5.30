/**
 * MMSҳ����
 *
 * @author �۷�(shaofeng@corp.netease.com, sf@163.org)
 */

#include "mms/Mms.h"
#include "mms/MmsPage.h"
#include "misc/Record.h"

namespace ntse {

/**
 * ��ҳ�����һ�������¼
 *
 * @param recPage ��¼ҳ
 * @param mmsRecord ��¼��
 * @return �Ƿ���Ҫ����ҳ��
 */
bool MmsPageOper::clearRecordInPage(MmsRecPage *recPage, MmsRecord *mmsRecord) {
	bool heapChangeNeeded;

	// �Ƿ���Ҫ����ҳ��
	if (offset2pointer(recPage, recPage->m_lruHead) == (byte *)mmsRecord)
		heapChangeNeeded = true;
	else 
		heapChangeNeeded = false;

	// ��ҳ��LRUɾ����¼
	delFromLruList(recPage, mmsRecord);

	// ��ҳ�����¼��
	setDirty(recPage, mmsRecord, false);

	// ��ռ�¼��
	memset(mmsRecord, 0, recPage->m_slotSize);

	// ��ӵ�ҳ�ڿ�������
	*((u8 *)mmsRecord) = recPage->m_freeList;
	recPage->m_freeList = pointer2offset(recPage, (byte *)mmsRecord);

	// ���ӿ��м�¼�۸���
	recPage->m_numFreeSlots++;

	// ���Ӱ汾��
	recPage->m_version++;

	// ����rpClassͳ����Ϣ
	recPage->m_rpClass->m_status.m_records--;
	recPage->m_rpClass->m_status.m_recordDeletes++;

	return heapChangeNeeded;
}

/**
 * ��ȡ���м�¼��
 * @pre �������Ѽ�ҳ�������ҵ�ǰҳ���ڿ��м�¼��
 *
 * @param recPage �����¼ҳָ��
 * @return ��¼��ָ��
 */
MmsRecord* MmsPageOper::getFreeRecSlot(MmsRecPage *recPage) {
	assert(recPage->m_numFreeSlots > 0);

	byte *tmpByte = offset2pointer(recPage, recPage->m_freeList);

	recPage->m_freeList = *tmpByte;
	recPage->m_numFreeSlots--;
	recPage->m_version++;

	// ����rpClassͳ����Ϣ
	recPage->m_rpClass->m_status.m_records++;
	recPage->m_rpClass->m_status.m_recordInserts++;

	return (MmsRecord *)tmpByte;
}

/** 
 * ��ȡҳ�����/��Сʱ�����ʹ�ü�¼��
 *
 * @param recPage ����ҳ��ַ
 * @param tsMin INOUT ��Сʱ���
 * @param tsMax INOUT ���ʱ���, �������ΪNULL
 * @param numRecs INOUT ҳ����Ч��¼����, �������ΪNULL
 */
void MmsPageOper::getRecPageTs(MmsRecPage *recPage, u32 *tsMin, u32 *tsMax, u8 *numRecs) {
	assert (recPage->m_lruHead);

	MmsRecord *mmsRecord;

	// �������¼����ʱ���
	mmsRecord = (MmsRecord *)offset2pointer(recPage, recPage->m_lruHead);
	*tsMin = mmsRecord->m_timestamp;

	// �������¼����ʱ���
	if (tsMax) {
		mmsRecord = (MmsRecord *)offset2pointer(recPage, recPage->m_lruTail);
		*tsMax = mmsRecord->m_timestamp;
	}

	// ��¼�۸���
	if (numRecs) {
		*numRecs = recPage->m_rpClass->m_numSlots - recPage->m_numFreeSlots;
	}
}

/** 
 * ��ȡĳ��ҳ�ڵ��������¼�� (��Ҫ�жϸ�ҳ�Ƿ�Ϊ��¼ҳ���Լ��Ƿ����ڵ�ǰ����)
 *
 * @param session �Ự
 * @param mms ȫ��MMS����
 * @param rpClass ҳ��
 * @param recPage ��¼ҳ
 * @param tmpRecArray ��ʱ����
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

	// �ǻ����¼ҳ��Ǹ�ҳ�������ļ�¼ҳ
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
 * ��ʼ����¼ҳ
 *
 * @param rpClass ҳ�����
 * @param recPage ��¼ҳָ��
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

	// ��ʼ����������
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
 * �ڻ����¼����д����(��rowId)
 *
 * @param recPage ��¼ҳ
 * @param mmsRecord	��¼��
 * @param record ��¼����
 */
void MmsPageOper::fillRecSlotEx(MmsRecPage *recPage, MmsRecord *mmsRecord, const Record *record) {
	// ��д��¼ͷ
	assert(record->m_size <= (u16)-1); /** ��¼����С��(u16)-1 */
	mmsRecord->m_size = (u16)record->m_size;
	mmsRecord->m_timestamp = System::fastTime();
	mmsRecord->m_updateBitmap = 0;
	mmsRecord->m_dirtyBitmap = 0;
	mmsRecord->m_valid = 1;
	mmsRecord->m_dirty = 0;
	mmsRecord->m_padding = 0;
	mmsRecord->m_compressed = record->m_format == REC_COMPRESSED ? 1 : 0;

	// ��д��¼����
	memcpy((byte *)mmsRecord + sizeof(MmsRecord), record->m_data, record->m_size);
	
	// ��ӵ�ҳ��LRU����ĩβ
	addToLruList(recPage, mmsRecord);
}

/**
 * ����һ����¼
 *				 ------------------------------<------------------------------                                                      
 * ҳ��LRU����	|   -------------	  -------------		 -------------        |
 *				--->|  lruHead  | --> |   lruNext |  --> |   lruNext |---------   
 *					------------- 	  -------------		 -------------              ���1�ֽ�
 *				----|  lruTail  | <-- |   lruPrev | <--  |   lruPrev |<--------   
 *			   |	-------------     -------------      -------------        |
 *			   --------------------------- > ---------------------------------- 
 *
 * @param recPage ��¼ҳ
 * @param mmsRecord ��¼��
 * @return ���ϻ���ҳ���Ƿ���Ҫ����
 */
bool MmsPageOper::touchRecord(MmsRecPage *recPage, MmsRecord *mmsRecord) {
	assert(mmsRecord->m_valid);

	bool heapChangeNeeded = false;
	byte *tmpByte;

	// ���ļ�¼��ʱ���
	u32 oldTimestamp = mmsRecord->m_timestamp;
	mmsRecord->m_timestamp = System::fastTime();

	// �жϵ�ǰ��¼���Ƿ�ΪLRU��������
	tmpByte = offset2pointer(recPage, recPage->m_lruHead);
	if (tmpByte == (byte *)mmsRecord)
		heapChangeNeeded = true;

	// �Ѽ�¼��LRU����ɾ��
	delFromLruList(recPage, mmsRecord);

	// ��ӵ�ĩβ
	addToLruList(recPage, mmsRecord);

	// �����LRU��ͷ��¼��ʱ���Tnew���LRU��ͷ��¼��ʱ���Told֮��Ĳ�ֵС��2�룬����Ҫ����ҳ��
	if (heapChangeNeeded) {
		tmpByte = offset2pointer(recPage, recPage->m_lruHead);
		if (((MmsRecord *)tmpByte)->m_timestamp - oldTimestamp < 1)
			heapChangeNeeded = false;
	}

	return heapChangeNeeded;
}

/**
 * ��ȡFPageֵ
 * FPageֵ���㹫ʽΪ�� FPage = (Fmin + Fmax) / 2
 * ���� F ��ʾ��̬Ƶ�ʣ����㷽ʽΪ F = 1 / (��ǰʱ��� �� ��¼ʱ�����
 *      N ��ʾ��¼��
 *
 * @param tsCurr ��ǰʱ���
 * @param tsMin ���ϼ�¼��ʱ���
 * @param tsMax ���¼�¼��ʱ���
 * @return FPageֵ
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
 * �жϼ�¼ҳ�Ƿ�Ϊ��ͷ���Ƶ��ҳ
 *
 * @param recPage ��¼ҳ
 * @param tsMin INOUT ����ʱ���
 * @param tsMax INOUT ����ʱ���
 * @param numRecs INOUT ��¼����
 * @param assigned ��ֵ����
 * @param victimTable �ͷ���
 * @param pgRatio ҳ�滻�ʣ�[0, 1]֮��������
 * @param ext ��չ�汾����ǿ�滻�㷨
 * @return �Ƿ�Ϊ��ͷ���Ƶ��ҳ
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
	if (ext) {// FPage = FPage �� { ��ѯ���� / ��¼ҳռ�ñ��� }�����η���
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

	// �����¼��ͼ�¼ҳ֮��Ƚϣ��迼�Ǽ�¼ҳ�滻Ȩ�ز���
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
