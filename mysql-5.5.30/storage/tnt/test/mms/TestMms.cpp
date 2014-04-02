/**
* MMS����
*
* @author �۷�(shaofeng@corp.netease.com, sf@163.org)
*/

#include <iostream>
#include "mms/TestMms.h"
#include "Test.h"
#include "api/Database.h"
#include "api/Table.h"
#include "misc/RecordHelper.h"
#include "util/File.h"
#include "util/Thread.h"
#include "mms/MmsPage.h"
#include "heap/TestFLRHeap.h"
#include <string>

#ifdef NTSE_UNIT_TEST

using namespace std;

#define FIX_TBL_HEAP "fixheap.nsd"
#define FIX_TBL_DEF "fixtbldef.nstd"
#define VAR_TBL_HEAP "varheap.nsd"
#define VAR_TBL_DEF "vartbldef.nstd"
#define HUN_TBL_HEAP "hunheap.nsd"
#define HUN_TBL_DEF "huntbldef.nstd"
#define VAR2_TBL_HEAP "var2heap.nsd"
#define VAR2_TBL_DEF "var2tbldef.nstd"

#define FIX_TBL_HEAP_BACKUP "fixheap.bak"
#define VAR_TBL_HEAP_BACKUP "varheap.bak"
#define HUN_TBL_HEAP_BACKUP	"hunheap.bak"

#define FIX_TBL_TID	1
#define VAR_TBL_TID 2
#define HUN_TBL_TID 3

const char* MmsTestCase::getName() {
	return "Mms test";
}

const char* MmsTestCase::getDescription() {
	return "Test various operations of MMS module";
}

bool MmsTestCase::isBig() {
	return false;
}

void MmsTestCase::setUp() {
	init(100, true, true);
}

void MmsTestCase::tearDown() {
	close();
}

/** 
 * �رղ�������
 *
 * @param delHeapFile �Ƿ�ɾ�����ļ�
 */
void MmsTestCase::close(bool delHeapFile, bool flush) {
	if (m_fMmsTable) {
		EXCPT_OPER(m_fMmsTable->drop(m_session, delHeapFile, flush));
		delete m_fMmsTable;
		m_fMmsTable = NULL;
	}
	if (m_vMmsTable) {
		EXCPT_OPER(m_vMmsTable->drop(m_session, delHeapFile, flush));
		delete m_vMmsTable;
		m_vMmsTable = NULL;
	}
	if (m_vMmsTable2) {
		EXCPT_OPER(m_vMmsTable2->drop(m_session, delHeapFile, flush));
		delete m_vMmsTable2;
		m_vMmsTable2 = NULL;
	}
	if (m_mms) {
		m_pagePool->preDelete();
		EXCPT_OPER(m_mms->close());
		delete m_mms;
		m_mms = NULL;
	}
	if (m_session) {
		m_db->getSessionManager()->freeSession(m_session);
		m_session = NULL;
	}
	if (m_conn) {
		m_db->freeConnection(m_conn);
		m_conn = NULL;
	}
	if (m_db) {
		m_db->close(true, flush);
		if (delHeapFile)
			Database::drop(".");
		delete m_db;
		m_db = NULL;
	}

	if (m_config) {
		delete m_config;
		m_config = NULL;
	}

	if (m_pagePool) {
		delete m_pagePool;
		m_pagePool = NULL;
	}
}

/** 
 * Mms::unregister�������ܲ���
 * �������̣�1. ��������MmsTable t1��t2��t3
 *			 2. ��ȫ��MMS��ע��t1��t2��t3
 *			 3. ��ȫ��MMS��ע��t2��t3
 *			 4. ע��t1��ע����Mms::close�׶����
 */
void MmsTestCase::testMms() {
	// ����
	MmsTable *t1 = new MmsTable(m_mms, m_db, m_fMmsTable->getHeap(), m_fMmsTable->getTableDef(), false, 100);
	MmsTable *t2 = new MmsTable(m_mms, m_db, m_fMmsTable->getHeap(), m_fMmsTable->getTableDef(), false, 100);
	MmsTable *t3 = new MmsTable(m_mms, m_db, m_fMmsTable->getHeap(), m_fMmsTable->getTableDef(), false, 100);

	m_mms->registerMmsTable(m_session, t1);
	m_mms->registerMmsTable(m_session, t2);
	m_mms->registerMmsTable(m_session, t3);

	m_mms->unregisterMmsTable(m_session, t2);
	m_mms->unregisterMmsTable(m_session, t3);

	t1->close(m_session, false);
	delete t1;
	t2->close(m_session, false);
	delete t2;
	t3->close(m_session, false);
	delete t3;
}

/** 
 * MmsTable::testPutIfNotExist�������ܲ���
 * �������̣� 1. FTable�̶����в���NR_RECORDS����¼�����ڲ���put����
 *			  2. FTable�̶�������������NR_RECORDS����¼�����ڲ���IfNotExist����
 *			  3. VTable�䳤���в���NR_RECORDS����¼
 */
void MmsTestCase::testPutIfNotExist() {
	// ������¼
	for(int i = 0; i < NR_RECORDS; i++) {
		m_fMmsTable->insert(m_session, i, false);
	}
	
	// ����putIfNotExist��Exist���
	m_fMmsTable->insertDouble(m_session, NR_RECORDS, false);

	// �䳤��¼
	char s[100];

	for (int i = 0; i < NR_RECORDS; i++) {
		sprintf(s, "%d", i);
		m_vMmsTable->insert(m_session, i, s, (int)strlen(s), false);
	}
}

/** 
 * MmsTable::getByRid�������ܲ���
 * �������̣� 1. FTable�̶������һ����¼
 *			  2. RowID��ѯMmsTable
 *			  3. ���ݲ�ѯ��ȡ��MmsRecord, ����SubRecord (TODO:�Ƚ�����һ���ԣ�
 *			  4. unpin��¼ҳ��unlock����
 */
void MmsTestCase::testGetByRid() {
	MmsRecord *mmsRecord;
	SubRecord subRecord;
	RowLockHandle *rlh = NULL;

	EXCPT_OPER(m_fMmsTable->insert(m_session, 10, false));
	EXCPT_OPER(mmsRecord = m_fMmsTable->selectByRid(m_session, 10, &rlh, Exclusived));
	SubrecExtractor *extractor = SubrecExtractor::createInst(NULL, m_fMmsTable->getTableDef(), subRecord.m_numCols, subRecord.m_columns,
		m_fMmsTable->getTableDef()->m_recFormat, REC_REDUNDANT);
	EXCPT_OPER(m_fMmsTable->getMmsTable()->getSubRecord(mmsRecord, extractor, &subRecord));
	delete extractor;
	EXCPT_OPER(m_fMmsTable->getMmsTable()->unpinRecord(m_session, mmsRecord));
	EXCPT_OPER(m_session->unlockRow(&rlh));
}

/** 
 * getSubRecord�������ܲ���
 */
void MmsTestCase::testGetSubRecord() {
	SubRecord subRecord;
	RowId rid = 10;

	subRecord.m_format = REC_REDUNDANT;
	subRecord.m_size = Limits::PAGE_SIZE;
	subRecord.m_data = (byte *)malloc(subRecord.m_size);
	subRecord.m_numCols = 2;
	subRecord.m_columns = (u16 *)malloc(sizeof(u16) * subRecord.m_numCols);
	for (int i = 0; i < subRecord.m_numCols; i++)
		subRecord.m_columns[i] = i;
	m_fMmsTable->insert(m_session, rid, false);
	m_fMmsTable->getMmsTable()->getTableLock();
	SubrecExtractor *extractor = SubrecExtractor::createInst(NULL, m_fMmsTable->getTableDef(),
		subRecord.m_numCols, subRecord.m_columns, m_fMmsTable->getTableDef()->m_recFormat, REC_REDUNDANT);
	CPPUNIT_ASSERT(!m_fMmsTable->getMmsTable()->getSubRecord(m_session, rid + 1, extractor, &subRecord, true, false, 0));
	CPPUNIT_ASSERT(m_fMmsTable->getMmsTable()->getSubRecord(m_session, rid, extractor, &subRecord, true, true, 0));
	CPPUNIT_ASSERT(m_fMmsTable->getMmsTable()->getSubRecord(m_session, rid, extractor, &subRecord, true, false, 0));
	free(subRecord.m_data);
	free(subRecord.m_columns);
	delete extractor;
}

/** 
 *  MmsTable::getRecord�������ܲ���
 */
void MmsTestCase::testGetRecord() {
	m_fMmsTable->getRecord(m_session, 1, true);
	testGetSubRecord();
	m_fMmsTable->getMmsTable()->setCacheUpdate(true);
	m_fMmsTable->getMmsTable()->setMaxRecordCount(100);
	m_mms->printStatus(cout);
}

/** 
 * MmsTable::update�������ܲ���
 */
void MmsTestCase::testUpdate() {
	doTestUpdate(true);
}

/** 
 * update�������Թ���ʵ��
 * �������̣� 1. ��FTable�������в���count����¼��ÿ����¼����step��
 *			  2. ��VTable�䳤���в���count/2����¼��ÿ����¼����101�� (touch����ΪTRUE)
 *			  3. ��VTable�䳤���в���count/2����¼��ÿ����¼����101�� (touch����ΪFALSE)
 *
 * @param insertHeap �Ƿ��ڶ��ϲ������� (�ڲ���MMS���������У����Բ�������д������)
 * @param count	������
 */
void MmsTestCase::doTestUpdate(bool insertHeap, int count) {
	const int step = 10;
	RowLockHandle *rlh = NULL;
	RowId rowId;
	int i, j;
	
	for (i = 0; i < count; i+=step) {
		EXCPT_OPER(rowId = m_fMmsTable->insert(m_session, i, insertHeap));
		for (j = i; j < i + step; j++) {
			EXCPT_OPER(m_fMmsTable->update(m_session, rowId, j));
		}
	}

	// �䳤��¼
	char s[1024];

	for (i = 0; i < count / 2; i++) {
		sprintf(s, "%d", i);
		EXCPT_OPER(rowId = m_vMmsTable->insert(m_session, i, s, (int)strlen(s), insertHeap));
		for (j = 0; j < 10; j++) {
			sprintf(s, "%d%s", i, s);
			m_vMmsTable->update(m_session, rowId, s);
		}
		sprintf(s, "%d", i);
		m_vMmsTable->update(m_session, rowId, s);
	}

	for (; i < count; i++) {
		sprintf(s, "%d", i);
		EXCPT_OPER(rowId = m_vMmsTable->insert(m_session, i, s, (int)strlen(s), insertHeap));
		for (j = 0; j < 10; j++) {
			sprintf(s, "%d%s", i, s);
			m_vMmsTable->update(m_session, rowId, s, false);
		}
		sprintf(s, "%d", i);
		m_vMmsTable->update(m_session, rowId, s, false);
	}
}

void MmsTestCase::testHeapMove() {
	RowId fid[500];
	RowId vid[500];

	fid[0] = m_vMmsTable->insert(m_session, 0, "a", 1, true);
	Thread::msleep(3000);  // �ȴ�3��
	for (int i = 0; i < 1; i++) {
		char name[40];
		for (int j = 0; j < 40; j++)
			name[j] = 'a';
		name[39] = '\0';
		vid[i] = m_vMmsTable->insert(m_session, i, name, 40, true);
	}
	for (int i = 0; i < 1; i++) {
		char name[80];
		for (int j = 0; j < 80; j++)
			name[j] = 'a';
		name[79] = '\0';
		vid[i] = m_vMmsTable->insert(m_session, i, name, 80, true);
	}
	for (int i = 0; i < 1; i++) {
		char name[160];
		for (int j = 0; j < 160; j++)
			name[j] = 'a';
		name[159] = '\0';
		vid[i] = m_vMmsTable->insert(m_session, i, name, 160, true);
	}
	for (int i = 1; i < 500; i++)
		fid[i] = m_vMmsTable->insert(m_session, i, "a", 1, true);
	Thread::msleep(3000);  // �ȴ�3��
	MmsRecord *mmsRecord;
	for (int i = 0; i < 500; i++) {
		mmsRecord = m_vMmsTable->getMmsTable()->getByRid(m_session, fid[i], true, NULL, None);
		m_vMmsTable->getMmsTable()->unpinRecord(m_session, mmsRecord);
	}
}

void MmsTestCase::doTestUpdate_simple(bool insertHeap, int count) {
	const int step = 10;
	RowLockHandle *rlh = NULL;
	RowId rowId;
	int i, j;

	for (i = 0; i < count; i+=step) {
		EXCPT_OPER(rowId = m_fMmsTable->insert(m_session, i, insertHeap));
		for (j = i; j < i + step; j++) {
			EXCPT_OPER(m_fMmsTable->update(m_session, rowId, j));
		}
	}

	// �䳤��¼
	char s[1024];

	for (i = 0; i < count / 2; i++) {
		sprintf(s, "%d", i);
		EXCPT_OPER(rowId = m_vMmsTable->insert(m_session, i, s, (int)strlen(s), insertHeap));
		for (j = 0; j < 2; j++) {
			sprintf(s, "%d%s", i, s);
			m_vMmsTable->update(m_session, rowId, s);
		}
		sprintf(s, "%d", i);
		m_vMmsTable->update(m_session, rowId, s);
	}

	for (; i < count; i++) {
		sprintf(s, "%d", i);
		EXCPT_OPER(rowId = m_vMmsTable->insert(m_session, i, s, (int)strlen(s), insertHeap));
		for (j = 0; j < 2; j++) {
			sprintf(s, "%d%s", i, s);
			m_vMmsTable->update(m_session, rowId, s, false);
		}
		sprintf(s, "%d", i);
		m_vMmsTable->update(m_session, rowId, s, false);
	}
}


/** 
 * update����������չ����ʵ��
 * �������̣�1. VTable�䳤���в���100����¼
 *			 2. ÿ����¼����101�Σ�touch����ΪFALSE)
 */
void MmsTestCase::testUpdateEx() {
	RowId rid[100];
	char s[1024];

	for (int i = 0; i < 100; i++) {
		sprintf(s, "%d", i);
		EXCPT_OPER(rid[i] = m_vMmsTable->insert(m_session, i, s, (int)strlen(s), true));
	}

	for (int i = 0; i < 100; i++) {
		sprintf(s, "%d", i);
		for (int j = 0; j < 100; j++)
			sprintf(s, "%d%s", i, s);
		m_vMmsTable->update(m_session, rid[i], s, false);
	}
}

/** 
 * FreqHeap����
 * �������̣�1. ��VTable�䳤���в���90����¼(ͬһ��RpClass)
 *		     2. �ٲ���2����¼����һ��RpClass��
 *			 3. RowID��ѯ��һ��RpClass�����еļ�¼��
 */
void MmsTestCase::testFreqHeap() {
	char s[1024];
	RowId rid[100];
	MmsRecord *mmsRecord;

	// һ��RPCLASS
	for (int i = 10; i < 100; i++) {
		sprintf(s, "%d", i);
		EXCPT_OPER(rid[i] = m_vMmsTable->insert(m_session, i, s, (int)strlen(s), true)); 
	}

	// ��һ��RPCLASS
	for (int i = 100; i < 102; i++) {
		sprintf(s, "%d", i);
		for (int j = 0; j < 50; j++) {
			sprintf(s, "%d%s", i, s);
		}
		EXCPT_OPER(m_vMmsTable->insert(m_session, i, s, (int)strlen(s), true)); 
	}

	for (int i = 10; i < 100; i++) {
		EXCPT_OPER(mmsRecord = m_vMmsTable->selectByRid(m_session, rid[i], NULL, None));
		CPPUNIT_ASSERT(mmsRecord);
		m_vMmsTable->getMmsTable()->unpinRecord(m_session, mmsRecord);
	}
}

/** 
 * ����MMS��¼ɾ�����ܼ�get��ع���
 * �������̣�(I)  ������¼�����
 *			 1. �������в���NR_RECORDS����¼
 *			 2. ��ѯMMS������ĵ�ǰ��¼��
 *			 3. ��ѯ(ʹ��getByRid)��ɾ����������50%��¼��, ÿ��ɾ��������Ƿ�ɹ�ɾ���ж�(ʹ��getByPK)
 *			 4. ��ѯ(ʹ��getByPK)��ɾ��������������50%��¼��, ÿ��ɾ��������Ƿ�ɹ�ɾ���ж�(ʹ��getByRid)
 *			 5. ��ѯMMS������ĵ�ǰ��¼����ɾ���ļ�¼�����������ж�
 *			 (II) �䳤��¼�����
 *			 1. �䳤���в���NR_RECORDS����¼
 *			 2. ��ѯMMS�䳤��ĵ�ǰ��¼��
 *			 3. ��ѯ(ʹ��getByRid)��ɾ���䳤����50%��¼��, ÿ��ɾ��������Ƿ�ɹ�ɾ���ж�(ʹ��getByPK)
 *			 4. ��ѯ(ʹ��getByPK)��ɾ���䳤��������50%��¼��, ÿ��ɾ��������Ƿ�ɹ�ɾ���ж�(ʹ��getByRid)
 *			 5. ��ѯMMS�䳤��ĵ�ǰ��¼����ɾ���ļ�¼�����������ж�
 *			 (III) ������¼��get����Touch���ܲ���
 *			 1. �������в���NR_RECORDS����¼
 *			 2. ��ѯ(ʹ��getByRid��touch����Ϊfalse)��ɾ�������������м�¼�
 */
void MmsTestCase::testDel() {
	MmsRecord *mmsRecord;
	RowLockHandle *rlh = NULL;
	u64 numDels = 0;
	u64 currMmsRecords = 0;

	// ���������
	for(u64 i = 0; i < NR_RECORDS; i++) {
		m_fMmsTable->insert(m_session, i, false);	
	}

	currMmsRecords = m_fMmsTable->getMmsTable()->getStatus().m_records;
	for (u64 j = 0; j < NR_RECORDS; j++) {
		EXCPT_OPER(mmsRecord = m_fMmsTable->selectByRid(m_session, j, &rlh, Exclusived));
		if (mmsRecord) {
			// TODO���ڵ���mmsTable.del֮ǰ���Ƿ����Ӽ�¼��
			EXCPT_OPER(m_session->unlockRow(&rlh));
			EXCPT_OPER(m_fMmsTable->getMmsTable()->del(m_session, mmsRecord));
			numDels++;
			// CPPUNIT_ASSERT(!m_fMmsTable->selectByPK(m_session, j, &rlh, Exclusived));
		}
	}
	CPPUNIT_ASSERT(numDels == currMmsRecords);
	CPPUNIT_ASSERT(!m_fMmsTable->getMmsTable()->getStatus().m_records);

	// �䳤��¼����
	char s[100];
	
	numDels = 0;
	for (u64 i = 0; i < NR_RECORDS; i++) {
		sprintf(s, "%d", i);
		m_vMmsTable->insert(m_session, i, s, (int)strlen(s), false);
	}
	currMmsRecords = m_vMmsTable->getMmsTable()->getStatus().m_records;
	
	for (u64 j = NR_RECORDS - 1; (int)j >= 0; j--) {
		EXCPT_OPER(mmsRecord = m_vMmsTable->selectByRid(m_session, j, &rlh, Exclusived));
		if (mmsRecord) {
			EXCPT_OPER(m_session->unlockRow(&rlh));
			EXCPT_OPER(m_vMmsTable->getMmsTable()->del(m_session, mmsRecord));
			numDels++;
		}
	}
	CPPUNIT_ASSERT(numDels == currMmsRecords);
	CPPUNIT_ASSERT(!m_vMmsTable->getMmsTable()->getStatus().m_records);

	for(u64 i = 0; i < NR_RECORDS; i++) {
		m_fMmsTable->insert(m_session, i, false);	
	}

	currMmsRecords = m_fMmsTable->getMmsTable()->getStatus().m_records;
	for (u64 j = 0; j < NR_RECORDS; j++) {
		EXCPT_OPER(mmsRecord = m_fMmsTable->getMmsTable()->getByRid(m_session, j, false, &rlh, Exclusived));
		if (mmsRecord) {
			// TODO���ڵ���mmsTable.del֮ǰ���Ƿ����Ӽ�¼��
			EXCPT_OPER(m_session->unlockRow(&rlh));
			EXCPT_OPER(m_fMmsTable->getMmsTable()->del(m_session, mmsRecord));
			numDels++;
		}
	}
}

/** 
 * ����MMS����ˢ�¹���
 * �������̣� 1. ����doTestUpdate����������һ��MMS���²���
 *			  2. ��󻺳������¼����Ϊ50(ˢдʱ,�ڻ���������sortAndFlush)
 *			  3. ���������ˢ��
 *			  4. �䳤�����ˢ��
 */
void MmsTestCase::testCheckPointFlush() {
	doTestUpdate(true, 100);
	assert (m_mms->getMaxNrDirtyRecs() > 0);
	m_mms->setMaxNrDirtyRecs(6);
	EXCPT_OPER(m_fMmsTable->getMmsTable()->flush(m_session, true));
	EXCPT_OPER(m_vMmsTable->getMmsTable()->flush(m_session, true));
}

/** 
 * ���������µĸ��¹���
 * �������̣�1. MMS����Ϊ���������
 *			 2. ִ��doTestUpdate���� (���ڼ����ڲ��������״̬�£�MMS���¹���)
 *			 3. MMS����Ϊ���»��沢��Ϊ�����и��»���
 *			 4. ִ��doTestUpdate���� (���ڼ����������и��»���״̬�£�MMS���¹���)
 */
void MmsTestCase::testUpdateWhenVariousSet() {
	if (!isEssentialOnly()) {
		close();
		
		init(1000, false, false);
		doTestUpdate(true);
		close();

		init(1000, true, true);
		doTestUpdate(true);
		close();
	}
}

/** 
 * ���Ի�ȡRpClass��¼�߽��С�Ķ���ӿں���
 */
void MmsTestCase::testRpClassRecSize() {
	RowId rid = 1;

	m_fMmsTable->insert(m_session, rid, false);
	MmsTable *mmsTable = m_fMmsTable->getMmsTable();
	int nrRPClasses;
	MmsRPClass** rpClasses = mmsTable->getMmsRPClass(&nrRPClasses);
	u16 sizeMax, sizeMin;

	for (int i = 0; i < nrRPClasses; i++) {
		if (rpClasses[i]) {
			sizeMax = rpClasses[i]->getMaxRecSize();
			sizeMin = rpClasses[i]->getMinRecSize();
			break;
		}
	}
	MmsRecord *mmsRecord = mmsTable->getByRid(m_session, rid, true, NULL, None);
	CPPUNIT_ASSERT(mmsRecord->m_size + sizeof(MmsRecord) <= sizeMax);
	CPPUNIT_ASSERT(mmsRecord->m_size + sizeof(MmsRecord) >= sizeMin);
	mmsTable->unpinRecord(m_session, mmsRecord);
}

/** 
 * ����Mms::freeSomePages()��������
 * �������̣� 1. ����testDel, ִ��һ��ɾ������
 *			  2. ����Mms::freeSomePages, �ͷ�5����¼ҳ
 *			  3. ����testPutIfNotExist, ִ��һ��������
 *			  4. ����Mms::freeSomePages, �ٴ��ͷ�5����¼ҳ
 */
void MmsTestCase::testFreeSomePages() {
	testDel();
	m_mms->freeSomePages(0, 5);
	testPutIfNotExist();
	m_mms->freeSomePages(0, 5);
}

/** REDO�������Ͷ��� */
enum MMS_REDO_TEST_TYPE {
	MMS_REDO_TEST_SINGLE_FIX = 1,    /** ������¼����־ */
	MMS_REDO_TEST_MULT_FIX,          /** ������¼����־ */
	MMS_REDO_TEST_MULT_VAR           /** �䳤��¼����־ */
};

/** 
 * ����MMS�ָ�����
 * �������̣� 1. ������¼����־����
 *			  2. ������¼����־����
 *			  3. �䳤��¼����־����
 */
void MmsTestCase::testRecover() {
	//testRedoUpdate(MMS_REDO_TEST_SINGLE_FIX); ȥ��single fix	
	testRedoUpdate(MMS_REDO_TEST_MULT_FIX);
	testRedoUpdate(MMS_REDO_TEST_MULT_VAR);
}

/** 
 * ����MMS�ָ�����
 * �������̣�1. ���ݲ�ͬ���Ͳ���������MMS����
 *			 2. ����������
 *		     3. ������־�ļ�������Mms::redoUpdate�ع�MMS����
 *			 4. �жϻָ���ȷ��
 *
 * @param cond ��������
 */
void MmsTestCase::testRedoUpdate(int cond) {
	const int step = 10;
	RowLockHandle *rlh = NULL;
	RowId rowId;
	RowId rowIds[100];

	// ��������
	if (MMS_REDO_TEST_SINGLE_FIX == cond) {
		for (int i = 0; i < 99; i+=step) {
			EXCPT_OPER(rowIds[i] = m_fMmsTable->insertReal(m_session, i, true));
			for (int j = i; j < i + step; j++) {
				EXCPT_OPER(m_fMmsTable->updateReal(m_session, rowIds[i], j));
			}
		}
	} else if (MMS_REDO_TEST_MULT_FIX == cond) {
		close();
		init(100, false, false);
		EXCPT_OPER(rowId = m_fMmsTable->insertReal(m_session, 0, true));
		EXCPT_OPER(m_fMmsTable->updateReal(m_session, rowId, 1));
		EXCPT_OPER(m_fMmsTable->updateRealEx(m_session, rowId, 2));
	} else if (MMS_REDO_TEST_MULT_VAR == cond) {
		char s[1024];
		
		close();
		init(100, false, false);
		int i = 0;
		sprintf(s, "%d", i);
		EXCPT_OPER(rowId = m_vMmsTable->insertReal(m_session, i, s, (int)strlen(s), true));
		for (int j = 0; j < 10; j++) {
			sprintf(s, "%d%s", i, s);
			m_vMmsTable->updateReal(m_session, rowId, s);
		}
		sprintf(s, "%d", i);
		m_vMmsTable->updateReal(m_session, rowId, s);
	}

	// ����������
	m_fMmsTable->getHeap()->getBuffer()->flushAll();
	doBackupFile(m_fMmsTable->getHeap()->getHeapFile(), FIX_TBL_HEAP_BACKUP);
	m_fMmsTable->getMmsTable()->disableAutoFlushLog();
	m_fMmsTable->getMmsTable()->flushMmsLog(m_session);
	close(false, false);

	// �ָ�
	doRestoreFile(FIX_TBL_HEAP_BACKUP, FIX_TBL_HEAP);
	init(1000, true, true, true, false);
	m_fMmsTable->getMmsTable()->disableAutoFlushLog();
	m_fMmsTable->getMmsTable()->flushMmsLog(m_session);   // ģ��REDO�׶εļ��ˢ��

	LogScanHandle *logHdl = m_db->getTxnlog()->beginScan(Txnlog::MIN_LSN, Txnlog::MAX_LSN);
	while (m_db->getTxnlog()->getNext(logHdl)) {
		u64 lsn = logHdl->curLsn();
		const LogEntry *le = logHdl->logEntry();
		switch (le->m_logType) {
			case LOG_MMS_UPDATE:
				if (le->m_tableId == FIX_TBL_TID)
					m_fMmsTable->getMmsTable()->redoUpdate(m_session, le->m_data, (uint)le->m_size);
				else if (le->m_tableId == VAR_TBL_TID)
					m_vMmsTable->getMmsTable()->redoUpdate(m_session, le->m_data, (uint)le->m_size);
				break;
			default:
				break;
		}
	}
	m_db->getTxnlog()->endScan(logHdl);
	m_mms->endRedo();

	// �ж�
	if (MMS_REDO_TEST_SINGLE_FIX == cond) {
		for (int i = 0; i < 99; i+=step) {
			u64 j = i + step - 1;
			CPPUNIT_ASSERT(NULL != m_fMmsTable->selectByRid(m_session, rowIds[i], NULL, None));
		}
	} else if (MMS_REDO_TEST_MULT_FIX == cond) {
		CPPUNIT_ASSERT(NULL != m_fMmsTable->selectByRid(m_session, rowId, NULL, None));
	} else if (MMS_REDO_TEST_MULT_VAR == cond) {
		CPPUNIT_ASSERT(NULL != m_vMmsTable->selectByRid(m_session, rowId, NULL, None));
	}
}

/** 
 * ���챸���ļ� (����TestFLRHeap::backupHeapFileʵ��)
 *
 * @param file �ļ����
 * @param backupName �����ļ���
 */
void MmsTestCase::doBackupFile(File *file, const char *backupName) {
	backupHeapFile(file, backupName);
}

/** 
 * Restore�ļ� (����TestFLRHeap::restoreHeapFileʵ��)
 *
 * @param backupFile �����ļ�
 * @param origFile ԭʼ�ļ�
 */
void MmsTestCase::doRestoreFile(const char *backupFile, const char *origFile) {
	restoreHeapFile(backupFile, origFile);
}

/** 
 * ����MMS�ڲ��滻����
 * ���Թ��ܣ� 1. MMS����Ϊ���и����л���
 *			  2. �������в���1000����¼
 *			  3. �䳤���в���100����¼
 */
void MmsTestCase::testReplacement() {
	this->close();
	
	this->init(10, true, true);
	for (int i = 0; i < 1000; i++)
		m_fMmsTable->insert(m_session, i, true);

	char username[1000];
	for (int i = 0; i < 100; i++) {
		sprintf(username, "%d", i);
		m_vMmsTable->insert(m_session, i, username, (int)strlen(username), true);
	}

	testReplaceThread();

	testFPage();

	testAllocFail();
}

/** 
 * ����MMS�ڲ��滻�̹߳���
 */
void MmsTestCase::testReplaceThread() {
	this->close();
	this->init(10, true, true);
	for (int i = 0; i < 1000; i++)
		m_fMmsTable->insert(m_session, i, true);
	char username[1000];
	for (int i = 0; i < 100; i++) {
		sprintf(username, "%d", i);
		m_vMmsTable->insert(m_session, i, username, (int)strlen(username), true);
	}
	m_mms->runReplacerForce();
}

void MmsTestCase::testUpdateWhenAllocFail() {
	if (isEssentialOnly())
		return;

	close();
	init(10, true, true);
	// ����һ���䳤���¼
	char username[1000];
	username[0] = 'a';
	username[1] = '\0';

	RowId rid = m_vMmsTable->insertReal(m_session, 0, username, (int)strlen(username), true);
	
	for (int i = 0; i < 1000; i++)
		if ((RowId)-1 == m_fMmsTable->insertEx(m_session, i, true))
			break;

	char *updateName = new char[100];
	memset(updateName, 1, 99);
	*(updateName + 99) = '\0';
	const char *cols = "1";
	SubRecord *subRecord;
	SubRecordBuilder srb(m_vMmsTable->getTableDef(), REC_REDUNDANT, rid);

	subRecord = srb.createSubRecordById(cols, (const char *)updateName);
	
	MmsRecord *mmsRecord = m_vMmsTable->getMmsTable()->getByRid(m_session, rid, true, NULL, None);
	MmsTable *mmsTable = m_vMmsTable->getMmsTable();
	u16 recSize;
	if (mmsTable->canUpdate(mmsRecord, subRecord, &recSize))
		mmsTable->update(m_session, mmsRecord, subRecord, recSize);
	else
		mmsTable->flushAndDel(m_session, mmsRecord);
	delete [] updateName;
	freeSubRecord(subRecord);
}

void MmsTestCase::testRidHashConflict() {
	for(u64 i = 0; i < NR_RECORDS; i++) {
		m_fMmsTable->insert(m_session, i, false);	
	}

	MmsRecord *mmsRecord;
	for (u64 j = 0; j < NR_RECORDS; j++) {
		EXCPT_OPER(mmsRecord = m_fMmsTable->selectByRid(m_session, j, NULL, None));
		m_fMmsTable->getMmsTable()->unpinRecord(m_session, mmsRecord);
	}

	int numParts = m_fMmsTable->getMmsTable()->getPartitionNumber();
	double avgConflictLen;
	size_t maxConflictLen;
	for (int i = 0; i < numParts; i++) {
		m_fMmsTable->getMmsTable()->getRidHashConflictStatus(i, &avgConflictLen, &maxConflictLen);
	}
}

/** 
 * MMS�ڲ���ʼ��
 * 
 * @param targetSize	Ŀ���С
 * @param cacheUpdate	�������
 * @param colUpdate		�ֶθ���
 */
void MmsTestCase::init(uint targetSize, bool cacheUpdate, bool colUpdate, bool needRedo, bool delHeapFile, bool varcharPK, bool partialCached, int partitionNr) {
	if (delHeapFile) {
		Database::drop(".");
	}

	m_config = new Config();
	EXCPT_OPER(m_db = Database::open(m_config, true, -1));

	// �������ݿ�
	/*TableDefBuilder *builder;*/

	// �����ػ�����
	m_conn = m_db->getConnection(true);
	m_session = m_db->getSessionManager()->allocSession("MmsTestCase::init", m_conn, false);

	m_pagePool = new PagePool(m_config->m_maxSessions + 1, Limits::PAGE_SIZE);
	
	// ����ȫ��MMS
	m_mms = new Mms (m_db, targetSize, m_pagePool, needRedo);
	m_pagePool->registerUser(m_mms);
	m_pagePool->init();

	// ����MMS��
	m_fMmsTable = new FMmsTable();
	m_fMmsTable->create(m_session, m_db, m_mms, FIX_TBL_TID, "MmsTest", "FTable", "Mms", 3, cacheUpdate, colUpdate, delHeapFile, partialCached, partitionNr);
	m_vMmsTable = new VMmsTable();
	m_vMmsTable->create(m_session, m_db, m_mms, VAR_TBL_TID, "MmsTest", "VTest", /*VAR_TBL_HEAP, */Limits::MAX_REC_SIZE /3, cacheUpdate, colUpdate, varcharPK, partialCached, delHeapFile, partitionNr);
	m_vMmsTable2 = NULL;
}

void MmsTestCase::testDelWhenDoTouch() {
	MmsTester tester(this, MTT_DEL_DOTOUCH);
	MmsTestHelper helper(this, &tester, MTT_DEL_DOTOUCH);

	tester.enableSyncPoint(SP_MMS_DOTOUCH_LOCK);
	tester.enableSyncPoint(SP_MMS_DOTOUCH_UNLOCK);
	tester.start();
	helper.start();

	helper.join();
	tester.join();
	tester.disableSyncPoint(SP_MMS_DOTOUCH_LOCK);
	tester.disableSyncPoint(SP_MMS_DOTOUCH_UNLOCK);
}

void MmsTestCase::doTestDelWhenTouch(MmsTester *tester) {
	m_fMmsTable->insert(m_session, 1, false);
	Thread::msleep(3000);  // �ȴ�3��
	m_fMmsTable->insert(m_session, 1, false);
}

void MmsTestCase::doHelpTestDelWhenTouch(MmsTester *tester) {
	tester->joinSyncPoint(SP_MMS_DOTOUCH_LOCK);
	m_fMmsTable->getMmsTable()->lockMmsTable(0);
	tester->disableSyncPoint(SP_MMS_DOTOUCH_LOCK);
	tester->notifySyncPoint(SP_MMS_DOTOUCH_LOCK);
	

	tester->joinSyncPoint(SP_MMS_DOTOUCH_UNLOCK);
	m_fMmsTable->getMmsTable()->unlockMmsTable(0);
	m_fMmsTable->getMmsTable()->delCurrRecord();
	tester->disableSyncPoint(SP_MMS_DOTOUCH_UNLOCK);
	tester->notifySyncPoint(SP_MMS_DOTOUCH_UNLOCK);
}



/** ����MmsTable::doTouch���ִ���
 *  �������̣� 1. ����doTouch���ͬ����
 *			   2. ����tester�߳�(run�������̣�
 *                ��helper�̣߳�runͬ������������)
 *			   3. ���̵߳ȴ����߳�ִ�����
 *			   4. ȡ��ͬ����
 */
void MmsTestCase::testDoTouch() {
	MmsTester tester(this, MTT_DOTOUCH);
	MmsTestHelper helper(this, &tester, MTT_DOTOUCH);
	
	tester.enableSyncPoint(SP_MMS_DOTOUCH_LOCK);
	tester.enableSyncPoint(SP_MMS_DOTOUCH_UNLOCK);
	tester.start();
	helper.start();
	
	helper.join();
	tester.join();
	tester.disableSyncPoint(SP_MMS_DOTOUCH_LOCK);
	tester.disableSyncPoint(SP_MMS_DOTOUCH_UNLOCK);
}

void MmsTestCase::testAllocFail() {
	close();
	init(5, true, true, false, true, true, false, 1);

	for (int i = 0; i < 400; i++)
		m_fMmsTable->insert(m_session, i, true);
	m_vMmsTable->insert(m_session, 0, "0", sizeof("0"), true);
}

/** 
 * ����doTouchͬ���㹦�ܣ���MmsTester::run����)
 * �������̣� 1. �������в���һ����¼
 *			  2. ����RowID��MMS�в�ѯ�ü�¼
 *			  3. �жϼ�¼�Ƿ����
 *			  4. unpin MMS��¼��
 *
 * @param tester ������ָ��
 */
void MmsTestCase::doTestTouch(MmsTester *tester) {
	MmsRecord *mmsRecord;

	m_fMmsTable->insert(m_session, 1, false);
	Thread::msleep(3000);  // �ȴ�3��
	EXCPT_OPER(mmsRecord = m_fMmsTable->getMmsTable()->getByRid(m_session, 1, true, NULL, Exclusived));
	CPPUNIT_ASSERT(mmsRecord);
	EXCPT_OPER(m_fMmsTable->getMmsTable()->unpinRecord(m_session, mmsRecord));
}

/** 
 * ����doTouchͬ���㹦�� (��MmsHelper::run����)
 * �������̣� 1. �ȴ�doTouch����ͬ����
 *			  2. ����Ӧ��MMS����
 *			  3. ֪ͨ�ȴ��߳�
 *			  4. �ȴ�doTouch����ͬ����
 *			  5. �ͷ��Ѽӵ�MMS����
 *			  6. ֪ͨ�ȴ��߳�
 *
 * ����Ŀ�ģ� ��doTouch������һ���޷���ȡMMS�������龰
 *
 * @param tester ��֪ͨ���߳�
 */
void MmsTestCase::doHelpTestTouch(MmsTester *tester) {
	tester->joinSyncPoint(SP_MMS_DOTOUCH_LOCK);
	m_fMmsTable->getMmsTable()->lockMmsTable(0);
	tester->notifySyncPoint(SP_MMS_DOTOUCH_LOCK);
	
	tester->joinSyncPoint(SP_MMS_DOTOUCH_UNLOCK);
	m_fMmsTable->getMmsTable()->unlockMmsTable(0);
	tester->notifySyncPoint(SP_MMS_DOTOUCH_UNLOCK);
}

/** 
 * ����MmsTable::getByRid��MmsTable::getByPrimaryKey ���������߳�LockRowʱ)
 */
void MmsTestCase::testGetRidWhenLockRow() {
	testGetRidWhenLockRow(MTT_GETRID_WHEN_LOCKROW_0);
	testGetRidWhenLockRow(MTT_GETRID_WHEN_LOCKROW_1);
	testGetRidWhenLockRow(MTT_GETRID_WHEN_LOCKROW_2);
	testGetRidWhenLockRow(MTT_GETRID_WHEN_LOCKROW_3);
}

/** 
 * MMS��LockRow��������
 * �������̣� 1. ���������һ����¼
 *			  2. ����LockRowͬ����
 *			  3. ����tester��helper�������߳�
 *			  4. �������̲߳��ȴ�
 *			  5. �ر�LockRowͬ����
 *
 * @param task ���������
 */
void MmsTestCase::testGetRidWhenLockRow(MmsThreadTask task) {
	// ���һ����¼, RowIDΪ1
	m_fMmsTable->insert(m_session, 1, false);

	MmsTester tester(this, task);
	MmsTestHelper helper(this, &tester, task);

	tester.enableSyncPoint(SP_MMS_RID_LOCKROW);
	tester.enableSyncPoint(SP_MMS_RID_UNLOCKROW);

	tester.start();
	helper.start();

	tester.join();
	helper.join();

	tester.disableSyncPoint(SP_MMS_RID_LOCKROW);
	tester.disableSyncPoint(SP_MMS_RID_UNLOCKROW);
}

/** 
 * LockRow�������������̺��� (��MmsTester::run����)
 * �������̣�  1. ����RowID��ȡMMS��¼��
 *			   2. ���������unpin�ü�¼�unlockRow
 *
 * @param tester ������ָ��
 */
void MmsTestCase::doGetRidWhenLockRow(MmsTester *tester) {
	RowLockHandle *rlh = NULL;
	MmsRecord *mmsRecord;

	EXCPT_OPER(mmsRecord = m_fMmsTable->getMmsTable()->getByRid(m_session, 1, true, &rlh, Shared));
	if (mmsRecord) {
		EXCPT_OPER(m_fMmsTable->getMmsTable()->unpinRecord(m_session, mmsRecord));
		EXCPT_OPER(m_session->unlockRow(&rlh));
	}
}

/** 
 * LockRow��������ͬ�������̺��� (��MmsHelper::run����)
 * �������̣�1. �ȴ�LockRow����ͬ����
 *			 2. ��MMS���ڴ���ļ�¼������
 *			 3. ֪ͨ�ȴ��߳�
 *			 4.1)  ����MTT_GETRID_WHEN_LOCKROW_1���޸�ҳ�汾��
 *			 4.2)  ����MTT_GETRID_WHEN_LOCKROW_2���޸�ҳ�汾�ź�m_validֵ��ͨ��ɾ��һ����¼�ʽ)
 *			 4.3)  ����MTT_GETRID_WHEN_LOCKROW_3���޸ļ�¼RowIDֵ
 *			 5. �ȴ�����ͬ����
 *			 6. ������
 *			 7. ֪ͨ�ȴ��߳�
 *
 * @param tester ��֪ͨ���߳�
 * @param task �����
 */
void MmsTestCase::doHelpGetRidWhenLockRow(MmsTester *tester, MmsThreadTask task) {
	RowLockHandle *rlh = NULL;
	RowId rid = 1;
	MmsRecord *mmsRecord = NULL;

	tester->joinSyncPoint(SP_MMS_RID_LOCKROW);
	rlh = LOCK_ROW(m_session, m_fMmsTable->getTid(), rid, Exclusived);
	tester->notifySyncPoint(SP_MMS_RID_LOCKROW);

	tester->joinSyncPoint(SP_MMS_RID_UNLOCKROW);
	EXCPT_OPER(m_session->unlockRow(&rlh));
	tester->notifySyncPoint(SP_MMS_RID_UNLOCKROW);
}

/** 
 * ����������testGetRidWhenLockRow
 */
void MmsTestCase::testGetPKWhenLockRow(MmsThreadTask task) {
	// ���һ����¼, RowIDΪ1
	m_fMmsTable->insert(m_session, 1, false);

	MmsTester tester(this, task);
	MmsTestHelper helper(this, &tester, task);

	tester.enableSyncPoint(SP_MMS_PK_LOCKROW);
	tester.enableSyncPoint(SP_MMS_PK_UNLOCKROW);

	tester.start();
	helper.start();

	tester.join();
	helper.join();

	tester.disableSyncPoint(SP_MMS_PK_LOCKROW);
	tester.disableSyncPoint(SP_MMS_PK_UNLOCKROW);
}

/** 
 * ����flushLog��GetSession�߼���������޷���ȡsession)
 * �������̣�1. �ر�MMS�Զ�flushLog����
 *			 2. ���ø���cache���Ϊ1��
 *			 3. �ȴ�2�� (����2��3����flushLog������һ�д��븲��)
 *			 4. ִ��MTT_GETSESSION_WHEN_FLUSHLOG_1��������
 *			 5. ִ��MTT_GETSESSION_WHEN_FLUSHLOG_2��������
 */
void MmsTestCase::testGetSessionWhenFlushLog() {
	m_fMmsTable->getMmsTable()->disableAutoFlushLog();
	m_fMmsTable->getMmsTable()->setUpdateCacheTime(1);
	Thread::msleep(2000);
	testGetSessionWhenFlushLog(MTT_GETSESSION_WHEN_FLUSHLOG_1);
	testGetSessionWhenFlushLog(MTT_GETSESSION_WHEN_FLUSHLOG_2);
}

/** 
 * ����flushLog��GetSession�߼����(ִ�в�������)
 *
 * @param task ���������
 */
void MmsTestCase::testGetSessionWhenFlushLog(MmsThreadTask task) {
	MmsTester tester(this, task);
	MmsTestHelper helper(this, &tester, task);

	tester.enableSyncPoint(SP_BGTASK_GET_SESSION_START);
	if (task == MTT_GETSESSION_WHEN_FLUSHLOG_1)
		tester.enableSyncPoint(SP_BGTASK_GET_SESSION_END);
	tester.enableSyncPoint(SP_MMS_FL);

	tester.start();
	helper.start();

	tester.join();
	helper.join();

	tester.disableSyncPoint(SP_BGTASK_GET_SESSION_START);
	if (task == MTT_GETSESSION_WHEN_FLUSHLOG_1)
		tester.disableSyncPoint(SP_BGTASK_GET_SESSION_END);
	tester.disableSyncPoint(SP_MMS_FL);
}

/** 
 * ����flushLog��GetSession�߼�����������̣�
 * �������̣� 1. �ر��Զ�ˢ����־
 *			  2. ����ˢд��־����
 *			  3. �������ΪMTT_GETSESSION_WHEN_FLUSHLOG_2, �ȴ���SP_MMS_FLͬ����
 *
 * @param tester ��֪ͨ���߳�
 * @param task	�����
 */
void MmsTestCase::doGetSessionWhenFlushLog(MmsTester *tester, MmsThreadTask task) {
	// ֱ�ӵ��ø��»���ˢд����
	m_fMmsTable->getMmsTable()->disableAutoFlushLog();
	m_fMmsTable->getMmsTable()->flushMmsLog(m_session);
	if (task == MTT_GETSESSION_WHEN_FLUSHLOG_2)
		SYNCHERE(SP_MMS_FL);
}

/** 
 * ����flushLog��GetSession�߼�������������̣�
 * �������̣�1. ��SP_MMS_FL_SESSION_START֮���ȡ����session
 *			 2. ��SP_MMS_FL_SESSION_END֮���ͷ�һ��session ��MTT_GETSESSION_WHEN_FLUSHLOG_1����)
 *			 3. ��SP_MMS_FL֮���ͷ�����δ�ͷ�session
 *
 * @param tester ��֪ͨ���߳�
 * @param task �����
 */
void MmsTestCase::doHelpGetSessionWhenFlushLog(MmsTester *tester, MmsThreadTask task) {
	int idx = 0;
	Connection* conns[10000];
	Session* sessions[10000];
	// Connection *conn = m_db->getConnection(true);
	char name[100];

	tester->joinSyncPoint(SP_BGTASK_GET_SESSION_START);

	
	// ��ȡ���е�Session
	do {
		sprintf(name, "%d", idx);
		conns[idx] = m_db->getConnection(true);
		assert(conns[idx]);
		sessions[idx] = m_db->getSessionManager()->allocSession(name, conns[idx], false);
	} while (sessions[idx++]);

	tester->notifySyncPoint(SP_BGTASK_GET_SESSION_START);
	if (task == MTT_GETSESSION_WHEN_FLUSHLOG_1) {
		tester->joinSyncPoint(SP_BGTASK_GET_SESSION_END);
		m_db->getSessionManager()->freeSession(sessions[0]);
		tester->notifySyncPoint(SP_BGTASK_GET_SESSION_END);
	}
	tester->joinSyncPoint(SP_MMS_FL);
	if (task == MTT_GETSESSION_WHEN_FLUSHLOG_1)
		CPPUNIT_ASSERT(!m_db->getSessionManager()->allocSession("0", conns[0], false));
	tester->notifySyncPoint(SP_MMS_FL);

	int i;
	if (task == MTT_GETSESSION_WHEN_FLUSHLOG_1)
		i = 1;
	else
		i = 0;
	for (; i < idx - 1; i++)
		m_db->getSessionManager()->freeSession(sessions[i]);
	for (int i = 0; i < idx; i++) {
		m_db->freeConnection(conns[i]);
	}
}

void MmsTestCase::testFlushLogForce() {
	doTestUpdate(true, 100);
	m_fMmsTable->getMmsTable()->runFlushTimerForce();
	close();
	init(100, true, true);
	m_fMmsTable->getMmsTable()->disableAutoFlushLog();
	m_vMmsTable->getMmsTable()->disableAutoFlushLog();
	doTestUpdate_simple(true, 5);
	m_fMmsTable->getMmsTable()->flushUpdateLog(m_session);
	m_vMmsTable->getMmsTable()->flushUpdateLog(m_session);
}

/** 
 * ����FlushLog��ҳ���Ͳ��Թ���
 */
void MmsTestCase::testPageType() {
	close();
	init(100, true, true);
	m_fMmsTable->getMmsTable()->disableAutoFlushLog();
	MmsTester tester(this, MTT_FLUSHLOG_VICTIMPAGE);
	MmsTestHelper helper(this, &tester, MTT_FLUSHLOG_VICTIMPAGE);

	tester.enableSyncPoint(SP_MMS_FL_LOCK_PG);
	tester.enableSyncPoint(SP_MMS_FL_UNLOCK_PG);

	tester.start();
	helper.start();

	tester.join();
	helper.join();
}

/** 
 * ����FlushLog��ҳ���Ͳ��Թ��ܣ�������)
 * �������̣� 1. ���벢����100����¼
 *			  2. ����flushLog
 *
 * @param tester ��֪ͨ���߳�
 */
void MmsTestCase::doTestPageType(MmsTester *tester) {
	RowId rowId;

	for(int i = 0; i < 100; i++) {
		rowId = m_fMmsTable->insert(m_session, i, true);
		m_fMmsTable->update(m_session, rowId, i + 10000);
	}
	m_fMmsTable->getMmsTable()->flushMmsLog(m_session);
}

/** 
 * ����FlushLog��ҳ���Ͳ��Թ��ܣ���������)
 * ��������:  1. ��SP_MMS_FL_LOCK_PGͬ���㣬�滻��Ӧ��MMS��¼ҳ
 *			  2. �ȴ�SP_MMS_FL_UNLOCK_PGͬ���㣨ע�⣺��MMS�����߼�����ȷ���ڸô����������)
 *			  3. �ر�ͬ���㣬��֪ͨ��Ӧ�߳�
 *
 * @param tester ��֪ͨ���߳�
 */
void MmsTestCase::doHelpTestPageType(MmsTester *tester) {
	tester->joinSyncPoint(SP_MMS_FL_LOCK_PG);
	m_fMmsTable->getMmsTable()->evictCurrPage(m_session);
	tester->notifySyncPoint(SP_MMS_FL_LOCK_PG);

	tester->joinSyncPoint(SP_MMS_FL_UNLOCK_PG);
	tester->disableSyncPoint(SP_MMS_FL_LOCK_PG);
	tester->disableSyncPoint(SP_MMS_FL_UNLOCK_PG);
	tester->notifySyncPoint(SP_MMS_FL_UNLOCK_PG);
}

/** 
 * ����FlushLog��RPClass��鹦��
 */
void MmsTestCase::testFlushLogRpClass() {
	close();
	init(100, true, true);
	m_fMmsTable->getMmsTable()->disableAutoFlushLog();

	MmsTester tester(this, MTT_FLUSHLOG_RPCLASS);
	MmsTestHelper helper(this, &tester, MTT_FLUSHLOG_RPCLASS);

	tester.enableSyncPoint(SP_MMS_FL_LOCK_PG);

	tester.start();
	helper.start();

	tester.join();
	helper.join();
}

/** 
 * ����FlushLog��RPClass��鹦�ܣ�������)
 * �������̣� 1. ���벢����100����¼
 *			  2. ����flushLog
 *
 * @param tester ��֪ͨ���߳�
 */
void MmsTestCase::doTestFlushLogRpClass(MmsTester *tester) {
	RowId rowId;

	for(int i = 0; i < 100; i++) {
		rowId = m_fMmsTable->insert(m_session, i, true);
		m_fMmsTable->update(m_session, rowId, i + 10000);
	}
	m_fMmsTable->getMmsTable()->flushMmsLog(m_session);
}

/** 
 * ����FlushLog��RPClass��鹦�ܣ���������)
 * �������̣� 1. ��SP_MMS_FL_LOCK_PGͬ����ʱ�����õ�ǰҳ��rpClass�ֶ�ΪNULL
 *			  2. �ر�SP_MMS_FL_LOCK_PGͬ���㣬��֪ͨ��Ӧ�ĵȴ��߳�
 *
 * @param tester ��֪ͨ���߳�
 */
void MmsTestCase::doHelpTestFlushLogRpClass(MmsTester *tester) {
	tester->joinSyncPoint(SP_MMS_FL_LOCK_PG);
	m_fMmsTable->getMmsTable()->setRpClass(NULL);
	tester->disableSyncPoint(SP_MMS_FL_LOCK_PG);
	tester->notifySyncPoint(SP_MMS_FL_LOCK_PG);
}

/** 
 * ���Լ���ˢ��ʱҳ�����жϹ���
 */
void MmsTestCase::testPageTypeWhenFlush() {
	MmsTester tester(this, MTT_FLUSH_VICTIMPAGE);
	MmsTestHelper helper(this, &tester, MTT_FLUSH_VICTIMPAGE);

	tester.enableSyncPoint(SP_MMS_SF_LOCK_PG);
	tester.enableSyncPoint(SP_MMS_SF_UNLOCK_PG);

	tester.start();
	helper.start();

	tester.join();
	helper.join();

	testPageType();
}

/** 
 * ���Լ���ˢ��ʱҳ�����жϹ���(������)
 * �������̣� 1. ����doTestUpdate����
 *			  2. ���������ˢ��
 *
 * @param tester ��֪ͨ���߳�
 */
void MmsTestCase::doTestPageTypeWhenFlush(MmsTester *tester) {
	doTestUpdate(true, 100);
	EXCPT_OPER(m_fMmsTable->getMmsTable()->flush(m_session, true));
}

/** 
 * ���Լ���ˢ��ʱҳ�����жϹ���(��������)
 * �������̣� 1. ��SP_MMS_SF_LOCK_PGͬ�����滻��ǰҳ
 *			  2. �ر�SP_MMS_SF_LOCK_PGͬ���㣬��֪ͨ�ȴ��߳�
 *			  3. �ȴ�SP_MMS_SF_UNLOCK_PGͬ����
 *			  4. �ر�SP_MMS_SF_UNLOCK_PGͬ���㣬��֪ͨ�ȴ��߳�
 *
 * @param tester ��֪ͨ���߳�
 */
void MmsTestCase::doHelpTestPageTypeWhenFlush(MmsTester *tester) {
	tester->joinSyncPoint(SP_MMS_SF_LOCK_PG);
	m_fMmsTable->getMmsTable()->evictCurrPage(m_session);
	tester->disableSyncPoint(SP_MMS_SF_LOCK_PG);
	tester->notifySyncPoint(SP_MMS_SF_LOCK_PG);

	tester->joinSyncPoint(SP_MMS_SF_UNLOCK_PG);
	tester->disableSyncPoint(SP_MMS_SF_UNLOCK_PG);
	tester->notifySyncPoint(SP_MMS_SF_UNLOCK_PG);
}

/**
 * ����sortFlush�е�RPClass��⹦��
 */
void MmsTestCase::testSortFlushRpClass() {
	MmsTester tester(this, MTT_SORTFLUSH_RPCLASS);
	MmsTestHelper helper(this, &tester, MTT_SORTFLUSH_RPCLASS);

	tester.enableSyncPoint(SP_MMS_SF_LOCK_PG);

	tester.start();
	helper.start();

	tester.join();
	helper.join();
}

/** 
 * ����sortFlush�е�RPClass��⹦��(������)
 *
 * @param tester ��֪ͨ���߳�
 */
void MmsTestCase::doTestSortFlushRpClass(MmsTester *tester) {
	doTestUpdate(true, 100);
	EXCPT_OPER(m_fMmsTable->getMmsTable()->flush(m_session, true));
	m_fMmsTable->getMmsTable()->setMmsTableInRpClass(m_fMmsTable->getMmsTable());
}

/** 
 * ����sortFlush�е�RPClass��⹦��(��������)
 *
 * @param tester ��֪ͨ���߳�
 */
void MmsTestCase::doHelpTestSortFlushRpClass(MmsTester *tester) {
	tester->joinSyncPoint(SP_MMS_SF_LOCK_PG);
	m_fMmsTable->getMmsTable()->setMmsTableInRpClass(NULL);
	tester->disableSyncPoint(SP_MMS_SF_LOCK_PG);
	tester->notifySyncPoint(SP_MMS_SF_LOCK_PG);	
}

/** 
 * ����ˢ��ʱҳ���Ͳ���
 */
void MmsTestCase::testPageTypeWhenGetDirRec() {
	MmsTester tester(this, MTT_DIRTY_REC);
	MmsTestHelper helper(this, &tester, MTT_DIRTY_REC);

	tester.enableSyncPoint(SP_MMS_GET_DIRTY_REC_1ST);
	tester.enableSyncPoint(SP_MMS_GET_DIRTY_REC_1ST_END);
	tester.enableSyncPoint(SP_MMS_GET_DIRTY_REC_2ND);
	tester.enableSyncPoint(SP_MMS_GET_DIRTY_REC_2ND_END);

	tester.start();
	helper.start();

	tester.join();
	helper.join();
}

/** 
 * ����ˢ��ʱҳ���Ͳ���(������)
 * �������̣�1. ���벢����500����¼
 *			 2. ����ˢ��
 *
 * @param tester ��֪ͨ���߳�
 */
void MmsTestCase::doTestPageTypeWhenGetDirRec(MmsTester *tester) {
	RowId rowId;

	// �������ݲ�����
	for (int i = 0; i < 500; i++) {
		EXCPT_OPER(rowId = m_fMmsTable->insert(m_session, i, true));
		EXCPT_OPER(m_fMmsTable->update(m_session, rowId, i+10));
	}
	// ����ˢ��
	m_fMmsTable->getMmsTable()->flush(m_session, true);
}

/** 
 * ����ˢ��ʱҳ���Ͳ���(��������)
 *
 * @param tester ��֪ͨ���߳�
 */
void MmsTestCase::doHelpTestPageTypeWhenGetDirRec(MmsTester *tester) {
	tester->joinSyncPoint(SP_MMS_GET_DIRTY_REC_1ST);
	m_fMmsTable->getMmsTable()->evictCurrPage(m_session);
	tester->disableSyncPoint(SP_MMS_GET_DIRTY_REC_1ST);
	tester->notifySyncPoint(SP_MMS_GET_DIRTY_REC_1ST);

	tester->joinSyncPoint(SP_MMS_GET_DIRTY_REC_1ST_END);
	tester->disableSyncPoint(SP_MMS_GET_DIRTY_REC_1ST_END);
	tester->notifySyncPoint(SP_MMS_GET_DIRTY_REC_1ST_END);

	tester->joinSyncPoint(SP_MMS_GET_DIRTY_REC_2ND);
	m_fMmsTable->getMmsTable()->setRpClass(NULL);
	tester->disableSyncPoint(SP_MMS_GET_DIRTY_REC_2ND);
	tester->notifySyncPoint(SP_MMS_GET_DIRTY_REC_2ND);

	tester->joinSyncPoint(SP_MMS_GET_DIRTY_REC_2ND_END);
	tester->disableSyncPoint(SP_MMS_GET_DIRTY_REC_2ND_END);
	tester->notifySyncPoint(SP_MMS_GET_DIRTY_REC_2ND_END);
}

/** 
 * ����MMS�滻ʱ��ҳ��pin���
 */
void MmsTestCase::testReplaceWhenPinTopPage() {
	MmsTester tester(this, MTT_GETPIN_REPLACEMENT);
	MmsTestHelper helper(this, &tester, MTT_GETPIN_REPLACEMENT);

	tester.enableSyncPoint(SP_MMS_GET_PIN_WHEN_REPLACEMENT);
	tester.enableSyncPoint(SP_MMS_GET_PIN_WHEN_REPLACEMENT_END);

	tester.start();
	helper.start();

	tester.join();
	helper.join();
}

/** 
 * ����MMS�滻ʱ��ҳ��pin��������̣߳�
 *
 * @param tester ��֪ͨ�߳�
 */
void MmsTestCase::doReplaceWhenPinTopPage(MmsTester *tester) {
	close();
	init(7, true, true);
	for (int i = 0; i < 2000; i++)
		m_fMmsTable->insert(m_session, i, true);
}

/** 
 * ����MMS�滻ʱ��ҳ��pin����������̣߳�
 *
 * @param tester ��֪ͨ�߳�
 */
void MmsTestCase::doHelpReplaceWhenPinTopPage(MmsTester *tester) {
	tester->joinSyncPoint(SP_MMS_GET_PIN_WHEN_REPLACEMENT);
	m_fMmsTable->getMmsTable()->pinCurrPage();
	tester->notifySyncPoint(SP_MMS_GET_PIN_WHEN_REPLACEMENT);
	tester->joinSyncPoint(SP_MMS_GET_PIN_WHEN_REPLACEMENT_END);
	m_fMmsTable->getMmsTable()->unpinCurrPage();
	
	tester->disableSyncPoint(SP_MMS_GET_PIN_WHEN_REPLACEMENT);
	tester->disableSyncPoint(SP_MMS_GET_PIN_WHEN_REPLACEMENT_END);
	tester->notifySyncPoint(SP_MMS_GET_PIN_WHEN_REPLACEMENT_END);
}

/** 
 * ���Ի������ʱ���ȡ/����
 */
void MmsTestCase::testCacheUpdateTime() {
	uint tm = m_fMmsTable->getMmsTable()->getUpdateCacheTime();
	m_fMmsTable->getMmsTable()->setUpdateCacheTime(tm * 2);
	CPPUNIT_ASSERT(tm * 2 == m_fMmsTable->getMmsTable()->getUpdateCacheTime());

	/*close();
	init(100, false, false);
	CPPUNIT_ASSERT(!m_fMmsTable->getMmsTable()->getUpdateCacheTime());*/
}

/** 
 * ����ͳ����Ϣ
 */
void MmsTestCase::testMmsStats() {
	doTestUpdate(true, 100);
	assert (m_mms->getMaxNrDirtyRecs() > 0);
	m_mms->setMaxNrDirtyRecs(6);
	EXCPT_OPER(m_fMmsTable->getMmsTable()->flush(m_session, true));
	EXCPT_OPER(m_vMmsTable->getMmsTable()->flush(m_session, true));

	u64 info;

	info = m_mms->getStatus().m_recordQueries;
	info = m_mms->getStatus().m_recordQueryHits;
	info = m_mms->getStatus().m_recordInserts;
	info = m_mms->getStatus().m_recordDeletes;
	info = m_mms->getStatus().m_recordUpdates;
	info = m_mms->getStatus().m_recordVictims;
	info = m_mms->getStatus().m_pageVictims;
	info = m_mms->getStatus().m_occupiedPages;

	MmsTable *mmsTable = m_fMmsTable->getMmsTable();

	info = mmsTable->getStatus().m_recordQueries;
	info = mmsTable->getStatus().m_recordQueryHits;
	info = mmsTable->getStatus().m_records;
	info = mmsTable->getStatus().m_recordPages;
	info = mmsTable->getStatus().m_recordInserts;
	info = mmsTable->getStatus().m_recordDeletes;
	info = mmsTable->getStatus().m_recordUpdates;
	info = mmsTable->getStatus().m_recordVictims;
	info = mmsTable->getStatus().m_pageVictims;

	int nrRPClass;
	MmsRPClass **rpClass = mmsTable->getMmsRPClass(&nrRPClass);

	for (int i = 0; i < nrRPClass; i++) {
		if (rpClass[i]) {
			info = rpClass[i]->getStatus().m_records;
			info = rpClass[i]->getStatus().m_recordPages;
			info = rpClass[i]->getStatus().m_freePages;
			info = rpClass[i]->getStatus().m_recordInserts;
			info = rpClass[i]->getStatus().m_recordDeletes;
			info = rpClass[i]->getStatus().m_recordUpdates;
			info = rpClass[i]->getStatus().m_recordVictims;
			info = rpClass[i]->getStatus().m_pageInserts;
			info = rpClass[i]->getStatus().m_pageDeletes;
			info = rpClass[i]->getStatus().m_pageVictims;
		}
	}

	// test mms unregister
	testMms();

	// update for paritial cached
	testUpdateWhenParitalCached();
}

/**
 * ���������²���
 */
void MmsTestCase::testUpdateNonPK() {
	close();

	init(1000, true, true, false, true, false);
	this->doTestUpdate(true);
}

/** 
 * ��������ΪVMmsTable, ���»����ֶ�2��4,
 * ��������1Ϊ��{ 4 } --> { 2, 4 } --> { 2 } --> { 1 } --> { 2 } --> { 3, 4 } --> { 2 } --> { 1, 2, 3, 4}
 * ��������2Ϊ��{ 1, 2, 3, 4 }
 *
 * ��������ΪFMmsTable, ���»����ֶ�Ϊ2��4
 * ��������2Ϊ: { 2 } --> { 1, 2, 3, 4 }
 */
void MmsTestCase::testUpdateWhenParitalCached() {
	RowId rowId;
	char s[1024];

	close();

	init(1000, true, true, false, true, true, true);

	for (int i = 0; i < NR_RECORDS;) {
		sprintf(s, "%d", i);
		EXCPT_OPER(rowId = m_vMmsTable->insert(m_session, i, s, (int)strlen(s), true));
		sprintf(s, "%d%s", i++, s);
		m_vMmsTable->updateColumn3(m_session, rowId, i);
		sprintf(s, "%d%s", i++, s);
		m_vMmsTable->updateColumn13(m_session, rowId, i, s);
		sprintf(s, "%d%s", i++, s);
		m_vMmsTable->updateColumn1(m_session, rowId, s);
		sprintf(s, "%d%s", i++, s);
		m_vMmsTable->updateColumn0(m_session, rowId, i);
		sprintf(s, "%d%s", i++, s);
		m_vMmsTable->updateColumn1(m_session, rowId, s);
		sprintf(s, "%d%s", i++, s);
		m_vMmsTable->updateColumn23(m_session, rowId, i);
		sprintf(s, "%d%s", i++, s);
		m_vMmsTable->updateColumn1(m_session, rowId, s);
		sprintf(s, "%d%s", i++, s);
		m_vMmsTable->updateColumn0123(m_session, rowId, i, s);
		sprintf(s, "%d%s", i++, s);
		m_vMmsTable->updateColumn1(m_session, rowId, s);
	}

	close();
	init(1000, true, true, false, true, true, true);
	int j = 0; 

	sprintf(s, "%d", j);
	EXCPT_OPER(rowId = m_vMmsTable->insert(m_session, j, s, (int)strlen(s), true));
	sprintf(s, "%d%s", j++, s);
	m_vMmsTable->updateColumn0123(m_session, rowId, j, s);

	j = 0;

	EXCPT_OPER(rowId = m_fMmsTable->insert(m_session, j++, true));
	m_fMmsTable->updateColumn1(m_session, rowId, j++);
	m_fMmsTable->update(m_session, rowId, j++);
}

/** 
 * allocMmsPage����
 */
void MmsTestCase::testAllocMmsPage() {
	testAllocMmsPage(MTT_AMP_GET_TBL);
	testAllocMmsPage(MTT_AMP_GET_PG);
	testAllocMmsPage(MTT_AMP_PIN_PG);
}

/** 
* getSubRecord�м�¼ҳʧЧ����Ĳ���
*/
void MmsTestCase::testDisablePgWhenGetSubRecord() {
	MmsTester tester(this, MTT_GETSUBRECORD_DISABLEPG);
	MmsTestHelper helper(this, &tester, MTT_GETSUBRECORD_DISABLEPG);

	tester.enableSyncPoint(SP_MMS_SUBRECORD_DISABLEPG);

	tester.start();
	helper.start();

	tester.join();
	helper.join();
}

void MmsTestCase::doTestDisablePgWhenGetSubRecord(MmsTester *tester, MmsThreadTask task) {
	SubRecord subRecord;
	RowId rid = 10;

	subRecord.m_format = REC_REDUNDANT;
	subRecord.m_size = Limits::PAGE_SIZE;
	subRecord.m_data = (byte *)malloc(subRecord.m_size);
	subRecord.m_numCols = 2;
	subRecord.m_columns = (u16 *)malloc(sizeof(u16) * subRecord.m_numCols);
	for (int i = 0; i < subRecord.m_numCols; i++)
		subRecord.m_columns[i] = i;
	m_fMmsTable->insert(m_session, rid, false);

	SubrecExtractor *extractor = SubrecExtractor::createInst(m_session->getMemoryContext(), m_fMmsTable->getTableDef(),
		subRecord.m_numCols, subRecord.m_columns, m_fMmsTable->getTableDef()->m_recFormat, REC_REDUNDANT);
	CPPUNIT_ASSERT(!m_fMmsTable->getMmsTable()->getSubRecord(m_session, rid, extractor, &subRecord, true, false, 0));
	free(subRecord.m_data);
	free(subRecord.m_columns);
}

void MmsTestCase::doHelpDisablePgWhenGetSubRecord(MmsTester *tester, MmsThreadTask task) {
	tester->joinSyncPoint(SP_MMS_SUBRECORD_DISABLEPG);
	m_fMmsTable->getMmsTable()->disableCurrPage();
	tester->disableSyncPoint(SP_MMS_SUBRECORD_DISABLEPG);
	tester->notifySyncPoint(SP_MMS_SUBRECORD_DISABLEPG);
}

void MmsTestCase::testTryLockWhenGetByRid() {
	MmsTester tester(this, MTT_GETRID_TRYLOCK);
	MmsTestHelper helper(this, &tester, MTT_GETRID_TRYLOCK);

	tester.enableSyncPoint(SP_MMS_RID_TRYLOCK);

	tester.start();
	helper.start();

	tester.join();
	helper.join();
}

void MmsTestCase::doTestTryLockWhenGetByRid(MmsTester *tester, MmsThreadTask task) {
	m_fMmsTable->insert(m_session, 10, false);
	m_fMmsTable->selectByRid(m_session, 10, NULL, None);
}

void MmsTestCase::doHelpTryLockWhenGetByRid(MmsTester *tester, MmsThreadTask task) {
	tester->joinSyncPoint(SP_MMS_RID_TRYLOCK);
	m_fMmsTable->getMmsTable()->lockCurrPage(m_session);
	tester->disableSyncPoint(SP_MMS_RID_TRYLOCK);
	tester->notifySyncPoint(SP_MMS_RID_TRYLOCK);
	m_fMmsTable->getMmsTable()->unlockCurrPage(m_session);
}


/** 
 * GetByRid�м�¼ҳʧЧ����Ĳ���
 */
void MmsTestCase::testDisablePgWhenGetByRid() {
	MmsTester tester(this, MTT_GETRID_DISABLEPG);
	MmsTestHelper helper(this, &tester, MTT_GETRID_DISABLEPG);

	tester.enableSyncPoint(SP_MMS_RID_DISABLEPG);

	tester.start();
	helper.start();

	tester.join();
	helper.join();
}

void MmsTestCase::doTestDisablePgWhenGetByRid(MmsTester *tester, MmsThreadTask task) {
	m_fMmsTable->insert(m_session, 10, false);
	CPPUNIT_ASSERT(NULL == m_fMmsTable->selectByRid(m_session, 10, NULL, None));
}

void MmsTestCase::doHelpDisablePgWhenGetByRid(MmsTester *tester, MmsThreadTask task) {
	tester->joinSyncPoint(SP_MMS_RID_DISABLEPG);
	m_fMmsTable->getMmsTable()->disableCurrPage();
	tester->disableSyncPoint(SP_MMS_RID_DISABLEPG);
	tester->notifySyncPoint(SP_MMS_RID_DISABLEPG);
}

/** 
* PutIfNotExist�м�¼ҳʧЧ����Ĳ���
*/
void MmsTestCase::testDisablePgWhenPutIfNotExist() {
	MmsTester tester(this, MTT_PUT_DISABLEPG);
	MmsTestHelper helper(this, &tester, MTT_PUT_DISABLEPG);

	tester.enableSyncPoint(SP_MMS_PUT_DISABLEPG);

	tester.start();
	helper.start();

	tester.join();
	helper.join();
}

void MmsTestCase::doTestDisablePgWhenPutIfNotExist(MmsTester *tester, MmsThreadTask task) {
	m_fMmsTable->insert(m_session, 10, false);
	// �ٴβ�����ͬ��¼
	m_fMmsTable->insert(m_session, 10, false);
}

void MmsTestCase::doHelpDisablePgWhenPutIfNotExist(MmsTester *tester, MmsThreadTask task) {
	tester->joinSyncPoint(SP_MMS_PUT_DISABLEPG);
	m_fMmsTable->getMmsTable()->disableCurrPage();
	tester->disableSyncPoint(SP_MMS_PUT_DISABLEPG);
	tester->notifySyncPoint(SP_MMS_PUT_DISABLEPG);
}

/** 
 * allocMmsPage���ԣ��������)
 *
 * @param task �����
 */
void MmsTestCase::testAllocMmsPage(MmsThreadTask task) {
	MmsTester tester(this, task);
	MmsTestHelper helper(this, &tester, task);

	switch(task) {
	case MTT_AMP_GET_TBL:
		tester.enableSyncPoint(SP_MMS_AMP_GET_TABLE);
		tester.enableSyncPoint(SP_MMS_AMP_GET_TABLE_END);
		break;
	case MTT_AMP_GET_PG:
		tester.enableSyncPoint(SP_MMS_AMP_GET_PAGE);
		tester.enableSyncPoint(SP_MMS_AMP_GET_PAGE_END);
		break;
	case MTT_AMP_PIN_PG:
		tester.enableSyncPoint(SP_MMS_AMP_PIN_PAGE);
		tester.enableSyncPoint(SP_MMS_AMP_PIN_PAGE_END);
		break;
	}

	tester.start();
	helper.start();

	tester.join();
	helper.join();
}

/**
 * allocMmsPage����(�����̣�
 *
 * @param tester ��֪ͨ���߳�
 * @param task �����
 */
void MmsTestCase::doTestAllocMmsPage(MmsTester *tester, MmsThreadTask task) {
	close();
	init(100, true, true);
	testFreeSomePages();
}

/**
 * allocMmsPage����(��������)
 *
 * @param tester ��֪ͨ���߳�
 * @param task �����
 */
void MmsTestCase::doHelpAllocMmsPage(MmsTester *tester, MmsThreadTask task) {
	if (MTT_AMP_GET_TBL == task) {
		tester->joinSyncPoint(SP_MMS_AMP_GET_TABLE);
		m_mms->lockMmsTable(0);
		tester->disableSyncPoint(SP_MMS_AMP_GET_TABLE);
		tester->notifySyncPoint(SP_MMS_AMP_GET_TABLE);

		tester->joinSyncPoint(SP_MMS_AMP_GET_TABLE_END);
		m_mms->unlockMmsTable(0);
		tester->disableSyncPoint(SP_MMS_AMP_GET_TABLE_END);
		tester->notifySyncPoint(SP_MMS_AMP_GET_TABLE_END);
	} else if (MTT_AMP_GET_PG == task) {
		tester->joinSyncPoint(SP_MMS_AMP_GET_PAGE);
		m_mms->lockRecPage(m_session);
		tester->disableSyncPoint(SP_MMS_AMP_GET_PAGE);
		tester->notifySyncPoint(SP_MMS_AMP_GET_PAGE);

		tester->joinSyncPoint(SP_MMS_AMP_GET_PAGE_END);
		m_mms->unlockRecPage(m_session);
		tester->disableSyncPoint(SP_MMS_AMP_GET_PAGE_END);
		tester->notifySyncPoint(SP_MMS_AMP_GET_PAGE_END);	
	} else if (MTT_AMP_PIN_PG == task) {
		tester->joinSyncPoint(SP_MMS_AMP_PIN_PAGE);
		m_mms->pinRecPage();
		tester->disableSyncPoint(SP_MMS_AMP_PIN_PAGE);
		tester->notifySyncPoint(SP_MMS_AMP_PIN_PAGE);

		tester->joinSyncPoint(SP_MMS_AMP_PIN_PAGE_END);
		m_mms->unpinRecPage();
		tester->disableSyncPoint(SP_MMS_AMP_PIN_PAGE_END);
		tester->notifySyncPoint(SP_MMS_AMP_PIN_PAGE_END);	
	}
}

/** 
 * ����FPage����
 */
void MmsTestCase::testFPage() {
	char s[1024];
	
	close();
	init(20, true, true);

	// Ĭ������ҳ�滻��Ϊ1
	CPPUNIT_ASSERT(1 == m_mms->getPageReplaceRatio());

	// ����ʧ�ܣ�
	CPPUNIT_ASSERT(!m_mms->setPageReplaceRatio(1.5));

	// ���óɹ�
	CPPUNIT_ASSERT(m_mms->setPageReplaceRatio((float)0.001));

	for (int i = 0; i < 100; i++)
		m_fMmsTable->insert(m_session, i, true);

	for (int i = 0; i < 1000; i++) {
		sprintf(s, "%d", i);
		m_vMmsTable->insert(m_session, i, s, (int)strlen(s), true);
	}
}

/**
 *						������������
 *
 *	1. ���Ա�˵����
 *	   �ṩ���ֲ��Ա��������䳤��Ͱ��ֶα�(100���ֶ�)���ֱ���FMmsTable��VMmsTable��HMmsTable������ʵ��
 *
 *  2. �����߳�˵����
 *	   �ṩ���ֲ����̣߳������ߡ������ߺ͸����ߣ��ֱ���MmsProducer��MmsConsumer��MmsChanger������ʵ��
 *	   �����ߣ������¼����
 *	   �����ߣ������¼ɾ��
 *	   �����ߣ������¼����
 *
 * 
 */
/************************************************************************/
/*                     ������FMmsTableʵ��                              */
/************************************************************************/

/** 
 * ���������� (���캯������ã�
 *
 * @param session		�ػ�����
 * @param db			�������ݿ�
 * @param tid			��ID
 * @param schema		ģʽ��
 * @param tablename		����
 * @param namePrefix	����ǰ׺
 * @param prefixSize	ǰ׺����
 * @param cacheUpdate	���»���
 * @param colUpdate		�ֶθ��»���
 */
void FMmsTable::create(Session *session, 
					   Database *db,
					   Mms *mms,
					   int tid, 
					   const char *schema, 
					   const char *tablename, 
					   const char *namePrefix, 
					   int prefixSize,
					   bool cacheUpdate,
					   bool colUpdate, 
					   bool delHeapFile,
					   bool partialCached,
					   int partitionNr) {
	m_db = db;
	m_tid = tid;
	m_namePrefix = namePrefix;
	m_prefixSize = prefixSize;
	if (m_tblDef != NULL) {
		delete m_tblDef;
		m_tblDef = NULL;
	}
	// ����������
	if (delHeapFile) {
		File fHeap(FIX_TBL_HEAP);
		fHeap.remove();
		TableDef::drop(FIX_TBL_DEF);
	}
	TableDefBuilder *builder = new TableDefBuilder(tid, schema, tablename);
	builder->addColumn("UserId", CT_BIGINT, false)->addColumnS("UserName", CT_CHAR, prefixSize + (u16)sizeof(u64), false);
	builder->addColumn("BankAccount", CT_BIGINT)->addColumn("Balance", CT_INT);
	builder->addIndex("PRIMARY", true, true, false, "UserId", 0, NULL);
	if (delHeapFile) {
		m_tblDef = builder->getTableDef();
		EXCPT_OPER(m_tblDef->writeFile(FIX_TBL_DEF));
		EXCPT_OPER(DrsHeap::create(m_db, FIX_TBL_HEAP, m_tblDef));
	} else {
		EXCPT_OPER(m_tblDef = TableDef::open(FIX_TBL_DEF));
	}

	EXCPT_OPER(m_heap = DrsHeap::open(m_db, session, FIX_TBL_HEAP, m_tblDef));
	delete builder;

	if(cacheUpdate) {
		m_tblDef->m_cacheUpdate = true;
		m_tblDef->m_updateCacheTime = 10;
	} else {
		m_tblDef->m_cacheUpdate = false;
		m_tblDef->m_updateCacheTime = 10;
	}

	if (colUpdate) {
		if (partialCached) {
			m_tblDef->m_columns[0]->m_cacheUpdate = false;
			m_tblDef->m_columns[1]->m_cacheUpdate = true;
			m_tblDef->m_columns[2]->m_cacheUpdate = false;
			m_tblDef->m_columns[3]->m_cacheUpdate = true;
		} else {
			for (u16 i = 0; i < m_tblDef->m_numCols; i++)
				m_tblDef->m_columns[i]->m_cacheUpdate = true;
		}
	}

	// ����MMS��
	m_mmsTable = new MmsTable(mms, m_db, m_heap, m_tblDef, m_tblDef->m_cacheUpdate, m_tblDef->m_updateCacheTime, partitionNr);
	mms->registerMmsTable(session, m_mmsTable);
	m_mms = mms;
}

/** 
 * ���ٶ����� ����������ǰ���ã�
 *
 * @param session �ػ�����
 * @param delHeapFile ɾ�����ļ�
 */
void FMmsTable::drop(Session *session, bool delHeapFile, bool flushDirty) {
	if (m_mmsTable != NULL) {
		EXCPT_OPER(m_mmsTable->close(session, flushDirty));
		EXCPT_OPER(m_mms->unregisterMmsTable(session, m_mmsTable));
		delete m_mmsTable;
		m_mmsTable = NULL;
	}
	if (m_heap != NULL) {
		EXCPT_OPER(m_heap->close(session, true));
		if (delHeapFile) {
			EXCPT_OPER(DrsHeap::drop(FIX_TBL_HEAP));
		}
		delete m_heap;
		m_heap = NULL;
	}

	if (m_tblDef != NULL) {
		if (delHeapFile) {
			EXCPT_OPER(TableDef::drop(FIX_TBL_DEF));
		}
		delete m_tblDef;
		m_tblDef = NULL;
	}
}

/** 
 * ���һ����¼
 *
 * @param session	�Ự����
 * @param userId	�û�ID
 * @param insertHeap д��DRS��
 * @return RowID
 */
RowId FMmsTable::insert(Session *session, u64 userId, bool insertHeap) {
	RowId rowId;
	u64 bankacc = userId + ((u64)userId << 32);
	u32	balance = (u32)((u64)(-1) - userId);
	Record *record;
	RecordBuilder rb(m_tblDef, 0, m_tblDef->m_recFormat);
	char username[Limits::PAGE_SIZE];
	MmsRecord *mmsRecord;
	RowLockHandle *rlh = NULL;

	rb.appendBigInt(userId);
	memcpy(username, m_namePrefix, m_prefixSize);
	memcpy(username + m_prefixSize, &userId, sizeof(u64));
	rb.appendChar(username);
	rb.appendBigInt(bankacc);
	rb.appendInt(balance);
	// TODO: �Բ��ԣ�
	record = rb.getRecord(0);
	
	if (insertHeap)
		record->m_rowId = m_heap->insert(session, record, &rlh);
	else
		record->m_rowId = userId; // α��userIdΪrowId
	rowId = record->m_rowId;
	EXCPT_OPER(mmsRecord = m_mmsTable->putIfNotExist(session, record));
	if (mmsRecord) {
		EXCPT_OPER(m_mmsTable->unpinRecord(session, mmsRecord));
	} else {
		;//EXCPT_OPER(NTSE_THROW);	
	}
	freeRecord(record);
	if (insertHeap)
		session->unlockRow(&rlh);
	return rowId;
}

RowId MmsTestCase::fixInsert(MmsTable *mmsTable, Session *session, u64 userId, bool insertHeap, DrsHeap *heap, const TableDef *tblDef) {
	RowId rowId;
	u64 bankacc = userId + ((u64)userId << 32);
	u32	balance = (u32)((u64)(-1) - userId);
	Record *record;
	RecordBuilder rb(tblDef, 0, tblDef->m_recFormat);
	char username[Limits::PAGE_SIZE];
	MmsRecord *mmsRecord;
	RowLockHandle *rlh = NULL;

	rb.appendBigInt(userId);
	memcpy(username, "mms", 3);
	memcpy(username + 3, &userId, sizeof(u64));
	rb.appendChar(username);
	rb.appendBigInt(bankacc);
	rb.appendInt(balance);
	// TODO: �Բ��ԣ�
	record = rb.getRecord(0);

	if (insertHeap)
		record->m_rowId = heap->insert(session, record, &rlh);
	else
		record->m_rowId = userId; // α��userIdΪrowId
	rowId = record->m_rowId;
	EXCPT_OPER(mmsRecord = mmsTable->putIfNotExist(session, record));
	if (mmsRecord) {
		EXCPT_OPER(mmsTable->unpinRecord(session, mmsRecord));
	} else {
		;//EXCPT_OPER(NTSE_THROW);	
	}
	freeRecord(record);
	if (insertHeap)
		session->unlockRow(&rlh);
	return rowId;
}

/** 
* ���һ����¼
*
* @param session	�Ự����
* @param userId	�û�ID
* @param insertHeap д��DRS��
* @return RowID
*/
RowId FMmsTable::insertEx(Session *session, u64 userId, bool insertHeap) {
	RowId rowId;
	u64 bankacc = userId + ((u64)userId << 32);
	u32	balance = (u32)((u64)(-1) - userId);
	Record *record;
	RecordBuilder rb(m_tblDef, 0, m_tblDef->m_recFormat);
	char username[Limits::PAGE_SIZE];
	MmsRecord *mmsRecord;
	RowLockHandle *rlh = NULL;

	rb.appendBigInt(userId);
	memcpy(username, m_namePrefix, m_prefixSize);
	memcpy(username + m_prefixSize, &userId, sizeof(u64));
	rb.appendChar(username);
	rb.appendBigInt(bankacc);
	rb.appendInt(balance);
	// TODO: �Բ��ԣ�
	record = rb.getRecord(0);

	if (insertHeap)
		record->m_rowId = m_heap->insert(session, record, &rlh);
	else
		record->m_rowId = userId; // α��userIdΪrowId
	rowId = record->m_rowId;
	EXCPT_OPER(mmsRecord = m_mmsTable->putIfNotExist(session, record));
	if (mmsRecord) {
		EXCPT_OPER(m_mmsTable->unpinRecord(session, mmsRecord));
	} else {
		freeRecord(record);
		if (insertHeap)
			session->unlockRow(&rlh);
		return (RowId)-1;
	}
	freeRecord(record);
	if (insertHeap)
		session->unlockRow(&rlh);
	return rowId;
}

/** 
* ���һ����ʵ��¼������ΪNatural��ʽ)
*
* @param session	�Ự����
* @param userId	�û�ID
* @param insertHeap д��DRS��
* @return RowID
*/
RowId FMmsTable::insertReal(Session *session, u64 userId, bool insertHeap) {
	RowId rowId;
	u64 bankacc = userId + ((u64)userId << 32);
	u32	balance = (u32)((u64)(-1) - userId);
	Record *record;
	RecordBuilder rb(m_tblDef, 0, m_tblDef->m_recFormat);
	char username[Limits::PAGE_SIZE];
	MmsRecord *mmsRecord;
	RowLockHandle *rlh = NULL;

	rb.appendBigInt(userId);
	memcpy(username, m_namePrefix, m_prefixSize);
	memcpy(username + m_prefixSize, &userId, sizeof(u64));
	rb.appendChar(username);
	rb.appendBigInt(bankacc);
	rb.appendInt(balance);
	// TODO: �Բ��ԣ�
	record = rb.getRecord(0);

	if (insertHeap)
		record->m_rowId = m_heap->insert(session, record, &rlh);
	else
		record->m_rowId = userId; // α��userIdΪrowId
	rowId = record->m_rowId;

	SubRecord keyRecord;

	keyRecord.m_format = KEY_NATURAL;
	keyRecord.m_numCols = m_tblDef->m_pkey->m_numCols;
	keyRecord.m_columns = m_tblDef->m_pkey->m_columns;
	keyRecord.m_size = m_tblDef->m_maxRecSize;
	keyRecord.m_data = (byte *)malloc(m_tblDef->m_maxRecSize);
	RecordOper::extractKeyFN(m_tblDef, m_tblDef->m_pkey, record, &keyRecord);

	EXCPT_OPER(mmsRecord = m_mmsTable->putIfNotExist(session, record));
	if (mmsRecord) {
		EXCPT_OPER(m_mmsTable->unpinRecord(session, mmsRecord));
	} else {
		;//EXCPT_OPER(NTSE_THROW);	
	}
	freeRecord(record);
	free(keyRecord.m_data);
	if (insertHeap)
		session->unlockRow(&rlh);
	return rowId;
}

/** 
 * �����¼����
 *
 * @param session �Ự
 * @param userId  �û�ID
 * @param insertHeap �Ƿ�����
 * @return RowID
 */
RowId FMmsTable::insertDouble(Session *session, u64 userId, bool insertHeap) {
	RowId rowId;
	u64 bankacc = userId + ((u64)userId << 32);
	u32 balance = (u32)((u64)(-1) - userId);
	Record *record;
	RecordBuilder rb(m_tblDef, 0, m_tblDef->m_recFormat);
	char username[Limits::PAGE_SIZE];
	MmsRecord *mmsRecord;
	RowLockHandle *rlh = NULL;

	rb.appendBigInt(userId);
	memcpy(username, m_namePrefix, m_prefixSize);
	memcpy(username + m_prefixSize, &userId, sizeof(u64));
	rb.appendChar(username);
	rb.appendBigInt(bankacc);
	rb.appendInt(balance);
	record = rb.getRecord(0);

	if (insertHeap)
		record->m_rowId = m_heap->insert(session, record, &rlh);
	else
		record->m_rowId = userId;
	rowId = record->m_rowId;
	for (int i = 0; i < 2; i++) {
		EXCPT_OPER(mmsRecord = m_mmsTable->putIfNotExist(session, record));
		if (mmsRecord) {
			EXCPT_OPER(m_mmsTable->unpinRecord(session, mmsRecord));
		} else {
			;//EXCPT_OPER(NTSE_THROW);
		}
	}
	freeRecord(record);
	if (insertHeap)
		session->unlockRow(&rlh);
	return rowId;
}

/**
 * ��ȡMMS��¼
 *
 * @param session �Ự
 * @param userId �û�ID
 * @param insertHeap �Ƿ�����
 */
void FMmsTable::getRecord(Session *session, u64 userId, bool insertHeap) {
	RowId rowId;
	u64 bankacc = userId + ((u64)userId << 32);
	u32 balance = (u32)((u64)(-1) - userId);
	Record *record;
	RecordBuilder rb(m_tblDef, 0, m_tblDef->m_recFormat);
	char username[Limits::PAGE_SIZE];
	MmsRecord *mmsRecord;
	RowLockHandle *rlh = NULL;

	rb.appendBigInt(userId);
	memcpy(username, m_namePrefix, m_prefixSize);
	memcpy(username + m_prefixSize, &userId, sizeof(u64));
	rb.appendChar(username);
	rb.appendBigInt(bankacc);
	rb.appendInt(balance);
	record = rb.getRecord(0);

	if (insertHeap)
		record->m_rowId = m_heap->insert(session, record, &rlh);
	else
		record->m_rowId = userId;
	rowId = record->m_rowId;
	for (int i = 0; i < 2; i++) {
		EXCPT_OPER(mmsRecord = m_mmsTable->putIfNotExist(session, record));
		if (mmsRecord) {
			EXCPT_OPER(m_mmsTable->unpinRecord(session, mmsRecord));
		} else {
			;//EXCPT_OPER(NTSE_THROW);
		}
	}
	if (insertHeap)
		session->unlockRow(&rlh);

	// ��MMS��ȡ��¼
	Record recordNew;

	recordNew.m_data = (byte *)malloc(m_tblDef->m_maxRecSize);
	
	CPPUNIT_ASSERT(mmsRecord = m_mmsTable->getByRid(session, rowId, false, NULL, None));

	m_mmsTable->getRecord(mmsRecord, &recordNew);

	CPPUNIT_ASSERT(RecordOper::isRecordEq(m_tblDef, record, &recordNew));

	free(recordNew.m_data);
	recordNew.m_data = NULL;

	// ����������
	CPPUNIT_ASSERT(mmsRecord = m_mmsTable->getByRid(session, rowId, false, NULL, None));

	m_mmsTable->getRecord(mmsRecord, &recordNew, false);

	CPPUNIT_ASSERT(RecordOper::isRecordEq(m_tblDef, record, &recordNew));

	freeRecord(record);
} 

/** 
 * ɾ��һ����¼
 *
 * @param session	�Ự����
 * @param userId	�û�ID
 * @return �Ƿ�ɹ�
 */
bool FMmsTable::del(Session *session, u64 userId) {
	bool succ = false;
	MmsRecord *mmsRecord;
	RowLockHandle *rlh = NULL;

	if (mmsRecord = this->selectByRid(session, userId, &rlh, Exclusived)) {
		EXCPT_OPER(m_mmsTable->del(session, mmsRecord));
		session->unlockRow(&rlh);
		return true;
	} else 
		return false;
}

/** 
 * ����һ����¼
 *
 * @param session	�Ự����
 * @param userId	�û�ID
 * @param updateId	����ID
 */
void FMmsTable::update(Session *session, u64 rowId, u64 updateId) {
	//u64 rowId = userId;
	byte username[Limits::PAGE_SIZE];
	u64 bankacc = updateId + ((u64)updateId << 32);
	u32	balance = (u32)((u64)(-1) - updateId);	
	SubRecordBuilder srb(m_tblDef, REC_REDUNDANT, rowId);
	SubRecord *subRecord;
	MmsRecord *mmsRecord;
	const char *cols = "0 1 2 3";
	RowLockHandle *rlh = NULL;

	memcpy(username, m_namePrefix, m_prefixSize);
	memcpy(username + m_prefixSize, &updateId, sizeof(u64));
	subRecord = srb.createSubRecordById(cols, &updateId, username, &bankacc, &balance);
	subRecord->m_rowId = rowId;
	EXCPT_OPER(mmsRecord = m_mmsTable->getByRid(session, rowId, true, &rlh, Exclusived));
	if (mmsRecord) {
		u16 recSize;
		if (m_mmsTable->canUpdate(mmsRecord, subRecord, &recSize))
			m_mmsTable->update(session, mmsRecord, subRecord, recSize);
		else
			m_mmsTable->flushAndDel(session, mmsRecord);
		session->unlockRow(&rlh);
	}
	freeSubRecord(subRecord);
}

/** 
 * ���µ�2���ֶ�
 *
 * @param session �Ự
 * @param rowId RowID
 * @param updateId ����ID
 */
void FMmsTable::updateColumn1(Session *session, u64 rowId, u64 updateId) {
	//u64 rowId = userId;
	byte username[Limits::PAGE_SIZE];
	SubRecordBuilder srb(m_tblDef, REC_REDUNDANT, rowId);
	SubRecord *subRecord;
	MmsRecord *mmsRecord;
	const char *cols = "1";
	RowLockHandle *rlh = NULL;

	memcpy(username, m_namePrefix, m_prefixSize);
	memcpy(username + m_prefixSize, &updateId, sizeof(u64));
	subRecord = srb.createSubRecordById(cols, username);
	subRecord->m_rowId = rowId;
	EXCPT_OPER(mmsRecord = m_mmsTable->getByRid(session, rowId, true, &rlh, Exclusived));
	if (mmsRecord) {
		//EXCPT_OPER(m_mmsTable->update(session, mmsRecord, subRecord));
		u16 recSize;
		if (m_mmsTable->canUpdate(mmsRecord, subRecord, &recSize))
			m_mmsTable->update(session, mmsRecord, subRecord, recSize);
		else
			m_mmsTable->flushAndDel(session, mmsRecord);
		session->unlockRow(&rlh);
	}
	freeSubRecord(subRecord);
}

/** 
* ����һ����ʵ��¼������ΪNatural��ʽ)
*
* @param session	�Ự����
* @param userId	�û�ID
* @param updateId	����ID
*/
void FMmsTable::updateReal(Session *session, u64 rowId, u64 updateId) {
	//u64 rowId = userId;
	byte username[Limits::PAGE_SIZE];
	u64 bankacc = updateId + ((u64)updateId << 32);
	u32	balance = (u32)((u64)(-1) - updateId);	
	SubRecordBuilder srb(m_tblDef, REC_REDUNDANT, rowId);
	SubRecord *subRecord;
	MmsRecord *mmsRecord;
	const char *cols = "0 1 2 3";
	RowLockHandle *rlh = NULL;

	memcpy(username, m_namePrefix, m_prefixSize);
	memcpy(username + m_prefixSize, &updateId, sizeof(u64));
	subRecord = srb.createSubRecordById(cols, &updateId, username, &bankacc, &balance);
	subRecord->m_rowId = rowId;
	EXCPT_OPER(mmsRecord = m_mmsTable->getByRid(session, rowId, true, &rlh, Exclusived));
	if (mmsRecord) {
		Record record;
		record.m_rowId = rowId;
		record.m_format = REC_FIXLEN;

		record.m_data = (byte *)malloc(mmsRecord->m_size);
		memcpy(record.m_data, (byte *)mmsRecord + sizeof(MmsRecord), mmsRecord->m_size);
		record.m_size = mmsRecord->m_size;

		RecordOper::updateRecordFR(m_tblDef, &record, subRecord);

		SubRecord keyRecord;

		keyRecord.m_format = KEY_NATURAL;
		keyRecord.m_numCols = m_tblDef->m_pkey->m_numCols;
		keyRecord.m_columns = m_tblDef->m_pkey->m_columns;
		keyRecord.m_data = (byte *)malloc(m_tblDef->m_maxRecSize);
		keyRecord.m_size = m_tblDef->m_maxRecSize;
		RecordOper::extractKeyFN(m_tblDef, m_tblDef->m_pkey, &record, &keyRecord);

		u16 recSize;
		if (m_mmsTable->canUpdate(mmsRecord, subRecord, &recSize))
			m_mmsTable->update(session, mmsRecord, subRecord, recSize);
		else {
			m_mmsTable->flushAndDel(session, mmsRecord);
			// TODO: ʵ�ʶѸ���
		}
		session->unlockRow(&rlh);
		free(keyRecord.m_data);
		free(record.m_data);
	}
	freeSubRecord(subRecord);
}

/** 
* ����һ����ʵ��¼����չ�汾������ΪNatural��ʽ)
*
* @param session	�Ự����
* @param userId	�û�ID
* @param updateId	����ID
*/
void FMmsTable::updateRealEx(Session *session, u64 rowId, u64 updateId) {
	//u64 rowId = userId;
	byte username[Limits::PAGE_SIZE];
	u64 bankacc = updateId + ((u64)updateId << 32);
	u32	balance = (u32)((u64)(-1) - updateId);	
	SubRecordBuilder srb(m_tblDef, REC_REDUNDANT, rowId);
	SubRecord *subRecord;
	MmsRecord *mmsRecord;
	const char *cols = "0 1 2";
	RowLockHandle *rlh = NULL;

	memcpy(username, m_namePrefix, m_prefixSize);
	memcpy(username + m_prefixSize, &updateId, sizeof(u64));
	subRecord = srb.createSubRecordById(cols, &updateId, username, &bankacc);
	subRecord->m_rowId = rowId;
	EXCPT_OPER(mmsRecord = m_mmsTable->getByRid(session, rowId, true, &rlh, Exclusived));
	if (mmsRecord) {
		Record record;
		record.m_rowId = rowId;
		record.m_format = REC_FIXLEN;

		record.m_data = (byte *)malloc(mmsRecord->m_size);
		memcpy(record.m_data, (byte *)mmsRecord + sizeof(MmsRecord), mmsRecord->m_size);
		record.m_size = mmsRecord->m_size;

		RecordOper::updateRecordFR(m_tblDef, &record, subRecord);

		SubRecord keyRecord;

		keyRecord.m_format = KEY_NATURAL;
		keyRecord.m_numCols = m_tblDef->m_pkey->m_numCols;
		keyRecord.m_columns = m_tblDef->m_pkey->m_columns;
		keyRecord.m_data = (byte *)malloc(m_tblDef->m_maxRecSize);
		keyRecord.m_size = m_tblDef->m_maxRecSize;
		RecordOper::extractKeyFN(m_tblDef, m_tblDef->m_pkey, &record, &keyRecord);

		u16 recSize;
		if (m_mmsTable->canUpdate(mmsRecord, subRecord, &recSize))
			m_mmsTable->update(session, mmsRecord, subRecord, recSize);
		else {
			m_mmsTable->flushAndDel(session, mmsRecord);
			// TODO: ʵ�ʶѸ���
		}
		session->unlockRow(&rlh);
		free(keyRecord.m_data);
		free(record.m_data);
	}
	freeSubRecord(subRecord);
}

/** 
 * ����RID��ȡһ����¼
 * 
 * @param session	�Ự����
 * @param rowId		RID
 * @param rlh		INOUT �������
 * @param lockMode	��ģʽ
 */
MmsRecord* FMmsTable::selectByRid(Session *session, u64 rowId, RowLockHandle **rlh, LockMode lockMode) {
	MmsRecord *mmsRecord;

	EXCPT_OPER(mmsRecord = m_mmsTable->getByRid(session, rowId, true, rlh, lockMode));
	return mmsRecord;
}

/** 
 * �ͷż�¼�������pin
 *
 * @param session	�Ự����
 * @param mmsRecord	MMS��¼, ����ΪNULL, ����Ҫ�ͷ�pin
 * @param rlh		�������, ����ΪNULL������Ҫ�ͷ�����
 */
void FMmsTable::release(Session *session, MmsRecord *mmsRecord, RowLockHandle **rlh) {
	if (mmsRecord)
		EXCPT_OPER(m_mmsTable->unpinRecord(session, mmsRecord));
	if (rlh)
		session->unlockRow(rlh);
}

/** 
 * ��ȡMMS��
 *
 * @return MMS��
 */
MmsTable* FMmsTable::getMmsTable() {
	return m_mmsTable;
}

/**
 * ��ȡTID
 *
 * @return TID
 */
int FMmsTable::getTid() {
	return m_tid;
}

TableDef* FMmsTable::getTableDef() {
	return m_tblDef;
}
/** 
 * ��ȡ��ָ��
 *
 * @return ��ָ��
 */
DrsHeap* FMmsTable::getHeap() {
	return m_heap;
}


/************************************************************************/
/*                     �䳤��VMmsTableʵ��                              */
/************************************************************************/

/** 
 * �����䳤�� (���캯������ã�
 *
 * @param session   �Ự����
 * @param db		�������ݿ�
 * @param tid		��ID
 * @param schema    ģʽ��
 * @param tablename	����
 * @param cacheUpdate �������
 * @param colUpdate �ֶθ���
 * @param varcharPK �䳤����
 */
void VMmsTable::create(Session *session, 
					   Database *db, 
					   Mms *mms, 
					   int tid, 
					   const char *schema, 
					   const char *tablename, 
					   /*const char *heapfilename,*/
					   int maxNameSize,
					   bool cacheUpdate,
					   bool colUpdate,
					   bool varcharPK,
					   bool paritalUpdate, 
					   bool delHeapFile,
					   int partitionNr) {
	m_db = db;
	m_tid = tid;
	m_maxNameSize = maxNameSize;
	m_varcharPK = varcharPK;

	if (m_tblDef != NULL) {
		delete m_tblDef;
		m_tblDef = NULL;
	}

	// �����䳤��
	if (delHeapFile) {
		File fHeap(VAR_TBL_HEAP);
		fHeap.remove();
		TableDef::drop(VAR_TBL_DEF);
	}
	TableDefBuilder *builder = new TableDefBuilder(tid, schema, tablename);
	builder->addColumn("UserId", CT_BIGINT, false)->addColumnS("UserName", CT_VARCHAR, maxNameSize, false, false);
	builder->addColumn("BankAccount", CT_BIGINT)->addColumn("Balance", CT_INT);
	if (varcharPK)
		builder->addIndex("PRIMARY", true, true, false, "UserName", 0, NULL);
	else
		builder->addIndex("PRIMARY", true, true, false, "UserId", 0, NULL);
	if (delHeapFile) {
		m_tblDef = builder->getTableDef();
		EXCPT_OPER(m_tblDef->writeFile(VAR_TBL_DEF));
		EXCPT_OPER(DrsHeap::create(m_db, VAR_TBL_HEAP, m_tblDef));
	} else {
		m_tblDef = TableDef::open(VAR_TBL_DEF);
	}
	EXCPT_OPER(m_heap = DrsHeap::open(m_db, session, VAR_TBL_HEAP, m_tblDef));
	delete builder;

	if(cacheUpdate) {
		m_tblDef->m_cacheUpdate = true;
		m_tblDef->m_updateCacheTime = 10;
	} else {
		m_tblDef->m_cacheUpdate = false;
		m_tblDef->m_updateCacheTime = 10;
	}

	if (colUpdate) {
		if (paritalUpdate) {
			m_tblDef->m_columns[0]->m_cacheUpdate = false;
			m_tblDef->m_columns[1]->m_cacheUpdate = true;
			m_tblDef->m_columns[2]->m_cacheUpdate = false;
			m_tblDef->m_columns[3]->m_cacheUpdate = true;
		} else {
			for (u16 i = 0; i < m_tblDef->m_numCols; i++)
				m_tblDef->m_columns[i]->m_cacheUpdate = true;
		}
	}

	// ����MMS��
	m_mmsTable = new MmsTable(mms, m_db, m_heap, m_tblDef, m_tblDef->m_cacheUpdate, m_tblDef->m_updateCacheTime, partitionNr);
	mms->registerMmsTable(session, m_mmsTable);	
	m_mms = mms;
}

/** 
 * ���ٱ䳤�� ����������ǰ���ã�
 *
 * @param session �Ự����
 * @param delHeapFile ɾ�����ļ�
 */
void VMmsTable::drop(Session *session, bool delHeapFile, bool flushDirty) {
	if (m_mmsTable) {
		EXCPT_OPER(m_mmsTable->close(session, flushDirty));
		EXCPT_OPER(m_mms->unregisterMmsTable(session, m_mmsTable));
		delete m_mmsTable;
		m_mmsTable = NULL;
	}
	if (m_heap) {
		EXCPT_OPER(m_heap->close(session, true));
		if (delHeapFile) {
			EXCPT_OPER(DrsHeap::drop(VAR_TBL_HEAP));
		}
		delete m_heap;
		m_heap = NULL;
	}

	if (m_tblDef != NULL) {
		if (delHeapFile) {
			TableDef::drop(VAR_TBL_DEF);
		}
		delete m_tblDef;
		m_tblDef = NULL;
	}
}

/** 
 * ���һ����¼
 *
 * @param session	�Ự����
 * @param userId	�û�ID
 * @param username  �û���
 * @param nameSize	�û�������
 * @param insertHeap ����DRS��
 * @return RowID
 */
RowId VMmsTable::insert(Session *session, u64 userId, const char *username, int nameSize, bool insertHeap) {
	u64 rowId;
	u64 bankacc = userId + ((u64)userId << 32);
	u32	balance = (u32)((u64)(-1) - userId);
	Record *record;
	RecordBuilder rb(m_tblDef, 0, m_tblDef->m_recFormat);
	MmsRecord *mmsRecord;
	RowLockHandle *rlh = NULL;

	rb.appendBigInt(userId);
	rb.appendVarchar(username);
	rb.appendBigInt(bankacc);
	rb.appendInt(balance);
	// TODO: 
	record = rb.getRecord(0);
	if (insertHeap)
		record->m_rowId = m_heap->insert(session, record, &rlh);
	else
		record->m_rowId = userId; // α��rowIdΪuserId
	rowId = record->m_rowId;
	EXCPT_OPER(mmsRecord = m_mmsTable->putIfNotExist(session, record));
	if (mmsRecord) {
		rowId = RID_READ(mmsRecord->m_rid);
		EXCPT_OPER(m_mmsTable->unpinRecord(session, mmsRecord));
	} else {
		;//EXCPT_OPER(NTSE_THROW);	
	}
	freeRecord(record);
	if (insertHeap)
		session->unlockRow(&rlh);
	return rowId;
}

/** 
* ���һ����ʵ��¼(����ΪNatural��ʽ)
*
* @param session	�Ự����
* @param userId	�û�ID
* @param username  �û���
* @param nameSize	�û�������
* @param insertHeap ����DRS��
* @return RowID
*/
RowId VMmsTable::insertReal(Session *session, u64 userId, const char *username, int nameSize, bool insertHeap) {
	u64 rowId;
	u64 bankacc = userId + ((u64)userId << 32);
	u32	balance = (u32)((u64)(-1) - userId);
	Record *record;
	RecordBuilder rb(m_tblDef, 0, m_tblDef->m_recFormat);
	MmsRecord *mmsRecord;
	RowLockHandle *rlh = NULL;

	rb.appendBigInt(userId);
	rb.appendVarchar(username);
	rb.appendBigInt(bankacc);
	rb.appendInt(balance);
	// TODO: 
	record = rb.getRecord(0);
	if (insertHeap)
		record->m_rowId = m_heap->insert(session, record, &rlh);
	else
		record->m_rowId = userId; // α��rowIdΪuserId
	rowId = record->m_rowId;
	assert(m_varcharPK);

	SubRecord keyRecord;

	keyRecord.m_format = KEY_NATURAL;
	keyRecord.m_numCols = m_tblDef->m_pkey->m_numCols;
	keyRecord.m_columns = m_tblDef->m_pkey->m_columns;
	keyRecord.m_data = (byte *)malloc(m_tblDef->m_maxRecSize);
	keyRecord.m_size = m_tblDef->m_maxRecSize;
	RecordOper::extractKeyVN(m_tblDef, m_tblDef->m_pkey, record, NULL, &keyRecord);
		
	EXCPT_OPER(mmsRecord = m_mmsTable->putIfNotExist(session, record));
	if (mmsRecord) {
		EXCPT_OPER(m_mmsTable->unpinRecord(session, mmsRecord));
	} else {
		;//EXCPT_OPER(NTSE_THROW);	
	}
	freeRecord(record);
	free(keyRecord.m_data);
	if (insertHeap)
		session->unlockRow(&rlh);
	return rowId;
}

/** 
 * ɾ��һ����¼
 *
 * @param session	�Ự����
 * @param rowId		RID
 * @return �Ƿ�ɹ�
 */
bool VMmsTable::del(Session *session, u64 rowId) {
	MmsRecord *mmsRecord;
	RowLockHandle *rlh = NULL;
	
	if (mmsRecord = selectByRid(session, rowId, &rlh, Exclusived)) {
		EXCPT_OPER(m_mmsTable->del(session, mmsRecord));
		session->unlockRow(&rlh);
		return true;
	} else
		return false;
}

/** 
 * ɾ��һ����¼
 *
 * @param session	�Ự����
 * @param username	�û���
 * @param nameSize  �û�������
 * @return �Ƿ�ɹ�
 */
//bool VMmsTable::del(Session *session, const char *username, int nameSize) {
//	MmsRecord *mmsRecord;
//	RowLockHandle *rlh = NULL;
//
//	assert(m_varcharPK);
//	if (mmsRecord = selectByPK(session, username, nameSize, &rlh, Exclusived)) {
//		EXCPT_OPER(m_mmsTable->del(mmsRecord));
//		session->unlockRow(&rlh);
//		return true;
//	} else
//		return false;
//}

/** 
* ����һ����¼
*
* @param session	�Ự����
* @param userId	�û�ID
* @param updateId	����ID
*/
void VMmsTable::update(Session *session, u64 rowId, u64 updateId) {
	byte username[Limits::PAGE_SIZE];
	u64 bankacc = updateId + ((u64)updateId << 32);
	u32	balance = (u32)((u64)(-1) - updateId);	
	SubRecordBuilder srb(m_tblDef, REC_REDUNDANT, rowId);
	SubRecord *subRecord;
	MmsRecord *mmsRecord;
	const char *cols = "0 1 2 3";
	RowLockHandle *rlh;
	int nameSize = (int) (updateId % (m_maxNameSize - 1) + 1);  // ���ֳ�����СΪ1
 
	memset(username, 1, nameSize);
	username[nameSize] = '\0';
	subRecord = srb.createSubRecordById(cols, &updateId, username, &bankacc, &balance);
	EXCPT_OPER(mmsRecord = m_mmsTable->getByRid(session, rowId, true, &rlh, Exclusived));
	if (mmsRecord) {
		u16 recSize;
		if (m_mmsTable->canUpdate(mmsRecord, subRecord, &recSize))
			m_mmsTable->update(session, mmsRecord, subRecord, recSize);
		else
			m_mmsTable->flushAndDel(session, mmsRecord);
		session->unlockRow(&rlh);
	}
	freeSubRecord(subRecord);
}

/** 
 * ��ͨ����
 *
 * @param session �Ự
 * @param rowId RowID
 * @param username �û���
 */
void VMmsTable::update(Session *session, u64 rowId, char *username) {
	SubRecordBuilder srb(m_tblDef, REC_REDUNDANT, rowId);
	SubRecord *subRecord;
	MmsRecord *mmsRecord;
	const char *cols = "1";
	RowLockHandle *rlh = NULL;

	subRecord = srb.createSubRecordById(cols, username);
	subRecord->m_rowId = rowId;
	EXCPT_OPER(mmsRecord = m_mmsTable->getByRid(session, rowId, true, &rlh, Exclusived));
	if (mmsRecord) {
		u16 recSize;
		if (m_mmsTable->canUpdate(mmsRecord, subRecord, &recSize))
			m_mmsTable->update(session, mmsRecord, subRecord, recSize);
		else
			m_mmsTable->flushAndDel(session, mmsRecord);
		session->unlockRow(&rlh);
	}
	freeSubRecord(subRecord);
}

/** 
 * ��ͨ����
 *
 * @param session �Ự
 * @param rowId RowID
 * @param username �û���
 * @param touch �Ƿ����ʱ���
 */
void VMmsTable::update(Session *session, u64 rowId, char *username, bool touch) {
	SubRecordBuilder srb(m_tblDef, REC_REDUNDANT, rowId);
	SubRecord *subRecord;
	MmsRecord *mmsRecord;
	const char *cols = "1";
	RowLockHandle *rlh = NULL;

	subRecord = srb.createSubRecordById(cols, username);
	subRecord->m_rowId = rowId;
	EXCPT_OPER(mmsRecord = m_mmsTable->getByRid(session, rowId, touch, &rlh, Exclusived));
	if (mmsRecord) {
		u16 recSize;
		if (m_mmsTable->canUpdate(mmsRecord, subRecord, &recSize))
			m_mmsTable->update(session, mmsRecord, subRecord, recSize);
		else
			m_mmsTable->flushAndDel(session, mmsRecord);
		session->unlockRow(&rlh);
	}
	freeSubRecord(subRecord);
}

/** 
 * ʵ�ʸ���
 *
 * @param session �Ự
 * @param rowId RowID
 * @param username �û���
 */
void VMmsTable::updateReal(Session *session, u64 rowId, const char *username) {
	SubRecordBuilder srb(m_tblDef, REC_REDUNDANT, rowId);
	SubRecord *subRecord;
	MmsRecord *mmsRecord;
	const char *cols = "1";
	RowLockHandle *rlh = NULL;

	subRecord = srb.createSubRecordById(cols, username);
	subRecord->m_rowId = rowId;
	EXCPT_OPER(mmsRecord = m_mmsTable->getByRid(session, rowId, true, &rlh, Exclusived));
	if (!mmsRecord) {
		freeSubRecord(subRecord);
		return;//TODO: ʵ��heap����
	}
	assert(m_varcharPK);
	
	Record record;
	record.m_rowId = rowId;
	record.m_format = REC_VARLEN;
	record.m_data = (byte *)malloc(m_tblDef->m_maxRecSize);
	memcpy(record.m_data, (byte *)mmsRecord + sizeof(MmsRecord), mmsRecord->m_size);
	record.m_size = mmsRecord->m_size;

	RecordOper::updateRecordVRInPlace(m_tblDef, &record, subRecord, m_tblDef->m_maxRecSize);
	
	u16 recSize;
	if (m_mmsTable->canUpdate(mmsRecord, subRecord, &recSize))
		m_mmsTable->update(session, mmsRecord, subRecord, recSize);
	else
		m_mmsTable->flushAndDel(session, mmsRecord);
	session->unlockRow(&rlh);
	freeSubRecord(subRecord);
	free(record.m_data);
}

/** 
 * ���±䳤���1�ֶ�
 *
 * @param session �Ự
 * @param rowId RowID
 * @param userId �û���ID
 */
void VMmsTable::updateColumn0(Session *session, u64 rowId, u64 userId) {
	SubRecordBuilder srb(m_tblDef, REC_REDUNDANT, rowId);
	const char *cols = "0";
	SubRecord *subRecord;
	MmsRecord *mmsRecord;
	RowLockHandle *rlh = NULL;

	subRecord = srb.createSubRecordById(cols, &userId);
	subRecord->m_rowId = rowId;
	EXCPT_OPER(mmsRecord = m_mmsTable->getByRid(session, rowId, true, &rlh, Exclusived));
	if (mmsRecord) {
		u16 recSize;
		if (m_mmsTable->canUpdate(mmsRecord, subRecord, &recSize))
			m_mmsTable->update(session, mmsRecord, subRecord, recSize);
		else
			m_mmsTable->flushAndDel(session, mmsRecord);
		session->unlockRow(&rlh);
	}
	freeSubRecord(subRecord);
}

/** 
 * ���±䳤���2�ֶ�
 *
 * @param session �Ự
 * @param rowId RowID
 * @param username �û���
 */
void VMmsTable::updateColumn1(Session *session, u64 rowId, char *username) {
	SubRecordBuilder srb(m_tblDef, REC_REDUNDANT, rowId);
	const char *cols = "1";
	SubRecord *subRecord;
	MmsRecord *mmsRecord;
	RowLockHandle *rlh = NULL;

	subRecord = srb.createSubRecordById(cols, username);
	subRecord->m_rowId = rowId;
	EXCPT_OPER(mmsRecord = m_mmsTable->getByRid(session, rowId, true, &rlh, Exclusived));
	if (mmsRecord) {
		u16 recSize;
		if (m_mmsTable->canUpdate(mmsRecord, subRecord, &recSize))
			m_mmsTable->update(session, mmsRecord, subRecord, recSize);
		else
			m_mmsTable->flushAndDel(session, mmsRecord);
		session->unlockRow(&rlh);
	}
	freeSubRecord(subRecord);
}

/** 
 * ���±䳤���4�ֶ�
 *
 * @param session �Ự
 * @param rowId RowID
 * @param userId �û���ID
 */
void VMmsTable::updateColumn3(Session *session, u64 rowId, u64 userId) {
	u32	balance = (u32)((u64)(-1) - userId);
	SubRecordBuilder srb(m_tblDef, REC_REDUNDANT, rowId);
	const char *cols = "3";
	SubRecord *subRecord;
	MmsRecord *mmsRecord;
	RowLockHandle *rlh = NULL;

	subRecord = srb.createSubRecordById(cols, &balance);
	subRecord->m_rowId = rowId;
	EXCPT_OPER(mmsRecord = m_mmsTable->getByRid(session, rowId, true, &rlh, Exclusived));
	u16 recSize;
	if (m_mmsTable->canUpdate(mmsRecord, subRecord, &recSize))
		m_mmsTable->update(session, mmsRecord, subRecord, recSize);
	else
		m_mmsTable->flushAndDel(session, mmsRecord);
	session->unlockRow(&rlh);
	freeSubRecord(subRecord);
}

/** 
 * ���±䳤��2��4�ֶ�
 *
 * @param session �Ự
 * @param rowId RowID
 * @param userId �û���ID
 * @param username �û���
 */
void VMmsTable::updateColumn13(Session *session, u64 rowId, u64 userId, char *username) {
	u32	balance = (u32)((u64)(-1) - userId);
	SubRecordBuilder srb(m_tblDef, REC_REDUNDANT, rowId);
	const char *cols = "1 3";
	SubRecord *subRecord;
	MmsRecord *mmsRecord;
	RowLockHandle *rlh = NULL;

	subRecord = srb.createSubRecordById(cols, username, &balance);
	subRecord->m_rowId = rowId;
	EXCPT_OPER(mmsRecord = m_mmsTable->getByRid(session, rowId, true, &rlh, Exclusived));
	u16 recSize;
	if (m_mmsTable->canUpdate(mmsRecord, subRecord, &recSize))
		m_mmsTable->update(session, mmsRecord, subRecord, recSize);
	else
		m_mmsTable->flushAndDel(session, mmsRecord);
	session->unlockRow(&rlh);
	freeSubRecord(subRecord);
}

/** 
 * ���±䳤��3��4�ֶ�
 *
 * @param session �Ự
 * @param rowId	RowID
 * @param userId �û�ID
 */
void VMmsTable::updateColumn23(Session *session, u64 rowId, u64 userId) {
	u64 bankacc = userId + ((u64)userId << 32);
	u32	balance = (u32)((u64)(-1) - userId);
	SubRecordBuilder srb(m_tblDef, REC_REDUNDANT, rowId);
	const char *cols = "2 3";
	SubRecord *subRecord;
	MmsRecord *mmsRecord;
	RowLockHandle *rlh = NULL;

	subRecord = srb.createSubRecordById(cols, &bankacc, &balance);
	subRecord->m_rowId = rowId;
	EXCPT_OPER(mmsRecord = m_mmsTable->getByRid(session, rowId, true, &rlh, Exclusived));
	if (mmsRecord) {
		u16 recSize;
		if (m_mmsTable->canUpdate(mmsRecord, subRecord, &recSize))
			m_mmsTable->update(session, mmsRecord, subRecord, recSize);
		else
			m_mmsTable->flushAndDel(session, mmsRecord);
		session->unlockRow(&rlh);
	}
	freeSubRecord(subRecord);
}

/** 
 * ���±䳤��1-4�ֶ�
 *
 * @param session �Ự
 * @param rowId RowID
 * @param userId �û���ID
 * @param username �û���
 */
void VMmsTable::updateColumn0123(Session *session, u64 rowId, u64 userId, char *username) {
	u64 bankacc = userId + ((u64)userId << 32);
	u32	balance = (u32)((u64)(-1) - userId);
	SubRecordBuilder srb(m_tblDef, REC_REDUNDANT, rowId);
	const char *cols = "0 1 2 3";
	SubRecord *subRecord;
	MmsRecord *mmsRecord;
	RowLockHandle *rlh = NULL;

	subRecord = srb.createSubRecordById(cols, &userId, username, &bankacc, &balance);
	subRecord->m_rowId = rowId;
	EXCPT_OPER(mmsRecord = m_mmsTable->getByRid(session, rowId, true, &rlh, Exclusived));
	if (mmsRecord) {
		u16 recSize;
		if (m_mmsTable->canUpdate(mmsRecord, subRecord, &recSize))
			m_mmsTable->update(session, mmsRecord, subRecord, recSize);
		else
			m_mmsTable->flushAndDel(session, mmsRecord);
		session->unlockRow(&rlh);
	}
	freeSubRecord(subRecord);
}

/** 
 * ����RID��ȡһ����¼
 * 
 * @param session	�Ự����
 * @param rowId		RID
 * @param rlh		INOUT �������
 * @param lockMode   ��������
 * @return MMS��¼��
 */
MmsRecord* VMmsTable::selectByRid(Session *session, u64 rowId, RowLockHandle **rlh, LockMode lockMode) {
	MmsRecord *mmsRecord;

	EXCPT_OPER(mmsRecord = m_mmsTable->getByRid(session, rowId, true, rlh, lockMode));
	return mmsRecord;
}

/** 
 * ����������ȡһ����¼
 *
 * @param session	�Ự����
 * @param userName	�û���
 * @param nameSize	�û�������
 * @param rlh		INOUT �������
 * @param lockMode	��������
 * @return MMS��¼��
 */
//MmsRecord* VMmsTable::selectByPK(Session *session, const char* userName, int nameSize, RowLockHandle **rlh, LockMode lockMode) {
//	MmsRecord *mmsRecord;
//	
//	assert(m_varcharPK);
//	EXCPT_OPER(mmsRecord = m_mmsTable->getByPrimaryKey(session, (const byte *)userName, nameSize, rlh, lockMode));
//	return mmsRecord;
//}

/** 
 * ����ʵ������ֵ��ѯMMS��¼��
 *
 * @param session �Ự
 * @param userName �û���
 * @param nameSize �û�������
 * @param rlh ��¼�����
 * @param lockMode ��¼������
 * @return MMS��¼��
 */
//MmsRecord* VMmsTable::selectByPKReal(Session *session, const char* userName, int nameSize, RowLockHandle **rlh, LockMode lockMode) {
//	SubRecordBuilder srb(m_tblDef, KEY_NATURAL);
//	SubRecord *subRecord;
//	MmsRecord *mmsRecord;
//	char *cols = "1";
//
//	subRecord = srb.createSubRecordById(cols, userName);
//	EXCPT_OPER(mmsRecord = m_mmsTable->getByPrimaryKey(session, subRecord->m_data, subRecord->m_size, rlh, lockMode));
//	freeSubRecord(subRecord);
//	return mmsRecord;
//}

/** 
 * ��ȡMMS��
 *
 * @param MMS��
 */
MmsTable* VMmsTable::getMmsTable() {
	return m_mmsTable;
}

TableDef* VMmsTable::getTableDef() {
	return m_tblDef;
}

/** 
 * ��ȡ��ָ��
 *
 * @return ��ָ��
 */
DrsHeap* VMmsTable::getHeap() {
	return m_heap;
}

/************************************************************************/
/*                     ������MmsTesterʵ��                              */
/************************************************************************/

// �������߳�
void MmsTester::run() {
	switch(m_task) {
	case MTT_DOTOUCH:
		m_testCase->doTestTouch(this);
		break;
	case MTT_DEL_DOTOUCH:
		m_testCase->doTestDelWhenTouch(this);
		break;
	case MTT_GETRID_WHEN_LOCKROW_0:
	case MTT_GETRID_WHEN_LOCKROW_1:
	case MTT_GETRID_WHEN_LOCKROW_2:
	case MTT_GETRID_WHEN_LOCKROW_3:
		m_testCase->doGetRidWhenLockRow(this);
		break;
	case MTT_GETRID_TRYLOCK:
		m_testCase->doTestTryLockWhenGetByRid(this, m_task);
		break;
	case MTT_GETRID_DISABLEPG:
		m_testCase->doTestDisablePgWhenGetByRid(this, m_task);
		break;
	case MTT_PUT_DISABLEPG:
		m_testCase->doTestDisablePgWhenPutIfNotExist(this, m_task);
		break;
	case MTT_GETSUBRECORD_DISABLEPG:
		m_testCase->doTestDisablePgWhenGetSubRecord(this, m_task);
		break;
	case MTT_GETSESSION_WHEN_FLUSHLOG_1:
	case MTT_GETSESSION_WHEN_FLUSHLOG_2:
		m_testCase->doGetSessionWhenFlushLog(this, m_task);
		break;
	case MTT_FLUSHLOG_VICTIMPAGE:
		m_testCase->doTestPageType(this);
		break;
	case MTT_FLUSH_VICTIMPAGE:
		m_testCase->doTestPageTypeWhenFlush(this);
		break;
	case MTT_DIRTY_REC:
		m_testCase->doTestPageTypeWhenGetDirRec(this);
		break;
	case MTT_GETPIN_REPLACEMENT:
		m_testCase->doReplaceWhenPinTopPage(this);
		break;
	case MTT_AMP_GET_TBL:
	case MTT_AMP_GET_PG:
	case MTT_AMP_PIN_PG:
		m_testCase->doTestAllocMmsPage(this, m_task);
		break;
	case MTT_FLUSHLOG_RPCLASS:
		m_testCase->doTestFlushLogRpClass(this);
		break;
	case MTT_SORTFLUSH_RPCLASS:
		m_testCase->doTestSortFlushRpClass(this);
		break;
	default:
		break;
	}
}

// ���������߳�
void MmsTestHelper::run() {
	switch(m_task) {
	case MTT_DOTOUCH:
		m_testCase->doHelpTestTouch(m_tester);
		break;
	case MTT_DEL_DOTOUCH:
		m_testCase->doHelpTestDelWhenTouch(m_tester);
		break;
	case MTT_GETRID_WHEN_LOCKROW_0:
	case MTT_GETRID_WHEN_LOCKROW_1:
	case MTT_GETRID_WHEN_LOCKROW_2:
	case MTT_GETRID_WHEN_LOCKROW_3:
		m_testCase->doHelpGetRidWhenLockRow(m_tester, m_task);
		break;
	case MTT_GETRID_TRYLOCK:
		m_testCase->doHelpTryLockWhenGetByRid(m_tester, m_task);
		break;
	case MTT_GETRID_DISABLEPG:
		m_testCase->doHelpDisablePgWhenGetByRid(m_tester, m_task);
		break;
	case MTT_PUT_DISABLEPG:
		m_testCase->doHelpDisablePgWhenPutIfNotExist(m_tester, m_task);
		break;
	case MTT_GETSUBRECORD_DISABLEPG:
		m_testCase->doHelpDisablePgWhenGetSubRecord(m_tester, m_task);
		break;
	case MTT_GETSESSION_WHEN_FLUSHLOG_1:
	case MTT_GETSESSION_WHEN_FLUSHLOG_2:
		m_testCase->doHelpGetSessionWhenFlushLog(m_tester, m_task);
		break;
	case MTT_FLUSHLOG_VICTIMPAGE:
		m_testCase->doHelpTestPageType(m_tester);
		break;
	case MTT_FLUSH_VICTIMPAGE:
		m_testCase->doHelpTestPageTypeWhenFlush(m_tester);
		break;
	case MTT_DIRTY_REC:
		m_testCase->doHelpTestPageTypeWhenGetDirRec(m_tester);
		break;
	case MTT_GETPIN_REPLACEMENT:
		m_testCase->doHelpReplaceWhenPinTopPage(m_tester);
		break;
	case MTT_AMP_GET_TBL:
	case MTT_AMP_GET_PG:
	case MTT_AMP_PIN_PG:
		m_testCase->doHelpAllocMmsPage(m_tester, m_task);
		break;
	case MTT_FLUSHLOG_RPCLASS:
		m_testCase->doHelpTestFlushLogRpClass(m_tester);
		break;
	case MTT_SORTFLUSH_RPCLASS:
		m_testCase->doHelpTestSortFlushRpClass(m_tester);
		break;
	default:
		break;
	}
}

#endif

