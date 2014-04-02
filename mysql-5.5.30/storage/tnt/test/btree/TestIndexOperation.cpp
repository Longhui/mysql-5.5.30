/**
 * ��������������
 *
 * @author �ձ�(bsu@corp.netease.com, naturally@163.org)
 */

#include <cppunit/config/SourcePrefix.h>
#include "btree/TestIndexOperation.h"
#include "Test.h"
#include "btree/Index.h"
#include "btree/IndexBPTree.h"
#include "btree/IndexBPTreesManager.h"
#include "api/Table.h"
#include "misc/Global.h"
#include "api/Database.h"
#include "heap/Heap.h"
#include "util/File.h"
#include "util/SmartPtr.h"
#include "misc/RecordHelper.h"
#include "misc/Session.h"
#include "misc/Trace.h"
#include <stdlib.h>
#include <iostream>
#include <vector>

using namespace std;
using namespace ntse;
#define INDEX_NAME	"testIndex.nti"			/** �����ļ��� */
#define HEAP_NAME	"testIndex.ntd"			/** ���ļ��� */
#define TBLDEF_NAME "testIndex.nttd"        /** TableDef�����ļ�*/
#define DATA_SCALE	2000					/** �Ѻ����������ݹ�ģ */
#define HOLE		1000					/** ���ڿ����������ݵ�һ����ֵ����ʼֵ����֤��ʹ��Ψһ��Լ��������������������ʼֵ֮ǰ�����������ɼ�ֵ���� */
#define SAME_ID		1200					/** ָ������ͬID�ļ�ֵ */
#define SAME_SCALE	20						/** ��ͬID��ֵ���ظ����� */
#define DUPLICATE_TIMES	3					/** ���ʹ�ã�����Զ��е�ÿһ����ͬ��¼������ָ�������ĸ��������Է�Ψһ������ */
#define END_SCALE	500						/** �������Ͷ�����Ĭ�Ϸ�Χ֮�⣬��ָ��һ�����ݷ�Χ */
#define FAKE_LONG_CHAR234 "abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyz" \
	"abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyz"
#define LONG_KEY_LENGTH	250					/** ����ֵ���Ե���󳤶� */

RowId rowids[DATA_SCALE];					/** ���ڴ洢�Ѿ�����ѵ������ݵ�rowid��Ϣ��������DATA_SCALE�����ݣ��������� */

#define MT_TEST_DATA_SCALE	1000000			/** ���̲߳��Ե����ݹ�ģ */
#define MT_TEST_THREAD_NUM	100				/** ���̲߳����߳��� */
#define MT_TEST_REPEAT_TIME	100000			/** ���߳��ظ�DML�������� */
#define MT_TEST_INIT_SCALE	60000			/** ���̲߳��Գ�ʼ������ģ */

#define HEAP_BACKUP_NAME	"testIndexHeapBackup.ntd"		/** ���ݵĶ��ļ��� */
#define INDEX_BACKUP_NAME	"testIndexIDXBackup.nti"		/** ���ݵ������ļ��� */
#define TBLDEF_BACKUP_NAME  "testIndexTblDefBackup.nttd"    /** ���ݵ�tabledef�ļ���*/


extern void backupHeapFile(File *file, const char *backupName);
extern void backupHeapFile(const char *origName, const char *backupName);
extern void clearRSFile(Config *cfg);
extern void backupTblDefFile(const char *origName, const char *backupName);

/**
 * ����һ���ļ���ָ��ԭ�ļ����ͱ��ݺ���ļ���
 */
void myrestoreHeapFile(const char *backupHeapFile, const char *origFile) {
	u64 errCode;
	File bk(backupHeapFile);
	File orig(origFile);
	errCode = orig.remove();
	if (File::getNtseError(errCode) != File::E_NO_ERROR) {
		cout << File::explainErrno(errCode) << endl;
		return;
	}
	while (true) {
		errCode = bk.move(origFile);
		if (File::getNtseError(errCode) != File::E_NO_ERROR) {
			//cout << File::explainErrno(errCode) << endl;
			continue;
		}
		return;
	}
}


/**
 * ����ָ����ֵ��������0�������
 */
static bool fetchByKeyFromIndex0(Session *session, DrsIndex *index, TableDef *tableDef, u64 userId, u64 bankaccount, RowId rowId) {
	IndexDef *indexDef = tableDef->m_indice[0];
	SubRecordBuilder sb(tableDef, KEY_PAD);
	SubRecord *findKey = sb.createSubRecordByName("UserId"" ""BankAccount", &userId, &bankaccount);
	findKey->m_rowId = rowId;

	SubToSubExtractor *extractor = SubToSubExtractor::createInst(session->getMemoryContext(), tableDef, indexDef,
		indexDef->m_numCols, indexDef->m_columns, indexDef->m_numCols, indexDef->m_columns, KEY_COMPRESS, KEY_PAD, 1);

	RowId rowId1;
	bool found = index->getByUniqueKey(session, findKey, None, &rowId1, NULL, NULL, extractor);
	bool succ = found && rowId == findKey->m_rowId;

	freeSubRecord(findKey);

	return succ;
}

/**
 * ��Χ����ɨ����֤��������������ָ��������ȣ�����������˵�����Ҷҳ���ǰ�����������ȷ
 */
bool rangeScanForwardCheck(Session *session, TableDef *tableDef, DrsIndex *index, SubRecord *findKey, SubRecord *getKey, uint totalKeys, bool forceCheck = false) {
	if (isEssentialOnly() && !forceCheck)
		return true;

	IndexDef *indexDef = (IndexDef*)((DrsBPTreeIndex*)index)->getIndexDef();
	SubToSubExtractor *extractor = (getKey == NULL) ? NULL : SubToSubExtractor::createInst(session->getMemoryContext(), tableDef, indexDef,
		indexDef->m_numCols, indexDef->m_columns, getKey->m_numCols, getKey->m_columns, KEY_COMPRESS, KEY_PAD, 1000);
	IndexScanHandle *indexHandle = index->beginScan(session, NULL, true, true, None, NULL, extractor);
	int count = 0;
	while (index->getNext(indexHandle, getKey)) {
		if (count == 0) {
			DrsIndexRangeScanHandle *handle = (DrsIndexRangeScanHandle*)indexHandle;
			PageHandle *pageHandle = handle->getScanInfo()->m_pageHandle;
			IndexPage *page = (IndexPage*)pageHandle->getPage();
			CPPUNIT_ASSERT(page->m_prevPage == INVALID_ROW_ID);
		}
		count++;
	}
	{
		DrsIndexRangeScanHandle *handle = (DrsIndexRangeScanHandle*)indexHandle;
		PageHandle *pageHandle = handle->getScanInfo()->m_pageHandle;
		IndexPage *page = (IndexPage*)pageHandle->getPage();
		CPPUNIT_ASSERT(page->m_nextPage == INVALID_ROW_ID);
	}
	index->endScan(indexHandle);
	CPPUNIT_ASSERT(count == totalKeys);
	return true;
}


/**
 * ��Χ����ɨ����֤���������������
 */
bool rangeScanBackwardCheck(Session *session, TableDef *tableDef, DrsIndex *index, SubRecord *findKey, SubRecord *getKey, uint totalKeys, bool forceCheck = false) {
	if (isEssentialOnly() && !forceCheck)
		return true;

	IndexDef *indexDef = (IndexDef*)((DrsBPTreeIndex*)index)->getIndexDef();
	SubToSubExtractor *extractor = (getKey == NULL) ? NULL : SubToSubExtractor::createInst(session->getMemoryContext(), tableDef, indexDef,
		indexDef->m_numCols, indexDef->m_columns, getKey->m_numCols, getKey->m_columns, KEY_COMPRESS, KEY_PAD, 1000);
	IndexScanHandle *indexHandle;
	indexHandle = index->beginScan(session, NULL, false, true, None, NULL, extractor);
	uint count = 0;
	while (index->getNext(indexHandle, getKey))
		count++;
	index->endScan(indexHandle);
	CPPUNIT_ASSERT(count == totalKeys);
	return true;
}


/**
 * ����һ��ָ����¼�����ڲ���ѻ������Ʋ���ʹ��
 * @post ʹ����Ҫ�ͷſռ�
 */
Record* createRecord(const TableDef *tableDef, u64 rowid, u64 userid, const char *username, u64 bankacc, u32 balance) {
	RecordBuilder rb(tableDef, rowid, REC_FIXLEN);
	rb.appendBigInt(userid);
	rb.appendChar(username);
	rb.appendBigInt(bankacc);
	rb.appendInt(balance);
	return rb.getRecord(tableDef->m_maxRecSize);
}


/**
 * ����ָ����Ϣ����һ���Ӽ�¼��ֻ�����û��������ڲ��Ը��²���
 * �ú�����ȫ�����ڱ������ļ����ж������Ķ���
 * @post ʹ����Ҫ�ͷſռ�
 */
SubRecord* createUpdateAfterRecordRedundant2(const TableDef *tableDef, u64 rowid, const char *username) {
	SubRecordBuilder srb(tableDef, REC_REDUNDANT, rowid);
	return srb.createSubRecordByName("UserName", username);
}


/**
 * ����ָ����Ϣ����һ���Ӽ�¼��ֻ�����û�id�����ڲ��Ը��²���
 * �ú�����ȫ�����ڱ������ļ����ж������Ķ���
 * @post ʹ����Ҫ�ͷſռ�
 */
SubRecord* createUpdateAfterRecordRedundant1(const TableDef *tableDef, u64 rowid, u64 userid) {
	SubRecordBuilder srb(tableDef, REC_REDUNDANT, rowid);
	return srb.createSubRecordByName("UserId", &userid);
}


/**
 * ����ָ����Ϣ����һ���Ӽ�¼��ֻ�����û�id��bankaccount�ֶΣ����ڲ��Ը��²���
 * �ú�����ȫ�����ڱ������ļ����ж������Ķ���
 * @post ʹ����Ҫ�ͷſռ�
 */
SubRecord* createUpdateAfterRecordRedundant3(const TableDef *tableDef, u64 rowid, u64 userid, u64 bankaccount) {
	SubRecordBuilder srb(tableDef, REC_REDUNDANT, rowid);
	return srb.createSubRecordByName("UserId"" ""BankAccount", &userid, &bankaccount);
}


/**
 * ����һ���������Ӽ�¼�������������ԣ����ڲ��Ը���
 * �ú�����ȫ�����ڱ������ļ����ж������Ķ���
 * @post ʹ����Ҫ�ͷſռ�
 */
SubRecord* createUpdateAfterRecordRedundantWhole(const TableDef *tableDef, u64 rowid, u64 userid, const char *username, u64 bankacc, u32 balance) {
	SubRecordBuilder srb(tableDef, REC_REDUNDANT, rowid);
	return srb.createSubRecordByName("UserId"" ""UserName"" ""BankAccount"" ""Balance", &userid, username, &bankacc, &balance);
}


/**
 * ����һ�������ʽ��ָ���ļ�¼
 * @post ʹ����Ҫ�ͷſռ�
 */
Record* createRecordRedundant(const TableDef *tableDef, u64 rowid, u64 userid, const char *username, u64 bankacc, u32 balance) {
	RecordBuilder rb(tableDef, rowid, REC_REDUNDANT);
	rb.appendBigInt(userid);
	rb.appendChar(username);
	rb.appendBigInt(bankacc);
	rb.appendInt(balance);
	return rb.getRecord(tableDef->m_maxRecSize);
}


/**
 * ����һ�������Ĳ��Ҽ�ֵ�Ӽ�¼
 * �ú�����ȫ�����ڱ��ļ��������Ķ��壬���Ҹò��Ҽ�ֵһ���Ƕ԰����û����ֶε������Ĳ���
 * ��Ϊ:1�����������������ֱ�۱��ڵ��ԣ�2���������Բ��Գ���ֵ��3���������Բ���Ψһ��Լ���ȵ�....
 */
SubRecord* makeFindKeyWhole(int count, DrsHeap *heap, const TableDef *tblDef, bool testBig = false, bool testPrefix = false) {
	char name[LONG_KEY_LENGTH];
	if (testBig) {
		if (testPrefix)
			sprintf(name, FAKE_LONG_CHAR234 "%d", count);
		else
			sprintf(name, "%d" FAKE_LONG_CHAR234, count);
	} else
		sprintf(name, "Kenneth Tse Jr. %d \0", count);
	u64 userid = count;
	u64 bankaccount = count + ((u64)count << 32);

	SubRecordBuilder sb(tblDef, KEY_PAD);
	return sb.createSubRecordByName("UserId"" ""UserName"" ""BankAccount", &userid, name, &bankaccount);
}


/**
 * ��������ģ���ȶ��Եļ�����
 * ���ж���̣߳�����ִ��DML����
 */

const char* IndexOPStabilityTestCase::getName() {
	return "Index stability test";
}

const char* IndexOPStabilityTestCase::getDescription() {
	return "Test Index stability(Multi-threads test).";
}

bool IndexOPStabilityTestCase::isBig() {
	return true;
}

void IndexOPStabilityTestCase::setUp() {
	m_heap = NULL;
	m_index = NULL;
	m_tblDef = NULL;
	Database::drop(".");
	EXCPT_OPER(m_db = Database::open(&m_cfg, true));
	m_conn = m_db->getConnection(false);
	m_session = m_db->getSessionManager()->allocSession("IndexOperationTestCase::setUp", m_conn);
	char name[255] = "testIndex.nti";
	memcpy(m_path, name, sizeof(name));
	srand((unsigned)time(NULL));
	clearRSFile(&m_cfg);
	ts.idx = true;
	vs.idx = true;
}

void IndexOPStabilityTestCase::tearDown() {
	// ������
	if (m_index != NULL) {
		m_index->close(m_session, true);
		delete m_index;
		DrsIndice::drop(m_path);
	}
	// ����
	if (m_heap != NULL) {
		closeSmallHeap(m_heap);
		DrsHeap::drop(HEAP_NAME);
	}

	if (m_tblDef != NULL) {
		TableDef::drop(TBLDEF_NAME);
		m_tblDef = NULL;
	}

	if (m_db != NULL) {
		m_db->getSessionManager()->freeSession(m_session);
		m_db->freeConnection(m_conn);
		m_db->close();
		delete m_db;
		Database::drop(".");
		m_db = NULL;
	}

	ts.idx = false;
	vs.idx = false;
}


/**
* ����ָ���ĶѺ��������壬�����������
*/
void IndexOPStabilityTestCase::createIndex(char *path, DrsHeap *heap, TableDef *tblDef) throw(NtseException) {
	File oldindexfile(path);
	oldindexfile.remove();

	DrsIndice::create(path, tblDef);
	CPPUNIT_ASSERT(m_index == NULL);
	m_index = DrsIndice::open(m_db, m_session, path, tblDef, NULL);
	try {
		for (uint i = 0; i < tblDef->m_numIndice; i++) {
			m_index->createIndexPhaseOne(m_session, tblDef->m_indice[i], tblDef, heap);
			m_index->createIndexPhaseTwo(tblDef->m_indice[i]);
		}
	} catch (NtseException &e) {
		cout << e.getMessage() << endl;
		CPPUNIT_ASSERT(false);
	}
}


/**
* �ر�ʹ���е�С��
*/
void IndexOPStabilityTestCase::closeSmallHeap(DrsHeap *heap) {
	EXCPT_OPER(m_heap->close(m_session, true));
	delete m_heap;
}




/**
* ������Ψһ�����������Զ��̲߳�����DML����
* ������ɲ�����ʽ��֮��ִ�в���
*/
void IndexOPStabilityTestCase::testMultiThreadsDML() {
	uint step = 4;
	// ����ǰ׺ѹ������128�ֽ�
	uint totalKeys;
	m_tblDef = createSmallTableDefWithSame();
	m_heap = createSmallHeapWithSame(m_tblDef);
	totalKeys = buildHeapWithSame(m_heap, m_tblDef, MT_TEST_INIT_SCALE, step);
	EXCPT_OPER(createIndex(m_path, m_heap, m_tblDef));

	u64 start = System::currentTimeMillis();
	cout << "Start time: " << start << endl;

	HalfHeartedMan *pool[MT_TEST_THREAD_NUM];
	Connection *connPool[MT_TEST_THREAD_NUM];
	for (uint i = 0; i < MT_TEST_THREAD_NUM; i++) {
		connPool[i] = m_db->getConnection(false);
		pool[i] = new HalfHeartedMan("I'm force to be of two minds, help...", m_db, connPool[i], m_heap, m_tblDef, m_index, 0, INVALID_ROW_ID, MT_TEST_REPEAT_TIME);
		pool[i]->start();
	}

	for (uint i = 0; i < MT_TEST_THREAD_NUM; i++) {
		pool[i]->join();
		delete pool[i];
		m_db->freeConnection(connPool[i]);
	}

	{	// ��Ҫ���������Ͷѵ�����һ��
		Connection *conn = m_db->getConnection(false);
		Session *session = m_db->getSessionManager()->allocSession("TestMultithreadDML", conn);
		MemoryContext *memoryContext = session->getMemoryContext();
		DrsIndex *index = m_index->getIndex(1);
		SubRecord *idxKey = IndexKey::allocSubRecordRED(memoryContext, m_tblDef->m_maxRecSize);
		idxKey->m_columns = m_tblDef->m_indice[1]->m_columns;
		idxKey->m_numCols = m_tblDef->m_indice[1]->m_numCols;

		IndexDef *indexDef = m_tblDef->m_indice[1];
		// TODO idxKey�����ԺŲ��ǵ�����
		DrsHeapScanHandle *scanHandle = m_heap->beginScan(session, SubrecExtractor::createInst(session, m_tblDef, idxKey), None, NULL, false);
		int count = 0;
		while (m_heap->getNext(scanHandle, idxKey)) {	// ��ÿһ����¼��������Ѱ�ң��ض����ҵ�
			idxKey->m_format = KEY_PAD;
			RowId rowId;
			bool found = index->getByUniqueKey(session, idxKey, None, &rowId, NULL, NULL, NULL);
			if (!found) {
				Record *record = IndexKey::allocRecord(memoryContext, m_tblDef->m_maxRecSize);
				record->m_format = REC_FIXLEN;
				m_heap->getRecord(session, idxKey->m_rowId, record, None, NULL);
				index->getByUniqueKey(session, idxKey, None, &rowId, NULL, NULL, NULL);
			}
			CPPUNIT_ASSERT(found);
			CPPUNIT_ASSERT(rowId == idxKey->m_rowId);
			idxKey->m_format = REC_REDUNDANT;
		}
		m_heap->endScan(scanHandle);
	}

	m_index->close(m_session, true);
	delete m_index;
	DrsIndice::drop(m_path);
	m_index = NULL;
	closeSmallHeap(m_heap);
	DrsHeap::drop(HEAP_NAME);
	m_heap = NULL;
	
	delete m_tblDef;
	m_tblDef = NULL;
	TableDef::drop(TBLDEF_NAME);

	u64 end = System::currentTimeMillis();
	cout << "End time: " << end << endl;
	cout << "Duration: " << end - start << endl;
}


TableDef* IndexOPStabilityTestCase::createSmallTableDefWithSame() {
	File oldsmallfile(TBLDEF_NAME);
	oldsmallfile.remove();

	TableDefBuilder *builder;
	TableDef *tableDef;

	// ����С��
	builder = new TableDefBuilder(99, "inventory", "User");
	builder->addColumn("UserId", CT_BIGINT, false)->addColumnS("UserName", CT_CHAR, 50);
	builder->addColumn("BankAccount", CT_BIGINT)->addColumn("Balance", CT_INT);
	builder->addIndex("RANGE", false, false, false, "UserId", 0, "BankAccount", 0, NULL);
	builder->addIndex("RANGE", false, false, false, "UserId", 0, "UserName", 0, "BankAccount", 0, NULL);
	tableDef = builder->getTableDef();
	EXCPT_OPER(tableDef->writeFile(TBLDEF_NAME));

	delete builder;

	return tableDef;
}
/**
* ����һ��С�Ѷ����Լ������������
* �����������Ƿ�Ψһ������
*/
DrsHeap* IndexOPStabilityTestCase::createSmallHeapWithSame(TableDef *tableDef) {
	DrsHeap *smallHeap;
	char tableName[255] = HEAP_NAME;
	File oldsmallfile(tableName);
	oldsmallfile.remove();

	EXCPT_OPER(DrsHeap::create(m_db, tableName, tableDef));
	EXCPT_OPER(smallHeap = DrsHeap::open(m_db, m_session, tableName, tableDef));

	return smallHeap;
}


/**
* ��ָ�����в������ݣ����Ҫ��֤�������ɸ��ظ���ֵ
* ��ҪΪ�˲�������Ψһ�Գ�ͻ��صĲ�������
*/
uint IndexOPStabilityTestCase::buildHeapWithSame(DrsHeap *heap, const TableDef *tblDef, uint size, uint step) {
	assert(step > 0);
	Record *record;
	for (int i = HOLE; i < size + HOLE; i += step) {
		char name[50];
		RowId rid;
		sprintf(name, "Kenneth Tse Jr. %d \0", i);
		record = createRecord(tblDef, 0, i, name, i + ((u64)i << 32), (u32)(-1) - i);
		for (int j = 0; j < DUPLICATE_TIMES; j++) {
			EXCPT_OPER(rid = heap->insert(m_session, record, NULL));
		}
		freeRecord(record);
	}

	int i = SAME_ID;
	for (int j = 0; j < SAME_SCALE; j++) {
		char name[50];
		RowId rid;
		sprintf(name, "Kenneth Tse Jr. %d \0", i);
		record = createRecord(tblDef, 0, i, name, i + ((u64)i << 32), (u32)(-1) - i);
		EXCPT_OPER(rid = heap->insert(m_session, record, NULL));
		freeRecord(record);
	}

	return size * DUPLICATE_TIMES / step + SAME_SCALE;

}


/************************************************************************/
/* ������������������
/* ����������ʹ��һ��С��ִ�У����������������壬��һ�������Ƿ�Ψһ�Ե�
/* �ڶ���������Ψһ����������������Ҫ����Ψһ�Գ�ͻʹ��
/* �е��������ܻ��õ���ѣ�Ҫ���������Գ���ֵ�Ĵ���
/* �еĲ����������ܻ��õ������������Ƿ�Ψһ��������С�ѣ�������Կ���������
/* �����������������õ����������ǲ���ǰ�������������ڲ�����һ������ִ��
/* ����Ĳ������ݲ��������Լ�������ѡ��
*/
/************************************************************************/

const char* IndexOperationTestCase::getName() {
	return "Index operation test";
}

const char* IndexOperationTestCase::getDescription() {
	return "Test Index operations(Unique scan/RangeScan/Insert/Delete/Update and so on).";
}

bool IndexOperationTestCase::isBig() {
	return false;
}

void IndexOperationTestCase::setUp() {
	m_heap = NULL;
	m_index = NULL;
	m_tblDef = NULL;
	Database::drop(".");
	EXCPT_OPER(m_db = Database::open(&m_cfg, true));
	m_conn = m_db->getConnection(false);
	m_session = m_db->getSessionManager()->allocSession("IndexOperationTestCase::setUp", m_conn);
	char name[255] = "testIndex.nti";
	memcpy(m_path, name, sizeof(name));
	srand((unsigned)time(NULL));
	ts.idx = true;
	vs.idx = true;
}

void IndexOperationTestCase::tearDown() {
	// ������
	if (m_index != NULL) {
		m_index->close(m_session, true);
		delete m_index;
		DrsIndice::drop(m_path);
	}
	// ����
	if (m_heap != NULL) {
		closeSmallHeap(m_heap);
		DrsHeap::drop(HEAP_NAME);
	}

	if (m_tblDef != NULL) {
		TableDef::drop(TBLDEF_NAME);
		m_tblDef = NULL;
	}

	if (m_db != NULL) {
		m_db->getSessionManager()->freeSession(m_session);
		m_db->freeConnection(m_conn);
		m_db->close();
		delete m_db;
		Database::drop(".");
		m_db = NULL;
	}

	vs.idx = false;
	ts.idx = false;
}


/**
 * ����ָ���ĶѺ��������壬�����������
 */
void IndexOperationTestCase::createIndex(char *path, DrsHeap *heap, const TableDef *tblDef) throw(NtseException) {
	File oldindexfile(path);
	oldindexfile.remove();

	DrsIndice::create(path, tblDef);
	CPPUNIT_ASSERT(m_index == NULL);
	m_index = DrsIndice::open(m_db, m_session, path, tblDef, NULL);
	try {
		for (uint i = 0; i < tblDef->m_numIndice; i++) {
			m_index->createIndexPhaseOne(m_session, tblDef->m_indice[i], tblDef, heap);
			m_index->createIndexPhaseTwo(tblDef->m_indice[i]);
		}
	} catch (NtseException &e) {
		cout << e.getMessage() << endl;
		CPPUNIT_ASSERT(false);
	}
}

TableDef* IndexOperationTestCase::createSmallTableDef() {
	File oldsmallfile(TBLDEF_NAME);
	oldsmallfile.remove();

	TableDefBuilder *builder;
	TableDef *tableDef;

	// ����С��
	builder = new TableDefBuilder(99, "inventory", "User");
	builder->addColumn("UserId", CT_BIGINT, false)->addColumnS("UserName", CT_CHAR, 50, false, false);
	builder->addColumn("BankAccount", CT_BIGINT, false)->addColumn("Balance", CT_INT, false);
	builder->addIndex("NOCONOSTRAIN", false, false, false, "UserId", 0, "BankAccount", 0, NULL);
	builder->addIndex("PRIMARY", true, true, false, "UserId", 0, "UserName", 0, "BankAccount", 0, NULL);
	tableDef = builder->getTableDef();
	EXCPT_OPER(tableDef->writeFile(TBLDEF_NAME));

	delete builder;

	return tableDef;
}

/**
 * ����һ��С�Ѷ����Լ���ص���������
 * ��������������һ���Ƿ�Ψһ���������ڶ�����Ψһ������
 */
DrsHeap* IndexOperationTestCase::createSmallHeap(const TableDef *tableDef) {
	DrsHeap *smallHeap;
	char tableName[255] = HEAP_NAME;
	File oldsmallfile(tableName);
	oldsmallfile.remove();

	EXCPT_OPER(DrsHeap::create(m_db, tableName, tableDef));
	EXCPT_OPER(smallHeap = DrsHeap::open(m_db, m_session, tableName, tableDef));

	return smallHeap;
}

TableDef *IndexOperationTestCase::createSmallTableDefWithSame() {
	File oldsmallfile(TBLDEF_NAME);
	oldsmallfile.remove();

	TableDefBuilder *builder;
	TableDef *tableDef;

	// ����С��
	builder = new TableDefBuilder(99, "inventory", "User");
	builder->addColumn("UserId", CT_BIGINT, false)->addColumnS("UserName", CT_CHAR, 50);
	builder->addColumn("BankAccount", CT_BIGINT)->addColumn("Balance", CT_INT);
	builder->addIndex("RANGE1", false, false, false, "UserId", 0, "BankAccount", 0, NULL);
	builder->addIndex("RANGE2", false, false, false, "UserId", 0, "UserName", 0, "BankAccount", 0, NULL);
	tableDef = builder->getTableDef();
	EXCPT_OPER(tableDef->writeFile(TBLDEF_NAME));

	delete builder;

	return tableDef;
}

/**
 * ����һ��С�Ѷ����Լ������������
 * �����������Ƿ�Ψһ������
 */
DrsHeap* IndexOperationTestCase::createSmallHeapWithSame(const TableDef *tableDef) {
	DrsHeap *smallHeap;
	char tableName[255] = HEAP_NAME;
	File oldsmallfile(tableName);
	oldsmallfile.remove();

	EXCPT_OPER(DrsHeap::create(m_db, tableName, tableDef));
	EXCPT_OPER(smallHeap = DrsHeap::open(m_db, m_session, tableName, tableDef));

	return smallHeap;
}



/**
 * �ر�ʹ���е�С��
 */
void IndexOperationTestCase::closeSmallHeap(DrsHeap *heap) {
	EXCPT_OPER(m_heap->close(m_session, true));
	delete m_heap;
}


/**
 * ����������ݣ��ñ�Ϊ�˲���������ֵ����128������
 * ��˱�������������ַ������ȱ���ﵽ��������128
 */
uint IndexOperationTestCase::buildBigHeap(DrsHeap *heap, const TableDef *tblDef, bool testPrefix, uint size, uint step) {
	Record *record;
	uint count = 0;
	for (uint i = HOLE; i < HOLE + size; i += step) {
		char name[LONG_KEY_LENGTH];
		RowId rid;
		if (testPrefix)
			sprintf(name, FAKE_LONG_CHAR234 "%d", i);
		else
			sprintf(name, "%d" FAKE_LONG_CHAR234, i);
		record = createRecord(tblDef, 0, i, name, i + ((u64)i << 32), (u32)(-1) - i);
		EXCPT_OPER(rid = heap->insert(m_session, record, NULL));
		rowids[i - HOLE] = rid;
		count++;
		freeRecord(record);
	}

	return count;
}

TableDef* IndexOperationTestCase::createBigTableDef() {
	File oldsmallfile(TBLDEF_NAME);
	oldsmallfile.remove();

	TableDefBuilder *builder;
	TableDef *tableDef;

	// ����С��
	builder = new TableDefBuilder(99, "inventory", "User");
	builder->addColumn("UserId", CT_BIGINT, false)->addColumnS("UserName", CT_CHAR, LONG_KEY_LENGTH);
	builder->addColumn("BankAccount", CT_BIGINT)->addColumn("Balance", CT_INT);
	builder->addIndex("RANGE", false, false, false, "UserId", 0, "UserName", 0, "BankAccount", 0, NULL);
	tableDef = builder->getTableDef();
	EXCPT_OPER(tableDef->writeFile(TBLDEF_NAME));
	delete builder;

	return tableDef;
}

/**
 * ������ѣ���¼���ȳ���100��
 * ����ֻ����һ�������������Ƿ�Ψһ�Ե�����
 */
DrsHeap* IndexOperationTestCase::createBigHeap(const TableDef *tableDef) {
	DrsHeap *smallHeap;
	char tableName[255] = HEAP_NAME;
	File oldsmallfile(tableName);
	oldsmallfile.remove();

	EXCPT_OPER(DrsHeap::create(m_db, tableName, tableDef));
	EXCPT_OPER(smallHeap = DrsHeap::open(m_db, m_session, tableName, tableDef));

	return smallHeap;
}


/**
 * ����ָ�������ݷ�Χ����ָ���Ķ��в�������
 */
uint IndexOperationTestCase::buildHeap(DrsHeap *heap, const TableDef *tblDef, uint size, uint step) {
	assert(step > 0);
	uint count = 0;
	Record *record;
	for (int i = HOLE; i < size + HOLE; i += step) {
		char name[50];
		RowId rid;
		sprintf(name, "Kenneth Tse Jr. %d \0", i);
		record = createRecord(tblDef, 0, i, name, i + ((u64)i << 32), (u32)(-1) - i);
		EXCPT_OPER(rid = heap->insert(m_session, record, NULL));
		if (i - HOLE < DATA_SCALE)
			rowids[i - HOLE] = rid;
		count++;
		freeRecord(record);
	}

	return count;
}


/**
 * ��ָ�����в������ݣ����Ҫ��֤�������ɸ��ظ���ֵ
 * ��ҪΪ�˲�������Ψһ�Գ�ͻ��صĲ�������
 */
uint IndexOperationTestCase::buildHeapWithSame(DrsHeap *heap, const TableDef *tblDef, uint size, uint step) {
	assert(step > 0);
	Record *record;
	for (int i = HOLE; i < size + HOLE; i += step) {
		char name[50];
		RowId rid;
		sprintf(name, "Kenneth Tse Jr. %d \0", i);
		record = createRecord(tblDef, 0, i, name, i + ((u64)i << 32), (u32)(-1) - i);
		for (int j = 0; j < DUPLICATE_TIMES; j++) {
			EXCPT_OPER(rid = heap->insert(m_session, record, NULL));
		}
		freeRecord(record);
	}

	int i = SAME_ID;
	for (int j = 0; j < SAME_SCALE; j++) {
		char name[50];
		RowId rid;
		sprintf(name, "Kenneth Tse Jr. %d \0", i);
		record = createRecord(tblDef, 0, i, name, i + ((u64)i << 32), (u32)(-1) - i);
		EXCPT_OPER(rid = heap->insert(m_session, record, NULL));
		freeRecord(record);
	}

	return size * DUPLICATE_TIMES / step + SAME_SCALE;
}



/**
 *  ���Գ���ֵ������256�ļ�ֵ�����������ֵ�洢����λ��Ҫ��λ�����
 */
void IndexOperationTestCase::testLongKey() {
	uint step = 2;
	uint testScale = DATA_SCALE;
#if NTSE_PAGE_SIZE < 8192
	testScale = DATA_SCALE / 10;
#endif
	// ����ǰ׺ѹ������128�ֽ�
	uint totalKeys;
	m_tblDef = createBigTableDef();
	m_heap = createBigHeap(m_tblDef);
	totalKeys = buildBigHeap(m_heap, m_tblDef, true, testScale, step);
	createIndex(m_path, m_heap, m_tblDef);

	Connection *conn = m_db->getConnection(false);
	Session *session = m_db->getSessionManager()->allocSession("IndexOperationTestCase::testUniqueScan", conn);
	MemoryContext *memoryContext = session->getMemoryContext();

	checkIndex(session, memoryContext, totalKeys);

	DrsIndex *index = m_index->getIndex(0);

	rangeScanForwardCheck(session, m_tblDef, index, NULL, NULL, totalKeys);
	rangeScanBackwardCheck(session, m_tblDef, index, NULL, NULL, totalKeys);


	// ���Գ���¼�����Ĳ���
	{
		for (uint i = HOLE + 1; i < HOLE + testScale * 3; i += step) {
			char name[LONG_KEY_LENGTH];
			RowId rid;
			sprintf(name, FAKE_LONG_CHAR234 "%d", i);
			Record *insertrecord = createRecord(m_tblDef, 0, i, name, i + ((u64)i << 32), (u32)(-1) - i);
			EXCPT_OPER(rid = m_heap->insert(m_session, insertrecord, NULL));

			freeRecord(insertrecord);

			insertrecord = createRecordRedundant(m_tblDef, 0, i, name, i + ((u64)i << 32), (u32)(-1) - i);
			insertrecord->m_rowId = rid;

			bool successful;
			uint dupIndex;
			session->startTransaction(TXN_INSERT, m_tblDef->m_id);
			EXCPT_OPER(successful = m_index->insertIndexEntries(session, insertrecord, &dupIndex));
			session->endTransaction(true);
			CPPUNIT_ASSERT(successful);
			totalKeys++;

			freeRecord(insertrecord);
		}

		// ��֤��������
		checkIndex(session, memoryContext, totalKeys);

		DrsIndex *index = m_index->getIndex(0);

		rangeScanForwardCheck(session, m_tblDef, index, NULL, NULL, totalKeys);
		rangeScanBackwardCheck(session, m_tblDef, index, NULL, NULL, totalKeys);
	}


	// ���Գ���¼������ɾ��
	{
//		SubRecord *record = IndexKey::allocSubRecordRED(memoryContext, m_tblDef->m_maxRecSize);
		SubRecord *record = IndexKey::allocSubRecord(memoryContext, m_tblDef->getIndexDef(0), KEY_PAD);
		record->m_columns = m_tblDef->m_indice[0]->m_columns;
		record->m_numCols = m_tblDef->m_indice[0]->m_numCols;
		RowId rowId;
		SubRecord *findKey;
		Record *heapRec = IndexKey::allocRecord(memoryContext, m_tblDef->m_maxRecSize);
		index = m_index->getIndex(0);
		IndexDef *indexDef = m_tblDef->m_indice[0];

		SubToSubExtractor *extractor = SubToSubExtractor::createInst(session->getMemoryContext(), m_tblDef, indexDef,
			indexDef->m_numCols, indexDef->m_columns, record->m_numCols, record->m_columns, KEY_COMPRESS, KEY_PAD, 1);

		uint foundKeys = 0;
		for (int i = HOLE; i < HOLE + testScale * 3; i++) {
			{
				char name[LONG_KEY_LENGTH];
				sprintf(name, FAKE_LONG_CHAR234 "%d", i);
				u64 userid = i;

				SubRecordBuilder sb(m_tblDef, KEY_PAD);
				findKey = sb.createSubRecordByName("UserId"" ""UserName", &userid, name);
			}

			findKey->m_rowId = INVALID_ROW_ID;
			bool found = index->getByUniqueKey(session, findKey, None, &rowId, record, NULL, extractor);
			if (found) {	// ��ȡ���¼��ִ��ɾ������
				CPPUNIT_ASSERT(record->m_rowId != INVALID_ROW_ID);

				foundKeys++;

				heapRec->m_format = REC_FIXLEN;
				m_heap->getRecord(session, record->m_rowId, heapRec);
				heapRec->m_format = REC_REDUNDANT;

				session->startTransaction(TXN_DELETE, m_tblDef->m_id);
				EXCPT_OPER(m_index->deleteIndexEntries(session, heapRec, NULL));
				session->endTransaction(true);
				totalKeys--;
			}

			freeSubRecord(findKey);
		}
		CPPUNIT_ASSERT(foundKeys != 0);

		// ��֤��������
		checkIndex(session, memoryContext, totalKeys);

		DrsIndex *index = m_index->getIndex(0);

		rangeScanForwardCheck(session, m_tblDef, index, NULL, NULL, totalKeys);
		rangeScanBackwardCheck(session, m_tblDef, index, NULL, NULL, totalKeys);
	}


	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(conn);

	m_index->close(m_session, true);
	delete m_index;
	DrsIndice::drop(m_path);
	m_index = NULL;
	closeSmallHeap(m_heap);
	DrsHeap::drop(HEAP_NAME);
	m_heap = NULL;
	delete m_tblDef;
	m_tblDef = NULL;
	TableDef::drop(TBLDEF_NAME);

	// ���Ժ�׺ѹ������128�ֽ�
	m_tblDef = createBigTableDef();
	m_heap = createBigHeap(m_tblDef);
	totalKeys = buildBigHeap(m_heap, m_tblDef, false, 100, step);
	createIndex(m_path, m_heap, m_tblDef);

	conn = m_db->getConnection(false);
	session = m_db->getSessionManager()->allocSession("IndexOperationTestCase::testUniqueScan", conn);
	memoryContext = session->getMemoryContext();

	checkIndex(session, memoryContext, totalKeys);

	index = m_index->getIndex(0);
	rangeScanForwardCheck(session, m_tblDef, index, NULL, NULL, totalKeys);
	rangeScanBackwardCheck(session, m_tblDef, index, NULL, NULL, totalKeys);

	m_index->close(m_session, true);
	delete m_index;
	DrsIndice::drop(m_path);
	m_index = NULL;
	closeSmallHeap(m_heap);
	DrsHeap::drop(HEAP_NAME);
	m_heap = NULL;
	delete m_tblDef;
	m_tblDef = NULL;
	TableDef::drop(TBLDEF_NAME);

	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(conn);
}


/**
 * ���Ը���ɨ��������ں��Ĳ��Ե�ʱ������
 * ���������Ŀ���μ�testUniqueScan��testRangeScan��˵��
 */
void IndexOperationTestCase::testScan() {
	if (!isEssentialOnly())
		return;

	uint step = 1;

	uint testScale = DATA_SCALE / 4;
	// ������������
	m_tblDef = createSmallTableDefWithSame();
	m_heap = createSmallHeapWithSame(m_tblDef);
	uint totalKeys = buildHeapWithSame(m_heap, m_tblDef, testScale, step);
	try {
		createIndex(m_path, m_heap, m_tblDef);
	} catch (NtseException) {
		CPPUNIT_ASSERT(false);
	}

	// Ψһ��ɨ����Բ���
	{
		Connection *conn = m_db->getConnection(false);
		Session *session = m_db->getSessionManager()->allocSession("IndexOperationTestCase::testUniqueScan", conn);
		MemoryContext *memoryContext = session->getMemoryContext();
		DrsIndex *index = m_index->getIndex(1);
		IndexDef *indexDef = m_tblDef->m_indice[1];

		// ������0���Ҳ��Ը������ÿɿ��ٱȽϼ�ֵΨһ���ҵĹ���
		{
			DrsIndex *index = m_index->getIndex(0);
			CPPUNIT_ASSERT(fetchByKeyFromIndex0(session, index, m_tblDef, HOLE, HOLE + ((u64)HOLE << 32), 8192));
		}

		// ������1���в���
		SubRecord *record = IndexKey::allocSubRecordRED(memoryContext, m_tblDef->m_maxRecSize);
		record->m_columns = m_tblDef->m_indice[1]->m_columns;
		record->m_numCols = m_tblDef->m_indice[1]->m_numCols;
		RowLockHandle *rlh;
		SubToSubExtractor *extractor = SubToSubExtractor::createInst(session->getMemoryContext(), m_tblDef, indexDef,
			indexDef->m_numCols, indexDef->m_columns, record->m_numCols, record->m_columns, KEY_COMPRESS, KEY_PAD, 1);

		// ��֤���в���ļ�ֵ���ܱ��ҵ���ͬʱ���Լ���
		rlh = NULL;
		for (int i = HOLE; i < testScale + HOLE; i += step) {
			CPPUNIT_ASSERT(findAKey(session, index, &rlh, i, extractor));
			if (rlh != NULL)
				session->unlockRow(&rlh);
		}

		// ��֤û�в���ļ�ֵ�϶����ᱻ�ҵ�
		for (int i = HOLE - 2; i < HOLE; i++)
			CPPUNIT_ASSERT(!findAKey(session, index, NULL, i, extractor));
		for (int i = testScale + HOLE; i < testScale + HOLE + 2; i++)
			CPPUNIT_ASSERT(!findAKey(session, index, NULL, i, extractor));

		if (step > 1) {
			for (int i = HOLE + 1; i < testScale + HOLE + HOLE; i += step)
				CPPUNIT_ASSERT(!findAKey(session, index, NULL, i, extractor));
		}

		m_db->getSessionManager()->freeSession(session);
		m_db->freeConnection(conn);
	}

	// ��Χɨ����Բ���
	{
		Connection *conn = m_db->getConnection(false);
		Session *session = m_db->getSessionManager()->allocSession("IndexOperationTestCase::testRangeScan", conn);
		MemoryContext *memoryContext = session->getMemoryContext();
		DrsIndex *index = m_index->getIndex(1);
		IndexDef *indexDef = m_tblDef->m_indice[1];

		// ������1���в���
		SubRecord *record = IndexKey::allocSubRecordRED(memoryContext, m_tblDef->m_maxRecSize);
		record->m_columns = m_tblDef->m_indice[1]->m_columns;
		record->m_numCols = m_tblDef->m_indice[1]->m_numCols;
		SubRecord *findKey;

		uint count;
		IndexScanHandle *scanHandle;
		RowLockHandle *rlh;

		SubToSubExtractor *extractor = SubToSubExtractor::createInst(session->getMemoryContext(), m_tblDef, indexDef,
			indexDef->m_numCols, indexDef->m_columns, record->m_numCols, record->m_columns, KEY_COMPRESS, KEY_PAD, 1000);

		/**********************************************************************************************************/
		// ����
		scanHandle = index->beginScan(session, NULL, true, false, Shared, &rlh, extractor);
		count = 0;
		while (index->getNext(scanHandle, record)) {
			count++;
			session->unlockRow(&rlh);
		}
		CPPUNIT_ASSERT(count == totalKeys);
		index->endScan(scanHandle);
		//2.����ȫɨ��
		scanHandle = index->beginScan(session, NULL, false, true, None, NULL, extractor);
		count = 0;
		while (index->getNext(scanHandle, record)) {
			count++;
		}
		CPPUNIT_ASSERT(count == totalKeys);
		index->endScan(scanHandle);

		/**********************************************************************************************************/


		/**********************************************************************************************************/
		// ����������RangeScan���ԣ�����������ɸ���ֵ(���ض�����������ֵ��������Χ��)����ÿ����ֵ��>=/>/</<=�������
		uint keyShouldFound;
		uint start, end;
		start = 0;
		end = 20;
		for (uint i = start/*DATA_SCALE + HOLE / 2*/; i < end/*DATA_SCALE + HOLE + 2*/; i++) {
			uint num = rand() % testScale + HOLE;
			//uint num = 1009;
			if (i == end - 1)	// �������жϱ�֤�����ܲ��Ե�������Χ��ļ�ֵ�Լ�����Χ��ͬ��ֵ����
				num = HOLE - 1;
			else if (i == end - 2)
				num = testScale + HOLE;

			findKey = makeFindKeyWhole(num, m_heap, m_tblDef);
			findKey->m_rowId = INVALID_ROW_ID;

			// >=����
			scanHandle = index->beginScan(session, findKey, true, true, None, NULL, extractor);
			count = 0;
			while (index->getNext(scanHandle, record))
				count++;
			if (num < HOLE)
				CPPUNIT_ASSERT(count == totalKeys);
			else if (num >= testScale + HOLE)
				CPPUNIT_ASSERT(count == 0);
			else if (num != SAME_ID) {
				if (num < SAME_ID)
					keyShouldFound = (testScale + HOLE - num) * DUPLICATE_TIMES + SAME_SCALE;
				else
					keyShouldFound = (testScale + HOLE - num) * DUPLICATE_TIMES;
				CPPUNIT_ASSERT(count == keyShouldFound);
			} else {
				keyShouldFound = SAME_SCALE + DUPLICATE_TIMES * (testScale + HOLE - SAME_ID);
				CPPUNIT_ASSERT(count == keyShouldFound);
			}
			index->endScan(scanHandle);

			// >����
			scanHandle = index->beginScan(session, findKey, true, false, None, NULL, extractor);
			count = 0;
			while (index->getNext(scanHandle, record))
				count++;
			if (num < HOLE)
				CPPUNIT_ASSERT(count == totalKeys);
			else if (num >= testScale + HOLE)
				CPPUNIT_ASSERT(count == 0);
			else if (num != SAME_ID) {
				if (num < SAME_ID)
					keyShouldFound = (testScale + HOLE - num - 1) * DUPLICATE_TIMES + SAME_SCALE;
				else
					keyShouldFound = (testScale + HOLE - num - 1) * DUPLICATE_TIMES;
				CPPUNIT_ASSERT(count == keyShouldFound);
			} else {
				keyShouldFound = DUPLICATE_TIMES * (testScale + HOLE - SAME_ID - 1);
				CPPUNIT_ASSERT(count == keyShouldFound);
			}
			index->endScan(scanHandle);

			// <=����
			scanHandle = index->beginScan(session, findKey, false, true, None, NULL, extractor);
			count = 0;
			while (index->getNext(scanHandle, record))
				count++;
			if (num < HOLE)
				CPPUNIT_ASSERT(count == 0);
			else if (num >= testScale + HOLE)
				CPPUNIT_ASSERT(count == totalKeys);
			else if (num != SAME_ID) {
				uint keyShouldFound;
				if (num > SAME_ID)
					keyShouldFound = totalKeys - (testScale + HOLE - num - 1) * DUPLICATE_TIMES;
				else
					keyShouldFound = (num - HOLE + 1) * DUPLICATE_TIMES;
				CPPUNIT_ASSERT(count == keyShouldFound);
			} else {
				keyShouldFound = SAME_SCALE + DUPLICATE_TIMES * (SAME_ID - HOLE + 1);
				CPPUNIT_ASSERT(count == keyShouldFound);
			}
			index->endScan(scanHandle);

			// <����
			scanHandle = index->beginScan(session, findKey, false, false, None, NULL, extractor);
			count = 0;
			while (index->getNext(scanHandle, record))
				count++;
			if (num < HOLE)
				CPPUNIT_ASSERT(count == 0);
			else if (num >= testScale + HOLE)
				CPPUNIT_ASSERT(count == totalKeys);
			else if (num != SAME_ID) {
				uint keyShouldFound;
				if (num > SAME_ID)
					keyShouldFound = totalKeys - (testScale + HOLE - num) * DUPLICATE_TIMES;
				else
					keyShouldFound = (num - HOLE) * DUPLICATE_TIMES;
				CPPUNIT_ASSERT(count == keyShouldFound);
			} else {
				keyShouldFound = DUPLICATE_TIMES * (SAME_ID - HOLE);
				CPPUNIT_ASSERT(count == keyShouldFound);
			}
			index->endScan(scanHandle);

			freeSubRecord(findKey);
		}
		/**********************************************************************************************************/

		m_index->close(m_session, true);
		delete m_index;
		m_db->getSessionManager()->freeSession(session);
		m_db->freeConnection(conn);
		DrsIndice::drop(m_path);
		m_index = NULL;
		closeSmallHeap(m_heap);
		DrsHeap::drop(HEAP_NAME);
		m_heap = NULL;
		delete m_tblDef;
		m_tblDef = NULL;
		TableDef::drop(TBLDEF_NAME);

	}
}


/**
 * ����Ψһ�Բ�ѯ��������
 * @case1: ����һ�������������ݵ�����
 *			��֤�������е�ÿһ����ֵ�����ҵ�
 *			��֤ÿ��������������������������¼֮��Ĳ��������������еļ�¼�����ܱ��ҵ�
 *			��֤����������ֵ��Χ��ļ�¼�����ܱ��ҵ�
 *			���Բ�ѯ��ͬʱ��Ҫ����
 */
void IndexOperationTestCase::testUniqueScan() {
	if (isEssentialOnly())
		return;

	uint step = 5;
	uint testScale = DATA_SCALE / 5;
	// ������������
	m_tblDef = createSmallTableDef();
	m_heap = createSmallHeap(m_tblDef);
	buildHeap(m_heap, m_tblDef, testScale, step);
	createIndex(m_path, m_heap, m_tblDef);

	Connection *conn = m_db->getConnection(false);
	Session *session = m_db->getSessionManager()->allocSession("IndexOperationTestCase::testUniqueScan", conn);
	MemoryContext *memoryContext = session->getMemoryContext();
	DrsIndex *index = m_index->getIndex(1);
	IndexDef *indexDef = m_tblDef->m_indice[1];

	// ������1���в���
//	SubRecord *record = IndexKey::allocSubRecordRED(memoryContext, m_tblDef->m_maxRecSize);
	SubRecord *record = IndexKey::allocSubRecord(memoryContext, m_tblDef->getIndexDef(1), KEY_PAD);
	record->m_columns = m_tblDef->m_indice[1]->m_columns;
	record->m_numCols = m_tblDef->m_indice[1]->m_numCols;
	RowId rowId;
	SubRecord *findKey;
	RowLockHandle *rlh;
	SubToSubExtractor *extractor = SubToSubExtractor::createInst(session->getMemoryContext(), m_tblDef, indexDef,
		indexDef->m_numCols, indexDef->m_columns, record->m_numCols, record->m_columns, KEY_COMPRESS, KEY_PAD, 1);

	// ��֤���в���ļ�ֵ���ܱ��ҵ���ͬʱ���Լ���
	rlh = NULL;
	for (int i = HOLE; i < testScale + HOLE; i += step) {
		findKey = makeFindKeyWhole(i, m_heap, m_tblDef);
		findKey->m_rowId = INVALID_ROW_ID;
		CPPUNIT_ASSERT(rlh == NULL);
		bool found = index->getByUniqueKey(session, findKey, Exclusived, &rowId, record, &rlh, extractor);
		session->unlockRow(&rlh);
		CPPUNIT_ASSERT(found);
		CPPUNIT_ASSERT(rowId == record->m_rowId);
		freeSubRecord(findKey);
	}

	// ��֤û�в���ļ�ֵ�϶����ᱻ�ҵ�
	for (int i = HOLE - 2; i < HOLE; i++) {
		findKey = makeFindKeyWhole(i, m_heap, m_tblDef);
		findKey->m_rowId = INVALID_ROW_ID;
		bool found = index->getByUniqueKey(session, findKey, None, &rowId, record, NULL, extractor);
		CPPUNIT_ASSERT(!found);
		freeSubRecord(findKey);
	}
	for (int i = testScale + HOLE; i < testScale + HOLE + 2; i++) {
		findKey = makeFindKeyWhole(i, m_heap, m_tblDef);
		findKey->m_rowId = INVALID_ROW_ID;
		bool found = index->getByUniqueKey(session, findKey, None, &rowId, record, NULL, extractor);
		CPPUNIT_ASSERT(!found);
		freeSubRecord(findKey);
	}

	if (step > 1 && !isEssentialOnly()) {
		for (int i = HOLE + 1; i < testScale + HOLE + HOLE; i += step) {
			findKey = makeFindKeyWhole(i, m_heap, m_tblDef);
			findKey->m_rowId = INVALID_ROW_ID;
			bool found = index->getByUniqueKey(session, findKey, None, &rowId, record, NULL, extractor);
			CPPUNIT_ASSERT(!found);
			freeSubRecord(findKey);
		}
	}

	m_index->close(m_session, true);
	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(conn);
	delete m_index;
	DrsIndice::drop(m_path);
	m_index = NULL;
	closeSmallHeap(m_heap);
	DrsHeap::drop(HEAP_NAME);
	m_heap = NULL;
	delete m_tblDef;
	m_tblDef = NULL;
	TableDef::drop(TBLDEF_NAME);
}


/**
 * ���Է�Χ��ѯ
 * @case1: ����һ��������������������������
 *			������ִ��ȫ��Χ������/����/�ֱ𶼼���������ɨ�裬��֤ɨ������ȷ
 * @case2: ��case1�����Ļ����ϣ�����������ɸ���ֵ��ʹ�øü�ֵ������ִ��>/>=/</<=�����ֲ�ѯ
 *			��֤ÿ�β�ѯ�Ľ������ȷ��Ҫ�ر���֤���ҵļ�ֵ������������ֵ��Χ֮��Ĳ�ѯ
 */
void IndexOperationTestCase::testRangeScan() {
	if (isEssentialOnly())
		return;

	uint step = 1;

	uint testScale = DATA_SCALE / 4;
	// ������������
	m_tblDef = createSmallTableDefWithSame();
	m_heap = createSmallHeapWithSame(m_tblDef);
	uint totalKeys = buildHeapWithSame(m_heap, m_tblDef, testScale, step);
	try {
		createIndex(m_path, m_heap, m_tblDef);
	} catch (NtseException) {
		CPPUNIT_ASSERT(false);
	}

	Connection *conn = m_db->getConnection(false);
	Session *session = m_db->getSessionManager()->allocSession("IndexOperationTestCase::testRangeScan", conn);
	MemoryContext *memoryContext = session->getMemoryContext();
	DrsIndex *index = m_index->getIndex(1);
	IndexDef *indexDef = m_tblDef->m_indice[1];

	// ������1���в���
//	SubRecord *record = IndexKey::allocSubRecordRED(memoryContext, m_tblDef->m_maxRecSize);
	SubRecord *record = IndexKey::allocSubRecord(memoryContext, m_tblDef->getIndexDef(1), KEY_PAD);
	record->m_columns = m_tblDef->m_indice[1]->m_columns;
	record->m_numCols = m_tblDef->m_indice[1]->m_numCols;
	SubRecord *findKey;

	uint count;
	IndexScanHandle *scanHandle;
	RowLockHandle *rlh;

	SubToSubExtractor *extractor = SubToSubExtractor::createInst(session->getMemoryContext(), m_tblDef, indexDef,
		indexDef->m_numCols, indexDef->m_columns, record->m_numCols, record->m_columns, KEY_COMPRESS, KEY_PAD, 1000);

	/**********************************************************************************************************/
	// ��ȫ����range����
	//// 1.����ȫɨ��
	//scanHandle = index->beginScan(session, NULL, true, true, None, NULL, extractor);
	//count = 0;
	//while (index->getNext(scanHandle, record)) {
	//	count++;
	//	CPPUNIT_ASSERT(scanHandle->getRowId() != INVALID_ROW_ID);
	//}
	//CPPUNIT_ASSERT(count == totalKeys);
	//index->endScan(scanHandle);

	// ����
	scanHandle = index->beginScan(session, NULL, true, false, Shared, &rlh, extractor);
	count = 0;
	while (index->getNext(scanHandle, record)) {
		count++;
		session->unlockRow(&rlh);
	}
	CPPUNIT_ASSERT(count == totalKeys);
	index->endScan(scanHandle);
	//2.����ȫɨ��
	scanHandle = index->beginScan(session, NULL, false, true, None, NULL, extractor);
	count = 0;
	while (index->getNext(scanHandle, record)) {
		count++;
	}
	CPPUNIT_ASSERT(count == totalKeys);
	index->endScan(scanHandle);

	//// ����
	//scanHandle = index->beginScan(session, NULL, false, false, Exclusived, &rlh, extractor);
	//count = 0;
	//while (index->getNext(scanHandle, record)) {
	//	count++;
	//	session->unlockRow(&rlh);
	//}
	//CPPUNIT_ASSERT(count == totalKeys);
	//index->endScan(scanHandle);
	/**********************************************************************************************************/


	/**********************************************************************************************************/
	// ����������RangeScan���ԣ�����������ɸ���ֵ(���ض�����������ֵ��������Χ��)����ÿ����ֵ��>=/>/</<=�������
	uint keyShouldFound;
	uint start, end;
	start = 0;
	end = 20;
	for (uint i = start/*DATA_SCALE + HOLE / 2*/; i < end/*DATA_SCALE + HOLE + 2*/; i++) {
		uint num = rand() % testScale + HOLE;
		//uint num = 1009;
		if (i == end - 1)	// �������жϱ�֤�����ܲ��Ե�������Χ��ļ�ֵ�Լ�����Χ��ͬ��ֵ����
			num = HOLE - 1;
		else if (i == end - 2)
			num = testScale + HOLE;

		findKey = makeFindKeyWhole(num, m_heap, m_tblDef);
		findKey->m_rowId = INVALID_ROW_ID;

		// >=����
		scanHandle = index->beginScan(session, findKey, true, true, None, NULL, extractor);
		count = 0;
		while (index->getNext(scanHandle, record))
			count++;
		if (num < HOLE)
			CPPUNIT_ASSERT(count == totalKeys);
		else if (num >= testScale + HOLE)
			CPPUNIT_ASSERT(count == 0);
		else if (num != SAME_ID) {
			if (num < SAME_ID)
				keyShouldFound = (testScale + HOLE - num) * DUPLICATE_TIMES + SAME_SCALE;
			else
				keyShouldFound = (testScale + HOLE - num) * DUPLICATE_TIMES;
			CPPUNIT_ASSERT(count == keyShouldFound);
		} else {
			keyShouldFound = SAME_SCALE + DUPLICATE_TIMES * (testScale + HOLE - SAME_ID);
			CPPUNIT_ASSERT(count == keyShouldFound);
		}
		index->endScan(scanHandle);

		// >����
		scanHandle = index->beginScan(session, findKey, true, false, None, NULL, extractor);
		count = 0;
		while (index->getNext(scanHandle, record))
			count++;
		if (num < HOLE)
			CPPUNIT_ASSERT(count == totalKeys);
		else if (num >= testScale + HOLE)
			CPPUNIT_ASSERT(count == 0);
		else if (num != SAME_ID) {
			if (num < SAME_ID)
				keyShouldFound = (testScale + HOLE - num - 1) * DUPLICATE_TIMES + SAME_SCALE;
			else
				keyShouldFound = (testScale + HOLE - num - 1) * DUPLICATE_TIMES;
			CPPUNIT_ASSERT(count == keyShouldFound);
		} else {
			keyShouldFound = DUPLICATE_TIMES * (testScale + HOLE - SAME_ID - 1);
			CPPUNIT_ASSERT(count == keyShouldFound);
		}
		index->endScan(scanHandle);

		// <=����
		scanHandle = index->beginScan(session, findKey, false, true, None, NULL, extractor);
		count = 0;
		while (index->getNext(scanHandle, record))
			count++;
		if (num < HOLE)
			CPPUNIT_ASSERT(count == 0);
		else if (num >= testScale + HOLE)
			CPPUNIT_ASSERT(count == totalKeys);
		else if (num != SAME_ID) {
			uint keyShouldFound;
			if (num > SAME_ID)
				keyShouldFound = totalKeys - (testScale + HOLE - num - 1) * DUPLICATE_TIMES;
			else
				keyShouldFound = (num - HOLE + 1) * DUPLICATE_TIMES;
			CPPUNIT_ASSERT(count == keyShouldFound);
		} else {
			keyShouldFound = SAME_SCALE + DUPLICATE_TIMES * (SAME_ID - HOLE + 1);
			CPPUNIT_ASSERT(count == keyShouldFound);
		}
		index->endScan(scanHandle);

		// <����
		scanHandle = index->beginScan(session, findKey, false, false, None, NULL, extractor);
		count = 0;
		while (index->getNext(scanHandle, record))
			count++;
		if (num < HOLE)
			CPPUNIT_ASSERT(count == 0);
		else if (num >= testScale + HOLE)
			CPPUNIT_ASSERT(count == totalKeys);
		else if (num != SAME_ID) {
			uint keyShouldFound;
			if (num > SAME_ID)
				keyShouldFound = totalKeys - (testScale + HOLE - num) * DUPLICATE_TIMES;
			else
				keyShouldFound = (num - HOLE) * DUPLICATE_TIMES;
			CPPUNIT_ASSERT(count == keyShouldFound);
		} else {
			keyShouldFound = DUPLICATE_TIMES * (SAME_ID - HOLE);
			CPPUNIT_ASSERT(count == keyShouldFound);
		}
		index->endScan(scanHandle);

		freeSubRecord(findKey);

//		printf("%d passed\n", i);
		// TODO����Χ��=����
	}
	/**********************************************************************************************************/

	m_index->close(m_session, true);
	delete m_index;
	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(conn);
	DrsIndice::drop(m_path);
	m_index = NULL;
	closeSmallHeap(m_heap);
	DrsHeap::drop(HEAP_NAME);
	m_heap = NULL;
	delete m_tblDef;
	m_tblDef = NULL;
	TableDef::drop(TBLDEF_NAME);
}


/**
 * ��������������ز���
 * @case1: ���ȴ���һ����������һ����������������Ψһ����
 *			��β���������ֵ��Χ�ڵ��ڵ�ǰ�������в����ڵļ�ֵ����֤���ܱ��ɹ�����
 *			���뵱ǰ��Χ���������¼����֤���ܱ�����
 *			��tableScan���õ���������¼����֤�Ѿ����ڵļ�¼�޷������룬��֤����Ψһ��Լ���ж���ȷ
 *			��֤�������в���֮�������ļ�ֵ�����ͱ�ļ�ֵ������ȣ�˵�����붼��ȷ
 */
void IndexOperationTestCase::testInsert() {
	/***************************************************************************************/
	uint testScale = DATA_SCALE / 5;
	// ����Ψһ����������
	uint totalKeys;
	uint step = 3;
	// ������������
	m_tblDef = createSmallTableDef();
	m_heap = createSmallHeap(m_tblDef);
	totalKeys = buildHeap(m_heap, m_tblDef, testScale, step);
	createIndex(m_path, m_heap, m_tblDef);

	Connection *conn = m_db->getConnection(false);
	Session *session = m_db->getSessionManager()->allocSession("IndexOperationTestCase::testInsert", conn);
	MemoryContext *memoryContext = session->getMemoryContext();

	checkIndex(session, memoryContext, totalKeys);

	// ������1���в���
	Record *record = IndexKey::allocRecord(memoryContext, m_tblDef->m_maxRecSize);
	uint dupIndex;

	// ��֤��������û�еļ�ֵ���ܱ�����
	for (int i = HOLE + 1; i < testScale + HOLE; i += step) {
		// ���Ȳ����
		Record *temp = insertSpecifiedRecord(session, memoryContext, i);

		checkIndex(session, memoryContext, totalKeys);

		totalKeys++;

		freeRecord(temp);
	}
	for (int i = HOLE + 2; i < testScale + HOLE + END_SCALE; i += step) {	// �ٸ��Ӳ��뷶Χ�������
		// ���Ȳ����
		Record *temp = insertSpecifiedRecord(session, memoryContext, i);

		checkIndex(session, memoryContext, totalKeys);

		totalKeys++;

		freeRecord(temp);
	}

//	SubRecord *idxKey = IndexKey::allocSubRecordRED(memoryContext, m_tblDef->m_maxRecSize);
	SubRecord *idxKey = IndexKey::allocSubRecord(memoryContext, m_tblDef->getIndexDef(0), KEY_PAD);
	u16 *columns = new u16[m_tblDef->m_numCols];
	for (int k = 0; k < m_tblDef->m_numCols; k++)
		columns[k] = (u16)k;

	// ��֤����֮�������ܵ��������
	// ������ɨ�裬�õ�������
	DrsIndex *index = m_index->getIndex(0);
	idxKey->m_columns = m_tblDef->m_indice[0]->m_columns;
	idxKey->m_numCols = m_tblDef->m_indice[0]->m_numCols;

	rangeScanBackwardCheck(session, m_tblDef, index, NULL, idxKey, totalKeys);
	rangeScanForwardCheck(session, m_tblDef, index, NULL, idxKey, totalKeys);

	// ��֤�Ѿ�����ļ�ֵ�����ܱ�����
	// ��tablescan���õ����ɸ�record����֤�����ܱ�����
	uint reinsertNums = 20;
	IndexDef *indexDef = m_tblDef->m_indice[1];
	idxKey = IndexKey::allocSubRecordRED(memoryContext, m_tblDef->m_maxRecSize);
	idxKey->m_columns = columns;
	idxKey->m_numCols = m_tblDef->m_numCols;
	DrsHeapScanHandle *scanHandle = m_heap->beginScan(session, SubrecExtractor::createInst(session, m_tblDef, idxKey), None, NULL, false);
	int count = 0;
	while (true) {
		if (m_heap->getNext(scanHandle, idxKey)) {
			if (!m_heap->getNext(scanHandle, idxKey))
				break;
		} else
			break;

		record->m_size = idxKey->m_size;
		record->m_rowId = idxKey->m_rowId;
		memcpy(record->m_data, idxKey->m_data, idxKey->m_size);

		try {
			session->startTransaction(TXN_INSERT, m_tblDef->m_id);
			CPPUNIT_ASSERT(!m_index->insertIndexEntries(session, record, &dupIndex));
			session->endTransaction(true);
		} catch (NtseException &e) {
			cout << e.getMessage() << endl;
		}

		checkIndex(session, memoryContext, totalKeys);

		count++;

		if (count > reinsertNums)
			break;
	}
	m_heap->endScan(scanHandle);

	// ��֤����֮�������ܵ��������
	// ������ɨ�裬�õ�������
	idxKey = IndexKey::allocSubRecord(memoryContext, m_tblDef->getIndexDef(0), KEY_PAD);
	idxKey->m_columns = m_tblDef->m_indice[0]->m_columns;
	idxKey->m_numCols = m_tblDef->m_indice[0]->m_numCols;

	rangeScanBackwardCheck(session, m_tblDef, index, NULL, idxKey, totalKeys);
	rangeScanForwardCheck(session, m_tblDef, index, NULL, idxKey, totalKeys);

	m_index->close(m_session, true);
	delete m_index;
	DrsIndice::drop(m_path);
	m_index = NULL;
	closeSmallHeap(m_heap);
	DrsHeap::drop(HEAP_NAME);
	m_heap = NULL;
	delete m_tblDef;
	m_tblDef = NULL;
	TableDef::drop(TBLDEF_NAME);

	delete[] columns;

	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(conn);
	/***************************************************************************************/
}


/**
 * ��������ɾ����ز���
 * @case1: ����һ��������������������������ݣ�Ȼ���ո�������ֵ��¼ɾ��һ���ķ�ʽ���������ɾ�������ļ�¼��
 *			����ѭ����ֱ������Ϊ�գ���֤ÿ��ɾ�����ǳɹ�
 * @case2: ����һ��������������������������ݣ�Ȼ���ո�������ֵ��¼ɾ��һ���ķ�ʽ���������ɾ�������ļ�¼��
 *			����ѭ����ֱ������Ϊ�գ���֤ÿ��ɾ�����ǳɹ�
 * @˵����ǰ������������Ϊ���ǲ�������ɾ����ʽ�����Ĳ�ͬɾ��SMO�ķ�ʽ��ͬ�����̶ȸ��Ǵ���
 * @case4: ִ�з�Χɾ��������������ɾ�գ���֤����Ϊ��
 */
void IndexOperationTestCase::testDelete() {
	Connection *conn = m_db->getConnection(false);
	Session *session = m_db->getSessionManager()->allocSession("IndexOperationTestCase::testDelete", conn);
	MemoryContext *memoryContext = session->getMemoryContext();
	uint testScale = DATA_SCALE / 5;

	/***************************************************************************************/
	// ����ɾ��
	{
		uint totalKeys, items;
		uint step = 1;
		// ������������
		m_tblDef = createSmallTableDef();
		m_heap = createSmallHeap(m_tblDef);
		items = totalKeys = buildHeap(m_heap, m_tblDef, testScale, step);
		createIndex(m_path, m_heap, m_tblDef);

		checkIndex(session, memoryContext, totalKeys);

		// ����������ɾ��
		Record *record = IndexKey::allocRecord(memoryContext, m_tblDef->m_maxRecSize);

		// ����һ�������¼�������ո�������¼ɾ��һ���ķ�ʽ����ɾ����ѭ��������Ϊ��
		DrsIndex *index = m_index->getIndex(0);
		int no[DATA_SCALE];
		int seq = 0;
		for (int i = HOLE; i < HOLE + testScale; i++) {
			no[i - HOLE] = 1;
		}
		for (int i = HOLE; i < HOLE + testScale; i++) {
			// ����ȷ����ǰҪɾ���ļ�¼
			for (int j = 0; j < 2; j++) {
				while (true) {
					seq++;
					if (seq == testScale)
						seq = 0;
					if (no[seq] != 0)
						break;
				}
			}
			no[seq] = 0;
			int key = seq + HOLE;

			char name[50];
			sprintf(name, "Kenneth Tse Jr. %d \0", key);
			record = createRecordRedundant(m_tblDef, 0, key, name, key + ((u64)key << 32), (u32)(-1) - key);
			record->m_rowId = rowids[seq];
			session->startTransaction(TXN_DELETE, m_tblDef->m_id);
			EXCPT_OPER(m_index->deleteIndexEntries(session, record, NULL));
			session->endTransaction(true);

			freeRecord(record);

			checkIndex(session, memoryContext, totalKeys);

			items--;
		}

		// ��Χɨ����֤����Ϊ��
		IndexScanHandle *indexHandle;
		//SubRecord *idxKey = IndexKey::allocSubRecordRED(memoryContext, m_tblDef->m_maxRecSize);
		SubRecord *idxKey = IndexKey::allocSubRecord(memoryContext, m_tblDef->getIndexDef(0), KEY_PAD);
		idxKey->m_columns = m_tblDef->m_indice[0]->m_columns;
		idxKey->m_numCols = m_tblDef->m_indice[0]->m_numCols;
		SubToSubExtractor *extractor = SubToSubExtractor::createInst(session->getMemoryContext(), m_tblDef, m_tblDef->m_indice[0],
			m_tblDef->m_indice[0]->m_numCols, m_tblDef->m_indice[0]->m_columns, idxKey->m_numCols, idxKey->m_columns, KEY_COMPRESS, KEY_PAD, 1000);
		indexHandle = index->beginScan(session, NULL, true, true, None, NULL, extractor);
		int count = 0;
		while (index->getNext(indexHandle, idxKey))
			count++;
		PageHandle *pageHandle = ((DrsIndexRangeScanHandle*)indexHandle)->getScanInfo()->m_pageHandle;
		IndexPage *page = (IndexPage*)pageHandle->getPage();
		CPPUNIT_ASSERT(page->m_prevPage == INVALID_ROW_ID);
		CPPUNIT_ASSERT(page->m_nextPage == INVALID_ROW_ID);
		index->endScan(indexHandle);
		CPPUNIT_ASSERT(count == items);
		//CPPUNIT_ASSERT(count == 0);

		m_index->close(m_session, true);
		delete m_index;
		DrsIndice::drop(m_path);
		m_index = NULL;
		closeSmallHeap(m_heap);
		DrsHeap::drop(HEAP_NAME);
		m_heap = NULL;
		delete m_tblDef;
		m_tblDef = NULL;
		TableDef::drop(TBLDEF_NAME);
	}

	{
		uint totalKeys, items;
		uint step = 1;
		// ������������
		m_tblDef = createSmallTableDef();
		m_heap = createSmallHeap(m_tblDef);
		items = totalKeys = buildHeap(m_heap, m_tblDef, testScale, step);
		createIndex(m_path, m_heap, m_tblDef);

		checkIndex(session, memoryContext, totalKeys);

		// ����������ɾ��
		Record *record = IndexKey::allocRecord(memoryContext, m_tblDef->m_maxRecSize);

		IndexScanHandle *indexHandle;
	//	SubRecord *idxKey = IndexKey::allocSubRecordRED(memoryContext, m_tblDef->m_maxRecSize);
		SubRecord *idxKey = IndexKey::allocSubRecord(memoryContext, m_tblDef->getIndexDef(0), KEY_PAD);
		idxKey->m_columns = m_tblDef->m_indice[0]->m_columns;
		idxKey->m_numCols = m_tblDef->m_indice[0]->m_numCols;

		SubToSubExtractor *extractor = SubToSubExtractor::createInst(session->getMemoryContext(), m_tblDef, m_tblDef->m_indice[0],
			m_tblDef->m_indice[0]->m_numCols, m_tblDef->m_indice[0]->m_columns, idxKey->m_numCols, idxKey->m_columns, KEY_COMPRESS, KEY_PAD, 1000);

		// ����һ�������¼�������ո�������¼ɾ��һ���ķ�ʽ����ɾ����ѭ��������Ϊ��
		DrsIndex *index = m_index->getIndex(0);
		int no[DATA_SCALE];
		int seq = testScale - 1;
		for (int i = HOLE; i < HOLE + testScale; i++) {
			no[i - HOLE] = 1;
		}
		for (int i = HOLE; i < HOLE + testScale; i++) {
			// ����ȷ����ǰҪɾ���ļ�¼
			for (int j = 0; j < 2; j++) {
				while (true) {
					seq--;
					if (seq == -1)
						seq = testScale - 1;
					if (no[seq] != 0)
						break;
				}
			}
			no[seq] = 0;
			int key = seq + HOLE;

			char name[50];
			sprintf(name, "Kenneth Tse Jr. %d \0", key);
			record = createRecordRedundant(m_tblDef, 0, key, name, key + ((u64)key << 32), (u32)(-1) - key);
			record->m_rowId = rowids[seq];
			try {
				session->startTransaction(TXN_DELETE, m_tblDef->m_id);
				m_index->deleteIndexEntries(session, record, NULL);
				session->endTransaction(true);
			} catch (NtseException &e) {
				cout << e.getMessage() << endl;
			}

			freeRecord(record);

			checkIndex(session, memoryContext, totalKeys);

			items--;
		}

		indexHandle = index->beginScan(session, NULL, true, true, None, NULL, extractor);
		int count = 0;
		while (index->getNext(indexHandle, idxKey))
			count++;
		PageHandle *pageHandle = ((DrsIndexRangeScanHandle*)indexHandle)->getScanInfo()->m_pageHandle;
		IndexPage *page = (IndexPage*)pageHandle->getPage();
		CPPUNIT_ASSERT(page->m_prevPage == INVALID_ROW_ID);
		CPPUNIT_ASSERT(page->m_nextPage == INVALID_ROW_ID);
		index->endScan(indexHandle);
		CPPUNIT_ASSERT(count == items);
		//CPPUNIT_ASSERT(count == 0);

		m_index->close(m_session, true);
		delete m_index;
		DrsIndice::drop(m_path);
		m_index = NULL;
		closeSmallHeap(m_heap);
		DrsHeap::drop(HEAP_NAME);
		m_heap = NULL;
		delete m_tblDef;
		m_tblDef = NULL;
		TableDef::drop(TBLDEF_NAME);
	}

	{
		uint testRangeScale = 30;
		uint totalKeys, items;
		uint step = 1;
		// ������������
		m_tblDef = createSmallTableDef();
		m_heap = createSmallHeap(m_tblDef);
		items = totalKeys = buildHeap(m_heap, m_tblDef, testRangeScale, step);
		createIndex(m_path, m_heap, m_tblDef);

		checkIndex(session, memoryContext, totalKeys);

		// ����������ɾ��
		Record *record = IndexKey::allocRecord(memoryContext, m_tblDef->m_maxRecSize);

		IndexScanHandle *indexHandle;
//		SubRecord *idxKey = IndexKey::allocSubRecordRED(memoryContext, m_tblDef->m_maxRecSize);
		SubRecord *idxKey = IndexKey::allocSubRecord(memoryContext, m_tblDef->getIndexDef(0), KEY_PAD);
		idxKey->m_columns = m_tblDef->m_indice[0]->m_columns;
		idxKey->m_numCols = m_tblDef->m_indice[0]->m_numCols;

		SubToSubExtractor *extractor = SubToSubExtractor::createInst(session->getMemoryContext(), m_tblDef, m_tblDef->m_indice[0],
			m_tblDef->m_indice[0]->m_numCols, m_tblDef->m_indice[0]->m_columns, idxKey->m_numCols, idxKey->m_columns, KEY_COMPRESS, KEY_PAD, 1000);

		u64 savePoint = memoryContext->setSavepoint();
		RowLockHandle *rlh;
		//	��Χɾ��ȫ������
		DrsIndex *rangeIndex = m_index->getIndex(0);
		indexHandle = rangeIndex->beginScan(session, NULL, true, true, Exclusived, &rlh, extractor);
		int count = 0;
		while (rangeIndex->getNext(indexHandle, idxKey)) {
			// ȥ��ӦHeapȡ��¼
			record->m_format = REC_FIXLEN;
			m_heap->getRecord(m_session, idxKey->m_rowId, record);
			record->m_format = REC_REDUNDANT;
			session->startTransaction(TXN_DELETE, m_tblDef->m_id);
			m_index->deleteIndexEntries(session, record, indexHandle);
			session->endTransaction(true);
			session->unlockRow(&rlh);
			count++;

			checkIndex(session, memoryContext, totalKeys);
		}
		rangeIndex->endScan(indexHandle);
		CPPUNIT_ASSERT(count == totalKeys);

		memoryContext->resetToSavepoint(savePoint);

		// ��֤����Ϊ��
		DrsIndex *index = m_index->getIndex(0);
		IndexDef *indexDef = m_tblDef->m_indice[0];
		indexHandle = index->beginScan(session, NULL, true, true, Exclusived, &rlh, extractor);
		CPPUNIT_ASSERT(!index->getNext(indexHandle, idxKey));
		PageHandle *pageHandle = ((DrsIndexRangeScanHandle*)indexHandle)->getScanInfo()->m_pageHandle;
		IndexPage *page = (IndexPage*)pageHandle->getPage();
		CPPUNIT_ASSERT(page->m_prevPage == INVALID_ROW_ID);
		CPPUNIT_ASSERT(page->m_nextPage == INVALID_ROW_ID);
		CPPUNIT_ASSERT(page->m_pageCount == 0);
		index->endScan(indexHandle);

		m_index->close(m_session, true);
		delete m_index;
		DrsIndice::drop(m_path);
		m_index = NULL;
		closeSmallHeap(m_heap);
		DrsHeap::drop(HEAP_NAME);
		m_heap = NULL;
		delete m_tblDef;
		m_tblDef = NULL;
		TableDef::drop(TBLDEF_NAME);
	}

	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(conn);
}


u64 getFirstLeafPageLSN(Session *session, MemoryContext *memoryContext, DrsIndex *index) {
	u64 lsn;
	IndexScanHandle *indexHandle;
	indexHandle = index->beginScan(session, NULL, true, true, None, NULL, NULL);
	while (index->getNext(indexHandle, NULL)) {
		lsn = ((DrsIndexRangeScanHandle*)indexHandle)->getScanInfo()->m_pageLSN;
		break;
	}
	index->endScan(indexHandle);
	return lsn;
}


/**
 * �����������²���
 * @case1: ���������������ݵ�����������������������ȫ�����£����Ҹ��µ�һ�����ԣ����������ĸ��»���ɴ�Χ�������ƶ�
 *			��֤����֮��ļ�ֵ���ܱ���ѯ�ҵ�������֮ǰ�Ķ�����
 * @case2: ��ǰ����µĻ����ϣ�����һ�����Եļ�ֵ���м�1���£�Ҳ���ǽ����ڵ�����������ֵ��ǰ���Ǹ���¼�ļ�ֵ����Ϊ�ͺ����¼��ֵ���
 *			����������Ψһ����������������Ĳ���������ɹ����������������һ����ֵ��ɹ��������ֻ֤��һ����³ɹ�
 */
void IndexOperationTestCase::testUpdate() {
	uint testScale = DATA_SCALE / 10;
	{
		uint totalKeys, items;
		uint step = 1;
		// ������������
		m_tblDef = createSmallTableDef();
		m_heap = createSmallHeap(m_tblDef);
		items = totalKeys = buildHeap(m_heap, m_tblDef, testScale, step);
		createIndex(m_path, m_heap, m_tblDef);

		Connection *conn = m_db->getConnection(false);
		Session *session = m_db->getSessionManager()->allocSession("IndexOperationTestCase::testUpdate", conn);
		MemoryContext *memoryContext = session->getMemoryContext();
//		SubRecord *idxKey = IndexKey::allocSubRecordRED(memoryContext, m_tblDef->m_maxRecSize);
		SubRecord *idxKey = IndexKey::allocSubRecord(memoryContext, m_tblDef->getIndexDef(1), KEY_PAD);
		idxKey->m_columns = m_tblDef->m_indice[1]->m_columns;
		idxKey->m_numCols = m_tblDef->m_indice[1]->m_numCols;

		/**
		// �����������������ĵ�һ����ֵ�������������ɾ��λ���нϴ�ƫ�Ƶ����
		// ԭ�������userid, name, bankaccount, balance,����userid��HOLE~~testScale+HOLE-1����������ʹ��userid��ֵ
		// ����ֻ��userid���£���ԭ����userid����testScale+HOLE�������������Ե�ֵ���䣬�Ծ�ʹ��HOLE~~testScale+HOLE
		 */
		{
			const struct IndexStatus *status = &(m_index->getIndex(1)->getStatus());

			SubRecord *before, *after;
			before = IndexKey::allocSubRecordRED(memoryContext, m_tblDef->m_maxRecSize);

			u16 *columns = new u16[m_tblDef->m_numCols];
			for (int k = 0; k < m_tblDef->m_numCols; k++)
				columns[k] = (u16)k;

			before->m_numCols = m_tblDef->m_numCols;
			before->m_columns = columns;

			Record *oldrecord;
			uint updated = 0;
			for (uint i = HOLE; i < testScale + HOLE; i++) {
				// �����¼�ֵ
				char name[50];
				sprintf(name, "Kenneth Tse Jr. %d \0", i);
				after = createUpdateAfterRecordRedundant1(m_tblDef, rowids[i - HOLE], i + testScale + HOLE);

				// ����ɼ�ֵ
				oldrecord = createRecord(m_tblDef, 0, i, name, i + ((u64)i << 32), (u32)(-1) - i);
				oldrecord->m_rowId = rowids[i - HOLE];
				before->m_size = oldrecord->m_size;
				before->m_data = oldrecord->m_data;
				before->m_rowId = oldrecord->m_rowId;

				session->startTransaction(TXN_UPDATE, m_tblDef->m_id);
				// ���и���
				uint dupIndex = (uint)-1;
				if (m_index->updateIndexEntries(session, before, after, false, &dupIndex))
					updated++;
				session->endTransaction(true);

				CPPUNIT_ASSERT(dupIndex == (uint)-1);

				freeSubRecord(after);
				freeRecord(oldrecord);

				checkIndex(session, memoryContext, totalKeys);
			}

			delete[] columns;

			CPPUNIT_ASSERT(status->m_dboStats->m_statArr[DBOBJ_ITEM_UPDATE] == updated);
		}

		// ��֤���º�ļ�ֵ�����ҵ�������֮ǰ�Ķ��Ҳ���
		{
			SubRecord *findKey;

			IndexDef *indexDef = m_tblDef->m_indice[1];
			DrsIndex *index = m_index->getIndex(1);
//			SubRecord *record = IndexKey::allocSubRecordRED(memoryContext, m_tblDef->m_maxRecSize);
			SubRecord *record = IndexKey::allocSubRecord(memoryContext, m_tblDef->getIndexDef(1), KEY_PAD);
			record->m_columns = m_tblDef->m_indice[1]->m_columns;
			record->m_numCols = m_tblDef->m_indice[1]->m_numCols;

			SubToSubExtractor *extractor = SubToSubExtractor::createInst(session->getMemoryContext(), m_tblDef, indexDef,
				indexDef->m_numCols, indexDef->m_columns, record->m_numCols, record->m_columns, KEY_COMPRESS, KEY_PAD, 1);

			for (uint i = HOLE; i < testScale + HOLE; i++) {
				char name[50];
				sprintf(name, "Kenneth Tse Jr. %d \0", i);
				u64 userid = i;
				u64 bankaccount = i + ((u64)i << 32);

				SubRecordBuilder sb(m_tblDef, KEY_PAD);
				findKey = sb.createSubRecordByName("UserId"" ""UserName"" ""BankAccount", &userid, name, &bankaccount);
				findKey->m_rowId = INVALID_ROW_ID;

				RowId rowId;
				bool found = index->getByUniqueKey(session, findKey, None, &rowId, record, NULL, extractor);
				CPPUNIT_ASSERT(!found);

				freeSubRecord(findKey);

				userid = userid + testScale + HOLE;
				findKey = sb.createSubRecordByName("UserId"" ""UserName"" ""BankAccount", &userid, name, &bankaccount);
				findKey->m_rowId = INVALID_ROW_ID;
				found = index->getByUniqueKey(session, findKey, None, &rowId, record, NULL, extractor);
				CPPUNIT_ASSERT(found);
				freeSubRecord(findKey);
			}

			rangeScanBackwardCheck(session, m_tblDef, index, NULL, idxKey, totalKeys);
			rangeScanForwardCheck(session, m_tblDef, index, NULL, idxKey, totalKeys);
		}


		/**
		// ���Ը���ʧ�ܻ��ˣ���ǰ����µĻ����ϣ���ǰһ����¼���³�˳��ĺ�һ����¼�����������л���ֳ�ͻ��
		// �������һ��������¼���Ը��³ɹ�������������ɹ�
		// ���磺��useridΪHOLE + testScale + HOLE�ļ�ֵ��ȫ����ΪHOLE+testScale+HOLE+1�ļ�ֵ����
		 */
		{
			const struct IndexStatus *status = &(m_index->getIndex(1)->getStatus());

			SubRecord *before, *after;
			before = IndexKey::allocSubRecordRED(memoryContext, m_tblDef->m_maxRecSize);

			u16 *columns = new u16[m_tblDef->m_numCols];
			for (int k = 0; k < m_tblDef->m_numCols; k++)
				columns[k] = (u16)k;

			before->m_numCols = m_tblDef->m_numCols;
			before->m_columns = columns;

			Record *oldrecord;
			uint totalUpdated = (uint)status->m_dboStats->m_statArr[DBOBJ_ITEM_UPDATE];
			for (uint i = HOLE; i < testScale + HOLE; i++) {
				// �����¼�ֵ
				char name[50];
				uint newi = i + testScale + HOLE + 1;
				sprintf(name, "Kenneth Tse Jr. %d \0", i + 1);
				after = createUpdateAfterRecordRedundantWhole(m_tblDef, rowids[i - HOLE], newi, name, (i + 1) + ((u64)(i + 1) << 32), (u32)(-1) - (i + 1));

				// ����ɼ�ֵ
				sprintf(name, "Kenneth Tse Jr. %d \0", i);
				oldrecord = createRecord(m_tblDef, 0, i + testScale + HOLE, name, i + ((u64)i << 32), (u32)(-1) - i);
				oldrecord->m_rowId = rowids[i - HOLE];
				before->m_size = oldrecord->m_size;
				before->m_data = oldrecord->m_data;
				before->m_rowId = oldrecord->m_rowId;

				session->startTransaction(TXN_UPDATE, m_tblDef->m_id);
				// ���и���
				uint dupIndex = (uint)-1;
				bool successful = m_index->updateIndexEntries(session, before, after, false, &dupIndex);
				session->endTransaction(true);
				CPPUNIT_ASSERT(i == testScale + HOLE - 1 || dupIndex == 1);
				if (i < testScale + HOLE - 1)
					CPPUNIT_ASSERT(!successful);
				else
					CPPUNIT_ASSERT(successful);

				if (successful)
					totalUpdated++;

				//checkIndex(session, memoryContext, totalKeys);

				freeSubRecord(after);
				freeRecord(oldrecord);
			}

			delete[] columns;

			CPPUNIT_ASSERT(status->m_dboStats->m_statArr[DBOBJ_ITEM_UPDATE] == totalUpdated);
		}

		// ��֤���º�ļ�ֵ�����ҵ�������֮ǰ�Ķ��Ҳ���
		{
			SubRecord *findKey;

			DrsIndex *index = m_index->getIndex(1);
			IndexDef *indexDef = m_tblDef->m_indice[1];
		//	SubRecord *record = IndexKey::allocSubRecordRED(memoryContext, m_tblDef->m_maxRecSize);
			SubRecord *record = IndexKey::allocSubRecord(memoryContext, m_tblDef->getIndexDef(1), KEY_PAD);
			record->m_columns = m_tblDef->m_indice[1]->m_columns;
			record->m_numCols = m_tblDef->m_indice[1]->m_numCols;

			SubToSubExtractor *extractor = SubToSubExtractor::createInst(session->getMemoryContext(), m_tblDef, indexDef,
				indexDef->m_numCols, indexDef->m_columns, record->m_numCols, record->m_columns, KEY_COMPRESS, KEY_PAD, 1);

			uint i = HOLE + testScale - 1;
			{
				char name[50];
				sprintf(name, "Kenneth Tse Jr. %d \0", i);
				u64 userid = i + testScale + HOLE;
				u64 bankaccount = i + ((u64)i << 32);

				SubRecordBuilder sb(m_tblDef, KEY_PAD);
				findKey = sb.createSubRecordByName("UserId"" ""UserName"" ""BankAccount", &userid, name, &bankaccount);
				findKey->m_rowId = INVALID_ROW_ID;

				RowId rowId;
				bool found = index->getByUniqueKey(session, findKey, None, &rowId, record, NULL, extractor);
				CPPUNIT_ASSERT(!found);
				freeSubRecord(findKey);

				uint newi = i + 1;
				sprintf(name, "Kenneth Tse Jr. %d \0", newi);
				userid = newi + testScale + HOLE;
				bankaccount = newi + ((u64)newi << 32);
				findKey = sb.createSubRecordByName("UserId"" ""UserName"" ""BankAccount", &userid, name, &bankaccount);
				findKey->m_rowId = INVALID_ROW_ID;
				found = index->getByUniqueKey(session, findKey, None, &rowId, record, NULL, extractor);
				CPPUNIT_ASSERT(found);

				freeSubRecord(findKey);
			}

			rangeScanBackwardCheck(session, m_tblDef, index, NULL, idxKey, totalKeys);
			rangeScanForwardCheck(session, m_tblDef, index, NULL, idxKey, totalKeys);
		}


		{	// ֻ����һ����������һ������Ҫ����
			// ��������1��2�Ķ���ֻ�����name��һ�����ԣ�ֻ������һ���ԣ�����һ�����������漰������
			SubRecord *before, *after;
			before = IndexKey::allocSubRecordRED(memoryContext, m_tblDef->m_maxRecSize);

			u16 *columns = new u16[m_tblDef->m_numCols];
			for (int k = 0; k < m_tblDef->m_numCols; k++)
				columns[k] = (u16)k;

			before->m_numCols = m_tblDef->m_numCols;
			before->m_columns = columns;

			Record *oldrecord;
			uint i = HOLE;
			{
				// �����¼�ֵ
				char name[50];
				uint newi = i + testScale + HOLE + 1;
				sprintf(name, "Kenneth Tse Jr. %d \0", i + testScale + HOLE + testScale + HOLE);
				//after = createUpdateAfterRecordRedundantWhole(tableDef, rowids[i - HOLE], newi, name, (i + 1) + ((u64)(i + 1) << 32), (u32)(-1) - (i + 1));
				after = createUpdateAfterRecordRedundant2(m_tblDef, rowids[i - HOLE], name);

				// ����ɼ�ֵ
				sprintf(name, "Kenneth Tse Jr. %d \0", i);
				oldrecord = createRecord(m_tblDef, 0, i + testScale + HOLE, name, i + ((u64)i << 32), (u32)(-1) - i);
				oldrecord->m_rowId = rowids[i - HOLE];
				before->m_size = oldrecord->m_size;
				before->m_data = oldrecord->m_data;
				before->m_rowId = oldrecord->m_rowId;

				// �õ������������������Ӧҳ���LSN
				u64 oldlsn1 = getFirstLeafPageLSN(session, memoryContext, m_index->getIndex(0));
				u64 oldlsn2 = getFirstLeafPageLSN(session, memoryContext, m_index->getIndex(1));

				session->startTransaction(TXN_UPDATE, m_tblDef->m_id);
				// ���и���
				uint dupIndex = (uint)-1;
				bool updated = m_index->updateIndexEntries(session, before, after, false, &dupIndex);
				session->endTransaction(true);
				CPPUNIT_ASSERT(updated);
				CPPUNIT_ASSERT(dupIndex == (uint)-1);

				// ���µõ������������������Ӧҳ���LSN
				u64 newlsn1 = getFirstLeafPageLSN(session, memoryContext, m_index->getIndex(0));
				u64 newlsn2 = getFirstLeafPageLSN(session, memoryContext, m_index->getIndex(1));

				// ��֤����������LSN��ȣ���ʾ������û�б�����
				CPPUNIT_ASSERT(oldlsn1 == newlsn1);
				// ��֤���µ�����LSN�ı䣬��ʾ�и���
				CPPUNIT_ASSERT(oldlsn2 != newlsn2);

				//checkIndex(session, memoryContext, totalKeys);

				freeSubRecord(after);
				freeRecord(oldrecord);
			}

			delete[] columns;
		}

		m_index->close(m_session, true);
		delete m_index;
		DrsIndice::drop(m_path);
		m_index = NULL;
		closeSmallHeap(m_heap);
		DrsHeap::drop(HEAP_NAME);
		m_heap = NULL;
		delete m_tblDef;
		m_tblDef = NULL;
		TableDef::drop(TBLDEF_NAME);

		m_db->getSessionManager()->freeSession(session);
		m_db->freeConnection(conn);
	}
}


/**
 * ����һ�������������Կ������ϵĲ�����ͬʱ���Ӹ�����
 * �����ڿ�����������Ψһ��ѯ�ͷ�Χ��ѯ����֤���Ҳ������
 * �ڿ������ϲ������������ݣ���֤Ҷ���ĸ�����������Ȼ��ɾ����һ��Ҷ���������
 * ֮�����������ȵ�ǰ�������м�ֵ��С�ļ�¼����֤��һ��Ҷ���ᱻ���ѣ����Բ���SMO������ĳ��·��
 */
void IndexOperationTestCase::testOpsOnEmptyIndex() {
	uint step = 1;

	// ������������
	m_tblDef = createSmallTableDefWithSame();
	m_heap = createSmallHeapWithSame(m_tblDef);
	try {
		createIndex(m_path, m_heap, m_tblDef);
	} catch (NtseException) {
		CPPUNIT_ASSERT(false);
	}

	Connection *conn = m_db->getConnection(false);
	Session *session = m_db->getSessionManager()->allocSession("IndexOperationTestCase::testOpsOnEmptyIndex", conn);
	MemoryContext *memoryContext = session->getMemoryContext();
	DrsIndex *index = m_index->getIndex(0);
	IndexDef *indexDef = m_tblDef->m_indice[0];

	// ������1���в���
	//SubRecord *record = IndexKey::allocSubRecordRED(memoryContext, m_tblDef->m_maxRecSize);
	SubRecord *record = IndexKey::allocSubRecord(memoryContext, m_tblDef->getIndexDef(1), KEY_PAD);
	record->m_columns = m_tblDef->m_indice[0]->m_columns;
	record->m_numCols = m_tblDef->m_indice[0]->m_numCols;

	SubToSubExtractor *extractor = SubToSubExtractor::createInst(session->getMemoryContext(), m_tblDef, indexDef,
		indexDef->m_numCols, indexDef->m_columns, record->m_numCols, record->m_columns, KEY_COMPRESS, KEY_PAD, 1000);

	SubRecord *findKey;

	uint count;
	IndexScanHandle *scanHandle;
	RowId rowId;

	// Unique Scan������ʲô��ֵ�϶��Ҳ�������
	findKey = makeFindKeyWhole(HOLE + 1, m_heap, m_tblDef);
	findKey->m_rowId = INVALID_ROW_ID;
	bool found = index->getByUniqueKey(session, findKey, None, &rowId, record, NULL, extractor);
	CPPUNIT_ASSERT(!found);

	// Range Scan������ʲô��Χ�϶��Ҳ�������
	scanHandle = index->beginScan(session, findKey, true, true, None, NULL, extractor);
	count = 0;
	while (index->getNext(scanHandle, record))
		count++;
	CPPUNIT_ASSERT(count == 0);
	index->endScan(scanHandle);
	freeSubRecord(findKey);

	// ���ݲ���ģʽ��ѡ��ɾ���������ݺͲ�����������
	u16 maxKeyNum;
	u16 nextinsertScale;
	{
		maxKeyNum = 1005;	// incorrect
		nextinsertScale = 10;
#if NTSE_PAGE_SIZE == 8192
		maxKeyNum = 1116;
		nextinsertScale = 20;
#endif
	}

	int totalKeys = 0;
	// ������������������
	RowId deleteRowId =  INVALID_ROW_ID;
	for (int i = HOLE; i < DATA_SCALE + HOLE; i++) {
		Record *insertedRecord = insertSpecifiedRecord(session, memoryContext, i);
		if (i == maxKeyNum)	// ���ֵ��Ҫ����������ͬ���������ǹ̶���
			deleteRowId = insertedRecord->m_rowId;

		totalKeys++;

		freeRecord(insertedRecord);
	}

	// ɾ����һ����ҳ��������ٲ���������¸�ҳ����ѣ����Է��ѹ��̸�ҳ���ֵ��Ҫ���������
	CPPUNIT_ASSERT(deleteRowId != INVALID_ROW_ID);
	int key = maxKeyNum;
	char name[50];
	sprintf(name, "Kenneth Tse Jr. %d \0", key);
	Record *deleterecord = createRecordRedundant(m_tblDef, 0, key, name, key + ((u64)key << 32), (u32)(-1) - key);
	deleterecord->m_rowId = deleteRowId;
	try {
		session->startTransaction(TXN_DELETE, m_tblDef->m_id);
		m_index->deleteIndexEntries(session, deleterecord, NULL);
		session->endTransaction(true);
		totalKeys--;
	} catch (NtseException &e) {
		cout << e.getMessage() << endl;
	}

	for (int i = HOLE - nextinsertScale; i < HOLE; i++) {
		Record *temp = insertSpecifiedRecord(session, memoryContext, i);

		totalKeys++;

		freeRecord(temp);
	}


	checkIndex(session, memoryContext, totalKeys);

	freeRecord(deleterecord);

	m_index->close(m_session, true);
	delete m_index;
	DrsIndice::drop(m_path);
	m_index = NULL;
	closeSmallHeap(m_heap);
	DrsHeap::drop(HEAP_NAME);
	m_heap = NULL;
	delete m_tblDef;
	m_tblDef = NULL;
	TableDef::drop(TBLDEF_NAME);

	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(conn);
}


/**
 * ���������ṹ��ȷ��
 * ��Ҫ����һ��3�����ϵ���������Ҫ����������ĳ����Ҷ�ڵ���ܳ��ֵļ�ֵ�ɾ���������
 * ���������£��������µĲ�����߲�ѯ��ִ�е��ýڵ㣬ͬʱ����ѯ���߲���ļ�ֵ
 * ��ȵ�ǰ�������ֵ������ȷ�Ķ�λ�Ƕ�λ����ҳ������һ�Ȼ��������¶�λ
 * ��������ɵľ������һ����ܳ��ֶ������ݱȸ��׽ڵ������
 */
void IndexOperationTestCase::testIndexStructure() {
	uint totalKeys, items;
	uint step = 2;
	// ������������
	m_tblDef = createSmallTableDef();
	m_heap = createSmallHeap(m_tblDef);
	items = totalKeys = buildHeap(m_heap, m_tblDef, DATA_SCALE * 10, step);
	createIndex(m_path, m_heap, m_tblDef);

	Connection *conn = m_db->getConnection(false);
	Session *session = m_db->getSessionManager()->allocSession("IndexOperationTestCase::testUpdate", conn);
	MemoryContext *memoryContext = session->getMemoryContext();
	SubRecord *idxKey = IndexKey::allocSubRecordRED(memoryContext, m_tblDef->m_maxRecSize);
	idxKey->m_columns = m_tblDef->m_indice[1]->m_columns;
	idxKey->m_numCols = m_tblDef->m_indice[1]->m_numCols;
	DrsIndex *index = m_index->getIndex(1);

	insertSpecifiedRecord(session, memoryContext, 1283);	// ��ǰһ��ҳ�����������SMO�Ľ���
	SubRecord *findKey = makeFindKeyWhole(1310, m_heap, m_tblDef);
	findKey->m_rowId = INVALID_ROW_ID;
	RowLockHandle *rlh;
	IndexScanHandle *handle = index->beginScan(session, findKey, false, true, Exclusived, &rlh, NULL);
	uint count = 0;
	while (index->getNext(handle, NULL) && count <= 13) {
		index->deleteCurrent(handle);
		count++;
		session->unlockRow(&rlh);
	}
	index->endScan(handle);

	getRangeRecords(session, index, NULL, true);

	m_index->close(m_session, true);
	delete m_index;
	DrsIndice::drop(m_path);
	m_index = NULL;
	closeSmallHeap(m_heap);
	DrsHeap::drop(HEAP_NAME);
	m_heap = NULL;
	delete m_tblDef;
	m_tblDef = NULL;
	TableDef::drop(TBLDEF_NAME);

	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(conn);
}


/**
 * �õ�ĳ������ָ����Χ֮��������ٸ���ֵ
 */
int IndexOperationTestCase::getRangeRecords(Session *session, DrsIndex *index, SubRecord *findKey, bool includekey) {
	IndexScanHandle *indexHandle = index->beginScan(session, findKey, true, includekey, None, NULL, NULL);
	int count = 0;
	while (index->getNext(indexHandle, NULL))
		count++;
	index->endScan(indexHandle);

	return count;
}



/**
 * ���Թ��㷶Χ��������Ч��
 * ������ַ�Χ���в���
 * ��Ҫ���Ӷ�ȡ��ʵ��Χ�����̣��Աȵó�ÿ�ι������
 */
void IndexOperationTestCase::testRecordsInRange() {
	uint step = 1;

	uint testScal = DATA_SCALE;
	{
	// ������������
	m_tblDef = createSmallTableDefWithSame();
	m_heap = createSmallHeapWithSame(m_tblDef);
	uint totalKeys = buildHeapWithSame(m_heap, m_tblDef, testScal, step);
	try {
		createIndex(m_path, m_heap, m_tblDef);
	} catch (NtseException) {
		CPPUNIT_ASSERT(false);
	}

	Connection *conn = m_db->getConnection(false);
	Session *session = m_db->getSessionManager()->allocSession("IndexOperationTestCase::testRecordsInRange", conn);
	MemoryContext *memoryContext = session->getMemoryContext();
	DrsIndex *index = m_index->getIndex(1);
	IndexDef *indexDef = m_tblDef->m_indice[1];

	// ������ֵ��HOLE-HOLE+DATASCALE������ÿ����ֵ�ظ�3�Σ�SAME_ID�ظ���SAME_SCALE��
	SubRecord *minKey, *maxKey;
	minKey = IndexKey::allocSubRecordRED(memoryContext, indexDef->m_maxKeySize);
	maxKey = IndexKey::allocSubRecordRED(memoryContext, indexDef->m_maxKeySize);
	minKey->m_format = maxKey->m_format = KEY_PAD;
	minKey->m_numCols = maxKey->m_numCols = 1;
	minKey->m_rowId = maxKey->m_rowId = INVALID_ROW_ID;
	u16 cols = 0;
	minKey->m_columns = &cols;
	maxKey->m_columns = &cols;
	int realRows;
	u64 id;
	SubRecordBuilder keyBuilder(m_tblDef, KEY_PAD);
	SubRecord *findKey;
	// ��ʼ���Ը�����Χ
	u64 savePoint = memoryContext->setSavepoint();
	// 1.����NULL-NULL
	int rows1 = (int)index->recordsInRange(session, NULL, true, NULL, false);
	memoryContext->resetToSavepoint(savePoint);
	int rows2 = (int)index->recordsInRange(session, NULL, false, NULL, true);
	memoryContext->resetToSavepoint(savePoint);
	int rows3 = (int)index->recordsInRange(session, NULL, true, NULL, true);
	memoryContext->resetToSavepoint(savePoint);
	int rows4 = (int)index->recordsInRange(session, NULL, false, NULL, false);
	memoryContext->resetToSavepoint(savePoint);
	CPPUNIT_ASSERT(rows1 == rows2 && rows1 == rows3 && rows1 == rows4);

	realRows = getRangeRecords(session, index, NULL, true);
	cout << "NULL	NULL: " << "Estimate: " << rows1 << "	Real: " << realRows << "	Diff: " << (realRows == 0 ? 1.0 : (abs(realRows - rows1)) * 1.0 / realRows) << endl;
	memoryContext->resetToSavepoint(savePoint);

	// 2. ����HOLE-NULL
	id = HOLE;
	findKey = keyBuilder.createSubRecordById("0", &id);
	minKey->m_size = findKey->m_size;
	memcpy(minKey->m_data, findKey->m_data, findKey->m_size);
	freeSubRecord(findKey);
	rows1 = (int)index->recordsInRange(session, minKey, true, NULL, false);
	memoryContext->resetToSavepoint(savePoint);
	realRows = getRangeRecords(session, index, NULL, true);
	cout << "HOLE	NULL: Estimate: " << rows1 << "	Real: " << realRows << "	Diff: " << (realRows - abs(realRows - rows1)) * 1.0 / realRows << endl;
	memoryContext->resetToSavepoint(savePoint);

	// 3. ����>= HOLE / <= DATA_SCALE * 5 + HOLE
	calcSpecifiedRange(session, m_tblDef, index, HOLE, HOLE + testScal, true, true, minKey, maxKey);
	memoryContext->resetToSavepoint(savePoint);

	// 4. ����> HOLE / < DATA_SCALE * 5 + HOLE
	calcSpecifiedRange(session, m_tblDef, index, HOLE, HOLE + testScal, false, false, minKey, maxKey);
	memoryContext->resetToSavepoint(savePoint);

	// 5. ���� >= HOLE / < SAME_ID
	calcSpecifiedRange(session, m_tblDef, index, HOLE, SAME_ID, true, false, minKey, maxKey);
	memoryContext->resetToSavepoint(savePoint);

	// 6. ���� > HOLE / <= SAME_ID
	calcSpecifiedRange(session, m_tblDef, index, HOLE, SAME_ID, false, true, minKey, maxKey);
	memoryContext->resetToSavepoint(savePoint);

	// 7. ���� >= SAME_ID / <= SAME_ID
	calcSpecifiedRange(session, m_tblDef, index, SAME_ID, SAME_ID, true, true, minKey, maxKey);
	memoryContext->resetToSavepoint(savePoint);

	// 8. ���� >= HOLE + 5 / <= HOLE + 5
	calcSpecifiedRange(session, m_tblDef, index, HOLE + 5, HOLE + 5, true, true, minKey, maxKey);
	memoryContext->resetToSavepoint(savePoint);

	// 9. ���� >= SAME_ID / < SAME_ID + 1
	calcSpecifiedRange(session, m_tblDef, index, SAME_ID, SAME_ID + 1, true, true, minKey, maxKey);
	memoryContext->resetToSavepoint(savePoint);

	// 10. ���� > SAME_ID + 1 / <= SAME_ID + 2
	calcSpecifiedRange(session, m_tblDef, index, SAME_ID + 1, SAME_ID + 2, false, true, minKey, maxKey);
	memoryContext->resetToSavepoint(savePoint);

	// 11. ���� > 2000 / < 8000
	calcSpecifiedRange(session, m_tblDef, index, 2000, 8000, false, false, minKey, maxKey);
	memoryContext->resetToSavepoint(savePoint);

	// 12. ���� > 3000 / < 12000
	calcSpecifiedRange(session, m_tblDef, index, 3000, 12000, false, false, minKey, maxKey);
	memoryContext->resetToSavepoint(savePoint);

	// 13. ���� < 0 / < 500
	calcSpecifiedRange(session, m_tblDef, index, 0, 500, false, false, minKey, maxKey);
	memoryContext->resetToSavepoint(savePoint);

	// 14. ���� > 100000 / < 110000
	calcSpecifiedRange(session, m_tblDef, index, 100000, 110000, false, false, minKey, maxKey);
	memoryContext->resetToSavepoint(savePoint);

	// 15. ���� > 0 / < 2000
	calcSpecifiedRange(session, m_tblDef, index, 0, 2000, false, false, minKey, maxKey);
	memoryContext->resetToSavepoint(savePoint);

	// 16. ���� > 8000 / < 100000
	calcSpecifiedRange(session, m_tblDef, index, 8000, 100000, false, false, minKey, maxKey);
	memoryContext->resetToSavepoint(savePoint);

	index = m_index->getIndex(0);
	indexDef = m_tblDef->m_indice[0];
	// 17. ���Կ��Խ���C�Ƚϵ������Ĺ���
	{
		checkIndex(session, memoryContext, totalKeys);

		SubRecordBuilder keyBuilder(m_tblDef, KEY_PAD);
		SubRecord *findKey;
		u64 key1 = 8000;
		u64 key2 = 100000000;
		u64 zero = 0;
		minKey->m_numCols = maxKey->m_numCols = 2;
		u16 cols0 = 0, cols2 = 2;
		minKey->m_columns[0] = cols0;
		maxKey->m_columns[0] = cols0;
		minKey->m_columns[1] = cols2;
		maxKey->m_columns[1] = cols2;

		findKey = keyBuilder.createSubRecordById("0 2", &key1, &zero);
		minKey->m_size = findKey->m_size;
		memcpy(minKey->m_data, findKey->m_data, findKey->m_size);
		freeSubRecord(findKey);
		findKey = keyBuilder.createSubRecordById("0 2", &key2, &zero);
		maxKey->m_size = findKey->m_size;
		memcpy(maxKey->m_data, findKey->m_data, findKey->m_size);
		freeSubRecord(findKey);
		int rows1 = (int)index->recordsInRange(session, minKey, true, maxKey, true);
		int realRows = getRangeRecords(session, index, minKey, true) - getRangeRecords(session, index, maxKey, false);
		cout << key1 << "	" << key2 << ": Estimate: " << rows1 << "	Real: " << realRows << "	Diff: " << (realRows == 0 ? 1.0 : (abs(realRows - rows1)) * 1.0 / realRows) << endl;
		m_tblDef->m_columns[2]->m_nullable = true;
	}

	m_index->close(m_session, true);
	delete m_index;
	DrsIndice::drop(m_path);
	m_index = NULL;
	closeSmallHeap(m_heap);
	DrsHeap::drop(HEAP_NAME);
	m_heap = NULL;
	delete m_tblDef;
	m_tblDef = NULL;
	TableDef::drop(TBLDEF_NAME);

	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(conn);
	}

	/***************************************************************************************/

	{
	// �ٲ���С�����ķ�Χ��ֵ��һ��ҳ�棬������ֵ��ֵ
	// ������������
	m_tblDef = createSmallTableDefWithSame();
	m_heap = createSmallHeapWithSame(m_tblDef);
	try {
		createIndex(m_path, m_heap, m_tblDef);
	} catch (NtseException) {
		CPPUNIT_ASSERT(false);
	}

	Connection *conn = m_db->getConnection(false);
	Session *session = m_db->getSessionManager()->allocSession("IndexOperationTestCase::testRecordsInRange", conn);
	MemoryContext *memoryContext = session->getMemoryContext();
	DrsIndex *index = m_index->getIndex(1);
	IndexDef *indexDef = m_tblDef->m_indice[1];

	Record *insertRec;
	insertRec = insertSpecifiedRecord(session, memoryContext, 0);
	freeRecord(insertRec);
	insertRec = insertSpecifiedRecord(session, memoryContext, 0);
	freeRecord(insertRec);
	insertRec = insertSpecifiedRecord(session, memoryContext, 1);
	freeRecord(insertRec);
	insertRec = insertSpecifiedRecord(session, memoryContext, 1);
	freeRecord(insertRec);
	insertRec = insertSpecifiedRecord(session, memoryContext, 1);
	freeRecord(insertRec);
	insertRec = insertSpecifiedRecord(session, memoryContext, 1);
	freeRecord(insertRec);
	insertRec = insertSpecifiedRecord(session, memoryContext, 2);
	freeRecord(insertRec);
	insertRec = insertSpecifiedRecord(session, memoryContext, 5);
	freeRecord(insertRec);

	SubRecord *minKey, *maxKey;
	minKey = IndexKey::allocSubRecordRED(memoryContext, indexDef->m_maxKeySize);
	maxKey = IndexKey::allocSubRecordRED(memoryContext, indexDef->m_maxKeySize);
	minKey->m_format = maxKey->m_format = KEY_PAD;
	minKey->m_numCols = maxKey->m_numCols = 1;
	minKey->m_rowId = maxKey->m_rowId = INVALID_ROW_ID;
	u16 cols = 0;
	minKey->m_columns = &cols;
	maxKey->m_columns = &cols;
	int realRows;
	SubRecordBuilder keyBuilder(m_tblDef, KEY_PAD);
	// ��ʼ���Ը�����Χ
	u64 savePoint = memoryContext->setSavepoint();
	// 1.����NULL-NULL
	int rows1 = (int)index->recordsInRange(session, NULL, true, NULL, false);
	memoryContext->resetToSavepoint(savePoint);
	int rows2 = (int)index->recordsInRange(session, NULL, false, NULL, true);
	memoryContext->resetToSavepoint(savePoint);
	int rows3 = (int)index->recordsInRange(session, NULL, true, NULL, true);
	memoryContext->resetToSavepoint(savePoint);
	int rows4 = (int)index->recordsInRange(session, NULL, false, NULL, false);
	memoryContext->resetToSavepoint(savePoint);
	CPPUNIT_ASSERT(rows1 == rows2 && rows1 == rows3 && rows1 == rows4);

	realRows = getRangeRecords(session, index, NULL, true);
	cout << "NULL	NULL: " << "Estimate: " << rows1 << "	Real: " << realRows << "	Diff: " << (realRows == 0 ? 1.0 : (abs(realRows - rows1)) * 1.0 / realRows) << endl;
	memoryContext->resetToSavepoint(savePoint);

	// 3. ����>=0 / <=6
	calcSpecifiedRange(session, m_tblDef, index, 0, 6, true, true, minKey, maxKey);
	memoryContext->resetToSavepoint(savePoint);

	// 4. ����>0 / <6
	calcSpecifiedRange(session, m_tblDef, index, 0, 6, false, false, minKey, maxKey);
	memoryContext->resetToSavepoint(savePoint);

	// 5. ���� >= 1 / < 2
	calcSpecifiedRange(session, m_tblDef, index, 1, 2, true, false, minKey, maxKey);
	memoryContext->resetToSavepoint(savePoint);

	// 6. ���� >= 1 / <= 1
	calcSpecifiedRange(session, m_tblDef, index, 1, 1, true, true, minKey, maxKey);
	memoryContext->resetToSavepoint(savePoint);

	// 7. ���� >= 5 / <= 5
	calcSpecifiedRange(session, m_tblDef, index, 5, 5, true, true, minKey, maxKey);
	memoryContext->resetToSavepoint(savePoint);

	// 8. ���� >= 3 / <= 4
	calcSpecifiedRange(session, m_tblDef, index, 3, 4, true, true, minKey, maxKey);
	memoryContext->resetToSavepoint(savePoint);

	// 9. ���� > 3 / < 4
	calcSpecifiedRange(session, m_tblDef, index, 3, 4, false, false, minKey, maxKey);
	memoryContext->resetToSavepoint(savePoint);

	// 10. ���� >= 3 / <= 3
	calcSpecifiedRange(session, m_tblDef, index, 3, 3, true, true, minKey, maxKey);
	memoryContext->resetToSavepoint(savePoint);

	// 11. ���� > 4 / <= 5
	calcSpecifiedRange(session, m_tblDef, index, 4, 5, false, true, minKey, maxKey);
	memoryContext->resetToSavepoint(savePoint);

	// 12. ���� >= 3 / <= 5
	calcSpecifiedRange(session, m_tblDef, index, 3, 5, true, true, minKey, maxKey);
	memoryContext->resetToSavepoint(savePoint);

	// 13. ���� >= 2 / <= 2
	calcSpecifiedRange(session, m_tblDef, index, 2, 2, true, true, minKey, maxKey);
	memoryContext->resetToSavepoint(savePoint);

	m_index->close(m_session, true);
	delete m_index;
	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(conn);
	DrsIndice::drop(m_path);
	m_index = NULL;
	closeSmallHeap(m_heap);
	DrsHeap::drop(HEAP_NAME);
	m_heap = NULL;
	delete m_tblDef;
	m_tblDef = NULL;
	TableDef::drop(TBLDEF_NAME);
	}

	/*******************************************************************************/
	// �ٲ��Կ������ķ�Χ��ֵ
	// ������������
	m_tblDef = createSmallTableDefWithSame();
	m_heap = createSmallHeapWithSame(m_tblDef);
	try {
		createIndex(m_path, m_heap, m_tblDef);
	} catch (NtseException) {
		CPPUNIT_ASSERT(false);
	}

	Connection *conn = m_db->getConnection(false);
	Session *session = m_db->getSessionManager()->allocSession("IndexOperationTestCase::testRecordsInRange", conn);
	MemoryContext *memoryContext = session->getMemoryContext();
	DrsIndex *index = m_index->getIndex(1);
	IndexDef *indexDef = m_tblDef->m_indice[1];

	SubRecord *minKey, *maxKey;
	minKey = IndexKey::allocSubRecordRED(memoryContext, indexDef->m_maxKeySize);
	maxKey = IndexKey::allocSubRecordRED(memoryContext, indexDef->m_maxKeySize);
	minKey->m_format = maxKey->m_format = KEY_PAD;
	minKey->m_numCols = maxKey->m_numCols = 1;
	u16 cols = 0;
	minKey->m_columns = &cols;
	maxKey->m_columns = &cols;
	int realRows;
	SubRecordBuilder keyBuilder(m_tblDef, KEY_PAD);
	// ��ʼ���Ը�����Χ
	u64 savePoint = memoryContext->setSavepoint();
	// 1.����NULL-NULL
	int rows1 = (int)index->recordsInRange(session, NULL, true, NULL, false);
	memoryContext->resetToSavepoint(savePoint);
	int rows2 = (int)index->recordsInRange(session, NULL, false, NULL, true);
	memoryContext->resetToSavepoint(savePoint);
	int rows3 = (int)index->recordsInRange(session, NULL, true, NULL, true);
	memoryContext->resetToSavepoint(savePoint);
	int rows4 = (int)index->recordsInRange(session, NULL, false, NULL, false);
	memoryContext->resetToSavepoint(savePoint);
	CPPUNIT_ASSERT(rows1 == rows2 && rows1 == rows3 && rows1 == rows4);

	realRows = getRangeRecords(session, index, NULL, true);
	cout << "NULL	NULL: " << "Estimate: " << rows1 << "	Real: " << realRows << "	Diff: " << (realRows == 0 ? 1.0 : (abs(realRows - rows1)) * 1.0 / realRows) << endl;
	memoryContext->resetToSavepoint(savePoint);

	m_index->close(m_session, true);
	delete m_index;
	DrsIndice::drop(m_path);
	m_index = NULL;
	closeSmallHeap(m_heap);
	DrsHeap::drop(HEAP_NAME);
	m_heap = NULL;
	delete m_tblDef;
	m_tblDef = NULL;
	TableDef::drop(TBLDEF_NAME);

	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(conn);
}

/**
 * ����ָ����Χ�ļ�ֵ��
 * @param session		�Ự���
 * @param tableDef		����
 * @param index			Ҫ�������������
 * @param key1			��Χ���޴�С
 * @param key2			��Χ���޴�С
 * @param includekey1	�Ƿ���>=��ѯ
 * @param includekey2	�Ƿ���<=��ѯ
 * @param minKey		��С��ֵ
 * @param maxKey		����ֵ
 */
void IndexOperationTestCase::calcSpecifiedRange(Session *session, TableDef *tableDef, DrsIndex *index, u64 key1, u64 key2, bool includekey1, bool includekey2, SubRecord *minKey, SubRecord *maxKey) {
	SubRecordBuilder keyBuilder(tableDef, KEY_PAD);
	SubRecord *findKey;
	findKey = keyBuilder.createSubRecordById("0", &key1);
	minKey->m_size = findKey->m_size;
	memcpy(minKey->m_data, findKey->m_data, findKey->m_size);
	freeSubRecord(findKey);
	findKey = keyBuilder.createSubRecordById("0", &key2);
	maxKey->m_size = findKey->m_size;
	memcpy(maxKey->m_data, findKey->m_data, findKey->m_size);
	freeSubRecord(findKey);
	int rows1 = (int)index->recordsInRange(session, minKey, includekey1, maxKey, includekey2);
	int realRows = getRangeRecords(session, index, minKey, includekey1) - getRangeRecords(session, index, maxKey, !includekey2);
	cout << key1 << "	" << key2 << ": Estimate: " << rows1 << "	Real: " << realRows << "	Diff: " << (realRows == 0 ? 1.0 : (abs(realRows - rows1)) * 1.0 / realRows) << endl;
}


/**
 * ��������״̬��ȡ
 */
void IndexOperationTestCase::testGetStatus() {
	uint step = 1;
	uint testScale = DATA_SCALE / 10;

	// ������������
	m_tblDef = createSmallTableDefWithSame();
	m_heap = createSmallHeapWithSame(m_tblDef);
	uint totalKeys = buildHeapWithSame(m_heap, m_tblDef, 20, step);
	try {
		createIndex(m_path, m_heap, m_tblDef);
	} catch (NtseException) {
		CPPUNIT_ASSERT(false);
	}

	Connection *conn = m_db->getConnection(false);
	Session *session = m_db->getSessionManager()->allocSession("IndexOperationTestCase::testRecordsInRange", conn);
	MemoryContext *memoryContext = session->getMemoryContext();
	DrsIndex *index = m_index->getIndex(1);
	IndexDef *indexDef = m_tblDef->m_indice[1];

	const struct IndexStatus *status = &(index->getStatus());
	// ����ȷ��������С���Ϊ128��ҳ��
	CPPUNIT_ASSERT(status->m_dataLength <= 128 * Limits::PAGE_SIZE);
	CPPUNIT_ASSERT(status->m_dataLength >= status->m_freeLength);

	// ������ִ��һϵ�в�����Ȼ���ȡͳ����Ϣ
	// ����ִ�з�Χɨ�裬�������һ��
	rangeScanForwardCheck(session, m_tblDef, index, NULL, NULL, totalKeys);
	rangeScanBackwardCheck(session, m_tblDef, index, NULL, NULL, totalKeys);
	CPPUNIT_ASSERT(status->m_backwardScans == (isEssentialOnly() ? 0 : 1));
	CPPUNIT_ASSERT(status->m_rowsBScanned == (isEssentialOnly() ? 0 : totalKeys));
	CPPUNIT_ASSERT(status->m_dboStats->m_statArr[DBOBJ_SCAN] == (isEssentialOnly() ? 0 : 2));
	CPPUNIT_ASSERT(status->m_dboStats->m_statArr[DBOBJ_SCAN_ITEM] == (isEssentialOnly() ? 0 : totalKeys * 2));

	// ���Բ���
	uint ops = 0;
	for (int i = HOLE + 1; i < testScale + HOLE; i += step) {
		Record *temp = insertSpecifiedRecord(session, memoryContext, i);
		totalKeys++;
		ops++;
		freeRecord(temp);
	}

	// ��֤����ҳ�����
	status = &(index->getStatus());
	CPPUNIT_ASSERT(status->m_dataLength - status->m_freeLength > Limits::PAGE_SIZE);
	CPPUNIT_ASSERT(status->m_dboStats->m_statArr[DBOBJ_ITEM_INSERT] == ops);

	// ��Χɾ��
	Record *record = IndexKey::allocRecord(memoryContext, m_tblDef->m_maxRecSize);
	RowLockHandle *rlh;
	IndexScanHandle *indexHandle = index->beginScan(session, NULL, true, true, Exclusived, &rlh, NULL);
	int count = 0;
	while (index->getNext(indexHandle, NULL)) {
		// ȥ��ӦHeapȡ��¼
		RowId rowId = indexHandle->getRowId();
		record->m_format = REC_FIXLEN;
		m_heap->getRecord(m_session, rowId, record);
		record->m_format = REC_REDUNDANT;
		session->startTransaction(TXN_DELETE, m_tblDef->m_id);
		m_index->deleteIndexEntries(session, record, indexHandle);
		session->endTransaction(true);
		session->unlockRow(&rlh);
		count++;
	}
	index->endScan(indexHandle);
	CPPUNIT_ASSERT(count == totalKeys);
	CPPUNIT_ASSERT(status->m_dboStats->m_statArr[DBOBJ_ITEM_DELETE] == count);

	// �ٴη�Χɨ����Ϊ0
	rangeScanForwardCheck(session, m_tblDef, index, NULL, NULL, 0);
	rangeScanBackwardCheck(session, m_tblDef, index, NULL, NULL, 0);
	CPPUNIT_ASSERT(status->m_backwardScans == (isEssentialOnly() ? 0 : 2));
	CPPUNIT_ASSERT(status->m_rowsBScanned == (isEssentialOnly() ? 0 : count - ops));
	CPPUNIT_ASSERT(status->m_dboStats->m_statArr[DBOBJ_SCAN] == (isEssentialOnly() ? 1 : 5));
	CPPUNIT_ASSERT(status->m_dboStats->m_statArr[DBOBJ_SCAN_ITEM] == (isEssentialOnly() ? count : 3 * (count - ops) + ops));

	// ��ǰֻ��һ����ҳ����ʹ����
	status = &(index->getStatus());
	CPPUNIT_ASSERT(status->m_dataLength == status->m_freeLength + Limits::PAGE_SIZE);

	// ������������ͳ�ƽӿ�
	((DrsBPTreeIndex*)index)->statisticDL();

	// ���Ի�ȡDBͳ����Ϣ�ӿ�
	index->getDBObjStats();

	m_index->close(m_session, true);
	delete m_index;
	DrsIndice::drop(m_path);
	m_index = NULL;
	closeSmallHeap(m_heap);
	DrsHeap::drop(HEAP_NAME);
	m_heap = NULL;
	delete m_tblDef;
	m_tblDef = NULL;
	TableDef::drop(TBLDEF_NAME);

	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(conn);
}


/**
 * ����deleteIndexEntry��LSN�жϲ����ʱ�������
 * ��Ҫ���������߳��������̣�һ��ɾ���̣߳�һ�������߳�
 * ɾ���߳�����������Χɾ��������������ִ��ɾ��֮ǰ������ĳ��ͬ����
 * ��ʱ���������̣߳��޸�ɾ���߳�Ҫɾ����������ҳ������ݣ���֤��ҳ��LSN�ı䣬ͬʱ���������߳�
 * ��ʱ����ɾ���̣߳�����ִ��ɾ�����ᵼ����Ҫ�ض�λ��Ҳ�͸������ض�λ�����
 */
void IndexOperationTestCase::testMTDeleteRelocate() {
	{	// ���Է�Χɾ��ɨ���ɾ��ʱ���ܳ�����Ҫ�ض�λ�����
		// ������������
		uint totalKeys;
		m_tblDef = createSmallTableDef();
		m_heap = createSmallHeap(m_tblDef);
		totalKeys = buildHeap(m_heap, m_tblDef, 100, 1);
		createIndex(m_path, m_heap, m_tblDef);

		Connection *conn = m_db->getConnection(false);
		Session *session = m_db->getSessionManager()->allocSession("IndexOperationTestCase::testUpdate", conn);
		MemoryContext *memoryContext = session->getMemoryContext();

		TroubleMaker trouble("I'm a trouble maker", m_db, conn, m_heap, m_tblDef, m_index, HOLE + 3, rowids[3]);
		SimpleMan simpleman("I'm a simple man", m_db, conn, m_heap, m_tblDef, m_index, HOLE, true, INVALID_ROW_ID);

		simpleman.enableSyncPoint(SP_IDX_CHECK_BEFORE_DELETE);
		trouble.enableSyncPoint(SP_IDX_FINISH_A_DELETE);

		// ����������Χɾ���̣߳�������ִ��ɾ��֮ǰ�ȴ�
		simpleman.start();
		simpleman.joinSyncPoint(SP_IDX_CHECK_BEFORE_DELETE);

		// ���������̣߳���ǰ�Ķ���Χɾ���߳�Ҫ����ҳ��
		trouble.enableSyncPoint(SP_IDX_FINISH_A_DELETE);
		trouble.start();
		trouble.joinSyncPoint(SP_IDX_FINISH_A_DELETE);
		// ������ɾ���̼߳�������ʱ���̱߳����ض�λɾ����¼
		trouble.disableSyncPoint(SP_IDX_FINISH_A_DELETE);
		trouble.notifySyncPoint(SP_IDX_FINISH_A_DELETE);

		simpleman.disableSyncPoint(SP_IDX_CHECK_BEFORE_DELETE);
		simpleman.notifySyncPoint(SP_IDX_CHECK_BEFORE_DELETE);

		trouble.join();
		simpleman.join();

		// ��֤���
		{	// ɾ���ļ�ֵ������
			DrsIndex *index = m_index->getIndex(1);
			SubRecord *findKey = makeFindKeyWhole(HOLE, m_heap, m_tblDef);
			findKey->m_rowId = INVALID_ROW_ID;
			RowId rowId;
			bool found = index->getByUniqueKey(session, findKey, None, &rowId, NULL, NULL, NULL);
			CPPUNIT_ASSERT(!found);
			freeSubRecord(findKey);

			findKey = makeFindKeyWhole(HOLE + 1, m_heap, m_tblDef);
			findKey->m_rowId = INVALID_ROW_ID;
			found = index->getByUniqueKey(session, findKey, None, &rowId, NULL, NULL, NULL);
			CPPUNIT_ASSERT(!found);
			freeSubRecord(findKey);

			findKey = makeFindKeyWhole(HOLE + 3, m_heap, m_tblDef);
			findKey->m_rowId = INVALID_ROW_ID;
			found = index->getByUniqueKey(session, findKey, None, &rowId, NULL, NULL, NULL);
			CPPUNIT_ASSERT(!found);
			freeSubRecord(findKey);

			// û�б�ɾ���ļ�ֵӦ�ô���
			findKey = makeFindKeyWhole(HOLE + 2, m_heap, m_tblDef);
			findKey->m_rowId = INVALID_ROW_ID;
			found = index->getByUniqueKey(session, findKey, None, &rowId, NULL, NULL, NULL);
			CPPUNIT_ASSERT(found);
			freeSubRecord(findKey);

			// �����ṹ����
			checkIndex(session, memoryContext, totalKeys);
		}

		m_index->close(m_session, true);
		delete m_index;
		DrsIndice::drop(m_path);
		m_index = NULL;
		closeSmallHeap(m_heap);
		DrsHeap::drop(HEAP_NAME);
		m_heap = NULL;
		delete m_tblDef;
		m_tblDef = NULL;
		TableDef::drop(TBLDEF_NAME);

		m_db->getSessionManager()->freeSession(session);
		m_db->freeConnection(conn);
	}
}


/**
 * �ò�����������deleteSMO������Ψһ��һ�ο��ܳ��ָ����ı���Ҫ�ض�λ�����
 * �ù�����Ҫ�����̣߳�һ��ɾ���̣߳�һ�������̣߳�����ѡ������߳����������
 * ɾ���߳�����ɾ�����»���ɾ��SMO����������SMOִ��֮ǰ������ĳ��ͬ����
 * ���������߳�����Χ���룬����ɾ���̴߳���SMO��ҳ��ĸ�ҳ��ᱻ���ѣ����������߳�
 * ����ɾ���̼߳���SMO���������ʱ�����ڸ���㱻�ı䣬��Ҫ���¶�λ����㣬������ش����
 * ע��totalScale��������ʼ��ģ��С������Ҫ��֤��2��ҳ�棬�Ŵ���ɾ���ϲ��Ŀ��ܣ�ͬʱ���������ȷ��λ�ý���
 *		preDeleteNum���ƽ���һ��Ҷҳ��ɾ����ֻʣ��һ�������Ϊֹ�����ᴥ��SMO
 *		insertNo	Ӧ��Ҫ��֤�����ڵڶ���Ҷҳ����ߺ�����Ҷҳ��
 *		insertNum	��֤��������ᴥ�����������SMO
 */
void IndexOperationTestCase::testMTDeleteSMORelocate() {
	{
		u16 totalScale = 100;
		u16 preDeleteNum = 11;
		u16 insertNo = 30;
		u16 insertNum = 15;

#if NTSE_PAGE_SIZE == 8192
		totalScale = 300;
		preDeleteNum = 115;
		insertNo = 301;
		insertNum = 120;
#endif

		// ������������
		uint totalKeys;
		m_tblDef = createSmallTableDef();
		m_heap = createSmallHeap(m_tblDef);
		totalKeys = buildHeap(m_heap, m_tblDef, totalScale, 1);
		createIndex(m_path, m_heap, m_tblDef);

		Connection *conn = m_db->getConnection(false);
		Session *session = m_db->getSessionManager()->allocSession("IndexOperationTestCase::testUpdate", conn);
		MemoryContext *memoryContext = session->getMemoryContext();

		// �Ƚ�������һ��ҳ��ɾ����ֻʣһ��ټ���ɾ���ᴥ��SMO����
		for (uint i = HOLE; i < HOLE + preDeleteNum; i++) {
			uint key = i;
			char name[50];
			sprintf(name, "Kenneth Tse Jr. %d \0", key);
			Record *record = createRecordRedundant(m_tblDef, 0, key, name, key + ((u64)key << 32), (u32)(-1) - key);
			record->m_rowId = rowids[key - HOLE];
			session->startTransaction(TXN_DELETE, m_tblDef->m_id);
			EXCPT_OPER(m_index->deleteIndexEntries(session, record, NULL));
			session->endTransaction(true);
			totalKeys--;

			freeRecord(record);
		}

		// �����̣߳�simpleman�߳̽��з�Χɾ������ʹ��һ���������߳���simpleman����SMO֮ǰ��������ɸ����ı�
		SimpleMan simpleman("I'm a simple man", m_db, conn, m_heap, m_tblDef, m_index, HOLE + preDeleteNum, true, INVALID_ROW_ID);
		Bee bee("I'm a hardworker", m_db, conn, m_heap, m_tblDef, m_index, HOLE + insertNo, insertNum, INVALID_ROW_ID);

		simpleman.enableSyncPoint(SP_IDX_CHECK_BEFORE_DELETE);

		// ����������Χɾ���̣߳�������ִ��ɾ��֮ǰ�ȴ�
		simpleman.start();
		simpleman.joinSyncPoint(SP_IDX_CHECK_BEFORE_DELETE);

		// �۷��߳̿�ʼ����һ�������������������
		bee.start();
		bee.join();
		totalKeys += insertNum;

		simpleman.disableSyncPoint(SP_IDX_CHECK_BEFORE_DELETE);
		simpleman.notifySyncPoint(SP_IDX_CHECK_BEFORE_DELETE);

		simpleman.join();
		totalKeys -= 2;

		// ��֤�����ȷ��
		checkIndex(session, memoryContext, totalKeys);
		DrsIndex *index = m_index->getIndex(1);
		rangeScanForwardCheck(session, m_tblDef, index, NULL, NULL, totalKeys);
		rangeScanBackwardCheck(session, m_tblDef, index, NULL, NULL, totalKeys);

		m_index->close(m_session, true);
		delete m_index;
		DrsIndice::drop(m_path);
		m_index = NULL;
		closeSmallHeap(m_heap);
		DrsHeap::drop(HEAP_NAME);
		m_heap = NULL;
		delete m_tblDef;
		m_tblDef = NULL;
		TableDef::drop(TBLDEF_NAME);

		m_db->getSessionManager()->freeSession(session);
		m_db->freeConnection(conn);
	}
}


/**
 * ���Զ�д����֮��ĳ�ͻ���������¼�����
 * case1. ��Χ��getNextRecord��ҳ���ͷ�ж�LSN��һ��
 *			�ù�����Ҫ����־Ը�ߣ�����Ҫһ��ɾ��־Ը�߰�æɾ����һ��ҳ����������ں�������
 *			һ��־Ը�߸�������Χ��ѯ��������Ҫ��ҳ���ʱ��Ⱥ���ͬ������
 *			��ʱ������һ��־Ը���߳̽��в��룬�ò��������ڿ�ҳ��ĵ�һ��ҳ������һ�Ȼ������߳�
 *			������Χ��ѯ�̣߳��ᷢ�ֵ�һ��ҳ�汻�޸ģ���Ҫ���¶�λ�����Ҵ�ʱ���Զ�λ��ǰһ���̲߳����������֤��ȡ������ȷ
 * 2. ɨ������SMO����ҳ��ȴ�SMO_Bit��λ
 *			�ù�����Ҫ����־Ը�߹�ͬ���
 *			����һ������־Ը�߻����Ĳ���������ݣ��������������з��ѣ������ڷ��ѽ�����û�лָ�SMOλ֮ǰ�����̵߳���ͬ������
 *			��ʱ����һ���������̣߳���ȡ���뷶Χ�ڵ����ݣ��ᷢ����������ִ��SMO�����������ĳ��SMOҳ��
 *			��һ��ʱ��֮�����ִ�в����߳�ʹ�������ִ�н�����Ȼ������������̲߳���
 *			����ܹ���֤�����ݲ����ܹ���ȷ�������ݣ�ͬʱ�ȴ�SMO��������Ҳ�����ǲ���
 * 3. ͬ2���������ڳ�ʼ�������Ĵ�С����ɵĲ�ͬ��SMO�����Ƿ�ᵼ�������������з���
 * 4. ɨ�����޸Ĳ�������latch��ͻ
 *			�ù�����Ҫ�����߳�
 *			���ȹ���һ����С�͹�ģ�����������������������
 *			����������Χɨ���̣߳���ȡ����ҳ���ͷ�ҳ������ʱ�̣��Ⱥ���һ��ͬ������
 *			��ʱ���������������̣߳��ֱ���ɨ���߳��ͷŵ�ҳ���Ҫ������ҳ��ֱ�������ݣ���������
 *			��������ɨ���̣߳��ᾭ��������Ҫ�����ж�LSN�����
 * 5. ���Է�Χɨ�����������ĳ��ҳ��LSN�ı�
 *			���Ƚ���һ��С������ֻ��Ҫһ��ҳ��
 *			��ʼһ����Χɨ�裬�ӵ�һ�ʼɨ�裬ɨ��һ��֮��Ⱥ���һ��ͬ����
 *			���������̶߳Ը�ҳ��ִ�в��룬�ı�ҳ��LSN���������߳�
 *			������Χɨ�裬��ʱ��Ҫ�ж��ض�λ
 *			������һ������������̣߳�ɾ��������������
 *			������Χɨ�裬��ʱ�ж�������Ϊ��
 */
void IndexOperationTestCase::testMTRWConflict() {
	vs.buf = true;
	{	// ��Χ��getNextRecord��ҳ���ж�LSN��һ��
		// ������������
		u16 totalScale;
		u16 deleteNo;
		u16 insertNo;

		{
			totalScale = 100;
			deleteNo = 24;
			insertNo = 23;
			#if NTSE_PAGE_SIZE == 8192
				totalScale = 500;
				deleteNo = 232;
				insertNo = 231;
			#endif
		}

		uint totalKeys;
		m_tblDef = createSmallTableDef();
		m_heap = createSmallHeap(m_tblDef);
		totalKeys = buildHeap(m_heap, m_tblDef, totalScale, 2);
		createIndex(m_path, m_heap, m_tblDef);

		Connection *conn = m_db->getConnection(false);

		// �������߳�ɾ��������һ����ҳ������һ��
		SimpleMan simpleman("I'm a simpleman", m_db, conn, m_heap, m_tblDef, m_index, HOLE + deleteNo, false, rowids[deleteNo]);
		simpleman.start();
		simpleman.join();
		totalKeys--;

		Bee bee("I'm a hardworker", m_db, conn, m_heap, m_tblDef, m_index, HOLE + insertNo, 1, INVALID_ROW_ID);	// ����Ϊ�˱�֤�����ڵ�һ��ҳ���ĩβ
		ReadingLover readinglover("I'm a reading fun", m_db, conn, m_heap, m_tblDef, m_index, HOLE, true, false, NULL, INVALID_ROW_ID);

		// �ö��߳��������ҵȴ��ڿ�ҳ��״̬
		readinglover.enableSyncPoint(SP_IDX_WANT_TO_GET_PAGE1);
		readinglover.start();
		readinglover.joinSyncPoint(SP_IDX_WANT_TO_GET_PAGE1);

		bee.start();
		bee.join();
		totalKeys++;

		readinglover.disableSyncPoint(SP_IDX_WANT_TO_GET_PAGE1);
		readinglover.notifySyncPoint(SP_IDX_WANT_TO_GET_PAGE1);
		readinglover.join();

		// ������
		uint gotCount = readinglover.getReadNum();
		CPPUNIT_ASSERT(gotCount == totalKeys);

		m_index->close(m_session, true);
		delete m_index;
		DrsIndice::drop(m_path);
		m_index = NULL;
		closeSmallHeap(m_heap);
		DrsHeap::drop(HEAP_NAME);
		m_heap = NULL;
		delete m_tblDef;
		m_tblDef = NULL;
		TableDef::drop(TBLDEF_NAME);

		m_db->freeConnection(conn);
	}

	{	// ����SMO�ȴ�����
		// ����С���������߳������������ݵ����������ѣ�ͬʱ�������̶߳����ݣ���ɵȴ�SMO_Bit���ͷŵ����

		u16 totalScale;

		{
			totalScale = 20;
			#if NTSE_PAGE_SIZE == 8192
				totalScale = 300;
			#endif
		}


		uint totalKeys;
		m_tblDef = createSmallTableDef();
		m_heap = createSmallHeap(m_tblDef);
		totalKeys = buildHeap(m_heap, m_tblDef, totalScale, 2);
		createIndex(m_path, m_heap, m_tblDef);

		Connection *conn = m_db->getConnection(false);

		Bee bee("I'm a laborer", m_db, conn, m_heap, m_tblDef, m_index, HOLE + 1, 30, INVALID_ROW_ID);
		ReadingLover readinglover("I like reading", m_db, conn, m_heap, m_tblDef, m_index, HOLE + 2, false, false, NULL, INVALID_ROW_ID);

		// ��ʼ���뵼��SMO���ȴ���Ҫ���SMO_Bit֮ǰ��ʱ��
		bee.enableSyncPoint(SP_IDX_BEFORE_CLEAR_SMO_BIT);
		bee.start();
		bee.joinSyncPoint(SP_IDX_BEFORE_CLEAR_SMO_BIT);

		// ���߳̿�ʼ���ң��ض������SMO_Bit���ж���
		readinglover.enableSyncPoint(SP_IDX_WAIT_FOR_SMO_BIT);
		readinglover.start();
		readinglover.joinSyncPoint(SP_IDX_WAIT_FOR_SMO_BIT);

		// SMO_Bit�ȴ���ʼ����������̼߳���ִ�����
		bee.disableSyncPoint(SP_IDX_BEFORE_CLEAR_SMO_BIT);
		bee.notifySyncPoint(SP_IDX_BEFORE_CLEAR_SMO_BIT);

		// ��ʱ��Ҫ�ض�λ��������ȷ���ܶ������
		readinglover.disableSyncPoint(SP_IDX_WAIT_FOR_SMO_BIT);
		readinglover.notifySyncPoint(SP_IDX_WAIT_FOR_SMO_BIT);

		readinglover.join();
		bee.join();

		CPPUNIT_ASSERT(readinglover.getReadNum() == 1);

		m_index->close(m_session, true);
		delete m_index;
		DrsIndice::drop(m_path);
		m_index = NULL;
		closeSmallHeap(m_heap);
		DrsHeap::drop(HEAP_NAME);
		m_heap = NULL;
		delete m_tblDef;
		m_tblDef = NULL;
		TableDef::drop(TBLDEF_NAME);

		m_db->freeConnection(conn);
	}

	if (!isEssentialOnly())
	{	// ����SMO�ȴ�����
		// ���������������߳������������ݵ����������ѣ�ͬʱ�������̶߳����ݣ���ɵȴ�SMO_Bit���ͷŵ����
		// ��������ǰһ�������Ĳ�ͬ��ǰ���С����SMO�ᵼ�¸�ҳ����ѣ������������֤��ҳ�治�����
		// ����ģ���޸Ĳ�����SMO��������ֱ�Ӷ�ɨ���߳�ʹ�õ�Ҷҳ������SMOλ�ķ�������������������

		uint totalScale;

		{
			totalScale = 5000;
#if NTSE_PAGE_SIZE == 8192
			totalScale = 200000;
#endif
		}

		uint totalKeys;
		m_tblDef = createSmallTableDef();
		m_heap = createSmallHeap(m_tblDef);
		totalKeys = buildHeap(m_heap, m_tblDef, totalScale, 2);
		createIndex(m_path, m_heap, m_tblDef);

		Connection *conn = m_db->getConnection(false);
		Session *session = m_db->getSessionManager()->allocSession("IndexOperationTestCase::testMTRWConflict", conn);
		MemoryContext *memoryContext = session->getMemoryContext();

		ReadingLover readinglover("I like reading", m_db, conn, m_heap, m_tblDef, m_index, HOLE + 2, false, false, NULL, INVALID_ROW_ID);

		// ����һ����Χɨ�裬��ȡ������ҳ��ĵ�һ��ҳ�棬������Ҫ������SMOλ
		DrsIndex *index = m_index->getIndex(1);
		IndexScanHandle *scanHandle = index->beginScan(session, NULL, true, true, None, NULL, NULL);
		PageId pageId ;
		while (index->getNext(scanHandle, NULL)) {
			DrsIndexRangeScanHandle *rangeHandle = (DrsIndexRangeScanHandle*)scanHandle;
			pageId = rangeHandle->getScanInfo()->m_pageId;	// ��ҳ���ʱ��pinס
			break;
		}
		index->endScan(scanHandle);

		// ��������SMOλ
		PageHandle *handle = GET_PAGE(session, ((DrsBPTreeIndice*)m_index)->getFileDesc(), PAGE_INDEX, pageId, Exclusived, m_index->getDBObjStats(), NULL);
		IndexPage *page = (IndexPage*)handle->getPage();
		page->setSMOBit(session, pageId, ((DrsBPTreeIndice*)m_index)->getLogger());
		session->markDirty(handle);
		session->unlockPage(&handle);

		// ���߳̿�ʼ���ң��ض������SMO_Bit���ж���
		readinglover.enableSyncPoint(SP_IDX_WAIT_FOR_SMO_BIT);
		readinglover.start();
		readinglover.joinSyncPoint(SP_IDX_WAIT_FOR_SMO_BIT);

		// SMO_Bit�ȴ���ʼ�����SMOλ
		LOCK_PAGE_HANDLE(session, handle, Exclusived);
		page = (IndexPage*)handle->getPage();
		page->clearSMOBit(session, pageId, ((DrsBPTreeIndice*)m_index)->getLogger());
		session->markDirty(handle);
		session->releasePage(&handle);

		// ��ʱ��Ҫ�ض�λ��������ȷ���ܶ������
		readinglover.disableSyncPoint(SP_IDX_WAIT_FOR_SMO_BIT);
		readinglover.notifySyncPoint(SP_IDX_WAIT_FOR_SMO_BIT);

		readinglover.join();

		CPPUNIT_ASSERT(readinglover.getReadNum() == 1);

		m_index->close(m_session, true);
		delete m_index;
		DrsIndice::drop(m_path);
		m_index = NULL;
		closeSmallHeap(m_heap);
		DrsHeap::drop(HEAP_NAME);
		m_heap = NULL;
		delete m_tblDef;
		m_tblDef = NULL;
		TableDef::drop(TBLDEF_NAME);

		m_db->getSessionManager()->freeSession(session);
		m_db->freeConnection(conn);
	}

	u16 totalScale;

	{
		totalScale = 30;
#if NTSE_PAGE_SIZE == 8192
		totalScale = 300;
#endif
	}

	{	// ����latchHoldingLatch�Ӳ�����LSN�жϲ��ȵ����
		uint totalKeys;
		m_tblDef = createSmallTableDef();
		m_heap = createSmallHeap(m_tblDef);
		totalKeys = buildHeap(m_heap, m_tblDef, totalScale, 2);
		createIndex(m_path, m_heap, m_tblDef);

		Connection *conn = m_db->getConnection(false);

		// ��ʱ������Ӧ������������ҳ�棬������ʾ��
		//
		//						|---------|Page1 pageId132
		//						|---------|
		//						/		\
		//	Page2 pageId130|-------|	|--------|Page3 pageId131
		//				   |-------|	|--------|
		// ��ʱ��Ҫ����Χɨ���page3��ҳ�浽page2���ڿ�ҳ��֮ǰpage2���ڱ��޸��̲߳�����
		// ɨ���߳�Ҫ�����ټ����������¼�ԭҳ������ʱ��ԭҳ������Ҫ����һ���޸��߳��޸�
		// ����LSN�ı�����

		Bee bee1("I'm a hardworker", m_db, conn, m_heap, m_tblDef, m_index, HOLE + 1, 1, INVALID_ROW_ID);
		Bee bee2("I'm also a hardworker", m_db, conn, m_heap, m_tblDef, m_index, HOLE + totalScale, 1, INVALID_ROW_ID);
		ReadingLover readinglover("I like reading", m_db, conn, m_heap, m_tblDef, m_index, HOLE, true, false, NULL, INVALID_ROW_ID);

		readinglover.enableSyncPoint(SP_IDX_WANT_TO_GET_PAGE2);
		readinglover.start();
		readinglover.joinSyncPoint(SP_IDX_WANT_TO_GET_PAGE2);
		readinglover.notifySyncPoint(SP_IDX_WANT_TO_GET_PAGE2);	// �Թ���λҶ�����ֵ�һ�εȴ�
		readinglover.joinSyncPoint(SP_IDX_WANT_TO_GET_PAGE2);

		bee1.enableSyncPoint(SP_IDX_WAIT_FOR_INSERT);
		bee1.start();
		bee1.joinSyncPoint(SP_IDX_WAIT_FOR_INSERT);

		readinglover.disableSyncPoint(SP_IDX_WANT_TO_GET_PAGE2);
		readinglover.enableSyncPoint(SP_IDX_WANT_TO_GET_PAGE3);
		readinglover.enableSyncPoint(SP_IDX_WANT_TO_GET_PAGE4);
		readinglover.notifySyncPoint(SP_IDX_WANT_TO_GET_PAGE2);
		readinglover.joinSyncPoint(SP_IDX_WANT_TO_GET_PAGE3);

		bee1.disableSyncPoint(SP_IDX_WAIT_FOR_INSERT);
		bee1.notifySyncPoint(SP_IDX_WAIT_FOR_INSERT);
		bee1.join();
		totalKeys++;

		readinglover.disableSyncPoint(SP_IDX_WANT_TO_GET_PAGE3);
		readinglover.notifySyncPoint(SP_IDX_WANT_TO_GET_PAGE3);
		readinglover.joinSyncPoint(SP_IDX_WANT_TO_GET_PAGE4);

		bee2.start();
		bee2.join();
		totalKeys++;

		readinglover.disableSyncPoint(SP_IDX_WANT_TO_GET_PAGE4);
		readinglover.notifySyncPoint(SP_IDX_WANT_TO_GET_PAGE4);
		readinglover.join();

		CPPUNIT_ASSERT(readinglover.getReadNum() == totalKeys - 1);

		m_index->close(m_session, true);
		delete m_index;
		DrsIndice::drop(m_path);
		m_index = NULL;
		closeSmallHeap(m_heap);
		DrsHeap::drop(HEAP_NAME);
		m_heap = NULL;
		delete m_tblDef;
		m_tblDef = NULL;
		TableDef::drop(TBLDEF_NAME);

		m_db->freeConnection(conn);
	}

	{	// ����latchHoldingLatch�Ӳ�����LSN�ж���ȵ����
		uint totalKeys;
		m_tblDef = createSmallTableDef();
		m_heap = createSmallHeap(m_tblDef);
		totalKeys = buildHeap(m_heap, m_tblDef, totalScale, 2);
		createIndex(m_path, m_heap, m_tblDef);

		Connection *conn = m_db->getConnection(false);

		// ��ʱ������Ӧ������������ҳ�棬������ʾ��
		//
		//						|---------|Page1 pageId132
		//						|---------|
		//						/		\
		//	Page2 pageId130|-------|	|--------|Page3 pageId131
		//				   |-------|	|--------|
		//	����pageId3>pageId2>pageId1
		// ��ʱ��Ҫ����Χɨ���page3��ҳ�浽page2���ڿ�ҳ��֮ǰpage2���ڱ��޸��̲߳�����
		// ɨ���߳�Ҫ�����ټ�������ҳ��֮ǰ���е�ԭҳ������û�иĶ�������LSN��������

		Bee bee1("I'm a hardworker", m_db, conn, m_heap, m_tblDef, m_index, HOLE + 1, 1, INVALID_ROW_ID);
		Bee bee2("I'm also a hardworker", m_db, conn, m_heap, m_tblDef, m_index, HOLE + totalScale, 1, INVALID_ROW_ID);
		ReadingLover readinglover("I like reading", m_db, conn, m_heap, m_tblDef, m_index, HOLE, true, false, NULL, INVALID_ROW_ID);

		readinglover.enableSyncPoint(SP_IDX_WANT_TO_GET_PAGE2);
		readinglover.start();
		readinglover.joinSyncPoint(SP_IDX_WANT_TO_GET_PAGE2);
		readinglover.notifySyncPoint(SP_IDX_WANT_TO_GET_PAGE2);	// �Թ���λҶ�����ֵ�һ�εȴ�
		readinglover.joinSyncPoint(SP_IDX_WANT_TO_GET_PAGE2);

		bee1.enableSyncPoint(SP_IDX_WAIT_FOR_INSERT);
		bee1.start();
		bee1.joinSyncPoint(SP_IDX_WAIT_FOR_INSERT);

		readinglover.disableSyncPoint(SP_IDX_WANT_TO_GET_PAGE2);
		readinglover.enableSyncPoint(SP_IDX_WANT_TO_GET_PAGE3);
		readinglover.notifySyncPoint(SP_IDX_WANT_TO_GET_PAGE2);
		readinglover.joinSyncPoint(SP_IDX_WANT_TO_GET_PAGE3);

		bee1.disableSyncPoint(SP_IDX_WAIT_FOR_INSERT);
		bee1.notifySyncPoint(SP_IDX_WAIT_FOR_INSERT);
		bee1.join();
		totalKeys++;

		readinglover.disableSyncPoint(SP_IDX_WANT_TO_GET_PAGE3);
		readinglover.notifySyncPoint(SP_IDX_WANT_TO_GET_PAGE3);
		readinglover.join();

		CPPUNIT_ASSERT(readinglover.getReadNum() == totalKeys - 1);

		m_index->close(m_session, true);
		delete m_index;
		DrsIndice::drop(m_path);
		m_index = NULL;
		closeSmallHeap(m_heap);
		DrsHeap::drop(HEAP_NAME);
		m_heap = NULL;
		delete m_tblDef;
		m_tblDef = NULL;
		TableDef::drop(TBLDEF_NAME);

		m_db->freeConnection(conn);
	}

	// case 5
	// ���Է�Χɨ��ȡ��һ����ض�λ
	{
		m_tblDef = createSmallTableDef();
		m_heap = createSmallHeap(m_tblDef);
		buildHeap(m_heap, m_tblDef, 10, 2);
		createIndex(m_path, m_heap, m_tblDef);

		Connection *conn = m_db->getConnection(false);

		Bee bee2("I'm also a hardworker", m_db, conn, m_heap, m_tblDef, m_index, HOLE + 1, 0, INVALID_ROW_ID);
		ReadingLover readinglover("I like reading", m_db, conn, m_heap, m_tblDef, m_index, HOLE, true, false, NULL, INVALID_ROW_ID);
		SimpleMan simpleman("I'm a simpleman", m_db, conn, m_heap, m_tblDef, m_index, HOLE, true, rowids[0], true);

		// ��ʼ��Χɨ�裬ȡ��һ��֮��
		readinglover.enableSyncPoint(SP_IDX_WAIT_FOR_GET_NEXT);
		readinglover.start();
		readinglover.joinSyncPoint(SP_IDX_WAIT_FOR_GET_NEXT);
		readinglover.notifySyncPoint(SP_IDX_WAIT_FOR_GET_NEXT);
		readinglover.joinSyncPoint(SP_IDX_WAIT_FOR_GET_NEXT);

		// ִ��һ�β���
		bee2.start();
		bee2.join();

		// ��ʱȡ���Ҫ�жϣ�Ȼ�����������һ����
		readinglover.notifySyncPoint(SP_IDX_WAIT_FOR_GET_NEXT);
		readinglover.joinSyncPoint(SP_IDX_WAIT_FOR_GET_NEXT);

		// ɾ������
		simpleman.start();
		simpleman.join();

		// ���ж�һ�Σ���������
		readinglover.disableSyncPoint(SP_IDX_WAIT_FOR_GET_NEXT);
		readinglover.notifySyncPoint(SP_IDX_WAIT_FOR_GET_NEXT);
		readinglover.join();

		m_index->close(m_session, true);
		delete m_index;
		DrsIndice::drop(m_path);
		m_index = NULL;
		closeSmallHeap(m_heap);
		DrsHeap::drop(HEAP_NAME);
		m_heap = NULL;
		delete m_tblDef;
		m_tblDef = NULL;
		TableDef::drop(TBLDEF_NAME);

		m_db->freeConnection(conn);
	}
}



/**
 * ���Գ���latch��Դ�����Ӳ�����Ҫ��latch��Դ�����
 * @case1:	�ù�����Ҫ����־Ը�����
 *			��������һ��־Ը���̶߳�ȡ����ȷ��ָ�����ݴ��ڣ��ڳ����ⲿ���������м���
 *			���������Ķ������̣߳����̶߳�ȡ��ͬ���ݣ�ͬʱҲ��Ҫ��������ʱ��Ӳ��������ͷ�ҳ��Latch������ĳ��ͬ����
 *			����һ��ɾ���̣߳���ǰ��Ҫ��ѯ������ɾ����ִ�н�������
 *			����ǰ��Ĳ�ѯ�̣߳���ʱ�ټ����ɹ������Ƿ���ҳ�汻�ı䣬���²��ң�Ӧ���Ҳ�������
 */
void IndexOperationTestCase::testMTLockHoldingLatch() {
	uint totalKeys;
	uint step = 2;
	m_tblDef = createSmallTableDef();
	m_heap = createSmallHeap(m_tblDef);
	totalKeys = buildHeap(m_heap, m_tblDef, 30, step);
	createIndex(m_path, m_heap, m_tblDef);

	Connection *conn = m_db->getConnection(false);
	Session *session = m_db->getSessionManager()->allocSession("IndexOperationTestCase::testMTLockHoldingLatch", conn);
	Connection *conn1 = m_db->getConnection(false);
	Session *session1 = m_db->getSessionManager()->allocSession("IndexOperationTestCase::testMTLockHoldingLatch1", conn1);

	{	// ����ɨ��
		RowLockHandle *rlh = NULL;
		ReadingLover readinglover("I like reading", m_db, conn, m_heap, m_tblDef, m_index, HOLE, false, false, &rlh, INVALID_ROW_ID);
		ReadingLover readinglover1("I also like reading", m_db, conn, m_heap, m_tblDef, m_index, HOLE, false, false, NULL, INVALID_ROW_ID);
		SimpleMan simpleman("I like simple", m_db, conn, m_heap, m_tblDef, m_index, HOLE, false, rowids[0]);

		// �ȶ�ȡ����ȷ�ϴ���
		readinglover1.start();
		readinglover1.join();
		CPPUNIT_ASSERT(readinglover1.getReadNum() == 1);

		// ��֤���̼߳Ӳ�����
		RowLockHandle *lockhandle = LOCK_ROW(session1, m_tblDef->m_id, rowids[0], Exclusived);
		readinglover.enableSyncPoint(SP_IDX_WAIT_TO_LOCK);
		readinglover.start();
		readinglover.joinSyncPoint(SP_IDX_WAIT_TO_LOCK);

		// �ⲿ������ͬʱɾ��Ҫ���ҵ�����
		session1->unlockRow(&lockhandle);
		simpleman.start();
		simpleman.join();

		readinglover.disableSyncPoint(SP_IDX_WAIT_TO_LOCK);
		readinglover.notifySyncPoint(SP_IDX_WAIT_TO_LOCK);
		readinglover.join();

		CPPUNIT_ASSERT(readinglover.getReadNum() == 0);
		CPPUNIT_ASSERT(rlh == NULL);
	}


	//{	// ����ɨ��
	//	RowLockHandle *rlh = NULL, *rlh1 = NULL;
	//	ReadingLover readinglover("I like reading", m_db, conn, m_heap, m_index, HOLE + step * 2, true, true, &rlh, INVALID_ROW_ID, 1);
	//	ReadingLover readinglover1("I also like reading", m_db, conn, m_heap, m_index, HOLE + step * 2, false, true, &rlh1, INVALID_ROW_ID);
	//	SimpleMan simpleman("I like simple", m_db, conn, m_heap, m_index, HOLE + step * 2, false, rowids[step * 2]);

	//	// �ȶ�ȡ����ȷ�ϴ���
	//	readinglover1.start();
	//	readinglover1.join();
	//	CPPUNIT_ASSERT(readinglover1.getReadNum() == 1);

	//	// ��֤���̼߳Ӳ�����
	//	RowLockHandle *lockhandle = LOCK_ROW(session1, m_tblDef->m_id, rowids[step * 2], Exclusived);
	//	readinglover.enableSyncPoint(SP_IDX_WAIT_TO_LOCK);
	//	readinglover.start();
	//	readinglover.joinSyncPoint(SP_IDX_WAIT_TO_LOCK);

	//	// �ⲿ������ͬʱɾ��Ҫ���ҵ�����
	//	session1->unlockRow(&lockhandle);
	//	simpleman.start();
	//	simpleman.join();

	//	readinglover.disableSyncPoint(SP_IDX_WAIT_TO_LOCK);
	//	readinglover.notifySyncPoint(SP_IDX_WAIT_TO_LOCK);
	//	readinglover.join();

	//	CPPUNIT_ASSERT(readinglover.getReadNum() == 1);
	//	CPPUNIT_ASSERT(rlh == NULL);
	//}

	m_index->close(m_session, true);
	delete m_index;
	DrsIndice::drop(m_path);
	m_index = NULL;
	closeSmallHeap(m_heap);
	DrsHeap::drop(HEAP_NAME);
	m_heap = NULL;
	delete m_tblDef;
	m_tblDef = NULL;
	TableDef::drop(TBLDEF_NAME);

	m_db->getSessionManager()->freeSession(session);
	m_db->getSessionManager()->freeSession(session1);
	m_db->freeConnection(conn);
	m_db->freeConnection(conn1);
}


/**
 * ���Լ�����ҳ�����Ķ��̲߳���
 * @case1 ���Բ����ҳ�����Ĺ����У���ҳ����ʧ�ܣ�ͬʱҳ�汻��һ���߳��޸ģ������߳�һ��������ڸ�ҳ�浱��
 * @case2 ���Բ����ҳ�����Ĺ����У���ҳ����ʧ�ܣ�ͬʱҳ�汻��һ���߳��޸ģ�������SMO�������������������ټ����ĳ���
 * @case3 ����case2��ֻ������һ���߳�ִ��ɾ������
 */
void IndexOperationTestCase::testMTLockIdxObj() {
	uint totalKeys;
	uint step = 3;
	uint makeSMOSize = 30;
	m_tblDef = createSmallTableDef();
	m_heap = createSmallHeap(m_tblDef);
	// ����50%�ķ��ѷ�ʽ����֤������������Ч
	m_tblDef->m_indice[0]->m_splitFactor = 50;
	m_tblDef->m_indice[1]->m_splitFactor = 50;
	totalKeys = buildHeap(m_heap, m_tblDef, 690, step);
	createIndex(m_path, m_heap, m_tblDef);

	DrsIndex *index = m_index->getIndex(1);
	Connection *conn = m_db->getConnection(false);
	Session *session = m_db->getSessionManager()->allocSession("IndexOperationTestCase::testMTLockHoldingLatch", conn);
	Connection *conn1 = m_db->getConnection(false);
	Session *session1 = m_db->getSessionManager()->allocSession("IndexOperationTestCase::testMTLockHoldingLatch1", conn1);

	uint firstPageLastId = 345;

	/************************ case 1 *****************************/
	{
		// ���ȸ��߳��ȳ��Բ���231�Ȼ�����ҳ������
		Bee bee1("I'm also a hardworker, of course", m_db, conn, m_heap, m_tblDef, m_index, HOLE + firstPageLastId - 1, 0, INVALID_ROW_ID);
		u64 firstPageId = 257;
		u64 lockObjId = ((u64)m_tblDef->m_id << ((sizeof(u64) - sizeof(m_tblDef->m_id)) * 8)) | firstPageId;
		CPPUNIT_ASSERT(session->lockIdxObject(lockObjId));
		bee1.enableSyncPoint(SP_IDX_WAIT_FOR_PAGE_LOCK);
		bee1.start();
		bee1.joinSyncPoint(SP_IDX_WAIT_FOR_PAGE_LOCK);
		CPPUNIT_ASSERT(session->unlockIdxObject(lockObjId));

		// ִ��һ�����룬����257ҳ�淢���˸ı䣬��Ҫ�ض�λ������û��SMO����
		Bee bee2("I'm also a hardworker, surely", m_db, conn1, m_heap, m_tblDef, m_index, HOLE + 1, 0, INVALID_ROW_ID);
		bee2.start();
		bee2.join();

		// ���»��Ѳ����̣߳�����Ӧ�óɹ�
		bee1.disableSyncPoint(SP_IDX_WAIT_FOR_PAGE_LOCK);
		bee1.notifySyncPoint(SP_IDX_WAIT_FOR_PAGE_LOCK);
		bee1.join();

		totalKeys += 2;
	}

	/************************ case 2 *****************************/
	{
		// ��������һ���߳���257ҳ����в��������Ȼ�����ɵ���
		u64 firstPageId = 257;
		u64 lockObjId = ((u64)m_tblDef->m_id << ((sizeof(u64) - sizeof(m_tblDef->m_id)) * 8)) | firstPageId;
		CPPUNIT_ASSERT(session->lockIdxObject(lockObjId));
		Bee bee3("I'm also a hardworker, of course", m_db, conn, m_heap, m_tblDef, m_index, HOLE + firstPageLastId - 2, 0, INVALID_ROW_ID);
		bee3.enableSyncPoint(SP_IDX_WAIT_FOR_PAGE_LOCK);
		bee3.start();
		bee3.joinSyncPoint(SP_IDX_WAIT_FOR_PAGE_LOCK);
		CPPUNIT_ASSERT(session->unlockIdxObject(lockObjId));

		// ִ��һ���������룬����257ҳ��SMO����
		for (int i = HOLE + 2; i < makeSMOSize * step + HOLE; i += step) {
			// ���Ȳ����
			Record *temp = insertSpecifiedRecord(session1, session1->getMemoryContext(), i);
			totalKeys++;
			freeRecord(temp);
		}

		// �������Ѳ����̣߳�����ɹ���������Ҫ������
		bee3.disableSyncPoint(SP_IDX_WAIT_FOR_PAGE_LOCK);
		bee3.notifySyncPoint(SP_IDX_WAIT_FOR_PAGE_LOCK);
		bee3.join();

		totalKeys += 1;
	}

	/************************ case 3 *****************************/
	{
		// ��������һ���߳���258ҳ�����ɾ��������Ȼ�����ɵ���������case2
		u64 secondPageId = 258;
		u64 lockObjId = ((u64)m_tblDef->m_id << ((sizeof(u64) - sizeof(m_tblDef->m_id)) * 8)) | secondPageId;
		CPPUNIT_ASSERT(session->lockIdxObject(lockObjId));
		SimpleMan simpleman("I like simple", m_db, conn, m_heap, m_tblDef, m_index, HOLE + firstPageLastId * 2 - step, false, rowids[firstPageLastId * 2 - step]);
		simpleman.enableSyncPoint(SP_IDX_WAIT_FOR_PAGE_LOCK);
		simpleman.start();
		simpleman.joinSyncPoint(SP_IDX_WAIT_FOR_PAGE_LOCK);
		CPPUNIT_ASSERT(session->unlockIdxObject(lockObjId));

		// ִ��һ���������룬����258ҳ��SMO����
		for (int i = HOLE + firstPageLastId + step + 1; i < makeSMOSize * step + HOLE + firstPageLastId + step + 1; i += step) {
			// ���Ȳ����
			Record *temp = insertSpecifiedRecord(session1, session1->getMemoryContext(), i);
			totalKeys++;
			freeRecord(temp);
		}

		// �������Ѳ����̣߳�����ɹ���������Ҫ������
		simpleman.disableSyncPoint(SP_IDX_WAIT_FOR_PAGE_LOCK);
		simpleman.notifySyncPoint(SP_IDX_WAIT_FOR_PAGE_LOCK);
		simpleman.join();

		totalKeys -= 1;
	}

	/************************ case 4 *****************************/
	{
		// ��������һ���߳���258ҳ�����ɾ��������Ȼ�����ɵ�����
		// �µ��߳�ֻ��ɾ��ҳ���ĳһ�������SMO����
		u64 secondPageId = 258;
		u64 lockObjId = ((u64)m_tblDef->m_id << ((sizeof(u64) - sizeof(m_tblDef->m_id)) * 8)) | secondPageId;
		CPPUNIT_ASSERT(session->lockIdxObject(lockObjId));
		SimpleMan simpleman("I like simple", m_db, conn, m_heap, m_tblDef, m_index, HOLE + firstPageLastId + step , true, rowids[firstPageLastId + step]);
		simpleman.enableSyncPoint(SP_IDX_WAIT_FOR_PAGE_LOCK);
		simpleman.start();
		simpleman.joinSyncPoint(SP_IDX_WAIT_FOR_PAGE_LOCK);
		CPPUNIT_ASSERT(session->unlockIdxObject(lockObjId));

		// ִ��һ�����룬����258ҳ��LSN�ı�
		for (int i = HOLE + firstPageLastId + step + 2; i < HOLE + firstPageLastId + step * 2 + 2; i += step) {
			// ���Ȳ����
			Record *temp = insertSpecifiedRecord(session1, session1->getMemoryContext(), i);
			totalKeys++;
			freeRecord(temp);
		}

		// �������Ѳ����̣߳�����ɹ���������Ҫ������
		simpleman.disableSyncPoint(SP_IDX_WAIT_FOR_PAGE_LOCK);
		simpleman.notifySyncPoint(SP_IDX_WAIT_FOR_PAGE_LOCK);
		simpleman.join();

		totalKeys -= 1;
	}

	m_index->close(m_session, true);
	delete m_index;
	DrsIndice::drop(m_path);
	m_index = NULL;
	closeSmallHeap(m_heap);
	DrsHeap::drop(HEAP_NAME);
	m_heap = NULL;
	delete m_tblDef;
	m_tblDef = NULL;
	TableDef::drop(TBLDEF_NAME);

	m_db->getSessionManager()->freeSession(session);
	m_db->getSessionManager()->freeSession(session1);
	m_db->freeConnection(conn);
	m_db->freeConnection(conn1);
}


/**
 * ����SMO������������Ҫ�����������ӵ�����
 * @case1 ���Ƚ���һ��������������㣬��֤�����ĸ�ҳ��������������÷���ϵ��Ϊ50%
 *			�������ĵ����ڶ���Ҷҳ�����һƬ���ݣ�ֱ��SMO�������ȴ����������ڵ�ĵط�
 *			������һ���̣߳���������β��ִ�д��������룬���¸��ڵ�Ҳ���ѣ������߳�
 *			������һ�������̣߳�����Ҫ�����������ڵ�
 */
void IndexOperationTestCase::testMTSearchParentInSMO() {
	uint step = 1;
	uint totalKeys;
	m_tblDef = createBigTableDef();
	m_heap = createBigHeap(m_tblDef);
	totalKeys = buildBigHeap(m_heap, m_tblDef, false, 700, step);
	m_tblDef->m_indice[0]->m_splitFactor = 50;
	createIndex(m_path, m_heap, m_tblDef);

	Connection *conn = m_db->getConnection(false);
	Session *session = m_db->getSessionManager()->allocSession("IndexOperationTestCase::testUniqueScan", conn);
	MemoryContext *memoryContext = session->getMemoryContext();
	DrsIndex *index = m_index->getIndex(0);

	//rangeScanBackwardCheck(session, tableDef, index, NULL, NULL, totalKeys);
	uint insertPos = 1500;
	// ĳ���߳̿�ʼ������������·��ѣ��ȴ���Ѱ�Ҹ��ڵ�λ��
	Bee bee("I'm also a hardworker, of course", m_db, conn, m_heap, m_tblDef, m_index, insertPos, 100, INVALID_ROW_ID);
	bee.enableSyncPoint(SP_IDX_RESEARCH_PARENT_IN_SMO);
	bee.start();
	bee.joinSyncPoint(SP_IDX_RESEARCH_PARENT_IN_SMO);
	bee.notifySyncPoint(SP_IDX_RESEARCH_PARENT_IN_SMO);
	bee.joinSyncPoint(SP_IDX_RESEARCH_PARENT_IN_SMO);
	bee.notifySyncPoint(SP_IDX_RESEARCH_PARENT_IN_SMO);
	bee.joinSyncPoint(SP_IDX_RESEARCH_PARENT_IN_SMO);

	// ���￪ʼִ�д�������β�����룬���¸�������
	for (uint i = 1700; i < 1820; i++) {
		char name[LONG_KEY_LENGTH];
		RowId rid;
		sprintf(name, FAKE_LONG_CHAR234 "%d", i);
		Record *insertrecord = createRecord(m_tblDef, 0, i, name, i + ((u64)i << 32), (u32)(-1) - i);
		EXCPT_OPER(rid = m_heap->insert(m_session, insertrecord, NULL));

		freeRecord(insertrecord);

		insertrecord = createRecordRedundant(m_tblDef, 0, i, name, i + ((u64)i << 32), (u32)(-1) - i);
		insertrecord->m_rowId = rid;

		bool successful;
		uint dupIndex;
		session->startTransaction(TXN_INSERT, m_tblDef->m_id);
		EXCPT_OPER(successful = m_index->insertIndexEntries(session, insertrecord, &dupIndex));
		session->endTransaction(true);
		CPPUNIT_ASSERT(successful);
		totalKeys++;

		freeRecord(insertrecord);
	}

	// ����ǰһ���̣߳���ʱҪ�ض�λ���ڵ�
	bee.disableSyncPoint(SP_IDX_RESEARCH_PARENT_IN_SMO);
	bee.notifySyncPoint(SP_IDX_RESEARCH_PARENT_IN_SMO);
	bee.join();

	totalKeys += 100;
	rangeScanBackwardCheck(session, m_tblDef, index, NULL, NULL, totalKeys);
	rangeScanForwardCheck(session, m_tblDef, index, NULL, NULL, totalKeys);

	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(conn);

	m_index->close(m_session, true);
	delete m_index;
	DrsIndice::drop(m_path);
	m_index = NULL;
	closeSmallHeap(m_heap);
	DrsHeap::drop(HEAP_NAME);
	m_heap = NULL;
	delete m_tblDef;
	m_tblDef = NULL;
	TableDef::drop(TBLDEF_NAME);
}

Record * IndexOperationTestCase::insertHeapSpecifiedRecord(Session *session, MemoryContext *memoryContext, uint i) {
	char name[50];
	RowId rid;
	sprintf(name, "Kenneth Tse Jr. %d \0", i);
	Record *insertrecord = createRecord(m_tblDef, 0, i, name, i + ((u64)i << 32), (u32)(-1) - i);
	EXCPT_OPER(rid = m_heap->insert(m_session, insertrecord, NULL));

	freeRecord(insertrecord);

	insertrecord = createRecordRedundant(m_tblDef, 0, i, name, i + ((u64)i << 32), (u32)(-1) - i);
	insertrecord->m_rowId = rid;

	return insertrecord;
}

void IndexOperationTestCase::insertIndexSpecifiedRecord(Session *session, MemoryContext *memoryContext, Record *insertrecord) {
	session->startTransaction(TXN_INSERT, m_tblDef->m_id);
	bool successful;
	uint dupIndex;
	EXCPT_OPER(successful = m_index->insertIndexEntries(session, insertrecord, &dupIndex));
	CPPUNIT_ASSERT(successful);
	session->endTransaction(true);
}

/**
 * ����ָ��ֵ�ı��¼��ͬʱ�ڶѺ������ж�����
 */
Record* IndexOperationTestCase::insertSpecifiedRecord(Session *session, MemoryContext *memoryContext, uint i) {
	Record *insertRecord = insertHeapSpecifiedRecord(session, memoryContext, i);
	insertIndexSpecifiedRecord(session, memoryContext, insertRecord);
	return insertRecord;
}


/**
 * �Խ������������������ṹ��֤����֤�ṹ����������ͬһ��ҳ����������ݴ�С��������
 */
void IndexOperationTestCase::checkIndex(Session *session, MemoryContext *memoryContext, uint keys) {
	SubRecord *key1 = IndexKey::allocSubRecord(memoryContext, m_tblDef->m_indice[0], KEY_COMPRESS);
	SubRecord *key2 = IndexKey::allocSubRecord(memoryContext, m_tblDef->m_indice[0], KEY_COMPRESS);
	SubRecord *pkey0 = IndexKey::allocSubRecord(memoryContext, m_tblDef->m_indice[0], KEY_PAD);
	DrsIndex *index = m_index->getIndex(0);
	NTSE_ASSERT(((DrsBPTreeIndex*)index)->verify(session, key1, key2, pkey0, true));

	if (m_tblDef->m_numIndice == 1)
		return;

	SubRecord *key3 = IndexKey::allocSubRecord(memoryContext, m_tblDef->m_indice[1], KEY_COMPRESS);
	SubRecord *key4 = IndexKey::allocSubRecord(memoryContext, m_tblDef->m_indice[1], KEY_COMPRESS);
	SubRecord *pkey1 = IndexKey::allocSubRecord(memoryContext, m_tblDef->m_indice[1], KEY_PAD);
	index = m_index->getIndex(1);
	NTSE_ASSERT(((DrsBPTreeIndex*)index)->verify(session, key3, key4, pkey1, true));
}

/**
 * ��������ģ���������
 */
void IndexOperationTestCase::testSample() {
	uint step = 1;

	{
		cout << endl << endl << "Test large index tress sampling" << endl;
		// ������������
		m_tblDef = createSmallTableDefWithSame();
		m_heap = createSmallHeapWithSame(m_tblDef);
		uint totalKeys = buildHeapWithSame(m_heap, m_tblDef, DATA_SCALE * 5, step);
		try {
			createIndex(m_path, m_heap, m_tblDef);
		} catch (NtseException) {
			CPPUNIT_ASSERT(false);
		}

		Connection *conn = m_db->getConnection(false);
		Session *session = m_db->getSessionManager()->allocSession("IndexOperationTestCase::testRecordsInRange", conn);
		MemoryContext *memoryContext = session->getMemoryContext();
		DrsIndex *index = m_index->getIndex(1);
		IndexDef *indexDef = m_tblDef->m_indice[1];

		// ��ʼִ�и��ֲ�����ģ��ͳ��
		sampleIndex(m_tblDef->m_indice[0]->m_name, m_index->getIndex(0), 10);
		sampleIndex(m_tblDef->m_indice[1]->m_name, m_index->getIndex(1), 10);
		cout << endl;

		sampleIndex(m_tblDef->m_indice[0]->m_name, m_index->getIndex(0), 40);
		sampleIndex(m_tblDef->m_indice[1]->m_name, m_index->getIndex(1), 40);
		cout << endl;

		sampleIndex(m_tblDef->m_indice[0]->m_name, m_index->getIndex(0), 100);
		sampleIndex(m_tblDef->m_indice[1]->m_name, m_index->getIndex(1), 100);
		cout << endl;

		sampleIndex(m_tblDef->m_indice[0]->m_name, m_index->getIndex(0), 1000);
		sampleIndex(m_tblDef->m_indice[1]->m_name, m_index->getIndex(1), 1000);
		cout << endl;

		sampleIndex(m_tblDef->m_indice[0]->m_name, m_index->getIndex(0), 2000);
		sampleIndex(m_tblDef->m_indice[1]->m_name, m_index->getIndex(1), 2000);
		cout << endl;

		sampleIndex(m_tblDef->m_indice[0]->m_name, m_index->getIndex(0), 3000);
		sampleIndex(m_tblDef->m_indice[1]->m_name, m_index->getIndex(1), 3000);
		cout << endl;

		// ɾ�����ַ�Χ,��������
		Record *record = IndexKey::allocRecord(memoryContext, m_tblDef->m_maxRecSize);

		IndexScanHandle *indexHandle;
//		SubRecord *idxKey = IndexKey::allocSubRecordRED(memoryContext, m_tblDef->m_maxRecSize);
		SubRecord *idxKey = IndexKey::allocSubRecord(memoryContext, m_tblDef->getIndexDef(0), KEY_PAD);
		idxKey->m_columns = m_tblDef->m_indice[0]->m_columns;
		idxKey->m_numCols = m_tblDef->m_indice[0]->m_numCols;

		SubToSubExtractor *extractor = SubToSubExtractor::createInst(session->getMemoryContext(), m_tblDef, indexDef,
			indexDef->m_numCols, indexDef->m_columns, idxKey->m_numCols, idxKey->m_columns, KEY_COMPRESS, KEY_PAD, 1000);

		RowLockHandle *rlh;
		DrsIndex *rangeIndex = m_index->getIndex(0);
		indexHandle = rangeIndex->beginScan(session, NULL, true, true, Exclusived, &rlh, extractor);
		int count = 0;
		while (rangeIndex->getNext(indexHandle, idxKey)) {
			// ȥ��ӦHeapȡ��¼
			record->m_format = REC_FIXLEN;
			m_heap->getRecord(m_session, idxKey->m_rowId, record);
			record->m_format = REC_REDUNDANT;
			session->startTransaction(TXN_DELETE, m_tblDef->m_id);
			m_index->deleteIndexEntries(session, record, indexHandle);
			session->endTransaction(true);
			session->unlockRow(&rlh);
			count++;
			if (count > 500)
				break;
		}
		rangeIndex->endScan(indexHandle);

		sampleIndex(m_tblDef->m_indice[0]->m_name, m_index->getIndex(0), 10);
		sampleIndex(m_tblDef->m_indice[1]->m_name, m_index->getIndex(1), 10);
		cout << endl;

		sampleIndex(m_tblDef->m_indice[0]->m_name, m_index->getIndex(0), 40);
		sampleIndex(m_tblDef->m_indice[1]->m_name, m_index->getIndex(1), 40);
		cout << endl;

		sampleIndex(m_tblDef->m_indice[0]->m_name, m_index->getIndex(0), 100);
		sampleIndex(m_tblDef->m_indice[1]->m_name, m_index->getIndex(1), 100);
		cout << endl;

		sampleIndex(m_tblDef->m_indice[0]->m_name, m_index->getIndex(0), 1000);
		sampleIndex(m_tblDef->m_indice[1]->m_name, m_index->getIndex(1), 1000);
		cout << endl;

		sampleIndex(m_tblDef->m_indice[0]->m_name, m_index->getIndex(0), 2000);
		sampleIndex(m_tblDef->m_indice[1]->m_name, m_index->getIndex(1), 2000);
		cout << endl;

		sampleIndex(m_tblDef->m_indice[0]->m_name, m_index->getIndex(0), 3000);
		sampleIndex(m_tblDef->m_indice[1]->m_name, m_index->getIndex(1), 3000);
		cout << endl;

		// ���������ṩ�Ľӿ�
		cout << "Use Index interface: " << indexDef->m_name << endl;
		index->updateExtendStatus(session, 30);
		IndexStatusEx status = index->getStatusEx();
		cout << "Result by 30: " << status.m_pctUsed << "	" << status.m_compressRatio << endl;
		index->updateExtendStatus(session, 50);
		status = index->getStatusEx();
		cout << "Result by 50: " << status.m_pctUsed << "	" << status.m_compressRatio << endl;
		index->updateExtendStatus(session, 80);
		status = index->getStatusEx();
		cout << "Result by 80: " << status.m_pctUsed << "	" << status.m_compressRatio << endl;

		m_index->close(m_session, true);
		delete m_index;
		DrsIndice::drop(m_path);
		m_index = NULL;
		closeSmallHeap(m_heap);
		DrsHeap::drop(HEAP_NAME);
		m_heap = NULL;
		delete m_tblDef;
		m_tblDef = NULL;
		TableDef::drop(TBLDEF_NAME);

		m_db->getSessionManager()->freeSession(session);
		m_db->freeConnection(conn);
	}

	{
		cout << endl << endl << "Test very little index tress sampling" << endl;
		// ������������
		m_tblDef = createSmallTableDef();
		m_heap = createSmallHeap(m_tblDef);
		uint totalKeys = buildHeap(m_heap, m_tblDef, 2, step);
		try {
			createIndex(m_path, m_heap, m_tblDef);
		} catch (NtseException) {
			CPPUNIT_ASSERT(false);
		}

		Connection *conn = m_db->getConnection(false);
		Session *session = m_db->getSessionManager()->allocSession("IndexOperationTestCase::testRecordsInRange", conn);
		MemoryContext *memoryContext = session->getMemoryContext();
		DrsIndex *index = m_index->getIndex(1);
		IndexDef *indexDef = m_tblDef->m_indice[1];

		// ��ʼִ�и��ֲ�����ģ��ͳ��
		sampleIndex(m_tblDef->m_indice[0]->m_name, m_index->getIndex(0), 10);
		sampleIndex(m_tblDef->m_indice[1]->m_name, m_index->getIndex(1), 10);
		cout << endl;

		sampleIndex(m_tblDef->m_indice[0]->m_name, m_index->getIndex(0), 40);
		sampleIndex(m_tblDef->m_indice[1]->m_name, m_index->getIndex(1), 40);
		cout << endl;

		sampleIndex(m_tblDef->m_indice[0]->m_name, m_index->getIndex(0), 100);
		sampleIndex(m_tblDef->m_indice[1]->m_name, m_index->getIndex(1), 100);
		cout << endl;

		// ���������ṩ�Ľӿ�
		cout << "Use Index interface: " << indexDef->m_name << endl;
		index->updateExtendStatus(session, 30);
		IndexStatusEx status = index->getStatusEx();
		cout << "Result by 30: " << status.m_pctUsed << "	" << status.m_compressRatio << endl;
		index->updateExtendStatus(session, 50);
		status = index->getStatusEx();
		cout << "Result by 50: " << status.m_pctUsed << "	" << status.m_compressRatio << endl;

		m_index->close(m_session, true);
		delete m_index;
		DrsIndice::drop(m_path);
		m_index = NULL;
		closeSmallHeap(m_heap);
		DrsHeap::drop(HEAP_NAME);
		m_heap = NULL;
		delete m_tblDef;
		m_tblDef = NULL;
		TableDef::drop(TBLDEF_NAME);

		m_db->getSessionManager()->freeSession(session);
		m_db->freeConnection(conn);
	}

	{
		cout << endl << endl << "Test empty index tress sampling" << endl;
		// ������������
		m_tblDef = createSmallTableDef();
		m_heap = createSmallHeap(m_tblDef);
		try {
			createIndex(m_path, m_heap, m_tblDef);
		} catch (NtseException) {
			CPPUNIT_ASSERT(false);
		}

		Connection *conn = m_db->getConnection(false);
		Session *session = m_db->getSessionManager()->allocSession("IndexOperationTestCase::testRecordsInRange", conn);
		MemoryContext *memoryContext = session->getMemoryContext();
		DrsIndex *index = m_index->getIndex(1);
		IndexDef *indexDef = m_tblDef->m_indice[1];

		// ��ʼִ�и��ֲ�����ģ��ͳ��
		sampleIndex(m_tblDef->m_indice[0]->m_name, m_index->getIndex(0), 40);
		sampleIndex(m_tblDef->m_indice[1]->m_name, m_index->getIndex(1), 40);
		cout << endl;

		sampleIndex(m_tblDef->m_indice[0]->m_name, m_index->getIndex(0), 100);
		sampleIndex(m_tblDef->m_indice[1]->m_name, m_index->getIndex(1), 100);
		cout << endl;

		// ���������ṩ�Ľӿ�
		cout << "Use Index interface: " << indexDef->m_name << endl;
		index->updateExtendStatus(session, 50);
		IndexStatusEx status = index->getStatusEx();
		cout << "Result by 50: " << status.m_pctUsed << "	" << status.m_compressRatio << endl;

		m_index->close(m_session, true);
		delete m_index;
		DrsIndice::drop(m_path);
		m_index = NULL;
		closeSmallHeap(m_heap);
		DrsHeap::drop(HEAP_NAME);
		m_heap = NULL;
		delete m_tblDef;
		m_tblDef = NULL;
		TableDef::drop(TBLDEF_NAME);

		m_db->getSessionManager()->freeSession(session);
		m_db->freeConnection(conn);
	}
}

/**
 * ��ָ����һ���������в���
 * @param name	��������
 * @param index	��������
 * @param samplePages	����ҳ����
 */
void IndexOperationTestCase::sampleIndex(const char *name, DrsIndex *index, uint samplePages) {
	Connection *conn = m_db->getConnection(false);
	Session *session = m_db->getSessionManager()->allocSession("IndexOperationTestCase::sampleIndex", conn);
	SampleResult *result = SampleAnalyse::sampleAnalyse(session, index, samplePages, 0);
	cout << "Index: " << name << "	 Sample range: " << samplePages << endl;
	cout << "field num is " << result->m_numFields << endl;
	cout << "sample num is " << result->m_numSamples << endl;
	cout << "pctUsed is " << result->m_fieldCalc[0].m_average * 1.0 / INDEX_PAGE_SIZE << endl;
	cout << "compress ratio is " << result->m_fieldCalc[2].m_average * 1.0 / result->m_fieldCalc[1].m_average << endl;
	delete result;

	result = SampleAnalyse::sampleAnalyse(session, index, samplePages, 50, false);
	cout << "Index: " << name << "	 Sample range: " << samplePages << endl;
	cout << "field num is " << result->m_numFields << endl;
	cout << "sample num is " << result->m_numSamples << endl;
	cout << "pctUsed is " << result->m_fieldCalc[0].m_average * 1.0 / INDEX_PAGE_SIZE << endl;
	cout << "compress ratio is " << result->m_fieldCalc[2].m_average * 1.0 / result->m_fieldCalc[1].m_average << endl;
	delete result;

	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(conn);
}

/**
 * ������ôһ����������Ƴ�ʼ������һ�������������������ҳ��R�����Ҷ���P1��P2
 * P1�����ֵk��R���еĵ�һ����ֵ����ʱִ����һ��P1����ֵk��ɾ��������
 * ֮���в�����ĳ����ֵk'���������³ɶ�Ӧk������������k'��rowId����k��
 * ����֮��k'���������P2ҳ�桪����ΪrowId��ԭ������R���еĵ�һ�����Ȼ��ֵ��ȡ�
 * ���ʱ��������k�����������в�ѯ��Ӧ�ñ�֤���ҵ�k'�����ֵ��¼
 *
 * ���ڲ����ʱ���бȽ�rowId�������������ϸ���key-rowId�������С����
 * ���ǲ�ѯ��ʱ�����ֻ��������û��rowId��������λ��Ҷҳ���ʱ����ܻ���ֶ�λ��׼ȷ����Ҫ���⴦��
 * ��ǰ������ӣ����û�����⴦��Ļ�����λk'��ʱ��ֻ�ᶨλ��P1��Ȼ��ͻط����Ҳ�����ֵ
 * ��bug���ڹ��ܲ���1.1.2�������ֵģ����http://172.20.0.53:3000/issues/show/667
 */
void IndexOperationTestCase::testStrangePKFetch() {
	// ������������
	m_tblDef = createSmallTableDef();
	m_heap = createSmallHeap(m_tblDef);
	uint totalKeys = buildHeap(m_heap, m_tblDef, 150, 1);
	try {
		createIndex(m_path, m_heap, m_tblDef);
	} catch (NtseException) {
		CPPUNIT_ASSERT(false);
	}

	Connection *conn = m_db->getConnection(false);
	Session *session = m_db->getSessionManager()->allocSession("IndexOperationTestCase::testRecordsInRange", conn);
	MemoryContext *memoryContext = session->getMemoryContext();
	DrsIndex *index = m_index->getIndex(1);
	IndexDef *indexDef = m_tblDef->m_indice[1];

	// ��ʱ��һ��Ҷ�ڵ�����һ����ֵ��������1099��ɾ�����ü�ֵ��Ȼ�������ͬ��ֵ������rowId����ļ�ֵ����ʱ��Ӧ�ò����ڵڶ���Ҷҳ��
	u64 key;

#if (NTSE_PAGE_SIZE == 1024)
	key = 1012;
#else
	key = 1115;
#endif
	char name[50];
	sprintf(name, "Kenneth Tse Jr. %d \0", key);
	Record *record = createRecordRedundant(m_tblDef, 0, key, name, (key + ((u64)key << 32)), (u32)((u32)(-1) - key));

	SubRecordBuilder keyBuilder(m_tblDef, KEY_PAD, INVALID_ROW_ID);
	SubRecord *findKey = keyBuilder.createSubRecordById("0", &key);
	RowId rowId;
	CPPUNIT_ASSERT(index->getByUniqueKey(session, findKey, None, &rowId, NULL, NULL, NULL));
	record->m_rowId = rowId;

	RowLockHandle *rlh;
	IndexScanHandle *handle = index->beginScan(session, findKey, true, true, Exclusived, &rlh, NULL);
	CPPUNIT_ASSERT(index->getNext(handle, NULL));
	session->startTransaction(TXN_DELETE, m_tblDef->m_id);
	m_index->deleteIndexEntries(session, record, handle);
	session->endTransaction(true);
	session->unlockRow(&rlh);
	index->endScan(handle);

	CPPUNIT_ASSERT(!index->getByUniqueKey(session, findKey, None, &rowId, NULL, NULL, NULL));

	record->m_rowId = INVALID_ROW_ID - 1;
	uint dupIndex;
	session->startTransaction(TXN_INSERT, m_tblDef->m_id);
	CPPUNIT_ASSERT(m_index->insertIndexEntries(session, record, &dupIndex));
	session->endTransaction(true);

	CPPUNIT_ASSERT(index->getByUniqueKey(session, findKey, None, &rowId, NULL, NULL, NULL));

	freeRecord(record);
	freeSubRecord(findKey);

	m_index->close(m_session, true);
	delete m_index;
	DrsIndice::drop(m_path);
	m_index = NULL;
	closeSmallHeap(m_heap);
	DrsHeap::drop(HEAP_NAME);
	m_heap = NULL;
	delete m_tblDef;
	m_tblDef = NULL;
	TableDef::drop(TBLDEF_NAME);

	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(conn);
}


/**
 * ��Ψһ�����ϲ���һ���ƶ����Ƶ�����
 */
bool IndexOperationTestCase::findAKey(Session *session, DrsIndex *index, RowLockHandle **rlh, uint i, SubToSubExtractor *extractor) {
	SubRecord *findKey = makeFindKeyWhole(i, m_heap, m_tblDef);
	findKey->m_rowId = INVALID_ROW_ID;
	RowId rowId;
	bool found = index->getByUniqueKey(session, findKey, (rlh == NULL ? None : Exclusived), &rowId, NULL, rlh, extractor);
	freeSubRecord(findKey);
	return found;
}


/**
 * ����Ψһ�Բ���͸���ʧ��֮��Ļ��˲��ִ���
 * @case1 ����һ��С�ѣ�����1����¼���ɣ����Ǹöѵ����������������������ǰ������Ψһ������
 *			���Բ���һ����ͬ�ļ�¼���ü�¼ֻ���ڵڶ���Ψһ����������ʱ�Ż��������Ӧ�ûᵼ�µ�һ�������������
 * @case2 ���Թ���ͬcase1����ͬ���ǲ������ɸ��²��������Ը��µĻ���
 */
void IndexOperationTestCase::testUniqueKeyConflict() {
	m_tblDef = createSmallTableDef();
	m_heap = createSmallHeap(m_tblDef);
	uint totalKeys = buildHeap(m_heap, m_tblDef, 2, 1);
	createIndex(m_path, m_heap, m_tblDef);

	Connection *conn = m_db->getConnection(false);
	Session *session = m_db->getSessionManager()->allocSession("IndexOperationTestCase::testUniqueKeyConflict", conn);
	MemoryContext *memoryContext = session->getMemoryContext();

	{	// ����һ��Ψһ������
		assert(memcmp(m_tblDef->m_columns[2]->m_name, "BankAccount", sizeof("BankAccount")) == 0);
		u32 prefixesArr[] = {0};
		IndexDef indexDef("AnotherUnique", 1, &m_tblDef->m_columns[2], prefixesArr, true, false);
		m_tblDef->addIndex(&indexDef);
		try {
			m_index->createIndexPhaseOne(m_session, m_tblDef->m_indice[2], m_tblDef, m_heap);
			m_index->createIndexPhaseTwo(m_tblDef->m_indice[2]);
		} catch (NtseException) { NTSE_ASSERT(false); }
	}

	// case1 ���Բ���
	// ��ʼ����һ���ᵼ��������2����BankAccount����Ψһ�Գ�ͻ�ļ�ֵ
	uint newKey = HOLE + 2;
	char name[50];
	sprintf(name, "Kenneth Tse Jr. %d \0", newKey);
	// �ü�¼������֤BankAccount�ֶ��ǳ�ͻ�ģ������ֶβ���ͻ
	Record *insertrecord = createRecordRedundant(m_tblDef, 0, newKey, name, HOLE + ((u64)HOLE << 32), (u32)(-1) - newKey);
	insertrecord->m_rowId = 12345;	// ���벻��ɹ���rowId����α��

	session->startTransaction(TXN_INSERT, m_tblDef->m_id);
	bool successful;
	uint dupIndex = (uint)-1;
	EXCPT_OPER(successful = m_index->insertIndexEntries(session, insertrecord, &dupIndex));
	CPPUNIT_ASSERT(!successful && dupIndex == 2);
	session->endTransaction(true);

	freeRecord(insertrecord);

	// ��֤���벻�ɹ���Ӧ�û���ֻ��1��
	rangeScanForwardCheck(session, m_tblDef, m_index->getIndex(2), NULL, NULL, totalKeys, true);
	rangeScanForwardCheck(session, m_tblDef, m_index->getIndex(1), NULL, NULL, totalKeys, true);

	/****************************************************************************************************/

	// case2 ���Բ���
	// ִ�и��²��������������ֶΣ�һ����userId�ֶΣ�����ֶα�֤�ܹ����³ɹ�
	// ��һ������bankaccount�ֶΣ����Ҫ���Ի��ͻ�����
	SubRecord *before, *after;
	before = IndexKey::allocSubRecordRED(memoryContext, m_tblDef->m_maxRecSize);

	u16 *columns = new u16[m_tblDef->m_numCols];
	for (int k = 0; k < m_tblDef->m_numCols; k++)
		columns[k] = (u16)k;

	before->m_numCols = m_tblDef->m_numCols;
	before->m_columns = columns;

	Record *oldrecord;
	uint updated = 0;
	// �����¼�ֵ
	sprintf(name, "Kenneth Tse Jr. %d \0", newKey);
	after = createUpdateAfterRecordRedundant3(m_tblDef, rowids[0], newKey, HOLE + 1 + ((u64)(HOLE + 1) << 32));

	// ����ɼ�ֵ
	sprintf(name, "Kenneth Tse Jr. %d \0", HOLE);
	oldrecord = createRecord(m_tblDef, 0, HOLE, name, HOLE + ((u64)HOLE << 32), (u32)(-1) - HOLE);
	oldrecord->m_rowId = rowids[0];
	before->m_size = oldrecord->m_size;
	before->m_data = oldrecord->m_data;
	before->m_rowId = oldrecord->m_rowId;

	session->startTransaction(TXN_UPDATE, m_tblDef->m_id);
	// ���и���
	dupIndex = (uint)-1;
	CPPUNIT_ASSERT(!m_index->updateIndexEntries(session, before, after, false, &dupIndex));
	session->endTransaction(true);

	CPPUNIT_ASSERT(dupIndex == 2);

	freeSubRecord(after);
	freeRecord(oldrecord);

	// ��֤���벻�ɹ���Ӧ�û���ֻ��1��
	rangeScanForwardCheck(session, m_tblDef, m_index->getIndex(2), NULL, NULL, totalKeys, true);
	rangeScanForwardCheck(session, m_tblDef, m_index->getIndex(1), NULL, NULL, totalKeys, true);

	delete [] columns;

	m_index->close(m_session, true);
	delete m_index;
	DrsIndice::drop(m_path);
	m_index = NULL;
	closeSmallHeap(m_heap);
	DrsHeap::drop(HEAP_NAME);
	m_heap = NULL;
	delete m_tblDef;
	m_tblDef = NULL;
	TableDef::drop(TBLDEF_NAME);

	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(conn);
}


class LockOnlyThread : public Thread {
public:
	LockOnlyThread(Session *session) : Thread("LockOnlyThread"), m_mutex("LockOnlyThreadMutex", __FILE__, __LINE__) {
		m_session = session;
		m_lockObj = (u64)-1;
		m_exit = false;
		m_lastSucc = false;
	}

	void run() {
		while (true) {
			u64 lockObj = (u64)-1;
			if (m_exit)
				break;
			LOCK(&m_mutex);
			if (m_lockObj != (u64)-1) {
				lockObj = m_lockObj;
				m_lockObj = (u64)-1;
			}
			UNLOCK(&m_mutex);
			if (lockObj != (u64)-1)
				m_lastSucc = m_session->lockIdxObject(lockObj);
		}

		m_session->unlockIdxAllObjects();
	}

	void setLock(u64 lockObj) {
		LOCK(&m_mutex);
		m_lockObj = lockObj;
		m_lastSucc = false;
		UNLOCK(&m_mutex);
	}

	void setExit() {
		m_exit = true;
	}

	bool getLastSucc() {
		return m_lastSucc;
	}

private:
	Session *m_session;
	u64		m_lockObj;
	Mutex	m_mutex;
	bool	m_exit;
	volatile bool	m_lastSucc;
};


/**
 * ͨ�����������ĳ���������DML�ĸ��²��������ж������Ĵ������̵���ȷ��
 * @case1 ���Բ�������е��������Ƚ���һ��������ֻ����������¼��ִ���������룬������������
 * @case2 ����ɾ�������е�������ͬcase1�Ĳ�������
 */
void IndexOperationTestCase::testDMLDeadLocks() {
	{	// case 1 ���Բ��������
		uint testScale = 80;
		m_tblDef = createSmallTableDef();
		m_heap = createSmallHeap(m_tblDef);
		uint totalKeys = buildHeap(m_heap, m_tblDef, testScale, 1);
		createIndex(m_path, m_heap, m_tblDef);

		Connection *conn = m_db->getConnection(false);
		Session *session = m_db->getSessionManager()->allocSession("IndexOperationTestCase::testDMLDeadLocks", conn);
		Connection *conn1 = m_db->getConnection(false);
		Session *session1 = m_db->getSessionManager()->allocSession("IndexOperationTestCase::testDMLDeadLocks", conn1);
		MemoryContext *memoryContext = session->getMemoryContext();

		Bee bee("I'm also a hardworker, of course", m_db, conn, m_heap, m_tblDef, m_index, HOLE + testScale, 50, INVALID_ROW_ID);

		// ��ʱ������Ӧ��ֻ��һ����ҳ�棬��������֮��ᵼ��SMO����
		// ��������һ���̣߳���SMO���������߳�һֱ����ֱ��׼����SMO����
		// ��һ���̴߳�ʱ�ȴ���ҳ�������ò����̼߳�������ʱ������߳�Ӧ�û���������
		// ר�ż������̴߳�ʱ�ͷ����е�����ʹ�ò����߳̿�����ɲ���
		// �����֤��ȷ��

		// �ȼ���SMO��
		u64 smoId = 2;
		u64 realLockObj = ((u64)m_tblDef->m_id << ((sizeof(u64) - sizeof(m_tblDef->m_id)) * 8)) | smoId;
		LockOnlyThread lot(session1);
		lot.start();
		lot.setLock(realLockObj);
		while (!lot.getLastSucc());	// ��������϶����Գɹ�

		// �����߳�׼��SMO
		bee.enableSyncPoint(SP_IDX_TO_LOCK_SMO);
		bee.start();
		bee.joinSyncPoint(SP_IDX_TO_LOCK_SMO);

		// ��Ԥ�ȼ���ҳ����
		u64 pageId = 257;
		realLockObj = ((u64)m_tblDef->m_id << ((sizeof(u64) - sizeof(m_tblDef->m_id)) * 8)) | pageId;			
		lot.setLock(realLockObj);
		Thread::msleep(100);
		CPPUNIT_ASSERT(!lot.getLastSucc());	// ���ʱ��Ӳ�����

		// �ò����̼߳����������룬������������
		bee.disableSyncPoint(SP_IDX_TO_LOCK_SMO);
		bee.notifySyncPoint(SP_IDX_TO_LOCK_SMO);

		// ��ʱbee�߳��������ˣ������߳̿��Ի������ָ������������֤������Լ�������
		while (!lot.getLastSucc());
		lot.setExit();

		bee.join();

		totalKeys += 50;

		// ��֤������ȷ��
		rangeScanForwardCheck(session, m_tblDef, m_index->getIndex(1), NULL, NULL, totalKeys, true);
		
		m_index->close(m_session, true);
		delete m_index;
		DrsIndice::drop(m_path);
		m_index = NULL;

		m_db->getSessionManager()->freeSession(session);
		m_db->freeConnection(conn);
		m_db->getSessionManager()->freeSession(session1);
		m_db->freeConnection(conn1);

		closeSmallHeap(m_heap);
		DrsHeap::drop(HEAP_NAME);
		m_heap = NULL;
		delete m_tblDef;
		m_tblDef = NULL;
		TableDef::drop(TBLDEF_NAME);
	}

	{	// case 2 ����ɾ��������
		uint testScale = 80;
		m_tblDef = createSmallTableDef();
		m_heap = createSmallHeap(m_tblDef);
		uint totalKeys = buildHeap(m_heap, m_tblDef, testScale, 1);
		createIndex(m_path, m_heap, m_tblDef);

		Connection *conn = m_db->getConnection(false);
		Session *session = m_db->getSessionManager()->allocSession("IndexOperationTestCase::testDMLDeadLocks", conn);
		Connection *conn1 = m_db->getConnection(false);
		Session *session1 = m_db->getSessionManager()->allocSession("IndexOperationTestCase::testDMLDeadLocks", conn1);
		MemoryContext *memoryContext = session->getMemoryContext();

		SimpleMan simpleman("I like simple", m_db, conn, m_heap, m_tblDef, m_index, HOLE, true, INVALID_ROW_ID, true);

		// ��ʱ������Ӧ��ֻ��һ����ҳ�棬����ɾ��֮��ᵼ��SMO����
		// ��������һ���̣߳���SMO����ɾ���߳�һֱ����ֱ��׼����SMO����
		// ��һ���̴߳�ʱ�ȴ���ҳ��������ɾ���̼߳�������ʱ��ɾ���߳�Ӧ�û���������
		// ר�ż������̴߳�ʱ�ͷ����е�����ʹ��ɾ���߳̿�����ɲ���
		// �����֤��ȷ��

		// �ȼ���SMO��
		u64 smoId = 1;
		u64 realLockObj = ((u64)m_tblDef->m_id << ((sizeof(u64) - sizeof(m_tblDef->m_id)) * 8)) | smoId;
		LockOnlyThread lot(session1);
		lot.start();
		lot.setLock(realLockObj);
		while (!lot.getLastSucc());	// ��������϶����Գɹ�

		// ɾ���߳�׼��SMO
		simpleman.enableSyncPoint(SP_IDX_TO_LOCK_SMO);
		simpleman.start();
		simpleman.joinSyncPoint(SP_IDX_TO_LOCK_SMO);

		// ��Ԥ�ȼ���ҳ����
		u64 pageId = 129;
		realLockObj = ((u64)m_tblDef->m_id << ((sizeof(u64) - sizeof(m_tblDef->m_id)) * 8)) | pageId;			
		lot.setLock(realLockObj);
		Thread::msleep(100);
		CPPUNIT_ASSERT(!lot.getLastSucc());	// ���ʱ��Ӳ�����

		// �ò����̼߳����������룬������������
		simpleman.disableSyncPoint(SP_IDX_TO_LOCK_SMO);
		simpleman.notifySyncPoint(SP_IDX_TO_LOCK_SMO);

		// ��ʱbee�߳��������ˣ������߳̿��Ի������ָ������������֤������Լ�������
		while (!lot.getLastSucc());
		lot.setExit();

		simpleman.join();

		totalKeys -= totalKeys;

		// ��֤������ȷ��
		rangeScanForwardCheck(session, m_tblDef, m_index->getIndex(1), NULL, NULL, totalKeys, true);

		m_index->close(m_session, true);
		delete m_index;
		DrsIndice::drop(m_path);
		m_index = NULL;

		m_db->getSessionManager()->freeSession(session);
		m_db->freeConnection(conn);
		m_db->getSessionManager()->freeSession(session1);
		m_db->freeConnection(conn1);

		closeSmallHeap(m_heap);
		DrsHeap::drop(HEAP_NAME);
		m_heap = NULL;
		delete m_tblDef;
		m_tblDef = NULL;
		TableDef::drop(TBLDEF_NAME);
	}
}
/*
void IndexOperationTestCase::testInsertAfterSearch() {
	uint testScale = DATA_SCALE / 5;
	// ����Ψһ����������
	uint totalKeys;
	uint step = 3;
	// ������������
	m_tblDef = createSmallTableDef();
	m_heap = createSmallHeap(m_tblDef);
	totalKeys = buildHeap(m_heap, m_tblDef, testScale, step);
	createIndex(m_path, m_heap, m_tblDef);

	Connection *conn = m_db->getConnection(false);
	Session *session = m_db->getSessionManager()->allocSession("IndexOperationTestCase::testInsert", conn);
	MemoryContext *memoryContext = session->getMemoryContext();

	checkIndex(session, memoryContext, totalKeys);

	// ������1���в���
	Record *record = IndexKey::allocRecord(memoryContext, m_tblDef->m_maxRecSize);

	DrsIndex *uniqueIndex = m_index->getIndex(1);

	// ��֤��������û�еļ�ֵ���ܱ�����
	for (int i = HOLE + 1; i < testScale + HOLE; i += step) {
		// ���Ȳ����
		Record *temp  = insertHeapSpecifiedRecord(session, memoryContext, i);

		SubRecord *key1 = IndexKey::allocSubRecord(session->getMemoryContext(), false, temp, 
				m_tblDef, m_tblDef->m_indice[1]);

		DrsIndexScanHandleInfo *scanHdlInfo = NULL;
		CPPUNIT_ASSERT(!uniqueIndex->checkDuplicate(session, key1, &scanHdlInfo));
		CPPUNIT_ASSERT(NULL != scanHdlInfo);

		session->startTransaction(TXN_INSERT, m_tblDef->m_id);
		//����Ψһ��������ֵ
		CPPUNIT_ASSERT(uniqueIndex->insertGotPage(scanHdlInfo));		
		SubRecord *key0 = IndexKey::allocSubRecord(session->getMemoryContext(), false, temp, m_tblDef, 
			m_tblDef->m_indice[0]);
		//�����Ψһ��������ֵ
		m_index->getIndex(0)->insertNoCheckDuplicate(session, key0);		
		session->endTransaction(true);

		CPPUNIT_ASSERT(uniqueIndex->checkDuplicate(session, key1, NULL));

		checkIndex(session, memoryContext, totalKeys);

		totalKeys++;

		freeRecord(temp);
	}
	for (int i = HOLE + 2; i < testScale + HOLE + END_SCALE; i += step) {	// �ٸ��Ӳ��뷶Χ�������
		// ���Ȳ����
		Record *temp = insertSpecifiedRecord(session, memoryContext, i);

		checkIndex(session, memoryContext, totalKeys);

		totalKeys++;

		freeRecord(temp);
	}

	SubRecord *idxKey = IndexKey::allocSubRecordRED(memoryContext, m_tblDef->m_maxRecSize);
	u16 *columns = new u16[m_tblDef->m_numCols];
	for (int k = 0; k < m_tblDef->m_numCols; k++)
		columns[k] = (u16)k;

	// ��֤����֮�������ܵ��������
	// ������ɨ�裬�õ�������
	DrsIndex *index = m_index->getIndex(0);
	idxKey->m_columns = m_tblDef->m_indice[0]->m_columns;
	idxKey->m_numCols = m_tblDef->m_indice[0]->m_numCols;

	rangeScanBackwardCheck(session, m_tblDef, index, NULL, idxKey, totalKeys);
	rangeScanForwardCheck(session, m_tblDef, index, NULL, idxKey, totalKeys);

	// ��֤�Ѿ�����ļ�ֵ�����ܱ�����
	// ��tablescan���õ����ɸ�record����֤�����ܱ�����
	uint reinsertNums = 20;
	idxKey->m_columns = columns;
	idxKey->m_numCols = m_tblDef->m_numCols;
	IndexDef *indexDef = m_tblDef->m_indice[1];
	DrsHeapScanHandle *scanHandle = m_heap->beginScan(session, SubrecExtractor::createInst(session, m_tblDef, idxKey), None, NULL, false);
	
	int count = 0;
	while (true) {
		if (m_heap->getNext(scanHandle, idxKey)) {
			if (!m_heap->getNext(scanHandle, idxKey))
				break;
		} else
			break;

		record->m_size = idxKey->m_size;
		record->m_rowId = idxKey->m_rowId;
		memcpy(record->m_data, idxKey->m_data, idxKey->m_size);

		u64 savepoint = session->getMemoryContext()->setSavepoint();
		SubRecord *key = IndexKey::allocSubRecord(session->getMemoryContext(), false, record, m_tblDef, indexDef);
		CPPUNIT_ASSERT(uniqueIndex->checkDuplicate(session, key, NULL));
		session->getMemoryContext()->resetToSavepoint(savepoint);

		checkIndex(session, memoryContext, totalKeys);

		count++;

		if (count > reinsertNums)
			break;
	}
	m_heap->endScan(scanHandle);

	// ��֤����֮�������ܵ��������
	// ������ɨ�裬�õ�������
	idxKey->m_columns = m_tblDef->m_indice[0]->m_columns;
	idxKey->m_numCols = m_tblDef->m_indice[0]->m_numCols;

	rangeScanBackwardCheck(session, m_tblDef, index, NULL, idxKey, totalKeys);
	rangeScanForwardCheck(session, m_tblDef, index, NULL, idxKey, totalKeys);

	m_index->close(m_session, true);
	delete m_index;
	DrsIndice::drop(m_path);
	m_index = NULL;
	closeSmallHeap(m_heap);
	DrsHeap::drop(HEAP_NAME);
	m_heap = NULL;
	delete m_tblDef;
	m_tblDef = NULL;
	TableDef::drop(TBLDEF_NAME);

	delete[] columns;

	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(conn);
}
*/
/////////////////////////////////////////////////////////////////////////////////////
/************************************************************************/
/* �����ָ���Ԫ����
/* ����������ʹ��һ��С��ִ�У����������������壬��һ�������Ƿ�Ψһ�Ե�
/* �ڶ���������Ψһ����������������Ҫ����Ψһ�Գ�ͻʹ��
/* �����������������õ����������ǲ���ǰ�������������ڲ�����һ������ִ��
/* ����Ĳ������ݲ��������Լ�������ѡ��
/************************************************************************/


const char* IndexRecoveryTestCase::getName() {
	return "Index recovery test";
}

const char* IndexRecoveryTestCase::getDescription() {
	return "Test Index recovery(recovery of all kinds of operations, including redo, undo and redo compensation logs).";
}

bool IndexRecoveryTestCase::isBig() {
	return false;
}

void IndexRecoveryTestCase::setUp() {
	m_index = NULL;
	m_heap = NULL;
	m_tblDef = NULL;
	Database::drop(".");
	EXCPT_OPER(m_db = Database::open(&m_cfg, true));
	m_conn = m_db->getConnection(false);
	m_session = m_db->getSessionManager()->allocSession("IndexRecoveryTestCase::setUp", m_conn);
	char name[255];	// = "testIndex.nti"
	sprintf(name, "%s", INDEX_NAME);
	memcpy(m_path, name, sizeof(name));
	srand((unsigned)time(NULL));
	clearRSFile(&m_cfg);
	ts.idx = true;
	vs.idx = true;
}

void IndexRecoveryTestCase::tearDown() {
	// ������
	if (m_index != NULL) {
		m_index->close(m_session, true);
		delete m_index;
		DrsIndice::drop(m_path);
	}
	// ����
	if (m_heap != NULL) {
		closeSmallHeap(m_heap);
		DrsHeap::drop(HEAP_NAME);
	}

	if (m_tblDef != NULL) {
		delete m_tblDef;
		m_tblDef = NULL;
		TableDef::drop(TBLDEF_NAME);
	}

	if (m_db != NULL) {
		m_db->getSessionManager()->freeSession(m_session);
		m_db->freeConnection(m_conn);
		m_db->close();
		delete m_db;
		m_db = NULL;
		Database::drop(".");
	}

	ts.idx = false;
	vs.idx = false;
}


/**
 * ���ݱ��崴�����������ݵ�����
 */
void IndexRecoveryTestCase::createIndex(char *path, DrsHeap *heap, const TableDef *tblDef) throw(NtseException) {
	File oldindexfile(path);
	oldindexfile.remove();

	DrsIndice::create(path, tblDef);
	CPPUNIT_ASSERT(m_index == NULL);
	m_index = DrsIndice::open(m_db, m_session, path, tblDef, NULL);
	try {
		m_index->createIndexPhaseOne(m_session, tblDef->m_indice[0], tblDef, heap);
		m_index->createIndexPhaseTwo(tblDef->m_indice[0]);
		if (tblDef->m_numIndice > 1) {
			m_index->createIndexPhaseOne(m_session, tblDef->m_indice[1], tblDef, heap);
			m_index->createIndexPhaseTwo(tblDef->m_indice[1]);
		}
	} catch (NtseException &e) {
		cout << e.getMessage() << endl;
		CPPUNIT_ASSERT(false);
	}
}

TableDef* IndexRecoveryTestCase::createSmallTableDef() {
	File oldsmallfile(TBLDEF_NAME);
	oldsmallfile.remove();

	TableDefBuilder *builder;
	TableDef *tableDef;

	// ����С��
	builder = new TableDefBuilder(99, "inventory", "User");
	builder->addColumn("UserId", CT_BIGINT, false)->addColumnS("UserName", CT_CHAR, 50, false, false);
	builder->addColumn("BankAccount", CT_BIGINT, false)->addColumn("Balance", CT_INT, false);
	builder->addIndex("NOCONOSTRAIN", false, false, false, "UserId", 0, "BankAccount", 0, NULL);
	builder->addIndex("PRIMARY", true, true, false, "UserId", 0, "UserName", 0, "BankAccount", 0, NULL);
	tableDef = builder->getTableDef();
	EXCPT_OPER(tableDef->writeFile(TBLDEF_NAME));
	delete builder;

	return tableDef;
}
/**
 * ����һ��С�ѣ���������
 */
DrsHeap* IndexRecoveryTestCase::createSmallHeap(TableDef *tableDef) {
	char tableName[255] = HEAP_NAME;
	File oldsmallfile(tableName);
	oldsmallfile.remove();
	DrsHeap *heap = NULL;

	EXCPT_OPER(DrsHeap::create(m_db, tableName, tableDef));
	EXCPT_OPER(heap = DrsHeap::open(m_db, m_session, tableName, tableDef));

	return heap;
}



/**
 * �رմ����Ķ�
 */
void IndexRecoveryTestCase::closeSmallHeap(DrsHeap *heap) {
	EXCPT_OPER(heap->close(m_session, true));
	delete heap;
}


/**
 * �ڶ��в���ָ����ģ������
 */
uint IndexRecoveryTestCase::buildHeap(DrsHeap *heap, const TableDef *tblDef, uint size, uint step) {
	assert(step > 0);
	uint count = 0;
	Record *record;
	for (int i = HOLE; i < size + HOLE; i += step) {
		char name[50];
		RowId rid;
		sprintf(name, "Kenneth Tse Jr. %d \0", i);
		record = createRecord(tblDef, 0, i, name, i + ((u64)i << 32), (u32)(-1) - i);
		EXCPT_OPER(rid = heap->insert(m_session, record, NULL));
		rowids[i - HOLE] = rid;
		count++;
		freeRecord(record);
	}

	return count;
}


/**
 * ����������ݣ��ñ�Ϊ�˲���������ֵ����128������
 * ��˱�������������ַ������ȱ���ﵽ��������128
 */
uint IndexRecoveryTestCase::buildBigHeap(DrsHeap *heap, const TableDef *tblDef, bool testPrefix, uint size, uint step) {
	Record *record;
	uint count = 0;
	for (uint i = HOLE; i < HOLE + size; i += step) {
		char name[LONG_KEY_LENGTH];
		RowId rid;
		if (testPrefix)
			sprintf(name, FAKE_LONG_CHAR234 "%d", i);
		else
			sprintf(name, "%d" FAKE_LONG_CHAR234, i);
		record = createRecord(tblDef, 0, i, name, i + ((u64)i << 32), (u32)(-1) - i);
		EXCPT_OPER(rid = heap->insert(m_session, record, NULL));
		rowids[i - HOLE] = rid;
		count++;
		freeRecord(record);
	}

	return count;
}

TableDef* IndexRecoveryTestCase::createBigTableDef() {
	char tableName[255] = HEAP_NAME;
	File oldsmallfile(tableName);
	oldsmallfile.remove();

	TableDefBuilder *builder;
	TableDef *tableDef;

	// ����С��
	builder = new TableDefBuilder(99, "inventory", "User");
	builder->addColumn("UserId", CT_BIGINT, false)->addColumnS("UserName", CT_CHAR, LONG_KEY_LENGTH);
	builder->addColumn("BankAccount", CT_BIGINT)->addColumn("Balance", CT_INT);
	builder->addIndex("RANGE", false, false, false, "UserId", 0, "UserName", 0, "BankAccount", 0, NULL);
	tableDef = builder->getTableDef();
	EXCPT_OPER(tableDef->writeFile(TBLDEF_NAME));
	delete builder;

	return tableDef;
}

/**
 * ������ѣ���¼���ȳ���100��
 * ����ֻ����һ�������������Ƿ�Ψһ�Ե�����
 */
DrsHeap* IndexRecoveryTestCase::createBigHeap(TableDef* tableDef) {
	DrsHeap *heap;
	char tableName[255] = HEAP_NAME;
	
	EXCPT_OPER(DrsHeap::create(m_db, tableName, tableDef));
	EXCPT_OPER(heap = DrsHeap::open(m_db, m_session, tableName, tableDef));

	return heap;
}


/**
 * �����������ָ�������Ҫʹ�õĲ���ظ��ӿ��Լ���¼�ָ����̸��½�����־�ӿ�
 * @case1	ֱ�Ӳ��Ը��½�����־�ӿڣ���߸�����
 * @case2	��һ��������ִ�в��룬����֮ǰ����αװ������ϢΪֻ��N��1��������
 *			����֮���ٵ��ò��ز���ָ��ӿڣ�������������������һ����
 * @case3	��case2���ƣ���ͬ���ǲ��Բ���ɾ���ӿ�
 * @case4	��case2���ƣ���ͬ���ǲ��Բ�����½ӿ�
 * @case4	��case4���ƣ���ͬ���ǲ��Բ������ȱ�ٲ���Ľӿڽӿ�
 */
void IndexRecoveryTestCase::testCPSTDMLInRecv() {
	RowId rowId;
	uint totalKeys;
	/********************* case 1 & case 2 ***************************/
	{
		m_tblDef = createSmallTableDef();
		m_heap = createSmallHeap(m_tblDef);
		totalKeys = 0;/*buildHeap(m_heap, 150, 1);*/
		createIndex(m_path, m_heap, m_tblDef);

		Connection *conn = m_db->getConnection(false);
		Session *session = m_db->getSessionManager()->allocSession("IndexRecoveryTestCase::testCPSTDMLInRecv", conn);
		MemoryContext *memoryContext = session->getMemoryContext();
		DrsIndex *index = m_index->getIndex(0);
		
		// ���Լ�¼��־�ӿ�
		m_index->logDMLDoneInRecv(session, 0, true);

		// �޸������ĸ���Ϊ1����Ȼ��ִ�в���һ����¼�Ĳ���
		((DrsBPTreeIndice*)m_index)->decIndexNum();
		Record *temp = insertSpecifiedRecord(session, memoryContext, 0);
		rowId = temp->m_rowId;
		// �޸�������Ϊ2��ִ�в������ӿ�
		((DrsBPTreeIndice*)m_index)->incIndexNum();
		m_index->recvInsertIndexEntries(session, temp, 0, 0);
		totalKeys++;
		freeRecord(temp);

		rangeScanForwardCheck(session, m_tblDef, index, NULL, NULL, totalKeys, true);

		m_db->getSessionManager()->freeSession(session);
		m_db->freeConnection(conn);
	}

	/********************* case 3 ***************************/
	{	// ��ǰһ��������������
		Connection *conn = m_db->getConnection(false);
		Session *session = m_db->getSessionManager()->allocSession("IndexRecoveryTestCase::testCPSTDMLInRecv", conn);
		MemoryContext *memoryContext = session->getMemoryContext();
		DrsIndex *index = m_index->getIndex(0);

		// �޸������ĸ���Ϊ1����Ȼ��ִ��ɾ��һ����¼�Ĳ���
		((DrsBPTreeIndice*)m_index)->decIndexNum();
		char name[50];
		sprintf(name, "Kenneth Tse Jr. %d \0", 0);
		Record *record = createRecordRedundant(m_tblDef, 0, 0, name, 0 + ((u64)0 << 32), (u32)(-1) - 0);
		record->m_rowId = rowId;
		session->startTransaction(TXN_DELETE, m_tblDef->m_id);
		EXCPT_OPER(m_index->deleteIndexEntries(session, record, NULL));
		session->endTransaction(true);
		((DrsBPTreeIndice*)m_index)->incIndexNum();
		m_index->recvDeleteIndexEntries(session, record, 0, 0);
		freeRecord(record);

		totalKeys--;
		rangeScanForwardCheck(session, m_tblDef, index, NULL, NULL, totalKeys, true);

		m_index->close(m_session, true);
		delete m_index;
		DrsIndice::drop(m_path);
		m_index = NULL;
		closeSmallHeap(m_heap);
		DrsHeap::drop(HEAP_NAME);
		m_heap = NULL;
		delete m_tblDef;
		m_tblDef = NULL;
		TableDef::drop(TBLDEF_NAME);
		m_db->getSessionManager()->freeSession(session);
		m_db->freeConnection(conn);
	}

	/********************* case 4 ***************************/
	{
		m_tblDef = createSmallTableDef();
		m_heap = createSmallHeap(m_tblDef);
		totalKeys = buildHeap(m_heap, m_tblDef, 1, 1);
		createIndex(m_path, m_heap, m_tblDef);

		Connection *conn = m_db->getConnection(false);
		Session *session = m_db->getSessionManager()->allocSession("IndexRecoveryTestCase::testCPSTDMLInRecv", conn);
		MemoryContext *memoryContext = session->getMemoryContext();
		DrsIndex *index = m_index->getIndex(0);
		RowId rowId = 8192;

		// �޸������ĸ���Ϊ1����Ȼ��ִ�в���һ����¼�Ĳ���
		((DrsBPTreeIndice*)m_index)->decIndexNum();

		SubRecord *before, *after;
		u64 oldKey = HOLE, newKey = 1 + HOLE;
		before = IndexKey::allocSubRecordRED(memoryContext, m_tblDef->m_maxRecSize);

		u16 *columns = new u16[m_tblDef->m_numCols];
		for (int k = 0; k < m_tblDef->m_numCols; k++)
			columns[k] = (u16)k;

		before->m_numCols = m_tblDef->m_numCols;
		before->m_columns = columns;

		Record *oldrecord;

		// �����¼�ֵ
		char name[50];
		sprintf(name, "Kenneth Tse Jr. %d \0", oldKey);
		after = createUpdateAfterRecordRedundant1(m_tblDef, rowId, newKey);

		// ����ɼ�ֵ
		oldrecord = createRecord(m_tblDef, rowId, oldKey, name, oldKey + ((u64)oldKey << 32), (u32)(-1) - (u32)oldKey);
		oldrecord->m_rowId = rowId;
		before->m_size = oldrecord->m_size;
		before->m_data = oldrecord->m_data;
		before->m_rowId = oldrecord->m_rowId;

		session->startTransaction(TXN_UPDATE, m_tblDef->m_id);
		// ���и���
		uint dupIndex = (uint)-1;
		CPPUNIT_ASSERT(m_index->updateIndexEntries(session, before, after, false, &dupIndex));
		session->endTransaction(true);
		CPPUNIT_ASSERT(dupIndex == (uint)-1);

		// �޸�������Ϊ2��ִ�в�����½ӿ�
		((DrsBPTreeIndice*)m_index)->incIndexNum();
		m_index->recvUpdateIndexEntries(session, before, after, 0, 0, true);

		// �������
		freeSubRecord(after);
		freeRecord(oldrecord);
		delete[] columns;

		// ��֤����0��������
		CPPUNIT_ASSERT(fetchByKeyFromIndex0(session, index, m_tblDef, newKey, HOLE + ((u64)HOLE << 32), 8192));
		rangeScanForwardCheck(session, m_tblDef, index, NULL, NULL, totalKeys, true);

		m_index->close(m_session, true);
		delete m_index;
		DrsIndice::drop(m_path);
		m_index = NULL;
		closeSmallHeap(m_heap);
		DrsHeap::drop(HEAP_NAME);
		m_heap = NULL;
		delete m_tblDef;
		m_tblDef = NULL;
		TableDef::drop(TBLDEF_NAME);
		m_db->getSessionManager()->freeSession(session);
		m_db->freeConnection(conn);
	}

	/********************* case 5 ***************************/
	{
		m_tblDef = createSmallTableDef();
		m_heap = createSmallHeap(m_tblDef);
		totalKeys = buildHeap(m_heap, m_tblDef, 1, 1);
		createIndex(m_path, m_heap, m_tblDef);

		Connection *conn = m_db->getConnection(false);
		Session *session = m_db->getSessionManager()->allocSession("IndexRecoveryTestCase::testCPSTDMLInRecv", conn);
		MemoryContext *memoryContext = session->getMemoryContext();
		DrsIndex *index = m_index->getIndex(0);
		RowId rowId = 8192;

		// �޸������ĸ���Ϊ1����Ȼ��ִ�в���һ����¼�Ĳ���
		((DrsBPTreeIndice*)m_index)->decIndexNum();

		SubRecord *before, *after;
		u64 oldKey = HOLE, newKey = 1 + HOLE;
		before = IndexKey::allocSubRecordRED(memoryContext, m_tblDef->m_maxRecSize);

		u16 *columns = new u16[m_tblDef->m_numCols];
		for (int k = 0; k < m_tblDef->m_numCols; k++)
			columns[k] = (u16)k;

		before->m_numCols = m_tblDef->m_numCols;
		before->m_columns = columns;

		Record *oldrecord;

		// �����¼�ֵ
		char name[50];
		sprintf(name, "Kenneth Tse Jr. %d \0", oldKey);
		after = createUpdateAfterRecordRedundant1(m_tblDef, rowId, newKey);

		// ����ɼ�ֵ
		oldrecord = createRecord(m_tblDef, rowId, oldKey, name, oldKey + ((u64)oldKey << 32), (u32)(-1) - (u32)oldKey);
		oldrecord->m_rowId = rowId;
		before->m_size = oldrecord->m_size;
		before->m_data = oldrecord->m_data;
		before->m_rowId = oldrecord->m_rowId;

		session->startTransaction(TXN_UPDATE, m_tblDef->m_id);
		// ���и���
		uint dupIndex = (uint)-1;
		CPPUNIT_ASSERT(m_index->updateIndexEntries(session, before, after, false, &dupIndex));
		session->endTransaction(true);
		CPPUNIT_ASSERT(dupIndex == (uint)-1);

		// �޸�������Ϊ2���Եڶ�����������һ��ɾ��������ʵ������ɾ��
		((DrsBPTreeIndice*)m_index)->incIndexNum();

		IndexDef *indexDef = m_tblDef->m_indice[0];
		//	ɾ���������ݣ�ģ���������ڸ���ֻֻ����ɾ��������״̬
		uint count = (uint)oldKey;
		sprintf(name, "Kenneth Tse Jr. %d \0", count);
		Record *deleterecord = createRecord(m_tblDef, rowId, count, name, count + ((u64)count << 32), (u32)(-1) - count);
		deleterecord->m_format = REC_REDUNDANT;
		bool keyNeedCompress = RecordOper::isFastCCComparable(m_tblDef, indexDef, indexDef->m_numCols, indexDef->m_columns);
		SubRecord *deletekey = IndexKey::allocSubRecord(memoryContext, keyNeedCompress, deleterecord, NULL, m_tblDef, indexDef);
		index->del(session, deletekey);

		// ִ�и��µ��в�������Ĳ������
		uint i = (uint)newKey;
		sprintf(name, "Kenneth Tse Jr. %d \0", i);
		Record *insertrecord = createRecord(m_tblDef, rowId, newKey, name, count + ((u64)count << 32), (u32)(-1) - count);
		insertrecord->m_format = REC_REDUNDANT;
//		SubRecord *insertkey = IndexKey::allocSubRecord(memoryContext, keyNeedCompress, insertrecord, NULL, m_tblDef, indexDef);
		SubRecord insertkey( REC_REDUNDANT, indexDef->m_numCols, indexDef->m_columns, insertrecord->m_data, m_tblDef->m_maxRecSize, insertrecord->m_rowId);

		NTSE_ASSERT(m_index->recvCompleteHalfUpdate(session, &insertkey, 0, 0, NULL, true) == 1);
		// TODO: ���Ժ�����������Ϊ0��NULL�����

		// �������
		freeSubRecord(after);
		freeRecord(oldrecord);
		delete[] columns;
		freeRecord(insertrecord);
		freeRecord(deleterecord);

		// ��֤����0��������
		CPPUNIT_ASSERT(fetchByKeyFromIndex0(session, index, m_tblDef, newKey, HOLE + ((u64)HOLE << 32), 8192));
		rangeScanForwardCheck(session, m_tblDef, index, NULL, NULL, totalKeys, true);

		m_index->close(m_session, true);
		delete m_index;
		DrsIndice::drop(m_path);
		m_index = NULL;
		closeSmallHeap(m_heap);
		DrsHeap::drop(HEAP_NAME);
		m_heap = NULL;
		delete m_tblDef;
		m_tblDef = NULL;
		TableDef::drop(TBLDEF_NAME);
		m_db->getSessionManager()->freeSession(session);
		m_db->freeConnection(conn);
	}
}

/**
 * ���Դ���������صĻָ�����
 * case1:	����������������һ������Ψһ��Լ�����ڶ���������Ψһ��Լ������֤���ݵ��д���Ψһ��Υ��
 *			�ڴ���������������֮�����ڵڶ�����������ΪΥ��Լ�������������ֻ��һ�����������ɹ�
 *			��δ�������������ݻ����ϣ�����ǰ�潨��������������־��Ӧ���ܱ�֤���ֻ��һ������������
 * case2:	��case1���ƣ�ֻ��redo������ʱ��������ͷҳ���λͼҳ�Ѿ���ǰ�洴����������ִ��֮���״̬��
 *			Ϊ���ǲ����������ļ����ݲ���ȷ��ʱ���ܹ���ȷ����ָ�֮���ڴ浱���������������
 * case3:	�����������������������ݣ������һ������������־�����ڽ��ڶ���������������undo��ģ��ڶ�����������δ���ϵͳ�����ĳ���
 *			����undoCreateIndex�ӿڣ�֮��ָ����ݵ����ݣ�����undoCreateIndexд�Ĳ�����־��ִ��redoCPSTCreateIndex
 */
void IndexRecoveryTestCase::testCreateIndexRecovery() {
	// ���������������̵Ļָ������Ȳ���Υ������Ψһ��Լ��֮��������־����redo����ʱ����������ȫ����
	{
		uint totalKeys, items;
		uint step = 1;
		m_tblDef = createSmallTableDef();
		m_heap = createSmallHeap(m_tblDef);
		items = totalKeys = buildHeap(m_heap, m_tblDef, DATA_SCALE / 10, step);
		{	// ȷ�����ݵ������ظ���
			int dup = HOLE + totalKeys - 1;
			char name[50];
			RowId rid;
			sprintf(name, "Kenneth Tse Jr. %d \0", dup);
			Record *record = createRecord(m_tblDef, 0, dup, name, dup + ((u64)dup << 32), (u32)(-1) - dup);
			EXCPT_OPER(rid = m_heap->insert(m_session, record, NULL));
			rowids[totalKeys] = rid;
			items++;
			totalKeys++;
			freeRecord(record);
		}

		File oldindexfile(m_path);
		oldindexfile.remove();
		DrsIndice::create(m_path, m_tblDef);

		u64 startLsn = m_session->getLastLsn();

		// �ر����ݿⱸ�������ļ��������ļ���Ȼ�����´�
		closeEnv();

		// ���ݵ�ǰ״̬���ݺ������ļ�
		backupHeapFile(HEAP_NAME, HEAP_BACKUP_NAME);
		backupHeapFile(INDEX_NAME, INDEX_BACKUP_NAME);
		backupTblDefFile(TBLDEF_NAME, TBLDEF_BACKUP_NAME);

		openEnv();

		// ��Ҫ����������������������
		m_session->startTransaction(TXN_ADD_INDEX, m_tblDef->m_id);

		// �����������ظ�����������������ȫ�����ˡ�
		try {	// ��������Ψһ��Լ����������������ᶪ������
			m_index->createIndexPhaseOne(m_session, m_tblDef->m_indice[0], m_tblDef, m_heap);
			m_index->createIndexPhaseTwo(m_tblDef->m_indice[0]);
			m_index->createIndexPhaseOne(m_session, m_tblDef->m_indice[1], m_tblDef, m_heap);
			m_index->createIndexPhaseTwo(m_tblDef->m_indice[1]);
		} catch (NtseException &e) {
			cout << e.getMessage() << endl;
		}

		// �ر�����
		m_session->endTransaction(true);

		// �ر����ݿ�ظ�ԭʼ�ļ������´�׼���ָ�����
		closeEnv();

		myrestoreHeapFile(HEAP_BACKUP_NAME, HEAP_NAME);
		myrestoreHeapFile(INDEX_BACKUP_NAME, INDEX_NAME);

		openEnv();

		IndexRecoveryManager manager(m_db, m_heap, m_tblDef, m_index);
		uint count = 0;
		// ���ݼ�¼��LSN����ȡ��־������������������
		LogScanHandle *logHdl = m_db->getTxnlog()->beginScan(startLsn, Txnlog::MAX_LSN);
		while (m_db->getTxnlog()->getNext(logHdl)) {
			LogEntry *logEntry = (LogEntry*)logHdl->logEntry();
			//redo����־
			manager.redoALog(m_session, logEntry, logHdl->curLsn());
			count++;
		}
		m_db->getTxnlog()->endScan(logHdl);

		// ��֤�����ļ�״̬
		CPPUNIT_ASSERT(m_index->getIndexNum() == 1);
		// ��ȡͷҳ���λͼҳ��֤�����ȷ
		PageHandle *headerPageHandle = GET_PAGE(m_session, ((DrsBPTreeIndice*)m_index)->getFileDesc(), PAGE_INDEX, 0, Exclusived, m_index->getDBObjStats(), NULL);
		IndexHeaderPage *headerPage = (IndexHeaderPage*)(headerPageHandle->getPage());
		CPPUNIT_ASSERT(headerPage->m_indexNum == 1);
		CPPUNIT_ASSERT(headerPage->m_indexIds[0] == 1);
		CPPUNIT_ASSERT(headerPage->m_indexIds[1] == 0);
		CPPUNIT_ASSERT(headerPage->m_indexUniqueId == 3);
		m_session->releasePage(&headerPageHandle);
		PageHandle *bmPageHandle = GET_PAGE(m_session, ((DrsBPTreeIndice*)m_index)->getFileDesc(), PAGE_INDEX, 1, Exclusived, m_index->getDBObjStats(), NULL);
		IndexFileBitMapPage *bmPage = (IndexFileBitMapPage*)bmPageHandle->getPage();
		byte *start = (byte*)bmPage + sizeof(Page);
		byte *end = start + 10;
		bool firstIndexExists = false;
		while (start != end) {
			CPPUNIT_ASSERT(*start != 0x02);
			if (*start == 0x01)
				firstIndexExists = true;
			++start;
		}
		CPPUNIT_ASSERT(firstIndexExists);
		m_session->releasePage(&bmPageHandle);

		// �������ԣ��رջ���
		m_index->close(m_session, true);
		delete m_index;
		DrsIndice::drop(m_path);
		m_index = NULL;
		closeSmallHeap(m_heap);
		DrsHeap::drop(HEAP_NAME);
		m_heap = NULL;
		delete m_tblDef;
		m_tblDef = NULL;
		TableDef::drop(TBLDEF_NAME);
	}

	{	// �����ڴ�������ʧ��֮ǰ�������ļ�ͷҳ���λͼҳ���Ѿ���д�뵽���̣������ָ���ʱ��������Ч���������ݼ�¼��ͷҳ��
		uint totalKeys, items;
		uint step = 1;
		m_tblDef = createSmallTableDef();
		m_heap = createSmallHeap(m_tblDef);
		items = totalKeys = buildHeap(m_heap, m_tblDef, DATA_SCALE / 10, step);
		{	// ȷ�����ݵ������ظ���
			int dup = HOLE + totalKeys - 1;
			char name[50];
			RowId rid;
			sprintf(name, "Kenneth Tse Jr. %d \0", dup);
			Record *record = createRecord(m_tblDef, 0, dup, name, dup + ((u64)dup << 32), (u32)(-1) - dup);
			EXCPT_OPER(rid = m_heap->insert(m_session, record, NULL));
			rowids[totalKeys] = rid;
			items++;
			totalKeys++;
			freeRecord(record);
		}

		File oldindexfile(m_path);
		oldindexfile.remove();
		DrsIndice::create(m_path, m_tblDef);

		u64 startLsn = m_session->getLastLsn();

		// �ر����ݿⱸ�������ļ��������ļ���Ȼ�����´�
		closeEnv();

		// ���ݵ�ǰ״̬���ݺ������ļ�
		backupHeapFile(HEAP_NAME, HEAP_BACKUP_NAME);
		backupHeapFile(INDEX_NAME, INDEX_BACKUP_NAME);
		backupTblDefFile(TBLDEF_NAME, TBLDEF_BACKUP_NAME);

		openEnv();

		Connection *conn = m_db->getConnection(false);
		Session *session = m_db->getSessionManager()->allocSession("IndexOperationTestCase::testUpdate", conn);
		MemoryContext *memoryContext = session->getMemoryContext();
		BigProject bigProject("I need create lots of indices", m_db, conn, m_heap, m_tblDef, m_index, 0);

		bigProject.enableSyncPoint(SP_IDX_ALLOCED_ROOT_PAGE);
		bigProject.start();
		bigProject.joinSyncPoint(SP_IDX_ALLOCED_ROOT_PAGE);	// ����������������ע�ڶ�����������
		bigProject.notifySyncPoint(SP_IDX_ALLOCED_ROOT_PAGE);
		bigProject.joinSyncPoint(SP_IDX_ALLOCED_ROOT_PAGE);

		// ��ʱ�ض��Ǵ��������������ĸ�ҳ����Ϣ�������߳��ڲ��Ѿ��������ļ�ͷҳ��ҳ��λͼҳ����
		// ��������������ҳ�������
		byte headerbkPage[Limits::PAGE_SIZE];
		byte bitmapbkPage[Limits::PAGE_SIZE];

		PageHandle *headerHandle = GET_PAGE(session, ((DrsBPTreeIndice*)m_index)->getFileDesc(), PAGE_INDEX, 0, Exclusived, m_index->getDBObjStats(), NULL);
		PageHandle *bitmapHandle = GET_PAGE(session, ((DrsBPTreeIndice*)m_index)->getFileDesc(), PAGE_INDEX, 1, Exclusived, m_index->getDBObjStats(), NULL);

		byte *start1 = (byte*)headerHandle->getPage();
		byte *start2 = (byte*)bitmapHandle->getPage();

		memcpy(headerbkPage, start1, Limits::PAGE_SIZE);
		memcpy(bitmapbkPage, start2, Limits::PAGE_SIZE);

		session->releasePage(&headerHandle);
		session->releasePage(&bitmapHandle);

		bigProject.disableSyncPoint(SP_IDX_ALLOCED_ROOT_PAGE);
		bigProject.notifySyncPoint(SP_IDX_ALLOCED_ROOT_PAGE);
		bigProject.join();

		m_db->getSessionManager()->freeSession(session);
		m_db->freeConnection(conn);
		// ���������߳̽�������ʼ�ָ�����
		closeEnv();

		// ���ݵ�ǰ״̬���ݺ������ļ�
		backupHeapFile(HEAP_NAME, HEAP_BACKUP_NAME);
		backupHeapFile(INDEX_NAME, INDEX_BACKUP_NAME);
		backupTblDefFile(TBLDEF_NAME, TBLDEF_BACKUP_NAME);

		// �ѱ��ݵ�����ҳ�棬д������ļ�
		File file(INDEX_NAME);
		CPPUNIT_ASSERT(file.open(false) == File::E_NO_ERROR);
		CPPUNIT_ASSERT(file.write(0, Limits::PAGE_SIZE, headerbkPage) == File::E_NO_ERROR);
		CPPUNIT_ASSERT(file.write(Limits::PAGE_SIZE, Limits::PAGE_SIZE, bitmapbkPage) == File::E_NO_ERROR);
		CPPUNIT_ASSERT(file.close() == File::E_NO_ERROR);

		// ���´��ļ���ȡ������׼���ָ�
		openEnv();

		IndexRecoveryManager manager(m_db, m_heap, m_tblDef, m_index);

		uint count = 0;
		// ���ݼ�¼��LSN����ȡ��־������������������
		LogScanHandle *logHdl = m_db->getTxnlog()->beginScan(startLsn, Txnlog::MAX_LSN);
		while (m_db->getTxnlog()->getNext(logHdl)) {
			LogEntry *logEntry = (LogEntry*)logHdl->logEntry();
			//redo����־
			manager.redoALog(m_session, logEntry, logHdl->curLsn());
			count++;
		}
		m_db->getTxnlog()->endScan(logHdl);

		// ��֤�����ļ�״̬
		CPPUNIT_ASSERT(m_index->getIndexNum() == 1);
		// ��ȡͷҳ���λͼҳ��֤�����ȷ
		PageHandle *headerPageHandle = GET_PAGE(m_session, ((DrsBPTreeIndice*)m_index)->getFileDesc(), PAGE_INDEX, 0, Exclusived, m_index->getDBObjStats(), NULL);
		IndexHeaderPage *headerPage = (IndexHeaderPage*)(headerPageHandle->getPage());
		CPPUNIT_ASSERT(headerPage->m_indexNum == 1);
		CPPUNIT_ASSERT(headerPage->m_indexIds[0] == 1);
		CPPUNIT_ASSERT(headerPage->m_indexIds[1] == 0);
		CPPUNIT_ASSERT(headerPage->m_indexUniqueId == 3);
		m_session->releasePage(&headerPageHandle);
		PageHandle *bmPageHandle = GET_PAGE(m_session, ((DrsBPTreeIndice*)m_index)->getFileDesc(), PAGE_INDEX, 1, Exclusived, m_index->getDBObjStats(), NULL);
		IndexFileBitMapPage *bmPage = (IndexFileBitMapPage*)bmPageHandle->getPage();
		byte *start = (byte*)bmPage + sizeof(Page);
		byte *end = start + 10;
		bool firstIndexExists = false;
		while (start != end) {
			CPPUNIT_ASSERT(*start != 0x02);
			if (*start == 0x01)
				firstIndexExists = true;
			++start;
		}
		CPPUNIT_ASSERT(firstIndexExists);
		m_session->releasePage(&bmPageHandle);

		// �������ԣ��رջ���
		m_index->close(m_session, true);
		delete m_index;
		DrsIndice::drop(m_path);
		m_index = NULL;
		closeSmallHeap(m_heap);
		DrsHeap::drop(HEAP_NAME);
		m_heap = NULL;
		delete m_tblDef;
		m_tblDef = NULL;
		TableDef::drop(TBLDEF_NAME);
	}

	{	// ����undoCreateIndex�ӿں�redoCPSTCreateIndex�ӿ�
		uint totalKeys, items;
		uint step = 1;
		m_tblDef = createSmallTableDef();
		m_heap = createSmallHeap(m_tblDef);
		items = totalKeys = buildHeap(m_heap, m_tblDef, DATA_SCALE / 10, step);

		File oldindexfile(m_path);
		oldindexfile.remove();
		DrsIndice::create(m_path, m_tblDef);

		m_index = DrsIndice::open(m_db, m_session, INDEX_NAME, m_tblDef, NULL);

		Connection *conn = m_db->getConnection(false);
		Session *session = m_db->getSessionManager()->allocSession("IndexOperationTestCase::testUpdate", conn);
		MemoryContext *memoryContext = session->getMemoryContext();

		session->startTransaction(TXN_ADD_INDEX, m_tblDef->m_id);

		try {
			m_index->createIndexPhaseOne(session, m_tblDef->m_indice[0], m_tblDef, m_heap);
			m_index->createIndexPhaseTwo(m_tblDef->m_indice[0]);
			m_index->createIndexPhaseOne(session, m_tblDef->m_indice[1], m_tblDef, m_heap);
			m_index->createIndexPhaseTwo(m_tblDef->m_indice[1]);
		} catch (NtseException &e) {
			cout << e.getMessage() << endl;
		}

		session->endTransaction(true);

		u64 startLsn = session->getLastLsn();

		{	// ����һ�����ݣ����ڲ���redoCPSTCreateIndex�ӿ�
			m_db->getSessionManager()->freeSession(session);
			m_db->freeConnection(conn);
			// ���������߳̽�������ʼ�ָ�����
			closeEnv();

			// ���ݵ�ǰ״̬���ݺ������ļ�
			backupHeapFile(HEAP_NAME, HEAP_BACKUP_NAME);
			backupHeapFile(INDEX_NAME, INDEX_BACKUP_NAME);
			backupTblDefFile(TBLDEF_NAME, TBLDEF_BACKUP_NAME);

			openEnv();

			conn = m_db->getConnection(false);
			session = m_db->getSessionManager()->allocSession("IndexOperationTestCase::testUpdate", conn);
		}

		// ��Ҫ����һ������������־���ڻ���
		byte savedIndexId = 0x02;

		session->startTransaction(TXN_ADD_INDEX, m_tblDef->m_id);

		m_index->undoCreateIndex(session, 0, &savedIndexId, 1, true);

		session->endTransaction(true);

		// ��֤���������׶���
		CPPUNIT_ASSERT(m_index->getIndexNum() == 1);
		// ��ȡͷҳ���λͼҳ��֤�����ȷ
		PageHandle *headerPageHandle = GET_PAGE(m_session, ((DrsBPTreeIndice*)m_index)->getFileDesc(), PAGE_INDEX, 0, Exclusived, m_index->getDBObjStats(), NULL);
		IndexHeaderPage *headerPage = (IndexHeaderPage*)(headerPageHandle->getPage());
		CPPUNIT_ASSERT(headerPage->m_indexNum == 1);
		CPPUNIT_ASSERT(headerPage->m_indexIds[0] == 1);
		CPPUNIT_ASSERT(headerPage->m_indexIds[1] == 0);
		CPPUNIT_ASSERT(headerPage->m_indexUniqueId == 3);
		m_session->releasePage(&headerPageHandle);
		PageHandle *bmPageHandle = GET_PAGE(m_session, ((DrsBPTreeIndice*)m_index)->getFileDesc(), PAGE_INDEX, 1, Exclusived, m_index->getDBObjStats(), NULL);
		IndexFileBitMapPage *bmPage = (IndexFileBitMapPage*)bmPageHandle->getPage();
		byte *start = (byte*)bmPage + sizeof(Page);
		byte *end = start + 10;
		bool firstIndexExists = false;
		while (start != end) {
			CPPUNIT_ASSERT(*start != 0x02);
			if (*start == 0x01)
				firstIndexExists = true;
			++start;
		}
		CPPUNIT_ASSERT(firstIndexExists);
		m_session->releasePage(&bmPageHandle);

		// �ر����ݿ�ظ�ԭʼ�ļ������´�׼���ָ�����
		closeEnv();

		myrestoreHeapFile(HEAP_BACKUP_NAME, HEAP_NAME);
		myrestoreHeapFile(INDEX_BACKUP_NAME, INDEX_NAME);

		openEnv();

		conn = m_db->getConnection(false);
		session = m_db->getSessionManager()->allocSession("IndexOperationTestCase::testUpdate", conn);
		// ���ݼ�¼��LSN����ȡ��־���ҵ�������־����
		bool redoneCPST = false;
		LogScanHandle *logHdl = m_db->getTxnlog()->beginScan(startLsn, Txnlog::MAX_LSN);
		while (m_db->getTxnlog()->getNext(logHdl)) {
			LogEntry *logEntry = (LogEntry*)logHdl->logEntry();
			if (logEntry->m_logType == LOG_IDX_ADD_INDEX_CPST) {
				m_index->redoCpstCreateIndex(session, logHdl->curLsn(), logEntry->m_data, logEntry->m_size);
				redoneCPST = true;
			}
		}
		m_db->getTxnlog()->endScan(logHdl);
		CPPUNIT_ASSERT(redoneCPST);

		// ��֤���������׶���
		CPPUNIT_ASSERT(m_index->getIndexNum() == 1);
		// ��ȡͷҳ���λͼҳ��֤�����ȷ
		headerPageHandle = GET_PAGE(m_session, ((DrsBPTreeIndice*)m_index)->getFileDesc(), PAGE_INDEX, 0, Exclusived, m_index->getDBObjStats(), NULL);
		headerPage = (IndexHeaderPage*)(headerPageHandle->getPage());
		CPPUNIT_ASSERT(headerPage->m_indexNum == 1);
		CPPUNIT_ASSERT(headerPage->m_indexIds[0] == 1);
		CPPUNIT_ASSERT(headerPage->m_indexIds[1] == 0);
		CPPUNIT_ASSERT(headerPage->m_indexUniqueId == 3);
		m_session->releasePage(&headerPageHandle);
		bmPageHandle = GET_PAGE(m_session, ((DrsBPTreeIndice*)m_index)->getFileDesc(), PAGE_INDEX, 1, Exclusived, m_index->getDBObjStats(), NULL);
		bmPage = (IndexFileBitMapPage*)bmPageHandle->getPage();
		start = (byte*)bmPage + sizeof(Page);
		end = start + 10;
		firstIndexExists = false;
		while (start != end) {
			CPPUNIT_ASSERT(*start != 0x02);
			if (*start == 0x01)
				firstIndexExists = true;
			++start;
		}
		CPPUNIT_ASSERT(firstIndexExists);
		m_session->releasePage(&bmPageHandle);

		// �������ԣ��رջ���
		m_index->close(m_session, true);
		delete m_index;
		DrsIndice::drop(m_path);
		m_index = NULL;
		closeSmallHeap(m_heap);
		DrsHeap::drop(HEAP_NAME);
		m_heap = NULL;
		delete m_tblDef;
		m_tblDef = NULL;
		TableDef::drop(TBLDEF_NAME);

		m_db->getSessionManager()->freeSession(session);
		m_db->freeConnection(conn);
	}
}


/**
 * ����DML�����Ļָ���redo��ز���
 * case1:	����redo������ȷ��
 *			���Ƚ���һ�������������ݵ�����������һ�ݵ�ǰ�������ļ����ݱ���
 *			��ʼִ�����ɲ����ɾ���������������������־
 *			�������ļ��ָ���δִ��DML����֮ǰ�������в�����־redo��������
 *			��֤���ʱ������redo���ɹ�������������ȷ
 */
void IndexRecoveryTestCase::testDMLRecoveryRedo() {
	IndexRecoveryManager *manager;
	// ģ�´��������ָ���һ�����������������������Ƚ�������DML������redo����DML��־��
	uint testScale = DATA_SCALE / 4;
	
	// Part1: ���������������������ݣ����浱ǰ״̬�������������ļ�
	uint totalKeys, items;
	uint step = 2;
	m_tblDef = createSmallTableDef();
	m_heap = createSmallHeap(m_tblDef);
	items = totalKeys = buildHeap(m_heap, m_tblDef, testScale, step);
	File oldindexfile(m_path);
	oldindexfile.remove();
	DrsIndice::create(m_path, m_tblDef);

	u16 tableId = m_tblDef->m_id;

	createIndex(m_path, m_heap, m_tblDef);

	u64 startLsn = m_session->getLastLsn();

	// �ر����ݿⱸ�������ļ��������ļ���Ȼ�����´�
	closeEnv();

	// ���ݵ�ǰ״̬���ݺ������ļ�
	backupHeapFile(HEAP_NAME, HEAP_BACKUP_NAME);
	backupHeapFile(INDEX_NAME, INDEX_BACKUP_NAME);
	backupTblDefFile(TBLDEF_NAME, TBLDEF_BACKUP_NAME);


	// Part2:�ڽ��������Ļ����ϣ��������������Ϊ���ǵõ������������־
	openEnv();

	Connection *conn = m_db->getConnection(false);
	Session *session = m_db->getSessionManager()->allocSession("IndexRecoveryTestCase::testDMLRecovery", conn);
	MemoryContext *memoryContext = session->getMemoryContext();

	{	// ��������������
		Record *insertrecord = IndexKey::allocRecord(memoryContext, m_tblDef->m_maxRecSize);
		RowId deleteRowId =  INVALID_ROW_ID;
		for (int i = HOLE + 1; i < testScale + HOLE; i += step) {
			Record *temp = insertSpecifiedRecord(session, memoryContext, i);
			totalKeys++;

			freeRecord(temp);
		}
	}

	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(conn);
	closeEnv();

	// Part3:��ԭ�ļ����ս����������ĳ�������ȡ��־���ݣ�����֮ǰ�Ĳ�����־
	myrestoreHeapFile(HEAP_BACKUP_NAME, HEAP_NAME);
	myrestoreHeapFile(INDEX_BACKUP_NAME, INDEX_NAME);

	openEnv();

	conn = m_db->getConnection(false);
	session = m_db->getSessionManager()->allocSession("IndexRecoveryTestCase::testDMLRecovery", conn);
	memoryContext = session->getMemoryContext();

	// ��֤�����ṹ����
	checkIndex(session, memoryContext, totalKeys);

	// ����ǰ������DML��־
	manager = new IndexRecoveryManager(m_db, m_heap, m_tblDef, m_index);
	manager->redoFromSpecifiedLogs(session, startLsn);
	delete manager;

	// Part4:��֤�����ṹ����
	uint count = 0;
	checkIndex(session, memoryContext, totalKeys);

	DrsIndex *index = m_index->getIndex(1);

	// ���������������֤����
	//SubRecord *idxKey = IndexKey::allocSubRecordRED(memoryContext, m_tblDef->m_maxRecSize);
	SubRecord *idxKey = IndexKey::allocSubRecord(memoryContext, m_tblDef->getIndexDef(1), KEY_PAD);
	IndexScanHandle *indexHandle;
	IndexDef *indexDef = m_tblDef->m_indice[1];

	idxKey->m_columns = m_tblDef->m_indice[1]->m_columns;
	idxKey->m_numCols = m_tblDef->m_indice[1]->m_numCols;
 
	SubToSubExtractor *extractor = SubToSubExtractor::createInst(session->getMemoryContext(), m_tblDef, indexDef,
		indexDef->m_numCols, indexDef->m_columns, idxKey->m_numCols, idxKey->m_columns, KEY_COMPRESS, KEY_PAD, 1000);

	indexHandle = index->beginScan(session, NULL, false, true, None, NULL, extractor);
	count = 0;
	while (index->getNext(indexHandle, idxKey))
		count++;
	index->endScan(indexHandle);
	CPPUNIT_ASSERT(count == totalKeys);
	indexHandle = index->beginScan(session, NULL, true, true, None, NULL, extractor);
	count = 0;
	while (index->getNext(indexHandle, idxKey))
		count++;
	index->endScan(indexHandle);
	CPPUNIT_ASSERT(count == totalKeys);

	u64 startLsn1 = m_db->getTxnlog()->tailLsn();	// �����������LSN��redoʹ��


	// �ر����ݿⱸ�������ļ��������ļ���Ȼ�����´�
	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(conn);
	closeEnv();

	// ���ݵ�ǰ״̬���ݺ������ļ�
	backupHeapFile(HEAP_NAME, HEAP_BACKUP_NAME);
	backupHeapFile(INDEX_NAME, INDEX_BACKUP_NAME);
	backupTblDefFile(TBLDEF_NAME, TBLDEF_BACKUP_NAME);


	openEnv();

	conn = m_db->getConnection(false);
	session = m_db->getSessionManager()->allocSession("IndexRecoveryTestCase::testDMLRecovery", conn);
	memoryContext = session->getMemoryContext();
	// ���˲��������־��redo��������
	// ���²�����Ҫ����ɾ��������redo
	// Part 5:ɾ��������õ�ɾ����־
//	SubRecord *record = IndexKey::allocSubRecordRED(memoryContext, m_tblDef->m_maxRecSize);
	SubRecord *record = IndexKey::allocSubRecord(memoryContext, m_tblDef->getIndexDef(1), KEY_PAD);
	record->m_columns = m_tblDef->m_indice[1]->m_columns;
	record->m_numCols = m_tblDef->m_indice[1]->m_numCols;
	RowId rowId;
	SubRecord *findKey;
	Record *heapRec = IndexKey::allocRecord(memoryContext, m_tblDef->m_maxRecSize);
	index = m_index->getIndex(1);
	indexDef = m_tblDef->getIndexDef(1);

	SubToSubExtractor *extractor1 = SubToSubExtractor::createInst(session->getMemoryContext(), m_tblDef, indexDef,
		m_tblDef->m_indice[1]->m_numCols, m_tblDef->m_indice[1]->m_columns, record->m_numCols, record->m_columns, KEY_COMPRESS, KEY_PAD, 1);

	uint foundKeys = 0;
	for (int i = HOLE; i < testScale + HOLE; i += step) {
		findKey = makeFindKeyWhole(i, m_heap, m_tblDef);
		findKey->m_rowId = INVALID_ROW_ID;
		bool found = index->getByUniqueKey(session, findKey, None, &rowId, record, NULL, extractor1);
		if (found) {	// ��ȡ���¼��ִ��ɾ������
			CPPUNIT_ASSERT(record->m_rowId != INVALID_ROW_ID);

			foundKeys++;

			heapRec->m_format = REC_FIXLEN;
			m_heap->getRecord(session, record->m_rowId, heapRec);
			heapRec->m_format = REC_REDUNDANT;

			session->startTransaction(TXN_DELETE, tableId);
			EXCPT_OPER(m_index->deleteIndexEntries(session, heapRec, NULL));
			session->endTransaction(true);
			totalKeys--;
		}
		freeSubRecord(findKey);
	}

	CPPUNIT_ASSERT(foundKeys != 0);

	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(conn);
	closeEnv();

	// Part6:��ԭ�ļ����������ʱ������״̬����ȡ��־���ݣ�����֮ǰ��ɾ����־
	myrestoreHeapFile(HEAP_BACKUP_NAME, HEAP_NAME);
	myrestoreHeapFile(INDEX_BACKUP_NAME, INDEX_NAME);

	openEnv();

	conn = m_db->getConnection(false);
	session = m_db->getSessionManager()->allocSession("IndexRecoveryTestCase::testDMLRecovery", conn);
	memoryContext = session->getMemoryContext();

	// ����ǰ������DML��־
	manager = new IndexRecoveryManager(m_db, m_heap, m_tblDef, m_index);
	manager->redoFromSpecifiedLogs(session, startLsn1);
	delete manager;

	// Part7:��֤�����ṹ����
	checkIndex(session, memoryContext, totalKeys);

	index = m_index->getIndex(1);
	// ���������������֤����
//	idxKey = IndexKey::allocSubRecordRED(memoryContext, m_tblDef->m_maxRecSize);
	idxKey = IndexKey::allocSubRecord(memoryContext, m_tblDef->getIndexDef(1), KEY_PAD);
	idxKey->m_columns = m_tblDef->m_indice[1]->m_columns;
	idxKey->m_numCols = m_tblDef->m_indice[1]->m_numCols;

	SubToSubExtractor *extractor2 = SubToSubExtractor::createInst(session->getMemoryContext(), m_tblDef, m_tblDef->getIndexDef(1),
		m_tblDef->m_indice[1]->m_numCols, m_tblDef->m_indice[1]->m_columns, idxKey->m_numCols, idxKey->m_columns, KEY_COMPRESS, KEY_PAD, 1000);

	indexHandle = index->beginScan(session, NULL, true, true, None, NULL, extractor2);
	count = 0;
	while (index->getNext(indexHandle, idxKey))
		count++;
	index->endScan(indexHandle);
	CPPUNIT_ASSERT(count == totalKeys);

	indexHandle = index->beginScan(session, NULL, false, true, None, NULL, extractor2);
	count = 0;
	while (index->getNext(indexHandle, idxKey))
		count++;
	index->endScan(indexHandle);
	CPPUNIT_ASSERT(count == totalKeys);

	// ����redo���ԣ��رջ���
	m_index->close(m_session, true);
	delete m_index;
	DrsIndice::drop(m_path);
	m_index = NULL;
	closeSmallHeap(m_heap);
	DrsHeap::drop(HEAP_NAME);
	m_heap = NULL;
	delete m_tblDef;
	m_tblDef = NULL;
	TableDef::drop(TBLDEF_NAME);

	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(conn);
}

/*
 * ����DML�����Ļָ���undo��ز���
 * case1:	����undo������ȷ��
 *			���Ƚ���һ�������������ݵ�����
 *			��������ִ������DML���������������ɾ����ÿִ����һ�β��������ò����������־��undo
 *			��ʱ���൱��DML����û��ִ�У�����������һ��DML����ʹ��DML��������Ӧ��������
 *			�����������ִ�в��������undo���ԣ���ִ��ɾ��������udno���ԣ������������ɾ��
 *			��֤������ȷ��
 */
void IndexRecoveryTestCase::testDMLRecoveryUndo() {
	IndexRecoveryManager *manager = NULL;
	uint testScale = DATA_SCALE / 4;
	m_db->setCheckpointEnabled(false);
	if (!isEssentialOnly())
	{	// �ò��ֲ�����־��undo�ָ�
		// Part1:����һ��С����������½����������ɾ�����ݣ����ڲ���SMO�Ļָ�
		uint origSize = 30;
		uint totalKeys, items;
		uint step = 2;
		m_tblDef = createSmallTableDef();
		m_heap = createSmallHeap(m_tblDef);
		items = totalKeys = buildHeap(m_heap, m_tblDef, origSize, step);
		File oldindexfile(m_path);
		oldindexfile.remove();
		DrsIndice::create(m_path, m_tblDef);

		u16 tableId = m_tblDef->m_id;

		createIndex(m_path, m_heap, m_tblDef);

		u64 startLsn = m_db->getTxnlog()->tailLsn();;

		DrsIndex *index;

		manager = new IndexRecoveryManager(m_db, m_heap, m_tblDef, m_index);

		Connection *conn = m_db->getConnection(false);
		Session *session = m_db->getSessionManager()->allocSession("IndexRecoveryTestCase::testDMLRecovery", conn);
		MemoryContext *memoryContext = session->getMemoryContext();

		manager->setTxnType(TXN_INSERT);
		for (uint i = HOLE + 1; i < HOLE + origSize * 20; i += step) {
			// ����һ���¼�¼
			session->startTransaction(TXN_INSERT, tableId);
			startLsn = session->getLastLsn();
			session->endTransaction(true);
			Record *insertedRecord = insertSpecifiedRecord(session, memoryContext, i);
			u64 curLsn = session->getLastLsn();
			CPPUNIT_ASSERT(curLsn > startLsn);
			// undo�ղŵĲ��룬��֤undo��ȷ��
			manager->undoFromSpecifiedLogs(session, startLsn, curLsn);

			// ���undo�Ľ����֤��ȷ
			checkIndex(session, memoryContext, totalKeys);

			// ������һ�θղŵĲ��룬��֤������Ч
			bool successful;
			uint dupIndex;
			session->startTransaction(TXN_INSERT, tableId);
			EXCPT_OPER(successful = m_index->insertIndexEntries(session, insertedRecord, &dupIndex));
			session->endTransaction(true);
			CPPUNIT_ASSERT(successful);

			totalKeys++;

			freeRecord(insertedRecord);
		}

		// �������
		checkIndex(session, memoryContext, totalKeys);
		index = m_index->getIndex(1);
//		SubRecord *idxKey = IndexKey::allocSubRecordRED(memoryContext, m_tblDef->m_maxRecSize);
		SubRecord *idxKey = IndexKey::allocSubRecord(memoryContext, m_tblDef->getIndexDef(1), KEY_PAD);
		idxKey->m_columns = m_tblDef->m_indice[1]->m_columns;
		idxKey->m_numCols = m_tblDef->m_indice[1]->m_numCols;
		rangeScanBackwardCheck(session, m_tblDef, index, NULL, idxKey, totalKeys);
		rangeScanForwardCheck(session, m_tblDef, index, NULL, idxKey, totalKeys);


		// Part2:�ڸղŽ�������Ļ����ϣ�������ɾ�գ�ÿɾһ�����undo����
		manager->setTxnType(TXN_DELETE);
		Record *record = IndexKey::allocRecord(memoryContext, m_tblDef->m_maxRecSize);
//		SubRecord *findrecord = IndexKey::allocSubRecordRED(memoryContext, m_tblDef->m_maxRecSize);
		SubRecord *findrecord = IndexKey::allocSubRecord(memoryContext, m_tblDef->getIndexDef(1), KEY_PAD);
		findrecord->m_columns = m_tblDef->m_indice[1]->m_columns;
		findrecord->m_numCols = m_tblDef->m_indice[1]->m_numCols;

		IndexDef *indexDef = m_tblDef->m_indice[1];
		SubToSubExtractor *extractor = SubToSubExtractor::createInst(session->getMemoryContext(), m_tblDef, indexDef,
			m_tblDef->m_indice[1]->m_numCols, m_tblDef->m_indice[1]->m_columns, findrecord->m_numCols, findrecord->m_columns, KEY_COMPRESS, KEY_PAD, 1);

		for (uint i = HOLE; i < HOLE + origSize * 20; i++) {
			SubRecord *findKey = makeFindKeyWhole(i, m_heap, m_tblDef);
			findKey->m_rowId = INVALID_ROW_ID;
			RowId rowId;
			bool found = index->getByUniqueKey(session, findKey, None, &rowId, findrecord, NULL, extractor);
			if (found) {	// ������ڣ����в���
				record->m_format = REC_FIXLEN;
				CPPUNIT_ASSERT(m_heap->getRecord(session, rowId, record));
				record->m_format = REC_REDUNDANT;
				session->startTransaction(TXN_DELETE, tableId);
				startLsn = session->getLastLsn();
				m_index->deleteIndexEntries(session, record, NULL);
				session->endTransaction(true);

				u64 curLsn = session->getLastLsn();
				CPPUNIT_ASSERT(curLsn > startLsn);
				// undo�ղŵĲ��룬��֤undo��ȷ��
				manager->undoFromSpecifiedLogs(session, startLsn, curLsn);

				// ���undo�Ľ����֤��ȷ
				checkIndex(session, memoryContext, totalKeys);

				session->startTransaction(TXN_DELETE, tableId);
				m_index->deleteIndexEntries(session, record, NULL);
				session->endTransaction(true);
				totalKeys--;

				checkIndex(session, memoryContext, totalKeys);
				index = m_index->getIndex(1);
				CPPUNIT_ASSERT(rangeScanBackwardCheck(session, m_tblDef, index, NULL, idxKey, totalKeys));
				CPPUNIT_ASSERT(rangeScanForwardCheck(session, m_tblDef, index, NULL, idxKey, totalKeys));
			}

			freeSubRecord(findKey);
		}

		CPPUNIT_ASSERT(totalKeys == 0);

		// ������������
		checkIndex(session, memoryContext, totalKeys);
		index = m_index->getIndex(1);
		rangeScanBackwardCheck(session, m_tblDef, index, NULL, idxKey, totalKeys);
		rangeScanForwardCheck(session, m_tblDef, index, NULL, idxKey, totalKeys);

		m_index->close(m_session, true);
		delete m_index;
		DrsIndice::drop(m_path);
		m_index = NULL;
		closeSmallHeap(m_heap);
		DrsHeap::drop(HEAP_NAME);
		m_heap = NULL;
		delete m_tblDef;
		m_tblDef = NULL;
		TableDef::drop(TBLDEF_NAME);

		m_db->getSessionManager()->freeSession(session);
		m_db->freeConnection(conn);
	}

	if (manager != NULL)
		delete manager;
}


/*
 * ����DML�����Ļָ���redoCPST��ز���
 * case1:	��������Ҫ����redo������־�Ĳ���
 *			���̺�redoUndo�ǳ����ƣ�ֻ����undo����־�Ļ����ϣ���redoһ��undo����־�Լ�undo���̲����Ĳ�����־
 *			��ʱʵ���ϻ��ǽ�DML�޸Ĳ���undo���ˣ�Ȼ������һ��������Ч��DML����������һ��
 *
 * ע�⣬�ò�������Ϊ��������ܣ��ԶѵĲ����������лָ����ݡ���˻����϶Ѻ��������ǲ�һ�µ�
 */
void IndexRecoveryTestCase::testDMLRecoveryRedoCPST() {
	uint testScale = DATA_SCALE / 4;

	// ����undo֮���redo������־����
	// ���̺�undo���Ժ����ƣ�ֻ������undo֮����Ҫ�Ӳ���ԭʼ״̬redo����������־�Ͳ�����־
	// �ò���Ӧ�����ô�ѣ���߲��Ը�����
	uint origSize = 10;
	uint totalKeys, items;
	uint step = 2;
	m_tblDef = createBigTableDef();
	m_heap = createBigHeap(m_tblDef);
	items = totalKeys = buildBigHeap(m_heap, m_tblDef, false, origSize * 5, step);
	File oldindexfile(m_path);
	oldindexfile.remove();
	DrsIndice::create(m_path, m_tblDef);

	u16 tableId = m_tblDef->m_id;

	createIndex(m_path, m_heap, m_tblDef);

	u64 startLsn = m_db->getTxnlog()->tailLsn();;

	DrsIndex *index;

	IndexRecoveryManager *manager = NULL;

	Connection *conn = m_db->getConnection(false);
	Session *session = m_db->getSessionManager()->allocSession("IndexRecoveryTestCase::testDMLRecoveryRedoCPST", conn);
	MemoryContext *memoryContext = session->getMemoryContext();

	for (uint i = HOLE + 1; i < HOLE + origSize * 3; i += step) {
		// �ر����ݿⱸ�������ļ��������ļ���Ȼ�����´�
		m_db->getSessionManager()->freeSession(session);
		m_db->freeConnection(conn);
		closeEnv();

		// ���ݵ�ǰ״̬���ݺ������ļ�
		//backupHeapFile(HEAP_NAME, HEAP_BACKUP_NAME);
		backupHeapFile(INDEX_NAME, INDEX_BACKUP_NAME);

		openEnv();

		conn = m_db->getConnection(false);
		session = m_db->getSessionManager()->allocSession("IndexRecoveryTestCase::testDMLRecoveryRedoCPST", conn);
		memoryContext = session->getMemoryContext();

		manager = new IndexRecoveryManager(m_db, m_heap, m_tblDef, m_index);
		manager->setTxnType(TXN_INSERT);

		// ����һ���¼�¼
		u64 startLsn = m_db->getTxnlog()->tailLsn();
		Record *insertedRecord = insertSpecifiedKey(session, memoryContext, i, true);
		freeRecord(insertedRecord);
		u64 curLsn = session->getLastLsn();
		CPPUNIT_ASSERT(curLsn > startLsn);
		// undo�ղŵĲ��룬��֤undo��ȷ��
		manager->undoFromSpecifiedLogs(session, startLsn, curLsn);
		delete manager;
		manager = NULL;

		// ���undo�Ľ����֤��ȷ
		checkIndex(session, memoryContext, totalKeys);

		// ��ʱ�õ�������־��������״̬�ָ������β���֮ǰ��redoԭ���������־�Լ�������־
		m_db->getSessionManager()->freeSession(session);
		m_db->freeConnection(conn);
		closeEnv();

		// ��ԭ�ļ����������ʱ������״̬����ȡ��־���ݣ�����֮ǰ��ɾ����־
		//myrestoreHeapFile(HEAP_BACKUP_NAME, HEAP_NAME);
		myrestoreHeapFile(INDEX_BACKUP_NAME, INDEX_NAME);

		openEnv();

		conn = m_db->getConnection(false);
		session = m_db->getSessionManager()->allocSession("IndexRecoveryTestCase::testDMLRecoveryRedoCPST", conn);
		memoryContext = session->getMemoryContext();

		// ����ǰ������DML��־
		manager = new IndexRecoveryManager(m_db, m_heap, m_tblDef, m_index);
		manager->setTxnType(TXN_INSERT);
		manager->redoFromSpecifiedLogs(session, startLsn);
		delete manager;
		manager = NULL;

		// ���redo�Ľ����֤��ȷ
		checkIndex(session, memoryContext, totalKeys);

		// ������һ�θղŵĲ��룬��֤������Ч
		Record *temp = insertSpecifiedKey(session, memoryContext, i, true);
		freeRecord(temp);

		totalKeys++;
	}

	// �������
	checkIndex(session, memoryContext, totalKeys);
	index = m_index->getIndex(0);
//	SubRecord *idxKey = IndexKey::allocSubRecordRED(memoryContext, m_tblDef->m_maxRecSize);
	SubRecord *idxKey = IndexKey::allocSubRecord(memoryContext, m_tblDef->getIndexDef(0), KEY_PAD);
	idxKey->m_columns = m_tblDef->m_indice[0]->m_columns;
	idxKey->m_numCols = m_tblDef->m_indice[0]->m_numCols;
	rangeScanBackwardCheck(session, m_tblDef, index, NULL, idxKey, totalKeys);
	rangeScanForwardCheck(session, m_tblDef, index, NULL, idxKey, totalKeys);

	// Part2:�ڸղŽ�������Ļ����ϣ�������ɾ�գ�ÿɾһ�����undo����
	SubRecord *findrecord;
	for (uint i = HOLE; i < HOLE + origSize * 5; i++) {
		SubRecord *findKey = makeFindKeyWhole(i, m_heap, m_tblDef, true);
		findKey->m_rowId = INVALID_ROW_ID;
		RowId rowId;
//		findrecord = IndexKey::allocSubRecordRED(memoryContext, m_tblDef->m_maxRecSize);
		findrecord = IndexKey::allocSubRecord(memoryContext, m_tblDef->getIndexDef(0), KEY_PAD);
		findrecord->m_columns = m_tblDef->m_indice[0]->m_columns;
		findrecord->m_numCols = m_tblDef->m_indice[0]->m_numCols;

		IndexDef *indexDef = m_tblDef->m_indice[0];
		SubToSubExtractor *extractor = SubToSubExtractor::createInst(session->getMemoryContext(), m_tblDef, indexDef,
			indexDef->m_numCols, indexDef->m_columns, findrecord->m_numCols, findrecord->m_columns, KEY_COMPRESS, KEY_PAD, 1);

		bool found = index->getByUniqueKey(session, findKey, None, &rowId, findrecord, NULL, extractor);
		if (found) {	// ������ڣ����в���
			// �ر����ݿⱸ�������ļ��������ļ���Ȼ�����´�
			m_db->getSessionManager()->freeSession(session);
			m_db->freeConnection(conn);
			closeEnv();

			// ���ݵ�ǰ״̬���ݺ������ļ�
			//backupHeapFile(HEAP_NAME, HEAP_BACKUP_NAME);
			backupHeapFile(INDEX_NAME, INDEX_BACKUP_NAME);

			openEnv();

			conn = m_db->getConnection(false);
			session = m_db->getSessionManager()->allocSession("IndexRecoveryTestCase::testDMLRecoveryRedoCPST", conn);
			memoryContext = session->getMemoryContext();

			Record *record = makeRecordForIAndD(i, true, false);
			record->m_rowId = rowId;
			// ִ��һ��ɾ������
			startLsn = m_db->getTxnlog()->tailLsn();;
			session->startTransaction(TXN_DELETE, tableId);
			m_index->deleteIndexEntries(session, record, NULL);
			session->endTransaction(true);

			u64 curLsn = session->getLastLsn();
			CPPUNIT_ASSERT(curLsn > startLsn);
			// undo�ղŵĲ��룬��֤undo��ȷ��
			manager = new IndexRecoveryManager(m_db, m_heap, m_tblDef, m_index);
			manager->setTxnType(TXN_DELETE);
			manager->undoFromSpecifiedLogs(session, startLsn, curLsn);
			delete manager;
			manager = NULL;

			// ���undo�Ľ����֤��ȷ
			checkIndex(session, memoryContext, totalKeys);

			// ��ʱ�õ�������־��������״̬�ָ������β���֮ǰ��redoԭ���������־�Լ�������־
			m_db->getSessionManager()->freeSession(session);
			m_db->freeConnection(conn);
			closeEnv();

			// ��ԭ�ļ����������ʱ������״̬����ȡ��־���ݣ�����֮ǰ��ɾ����־
			//myrestoreHeapFile(HEAP_BACKUP_NAME, HEAP_NAME);
			myrestoreHeapFile(INDEX_BACKUP_NAME, INDEX_NAME);

			openEnv();

			conn = m_db->getConnection(false);
			session = m_db->getSessionManager()->allocSession("IndexRecoveryTestCase::testDMLRecoveryRedoCPST", conn);
			memoryContext = session->getMemoryContext();

			// ����ǰ������DML��־
			manager = new IndexRecoveryManager(m_db, m_heap, m_tblDef, m_index);
			manager->setTxnType(TXN_DELETE);
			manager->redoFromSpecifiedLogs(session, startLsn);
			delete manager;
			manager = NULL;

			// ���redo�Ľ����֤��ȷ
			checkIndex(session, memoryContext, totalKeys);

			session->startTransaction(TXN_DELETE, tableId);
			m_index->deleteIndexEntries(session, record, NULL);
			session->endTransaction(true);
			freeRecord(record);
			totalKeys--;

			checkIndex(session, memoryContext, totalKeys);
			index = m_index->getIndex(0);
//			idxKey = IndexKey::allocSubRecordRED(memoryContext, m_tblDef->m_maxRecSize);
			idxKey = IndexKey::allocSubRecord(memoryContext, m_tblDef->getIndexDef(0), KEY_PAD);
			idxKey->m_columns = m_tblDef->m_indice[0]->m_columns;
			idxKey->m_numCols = m_tblDef->m_indice[0]->m_numCols;
			CPPUNIT_ASSERT(rangeScanBackwardCheck(session, m_tblDef, index, NULL, idxKey, totalKeys));
			CPPUNIT_ASSERT(rangeScanForwardCheck(session, m_tblDef, index, NULL, idxKey, totalKeys));
		}
		freeSubRecord(findKey);
	}

	CPPUNIT_ASSERT(totalKeys == 0);

	// ������������
	checkIndex(session, memoryContext, totalKeys);
	index = m_index->getIndex(0);
	rangeScanBackwardCheck(session, m_tblDef, index, NULL, idxKey, totalKeys);
	rangeScanForwardCheck(session, m_tblDef, index, NULL, idxKey, totalKeys);

	// ����redo���ԣ��رջ���
	m_index->close(m_session, true);
	delete m_index;
	DrsIndice::drop(m_path);
	m_index = NULL;
	closeSmallHeap(m_heap);
	DrsHeap::drop(HEAP_NAME);
	m_heap = NULL;
	delete m_tblDef;
	m_tblDef = NULL;
	TableDef::drop(TBLDEF_NAME);

	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(conn);
}


/**
 * ����������������Ľṹ��ȷ��
 */
void IndexRecoveryTestCase::checkIndex(Session *session, MemoryContext *memoryContext, uint keys) {
	if (isEssentialOnly())
		return;

	SubRecord *key1 = IndexKey::allocSubRecord(memoryContext, m_tblDef->m_indice[0], KEY_COMPRESS);
	SubRecord *key2 = IndexKey::allocSubRecord(memoryContext, m_tblDef->m_indice[0], KEY_COMPRESS);
	SubRecord *pkey0 = IndexKey::allocSubRecord(memoryContext, m_tblDef->m_indice[0], KEY_PAD);

	DrsIndex *index = m_index->getIndex(0);
	CPPUNIT_ASSERT(((DrsBPTreeIndex*)index)->verify(session, key1, key2, pkey0, true));

	if (m_index->getIndexNum() <= 1)
		return;

	SubRecord *key3 = IndexKey::allocSubRecord(memoryContext, m_tblDef->m_indice[1], KEY_COMPRESS);
	SubRecord *key4 = IndexKey::allocSubRecord(memoryContext, m_tblDef->m_indice[1], KEY_COMPRESS);
	SubRecord *pkey1 = IndexKey::allocSubRecord(memoryContext, m_tblDef->m_indice[1], KEY_PAD);

	index = m_index->getIndex(1);
	CPPUNIT_ASSERT(((DrsBPTreeIndex*)index)->verify(session, key3, key4, pkey1, true));
}


/**
 * ��ָ���ļ�¼����Ѻ�����
 */
Record* IndexRecoveryTestCase::insertSpecifiedRecord(Session *session, MemoryContext *memoryContext, uint i, bool testBig, bool testPrefix) {
	char name[LONG_KEY_LENGTH];
	RowId rid;
	if (testBig) {
		if (testPrefix)
			sprintf(name, FAKE_LONG_CHAR234 "%d", i);
		else
			sprintf(name, "%d" FAKE_LONG_CHAR234, i);
	} else
		sprintf(name, "Kenneth Tse Jr. %d \0", i);
	Record *insertrecord = createRecord(m_tblDef, 0, i, name, i + ((u64)i << 32), (u32)(-1) - i);
	EXCPT_OPER(rid = m_heap->insert(m_session, insertrecord, NULL));
	freeRecord(insertrecord);
	insertrecord = createRecordRedundant(m_tblDef, 0, i, name, i + ((u64)i << 32), (u32)(-1) - i);
	insertrecord->m_rowId = rid;

	bool successful;
	uint dupIndex;
	session->startTransaction(TXN_INSERT, m_tblDef->m_id);
	EXCPT_OPER(successful = m_index->insertIndexEntries(session, insertrecord, &dupIndex));
	session->endTransaction(true);
	CPPUNIT_ASSERT(successful);

	return insertrecord;
}

/**
 * ��ָ���ļ�¼��������
 */
Record* IndexRecoveryTestCase::insertSpecifiedKey(Session *session, MemoryContext *memoryContext, uint i, bool testBig, bool testPrefix) {
	Record *insertrecord = makeRecordForIAndD(i, testBig, testPrefix);
	insertrecord->m_rowId = i;

	bool successful;
	uint dupIndex;
	session->startTransaction(TXN_INSERT, m_tblDef->m_id);
	EXCPT_OPER(successful = m_index->insertIndexEntries(session, insertrecord, &dupIndex));
	session->endTransaction(true);
	CPPUNIT_ASSERT(successful);

	return insertrecord;
}



Record* IndexRecoveryTestCase::makeRecordForIAndD( uint i, bool testBig /*= false*/, bool testPrefix /*= false*/ ) {
	char name[LONG_KEY_LENGTH];
	if (testBig) {
		if (testPrefix)
			sprintf(name, FAKE_LONG_CHAR234 "%d", i);
		else
			sprintf(name, "%d" FAKE_LONG_CHAR234, i);
	} else
		sprintf(name, "Kenneth Tse Jr. %d \0", i);
	Record *insertrecord = createRecordRedundant(m_tblDef, 0, i, name, i + ((u64)i << 32), (u32)(-1) - i);
	return insertrecord;
}


/**
 * ����һ���Ự����ʼ��־LSN
 * ����ʼ��־LSN��ʼredo����������ص���־ֱ����־�ļ���β
 */
void IndexRecoveryManager::redoFromSpecifiedLogs(Session *session, u64 startLsn) {
	uint count = 0;
	// ���ݼ�¼��LSN����ȡ��־������������������
	Txnlog *txnLog = m_db->getTxnlog();
	LogScanHandle *logHdl = txnLog->beginScan(startLsn, Txnlog::MAX_LSN);
	while (txnLog->getNext(logHdl)) {
		LogEntry *logEntry = (LogEntry*)logHdl->logEntry();
		//redo����־
		redoALog(session, logEntry, logHdl->curLsn());
		count++;
	}
	txnLog->endScan(logHdl);
}


/**
 * ����һ���Ự����ʼ������LSN
 * ����ʼ������LSN�ķ�Χ�ڣ�undo����������ص���־
 */
void IndexRecoveryManager::undoFromSpecifiedLogs(Session *session, u64 startLsn, u64 targetLsn) {
	LogEntry logs[1000];	// �������1000��undo��־
	u64 lsns[1000];
	uint count = 0;
	// ���ȱ�֤Ҫundo����־����ˢ������
	Txnlog *txnLog = m_db->getTxnlog();
	txnLog->flush(targetLsn);
	// ���ȶ�ȡ֮ǰ������������־
	LogScanHandle *logHdl = txnLog->beginScan(startLsn, Txnlog::MAX_LSN);
	while (txnLog->getNext(logHdl)) {
		LogEntry *logEntry = (LogEntry*)logHdl->logEntry();
		lsns[count] = logHdl->curLsn();
		// ������־����
		memcpy(&logs[count], (byte*)logEntry, sizeof(LogEntry));
		logs[count].m_data = new byte[logEntry->m_size];
		memcpy(logs[count].m_data, logEntry->m_data, logEntry->m_size);
		count++;
	}
	txnLog->endScan(logHdl);

	uint totalLogs = count;
	// ����undo��־�����˸ղŵĲ���
	while (count > 0) {
		count--;
		LogEntry *logEntry = &logs[count];
		undoALog(session, logEntry, lsns[count]);
	}

	// �ͷ���־���ݿռ�
	for (uint i = 0; i < totalLogs; i++) {
		delete[] logs[i].m_data;
	}
}


/**
 * ����һ��ָ������־�������������صĻ�
 * ����Ĵ������ص���־���Ϳ��Բμ����뵱�е�case�ж�
 */
void IndexRecoveryManager::redoALog(Session *session, LogEntry *logEntry, u64 lsn) {
	LogType type = logEntry->m_logType;
	byte *log = logEntry->m_data;
	uint size = (uint)logEntry->m_size;
	switch (type) {
		case LOG_IDX_DROP_INDEX:	/** ����ָ������ */
			m_indice->redoDropIndex(session, lsn, log, size);
			break;
		case LOG_IDX_DML:			/** ������ֵ���롢ɾ����Append��Ӳ��� */
			m_indice->redoDML(session, lsn, log, size);
			break;
		case LOG_IDX_SMO:			/** ����SMO���� */
			m_indice->redoSMO(session, lsn, log, size);
			break;
		case LOG_IDX_SET_PAGE:		/** ����ҳ���޸Ĳ��� */
			m_indice->redoPageSet(session, lsn, log, size);
			break;
		case LOG_IDX_DML_CPST:		/** ��Ŷ���ֵ���롢ɾ����Append�����Ĳ�����־ */
			m_indice->redoCpstDML(session, lsn, log, size);
			break;
		case LOG_IDX_SMO_CPST:		/** ����SMO����������־ */
			m_indice->redoCpstSMO(session, lsn, log, size);
			break;
		case LOG_IDX_SET_PAGE_CPST:	/** ����ҳ���޸Ĳ���������־ */
			m_indice->redoCpstPageSet(session, lsn, log, size);
			break;
		case LOG_IDX_ADD_INDEX_CPST:	/** ��������������־ */
			m_indice->redoCpstCreateIndex(session, lsn, log, size);
			break;
		case LOG_IDX_CREATE_BEGIN:		/** ��ʼ�������� */
			break;
		case LOG_IDX_CREATE_END:		/** ������������ */
			m_indice->redoCreateIndexEnd(log, size);
			break;
		case LOG_IDX_DML_BEGIN:		/** ��ʼ����DML�޸Ĳ��� */
			// ��ʱ�����ض��������ģ�������֤������
			break;
		case LOG_IDX_DML_END:		/** ��������DML�޸Ĳ��� */
			// ��ʱ�����ض��������ģ�������֤������
			m_indice->isIdxDMLSucceed(log, size);
			break;
		case LOG_TXN_START:
			TxnType type;
			type = Session::parseTxnStartLog(logEntry);
			session->startTransaction(type, m_tblDef->m_id, false);
			break;
		case LOG_TXN_END:
			session->endTransaction(true, false);
			break;
		case LOG_IDX_DMLDONE_IDXNO:
			m_indice->getLastUpdatedIdxNo(log, size);
			break;
		default:
			break;
	}
}


/**
 * undoһ��ָ������־����־��Ҫ��������ص�
 * ��ص����ͺ����������̿��Բμ��������
 */
void IndexRecoveryManager::undoALog(Session *session, LogEntry *logEntry, u64 lsn) {
	LogType type = logEntry->m_logType;
	switch (type) {
		case LOG_IDX_DROP_INDEX:	/** ����ָ������ */
			break;
		case LOG_IDX_DML:			/** ������ֵ���롢ɾ����Append��Ӳ��� */
			m_indice->undoDML(session, lsn, logEntry->m_data, (uint)logEntry->m_size, true);
			break;
		case LOG_IDX_SMO:			/** ����SMO���� */
			m_indice->undoSMO(session, lsn, logEntry->m_data, (uint)logEntry->m_size, true);
			break;
		case LOG_IDX_SET_PAGE:		/** ����ҳ���޸Ĳ��� */
			m_indice->undoPageSet(session, lsn, logEntry->m_data, (uint)logEntry->m_size, true);
			break;
		case LOG_IDX_DML_CPST:		/** ��������ֵ���롢ɾ����Append�����Ĳ�����־ */
			break;
		case LOG_IDX_SMO_CPST:		/** ����SMO����������־ */
			break;
		case LOG_IDX_SET_PAGE_CPST:	/** ����ҳ���޸Ĳ���������־ */
			break;
		case LOG_IDX_ADD_INDEX_CPST:	/** ��������������־ */
			break;
		case LOG_IDX_CREATE_BEGIN:		/** ��ʼ�������� */
			break;
		case LOG_IDX_CREATE_END:		/** ������������ */
			break;
		case LOG_IDX_DML_BEGIN:		/** ��ʼ����DML�޸Ĳ��� */
			break;
		case LOG_IDX_DML_END:		/** ��������DML�޸Ĳ��� */
			break;

			// undo���̿�ʼ���ύ����˳��Ӧ���ǵߵ���
		case LOG_TXN_START:
			session->endTransaction(true, false);
			break;
		case LOG_TXN_END:
			session->startTransaction(m_txnType, m_tblDef->m_id, false);
			break;
		default:
			break;
	}

}


/**
 * �رյ�ǰ�Ĳ��Ի������رնѺ������ļ����ر����ݿ⡣��ɾ���κ���Դ
 * ֻˢ����־����ˢ����
 */
void IndexRecoveryTestCase::closeEnv() {
	m_heap->close(m_session, true);
	delete m_heap;
	m_heap = NULL;
	if (m_index != NULL) {
		m_index->close(m_session, true);
		delete m_index;
		m_index = NULL;
	}
	if (m_tblDef != NULL) {
		delete m_tblDef;
		m_tblDef = NULL;
	}
	m_db->getSessionManager()->freeSession(m_session);
	m_db->freeConnection(m_conn);
	m_db->close(true, false);
	delete m_db;
	m_db = NULL;
}


/**
 * �򿪵�ǰ�Ĳ��Ի���
 * �����ݿ⡢���ļ��������ļ�
 */
void IndexRecoveryTestCase::openEnv() {
	EXCPT_OPER(m_db = Database::open(&m_cfg, 1, -1));
	m_conn = m_db->getConnection(true);
	m_session = m_db->getSessionManager()->allocSession("FLRHeapTestCase::testRedoInsert", m_conn);
	EXCPT_OPER(m_tblDef = TableDef::open(TBLDEF_NAME));
	EXCPT_OPER(m_heap = DrsHeap::open(m_db, m_session, HEAP_NAME, m_tblDef));
	m_index = DrsIndice::open(m_db, m_session, INDEX_NAME, m_tblDef, NULL);
}



static bool deleteUnique(Database *db, Session *session, DrsHeap *heap, const TableDef *tblDef, DrsIndice *indice, uint recordid, RowId rowId) {
	MemoryContext *memoryContext = session->getMemoryContext();

	uint key = recordid;
	char name[50];
	sprintf(name, "Kenneth Tse Jr. %d \0", key);
	Record *record = createRecordRedundant(tblDef, 0, key, name, key + ((u64)key << 32), (u32)(-1) - key);
	record->m_rowId = rowId;
	session->startTransaction(TXN_DELETE, tblDef->m_id);
	EXCPT_OPER(indice->deleteIndexEntries(session, record, NULL));
	session->endTransaction(true);
	CPPUNIT_ASSERT(rowId != INVALID_ROW_ID);
	heap->del(session, rowId);
	freeRecord(record);

	return true;
}

static uint deleteRange(Database *db, Session *session, DrsHeap *heap, const TableDef *tblDef, DrsIndice *indice, uint recordid, bool deleteAll = false) {
	MemoryContext *memoryContext = session->getMemoryContext();

	Record *record = IndexKey::allocRecord(memoryContext, tblDef->m_maxRecSize);

	IndexScanHandle *indexHandle;
//	SubRecord *idxKey = IndexKey::allocSubRecordRED(memoryContext, tblDef->m_maxRecSize);
	SubRecord *idxKey = IndexKey::allocSubRecord(memoryContext, tblDef->getIndexDef(1), KEY_PAD);
	idxKey->m_columns = tblDef->m_indice[1]->m_columns;
	idxKey->m_numCols = tblDef->m_indice[1]->m_numCols;
	IndexDef *indexDef = tblDef->m_indice[1];

	SubToSubExtractor *extractor = SubToSubExtractor::createInst(session->getMemoryContext(), tblDef, indexDef,
		indexDef->m_numCols, indexDef->m_columns, idxKey->m_numCols, idxKey->m_columns, KEY_COMPRESS, KEY_PAD, 1000);

	u64 savePoint = memoryContext->setSavepoint();
	RowLockHandle *rlh;
	//	��Χɾ������������¼
	DrsIndex *index = indice->getIndex(1);
	SubRecord *findKey = makeFindKeyWhole(recordid, heap, tblDef);
	findKey->m_rowId = INVALID_ROW_ID;
	indexHandle = index->beginScan(session, findKey, true, true, Exclusived, &rlh, extractor);
	uint count = 0;
	while (index->getNext(indexHandle, idxKey)) {
		// ȥ��ӦHeapȡ��¼
		record->m_format = REC_FIXLEN;
		heap->getRecord(session, idxKey->m_rowId, record);
		record->m_format = REC_REDUNDANT;
		session->startTransaction(TXN_DELETE, tblDef->m_id);
		indice->deleteIndexEntries(session, record, indexHandle);
		session->endTransaction(true);
		heap->del(session, idxKey->m_rowId);
		session->unlockRow(&rlh);
		count++;
		if (!deleteAll && count == 2)
			break;
	}
	index->endScan(indexHandle);
	freeSubRecord(findKey);

	return count;
}

static bool findUnique(Database *db, Session *session, DrsHeap *heap, const TableDef *tblDef, DrsIndice *indice, uint recordid, RowId rowId, RowLockHandle **rowLockHandle) {
	MemoryContext *memoryContext = session->getMemoryContext();

	SubRecord *findKey = makeFindKeyWhole(recordid, heap, tblDef);
	findKey->m_rowId = rowId;
	DrsIndex *index = indice->getIndex(1);
	RowId gotrowId;
	bool found;
	if (rowLockHandle == NULL)
		found = index->getByUniqueKey(session, findKey, None, &gotrowId, NULL, NULL, NULL);
	else
		found = index->getByUniqueKey(session, findKey, Exclusived, &gotrowId, NULL, rowLockHandle, NULL);

	if (found && rowLockHandle != NULL)
		session->unlockRow(rowLockHandle);
	freeSubRecord(findKey);

	return found;
}

static uint findRange(Database *db, Session *session, DrsHeap *heap, const TableDef *tblDef, DrsIndice *indice, uint recordid, RowId rowId, bool upsidedown, RowLockHandle **rowLockHandle = NULL, u64 rangeSize = (u64)-1) {
	MemoryContext *memoryContext = session->getMemoryContext();

	SubRecord *findKey = makeFindKeyWhole(recordid, heap, tblDef);
	findKey->m_rowId = rowId;
	bool forward = !upsidedown;

	DrsIndex *index = indice->getIndex(1);
	IndexScanHandle *scanHandle = index->beginScan(session, findKey, forward, true, (rowLockHandle == NULL ? None : Shared), rowLockHandle, NULL);
	uint count = 0;
	while (index->getNext(scanHandle, NULL)) {
		count++;
		if (rowLockHandle != NULL)
			session->unlockRow(rowLockHandle);
		if (count >= rangeSize)
			break;
	}
	index->endScan(scanHandle);
	freeSubRecord(findKey);

	return count;
}

static void insert(Database *db, Session *session, DrsHeap *heap, const TableDef *tblDef, DrsIndice *indice, uint recordid, uint rangeSize) {
	if (rangeSize == 0)
		rangeSize++;

	MemoryContext *memoryContext = session->getMemoryContext();

	for (uint i = 0; i < rangeSize; i++) {
		uint key = i + recordid;
		char name[50];
		RowId rid;
		sprintf(name, "Kenneth Tse Jr. %d \0", key);
		Record *insertrecord = createRecord(tblDef, 0, key, name, key + ((u64)key << 32), (u32)(-1) - key);
		EXCPT_OPER(rid = heap->insert(session, insertrecord, NULL));
		freeRecord(insertrecord);
		insertrecord = createRecordRedundant(tblDef, 0, key, name, key + ((u64)key << 32), (u32)(-1) - key);
		insertrecord->m_rowId = rid;

		bool successful;
		uint dupIndex;
		session->startTransaction(TXN_INSERT, tblDef->m_id);
		EXCPT_OPER(successful = indice->insertIndexEntries(session, insertrecord, &dupIndex));
		session->endTransaction(true);
		CPPUNIT_ASSERT(successful);
		freeRecord(insertrecord);
	}
}




/**
 * ������������ȫ����
 * ��ָ��λ��ɾ��ָ����һ����¼
 */
void TroubleMaker::run() {
	Connection *conn = m_db->getConnection(false);
	Session *session = m_db->getSessionManager()->allocSession("IndexRecoveryTestCase::testDMLRecovery", conn);
	MemoryContext *memoryContext = session->getMemoryContext();
	uint key = m_recordid;
	char name[50];
	sprintf(name, "Kenneth Tse Jr. %d \0", key);
	Record *record = createRecordRedundant(m_tblDef, 0, key, name, key + ((u64)key << 32), (u32)(-1) - key);
	record->m_rowId = m_rowId;
	session->startTransaction(TXN_DELETE, m_tblDef->m_id);
	EXCPT_OPER(m_indice->deleteIndexEntries(session, record, NULL));
	session->endTransaction(true);

	freeRecord(record);

	CPPUNIT_ASSERT(m_rowId != INVALID_ROW_ID);
	m_heap->del(session, m_rowId);

	SYNCHERE(SP_IDX_FINISH_A_DELETE);

	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(conn);
}


/**
 * ��������߽��ϸ�ڴ��ع�
 * ����ָ����ɾ����ʽ��ѡ���Ƿ�Χɾ������Ψһɾ��ָ����������¼����Χɾ��������ɾ�������������ݣ�����ָ�������ݾ����Ӳ�����
 * Ψһɾ���ض�������
 */
void SimpleMan::run() {
	Connection *conn = m_db->getConnection(false);
	Session *session = m_db->getSessionManager()->allocSession("SimpleMan::run", conn);
	if (m_captious) {	// ��Χɾ��
		deleteRange(m_db, session, m_heap, m_tblDef, m_indice, m_recordid, m_deleteAll);
	} else {	// ���ɾ��
		deleteUnique(m_db, session, m_heap, m_tblDef, m_indice, m_recordid, m_rowId);
	}
	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(conn);
}


/**
 * ���͵��۷��̣߳�����ָ���ķ�Χ��С�������������ݵ�����
 */
void Bee::run() {
	if (m_rangeSize == 0)
		m_rangeSize++;

	Connection *conn = m_db->getConnection(false);
	Session *session = m_db->getSessionManager()->allocSession("IndexRecoveryTestCase::testDMLRecovery", conn);
	MemoryContext *memoryContext = session->getMemoryContext();

	for (uint i = 0; i < m_rangeSize; i++) {
		uint key = i + m_recordid;
		char name[50];
		RowId rid;
		sprintf(name, "Kenneth Tse Jr. %d \0", key + 1);	// ���ﹹ��Ĳ����ֵ�������ط���һ�������1
		Record *insertrecord = createRecord(m_tblDef, 0, key, name, key + ((u64)key << 32), (u32)(-1) - key);
		EXCPT_OPER(rid = m_heap->insert(session, insertrecord, NULL));
		freeRecord(insertrecord);
		insertrecord = createRecordRedundant(m_tblDef, 0, key, name, key + ((u64)key << 32), (u32)(-1) - key);
		insertrecord->m_rowId = rid;

		bool successful;
		uint dupIndex;
		session->startTransaction(TXN_INSERT, m_tblDef->m_id);
		EXCPT_OPER(successful = m_indice->insertIndexEntries(session, insertrecord, &dupIndex));
		session->endTransaction(true);
		CPPUNIT_ASSERT(successful);
		freeRecord(insertrecord);
	}

	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(conn);
}


/**
 * ���͵ı�����۷��̣߳�����ָ���ķ�Χ��С�������������ݵ�����
 */
void VariantBee::run() {
	Connection *conn = m_db->getConnection(false);
	Session *session = m_db->getSessionManager()->allocSession("VariantBee::run", conn);
	insert(m_db, session, m_heap, m_tblDef, m_indice, m_recordid, m_rangeSize);
	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(conn);
}



/**
 * �������
 * ����ָ���ķ�ʽִ������ɨ�裬��Χɨ���Ψһ�Բ�ѯ
 * ��Χ��ѯ��������Ψһ�Բ�ѯ����ָ����������
 * ������m_readNum��¼���ҵ��ļ�¼�������ڵ�����ͳ��
 */
void ReadingLover::run() {
	Connection *conn = m_db->getConnection(false);
	Session *session = m_db->getSessionManager()->allocSession("ReadingLover::run", conn);
	if (m_crazy) {	// ����Χ����ɨ��
		m_readNum = findRange(m_db, session, m_heap, m_tblDef, m_indice, m_recordid, m_rowId, m_upsidedown, m_rowLockHandle, m_rangeSize);
	} else {	// Ψһ��ѯ
		bool found = findUnique(m_db, session, m_heap, m_tblDef, m_indice, m_recordid, m_rowId, m_rowLockHandle);
		m_readNum = found ? 1 : 0;
	}
	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(conn);
}


/**
 * ����Ǹ��󹤳�
 * ����ָ���Ķѣ������������������Ϊ�˰�ȫ��ȷ�������ÿ�����ִ��
 */
void BigProject::run() {
	Connection *conn = m_db->getConnection(false);
	Session *session = m_db->getSessionManager()->allocSession("IndexRecoveryTestCase::testDMLRecovery", conn);
	MemoryContext *memoryContext = session->getMemoryContext();

	// ��Ҫ����������������������
	session->startTransaction(TXN_ADD_INDEX, m_tblDef->m_id);

	// �����������ظ�����������������ȫ�����ˡ�
	try {	// ��������Ψһ��Լ����������������ᶪ������
		m_indice->createIndexPhaseOne(session, m_tblDef->m_indice[0], m_tblDef, m_heap);
		m_indice->createIndexPhaseTwo(m_tblDef->m_indice[0]);
		m_indice->createIndexPhaseOne(session, m_tblDef->m_indice[1], m_tblDef, m_heap);
		m_indice->createIndexPhaseTwo(m_tblDef->m_indice[1]);
	} catch (NtseException &e) {
		cout << e.getMessage() << endl;
	}

	session->endTransaction(true);

	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(conn);
}


/**
 * ���Ķ����ߵĻ�������
 * ����ָ���ظ��������ظ�ִ�����ɴ�DML����
 * ÿ��DML�������ᴴ��һ�����߳�ִ��
 * Ϊ��ģ����ʵ��������������˻��ǻ��Դ�ϲ���Ķ���������ѯ�Ļ������Խϴ�
 */
void HalfHeartedMan::run() {
	Connection *conn = NULL;
	Session *session = NULL;
	for (u64 i = 0; i < m_times; i++) {
		// ���������������DML���ͣ��󲿷�Ӧ���ǲ�ѯ����
		if (i % 100 == 0) {	// ��һ���µ�����
			if (conn != NULL) {
				if (session != NULL)
					m_db->getSessionManager()->freeSession(session);
				m_db->freeConnection(conn);
			}
			conn = m_db->getConnection(false);
			if (conn == NULL)
				continue;
			session = m_db->getSessionManager()->allocSession("HalfHeartedMan::run", conn);
		}
		uint type = rand() % 6;
		if (type <= 1) {	// ���룬�������͵��۷��æ
			// Ϊ�˼򵥲�������߲��Բ����ԣ�������ִֻ�е�������
			uint insertNo = rand() % MT_TEST_DATA_SCALE;
			insert(m_db, session, m_heap, m_tblDef, m_indice, insertNo, 1);
			cout << this->getId() << " " << insertNo << " inserted" << endl;
		}

		if (type == 2) {	// ɾ�����������̼�������ߵĽ��
			uint deleteNo = rand() % MT_TEST_DATA_SCALE;	// ɾ������ʼ����
			deleteRange(m_db, session, m_heap, m_tblDef, m_indice, deleteNo);
			cout << this->getId() << " done a range delete from " << deleteNo << endl;
		}

		if (type > 2) {	// ��ѯ��reading is my favorite thing
			uint readNo = rand() & MT_TEST_DATA_SCALE;
			bool forward;
			forward = (rand() % 2 == 0);
			findRange(m_db, session, m_heap, m_tblDef, m_indice, readNo, INVALID_ROW_ID, !forward);
			cout << this->getId() << " done a range scan from " << readNo << endl;
		}

		Thread::msleep(100);
	}
}

/************************************************************************/
/* ���Դ��ģ��SMO�����������ṹ��ȷ�Ե���֤                            */
/************************************************************************/

void IndexSMOTestCase::setUp() {
	m_heap = NULL;
	m_index = NULL;
	Database::drop(".");
	EXCPT_OPER(m_db = Database::open(&m_cfg, true));
	char name[255] = "testIndex.nti";
	memcpy(m_path, name, sizeof(name));
	srand((unsigned)time(NULL));
	clearRSFile(&m_cfg);
	ts.idx = true;
	vs.idx = true;
}

void IndexSMOTestCase::tearDown() {
	// ������
	Connection *conn = m_db->getConnection(false);
	Session *session = m_db->getSessionManager()->allocSession("IndexSMOTestCase::tearDown", conn);

	if (m_index != NULL) {
		m_index->close(session, true);
		delete m_index;
		DrsIndice::drop(m_path);
	}
	// ����
	if (m_heap != NULL) {
		EXCPT_OPER(m_heap->close(session, true));
		delete m_heap;
		DrsHeap::drop(HEAP_NAME);
	}

	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(conn);
	m_db->close();
	delete m_db;
	Database::drop(".");
	m_db = NULL;

	ts.idx = false;
	vs.idx = false;
}

/**
 * ���Ե��̶߳���²�����������������SMO
 */
void IndexSMOTestCase::testLotsUpdates() {
	uint userNum = 200000;

	m_tblDef = createTableDef();
	m_heap = createHeapAndLoadData(m_db,m_tblDef, userNum);
	m_index = createIndex(m_db, m_path, m_heap, m_tblDef);

	Connection *conn = m_db->getConnection(false);
	Session *session = m_db->getSessionManager()->allocSession("IndexSMOTestCase::testLotsUpdates", conn);
	MemoryContext *memoryContext = session->getMemoryContext();
	DrsIndex *index = m_index->getIndex(0);
	IndexDef *indexDef = m_tblDef->m_indice[0];

	Record *record = IndexKey::allocRecord(memoryContext, m_tblDef->m_maxRecSize);
	record->m_format = REC_FIXLEN;
	u64 count = 0;
	while (true) {
		count++;
		u64 savePoint = memoryContext->setSavepoint();

		// �����ȡһ��IDֵ��������ɨ�裬������AccessCount��1����
		uint idplus = rand() % userNum;
		u64 userId = USER_MIN_ID + idplus;

		// ��ʼһ������ɨ�裬��ÿһ��ȡ�õļ�¼���޸ĶѺ�����
		SubRecord *findKey = makeFindKeyByUserId(m_tblDef, userId);
//		SubRecord *idxKey = IndexKey::allocSubRecordRED(memoryContext, m_tblDef->m_maxRecSize);
		SubRecord *idxKey = IndexKey::allocSubRecord(memoryContext, indexDef, KEY_PAD);
		idxKey->m_columns = indexDef->m_columns;
		idxKey->m_numCols = indexDef->m_numCols;

		SubRecord *updateSub = IndexKey::allocSubRecordRED(memoryContext, m_tblDef->m_maxRecSize);
		u16 colno = 2;
		updateSub->m_columns = &colno;
		updateSub->m_numCols = 1;

		SubToSubExtractor *extractor = SubToSubExtractor::createInst(memoryContext, m_tblDef, indexDef,
			indexDef->m_numCols, indexDef->m_columns, idxKey->m_numCols, idxKey->m_columns, KEY_COMPRESS, KEY_PAD, 1000);

		RowLockHandle *rlh;

		IndexScanHandle *indexHandle = index->beginScan(session, findKey, true, true, Exclusived, &rlh, extractor);

		std::vector<RowId> rowIds;

		while (index->getNext(indexHandle, idxKey)) {
			session->unlockRow(&rlh);

			u64 keyUserId = RedRecord::readBigInt(m_tblDef, idxKey->m_data, 1);
			if (keyUserId != userId)
				break;

			rowIds.push_back(idxKey->m_rowId);
		}

		index->endScan(indexHandle);

		for (uint i = 0; i < rowIds.size(); i++) {
			// ȡ�ö�Ӧ�ѵļ�¼
			record->m_format = REC_FIXLEN;
			m_heap->getRecord(session, rowIds[i], record);
			record->m_format = REC_REDUNDANT;

			memcpy(idxKey->m_data, record->m_data, record->m_size);
			idxKey->m_rowId = rowIds[i];
			updateSub->m_rowId = rowIds[i];

			NTSE_ASSERT(RedRecord::readBigInt(m_tblDef, idxKey->m_data, 1) == userId);

			uint accessCount = RedRecord::readInt(m_tblDef, record->m_data, colno);
			RedRecord::writeNumber(m_tblDef, colno, updateSub->m_data, accessCount + 1);

			// ִ�и��²��������ȶ�ɨ��õ�ɨ������Ȼ����¶�
			NTSE_ASSERT(m_heap->update(session, rowIds[i], updateSub));
			uint dupIndex;
			NTSE_ASSERT(m_index->updateIndexEntries(session, idxKey, updateSub, false, &dupIndex));
		}

		memoryContext->resetToSavepoint(savePoint);
		freeSubRecord(findKey);
	}

	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(conn);
}

TableDef* IndexSMOTestCase::createTableDef() {
	File oldsmallfile(TBLDEF_NAME);
	oldsmallfile.remove();

	TableDefBuilder *builder;
	TableDef *tblDef;

	// ������
	builder = new TableDefBuilder(1, "Photo", "UserActive");
	builder->addColumn("ID", CT_BIGINT, false)->addColumn("UserID", CT_BIGINT, false);
	builder->addColumn("AccessCount", CT_INT, false)->addColumnS("UserName", CT_CHAR, 100, false, false);
	builder->addIndex("Active", false, false, false, "UserID", 0, "AccessCount", 0, "UserName", 0, NULL);
	tblDef = builder->getTableDef();
	delete builder;
	EXCPT_OPER(tblDef->writeFile(TBLDEF_NAME));

	return tblDef;
}

DrsHeap* IndexSMOTestCase::createHeapAndLoadData(Database *db, const TableDef *tblDef, uint size) {
	char tableName[255] = HEAP_NAME;
	File oldsmallfile(tableName);
	oldsmallfile.remove();
	DrsHeap *heap = NULL;
	
	Connection *conn = db->getConnection(false);
	Session *session = db->getSessionManager()->allocSession("IndexSMOTestCase::createHeapAndLoadData", conn);

	EXCPT_OPER(DrsHeap::create(db, tableName, tblDef));
	EXCPT_OPER(heap = DrsHeap::open(db, session, tableName, tblDef));

	// װ������
	const char userName[100] = "Mr. all rights, but now he is not good. Some bugs are bothering him. ";
	uint duplicatePerUser = 100;	// ÿ���û�40����Ϣ
	u64 totalIds = 0;
	for (uint i = 0; i < size; i++) {
		for (uint j = 0; j < duplicatePerUser; j++) {
			RecordBuilder rb(tblDef, -1, REC_FIXLEN);
			rb.appendBigInt(totalIds++);
			rb.appendBigInt(USER_MIN_ID + i);
			rb.appendInt(0);
			rb.appendChar(userName);
			Record *record = rb.getRecord(tblDef->m_maxRecSize);
			EXCPT_OPER(heap->insert(session, record, NULL));
			freeRecord(record);
		}
	}

	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(conn);

	return heap;
}

DrsIndice* IndexSMOTestCase::createIndex( Database *db, char *path, DrsHeap *heap, const TableDef *tblDef) throw(NtseException) {
	Connection *conn = db->getConnection(false);
	Session *session = db->getSessionManager()->allocSession("IndexSMOTestCase::createHeapAndLoadData", conn);

	File oldindexfile(path);
	oldindexfile.remove();

	DrsIndice::create(path, tblDef);
	CPPUNIT_ASSERT(m_index == NULL);
	m_index = DrsIndice::open(db, session, path, tblDef, NULL);
	try {
		for (uint i = 0; i < tblDef->m_numIndice; i++) {
			m_index->createIndexPhaseOne(session, tblDef->m_indice[i], tblDef, heap);
			m_index->createIndexPhaseTwo(tblDef->m_indice[i]);
		}
	} catch (NtseException &e) {
		cout << e.getMessage() << endl;
		CPPUNIT_ASSERT(false);
	}

	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(conn);

	return m_index;
}

/**
 * �����ֵ�ȵ�ǰҳ�������м�ֵ����ʱ������SMO��ȷ��
 * TODO: ���������ȶ����޷���ҳ���С�����仯
 *		�Ľ��������������ⲿ��һ��ѭ���� ���Զ�Σ�ÿ���ǲ�ͬ�ļ�¼
 */
void IndexSMOTestCase::testInsertMaxKeySMO() {
	
	const int INDEX_PAGE_SIZE = (IndexPage::INDEXPAGE_INIT_MAX_USAGE - sizeof(IndexPage));
	const int KEY_SIZE =  INDEX_PAGE_SIZE / 20;	// ��ͨ��ֵ����
	const int NAME_SIZE = KEY_SIZE - 8;				// NAME�ֶγ���
	const int PROFILE_SIZE = 3 * KEY_SIZE;			// PROFILE�ֶγ���
	const int FULL_KEY_SIZE = KEY_SIZE + PROFILE_SIZE;	// ����ֵ����
	AutoPtr<TableDefBuilder> builder(new TableDefBuilder(99, "INDEX_BUGS", "MaxKeySMO"));
	builder->addColumn("UserId", CT_BIGINT, false)
		->addColumnS("UserName", CT_CHAR, NAME_SIZE)
		->addColumnS("Profile", CT_CHAR, PROFILE_SIZE, false)
		->addIndex("MyIndex", false, false, false, "UserId", 0, "UserName", 0, "Profile", 0, NULL);
	AutoPtr<TableDef> tableDef(builder->getTableDef());

	int recordPerPage = INDEX_PAGE_SIZE / KEY_SIZE;
	try {
		for (int vitim = recordPerPage - 1; vitim <= recordPerPage + 1; ++vitim) {
			tearDown();
			setUp();
			File oldsmallfile(HEAP_NAME);
			oldsmallfile.remove();

			Connection *conn = m_db->getConnection(false);
			Session *s = m_db->getSessionManager()->allocSession("IndexOperationTestCase::testInsertMaxKeySMO", conn);

			// ������
			EXCPT_OPER(DrsHeap::create(m_db, HEAP_NAME, tableDef));
			DrsHeap *heap = NULL;
			EXCPT_OPER(heap = DrsHeap::open(m_db, s, HEAP_NAME, (TableDef *)tableDef));


			// �����¼, ��������
			char randomString[Limits::PAGE_SIZE];
			memset(randomString, 'X', sizeof(randomString));
			randomString[sizeof(randomString) - 1] = '\0';

			for (int i = 1; i <= recordPerPage * 2; ++i) {
				RecordBuilder rb(tableDef, INVALID_ROW_ID, REC_FIXLEN);
				rb.appendBigInt( ((u64)i) << 56); // ��ֵ����ѹ��
				rb.appendChar(randomString, NAME_SIZE);
				rb.appendNull();
				Record *rec = rb.getRecord(tableDef->m_maxRecSize);
				heap->insert(s, rec, NULL);
				freeRecord(rec);
			}
			createIndex(m_db, m_path, heap, (TableDef *)tableDef);

			// ������ɾ���˵�1������ҳ������һ��
			RowId rowId = INVALID_ROW_ID;
			byte findKeyBuf[Limits::PAGE_SIZE];
			SubRecord findKey(KEY_PAD, tableDef->m_indice[0]->m_numCols, tableDef->m_indice[0]->m_columns
				, findKeyBuf, tableDef->m_maxRecSize, INVALID_ROW_ID);
			RedRecord lastRecOnFirstPage(tableDef, REC_REDUNDANT);
			lastRecOnFirstPage.writeNumber(0, ((u64)vitim) << 56);
			lastRecOnFirstPage.writeChar(1, randomString, NAME_SIZE);
			lastRecOnFirstPage.setNull(2);
			RecordOper::extractKeyRP((TableDef *)tableDef, tableDef->m_indice[0], lastRecOnFirstPage.getRecord(), NULL, &findKey);
			CPPUNIT_ASSERT(m_index->getIndex(0)->getByUniqueKey(s, &findKey, None, &rowId, NULL, NULL, NULL));
			lastRecOnFirstPage.setRowId(rowId);
			s->startTransaction(TXN_DELETE, tableDef->m_id);
			EXCPT_OPER(m_index->deleteIndexEntries(s, (Record *)lastRecOnFirstPage.getRecord(), NULL));
			s->endTransaction(true);
			


			// ���������ֵһ��������rowid��ͬ�ļ�¼�����·���
			RedRecord largerKey(tableDef, REC_REDUNDANT);
			for (int i = 0; i < KEY_SIZE / 4; ++i) {
				largerKey.setRowId(i + 1); // һ����С��rowid
				largerKey.writeNumber(0, ((u64)vitim) << 56);
				largerKey.writeChar(1, randomString, NAME_SIZE); 
				largerKey.setNull(2);
				s->startTransaction(TXN_INSERT, tableDef->m_id);
				EXCPT_OPER(m_index->insertIndexEntries(s, (Record *)largerKey.getRecord(), NULL));
				s->endTransaction(true);

				// �������������²���ļ�¼����֤���ҳɹ�
				RecordOper::extractKeyRP((TableDef *)tableDef, tableDef->m_indice[0], largerKey.getRecord(), NULL, &findKey);
				CPPUNIT_ASSERT(m_index->getIndex(0)->getByUniqueKey(s, &findKey, None, &rowId, NULL, NULL, NULL));
			}
			

			// ����
			heap->close(s, false);
			DrsHeap::drop(HEAP_NAME);
			m_db->getSessionManager()->freeSession(s);
			m_db->getSessionManager()->freeConnection(conn);
		}
	} catch( NtseException &e) {
		cout << e.getMessage() << endl;
	}

}

void IndexSMOTestCase::testBugNTSETNT39() {
	const int INDEX_PAGE_SIZE = (IndexPage::INDEXPAGE_INIT_MAX_USAGE - sizeof(IndexPage));
	const int KEY_SIZE =  INDEX_PAGE_SIZE / 20;	// ��ͨ��ֵ����
	const int NAME_SIZE = KEY_SIZE - 8;				// NAME�ֶγ���
	const int PROFILE_SIZE = 8 * KEY_SIZE;			// PROFILE�ֶγ���
	const int FULL_KEY_SIZE = KEY_SIZE + PROFILE_SIZE;	// ����ֵ����
	AutoPtr<TableDefBuilder> builder(new TableDefBuilder(99, "INDEX_BUGS", "NTSETNT39"));
	builder->addColumn("UserId", CT_BIGINT, false)
		->addColumnS("UserName", CT_CHAR, NAME_SIZE)
		->addColumnS("Profile", CT_CHAR, PROFILE_SIZE, false)
		->addIndex("MyIndex", false, false, false, "UserId", 0, "UserName", 0, "Profile", 0, NULL);
	AutoPtr<TableDef> tableDef(builder->getTableDef());

	int recordPerPage = INDEX_PAGE_SIZE / KEY_SIZE;

	try {	
		tearDown();
		setUp();
		File oldsmallfile(HEAP_NAME);
		oldsmallfile.remove();

		Connection *conn = m_db->getConnection(false);
		Session *s = m_db->getSessionManager()->allocSession("IndexOperationTestCase::testBugNTSETNT39", conn);

		// ������
		EXCPT_OPER(DrsHeap::create(m_db, HEAP_NAME, tableDef));
		DrsHeap *heap = NULL;
		EXCPT_OPER(heap = DrsHeap::open(m_db, s, HEAP_NAME, (TableDef *)tableDef));


		// �����¼, ��������
		char randomString[Limits::PAGE_SIZE];
		memset(randomString, 'X', sizeof(randomString));
		randomString[sizeof(randomString) - 1] = '\0';

		createIndex(m_db, m_path, heap, (TableDef *)tableDef);

		RowId rowId = INVALID_ROW_ID;
		byte findKeyBuf[Limits::PAGE_SIZE];
		SubRecord findKey(KEY_PAD, tableDef->m_indice[0]->m_numCols, tableDef->m_indice[0]->m_columns
			, findKeyBuf, tableDef->m_maxRecSize, INVALID_ROW_ID);

		// ����5��ṹ�������������棩
		RedRecord largerKey(tableDef, REC_REDUNDANT);
		for (int i = 1; i < 7; ++i) {
			largerKey.setRowId(10 * i ); // һ����С��rowid
			largerKey.writeNumber(0, (u64)(10 * i ));
			largerKey.writeChar(1, randomString, NAME_SIZE); 
			//largerKey.setNull(2);
			char *randomProfile = randomStr(PROFILE_SIZE);
			largerKey.writeChar(2, randomProfile, PROFILE_SIZE);
			s->startTransaction(TXN_INSERT, tableDef->m_id);
			EXCPT_OPER(m_index->insertIndexEntries(s, (Record *)largerKey.getRecord(), NULL));
			s->endTransaction(true);
			delete[] randomProfile;
			// �������������²���ļ�¼����֤���ҳɹ�
			RecordOper::extractKeyRP((TableDef *)tableDef, tableDef->m_indice[0], largerKey.getRecord(), NULL, &findKey);
			CPPUNIT_ASSERT(m_index->getIndex(0)->getByUniqueKey(s, &findKey, None, &rowId, NULL, NULL, NULL));
		}

		//���߳����������ҽڵ���Ѳ���
		IndexSMOThread smoThread(m_db, tableDef, m_index);
		smoThread.enableSyncPoint(SP_IDX_WAIT_TO_FIND_SPECIAL_LEVEL_PAGE);
		smoThread.start();
		smoThread.joinSyncPoint(SP_IDX_WAIT_TO_FIND_SPECIAL_LEVEL_PAGE);
		
		//��ǰ���������ҽڵ㹲ͬ�Ĳ���PageLevelΪ��2�ĸ��ڵ㲢���·��ѣ��˴�Ҫ��֤�����Ҷ�ڵ㲻�ܺ�����Ҷ�ڵ�����
		for (int i = 1; i < 4; ++i) {
			largerKey.setRowId(30 - i); 
			largerKey.writeNumber(0, (u64)(30 - i));
			largerKey.writeChar(1, randomString, NAME_SIZE); 
			//largerKey.setNull(2);
			char *randomProfile = randomStr(PROFILE_SIZE);
			largerKey.writeChar(2, randomProfile, PROFILE_SIZE);
			s->startTransaction(TXN_INSERT, tableDef->m_id);
			EXCPT_OPER(m_index->insertIndexEntries(s, (Record *)largerKey.getRecord(), NULL));
			s->endTransaction(true);
			delete[] randomProfile;
			// �������������²���ļ�¼����֤���ҳɹ�
			RecordOper::extractKeyRP((TableDef *)tableDef, tableDef->m_indice[0], largerKey.getRecord(), NULL, &findKey);
			CPPUNIT_ASSERT(m_index->getIndex(0)->getByUniqueKey(s, &findKey, None, &rowId, NULL, NULL, NULL));
		}

		smoThread.disableSyncPoint(SP_IDX_WAIT_TO_FIND_SPECIAL_LEVEL_PAGE);
		smoThread.notifySyncPoint(SP_IDX_WAIT_TO_FIND_SPECIAL_LEVEL_PAGE);
		smoThread.join(-1);
		
		// ����
		heap->close(s, false);
		DrsHeap::drop(HEAP_NAME);
		m_db->getSessionManager()->freeSession(s);
		m_db->getSessionManager()->freeConnection(conn);
		
	} catch( NtseException &e) {
		cout << e.getMessage() << endl;
	}
}

IndexSMOThread::IndexSMOThread(Database *db, TableDef * tableDef, DrsIndice *index):Thread("InsertSMOThread") {
	m_db = db;
	m_tableDef = tableDef;
	m_index = index;
}

void IndexSMOThread::run() {
	const int INDEX_PAGE_SIZE = (IndexPage::INDEXPAGE_INIT_MAX_USAGE - sizeof(IndexPage));
	const int KEY_SIZE =  INDEX_PAGE_SIZE / 20;	// ��ͨ��ֵ����
	const int NAME_SIZE = KEY_SIZE - 8;				// NAME�ֶγ���
	const int PROFILE_SIZE = 8 * KEY_SIZE;			// PROFILE�ֶγ���
	const int FULL_KEY_SIZE = KEY_SIZE + PROFILE_SIZE;	// ����ֵ����

	Connection *conn = m_db->getConnection(false);
	Session *s = m_db->getSessionManager()->allocSession("IndexSMOThread", conn);
	
	RedRecord largerKey(m_tableDef, REC_REDUNDANT);
	char randomString[Limits::PAGE_SIZE];
	memset(randomString, 'X', sizeof(randomString));
	randomString[sizeof(randomString) - 1] = '\0';

	//��������Ҷ�ڵ㵼�¼������������ڵ�
	largerKey.setRowId(70); // һ����С��rowid
	largerKey.writeNumber(0, (u64)(70));
	largerKey.writeChar(1, randomString, NAME_SIZE); 
	//largerKey.setNull(2);
	char *randomProfile = randomStr(PROFILE_SIZE);
	largerKey.writeChar(2, randomProfile, PROFILE_SIZE);
	s->startTransaction(TXN_INSERT, m_tableDef->m_id);
	EXCPT_OPER(m_index->insertIndexEntries(s, (Record *)largerKey.getRecord(), NULL));
	s->endTransaction(true);	
	delete[] randomProfile;

	m_db->getSessionManager()->freeSession(s);
	m_db->getSessionManager()->freeConnection(conn);
}


SubRecord* IndexSMOTestCase::makeFindKeyByUserId(const TableDef *tableDef, u64 userId ) {
	SubRecordBuilder sb(tableDef, KEY_PAD);
	return sb.createSubRecordByName("UserID", &userId);
}

const char* IndexSMOTestCase::getName() {
	return "Test index SMO";
}

const char* IndexSMOTestCase::getDescription() {
	return "Test Index smo by updating index again and againd";
}

bool IndexSMOTestCase::isBig() {
	return true;
}

/**********************************************************************************************/
/** QAƽ̨60214bug���޸Ļع�����
* ��������Ҫ������ڸ��²��������в���������������˵��µ�ɾ���޷�һ����˵�����
*/

u64 getRealObjectId(const TableDef *tableDef, u64 objectId) {
	return ((u64)tableDef->m_id << ((sizeof(u64) - sizeof(tableDef->m_id)) * 8)) | objectId;
}

class BugsVolunteer : public Thread {
public:
	BugsVolunteer(const char *name, Database *db, DrsHeap *heap, const TableDef *tblDef, DrsIndice *indice) : Thread(name) {
		m_db = db;
		m_indice = indice;
		m_heap = heap;
		m_tblDef = tblDef;
	}

	virtual void run() {
		// ���¶�ĳ�����ݽ���һ�����µĲ���
		Connection *conn = m_db->getConnection(false);
		Session *session = m_db->getSessionManager()->allocSession("IndexBugsTestCase::testBugQA60214", conn);

		// �������ǰ�����������
		char commonPrefix[500];
		for (uint i = 0; i < 400; i++)
			commonPrefix[i] = 'a';
		commonPrefix[400] = '\0';
		const IndexDef *indexDef = ((DrsBPTreeIndex*)m_indice->getIndex(0))->getIndexDef();
		SubRecord *before, *after;
		u32 updateCount = 8;

		RowId rowId = 8192 + 1000 * updateCount;
		char userName[500];
		sprintf(userName, "%d %s", updateCount, commonPrefix);
		char newUserName[500];
		sprintf(newUserName, "%s %d %d", commonPrefix, 55, 55);

		SubRecordBuilder srbBefore(m_tblDef, REC_REDUNDANT, rowId);
		before = srbBefore.createSubRecordByName("BookName", userName);
		SubRecordBuilder srbAfter(m_tblDef, REC_REDUNDANT, rowId);
		after = srbAfter.createSubRecordByName("BookName", newUserName);

		// ִ�и��²���
		session->startTransaction(TXN_UPDATE, m_tblDef->m_id);
		// ���и���
		uint dupIndex = (uint)-1;
		NTSE_ASSERT(m_indice->updateIndexEntries(session, before, after, false, &dupIndex));
		session->endTransaction(true);

		freeSubRecord(before);
		freeSubRecord(after);

		m_db->getSessionManager()->freeSession(session);
		m_db->freeConnection(conn);
	}

private:
	Database *m_db;
	DrsIndice *m_indice;
	DrsHeap *m_heap;
	const TableDef *m_tblDef;
};

class BugsLockObjectThread : public Thread {
public:
	BugsLockObjectThread(Session *session, const TableDef *tableDef, u64 objectId) : Thread("BugsLockObjectThread") {
		m_session = session;
		m_objectId = objectId;
		m_tableDef = tableDef;
	}

	virtual void run() {
		NTSE_ASSERT(m_session->lockIdxObject(getRealObjectId(m_tableDef, m_objectId)));
		NTSE_ASSERT(m_session->unlockIdxObject(getRealObjectId(m_tableDef, m_objectId)));
	}

private:
	Session *m_session;
	u64 m_objectId;
	const TableDef *m_tableDef;
};


void IndexBugsTestCase::testBugQA60214() {
	prepareDataBug60214();

	/****************************  ���Ը�bug��������������  ********************************/
	// ���ݵ�ǰ�����ļ�
	// �ر����ݿⱸ�������ļ��������ļ���Ȼ�����´�
	Connection *conn = m_db->getConnection(false);
	Session *session = m_db->getSessionManager()->allocSession("IndexBugsTestCase::testBugQA60214", conn);
	m_heap->close(session, true);
	delete m_heap;
	m_indice->close(session, true);
	delete m_indice;
	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(conn);
	m_db->close(true, true);
	delete m_db;
	delete m_tblDef;
	// ���ݵ�ǰ״̬���ݺ������ļ�
	backupHeapFile(HEAP_NAME, HEAP_BACKUP_NAME);
	backupHeapFile(INDEX_NAME, INDEX_BACKUP_NAME);
	backupTblDefFile(TBLDEF_NAME, TBLDEF_BACKUP_NAME);
	// �����ݿ�׼������
	EXCPT_OPER(m_db = Database::open(&m_cfg, 1, -1));
	conn = m_db->getConnection(false);
	session = m_db->getSessionManager()->allocSession("IndexBugsTestCase::testBugQA60214", conn);
	EXCPT_OPER(m_tblDef = TableDef::open(TBLDEF_NAME));
	EXCPT_OPER(m_heap = DrsHeap::open(m_db, session, HEAP_NAME, m_tblDef));
	m_indice = DrsIndice::open(m_db, session, INDEX_NAME, m_tblDef, NULL);
	
	u64 txnStartLsn = (u64)-1, txnEndLsn = (u64)-1;
	txnStartLsn = m_db->getTxnlog()->tailLsn();

	// ִ��һ�λᵼ��ʧ�ܵĸ��²���
	// ���ݵ�ǰҳ��
	byte backupRootPage[Limits::PAGE_SIZE];
	File *file = ((DrsBPTreeIndice*)m_indice)->getFileDesc();
	PageHandle *handle = GET_PAGE(session, file, PAGE_INDEX, IndicePageManager::NON_DATA_PAGE_NUM, Exclusived, m_indice->getDBObjStats(), NULL);
	IndexPage *rootPage = (IndexPage*)handle->getPage();;
	memcpy(backupRootPage, (byte*)rootPage, Limits::PAGE_SIZE);
	session->releasePage(&handle);

	// �ȼ�����SMO����Ϊ����������׼��
	NTSE_ASSERT(session->lockIdxObject(getRealObjectId(m_tblDef, (u64)((DrsBPTreeIndex*)m_indice->getIndex(0))->getIndexId())));

	// ���������̣߳��ȴ��ڸ��²���ļ����׶�֮ǰ
	BugsVolunteer *volunteer = new BugsVolunteer("BugsVolunteer", m_db, m_heap, m_tblDef, m_indice);
	volunteer->enableSyncPoint(SP_IDX_WAIT_FOR_INSERT);
	volunteer->start();
	volunteer->joinSyncPoint(SP_IDX_WAIT_FOR_INSERT);

	// �����������������Ǳ��̵߳ļ���Ҫ�ɹ����ڴ����Ǹ����̳߳�������
	BugsLockObjectThread *bloThread = new BugsLockObjectThread(session, m_tblDef, IndicePageManager::NON_DATA_PAGE_NUM);
	bloThread->start();
	Thread::msleep(2000);	//	ȷ�������߳��Ѿ�ִ��

	// �����̼߳���ִ�У����²���Ӧ���ܳɹ�
	volunteer->disableSyncPoint(SP_IDX_WAIT_FOR_INSERT);
	volunteer->notifySyncPoint(SP_IDX_WAIT_FOR_INSERT);

	// ��ʱ�ͷ�����Դ�����²������Գɹ�
	bloThread->join();
	NTSE_ASSERT(session->unlockIdxObject(getRealObjectId(m_tblDef, (u64)((DrsBPTreeIndex*)m_indice->getIndex(0))->getIndexId())));

	volunteer->join();

	// �Աȸ��½��
	SubRecord *key1 = IndexKey::allocSubRecord(session->getMemoryContext(), m_tblDef->m_indice[0], KEY_COMPRESS);
	SubRecord *key2 = IndexKey::allocSubRecord(session->getMemoryContext(), m_tblDef->m_indice[0], KEY_COMPRESS);
	SubRecord *pkey0 = IndexKey::allocSubRecord(session->getMemoryContext(), m_tblDef->m_indice[0], KEY_PAD);
	DrsIndex *index = m_indice->getIndex(0);
	NTSE_ASSERT(((DrsBPTreeIndex*)index)->verify(session, key1, key2, pkey0, true));

	txnEndLsn = m_db->getTxnlog()->tailLsn();

	/*****************************  ���Ը�bug����޸ĵ�redo��undo����  *********************************/
	// ��ȡ���������ļ���׼�����Իָ�
	m_heap->close(session, true);
	delete m_heap;
	m_indice->close(session, true);
	delete m_indice;
	delete m_tblDef;
	m_tblDef = NULL;
	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(conn);
	m_db->close(true, false);
	delete m_db;
	// �ָ���ǰ״̬���ݺ������ļ�
	myrestoreHeapFile(HEAP_BACKUP_NAME, HEAP_NAME);
	myrestoreHeapFile(INDEX_BACKUP_NAME, INDEX_NAME);
	myrestoreHeapFile(TBLDEF_BACKUP_NAME, TBLDEF_NAME);
	// �����ݿ�׼������
	EXCPT_OPER(m_db = Database::open(&m_cfg, 1, -1));
	conn = m_db->getConnection(false);
	session = m_db->getSessionManager()->allocSession("IndexBugsTestCase::testBugQA60214", conn);
	EXCPT_OPER(m_tblDef = TableDef::open(TBLDEF_NAME));
	EXCPT_OPER(m_heap = DrsHeap::open(m_db, session, HEAP_NAME, m_tblDef));
	m_indice = DrsIndice::open(m_db, session, INDEX_NAME, m_tblDef, NULL);

	conn = m_db->getConnection(false);
	session = m_db->getSessionManager()->allocSession("IndexBugsTestCase::testBugQA60214", conn);

	// ���ȴ�����ʼ��redo����
	IndexRecoveryManager manager(m_db, m_heap, m_tblDef, m_indice);
	manager.setTxnType(TXN_UPDATE);
	manager.redoFromSpecifiedLogs(session, txnStartLsn);
	// ����������ȷ��
	key1 = IndexKey::allocSubRecord(session->getMemoryContext(), m_tblDef->m_indice[0], KEY_COMPRESS);
	key2 = IndexKey::allocSubRecord(session->getMemoryContext(), m_tblDef->m_indice[0], KEY_COMPRESS);
	pkey0 = IndexKey::allocSubRecord(session->getMemoryContext(), m_tblDef->m_indice[0], KEY_PAD);
	index = m_indice->getIndex(0);
	NTSE_ASSERT(((DrsBPTreeIndex*)index)->verify(session, key1, key2, pkey0, true));

	// �ٴ������������undo���в���
	manager.undoFromSpecifiedLogs(session, txnStartLsn, txnEndLsn);
	// ��֤��ȷ��
	NTSE_ASSERT(((DrsBPTreeIndex*)index)->verify(session, key1, key2, pkey0, true));
	file = ((DrsBPTreeIndice*)m_indice)->getFileDesc();
	handle = GET_PAGE(session, file, PAGE_INDEX, IndicePageManager::NON_DATA_PAGE_NUM, Exclusived, m_indice->getDBObjStats(), NULL);
	rootPage = (IndexPage*)handle->getPage();
	IndexPage *oldRootPage = (IndexPage*)backupRootPage;
	NTSE_ASSERT(!memcmp((byte*)backupRootPage + sizeof(IndexPage), (byte*)rootPage + sizeof(IndexPage), Limits::PAGE_SIZE - sizeof(IndexPage)));
	session->releasePage(&handle);

	delete volunteer;
	delete bloThread;

	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(conn);

	cleanDataBug60214();
}

const char* IndexBugsTestCase::getName() {
	return "IndexBugsTestCase";
}

const char* IndexBugsTestCase::getDescription() {
	return "Test all index bugs";
}

bool IndexBugsTestCase::isBig() {
	return false;
}

void IndexBugsTestCase::setUp() {
	m_tblDef = NULL;
	Database::drop(".");
	EXCPT_OPER(m_db = Database::open(&m_cfg, true));
}

void IndexBugsTestCase::tearDown() {
	if (m_db != NULL) {
		m_db->close();
		delete m_db;
		Database::drop(".");
		m_db = NULL;
	}
}

bool IndexBugsTestCase::prepareDataBug60214() {
	Connection *conn = m_db->getConnection(false);
	Session *session = m_db->getSessionManager()->allocSession("IndexBugsTestCase::prepareDataBug60214", conn);

	// �����Ѻ�����
	char tableName[255] = HEAP_NAME;
	File oldheapfile(tableName);
	oldheapfile.remove();
	TableDefBuilder *builder;
	//TableDef *tableDef;
	builder = new TableDefBuilder(1, "BookStore", "Book");
	builder->addColumn("BookId", CT_BIGINT, false)->addColumnS("BookName", CT_VARCHAR, 500, false, false);
	builder->addColumnS("Author", CT_VARCHAR, 128)->addColumn("Pages", CT_INT)->addColumn("Price", CT_INT);
	builder->addIndex("BookName", true, true, false, "BookName", 0, NULL);
	m_tblDef = builder->getTableDef();
	EXCPT_OPER(m_tblDef->writeFile(TBLDEF_NAME));
	EXCPT_OPER(DrsHeap::create(m_db, tableName, m_tblDef));
	EXCPT_OPER(m_heap = DrsHeap::open(m_db, session, tableName, m_tblDef));
	delete builder;
	//delete tableDef;

	char indexName[255] = INDEX_NAME;
	File oldindexfile(indexName);
	oldindexfile.remove();
	DrsIndice::create(indexName, m_tblDef);
	m_indice = DrsIndice::open(m_db, session, indexName, m_tblDef, NULL);
	try {
		for (uint i = 0; i < m_tblDef->m_numIndice; i++) {
			m_indice->createIndexPhaseOne(session, m_tblDef->m_indice[i], m_tblDef, m_heap);
			m_indice->createIndexPhaseTwo(m_tblDef->m_indice[i]);
		}
	} catch (NtseException &e) {
		cout << e.getMessage() << endl;
		CPPUNIT_ASSERT(false);
	}

	char commonPrefix[500];
	for (uint i = 0; i < 400; i++)
		commonPrefix[i] = 'a';
	commonPrefix[400] = '\0';
	// �����������ݣ��ƹ��ѱ���
	// �������Ȳ���ļ�¼��ʾ��������ǰ׺ѹ������֤���������ڲ���1��ҳ��
	// ������������Դ���ǰ׺ѹ�������ݣ�����һ���ܡ����ա���MiniPage������������
	// ����ֵ���������ļ�¼���ݺ�ǰ������ݶ�����ǰ׺ѹ������������ϲ���15����¼�Ϳ���
	uint count = 0;
	uint separating = 17, nextBegining = 50, nextScale = 15;
	uint keys = 0;
	while (true) {
		RowId rowId = 8192 + 1000 * count;
		u64 userId = count * 5;
		char bookName[500];
		if (count <= separating)
			sprintf(bookName, "%d %s", count, commonPrefix);
		else
			sprintf(bookName, "%s %d", commonPrefix, count);
		char author[100] = "eagle";
		u32 pages = System::currentTimeMillis() % 1000;
		u32 price = pages / (System::currentOSThreadID() % 10 + 1);
		RecordBuilder rb(m_tblDef, rowId, REC_REDUNDANT);
		rb.appendBigInt(userId);
		rb.appendVarchar(bookName);
		rb.appendVarchar(author);
		rb.appendInt(pages);
		rb.appendInt(price);

		Record *record = rb.getRecord();
		record->m_rowId = rowId;
		session->startTransaction(TXN_INSERT, m_tblDef->m_id);
		bool successful;
		uint dupIndex;
		EXCPT_OPER(successful = m_indice->insertIndexEntries(session, record, &dupIndex));
		CPPUNIT_ASSERT(successful);
		session->endTransaction(true);
		freeRecord(record);
		
		++keys;

		if (count >= separating) {
			if (count < nextBegining)
				count = nextBegining;
			else
				count = count + 2;
		} else {
			count++;
		}

		if (count >= nextScale * 2 + nextBegining)
			break;
	}

	// ��֤���ݵ���ȷ��
	File *file = ((DrsBPTreeIndice*)m_indice)->getFileDesc();
	PageHandle *handle = GET_PAGE(session, file, PAGE_INDEX, IndicePageManager::NON_DATA_PAGE_NUM, Exclusived, m_indice->getDBObjStats(), NULL);
	IndexPage *rootPage = (IndexPage*)handle->getPage();;
	NTSE_ASSERT(rootPage->m_pageCount == keys);
	NTSE_ASSERT(rootPage->isPageLeaf());
	NTSE_ASSERT(rootPage->m_freeSpace < 200);
	NTSE_ASSERT(*((byte*)rootPage + rootPage->m_dirStart + (rootPage->m_miniPageNum - 1) * IndexPage::MINI_PAGE_DIR_SIZE + 2) == IndexPage::MINI_PAGE_MAX_ITEMS);
	session->releasePage(&handle);

	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(conn);

	return true;
}

void IndexBugsTestCase::cleanDataBug60214() {
	Connection *conn = m_db->getConnection(false);
	Session *session = m_db->getSessionManager()->allocSession("IndexBugsTestCase::prepareDataBug60214", conn);
	// ������
	if (m_indice != NULL) {
		m_indice->close(session, true);
		delete m_indice;
		DrsIndice::drop("testIndex.nti");
	}
	// ����
	if (m_heap != NULL) {
		EXCPT_OPER(m_heap->close(session, true));
		delete m_heap;
		DrsHeap::drop(HEAP_NAME);
	}

	if (m_tblDef != NULL) {
		TableDef::drop(TBLDEF_NAME);
		delete m_tblDef;
		m_tblDef = NULL;
	}

	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(conn);
}
