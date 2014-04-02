/**
 *	����ģ���ȶ��Բ���
 *
 *	@author �ձ�(bsu@corp.netease.com, naturally@163.org)
 */


#ifndef _NTSE_IDXSTABILITY_TEST_H_
#define _NTSE_IDXSTABILITY_TEST_H_

#include <cppunit/extensions/HelperMacros.h>
#include "api/Database.h"
#include "util/Sync.h"
#include "btree/Index.h"
#include "util/File.h"
#include "MemHeap.h"
#include "ResLogger.h"

using namespace ntse;

class WorkingThread;
class StatusThread;

class OpGenerator {
public:
	OpGenerator() {}
	virtual ~OpGenerator();

	virtual uint getOp(uint *weights = NULL) {
		if (weights == NULL)
			return getOpKind(m_weights, m_kinds);
		return getOpKind(weights, m_kinds);
	}

	void setSomeWeight(uint weight, uint wNo) {
		if (wNo >= m_kinds)
			return;
		m_weights[wNo] = weight;
	}

protected:
	uint getOpKind(uint *weights, uint opkinds, uint total = 100);

protected:
	uint *m_weights;
	uint m_kinds;
};

class TotalOpGenerator : public OpGenerator {
public:
	TotalOpGenerator();

private:
	// �������Ȩ��
	static const uint IDX_INSERT_WEIGHT;
	static const uint IDX_DELETE_WEIGHT;
	static const uint IDX_UPDATE_WEIGHT;
	static const uint IDX_SCAN_WEIGHT;

public:
	static const uint IDX_INSERT = 0;
	static const uint IDX_DELETE = 1;
	static const uint IDX_UPDATE = 2;
	static const uint IDX_SCAN = 3;
};

class UpdateOpGenerator : public OpGenerator {
public:
	UpdateOpGenerator();

private:
	// ���²���Ȩ��
	static const uint IDX_UPDATE_FROM_TABLE_WEIGHT;
	static const uint IDX_UPDATE_SINGLE_WEIGHT;
	static const uint IDX_UPDATE_RANGE_FORWARD_WEIGHT;
	static const uint IDX_UPDATE_RANGE_BACKWARD_WEIGHT;

public:
	static const uint UPDATE_FROM_TABLE = 0;
	static const uint UPDATE_SINGLE = 1;
	static const uint UPDATE_RANGE_FORWARD = 2;
	static const uint UPDATE_RANGE_BACKWARD = 3;
};

class DeleteOpGenerator : public OpGenerator {
public:
	DeleteOpGenerator();

private:
	// ɾ������Ȩ��
	static const uint IDX_DELETE_FROM_TABLE_WEIGHT;
	static const uint IDX_DELETE_SINGLE_WEIGHT;
	static const uint IDX_DELETE_RANGE_FORWARD_WEIGHT;
	static const uint IDX_DELETE_RANGE_BACKWARD_WEIGHT;

public:
	static const uint DELETE_FROM_TABLE = 0;
	static const uint DELETE_SINGLE = 1;
	static const uint DELETE_RANGE_FORWARD = 2;
	static const uint DELETE_RANGE_BACKWARD = 3;
};

class ScanOpGenerator : public OpGenerator {
public:
	ScanOpGenerator();

private:
	// ɨ�����Ȩ��
	static const uint IDX_SCAN_SINGLE_WEIGHT;
	static const uint IDX_SCAN_RANGE_FORWARD_WEIGHT;
	static const uint IDX_SCAN_RANGE_BACKWARD_WEIGHT;

public:
	static const uint SCAN_SINGLE = 0;
	static const uint SCAN_RANGE_FORWARD = 1;
	static const uint SCAN_RANGE_BACKWARD = 2;
};

class ISTableInfo {
public:
	ISTableInfo(Database *db, const TableDef *tableDef) : m_tableDef(tableDef), m_db(db) {
		m_indice = NULL;
		m_memHeap = NULL;
	}

	~ISTableInfo() {
		if (m_memHeap)
			delete m_memHeap;
		if (m_indice) {
			Connection *conn = m_db->getConnection(false);
			Session *session = m_db->getSessionManager()->allocSession("EmptyTestCase::openTable", conn);
			m_indice->close(session, true);
			m_db->getSessionManager()->freeSession(session);
			m_db->freeConnection(conn);
			delete m_indice;
		}
	}

	bool createHeap(uint maxRecs, u16 numCkCols = 0, u16 *ckCols = NULL);
	bool createIndex();

	MemHeap* getMemHeap() { return m_memHeap; }
	DrsIndice* getIndice() { return m_indice; }
	const TableDef* getTableDef() { return m_tableDef; }

private:
	Database *m_db;				/** ���ݿ���� */
	MemHeap *m_memHeap;			/** �ڴ�Ѷ��� */
	const TableDef *m_tableDef;	/** ���� */
	DrsIndice *m_indice;		/** ��Ӧ������ */
};

struct TestEnvVars {
public:
	static const uint BLOG_TABLE_MAX_REC_NUM_BITS;	/** ��������ݹ�ģֵ��ռ����Чλ�� */
	static const uint BLOG_TABLE_MAX_REC_NUM;		/** ���ʼ���ݹ�ģ = (1 << BLOG_TABLE_MAX_REC_NUM_BITS) - 1 */

	// ���Ի�����Ϣ
	static const uint INDEX_MAX_RANGE;				/** �������������Χ */
	static const uint WORKING_THREAD_NUM;
	static const uint CHECK_DURATION;
	static const double NULL_RATIO;
	static const double DELETE_RANGE_RATIO;
	static const double UPDATE_RANGE_RATIO;

	static const uint STATUS_DURATION;				/** ��ӡͳ����Ϣ�ļ�� */
};


class IndexStabilityTestCase : public CPPUNIT_NS::TestFixture {
	CPPUNIT_TEST_SUITE(IndexStabilityTestCase);
	CPPUNIT_TEST(testStability);
	CPPUNIT_TEST_SUITE_END();

public:
	static const char* getName();
	static const char* getDescription();
	static bool isBig();
	void setUp();
	void tearDown();

public:
	// �������߳�ʹ�õĽӿ�
	void test(Connection *conn, ISTableInfo *isTableInfo);
	Connection* getAConnection();
	void closeAConnection(Connection *conn);

protected:
	// ��������
	void testStability();

private:
	// ���Ի������ƽӿ�
	void init();
	void clean();
	bool verify(bool simple);
	bool suspendWorkingThreads();
	bool resumeWorkingThreads();
	bool stopWorkingThreads();
	void printStatus(const char *logFile);

	// �������������ӿ�
	void doInsert(Connection *conn, ISTableInfo *isTableInfo);
	void doUpdate(Connection *conn, ISTableInfo *isTableInfo);
	void doDelete(Connection *conn, ISTableInfo *isTableInfo);
	void doScan(Connection *conn, ISTableInfo *isTableInfo);

	// ��������
	void randomUpdateRecord(Session *session, MemoryContext *memoryContext, TableDef *tableDef, IndexDef *indexDef, DrsIndice *indice, MemHeapRecord *memRec, RowId rowId);
	//void deleteSpecifiedIndexKey(Session *session, MemoryContext *memoryContext, TableDef *tableDef, DrsIndice *indice, MemHeapRecord *memRec, RowId rowId, DrsIndexScanHandle *handle);
	void deleteSpecifiedIndexKey(Session *session, MemoryContext *memoryContext, TableDef *tableDef, DrsIndice *indice, MemHeapRecord *memRec, RowId rowId, IndexScanHandle *handle);
	MemHeapRecord *getRandMemRecord(Session *session, MemHeap *memHeap, RowLockHandle **rlh, LockMode lockMode);
	// MemHeap Id��RowIdת���ӿ�
	RowId generateRowIdFromSlotNo(uint SlotNo);
	uint getSlotNoFromRowId(RowId rowId);

public:
	bool m_keepWorking;				/** true��ʾ����Ӧ�ü���ִ�У�false��ʾֹͣ */

private:
	ResLogger *m_resLogger;			/** ���ݿ���Դ��� */
	ISTableInfo **m_tableInfo;		/** ָ���������Ϣ */
	uint m_tables;					/** ������ĸ��� */
	Database *m_db;					/** ���ݿ���� */
	Config *m_config;				/** ������Ϣ */
	Atomic<int> m_atomic;			/** ԭ�ӱ�������������RowId */

	WorkingThread **m_workingThread;		/** �ȶ��Բ����߳� */
	StatusThread *m_statusThread;			/** ִ��ͳ����Ϣ���߳� */

	TotalOpGenerator *m_totalGenerator;		/** ��������������� */
	ScanOpGenerator *m_scanGenerator;		/** ɨ����������� */
	DeleteOpGenerator *m_deleteGenerator;	/** ɾ������������ */
	UpdateOpGenerator *m_updateGenerator;	/** ���²��������� */

friend class StatusThread;
};


class WorkingThread : public Task {
public:
	WorkingThread(IndexStabilityTestCase *testcase, ISTableInfo *tableInfo) : Task("Working thread", 0), 
		m_testcase(testcase), m_tableInfo(tableInfo) {		
	}

	virtual void run() {
		Connection *conn = m_testcase->getAConnection();
		m_testcase->test(conn, m_tableInfo);
		m_testcase->closeAConnection(conn);
	}

private:
	IndexStabilityTestCase *m_testcase;	/** �ȶ���ʵ�� */
	ISTableInfo *m_tableInfo;			/** �̲߳����ı���Ϣ */
};

class StatusThread : public Task {
public:
	StatusThread(IndexStabilityTestCase *testcase, uint interval) : Task("Status thread", interval), m_testcase(testcase) {
		File f(STATUS_LOG);
		f.remove();
	}

	virtual void run() {
		m_testcase->printStatus(STATUS_LOG);
	}

private:
	IndexStabilityTestCase *m_testcase;	/** �ȶ���ʵ�� */

	static const char* STATUS_LOG;
};

class RecordHelper {
public:
	static SubRecord* generateSubRecord(TableDef *tableDef, u16 *columns, u16 numCols, u16 maxSize, RecFormat format, RowId rowId = INVALID_ROW_ID);
	static Record* generateRecord(TableDef *tableDef, RowId rowId = INVALID_ROW_ID);
	static Record* generateNewRecord(TableDef *tableDef, RowId rid);
	static Record* updateRecord(TableDef *tableDef, Record *rec, RowId rid, IndexDef *indexDef, u16 **updateCols, u16 *numUpdCols);
	static void formUpdateSubRecords(TableDef *tableDef, IndexDef *indexDef, Record *record, SubRecord **before, SubRecord **after);
	static SubRecord* formFindKeyFromRecord(Session *session, TableDef *tableDef, IndexDef *indexDef, MemHeapRecord *record);
	static void writeAColumn(TableDef *tableDef, ColumnDef *columnDef, byte *data, uint colNo, u64 ranINT);
	static char* getLongChar(size_t size, const TableDef *tableDef, const ColumnDef *columnDef, uint ranINT);
	static SubRecord* makeRandomFindKey(TableDef *tableDef, IndexDef *indexDef);
};

#endif