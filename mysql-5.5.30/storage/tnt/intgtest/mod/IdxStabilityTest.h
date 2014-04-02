/**
 *	索引模块稳定性测试
 *
 *	@author 苏斌(bsu@corp.netease.com, naturally@163.org)
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
	// 各类操作权重
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
	// 更新操作权重
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
	// 删除操作权重
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
	// 扫描操作权重
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
	Database *m_db;				/** 数据库对象 */
	MemHeap *m_memHeap;			/** 内存堆对象 */
	const TableDef *m_tableDef;	/** 表定义 */
	DrsIndice *m_indice;		/** 对应的索引 */
};

struct TestEnvVars {
public:
	static const uint BLOG_TABLE_MAX_REC_NUM_BITS;	/** 表出世数据规模值所占的有效位数 */
	static const uint BLOG_TABLE_MAX_REC_NUM;		/** 表初始数据规模 = (1 << BLOG_TABLE_MAX_REC_NUM_BITS) - 1 */

	// 测试环境信息
	static const uint INDEX_MAX_RANGE;				/** 索引操作的最大范围 */
	static const uint WORKING_THREAD_NUM;
	static const uint CHECK_DURATION;
	static const double NULL_RATIO;
	static const double DELETE_RANGE_RATIO;
	static const double UPDATE_RANGE_RATIO;

	static const uint STATUS_DURATION;				/** 打印统计信息的间隔 */
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
	// 供测试线程使用的接口
	void test(Connection *conn, ISTableInfo *isTableInfo);
	Connection* getAConnection();
	void closeAConnection(Connection *conn);

protected:
	// 测试用例
	void testStability();

private:
	// 测试环境控制接口
	void init();
	void clean();
	bool verify(bool simple);
	bool suspendWorkingThreads();
	bool resumeWorkingThreads();
	bool stopWorkingThreads();
	void printStatus(const char *logFile);

	// 索引各个操作接口
	void doInsert(Connection *conn, ISTableInfo *isTableInfo);
	void doUpdate(Connection *conn, ISTableInfo *isTableInfo);
	void doDelete(Connection *conn, ISTableInfo *isTableInfo);
	void doScan(Connection *conn, ISTableInfo *isTableInfo);

	// 辅助函数
	void randomUpdateRecord(Session *session, MemoryContext *memoryContext, TableDef *tableDef, IndexDef *indexDef, DrsIndice *indice, MemHeapRecord *memRec, RowId rowId);
	//void deleteSpecifiedIndexKey(Session *session, MemoryContext *memoryContext, TableDef *tableDef, DrsIndice *indice, MemHeapRecord *memRec, RowId rowId, DrsIndexScanHandle *handle);
	void deleteSpecifiedIndexKey(Session *session, MemoryContext *memoryContext, TableDef *tableDef, DrsIndice *indice, MemHeapRecord *memRec, RowId rowId, IndexScanHandle *handle);
	MemHeapRecord *getRandMemRecord(Session *session, MemHeap *memHeap, RowLockHandle **rlh, LockMode lockMode);
	// MemHeap Id和RowId转换接口
	RowId generateRowIdFromSlotNo(uint SlotNo);
	uint getSlotNoFromRowId(RowId rowId);

public:
	bool m_keepWorking;				/** true表示用例应该继续执行，false表示停止 */

private:
	ResLogger *m_resLogger;			/** 数据库资源监控 */
	ISTableInfo **m_tableInfo;		/** 指向各个表信息 */
	uint m_tables;					/** 包含表的个数 */
	Database *m_db;					/** 数据库对象 */
	Config *m_config;				/** 配置信息 */
	Atomic<int> m_atomic;			/** 原子变量，用于生成RowId */

	WorkingThread **m_workingThread;		/** 稳定性测试线程 */
	StatusThread *m_statusThread;			/** 执行统计信息的线程 */

	TotalOpGenerator *m_totalGenerator;		/** 各类操作总生成器 */
	ScanOpGenerator *m_scanGenerator;		/** 扫描操作生成器 */
	DeleteOpGenerator *m_deleteGenerator;	/** 删除操作生成器 */
	UpdateOpGenerator *m_updateGenerator;	/** 更新操作生成器 */

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
	IndexStabilityTestCase *m_testcase;	/** 稳定性实例 */
	ISTableInfo *m_tableInfo;			/** 线程操作的表信息 */
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
	IndexStabilityTestCase *m_testcase;	/** 稳定性实例 */

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