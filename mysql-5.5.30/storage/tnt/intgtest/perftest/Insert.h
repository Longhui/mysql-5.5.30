/**
 * INSERT集成测试
 *
 * @author 谢可(xieke@corp.netease.com, ken@163.org)
 */
#ifndef _PREFTEST_INSERT_H_
#define _PREFTEST_INSERT_H_


#include "PerfTest.h"
#include "Generator.h"
#include "heap/Heap.h"

using namespace ntse;


namespace ntseperf {

/**
 * insert集成测试用例
 */
class InsertPerfTest: public TestCase {
public:
	InsertPerfTest(const char * tableName, HeapVersion ver, Scale scale, uint threadCount, Order idOrder, double dataSizeFact);

	virtual string getName() const;
	virtual string getDescription() const;
	
	virtual void loadData(u64 *totalRecSize, u64 *recCnt);
	virtual void warmUp();
	virtual u64 getOpCnt() = 0;
	virtual u64 getDataSize() = 0;
	virtual u64 getMillis() = 0;
	virtual void run();
	virtual void tearDown();


protected:
	const char *m_tableName;/** 表名 */
	TestTable m_testTable;	/** 测试用表 */
	Database *m_db;			/** 数据库 */
	HeapVersion m_heapVer;	/** 表版本 */
	Scale m_scale;			/** 测试规模 */
	Table *m_currTab;		/** 当前表 */
	Config *m_cfg;			/** 数据库配置 */
	u64 m_dataSize;			/** 数据量	 */
	u64	m_recCnt;			/** 记录数	 */
	u64 m_opCnt;			/** 测试用例中操作的记录数 */
	u64 m_totalMillis;		/** 测试操作所占用的运行时间，不包含loadData和tearDown的时间 */
	uint m_threadCnt;		/** 并发线程数 */
	Order m_idOrder;		/** 线程对id的取用顺序 */
	uint m_recCntPerThd;	/** 每线程的记录操作数 */
	double m_dataSizeFact;	/** 数据量大小和buffer size大小的比值 */

	/* 模板方法 */
	virtual Config *getConfig();
	virtual void createTableAndRecCnt(Session *session);
	virtual uint getRecordSize();
};


class FLRInsertTest: public InsertPerfTest {
public:
	FLRInsertTest(const char * tableName, Scale scale, uint threadCount, Order idOrder, double dataSizeFact):InsertPerfTest(tableName, HEAP_VERSION_FLR, scale, threadCount, idOrder, dataSizeFact) {
	}
public:
	virtual u64 getOpCnt();
	virtual u64 getDataSize();
	virtual u64 getMillis();
};


class VLRInsertTest: public FLRInsertTest {
public:
	VLRInsertTest(const char * tableName, Scale scale, uint threadCount, Order idOrder, double dataSizeFact):FLRInsertTest(tableName, scale, threadCount, idOrder, dataSizeFact) {};
};



}




#endif  // #ifndef _PREFTEST_INSERT_H_
