#ifndef _PREFTEST_TNT_INSERT_H_
#define _PREFTEST_TNT_INSERT_H_

#include "PerfTest.h"
#include "api/TNTDatabase.h"
#include "api/TNTTable.h"
#include "Generator.h"

using namespace tnt;
using namespace ntseperf;

namespace tntperf {
class TNTInsertPerfTest: public TestCase {
	public:
		TNTInsertPerfTest(const char * tableName, Scale scale, uint threadCount, Order idOrder, double dataSizeFact);
		~TNTInsertPerfTest(void);
		virtual string getName() const;
		virtual string getDescription() const;

		virtual void loadData(u64 *totalRecSize, u64 *recCnt);
		virtual void warmUp();
		virtual u64 getOpCnt() = 0;
		virtual u64 getDataSize() = 0;
		virtual u64 getMillis() = 0;
		virtual void run();
		virtual void setUp();
		virtual void tearDown();

	protected:
		TNTDatabase *m_db;
		TNTTable    *m_table;
		const char  *m_tableName;/** 表名 */
		TestTable   m_testTable;	/** 测试用表 */
		Scale       m_scale;		/** 测试规模 */
		TNTConfig	*m_cfg;			/** 数据库配置 */
		u64			m_dataSize;		/** 数据量	 */
		u64			m_recCnt;		/** 记录数	 */
		u64			m_opCnt;		/** 测试用例中操作的记录数 */
		u64			m_totalMillis;	/** 测试操作所占用的运行时间，不包含loadData和tearDown的时间 */
		uint		m_threadCnt;	/** 并发线程数 */
		Order		m_idOrder;		/** 线程对id的取用顺序 */
		uint		m_recCntPerThd;	/** 每线程的记录操作数 */
		double		m_dataSizeFact;	/** 数据量大小和buffer size大小的比值 */
		double      m_tntRatio;

		/* 模板方法 */
		virtual TNTConfig *getTNTConfig();
		virtual void createTableAndRecCnt(Session *session);
		virtual uint getRecordSize();
};

class TNTFLRInsertTest: public TNTInsertPerfTest {
public:
	TNTFLRInsertTest(const char * tableName, Scale scale, uint threadCount, Order idOrder, double dataSizeFact):
	  TNTInsertPerfTest(tableName, scale, threadCount, idOrder, dataSizeFact) {
	}
public:
	virtual u64 getOpCnt();
	virtual u64 getDataSize();
	virtual u64 getMillis();
};

class TNTVLRInsertTest: public TNTFLRInsertTest {
public:
	TNTVLRInsertTest(const char * tableName, Scale scale, uint threadCount, Order idOrder, double dataSizeFact):
	  TNTFLRInsertTest(tableName, scale, threadCount, idOrder, dataSizeFact) {};
};
}
#endif
