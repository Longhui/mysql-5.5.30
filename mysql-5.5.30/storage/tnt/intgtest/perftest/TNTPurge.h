#ifndef _PREFTEST_TNT_PURGE_H_
#define _PREFTEST_TNT_PURGE_H_

#include "PerfTest.h"
#include "api/TNTDatabase.h"
#include "api/TNTTable.h"
#include "TNTEmptyTestCase.h"
#include "Generator.h"

using namespace tnt;
using namespace ntseperf;

namespace tntperf {
class TNTPurgePerfTest: public TNTEmptyTestCase {
public:
	TNTPurgePerfTest(const char * tableName, Scale scale, bool useMms);
	~TNTPurgePerfTest(void);
	virtual string getName() const;
	virtual string getDescription() const;
	virtual void setUp();
	virtual void tearDown();

	virtual void loadData(u64 *totalRecSize, u64 *recCnt);
	virtual void warmUp();
	virtual u64 getOpCnt();
	virtual u64 getDataSize();
	virtual u64 getMillis();
	virtual void run();

protected:
	//TNTDatabase *m_db;
	//TNTTable    *m_table;
	const char  *m_tableName;/** 表名 */
	TestTable   m_testTable;	/** 测试用表 */
	Scale       m_scale;		/** 测试规模 */
	double      m_ratio;        /** buffer用于记录的使用比率 */
	u64			m_dataSize;		/** 数据量	 */
	u64			m_recCnt;		/** 记录数	 */
	//u64			m_opCnt;		/** 测试用例中操作的记录数 */
	u64			m_totalMillis;	/** 测试操作所占用的运行时间，不包含loadData和tearDown的时间 */
	bool        m_useMms;
	static const uint  BATCH_OP_SIZE = 1000;

	/* 模板方法 */
	//virtual TNTConfig *getTNTConfig();
	//virtual void createTableAndRecCnt(Session *session);
	//virtual uint getRecordSize();
};

}
#endif
