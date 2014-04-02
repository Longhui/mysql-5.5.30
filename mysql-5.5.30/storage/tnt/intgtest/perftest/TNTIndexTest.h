/**
* TNT全表扫描性能测试
* 
* @author hw(hzhuwei@corp.netease.com)
*/
#ifndef _TNTTEST_INDEX_H_
#define _TNTTEST_INDEX_H_

#include "PerfTest.h"
#include "TNTEmptyTestCase.h"
#include "Generator.h"

using namespace ntse;
using namespace tnt;

namespace tntperf{

#define TNT_INDEX_SCAN_REDUCE_RATIO 2
#define TNT_INDEX_SCAN_MMS_MULTIPLE 10

enum TNT_INDEX_TESTCASE {
	PURGE_TABLE,
	BEGIN_END_SCAN,
	UNIQUE_SCAN,
	RANGE_SCAN,
	TABLE_SCAN
};

class TNTIndexTest: public TNTEmptyTestCase
{
public:
	TNTIndexTest(const char * tableName, Scale scale, bool useMms, bool recInMms, bool isVar, enum TNT_INDEX_TESTCASE testcase);

	string getName() const;
	string getDescription() const;
	virtual string getCachedName() const;	
	virtual void loadData(u64 *totalRecSize, u64 *recCnt);
	virtual void warmUp();
	virtual void run();
	virtual u64 getMillis();
	void setUp();
	void tearDown();

public:
	//static u32 VOLUMN_RATIO;
private: 
	void scanTableOneTime();
	void insertRecord(TNTDatabase *db, TNTTable *table,  u64 count);
	void updateTable(TNTDatabase *db, TNTTable *table);
	char* randomStr(size_t size);

	//以下是各测试方法
	void purgeTable(TNTDatabase *db, TNTTable *table);
	void beginAndEndIdxScan(TNTDatabase *db, TNTTable *table);
	void uniqueIdxScan(TNTDatabase *db, TNTTable *table);
	void rangeIdxScan(TNTDatabase *db, TNTTable *table);
	void tableRangeScan(TNTDatabase *db, TNTTable *table);
protected:
	bool m_useMms;		/** 表是否使用Mms */
	bool m_recInMms;	/** 所有记录是否在Mms中 */
	bool m_isVar;		/** 是否为变长表 */

	bool m_backTable;   /** 查询是否回表*/
	bool m_ntseTest;    /** 是否直接调用NTSE接口操作 */

	TNTDatabase *m_db;
	TNTTable    *m_table;
	const char  *m_tableName;/** 表名 */
	TestTable   m_testTable;	/** 测试用表 */
	Scale       m_scale;		/** 测试规模 */
	double      m_ratio;        /** buffer用于记录的使用比率 */
	u64			m_dataSize;		/** 数据量	 */
	u64			m_recCnt;		/** 记录数	 */
	//u64			m_opCnt;		/** 测试用例中操作的记录数 */
	u64			m_totalMillis;	/** 测试操作所占用的运行时间，不包含loadData和tearDown的时间 */
	static uint  BATCH_OP_SIZE;

	enum TNT_INDEX_TESTCASE m_testcase;
};

}

#endif // _TNTTEST_INDEXSCAN_H_