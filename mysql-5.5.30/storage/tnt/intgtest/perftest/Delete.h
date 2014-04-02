/**
 * DELETE集成测试
 *
 * @author 谢可(xieke@corp.netease.com, ken@163.org)
 */
#ifndef _PREFTEST_DELETE_H_
#define _PREFTEST_DELETE_H_


#include "PerfTest.h"
#include "Generator.h"
#include "heap/Heap.h"
#include "Insert.h"

using namespace ntse;


namespace ntseperf {

/**
 * Delete集成测试用例
 */
class DeletePerfTest: public TestCase {
public:
	DeletePerfTest(Scale scale, double dataSizeFact) {
		m_dataSizeFact = dataSizeFact;
		m_scale = scale;
		m_threadCnt = 500;
	}
protected:
	double m_dataSizeFact;
	Scale m_scale;
	vector<RowId> m_IdRowIdVec;
	Config *m_cfg;			/** 数据库配置 */
	Database *m_db;			/** 数据库 */
	Table *m_currTab;		/** 当前表 */
	u64 m_dataSize;			/** 数据量	 */
	u64	m_recCnt;			/** 记录数	 */
	u64 m_opCnt;			/** 测试用例中操作的记录数 */
	u64 m_totalMillis;		/** 测试操作所占用的运行时间，不包含loadData和tearDown的时间 */
	uint m_threadCnt;		/** 并发线程数 */
	Order m_idOrder;		/** 线程对id的取用顺序 */
	uint m_recCntPerThd;	/** 每线程的记录操作数 */

public:
	virtual string getName() const;
	virtual string getDescription() const;
	
	virtual void loadData(u64 *totalRecSize, u64 *recCnt);
	virtual void warmUp();
	virtual u64 getOpCnt();
	virtual u64 getDataSize();
	virtual u64 getMillis();
	virtual void run();
	virtual void setUp();
	virtual void tearDown();
	virtual bool cacheEnabled() { return true;}
};

class Deleter : public Thread {
public:
	Deleter(vector<RowId> *ridVec, uint threadId, uint delCnt, Database *db, Table *tbl) : Thread("Deleter") {
		m_ridVec = ridVec;
		m_threadId = threadId;
		m_delCnt = delCnt;
		m_db = db;
		m_tbl = tbl;
	}
private:
	vector<RowId> *m_ridVec;
	uint m_threadId;
	uint m_delCnt;
	Database *m_db;
	Table *m_tbl;
protected:
	virtual void run();
};



}

#endif  // #ifndef _PREFTEST_DELETE_H_