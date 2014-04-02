/**
 * UPDATE集成测试
 *
 * @author 谢可(xieke@corp.netease.com, ken@163.org)
 */
#ifndef _PREFTEST_UPDATE_H_
#define _PREFTEST_UPDATE_H_


#include "PerfTest.h"
#include "Generator.h"
#include "heap/Heap.h"
#include "Insert.h"
#include "CountTable.h"
#include "AccountTable.h"
#include "LongCharTable.h"

using namespace ntse;


namespace ntseperf {

/**
 * 定长表并发update更新测试
 */
class UpdatePerfTest_1: public TestCase {
public:
	UpdatePerfTest_1(Scale scale, bool useMms, bool delayUpdate, uint threadCnt, int threadLoop = 10000) {
		//m_scale = MEDIUM;
		m_scale = scale;
		m_threadCnt = threadCnt;
		m_useMms = useMms;
		m_delayUpd = delayUpdate;
		m_tblName = TABLE_NAME_COUNT;
		m_thdLoop = threadLoop;
	}
protected:
	//double m_dataSizeFact;
	Scale m_scale;
	vector<RowId> m_IdRowIdVec;
	Config *m_cfg;
	Database *m_db;
	Table *m_currTab;
	u64 m_dataSize;
	u64	m_recCnt;	
	u64 m_opCnt;	
	u64 m_totalMillis;
	uint m_threadCnt;
	int m_thdLoop;
	bool m_useMms;
	bool m_delayUpd;
	const char * m_tblName;

public:
	virtual string getName() const;
	virtual string getDescription() const;
	
	virtual void loadData(u64 *totalRecSize, u64 *recCnt);
	virtual void warmUp();
	virtual u64 getOpCnt() { return m_opCnt; };
	virtual u64 getDataSize() { return 0; };
	virtual u64 getMillis() { return m_totalMillis; };
	virtual void run();
	virtual void tearDown();
};


/**
 * 变长表并发UPDATE性能测试
 */
class UpdatePerfTest_2 : public UpdatePerfTest_1 {
protected:
	int m_recAvgSize;
	vector<int> m_recSize;
public:
	UpdatePerfTest_2(Scale scale, bool useMms, bool delayUpdate, uint threadCnt, int threadLoop = 10000) : UpdatePerfTest_1(scale, useMms, delayUpdate, threadCnt, threadLoop) {
		m_recAvgSize = 12;
		m_tblName = TABLE_NAME_ACCOUNT;
	}
	
	virtual void loadData(u64 *totalRecSize, u64 *recCnt);
	virtual void warmUp();

	virtual void run();
	virtual void tearDown();

};


/**
 * 定长表并发UPDATE性能/更新主键
 */
class UpdatePerfTest_3: public TestCase {
public:
	UpdatePerfTest_3(Scale scale, bool useMms, uint threadCnt, int threadLoop = 10000) {
		m_scale = scale;
		m_threadCnt = threadCnt;
		m_useMms = useMms;
		m_tblName = TABLE_NAME_COUNT;
		m_thdLoop = threadLoop;
	}
protected:
	//double m_dataSizeFact;
	Scale m_scale;
	vector<RowId> m_IdRowIdVec;
	Config *m_cfg;
	Database *m_db;
	Table *m_currTab;
	u64 m_dataSize;
	u64	m_recCnt;	
	u64 m_opCnt;	
	u64 m_totalMillis;
	uint m_threadCnt;
	int m_thdLoop;
	bool m_useMms;
	//bool m_delayUpd;
	u64 m_maxID;
	const char * m_tblName;

public:
	virtual string getName() const;
	virtual string getDescription() const;
	virtual void setUp();
	virtual bool cacheEnabled() { return true;}
	virtual void loadData(u64 *totalRecSize, u64 *recCnt);
	virtual void warmUp();
	virtual u64 getOpCnt();// { return m_opCnt; };
	virtual u64 getDataSize();// { return m_dataSize; };
	virtual u64 getMillis() { return m_totalMillis; };
	virtual void run();
	virtual void tearDown();
};


/**
 * 变长表并发UPDATE性能/更新主键
 */
class UpdatePerfTest_4: public UpdatePerfTest_3 {
public:
	UpdatePerfTest_4(Scale scale, bool useMms, uint threadCnt, int threadLoop = 10000) : UpdatePerfTest_3(scale, useMms, threadCnt, threadLoop) {
		m_avgRecSize = 12;
		m_tblName = TABLE_NAME_ACCOUNT;
	}
	virtual void loadData(u64 *totalRecSize, u64 *recCnt);
	virtual bool cacheEnabled() { return true;}
	virtual void setUp();
	virtual void warmUp();
	virtual void run();
	virtual void tearDown();
	virtual u64 getDataSize();
protected:
	uint m_avgRecSize;
};


/**
 * 定长表UPDATE性能/更新的IO效率
 */
class UpdatePerfTest_5: public TestCase {
public:
	UpdatePerfTest_5(Scale scale, uint threadCnt, double dataSizeRatio, int threadLoop = 10000) {
		m_scale = scale;
		m_threadCnt = threadCnt;
		m_useMms = true;
		//m_dataSizeFact = 100.0;
		m_dataSizeFact = dataSizeRatio;
		m_tblName = TABLE_NAME_LONGCHAR;
		m_thdLoop = threadLoop;
	}
protected:
	Scale m_scale;
	//vector<RowId> m_IdRowIdVec;
	Config *m_cfg;
	Database *m_db;
	Table *m_currTab;
	u64 m_dataSize;
	u64	m_recCnt;	
	u64 m_opCnt;	
	u64 m_totalMillis;
	uint m_threadCnt;
	int m_thdLoop;
	bool m_useMms;
	double m_dataSizeFact;
	//bool m_delayUpd;
	u64 m_maxID;
	vector<u64> m_idVec;
	const char *m_tblName;

public:
	virtual string getName() const;
	virtual string getDescription() const;
	
	virtual void loadData(u64 *totalRecSize, u64 *recCnt);
	virtual void warmUp();
	virtual u64 getOpCnt() { return m_opCnt; };
	virtual u64 getDataSize() { return 0; }; // 更新，记录数据量不好计算
	virtual u64 getMillis() { return m_totalMillis; };
	virtual bool cacheEnabled() { return true;}
	virtual void run();
	virtual void tearDown();
	virtual void setUp();
};


/**
 * 变长表UPDATE性能/更新的IO效率
 */
class UpdatePerfTest_6 : public UpdatePerfTest_5 {
public:
	UpdatePerfTest_6(Scale scale, uint threadCnt, double dataSizeRatio, int threadLoop = 10000)
		: UpdatePerfTest_5(scale, threadCnt, dataSizeRatio, threadLoop) {
		m_scale = scale;
		m_useMms = true;
		m_tblName = TABLE_NAME_ACCOUNT;
		m_avgRecSize = 200;
	}
	virtual void loadData(u64 *totalRecSize, u64 *recCnt);
	virtual void setUp();
	virtual void warmUp();
	virtual void run();
	virtual void tearDown();
protected:
	uint m_avgRecSize;
};


}

#endif //#ifndef _PREFTEST_UPDATE_H_
