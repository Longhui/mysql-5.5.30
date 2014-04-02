#ifndef _PREFTEST_TNT_UPDATE_H_
#define _PREFTEST_TNT_UPDATE_H_

#include "PerfTest.h"
#include "Generator.h"
#include "TNTCountTable.h"
#include "TNTAccountTable.h"
#include "api/TNTDatabase.h"
#include "api/TNTTable.h"

using namespace tnt;
using namespace ntseperf;

namespace tntperf {
/**
 * 定长表并发UPDATE性能/更新主键
 */
class TNTFLRUpdatePerfTest: public TestCase {
public:
	TNTFLRUpdatePerfTest(Scale scale, bool useMms, uint threadCnt, int threadLoop = 10000) {
		m_scale = scale;
		m_threadCnt = threadCnt;
		m_useMms = useMms;
		m_tblName = TNTTABLE_NAME_COUNT;
		m_thdLoop = threadLoop;
	}
protected:
	//double m_dataSizeFact;
	Scale m_scale;
	vector<RowId> m_IdRowIdVec;
	TNTConfig *m_cfg;
	TNTDatabase *m_db;
	TNTTable *m_currTab;
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
class TNTVLRUpdatePerfTest: public TNTFLRUpdatePerfTest {
public:
	TNTVLRUpdatePerfTest(Scale scale, bool useMms, uint threadCnt, int threadLoop = 10000) : TNTFLRUpdatePerfTest(scale, useMms, threadCnt, threadLoop) {
		m_avgRecSize = 12;
		m_tblName = TNTTABLE_NAME_ACCOUNT;
	}
	virtual string getName() const;
	virtual string getDescription() const;
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
}
#endif