/**
 * 内存索引扫描性能测试 （已完成 )
 *
 * @author 邵峰(shaofeng@corp.netease.com, sf@163.org)
 */
#ifndef _NTSETEST_MEMORY_INDEXSCAN_H_
#define _NTSETEST_MEMORY_INDEXSCAN_H_


#include "PerfTest.h"
#include "EmptyTestCase.h"

using namespace ntse;


namespace ntseperf {

#define MEM_INDEX_SCAN_REDUCE_RATIO 2
#define MEM_INDEX_SCAN_MMS_MULTIPLE 10

class MemoryIndexScanTest: public EmptyTestCase {
public:
	MemoryIndexScanTest(bool useMms, bool recInMms, bool isVar);

	string getName() const;
	string getDescription() const;
	virtual string getCachedName() const;
	
	virtual void loadData(u64 *totalRecSize, u64 *recCnt);
	virtual void warmUp();
	virtual void run();
	void setUp();

private: 
	u64 scanIndexOneTime();

private:
	bool m_useMms;		/** 表是否使用Mms */
	bool m_recInMms;	/** 所有记录是否在Mms中 */
	bool m_isVar;		/** 是否为变长表 */
};

}

#endif // _NTSETEST_MEMORY_INDEXSCAN_H_
