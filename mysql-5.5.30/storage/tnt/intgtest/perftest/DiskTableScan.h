/**
 * 外存表扫描性能测试 (已实现)
 *
 * @author 邵峰(shaofeng@corp.netease.com, sf@163.org)
 */
#ifndef _NTSETEST_DISK_TABLESCAN_H_
#define _NTSETEST_DISK_TABLESCAN_H_


#include "PerfTest.h"
#include "EmptyTestCase.h"

using namespace ntse;


namespace ntseperf {

/** MMS命中率比例 */
#define DISK_TABLE_SCAN_MMS_HITRATIO 0.1f

class DiskTableScanTest: public EmptyTestCase {
public:
	DiskTableScanTest(bool useMms, bool recInMms, bool isVar);

	string getName() const;
	string getDescription() const;
	virtual string getCachedName() const;
	
	virtual void loadData(u64 *totalRecSize, u64 *recCnt);
	virtual void warmUp();
	virtual void run();
	void setUp();

public:
	static u32 VOLUMN_RATIO;
private: 
	void scanTableOneTime();

private:
	//Table *m_currTab;	 
	//u64 m_dataSize;		 /** 数据量	*/
	//u64	m_recCnt;		 /** 记录数	*/
	//u64 m_opCnt;
	bool m_useMms;		/** 表是否使用Mms */
	bool m_recInMms;	/** 所有记录是否在Mms中 */
	bool m_isVar;		/** 是否为变长表 */
};

}

#endif // _NTSETEST_DISK_TABLESCAN_H_
