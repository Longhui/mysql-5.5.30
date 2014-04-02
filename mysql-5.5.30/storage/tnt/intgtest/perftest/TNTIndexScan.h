/**
* TNT全表扫描性能测试
* 
* @author 赵继远(hzzhaojy@corp.netease.com)
*/
#ifndef _TNTTEST_INDEXSCAN_H_
#define _TNTTEST_INDEXSCAN_H_

#include "PerfTest.h"
#include "TNTEmptyTestCase.h"

using namespace ntse;
using namespace tnt;

namespace tntperf{

    #define TNT_INDEX_SCAN_REDUCE_RATIO 2
    #define TNT_INDEX_SCAN_MMS_MULTIPLE 10

	class TNTIndexScanTest: public TNTEmptyTestCase
	{
public:
	     TNTIndexScanTest(bool useMms, bool recInMms, bool isVar);
	    
		 string getName() const;
	     string getDescription() const;
	     virtual string getCachedName() const;	
	     virtual void loadData(u64 *totalRecSize, u64 *recCnt);
	     virtual void warmUp();
	     virtual void run();
	     void setUp();

public:
	     //static u32 VOLUMN_RATIO;
private: 
	     void scanTableOneTime();

private:
	     bool m_useMms;		/** 表是否使用Mms */
	     bool m_recInMms;	/** 所有记录是否在Mms中 */
	     bool m_isVar;		/** 是否为变长表 */
	};

}

#endif // _TNTTEST_INDEXSCAN_H_