/**
* TNTȫ��ɨ�����ܲ���
* 
* @author �Լ�Զ(hzzhaojy@corp.netease.com)
*/
#ifndef _TNTTEST_TABLESCAN_H_
#define _TNTTEST_TABLESCAN_H_

#include "PerfTest.h"
#include "TNTEmptyTestCase.h"

using namespace ntse;
using namespace tnt;

namespace tntperf{

    #define TNT_TABLE_SCAN_REDUCE_RATIO 2
    #define TNT_TABLE_SCAN_MMS_MULTIPLE 10

	class TNTTableScanTest: public TNTEmptyTestCase
	{
public:
	     TNTTableScanTest(bool useMms, bool recInMms, bool isVar);
	    
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
	     //void scanTableOneTime();

private:
	     bool m_useMms;		/** ���Ƿ�ʹ��Mms */
	     bool m_recInMms;	/** ���м�¼�Ƿ���Mms�� */
	     bool m_isVar;		/** �Ƿ�Ϊ�䳤�� */
	};

}

#endif // _TNTTEST_TABLESCAN_H_