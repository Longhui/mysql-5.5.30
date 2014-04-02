/**
* TNT全表扫描性能测试
* 
* @author 赵继远(hzzhaojy@corp.netease.com)
*/
#ifndef _TNTTEST_DELETE_H_
#define _TNTTEST_DELETE_H_

#include "PerfTest.h"
#include "TNTEmptyTestCase.h"
#include "PerfTest.h"
#include "Generator.h"
#include "heap/Heap.h"
#include "Insert.h"

using namespace ntse;
using namespace tnt;

namespace tntperf {

    //#define TNT_TABLE_SCAN_REDUCE_RATIO 2
    //#define TNT_TABLE_SCAN_MMS_MULTIPLE 10

class TNTDeleteTest: public TNTEmptyTestCase
{

public:
	     TNTDeleteTest(Scale scale, double dataSizeFact) {
		     m_dataSizeFact = dataSizeFact;
		     m_scale = scale;
		     m_threadCnt = 1;
	     }
	    
		 string getName() const;
	     string getDescription() const;
	     //virtual string getCachedName() const;	
	     virtual void loadData(u64 *totalRecSize, u64 *recCnt);
	     virtual void warmUp();
	     virtual void run();
	     virtual void setUp();
		 //virtual void tearDown();
		 virtual u64 getOpCnt();
	     virtual u64 getDataSize();
	     virtual u64 getMillis();

		 virtual bool cacheEnabled() { return true;}

public:
	     //static u32 VOLUMN_RATIO;
private: 
	     //void scanTableOneTime();

private:
		 double m_dataSizeFact;
	     Scale m_scale;
         vector<RowId> m_IdRowIdVec;

	     u64 m_totalMillis;		/** 测试操作所占用的运行时间，不包含loadData和tearDown的时间 */
	     uint m_threadCnt;		/** 并发线程数 */
         uint m_recCntPerThd;	/** 每线程的记录操作数 */

	     bool m_useMms;		/** 表是否使用Mms */
	     bool m_recInMms;	/** 所有记录是否在Mms中 */
	     bool m_isVar;		/** 是否为变长表 */
};



class TNTDeleter : public Thread {
public:
	TNTDeleter(vector<RowId> *ridVec, uint threadId, uint delCnt, TNTDatabase *db, TNTTable *tbl) : Thread("Deleter") {
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
	TNTDatabase *m_db;
	TNTTable *m_tbl;
protected:
	virtual void run();
};


}//namespace tntperf



#endif // _TNTTEST_DELETE_H_