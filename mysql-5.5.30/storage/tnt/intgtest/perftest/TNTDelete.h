/**
* TNTȫ��ɨ�����ܲ���
* 
* @author �Լ�Զ(hzzhaojy@corp.netease.com)
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

	     u64 m_totalMillis;		/** ���Բ�����ռ�õ�����ʱ�䣬������loadData��tearDown��ʱ�� */
	     uint m_threadCnt;		/** �����߳��� */
         uint m_recCntPerThd;	/** ÿ�̵߳ļ�¼������ */

	     bool m_useMms;		/** ���Ƿ�ʹ��Mms */
	     bool m_recInMms;	/** ���м�¼�Ƿ���Mms�� */
	     bool m_isVar;		/** �Ƿ�Ϊ�䳤�� */
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