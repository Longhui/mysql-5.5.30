/**
 * �ڴ�����ɨ�����ܲ��� ������� )
 *
 * @author �۷�(shaofeng@corp.netease.com, sf@163.org)
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
	bool m_useMms;		/** ���Ƿ�ʹ��Mms */
	bool m_recInMms;	/** ���м�¼�Ƿ���Mms�� */
	bool m_isVar;		/** �Ƿ�Ϊ�䳤�� */
};

}

#endif // _NTSETEST_MEMORY_INDEXSCAN_H_
