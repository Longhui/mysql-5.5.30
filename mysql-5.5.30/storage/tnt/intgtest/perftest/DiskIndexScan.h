/**
 * ��������ɨ�����ܲ���
 *
 * @author �۷�(shaofeng@corp.netease.com, sf@163.org)
 */
#ifndef _NTSETEST_DISK_INDEXSCAN_H_
#define _NTSETEST_DISK_INDEXSCAN_H_


#include "PerfTest.h"
#include "EmptyTestCase.h"

using namespace ntse;


namespace ntseperf {

/** ������ */
#define DISK_INDEX_SCAN_MMS_HIT_RATIO 0.1f

class DiskIndexScanTest: public EmptyTestCase {
public:
	DiskIndexScanTest(bool isVar);

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
	u64 scanIndexOneTime(float mmsRatio = 1);

private:
	bool m_isVar;		/** �Ƿ�Ϊ�䳤�� */
};

}

#endif // _NTSETEST_DISK_INDEXSCAN_H_
