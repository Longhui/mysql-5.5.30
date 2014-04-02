/**
 * �ڴ��ɨ�����ܲ��� (��ʵ��)
 *
 * @author ������(yulihua@corp.netease.com, ylh@163.org)
 */
#ifndef _NTSETEST_MEMORY_TABLESCAN_H_
#define _NTSETEST_MEMORY_TABLESCAN_H_


#include "PerfTest.h"
#include "EmptyTestCase.h"

using namespace ntse;


namespace ntseperf {

#define MEM_TABLE_SCAN_REDUCE_RATIO 2
#define MEM_TABLE_SCAN_MMS_MULTIPLE 10

class MemoryTableScanTest: public EmptyTestCase {
public:
	MemoryTableScanTest(bool useMms, bool recInMms, bool isVar);

	string getName() const;
	string getDescription() const;
	virtual string getCachedName() const;
	
	virtual void loadData(u64 *totalRecSize, u64 *recCnt);
	virtual void warmUp();
	virtual void run();
	void setUp();

private: 
	void scanTableOneTime();

private:
	bool m_useMms;		/** ���Ƿ�ʹ��Mms */
	bool m_recInMms;	/** ���м�¼�Ƿ���Mms�� */
	bool m_isVar;		/** �Ƿ�Ϊ�䳤�� */
};

}

#endif // _NTSETEST_MEMORY_TABLESCAN_H_
