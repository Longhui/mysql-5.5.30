/**
 * ����ɨ�����ܲ��� (��ʵ��)
 *
 * @author �۷�(shaofeng@corp.netease.com, sf@163.org)
 */
#ifndef _NTSETEST_DISK_TABLESCAN_H_
#define _NTSETEST_DISK_TABLESCAN_H_


#include "PerfTest.h"
#include "EmptyTestCase.h"

using namespace ntse;


namespace ntseperf {

/** MMS�����ʱ��� */
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
	//u64 m_dataSize;		 /** ������	*/
	//u64	m_recCnt;		 /** ��¼��	*/
	//u64 m_opCnt;
	bool m_useMms;		/** ���Ƿ�ʹ��Mms */
	bool m_recInMms;	/** ���м�¼�Ƿ���Mms�� */
	bool m_isVar;		/** �Ƿ�Ϊ�䳤�� */
};

}

#endif // _NTSETEST_DISK_TABLESCAN_H_
