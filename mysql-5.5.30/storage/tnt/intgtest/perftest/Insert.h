/**
 * INSERT���ɲ���
 *
 * @author л��(xieke@corp.netease.com, ken@163.org)
 */
#ifndef _PREFTEST_INSERT_H_
#define _PREFTEST_INSERT_H_


#include "PerfTest.h"
#include "Generator.h"
#include "heap/Heap.h"

using namespace ntse;


namespace ntseperf {

/**
 * insert���ɲ�������
 */
class InsertPerfTest: public TestCase {
public:
	InsertPerfTest(const char * tableName, HeapVersion ver, Scale scale, uint threadCount, Order idOrder, double dataSizeFact);

	virtual string getName() const;
	virtual string getDescription() const;
	
	virtual void loadData(u64 *totalRecSize, u64 *recCnt);
	virtual void warmUp();
	virtual u64 getOpCnt() = 0;
	virtual u64 getDataSize() = 0;
	virtual u64 getMillis() = 0;
	virtual void run();
	virtual void tearDown();


protected:
	const char *m_tableName;/** ���� */
	TestTable m_testTable;	/** �����ñ� */
	Database *m_db;			/** ���ݿ� */
	HeapVersion m_heapVer;	/** ��汾 */
	Scale m_scale;			/** ���Թ�ģ */
	Table *m_currTab;		/** ��ǰ�� */
	Config *m_cfg;			/** ���ݿ����� */
	u64 m_dataSize;			/** ������	 */
	u64	m_recCnt;			/** ��¼��	 */
	u64 m_opCnt;			/** ���������в����ļ�¼�� */
	u64 m_totalMillis;		/** ���Բ�����ռ�õ�����ʱ�䣬������loadData��tearDown��ʱ�� */
	uint m_threadCnt;		/** �����߳��� */
	Order m_idOrder;		/** �̶߳�id��ȡ��˳�� */
	uint m_recCntPerThd;	/** ÿ�̵߳ļ�¼������ */
	double m_dataSizeFact;	/** ��������С��buffer size��С�ı�ֵ */

	/* ģ�巽�� */
	virtual Config *getConfig();
	virtual void createTableAndRecCnt(Session *session);
	virtual uint getRecordSize();
};


class FLRInsertTest: public InsertPerfTest {
public:
	FLRInsertTest(const char * tableName, Scale scale, uint threadCount, Order idOrder, double dataSizeFact):InsertPerfTest(tableName, HEAP_VERSION_FLR, scale, threadCount, idOrder, dataSizeFact) {
	}
public:
	virtual u64 getOpCnt();
	virtual u64 getDataSize();
	virtual u64 getMillis();
};


class VLRInsertTest: public FLRInsertTest {
public:
	VLRInsertTest(const char * tableName, Scale scale, uint threadCount, Order idOrder, double dataSizeFact):FLRInsertTest(tableName, scale, threadCount, idOrder, dataSizeFact) {};
};



}




#endif  // #ifndef _PREFTEST_INSERT_H_
