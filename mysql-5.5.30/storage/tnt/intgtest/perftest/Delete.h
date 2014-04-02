/**
 * DELETE���ɲ���
 *
 * @author л��(xieke@corp.netease.com, ken@163.org)
 */
#ifndef _PREFTEST_DELETE_H_
#define _PREFTEST_DELETE_H_


#include "PerfTest.h"
#include "Generator.h"
#include "heap/Heap.h"
#include "Insert.h"

using namespace ntse;


namespace ntseperf {

/**
 * Delete���ɲ�������
 */
class DeletePerfTest: public TestCase {
public:
	DeletePerfTest(Scale scale, double dataSizeFact) {
		m_dataSizeFact = dataSizeFact;
		m_scale = scale;
		m_threadCnt = 500;
	}
protected:
	double m_dataSizeFact;
	Scale m_scale;
	vector<RowId> m_IdRowIdVec;
	Config *m_cfg;			/** ���ݿ����� */
	Database *m_db;			/** ���ݿ� */
	Table *m_currTab;		/** ��ǰ�� */
	u64 m_dataSize;			/** ������	 */
	u64	m_recCnt;			/** ��¼��	 */
	u64 m_opCnt;			/** ���������в����ļ�¼�� */
	u64 m_totalMillis;		/** ���Բ�����ռ�õ�����ʱ�䣬������loadData��tearDown��ʱ�� */
	uint m_threadCnt;		/** �����߳��� */
	Order m_idOrder;		/** �̶߳�id��ȡ��˳�� */
	uint m_recCntPerThd;	/** ÿ�̵߳ļ�¼������ */

public:
	virtual string getName() const;
	virtual string getDescription() const;
	
	virtual void loadData(u64 *totalRecSize, u64 *recCnt);
	virtual void warmUp();
	virtual u64 getOpCnt();
	virtual u64 getDataSize();
	virtual u64 getMillis();
	virtual void run();
	virtual void setUp();
	virtual void tearDown();
	virtual bool cacheEnabled() { return true;}
};

class Deleter : public Thread {
public:
	Deleter(vector<RowId> *ridVec, uint threadId, uint delCnt, Database *db, Table *tbl) : Thread("Deleter") {
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
	Database *m_db;
	Table *m_tbl;
protected:
	virtual void run();
};



}

#endif  // #ifndef _PREFTEST_DELETE_H_