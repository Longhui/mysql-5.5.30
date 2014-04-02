#ifndef _PREFTEST_TNT_INSERT_H_
#define _PREFTEST_TNT_INSERT_H_

#include "PerfTest.h"
#include "api/TNTDatabase.h"
#include "api/TNTTable.h"
#include "Generator.h"

using namespace tnt;
using namespace ntseperf;

namespace tntperf {
class TNTInsertPerfTest: public TestCase {
	public:
		TNTInsertPerfTest(const char * tableName, Scale scale, uint threadCount, Order idOrder, double dataSizeFact);
		~TNTInsertPerfTest(void);
		virtual string getName() const;
		virtual string getDescription() const;

		virtual void loadData(u64 *totalRecSize, u64 *recCnt);
		virtual void warmUp();
		virtual u64 getOpCnt() = 0;
		virtual u64 getDataSize() = 0;
		virtual u64 getMillis() = 0;
		virtual void run();
		virtual void setUp();
		virtual void tearDown();

	protected:
		TNTDatabase *m_db;
		TNTTable    *m_table;
		const char  *m_tableName;/** ���� */
		TestTable   m_testTable;	/** �����ñ� */
		Scale       m_scale;		/** ���Թ�ģ */
		TNTConfig	*m_cfg;			/** ���ݿ����� */
		u64			m_dataSize;		/** ������	 */
		u64			m_recCnt;		/** ��¼��	 */
		u64			m_opCnt;		/** ���������в����ļ�¼�� */
		u64			m_totalMillis;	/** ���Բ�����ռ�õ�����ʱ�䣬������loadData��tearDown��ʱ�� */
		uint		m_threadCnt;	/** �����߳��� */
		Order		m_idOrder;		/** �̶߳�id��ȡ��˳�� */
		uint		m_recCntPerThd;	/** ÿ�̵߳ļ�¼������ */
		double		m_dataSizeFact;	/** ��������С��buffer size��С�ı�ֵ */
		double      m_tntRatio;

		/* ģ�巽�� */
		virtual TNTConfig *getTNTConfig();
		virtual void createTableAndRecCnt(Session *session);
		virtual uint getRecordSize();
};

class TNTFLRInsertTest: public TNTInsertPerfTest {
public:
	TNTFLRInsertTest(const char * tableName, Scale scale, uint threadCount, Order idOrder, double dataSizeFact):
	  TNTInsertPerfTest(tableName, scale, threadCount, idOrder, dataSizeFact) {
	}
public:
	virtual u64 getOpCnt();
	virtual u64 getDataSize();
	virtual u64 getMillis();
};

class TNTVLRInsertTest: public TNTFLRInsertTest {
public:
	TNTVLRInsertTest(const char * tableName, Scale scale, uint threadCount, Order idOrder, double dataSizeFact):
	  TNTFLRInsertTest(tableName, scale, threadCount, idOrder, dataSizeFact) {};
};
}
#endif
