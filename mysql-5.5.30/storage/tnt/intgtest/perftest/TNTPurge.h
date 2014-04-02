#ifndef _PREFTEST_TNT_PURGE_H_
#define _PREFTEST_TNT_PURGE_H_

#include "PerfTest.h"
#include "api/TNTDatabase.h"
#include "api/TNTTable.h"
#include "TNTEmptyTestCase.h"
#include "Generator.h"

using namespace tnt;
using namespace ntseperf;

namespace tntperf {
class TNTPurgePerfTest: public TNTEmptyTestCase {
public:
	TNTPurgePerfTest(const char * tableName, Scale scale, bool useMms);
	~TNTPurgePerfTest(void);
	virtual string getName() const;
	virtual string getDescription() const;
	virtual void setUp();
	virtual void tearDown();

	virtual void loadData(u64 *totalRecSize, u64 *recCnt);
	virtual void warmUp();
	virtual u64 getOpCnt();
	virtual u64 getDataSize();
	virtual u64 getMillis();
	virtual void run();

protected:
	//TNTDatabase *m_db;
	//TNTTable    *m_table;
	const char  *m_tableName;/** ���� */
	TestTable   m_testTable;	/** �����ñ� */
	Scale       m_scale;		/** ���Թ�ģ */
	double      m_ratio;        /** buffer���ڼ�¼��ʹ�ñ��� */
	u64			m_dataSize;		/** ������	 */
	u64			m_recCnt;		/** ��¼��	 */
	//u64			m_opCnt;		/** ���������в����ļ�¼�� */
	u64			m_totalMillis;	/** ���Բ�����ռ�õ�����ʱ�䣬������loadData��tearDown��ʱ�� */
	bool        m_useMms;
	static const uint  BATCH_OP_SIZE = 1000;

	/* ģ�巽�� */
	//virtual TNTConfig *getTNTConfig();
	//virtual void createTableAndRecCnt(Session *session);
	//virtual uint getRecordSize();
};

}
#endif
