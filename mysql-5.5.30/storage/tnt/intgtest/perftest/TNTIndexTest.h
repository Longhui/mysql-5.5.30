/**
* TNTȫ��ɨ�����ܲ���
* 
* @author hw(hzhuwei@corp.netease.com)
*/
#ifndef _TNTTEST_INDEX_H_
#define _TNTTEST_INDEX_H_

#include "PerfTest.h"
#include "TNTEmptyTestCase.h"
#include "Generator.h"

using namespace ntse;
using namespace tnt;

namespace tntperf{

#define TNT_INDEX_SCAN_REDUCE_RATIO 2
#define TNT_INDEX_SCAN_MMS_MULTIPLE 10

enum TNT_INDEX_TESTCASE {
	PURGE_TABLE,
	BEGIN_END_SCAN,
	UNIQUE_SCAN,
	RANGE_SCAN,
	TABLE_SCAN
};

class TNTIndexTest: public TNTEmptyTestCase
{
public:
	TNTIndexTest(const char * tableName, Scale scale, bool useMms, bool recInMms, bool isVar, enum TNT_INDEX_TESTCASE testcase);

	string getName() const;
	string getDescription() const;
	virtual string getCachedName() const;	
	virtual void loadData(u64 *totalRecSize, u64 *recCnt);
	virtual void warmUp();
	virtual void run();
	virtual u64 getMillis();
	void setUp();
	void tearDown();

public:
	//static u32 VOLUMN_RATIO;
private: 
	void scanTableOneTime();
	void insertRecord(TNTDatabase *db, TNTTable *table,  u64 count);
	void updateTable(TNTDatabase *db, TNTTable *table);
	char* randomStr(size_t size);

	//�����Ǹ����Է���
	void purgeTable(TNTDatabase *db, TNTTable *table);
	void beginAndEndIdxScan(TNTDatabase *db, TNTTable *table);
	void uniqueIdxScan(TNTDatabase *db, TNTTable *table);
	void rangeIdxScan(TNTDatabase *db, TNTTable *table);
	void tableRangeScan(TNTDatabase *db, TNTTable *table);
protected:
	bool m_useMms;		/** ���Ƿ�ʹ��Mms */
	bool m_recInMms;	/** ���м�¼�Ƿ���Mms�� */
	bool m_isVar;		/** �Ƿ�Ϊ�䳤�� */

	bool m_backTable;   /** ��ѯ�Ƿ�ر�*/
	bool m_ntseTest;    /** �Ƿ�ֱ�ӵ���NTSE�ӿڲ��� */

	TNTDatabase *m_db;
	TNTTable    *m_table;
	const char  *m_tableName;/** ���� */
	TestTable   m_testTable;	/** �����ñ� */
	Scale       m_scale;		/** ���Թ�ģ */
	double      m_ratio;        /** buffer���ڼ�¼��ʹ�ñ��� */
	u64			m_dataSize;		/** ������	 */
	u64			m_recCnt;		/** ��¼��	 */
	//u64			m_opCnt;		/** ���������в����ļ�¼�� */
	u64			m_totalMillis;	/** ���Բ�����ռ�õ�����ʱ�䣬������loadData��tearDown��ʱ�� */
	static uint  BATCH_OP_SIZE;

	enum TNT_INDEX_TESTCASE m_testcase;
};

}

#endif // _TNTTEST_INDEXSCAN_H_