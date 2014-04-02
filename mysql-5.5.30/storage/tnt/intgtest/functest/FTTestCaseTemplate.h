/**
 * ���ܲ��Կ�ܻ���ʵ����
 *
 * @author �ձ�(bsu@corp.netease.com, naturally@163.org)
 */

#ifndef _NTSETEST_FTTESTCASETEMPLATE_H_
#define _NTSETEST_FTTESTCASETEMPLATE_H_

#include "FuncTest.h"
#include "api/Table.h"
#include "misc/TableDef.h"
#include "IntgTestHelper.h"
#include "util/Sync.h"

using namespace std;
using namespace ntsefunc;

typedef struct TestTableInfo {
	u64 m_recCntLoaded;			/** ��װ�ص��ܼ�¼�� */
	Table *m_table;				/** ʹ�õı� */
	TableDef *m_tableDef;		/** ��Ӧ���� */
} FTTestTableInfo;

class FTTestCaseTemplate : public TestCase {
public:
	FTTestCaseTemplate(size_t threadNum);
	virtual ~FTTestCaseTemplate();

	virtual string getName() const {
		return "FTTestCaseTemplate";
	}

	virtual string getDescription() const {
		return "FTTestCaseTemplate's Description";
	}

	virtual void run();

	virtual void status() {
		for (uint i = 0; i < m_tables; i++) {
			TableDMLHelper::printStatus(m_tableInfo[i]->m_table);
		}
	}

	/**
	 * �����Ѽ���������Ϣ
	 *	�Ӳ������ݻ���������ݺ󣬲��Կ�ܵ��ñ����������Ѽ���������Ϣ
	 * @param totalRecSize ��������
	 */
	virtual void setLoadedDataInfo(u64 totalRecSize) {
		m_totalRecSizeLoaded = totalRecSize;
	}

	/** ���������������������� */
	virtual u64 getDataSize() {
		return m_opDataSize;
	}
	/** ���������Ĳ������� */
	virtual u64 getOpCnt() {
		return m_opCnt;
	}

	virtual uint chooseTableNo(uint factor) {
		return (factor % 2 == 0 || m_tables == 1) ? 0 : 1;
	}


protected:
	void openTable(bool create = false, int recover = 0);
	void closeDatabase(bool normalClose = true);
	void setTableDef(TableDef **descTableDef, const TableDef *srcTableDef);
	void setConfig(const Config *cfg);

protected:
	Database *m_db;				/** �������ݿ� */
	Config *m_config;			/** ���ݿ����� */
	FTTestTableInfo **m_tableInfo;		/** ��������ʹ�õĸ��������Ϣ */
	u16	m_tables;				/** ��������ʹ�õı���Ŀ */

	u64 m_totalRecSizeLoaded;	/** װ�ص���������С */
	u64 m_opCnt;				/** �������� */
	u64 m_opDataSize;			/** ���������� */

	size_t m_threadNum;					/** ������Ҫ�߳��� */
	TestOperationThread **m_threads;	/** ����ʹ�õ��߳� */
	
	static const uint MAX_DATA_ID = 100000;	/** ���Ե���ʹ������ID�����ֵ */
};

#endif