/**
 * ���ɲ���5.1.1���Լ�¼����ԭ����
 *
 * �ձ�(...)
 */

#ifndef _NTSETEST_ATOMICONE_H_
#define _NTSETEST_ATOMICONE_H_

#include "misc/Global.h"
#include "DbConfigs.h"
#include "api/Database.h"
#include "FTTestCaseTemplate.h"
#include "BlogTable.h"
#include "CountTable.h"

using namespace ntse;

namespace ntsefunc {

class AtomicOne : public FTTestCaseTemplate {
public:
	AtomicOne(uint recCount, size_t threadNum) : FTTestCaseTemplate(threadNum) {
		setConfig(CommonDbConfig::getSmall());
		m_tables = 2;
		m_tableInfo = new FTTestTableInfo*[m_tables];
		m_tableInfo[0] = new FTTestTableInfo();
		m_tableInfo[1] = new FTTestTableInfo();
		setTableDef(&(m_tableInfo[0]->m_tableDef), BlogTable::getTableDef(true));
		setTableDef(&(m_tableInfo[1]->m_tableDef), CountTable::getTableDef(true));
		m_recCount = recCount;
	}
	~AtomicOne() {}

	string getName() const;
	string getDescription() const;
	void run();

	virtual void loadData(u64 *totalRecSize, u64 *recCnt);
	virtual void warmUp();
	virtual bool verify();
	virtual void mtOperation(void *param);

private:
	uint m_recCount;	/** ��������ʹ�ü�¼�� */

public:
	static const uint TASK_OPERATION_NUM = 100;	/** ������µĴ��� */
};



}

#endif