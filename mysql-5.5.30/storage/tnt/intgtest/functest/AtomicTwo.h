/**
 * ���ɲ���5.1.2���Լ�¼����ԭ����
 *
 * �ձ�(...)
 */

#ifndef _NTSETEST_ATOMICTWO_H_
#define _NTSETEST_ATOMICTWO_H_

#include "misc/Global.h"
#include "DbConfigs.h"
#include "api/Database.h"
#include "FTTestCaseTemplate.h"
#include "BlogTable.h"
#include "CountTable.h"

using namespace ntse;

namespace ntsefunc {

class AtomicTwo : public FTTestCaseTemplate {
public:
	AtomicTwo(uint recCount, size_t threadNum) : FTTestCaseTemplate(threadNum) {
		setConfig(CommonDbConfig::getSmall());
		m_tables = 2;
		m_tableInfo = new FTTestTableInfo*[m_tables];
		m_tableInfo[0] = new FTTestTableInfo();
		m_tableInfo[1] = new FTTestTableInfo();
		setTableDef(&(m_tableInfo[0]->m_tableDef), BlogTable::getTableDef(false));
		setTableDef(&(m_tableInfo[1]->m_tableDef), CountTable::getTableDef(false));
		m_tblLock = new RWLock *[m_tables];
		m_tblLock[0] = new RWLock("BlogTable lock", __FILE__, __LINE__);
		m_tblLock[1] = new RWLock("CountTable lock", __FILE__, __LINE__);
		m_recCount = recCount;
		m_mirror = NULL;
	}


	~AtomicTwo() {
		// ɾ���ڴ澵������
		if (m_mirror != NULL) {
			for (uint i = 0; i < m_tables; i++) {
				for (uint j = 0; j < MAX_ID; j++) {
					delete [] m_mirror[i][j];
				}
				delete [] m_mirror[i];
			}
			delete [] m_mirror;
			m_mirror = NULL;
		}
		for (uint i = 0; i < m_tables; ++i) {
			delete m_tblLock[i];
		}
		delete [] m_tblLock;
	}

	string getName() const;
	string getDescription() const;
	void run();

	virtual void loadData(u64 *totalRecSize, u64 *recCnt);
	virtual void warmUp();
	virtual bool verify() {
		bool success = verifyrange((uint)-1, (uint)-1);
		closeDatabase();
		return success;
	}
	virtual void mtOperation(void *param);

private:
	void initMirror();
	bool verifyrange(uint minTable, uint maxTable);

private:
	uint m_recCount;	/** ��������ʹ�ü�¼�� */
	byte ***m_mirror;	/** ���������������ڴ��еľ��� */
	RWLock **m_tblLock;	/** �����ñ��� */

public:
	static const uint TASK_OPERATION_NUM = 10000;	/** �����߳�ִ�д��� */
	static const uint MAX_ID = 1000;				/** ���ֵ����С����С�ļ�¼RowId�����ܲ���ͻ */
	static const uint RANDOM_UPDATE_COLUMNS = 3;	/** ������²������µ����� */
};

}

#endif