/**
 * ��֤���ݿ�ĳ־��Ժ�һ���Բ�������
 *
 * �ձ�(bsu@corp.netease.com, naturally@163.org)
 */

#ifndef _NTSETEST_CONSISTENCYANDDURABILITY_H_
#define _NTSETEST_CONSISTENCYANDDURABILITY_H_

#include "misc/Global.h"
#include "DbConfigs.h"
#include "api/Database.h"
#include "FTTestCaseTemplate.h"
#include "BlogTable.h"
#include "CountTable.h"

using namespace ntse;

namespace ntsefunc {

enum OpKind {
	OP_INSERT = 0,	/** �������*/
	OP_DELETE,		/** ɾ������*/
	OP_UPDATE		/** ���²���*/
};

typedef struct opInfo {
	uint m_tableNo;		/** ������Ӧ�ı���� */
	u64 m_lsn;			/** ����LSN */
	uint m_opId;		/** ������� */
	uint m_opKey;		/** ������Ӧ�ļ�ֵ */
	uint m_updateKey;	/** ���²���ʹ�õĸ��¼�ֵ */
	u8 m_opKind;		/** �������� */
	bool m_succ;		/** �ò�����־�Ƿ�ɹ� */
} OperationInfo;

#define OP_LOG_FILE	"op.log"				/** ������־���浽���ļ� */

const uint OPERATION_TIMES = 100000;		/** �̲߳��������� */
const uint SLAVE_RUN_TIMES = 1000;			/** ģ������߳�ִ�д��� */

class CDTest : public FTTestCaseTemplate {
public:
	CDTest(u64 dataSize, size_t threadNum) : FTTestCaseTemplate(threadNum) {
		setConfig(CommonDbConfig::getSmall());
		m_tables = 2;
		m_tableInfo = new FTTestTableInfo*[m_tables];
		m_tableInfo[0] = new FTTestTableInfo();
		m_tableInfo[1] = new FTTestTableInfo();
		setTableDef(&(m_tableInfo[0]->m_tableDef), BlogTable::getTableDef(useMms));
		setTableDef(&(m_tableInfo[1]->m_tableDef), CountTable::getTableDef(useMms));
		m_totalRecSizeLoaded = dataSize;
		m_threadSignal = true;
	}
	~CDTest() {}

	string getName() const;
	string getDescription() const;
	void run();

	virtual void loadData(u64 *totalRecSize, u64 *recCnt);
	virtual void warmUp();
	virtual bool verify();
	virtual void mtOperation(void *param);

	bool getSignal() { return m_threadSignal; }
	Database* getDatabase() { return m_db; }
	Table *getTable(uint tableNo) { return tableNo < m_tables ? m_tableInfo[tableNo]->m_table : NULL; }

private:
	void discardTailOpInfo(u64 tailLsn);
	void mergeMTSOpInfoAndSaveToFile();
	bool checkOpLogs(u64 tailLsn);
	u64 getRealTailLSN();
	void backupDBFiles();

private:
	bool m_threadSignal;		/** ���߳��źţ�true��ʾ���߳̿���ִ�У�false��ʾ��Ҫֹͣ */

public:
	static const uint MAX_ID = 100000;	/** ʹ�õ����ID�� */
	static const bool useMms = true;
};

class SlaveThread : public TestOperationThread {
public:
	SlaveThread(CDTest *testcase, u64 threadId) 
		: TestOperationThread(testcase, threadId) {
			memset(&m_opLog, 0, OPERATION_TIMES * sizeof(OperationInfo));
	}
	
	virtual void run();

	void saveLastOpInfo(bool succ, uint tableNo, uint opCount, u64 lsn, u8 opKind, uint opId, uint k, uint kplus = 0);

	OperationInfo* getOpInfo(uint *size) { 
		*size = m_runTimes; 
		return m_opLog; 
	}

private:
	OperationInfo m_opLog[OPERATION_TIMES];	/** �����߳���������������е����� */
	uint m_runTimes;						/** ���߳�ʵ��ִ�е�DML�������� */
};

}

#endif