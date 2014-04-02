/**
 * ���ɲ���5.2.1�������ݿ��ȶ�������
 *
 * �ձ�(...)
 */

#ifndef _NTSETEST_STABILITY_H_
#define _NTSETEST_STABILITY_H_

#include "DbConfigs.h"
#include "api/Database.h"
#include "FTTestCaseTemplate.h"
#include "BlogTable.h"
#include "CountTable.h"

using namespace ntse;

namespace ntsefunc {

const static char *STATUS_FILE = "status.inf";

class StableHelperThread : public Thread {
public:
	StableHelperThread(Database *db, Table *table, uint period) : Thread("StableHelperThread"), m_db(db), m_table(table), m_period(period) {}

protected:
	Database *m_db;		/** ���������ݿ� */
	Table *m_table;		/** �����ı� */
	uint m_period;		/** �����߳�ѭ������0xffffffffffffffff��ʾ����Ҫͣ�٣������ʾͣ�ٵȴ�ʱ�� 
							ͬʱ������ָ���Ǿ��ȴ�ʱ������ǲ����ӵȴ�ʱ����ܺ�*/
};

class Stability : public FTTestCaseTemplate {
public:
	Stability(uint dataSize, uint threadNum, uint defragPeriod, uint backupPeriod, uint statusPeriod) : FTTestCaseTemplate(threadNum) {
		setConfig(CommonDbConfig::getSmall());
		m_tables = 1;
		m_tableInfo = new FTTestTableInfo*[m_tables];
		m_tableInfo[0] = new FTTestTableInfo();
		setTableDef(&(m_tableInfo[0]->m_tableDef), BlogTable::getTableDef(true));
		m_totalRecSizeLoaded = dataSize;
		m_defragPeriod = defragPeriod;
		m_backupPeriod = backupPeriod;
		m_statusPeriod = statusPeriod;
		m_tableChecker = m_backuper = m_envStatusRecorder = m_lobDefrager = NULL;
		m_indexChecker = NULL;
	}

	~Stability() {
		if (m_tableChecker)
			delete m_tableChecker;
		if (m_indexChecker) {
			for (uint i = 0; i < m_tableInfo[0]->m_table->getTableDef()->m_numIndice; i++)
				if (m_indexChecker[i])
					delete m_indexChecker[i];
			delete [] m_indexChecker;
		}
		if (m_backuper)
			delete m_backuper;
		if (m_lobDefrager)
			delete m_lobDefrager;
		if (m_envStatusRecorder)
			delete m_envStatusRecorder;

		closeDatabase();
	}

	string getName() const;
	string getDescription() const;
	void run();

	virtual void loadData(u64 *totalRecSize, u64 *recCnt);
	virtual void warmUp();
	virtual bool verify();
	virtual void mtOperation(void *param);

	uint getTotalUpdateOp() { return m_totalUpdateOp.get(); }

public:
	static const uint MAX_ID = 100000;

private:
	StableHelperThread *m_tableChecker;			/** ������ȷ�Ե��߳� */
	StableHelperThread *m_lobDefrager;			/** �������Ƭ�����߳� */
	StableHelperThread *m_backuper;				/** ���ݲ����߳� */
	StableHelperThread *m_envStatusRecorder;	/** ��¼CPU���ڴ桢io��ͳ����Ϣ��Linux������Ч */
	StableHelperThread **m_indexChecker;		/** ���������ȷ�Ե��߳� */

	uint m_defragPeriod;			/** �������Ƭ�������� */
	uint m_backupPeriod;			/** �����߳����� */
	uint m_statusPeriod;			/** ״̬ͳ���߳����� */

	Atomic<int> m_totalUpdateOp;	/** ���������ܹ����´��� */
	u64 m_startTime;						/** ����������ʼʱ�� */

public:
	static bool m_isDmlRunning;
};


class TableChecker : public StableHelperThread {
public:
	TableChecker(Database *db, Table *table, uint period) : StableHelperThread(db, table, period) {}
	virtual void run();
};

class IndexChecker : public StableHelperThread {
public:
	IndexChecker(Database *db, Table *table, uint period, uint indexNo) : StableHelperThread(db, table, period) {
		m_indexNo = indexNo;
	}
	virtual void run();
private:
	uint m_indexNo;	/** ���߳���Ҫɨ���������� */
};

class LobDefrager : public StableHelperThread {
public:
	LobDefrager(Database *db, Table *table, uint period) : StableHelperThread(db, table, period) {}
	virtual void run();
};

class Backuper : public StableHelperThread {
public:
	Backuper(Database *db, Table *table, uint period) : StableHelperThread(db, table, period) {}
	virtual void run();
};

class EnvStatusRecorder : public StableHelperThread {
public:
	EnvStatusRecorder(Database *db, Table *table, uint period, Stability *testcase, u64 startTime) : StableHelperThread(db, table, period), m_startTime(startTime), m_testcase(testcase) {}
	virtual void run();

#ifndef WIN32
private:
	bool executeShellCMD(const char *command);
#endif

private:
	Stability *m_testcase;	/** ���������� */
	u64 m_startTime;		/** ������������ʼʱ�� */
};

}

#endif