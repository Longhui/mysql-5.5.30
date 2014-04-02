/**
 * 集成测试5.2.1测试数据库稳定性用例
 *
 * 苏斌(...)
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
	Database *m_db;		/** 操作的数据库 */
	Table *m_table;		/** 操作的表 */
	uint m_period;		/** 帮助线程循环周期0xffffffffffffffff表示不需要停顿，否则表示停顿等待时间 
							同时该周期指的是净等待时间而不是操作加等待时间的总和*/
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
	StableHelperThread *m_tableChecker;			/** 检查表正确性的线程 */
	StableHelperThread *m_lobDefrager;			/** 大对象碎片整理线程 */
	StableHelperThread *m_backuper;				/** 备份操作线程 */
	StableHelperThread *m_envStatusRecorder;	/** 记录CPU、内存、io等统计信息，Linux环境有效 */
	StableHelperThread **m_indexChecker;		/** 检查索引正确性的线程 */

	uint m_defragPeriod;			/** 大对象碎片整理周期 */
	uint m_backupPeriod;			/** 备份线程周期 */
	uint m_statusPeriod;			/** 状态统计线程周期 */

	Atomic<int> m_totalUpdateOp;	/** 测试用例总共更新次数 */
	u64 m_startTime;						/** 测试用力起始时间 */

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
	uint m_indexNo;	/** 该线程需要扫描的索引编号 */
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
	Stability *m_testcase;	/** 主测试用例 */
	u64 m_startTime;		/** 主测试用例起始时间 */
};

}

#endif