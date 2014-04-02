/**
 * 验证数据库的持久性和一致性测试用例
 *
 * 苏斌(bsu@corp.netease.com, naturally@163.org)
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
	OP_INSERT = 0,	/** 插入操作*/
	OP_DELETE,		/** 删除操作*/
	OP_UPDATE		/** 更新操作*/
};

typedef struct opInfo {
	uint m_tableNo;		/** 操作对应的表序号 */
	u64 m_lsn;			/** 操作LSN */
	uint m_opId;		/** 操作序号 */
	uint m_opKey;		/** 操作对应的键值 */
	uint m_updateKey;	/** 更新操作使用的更新键值 */
	u8 m_opKind;		/** 操作类型 */
	bool m_succ;		/** 该操作日志是否成功 */
} OperationInfo;

#define OP_LOG_FILE	"op.log"				/** 操作日志保存到的文件 */

const uint OPERATION_TIMES = 100000;		/** 线程操作最大次数 */
const uint SLAVE_RUN_TIMES = 1000;			/** 模拟崩溃线程执行次数 */

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
	bool m_threadSignal;		/** 子线程信号，true表示子线程可以执行，false表示需要停止 */

public:
	static const uint MAX_ID = 100000;	/** 使用的最大ID号 */
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
	OperationInfo m_opLog[OPERATION_TIMES];	/** 各个线程用来保存操作序列的数组 */
	uint m_runTimes;						/** 本线程实际执行的DML操作个数 */
};

}

#endif