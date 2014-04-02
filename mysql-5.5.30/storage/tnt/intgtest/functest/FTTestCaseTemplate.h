/**
 * 功能测试框架基本实现类
 *
 * @author 苏斌(bsu@corp.netease.com, naturally@163.org)
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
	u64 m_recCntLoaded;			/** 已装载的总记录数 */
	Table *m_table;				/** 使用的表 */
	TableDef *m_tableDef;		/** 对应表定义 */
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
	 * 设置已加载数据信息
	 *	从测试数据缓存加载数据后，测试框架调用本函数设置已加载数据信息
	 * @param totalRecSize 总数据量
	 */
	virtual void setLoadedDataInfo(u64 totalRecSize) {
		m_totalRecSizeLoaded = totalRecSize;
	}

	/** 返回用例操作的总数据量 */
	virtual u64 getDataSize() {
		return m_opDataSize;
	}
	/** 返回用例的操作次数 */
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
	Database *m_db;				/** 测试数据库 */
	Config *m_config;			/** 数据库配置 */
	FTTestTableInfo **m_tableInfo;		/** 测试用例使用的各个表的信息 */
	u16	m_tables;				/** 测试用例使用的表数目 */

	u64 m_totalRecSizeLoaded;	/** 装载的数据量大小 */
	u64 m_opCnt;				/** 操作次数 */
	u64 m_opDataSize;			/** 操作数据量 */

	size_t m_threadNum;					/** 测试需要线程数 */
	TestOperationThread **m_threads;	/** 测试使用的线程 */
	
	static const uint MAX_DATA_ID = 100000;	/** 测试当中使用数据ID的最大值 */
};

#endif