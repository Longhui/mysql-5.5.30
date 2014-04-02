/**
 * 空测试用例, 实现测试用例的一些常用功能
 *
 * @author 赵继远(hzzhaojy@corp.netease.com)
 */
#ifndef _TNTTEST_EMPTY_TESTCASE_H_
#define _TNTTEST_EMPTY_TESTCASE_H_

#include "PerfTest.h"
#include "api/TNTTable.h"
#include "api/TNTDatabase.h"
#include "misc/TableDef.h"

//using namespace tntperf;
using namespace tnt;
using namespace ntseperf;

/** 
 * 空测试用例，简化测试用例的构造
 */

namespace tntperf{

class TNTEmptyTestCase: public TestCase {
public:
	TNTEmptyTestCase();

	~TNTEmptyTestCase();

	virtual string getName() const {
		return "TNTEmptyTestCase";
	}

	virtual string getDescription() const {
		return "TNTEmptyTestCase's Description";
	}

	virtual void run();
	
	virtual void tearDown() {
		closeDatabase();
	}

	virtual TNTDatabase* getTNTDatabase() {
		return m_db;
	}

	/**
	 * 设置已加载数据信息
	 *	从测试数据缓存加载数据后，测试框架调用本函数设置已加载数据信息
	 * @param totalRecSize 总数据量
	 * @param recCnt 记录数
	 */
	virtual void setLoadedDataInfo(u64 totalRecSize, u64 recCnt) {
		m_recCntLoaded = recCnt;
		m_totalRecSizeLoaded = totalRecSize;
	}

	virtual bool cacheEnabled() {
		return m_enableCache;
	}

	/** 返回用例操作的总数据量 */
	virtual u64 getDataSize() {
		return m_opDataSize;
	}
	/** 返回用例的操作次数 */
	virtual u64 getOpCnt() {
		return m_opCnt;
	}

	/** 是否内存测试用例(内存用例不能有IO) */
	virtual bool isMemoryCase() {
		return m_isMemoryCase;
	}

	/**
	 * 获取统计信息
	 * @return 统计信息，返回0表示没有统计信息
	 */
	virtual const TestCaseStatus* getStatus() const;


protected:
	void openTable(bool create = false);
	void closeDatabase();
	void disableIo();

	void setTableDef(const TableDef *tableDef);
	void setConfig(TNTConfig *cfg);

protected:
	bool m_enableCache;			/** 是否开启测试数据缓存功能 */
	bool m_isMemoryCase;		/** 是否内存测试用例 */

	u64 m_totalRecSizeLoaded;	/** 已装载总数据量 */
	u64 m_recCntLoaded;			/** 已装载的总记录数 */

	u64 m_opCnt;				/** 操作次数 */
	u64 m_opDataSize;			/** 操作数据量 */

	TestCaseStatus *m_status;	/** 用例统计信息 */

	TNTDatabase *m_db;				/** 测试数据库 */
	TNTTable *m_table;				/** 测试表 */
	TableDef *m_tableDef;		/** 表定义 */
	TNTConfig *m_config;			/** 数据库配置 */
};

}

#endif // _TNTTEST_EMPTY_TESTCASE_H_
