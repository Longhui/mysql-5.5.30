/**
 * 索引创建性能测试实现
 *
 * 测试目的：索引创建性能和数据量之间的函数关系
 * 表模式：Count
 * 测试配置：NTSE配置：NC_MEDIUM data_size = 16M, 32M, 64M, 128M, 256M, 512M
 * 测试数据：随机新数据
 * 测试流程：
 *		1.	装载数据
 *		2.	为Count列创建索引
 *		3.	记录索引大小，并根据索引大小和记录数计算索引创建性能
 *
 * @author 苏斌(bsu@corp.netease.com naturally@163.org)
 */

#include "IndexCreation.h"
#include "DbConfigs.h"
#include "api/Table.h"
#include "api/Database.h"
#include "misc/Session.h"
#include "misc/TableDef.h"
#include "CountTable.h"
#include "btree/Index.h"
#include <sstream>

using namespace ntse;
using namespace ntseperf;
using namespace std;

#define INDEX_NAME	"index.nti"

/**
 * 构造函数
 * 根据dataSize指定索引初始数据大小
 */
IndexCreationTest::IndexCreationTest(u64 dataSize) {
	m_dataSize = dataSize;
	m_totalRecSizeLoaded = dataSize;
	m_opCnt = 0;
	m_indice = NULL;
	setConfig(CommonDbConfig::getMedium());
	setTableDef(CountTable::getTableDef(true));
}


/**
 * 取得用例名
 */
string IndexCreationTest::getName() const {
	stringstream ss;
	ss << "IndexCreation(DataSize:";
	ss << m_dataSize / 1024 / 1024;
	ss << "MB)";
	return ss.str();
}


/**
 * 取得用例描述
 */
string IndexCreationTest::getDescription() const {
	return "Test index creation of a specified data size heap";
}


/**
 * 装载数据，统一使用Count表
 */
void IndexCreationTest::loadData(u64 *totalRecSize, u64 *recCnt) {
	openTable(true);
	// 创建Session
	Connection *conn = m_db->getConnection(false);
	Session *session = m_db->getSessionManager()->allocSession("IndexCreationTest::loadData", conn);

	assert(m_totalRecSizeLoaded > 0);
	m_recCntLoaded = CountTable::populate(session, m_table, &m_totalRecSizeLoaded);
	*totalRecSize = m_totalRecSizeLoaded;
	*recCnt = m_recCntLoaded;

	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(conn);

	closeDatabase();
}


void IndexCreationTest::warmUp() {
	openTable(false);
	return;
}


/**
 * 执行测试主题创建索引操作
 */
void IndexCreationTest::run() {
	m_opCnt = m_recCntLoaded;
	m_opDataSize = m_totalRecSizeLoaded;

	Connection *conn = m_db->getConnection(false);
	Session *session = m_db->getSessionManager()->allocSession("IndexCreationTest::run", conn);

	IndexDef *indexDef = newCountColIndexDefOfCountTable();
	try {
		//m_table->addIndex(session, false, 1, (const IndexDef **)&indexDef);
		m_table->addIndex(session, 1, (const IndexDef **)&indexDef);
	} catch (NtseException &e) {
		cout << e.getMessage() << endl;
		return;
	}

	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(conn);

	delete indexDef;
}


/**
 * 创建并返回Count表COUNT列的索引定义
 */
IndexDef* IndexCreationTest::newCountColIndexDefOfCountTable() {
	ColumnDef *colDef = m_table->getTableDef()->m_columns[1];
	IndexDef *indexDef = new IndexDef("COUNT", 1, &colDef);
	indexDef->m_maxKeySize = 1 + 4;	//	位图位1＋数据位4

	return indexDef;
}

