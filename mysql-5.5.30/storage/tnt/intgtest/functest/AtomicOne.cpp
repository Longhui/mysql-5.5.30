/**
 * 集成测试5.1.1测试记录操作原子性
 *
 * 测试目的：验证NTSE记录操作的原子性（实时验证），每次更新操作之间，都对记录的原子性进行实时验证
 * 表模式：Blog和Count
 * 测试配置：NTSE配置：NC_SMALL thread_count = 500
 * 测试数据：操作唯一记录生成方法：给定一个操作ID:opid，生成的记录必须满足“除了ID列之外，所有列的内容都必须opid一一对应”
 * 测试流程：
 *	1.	生成100条记录
 *	2.	为两张表分别运行thread_count/2个更新任务线程，每个线程执行10000次任务，每个任务包括如下操作：
 *		a)获取当前这次操作的操作ID: opid
 *		b)产生一个0和100之间的随机数id，找到满足ID>=id的第一条记录
 *		c)验证当期记录R的所有列（除了ID之外）都对应同一个操作ID:opid
 *		d)根据opid生成新的记录内容，更新这条记录
 *	3.	待更新任务运行完成之后，进行一次表扫描，对于当前记录为R，验证R的所有列（除了ID之外）都对应同一个操作ID:opid
 *
 * 苏斌(...)
 */

#include "AtomicOne.h"
#include "DbConfigs.h"
#include "Random.h"
#include "IntgTestHelper.h"

using namespace std;
using namespace ntsefunc;

/** 得到用例名字 */
string AtomicOne::getName() const {
	return "Atomic test: checking real-time";
}

/** 用例描述 */
string AtomicOne::getDescription() const {
	return "Test whether the database operation is atomic by random updating and checking at last";
}

/**
 * 对数据进行随机更新
 */
void AtomicOne::run() {
	m_threads = new TestOperationThread*[m_threadNum];
	for (uint i = 0; i < m_threadNum; i++) {
		m_threads[i] = new TestOperationThread((TestCase*)this, i);
	}

	for (size_t i = 0; i < m_threadNum; i++) {
		m_threads[i]->start();
	}

	for (size_t i = 0; i < m_threadNum; i++) {
		m_threads[i]->join();
	}
}


/**
* 生成表数据
* @param totalRecSize	IN/OUT 要生成数据的大小，返回生成后的真实大小
* @param recCnt		IN/OUT 生成记录的个数，返回生成后的真实记录数
*/
void AtomicOne::loadData(u64 *totalRecSize, u64 *recCnt) {
	openTable(true);

	Connection *conn = m_db->getConnection(false);
	Session *session = m_db->getSessionManager()->allocSession("AtomicOne::loadData", conn);

	u64 dataSize1 = m_recCount * m_tableInfo[0]->m_table->getTableDef()->m_maxRecSize;
	m_tableInfo[0]->m_recCntLoaded = BlogTable::populate(session, m_tableInfo[0]->m_table, &dataSize1, MAX_DATA_ID, 0);
	u64 dataSize2 = m_recCount * m_tableInfo[1]->m_table->getTableDef()->m_maxRecSize;
	m_tableInfo[1]->m_recCntLoaded = CountTable::populate(session, m_tableInfo[1]->m_table, &dataSize2, MAX_DATA_ID, 0);

	*totalRecSize = dataSize1 + dataSize2;
	*recCnt = m_recCount;

	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(conn);

	closeDatabase();
}

/**
 * 预热函数
 */
void AtomicOne::warmUp() {
	openTable();
	return;
}

/**
 * 执行最后的数据检验函数，进行表扫描同时验证记录每个字段都对应相同的一个opid
 */
bool AtomicOne::verify() {
	for (u16 i = 0; i < m_tables; i++)
		if (!ResultChecker::checkTableToIndice(m_db, m_tableInfo[i]->m_table, false, true))
			return false;

	return true;
}

/**
 * 执行指定次数的更新
 * 更新过程中会根据步骤2当中描述的对记录进行一致性检查
 * @param param	线程参数
 */
void AtomicOne::mtOperation(void *param) {
	u64 mark = *((u64*)param);
	Table *table = m_tableInfo[chooseTableNo((uint)mark)]->m_table;

	uint count = 0;
	while (true) {
		Connection *conn = m_db->getConnection(false);
		Session *session = m_db->getSessionManager()->allocSession("AtomicOne::mtOperation", conn);
		u64 key = RandomGen::nextInt(0, 10000);
		if (TableDMLHelper::updateRecord(session, table, &key, &key, count, true) == SUCCESS)
			count++;
		if (count >= TASK_OPERATION_NUM)	// 保证确实更新了TASK_OPERATION_NUM次
			break;
		m_db->getSessionManager()->freeSession(session);
		m_db->freeConnection(conn);
	}

}