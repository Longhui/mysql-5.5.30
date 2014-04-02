/**
 * ���ɲ���5.1.1���Լ�¼����ԭ����
 *
 * ����Ŀ�ģ���֤NTSE��¼������ԭ���ԣ�ʵʱ��֤����ÿ�θ��²���֮�䣬���Լ�¼��ԭ���Խ���ʵʱ��֤
 * ��ģʽ��Blog��Count
 * �������ã�NTSE���ã�NC_SMALL thread_count = 500
 * �������ݣ�����Ψһ��¼���ɷ���������һ������ID:opid�����ɵļ�¼�������㡰����ID��֮�⣬�����е����ݶ�����opidһһ��Ӧ��
 * �������̣�
 *	1.	����100����¼
 *	2.	Ϊ���ű�ֱ�����thread_count/2�����������̣߳�ÿ���߳�ִ��10000������ÿ������������²�����
 *		a)��ȡ��ǰ��β����Ĳ���ID: opid
 *		b)����һ��0��100֮��������id���ҵ�����ID>=id�ĵ�һ����¼
 *		c)��֤���ڼ�¼R�������У�����ID֮�⣩����Ӧͬһ������ID:opid
 *		d)����opid�����µļ�¼���ݣ�����������¼
 *	3.	�����������������֮�󣬽���һ�α�ɨ�裬���ڵ�ǰ��¼ΪR����֤R�������У�����ID֮�⣩����Ӧͬһ������ID:opid
 *
 * �ձ�(...)
 */

#include "AtomicOne.h"
#include "DbConfigs.h"
#include "Random.h"
#include "IntgTestHelper.h"

using namespace std;
using namespace ntsefunc;

/** �õ��������� */
string AtomicOne::getName() const {
	return "Atomic test: checking real-time";
}

/** �������� */
string AtomicOne::getDescription() const {
	return "Test whether the database operation is atomic by random updating and checking at last";
}

/**
 * �����ݽ����������
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
* ���ɱ�����
* @param totalRecSize	IN/OUT Ҫ�������ݵĴ�С���������ɺ����ʵ��С
* @param recCnt		IN/OUT ���ɼ�¼�ĸ������������ɺ����ʵ��¼��
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
 * Ԥ�Ⱥ���
 */
void AtomicOne::warmUp() {
	openTable();
	return;
}

/**
 * ִ���������ݼ��麯�������б�ɨ��ͬʱ��֤��¼ÿ���ֶζ���Ӧ��ͬ��һ��opid
 */
bool AtomicOne::verify() {
	for (u16 i = 0; i < m_tables; i++)
		if (!ResultChecker::checkTableToIndice(m_db, m_tableInfo[i]->m_table, false, true))
			return false;

	return true;
}

/**
 * ִ��ָ�������ĸ���
 * ���¹����л���ݲ���2���������ĶԼ�¼����һ���Լ��
 * @param param	�̲߳���
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
		if (count >= TASK_OPERATION_NUM)	// ��֤ȷʵ������TASK_OPERATION_NUM��
			break;
		m_db->getSessionManager()->freeSession(session);
		m_db->freeConnection(conn);
	}

}