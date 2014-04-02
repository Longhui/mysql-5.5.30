/**
 * �����������ܲ���ʵ��
 *
 * ����Ŀ�ģ������������ܺ�������֮��ĺ�����ϵ
 * ��ģʽ��Count
 * �������ã�NTSE���ã�NC_MEDIUM data_size = 16M, 32M, 64M, 128M, 256M, 512M
 * �������ݣ����������
 * �������̣�
 *		1.	װ������
 *		2.	ΪCount�д�������
 *		3.	��¼������С��������������С�ͼ�¼������������������
 *
 * @author �ձ�(bsu@corp.netease.com naturally@163.org)
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
 * ���캯��
 * ����dataSizeָ��������ʼ���ݴ�С
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
 * ȡ��������
 */
string IndexCreationTest::getName() const {
	stringstream ss;
	ss << "IndexCreation(DataSize:";
	ss << m_dataSize / 1024 / 1024;
	ss << "MB)";
	return ss.str();
}


/**
 * ȡ����������
 */
string IndexCreationTest::getDescription() const {
	return "Test index creation of a specified data size heap";
}


/**
 * װ�����ݣ�ͳһʹ��Count��
 */
void IndexCreationTest::loadData(u64 *totalRecSize, u64 *recCnt) {
	openTable(true);
	// ����Session
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
 * ִ�в������ⴴ����������
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
 * ����������Count��COUNT�е���������
 */
IndexDef* IndexCreationTest::newCountColIndexDefOfCountTable() {
	ColumnDef *colDef = m_table->getTableDef()->m_columns[1];
	IndexDef *indexDef = new IndexDef("COUNT", 1, &colDef);
	indexDef->m_maxKeySize = 1 + 4;	//	λͼλ1������λ4

	return indexDef;
}

