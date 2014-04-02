/**
 * �����ܵĻָ�����
 *
 * @author ��Դ(wangyuan@corp.netease.com, wy@163.org)
 */

#ifndef _NTSETEST_RECOVER_H_
#define _NTSETEST_RECOVER_H_

#include <cppunit/extensions/HelperMacros.h>
#include "api/Database.h"
#include <string>
#include <util/System.h>
#include <util/Sync.h>

using namespace std;
using namespace ntse;

/** �ָ����̲��Ի��࣬���ָ��������ۺϲ��� */
class RecoverTestCase: public CPPUNIT_NS::TestFixture {
	CPPUNIT_TEST_SUITE(RecoverTestCase);
	CPPUNIT_TEST(testRecoveryFailed);
	CPPUNIT_TEST(testBug14617);
	CPPUNIT_TEST(testModifyConfig);
	CPPUNIT_TEST_SUITE_END();

public:
	static const char* getName();
	static const char* getDescription();
	static bool isBig();
	virtual void setUp();
	virtual void tearDown();

	void testRecoveryFailed();
	void testBug14617();
	void testModifyConfig();
protected:
	void truncateLog(LogType logType, int nth = 0, bool inclusive = false);
	void prepareBlogCount(bool useMms);
	void prepareBlog(bool useMms);
	void prepareTable(TableDef *tableDef);
	void openTable(const char *path) throw(NtseException);
	void closeTable();
	void backupTable(const char *path, bool hasLob);
	void restoreTable(const char *path, bool hasLob);
	void checkRecords(Table *table, Record **records, uint numRows);
	void freeMySQLRecords(const TableDef *tableDef, Record **records, uint numRows);
	void freeMySQLRecord(const TableDef *tableDef, Record *record);
	void freeEngineMySQLRecords(const TableDef *tableDef, Record **records, uint numRows);
	void freeEngineMySQLRecord(const TableDef *tableDef, Record *record);
	void freeUppMySQLRecords(const TableDef *tableDef, Record **records, uint numRows);
	void freeUppMySQLRecord(const TableDef *tableDef, Record *record);
	bool loadConnectionAndSession();
	void freeConnectionAndSession();
	void closeDatabase(bool flushLog = true, bool flushData = false);

protected:
	Config		m_config;	/** ����ʱʹ�õ����ݿ����� */
	Database	*m_db;		/** ����ʱ���������ݿ�ʵ�� */
	Table		*m_table;	/** ����ʱ�����ı� */
	Connection	*m_conn;	/** ����ʱ�õ����ݿ����� */
	Session		*m_session;	/** ����ʱ�õ����ݿ�Ự */
};

#endif

