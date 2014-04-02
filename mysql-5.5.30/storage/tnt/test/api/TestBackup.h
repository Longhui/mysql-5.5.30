/**
 * 测试数据库备份恢复功能
 *
 * @author 余利华(ylh@163.org)
 */

#ifndef _NTSETEST_BACKUP_H_
#define _NTSETEST_BACKUP_H_

#include <cppunit/extensions/HelperMacros.h>
#include "api/Database.h"
#include <string>
#include <util/System.h>
#include <util/Sync.h>
using namespace std;
using namespace ntse;
/** 备份测试类 */
struct BackupTest {
	BackupTest(): m_mutex("BackupTest::mutex", __FILE__, __LINE__) {
		m_refBackupDir = "testdb_reference_backup";
		m_backupDir = "testdb_backup";
		m_dbdir = "db";
		m_refdbdir = "refdb";
		m_logFileSize = 60 * 1024;
		m_mysqldb = "mysql";
		delete [] m_dbConfig.m_basedir;
		m_dbConfig.m_basedir = System::strdup(m_dbdir.c_str());
		m_dbConfig.m_logFileSize = m_logFileSize;
		m_dbConfig.m_mmsSize = 10;
		m_blogId = 0;
		m_backupTailLsn = INVALID_LSN;
	}
	
	void setUp();
	void tearDown();
	void prepare() throw(NtseException);
	void verify() throw(NtseException);

	string m_refBackupDir;
	string m_backupDir;
	string m_dbdir;
	string m_refdbdir;
	string m_mysqldb;
	Config m_dbConfig;
	

	uint	m_logFileSize;
	u64		m_blogId;
	ntse::Mutex	m_mutex;
	
	LsnType		m_backupTailLsn;
};

class BackupTestCase: public CPPUNIT_NS::TestFixture {
	CPPUNIT_TEST_SUITE(BackupTestCase);
	CPPUNIT_TEST(testBackupSpecial);
	CPPUNIT_TEST(testBackupBasic);
	CPPUNIT_TEST(testBackupMT);
	CPPUNIT_TEST(testBackupHeapExtended);
	CPPUNIT_TEST(testReVerifyOnError);
	CPPUNIT_TEST_SUITE_END();

public:
	static const char* getName();
	static const char* getDescription();
	static bool isBig();
	void setUp();
	void tearDown();

protected:
	void testBackupSpecial();
	void testBackupBasic();
	void testBackupMT();
	void testBackupHeapExtended();
	void testReVerifyOnError();
private:
	void verifyDb();
	void initBackupTest();
	void destroyBackupTest();
private:
	Database	*m_db;
	BackupTest	m_backupTest;

};

#endif

