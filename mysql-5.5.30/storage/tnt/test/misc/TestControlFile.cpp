#include "misc/TestControlFile.h"
#include "util/File.h"
#include "misc/Syslog.h"
#include "Test.h"
#include <iostream>

using namespace std;

const char* CtrlFileTestCase::getName() {
	return "Control file tests";
}

const char* CtrlFileTestCase::getDescription() {
	return "Test function of control file such create/open, table id allocation, table create/drop etc.";
}

bool CtrlFileTestCase::isBig() {
	return false;
}

void CtrlFileTestCase::setUp() {
	File f("ntse_ctrl");
	f.remove();
}

void CtrlFileTestCase::tearDown() {
	if (m_ctrlFile) {
		m_ctrlFile->close();
		delete m_ctrlFile;
	}
	File f("ntse_ctrl");
	f.remove();
}

void CtrlFileTestCase::testBasic() {
	Syslog syslog("ntse.log", EL_LOG, true, true);
	EXCPT_OPER(ControlFile::create("ntse_ctrl", &syslog));

	EXCPT_OPER(m_ctrlFile = ControlFile::open("ntse_ctrl", &syslog));
	CPPUNIT_ASSERT(m_ctrlFile->isCleanClosed());
	m_ctrlFile->close();
	delete m_ctrlFile;
	m_ctrlFile = NULL;

	EXCPT_OPER(m_ctrlFile = ControlFile::open("ntse_ctrl", &syslog));
	CPPUNIT_ASSERT(m_ctrlFile->isCleanClosed());
	m_ctrlFile->close();
	delete m_ctrlFile;
	m_ctrlFile = NULL;

	CPPUNIT_ASSERT(File("ntse_ctrl").remove() == File::E_NO_ERROR);

	// 控制文件存但格式不正确
	/// 控制文件为空
	{
		File f("ntse_ctrl");
		CPPUNIT_ASSERT(f.create(false, false)  == File::E_NO_ERROR);
		CPPUNIT_ASSERT(f.close()  == File::E_NO_ERROR);
		try {
			ControlFile::open("ntse_ctrl", &syslog);
			CPPUNIT_FAIL("Should fail here");
		} catch (NtseException &e) {
			CPPUNIT_ASSERT(e.getErrorCode() == NTSE_EC_FORMAT_ERROR);
		}
		CPPUNIT_ASSERT(File("ntse_ctrl").remove() == File::E_NO_ERROR);
	}

	/// 控制文件内容不一致
	{
		EXCPT_OPER(ControlFile::create("ntse_ctrl", &syslog));
		EXCPT_OPER(m_ctrlFile = ControlFile::open("ntse_ctrl", &syslog));
		CPPUNIT_ASSERT(m_ctrlFile->m_header.m_numTables == 0);
		CPPUNIT_ASSERT(m_ctrlFile->allocTableId() == 1);
		m_ctrlFile->createTable("test", 1, false);

		// 人为制造出一个不一致
		CPPUNIT_ASSERT(m_ctrlFile->m_header.m_nextTableId == 2);
		m_ctrlFile->m_header.m_nextTableId = 1;

		string s = m_ctrlFile->serialize();
		
		m_ctrlFile->m_header.m_nextTableId = 2;
		m_ctrlFile->close();
		delete m_ctrlFile;
		m_ctrlFile = NULL;

		CPPUNIT_ASSERT(File("ntse_ctrl").remove() == File::E_NO_ERROR);
		File f("ntse_ctrl");
		CPPUNIT_ASSERT(f.create(false, false)  == File::E_NO_ERROR);
		CPPUNIT_ASSERT(f.setSize(s.size())  == File::E_NO_ERROR);
		CPPUNIT_ASSERT(f.write(0, s.size(), s.c_str()) == File::E_NO_ERROR);
		CPPUNIT_ASSERT(f.close()  == File::E_NO_ERROR);

		try {
			ControlFile::open("ntse_ctrl", &syslog);
			CPPUNIT_FAIL("Should fail here");
		} catch (NtseException &e) {
			CPPUNIT_ASSERT(e.getErrorCode() == NTSE_EC_FORMAT_ERROR);
		}

		CPPUNIT_ASSERT(File("ntse_ctrl").remove() == File::E_NO_ERROR);
	}
}

void CtrlFileTestCase::testCreateTable() {
	Syslog syslog("ntse.log", EL_LOG, true, true);
	EXCPT_OPER(ControlFile::create("ntse_ctrl", &syslog));

	EXCPT_OPER(m_ctrlFile = ControlFile::open("ntse_ctrl", &syslog));
	CPPUNIT_ASSERT(m_ctrlFile->allocTableId() == 1);
	m_ctrlFile->createTable("test", 1, false);
	m_ctrlFile->close();
	delete m_ctrlFile;
	m_ctrlFile = NULL;

	EXCPT_OPER(m_ctrlFile = ControlFile::open("ntse_ctrl", &syslog));
	CPPUNIT_ASSERT(m_ctrlFile->allocTableId() == 2);
	m_ctrlFile->createTable("test2", 2, true);
	m_ctrlFile->close();
	delete m_ctrlFile;
	m_ctrlFile = NULL;
}

// 单元测试时有时发现ControlFile::updateFile会不成功，因此增加这个测试用例
// 疯狂的更新控制文件
void CtrlFileTestCase::testCrazyUpdate() {
	Syslog syslog("ntse.log", EL_LOG, true, true);
	EXCPT_OPER(ControlFile::create("ntse_ctrl", &syslog));

	EXCPT_OPER(m_ctrlFile = ControlFile::open("ntse_ctrl", &syslog));
	for (int i = 0; i < 10000; i++) {
		EXCPT_OPER(m_ctrlFile->allocTableId());
	}
	m_ctrlFile->close();
	delete m_ctrlFile;
	m_ctrlFile = NULL;
}
