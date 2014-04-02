/**
* 测试大对象基本操作
*
* @author zx (zx@corp.netease.com, zx@163.org)
*/

#ifndef _NTSETEST_LOBOPER_H_
#define _NTSETEST_LOBOPER_H_

#include <cppunit/extensions/HelperMacros.h>
#include "lob/Lob.h"
#include "api/Database.h"
#include "util/Thread.h"

using namespace std;
using namespace ntse;

class LobTester;
enum LobTask {
	 LOB_INSERT1 = 0,
	 LOB_INSERT2,
	 LOB_INSERT,
	 L0B_READ,
	 LOB_DEL,
	 LOB_DELBYID,
	 LOB_UPDATE,
	 LOB_DEFRAG_DOING,
	 LOB_DEFRAG_FINISH,
	 LOB_MAX
};

class LobOperTestCase: public CPPUNIT_NS::TestFixture {
	CPPUNIT_TEST_SUITE(LobOperTestCase);
	CPPUNIT_TEST(testAllReadBig);
	CPPUNIT_TEST(testAllBigUpdate);
	CPPUNIT_TEST(testAllDelBig);
	CPPUNIT_TEST(testAllRedoCreate);
	CPPUNIT_TEST(testRedoInsertAndDel);
	CPPUNIT_TEST(testRedoInsertAndDelEx);
	CPPUNIT_TEST(testAllRedoUpdate);
	CPPUNIT_TEST(testAllInsertSmall);
	CPPUNIT_TEST(testAllInsertTOBig);
	CPPUNIT_TEST(testAllSLRedoInsert);
	CPPUNIT_TEST(testAllSLRedoUpdate);
	CPPUNIT_TEST(testAllSLRedoUpdateMms);
	CPPUNIT_TEST(testAllSLRedoDel);
	CPPUNIT_TEST(testGetFiles);
	CPPUNIT_TEST(insertLobAndLogReverse);
	CPPUNIT_TEST(insertRedoAndExtendFile);
	CPPUNIT_TEST(defragFirstAndRead);
	CPPUNIT_TEST(defragFirstAndDel);
	CPPUNIT_TEST(defragFirstAndUpdate);
	CPPUNIT_TEST(insertGetFreePage);
	CPPUNIT_TEST(insertGetNoFreePage);
	CPPUNIT_TEST(insertGetNoFirstPage);
	CPPUNIT_TEST(defragAndInsert);
	CPPUNIT_TEST(testSizeZero);
	CPPUNIT_TEST(testStatus);
	CPPUNIT_TEST(testCreateBigLobException);
	CPPUNIT_TEST(testCreateLobIndexException);
	CPPUNIT_TEST(testCreateSmallLobException);
	CPPUNIT_TEST(testOpenBigLobException);
	CPPUNIT_TEST(testOpenLobIndexException);
	CPPUNIT_TEST_SUITE_END();

public:
	static const char* getName();
	static const char* getDescription();
	static bool isBig();
	void setUp();
	void tearDown();

	//测试真实blog数据
	void testBlog();
	//测试size为0情况
	void  testSizeZero();
	void  testStatus();


	//测试多线程
	void testCreateBigLobException();
	void testCreateSmallLobException();
	void testCreateLobIndexException();

	void testOpenBigLobException();
	void testOpenLobIndexException();

	void insertLobAndLogReverse();
	void doInsert1();
	void doInsert2();

	void defragFirstAndRead();
	void doRead();
	void doDefragForRead();

	void defragFirstAndDel();
	void doDel();

	void defragFirstAndUpdate();
	void doUpdate();

	void defragAndInsert();
	void doInsert(LobTester *tester);
	void doDefrag();

	void insertGetFreePage();

	void insertGetNoFreePage();

	void insertGetNoFirstPage();
	void doDelByLobid(LobId  lid);
	void doDelByLobid();

	void insertRedoAndExtendFile();
protected:
	void testAllInsertBig();
	void testAllReadBig();
	void testAllBigUpdate();
	void testAllDelBig();
	void testAllRedoInsert();
	void testRedoInsertAndDel();
	void testRedoInsertAndDelEx();
	void testAllUpdateLog();
	void testAllRedoUpdate();
	void testAllRedoCreate();

	void testAllInsertSmall();
	void testAllInsertTOBig();
	void testAllSLRedoInsert();
	void testAllSLRedoUpdate();
	void testAllSLRedoUpdateMms();
	void testAllSLRedoDel();

	void testInsertBig(bool isCompress);
	void testReadBig(bool isCompress);
	//void testSmallInsert();
	//测试更新大型大对象
	void testBigUpdate(bool isCompress);
	//测试删除大型大对象
	void testDelBig(bool isCompress);
	//void testTruncate();
	void testRedoCreate(bool isCompress);
	void testRedoInsert(bool isCompress);
	void testRedoDel(bool isCompress);
	void testRedoDelEx(bool real);
	void testUpdateLog(bool isCompress);
	void testRedoUpdate(bool isCompress);
	void testRedoUpdateEx();
	//void testRedoTruncate();

	//测试小型大对象
	void testInsertSmall(bool isCompress);
	//void testInsertSmallAndCompress();
	void testInsertTOBig(bool isCompress);
	void testSLRedoInsert(bool isCompress);
	void testSLRedoUpdate(bool isCompress);
	void testSLRedoUpdateMMS(bool isCompress);
	void testSLRedoDel(bool isCompress);

	void testGetFiles();
	void testGetStatus();

	u32 byteReverseEndian(byte *l);
	//void helpInsert();
private:
	Config m_cfg;             /** 配置文件 */
	Database *m_db;           /** 数据库对象 */
	TableDef *m_tableDef;	  /** 表定义 */
	Connection *m_conn;       /** 连接对象 */
	LobStorage *m_lobStorage; /** 大对象存储对象 */
	LobStorage *createLobStorage();
	void dropLobStorage();
	void closeLobStorage();

	void insertBig(bool isCompress);
	LobId insertTwoPages(bool isCompress);
	LobId insertOnePage(bool isCompress);
	LobId insertTotalPage(bool isCompress, bool random = false);
	LobId insertBoundaryPage(bool isCompress);

	byte* createLob(uint *len);
	byte* createLob(uint len, bool useRandom = false);
	char* randomString(uint size);
	char* pesuRandomString(uint size);
	bool lobIsSame(byte *src, byte *dest, uint len);
	uint getLobSize(uint size, bool isCompress);
	u8 getRandom();

	u64 getLobIndexPageLSN(Session *session, u64 pageNum);
	void setLobIndexPageLSN(Session *session, u64 pageNum, u64 lsn);
	u64 getLobPageLSN(Session *session, u64 pageNum);
	void setLobPageLSN(Session *session, u64 pageNum, u64 lsn);
	bool compareLobs();
	void clearLobs();

	void restoreLobPage(Session *session, u64 pageNum, byte *pageBuffer);
	void restoreLobIndexPage(Session *session, u64 pageNum, byte *pageBuffer);
	void backupLobPage(Session *session, u64 pageNum, byte *pageBuffer);
	void backupLobIndexPage(Session *session, u64 pageNum, byte *pageBuffer);

	void insertAndDelLobs(bool isCompress);
	void insertLob(Session *session, uint size, bool isCompress);
};

static void backupLobFile(File *file, const char *backupName);
static void backupLobFile(const char *origName, const char *backupName);
static void restoreLobFile(const char *backupFile, const char *origFile);
//int compareFile(File *file1, File *file2, int startPage = 0);



/************************************************************************/
/*                     测试线程类									    */
/************************************************************************/
class LobTester: public ntse::Thread {

public:
	LobTester(LobOperTestCase *lobTestCase, LobTask lobTask) : Thread("LobTester") {
		m_testCase = lobTestCase;
	    m_task = lobTask;
		m_testHelper = NULL;
	}

	LobTester(LobOperTestCase *lobTestCase, LobTask lobTask, LobTester *helper) : Thread("LobTester") {
		m_testCase = lobTestCase;
		m_task = lobTask;
		m_testHelper = helper;
	}
private:
	virtual void run();

	LobOperTestCase *m_testCase;
	LobTask m_task;
	LobTester *m_testHelper;

};

#endif
