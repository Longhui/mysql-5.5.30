/**
* 测试大对象碎片整理操作
* 
* @author zx (zx@corp.netease.com, zx@163.org)
*/

#ifndef _NTSETEST_LOBDFRAGE_H_

#include <cppunit/extensions/HelperMacros.h>
#include "lob/Lob.h"
#include "api/Database.h"

using namespace std;
using namespace ntse;

class LobDefragTestCase: public CPPUNIT_NS::TestFixture {
	CPPUNIT_TEST_SUITE(LobDefragTestCase);
	CPPUNIT_TEST(testAllDefrag);
	CPPUNIT_TEST(testDefragEx);
	CPPUNIT_TEST(testAllRedoMove);
	CPPUNIT_TEST(testAllOper);
	CPPUNIT_TEST_SUITE_END();

public:
	static const char* getName();
	static const char* getDescription();
	static bool isBig();
	void setUp();
	void tearDown();
protected:
	void testAllDefrag();
	void testAllRedoMove();
	void testDefrag(bool isCompress);
	void testDefragEx();
	void testRedoMove();

	//准备环境
	void insertAndDelLobs(uint);
	void insertAndDelLobs(bool isCompress);
	void insertAndDelLobs2();
	
	//测试无mms的小型大对象操作
	void testAllSmallOper();
	void testAllSmallToBigOper();
	void testAllOper();

private:
	Config m_cfg;             /** 配置文件 */
	Database *m_db;           /** 数据库对象 */
	TableDef *m_tableDef;	  /** 表定义 */
	Connection *m_conn;       /** 连接对象 */
	LobStorage *m_lobStorage; /** 大对象存储对象 */
	LobStorage *createLobStorage();
	void dropLobStorage();
	byte* createLob(uint len);
	bool lobIsSame(byte *src, byte *dest, uint len);
	uint getLobSize(uint size,  bool isCompress);
	void clearLobs();
	void insertLob(Session *session, uint size, bool isCompress);

	u64 getLobIndexPageLSN(Session *session, u64 pageNum);
	u64 getLobPageLSN(Session *session, u64 pageNum);

	void restoreLobPage(Session *session, u64 pageNum, byte *pageBuffer);
	void restoreLobIndexPage(Session *session, u64 pageNum, byte *pageBuffer);
	void backupLobPage(Session *session, u64 pageNum, byte *pageBuffer);
	void backupLobIndexPage(Session *session, u64 pageNum, byte *pageBuffer);

	//下面是新增的当小型大对象不放在mms情况
	void testSmallOper(bool);
	void testSmallToBigOper(bool);
};


#endif
