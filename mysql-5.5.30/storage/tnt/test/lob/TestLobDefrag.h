/**
* ���Դ������Ƭ�������
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

	//׼������
	void insertAndDelLobs(uint);
	void insertAndDelLobs(bool isCompress);
	void insertAndDelLobs2();
	
	//������mms��С�ʹ�������
	void testAllSmallOper();
	void testAllSmallToBigOper();
	void testAllOper();

private:
	Config m_cfg;             /** �����ļ� */
	Database *m_db;           /** ���ݿ���� */
	TableDef *m_tableDef;	  /** ���� */
	Connection *m_conn;       /** ���Ӷ��� */
	LobStorage *m_lobStorage; /** �����洢���� */
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

	//�����������ĵ�С�ʹ���󲻷���mms���
	void testSmallOper(bool);
	void testSmallToBigOper(bool);
};


#endif
