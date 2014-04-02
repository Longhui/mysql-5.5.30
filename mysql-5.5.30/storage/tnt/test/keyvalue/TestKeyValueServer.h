/*
*	����KeyValueServer��api
*
*	@author	�ζ���(liaodingbai@corp.netease.com)
*/

#ifndef _NTSETEST_KEYVALUE_SERVER_H_
#define _NTSETEST_KEYVALUE_SERVER_H_

#include <cppunit/extensions/HelperMacros.h>
#include "keyvalue/KeyValueServer.h"
#include "api/Database.h"
#include "api/TestHelper.h"

using namespace ntse;
using namespace std;

/**
*	KeyValueServer�Ĳ�����
*/
class KeyValueServerTestCase: public CPPUNIT_NS::TestFixture {
	CPPUNIT_TEST_SUITE(KeyValueServerTestCase);

	//CPPUNIT_TEST(testSimpleRun);
	CPPUNIT_TEST(testThreadPoolRun);
	CPPUNIT_TEST(testThreadedRun);
	CPPUNIT_TEST(testClose);

	CPPUNIT_TEST_SUITE_END();

public:
	static const char* getName();
	static const char* getDescription();
	static bool isBig();
	void setUp();
	void tearDown();

protected:
	
	void testSimpleRun();
	void testThreadPoolRun();
	void testThreadedRun();

	void testClose();

private:
	Database	*m_db;				/** ntse���ݿ� */
	Config		m_config;			/** ���ݿ�������� */
	TblInterface	*m_table;		/** ��ǰ���������ݿ�� */

	static const string baseDir;	/** ��Ŀ¼ */
};

#endif