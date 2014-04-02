/*
*	测试KeyValueHandler函数api
*
*	@author	廖定柏(liaodingbai@corp.netease.com)
*/

#ifndef _NTSETEST_KEYVALUE_HANDLER_H_
#define _NTSETEST_KEYVALUE_HANDLER_H_

#include <cppunit/extensions/HelperMacros.h>
#include "keyvalue/KeyValueHandler.h"
#include "api/Table.h"
#include "api/Database.h"
#include "api/TestHelper.h"

using namespace ntse;

#ifndef WIN32
#define FLT_EPSILON     1.192092896e-07F        /* smallest such that 1.0+FLT_EPSILON != 1.0 */
#endif

/**
 *	KeyValueHandler的测试类
 */
class KeyValueHandlerTestCase: public CPPUNIT_NS::TestFixture {
	CPPUNIT_TEST_SUITE(KeyValueHandlerTestCase);

	CPPUNIT_TEST(testGet_Accuracy1);
	CPPUNIT_TEST(testGet_Accuracy2);
	CPPUNIT_TEST(testGet_Accuracy3);

	CPPUNIT_TEST(testPut_Accuracy1);
	CPPUNIT_TEST(testPut_Accuracy2);
	CPPUNIT_TEST(testPut_Accuracy3);

	CPPUNIT_TEST(testSetrec_Accuracy1);
	CPPUNIT_TEST(testSetrec_Accuracy2);

	CPPUNIT_TEST(testReplace_Accuracy1);
	CPPUNIT_TEST(testReplace_Accuracy2);

	CPPUNIT_TEST(testRemove_Accuracy1);
	CPPUNIT_TEST(testRemove_Accuracy2);

	CPPUNIT_TEST(testUpdate_Accuracy1);
	CPPUNIT_TEST(testUpdate_Accuracy2);
	CPPUNIT_TEST(testUpdate_Accuracy3);
	CPPUNIT_TEST(testUpdate_Accuracy4);
	CPPUNIT_TEST(testUpdate_Accuracy5);
	CPPUNIT_TEST(testUpdate_Accuracy6);
	CPPUNIT_TEST(testUpdate_Accuracy7);
	CPPUNIT_TEST(testUpdate_Accuracy8);
	CPPUNIT_TEST(testUpdate_Accuracy9);
	CPPUNIT_TEST(testUpdate_Accuracy10);
	//CPPUNIT_TEST(testUpdate_Accuracy11);
	CPPUNIT_TEST(testUpdate_Accuracy12);

	CPPUNIT_TEST(testPutOrUpdate_Accuracy1);
	CPPUNIT_TEST(testPutOrUpdate_Accuracy2);

	CPPUNIT_TEST(testMultiget_Accuracy1);
	CPPUNIT_TEST(testMultiget_Accuracy2);

	CPPUNIT_TEST(testGetTableDef);

	CPPUNIT_TEST(testGetTableDef_Failed);

	CPPUNIT_TEST(testVersionMissMatched);

	CPPUNIT_TEST_SUITE_END();

public:
	static const char* getName();
	static const char* getDescription();
	static bool isBig();
	void setUp();
	void tearDown();

protected:
	void testGet_Accuracy1();
	void testGet_Accuracy2();
	void testGet_Accuracy3();

	void testPut_Accuracy1();
	void testPut_Accuracy2();
	void testPut_Accuracy3();

	void testSetrec_Accuracy1();
	void testSetrec_Accuracy2();

	void testReplace_Accuracy1();
	void testReplace_Accuracy2();

	void testRemove_Accuracy1();
	void testRemove_Accuracy2();

	void testUpdate_Accuracy1();
	void testUpdate_Accuracy2();
	void testUpdate_Accuracy3();
	void testUpdate_Accuracy4();
	void testUpdate_Accuracy5();
	void testUpdate_Accuracy6();
	void testUpdate_Accuracy7();
	void testUpdate_Accuracy8();
	void testUpdate_Accuracy9();
	void testUpdate_Accuracy10();
	void testUpdate_Accuracy11();
	void testUpdate_Accuracy12();

	void testPutOrUpdate_Accuracy1();
	void testPutOrUpdate_Accuracy2();

	void testMultiget_Accuracy1();
	void testMultiget_Accuracy2();

	void testGetTableDef();
	void testGetTableDef_Failed();

	void testVersionMissMatched();

private:
	/**
	 *	将数值类型转化成字节流存储在string中
	 *
	 *	@param	retValue	存储字节流的字符串
	 *	@param	number	要转化的数值
	 *	
	 *	@return	包含二进制数据的字符串
	 */
	template<typename T> static void number2String(string& retValue, T number)	{
		byte *value = (byte*)&number;
		bytes2String(retValue, value, sizeof(number));
	}

	/**
	*	比较两个浮点数
	*
	*	@param	浮点数一
	*	@param	浮点数二
	*	@return	相等返回true，否则false
	*/
	static bool isEqual(float a, float b)	{
		float x = a - b;

		if ((x >= -FLT_EPSILON) && (x <= FLT_EPSILON))
			return true;
		return false;
	}

	Database	*m_db;				/** ntse数据库 */
	Config		m_config;			/** 数据库相关配置 */
	TblInterface	*m_table;		/** 当前操作的数据库表 */

	KeyValueHandler *m_instance;	/** KeyValueHandler测试实例 */

	static const string baseDir;	/** 根目录 */
};

#endif

