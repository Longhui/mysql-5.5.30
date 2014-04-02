/**
 * ���Է�����PAR�ļ���������
 * 
 * @author wanggongpu(wanggongpu@corp.netease.com)
 */
#ifndef _NTSETEST_PARFILE_PARSER_H_
#define _NTSETEST_PARFILE_PARSER_H_

#include <cppunit/extensions/HelperMacros.h>

/** ParFileParser��ӿڹ��ܲ��� */
class ParFileParserTestCase : public CPPUNIT_NS::TestFixture {
	CPPUNIT_TEST_SUITE(ParFileParserTestCase);
	CPPUNIT_TEST(testCheckPartitionByPhyTabName);
	CPPUNIT_TEST(testCheckPartitionByLogPath);
	CPPUNIT_TEST(testParseLogicTabName);
	CPPUNIT_TEST(testParseParFile);
	CPPUNIT_TEST_SUITE_END();

public:
	static const char* getName();
	static const char* getDescription();
	static bool isBig();

protected:
	void testCheckPartitionByPhyTabName();
	void testCheckPartitionByLogPath();
	void testParseLogicTabName();
	void testParseParFile();	
};

#endif