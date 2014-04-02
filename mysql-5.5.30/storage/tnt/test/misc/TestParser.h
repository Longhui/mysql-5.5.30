/**
* 测试<code>Parser</code>.
*
* @author 聂明军(niemingjun@corp.netease.com, niemingjun@163.org)
*/
#include "misc/Parser.h"

#ifndef _NTSETEST_PARSER_H_
#define _NTSETEST_PARSER_H_

#include <cppunit/extensions/HelperMacros.h>



/** Mutex同步机制功能测试 */
class ParserTestCase : public CPPUNIT_NS::TestFixture {
	CPPUNIT_TEST_SUITE(ParserTestCase);
	CPPUNIT_TEST(testMatch);
	CPPUNIT_TEST(testParser);
	CPPUNIT_TEST(testParseList);
	CPPUNIT_TEST(testParsePair);
	CPPUNIT_TEST(testParseBool);
	CPPUNIT_TEST(testParseInt);
	CPPUNIT_TEST(testParseU64);
	CPPUNIT_TEST(testParseBracketsGroups);
	CPPUNIT_TEST(testIsCommand);
	CPPUNIT_TEST(testNextInt);
	CPPUNIT_TEST(testNextString);
	CPPUNIT_TEST(testRemainingString);
	CPPUNIT_TEST(testTrimWhiteSpace);

	CPPUNIT_TEST_SUITE_END();

public:
	static const char* getName();
	static const char* getDescription();
	static bool isBig();

protected:
	void testMatch();
	void testParser();
	void testParseList();
	void testParsePair();
	void testParseBool();
	void testParseInt();
	void testParseU64();
	void testParseBracketsGroups();
	void testIsCommand();
	void testNextInt();
	void testNextString();
	void testRemainingString();
	void testTrimWhiteSpace();
};

#endif

