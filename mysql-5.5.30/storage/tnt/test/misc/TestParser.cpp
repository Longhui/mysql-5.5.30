/**
* 测试<code>Parser</code>.
*
* @author 聂明军(niemingjun@corp.netease.com, niemingjun@163.org)
* @version 0.3
*/

#include "misc/TestParser.h"
#include "util/SmartPtr.h"
#include "Test.h"

#include <cppunit/config/SourcePrefix.h>
#include <iostream>
using namespace std;
using namespace ntse;

/** 
 * 获取测试用例名
 *
 * @return parser测试用例名
 */
const char* ParserTestCase::getName() {
	return "Parser basic tests";
}
/** 
 * 获取测试用例描述
 *
 * @return parser测试用例描述
 */
const char* ParserTestCase::getDescription() {
	return "Test Parser functions.";
}
/** 
 * 获取测试用例是否大型测试
 *
 * @return false 表示小型单元测试
 */
bool ParserTestCase::isBig() {
	return false;
}

/** 
 * 测试Parser::match
 */
void ParserTestCase::testMatch() {
	const char *str = "This iS Me";
	bool isCatched = false;

	AutoPtr<Parser> parser(new Parser(str));
	parser->match("This");
	parser->match("Is", false);
	try {
		parser->match("Me ");
	} catch (NtseException &) {
		//no match exception expected 
		isCatched = true;
	}	
}
/** 
 * 测试Parser的基本功能。
 *
 */
void ParserTestCase::testParser() {
	const char *str = NULL;
	const char *p = NULL;
	int ret = -1;
	bool isCatched = false;
	const char *delim = NULL;
	//1. 测试单字符	
	//1.1 单个单词数字组合
	str = "this76";
	//use match
	AutoPtr<Parser> parser1_0(new Parser(str));
	EXCPT_OPER(parser1_0->match("this76"));
	EXCPT_OPER(parser1_0->checkNoRemain());
	//use nextToken
	AutoPtr<Parser> parser1_1(new Parser(str));
	p = parser1_1->nextToken();
	ret = strcmp(p, "this76");
	CPPUNIT_ASSERT(!ret);
	EXCPT_OPER(parser1_1->checkNoRemain());
	//use nextString
	AutoPtr<Parser> parser1_3(new Parser(str));
	p = parser1_3->nextString();
	ret = strcmp(p, "this76");
	CPPUNIT_ASSERT(!ret);
	EXCPT_OPER(parser1_3->checkNoRemain());

	//1.2 前导空格 + TAB + 数字字母单词 + TAB + 空格
	str = "		12this 	";
	//use match
	AutoPtr<Parser> parser2_0(new Parser(str));
	EXCPT_OPER(parser2_0->match("12this"));
	EXCPT_OPER(parser2_0->checkNoRemain());
	//use nextToke
	AutoPtr<Parser> parser2_1(new Parser(str));
	p = parser2_1->nextToken();
	ret = strcmp(p, "12this");
	CPPUNIT_ASSERT(!ret);
	EXCPT_OPER(parser2_1->checkNoRemain());
	//use nextString
	AutoPtr<Parser> parser2_3(new Parser(str));
	p = parser2_3->nextString();
	ret = strcmp(p, "12this");
	CPPUNIT_ASSERT(!ret);
	EXCPT_OPER(parser2_3->checkNoRemain());

	//1.3 测试Empty string
	str = "			 "; //一个空格 + TAB + TAB + 空格
	//use match
	AutoPtr<Parser> parser3_0(new Parser(str));
	try {
		parser3_0->match("");
	} catch (NtseException &) {
		//no next token exception expected 
		isCatched = true;
	}
	CPPUNIT_ASSERT(isCatched);
	isCatched = false;

	EXCPT_OPER(parser3_0->checkNoRemain());
	//use nextToke
	AutoPtr<Parser> parser3_1(new Parser(str));
	try {
		p = parser3_1->nextToken();
	} catch (NtseException &) {
		//no next token exception expected 
		isCatched = true;
	}
	CPPUNIT_ASSERT(isCatched);
	isCatched = false;
	//use nextString
	AutoPtr<Parser> parser3_3(new Parser(str));
	try {
		p = parser3_0->nextString();
	} catch (NtseException &) {
		//no next token exception expected 
		isCatched = true;
	}
	CPPUNIT_ASSERT(isCatched);
	isCatched = false;

	//2 常用情况测试

	//2.1 测试多个单词 " alter table   set   schema.table.cmd	=	value "
	str = " alter table   set   schema.table.cmd	=	value ";
	AutoPtr<Parser> parser4_0(new Parser(str));
	EXCPT_OPER(parser4_0->match("alter"));
	EXCPT_OPER(parser4_0->match("table"));
	EXCPT_OPER(parser4_0->match("set"));
	EXCPT_OPER(parser4_0->match("schema"));
	EXCPT_OPER(parser4_0->match("."));
	EXCPT_OPER(parser4_0->match("table"));
	EXCPT_OPER(parser4_0->match("."));
	EXCPT_OPER(parser4_0->match("cmd"));
	EXCPT_OPER(parser4_0->match("="));
	EXCPT_OPER(parser4_0->match("value"));

	EXCPT_OPER(parser4_0->checkNoRemain());
	//2.2 测试多个单词，采用nextToken
	str = " alter table   set   myschema.mytable.mycmd	=	value ";
	AutoPtr<Parser> parser4_1(new Parser(str));
	EXCPT_OPER(parser4_1->match("alter"));
	EXCPT_OPER(parser4_1->match("table"));
	EXCPT_OPER(parser4_1->match("set"));

	p = parser4_1->nextToken();
	ret = strcmp(p, "myschema");
	CPPUNIT_ASSERT(!ret);

	EXCPT_OPER(parser4_1->match("."));

	p = parser4_1->nextToken();
	ret = strcmp(p, "mytable");
	CPPUNIT_ASSERT(!ret);

	EXCPT_OPER(parser4_1->match("."));

	p = parser4_1->nextToken();
	ret = strcmp(p, "mycmd");
	CPPUNIT_ASSERT(!ret);

	EXCPT_OPER(parser4_1->match("="));
	EXCPT_OPER(parser4_1->match("value"));

	EXCPT_OPER(parser4_1->checkNoRemain());
	//2.3 测试多个单词，采用nextToken带自定义分隔符号参数。
	delim = ",";
	str = "alter table set db123.table123..123cmd123	=	enable a1, b2,	c3,	d4	";
	AutoPtr<Parser> parser4_2(new Parser(str));
	EXCPT_OPER(parser4_2->match("alter"));
	EXCPT_OPER(parser4_2->match("table"));
	EXCPT_OPER(parser4_2->match("set"));
	p = parser4_2->nextToken();
	ret = strcmp(p, "db123");
	CPPUNIT_ASSERT(!ret);
	EXCPT_OPER(parser4_2->match("."));
	p = parser4_2->nextToken();
	ret = strcmp(p, "table123");
	CPPUNIT_ASSERT(!ret);
	EXCPT_OPER(parser4_2->match(".."));
	p = parser4_2->nextToken();
	ret = strcmp(p, "123cmd123");
	CPPUNIT_ASSERT(!ret);
	parser4_2->match("=");
	parser4_2->match("enable");
	p = parser4_2->nextToken(delim);
	CPPUNIT_ASSERT(!strcmp(p, "a1"));
	p = parser4_2->nextToken(delim);
	CPPUNIT_ASSERT(!strcmp(p, "b2"));
	p = parser4_2->nextToken(delim);
	CPPUNIT_ASSERT(!strcmp(p, "c3"));
	p = parser4_2->nextToken(delim);
	CPPUNIT_ASSERT(!strcmp(p, "d4"));
	parser4_2->checkNoRemain();
	try {
		parser4_2->nextToken();
	} catch (NtseException &) {
		isCatched = true;
	}
	CPPUNIT_ASSERT(isCatched);
	isCatched = false;
}

/** 
 * 测试Parser::parseList
 */
void ParserTestCase::testParseList() {
	const char *str = "a, b ,c,d	,e,	f	,g ";//mixed with 空格+TAB
	AutoPtr<Parser> parser(new Parser(str));
	Array<char *> *colArr = NULL;
	EXCPT_OPER(colArr = Parser::parseList(str, ','));
	
	char arr[2] = {'a', '\0'};
	for (size_t i = 0; i <colArr->getSize(); ++i) {
		CPPUNIT_ASSERT(!strcmp((*colArr)[i], arr)) ;		
		arr[0]++;
	}
	for (size_t i = 0; i <colArr->getSize(); ++i) {
		delete[] (*colArr)[i];
	}
	delete colArr;
	
}
/** 
 * 测试Parser::parsePair
 */
void ParserTestCase::testParsePair() {
	const char *str = "			name		: 		value string";//mixed with 空格+TAB
	char * first = NULL;
	char * second = NULL;
	EXCPT_OPER(Parser::parsePair(str, ':', &first, &second));
	
	CPPUNIT_ASSERT(!strcmp(first, "name"));
	CPPUNIT_ASSERT(!strcmp(second, "value string"));	
	delete[] first;
	delete[] second;
}

/** 
 *	测试Parser::parseBool
 */
void ParserTestCase::testParseBool() {
	//true, false, 大小写混杂
	const char *str = "true";
	CPPUNIT_ASSERT(Parser::parseBool(str));
	str = "false";
	CPPUNIT_ASSERT(!Parser::parseBool(str));
	str = "TRue";
	CPPUNIT_ASSERT(Parser::parseBool(str));
	//测试非法字符串
	str = "T";	
	bool isCaught = false;
	try {
		Parser::parseBool(str);
	} catch (NtseException &) {
		isCaught = true;
	}
	CPPUNIT_ASSERT(isCaught);
	isCaught = false;
	str = "F";
	try {
		Parser::parseBool(str);
	} catch (NtseException &) {
		isCaught = true;
	}
	CPPUNIT_ASSERT(isCaught);
	//测试数字:
	str = "0";	
	isCaught = false;
	try {
		Parser::parseBool(str);
	} catch (NtseException &) {
		isCaught = true;
	}
	CPPUNIT_ASSERT(isCaught);
	str = "1";
	isCaught = false;
	try {
		Parser::parseBool(str);
	} catch (NtseException &) {
		isCaught = true;
	}
	CPPUNIT_ASSERT(isCaught);
}

/** 
 * 测试Parser::parseInt
 * @see <code>Parser::parseInt</code>
 */
void ParserTestCase::testParseInt() {
	//MIN_INT, MAX_INT
	const char *str = "2147483647";
	CPPUNIT_ASSERT(2147483647 == Parser::parseInt(str));
	str = "-2147483648";
	CPPUNIT_ASSERT(-2147483648 == Parser::parseInt(str));

	//0 and normal int
	str = "0";
	CPPUNIT_ASSERT(0 == Parser::parseInt(str));
	str = "-123";
	CPPUNIT_ASSERT(-123 == Parser::parseInt(str));
	str = "23";
	CPPUNIT_ASSERT(23 == Parser::parseInt(str));

	//abnormal int:
	bool isCaught = false;
	char *strArr[5] = {"+b123", "-123B", "", "+9223372036854775808", "-92233720368547758089223372036854775808"};
	for (int i = 0; i < 5; ++i) {
		try {
			Parser::parseInt(strArr[i]);
		} catch (NtseException &) {
			isCaught = true;
		}
		CPPUNIT_ASSERT(isCaught);
		isCaught = false;
	}	

	//test parse int with lower and upper	
	int wellNum = 155;
	const char *wellStr = "155";
	int low = 100, up = 200;
	const char *strTestLower = "22", *strTestUpper = "555";
	CPPUNIT_ASSERT(wellNum == Parser::parseInt(wellStr, low, up));

	isCaught = false;
	try {
		Parser::parseInt(strTestLower, low, up);
	} catch (NtseException &e) {
		UNREFERENCED_PARAMETER(e);
		isCaught = true;
	}
	CPPUNIT_ASSERT(isCaught);

	isCaught = false;
	try {
		Parser::parseInt(strTestUpper, low, up);
	} catch (NtseException &e) {
		UNREFERENCED_PARAMETER(e);
		isCaught = true;
	}
	CPPUNIT_ASSERT(isCaught);
}

/** 
 * 测试Parser::parseU64
 * @see <code>Parser::parseU64</code>
 */
void ParserTestCase::testParseU64() {
	//测试正常范围数值
	const char *str = "2147483648";
	u64 num = 0;	
	EXCPT_OPER(num = Parser::parseU64(str));
	CPPUNIT_ASSERT(2147483648 == num);
	str = "18446744073709551615";
	EXCPT_OPER(num = Parser::parseU64(str));
	CPPUNIT_ASSERT(18446744073709551615 == num);

	//测试设定范围外数值	
	bool isCatch = false;
	isCatch = false;
	str = "1234";
	int minNum = 1235;
	int maxNum = 22222;	
	try {
		Parser::parseU64(str, minNum, maxNum);
	} catch (NtseException &) {
		isCatch = true;
	}
	CPPUNIT_ASSERT(isCatch);
	isCatch = false;

	//测试包含非法字符
	str = "234b";
	try {
		Parser::parseU64(str, minNum, maxNum);
	} catch (NtseException &) {
		isCatch = true;
	}
	CPPUNIT_ASSERT(isCatch);
}

/**
 * 测试Parser::parseBracketsGroups
 */
void ParserTestCase::testParseBracketsGroups() {
	const char *trueColGrp[4] = {
		"a,b",
		"c, d",
		"e,	f",
		"g"
	};

	{
		string str = string("(") + trueColGrp[0] + "), (" + trueColGrp[1] + "),	("+ trueColGrp[2] + ") ,(" + trueColGrp[3] + ")";
		AutoPtr<Parser> parser(new Parser(str.c_str()));
		Array<char *> *colGrpArr = NULL;
		EXCPT_OPER(colGrpArr = Parser::parseBracketsGroups(str.c_str()));

		for (size_t i = 0; i < colGrpArr->getSize(); ++i) {
			CPPUNIT_ASSERT(!strcmp((*colGrpArr)[i], trueColGrp[i]));
			delete [] (*colGrpArr)[i];
		}
		delete colGrpArr;
	}
	{
		string invalidstr = string("(") + trueColGrp[0] + "),, (" + trueColGrp[1] + "),	("+ trueColGrp[2] + ") ,(" + trueColGrp[3] + ")";
		AutoPtr<Parser> parser(new Parser(invalidstr.c_str()));
		Array<char *> *colGrpArr = NULL;
		NEED_EXCPT(colGrpArr = Parser::parseBracketsGroups(invalidstr.c_str()));
		if (colGrpArr) {
			for (size_t i = 0; i < colGrpArr->getSize(); ++i) {
				delete [] (*colGrpArr)[i];
			}
			delete colGrpArr;
		}
	}
	{
		string invalidstr = string("(") + trueColGrp[0] + "),a (" + trueColGrp[1] + "),	("+ trueColGrp[2] + ") ,(" + trueColGrp[3] + ")";
		AutoPtr<Parser> parser(new Parser(invalidstr.c_str()));
		Array<char *> *colGrpArr = NULL;
		NEED_EXCPT(colGrpArr = Parser::parseBracketsGroups(invalidstr.c_str()));
		if (colGrpArr) {
			for (size_t i = 0; i < colGrpArr->getSize(); ++i) {
				delete [] (*colGrpArr)[i];
			}
			delete colGrpArr;
		}
	}
	{
		string invalidstr = string("(") + trueColGrp[0] + "), (" + trueColGrp[1] + "),	awfwaef,("+ trueColGrp[2] + ") ,(" + trueColGrp[3] + ")";
		AutoPtr<Parser> parser(new Parser(invalidstr.c_str()));
		Array<char *> *colGrpArr = NULL;
		NEED_EXCPT(colGrpArr = Parser::parseBracketsGroups(invalidstr.c_str()));
		if (colGrpArr) {
			for (size_t i = 0; i < colGrpArr->getSize(); ++i) {
				delete [] (*colGrpArr)[i];
			}
			delete colGrpArr;
		}
	}
	{
		string invalidstr = string("(") + trueColGrp[0] + "), ()";
		AutoPtr<Parser> parser(new Parser(invalidstr.c_str()));
		Array<char *> *colGrpArr = NULL;
		EXCPT_OPER(colGrpArr = Parser::parseBracketsGroups(invalidstr.c_str()));

		CPPUNIT_ASSERT(!strcmp((*colGrpArr)[0], trueColGrp[0]));
		delete [] (*colGrpArr)[0];
		CPPUNIT_ASSERT(!strcmp((*colGrpArr)[1], ""));
		delete [] (*colGrpArr)[1];

		delete colGrpArr;
	}
}

/** 
 * 测试Parser::isCommand.
 */
void ParserTestCase::testIsCommand() {
	CPPUNIT_ASSERT(Parser::isCommand("alter table set", "alter", "table", "set", NULL));
	CPPUNIT_ASSERT(!Parser::isCommand("alter table set", "alter", "table", "s", NULL));
}

/** 
 * 测试Parser::nextInt.
 */
void ParserTestCase::testNextInt() {
	const char *str = "+1234";
	AutoPtr<Parser> parser(new Parser(str));
	int num = 0;
	EXCPT_OPER(num = parser->nextInt());
	CPPUNIT_ASSERT(1234 == num);

	str = "-2147483648 hello";
	AutoPtr<Parser> parser2(new Parser(str));
	EXCPT_OPER(num = parser2->nextInt());
	EXCPT_OPER(parser2->match("hello"));

	str = "234b";
	AutoPtr<Parser> parser3(new Parser(str));
	bool isCatch = false;
	try {
		num = parser3->nextInt();
	} catch (NtseException &) {
		isCatch = true;
	}
	CPPUNIT_ASSERT(isCatch);
	isCatch = false;

	str = "-12";
	AutoPtr<Parser> parser4(new Parser(str));
	isCatch = false;
	try {
		num = parser4->nextInt(-123, -13);
	} catch (NtseException &) {
		isCatch = true;
	}
	CPPUNIT_ASSERT(isCatch);
	//test no next (token)int
	try {
		num = parser4->nextInt();
	} catch (NtseException &) {
		isCatch = true;
	}
	CPPUNIT_ASSERT(isCatch);

	str = "-abc";
	AutoPtr<Parser> parser5(new Parser(str));
	isCatch = false;
	try {
		num = parser5->nextInt();
	} catch (NtseException &) {
		isCatch = true;
	}
	CPPUNIT_ASSERT(isCatch);
}

/** 
 * 测试Parser::nextString.
 */
void ParserTestCase::testNextString() {
	//测试双引号及转义字符
	const char *str = "start \"this is \\n \\r \\t \\\\ \\\" \\\' a next string quote test.\"";
	AutoPtr<Parser> parser(new Parser(str));
	parser->match("start");
	const char *strVal = parser->nextString();
	CPPUNIT_ASSERT(!strcmp("this is \n \r \t \\ \" \' a next string quote test.", strVal));

	//测试单引号及转义字符
	str = "\'test single-quoted string:this is \\n \\r \\t \\\\ \\\" \\\'\'";
	AutoPtr<Parser> parser1(new Parser(str));
	strVal = parser1->nextString();
	CPPUNIT_ASSERT(!strcmp("test single-quoted string:this is \n \r \t \\ \" \'", strVal));

	//测试无引号及转义
	str = "this_is_\\n\\r\\t\\\\\\\"\\\'Escape_TEST_WITHOUT_QUOTATION_MARK.";
	AutoPtr<Parser> parser1_0(new Parser(str));
	strVal = parser1_0->nextString();
	CPPUNIT_ASSERT(!strcmp("this_is_\n\r\t\\\"\'Escape_TEST_WITHOUT_QUOTATION_MARK.", strVal));
	
	//测试nextString 转移字符不完整
	str = "test\\";	
	AutoPtr<Parser> parser2(new Parser(str));
	bool isCatch = false;
	try {
		strVal = parser2->nextString();
	} catch (NtseException &) {
		isCatch = true;
	}
	CPPUNIT_ASSERT(isCatch);
	//测试无结束double quotation mark。
	str = "\"test\'";
	AutoPtr<Parser> parser3(new Parser(str));
	isCatch = false;
	try {
		strVal = parser3->nextString();
	} catch (NtseException &) {
		isCatch = true;
	}
	CPPUNIT_ASSERT(isCatch);
	//测试无结束single quotation mark。
	str = "\'test\"";
	AutoPtr<Parser> parser4(new Parser(str));
	isCatch = false;
	try {
		strVal = parser4->nextString();
	} catch (NtseException &) {
		isCatch = true;
	}
	CPPUNIT_ASSERT(isCatch);
	isCatch = false;
	//测试无非法转移字符
	str = "\\k";
	AutoPtr<Parser> parser5(new Parser(str));
	isCatch = false;
	try {
		strVal = parser5->nextString();
	} catch (NtseException &) {
		isCatch = true;
	}
	CPPUNIT_ASSERT(isCatch);

}

/** 
 * 测试<code> Parser::remainingString</code>
 */
void ParserTestCase::testRemainingString() {
	const char *str = "Test remaining string.";
	AutoPtr<Parser> parser(new Parser(str));
	parser->match("Test");
	const char *strVal = parser->remainingString();
	CPPUNIT_ASSERT(!strcmp(strVal, "remaining string."));
	parser->match("remaining");
	parser->match("string");
	parser->match(".");
	parser->checkNoRemain();
	try {
		strVal = parser->remainingString();
	} catch (NtseException &) {
		return;
	}
	CPPUNIT_FAIL("No remaining string exception.");
}

/** 
* 测试<code> Parser::testTrimWhiteSpace</code>
*/
void ParserTestCase::testTrimWhiteSpace() {
	const char *str = "Test trim	wh\ri\nte space";//包含各种空白字符
	const char *newStr = Parser::trimWhitespace(str);
	CPPUNIT_ASSERT(!System::stricmp(newStr, "Testtrimwhitespace"));
	delete []newStr;

	const char *str2 = "idon\'thavewhitespace,hello,who'reyou";
	const char *newStr2 = Parser::trimWhitespace(str2);
	CPPUNIT_ASSERT(!System::stricmp(newStr2, str2));
	delete []newStr2;
}