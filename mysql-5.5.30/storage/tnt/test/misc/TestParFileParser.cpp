/**
 * 测试分区表PAR文件解析操作
 * 
 * @author wanggongpu(wanggongpu@corp.netease.com)
 */
#include "misc/TestParFileParser.h"
#include "misc/ParFileParser.h"
#include "Test.h"

using namespace std;
using namespace ntse;

/** 
 * 获取测试用例名
 *
 * @return partition parser 测试用例名
 */
const char* ParFileParserTestCase::getName() {
	return "Partition Parser tests";
}

/** 
 * 获取测试用例描述
 *
 * @return partition parser测试用例描述
 */
const char* ParFileParserTestCase::getDescription() {
	return "Test Partition Parser functions.";
}

/** 
 * 获取测试用例是否大型测试
 *
 * @return false 表示小型单元测试
 */
bool ParFileParserTestCase::isBig() {
	return false;
}

/** 
 * 测试通过对物理表表名的解析验证是否为分区表
 *
 */
void ParFileParserTestCase::testCheckPartitionByPhyTabName() {
	const char *str = NULL;
	bool ret = false;

	str = "t1";
	ret = ParFileParser::isPartitionByPhyicTabName(str);
	CPPUNIT_ASSERT(!ret);
	
	str = "t1#p0";
	ret = ParFileParser::isPartitionByPhyicTabName(str);
	CPPUNIT_ASSERT(!ret);
	
	// #p#不是分区标记符，大小写敏感
	str = "t1#p#p0";
	ret = ParFileParser::isPartitionByPhyicTabName(str);
	CPPUNIT_ASSERT(!ret);

	str = "t1#P#p0";
	ret = ParFileParser::isPartitionByPhyicTabName(str);
	CPPUNIT_ASSERT(ret);
	
	str = "t1#P#p0#SUB#s0";
	ret = ParFileParser::isPartitionByPhyicTabName(str);
	CPPUNIT_ASSERT(ret);
}

/** 
 * 测试通过对逻辑表路径下是否存在.par文件，验证是否为分区表
 *
 */
void ParFileParserTestCase::testCheckPartitionByLogPath() {
	const char *logicTabPath = "t";
	ParFileParser parser(logicTabPath);
	CPPUNIT_ASSERT(!parser.isPartitionByLogicTabPath());

	File *f = new File("t.par");
	bool exist = false;
	f->isExist(&exist);
	CPPUNIT_ASSERT(!exist);
	CPPUNIT_ASSERT(!strcmp(f->getPath(), "t.par"));

	u64 code = 0;
	code = f->create(true, false);
	CPPUNIT_ASSERT(code == File::E_NO_ERROR);
	f->isExist(&exist);	
	CPPUNIT_ASSERT(exist);
	CPPUNIT_ASSERT(parser.isPartitionByLogicTabPath());

	f->close();
	code = f->remove();
	CPPUNIT_ASSERT(code == File::E_NO_ERROR);
	f->isExist(&exist);
	CPPUNIT_ASSERT(!exist);
	delete f;
}

/** 
 * 测试通过对物理表表名的解析验证是否为分区表
 *
 */
void ParFileParserTestCase::testParseLogicTabName() {
	const char *phyTabName = NULL;
	char logicTabName[Limits::MAX_NAME_LEN+1] = {0};
	bool ret = false;

	phyTabName = "t";
	ret = ParFileParser::parseLogicTabName(phyTabName, logicTabName, Limits::MAX_NAME_LEN + 1);
	CPPUNIT_ASSERT(!ret);

	phyTabName = "t#P#p0";
	ret = ParFileParser::parseLogicTabName(phyTabName, logicTabName, Limits::MAX_NAME_LEN + 1);
	CPPUNIT_ASSERT(ret);
	CPPUNIT_ASSERT(!strcmp(logicTabName, "t"));

	phyTabName = "t#P#p0#SUB#s0";
	ret = ParFileParser::parseLogicTabName(phyTabName, logicTabName, Limits::MAX_NAME_LEN + 1);
	CPPUNIT_ASSERT(ret);
	CPPUNIT_ASSERT(!strcmp(logicTabName, "t"));
}

/** 
 * 测试通过对物理表表名的解析验证是否为分区表
 *
 */
void ParFileParserTestCase::testParseParFile() {
	const char *parfilePath = NULL;
	std::vector<std::string> partitionNames;
	ParFileParser *parser = NULL;

	// 无分区表的.par文件，将原有路径原样存入partitionNames
	parfilePath = "a";	
	parser = new ParFileParser(parfilePath);
	parser->parseParFile(partitionNames);
	CPPUNIT_ASSERT((partitionNames.size() == 1) && (!strcmp(partitionNames[0].c_str(), "a")));
	delete parser;

	// 无分区表的.par文件，将原有路径去除分区标记符和子分区名称后存入partitionNames
	// 因为如果建表时通过``符号建含有#P#表名的表时，MYSQL内部对#和P都做了转义处理
	// 所以除非故意删除.par文件，否则并不存在表路径中含分区标记符，而不存在.par文件的情形
	parfilePath = "a#P#p0";	
	parser = new ParFileParser(parfilePath);
	partitionNames.clear();
	parser->parseParFile(partitionNames);
	CPPUNIT_ASSERT((partitionNames.size() == 1) && (!strcmp(partitionNames[0].c_str(), "a")));
	delete  parser;
	
	parfilePath = "a#P#p0#SUB#s0";	
	parser = new ParFileParser(parfilePath);
	partitionNames.clear();
	parser->parseParFile(partitionNames);
	CPPUNIT_ASSERT((partitionNames.size() == 1) && (!strcmp(partitionNames[0].c_str(), "a")));
	delete  parser;

	File *f = new File("a.par");
	bool exist = false;
	f->isExist(&exist);
	CPPUNIT_ASSERT(!exist);
	CPPUNIT_ASSERT(!strcmp(f->getPath(), "a.par"));
	u64 code = 0;
	code = f->create(false, false);
	CPPUNIT_ASSERT(code == File::E_NO_ERROR);
	f->isExist(&exist);	
	CPPUNIT_ASSERT(exist);

	//构造一个简单的par文件供解析
	char parFileContent[32] = {0};
	const char *p = parFileContent;
	*(long *)p = 0x8;
	*(long *)(p + 8) = 0x3;
	*(long *)(p + 16) = 0x9;
	memcpy((char *)(p + 20), "p0", 2);
	memcpy((char *)(p + 23), "p1", 2);
	memcpy((char *)(p + 26), "p2", 2);
	f->setSize(32);
	f->write(0, 32, parFileContent);
	f->close();

	parfilePath = "a";	
	parser = new ParFileParser(parfilePath);
	partitionNames.clear();
	parser->parseParFile(partitionNames);
	CPPUNIT_ASSERT(partitionNames.size() == 3);
	CPPUNIT_ASSERT(!strcmp(partitionNames[0].c_str(), "a#P#p0"));
	CPPUNIT_ASSERT(!strcmp(partitionNames[1].c_str(), "a#P#p1"));
	CPPUNIT_ASSERT(!strcmp(partitionNames[2].c_str(), "a#P#p2"));
	delete parser;

	code = f->remove();
	CPPUNIT_ASSERT(code == File::E_NO_ERROR);
	f->isExist(&exist);
	CPPUNIT_ASSERT(!exist);
	delete f;
}