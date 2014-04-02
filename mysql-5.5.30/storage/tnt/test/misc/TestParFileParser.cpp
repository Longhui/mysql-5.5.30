/**
 * ���Է�����PAR�ļ���������
 * 
 * @author wanggongpu(wanggongpu@corp.netease.com)
 */
#include "misc/TestParFileParser.h"
#include "misc/ParFileParser.h"
#include "Test.h"

using namespace std;
using namespace ntse;

/** 
 * ��ȡ����������
 *
 * @return partition parser ����������
 */
const char* ParFileParserTestCase::getName() {
	return "Partition Parser tests";
}

/** 
 * ��ȡ������������
 *
 * @return partition parser������������
 */
const char* ParFileParserTestCase::getDescription() {
	return "Test Partition Parser functions.";
}

/** 
 * ��ȡ���������Ƿ���Ͳ���
 *
 * @return false ��ʾС�͵�Ԫ����
 */
bool ParFileParserTestCase::isBig() {
	return false;
}

/** 
 * ����ͨ�������������Ľ�����֤�Ƿ�Ϊ������
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
	
	// #p#���Ƿ�����Ƿ�����Сд����
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
 * ����ͨ�����߼���·�����Ƿ����.par�ļ�����֤�Ƿ�Ϊ������
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
 * ����ͨ�������������Ľ�����֤�Ƿ�Ϊ������
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
 * ����ͨ�������������Ľ�����֤�Ƿ�Ϊ������
 *
 */
void ParFileParserTestCase::testParseParFile() {
	const char *parfilePath = NULL;
	std::vector<std::string> partitionNames;
	ParFileParser *parser = NULL;

	// �޷������.par�ļ�����ԭ��·��ԭ������partitionNames
	parfilePath = "a";	
	parser = new ParFileParser(parfilePath);
	parser->parseParFile(partitionNames);
	CPPUNIT_ASSERT((partitionNames.size() == 1) && (!strcmp(partitionNames[0].c_str(), "a")));
	delete parser;

	// �޷������.par�ļ�����ԭ��·��ȥ��������Ƿ����ӷ������ƺ����partitionNames
	// ��Ϊ�������ʱͨ��``���Ž�����#P#�����ı�ʱ��MYSQL�ڲ���#��P������ת�崦��
	// ���Գ��ǹ���ɾ��.par�ļ������򲢲����ڱ�·���к�������Ƿ�����������.par�ļ�������
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

	//����һ���򵥵�par�ļ�������
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