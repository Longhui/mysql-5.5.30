/**
* 测试记录压缩
*
* @author 李伟钊(liweizhao@corp.netease.com, liweizhao@163.org)
*/

#include "compress/TestRowCompress.h"
#include "compress/RowCompress.h"
#include "misc/Global.h"
#include "misc/TableDef.h"
#include "misc/Record.h"
#include "Test.h"

using namespace ntse;

/************************************************************************/
/* 全局字典                                                                     */
/************************************************************************/
const char* RCDictionaryTestCase::getName() {
	return "Global dictionary test";
}

const char* RCDictionaryTestCase::getDescription() {
	return "Functional test for global dictionary of row compression model.";
}

void RCDictionaryTestCase::setUp() {
}

void RCDictionaryTestCase::tearDown() {
}

void RCDictionaryTestCase::init(uint capacity, uint dicItemCnt) {
	m_tableId = (u16)System::random();
	m_capacity = capacity;
	m_dicItemCnt = dicItemCnt;
	m_dictionary = new RCDictionary(m_tableId, NULL, m_capacity);

	m_dicDatas = new DicItem*[m_dicItemCnt];
	for (uint i = 0; i < m_dicItemCnt; i++) {
		m_dicDatas[i] = new DicItem();
		m_dicDatas[i]->m_dicLen = i % 37 + 4;
		m_dicDatas[i]->m_data = new byte[m_dicDatas[i]->m_dicLen];
		for (uint j = 0; j < m_dicDatas[i]->m_dicLen; j++) {
			m_dicDatas[i]->m_data[j] = i;
		}
	}
}

void RCDictionaryTestCase::cleanUp() {
	if (m_dictionary != NULL) {
		m_dictionary->close();
		delete m_dictionary;
		m_dictionary = NULL;
	}

	for (uint i = 0; i < m_dicItemCnt; i++) {
		delete m_dicDatas[i];
	}
	delete [] m_dicDatas;
	m_dicDatas = NULL;
}

void RCDictionaryTestCase::testCommonOpers() {
	init();

	//1、test get/set dictionary item
	CPPUNIT_ASSERT(m_dictionary->capacity() == m_capacity);
	for (uint i = 0; i < m_capacity; i++) {
		m_dictionary->setDicItem(i, m_dicDatas[i]->m_data, m_dicDatas[i]->m_dicLen);
	}

	for (uint i = 0; i < m_capacity; i++) {
		const byte *dicData;
		size_t dicItemLen = m_dictionary->getDicItem(i, &dicData);
		CPPUNIT_ASSERT(dicItemLen == m_dicDatas[i]->m_dicLen);
		CPPUNIT_ASSERT(0 == memcmp(dicData, m_dicDatas[i]->m_data, dicItemLen));
	}

	CPPUNIT_ASSERT(m_tableId == m_dictionary->getTblId());

	cleanUp();

	//2、test codeIndex Border
	init(CODED_TWO_BYTE_MAXNUM + 1);
	uint testCases = 4;
	u8 byteLens[] = { ONE_BYTE, TWO_BYTE};
	u8 flags[] = { ONE_BYTE_FLAG, TWO_BYTE_FLAG};
	byte *out = NULL;
	uint des[] = {0, 0, 0, 0};
	uint src[] = { 0, CODED_ONE_BYTE_MAXNUM, 
		CODED_ONE_BYTE_MAXNUM + 1, CODED_TWO_BYTE_MAXNUM
	};

	for (uint i = 0; i < testCases; i++) {
		m_dictionary->setDicItem(src[i], m_dicDatas[i]->m_data, m_dicDatas[i]->m_dicLen);
		const byte *dicData;
		size_t dicItemLen = m_dictionary->getDicItem(src[i], &dicData);
		CPPUNIT_ASSERT(dicItemLen == m_dicDatas[i]->m_dicLen);
		CPPUNIT_ASSERT(0 == memcmp(dicData, m_dicDatas[i]->m_data, dicItemLen));
	}	
	cleanUp();

	//3、test default constructor
	CPPUNIT_ASSERT(m_dictionary == NULL);
	m_dictionary = new RCDictionary();
	CPPUNIT_ASSERT(m_dictionary->getTblId() == 0);
	CPPUNIT_ASSERT(m_dictionary->size() == 0);
	CPPUNIT_ASSERT(m_dictionary->capacity() == 0);
	m_dictionary->close();
	delete m_dictionary;
	m_dictionary = NULL;
}

void RCDictionaryTestCase::testBuildDatAndFindKey() {
	init();

	for (uint i = 0; i < m_capacity; i++) {
		CPPUNIT_ASSERT(m_dicDatas[i]->m_data != NULL);
		CPPUNIT_ASSERT(m_dicDatas[i]->m_dicLen > 0);
		m_dictionary->setDicItem(i, m_dicDatas[i]->m_data, m_dicDatas[i]->m_dicLen);
	}
	m_dictionary->buildDat();
	for (uint i = 0; i < m_capacity; i++) {
		MatchResult result;
		bool rtn = m_dictionary->findKey(m_dicDatas[i]->m_data, 0, 
			m_dicDatas[i]->m_data + m_dicDatas[i]->m_dicLen, &result);
		CPPUNIT_ASSERT(rtn);
		CPPUNIT_ASSERT(result.matchLen == m_dicDatas[i]->m_dicLen);
		//CodedBytes len = (i >= 64 ? TWO_BYTE : ONE_BYTE);
		uint index = 0;
		RCMIntegerConverter::decodeBytesToInt(result.value, 0, (CodedBytes)result.valueLen, &index);
		CPPUNIT_ASSERT(i == index);
	}
	cleanUp();
}

void RCDictionaryTestCase::testCreateOpenDropClose() {
	//open not exist dictionary
	m_dictionary = NULL;
	bool caughtExcpt = false;
	try {
		m_dictionary = RCDictionary::open("./nonExistDic.ndic");
	} catch (NtseException &e) {
		caughtExcpt = CHECK_FILE_ERR(e.getErrorCode());
	}
	CPPUNIT_ASSERT(caughtExcpt);

	//open invalid dictionary of size 0
	const char * invlidFilePath = "./invalidDic.ndic";
	File invlidFile(invlidFilePath);
	CPPUNIT_ASSERT(File::E_NO_ERROR == File::getNtseError(invlidFile.create(false, false)));
	CPPUNIT_ASSERT(File::E_NO_ERROR == File::getNtseError(invlidFile.close()));
	caughtExcpt = false;
	try {
		m_dictionary = RCDictionary::open(invlidFilePath);
	} catch (NtseException &e) {
		caughtExcpt = CHECK_FILE_ERR(e.getErrorCode());
	}
	invlidFile.remove();
	CPPUNIT_ASSERT(caughtExcpt);

	//open invalid dictionary of size not 0
	CPPUNIT_ASSERT(File::E_NO_ERROR == File::getNtseError(invlidFile.create(false, false)));
	byte nullFileContent[1];
	CPPUNIT_ASSERT(File::E_NO_ERROR == File::getNtseError(invlidFile.setSize(sizeof(nullFileContent))));
	memset(nullFileContent, 0, sizeof(nullFileContent));
	CPPUNIT_ASSERT(File::E_NO_ERROR == File::getNtseError(invlidFile.write(0, sizeof(nullFileContent), nullFileContent)));
	CPPUNIT_ASSERT(File::E_NO_ERROR == File::getNtseError(invlidFile.close()));
	caughtExcpt = false;
	try {
		m_dictionary = RCDictionary::open(invlidFilePath);
	} catch (NtseException &e) {
		caughtExcpt = CHECK_FILE_ERR(e.getErrorCode());
	}
	invlidFile.remove();
	CPPUNIT_ASSERT(caughtExcpt);
	CPPUNIT_ASSERT(m_dictionary == NULL);

	//create valid dictionary
	init();

	for (uint i = 0; i < m_capacity; i++) {
		m_dictionary->setDicItem(i, m_dicDatas[i]->m_data, m_dicDatas[i]->m_dicLen);
	}
	EXCPT_OPER(m_dictionary->buildDat());
	
	string filePath = string("./testGlobalDictionary") + Limits::NAME_GLBL_DIC_EXT;
	TableDef *tableDef = new TableDef();
	tableDef->m_id = m_tableId;
	tableDef->m_name = System::strdup("testGlobalDictionary");

	bool createSuc = false;
	try {
		//create failed
		{
			File testFile(filePath.c_str());
			testFile.remove();
			CPPUNIT_ASSERT(File::E_NO_ERROR == File::getNtseError(testFile.create(false, true)));
			NEED_EXCPT(RCDictionary::create(filePath.c_str(), m_dictionary));
			testFile.close();
		}
		CPPUNIT_ASSERT(!File::isExist(filePath.c_str()));
		//create successful
		EXCPT_OPER(RCDictionary::create(filePath.c_str(), m_dictionary));
		createSuc = true;	
		m_dictionary->close();
		delete m_dictionary;
		m_dictionary = NULL;

		//open valid dictionary
		RCDictionary * rebuildDic = NULL;
		EXCPT_OPER(rebuildDic = RCDictionary::open(filePath.c_str()));

		CPPUNIT_ASSERT(rebuildDic != NULL);

		//copy dictionary
		u16 newTblId = (u16)System::random();;
		RCDictionary *copyDic = RCDictionary::copy(rebuildDic, newTblId);
		CPPUNIT_ASSERT(copyDic);
		CPPUNIT_ASSERT(copyDic->getTblId() == newTblId);
		CPPUNIT_ASSERT(copyDic->size() == rebuildDic->size());
		CPPUNIT_ASSERT(copyDic->capacity() == rebuildDic->capacity());
		for (uint i = 0; i < copyDic->size(); i++) {
			const byte * itemData1;
			uint itemLen1 = copyDic->getDicItem(i, &itemData1);
			const byte * itemData2;
			uint itemLen2 = rebuildDic->getDicItem(i, &itemData2);
			CPPUNIT_ASSERT(itemLen1 == itemLen2);
			CPPUNIT_ASSERT(0 == memcmp(itemData1, itemData2, itemLen2));
		}
		byte* trieSerialData1 = NULL;
		uint trieSerialDataLen1;
		((dastrie::trie<RCDictionary::ValueType, RCDictionary::DicItemLenType, doublearray5_traits> *)copyDic->getCompressionTrie())->getTrieData(&trieSerialData1, &trieSerialDataLen1);
		byte* trieSerialData2 = NULL;
		uint trieSerialDataLen2;
		((dastrie::trie<RCDictionary::ValueType, RCDictionary::DicItemLenType, doublearray5_traits> *)rebuildDic->getCompressionTrie())->getTrieData(&trieSerialData2, &trieSerialDataLen2);
		CPPUNIT_ASSERT(trieSerialDataLen1 == trieSerialDataLen2);
		CPPUNIT_ASSERT(0 == memcmp(trieSerialData1, trieSerialData2, trieSerialDataLen2));

		copyDic->close();
		delete copyDic;
		copyDic = NULL;

		//set table id
		newTblId = (u16)System::random();
		rebuildDic->setTableId(newTblId);
		CPPUNIT_ASSERT(rebuildDic->getTblId() == newTblId);

		rebuildDic->close();
		delete rebuildDic;
		rebuildDic = NULL;
	} catch (NtseException &e) {
		if (createSuc) {
			m_dictionary->close();
			RCDictionary::drop(filePath.c_str());
			delete tableDef;
		}
		cout << e.getMessage() << endl;
		CPPUNIT_ASSERT(false);
	}

	delete tableDef;

	EXCPT_OPER(RCDictionary::drop(filePath.c_str()));
	File checkFile(filePath.c_str());
	bool isFileExist;
	checkFile.isExist(&isFileExist);
	CPPUNIT_ASSERT(!isFileExist);

	cleanUp();
}

void RCDictionaryTestCase::testCompareDicItem() {
	RCDictionary::DicItemType x;
	x.keyLen = 2;
	x.key = new byte[x.keyLen];
	x.key[0] = 0;
	x.key[1] = 1;

	RCDictionary::DicItemType y;
	y.keyLen = 1;
	y.key = new byte[y.keyLen];
	y.key[0] = 0;

	CPPUNIT_ASSERT(RCDictionary::compareDicItem(y, x));
	y.key[0] = 1;
	CPPUNIT_ASSERT(RCDictionary::compareDicItem(x, y));
	delete []y.key;
	y.keyLen = 2;
	y.key = new byte[y.keyLen];
	y.key[0] = 0;
	y.key[1] = 1;
	CPPUNIT_ASSERT(!RCDictionary::compareDicItem(x, y));

	delete []x.key;
	delete []y.key;
}

/************************************************************************/
/*  记录压缩模块                                                                  */
/************************************************************************/

const char* RowCompressTestCase::getName() {
	return "Row compression test";
}

const char* RowCompressTestCase::getDescription() {
	return "Functional test for row compression.";
}

void RowCompressTestCase::setUp() {
}

void RowCompressTestCase::tearDown() {
}

void RowCompressTestCase::init(u64 tableSize, bool onlyOneColGrp, bool largeRecord) {
	Database::drop(".");
	EXCPT_OPER(m_db = Database::open(&m_cfg, true));

	m_cprsCfg = new RowCompressCfg();

	//create test compress table
	m_cprsTblBuilder = new CompressTableBuilder(m_db, onlyOneColGrp, largeRecord);
	m_tableDef = m_cprsTblBuilder->getTableDef();	

	if (tableSize > 0)
		m_cprsTblBuilder->createTable(tableSize);
	m_records = m_cprsTblBuilder->getRecords();
}

void RowCompressTestCase::cleanUp() {
	if (m_records != NULL)
		m_cprsTblBuilder->dropTable();
	delete m_cprsTblBuilder;
	m_cprsTblBuilder = NULL;

	delete m_cprsCfg;
	m_cprsCfg = NULL;

	m_db->close();
	delete m_db;
	Database::drop(".");
	m_db = NULL;
}

void RowCompressTestCase::testCodeAndDecode() {
	CodedBytes byteLens[] = { ONE_BYTE, TWO_BYTE };
	CodedFlag flags[] = { ONE_BYTE_FLAG, TWO_BYTE_FLAG };
	byte *out = NULL;
	uint src[] = { 0, CODED_ONE_BYTE_MAXNUM, 
		CODED_ONE_BYTE_MAXNUM + 1, CODED_TWO_BYTE_MAXNUM,
	};
	uint des[] = {0, 0, 0, 0};
	uint testCases = sizeof(src) / sizeof(uint);
	
	for (u8 i = 0; i < testCases; i++) {
		out = new byte[byteLens[i >> 1]];
		RCMIntegerConverter::codeIntToCodedBytes(src[i], flags[i >> 1], out);
		RCMIntegerConverter::decodeBytesToInt(out, 0, byteLens[i >> 1], &des[i]);
		NTSE_ASSERT(des[i] == src[i]);
		delete []out;
	}
}

void RowCompressTestCase::testRowCompressCfg() {
	uint dicSize = 1 << 22;
	u8 dicItemMinLen = 4;
	u8 dicItemMaxLen = 40;
	u8 cprsThresholdPct = 80;

	RowCompressCfg *config = new RowCompressCfg(dicSize, dicItemMinLen, dicItemMaxLen, cprsThresholdPct);
	CPPUNIT_ASSERT(config->dicSize() == dicSize);
	CPPUNIT_ASSERT(config->dicItemMinLen() == dicItemMinLen);
	CPPUNIT_ASSERT(config->dicItemMaxLen() == dicItemMaxLen);
	CPPUNIT_ASSERT(config->compressThreshold() == cprsThresholdPct);

	u32 serialLen = 0;
	byte *serialData = NULL;
	RowCompressCfg *rebuildCfg = NULL;
	try {
		config->writeToKV(&serialData, &serialLen);
	
		rebuildCfg = new RowCompressCfg();
		rebuildCfg->readFromKV(serialData, serialLen);
		CPPUNIT_ASSERT(*config == *rebuildCfg);
		delete rebuildCfg;
		rebuildCfg = NULL;

		delete []serialData;
		serialData = NULL;

	} catch(NtseException &e) {
		if (serialData != NULL)
			delete []serialData;
			serialData = NULL;
		if (rebuildCfg != NULL) {
			delete rebuildCfg;
			rebuildCfg = NULL;
		}
		cout << e.getMessage() << endl;
		CPPUNIT_ASSERT(false);
	}	

	RowCompressCfg *copy = new RowCompressCfg(*config);
	CPPUNIT_ASSERT(*copy == *config);

	copy->setDicSize(1);
	CPPUNIT_ASSERT(!(*copy == *config));
	copy->setDicSize(dicSize);

	copy->setDicItemMinLen(1);
	CPPUNIT_ASSERT(!(*copy == *config));
	copy->setDicItemMinLen(dicItemMinLen);

	copy->setDicItemMaxLen(1);
	CPPUNIT_ASSERT(!(*copy == *config));
	copy->setDicItemMaxLen(dicItemMaxLen);

	copy->setCompressThreshold(10);
	CPPUNIT_ASSERT(!(*copy == *config));
	copy->setCompressThreshold(cprsThresholdPct);

	delete copy;
	copy = NULL;
	delete config;
	config = NULL;
}

void RowCompressTestCase::testValidateCompressCfg() {
	uint invalidSize = (1 << 16) + 1;
	uint validSize = 1 << 16;
	uint invalidMinLen = 10;
	uint validMinLen = 3;
	uint invalidMaxLen1 = 7;
	uint invalidMaxLen2 = 257;
	uint validMaxLen = 40;
	uint invalidThreshold = 101;
	uint validThreshold = 80;
	CPPUNIT_ASSERT(!RowCompressCfg::validateDictSize(invalidSize));
	CPPUNIT_ASSERT(RowCompressCfg::validateDictSize(validSize));
	CPPUNIT_ASSERT(!RowCompressCfg::validateDictMinLen(invalidMinLen));
	CPPUNIT_ASSERT(RowCompressCfg::validateDictMinLen(validMinLen));
	CPPUNIT_ASSERT(!RowCompressCfg::validateDictMaxLen(invalidMaxLen1));
	CPPUNIT_ASSERT(!RowCompressCfg::validateDictMaxLen(invalidMaxLen2));
	CPPUNIT_ASSERT(RowCompressCfg::validateDictMaxLen(validMaxLen));
	CPPUNIT_ASSERT(!RowCompressCfg::validateCompressThreshold(invalidThreshold));
	CPPUNIT_ASSERT(RowCompressCfg::validateCompressThreshold(validThreshold));
}

void RowCompressTestCase::testCprsAndDcprd() {
	vs.compress = true;

	u64 tableSize = 10000;

	{
		//1、只有一个属性组
		std::cout << std::endl <<  "*******************************************" << std::endl;
		std::cout << "* one column group                        *" << std::endl;
		std::cout << "*******************************************" << std::endl;

		init(tableSize);

		RCDictionary *dictionary = NULL;
		//alloc session
		Connection *conn = m_db->getConnection(false);
		Session *session = m_db->getSessionManager()->allocSession(
			"RowCompressTestCase::testCprsAndDcprd", conn);
		conn->getLocalConfig()->m_tblSmplPct = 80;

		RCMSampleTbl *sampleTableHdl = new RCMSampleTbl(session, m_db, m_tableDef, m_records);
		SmpTblErrCode errCode = sampleTableHdl->beginSampleTbl();
		sampleTableHdl->endSampleTbl();
		EXCPT_OPER(dictionary = sampleTableHdl->createDictionary(session));
		CPPUNIT_ASSERT(dictionary != NULL);
		delete sampleTableHdl;
		sampleTableHdl = NULL;
		
		m_rowCompressMng = new RowCompressMng(m_db, m_tableDef, dictionary);
		CPPUNIT_ASSERT(m_rowCompressMng != NULL);
		CPPUNIT_ASSERT(m_rowCompressMng->getDictionary() != NULL);

		for (u64 i = 0; i < tableSize; i++) {
			u64 sp = session->getMemoryContext()->setSavepoint();
			Record *rcd = m_cprsTblBuilder->createRecord(i, REC_REDUNDANT);

			uint size = 0;
			byte *b = NULL;
			size_t *b2 = NULL;
			size = m_tableDef->m_maxRecSize;
			b = (byte *)session->getMemoryContext()->alloc(size);
			b2 = (size_t *)session->getMemoryContext()->alloc(m_tableDef->m_numColGrps * sizeof(size_t));
			CompressOrderRecord uncprsRcd(i, b, size, m_tableDef->m_numColGrps, b2);
			//从redundant格式转化为非真实压缩格式
			RecordOper::convRecordRedToCO(m_tableDef, rcd, &uncprsRcd);

			size = 2 * uncprsRcd.m_size;
			b = (byte *)session->getMemoryContext()->alloc(size);
			Record compressRcd(i, REC_COMPRESSED, b, size);

			m_rowCompressMng->compressRecord(&uncprsRcd, &compressRcd);

			CompressOrderRecord validateRcd;
			validateRcd.m_size = uncprsRcd.m_size;
			validateRcd.m_data = (byte *)session->getMemoryContext()->alloc(validateRcd.m_size);

			m_rowCompressMng->decompressRecord(&compressRcd, &validateRcd);
			CPPUNIT_ASSERT(validateRcd.m_rowId == uncprsRcd.m_rowId);
			CPPUNIT_ASSERT(validateRcd.m_size = uncprsRcd.m_size);
			CPPUNIT_ASSERT(0 == memcmp(validateRcd.m_data, uncprsRcd.m_data, validateRcd.m_size));

			u64 fastSize = m_rowCompressMng->calcRcdDecompressSize(&compressRcd);
			CPPUNIT_ASSERT(uncprsRcd.m_size == fastSize);

			freeRecord(rcd);
			session->getMemoryContext()->resetToSavepoint(sp);
		}

		//free session
		m_db->getSessionManager()->freeSession(session);
		m_db->freeConnection(conn);

		dictionary->close();
		delete dictionary;
		dictionary = NULL;

		delete m_rowCompressMng;
		m_rowCompressMng = NULL;
		cleanUp();
	}
	{
		//2、2个属性组
		std::cout << std::endl << "*******************************************" << std::endl;
		std::cout << "* two column group                        *" << std::endl;
		std::cout << "*******************************************" << std::endl;
		init(tableSize, false);

		RCDictionary *dictionary = NULL;
		//alloc session
		Connection *conn = m_db->getConnection(false);
		Session *session = m_db->getSessionManager()->allocSession(
			"RowCompressTestCase::testCprsAndDcprd", conn);
		conn->getLocalConfig()->m_tblSmplPct = 80;

		RCMSampleTbl *sampleTableHdl = new RCMSampleTbl(session, m_db, m_tableDef, m_records);
		SmpTblErrCode errCode = sampleTableHdl->beginSampleTbl();
		sampleTableHdl->endSampleTbl();
		EXCPT_OPER(dictionary = sampleTableHdl->createDictionary(session));
		CPPUNIT_ASSERT(dictionary != NULL);
		delete sampleTableHdl;
		sampleTableHdl = NULL;

		m_rowCompressMng = new RowCompressMng(m_db, m_tableDef, dictionary);
		CPPUNIT_ASSERT(m_rowCompressMng != NULL);
		CPPUNIT_ASSERT(m_rowCompressMng->getDictionary() != NULL);

		for (u64 i = 0; i < tableSize; i++) {
			u64 sp = session->getMemoryContext()->setSavepoint();
			Record *rcd = m_cprsTblBuilder->createRecord(i, REC_REDUNDANT);

			CompressOrderRecord uncprsRcd;
			uncprsRcd.m_rowId = i;
			uncprsRcd.m_numSeg = m_tableDef->m_numColGrps;
			uncprsRcd.m_segSizes = (size_t *)session->getMemoryContext()->alloc(uncprsRcd.m_numSeg * sizeof(size_t));
			uncprsRcd.m_size = m_tableDef->m_maxRecSize;
			uncprsRcd.m_data = (byte *)session->getMemoryContext()->alloc(uncprsRcd.m_size);
			//从redundant格式转化为非真实压缩格式
			RecordOper::convRecordRedToCO(m_tableDef, rcd, &uncprsRcd);

			Record compressRcd;
			compressRcd.m_format = REC_COMPRESSED;
			compressRcd.m_size = 2 * uncprsRcd.m_size;
			compressRcd.m_data = (byte *)session->getMemoryContext()->alloc(compressRcd.m_size);

			m_rowCompressMng->compressRecord(&uncprsRcd, &compressRcd);

			CompressOrderRecord validateRcd;
			validateRcd.m_size = uncprsRcd.m_size;
			validateRcd.m_data = (byte *)session->getMemoryContext()->alloc(validateRcd.m_size);

			m_rowCompressMng->decompressRecord(&compressRcd, &validateRcd);
			CPPUNIT_ASSERT(validateRcd.m_rowId == uncprsRcd.m_rowId);
			CPPUNIT_ASSERT(validateRcd.m_size = uncprsRcd.m_size);
			CPPUNIT_ASSERT(0 == memcmp(validateRcd.m_data, uncprsRcd.m_data, validateRcd.m_size));

			u64 fastSize = m_rowCompressMng->calcRcdDecompressSize(&compressRcd);
			CPPUNIT_ASSERT(uncprsRcd.m_size == fastSize);

			freeRecord(rcd);
			session->getMemoryContext()->resetToSavepoint(sp);
		}

		//free session
		m_db->getSessionManager()->freeSession(session);
		m_db->freeConnection(conn);

		dictionary->close();
		delete dictionary;
		dictionary = NULL;

		delete m_rowCompressMng;
		m_rowCompressMng = NULL;
		cleanUp();
	}
	
	vs.compress = false;
}

void RowCompressTestCase::testCprsSegAndDcprsSeg() {
	
	init(10000);

	RCDictionary *dictionary = NULL;
	//alloc session
	Connection *conn = m_db->getConnection(false);
	Session *session = m_db->getSessionManager()->allocSession(
		"RowCompressTestCase::testCprsAndDcprd", conn);
	conn->getLocalConfig()->m_tblSmplPct = 80;

	RCMSampleTbl *sampleTableHdl = new RCMSampleTbl(session, m_db, m_tableDef, m_records);
	SmpTblErrCode errCode = sampleTableHdl->beginSampleTbl();
	sampleTableHdl->endSampleTbl();
	EXCPT_OPER(dictionary = sampleTableHdl->createDictionary(session));
	CPPUNIT_ASSERT(dictionary != NULL);
	delete sampleTableHdl;
	sampleTableHdl = NULL;

	m_rowCompressMng = new RowCompressMng(m_db, m_tableDef, dictionary);

	uint srcLen = 102400;
	byte *src = new byte[srcLen];
	for (uint i = 0; i < srcLen; i++) {
		src[i] = (byte)System::random() & 255;
	}
	byte *dest = new byte[srcLen * 2];
	uint destLen = 0;
	m_rowCompressMng->compressColGroup(src, 0, srcLen, dest, &destLen);
	
	byte *decprs = new byte[srcLen];
	uint decprsLen = 0;
	m_rowCompressMng->decompressColGroup(dest, 0, destLen, decprs, &decprsLen);
	CPPUNIT_ASSERT(srcLen == decprsLen);
	CPPUNIT_ASSERT(0 == memcmp(src, decprs, srcLen));

	delete [] src;
	delete []dest;
	delete []decprs;

	dictionary->close();
	delete dictionary;
	dictionary = NULL;

	delete m_rowCompressMng;
	m_rowCompressMng = NULL;
	cleanUp();
}

void RowCompressTestCase::testCreateDropOpenClose() {
	init();

	//open not exists file
	bool caughtExcpt = false;
	try {
		m_rowCompressMng = RowCompressMng::open(m_db, m_tableDef, "nonExistDicFile");
	} catch (NtseException &e) {
		CPPUNIT_ASSERT(e.getErrorCode() == NTSE_EC_NO_DICTIONARY);
	}

	//open invalid exists file
	const char *invalidFilePath = "./invalidGlobalDic";
	File invalidFile((string(invalidFilePath) + Limits::NAME_GLBL_DIC_EXT).c_str());
	CPPUNIT_ASSERT(File::getNtseError(invalidFile.create(false, false)) == File::E_NO_ERROR);
	CPPUNIT_ASSERT(File::getNtseError(invalidFile.close() == File::E_NO_ERROR));
	caughtExcpt = false;
	try {
		m_rowCompressMng = RowCompressMng::open(m_db, m_tableDef, invalidFilePath);
	} catch (NtseException &e) {
		caughtExcpt = CHECK_FILE_ERR(e.getErrorCode());
	}
	CPPUNIT_ASSERT(caughtExcpt);
	CPPUNIT_ASSERT(File::getNtseError(invalidFile.remove()) == File::E_NO_ERROR);
	
	//test create
	RCDictionary * testDic = new RCDictionary(m_tableDef->m_id, NULL);
	string path("./testGlobalDictionary");
	for (uint i = 0; i < 10; i++) {
		u8 s = i + 4;
		byte *tmp = new byte[s];
		for (uint j = 0; j < s; j++) {
			tmp[j] = j;
		}
		testDic->setDicItem(i, tmp, s);
		delete []tmp;
	}
	EXCPT_OPER(testDic->buildDat());

	string fullPath = path + ".ndic";
	File dicFile(fullPath.c_str());

	//create failed
	CPPUNIT_ASSERT(File::E_NO_ERROR == File::getNtseError(dicFile.create(false, true)));
	NEED_EXCPT(RowCompressMng::create(fullPath.c_str(), testDic));
	dicFile.close();

	CPPUNIT_ASSERT(!File::isExist(fullPath.c_str()));
	EXCPT_OPER(RowCompressMng::create(fullPath.c_str(), testDic));
	testDic->close();
	delete testDic;
	testDic = NULL;

	//open valid file
	EXCPT_OPER(m_rowCompressMng = RowCompressMng::open(m_db, m_tableDef, path.c_str()));
	CPPUNIT_ASSERT(m_rowCompressMng != NULL);
	RCDictionary *validDic = m_rowCompressMng->getDictionary();
	CPPUNIT_ASSERT(validDic != NULL);
	CPPUNIT_ASSERT(validDic->getTblId() == m_tableDef->m_id);
	CPPUNIT_ASSERT(10 == validDic->size());

	//close
	m_rowCompressMng->close();
	CPPUNIT_ASSERT(m_rowCompressMng->getDictionary() == NULL);
	CPPUNIT_ASSERT(NULL == m_rowCompressMng->getDictionary());

	//drop
	EXCPT_OPER(RowCompressMng::drop((path + Limits::NAME_GLBL_DIC_EXT).c_str()));
	File checkFile((path + Limits::NAME_GLBL_DIC_EXT).c_str());
	bool isFileExist;
	checkFile.isExist(&isFileExist);
	CPPUNIT_ASSERT(!isFileExist);

	delete m_rowCompressMng;
	m_rowCompressMng = NULL;
	cleanUp();
}