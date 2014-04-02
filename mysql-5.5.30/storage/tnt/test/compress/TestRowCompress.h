/**
* ≤‚ ‘º«¬º—πÀı
*
* @author ¿ÓŒ∞Ó»(liweizhao@corp.netease.com, liweizhao@163.org)
*/

#ifndef _NTSETEST_ROWCOMPRESS_H_
#define _NTSETEST_ROWCOMPRESS_H_

#ifdef _DEBUG_DICTIONARY_
#undef _DEBUG_DICTIONARY_
#endif

#include <cppunit/extensions/HelperMacros.h>
#include "api/Database.h"
#include "compress/RowCompressCoder.h"
#include "compress/TestSmplTrie.h"
#include "compress/TestRCMSmplTbl.h"
#include "compress/RowCompress.h"

class DicEqualer {
public:
	static bool isDicEqual(const RCDictionary *x, const RCDictionary *y) {
		if (x->getTblId() != y->getTblId())
			return false;
		if (x->capacity() != y->capacity())
			return false;
		if (x->size() != y->size())
			return false;
		/*
		for (size_t i  = 0; i < x->size(); i++) {
		if (m_mapTbl[i].keyLen != another.m_mapTbl[i].keyLen)
		return false;
		if (0 != memcmp(m_mapTbl[i].key, another.m_mapTbl[i].key, m_mapTbl[i].keyLen))
		return false;
		if (m_mapTbl[i].valueLen != another.m_mapTbl[i].valueLen)
		return false;
		if (0 != memcmp(m_mapTbl[i].value, another.m_mapTbl[i].value, m_mapTbl[i].valueLen))
		return false;
		}
		*/
		if (x->getCompressionTrie() == NULL || x->getCompressionTrie() == NULL)
			return x->getCompressionTrie() == NULL && y->getCompressionTrie() == NULL;
		else if (*x->getCompressionTrie() != *x->getCompressionTrie())
			return false;
		return true;
	}
};


class RCDictionaryTestCase : public CPPUNIT_NS::TestFixture {
	CPPUNIT_TEST_SUITE(RCDictionaryTestCase);
	CPPUNIT_TEST(testCommonOpers);
	CPPUNIT_TEST(testBuildDatAndFindKey);
	CPPUNIT_TEST(testCreateOpenDropClose);
	CPPUNIT_TEST(testCompareDicItem);
	CPPUNIT_TEST_SUITE_END();

public:
	static const char* getName();
	static const char* getDescription();
	static bool isBig() {
		return false;
	}
	void setUp();
	void tearDown();

protected:
	void testCommonOpers();
	void testBuildDatAndFindKey();
	void testCreateOpenDropClose();
	void testCompareDicItem();

private:
	void init(uint capacity = 256, uint dicItemCnt = 256);
	void cleanUp();

private:
	u16 m_tableId;
	size_t m_capacity;
	size_t m_dicItemCnt;
	DicItem **m_dicDatas;
	RCDictionary *m_dictionary;
};

class RowCompressTestCase : public CPPUNIT_NS::TestFixture {
	CPPUNIT_TEST_SUITE(RowCompressTestCase);
	CPPUNIT_TEST(testCodeAndDecode);
	CPPUNIT_TEST(testRowCompressCfg);
	CPPUNIT_TEST(testValidateCompressCfg);
	CPPUNIT_TEST(testCprsAndDcprd);
	CPPUNIT_TEST(testCprsSegAndDcprsSeg);
	CPPUNIT_TEST(testCreateDropOpenClose);
	CPPUNIT_TEST_SUITE_END();

public:
	static const char* getName();
	static const char* getDescription();
	static bool isBig() {
		return false;
	}
	void setUp();
	void tearDown();

protected:
	void testCodeAndDecode();
	void testRowCompressCfg();
	void testValidateCompressCfg();
	void testCprsAndDcprd();
	void testCprsSegAndDcprsSeg();
	void testCreateDropOpenClose();

private:
	void init(u64 tableSize = 0, bool onlyOneColGrp = true, bool largeRecord = false);
	void cleanUp();

private:
	Config m_cfg;
	Database *m_db;
	const TableDef *m_tableDef;
	RowCompressCfg *m_cprsCfg;

	Records* m_records;
	RowCompressMng *m_rowCompressMng;
	CompressTableBuilder *m_cprsTblBuilder;
};

#endif
