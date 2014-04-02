/**
* ²âÊÔË«Êý×éTrieÊ÷
*
* @author ÀîÎ°îÈ(liweizhao@corp.netease.com)
*/

#ifndef _NTSETEST_SDAT_H_
#define _NTSETEST_SDAT_H_

#define _UNIT_TEST_MODE_

//#ifdef _UNIT_TEST_MODE_
//#define protected public
//#endif

#include <cppunit/extensions/HelperMacros.h>
#include "compress/dastrie.h"
#include "compress/RowCompress.h"

using namespace ntse;

/*
typedef byte* KeyType;
typedef byte* ValueType;
typedef u8 DicItemLenType;
typedef dastrie::builder<KeyType, ValueType, DicItemLenType> BuilderType;
typedef dastrie::trie<ValueType, DicItemLenType> TrieType;
typedef BuilderType::record_type DicItemType;
*/

class TrieTestCase: public CPPUNIT_NS::TestFixture {
	CPPUNIT_TEST_SUITE(TrieTestCase);
	CPPUNIT_TEST(testTraits);
	CPPUNIT_TEST(testBuild);
	CPPUNIT_TEST(testSearchDicItem);
	CPPUNIT_TEST(testReadWrite);
	CPPUNIT_TEST_SUITE_END();

public:
	static const char* getName();
	static const char* getDescription();
	static bool isBig(){
		return false;
	}
	void setUp();
	void tearDown();

protected:
	void testTraits();
	void testBuild();
	void testSearchDicItem();
	void testReadWrite();

private:
	void init();
	void cleanUp();

private:
	size_t m_dicSize;
	RCDictionary::DicItemType *m_dictionary;
	RCDictionary::BuilderType *m_builder;
	RCDictionary::TrieType *m_trie;
};

//#undef _UNIT_TEST_MODE_
//#undef protected

#endif