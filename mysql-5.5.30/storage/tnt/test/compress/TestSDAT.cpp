/**
* ²âÊÔË«Êý×éTrieÊ÷
*
* @author ÀîÎ°îÈ(liweizhao@corp.netease.com)
*/

#include <algorithm>
#include <limits.h>
#include "util/Portable.h"
#include "compress/TestSDAT.h"
#include "compress/RowCompress.h"

using namespace std;

const char* TrieTestCase::getName() {
	return "Static double array trie test";
}

const char* TrieTestCase::getDescription() {
	return "Functional test for static double array trie.";
}

void TrieTestCase::setUp() {
}

void TrieTestCase::tearDown() {
}

void TrieTestCase::init() {
	m_dicSize = 1024;
	m_dictionary = new RCDictionary::DicItemType[m_dicSize];

	for (size_t i = 0; i < m_dicSize; i++) {
		m_dictionary[i].keyLen = (i + 3) % 40 + 1;
		m_dictionary[i].key = new byte[m_dictionary[i].keyLen];
		for (RCDictionary::DicItemLenType j = 0; j < m_dictionary[i].keyLen; j++) {
			m_dictionary[i].key[j] = (u8)((i + j) % 256);
		}
		m_dictionary[i].valueLen = m_dictionary[i].keyLen;
		m_dictionary[i].value = m_dictionary[i].key;
	}

	RCDictionary::DicItemType *tmp = new RCDictionary::DicItemType[m_dicSize];
	for (size_t i = 0; i < m_dicSize; i++)
		tmp[i] = m_dictionary[i];
	sort(&tmp[0], &tmp[m_dicSize], RCDictionary::compareDicItem);

	m_builder = new RCDictionary::BuilderType();
	try {
		m_builder->build(tmp, tmp + m_dicSize);
	} catch (exception &e) {
		UNREFERENCED_PARAMETER(e);
		CPPUNIT_ASSERT(false);
	}
	delete []tmp;
	tmp = NULL;
}

void TrieTestCase::testTraits() {
	doublearray5_traits traits;
	UNREFERENCED_PARAMETER(traits);
	s32 bases[] = { -(1 << 16), (1 << 16) - 1 };
	u16 checks[] = { 0, (1 << 16) - 1 };
	
	for (uint i = 0; i < 2; i++) {
		doublearray5_traits::element_type elem;
		traits.set_base(elem, bases[i]);
		traits.set_check(elem, checks[i]);
		CPPUNIT_ASSERT(traits.get_base(elem) == bases[i]);
		CPPUNIT_ASSERT(traits.get_check(elem) == checks[i]);
	}
}

void TrieTestCase::cleanUp() {
	for (size_t i = 0; i < m_dicSize; i++) {
		delete [] m_dictionary[i].key;
		m_dictionary[i].key = NULL;
		m_dictionary[i].value = NULL;
	}
	delete [] m_dictionary;
	m_dictionary = NULL;

	delete m_builder;
	m_builder = NULL;
}

void TrieTestCase::testBuild() {
	init();
	CPPUNIT_ASSERT(m_builder->getLeavesStat() == m_dicSize);	
	cleanUp();
}

void TrieTestCase::testSearchDicItem() {
	init();
	
	byte *serialData;
	uint serialDataLen;
	m_builder->write(&serialData, &serialDataLen);

	m_trie = new RCDictionary::TrieType();
	m_trie->read(serialData, 0, serialDataLen);

	for (size_t i = 0; i < m_dicSize; i++) {
		MatchResult result;
		CPPUNIT_ASSERT(m_trie->searchDicItem(m_dictionary[i].key, m_dictionary[i].key 
			+ m_dictionary[i].keyLen, &result));
		CPPUNIT_ASSERT(result.matchLen == m_dictionary[i].keyLen);
		CPPUNIT_ASSERT(result.valueLen == m_dictionary[i].valueLen);
		CPPUNIT_ASSERT(0 == memcmp(result.value, m_dictionary[i].value, m_dictionary[i].valueLen));
	}
	delete []serialData;
	serialData = NULL;

	delete m_trie;
	m_trie = NULL;
	cleanUp();
}

void TrieTestCase::testReadWrite() {
	init();
	byte *serialData;
	uint serialDataLen;
	m_builder->write(&serialData, &serialDataLen);

	m_trie = new RCDictionary::TrieType();
	m_trie->read(serialData, 0, serialDataLen);

	byte *trieData;
	uint trieDataLen;
	m_trie->getTrieData(&trieData, &trieDataLen);
	CPPUNIT_ASSERT(trieDataLen == serialDataLen);
	CPPUNIT_ASSERT(0 == memcmp(trieData, serialData, trieDataLen));
	delete []serialData;
	serialData = NULL;

	delete m_trie;
	m_trie = NULL;
	cleanUp();
}