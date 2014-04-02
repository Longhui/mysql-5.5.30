/**
* ≤‚ ‘≤…—˘Trie ˜
*
* @author ¿ÓŒ∞Ó»(liweizhao@corp.netease.com)
*/

#ifndef _NTSETEST_SMPLTRIE_H_
#define _NTSETEST_SMPLTRIE_H_

#include <cppunit/extensions/HelperMacros.h>
#include "compress/SmplTrie.h"
#include "misc/MemCtx.h"

using namespace ntse;

class SmplTrieNodeFactory {
public:
	static void createNodes(ObjMemoryPool<SmplTrieNode> *nodePool, size_t nodesNum, SmplTrieNode **nodes) {
		nodes[0] = new (nodePool->getFree())SmplTrieNode();
		CPPUNIT_ASSERT(NULL != nodes[0]);
		for (uint i = 1; i < nodesNum; i++) {
			nodes[i] = nodes[i - 1]->addChildren(nodePool, (i - 1)% 256);
			CPPUNIT_ASSERT(NULL != nodes[i]);
		}
	}

	static void createLeafNodes(size_t nodesNum, SmplTrieNode **nodes) {
		nodes[0] = new SmplTrieNode();//as root
		CPPUNIT_ASSERT(NULL != nodes[0]);
		for (uint i = 1; i < nodesNum; i++) {
			nodes[i] = new SmplTrieNode();
			CPPUNIT_ASSERT(NULL != nodes[i]);
		}
	}
};

class DicItem {
public:
	DicItem() : m_data(NULL), m_dicLen(0) {}
	DicItem(byte *data, u8 dicLen):m_data(data), m_dicLen(dicLen) {}
	~DicItem() {
		if (m_data) {
			delete [] m_data;
			m_data = NULL;
		}
	}
public:
	byte *m_data;
	u8 m_dicLen;
};

class SmplTrieTestCase: public CPPUNIT_NS::TestFixture {
	CPPUNIT_TEST_SUITE(SmplTrieTestCase);
	CPPUNIT_TEST(testAddItem);
	CPPUNIT_TEST(testExtractDictionary);
	CPPUNIT_TEST(testCheckSize);
	CPPUNIT_TEST(testGetFullKey);
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
	void testAddItem();
	void testCheckSize();
	void testExtractDictionary();
	void testGetFullKey();

private:
	void init(uint trieSize = 0, uint batchDelSize = 4096);
	void cleanUp();

private:
	MemoryContext *mtx;
	SmplTrie *m_trie;
	uint m_capacity;
	uint m_nodeCount;
	DicItem **m_dictionary;
};

class SmplTrieNodeTestCase: public CPPUNIT_NS::TestFixture {
	CPPUNIT_TEST_SUITE(SmplTrieNodeTestCase);
	CPPUNIT_TEST(testCommonOper);
	//CPPUNIT_TEST(testReCulPrefixRf);
	CPPUNIT_TEST(testCompareWeight);
	CPPUNIT_TEST(testObjMemoryPool);
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
	void testCommonOper(); 
	//void testReCulPrefixRf();
	void testCompareWeight();
	void testObjMemoryPool();

private:
	void init();
	void cleanUp();

private:
	ObjMemoryPool<SmplTrieNode> *m_nodePool;
	uint m_capacity;
	SmplTrieNode *m_root;
	SmplTrieDList *m_dList;
	SmplTrieNodeHeap *m_maxHeap;
};

class SmplTrieDListTestCase: public CPPUNIT_NS::TestFixture {
	CPPUNIT_TEST_SUITE(SmplTrieDListTestCase);
	CPPUNIT_TEST(testDList);
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
	void testDList();

private:
	void init();
	void cleanUp();
private:
	SmplTrieDList *m_dList;
	size_t m_capacity;
	SmplTrieNode **m_nodes;
};

class SmplTrieMaxHeapTestCase: public CPPUNIT_NS::TestFixture {
	CPPUNIT_TEST_SUITE(SmplTrieMaxHeapTestCase);
	CPPUNIT_TEST(testMaxHeap);
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
	void testMaxHeap();

private:
	void init();
	void cleanUp();
	
private:
	ObjMemoryPool<SmplTrieNode> *m_nodePool;
	size_t m_capacity;
	SmplTrieNodeHeap *m_maxHeap;
	SmplTrieNode **m_nodes;
};

#endif