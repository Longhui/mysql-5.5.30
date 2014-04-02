/**
* 测试采样Trie树
*
* @author 李伟钊(liweizhao@corp.netease.com)
*/
#include <iterator>
#include <cppunit/config/SourcePrefix.h>
#include "compress/RowCompressCfg.h"
#include "compress/TestSmplTrie.h"

using namespace ntse;
using namespace std;

const char* SmplTrieTestCase::getName() {
	return "Sample trie test";
}

const char* SmplTrieTestCase::getDescription() {
	return "Functional test for sample trie.";
}

void SmplTrieTestCase::setUp() {
}

void SmplTrieTestCase::tearDown() {
}

void SmplTrieTestCase::init(uint trieSize, uint batchDelSize) {
	mtx = new MemoryContext(1024, 4);
	m_capacity = 256;
	m_nodeCount = 0;
	m_dictionary = new DicItem*[m_capacity];
	for (uint i = 0; i < m_capacity; i++) {
		m_dictionary[i] = new DicItem();
		m_dictionary[i]->m_data = new byte[m_capacity];
		m_dictionary[i]->m_dicLen = i % 37 + 4;
		for (uint j = 0; j < m_dictionary[i]->m_dicLen; j++) {
			m_dictionary[i]->m_data[j] = (byte)i;
			m_nodeCount++;
		}		
	}

	if (trieSize == 0) {
		m_trie = new SmplTrie(mtx, m_capacity, RowCompressCfg::DEFAULT_DIC_ITEM_MIN_LEN, 
			RowCompressCfg::DEFAULT_DIC_ITEM_MAX_LEN, batchDelSize, 8);
	} else {
		m_trie = new SmplTrie(mtx, trieSize, RowCompressCfg::DEFAULT_DIC_ITEM_MIN_LEN, 
			RowCompressCfg::DEFAULT_DIC_ITEM_MAX_LEN, batchDelSize, 8);
	}
}

void SmplTrieTestCase::cleanUp() {
	mtx->reset();
	delete mtx;
	delete m_trie;
	m_trie = NULL;

	for (uint i = 0; i < m_capacity; i++) {
		delete m_dictionary[i];
	}
	delete [] m_dictionary;
	m_dictionary = NULL;
}
void SmplTrieTestCase::testAddItem() {
	init(4096);

	CPPUNIT_ASSERT(m_trie->getLeavesList()->size() == 0);
	for (uint addTimes = 1; addTimes <= 2; addTimes++) {
		for (uint i = 0; i < m_capacity; i++) {
			m_trie->addItem(m_dictionary[i]->m_data, m_dictionary[i]->m_dicLen);
			if (addTimes == 1)
				CPPUNIT_ASSERT(m_trie->getLeavesList()->size() == i + 1);
			else
				CPPUNIT_ASSERT(m_trie->getLeavesList()->size() == m_capacity);

			SmplTrieNode *root = m_trie->getRoot();
			SmplTrieNode *current = root;
			for (uint j = 0; j < m_dictionary[i]->m_dicLen; j++) {
				TrieNodeAddr nodeAddr = current->getChildren(m_dictionary[i]->m_data[j]);
				current = m_trie->getRealNodeAddr(nodeAddr);
				CPPUNIT_ASSERT(current != NULL);
				if (j != m_dictionary[i]->m_dicLen - 1) {
					CPPUNIT_ASSERT(!current->isLeaf());
					CPPUNIT_ASSERT(!current->isInLeafDList());
				} else {
					CPPUNIT_ASSERT(current->isLeaf());
					CPPUNIT_ASSERT(current->isInLeafDList());
				}
				CPPUNIT_ASSERT(current->getRfCounts() == addTimes);
			}
		}
	}

	cleanUp();
}
void SmplTrieTestCase::testCheckSize() {
	uint capacity = 256;
	uint batchDelSize = 16;

	init(capacity, batchDelSize);

	size_t size = 0;
	for (uint i = 0; i < m_capacity; i++) {
		m_trie->addItem(m_dictionary[i]->m_data, m_dictionary[i]->m_dicLen);
		size += m_dictionary[i]->m_dicLen;
		CPPUNIT_ASSERT(m_trie->size() == size);

		while(m_trie->size() > capacity * 8) {
			uint sizeBeforeDel = m_trie->size();
			uint leafSize = m_trie->getLeavesList()->size();
			m_trie->checkSize();

			if (sizeBeforeDel > capacity * 8) {
				size -= min(leafSize, batchDelSize);
			}
			CPPUNIT_ASSERT(m_trie->size() == size);
		}
	}

	cleanUp();
}
void SmplTrieTestCase::testExtractDictionary() {
	init(4096);
	for (uint i = 0; i < m_capacity; i++) {
		m_trie->addItem(m_dictionary[i]->m_data, m_dictionary[i]->m_dicLen);
	}
	std::vector<SmplTrieNode*> *arr = new std::vector<SmplTrieNode*>(m_nodeCount);
	size_t dicSize = m_trie->extractDictionary(arr);

	size_t cnt = 0;
	for (std::vector<SmplTrieNode*>::iterator it = arr->begin(); cnt < dicSize && it != arr->end(); it++) {
		CPPUNIT_ASSERT(*it != NULL);
		CPPUNIT_ASSERT((*it)->isDicNode());
		(*it)->setDicNode(false);
		cnt++;
	}
	CPPUNIT_ASSERT(dicSize == cnt);

	delete arr;
	arr = NULL;
	cleanUp();
}

void SmplTrieTestCase::testGetFullKey() {
	MemoryContext *mtx = new MemoryContext(1024, 4);
	SmplTrie *trie = new SmplTrie(mtx, 1024, 4, 40, 4096, 8);

	SmplTrieNode *root = trie->getRoot();
	byte child[2][40];

	for (uint i = 0; i < 40; i++) {
		child[0][i] = i;
		child[1][i] = 255 - i;
	}
	trie->addItem(child[0], 40);
	trie->addItem(child[1], 40);

	byte *buf = new byte[40];
	for (uint j = 0; j < 2; j++) {
		SmplTrieNode *cur = root;
		for (uint i = 0; i < 40; i++) {
			cur = trie->getRealNodeAddr(cur->getChildren(child[j][i]));
		}
		trie->getFullKey(cur, buf);
		CPPUNIT_ASSERT(0 == memcmp(buf, child[j], 40));
	}

	delete []buf;
	delete trie;
	mtx->reset();
	delete mtx;
}

/************************************************************************/
/* 采样Trie树节点                                                                     */
/************************************************************************/

const char* SmplTrieNodeTestCase::getName() {
	return "Sample trie node test";
}

const char* SmplTrieNodeTestCase::getDescription() {
	return "Functional test for sample trie node.";
}

void SmplTrieNodeTestCase::setUp() {
}

void SmplTrieNodeTestCase::tearDown() {
}

void SmplTrieNodeTestCase::init() {
	m_capacity = 255;
	m_nodePool = new ObjMemoryPool<SmplTrieNode>(m_capacity * 2);
	m_root = new (m_nodePool->getFree())SmplTrieNode();
	m_dList = NULL;
	m_maxHeap = NULL;
}

void SmplTrieNodeTestCase::cleanUp() {
	delete m_nodePool;
	m_nodePool = NULL;
}

void SmplTrieNodeTestCase::testCommonOper() {
	init();

	m_dList = new SmplTrieDList();

	CPPUNIT_ASSERT(!m_root->isDicNode());
	CPPUNIT_ASSERT(m_root->height() == 0);
	CPPUNIT_ASSERT(m_root->isLeaf());
	CPPUNIT_ASSERT(!m_root->isInLeafDList());
	CPPUNIT_ASSERT(m_root->childCounts() == 0);
	CPPUNIT_ASSERT(m_root->parent() == INVALID_NODE_ADDR);
	CPPUNIT_ASSERT(m_root->getRfCounts() == 0);

	for (uint i = 0; i < m_capacity; i++) {
		SmplTrieNode *child = m_root->addChildren(m_nodePool, (u8)i);
		CPPUNIT_ASSERT(m_nodePool->logic2Real(m_root->getChildren(i)) == child);
		CPPUNIT_ASSERT(child != NULL);
		CPPUNIT_ASSERT(m_root->childCounts() == i + 1);
		CPPUNIT_ASSERT(!m_root->isLeaf());
		CPPUNIT_ASSERT(!m_root->isInLeafDList());

		CPPUNIT_ASSERT(child->isLeaf());
		CPPUNIT_ASSERT(!child->isInLeafDList());
		m_dList->addToTail(child);
		CPPUNIT_ASSERT(child->isInLeafDList());
		m_dList->remove(child);
		CPPUNIT_ASSERT(!child->isInLeafDList());

		CPPUNIT_ASSERT(child->getKey() == i);
		CPPUNIT_ASSERT(child->height() == m_root->height() + 1);

		CPPUNIT_ASSERT(child->getRfCounts() == 0);
		child->increaseRfCounts();
		CPPUNIT_ASSERT(child->getRfCounts() == 1);

		CPPUNIT_ASSERT(!child->isDicNode());
		child->setDicNode(true);
		CPPUNIT_ASSERT(child->isDicNode());
		child->setDicNode(false);
		CPPUNIT_ASSERT(!child->isDicNode());
	}
	for (uint i = 0; i < m_capacity; i++) {
		SmplTrieNode *child = (SmplTrieNode *)m_nodePool->logic2Real(m_root->getChildren((u8)i));
		SmplTrieNode *parent = (SmplTrieNode *)m_nodePool->logic2Real(child->parent());
		parent->removeChildren(child->getKey());
		CPPUNIT_ASSERT(m_root->getChildren((u8)i) == INVALID_NODE_ADDR);
		CPPUNIT_ASSERT(m_root->childCounts() == m_capacity - 1 - i);
	}
	{
		CPPUNIT_ASSERT(m_root->childCounts() == 0);
		CPPUNIT_ASSERT(m_root->getChildren(0) == INVALID_NODE_ADDR);
	}

	delete m_dList;
	m_dList = NULL;

	cleanUp();
}

/*
void SmplTrieNodeTestCase::testReCulPrefixRf() {
	init();

	m_maxHeap = new SmplTrieMaxHeap(m_capacity);
	SmplTrieNode **nodes = new SmplTrieNode*[m_capacity];
	SmplTrieNode *current = m_root;
	for (uint i = 0; i < m_capacity; i++) {
		current = current->addChildren(i);
		m_maxHeap->add(current);
		nodes[i] = current;
		for (uint j = 0; j <= i; j++) {
			nodes[j]->increaseRfCounts();
		}
	}
	for (uint i = 0; i < m_capacity; i++) {
		CPPUNIT_ASSERT(nodes[i]->getRfCounts() == m_capacity - i);
	}
	for (s64 i = m_capacity - 1; i >= 0; i--) {
		//nodes[i]->remove();
		nodes[i]->reCalcPrefixRf(m_maxHeap);
		for (uint j = 0; j < i; j++) {
			CPPUNIT_ASSERT(nodes[j]->getRfCounts() == i - j);
		}
	}
	
	delete []nodes;
	nodes = NULL;
	delete m_maxHeap;
	m_maxHeap = NULL;

	cleanUp();
}
*/

void SmplTrieNodeTestCase::testCompareWeight() {
	init();

	SmplTrieNodeComparator comparator;

	SmplTrieNode *height3Node = m_root->addChildren(m_nodePool, 0);
	for (u8 i = 0; i < 2; i++) {
		height3Node = height3Node->addChildren(m_nodePool, 0);
	}

	SmplTrieNode *height4Node1 = height3Node->addChildren(m_nodePool, 0);
	height4Node1->increaseRfCounts();
	SmplTrieNode *height4Node2 = height3Node->addChildren(m_nodePool, 1);
	height4Node2->increaseRfCounts();

	SmplTrieNode *height5Node1 = height4Node1->addChildren(m_nodePool, 0);
	height5Node1->increaseRfCounts();

	//height1Node1 == height1Node2
	CPPUNIT_ASSERT(!comparator(height4Node1, height4Node2));
	
	height4Node2->increaseRfCounts();
	CPPUNIT_ASSERT(!comparator(height4Node2, height5Node1));
	CPPUNIT_ASSERT(height5Node1->height() == 5 && height4Node2->height() == 4);
	CPPUNIT_ASSERT(height5Node1->getRfCounts() == 1);
	CPPUNIT_ASSERT(height4Node2->getRfCounts() == 2);
	CPPUNIT_ASSERT(comparator(height5Node1, height4Node2));

	CPPUNIT_ASSERT(!comparator(height5Node1, height4Node1));
	CPPUNIT_ASSERT(comparator(height4Node1, height5Node1));

	cleanUp();
}

void SmplTrieNodeTestCase::testObjMemoryPool() {
	const uint capacity = 1024;
	ObjMemoryPool<SmplTrieNode> memPool(capacity);

	SmplTrieNode * nodes[capacity];
	for (uint i = 0;i < capacity; i++) {
		nodes[i] = new (memPool.getFree())SmplTrieNode();
		CPPUNIT_ASSERT((i + 1) == memPool.getSize());
		TrieNodeAddr addr = memPool.real2Logic(nodes[i]);
		CPPUNIT_ASSERT(nodes[i] == (SmplTrieNode *)memPool.logic2Real(addr));
	}
	CPPUNIT_ASSERT(NULL == memPool.getFree());
	for (uint i = 0; i < capacity; i++) {
		CPPUNIT_ASSERT((capacity - i) == memPool.getSize());
		nodes[i]->~SmplTrieNode();
		memPool.markFree(nodes[i]);
		CPPUNIT_ASSERT((capacity - i - 1) == memPool.getSize());
	}
}

/************************************************************************/
/*  双向链表                                                                     */
/************************************************************************/

const char* SmplTrieDListTestCase::getName() {
	return "Double linked list for sample trie test";
}

const char* SmplTrieDListTestCase::getDescription() {
	return "Functional test for double linked list of sample trie.";
}

void SmplTrieDListTestCase::setUp() {
}

void SmplTrieDListTestCase::tearDown() {
}

void SmplTrieDListTestCase::init() {
	m_dList = new SmplTrieDList();
	CPPUNIT_ASSERT(m_dList->size() == 0);

	m_capacity = 256;
	m_nodes = new SmplTrieNode*[m_capacity];
	SmplTrieNodeFactory::createLeafNodes(m_capacity, m_nodes);

	for (size_t i = 0; i < m_capacity; i++) {
		m_dList->addToTail(m_nodes[i]);
		CPPUNIT_ASSERT(m_dList->size() == (i + 1));
	}
}

void SmplTrieDListTestCase::cleanUp() {
	for (uint i = 0; i < m_capacity; i++) {
		delete m_nodes[i];
		m_nodes[i] = NULL;
	}
	delete []m_nodes;
	m_nodes = NULL;

	delete m_dList;
	m_dList = NULL;
}

void SmplTrieDListTestCase::testDList() {
	init();

	//test
	SmplTrieNode** arr = new SmplTrieNode*[m_capacity];
	CPPUNIT_ASSERT(m_capacity == m_dList->toArray(arr));
	size_t index = 0;
	for (size_t i = 0; i < m_capacity; i++) {
		CPPUNIT_ASSERT(m_nodes[index]->getKey() == arr[i]->getKey());
		CPPUNIT_ASSERT(m_nodes[index]->height() == arr[i]->height());
		CPPUNIT_ASSERT(m_nodes[index]->getRfCounts() == arr[i]->getRfCounts());
		CPPUNIT_ASSERT(m_nodes[index]->parent() == arr[i]->parent());
		CPPUNIT_ASSERT(m_nodes[index]->isDicNode() == arr[i]->isDicNode());		
		index++;
	}
	delete []arr;
	arr = NULL;

	for (size_t i = 0; i < m_capacity; i++) {
		m_dList->removeTail();
		CPPUNIT_ASSERT(m_dList->size() == (m_capacity - i - 1));
	}

	cleanUp();
}

/************************************************************************/
/*  最大堆                                                                    */
/************************************************************************/
const char* SmplTrieMaxHeapTestCase::getName() {
	return "Max heap for sample trie test";
}

const char* SmplTrieMaxHeapTestCase::getDescription() {
	return "Functional test for max heap of sample trie.";
}

void SmplTrieMaxHeapTestCase::setUp() {
}

void SmplTrieMaxHeapTestCase::tearDown() {
}

void SmplTrieMaxHeapTestCase::init() {
	m_capacity = 256;
	m_maxHeap = new SmplTrieNodeHeap(m_capacity);

	m_nodePool = new ObjMemoryPool<SmplTrieNode>(m_capacity);
	m_nodes = new SmplTrieNode*[m_capacity];
	memset(m_nodes, 0, sizeof(SmplTrieNode*) * m_capacity);
	SmplTrieNodeFactory::createNodes(m_nodePool, m_capacity, m_nodes);
	for (size_t i = 1; i < m_capacity; i++) {
		for (size_t j = i; j > 0; j--)
			m_nodes[i]->increaseRfCounts();
	}
}

void SmplTrieMaxHeapTestCase::cleanUp() {
	delete []m_nodes;
	m_nodes = NULL;

	delete m_maxHeap;
	m_maxHeap = NULL;

	delete m_nodePool;
}

void SmplTrieMaxHeapTestCase::testMaxHeap() {
	init();

	//顺序插入堆
	uint middle = m_capacity >> 1;
	for (uint i = 0; i < middle; i++) {
		m_maxHeap->add(m_nodes[i]);
	}
	//倒序插入堆
	for (uint i = m_capacity - 1; i >= middle; i--) {
		m_maxHeap->add(m_nodes[i]);
	}

	SmplTrieNode *top = m_maxHeap->peek();
	CPPUNIT_ASSERT(m_maxHeap->peek() == top);
	SmplTrieNode *cur = m_maxHeap->poll();
	CPPUNIT_ASSERT(cur == top);
	SmplTrieNode *last = cur;
	SmplTrieNodeComparator compare;
	for (uint i = 1; i < m_capacity; i++) {
		cur = m_maxHeap->poll();
		CPPUNIT_ASSERT(!compare(last, cur));
		last = cur;
	}

	cleanUp();
}