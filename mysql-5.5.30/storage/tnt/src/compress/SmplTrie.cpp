/**
* 采样Trie树
*
* @author 李伟钊(liweizhao@corp.netease.com, liweizhao@163.org)
*/
#include <algorithm>
#include <vector>
#include <stack>
#include <queue>
#include <iterator>
#include "compress/SmplTrie.h"
#include "util/Portable.h"
#include "util/Queue.h"

using namespace std;

namespace ntse {

	/************************************************************************/
	/* 采样Trie树节点相关                                                                     */
	/************************************************************************/
	/**
	* 构造一个Trie树节点
	* @param key 本节点对应的字节
	* @param height 高度
	* @param parent 父节点相对地址
	*/
	SmplTrieNode::SmplTrieNode(byte key /*=0*/, u8 height /*=0*/, const TrieNodeAddr& parent /*=INVALID_NODE_ADDR*/):
		m_key(key), m_height(height), m_rfCounts(0), m_childCounts(0), m_parent(parent), m_isDicNode(false), 
		m_prev(NULL), m_next(NULL), m_heapPosition(0) {
			for (uint i = 0; i < MAX_TRIE_NODE; i++)
				m_childEntry[i] = INVALID_NODE_ADDR;
	}

	SmplTrieNode::~SmplTrieNode() {
	}

	/************************************************************************/
	/* 采样Trie树相关                                                                     */
	/************************************************************************/

	/**
	* 构造一颗Trie树
	* @param mtx      内存分配上下文
	* @param capacity 字典容量
	* @param minLen   字典项最小长度
	* @param maxLen   字典项最大长度
	* @param delSize  批删除大小
	* @param cte      膨胀系数
	*/
	SmplTrie::SmplTrie(MemoryContext *mtx, size_t capacity, u16 minLen, u16 maxLen, 
		size_t delSize, u8 cte) throw (NtseException) :
		m_mtx(mtx), m_capacity(capacity), m_size(0), m_delSize(delSize), 
		m_cte(cte), m_minLen(minLen), m_maxLen(maxLen) {
			size_t poolSize = capacity * m_cte + 256;
			m_nodePool = new ObjMemoryPool<SmplTrieNode>(poolSize);
			m_leavesList = new SmplTrieDList();
			m_root = new (m_nodePool->getFree())SmplTrieNode();
			m_leafNodeMinHeap = new SmplTrieNodeHeap(m_cte * m_capacity, false);
	}

	SmplTrie::~SmplTrie() {
		m_root->~SmplTrieNode();
		m_nodePool->markFree(m_root);
		delete m_leavesList;
		delete m_nodePool;
		delete m_leafNodeMinHeap;
	}

	/**
	* 删除一个Trie树叶子节点
	* @param nodeToRemove 要删除的叶子节点
	*/
	void SmplTrie::removeLeafNode(SmplTrieNode *nodeToRemove) {
		assert(nodeToRemove->height() > 0);
		assert(nodeToRemove->parent() != INVALID_NODE_ADDR);
		assert(nodeToRemove->childCounts() == 0);
		SmplTrieNode *parent = getRealNodeAddr(nodeToRemove->parent());
		parent->removeChildren(nodeToRemove->getKey());//移除父节点到本节点的指针
		nodeToRemove->~SmplTrieNode();
		m_nodePool->markFree(nodeToRemove);
		m_size--;
	}

	/**
	* 在Trie树中添加一个字典项
	* @param dicItemData 字典项数据起始地址
	* @param len 字典项长度
	*/
	void SmplTrie::addItem(const byte * dicItemData, const u16 & len) {
		assert(m_root != NULL);
		assert(len >= m_minLen && len <= m_maxLen);

		SmplTrieNode *current = m_root;
		const byte *addNodeKey = dicItemData;
		TrieNodeAddr childAddr = INVALID_NODE_ADDR;
		bool foundOldLeaf = false;
		for (uint i = 0; i < len; i++) {
			if ((childAddr = current->getChildren(*addNodeKey)) == INVALID_NODE_ADDR) {///子节点不存在
				if (!foundOldLeaf && //已经找到要从链表删除的叶子节点
					current->isLeaf() && //当前节点是叶子节点
					current->isInLeafDList()) {//并且当前节点在叶子节点链表中
						m_leavesList->remove(current);
						foundOldLeaf = true;
				}
				current = current->addChildren(m_nodePool, *addNodeKey);
				m_size++;
			} else {
				current = getRealNodeAddr(childAddr);
			}
			assert(current);
			current->increaseRfCounts();
			++addNodeKey;
		}
		//添加新的叶子节点到链表尾
		if (current->isLeaf() && !current->isInLeafDList()) {
			assert(current != m_root);
			m_leavesList->addToTail(current);
		}
	}

	/**
	 * 获得一个节点的所有前缀关键字
	 * @param node    要获取完整前缀的节点
	 * @param values  OUT 存储完整前缀key，有调用方分配足够内存
	 * @return        返回完整前缀长度
	 */
	u8 SmplTrie::getFullKey(SmplTrieNode *node, byte* values) {
		assert(values != NULL);
		stack<byte> stk;
		for (SmplTrieNode *current = node; current->parent() != INVALID_NODE_ADDR; 
			current = getRealNodeAddr(current->parent())) {
				stk.push(current->m_key);
		}
		u8 cnt = 0;
		byte *cursor = values;
		while (!stk.empty()) {
			(*cursor++) = stk.top();
			stk.pop();
			cnt++;
		}
		return cnt;
	}


	/**
	* 删除一批叶子节点
	* @param delSize 批删除大小
	*/
	size_t SmplTrie::batchDelLeaves(size_t delSize) {
		m_leafNodeMinHeap->clear();
		SmplTrieNode *leafListHead = m_leavesList->getHeader();
		for (SmplTrieNode *it = leafListHead->next(); it != leafListHead; it = it->next()) {
			m_leafNodeMinHeap->add(it);
		}
		size_t realDelSize = min((size_t)m_leafNodeMinHeap->size(), delSize);
		for (size_t i = 0; i < realDelSize; i++) {
			SmplTrieNode *current = m_leafNodeMinHeap->poll();
			assert(current);
			assert(current != m_root);
			SmplTrieNode* parent = getRealNodeAddr(current->parent());
			m_leavesList->remove(current);
			removeLeafNode(current);
			if (parent->isLeaf() && !parent->isInLeafDList() && parent->height() > 0) {
				m_leavesList->addToTail(parent);
				m_leafNodeMinHeap->add(parent);
			}
		}
		return realDelSize;
	}

	/**
	* 提取为字典项的节点
	* @param nodeList OUT 输出参数，保存为字典项的节点，调用方保证有足够内存
	* @return 提取到的实际字典项数
	*/
	size_t SmplTrie::extractDictionary(vector<SmplTrieNode*>* nodeList) {
		vector<SmplTrieNode*> *allTrieNodes = new vector<SmplTrieNode*>((size_t)m_size);
		size_t getSize = travelTrieTree(allTrieNodes);
		if (unlikely(getSize != m_size)) {
			delete allTrieNodes;
			allTrieNodes = NULL;
			NTSE_ASSERT(false);
		}

		SmplTrieNodeHeap *maxHeap = new SmplTrieNodeHeap(m_size);
		for (vector<SmplTrieNode*>::iterator it = allTrieNodes->begin();
			it != allTrieNodes->end(); it++) {
				assert(*it != NULL);
				u8 height = (*it)->height();
				if (height >= m_minLen && height <= m_maxLen)
					maxHeap->add((*it));
		}

		size_t buildSize = 0;
		for (size_t i = 0; i < m_size && buildSize < m_capacity; i++) {
			SmplTrieNode* top = maxHeap->poll();
			if (!top) 
				break;
			assert(top->height() >= m_minLen && top->height() <= m_maxLen);
			assert(!top->isDicNode());
			if (getRealNodeAddr(top->parent())->isDicNode() 
				&& top->height() > (m_minLen + 1)) {
					continue;
			}
			top->setDicNode(true);
			(*nodeList)[buildSize++] = top;
			reCalcPrefixRf(top, maxHeap);//重新计算所有前缀节点的引用计数并调整最大堆
		}
		delete maxHeap;
		maxHeap = NULL;
		delete allTrieNodes;
		allTrieNodes = NULL;

		return buildSize;
	}

	/**
	* 层序遍历一棵Trie树
	* @nodeList OUT 输出参数，用于保存遍历结果，调用方保证有足够空间
	* @return Trie树的节点数
	*/
	size_t SmplTrie::travelTrieTree(vector<SmplTrieNode*>* nodeList) {
		assert(m_root != NULL);

		Queue<TrieNodeAddr> nodeQueue(m_size);
		nodeQueue.offer((TrieNodeAddr)m_nodePool->real2Logic(m_root));

		TrieNodeAddr childBuffer[SmplTrieNode::MAX_TRIE_NODE];
		size_t buildSize = 0;
		TrieNodeAddr node = INVALID_NODE_ADDR;
		while (nodeQueue.size() > 0) {
			node = nodeQueue.peek();
			assert(node != INVALID_NODE_ADDR);
			u16 childCnts = getRealNodeAddr(node)->childCounts();
			size_t cc = getAllChild(node, childBuffer);
			UNREFERENCED_PARAMETER(cc);
			assert(cc == childCnts);
			for (uint i = 0; i < childCnts; i++) {
				nodeQueue.offer(childBuffer[i]);
				(*nodeList)[buildSize++] = getRealNodeAddr(childBuffer[i]);
			}
			nodeQueue.poll();
		}
		return buildSize;
	}

	/**
	* 获得指定节点的所有子节点
	* @param nodeAddr   要获取子节点的节点的相对地址
	* @param values     OUT 保存结果，调用方保证足够空间
	* @return           子节点数目
	*/
	size_t SmplTrie::getAllChild(TrieNodeAddr nodeAddr, TrieNodeAddr* values) const {
		assert(values != NULL);
		SmplTrieNode *node = getRealNodeAddr(nodeAddr);
		if (node->childCounts() == 0) {
			return 0;
		} else {
			TrieNodeAddr* cursor = values;
			uint count = 0;
			for (uint i = 0; i < SmplTrieNode::MAX_TRIE_NODE && count < node->childCounts(); i++) {
				TrieNodeAddr childAddr = node->getChildren((byte)i);
				if (INVALID_NODE_ADDR != childAddr) {
					*cursor = childAddr;
					count++;
					cursor++;
				}
			}
			assert(count == node->childCounts());
			return count;
		}
	}

	/**
	* 重新计算指定节点的所有前缀节点的引用计数
	* @param node    指定节点
	* @param maxHeap 要调整的最大堆
	*/
	void SmplTrie::reCalcPrefixRf(SmplTrieNode *node, SmplTrieNodeHeap *maxHeap) {
		SmplTrieNode* current = getRealNodeAddr(node->parent());
		while (current->parent() != INVALID_NODE_ADDR) {
			if (current->m_rfCounts < node->m_rfCounts ||  current->height() < m_minLen)
				break;
			current->m_rfCounts -= node->m_rfCounts;
			maxHeap->fixDown(current->m_heapPosition);
			current = getRealNodeAddr(current->parent());
		}
	}
}