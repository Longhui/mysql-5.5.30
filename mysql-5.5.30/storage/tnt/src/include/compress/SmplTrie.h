/**
* 采样Trie树
*
* @author 李伟钊(liweizhao@corp.netease.com, liweizhao@163.org)
*/
#ifndef _NTSETEST_SMPL_TRIE_H_
#define _NTSETEST_SMPL_TRIE_H_

#include <vector>
#include "misc/Global.h"
#include "util/Hash.h"
#include "misc/MemCtx.h"
#include <stack>

using namespace std;

namespace ntse {

	class SmplTrieDList;
	class SmplTrieNodeHeap;
	class SmplTrieNode;
	/**
	 * 对象内存池
	 *
	 * 由于trie树的生长和剪枝过程中，以及滑动窗口的哈希表在在采样过程中需要new非常多的小块内存，
	 * 极易造成内存碎片且难以被OS回收，所以采用对象内存池来分配一整块的内存，当需要生成对象的时候
	 * 从内存池中获得空闲的槽即可，释放的时候返回内存池。由于只用于一种对象类型，所以内存池槽的大小是
	 * 固定的，如果需要实现不定长的槽，会比较复杂
	 */
	template<typename T>
	class ObjMemoryPool {
	public:
		static const u32 POOL_PAGE_SIZE = 1 << 16;  /** 对象内存池页大小 */

		typedef u32 LogicalAddr;//头2个字节表示页号，后2个字节表示页内偏移量

		struct MemPoolPage {
			byte *m_data; //页面数据起始地址
		};

		struct ObjSlot {
			T           m_obj;        //对象
			LogicalAddr m_address;    //逻辑地址
			u8          m_isFree : 1; //是否可用
		} ;

	public:
		/**
		 * 构造一个对象内存池
		 * @param capacity 内存池最大槽数，总内存池大小为capacity * sizeof(T)
		 */
		ObjMemoryPool(u32 capacity) throw(NtseException) : m_capacity(capacity) {
			assert(sizeof(ObjSlot) <= POOL_PAGE_SIZE);
			m_maxPageNo = 0;
			m_freeSpaceInLastPage = -1;

			u16 numPerPage = POOL_PAGE_SIZE / sizeof(ObjSlot);
			m_pagesNum = (u16)(capacity / numPerPage + 1);
			m_pageArr = (MemPoolPage *)malloc(sizeof(MemPoolPage) * m_pagesNum);
			assert(m_pageArr);
			memset(m_pageArr, 0, sizeof(MemPoolPage) * m_pagesNum);
			
			m_stackCursor = 0;
			m_stack = (LogicalAddr *)malloc(capacity * sizeof(LogicalAddr));
			memset(m_stack, 0, capacity * sizeof(LogicalAddr));
			assert(m_stack);

			m_usedSize = 0;
		}
		/**
		 * 释放对象内存池
		 * 如果内存池中还有没有显式调用markFree释放的对象，在这里也会被释放
		 */
		~ObjMemoryPool() {
 			for (u32 i = 0; i < m_pagesNum; i++) {
 				free(m_pageArr[i].m_data);
 			}
 			free(m_pageArr);
			free(m_stack);
		}

		/**
		 * 分配一个内存池槽，槽大小为sizeof(T)
		 * @return 
		 */
		inline void* getFree() {
			if (unlikely(m_stackCursor == 0)) {
				if (m_usedSize == m_capacity)
					return NULL;
				else {
					allocNewSlot();
				}
			}
			LogicalAddr la = m_stack[--m_stackCursor];
			ObjSlot *slot = getSlot(la);
			assert(1 == slot->m_isFree);
			slot->m_isFree = 0;
			++m_usedSize;
			return (void *)slot;
		}
		/**
		 * 释放一个内存池槽
		 * 释放后该槽变为可用内存池槽并返回内存池
		 * @param obj 对象占用的内存池槽地址
		 */
		inline void markFree(T *t) {
			assert(m_usedSize > 0);
			if (unlikely(t == NULL))
				return;
			ObjSlot *slot = (ObjSlot *)t;
			assert(0 == slot->m_isFree);
			slot->m_isFree = 1;
			m_stack[m_stackCursor++] = slot->m_address;
			--m_usedSize;
		}
		/**
		 * 获得当前被使用中的内存池槽数
		 * @return
		 */
		inline u32 getSize() const {
			return m_usedSize;
		}
		
		/**
		* 转化逻辑地址为实际地址
		* @param i
		* @return
		*/
		inline void* logic2Real(LogicalAddr i) const {
			ObjSlot *slot = getSlot(i);
			return &slot->m_obj;
		}
		/**
		* 转化实际地址为逻辑地址
		* @pre obj必须在内存池的范围内
		* @param obj 
		* @return
		*/
		inline LogicalAddr real2Logic(T *t) const {
			return ((ObjSlot *)t)->m_address;
		}

	private:
		/**
		 * 分配一个新内存槽
		 */
		inline ObjSlot *allocNewSlot() {
			if (m_freeSpaceInLastPage < 0) {
				m_maxPageNo = 0;
				m_freeSpaceInLastPage = POOL_PAGE_SIZE;
				m_pageArr[m_maxPageNo].m_data = (byte *)malloc(POOL_PAGE_SIZE);
			} else if (m_freeSpaceInLastPage < (s32)sizeof(ObjSlot)) {
				m_freeSpaceInLastPage = POOL_PAGE_SIZE;
				m_pageArr[++m_maxPageNo].m_data = (byte *)malloc(POOL_PAGE_SIZE);
			}
			assert(m_pageArr[m_maxPageNo].m_data);
			assert(m_maxPageNo <= m_pagesNum);
			u16 offset = (u16)(POOL_PAGE_SIZE - m_freeSpaceInLastPage);
			ObjSlot* slot = (ObjSlot *)(m_pageArr[m_maxPageNo].m_data + offset);
			initSlot(slot, m_maxPageNo, offset);
			m_freeSpaceInLastPage -= sizeof(ObjSlot);
			return slot;
		}
		/**
		 * 初始化新内存槽并压入可用栈
		 * @param slot 内存槽
		 * @param pageNo 页号
		 * @param pageOffset 页内偏移量
		 */
		inline void initSlot(ObjSlot *slot, u16 pageNo, u16 pageOffset) {
			slot->m_address = (LogicalAddr)(pageNo << 16 | pageOffset);
			slot->m_isFree = 1;
			m_stack[m_stackCursor++] = slot->m_address;
		}

		/**
		 * 通过逻辑地址获得内存槽地址
		 * @param i
		 * @return
		 */
		inline ObjSlot *getSlot(LogicalAddr i) const {
			u16 pageNo = (u16)(i >> 16);
			assert(pageNo <= m_maxPageNo);
			u16 pageOffset = (u16)i;
			ObjSlot *slot = (ObjSlot *)(m_pageArr[pageNo].m_data + pageOffset);
			return slot;
		}

	private:
		u16               m_pagesNum;     /** 内存池页面数 */
		MemPoolPage       *m_pageArr;     /** 用于保存内存池各个页面地址 */
		u16               m_maxPageNo;    /** 最大页号 */
		s32               m_freeSpaceInLastPage;/** 最后一个页面剩余空闲空间大小 */
		LogicalAddr       *m_stack;       /** 用于保存可用内存的栈 */
		u32               m_stackCursor;  /** 栈顶指针 */
		u32               m_capacity;     /** 内存池槽数 */
		u32               m_usedSize;     /** 当前可用的内存槽数 */
	};

	typedef u32 TrieNodeAddr;                       //Trie树节点相对地址类型
	#define INVALID_NODE_ADDR ((TrieNodeAddr)(-1))  //非法Trie树节点相对地址

	/** 采样用Trie树节点 */
	class SmplTrieNode {
	public:
		const static u16 MAX_TRIE_NODE = 256;        //Trie树节点的最大子节点数

	public:
		SmplTrieNode(byte key = 0, u8 height = 0, const TrieNodeAddr& parent = INVALID_NODE_ADDR);
		~SmplTrieNode();
		
		/**
		 * 获得指定的子节点
		 * @param childKey 要获取的子节点对应的字节
		 * @return 子节点相对地址
		 */
		inline TrieNodeAddr getChildren(byte childKey) const {
			return m_childEntry[childKey];
		}

		/**
		 * 添加一个指定的子节点
		 * @pre 不存在这样的子节点
		 * @param childKey 子节点对应的字节
		 * @param childAddr 子节点相对地址
		 */
		inline SmplTrieNode* addChildren(ObjMemoryPool<SmplTrieNode> *nodePool, byte childKey) {
			assert(m_height < MAX_TRIE_NODE - 1);
			assert(m_childEntry[childKey] == INVALID_NODE_ADDR);
			TrieNodeAddr curAddr = nodePool->real2Logic(this);
			SmplTrieNode * child = new (nodePool->getFree())SmplTrieNode(childKey, m_height + 1, curAddr);
			TrieNodeAddr childAddr = nodePool->real2Logic(child);
			m_childEntry[childKey] = childAddr;
			m_childCounts++;
			return child;
		}

		/** 获得节点的键值 */
		inline byte getKey() const {
			return m_key;
		}
		/** 获得节点的父节点相对地址 */
		inline TrieNodeAddr parent() const { 
			return m_parent;
		}
		/** 获得节点高度 */
		inline u8 height() const { 
			return m_height; 
		}
		/** 获取节点的子节点数 */
		inline u16 childCounts() const { 
			return m_childCounts; 
		}
		/** 引用计数加1 */
		inline void increaseRfCounts() { 
			m_rfCounts++; 
		}
		/** 获得当前节点的引用计数 */
		inline u64 getRfCounts() const {
			return m_rfCounts;
		}
		/** 是否是叶子节点 */
		inline bool isLeaf() const { 
			return m_childCounts == 0;
		}
		/** 是否被标记为字典项节点 */
		inline bool isDicNode() const {
			return m_isDicNode;
		}
		/** 将当前节点标记为字典项 */
		inline void setDicNode(bool isDicNode) {
			m_isDicNode = isDicNode;
		}

		/** 是否在叶子节点链表中 */
		inline bool isInLeafDList() const { 
			return (NULL != m_prev) && (NULL != m_next);
		}

		/**
		* 删除指定的子节点
		* @pre 存在此子节点
		* @param childKey 子节点对应的字节
		*/
		inline void removeChildren(const byte& childKey) {
			m_childCounts--;
			assert(m_childEntry[childKey] != INVALID_NODE_ADDR);
			m_childEntry[childKey] = INVALID_NODE_ADDR;
		}

		inline SmplTrieNode *prev() const { return m_prev; }
		inline SmplTrieNode *next() const { return m_next; }

	private:
		/** 获得当前节点的权重 */
		inline u64 getWeight() const {
			return (m_height - 2) * m_rfCounts; 
		}

	private:
		byte          m_key;                          /** 节点关键字 */
		u8            m_height;                       /** 节点深度 */
		u64           m_rfCounts;                     /** 引用计数 */
		u16			  m_childCounts;                  /** 子节点数 */
		TrieNodeAddr  m_parent;                       /** 父节点 */                     
		TrieNodeAddr  m_childEntry[MAX_TRIE_NODE];    /** 子节点入口 */
		bool          m_isDicNode;                    /** 是否被标记为字典项节点 */

		/** 叶子节点双向链表相关 */
		SmplTrieNode  *m_prev;        /** 双向链表前向节点 */     
		SmplTrieNode  *m_next;        /** 双向链表后向节点 */

		/** 最大堆相关 */
		uint  m_heapPosition;         /** 在最大堆中的位置 */

	public:
		friend class SmplTrie;
		friend class SmplTrieDList;
		friend class SmplTrieNodeHeap;	
		friend class SmplTrieNodeComparator;
	};

	/**
	 * 采样Trie树节点权重比较仿函数
	 */
	class SmplTrieNodeComparator {
	public:
		/**
		 * 比较两结点权重
		 * @return 如果x < y返回true，否则false
		 */
		inline bool operator()(SmplTrieNode* x, SmplTrieNode* y) const {
			u64 xWeight = x->getWeight();
			u64 yWeight = y->getWeight();
			return xWeight == yWeight ? (x->m_rfCounts < y->m_rfCounts) : (xWeight < yWeight);
		}
			
	};

	/************************************************************************/
	/* 采样用Trie树                                                                     */
	/************************************************************************/
	class SmplTrie {
	public:
		SmplTrie(MemoryContext *mtx, size_t capacity, u16 minLen, 
			u16 maxLen, size_t delSize, u8 cte) throw (NtseException);
		~SmplTrie();
		void removeLeafNode(SmplTrieNode *nodeToRemove);
		void addItem(const byte *dicItemData, const u16 &len);
		u8 getFullKey(SmplTrieNode *node, byte* values); 
		size_t getAllChild(TrieNodeAddr nodeAddr, TrieNodeAddr* values) const;
		void reCalcPrefixRf(SmplTrieNode *node, SmplTrieNodeHeap *maxHeap);
		size_t batchDelLeaves(size_t delSize);
		size_t extractDictionary(std::vector<SmplTrieNode*> *nodeList);
		/** 
		 * 检查当前节点数
		 * 必要时裁剪一批叶子节点 
		 */
		inline void checkSize() {
			if (unlikely(m_size > (m_capacity * m_cte)))
				batchDelLeaves(m_delSize);
		};
		/**
		 * 获得Trie树根节点
		 */
		inline SmplTrieNode* getRoot() const {
			return m_root;
		}
		/**
		 * 获得当前Trie树节点数
		 */
		inline size_t size() const {
			return m_size;
		}
		/**
		 * 获得叶子节点列表
		 */
		inline SmplTrieDList *getLeavesList() {
			return m_leavesList;
		}
		/**
		 * 根据相对地址计算节点的实际地址
		 * @param i 
		 */
		inline SmplTrieNode* getRealNodeAddr(TrieNodeAddr i) const {
			return (SmplTrieNode *)m_nodePool->logic2Real(i);
		}

		size_t travelTrieTree(std::vector<SmplTrieNode*> *nodeList);

	private:
		MemoryContext *m_mtx;                     /** 内存分配上下文 */
		SmplTrieNode  *m_root;                    /** 根节点 */
		size_t        m_capacity;                 /** Trie树限制大小 */
		size_t        m_size;                     /** Trie树实际大小 */
		size_t        m_delSize;                  /** 批删除叶子节点大小 */
		u8            m_cte;                      /** 膨胀系数 */
		u16           m_minLen;                   /** 添加到Trie树的字典项的最小长度 */
		u16           m_maxLen;                   /** 添加到Trie树的字典项的最大长度 */
		SmplTrieDList *m_leavesList;              /** 链接所有叶子节点的双向链表 */
		ObjMemoryPool<SmplTrieNode> *m_nodePool;  /** 用于复用内存的内存池 */
		SmplTrieNodeHeap *m_leafNodeMinHeap;       /** 叶子节点最小堆 */
	};

	/** 用于组织Trie树叶子节点的双向链表 */
	class SmplTrieDList {
	public:
		/**
		 * 构造一个叶子节点双向链表
		 */
		SmplTrieDList() {
			m_header.m_prev = &m_header;
			m_header.m_next = &m_header;
			m_size = 0;
		}
		~SmplTrieDList() {
		}
		/**
		 * 在链表尾插入叶子节点
		 * @param node 要添加的叶子节点
		 */
		inline void addToTail(SmplTrieNode *node) { 
			assert(node->isLeaf());
			m_header.m_prev->m_next = node;
			node->m_prev = m_header.m_prev;
			m_header.m_prev = node;
			node->m_next = &m_header;
			m_size++;
		}
		/**
		 * 移除双向链表尾部元素
		 */
		inline void removeTail() { 
			remove(m_header.m_prev); 
		}
		/** 
		 * 获得当前链表节点数
		 * @return 
		 */
		inline size_t size() const { return m_size; }
		/**
		 * 返回双向列表的数组结果
		 * @param vec OUT，调用方分配足够的内存
		 */
		size_t toArray(SmplTrieNode** arr) const {
			assert (&m_header != m_header.m_next);
			SmplTrieNode *current = m_header.m_next;
			for (uint i = 0; i < m_size && current != &m_header; 
				i++, current = current->m_next) {
				arr[i] = current;
			}
			return m_size;
		}
		/**
		* 从双向链表中删除指定的节点
		* @param nodeToRemove 要删除的节点
		*/
		inline void remove(SmplTrieNode *nodeToRemove) {
			nodeToRemove->m_prev->m_next = nodeToRemove->m_next;
			nodeToRemove->m_next->m_prev = nodeToRemove->m_prev;
			nodeToRemove->m_prev = nodeToRemove->m_next = NULL;
			m_size--;
		}
		inline SmplTrieNode *getHeader() {
			return &m_header;
		}
	private:
		SmplTrieNode m_header; /** 双向链表头 */
		size_t       m_size;   /** 双向链表当前大小 */
	};

	/** 辅助提取字典项以及剪枝的堆 */
	class SmplTrieNodeHeap {
	public:
		/**
		 * 构造堆
		 * @param capacity 最大堆容量
		 */
		SmplTrieNodeHeap(uint capacity, bool isMaxHeap = true):
		m_capacity(capacity), m_size(0), m_isMaxHeap(isMaxHeap) {
			 uint s = capacity + 1;
			 m_heapData = new SmplTrieNode*[s];
			 for (uint i = 0; i < s; i++)
				m_heapData[i] = NULL;
		}
		virtual ~SmplTrieNodeHeap() {
			delete []m_heapData;
			m_heapData = NULL;
		}
		/** 
		 * 获得堆当前大小
		 */
		inline uint size() const { return m_size; }
		/**
		 * 清空堆
		 */
		inline void clear() {
			memset(m_heapData + 1, 0, m_size * sizeof(SmplTrieNode *));
			m_size = 0;
		}
		/**
		 * 往堆添加一个元素
		 */
		inline void add(SmplTrieNode* node) {
			assert(m_size < m_capacity);
			m_heapData[++m_size] = node;
			node->m_heapPosition = m_size;
			fixUp(m_size);
		}

		/**
		 * 返回堆顶元素
		 * @return
		 */
		inline SmplTrieNode* peek() { 
			return m_size > 0 ? m_heapData[1] : NULL; 
		}
		/**
		 * 移除并返回堆顶元素
		 */
		inline SmplTrieNode* poll() {
			if (likely(m_size > 0)) {
				SmplTrieNode* top = m_heapData[1];
				swap(1, m_size--);
				fixDown(1);
				return top;
			}
			return NULL;
		}
		/**
		 * 从上往下重整堆
		 * @param fixPos
		 */
		inline void fixDown(uint fixPos) {
			uint current = 0;
			while ((current = fixPos << 1) <= m_size) {
				if (current < m_size && doCompare(m_heapData[current + 1], m_heapData[current]))
					current++;
				if (doCompare(m_heapData[fixPos], m_heapData[current])) // 不用交换
					break;
				swap(current, fixPos);
				fixPos = current;
			}
		}
		/**
		 * 从下往上重整堆
		 * @param fixPos
		 */
		inline void fixUp(uint fixPos) {
			while (fixPos > 1) {
				uint current = fixPos >> 1;
				if (doCompare(m_heapData[current], m_heapData[fixPos]))
					break;
				swap(current, fixPos);
				fixPos = current;
			}			
		}

	protected:
		/**
		 * 比较两结点权重
		 * @param e1 
		 * @param e2 
		 * @return 对于最小堆，如果 e1 < e2返回true，否则返回false；对于最大堆，如果 e2 < e1返回true，否则返回false
		 */
		inline bool doCompare(SmplTrieNode *e1, SmplTrieNode *e2) {
			return m_isMaxHeap ? compare(e2, e1) : compare(e1, e2);
		}
		/**
		 * 交换堆内元素位置
		 */
		inline void swap(const uint& e1, const uint& e2) {
			assert(m_heapData[e1] != NULL && m_heapData[e2] != NULL);
			std::swap(m_heapData[e1], m_heapData[e2]);
			m_heapData[e2]->m_heapPosition = e2;
			m_heapData[e1]->m_heapPosition = e1;
		}

	protected:
		uint           m_capacity;           /** 堆容量 */
		uint           m_size;               /** 堆当前大小 */
		SmplTrieNode** m_heapData;           /** 存储堆数据的数组 */
		bool           m_isMaxHeap;          /** 是否为最大堆，否则为最小堆 */
		SmplTrieNodeComparator compare;      /** 比较器 */
	};
}

#endif
