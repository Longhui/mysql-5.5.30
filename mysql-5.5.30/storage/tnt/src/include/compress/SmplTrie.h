/**
* ����Trie��
*
* @author ��ΰ��(liweizhao@corp.netease.com, liweizhao@163.org)
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
	 * �����ڴ��
	 *
	 * ����trie���������ͼ�֦�����У��Լ��������ڵĹ�ϣ�����ڲ�����������Ҫnew�ǳ����С���ڴ棬
	 * ��������ڴ���Ƭ�����Ա�OS���գ����Բ��ö����ڴ��������һ������ڴ棬����Ҫ���ɶ����ʱ��
	 * ���ڴ���л�ÿ��еĲۼ��ɣ��ͷŵ�ʱ�򷵻��ڴ�ء�����ֻ����һ�ֶ������ͣ������ڴ�ز۵Ĵ�С��
	 * �̶��ģ������Ҫʵ�ֲ������Ĳۣ���Ƚϸ���
	 */
	template<typename T>
	class ObjMemoryPool {
	public:
		static const u32 POOL_PAGE_SIZE = 1 << 16;  /** �����ڴ��ҳ��С */

		typedef u32 LogicalAddr;//ͷ2���ֽڱ�ʾҳ�ţ���2���ֽڱ�ʾҳ��ƫ����

		struct MemPoolPage {
			byte *m_data; //ҳ��������ʼ��ַ
		};

		struct ObjSlot {
			T           m_obj;        //����
			LogicalAddr m_address;    //�߼���ַ
			u8          m_isFree : 1; //�Ƿ����
		} ;

	public:
		/**
		 * ����һ�������ڴ��
		 * @param capacity �ڴ�������������ڴ�ش�СΪcapacity * sizeof(T)
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
		 * �ͷŶ����ڴ��
		 * ����ڴ���л���û����ʽ����markFree�ͷŵĶ���������Ҳ�ᱻ�ͷ�
		 */
		~ObjMemoryPool() {
 			for (u32 i = 0; i < m_pagesNum; i++) {
 				free(m_pageArr[i].m_data);
 			}
 			free(m_pageArr);
			free(m_stack);
		}

		/**
		 * ����һ���ڴ�زۣ��۴�СΪsizeof(T)
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
		 * �ͷ�һ���ڴ�ز�
		 * �ͷź�ò۱�Ϊ�����ڴ�ز۲������ڴ��
		 * @param obj ����ռ�õ��ڴ�ز۵�ַ
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
		 * ��õ�ǰ��ʹ���е��ڴ�ز���
		 * @return
		 */
		inline u32 getSize() const {
			return m_usedSize;
		}
		
		/**
		* ת���߼���ַΪʵ�ʵ�ַ
		* @param i
		* @return
		*/
		inline void* logic2Real(LogicalAddr i) const {
			ObjSlot *slot = getSlot(i);
			return &slot->m_obj;
		}
		/**
		* ת��ʵ�ʵ�ַΪ�߼���ַ
		* @pre obj�������ڴ�صķ�Χ��
		* @param obj 
		* @return
		*/
		inline LogicalAddr real2Logic(T *t) const {
			return ((ObjSlot *)t)->m_address;
		}

	private:
		/**
		 * ����һ�����ڴ��
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
		 * ��ʼ�����ڴ�۲�ѹ�����ջ
		 * @param slot �ڴ��
		 * @param pageNo ҳ��
		 * @param pageOffset ҳ��ƫ����
		 */
		inline void initSlot(ObjSlot *slot, u16 pageNo, u16 pageOffset) {
			slot->m_address = (LogicalAddr)(pageNo << 16 | pageOffset);
			slot->m_isFree = 1;
			m_stack[m_stackCursor++] = slot->m_address;
		}

		/**
		 * ͨ���߼���ַ����ڴ�۵�ַ
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
		u16               m_pagesNum;     /** �ڴ��ҳ���� */
		MemPoolPage       *m_pageArr;     /** ���ڱ����ڴ�ظ���ҳ���ַ */
		u16               m_maxPageNo;    /** ���ҳ�� */
		s32               m_freeSpaceInLastPage;/** ���һ��ҳ��ʣ����пռ��С */
		LogicalAddr       *m_stack;       /** ���ڱ�������ڴ��ջ */
		u32               m_stackCursor;  /** ջ��ָ�� */
		u32               m_capacity;     /** �ڴ�ز��� */
		u32               m_usedSize;     /** ��ǰ���õ��ڴ���� */
	};

	typedef u32 TrieNodeAddr;                       //Trie���ڵ���Ե�ַ����
	#define INVALID_NODE_ADDR ((TrieNodeAddr)(-1))  //�Ƿ�Trie���ڵ���Ե�ַ

	/** ������Trie���ڵ� */
	class SmplTrieNode {
	public:
		const static u16 MAX_TRIE_NODE = 256;        //Trie���ڵ������ӽڵ���

	public:
		SmplTrieNode(byte key = 0, u8 height = 0, const TrieNodeAddr& parent = INVALID_NODE_ADDR);
		~SmplTrieNode();
		
		/**
		 * ���ָ�����ӽڵ�
		 * @param childKey Ҫ��ȡ���ӽڵ��Ӧ���ֽ�
		 * @return �ӽڵ���Ե�ַ
		 */
		inline TrieNodeAddr getChildren(byte childKey) const {
			return m_childEntry[childKey];
		}

		/**
		 * ���һ��ָ�����ӽڵ�
		 * @pre �������������ӽڵ�
		 * @param childKey �ӽڵ��Ӧ���ֽ�
		 * @param childAddr �ӽڵ���Ե�ַ
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

		/** ��ýڵ�ļ�ֵ */
		inline byte getKey() const {
			return m_key;
		}
		/** ��ýڵ�ĸ��ڵ���Ե�ַ */
		inline TrieNodeAddr parent() const { 
			return m_parent;
		}
		/** ��ýڵ�߶� */
		inline u8 height() const { 
			return m_height; 
		}
		/** ��ȡ�ڵ���ӽڵ��� */
		inline u16 childCounts() const { 
			return m_childCounts; 
		}
		/** ���ü�����1 */
		inline void increaseRfCounts() { 
			m_rfCounts++; 
		}
		/** ��õ�ǰ�ڵ�����ü��� */
		inline u64 getRfCounts() const {
			return m_rfCounts;
		}
		/** �Ƿ���Ҷ�ӽڵ� */
		inline bool isLeaf() const { 
			return m_childCounts == 0;
		}
		/** �Ƿ񱻱��Ϊ�ֵ���ڵ� */
		inline bool isDicNode() const {
			return m_isDicNode;
		}
		/** ����ǰ�ڵ���Ϊ�ֵ��� */
		inline void setDicNode(bool isDicNode) {
			m_isDicNode = isDicNode;
		}

		/** �Ƿ���Ҷ�ӽڵ������� */
		inline bool isInLeafDList() const { 
			return (NULL != m_prev) && (NULL != m_next);
		}

		/**
		* ɾ��ָ�����ӽڵ�
		* @pre ���ڴ��ӽڵ�
		* @param childKey �ӽڵ��Ӧ���ֽ�
		*/
		inline void removeChildren(const byte& childKey) {
			m_childCounts--;
			assert(m_childEntry[childKey] != INVALID_NODE_ADDR);
			m_childEntry[childKey] = INVALID_NODE_ADDR;
		}

		inline SmplTrieNode *prev() const { return m_prev; }
		inline SmplTrieNode *next() const { return m_next; }

	private:
		/** ��õ�ǰ�ڵ��Ȩ�� */
		inline u64 getWeight() const {
			return (m_height - 2) * m_rfCounts; 
		}

	private:
		byte          m_key;                          /** �ڵ�ؼ��� */
		u8            m_height;                       /** �ڵ���� */
		u64           m_rfCounts;                     /** ���ü��� */
		u16			  m_childCounts;                  /** �ӽڵ��� */
		TrieNodeAddr  m_parent;                       /** ���ڵ� */                     
		TrieNodeAddr  m_childEntry[MAX_TRIE_NODE];    /** �ӽڵ���� */
		bool          m_isDicNode;                    /** �Ƿ񱻱��Ϊ�ֵ���ڵ� */

		/** Ҷ�ӽڵ�˫��������� */
		SmplTrieNode  *m_prev;        /** ˫������ǰ��ڵ� */     
		SmplTrieNode  *m_next;        /** ˫���������ڵ� */

		/** ������� */
		uint  m_heapPosition;         /** �������е�λ�� */

	public:
		friend class SmplTrie;
		friend class SmplTrieDList;
		friend class SmplTrieNodeHeap;	
		friend class SmplTrieNodeComparator;
	};

	/**
	 * ����Trie���ڵ�Ȩ�رȽϷº���
	 */
	class SmplTrieNodeComparator {
	public:
		/**
		 * �Ƚ������Ȩ��
		 * @return ���x < y����true������false
		 */
		inline bool operator()(SmplTrieNode* x, SmplTrieNode* y) const {
			u64 xWeight = x->getWeight();
			u64 yWeight = y->getWeight();
			return xWeight == yWeight ? (x->m_rfCounts < y->m_rfCounts) : (xWeight < yWeight);
		}
			
	};

	/************************************************************************/
	/* ������Trie��                                                                     */
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
		 * ��鵱ǰ�ڵ���
		 * ��Ҫʱ�ü�һ��Ҷ�ӽڵ� 
		 */
		inline void checkSize() {
			if (unlikely(m_size > (m_capacity * m_cte)))
				batchDelLeaves(m_delSize);
		};
		/**
		 * ���Trie�����ڵ�
		 */
		inline SmplTrieNode* getRoot() const {
			return m_root;
		}
		/**
		 * ��õ�ǰTrie���ڵ���
		 */
		inline size_t size() const {
			return m_size;
		}
		/**
		 * ���Ҷ�ӽڵ��б�
		 */
		inline SmplTrieDList *getLeavesList() {
			return m_leavesList;
		}
		/**
		 * ������Ե�ַ����ڵ��ʵ�ʵ�ַ
		 * @param i 
		 */
		inline SmplTrieNode* getRealNodeAddr(TrieNodeAddr i) const {
			return (SmplTrieNode *)m_nodePool->logic2Real(i);
		}

		size_t travelTrieTree(std::vector<SmplTrieNode*> *nodeList);

	private:
		MemoryContext *m_mtx;                     /** �ڴ���������� */
		SmplTrieNode  *m_root;                    /** ���ڵ� */
		size_t        m_capacity;                 /** Trie�����ƴ�С */
		size_t        m_size;                     /** Trie��ʵ�ʴ�С */
		size_t        m_delSize;                  /** ��ɾ��Ҷ�ӽڵ��С */
		u8            m_cte;                      /** ����ϵ�� */
		u16           m_minLen;                   /** ��ӵ�Trie�����ֵ������С���� */
		u16           m_maxLen;                   /** ��ӵ�Trie�����ֵ������󳤶� */
		SmplTrieDList *m_leavesList;              /** ��������Ҷ�ӽڵ��˫������ */
		ObjMemoryPool<SmplTrieNode> *m_nodePool;  /** ���ڸ����ڴ���ڴ�� */
		SmplTrieNodeHeap *m_leafNodeMinHeap;       /** Ҷ�ӽڵ���С�� */
	};

	/** ������֯Trie��Ҷ�ӽڵ��˫������ */
	class SmplTrieDList {
	public:
		/**
		 * ����һ��Ҷ�ӽڵ�˫������
		 */
		SmplTrieDList() {
			m_header.m_prev = &m_header;
			m_header.m_next = &m_header;
			m_size = 0;
		}
		~SmplTrieDList() {
		}
		/**
		 * ������β����Ҷ�ӽڵ�
		 * @param node Ҫ��ӵ�Ҷ�ӽڵ�
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
		 * �Ƴ�˫������β��Ԫ��
		 */
		inline void removeTail() { 
			remove(m_header.m_prev); 
		}
		/** 
		 * ��õ�ǰ����ڵ���
		 * @return 
		 */
		inline size_t size() const { return m_size; }
		/**
		 * ����˫���б��������
		 * @param vec OUT�����÷������㹻���ڴ�
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
		* ��˫��������ɾ��ָ���Ľڵ�
		* @param nodeToRemove Ҫɾ���Ľڵ�
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
		SmplTrieNode m_header; /** ˫������ͷ */
		size_t       m_size;   /** ˫������ǰ��С */
	};

	/** ������ȡ�ֵ����Լ���֦�Ķ� */
	class SmplTrieNodeHeap {
	public:
		/**
		 * �����
		 * @param capacity ��������
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
		 * ��öѵ�ǰ��С
		 */
		inline uint size() const { return m_size; }
		/**
		 * ��ն�
		 */
		inline void clear() {
			memset(m_heapData + 1, 0, m_size * sizeof(SmplTrieNode *));
			m_size = 0;
		}
		/**
		 * �������һ��Ԫ��
		 */
		inline void add(SmplTrieNode* node) {
			assert(m_size < m_capacity);
			m_heapData[++m_size] = node;
			node->m_heapPosition = m_size;
			fixUp(m_size);
		}

		/**
		 * ���ضѶ�Ԫ��
		 * @return
		 */
		inline SmplTrieNode* peek() { 
			return m_size > 0 ? m_heapData[1] : NULL; 
		}
		/**
		 * �Ƴ������ضѶ�Ԫ��
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
		 * ��������������
		 * @param fixPos
		 */
		inline void fixDown(uint fixPos) {
			uint current = 0;
			while ((current = fixPos << 1) <= m_size) {
				if (current < m_size && doCompare(m_heapData[current + 1], m_heapData[current]))
					current++;
				if (doCompare(m_heapData[fixPos], m_heapData[current])) // ���ý���
					break;
				swap(current, fixPos);
				fixPos = current;
			}
		}
		/**
		 * ��������������
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
		 * �Ƚ������Ȩ��
		 * @param e1 
		 * @param e2 
		 * @return ������С�ѣ���� e1 < e2����true�����򷵻�false���������ѣ���� e2 < e1����true�����򷵻�false
		 */
		inline bool doCompare(SmplTrieNode *e1, SmplTrieNode *e2) {
			return m_isMaxHeap ? compare(e2, e1) : compare(e1, e2);
		}
		/**
		 * ��������Ԫ��λ��
		 */
		inline void swap(const uint& e1, const uint& e2) {
			assert(m_heapData[e1] != NULL && m_heapData[e2] != NULL);
			std::swap(m_heapData[e1], m_heapData[e2]);
			m_heapData[e2]->m_heapPosition = e2;
			m_heapData[e1]->m_heapPosition = e1;
		}

	protected:
		uint           m_capacity;           /** ������ */
		uint           m_size;               /** �ѵ�ǰ��С */
		SmplTrieNode** m_heapData;           /** �洢�����ݵ����� */
		bool           m_isMaxHeap;          /** �Ƿ�Ϊ���ѣ�����Ϊ��С�� */
		SmplTrieNodeComparator compare;      /** �Ƚ��� */
	};
}

#endif
