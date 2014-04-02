/**
 * 随机定位链表
 *
 * @author 汪源(wangyuan@corp.netease.com, wy@163.org)
 */

#ifndef _NTSE_RA_LIST_H
#define _NTSE_RA_LIST_H

#include <vector>
#include "util/DList.h"
#include "util/Hash.h"

using namespace std;

namespace ntse {

template<typename T> struct RAListChunk;
/** 链表节点
 * @param T 节点数据类型
 */
template<typename T>
struct RAListNode {
	DLink<RAListNode<T>*>	m_linkPos;	/** 节点链表中的位置 */
	RAListChunk				*m_chunk;	/** 所属节点片 */
	T						m_data;		/** 节点数据 */
public:
	RAListNode(): m_linkPos(this) {
		m_chunk = NULL;
	}
	RAListNode(T data): m_linkPos(this), m_data(data) {
		m_chunk = NULL;
	}
};

template<typename T> struct RAListPart;
/** 节点片
 * @param T 节点数据类型
 */
template<typename T>
struct RAListChunk {
	DLink<RAListChunk<T>*>	m_linkPos;	/** 在节点片链表中的位置 */
	RAListNode<T>			*m_first;	/** 片中第一个节点 */
	RAListPart<T>			*m_part;	/** 所属节点分区 */
	size_t					m_size;		/** 片中包含节点数 */
public:
	RAListChunk(): m_linkPos(this) {
		m_first = NULL;
		m_part = NULL;
		m_size = 0;
	}
};

/** 节点分区
 * @param T 节点数据类型
 */
template<typename T>
struct RAListPart {
	DLink<RAListPart<T>*>	m_linkPos;	/** 在节点分区链表中的位置 */
	RAListChunk<T>			*m_first;	/** 分区中的第一片 */
	size_t					m_chunks;	/** 分区中包含的片数 */
	size_t					m_size;		/** 分区中包含的节点数 */
public:
	RAListPart(): m_linkPos(this) {
		m_first = NULL;
		m_chunks = m_size = 0;
	}
};

/** 支持快速随机访问的链表
 * @param T 节点数据类型
 */
template<typename T>
class RAList {
	static ObjectPool<RAListNode<T> > *m_nodePool;
	static ObjectPool<RAListNode<T> > *m_chunkPool;
	static ObjectPool<RAListNode<T> > *m_partPool;

	size_t			m_splitThreshold;
	size_t			m_mergeThreshold;
	size_t			m_size;
	RAListPart<T>	*m_list;
typedef DynHash<T, RAListNode<T> *, 
	LinearHash<RAListNode>	hash;
public:
	RAList(const vector<long> &vec, size_t chunkSize);
	void addHead(long value);
	bool remove(long value);
	bool moveToHead(long value);
	size_t getRange(size_t offset, size_t limit, long buf[]);
	long operator [](size_t i);
	void check(void);
	size_t size(void);
	RAListNode* getHead(void);
	static void destroy_pool(void);
public:
	~RAList(void);
private:
	void addToListHead(RAListNode *node);
	void removeFromList(RAListNode *node);
	RAListNode* allocNode(long value);
	void freeNode(RAListNode *node);
	RAListChunk* allocChunk(void);
	void freeChunk(RAListChunk *chunk);
	RAListPart* allocPart(void);
	void freePart(RAListPart *part);

public:
	class iterator
	{
		RAListNode	*node;
	public:
		explicit iterator(RAList *list);
		bool hasNext(void);
		long next(void);
	};
};

#endif
