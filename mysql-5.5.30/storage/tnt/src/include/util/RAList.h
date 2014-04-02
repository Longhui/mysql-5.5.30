/**
 * �����λ����
 *
 * @author ��Դ(wangyuan@corp.netease.com, wy@163.org)
 */

#ifndef _NTSE_RA_LIST_H
#define _NTSE_RA_LIST_H

#include <vector>
#include "util/DList.h"
#include "util/Hash.h"

using namespace std;

namespace ntse {

template<typename T> struct RAListChunk;
/** ����ڵ�
 * @param T �ڵ���������
 */
template<typename T>
struct RAListNode {
	DLink<RAListNode<T>*>	m_linkPos;	/** �ڵ������е�λ�� */
	RAListChunk				*m_chunk;	/** �����ڵ�Ƭ */
	T						m_data;		/** �ڵ����� */
public:
	RAListNode(): m_linkPos(this) {
		m_chunk = NULL;
	}
	RAListNode(T data): m_linkPos(this), m_data(data) {
		m_chunk = NULL;
	}
};

template<typename T> struct RAListPart;
/** �ڵ�Ƭ
 * @param T �ڵ���������
 */
template<typename T>
struct RAListChunk {
	DLink<RAListChunk<T>*>	m_linkPos;	/** �ڽڵ�Ƭ�����е�λ�� */
	RAListNode<T>			*m_first;	/** Ƭ�е�һ���ڵ� */
	RAListPart<T>			*m_part;	/** �����ڵ���� */
	size_t					m_size;		/** Ƭ�а����ڵ��� */
public:
	RAListChunk(): m_linkPos(this) {
		m_first = NULL;
		m_part = NULL;
		m_size = 0;
	}
};

/** �ڵ����
 * @param T �ڵ���������
 */
template<typename T>
struct RAListPart {
	DLink<RAListPart<T>*>	m_linkPos;	/** �ڽڵ���������е�λ�� */
	RAListChunk<T>			*m_first;	/** �����еĵ�һƬ */
	size_t					m_chunks;	/** �����а�����Ƭ�� */
	size_t					m_size;		/** �����а����Ľڵ��� */
public:
	RAListPart(): m_linkPos(this) {
		m_first = NULL;
		m_chunks = m_size = 0;
	}
};

/** ֧�ֿ���������ʵ�����
 * @param T �ڵ���������
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
