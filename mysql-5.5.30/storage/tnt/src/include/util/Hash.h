/**
 * 哈希表
 *
 * @author 汪源(wangyuan@corp.netease.com, wy@163.org)
 */

#ifndef _NTSE_HASH_H_
#define _NTSE_HASH_H_

#include <string.h>
#include <functional>
#include <iostream>
#include <math.h>
#include "util/PagePool.h"
#include "util/PagePool.h"
#include "util/Array.h"
#include "util/ObjectPool.h"
#include "util/System.h"

namespace ntse {
/**
 * 为常用类型提供计算哈希值功能的函数对象
 * 各种数据类型哈希函数的计算参考Java实现
 * 
 * @param T 数据类型
 */
template<typename T>
class Hasher {
public:
	inline unsigned int operator()(const T &v) const {
		return hashCode(v);
	}
private:
	static inline unsigned int hashCode(int v) {
		return v;
	}

	static inline unsigned int hashCode(long v) {
		return (unsigned int)v;
	}
	
	static inline unsigned int hashCode(u64 v) {
		return (unsigned int)v;
	}

	static inline unsigned int hashCode(const char *v) {
		unsigned int h = 0;
		const char *p = v;
		while (*p != '\0') {
			h = 31 * h + *p;
			p++;
		}
		return h;
	}

	static inline unsigned int hashCode(u16 v) {
		return (unsigned int)v;
	}
};

/**
 * 为常用类型提供等值比较的函数对象
 *
 * @param T1 参数1类型
 * @param T2 参数2类型
 */
template<typename T1, typename T2>
class Equaler {
public:
	inline bool operator()(const T1 &v1, const T2 &v2) const {
		return equals(v1, v2);
	}

private:
	static bool equals(int v1, int v2) {
		return v1 == v2;
	}

	static bool equals(long v1, long v2) {
		return v1 == v2;
	}

	static inline bool equals(u64 v1, u64 v2) {
		return v1 == v2;
	}

	static bool equals(const char *v1, const char *v2) {
		return strcmp(v1, v2) == 0;
	}
};

/** 不考虑大小写，计算字符串哈希值的函数对象 */
class StrNoCaseHasher {
public:
	inline unsigned int operator()(const char *str) const {
		unsigned int h = 0;
		const char *p = str;
		while (*p != '\0') {
			char ch = (char )tolower(*p);
			h = 31 * h + ch;
			p++;
		}
		return h;
	}
};

/** 不考虑大小写，比较两个字符串是否相等的函数对象 */
class StrNoCaseEqualer {
public:
	inline bool operator()(const char *s1, const char *s2) const {
		return System::stricmp(s1, s2) == 0;
	}
};

/** 动态哈希表中的元素 */
template<typename E>
struct DHElem {
	E		entry;		/** 入口项 */
	unsigned int	hashCode;	/** 哈希值 */
	int		next;		/** 冲突链表中下一节点位置，负数表示没有下一节点 */
};

/** 动态哈希。使用线性哈希算法实现，每次只会扩展一个桶，对一个桶中的数据重新
 * 计算哈希值。
 *
 * @param K 键类型
 * @param E 项类型
 * @param EH 根据项计算哈希值的对象类型，可以省略。若省略则由Hasher<K>类提供
 * @param EH 根据键计算哈希值的对象类型，可以省略。若省略则由Hasher<K>类提供
 * @param KEC 比较项与值是否相等的比较函数对象类型，可以省略。若省略则为
 *   Equaler<K, E>
 */
template<typename K, typename E, typename EH = Hasher<E>, 
	typename KH = Hasher<K>, typename KEC = Equaler<K, E> >
class DynHash {
public:
	/**
	 * 创建一个不使用内存页池的动态哈希表
	 */
	DynHash(): m_data() {
		init();
	}

	/**
	 * 创建一个使用内存页池的动态哈希表
	 * 
	 * @param poolUser 内存页池用户
	 * @param pageType 我使用的页类型
	 */
	DynHash(PagePoolUser *poolUser, PageType pageType): m_data(poolUser, pageType) {
		init();
	}

	/** 插入一个数据项
	 * 
	 * @param entry 数据项，其中包含键与值
	 * @return 成功与否
	 */
	bool put(const E &entry) {
		int hashCode = m_entryHasher(entry);
		unsigned int bucket = getBucket(hashCode);
		if (m_size == 0) {
			if (!m_data.expand())
				return false;
			setElem(&m_data[m_size], entry, hashCode, -1);
		} else {
			// 插入点有数据，这时有两种情况
			// 1. 待插入数据与插入点原数据从属于同一冲突链表，这时插入点原数据
			// 一定是冲突链表头，这时应该将待插入数据存储到最后，同时将待插入
			// 数据插入到冲突链表的第二个
			// 2. 待插入数据与插入点原数据不属于同一冲突链表，这时插入点原数据
			// 一定不是其所属冲突链表头，这时应该将原插入点数据移到最后
			DHElem<E> *bucketElem = &m_data[bucket], *sizeElem;
			unsigned int oldBucket = getBucket(bucketElem->hashCode);
			if (oldBucket == bucket) {
				if (!m_data.expand())
					return false;
				sizeElem = &m_data[m_size];
				setElem(sizeElem, entry, hashCode, bucketElem->next);
				bucketElem->next = (int)m_size;
			} else {
				if (!m_data.expand())
					return false;
				sizeElem = &m_data[m_size];
				moveBucket(oldBucket, bucket, m_size, bucketElem, sizeElem);
				setElem(bucketElem, entry, hashCode, -1);
			}

			// 分裂m_split对应的桶，这些节点将分布到m_split或m_h1 + m_split
			// (即m_size)这两个桶中。遍历m_split桶的冲突链表，对于分裂后仍属于
			// m_split的节点不动，对于分裂后属于m_h1 + m_split(m_size)的
			// 节点，将这一节点从冲突链表中删除，将加入到m_h1 + m_split
			// 链表中
			DHElem<E> *currElem = &m_data[m_split], *nextElem, *prevElem = NULL;
			if ((currElem->hashCode & (m_h1 - 1)) == m_split) {	
				// m_split是其所在冲突链表的头
				int currPos = (int)m_split, nextPos, prevPos = -1;
				bool newListIsEmpty = true;	// m_h1 + m_split链表是否为空
				while (currPos >= 0) {
					nextPos = currElem->next;
					nextElem = NULL;
					if ((currElem->hashCode & (m_h2 - 1)) 
						== m_h1 + m_split) {
						DHElem<E> currElemSave = *currElem;
						// 这个节点应该从属于新链表
						if (newListIsEmpty) {
							// 若m_h1 + m_split链表为空，则需要将当前节点
							// 移到m_size做为新链表的头节点
							// 若移走的是m_split链表头，并且有后一节点，应该将
							// 后一节点移到当前位置，然后将m_size移到后一节点
							// 所在位置；若没有后一节点，应将m_size移到当前位置
							if (currPos == (int)m_split) {
								// 当前节点是m_split链表头
								if (nextPos == (int)m_size) {
									nextElem = &m_data[nextPos];
									*currElem = *nextElem;
								} else if (nextPos >= 0) {
									nextElem = &m_data[nextPos];
									*currElem = *nextElem;
									moveBucket(m_size, nextPos, sizeElem, nextElem);
								} else {
									moveBucket(m_size, currPos, sizeElem, currElem);
									currPos = -1;
								}
							} else {
								assert(prevPos >= 0);
								prevElem->next = nextPos;
								if (currPos != (int)m_size) {
									moveBucket(m_size, currPos, sizeElem, currElem);
									if (prevPos == (int)m_size) {
										prevPos = currPos;
										prevElem = currElem;
									}
								}
								currPos = prevElem->next;
								if (currPos >= 0)
									currElem = &m_data[currPos];
							}
							*sizeElem = currElemSave;
							sizeElem->next = -1;
							newListIsEmpty = false;
						} else {
							// m_h1 + m_split链表非空
							if (prevPos < 0 && nextPos >= 0) {
								// 移走m_split的链表头，并且有后一节点，则将后一
								// 节点移到当前位置，然后将当前节点插入到后一
								// 位置，并插入到m_size链表的第二个节点
								DHElem<E> currElemSave = *currElem;
								nextElem = &m_data[nextPos];
								*currElem = *nextElem;
								setElem(nextElem, currElemSave.entry, currElemSave.hashCode, sizeElem->next);
								sizeElem->next = nextPos;
							} else {
								currElem->next = sizeElem->next;
								sizeElem->next = currPos;
								if (prevPos >= 0)
									prevElem->next = nextPos;
								currPos = nextPos;
								if (currPos >= 0)
									currElem = &m_data[currPos];
							}
						}
					} else {
						prevPos = currPos;
						prevElem = currElem;
						currPos = nextPos;
						if (currPos >= 0)
							currElem = &m_data[currPos];
					}
				}
			}

			m_split++;
			if (m_split == m_h1) {
				m_h1 <<= 1;
				m_h2 = m_h1 << 1;
				m_split = 0;
			}
		}
		m_size++;
		return true;
	}

	/** 搜索与给定键相等的数据项
	 * 
	 * @param key 要搜索的数据项
	 * @return 当哈希表中存在与key相等的数据项则返回该数据项，否则返回(E)0
	 */
	E get(const K &key) const {
		if (m_size == 0)
			return (E)0;
		
		unsigned int hashCode = m_keyHasher(key);
		unsigned int bucket = getBucket(hashCode);
		DHElem<E> *hashElem = &m_data[bucket];
		while (hashElem->hashCode != hashCode 
			|| !m_equaler(key, hashElem->entry)) {
			if (hashElem->next < 0)
				return (E)0;
			hashElem = &m_data[hashElem->next];
		}
		return hashElem->entry;
	}

	/**
	 * 获取位置为i的数据项
	 *
	 * @param i 存取位置，在[0, m_size)之间
	 */
	E getAt(size_t i) const {
		assert(i < m_size);
		return m_data[i].entry;
	}

	/** 删除与给定键值相等的数据项
	 * 
	 * @param key 要删除的键值
	 * @return 当哈希表中存在与key相等的数据项则返回该数据项，否则返回(E)0
	 */
	E remove(const K &key) {
		if (m_size == 0)
			return (E)0;

		E ret;
		unsigned int hashCode = m_keyHasher(key);
		unsigned int bucket = getBucket(hashCode);
		int currPos = bucket, prevPos = -1;
		DHElem<E> *currElem = &m_data[bucket], *prevElem = NULL;
		while (currElem->hashCode != hashCode 
			|| !m_equaler(key, currElem->entry)) {
			if (currElem->next < 0)
				return (E)0;
			prevPos = currPos;
			prevElem = currElem;
			currPos = currElem->next;
			currElem = &m_data[currPos];
		}
		ret = currElem->entry;

		unsigned int deletedPos;
		// 删除找到的节点，若找到的节点不是所属冲突链表的链表头，则从链表中
		// 删除这一节点，否则需要将冲突链表的后一个节点挪到当前位置
		if (prevPos >= 0) {
			prevElem->next = currElem->next;
			deletedPos = currPos;
		} else if (currElem->next < 0)
			deletedPos = currPos;
		else {
			deletedPos = currElem->next;
			*currElem = m_data[deletedPos];
		}
		
		m_size--;
		if (m_size == 0) {
			m_h1 = 1;
			m_h2 = 2;
			m_split = 0;
		} else {
			size_t oldList, newLevelPower;
			if (m_split > 0) {
				newLevelPower = m_h1;
				oldList = m_split - 1;
			} else {
				newLevelPower = m_h1 >> 1;
				oldList = newLevelPower - 1;
			}
			if (deletedPos != m_size) {
				// 若m_size冲突链表非空，则合并m_size与oldList到oldList
				DHElem<E> *olElem = &m_data[oldList];
				if (oldList == deletedPos) {
					// 这时oldList冲突链表为空，不管m_size是不是其所在冲突链表
					// 的头，只需要将它移到oldList即可
					moveBucket(m_size, oldList, &m_data[m_size], olElem);
				} else if (getBucket(olElem->hashCode) == oldList) {
					// oldList上的数据是要合并的冲突链表头
					DHElem<E> *sizeElem = &m_data[m_size];
					if (getBucket(sizeElem->hashCode) == m_size) {
						// 要合并的两个冲突链表都不为空
						m_data[deletedPos] = *sizeElem;
						int tail = (int)oldList;
						while (olElem->next >= 0) {
							tail = olElem->next;
							olElem = &m_data[tail];
						}
						olElem->next = deletedPos;
					} else {
						// m_size链表为空
						moveBucket(m_size, deletedPos, sizeElem, &m_data[deletedPos]);
					}
				} else {
					// oldList链表为空
					DHElem<E> *sizeElem = &m_data[m_size];
					if ((sizeElem->hashCode & (newLevelPower - 1)) == oldList) {
						moveBucket(oldList, deletedPos, olElem, &m_data[deletedPos]);
						*olElem = *sizeElem;
					} else {
						moveBucket(m_size, deletedPos, sizeElem, &m_data[deletedPos]);
					}
				}
			}
			if (m_split == 0) {
				m_h1 >>= 1;
				m_h2 = m_h1 << 1;
				m_split = m_h1;
			}
			m_split--;
		}
		m_data.pop();

		return ret;
	}

	/**
	 * 得到哈希表当前大小
	 *
	 * @return 哈希表当前大小
	 */
	size_t getSize() const {
		return m_size;
	}

	/**
	 * 设定预留空间
	 *
	 * @param size 预留空间
	 * @param forceAllocForPool 使用内存页池分配空间时是否用allocPageForce接口
	 * @return 是否成功
	 */
	bool reserveSize(size_t size, bool forceAllocForPool = false) {
		return m_data.setReservedSize(size, forceAllocForPool);
	}

	/** 清空哈希表中所有数据 */
	void clear() {
		m_data.clear();
		init();
	}

	/** 获得哈希表占用的内存大小
	 * @param 占用的内存大小
	 */
	size_t getMemUsage() const {
		return m_data.getMemUsage();
	}

	/**
	 * 检查哈希表正确性。只在哈希表单元测试过程中使用
	 */
	void check() {
		for (unsigned int i = 0; i < m_size; i++) {
			DHElem<E> *e = &m_data[i];
			if (getBucket(e->hashCode) == i) {
				while (true) {
					assert(getBucket(e->hashCode) == i);
					if (e->next < 0)
						break;
					e = &m_data[e->next];
				}
			} else {
				unsigned int bucket = getBucket(e->hashCode);
				e = &m_data[bucket];
				while (e->next != i)
					e = &m_data[e->next];
			}
		}
	}

	/**
	 * 获得哈希表冲突情况统计信息
	 *
	 * @param avgConflictLen OUT，冲突链表平均长度
	 * @param maxConflictLen OUT，冲突链表最大长度
	 */
	void getConflictStatus(double *avgConflictLen, size_t *maxConflictLen) {
		size_t numConflictLists = 0;
		size_t totalConflictLen = 0;
		*maxConflictLen = 0;
		for (unsigned int i = 0; i < m_size; i++) {
			DHElem<E> *e = &m_data[i];
			if (getBucket(e->hashCode) == i) {
				numConflictLists++;
				size_t conflictLen = 1;
				while (true) {
					assert(getBucket(e->hashCode) == i);
					if (e->next < 0)
						break;
					conflictLen++;
					e = &m_data[e->next];
				}
				if (conflictLen > *maxConflictLen)
					*maxConflictLen = conflictLen;
				totalConflictLen += conflictLen;
			}
		}
		if (numConflictLists)
			*avgConflictLen = (double)totalConflictLen / numConflictLists;
		else
			*avgConflictLen = 0;
	}

	/**
	 * 打印哈希表内容到标准输出
	 */
	void print() {
		std::cout << "== begin dump hash table == " << std::endl;
		std::cout << "size: " << m_size << std::endl;
		std::cout << "split: " << m_split << std::endl;
		std::cout << "h1: " << m_h1 << std::endl;
		std::cout << "buckets: " << std::endl;
		for (size_t i = 0; i < m_size; i++) {
			DHElem<E> *e = &m_data[i];
			std::cout << "  " << i << ": " << e->entry << "/" << e->hashCode;
			if (e->next >= 0)
				std::cout << " --> " << e->next;
			std::cout << std::endl;
		}
		std::cout << "== end dump hash table == " << std::endl;
	}

private:
	unsigned int getBucket(unsigned int hashCode) const {
		unsigned int bucket = hashCode & (m_h1 - 1);
		if (bucket < m_split)	// 这个桶已经分裂过
			bucket = hashCode & (m_h2 - 1);
		return bucket;
	}

	inline void setElem(DHElem<E> *elem, const E &entry, int hashCode, int next) {
		elem->entry = entry;
		elem->hashCode = hashCode;
		elem->next = next;
	}

	/** 将原位于from的元素移到to，维持调整冲突链表结构
	 * 当知道元素所属冲突链表头时，请使用3参数的moveBucket性能更高
	 *
	 * @param from 元素原来所在位置
	 * @param to 元素的新位置
	 * @param fromElem 原元素
	 * @param toElem 目标元素
	 */
	void moveBucket(size_t from, size_t to, DHElem<E> *fromElem, DHElem<E> *toElem) {
		DHElem<E> *p;
		int hashCode = fromElem->hashCode;
		unsigned int bucket = getBucket(hashCode);
		if (bucket != from) {
			p = &m_data[bucket];
			while (p->next != (int)from) {
				bucket = p->next;
				p = &m_data[bucket];
			}
			p->next = (int)to;
		}
		*toElem = *fromElem;
	}

	/** 将原位于from的元素移到to，维持调整冲突链表结构
	 * 当知道元素所属冲突链表头时，使用本函数比使用2参数的moveBucket性能更高
	 *
	 * @param listHeader 元素所属冲突链表头
	 * @param from 元素原来所在位置
	 * @param to 元素的新位置
	 * @param fromElem 原元素
	 * @param toElem 目标元素
	 */
	void moveBucket(size_t listHeader, size_t from, size_t to, DHElem<E> *fromElem, DHElem<E> *toElem) {
		DHElem<E> *p;
		unsigned int bucket = (unsigned int)listHeader;
		if (bucket != from) {
			p = &m_data[bucket];
			while (p->next != (int)from) {
				bucket = p->next;
				p = &m_data[bucket];
			}
			p->next = (int)to;
		}
		*toElem = *fromElem;
	}

private:
	void init() {
		m_h1 = 1;
		m_h2 = 2;
		m_split = 0;
		m_size = 0;
	}

private:
	Array<DHElem<E> >	m_data;	/** 数据 */
	size_t	m_h1;				/** 哈希函数1 */
	size_t	m_h2;				/** 2 * m_h1，哈希函数2 */
	size_t	m_split;			/** 若桶号< m_split，表示该桶已经分裂，适合于哈希函数2，否则适合于哈希函数1 */
	size_t	m_size;				/** 当前大小，m_size = m_h1 + m_split */
	EH		m_entryHasher;		/** 用于根据项计算函数值的函数对象 */
	KH		m_keyHasher;		/** 用于根据键计算哈希值的函数对象 */
	KEC		m_equaler;			/** 用于比较键值相等的函数对象 */
};

/** 静态哈希表中的元素 */
template<typename K, typename V>
struct HashElem {
	K		key;
	V		value;
	int		next;
	bool	empty;
};

/** 静态哈希
 * 
 * 由于静态哈希通常只用于对大小可控的数据集进行索引（大小不可控的数据集会使用动态哈希表），
 * 因此，静态哈希最重要的是优化操作性能而非内存效率。
 * 静态哈希设计中最关键是的冲突处理算法的选择。实现最简单的是direct chaining
 * 技术，即属于同一哈希桶的元素使用链表维护，各哈希桶中只保存一个指针。
 * 这一方法搜索时至少会导致两次内存访问，缓存失效会比较高。
 * 另一种常用策略称为probing，也就是当发生冲突时，将数据存储在其它bucket中，
 * probing中的linear probing（即存储在下一个空闲bucket）技术具有良好的缓存访问性能，
 * 但实现比较复杂，并且在哈希表接近于满时的性能很差。
 *
 * 本类提供的哈希算法使用链表来处理冲突，但链表的第一个元素是内嵌在哈希桶数组之中，
 * 这样当冲突现象不怎么严重时，大部分搜索都只需要访问一次内存。
 * 冲突链表的后续元素存储在单独的溢出区中。
 *
 * @param K 键类型
 * @param V 值类型
 * @param H 哈希值计算函数对象类型，可以省略。若省略则由Hasher类提供
 * @param E 等值比较函数对象类型，可以省略。若省略则为std::equal_to
 */
template<typename K, typename V, typename H = Hasher<K>, typename E = std::equal_to<K> >
class Hash {
public:
	/**
	 * 创建一个静态哈希表
	 *
	 * @param capacity 哈希表大小
	 */
	Hash(size_t capacity) {
		m_capacity = capacity;
		m_size = 0;
		m_numBuckets = nextPrime(capacity);
		m_buckets = (HashElem<K, V> *)malloc(sizeof(HashElem<K, V>) * m_numBuckets);
		for (size_t i = 0; i < m_numBuckets; i++) {
			m_buckets[i].empty = true;
			m_buckets[i].next = -1;
		}
	}

	virtual ~Hash() {
		free(m_buckets);
		m_buckets = NULL;
	}

	/**
	 * 插入一个数据项
	 * 注：哈希表允许插入重复数据
	 *
	 * @param key 键值
	 * @param value 数据
	 * @return 是否成功
	 */
	bool put(const K &key, const V &value) {
		if (m_size >= m_capacity)
			return false;
		size_t bucket = m_hasher(key) % m_numBuckets;
		HashElem<K, V> *e = &m_buckets[bucket];
		if (e->empty) {
			assert(e->next == -1);
			e->key = key;
			e->value = value;
			e->empty = false;
		} else {
			// 发生了冲突
			size_t i = allocOverflowElem();
			HashElem<K, V> *newElem = &m_overflowArea[i];
			newElem->next = e->next;
			newElem->key = key;
			newElem->value = value;
			newElem->empty = false;
			e->next = (int)i;
		}
		m_size++;
		return true;
	}

	/**
	 * 查找。如果哈希表中存储多个拥有要搜索的键值的项，本函数返回其中一个
	 *
	 * @param key 要搜索的键值
	 * @return 找到返回数据，找不到返回(V)0
	 */
	V get(const K &key) {
		size_t bucket = m_hasher(key) % m_numBuckets;
		HashElem<K, V> *e = &m_buckets[bucket];
		if (!e->empty && m_equaler(e->key, key))
			return e->value;
		else if (e->empty || e->next < 0)
			return (V)0;
		else {
			e = &m_overflowArea[e->next];
			while (!m_equaler(e->key, key)) {
				if (e->next < 0)
					return (V)0;
				e = &m_overflowArea[e->next];
			}
			return e->value;
		}
	}

	/**
	 * 消除指定键值的项
	 * 
	 * @param key 要删除的键值
	 * @return 被删除的数据，不存在时返回(V)0
	 */
	V remove(const K &key) {
		size_t bucket = m_hasher(key) % m_numBuckets;
		HashElem<K, V> *e = &m_buckets[bucket];
		if (!e->empty && m_equaler(e->key, key)) {
			// 命中未冲突链表元素
			V ret = e->value;
			if (e->next >= 0) {
				int next = e->next;
				HashElem<K, V> *nextElem = &m_overflowArea[next];
				e->key = nextElem->key;
				e->value = nextElem->value;
				e->next = nextElem->next;
				freeOverflowElem(next);
			} else {
				e->empty = true;
				e->next = -1;
			}
			m_size--;
			return ret;
		} else if (e->empty)
			return (V)0;
		else {
			HashElem<K, V> *prev = e;
			e = &m_overflowArea[e->next];
			while (!m_equaler(e->key, key)) {
				if (e->next < 0)
					return (V)0;
				prev = e;
				e = &m_overflowArea[e->next];
			}
			// 命中冲突链表中的元素
			V ret = e->value;
			int curr = prev->next;
			prev->next = e->next;
			freeOverflowElem(curr);
			m_size--;
			return ret;
		}
	}
	
	/**
	 * 得到哈希表当前大小
	 *
	 * @return 哈希表当前大小
	 */
	size_t getSize() {
		return m_size;
	}

	/** 清空哈希表中所有数据 */
	void clear() {
		for (size_t i = 0; i < m_numBuckets; i++) {
			m_buckets[i].empty = true;
			m_buckets[i].next = -1;
		}
		m_overflowArea.clear();
		m_size = 0;
	}

	/**
	 * 得到哈希表中的所有元素
	 *
	 * @param keys 输出参数，用于存储每个元素的Key
	 * @param values 输出参数，用于存储每个元素的值
	 */
	void elements(K *keys, V *values) {
		for (size_t bucket = 0; bucket < m_numBuckets; bucket++) {
			HashElem<K, V> *e = &m_buckets[bucket];
			if (e->empty)
				continue;
			*keys = e->key;
			*values = e->value;
			keys++;
			values++;

			int overflow = e->next;
			while (overflow >= 0) {
				e = &m_overflowArea[overflow];
				assert(!e->empty);
				*keys = e->key;
				*values = e->value;
				keys++;
				values++;
				overflow = e->next;
			}
		}
	}

	/**
	 * 得到哈希表中的所有元素值
	 *
	 * @param values 输出参数，用于存储每个元素的值
	 */
	void values(V *values) {
		for (size_t bucket = 0; bucket < m_numBuckets; bucket++) {
			HashElem<K, V> *e = &m_buckets[bucket];
			if (e->empty)
				continue;
			*values = e->value;
			values++;

			int overflow = e->next;
			while (overflow >= 0) {
				e = &m_overflowArea[overflow];
				assert(!e->empty);
				*values = e->value;
				values++;
				overflow = e->next;
			}
		}
	}

private:
	/**
	 * 计算不小于n的最小一个素数
	 *
	 * @return 不小于n的最小素数
	 */
	size_t nextPrime(size_t n) {
		if (n % 2 == 0)
			n++;
		while (true) {
			if (isPrime(n))
				return n;
			n++;
		}
	}

	bool isPrime(size_t n) {
		for (size_t i = 2; i < (size_t)sqrt((double)n); i++)
			if (n % i == 0)
				return false;
		return true;
	}

	size_t allocOverflowElem() {
		return m_overflowArea.alloc();
	}

	void freeOverflowElem(size_t i) {
		m_overflowArea.free(i);
	}

private:
	size_t	m_size;					/** 当前大小，一定不超过容量 */
	size_t	m_capacity;				/** 哈希表容量 */
	size_t	m_numBuckets;			/** 桶数，总是大于或等于容量 */
	HashElem<K, V>	*m_buckets;		/** 哈希桶数组 */
	ObjectPool<HashElem<K, V> >	m_overflowArea;	/** 存储冲突链表的溢出数据 */
	H		m_hasher;				/** 用于计算哈希值的函数对象 */
	E		m_equaler;				/** 用于比较键值相等的函数对象 */
};

} // namespace ntse
#endif

