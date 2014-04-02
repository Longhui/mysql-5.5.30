/**
 * ��ϣ��
 *
 * @author ��Դ(wangyuan@corp.netease.com, wy@163.org)
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
 * Ϊ���������ṩ�����ϣֵ���ܵĺ�������
 * �����������͹�ϣ�����ļ���ο�Javaʵ��
 * 
 * @param T ��������
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
 * Ϊ���������ṩ��ֵ�Ƚϵĺ�������
 *
 * @param T1 ����1����
 * @param T2 ����2����
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

/** �����Ǵ�Сд�������ַ�����ϣֵ�ĺ������� */
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

/** �����Ǵ�Сд���Ƚ������ַ����Ƿ���ȵĺ������� */
class StrNoCaseEqualer {
public:
	inline bool operator()(const char *s1, const char *s2) const {
		return System::stricmp(s1, s2) == 0;
	}
};

/** ��̬��ϣ���е�Ԫ�� */
template<typename E>
struct DHElem {
	E		entry;		/** ����� */
	unsigned int	hashCode;	/** ��ϣֵ */
	int		next;		/** ��ͻ��������һ�ڵ�λ�ã�������ʾû����һ�ڵ� */
};

/** ��̬��ϣ��ʹ�����Թ�ϣ�㷨ʵ�֣�ÿ��ֻ����չһ��Ͱ����һ��Ͱ�е���������
 * �����ϣֵ��
 *
 * @param K ������
 * @param E ������
 * @param EH ����������ϣֵ�Ķ������ͣ�����ʡ�ԡ���ʡ������Hasher<K>���ṩ
 * @param EH ���ݼ������ϣֵ�Ķ������ͣ�����ʡ�ԡ���ʡ������Hasher<K>���ṩ
 * @param KEC �Ƚ�����ֵ�Ƿ���ȵıȽϺ����������ͣ�����ʡ�ԡ���ʡ����Ϊ
 *   Equaler<K, E>
 */
template<typename K, typename E, typename EH = Hasher<E>, 
	typename KH = Hasher<K>, typename KEC = Equaler<K, E> >
class DynHash {
public:
	/**
	 * ����һ����ʹ���ڴ�ҳ�صĶ�̬��ϣ��
	 */
	DynHash(): m_data() {
		init();
	}

	/**
	 * ����һ��ʹ���ڴ�ҳ�صĶ�̬��ϣ��
	 * 
	 * @param poolUser �ڴ�ҳ���û�
	 * @param pageType ��ʹ�õ�ҳ����
	 */
	DynHash(PagePoolUser *poolUser, PageType pageType): m_data(poolUser, pageType) {
		init();
	}

	/** ����һ��������
	 * 
	 * @param entry ��������а�������ֵ
	 * @return �ɹ����
	 */
	bool put(const E &entry) {
		int hashCode = m_entryHasher(entry);
		unsigned int bucket = getBucket(hashCode);
		if (m_size == 0) {
			if (!m_data.expand())
				return false;
			setElem(&m_data[m_size], entry, hashCode, -1);
		} else {
			// ����������ݣ���ʱ���������
			// 1. ����������������ԭ���ݴ�����ͬһ��ͻ������ʱ�����ԭ����
			// һ���ǳ�ͻ����ͷ����ʱӦ�ý����������ݴ洢�����ͬʱ��������
			// ���ݲ��뵽��ͻ����ĵڶ���
			// 2. ����������������ԭ���ݲ�����ͬһ��ͻ������ʱ�����ԭ����
			// һ��������������ͻ����ͷ����ʱӦ�ý�ԭ����������Ƶ����
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

			// ����m_split��Ӧ��Ͱ����Щ�ڵ㽫�ֲ���m_split��m_h1 + m_split
			// (��m_size)������Ͱ�С�����m_splitͰ�ĳ�ͻ�������ڷ��Ѻ�������
			// m_split�Ľڵ㲻�������ڷ��Ѻ�����m_h1 + m_split(m_size)��
			// �ڵ㣬����һ�ڵ�ӳ�ͻ������ɾ���������뵽m_h1 + m_split
			// ������
			DHElem<E> *currElem = &m_data[m_split], *nextElem, *prevElem = NULL;
			if ((currElem->hashCode & (m_h1 - 1)) == m_split) {	
				// m_split�������ڳ�ͻ�����ͷ
				int currPos = (int)m_split, nextPos, prevPos = -1;
				bool newListIsEmpty = true;	// m_h1 + m_split�����Ƿ�Ϊ��
				while (currPos >= 0) {
					nextPos = currElem->next;
					nextElem = NULL;
					if ((currElem->hashCode & (m_h2 - 1)) 
						== m_h1 + m_split) {
						DHElem<E> currElemSave = *currElem;
						// ����ڵ�Ӧ�ô�����������
						if (newListIsEmpty) {
							// ��m_h1 + m_split����Ϊ�գ�����Ҫ����ǰ�ڵ�
							// �Ƶ�m_size��Ϊ�������ͷ�ڵ�
							// �����ߵ���m_split����ͷ�������к�һ�ڵ㣬Ӧ�ý�
							// ��һ�ڵ��Ƶ���ǰλ�ã�Ȼ��m_size�Ƶ���һ�ڵ�
							// ����λ�ã���û�к�һ�ڵ㣬Ӧ��m_size�Ƶ���ǰλ��
							if (currPos == (int)m_split) {
								// ��ǰ�ڵ���m_split����ͷ
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
							// m_h1 + m_split����ǿ�
							if (prevPos < 0 && nextPos >= 0) {
								// ����m_split������ͷ�������к�һ�ڵ㣬�򽫺�һ
								// �ڵ��Ƶ���ǰλ�ã�Ȼ�󽫵�ǰ�ڵ���뵽��һ
								// λ�ã������뵽m_size����ĵڶ����ڵ�
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

	/** �������������ȵ�������
	 * 
	 * @param key Ҫ������������
	 * @return ����ϣ���д�����key��ȵ��������򷵻ظ���������򷵻�(E)0
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
	 * ��ȡλ��Ϊi��������
	 *
	 * @param i ��ȡλ�ã���[0, m_size)֮��
	 */
	E getAt(size_t i) const {
		assert(i < m_size);
		return m_data[i].entry;
	}

	/** ɾ���������ֵ��ȵ�������
	 * 
	 * @param key Ҫɾ���ļ�ֵ
	 * @return ����ϣ���д�����key��ȵ��������򷵻ظ���������򷵻�(E)0
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
		// ɾ���ҵ��Ľڵ㣬���ҵ��Ľڵ㲻��������ͻ���������ͷ�����������
		// ɾ����һ�ڵ㣬������Ҫ����ͻ����ĺ�һ���ڵ�Ų����ǰλ��
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
				// ��m_size��ͻ����ǿգ���ϲ�m_size��oldList��oldList
				DHElem<E> *olElem = &m_data[oldList];
				if (oldList == deletedPos) {
					// ��ʱoldList��ͻ����Ϊ�գ�����m_size�ǲ��������ڳ�ͻ����
					// ��ͷ��ֻ��Ҫ�����Ƶ�oldList����
					moveBucket(m_size, oldList, &m_data[m_size], olElem);
				} else if (getBucket(olElem->hashCode) == oldList) {
					// oldList�ϵ�������Ҫ�ϲ��ĳ�ͻ����ͷ
					DHElem<E> *sizeElem = &m_data[m_size];
					if (getBucket(sizeElem->hashCode) == m_size) {
						// Ҫ�ϲ���������ͻ������Ϊ��
						m_data[deletedPos] = *sizeElem;
						int tail = (int)oldList;
						while (olElem->next >= 0) {
							tail = olElem->next;
							olElem = &m_data[tail];
						}
						olElem->next = deletedPos;
					} else {
						// m_size����Ϊ��
						moveBucket(m_size, deletedPos, sizeElem, &m_data[deletedPos]);
					}
				} else {
					// oldList����Ϊ��
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
	 * �õ���ϣ��ǰ��С
	 *
	 * @return ��ϣ��ǰ��С
	 */
	size_t getSize() const {
		return m_size;
	}

	/**
	 * �趨Ԥ���ռ�
	 *
	 * @param size Ԥ���ռ�
	 * @param forceAllocForPool ʹ���ڴ�ҳ�ط���ռ�ʱ�Ƿ���allocPageForce�ӿ�
	 * @return �Ƿ�ɹ�
	 */
	bool reserveSize(size_t size, bool forceAllocForPool = false) {
		return m_data.setReservedSize(size, forceAllocForPool);
	}

	/** ��չ�ϣ������������ */
	void clear() {
		m_data.clear();
		init();
	}

	/** ��ù�ϣ��ռ�õ��ڴ��С
	 * @param ռ�õ��ڴ��С
	 */
	size_t getMemUsage() const {
		return m_data.getMemUsage();
	}

	/**
	 * ����ϣ����ȷ�ԡ�ֻ�ڹ�ϣ��Ԫ���Թ�����ʹ��
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
	 * ��ù�ϣ���ͻ���ͳ����Ϣ
	 *
	 * @param avgConflictLen OUT����ͻ����ƽ������
	 * @param maxConflictLen OUT����ͻ������󳤶�
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
	 * ��ӡ��ϣ�����ݵ���׼���
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
		if (bucket < m_split)	// ���Ͱ�Ѿ����ѹ�
			bucket = hashCode & (m_h2 - 1);
		return bucket;
	}

	inline void setElem(DHElem<E> *elem, const E &entry, int hashCode, int next) {
		elem->entry = entry;
		elem->hashCode = hashCode;
		elem->next = next;
	}

	/** ��ԭλ��from��Ԫ���Ƶ�to��ά�ֵ�����ͻ����ṹ
	 * ��֪��Ԫ��������ͻ����ͷʱ����ʹ��3������moveBucket���ܸ���
	 *
	 * @param from Ԫ��ԭ������λ��
	 * @param to Ԫ�ص���λ��
	 * @param fromElem ԭԪ��
	 * @param toElem Ŀ��Ԫ��
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

	/** ��ԭλ��from��Ԫ���Ƶ�to��ά�ֵ�����ͻ����ṹ
	 * ��֪��Ԫ��������ͻ����ͷʱ��ʹ�ñ�������ʹ��2������moveBucket���ܸ���
	 *
	 * @param listHeader Ԫ��������ͻ����ͷ
	 * @param from Ԫ��ԭ������λ��
	 * @param to Ԫ�ص���λ��
	 * @param fromElem ԭԪ��
	 * @param toElem Ŀ��Ԫ��
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
	Array<DHElem<E> >	m_data;	/** ���� */
	size_t	m_h1;				/** ��ϣ����1 */
	size_t	m_h2;				/** 2 * m_h1����ϣ����2 */
	size_t	m_split;			/** ��Ͱ��< m_split����ʾ��Ͱ�Ѿ����ѣ��ʺ��ڹ�ϣ����2�������ʺ��ڹ�ϣ����1 */
	size_t	m_size;				/** ��ǰ��С��m_size = m_h1 + m_split */
	EH		m_entryHasher;		/** ���ڸ�������㺯��ֵ�ĺ������� */
	KH		m_keyHasher;		/** ���ڸ��ݼ������ϣֵ�ĺ������� */
	KEC		m_equaler;			/** ���ڱȽϼ�ֵ��ȵĺ������� */
};

/** ��̬��ϣ���е�Ԫ�� */
template<typename K, typename V>
struct HashElem {
	K		key;
	V		value;
	int		next;
	bool	empty;
};

/** ��̬��ϣ
 * 
 * ���ھ�̬��ϣͨ��ֻ���ڶԴ�С�ɿص����ݼ�������������С���ɿص����ݼ���ʹ�ö�̬��ϣ����
 * ��ˣ���̬��ϣ����Ҫ�����Ż��������ܶ����ڴ�Ч�ʡ�
 * ��̬��ϣ�������ؼ��ǵĳ�ͻ�����㷨��ѡ��ʵ����򵥵���direct chaining
 * ������������ͬһ��ϣͰ��Ԫ��ʹ������ά��������ϣͰ��ֻ����һ��ָ�롣
 * ��һ��������ʱ���ٻᵼ�������ڴ���ʣ�����ʧЧ��Ƚϸߡ�
 * ��һ�ֳ��ò��Գ�Ϊprobing��Ҳ���ǵ�������ͻʱ�������ݴ洢������bucket�У�
 * probing�е�linear probing�����洢����һ������bucket�������������õĻ���������ܣ�
 * ��ʵ�ֱȽϸ��ӣ������ڹ�ϣ��ӽ�����ʱ�����ܺܲ
 *
 * �����ṩ�Ĺ�ϣ�㷨ʹ�������������ͻ��������ĵ�һ��Ԫ������Ƕ�ڹ�ϣͰ����֮�У�
 * ��������ͻ������ô����ʱ���󲿷�������ֻ��Ҫ����һ���ڴ档
 * ��ͻ����ĺ���Ԫ�ش洢�ڵ�����������С�
 *
 * @param K ������
 * @param V ֵ����
 * @param H ��ϣֵ���㺯���������ͣ�����ʡ�ԡ���ʡ������Hasher���ṩ
 * @param E ��ֵ�ȽϺ����������ͣ�����ʡ�ԡ���ʡ����Ϊstd::equal_to
 */
template<typename K, typename V, typename H = Hasher<K>, typename E = std::equal_to<K> >
class Hash {
public:
	/**
	 * ����һ����̬��ϣ��
	 *
	 * @param capacity ��ϣ���С
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
	 * ����һ��������
	 * ע����ϣ����������ظ�����
	 *
	 * @param key ��ֵ
	 * @param value ����
	 * @return �Ƿ�ɹ�
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
			// �����˳�ͻ
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
	 * ���ҡ������ϣ���д洢���ӵ��Ҫ�����ļ�ֵ�����������������һ��
	 *
	 * @param key Ҫ�����ļ�ֵ
	 * @return �ҵ��������ݣ��Ҳ�������(V)0
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
	 * ����ָ����ֵ����
	 * 
	 * @param key Ҫɾ���ļ�ֵ
	 * @return ��ɾ�������ݣ�������ʱ����(V)0
	 */
	V remove(const K &key) {
		size_t bucket = m_hasher(key) % m_numBuckets;
		HashElem<K, V> *e = &m_buckets[bucket];
		if (!e->empty && m_equaler(e->key, key)) {
			// ����δ��ͻ����Ԫ��
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
			// ���г�ͻ�����е�Ԫ��
			V ret = e->value;
			int curr = prev->next;
			prev->next = e->next;
			freeOverflowElem(curr);
			m_size--;
			return ret;
		}
	}
	
	/**
	 * �õ���ϣ��ǰ��С
	 *
	 * @return ��ϣ��ǰ��С
	 */
	size_t getSize() {
		return m_size;
	}

	/** ��չ�ϣ������������ */
	void clear() {
		for (size_t i = 0; i < m_numBuckets; i++) {
			m_buckets[i].empty = true;
			m_buckets[i].next = -1;
		}
		m_overflowArea.clear();
		m_size = 0;
	}

	/**
	 * �õ���ϣ���е�����Ԫ��
	 *
	 * @param keys ������������ڴ洢ÿ��Ԫ�ص�Key
	 * @param values ������������ڴ洢ÿ��Ԫ�ص�ֵ
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
	 * �õ���ϣ���е�����Ԫ��ֵ
	 *
	 * @param values ������������ڴ洢ÿ��Ԫ�ص�ֵ
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
	 * ���㲻С��n����Сһ������
	 *
	 * @return ��С��n����С����
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
	size_t	m_size;					/** ��ǰ��С��һ������������ */
	size_t	m_capacity;				/** ��ϣ������ */
	size_t	m_numBuckets;			/** Ͱ�������Ǵ��ڻ�������� */
	HashElem<K, V>	*m_buckets;		/** ��ϣͰ���� */
	ObjectPool<HashElem<K, V> >	m_overflowArea;	/** �洢��ͻ������������ */
	H		m_hasher;				/** ���ڼ����ϣֵ�ĺ������� */
	E		m_equaler;				/** ���ڱȽϼ�ֵ��ȵĺ������� */
};

} // namespace ntse
#endif

