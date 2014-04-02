/**
* 使用数组实现的队列
*
* @author 李伟钊(liweizhao@corp.netease.com, liweizhao@163.org)
*/

#ifndef _NTSE_QUEUE_H_
#define _NTSE_QUEUE_H_

template<typename T>
class Queue {
public:
	/**
	* 构造一个队列
	* @param 队列最大长度
	*/
	Queue(size_t capacity) :m_capacity(capacity), m_size(0) {
		m_head = 0;
		m_array = new T[m_capacity];
	}
	/**
	* 释放队列
	*/
	~Queue() {
		delete [] m_array;
		m_array = NULL;
	}
	/**
	* 获得当前队列长度
	* @return
	*/
	inline size_t size() const {
		return m_size;
	}
	/**
	* 在队列尾部插入一元素
	* @param ele 要插入的元素
	* @return 成功返回true，否则false
	*/
	inline bool offer(const T& ele) {
		if (m_size < m_capacity) {
			m_array[(m_head + m_size) % m_capacity] = ele;
			++m_size;
			return true;
		} else
			return false;
	}
	/**
	* 获得但不移除队列头元素
	* @return 队列头元素
	*/
	inline T& peek() const {
		assert(m_size > 0);
		return m_array[m_head];
	}
	/**
	* 获得并移除队列头元素
	* @return 队列头元素
	*/
	inline T& poll() {
		assert(m_size > 0);
		T& ele = m_array[m_head];
		m_head = (m_head + 1) % m_capacity ;
		--m_size;
		return ele;
	}
private:
	size_t   m_capacity;  /** 队列最大长度 */
	size_t   m_size;      /** 队列当前长度 */
	size_t   m_head;      /** 队列头下标 */
	T        *m_array;    /** 存储队列数据的数组 */
};

#endif