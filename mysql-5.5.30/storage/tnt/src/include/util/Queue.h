/**
* ʹ������ʵ�ֵĶ���
*
* @author ��ΰ��(liweizhao@corp.netease.com, liweizhao@163.org)
*/

#ifndef _NTSE_QUEUE_H_
#define _NTSE_QUEUE_H_

template<typename T>
class Queue {
public:
	/**
	* ����һ������
	* @param ������󳤶�
	*/
	Queue(size_t capacity) :m_capacity(capacity), m_size(0) {
		m_head = 0;
		m_array = new T[m_capacity];
	}
	/**
	* �ͷŶ���
	*/
	~Queue() {
		delete [] m_array;
		m_array = NULL;
	}
	/**
	* ��õ�ǰ���г���
	* @return
	*/
	inline size_t size() const {
		return m_size;
	}
	/**
	* �ڶ���β������һԪ��
	* @param ele Ҫ�����Ԫ��
	* @return �ɹ�����true������false
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
	* ��õ����Ƴ�����ͷԪ��
	* @return ����ͷԪ��
	*/
	inline T& peek() const {
		assert(m_size > 0);
		return m_array[m_head];
	}
	/**
	* ��ò��Ƴ�����ͷԪ��
	* @return ����ͷԪ��
	*/
	inline T& poll() {
		assert(m_size > 0);
		T& ele = m_array[m_head];
		m_head = (m_head + 1) % m_capacity ;
		--m_size;
		return ele;
	}
private:
	size_t   m_capacity;  /** ������󳤶� */
	size_t   m_size;      /** ���е�ǰ���� */
	size_t   m_head;      /** ����ͷ�±� */
	T        *m_array;    /** �洢�������ݵ����� */
};

#endif