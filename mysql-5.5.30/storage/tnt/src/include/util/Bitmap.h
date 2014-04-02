/**
 * λͼ
 *
 * @author ������(yulihua@corp.netease.com, ylh@163.org)
 */


#ifndef _NTSE_BITMAP_H_
#define _NTSE_BITMAP_H_

#include <assert.h>
#include <string.h>
#include "util/Portable.h"

namespace ntse {


	
/** 
 * λͼ������
 */
class BitmapOper {
public:
	/** 
	 * ���λͼĳ��λ�Ƿ�Ϊ1
	 * @param bitmap λͼ�ڴ��ַ
	 * @param size λͼ��С(������)
	 * @param offset ƫ��
	 * @return ����true�����λ����1������false
	 */
	static bool isSet(const void* bitmap, size_t size, size_t offset) {
		UNREFERENCED_PARAMETER(size);
		assert (offset < size);
		assert (size % 8 == 0);
		return (((byte*)bitmap)[offset >> 3] & (1 << (offset & 7))) != 0;
	}
	
	/** 
	 * ����λͼĳ��λΪ1
	 * @param bitmap λͼ�ڴ��ַ
	 * @param size λͼ��С(������)
	 * @param offset ƫ��
	 */
	inline static void setBit(void* bitmap, size_t size, size_t offset) {
		UNREFERENCED_PARAMETER(size);
		assert (offset < size);
		assert (size % 8 == 0);
		((byte*)bitmap)[offset >> 3] |= (1 << (offset & 7));
	}
	
	/** 
	 * ����λͼĳ��λΪ0
	 * @param bitmap λͼ�ڴ��ַ
	 * @param size λͼ��С(������)
	 * @param offset ƫ��
	 */
	inline static void clearBit(void* bitmap, size_t size, size_t offset) {
		UNREFERENCED_PARAMETER(size);
		assert (offset < size);
		assert (size % 8 == 0);
		((byte*)bitmap)[offset >> 3] &= ~(1 << (offset & 7));
	}
	/** 
	 * ��ȡ��һ��Ϊ1��λ 
	 * @param bitmap λͼ�ڴ��ַ
	 * @param size λͼ��С(������)
	 * @param offset ��ʼƫ�ƣ������￪ʼ����������offset��Ӧ��λ
	 * @return �ҵ�������ƫ�ƣ����򷵻�-1
	 */
	inline static size_t nextSet(void* bitmap, size_t size, size_t offset) {
		UNREFERENCED_PARAMETER(bitmap);
		UNREFERENCED_PARAMETER(size);
		UNREFERENCED_PARAMETER(offset);
		assert (size % 8 == 0);
		assert (offset < size);
		byte *data = (byte*)bitmap;
		size_t bmBytes = size >> 3;			// λͼ�ֽ���
		size_t byteOffset = offset >> 3;	// �ֽ�ƫ��
		size_t bitOffset = offset & 7; // �����ֽ���ƫ��
		
		while (byteOffset < bmBytes) {
			byte curByte = (byte)(data[byteOffset] >> bitOffset);
			if (curByte != 0) { // ��0�ֽ�
				size_t shifted = 0;
				do {
					if (curByte & 1) // ���λΪ1
						return (int)((byteOffset << 3) + bitOffset + shifted);
					curByte >>= 1;
					++shifted;
				} while(curByte);
			} else {
				bitOffset = 0;
			}
			++byteOffset;
		}
		return (size_t)-1;
	}

	/**
	 * �ж�λͼ�Ƿ�ȫ��
	 * @param bitmap λͼ�ڴ��ַ
	 * @param bmBytes λͼ�ֽ�������
	 * @return λͼ�Ƿ�ȫ��
	 */
	inline static bool isZero(void* bitmap, size_t bmBytes) {
		switch(bmBytes) { // make the common case fast
			case 1:
				return *(u8 *)bitmap == 0;
			case 2:
				return *(u16 *)bitmap == 0;
		}
		int n = (int)bmBytes;
		while (--n >= 0 && *((u8 *)bitmap + n) == 0)
			;
		return n < 0;
	}
};


/** 
 * λͼ��
 */
class Bitmap {
public:
	/**
	 * ����λͼ
	 * @param size λͼ����(���м�λ)
	 */
	Bitmap(size_t size) {
		m_bitmap = new byte[(size + 7) / 8];
		memset(m_bitmap, 0, (size + 7) / 8);
		m_size = size;
		m_myMemory = true;
	}
	/**
	 * ����λͼ, λͼ�ڴ�ʹ�ò���ָ����ַ
	 * @param buf  λͼ�ڴ�
	 * @param size λͼ����(���м�λ)
	 */
	Bitmap(void* buf, size_t size) {
		m_myMemory = false;
		m_bitmap = (byte*)buf;
		m_size = size;
	}

	/**
	 * ����λͼ
	 * ���ڲ���Bitmap(void* buf, uint size)����Ķ��󣬲�ɾ���ڴ�
	 */ 
	~Bitmap() {
		if (m_myMemory)
			delete[] m_bitmap;
	}

	/**
	 * ��n��bit�Ƿ���1
	 *
	 * @param n ��n��λ����0��ʼ
	 * @return ��n��bit�Ƿ���1
	 */
	inline bool isSet(size_t n) {
		return BitmapOper::isSet(m_bitmap, m_size, n);
	}

	/**
	 * ���õ�n��bitΪ1 
	 *
	 * @param n ��n��λ����0��ʼ
	 */
	inline void setBit(size_t n) {
		BitmapOper::setBit(m_bitmap, m_size, n);
	}

	/**
	 * ���õ�n��bitΪ0 
	 *
	 * @param n ��n��λ����0��ʼ
	 */
	void clearBit(size_t n) {
		BitmapOper::clearBit(m_bitmap, m_size, n);
	}


private:
	byte*	m_bitmap;	/** λͼ�ڴ�ռ� */
	size_t  m_size;		/** λͼ������ */
	bool	m_myMemory;	/** �ڴ�����־��true��ʾ�ڴ��Ƕ�̬��������� */
};

}// namespace ntse 

#endif // _NTSE_BITMAP_H_


