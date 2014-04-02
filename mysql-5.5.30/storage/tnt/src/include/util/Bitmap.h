/**
 * 位图
 *
 * @author 余利华(yulihua@corp.netease.com, ylh@163.org)
 */


#ifndef _NTSE_BITMAP_H_
#define _NTSE_BITMAP_H_

#include <assert.h>
#include <string.h>
#include "util/Portable.h"

namespace ntse {


	
/** 
 * 位图操作类
 */
class BitmapOper {
public:
	/** 
	 * 检查位图某个位是否为1
	 * @param bitmap 位图内存地址
	 * @param size 位图大小(比特数)
	 * @param offset 偏移
	 * @return 返回true如果该位等于1，否则false
	 */
	static bool isSet(const void* bitmap, size_t size, size_t offset) {
		UNREFERENCED_PARAMETER(size);
		assert (offset < size);
		assert (size % 8 == 0);
		return (((byte*)bitmap)[offset >> 3] & (1 << (offset & 7))) != 0;
	}
	
	/** 
	 * 设置位图某个位为1
	 * @param bitmap 位图内存地址
	 * @param size 位图大小(比特数)
	 * @param offset 偏移
	 */
	inline static void setBit(void* bitmap, size_t size, size_t offset) {
		UNREFERENCED_PARAMETER(size);
		assert (offset < size);
		assert (size % 8 == 0);
		((byte*)bitmap)[offset >> 3] |= (1 << (offset & 7));
	}
	
	/** 
	 * 设置位图某个位为0
	 * @param bitmap 位图内存地址
	 * @param size 位图大小(比特数)
	 * @param offset 偏移
	 */
	inline static void clearBit(void* bitmap, size_t size, size_t offset) {
		UNREFERENCED_PARAMETER(size);
		assert (offset < size);
		assert (size % 8 == 0);
		((byte*)bitmap)[offset >> 3] &= ~(1 << (offset & 7));
	}
	/** 
	 * 获取下一个为1的位 
	 * @param bitmap 位图内存地址
	 * @param size 位图大小(比特数)
	 * @param offset 起始偏移，从这里开始搜索，包含offset对应的位
	 * @return 找到：返回偏移；否则返回-1
	 */
	inline static size_t nextSet(void* bitmap, size_t size, size_t offset) {
		UNREFERENCED_PARAMETER(bitmap);
		UNREFERENCED_PARAMETER(size);
		UNREFERENCED_PARAMETER(offset);
		assert (size % 8 == 0);
		assert (offset < size);
		byte *data = (byte*)bitmap;
		size_t bmBytes = size >> 3;			// 位图字节数
		size_t byteOffset = offset >> 3;	// 字节偏移
		size_t bitOffset = offset & 7; // 计算字节内偏移
		
		while (byteOffset < bmBytes) {
			byte curByte = (byte)(data[byteOffset] >> bitOffset);
			if (curByte != 0) { // 非0字节
				size_t shifted = 0;
				do {
					if (curByte & 1) // 最低位为1
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
	 * 判定位图是否全零
	 * @param bitmap 位图内存地址
	 * @param bmBytes 位图字节数！！
	 * @return 位图是否全零
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
 * 位图类
 */
class Bitmap {
public:
	/**
	 * 创建位图
	 * @param size 位图长度(共有几位)
	 */
	Bitmap(size_t size) {
		m_bitmap = new byte[(size + 7) / 8];
		memset(m_bitmap, 0, (size + 7) / 8);
		m_size = size;
		m_myMemory = true;
	}
	/**
	 * 创建位图, 位图内存使用参数指定地址
	 * @param buf  位图内存
	 * @param size 位图长度(共有几位)
	 */
	Bitmap(void* buf, size_t size) {
		m_myMemory = false;
		m_bitmap = (byte*)buf;
		m_size = size;
	}

	/**
	 * 销毁位图
	 * 对于采用Bitmap(void* buf, uint size)构造的对象，不删除内存
	 */ 
	~Bitmap() {
		if (m_myMemory)
			delete[] m_bitmap;
	}

	/**
	 * 第n个bit是否是1
	 *
	 * @param n 第n个位，从0开始
	 * @return 第n个bit是否是1
	 */
	inline bool isSet(size_t n) {
		return BitmapOper::isSet(m_bitmap, m_size, n);
	}

	/**
	 * 设置第n个bit为1 
	 *
	 * @param n 第n个位，从0开始
	 */
	inline void setBit(size_t n) {
		BitmapOper::setBit(m_bitmap, m_size, n);
	}

	/**
	 * 设置第n个bit为0 
	 *
	 * @param n 第n个位，从0开始
	 */
	void clearBit(size_t n) {
		BitmapOper::clearBit(m_bitmap, m_size, n);
	}


private:
	byte*	m_bitmap;	/** 位图内存空间 */
	size_t  m_size;		/** 位图比特数 */
	bool	m_myMemory;	/** 内存分配标志，true表示内存是动态分配出来的 */
};

}// namespace ntse 

#endif // _NTSE_BITMAP_H_


