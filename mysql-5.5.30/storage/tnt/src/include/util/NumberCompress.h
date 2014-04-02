/**
 * 有序整数压缩算法
 *
 * @author 余利华(yulihua@corp.netease.com, ylh@163.org)
 */

#ifndef _NTSE_NUMBERCOMPRESSOR_H_
#define _NTSE_NUMBERCOMPRESSOR_H_

#include <assert.h>
#include <string.h>
#include "util/Portable.h"

namespace ntse {

/** 
 * 有序整数前缀压缩类 
 * - 正数压缩前缀0字节，负数压缩前缀-1字节；
 * - 压缩数据格式为 <HeadByte?><Compressed Data> 
 */
class NumberCompressor {
	/** 压缩数据头 */
	struct HeadByte {
		/** 先定义的成员在低位 */
		u8 m_data:3;	/** 内嵌数据                                   */
		u8 m_len:4;		/** 后续字节数，最多8字节, 负数m_len=(15-mlen) */
		u8 m_sign:1;	/** 符号位，正数和零为1，负数为0              */

		static const s8 INLINE_MIN = -7;     /** 内嵌数据最大值 */
		static const u8 INLINE_MAX = (u8)7;  /** 内嵌数据最小值 */
		static const u8 MAX_LENGTH = (u8)15; /** 最大长度       */
		static const u8 NEG_MASK = (u8)~INLINE_MAX; /** 解压负数时，用于取出内嵌数据 */

		/** 获取后续字节长度 */
		inline u8 dataSize() const {
			return m_sign == 1 ? m_len  : MAX_LENGTH - m_len;
		}
		/** 设置负数的m_len */
		inline void setNegSize(u8 len) {
			assert(m_sign == 0);
			assert(len <= 8);
			m_len = MAX_LENGTH - len;
		}
	};

public:
	/** 前缀压缩一个整型。输入用小端表示，输出用大端表示，压缩之后的数据支持用memcmp比较大小
	 * @note 仅对有符号整数支持压缩后用memcmp比较大小
	 
	 * @param input 输入整形
	 * @param inputSize 整形大小
	 * @param output 输出缓存
	 * @param outCapacity 输出缓存大小
	 * @param outSize 压缩后数据大小
	 */
	static inline bool compress(const byte* input, size_t inputSize, byte* output, size_t outCapacity, size_t* outSize) {
		assert(inputSize <= 8 && outCapacity >= 1);
		byte bigEndian[8];
		changeEndian(bigEndian, input, inputSize);
		input = bigEndian;

		HeadByte* hb = (HeadByte*)output;
		if ((s8)(*input) >= 0) { // 0或者正数
			// 找到第一个非0字节
			size_t start = 0;
			for (; start < inputSize && input[start] == 0; ++start)
				;
			hb->m_sign = 1;
			hb->m_len = (u8)(inputSize - start);
			if (hb->m_len) { // 不是零
				if (input[start] <= HeadByte::INLINE_MAX) { // 第一个字节可以内嵌
					hb->m_data = input[start];
					--hb->m_len;
					++start;
				} else { // 不能内嵌，则设置为0
					hb->m_data = 0;
				}
				if (outCapacity < (size_t)hb->m_len + 1) {
					assert(false);
					return false;
				}
				memcpy(output+1, input+start, hb->m_len);
			} else { // 零
				hb->m_data = 0;
			}
			*outSize = hb->m_len + 1;
		} else { // 负数
			// 找到第一个非FF字节
			size_t start = 0;
			for (; start < inputSize && input[start] == 0xFF; ++start)
				;
			hb->m_sign = 0;
			hb->m_len = (u8)(inputSize - start);
			if (hb->m_len && (s8)(input[start]) >= HeadByte::INLINE_MIN 
				&& (s8)(input[start]) < 0) { // 第一个字节可以内嵌
					hb->m_data = input[start];
					--hb->m_len;
					++start;
			} else { // 不能内嵌，则设置m_data二进制为111
				hb->m_data = (u8)HeadByte::INLINE_MAX;
			}
			if (outCapacity < (size_t)hb->m_len + 1) {
				assert(false);
				return false;
			}
			memcpy(output+1, input+start, hb->m_len);
			*outSize = hb->m_len + 1;
			// 纠正负数的长度
			hb->setNegSize(hb->m_len);
		}

		return true;
	}

	/** 解压一个整数。输入用大端表示，输出用小端表示
	 *
	 * @param input 压缩数据
	 * @param inputSize 压缩数据 长度
	 * @param output 输出缓存
	 * @param outputSize 解压后的整数的字节数
	 */
	static inline void decompress(const byte* input, size_t inputSize, byte* output, size_t outputSize) {
		HeadByte* hb = (HeadByte *)input;
		assert(hb->dataSize() <= outputSize + 1);
		byte *outputGuard = output + outputSize;
		const byte *p = input + inputSize - 1;
		while (p >= input + 1)
			*output++ = *p--;
		if (output == outputGuard)
			return;
		u8 sign = hb->m_sign;
		*output++ = (HeadByte::NEG_MASK & (sign - 1)) | hb->m_data;
		while (output < outputGuard)
			*output++ = sign - 1;
	}

	/** 获取压缩整数占用的字节数
	 * @param buf 压缩后的整数
	 * @return 占用字节数
	 */
	static inline size_t sizeOfCompressed(const byte* buf) {
		HeadByte* hb = (HeadByte *)buf;
		return 1 + hb->dataSize();
	}
};

} // namespace ntse
#endif // _NTSE_NUMBERCOMPRESSOR_H_

