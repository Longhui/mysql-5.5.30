/**
 * ��������ѹ���㷨
 *
 * @author ������(yulihua@corp.netease.com, ylh@163.org)
 */

#ifndef _NTSE_NUMBERCOMPRESSOR_H_
#define _NTSE_NUMBERCOMPRESSOR_H_

#include <assert.h>
#include <string.h>
#include "util/Portable.h"

namespace ntse {

/** 
 * ��������ǰ׺ѹ���� 
 * - ����ѹ��ǰ׺0�ֽڣ�����ѹ��ǰ׺-1�ֽڣ�
 * - ѹ�����ݸ�ʽΪ <HeadByte?><Compressed Data> 
 */
class NumberCompressor {
	/** ѹ������ͷ */
	struct HeadByte {
		/** �ȶ���ĳ�Ա�ڵ�λ */
		u8 m_data:3;	/** ��Ƕ����                                   */
		u8 m_len:4;		/** �����ֽ��������8�ֽ�, ����m_len=(15-mlen) */
		u8 m_sign:1;	/** ����λ����������Ϊ1������Ϊ0              */

		static const s8 INLINE_MIN = -7;     /** ��Ƕ�������ֵ */
		static const u8 INLINE_MAX = (u8)7;  /** ��Ƕ������Сֵ */
		static const u8 MAX_LENGTH = (u8)15; /** ��󳤶�       */
		static const u8 NEG_MASK = (u8)~INLINE_MAX; /** ��ѹ����ʱ������ȡ����Ƕ���� */

		/** ��ȡ�����ֽڳ��� */
		inline u8 dataSize() const {
			return m_sign == 1 ? m_len  : MAX_LENGTH - m_len;
		}
		/** ���ø�����m_len */
		inline void setNegSize(u8 len) {
			assert(m_sign == 0);
			assert(len <= 8);
			m_len = MAX_LENGTH - len;
		}
	};

public:
	/** ǰ׺ѹ��һ�����͡�������С�˱�ʾ������ô�˱�ʾ��ѹ��֮�������֧����memcmp�Ƚϴ�С
	 * @note �����з�������֧��ѹ������memcmp�Ƚϴ�С
	 
	 * @param input ��������
	 * @param inputSize ���δ�С
	 * @param output �������
	 * @param outCapacity ��������С
	 * @param outSize ѹ�������ݴ�С
	 */
	static inline bool compress(const byte* input, size_t inputSize, byte* output, size_t outCapacity, size_t* outSize) {
		assert(inputSize <= 8 && outCapacity >= 1);
		byte bigEndian[8];
		changeEndian(bigEndian, input, inputSize);
		input = bigEndian;

		HeadByte* hb = (HeadByte*)output;
		if ((s8)(*input) >= 0) { // 0��������
			// �ҵ���һ����0�ֽ�
			size_t start = 0;
			for (; start < inputSize && input[start] == 0; ++start)
				;
			hb->m_sign = 1;
			hb->m_len = (u8)(inputSize - start);
			if (hb->m_len) { // ������
				if (input[start] <= HeadByte::INLINE_MAX) { // ��һ���ֽڿ�����Ƕ
					hb->m_data = input[start];
					--hb->m_len;
					++start;
				} else { // ������Ƕ��������Ϊ0
					hb->m_data = 0;
				}
				if (outCapacity < (size_t)hb->m_len + 1) {
					assert(false);
					return false;
				}
				memcpy(output+1, input+start, hb->m_len);
			} else { // ��
				hb->m_data = 0;
			}
			*outSize = hb->m_len + 1;
		} else { // ����
			// �ҵ���һ����FF�ֽ�
			size_t start = 0;
			for (; start < inputSize && input[start] == 0xFF; ++start)
				;
			hb->m_sign = 0;
			hb->m_len = (u8)(inputSize - start);
			if (hb->m_len && (s8)(input[start]) >= HeadByte::INLINE_MIN 
				&& (s8)(input[start]) < 0) { // ��һ���ֽڿ�����Ƕ
					hb->m_data = input[start];
					--hb->m_len;
					++start;
			} else { // ������Ƕ��������m_data������Ϊ111
				hb->m_data = (u8)HeadByte::INLINE_MAX;
			}
			if (outCapacity < (size_t)hb->m_len + 1) {
				assert(false);
				return false;
			}
			memcpy(output+1, input+start, hb->m_len);
			*outSize = hb->m_len + 1;
			// ���������ĳ���
			hb->setNegSize(hb->m_len);
		}

		return true;
	}

	/** ��ѹһ�������������ô�˱�ʾ�������С�˱�ʾ
	 *
	 * @param input ѹ������
	 * @param inputSize ѹ������ ����
	 * @param output �������
	 * @param outputSize ��ѹ����������ֽ���
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

	/** ��ȡѹ������ռ�õ��ֽ���
	 * @param buf ѹ���������
	 * @return ռ���ֽ���
	 */
	static inline size_t sizeOfCompressed(const byte* buf) {
		HeadByte* hb = (HeadByte *)buf;
		return 1 + hb->dataSize();
	}
};

} // namespace ntse
#endif // _NTSE_NUMBERCOMPRESSOR_H_

