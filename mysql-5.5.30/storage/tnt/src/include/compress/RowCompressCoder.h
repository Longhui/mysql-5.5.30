/**
 * 记录压缩编解码
 *
 * @author 李伟钊(liweizhao@corp.netease.com)
 */
#ifndef _NTSE_ROW_COMPRESS_CODER_H_
#define _NTSE_ROW_COMPRESS_CODER_H_

#include "misc/Global.h"

using namespace std;

namespace ntse {

	/** 编码标志位 */
	enum CodedFlag {
		UNCODE_2BITS_FLAG = 0,    /* 半字节未编码标志00 */
		ONE_BYTE_FLAG = 1,        /* 1字节编码标志01 */
		TWO_BYTE_FLAG = 2,        /* 2字节编码标志10 */
		UNCODE_4BITS_FLAG = 3,    /* 1字节未编码标志11 */
	};

	/** 编码字节数 */
	enum CodedBytes {
		ONE_BYTE = 1,             /* 1字节 */
		TWO_BYTE = 2,             /* 2字节 */
	};

	const uint CODED_ONE_BYTE_MAXNUM = (1 << 8) - 1;   /** 1字节编码最大值 */
	const uint CODED_TWO_BYTE_MAXNUM = (1 << 16) - 1;  /** 2字节编码最大值 */
	const uint UNCODED_LEN_2BITS_MAXNUM = 1 << 2;
	const uint UNCODED_LEN_4BITS_MAXNUM = UNCODED_LEN_2BITS_MAXNUM + (1 << 4);

	class RCMIntegerConverter {
	public:
		static void codeIntToCodedBytes(const uint& in, CodedFlag flag, byte* out) {
			switch (flag)
			{
			case ONE_BYTE_FLAG:
				assert(in <= CODED_ONE_BYTE_MAXNUM);
				out[0] = (u8)in;			
				break;
			case TWO_BYTE_FLAG:
				assert(in <= CODED_TWO_BYTE_MAXNUM);
				write2BytesLittleEndian(out, in);
				break;
			default:
				NTSE_ASSERT(false);
			}
		}

		static void decodeBytesToInt(const byte* in, const uint& offset, CodedBytes len, uint* out) {
			assert (in != NULL);
			assert (out != NULL);
			switch (len) 
			{
			case ONE_BYTE:
				decodeOneByteToInt(in, offset, out);
				break;
			case TWO_BYTE:
				decodeTwoBytesToInt(in, offset, out);
				break;
			default:
				NTSE_ASSERT(false);
			}
		}

		static void decodeOneByteToInt(const byte* in, const uint& offset, uint* out) {
			*out = (uint)(*(in + offset));
		}

		static void decodeTwoBytesToInt(const byte* in, const uint& offset, uint* out) {
			*out = (uint)read2BytesLittleEndian(in + offset);
		}
	};
}

#endif