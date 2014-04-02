/**
 * 压缩和解压接口
 * 经过实验，决定使用lzo1x-111压缩方法，使用300作为分界值，当小于这个值时候不压缩。
 * @author zx(zhangxiao@corp.netease.com, zx@163.org)
 */

#ifndef _NTSE_LZO_H_
#define _NTSE_LZO_H_

#include "util/Portable.h"
#include "util/System.h"
#include "util/DList.h"

namespace ntse {

/** 压缩的字典单位长度 */
#define LZO_SIZEOF_DICT			((unsigned)sizeof(lzo_bytep))
/** 每个工作空间大小 */
#define COMPRESS_WORK_SPACE_LEN	((u32)(2048L * LZO_SIZEOF_DICT))
/** 工作空间池保留大小 */
#define RESERVED_WORK_SPACE		5

class CompressMem;
struct Mutex;
/** LZO压缩/解压类 */
class LzoOper {
public:
	LzoOper();
	~LzoOper();
	uint compress(const byte *lob, uint lobsize, byte *out, uint *outSize);
	bool decompress(byte *lob, uint lobsize, byte *out, uint *outSize);

private:
	void init();
	CompressMem* getCompressMem();
	void freeCompressMem(CompressMem *mem);

	bool					m_init;		/** 是否已经初始化 */
	DList<CompressMem *>	*m_mPool;	/** 压缩工作空间池 */
	Mutex					*m_mpLock;	/** 保护工作空间池的锁 */
};

/** 压缩时所用的工作空间 */
class CompressMem {
public:	
	CompressMem();
	~CompressMem();

private:
	DLink<CompressMem *>	m_mpLink;	/** 在池中的链接 */
	byte					*m_mem;		/** 压缩工作空间 */
friend class LzoOper;
};
}
#endif
