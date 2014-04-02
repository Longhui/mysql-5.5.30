/**
 * 压缩和解压接口实现
 *
 * @author zx(zhangxiao@corp.netease.com, zx@163.org)
 */

#include "lob/Lzo.h"
#include "lob/lzo/lzo1x.h"
#include "util/Sync.h"
#include "lob/lzo/lzoconf.h"
#include "misc/Global.h"

namespace ntse {  

/** 
 * 构造函数
 */
CompressMem::CompressMem() {
	m_mem = (byte *)System::virtualAlloc(COMPRESS_WORK_SPACE_LEN);
	memset(m_mem, 0, COMPRESS_WORK_SPACE_LEN);
	m_mpLink.set(this);
}

/** 
 * 析构函数
 */
CompressMem::~CompressMem() {
	System::virtualFree(m_mem);
	m_mem = NULL;
	m_mpLink.set(NULL);
}

/** 
 * 构造函数
 */
LzoOper::LzoOper() {
	init();
	m_init = true;
	// 创建工作空间池
	m_mPool = new DList<CompressMem *>();
	m_mpLock = new Mutex("LzoOper::mpLock", __FILE__, __LINE__);
}

/** 
 * 析构函数
 */
LzoOper::~LzoOper() {
	while (!m_mPool->isEmpty()) {
		CompressMem *cm = m_mPool->removeFirst()->get();
		delete cm;
	}

	delete m_mPool;
	m_mPool = NULL;

	delete m_mpLock;
	m_mpLock = NULL;
}

/**
 * 初始化LZO库，可以初始化多次
 */
void LzoOper::init() {
	NTSE_ASSERT(lzo_init() == LZO_E_OK);
}

/**
 * 压缩
 *
 * @param in 要压缩的数据
 * @param inSize 要压缩的数据长度
 * @param out 压缩后的数据
 * @param outSize OUT 压缩后的数据长度
 * @return 0：压缩成功；1：压缩比原来长；2：压缩没成功
 */
uint LzoOper::compress(const byte *in, uint inSize, byte *out, uint *outSize) {
	lzo_uint out_len;
	CompressMem *mem = getCompressMem();
	byte *wrk = mem->m_mem; 
	int ret = lzo1x_1_11_compress((lzo_bytep)in, (lzo_uint)inSize, (lzo_bytep)out,
		(lzo_uintp)&out_len, (lzo_voidp)wrk);
	freeCompressMem(mem);
	*outSize = (uint)out_len;
	if (ret != LZO_E_OK)
		return 2;
	if (*outSize >= inSize)
		return 1;
	return 0;
}

/**
 * 解压缩
 *
 * @param in 要解压的数据
 * @param inSize 要解压的数据长度
 * @param out 解压后的数据
 * @param outSize OUT 解压后的数据长度
 * @return 解压缩是否成功
 */
bool LzoOper::decompress(byte *in, uint inSize, byte *out, uint *outSize) {
	lzo_uint out_len;
	int ret =  lzo1x_decompress((lzo_bytep)in, (lzo_uint)inSize, (lzo_bytep)out,
		(lzo_uintp)&out_len, NULL);
	*outSize = (uint)out_len;
	if (ret == LZO_E_OK) {
		return true;
	}
	return false;
}

/**
 * 获取一个压缩工作空间
 *
 * @return 工作空间
 */
CompressMem* LzoOper::getCompressMem() {
	LOCK(m_mpLock);
	if (!m_mPool->isEmpty()) {
		CompressMem *compressMem = m_mPool->removeFirst()->get();
		UNLOCK(m_mpLock);
		memset(compressMem->m_mem, 0, COMPRESS_WORK_SPACE_LEN);
		return compressMem;
	}
	UNLOCK(m_mpLock);
	return new CompressMem();
}

/**
 * 释放压缩工作空间
 *
 * @param 工作空间
 */
void LzoOper::freeCompressMem(CompressMem *mem) {
	assert(mem && mem->m_mpLink.get() == mem);

	if (m_mPool->getSize() < RESERVED_WORK_SPACE) {
		LOCK(m_mpLock);
		m_mPool->addFirst(&mem->m_mpLink);
		UNLOCK(m_mpLock);
	} else
		delete mem;
}

}
