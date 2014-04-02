/**
 * ѹ���ͽ�ѹ�ӿ�ʵ��
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
 * ���캯��
 */
CompressMem::CompressMem() {
	m_mem = (byte *)System::virtualAlloc(COMPRESS_WORK_SPACE_LEN);
	memset(m_mem, 0, COMPRESS_WORK_SPACE_LEN);
	m_mpLink.set(this);
}

/** 
 * ��������
 */
CompressMem::~CompressMem() {
	System::virtualFree(m_mem);
	m_mem = NULL;
	m_mpLink.set(NULL);
}

/** 
 * ���캯��
 */
LzoOper::LzoOper() {
	init();
	m_init = true;
	// ���������ռ��
	m_mPool = new DList<CompressMem *>();
	m_mpLock = new Mutex("LzoOper::mpLock", __FILE__, __LINE__);
}

/** 
 * ��������
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
 * ��ʼ��LZO�⣬���Գ�ʼ�����
 */
void LzoOper::init() {
	NTSE_ASSERT(lzo_init() == LZO_E_OK);
}

/**
 * ѹ��
 *
 * @param in Ҫѹ��������
 * @param inSize Ҫѹ�������ݳ���
 * @param out ѹ���������
 * @param outSize OUT ѹ��������ݳ���
 * @return 0��ѹ���ɹ���1��ѹ����ԭ������2��ѹ��û�ɹ�
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
 * ��ѹ��
 *
 * @param in Ҫ��ѹ������
 * @param inSize Ҫ��ѹ�����ݳ���
 * @param out ��ѹ�������
 * @param outSize OUT ��ѹ������ݳ���
 * @return ��ѹ���Ƿ�ɹ�
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
 * ��ȡһ��ѹ�������ռ�
 *
 * @return �����ռ�
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
 * �ͷ�ѹ�������ռ�
 *
 * @param �����ռ�
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
