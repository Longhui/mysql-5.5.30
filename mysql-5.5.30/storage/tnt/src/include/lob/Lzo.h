/**
 * ѹ���ͽ�ѹ�ӿ�
 * ����ʵ�飬����ʹ��lzo1x-111ѹ��������ʹ��300��Ϊ�ֽ�ֵ����С�����ֵʱ��ѹ����
 * @author zx(zhangxiao@corp.netease.com, zx@163.org)
 */

#ifndef _NTSE_LZO_H_
#define _NTSE_LZO_H_

#include "util/Portable.h"
#include "util/System.h"
#include "util/DList.h"

namespace ntse {

/** ѹ�����ֵ䵥λ���� */
#define LZO_SIZEOF_DICT			((unsigned)sizeof(lzo_bytep))
/** ÿ�������ռ��С */
#define COMPRESS_WORK_SPACE_LEN	((u32)(2048L * LZO_SIZEOF_DICT))
/** �����ռ�ر�����С */
#define RESERVED_WORK_SPACE		5

class CompressMem;
struct Mutex;
/** LZOѹ��/��ѹ�� */
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

	bool					m_init;		/** �Ƿ��Ѿ���ʼ�� */
	DList<CompressMem *>	*m_mPool;	/** ѹ�������ռ�� */
	Mutex					*m_mpLock;	/** ���������ռ�ص��� */
};

/** ѹ��ʱ���õĹ����ռ� */
class CompressMem {
public:	
	CompressMem();
	~CompressMem();

private:
	DLink<CompressMem *>	m_mpLink;	/** �ڳ��е����� */
	byte					*m_mem;		/** ѹ�������ռ� */
friend class LzoOper;
};
}
#endif
