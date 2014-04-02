/**
 * ������
 *
 * @author ��Դ(wangyuan@corp.netease.com, wy@163.org)
 */

#ifndef _NTSE_STREAM_H_
#define _NTSE_STREAM_H_

#include "misc/Global.h"

namespace ntse {

/** �����ṩ�����ж�ȡ��д�볣���������͵ķ��� 
 * ������Java��DataInputStream/DataOutputStream�Ĺ���
 */
struct Stream {
private:
	size_t	m_capacity;	/** m_data����Ŀռ��С */
	byte	*m_data;	/** ���� */
	byte	*m_currPtr;	/** ��ǰд����ȡλ�� */
	byte	*m_end;		/** ���ݽ���λ�� */

public:
	Stream(byte *data, size_t size);
	size_t getSize();
	/** �õ���ǰд����ȡλ��
	 * @return ��ǰд����ȡλ��
	 */
	byte* currPtr() const {
		return m_currPtr;
	}
	Stream* write(u8 v) throw(NtseException);
	Stream* write(s8 v) throw(NtseException);
	Stream* write(u16 v) throw(NtseException);
	Stream* write(s16 v) throw(NtseException);
	Stream* write(u32 v) throw(NtseException);
	Stream* write(s32 v) throw(NtseException);
	Stream* write(u64 v) throw(NtseException);
	Stream* write(s64 v) throw(NtseException);
	Stream* write(double v) throw(NtseException);
	Stream* write(bool v) throw(NtseException);
	Stream* writeRid(RowId rid) throw(NtseException);
	Stream* write(const char *str) throw(NtseException);
	Stream* write(const byte *buf, size_t size) throw(NtseException);
	Stream* write(void *p) throw(NtseException);
	Stream* read(u8 *v) throw(NtseException);
	Stream* read(s8 *v) throw(NtseException);
	Stream* read(u16 *v) throw(NtseException);
	Stream* read(s16 *v) throw(NtseException);
	Stream* read(u32 *v) throw(NtseException);
	Stream* read(s32 *v) throw(NtseException);
	Stream* read(u64 *v) throw(NtseException);
	Stream* read(s64 *v) throw(NtseException);
	Stream* read(double *d) throw(NtseException);
	Stream* read(bool *v) throw(NtseException);
	Stream* readRid(RowId *rid) throw(NtseException);
	Stream* readString(char **str) throw(NtseException);
	Stream* readPtr(void **p) throw(NtseException);
	char* readString() throw(NtseException);
	Stream* readBytes(byte *buf, size_t size) throw(NtseException);
	Stream* skip(size_t bytes) throw(NtseException);

private:
	void checkOverflow(size_t size) throw(NtseException);
};

}

#endif
