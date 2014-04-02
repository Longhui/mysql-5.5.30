/**
 * ������ʵ�֡�
 *
 * @author ��Դ(wangyuan@corp.netease.com, wy@163.org)
 */

#include <string.h>
#include "util/Stream.h"

using namespace ntse;

namespace ntse {

/**
 * ����һ��������
 *
 * @param data ������ʹ�õ��ڴ�
 * @param size �������������ڴ��С
 */
Stream::Stream(byte *data, size_t size) {
	m_capacity = size;
	m_data = data;
	m_currPtr = m_data;
	m_end = m_data + size;
}

/**
 * �������������Ѿ�д����ȡ�����ݴ�С
 *
 * @return �Ѿ�д����ȡ�����ݴ�С
 */
size_t Stream::getSize() {
	return (uint)(m_currPtr - m_data);
}

/**
 * д��һ���޷��ŵ��ֽ�
 *
 * @param v �޷��ŵ��ֽ�
 * @return ��������
 * @throw NtseException Խ�����
 */
Stream* Stream::write(u8 v) throw(NtseException) {
	checkOverflow(sizeof(u8));
	*m_currPtr = v;
	m_currPtr++;
	return this;
}

/**
 * д��һ���з��ŵ��ֽ�
 *
 * @param v �з��ŵ��ֽ�
 * @return ��������
 * @throw NtseException Խ�����
 */
Stream* Stream::write(s8 v) throw(NtseException) {
	checkOverflow(sizeof(s8));
	*((s8 *)m_currPtr) = v;
	m_currPtr++;
	return this;
}

/**
 * д��һ���޷���˫�ֽ�
 *
 * @param v �޷���˫�ֽ�
 * @return ��������
 * @throw NtseException Խ�����
 */
Stream* Stream::write(u16 v) throw(NtseException) {
	checkOverflow(sizeof(u16));
	*((u16 *)m_currPtr) = v;
	m_currPtr += sizeof(u16);
	return this;
}

/**
 * д��һ���з���˫�ֽ�
 *
 * @param v �з���˫�ֽ�
 * @return ��������
 * @throw NtseException Խ�����
 */
Stream* Stream::write(s16 v) throw(NtseException) {
	checkOverflow(sizeof(s16));
	*((s16 *)m_currPtr) = v;
	m_currPtr += sizeof(s16);
	return this;
}

/**
 * д��һ���޷������ֽ�
 *
 * @param v �޷������ֽ�
 * @return ��������
 * @throw NtseException Խ�����
 */
Stream* Stream::write(u32 v) throw(NtseException) {
	checkOverflow(sizeof(u32));
	*((u32 *)m_currPtr) = v;
	m_currPtr += sizeof(u32);
	return this;
}

/**
 * д��һ���з������ֽ�
 *
 * @param v �з������ֽ�
 * @return ��������
 * @throw NtseException Խ�����
 */
Stream* Stream::write(s32 v) throw(NtseException) {
	checkOverflow(sizeof(s32));
	*((s32 *)m_currPtr) = v;
	m_currPtr += sizeof(s32);
	return this;
}

/**
 * д��һ���޷��Ű��ֽ�
 *
 * @param v �޷��Ű��ֽ�
 * @return ��������
 * @throw NtseException Խ�����
 */
Stream* Stream::write(u64 v) throw(NtseException) {
	checkOverflow(sizeof(u64));
	*((u64 *)m_currPtr) = v;
	m_currPtr += sizeof(u64);
	return this;
}

/**
 * д��һ���з��Ű��ֽ�
 *
 * @param v �з��Ű��ֽ�
 * @return ��������
 */
Stream* Stream::write(s64 v) throw(NtseException) {
	checkOverflow(sizeof(s64));
	*((s64 *)m_currPtr) = v;
	m_currPtr += sizeof(s64);
	return this;
}

/**
 * д��һ��������
 *
 * @param v ������
 * @return ��������
 */
Stream* Stream::write(bool v) throw(NtseException) {
	checkOverflow(sizeof(u8));
	*((u8 *)m_currPtr) = (u8)v;
	m_currPtr += sizeof(u8);
	return this;
}

/**
 * д��һ��RID
 *
 * @param rid RID
 * @return ��������
 * @throw NtseException Խ�����
 */
Stream* Stream::writeRid(RowId rid) throw(NtseException) {
	checkOverflow(RID_BYTES);
	RID_WRITE(rid, m_currPtr);
	m_currPtr += RID_BYTES;
	return this;
}

/**
 * д��һ����\0��β���ַ�����ռ�ÿռ�Ϊ�ַ�������+5�ֽ�
 *
 * @param str �ַ���
 * @return ��������
 * @throw NtseException Խ�����
 */
Stream* Stream::write(const char *str) throw(NtseException) {
	checkOverflow(sizeof(u32) + strlen(str) + 1);
	write((u32)strlen(str));
	write((const byte *)str, strlen(str) + 1);
	return this;
}

/**
 * д��һ���ڴ档ռ�ÿռ�Ϊsize�ֽ�
 *
 * @param buf �ڴ��ַ
 * @param size Ҫд����ֽ���
 * @return ��������
 */
Stream* Stream::write(const byte *buf, size_t size) throw(NtseException) {
	checkOverflow(size);
	memcpy(m_currPtr, buf, size);
	m_currPtr += size;
	return this;
}

/**
 * д��һ��˫���ȸ�����
 *
 * @param v Ҫд���˫���ȸ�����
 * @return ��������
 */
Stream* Stream::write(double v) throw(NtseException) {
	checkOverflow(sizeof(double));
	*((double *)m_currPtr) = v;
	m_currPtr += sizeof(double);
	return this;
}

/**
 * д��һ��ָ�����
 *
 * @param p Ҫд���ָ��
 * @return ��������
 */
Stream* Stream::write(void *p) throw(NtseException) {
	checkOverflow(sizeof(u64));
	memcpy(m_currPtr, (byte*)&p, sizeof(void *));
	// ��֤һ��д8�ֽڣ�����ͳһ64λ��32λ����ϵͳ
	if (sizeof(void *) < sizeof(u64))
		memset(m_currPtr + sizeof(void *), 0, sizeof(u64) - sizeof(void *));
	m_currPtr += sizeof(u64);
	return this;
}

/**
 * ��ȡһ���޷��ŵ��ֽ�
 *
 * @param v �����������ȡ������
 * @return ��������
 * @throw NtseException Խ�����
 */
Stream* Stream::read(u8 *v) throw(NtseException) {
	checkOverflow(sizeof(u8));
	*v = *m_currPtr;
	m_currPtr++;
	return this;
}

/**
 * ��ȡһ���з��ŵ��ֽ�
 *
 * @param v �����������ȡ������
 * @return ��������
 */
Stream* Stream::read(s8 *v) throw(NtseException) {
	checkOverflow(sizeof(s8));
	*v = *((s8 *)m_currPtr);
	m_currPtr++;
	return this;
}

/**
 * ��ȡһ���޷���˫�ֽ�
 *
 * @param v �����������ȡ������
 * @return ��������
 * @throw NtseException Խ�����
 */
Stream* Stream::read(u16 *v) throw(NtseException) {
	checkOverflow(sizeof(u16));
	*v = *((u16 *)m_currPtr);
	m_currPtr += sizeof(u16);
	return this;
}

/**
 * ��ȡһ���з���˫�ֽ�
 *
 * @param v �����������ȡ������
 * @return ��������
 * @throw NtseException Խ�����
 */
Stream* Stream::read(s16 *v) throw(NtseException) {
	checkOverflow(sizeof(s16));
	*v = *((s16 *)m_currPtr);
	m_currPtr += sizeof(s16);
	return this;
}

/**
 * ��ȡһ���޷������ֽ�
 *
 * @param v �����������ȡ������
 * @return ��������
 * @throw NtseException Խ�����
 */
Stream* Stream::read(u32 *v) throw(NtseException) {
	checkOverflow(sizeof(u32));
	*v = *((u32 *)m_currPtr);
	m_currPtr += sizeof(u32);
	return this;
}

/**
 * ��ȡһ���з������ֽ�
 *
 * @param v �����������ȡ������
 * @return ��������
 */
Stream* Stream::read(s32 *v) throw(NtseException) {
	checkOverflow(sizeof(s32));
	*v = *((s32 *)m_currPtr);
	m_currPtr += sizeof(s32);
	return this;
}

/**
 * ��ȡһ���޷��Ű��ֽ�
 *
 * @param v �����������ȡ������
 * @return ��������
 * @throw NtseException Խ�����
 */
Stream* Stream::read(u64 *v) throw(NtseException) {
	checkOverflow(sizeof(u64));
	*v = *((u64 *)m_currPtr);
	m_currPtr += sizeof(u64);
	return this;
}

/**
 * ��ȡһ���з��Ű��ֽ�
 *
 * @param v �����������ȡ������
 * @return ��������
 * @throw NtseException Խ�����
 */
Stream* Stream::read(s64 *v) throw(NtseException) {
	checkOverflow(sizeof(s64));
	*v = *((s64 *)m_currPtr);
	m_currPtr += sizeof(s64);
	return this;
}

/**
 * ��ȡһ��������
 *
 * @param v �����������ȡ������
 * @return ��������
 * @throw NtseException Խ�����
 */
Stream* Stream::read(bool *v) throw(NtseException) {
	checkOverflow(sizeof(u8));
	*v = *((u8 *)m_currPtr) != 0;
	m_currPtr += sizeof(u8);
	return this;
}

/**
 * ��ȡһ��������
 *
 * @param v �����������ȡ������
 * @return ��������
 * @throw NtseException Խ�����
 */
Stream* Stream::read(double *d) throw(NtseException) {
	checkOverflow(sizeof(double));
	*d = *((double *)m_currPtr);
	m_currPtr += sizeof(double);
	return this;
}

/**
 * ��ȡһ��RID
 *
 * @param v �����������ȡ������
 * @return ��������
 * @throw NtseException Խ�����
 */
Stream* Stream::readRid(RowId *rid) throw(NtseException) {
	checkOverflow(RID_BYTES);
	*rid = RID_READ(m_currPtr);
	m_currPtr += RID_BYTES;
	return this;
}

/**
 * ��ȡһ���ַ���
 *
 * @param str �����������ȡ�����ݣ�ʹ��new��������
 * @return ��������
 * @throw NtseException Խ�����
 */
Stream* Stream::readString(char **str) throw(NtseException) {
	checkOverflow(sizeof(u32));
	u32 size;
	read(&size);
	*str = new char[size + 1];
	readBytes((byte *)*str, size + 1);
	return this;
}

/**
 * ��ȡһ���ַ���
 *
 * @return �ַ������ݣ�ʹ��new����
 * @throw NtseException Խ�����
 */
char* Stream::readString() throw(NtseException) {
	char *r;
	readString(&r);
	return r;
}

/**
 * ��ȡһ���ڴ�
 *
 * @param buf �����������ȡ������
 * @param size Ҫ��ȡ���ֽ���
 * @return ��������
 * @throw NtseException Խ�����
 */
Stream* Stream::readBytes(byte *buf, size_t size) throw(NtseException) {
	checkOverflow(size);
	memcpy(buf, m_currPtr, size);
	m_currPtr += size;
	return this;
}

/**
 * ��ȡһ������ָ��
 *
 * @param p	out ��ȡָ�븳ֵ�Ķ���
 * @return ��������
 * @throw NtseException Խ�����
 */
Stream* Stream::readPtr(void **p) throw(NtseException) {
	checkOverflow(sizeof(void *));
	memcpy((byte*)(&(*p)), m_currPtr, sizeof(void *));
	m_currPtr += sizeof(void *);
	if (sizeof(void *) < sizeof(u64))
		skip(sizeof(u64) - sizeof(void *));
	return this;
}

/**
 * ����ָ���ֽڵ�����
 *
 * @param size Ҫ�������ֽ���
 * @return ��������
 * @throw NtseException Խ�����
 */
Stream* Stream::skip(size_t size) throw(NtseException) {
	checkOverflow(size);
	m_currPtr += size;
	return this;
}

void Stream::checkOverflow(size_t size) throw(NtseException) {
	if (m_currPtr + size > m_end)
		NTSE_THROW(NTSE_EC_OVERFLOW, "Stream overflow, need %d bytes, only %d bytes available.", size, m_end - m_currPtr);
}


}

