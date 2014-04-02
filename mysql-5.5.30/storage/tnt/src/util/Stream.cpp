/**
 * 流对象实现。
 *
 * @author 汪源(wangyuan@corp.netease.com, wy@163.org)
 */

#include <string.h>
#include "util/Stream.h"

using namespace ntse;

namespace ntse {

/**
 * 构造一个流对象
 *
 * @param data 流对象使用的内存
 * @param size 给流对象分配的内存大小
 */
Stream::Stream(byte *data, size_t size) {
	m_capacity = size;
	m_data = data;
	m_currPtr = m_data;
	m_end = m_data + size;
}

/**
 * 返回流对象中已经写入或读取的数据大小
 *
 * @return 已经写入或读取的数据大小
 */
size_t Stream::getSize() {
	return (uint)(m_currPtr - m_data);
}

/**
 * 写入一个无符号单字节
 *
 * @param v 无符号单字节
 * @return 流对象本身
 * @throw NtseException 越界错误
 */
Stream* Stream::write(u8 v) throw(NtseException) {
	checkOverflow(sizeof(u8));
	*m_currPtr = v;
	m_currPtr++;
	return this;
}

/**
 * 写入一个有符号单字节
 *
 * @param v 有符号单字节
 * @return 流对象本身
 * @throw NtseException 越界错误
 */
Stream* Stream::write(s8 v) throw(NtseException) {
	checkOverflow(sizeof(s8));
	*((s8 *)m_currPtr) = v;
	m_currPtr++;
	return this;
}

/**
 * 写入一个无符号双字节
 *
 * @param v 无符号双字节
 * @return 流对象本身
 * @throw NtseException 越界错误
 */
Stream* Stream::write(u16 v) throw(NtseException) {
	checkOverflow(sizeof(u16));
	*((u16 *)m_currPtr) = v;
	m_currPtr += sizeof(u16);
	return this;
}

/**
 * 写入一个有符号双字节
 *
 * @param v 有符号双字节
 * @return 流对象本身
 * @throw NtseException 越界错误
 */
Stream* Stream::write(s16 v) throw(NtseException) {
	checkOverflow(sizeof(s16));
	*((s16 *)m_currPtr) = v;
	m_currPtr += sizeof(s16);
	return this;
}

/**
 * 写入一个无符号四字节
 *
 * @param v 无符号四字节
 * @return 流对象本身
 * @throw NtseException 越界错误
 */
Stream* Stream::write(u32 v) throw(NtseException) {
	checkOverflow(sizeof(u32));
	*((u32 *)m_currPtr) = v;
	m_currPtr += sizeof(u32);
	return this;
}

/**
 * 写入一个有符号四字节
 *
 * @param v 有符号四字节
 * @return 流对象本身
 * @throw NtseException 越界错误
 */
Stream* Stream::write(s32 v) throw(NtseException) {
	checkOverflow(sizeof(s32));
	*((s32 *)m_currPtr) = v;
	m_currPtr += sizeof(s32);
	return this;
}

/**
 * 写入一个无符号八字节
 *
 * @param v 无符号八字节
 * @return 流对象本身
 * @throw NtseException 越界错误
 */
Stream* Stream::write(u64 v) throw(NtseException) {
	checkOverflow(sizeof(u64));
	*((u64 *)m_currPtr) = v;
	m_currPtr += sizeof(u64);
	return this;
}

/**
 * 写入一个有符号八字节
 *
 * @param v 有符号八字节
 * @return 流对象本身
 */
Stream* Stream::write(s64 v) throw(NtseException) {
	checkOverflow(sizeof(s64));
	*((s64 *)m_currPtr) = v;
	m_currPtr += sizeof(s64);
	return this;
}

/**
 * 写入一个布尔型
 *
 * @param v 布尔型
 * @return 流对象本身
 */
Stream* Stream::write(bool v) throw(NtseException) {
	checkOverflow(sizeof(u8));
	*((u8 *)m_currPtr) = (u8)v;
	m_currPtr += sizeof(u8);
	return this;
}

/**
 * 写入一个RID
 *
 * @param rid RID
 * @return 流对象本身
 * @throw NtseException 越界错误
 */
Stream* Stream::writeRid(RowId rid) throw(NtseException) {
	checkOverflow(RID_BYTES);
	RID_WRITE(rid, m_currPtr);
	m_currPtr += RID_BYTES;
	return this;
}

/**
 * 写入一个以\0结尾的字符串。占用空间为字符串长度+5字节
 *
 * @param str 字符串
 * @return 流对象本身
 * @throw NtseException 越界错误
 */
Stream* Stream::write(const char *str) throw(NtseException) {
	checkOverflow(sizeof(u32) + strlen(str) + 1);
	write((u32)strlen(str));
	write((const byte *)str, strlen(str) + 1);
	return this;
}

/**
 * 写入一段内存。占用空间为size字节
 *
 * @param buf 内存地址
 * @param size 要写入的字节数
 * @return 流对象本身
 */
Stream* Stream::write(const byte *buf, size_t size) throw(NtseException) {
	checkOverflow(size);
	memcpy(m_currPtr, buf, size);
	m_currPtr += size;
	return this;
}

/**
 * 写入一个双精度浮点数
 *
 * @param v 要写入的双精度浮点数
 * @return 流对象本身
 */
Stream* Stream::write(double v) throw(NtseException) {
	checkOverflow(sizeof(double));
	*((double *)m_currPtr) = v;
	m_currPtr += sizeof(double);
	return this;
}

/**
 * 写入一个指针对象
 *
 * @param p 要写入的指针
 * @return 流对象本身
 */
Stream* Stream::write(void *p) throw(NtseException) {
	checkOverflow(sizeof(u64));
	memcpy(m_currPtr, (byte*)&p, sizeof(void *));
	// 保证一定写8字节，便于统一64位和32位操作系统
	if (sizeof(void *) < sizeof(u64))
		memset(m_currPtr + sizeof(void *), 0, sizeof(u64) - sizeof(void *));
	m_currPtr += sizeof(u64);
	return this;
}

/**
 * 读取一个无符号单字节
 *
 * @param v 输出参数，读取的内容
 * @return 流对象本身
 * @throw NtseException 越界错误
 */
Stream* Stream::read(u8 *v) throw(NtseException) {
	checkOverflow(sizeof(u8));
	*v = *m_currPtr;
	m_currPtr++;
	return this;
}

/**
 * 读取一个有符号单字节
 *
 * @param v 输出参数，读取的内容
 * @return 流对象本身
 */
Stream* Stream::read(s8 *v) throw(NtseException) {
	checkOverflow(sizeof(s8));
	*v = *((s8 *)m_currPtr);
	m_currPtr++;
	return this;
}

/**
 * 读取一个无符号双字节
 *
 * @param v 输出参数，读取的内容
 * @return 流对象本身
 * @throw NtseException 越界错误
 */
Stream* Stream::read(u16 *v) throw(NtseException) {
	checkOverflow(sizeof(u16));
	*v = *((u16 *)m_currPtr);
	m_currPtr += sizeof(u16);
	return this;
}

/**
 * 读取一个有符号双字节
 *
 * @param v 输出参数，读取的内容
 * @return 流对象本身
 * @throw NtseException 越界错误
 */
Stream* Stream::read(s16 *v) throw(NtseException) {
	checkOverflow(sizeof(s16));
	*v = *((s16 *)m_currPtr);
	m_currPtr += sizeof(s16);
	return this;
}

/**
 * 读取一个无符号四字节
 *
 * @param v 输出参数，读取的内容
 * @return 流对象本身
 * @throw NtseException 越界错误
 */
Stream* Stream::read(u32 *v) throw(NtseException) {
	checkOverflow(sizeof(u32));
	*v = *((u32 *)m_currPtr);
	m_currPtr += sizeof(u32);
	return this;
}

/**
 * 读取一个有符号四字节
 *
 * @param v 输出参数，读取的内容
 * @return 流对象本身
 */
Stream* Stream::read(s32 *v) throw(NtseException) {
	checkOverflow(sizeof(s32));
	*v = *((s32 *)m_currPtr);
	m_currPtr += sizeof(s32);
	return this;
}

/**
 * 读取一个无符号八字节
 *
 * @param v 输出参数，读取的内容
 * @return 流对象本身
 * @throw NtseException 越界错误
 */
Stream* Stream::read(u64 *v) throw(NtseException) {
	checkOverflow(sizeof(u64));
	*v = *((u64 *)m_currPtr);
	m_currPtr += sizeof(u64);
	return this;
}

/**
 * 读取一个有符号八字节
 *
 * @param v 输出参数，读取的内容
 * @return 流对象本身
 * @throw NtseException 越界错误
 */
Stream* Stream::read(s64 *v) throw(NtseException) {
	checkOverflow(sizeof(s64));
	*v = *((s64 *)m_currPtr);
	m_currPtr += sizeof(s64);
	return this;
}

/**
 * 读取一个布尔型
 *
 * @param v 输出参数，读取的内容
 * @return 流对象本身
 * @throw NtseException 越界错误
 */
Stream* Stream::read(bool *v) throw(NtseException) {
	checkOverflow(sizeof(u8));
	*v = *((u8 *)m_currPtr) != 0;
	m_currPtr += sizeof(u8);
	return this;
}

/**
 * 读取一个布尔型
 *
 * @param v 输出参数，读取的内容
 * @return 流对象本身
 * @throw NtseException 越界错误
 */
Stream* Stream::read(double *d) throw(NtseException) {
	checkOverflow(sizeof(double));
	*d = *((double *)m_currPtr);
	m_currPtr += sizeof(double);
	return this;
}

/**
 * 读取一个RID
 *
 * @param v 输出参数，读取的内容
 * @return 流对象本身
 * @throw NtseException 越界错误
 */
Stream* Stream::readRid(RowId *rid) throw(NtseException) {
	checkOverflow(RID_BYTES);
	*rid = RID_READ(m_currPtr);
	m_currPtr += RID_BYTES;
	return this;
}

/**
 * 读取一个字符串
 *
 * @param str 输出参数，读取的内容，使用new操作分配
 * @return 流对象本身
 * @throw NtseException 越界错误
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
 * 读取一个字符串
 *
 * @return 字符串内容，使用new分配
 * @throw NtseException 越界错误
 */
char* Stream::readString() throw(NtseException) {
	char *r;
	readString(&r);
	return r;
}

/**
 * 读取一段内存
 *
 * @param buf 输出参数，读取的内容
 * @param size 要读取的字节数
 * @return 流对象本身
 * @throw NtseException 越界错误
 */
Stream* Stream::readBytes(byte *buf, size_t size) throw(NtseException) {
	checkOverflow(size);
	memcpy(buf, m_currPtr, size);
	m_currPtr += size;
	return this;
}

/**
 * 读取一个范型指针
 *
 * @param p	out 读取指针赋值的对象
 * @return 流对象本身
 * @throw NtseException 越界错误
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
 * 跳过指定字节的内容
 *
 * @param size 要跳过的字节数
 * @return 流对象本身
 * @throw NtseException 越界错误
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

