/**
 * �������ͼ�һЩ�����ֲ����صĶ���
 *
 * @author ��Դ(wangyuan@corp.netease.com, wy@163.org)
 */

#ifndef _NTSE_PORTABLE_H_
#define _NTSE_PORTABLE_H_

typedef signed char	    s8;	
typedef unsigned char	u8;
typedef signed short	s16;
typedef unsigned short	u16;
typedef signed int	    s32;
typedef unsigned int	u32;
typedef long long	    s64;
typedef unsigned long long u64;
typedef unsigned int    uint;
typedef unsigned char	byte;

/** ��ָ�� */
#ifndef __cplusplus
#ifndef NULL
#define	NULL	((void*)0)
#endif
#else
#ifndef NULL
#define	NULL	0
#endif
#endif

#ifdef WIN32
#define I64FORMAT	"%I64"
#else
#define I64FORMAT	"%ll"
#endif

#ifndef UNREFERENCED_PARAMETER
#ifdef WIN32
#define UNREFERENCED_PARAMETER(P) (P)
#else
#define UNREFERENCED_PARAMETER(P) (void)(P)
#endif
#endif

#pragma warning (disable: 4127)	/** while(true)�������̫������ */

/** �߳�˽�б��� */
#ifdef WIN32
#define TLS	__declspec(thread)
#else
#define TLS	__thread
#endif

#ifdef WIN32
#define __FUNC__	__FUNCTION__
#else
#define __FUNC__	__PRETTY_FUNCTION__
#endif

/** ���κ�����¶���Ч��assert */
#define assert_always(expr) do {															\
	if (!(expr)) {																			\
		fprintf(stderr, "Assertion failed in file %s line %d\n", __FILE__, __LINE__);		\
		fprintf(stderr, "Failing assertion: %s\n", #expr);									\
		*((char *)0) = 0;																	\
	}																						\
} while(0)

/** ��Զ��С�� */
#ifndef LITTLE_ENDIAN
#define LITTLE_ENDIAN
#endif

/** ��С�������ֽ����ȡ�������� */
#if defined(LITTLE_ENDIAN)
inline u32 read2BytesLittleEndian(const byte *p) {
	return *((u16 *)p);
}

inline void write2BytesLittleEndian(byte *p, u32 v) {
	*((u16 *)p) = (u16)v;
}
#else
inline u32 read2BytesLittleEndian(const byte *p) {
	u32 ret = p[0];
	ret |= (p[1] << 8);
	return ret;
}

inline void write2BytesLittleEndian(byte *p, u32 v) {
	p[0] = (byte )v;
	p[1] = (byte )(v >> 8);
}

#endif

inline u32 read3BytesLittleEndian(const byte *p) {
	u32 ret = p[0];
	ret |= (p[1] << 8);
	ret |= (p[2] << 16);
	return ret;
}

inline void write3BytesLittleEndian(byte *p, u32 v) {
	p[0] = (byte )v;
	p[1] = (byte )(v >> 8);
	p[2] = (byte )(v >> 16);
}

/** ��С�˵������ֽ���ת�� */

/**
 * ��/С��ת��
 * @param dest [out] ������������
 * @param src �����������
 * @param size �����ֽ���
 */
inline void changeEndian(byte *dest, const byte *src, u32 size) {
	for (u32 i = 0; i < size; ++i)
		dest[i] = src[size - 1 - i];
}
#if defined(LITTLE_ENDIAN)
template<typename T> inline T littleEndianToHost(T v) {
	return v;
}
#else
template<typename T> inline T littleEndianToHost(T v) {
	T ret;
	changeEndian((byte *)&ret, (byte *)&v, (u32)sizeof(T));
	return ret;
}
#endif // defined(LITTLE_ENDIAN)

#endif // _NTSE_PORTABLE_H_

