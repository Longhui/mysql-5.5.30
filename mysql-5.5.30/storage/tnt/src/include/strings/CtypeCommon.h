/**
 * 字符集处理公共头文件
 *
 * @author 余利华(yulihua@corp.netease.com, ylh@163.org)
 */

#ifndef _NTSE_CTYPE_COMMON_H_
#define _NTSE_CTYPE_COMMON_H_

#include <stddef.h>
#include "util/Portable.h"

namespace ntse {

/** 为了兼容Mysql代码，定义以下类型 */
typedef byte uchar;
typedef u16 uint16;
typedef unsigned long	ulong;

/****** 以下来自：M_ctype.h *******/
#define my_wc_t ulong

typedef struct unicase_info_st
{
  uint16 toupper;
  uint16 tolower;
  uint16 sort;
} MY_UNICASE_INFO;

/* wm_wc and wc_mb return codes */
#define MY_CS_ILSEQ	0     /* Wrong by sequence: wb_wc                   */
#define MY_CS_ILUNI	0     /* Cannot encode Unicode to charset: wc_mb    */
#define MY_CS_TOOSMALL  -101  /* Need at least one byte:    wc_mb and mb_wc */
#define MY_CS_TOOSMALL2 -102  /* Need at least two bytes:   wc_mb and mb_wc */
#define MY_CS_TOOSMALL3 -103  /* Need at least three bytes: wc_mb and mb_wc */
/* These following three are currently not really used */
#define MY_CS_TOOSMALL4 -104  /* Need at least 4 bytes: wc_mb and mb_wc */
#define MY_CS_TOOSMALL5 -105  /* Need at least 5 bytes: wc_mb and mb_wc */
#define MY_CS_TOOSMALL6 -106  /* Need at least 6 bytes: wc_mb and mb_wc */
/* A helper macros for "need at least n bytes" */
#define MY_CS_TOOSMALLN(n)    (-100-(n))

/****** 以上来自：M_ctype.h  ******/


/****** 以下来自： My_gloabl.h ******/
#if defined(_lint) || defined(FORCE_INIT_OF_VARS)
#define LINT_INIT(var)	var=0			/* No uninitialize-warning */
#else
#define LINT_INIT(var)
#endif
/****** 以上来自： My_gloabl.h ******/


extern int my_strnncollsp_gbk(const uchar *a, size_t a_length,
						const uchar *b, size_t b_length);
extern int my_strnncollsp_utf8(const uchar *s, size_t slen,
						const uchar *t, size_t tlen);
extern int my_strnncollsp_latin1(const uchar *s, size_t slen,
						const uchar *t, size_t tlen);
extern int my_strnncoll_bin(const uchar *s, size_t slen,
						const uchar *t, size_t tlen);

extern uint ismbchar_gbk(const char *b, const char *e);

extern uint ismbchar_utf8(const char *b, const char *e);

} // _NTSE_CTYPE_COMMON_H_

#endif // _NTSE_CTYPE_COMMON_H_
