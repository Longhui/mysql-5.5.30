/**
 * 扩展验证配置
 *
 * @author 汪源(wangyuan@corp.netease.com, wy@163.org)
 */

#ifndef _NTSE_VERIFY_H_
#define _NTSE_VERIFY_H_

#include "util/Portable.h"
#include "misc/Global.h"
#include <memory.h>

namespace ntse {

/** 各项扩展验证功能是否开启的配置 */
struct VerifySetting {
public:
	VerifySetting();

public:
	bool	buf;	  /** 是否启用页面缓存模块的扩展验证功能 */
	bool	hp;		  /** 是否启用堆模块的扩展验证功能 */
	bool	hpheavy;  /** 是否启用堆模块中特别费时的验证操作 */
	bool	idx;	  /** 是否启用索引模块的扩展验证功能 */
	bool	tbl;	  /** 是否启用表模块的扩展验证功能 */
	bool	lob;	  /** 是否启用大对象模块的扩展验证功能 */
	bool	mms;	  /** 是否启用MMS模块的扩展验证功能 */
	bool	pool;	  /** 是否启用内存页池模块的扩展验证功能 */
	bool	lsn;	  /** 是否启用redo时页面lsn检查 */
	bool    compress; /** 是否启动记录压缩模块的扩展验证功能 */
	bool	log;	/** 是否日志模块的扩展验证功能 */
	bool    mheap;    /** 是否启用内存堆记录验证功能*/
};

extern VerifySetting vs;

#ifdef NTSE_VERIFY_EX
/**
 * 进行扩展验证的宏。当aspect为true时验证expr为真
 *
 * @param aspect 形如vs.member，指定验证代码所属的类别
 * @param expr 当aspect为true时要验证的表达式
 */
#define verify_ex(aspect, expr)		\
	do {							\
		if (aspect) {				\
			NTSE_ASSERT(expr);		\
		}							\
	} while(0)

/**
 * 只在开启了扩展验证时执行的代码。
 *
 * @param aspect 形如vs.member，指定代码所属的类别
 * @param block 当aspect为true时要执行的语句
 */
#define vecode(aspect, block)		\
	do {							\
		if (aspect) {				\
			block;					\
		}							\
	} while(0)
#else
#define verify_ex(aspect, expr)
#define vecode(aspect, block)
#endif
}

#endif
