/**
 * 记录缓存
 *
 * @author 汪源(wangyuan@corp.netease.com, wy@163.org)
 */

#ifndef _NTSE_ROW_CACHE_
#define _NTSE_ROW_CACHE_

#ifndef WIN32
#include <my_global.h>
#include <sql_priv.h>
#include <sql_class.h>
#endif
#include "misc/Global.h"
#include "util/Array.h"
#include "misc/TableDef.h"
#include "misc/Record.h"
#include "misc/MemCtx.h"

using namespace ntse;

/** 记录缓存中的记录 */
struct CachedRow {
	RowId	m_rid;			/** 记录ID */
	uint	m_totalSize;	/** 总大小 */
	byte	m_dat[1];		/** 记录数据序列化起始位置 */
};

#ifdef NTSE_UNIT_TEST
class RowCacheTestCase;
#endif

/** 记录缓存。这一缓存是给NTSE存储引擎的handler使用的，
 * MySQL在处理一些查询时，如Filesort，会进行表扫描或索引扫描，记录
 * 当前记录的RID，然后再来根据RID重复取这条记录。
 * 由于NTSE不能锁定多条记录，为了保证缓存的RID的记录重复读取时能读到
 * 与当初一样的记录，使用本数据结果来缓存这些记录的内容。
 * 由于这一数据结构只被单个handler使用，不会进行并发控制。
 */
class RowCache {
public:
	RowCache(size_t maxSize, const TableDef *tableDef, u16 numCols, const u16 *cols, const MY_BITMAP *readSet);
	~RowCache();
	long put(RowId rid, const byte *buf);
	void get(long id, byte *buf) const;
	bool hasAttrs(const MY_BITMAP *readSet) const;

private:
	const TableDef	*m_tableDef;/** 表定义 */
	u16			m_numLob;		/** 大对象字段个数 */
	size_t		m_maxSize;		/** 最大占用内存量 */
	u16			m_numCols;		/** 属性数 */
	u16			*m_cols;		/** 各属性号 */
	MemoryContext	*m_ctx;		/** 用于分配缓存记录所用内存的内存分配上下文 */
	Array<CachedRow *>	m_rows;	/** 缓存记录数组 */
	bool		m_isFull;		/** 是否满了 */
	uint		m_maxVSize;		/** 要存储的属性转化为REC_VARLEN格式后最大大小，不含大对象 */
	MY_BITMAP	m_readSet;    	/** 缓存记录包含的属性以bitmap形式的表示 */
	uint32		*m_rsBuf;		/** m_readSet所用来存储位图的内存 */

#ifdef NTSE_UNIT_TEST
public:
	friend class ::RowCacheTestCase;
#endif
};

#endif

