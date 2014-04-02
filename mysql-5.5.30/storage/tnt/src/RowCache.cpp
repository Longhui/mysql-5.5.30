/**
 * 记录缓存实现
 *
 * @author 汪源(wangyuan@corp.netease.com, wy@163.org)
 */

#ifdef WIN32
#include <my_global.h>
#include <sql_priv.h>
#include <sql_class.h>
#endif
#include "api/Table.h"
#include "RowCache.h"
#include "util/Stream.h"

using namespace ntse;

/** 创建一个记录缓存
 * @param maxSize 最大大小，即占用的内存空间
 * @param tableDef 用于缓存这个表的记录
 * @param numCols 要缓存的记录中包含的属性个数
 * @param cols 要缓存的记录中包含的各个属性。拷贝一份存储
 * @param readSet 要缓存的属性以bitmap形式的表示，拷贝使用
 */
RowCache::RowCache(size_t maxSize, const TableDef *tableDef, u16 numCols, const u16 *cols, const MY_BITMAP *readSet) {
	m_maxSize = maxSize;
	m_tableDef = tableDef;
	m_ctx = new MemoryContext(Limits::PAGE_SIZE, 1);
	m_numCols = numCols;
	m_cols = (u16 *)m_ctx->alloc(sizeof(u16) * numCols);
	memcpy(m_cols, cols, sizeof(u16) * numCols);
	m_numLob = 0;
	m_maxVSize = tableDef->m_bmBytes;
	for (u16 i = 0; i < numCols; i++) {
		m_maxVSize += tableDef->m_columns[cols[i]]->m_size;
		if (tableDef->m_columns[cols[i]]->isLob()) {
			m_numLob++;
		}
	}
	m_isFull = false;
	m_rsBuf = new uint32[bitmap_buffer_size(tableDef->m_numCols)];
	bitmap_init(&m_readSet, m_rsBuf, tableDef->m_numCols, FALSE);
	bitmap_copy(&m_readSet, readSet);
}

/** 析构函数 */
RowCache::~RowCache() {
	delete m_ctx;
	m_ctx = NULL;
	delete m_rsBuf;
	m_rsBuf = NULL;
}

/** 尝试插入一条记录
 * @param rid 待插入记录的RID
 * @param buf 待插入记录的内容
 * @return 成功ID，失败返回-1
 */
long RowCache::put(RowId rid, const byte *buf) {
	if (m_isFull)
		return -1;
	
	// 构造MYSQL格式的子记录，计算缓存所需空间大小
	SubRecord srR(REC_MYSQL, m_numCols, m_cols, (byte *)buf, m_tableDef->m_maxRecSize);	// TODO: 如果该模块允许缓存REC_REDUNDANT格式的记录，该格式应该作为参数传入
	size_t allocSize = offsetof(CachedRow, m_dat) + RecordOper::getSubRecordSerializeSize(m_tableDef, &srR, true, true);

	// 分配空间，由于无法预估使用内存上下文是否会向操作系统申请空间
	// 采用先分配再检查是否超出大小限制的方法
	u64 mcSave = m_ctx->setSavepoint();

	CachedRow *row = (CachedRow *)m_ctx->alloc(allocSize);
	if (!row) {
		m_isFull = true;
		return -1;
	}
	if ((m_rows.getMemUsage() + m_ctx->getMemUsage()) > m_maxSize) {
		m_isFull = true;
		m_ctx->resetToSavepoint(mcSave);
		return -1;
	}
       
	// 序列化记录到缓存
	row->m_rid = rid;
	row->m_totalSize = (uint)allocSize;
	Stream s(row->m_dat, allocSize - offsetof(CachedRow, m_dat));
	RecordOper::serializeSubRecordMNR(&s, m_tableDef, &srR, true, true);

	if (!m_rows.push(row)) {
		m_isFull = true;
		m_ctx->resetToSavepoint(mcSave);
		return -1;
	}
	if ((m_rows.getMemUsage() + m_ctx->getMemUsage()) > m_maxSize) {
		m_isFull = true;
		m_rows.pop();
		m_ctx->resetToSavepoint(mcSave);
		return -1;
	}

	return (long)m_rows.getSize() - 1;
}

/** 从缓存中获取一条记录
 * @param id 缓存的记录的ID，即put的返回值
 * @param buf OUT，存储所获取的记录内容
 * @return 缓存中是否存在指定记录
 */
void RowCache::get(long id, byte *buf) const {
	assert(id >= 0 && (size_t)id < m_rows.getSize());
	CachedRow *row = m_rows[id];
	Stream s(row->m_dat, row->m_totalSize - offsetof(CachedRow, m_dat));
	RecordOper::unserializeSubRecordMNR(&s, m_tableDef, m_numCols, m_cols, buf);
}

/** 检查缓存的记录是否包含指定的属性
 * @param readSet 需要的属性
 * @return 缓存的记录是否包含指定的属性
 */
bool RowCache::hasAttrs(const MY_BITMAP *readSet) const {
	return bitmap_is_subset(readSet, &m_readSet);
}
