/**
 * ��¼����ʵ��
 *
 * @author ��Դ(wangyuan@corp.netease.com, wy@163.org)
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

/** ����һ����¼����
 * @param maxSize ����С����ռ�õ��ڴ�ռ�
 * @param tableDef ���ڻ��������ļ�¼
 * @param numCols Ҫ����ļ�¼�а��������Ը���
 * @param cols Ҫ����ļ�¼�а����ĸ������ԡ�����һ�ݴ洢
 * @param readSet Ҫ�����������bitmap��ʽ�ı�ʾ������ʹ��
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

/** �������� */
RowCache::~RowCache() {
	delete m_ctx;
	m_ctx = NULL;
	delete m_rsBuf;
	m_rsBuf = NULL;
}

/** ���Բ���һ����¼
 * @param rid �������¼��RID
 * @param buf �������¼������
 * @return �ɹ�ID��ʧ�ܷ���-1
 */
long RowCache::put(RowId rid, const byte *buf) {
	if (m_isFull)
		return -1;
	
	// ����MYSQL��ʽ���Ӽ�¼�����㻺������ռ��С
	SubRecord srR(REC_MYSQL, m_numCols, m_cols, (byte *)buf, m_tableDef->m_maxRecSize);	// TODO: �����ģ��������REC_REDUNDANT��ʽ�ļ�¼���ø�ʽӦ����Ϊ��������
	size_t allocSize = offsetof(CachedRow, m_dat) + RecordOper::getSubRecordSerializeSize(m_tableDef, &srR, true, true);

	// ����ռ䣬�����޷�Ԥ��ʹ���ڴ��������Ƿ�������ϵͳ����ռ�
	// �����ȷ����ټ���Ƿ񳬳���С���Ƶķ���
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
       
	// ���л���¼������
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

/** �ӻ����л�ȡһ����¼
 * @param id ����ļ�¼��ID����put�ķ���ֵ
 * @param buf OUT���洢����ȡ�ļ�¼����
 * @return �������Ƿ����ָ����¼
 */
void RowCache::get(long id, byte *buf) const {
	assert(id >= 0 && (size_t)id < m_rows.getSize());
	CachedRow *row = m_rows[id];
	Stream s(row->m_dat, row->m_totalSize - offsetof(CachedRow, m_dat));
	RecordOper::unserializeSubRecordMNR(&s, m_tableDef, m_numCols, m_cols, buf);
}

/** ��黺��ļ�¼�Ƿ����ָ��������
 * @param readSet ��Ҫ������
 * @return ����ļ�¼�Ƿ����ָ��������
 */
bool RowCache::hasAttrs(const MY_BITMAP *readSet) const {
	return bitmap_is_subset(readSet, &m_readSet);
}
