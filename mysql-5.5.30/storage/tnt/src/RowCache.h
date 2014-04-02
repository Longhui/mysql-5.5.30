/**
 * ��¼����
 *
 * @author ��Դ(wangyuan@corp.netease.com, wy@163.org)
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

/** ��¼�����еļ�¼ */
struct CachedRow {
	RowId	m_rid;			/** ��¼ID */
	uint	m_totalSize;	/** �ܴ�С */
	byte	m_dat[1];		/** ��¼�������л���ʼλ�� */
};

#ifdef NTSE_UNIT_TEST
class RowCacheTestCase;
#endif

/** ��¼���档��һ�����Ǹ�NTSE�洢�����handlerʹ�õģ�
 * MySQL�ڴ���һЩ��ѯʱ����Filesort������б�ɨ�������ɨ�裬��¼
 * ��ǰ��¼��RID��Ȼ����������RID�ظ�ȡ������¼��
 * ����NTSE��������������¼��Ϊ�˱�֤�����RID�ļ�¼�ظ���ȡʱ�ܶ���
 * �뵱��һ���ļ�¼��ʹ�ñ����ݽ����������Щ��¼�����ݡ�
 * ������һ���ݽṹֻ������handlerʹ�ã�������в������ơ�
 */
class RowCache {
public:
	RowCache(size_t maxSize, const TableDef *tableDef, u16 numCols, const u16 *cols, const MY_BITMAP *readSet);
	~RowCache();
	long put(RowId rid, const byte *buf);
	void get(long id, byte *buf) const;
	bool hasAttrs(const MY_BITMAP *readSet) const;

private:
	const TableDef	*m_tableDef;/** ���� */
	u16			m_numLob;		/** ������ֶθ��� */
	size_t		m_maxSize;		/** ���ռ���ڴ��� */
	u16			m_numCols;		/** ������ */
	u16			*m_cols;		/** �����Ժ� */
	MemoryContext	*m_ctx;		/** ���ڷ��仺���¼�����ڴ���ڴ���������� */
	Array<CachedRow *>	m_rows;	/** �����¼���� */
	bool		m_isFull;		/** �Ƿ����� */
	uint		m_maxVSize;		/** Ҫ�洢������ת��ΪREC_VARLEN��ʽ������С����������� */
	MY_BITMAP	m_readSet;    	/** �����¼������������bitmap��ʽ�ı�ʾ */
	uint32		*m_rsBuf;		/** m_readSet�������洢λͼ���ڴ� */

#ifdef NTSE_UNIT_TEST
public:
	friend class ::RowCacheTestCase;
#endif
};

#endif

