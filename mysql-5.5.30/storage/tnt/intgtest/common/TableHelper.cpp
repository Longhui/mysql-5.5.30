#include "TableHelper.h"
#include "util/File.h"
#include "btree/IndexBPTreesManager.h"

/**
 * ��ɨ��
 * @param db ���ݿ�
 * @param table ��
 * @param numCols Ҫ��ȡ��������
 * @param columns Ҫ��ȡ�ĸ����Ժţ���0��ʼ�������ź���ֱ��ʹ���ڴ治�������ڴ��MemoryContext�з��䣬
 *   endScan���Զ��ͷ�
 * @return ɨ����ļ�¼��
 */
u64 tableScan(Database *db, Table *table, u16 numCols, u16 *columns) {
	byte *buf = new byte[table->getTableDef()->m_maxRecSize];
	Connection *conn = db->getConnection(false);
	Session *session = db->getSessionManager()->allocSession("::scanTable", conn);
	TblScan *scanHandle = table->tableScan(session, OP_READ, numCols, columns);

	u64 opCnt = 0;
	while(table->getNext(scanHandle, buf))
		opCnt++;
	table->endScan(scanHandle);

	db->getSessionManager()->freeSession(session);
	db->freeConnection(conn);
	delete [] buf;
	return opCnt;
}

/**
 * ����ɨ�裬���õ�0������
 * @param db ���ݿ�
 * @param table ��
 * @param numCols Ҫ��ȡ��������
 * @param columns Ҫ��ȡ�ĸ����Ժţ���0��ʼ�������ź���ֱ��ʹ���ڴ治�������ڴ��MemoryContext�з��䣬
 *   endScan���Զ��ͷ�
 * @param maxRec ɨ��ļ�¼���ޣ�-1����ɨ�����м�¼
 * @return ɨ����ļ�¼��
 */
u64 index0Scan(Database *db, Table *table, u16 numCols, u16 *columns, u64 maxRec) {
	Connection *conn = db->getConnection(false);
	Session *session = db->getSessionManager()->allocSession("::index0Scan", conn);

	byte *buf = new byte [table->getTableDef()->m_maxRecSize];
	IndexScanCond cond(0, NULL, true, true, false);
	TblScan *scanHandle = table->indexScan(session, OP_READ, &cond, numCols, columns);

	u64 opCnt = 0;
	for (; opCnt < maxRec && table->getNext(scanHandle, buf); opCnt++)
		;
	table->endScan(scanHandle);
	delete [] buf;
	db->getSessionManager()->freeSession(session);
	db->freeConnection(conn);
	return opCnt;
}

/**
 * �����Ż�������ȫɨ���������ĳ�������ļ�������ҳ��鶼���뵽�ڴ浱��
 *
 * ������õİ취��ͨ����ȡ���������ļ����ڴ�ʵ��Ԥ�ȣ�ʵ�ֹ���֪����������ʵ��ϸ��
 *
 * @param db	���ݿ�
 * @param table	�����
 */
void indexFullScan(Database *db, Table *table) {
	Connection *conn = db->getConnection(false);
	Session *session = db->getSessionManager()->allocSession("::indexFullScan", conn);

	TableDef *tableDef = table->getTableDef();
	char indexName[255];
	sprintf(indexName, "%s/%s.nsi", db->getConfig()->m_tmpdir, tableDef->m_name);

	DrsIndice *indice = table->getIndice();
	File *file = ((DrsBPTreeIndice*)indice)->getFileDesc();

	u64 size, errNo;
	errNo = file->getSize(&size);
	assert(File::getNtseError(errNo) == File::E_NO_ERROR);
	u64 pages = size / Limits::PAGE_SIZE;

	for (uint i = 0; i < pages; i++) {
		BufferPageHandle *handle = GET_PAGE(session, file, PAGE_INDEX, i, Shared, indice->getDBObjStats(), NULL);
		session->releasePage(&handle);
	}
}