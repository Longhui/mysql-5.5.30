#include "TableHelper.h"
#include "util/File.h"
#include "btree/IndexBPTreesManager.h"

/**
 * 表扫描
 * @param db 数据库
 * @param table 表
 * @param numCols 要读取的属性数
 * @param columns 要读取的各属性号，从0开始，递增排好序。直接使用内存不拷贝，内存从MemoryContext中分配，
 *   endScan后自动释放
 * @return 扫描过的记录数
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
 * 索引扫描，采用第0个索引
 * @param db 数据库
 * @param table 表
 * @param numCols 要读取的属性数
 * @param columns 要读取的各属性号，从0开始，递增排好序。直接使用内存不拷贝，内存从MemoryContext中分配，
 *   endScan后自动释放
 * @param maxRec 扫描的记录上限，-1代表扫描所有记录
 * @return 扫描过的记录数
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
 * 不做优化的索引全扫描操作，将某个索引文件的所有页面块都读入到内存当中
 *
 * 这里采用的办法是通过读取整个索引文件到内存实现预热，实现过程知道了索引的实现细节
 *
 * @param db	数据库
 * @param table	表对象
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