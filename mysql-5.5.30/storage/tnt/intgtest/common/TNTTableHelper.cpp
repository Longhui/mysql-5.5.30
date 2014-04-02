#include "TNTTableHelper.h"
#include "api/Table.h"

using namespace ntse;
/**
 * 表扫描
 * @param db 数据库
 * @param table 表
 * @param numCols 要读取的属性数
 * @param columns 要读取的各属性号，从0开始，递增排好序。直接使用内存不拷贝，内存从MemoryContext中分配，
 *   endScan后自动释放
 * @return 扫描过的记录数
 */
u64 TNTTableScan(TNTDatabase *db, TNTTable *table, u16 numCols, u16 *columns) {
	TNTOpInfo opInfo;
	initTNTOpInfo(&opInfo, TL_NO);
	Database *ntsedb = db->getNtseDb();
	byte *buf = new byte[table->getNtseTable()->getTableDef()->m_maxRecSize];
	Connection *conn = ntsedb->getConnection(false);
	Session *session = ntsedb->getSessionManager()->allocSession("::TNTTableScan", conn);
	TNTTransaction *trx = startTrx(db->getTransSys(), session);
	TNTTblScan *scanHandle = table->tableScan(session, OP_READ, &opInfo, numCols, columns);

	u64 opCnt = 0;
	while(table->getNext(scanHandle, buf))
		opCnt++;
	table->endScan(scanHandle);

	commitTrx(db->getTransSys(), trx);
	ntsedb->getSessionManager()->freeSession(session);
	ntsedb->freeConnection(conn);
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
u64 TNTIndex0Scan(TNTDatabase *db, TNTTable *table, u16 numCols, u16 *columns, u64 maxRec) {
	TNTOpInfo opInfo;
	initTNTOpInfo(&opInfo, TL_NO);
	Database *ntsedb = db->getNtseDb();
	Connection *conn = ntsedb->getConnection(false);
	Session *session = ntsedb->getSessionManager()->allocSession("::TNTIndex0Scan", conn);
	TNTTransaction *trx = startTrx(db->getTransSys(), session);

	byte *buf = new byte [table->getNtseTable()->getTableDef()->m_maxRecSize];
	IndexScanCond cond(0, NULL, true, true, false);
	TNTTblScan *scanHandle = table->indexScan(session, OP_READ, &opInfo, &cond, numCols, columns);

	u64 opCnt = 0;
	for (; opCnt < maxRec && table->getNext(scanHandle, buf); opCnt++)
		;
	table->endScan(scanHandle);
	commitTrx(db->getTransSys(), trx);
	delete [] buf;
	ntsedb->getSessionManager()->freeSession(session);
	ntsedb->freeConnection(conn);
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
void TNTIndexFullScan(TNTDatabase *db, TNTTable *table, TNTOpInfo *opInfo) {
	/*Database *ntsedb = db->getNtseDb();
	Connection *conn = ntsedb->getConnection(false);
	Session *session = ntsedb->getSessionManager()->allocSession("::TNTIndexFullScan", conn);

	TableDef *tableDef = table->getNtseTable()->getTableDef();
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
	}*/
}

TNTTransaction *startTrx(TNTTrxSys *trxSys, Session *session, bool log/* = false*/) {
	TNTTransaction *trx = trxSys->allocTrx();
	if (!log) {
		trx->disableLogging();
	}
	trx->startTrxIfNotStarted(session->getConnection());
	session->setTrans(trx);
	return trx;
}

void commitTrx(TNTTrxSys *trxSys, TNTTransaction *trx) {
	trx->commitTrx();
	trxSys->freeTrx(trx);
}

void rollBackTrx(TNTTrxSys *trxSys, TNTTransaction *trx) {
	trx->rollbackTrx();
	trxSys->freeTrx(trx);
}

void initTNTOpInfo(TNTOpInfo *opInfo, TLockMode mode) {
	opInfo->m_selLockType = mode;
	opInfo->m_sqlStatStart = true;
	opInfo->m_mysqlHasLocked = false;
	opInfo->m_mysqlOper = true;
}