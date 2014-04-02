/**
* TNT引擎，扫描句柄模块。
*
* @author 何登成
*/

#include "api/TNTTblScan.h"
#include "api/TNTTable.h"
#include "rec/Records.h"
#include "misc/Record.h"
#include "trx/TNTTransaction.h"
#include "btree/IndexKey.h"

using namespace ntse;

namespace tnt {

TNTTblScan::TNTTblScan() {
	memset(this, 0, sizeof(TNTTblScan));
}

/**	根据扫描事务信息，确定当前扫描的加锁模式
*	@pre 行锁模式，在external lock，store lock函数中设置
*	@pre 表锁模式，通过行锁模式推导得出
*/
void TNTTblScan::determineRowLockPolicy() {
	/*
	TLockMode lockMode = m_trx->getLockMode();
	
	m_rowLockMode = lockMode;
	if (lockMode == TL_NO)
		m_tabLockMode = TL_NO;
	else if (lockMode == TL_S)
		m_tabLockMode = TL_IS;
	else if (lockMode == TL_X)
		m_tabLockMode = TL_IX;
	else
		assert(false);
	*/
}

/**	根据Handler模块传入的select lock mode信息，确定本次scan的加锁模式
*	@pre 行锁模式，在external lock，store lock函数中设置
*	@pre 表锁模式，通过行锁模式推导得出
*	@post 此函数在Handler模块调用，不同于NTSE，锁模式由操作模式推导而来
*/
void TNTTblScan::determineScanLockMode(TLockMode selLockMode) {
	m_rowLockMode = selLockMode;
	if (selLockMode == TL_NO) 
		m_tabLockMode = TL_NO;
	else if (selLockMode == TL_S)
		m_tabLockMode = TL_IS;
	else if (selLockMode == TL_X)
		m_tabLockMode = TL_IX;
	else
		assert(false);
}

/**	给定rowid，对比此rowid的可见版本，与索引返回记录，是否完全一致
*	@pre 索引记录：	scan->m_idxRec，SubRecord，REC_REDUNDANT格式
*	@pre 堆记录：	scan->m_fullRec，Record，REC_REDUNDANT格式
*	@return 堆记录，与索引key相同，返回true；否则返回false
*/
bool TNTTblScan::doubleCheckRecord() {
	McSavepoint lobSavepoint(m_session->getLobContext());
	int compareResult; 
	SubRecord *key = m_fullRow;
	// 用于提取前缀记录
	if (m_indexDef->m_prefix) {
		Array<LobPair*> lobArray;
		if (m_indexDef->hasLob()) {
			RecordOper::extractLobFromR(m_session, m_tableDef, m_indexDef, m_tntTable->getNtseTable()->getLobStorage(), m_fullRec, &lobArray);
		}
		// 前缀列必然是不能快速比较的类型，因此记录已经被转成了PAD格式键
		key = IndexKey::allocSubRecord(m_session->getMemoryContext(), false, m_fullRec, &lobArray, m_tableDef, m_indexDef);
		compareResult = RecordOper::compareKeyPP(m_tableDef, m_idxKey, key, m_indexDef);
	} else {
		compareResult = RecordOper::compareKeyPR(m_tableDef, m_idxKey, key, m_indexDef);
	}


	if (compareResult == 0)
		return true;
	else 
		return false;
}

/**	读取用户指定的大字段属性
*
*
*/
void TNTTblScan::readLobs() {
	assert(m_mysqlRow->m_format == REC_MYSQL);
	SubRecord* subRowPoint;
	//判断哪个SubRecord是可见的
	if(m_lastRecPlace == LAST_REC_IN_FULLROW){
		assert(m_fullRow->m_format == REC_REDUNDANT);
		subRowPoint = m_fullRow;
	}
	else{
		assert(m_redRow->m_format == REC_REDUNDANT);
		subRowPoint = m_redRow;
	}
	Records::BulkFetch *fetch = (Records::BulkFetch *)(m_recInfo);
	if(fetch)
		fetch->readLob(subRowPoint, m_mysqlRow);
	else {
		for (u16 col = 0; col < m_readCols.m_size; col++) {
			u16 cno = m_readCols.m_cols[col];
			ColumnDef *columnDef = m_tableDef->m_columns[cno];
			if (!columnDef->isLob())
				continue;
			uint lobSize = 0;
			byte *lob = NULL;
			if (!RecordOper::isNullR(m_tableDef, subRowPoint, cno)) {
				LobId lobId = RecordOper::readLobId(subRowPoint->m_data, columnDef);
				assert(lobId != INVALID_LOB_ID);
				assert(RecordOper::readLobSize(subRowPoint->m_data, columnDef) == 0);
				Records *ntseRecs = m_tntTable->getRecords();
				lob = ntseRecs->getLobStorage()->get(m_session, m_lobCtx, lobId, &lobSize, false);
				assert(lob);
			}
			RecordOper::writeLobSize(m_mysqlRow->m_data, columnDef, lobSize);
			RecordOper::writeLob(m_mysqlRow->m_data, columnDef, lob);
		}
	}
}

/**	释放上一返回行的资源
*	@bRetainLobs	是否保留Lob空间
*
*/
void TNTTblScan::releaseLastRow(bool bRetainLobs) {
	
	// 底层不需要考虑大对象的读取与释放
	if (m_recInfo != NULL) {
		m_recInfo->releaseLastRow(bRetainLobs);
	}

	// 注意：由于bRetainLobs参数，BulkOperation的releaseLastRow会判断，因此此处暂时不进行判断
	// if (!bRetainLobs && !m_externalLobCtx)
	//    m_lobCtx->reset();

	m_fullRowFetched = false;
	m_lastRecPlace	 = LAST_REC_NO_WHERE;
}

/**	将update操作，指定更新的列，拼装为SubRecord，并且计算是否更新Lob字段
*	@numCols	更新操作，指定的列数
*	@columns	列号数组
*	@needCreate	是否需要新建m_updSubRec
*/
void TNTTblScan::setUpdateSubRecord(u16 numCols, u16 *columns, bool needCreate)  {
	if (needCreate) {
		assert(m_updateRed == NULL && m_updateMysql == NULL);

		void *p = m_session->getMemoryContext()->alloc(sizeof(SubRecord));
		m_updateRed = new (p)SubRecord(REC_REDUNDANT, numCols, columns, NULL, m_tableDef->m_maxRecSize);
		void *p1 = m_session->getMemoryContext()->alloc(sizeof(SubRecord));
		m_updateMysql = new (p1)SubRecord(REC_MYSQL, numCols, columns, NULL, m_tableDef->m_maxRecSize);

		//复制的原因，含有大对象的表就一定需要拷贝一份更新后项，详细参加Jira ###NTSETNT-4###
		if(m_tableDef->hasLob())
			m_updateRed->m_data = (byte *)m_session->getMemoryContext()->alloc(m_tableDef->m_maxRecSize); 

	} else {
		assert(m_updateRed != NULL && m_updateMysql != NULL);

		m_updateRed->resetColumns(numCols, columns);
		m_updateMysql->resetColumns(numCols, columns);
		NTSE_ASSERT(m_updateRed->m_size == m_tableDef->m_maxRecSize && m_updateMysql->m_size == m_tableDef->m_maxRecSize); 
	}

	
	if (m_tableDef->hasLob(numCols, columns)) {
		m_lobUpdated = true;
	} else
		m_lobUpdated = false;
}

/**	得到扫描类型对应的表扫描操作统计类型
*	@scanType	扫描类型
*	@return		统计类型
*/
StatType TNTTblScan::getTblStatTypeForScanType(ScanType scanType) {
	if (scanType == ST_TBL_SCAN)
		return OPS_TBL_SCAN;
	else if (scanType == ST_IDX_SCAN)
		return OPS_IDX_SCAN;
	else {
		assert(scanType == ST_POS_SCAN);
		return OPS_POS_SCAN;
	}
}

/**	得到扫描类型对应的行扫描操作统计类型
*	@scanType	扫描类型
*	@return		统计类型
*/
StatType TNTTblScan::getRowStatTypeForScanType(ScanType scanType) {
	if (scanType == ST_TBL_SCAN)
		return OPS_TBL_SCAN_ROWS;
	else if (scanType == ST_IDX_SCAN)
		return OPS_IDX_SCAN_ROWS;
	else {
		assert(scanType == ST_POS_SCAN);
		return OPS_POS_SCAN_ROWS;
	}
}

/**	检查是否已经EOF了
*	@return	是否已经EOF
*
*/
bool TNTTblScan::checkEofOnGetNext() {
	if (m_eof)
		return true;
	if (m_scanType == ST_IDX_SCAN && m_singleFetch && !m_bof) {
		m_eof = true;
		return true;
	}
	return false;
}
}