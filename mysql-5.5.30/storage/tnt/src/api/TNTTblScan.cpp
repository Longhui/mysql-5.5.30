/**
* TNT���棬ɨ����ģ�顣
*
* @author �εǳ�
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

/**	����ɨ��������Ϣ��ȷ����ǰɨ��ļ���ģʽ
*	@pre ����ģʽ����external lock��store lock����������
*	@pre ����ģʽ��ͨ������ģʽ�Ƶ��ó�
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

/**	����Handlerģ�鴫���select lock mode��Ϣ��ȷ������scan�ļ���ģʽ
*	@pre ����ģʽ����external lock��store lock����������
*	@pre ����ģʽ��ͨ������ģʽ�Ƶ��ó�
*	@post �˺�����Handlerģ����ã���ͬ��NTSE����ģʽ�ɲ���ģʽ�Ƶ�����
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

/**	����rowid���Աȴ�rowid�Ŀɼ��汾�����������ؼ�¼���Ƿ���ȫһ��
*	@pre ������¼��	scan->m_idxRec��SubRecord��REC_REDUNDANT��ʽ
*	@pre �Ѽ�¼��	scan->m_fullRec��Record��REC_REDUNDANT��ʽ
*	@return �Ѽ�¼��������key��ͬ������true�����򷵻�false
*/
bool TNTTblScan::doubleCheckRecord() {
	McSavepoint lobSavepoint(m_session->getLobContext());
	int compareResult; 
	SubRecord *key = m_fullRow;
	// ������ȡǰ׺��¼
	if (m_indexDef->m_prefix) {
		Array<LobPair*> lobArray;
		if (m_indexDef->hasLob()) {
			RecordOper::extractLobFromR(m_session, m_tableDef, m_indexDef, m_tntTable->getNtseTable()->getLobStorage(), m_fullRec, &lobArray);
		}
		// ǰ׺�б�Ȼ�ǲ��ܿ��ٱȽϵ����ͣ���˼�¼�Ѿ���ת����PAD��ʽ��
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

/**	��ȡ�û�ָ���Ĵ��ֶ�����
*
*
*/
void TNTTblScan::readLobs() {
	assert(m_mysqlRow->m_format == REC_MYSQL);
	SubRecord* subRowPoint;
	//�ж��ĸ�SubRecord�ǿɼ���
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

/**	�ͷ���һ�����е���Դ
*	@bRetainLobs	�Ƿ���Lob�ռ�
*
*/
void TNTTblScan::releaseLastRow(bool bRetainLobs) {
	
	// �ײ㲻��Ҫ���Ǵ����Ķ�ȡ���ͷ�
	if (m_recInfo != NULL) {
		m_recInfo->releaseLastRow(bRetainLobs);
	}

	// ע�⣺����bRetainLobs������BulkOperation��releaseLastRow���жϣ���˴˴���ʱ�������ж�
	// if (!bRetainLobs && !m_externalLobCtx)
	//    m_lobCtx->reset();

	m_fullRowFetched = false;
	m_lastRecPlace	 = LAST_REC_NO_WHERE;
}

/**	��update������ָ�����µ��У�ƴװΪSubRecord�����Ҽ����Ƿ����Lob�ֶ�
*	@numCols	���²�����ָ��������
*	@columns	�к�����
*	@needCreate	�Ƿ���Ҫ�½�m_updSubRec
*/
void TNTTblScan::setUpdateSubRecord(u16 numCols, u16 *columns, bool needCreate)  {
	if (needCreate) {
		assert(m_updateRed == NULL && m_updateMysql == NULL);

		void *p = m_session->getMemoryContext()->alloc(sizeof(SubRecord));
		m_updateRed = new (p)SubRecord(REC_REDUNDANT, numCols, columns, NULL, m_tableDef->m_maxRecSize);
		void *p1 = m_session->getMemoryContext()->alloc(sizeof(SubRecord));
		m_updateMysql = new (p1)SubRecord(REC_MYSQL, numCols, columns, NULL, m_tableDef->m_maxRecSize);

		//���Ƶ�ԭ�򣬺��д����ı��һ����Ҫ����һ�ݸ��º����ϸ�μ�Jira ###NTSETNT-4###
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

/**	�õ�ɨ�����Ͷ�Ӧ�ı�ɨ�����ͳ������
*	@scanType	ɨ������
*	@return		ͳ������
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

/**	�õ�ɨ�����Ͷ�Ӧ����ɨ�����ͳ������
*	@scanType	ɨ������
*	@return		ͳ������
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

/**	����Ƿ��Ѿ�EOF��
*	@return	�Ƿ��Ѿ�EOF
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