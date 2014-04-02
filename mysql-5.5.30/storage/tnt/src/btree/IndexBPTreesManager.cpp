/**
 * NTSE B+������������
 *
 * author: naturally (naturally@163.org)
 */

// IndexBPTreesManager.cpp: implementation of the DrsBPTreeIndice class.
//
////////////////////////////////////////////////////////////////////

#include "api/Database.h"
#include "btree/IndexBPTreesManager.h"
#include "btree/IndexBPTree.h"
#include "btree/IndexPage.h"
#include "btree/OuterSorter.h"
#include "btree/IndexLog.h"
#include "btree/IndexKey.h"
#include "util/PagePool.h"
#include "util/Sync.h"
#include "util/File.h"
#include "misc/Syslog.h"
#include "misc/Buffer.h"
#include "misc/Record.h"
#include "misc/Session.h"
#include "misc/Trace.h"
#include "api/Table.h"
#include "misc/Txnlog.h"
#include "util/Thread.h"
#include "util/Stream.h"
#include <vector>
#include <algorithm>
#include "misc/Profile.h"
#include <iostream>
#include <string>
#include <sstream>
using namespace std;

namespace ntse {


/**
 * Indice���ʼ�����캯��
 * @param db			���ݿ����
 * @param tableDef		����
 * @param file			�ļ�����������������䣬������Ҫ�����������ͷ�
 * @param lobStorage	����������
 * @param headerPage	ͷҳ��
 * @param dbObjStats	���ݿ�ͳ�ƶ���
 */
DrsBPTreeIndice::DrsBPTreeIndice(Database *db, const TableDef *tableDef, File *file, LobStorage *lobStorage, Page *headerPage, DBObjStats* dbObjStats) {
	ftrace(ts.idx, tout << file);

	m_db = db;
	m_file = file;
	m_indice = new DrsIndex* [Limits::MAX_INDEX_NUM];
	m_tableDef = tableDef;
	m_lobStorage = lobStorage;

	m_indexNum = (u16)((IndexHeaderPage*)headerPage)->m_indexNum;
	m_pagesManager = new IndicePageManager(tableDef, file, headerPage, db->getSyslog(), dbObjStats);
	m_logger = new IndexLog(tableDef->m_id);
	m_mutex = new Mutex("DrsBPTreeIndice::mutex", __FILE__, __LINE__);
	m_newIndexId = -1;
	m_dboStats = dbObjStats;

	if (db->getStat() >= DB_RUNNING) {
		NTSE_ASSERT(tableDef->m_numIndice >= m_indexNum);
	} else {
		m_indexNum = min((u16)tableDef->m_numIndice, m_indexNum);
	}

	for (u16 i = 0; i < m_indexNum; i++) {
		IndexDef *indexDef = tableDef->m_indice[i];
		DrsBPTreeIndex *index = new DrsBPTreeIndex(this, tableDef, indexDef, ((IndexHeaderPage*)headerPage)->m_indexIds[i], ((IndexHeaderPage*)headerPage)->m_rootPageIds[i]);
		m_indice[i] = index;
		m_orderedIndices.add(i, index->getIndexId(), indexDef->m_unique);
	}
}


/**
 * ����һ����¼ʱ�������������е����������С�
 * ���������Ψһ�Գ�ͻ������ģ��Ӧ�Զ��ع��Ѿ�����������
 *
 * @param session		�Ự���
 * @param record		�²���ļ�¼���ݣ�����RID
 * @param dupIndex		OUT	��������ͻ��������ͻ������
 * @return �Ƿ�ɹ���true��ʾ�ɹ���false��ʾΨһ�Գ�ͻ
 */
bool DrsBPTreeIndice::insertIndexEntries(Session *session, const Record *record, uint *dupIndex) {
	PROFILE(PI_DrsIndice_insertIndexEntries);
	ftrace(ts.irl, tout << session->getId() << record);

	u64 opLSN = m_logger->logDMLUpdateBegin(session);
	bool result = true;
	bool duplicateKey;
	MemoryContext *memoryContext = session->getMemoryContext();
	u64 savePoint = memoryContext->setSavepoint();

	for (u8 i = 0; i < m_indexNum; i++) {
		McSavepoint lobSavepoint(session->getLobContext());
		// ʹ��m_indexSeq���鱣֤�ȸ���Ψһ����
		u16 realInsertIndexNo = m_orderedIndices.getOrder(i);
		DrsBPTreeIndex *index = (DrsBPTreeIndex*)m_indice[realInsertIndexNo];
		const IndexDef *indexDef = index->getIndexDef();
		bool keyNeedCompress = RecordOper::isFastCCComparable(m_tableDef, indexDef, indexDef->m_numCols, indexDef->m_columns);
		// ƴװ������
		Array<LobPair*> lobArray;
		if (indexDef->hasLob()) {
			RecordOper::extractLobFromR(session, m_tableDef, indexDef, m_lobStorage, record, &lobArray);
		}

		SubRecord *key = IndexKey::allocSubRecord(memoryContext, keyNeedCompress, record, &lobArray, m_tableDef, indexDef);


		u64 token = session->getToken();
		bool success;
		while ((success = index->insert(session, key, &duplicateKey)) == false && !duplicateKey) {
			nftrace(ts.irl, tout << session->getId() << " I dl and redo");
			session->unlockIdxObjects(token);	// ���ڳ����������������Ҫ�����ٲ���
			index->statisticDL();
		}
		NTSE_ASSERT(!success || !duplicateKey);

		// Υ��Ψһ��Լ�����������в���
		if (duplicateKey) {
			nftrace(ts.irl, tout << session->getId() << " I dk and rb all");

			*dupIndex = realInsertIndexNo;

			for (u8 j = 0; j < i; j++) {
				u16 realUndoIdxNo = m_orderedIndices.getOrder(j);
				const IndexDef *indexDef = ((DrsBPTreeIndex*)m_indice[realUndoIdxNo])->getIndexDef();
				bool keyNeedCompress = RecordOper::isFastCCComparable(m_tableDef, indexDef, indexDef->m_numCols, indexDef->m_columns);
				// ƴװ������			
				if (indexDef->m_prefix) {
					RecordOper::extractLobFromR(session, m_tableDef, indexDef, m_lobStorage, record, &lobArray);
				}
				SubRecord *key = IndexKey::allocSubRecord(memoryContext, keyNeedCompress, record, &lobArray, m_tableDef, indexDef);
				index = (DrsBPTreeIndex*)m_indice[realUndoIdxNo];

				// ����Ļ��˲������ܻᵼ��SMO�����ǲ�Ӧ�û�������������ͬupdateIndexEntries�����˵��
				NTSE_ASSERT(index->del(session, key));
				index->statisticOp(IDX_INSERT, -1);
			}

			result = false;
			//ftrace(ts.idx, tout.setAutoComma(true) << "I: " << *dupIndex << "Record id: " << record->m_rowId;);
			session->setTxnDurableLsn(session->getLastLsn());		// Ψһ����������׶λ��ˣ���Ҫ���ÿɳ־û�LSN
			session->unlockIdxAllObjects();
			break;
		}

		index->statisticOp(IDX_INSERT, 1);
		m_logger->logDMLDoneUpdateIdxNo(session, i);

		// �������Ψһ����������ϣ����ÿɳ־û�LSN
		if (i + 1 == m_orderedIndices.getUniqueIdxNum())
			session->setTxnDurableLsn(session->getLastLsn());
		// ���һ��Ψһ�����Լ����з�Ψһ�����ĸ��½���֮�������������
		if (i + 1 >= m_orderedIndices.getUniqueIdxNum())
			session->unlockIdxAllObjects();
	}

	memoryContext->resetToSavepoint(savePoint);
	m_logger->logDMLUpdateEnd(session, opLSN, result);
	NTSE_ASSERT(!session->hasLocks());

	return result;
}

#ifdef TNT_ENGINE
/**
 * ����һ����¼ʱ�������������е����������С�
 * @pre �Ѿ�����Ψһ�ԣ�һ�����ᷢ��Ψһ�Գ�ͻ
 *
 * @param session		�Ự���
 * @param record		�²���ļ�¼���ݣ�����RID
 */
void DrsBPTreeIndice::insertIndexNoDupEntries(Session *session, const Record *record) {
		assert(REC_REDUNDANT == record->m_format);
	uint indexNum = getIndexNum();
	uint uniqueIndexNum = getUniqueIndexNum();
	MemoryContext *mtx = session->getMemoryContext();
	McSavepoint mcSavepoint(mtx);
	McSavepoint lobSavepoint(session->getLobContext());
	//����������в���������ֵ
	
	u64 opLSN = m_logger->logDMLUpdateBegin(session);
	for (u16 i = 0; i < indexNum; i++) {
		u8 idxNo = (u8)m_orderedIndices.getOrder(i);
		DrsBPTreeIndex *drsIndex = (DrsBPTreeIndex*)m_indice[idxNo];

		const IndexDef *indexDef = m_tableDef->getIndexDef(idxNo);
		bool keyNeedCompress = RecordOper::isFastCCComparable(m_tableDef, indexDef, indexDef->m_numCols, 
			indexDef->m_columns);

		// ƴװ������
		Array<LobPair*> lobArray;
		if (indexDef->hasLob()) {
			RecordOper::extractLobFromR(session, m_tableDef, indexDef, m_lobStorage, record, &lobArray);
		}

		SubRecord *key = IndexKey::allocSubRecord(mtx, keyNeedCompress, record, &lobArray, m_tableDef, indexDef);
		drsIndex->insertNoCheckDuplicate(session, key);

		drsIndex->statisticOp(IDX_INSERT, 1);

		m_logger->logDMLDoneUpdateIdxNo(session, (u8)i);

		// �������Ψһ����������ϣ����ÿɳ־û�LSN
		if ((uint)i + 1 == uniqueIndexNum)
			session->setTxnDurableLsn(session->getLastLsn());

		// ��Ϊ�ز����ܷ���Ψһ��������ͻ������ÿ�������ĸ��½���֮�������������
		session->unlockIdxAllObjects();
	}
	m_logger->logDMLUpdateEnd(session, opLSN, true);
	NTSE_ASSERT(!session->hasLocks());
}



#endif


/**
 * ɾ��һ����¼ʱ������������ɾ����¼��Ӧ��������
 *
 * @param session		�Ự���
 * @param record		��ɾ���ļ�¼���ݣ�����RID��һ����REC_REDUNDANT��ʽ
 * @param scanHandle	ɨ�����������ΪNULL˵����ɾ���ǽ�����ĳ������ɨ������ϵģ�����ʹ��deleteCurrent�޸ĸ�������������
 */
void DrsBPTreeIndice::deleteIndexEntries(Session *session, const Record *record, IndexScanHandle *scanHandle) {
	assert(record->m_rowId != INVALID_ROW_ID);
	PROFILE(PI_DrsIndice_deleteIndexEntries);

	ftrace(ts.irl, tout << session->getId() << record);

	SYNCHERE(SP_IDX_CHECK_BEFORE_DELETE);

	u64 opLSN = m_logger->logDMLUpdateBegin(session);

	DrsIndexRangeScanHandle *handle = (DrsIndexRangeScanHandle*)scanHandle;
	MemoryContext *memoryContext = session->getMemoryContext();
	u64 savePoint = memoryContext->setSavepoint();

	for (u8 i = 0; i < m_indexNum; i++) {	// ��һ��ÿ����������ɾ��
		McSavepoint lobSavepoint(session->getLobContext());

		DrsBPTreeIndex *index = (DrsBPTreeIndex*)m_indice[i];
		const IndexDef *indexDef = index->getIndexDef();

		if (handle != NULL && handle->getScanInfo()->m_indexDef == indexDef) {	// ����ʹ��deleteCurrentɾ��
			while (!index->deleteCurrent(scanHandle)) {
				nftrace(ts.irl, tout << session->getId() << " D dl and redo");
				session->unlockIdxAllObjects();
				index->statisticDL();
			}
			index->statisticOp(IDX_DELETE, 1);
		} else {
			bool keyNeedCompress = RecordOper::isFastCCComparable(m_tableDef, indexDef, indexDef->m_numCols, indexDef->m_columns);
			// ƴװ������
			Array<LobPair*> lobArray;
			if (indexDef->hasLob()) {
				RecordOper::extractLobFromR(session, m_tableDef, indexDef, m_lobStorage, record, &lobArray);
			}
			SubRecord *key = IndexKey::allocSubRecord(memoryContext, keyNeedCompress, record, &lobArray, m_tableDef, indexDef);

			while (!index->del(session, key)) {
				nftrace(ts.irl, tout << session->getId() << " D dl and redo");
				session->unlockIdxAllObjects();
				index->statisticDL();
			}
			index->statisticOp(IDX_DELETE, 1);
		}

		m_logger->logDMLDoneUpdateIdxNo(session, i);
		if (i == 0)
			session->setTxnDurableLsn(session->getLastLsn());
		// �ͷű��β������ܳ��е���Դ��
		NTSE_ASSERT(session->unlockIdxAllObjects());
	}

	memoryContext->resetToSavepoint(savePoint);

	m_logger->logDMLUpdateEnd(session, opLSN, true);

	NTSE_ASSERT(!session->hasLocks());
}


/**
 * ����һ����¼ʱ��������������
 * ���������Ψһ�Գ�ͻ������ģ��Ӧ�Զ��ع��Ѿ����еĲ�����
 * ��֤�������ݵ�һ����
 *
 * @param session		�Ự���
 * @param before		���ٰ������и����漰�����������������Ե�ǰ��һ����REC_REDUNDANT��ʽ
 * @param after			��������������Ҫ���µ��������Եĺ���һ����REC_REDUNDANT��ʽ
 * @param updateLob		�Ƿ���´���󣬶���NTSE��update������Ϊtrue�� ��TNT purge����ʱ������Ϊfalse
 * @param dupIndex		OUT	������³�ͻ��������ͻ������
 * @return �Ƿ�ɹ���true��ʾ����ɹ���false��ʾ��Ψһ�Գ�ͻ
 */
bool DrsBPTreeIndice::updateIndexEntries(Session *session, const SubRecord *before, SubRecord *after, bool updateLob, uint *dupIndex) {
	PROFILE(PI_DrsIndice_updateIndexEntries);

	ftrace(ts.irl, tout << session->getId() << before << after);

	assert(before != NULL && after != NULL);
	assert(before->m_rowId != INVALID_ROW_ID);

	u64 opLSN = m_logger->logDMLUpdateBegin(session);
	bool result = true;
	bool duplicateKey;
	MemoryContext *memoryContext = session->getMemoryContext();
	u64 savePoint = memoryContext->setSavepoint();

	u16 updates, updateUniques;
	u16 *updateIndicesNo;
	getUpdateIndices(memoryContext, after, &updates, &updateIndicesNo, &updateUniques);

	// ����before��after��record��ʽ
	RecordOper::mergeSubRecordRR(m_tableDef, after, before);
	Record rec1(before->m_rowId, REC_REDUNDANT, before->m_data, before->m_size);
	Record rec2(after->m_rowId, REC_REDUNDANT, after->m_data, after->m_size);

#ifdef NTSE_UNIT_TEST
	// ��Ԫ���Ե�ʱ�򣬿��ܴ����ƹ��������壬�޸�m_indexNum�������������������
	if (updates > m_indexNum)
		updates = m_indexNum;
#endif

	for (u8 i = 0; i < updates; i++) {	// ��һ��ÿ���������и���
		McSavepoint lobSavepoint(session->getLobContext());
		u64 token = (u64)-1;
		u16 realUpdateIdxNo = updateIndicesNo[i];
		DrsBPTreeIndex *index = (DrsBPTreeIndex*)m_indice[realUpdateIdxNo];
		const IndexDef *indexDef = index->getIndexDef();

		bool keyNeedCompress = RecordOper::isFastCCComparable(m_tableDef, indexDef, indexDef->m_numCols, indexDef->m_columns);
		bool success;
		// ƴװ��������������ȡ�ķ���ȡ�����Ƿ�������»���TNT purge update
		Array<LobPair*> lobArray1, lobArray2;
		if (indexDef->hasLob()) {
			// ǰ����Redundant��ʽ��¼
			RecordOper::extractLobFromR(session, m_tableDef, indexDef, m_lobStorage, &rec1, &lobArray1);

			if (!updateLob) {
				// TNT purge ������������´���󣬺���Ĵ�����Ѿ������ڴ�����������
				RecordOper::extractLobFromR(session, m_tableDef, indexDef, m_lobStorage, &rec2, &lobArray2);
			} else {
				// NTSE ���£�����ΪREDUNANT��MYSQL��ϸ�ʽ
				RecordOper::extractLobFromMixedMR(session, m_tableDef, indexDef, m_lobStorage, &rec2, after->m_numCols, after->m_columns, &lobArray2);
			}
		}
		SubRecord *key1 = IndexKey::allocSubRecord(memoryContext, keyNeedCompress, const_cast<Record*>(&rec1), &lobArray1, m_tableDef, indexDef);
		SubRecord *key2 = IndexKey::allocSubRecord(memoryContext, keyNeedCompress, const_cast<Record*>(&rec2), &lobArray2, m_tableDef, indexDef);

		// update������ɾ������룬���ܻ����������ʱ��Ҫ���˶Ե�ǰ�����Ĳ����������ټ������Ը��±�����
		while (true) {
			token = session->getToken();
			while (!index->del(session, key1)) {
				nftrace(ts.irl, tout << session->getId() << " D in U dl and redo");
				session->unlockIdxObjects(token);
				index->statisticDL();
			}

			if (!indexDef->m_unique) {
				// ����Ƿ�Ψһ������������ǰ�ŵ�ɾ��������������Ϊ��������������
				// ͬʱ����Ҫ��֤��д��־��ǵ�ǰ��ɾ���ɹ�
				m_logger->logDMLDeleteInUpdateDone(session);
				NTSE_ASSERT(session->unlockIdxAllObjects());

				// ����Ĳ��뼴ʹ�������ˣ�Ҳ��Ӧ�û���ɾ������
				while ((success = index->insert(session, key2, &duplicateKey)) == false) {
					nftrace(ts.irl, tout << session->getId() << " I in U dl and redo");
					session->unlockIdxAllObjects();	// ���ڳ����������������Ҫ�����ٲ���
					index->statisticDL();
				}
			} else {
				success = index->insert(session, key2, &duplicateKey);
				if (!success && !duplicateKey) {
					nftrace(ts.irl, tout << session->getId() << "I in U dl and redo");
					// ��������ʧ�ܣ���Ҫ����ɾ���������������¸���
					NTSE_ASSERT(index->insert(session, key1, &duplicateKey, false));
					session->unlockIdxObjects(token);
					index->statisticDL();
					continue;
				}
			}

			break;
		}

		// ��������Ψһ�Գ�ͻ���µĸ���ʧ�ܣ�ȫ��������Ҫ����
		// TNT purge updateһ��������Ψһ�Գ�ͻ
		if (!success && duplicateKey) {
			nftrace(ts.irl, tout << session->getId() << " U dk and rb all");
			ftrace(ts.idx, tout << "Update the " << i << "th index failed and rollback, Record id: " << rid(before->m_rowId));

			*dupIndex = realUpdateIdxNo;

			NTSE_ASSERT(index->insert(session, key1, &duplicateKey, false));

			for (u8 j = 0; j < i; j++) {
				u16 realUndoIdxNo = updateIndicesNo[j];
				index = (DrsBPTreeIndex*)m_indice[realUndoIdxNo];
				const IndexDef *indexDef = index->getIndexDef();

				bool keyNeedCompress = RecordOper::isFastCCComparable(m_tableDef, indexDef, indexDef->m_numCols, indexDef->m_columns);

				Array<LobPair*> lobArray1, lobArray2;
				if (indexDef->hasLob()) {
					RecordOper::extractLobFromR(session, m_tableDef, indexDef, m_lobStorage, &rec1, &lobArray1);
					// ���ڻع�����ֻ��NTSE��������˴����һ������mysql��ʽ��¼��
					RecordOper::extractLobFromMixedMR(session, m_tableDef, indexDef, m_lobStorage, &rec2, 
						after->m_numCols, after->m_columns, &lobArray2);
				}

				SubRecord *key1 = IndexKey::allocSubRecord(memoryContext, keyNeedCompress, const_cast<Record*>(&rec2), &lobArray1, m_tableDef, indexDef);
				SubRecord *key2 = IndexKey::allocSubRecord(memoryContext, keyNeedCompress, const_cast<Record*>(&rec1), &lobArray2, m_tableDef, indexDef);

				/**
				 * ���ڵĸ��»��˿�����Ҫ��SMO�������ǲ�Ӧ�õ���SMO����ҳ����֮�����������Ϊ��
				 * ���ڸ��»����޸ĵ�ҳ��϶�����������ʱ���޸Ĺ���ҳ�棬�����Щҳ��������������ʱ��
				 * ����ҪSMO����ô���ڵĻ��˲��������ڱ�ҳ����ɣ����ᵼ��SMO������
				 * �������������SMO������ô�Ѿ�����SMO��������������
				 * ��֮������Ļ��˵Ĺ����У����������Ѿ�������Դ������κ���Դ
			     */
				NTSE_ASSERT(index->del(session, key1));
				NTSE_ASSERT(index->insert(session, key2, &duplicateKey, false));

				index->statisticOp(IDX_UPDATE, -1);
			}

			session->setTxnDurableLsn(session->getLastLsn());		// Ψһ���������½׶λ��ˣ���Ҫ���ÿɳ־û�LSN
			NTSE_ASSERT(session->unlockIdxAllObjects());
			memoryContext->resetToSavepoint(savePoint);
			result = false;

			break;
		}

		index->statisticOp(IDX_UPDATE, 1);

		m_logger->logDMLDoneUpdateIdxNo(session, i);

		// �������Ψһ����������ϣ����ÿɳ־û�LSN
		if (i + 1 == updateUniques)
			session->setTxnDurableLsn(session->getLastLsn());
		// ���һ��Ψһ�����Լ����з�Ψһ�����ĸ��½���֮�������������
		if (i + 1 >= updateUniques)
			session->unlockIdxAllObjects();
	}

	memoryContext->resetToSavepoint(savePoint);

	m_logger->logDMLUpdateEnd(session, opLSN, result);

	vecode(vs.idx, NTSE_ASSERT(!session->hasLocks()));

	return result;
}


/**
 * �ϲ�ָ�ʱ����õĲ���дDML���������ӿ�
 * @param session	�Ự���
 * @param beginLSN	��DML��Ӧ�Ŀ�ʼ��־��LSN
 * @param succ		��ʾ��DML�����ǳɹ�������Ҫ���ع�������Ĭ����false����ʾ�Ǳ��ع���
 */
void DrsBPTreeIndice::logDMLDoneInRecv(Session *session, u64 beginLSN, bool succ) {
	m_logger->logDMLUpdateEnd(session, beginLSN, succ);
}


/**
 * �ָ�����ʹ�õĲ����������ӿ�
 * @param session		�Ự���
 * @param record		Ҫ����ļ�¼����Ҫ��REC_REDUNDANT��ʽ�ļ�¼
 * @param lastDoneIdxNo	���ĸ�����֮��ʼִ�в��룬�����˳����ָ�ڴ浱�������������е��±꣬��־���м�¼
 * @param beginLSN		�������һ��ʼ��¼�Ŀ�ʼ������LSN
 */
void DrsBPTreeIndice::recvInsertIndexEntries(Session *session, const Record *record, s16 lastDoneIdxNo, u64 beginLSN) {
	ftrace(ts.idx, tout << record << lastDoneIdxNo << beginLSN);

	// ���û���κ�����������ɹ������������������Ƿ�Ψһ��������ôlastDoneIdxNo==-1����1��պô�0��ʼ
	// �����ɾ�������²����������ԭ��һ��
	assert(lastDoneIdxNo + 1 >= m_orderedIndices.getUniqueIdxNum());	// ��ǰ�������������϶����Ƿ�Ψһ����

	bool duplicateKey;
	MemoryContext *memoryContext = session->getMemoryContext();
	u64 savePoint = memoryContext->setSavepoint();

	for (u8 i = (u8)(lastDoneIdxNo + 1); i < m_indexNum; i++) {
		McSavepoint lobSavepoint(session->getLobContext());
		u16 realInsertIndexNo = m_orderedIndices.getOrder(i);
		DrsBPTreeIndex *index = (DrsBPTreeIndex*)m_indice[realInsertIndexNo];
		const IndexDef *indexDef = index->getIndexDef();
		bool keyNeedCompress = RecordOper::isFastCCComparable(m_tableDef, indexDef, indexDef->m_numCols, indexDef->m_columns);
		// ƴװ������
		Array<LobPair*> lobArray;
		if (indexDef->hasLob()) {
			RecordOper::extractLobFromR(session, m_tableDef, indexDef, m_lobStorage, record, &lobArray);
		}

		SubRecord *key = IndexKey::allocSubRecord(memoryContext, keyNeedCompress, record, &lobArray, m_tableDef, indexDef);

		// ����ִ�еĲ���϶�����ɹ�������Υ��Ψһ��Լ�������ҵ��̲߳�������
		NTSE_ASSERT(index->insert(session, key, &duplicateKey));

		// ���Լ�ʱ�ͷ�����Դ
		index->statisticOp(IDX_INSERT, 1);
		m_logger->logDMLDoneUpdateIdxNo(session, i);
		session->unlockIdxAllObjects();
	}

	memoryContext->resetToSavepoint(savePoint);
	m_logger->logDMLUpdateEnd(session, beginLSN, true);
}

/**
 * �ָ�����ʹ�õĲ���ɾ�������ӿ�
 * @param session		�Ự���
 * @param record		Ҫɾ���ļ�¼����Ҫ��REC_REDUNDANT��ʽ�ļ�¼
 * @param lastDoneIdxNo	���ĸ�����֮��ʼִ��ɾ���������˳����ָ�ڴ浱�������������е��±꣬��־���м�¼
 * @param beginLSN		�������һ��ʼ��¼�Ŀ�ʼ������LSN
 */
void DrsBPTreeIndice::recvDeleteIndexEntries(Session *session, const Record *record, s16 lastDoneIdxNo, u64 beginLSN) {
	ftrace(ts.idx, tout << record << lastDoneIdxNo << beginLSN);

	MemoryContext *memoryContext = session->getMemoryContext();
	u64 savePoint = memoryContext->setSavepoint();

	for (u8 i = (u8)(lastDoneIdxNo + 1); i < m_indexNum; i++) {	// ��һ��ÿ����������ɾ��
		McSavepoint lobSavepoint(session->getLobContext());
		DrsBPTreeIndex *index = (DrsBPTreeIndex*)m_indice[i];
		const IndexDef *indexDef = index->getIndexDef();

		bool keyNeedCompress = RecordOper::isFastCCComparable(m_tableDef, indexDef, indexDef->m_numCols, indexDef->m_columns);
		Array<LobPair*> lobArray;
		if (indexDef->hasLob()) {
			RecordOper::extractLobFromR(session, m_tableDef, indexDef, m_lobStorage, record, &lobArray);
		}

		SubRecord *key = IndexKey::allocSubRecord(memoryContext, keyNeedCompress, record, &lobArray, m_tableDef, indexDef);

		NTSE_ASSERT(index->del(session, key));
		index->statisticOp(IDX_DELETE, 1);
		m_logger->logDMLDoneUpdateIdxNo(session, i);

		// �ͷű��β������ܳ��е���Դ��
		NTSE_ASSERT(session->unlockIdxAllObjects());
	}

	memoryContext->resetToSavepoint(savePoint);

	m_logger->logDMLUpdateEnd(session, beginLSN, true);
}


/**
 * �ָ�����ʹ�õĲ�����²����ӿ�
 * @param session		�Ự���
 * @param before		�������и������Ե�ǰ���Ҫ��REC_REDUNDANT��ʽ�ļ�¼
 * @Param after			�������и������Եĺ����Ҫ��REC_REDUNDANT��ʽ�ļ�¼
 * @param lastDoneIdxNo	���ĸ�����֮��ʼִ�в��룬�����˳����ָ�ڴ浱�������������е��±꣬��־���м�¼
 * @param beginLSN		�������һ��ʼ��¼�Ŀ�ʼ������LSN
 * @param isUpdateLob   �ж���ntse�ĸ��»���TNT��purge update������������ȡ�����Ĵ����Դ�й�
 */
void DrsBPTreeIndice::recvUpdateIndexEntries(Session *session, const SubRecord *before, const SubRecord *after, s16 lastDoneIdxNo, u64 beginLSN, bool isUpdateLob) {
	ftrace(ts.idx, tout << before << after << lastDoneIdxNo << beginLSN);

	assert(before != NULL && after != NULL);
	assert(lastDoneIdxNo + 1 >= m_orderedIndices.getUniqueIdxNum());

	bool duplicateKey;
	MemoryContext *memoryContext = session->getMemoryContext();
	u64 savePoint = memoryContext->setSavepoint();

	u16 updates, updateUniques;
	u16 *updateIndicesNo;
	getUpdateIndices(memoryContext, after, &updates, &updateIndicesNo, &updateUniques);

	// ����before��after��record��ʽ
	RecordOper::mergeSubRecordRR(m_tableDef, (SubRecord*)after, (SubRecord*)before);
	Record rec1, rec2;
	rec1.m_rowId = before->m_rowId;
	rec2.m_rowId = after->m_rowId;
	rec1.m_format = rec2.m_format = REC_REDUNDANT;
	rec1.m_size = before->m_size;
	rec1.m_data = before->m_data;
	rec2.m_size = after->m_size;
	rec2.m_data = after->m_data;

	assert((u8)(lastDoneIdxNo + 1) <= updates);
	for (u8 i = (u8)(lastDoneIdxNo + 1); i < updates; i++) {	// ��һ��ÿ���������и���
		McSavepoint lobSavepoint(session->getLobContext());
		u16 realUpdateIdxNo = updateIndicesNo[i];
		DrsBPTreeIndex *index = (DrsBPTreeIndex*)m_indice[realUpdateIdxNo];
		const IndexDef *indexDef = index->getIndexDef();

		bool keyNeedCompress = RecordOper::isFastCCComparable(m_tableDef, indexDef, indexDef->m_numCols, indexDef->m_columns);
		Array<LobPair*> lobArray1, lobArray2;
		if (indexDef->hasLob()) {
			RecordOper::extractLobFromR(session, m_tableDef, indexDef, m_lobStorage, &rec1, &lobArray1);

			if (isUpdateLob) {
				RecordOper::extractLobFromMixedMR(session, m_tableDef, indexDef, m_lobStorage, &rec2, after->m_numCols, after->m_columns, &lobArray2);
			} else {
				RecordOper::extractLobFromR(session, m_tableDef, indexDef, m_lobStorage, &rec2, &lobArray2);
			}
		}

		SubRecord *key1 = IndexKey::allocSubRecord(memoryContext, keyNeedCompress, const_cast<Record*>(&rec1), &lobArray1, m_tableDef, indexDef);
		SubRecord *key2 = IndexKey::allocSubRecord(memoryContext, keyNeedCompress, const_cast<Record*>(&rec2), &lobArray2, m_tableDef, indexDef);

		// update������ɾ������룬��ʱ�����ȵ��̲߳�����������β������Ψһ��ֵ��ͻ
		NTSE_ASSERT(index->del(session, key1));
		// �ָ����̵ļ�¼��־��ʽӦ�ñ�֤����������һ��
		m_logger->logDMLDeleteInUpdateDone(session);
		NTSE_ASSERT(index->insert(session, key2, &duplicateKey));
		index->statisticOp(IDX_UPDATE, 1);
		m_logger->logDMLDoneUpdateIdxNo(session, i);
		// ��ʱ�ͷ�����Դ
		NTSE_ASSERT(session->unlockIdxAllObjects());
	}

	memoryContext->resetToSavepoint(savePoint);

	m_logger->logDMLUpdateEnd(session, beginLSN, true);
}


/**
 * ��ָ�������������и��¹����еĲ��룬ͬʱ��¼���������²����ɹ���־
 * �ù���ר��Ϊ���¹����еĲ���ʹ��
 * @param session		�Ự���
 * @param record		Ҫ����ļ�¼
 * @param lastDoneIdxNo	���������֮�������ִ�в��룬�����˳����ָ�ڴ浱�������������е��±꣬��־���м�¼
 * @param updateIdxNum	�ø��²������漰������������
 * @param updateIndices	�ø��²������µ�����������飬������������ͨ������ģ�����ó�����ʱ������������Ѿ����������ڲ������������
 * @param isUpdateLob   �ж���ntse�ĸ��»���TNT��purge update������������ȡ�����Ĵ����Դ�й�
 * @return ���ص�ǰִ�в����������Ҳ���Ǹ�����ȫ�����һ��������ţ��������ź����lastDoneIdxNo����ͬ��
 */
u8 DrsBPTreeIndice::recvCompleteHalfUpdate(Session *session, const SubRecord *record, s16 lastDoneIdxNo, u16 updateIdxNum, u16 *updateIndices, bool isUpdateLob) {
	ftrace(ts.idx, tout << record << lastDoneIdxNo);

	if (lastDoneIdxNo == -1) {
		// ������˵�������²���ֻ�漰��Ψһ����
		// ���ҵ�һ����Ҫ���µ�Ψһ�����ĸ��»�δ���
		// ��ʱ��Ҫͨ��updateIndices���������ĸ�������Ҫ��ִ�в������
		// ʵ��ȡ�����������鵱�е�һ����Ҫ���µķ�Ψһ����
		u16 i;
		for (i = 0; i < updateIdxNum; i++)
			if (!m_tableDef->m_indice[updateIndices[i]]->m_unique)
				break;
		assert(i < updateIdxNum);	// �ض����ҵ�һ����Ψһ����
		lastDoneIdxNo = m_orderedIndices.find(updateIndices[i]) - 1;
		assert(lastDoneIdxNo != -2);	// �������Ҳ���
	}

	u8 insertIdxNo = (u8)(lastDoneIdxNo + 1);
	assert(insertIdxNo >= m_orderedIndices.getUniqueIdxNum());	// ��ǰ�������������϶����Ƿ�Ψһ����

	bool duplicateKey;
	MemoryContext *memoryContext = session->getMemoryContext();
	u64 savePoint = memoryContext->setSavepoint();
	McSavepoint lobSavepoint(session->getLobContext());

	u16 realInsertIndexNo = m_orderedIndices.getOrder(insertIdxNo);
	DrsBPTreeIndex *index = (DrsBPTreeIndex*)m_indice[realInsertIndexNo];

	const IndexDef *indexDef = index->getIndexDef();
	Record fullRecord(record->m_rowId, REC_REDUNDANT, record->m_data, m_tableDef->m_maxRecSize);
	bool keyNeedCompress = RecordOper::isFastCCComparable(m_tableDef, indexDef, indexDef->m_numCols, indexDef->m_columns);
	Array<LobPair*> lobArray;
	if (indexDef->hasLob()) {
		if(isUpdateLob) {
			RecordOper::extractLobFromMixedMR(session, m_tableDef, indexDef, m_lobStorage, &fullRecord, 
				record->m_numCols, record->m_columns, &lobArray);
		} else {
			RecordOper::extractLobFromR(session, m_tableDef, indexDef, m_lobStorage, &fullRecord, &lobArray);
		}
	}

	SubRecord *key = IndexKey::allocSubRecord(memoryContext, keyNeedCompress, &fullRecord, &lobArray, m_tableDef, indexDef);

	// ����ִ�еĲ���϶�����ɹ�������Υ��Ψһ��Լ�������ҵ��̲߳�������
	NTSE_ASSERT(index->insert(session, key, &duplicateKey));

	// ���Լ�ʱ�ͷ�����Դ
	index->statisticOp(IDX_UPDATE, 1);
	m_logger->logDMLDoneUpdateIdxNo(session, insertIdxNo);
	session->unlockIdxAllObjects();

	memoryContext->resetToSavepoint(savePoint);

	return insertIdxNo;
}



/**
 * ����һ��������һ�׶Σ��������ļ����д���д��������Ϣ
 * @post ����ɹ���֮��������createIndexPhaseTwo�����ڴ�����ӳ��
 * @param session	�Ự���
 * @param def		��������
 * @param heap		DRS��
 * @throw NtseException IO�쳣��Ψһ���쳣����������������
 */
void DrsBPTreeIndice::createIndexPhaseOne(Session *session, const IndexDef *indexDef, const TableDef *tblDef, DrsHeap *heap) throw(NtseException) {
	ftrace(ts.idx, tout << indexDef);

	m_newIndexId = createNewIndex(session, indexDef, tblDef, heap);
	if (m_newIndexId == 0)
		NTSE_THROW(NTSE_EC_EXCEED_LIMIT, "Create indices on table %s too many times!", tblDef->m_name);
}



/**
 * �����������ڶ��׶�
 * @pre	�����ȵ���createIndexPhaseOne���ܵ��ñ����������m_newIndexId���벻������-1
 * @param def	��������
 * @return	���ش�������������
 */
DrsIndex* DrsBPTreeIndice::createIndexPhaseTwo(const IndexDef *def) {
	ftrace(ts.idx, tout << def);

	assert(m_newIndexId > 0 && m_newIndexId <= 0xFF);

	DrsBPTreeIndex *index = new DrsBPTreeIndex(this, m_tableDef, const_cast<IndexDef*>(def), (u8)m_newIndexId, m_pagesManager->getFileHeaderPage()->m_rootPageIds[m_indexNum]);
	m_indice[m_indexNum++] = index;
	m_newIndexId = -1;

	m_orderedIndices.add(m_indexNum - 1, index->getIndexId(), def->m_unique);

	nftrace(ts.idx, tout << "New index " << m_newIndexId << " created, unique: " << def->m_unique;);

	return index;
}


/**
 * �õ�ָ��������
 *
 * @param index ������ţ���0��ʼ
 * @return ����
 */
DrsIndex* DrsBPTreeIndice::getIndex(uint index) {
	assert(index < Limits::MAX_INDEX_NUM && index < m_tableDef->m_numIndice);
	assert(m_indice[index] != NULL);
	return m_indice[index];
}


/**
 * ���ص�ǰ�������ļ�������������
 *
 * @return	��������
 */
uint DrsBPTreeIndice::getIndexNum() const {
	return m_indexNum;
}

/**
 * ����Ψһ����������Ŀ
 * @return 
 */
uint DrsBPTreeIndice::getUniqueIndexNum() const {
	return m_orderedIndices.getUniqueIdxNum();
}

/**
 * @param files [in/out] ��ģ�������Fileָ�����飬 �ռ�����߷���
 * @param pageTypes [in/out] File��Ӧ��ҳ����
 * @param numFile files�����pageTypes���鳤��
 * @return ��ģ��File�������
 */
int DrsBPTreeIndice::getFiles(File **files, PageType *pageTypes, int numFile) {
	UNREFERENCED_PARAMETER(numFile);
	assert(numFile >= 1);
	files[0] = m_file;
	pageTypes[0] = PAGE_INDEX;

	return 1;
}


/**
 * ɾ��һ��ָ����������һ�׶Σ�ɾ���ڴ浱�е�ӳ��
 * @pre	ʹ����Ҫע��ͬ���ڴ�
 * @post �������dropPhaseTwo����������ļ������������ݵ�ɾ�� 
 * @param session	�Ự���
 * @param idxNo		Ҫɾ�����������
 */
void DrsBPTreeIndice::dropPhaseOne(Session *session, uint idxNo) {
	ftrace(ts.idx, tout << idxNo);

	UNREFERENCED_PARAMETER(session);
	assert(idxNo < Limits::MAX_INDEX_NUM && idxNo < m_tableDef->m_numIndice);
	assert(m_indice[idxNo] != NULL);
	m_droppedIndex = m_indice[idxNo];

	// �����޸��ڴ�����ά���������ṹ
	m_orderedIndices.remove((u16)idxNo, ((DrsBPTreeIndex*)m_droppedIndex)->getIndexDef()->m_unique);
	// ��������ɾ��
	memmove(&m_indice[idxNo], &m_indice[idxNo + 1], (m_indexNum - idxNo - 1) * sizeof(DrsIndex*));
	m_indexNum--;
	m_indice[m_indexNum] = NULL;
}


/**
 * ɾ��һ��ָ���������ڶ��׶Σ�ɾ�������ļ�������������Ϣ
 * @pre	�����ȵ��ù�dropPhaseOne
 * @param session	�Ự���
 * @param idxNo		ɾ��������ԭ���ڱ����е����
 */
void DrsBPTreeIndice::dropPhaseTwo(Session *session, uint idxNo) {
	ftrace(ts.idx, tout << idxNo);

	assert(m_droppedIndex != NULL);
	u8 indexId = ((DrsBPTreeIndex*)m_droppedIndex)->getIndexId();

	// ���ļ��б��ɾ��
	m_pagesManager->discardIndexByIDXId(session, m_logger, indexId, idxNo);

	// ����buffer������ҳ��
	Buffer *buffer = m_db->getPageBuffer();
	buffer->freePages(session, m_file, indexId, DrsBPTreeIndice::bufferCallBackSpecifiedIDX);

	delete m_droppedIndex;
	m_droppedIndex = NULL;
}


/**
 * ���ҽ���������������֮ǰ���øú������ͷű�������е�������Դ
 *
 * @param session �Ự
 * @param flushDirty �Ƿ�д��������
 */
void DrsBPTreeIndice::close(Session *session, bool flushDirty) {
	ftrace(ts.idx, tout << session->getId() << flushDirty);

	assert(m_file != NULL);

	Buffer *buffer = m_db->getPageBuffer();
	buffer->unpinPage(m_pagesManager->getFileHeaderPage());
	buffer->freePages(session, m_file, flushDirty);

	u64 errNo = m_file->close();
	if (File::getNtseError(errNo) != File::E_NO_ERROR) {
		m_db->getSyslog()->fopPanic(errNo, "Cannot close index file %s.", m_file->getPath());
	}

	for (uint i = 0; i < m_indexNum; i++) {
		assert(m_indice[i] != NULL);
		delete m_indice[i];
		m_indice[i] = NULL;
	}

	delete m_mutex;
	delete m_pagesManager;
	delete m_logger;
	delete [] m_indice;
	delete m_file;
	delete m_dboStats;
}
/**
 * ˢ��������
 * @pre ���е�д�����Ѿ�������
 *
 * @param session �Ự
 */
void DrsBPTreeIndice::flush(Session *session) {
	ftrace(ts.idx, tout << session->getId());
	m_db->getPageBuffer()->flushDirtyPages(session, m_file);
}

/**
 * �õ��൱���ļ�������
 * @return �����ļ�������
 */
File* DrsBPTreeIndice::getFileDesc() {
	return m_file;
}


/**
 * �õ�������Ӧ���ΨһID
 *
 * ���ر�ID
 */
u16 DrsBPTreeIndice::getTableId() {
	return m_tableDef->m_id;
}


/**
 * �õ�����ҳ�������������
 *
 * @return ��������ҳ�������������
 */
IndicePageManager* DrsBPTreeIndice::getPagesManager() {
	return m_pagesManager;
}



/**
 * �õ���־������
 *
 * @return ������־������
 */
IndexLog* DrsBPTreeIndice::getLogger() {
	return m_logger;
}

const OrderedIndices* DrsBPTreeIndice::getOrderedIndices() const {
	return &m_orderedIndices;
}

/**
 * ����һ���µ�����
 *
 * @param session �Ự���
 * @param def	��������
 * @param heap	��ضѶ���
 * @return indexId ID��ʾ�ɹ���0��ʾʧ��
 * @throw �ļ���д�쳣������Ψһ�Գ�ͻ�쳣�������޷������쳣
 */
u8 DrsBPTreeIndice::createNewIndex(Session *session, const IndexDef *indexDef, const TableDef *tblDef, DrsHeap *heap) throw(NtseException) {
	ftrace(ts.idx, tout << indexDef);

	u8 indexId, indexNo;
	PageId rootPageId;

	// ��¼����������ʼ��־
	u64 opLSN = m_logger->logCreateIndexBegin(session, m_pagesManager->getFileHeaderPage()->getAvailableIndexId());

	PageHandle *rootHandle = m_pagesManager->createNewIndexRoot(m_logger, session, &indexNo, &indexId, &rootPageId);
	if (rootHandle == NULL) {
		m_logger->logCreateIndexEnd(session, indexDef, opLSN, indexId, false);
		return 0;
	}

	SYNCHERE(SP_IDX_ALLOCED_ROOT_PAGE);

	// ��ʼ����ҳ��
	IndexPage *rootPage = (IndexPage*)rootHandle->getPage();
	u32 mark = IndexPage::formPageMark(indexId);
	rootPage->initPage(session, m_logger, true, false, rootPageId, mark, ROOT_AND_LEAF, 0, INVALID_PAGE_ID, INVALID_PAGE_ID);
	vecode(vs.idx, rootPage->traversalAndVerify(NULL, NULL, NULL, NULL, NULL));
	session->markDirty(rootHandle);
	nftrace(ts.idx, tout << "Create index, indexNo: " << indexNo << " indexId: " << indexId << " root page: " << rootPageId);

	if (heap == NULL) {	// ���ôӶѴ�����ֱ�ӷ���
		session->releasePage(&rootHandle);
		goto Success;
	} else {
		try {
			//appendIndexFromHeapBySort�е�sort��ռ�úܳ�ʱ�䣬���ܳ��ڳ���latchȥsort������ʱpage��pinס��
			session->unlockPage(&rootHandle);
			appendIndexFromHeapBySort(session, indexDef, tblDef, heap, indexId, rootPageId, rootHandle);
			nftrace(ts.idx, tout << "Created index by reading data from heap");
		} catch (NtseException &e) {
			m_logger->logCreateIndexEnd(session, indexDef, opLSN, indexId, false);
			throw e;
		}
	}

Success:
	m_logger->logCreateIndexEnd(session, indexDef, opLSN, indexId, true);
	return indexId;
}



/**
 * ʹ���ⲿ��������ָ���ѵ����ݶ�ȡ������������
 *
 * @pre	����֮ǰ�����Ѿ������˸�ҳ���������ļ�ͷ�������˸������������Ϣ
 * @pre ����֮ǰ��ҳ���Ѿ���pinס�����ǲ�����
 * @post �������κ���Դ
 *
 * @param session		�Ự���
 * @param def			Ҫ��������������
 * @param heap			��صĶѶ���
 * @param indexId		����ID
 * @param rootPageId	�����ĸ�ҳ��ID
 * @param rootHandle	��ҳ����
 * @throw �ļ���д��������ҳ���д��������Ψһ��Լ��Υ���쳣
 */
void DrsBPTreeIndice::appendIndexFromHeapBySort(Session *session, const IndexDef *indexDef, const TableDef *tblDef, DrsHeap *heap, u8 indexId, PageId rootPageId, PageHandle *rootHandle) throw(NtseException) {
	MemoryContext *memoryContext = session->getMemoryContext();
	u64 savePoint = memoryContext->setSavepoint();
	BPTreeCreateTrace *createTrace = allocCreateTrace(memoryContext, indexDef, indexId);
	createTrace->m_pageId[0] = rootPageId;
	createTrace->m_pageHandle[0] = rootHandle;
	SubRecord **lastRecords = createTrace->m_lastRecord;
	SubRecord *padKey = IndexKey::allocSubRecord(memoryContext, indexDef, KEY_PAD);
	bool isFastComparable = RecordOper::isFastCCComparable(m_tableDef, indexDef, indexDef->m_numCols, indexDef->m_columns);

	bool isUniqueIndex = indexDef->m_unique;

	try {
		// ��ʼ��������������׼��ȡ����
		Sorter sorter(m_db->getControlFile(), m_lobStorage, indexDef, tblDef);
		sorter.sort(session, heap);

		SubRecord *leafLastRecord = lastRecords[0];
		lastRecords[0]->m_size = 0;

		latchPage(session, createTrace->m_pageHandle[0], Exclusived);
		while (true) {
			SubRecord *record = sorter.getSortedNextKey();
			if (record == NULL)	// �������
				break;

			if (isUniqueIndex && isKeyEqual(indexDef, record, lastRecords[0], padKey, isFastComparable)) {	// ������Υ��Ψһ��Լ�������������׳��쳣
				completeIndexCreation(session, createTrace);
				NTSE_THROW(NTSE_EC_INDEX_UNQIUE_VIOLATION, "Index %s unique violated!", indexDef->m_name);
			}

			IndexPage *page = (IndexPage*)createTrace->m_pageHandle[0]->getPage();
			if (!(page->isPageStillFree() && page->appendIndexKey(session, m_logger, createTrace->m_pageId[0], record, leafLastRecord) == INSERT_SUCCESS)) {	// ����ҳ���Ѿ���
				PageId newPageId;
				PageHandle *newPageHandle = createNewPageAndAppend(session, createTrace, 0, record, &newPageId, LEAF_PAGE);
				// ������ҳ�����������
				appendIndexNonLeafPage(session, createTrace);
				// ����·����Ϣ
				updateLevelInfo(session, createTrace, 0, newPageId, newPageHandle);
			}

			IndexKey::copyKey(leafLastRecord, record, false);
		}

		completeIndexCreation(session, createTrace);
		memoryContext->resetToSavepoint(savePoint);
	} catch (NtseException &e) {
		//unlatchAllTracePages(session, createTrace);
		//�Ե�ǰ�����Ҷҳ�����mark dirty��unlatch
		//markDirtyAndUnlatch(session, createTrace->m_pageHandle[0]);
		//unPinAllTracePages(session, createTrace);
		m_pagesManager->discardIndexByIDXId(session, m_logger, createTrace->m_indexId, -1);
		memoryContext->resetToSavepoint(savePoint);
		throw e;
	}
}



/**
 * �Ƚ�����key�Ƿ����
 *
 * @param indexDef		��������
 * @param key1			��ֵ1
 * @param key2			��ֵ2
 * @param padkey		pad��ʽ����ʱ��ֵ
 * @param isFastable	��ʾ��ǰ��ֵ�Ƚ��ܷ���ٱȽ�
 * @return ���TRUE������FALSE
 */
bool DrsBPTreeIndice::isKeyEqual(const IndexDef *indexDef, SubRecord *key1, SubRecord *key2, SubRecord *padKey, bool isFastable) {
	if (isFastable)
		return IndexKey::isKeyValueEqual(key1, key2);
	else {
		if (!IndexKey::isKeyValid(key1) || !IndexKey::isKeyValid(key2))
			return false;
		// ��Ҫ��ת����pad��ʽ�ٱȽ�
		RecordOper::convertKeyCP(m_tableDef, indexDef, key1, padKey);
		return (RecordOper::compareKeyPC(m_tableDef, padKey, key2, indexDef) == 0);
	}
}



/**
 * ����ָ����ֵ�����ݣ����ڴ�����������ҳ�����ʱ����page�ض�����Ҷҳ��
 *
 * @param lastRecord	Ҫ�����ļ�ֵ1
 * @param appendKey		Ҫ�����ļ�ֵ2
 * @param page			appendKey�Ѿ������ڸö�Ӧҳ��ĵ�һ��
 */
void DrsBPTreeIndice::swapKeys(SubRecord *lastRecord, SubRecord *appendKey, Page *page) {
	IndexKey::copyKey(appendKey, lastRecord, true);
	KeyInfo keyInfo;
	((IndexPage*)page)->getFirstKey(lastRecord, &keyInfo);
}



/**
 * ������������Ϣ����ָ�������Ϣ���³��µ���Ϣ�������Ҫ���ͷž���Ϣҳ�����
 *
 * @param session		�Ự���
 * @param createTrace	����������Ϣ
 * @param level			Ҫ���µĲ���
 * @param pageId		�µ�ҳ��ID
 * @param pageHandle	�µ�ҳ����
 */
void DrsBPTreeIndice::updateLevelInfo(Session *session, BPTreeCreateTrace *createTrace, u16 level, PageId pageId, PageHandle *pageHandle) {
	if (createTrace->m_pageHandle[level] != NULL) {
		session->markDirty(createTrace->m_pageHandle[level]);
		session->releasePage(&(createTrace->m_pageHandle[level]));
	}
	createTrace->m_pageId[level] = pageId;
	createTrace->m_pageHandle[level] = pageHandle;
}



/**
 * ����һ����ҳ�棬����ָ��������createTrace����
 *
 * @post	��ҳ���Ѿ������˻��������ұ�־Ϊ��
 * @param session		�Ự���
 * @param createTrace	����������Ϣ
 * @param level			����ҳ��Ĳ��������ܴ�������ҳ��Ĳ���
 * @param appendKey		Ҫ��ӵļ�ֵ
 * @param pageId		IN/OUT	���ش���ҳ���ID
 * @param pageType		Ҫ����ҳ�������
 * @return ���ش���ҳ����
 */
PageHandle* DrsBPTreeIndice::createNewPageAndAppend(Session *session, BPTreeCreateTrace *createTrace, u8 level, const SubRecord *appendKey, PageId *pageId, IndexPageType pageType) {
	PageHandle *pageHandle = m_pagesManager->allocPage(m_logger, session, createTrace->m_indexId, pageId);
	session->markDirty(pageHandle);
	IndexPage *page = (IndexPage*)pageHandle->getPage();
	IndexPage *prevPage;

	if (level < createTrace->m_indexLevel) {
		prevPage = (IndexPage*)createTrace->m_pageHandle[level]->getPage();
		prevPage->m_nextPage = *pageId;
	} else	// �´�����ҳ�棬û��ǰ��
		prevPage = NULL;

	page->initPage(session, m_logger, true, false, *pageId, createTrace->m_pageMark, pageType, level, prevPage == NULL ? INVALID_PAGE_ID : createTrace->m_pageId[level], INVALID_PAGE_ID);
	page->appendIndexKey(session, m_logger, *pageId, appendKey, NULL);	// �϶����Բ���ɹ�

	return pageHandle;
}



/**
 * ��������ʱ����Ҷҳ��Ŀ���ҳ��������������У��ù��̿��ܻ�һֱ���е������
 *
 * @pre		�����ǵ�ǰҶҳ���Ѿ�����
 * @post	�漰���ķ�Ҷ����lastRecord���ᱻ����
 *
 * @param session		�Ự���
 * @param createTrace	����������Ϣ
 * @return
 */
void DrsBPTreeIndice::appendIndexNonLeafPage(Session *session, BPTreeCreateTrace *createTrace) {
	u8 level = 1;
	SubRecord **lastRecords = createTrace->m_lastRecord;
	SubRecord *idxKey = createTrace->m_idxKey2;

	// ����Ҫ����ļ�ֵ������PageId��Ϣ
	IndexKey::copyKey(idxKey, lastRecords[0], false);
	PageId sonPageId = createTrace->m_pageId[0];
	IndexKey::appendPageId(idxKey, sonPageId);

	if (createTrace->m_indexLevel == 1) {	// ��ǰֻ��һ�㣬���ո������ѷ�ʽ����
		createNewRoot(session, createTrace, idxKey);
		//��rootҳ����markdirty��unlatch������ʱroot���ǳ���pin��
		markDirtyAndUnlatch(session, createTrace->m_pageHandle[createTrace->m_indexLevel - 1]);
		return;
	}

	while (true) {
		assert(level < createTrace->m_indexLevel);
		PageId pageId = createTrace->m_pageId[level];
		latchPage(session, createTrace->m_pageHandle[level], Exclusived);
		IndexPage *page = (IndexPage*)createTrace->m_pageHandle[level]->getPage();

		if (page->isPageStillFree() && page->appendIndexKey(session, m_logger, createTrace->m_pageId[level], idxKey, lastRecords[level]) == INSERT_SUCCESS) {	// ����ֱ�Ӳ���ɹ�
			IndexKey::copyKey(lastRecords[level], idxKey, true);
			markDirtyAndUnlatch(session, createTrace->m_pageHandle[level]);
			return;
		} else {	// ��ǰ����Ҫ������ҳ��
			if (page->isPageRoot()) {
				PageId newPageId;
				PageHandle *newHandle = createNewPageAndAppend(session, createTrace, level, idxKey, &newPageId, NON_LEAF_PAGE);

				// ������ǰ����ֵ���뵽�µĸ�ҳ�棬���±���lastRecord
				swapKeys(lastRecords[level], idxKey, newHandle->getPage());
				IndexKey::appendPageId(idxKey, pageId);
				createNewRoot(session, createTrace, idxKey);
				//��rootҳ����markdirty��unlatch������ʱroot���ǳ���pin��
				markDirtyAndUnlatch(session, createTrace->m_pageHandle[createTrace->m_indexLevel - 1]);

				// ���±�����Ϣ
				updateLevelInfo(session, createTrace, level, newPageId, newHandle);
				//��newHandle mark dirty��unlatch������Ȼ����pin��newHandle��һ���µķ�ҳ�ڵ�
				markDirtyAndUnlatch(session, createTrace->m_pageHandle[level]);

				return;
			} else {
				PageId newPageId;
				PageHandle *newHandle = createNewPageAndAppend(session, createTrace, level, idxKey, &newPageId, NON_LEAF_PAGE);

				{	// ���浱ǰ����Ҫ����ļ�ֵ�Լ����µ�ǰ���lastRecord
					swapKeys(lastRecords[level], idxKey, newHandle->getPage());
					IndexKey::appendPageId(idxKey, pageId);
				}

				updateLevelInfo(session, createTrace, level, newPageId, newHandle);
				//��newHandle mark dirty��unlatch������Ȼ����pin��newHandle��һ���µķ�ҳ�ڵ�
				markDirtyAndUnlatch(session, createTrace->m_pageHandle[level]);
				level++;
			}
		}
	}
}


/**
 * �����µĸ�ҳ�棬����ʼ������������Ϣ�и�ҳ���Ӧ�����Ϣ
 *
 * @param session		�Ự���
 * @param createTrace	����������Ϣ
 * @param appendKey		Ҫ��ӵ���ҳ��ļ�ֵ
 */
void DrsBPTreeIndice::createNewRoot(Session *session, BPTreeCreateTrace *createTrace, SubRecord *appendKey) {
	u8 level = createTrace->m_indexLevel;
	// ����ԭrootҳ������
	IndexPage *page = (IndexPage*)createTrace->m_pageHandle[level - 1]->getPage();
	u8 newType = (u8)(level == 1 ? LEAF_PAGE : NON_LEAF_PAGE);
	page->updateInfo(session, m_logger, createTrace->m_pageId[level - 1], OFFSET(IndexPage, m_pageType), 1, (byte*)&newType);
	// ������rootҳ��
	PageId rootPageId;
	PageHandle *rootHandle = createNewPageAndAppend(session, createTrace, level, appendKey, &rootPageId, ROOT_PAGE);
	// �޸������Ϣ
	updateLevelInfo(session, createTrace, level, rootPageId, rootHandle);
	IndexKey::copyKey(createTrace->m_lastRecord[level], appendKey, true);
	++createTrace->m_indexLevel;
}



/**
 * ��������Ĵ�����������ҳ�������������޸������ļ�ͷ��ҳ��ID��Ϣ
 *
 * @param session		�Ự���
 * @param createTrace	����������Ϣ
 */
void DrsBPTreeIndice::completeIndexCreation(Session *session, BPTreeCreateTrace *createTrace) {
	PageId rootPageId = createTrace->m_pageId[createTrace->m_indexLevel - 1];

	if (createTrace->m_indexLevel > 1) {	// Ҫ��createTrace���е�ҳ�涼���뵽��������
		SubRecord *key1 = createTrace->m_idxKey1, *key2 = createTrace->m_idxKey2;
		SubRecord **lastRecords = createTrace->m_lastRecord;
		key1->m_size = key2->m_size = 0;

		// ׼��Ҫ���������
		PageId sonPageId = createTrace->m_pageId[0];
		IndexKey::copyKey(key1, lastRecords[0], false);
		IndexKey::appendPageId(key1, sonPageId);

		u16 level = createTrace->m_indexLevel;
		for (u8 i = 1; i < level; i++) {
			// ����²��Append��������ҳ��Ĵ�����������ҪAppend����item
			s16 items = !IndexKey::isKeyValid(key2) ? 1 : 2;
			SubRecord *appendKey = key1;
			while (--items >= 0) {
				latchPage(session, createTrace->m_pageHandle[i], Exclusived);
				IndexPage *page = (IndexPage*)createTrace->m_pageHandle[i]->getPage();
				if (page->isPageStillFree() && page->appendIndexKey(session, m_logger, createTrace->m_pageId[i], appendKey, lastRecords[i]) == INSERT_SUCCESS) {
					// ����ɹ������浱ǰ�����ֵ
					IndexKey::copyKey(lastRecords[i], appendKey, true);
					appendKey->m_size = 0;
				} else {	// ��Ҫ�����µ�ҳ�棬���ֻ�����һ��
					PageId newPageId;
					PageHandle *newPageHandle = createNewPageAndAppend(session, createTrace, i, appendKey, &newPageId, NON_LEAF_PAGE);

					// lastRecord��ʾ����ֵ������ָ���ʾԭҳ��������һ�����
					// ִ������һ����appendKey��ǰһ��page�����key��lastRecord������page�ĵ�һ��key
					swapKeys(lastRecords[i], appendKey, newPageHandle->getPage());
					IndexKey::appendPageId(appendKey, createTrace->m_pageId[i]);

					// �����������ҳ�棬��Ҫ��ҳ�����־λȥ��
					if (i == level - 1) {
						u8 newType = (u8)NON_LEAF_PAGE;
						page->updateInfo(session, m_logger, createTrace->m_pageId[i], OFFSET(IndexPage, m_pageType), 1, (byte*)&newType);
					}

					updateLevelInfo(session, createTrace, i, newPageId, newPageHandle);
				}

				markDirtyAndUnlatch(session, createTrace->m_pageHandle[i]);
				appendKey = key2;	// �������Ҫ����key2
			}

			if (i == level - 1 && !IndexKey::isKeyValid(key1) && !IndexKey::isKeyValid(key2))	// append����ҳ�棬��û�з��ѣ���������
				break;

			// ���key1����key2��һ����size��Ϊ0��˵����������о�������
			// ��ʹ��ˣ����ֻ������һ����size��Ϊ0����֤���ֵ��key1
			if (IndexKey::isKeyValid(key2)) {
				IndexKey::swapKey(&key1, &key2);	// TODO��ȷ�ϸ��������ô�����Լ�����ı�Ҫ��. key1����������key2�Ĳ��뵼��ҳ�����
			}

			// ��¼������������ݹ��ϲ����ʹ��
			SubRecord *maxKey = !IndexKey::isKeyValid(key1) ? key1 : key2;
			IndexKey::copyKey(maxKey, lastRecords[i], true);
			IndexKey::appendPageId(maxKey, createTrace->m_pageId[i]);
		}

		if (IndexKey::isKeyValid(key1)) {	// �������ѹ�����key1��key2����������µĸ����
			assert(IndexKey::isKeyValid(key2));
			// ������rootҳ��
			PageId newRootPageId;
			PageHandle *newRootHandle = createNewPageAndAppend(session, createTrace, level, key1, &newRootPageId, ROOT_PAGE);
			// �޸������Ϣ
			updateLevelInfo(session, createTrace, level, newRootPageId, newRootHandle);
			IndexKey::copyKey(createTrace->m_lastRecord[level], key1, true);
			++createTrace->m_indexLevel;

			IndexPage *page = (IndexPage*)newRootHandle->getPage();
			page->appendIndexKey(session, m_logger, createTrace->m_pageId[createTrace->m_indexLevel - 1], key2, key1);
			rootPageId = createTrace->m_pageId[createTrace->m_indexLevel - 1];
			markDirtyAndUnlatch(session, newRootHandle);
		}
	}

	//unlatchAllTracePages(session, createTrace);
	//��Ҷ�ӽڵ����mark dirty and unlatch
	markDirtyAndUnlatch(session, createTrace->m_pageHandle[0]);
	unPinAllTracePages(session, createTrace);

	// ���������ļ�ͷҳ����Ϣ
	m_pagesManager->updateIndexRootPageId(m_logger, session, m_pagesManager->getIndexNo(createTrace->m_indexId), rootPageId);
}



/**
 * һ�����ͷŴ�����Ϣ�����漰��ҳ�������latch�������ε���
 *
 * @param session		�Ự���
 * @param createTrace	������Ϣ
 */
void DrsBPTreeIndice::unlatchAllTracePages(Session *session, BPTreeCreateTrace *createTrace) {
	PageHandle **pageHandles = createTrace->m_pageHandle;
	for (u16 i = 0; i < createTrace->m_indexLevel; i++) {
		if (pageHandles[i] == NULL)
			continue;

		session->markDirty(pageHandles[i]);
		session->releasePage(&pageHandles[i]);
	}
}

/** latch ҳ��
 * @param session �Ự���
 * @param pageHandle ��Ҫ������ҳ����
 * @param lockMode ����ģʽ
 */
void DrsBPTreeIndice::latchPage(Session *session, PageHandle *pageHandle, LockMode lockMode) {
	LOCK_PAGE_HANDLE(session, pageHandle, lockMode);
}

/** ��ָ��ҳ���ʶλ�࣬ͬʱunlatch��ҳ��
 * @param session �Ự���
 * @param pageHandle ָ��ҳ��
 */
void DrsBPTreeIndice::markDirtyAndUnlatch(Session *session, PageHandle *pageHandle) {
	session->markDirty(pageHandle);
	session->unlockPage(&pageHandle);
}

/** ��trace�ϵ�ҳ��ȫunpin
 * @param session �Ự���
 * @param createTrace ������Ϣ
 */
void DrsBPTreeIndice::unPinAllTracePages(Session *session, BPTreeCreateTrace *createTrace) {
	PageHandle **pageHandles = createTrace->m_pageHandle;
	for (u16 i = 0; i < createTrace->m_indexLevel; i++) {
		if (pageHandles[i] == NULL)
			continue;

		session->unpinPage(&pageHandles[i]);
	}
}


/**
 * ��������������Ϣ�ṹ
 *
 * @param memoryContext	�ڴ����������
 * @param def			��������
 * @param indexId		����ID
 * @return ����������Ϣ�ṹ
 */
BPTreeCreateTrace* DrsBPTreeIndice::allocCreateTrace(MemoryContext *memoryContext, const IndexDef *def, u8 indexId) {
	BPTreeCreateTrace *createTrace = (BPTreeCreateTrace*)memoryContext->alloc(sizeof(BPTreeCreateTrace));

	memset(createTrace->m_pageHandle, 0, sizeof(void*) * Limits::MAX_BTREE_LEVEL);

	for (u32 i = 0; i < Limits::MAX_BTREE_LEVEL; i++)
		createTrace->m_lastRecord[i] = IndexKey::allocSubRecord(memoryContext, def, KEY_COMPRESS);

	createTrace->m_idxKey1 = IndexKey::allocSubRecord(memoryContext, def, KEY_COMPRESS);
	createTrace->m_idxKey2 = IndexKey::allocSubRecord(memoryContext, def, KEY_COMPRESS);

	createTrace->m_pageMark = IndexPage::formPageMark(indexId);
	createTrace->m_indexLevel = 1;
	createTrace->m_indexId = indexId;

	return createTrace;
}


/**
 * �ڻָ��Ĺ����С�������redoDropIndex��undoCreateIndex��ͬ�������ڴ澵����Ϣ
 * @param session	�Ự���
 * @param indexId	ɾ���������ڲ�ID��
 */
void DrsBPTreeIndice::recvMemSync(Session *session, u16 indexId) {
	ftrace(ts.idx, tout << indexId);

	// �����Ҫ��ɾ���ڴ��еľ�������m_indice��m_indexSeq��������ʼ��Ӧ��һ�£�Ҫô����ҪҪô������Ҫ
	for (u16 idxSeq = 0; idxSeq < m_indexNum; idxSeq++) {
		if (((DrsBPTreeIndex*)m_indice[idxSeq])->getIndexId() == indexId) {
			DrsBPTreeIndex *index = (DrsBPTreeIndex *)m_indice[idxSeq];
			// ��������ɾ��
			memmove((DrsIndex*)m_indice + idxSeq, (DrsIndex*)m_indice + idxSeq + 1, (m_indexNum - idxSeq - 1) * sizeof(DrsIndex*));
			m_indice[m_indexNum] = NULL;
			m_indexNum--;

			// ����buffer������ҳ��
			Buffer *buffer = m_db->getPageBuffer();
			buffer->freePages(session, m_file, indexId, DrsBPTreeIndice::bufferCallBackSpecifiedIDX);

			m_orderedIndices.remove(idxSeq, index->getIndexDef()->m_unique);

			delete index;

			break;
		}
	}
}


/**
 * ��������ĳ�������Ĳ���
 *
 * @param session	�Ự���
 * @param lsn		��־LSN
 * @param log		��־����
 * @param size		��־����
 * @return ɾ�����������
 */
s32 DrsBPTreeIndice::redoDropIndex(Session *session, u64 lsn, const byte *log, uint size) {
	u8 indexId;
	s32 idxNo;
	m_logger->decodeDropIndex(log, size, &indexId, &idxNo);
	ftrace(ts.idx, tout << indexId << idxNo);

	// ���ļ��б��ɾ��
	m_pagesManager->discardIndexByIDXIdRecv(session, indexId, true, lsn);
	recvMemSync(session, indexId);

	return idxNo;
}



/**
 * ������������������������ʱ��Ҫͬ���ڴ�������ļ����е����ݣ���֤������������Ϣ�����ڴ�
 * @param log		��־����
 * @param size		��־����
 */
void DrsBPTreeIndice::redoCreateIndexEnd(const byte *log, uint size) {
	u8 indexId;
	bool successful;
	IndexDef indexDef;

	m_logger->decodeCreateIndexEnd(log, size, &indexDef, &indexId, &successful);

	ftrace(ts.idx, tout << indexId << successful;);

	if (successful) {
		// ��Ҫ�ж���������Ϣ�Ƿ��Ѿ��������ڴ浱�У���������ڣ�ǿ�����´�ͷҳ���ȡ��Ϣ
		for (uint i = 0; i < m_indexNum; i++)
			if (((DrsBPTreeIndex*)m_indice[i])->getIndexId() == indexId)
				return;

		DrsBPTreeIndex *index = new DrsBPTreeIndex(this, m_tableDef, &indexDef, indexId, m_pagesManager->getFileHeaderPage()->m_rootPageIds[m_indexNum]);
		m_indice[m_indexNum++] = index;

		// ͬʱҪά���������������
		m_orderedIndices.add(m_indexNum - 1, index->getIndexId(), indexDef.m_unique);
	}
}


/**
 * redo���롢ɾ����Append��Ӳ���
 *
 * @param session		�Ự���
 * @param lsn			��־LSN
 * @param log			��־����
 * @param size			��־����
 */
void DrsBPTreeIndice::redoDML(Session *session, u64 lsn, const byte *log, uint size) {
	PageId pageId;
	u16 offset;
	u16 miniPageNo;
	byte *oldValue, *newValue;
	u16 oldSize, newSize;
	IDXLogType type;
	u64 origLSN;
	m_logger->decodeDMLUpdate(log, size, &type, &pageId, &offset, &miniPageNo, &oldValue, &oldSize, &newValue, &newSize, &origLSN);
	PageHandle *pageHandle = GET_PAGE(session, m_file, PAGE_INDEX, pageId, Exclusived, m_dboStats, NULL);
	IndexPage *page = (IndexPage*)pageHandle->getPage();

	ftrace(ts.idx, tout << type << lsn << pageId << origLSN << offset << oldSize << newSize << (page->m_lsn < lsn););

	if (page->m_lsn < lsn) {
		verify_ex(vs.idx, origLSN == page->m_lsn);
		page->redoDMLUpdate(type, offset, miniPageNo, oldSize, newValue, newSize);
		page->m_lsn = lsn;
		session->markDirty(pageHandle);
	}

	session->releasePage(&pageHandle);
}


/**
 * ��������SMO����
 * @param session	�Ự���
 * @param lsn		��־lsn
 * @param log		��־����
 * @param size		��־����
 */
void DrsBPTreeIndice::redoSMO(Session *session, u64 lsn, const byte *log, uint size) {
	IDXLogType type = m_logger->getType(log, size);

	PageId pageId;
	byte *moveData, *moveDir;
	u16 dataSize, dirSize;
	PageHandle *pageHandle, *nextPageHandle;
	u64 origLSN1, origLSN2;

	if (type == IDX_LOG_SMO_MERGE) {
		PageId mergePageId, prevPageId;
		m_logger->decodeSMOMerge(log, size, &pageId, &mergePageId, &prevPageId, &moveData, &dataSize, &moveDir, &dirSize, &origLSN1, &origLSN2);
		pageHandle = GET_PAGE(session, m_file, PAGE_INDEX, pageId, Exclusived, m_dboStats, NULL);
		IndexPage *page = (IndexPage*)pageHandle->getPage();
		nextPageHandle = GET_PAGE(session, m_file, PAGE_INDEX, mergePageId, Exclusived, m_dboStats, NULL);
		IndexPage *mergePage = (IndexPage*)nextPageHandle->getPage();

		ftrace(ts.idx, tout << type << lsn << pageId << origLSN1 << mergePageId << origLSN2 << dataSize << dirSize << (page->m_lsn < lsn) << (mergePage->m_lsn < lsn););

		if (page->m_lsn < lsn) {
			verify_ex(vs.idx, page->m_lsn == origLSN1);
			page->redoSMOMergeOrig(prevPageId, moveData, dataSize, moveDir, dirSize, lsn);;
			page->m_lsn = lsn;
			session->markDirty(pageHandle);
		}
		if (mergePage->m_lsn < lsn) {
			verify_ex(vs.idx, mergePage->m_lsn == origLSN2);
			mergePage->redoSMOMergeRel();
			mergePage->m_lsn = lsn;
			session->markDirty(nextPageHandle);
		}
	} else if (type == IDX_LOG_SMO_SPLIT) {
		PageId newPageId, nextPageId;
		byte *oldSplitKey, *newSplitKey;
		u16 oldSKLen, newSKLen;
		u8 mpLeftCount, mpMoveCount;
		m_logger->decodeSMOSplit(log, size, &pageId, &newPageId, &nextPageId, &moveData, &dataSize, &oldSplitKey, &oldSKLen, &newSplitKey, &newSKLen, &moveDir, &dirSize, &mpLeftCount, &mpMoveCount, &origLSN1, &origLSN2);
		pageHandle = GET_PAGE(session, m_file, PAGE_INDEX, pageId, Exclusived, m_dboStats, NULL);
		IndexPage *page = (IndexPage*)pageHandle->getPage();
		nextPageHandle = GET_PAGE(session, m_file, PAGE_INDEX, newPageId, Exclusived, m_dboStats, NULL);
		IndexPage *newPage = (IndexPage*)nextPageHandle->getPage();

		ftrace(ts.idx, tout << type << lsn << pageId << origLSN1 << newPageId << origLSN2 << dataSize << dirSize << (page->m_lsn < lsn) << (newPage->m_lsn < lsn););

		if (page->m_lsn < lsn) {
			verify_ex(vs.idx, page->m_lsn == origLSN1);
			page->redoSMOSplitOrig(newPageId, dataSize, dirSize, oldSKLen, mpLeftCount, mpMoveCount, lsn);
			page->m_lsn = lsn;
			session->markDirty(pageHandle);
		}
		if (newPage->m_lsn < lsn) {
			verify_ex(vs.idx, newPage->m_lsn == origLSN2);
			newPage->redoSMOSplitRel(moveData, dataSize, moveDir, dirSize, newSplitKey, newSKLen, mpLeftCount, mpMoveCount, lsn);
			newPage->m_lsn = lsn;
			session->markDirty(nextPageHandle);
		}
	}

	session->releasePage(&pageHandle);
	session->releasePage(&nextPageHandle);
}


/**
 * redoҳ����Ϣ�޸Ĳ���
 * @param session	�Ự���
 * @param lsn		��־lsn
 * @param log		��־����
 * @param size		��־����
 */
void DrsBPTreeIndice::redoPageSet(Session *session, u64 lsn, const byte *log, uint size) {
	PageId pageId;
	IDXLogType type = m_logger->getType(log, size);
	u64 origLSN;

	if (type == IDX_LOG_UPDATE_PAGE) {
		u16 offset, valueLen;
		byte *newValue, *oldValue;
		bool clearPageFirst;
		m_logger->decodePageUpdate(log, size, &pageId, &offset, &newValue, &oldValue, &valueLen, &origLSN, &clearPageFirst);
		if (m_pagesManager->isPageByteMapPage(pageId)) {	// λͼҳ�޸ģ���Ҫ�ж��Ƿ���Ҫ��չ�����ļ�
			m_pagesManager->extendFileIfNecessary(m_pagesManager->getPageBlockStartId(pageId, offset));
		}

		PageHandle *pageHandle = GET_PAGE(session, m_file, PAGE_INDEX, pageId, Exclusived, m_dboStats, NULL);
		IndexPage *page = (IndexPage*)pageHandle->getPage();

		ftrace(ts.idx, tout << type << lsn << pageId << origLSN << offset << valueLen << (page->m_lsn < lsn););

		if (page->m_lsn < lsn) {
			verify_ex(vs.idx, page->m_lsn == origLSN);
			page->redoPageUpdate(offset, newValue, valueLen, clearPageFirst);
			page->m_lsn = lsn;
			session->markDirty(pageHandle);
		}
		session->releasePage(&pageHandle);
	} else if (type == IDX_LOG_ADD_MP) {
		byte *keyValue;
		u16 dataSize, miniPageNo;
		m_logger->decodePageAddMP(log, size, &pageId, &keyValue, &dataSize, &miniPageNo, &origLSN);
		PageHandle *pageHandle = GET_PAGE(session, m_file, PAGE_INDEX, pageId, Exclusived, m_dboStats, NULL);
		IndexPage *page = (IndexPage*)pageHandle->getPage();

		ftrace(ts.idx, tout << type << lsn << pageId << origLSN << dataSize << miniPageNo << (page->m_lsn < lsn););

		if (page->m_lsn < lsn) {
			verify_ex(vs.idx, page->m_lsn == origLSN);
			page->redoPageAddMP(keyValue, dataSize, miniPageNo);
			page->m_lsn = lsn;
			session->markDirty(pageHandle);
		}
		session->releasePage(&pageHandle);
	} else if (type == IDX_LOG_DELETE_MP) {
		byte *keyValue;
		u16 dataSize, miniPageNo;
		m_logger->decodePageDeleteMP(log, size, &pageId, &keyValue, &dataSize, &miniPageNo, &origLSN);
		PageHandle *pageHandle = GET_PAGE(session, m_file, PAGE_INDEX, pageId, Exclusived, m_dboStats, NULL);
		IndexPage *page = (IndexPage*)pageHandle->getPage();

		ftrace(ts.idx, tout << type << lsn << pageId << origLSN << dataSize << miniPageNo;);

		if (page->m_lsn < lsn) {
			verify_ex(vs.idx, page->m_lsn == origLSN);
			page->redoPageDeleteMP(miniPageNo);
			page->m_lsn = lsn;
			session->markDirty(pageHandle);
		}
		session->releasePage(&pageHandle);
	} else if (type == IDX_LOG_MERGE_MP) {
		u16 offset, oldSize, newSize, originalMPKeyCounts, miniPageNo;
		byte *oldValue, *newValue;
		m_logger->decodePageMergeMP(log, size, &pageId, &offset, &newValue, &newSize, &oldValue, &oldSize, &originalMPKeyCounts, &miniPageNo, &origLSN);
		PageHandle *pageHandle = GET_PAGE(session, m_file, PAGE_INDEX, pageId, Exclusived, m_dboStats, NULL);
		IndexPage *page = (IndexPage*)pageHandle->getPage();

		ftrace(ts.idx, tout << type << lsn << pageId << origLSN << (page->m_lsn < lsn) << originalMPKeyCounts;);

		if (page->m_lsn < lsn) {
			verify_ex(vs.idx, page->m_lsn == origLSN);
			page->redoPageMergeMP(offset, oldSize, newValue, newSize, miniPageNo);
			page->m_lsn = lsn;
			session->markDirty(pageHandle);
		}
		session->releasePage(&pageHandle);
	} else {
		assert(type == IDX_LOG_SPLIT_MP);
		u16 offset, oldSize, newSize, leftItems, miniPageNo;
		byte *oldValue, *newValue;
		m_logger->decodePageSplitMP(log, size, &pageId, &offset, &oldValue, &oldSize, &newValue, &newSize, &leftItems, &miniPageNo, &origLSN);
		PageHandle *pageHandle = GET_PAGE(session, m_file, PAGE_INDEX, pageId, Exclusived, m_dboStats, NULL);
		IndexPage *page = (IndexPage*)pageHandle->getPage();

		ftrace(ts.idx, tout << type << lsn << pageId << origLSN << (page->m_lsn < lsn) << leftItems;);

		if (page->m_lsn < lsn) {
			verify_ex(vs.idx, page->m_lsn == origLSN);
			page->redoPageSplitMP(offset, oldSize, newValue, newSize, leftItems, miniPageNo);
			page->m_lsn = lsn;
			session->markDirty(pageHandle);
		}
		session->releasePage(&pageHandle);
	}
}

/**
 * ����LOG_IDX_DML_END��־�ж������޸��Ƿ�ɹ�
 *
 * @param log	LOG_IDX_DML_END��־����
 * @param size	LOG_IDX_DML_END��־���ݴ�С
 * @return �����޸��Ƿ�ɹ�
 */
bool DrsBPTreeIndice::isIdxDMLSucceed(const byte *log, uint size) {
	bool succ;
	m_logger->decodeDMLUpdateEnd(log, size, &succ);
	ftrace(ts.idx, tout << succ);

	return succ;
}


/**
 * ����LOG_IDX_DMLDONE_IDXNO��־������ǰ��־�����ĸ������޸ĳɹ�
 * @param log	��־����
 * @param size	��־����
 * @return ��־�����ĸ��³ɹ����������
 */
u8 DrsBPTreeIndice::getLastUpdatedIdxNo(const byte *log, uint size) {
	u8 indexNo;
	m_logger->decodeDMLDoneUpdateIdxNo(log, size, &indexNo);
	ftrace(ts.idx, tout << indexNo);

	return indexNo;
}


/**
 * ����DML����
 * @param session		�Ự���
 * @param lsn			����LSN
 * @param log			��־����
 * @param size			��־����
 * @param logCPST		�Ƿ��¼������־
 */
void DrsBPTreeIndice::undoDML(Session *session, u64 lsn, const byte *log, uint size, bool logCPST) {
	PageId pageId;
	u16 offset;
	u16 miniPageNo;
	byte *oldValue, *newValue;
	u16 oldSize, newSize;
	IDXLogType type;
	u64 origLSN;
	m_logger->decodeDMLUpdate(log, size, &type, &pageId, &offset, &miniPageNo, &oldValue, &oldSize, &newValue, &newSize, &origLSN);

	PageHandle *pageHandle = GET_PAGE(session, m_file, PAGE_INDEX, pageId, Exclusived, m_dboStats, NULL);
	IndexPage *page = (IndexPage*)pageHandle->getPage();

	ftrace(ts.idx, tout << type << lsn << logCPST << pageId;);

	// д������־
	if (logCPST) {
		page->m_lsn = m_logger->logDMLUpdateCPST(session, lsn, log, size);
		page->undoDMLUpdate(type, offset, miniPageNo, oldValue, oldSize, newSize);
		session->markDirty(pageHandle);
	} else {
		if (page->m_lsn < lsn) {
			page->undoDMLUpdate(type, offset, miniPageNo, oldValue, oldSize, newSize);
			page->m_lsn = lsn;
			session->markDirty(pageHandle);
		}
	}

	session->releasePage(&pageHandle);
}


/**
 * ����SMO����
 * @param session		�Ự���
 * @param lsn			����LSN
 * @param log			��־����
 * @param size			��־����
 * @param logCPST		�Ƿ��¼������־
 */
void DrsBPTreeIndice::undoSMO(Session *session, u64 lsn, const byte *log, uint size, bool logCPST) {
	IDXLogType type = m_logger->getType(log, size);

	PageId pageId;
	byte *moveData, *moveDir;
	u16 dataSize, dirSize;
	PageHandle *pageHandle, *nextPageHandle;
	u64 origLSN1, origLSN2;

	if (type == IDX_LOG_SMO_MERGE) {
		PageId mergePageId, prevPageId;
		m_logger->decodeSMOMerge(log, size, &pageId, &mergePageId, &prevPageId, &moveData, &dataSize, &moveDir, &dirSize, &origLSN1, &origLSN2);

		ftrace(ts.idx, tout << type << logCPST <<  lsn << pageId << mergePageId;);

		pageHandle = GET_PAGE(session, m_file, PAGE_INDEX, pageId, Exclusived, m_dboStats, NULL);
		IndexPage *page = (IndexPage*)pageHandle->getPage();
		nextPageHandle = GET_PAGE(session, m_file, PAGE_INDEX, mergePageId, Exclusived, m_dboStats, NULL);
		IndexPage *mergePage = (IndexPage*)nextPageHandle->getPage();
		assert(mergePage->m_nextPage == pageId);
		if (logCPST) {
			page->m_lsn = m_logger->logSMOCPST(session, lsn, log, size);
			mergePage->m_lsn = page->m_lsn;
			page->undoSMOMergeOrig(mergePageId, dataSize, dirSize);
			mergePage->undoSMOMergeRel(prevPageId, moveData, dataSize, moveDir, dirSize);
			session->markDirty(pageHandle);
			session->markDirty(nextPageHandle);
		} else {	// redo������־
			if (page->m_lsn < lsn) {
				page->undoSMOMergeOrig(mergePageId, dataSize, dirSize);
				page->m_lsn = lsn;
				session->markDirty(pageHandle);
			}
			if (mergePage->m_lsn < lsn) {
				mergePage->undoSMOMergeRel(prevPageId, moveData, dataSize, moveDir, dirSize);
				mergePage->m_lsn = lsn;
				session->markDirty(nextPageHandle);
			}
		}
	} else if (type == IDX_LOG_SMO_SPLIT) {
		PageId newPageId, nextPageId;
		byte *oldSplitKey, *newSplitKey;
		u16 oldSKLen, newSKLen;
		u8 mpLeftCount, mpMoveCount;
		m_logger->decodeSMOSplit(log, size, &pageId, &newPageId, &nextPageId, &moveData, &dataSize, &oldSplitKey, &oldSKLen, &newSplitKey, &newSKLen, &moveDir, &dirSize, &mpLeftCount, &mpMoveCount, &origLSN1, &origLSN2);

		ftrace(ts.idx, tout << type << lsn << logCPST << pageId << newPageId;);

		pageHandle = GET_PAGE(session, m_file, PAGE_INDEX, pageId, Exclusived, m_dboStats, NULL);
		IndexPage *page = (IndexPage*)pageHandle->getPage();
		nextPageHandle = GET_PAGE(session, m_file, PAGE_INDEX, newPageId, Exclusived, m_dboStats, NULL);
		IndexPage *nextPage = (IndexPage*)nextPageHandle->getPage();
		if (logCPST) {
			page->m_lsn = m_logger->logSMOCPST(session, lsn, log, size);
			nextPage->m_lsn = page->m_lsn;
			page->undoSMOSplitOrig(nextPageId, moveData, dataSize, moveDir, dirSize, oldSplitKey, oldSKLen, newSKLen, mpLeftCount, mpMoveCount);
			nextPage->undoSMOSplitRel();
			session->markDirty(pageHandle);
			session->markDirty(nextPageHandle);
		} else {	// redo������־
			if (page->m_lsn < lsn) {
				page->undoSMOSplitOrig(nextPageId, moveData, dataSize, moveDir, dirSize, oldSplitKey, oldSKLen, newSKLen, mpLeftCount, mpMoveCount);
				page->m_lsn = lsn;
				session->markDirty(pageHandle);
			}
			if (nextPage->m_lsn < lsn) {
				nextPage->undoSMOSplitRel();
				nextPage->m_lsn = lsn;
				session->markDirty(nextPageHandle);
			}
		}
	}

	session->releasePage(&pageHandle);
	session->releasePage(&nextPageHandle);
}


/**
 * ����ҳ���޸Ĳ���
 * @param session		�Ự���
 * @param lsn			����LSN
 * @param log			��־����
 * @param size			��־����
 * @param logCPST		�Ƿ��¼������־
 */
void DrsBPTreeIndice::undoPageSet(Session *session, u64 lsn, const byte *log, uint size, bool logCPST) {
	PageId pageId;
	u64 origLSN;
	IDXLogType type = m_logger->getType(log, size);

	if (type == IDX_LOG_UPDATE_PAGE) {
		u16 offset, valueLen;
		byte *newValue, *oldValue;
		bool clearPageFirst;
		m_logger->decodePageUpdate(log, size, &pageId, &offset, &newValue, &oldValue, &valueLen, &origLSN, &clearPageFirst);

		ftrace(ts.idx, tout << type << lsn << logCPST << pageId;);

		PageHandle *pageHandle = GET_PAGE(session, m_file, PAGE_INDEX, pageId, Exclusived, m_dboStats, NULL);
		IndexPage *page = (IndexPage*)pageHandle->getPage();
		if (logCPST) {
			page->m_lsn = m_logger->logPageSetCPST(session, lsn, log, size);
			page->undoPageUpdate(offset, oldValue, valueLen);
			session->markDirty(pageHandle);
		} else {	// redo������־
			if (page->m_lsn < lsn) {
				page->undoPageUpdate(offset, oldValue, valueLen);
				page->m_lsn = lsn;
				session->markDirty(pageHandle);
			}
		}
		session->releasePage(&pageHandle);
	} else if (type == IDX_LOG_ADD_MP) {
		byte *keyValue;
		u16 dataSize, miniPageNo;
		m_logger->decodePageAddMP(log, size, &pageId, &keyValue, &dataSize, &miniPageNo, &origLSN);

		ftrace(ts.idx, tout << type << lsn << logCPST << pageId;);

		PageHandle *pageHandle = GET_PAGE(session, m_file, PAGE_INDEX, pageId, Exclusived, m_dboStats, NULL);
		IndexPage *page = (IndexPage*)pageHandle->getPage();
		if (logCPST) {
			page->m_lsn = m_logger->logPageSetCPST(session, lsn, log, size);
			page->undoPageAddMP(dataSize, miniPageNo);
			session->markDirty(pageHandle);
		} else {	// redo������־
			if (page->m_lsn < lsn) {
				page->undoPageAddMP(dataSize, miniPageNo);
				page->m_lsn = lsn;
				session->markDirty(pageHandle);
			}
		}
		session->releasePage(&pageHandle);
	} else if (type == IDX_LOG_DELETE_MP) {
		byte *keyValue;
		u16 dataSize, miniPageNo;
		m_logger->decodePageDeleteMP(log, size, &pageId, &keyValue, &dataSize, &miniPageNo, &origLSN);

		ftrace(ts.idx, tout << type << lsn << logCPST << pageId;);

		PageHandle *pageHandle = GET_PAGE(session, m_file, PAGE_INDEX, pageId, Exclusived, m_dboStats, NULL);
		IndexPage *page = (IndexPage*)pageHandle->getPage();
		if (logCPST) {
			page->m_lsn = m_logger->logPageSetCPST(session, lsn, log, size);
			page->undoPageDeleteMP(keyValue, dataSize, miniPageNo);
			session->markDirty(pageHandle);
		} else {	// redo������־
			if (page->m_lsn < lsn) {
				page->undoPageDeleteMP(keyValue, dataSize, miniPageNo);
				page->m_lsn = lsn;
				session->markDirty(pageHandle);
			}
		}
		session->releasePage(&pageHandle);
	} else if (type == IDX_LOG_MERGE_MP) {
		u16 offset, oldSize, newSize, originalMPKeyCounts, miniPageNo;
		byte *oldValue, *newValue;
		m_logger->decodePageSplitMP(log, size, &pageId, &offset, &newValue, &newSize, &oldValue, &oldSize, &originalMPKeyCounts, &miniPageNo, &origLSN);

		ftrace(ts.idx, tout << type << lsn << logCPST << pageId;);

		PageHandle *pageHandle = GET_PAGE(session, m_file, PAGE_INDEX, pageId, Exclusived, m_dboStats, NULL);
		IndexPage *page = (IndexPage*)pageHandle->getPage();
		if (logCPST) {
			page->m_lsn = m_logger->logPageSetCPST(session, lsn, log, size);
			page->undoPageMergeMP(offset, oldValue, oldSize, newSize, originalMPKeyCounts, miniPageNo);
			session->markDirty(pageHandle);
		} else {	// redo������־
			if (page->m_lsn < lsn) {
				page->undoPageMergeMP(offset, oldValue, oldSize, newSize, originalMPKeyCounts, miniPageNo);
				page->m_lsn = lsn;
				session->markDirty(pageHandle);
			}
		}
		session->releasePage(&pageHandle);
	} else {
		assert(type == IDX_LOG_SPLIT_MP);
		u16 offset, oldSize, newSize, leftItems, miniPageNo;
		byte *oldValue, *newValue;
		m_logger->decodePageSplitMP(log, size, &pageId, &offset, &oldValue, &oldSize, &newValue, &newSize, &leftItems, &miniPageNo, &origLSN);

		ftrace(ts.idx, tout << type << lsn << logCPST << pageId;);

		PageHandle *pageHandle = GET_PAGE(session, m_file, PAGE_INDEX, pageId, Exclusived, m_dboStats, NULL);
		IndexPage *page = (IndexPage*)pageHandle->getPage();
		if (logCPST) {
			page->m_lsn = m_logger->logPageSetCPST(session, lsn, log, size);
			page->undoPageSplitMP(offset, oldValue, oldSize, newSize, miniPageNo);
			session->markDirty(pageHandle);
		} else {	// redo������־
			if (page->m_lsn < lsn) {
				page->undoPageSplitMP(offset, oldValue, oldSize, newSize, miniPageNo);
				page->m_lsn = lsn;
				session->markDirty(pageHandle);
			}
		}
		session->releasePage(&pageHandle);
	}
}



/**
 * ���˴�����������������undo���û���redo������־ʱ��ʹ��
 * ������Ҫע����ǣ�undo��ʱ���������ȷʵ�Ѿ��������ˣ���ʱ��Ͳ���Ҫ����ʵ�ʵ�undo
 * redo������־��ʱ��Ҳ�������Ѿ������ڵ�����
 * @param session		�Ự���
 * @param lsn			����LSN
 * @param log			��־����
 * @param size			��־����
 * @param logCPST		�Ƿ��¼������־
 */
void DrsBPTreeIndice::undoCreateIndex(Session *session, u64 lsn, const byte *log, uint size, bool logCPST) {
	// ʹ���߼�undo���ٸò���������ڴ����־����
	u8 indexId;
	m_logger->decodeCreateIndex(log, size, &indexId);
	ftrace(ts.idx, tout << indexId;);

	if (logCPST) {
		u64 cpstLsn = m_logger->logCreateIndexCPST(session, lsn, log, size);
		m_pagesManager->discardIndexByIDXIdRecv(session, indexId, false, cpstLsn);
	} else {
		m_pagesManager->discardIndexByIDXIdRecv(session, indexId, true, lsn);
	}

	recvMemSync(session, indexId);
}



/**
 * ����DML�����Ĳ�����־
 * @param session	�Ự���
 * @param lsn		����LSN
 * @param log		������־����
 * @param size		������־����
 */
void DrsBPTreeIndice::redoCpstDML(Session *session, u64 lsn, const byte *log, uint size) {
	ftrace(ts.idx, );
	undoDML(session, lsn, log, size, false);
}



/**
 * �����������������Ĳ�����־
 * @param session	�Ự���
 * @param lsn		����LSN
 * @param log		������־����
 * @param size		������־����
 */
void DrsBPTreeIndice::redoCpstCreateIndex(Session *session, u64 lsn, const byte *log, uint size) {
	ftrace(ts.idx, );
	undoCreateIndex(session, lsn, log, size, false);
}



/**
 * ����SMO�����Ĳ�����־
 * @param session	�Ự���
 * @param lsn		����LSN
 * @param log		������־����
 * @param size		������־����
 */
void DrsBPTreeIndice::redoCpstSMO(Session *session, u64 lsn, const byte *log, uint size) {
	ftrace(ts.idx, );
	undoSMO(session, lsn, log, size, false);
}



/**
 * ����ҳ���޸Ĳ����Ĳ�����־
 * @param session	�Ự���"
 * @param lsn		����LSN
 * @param log		������־����
 * @param size		������־����
 */
void DrsBPTreeIndice::redoCpstPageSet(Session *session, u64 lsn, const byte *log, uint size) {
	ftrace(ts.idx, );
	undoPageSet(session, lsn, log, size, false);
}



/**
 * Buffer����ҳ��ص��������ж�ĳ��ҳ���Ƿ���ָ��������
 * ��Ҫ����pageId���жϵ�ǰҳ���ǲ���ͷҳ���������λͼҳ
 * TODO: ���õķ�������IndicePage�൱�����һ���������
 * �������ַ������ᵼ�����е��������������ļ��������ã������޸�
 *
 * @param page		Ҫ�жϵĻ���ҳ��
 * @param pageId	����ҳ���PageId��Ϣ
 * @param indexId	ָ��������ID
 * @return ����ָ������true/������false
 */
bool DrsBPTreeIndice::bufferCallBackSpecifiedIDX(Page *page, PageId pageId, uint indexId) {
	return pageId >= IndicePageManager::NON_DATA_PAGE_NUM && IndexPage::getIndexId(((IndexPage*)page)->m_pageMark) == indexId;
}

/**
 * ��ȡ������������ռ�ÿռ��С���������Ѿ����䵫û��ʹ�õ�ҳ��
 *
 * @param includeMeta �Ƿ���������ļ�ͷ��ҳ�����λͼ�ȷ�����ҳ
 * @return ������������ռ�ÿռ��С
 */
u64 DrsBPTreeIndice::getDataLength(bool includeMeta) {
	u64 length = 0;
	// �������������ۼ�ͳ����Ϣ
	for (uint i = 0; i < m_indexNum; i++) {
		DrsIndex *index = m_indice[i];
		IndexStatus status = index->getStatus();
		length = status.m_dataLength - status.m_freeLength + length;
	}
	if (includeMeta)
		length +=  IndicePageManager::NON_DATA_PAGE_NUM * INDEX_PAGE_SIZE;
	return length;
}

/**
 * �õ�ָ��ҳ��������������
 *
 * @param page ҳ�����ݣ��Ѿ���S������
 * @param pageId ҳ��
 * @return ���ָ����ҳ��Ϊĳ������ռ�õ�ҳ���򷵻������ţ�����ǲ�������ĳ�������Ĺ���ҳ���򷵻�-1
 */
int DrsBPTreeIndice::getIndexNo(const BufferPageHdr *page, u64 pageId) {
	assert(page != NULL && pageId < m_pagesManager->getFileSize() / INDEX_PAGE_SIZE);
	if (pageId < IndicePageManager::NON_DATA_PAGE_NUM)
		return -1;

	IndexPage *indexPage = (IndexPage*)page;
	u8 indexId = (u8)IndexPage::getIndexId(indexPage->m_pageMark);
	u8 indexNo = m_pagesManager->getIndexNo(indexId);

	// �����������ķ�ʽ�ٴ�ȷ��ĳ��ҳ���Ƿ�����ĳ������
	IndexHeaderPage *header = m_pagesManager->getFileHeaderPage();
	if (indexNo >= header->m_indexNum || header->m_indexIds[indexNo] != indexId)
		return -1;

	return indexNo;
}

/**
 * ���ش���������
 */
LobStorage* DrsBPTreeIndice::getLobStorage() {
	return m_lobStorage;
}

/**
 * �������ݿ����
 */
Database* DrsBPTreeIndice::getDatabase() {
	return m_db;
}


/**
 * �������Ŀ¼���ݶ���ͳ����Ϣ 
 * @return ���ݶ���״̬
 */
DBObjStats* DrsBPTreeIndice::getDBObjStats() {
	return m_dboStats;
}

/**
 * ����Ҫ���µĺ�����㵱ǰ����Щ������Ҫ����
 * @param memoryContext		�ڴ���������ģ�����������µ�����������飬������ע��ռ��ʹ�ú��ͷ�
 * @param update			���²����ĺ�����Ϣ�������ֶ���������
 * @param updateNum			out �����ж��ٸ�������Ҫ������
 * @param updateIndices		out ָ��memoryContext����Ŀռ䣬������Ҫ���µ�������ţ�����ź������ڲ��������к�һ��
 * @param updateUniques		out ���ض���Ψһ������Ҫ������
 */
void DrsBPTreeIndice::getUpdateIndices( MemoryContext *memoryContext, const SubRecord *update, u16 *updateNum, u16 **updateIndices, u16 *updateUniques ) {
	u16 *toBeUpdated = (u16*)memoryContext->alloc(sizeof(update->m_columns[0]) * Limits::MAX_INDEX_NUM);
	u16 updates = 0;
	u16 uniques = 0;

	for (u16 i = 0; i < m_tableDef->m_numIndice; i++) {
		u16 indexNo = m_orderedIndices.getOrder(i);
		IndexDef *indexDef = m_tableDef->m_indice[indexNo];
		for (u16 j = 0; j < indexDef->m_numCols; j++) {
			// ����ĸ����ֶΣ����Զ��ֲ���
			if (std::binary_search(update->m_columns, update->m_columns + update->m_numCols, indexDef->m_columns[j])) {
				toBeUpdated[updates++] = indexNo;
				if (indexDef->m_unique)
					uniques++;
				break;
			}
		}
	}

	*updateNum = updates;
	*updateIndices = toBeUpdated;
	*updateUniques = uniques;
}

//
//	Implementation of class IndicePageManager
//
////////////////////////////////////////////////////////////////////////////////////


/**
 * ����ҳ����������캯��
 * @param tableDef		��������
 * @param file			�ļ����
 * @param headerPage	�ļ�ͷҳ��
 * @param syslog		ϵͳ��־����
 * @param dbObjStats	���ݿ�ͳ�ƶ���
 */
IndicePageManager::IndicePageManager(const TableDef *tableDef, File *file, Page *headerPage, Syslog *syslog, DBObjStats *dbObjStats) {
	m_file = file;
	m_headerPage = (IndexHeaderPage*)headerPage;
	m_logger = syslog;
	m_dbObjStats = dbObjStats;
	m_tableDef = tableDef;
	u64 errNo =	file->getSize(&m_fileSize);
	if (File::getNtseError(errNo) != File::E_NO_ERROR)
		NTSE_ASSERT(false);
}

/**
 * ��ʼ��ָ�������ļ���ͷ����Ϣ����λͼ
 * @param file �����ļ�������
 */
void IndicePageManager::initIndexFileHeaderAndBitmap(File *file) {
	u64 offset = INDEX_PAGE_SIZE;
	// ��ʼ�������ļ�ͷ����Ϣ��д���ļ�
	byte *page = (byte*)System::virtualAlloc(INDEX_PAGE_SIZE);

	IndexHeaderPage *iFileHeader = new IndexHeaderPage();
	memset(page, 0, INDEX_PAGE_SIZE);
	memcpy(page, iFileHeader, sizeof(IndexHeaderPage));

	u64 errNo = file->setSize(INDEX_PAGE_SIZE * (BITMAP_PAGE_NUM + 1));
	if (File::getNtseError(errNo) != File::E_NO_ERROR) {
		goto Fail;
	}

	errNo = file->write((u64)0, INDEX_PAGE_SIZE, page);
	if (File::getNtseError(errNo) != File::E_NO_ERROR) {
		goto Fail;
	}

	// ��ʼ�������ļ�λͼ��Ϣ��д���ļ�
	memset(page, 0, INDEX_PAGE_SIZE);
	for (uint i = 0; i < BITMAP_PAGE_NUM; i++) {
		errNo = file->write(offset, INDEX_PAGE_SIZE, page);
		if (File::getNtseError(errNo) != File::E_NO_ERROR) {
			goto Fail;
		}
		offset += INDEX_PAGE_SIZE;
	}

	errNo = file->sync();
	if (File::getNtseError(errNo) != File::E_NO_ERROR) {
		goto Fail;
	}

	System::virtualFree(page);
	delete iFileHeader;
	return;

Fail:
	System::virtualFree(page);
	delete iFileHeader;
	NTSE_ASSERT(false);
}



/**
 * ���㵱ǰ�ĸ�λͼ���ʾ��ҳ������
 *
 * @pre �������Ѿ�������ͷҳ��ӹ�����������λͼҳ��������ֱ�ӻ�ȡ
 * @param logger	��־��¼��
 * @param session	�Ự���
 * @param indexId	�������к�
 * @return ����ҳ������ʼҳ��ID
 */
u64 IndicePageManager::calcPageBlock( IndexLog *logger, Session *session, u8 indexId) {
	/* ͨ��λͼ����һ���µ������飬�����ҳ�� */
	u32 bmOffset = ((IndexHeaderPage*)m_headerPage)->m_curFreeBitMap;
	u32 inOffset = bmOffset % INDEX_PAGE_SIZE;
	PageId bmPageId = bmOffset / INDEX_PAGE_SIZE + HEADER_PAGE_NUM - 1;

	if (inOffset == 0)	// ����һ���µ�λͼҳ��
		inOffset = sizeof(Page);

	Page *bmPage;
	PageHandle *bmHandle;
	while (true) {
		bmHandle = GET_PAGE(session, m_file, PAGE_INDEX, bmPageId, Exclusived, m_dbObjStats, NULL);
		bmPage = bmHandle->getPage();

		byte *start = (byte*)bmPage + inOffset;
		while (true) {
			if (inOffset < INDEX_PAGE_SIZE && *start != 0x00) {		// ��Byte��ʾ������ʹ��
				inOffset++;
				start++;
				continue;
			}

			break;	// ���ڿհ�ҳ������ҳ���������
		}

		if (inOffset >= INDEX_PAGE_SIZE) {		// ��Ҫ�����һ��λͼҳ
			session->releasePage(&bmHandle);
			bmPageId++;
			if (bmPageId >= BITMAP_PAGE_NUM) {	// ��Ҫѭ������
				bmPageId = HEADER_PAGE_NUM;
			}
			inOffset = sizeof(Page);
			continue;
		}

		// ����˵����ǰλͼҳ��inOffsetλ�õĵ�posλ��ʾ�Ŀ���п���
		((IndexFileBitMapPage*)bmPage)->updateInfo(session, logger, bmPageId, (u16)(start - (byte*)bmPage), 1, (byte*)&indexId);
		session->markDirty(bmHandle);
		session->releasePage(&bmHandle);
		u32 newCurFreeBitMap = INDEX_PAGE_SIZE * (u32)bmPageId + inOffset + 1;
		((IndexHeaderPage*)m_headerPage)->updateInfo(session, logger, 0, OFFSET(IndexHeaderPage, m_curFreeBitMap), sizeof(u32), (byte*)&newCurFreeBitMap);
		if (bmPageId > ((IndexHeaderPage*)m_headerPage)->m_maxBMPageIds[getIndexNo(indexId)]) {
			u32 newMaxBMPageId = (u32)bmPageId;
			IndexHeaderPage *headerPage = (IndexHeaderPage*)m_headerPage;
			headerPage->updateInfo(session, logger, 0, OFFSETOFARRAY(IndexHeaderPage, m_maxBMPageIds, sizeof(u32), getIndexNo(indexId)), sizeof(u32), (byte*)&newMaxBMPageId);
		}

		return getPageBlockStartId(bmPageId, (u16)inOffset);
	}
}


/**
 * ����ָ����ҳ�����ʼ��ַ������/��ʼ��һ���µ�ҳ���
 * ����ҳ������ҳ�湩ʹ�ã�������Ҫ��ʼ������ҳ�����X Latch
 *
 * @pre �Ѿ������ļ�ͷҳ���Latch������ֱ�Ӷ�m_headerPage���ж�д
 * @param logger		��־��¼��
 * @param session		�Ự���
 * @param blockStartId	Ҫ����ҳ�����ʼҳ���ID
 * @param indexId		��Ҫ����ҳ�������ID
 * @param incrSize		�����Ҫ��չ�ļ���ÿ��������չ�ô�С�ķ�Χ
 * @return ҳ������ҳ��X Latch�����ʹ�����ͷ�
 */
PageHandle* IndicePageManager::allocPageBlock(IndexLog *logger, Session *session, u64 blockStartId, u8 indexId) {
	//ftrace(ts.idx, tout << indexId << " alloc " << blockStartId;);
	u8 indexNo = getIndexNo(indexId);

	extendFileIfNecessary(blockStartId);
	IndexHeaderPage *headerPage = (IndexHeaderPage*)m_headerPage;
	headerPage->updatePageUsedStatus(session, logger, indexNo, headerPage->m_indexDataPages[indexNo] + PAGE_BLOCK_NUM, PAGE_BLOCK_NUM);

	// ��ʼ������ÿһ������ҳ�棨��������һ��ҳ�棩
	PageId idOffset = 1;
	u32 mark = IndexPage::formPageMark(indexId);
	while (idOffset < PAGE_BLOCK_NUM) {
		PageId pageId = blockStartId + idOffset;
		PageHandle *pHandle = NEW_PAGE(session, m_file, PAGE_INDEX, pageId, Exclusived, m_dbObjStats);	// ���ڳ���ͷҳ��X-Latch����ҳ��鲻���ܱ�������ʹ�ã���ֱ�Ӽ���
		IndexPage *prevPage = (IndexPage*)(pHandle->getPage());
		session->markDirty(pHandle);
		prevPage->clearData();	// Ϊ�˱�֤ҳ�������֤������ȷ�ԣ����ҳ�������������Ϣ��ͷ��������
		prevPage->initPage(session, logger, false, false, pageId, mark, FREE, 0, INVALID_PAGE_ID, idOffset < PAGE_BLOCK_NUM - 1 ? pageId + 1: INVALID_PAGE_ID);
		session->releasePage(&pHandle);

		idOffset++;
	}

	PageHandle *handle = GET_PAGE(session, m_file, PAGE_INDEX, blockStartId, Exclusived, m_dbObjStats, NULL);
	((IndexPage*)handle->getPage())->clearData();
	return handle;
}



/**
 * Ϊָ����������һ����ҳ�棬�����ǰ����ҳ������Ϊ�����·���һ��ҳ��飬����ҳ�����X-Latch
 *
 * @param logger		��־��¼��
 * @param session		�Ự���
 * @param indexId		����ID
 * @param pageId		IN/OUT	�������ҳ���ID��
 * @return PageHandle*	���ؼ���X-Latch���·����ҳ�棬��㸺���ͷ�Latch
 */
PageHandle* IndicePageManager::allocPage( IndexLog *logger, Session *session, u8 indexId, PageId *pageId) {
	PageHandle *pHeaderHandle = GET_PAGE(session, m_file, PAGE_INDEX, 0, Exclusived, m_dbObjStats, m_headerPage);
	IndexHeaderPage *headerPage = (IndexHeaderPage*)(pHeaderHandle->getPage());
	PageHandle *newPageHandle;

	u8 indexNo = getIndexNo(indexId);
	if (headerPage->m_freeListPageIds[indexNo] == INVALID_PAGE_ID) {
		// ��Ҫ������һ��ҳ���
		*pageId = calcPageBlock(logger, session, indexId);
		newPageHandle = allocPageBlock(logger, session, *pageId, indexId);
		PageId freePageId = *pageId + 1;
		headerPage->updateInfo(session, logger, 0, OFFSETOFARRAY(IndexHeaderPage, m_freeListPageIds, sizeof(PageId), indexNo), sizeof(PageId), (byte*)&freePageId);
	} else {
		// ֱ�ӻ�õ�ǰ����ҳ�����ף�����õ���ҳ����������϶�����յ�
		*pageId = headerPage->m_freeListPageIds[indexNo];
		newPageHandle = GET_PAGE(session, m_file, PAGE_INDEX, *pageId, Exclusived, m_dbObjStats, NULL);
		PageId freePageId = ((IndexPage*)newPageHandle->getPage())->m_nextPage;
		headerPage->updateInfo(session, logger, 0, OFFSETOFARRAY(IndexHeaderPage, m_freeListPageIds, sizeof(PageId), indexNo), sizeof(PageId), (byte*)&freePageId);
	}

	// ���ҳ��ʹ��״̬��Ϣ
	headerPage->updatePageUsedStatus(session, logger, indexNo, (u64)-1, headerPage->m_indexFreePages[indexNo] - 1);

	session->markDirty(pHeaderHandle);
	session->releasePage(&pHeaderHandle);

	assert(newPageHandle != NULL);
	vecode(vs.idx, ((IndexPage*)newPageHandle->getPage())->isDataClean());
	return newPageHandle;
}


/**
 * Ϊ�´�������������һ���µĸ�ҳ�棬��ͷҳ���¼�����Ϣ������ҳ�����X-Latch
 *
 * @param logger	��־��¼��
 * @param session	�Ự���
 * @param indexNo	OUT �������
 * @param indexId	OUT	����ID
 * @param pageId	OUT	�������ҳ���ID��
 * @return PageHandle*	���ؼ���X-Latch���·����ҳ�棬��㸺���ͷ�Latch
 */
PageHandle* IndicePageManager::createNewIndexRoot(IndexLog *logger, Session *session, u8 *indexNo, u8 *indexId, PageId *rootPageId) {
	// ��ȡ�ļ�ͷҳ����Ϣ
	PageHandle *headerHandle = GET_PAGE(session, m_file, PAGE_INDEX, 0, Exclusived, m_dbObjStats, m_headerPage);
	IndexHeaderPage *headerPage = (IndexHeaderPage*)(headerHandle->getPage());

	*indexId = headerPage->getAvailableIndexIdAndMark(session, logger);
	u32 num = headerPage->m_indexNum + 1;
	headerPage->updateInfo(session, logger, 0, OFFSET(IndexHeaderPage, m_indexNum), sizeof(u32), (byte*)&num);

	*indexNo = getIndexNo(*indexId);
	// ���㲢����ҳ��飬��ʼ����ҳ��
	PageId rootId = calcPageBlock(logger, session, *indexId);
	PageHandle *rootHandle = allocPageBlock(logger, session, rootId, *indexId);

	// �����¸�ҳ������������ͷ��ļ�ͷҳ����
	headerPage->updateInfo(session, logger, 0, OFFSETOFARRAY(IndexHeaderPage, m_rootPageIds, sizeof(PageId), headerPage->m_indexNum - 1), sizeof(PageId), (byte*)&rootId);
	PageId freePageId = rootId + 1;
	headerPage->updateInfo(session, logger, 0, OFFSETOFARRAY(IndexHeaderPage, m_freeListPageIds, sizeof(PageId), headerPage->m_indexNum - 1), sizeof(PageId), (byte*)&freePageId);
	// ����ҳ��ʹ��ͳ����Ϣ
	headerPage->updatePageUsedStatus(session, logger, *indexNo, PAGE_BLOCK_NUM, PAGE_BLOCK_NUM - 1);

	session->markDirty(headerHandle);
	session->releasePage(&headerHandle);

	*rootPageId = rootId;
	return rootHandle;
}




/**
 * �ͷ�ָ��ҳ�棬�����Ӧ�����Ŀ���ҳ������
 *
 * @pre �����Ҫ����Ҫ�ͷŵ�ҳ�棬���ҶԸ�ҳ�����X-Latch
 * @pre ���ݱ���������ƣ������ܳ��ֳ���ͷҳ������ĳ������ʹ�õ�����ҳ����������
 *
 * @param logger	��־��¼��
 * @param session	�Ự���
 * @param pageId	Ҫ�ͷ�ҳ��ID
 * @param indexPage	Ҫ�ͷŵ�����ҳ��
 */
void IndicePageManager::freePage(IndexLog *logger, Session *session, PageId pageId, Page *indexPage) {
	IndexPage *page = (IndexPage*)indexPage;
	assert(page->m_pageType != FREE);
	u8 indexId = (u8)IndexPage::getIndexId(page->m_pageMark);
	u8 indexNo = getIndexNo(indexId);

	PageHandle *pHeaderHandle = GET_PAGE(session, m_file, PAGE_INDEX, 0, Exclusived, m_dbObjStats, m_headerPage);	// �ô�����ֱ�Ӽ���������������
	IndexHeaderPage *headerPage = (IndexHeaderPage*)pHeaderHandle->getPage();
	page->initPage(session, logger, false, false, pageId, page->m_pageMark, FREE, page->m_pageLevel, INVALID_PAGE_ID, headerPage->m_freeListPageIds[indexNo]);
	headerPage->updateInfo(session, logger, 0, OFFSETOFARRAY(IndexHeaderPage, m_freeListPageIds, sizeof(PageId), indexNo), sizeof(PageId), (byte*)&pageId);
	// ���ҳ��ʹ��״̬��Ϣ
	headerPage->updatePageUsedStatus(session, logger, indexNo, (u64)-1, headerPage->m_indexFreePages[indexNo] + 1);

	session->markDirty(pHeaderHandle);
	session->releasePage(&pHeaderHandle);
}


/**
 * ��λͼ�н�����ָ��������ҳ��鸴λ����
 *
 * @pre ��Ҫ�ȶ������ļ�ͷҳ����мӻ�����
 *
 * @param session		�Ự���
 * @param indexId		ָ��������ID
 * @param setZero		�ͷ�ҳ����Ƿ���Ҫ����
 * @param maxFreePageId	������ʹ��λͼλ�����ƫ����
 * @param opLsn			�����ò�����LSN
 * @param isRedo		���������Ƿ���redo����
 * @return �ͷ��Ժ���С�Ŀ���λͼλ��ƫ����
 */
u32 IndicePageManager::freePageBlocksByIDXId(Session *session, u8 indexId, bool setZero, u32 maxFreePageId, u64 opLsn, bool isRedo) {
	assert(opLsn != INVALID_LSN);
	PageId maxBMPageId = maxFreePageId;
	assert(maxBMPageId >= HEADER_PAGE_NUM && maxBMPageId < NON_DATA_PAGE_NUM);
	UNREFERENCED_PARAMETER(setZero);
	u32 minFreeByteMap = (u32)-1;

	for (uint id = HEADER_PAGE_NUM; id <= maxBMPageId; id++) {
		bool changed = false;
		PageHandle *handle = GET_PAGE(session, m_file, PAGE_INDEX, id, Exclusived, m_dbObjStats, NULL);
		Page *page = handle->getPage();

		if (isRedo && page->m_lsn >= opLsn) {	// redo������¿����ж�LSN�Ż�
			session->releasePage(&handle);
			continue;
		}

		byte *mark = (byte*)page + sizeof(Page);
		byte *end = (byte*)page + INDEX_PAGE_SIZE;
		while (mark < end) {
			if (*mark == indexId) {
				if (minFreeByteMap == (u32)-1)
					minFreeByteMap = id * INDEX_PAGE_SIZE + uint(mark - (byte*)page);
				*mark = 0x00;
				changed = true;
				page->m_lsn = opLsn;
				// TODO:�����ͷ�ҳ��Ҫ����Ĳ���
			}
			mark++;
		}

		if (changed)
			session->markDirty(handle);
		session->releasePage(&handle);
	}

	return minFreeByteMap;
}


/**
 * ����ָ��id���������޸�ͷҳ�桢��ո�λ����ʹ�õ�λͼҳ���λ��Ϣ
 *
 * @param session	�Ự���
 * @param logger	��־��¼��
 * @param indexId	����ID
 * @param idxNo		ɾ�������ڱ����е����
 * @return
 */
void IndicePageManager::discardIndexByIDXId(Session *session, IndexLog *logger, u8 indexId, s32 idxNo) {
	assert(idxNo <= Limits::MAX_INDEX_NUM);
	ftrace(ts.idx, tout << indexId << idxNo;);
	// ��ȡͷҳ�棬�õ���������λͼҳʹ�����Χ
	PageHandle *rootHandle = GET_PAGE(session, m_file, PAGE_INDEX, 0, Exclusived, m_dbObjStats, m_headerPage);
	IndexHeaderPage *headerPage = (IndexHeaderPage*)rootHandle->getPage();
	u32 maxFreePageId = headerPage->m_maxBMPageIds[getIndexNo(indexId)];

	u64 lsn = logger->logDropIndex(session, indexId, idxNo);
	u32 minFreeByteMap = freePageBlocksByIDXId(session, indexId, false, maxFreePageId, lsn, false);

	u8 indexNo = getIndexNo(indexId);
	// �޸�ͷҳ����Ϣ
	headerPage->discardIndexChangeHeaderPage(indexNo, minFreeByteMap);
	m_headerPage->m_lsn = lsn;
	session->markDirty(rootHandle);

	session->releasePage(&rootHandle);
}



/**
 * ����ָ��id���������޸�ͷҳ�桢��ո�λ����ʹ�õ�λͼҳ���λ��Ϣ
 *
 * �ú�����Ȼ�޸���λͼҳ�������Ϣ�����������ǻָ����̵��ã����̲߳���Ҫͬ��
 *
 * @param session	�Ự���
 * @param indexId	����ID
 * @param isRedo	���������Ƿ���redo����
 * @param opLsn		����������LSN
 * @return
 */
void IndicePageManager::discardIndexByIDXIdRecv(Session *session, u8 indexId, bool isRedo, u64 opLsn) {
	PageHandle *headerHandle = GET_PAGE(session, m_file, PAGE_INDEX, 0, Exclusived, m_dbObjStats, m_headerPage);
	IndexHeaderPage *headerPage = (IndexHeaderPage*)headerHandle->getPage();

	u32 minFreeByteMap = freePageBlocksByIDXId(session, indexId, false, NON_DATA_PAGE_NUM - 1, opLsn, isRedo);

	if (!isRedo || headerPage->m_lsn < opLsn) {
		u8 indexNo = getIndexNo(indexId);
		if (indexNo != Limits::MAX_INDEX_NUM) {	// ����Ҳ�����˵�������Ѿ���������
			headerPage->discardIndexChangeHeaderPage(indexNo, minFreeByteMap);
			m_headerPage->m_lsn = opLsn;
			session->markDirty(headerHandle);
		}
	}

	session->releasePage(&headerHandle);
}


/**
 * ����ָ������ID��ͷҳ����Ѱ�Ҷ�Ӧ���߼����
 *
 * m_indexIds�����ͬ����Ҫ���ϲ��DLL��ɾ�����޸�����֤���������ֱ�ӷ���
 *
 * @pre ����ͷҳ���S-Latch��X
 * @param indexId		����ID
 * @return ������ͷҳ���е��ڲ��߼����
 */
u8 IndicePageManager::getIndexNo(u8 indexId) {
	IndexHeaderPage *header = (IndexHeaderPage*)m_headerPage;
	for (u8 indexNo = 0; indexNo < header->m_indexNum; indexNo++)
		if (header->m_indexIds[indexNo] == indexId)
			return indexNo;

	return Limits::MAX_INDEX_NUM;
}


/**
 * �ж�һ��ҳ���Ƿ���λͼҳ
 * @param pageId	ҳ��ID
 * @return true���ڣ�false��ʾ������
 */
bool IndicePageManager::isPageByteMapPage(PageId pageId) {
	return pageId >= HEADER_PAGE_NUM && pageId < NON_DATA_PAGE_NUM;
}


/**
 * ����ָ��λͼ���ڲ�ƫ�ƣ�����ָ����Ӧ�������ļ�ҳ��ID
 * @param bmPageId	λͼҳID
 * @param offset	λͼҳ�ڱ����õ�byteƫ��
 * @return	��Ӧҳ������ʼҳ��ID
 */
PageId IndicePageManager::getPageBlockStartId(PageId bmPageId, u16 offset) {
	assert(bmPageId <= NON_DATA_PAGE_NUM);
	u32 relativeOffset = offset - sizeof(Page);
	return (HEADER_PAGE_NUM + BITMAP_PAGE_NUM) + (((INDEX_PAGE_SIZE - sizeof(Page)) * (bmPageId - HEADER_PAGE_NUM) + relativeOffset)) * PAGE_BLOCK_NUM;
}


/**
 * ����ָ����ҳ������ʼҳ��ID���жϵ�ǰ�Ƿ��б�Ҫ��չ�����ļ�
 * �����ǰ���ļ�����ָ��ҳ�������Ҫ�Ŀռ䣬����չ
 * @param blockStartId	��չ�����ʼҳ��ID
 * @return TRUE��ʾ��չ�˿ռ�/FALSE��ʾû����չ
 */
bool IndicePageManager::extendFileIfNecessary(PageId blockStartId) {
	u64 errNo;
	u64 newLeastSize = (blockStartId + PAGE_BLOCK_NUM) * INDEX_PAGE_SIZE;
	// �ж��Ƿ���Ҫ��չ�ļ�
	if (newLeastSize > m_fileSize) {
		ftrace(ts.idx, tout << blockStartId;);
		u16 incrSize = Database::getBestIncrSize(m_tableDef, m_fileSize);
		while (m_fileSize < newLeastSize)
			m_fileSize += max((u16)PAGE_BLOCK_NUM, incrSize) * INDEX_PAGE_SIZE;
		errNo = m_file->setSize(m_fileSize);
		if (File::getNtseError(errNo) != File::E_NO_ERROR)
			m_logger->fopPanic(errNo, "Cannot set index file %s's size.", m_file->getPath());
		return true;
	}

	return false;
}


/**
 * ��Ҫͳ�Ƶ�ʱ���������ͳ����Ϣ���е�ҳ��ʹ�������Ϣ
 * ���ｫ������������ļ�ͷҳ��İ취��ȡ��ͳ����Ϣ������û�в������ƣ���Ϣ���ܲ���׼ȷ������ֻҪ���ˢ�±ض�����׼ȷ
 * @param indexId	�����ڲ����
 * @param status	״̬����
 */
void IndicePageManager::updateNewStatus(u8 indexId, struct IndexStatus *status) {
	u8 indexNo = getIndexNo(indexId);
	IndexHeaderPage *header = (IndexHeaderPage*)m_headerPage;
	status->m_dataLength = header->m_indexDataPages[indexNo] * Limits::PAGE_SIZE;
	status->m_freeLength = header->m_indexFreePages[indexNo] * Limits::PAGE_SIZE;
}

/** ����ָ�������ĸ�ҳ����Ϣ
 * @param logger		������־��¼��
 * @param session		�Ự���
 * @param indexNo		Ҫ���µ��������
 * @param rootPageId	�������ĸ�ҳ��ID
 */
void IndicePageManager::updateIndexRootPageId( IndexLog *logger, Session *session, u8 indexNo, PageId rootPageId ) {
	PageHandle *headerHandle = GET_PAGE(session, m_file, PAGE_INDEX, 0, Exclusived, m_dbObjStats, m_headerPage);
	((IndexHeaderPage*)m_headerPage)->updateInfo(session, logger, 0, OFFSETOFARRAY(IndexHeaderPage, m_rootPageIds, sizeof(u64), indexNo), sizeof(u64), (byte*)&rootPageId);
	session->markDirty(headerHandle);
	session->releasePage(&headerHandle);
}

/** 
 * ���캯��������������������
 */
OrderedIndices::OrderedIndices() {
	m_uniqueIdxNum = m_indexNum = 0;
	memset(&m_orderedIdxNo[0], -1, sizeof(m_orderedIdxNo[0]) * Limits::MAX_INDEX_NUM);
	memset(&m_orderedIdxId[0], -1, sizeof(m_orderedIdxId[0]) * Limits::MAX_INDEX_NUM);
}

/** ��������������������һ���µ�����
 * @post ���ı���е��������
 * @param order		�������ڴ���ţ���Ӧ������m_indice�����˳��
 * @param idxId		ÿ������Ψһ��ID�ţ������ڲ�����
 * @param unique	�������Ƿ�Ψһ
 * @return true���ӳɹ���false�޷�����
 */
bool OrderedIndices::add( u16 order, u8 idxId, bool unique ) {
	if (m_indexNum >= Limits::MAX_INDEX_NUM)
		return false;

	u16 start = unique ? 0 : m_uniqueIdxNum;
	u16 end = unique ? m_uniqueIdxNum : m_indexNum;

	u16 i = 0;
	for (i = start; i < end; i++) {
		if (m_orderedIdxId[i] > idxId)
			break;
		assert(m_orderedIdxId[i] != idxId);
	}

	if (i != m_indexNum) {
		memmove(&m_orderedIdxNo[i + 1], &m_orderedIdxNo[i], sizeof(m_orderedIdxNo[0]) * (m_indexNum - i));
		memmove(&m_orderedIdxId[i + 1], &m_orderedIdxId[i], sizeof(m_orderedIdxId[0]) * (m_indexNum - i));
	}
	m_orderedIdxNo[i] = order;
	m_orderedIdxId[i] = idxId;

	if (unique)
		m_uniqueIdxNum++;
	m_indexNum++;

	return true;
	// ���Զ��ԣ���Ψһ�ͷ�Ψһ�������֣�id�Ծ���������
}

/** �����������������Ƴ�ָ��������
 * @param order		�������ڴ���ţ���Ӧ������m_indice�����˳��
 * @param unique	Ҫ�Ƴ��������ǲ���Ψһ
 * @return trueɾ���ɹ���false�Ҳ���ָ��������
 */
bool OrderedIndices::remove( u16 order, bool unique ) {
	for (u16 removeNo = 0; removeNo < m_indexNum; removeNo++) {
		if (m_orderedIdxNo[removeNo] == order) {
			memmove(&m_orderedIdxNo[removeNo], &m_orderedIdxNo[removeNo + 1], sizeof(m_orderedIdxNo[0]) * (m_indexNum - removeNo - 1));
			memmove(&m_orderedIdxId[removeNo], &m_orderedIdxId[removeNo + 1], sizeof(m_orderedIdxId[0]) * (m_indexNum - removeNo - 1));
			m_orderedIdxNo[m_indexNum - 1] = (u16)-1;
			m_orderedIdxId[m_indexNum - 1] = (u8)-1;

			// ��Ҫ��ɾ������֮�����Щ������ŵ���
			for (u16 i = 0; i < m_indexNum - 1; i++) {
				if (m_orderedIdxNo[i] > order)
					--m_orderedIdxNo[i];
			}

			if (unique)
				--m_uniqueIdxNum;
			--m_indexNum;

			return true;
		}
	}

	return false;
}

/** ȡ��ָ��λ�õ��������ڴ�δ������ţ�ͬm_indice�������
 * @param no	Ҫȡ������������λ��
 * @return ��Ӧ��������m_indice��������
 */
u16 OrderedIndices::getOrder( u16 no ) const {
	assert(no < m_indexNum);
	return m_orderedIdxNo[no];
}

/** �õ�Ψһ�����ĸ���
 * @return Ψһ��������
 */
u16 OrderedIndices::getUniqueIdxNum() const {
	return m_uniqueIdxNum;
}

/** ����ָ����ŵ��������ڲ������
 * @return ��������ţ���������ڣ�����(u16)-1
 */
u16 OrderedIndices::find( u16 order ) const {
	for (u16 i = 0; i < m_indexNum; i++)
		if (order == m_orderedIdxNo[i])
			return i;

	return (u16)-1;
}

void OrderedIndices::getOrderUniqueIdxNo(const u16 ** const orderUniqueIdxNo, u16 *uniqueIdxNum) const  {
	*orderUniqueIdxNo = &m_orderedIdxNo[0];
	*uniqueIdxNum = m_uniqueIdxNum;
}
}
