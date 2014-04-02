/**
* TNT�ڴ���������
*
* @author ��ΰ��(liweizhao@corp.netease.com)
*/
#include "btree/IndexBLinkTreeManager.h"
#include "btree/IndexBLinkTree.h"
#include <algorithm>

namespace tnt {

BLinkTreeIndice::BLinkTreeIndice(TNTDatabase *db, Session *session, uint indexNum, TableDef **tableDef, 
								 LobStorage *lobStorage, const DoubleChecker *doubleChecker)
								 : m_db(db), m_tableDef(tableDef), m_indexNum((u16)indexNum),
								 m_lobStorage(lobStorage), m_doubleChecker(doubleChecker) {
	m_pageManager = m_db->getTNTIMPageManager();				 
}

BLinkTreeIndice::~BLinkTreeIndice() {
	assert(m_indice == NULL);
}


bool BLinkTreeIndice::init(Session *session) {
	m_indice = new MIndex*[Limits::MAX_INDEX_NUM];
	memset(m_indice, 0, Limits::MAX_INDEX_NUM * sizeof(MIndex*));
	for (uint i = 0; i < m_indexNum; i++) {
		m_indice[i] = new BLinkTree(session, this, m_pageManager, m_doubleChecker, m_tableDef, 
			(*m_tableDef)->m_indice[i], (u8)i);
		if (! m_indice[i]->init(session, false)) {
			for(uint j = 0; j < i; j++) {
				m_indice[j]->close(session);
				delete m_indice[j];
			}
			delete m_indice[i];
			delete [] m_indice;
			m_indice = NULL;
			return false;
		}
	}
	return true;
}
/**
 * @see MIndice#close
 */
void BLinkTreeIndice::close(Session *session) {
	for (uint i = 0; i < m_indexNum; i++) {
		m_indice[i]->close(session);
		delete m_indice[i];
		m_indice[i] = NULL;
	}
	delete [] m_indice;
	m_indice = NULL;
	m_pageManager = NULL;
	m_db = NULL;
}

/**
 * @see MIndice#createIndex
 */
MIndex* BLinkTreeIndice::createIndex(Session *session, const IndexDef *def, u8 indexId) {
	MIndex *mIndex = MIndex::open(m_db, session, this, m_tableDef, def, indexId, m_doubleChecker);
	m_indice[m_indexNum++] = mIndex;
	return mIndex;
}

/**
 * @see MIndice#dropIndex
 */
void BLinkTreeIndice::dropIndex(Session *session, uint idxNo) {
	m_indice[idxNo]->close(session);
	delete m_indice[idxNo];

	// ��������ɾ��
	memmove(&m_indice[idxNo], &m_indice[idxNo + 1], (m_indexNum - idxNo - 1) * sizeof(MIndex*));
	m_indexNum--;
}

/**
 * @see MIndice#deleteIndexEntries
 */
void BLinkTreeIndice::deleteIndexEntries(Session *session, const Record *record, RowIdVersion version) {
	for (uint i = 0; i < m_indexNum; i++) {
		McSavepoint mcSavepoint(session->getMemoryContext());
		McSavepoint lobSavepoint(session->getLobContext());
		IndexDef *indexDef = (*m_tableDef)->getIndexDef(i);

		Array<LobPair*> lobArray;
		if (indexDef->hasLob()) {
			RecordOper::extractLobFromR(session, *m_tableDef, indexDef, m_lobStorage, record, &lobArray);
		}
		// ����¼ת��ΪPAD��ʽ���������������������
		SubRecord *key = IndexKey::allocSubRecord(session->getMemoryContext(), false, record, &lobArray, *m_tableDef, indexDef);

		NTSE_ASSERT(m_indice[i]->delByMark(session, key, &version));
	}
}


/**
 * @see MIndice#insertIndexEntries
 */
bool BLinkTreeIndice::insertIndexEntries(Session *session, SubRecord *after, RowIdVersion version) {
	assert(after->m_format == REC_REDUNDANT);

	for (uint i = 0; i < m_indexNum; i++) {
		McSavepoint mcSavepoint(session->getMemoryContext());
		McSavepoint lobSavepoint(session->getLobContext());
		
		IndexDef *indexDef = (*m_tableDef)->getIndexDef(i);
		Array<LobPair*> lobArray;
		if (indexDef->hasLob()) {
			RecordOper::extractLobFromR(session, *m_tableDef, indexDef, m_lobStorage, after, &lobArray);
		}
		// ����¼ת��ΪPAD��ʽ���������������������
		SubRecord *key = MIndexKeyOper::convertKeyRP(session->getMemoryContext(), after, &lobArray, *m_tableDef, indexDef);
		m_indice[i]->insert(session, key, version);
	}
	return true;
}

/**
 * @see MIndice#updateIndexEntries
 */
bool BLinkTreeIndice::updateIndexEntries(Session *session, const SubRecord *before, SubRecord *after) {
	assert(before->m_format == REC_REDUNDANT);
	assert(after->m_format == REC_REDUNDANT);

	u16		*updateIndexNoArray;
	u16		updateIndexNum;			
	getUpdateIndices(session->getMemoryContext(), after, &updateIndexNum, &updateIndexNoArray);
	for (uint i = 0; i < updateIndexNum; i++) {
		McSavepoint mcSavepoint(session->getMemoryContext());
		McSavepoint lobSavepoint(session->getLobContext());
		IndexDef *indexDef = (*m_tableDef)->getIndexDef(updateIndexNoArray[i]);

		Array<LobPair*> lobArray1, lobArray2;
		if (indexDef->hasLob()) {
			RecordOper::extractLobFromR(session, *m_tableDef, indexDef, m_lobStorage, before, &lobArray1);
			RecordOper::extractLobFromR(session, *m_tableDef, indexDef, m_lobStorage, after, &lobArray2);
		}
		// ����¼ת��ΪPAD��ʽ���������������������
		SubRecord *keyBefore = MIndexKeyOper::convertKeyRP(session->getMemoryContext(), before, &lobArray1, *m_tableDef, indexDef);
		SubRecord *keyAfter = MIndexKeyOper::convertKeyRP(session->getMemoryContext(), after, &lobArray2, *m_tableDef, indexDef);

		RowIdVersion version = INVALID_VERSION;
		NTSE_ASSERT(m_indice[updateIndexNoArray[i]]->delByMark(session, keyBefore, &version));
		m_indice[updateIndexNoArray[i]]->insert(session, keyAfter, version);
	}
	return true;
}

void BLinkTreeIndice::setTableDef(TableDef **tableDef) {
	assert(!*tableDef || (*tableDef)->m_numIndice == m_indexNum);
	m_tableDef = tableDef;
	for (uint i = 0; i < m_indexNum; i++) {
		assert(NULL != m_indice[i]);
		m_indice[i]->setTableDef(tableDef);
		m_indice[i]->setIndexDef(*tableDef ? (*tableDef)->getIndexDef(i) : NULL);
	}
}

void BLinkTreeIndice::setLobStorage(LobStorage *lobStorage) {
	m_lobStorage = lobStorage;
}

/**
 * �Ե�һ��ɾ�������Ļع�
 * @param session			�Ự
 * @param record			��һ��ɾ��Ϊɾ���ļ�¼����һ�θ���Ϊ���µĺ���
 */
void BLinkTreeIndice::undoFirstUpdateOrDeleteIndexEntries(Session *session, const Record *record) {
	for (uint i = 0; i < m_indexNum; i++) {
		McSavepoint mcSavepoint(session->getMemoryContext());
		McSavepoint lobSavepoint(session->getLobContext());
		IndexDef *indexDef = (*m_tableDef)->getIndexDef(i);
		Array<LobPair*> lobArray;
		if (indexDef->hasLob()) {
			RecordOper::extractLobFromR(session, *m_tableDef, indexDef, m_lobStorage, record, &lobArray);
		}
		// ����¼ת��ΪPAD��ʽ���������������������
		SubRecord *key = IndexKey::allocSubRecord(session->getMemoryContext(), false, record, &lobArray, *m_tableDef, indexDef);
		RowIdVersion version = INVALID_VERSION;
		m_indice[i]->delRec(session, key, &version);
	}
}

/**
 * �Եڶ��θ��²����Ļع�
 * @param session		
 * @param before			���µ�ǰ��
 * @param after				���µĺ���
 */
bool BLinkTreeIndice::undoSecondUpdateIndexEntries(Session *session, const SubRecord *before, SubRecord *after) {
	assert(before->m_format == REC_REDUNDANT);
	assert(after->m_format == REC_REDUNDANT);

	u16		*updateIndexNoArray;
	u16		updateIndexNum;			
	getUpdateIndices(session->getMemoryContext(), after, &updateIndexNum, &updateIndexNoArray);
	for (uint i = 0;i < updateIndexNum; i++) {
		McSavepoint mcSavepoint(session->getMemoryContext());
		McSavepoint lobSavepoint(session->getLobContext());
		IndexDef *indexDef = (*m_tableDef)->getIndexDef(updateIndexNoArray[i]);

		Array<LobPair*> lobArray1, lobArray2;
		if (indexDef->hasLob()) {
			RecordOper::extractLobFromR(session, *m_tableDef, indexDef, m_lobStorage, before, &lobArray1);
			RecordOper::extractLobFromR(session, *m_tableDef, indexDef, m_lobStorage, after, &lobArray2);
		}
		// ����¼ת��ΪPAD��ʽ���������������������
		SubRecord *keyBefore = MIndexKeyOper::convertKeyRP(session->getMemoryContext(), before, &lobArray1, *m_tableDef, indexDef);
		SubRecord *keyAfter = MIndexKeyOper::convertKeyRP(session->getMemoryContext(), after, &lobArray2, *m_tableDef, indexDef);

		RowIdVersion version = INVALID_VERSION;
		m_indice[updateIndexNoArray[i]]->delRec(session, keyAfter, &version);
		NTSE_ASSERT(m_indice[updateIndexNoArray[i]]->undoDelByMark(session, keyBefore, version));
	}
	return true;
}

/**
 * �Եڶ���ɾ�������Ļع�
 * @param session			�Ự
 * @param record			ɾ���ļ�¼
 */
void BLinkTreeIndice::undoSecondDeleteIndexEntries(Session *session, const Record *record) {
	for (uint i = 0; i < m_indexNum; i++) {
		McSavepoint mcSavepoint(session->getMemoryContext());
		McSavepoint lobSavepoint(session->getLobContext());

		IndexDef *indexDef = (*m_tableDef)->getIndexDef(i);
		Array<LobPair*> lobArray;
		if (indexDef->hasLob()) {
			RecordOper::extractLobFromR(session, *m_tableDef, indexDef, m_lobStorage, record, &lobArray);
		}
		// ����¼ת��ΪPAD��ʽ���������������������
		SubRecord *key = IndexKey::allocSubRecord(session->getMemoryContext(), false, record, &lobArray, *m_tableDef, indexDef);
		NTSE_ASSERT(m_indice[i]->undoDelByMark(session, key, INVALID_VERSION));
	}
}

/**
 * ����Ҫ���µĺ�����㵱ǰ����Щ������Ҫ����
 * @param memoryContext		�ڴ���������ģ�����������µ�����������飬������ע��ռ��ʹ�ú��ͷ�
 * @param update			���²����ĺ�����Ϣ�������ֶ���������
 * @param updateNum			out �����ж��ٸ�������Ҫ������
 * @param updateIndices		out ָ��memoryContext����Ŀռ䣬������Ҫ���µ�������ţ�����ź������ڲ��������к�һ��
 */
void BLinkTreeIndice::getUpdateIndices( MemoryContext *memoryContext, const SubRecord *update, u16 *updateNum, u16 **updateIndices ) {
	u16 *toBeUpdated = (u16*)memoryContext->alloc(sizeof(update->m_columns[0]) * Limits::MAX_INDEX_NUM);
	u16 updates = 0;

	for (u16 indexNo = 0; indexNo < (*m_tableDef)->m_numIndice; indexNo++) {
		IndexDef *indexDef = (*m_tableDef)->m_indice[indexNo];
		for (u16 j = 0; j < indexDef->m_numCols; j++) {
			// ����ĸ����ֶΣ����Զ��ֲ���
			if (std::binary_search(update->m_columns, update->m_columns + update->m_numCols, indexDef->m_columns[j])) {
				toBeUpdated[updates++] = indexNo;
				break;
			}
		}
	}

	*updateNum = updates;
	*updateIndices = toBeUpdated;
}

}