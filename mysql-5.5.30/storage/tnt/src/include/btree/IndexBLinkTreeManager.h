/**
* TNT�ڴ���������
*
* @author ��ΰ��(liweizhao@corp.netease.com)
*/
#ifndef _TNT_INDEX_BLINK_TREE_MANAGER_
#define _TNT_INDEX_BLINK_TREE_MANAGER_

#include "misc/Session.h"
#include "misc/Record.h"
#include "misc/DoubleChecker.h"
#include "misc/TNTIMPageManager.h"
#include "btree/MIndex.h"
#include "btree/MIndexPage.h"
#include "api/TNTDatabase.h"

using namespace ntse;

namespace tnt {

class TNTDatabase;

class BLinkTreeIndice : public MIndice {
public:
	BLinkTreeIndice(TNTDatabase *db, Session *session, uint indexNum, TableDef **tableDef, LobStorage *lobStorage,
		const DoubleChecker *doubleChecker);
	virtual ~BLinkTreeIndice();
	bool init(Session *session);
	void close(Session *session);
	MIndex* createIndex(Session *session, const IndexDef *def, u8 indexId);
	void dropIndex(Session *session, uint idxNo);
	
	// ���������ӿ�
	void deleteIndexEntries(Session *session, const Record *record, RowIdVersion version);
	bool updateIndexEntries(Session *session, const SubRecord *before, SubRecord *after);
	bool insertIndexEntries(Session *session, SubRecord *after, RowIdVersion version);

	void undoFirstUpdateOrDeleteIndexEntries(Session *session, const Record *record);
	bool undoSecondUpdateIndexEntries(Session *session, const SubRecord *before, SubRecord *after);
	void undoSecondDeleteIndexEntries(Session *session, const Record *record);

	void setTableDef(TableDef **tableDef);

	void getUpdateIndices( MemoryContext *memoryContext, const SubRecord *update, u16 *updateNum, u16 **updateIndices);
	void setLobStorage(LobStorage *lobStorage);
	/**
	 * ����ڴ�������Ŀ
	 * @return 
	 */
	uint getIndexNum() const {
		return m_indexNum;
	}

	/**
	 * ���������Ż�ȡָ�����ڴ�����
	 * @param index Ҫ��ȡ��������������
	 * @return 
	 */
	MIndex* getIndex(uint index) const {
		assert(index < Limits::MAX_INDEX_NUM && index < (*m_tableDef)->m_numIndice);
		assert(m_indice[index] != NULL);
		return m_indice[index];
	}

	u64 getMemUsed(bool includeMeta = true) {
		//TODO: �����ڴ�ʹ����
		UNREFERENCED_PARAMETER(includeMeta);
		return 0;
	}

	void setDoubleChecker(const DoubleChecker *doubleChecker) {
		m_doubleChecker = doubleChecker;
	}

private:
	TNTDatabase			*m_db;				/** ���ݿ� */
	TableDef			**m_tableDef;		/** ������Ķ��� */
	LobStorage			*m_lobStorage;		/** ���������� */
	u16                 m_indexNum;
	MIndex			    **m_indice;			/** �����ڴ����� */
	TNTIMPageManager    *m_pageManager;     /** ������䡢���ո����ڴ�����ҳ�� */
	const DoubleChecker *m_doubleChecker;

	//Mutex				*m_mutex;			/** ͬ�������޸Ĳ����Ļ����� */
	//DBObjStats		*m_dboStats;		/** ����ҳ�����ͼ���ݶ���״̬�ṹ */
	//OrderedIndices	m_orderedIndices;	/** �ڴ浱��ά���������������� */
};

}

#endif