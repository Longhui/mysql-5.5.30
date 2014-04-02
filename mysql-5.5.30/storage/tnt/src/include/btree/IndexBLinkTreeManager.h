/**
* TNT内存索引管理
*
* @author 李伟钊(liweizhao@corp.netease.com)
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
	
	// 索引操作接口
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
	 * 获得内存索引数目
	 * @return 
	 */
	uint getIndexNum() const {
		return m_indexNum;
	}

	/**
	 * 根据索引号获取指定的内存索引
	 * @param index 要获取的索引的索引号
	 * @return 
	 */
	MIndex* getIndex(uint index) const {
		assert(index < Limits::MAX_INDEX_NUM && index < (*m_tableDef)->m_numIndice);
		assert(m_indice[index] != NULL);
		return m_indice[index];
	}

	u64 getMemUsed(bool includeMeta = true) {
		//TODO: 计算内存使用量
		UNREFERENCED_PARAMETER(includeMeta);
		return 0;
	}

	void setDoubleChecker(const DoubleChecker *doubleChecker) {
		m_doubleChecker = doubleChecker;
	}

private:
	TNTDatabase			*m_db;				/** 数据库 */
	TableDef			**m_tableDef;		/** 所属表的定义 */
	LobStorage			*m_lobStorage;		/** 大对象管理器 */
	u16                 m_indexNum;
	MIndex			    **m_indice;			/** 各个内存索引 */
	TNTIMPageManager    *m_pageManager;     /** 管理分配、回收各个内存索引页面 */
	const DoubleChecker *m_doubleChecker;

	//Mutex				*m_mutex;			/** 同步索引修改操作的互斥量 */
	//DBObjStats		*m_dboStats;		/** 索引页面分配图数据对象状态结构 */
	//OrderedIndices	m_orderedIndices;	/** 内存当中维护的有序索引队列 */
};

}

#endif