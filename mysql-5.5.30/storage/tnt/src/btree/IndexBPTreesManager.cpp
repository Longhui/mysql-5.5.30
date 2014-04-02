/**
 * NTSE B+树索引管理类
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
 * Indice类初始化构造函数
 * @param db			数据库对象
 * @param tableDef		表定义
 * @param file			文件句柄，该项在外层分配，但是需要在析构函数释放
 * @param lobStorage	大对象管理器
 * @param headerPage	头页面
 * @param dbObjStats	数据库统计对象
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
 * 插入一条记录时插入各索引项到表中的所有索引中。
 * 如果发生了唯一性冲突，索引模块应自动回滚已经插入的索引项。
 *
 * @param session		会话句柄
 * @param record		新插入的记录内容，包括RID
 * @param dupIndex		OUT	如果插入冲突，给出冲突索引号
 * @return 是否成功，true表示成功，false表示唯一性冲突
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
		// 使用m_indexSeq数组保证先更新唯一索引
		u16 realInsertIndexNo = m_orderedIndices.getOrder(i);
		DrsBPTreeIndex *index = (DrsBPTreeIndex*)m_indice[realInsertIndexNo];
		const IndexDef *indexDef = index->getIndexDef();
		bool keyNeedCompress = RecordOper::isFastCCComparable(m_tableDef, indexDef, indexDef->m_numCols, indexDef->m_columns);
		// 拼装索引键
		Array<LobPair*> lobArray;
		if (indexDef->hasLob()) {
			RecordOper::extractLobFromR(session, m_tableDef, indexDef, m_lobStorage, record, &lobArray);
		}

		SubRecord *key = IndexKey::allocSubRecord(memoryContext, keyNeedCompress, record, &lobArray, m_tableDef, indexDef);


		u64 token = session->getToken();
		bool success;
		while ((success = index->insert(session, key, &duplicateKey)) == false && !duplicateKey) {
			nftrace(ts.irl, tout << session->getId() << " I dl and redo");
			session->unlockIdxObjects(token);	// 对于出现死锁的情况，需要放锁再插入
			index->statisticDL();
		}
		NTSE_ASSERT(!success || !duplicateKey);

		// 违反唯一性约束，回退所有插入
		if (duplicateKey) {
			nftrace(ts.irl, tout << session->getId() << " I dk and rb all");

			*dupIndex = realInsertIndexNo;

			for (u8 j = 0; j < i; j++) {
				u16 realUndoIdxNo = m_orderedIndices.getOrder(j);
				const IndexDef *indexDef = ((DrsBPTreeIndex*)m_indice[realUndoIdxNo])->getIndexDef();
				bool keyNeedCompress = RecordOper::isFastCCComparable(m_tableDef, indexDef, indexDef->m_numCols, indexDef->m_columns);
				// 拼装索引键			
				if (indexDef->m_prefix) {
					RecordOper::extractLobFromR(session, m_tableDef, indexDef, m_lobStorage, record, &lobArray);
				}
				SubRecord *key = IndexKey::allocSubRecord(memoryContext, keyNeedCompress, record, &lobArray, m_tableDef, indexDef);
				index = (DrsBPTreeIndex*)m_indice[realUndoIdxNo];

				// 这里的回退操作可能会导致SMO，但是不应该会有死锁，理由同updateIndexEntries里面的说明
				NTSE_ASSERT(index->del(session, key));
				index->statisticOp(IDX_INSERT, -1);
			}

			result = false;
			//ftrace(ts.idx, tout.setAutoComma(true) << "I: " << *dupIndex << "Record id: " << record->m_rowId;);
			session->setTxnDurableLsn(session->getLastLsn());		// 唯一性索引插入阶段回退，需要设置可持久化LSN
			session->unlockIdxAllObjects();
			break;
		}

		index->statisticOp(IDX_INSERT, 1);
		m_logger->logDMLDoneUpdateIdxNo(session, i);

		// 如果所有唯一索引插入完毕，设置可持久化LSN
		if (i + 1 == m_orderedIndices.getUniqueIdxNum())
			session->setTxnDurableLsn(session->getLastLsn());
		// 最后一个唯一索引以及所有非唯一索引的更新结束之后可以立即放锁
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
 * 插入一条记录时插入各索引项到表中的所有索引中。
 * @pre 已经检查过唯一性，一定不会发生唯一性冲突
 *
 * @param session		会话句柄
 * @param record		新插入的记录内容，包括RID
 */
void DrsBPTreeIndice::insertIndexNoDupEntries(Session *session, const Record *record) {
		assert(REC_REDUNDANT == record->m_format);
	uint indexNum = getIndexNum();
	uint uniqueIndexNum = getUniqueIndexNum();
	MemoryContext *mtx = session->getMemoryContext();
	McSavepoint mcSavepoint(mtx);
	McSavepoint lobSavepoint(session->getLobContext());
	//在外存索引中插入索引键值
	
	u64 opLSN = m_logger->logDMLUpdateBegin(session);
	for (u16 i = 0; i < indexNum; i++) {
		u8 idxNo = (u8)m_orderedIndices.getOrder(i);
		DrsBPTreeIndex *drsIndex = (DrsBPTreeIndex*)m_indice[idxNo];

		const IndexDef *indexDef = m_tableDef->getIndexDef(idxNo);
		bool keyNeedCompress = RecordOper::isFastCCComparable(m_tableDef, indexDef, indexDef->m_numCols, 
			indexDef->m_columns);

		// 拼装索引键
		Array<LobPair*> lobArray;
		if (indexDef->hasLob()) {
			RecordOper::extractLobFromR(session, m_tableDef, indexDef, m_lobStorage, record, &lobArray);
		}

		SubRecord *key = IndexKey::allocSubRecord(mtx, keyNeedCompress, record, &lobArray, m_tableDef, indexDef);
		drsIndex->insertNoCheckDuplicate(session, key);

		drsIndex->statisticOp(IDX_INSERT, 1);

		m_logger->logDMLDoneUpdateIdxNo(session, (u8)i);

		// 如果所有唯一索引插入完毕，设置可持久化LSN
		if ((uint)i + 1 == uniqueIndexNum)
			session->setTxnDurableLsn(session->getLastLsn());

		// 因为必不可能发生唯一性索引冲突，所以每个索引的更新结束之后可以立即放锁
		session->unlockIdxAllObjects();
	}
	m_logger->logDMLUpdateEnd(session, opLSN, true);
	NTSE_ASSERT(!session->hasLocks());
}



#endif


/**
 * 删除一条记录时从所有索引中删除记录对应的索引项
 *
 * @param session		会话句柄
 * @param record		待删除的记录内容，包括RID，一定是REC_REDUNDANT格式
 * @param scanHandle	扫描句柄，如果不为NULL说明该删除是建立在某个索引扫描基础上的，可以使用deleteCurrent修改该索引，否则不行
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

	for (u8 i = 0; i < m_indexNum; i++) {	// 逐一对每个索引进行删除
		McSavepoint lobSavepoint(session->getLobContext());

		DrsBPTreeIndex *index = (DrsBPTreeIndex*)m_indice[i];
		const IndexDef *indexDef = index->getIndexDef();

		if (handle != NULL && handle->getScanInfo()->m_indexDef == indexDef) {	// 可以使用deleteCurrent删除
			while (!index->deleteCurrent(scanHandle)) {
				nftrace(ts.irl, tout << session->getId() << " D dl and redo");
				session->unlockIdxAllObjects();
				index->statisticDL();
			}
			index->statisticOp(IDX_DELETE, 1);
		} else {
			bool keyNeedCompress = RecordOper::isFastCCComparable(m_tableDef, indexDef, indexDef->m_numCols, indexDef->m_columns);
			// 拼装索引键
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
		// 释放本次操作可能持有的资源锁
		NTSE_ASSERT(session->unlockIdxAllObjects());
	}

	memoryContext->resetToSavepoint(savePoint);

	m_logger->logDMLUpdateEnd(session, opLSN, true);

	NTSE_ASSERT(!session->hasLocks());
}


/**
 * 更新一条记录时更新所有索引项
 * 如果发生了唯一性冲突，索引模块应自动回滚已经进行的操作，
 * 保证索引数据的一致性
 *
 * @param session		会话句柄
 * @param before		至少包含所有更新涉及到的索引的所有属性的前像，一定是REC_REDUNDANT格式
 * @param after			包含所有真正需要更新的索引属性的后像，一定是REC_REDUNDANT格式
 * @param updateLob		是否更新大对象，对于NTSE的update，此项为true， 而TNT purge调用时，此项为false
 * @param dupIndex		OUT	如果更新冲突，给出冲突索引号
 * @return 是否成功，true表示插入成功，false表示有唯一性冲突
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

	// 构造before和after的record格式
	RecordOper::mergeSubRecordRR(m_tableDef, after, before);
	Record rec1(before->m_rowId, REC_REDUNDANT, before->m_data, before->m_size);
	Record rec2(after->m_rowId, REC_REDUNDANT, after->m_data, after->m_size);

#ifdef NTSE_UNIT_TEST
	// 单元测试的时候，可能存在绕过索引定义，修改m_indexNum计数的情况，单独处理
	if (updates > m_indexNum)
		updates = m_indexNum;
#endif

	for (u8 i = 0; i < updates; i++) {	// 逐一对每个索引进行更新
		McSavepoint lobSavepoint(session->getLobContext());
		u64 token = (u64)-1;
		u16 realUpdateIdxNo = updateIndicesNo[i];
		DrsBPTreeIndex *index = (DrsBPTreeIndex*)m_indice[realUpdateIdxNo];
		const IndexDef *indexDef = index->getIndexDef();

		bool keyNeedCompress = RecordOper::isFastCCComparable(m_tableDef, indexDef, indexDef->m_numCols, indexDef->m_columns);
		bool success;
		// 拼装索引键，大对象获取的方法取决于是非事务更新还是TNT purge update
		Array<LobPair*> lobArray1, lobArray2;
		if (indexDef->hasLob()) {
			// 前项是Redundant格式记录
			RecordOper::extractLobFromR(session, m_tableDef, indexDef, m_lobStorage, &rec1, &lobArray1);

			if (!updateLob) {
				// TNT purge 操作，不会更新大对象，后项的大对象已经存在于大对象管理器中
				RecordOper::extractLobFromR(session, m_tableDef, indexDef, m_lobStorage, &rec2, &lobArray2);
			} else {
				// NTSE 更新，后项为REDUNANT和MYSQL混合格式
				RecordOper::extractLobFromMixedMR(session, m_tableDef, indexDef, m_lobStorage, &rec2, after->m_numCols, after->m_columns, &lobArray2);
			}
		}
		SubRecord *key1 = IndexKey::allocSubRecord(memoryContext, keyNeedCompress, const_cast<Record*>(&rec1), &lobArray1, m_tableDef, indexDef);
		SubRecord *key2 = IndexKey::allocSubRecord(memoryContext, keyNeedCompress, const_cast<Record*>(&rec2), &lobArray2, m_tableDef, indexDef);

		// update操作先删除后插入，可能会死锁，这个时候要回退对当前索引的操作，放锁再继续尝试更新本索引
		while (true) {
			token = session->getToken();
			while (!index->del(session, key1)) {
				nftrace(ts.irl, tout << session->getId() << " D in U dl and redo");
				session->unlockIdxObjects(token);
				index->statisticDL();
			}

			if (!indexDef->m_unique) {
				// 如果是非唯一索引，可以提前放掉删除操作的锁，因为插入操作不会回退
				// 同时还需要保证先写日志标记当前的删除成功
				m_logger->logDMLDeleteInUpdateDone(session);
				NTSE_ASSERT(session->unlockIdxAllObjects());

				// 这里的插入即使死锁回退，也不应该回退删除操作
				while ((success = index->insert(session, key2, &duplicateKey)) == false) {
					nftrace(ts.irl, tout << session->getId() << " I in U dl and redo");
					session->unlockIdxAllObjects();	// 对于出现死锁的情况，需要放锁再插入
					index->statisticDL();
				}
			} else {
				success = index->insert(session, key2, &duplicateKey);
				if (!success && !duplicateKey) {
					nftrace(ts.irl, tout << session->getId() << "I in U dl and redo");
					// 死锁导致失败，需要回退删除操作放锁，重新更新
					NTSE_ASSERT(index->insert(session, key1, &duplicateKey, false));
					session->unlockIdxObjects(token);
					index->statisticDL();
					continue;
				}
			}

			break;
		}

		// 处理由于唯一性冲突导致的更新失败，全部操作都要回退
		// TNT purge update一定不会有唯一性冲突
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
					// 由于回滚操作只有NTSE操作，因此大对象一定是在mysql格式记录中
					RecordOper::extractLobFromMixedMR(session, m_tableDef, indexDef, m_lobStorage, &rec2, 
						after->m_numCols, after->m_columns, &lobArray2);
				}

				SubRecord *key1 = IndexKey::allocSubRecord(memoryContext, keyNeedCompress, const_cast<Record*>(&rec2), &lobArray1, m_tableDef, indexDef);
				SubRecord *key2 = IndexKey::allocSubRecord(memoryContext, keyNeedCompress, const_cast<Record*>(&rec1), &lobArray2, m_tableDef, indexDef);

				/**
				 * 现在的更新回退可能需要加SMO锁，但是不应该导致SMO锁和页面锁之间的死锁，因为：
				 * 现在更新回退修改的页面肯定是正常操作时候修改过的页面，如果那些页面在正常操作的时候
				 * 不需要SMO，那么现在的回退操作可以在本页面完成，不会导致SMO产生，
				 * 否则如果本来就SMO过，那么已经持有SMO锁，不可能死锁
				 * 总之在这里的回退的过程中，不会申请已经持有资源以外的任何资源
			     */
				NTSE_ASSERT(index->del(session, key1));
				NTSE_ASSERT(index->insert(session, key2, &duplicateKey, false));

				index->statisticOp(IDX_UPDATE, -1);
			}

			session->setTxnDurableLsn(session->getLastLsn());		// 唯一性索引更新阶段回退，需要设置可持久化LSN
			NTSE_ASSERT(session->unlockIdxAllObjects());
			memoryContext->resetToSavepoint(savePoint);
			result = false;

			break;
		}

		index->statisticOp(IDX_UPDATE, 1);

		m_logger->logDMLDoneUpdateIdxNo(session, i);

		// 如果所有唯一索引插入完毕，设置可持久化LSN
		if (i + 1 == updateUniques)
			session->setTxnDurableLsn(session->getLastLsn());
		// 最后一个唯一索引以及所有非唯一索引的更新结束之后可以立即放锁
		if (i + 1 >= updateUniques)
			session->unlockIdxAllObjects();
	}

	memoryContext->resetToSavepoint(savePoint);

	m_logger->logDMLUpdateEnd(session, opLSN, result);

	vecode(vs.idx, NTSE_ASSERT(!session->hasLocks()));

	return result;
}


/**
 * 上层恢复时候调用的补充写DML操作结束接口
 * @param session	会话句柄
 * @param beginLSN	该DML对应的开始日志的LSN
 * @param succ		表示该DML操作是成功还是需要被回滚，这里默认是false，表示是被回滚的
 */
void DrsBPTreeIndice::logDMLDoneInRecv(Session *session, u64 beginLSN, bool succ) {
	m_logger->logDMLUpdateEnd(session, beginLSN, succ);
}


/**
 * 恢复过程使用的补充插入操作接口
 * @param session		会话句柄
 * @param record		要插入的记录，需要是REC_REDUNDANT格式的记录
 * @param lastDoneIdxNo	从哪个索引之后开始执行插入，这里的顺序是指内存当中有序索引序列的下标，日志中有记录
 * @param beginLSN		补充操作一开始记录的开始操作的LSN
 */
void DrsBPTreeIndice::recvInsertIndexEntries(Session *session, const Record *record, s16 lastDoneIdxNo, u64 beginLSN) {
	ftrace(ts.idx, tout << record << lastDoneIdxNo << beginLSN);

	// 如果没有任何索引被插入成功，但是所有索引都是非唯一索引，那么lastDoneIdxNo==-1，加1后刚好从0开始
	// 下面的删除，更新操作和这里的原理一样
	assert(lastDoneIdxNo + 1 >= m_orderedIndices.getUniqueIdxNum());	// 当前补充插入的索引肯定都是非唯一索引

	bool duplicateKey;
	MemoryContext *memoryContext = session->getMemoryContext();
	u64 savePoint = memoryContext->setSavepoint();

	for (u8 i = (u8)(lastDoneIdxNo + 1); i < m_indexNum; i++) {
		McSavepoint lobSavepoint(session->getLobContext());
		u16 realInsertIndexNo = m_orderedIndices.getOrder(i);
		DrsBPTreeIndex *index = (DrsBPTreeIndex*)m_indice[realInsertIndexNo];
		const IndexDef *indexDef = index->getIndexDef();
		bool keyNeedCompress = RecordOper::isFastCCComparable(m_tableDef, indexDef, indexDef->m_numCols, indexDef->m_columns);
		// 拼装索引键
		Array<LobPair*> lobArray;
		if (indexDef->hasLob()) {
			RecordOper::extractLobFromR(session, m_tableDef, indexDef, m_lobStorage, record, &lobArray);
		}

		SubRecord *key = IndexKey::allocSubRecord(memoryContext, keyNeedCompress, record, &lobArray, m_tableDef, indexDef);

		// 这里执行的插入肯定都会成功，不会违反唯一性约束，并且单线程不会死锁
		NTSE_ASSERT(index->insert(session, key, &duplicateKey));

		// 可以及时释放锁资源
		index->statisticOp(IDX_INSERT, 1);
		m_logger->logDMLDoneUpdateIdxNo(session, i);
		session->unlockIdxAllObjects();
	}

	memoryContext->resetToSavepoint(savePoint);
	m_logger->logDMLUpdateEnd(session, beginLSN, true);
}

/**
 * 恢复过程使用的补充删除操作接口
 * @param session		会话句柄
 * @param record		要删除的记录，需要是REC_REDUNDANT格式的记录
 * @param lastDoneIdxNo	从哪个索引之后开始执行删除，这里的顺序是指内存当中有序索引序列的下标，日志中有记录
 * @param beginLSN		补充操作一开始记录的开始操作的LSN
 */
void DrsBPTreeIndice::recvDeleteIndexEntries(Session *session, const Record *record, s16 lastDoneIdxNo, u64 beginLSN) {
	ftrace(ts.idx, tout << record << lastDoneIdxNo << beginLSN);

	MemoryContext *memoryContext = session->getMemoryContext();
	u64 savePoint = memoryContext->setSavepoint();

	for (u8 i = (u8)(lastDoneIdxNo + 1); i < m_indexNum; i++) {	// 逐一对每个索引进行删除
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

		// 释放本次操作可能持有的资源锁
		NTSE_ASSERT(session->unlockIdxAllObjects());
	}

	memoryContext->resetToSavepoint(savePoint);

	m_logger->logDMLUpdateEnd(session, beginLSN, true);
}


/**
 * 恢复过程使用的补充更新操作接口
 * @param session		会话句柄
 * @param before		包括所有更新属性的前项，需要是REC_REDUNDANT格式的记录
 * @Param after			包括所有更新属性的后项，需要是REC_REDUNDANT格式的记录
 * @param lastDoneIdxNo	从哪个索引之后开始执行插入，这里的顺序是指内存当中有序索引序列的下标，日志中有记录
 * @param beginLSN		补充操作一开始记录的开始操作的LSN
 * @param isUpdateLob   判断是ntse的更新还是TNT的purge update，与索引键提取大对象的大对象源有关
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

	// 构造before和after的record格式
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
	for (u8 i = (u8)(lastDoneIdxNo + 1); i < updates; i++) {	// 逐一对每个索引进行更新
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

		// update操作先删除后插入，这时候首先单线程不会死锁，其次不会产生唯一键值冲突
		NTSE_ASSERT(index->del(session, key1));
		// 恢复过程的记录日志方式应该保证和正常流程一致
		m_logger->logDMLDeleteInUpdateDone(session);
		NTSE_ASSERT(index->insert(session, key2, &duplicateKey));
		index->statisticOp(IDX_UPDATE, 1);
		m_logger->logDMLDoneUpdateIdxNo(session, i);
		// 及时释放锁资源
		NTSE_ASSERT(session->unlockIdxAllObjects());
	}

	memoryContext->resetToSavepoint(savePoint);

	m_logger->logDMLUpdateEnd(session, beginLSN, true);
}


/**
 * 对指定的索引，进行更新过程中的插入，同时记录改索引更新操作成功日志
 * 该过程专门为更新过程中的插入使用
 * @param session		会话句柄
 * @param record		要插入的记录
 * @param lastDoneIdxNo	对这个索引之后的索引执行插入，这里的顺序是指内存当中有序索引序列的下标，日志中有记录
 * @param updateIdxNum	该更新操作会涉及到的索引个数
 * @param updateIndices	该更新操作更新的索引序号数组，该数组现在是通过索引模块计算得出，此时各个索引序号已经按照索引内部有序规则排列
 * @param isUpdateLob   判断是ntse的更新还是TNT的purge update，与索引键提取大对象的大对象源有关
 * @return 返回当前执行插入的索引，也就是更新完全的最后一个索引序号，这里的序号含义和lastDoneIdxNo是相同的
 */
u8 DrsBPTreeIndice::recvCompleteHalfUpdate(Session *session, const SubRecord *record, s16 lastDoneIdxNo, u16 updateIdxNum, u16 *updateIndices, bool isUpdateLob) {
	ftrace(ts.idx, tout << record << lastDoneIdxNo);

	if (lastDoneIdxNo == -1) {
		// 这个情况说明，更新操作只涉及了唯一索引
		// 并且第一个需要更新的唯一索引的更新还未完成
		// 此时需要通过updateIndices来计算是哪个索引需要被执行补充插入
		// 实际取的是有序数组当中第一个需要更新的非唯一索引
		u16 i;
		for (i = 0; i < updateIdxNum; i++)
			if (!m_tableDef->m_indice[updateIndices[i]]->m_unique)
				break;
		assert(i < updateIdxNum);	// 必定能找到一个非唯一索引
		lastDoneIdxNo = m_orderedIndices.find(updateIndices[i]) - 1;
		assert(lastDoneIdxNo != -2);	// 不可能找不到
	}

	u8 insertIdxNo = (u8)(lastDoneIdxNo + 1);
	assert(insertIdxNo >= m_orderedIndices.getUniqueIdxNum());	// 当前补充插入的索引肯定都是非唯一索引

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

	// 这里执行的插入肯定都会成功，不会违反唯一性约束，并且单线程不会死锁
	NTSE_ASSERT(index->insert(session, key, &duplicateKey));

	// 可以及时释放锁资源
	index->statisticOp(IDX_UPDATE, 1);
	m_logger->logDMLDoneUpdateIdxNo(session, insertIdxNo);
	session->unlockIdxAllObjects();

	memoryContext->resetToSavepoint(savePoint);

	return insertIdxNo;
}



/**
 * 创建一个索引第一阶段，在物理文件当中创建写入索引信息
 * @post 如果成功，之后必须调用createIndexPhaseTwo更改内存索引映象
 * @param session	会话句柄
 * @param def		索引定义
 * @param heap		DRS堆
 * @throw NtseException IO异常，唯一性异常，创建过多索引等
 */
void DrsBPTreeIndice::createIndexPhaseOne(Session *session, const IndexDef *indexDef, const TableDef *tblDef, DrsHeap *heap) throw(NtseException) {
	ftrace(ts.idx, tout << indexDef);

	m_newIndexId = createNewIndex(session, indexDef, tblDef, heap);
	if (m_newIndexId == 0)
		NTSE_THROW(NTSE_EC_EXCEED_LIMIT, "Create indices on table %s too many times!", tblDef->m_name);
}



/**
 * 创建新索引第二阶段
 * @pre	必须先调用createIndexPhaseOne才能调用本函数，因此m_newIndexId必须不可能是-1
 * @param def	索引定义
 * @return	返回创建的索引对象
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
 * 得到指定的索引
 *
 * @param index 索引编号，从0开始
 * @return 索引
 */
DrsIndex* DrsBPTreeIndice::getIndex(uint index) {
	assert(index < Limits::MAX_INDEX_NUM && index < m_tableDef->m_numIndice);
	assert(m_indice[index] != NULL);
	return m_indice[index];
}


/**
 * 返回当前该索引文件包含索引个数
 *
 * @return	索引个数
 */
uint DrsBPTreeIndice::getIndexNum() const {
	return m_indexNum;
}

/**
 * 返回唯一性索引的数目
 * @return 
 */
uint DrsBPTreeIndice::getUniqueIndexNum() const {
	return m_orderedIndices.getUniqueIdxNum();
}

/**
 * @param files [in/out] 该模块的所有File指针数组， 空间调用者分配
 * @param pageTypes [in/out] File对应的页类型
 * @param numFile files数组和pageTypes数组长度
 * @return 该模块File对象个数
 */
int DrsBPTreeIndice::getFiles(File **files, PageType *pageTypes, int numFile) {
	UNREFERENCED_PARAMETER(numFile);
	assert(numFile >= 1);
	files[0] = m_file;
	pageTypes[0] = PAGE_INDEX;

	return 1;
}


/**
 * 删除一个指定的索引第一阶段，删除内存当中的映象
 * @pre	使用者要注意同步内存
 * @post 必须调用dropPhaseTwo，完成物理文件当中索引内容的删除 
 * @param session	会话句柄
 * @param idxNo		要删除的索引编号
 */
void DrsBPTreeIndice::dropPhaseOne(Session *session, uint idxNo) {
	ftrace(ts.idx, tout << idxNo);

	UNREFERENCED_PARAMETER(session);
	assert(idxNo < Limits::MAX_INDEX_NUM && idxNo < m_tableDef->m_numIndice);
	assert(m_indice[idxNo] != NULL);
	m_droppedIndex = m_indice[idxNo];

	// 首先修改内存有序维护的索引结构
	m_orderedIndices.remove((u16)idxNo, ((DrsBPTreeIndex*)m_droppedIndex)->getIndexDef()->m_unique);
	// 从数组中删除
	memmove(&m_indice[idxNo], &m_indice[idxNo + 1], (m_indexNum - idxNo - 1) * sizeof(DrsIndex*));
	m_indexNum--;
	m_indice[m_indexNum] = NULL;
}


/**
 * 删除一个指定的索引第二阶段，删除物理文件当中索引的信息
 * @pre	必须先调用过dropPhaseOne
 * @param session	会话句柄
 * @param idxNo		删除的索引原来在表定义中的序号
 */
void DrsBPTreeIndice::dropPhaseTwo(Session *session, uint idxNo) {
	ftrace(ts.idx, tout << idxNo);

	assert(m_droppedIndex != NULL);
	u8 indexId = ((DrsBPTreeIndex*)m_droppedIndex)->getIndexId();

	// 在文件中标记删除
	m_pagesManager->discardIndexByIDXId(session, m_logger, indexId, idxNo);

	// 调用buffer清除相关页面
	Buffer *buffer = m_db->getPageBuffer();
	buffer->freePages(session, m_file, indexId, DrsBPTreeIndice::bufferCallBackSpecifiedIDX);

	delete m_droppedIndex;
	m_droppedIndex = NULL;
}


/**
 * 当且仅当在析构本对象之前调用该函数，释放本对象持有的所有资源
 *
 * @param session 会话
 * @param flushDirty 是否写出脏数据
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
 * 刷出脏数据
 * @pre 表中的写操作已经被禁用
 *
 * @param session 会话
 */
void DrsBPTreeIndice::flush(Session *session) {
	ftrace(ts.idx, tout << session->getId());
	m_db->getPageBuffer()->flushDirtyPages(session, m_file);
}

/**
 * 得到类当中文件描述符
 * @return 返回文件描述符
 */
File* DrsBPTreeIndice::getFileDesc() {
	return m_file;
}


/**
 * 得到索引对应表的唯一ID
 *
 * 返回表ID
 */
u16 DrsBPTreeIndice::getTableId() {
	return m_tableDef->m_id;
}


/**
 * 得到索引页面管理分配回收器
 *
 * @return 返回索引页面管理分配回收器
 */
IndicePageManager* DrsBPTreeIndice::getPagesManager() {
	return m_pagesManager;
}



/**
 * 得到日志管理器
 *
 * @return 返回日志管理器
 */
IndexLog* DrsBPTreeIndice::getLogger() {
	return m_logger;
}

const OrderedIndices* DrsBPTreeIndice::getOrderedIndices() const {
	return &m_orderedIndices;
}

/**
 * 创建一个新的索引
 *
 * @param session 会话句柄
 * @param def	索引定义
 * @param heap	相关堆定义
 * @return indexId ID表示成功，0表示失败
 * @throw 文件读写异常，索引唯一性冲突异常，索引无法创建异常
 */
u8 DrsBPTreeIndice::createNewIndex(Session *session, const IndexDef *indexDef, const TableDef *tblDef, DrsHeap *heap) throw(NtseException) {
	ftrace(ts.idx, tout << indexDef);

	u8 indexId, indexNo;
	PageId rootPageId;

	// 记录索引创建开始日志
	u64 opLSN = m_logger->logCreateIndexBegin(session, m_pagesManager->getFileHeaderPage()->getAvailableIndexId());

	PageHandle *rootHandle = m_pagesManager->createNewIndexRoot(m_logger, session, &indexNo, &indexId, &rootPageId);
	if (rootHandle == NULL) {
		m_logger->logCreateIndexEnd(session, indexDef, opLSN, indexId, false);
		return 0;
	}

	SYNCHERE(SP_IDX_ALLOCED_ROOT_PAGE);

	// 初始化根页面
	IndexPage *rootPage = (IndexPage*)rootHandle->getPage();
	u32 mark = IndexPage::formPageMark(indexId);
	rootPage->initPage(session, m_logger, true, false, rootPageId, mark, ROOT_AND_LEAF, 0, INVALID_PAGE_ID, INVALID_PAGE_ID);
	vecode(vs.idx, rootPage->traversalAndVerify(NULL, NULL, NULL, NULL, NULL));
	session->markDirty(rootHandle);
	nftrace(ts.idx, tout << "Create index, indexNo: " << indexNo << " indexId: " << indexId << " root page: " << rootPageId);

	if (heap == NULL) {	// 不用从堆创建，直接返回
		session->releasePage(&rootHandle);
		goto Success;
	} else {
		try {
			//appendIndexFromHeapBySort中的sort会占用很长时间，不能长期持有latch去sort，但此时page是pin住的
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
 * 使用外部排序器将指定堆的数据读取出来建立索引
 *
 * @pre	调用之前必须已经分配了根页面在索引文件头部创建了该索引的相关信息
 * @pre 调用之前根页面已经被pin住，但是不加锁
 * @post 不持有任何资源
 *
 * @param session		会话句柄
 * @param def			要创建的索引定义
 * @param heap			相关的堆对象
 * @param indexId		索引ID
 * @param rootPageId	索引的根页面ID
 * @param rootHandle	根页面句柄
 * @throw 文件读写错误，索引页面读写错误，索引唯一性约束违背异常
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
		// 初始化排序器，排序准备取数据
		Sorter sorter(m_db->getControlFile(), m_lobStorage, indexDef, tblDef);
		sorter.sort(session, heap);

		SubRecord *leafLastRecord = lastRecords[0];
		lastRecords[0]->m_size = 0;

		latchPage(session, createTrace->m_pageHandle[0], Exclusived);
		while (true) {
			SubRecord *record = sorter.getSortedNextKey();
			if (record == NULL)	// 排序结束
				break;

			if (isUniqueIndex && isKeyEqual(indexDef, record, lastRecords[0], padKey, isFastComparable)) {	// 该索引违背唯一性约束，丢弃索引抛出异常
				completeIndexCreation(session, createTrace);
				NTSE_THROW(NTSE_EC_INDEX_UNQIUE_VIOLATION, "Index %s unique violated!", indexDef->m_name);
			}

			IndexPage *page = (IndexPage*)createTrace->m_pageHandle[0]->getPage();
			if (!(page->isPageStillFree() && page->appendIndexKey(session, m_logger, createTrace->m_pageId[0], record, leafLastRecord) == INSERT_SUCCESS)) {	// 最右页面已经满
				PageId newPageId;
				PageHandle *newPageHandle = createNewPageAndAppend(session, createTrace, 0, record, &newPageId, LEAF_PAGE);
				// 将满的页面插入索引树
				appendIndexNonLeafPage(session, createTrace);
				// 更新路径信息
				updateLevelInfo(session, createTrace, 0, newPageId, newPageHandle);
			}

			IndexKey::copyKey(leafLastRecord, record, false);
		}

		completeIndexCreation(session, createTrace);
		memoryContext->resetToSavepoint(savePoint);
	} catch (NtseException &e) {
		//unlatchAllTracePages(session, createTrace);
		//对当前插入的叶页面进行mark dirty和unlatch
		//markDirtyAndUnlatch(session, createTrace->m_pageHandle[0]);
		//unPinAllTracePages(session, createTrace);
		m_pagesManager->discardIndexByIDXId(session, m_logger, createTrace->m_indexId, -1);
		memoryContext->resetToSavepoint(savePoint);
		throw e;
	}
}



/**
 * 比较两个key是否相等
 *
 * @param indexDef		索引定义
 * @param key1			键值1
 * @param key2			键值2
 * @param padkey		pad格式的临时键值
 * @param isFastable	表示当前键值比较能否快速比较
 * @return 相等TRUE，否则FALSE
 */
bool DrsBPTreeIndice::isKeyEqual(const IndexDef *indexDef, SubRecord *key1, SubRecord *key2, SubRecord *padKey, bool isFastable) {
	if (isFastable)
		return IndexKey::isKeyValueEqual(key1, key2);
	else {
		if (!IndexKey::isKeyValid(key1) || !IndexKey::isKeyValid(key2))
			return false;
		// 需要先转换成pad格式再比较
		RecordOper::convertKeyCP(m_tableDef, indexDef, key1, padKey);
		return (RecordOper::compareKeyPC(m_tableDef, padKey, key2, indexDef) == 0);
	}
}



/**
 * 交换指定键值的内容，用于创建索引出现页面分裂时，且page必定不是叶页面
 *
 * @param lastRecord	要交换的键值1
 * @param appendKey		要交换的键值2
 * @param page			appendKey已经保存在该对应页面的第一项
 */
void DrsBPTreeIndice::swapKeys(SubRecord *lastRecord, SubRecord *appendKey, Page *page) {
	IndexKey::copyKey(appendKey, lastRecord, true);
	KeyInfo keyInfo;
	((IndexPage*)page)->getFirstKey(lastRecord, &keyInfo);
}



/**
 * 将创建索引信息当中指定层的信息更新成新的信息，如果需要，释放就信息页面的锁
 *
 * @param session		会话句柄
 * @param createTrace	创建索引信息
 * @param level			要更新的层数
 * @param pageId		新的页面ID
 * @param pageHandle	新的页面句柄
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
 * 创建一个新页面，插入指定项，项保存在createTrace当中
 *
 * @post	新页面已经被加了互斥锁，且标志为脏
 * @param session		会话句柄
 * @param createTrace	创建索引信息
 * @param level			创建页面的层数，可能大于已有页面的层数
 * @param appendKey		要添加的键值
 * @param pageId		IN/OUT	返回创建页面的ID
 * @param pageType		要创建页面的类型
 * @return 返回创建页面句柄
 */
PageHandle* DrsBPTreeIndice::createNewPageAndAppend(Session *session, BPTreeCreateTrace *createTrace, u8 level, const SubRecord *appendKey, PageId *pageId, IndexPageType pageType) {
	PageHandle *pageHandle = m_pagesManager->allocPage(m_logger, session, createTrace->m_indexId, pageId);
	session->markDirty(pageHandle);
	IndexPage *page = (IndexPage*)pageHandle->getPage();
	IndexPage *prevPage;

	if (level < createTrace->m_indexLevel) {
		prevPage = (IndexPage*)createTrace->m_pageHandle[level]->getPage();
		prevPage->m_nextPage = *pageId;
	} else	// 新创建根页面，没有前驱
		prevPage = NULL;

	page->initPage(session, m_logger, true, false, *pageId, createTrace->m_pageMark, pageType, level, prevPage == NULL ? INVALID_PAGE_ID : createTrace->m_pageId[level], INVALID_PAGE_ID);
	page->appendIndexKey(session, m_logger, *pageId, appendKey, NULL);	// 肯定可以插入成功

	return pageHandle;
}



/**
 * 创建索引时，将叶页面的空闲页面加入索引树当中，该过程可能会一直进行到根结点
 *
 * @pre		必须是当前叶页面已经插满
 * @post	涉及到的非叶结点的lastRecord都会被更新
 *
 * @param session		会话句柄
 * @param createTrace	创建索引信息
 * @return
 */
void DrsBPTreeIndice::appendIndexNonLeafPage(Session *session, BPTreeCreateTrace *createTrace) {
	u8 level = 1;
	SubRecord **lastRecords = createTrace->m_lastRecord;
	SubRecord *idxKey = createTrace->m_idxKey2;

	// 构造要插入的键值，加入PageId信息
	IndexKey::copyKey(idxKey, lastRecords[0], false);
	PageId sonPageId = createTrace->m_pageId[0];
	IndexKey::appendPageId(idxKey, sonPageId);

	if (createTrace->m_indexLevel == 1) {	// 当前只有一层，按照根结点分裂方式分裂
		createNewRoot(session, createTrace, idxKey);
		//对root页进行markdirty和unlatch，但此时root还是持有pin的
		markDirtyAndUnlatch(session, createTrace->m_pageHandle[createTrace->m_indexLevel - 1]);
		return;
	}

	while (true) {
		assert(level < createTrace->m_indexLevel);
		PageId pageId = createTrace->m_pageId[level];
		latchPage(session, createTrace->m_pageHandle[level], Exclusived);
		IndexPage *page = (IndexPage*)createTrace->m_pageHandle[level]->getPage();

		if (page->isPageStillFree() && page->appendIndexKey(session, m_logger, createTrace->m_pageId[level], idxKey, lastRecords[level]) == INSERT_SUCCESS) {	// 可以直接插入成功
			IndexKey::copyKey(lastRecords[level], idxKey, true);
			markDirtyAndUnlatch(session, createTrace->m_pageHandle[level]);
			return;
		} else {	// 当前层需要分裂新页面
			if (page->isPageRoot()) {
				PageId newPageId;
				PageHandle *newHandle = createNewPageAndAppend(session, createTrace, level, idxKey, &newPageId, NON_LEAF_PAGE);

				// 创建当前最大键值插入到新的根页面，更新本层lastRecord
				swapKeys(lastRecords[level], idxKey, newHandle->getPage());
				IndexKey::appendPageId(idxKey, pageId);
				createNewRoot(session, createTrace, idxKey);
				//对root页进行markdirty和unlatch，但此时root还是持有pin的
				markDirtyAndUnlatch(session, createTrace->m_pageHandle[createTrace->m_indexLevel - 1]);

				// 更新本层信息
				updateLevelInfo(session, createTrace, level, newPageId, newHandle);
				//对newHandle mark dirty并unlatch，但仍然持有pin。newHandle是一个新的非页节点
				markDirtyAndUnlatch(session, createTrace->m_pageHandle[level]);

				return;
			} else {
				PageId newPageId;
				PageHandle *newHandle = createNewPageAndAppend(session, createTrace, level, idxKey, &newPageId, NON_LEAF_PAGE);

				{	// 保存当前层需要插入的键值以及更新当前层的lastRecord
					swapKeys(lastRecords[level], idxKey, newHandle->getPage());
					IndexKey::appendPageId(idxKey, pageId);
				}

				updateLevelInfo(session, createTrace, level, newPageId, newHandle);
				//对newHandle mark dirty并unlatch，但仍然持有pin。newHandle是一个新的非页节点
				markDirtyAndUnlatch(session, createTrace->m_pageHandle[level]);
				level++;
			}
		}
	}
}


/**
 * 创建新的根页面，并初始化创建索引信息中根页面对应层的信息
 *
 * @param session		会话句柄
 * @param createTrace	创建索引信息
 * @param appendKey		要添加到新页面的键值
 */
void DrsBPTreeIndice::createNewRoot(Session *session, BPTreeCreateTrace *createTrace, SubRecord *appendKey) {
	u8 level = createTrace->m_indexLevel;
	// 更新原root页面类型
	IndexPage *page = (IndexPage*)createTrace->m_pageHandle[level - 1]->getPage();
	u8 newType = (u8)(level == 1 ? LEAF_PAGE : NON_LEAF_PAGE);
	page->updateInfo(session, m_logger, createTrace->m_pageId[level - 1], OFFSET(IndexPage, m_pageType), 1, (byte*)&newType);
	// 创建新root页面
	PageId rootPageId;
	PageHandle *rootHandle = createNewPageAndAppend(session, createTrace, level, appendKey, &rootPageId, ROOT_PAGE);
	// 修改相关信息
	updateLevelInfo(session, createTrace, level, rootPageId, rootHandle);
	IndexKey::copyKey(createTrace->m_lastRecord[level], appendKey, true);
	++createTrace->m_indexLevel;
}



/**
 * 完成索引的创建，将空闲页面链入索引，修改索引文件头根页面ID信息
 *
 * @param session		会话句柄
 * @param createTrace	创建索引信息
 */
void DrsBPTreeIndice::completeIndexCreation(Session *session, BPTreeCreateTrace *createTrace) {
	PageId rootPageId = createTrace->m_pageId[createTrace->m_indexLevel - 1];

	if (createTrace->m_indexLevel > 1) {	// 要将createTrace当中的页面都链入到索引树中
		SubRecord *key1 = createTrace->m_idxKey1, *key2 = createTrace->m_idxKey2;
		SubRecord **lastRecords = createTrace->m_lastRecord;
		key1->m_size = key2->m_size = 0;

		// 准备要插入的数据
		PageId sonPageId = createTrace->m_pageId[0];
		IndexKey::copyKey(key1, lastRecords[0], false);
		IndexKey::appendPageId(key1, sonPageId);

		u16 level = createTrace->m_indexLevel;
		for (u8 i = 1; i < level; i++) {
			// 如果下层的Append引起了新页面的创建，本层需要Append两个item
			s16 items = !IndexKey::isKeyValid(key2) ? 1 : 2;
			SubRecord *appendKey = key1;
			while (--items >= 0) {
				latchPage(session, createTrace->m_pageHandle[i], Exclusived);
				IndexPage *page = (IndexPage*)createTrace->m_pageHandle[i]->getPage();
				if (page->isPageStillFree() && page->appendIndexKey(session, m_logger, createTrace->m_pageId[i], appendKey, lastRecords[i]) == INSERT_SUCCESS) {
					// 插入成功，保存当前层最大值
					IndexKey::copyKey(lastRecords[i], appendKey, true);
					appendKey->m_size = 0;
				} else {	// 需要创建新的页面，最多只会出现一次
					PageId newPageId;
					PageHandle *newPageHandle = createNewPageAndAppend(session, createTrace, i, appendKey, &newPageId, NON_LEAF_PAGE);

					// lastRecord表示插入值，插入指针表示原页面最大项供上一层插入
					// 执行完这一步后appendKey是前一个page的最大key，lastRecord是最新page的第一个key
					swapKeys(lastRecords[i], appendKey, newPageHandle->getPage());
					IndexKey::appendPageId(appendKey, createTrace->m_pageId[i]);

					// 如果本层代表根页面，需要将页面根标志位去除
					if (i == level - 1) {
						u8 newType = (u8)NON_LEAF_PAGE;
						page->updateInfo(session, m_logger, createTrace->m_pageId[i], OFFSET(IndexPage, m_pageType), 1, (byte*)&newType);
					}

					updateLevelInfo(session, createTrace, i, newPageId, newPageHandle);
				}

				markDirtyAndUnlatch(session, createTrace->m_pageHandle[i]);
				appendKey = key2;	// 如果还需要插入key2
			}

			if (i == level - 1 && !IndexKey::isKeyValid(key1) && !IndexKey::isKeyValid(key2))	// append到根页面，且没有分裂，结束插入
				break;

			// 如果key1或者key2有一个的size不为0，说明本层插入有经历分裂
			// 即使如此，最多只有其中一个的size不为0，保证这个值是key1
			if (IndexKey::isKeyValid(key2)) {
				IndexKey::swapKey(&key1, &key2);	// TODO：确认该情况是怎么发生以及处理的必要性. key1插入正常，key2的插入导致页面分裂
			}

			// 记录本层最大项内容供上层插入使用
			SubRecord *maxKey = !IndexKey::isKeyValid(key1) ? key1 : key2;
			IndexKey::copyKey(maxKey, lastRecords[i], true);
			IndexKey::appendPageId(maxKey, createTrace->m_pageId[i]);
		}

		if (IndexKey::isKeyValid(key1)) {	// 根结点分裂过，将key1和key2的内容组成新的根结点
			assert(IndexKey::isKeyValid(key2));
			// 创建新root页面
			PageId newRootPageId;
			PageHandle *newRootHandle = createNewPageAndAppend(session, createTrace, level, key1, &newRootPageId, ROOT_PAGE);
			// 修改相关信息
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
	//对叶子节点进行mark dirty and unlatch
	markDirtyAndUnlatch(session, createTrace->m_pageHandle[0]);
	unPinAllTracePages(session, createTrace);

	// 设置索引文件头页面信息
	m_pagesManager->updateIndexRootPageId(m_logger, session, m_pagesManager->getIndexNo(createTrace->m_indexId), rootPageId);
}



/**
 * 一次性释放创建信息当中涉及到页面的所有latch，允许多次调用
 *
 * @param session		会话句柄
 * @param createTrace	创建信息
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

/** latch 页面
 * @param session 会话句柄
 * @param pageHandle 需要加锁的页面句柄
 * @param lockMode 加锁模式
 */
void DrsBPTreeIndice::latchPage(Session *session, PageHandle *pageHandle, LockMode lockMode) {
	LOCK_PAGE_HANDLE(session, pageHandle, lockMode);
}

/** 将指定页面标识位脏，同时unlatch该页面
 * @param session 会话句柄
 * @param pageHandle 指定页面
 */
void DrsBPTreeIndice::markDirtyAndUnlatch(Session *session, PageHandle *pageHandle) {
	session->markDirty(pageHandle);
	session->unlockPage(&pageHandle);
}

/** 将trace上的页面全unpin
 * @param session 会话句柄
 * @param createTrace 创建信息
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
 * 创建建立索引信息结构
 *
 * @param memoryContext	内存分配上下文
 * @param def			索引定义
 * @param indexId		索引ID
 * @return 索引创建信息结构
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
 * 在恢复的过程中——包括redoDropIndex和undoCreateIndex，同步索引内存镜像信息
 * @param session	会话句柄
 * @param indexId	删除的索引内部ID号
 */
void DrsBPTreeIndice::recvMemSync(Session *session, u16 indexId) {
	ftrace(ts.idx, tout << indexId);

	// 如果需要，删除内存中的镜像，这里m_indice和m_indexSeq两个数组始终应该一致，要么都需要要么都不需要
	for (u16 idxSeq = 0; idxSeq < m_indexNum; idxSeq++) {
		if (((DrsBPTreeIndex*)m_indice[idxSeq])->getIndexId() == indexId) {
			DrsBPTreeIndex *index = (DrsBPTreeIndex *)m_indice[idxSeq];
			// 从数组中删除
			memmove((DrsIndex*)m_indice + idxSeq, (DrsIndex*)m_indice + idxSeq + 1, (m_indexNum - idxSeq - 1) * sizeof(DrsIndex*));
			m_indice[m_indexNum] = NULL;
			m_indexNum--;

			// 调用buffer清除相关页面
			Buffer *buffer = m_db->getPageBuffer();
			buffer->freePages(session, m_file, indexId, DrsBPTreeIndice::bufferCallBackSpecifiedIDX);

			m_orderedIndices.remove(idxSeq, index->getIndexDef()->m_unique);

			delete index;

			break;
		}
	}
}


/**
 * 重做丢弃某个索引的操作
 *
 * @param session	会话句柄
 * @param lsn		日志LSN
 * @param log		日志内容
 * @param size		日志长度
 * @return 删除的索引编号
 */
s32 DrsBPTreeIndice::redoDropIndex(Session *session, u64 lsn, const byte *log, uint size) {
	u8 indexId;
	s32 idxNo;
	m_logger->decodeDropIndex(log, size, &indexId, &idxNo);
	ftrace(ts.idx, tout << indexId << idxNo);

	// 在文件中标记删除
	m_pagesManager->discardIndexByIDXIdRecv(session, indexId, true, lsn);
	recvMemSync(session, indexId);

	return idxNo;
}



/**
 * 重做创建索引结束操作，此时需要同步内存和索引文件当中的内容，保证各个索引的信息都在内存
 * @param log		日志内容
 * @param size		日志长度
 */
void DrsBPTreeIndice::redoCreateIndexEnd(const byte *log, uint size) {
	u8 indexId;
	bool successful;
	IndexDef indexDef;

	m_logger->decodeCreateIndexEnd(log, size, &indexDef, &indexId, &successful);

	ftrace(ts.idx, tout << indexId << successful;);

	if (successful) {
		// 需要判断索引的信息是否已经存在在内存当中，如果不存在，强制重新从头页面读取信息
		for (uint i = 0; i < m_indexNum; i++)
			if (((DrsBPTreeIndex*)m_indice[i])->getIndexId() == indexId)
				return;

		DrsBPTreeIndex *index = new DrsBPTreeIndex(this, m_tableDef, &indexDef, indexId, m_pagesManager->getFileHeaderPage()->m_rootPageIds[m_indexNum]);
		m_indice[m_indexNum++] = index;

		// 同时要维护有序的索引数组
		m_orderedIndices.add(m_indexNum - 1, index->getIndexId(), indexDef.m_unique);
	}
}


/**
 * redo插入、删除、Append添加操作
 *
 * @param session		会话句柄
 * @param lsn			日志LSN
 * @param log			日志内容
 * @param size			日志长度
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
 * 重做各种SMO操作
 * @param session	会话句柄
 * @param lsn		日志lsn
 * @param log		日志内容
 * @param size		日志长度
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
 * redo页面信息修改操作
 * @param session	会话句柄
 * @param lsn		日志lsn
 * @param log		日志内容
 * @param size		日志长度
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
		if (m_pagesManager->isPageByteMapPage(pageId)) {	// 位图页修改，需要判断是否需要扩展索引文件
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
 * 根据LOG_IDX_DML_END日志判断索引修改是否成功
 *
 * @param log	LOG_IDX_DML_END日志内容
 * @param size	LOG_IDX_DML_END日志内容大小
 * @return 索引修改是否成功
 */
bool DrsBPTreeIndice::isIdxDMLSucceed(const byte *log, uint size) {
	bool succ;
	m_logger->decodeDMLUpdateEnd(log, size, &succ);
	ftrace(ts.idx, tout << succ);

	return succ;
}


/**
 * 根据LOG_IDX_DMLDONE_IDXNO日志解析当前标志的是哪个索引修改成功
 * @param log	日志内容
 * @param size	日志长度
 * @return 日志包含的更新成功的索引序号
 */
u8 DrsBPTreeIndice::getLastUpdatedIdxNo(const byte *log, uint size) {
	u8 indexNo;
	m_logger->decodeDMLDoneUpdateIdxNo(log, size, &indexNo);
	ftrace(ts.idx, tout << indexNo);

	return indexNo;
}


/**
 * 回退DML操作
 * @param session		会话句柄
 * @param lsn			操作LSN
 * @param log			日志内容
 * @param size			日志长度
 * @param logCPST		是否记录补偿日志
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

	// 写补偿日志
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
 * 回退SMO操作
 * @param session		会话句柄
 * @param lsn			操作LSN
 * @param log			日志内容
 * @param size			日志长度
 * @param logCPST		是否记录补偿日志
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
		} else {	// redo补偿日志
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
		} else {	// redo补偿日志
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
 * 回退页面修改操作
 * @param session		会话句柄
 * @param lsn			操作LSN
 * @param log			日志内容
 * @param size			日志长度
 * @param logCPST		是否记录补偿日志
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
		} else {	// redo补偿日志
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
		} else {	// redo补偿日志
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
		} else {	// redo补偿日志
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
		} else {	// redo补偿日志
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
		} else {	// redo补偿日志
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
 * 回退创建索引操作，正常undo调用或者redo补偿日志时候使用
 * 这里需要注意的是，undo的时候可能索引确实已经不存在了，这时候就不需要进行实质的undo
 * redo补偿日志的时候，也可能是已经不存在的索引
 * @param session		会话句柄
 * @param lsn			操作LSN
 * @param log			日志内容
 * @param size			日志长度
 * @param logCPST		是否记录补偿日志
 */
void DrsBPTreeIndice::undoCreateIndex(Session *session, u64 lsn, const byte *log, uint size, bool logCPST) {
	// 使用逻辑undo减少该步骤带来的内存和日志开销
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
 * 重做DML操作的补偿日志
 * @param session	会话句柄
 * @param lsn		操作LSN
 * @param log		补偿日志内容
 * @param size		补偿日志长度
 */
void DrsBPTreeIndice::redoCpstDML(Session *session, u64 lsn, const byte *log, uint size) {
	ftrace(ts.idx, );
	undoDML(session, lsn, log, size, false);
}



/**
 * 重做创建索引操作的补偿日志
 * @param session	会话句柄
 * @param lsn		操作LSN
 * @param log		补偿日志内容
 * @param size		补偿日志长度
 */
void DrsBPTreeIndice::redoCpstCreateIndex(Session *session, u64 lsn, const byte *log, uint size) {
	ftrace(ts.idx, );
	undoCreateIndex(session, lsn, log, size, false);
}



/**
 * 重做SMO操作的补偿日志
 * @param session	会话句柄
 * @param lsn		操作LSN
 * @param log		补偿日志内容
 * @param size		补偿日志长度
 */
void DrsBPTreeIndice::redoCpstSMO(Session *session, u64 lsn, const byte *log, uint size) {
	ftrace(ts.idx, );
	undoSMO(session, lsn, log, size, false);
}



/**
 * 重做页面修改操作的补偿日志
 * @param session	会话句柄"
 * @param lsn		操作LSN
 * @param log		补偿日志内容
 * @param size		补偿日志长度
 */
void DrsBPTreeIndice::redoCpstPageSet(Session *session, u64 lsn, const byte *log, uint size) {
	ftrace(ts.idx, );
	undoPageSet(session, lsn, log, size, false);
}



/**
 * Buffer清理页面回调函数，判断某个页面是否是指定索引的
 * 需要传入pageId来判断当前页面是不是头页面或者索引位图页
 * TODO: 更好的方法是在IndicePage类当中添加一个变量标记
 * 但是这种方法，会导致现有的所有索引数据文件都不可用，慎重修改
 *
 * @param page		要判断的回收页面
 * @param pageId	回收页面的PageId信息
 * @param indexId	指定索引的ID
 * @return 属于指定索引true/不属于false
 */
bool DrsBPTreeIndice::bufferCallBackSpecifiedIDX(Page *page, PageId pageId, uint indexId) {
	return pageId >= IndicePageManager::NON_DATA_PAGE_NUM && IndexPage::getIndexId(((IndexPage*)page)->m_pageMark) == indexId;
}

/**
 * 获取表中索引数据占用空间大小，不包含已经分配但没有使用的页面
 *
 * @param includeMeta 是否包含索引文件头，页面分配位图等非数据页
 * @return 表中索引数据占用空间大小
 */
u64 DrsBPTreeIndice::getDataLength(bool includeMeta) {
	u64 length = 0;
	// 遍历各个索引累加统计信息
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
 * 得到指定页面所属的索引号
 *
 * @param page 页面数据，已经用S锁锁定
 * @param pageId 页号
 * @return 如果指定的页面为某个索引占用的页面则返回索引号，如果是不从属于某个索引的共用页面则返回-1
 */
int DrsBPTreeIndice::getIndexNo(const BufferPageHdr *page, u64 pageId) {
	assert(page != NULL && pageId < m_pagesManager->getFileSize() / INDEX_PAGE_SIZE);
	if (pageId < IndicePageManager::NON_DATA_PAGE_NUM)
		return -1;

	IndexPage *indexPage = (IndexPage*)page;
	u8 indexId = (u8)IndexPage::getIndexId(indexPage->m_pageMark);
	u8 indexNo = m_pagesManager->getIndexNo(indexId);

	// 这里采用脏读的方式再次确认某个页面是否属于某个索引
	IndexHeaderPage *header = m_pagesManager->getFileHeaderPage();
	if (indexNo >= header->m_indexNum || header->m_indexIds[indexNo] != indexId)
		return -1;

	return indexNo;
}

/**
 * 返回大对象管理器
 */
LobStorage* DrsBPTreeIndice::getLobStorage() {
	return m_lobStorage;
}

/**
 * 返回数据库对象
 */
Database* DrsBPTreeIndice::getDatabase() {
	return m_db;
}


/**
 * 获得索引目录数据对象统计信息 
 * @return 数据对象状态
 */
DBObjStats* DrsBPTreeIndice::getDBObjStats() {
	return m_dboStats;
}

/**
 * 根据要更新的后项，计算当前有哪些索引需要更新
 * @param memoryContext		内存分配上下文，用来保存更新的索引序号数组，调用者注意空间的使用和释放
 * @param update			更新操作的后项信息，更新字段有序排列
 * @param updateNum			out 返回有多少个索引需要被更新
 * @param updateIndices		out 指向memoryContext分配的空间，返回需要更新的索引序号，该序号和索引内部有序序列号一致
 * @param updateUniques		out 返回多少唯一索引需要被更新
 */
void DrsBPTreeIndice::getUpdateIndices( MemoryContext *memoryContext, const SubRecord *update, u16 *updateNum, u16 **updateIndices, u16 *updateUniques ) {
	u16 *toBeUpdated = (u16*)memoryContext->alloc(sizeof(update->m_columns[0]) * Limits::MAX_INDEX_NUM);
	u16 updates = 0;
	u16 uniques = 0;

	for (u16 i = 0; i < m_tableDef->m_numIndice; i++) {
		u16 indexNo = m_orderedIndices.getOrder(i);
		IndexDef *indexDef = m_tableDef->m_indice[indexNo];
		for (u16 j = 0; j < indexDef->m_numCols; j++) {
			// 有序的更新字段，可以二分查找
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
 * 索引页面管理器构造函数
 * @param tableDef		所属表定义
 * @param file			文件句柄
 * @param headerPage	文件头页面
 * @param syslog		系统日志对象
 * @param dbObjStats	数据库统计对象
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
 * 初始化指定索引文件的头部信息包含位图
 * @param file 索引文件描述符
 */
void IndicePageManager::initIndexFileHeaderAndBitmap(File *file) {
	u64 offset = INDEX_PAGE_SIZE;
	// 初始化索引文件头部信息并写入文件
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

	// 初始化索引文件位图信息并写入文件
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
 * 计算当前哪个位图项表示的页面块可用
 *
 * @pre 调用者已经对索引头页面加过互斥锁，加位图页面锁可以直接获取
 * @param logger	日志记录器
 * @param session	会话句柄
 * @param indexId	索引序列号
 * @return 可用页面块的起始页面ID
 */
u64 IndicePageManager::calcPageBlock( IndexLog *logger, Session *session, u8 indexId) {
	/* 通过位图查找一块新的索引块，分配根页面 */
	u32 bmOffset = ((IndexHeaderPage*)m_headerPage)->m_curFreeBitMap;
	u32 inOffset = bmOffset % INDEX_PAGE_SIZE;
	PageId bmPageId = bmOffset / INDEX_PAGE_SIZE + HEADER_PAGE_NUM - 1;

	if (inOffset == 0)	// 到达一个新的位图页面
		inOffset = sizeof(Page);

	Page *bmPage;
	PageHandle *bmHandle;
	while (true) {
		bmHandle = GET_PAGE(session, m_file, PAGE_INDEX, bmPageId, Exclusived, m_dbObjStats, NULL);
		bmPage = bmHandle->getPage();

		byte *start = (byte*)bmPage + inOffset;
		while (true) {
			if (inOffset < INDEX_PAGE_SIZE && *start != 0x00) {		// 该Byte表示的区域被使用
				inOffset++;
				start++;
				continue;
			}

			break;	// 存在空白页面块或者页面遍历结束
		}

		if (inOffset >= INDEX_PAGE_SIZE) {		// 需要获得下一个位图页
			session->releasePage(&bmHandle);
			bmPageId++;
			if (bmPageId >= BITMAP_PAGE_NUM) {	// 需要循环遍历
				bmPageId = HEADER_PAGE_NUM;
			}
			inOffset = sizeof(Page);
			continue;
		}

		// 至此说明当前位图页的inOffset位置的第pos位表示的块空闲可用
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
 * 根据指定的页面块起始地址，分配/初始化一个新的页面块
 * 返回页面块的首页面供使用，由于需要初始化，该页面持有X Latch
 *
 * @pre 已经持有文件头页面的Latch，可以直接对m_headerPage进行读写
 * @param logger		日志记录器
 * @param session		会话句柄
 * @param blockStartId	要申请页面块起始页面的ID
 * @param indexId		需要分配页面的索引ID
 * @param incrSize		如果需要扩展文件，每次至少扩展该大小的范围
 * @return 页面块的首页面X Latch，外层使用完释放
 */
PageHandle* IndicePageManager::allocPageBlock(IndexLog *logger, Session *session, u64 blockStartId, u8 indexId) {
	//ftrace(ts.idx, tout << indexId << " alloc " << blockStartId;);
	u8 indexNo = getIndexNo(indexId);

	extendFileIfNecessary(blockStartId);
	IndexHeaderPage *headerPage = (IndexHeaderPage*)m_headerPage;
	headerPage->updatePageUsedStatus(session, logger, indexNo, headerPage->m_indexDataPages[indexNo] + PAGE_BLOCK_NUM, PAGE_BLOCK_NUM);

	// 初始化块内每一个空闲页面（不包含第一个页面）
	PageId idOffset = 1;
	u32 mark = IndexPage::formPageMark(indexId);
	while (idOffset < PAGE_BLOCK_NUM) {
		PageId pageId = blockStartId + idOffset;
		PageHandle *pHandle = NEW_PAGE(session, m_file, PAGE_INDEX, pageId, Exclusived, m_dbObjStats);	// 由于持有头页面X-Latch，新页面块不可能被其他人使用，可直接加锁
		IndexPage *prevPage = (IndexPage*)(pHandle->getPage());
		session->markDirty(pHandle);
		prevPage->clearData();	// 为了保证页面便于验证数据正确性，清空页面数据区域的信息，头部不清理
		prevPage->initPage(session, logger, false, false, pageId, mark, FREE, 0, INVALID_PAGE_ID, idOffset < PAGE_BLOCK_NUM - 1 ? pageId + 1: INVALID_PAGE_ID);
		session->releasePage(&pHandle);

		idOffset++;
	}

	PageHandle *handle = GET_PAGE(session, m_file, PAGE_INDEX, blockStartId, Exclusived, m_dbObjStats, NULL);
	((IndexPage*)handle->getPage())->clearData();
	return handle;
}



/**
 * 为指定索引分配一个新页面，如果当前空闲页面链表为空则新分配一个页面块，返回页面加了X-Latch
 *
 * @param logger		日志记录器
 * @param session		会话句柄
 * @param indexId		索引ID
 * @param pageId		IN/OUT	保存分配页面的ID号
 * @return PageHandle*	返回加了X-Latch的新分配的页面，外层负责释放Latch
 */
PageHandle* IndicePageManager::allocPage( IndexLog *logger, Session *session, u8 indexId, PageId *pageId) {
	PageHandle *pHeaderHandle = GET_PAGE(session, m_file, PAGE_INDEX, 0, Exclusived, m_dbObjStats, m_headerPage);
	IndexHeaderPage *headerPage = (IndexHeaderPage*)(pHeaderHandle->getPage());
	PageHandle *newPageHandle;

	u8 indexNo = getIndexNo(indexId);
	if (headerPage->m_freeListPageIds[indexNo] == INVALID_PAGE_ID) {
		// 需要新申请一个页面块
		*pageId = calcPageBlock(logger, session, indexId);
		newPageHandle = allocPageBlock(logger, session, *pageId, indexId);
		PageId freePageId = *pageId + 1;
		headerPage->updateInfo(session, logger, 0, OFFSETOFARRAY(IndexHeaderPage, m_freeListPageIds, sizeof(PageId), indexNo), sizeof(PageId), (byte*)&freePageId);
	} else {
		// 直接获得当前空闲页面链首，这里得到的页面数据区域肯定是清空的
		*pageId = headerPage->m_freeListPageIds[indexNo];
		newPageHandle = GET_PAGE(session, m_file, PAGE_INDEX, *pageId, Exclusived, m_dbObjStats, NULL);
		PageId freePageId = ((IndexPage*)newPageHandle->getPage())->m_nextPage;
		headerPage->updateInfo(session, logger, 0, OFFSETOFARRAY(IndexHeaderPage, m_freeListPageIds, sizeof(PageId), indexNo), sizeof(PageId), (byte*)&freePageId);
	}

	// 标记页面使用状态信息
	headerPage->updatePageUsedStatus(session, logger, indexNo, (u64)-1, headerPage->m_indexFreePages[indexNo] - 1);

	session->markDirty(pHeaderHandle);
	session->releasePage(&pHeaderHandle);

	assert(newPageHandle != NULL);
	vecode(vs.idx, ((IndexPage*)newPageHandle->getPage())->isDataClean());
	return newPageHandle;
}


/**
 * 为新创建的索引分配一个新的根页面，在头页面记录相关信息，返回页面加了X-Latch
 *
 * @param logger	日志记录器
 * @param session	会话句柄
 * @param indexNo	OUT 索引序号
 * @param indexId	OUT	索引ID
 * @param pageId	OUT	保存分配页面的ID号
 * @return PageHandle*	返回加了X-Latch的新分配的页面，外层负责释放Latch
 */
PageHandle* IndicePageManager::createNewIndexRoot(IndexLog *logger, Session *session, u8 *indexNo, u8 *indexId, PageId *rootPageId) {
	// 获取文件头页面信息
	PageHandle *headerHandle = GET_PAGE(session, m_file, PAGE_INDEX, 0, Exclusived, m_dbObjStats, m_headerPage);
	IndexHeaderPage *headerPage = (IndexHeaderPage*)(headerHandle->getPage());

	*indexId = headerPage->getAvailableIndexIdAndMark(session, logger);
	u32 num = headerPage->m_indexNum + 1;
	headerPage->updateInfo(session, logger, 0, OFFSET(IndexHeaderPage, m_indexNum), sizeof(u32), (byte*)&num);

	*indexNo = getIndexNo(*indexId);
	// 计算并分配页面块，初始化根页面
	PageId rootId = calcPageBlock(logger, session, *indexId);
	PageHandle *rootHandle = allocPageBlock(logger, session, rootId, *indexId);

	// 持有新根页面的锁，尽早释放文件头页的锁
	headerPage->updateInfo(session, logger, 0, OFFSETOFARRAY(IndexHeaderPage, m_rootPageIds, sizeof(PageId), headerPage->m_indexNum - 1), sizeof(PageId), (byte*)&rootId);
	PageId freePageId = rootId + 1;
	headerPage->updateInfo(session, logger, 0, OFFSETOFARRAY(IndexHeaderPage, m_freeListPageIds, sizeof(PageId), headerPage->m_indexNum - 1), sizeof(PageId), (byte*)&freePageId);
	// 更改页面使用统计信息
	headerPage->updatePageUsedStatus(session, logger, *indexNo, PAGE_BLOCK_NUM, PAGE_BLOCK_NUM - 1);

	session->markDirty(headerHandle);
	session->releasePage(&headerHandle);

	*rootPageId = rootId;
	return rootHandle;
}




/**
 * 释放指定页面，链入对应索引的空闲页面链表
 *
 * @pre 外层需要传入要释放的页面，并且对该页面持有X-Latch
 * @pre 根据本索引的设计，不可能出现持有头页面来对某个正在使用的索引页面加锁的情况
 *
 * @param logger	日志记录器
 * @param session	会话句柄
 * @param pageId	要释放页面ID
 * @param indexPage	要释放的索引页面
 */
void IndicePageManager::freePage(IndexLog *logger, Session *session, PageId pageId, Page *indexPage) {
	IndexPage *page = (IndexPage*)indexPage;
	assert(page->m_pageType != FREE);
	u8 indexId = (u8)IndexPage::getIndexId(page->m_pageMark);
	u8 indexNo = getIndexNo(indexId);

	PageHandle *pHeaderHandle = GET_PAGE(session, m_file, PAGE_INDEX, 0, Exclusived, m_dbObjStats, m_headerPage);	// 该处可以直接加锁，不引起死锁
	IndexHeaderPage *headerPage = (IndexHeaderPage*)pHeaderHandle->getPage();
	page->initPage(session, logger, false, false, pageId, page->m_pageMark, FREE, page->m_pageLevel, INVALID_PAGE_ID, headerPage->m_freeListPageIds[indexNo]);
	headerPage->updateInfo(session, logger, 0, OFFSETOFARRAY(IndexHeaderPage, m_freeListPageIds, sizeof(PageId), indexNo), sizeof(PageId), (byte*)&pageId);
	// 标记页面使用状态信息
	headerPage->updatePageUsedStatus(session, logger, indexNo, (u64)-1, headerPage->m_indexFreePages[indexNo] + 1);

	session->markDirty(pHeaderHandle);
	session->releasePage(&pHeaderHandle);
}


/**
 * 在位图中将属于指定索引的页面块复位清零
 *
 * @pre 需要先对索引文件头页面进行加互斥锁
 *
 * @param session		会话句柄
 * @param indexId		指定的索引ID
 * @param setZero		释放页面块是否需要置零
 * @param maxFreePageId	该索引使用位图位的最大偏移量
 * @param opLsn			触发该操作的LSN
 * @param isRedo		触发操作是否是redo操作
 * @return 释放以后最小的空闲位图位的偏移量
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

		if (isRedo && page->m_lsn >= opLsn) {	// redo的情况下可以判断LSN优化
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
				// TODO:对于释放页面要清零的操作
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
 * 丢弃指定id的索引，修改头页面、清空复位索引使用的位图页相关位信息
 *
 * @param session	会话句柄
 * @param logger	日志记录器
 * @param indexId	索引ID
 * @param idxNo		删除索引在表定义中的序号
 * @return
 */
void IndicePageManager::discardIndexByIDXId(Session *session, IndexLog *logger, u8 indexId, s32 idxNo) {
	assert(idxNo <= Limits::MAX_INDEX_NUM);
	ftrace(ts.idx, tout << indexId << idxNo;);
	// 获取头页面，得到该索引在位图页使用最大范围
	PageHandle *rootHandle = GET_PAGE(session, m_file, PAGE_INDEX, 0, Exclusived, m_dbObjStats, m_headerPage);
	IndexHeaderPage *headerPage = (IndexHeaderPage*)rootHandle->getPage();
	u32 maxFreePageId = headerPage->m_maxBMPageIds[getIndexNo(indexId)];

	u64 lsn = logger->logDropIndex(session, indexId, idxNo);
	u32 minFreeByteMap = freePageBlocksByIDXId(session, indexId, false, maxFreePageId, lsn, false);

	u8 indexNo = getIndexNo(indexId);
	// 修改头页面信息
	headerPage->discardIndexChangeHeaderPage(indexNo, minFreeByteMap);
	m_headerPage->m_lsn = lsn;
	session->markDirty(rootHandle);

	session->releasePage(&rootHandle);
}



/**
 * 丢弃指定id的索引，修改头页面、清空复位索引使用的位图页相关位信息
 *
 * 该函数虽然修改了位图页等相关信息，但是由于是恢复过程调用，单线程不需要同步
 *
 * @param session	会话句柄
 * @param indexId	索引ID
 * @param isRedo	触发操作是否是redo操作
 * @param opLsn		触发操作的LSN
 * @return
 */
void IndicePageManager::discardIndexByIDXIdRecv(Session *session, u8 indexId, bool isRedo, u64 opLsn) {
	PageHandle *headerHandle = GET_PAGE(session, m_file, PAGE_INDEX, 0, Exclusived, m_dbObjStats, m_headerPage);
	IndexHeaderPage *headerPage = (IndexHeaderPage*)headerHandle->getPage();

	u32 minFreeByteMap = freePageBlocksByIDXId(session, indexId, false, NON_DATA_PAGE_NUM - 1, opLsn, isRedo);

	if (!isRedo || headerPage->m_lsn < opLsn) {
		u8 indexNo = getIndexNo(indexId);
		if (indexNo != Limits::MAX_INDEX_NUM) {	// 如果找不到，说明索引已经丢弃过了
			headerPage->discardIndexChangeHeaderPage(indexNo, minFreeByteMap);
			m_headerPage->m_lsn = opLsn;
			session->markDirty(headerHandle);
		}
	}

	session->releasePage(&headerHandle);
}


/**
 * 根据指定索引ID在头页面中寻找对应的逻辑编号
 *
 * m_indexIds数组的同步需要由上层的DLL增删索引修改锁保证，这里可以直接访问
 *
 * @pre 持有头页面的S-Latch或X
 * @param indexId		索引ID
 * @return 索引在头页面中的内部逻辑编号
 */
u8 IndicePageManager::getIndexNo(u8 indexId) {
	IndexHeaderPage *header = (IndexHeaderPage*)m_headerPage;
	for (u8 indexNo = 0; indexNo < header->m_indexNum; indexNo++)
		if (header->m_indexIds[indexNo] == indexId)
			return indexNo;

	return Limits::MAX_INDEX_NUM;
}


/**
 * 判断一个页面是否是位图页
 * @param pageId	页面ID
 * @return true属于，false表示不属于
 */
bool IndicePageManager::isPageByteMapPage(PageId pageId) {
	return pageId >= HEADER_PAGE_NUM && pageId < NON_DATA_PAGE_NUM;
}


/**
 * 根据指定位图和内部偏移，计算指定对应的索引文件页面ID
 * @param bmPageId	位图页ID
 * @param offset	位图页内被设置的byte偏移
 * @return	对应页面块的起始页面ID
 */
PageId IndicePageManager::getPageBlockStartId(PageId bmPageId, u16 offset) {
	assert(bmPageId <= NON_DATA_PAGE_NUM);
	u32 relativeOffset = offset - sizeof(Page);
	return (HEADER_PAGE_NUM + BITMAP_PAGE_NUM) + (((INDEX_PAGE_SIZE - sizeof(Page)) * (bmPageId - HEADER_PAGE_NUM) + relativeOffset)) * PAGE_BLOCK_NUM;
}


/**
 * 根据指定的页面块的起始页面ID，判断当前是否有必要扩展索引文件
 * 如果当前的文件大于指定页面块所需要的空间，则不扩展
 * @param blockStartId	扩展块的起始页面ID
 * @return TRUE表示扩展了空间/FALSE表示没有扩展
 */
bool IndicePageManager::extendFileIfNecessary(PageId blockStartId) {
	u64 errNo;
	u64 newLeastSize = (blockStartId + PAGE_BLOCK_NUM) * INDEX_PAGE_SIZE;
	// 判断是否需要扩展文件
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
 * 需要统计的时候更新索引统计信息当中的页面使用情况信息
 * 这里将采用脏读索引文件头页面的办法来取得统计信息，由于没有并发控制，信息可能不够准确，但是只要多次刷新必定趋于准确
 * @param indexId	索引内部编号
 * @param status	状态对象
 */
void IndicePageManager::updateNewStatus(u8 indexId, struct IndexStatus *status) {
	u8 indexNo = getIndexNo(indexId);
	IndexHeaderPage *header = (IndexHeaderPage*)m_headerPage;
	status->m_dataLength = header->m_indexDataPages[indexNo] * Limits::PAGE_SIZE;
	status->m_freeLength = header->m_indexFreePages[indexNo] * Limits::PAGE_SIZE;
}

/** 更新指定索引的根页面信息
 * @param logger		索引日志记录器
 * @param session		会话句柄
 * @param indexNo		要更新的索引序号
 * @param rootPageId	该索引的根页面ID
 */
void IndicePageManager::updateIndexRootPageId( IndexLog *logger, Session *session, u8 indexNo, PageId rootPageId ) {
	PageHandle *headerHandle = GET_PAGE(session, m_file, PAGE_INDEX, 0, Exclusived, m_dbObjStats, m_headerPage);
	((IndexHeaderPage*)m_headerPage)->updateInfo(session, logger, 0, OFFSETOFARRAY(IndexHeaderPage, m_rootPageIds, sizeof(u64), indexNo), sizeof(u64), (byte*)&rootPageId);
	session->markDirty(headerHandle);
	session->releasePage(&headerHandle);
}

/** 
 * 构造函数，生成有序索引对象
 */
OrderedIndices::OrderedIndices() {
	m_uniqueIdxNum = m_indexNum = 0;
	memset(&m_orderedIdxNo[0], -1, sizeof(m_orderedIdxNo[0]) * Limits::MAX_INDEX_NUM);
	memset(&m_orderedIdxId[0], -1, sizeof(m_orderedIdxId[0]) * Limits::MAX_INDEX_NUM);
}

/** 往有序索引队列中增加一个新的索引
 * @post 不改变队列的升序规则
 * @param order		新索引内存序号，对应于其在m_indice数组的顺序
 * @param idxId		每个索引唯一的ID号，用于内部排序
 * @param unique	新索引是否唯一
 * @return true增加成功，false无法增加
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
	// 可以断言，在唯一和非唯一两个部分，id仍旧升序排列
}

/** 从有序索引队列中移除指定的索引
 * @param order		新索引内存序号，对应于其在m_indice数组的顺序
 * @param unique	要移除的索引是不是唯一
 * @return true删除成功，false找不到指定的索引
 */
bool OrderedIndices::remove( u16 order, bool unique ) {
	for (u16 removeNo = 0; removeNo < m_indexNum; removeNo++) {
		if (m_orderedIdxNo[removeNo] == order) {
			memmove(&m_orderedIdxNo[removeNo], &m_orderedIdxNo[removeNo + 1], sizeof(m_orderedIdxNo[0]) * (m_indexNum - removeNo - 1));
			memmove(&m_orderedIdxId[removeNo], &m_orderedIdxId[removeNo + 1], sizeof(m_orderedIdxId[0]) * (m_indexNum - removeNo - 1));
			m_orderedIdxNo[m_indexNum - 1] = (u16)-1;
			m_orderedIdxId[m_indexNum - 1] = (u8)-1;

			// 需要把删除索引之后的那些索引序号调正
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

/** 取得指定位置的索引的内存未排序序号，同m_indice数组序号
 * @param no	要取的排序索引的位置
 * @return 对应的索引在m_indice数组的序号
 */
u16 OrderedIndices::getOrder( u16 no ) const {
	assert(no < m_indexNum);
	return m_orderedIdxNo[no];
}

/** 得到唯一索引的个数
 * @return 唯一索引个数
 */
u16 OrderedIndices::getUniqueIdxNum() const {
	return m_uniqueIdxNum;
}

/** 查找指定序号的索引的内部排序号
 * @return 返回排序号，如果不存在，返回(u16)-1
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
