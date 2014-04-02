/**多版本机制中版本池实现
 * author 忻丁峰 xindingfeng@corp.netease.com
 */
#include "heap/VersionPool.h"
#include "api/TNTDatabase.h"
#include "api/Table.h"
#include "misc/TableDef.h"
#include "misc/RecordHelper.h"
#include "misc/Record.h"
#include "misc/ControlFile.h"
//TODO 版本池对应的minTxnId和maxTxnId必须有controlfile提供，还有activeIndex。minTxnId和maxTxnId为推送记录前像进版本池的事务id。
//只有在版本池切换的时候，需要修改前一个版本池的maxTxnId和下一个版本池的minTxnId，同时需要将修改内容刷入外存
//只有到版本池的maxTxnId小于minReadView的时候，该版本池可以回收的。就是推送记录进版本池的全部提交且都可见

//关于版本池表的加锁，版本池表的操作主要有获取版本记录，insert和回收大对象。
//回收大对象时，肯定不会有获取版本记录和insert操作,所以回收大对象可以在不加版本池表锁情况下进行
//获取版本记录和insert由版本池的特性决定，一旦版本记录被插入，肯定不会被修改，插入过程中，肯定不会有get操作，所以也可以在无版本池表锁的情况下操作
//现在加的关于版本池表锁，主要是为了通过ntse的assert操作才做的
namespace tnt {
enum VTableCol {
	VTABLE_COL_TBLID = 0,	//更新记录所属表的ID
	VTABLE_COL_ROWID,		//Heap Record的RID前像
	VTABLE_COL_VTABLEINDEX,	//Heap Record的vTableIndex前像
	VTABLE_COL_TXNID,		//Heap Record的txnId前像
	VTABLE_COL_DIFFFORMAT,	//Diff数据类型，0表示为SmallDiff，1表示为BigDiff
	VTABLE_COL_SMALLDIFF,	//小型更新内容Diff
	VTABLE_COL_BIGDIFF,		//大型更新内容Diff
	VTABLE_COL_LOB,         //更新内容中是否有大对象的更新
	VTABLE_COL_DELBIT,      //Heap Record的delBit前像
	VTABLE_COL_PUSHTXNID    //推送Heap Record入版本池的txnId
};

enum RollBackTBLCol {
	ROLLBACK_TABLE_COL_TXNID = 0
};

const char *VersionPool::SCHEMA_NAME = "SYS_TNTENGINE";
const char *VersionPool::VTABLE_NAME = "SYS_VersionPool";
const char *VersionPool::VTABLE_COLNAME[] = {"TableID", "RowID", "VTableIndex", "TxnID", "DiffFormat", "SmallDiff", "BigDiff", "Lob", "DelBit", "PushTxnId"};

const char *VersionPool::ROLLBACK_TABLE_NAME = "SYS_RollBack";

const char *VersionPool::ROLLBACK_TBL_COLNAME[] = {"DbTxnId"};

class TableHasher {
public:
	inline unsigned int operator()(const Table *table) const {
		return table->getTableDef()->m_id;
	}
};

template<typename T1, typename T2>
class TableEqualer {
public:
	inline bool operator()(const T1 &v1, const T2 &v2) const {
		return equals(v1, v2);
	}

private:
	static bool equals(const u16 &tableId, const Table* table) {
		return tableId == table->getTableDef()->m_id;
	}
};

typedef DynHash<u16, Table*, TableHasher, Hasher<u16>, TableEqualer<u16, Table*> > TableHash;

VersionPool::VersionPool(Database *db, VersionTables *vTables, u8 count)
{
	m_db = db;
	m_vTables = vTables;
	m_count = count;
	u16 totalCol = sizeof(VTABLE_COLNAME)/sizeof(char *);
	m_columns = new u16[totalCol]; //记录旧TableDef的列
	for (u16 i = 0; i < totalCol; ++i) {
		m_columns[i] = i;
	}
}

VersionPool::~VersionPool(void)
{
	delete[] m_columns;
	delete[] m_vTables;
}

/** 创建版本池中的某一个表
 * @param basePath 创建版本池中某一个表的路径
 * @param index 创建版本池中某一个表的序号
 */
void VersionPool::createVersionTable(Database *db, Session *session, const char *basePath, u8 index) throw (NtseException) {
	TableDef *vTblDef = NULL;
	MemoryContext *ctx = session->getMemoryContext();
	McSavepoint msp(ctx);
	size_t tableNameSize = max(strlen(VTABLE_NAME), strlen(ROLLBACK_TABLE_NAME)) + 10;
	char *tableName = (char *)ctx->alloc(tableNameSize);
	size_t pathSize = strlen(basePath) + tableNameSize + 20;
	char *path = (char *)ctx->alloc(pathSize);

	System::snprintf_mine(tableName, tableNameSize, "%s%d", VTABLE_NAME, index);
	System::snprintf_mine(path, pathSize, "%s/%s", basePath, tableName);
	vTblDef = createVersionTableDef(SCHEMA_NAME, tableName);
	db->createTable(session, path, vTblDef);
	delete vTblDef;

	System::snprintf_mine(tableName, tableNameSize, "%s%d", ROLLBACK_TABLE_NAME, index);
	System::snprintf_mine(path, pathSize, "%s/%s", basePath, tableName);
	vTblDef = createRollBackTableDef(SCHEMA_NAME, tableName);
	db->createTable(session, path, vTblDef);
	delete vTblDef;
}

/** 创建版本池中所有表
 * @param basePath 版本池中存放表的路径
 * @param count 版本池中记录表总个数
 */
void VersionPool::create(Database *db, Session *session, const char *basePath, u8 count) throw (NtseException) {
	for (u8 i = 0; i < count; i++) {
		createVersionTable(db, session, basePath, i);
	}
}

/** 设置版本池表是否有大对象的标志位
 * @param activeVersionPoolId 当前活跃版本池Id
 * @param hasLob	是否有大对象标志位
 */
void VersionPool::setVersionPoolHasLobBit(uint activeVersionPoolId, bool hasLob) {
	m_vTables[activeVersionPoolId].m_hasLob = hasLob;
}

/** 打开版本池的表
 * @param basePath 版本池表数据所在路径
 * @param count 版本池表的总个数
 * @param activeIndex 当前活跃的版本池表的序号
 */
VersionPool *VersionPool::open(Database *db, Session *session, const char *basePath, u8 count) throw (NtseException) {
	assert(count > 0);
	VersionTables *vTables = new VersionTables[count];
	memset(vTables, 0, count*sizeof(VersionTables));
	char *path = (char *)alloca(strlen(basePath) + 50);
	try {
		for (u8 i = 0; i < count; i++) {
			sprintf(path, "%s/%s%d", basePath, VTABLE_NAME, i);
			vTables[i].m_table = db->openTable(session, path);

			sprintf(path, "%s/%s%d", basePath, ROLLBACK_TABLE_NAME, i);
			vTables[i].m_rollBack = db->openTable(session, path);
			//vTables[i].m_pageNum = 0;
			vTables[i].m_hasLob = true;
		}
	} catch (NtseException &e) {
		for (u8 i = 0; i < count; i++) {
			if (vTables[i].m_table != NULL) {
				db->closeTable(session, vTables[i].m_table);
			}

			if (vTables[i].m_rollBack != NULL) {
				db->closeTable(session, vTables[i].m_rollBack);
			}
		}
		delete [] vTables;
		throw e;
	}

	return new VersionPool(db, vTables, count);
}

void VersionPool::close(Session *session) {
	for (int i = 0; i < m_count; i++) {
		m_db->closeTable(session, m_vTables[i].m_table);
		m_db->closeTable(session, m_vTables[i].m_rollBack);
	}
}

void VersionPool::drop(const char *baseDir, u8 count) {
	char path[200];
	for (int i = 0; i < count; i++) {
		sprintf(path, "%s%d", VTABLE_NAME, i);
		Table::drop(baseDir, path);
		//m_db->dropTable(session, path);

		sprintf(path, "%s%d", ROLLBACK_TABLE_NAME, i);
		Table::drop(baseDir, path);
		//m_db->dropTable(session, path);
	}
}

/** 创建版本池表的表定义
 * @param schemaName 关系名称
 * @param tableName 表名称
 * return 版本池表的表定义
 */
TableDef* VersionPool::createVersionTableDef(const char *schemaName, const char *tableName) {
	TableDef *vTableDef = NULL;

	TableDefBuilder *tbuilder = new TableDefBuilder(0, schemaName, tableName);
	tbuilder->addColumn(VTABLE_COLNAME[VTABLE_COL_TBLID], CT_SMALLINT, false);
	tbuilder->addColumn(VTABLE_COLNAME[VTABLE_COL_ROWID], CT_BIGINT, false);
	tbuilder->addColumn(VTABLE_COLNAME[VTABLE_COL_VTABLEINDEX], CT_TINYINT, false);
	tbuilder->addColumn(VTABLE_COLNAME[VTABLE_COL_TXNID], CT_BIGINT, false);
	tbuilder->addColumn(VTABLE_COLNAME[VTABLE_COL_DIFFFORMAT], CT_TINYINT, false);
	tbuilder->addColumnS(VTABLE_COLNAME[VTABLE_COL_SMALLDIFF], CT_VARCHAR, VERSIONPOOL_SMALLDIFF_MAX, false, true);
	tbuilder->addColumn(VTABLE_COLNAME[VTABLE_COL_BIGDIFF], CT_MEDIUMLOB, true);
	tbuilder->addColumn(VTABLE_COLNAME[VTABLE_COL_LOB], CT_TINYINT, false);
	tbuilder->addColumn(VTABLE_COLNAME[VTABLE_COL_DELBIT], CT_TINYINT, false);
	tbuilder->addColumn(VTABLE_COLNAME[VTABLE_COL_PUSHTXNID], CT_BIGINT, false);
	vTableDef = tbuilder->getTableDef();
	delete tbuilder;
	vTableDef->setTableStatus(TS_SYSTEM);
	vTableDef->setUseMms(false);
	return vTableDef;
}

/** 创建版本池中回滚表的表定义
 * @param schemaName 关系名称
 * @param tableName 表名称
 * return 版本池中回滚表的表定义
 */
TableDef* VersionPool::createRollBackTableDef(const char *schemaName, const char *tableName) {
	TableDef *vTableDef = NULL;

	TableDefBuilder *tbuilder = new TableDefBuilder(0, schemaName, tableName);
	tbuilder->addColumn(ROLLBACK_TBL_COLNAME[ROLLBACK_TABLE_COL_TXNID], CT_BIGINT, false);
	vTableDef = tbuilder->getTableDef();
	delete tbuilder;
	vTableDef->setTableStatus(TS_SYSTEM);
	vTableDef->setUseMms(false);
	return vTableDef;
}

u64 VersionPool::getDataLen(Session *session, u8 tblIndex) {
	u64 dataLen = 0;
	m_vTables[tblIndex].m_table->lockMeta(session, IL_S, -1, __FILE__, __LINE__);
	dataLen = m_vTables[tblIndex].m_table->getDataLength(session);
	m_vTables[tblIndex].m_table->unlockMeta(session, IL_S);
	return dataLen;
}

/** 向版本池中插入记录
 * @param session 会话
 * @param table 更新记录所属NTSE表
 * @param rollBackId 上一条的版本池记录的RID
 * @param vtableIndex 版本池表序号
 * @param txnId 这条版本池记录的事务号
 * @param update 更新前像
 * @param delBit 是否删除
 * return 插入记录的rowId
 */
RowId VersionPool::insert(Session *session, u8 tblIndex, TableDef *recTblDef, RowId rollBackId, u8 vTableIndex, TrxId txnId, SubRecord *update, u8 delBit, TrxId pushTxnId) {
	assert(update->m_format == REC_REDUNDANT);
	assert(recTblDef->m_id != TableDef::INVALID_TABLEID);
	bool log = false;
	TNTTransaction *trx = session->getTrans();
	if (trx != NULL && trx->isLogging()) {
		log = true;
		trx->disableLogging();
	}
	RowId rowId = 0;
	TableDef *vTblDef = m_vTables[tblIndex].m_table->getTableDef();
	MemoryContext *ctx = session->getMemoryContext();
	McSavepoint msp(ctx);

	Record *rec = new (ctx->alloc(sizeof(Record))) Record(INVALID_ROW_ID, REC_MYSQL, 
		(byte *)ctx->calloc(vTblDef->m_maxRecSize), vTblDef->m_maxRecSize);
	RedRecord redRec(vTblDef, rec, false);
	redRec.writeNumber(VTABLE_COL_TBLID, recTblDef->m_id);
	redRec.writeNumber(VTABLE_COL_ROWID, rollBackId);
	redRec.writeNumber(VTABLE_COL_VTABLEINDEX, vTableIndex);
	redRec.writeNumber(VTABLE_COL_TXNID, txnId);
	if (update->m_numCols == 0) {
		redRec.writeNumber(VTABLE_COL_DIFFFORMAT, SMALL_DIFF_FORMAT);
		redRec.setNull(VTABLE_COL_SMALLDIFF);
		redRec.setNull(VTABLE_COL_BIGDIFF);
	} else {
		size_t bufSize = RecordOper::getSubRecordSerializeSize(recTblDef, update, false);
		byte *buf = (byte *)ctx->alloc(bufSize);
		Stream s(buf, bufSize);
		RecordOper::serializeSubRecordMNR(&s, recTblDef, update, false);

		if (s.getSize() > VERSIONPOOL_SMALLDIFF_MAX) {
			redRec.writeNumber(VTABLE_COL_DIFFFORMAT, BIG_DIFF_FORMAT);
			redRec.setNull(VTABLE_COL_SMALLDIFF);
			redRec.writeLob(VTABLE_COL_BIGDIFF, buf, s.getSize());
		} else {
			redRec.writeNumber(VTABLE_COL_DIFFFORMAT, SMALL_DIFF_FORMAT);
			redRec.writeVarchar(VTABLE_COL_SMALLDIFF, buf, s.getSize());
			redRec.setNull(VTABLE_COL_BIGDIFF);
		}
	}

	u8 lob = 0;
	for (u32 i = 0; i < update->m_numCols; i++) {
		ColumnDef *columnDef = recTblDef->m_columns[update->m_columns[i]];
		if (columnDef->isLob()) {
			lob = 1;
			break;
		}
	}
	redRec.writeNumber(VTABLE_COL_LOB, lob);
	redRec.writeNumber(VTABLE_COL_DELBIT, delBit);
	redRec.writeNumber(VTABLE_COL_PUSHTXNID, pushTxnId);

	rowId = m_vTables[tblIndex].m_table->insert(session, redRec.getRecord()->m_data, true, NULL);
	if(!m_vTables[tblIndex].m_hasLob && lob > 0)
		m_vTables[tblIndex].m_hasLob = true;
	if (log) {
		trx->enableLogging();
	}
	return rowId;
}

/** 根据当前record记录和回滚的rowid恢复回滚的内存堆记录
 * @param vTableIndex 版本池中table的序号
 * @param tableId 记录更新前像所属的表定义
 * @param record 回滚前的record全像,为REC_REDUNDANT格式
 * @param rollBackId 回滚前像的rowId
 * @param ctx 上下文分配空间
 * return 回滚的内存堆记录，此时返回heapRec中rec的格式为REC_REDUNDANT
 */
MHeapRec *VersionPool::getRollBackHeapRec(Session *session, TableDef *recTblDef, u8 vTableIndex, Record *record, RowId rollBackId) {
	assert(record != NULL && record->m_format == REC_REDUNDANT);
	if (rollBackId == 0) {
		return NULL;
	}

	MemoryContext *ctx = session->getMemoryContext();
	MHeapRec *ret = new (ctx->alloc(sizeof(MHeapRec))) MHeapRec();
	memcpy(&ret->m_rec, record, sizeof(Record));
	Table *vTable = m_vTables[vTableIndex].m_table;
	TableDef *vTableDef = vTable->getTableDef();
	McSavepoint msp(ctx);

	// 记录
	Record mysqlRec(INVALID_ROW_ID, REC_MYSQL, (byte *)ctx->alloc(vTableDef->m_maxRecSize), vTableDef->m_maxRecSize);

	TblScan *scanHdl = vTable->positionScan(session, OP_READ, vTableDef->m_numCols, m_columns, true);
	if (vTable->getNext(scanHdl, mysqlRec.m_data, rollBackId)) {
		Record redRow(scanHdl->getCurrentRid(), scanHdl->getRedRow()->m_format, scanHdl->getRedRow()->m_data, scanHdl->getRedRow()->m_size);
		RedRecord redRec(vTableDef, &redRow, false);

		assert(redRec.readSmallInt(VTABLE_COL_TBLID) == recTblDef->m_id);
		u8 diffFormat = redRec.readTinyInt(VTABLE_COL_DIFFFORMAT);
		byte *diff = NULL;
		size_t diffSize = 0;
		if (diffFormat == SMALL_DIFF_FORMAT) {
			redRec.readVarchar(VTABLE_COL_SMALLDIFF, &diff, &diffSize);
		} else {
			assert(diffFormat == BIG_DIFF_FORMAT);
			//////以下代码是因为table层注释掉了afterFetch///////
			((Records::Scan *)scanHdl->getRecInfo())->afterFetch();
			////////////////////////////////////////////////////
			RedRecord mysqlRedRec(vTableDef, &mysqlRec, false);
			mysqlRedRec.readLob(VTABLE_COL_BIGDIFF, &diff, &diffSize);
		}

		ret->m_rollBackId = redRec.readBigInt(VTABLE_COL_ROWID);
		ret->m_vTableIndex = redRec.readTinyInt(VTABLE_COL_VTABLEINDEX);
		ret->m_txnId = (TrxId)redRec.readBigInt(VTABLE_COL_TXNID);
		ret->m_del = redRec.readTinyInt(VTABLE_COL_DELBIT);

		if (INVALID_ROW_ID != ret->m_rollBackId && diff != NULL) {
			Stream s(diff, diffSize);
			//反序列化后的subRecord为REC_REDUNDANT格式
			SubRecord *updateSubRec = RecordOper::unserializeSubRecordMNR(&s, recTblDef, ctx);
			//根据更新前像，回滚到该版本的完整记录
			RecordOper::updateRecordRR(recTblDef, record, updateSubRec);
		}
	} else {
		NTSE_ASSERT(false);
	}

	vTable->endScan(scanHdl);

	return ret;
}

/** 向回滚表中插入回滚的事务号
 * param txnId 回滚的事务号
 */
void VersionPool::rollBack(Session *session, u8 tblIndex, TrxId txnId) {
	//改用下面的方式，是为了减少每次rollBack需要new的缺点
	TableDef *rollBackTblDef = m_vTables[tblIndex].m_rollBack->getTableDef();
	MemoryContext *ctx = session->getMemoryContext();
	McSavepoint msp(ctx);

	Record *rec = new (ctx->alloc(sizeof(Record))) Record(INVALID_ROW_ID, REC_MYSQL, 
		(byte *)ctx->calloc(rollBackTblDef->m_maxRecSize), rollBackTblDef->m_maxRecSize);
	RedRecord redRec(rollBackTblDef, rec, false);
	redRec.writeNumber(ROLLBACK_TABLE_COL_TXNID, txnId);
	RowId rid = m_vTables[tblIndex].m_rollBack->insert(session, rec->m_data, true, NULL);
	assert(rid != INVALID_ROW_ID);
}

/** 读取指定版本池中回滚表中所有的回滚事务号
 * @param tableIndex 版本池的序号，注：序号从1开始
 * @param allRollBack out 回滚表中所有的回滚事务号
 */
void VersionPool::readAllRollBackTxnId(Session *session, u8 tableIndex, TrxIdHashMap *allRollBack) {
	MemoryContext *ctx = session->getMemoryContext();
	McSavepoint msp(ctx);
	Table *table = m_vTables[tableIndex].m_rollBack;
	TableDef *tblDef = table->getTableDef();
	// 记录
	Record mysqlRec(INVALID_ROW_ID, REC_MYSQL, (byte *)ctx->alloc(tblDef->m_maxRecSize), tblDef->m_maxRecSize);

	TblScan *scanHdl = table->tableScan(session, OP_READ, tblDef->m_numCols, m_columns, true);
	while (table->getNext(scanHdl, mysqlRec.m_data)) {
		RedRecord redRec(tblDef, &mysqlRec, false);
		//由于ntse中不存在set结构，所以为了查找更快，暂时采用map，同时将value值设为1，这样根据key找到的value值为1说明存在，为0说明不存在
		allRollBack->put(redRec.readBigInt(ROLLBACK_TABLE_COL_TXNID));
	}
	table->endScan(scanHdl);

}

/** 获取readView可见的完整版本信息
 * @param session 会话
 * @param recTblDef record记录表的定义
 * @param heapRec 内存堆中更新前的全像
 * @param readView 可见版本
 * @param rec [in out] [in]更新前内存rec的全像，为REC_REDUNDANT格式 [out]返回可见版本的全像，为REC_REDUNDANT格式
 * @param stat [out] 如果返回值为NULL(即TNTIM中不存在有效可见版本)，此时可能存在ntse对该readView可见，不可见和该记录被删除这三种情况。
 *                   其实版本池中肯定不存在del为1的记录
 * return readView可见的完整的版本记录。如果TNTIM中不存在有效的可见版本，返回NULL
 */
MHeapRec *VersionPool::getVersionRecord(Session *session, TableDef *recTblDef, MHeapRec *heapRec, ReadView *readView, Record *rec, HeapRecStat *stat) {
	assert(rec != NULL && rec->m_format == REC_REDUNDANT);
	if (heapRec->m_rollBackId == 0) {
		*stat = NTSE_VISIBLE;
		return NULL;
	}

	//Record *record = &heapRec->m_rec;
	u8 vTableIndex = heapRec->m_vTableIndex;
	bool found = false;
	Table *vTable = m_vTables[heapRec->m_vTableIndex].m_table;
	TableDef *vTableDef = vTable->getTableDef();

	MemoryContext *ctx = session->getMemoryContext();
	u64 sp1 = ctx->setSavepoint();
	MHeapRec *destHeapRec = new (ctx->alloc(sizeof(MHeapRec))) MHeapRec(heapRec->m_txnId, heapRec->m_rollBackId, vTableIndex, rec, heapRec->m_del);

	u64 sp2 = ctx->setSavepoint();
	// 记录
	Record mysqlRec(INVALID_ROW_ID, REC_MYSQL, (byte *)ctx->alloc(vTableDef->m_maxRecSize), vTableDef->m_maxRecSize);

	while (true) {
		vTable = m_vTables[destHeapRec->m_vTableIndex].m_table;
		vTableIndex = destHeapRec->m_vTableIndex;
		TblScan *scanHdl = vTable->positionScan(session, OP_READ, vTableDef->m_numCols, m_columns, true);
		while (vTableIndex == destHeapRec->m_vTableIndex) {
			NTSE_ASSERT(vTable->getNext(scanHdl, mysqlRec.m_data, destHeapRec->m_rollBackId));
			Record redRow(scanHdl->getCurrentRid(), scanHdl->getRedRow()->m_format, scanHdl->getRedRow()->m_data, scanHdl->getRedRow()->m_size);
			RedRecord redRec(vTableDef, &redRow, false);

			u8 diffFormat = redRec.readTinyInt(VTABLE_COL_DIFFFORMAT);
			byte *diff = NULL;
			size_t diffSize = 0;
			if (diffFormat == SMALL_DIFF_FORMAT) {
				redRec.readVarchar(VTABLE_COL_SMALLDIFF, &diff, &diffSize);
			} else {
				assert(diffFormat == BIG_DIFF_FORMAT);
				//////以下代码是因为table层注释掉了afterFetch///////
				((Records::Scan *)scanHdl->getRecInfo())->afterFetch();
				////////////////////////////////////////////////////
				RedRecord mysqlRedRec(vTableDef, &mysqlRec, false);
				mysqlRedRec.readLob(VTABLE_COL_BIGDIFF, &diff, &diffSize);
			}

			destHeapRec->m_rollBackId = redRec.readBigInt(VTABLE_COL_ROWID);
			destHeapRec->m_vTableIndex = redRec.readTinyInt(VTABLE_COL_VTABLEINDEX);
			if (INVALID_ROW_ID != destHeapRec->m_rollBackId && diff != NULL) {
				Stream s(diff, diffSize);
				//反序列化后的subRecord为REC_REDUNDANT格式
				SubRecord *updateSubRec = RecordOper::unserializeSubRecordMNR(&s, recTblDef, ctx);
				//根据更新前像，回滚到该版本的完整记录
				RecordOper::updateRecordRR(recTblDef, rec, updateSubRec);
			}

			destHeapRec->m_txnId = (TrxId)redRec.readBigInt(VTABLE_COL_TXNID);
			if (readView->isVisible(destHeapRec->m_txnId)) {
				found = true;
				destHeapRec->m_del = redRec.readTinyInt(VTABLE_COL_DELBIT);
				vTable->endScan(scanHdl);
				goto _Found;
			} else if (0 == destHeapRec->m_rollBackId || INVALID_ROW_ID == destHeapRec->m_rollBackId) {
				//TNT不存在有效可见版本，而且ntse记录对当前readview也不可见
				found = false;
				vTable->endScan(scanHdl);
				goto _Found;
			}
		}
		//版本池发生切换
		vTable->endScan(scanHdl);
	}

_Found:
	if (found) {
		ctx->resetToSavepoint(sp2);
		if (destHeapRec->m_rollBackId == INVALID_ROW_ID) {
			*stat = NTSE_VISIBLE;
		} else if (destHeapRec->m_del == FLAG_MHEAPREC_DEL) {
			*stat = DELETED;
			assert(false);
		} else {
			*stat = VALID;
		}
		destHeapRec->m_size = destHeapRec->getSerializeSize();
		return destHeapRec;
	} else {
		if (destHeapRec->m_rollBackId == 0) {
			*stat = NTSE_VISIBLE;
		} else {
			assert(destHeapRec->m_rollBackId == INVALID_ROW_ID);
			*stat = NTSE_UNVISIBLE;
		}
		ctx->resetToSavepoint(sp1);
		return NULL;
	}
}

/** 根据版本池表回收大对象，因为大对象是直接插入到外存的
 * @param tableIndex 需要回收的版本池表的序号，注：序号从1开始
 * @param allRollBackId 回滚事务号集合，因为事务被回滚，所以这时大对象已经被删除，没有必要去回收
 * @param exceptLobIds 在回收大对象时，重用的大对象集合。这些大对象不能被回收，否则会导致误删
 */
void VersionPool::defragLob(Session *session, u8 tableIndex, TrxIdHashMap *allRollBack) {
	TrxId txnId = 0;
	u16 tableId = 0;
	u8 format = 0;
	byte *diff = NULL;
	size_t size = 0;
	TableDef *tblDef  = NULL;
	SubRecord* sub = NULL;
	LobId lobId = 0;
	u64 lobReclaimCnt = 0;
	//Table *tables = NULL;
	MemoryContext *ctx = session->getMemoryContext();
	McSavepoint msp(ctx);
	Table *vTable = m_vTables[tableIndex].m_table;
	TableDef *vTblDef = vTable->getTableDef();
	TblLob tblLob;
	// 记录
	Record mysqlRec(INVALID_ROW_ID, REC_MYSQL, (byte *)ctx->alloc(vTblDef->m_maxRecSize), vTblDef->m_maxRecSize);
	TableHash openTables;
	Table *table = NULL;

	//记录大对象回收开始时间
	u32 defragLobStart = System::fastTime();

	TblScan *scanHdl = vTable->tableScan(session, OP_READ, vTblDef->m_numCols, m_columns, true);
	while (vTable->getNext(scanHdl, mysqlRec.m_data)) {
		McSavepoint msp1(ctx);
		Record redRow(scanHdl->getCurrentRid(), scanHdl->getRedRow()->m_format, scanHdl->getRedRow()->m_data, scanHdl->getRedRow()->m_size);
		RedRecord redRec(vTable->getTableDef(), &redRow, false);
		if (!redRec.readTinyInt(VTABLE_COL_LOB)) {
			continue;
		}

		txnId = redRec.readBigInt(VTABLE_COL_PUSHTXNID);
		if (allRollBack->get(txnId) != 0) {
			continue;
		}

		tableId = redRec.readSmallInt(VTABLE_COL_TBLID);
		format = redRec.readTinyInt(VTABLE_COL_DIFFFORMAT);
		if (format == SMALL_DIFF_FORMAT) {
			redRec.readVarchar(VTABLE_COL_SMALLDIFF, &diff, &size);
		} else {
			assert(format == BIG_DIFF_FORMAT);
			//////以下代码是因为table层注释掉了afterFetch///////
			((Records::Scan *)scanHdl->getRecInfo())->afterFetch();
			////////////////////////////////////////////////////
			RedRecord mysqlRedRec(vTable->getTableDef(), &mysqlRec, false);
			mysqlRedRec.readLob(VTABLE_COL_BIGDIFF, &diff, &size);
		}
		Stream s(diff, size);

		if ((table = openTables.get(tableId)) == NULL) {
			try {
				table = m_db->openTable(session, m_db->getControlFile()->getTablePath(tableId).c_str());
			} catch (NtseException &e) {
				UNREFERENCED_PARAMETER(e);
				continue;
			}
			openTables.put(table);
		}
		table->lockMeta(session, IL_S, -1, __FILE__, __LINE__);
		tblDef = table->getTableDef();
		//返回是冗余格式的subRecord
		sub = RecordOper::unserializeSubRecordMNR(&s, tblDef, session->getMemoryContext());

		for (u16 i = 0; i < sub->m_numCols; i++) {
			ColumnDef *columnDef = tblDef->m_columns[sub->m_columns[i]];
			if (!columnDef->isLob() || RecordOper::isNullR(tblDef, sub, sub->m_columns[i])) {
				continue;
			}
			lobId = RecordOper::readLobId(sub->m_data, columnDef);
			tblLob.m_lobId = lobId;
			tblLob.m_tableId = tableId;
		
			table->deleteLob(session, lobId);
		
			lobReclaimCnt++;
		}
		table->unlockMeta(session, IL_S);
	}
	vTable->endScan(scanHdl);

	//记录大对象回收时间并且打印信息
	u32 defragLobEnd = System::fastTime();
	m_db->getSyslog()->log(EL_LOG, "==> Defrag Lob and time is %d s", defragLobEnd - defragLobStart);
	m_db->getSyslog()->log(EL_LOG, "==> Reclaim Lob Result: %d lobs have been reclaimed", lobReclaimCnt);
 	m_status.m_reclaimLobCnt += lobReclaimCnt;
 	m_status.m_relaimLobTime += (defragLobEnd - defragLobStart);

	size = openTables.getSize();
	for (u16 i = 0; i < size; i++) {
		m_db->closeTable(session, openTables.getAt(i));
	}
}

/** 整理所有事务号小于minReadView的版本池表，以期该表再被使用
 * @param minReadView 可以被回收的最大事务号，一般是小于当前活跃事务的最小事务号和当前活跃事务的最小readView
 * @param exceptLobIds 在回收大对象时，重用的大对象集合。这些大对象不能被回收，否则会导致误删
 */
void VersionPool::defrag(Session *session, u8 tblIndex, bool isRecovering) {
	//恢复阶段不做大对象回收，只truncate版本池表
	//此处优化，当版本池中未有大对象时不需要遍历版本池表进行大对象回收

	//TODO：由于暂时没有更好的方法保证外存记录和大对象的一致性，因此暂时废弃版本池大对象回收
// 	if(!isRecovering && m_vTables[tblIndex].m_hasLob) {
// 		TrxIdHashMap allRollBack;
// 		readAllRollBackTxnId(session, tblIndex, &allRollBack);
// 		defragLob(session, tblIndex, &allRollBack);
// 	}

	try {
		u32 truncateStart = System::fastTime();
		//此处必须先truncate版本池表，防止崩溃恢复重做recalim时根据空的rollback表去删大对象
		session->getNtseDb()->truncateTable(session, m_vTables[tblIndex].m_table);
		session->getNtseDb()->truncateTable(session, m_vTables[tblIndex].m_rollBack);
		u32 truncateEnd = System::fastTime();
		m_db->getSyslog()->log(EL_LOG, "==> Reclaim Truncate Table and time is %d s", truncateEnd - truncateStart);
	} catch (NtseException &e) {	
		m_db->getSyslog()->log(EL_PANIC, "truncate table error and reason is %s", e.getMessage());
	}
}


/**
 * 获得版本池运行状态
 *
 * @return 版本池运行状态
 */
const VerpoolStatus& VersionPool::getStatus() {
	return m_status;
}
}