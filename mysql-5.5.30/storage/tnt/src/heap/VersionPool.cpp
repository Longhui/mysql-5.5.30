/**��汾�����а汾��ʵ��
 * author �ö��� xindingfeng@corp.netease.com
 */
#include "heap/VersionPool.h"
#include "api/TNTDatabase.h"
#include "api/Table.h"
#include "misc/TableDef.h"
#include "misc/RecordHelper.h"
#include "misc/Record.h"
#include "misc/ControlFile.h"
//TODO �汾�ض�Ӧ��minTxnId��maxTxnId������controlfile�ṩ������activeIndex��minTxnId��maxTxnIdΪ���ͼ�¼ǰ����汾�ص�����id��
//ֻ���ڰ汾���л���ʱ����Ҫ�޸�ǰһ���汾�ص�maxTxnId����һ���汾�ص�minTxnId��ͬʱ��Ҫ���޸�����ˢ�����
//ֻ�е��汾�ص�maxTxnIdС��minReadView��ʱ�򣬸ð汾�ؿ��Ի��յġ��������ͼ�¼���汾�ص�ȫ���ύ�Ҷ��ɼ�

//���ڰ汾�ر�ļ������汾�ر�Ĳ�����Ҫ�л�ȡ�汾��¼��insert�ͻ��մ����
//���մ����ʱ���϶������л�ȡ�汾��¼��insert����,���Ի��մ��������ڲ��Ӱ汾�ر�������½���
//��ȡ�汾��¼��insert�ɰ汾�ص����Ծ�����һ���汾��¼�����룬�϶����ᱻ�޸ģ���������У��϶�������get����������Ҳ�������ް汾�ر���������²���
//���ڼӵĹ��ڰ汾�ر�������Ҫ��Ϊ��ͨ��ntse��assert����������
namespace tnt {
enum VTableCol {
	VTABLE_COL_TBLID = 0,	//���¼�¼�������ID
	VTABLE_COL_ROWID,		//Heap Record��RIDǰ��
	VTABLE_COL_VTABLEINDEX,	//Heap Record��vTableIndexǰ��
	VTABLE_COL_TXNID,		//Heap Record��txnIdǰ��
	VTABLE_COL_DIFFFORMAT,	//Diff�������ͣ�0��ʾΪSmallDiff��1��ʾΪBigDiff
	VTABLE_COL_SMALLDIFF,	//С�͸�������Diff
	VTABLE_COL_BIGDIFF,		//���͸�������Diff
	VTABLE_COL_LOB,         //�����������Ƿ��д����ĸ���
	VTABLE_COL_DELBIT,      //Heap Record��delBitǰ��
	VTABLE_COL_PUSHTXNID    //����Heap Record��汾�ص�txnId
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
	m_columns = new u16[totalCol]; //��¼��TableDef����
	for (u16 i = 0; i < totalCol; ++i) {
		m_columns[i] = i;
	}
}

VersionPool::~VersionPool(void)
{
	delete[] m_columns;
	delete[] m_vTables;
}

/** �����汾���е�ĳһ����
 * @param basePath �����汾����ĳһ�����·��
 * @param index �����汾����ĳһ��������
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

/** �����汾�������б�
 * @param basePath �汾���д�ű��·��
 * @param count �汾���м�¼���ܸ���
 */
void VersionPool::create(Database *db, Session *session, const char *basePath, u8 count) throw (NtseException) {
	for (u8 i = 0; i < count; i++) {
		createVersionTable(db, session, basePath, i);
	}
}

/** ���ð汾�ر��Ƿ��д����ı�־λ
 * @param activeVersionPoolId ��ǰ��Ծ�汾��Id
 * @param hasLob	�Ƿ��д�����־λ
 */
void VersionPool::setVersionPoolHasLobBit(uint activeVersionPoolId, bool hasLob) {
	m_vTables[activeVersionPoolId].m_hasLob = hasLob;
}

/** �򿪰汾�صı�
 * @param basePath �汾�ر���������·��
 * @param count �汾�ر���ܸ���
 * @param activeIndex ��ǰ��Ծ�İ汾�ر�����
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

/** �����汾�ر�ı���
 * @param schemaName ��ϵ����
 * @param tableName ������
 * return �汾�ر�ı���
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

/** �����汾���лع���ı���
 * @param schemaName ��ϵ����
 * @param tableName ������
 * return �汾���лع���ı���
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

/** ��汾���в����¼
 * @param session �Ự
 * @param table ���¼�¼����NTSE��
 * @param rollBackId ��һ���İ汾�ؼ�¼��RID
 * @param vtableIndex �汾�ر����
 * @param txnId �����汾�ؼ�¼�������
 * @param update ����ǰ��
 * @param delBit �Ƿ�ɾ��
 * return �����¼��rowId
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

/** ���ݵ�ǰrecord��¼�ͻع���rowid�ָ��ع����ڴ�Ѽ�¼
 * @param vTableIndex �汾����table�����
 * @param tableId ��¼����ǰ�������ı���
 * @param record �ع�ǰ��recordȫ��,ΪREC_REDUNDANT��ʽ
 * @param rollBackId �ع�ǰ���rowId
 * @param ctx �����ķ���ռ�
 * return �ع����ڴ�Ѽ�¼����ʱ����heapRec��rec�ĸ�ʽΪREC_REDUNDANT
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

	// ��¼
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
			//////���´�������Ϊtable��ע�͵���afterFetch///////
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
			//�����л����subRecordΪREC_REDUNDANT��ʽ
			SubRecord *updateSubRec = RecordOper::unserializeSubRecordMNR(&s, recTblDef, ctx);
			//���ݸ���ǰ�񣬻ع����ð汾��������¼
			RecordOper::updateRecordRR(recTblDef, record, updateSubRec);
		}
	} else {
		NTSE_ASSERT(false);
	}

	vTable->endScan(scanHdl);

	return ret;
}

/** ��ع����в���ع��������
 * param txnId �ع��������
 */
void VersionPool::rollBack(Session *session, u8 tblIndex, TrxId txnId) {
	//��������ķ�ʽ����Ϊ�˼���ÿ��rollBack��Ҫnew��ȱ��
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

/** ��ȡָ���汾���лع��������еĻع������
 * @param tableIndex �汾�ص���ţ�ע����Ŵ�1��ʼ
 * @param allRollBack out �ع��������еĻع������
 */
void VersionPool::readAllRollBackTxnId(Session *session, u8 tableIndex, TrxIdHashMap *allRollBack) {
	MemoryContext *ctx = session->getMemoryContext();
	McSavepoint msp(ctx);
	Table *table = m_vTables[tableIndex].m_rollBack;
	TableDef *tblDef = table->getTableDef();
	// ��¼
	Record mysqlRec(INVALID_ROW_ID, REC_MYSQL, (byte *)ctx->alloc(tblDef->m_maxRecSize), tblDef->m_maxRecSize);

	TblScan *scanHdl = table->tableScan(session, OP_READ, tblDef->m_numCols, m_columns, true);
	while (table->getNext(scanHdl, mysqlRec.m_data)) {
		RedRecord redRec(tblDef, &mysqlRec, false);
		//����ntse�в�����set�ṹ������Ϊ�˲��Ҹ��죬��ʱ����map��ͬʱ��valueֵ��Ϊ1����������key�ҵ���valueֵΪ1˵�����ڣ�Ϊ0˵��������
		allRollBack->put(redRec.readBigInt(ROLLBACK_TABLE_COL_TXNID));
	}
	table->endScan(scanHdl);

}

/** ��ȡreadView�ɼ��������汾��Ϣ
 * @param session �Ự
 * @param recTblDef record��¼��Ķ���
 * @param heapRec �ڴ���и���ǰ��ȫ��
 * @param readView �ɼ��汾
 * @param rec [in out] [in]����ǰ�ڴ�rec��ȫ��ΪREC_REDUNDANT��ʽ [out]���ؿɼ��汾��ȫ��ΪREC_REDUNDANT��ʽ
 * @param stat [out] �������ֵΪNULL(��TNTIM�в�������Ч�ɼ��汾)����ʱ���ܴ���ntse�Ը�readView�ɼ������ɼ��͸ü�¼��ɾ�������������
 *                   ��ʵ�汾���п϶�������delΪ1�ļ�¼
 * return readView�ɼ��������İ汾��¼�����TNTIM�в�������Ч�Ŀɼ��汾������NULL
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
	// ��¼
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
				//////���´�������Ϊtable��ע�͵���afterFetch///////
				((Records::Scan *)scanHdl->getRecInfo())->afterFetch();
				////////////////////////////////////////////////////
				RedRecord mysqlRedRec(vTableDef, &mysqlRec, false);
				mysqlRedRec.readLob(VTABLE_COL_BIGDIFF, &diff, &diffSize);
			}

			destHeapRec->m_rollBackId = redRec.readBigInt(VTABLE_COL_ROWID);
			destHeapRec->m_vTableIndex = redRec.readTinyInt(VTABLE_COL_VTABLEINDEX);
			if (INVALID_ROW_ID != destHeapRec->m_rollBackId && diff != NULL) {
				Stream s(diff, diffSize);
				//�����л����subRecordΪREC_REDUNDANT��ʽ
				SubRecord *updateSubRec = RecordOper::unserializeSubRecordMNR(&s, recTblDef, ctx);
				//���ݸ���ǰ�񣬻ع����ð汾��������¼
				RecordOper::updateRecordRR(recTblDef, rec, updateSubRec);
			}

			destHeapRec->m_txnId = (TrxId)redRec.readBigInt(VTABLE_COL_TXNID);
			if (readView->isVisible(destHeapRec->m_txnId)) {
				found = true;
				destHeapRec->m_del = redRec.readTinyInt(VTABLE_COL_DELBIT);
				vTable->endScan(scanHdl);
				goto _Found;
			} else if (0 == destHeapRec->m_rollBackId || INVALID_ROW_ID == destHeapRec->m_rollBackId) {
				//TNT��������Ч�ɼ��汾������ntse��¼�Ե�ǰreadviewҲ���ɼ�
				found = false;
				vTable->endScan(scanHdl);
				goto _Found;
			}
		}
		//�汾�ط����л�
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

/** ���ݰ汾�ر���մ������Ϊ�������ֱ�Ӳ��뵽����
 * @param tableIndex ��Ҫ���յİ汾�ر����ţ�ע����Ŵ�1��ʼ
 * @param allRollBackId �ع�����ż��ϣ���Ϊ���񱻻ع���������ʱ������Ѿ���ɾ����û�б�Ҫȥ����
 * @param exceptLobIds �ڻ��մ����ʱ�����õĴ���󼯺ϡ���Щ������ܱ����գ�����ᵼ����ɾ
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
	// ��¼
	Record mysqlRec(INVALID_ROW_ID, REC_MYSQL, (byte *)ctx->alloc(vTblDef->m_maxRecSize), vTblDef->m_maxRecSize);
	TableHash openTables;
	Table *table = NULL;

	//��¼�������տ�ʼʱ��
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
			//////���´�������Ϊtable��ע�͵���afterFetch///////
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
		//�����������ʽ��subRecord
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

	//��¼��������ʱ�䲢�Ҵ�ӡ��Ϣ
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

/** �������������С��minReadView�İ汾�ر����ڸñ��ٱ�ʹ��
 * @param minReadView ���Ա����յ��������ţ�һ����С�ڵ�ǰ��Ծ�������С����ź͵�ǰ��Ծ�������СreadView
 * @param exceptLobIds �ڻ��մ����ʱ�����õĴ���󼯺ϡ���Щ������ܱ����գ�����ᵼ����ɾ
 */
void VersionPool::defrag(Session *session, u8 tblIndex, bool isRecovering) {
	//�ָ��׶β����������գ�ֻtruncate�汾�ر�
	//�˴��Ż������汾����δ�д����ʱ����Ҫ�����汾�ر���д�������

	//TODO��������ʱû�и��õķ�����֤����¼�ʹ�����һ���ԣ������ʱ�����汾�ش�������
// 	if(!isRecovering && m_vTables[tblIndex].m_hasLob) {
// 		TrxIdHashMap allRollBack;
// 		readAllRollBackTxnId(session, tblIndex, &allRollBack);
// 		defragLob(session, tblIndex, &allRollBack);
// 	}

	try {
		u32 truncateStart = System::fastTime();
		//�˴�������truncate�汾�ر���ֹ�����ָ�����recalimʱ���ݿյ�rollback��ȥɾ�����
		session->getNtseDb()->truncateTable(session, m_vTables[tblIndex].m_table);
		session->getNtseDb()->truncateTable(session, m_vTables[tblIndex].m_rollBack);
		u32 truncateEnd = System::fastTime();
		m_db->getSyslog()->log(EL_LOG, "==> Reclaim Truncate Table and time is %d s", truncateEnd - truncateStart);
	} catch (NtseException &e) {	
		m_db->getSyslog()->log(EL_PANIC, "truncate table error and reason is %s", e.getMessage());
	}
}


/**
 * ��ð汾������״̬
 *
 * @return �汾������״̬
 */
const VerpoolStatus& VersionPool::getStatus() {
	return m_status;
}
}