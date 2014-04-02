#include "TNTCountTable.h"
#include "misc/RecordHelper.h"
#include "TNTTableHelper.h"
#include <iostream>
#include "Random.h"

TNTCountTable TNTCountTable::m_inst;

TNTCountTable::TNTCountTable() {
	m_tableDefMms = makeTableDef(true);
	m_tableDefNoMms = makeTableDef(false);
}

TNTCountTable::~TNTCountTable() {
	if (m_tableDefMms)
		delete m_tableDefMms;
	if (m_tableDefNoMms)
		delete m_tableDefNoMms;
}

/**
 * 获取CountTable表定义
 *
 * CREATE TABLE Count (
 *   ID bigint PRIMARY KEY,
 *   Count INT
 * );
 *
 * @param useMms 不是使用MMS
 * @return 表定义
 */
const TableDef* TNTCountTable::getTableDef(bool useMms) {
	if (useMms)
		return m_inst.m_tableDefMms;
	else
		return m_inst.m_tableDefNoMms;
}
/**
 * 创建一条记录
 * @param id ID列
 * @param count Count列
 * @return 创建好的记录
 */
Record* TNTCountTable::createRecord(u64 id, int count) {
	RecordBuilder rb(TNTCountTable::getTableDef(true), RID(0, 0), REC_REDUNDANT);
	
	rb.appendBigInt(id);
	rb.appendInt(count);

	return rb.getRecord();
}

/**
 * 构造总数量为dataSize的记录插入到table中,记录ID从startId开始连续分配
 * @param table 数据表
 * @param dataSize INOUT 输入为希望创建的记录总大小，输出为实际创建的记录总大小
 * @param startId 起始ID
 * @return 记录数
 */
uint TNTCountTable::populate(Session *session, TNTTable *table, TNTOpInfo *opInfo, u64 *dataSize, u64 startId) {
	assert(session->getTrans() != NULL);
	Record *rec;
	u64 volumnCnt = *dataSize;
	uint recCnt = 0;
	uint dupIdx;
	RowId rid;

	while (true) {
		// 创建记录
		rec = createRecord((u64)recCnt, (int)startId++);
		if (volumnCnt < getRecordSize()) {
			freeRecord(rec);
			break;
		}
		// 插入记录
		rid = table->insert(session, rec->m_data, &dupIdx, opInfo);
		if (rid != INVALID_ROW_ID) {
			recCnt++;
			volumnCnt -= getRecordSize();
		}
		freeRecord(rec);
	}

	*dataSize -= volumnCnt;
	return recCnt;
}

/**
* 随机构造总大小为dataSize的记录插入到table中,
* 记录ID从在[minidx, minid]之间随机生成
* @param table 数据表
* @param dataSize 记录总大小
* @param maxid 最大id 
* @param minid 最小id
* @return 记录数
*/
uint TNTCountTable::populate(Session *session, TNTTable *table, TNTOpInfo *opInfo, u64 *dataSize, u64 maxId, u64 minId) {
	assert(session->getTrans() != NULL);
	u64 volumnCnt = *dataSize;
	uint recCnt = 0;
	uint dupIdx;
	int startId = 0;
	Record *rec;
	RowId rid;
	while (true) {
		// 创建记录
		u64 id = (u64)RandomGen::nextInt((u32)minId, (u32)maxId);
		rec = createRecord((u64)recCnt, (int)startId++);
		if (volumnCnt < getRecordSize()) {
			freeRecord(rec);
			break;
		}
		// 插入记录
		rid = table->insert(session, rec->m_data, &dupIdx, opInfo);
		if (rid != INVALID_ROW_ID) {
			recCnt++;
			volumnCnt -= getRecordSize();
		}
		freeRecord(rec);
	}

	*dataSize -= volumnCnt;
	return recCnt;
}

/**
 * 创建表定义
 * @param useMms 是否使用Mms
 */
TableDef* TNTCountTable::makeTableDef(bool useMms) {
	TableDefBuilder *builder;
	if (useMms)
		builder = new TableDefBuilder(TableDef::INVALID_TABLEID, "perfTest", "TNTCountUseMms");
	else
		builder = new TableDefBuilder(TableDef::INVALID_TABLEID, "perfTest", "TNTCountNotUseMms");
	builder->addColumn(COUNT_ID, CT_BIGINT, false)->addColumn(COUNT_COUNT, CT_INT);
	builder->addIndex("PRIMARY", true, true, false, COUNT_ID, NULL);
	TableDef *tableDef = builder->getTableDef();
	tableDef->m_useMms = useMms;
	delete builder;
	return tableDef;
}
/**
 * 更新记录的ID列
 * @param rec 待更新记录
 * @param id 新id
 * @return 更新之后的记录
 */
Record* TNTCountTable::updateId(Record *rec, u64 id) {
	assert(rec->m_format == REC_REDUNDANT);
	const TableDef *tableDef = getTableDef(false);
	ColumnDef *columnDef = tableDef->m_columns[COUNT_ID_CNO];
	*(u64 *)(rec->m_data + columnDef->m_offset) = id;
	return rec;
}
/**
 * 更新记录的Count列
 * @param rec 待更新记录
 * @param count 新count
 * @return 更新之后的记录
 */
Record* TNTCountTable::updateCount(Record *rec, int count) {
	assert(rec->m_format == REC_REDUNDANT);
	const TableDef *tableDef = getTableDef(false);
	ColumnDef *columnDef = tableDef->m_columns[COUNT_COUNT_CNO];
	*(s32 *)(rec->m_data + columnDef->m_offset) = count;
	return rec;
}
/**
 * 更新记录
 * @param rec 待更新记录
 * @param id 新id
 * @param count 新count
 * @return 更新之后的记录
 */
Record* TNTCountTable::updateRecord(Record *rec, u64 id, int count) {
	assert(rec->m_format == REC_REDUNDANT);
	const TableDef *tableDef = getTableDef(false);
	*(u64 *)(rec->m_data + tableDef->m_columns[COUNT_ID_CNO]->m_offset) = id;
	*(s32 *)(rec->m_data + tableDef->m_columns[COUNT_COUNT_CNO]->m_offset) = count;
	return rec;
}
/**
 * 更新关键字
 * @param key 待更新key，格式为KEY_PAD
 * @param id 新id
 * @return 更新之后的key
 */
SubRecord* TNTCountTable::updateKey(SubRecord *key, u64 id) {
	assert(key->m_format == KEY_PAD);
	IndexDef *pkeyIndexDef = getTableDef(false)->m_pkey;
	*(u64 *)(key->m_data + pkeyIndexDef->m_offsets[0]) = id;
	return key;
}

/**
 * 表扫描
 * @param db 数据库
 * @param table 表
 * @return 扫描过的记录数
 */
u64 TNTCountTable::scanTable(TNTDatabase *db, TNTTable *table) {
	u16 columns[] = {COUNT_ID_CNO, COUNT_COUNT_CNO};
	return ::TNTTableScan(db, table, (u16)(sizeof(columns)/sizeof(columns[0])), columns);
}