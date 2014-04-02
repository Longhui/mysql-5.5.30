#include "LongCharTable.h"
#include "misc/RecordHelper.h"
#include "TableHelper.h"

LongCharTable LongCharTable::m_inst;	/** 单例 */

/**
 * 获取表定义
 * @param useMms 是否使用Mms
 */
const TableDef* LongCharTable::getTableDef(bool useMMs) {
	if (useMMs)
		return m_inst.m_tableDefMms;
	else
		return m_inst.m_tableDefNoMms;
}

/**
 * 创建一条记录
 * @param id ID列
 */
Record* LongCharTable::createRecord(u64 id) {
	RecordBuilder rb(getTableDef(true), RID(0, 0), REC_REDUNDANT);
	int seed = (int)(id % 10 + 'a');
	char nm[NAME_LEN + 1];

	memset(nm, seed, NAME_LEN);
	*(nm + NAME_LEN) = '\0';
	rb.appendBigInt(id);
	rb.appendChar(nm);

	return rb.getRecord(getTableDef(true)->m_maxRecSize);
}


/**
 * 更新ID列
 * @param rec 待更新记录
 * @param id 新的id
 */
Record* LongCharTable::updateId(Record *rec, u64 id) {
	assert(rec->m_format == REC_REDUNDANT);
	const TableDef *tableDef = getTableDef(false);
	ColumnDef *columnDef = tableDef->m_columns[LONGCHAR_ID_CNO];
	*(u64 *)(rec->m_data + columnDef->m_offset) = id;
	return rec;
}

/**
 * 更新关键字
 * @param key 待更新key，格式为KEY_PAD
 * @param id 新id
 * @return 更新之后的key
 */
SubRecord* LongCharTable::updateKey(SubRecord *key, u64 id) {
	assert(key->m_format == KEY_PAD);
	IndexDef *pkeyIndexDef = getTableDef(false)->m_pkey;
	*(u64 *)(key->m_data + pkeyIndexDef->m_offsets[0]) = id;
	return key;
}


/**
 * 随机构造总大小为dataSize的记录插入到table中
 * 记录ID从startId开始递增生成
 * @param table 数据表
 * @param dataSize 记录总大小
 * @param startId 起始ID 
 * @return 记录数
 */
uint LongCharTable::populate(Session *session, Table *table, u64 *dataSize, u64 startId) {
	Record *rec;
	u64 volumnCnt = *dataSize;
	uint recCnt = 0;
	uint dupIdx;
	RowId rid;

	while (true) {
		// 创建记录
		rec = createRecord(startId++);
		if (volumnCnt < getRecordSize())
			break;
		// 插入记录
		rid = table->insert(session, rec->m_data, &dupIdx);
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
TableDef* LongCharTable::makeTableDef(bool useMms) {
	TableDefBuilder *builder;
	if (useMms)
		builder = new TableDefBuilder(TableDef::INVALID_TABLEID, "perfTest", "LongCharUseMms");
	else
		builder = new TableDefBuilder(TableDef::INVALID_TABLEID, "perfTest", "LongCharNotUseMms");
	builder->addColumn(LONGCHAR_ID, CT_BIGINT, false)->addColumnS(LONGCHAR_NAME, CT_CHAR, NAME_LEN, false);
	builder->addIndex("PRIMARY", true, true, LONGCHAR_ID, NULL);
	TableDef *tableDef = builder->getTableDef();
	tableDef->m_useMms = useMms;
	delete builder;
	return tableDef;
}

/** 初始化表定义 */
LongCharTable::LongCharTable() {
	m_tableDefMms = makeTableDef(true);
	m_tableDefNoMms = makeTableDef(false);
}

LongCharTable::~LongCharTable() {
	if (m_tableDefMms)
		delete m_tableDefMms;
	if (m_tableDefNoMms)
		delete m_tableDefNoMms;
}

/**
 * 表扫描
 * @param db 数据库
 * @param table 表
 * @return 扫描过的记录数
 */
u64 LongCharTable::scanTable(Database *db, Table *table) {
	u16 columns[] = {LONGCHAR_ID_CNO, LONGCHAR_NAME_CNO};
	return ::tableScan(db, table, (u16)(sizeof(columns)/sizeof(columns[0])), columns);
}

bool LongCharTable::insertRecord(Session *session, Table *table, Record *rec) {
	uint dupIdx;
	RowId rid = table->insert(session, rec->m_data, &dupIdx);
	return rid != INVALID_ROW_ID;
}

