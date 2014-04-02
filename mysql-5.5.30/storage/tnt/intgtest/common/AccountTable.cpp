#include "AccountTable.h"
#include "Random.h"
#include "misc/RecordHelper.h"
#include "TableHelper.h"


#include <iostream>

using namespace std;
using namespace ntse;



AccountTable AccountTable::m_inst;


AccountTable::AccountTable() {
	m_tableDefMms = makeTableDef(true);
	m_tableDefNoMms = makeTableDef(false);
}

AccountTable::~AccountTable() {
	if (m_tableDefMms)
		delete m_tableDefMms;
	if (m_tableDefNoMms)
		delete m_tableDefNoMms;
}

/**
 * 获取AccountTable表定义
 *
 * @param useMms 不是使用MMS
 * @return 表定义
 */
const TableDef* AccountTable::getTableDef(bool useMms) {
	if (useMms)
		return m_inst.m_tableDefMms;
	else
		return m_inst.m_tableDefNoMms;
}
	
/**
 * 创建一条记录
 * @param id ID列
 * @param outRecSize 实际记录长度
 * @param minRecSize 记录最小长度
 * @param avgRecSize 平均记录长度
 * @return 记录
 */
Record* AccountTable::createRecord(u64 id, int *outRecSize, u32 minRecSize, u32 avgRecSize) {
	assert(minRecSize >= 8);
	assert(2 * avgRecSize - minRecSize <= getTableDef(true)->m_maxRecSize);

	int recLen = RandomGen::randNorm((int)avgRecSize, (int)minRecSize);
	if (outRecSize)
		*outRecSize = recLen;
	// 按比例分配剩余长度
	recLen -= sizeof(u64);
	int nameLen = recLen * USERNAME_LEN / (USERNAME_LEN + PASSPORT_LEN);
	int passportLen = recLen - nameLen;

	// 写入ID列
	RecordBuilder rb(getTableDef(true), RID(0, 0), REC_REDUNDANT);
	rb.appendBigInt(id);
	// NAME列
	char buffer[Limits::PAGE_SIZE];
	memset(buffer, nameLen % 10 + '0', nameLen);
	*(buffer + nameLen) = '\0';
	rb.appendVarchar(buffer);
	// PASSPORT 列
	memset(buffer, passportLen % 26 + 'a', passportLen);
	*(buffer + passportLen) = '\0';
	rb.appendVarchar(buffer);

	return rb.getRecord(getTableDef(true)->m_maxRecSize);
}
/**
 * 更新记录
 * @param rec 待更新记录
 * @param ioutRecSized 已创建记录长度
 * @param id ID列在值
 * @param outRecSize 实际记录长度
 * @param minRecSize 记录最小长度
 * @param avgRecSize 记录平均长度
 * @param 更新之后的记录
 * @pre 保证rec->m_data占用的实际内存>=getTableDef(true)->m_maxRecSize
 *		createRecord函数创建的记录都满足该条件
 */
Record* AccountTable::updateRecord(Record *rec, u64 id, int *outRecSize, u32 minRecSize, u32 avgRecSize) {
	assert(rec->m_format == REC_REDUNDANT);
	assert(minRecSize >= sizeof(u64) + 2);

	const TableDef *tableDef = getTableDef(false);
	*(u64 *)(rec->m_data + tableDef->m_columns[ACCOUNT_ID_CNO]->m_offset) = id;
	//TODO:
	int recLen = RandomGen::randNorm((int)avgRecSize, (int)minRecSize);
	if (outRecSize)
		*outRecSize = recLen;

	recLen -= sizeof(u64);
	int nameLen = recLen * USERNAME_LEN / (USERNAME_LEN + PASSPORT_LEN);
	int passportLen = recLen - nameLen;
	RedRecord::writeVarcharLen(tableDef, rec->m_data, ACCOUNT_USERNAME_CNO, (size_t)nameLen);
	RedRecord::writeVarcharLen(tableDef, rec->m_data, ACCOUNT_PASSPORTNAME_CNO, (size_t)(passportLen));
	
	return rec;
}


/**
 * 更新ID列
 * @param rec 待更新记录
 * @param id ID列的值
 */
Record* AccountTable::updateId(Record *rec, u64 id) {
	assert(rec->m_format == REC_REDUNDANT);
	const TableDef *tableDef = getTableDef(false);
	*(u64 *)(rec->m_data + tableDef->m_columns[ACCOUNT_ID_CNO]->m_offset) = id;
	return rec;
}

/**
 * 更新关键字
 * @param key 待更新key，格式为KEY_PAD
 * @param id 新id
 * @return 更新之后的key
 */
SubRecord* AccountTable::updateKey(SubRecord *key, u64 id) {
	assert(key->m_format == KEY_PAD);
	IndexDef *pkeyIndexDef = getTableDef(false)->m_pkey;
	*(u64 *)(key->m_data + pkeyIndexDef->m_offsets[0]) = id;
	return key;
}


// 顺序生成
uint AccountTable::populate(Session *session, Table *table, u64 *dataSize, u64 startId) {
	Record *rec;
	u64 volumnCnt = *dataSize;
	uint recCnt = 0;
	uint dupIdx;
	RowId rid;
	int outRecSize;

	rec = createRecord(0, &outRecSize);
	while (true) {
		// 创建记录
		rec = updateRecord(rec, startId++, &outRecSize);
		if (volumnCnt < outRecSize)
			break;
		// 插入记录
		rid = table->insert(session, rec->m_data, &dupIdx);
		if (rid != INVALID_ROW_ID) {
			recCnt++;
			volumnCnt -= outRecSize;
		}
	}
	freeRecord(rec);
	*dataSize -= volumnCnt;
	return recCnt;
}

// 随机生成
uint AccountTable::populate(Session *session, Table *table, u64 *dataSize, u64 maxId, u64 minId) {
	Record *rec;
	u64 volumnCnt = *dataSize;
	uint recCnt = 0;
	uint dupIdx;
	RowId rid;
	int outRecSize;

	rec = createRecord(0, &outRecSize);
	while (true) {
		// 创建记录
		rec = updateRecord(rec, (u64)RandomGen::nextInt((int)minId, (int)maxId), &outRecSize);
		if (volumnCnt < outRecSize)
			break;
		// 插入记录
		rid = table->insert(session, rec->m_data, &dupIdx);
		if (rid != INVALID_ROW_ID) {
			recCnt++;
			volumnCnt -= outRecSize;
		}
	}
	freeRecord(rec);
	*dataSize -= volumnCnt;
	return recCnt;	
}

/**
* 创建表定义
* @param useMms 是否使用Mms
*/
TableDef* AccountTable::makeTableDef(bool useMms) {
	TableDefBuilder *builder;
	if(useMms)
		builder = new TableDefBuilder(TableDef::INVALID_TABLEID, "perfTest", "AccountUseMms");
	else
		builder = new TableDefBuilder(TableDef::INVALID_TABLEID, "perfTest", "AccountNotUseMms");

	builder->addColumn(ACCOUNT_ID, CT_BIGINT, false)->addColumnS(ACCOUNT_PASSPORTNAME, CT_VARCHAR, PASSPORT_LEN, false);
	builder->addColumnS(ACCOUNT_USERNAME, CT_VARCHAR, USERNAME_LEN, false);
	builder->addIndex("PRIMARY", true, true, ACCOUNT_ID, NULL);
	TableDef *tableDef = builder->getTableDef();
	tableDef->m_useMms = useMms;
	delete builder;
	return tableDef;
}

bool AccountTable::insertRecord(Session *session, Table *table, Record *rec) {
	uint dupIdx;
	RowId rid = table->insert(session, rec->m_data, &dupIdx);
	return rid != INVALID_ROW_ID;
}


/**
 * 表扫描
 * @param db 数据库
 * @param table 表
 * @return 扫描过的记录数
 */
u64 AccountTable::scanTable(Database *db, Table *table) {
	u16 columns[] = {ACCOUNT_ID_CNO, ACCOUNT_PASSPORTNAME_CNO, ACCOUNT_USERNAME_CNO};
	return ::tableScan(db, table, (u16)(sizeof(columns)/sizeof(columns[0])), columns);
}
