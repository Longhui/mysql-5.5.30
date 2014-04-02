#include "BlogTable.h"
#include "misc/RecordHelper.h"
#include "Random.h"
#include <iostream>

using namespace std;
using namespace ntse;

BlogTable BlogTable::m_inst;

BlogTable::BlogTable() {
	m_tableDefMms = makeTableDef(true);
	m_tableDefNoMms = makeTableDef(false);
}

BlogTable::~BlogTable() {
	if (m_tableDefMms)
		delete m_tableDefMms;
	if (m_tableDefNoMms)
		delete m_tableDefNoMms;
}

/**
 *
 * @param useMms 是否使用Mms
 * @return 表定义
 */
const TableDef* BlogTable::getTableDef(bool useMms) {
	if (useMms)
		return m_inst.m_tableDefMms;
	else
		return m_inst.m_tableDefNoMms;
}

bool BlogTable::insertRecord(Session *session, Table *table, u64 id, char *buffer, RedRecord *rRecord, u32 minRecSize, u32 avgRecSize) {
	uint dupIndex;
	int recLen = RandomGen::randNorm((int)avgRecSize, (int)minRecSize);
	recLen -= sizeof(u64);

//	memset(buffer, recLen % 10 + 'a', recLen);
	rRecord->writeLob(BLOG_CONTENT_CNO, (byte *)buffer, (uint)recLen);
	rRecord->writeNumber(BLOG_ID_CNO, id);
	return INVALID_ROW_ID != table->insert(session, rRecord->getRecord()->m_data, &dupIndex);
}

bool BlogTable::insertRecord(Session *session, Table *table, u64 id, int *outRecSize, u32 minRecSize, u32 avgRecSize) {
	const TableDef *td = getTableDef(true);
	SubRecordBuilder srb(td, REC_REDUNDANT, RID(0, 0));
	SubRecord *subRec = srb.createSubRecordById("0 1 2 3 4 6", &id, NULL, NULL, NULL, NULL, NULL);
	int recLen = RandomGen::randNorm((int)avgRecSize, (int)minRecSize);
	if (outRecSize)
		*outRecSize = recLen;
	recLen -= sizeof(u64);
	char *content = new char[recLen];
	uint dupIndex;
	RowId rid;

	memset(content, 0, recLen);
//	memset(content, recLen % 10 + 'a', recLen);
	RecordOper::writeLob(subRec->m_data, td->m_columns[5], (byte *)content);
	RecordOper::writeLobSize(subRec->m_data, td->m_columns[5], (uint)recLen);
	Record rec(RID(0, 0), REC_MYSQL, subRec->m_data, td->m_maxRecSize);
	rec.m_format = REC_MYSQL;
	rid = table->insert(session, rec.m_data, &dupIndex);
	delete [] content;
	freeSubRecord(subRec);
	return rid != INVALID_ROW_ID;
}

/**
 * 随机构造总数量为dataSize的记录插入到table中,记录ID从startId开始连续分配
 * @param table 数据表
 * @param dataSize INOUT 输入为希望创建的记录总大小，输出为实际创建的记录总大小
 * @param startId 起始ID
 * @return 记录数
 */
uint BlogTable::populate(Session *session, Table *table, u64 *dataSize, u64 startId) {
	u64 volumnCnt = *dataSize;
	uint recCnt = 0;
	int outRecSize;

	while (true) {
		if (insertRecord(session, table, startId + recCnt++, &outRecSize)) {
			recCnt++;
			if (volumnCnt <= outRecSize) {
				*dataSize += (outRecSize - volumnCnt);
				break;
			}
			volumnCnt -= outRecSize;
		}
	}
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
uint BlogTable::populate(Session *session, Table *table, u64 *dataSize, u64 maxId, u64 minId) {
	u64 volumnCnt = *dataSize;
	uint recCnt = 0;
	int outRecSize;

	while (true) {
		if (insertRecord(session, table, (u64)RandomGen::nextInt((u32)minId, (u32)maxId), &outRecSize)) {
			recCnt++;
			if (volumnCnt <= outRecSize) {
				*dataSize += (outRecSize - volumnCnt);
				break;
			}
			volumnCnt -= outRecSize;
		}
	}
	return recCnt;
}

/**
 * 创建表定义
 * @param useMms 是否使用Mms
 */
TableDef* BlogTable::makeTableDef(bool useMms) {
	TableDefBuilder *builder;
	if (useMms)
		builder = new TableDefBuilder(TableDef::INVALID_TABLEID, "perfTest", "BlogUseMms");
	else
		builder = new TableDefBuilder(TableDef::INVALID_TABLEID, "perfTest", "BlogNotUseMms");
	builder->addColumn(BLOG_ID, CT_BIGINT, false)->addColumn(BLOG_USERID, CT_INT, true);
	builder->addColumn(BLOG_PUBLISHTIME, CT_BIGINT, true)->addColumnS(BLOG_TITLE, CT_VARCHAR, Limits::MAX_REC_SIZE - 200, true);
	builder->addColumn(BLOG_ABSTRACT, CT_MEDIUMLOB, true)->addColumn(BLOG_CONTENT, CT_MEDIUMLOB);
	builder->addColumnS(BLOG_PERMALINK, CT_VARCHAR, 128, true);
	builder->addIndex("PRIMARY", true, true, BLOG_ID, NULL);
	builder->addIndex("IDX BLOG PERMA", false, false, BLOG_PERMALINK, NULL);
	builder->addIndex("IDX BLOG PUTTIME", false, false, BLOG_USERID, BLOG_PUBLISHTIME, NULL);
	TableDef *tableDef = builder->getTableDef();
	tableDef->m_useMms = useMms;
	delete builder;
	return tableDef;
}

/**
* 更新关键字
* @param key 待更新key，格式为KEY_PAD
* @param id 新id
* @return 更新之后的key
*/
SubRecord* BlogTable::updateKey(SubRecord *key, u64 id) {
	assert(key->m_format == KEY_PAD);
	IndexDef *pkeyIndexDef = getTableDef(false)->m_pkey;
	*(u64 *)(key->m_data + pkeyIndexDef->m_offsets[0]) = id;
	return key;
}

