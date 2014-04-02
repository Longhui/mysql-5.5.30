#include "TNTBlogTable.h"
#include "misc/RecordHelper.h"
#include "Random.h"
#include <iostream>

using namespace std;
using namespace ntse;

TNTBlogTable TNTBlogTable::m_inst;

TNTBlogTable::TNTBlogTable() {
	m_tableDefMms = makeTableDef(true);
	m_tableDefNoMms = makeTableDef(false);
}

TNTBlogTable::~TNTBlogTable() {
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
const TableDef* TNTBlogTable::getTableDef(bool useMms) {
	if (useMms)
		return m_inst.m_tableDefMms;
	else
		return m_inst.m_tableDefNoMms;
}

bool TNTBlogTable::insertRecord(Session *session, TNTTable *table, TNTOpInfo *opInfo, u64 id, char *buffer, RedRecord *rRecord, u32 minRecSize, u32 avgRecSize) {
	uint dupIndex;
	int recLen = RandomGen::randNorm((int)avgRecSize, (int)minRecSize);
	recLen -= sizeof(u64);

//	memset(buffer, recLen % 10 + 'a', recLen);
	rRecord->writeLob(BLOG_CONTENT_CNO, (byte *)buffer, (uint)recLen);
	rRecord->writeNumber(BLOG_ID_CNO, id);
	return INVALID_ROW_ID != table->insert(session, rRecord->getRecord()->m_data, &dupIndex, opInfo);
}

bool TNTBlogTable::insertRecord(Session *session, TNTTable *table, TNTOpInfo *opInfo, u64 id, int *outRecSize, u32 minRecSize, u32 avgRecSize) {
	const TableDef *td = getTableDef(true);
	SubRecordBuilder srb(td, REC_REDUNDANT, RID(0, 0));
	char* permilink = randomStr(128);
	SubRecord *subRec = srb.createSubRecordById("0 1 2 3 4 6", &id, NULL, NULL, NULL, NULL, permilink);
	int recLen = RandomGen::randNorm((int)avgRecSize, (int)minRecSize);
	if (outRecSize)
		*outRecSize = recLen;
	recLen -= sizeof(u64);
	char *content = new char[recLen];
	uint dupIndex;
	RowId rid = INVALID_ROW_ID;

	memset(content, 0, recLen);
//	memset(content, recLen % 10 + 'a', recLen);
	RecordOper::writeLob(subRec->m_data, td->m_columns[5], (byte *)content);
	RecordOper::writeLobSize(subRec->m_data, td->m_columns[5], (uint)recLen);
	Record rec(RID(0, 0), REC_MYSQL, subRec->m_data, td->m_maxRecSize);
	rec.m_format = REC_MYSQL;
	rid = table->insert(session, rec.m_data, &dupIndex, opInfo);
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
uint TNTBlogTable::populate(Session *session, TNTTable *table, TNTOpInfo *opInfo, u64 *dataSize, u64 startId) {
	u64 volumnCnt = *dataSize;
	uint recCnt = 0;
	int outRecSize;

	while (true) {
		if (insertRecord(session, table, opInfo, startId + (recCnt++)*2, &outRecSize)) {
// 			if (volumnCnt <= outRecSize) {
// 				*dataSize += (outRecSize - volumnCnt);
// 				break;
// 			}
			if(recCnt == 100000)
				break;
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
uint TNTBlogTable::populate(Session *session, TNTTable *table, TNTOpInfo *opInfo, u64 *dataSize, u64 maxId, u64 minId) {
	u64 volumnCnt = *dataSize;
	uint recCnt = 0;
	int outRecSize;

	while (true) {
		if (insertRecord(session, table, opInfo, (u64)RandomGen::nextInt((u32)minId, (u32)maxId), &outRecSize)) {
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
TableDef* TNTBlogTable::makeTableDef(bool useMms) {
	TableDefBuilder *builder;
	if (useMms)
		builder = new TableDefBuilder(TableDef::INVALID_TABLEID, "perfTest", "BlogUseMms");
	else
		builder = new TableDefBuilder(TableDef::INVALID_TABLEID, "perfTest", "BlogNotUseMms");
	builder->addColumn(BLOG_ID, CT_BIGINT, false)->addColumn(BLOG_USERID, CT_INT, true);
	builder->addColumn(BLOG_PUBLISHTIME, CT_BIGINT, true)->addColumnS(BLOG_TITLE, CT_VARCHAR, Limits::MAX_REC_SIZE - 200, true);
	builder->addColumn(BLOG_ABSTRACT, CT_MEDIUMLOB, true)->addColumn(BLOG_CONTENT, CT_MEDIUMLOB);
	builder->addColumnS(BLOG_PERMALINK, CT_VARCHAR, 128, true);
	builder->addIndex("PRIMARY", true, true, false, BLOG_ID, NULL);
	builder->addIndex("IDX BLOG PERMA", false, false, false, BLOG_PERMALINK, NULL);
	builder->addIndex("IDX BLOG PUTTIME", false, false, false, BLOG_USERID, BLOG_PUBLISHTIME, NULL);
	TableDef *tableDef = builder->getTableDef();
	tableDef->m_useMms = useMms;
	delete builder;
	return tableDef;
}

/**
* 更新主键关键字
* @param key 待更新key，格式为KEY_PAD
* @param id 新id
* @return 更新之后的key
*/
SubRecord* TNTBlogTable::updatePrimaryKey(SubRecord *key, u64 id) {
	assert(key->m_format == KEY_PAD);
	IndexDef *pkeyIndexDef = getTableDef(false)->m_pkey;
	*(u64 *)(key->m_data + pkeyIndexDef->m_offsets[0]) = id;
	return key;
}


SubRecord* TNTBlogTable::updateSecondKey(SubRecord *key, char* permailLink, size_t len) {
	assert(key->m_format == KEY_PAD);
	IndexDef *secondIndexDef = getTableDef(false)->getIndexDef(1);
	memcmp((key->m_data + secondIndexDef->m_offsets[6]), permailLink, len);
	return key;
}

char* TNTBlogTable::randomStr(size_t size) {
	char *s = new char[size + 1];
	for (size_t i = 0; i < size; i++)
		s[i] = (char )('A' + System::random() % 26);
	s[size] = '\0';
	return s;
}

/**
 * 创建一条记录
 * @param id ID列
 * @param outRecSize 实际记录长度
 * @param minRecSize 记录最小长度
 * @param avgRecSize 平均记录长度
 * @return 记录
 */
Record* TNTBlogTable::createRecord(u64 id, int *outRecSize, u32 minRecSize, u32 avgRecSize) {
	assert(minRecSize >= 8);
	assert(2 * avgRecSize - minRecSize <= getTableDef(true)->m_maxRecSize);

	const TableDef *td = getTableDef(true);
	SubRecordBuilder srb(td, REC_REDUNDANT, RID(0, 0));
	
	u64 userId = id;
	u64 pubTime = System::currentTimeMillis();
	char* title = randomStr(TITLE_LEN);
	char* permilink = randomStr(PERMILINK_LEN);
	SubRecord *subRec = srb.createSubRecordById("0 1 2 3 4 6", &id, &userId, &pubTime, title, NULL, permilink);
	int recLen = RandomGen::randNorm((int)avgRecSize, (int)minRecSize);
	if (outRecSize)
		*outRecSize = recLen;
	recLen -= sizeof(u64) * 3 + TITLE_LEN + PERMILINK_LEN;
	char *content = randomStr(recLen);
	uint dupIndex;
	RowId rid = INVALID_ROW_ID;

	memset(content, 0, recLen);
	//	memset(content, recLen % 10 + 'a', recLen);
	RecordOper::writeLob(subRec->m_data, td->m_columns[5], (byte *)content);
	RecordOper::writeLobSize(subRec->m_data, td->m_columns[5], (uint)recLen);
	Record* rec = new Record(RID(0, 0), REC_MYSQL, subRec->m_data, td->m_maxRecSize);
	rec->m_format = REC_MYSQL;
	delete [] content;
	delete [] title;
	delete [] permilink;
//	freeSubRecord(subRec);
	return rec;
}