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
 * @param useMms �Ƿ�ʹ��Mms
 * @return ����
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
 * �������������ΪdataSize�ļ�¼���뵽table��,��¼ID��startId��ʼ��������
 * @param table ���ݱ�
 * @param dataSize INOUT ����Ϊϣ�������ļ�¼�ܴ�С�����Ϊʵ�ʴ����ļ�¼�ܴ�С
 * @param startId ��ʼID
 * @return ��¼��
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
 * ��������ܴ�СΪdataSize�ļ�¼���뵽table��,
 * ��¼ID����[minidx, minid]֮���������
 * @param table ���ݱ�
 * @param dataSize ��¼�ܴ�С
 * @param maxid ���id 
 * @param minid ��Сid
 * @return ��¼��
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
 * ��������
 * @param useMms �Ƿ�ʹ��Mms
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
* ���������ؼ���
* @param key ������key����ʽΪKEY_PAD
* @param id ��id
* @return ����֮���key
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
 * ����һ����¼
 * @param id ID��
 * @param outRecSize ʵ�ʼ�¼����
 * @param minRecSize ��¼��С����
 * @param avgRecSize ƽ����¼����
 * @return ��¼
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