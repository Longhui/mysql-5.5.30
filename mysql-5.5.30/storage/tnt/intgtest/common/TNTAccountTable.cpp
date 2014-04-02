#include "TNTAccountTable.h"
#include "misc/RecordHelper.h"
#include "Random.h"
#include "TNTTableHelper.h"

#include <iostream>

using namespace std;
using namespace tnt;

TNTAccountTable TNTAccountTable::m_inst;

TNTAccountTable::TNTAccountTable() {
	m_tableDefMms = makeTableDef(true);
	m_tableDefNoMms = makeTableDef(false);
}

TNTAccountTable::~TNTAccountTable() {
	if (m_tableDefMms)
		delete m_tableDefMms;
	if (m_tableDefNoMms)
		delete m_tableDefNoMms;
}

/**
 * ��ȡAccountTable����
 *
 * @param useMms ����ʹ��MMS
 * @return ����
 */
const TableDef* TNTAccountTable::getTableDef(bool useMms) {
	if (useMms)
		return m_inst.m_tableDefMms;
	else
		return m_inst.m_tableDefNoMms;
}
	
/**
 * ����һ����¼
 * @param id ID��
 * @param outRecSize ʵ�ʼ�¼����
 * @param minRecSize ��¼��С����
 * @param avgRecSize ƽ����¼����
 * @return ��¼
 */
Record* TNTAccountTable::createRecord(u64 id, int *outRecSize, u32 minRecSize, u32 avgRecSize) {
	assert(minRecSize >= 8);
	assert(2 * avgRecSize - minRecSize <= getTableDef(true)->m_maxRecSize);

	int recLen = RandomGen::randNorm((int)avgRecSize, (int)minRecSize);
	if (outRecSize)
		*outRecSize = recLen;
	// ����������ʣ�೤��
	recLen -= sizeof(u64);
	int nameLen = recLen * USERNAME_LEN / (USERNAME_LEN + PASSPORT_LEN);
	int passportLen = recLen - nameLen;

	// д��ID��
	RecordBuilder rb(getTableDef(true), RID(0, 0), REC_REDUNDANT);
	rb.appendBigInt(id);
	// NAME��
	char buffer[Limits::PAGE_SIZE];
	memset(buffer, nameLen % 10 + '0', nameLen);
	*(buffer + nameLen) = '\0';
	rb.appendVarchar(buffer);
	// PASSPORT ��
	memset(buffer, passportLen % 26 + 'a', passportLen);
	*(buffer + passportLen) = '\0';
	rb.appendVarchar(buffer);

	return rb.getRecord(getTableDef(true)->m_maxRecSize);
}
/**
 * ���¼�¼
 * @param rec �����¼�¼
 * @param ioutRecSized �Ѵ�����¼����
 * @param id ID����ֵ
 * @param outRecSize ʵ�ʼ�¼����
 * @param minRecSize ��¼��С����
 * @param avgRecSize ��¼ƽ������
 * @param ����֮��ļ�¼
 * @pre ��֤rec->m_dataռ�õ�ʵ���ڴ�>=getTableDef(true)->m_maxRecSize
 *		createRecord���������ļ�¼�����������
 */
Record* TNTAccountTable::updateRecord(Record *rec, u64 id, int *outRecSize, u32 minRecSize, u32 avgRecSize) {
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
 * ����ID��
 * @param rec �����¼�¼
 * @param id ID�е�ֵ
 */
Record* TNTAccountTable::updateId(Record *rec, u64 id) {
	assert(rec->m_format == REC_REDUNDANT);
	const TableDef *tableDef = getTableDef(false);
	*(u64 *)(rec->m_data + tableDef->m_columns[ACCOUNT_ID_CNO]->m_offset) = id;
	return rec;
}

/**
 * ���¹ؼ���
 * @param key ������key����ʽΪKEY_PAD
 * @param id ��id
 * @return ����֮���key
 */
SubRecord* TNTAccountTable::updateKey(SubRecord *key, u64 id) {
	assert(key->m_format == KEY_PAD);
	IndexDef *pkeyIndexDef = getTableDef(false)->m_pkey;
	*(u64 *)(key->m_data + pkeyIndexDef->m_offsets[0]) = id;
	return key;
}


// ˳������
uint TNTAccountTable::populate(Session *session, TNTTable *table, TNTOpInfo *opInfo, u64 *dataSize, u64 startId) {
	assert(session->getTrans() != NULL);
	Record *rec;
	u64 volumnCnt = *dataSize;
	uint recCnt = 0;
	uint dupIdx;
	RowId rid;
	int outRecSize;

	rec = createRecord(0, &outRecSize);
	while (true) {
		// ������¼
		rec = updateRecord(rec, startId++, &outRecSize);
		if (volumnCnt < outRecSize)
			break;
		// �����¼
		rid = table->insert(session, rec->m_data, &dupIdx, opInfo);
		if (rid != INVALID_ROW_ID) {
			recCnt++;
			volumnCnt -= outRecSize;
		}
	}
	freeRecord(rec);
	*dataSize -= volumnCnt;
	return recCnt;
}

// �������
uint TNTAccountTable::populate(Session *session, TNTTable *table, TNTOpInfo *opInfo, u64 *dataSize, u64 maxId, u64 minId) {
	assert(session->getTrans() != NULL);
	Record *rec;
	u64 volumnCnt = *dataSize;
	uint recCnt = 0;
	uint dupIdx;
	RowId rid;
	int outRecSize;

	rec = createRecord(0, &outRecSize);
	while (true) {
		// ������¼
		rec = updateRecord(rec, (u64)RandomGen::nextInt((int)minId, (int)maxId), &outRecSize);
		if (volumnCnt < outRecSize)
			break;
		// �����¼
		rid = table->insert(session, rec->m_data, &dupIdx, opInfo);
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
* ��������
* @param useMms �Ƿ�ʹ��Mms
*/
TableDef* TNTAccountTable::makeTableDef(bool useMms) {
	TableDefBuilder *builder;
	if(useMms)
		builder = new TableDefBuilder(TableDef::INVALID_TABLEID, "perfTest", "TNTAccountUseMms");
	else
		builder = new TableDefBuilder(TableDef::INVALID_TABLEID, "perfTest", "TNTAccountNotUseMms");

	builder->addColumn(ACCOUNT_ID, CT_BIGINT, false)->addColumnS(ACCOUNT_PASSPORTNAME, CT_VARCHAR, PASSPORT_LEN, false);
	builder->addColumnS(ACCOUNT_USERNAME, CT_VARCHAR, USERNAME_LEN, false);
	builder->addIndex("PRIMARY", true, true, false, ACCOUNT_ID, NULL);
	TableDef *tableDef = builder->getTableDef();
	tableDef->m_useMms = useMms;
	delete builder;
	return tableDef;
}

bool TNTAccountTable::insertRecord(Session *session, TNTTable *table, TNTOpInfo *opInfo, Record *rec) {
	assert(session->getTrans() != NULL);
	uint dupIdx;
	RowId rid = table->insert(session, rec->m_data, &dupIdx, opInfo);
	return rid != INVALID_ROW_ID;
}


/**
 * ��ɨ��
 * @param db ���ݿ�
 * @param table ��
 * @return ɨ����ļ�¼��
 */
u64 TNTAccountTable::scanTable(TNTDatabase *db, TNTTable *table) {
	u16 columns[] = {ACCOUNT_ID_CNO, ACCOUNT_PASSPORTNAME_CNO, ACCOUNT_USERNAME_CNO};
	return ::TNTTableScan(db, table, (u16)(sizeof(columns)/sizeof(columns[0])), columns);
}