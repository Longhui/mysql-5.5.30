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
 * ��ȡCountTable����
 *
 * CREATE TABLE Count (
 *   ID bigint PRIMARY KEY,
 *   Count INT
 * );
 *
 * @param useMms ����ʹ��MMS
 * @return ����
 */
const TableDef* TNTCountTable::getTableDef(bool useMms) {
	if (useMms)
		return m_inst.m_tableDefMms;
	else
		return m_inst.m_tableDefNoMms;
}
/**
 * ����һ����¼
 * @param id ID��
 * @param count Count��
 * @return �����õļ�¼
 */
Record* TNTCountTable::createRecord(u64 id, int count) {
	RecordBuilder rb(TNTCountTable::getTableDef(true), RID(0, 0), REC_REDUNDANT);
	
	rb.appendBigInt(id);
	rb.appendInt(count);

	return rb.getRecord();
}

/**
 * ����������ΪdataSize�ļ�¼���뵽table��,��¼ID��startId��ʼ��������
 * @param table ���ݱ�
 * @param dataSize INOUT ����Ϊϣ�������ļ�¼�ܴ�С�����Ϊʵ�ʴ����ļ�¼�ܴ�С
 * @param startId ��ʼID
 * @return ��¼��
 */
uint TNTCountTable::populate(Session *session, TNTTable *table, TNTOpInfo *opInfo, u64 *dataSize, u64 startId) {
	assert(session->getTrans() != NULL);
	Record *rec;
	u64 volumnCnt = *dataSize;
	uint recCnt = 0;
	uint dupIdx;
	RowId rid;

	while (true) {
		// ������¼
		rec = createRecord((u64)recCnt, (int)startId++);
		if (volumnCnt < getRecordSize()) {
			freeRecord(rec);
			break;
		}
		// �����¼
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
* ��������ܴ�СΪdataSize�ļ�¼���뵽table��,
* ��¼ID����[minidx, minid]֮���������
* @param table ���ݱ�
* @param dataSize ��¼�ܴ�С
* @param maxid ���id 
* @param minid ��Сid
* @return ��¼��
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
		// ������¼
		u64 id = (u64)RandomGen::nextInt((u32)minId, (u32)maxId);
		rec = createRecord((u64)recCnt, (int)startId++);
		if (volumnCnt < getRecordSize()) {
			freeRecord(rec);
			break;
		}
		// �����¼
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
 * ��������
 * @param useMms �Ƿ�ʹ��Mms
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
 * ���¼�¼��ID��
 * @param rec �����¼�¼
 * @param id ��id
 * @return ����֮��ļ�¼
 */
Record* TNTCountTable::updateId(Record *rec, u64 id) {
	assert(rec->m_format == REC_REDUNDANT);
	const TableDef *tableDef = getTableDef(false);
	ColumnDef *columnDef = tableDef->m_columns[COUNT_ID_CNO];
	*(u64 *)(rec->m_data + columnDef->m_offset) = id;
	return rec;
}
/**
 * ���¼�¼��Count��
 * @param rec �����¼�¼
 * @param count ��count
 * @return ����֮��ļ�¼
 */
Record* TNTCountTable::updateCount(Record *rec, int count) {
	assert(rec->m_format == REC_REDUNDANT);
	const TableDef *tableDef = getTableDef(false);
	ColumnDef *columnDef = tableDef->m_columns[COUNT_COUNT_CNO];
	*(s32 *)(rec->m_data + columnDef->m_offset) = count;
	return rec;
}
/**
 * ���¼�¼
 * @param rec �����¼�¼
 * @param id ��id
 * @param count ��count
 * @return ����֮��ļ�¼
 */
Record* TNTCountTable::updateRecord(Record *rec, u64 id, int count) {
	assert(rec->m_format == REC_REDUNDANT);
	const TableDef *tableDef = getTableDef(false);
	*(u64 *)(rec->m_data + tableDef->m_columns[COUNT_ID_CNO]->m_offset) = id;
	*(s32 *)(rec->m_data + tableDef->m_columns[COUNT_COUNT_CNO]->m_offset) = count;
	return rec;
}
/**
 * ���¹ؼ���
 * @param key ������key����ʽΪKEY_PAD
 * @param id ��id
 * @return ����֮���key
 */
SubRecord* TNTCountTable::updateKey(SubRecord *key, u64 id) {
	assert(key->m_format == KEY_PAD);
	IndexDef *pkeyIndexDef = getTableDef(false)->m_pkey;
	*(u64 *)(key->m_data + pkeyIndexDef->m_offsets[0]) = id;
	return key;
}

/**
 * ��ɨ��
 * @param db ���ݿ�
 * @param table ��
 * @return ɨ����ļ�¼��
 */
u64 TNTCountTable::scanTable(TNTDatabase *db, TNTTable *table) {
	u16 columns[] = {COUNT_ID_CNO, COUNT_COUNT_CNO};
	return ::TNTTableScan(db, table, (u16)(sizeof(columns)/sizeof(columns[0])), columns);
}