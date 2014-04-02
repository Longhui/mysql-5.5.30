/**
* �������Ա�:Paper
*
* @author zx(zx@163.org)
*/
#include "PaperTable.h"
#include "misc/RecordHelper.h"
#include <iostream>
#include <set>
#include <Random.h>

using namespace std;
using namespace ntse;


/**
 * ��ȡPaper����
 *
 * CREATE TABLE Paper (
 *   ID bigint PRIMARY KEY,
 *   Content mediumtext
 * );
 */

/**
 * ���ɳ���Ϊlen�Ĵ����
 *
 * @param len
 */
byte* creatLob (uint len){
	char *abs = new char[len];
	for (uint i = 0; i < len; i++ ) {
		abs[i] = (char )('A' + System::random() % 10);
	}
	return (byte *)abs;
}

PaperTable PaperTable::m_inst;

PaperTable::PaperTable() {
	m_tableDef = makeTableDef();
}

PaperTable::~PaperTable() {
	if (m_tableDef)
		delete m_tableDef;
}

/**
 * ��ȡCountTable����
 *
 * @param useMms ����ʹ��MMS
 * @return ����
 */
const TableDef* PaperTable::getTableDef() {
	return m_inst.m_tableDef;
}


/**
 * ����һ����¼
 * @param id ID��
 * @param len content����У�lob���ĳ���
 * @param table �������Ϊ�����д��Ҫ�����
 */
Record* PaperTable::createRecord(u64 id, uint len, Table *table) {
	RecordBuilder rb(PaperTable::getTableDef(), RID(0, 0), REC_REDUNDANT);
	rb.appendBigInt(id);
	Record *rec = rb.getRecord(PaperTable::getTableDef()->m_maxRecSize);
	rec->m_format = REC_MYSQL;
	RecordOper::writeLob(rec->m_data, table->getTableDef()->m_columns[1], creatLob(len));
	RecordOper::writeLobSize(rec->m_data, table->getTableDef()->m_columns[1], len);
	return rec;
}

/**
 * �����������ΪsizeCnt��С��¼
 * @param session �Ự����
 * @param table ���ݱ�
 * @param sizeCnt INOUT ����Ϊϣ�������ļ�¼�ܴ�С�����Ϊʵ�ʴ����ļ�¼�ܴ�С
 * @param len lob�еĳ���
 * @param recCount ��¼��
 * @return ��¼����
 */
Record** PaperTable::populate(Session *session, Table *table, u64 *sizeCnt, uint len, u64 recCount) {
	UNREFERENCED_PARAMETER(session);
	u64 volumnCnt = *sizeCnt;
	Record **records = new Record *[(size_t)recCount];
	uint recCnt = 0;
	u64 recNum = recCount;
	while (recNum > 0) {
		// ������¼
		records[recCnt] = createRecord((u64)recCnt, len, table);
		recCnt++;
		volumnCnt -= (sizeof (u64) + len);
		recNum--;
	}

	*sizeCnt -= volumnCnt;
	return records;
}

/**
 * �����������ΪsizeCnt��С�ļ�¼��������ҪΪ��ÿ�������߳�׼��һ����¼
 * @param session �Ự����
 * @param table �����
 * @param len lob�еĳ���
 * @param recCount ��¼��
 * @return ��¼����
 */
Record** PaperTable::populate(Session *session, Table *table, uint len, u64 recCount){
	UNREFERENCED_PARAMETER(session);
	Record **records = new Record *[(size_t)recCount];
	uint recCnt = 0;
	u64 recNum = recCount;
	while (recNum > 0) {
		// ������¼
		records[recCnt] = createRecord((u64)recCnt, len, table);
		recCnt++;
		recNum--;
	}
	return records;
}

/**
 * �����������ΪdataSize��С�ļ�¼�����������ݱ���
 * @param session �Ự����
 * @param table ���ݱ�
 * @param dataSize INOUT ����Ϊϣ�������ļ�¼�ܴ�С�����Ϊʵ�ʴ����ļ�¼�ܴ�С
 * @param len lob�еĳ���
 * @param isRand ID�Ƿ�������ɻ���˳������
 * @return ��¼��
 */
uint PaperTable::populate(Session *session, Table *table, u64 *dataSize, uint len, bool isRand) {
	Record *rec;
	u64 volumnCnt = *dataSize;
	uint recCnt = 0;
	uint dupIdx;
	RowId rid;
	uint outRecSize = len + sizeof(u64);
	if (!isRand) {
		while (true) {
			// ������¼
			rec = createRecord((u64)recCnt, len, table);
			if (volumnCnt < outRecSize)
				break;
			// �����¼
			rid = table->insert(session, rec->m_data, &dupIdx);
			if (rid != INVALID_ROW_ID) {
				recCnt++;
				volumnCnt -= outRecSize;
			}
			freeRecord(rec);
		}
	} 
	/*else {
		set<u64> idSet = new set<u64>;
		u64 recCount =  *dataSize / outRecSize;
		while (recCount > 0) {
			u64 newId = (u64)RandomGen::nextInt();
			while (idSet.end() == idSet.find(newId)) {
				newId = RandomGen::nextInt();
			}
			idSet.insert(newId);
			rec = createRecord((u64)newId, len, table);
			if (volumnCnt < outRecSize)
				break;
			 �����¼
			rid = table->insert(session, rec->m_data, &dupIdx);
			if (rid != INVALID_ROW_ID) {
				recCnt++;
				volumnCnt -= outRecSize;
				recCount--;
			}
			freeRecord(rec);
		 
	}*/
	*dataSize -= volumnCnt;
	return recCnt;

}
/**
* ���¼�¼��ID��
* @param rec �����¼�¼
* @param id ��id
*/
void PaperTable::updateId(Record *rec, u64 newid) {
	//assert(rec->m_format == REC_REDUNDANT);
	const TableDef *tableDef = getTableDef();
	ColumnDef *columnDef = tableDef->m_columns[0];
	*(u64 *)(rec->m_data + columnDef->m_offset) = newid;
}

/**
 *  ���ɱ���
 */
TableDef* PaperTable::makeTableDef() {
	TableDefBuilder *builder = new TableDefBuilder(TableDef::INVALID_TABLEID, "perfTest", "PaperUseMms");
	builder->addColumn(PAPER_ID, CT_BIGINT, false)->addColumn(PAPER_CONTENT, CT_MEDIUMLOB);
	builder->addIndex("PRIMARY", true, true, PAPER_ID, NULL);
	TableDef *tableDef = builder->getTableDef();
	tableDef->m_useMms = true;
	delete builder;
	return tableDef;
}

