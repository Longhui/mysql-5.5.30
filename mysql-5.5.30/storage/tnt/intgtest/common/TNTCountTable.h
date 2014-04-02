#ifndef _TNTTEST_COUNT_TABLE_H_
#define _TNTTEST_COUNT_TABLE_H_

#include "api/TNTDatabase.h"
#include "api/TNTTable.h"

/** Count������� */
#define COUNT_ID				"ID"
#define COUNT_COUNT				"Count"
/** ����� */
#define COUNT_ID_CNO			0
#define COUNT_COUNT_CNO			1

#define TNTTABLE_NAME_COUNT		"Count"

class TNTCountTable
{
public:

	static const TableDef* getTableDef(bool useMMs);

	/**
	 * ��¼���ȣ�������NTSE�Ķ���ռ�����
	 */
	static inline u32 getRecordSize() {
		return 8 + 4;
	}

	static Record* createRecord(u64 id, int count);
	static Record* updateId(Record *rec, u64 id);
	static Record* updateCount(Record *rec, int count);
	static Record* updateRecord(Record *rec, u64 id, int count);
	static SubRecord* updateKey(SubRecord *key, u64 id);

	static uint populate(Session *session, TNTTable *table, TNTOpInfo *opInfo, u64 *dataSize, u64 startId = 0);
	static uint populate(Session *session, TNTTable *table, TNTOpInfo *opInfo, u64 *dataSize, u64 maxId, u64 minId = 0);
	static u64 scanTable(TNTDatabase *db, TNTTable *table);
private:

	static TableDef* makeTableDef(bool useMms);

	/** ��ʼ������ */
	TNTCountTable();
	~TNTCountTable();

private:
	TableDef *m_tableDefMms;		/** ���壬useMMs */
	TableDef *m_tableDefNoMms;		/** ���壬notUseMMs */
	static TNTCountTable m_inst;		/** ���� */
};
#endif
