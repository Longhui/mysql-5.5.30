#ifndef _TNTTEST_LONGCHAR_TABLE_H_
#define _TNTTEST_LONGCHAR_TABLE_H_

#include "api/TNTTable.h"

using namespace tnt;

/** LongChar������� */
#define LONGCHAR_ID				"ID"
#define LONGCHAR_NAME			"Name"
#define TNTTABLE_NAME_LONGCHAR	"LongChar"
/** LongChar����к� */
#define LONGCHAR_ID_CNO			0
#define LONGCHAR_NAME_CNO		1

class TNTLongCharTable
{
public:
	static const TableDef* getTableDef(bool useMMs);

	/**
	 * ��¼���ȣ�������NTSE�Ķ���ռ�����
	 */
	static inline u32 getRecordSize() {
		return sizeof(u64) + NAME_LEN;
	}

	static Record* createRecord(u64 id);
	static Record* updateId(Record *rec, u64 id);
	static SubRecord* updateKey(SubRecord *key, u64 id);
	static bool insertRecord(Session *session, TNTTable *table, TNTOpInfo *opInfo, Record *rec);

	static uint populate(Session *session, TNTTable *table, TNTOpInfo *opInfo, u64 *dataSize, u64 startId = 0);
	static u64 scanTable(TNTDatabase *db, TNTTable *table);
private:
	/**
	* ��������
	* @param useMms �Ƿ�ʹ��Mms
	*/
	static TableDef* makeTableDef(bool useMms);

	/** ��ʼ������ */
	TNTLongCharTable();
	~TNTLongCharTable();

	static const u16 NAME_LEN = 200;

private:
	TableDef *m_tableDefMms;		/** ���壬useMMs */
	TableDef *m_tableDefNoMms;		/** ���壬notUseMMs */
	static TNTLongCharTable m_inst;	/** ���� */
};
#endif
