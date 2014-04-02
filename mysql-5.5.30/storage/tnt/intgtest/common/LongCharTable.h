/**
 * �������Ա�LongChar
 * 
 * @author ������(yulihua@corp.netease.com, ylh@163.org)
 */
#ifndef _NTSETEST_LONGCHAR_TABLE_H_
#define _NTSETEST_LONGCHAR_TABLE_H_

#include "api/Table.h"
#include "misc/TableDef.h"

using namespace ntse;


/** LongChar������� */
#define LONGCHAR_ID				"ID"
#define LONGCHAR_NAME			"Name"
#define TABLE_NAME_LONGCHAR		"LongChar"
/** LongChar����к� */
#define LONGCHAR_ID_CNO			0
#define LONGCHAR_NAME_CNO		1

/** LongChar�������� */
class LongCharTable {
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
	static bool insertRecord(Session *session, Table *table, Record *rec);

	static uint populate(Session *session, Table *table, u64 *dataSize, u64 startId = 0);
	static u64 scanTable(Database *db, Table *table);
private:
	/**
	* ��������
	* @param useMms �Ƿ�ʹ��Mms
	*/
	static TableDef* makeTableDef(bool useMms);

	/** ��ʼ������ */
	LongCharTable();
	~LongCharTable();

	static const u16 NAME_LEN = 200;

private:
	TableDef *m_tableDefMms;		/** ���壬useMMs */
	TableDef *m_tableDefNoMms;		/** ���壬notUseMMs */
	static LongCharTable m_inst;	/** ���� */
};


#endif // _NTSETEST_LONGCHAR_TABLE_H_
