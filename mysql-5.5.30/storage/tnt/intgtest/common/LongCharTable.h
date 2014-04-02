/**
 * 公共测试表LongChar
 * 
 * @author 余利华(yulihua@corp.netease.com, ylh@163.org)
 */
#ifndef _NTSETEST_LONGCHAR_TABLE_H_
#define _NTSETEST_LONGCHAR_TABLE_H_

#include "api/Table.h"
#include "misc/TableDef.h"

using namespace ntse;


/** LongChar表的列名 */
#define LONGCHAR_ID				"ID"
#define LONGCHAR_NAME			"Name"
#define TABLE_NAME_LONGCHAR		"LongChar"
/** LongChar表的列号 */
#define LONGCHAR_ID_CNO			0
#define LONGCHAR_NAME_CNO		1

/** LongChar表辅助函数 */
class LongCharTable {
public:
	static const TableDef* getTableDef(bool useMMs);

	/**
	 * 记录长度，不包括NTSE的额外空间消耗
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
	* 创建表定义
	* @param useMms 是否使用Mms
	*/
	static TableDef* makeTableDef(bool useMms);

	/** 初始化表定义 */
	LongCharTable();
	~LongCharTable();

	static const u16 NAME_LEN = 200;

private:
	TableDef *m_tableDefMms;		/** 表定义，useMMs */
	TableDef *m_tableDefNoMms;		/** 表定义，notUseMMs */
	static LongCharTable m_inst;	/** 单例 */
};


#endif // _NTSETEST_LONGCHAR_TABLE_H_
