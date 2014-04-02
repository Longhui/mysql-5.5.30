#ifndef _TNTTEST_ACCOUNT_TABLE_H_
#define _TNTTEST_ACCOUNT_TABLE_H_

#include "api/TNTTable.h"

using namespace tnt;

	/** Account表的列名 */
#define ACCOUNT_ID				"ID"
#define ACCOUNT_PASSPORTNAME	"PassPortName"
#define ACCOUNT_USERNAME		"Username"
#define TNTTABLE_NAME_ACCOUNT	"Account"

	/** Account表的列号 */
#define ACCOUNT_ID_CNO				0
#define ACCOUNT_PASSPORTNAME_CNO	1
#define ACCOUNT_USERNAME_CNO		2

class TNTAccountTable
{
public:
	/**
	 * 获取表定义
	 * @param useMms 是否使用Mms
	 */
	static const TableDef* getTableDef(bool useMMs);
	

	/**
	 * 随机构造总大小为dataSize的记录插入到table中
	 * 记录ID从startId开始递增生成
	 * @param table 数据表
	 * @param dataSize 记录总大小
	 * @param startId 起始ID 
	 * @return 记录数
	 */
	static uint populate(Session *session, TNTTable *table, TNTOpInfo *opInfo, u64 *dataSize, u64 startId = 0);
	
	/**
	 * 随机构造总大小为dataSize的记录插入到table中,
	 * 记录ID从在[minidx, minid]之间随机生成
	 * @param table 数据表
	 * @param dataSize 记录总大小
	 * @param maxid 最大id 
	 * @param minid 最小id
	 * @return 记录数
	 */
	static uint populate(Session *session, TNTTable *table, TNTOpInfo *opInfo, u64 *dataSize, u64 maxId, u64 minId = 0);

	/** 
	 * 把记录插入到table中
	 *
	 * @param session 会话
	 * @param table 表
	 * @param rec 记录
	 * @return 是否成功
	 */
	static bool insertRecord(Session *session, TNTTable *table, TNTOpInfo *opInfo, Record *rec);

	static u64 scanTable(TNTDatabase *db, TNTTable *table);

	static Record* createRecord(u64 id, int *outRecSize = NULL
		, u32 minRecSize = DEFAULT_MIN_REC_SIZE, u32 avgRecSize = DEFAULT_AVG_REC_SIZE);

	static Record* updateRecord(Record *rec, u64 id, int *outRecSize = NULL, u32 minRecSize = DEFAULT_MIN_REC_SIZE
		, u32 avgRecSize = DEFAULT_AVG_REC_SIZE);
	static Record* updateId(Record *rec, u64 id);
	
	static SubRecord* updateKey(SubRecord *key, u64 id);

public:
	const static u32 DEFAULT_AVG_REC_SIZE = 200;	/** 记录平均长度 */
	const static u32 DEFAULT_MIN_REC_SIZE = 150;	/** 记录最小长度 */
	const static u16 USERNAME_LEN = Limits::MAX_REC_SIZE / 2 - 16;	/** UserName 列长度 */
	const static u16 PASSPORT_LEN = Limits::MAX_REC_SIZE / 2;	/** PassPortName 列长度 */
private:
	/**
	 * 创建表定义
	 * @param useMms 是否使用Mms
	 */
	static TableDef* makeTableDef(bool useMms);

	/** 初始化表定义 */
	TNTAccountTable();
	~TNTAccountTable();

private:
	TableDef *m_tableDefMms;		/** 表定义，useMMs */
	TableDef *m_tableDefNoMms;		/** 表定义，notUseMMs */
	static TNTAccountTable m_inst;		/** 单例 */
};
#endif
