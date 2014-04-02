/**
* 公共测试表:Paper
* 
* @author zx(zx@163.org)
*/
#ifndef _NTSETEST_PAPER_TABLE_H_
#define _NTSETEST_PAPER_TABLE_H_

#include "api/Table.h"
#include "misc/TableDef.h"

using namespace ntse;


/** Paper表的列名 */
#define PAPER_ID				"ID"
#define PAPER_CONTENT			"Content"
#define TABLE_NAME_PAPER		"Paper"

/** Paper表的列号 */
#define PAPER_ID_CNO			0
#define PAPER_CONTENT_CNO		1
/** Paper表类 */
class PaperTable {
public:
	//获得表定义
	static const TableDef* getTableDef();
	//生成记录
	static Record* createRecord(u64 id, uint len, Table *table);
   
	static Record** populate(Session *session, Table *table, u64 *sizeCnt, uint len, u64 recCount);
	static Record** populate(Session *session, Table *table, uint len, u64 recCount);
	static uint populate(Session *session, Table *table, u64 *sizeCnt, uint len, bool isRand);
	static void updateId(Record *rec, u64 newid);

private:

	static TableDef* makeTableDef();
	PaperTable();
	~PaperTable();

private:
	TableDef *m_tableDef;			/** 表定义 */
	static PaperTable m_inst;		/** 单例 */
};

#endif 
