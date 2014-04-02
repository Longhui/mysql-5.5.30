/**
* �������Ա�:Paper
* 
* @author zx(zx@163.org)
*/
#ifndef _NTSETEST_PAPER_TABLE_H_
#define _NTSETEST_PAPER_TABLE_H_

#include "api/Table.h"
#include "misc/TableDef.h"

using namespace ntse;


/** Paper������� */
#define PAPER_ID				"ID"
#define PAPER_CONTENT			"Content"
#define TABLE_NAME_PAPER		"Paper"

/** Paper����к� */
#define PAPER_ID_CNO			0
#define PAPER_CONTENT_CNO		1
/** Paper���� */
class PaperTable {
public:
	//��ñ���
	static const TableDef* getTableDef();
	//���ɼ�¼
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
	TableDef *m_tableDef;			/** ���� */
	static PaperTable m_inst;		/** ���� */
};

#endif 
