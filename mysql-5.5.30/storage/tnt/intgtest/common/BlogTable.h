/**
* �������Ա�Blog
* 
* @author ������(yulihua@corp.netease.com, ylh@163.org)
*/
#ifndef _NTSETEST_BLOG_TABLE_H_
#define _NTSETEST_BLOG_TABLE_H_

#include "api/Table.h"
#include "misc/TableDef.h"
#include "misc/RecordHelper.h"

using namespace ntse;

/** Blog������� */
#define BLOG_ID				"ID"
#define BLOG_USERID			"UserID"
#define BLOG_PUBLISHTIME	"PublishTime"
#define BLOG_TITLE			"Title"
#define BLOG_ABSTRACT		"Abstract"
#define BLOG_CONTENT		"Content"
#define BLOG_PERMALINK		"Permalink"
#define TABLE_NAME_BLOG		"Blog"

/** Blog�������� */
#define BLOG_ID_CNO				0
#define BLOG_USERID_CNO			1
#define BLOG_PUBLISHTIME_CNO	2
#define BLOG_TITLE_CNO			3
#define BLOG_ABSTRACT_CNO		4
#define BLOG_CONTENT_CNO		5
#define BLOG_PERMALINK_CNO		6




/** Blog�������� */
class BlogTable {
public:
	static const TableDef* getTableDef(bool useMMs);
	static uint populate(Session *session, Table *table, u64 *dataSize, u64 startId = 0);
	static uint populate(Session *session, Table *table, u64 *dataSize, u64 maxId, u64 minId = 0);
	static SubRecord* updateKey(SubRecord *key, u64 id);

	static bool insertRecord(Session *session, Table *table, u64 id, char *buffer, RedRecord *rRecord, u32 minRecSize = DEFAULT_MIN_REC_SIZE, u32 avgRecSize = DEFAULT_AVG_REC_SIZE);
	static bool insertRecord(Session *session, Table *table, u64 id, int *outRecSize = NULL, u32 minRecSize = DEFAULT_MIN_REC_SIZE, u32 avgRecSize = DEFAULT_AVG_REC_SIZE);
public:
	const static u32 DEFAULT_AVG_REC_SIZE = 16 * 1024;	/** ��¼ƽ������ */
	const static u32 DEFAULT_MIN_REC_SIZE = 1024;		/** ��¼��С���� */

private:
	/**
	* ��������
	* @param useMms �Ƿ�ʹ��Mms
	*/
	static TableDef* makeTableDef(bool useMms);

	/** ��ʼ������ */
	BlogTable();
	~BlogTable();

private:
	TableDef *m_tableDefMms;		/** ���壬useMMs */
	TableDef *m_tableDefNoMms;		/** ���壬notUseMMs */
	static BlogTable m_inst;		/** ���� */

};



#endif // _NTSETEST_BLOG_TABLE_H_


