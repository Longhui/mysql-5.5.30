#ifndef _TNTTEST_ACCOUNT_TABLE_H_
#define _TNTTEST_ACCOUNT_TABLE_H_

#include "api/TNTTable.h"

using namespace tnt;

	/** Account������� */
#define ACCOUNT_ID				"ID"
#define ACCOUNT_PASSPORTNAME	"PassPortName"
#define ACCOUNT_USERNAME		"Username"
#define TNTTABLE_NAME_ACCOUNT	"Account"

	/** Account����к� */
#define ACCOUNT_ID_CNO				0
#define ACCOUNT_PASSPORTNAME_CNO	1
#define ACCOUNT_USERNAME_CNO		2

class TNTAccountTable
{
public:
	/**
	 * ��ȡ����
	 * @param useMms �Ƿ�ʹ��Mms
	 */
	static const TableDef* getTableDef(bool useMMs);
	

	/**
	 * ��������ܴ�СΪdataSize�ļ�¼���뵽table��
	 * ��¼ID��startId��ʼ��������
	 * @param table ���ݱ�
	 * @param dataSize ��¼�ܴ�С
	 * @param startId ��ʼID 
	 * @return ��¼��
	 */
	static uint populate(Session *session, TNTTable *table, TNTOpInfo *opInfo, u64 *dataSize, u64 startId = 0);
	
	/**
	 * ��������ܴ�СΪdataSize�ļ�¼���뵽table��,
	 * ��¼ID����[minidx, minid]֮���������
	 * @param table ���ݱ�
	 * @param dataSize ��¼�ܴ�С
	 * @param maxid ���id 
	 * @param minid ��Сid
	 * @return ��¼��
	 */
	static uint populate(Session *session, TNTTable *table, TNTOpInfo *opInfo, u64 *dataSize, u64 maxId, u64 minId = 0);

	/** 
	 * �Ѽ�¼���뵽table��
	 *
	 * @param session �Ự
	 * @param table ��
	 * @param rec ��¼
	 * @return �Ƿ�ɹ�
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
	const static u32 DEFAULT_AVG_REC_SIZE = 200;	/** ��¼ƽ������ */
	const static u32 DEFAULT_MIN_REC_SIZE = 150;	/** ��¼��С���� */
	const static u16 USERNAME_LEN = Limits::MAX_REC_SIZE / 2 - 16;	/** UserName �г��� */
	const static u16 PASSPORT_LEN = Limits::MAX_REC_SIZE / 2;	/** PassPortName �г��� */
private:
	/**
	 * ��������
	 * @param useMms �Ƿ�ʹ��Mms
	 */
	static TableDef* makeTableDef(bool useMms);

	/** ��ʼ������ */
	TNTAccountTable();
	~TNTAccountTable();

private:
	TableDef *m_tableDefMms;		/** ���壬useMMs */
	TableDef *m_tableDefNoMms;		/** ���壬notUseMMs */
	static TNTAccountTable m_inst;		/** ���� */
};
#endif
