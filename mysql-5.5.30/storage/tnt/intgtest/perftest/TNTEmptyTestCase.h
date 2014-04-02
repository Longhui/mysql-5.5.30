/**
 * �ղ�������, ʵ�ֲ���������һЩ���ù���
 *
 * @author �Լ�Զ(hzzhaojy@corp.netease.com)
 */
#ifndef _TNTTEST_EMPTY_TESTCASE_H_
#define _TNTTEST_EMPTY_TESTCASE_H_

#include "PerfTest.h"
#include "api/TNTTable.h"
#include "api/TNTDatabase.h"
#include "misc/TableDef.h"

//using namespace tntperf;
using namespace tnt;
using namespace ntseperf;

/** 
 * �ղ����������򻯲��������Ĺ���
 */

namespace tntperf{

class TNTEmptyTestCase: public TestCase {
public:
	TNTEmptyTestCase();

	~TNTEmptyTestCase();

	virtual string getName() const {
		return "TNTEmptyTestCase";
	}

	virtual string getDescription() const {
		return "TNTEmptyTestCase's Description";
	}

	virtual void run();
	
	virtual void tearDown() {
		closeDatabase();
	}

	virtual TNTDatabase* getTNTDatabase() {
		return m_db;
	}

	/**
	 * �����Ѽ���������Ϣ
	 *	�Ӳ������ݻ���������ݺ󣬲��Կ�ܵ��ñ����������Ѽ���������Ϣ
	 * @param totalRecSize ��������
	 * @param recCnt ��¼��
	 */
	virtual void setLoadedDataInfo(u64 totalRecSize, u64 recCnt) {
		m_recCntLoaded = recCnt;
		m_totalRecSizeLoaded = totalRecSize;
	}

	virtual bool cacheEnabled() {
		return m_enableCache;
	}

	/** ���������������������� */
	virtual u64 getDataSize() {
		return m_opDataSize;
	}
	/** ���������Ĳ������� */
	virtual u64 getOpCnt() {
		return m_opCnt;
	}

	/** �Ƿ��ڴ��������(�ڴ�����������IO) */
	virtual bool isMemoryCase() {
		return m_isMemoryCase;
	}

	/**
	 * ��ȡͳ����Ϣ
	 * @return ͳ����Ϣ������0��ʾû��ͳ����Ϣ
	 */
	virtual const TestCaseStatus* getStatus() const;


protected:
	void openTable(bool create = false);
	void closeDatabase();
	void disableIo();

	void setTableDef(const TableDef *tableDef);
	void setConfig(TNTConfig *cfg);

protected:
	bool m_enableCache;			/** �Ƿ����������ݻ��湦�� */
	bool m_isMemoryCase;		/** �Ƿ��ڴ�������� */

	u64 m_totalRecSizeLoaded;	/** ��װ���������� */
	u64 m_recCntLoaded;			/** ��װ�ص��ܼ�¼�� */

	u64 m_opCnt;				/** �������� */
	u64 m_opDataSize;			/** ���������� */

	TestCaseStatus *m_status;	/** ����ͳ����Ϣ */

	TNTDatabase *m_db;				/** �������ݿ� */
	TNTTable *m_table;				/** ���Ա� */
	TableDef *m_tableDef;		/** ���� */
	TNTConfig *m_config;			/** ���ݿ����� */
};

}

#endif // _TNTTEST_EMPTY_TESTCASE_H_
