/**
 * ����MMS�滻����Ч��
 *
 * @author �۷�(shaofeng@corp.netease.com sf@163.org)
 */

#ifndef _NTSETEST_MMS_REPLACE_POLICY_H_
#define _NTSETEST_MMS_REPLACE_POLICY_H_

#include "PerfTest.h"
#include "EmptyTestCase.h"
#include "Random.h"
#include <sstream>

using namespace ntse;

class MmsReplaceWorker;
class MmsReplaceVerifier;

class MmsReplacePolicyTest: public EmptyTestCase {
public:
	MmsReplacePolicyTest();
	void setUp();
	void tearDown();
	void run();
	string getName() const;
	string getDescription() const;
	static bool isBig();

	void testMmsReplace();

	void doVerify();
	void doWork();

	static const int	WORK_UPDATE_IF_EXIST = 1;
	static const int	WORK_DELETE_IF_EXIST = 2;
	static const int	WORK_GET_IF_EXIST = 3;
	static const int	WORK_INSERT_IF_NOT_EXIST = 4;
	static const int	WORK_NOTHING_IF_NOT_EXIST = 5;

private:
	bool getTaskType(int *workIfExist, int *workIfNotExist);
	void doSelect(int k, u64 rowId);
	void startThreads();
	void stopThreads();
	void warm();

	u64				m_dataVolume;	/** ������ */
	MmsReplaceVerifier	*m_verifierThread;	/** ��֤�߳� */
	MmsReplaceWorker	**m_workerThreads;	/** �����߳��� */
	int				m_nrWorkThreads;	/** �����̸߳��� */
	Database		*m_db;				/** �������ݿ�ʵ�� */
	Array<MmsTable *> m_mmsTblArray;	/** ����MMS������ */
	Mms				*m_mms;				/** ����MMSȫ��ʵ�� */
	MmsTable		**m_mmsTables;		/** MMS������ */
	DrsHeap			**m_heaps;			/** ������ */
	Table			**m_tables;			/** ������ */
	int				m_numTables;		/** ����� */
	int				m_recCount;			/** ��¼�� */
	Config			*m_cfg;				/** ���ݿ����� */
	TableDef		**m_tableDefs;		/** ������ */
	RowId			**m_rowIdTables;	/** RowIDӳ��� */
	ZipfRandom		**m_zipfRandoms;	/** zipf��������� */

	long			m_numVerify;		/** ��֤�������� */
	long			m_numSelect;		/** ѡ��������� */
	long			m_numDelete;		/** ɾ���������� */
	long			m_numInsert;		/** ����������� */
	long			m_numUpdate;		/** ���²������� */
	long			m_numGet;			/** ��ȡ�������� */
	long			m_numNothing;		/** �ղ������� */
	long			m_beginTime;		/** ��ʼ��ʱ */
	long			m_endTime;			/** ������ʱ */
	long			m_numPreSelect;		/** ѡ��������� */
	long			m_numPreDelete;		/** ɾ���������� */
	long			m_numPreInsert;		/** ����������� */
	long			m_numPreUpdate;		/** ���²������� */
	long			m_numPreGet;		/** ��ȡ�������� */
	long			m_numPreNothing;	/** �ղ������� */

	u64				m_mmsRecordQueries;	/** MMS��¼��ѯ����	*/			
	u64				m_mmsRecordQueryHits;	/** MMS��¼��ѯ���д��� */
	u64				m_mmsRecordInserts;	/** MMS��¼�������	*/			
	u64				m_mmsRecordDeletes;	/** MMS��¼��ɾ������ */	
	u64				m_mmsRecordUpdates;	/** MMS��¼�����´��� */
	u64				m_mmsRecordVictims; /** MMS��¼���滻���� */
	u64				m_mmsPageVictims;	/** MMS����ҳ���滻���� */	
	u64				m_mmsOccupiedPages;	/** MMSռ�õ��ڴ�ҳ���� */
	u64				m_mmsFreePages;		/** MMSռ�õ����ɿ���ҳ���� */

	static const uint UPDATE_IF_EXIST;
	static const uint DELETE_IF_EXIST;
	static const uint GET_IF_EXIST;
	static const uint INSERT_IF_NO_EXIST;
	static const uint NOTHING_IF_NO_EXIST;

	static const uint MAX_REC_COUNT_IN_TABLE;
	static const uint TABLE_NUM_IN_SAME_TEMPLATE;
	static const uint WORKING_THREAD_NUM;
	static const uint VERIFY_DURATION;
	static const uint FOREVER_DURATION;
	static const uint RUN_DURATION;

	static const uint PAGE_NUM;
	static const uint SLOT_NUM;

	friend class MmsReplaceWorker;
};

/** ���������߳� */
class MmsReplaceWorker : public Task {
public:
	MmsReplaceWorker(MmsReplacePolicyTest *testCase) : Task("MmsReplaceWorker", 0) { 
		m_case = testCase;
	}

	void run() {
		m_case->doWork();
	}

private:
	MmsReplacePolicyTest *m_case;	/** �������� */
};

/** ��֤�̣߳�������֤MMS���ݵ���ȷ�� */
class MmsReplaceVerifier : public Task {
public:
	MmsReplaceVerifier(MmsReplacePolicyTest *testCase, uint interval) : Task("MmsReplaceVerifier", interval), m_case(testCase){
	}

	void run() {
		m_case->doVerify();
	}

private:
	MmsReplacePolicyTest *m_case;		/** �������� */
};


#endif
