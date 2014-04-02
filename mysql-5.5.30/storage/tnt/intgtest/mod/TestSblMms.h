/**
* MMS�ȶ��Բ���
*
* @author �۷�(shaofeng@corp.netease.com, sf@163.org)
*/

#ifndef _NTSESBLTEST_MMS_H_
#define _NTSESBLTEST_MMS_H_


#include <cppunit/extensions/HelperMacros.h>
#include "api/Database.h"
#include "util/PagePool.h"
#include "util/Thread.h"
#include "mms/Mms.h"
#include "MemHeap.h"
#include "ResLogger.h"

using namespace ntse;

class MmsSblMaster;
class MmsSblWorker;
class MmsSblTimer;
class MmsSblVerifier;

class MmsSblTestCase : public CPPUNIT_NS::TestFixture {
	CPPUNIT_TEST_SUITE(MmsSblTestCase);
	
	CPPUNIT_TEST(testMmsStability);
	
	CPPUNIT_TEST_SUITE_END();

public:
	void setUp();
	void tearDown();
	static const char* getName();
	static const char* getDescription();
	static bool isBig();

	void testMmsStability();
	
	//void doCheckPoint();
	void doVerify();
	void doWork();

	static const int	WORK_TASK_TYPE_INSERT = 1;
	static const int	WORK_TASK_TYPE_DELETE = 2;
	static const int	WORK_TASK_TYPE_SELECT = 3;

private:
	int	getTaskType();
	void doInsert();
	void doDelete();
	void doSelect();
	void startThreads();
	void stopThreads();

	ResLogger *m_resLogger;
	byte	m_mode;				/** ��ǰ����ģʽ */
	Atomic<long> dataVolumn;	/** ������ */
	// MmsSblMaster	*m_masterThread;	/** �������߳� */
	//MmsSblTimer		*m_timerThread;		/** ʱ���߳� */
	MmsSblVerifier	*m_verifierThread;	/** ��֤�߳� */
	MmsSblWorker	**m_workerThreads;	/** �����߳��� */
	int				m_nrWorkThreads;	/** �����̸߳��� */
	Database		*m_db;				/** �������ݿ�ʵ�� */
	Array<MmsTable *> m_mmsTblArray;	/** ����MMS������ */
	Mms				*m_mms;				/** ����MMSȫ��ʵ�� */
	MmsTable		**m_mmsTables;		/** MMS������ */
	DrsHeap			**m_heaps;			/** ������ */
	Table			**m_tables;			/** ������ */
	MemHeap			**m_memHeaps;		/** �ڴ������ */
	int				m_numTables;		/** ��¼�� */
	Config			*m_cfg;				/** ���ݿ����� */
	TableDef		**m_tableDefs;		/** ������ */

	long			m_numVerify;		/** ��֤�������� */
	long			m_numSelect;		/** ѡ��������� */
	long			m_numDelete;		/** ɾ���������� */
	long			m_numInsert;		/** ����������� */
	long			m_numUpdate;		/** ���²������� */
	long			m_beginTime;		/** ��ʼ��ʱ */
	long			m_endTime;			/** ������ʱ */
	long			m_numPreSelect;		/** ѡ��������� */
	long			m_numPreDelete;		/** ɾ���������� */
	long			m_numPreInsert;		/** ����������� */
	long			m_numPreUpdate;		/** ���²������� */

	u64				m_mmsRecordQueries;	/** MMS��¼��ѯ����	*/			
	u64				m_mmsRecordQueryHits;	/** MMS��¼��ѯ���д��� */
	u64				m_mmsRecordInserts;	/** MMS��¼�������	*/			
	u64				m_mmsRecordDeletes;	/** MMS��¼��ɾ������ */	
	u64				m_mmsRecordUpdates;	/** MMS��¼�����´��� */
	u64				m_mmsRecordVictims;    /** MMS��¼���滻���� */
	u64				m_mmsPageVictims;		/** MMS����ҳ���滻���� */	
	u64				m_mmsOccupiedPages;	/** MMSռ�õ��ڴ�ҳ���� */
	u64				m_mmsFreePages;		/** MMSռ�õ����ɿ���ҳ���� */	

	static const uint SELECT_WEIGHT;	/** ѡ�����Ȩ�� */
	static const uint DELETE_WEIGHT;	/** ɾ������Ȩ�� */
	// static const uint GET_BY_RID_WEIGHT; /** RID��ѯȨ�� */
	static const uint UPDATE_WEIGHT_WHEN_SELECT; /** ѡ�������ִ�и���Ȩ�� */
	static const uint INSERT_MMS_WEIGHT_WHEN_SELECT; /** ѡ������л�дMMSȨ�� */
	//static const uint PRIMARY_KEY_UPDATE_WEIGHT; /** ��������Ȩ�� */
	static const uint MAX_REC_COUNT_IN_TABLE;	/** ��������¼�� */
	static const uint TABLE_NUM_IN_SAME_TEMPLATE; /** ͬһģ��ı���� */
	static const uint WORKING_THREAD_NUM;		/** �����߳��� */
	static const uint VERIFY_DURATION;			/** ��֤���ʱ�� */
	//static const uint CHECKPOINT_DURATION;		/** ����ˢ�¼��ʱ�� */
	static const uint RUN_DURATION;				/** ���м��ʱ�� */
	static const uint FOREVER_DURATION;			/** ��Զ���� */
	
	friend class MmsSblMaster;
	friend class MmsSblWorker;
};

/** ���������߳� */
class MmsSblWorker : public Task {
public:
	MmsSblWorker(MmsSblTestCase *testCase) : Task("MmsSblWorker", 0) { 
		this->m_case = testCase;
	}

	void run() {
		m_case->doWork();
	}

private:
	MmsSblTestCase *m_case;	/** �������� */
};

/** ����ʱ���̣߳�����ִ�м���ˢдMMS������ */
//class MmsSblTimer : public Task {
//public:
//	MmsSblTimer(MmsSblTestCase *testCase, uint interval) : Task("MmsSblTimer", interval), m_case(testCase){
//	}
//
//	void run() {
//		m_case->doCheckPoint();
//	}
//
//private:
//	MmsSblTestCase *m_case;		/** �������� */
//};

/** ��֤�̣߳�������֤MMS���ݵ���ȷ�� */
class MmsSblVerifier : public Task {
public:
	MmsSblVerifier(MmsSblTestCase *testCase, uint interval) : Task("MmsSblVerifier", interval), m_case(testCase){
	}

	void run() {
		m_case->doVerify();
	}

private:
	MmsSblTestCase *m_case;		/** �������� */
};

#endif // _NTSESBLTEST_MMS_H_
