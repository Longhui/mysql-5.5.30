/**
 * MMSģ���ۺ����ܲ���
 *
 * @author sf
 */
#ifndef _NTSETEST_MMS_SYNTHESIS_H_
#define _NTSETEST_MMS_SYNTHESIS_H_


#include "api/Database.h"
#include "util/PagePool.h"
#include "util/Thread.h"
#include "mms/Mms.h"
#include "MemHeap.h"
#include "EmptyTestCase.h"
#include "Random.h"
#include <sstream>

using namespace ntse;

class MmsPerfWorker;
class MmsPerfVerifier;

class MmsPerfTestCase : public EmptyTestCase {
public:
	MmsPerfTestCase(bool zipf = false, bool range = false);
	void setUp();
	void tearDown();
	void run();
	string getName() const;
	string getDescription() const;
	static bool isBig();

	void testMmsPerf();

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
	void doSelectEx(int k, u64 rowId);
	void startThreads();
	void stopThreads();
	void warm();

	u64				m_dataVolume;	/** ������ */
	MmsPerfVerifier	*m_verifierThread;	/** ��֤�߳� */
	MmsPerfWorker	**m_workerThreads;	/** �����߳��� */
	int				m_nrWorkThreads;	/** �����̸߳��� */
	Database		*m_db;				/** �������ݿ�ʵ�� */
	Array<MmsTable *> m_mmsTblArray;	/** ����MMS������ */
	Mms				*m_mms;				/** ����MMSȫ��ʵ�� */
	MmsTable		**m_mmsTables;		/** MMS������ */
	DrsHeap			**m_heaps;			/** ������ */
	Table			**m_tables;			/** ������ */
	int				m_numTables;		/** ����� */
	u64				*m_recCounts;		/** ��¼�� */
	Config			*m_cfg;				/** ���ݿ����� */
	TableDef		**m_tableDefs;		/** ������ */
	RowId			**m_rowIdTables;	/** RowIDӳ��� */
	bool			m_zipf;				/** Zipf�ֲ� */
	bool			m_range;			/** ��Χ��ѯ */
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

	friend class MmsPerfWorker;
};

/** ���������߳� */
class MmsPerfWorker : public Task {
public:
	MmsPerfWorker(MmsPerfTestCase *testCase) : Task("MmsPerfWorker", 0) { 
		this->m_case = testCase;
	}

	void run() {
		m_case->doWork();
	}

private:
	MmsPerfTestCase *m_case;	/** �������� */
};

/** ��֤�̣߳�������֤MMS���ݵ���ȷ�� */
class MmsPerfVerifier : public Task {
public:
	MmsPerfVerifier(MmsPerfTestCase *testCase, uint interval) : Task("MmsPerfVerifier", interval), m_case(testCase){
	}

	void run() {
		m_case->doVerify();
	}

private:
	MmsPerfTestCase *m_case;		/** �������� */
};

#endif // _NTSETEST_MMS_SYNTHESIS_H_

