/**
*	�����ģ���ȶ��Բ���
*
*	@author zx(zx@163.org)
*/

#ifndef _NTSE_LOBSTABILITY_TEST_H_
#define _NTSE_LOBSTABILITY_TEST_H_

#include <cppunit/extensions/HelperMacros.h>
#include "api/Database.h"
#include "util/Sync.h"
#include "util/Thread.h"
#include "lob/Lob.h"
#include "MemHeap.h"
#include "Random.h"
#include "ResLogger.h"

using namespace ntse;
class LobSblVerifier;
class LobSblWorker;
class LobDefragWorker;

class LobSblTestCase : public CPPUNIT_NS::TestFixture {
	CPPUNIT_TEST_SUITE(LobSblTestCase);
	CPPUNIT_TEST(testLobStability);
	CPPUNIT_TEST_SUITE_END();
public :
	static const char* getName();
	static const char* getDescription();
	static bool isBig();
	void setUp();
	void tearDown();
	void init(uint lobLen, u64 dataSize, bool useMms);
	
	void testLobStability();
	//�߳���֤
	void doVerify();
	void stopThreads();
	void startThreads();
	//�����ȵ��߳�
	void startNewThreads();

	static const int  MIN_LOB_LEN = 0;
	static const int  MAX_LOB_LEN = 128 * 1024;
	static const u32  THREAD_COUNT = 10;
	static const u32  VERIFY_INTERNAL = 15 * 60 * 1000;
	static const u32  DEFRAG_INTERNAL =  30 * 60 * 1000;
	/** ѡ�����Ȩ�� */
	static const u32 SELECT_WEIGHT = 30 ;
	/** �������Ȩ�� */
	static const u32 INSERT_WEIGHT = 30;
	/** ɾ������Ȩ�� */
	static const u32 DELETE_WEIGHT = 20;
	/** ���²���Ȩ�� */
	static const u32 UPDATE_WEIGHT = 20;

	/** �µĲ������Ȩ��*/
	static const u32 NEW_INSERT_WEIGHT = 40;
	/** �µ�ɾ������Ȩ�� */
	static const u32 NEW_DELETE_WEIGHT = 30;
	
	static const int WORK_TASK_TYPE_SELECT = 0;
	static const int WORK_TASK_TYPE_INSERT = 1;
	static const int WORK_TASK_TYPE_UPDATE = 2;
	static const int WORK_TASK_TYPE_DELETE = 3;
	
	//������ƽ������
	static const uint AVG_LOB_LEN_1 = 1024; 
	static const uint AVG_LOB_LEN_2 = 4 * 1024; 
	static const uint AVG_LOB_LEN_3 = 8 * 1024; 
	static const uint AVG_LOB_LEN_4 = 16 * 1024; 


protected :

	void doInsert();
	void doUpdate();
	void doDel();
	void doRead();
	void doDefrag();
	void doWork();
	/** ������̬�ֲ��õ�һ������*/
	uint getLenByNDist();
	byte* createLob(uint len);
	Record* createRecord(LobId lid, byte *lob, uint len);
	SubRecord* createSubRec(byte *lob, uint len);
	void createVitualTable();
	uint getTaskType();
	//���Ժ���
	void runTest();
	bool lastVerify();

private :
	ResLogger *m_resLogger;				/** ���ݿ���Դ��� */
	Config m_cfg;						/** �����ļ� */
	u64 m_dataSize;						/** ���������*/
	bool m_useMms;						/** �Ƿ�ʹ��mms */
	Table *m_table;						/** ����� */
	Database *m_db;						/** ���ݿ���� */
	Config *m_config;					/** ���ݿ����� */
	LobStorage *m_lobS;					/** �����洢*/
	TableDef *m_vTableDef;				/** ����� */
	MemHeap *m_memHeap;					/** ��Ӧ���ڴ�� */
	u32 m_avgLobSize;					/** ƽ�������ĳ��ȣ�����ֻ����̬�ֲ��� */
	LobSblVerifier	*m_verifierThread;	/** ��֤�߳� */
	LobSblWorker	**m_workerThreads;	/** �����߳��� */
	uint m_threadCount;					/** �߳����� */
	LobDefragWorker *m_defragThread;    /** defrag�߳� */
	RandDist *m_randDist;               /** ��������������� */
	volatile bool m_flag;				/** ��־λ���Ƿ�Ҫֹͣ�����߳�*/
	volatile bool m_stop;               /** �Ƿ�ֹͣ����*/
	friend class LobSblWorker;
	friend class LobSblVerifier;
	friend class LobDefragWorker;
};

/** ���������߳� */
class LobSblWorker : public Thread {
public:
	LobSblWorker(LobSblTestCase *testCase) : Thread("LobSblWorker") { 
		this->m_case = testCase;
	}
	void run() {
		m_case->doWork();
	}

private:
	LobSblTestCase *m_case;	/** �������� */
};


/** ��֤�̣߳�������֤Lob���ݵ���ȷ�� */
class LobSblVerifier : public Task {
public:
	LobSblVerifier(LobSblTestCase *testCase, uint interval) : Task("LobSblVerifier", interval), m_case(testCase){
	}

	void run() {
		m_case->doVerify();
	}

private:
	LobSblTestCase *m_case;		/** �������� */
};

/** defrag �̣߳�������Ҫ����ʱ�䳤���������м����Ҫ��*/
class LobDefragWorker : public Task   {
public :
	LobDefragWorker(LobSblTestCase *testCase, uint interval): Task("LobDefragWorker", interval), m_case(testCase) {
	};
	void run() {
		m_case->doDefrag();
	}
	
private :
	LobSblTestCase *m_case;
};

#endif // NTSE_LOBSTABILITY_TEST_H_