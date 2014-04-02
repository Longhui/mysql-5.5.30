/**
*	大对象模块稳定性测试
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
	//线程验证
	void doVerify();
	void stopThreads();
	void startThreads();
	//启动先的线程
	void startNewThreads();

	static const int  MIN_LOB_LEN = 0;
	static const int  MAX_LOB_LEN = 128 * 1024;
	static const u32  THREAD_COUNT = 10;
	static const u32  VERIFY_INTERNAL = 15 * 60 * 1000;
	static const u32  DEFRAG_INTERNAL =  30 * 60 * 1000;
	/** 选择操作权重 */
	static const u32 SELECT_WEIGHT = 30 ;
	/** 插入操作权重 */
	static const u32 INSERT_WEIGHT = 30;
	/** 删除操作权重 */
	static const u32 DELETE_WEIGHT = 20;
	/** 更新操作权重 */
	static const u32 UPDATE_WEIGHT = 20;

	/** 新的插入操作权重*/
	static const u32 NEW_INSERT_WEIGHT = 40;
	/** 新的删除操作权重 */
	static const u32 NEW_DELETE_WEIGHT = 30;
	
	static const int WORK_TASK_TYPE_SELECT = 0;
	static const int WORK_TASK_TYPE_INSERT = 1;
	static const int WORK_TASK_TYPE_UPDATE = 2;
	static const int WORK_TASK_TYPE_DELETE = 3;
	
	//大对象的平均长度
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
	/** 根据正态分布得到一个长度*/
	uint getLenByNDist();
	byte* createLob(uint len);
	Record* createRecord(LobId lid, byte *lob, uint len);
	SubRecord* createSubRec(byte *lob, uint len);
	void createVitualTable();
	uint getTaskType();
	//测试函数
	void runTest();
	bool lastVerify();

private :
	ResLogger *m_resLogger;				/** 数据库资源监控 */
	Config m_cfg;						/** 配置文件 */
	u64 m_dataSize;						/** 最大数据量*/
	bool m_useMms;						/** 是否使用mms */
	Table *m_table;						/** 表对象 */
	Database *m_db;						/** 数据库对象 */
	Config *m_config;					/** 数据库配置 */
	LobStorage *m_lobS;					/** 大对象存储*/
	TableDef *m_vTableDef;				/** 虚拟表 */
	MemHeap *m_memHeap;					/** 对应的内存堆 */
	u32 m_avgLobSize;					/** 平均大对象的长度，这里只是正态分布用 */
	LobSblVerifier	*m_verifierThread;	/** 验证线程 */
	LobSblWorker	**m_workerThreads;	/** 工作线程组 */
	uint m_threadCount;					/** 线程序数 */
	LobDefragWorker *m_defragThread;    /** defrag线程 */
	RandDist *m_randDist;               /** 按概率随机生成类 */
	volatile bool m_flag;				/** 标志位，是否要停止工作线程*/
	volatile bool m_stop;               /** 是否停止测试*/
	friend class LobSblWorker;
	friend class LobSblVerifier;
	friend class LobDefragWorker;
};

/** 测试运行线程 */
class LobSblWorker : public Thread {
public:
	LobSblWorker(LobSblTestCase *testCase) : Thread("LobSblWorker") { 
		this->m_case = testCase;
	}
	void run() {
		m_case->doWork();
	}

private:
	LobSblTestCase *m_case;	/** 测试用例 */
};


/** 验证线程，用于验证Lob数据的正确性 */
class LobSblVerifier : public Task {
public:
	LobSblVerifier(LobSblTestCase *testCase, uint interval) : Task("LobSblVerifier", interval), m_case(testCase){
	}

	void run() {
		m_case->doVerify();
	}

private:
	LobSblTestCase *m_case;		/** 测试用例 */
};

/** defrag 线程，由于需要运行时间长，所以运行间隔需要大*/
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