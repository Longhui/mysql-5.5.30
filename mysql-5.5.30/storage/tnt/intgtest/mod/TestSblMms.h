/**
* MMS稳定性测试
*
* @author 邵峰(shaofeng@corp.netease.com, sf@163.org)
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
	byte	m_mode;				/** 当前运行模式 */
	Atomic<long> dataVolumn;	/** 数据量 */
	// MmsSblMaster	*m_masterThread;	/** 主测试线程 */
	//MmsSblTimer		*m_timerThread;		/** 时间线程 */
	MmsSblVerifier	*m_verifierThread;	/** 验证线程 */
	MmsSblWorker	**m_workerThreads;	/** 工作线程组 */
	int				m_nrWorkThreads;	/** 工作线程个数 */
	Database		*m_db;				/** 测试数据库实例 */
	Array<MmsTable *> m_mmsTblArray;	/** 测试MMS表数组 */
	Mms				*m_mms;				/** 测试MMS全局实例 */
	MmsTable		**m_mmsTables;		/** MMS表数组 */
	DrsHeap			**m_heaps;			/** 堆数组 */
	Table			**m_tables;			/** 表数组 */
	MemHeap			**m_memHeaps;		/** 内存堆数组 */
	int				m_numTables;		/** 记录数 */
	Config			*m_cfg;				/** 数据库配置 */
	TableDef		**m_tableDefs;		/** 表配置 */

	long			m_numVerify;		/** 验证操作次数 */
	long			m_numSelect;		/** 选择操作次数 */
	long			m_numDelete;		/** 删除操作次数 */
	long			m_numInsert;		/** 插入操作次数 */
	long			m_numUpdate;		/** 更新操作次数 */
	long			m_beginTime;		/** 开始计时 */
	long			m_endTime;			/** 结束计时 */
	long			m_numPreSelect;		/** 选择操作次数 */
	long			m_numPreDelete;		/** 删除操作次数 */
	long			m_numPreInsert;		/** 插入操作次数 */
	long			m_numPreUpdate;		/** 更新操作次数 */

	u64				m_mmsRecordQueries;	/** MMS记录查询次数	*/			
	u64				m_mmsRecordQueryHits;	/** MMS记录查询命中次数 */
	u64				m_mmsRecordInserts;	/** MMS记录插入次数	*/			
	u64				m_mmsRecordDeletes;	/** MMS记录被删除次数 */	
	u64				m_mmsRecordUpdates;	/** MMS记录被更新次数 */
	u64				m_mmsRecordVictims;    /** MMS记录被替换次数 */
	u64				m_mmsPageVictims;		/** MMS缓存页被替换次数 */	
	u64				m_mmsOccupiedPages;	/** MMS占用的内存页个数 */
	u64				m_mmsFreePages;		/** MMS占用的自由空闲页个数 */	

	static const uint SELECT_WEIGHT;	/** 选择操作权重 */
	static const uint DELETE_WEIGHT;	/** 删除操作权重 */
	// static const uint GET_BY_RID_WEIGHT; /** RID查询权重 */
	static const uint UPDATE_WEIGHT_WHEN_SELECT; /** 选择操作中执行更新权重 */
	static const uint INSERT_MMS_WEIGHT_WHEN_SELECT; /** 选择操作中回写MMS权重 */
	//static const uint PRIMARY_KEY_UPDATE_WEIGHT; /** 主键更新权重 */
	static const uint MAX_REC_COUNT_IN_TABLE;	/** 表中最多记录数 */
	static const uint TABLE_NUM_IN_SAME_TEMPLATE; /** 同一模板的表个数 */
	static const uint WORKING_THREAD_NUM;		/** 工作线程数 */
	static const uint VERIFY_DURATION;			/** 验证间隔时间 */
	//static const uint CHECKPOINT_DURATION;		/** 检查点刷新间隔时间 */
	static const uint RUN_DURATION;				/** 运行间隔时间 */
	static const uint FOREVER_DURATION;			/** 永远运行 */
	
	friend class MmsSblMaster;
	friend class MmsSblWorker;
};

/** 测试运行线程 */
class MmsSblWorker : public Task {
public:
	MmsSblWorker(MmsSblTestCase *testCase) : Task("MmsSblWorker", 0) { 
		this->m_case = testCase;
	}

	void run() {
		m_case->doWork();
	}

private:
	MmsSblTestCase *m_case;	/** 测试用例 */
};

/** 测试时钟线程，用于执行检查点刷写MMS脏数据 */
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
//	MmsSblTestCase *m_case;		/** 测试用例 */
//};

/** 验证线程，用于验证MMS数据的正确性 */
class MmsSblVerifier : public Task {
public:
	MmsSblVerifier(MmsSblTestCase *testCase, uint interval) : Task("MmsSblVerifier", interval), m_case(testCase){
	}

	void run() {
		m_case->doVerify();
	}

private:
	MmsSblTestCase *m_case;		/** 测试用例 */
};

#endif // _NTSESBLTEST_MMS_H_
