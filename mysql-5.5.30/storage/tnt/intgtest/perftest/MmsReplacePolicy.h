/**
 * 测试MMS替换策略效果
 *
 * @author 邵峰(shaofeng@corp.netease.com sf@163.org)
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

	u64				m_dataVolume;	/** 数据量 */
	MmsReplaceVerifier	*m_verifierThread;	/** 验证线程 */
	MmsReplaceWorker	**m_workerThreads;	/** 工作线程组 */
	int				m_nrWorkThreads;	/** 工作线程个数 */
	Database		*m_db;				/** 测试数据库实例 */
	Array<MmsTable *> m_mmsTblArray;	/** 测试MMS表数组 */
	Mms				*m_mms;				/** 测试MMS全局实例 */
	MmsTable		**m_mmsTables;		/** MMS表数组 */
	DrsHeap			**m_heaps;			/** 堆数组 */
	Table			**m_tables;			/** 表数组 */
	int				m_numTables;		/** 表个数 */
	int				m_recCount;			/** 记录数 */
	Config			*m_cfg;				/** 数据库配置 */
	TableDef		**m_tableDefs;		/** 表配置 */
	RowId			**m_rowIdTables;	/** RowID映射表 */
	ZipfRandom		**m_zipfRandoms;	/** zipf随机产生器 */

	long			m_numVerify;		/** 验证操作次数 */
	long			m_numSelect;		/** 选择操作次数 */
	long			m_numDelete;		/** 删除操作次数 */
	long			m_numInsert;		/** 插入操作次数 */
	long			m_numUpdate;		/** 更新操作次数 */
	long			m_numGet;			/** 获取操作次数 */
	long			m_numNothing;		/** 空操作次数 */
	long			m_beginTime;		/** 开始计时 */
	long			m_endTime;			/** 结束计时 */
	long			m_numPreSelect;		/** 选择操作次数 */
	long			m_numPreDelete;		/** 删除操作次数 */
	long			m_numPreInsert;		/** 插入操作次数 */
	long			m_numPreUpdate;		/** 更新操作次数 */
	long			m_numPreGet;		/** 获取操作次数 */
	long			m_numPreNothing;	/** 空操作次数 */

	u64				m_mmsRecordQueries;	/** MMS记录查询次数	*/			
	u64				m_mmsRecordQueryHits;	/** MMS记录查询命中次数 */
	u64				m_mmsRecordInserts;	/** MMS记录插入次数	*/			
	u64				m_mmsRecordDeletes;	/** MMS记录被删除次数 */	
	u64				m_mmsRecordUpdates;	/** MMS记录被更新次数 */
	u64				m_mmsRecordVictims; /** MMS记录被替换次数 */
	u64				m_mmsPageVictims;	/** MMS缓存页被替换次数 */	
	u64				m_mmsOccupiedPages;	/** MMS占用的内存页个数 */
	u64				m_mmsFreePages;		/** MMS占用的自由空闲页个数 */

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

/** 测试运行线程 */
class MmsReplaceWorker : public Task {
public:
	MmsReplaceWorker(MmsReplacePolicyTest *testCase) : Task("MmsReplaceWorker", 0) { 
		m_case = testCase;
	}

	void run() {
		m_case->doWork();
	}

private:
	MmsReplacePolicyTest *m_case;	/** 测试用例 */
};

/** 验证线程，用于验证MMS数据的正确性 */
class MmsReplaceVerifier : public Task {
public:
	MmsReplaceVerifier(MmsReplacePolicyTest *testCase, uint interval) : Task("MmsReplaceVerifier", interval), m_case(testCase){
	}

	void run() {
		m_case->doVerify();
	}

private:
	MmsReplacePolicyTest *m_case;		/** 测试用例 */
};


#endif
