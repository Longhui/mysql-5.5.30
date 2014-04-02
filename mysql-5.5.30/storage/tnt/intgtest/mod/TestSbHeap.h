/**
* 堆稳定性测试
*
* @author 谢可(xieke@corp.netease.com, ken@163.org)
*/

#ifndef _INTGTEST_STABILITY_HEAP_H
#define _INTGTEST_STABILITY_HEAP_H

#include <cppunit/extensions/HelperMacros.h>
#include "misc/Global.h"
#include "misc/Record.h"
#include "misc/TableDef.h"
#include "util/Sync.h"
#include "misc/Session.h"
#include "util/Thread.h"
#include "MemHeap.h"
#include "heap/Heap.h"
#include "api/Database.h"
#include "util/Thread.h"
#include "ResLogger.h"

using namespace std;
using namespace ntse;

class MemHeap;
class ActionOperator;

#define MAX_OP_PER_CONN 10

/** 线程行为 **/
enum ThreadAction {
	TA_GET,			/** get操作，验证数据 */
	TA_INSERT,		/** insert操作 */
	TA_DELETE,		/** del操作 */
	TA_UPDATE,		/** update操作 */
	TA_RSCANTBL,	/** 只读表扫描操作 */
	TA_MSCANTBL,	/** 修改表扫描操作 */
	TA_VERIFY,		/** 内存堆反向验证操作 */
	TA_SAMPLE,		/** 状态获取操作 */

	TA_MAX,			/** 计数用 */
	TA_PAUSE,		/** 暂停 */
	TA_STOP,		/** 停止 */
};

/** 非只读表扫描行为 **/
enum MScanAction {
	MSA_GET,		/** getNext */
	MSA_UPDATE,		/** updateCurrent */
	MSA_DELETE,		/** deleteCurrent */

	MSA_MAX,		/** 计数用 */
};

/** 动作配置 **/
class ActionConfig {
public:
	ActionConfig(const uint *taProp, const uint *msaProp);
	ActionConfig() {};
	void init(const uint *taProp, const uint *msaProp);
	ThreadAction getRandAction();
	MScanAction getRandScanAction();
private:
	uint m_taProportion[TA_MAX];
	uint m_msaProportion[MSA_MAX];
	friend class HeapSbTestConfig;
};

/** 堆稳定性测试配置 **/
class HeapSbTestConfig {
public:
	HeapSbTestConfig() {
		/* 默认三个配置都为0 */
		uint taProp[TA_MAX];
		uint msaProp[MSA_MAX];
		memset(taProp, 0, sizeof(taProp));
		memset(msaProp, 0, sizeof(msaProp));
		m_ins.init(taProp, msaProp);
		m_del.init(taProp, msaProp);
		m_wup.init(taProp, msaProp);
	}
	ActionConfig *insConf() {return &m_ins;}
	ActionConfig *delConf() {return &m_del;}
	ActionConfig *wupConf() {return &m_wup;}
	/* 有Ins字样为上升期配置，有Del字样为下降期配置，有WUp字样的为预热期配置 */
	void setInsProp(ThreadAction ta, uint prop) {setProp(&m_ins, ta, prop);}
	void setDelProp(ThreadAction ta, uint prop) {setProp(&m_del, ta, prop);}
	void setWUpProp(ThreadAction ta, uint prop) {setProp(&m_wup, ta, prop);}
	void setInsProp(uint *props) {setProp(&m_ins, props);}
	void setDelProp(uint *props) {setProp(&m_del, props);}
	void setWUpProp(uint *props) {setProp(&m_wup, props);}
	void setInsMsaProp(MScanAction ma, uint prop) {setMsaProp(&m_ins, ma, prop);}
	void setDelMsaProp(MScanAction ma, uint prop) {setMsaProp(&m_del, ma, prop);}
	void setWUpMsaProp(MScanAction ma, uint prop) {setMsaProp(&m_wup, ma, prop);}
private:
	ActionConfig m_ins;
	ActionConfig m_del;
	ActionConfig m_wup;
	void setProp(ActionConfig *cfg, ThreadAction ta, uint prop);
	void setProp(ActionConfig *cfg, uint * props);
	void setMsaProp(ActionConfig *cfg, MScanAction ma, uint prop);
};


/** 配置监察 **/ 
class ConfigWatcher {
public:
	ConfigWatcher(int emptyPct, int fullPct, HeapSbTestConfig *config, uint interval = 500)
		: m_downPct(emptyPct), m_upPct(fullPct), m_config(config), m_interval(interval) {};
	/**
	 * 根据MemHeap的使用情况调整配置指针
	 **/
	void watch(MemHeap *, ActionConfig **);
	ActionConfig *getWarmupConf() {return m_config->wupConf();}
	uint getInterval() {return m_interval;}
private:
	int m_downPct;
	int m_upPct;
	uint m_interval;
	HeapSbTestConfig *m_config;
};


/**
* 不同表的操作类
*/
class HeapOp { 
public:
	HeapOp(Database *db) : m_db(db) {};
	virtual ~HeapOp() {};
	virtual void close();
	/**
	 * 获取须更新列的序号
	 */
	virtual u16 * getUpdCols() = 0;
	/**
	 * 获取须更新列的数量
	 */
	virtual u16 getUpdColNum() = 0;
	/**
	 * 创建一个以ID为关键字的随机SubRecord
	 */
	virtual SubRecord * createSubRec(u64 ID = 0) = 0;
	virtual void updateSubRec(SubRecord *sr) = 0;
	virtual bool getSubRecord(Session *, RowId, SubRecord *, LockMode, RowLockHandle **);
	virtual MemHeapRid getMemHeapRid(SubRecord *subRecord) = 0;
	virtual bool compareSubRecord(Session *session, MemHeapRecord *mhRec, SubRecord *subRec) = 0;
	virtual Record *createRecord(MemHeapRid mhRid) = 0;
	virtual RowId insert(Session *session, Record *record, RowLockHandle **rlh);
	virtual bool del(Session *session, RowId rid);
	virtual bool update(Session *session, RowId rid, SubRecord *sr);
	virtual bool update(Session *session, RowId rid, Record *rec);
	virtual DrsHeapScanHandle *beginScan(Session *, LockMode, RowLockHandle **, bool returnLinkSrc = false);
	virtual bool getNext(DrsHeapScanHandle *scanHdl, SubRecord *subRec);
	virtual void endScan(DrsHeapScanHandle *scanHdl);
	virtual void deleteCurrent(DrsHeapScanHandle *scanHdl);
	virtual void updateCurrent(DrsHeapScanHandle *scanHdl, const SubRecord *subRecord);
	virtual const HeapStatus& getStatus();
	virtual const HeapStatusEx& getStatusEx();
protected:
	DrsHeap *m_heap;
	Database *m_db;
};


/**
 * 收集各线程完成情况，打印输出
 */
class Reporter {
public:
	Reporter() : m_lock("Reporter::lock", __FILE__, __LINE__) {
		for (int i = 0; i < TA_MAX; ++i) {
			m_taCnt[i] = 0;
		}
		for (int i = 0; i < MSA_MAX; ++i) {
			m_msaCnt[i] = 0;
		}
	};
	virtual void countActions(ActionOperator *aOp);
	virtual void report();
protected:
	Mutex m_lock;
	uint m_taCnt[TA_MAX];
	uint m_msaCnt[MSA_MAX];
};

/**
 * 堆稳定性测试的主线程类
 */
class HeapStabilityTest : public Thread {
public:
	HeapStabilityTest(const char* tableName, int threadCnt, ConfigWatcher *watcher ,Reporter *reporter, double dataBufRatio = 1.0) : Thread("Heap stability test.") {
		m_tblName = tableName;
		m_threadCnt = threadCnt;
		m_reporter = reporter;
		m_cfgWatcher = watcher;
		m_dataBufRatio = dataBufRatio;
	}
	virtual ~HeapStabilityTest();
protected:
	virtual void setup();
	virtual void run();
	virtual void configInsCfg(uint *taProp, uint *msaProp);
	virtual void configDelCfg(uint *taProp, uint *msaProp);
	const char *m_tblName;
	Config *m_cfg;
	Database *m_db;
	HeapOp *m_heapOp;
	int m_threadCnt;
	MemHeap *m_memHeap;
	//HeapSbTestConfig *m_config;
	Reporter *m_reporter;
	ConfigWatcher *m_cfgWatcher;
	double m_dataBufRatio;
	ResLogger *m_resLogger;
};


/**
 * 操作线程类
 */
class ActionOperator : public Thread {
public:
	ActionOperator(Database *db, HeapOp *heapOp, MemHeap *memHeap, Reporter *reporter);
	~ActionOperator();
protected:
	void run();
	void opGet(Connection *conn);
	void opIns(Connection *conn);
	void opDel(Connection *conn);
	void opUpd(Connection *conn);
	void opRTS(Connection *conn);
	void opMTS(Connection *conn);
	void opVry(Connection *conn);
	void opSmp(Connection *conn);

	Database *m_db;			/** 数据库 */ 
	HeapOp *m_heapOp;		/** 需要用到的堆操作封装 */

	MemHeap *m_memHeap;
	Reporter *m_reporter;

	uint m_taCnt[TA_MAX];
	uint m_msaCnt[MSA_MAX];

	friend class Reporter;
};




class HeapStabilityTestCase: public CPPUNIT_NS::TestFixture {
	CPPUNIT_TEST_SUITE(HeapStabilityTestCase);
	//CPPUNIT_TEST(testInsDelUpd);
	CPPUNIT_TEST(testFLRHeapCpuConsume);
	CPPUNIT_TEST(testFLRHeapIoConsume);
	CPPUNIT_TEST(testVLRHeapCpuConsume);
	CPPUNIT_TEST(testVLRHeapIoConsume);
	CPPUNIT_TEST_SUITE_END();

public:
	void setUp();
	void tearDown();

protected:
	void testInsDelUpd();
	void testFLRHeapCpuConsume();
	void testFLRHeapIoConsume();
	void testVLRHeapCpuConsume();
	void testVLRHeapIoConsume();
};



#endif

