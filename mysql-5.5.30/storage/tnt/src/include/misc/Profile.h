/**
 * Profile功能
 *
 * @author 潘宁(panning@corp.netease.com, panning@163.org)
 */

#ifndef NTSE_PROFILE
#define PROFILE(pid)
#else

#ifdef NTSE_PROFILE

#ifndef _NTSE_PROFILE_H_
#define _NTSE_PROFILE_H_

#include <iostream>
#include <map>
#include <assert.h>
#include "util/Portable.h"
#include "util/DList.h"
#include "util/Array.h"

using namespace std;

/** 在函数的顶层块中放置这个宏，表示要对这个函数进行Profile统计
 * 注：由于Profile本身的开销为几十个时钟周期，不要对执行时间超
 * 短的函数进行Profile统计以防止过度影响系统性能
 *
 * @param pid Profile ID
 */
#define PROFILE(pid) ProfilePoint __profile_point__(pid);

namespace ntse {

extern int g_funcListSize;

/**
 * 定义各个PROFILE ID于此。注意请保持这里定义的Profile ID与Profile.cpp
 * 中的g_funcNameList数组一一对应
 */
enum ProfileId {
	PI_ROOT,	/** 调用堆栈的根，不可删除*/
#ifdef NTSE_UNIT_TEST
	TestedClass_haveProfilePoint, TestedClass_doA, TestedClass_doB, TestedClass_doC, TestedClass_doD,
#endif
	// Table
	PI_Table_tableScan,
	PI_Table_indexScan,
	PI_Table_positionScan,
	PI_Table_getNext,
	PI_Table_updateCurrent,
	PI_Table_deleteCurrent,
	PI_Table_endScan,
	PI_Table_insert,
	PI_Table_insertForDupUpdate,
	PI_Table_updateDuplicate,
	PI_Table_deleteDuplicate,
	PI_Table_freeIUSequenceDirect,
	// Heap
	PI_FixedLengthRecordHeap_beginScan,
	PI_FixedLengthRecordHeap_getNext,
	PI_FixedLengthRecordHeap_updateCurrent,
	PI_FixedLengthRecordHeap_deleteCurrent,
	PI_FixedLengthRecordHeap_endScan,
	PI_FixedLengthRecordHeap_getSubRecord,
	PI_FixedLengthRecordHeap_getRecord,
	PI_FixedLengthRecordHeap_insert,
	PI_FixedLengthRecordHeap_del,
	PI_FixedLengthRecordHeap_update_SubRecord,
	PI_FixedLengthRecordHeap_update_Record,
	PI_VariableLengthRecordHeap_beginScan,
	PI_VariableLengthRecordHeap_getNext,
	PI_VariableLengthRecordHeap_updateCurrent,
	PI_VariableLengthRecordHeap_deleteCurrent,
	PI_VariableLengthRecordHeap_endScan,
	PI_VariableLengthRecordHeap_getSubRecord,
	PI_VariableLengthRecordHeap_getRecord,
	PI_VariableLengthRecordHeap_insert,
	PI_VariableLengthRecordHeap_del,
	PI_VariableLengthRecordHeap_update_SubRecord,
	PI_VariableLengthRecordHeap_update_Record,
	// Index
	PI_DrsIndice_insertIndexEntries,
	PI_DrsIndice_deleteIndexEntries,
	PI_DrsIndice_updateIndexEntries,
	PI_DrsIndex_getByUniqueKey,
	PI_DrsIndex_beginScan,
	PI_DrsIndex_getNext,
	PI_DrsIndex_deleteCurrent,
	PI_DrsIndex_endScan,
	PI_DrsIndex_recordsInRange,
	PI_DrsIndex_insertSMO,
	PI_DrsIndex_deleteSMO,
	// MMS
	PI_MmsTable_getByPrimaryKey,
	PI_MmsTable_getByRid,
	PI_MmsTable_putIfNotExist,
	PI_MmsTable_update,
	PI_MmsTable_del,
	PI_MmsTable_getSubRecord,
	PI_MmsTable_getRecord,
	PI_MmsTable_evictMmsRecord,
	PI_MmsTable_evictMmsPage,
	PI_MmsTable_doTouch,
	// LOB
	PI_LobStorage_get,
	PI_LobStorage_insert,
	PI_LobStorage_del,
	PI_LobStorage_update,
	// Buffer
	PI_Buffer_updateExtendStatus,
#ifdef NTSE_KEYVALUE_SERVER
	// KeyValue
	PI_KeyValue_get,
	PI_KeyValue_multiGet,
	PI_KeyValue_put,
	PI_KeyValue_set,
	PI_KeyValue_replace,
	PI_KeyValue_update,
	PI_KeyValue_putOrUpdate,
	PI_KeyValue_remove,
	PI_KeyValue_getTableDef,
#endif
	
	PI_END		/** 为方便check枚举函数名对，不可删除*/
};

/**
 * 函数ID与函数名的值对
 */
struct IdFuncPair {
	IdFuncPair(ProfileId id, const char* name) : m_id(id), m_Func(name) {}
	ProfileId	m_id;	/** 函数id */
	const char*	m_Func;	/** 函数名 */
};

/**
 * ProfileDataSet中存储的每一个Profile的数据记录
 */
struct PointRecord {
public:
	PointRecord() : m_count(0), m_sumt(0), m_maxt(0) {}
	u64	m_count;	/** 运行次数 */
	u64	m_sumt;		/** 总运行时钟周期 */
	u64	m_maxt;		/** 最大运行周期 */
};

/** 线程类型 */
enum ThreadType {
	BG_THREAD,		/** 后台线程 */
	CONN_THREAD		/** 连接对应线程 */
};

/**
 * Profile控制参数
 */
enum ProfileControl {
	Shutdown_Clean = -1,	/** -1: 关闭Profile并清除数据 */
	Shutdown_Keep = 0,		/** 0: 关闭Profile保留数据 */
	OpenIt = 1				/** 1: 开启Profile */
};

/**
 * 线程和连接私有的Profile统计结果盒子
 * 其中包括ID、类型和n*n的profile结果矩阵，n的大小取决于profile点的多少
 */
struct LocalProfResBox {
public:
	LocalProfResBox() : m_id(0), m_type((ThreadType)0), m_callMatrix(new PointRecord[g_funcListSize * g_funcListSize]){}
	~LocalProfResBox() {
		if (m_callMatrix) {
			delete[] m_callMatrix;
		}
	}
	u64			m_id;			/** 线程ID，在同类线程中唯一 */
	ThreadType	m_type;			/** 线程类型 */
	PointRecord *m_callMatrix;	/** 统计结果矩阵 */
};

/** 线程调用堆栈大小 */
const u8 FUNC_CALL_STACK_SIZE = 102;

/** 线程Profile信息结构 */
struct ThdProfileInfo {
public:
	void prepareProfile(int id, ThreadType type, bool open);
	void endProfile();
	void openProfile();
	void shutdownClean();
	void shutdownKeep();
	void update(u64 duration);
	/**
	 * 检测Profile是否开启
	 *
	 * @return 返回Profile功能是否开启
	 */
	bool profileOpened() {
		return m_openProfile;
	}
	/**
	 * 获取线程或连接id
	 *
	 *@return 返回id
	 */
	int getId() {
		return m_Id;
	}
	/**
	 * 获取线程类型
	 *
	 *@return 线程类型
	 */
	ThreadType getType() {
		return m_threadType;
	}
	/**
	 * 查看是否设置了关闭清除标志位
	 *
	 * @return 关闭清除标志位
	 */
	bool shutCleanSetted() {
		return m_shutdownclean;
	}

	DLink<LocalProfResBox *>	*m_dlinkLocalResultBox;	/** 线程和连接局部的Profile结果矩阵在池的队列中的位置 */
	u8		m_funcCallStack[FUNC_CALL_STACK_SIZE];	/** 函数调用堆栈，存函数ID */
	u8		m_stackTop;								/** 函数调用堆栈顶，初始为0，当m_stackTop==0时，
													 * 调用栈为空，即指向ROOT
													 */
	int		m_Id;									/** 线程id */
	ThreadType m_threadType;						/** 线程类型 */
	bool	m_openProfile;							/** 是否开启Profile */
	bool m_shutdownclean;						/** 为延迟关闭并清空线程Profile存储结构设置的标志位 */
private:
	void cleanThreadProfileData();
};

typedef multimap<int, ThdProfileInfo *> ThdProfileInfoMap;
typedef ThdProfileInfoMap::iterator TPMIter;


/** 全局Profile汇总时使用的结果信息结构 */
struct ProfileInfoRecord {
public:
	ProfileInfoRecord(const char *caller, const char *funcName, u64 count, u64 sumt, u64 maxt) 
		:m_caller(caller), m_funcName(funcName), m_count(count), m_sumt(sumt), m_maxt(maxt) {
	}
	virtual ~ProfileInfoRecord() {
	}
	const char* getCaller() {
		return m_caller;
	}
	const char* getFuncName() {
		return m_funcName;
	}
	u64 getCount() {
		return m_count;
	}
	u64 getSumT() {
		return m_sumt;
	}
	u64 getAvgT() {
		return m_sumt / m_count; 
	}
	u64 getMaxT() {
		return m_maxt;
	}

private:
	const char*	m_caller;	/** 调用函数名*/
	const char*	m_funcName;	/** 函数名 */
	u64			m_count;	/** 调用次数 */
	u64			m_sumt;		/** 总运行时间 */
	u64			m_maxt;		/** 最大运行时间 */
};

/** 线程的Profile汇总时使用的信息结构 */
struct ThdProfileInfoRecord: public ProfileInfoRecord {
public:
	ThdProfileInfoRecord(u64 id, const char* caller, const char *funcName, u64 count, u64 sumt, u64 maxt)
		: ProfileInfoRecord(caller, funcName, count, sumt, maxt), m_id(id) {
	}
	~ThdProfileInfoRecord() {
	}
	u64 getId() {
		return m_id;
	}

private:
	u64 m_id;	/** 线程ID */
};

struct Mutex;

/**
 * 全局的Profile控制器
 */
class Profiler {
public:
	Profiler();
	~Profiler();
	bool checkFuncPairs();
	DLink<LocalProfResBox *>* getDlinkLocResBox();
	void releaseDlinkLocResBox(DLink<LocalProfResBox *> *localResBox);
	bool control(int id, ThreadType type, ProfileControl value);
	/**
	 * 打开全局Profile
	 */
	void openGlobalProfile() {
		m_openGlobalProfile = true;
	}
	/**
	 * 返回全局profile是否打开
	 */
	bool globalProfileOpened() {
		return m_openGlobalProfile;
	}
	/**
	 * 关闭全局Profile，并清理全局Profile数据
	 */
	void shutdownGlobalClean() {
		m_openGlobalProfile = false;
		memset(m_globalResultMatrix, 0, sizeof(PointRecord) * g_funcListSize * g_funcListSize);
	}
	/**
	 * 关闭全局Profile，保留全局Profile数据
	 */
	void shutdownGlobalKeep() {
		m_openGlobalProfile = false;
	}
	/**
	 * 设置线程profile是否默认自动开启
	 */
	void setThreadProfileAutorun(bool autorun) {
		m_threadProfileAutoRun = autorun;
	}
	/**
	 * 读取线程profile是否自动开启
	 */
	bool getThreadProfileAutorun() {
		return m_threadProfileAutoRun;
	}
	void regThdProfInfo(int id, ThdProfileInfo *ptrThdProfileInfo);
	void unregThdProfInfo(int id, ThdProfileInfo *ptrThdProfileInfo);
	Array<ProfileInfoRecord *>* getGlobalProfileInfos();
	Array<ThdProfileInfoRecord *>* getThdsProfileInfos(ThreadType type);

	PointRecord  *m_globalResultMatrix; /** 全局结果矩阵 */
private:
	void expandLocIdleResPool(int factor);

	Mutex *m_mutex;			/** 保护结果数组的锁 */
	Mutex *m_mapMutex;		/** 保护线程Prorfile信息注册Map的锁 */
	DList<LocalProfResBox *> m_localIdleResultPool; /** 存储空闲的线程私有profile信息对象 */
	DList<LocalProfResBox *> m_localBusyResultPool; /** 存储正在使用的线程私有profile信息对象 */
	
	ThdProfileInfoMap m_thdProfInfoMap;	/** 已经注册的线程私有Profile信息 */
	bool m_openGlobalProfile;	/** 是否开启全局Profile */
	bool m_threadProfileAutoRun; /** 线程profile是否自动注册开启*/

	static const int INIT_LOCAL_RESULT_POOL_SIZE = 30;	/** 线程私有Profile结果池初始大小 */
	static const int EXPAND_FACTOR = 10;				/** 线程私有Profile结果池扩展大小 */
};


extern Profiler g_profiler; /** 全局唯一Profiler */
extern TLS ThdProfileInfo	g_tlsProfileInfo; /** 线程本地存储的Profile信息结构 */

/**
 * ProfilePoint,在函数中的一个Profile检查点
 */
class ProfilePoint {
public:
	/**
	 * ProfilePoint的构造函数
	 *
	 * @param profileId Profile点的全局唯一的ID，必须确定其唯一性，否则出现多个Point交叉
	 *   更新Profile数据的冲突
	 */
	ProfilePoint(ProfileId profileId) {
		g_tlsProfileInfo.m_funcCallStack[++g_tlsProfileInfo.m_stackTop] = (u8)profileId;
		if (g_profiler.globalProfileOpened() || g_tlsProfileInfo.profileOpened())
			m_clock = System::clockCycles();
		else
			m_clock = 0;
	}
	/**
	 * ProfilePoint的析构函数，在此处计算函数运行时间，并通过g_profiler更新全局profile信息，
	 * g_tlsProfileInfo更新线程profile信息
	 */
	~ProfilePoint() {
		if(m_clock == 0 || (!g_profiler.globalProfileOpened() && !g_tlsProfileInfo.profileOpened())){
			g_tlsProfileInfo.m_stackTop--;
			return;
		}
		update();
	}
private:
	void update();

	u64		m_clock;	/** 构造时的时钟周期数，若未开启统计则为0 */
};

}

#endif

#endif

#endif
