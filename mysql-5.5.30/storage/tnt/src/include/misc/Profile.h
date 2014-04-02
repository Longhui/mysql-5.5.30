/**
 * Profile����
 *
 * @author ����(panning@corp.netease.com, panning@163.org)
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

/** �ں����Ķ�����з�������꣬��ʾҪ�������������Profileͳ��
 * ע������Profile����Ŀ���Ϊ��ʮ��ʱ�����ڣ���Ҫ��ִ��ʱ�䳬
 * �̵ĺ�������Profileͳ���Է�ֹ����Ӱ��ϵͳ����
 *
 * @param pid Profile ID
 */
#define PROFILE(pid) ProfilePoint __profile_point__(pid);

namespace ntse {

extern int g_funcListSize;

/**
 * �������PROFILE ID�ڴˡ�ע���뱣�����ﶨ���Profile ID��Profile.cpp
 * �е�g_funcNameList����һһ��Ӧ
 */
enum ProfileId {
	PI_ROOT,	/** ���ö�ջ�ĸ�������ɾ��*/
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
	
	PI_END		/** Ϊ����checkö�ٺ������ԣ�����ɾ��*/
};

/**
 * ����ID�뺯������ֵ��
 */
struct IdFuncPair {
	IdFuncPair(ProfileId id, const char* name) : m_id(id), m_Func(name) {}
	ProfileId	m_id;	/** ����id */
	const char*	m_Func;	/** ������ */
};

/**
 * ProfileDataSet�д洢��ÿһ��Profile�����ݼ�¼
 */
struct PointRecord {
public:
	PointRecord() : m_count(0), m_sumt(0), m_maxt(0) {}
	u64	m_count;	/** ���д��� */
	u64	m_sumt;		/** ������ʱ������ */
	u64	m_maxt;		/** ����������� */
};

/** �߳����� */
enum ThreadType {
	BG_THREAD,		/** ��̨�߳� */
	CONN_THREAD		/** ���Ӷ�Ӧ�߳� */
};

/**
 * Profile���Ʋ���
 */
enum ProfileControl {
	Shutdown_Clean = -1,	/** -1: �ر�Profile��������� */
	Shutdown_Keep = 0,		/** 0: �ر�Profile�������� */
	OpenIt = 1				/** 1: ����Profile */
};

/**
 * �̺߳�����˽�е�Profileͳ�ƽ������
 * ���а���ID�����ͺ�n*n��profile�������n�Ĵ�Сȡ����profile��Ķ���
 */
struct LocalProfResBox {
public:
	LocalProfResBox() : m_id(0), m_type((ThreadType)0), m_callMatrix(new PointRecord[g_funcListSize * g_funcListSize]){}
	~LocalProfResBox() {
		if (m_callMatrix) {
			delete[] m_callMatrix;
		}
	}
	u64			m_id;			/** �߳�ID����ͬ���߳���Ψһ */
	ThreadType	m_type;			/** �߳����� */
	PointRecord *m_callMatrix;	/** ͳ�ƽ������ */
};

/** �̵߳��ö�ջ��С */
const u8 FUNC_CALL_STACK_SIZE = 102;

/** �߳�Profile��Ϣ�ṹ */
struct ThdProfileInfo {
public:
	void prepareProfile(int id, ThreadType type, bool open);
	void endProfile();
	void openProfile();
	void shutdownClean();
	void shutdownKeep();
	void update(u64 duration);
	/**
	 * ���Profile�Ƿ���
	 *
	 * @return ����Profile�����Ƿ���
	 */
	bool profileOpened() {
		return m_openProfile;
	}
	/**
	 * ��ȡ�̻߳�����id
	 *
	 *@return ����id
	 */
	int getId() {
		return m_Id;
	}
	/**
	 * ��ȡ�߳�����
	 *
	 *@return �߳�����
	 */
	ThreadType getType() {
		return m_threadType;
	}
	/**
	 * �鿴�Ƿ������˹ر������־λ
	 *
	 * @return �ر������־λ
	 */
	bool shutCleanSetted() {
		return m_shutdownclean;
	}

	DLink<LocalProfResBox *>	*m_dlinkLocalResultBox;	/** �̺߳����Ӿֲ���Profile��������ڳصĶ����е�λ�� */
	u8		m_funcCallStack[FUNC_CALL_STACK_SIZE];	/** �������ö�ջ���溯��ID */
	u8		m_stackTop;								/** �������ö�ջ������ʼΪ0����m_stackTop==0ʱ��
													 * ����ջΪ�գ���ָ��ROOT
													 */
	int		m_Id;									/** �߳�id */
	ThreadType m_threadType;						/** �߳����� */
	bool	m_openProfile;							/** �Ƿ���Profile */
	bool m_shutdownclean;						/** Ϊ�ӳٹرղ�����߳�Profile�洢�ṹ���õı�־λ */
private:
	void cleanThreadProfileData();
};

typedef multimap<int, ThdProfileInfo *> ThdProfileInfoMap;
typedef ThdProfileInfoMap::iterator TPMIter;


/** ȫ��Profile����ʱʹ�õĽ����Ϣ�ṹ */
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
	const char*	m_caller;	/** ���ú�����*/
	const char*	m_funcName;	/** ������ */
	u64			m_count;	/** ���ô��� */
	u64			m_sumt;		/** ������ʱ�� */
	u64			m_maxt;		/** �������ʱ�� */
};

/** �̵߳�Profile����ʱʹ�õ���Ϣ�ṹ */
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
	u64 m_id;	/** �߳�ID */
};

struct Mutex;

/**
 * ȫ�ֵ�Profile������
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
	 * ��ȫ��Profile
	 */
	void openGlobalProfile() {
		m_openGlobalProfile = true;
	}
	/**
	 * ����ȫ��profile�Ƿ��
	 */
	bool globalProfileOpened() {
		return m_openGlobalProfile;
	}
	/**
	 * �ر�ȫ��Profile��������ȫ��Profile����
	 */
	void shutdownGlobalClean() {
		m_openGlobalProfile = false;
		memset(m_globalResultMatrix, 0, sizeof(PointRecord) * g_funcListSize * g_funcListSize);
	}
	/**
	 * �ر�ȫ��Profile������ȫ��Profile����
	 */
	void shutdownGlobalKeep() {
		m_openGlobalProfile = false;
	}
	/**
	 * �����߳�profile�Ƿ�Ĭ���Զ�����
	 */
	void setThreadProfileAutorun(bool autorun) {
		m_threadProfileAutoRun = autorun;
	}
	/**
	 * ��ȡ�߳�profile�Ƿ��Զ�����
	 */
	bool getThreadProfileAutorun() {
		return m_threadProfileAutoRun;
	}
	void regThdProfInfo(int id, ThdProfileInfo *ptrThdProfileInfo);
	void unregThdProfInfo(int id, ThdProfileInfo *ptrThdProfileInfo);
	Array<ProfileInfoRecord *>* getGlobalProfileInfos();
	Array<ThdProfileInfoRecord *>* getThdsProfileInfos(ThreadType type);

	PointRecord  *m_globalResultMatrix; /** ȫ�ֽ������ */
private:
	void expandLocIdleResPool(int factor);

	Mutex *m_mutex;			/** �������������� */
	Mutex *m_mapMutex;		/** �����߳�Prorfile��Ϣע��Map���� */
	DList<LocalProfResBox *> m_localIdleResultPool; /** �洢���е��߳�˽��profile��Ϣ���� */
	DList<LocalProfResBox *> m_localBusyResultPool; /** �洢����ʹ�õ��߳�˽��profile��Ϣ���� */
	
	ThdProfileInfoMap m_thdProfInfoMap;	/** �Ѿ�ע����߳�˽��Profile��Ϣ */
	bool m_openGlobalProfile;	/** �Ƿ���ȫ��Profile */
	bool m_threadProfileAutoRun; /** �߳�profile�Ƿ��Զ�ע�Ὺ��*/

	static const int INIT_LOCAL_RESULT_POOL_SIZE = 30;	/** �߳�˽��Profile����س�ʼ��С */
	static const int EXPAND_FACTOR = 10;				/** �߳�˽��Profile�������չ��С */
};


extern Profiler g_profiler; /** ȫ��ΨһProfiler */
extern TLS ThdProfileInfo	g_tlsProfileInfo; /** �̱߳��ش洢��Profile��Ϣ�ṹ */

/**
 * ProfilePoint,�ں����е�һ��Profile����
 */
class ProfilePoint {
public:
	/**
	 * ProfilePoint�Ĺ��캯��
	 *
	 * @param profileId Profile���ȫ��Ψһ��ID������ȷ����Ψһ�ԣ�������ֶ��Point����
	 *   ����Profile���ݵĳ�ͻ
	 */
	ProfilePoint(ProfileId profileId) {
		g_tlsProfileInfo.m_funcCallStack[++g_tlsProfileInfo.m_stackTop] = (u8)profileId;
		if (g_profiler.globalProfileOpened() || g_tlsProfileInfo.profileOpened())
			m_clock = System::clockCycles();
		else
			m_clock = 0;
	}
	/**
	 * ProfilePoint�������������ڴ˴����㺯������ʱ�䣬��ͨ��g_profiler����ȫ��profile��Ϣ��
	 * g_tlsProfileInfo�����߳�profile��Ϣ
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

	u64		m_clock;	/** ����ʱ��ʱ������������δ����ͳ����Ϊ0 */
};

}

#endif

#endif

#endif
