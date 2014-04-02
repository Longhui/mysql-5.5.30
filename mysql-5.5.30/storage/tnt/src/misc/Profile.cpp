/**
 * Profile����
 *
 * @author ����(panning@corp.netease.com, panning@163.org)
 */

#ifdef NTSE_PROFILE

#include "misc/Profile.h"
#include "util/Thread.h"
#include "util/Sync.h"

using namespace std;
using namespace ntse;

namespace ntse {

/**
 * ��ProfileId�뺯�����ƶ�Ӧ��ϵ����Ҫ����Profile.h�ļ����ProfileIdö������
 * ͬʱ���������g_funcNameList����
 */
static const IdFuncPair g_funcNameList[] = {
	IdFuncPair(PI_ROOT, "ROOT"),	/** ���ö�ջ�ĸ�������ɾ��*/
#ifdef NTSE_UNIT_TEST
	IdFuncPair(TestedClass_haveProfilePoint, "TestedClass::haveProfilePoint"),
	IdFuncPair(TestedClass_doA, "TestedClass::doA"),
	IdFuncPair(TestedClass_doB, "TestedClass::doB"),
	IdFuncPair(TestedClass_doC, "TestedClass::doC"),
	IdFuncPair(TestedClass_doD, "TestedClass::doD"),
#endif
	// Table
	IdFuncPair(PI_Table_tableScan, "Table::tableScan"),
	IdFuncPair(PI_Table_indexScan, "Table::indexScan"),
	IdFuncPair(PI_Table_positionScan, "Table::positionScan"),
	IdFuncPair(PI_Table_getNext, "Table::getNext"),
	IdFuncPair(PI_Table_updateCurrent, "Table::updateCurrent"),
	IdFuncPair(PI_Table_deleteCurrent, "Table::deleteCurrent"),
	IdFuncPair(PI_Table_endScan, "Table::endScan"),
	IdFuncPair(PI_Table_insert, "Table::insert"),
	IdFuncPair(PI_Table_insertForDupUpdate, "Table::insertForDupUpdate"),
	IdFuncPair(PI_Table_updateDuplicate, "Table::updateDuplicate"),
	IdFuncPair(PI_Table_deleteDuplicate, "Table::deleteDuplicate"),
	IdFuncPair(PI_Table_freeIUSequenceDirect, "Table::freeIUSequenceDirect"),
	// Heap
	IdFuncPair(PI_FixedLengthRecordHeap_beginScan, "FixedLengthRecordHeap::beginScan"),
	IdFuncPair(PI_FixedLengthRecordHeap_getNext, "FixedLengthRecordHeap::getNext"),
	IdFuncPair(PI_FixedLengthRecordHeap_updateCurrent, "FixedLengthRecordHeap::updateCurrent"),
	IdFuncPair(PI_FixedLengthRecordHeap_deleteCurrent, "FixedLengthRecordHeap::deleteCurrent"),
	IdFuncPair(PI_FixedLengthRecordHeap_endScan, "FixedLengthRecordHeap::endScan"),
	IdFuncPair(PI_FixedLengthRecordHeap_getSubRecord, "FixedLengthRecordHeap::getSubRecord"),
	IdFuncPair(PI_FixedLengthRecordHeap_getRecord, "FixedLengthRecordHeap::getRecord"),
	IdFuncPair(PI_FixedLengthRecordHeap_insert, "FixedLengthRecordHeap::insert"),
	IdFuncPair(PI_FixedLengthRecordHeap_del, "FixedLengthRecordHeap::del"),
	IdFuncPair(PI_FixedLengthRecordHeap_update_SubRecord, "FixedLengthRecordHeap::update(SubRecord)"),
	IdFuncPair(PI_FixedLengthRecordHeap_update_Record, "FixedLengthRecordHeap::update(Record)"),
	IdFuncPair(PI_VariableLengthRecordHeap_beginScan, "VariableLengthRecordHeap::beginScan"),
	IdFuncPair(PI_VariableLengthRecordHeap_getNext, "VariableLengthRecordHeap::getNext"),
	IdFuncPair(PI_VariableLengthRecordHeap_updateCurrent, "VariableLengthRecordHeap::updateCurrent"),
	IdFuncPair(PI_VariableLengthRecordHeap_deleteCurrent, "VariableLengthRecordHeap::deleteCurrent"),
	IdFuncPair(PI_VariableLengthRecordHeap_endScan, "VariableLengthRecordHeap::endScan"),
	IdFuncPair(PI_VariableLengthRecordHeap_getSubRecord, "VariableLengthRecordHeap::getSubRecord"),
	IdFuncPair(PI_VariableLengthRecordHeap_getRecord, "VariableLengthRecordHeap::getRecord"),
	IdFuncPair(PI_VariableLengthRecordHeap_insert, "VariableLengthRecordHeap::insert"),
	IdFuncPair(PI_VariableLengthRecordHeap_del, "VariableLengthRecordHeap::del"),
	IdFuncPair(PI_VariableLengthRecordHeap_update_SubRecord, "VariableLengthRecordHeap::update(SubRecord)"),
	IdFuncPair(PI_VariableLengthRecordHeap_update_Record, "VariableLengthRecordHeap::update(Record)"),
	// Index
	IdFuncPair(PI_DrsIndice_insertIndexEntries, "DrsIndice::insertIndexEntries"),
	IdFuncPair(PI_DrsIndice_deleteIndexEntries, "DrsIndice::deleteIndexEntries"),
	IdFuncPair(PI_DrsIndice_updateIndexEntries, "DrsIndice::updateIndexEntries"),
	IdFuncPair(PI_DrsIndex_getByUniqueKey, "DrsIndice::getByUniqueKey"),
	IdFuncPair(PI_DrsIndex_beginScan, "DrsIndice::beginScan"),
	IdFuncPair(PI_DrsIndex_getNext, "DrsIndice::getNext"),
	IdFuncPair(PI_DrsIndex_deleteCurrent, "DrsIndice::deleteCurrent"),
	IdFuncPair(PI_DrsIndex_endScan, "DrsIndice::endScan"),
	IdFuncPair(PI_DrsIndex_recordsInRange, "DrsIndice::recordsInRange"),
	IdFuncPair(PI_DrsIndex_insertSMO, "DrsIndice::insertSMO"),
	IdFuncPair(PI_DrsIndex_deleteSMO, "DrsIndice::deleteSMO"),
	// MMS
	IdFuncPair(PI_MmsTable_getByPrimaryKey, "MmsTable::getByPrimaryKey"),
	IdFuncPair(PI_MmsTable_getByRid, "MmsTable::getByRid"),
	IdFuncPair(PI_MmsTable_putIfNotExist, "MmsTable::putIfNotExist"),
	IdFuncPair(PI_MmsTable_update, "MmsTable::update"),
	IdFuncPair(PI_MmsTable_del, "MmsTable::del"),
	IdFuncPair(PI_MmsTable_getSubRecord, "MmsTable::getSubRecord"),
	IdFuncPair(PI_MmsTable_getRecord, "MmsTable::getRecord"),
	IdFuncPair(PI_MmsTable_evictMmsRecord, "MmsTable::evictMmsRecord"),
	IdFuncPair(PI_MmsTable_evictMmsPage, "MmsTable::evictMmsPage"),
	IdFuncPair(PI_MmsTable_doTouch, "MmsTable::doTouch"),
	// LOB
	IdFuncPair(PI_LobStorage_get, "LobStorage::get"),
	IdFuncPair(PI_LobStorage_insert, "LobStorage::insert"),
	IdFuncPair(PI_LobStorage_del, "LobStorage::del"),
	IdFuncPair(PI_LobStorage_update, "LobStorage::update"),
	// Buffer
	IdFuncPair(PI_Buffer_updateExtendStatus, "Buffer::updateExtendStatus"),
	
#ifdef NTSE_KEYVALUE_SERVER
	IdFuncPair(PI_KeyValue_get, "KeyValueHandler::get"),
	IdFuncPair(PI_KeyValue_multiGet, "KeyValueHandler::multi_get"),
	IdFuncPair(PI_KeyValue_put, "KeyValueHandler::put"),
	IdFuncPair(PI_KeyValue_set, "KeyValueHandler::setrec"),
	IdFuncPair(PI_KeyValue_replace, "KeyValueHandler::replace"),
	IdFuncPair(PI_KeyValue_update, "KeyValueHandler::update"),
	IdFuncPair(PI_KeyValue_putOrUpdate, "KeyValueHandler::put_or_update"),
	IdFuncPair(PI_KeyValue_remove, "KeyValueHandler::remove"),
	IdFuncPair(PI_KeyValue_getTableDef, "KeyValueHandler::getTableDef"),
#endif

	IdFuncPair(PI_END, "END")		/** Ϊ����checkö�ٺ������ԣ�����ɾ��*/
};

int g_funcListSize = sizeof(g_funcNameList) / sizeof(IdFuncPair); /** ͨ������õ�Profile����Ŀ��ROOT��ռ��һ���ռ�*/

Profiler g_profiler; /** ȫ��Ψһ��Profiler */
TLS ThdProfileInfo	g_tlsProfileInfo; /** �̱߳��ص�Profile��Ϣ */

/**
 * ΪProfile����׼��������߳�˽�е�profile��Ϣ�洢�ṹ
 *
 * @param id ��̨�̻߳����ӵ�id
 * @param type �߳�����
 * @param open �Ƿ���׼��ʱ����profile
 */
void ThdProfileInfo::prepareProfile(int id, ThreadType type, bool open) {
	if (!m_openProfile)
		m_openProfile = open;
	m_Id = id;
	m_threadType = type;
	if (m_shutdownclean && m_dlinkLocalResultBox) {
		g_profiler.releaseDlinkLocResBox(m_dlinkLocalResultBox);
		m_dlinkLocalResultBox = NULL;
	}
	m_shutdownclean = false;
	g_profiler.regThdProfInfo(id, this);
}

/**
 * �߳̽�������ã�ע����Profiler�е�ע����Ϣ
 */
void ThdProfileInfo::endProfile() {
	m_openProfile = false;
	m_shutdownclean = false;
	g_profiler.unregThdProfInfo(m_Id, this);
}

/**
 * ����Profile���ܣ�����߳�˽�е�profile��Ϣ�洢�ṹ
 */
void ThdProfileInfo::openProfile() {
	m_openProfile = true;
	if (m_shutdownclean && m_dlinkLocalResultBox) {
		g_profiler.releaseDlinkLocResBox(m_dlinkLocalResultBox);
		m_dlinkLocalResultBox = NULL;
	}
	m_shutdownclean = false;
}

/**
 * �ر�Profile���ܲ�����Profile���
 */
void ThdProfileInfo::shutdownClean() {
	m_openProfile = false;
	m_shutdownclean = true;
}

/**
 * �ر�Profile������Profile���
 */
void ThdProfileInfo::shutdownKeep() {
	m_openProfile = false;
}

/**
 * ����ȫ�ֺ��̱߳��ص�Profile��Ϣ
 * �����˺�m_stackTopΪ0ʱ��˵�����ö�ջΪ���޵��ù�ϵ����Ҫ����Matrix[0][profileId]�Ľ��
 * �����˺�m_stackTop���㣬����ָ����IDΪProfilePoint���ں����ĵ��ú�������Ҫ����Matrix[stack[top]][profileid]��Ӧ��Profile���
 */
void ProfilePoint::update() {
	u64 duration = System::clockCycles() - m_clock;
	g_tlsProfileInfo.m_stackTop--;
	int index = g_tlsProfileInfo.m_funcCallStack[g_tlsProfileInfo.m_stackTop] * g_funcListSize
		+ g_tlsProfileInfo.m_funcCallStack[g_tlsProfileInfo.m_stackTop + 1];
	if (g_profiler.globalProfileOpened()) {
		g_profiler.m_globalResultMatrix[index].m_count++;
		g_profiler.m_globalResultMatrix[index].m_sumt += duration;
		if (g_profiler.m_globalResultMatrix[index].m_maxt < duration) {
			g_profiler.m_globalResultMatrix[index].m_maxt = duration;
		}
	}
	if (g_tlsProfileInfo.profileOpened()) {
		if (!g_tlsProfileInfo.m_dlinkLocalResultBox) {
			g_tlsProfileInfo.m_dlinkLocalResultBox = g_profiler.getDlinkLocResBox();
			g_tlsProfileInfo.m_dlinkLocalResultBox->get()->m_id = g_tlsProfileInfo.m_Id;
			g_tlsProfileInfo.m_dlinkLocalResultBox->get()->m_type = g_tlsProfileInfo.m_threadType;
		}
		LocalProfResBox *ptrBox = g_tlsProfileInfo.m_dlinkLocalResultBox->get();
		ptrBox->m_callMatrix[index].m_count++;
		ptrBox->m_callMatrix[index].m_sumt += duration;
		if (ptrBox->m_callMatrix[index].m_maxt < duration) {
			ptrBox->m_callMatrix[index].m_maxt = duration;
		}
	}
	if (g_tlsProfileInfo.shutCleanSetted()) {
		g_profiler.releaseDlinkLocResBox(g_tlsProfileInfo.m_dlinkLocalResultBox);
		g_tlsProfileInfo.m_dlinkLocalResultBox = NULL;
		g_tlsProfileInfo.m_shutdownclean = false;
	}
}

Profiler::Profiler() : m_openGlobalProfile(false), m_threadProfileAutoRun(false) {
	if (!checkFuncPairs()) {
		abort();
	}
	m_mutex = new Mutex("Profiler::mutex", __FILE__, __LINE__);
	m_mapMutex = new Mutex("Profiler::mapMutex", __FILE__, __LINE__);
	m_globalResultMatrix = new PointRecord[g_funcListSize * g_funcListSize];
	for (int i = 0; i < INIT_LOCAL_RESULT_POOL_SIZE; i++) {
		m_localIdleResultPool.addFirst(new DLink<LocalProfResBox*>(new LocalProfResBox()));
	}
}

Profiler::~Profiler() {
	if (m_mutex)
		delete m_mutex;
	if (m_mapMutex) 
		delete m_mapMutex;
	if (m_globalResultMatrix) {
		delete[] m_globalResultMatrix;
	}
	DLink<LocalProfResBox*>* ptrDlink;
	while (m_localIdleResultPool.getSize()) {
		ptrDlink = m_localIdleResultPool.removeFirst();
		delete ptrDlink->get();
		delete ptrDlink;
	}
	while (m_localBusyResultPool.getSize()) {
		ptrDlink = m_localBusyResultPool.removeFirst();
		delete ptrDlink->get();
		delete ptrDlink;
	}
}

/**
 * ���ProfileId��g_funcNameList�Ƿ�һ��
 * 
 * @return ���ProfileId�е�ö�ٺ�g_funcNameList�ж�Ӧλ�õ�ö��ֵ��������ƥ�䷵��false�����򷵻�true��
 *	 ���ProfileId�е�ö�ٸ�����g_funcNameList�Ĵ�С��һ�£�ͬ������false
 */
bool Profiler::checkFuncPairs() {
	int count = 0;
	int enumSize=(int)PI_END - (int)PI_ROOT + 1;
	if (enumSize != g_funcListSize)
		return false;
	for (int i = (int)PI_ROOT; i <= (int)PI_END; i++) {
		++count;
		if (g_funcNameList[i].m_id != (ProfileId)i)
			return false;
	}
	if (count != g_funcListSize) {
		return false;
	}
	return true;
}

/**
 * ���߳�ProfileInfo�ṹ�Ž�ȫ��Profiler��һ��map�У��Ա����̼߳�Ļ��࿪���ر�profile����
 *
 * @param id �̻߳����ӵ�id
 * @param thdProfileInfo ָ���߳�˽��ProfileInfo�ṹ��ָ��
 */
void Profiler::regThdProfInfo(int id, ThdProfileInfo *thdProfileInfo) {
	pair<ThdProfileInfoMap::iterator,ThdProfileInfoMap::iterator> ret;
	ThdProfileInfoMap::iterator it;
	LOCK(m_mapMutex);
	ret = m_thdProfInfoMap.equal_range(id);
	for (it = ret.first; it != ret.second; ++it) {
		if ((*it).second == thdProfileInfo) {
			UNLOCK(m_mapMutex);
			return;
		}
	}
	m_thdProfInfoMap.insert(ThdProfileInfoMap::value_type(id, thdProfileInfo));
	UNLOCK(m_mapMutex);
}

/**
 * ���߳�ProfileInfo�ṹ��Profiler��ע��
 *
 * @param id �̻߳����ӵ�id
 * @param thdProfileInfo ָ���߳�˽��ProfileInfo�ṹ��ָ��
 */
void Profiler::unregThdProfInfo(int id, ThdProfileInfo *thdProfileInfo) {
	pair<ThdProfileInfoMap::iterator,ThdProfileInfoMap::iterator> ret;
	ThdProfileInfoMap::iterator it;
	LOCK(m_mapMutex);
	ret = m_thdProfInfoMap.equal_range(id);
	for (it = ret.first; it != ret.second; ++it) {
		if ((*it).second == thdProfileInfo) {
			m_thdProfInfoMap.erase(it);
			break;
		}
	}
	UNLOCK(m_mapMutex);
}

/**
 * ��ȫ��profiler�Ŀ����߳�˽��profile��Ϣ���л��һ����LocalProfResBox�ṹ����������DLink��
 *
 * @return �ӳ��л��һ��LocalProfResBox����
 */
DLink<LocalProfResBox*>* Profiler::getDlinkLocResBox() {
	LOCK(m_mutex);
	if (!m_localIdleResultPool.getSize()) {
		expandLocIdleResPool(EXPAND_FACTOR);
	}
	DLink<LocalProfResBox*> *ptr = m_localIdleResultPool.removeFirst();
	m_localBusyResultPool.addFirst(ptr);
	UNLOCK(m_mutex);
	return ptr;
}
/**
 * �黹LocalProfResBox��Idle����
 *
 * @param Ҫ�黹��DLink�ṹ��ָ��
 */
void Profiler::releaseDlinkLocResBox(DLink<LocalProfResBox*> *local) {
	local->get()->m_id = 0;
	local->get()->m_type = (ThreadType)0;
	memset(local->get()->m_callMatrix, 0, sizeof(LocalProfResBox) * g_funcListSize * g_funcListSize);
	LOCK(m_mutex);
	local->unLink();
	m_localIdleResultPool.addFirst(local);
	UNLOCK(m_mutex);
}

/**
 * ��չm_localResultPool
 *
 * @param Ҫ�����������Ĵ�С
 */
void Profiler::expandLocIdleResPool(int factor) {
	for (int i=0; i < factor; i++) {
		m_localIdleResultPool.addFirst(new DLink<LocalProfResBox *>(new LocalProfResBox()));
	}
}

/**
 * �����̵߳�����״̬
 *
 * @param id �߳�id
 * @param type �߳�����
 * @param value 1/0/-1 Profile�Ŀ���ѡ��
 */
bool Profiler::control(int id, ThreadType type, ProfileControl value) {
	TPMIter iter;
	LOCK(m_mapMutex);
	for (iter = m_thdProfInfoMap.begin(); iter != m_thdProfInfoMap.end(); iter++) {
		if ((*iter).first == id && (*iter).second->m_threadType == type) {
			switch (value) {
			case Shutdown_Clean:
				(*iter).second->shutdownClean();
				break;
			case Shutdown_Keep:
				(*iter).second->shutdownKeep();
				break;
			case OpenIt:
				(*iter).second->openProfile();
				break;
			default:
				assert(false);
				break;
			}
			UNLOCK(m_mapMutex);
			return true;
		}
	}
	UNLOCK(m_mapMutex);
	return false;
}

/**
 * ��ȡȫ��Profile�����
 *
 * @return ProfileInfo����
 */
Array<ProfileInfoRecord *>* Profiler::getGlobalProfileInfos() {
	Array<ProfileInfoRecord *> *ptrGlobProfInfoArray = new Array<ProfileInfoRecord*>();
	for (int i = 0; i < g_funcListSize; i++) {
		for (int j = 1; j < g_funcListSize; j++) {
			int index = i * g_funcListSize + j;
			if (m_globalResultMatrix[index].m_count) {
				ptrGlobProfInfoArray->push(new ProfileInfoRecord(g_funcNameList[i].m_Func, g_funcNameList[j].m_Func,
					m_globalResultMatrix[index].m_count, m_globalResultMatrix[index].m_sumt, m_globalResultMatrix[index].m_maxt));
			}
		}
	}
	return ptrGlobProfInfoArray;
}
/**
 * ��ȡ��̨�̻߳����ӵ�Profile�����
 *
 * @param type �߳�����
 */
Array<ThdProfileInfoRecord *>* Profiler::getThdsProfileInfos(ThreadType type) {
	Array<ThdProfileInfoRecord *> *ptrThdsProfileInfoArray = new Array<ThdProfileInfoRecord *>();
	DLink<LocalProfResBox *> *ptrDlink = NULL;
	LocalProfResBox* ptrBox = NULL;
	LOCK(m_mutex);
	ptrDlink =	m_localBusyResultPool.getHeader();
	for (ptrDlink = m_localBusyResultPool.getHeader()->getNext(); ptrDlink != m_localBusyResultPool.getHeader(); ptrDlink = ptrDlink->getNext()) {
		if (ptrDlink->get()->m_type == type) {
			ptrBox = ptrDlink->get();
			for (int i = 0; i < g_funcListSize; i++) {
				for (int j = 1; j < g_funcListSize; j++) {
					int index = i * g_funcListSize + j;
					if (ptrBox->m_callMatrix[index].m_count)
						ptrThdsProfileInfoArray->push(new ThdProfileInfoRecord(ptrBox->m_id, g_funcNameList[i].m_Func, g_funcNameList[j].m_Func,
							ptrBox->m_callMatrix[index].m_count, ptrBox->m_callMatrix[index].m_sumt, ptrBox->m_callMatrix[index].m_maxt));
				}
			}
		}
	}
	UNLOCK(m_mutex);
	return ptrThdsProfileInfoArray;
}

}
#endif
