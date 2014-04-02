/**
 * ��ִ̨�е��û��߳�
 *
 * @author ��ΰ��(liweizhao@corp.netease.com)
 */
#include "misc/Syslog.h"
#include "api/Database.h"
#include "api/Table.h"
#include <string>
#include "misc/BgCustomThread.h"

namespace ntse {

/**
 * BgCustomThread���캯��
 * @param name ����
 * @param db   �������ݿ�
 * @param bgThdManager ��̨�û��̹߳�����
 * @param table �����ı�
 * @param needUnregister �Ƿ���Ҫ���̨�û��̹߳�����ע���Լ�
 * @return 
 */
BgCustomThread::BgCustomThread(const char *name, Database *db, BgCustomThreadManager *bgThdManager, 
							   Table *table, bool needUnregister, bool trxConn /* = false*/) 
	: Thread(name), m_bgThdManager(bgThdManager), m_db(db), m_needUnregister(needUnregister), 
	m_cancelFlag(false) {
		m_path = System::strdup(table->getPath());
		string tbdInfo = string(name) + "(" + m_path + ")";
		m_conn = m_db->getConnection(true, tbdInfo.c_str());
		m_conn->setTrx(trxConn);
		m_session = m_db->getSessionManager()->allocSession(tbdInfo.c_str(), m_conn);
		DLink<BgCustomThread *>::set(this);
}

BgCustomThread::~BgCustomThread() {
	m_db->getSessionManager()->freeSession(m_session);
	m_session = NULL;
	m_db->freeConnection(m_conn);
	m_conn = NULL;
	delete [] m_path;
}

/**
 * ����Thread::run
 */
void BgCustomThread::run() {
	try {
		doBgWork();
	} catch (NtseException &e) {
		m_db->getSyslog()->log(EL_ERROR, "Error occured in execution of background custom thread: %s\n", 
			e.getMessage());
	}

	if (m_needUnregister) {
		m_bgThdManager->unRegisterThd(this);
	}
}

/**
 * BgCustomThreadManager������
 * @param db �������ݿ�
 * @param interval ���м��
 */
BgCustomThreadManager::BgCustomThreadManager(Database *db, uint interval) 
	: BgTask(db, "Database::BgCustomThreadManager", interval, true), 
	m_mutex("BgCustomThreadManager Mutex", __FILE__, __LINE__) {
		m_runningThds = new DList<BgCustomThread*>();
		m_zombieThds = new DList<BgCustomThread*>();
}

BgCustomThreadManager::~BgCustomThreadManager() {
	delete m_runningThds;
	m_runningThds = NULL;
	delete m_zombieThds;
	m_zombieThds = NULL;
}

/**
 * ������̨�߳�
 * @param thd Ҫ�����ĺ�̨�߳�
 */
void BgCustomThreadManager::startBgThd(BgCustomThread *thd) {
	registerThd(thd);
	thd->start();
}

/**
 * ע��Optimize�ĺ�̨�߳�
 * @param thd
 */
void BgCustomThreadManager::registerThd(BgCustomThread *thd) {
	MutexGuard mutexGuard(&m_mutex, __FILE__, __LINE__);
	m_runningThds->addLast(dynamic_cast<DLink<BgCustomThread*>*>(thd));
}

/**
 * ע��Optimize�ĺ�̨�߳�
 * @param thd
 */
void BgCustomThreadManager::unRegisterThd(BgCustomThread *thd) {
	MutexGuard mutexGuard(&m_mutex, __FILE__, __LINE__);
	thd->unLink();
	m_zombieThds->addLast(dynamic_cast<DLink<BgCustomThread*>*>(thd));
	signal();
}

/**
 * ����BgTask::runIt
 */
void BgCustomThreadManager::runIt() {
	MutexGuard mutexGuard(&m_mutex, __FILE__, __LINE__);
	reclaimThd();
}

/**
 * ֹͣ���������еĺ�̨�߳�
 * @param forceStop �Ƿ�ǿ��ȡ������ִ�еĲ���
 */
void BgCustomThreadManager::stopAll(bool forceStop) {
	stop();

	m_mutex.lock(__FILE__, __LINE__);

	if (forceStop) {
		DLink<BgCustomThread*> *header = m_runningThds->getHeader();
		for (DLink<BgCustomThread*> *it = header->getNext(); header != it; it = it->getNext()) {
			BgCustomThread *thd = it->get();
			thd->cancel();
		}
	} else {
		while (m_runningThds->getSize() > 0) {
			m_mutex.unlock();
			Thread::msleep(100);
			m_mutex.lock(__FILE__, __LINE__);
		}
	}

	reclaimThd();
	m_mutex.unlock();
}

/**
 * �����Ѿ���ɲ�����Optimize��̨�߳�
 */
void BgCustomThreadManager::reclaimThd() {
	assert(m_mutex.isLocked());

	if (m_zombieThds->getSize() > 0) {
		DLink<BgCustomThread*> *header = m_zombieThds->getHeader();
		for (DLink<BgCustomThread*> *it = header->getNext(); header != it; it = it->getNext()) {
			BgCustomThread *thd = it->get();
			if (!thd->isAlive()) {
				if (thd->join()) {
					DLink<BgCustomThread*> *tmp = it->getNext();
					it->unLink();
					delete thd;
					it = tmp;
					continue;
				}
			}//if (!thd->isAlive()) {
		}//for
	}
}

/**
 * ȡ����̨���е��û��߳�
 * @param connId ���ݿ�����ID
 * @throw NtseException ָ�������Ӳ�����
 */
void BgCustomThreadManager::cancelThd(uint connId) throw(NtseException) {
	MutexGuard mutexGuard(&m_mutex, __FILE__, __LINE__);

	DLink<BgCustomThread*> *header = m_runningThds->getHeader();
	for (DLink<BgCustomThread*> *it = header->getNext(); header != it; it = it->getNext()) {
		BgCustomThread *thd = it->get();
		if (thd->getConnId() == connId) {
			thd->cancel();
			return;
		}
	}

	NTSE_THROW(NTSE_EC_NONECONNECTION, "No such connection or operation not permitted!");
}

}