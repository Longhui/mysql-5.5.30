/**
 * MMS���ߺ���������Ҫ�����ṩ���Թ��ߺ���
 *
 * @author �۷�(shaofeng@corp.netease.com, sf@163.org)
 */

#include "mms/Mms.h"
#include "mms/MmsHeap.h"
#include "mms/MmsPage.h"
#include "mms/MmsMap.h"
#include "api/Database.h"

namespace ntse {

// ��Ԫ����ʹ��
#ifdef NTSE_UNIT_TEST

/** 
 * ��MMS����
 */
void MmsTable::lockMmsTable(u16 userId) {
	m_mmsTblLock.lock(userId, Exclusived, __FILE__, __LINE__);
}

/** 
 * ��MMS����
 */
void MmsTable::unlockMmsTable(u16 userId) {
	m_mmsTblLock.unlock(userId, Exclusived);
}

/**
 * ˢд������־
 *
 * @param session �Ự
 */
void MmsTable::flushMmsLog(Session *session) {
	flushLog(session);
}

/** 
 * �ر��Զ����»���ˢ��
 */
void MmsTable::disableAutoFlushLog() {
	m_autoFlushLog = false;
}

/** 
 * ���õ�ǰҳ������RPClass
 *
 * @param rpClass ������RPClass
 */
void MmsTable::setRpClass(MmsRPClass *rpClass) {
	m_testCurrPage->m_rpClass = rpClass;
}

/** 
* ���õ�ǰҳ����RPClass��MMS��ֵ
*
* @param mmsTable MMS��
*/
void MmsTable::setMmsTableInRpClass(MmsTable *mmsTable) {
	m_testCurrRPClass->m_mmsTable = mmsTable;
}

/** 
 * ��ȡ��ǰ��¼ҳ
 *
 * @param recPage ��¼ҳ
 */
void MmsTable::mmsTableTestGetPage(MmsRecPage *recPage) {
	this->m_testCurrPage = recPage;
}

/** 
 * �ͷ���ǰҳ
 * @param session �Ự
 */
void MmsTable::evictCurrPage(Session *session) {
	if (m_testCurrPage) {
		MMS_RWLOCK(session->getId(), &m_mmsTblLock, Exclusived);
		MMS_LOCK_REC_PAGE(session, m_mms, m_testCurrPage);
		evictMmsPage(session, m_testCurrPage);
		MmsPageOper::mmsRWUNLock(session->getId(), &m_mmsTblLock, Exclusived);
		m_mms->m_numPagesInUse.decrement();

		// ����ҳ����
		m_mms->m_pagePool->setInfoAndType(m_testCurrPage, NULL, PAGE_EMPTY);
	}
}

/** 
 * �Ե�ǰҳ��pin (���ڲ��ԣ�����ҳ����
 */
void MmsTable::pinCurrPage() {
	if (m_testCurrPage)
		m_testCurrPage->m_numPins.increment();
}

/** 
 * �Ե�ǰҳ��unpin �����ڲ��ԣ�����ҳ��)
 */
void MmsTable::unpinCurrPage() {
	if (m_testCurrPage)
		m_testCurrPage->m_numPins.decrement();
}

/** 
 * ����ǰ��¼ҳ
 *
 * @param session �Ự
 */
void MmsTable::lockCurrPage(Session *session) {
	if (m_testCurrPage)
		MMS_LOCK_REC_PAGE(session, m_mms, m_testCurrPage);
}

/** 
 * ������ǰ��¼ҳ
 *
 * @param session �Ự
 */
void MmsTable::unlockCurrPage(Session *session) {
	if (m_testCurrPage)
		MmsPageOper::unlockRecPage(session, m_mms, m_testCurrPage);
}

/** 
 * �ѵ�ǰҳʧЧ �����ڲ��ԣ�����ҳ��)
 */
void MmsTable::disableCurrPage() {
	if (m_testCurrPage)
		m_testCurrPage->m_numPins.set(-1);
}

/** 
 * ɾ����ǰ��¼
 */
void MmsTable::delCurrRecord() {
	if (m_testCurrRecord) {
		m_testCurrPage->m_numPins.increment();
		Connection *conn = m_mms->m_db->getConnection(true);
		Session *session = m_mms->m_db->getSessionManager()->allocSession("MmsTable::delCurrRecord", conn); // ����ɹ�
		this->del(session, m_testCurrRecord);
		m_mms->m_db->getSessionManager()->freeSession(session);
		m_mms->m_db->freeConnection(conn);
	}
}

/** 
 * ǿ�����м��ˢ���߳�
 */
void MmsTable::runFlushTimerForce() {
	m_flushTimer->pause(true);
	m_flushTimer->run();
	m_flushTimer->resume();
}

/** 
 * ǿ�������滻�߳�
 */
void Mms::runReplacerForce() {
	m_replacer->pause(true);
	m_replacer->run();
	m_replacer->resume();
}

/** 
 * ���ò�������
 *
 * @param mmsTable �����漰��MMS��
 * @param rpClass �����漰��RPClass
 * @param taskCount �������
 */
void Mms::mmsTestSetTask(MmsRPClass *rpClass, int taskCount) {
	m_taskTopHeap = NULL;
	m_taskBottomHeap = rpClass->m_oldestHeap;
	m_taskCount = taskCount;
	m_taskNum = 0;
}

/** 
 * ��MMS����
 */
void Mms::lockMmsTable(u16 userId) {
	if (m_taskTable) {
		MMS_RWLOCK(userId, &m_taskTable->m_mmsTblLock, Exclusived);
	}
}

/** 
 * ��MMS����
 */
void Mms::unlockMmsTable(u16 userId) {
	if (m_taskTable) {
		MmsPageOper::mmsRWUNLock(userId, &m_taskTable->m_mmsTblLock, Exclusived);
	}
}

/** 
 * ��ȡ���������¼ҳ
 *
 * @param recPage ��¼ҳ
 */
void Mms::mmsTestGetPage(MmsRecPage *recPage) {
	m_taskPage = recPage;
}

/** 
 * ��ȡ��������MMS��
 *
 * @param mmsTable MMS��
 */
void Mms::mmsTestGetTable(MmsTable *mmsTable) {
	m_taskTable = mmsTable;
}

/** 
 * ����ǰ��¼ҳ
 * @param session �Ự
 */
void Mms::lockRecPage(Session *session) {
	if (m_taskPage)
		MMS_LOCK_REC_PAGE(session, this, m_taskPage);
}

/** 
 * ���¼��
 *
 * @param session �Ự
 */
void Mms::unlockRecPage(Session *session) {
	if (m_taskPage)
		MmsPageOper::unlockRecPage(session, this, m_taskPage);
}

/** 
 * pin��ǰ��¼ҳ(������)
 */
void Mms::pinRecPage() {
	m_taskPage->m_numPins.increment();
}

/** 
 * unpin��ǰ��¼ҳ(������)
 */
void Mms::unpinRecPage() {
	m_taskPage->m_numPins.decrement();
}

#endif

};