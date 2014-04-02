/**
 * MMS工具函数集，主要用于提供测试工具函数
 *
 * @author 邵峰(shaofeng@corp.netease.com, sf@163.org)
 */

#include "mms/Mms.h"
#include "mms/MmsHeap.h"
#include "mms/MmsPage.h"
#include "mms/MmsMap.h"
#include "api/Database.h"

namespace ntse {

// 单元测试使用
#ifdef NTSE_UNIT_TEST

/** 
 * 加MMS表锁
 */
void MmsTable::lockMmsTable(u16 userId) {
	m_mmsTblLock.lock(userId, Exclusived, __FILE__, __LINE__);
}

/** 
 * 解MMS表锁
 */
void MmsTable::unlockMmsTable(u16 userId) {
	m_mmsTblLock.unlock(userId, Exclusived);
}

/**
 * 刷写更新日志
 *
 * @param session 会话
 */
void MmsTable::flushMmsLog(Session *session) {
	flushLog(session);
}

/** 
 * 关闭自动更新缓存刷新
 */
void MmsTable::disableAutoFlushLog() {
	m_autoFlushLog = false;
}

/** 
 * 设置当前页所属的RPClass
 *
 * @param rpClass 所属的RPClass
 */
void MmsTable::setRpClass(MmsRPClass *rpClass) {
	m_testCurrPage->m_rpClass = rpClass;
}

/** 
* 设置当前页所属RPClass的MMS表值
*
* @param mmsTable MMS表
*/
void MmsTable::setMmsTableInRpClass(MmsTable *mmsTable) {
	m_testCurrRPClass->m_mmsTable = mmsTable;
}

/** 
 * 获取当前记录页
 *
 * @param recPage 记录页
 */
void MmsTable::mmsTableTestGetPage(MmsRecPage *recPage) {
	this->m_testCurrPage = recPage;
}

/** 
 * 惩罚当前页
 * @param session 会话
 */
void MmsTable::evictCurrPage(Session *session) {
	if (m_testCurrPage) {
		MMS_RWLOCK(session->getId(), &m_mmsTblLock, Exclusived);
		MMS_LOCK_REC_PAGE(session, m_mms, m_testCurrPage);
		evictMmsPage(session, m_testCurrPage);
		MmsPageOper::mmsRWUNLock(session->getId(), &m_mmsTblLock, Exclusived);
		m_mms->m_numPagesInUse.decrement();

		// 设置页类型
		m_mms->m_pagePool->setInfoAndType(m_testCurrPage, NULL, PAGE_EMPTY);
	}
}

/** 
 * 对当前页加pin (用于测试，不加页锁）
 */
void MmsTable::pinCurrPage() {
	if (m_testCurrPage)
		m_testCurrPage->m_numPins.increment();
}

/** 
 * 对当前页加unpin （用于测试，不加页锁)
 */
void MmsTable::unpinCurrPage() {
	if (m_testCurrPage)
		m_testCurrPage->m_numPins.decrement();
}

/** 
 * 锁当前记录页
 *
 * @param session 会话
 */
void MmsTable::lockCurrPage(Session *session) {
	if (m_testCurrPage)
		MMS_LOCK_REC_PAGE(session, m_mms, m_testCurrPage);
}

/** 
 * 解锁当前记录页
 *
 * @param session 会话
 */
void MmsTable::unlockCurrPage(Session *session) {
	if (m_testCurrPage)
		MmsPageOper::unlockRecPage(session, m_mms, m_testCurrPage);
}

/** 
 * 把当前页失效 （用于测试，不加页锁)
 */
void MmsTable::disableCurrPage() {
	if (m_testCurrPage)
		m_testCurrPage->m_numPins.set(-1);
}

/** 
 * 删除当前记录
 */
void MmsTable::delCurrRecord() {
	if (m_testCurrRecord) {
		m_testCurrPage->m_numPins.increment();
		Connection *conn = m_mms->m_db->getConnection(true);
		Session *session = m_mms->m_db->getSessionManager()->allocSession("MmsTable::delCurrRecord", conn); // 必须成功
		this->del(session, m_testCurrRecord);
		m_mms->m_db->getSessionManager()->freeSession(session);
		m_mms->m_db->freeConnection(conn);
	}
}

/** 
 * 强制运行间隔刷新线程
 */
void MmsTable::runFlushTimerForce() {
	m_flushTimer->pause(true);
	m_flushTimer->run();
	m_flushTimer->resume();
}

/** 
 * 强制运行替换线程
 */
void Mms::runReplacerForce() {
	m_replacer->pause(true);
	m_replacer->run();
	m_replacer->resume();
}

/** 
 * 设置测试任务
 *
 * @param mmsTable 任务涉及的MMS表
 * @param rpClass 任务涉及的RPClass
 * @param taskCount 任务个数
 */
void Mms::mmsTestSetTask(MmsRPClass *rpClass, int taskCount) {
	m_taskTopHeap = NULL;
	m_taskBottomHeap = rpClass->m_oldestHeap;
	m_taskCount = taskCount;
	m_taskNum = 0;
}

/** 
 * 加MMS表锁
 */
void Mms::lockMmsTable(u16 userId) {
	if (m_taskTable) {
		MMS_RWLOCK(userId, &m_taskTable->m_mmsTblLock, Exclusived);
	}
}

/** 
 * 解MMS表锁
 */
void Mms::unlockMmsTable(u16 userId) {
	if (m_taskTable) {
		MmsPageOper::mmsRWUNLock(userId, &m_taskTable->m_mmsTblLock, Exclusived);
	}
}

/** 
 * 获取测试所需记录页
 *
 * @param recPage 记录页
 */
void Mms::mmsTestGetPage(MmsRecPage *recPage) {
	m_taskPage = recPage;
}

/** 
 * 获取测试所需MMS表
 *
 * @param mmsTable MMS表
 */
void Mms::mmsTestGetTable(MmsTable *mmsTable) {
	m_taskTable = mmsTable;
}

/** 
 * 锁当前记录页
 * @param session 会话
 */
void Mms::lockRecPage(Session *session) {
	if (m_taskPage)
		MMS_LOCK_REC_PAGE(session, this, m_taskPage);
}

/** 
 * 解记录锁
 *
 * @param session 会话
 */
void Mms::unlockRecPage(Session *session) {
	if (m_taskPage)
		MmsPageOper::unlockRecPage(session, this, m_taskPage);
}

/** 
 * pin当前记录页(不加锁)
 */
void Mms::pinRecPage() {
	m_taskPage->m_numPins.increment();
}

/** 
 * unpin当前记录页(不加锁)
 */
void Mms::unpinRecPage() {
	m_taskPage->m_numPins.decrement();
}

#endif

};