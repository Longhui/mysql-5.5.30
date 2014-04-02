#include "api/Table.h"
#include "api/Database.h"
#include "THDInfoBase.h"

THDInfo::THDInfo() {
	m_conn = NULL;
	m_cmdInfo = new CmdInfo();
	m_pendingOper = NULL;
	m_pendingOpType = POT_NONE;
	m_nextCreateArgs = NULL;
	m_inLockTables = false; 
}

THDInfo::~THDInfo() {
	assert(!m_pendingOper);
	delete m_cmdInfo;
}

/** 设置进行到一半的操作信息
 * @param pendingOper 进行到一半的操作
 * @param type 操作类型
 */
void THDInfo::setPendingOper(void *pendingOper, PendingOperType type) {
	assert(!m_pendingOper && pendingOper && type != POT_NONE);
	m_pendingOper = pendingOper;
	m_pendingOpType = type;
}

/** 重置进行到一半的操作信息 */
void THDInfo::resetPendingOper() {
	assert(m_pendingOper);
	m_pendingOper = NULL;
	m_pendingOpType = POT_NONE;
}

/** 设置下一个建表语句所使用的非标准表定义信息
 * @param createArgs 非标准表定义信息
 */
void THDInfo::setNextCreateArgs(const char *createArgs) {
	delete []m_nextCreateArgs;
	m_nextCreateArgs = System::strdup(createArgs);
}

/** 重置非标准表定义信息 */
void THDInfo::resetNextCreateArgs() {
	if (m_nextCreateArgs) {
		delete []m_nextCreateArgs;
		m_nextCreateArgs = NULL;
	}
}