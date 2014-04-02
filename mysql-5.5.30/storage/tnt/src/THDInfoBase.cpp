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

/** ���ý��е�һ��Ĳ�����Ϣ
 * @param pendingOper ���е�һ��Ĳ���
 * @param type ��������
 */
void THDInfo::setPendingOper(void *pendingOper, PendingOperType type) {
	assert(!m_pendingOper && pendingOper && type != POT_NONE);
	m_pendingOper = pendingOper;
	m_pendingOpType = type;
}

/** ���ý��е�һ��Ĳ�����Ϣ */
void THDInfo::resetPendingOper() {
	assert(m_pendingOper);
	m_pendingOper = NULL;
	m_pendingOpType = POT_NONE;
}

/** ������һ�����������ʹ�õķǱ�׼������Ϣ
 * @param createArgs �Ǳ�׼������Ϣ
 */
void THDInfo::setNextCreateArgs(const char *createArgs) {
	delete []m_nextCreateArgs;
	m_nextCreateArgs = System::strdup(createArgs);
}

/** ���÷Ǳ�׼������Ϣ */
void THDInfo::resetNextCreateArgs() {
	if (m_nextCreateArgs) {
		delete []m_nextCreateArgs;
		m_nextCreateArgs = NULL;
	}
}