#ifndef _NTSE_THDINFOBASE_H_
#define _NTSE_THDINFOBASE_H_

#include "misc/Session.h"

using namespace ntse;

/** ��ɵ�һ��Ĳ������� */
enum PendingOperType {
	POT_NONE,		/** û�н��е�һ��Ĳ��� */
	POT_BACKUP,		/** ���ݲ��� */
};

struct CmdInfo;
/** NTSE�洢������ÿ��THD��Ӧ�����ݽṹ������
 * ���ӣ�������Ϣ��
 */
struct THDInfo {
	Connection		*m_conn;			/** NTSE���� */
	CmdInfo			*m_cmdInfo;			/** ����ִ�н��*/
	void			*m_pendingOper;		/** ���е�һ��Ĳ������籸�ݵ� */
	PendingOperType	m_pendingOpType;	/** ���е�һ��Ĳ������� */
	const char		*m_nextCreateArgs;	/** �Ǳ�׼������Ϣ */
	bool			m_inLockTables;		/** mysql thd�Ƿ���lock tables�У�����˽��cmdʱ�ж��ܷ�ִ�� */

	THDInfo();
	virtual ~THDInfo();
	void setPendingOper(void *pendingOper, PendingOperType type);
	void resetPendingOper();
	void setNextCreateArgs(const char *createArgs);
	void resetNextCreateArgs();
};

/** ����ִ��״̬ */
enum CmdStatus {
	CS_SUCCEEED,	/** ����ִ�гɹ� */
	CS_FAILED,		/** ����ִ��ʧ�� */
	CS_INIT,		/** û��ִ�й����� */
};

/** ����ִ��״̬���������Ϣ */
struct CmdInfo {
public:
	CmdInfo();
	~CmdInfo();
	CmdStatus getStatus() const;
	void setStatus(CmdStatus status);
	const char* getCommand() const;
	void setCommand(const char *cmd);
	const char* getInfo() const;
	void setInfo(const char *info);
	static const char* getStatusStr(CmdStatus status);

private:
	unsigned long	m_thdId;		/** ������������Ӻ� */
	const char		*m_cmd;			/** ���� */
	CmdStatus		m_status;		/** ִ��״̬ */
	const char		*m_info;		/** �ɹ�ִ��ʱ�Ľ����ʧ��ʱ�Ĵ�����Ϣ��ִ�й����е�״̬��Ϣ */
	void			*m_data;		/** ��չ���� */
};

/** ����ִ�п�� */
class CmdExecutor {
public:
	CmdExecutor();
	~CmdExecutor();
	void doCommand(THDInfo *thdInfo, CmdInfo *cmdInfo);

private:
	bool executeCommand(THDInfo *thdInfo, CmdInfo *cmdInfo) throw(NtseException);
};

#endif