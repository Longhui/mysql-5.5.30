#ifndef _NTSE_THDINFOBASE_H_
#define _NTSE_THDINFOBASE_H_

#include "misc/Session.h"

using namespace ntse;

/** 完成到一半的操作类型 */
enum PendingOperType {
	POT_NONE,		/** 没有进行到一半的操作 */
	POT_BACKUP,		/** 备份操作 */
};

struct CmdInfo;
/** NTSE存储引擎中每个THD对应的数据结构，包括
 * 连接，表锁信息等
 */
struct THDInfo {
	Connection		*m_conn;			/** NTSE连接 */
	CmdInfo			*m_cmdInfo;			/** 命令执行结果*/
	void			*m_pendingOper;		/** 进行到一半的操作，如备份等 */
	PendingOperType	m_pendingOpType;	/** 进行到一半的操作类型 */
	const char		*m_nextCreateArgs;	/** 非标准建表信息 */
	bool			m_inLockTables;		/** mysql thd是否处于lock tables中，用于私有cmd时判断能否执行 */

	THDInfo();
	virtual ~THDInfo();
	void setPendingOper(void *pendingOper, PendingOperType type);
	void resetPendingOper();
	void setNextCreateArgs(const char *createArgs);
	void resetNextCreateArgs();
};

/** 命令执行状态 */
enum CmdStatus {
	CS_SUCCEEED,	/** 命令执行成功 */
	CS_FAILED,		/** 命令执行失败 */
	CS_INIT,		/** 没有执行过命令 */
};

/** 命令执行状态，结果等信息 */
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
	unsigned long	m_thdId;		/** 发起命令的连接号 */
	const char		*m_cmd;			/** 命令 */
	CmdStatus		m_status;		/** 执行状态 */
	const char		*m_info;		/** 成功执行时的结果，失败时的错误消息或执行过程中的状态信息 */
	void			*m_data;		/** 扩展数据 */
};

/** 命令执行框架 */
class CmdExecutor {
public:
	CmdExecutor();
	~CmdExecutor();
	void doCommand(THDInfo *thdInfo, CmdInfo *cmdInfo);

private:
	bool executeCommand(THDInfo *thdInfo, CmdInfo *cmdInfo) throw(NtseException);
};

#endif