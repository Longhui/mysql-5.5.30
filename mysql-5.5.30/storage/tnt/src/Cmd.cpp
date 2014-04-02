/**
 * 命令执行框架及命令实现
 *
 */
#include <vector>
#ifdef WIN32
#include <my_global.h>
#include <sql_priv.h>
#include <sql_class.h>
#include "ha_tnt.h"
#include <mysql/plugin.h>
#endif
#include "api/Database.h"
#ifndef WIN32
#include <my_global.h>
#include <sql_priv.h>
#include <sql_class.h>
#include "ha_tnt.h"
#include <mysql/plugin.h>
#endif
#include "util/SmartPtr.h"
#include "misc/Parser.h"
#include "api/TblArgAlter.h"
#include "api/LobDefraggler.h"
#include "api/IdxPreAlter.h"
#ifdef TNT_ENGINE
#include "api/TNTTblArgAlter.h"
#endif
#include "compress/CreateDicHelper.h"
#include "THDInfoBase.h"

#ifdef NTSE_PROFILE
#include "misc/Profile.h"
#endif

using namespace ntse;

/** 构造函数*/
CmdInfo::CmdInfo() {
	m_cmd = System::strdup("");
	m_status = CS_INIT;
	m_info = System::strdup("");
	m_data = NULL;
}

/** 析构函数 */
CmdInfo::~CmdInfo() {
	delete []m_cmd;
	delete []m_info;
}

/** 获取命令执行状态
 * @return 命令执行状态
 */
CmdStatus CmdInfo::getStatus() const {
	return m_status;
}

/** 设置命令执行状态
 * @param status 新的命令执行状态
 */
void CmdInfo::setStatus(CmdStatus status) {
	m_status = status;
}

/** 获取命令字符串。不可能返回NULL
 * @return 命令字符串
 */
const char* CmdInfo::getCommand() const {
	return m_cmd;
}

/** 设置命令
 * @param cmd 命令，不能为NULL
 */
void CmdInfo::setCommand(const char *cmd) {
	assert(cmd);
	delete []m_cmd;
	m_cmd = System::strdup(cmd);
}

/** 获取命令信息
 * @return 命令信息，不可能为NULL
 */
const char* CmdInfo::getInfo() const {
	return m_info;
}

/** 设置信息
 * @param info 信息，不允许为NULL
 */
void CmdInfo::setInfo(const char *info) {
	delete []m_info;
	m_info = System::strdup(info);
}

/** 将命令执行状态显示为字符串
 * @param status 命令执行状态
 * @return 字符串表示，为字符串常量
 */
const char* CmdInfo::getStatusStr(CmdStatus status) {
	switch (status) {
	case CS_INIT:
		return "Init";
	case CS_SUCCEEED:
		return "Succeed";
	case CS_FAILED:
		return "Failed";
	default:
		assert(false);
		return "Unknown";
	}
}

/** 执行命令
 * @param thdInfo 连接信息
 * @param cmdInfo 要执行的命令
 * @return 是否执行成功
 * @throw NtseException 执行过程中发生异常，发生异常也认为是执行命令失败
 */
bool CmdExecutor::executeCommand(THDInfo *thdInfo, CmdInfo *cmdInfo) throw(NtseException) {
	
	Database *ntse_db = tnt_db->getNtseDb();

	AutoPtr<Parser> parser(new Parser(cmdInfo->getCommand()));
	const char *token = parser->nextToken();
	if (!System::stricmp(token, "start")) {
		parser->match("backup");
		parser->match("to");
		const char *dir = parser->nextString();
		parser->checkNoRemain();
		assert(!thdInfo->m_pendingOper);
#ifdef TNT_ENGINE
		TNTBackupProcess *bp = tnt_db->initBackup(dir);
		try {
			tnt_db->doBackup(bp);
		} catch (NtseException &e) {
			tnt_db->doneBackup(bp);
			throw e;
		}
#else
		BackupProcess *bp = ntse_db->initBackup(dir);
		try {
			ntse_db->doBackup(bp);
		} catch (NtseException &e) {
			ntse_db->doneBackup(bp);
			throw e;
		}
#endif
		thdInfo->setPendingOper(bp, POT_BACKUP);
	} else if (!System::stricmp(token, "finishing")) {
		parser->match("backup");
		parser->match("and");
		parser->match("lock");
		parser->checkNoRemain();
		assert(thdInfo->m_pendingOper && thdInfo->m_pendingOpType == POT_BACKUP);
#ifdef TNT_ENGINE
		TNTBackupProcess *bp = (TNTBackupProcess *)thdInfo->m_pendingOper;
		tnt_db->finishingBackupAndLock(bp);
#else
		BackupProcess *bp = (BackupProcess *)thdInfo->m_pendingOper;
		ntse_db->finishingBackupAndLock(bp);
#endif
	} else if (!System::stricmp(token, "end")) {
		parser->match("backup");
		parser->checkNoRemain();
		assert(thdInfo->m_pendingOper && thdInfo->m_pendingOpType == POT_BACKUP);
#ifdef TNT_ENGINE
		TNTBackupProcess *bp = (TNTBackupProcess *)thdInfo->m_pendingOper;
		tnt_db->doneBackup(bp);
#else
		BackupProcess *bp = (BackupProcess *)thdInfo->m_pendingOper;
		ntse_db->doneBackup(bp);
#endif
		thdInfo->resetPendingOper();
	}  else if (!System::stricmp(token, "checkpoint")) {
#ifdef TNT_ENGINE
		tnt_db->getNtseDb()->doCheckpoint();
#else
		ntse_db->doCheckpoint();
#endif
	} else if (!System::stricmp(token, "alter")) {
		//动态修改表参数
#ifdef TNT_ENGINE
		TNTTableArgAlterHelper tblAltHelp(tnt_db, thdInfo->m_conn, parser.detatch(), ntse_db->getConfig()->m_tlTimeout, thdInfo->m_inLockTables);
#else
		TableArgAlterHelper tblAltHelp(ntse_db, thdInfo->m_conn, parser.detatch(), ntse_db->getConfig()->m_tlTimeout);
#endif
		tblAltHelp.alterTableArgument();
	}/* else if (!System::stricmp(token, "defrag")) {
		parser->match("lob");
		const char *tableName = parser->nextString();
		parser->checkNoRemain();
		//在线大对象整理
		LobDefraggler lobDefraggler(ntse_db, thdInfo->m_conn, tableName);
		lobDefraggler.startDefrag();
	}*/ else if (!System::stricmp(token, "add")) {
		//在线创建索引
		IdxPreAlter idxPreAlter(tnt_db, thdInfo->m_conn, parser.detatch());
		idxPreAlter.createOnlineIndex();
	} else if (!System::stricmp(token, "drop")) {
		//在线删除索引
		IdxPreAlter idxPreAlter(tnt_db, thdInfo->m_conn, parser.detatch());
		idxPreAlter.deleteOnlineIndex();
	} else if (!System::stricmp(token, "set")) {
		token = parser->nextToken();
		if (!System::stricmp(token, "next_create_args")) {
			parser->match("=");
			const char *create_args = Parser::trimWhitespace(parser->remainingString());
			thdInfo->setNextCreateArgs(create_args);
			delete []create_args;
		}
#ifdef NTSE_PROFILE
		/*else if (!System::stricmp(token, "profile")) {
			int value = parser->nextInt();
			parser->match("for");
			token = parser->nextToken();
			if (!System::stricmp(token, "bgthread")) {
				int id = parser->nextInt();
				if (!g_profiler.control(id, BG_THREAD, (ProfileControl)value)) {
					NTSE_THROW(NTSE_EC_SYNTAX_ERROR, "This BG Thread does not exist or registered for profiling!");
				}
			} else if (!System::stricmp(token, "connection")) {
				int id = parser->nextInt();
				if (!g_profiler.control(id, CONN_THREAD, (ProfileControl)value)) {
					NTSE_THROW(NTSE_EC_SYNTAX_ERROR, "This Connection does not exist or registered for profiling!");
				}
			} else {
				NTSE_THROW(NTSE_EC_SYNTAX_ERROR, "Profile command just accept bgthread & connection!");
			}
		}*/
#endif
		else
			NTSE_THROW(NTSE_EC_SYNTAX_ERROR, "Unknown command: set %s", token);
	} else if (!System::stricmp(token, "reorganize")) {
		parser->match("table");
		const char *tableName = System::strdup(parser->nextString());
		const char *nextToken = parser->nextToken();
		if (!System::stricmp(nextToken, "create")) {
			parser->match("dictionary");
			parser->checkNoRemain();

			CreateDicHelper createDicHelper(ntse_db, thdInfo->m_conn, tableName);
			createDicHelper.createDictionary();
		} else {
			delete []tableName;
			NTSE_THROW(NTSE_EC_SYNTAX_ERROR, "Unknown command: %s", nextToken);
		}
		delete []tableName;
		tableName = NULL;
	}  else if (!System::stricmp(token, "kill")) {
		//取消后台正在运行的用户线程
		parser->match("connection");
		const char *nextToken = parser->nextToken();
		uint connId = Parser::parseInt(nextToken); 
#ifdef TNT_ENGINE
		tnt_db->cancelBgCustomThd(connId);
#else
		ntse_db->cancelBgCustomThd(connId);
#endif
	} else
		NTSE_THROW(NTSE_EC_SYNTAX_ERROR, "Unknown command: %s", token);
	return true;
}


/** 构造函数 */
CmdExecutor::CmdExecutor() {
}

/** 析构函数，自动清理资源 */
CmdExecutor::~CmdExecutor() {
}

/** 执行命令
 * @param thdInfo 连接信息
 * @param cmdInfo 要执行的命令
 */
void CmdExecutor::doCommand(THDInfo *thdInfo, CmdInfo *cmdInfo) {
	cmdInfo->setInfo("");
	try {
		bool succ = executeCommand(thdInfo, cmdInfo);
		cmdInfo->setStatus(succ ? CS_SUCCEEED: CS_FAILED);
	} catch (NtseException &e) {
		cmdInfo->setStatus(CS_FAILED);
		cmdInfo->setInfo(e.getMessage());
	}
}

