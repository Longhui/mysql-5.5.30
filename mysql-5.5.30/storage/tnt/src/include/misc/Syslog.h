/**
 * 系统日志
 *
 * @author 汪源(wangyuan@corp.netease.com, wy@163.org)
 */

#ifndef _NTSE_SYSLOG_H_
#define _NTSE_SYSLOG_H_

#include <stdio.h>
#include "Global.h"
#include "util/Sync.h"

namespace ntse {
enum ErrLevel {
	EL_NONE,		/** 没有日志 */
	EL_DEBUG,		/** 调试信息，默认不会被输出到日志文件中 */
	EL_LOG,			/** 普通的操作日志，默认会被输出 */
	EL_WARN,		/** 警告信息 */
	EL_ERROR,		/** 不影响NTSE运行的错误 */
	EL_PANIC		/** 非常严重的错误，程序退出 */
};

/** 负责记录系统日志 */
class Syslog {
public:
	Syslog(const char *path, ErrLevel errLevel, bool autoFlush, bool printToStdout);
	~Syslog();
	ErrLevel getErrLevel();
	void setErrLevel(ErrLevel errLevel);
	bool getAutoFlush();
	void setAutoFlush(bool autoFlush);
	bool getPrintToStdout();
	void setPrintToStdout(bool printToStdout);
	void log(ErrLevel errLevel, const char *fmt, ...);
	void fopPanic(u64 code, const char *fmt, ...);
	void flush();

private:
	void writeLogMsg(const char *msg, ErrLevel errLevel);
	
private:
	FILE		*m_file;
	ErrLevel	m_errLevel;
	Mutex		m_mutex;
	bool		m_autoFlush;
	bool		m_printToStdout;
};

}

#endif
