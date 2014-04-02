/**
 * 系统日志实现
 *
 * @author 汪源(wangyuan@corp.netease.com, wy@163.org)
 */

#include "misc/Syslog.h"
#include "util/System.h"
#include "util/File.h"
#include <time.h>

namespace ntse {

/**
 * 创建一个系统日志记录器
 * 
 * @param path 日志文件路径。可以为NULL，若为NULL则不会创建日志文件，并且日志
 *   会写出到标准输出中，即使参数printToStdout为false。
 * @param errLevel 要输出的日志的最低级别
 * @param autoFlush 是否每写一条日志都flush到文件中
 * @param printToStdout 日志信息是否也输出到系统的标准输出
 */
Syslog::Syslog(const char *path, ErrLevel errLevel, bool autoFlush, bool printToStdout):
	m_mutex("Syslog::mutex", __FILE__, __LINE__) {
	m_errLevel = errLevel;
	m_autoFlush = autoFlush;
	m_printToStdout = printToStdout;
	if (path) {
		m_file = fopen(path, "a+");
		if (m_file == NULL) {
			printf("Unable to open log file %s, syslog will not write to this file.", path);
			m_printToStdout = true;
		}
	} else {
		m_file = NULL;
		m_printToStdout = true;
	}
}

/**
 * 析构函数，自动flush并关闭日志文件
 */
Syslog::~Syslog() {
	flush();
	if (m_file) {
		fclose(m_file);
		m_file = NULL;
	}
}

/** 
 * 得到要输出的日志的最低级别
 *
 * @return 要输出的日志的最低级别
 */
ErrLevel Syslog::getErrLevel() {
	return m_errLevel;
}

/**
 * 设置要输出的日志的最低级别
 * 
 * @param errLevel 要输出的日志的最低级别
 */
void Syslog::setErrLevel(ErrLevel errLevel) {
	m_errLevel = errLevel;
}

/**
 * 得到是否自动flush每条日志到文件
 *
 * @return 是否自动flush每条日志到文件
 */
bool Syslog::getAutoFlush() {
	return m_autoFlush;
}

/**
 * 设置是否自动flush每条日志到文件
 *
 * @param autoFlush 是否自动flush每条日志到文件
 */
void Syslog::setAutoFlush(bool autoFlush) {
	if (!m_autoFlush && autoFlush && m_file)
		fflush(m_file);
	m_autoFlush = autoFlush;
}

/** 
 * 得到是否将日志也输出到标准输出
 *
 * @return 是否将日志也输出到标准输出
 */
bool Syslog::getPrintToStdout() {
	return m_printToStdout;
}

/**
 * 设置是否将日志也输出到标准输出
 *
 * @param printToStdout 是否将日志也输出到标准输出
 */
void Syslog::setPrintToStdout(bool printToStdout) {
	m_printToStdout = printToStdout;
}

/**
 * 写一条日志。如果要写的日志级别低于设置的最低级别，则不会写这条日志。
 * 本函数已经进行了同步控制，保证多线程并发记录日志时的正确性。
 *
 * @param errLevel 要记录的日志级别
 * @param fmt 日志格式化字符串
 * @param ... 可变参数
 */
void Syslog::log(ErrLevel errLevel, const char *fmt, ...) {
	if (errLevel < m_errLevel)
		return;
	if (!m_file && !m_printToStdout)
		return;

	char msg[1024];
	
	va_list	args;
	va_start(args, fmt);
	System::vsnprintf(msg, sizeof(msg), fmt, args);
	va_end(args);

	writeLogMsg(msg, errLevel);
	if (errLevel == EL_PANIC) {
		flush();
		NTSE_ASSERT(false);
	}
}

/**
 * 在文件操作出错时写错误日志并退出系统
 *
 * @param code 文件操作错误码
 * @param fmt 日志格式化字符串
 * @param ... 可变参数
 */
void Syslog::fopPanic(u64 code, const char *fmt, ...) {
	char msgPart[1024], msg[1024];
	
	va_list	args;
	va_start(args, fmt);
	System::vsnprintf(msgPart, sizeof(msgPart), fmt, args);
	va_end(args);

	System::snprintf_mine(msg, sizeof(msg), "%s[error reason: %s, os code: %d]", msgPart, File::explainErrno(code), File::getOsError(code));
	writeLogMsg(msg, EL_PANIC);
	flush();
	NTSE_ASSERT(false);
}

/**
 * 手动控制flush日志文件，如果设置了autoFlush则调用本函数时不需要进行任何工作。
 */
void Syslog::flush() {
	if (m_file && !m_autoFlush)
		fflush(m_file);
}

void Syslog::writeLogMsg(const char *msg, ErrLevel errLevel) {
	time_t now = time(NULL);
	char timestr[30];
	System::formatTime(timestr, sizeof(timestr), &now);
	if (m_file) {
		LOCK(&m_mutex);
		fprintf(m_file, "%s: %s\n", timestr, msg);
		if (m_autoFlush || errLevel >= EL_LOG)
			fflush(m_file);
		UNLOCK(&m_mutex);
	}
	if (m_printToStdout) {
		LOCK(&m_mutex);
		printf("%s: %s\n", timestr, msg);
		UNLOCK(&m_mutex);
	}
}

}

