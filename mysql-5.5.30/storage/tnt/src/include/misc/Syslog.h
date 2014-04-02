/**
 * ϵͳ��־
 *
 * @author ��Դ(wangyuan@corp.netease.com, wy@163.org)
 */

#ifndef _NTSE_SYSLOG_H_
#define _NTSE_SYSLOG_H_

#include <stdio.h>
#include "Global.h"
#include "util/Sync.h"

namespace ntse {
enum ErrLevel {
	EL_NONE,		/** û����־ */
	EL_DEBUG,		/** ������Ϣ��Ĭ�ϲ��ᱻ�������־�ļ��� */
	EL_LOG,			/** ��ͨ�Ĳ�����־��Ĭ�ϻᱻ��� */
	EL_WARN,		/** ������Ϣ */
	EL_ERROR,		/** ��Ӱ��NTSE���еĴ��� */
	EL_PANIC		/** �ǳ����صĴ��󣬳����˳� */
};

/** �����¼ϵͳ��־ */
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
