/**
 * ϵͳ��־ʵ��
 *
 * @author ��Դ(wangyuan@corp.netease.com, wy@163.org)
 */

#include "misc/Syslog.h"
#include "util/System.h"
#include "util/File.h"
#include <time.h>

namespace ntse {

/**
 * ����һ��ϵͳ��־��¼��
 * 
 * @param path ��־�ļ�·��������ΪNULL����ΪNULL�򲻻ᴴ����־�ļ���������־
 *   ��д������׼����У���ʹ����printToStdoutΪfalse��
 * @param errLevel Ҫ�������־����ͼ���
 * @param autoFlush �Ƿ�ÿдһ����־��flush���ļ���
 * @param printToStdout ��־��Ϣ�Ƿ�Ҳ�����ϵͳ�ı�׼���
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
 * �����������Զ�flush���ر���־�ļ�
 */
Syslog::~Syslog() {
	flush();
	if (m_file) {
		fclose(m_file);
		m_file = NULL;
	}
}

/** 
 * �õ�Ҫ�������־����ͼ���
 *
 * @return Ҫ�������־����ͼ���
 */
ErrLevel Syslog::getErrLevel() {
	return m_errLevel;
}

/**
 * ����Ҫ�������־����ͼ���
 * 
 * @param errLevel Ҫ�������־����ͼ���
 */
void Syslog::setErrLevel(ErrLevel errLevel) {
	m_errLevel = errLevel;
}

/**
 * �õ��Ƿ��Զ�flushÿ����־���ļ�
 *
 * @return �Ƿ��Զ�flushÿ����־���ļ�
 */
bool Syslog::getAutoFlush() {
	return m_autoFlush;
}

/**
 * �����Ƿ��Զ�flushÿ����־���ļ�
 *
 * @param autoFlush �Ƿ��Զ�flushÿ����־���ļ�
 */
void Syslog::setAutoFlush(bool autoFlush) {
	if (!m_autoFlush && autoFlush && m_file)
		fflush(m_file);
	m_autoFlush = autoFlush;
}

/** 
 * �õ��Ƿ���־Ҳ�������׼���
 *
 * @return �Ƿ���־Ҳ�������׼���
 */
bool Syslog::getPrintToStdout() {
	return m_printToStdout;
}

/**
 * �����Ƿ���־Ҳ�������׼���
 *
 * @param printToStdout �Ƿ���־Ҳ�������׼���
 */
void Syslog::setPrintToStdout(bool printToStdout) {
	m_printToStdout = printToStdout;
}

/**
 * дһ����־�����Ҫд����־����������õ���ͼ����򲻻�д������־��
 * �������Ѿ�������ͬ�����ƣ���֤���̲߳�����¼��־ʱ����ȷ�ԡ�
 *
 * @param errLevel Ҫ��¼����־����
 * @param fmt ��־��ʽ���ַ���
 * @param ... �ɱ����
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
 * ���ļ���������ʱд������־���˳�ϵͳ
 *
 * @param code �ļ�����������
 * @param fmt ��־��ʽ���ַ���
 * @param ... �ɱ����
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
 * �ֶ�����flush��־�ļ������������autoFlush����ñ�����ʱ����Ҫ�����κι�����
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

