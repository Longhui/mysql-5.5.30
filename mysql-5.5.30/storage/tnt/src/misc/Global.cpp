/**
 * NTSE�洢��������õĶ���
 *
 * @author ��Դ(wangyuan@corp.netease.com, wy@163.org)
 */

#include "misc/Global.h"
#include "misc/GlobalFactory.h"
#include "util/System.h"
#include "util/File.h"
#include "util/SmartPtr.h"
#include <assert.h>
#include <string.h>
#include <iostream>

using namespace std;

namespace ntse {


GlobalObject GlobalFactory::m_globalObjects[] = {	
	{"EventMonitorHelper", &(GlobalObjectCreator<EventMonitorHelper>), &(GlobalObjectDestructor<EventMonitorHelper>), NULL},	// ������System����֮ǰ��ʼ��
	{"System", &(GlobalObjectCreator<System>), &(GlobalObjectDestructor<System>), NULL},
};		/** ������Ҫ��ȫ�ֹ�������ע�� */

GlobalFactory *GlobalFactory::m_instance = NULL;	/** ȫ�ֹ���Ψһʵ�� */


/** �õ�ָ��������ȫ�ֶ���
 * @param name	����������
 * @return ����ָ���������ָ��
 */
void* GlobalFactory::getObject(const char* name) {
	size_t objectNum = sizeof(GlobalFactory::m_globalObjects) / sizeof(struct GlobalObject);
	for (size_t i = 0; i < objectNum; i++)
		if (!strcmp(GlobalFactory::m_globalObjects[i].m_objectName, name))
			return GlobalFactory::m_globalObjects[i].m_object;

	return NULL;
}

/** ���캯�� */
GlobalFactory::GlobalFactory() {
	size_t objectNum = sizeof(GlobalFactory::m_globalObjects) / sizeof(struct GlobalObject);
	for (size_t i = 0; i < objectNum; i++) {
		if (!GlobalFactory::m_globalObjects[i].m_object)
			GlobalFactory::m_globalObjects[i].m_object = (*GlobalFactory::m_globalObjects[i].m_creator)();
	}
}

/** �������� */
GlobalFactory::~GlobalFactory() {
	size_t objectNum = sizeof(GlobalFactory::m_globalObjects) / sizeof(struct GlobalObject);
	for (size_t i = 0; i < objectNum; i++)
		GlobalFactory::m_globalObjects[i].m_destructor(GlobalFactory::m_globalObjects[i].m_object);
}


/**
 * ���캯����ע��Ҫʹ��NTSE_THROW�����׳��쳣����Ҫֱ��ʹ�ñ����캯��
 *
 * @param file �׳��쳣���ļ�
 * @param line �׳��쳣���ļ���
 */
NtseException::NtseException(const char *file, uint line) {
	m_file = file;
	m_line = line;
	m_msg = NULL;
}

/**
 * �������캯��
 *
 * @param copy �쳣����
 */
NtseException::NtseException(const NtseException &copy) {
	m_errorCode = copy.m_errorCode;
	m_file = copy.m_file;
	m_line = copy.m_line;
	if (copy.m_msg) {
		m_msg = new char[strlen(copy.m_msg) + 1];
		strcpy(m_msg, copy.m_msg);
	} else
		m_msg = NULL;
}

NtseException::~NtseException() {
	delete[] m_msg;
}

/**
 * �����쳣��Ϣ����һ������Ӧ��ֱ�ӵ��ã���ʹ��NTSE_THROW��ʱ�ᱻ�Զ�����
 *
 * @param errorCode �����
 * @param fmt ��ʽ���ַ���
 * @param vargs �ɱ����
 */
NtseException& NtseException::operator() (ErrorCode errorCode, const char *fmt, ...) {
	m_errorCode = errorCode;
	int size = 256;
	while (true) {
		m_msg = new char[size];
		va_list	args;
		va_start(args, fmt);
		int needSize = System::vsnprintf(m_msg, size, fmt, args);
		va_end(args);
	
		if (needSize >= 0 && needSize < size)
			break;
		
		delete[] m_msg;
		size *= 2;
	}
	return *this;
}

/**
 * �����ļ���������빹���쳣��Ϣ����һ������Ӧ��ֱ�ӵ��ã���ʹ��NTSE_THROW��ʱ�ᱻ�Զ�����
 *
 * @param fileCode �ļ����������
 * @param fmt ��ʽ���ַ���
 * @param vargs �ɱ����
 */
NtseException& NtseException::operator() (u64 fileCode, const char *fmt, ...) {
	m_errorCode = getFileExcptCode(fileCode);
	int size = 256;
	while (true) {
		m_msg = new char[size];
		va_list	args;
		va_start(args, fmt);
		int needSize = System::vsnprintf(m_msg, size, fmt, args);
		va_end(args);
	
		if (needSize >= 0 && needSize < size)
			break;
		
		delete[] m_msg;
		size *= 2;
	}
	char *msg = m_msg;
	while (true) {
		m_msg = new char[size];
		if (System::snprintf_mine(m_msg, size, "%s[error reason: %s, os code: %d]", msg, 
			File::explainErrno(fileCode), File::getOsError(fileCode)) > 0)
			break;
		delete []m_msg;
		size *= 2;
	}
	delete []msg;
	return *this;
}

/**
 * ���ش����
 *
 * @return �����
 */
ErrorCode NtseException::getErrorCode() {
	return m_errorCode;
}

/**
 * �����쳣��Ϣ
 *
 * @return �쳣��Ϣ
 */
const char* NtseException::getMessage() {
	return m_msg;
}

/**
 * �����׳��쳣�Ĵ���λ������Դ�ļ�
 *
 * @return �׳��쳣�Ĵ���λ������Դ�ļ�
 */
const char* NtseException::getFile() {
	return m_file;
}

/**
 * �����׳��쳣�Ĵ����к�
 *
 * @return �׳��쳣�Ĵ����к�
 */
uint NtseException::getLine() {
	return m_line;
}

ErrorCode NtseException::getFileExcptCode(u64 code) {
	u32 ntseError = File::getNtseError(code);
	if (ntseError == File::E_NOT_EXIST)
		return NTSE_EC_FILE_NOT_EXIST;
	else if (ntseError == File::E_DISK_FULL)
		return NTSE_EC_DISK_FULL;
	else if (ntseError == File::E_EOF)
		return NTSE_EC_FILE_EOF;
	else if (ntseError == File::E_EXIST)
		return NTSE_EC_FILE_EXIST;
	else if (ntseError == File::E_IN_USE)
		return NTSE_EC_FILE_IN_USE;
	else if (ntseError == File::E_NOT_EXIST)
		return NTSE_EC_FILE_NOT_EXIST;
	else if (ntseError == File::E_PERM_ERR)
		return NTSE_EC_FILE_PERM_ERROR;
	else if (ntseError == File::E_READ)
		return NTSE_EC_READ_FAIL;
	else if (ntseError == File::E_WRITE)
		return NTSE_EC_WRITE_FAIL;
	else
		return NTSE_EC_FILE_FAIL;
}

/****************************************************************************
 * ���༫�޺ͳ�������
 ***************************************************************************/
const char	*Limits::NAME_IDX_EXT = ".nsi";	/** �����ļ��ĺ�׺ */
const u32	Limits::NAME_IDX_EXT_LEN = 4;	/** �����ļ���׺���� */
const char	*Limits::NAME_TMP_IDX_EXT = ".tmpsni";	/** ���߽�����ʱ����ʱ�����ļ���׺*/
const char	*Limits::NAME_HEAP_EXT = ".nsd";	/** ���ļ���׺ */
const u32	Limits::NAME_HEAP_EXT_LEN = 4;	/** ���ļ���׺���� */
const char	*Limits::NAME_TBLDEF_EXT = ".nstd"; /** Ԫ�����ļ���׺*/
const u32	Limits::NAME_TBLDEF_EXT_LEN = 5; /**Ԫ�����ļ���׺����*/
const char	*Limits::NAME_SOBH_EXT = ".nsso";/** С�ʹ������ļ� */
const u32	Limits::NAME_SOBH_EXT_LEN = 5;	/** С�ʹ������ļ���׺���� */
const char  *Limits::NAME_SOBH_TBLDEF_EXT = ".nsstd"; /**С�ʹ��������ļ�*/
const u32   Limits::NAME_SOBH_TBLDEF_EXT_LEN = 6; /**С�ʹ��������ļ��ĺ�׺����*/
const char	*Limits::NAME_LOBI_EXT = ".nsli";/** ���ʹ���������ļ���Ŀ¼�ļ�����׺ */
const u32	Limits::NAME_LOBI_EXT_LEN = 5;	/** ���ʹ���������ļ���Ŀ¼�ļ�����׺���� */
const char	*Limits::NAME_LOBD_EXT = ".nsld";/** ���ʹ���������ļ���׺ */
const u32	Limits::NAME_LOBD_EXT_LEN = 5;	/** ���ʹ���������ļ���׺���� */
const char  *Limits::NAME_GLBL_DIC_EXT = ".ndic";/** ȫ���ֵ��ļ���׺ */
const char  *Limits::NAME_TEMP_GLBL_DIC_EXT = ".tmpndic";/** ��ʱȫ���ֵ��ļ���׺ */
const u32   Limits::NAME_GLBL_DIC_EXT_LEN = 5;/** ȫ���ֵ��ļ���׺���� */
const char	*Limits::NAME_CTRL_FILE = "ntse_ctrl";	/** �����ļ��� */
const char	*Limits::NAME_TNT_CTRL_FILE = "tnt_ctrl"; /** TNT�����ļ��� */
const char  *Limits::NAME_CTRL_SWAP_FILE_EXT = ".tmp";	/** ���������Ŀ����ļ��ĺ�׺ */
const char	*Limits::NAME_TXNLOG = "ntse_log";	/** ������־�ļ� */
//const char	*Limits::NAME_TNT_TXNLOG = "tnt_log"; /** TNT������־�ļ� */
const char	*Limits::NAME_SYSLOG = "ntse.log";	/** ϵͳ��־�ļ� */
const char	*Limits::NAME_TNT_SYSLOG = "tnt.log"; /** TNT����redo��־�ļ� */
const char	*Limits::TEMP_FILE_PREFIX = "ntse_temp_file";	/** ��ʱ�ļ�ǰ׺ */
const char	*Limits::NAME_TEMP_TABLE = "_oltmp_";	/** ��ʱ���ļ������� */
const char  *Limits::NAME_DUMP_EXT = ".dump";       /** DUMP�ļ�����׺ */
const char  *Limits::NAME_CONFIG_BACKUP = "ntsecfg"; /** ntse�ı����ļ����� */
const char  *Limits::NAME_TNTCONFIG_BACKUP = "tntcfg"; /** tnt�ı����ļ����� */

const char *Limits::EXTS[] = {
	NAME_IDX_EXT, 
	NAME_HEAP_EXT, 
	NAME_TBLDEF_EXT,
	NAME_SOBH_EXT, 
	NAME_SOBH_TBLDEF_EXT, 
	NAME_LOBI_EXT, 
	NAME_LOBD_EXT,
	NAME_GLBL_DIC_EXT,
};

const int Limits::EXTNUM = sizeof(Limits::EXTS) / sizeof(const char *);
const int Limits::EXTNUM_NOLOB = 3;

}

