/**
 * NTSE存储引擎中最常用的定义
 *
 * @author 汪源(wangyuan@corp.netease.com, wy@163.org)
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
	{"EventMonitorHelper", &(GlobalObjectCreator<EventMonitorHelper>), &(GlobalObjectDestructor<EventMonitorHelper>), NULL},	// 必须在System对象之前初始化
	{"System", &(GlobalObjectCreator<System>), &(GlobalObjectDestructor<System>), NULL},
};		/** 所有需要用全局工厂的类注册 */

GlobalFactory *GlobalFactory::m_instance = NULL;	/** 全局工厂唯一实例 */


/** 得到指定类名的全局对象
 * @param name	对象类名字
 * @return 返回指定的类对象指针
 */
void* GlobalFactory::getObject(const char* name) {
	size_t objectNum = sizeof(GlobalFactory::m_globalObjects) / sizeof(struct GlobalObject);
	for (size_t i = 0; i < objectNum; i++)
		if (!strcmp(GlobalFactory::m_globalObjects[i].m_objectName, name))
			return GlobalFactory::m_globalObjects[i].m_object;

	return NULL;
}

/** 构造函数 */
GlobalFactory::GlobalFactory() {
	size_t objectNum = sizeof(GlobalFactory::m_globalObjects) / sizeof(struct GlobalObject);
	for (size_t i = 0; i < objectNum; i++) {
		if (!GlobalFactory::m_globalObjects[i].m_object)
			GlobalFactory::m_globalObjects[i].m_object = (*GlobalFactory::m_globalObjects[i].m_creator)();
	}
}

/** 析构函数 */
GlobalFactory::~GlobalFactory() {
	size_t objectNum = sizeof(GlobalFactory::m_globalObjects) / sizeof(struct GlobalObject);
	for (size_t i = 0; i < objectNum; i++)
		GlobalFactory::m_globalObjects[i].m_destructor(GlobalFactory::m_globalObjects[i].m_object);
}


/**
 * 构造函数。注意要使用NTSE_THROW宏来抛出异常，不要直接使用本构造函数
 *
 * @param file 抛出异常的文件
 * @param line 抛出异常的文件行
 */
NtseException::NtseException(const char *file, uint line) {
	m_file = file;
	m_line = line;
	m_msg = NULL;
}

/**
 * 拷贝构造函数
 *
 * @param copy 异常对象
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
 * 构造异常信息。这一函数不应被直接调用，在使用NTSE_THROW宏时会被自动调用
 *
 * @param errorCode 错误号
 * @param fmt 格式化字符串
 * @param vargs 可变参数
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
 * 根据文件操作完成码构造异常信息。这一函数不应被直接调用，在使用NTSE_THROW宏时会被自动调用
 *
 * @param fileCode 文件操作完成码
 * @param fmt 格式化字符串
 * @param vargs 可变参数
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
 * 返回错误号
 *
 * @return 错误号
 */
ErrorCode NtseException::getErrorCode() {
	return m_errorCode;
}

/**
 * 返回异常信息
 *
 * @return 异常信息
 */
const char* NtseException::getMessage() {
	return m_msg;
}

/**
 * 返回抛出异常的代码位置所在源文件
 *
 * @return 抛出异常的代码位置所在源文件
 */
const char* NtseException::getFile() {
	return m_file;
}

/**
 * 返回抛出异常的代码行号
 *
 * @return 抛出异常的代码行号
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
 * 各类极限和常数定义
 ***************************************************************************/
const char	*Limits::NAME_IDX_EXT = ".nsi";	/** 索引文件的后缀 */
const u32	Limits::NAME_IDX_EXT_LEN = 4;	/** 索引文件后缀长度 */
const char	*Limits::NAME_TMP_IDX_EXT = ".tmpsni";	/** 在线建索引时的临时索引文件后缀*/
const char	*Limits::NAME_HEAP_EXT = ".nsd";	/** 堆文件后缀 */
const u32	Limits::NAME_HEAP_EXT_LEN = 4;	/** 堆文件后缀长度 */
const char	*Limits::NAME_TBLDEF_EXT = ".nstd"; /** 元数据文件后缀*/
const u32	Limits::NAME_TBLDEF_EXT_LEN = 5; /**元数据文件后缀长度*/
const char	*Limits::NAME_SOBH_EXT = ".nsso";/** 小型大对象堆文件 */
const u32	Limits::NAME_SOBH_EXT_LEN = 5;	/** 小型大对象堆文件后缀长度 */
const char  *Limits::NAME_SOBH_TBLDEF_EXT = ".nsstd"; /**小型大对象表定义文件*/
const u32   Limits::NAME_SOBH_TBLDEF_EXT_LEN = 6; /**小型大对象表定义文件的后缀长度*/
const char	*Limits::NAME_LOBI_EXT = ".nsli";/** 大型大对象索引文件（目录文件）后缀 */
const u32	Limits::NAME_LOBI_EXT_LEN = 5;	/** 大型大对象索引文件（目录文件）后缀长度 */
const char	*Limits::NAME_LOBD_EXT = ".nsld";/** 大型大对象数据文件后缀 */
const u32	Limits::NAME_LOBD_EXT_LEN = 5;	/** 大型大对象数据文件后缀长度 */
const char  *Limits::NAME_GLBL_DIC_EXT = ".ndic";/** 全局字典文件后缀 */
const char  *Limits::NAME_TEMP_GLBL_DIC_EXT = ".tmpndic";/** 临时全局字典文件后缀 */
const u32   Limits::NAME_GLBL_DIC_EXT_LEN = 5;/** 全局字典文件后缀长度 */
const char	*Limits::NAME_CTRL_FILE = "ntse_ctrl";	/** 控制文件名 */
const char	*Limits::NAME_TNT_CTRL_FILE = "tnt_ctrl"; /** TNT控制文件名 */
const char  *Limits::NAME_CTRL_SWAP_FILE_EXT = ".tmp";	/** 用作交换的控制文件的后缀 */
const char	*Limits::NAME_TXNLOG = "ntse_log";	/** 事务日志文件 */
//const char	*Limits::NAME_TNT_TXNLOG = "tnt_log"; /** TNT事务日志文件 */
const char	*Limits::NAME_SYSLOG = "ntse.log";	/** 系统日志文件 */
const char	*Limits::NAME_TNT_SYSLOG = "tnt.log"; /** TNT引擎redo日志文件 */
const char	*Limits::TEMP_FILE_PREFIX = "ntse_temp_file";	/** 临时文件前缀 */
const char	*Limits::NAME_TEMP_TABLE = "_oltmp_";	/** 临时表文件名特征 */
const char  *Limits::NAME_DUMP_EXT = ".dump";       /** DUMP文件名后缀 */
const char  *Limits::NAME_CONFIG_BACKUP = "ntsecfg"; /** ntse的备份文件名称 */
const char  *Limits::NAME_TNTCONFIG_BACKUP = "tntcfg"; /** tnt的备份文件名称 */

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

