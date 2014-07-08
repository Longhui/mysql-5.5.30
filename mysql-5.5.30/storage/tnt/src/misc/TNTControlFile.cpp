/**
* TNT引擎，TNTControlFile and TNTConfig。
*
* @author 何登成
*/
#include <fstream>
#include <sstream>
#include <string>
#include "misc/TNTControlFile.h"
#include "misc/Parser.h"
#include "util/SmartPtr.h"
#include "util/File.h"
#include "misc/TableDef.h"

using namespace ntse;

namespace tnt {
/**	构造函数
*	@file	控制文件
*
*/
TNTControlFile::TNTControlFile(File *file): m_lock("TNTControlFile::lock", __FILE__, __LINE__) {
	init();
	m_file = file;
}

/**	初始化对象状态
*
*/
void TNTControlFile::init() 
{
	m_fileBuf	= NULL;
	m_bufSize	= 0;
	memset(&m_dumpHeader, 0, sizeof(m_dumpHeader));
	memset(&m_verpoolHeader, 0, sizeof(m_verpoolHeader));
	m_file		= NULL;
	m_maxTrxId  = 0;
	m_cleanClosed = false;
	//m_numTxnlogs= 0; 	
	m_closed	= false;
	m_syslog	= NULL;
}

/**	打开 TNT 控制文件
*	@path		控制文件路径
*	@m_syslog	系统日志
*/
TNTControlFile* TNTControlFile::open(const char *path, Syslog *syslog) throw(NtseException)
{
	u64 errCode;
	AutoPtr<File> file(new File(path));
	AutoPtr<TNTControlFile> ret(new TNTControlFile(file));
	ret->m_syslog = syslog;

	if ((errCode = file->open(false)) != File::E_NO_ERROR)
		NTSE_THROW(errCode, "Can not open file %s", path);
	u64 fileSize;
	file->getSize(&fileSize);
	AutoPtr<char> fileBuf(new char[(size_t)fileSize + 1], true);
	NTSE_ASSERT(file->read(0, (u32)fileSize, fileBuf) == File::E_NO_ERROR);
	fileBuf[fileSize] = '\0';

	stringstream ss((char *)fileBuf);
	ss.exceptions((ifstream::eofbit | ifstream::failbit | ifstream::badbit));

	// 解析TNT控制文件内容
	try {
		char buf[Limits::MAX_PATH_LEN + 1];
		// 头三行 --Warn
		ss.getline(buf, sizeof(buf));
		ss.getline(buf, sizeof(buf));
		ss.getline(buf, sizeof(buf));
		// [basic info]
		getLineWithCheck(ss, true, '\n', buf, sizeof(buf), "[basic info]");
		// --Warn
		ss.getline(buf, sizeof(buf));

		getLineWithCheck(ss, false, ':', buf, sizeof(buf), "max_trx_id");
		ss.getline(buf, sizeof(buf));
		ret->m_maxTrxId = Parser::parseU64(buf);

		getLineWithCheck(ss, false, ':', buf, sizeof(buf), "clean_closed");
		ss.getline(buf, sizeof(buf));
		ret->m_dumpHeader.m_cleanClosed = Parser::parseBool(buf);

		getLineWithCheck(ss, false, ':', buf, sizeof(buf), "dump_lsn");
		ss.getline(buf, sizeof(buf));
		ret->m_dumpHeader.m_dumpLSN = Parser::parseU64(buf);

		getLineWithCheck(ss, false, ':', buf, sizeof(buf), "version_pool_count");
		ss.getline(buf, sizeof(buf));
		ret->m_verpoolHeader.m_verpoolCnt = (u8)Parser::parseInt(buf, 0);

		getLineWithCheck(ss, false, ':', buf, sizeof(buf), "active_version_pool");
		ss.getline(buf, sizeof(buf));
		ret->m_verpoolHeader.m_activeVerpool = (u8)Parser::parseInt(buf, 0);

		getLineWithCheck(ss, false, ':', buf, sizeof(buf), "reclaimed_version_pool");
		ss.getline(buf, sizeof(buf));
		ret->m_verpoolHeader.m_reclaimedVerpool = (u8)Parser::parseInt(buf, 0);

		// version pool information
		getLineWithCheck(ss, true, '\n', buf, sizeof(buf), "[version pool info]");
		ss.getline(buf, sizeof(buf));

		ret->m_verpoolHeader.m_vpInfo = new TNTVerpoolInfo[ret->m_verpoolHeader.m_verpoolCnt];
		for (int i = 0; i < ret->m_verpoolHeader.m_verpoolCnt; i++) {
			u16		versionpoolId;
			u16		verpoolStatus;
			TrxId	mintrxid, maxtrxid;
			
			ss.getline(buf, sizeof(buf), ':');
			versionpoolId = (u16)Parser::parseInt(buf, 0);
			assert(versionpoolId == i);

			ss.getline(buf, sizeof(buf), ',');
			ret->m_verpoolHeader.m_vpInfo[i].m_minTrxId = Parser::parseU64(buf);
			mintrxid = Parser::parseU64(buf);

			ss.getline(buf, sizeof(buf), ',');
			ret->m_verpoolHeader.m_vpInfo[i].m_maxTrxId = Parser::parseU64(buf);
			maxtrxid = Parser::parseU64(buf);

			ss.getline(buf, sizeof(buf));
			verpoolStatus = (u16)Parser::parseInt(buf, 0);
			switch (verpoolStatus) {
				case 0: ret->m_verpoolHeader.m_vpInfo[i].m_stat = VP_FREE; break;
				case 1: ret->m_verpoolHeader.m_vpInfo[i].m_stat = VP_ACTIVE; break;
				case 2: ret->m_verpoolHeader.m_vpInfo[i].m_stat = VP_USED; break;
				case 3: ret->m_verpoolHeader.m_vpInfo[i].m_stat = VP_RECLAIMING; break;
				default: assert(false);
			}
		}
	} catch (stringstream::failure &e) {
		file->close();
		ret->freeMem();
		NTSE_THROW(NTSE_EC_FORMAT_ERROR, "Invalid tnt control file: %s", e.what());
	}

	try {
		ret->check();
	} catch (NtseException &e) {
		file->close();
		ret->freeMem();
		throw e;
	}

	ret->m_cleanClosed = ret->m_dumpHeader.m_cleanClosed != 0;
	// 打开后，即将文件中的cleanClosed设置为false
	ret->m_dumpHeader.m_cleanClosed = (u32)false;
	ret->m_fileBuf = (byte *)(char *)fileBuf;
	ret->m_bufSize = (u32)fileSize;
	ret->updateFile();

	file.detatch();
	fileBuf.detatch();
	return ret.detatch();
}

/**	创建并初始化TNT控制文件
*	@path	控制文件路径
*	@syslog	系统日志
*/
void TNTControlFile::create(const char *path, TNTConfig *config, Syslog *syslog) throw(NtseException)
{
	u64	errCode;
	

	AutoPtr<File> file(new File(path));
	if ((errCode = file->create(false, false)) != File::E_NO_ERROR)
		NTSE_THROW(errCode, "Can not create file %s", path);

	TNTControlFile *cf = new TNTControlFile(file);
	file.detatch();

	
	// 设置dump相关参数
	cf->m_dumpHeader.m_dumpLSN		= 0;

	// 设置version pool相关参数
	cf->m_verpoolHeader.m_verpoolCnt		= config->m_verpoolCnt;
	cf->m_verpoolHeader.m_activeVerpool		= 0;
	cf->m_verpoolHeader.m_reclaimedVerpool	= INVALID_VERSION_POOL_INDEX;
	cf->m_verpoolHeader.m_vpInfo			= new TNTVerpoolInfo[config->m_verpoolCnt];
	cf->m_verpoolHeader.m_vpInfo[0].m_maxTrxId	= 0;
	cf->m_verpoolHeader.m_vpInfo[0].m_minTrxId	= 0;
	cf->m_verpoolHeader.m_vpInfo[0].m_stat	= VP_ACTIVE;
	for (uint i = 1; i < config->m_verpoolCnt; i++) {
		cf->m_verpoolHeader.m_vpInfo[i].m_maxTrxId = 0;
		cf->m_verpoolHeader.m_vpInfo[i].m_minTrxId	= 0;
		cf->m_verpoolHeader.m_vpInfo[i].m_stat	= VP_FREE;
	}

	cf->updateFile();

	cf->close();
	delete cf;

	syslog->log(EL_LOG, "Control file created.");
}

void TNTControlFile::lockCFMutex() {
	// assert(!m_lock.isLocked());

	m_lock.lock(__FILE__, __LINE__);
}

void TNTControlFile::unlockCFMutex() {
	assert(m_lock.isLocked());

	m_lock.unlock();
}

/**	安全关闭控制文件
*	@clean			是否安全关闭
*/
void TNTControlFile::close(bool clean)
{
	if (m_closed)
		return;
	m_cleanClosed = clean;

	updateFile();

	freeMem();
	
	m_file->close();
	delete m_file;
	delete []m_fileBuf;
	init();
	m_closed = true;
}

/*void TNTControlFile::setNumTxnlogs(u32 n) {
	assert(!m_closed);
	MutexGuard guard(&m_lock, __FILE__, __LINE__);
	m_numTxnlogs = n;
	updateFile();
}*/

LsnType	TNTControlFile::getDumpLsn() {
	MutexGuard guard(&m_lock, __FILE__, __LINE__);
	return m_dumpHeader.m_dumpLSN;
}


/**
* 获取当前系统的Active Version Pool
* @return 返回系统中的active version pool number
*/
uint TNTControlFile::getActvieVerPool() {
	MutexGuard guard(&m_lock, __FILE__, __LINE__);

	return m_verpoolHeader.m_activeVerpool;
}

/**	切换活动版本池，并且写入最大最小事务ID
*	@m_newActiveId	切换后的active version pool
*	@m_currTrxId	当前事务ID
*/
bool TNTControlFile::switchActiveVerPool(u8 newActiveId, TrxId currTrxId) {
	assert(m_lock.isLocked());

	if (newActiveId == 0) 
		assert(m_verpoolHeader.m_activeVerpool == m_verpoolHeader.m_verpoolCnt - 1);
	else 
		assert(m_verpoolHeader.m_activeVerpool == newActiveId - 1);
	
	// 切换的version pool一定是free状态
	if (m_verpoolHeader.m_vpInfo[newActiveId].m_stat != VP_FREE) {
	//	assert(m_verpoolHeader.m_vpInfo[newActiveId].m_status == VP_USED);
		return false;
	}
	
	// 设置旧active version pool的max trx id
	m_verpoolHeader.m_vpInfo[m_verpoolHeader.m_activeVerpool].m_maxTrxId = currTrxId - 1;
	m_verpoolHeader.m_vpInfo[m_verpoolHeader.m_activeVerpool].m_stat	 = VP_USED;

	// 设置新active version pool的min trx id
	m_verpoolHeader.m_vpInfo[newActiveId].m_minTrxId	= currTrxId;
	m_verpoolHeader.m_vpInfo[newActiveId].m_stat	= VP_ACTIVE;

	m_verpoolHeader.m_activeVerpool = newActiveId;
	updateFile();

	return true;
}

void TNTControlFile::writeBeginReclaimPool(u8 verPoolNum) {
	assert(m_verpoolHeader.m_vpInfo[verPoolNum].m_stat == VP_USED);
	m_verpoolHeader.m_vpInfo[verPoolNum].m_stat = VP_RECLAIMING;
	updateFile();
}

/**	写入上一次成功回收的版本池号
*	@m_verpoolNum	版本池号
*
*/
void TNTControlFile::writeLastReclaimedPool(u8 verpoolNum)
{
	assert(m_verpoolHeader.m_vpInfo[verpoolNum].m_stat == VP_RECLAIMING);
	m_verpoolHeader.m_reclaimedVerpool = verpoolNum;
	m_verpoolHeader.m_vpInfo[verpoolNum].m_stat = VP_FREE;

	updateFile();
}


/**	计算校验和。使用64位的FVN hash算法
*	@buf	数据
*	@size	数据大小
*	@return	校验和
*/
u64 TNTControlFile::checksum(const byte *buf, size_t size)
{
	
	return checksum64(buf, size);
}

/**	更新控制文件
*	实现直接copy自NTSE::ControlFile.cpp
*/
void TNTControlFile::updateFile()
{
	try {
		check();
	} catch (NtseException &e) {
		fprintf(stderr, "%s", e.getMessage());
		assert(false);
	}

	string str = serialize();

	delete []m_fileBuf;
	m_bufSize = (u32)str.size();
	m_fileBuf = new byte[m_bufSize];
	memcpy(m_fileBuf, str.c_str(), m_bufSize);

	// 写到临时文件
	string newPath = string(m_file->getPath()) + ".tmp";		// 新的控制文件
	string bakPath = string(m_file->getPath()) + ".tmpbak";		// 原控制文件的备份
	u64 code = File(newPath.c_str()).remove();
	if (!(code == File::E_NO_ERROR || File::getNtseError(code) == File::E_NOT_EXIST))
		m_syslog->fopPanic(code, "Unable to remove old tmp file: %s", newPath.c_str());
	code = File(bakPath.c_str()).remove();
	if (!(code == File::E_NO_ERROR || File::getNtseError(code) == File::E_NOT_EXIST))
		m_syslog->fopPanic(code, "Unable to remove old tmp bak file: %s", bakPath.c_str());

	File newFile(newPath.c_str());

	code = newFile.create(false, false);
	if (code != File::E_NO_ERROR)
		m_syslog->fopPanic(code, "Unable to create file: %s", newFile.getPath());

	code = newFile.setSize(m_bufSize);
	if (code != File::E_NO_ERROR)
		m_syslog->fopPanic(code, "Unable to set size of file %s to %d", newFile.getPath(), m_bufSize);

	code = newFile.write(0, m_bufSize, m_fileBuf);
	if (code != File::E_NO_ERROR)
		m_syslog->fopPanic(code, "Unable to write %d bytes to file", m_bufSize, newFile.getPath());

	code = newFile.close();
	if (code != File::E_NO_ERROR)
		m_syslog->fopPanic(code, "Unable to close file: %s", newFile.getPath());

	// 替换。测试发现删除原文件，再用新文件替换；或者直接用新文件替换指定覆盖原文件时，
	// 在某些机器上会莫名其妙的失败。经实验将原文件重命名，然后重名名新文件到原文件，
	// 再删除重命名之后的原文件就没有问题
	code = m_file->close();
	if (code != File::E_NO_ERROR)
		m_syslog->fopPanic(code, "Unable to close file: %s", m_file->getPath());

	code = m_file->move(bakPath.c_str());
	if (code != File::E_NO_ERROR)
		m_syslog->fopPanic(code, "Unable to move %s to %s", m_file->getPath(), bakPath.c_str());

	code = File(newPath.c_str()).move(m_file->getPath());
	if (code != File::E_NO_ERROR)
		m_syslog->fopPanic(code, "Unable to move %s to %s", newPath.c_str(), m_file->getPath());

	code = File(bakPath.c_str()).remove();
	if (code != File::E_NO_ERROR)
		m_syslog->fopPanic(code, "Unable to remove file %s", bakPath.c_str());

	code = m_file->open(false);
	if (code != File::E_NO_ERROR)
		m_syslog->fopPanic(code, "Unable to open file: %s", m_file->getPath());
}

/**	检查数据一致性
*
*/
void TNTControlFile::check() throw(NtseException)
{
	// TNT控制文件，暂时没有需要check的内容
	return;
}

/**	释放内存
*
*/
void TNTControlFile::freeMem()
{
	// 释放path与version pool info数组空间
	delete []m_verpoolHeader.m_vpInfo;
	m_verpoolHeader.m_vpInfo = NULL;

}

/**	序列化控制文件内容
*	@return	序列化结果
*/
string TNTControlFile::serialize()
{
	stringstream	ss;
	TNTVerpoolInfo	*vpInfo = NULL;
	ss.exceptions(ifstream::eofbit | ifstream::failbit | ifstream::badbit);

	ss << "-- Warn: do not edit this file by hand unless you're the SUPER MAN with enough skills." << endl;
	ss << "-- Warn: don't change the order of each section." << endl;
	ss << "-- Warn: don't add/remove/edit any comments." << endl;

	ss << "[basic info]" << endl;
	ss << "-- Warn: don't change the order of each variable in this section." << endl;
	ss << "max_trx_id:" << m_maxTrxId << endl;
	ss << "clean_closed:" << (m_dumpHeader.m_cleanClosed ? "true": "false") << endl;
	ss << "dump_lsn:" << m_dumpHeader.m_dumpLSN << endl;
	ss << "version_pool_count:" << (u16)m_verpoolHeader.m_verpoolCnt << endl;
	ss << "active_version_pool:" << (u16)m_verpoolHeader.m_activeVerpool << endl;
	ss << "reclaimed_version_pool:" << (u16)m_verpoolHeader.m_reclaimedVerpool << endl;

	ss << "[version pool info]" << endl;
	ss << "-- Format: id,min_trx_id,max_trx_id,versionpool_status" << endl;
	
	vpInfo = m_verpoolHeader.m_vpInfo;

	for (int i = 0; i < m_verpoolHeader.m_verpoolCnt; i++)
		ss << i << ":" << vpInfo[i].m_minTrxId << "," << vpInfo[i].m_maxTrxId << "," << (int)vpInfo[i].m_stat << endl;

	return ss.str();
}

/**	从输入流中读取一行并检查是否与期望的内容相匹配
*	@ss		输入流
*	@eol	行是否以换行结束
*	@delim	当eol为false时，指定行结束符，在eol为true时不用
*	@buf	存储读入数据的内存区
*	@bufSize	buf的大小	
*	@expected	期望的内容
*/
void TNTControlFile::getLineWithCheck(stringstream &ss, bool eol, char delim, char *buf, size_t bufSize, const char *expected) throw(NtseException)
{
	if (eol)
		ss.getline(buf, bufSize);
	else
		ss.getline(buf, bufSize, delim);
	if (strcmp(buf, expected))
		NTSE_THROW(NTSE_EC_FORMAT_ERROR, "Invalid control file, expect %s, but was %s", expected, buf);
}

}


/**
 * 复制控制文件内容
 * @param size [out] 控制文件长度
 * @return 控制文件内容（调用者释放)
 */
byte* TNTControlFile::dupContent(u32 *size) {
	assert(m_lock.isLocked());
	assert(!m_closed);

	*size = m_bufSize;
	byte *outBuffer = new byte[m_bufSize];
	memcpy(outBuffer, m_fileBuf, m_bufSize);
	return outBuffer;
}
