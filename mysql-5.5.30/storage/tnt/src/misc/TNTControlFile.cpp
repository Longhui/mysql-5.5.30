/**
* TNT���棬TNTControlFile and TNTConfig��
*
* @author �εǳ�
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
/**	���캯��
*	@file	�����ļ�
*
*/
TNTControlFile::TNTControlFile(File *file): m_lock("TNTControlFile::lock", __FILE__, __LINE__) {
	init();
	m_file = file;
}

/**	��ʼ������״̬
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

/**	�� TNT �����ļ�
*	@path		�����ļ�·��
*	@m_syslog	ϵͳ��־
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

	// ����TNT�����ļ�����
	try {
		char buf[Limits::MAX_PATH_LEN + 1];
		// ͷ���� --Warn
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
	// �򿪺󣬼����ļ��е�cleanClosed����Ϊfalse
	ret->m_dumpHeader.m_cleanClosed = (u32)false;
	ret->m_fileBuf = (byte *)(char *)fileBuf;
	ret->m_bufSize = (u32)fileSize;
	ret->updateFile();

	file.detatch();
	fileBuf.detatch();
	return ret.detatch();
}

/**	��������ʼ��TNT�����ļ�
*	@path	�����ļ�·��
*	@syslog	ϵͳ��־
*/
void TNTControlFile::create(const char *path, TNTConfig *config, Syslog *syslog) throw(NtseException)
{
	u64	errCode;
	

	AutoPtr<File> file(new File(path));
	if ((errCode = file->create(false, false)) != File::E_NO_ERROR)
		NTSE_THROW(errCode, "Can not create file %s", path);

	TNTControlFile *cf = new TNTControlFile(file);
	file.detatch();

	
	// ����dump��ز���
	cf->m_dumpHeader.m_dumpLSN		= 0;

	// ����version pool��ز���
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

/**	��ȫ�رտ����ļ�
*	@clean			�Ƿ�ȫ�ر�
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
* ��ȡ��ǰϵͳ��Active Version Pool
* @return ����ϵͳ�е�active version pool number
*/
uint TNTControlFile::getActvieVerPool() {
	MutexGuard guard(&m_lock, __FILE__, __LINE__);

	return m_verpoolHeader.m_activeVerpool;
}

/**	�л���汾�أ�����д�������С����ID
*	@m_newActiveId	�л����active version pool
*	@m_currTrxId	��ǰ����ID
*/
bool TNTControlFile::switchActiveVerPool(u8 newActiveId, TrxId currTrxId) {
	assert(m_lock.isLocked());

	if (newActiveId == 0) 
		assert(m_verpoolHeader.m_activeVerpool == m_verpoolHeader.m_verpoolCnt - 1);
	else 
		assert(m_verpoolHeader.m_activeVerpool == newActiveId - 1);
	
	// �л���version poolһ����free״̬
	if (m_verpoolHeader.m_vpInfo[newActiveId].m_stat != VP_FREE) {
	//	assert(m_verpoolHeader.m_vpInfo[newActiveId].m_status == VP_USED);
		return false;
	}
	
	// ���þ�active version pool��max trx id
	m_verpoolHeader.m_vpInfo[m_verpoolHeader.m_activeVerpool].m_maxTrxId = currTrxId - 1;
	m_verpoolHeader.m_vpInfo[m_verpoolHeader.m_activeVerpool].m_stat	 = VP_USED;

	// ������active version pool��min trx id
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

/**	д����һ�γɹ����յİ汾�غ�
*	@m_verpoolNum	�汾�غ�
*
*/
void TNTControlFile::writeLastReclaimedPool(u8 verpoolNum)
{
	assert(m_verpoolHeader.m_vpInfo[verpoolNum].m_stat == VP_RECLAIMING);
	m_verpoolHeader.m_reclaimedVerpool = verpoolNum;
	m_verpoolHeader.m_vpInfo[verpoolNum].m_stat = VP_FREE;

	updateFile();
}


/**	����У��͡�ʹ��64λ��FVN hash�㷨
*	@buf	����
*	@size	���ݴ�С
*	@return	У���
*/
u64 TNTControlFile::checksum(const byte *buf, size_t size)
{
	
	return checksum64(buf, size);
}

/**	���¿����ļ�
*	ʵ��ֱ��copy��NTSE::ControlFile.cpp
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

	// д����ʱ�ļ�
	string newPath = string(m_file->getPath()) + ".tmp";		// �µĿ����ļ�
	string bakPath = string(m_file->getPath()) + ".tmpbak";		// ԭ�����ļ��ı���
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

	// �滻�����Է���ɾ��ԭ�ļ����������ļ��滻������ֱ�������ļ��滻ָ������ԭ�ļ�ʱ��
	// ��ĳЩ�����ϻ�Ī�������ʧ�ܡ���ʵ�齫ԭ�ļ���������Ȼ�����������ļ���ԭ�ļ���
	// ��ɾ��������֮���ԭ�ļ���û������
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

/**	�������һ����
*
*/
void TNTControlFile::check() throw(NtseException)
{
	// TNT�����ļ�����ʱû����Ҫcheck������
	return;
}

/**	�ͷ��ڴ�
*
*/
void TNTControlFile::freeMem()
{
	// �ͷ�path��version pool info����ռ�
	delete []m_verpoolHeader.m_vpInfo;
	m_verpoolHeader.m_vpInfo = NULL;

}

/**	���л������ļ�����
*	@return	���л����
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

/**	���������ж�ȡһ�в�����Ƿ���������������ƥ��
*	@ss		������
*	@eol	���Ƿ��Ի��н���
*	@delim	��eolΪfalseʱ��ָ���н���������eolΪtrueʱ����
*	@buf	�洢�������ݵ��ڴ���
*	@bufSize	buf�Ĵ�С	
*	@expected	����������
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
 * ���ƿ����ļ�����
 * @param size [out] �����ļ�����
 * @return �����ļ����ݣ��������ͷ�)
 */
byte* TNTControlFile::dupContent(u32 *size) {
	assert(m_lock.isLocked());
	assert(!m_closed);

	*size = m_bufSize;
	byte *outBuffer = new byte[m_bufSize];
	memcpy(outBuffer, m_fileBuf, m_bufSize);
	return outBuffer;
}
