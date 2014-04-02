/**
 * �����ļ�ʵ��
 *
 * @author ��Դ(wangyuan@corp.netease.com, wy@163.org)
 */

#include <fstream>
#include <sstream>
#include <string>
#include "misc/ControlFile.h"
#include "misc/Syslog.h"
#include "util/File.h"
#include "util/Stream.h"
#include "util/System.h"
#include "util/SmartPtr.h"
#include "api/Table.h"
#include "util/System.h"
#include "misc/Txnlog.h"
#include "misc/Parser.h"

using namespace std;

namespace ntse {

/** ����һ������Ϣ����
 * @param id ��ID
 * @param path ��·��
 * @param hasLob ���Ƿ���������
 * @param hasCprsDic �����Ƿ���ѹ��ȫ���ֵ�
 */
CFTblInfo::CFTblInfo(u16 id, const char *path, bool hasLob, bool hasCprsDic) {
	m_id = id;
	m_path = System::strdup(path);
	m_hasLob = hasLob;
	m_flushLsn = 0;
	m_hasCprsDict = hasCprsDic;
	m_tntFlushLsn = 0;
}

/** ����һ���յı���Ϣ���� */
CFTblInfo::CFTblInfo() {
	m_path = NULL;
	m_tntFlushLsn = 0;
}

/** ����һ������Ϣ���� */
CFTblInfo::~CFTblInfo() {
	delete []m_path;
}

/** ���ñ�·������RENAMEʱ����
 * @param newPath �µı�·��
 */
void CFTblInfo::setPath(const char *newPath) {
	delete []m_path;
	m_path = System::strdup(newPath);
}

/**
 * ���ñ��Ƿ���ѹ���ֵ��ļ�
 * @param hasCprsDic �Ƿ���ѹ���ֵ��ļ�
 */
void CFTblInfo::setHasCprsDic(bool hasCprsDict) {
	m_hasCprsDict = hasCprsDict;
}

/** �����ж�ȡһ������Ϣ����
 * @param s ��
 * @param tnt ����tnt�����������Ϊtrue��������ntse�������
 * @throw Խ��
 * @return ��ȡ�ı���Ϣ����
 */
CFTblInfo* CFTblInfo::read(istream &s, bool tnt) throw(NtseException) {
	char buf[Limits::MAX_PATH_LEN + 1];
	CFTblInfo *r = new CFTblInfo();
	
	try {
		s.getline(buf, sizeof(buf), ':');
		r->m_id = (u16)Parser::parseInt(buf, 1, TableDef::MAX_NORMAL_TABLEID);
		
		s.getline(buf, sizeof(buf), ',');
		r->m_path = System::strdup(buf);

		s.getline(buf, sizeof(buf), ',');
		r->m_flushLsn = Parser::parseU64(buf);

		s.getline(buf, sizeof(buf), ',');
		r->m_hasLob = Parser::parseBool(buf);

		if (!tnt) {
			s.getline(buf, sizeof(buf));
			r->m_hasCprsDict = Parser::parseBool(buf);
		} else {
			s.getline(buf, sizeof(buf), ',');
			r->m_hasCprsDict = Parser::parseBool(buf);

			s.getline(buf, sizeof(buf));
			r->m_tntFlushLsn = Parser::parseU64(buf);
		}
	} catch (NtseException &e) {
		delete r;
		throw e;
	} catch (stringstream::failure &e) {
		delete r;
		throw e;
	}
	return r;
}

/** д��һ������Ϣ��������
 * @param s ��
 * @throw дԽ��
 */
void CFTblInfo::write(ostream &s) throw(NtseException) {
	s << m_id << ':' << m_path << ',' << m_flushLsn << ',' << (m_hasLob? "true": "false") << "," << (m_hasCprsDict ? "true" : "false") << "," << m_tntFlushLsn << endl;
}

/** ���캯��
 * @param file �����ļ�
 */
ControlFile::ControlFile(File *file): m_tables(TableDef::MAX_NORMAL_TABLEID), 
	m_pathToIds(TableDef::MAX_NORMAL_TABLEID),
	m_deleted(TableDef::MAX_NORMAL_TABLEID),
	m_lock("ControlFile::lock", __FILE__, __LINE__) {
	init();
	m_file = file;
	for (int i = TableDef::MIN_TEMP_TABLEID; i <= (int)TableDef::MAX_TEMP_TABLEID; i++) {
		m_tempTableIdUsed.push_back(false);
	}
}

/** ��ʼ������״̬ */
void ControlFile::init() {
	m_fileBuf = NULL;
	m_bufSize = 0;
	memset(&m_header, 0, sizeof(m_header));
	m_file = NULL;
	m_closed = false;
	m_cleanClosed = false;
}

/** �򿪿����ļ�
 * 
 * @param path �����ļ�·��
 * @param syslog ϵͳ��־
 * @param tnt ��Ҫ�򿪵��Ƿ�Ϊtnt��control�ļ�
 * @return �����ļ�
 * @throw NtseException �ļ������ڣ��ļ���ʽ����ȷ��
 */
ControlFile* ControlFile::open(const char *path, Syslog *syslog, bool tnt) throw(NtseException) {
	u64 errCode;
	AutoPtr<File> file(new File(path));
	AutoPtr<ControlFile> ret(new ControlFile(file));
	ret->m_syslog = syslog;

	if ((errCode = file->open(false)) != File::E_NO_ERROR)
		NTSE_THROW(errCode, "Can not open file %s", path);
	u64 fileSize;
	file->getSize(&fileSize);
	AutoPtr<char> fileBuf(new char[(size_t)fileSize + 1], true);
	NTSE_ASSERT(file->read(0, (u32)fileSize, fileBuf) == File::E_NO_ERROR);
	fileBuf[fileSize] = '\0';

	stringstream ss((char *)fileBuf);
	ss.exceptions(ifstream::eofbit | ifstream::failbit | ifstream::badbit);

	// �����ļ�����
	try {
		char buf[Limits::MAX_PATH_LEN + 1];
		// ͷ����--Warn
		ss.getline(buf, sizeof(buf));
		ss.getline(buf, sizeof(buf));
		ss.getline(buf, sizeof(buf));
		// [basic info]
		getLineWithCheck(ss, true, '\n', buf, sizeof(buf), "[basic info]");
		/// --Warn
		ss.getline(buf, sizeof(buf));

		getLineWithCheck(ss, false, ':', buf, sizeof(buf), "clean_closed");
		ss.getline(buf, sizeof(buf));
		ret->m_header.m_cleanClosed = Parser::parseBool(buf);

		getLineWithCheck(ss, false, ':', buf, sizeof(buf), "checkpoint_lsn");
		ss.getline(buf, sizeof(buf));
		ret->m_header.m_checkpointLsn = Parser::parseU64(buf);

		getLineWithCheck(ss, false, ':', buf, sizeof(buf), "next_table_id");
		ss.getline(buf, sizeof(buf));
		ret->m_header.m_nextTableId = (u16)Parser::parseInt(buf, 1, TableDef::MAX_NORMAL_TABLEID + 1);

		getLineWithCheck(ss, false, ':', buf, sizeof(buf), "num_tables");
		ss.getline(buf, sizeof(buf));
		ret->m_header.m_numTables = (u16)Parser::parseInt(buf, 0, TableDef::MAX_NORMAL_TABLEID);

		getLineWithCheck(ss, false, ':', buf, sizeof(buf), "num_deleted_tables");
		ss.getline(buf, sizeof(buf));
		ret->m_header.m_numDeleted = (u16)Parser::parseInt(buf, 0, TableDef::MAX_NORMAL_TABLEID);

		getLineWithCheck(ss, false, ':', buf, sizeof(buf), "num_txn_logs");
		ss.getline(buf, sizeof(buf));
		ret->m_header.m_numTxnlogs = (u16)Parser::parseInt(buf, 1, TableDef::MAX_NORMAL_TABLEID);

		getLineWithCheck(ss, false, ':', buf, sizeof(buf), "num_temp_files");
		ss.getline(buf, sizeof(buf));
		ret->m_header.m_numTempFiles = Parser::parseU64(buf);

		getLineWithCheck(ss, false, ':', buf, sizeof(buf), "temp_file_seq");
		ss.getline(buf, sizeof(buf));
		ret->m_header.m_tempFileSeq = Parser::parseU64(buf);

		getLineWithCheck(ss, false, ':', buf, sizeof(buf), "rec_storage_version");
		ss.getline(buf, sizeof(buf));
		int recStorageVersion = Parser::parseInt(buf);
		if (recStorageVersion != REC_STORAGE_VERSION)
			NTSE_THROW(NTSE_EC_FORMAT_ERROR, "Wrong record storage format");

		getLineWithCheck(ss, false, ':', buf, sizeof(buf), "idx_storage_version");
		ss.getline(buf, sizeof(buf));
		int idxStorageVersion = Parser::parseInt(buf);
		if (idxStorageVersion != IDX_STORAGE_VERSION)
			NTSE_THROW(NTSE_EC_FORMAT_ERROR, "Wrong index storage format");

		getLineWithCheck(ss, true, '\n', buf, sizeof(buf), "[tables]");
		ss.getline(buf, sizeof(buf));
		for (u16 i = 0; i < ret->m_header.m_numTables; i++) {
			CFTblInfo *tblInfo = CFTblInfo::read(ss, tnt);
			ret->m_tables.put(tblInfo->m_id, tblInfo);
			ret->m_pathToIds.put(tblInfo->m_path, tblInfo->m_id);
		}

		getLineWithCheck(ss, true, '\n', buf, sizeof(buf), "[deleted tables]");
		for (u16 i = 0; i < ret->m_header.m_numDeleted; i++) {
			CFTblInfo *tblInfo = CFTblInfo::read(ss, tnt);
			ret->m_deleted.put(tblInfo->m_id, tblInfo);
		}

		getLineWithCheck(ss, true, '\n', buf, sizeof(buf), "[temp files]");
		for (u64 i = 0; i < ret->m_header.m_numTempFiles; i++) {
			ss.getline(buf, sizeof(buf));
			ret->m_tempFiles.insert(string(buf));
		}
	} catch (stringstream::failure &e) {
		file->close();
		ret->freeMem();
		NTSE_THROW(NTSE_EC_FORMAT_ERROR, "Invalid control file: %s", e.what());
	}

	try {
		ret->check();
	} catch (NtseException &e) {
		file->close();
		ret->freeMem();
		throw e;
	}

	ret->m_cleanClosed = ret->m_header.m_cleanClosed != 0;
	// �򿪺󣬼����ļ��е�cleanClosed��Ϊfalse
	ret->m_header.m_cleanClosed = (u32)false;
	ret->m_fileBuf = (byte *)(char *)fileBuf;
	ret->m_bufSize = (u32)fileSize;
	ret->updateFile();

	file.detatch();
	fileBuf.detatch();
	return ret.detatch();
}

/** ���������ж�ȡһ�в�����Ƿ���������������ƥ��
 * @param ss ������
 * @param eol ���Ƿ��Ի��н���
 * @param delim ��eolΪfalseʱָ���н���������eolΪtrueʱ����
 * @param buf �洢�������ݵ��ڴ���
 * @param bufSize buf�Ĵ�С
 * @param expected ����������
 * @throw NtseException ��������������������ݲ�ͬ
 * @throw stringstream::failure ��ȡʧ��
 */
void ControlFile::getLineWithCheck(stringstream &ss, bool eol, char delim, char *buf, size_t bufSize, const char *expected) throw(NtseException) {
	if (eol)
		ss.getline(buf, bufSize);
	else
		ss.getline(buf, bufSize, delim);
	if (strcmp(buf, expected))
		NTSE_THROW(NTSE_EC_FORMAT_ERROR, "Invalid control file, expect %s, but was %s", expected, buf);
}

/**
 * ��������ʼ�������ļ�
 *
 * @param path �����ļ�·��
 * @param syslog ϵͳ��־
 * @throw NtseException �ļ��Ѿ����ڣ���Ȩ���޷������ļ���
 */
#ifdef TNT_ENGINE
void ControlFile::create(const char *path, Syslog *syslog, uint numTxnLogs) throw(NtseException) {
#else
void ControlFile::create(const char *path, Syslog *syslog) throw(NtseException) {
#endif
	u64 errCode;
	AutoPtr<File> file(new File(path));
	if ((errCode = file->create(false, false)) != File::E_NO_ERROR)
		NTSE_THROW(errCode, "Can not create file %s", path);
	
	ControlFile *cf = new ControlFile(file);
	file.detatch();

	cf->m_header.m_numTables = 0;
	cf->m_header.m_numDeleted = 0;
	cf->m_header.m_nextTableId = 1;
	cf->m_header.m_checkpointLsn = 0;
	cf->m_header.m_cleanClosed = (u32)true;
#ifdef TNT_ENGINE
	cf->m_header.m_numTxnlogs = numTxnLogs;
#else
	cf->m_header.m_numTxnlogs = LogConfig::DEFAULT_NUM_TXNLOGS;
#endif
	cf->updateFile();
	
	cf->close();
	delete cf;

	syslog->log(EL_LOG, "Control file created.");
}

/**
 * ��ȫ�رտ����ļ�
 *
 * @param cleanUpTemps �Ƿ�������ʱ�ļ�
 * @param clean �Ƿ�ȫ�ر�
 */
void ControlFile::close(bool cleanUpTemps, bool clean) {
	if (m_closed)
		return;
	if (cleanUpTemps) {
		cleanUpTempFiles();
	}
	m_header.m_cleanClosed = (u32)clean;
	updateFile();

	freeMem();

	m_file->close();
	delete m_file;

	delete []m_fileBuf;
	init();
	m_closed = true;
}

/** �ͷ�ռ�õ��ڴ� */
void ControlFile::freeMem() {
	// �ͷ�m_tables��m_pathToIdsռ�õĿռ�
	CFTblInfo **tblInfos = new CFTblInfo *[m_tables.getSize()];
	m_tables.values(tblInfos);
	for (u16 i = 0; i < m_header.m_numTables; i++)
		delete tblInfos[i];
	delete []tblInfos;
	m_tables.clear();
	m_pathToIds.clear();

	// �ͷ�m_deletedռ�õĿռ�
	tblInfos = new CFTblInfo *[m_deleted.getSize()];
	m_deleted.values(tblInfos);
	for (u16 i = 0; i < m_header.m_numDeleted; i++)
		delete tblInfos[i];
	delete []tblInfos;
	m_deleted.clear();
}

/**
 * ����һ�����õı�ID��ֻ���ڷ�����ͨ���Ǵ�����������ID
 *
 * @return ��ID
 * @throw NtseException ��ID�Ѿ�����
 */
u16 ControlFile::allocTableId() throw(NtseException) {
	assert(!m_closed);

	MutexGuard guard(&m_lock, __FILE__, __LINE__);

	if (m_header.m_nextTableId > TableDef::MAX_NORMAL_TABLEID)
		NTSE_THROW(NTSE_EC_EXCEED_LIMIT, "All table ids has been used");
	m_header.m_nextTableId++;
	updateFile();
		
	return m_header.m_nextTableId - 1;
}

/**
 * �����±�ʱ֪ͨ�����ļ����ӱ���Ϣ
 * @pre ָ��ID��·���ı����ڡ�tableIdһ��Ϊ�շ����ȥ��ID
 *
 * @param path ���ļ�·��(�����basedir)
 * @param tableId ��ID
 * @param hasLob ���Ƿ���������
 * @param hasCprsDic ���Ƿ���ѹ���ֵ��ļ�
 */
void ControlFile::createTable(const char *path, u16 tableId, bool hasLob, bool hasCprsDict/*=false*/) {
	assert(!m_closed && !m_tables.get(tableId) && !m_pathToIds.get((char *)path) && TableDef::tableIdIsNormal(tableId));
	assert(tableId == m_header.m_nextTableId - 1);

	MutexGuard guard(&m_lock, __FILE__, __LINE__);

	CFTblInfo *tblInfo = new CFTblInfo(tableId, path, hasLob, hasCprsDict);
	m_tables.put(tableId, tblInfo);
	m_pathToIds.put(tblInfo->m_path, tableId);
	m_header.m_numTables++;
	assert(m_tables.getSize() == m_header.m_numTables);

	updateFile();
}

/**
 * ɾ����ʱ֪ͨ�����ļ�ɾ������Ϣ
 *
 * @param tableId ��ID
 */
void ControlFile::dropTable(u16 tableId) {
	assert(!m_closed && TableDef::tableIdIsNormal(tableId));

	MutexGuard guard(&m_lock, __FILE__, __LINE__);

	CFTblInfo *tblInfo = m_tables.remove(tableId);
	assert(tblInfo);
	NTSE_ASSERT(m_pathToIds.remove(tblInfo->m_path));
	m_header.m_numTables--;
	m_header.m_numDeleted++;
	assert(!m_deleted.get(tableId));
	m_deleted.put(tableId, tblInfo);

	updateFile();
}

/**
 * ��������
 * @pre ���������̶����ǻָ��У�oldPathӦ���ڣ�newPath������
 *
 * @param oldPath ��ԭ���Ĵ洢·��(�����basedir)
 * @param newPath �����ڵĴ洢·��(�����basedir)
 */
void ControlFile::renameTable(const char *oldPath, const char *newPath) {
	assert(!m_closed);

	MutexGuard guard(&m_lock, __FILE__, __LINE__);
	u16 tableId = m_pathToIds.get((char *)oldPath);
	if (tableId) {	// �ָ�ʱ���ܳ��ֿ����ļ��е�·���Ѿ��޸ĵ����
		CFTblInfo *tblInfo = m_tables.get(tableId);
		assert(!strcmp(tblInfo->m_path, oldPath) && tblInfo->m_id == tableId);
		m_pathToIds.remove((char *)oldPath);
		tblInfo->setPath(newPath);
		assert(!m_pathToIds.get((char *)newPath));
		m_pathToIds.put(tblInfo->m_path, tableId);
		
		updateFile();
	}
}

/**
 * �޸ı��Ƿ���ѹ���ֵ��ļ�
 * @pre �ϲ���˱�Ҫ����
 * @param tableId ��Id
 * @param hasCprsDic �Ƿ���ȫ��ѹ���ֵ��ļ�
 */
void ControlFile::alterHasCprsDic(u16 tableId, bool hasCprsDict) {
	assert(!m_closed);

	MutexGuard guard(&m_lock, __FILE__, __LINE__);

	if (tableId) {
		CFTblInfo *tblInfo = m_tables.get(tableId);
		if (tblInfo->m_hasCprsDict == hasCprsDict)
			return;
		tblInfo->setHasCprsDic(hasCprsDict);
		updateFile();
	}
}

/** ���ñ��flushLsn�����ñ��flushLsn�����ڶԱ���нṹ���޸�֮ǰ���ã�ͨ�����±��
 * flushLsn��ʹ���ڻָ�ʱ����֮ǰ����־��������Ϊ�Ա���нṹ���޸�֮��ԭ������־����
 * ���������µı�ṹ������������Ϊ�˱�֤������־��ϵͳ�Կ��Իָ����ڶԱ���нṹ���޸�
 * ʱ��Ӧ��ѭ���´���:
 * 1. �رձ��ǿ��д���������������ݣ�
 * 2. ���ñ��flushLsn����Ҫֱ�ӵ��ñ�������Ӧ����Database::bumpFlushLsn��
 * 3. ���нṹ���޸�
 *
 * ��Ľṹ���޸İ����κζԱ��壨TableDef�ṹ�����޸ġ�RENAME��TRUNCATE������
 *
 * @param tableId ��ID
 * @param flushLsn �µ�flushLsn�������ԭ����flushLsn��
 */
void ControlFile::bumpFlushLsn(u16 tableId, u64 flushLsn) {
	assert(!m_closed && TableDef::tableIdIsNormal(tableId));

	MutexGuard guard(&m_lock, __FILE__, __LINE__);
	CFTblInfo *tblInfo = m_tables.get(tableId);
	assert(tblInfo->m_flushLsn < flushLsn);
	tblInfo->m_flushLsn = flushLsn;
	updateFile();
}

/** ���ñ��flushLsn��tntFlushLsn
 * @param tableId ��Id
 * @param flushLsn ntse��flushLsn��ntse recover����С�ڸ�lsn��redo/undo
 * @param tntFlushLsn tnt��flushLsn��tnt recover����С�ڸ�lsn��redo/undo
 */
void ControlFile::bumpTntAndNtseFlushLsn(u16 tableId, u64 flushLsn, u64 tntFlushLsn) {
	assert(!m_closed && TableDef::tableIdIsNormal(tableId));

	MutexGuard guard(&m_lock, __FILE__, __LINE__);
	CFTblInfo *tblInfo = m_tables.get(tableId);
	assert(tblInfo->m_flushLsn < flushLsn);
	tblInfo->m_flushLsn = flushLsn;

	assert(tblInfo->m_tntFlushLsn < tntFlushLsn);
	tblInfo->m_tntFlushLsn = tntFlushLsn;

	updateFile();
}

/** ��ñ��flushLsn
 * @param tableId ��ID
 * @return ���flushLsn
 */
u64 ControlFile::getFlushLsn(u16 tableId) {
	assert(!m_closed && TableDef::tableIdIsNormal(tableId));

	MutexGuard guard(&m_lock, __FILE__, __LINE__);
	return m_tables.get(tableId)->m_flushLsn;
}

/** ��ñ��flushLsn
 * @param tableId ��ID
 * @return ���tntFlushLsn
 */
u64 ControlFile::getTntFlushLsn(u16 tableId) {
	assert(!m_closed && TableDef::tableIdIsNormal(tableId));

	MutexGuard guard(&m_lock, __FILE__, __LINE__);
	return m_tables.get(tableId)->m_tntFlushLsn;
}

/**
 * �õ����һ�μ���LSN
 *
 * @return ���һ�μ���LSN
 */
u64 ControlFile::getCheckpointLSN() {
	assert(!m_closed);
#ifdef TNT_ENGINE
	MutexGuard guard(&m_lock, __FILE__, __LINE__);
#endif
	return m_header.m_checkpointLsn;
}

/**
 * ���ü���LSN
 *	������ֻ��Txnlog::setCheckpointLSN����
 * @param lsn ����LSN
 */
void ControlFile::setCheckpointLSN(u64 lsn) {
	assert(!m_closed);

	MutexGuard guard(&m_lock, __FILE__, __LINE__);

	m_header.m_checkpointLsn = lsn;
	updateFile();
}

/**
 * �Ƿ�ȫ�ر�
 * @return ϵͳ�Ƿ�ȫ�ر�
 */
bool ControlFile::isCleanClosed() {
	assert(!m_closed);
	return m_cleanClosed;
}

/**
 * ����������־�ļ�����
 * 
 * @param n ������־�ļ�����
 */
void ControlFile::setNumTxnlogs(u32 n) {
	assert(!m_closed);
	
	MutexGuard guard(&m_lock, __FILE__, __LINE__);

	m_header.m_numTxnlogs = n;
	updateFile();
}

/**
 * ����������־�ļ�����
 *
 * @return ������־�ļ�����
 */
u32 ControlFile::getNumTxnlogs() {
	assert(!m_closed);
	return m_header.m_numTxnlogs;
}

/**
 * ���ر���Ŀ
 *
 * @return ���ݿ��еı����
 */
u16 ControlFile::getNumTables() {
	assert(!m_closed);
	return m_header.m_numTables;
}

/** 
 * �������ݿ��е����б��ID
 * @param tableIds OUT�����tableid�����飬�ڴ�ռ�����߷���
 * @param maxTabCnt tableIds�������Ԫ�ظ���
 * @return ʵ������tableid����
 */
u16 ControlFile::listAllTables(u16 *tableIds, u16 maxTabCnt) {
	assert(!m_closed);

	MutexGuard guard(&m_lock, __FILE__, __LINE__);
	u16 *allTabIds = new u16[m_header.m_numTables];
	CFTblInfo **allTblInfos = new CFTblInfo *[m_header.m_numTables];
	m_tables.elements(allTabIds, allTblInfos);
	u16 cnt = min(m_header.m_numTables, maxTabCnt);
	memcpy(tableIds, allTabIds, sizeof(u16) * cnt);
	delete [] allTabIds;
	delete [] allTblInfos;
	return cnt;
}

/**
 * ���ݱ�ID�õ����·��
 *
 * @param tableId ��ID
 * @return ָ���ı��·��(�����basedir)
 */
string ControlFile::getTablePath(u16 tableId) {
//	assert(!m_closed && TableDef::tableIdIsNormal(tableId));   �˴�Ҫ�ж��Ƿ���С�ʹ�������ͨ��tableId���жϣ�

	MutexGuard guard(&m_lock, __FILE__, __LINE__);
	CFTblInfo *tblInfo = m_tables.get(tableId);
	if (!tblInfo)
		return "";
	return string(tblInfo->m_path);
}

/**
 * ���ݱ�·���õ���ID
 *
 * @param path ��·��(�����basedir)����Сд������
 * @return ָ���ı��ID�����Ҳ�������INVALID_TABLEID
 */
u16 ControlFile::getTableId(const char *path) {
	assert(!m_closed);

	MutexGuard guard(&m_lock, __FILE__, __LINE__);
	return m_pathToIds.get((char *)path);
}

/** ���ָ���ı��Ƿ���������
 * @param tableId ��ID
 * @return ָ���ı��Ƿ���������
 */
bool ControlFile::hasLob(u16 tableId) {
	assert(!m_closed && TableDef::tableIdIsNormal(tableId));

	MutexGuard guard(&m_lock, __FILE__, __LINE__);
	return m_tables.get(tableId)->m_hasLob;
}

/**
 * �ж�ָ���ı��Ƿ���ѹ���ֵ��ļ�
 * @param tableId ��ID
 * @param ָ���ı��Ƿ���ѹ���ֵ��ļ�
 */
bool ControlFile::hasCprsDict(u16 tableId) {
	assert(!m_closed && TableDef::tableIdIsNormal(tableId));

	MutexGuard guard(&m_lock, __FILE__, __LINE__);
	return m_tables.get(tableId)->m_hasCprsDict;
}

/** �ж�ָ��ID�ı��Ƿ��Ѿ���ɾ��
 *
 * @param tableId ��ID
 * @return ָ��ID�ı��Ƿ��Ѿ���ɾ��
 */
bool ControlFile::isDeleted(u16 tableId) {
	assert(!m_closed && TableDef::tableIdIsNormal(tableId));

	MutexGuard guard(&m_lock, __FILE__, __LINE__);
	return m_deleted.get(tableId) != NULL;
}

/**
 * ���ƿ����ļ�����
 * @param size [out] �����ļ�����
 * @return �����ļ����ݣ��������ͷ�)
 */
byte* ControlFile::dupContent(u32 *size) {
	assert(!m_closed);

	MutexGuard guard(&m_lock, __FILE__, __LINE__);
	*size = m_bufSize;
	byte *outBuffer = new byte[m_bufSize];
	memcpy(outBuffer, m_fileBuf, m_bufSize);
	return outBuffer;
}

/** ����һ����ʱ�ļ�·����ע���ڸ���ʱ�ļ������Ҫ�ǵõ���unregisterTempFileע��
 * @param schemaName �����ı��SCHEMA�������ı�ָ��Ҫ������ʱ�ļ��Ĳ����������ı���
 *   ����Ϊ�˶Ա�space.Blog��������������ʱ�ļ���������ı�Ϊspace.Blog
 * @param tableName �����ı������
 * @return ��ʱ�ļ�·����ʹ��new[]�����ڴ棬�ɵ��������ͷ�
 */
char* ControlFile::allocTempFile(const char *schemaName, const char *tableName) {
	assert(!m_closed);

	MutexGuard guard(&m_lock, __FILE__, __LINE__);

	stringstream ss;
	ss << Limits::TEMP_FILE_PREFIX << "_" << schemaName << "_" << tableName << "_" << m_header.m_tempFileSeq++;
	string path = ss.str();

	assert(m_tempFiles.find(string(path)) == m_tempFiles.end());
	m_tempFiles.insert(path);

	m_header.m_numTempFiles++;
	updateFile();

	return System::strdup(path.c_str());
}

/** ע��һ����ʱ�ļ�
 * @param path ��ʱ�ļ�·��
 */
void ControlFile::unregisterTempFile(const char *path) {
	assert(!m_closed);

	MutexGuard guard(&m_lock, __FILE__, __LINE__);
	
	assert(m_tempFiles.find(string(path)) != m_tempFiles.end());
	m_tempFiles.erase(string(path));
	m_header.m_numTempFiles--;
	updateFile();
}

/** �����ʱ�ļ� */
void ControlFile::cleanUpTempFiles() {
	assert(!m_closed);

	MutexGuard guard(&m_lock, __FILE__, __LINE__);

	for (set<string>::iterator it = m_tempFiles.begin(); it != m_tempFiles.end(); it++) {
		string path = *it;
		File(path.c_str()).remove();
	}
	m_tempFiles.clear();
	m_header.m_numTempFiles = 0;
	updateFile();
}

/** ������ʱ��ID
 * @return ��ʱ��ID
 * @throw NtseException ID�Ѿ���������
 */
u16 ControlFile::allocTempTableId() throw(NtseException) {
	assert(!m_closed);

	MutexGuard guard(&m_lock, __FILE__, __LINE__);

	for (size_t i = 0; i < m_tempTableIdUsed.size(); i++) {
		if (!m_tempTableIdUsed[i]) {
			m_tempTableIdUsed[i] = true;
			return (u16)(TableDef::MIN_TEMP_TABLEID + i);
		}
	}
	NTSE_THROW(NTSE_EC_EXCEED_LIMIT, "All temp table ids has been used");
}

/** �ͷ���ʱ��ID��ʹ�����ID���Ա��ظ�����
 * @param tableId һ���Ѿ��������ʱ��ID
 */
void ControlFile::releaseTempTableId(u16 tableId) {
	assert(!m_closed && TableDef::tableIdIsTemp(tableId));

	MutexGuard guard(&m_lock, __FILE__, __LINE__);
	int idx = tableId - TableDef::MIN_TEMP_TABLEID;
	assert(m_tempTableIdUsed[idx]);
	m_tempTableIdUsed[idx] = false;
}

/**
 * �������͡�ʹ��64λ��FVN��ϣ�㷨
 *
 * @param buf ����
 * @param size ���ݴ�С
 * @return �����
 */
u64 ControlFile::checksum(const byte *buf, size_t size) {
	return checksum64(buf, size);
}

/** ���¿����ļ� */
void ControlFile::updateFile() {
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

/** ���л������ļ�����
 * @return ���л����
 */
string ControlFile::serialize() {
	stringstream ss;
	ss.exceptions(ifstream::eofbit | ifstream::failbit | ifstream::badbit);
	
	ss << "-- Warn: do not edit this file by hand unless you're the SUPER MAN with enough skills." << endl;
	ss << "-- Warn: don't change the order of each section." << endl;
	ss << "-- Warn: don't add/remove/edit any comments." << endl;

	ss << "[basic info]" << endl;
	ss << "-- Warn: don't change the order of each variable in this section." << endl;
	ss << "clean_closed:" << (m_header.m_cleanClosed? "true": "false") << endl;
	ss << "checkpoint_lsn:" << m_header.m_checkpointLsn << endl;
	ss << "next_table_id:" << m_header.m_nextTableId << endl;
	ss << "num_tables:" << m_header.m_numTables << endl;
	ss << "num_deleted_tables:" << m_header.m_numDeleted << endl;
	ss << "num_txn_logs:" << m_header.m_numTxnlogs << endl;
	ss << "num_temp_files:" << m_header.m_numTempFiles << endl;
	ss << "temp_file_seq:" << m_header.m_tempFileSeq << endl;
	ss << "rec_storage_version:" << REC_STORAGE_VERSION << endl;
	ss << "idx_storage_version:" << IDX_STORAGE_VERSION << endl;
	
	ss << "[tables]" << endl;
	ss << "-- Format: id:path,flushLsn,hasLob,hasCprsDict,tntFlushLsn." << endl;
	AutoPtr<CFTblInfo *> infos(new CFTblInfo *[m_header.m_numTables], true);
	m_tables.values(infos);
	for (u16 i = 0; i < m_header.m_numTables; i++)
		infos[i]->write(ss);

	ss << "[deleted tables]" << endl;
	AutoPtr<CFTblInfo *> infosDel(new CFTblInfo *[m_header.m_numDeleted], true);
	m_deleted.values(infosDel);
	for (u16 i = 0; i < m_header.m_numDeleted; i++)
		infosDel[i]->write(ss);

	ss << "[temp files]" << endl;
	for (set<string>::iterator it = m_tempFiles.begin(); it != m_tempFiles.end(); it++) {
		ss << *it << endl;
	}

	return ss.str();
}

/** �������һ����
 * @throw NtseException ��һ��
 */
void ControlFile::check() throw (NtseException) {
	if (m_header.m_numTables != m_tables.getSize())
		NTSE_THROW(NTSE_EC_FORMAT_ERROR, "m_numTables %d doesn't equal to m_tables's size %d", m_header.m_numTables, m_tables.getSize());
	if (m_header.m_numTables != m_pathToIds.getSize())
		NTSE_THROW(NTSE_EC_FORMAT_ERROR, "m_numTables %d doesn't equal to m_pathToIds's size %d", m_header.m_numTables, m_pathToIds.getSize());
	if (m_header.m_numDeleted != m_deleted.getSize())
		NTSE_THROW(NTSE_EC_FORMAT_ERROR, "m_numDeleted %d doesn't equal to m_deleted's size %d", m_header.m_numTables, m_deleted.getSize());
	if (m_header.m_numTempFiles != m_tempFiles.size())
		NTSE_THROW(NTSE_EC_FORMAT_ERROR, "m_numTempFiles %d doesn't equal to m_tempFiles's size %d", m_header.m_numTables, m_tempFiles.size());

	// ��֤m_tables�еı�IDû���ظ�
	AutoPtr<u16> tableIds(new u16[m_header.m_numTables], true);
	AutoPtr<CFTblInfo *> infos(new CFTblInfo *[m_header.m_numTables], true);
	m_tables.elements(tableIds, infos);
	for (int i = 0; i < m_header.m_numTables; i++) {
		u16 id = tableIds[i];
		if (id >= m_header.m_nextTableId)
			NTSE_THROW(NTSE_EC_FORMAT_ERROR, "Id %d of table %s is not less than m_nextTableId %d", id, infos[i]->m_path, m_header.m_nextTableId);
		if (id != infos[i]->m_id)
			NTSE_THROW(NTSE_EC_FORMAT_ERROR, "m_tables inconsistent, key is %d, id is %d for table %s",
				id, infos[i]->m_id, infos[i]->m_path);
		for (int j = 0; i < i; j++) {
			if (tableIds[j] == id)
				NTSE_THROW(NTSE_EC_FORMAT_ERROR, "Table %d occurs multiple times", id);
		}
	}

	// ��֤m_pathToIds�еı�·��û���ظ�
	AutoPtr<u16> tableIds2(new u16[m_header.m_numTables], true);
	AutoPtr<char *> pathes2(new char *[m_header.m_numTables], true);
	m_pathToIds.elements(pathes2, tableIds2);
	for (int i = 0; i < m_header.m_numTables; i++) {
		char *path = pathes2[i];
		for (int j = 0; i < i; j++) {
			if (strcmp(pathes2[j], path))
				NTSE_THROW(NTSE_EC_FORMAT_ERROR, "Table %s occurs multiple times", path);
		}
	}

	// ��֤m_tables��m_pathToIdsһ��
	for (int i = 0; i < m_header.m_numTables; i++) {
		u16 id = tableIds[i];
		char *path = infos[i]->m_path;
		u16 id2 = m_pathToIds.get(path);
		if (id2 != id)
			NTSE_THROW(NTSE_EC_FORMAT_ERROR, "m_tables and m_pathToIds inconsitent, id1 %d, id2 %d, path %s", id, id2, path);
	}
}

}
