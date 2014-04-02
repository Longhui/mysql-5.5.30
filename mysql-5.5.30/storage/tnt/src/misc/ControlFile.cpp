/**
 * 控制文件实现
 *
 * @author 汪源(wangyuan@corp.netease.com, wy@163.org)
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

/** 构建一个表信息对象
 * @param id 表ID
 * @param path 表路径
 * @param hasLob 表是否包含大对象
 * @param hasCprsDic 表中是否有压缩全局字典
 */
CFTblInfo::CFTblInfo(u16 id, const char *path, bool hasLob, bool hasCprsDic) {
	m_id = id;
	m_path = System::strdup(path);
	m_hasLob = hasLob;
	m_flushLsn = 0;
	m_hasCprsDict = hasCprsDic;
	m_tntFlushLsn = 0;
}

/** 构建一个空的表信息对象 */
CFTblInfo::CFTblInfo() {
	m_path = NULL;
	m_tntFlushLsn = 0;
}

/** 销毁一个表信息对象 */
CFTblInfo::~CFTblInfo() {
	delete []m_path;
}

/** 设置表路径，在RENAME时调用
 * @param newPath 新的表路径
 */
void CFTblInfo::setPath(const char *newPath) {
	delete []m_path;
	m_path = System::strdup(newPath);
}

/**
 * 设置表是否含有压缩字典文件
 * @param hasCprsDic 是否含有压缩字典文件
 */
void CFTblInfo::setHasCprsDic(bool hasCprsDict) {
	m_hasCprsDict = hasCprsDict;
}

/** 从流中读取一个表信息对象
 * @param s 流
 * @param tnt 按照tnt规则解析，设为true；否则按照ntse规则解析
 * @throw 越界
 * @return 读取的表信息对象
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

/** 写出一个表信息对象到流中
 * @param s 流
 * @throw 写越界
 */
void CFTblInfo::write(ostream &s) throw(NtseException) {
	s << m_id << ':' << m_path << ',' << m_flushLsn << ',' << (m_hasLob? "true": "false") << "," << (m_hasCprsDict ? "true" : "false") << "," << m_tntFlushLsn << endl;
}

/** 构造函数
 * @param file 控制文件
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

/** 初始化对象状态 */
void ControlFile::init() {
	m_fileBuf = NULL;
	m_bufSize = 0;
	memset(&m_header, 0, sizeof(m_header));
	m_file = NULL;
	m_closed = false;
	m_cleanClosed = false;
}

/** 打开控制文件
 * 
 * @param path 控制文件路径
 * @param syslog 系统日志
 * @param tnt 需要打开的是否为tnt的control文件
 * @return 控制文件
 * @throw NtseException 文件不存在，文件格式不正确等
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

	// 解析文件内容
	try {
		char buf[Limits::MAX_PATH_LEN + 1];
		// 头三行--Warn
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
	// 打开后，即将文件中的cleanClosed设为false
	ret->m_header.m_cleanClosed = (u32)false;
	ret->m_fileBuf = (byte *)(char *)fileBuf;
	ret->m_bufSize = (u32)fileSize;
	ret->updateFile();

	file.detatch();
	fileBuf.detatch();
	return ret.detatch();
}

/** 从输入流中读取一行并检查是否与期望的内容相匹配
 * @param ss 输入流
 * @param eol 行是否以换行结束
 * @param delim 当eol为false时指定行结束符，在eol为true时不用
 * @param buf 存储读入数据的内存区
 * @param bufSize buf的大小
 * @param expected 期望的内容
 * @throw NtseException 读入的内容与期望的内容不同
 * @throw stringstream::failure 读取失败
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
 * 创建并初始化控制文件
 *
 * @param path 控制文件路径
 * @param syslog 系统日志
 * @throw NtseException 文件已经存在，无权限无法创建文件等
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
 * 安全关闭控制文件
 *
 * @param cleanUpTemps 是否清理临时文件
 * @param clean 是否安全关闭
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

/** 释放占用的内存 */
void ControlFile::freeMem() {
	// 释放m_tables与m_pathToIds占用的空间
	CFTblInfo **tblInfos = new CFTblInfo *[m_tables.getSize()];
	m_tables.values(tblInfos);
	for (u16 i = 0; i < m_header.m_numTables; i++)
		delete tblInfos[i];
	delete []tblInfos;
	m_tables.clear();
	m_pathToIds.clear();

	// 释放m_deleted占用的空间
	tblInfos = new CFTblInfo *[m_deleted.getSize()];
	m_deleted.values(tblInfos);
	for (u16 i = 0; i < m_header.m_numDeleted; i++)
		delete tblInfos[i];
	delete []tblInfos;
	m_deleted.clear();
}

/**
 * 分配一个可用的表ID。只用于分配普通表（非大对象虚拟表）的ID
 *
 * @return 表ID
 * @throw NtseException 表ID已经用完
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
 * 创建新表时通知控制文件增加表信息
 * @pre 指定ID和路径的表不存在。tableId一定为刚分配出去的ID
 *
 * @param path 表文件路径(相对于basedir)
 * @param tableId 表ID
 * @param hasLob 表是否包含大对象
 * @param hasCprsDic 表是否含有压缩字典文件
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
 * 删除表时通知控制文件删除表信息
 *
 * @param tableId 表ID
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
 * 重命名表
 * @pre 在正常流程而不是恢复中，oldPath应存在，newPath不存在
 *
 * @param oldPath 表原来的存储路径(相对于basedir)
 * @param newPath 表现在的存储路径(相对于basedir)
 */
void ControlFile::renameTable(const char *oldPath, const char *newPath) {
	assert(!m_closed);

	MutexGuard guard(&m_lock, __FILE__, __LINE__);
	u16 tableId = m_pathToIds.get((char *)oldPath);
	if (tableId) {	// 恢复时可能出现控制文件中的路径已经修改的情况
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
 * 修改表是否含有压缩字典文件
 * @pre 上层加了必要的锁
 * @param tableId 表Id
 * @param hasCprsDic 是否含有全局压缩字典文件
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

/** 设置表的flushLsn。设置表的flushLsn必须在对表进行结构性修改之前调用，通过更新表的
 * flushLsn，使得在恢复时跳过之前的日志。这是因为对表进行结构性修改之后，原来的日志不能
 * 再作用于新的表结构，必须跳过。为了保证跳过日志后系统仍可以恢复，在对表进行结构性修改
 * 时，应遵循以下次序:
 * 1. 关闭表或强制写出表中所有脏数据；
 * 2. 设置表的flushLsn（不要直接调用本函数，应调用Database::bumpFlushLsn）
 * 3. 进行结构性修改
 *
 * 表的结构性修改包括任何对表定义（TableDef结构）的修改、RENAME及TRUNCATE操作。
 *
 * @param tableId 表ID
 * @param flushLsn 新的flushLsn，必须比原来的flushLsn大
 */
void ControlFile::bumpFlushLsn(u16 tableId, u64 flushLsn) {
	assert(!m_closed && TableDef::tableIdIsNormal(tableId));

	MutexGuard guard(&m_lock, __FILE__, __LINE__);
	CFTblInfo *tblInfo = m_tables.get(tableId);
	assert(tblInfo->m_flushLsn < flushLsn);
	tblInfo->m_flushLsn = flushLsn;
	updateFile();
}

/** 设置表的flushLsn和tntFlushLsn
 * @param tableId 表Id
 * @param flushLsn ntse的flushLsn，ntse recover不做小于该lsn的redo/undo
 * @param tntFlushLsn tnt的flushLsn，tnt recover不做小于该lsn的redo/undo
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

/** 获得表的flushLsn
 * @param tableId 表ID
 * @return 表的flushLsn
 */
u64 ControlFile::getFlushLsn(u16 tableId) {
	assert(!m_closed && TableDef::tableIdIsNormal(tableId));

	MutexGuard guard(&m_lock, __FILE__, __LINE__);
	return m_tables.get(tableId)->m_flushLsn;
}

/** 获得表的flushLsn
 * @param tableId 表ID
 * @return 表的tntFlushLsn
 */
u64 ControlFile::getTntFlushLsn(u16 tableId) {
	assert(!m_closed && TableDef::tableIdIsNormal(tableId));

	MutexGuard guard(&m_lock, __FILE__, __LINE__);
	return m_tables.get(tableId)->m_tntFlushLsn;
}

/**
 * 得到最后一次检查点LSN
 *
 * @return 最后一次检查点LSN
 */
u64 ControlFile::getCheckpointLSN() {
	assert(!m_closed);
#ifdef TNT_ENGINE
	MutexGuard guard(&m_lock, __FILE__, __LINE__);
#endif
	return m_header.m_checkpointLsn;
}

/**
 * 设置检查点LSN
 *	理论上只被Txnlog::setCheckpointLSN调用
 * @param lsn 检查点LSN
 */
void ControlFile::setCheckpointLSN(u64 lsn) {
	assert(!m_closed);

	MutexGuard guard(&m_lock, __FILE__, __LINE__);

	m_header.m_checkpointLsn = lsn;
	updateFile();
}

/**
 * 是否安全关闭
 * @return 系统是否安全关闭
 */
bool ControlFile::isCleanClosed() {
	assert(!m_closed);
	return m_cleanClosed;
}

/**
 * 设置事务日志文件个数
 * 
 * @param n 事务日志文件个数
 */
void ControlFile::setNumTxnlogs(u32 n) {
	assert(!m_closed);
	
	MutexGuard guard(&m_lock, __FILE__, __LINE__);

	m_header.m_numTxnlogs = n;
	updateFile();
}

/**
 * 返回事务日志文件个数
 *
 * @return 事务日志文件个数
 */
u32 ControlFile::getNumTxnlogs() {
	assert(!m_closed);
	return m_header.m_numTxnlogs;
}

/**
 * 返回表数目
 *
 * @return 数据库中的表个数
 */
u16 ControlFile::getNumTables() {
	assert(!m_closed);
	return m_header.m_numTables;
}

/** 
 * 返回数据库中的所有表的ID
 * @param tableIds OUT，存放tableid的数组，内存空间调用者分配
 * @param maxTabCnt tableIds数组最大元素个数
 * @return 实际填充的tableid个数
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
 * 根据表ID得到表的路径
 *
 * @param tableId 表ID
 * @return 指定的表的路径(相对于basedir)
 */
string ControlFile::getTablePath(u16 tableId) {
//	assert(!m_closed && TableDef::tableIdIsNormal(tableId));   此处要判断是否是小型大对象，如果通过tableId来判断？

	MutexGuard guard(&m_lock, __FILE__, __LINE__);
	CFTblInfo *tblInfo = m_tables.get(tableId);
	if (!tblInfo)
		return "";
	return string(tblInfo->m_path);
}

/**
 * 根据表路径得到表ID
 *
 * @param path 表路径(相对于basedir)，大小写不敏感
 * @return 指定的表的ID，若找不到返回INVALID_TABLEID
 */
u16 ControlFile::getTableId(const char *path) {
	assert(!m_closed);

	MutexGuard guard(&m_lock, __FILE__, __LINE__);
	return m_pathToIds.get((char *)path);
}

/** 获得指定的表是否包含大对象
 * @param tableId 表ID
 * @return 指定的表是否包含大对象
 */
bool ControlFile::hasLob(u16 tableId) {
	assert(!m_closed && TableDef::tableIdIsNormal(tableId));

	MutexGuard guard(&m_lock, __FILE__, __LINE__);
	return m_tables.get(tableId)->m_hasLob;
}

/**
 * 判断指定的表是否含有压缩字典文件
 * @param tableId 表ID
 * @param 指定的表是否含有压缩字典文件
 */
bool ControlFile::hasCprsDict(u16 tableId) {
	assert(!m_closed && TableDef::tableIdIsNormal(tableId));

	MutexGuard guard(&m_lock, __FILE__, __LINE__);
	return m_tables.get(tableId)->m_hasCprsDict;
}

/** 判断指定ID的表是否已经被删除
 *
 * @param tableId 表ID
 * @return 指定ID的表是否已经被删除
 */
bool ControlFile::isDeleted(u16 tableId) {
	assert(!m_closed && TableDef::tableIdIsNormal(tableId));

	MutexGuard guard(&m_lock, __FILE__, __LINE__);
	return m_deleted.get(tableId) != NULL;
}

/**
 * 复制控制文件内容
 * @param size [out] 控制文件长度
 * @return 控制文件内容（调用者释放)
 */
byte* ControlFile::dupContent(u32 *size) {
	assert(!m_closed);

	MutexGuard guard(&m_lock, __FILE__, __LINE__);
	*size = m_bufSize;
	byte *outBuffer = new byte[m_bufSize];
	memcpy(outBuffer, m_fileBuf, m_bufSize);
	return outBuffer;
}

/** 分配一个临时文件路径。注意在该临时文件用完后要记得调用unregisterTempFile注销
 * @param schemaName 关联的表的SCHEMA，关联的表指需要创建临时文件的操作所操作的表，如
 *   若是为了对表space.Blog建索引而建立临时文件，则关联的表即为space.Blog
 * @param tableName 关联的表的名称
 * @return 临时文件路径，使用new[]分配内存，由调用者来释放
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

/** 注销一个临时文件
 * @param path 临时文件路径
 */
void ControlFile::unregisterTempFile(const char *path) {
	assert(!m_closed);

	MutexGuard guard(&m_lock, __FILE__, __LINE__);
	
	assert(m_tempFiles.find(string(path)) != m_tempFiles.end());
	m_tempFiles.erase(string(path));
	m_header.m_numTempFiles--;
	updateFile();
}

/** 清除临时文件 */
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

/** 分配临时表ID
 * @return 临时表ID
 * @throw NtseException ID已经分配完了
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

/** 释放临时表ID，使得这个ID可以被重复利用
 * @param tableId 一个已经分配的临时表ID
 */
void ControlFile::releaseTempTableId(u16 tableId) {
	assert(!m_closed && TableDef::tableIdIsTemp(tableId));

	MutexGuard guard(&m_lock, __FILE__, __LINE__);
	int idx = tableId - TableDef::MIN_TEMP_TABLEID;
	assert(m_tempTableIdUsed[idx]);
	m_tempTableIdUsed[idx] = false;
}

/**
 * 计算检验和。使用64位的FVN哈希算法
 *
 * @param buf 数据
 * @param size 数据大小
 * @return 检验和
 */
u64 ControlFile::checksum(const byte *buf, size_t size) {
	return checksum64(buf, size);
}

/** 更新控制文件 */
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

/** 序列化控制文件内容
 * @return 序列化结果
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

/** 检查数据一致性
 * @throw NtseException 不一致
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

	// 验证m_tables中的表ID没有重复
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

	// 验证m_pathToIds中的表路径没有重复
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

	// 验证m_tables与m_pathToIds一致
	for (int i = 0; i < m_header.m_numTables; i++) {
		u16 id = tableIds[i];
		char *path = infos[i]->m_path;
		u16 id2 = m_pathToIds.get(path);
		if (id2 != id)
			NTSE_THROW(NTSE_EC_FORMAT_ERROR, "m_tables and m_pathToIds inconsitent, id1 %d, id2 %d, path %s", id, id2, path);
	}
}

}
