/**
 * 表管理操作实现
 *
 * @author 汪源(wangyuan@corp.netease.com, wy@163.org)
 */
#include <string>
#include <stdio.h>
#include "api/Table.h"
#include "util/System.h"
#include "api/Database.h"
#include "heap/Heap.h"
#include "btree/Index.h"
#include "misc/Session.h"
#include "mms/Mms.h"
#include "misc/Record.h"
#include "api/Transaction.h"
#include "util/File.h"
#include "misc/Trace.h"
#include "misc/RecordHelper.h"
#include "misc/MemCtx.h"
#include "misc/Profile.h"
#include "misc/Callbacks.h"
#include "api/TblMnt.h"
#include "compress/RCMSampleTbl.h"

#ifdef TNT_ENGINE
#include "trx/TNTTransaction.h"
#endif

/*
关于内存分配上下文所分配内存生存周期的使用规范:

NTSE中每个会话拥有两个内存分配上下文，一是通用内存分配上下文，即
getMemoryContext的返回值；二是专用于存储大对象数据的内存分配上下文，即
getLobContext的返回值。这两类内存分配上下文所分配内存生存周期的使用规范
如下：

1. tableScan/indexScan/posScan开始扫描时在通用内存分配上下文中分配的内存，
   在结束扫描后不会被释放；
2. updateCurrent/deleteCurrent/insert/getNext操作过程中在通用内存分配上下文
   中分配的内存，在操作结束后自动释放；
3. 大对象内存分配上下文专用于存储getNext操作中所读取的大对象的值，不用于
   IUD操作中的大对象模块相关处理。getNext操作中在大对象内存分配上下文中
   分配的内存，在下一次getNext或下一次用同一个会话再次开始扫描时释放，但
   endScan时并不会释放，这是由于MySQL上层是先调用endScan再来读大对象的内容；
4. 所有内存分配上下文在会话释放时都会自动重置；
*/

using namespace std;

namespace ntse {

/** 构造一个表对象
 *
 * @param db 数据库
 * @param path 表路径，相对于basedir，不含后缀
 * @param records 记录管理器
 * @param tableDef 表定义，直接引用
 * @param hasCprsDict 是否含有压缩全局字典
 */
Table::Table(Database *db, const char *path, Records *records, TableDef *tableDef, bool hasCprsDict): m_metaLock(db->getConfig()->m_maxSessions, tableDef->m_name, __FILE__, __LINE__),
	m_tblLock(db->getConfig()->m_maxSessions, tableDef->m_name, __FILE__, __LINE__) {
	m_db = db;
	m_path = path;
	m_records = records;
	m_tableDef = tableDef;
	m_indice = NULL;
	m_estimateRows = 0;
	m_tableDefWithAddingIndice = m_tableDef;
	memset(m_rlockCnts, 0, sizeof(m_rlockCnts));

	m_mmsCallback = new MMSBinlogCallBack(&binlogMmsUpdate, this);
	m_mysqlTmpTable = false;
	m_hasCprsDict = hasCprsDict;
	
	m_openTablesLink.set(this);
}

/** 创建表
 * 若失败则由本函数新建的文件已经被删除。
 *
 * @param db 数据库
 * @param session 会话对象
 * @param path 文件路径(相对于basedir)，不包含后缀
 * @param tableDef 表定义
 * @throw NtseException 文件操作失败，表定义不符合要求等
 */
void Table::create(Database *db, Session *session, const char *path, TableDef *tableDef) throw(NtseException) {
	ftrace(ts.ddl, tout << db << session << path << (void *)tableDef);
	tableDef->check();

	bool logging = session->isLogging();
	session->disableLogging();

	string basePath = string(db->getConfig()->m_basedir) + NTSE_PATH_SEP + path;
	string tblDefPath = basePath + Limits::NAME_TBLDEF_EXT;
	bool tblCreated = false, recCreated = false, indexCreated = false;
	
	try {
		tableDef->writeFile(tblDefPath.c_str());
		tblCreated = true;

		Records::create(db, basePath.c_str(), tableDef);
		recCreated = true;

		string fullPath = basePath + Limits::NAME_IDX_EXT;
		DrsIndice::create(fullPath.c_str(), tableDef);
		indexCreated = true;

		// 在表创建时Records已关闭，无法获得lobStorage管理器，因此可以暂时不传入，在后面打开时会再次传入正确的大对象管理器指针
		DrsIndice *indice = DrsIndice::open(db, session, fullPath.c_str(), tableDef, NULL);
		for (u16 idx = 0; idx < tableDef->m_numIndice; idx++) {
			indice->createIndexPhaseOne(session, tableDef->m_indice[idx], tableDef, NULL);
			indice->createIndexPhaseTwo(tableDef->m_indice[idx]);
		}
		indice->close(session, true);
		delete indice;
	} catch (NtseException &e) {
		if (tblCreated) {
			TableDef::drop(tblDefPath.c_str());
		}
		if (recCreated)
			Records::drop(basePath.c_str());
		if (indexCreated) {
			string fullPath = basePath + Limits::NAME_IDX_EXT;
			DrsIndice::drop(fullPath.c_str());
		}
		if (logging)
			session->enableLogging();
		throw e;
	}

	if (logging)
		session->enableLogging();
}

/** 删除表
 *
 * @param db 数据库
 * @param path 文件路径(相对于basedir)，不包含后缀
 * @throw NtseException 文件操作失败等
 */
void Table::drop(Database *db, const char *path) throw(NtseException) {
	ftrace(ts.ddl, tout << path);

	drop(db->getConfig()->m_basedir, path);
	/*string basePath = string(db->getConfig()->m_basedir) + NTSE_PATH_SEP + path;
	string tblDefPath = basePath + Limits::NAME_TBLDEF_EXT;
	TableDef::drop(tblDefPath.c_str());

	Records::drop(basePath.c_str());
	string fullPath = basePath + Limits::NAME_IDX_EXT;
	DrsIndice::drop(fullPath.c_str());*/
}

/** 删除表
 *
 * @param basedir 基准路径
 * @param path 文件路径(相对于basedir)，不包含后缀
 * @throw NtseException 文件操作失败等
 */
void Table::drop(const char *baseDir, const char *path) throw(NtseException) {
	ftrace(ts.ddl, tout << path);

	string basePath = string(baseDir) + NTSE_PATH_SEP + path;
	string tblDefPath = basePath + Limits::NAME_TBLDEF_EXT;
	TableDef::drop(tblDefPath.c_str());

	Records::drop(basePath.c_str());
	string fullPath = basePath + Limits::NAME_IDX_EXT;
	DrsIndice::drop(fullPath.c_str());
}

/** 打开表
 *
 * @param db 数据库
 * @param session 会话
 * @param path 文件路径，相对于basedir，不包含后缀
 * @param hasCprsDict 是否含有全局字典文件
 * @throw NtseException 文件不存在，格式不正确等
 */
Table* Table::open(Database *db, Session *session, const char *path, bool hasCprsDict/*=false*/) throw(NtseException) {
	ftrace(ts.ddl, tout << db << session << path);

	string basePath(string(db->getConfig()->m_basedir) + NTSE_PATH_SEP + path);
	
	string tblDefPath = basePath + Limits::NAME_TBLDEF_EXT;
	TableDef *tableDef = TableDef::open(tblDefPath.c_str());

	Records *records = NULL;
	try {
		records = Records::open(db, session, basePath.c_str(), tableDef, hasCprsDict);
	} catch (NtseException &e) {
		delete tableDef;
		throw e;
	}

	Table *ret = new Table(db, System::strdup(path), records, tableDef, hasCprsDict);
	records->setMmsCallback(ret->m_mmsCallback);
	try {
		string idxPath = basePath + Limits::NAME_IDX_EXT;
		ret->m_indice = DrsIndice::open(db, session, idxPath.c_str(), ret->m_tableDef, records->getLobStorage());
		ret->m_tableDef->m_version = System::microTime();
	} catch (NtseException &e) {
		ret->close(session, false);
		delete ret;
		throw e;
	}
	return ret;
}

/** 关闭表
 *
 * @param session 会话
 * @param flushDirty 是否写出脏数据
 * @param closeComponents 是否只是关闭各个底层组件
 */
void Table::close(Session *session, bool flushDirty, bool closeComponents) {
	ftrace(ts.ddl, tout << session << flushDirty);

	if (m_indice) {
		m_indice->close(session, flushDirty);
		delete m_indice;
		m_indice = NULL;
	}

	// 首先要刷MMS中可能的更新缓存日志，然后调用关闭表对应的回调，这一调用
	// 需要在关闭Records之前，否则得不到m_tableDef
	if (m_records && flushDirty) {
		m_records->flush(session, false, true, false);
	}

	if (m_mmsCallback) {
		if (!TableDef::tableIdIsTemp(m_tableDef->m_id) && !m_mysqlTmpTable)
			m_db->getNTSECallbackManager()->callback(TABLE_CLOSE, m_tableDef, NULL, NULL, NULL);
		delete m_mmsCallback;
		m_mmsCallback = NULL;
	}

	if (m_records) {
		m_records->close(session, flushDirty);
		delete m_records;
		m_records = NULL;
	}

	string tblDefPath(string(m_db->getConfig()->m_basedir) + NTSE_PATH_SEP + m_path + Limits::NAME_TBLDEF_EXT);
	if (flushDirty) {
		m_tableDef->writeFile(tblDefPath.c_str());
	}

	if (m_tableDef != NULL) {
		delete m_tableDef;
		m_tableDef = NULL;
	}
	delete []m_path;
	m_path = NULL;
	if (!closeComponents && getMetaLock(session) != IL_NO)
		session->removeLock(&m_metaLock);
}

/** 检查是否能够进行重命名操作
 *
 * @param db 数据库
 * @param oldPath 原路径(相对于basedir)，不包含后缀名
 * @param newPath 新路径(相对于basedir)，不包含后缀名
 * @param hasLob 表中是否包含大对象
 * @throw NtseException 不能进行重命名时通过此异常汇报原因
 */
void Table::checkRename(Database *db, const char *oldPath, const char *newPath, bool hasLob, bool hasCprsDict/*=false*/) throw(NtseException) {
	ftrace(ts.ddl || ts.recv, tout << db << oldPath << newPath << hasLob);

	string basePath1(string(db->getConfig()->m_basedir) + NTSE_PATH_SEP + oldPath);
	string basePath2(string(db->getConfig()->m_basedir) + NTSE_PATH_SEP + newPath);
	const char **exts = Limits::EXTS;

	int numFiles = Limits::EXTNUM;
	for (int i = 0; i < numFiles; i++) {
		if (!hasCprsDict && !System::stricmp(Limits::NAME_GLBL_DIC_EXT, exts[i])) {//如果没有字典文件，则跳过
			continue;
		}
		if (!hasLob && (!System::stricmp(Limits::NAME_SOBH_EXT, exts[i]) 
			|| !System::stricmp(Limits::NAME_SOBH_TBLDEF_EXT, exts[i]) 
			|| !System::stricmp(Limits::NAME_LOBI_EXT, exts[i]) 
			|| !System::stricmp(Limits::NAME_LOBD_EXT, exts[i]))) {
				continue;
		}
		// 验证目标文件不存在且有权限创建
		string newPath = basePath2 + exts[i];
		bool exist;
		u64 code = File(newPath.c_str()).isExist(&exist);
		if (code != File::E_NO_ERROR)
			NTSE_THROW(code, "Can not get existence of file %s", newPath.c_str());
		code = File(newPath.c_str()).create(db->getConfig()->m_directIo, true);
		if (code != File::E_NO_ERROR)
			NTSE_THROW(code, "Can not create file %s", newPath.c_str());
		File(newPath.c_str()).remove();
	}
}

/** 重命名表。
 * @pre 表已经被关闭，并且已经调用过checkRename进行检查
 *
 * @param db 数据库
 * @param session 会话
 * @param oldPath 原路径(相对于basedir)，不包含后缀名
 * @param newPath 新路径(相对于basedir)，不包含后缀名
 * @param hasLob 表中是否包含大对象
 * @param redo 是否是重做RENAME操作。REDO时可能出现部分文件已经重命名的情况
 */
void Table::rename(Database *db, Session *session, const char *oldPath, const char *newPath, bool hasLob, 
				   bool redo, bool hasCprsDict /*=false*/) {
	ftrace(ts.ddl || ts.recv, tout << db << session << oldPath << newPath << hasLob << redo);

	UNREFERENCED_PARAMETER(session);

	string basePath1(string(db->getConfig()->m_basedir) + NTSE_PATH_SEP + oldPath);
	string basePath2(string(db->getConfig()->m_basedir) + NTSE_PATH_SEP + newPath);
	const char **exts = Limits::EXTS;

	int numFiles = Limits::EXTNUM;
	for (int i = 0; i < numFiles; i++) {
		if (!hasCprsDict && !System::stricmp(Limits::NAME_GLBL_DIC_EXT, exts[i])) {//如果没有字典文件，则跳过
			continue;
		}
		if (!hasLob && (!System::stricmp(Limits::NAME_SOBH_EXT, exts[i]) 
			|| !System::stricmp(Limits::NAME_SOBH_TBLDEF_EXT, exts[i]) 
			|| !System::stricmp(Limits::NAME_LOBI_EXT, exts[i]) 
			|| !System::stricmp(Limits::NAME_LOBD_EXT, exts[i]))) {
			continue;
		}
		string fullPath1, fullPath2;
		fullPath1 = basePath1 + exts[i];
		fullPath2 = basePath2 + exts[i];
		File file(fullPath1.c_str());
		if (redo && (!File::isExist(fullPath1.c_str()) && File::isExist(fullPath2.c_str()))) {
			nftrace(ts.recv, tout << "Skip moving of already moved file " << fullPath1.c_str()); 
			continue;
		}
		u64 code = file.move(fullPath2.c_str(), false);
		if (code != File::E_NO_ERROR) {
			NTSE_THROW(NTSE_EC_FILE_FAIL, "%s move to %s error", fullPath1.c_str(), fullPath2.c_str());
		}
	}

	string fullPath = basePath2 + Limits::NAME_TBLDEF_EXT;
	TableDef *tableDef = NULL;
	try {
		tableDef = TableDef::open(fullPath.c_str());
		if (tableDef->m_schemaName != NULL) {
			delete[] tableDef->m_schemaName;
			tableDef->m_schemaName = NULL;
		}

		if (tableDef->m_name != NULL) {
			delete[] tableDef->m_name;
			tableDef->m_name = NULL;
		}
		getSchemaTableFromPath(basePath2.c_str(), &tableDef->m_schemaName, &tableDef->m_name);
		tableDef->writeFile(fullPath.c_str());
	} catch (NtseException& e) {
		delete tableDef;
		throw e;
	}
	delete tableDef;
}

/** TRUNCATE一张表
 *
 * @param session        会话
 * @param tblLock        是否加表锁
 * @param newHasDic      OUT 新表是否含有全局字典文件
 * @param isTruncateOper 是否是truncate操作
 * @throw NtseException  加表锁超时，文件操作出错等
 */
void Table::truncate(Session *session, bool tblLock /*=true*/, bool *newHasDic /*=NULL*/, bool isTruncateOper /*=true*/) throw(NtseException) {
	ftrace(ts.ddl, tout << session);
	assert(tblLock || (getMetaLock(session) == IL_X && (session->getTrans() != NULL || getLock(session) == IL_X)));
	if (tblLock) {
		lockMeta(session, IL_X, m_db->getConfig()->m_tlTimeout * 1000, __FILE__, __LINE__);
		try {
			lock(session, IL_X, m_db->getConfig()->m_tlTimeout * 1000, __FILE__, __LINE__);
		} catch (NtseException &e) {
			unlockMeta(session, IL_X);
			throw e;
		}
	} 

	TableDef tableDefBak(m_tableDef);
	string pathBak(m_path);

	close(session, false, true);

	// TODO 应修改为先建立新表再删除旧表
	
	m_db->bumpFlushLsn(session, tableDefBak.m_id, false);

	m_db->getTxnlog()->flush(writeTruncateLog(session, &tableDefBak, m_hasCprsDict, isTruncateOper));

	string basePath = string(m_db->getConfig()->m_basedir) + NTSE_PATH_SEP + pathBak;
	string dictPath = string(basePath + Limits::NAME_GLBL_DIC_EXT);
	string tmpDictPath = string(basePath + Limits::NAME_TEMP_GLBL_DIC_EXT);
	bool copyDict = false;
	if (newHasDic != NULL)
		(*newHasDic) = (!isTruncateOper || tableDefBak.m_isCompressedTbl) && m_hasCprsDict;
	try {
		if (newHasDic != NULL && *newHasDic) {
			//先拷贝字典文件到临时字典文件
			u64 errCode = File::copyFile(tmpDictPath.c_str(), dictPath.c_str(), true);
			if (File::E_NO_ERROR != File::getNtseError(errCode)) {
				NTSE_THROW(NTSE_EC_FILE_FAIL, "copy compressed dictionary failed when truncate table.");
			}
			copyDict = true;
		}

#ifdef NTSE_KEYVALUE_SERVER
		u64 saveVersion = 0 ;
		if (m_tableDef != NULL) {
			saveVersion = m_tableDef->m_version;
		}
#endif
		drop(m_db, pathBak.c_str());
		SYNCHERE(SP_TBL_TRUNCATE_AFTER_DROP);

		create(m_db, session, pathBak.c_str(), &tableDefBak);

		//拷贝临时字典文件到新表
		if (copyDict) {
			u64 errCode = File::copyFile(dictPath.c_str(), tmpDictPath.c_str(), true);
			if (File::E_NO_ERROR != File::getNtseError(errCode)) {
				NTSE_THROW(NTSE_EC_FILE_FAIL, "copy compressed dictionary failed when truncate table.");
			}
		}

		Table *newTable = open(m_db, session, pathBak.c_str(), newHasDic != NULL ? *newHasDic : false);
		replaceComponents(newTable);

#ifdef NTSE_KEYVALUE_SERVER
		if (m_tableDef != NULL) {
			m_tableDef->m_version = saveVersion;
		}
#endif
		delete newTable;
	} catch (NtseException &e) {
		nftrace(ts.ddl, tout << "truncate failed: " << e.getMessage());
		if (copyDict) {
			File tmpDictFile(tmpDictPath.c_str());
			u64 errCode = tmpDictFile.remove();
			UNREFERENCED_PARAMETER(errCode);
			assert(File::E_NO_ERROR == File::getNtseError(errCode));
		}
		if (tblLock) {
			unlock(session, IL_X);
			unlockMeta(session, IL_X);
		}
		throw e;
	}

	//删除临时字典文件
	if (copyDict) {
		File tmpDictFile(tmpDictPath.c_str());
		u64 errCode = tmpDictFile.remove();
		u32 ntseErrCode = File::getNtseError(errCode);
		UNREFERENCED_PARAMETER(ntseErrCode);
		assert(File::E_NO_ERROR == ntseErrCode);
	}

	if (tblLock) {
		unlock(session, IL_X);
		unlockMeta(session, IL_X);
	}	
	m_estimateRows = 0;
}

/** 重做TRUNCATE操作
 * @param db 数据库
 * @param session 会话
 * @param log 日志内容
 * @param path 表路径
 * @param newHasDict OUT 新表是否含有字典
 * @throw NtseException 文件操作出错等
 */
void Table::redoTrunate(Database *db, Session *session, const LogEntry *log, const char *path, bool *newHasDict) throw(NtseException) {
	bool hasDict;
	bool isTruncateOper;
	TableDef *tableDefBak = NULL;
	parseTruncateLog(log, &tableDefBak, &hasDict, &isTruncateOper);

	string basePath = string(db->getConfig()->m_basedir) + NTSE_PATH_SEP + path;
	string dictPath = string(basePath + Limits::NAME_GLBL_DIC_EXT);
	string tmpDictPath = string(basePath + Limits::NAME_TEMP_GLBL_DIC_EXT);
	File oldDictFile(dictPath.c_str());
	File tmpDictFile(tmpDictPath.c_str());
	*newHasDict = (isTruncateOper || tableDefBak->m_isCompressedTbl) && hasDict;

	if (*newHasDict) {
		if (File::isExist(dictPath.c_str())) {
			//先拷贝字典文件到临时字典文件
			u64 errCode = File::copyFile(tmpDictPath.c_str(), dictPath.c_str(), true);
			if (File::E_NO_ERROR != File::getNtseError(errCode)) {
				delete tableDefBak;
				NTSE_THROW(NTSE_EC_FILE_FAIL, "copy compressed dictionary failed when truncate table.");
			}
		} else if (!File::isExist(tmpDictPath.c_str())) {
			delete tableDefBak;
			return;//已经完成替换，只需要修改控制文件
		}
	}

	try {
		drop(db, path);

		create(db, session, path, tableDefBak);

		//拷贝临时字典文件到新表
		if (*newHasDict) {
			assert(File::isExist(tmpDictPath.c_str()));
			u64 errCode = File::copyFile(dictPath.c_str(), tmpDictPath.c_str(), true);
			if (File::E_NO_ERROR != File::getNtseError(errCode)) {
				NTSE_THROW(NTSE_EC_FILE_FAIL, "copy compressed dictionary failed when truncate table.");
			}
		}
	} catch (NtseException &e) {
		delete tableDefBak;
		throw e;
	}
	if (*newHasDict) {
		//删除临时字典文件
		u64 errCode = tmpDictFile.remove();
		UNREFERENCED_PARAMETER(errCode);
		assert(File::E_NOT_EXIST == File::getNtseError(errCode) || File::E_NO_ERROR == File::getNtseError(errCode));
	}
	delete tableDefBak;
}

/** 判断该表上是否存在未完成的在线索引
 * @param idxInfo 如果存在未完成的在线索引，返回相关索引的描述信息
 * return 存在未完成的在线索引返回true，否则返回false
 */
bool Table::hasOnlineIndex(string *idxInfo) {
	bool exist = false;
	//遍历当前表上的索引定义找出所有未完成的在线索引
	for (int i = 0; i < m_tableDef->m_numIndice; i++) {
		const IndexDef *indexDef = m_tableDef->getIndexDef(i);
		if (indexDef->m_online) {
			if (!exist) {
				exist = true;
			}
			*idxInfo += indexDef->m_name;
			*idxInfo += "(";
			for (int j = 0; j < indexDef->m_numCols; j++) {
				const ColumnDef *columnDef = m_tableDef->getColumnDef(indexDef->m_columns[j]);
				*idxInfo += columnDef->m_name;
				if (j < indexDef->m_numCols - 1) {
					*idxInfo += ",";
				}
			}
			*idxInfo += "), ";
		}
	}
	if (idxInfo->size() > 0) {
		idxInfo->erase(idxInfo->size() - 2, 2);
	}

	return exist;
}

/** 判断添加索引是否为已完成的online index
 * @param indexDefs 索引定义信息
 * @param numIndice 索引定义个数
 * return 如果全是已完成的online index返回true，否则返回false
 */
bool Table::isDoneOnlineIndex(const IndexDef **indexDefs, u16 numIndice) {
	bool done = true;
	for (uint i = 0; i < numIndice; i++) {
		const IndexDef *indexDef = m_tableDef->getIndexDef(indexDefs[i]->m_name);
		if (indexDef != NULL && indexDef->m_online && indexDefs[i]->m_numCols == indexDef->m_numCols) {
			for (uint j = 0; j < indexDef->m_numCols; j++) {
				if (!(indexDefs[i]->m_columns[j] == indexDef->m_columns[j] && 
					indexDefs[i]->m_prefixLens[j] == indexDef->m_prefixLens[j])) {	// 判断是否是online索引必须同时判断列定义和前缀长度
					done = false;
					break;
				}
			}
		} else {
			done = false;
			break;
		}
	}
	return done;
}

/** 判断在线建索引顺序是否正确
 * @param indexDefs 在线索引信息
 * @param numIndice 在线索引个数
 * return 在线索引顺序正确返回true，否则返回false
 */
bool Table::isOnlineIndexOrder(const IndexDef **indexDefs, u16 numIndice) {
	int lastIndexNo = m_tableDef->getIndexNo(indexDefs[0]->m_name);
	if (lastIndexNo) {
		--lastIndexNo;
		//如果在线索引定义存在前一项的索引定义，必须确保前一个索引为非online
		const IndexDef *lastIndexDef = m_tableDef->getIndexDef(lastIndexNo);
		if (lastIndexDef->m_online) {
			return false;
		}
	}

	for (u16 i = 0; i < numIndice - 1; i++) {
		//确保在线索引定义的第i项索引定义Num跟第i+1项索引定义Num是紧挨的
		if (m_tableDef->getIndexNo(indexDefs[i]->m_name) + 1 != m_tableDef->getIndexNo(indexDefs[i + 1]->m_name)) {
			return false;
		}
	}

	return true;
}

/** 创建索引
 *
 * @param session 会话对象
 * @param numIndice 要增加的索引个数，为online为false只能为1
 * @param indexDefs 新索引定义
 * @throw NtseException 加表锁超时，索引无法支持，增加唯一性索引时有重复键值等
 */
void Table::addIndex(Session *session, u16 numIndice, const IndexDef **indexDefs) throw(NtseException) {
	ftrace(ts.ddl, tout << session << m_tableDef->m_name << numIndice);
	//1.当前创建索引是ntse创建索引
	if (indexDefs[0]->m_online) {
		TblMntAlterIndex alter(this, numIndice, indexDefs, 0, NULL);
		alter.alterTable(session);
		return;
	}

	ILMode oldMetaLock = getMetaLock(session);
	upgradeMetaLock(session, oldMetaLock, IL_U, m_db->getConfig()->m_tlTimeout * 1000, __FILE__, __LINE__);

	//2.当前创建索引是mysql创建索引
	string idxInfo;

	//2.1 当前表上含有未完成的在线索引
	if (hasOnlineIndex(&idxInfo)) {
		if (isDoneOnlineIndex(indexDefs, numIndice)) {
			//判断在线建索引顺序是否正确
			if (!isOnlineIndexOrder(indexDefs, numIndice)) {
				downgradeMetaLock(session, getMetaLock(session), oldMetaLock, __FILE__, __LINE__);
				NTSE_THROW(NTSE_EC_GENERIC, "Add online index order was wrong, %s.", idxInfo.c_str());
			}
			//完成快速创建在线索引的最后步骤
			dualFlushAndBump(session, m_db->getConfig()->m_tlTimeout * 1000); //这个函数会将metaLock从IL_U升为IL_X
			for (uint i = 0; i < numIndice; i++) {
				m_tableDef->getIndexDef(indexDefs[i]->m_name)->m_online = false;
			}

			writeTableDef();
			downgradeMetaLock(session, getMetaLock(session), oldMetaLock, __FILE__, __LINE__);
		} else {
			downgradeMetaLock(session, getMetaLock(session), oldMetaLock, __FILE__, __LINE__);
			NTSE_THROW(NTSE_EC_GENERIC, "Add online index was not completely, %s.", idxInfo.c_str());
		}

		return;
	}

	//2.2 当前表上没有未完成的在线索引
	if (numIndice == 1) {
		ILMode oldLock = getLock(session);
		// 创建索引期间禁止对表进行写操作
		try {
			upgradeLock(session, oldLock, IL_S, m_db->getConfig()->m_tlTimeout * 1000, __FILE__, __LINE__);
		} catch (NtseException &e) {
			downgradeMetaLock(session, getMetaLock(session), oldMetaLock, __FILE__, __LINE__);
			throw e;
		}

		const IndexDef *indexDef = indexDefs[0];
		try {
			if (indexDef->m_primaryKey && m_tableDef->m_pkey) {
				NTSE_THROW(NTSE_EC_EXCEED_LIMIT, "Can not add primary key index when table already has a primary key.");
			}
			indexDef->check(m_tableDef);
		} catch (NtseException &e) {
			downgradeLock(session, getLock(session), oldLock, __FILE__, __LINE__);
			downgradeMetaLock(session, getMetaLock(session), oldMetaLock, __FILE__, __LINE__);
			throw e;
		}

		flush(session);
		m_db->bumpFlushLsn(session, m_tableDef->m_id);

		session->startTransaction(TXN_ADD_INDEX, m_tableDef->m_id, true);
		writeAddIndexLog(session, indexDef);
		try {
			m_indice->createIndexPhaseOne(session, indexDef, m_tableDef, m_records->getHeap());
		} catch (NtseException &e) {
			nftrace(ts.ddl, tout << "Failed in createIndexPhaseOne");
			session->endTransaction(false, true);
			// 保证ADD_INDEX日志被写出。TODO: 为什么?
			m_db->getTxnlog()->flush(session->getLastLsn());
			downgradeLock(session, getLock(session), oldLock, __FILE__, __LINE__);
			downgradeMetaLock(session, getMetaLock(session), oldMetaLock, __FILE__, __LINE__);
			throw e;
		}

		session->setTxnDurableLsn(session->getLastLsn());

		// 短期禁止对表进行读操作，完成表定义修改
		// 由MySQL上层保证不会发生死锁
		upgradeMetaLock(session, getMetaLock(session), IL_X, -1, __FILE__, __LINE__);
		upgradeLock(session, getLock(session), IL_X, -1, __FILE__, __LINE__);

		m_indice->createIndexPhaseTwo(indexDef);

		m_tableDef->addIndex(indexDef);
		writeTableDef();

		session->endTransaction(true, true);

		nftrace(ts.ddl, tout << "add index succeed");
		
		downgradeLock(session, getLock(session), oldLock, __FILE__, __LINE__);
		downgradeMetaLock(session, getMetaLock(session), oldMetaLock, __FILE__, __LINE__);
	} else {
		downgradeMetaLock(session, getMetaLock(session), oldMetaLock, __FILE__, __LINE__);
		NTSE_THROW(NTSE_EC_NOT_SUPPORT, "Can not create multiple indices, you can use online's create indices.");
	}
}

/** Inplace创建索引第一阶段，在Meta Lock U锁的保护下，读取堆数据，创建索引，此步骤较为耗时
 *
 * @param session 会话对象
 * @param numIndice 要增加的索引个数，Inplace时只能为1(非Online)
 * @param indexDefs 新索引定义
 * @throw NtseException 加表锁超时，索引无法支持，增加唯一性索引时有重复键值等
 */
void Table::addIndexPhaseOne(Session *session, u16 numIndice, const IndexDef **indexDefs) throw (NtseException) {
	// Inplace下创建索引，索引数量只能为1
	if (numIndice > 1) {
		NTSE_THROW(NTSE_EC_NOT_SUPPORT, "Can not create multiple indices, you can use online's create indices.");
	}

	assert(numIndice == 1);

	const IndexDef *indexDef = indexDefs[0];
	try {
		if (indexDef->m_primaryKey && m_tableDef->m_pkey) {
			NTSE_THROW(NTSE_EC_EXCEED_LIMIT, "Can not add primary key index when table already has a primary key.");
		}
		indexDef->check(m_tableDef);
	} catch (NtseException &e) {
		throw e;
	}

	flush(session);
	m_db->bumpFlushLsn(session, m_tableDef->m_id);

	session->startTransaction(TXN_ADD_INDEX, m_tableDef->m_id, true);
	writeAddIndexLog(session, indexDef);
	try {
		m_indice->createIndexPhaseOne(session, indexDef, m_tableDef, m_records->getHeap());
	} catch (NtseException &e) {
		nftrace(ts.ddl, tout << "Failed in createIndexPhaseOne");
		session->endTransaction(false, true);
		// 保证ADD_INDEX日志被写出。TODO: 为什么?
		m_db->getTxnlog()->flush(session->getLastLsn());
		throw e;
	}

	session->setTxnDurableLsn(session->getLastLsn());
}

/** Inplace创建索引第二阶段，在Meta Lock X锁的保护下，修改TableDef信息，此步骤很快
 *
 * @param session 会话对象
 * @param numIndice 要增加的索引个数，Inplace时只能为1(非Online)
 * @param indexDefs 新索引定义
 * @throw NtseException 加表锁超时，索引无法支持，增加唯一性索引时有重复键值等
 */
void Table::addIndexPhaseTwo(Session *session, u16 numIndice, const IndexDef **indexDefs) throw(NtseException) {
	assert(numIndice == 1);

	const IndexDef *indexDef = indexDefs[0];

	m_indice->createIndexPhaseTwo(indexDef);

	m_tableDef->addIndex(indexDef);
	writeTableDef();

	// session对应的NTSE微事务，在索引创建的第一阶段开始，此处提交
	session->endTransaction(true, true);

	nftrace(ts.ddl, tout << "add index succeed");
}

/** 将tabledef的写入定义文件
 *
 */
void Table::writeTableDef() {
	string tblDefPath(string(m_db->getConfig()->m_basedir) + NTSE_PATH_SEP + m_path + Limits::NAME_TBLDEF_EXT);
	try {
		m_tableDef->writeFile(tblDefPath.c_str());
	} catch (NtseException &e) {
		m_db->getSyslog()->fopPanic(e.getErrorCode(), e.getMessage());
	}
}

/** 删除索引
 *
 * @param session 会话对象
 * @param idx 要删除的是第几个索引
 * @throw NtseException 加表锁超时
 */
void Table::dropIndex(Session *session, uint idx) throw(NtseException) {
	ftrace(ts.ddl, tout << session << m_tableDef->m_name << idx);
	assert(idx < m_tableDef->m_numIndice);

	ILMode oldMetaLock = getMetaLock(session);
	ILMode oldLock = getLock(session);
	
	upgradeMetaLock(session, oldMetaLock, IL_U, m_db->getConfig()->m_tlTimeout * 1000, __FILE__, __LINE__);	// 可能超时，直接抛出异常

	// 由于binlog需要记录主键，当表没有主键时，无法启用MMS更新缓存。为实现简便，所有情况下都不允许在表没有主键时
	// 启用MMS更新缓存
	if (m_tableDef->m_indice[idx]->m_primaryKey && m_tableDef->m_cacheUpdate) {
		assert(m_tableDef->m_useMms);
		try {
			doAlterCacheUpdate(session, false, m_db->getConfig()->m_tlTimeout * 1000);
		} catch (NtseException &e) {
			downgradeMetaLock(session, getMetaLock(session), oldMetaLock, __FILE__, __LINE__);
			throw e;
		}
		// 此时已经加了表元数据X锁并刷出了脏数据
		assert(getMetaLock(session) == IL_X);
	} else {
		// 先不加表锁刷一遍数据，缩短后续加表锁是刷数据用时
		try {
			upgradeLock(session, oldLock, IL_IS, m_db->getConfig()->m_tlTimeout * 1000, __FILE__, __LINE__);
		} catch (NtseException &e) {
			downgradeMetaLock(session, getMetaLock(session), oldMetaLock, __FILE__, __LINE__);
			throw e;
		}
		flush(session);

		// 再加表锁刷数据，由于前面刚刚刷过一次，这次会比较快
		try {
			upgradeLock(session, getLock(session), IL_S, m_db->getConfig()->m_tlTimeout * 1000, __FILE__, __LINE__);
		} catch (NtseException &e) {
			downgradeLock(session, getLock(session), oldLock, __FILE__, __LINE__);
			downgradeMetaLock(session, getMetaLock(session), oldMetaLock, __FILE__, __LINE__);
			throw e;
		}
		flush(session);
	}
	m_db->bumpFlushLsn(session, m_tableDef->m_id);

	// 短期禁止对表的读操作，修改内存中的索引定义
	try {
		upgradeMetaLock(session, getMetaLock(session), IL_X, m_db->getConfig()->m_tlTimeout * 1000, __FILE__, __LINE__);
		upgradeLock(session, getLock(session), IL_X, m_db->getConfig()->m_tlTimeout * 1000, __FILE__, __LINE__);
	} catch (NtseException &e) {
		downgradeLock(session, getLock(session), oldLock, __FILE__, __LINE__);
		downgradeMetaLock(session, getMetaLock(session), oldMetaLock, __FILE__, __LINE__);
		throw e;
	}

	/** 修改表版本号 */
	m_tableDef->m_version = System::microTime();

	m_indice->dropPhaseOne(session, idx);
	m_tableDef->removeIndex(idx);

	downgradeLock(session, getLock(session), IL_S, __FILE__, __LINE__);
	downgradeMetaLock(session, getMetaLock(session), IL_U, __FILE__, __LINE__);

	m_indice->dropPhaseTwo(session, idx);
	//为了保证在写完tabledef后，索引文件的头页和位图页也被刷出去了
	//否则会导致打开索引时，索引文件头页中的索引个数与tabledef定义文件中的索引个数不一致
	flush(session);

	writeTableDef();

	nftrace(ts.ddl, tout << "drop index succeed");

	downgradeLock(session, getLock(session), oldLock, __FILE__, __LINE__);
	downgradeMetaLock(session, getMetaLock(session), oldMetaLock, __FILE__, __LINE__);
}

//Inplace/Online 删除索引时刷数据第一阶段
void Table::dropIndexFlushPhaseOne(Session *session) throw(NtseException) {
	doAlterCacheUpdate(session, false, m_db->getConfig()->m_tlTimeout * 1000);
}
//Inplace/Online 删除索引时刷数据第一阶段
void Table::dropIndexFlushPhaseTwo(Session *session, ILMode oldLock) throw(NtseException) {
	// 先不加表锁刷一遍数据，缩短后续加表锁是刷数据用时
	try {
		upgradeLock(session, oldLock, IL_IS, m_db->getConfig()->m_tlTimeout * 1000, __FILE__, __LINE__);
	} catch (NtseException &e) {
		throw e;
	}
	flush(session);

	// 再加表锁刷数据，由于前面刚刚刷过一次，这次会比较快
	try {
		upgradeLock(session, getLock(session), IL_S, m_db->getConfig()->m_tlTimeout * 1000, __FILE__, __LINE__);
	} catch (NtseException &e) {
		downgradeLock(session, getLock(session), oldLock, __FILE__, __LINE__);
		throw e;
	}
	flush(session);
}


//Inplace/Online 删除索引第二阶段（改表定义）
void Table::dropIndexPhaseOne(Session *session, uint idx, ILMode oldLock) throw(NtseException) {
	try {
		upgradeLock(session, getLock(session), IL_X, m_db->getConfig()->m_tlTimeout * 1000, __FILE__, __LINE__);
	} catch (NtseException &e) {
		downgradeLock(session, getLock(session), oldLock, __FILE__, __LINE__);
		throw e;
	}
	m_indice->dropPhaseOne(session, idx);
	m_tableDef->removeIndex(idx);

	downgradeLock(session, getLock(session), oldLock, __FILE__, __LINE__);

}
//Inplace/Online 删除索引第三阶段（删索引）
void Table::dropIndexPhaseTwo(Session *session, uint idx, ILMode oldLock) throw(NtseException) {
	m_indice->dropPhaseTwo(session, idx);
	//为了保证在写完tabledef后，索引文件的头页和位图页也被刷出去了
	//否则会导致打开索引时，索引文件头页中的索引个数与tabledef定义文件中的索引个数不一致
	flush(session);

	writeTableDef();

	nftrace(ts.ddl, tout << "drop index succeed");

	downgradeLock(session, getLock(session), oldLock, __FILE__, __LINE__);
}

/** 重做删除索引操作
 *
 * @param session 会话
 * @param lsn 日志LSN
 * @param log LOG_IDX_DROP_INDEX日志
 */
void Table::redoDropIndex(Session *session, u64 lsn, const LogEntry *log) {
	ftrace(ts.recv, tout << session << lsn << log);
	assert(log->m_logType == LOG_IDX_DROP_INDEX);

	redoFlush(session);

	m_tableDef->m_version = System::microTime();

	u16 idx = (u16 )m_indice->redoDropIndex(session, lsn, log->m_data, (uint)log->m_size);
	if (m_indice->getIndexNum() < m_tableDef->m_numIndice) {
		assert(m_indice->getIndexNum() == (uint)(m_tableDef->m_numIndice - 1));
		m_tableDef->removeIndex(idx);
		redoFlush(session);
		writeTableDef();
	}
}

/** 优化表数据
 *
 * @param session   会话
 * @param keepDict  是否指定了保留旧字典(如果有全局字典的话)
 * @param newHasDic OUT 新表是否含有全局字典文件
 * @param cancelFlag 操作取消标志
 * @throw NtseException 加表锁超时或文件操作异常，在发生异常时，本函数保证原表数据不会被破坏
 */
void Table::optimize(Session *session, bool keepDict, bool *newHasDic, bool *cancelFlag) throw(NtseException) {
	if (m_tableDef->m_indexOnly)
		NTSE_THROW(NTSE_EC_NOT_SUPPORT, "Index only table can not been optimized");

	// OPTIMIZE通过一个即不增加任何字段也不删除任何字段的在线增删字段操作实现
	TblMntOptimizer optimizer(this, session->getConnection(), cancelFlag, keepDict);
	optimizer.alterTable(session);

	(*newHasDic) = optimizer.isNewTbleHasDict();
}

/** 从表文件路径中提取出表名和SCHEMA名
 * @param path 表文件路径
 * @param schemaName OUT，SCHEMA名，内存使用new分配
 * @param tableName OUT，表名，内存使用new分配
 */
void Table::getSchemaTableFromPath(const char *path, char **schemaName, char **tableName) {
	char *slash = (char *)path + strlen(path);
	while (*slash != '/')
		slash--;
	*tableName = System::strdup(slash + 1);
	char *prev_slash = slash - 1;
	while (prev_slash >= path && *prev_slash != '/')
		prev_slash--;
	size_t len = slash - prev_slash - 1;
	*schemaName = new char[len + 1];
	memcpy(*schemaName, prev_slash + 1, len);
	(*schemaName)[len] = '\0';
}

/** 设置在线创建的索引。在在线创建索引之前需调用本接口通知表管理模块
 * 在日志中记录更新涉及的待建索引属性。
 * @pre 加了元数据X锁
 * 
 * @param session 会话
 * @param numIndice 待建索引个数
 * @param indice 各待建索引。拷贝使用
 */
void Table::setOnlineAddingIndice(Session *session, u16 numIndice, const IndexDef **indice) {
	UNREFERENCED_PARAMETER(session);
	assert(m_metaLock.isLocked(session->getId(), IL_X));
	assert(m_tableDefWithAddingIndice == m_tableDef && numIndice > 0);
	m_tableDefWithAddingIndice = new TableDef(m_tableDef);
	for (u16 i = 0; i < numIndice; i++)
		m_tableDefWithAddingIndice->addIndex(indice[i]);
}

/** 清除在线创建的索引。在在线创建索引完成时调用本接口。
 * @pre 加了元数据X锁
 *
 * @param session 会话
 */
void Table::resetOnlineAddingIndice(Session *session) {
	UNREFERENCED_PARAMETER(session);
	assert(m_metaLock.isLocked(session->getId(), IL_X));
	assert(m_tableDefWithAddingIndice != m_tableDef);
	delete m_tableDefWithAddingIndice;
	m_tableDefWithAddingIndice = m_tableDef;
}

/** 开始表扫描
 * @pre 若tblLock为true则必须已经加好了表锁
 *
 * @param session 会话对象
 * @param opType 操作类型
 * @param numReadCols 要读取的属性数，不能为0
 * @param readCols 要读取的各属性号，从0开始，递增排好序。不能为NULL
 *   内存使用约定：拷贝
 * @param tblLock 是否要加表锁
 * @param lobCtx 用于存储所返回的大对象内容的内存分配上下文，若为NULL，则使用Session::getLobContext。
 *   当使用Session::getLobContext保存大对象内容时，在获取一下条记录或endScan时，这一内存会被自动释放，
 *   若保存大对象内容的内存分配上下文由外部指定，则由外部负责释放
 * @throw NtseException 加表锁超时
 * @return 扫描句柄
 */
TblScan* Table::tableScan(Session *session, OpType opType, u16 numReadCols, u16 *readCols, bool tblLock, MemoryContext *lobCtx) throw(NtseException) {
	ftrace(ts.dml, tout << session << opType << u16a(numReadCols, readCols));
	assert(numReadCols && readCols);
	//assert(session->getTrans() != NULL || (tblLock || checkLock(session, opType)));

	PROFILE(PI_Table_tableScan);

	TblScan *scan = beginScan(session, ST_TBL_SCAN, opType, numReadCols, readCols, tblLock);
	scan->determineRowLockPolicy();
	Records::Scan *recScan = m_records->beginScan(session, opType, ColList(numReadCols, readCols), lobCtx,
		scan->m_rowLock, scan->m_pRlh, session->getConnection()->getLocalConfig()->m_accurateTblScan);
	scan->m_recInfo = recScan;
	scan->m_mysqlRow = recScan->getMysqlRow();
	scan->m_redRow = recScan->getRedRow();
	return scan;
}

/** 开始索引扫描
 * @pre 若tblLock为true则必须已经加好了表锁
 *
 * @param session 会话对象
 * @param opType 操作类型
 * @param cond 扫描条件，本函数会浅拷贝一份。若条件为空，则设置cond->m_key为NULL，
 *   不允许cond->m_key不为NULL，但其m_data为NULL或m_numCols为0。
 * @param numReadCols 要读取的属性数，不能为0
 * @param readCols 要读取的各属性号，从0开始，递增排好序。不能为NULL
 *   内存使用约定：拷贝
 * @param tblLock 是否要加表锁
 * @param lobCtx 用于存储所返回的大对象内容的内存分配上下文，见tableScan说明
 * @throw NtseException 加表锁超时
 * @return 扫描句柄
 */
TblScan* Table::indexScan(Session *session, OpType opType, const IndexScanCond *cond, u16 numReadCols,
	u16 *readCols, bool tblLock, MemoryContext *lobCtx) throw(NtseException) {
	ftrace(ts.dml, tout << session << opType << cond << u16a(numReadCols, readCols));
	assert(!cond->m_key || (cond->m_key->m_format == KEY_PAD && cond->m_key->m_numCols > 0 && cond->m_key->m_rowId == INVALID_ROW_ID));
	assert(!cond->m_singleFetch || (cond->m_forward && cond->m_includeKey));
	assert(numReadCols && readCols);
	assert(tblLock || checkLock(session, opType));

	PROFILE(PI_Table_indexScan);

	TblScan *scan = beginScan(session, ST_IDX_SCAN, opType, numReadCols, readCols, tblLock);
	scan->m_indexKey = cond->m_key;
	scan->m_indexDef = m_tableDef->m_indice[cond->m_idx];
	scan->m_index = m_indice->getIndex(cond->m_idx);
	scan->m_singleFetch = cond->m_singleFetch;
	scan->m_coverageIndex = isCoverageIndex(m_tableDef, scan->m_indexDef, scan->m_readCols);
	scan->determineRowLockPolicy();
	if (scan->m_coverageIndex) {
		// redRow由于格式原因，不能存储超长字段的实际内容，因此不能和idxKey共享空间
		void *p = session->getMemoryContext()->alloc(sizeof(SubRecord));
		scan->m_redRow = new (p)SubRecord(REC_REDUNDANT, scan->m_readCols.m_size, scan->m_readCols.m_cols, NULL, m_tableDef->m_maxRecSize);
		// 如果是索引覆盖扫描，mysqlRow和redundantRow可以共享空间
		scan->m_mysqlRow = scan->m_redRow;
		if (opType != OP_READ)
			scan->m_recInfo = m_records->beginBulkUpdate(session, opType, scan->m_readCols);
	} else {
		Records::BulkFetch *fetch = m_records->beginBulkFetch(session, opType, scan->m_readCols, lobCtx, scan->m_rowLock);
		scan->m_recInfo = fetch;
		scan->m_mysqlRow = fetch->getMysqlRow();
		scan->m_redRow = fetch->getRedRow();
	}

	//  无论是否索引覆盖，都需要传出到PAD格式的idxKey中， idxKey会在handler层比对数据时用到
	scan->m_idxExtractor = SubToSubExtractor::createInst(session->getMemoryContext(), m_tableDef, scan->m_indexDef,
		scan->m_indexDef->m_numCols, scan->m_indexDef->m_columns, numReadCols, readCols, KEY_COMPRESS, KEY_PAD, cond->m_singleFetch? 1: 1000);

	void *p = session->getMemoryContext()->alloc(sizeof(SubRecord));
	byte *data = (byte*)session->getMemoryContext()->alloc(scan->m_indexDef->m_maxKeySize + 1);
	scan->m_idxKey = new (p)SubRecord(KEY_PAD, scan->m_indexDef->m_numCols, scan->m_indexDef->m_columns, data, m_tableDef->m_maxRecSize);

	if (cond->m_singleFetch) {
		assert(m_tableDef->m_indice[cond->m_idx]->m_unique 
			&& cond->m_key->m_numCols == m_tableDef->m_indice[cond->m_idx]->m_numCols);
		return scan;
	}

	scan->m_indexScan = scan->m_index->beginScan(session,
		cond->m_key, cond->m_forward, cond->m_includeKey, scan->m_rowLock, scan->m_pRlh, scan->m_idxExtractor);
	return scan;
}

/** 开始指定RID取记录的定位扫描，延迟更新时使用
 * @pre 若tblLock为true则必须已经加好了表锁
 *
 * @param session 会话对象
 * @param opType 操作类型
 * @param numReadCols 要读取的属性数，不能为0
 * @param readCols 要读取的各属性号，从0开始，递增排好序。不能为NULL
 *   内存使用约定：拷贝
 * @param tblLock 是否要加表锁
 * @param lobCtx 用于存储所返回的大对象内容的内存分配上下文，见tableScan说明
 * @throw NtseException 加表锁超时
 * @return 扫描句柄
 */
TblScan* Table::positionScan(Session *session, OpType opType, u16 numReadCols, u16 *readCols, bool tblLock,
	MemoryContext *lobCtx) throw(NtseException) {
	ftrace(ts.dml, tout << session << opType << u16a(numReadCols, readCols));
	assert(numReadCols && readCols);
	assert(tblLock || checkLock(session, opType));

	PROFILE(PI_Table_positionScan);

	TblScan *scan = beginScan(session, ST_POS_SCAN, opType, numReadCols, readCols, tblLock);
	scan->determineRowLockPolicy();
	Records::BulkFetch *fetch = m_records->beginBulkFetch(session, opType, scan->m_readCols, lobCtx, scan->m_rowLock);
	scan->m_recInfo = fetch;
	scan->m_mysqlRow = fetch->getMysqlRow();
	scan->m_redRow = fetch->getRedRow();
	return scan;
}

/** 开始扫描，完成各类扫描共同的初始化
 * @param session 会话
 * @param type 扫描类型
 * @param opType 操作类型
 * @param numReadCols 要读取的属性数，不能为0
 * @param readCols 要读取的各属性号，从0开始，递增排好序。不能为NULL
 *   内存使用约定：拷贝
 * @param tblLock 是否要加表锁
 * @return 扫描句柄
 */
TblScan* Table::beginScan(Session *session, ScanType type, OpType opType,
	u16 numReadCols, u16 *readCols, bool tblLock) {
	if (tblLock)
		lockTable(session, opType);
	session->getLobContext()->reset();

	TblScan *scan = allocScan(session, numReadCols, readCols, opType, tblLock);
	scan->m_type = type;
	scan->m_scanRowsType = TblScan::getRowStatTypeForScanType(type);
	session->incOpStat(TblScan::getTblStatTypeForScanType(type));

	return scan;
}

/** 定位到扫描的下一条记录。上一条记录上的锁和大对象占用的内存将被自动释放，
 * 但SI_READ模式下的表扫描或覆盖性索引扫描不会加行锁
 * @post 更新语句、定位扫描或非覆盖性索引扫描时当前行已锁定
 * @post 若为读操作，则当前行的锁已经释放，大对象数据仍可用
 * @post 上一条记录占用的所有资源已经被释放
 *
 * @param scan 扫描句柄
 * @param mysqlRowUpperLayer OUT，存储记录内容，存储为REC_UPPMYSQL格式
 * @param rid 可选参数，只在定位扫描时取指定的记录
 * @param needConvertToUppMysqlFormat 可选参数，选择是否需要将mysqlRowUpperLayer转成上层格式，只有需要返回mysql上层时才设置为true
 * @return 是否定位到一条记录
 */
bool Table::getNext(TblScan *scan, byte *mysqlRowUpperLayer, RowId rid, bool needConvertToUppMysqlFormat) {
	assert(scan->m_type == ST_POS_SCAN || rid == INVALID_ROW_ID);
	PROFILE(PI_Table_getNext);

	releaseLastRow(scan, false);
	if (scan->checkEofOnGetNext())
		return false;
	McSavepoint savepoint(scan->m_session->getMemoryContext());

	byte *mysqlRow = mysqlRowUpperLayer;

	if (m_tableDef->hasLongVar() && needConvertToUppMysqlFormat) {
		mysqlRow = (byte *) scan->m_session->getMemoryContext()->alloc(m_tableDef->m_maxRecSize);
	}

	bool exist;
	if (scan->m_type == ST_TBL_SCAN) {
		Records::Scan *rs = (Records::Scan *)(scan->m_recInfo);
		exist = rs->getNext(mysqlRow);
	} else if (scan->m_type == ST_IDX_SCAN) {
		if (scan->m_coverageIndex)
			scan->m_redRow->m_data = mysqlRow;
		RowId rid;
		if (!scan->m_singleFetch) {
			exist = scan->m_index->getNext(scan->m_indexScan, scan->m_idxKey);
			rid = scan->m_indexScan->getRowId();
		} else {
			exist = scan->m_index->getByUniqueKey(scan->m_session, scan->m_indexKey, scan->m_rowLock, 
				&rid, scan->m_idxKey, scan->m_pRlh, scan->m_idxExtractor);
		}
		if (exist && !scan->m_coverageIndex) {
			Records::BulkFetch *fetch = (Records::BulkFetch *)(scan->m_recInfo);
			NTSE_ASSERT(fetch->getNext(rid, mysqlRow, None, NULL));
		}  else if (exist){
			// 一定是索引覆盖扫描，因此索引不会有大对象列，这里可以将PAD格式直接转成REDUNDANT格式
			RecordOper::extractSubRecordPRNoLobColumn(m_tableDef, scan->m_indexDef, scan->m_idxKey, scan->m_redRow);
		}
	} else {
		assert(scan->m_type == ST_POS_SCAN);
		Records::BulkFetch *fetch = (Records::BulkFetch *)(scan->m_recInfo);
		exist = fetch->getNext(rid, mysqlRow, scan->m_rowLock, scan->m_pRlh);
	}

	scan->m_bof = false;
	if (!exist) {
		stopUnderlyingScan(scan);
		scan->m_eof = true;
		return false;
	}

	// 马上释放行锁，防止连接外表加锁时间过长
	if (scan->m_opType == OP_READ)
		releaseLastRow(scan, true);
	
	// 将mysqlrow格式转换成上层的格式
	if (m_tableDef->hasLongVar() && needConvertToUppMysqlFormat) {
		SubRecord upperMysqlSubRec(REC_UPPMYSQL, scan->m_readCols.m_size, scan->m_readCols.m_cols, 
			mysqlRowUpperLayer, m_tableDef->m_maxMysqlRecSize);
		SubRecord engineMysqlSubRec(REC_MYSQL, scan->m_readCols.m_size, scan->m_readCols.m_cols, 
			mysqlRow, m_tableDef->m_maxRecSize);
		RecordOper::convertSubRecordMEngineToUp(m_tableDef, &engineMysqlSubRec, &upperMysqlSubRec);
	}


	scan->m_session->incOpStat(OPS_ROW_READ);
	scan->m_session->incOpStat(scan->m_scanRowsType);
	m_rlockCnts[scan->m_rowLock]++;
	return true;
}

/** 更新当前扫描的记录
 * @pre 记录被用X锁锁定
 * @post 记录上的锁已经释放，大对象内容仍可用
 *
 * @param scan 扫描句柄
 * @param update 要更新的属性及其值，使用REC_MYSQL格式
 * @param isUpdateEngineFormat 要更新的update是否是引擎层的MYSQL格式(针对超长变长字段)
 * @param dupIndex OUT，发生唯一性索引冲突时返回导致冲突的索引号。若为NULL则不给出
 * @param oldRow 若不为NULL，则指定记录的前像，为REC_MYSQL格式
 * @param cbParam	对于需要回调的情况,使用该参数作为回调参数,默认为NULL
 * @throw NtseException 记录超长，属性不足等
 * @return 成功返回true，由于唯一性索引冲突失败返回false
 */
bool Table::updateCurrent(TblScan *scan, const byte *update, bool isUpdateEngineFormat, uint *dupIndex, const byte *oldRow, void *cbParam) throw(NtseException) {
	ftrace(ts.dml, tout << scan << update);
	assert(!scan->m_eof);
	assert(scan->m_opType == OP_UPDATE || scan->m_opType == OP_WRITE);
	assert(scan->m_rlh && scan->m_rlh->getLockMode() == Exclusived);
	assert(scan->m_session->isRowLocked(m_tableDef->m_id, scan->m_redRow->m_rowId, Exclusived));
	
	PROFILE(PI_Table_updateCurrent);

	if (m_tableDef->m_indexOnly && scan->m_readCols.m_size < m_tableDef->m_numCols)
		NTSE_THROW(NTSE_EC_NOT_SUPPORT, "All columns must been read before update index only tables.");

	Session *session = scan->m_session;
	RSUpdateInfo *rsUpdate = scan->m_recInfo->getUpdInfo();
	TSModInfo *tsMod = scan->m_updInfo;
	
	McSavepoint savepoint(scan->m_session->getMemoryContext());

	scan->prepareForUpdate(update, oldRow, isUpdateEngineFormat);
	// 更新前项可能含有超长字段，因此需要用转换成引擎层格式之后的前项
	SubRecord updatePreImage(REC_MYSQL, rsUpdate->m_updateMysql.m_numCols, rsUpdate->m_updateMysql.m_columns, (byte*)scan->m_mysqlRow->m_data, rsUpdate->m_updateMysql.m_size);
	nftrace(ts.dml, tout << "Update preImage: " << t_srec(m_tableDef, &updatePreImage) << " postImage: " << t_srec(m_tableDef, &rsUpdate->m_updateMysql));

	if (!m_tableDef->m_useMms || (!m_db->getConfig()->m_enableMmsCacheUpdate || !scan->m_recInfo->tryMmsCachedUpdate())) {
		size_t preUpdateLogSize;
		byte *preUpdateLog = session->constructPreUpdateLog(m_tableDef, scan->m_redRow, &rsUpdate->m_updateMysql,
			rsUpdate->m_updLob, tsMod->m_indexPreImage, &preUpdateLogSize);	// 异常直接抛出
		
		session->startTransaction(TXN_UPDATE, m_tableDef->m_id);
		session->writePreUpdateLog(m_tableDef, preUpdateLog, preUpdateLogSize);
		SYNCHERE(SP_TBL_UPDATE_AFTER_STARTTXN_LOG);
		
		if (tsMod->m_updIndex) {
			uint dupIndex2;
			// 根据是否更新大对象来判断是非事务表更新还是事务表purge	
			bool succ = m_indice->updateIndexEntries(session, tsMod->m_indexPreImage, &rsUpdate->m_updateRed, rsUpdate->m_updLob, &dupIndex2);
			if (!succ) {
				vecode(vs.tbl, try {
						verifyRecord(scan->m_session, scan->m_redRow->m_rowId, NULL);
					} catch (NtseException &e) {
						m_db->getSyslog()->log(EL_PANIC, "%s", e.getMessage());
						assert_always(false);
					});
				session->endTransaction(false);
				if (dupIndex)
					*dupIndex = dupIndex2;
				releaseLastRow(scan, true);
				return false;
			}
		} else
			session->setTxnDurableLsn(session->getLastLsn());
		
		// 更新完索引成功之后就可以记录binlog，否则因为更新涉及大对象会比较慢
		// NTSE内部认为索引更新成功该操作一定成功，如果更新大对象系统崩溃容易产生不同步
		if (!TableDef::tableIdIsTemp(m_tableDef->m_id) && !m_mysqlTmpTable) {
			SubRecord updateRec(REC_UPPMYSQL, rsUpdate->m_updateMysql.m_numCols, rsUpdate->m_updateMysql.m_columns, (byte*)update, m_tableDef->m_maxMysqlRecSize);
			SubRecord oldRec(REC_UPPMYSQL, scan->m_mysqlRow->m_numCols, scan->m_mysqlRow->m_columns, (byte*)oldRow, m_tableDef->m_maxMysqlRecSize);
			m_db->getNTSECallbackManager()->callback(ROW_UPDATE, m_tableDef, &oldRec, &updateRec, cbParam);
		}

		if (!m_tableDef->m_indexOnly)
			scan->m_recInfo->updateRow();
		session->endTransaction(true);
	}

	vecode(vs.tbl, verifyRecord(scan->m_session, scan->m_redRow->m_rowId, &rsUpdate->m_updateMysql));

	releaseLastRow(scan, true);

	session->incOpStat(OPS_ROW_UPDATE);
	return true;
}

/** 删除当前扫描的记录
 * @pre 记录被用X锁锁定
 * @post 记录锁已经释放，大对象内容仍可用
 *
 * @param scan 扫描句柄
 * @param cbParam	对于需要回调的情况,使用该参数作为回调参数,默认为NULL
 */
void Table::deleteCurrent(TblScan *scan, void *cbParam) {
	ftrace(ts.dml, tout << scan);
	assert(!scan->m_eof);
	assert(scan->m_opType == OP_DELETE || scan->m_opType == OP_WRITE);
	assert(scan->m_rlh && scan->m_rlh->getLockMode() == Exclusived);
	assert(scan->m_session->isRowLocked(m_tableDef->m_id, scan->m_redRow->m_rowId, Exclusived));

	PROFILE(PI_Table_deleteCurrent);
	
	if (m_tableDef->m_indexOnly && scan->m_readCols.m_size < m_tableDef->m_numCols)
		NTSE_THROW(NTSE_EC_NOT_SUPPORT, "All columns must been read before update index only tables.");

	Session *session = scan->m_session;
	SubRecord *currentRow = scan->m_redRow;
	if (!scan->m_delInfo)
		scan->initDelInfo();
	TSModInfo *modInfo = scan->m_delInfo;

	McSavepoint savepoint(scan->m_session->getMemoryContext());

	scan->prepareForDelete();

	session->startTransaction(TXN_DELETE, m_tableDef->m_id);
	writePreDeleteLog(scan->m_session, currentRow->m_rowId, currentRow->m_data, modInfo->m_indexPreImage);

	Record rec(currentRow->m_rowId, REC_REDUNDANT, currentRow->m_data, m_tableDef->m_maxRecSize);
	if (m_tableDef->m_numIndice > 0) {
		m_indice->deleteIndexEntries(session, &rec, scan->m_indexScan);
	} else
		session->setTxnDurableLsn(session->getLastLsn());

	if (!m_tableDef->m_indexOnly)
		scan->m_recInfo->deleteRow();

	vecode(vs.tbl, verifyDeleted(scan->m_session, currentRow->m_rowId, modInfo->m_indexPreImage));

	// TODO: 超长字段引擎层写删除binlog暂时还存在问题
	if (!TableDef::tableIdIsTemp(m_tableDef->m_id) && !m_mysqlTmpTable)
		m_db->getNTSECallbackManager()->callback(ROW_DELETE, m_tableDef, scan->m_mysqlRow, NULL, cbParam);

	session->endTransaction(true);

	releaseLastRow(scan, true);
	session->incOpStat(OPS_ROW_DELETE);
	if (m_estimateRows > 0)
		m_estimateRows--;
}


/** 结束一次扫描。结束扫描之前，当前行占用的所有资源都被释放。
 * @post 若开始扫描时指定tblLock为true则本函数返回时表锁已经释放
 *
 * @param scan 扫描句柄，将被释放
 */
void Table::endScan(TblScan *scan) {
	ftrace(ts.dml, tout << scan);

	PROFILE(PI_Table_endScan);

	releaseLastRow(scan, false);
	stopUnderlyingScan(scan);
	if (scan->m_tblLock)
		unlockTable(scan->m_session, scan->m_opType);
}

/** 插入一条记录
 *
 * @param session 会话对象
 * @param record 要插入的记录，使用REC_MYSQL/REC_UPPMYSQL格式
 * @param isRecordEngineFormat 是否是引擎层格式
 * @param dupIndex 输出参数，导致冲突的索引序号
 * @param tblLock 是否加表锁，默认是在进行INSERT操作时自动加表锁，若上层已经加了表锁，则应指定为false
 * @param cbParam	对于需要回调的情况,使用该参数作为回调参数,默认为NULL
 * @throw NtseException 加表锁超时，或记录超长
 * @return 成功返回记录RID，由于唯一性索引冲突失败返回INVALID_ROW_ID
 */
RowId Table::insert(Session *session, const byte *record, bool isRecordEngineFormat, uint *dupIndex, bool tblLock, void *cbParam) throw(NtseException) {
	ftrace(ts.dml, tout << session << record);

	// assert(tblLock || checkLock(session, OP_WRITE));

	PROFILE(PI_Table_insert);

	McSavepoint savepoint(session->getMemoryContext());
	RowLockHandle *rlh = NULL;
	Record *redRec = NULL;

	Record mysqlRec(INVALID_ROW_ID, REC_UPPMYSQL, (byte *)record, m_tableDef->m_maxMysqlRecSize);

	// 如果含有超长变长字段，需要对mysqlRow格式记做转换
	if (!isRecordEngineFormat && m_tableDef->hasLongVar()) {
		byte *data = (byte *)session->getMemoryContext()->alloc(m_tableDef->m_maxRecSize);
		Record realMysqlRec(INVALID_ROW_ID, REC_MYSQL, data, m_tableDef->m_maxRecSize);
		RecordOper::convertRecordMUpToEngine(m_tableDef, &mysqlRec, &realMysqlRec);	
		mysqlRec.m_data = realMysqlRec.m_data;
		mysqlRec.m_size = realMysqlRec.m_size;
		mysqlRec.m_format = REC_MYSQL;
	} else {
		mysqlRec.m_format = REC_MYSQL;
	}
	m_records->prepareForInsert(&mysqlRec);

	if (tblLock)
		lockTable(session, OP_WRITE);

	session->startTransaction(TXN_INSERT, m_tableDef->m_id);
	if (!m_tableDef->m_indexOnly) {
		try {
			redRec = m_records->insert(session, &mysqlRec, &rlh);
		} catch (NtseException) {
			// 走原Ntse 的table::insert 不需要加事务行锁，不会抛错
			assert(false);
		}
	}else {
		mysqlRec.m_rowId = checksum64(record, m_tableDef->m_maxRecSize);
		void *p = session->getMemoryContext()->alloc(sizeof(Record));
		redRec = new (p)Record(mysqlRec.m_rowId, REC_REDUNDANT, (byte *)record, m_tableDef->m_maxRecSize);
		rlh = TRY_LOCK_ROW(session, m_tableDef->m_id, mysqlRec.m_rowId, Exclusived);
	}

	bool succ = true;
	if (m_tableDef->m_numIndice > 0) {
		succ = m_indice->insertIndexEntries(session, redRec, dupIndex);
		if (!succ) {
			nftrace(ts.dml, tout << "duplicate key:" << *dupIndex);
			if (!m_tableDef->m_indexOnly)
				m_records->undoInsert(session, redRec);
		}
	} else
		session->setTxnDurableLsn(session->getLastLsn());
	vecode(vs.tbl && succ, verifyRecord(session, &mysqlRec));

	if (succ && !TableDef::tableIdIsTemp(m_tableDef->m_id) && !m_mysqlTmpTable) {
		ColList allCols = ColList::generateAscColList(session->getMemoryContext(), 0, m_tableDef->m_numCols);
		SubRecord insertRec(REC_UPPMYSQL, allCols.m_size, allCols.m_cols, (byte*)record, m_tableDef->m_maxMysqlRecSize);
		m_db->getNTSECallbackManager()->callback(ROW_INSERT, m_tableDef, NULL, &insertRec, cbParam);
	}

	session->endTransaction(succ);
	session->unlockRow(&rlh);

	if (tblLock)
		unlockTable(session, OP_WRITE);

	if (succ) {
		session->incOpStat(OPS_ROW_INSERT);
		m_estimateRows++;
	}
	return succ ? mysqlRec.m_rowId : INVALID_ROW_ID;
}

#ifdef TNT_ENGINE
RowId Table::insert(Session *session, const byte *record, uint *dupIndex, RowLockHandle **rlh) throw(NtseException) {
	ftrace(ts.dml, tout << session << record);
	assert(*rlh == NULL);

	PROFILE(PI_Table_insert);

	Record mysqlRec(INVALID_ROW_ID, REC_MYSQL, (byte *)record, m_tableDef->m_maxRecSize);
	m_records->prepareForInsert(&mysqlRec);

	McSavepoint savepoint(session->getMemoryContext());
	Record *redRec;

	session->startTransaction(TXN_INSERT, m_tableDef->m_id);
	if (!m_tableDef->m_indexOnly) {
		try {	
			redRec = m_records->insert(session, &mysqlRec, rlh);
		} catch (NtseException &e) {
			session->endTransaction(false);
			throw e;
		}
	} else {
		mysqlRec.m_rowId = checksum64(record, m_tableDef->m_maxRecSize);
		void *p = session->getMemoryContext()->alloc(sizeof(Record));
		redRec = new (p)Record(mysqlRec.m_rowId, REC_REDUNDANT, (byte *)record, m_tableDef->m_maxRecSize);
		*rlh = TRY_LOCK_ROW(session, m_tableDef->m_id, mysqlRec.m_rowId, Exclusived);
		tnt::TNTTransaction *trx = session->getTrans();
		if (trx != NULL && m_tableDef->isTNTTable()) {
			DrsHeap::writeInsertTNTLog(session, m_tableDef->m_id, trx->getTrxId(), trx->getTrxLastLsn(), mysqlRec.m_rowId);
		}
	}

	bool succ = true;
	if (m_tableDef->m_numIndice > 0) {
		if (NULL != session->getTrans()) {
			UNREFERENCED_PARAMETER(dupIndex);
			m_indice->insertIndexNoDupEntries(session, redRec);
		} else {
			succ = m_indice->insertIndexEntries(session, redRec, dupIndex);
			if (!succ) {
				nftrace(ts.dml, tout << "duplicate key:" << *dupIndex);
				if (!m_tableDef->m_indexOnly)
					m_records->undoInsert(session, redRec);
			}
		}	
	} else
		session->setTxnDurableLsn(session->getLastLsn());
	vecode(vs.tbl && succ, verifyRecord(session, &mysqlRec));

	session->endTransaction(succ);

	if (succ) {
		session->incOpStat(OPS_ROW_INSERT);
		m_estimateRows++;
	}
	return succ ? mysqlRec.m_rowId : INVALID_ROW_ID;
}
#endif

/** 执行REPLACE和INSERT ... ON DUPLICATE KEY UPDATE语句时先试着插入记录
 * 注: 本函数可能从session的内存分配上下文中分配空间，因此若一个session中
 * 调用本函数多次可能导致内存无限增长
 *
 * @param session 会话对象
 * @param record 要插入的记录，一定是MySQL格式
 * @param tblLock 是否加表锁，默认是在进行INSERT操作时自动加表锁，若上层已经加了表锁，则应指定为false
 * @param cbParam	对于需要回调的情况,使用该参数作为回调参数,默认为NULL
 * @throw NtseException 加表锁超时，记录超长
 * @return 成功返回NULL，失败返回IDU操作序列
 */
#ifdef TNT_ENGINE
IUSequence<TblScan *>* Table::insertForDupUpdate(Session *session, const byte *record, bool tblLock, void *cbParam) throw(NtseException) {
#else
IUSequence* Table::insertForDupUpdate(Session *session, const byte *record, bool tblLock, void *cbParam) throw(NtseException) {
#endif
	ftrace(ts.dml, tout << session << record);

	PROFILE(PI_Table_insertForDupUpdate);

	while (true) {
		uint dupIndex;
		if (insert(session, record, false, &dupIndex, tblLock, cbParam) != INVALID_ROW_ID)
			return NULL;
		assert(m_tableDef->m_indice[dupIndex]->m_unique);

		SYNCHERE(SP_TBL_REPLACE_AFTER_DUP);

		u64 mcSavepoint = session->getMemoryContext()->setSavepoint();
#ifdef TNT_ENGINE
		IUSequence<TblScan *> *iuSeq = (IUSequence<TblScan *> *)session->getMemoryContext()->alloc(sizeof(IUSequence<TblScan *>));
#else
		IUSequence *iuSeq = (IUSequence *)session->getMemoryContext()->alloc(sizeof(IUSequence));
#endif
		iuSeq->m_mysqlRow = (byte *)session->getMemoryContext()->alloc(m_tableDef->m_maxRecSize);
		iuSeq->m_tableDef = m_tableDef;
		iuSeq->m_dupIndex = dupIndex;

		IndexDef *index = m_tableDef->m_indice[dupIndex];
		SubRecord *key = (SubRecord *)session->getMemoryContext()->alloc(sizeof(SubRecord));
		byte *keyDat = (byte *)session->getMemoryContext()->alloc(index->m_maxKeySize);
		new (key)SubRecord(KEY_PAD, index->m_numCols, index->m_columns, keyDat, index->m_maxKeySize);

		Record rec(INVALID_ROW_ID, REC_REDUNDANT, (byte *)record, m_tableDef->m_maxRecSize);

		// 如果索引列包含大对象，则需要拼装真正的PAD格式键
		Array<LobPair*> lobArray;
		if (index->hasLob()) {
			RecordOper::extractLobFromM(session, m_tableDef, index, &rec, &lobArray);
		}

		RecordOper::extractKeyRP(m_tableDef, index, &rec, &lobArray, key);

		// TODO 只获取查询需要的属性
		ColList readCols = ColList::generateAscColList(session->getMemoryContext(), 0, m_tableDef->m_numCols);
		IndexScanCond cond((u16 )dupIndex, key, true, true, index == m_tableDef->m_pkey);
		TblScan *scan = indexScan(session, OP_WRITE, &cond, readCols.m_size, readCols.m_cols, tblLock);
		if (getNext(scan, iuSeq->m_mysqlRow)) {
			iuSeq->m_scan = scan;
			return iuSeq;
		}
		// 由于INSERT的时候没有加锁，可能又找不到记录了
		// 这时再重试插入
		endScan(scan);
		session->getMemoryContext()->resetToSavepoint(mcSavepoint);
	}
}

/** 执行REPLACE和INSERT ... ON DUPLICATE KEY UPDATE语句时，发生冲突后更新记录
 *
 * @param iuSeq INSERT ... ON DUPLICATE KEY UPDATE操作序列
 * @param update 发生冲突的记录将要被更新成的值，为REC_MYSQL格式
 * @param numUpdateCols 要更新的属性个数
 * @param updateCols 要更新的属性
 * @param dupIndex OUT，唯一性冲突时返回导致冲突的索引号，若为NULL则不返回索引号
 * @param cbParam	对于需要回调的情况,使用该参数作为回调参数,默认为NULL
 * @throw NtseException 记录超长
 * @return 更新成功返回true，由于唯一性索引冲突失败返回false
 */
#ifdef TNT_ENGINE
bool Table::updateDuplicate(IUSequence<TblScan *> *iuSeq, byte *update, u16 numUpdateCols, u16 *updateCols, uint *dupIndex, void *cbParam) throw(NtseException) {
#else
bool Table::updateDuplicate(IUSequence *iuSeq, byte *update, u16 numUpdateCols, u16 *updateCols, uint *dupIndex, void *cbParam) throw(NtseException) {
#endif
	assert(iuSeq->m_scan);

	PROFILE(PI_Table_updateDuplicate);

	iuSeq->m_scan->setUpdateColumns(numUpdateCols, updateCols);
	bool succ;
	try {
		// 根据是否更新大对象来判断究竟是非事务表的更新还是事务表的更新
		succ = updateCurrent(iuSeq->m_scan, update, iuSeq->m_scan->m_recInfo->getUpdInfo()->m_updLob, dupIndex, NULL, cbParam);
	} catch (NtseException &e) {
		endScan(iuSeq->m_scan);
		iuSeq->m_scan = NULL;
		throw e;
	}
	endScan(iuSeq->m_scan);
	iuSeq->m_scan = NULL;
	return succ;
}

/** 执行REPLACE和INSERT ... ON DUPLICATE KEY UPDATE语句时，发生冲突后删除原记录，
 * 在指定了自动生成ID时会调用这一函数而不是updateDuplicate
 *
 * @param iuSeq INSERT ... ON DUPLICATE KEY UPDATE操作序列
 * @param cbParam	对于需要回调的情况,使用该参数作为回调参数,默认为NULL
 */
#ifdef TNT_ENGINE
void Table::deleteDuplicate(IUSequence<TblScan *> *iuSeq, void *cbParam) {
#else
void Table::deleteDuplicate(IUSequence *iuSeq, void *cbParam) {
#endif
	assert(iuSeq->m_scan);

	PROFILE(PI_Table_deleteDuplicate);

	deleteCurrent(iuSeq->m_scan, cbParam);
	endScan(iuSeq->m_scan);
	iuSeq->m_scan = NULL;
}

/** 直接释放INSERT ... ON DUPLICATE KEY UPDATE操作序列
 * @param iduSeq INSERT ... ON DUPLICATE KEY UPDATE操作序列
 */
#ifdef TNT_ENGINE
void Table::freeIUSequenceDirect(IUSequence<TblScan *> *iuSeq) {
#else
void Table::freeIUSequenceDirect(IUSequence *iuSeq) {
#endif
	PROFILE(PI_Table_freeIUSequenceDirect);

	endScan(iuSeq->m_scan);
	iuSeq->m_scan = NULL;
}

/** 更新表记录数估计信息
 * @param session 会话
 */
void Table::refreshRows(Session *session) {
	m_records->getHeap()->updateExtendStatus(session, 4);
	m_estimateRows = m_records->getHeap()->getStatusEx().m_numRows;
}

/** 得到表中(预计的)记录数
 *
 * @return 表中(预计的)记录数
 */
u64 Table::getRows() {
	s64 r = m_estimateRows;
	return r >= 0? r: 0;
}

/** 得到记录占用存储空间大小
 * @pre 已经加了表元数据锁
 *
 * @param session 会话
 * @return 记录占用存储空间大小
 */
u64 Table::getDataLength(Session *session) {
	UNREFERENCED_PARAMETER(session);
	assert(getMetaLock(session) != IL_NO);
	return m_records->getDataLength();
}

/** 得到索引占用存储空间大小
 * @pre 已经加了表元数据锁
 *
 * @param session 会话
 * @param includeMeta 是否包含索引头，页面分配位图等非数据页
 * @return 索引占用存储空间大小
 */
u64 Table::getIndexLength(Session *session, bool includeMeta) {
	UNREFERENCED_PARAMETER(session);
	assert(getMetaLock(session) != IL_NO);
	return m_indice->getDataLength(includeMeta);
}

/** 刷出表中脏数据
 *
 * @param session 会话
 */
void Table::flush(Session *session) {
	flushComponent(session, true, true, true, true);
}

/**
* 刷出表中组件
* @pre 已经禁止对表的写操作
*
* @param session 会话
* @param flushHeap, flushIndice, flushMms, flushLob  是否涮出各个组件
*/
void Table::flushComponent(Session *session, bool flushHeap, bool flushIndice, bool flushMms, bool flushLob) {
	m_records->flush(session, flushHeap, flushMms, flushLob);
	if (flushIndice)
		m_indice->flush(session);
}

/** 恢复REDO时写出表中脏数据。刷脏数据过程中不应该写出任何日志
 * @param session 会话
 */
void Table::redoFlush(Session *session) {
	u64 beforeLsn = session->getLastLsn();
	flush(session);
	NTSE_ASSERT(beforeLsn == session->getLastLsn());
}

/**
 * 为表创建压缩全局字典
 *
 * @pre 上层持有表元数据U锁
 * @param session       会话
 * @param metaLockMode  OUT，本函数执行完的表元数据锁级别
 * @param dataLockMode  OUT，本函数执行完的表数据锁级别
 * @throw NtseException 采样出错
 */
void Table::createDictionary(Session *session, ILMode *metaLockMode, ILMode *dataLockMode) throw(NtseException) {
	assert(m_tableDef->m_isCompressedTbl);
	assert(NULL != m_tableDef->m_rowCompressCfg);

	*metaLockMode = getMetaLock(session);
	*dataLockMode = getLock(session);
	string tblFullPath = string(m_db->getConfig()->m_basedir) + NTSE_PATH_SEP + m_path;
	string tmpDictFilePath = tblFullPath + Limits::NAME_TEMP_GLBL_DIC_EXT;
	string newDictFilePath = tblFullPath + Limits::NAME_GLBL_DIC_EXT;

	RCDictionary *tmpDic = NULL;
	try {
		tmpDic = extractCompressDict(session);

		try {
			//后面将修改表的全局字典，所以需要升级表元数据锁为X锁，然后加表数据X锁，防止对表进行读写
			upgradeMetaLock(session, IL_U, IL_X, -1, __FILE__, __LINE__);
			*metaLockMode = getMetaLock(session);
			lock(session, IL_X, -1, __FILE__, __LINE__);
			*dataLockMode = getLock(session);
		} catch (NtseException &) { assert(false); }

		//创建临时字典文件
		createTmpCompressDictFile(tmpDictFilePath.c_str(), tmpDic);

		//写创建字典日志
		m_db->getTxnlog()->flush(writeCreateDictLog(session));

		//替换压缩组件
		m_records->resetCompressComponent(tblFullPath.c_str());
		if (tmpDic) {
			tmpDic->close();
			delete tmpDic;
			tmpDic = NULL;
		}

		//修改Database控制文件
		m_db->alterCtrlFileHasCprsDic(session, m_tableDef->m_id, true);
		m_hasCprsDict = true;

		//删除临时字典文件
		File tmpFile(tmpDictFilePath.c_str());
		u64 errCode = tmpFile.remove();
		UNREFERENCED_PARAMETER(errCode);
		assert(errCode == File::E_NO_ERROR);

	} catch (NtseException &e) {
		if (tmpDic) {
			tmpDic->close();
			delete tmpDic;
			tmpDic = NULL;
		}
		//删除临时字典文件
		File tmpFile(tmpDictFilePath.c_str());
		tmpFile.remove();

		throw e;
	}
}

/**
 * 创建临时压缩字典文件
 *
 * @param dicFullPath   临时压缩字典完整路径，含后缀
 * @param tmpDict       临时字典
 * @throw NtseException 文件操作出错
 */
void Table::createTmpCompressDictFile(const char *dicFullPath, const RCDictionary *tmpDict) throw(NtseException) {
	if (File::isExist(dicFullPath)) {//如果有垃圾文件，先删除之
		File tmpFile(dicFullPath);
		u64 errCode = tmpFile.remove();
		UNREFERENCED_PARAMETER(errCode);
		assert(File::getNtseError(errCode) == File::E_NO_ERROR);
	}
	m_records->createTmpDictFile(dicFullPath, tmpDict);
}

/**
 * 采样提取压缩字典
 *
 * @param session  会话
 * @return         压缩字典(内部new分配，由调用方关闭字典和回收内存)
 */
RCDictionary* Table::extractCompressDict(Session *session) throw (NtseException) {
	assert(m_tableDef->m_isCompressedTbl);
	assert(NULL != m_tableDef->m_rowCompressCfg);
	assert(IntentionLock::isHigher(IL_IS, getMetaLock(session)));

	McSavepoint mcSavePoint(session->getMemoryContext());

	RCMSampleTbl *sampleTblHdl = new RCMSampleTbl(session, m_db, m_tableDef, m_records);

	SmpTblErrCode errCode = sampleTblHdl->beginSampleTbl();
	sampleTblHdl->endSampleTbl();

	RCDictionary *newDic = NULL;
	if (errCode == SMP_NO_ERR) {
		newDic = sampleTblHdl->createDictionary(session);
		assert(newDic);
	} else {
		delete sampleTblHdl;
		sampleTblHdl = NULL;
		assert(!newDic);
		NTSE_THROW(NTSE_EC_GENERIC, "Error occured when sample table to create global dictionary, %s!", SmplTblErrMsg[errCode]); 
	}
	delete sampleTblHdl;
	sampleTblHdl = NULL;
	return newDic;
}

/**
 * 获得表相关的数据对象状态
 * @return 返回表中数据对象列表
 */
Array<DBObjStats*>* Table::getDBObjStats() {
	Array<DBObjStats*> *dbObjArr = new Array<DBObjStats*>();
	if (m_records)
		m_records->getDBObjStats(dbObjArr);
	if (m_indice) {
		dbObjArr->push(m_indice->getDBObjStats());
		for (uint i = 0; i < m_indice->getIndexNum(); i++)
			dbObjArr->push(m_indice->getIndex(i)->getDBObjStats());
	}
	return dbObjArr;
}

/**
 * @see Mms.h中MMSBinlogWriter的定义
 */
void Table::binlogMmsUpdate( Session *session, SubRecord *dirtyRecord, void *data ) {
	UNREFERENCED_PARAMETER(session);
	Table *table = (Table*)data;
	assert(!TableDef::tableIdIsTemp(table->m_tableDef->m_id) && !table->m_mysqlTmpTable);
	table->m_db->getNTSECallbackManager()->callback(ROW_UPDATE, table->getTableDef(), dirtyRecord, dirtyRecord, NULL);
}

#ifdef TNT_ENGINE
/** 删除大对象方法 用于数据库正常情况下的删除
 * @param session 会话
 * @param lobId	  大对象号
 */
void Table::deleteLob(Session* session,  LobId lobId){
	session->startTransaction(TXN_LOB_DELETE,m_tableDef->m_id);
	writePreDeleteLobLog(session,lobId);
	m_records->getLobStorage()->del(session,lobId);
	session->endTransaction(true);
}

/** 删除大对象方法 允许重复删除同一个LobId, 可用于purge及版本池大对象回收恢复时使用
 * @param session 会话
 * @param lobId	  大对象号
 */
void Table::deleteLobAllowDupDel(Session* session,  LobId lobId){
	session->startTransaction(TXN_LOB_DELETE,m_tableDef->m_id);
	writePreDeleteLobLog(session,lobId);
	m_records->getLobStorage()->delAtCrash(session,lobId);
	session->endTransaction(true);
}
#endif
}
