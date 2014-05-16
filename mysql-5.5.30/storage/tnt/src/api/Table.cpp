/**
 * ��������ʵ��
 *
 * @author ��Դ(wangyuan@corp.netease.com, wy@163.org)
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
�����ڴ�����������������ڴ��������ڵ�ʹ�ù淶:

NTSE��ÿ���Ựӵ�������ڴ���������ģ�һ��ͨ���ڴ���������ģ���
getMemoryContext�ķ���ֵ������ר���ڴ洢��������ݵ��ڴ���������ģ���
getLobContext�ķ���ֵ���������ڴ�����������������ڴ��������ڵ�ʹ�ù淶
���£�

1. tableScan/indexScan/posScan��ʼɨ��ʱ��ͨ���ڴ�����������з�����ڴ棬
   �ڽ���ɨ��󲻻ᱻ�ͷţ�
2. updateCurrent/deleteCurrent/insert/getNext������������ͨ���ڴ����������
   �з�����ڴ棬�ڲ����������Զ��ͷţ�
3. ������ڴ����������ר���ڴ洢getNext����������ȡ�Ĵ�����ֵ��������
   IUD�����еĴ����ģ����ش���getNext�������ڴ�����ڴ������������
   ������ڴ棬����һ��getNext����һ����ͬһ���Ự�ٴο�ʼɨ��ʱ�ͷţ���
   endScanʱ�������ͷţ���������MySQL�ϲ����ȵ���endScan���������������ݣ�
4. �����ڴ�����������ڻỰ�ͷ�ʱ�����Զ����ã�
*/

using namespace std;

namespace ntse {

/** ����һ�������
 *
 * @param db ���ݿ�
 * @param path ��·���������basedir��������׺
 * @param records ��¼������
 * @param tableDef ���壬ֱ������
 * @param hasCprsDict �Ƿ���ѹ��ȫ���ֵ�
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

/** ������
 * ��ʧ�����ɱ������½����ļ��Ѿ���ɾ����
 *
 * @param db ���ݿ�
 * @param session �Ự����
 * @param path �ļ�·��(�����basedir)����������׺
 * @param tableDef ����
 * @throw NtseException �ļ�����ʧ�ܣ����岻����Ҫ���
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

		// �ڱ���ʱRecords�ѹرգ��޷����lobStorage����������˿�����ʱ�����룬�ں����ʱ���ٴδ�����ȷ�Ĵ���������ָ��
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

/** ɾ����
 *
 * @param db ���ݿ�
 * @param path �ļ�·��(�����basedir)����������׺
 * @throw NtseException �ļ�����ʧ�ܵ�
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

/** ɾ����
 *
 * @param basedir ��׼·��
 * @param path �ļ�·��(�����basedir)����������׺
 * @throw NtseException �ļ�����ʧ�ܵ�
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

/** �򿪱�
 *
 * @param db ���ݿ�
 * @param session �Ự
 * @param path �ļ�·���������basedir����������׺
 * @param hasCprsDict �Ƿ���ȫ���ֵ��ļ�
 * @throw NtseException �ļ������ڣ���ʽ����ȷ��
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

/** �رձ�
 *
 * @param session �Ự
 * @param flushDirty �Ƿ�д��������
 * @param closeComponents �Ƿ�ֻ�ǹرո����ײ����
 */
void Table::close(Session *session, bool flushDirty, bool closeComponents) {
	ftrace(ts.ddl, tout << session << flushDirty);

	if (m_indice) {
		m_indice->close(session, flushDirty);
		delete m_indice;
		m_indice = NULL;
	}

	// ����ҪˢMMS�п��ܵĸ��»�����־��Ȼ����ùرձ��Ӧ�Ļص�����һ����
	// ��Ҫ�ڹر�Records֮ǰ������ò���m_tableDef
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

/** ����Ƿ��ܹ���������������
 *
 * @param db ���ݿ�
 * @param oldPath ԭ·��(�����basedir)����������׺��
 * @param newPath ��·��(�����basedir)����������׺��
 * @param hasLob �����Ƿ���������
 * @throw NtseException ���ܽ���������ʱͨ�����쳣�㱨ԭ��
 */
void Table::checkRename(Database *db, const char *oldPath, const char *newPath, bool hasLob, bool hasCprsDict/*=false*/) throw(NtseException) {
	ftrace(ts.ddl || ts.recv, tout << db << oldPath << newPath << hasLob);

	string basePath1(string(db->getConfig()->m_basedir) + NTSE_PATH_SEP + oldPath);
	string basePath2(string(db->getConfig()->m_basedir) + NTSE_PATH_SEP + newPath);
	const char **exts = Limits::EXTS;

	int numFiles = Limits::EXTNUM;
	for (int i = 0; i < numFiles; i++) {
		if (!hasCprsDict && !System::stricmp(Limits::NAME_GLBL_DIC_EXT, exts[i])) {//���û���ֵ��ļ���������
			continue;
		}
		if (!hasLob && (!System::stricmp(Limits::NAME_SOBH_EXT, exts[i]) 
			|| !System::stricmp(Limits::NAME_SOBH_TBLDEF_EXT, exts[i]) 
			|| !System::stricmp(Limits::NAME_LOBI_EXT, exts[i]) 
			|| !System::stricmp(Limits::NAME_LOBD_EXT, exts[i]))) {
				continue;
		}
		// ��֤Ŀ���ļ�����������Ȩ�޴���
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

/** ��������
 * @pre ���Ѿ����رգ������Ѿ����ù�checkRename���м��
 *
 * @param db ���ݿ�
 * @param session �Ự
 * @param oldPath ԭ·��(�����basedir)����������׺��
 * @param newPath ��·��(�����basedir)����������׺��
 * @param hasLob �����Ƿ���������
 * @param redo �Ƿ�������RENAME������REDOʱ���ܳ��ֲ����ļ��Ѿ������������
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
		if (!hasCprsDict && !System::stricmp(Limits::NAME_GLBL_DIC_EXT, exts[i])) {//���û���ֵ��ļ���������
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

/** TRUNCATEһ�ű�
 *
 * @param session        �Ự
 * @param tblLock        �Ƿ�ӱ���
 * @param newHasDic      OUT �±��Ƿ���ȫ���ֵ��ļ�
 * @param isTruncateOper �Ƿ���truncate����
 * @throw NtseException  �ӱ�����ʱ���ļ����������
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

	// TODO Ӧ�޸�Ϊ�Ƚ����±���ɾ���ɱ�
	
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
			//�ȿ����ֵ��ļ�����ʱ�ֵ��ļ�
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

		//������ʱ�ֵ��ļ����±�
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

	//ɾ����ʱ�ֵ��ļ�
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

/** ����TRUNCATE����
 * @param db ���ݿ�
 * @param session �Ự
 * @param log ��־����
 * @param path ��·��
 * @param newHasDict OUT �±��Ƿ����ֵ�
 * @throw NtseException �ļ����������
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
			//�ȿ����ֵ��ļ�����ʱ�ֵ��ļ�
			u64 errCode = File::copyFile(tmpDictPath.c_str(), dictPath.c_str(), true);
			if (File::E_NO_ERROR != File::getNtseError(errCode)) {
				delete tableDefBak;
				NTSE_THROW(NTSE_EC_FILE_FAIL, "copy compressed dictionary failed when truncate table.");
			}
		} else if (!File::isExist(tmpDictPath.c_str())) {
			delete tableDefBak;
			return;//�Ѿ�����滻��ֻ��Ҫ�޸Ŀ����ļ�
		}
	}

	try {
		drop(db, path);

		create(db, session, path, tableDefBak);

		//������ʱ�ֵ��ļ����±�
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
		//ɾ����ʱ�ֵ��ļ�
		u64 errCode = tmpDictFile.remove();
		UNREFERENCED_PARAMETER(errCode);
		assert(File::E_NOT_EXIST == File::getNtseError(errCode) || File::E_NO_ERROR == File::getNtseError(errCode));
	}
	delete tableDefBak;
}

/** �жϸñ����Ƿ����δ��ɵ���������
 * @param idxInfo �������δ��ɵ������������������������������Ϣ
 * return ����δ��ɵ�������������true�����򷵻�false
 */
bool Table::hasOnlineIndex(string *idxInfo) {
	bool exist = false;
	//������ǰ���ϵ����������ҳ�����δ��ɵ���������
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

/** �ж���������Ƿ�Ϊ����ɵ�online index
 * @param indexDefs ����������Ϣ
 * @param numIndice �����������
 * return ���ȫ������ɵ�online index����true�����򷵻�false
 */
bool Table::isDoneOnlineIndex(const IndexDef **indexDefs, u16 numIndice) {
	bool done = true;
	for (uint i = 0; i < numIndice; i++) {
		const IndexDef *indexDef = m_tableDef->getIndexDef(indexDefs[i]->m_name);
		if (indexDef != NULL && indexDef->m_online && indexDefs[i]->m_numCols == indexDef->m_numCols) {
			for (uint j = 0; j < indexDef->m_numCols; j++) {
				if (!(indexDefs[i]->m_columns[j] == indexDef->m_columns[j] && 
					indexDefs[i]->m_prefixLens[j] == indexDef->m_prefixLens[j])) {	// �ж��Ƿ���online��������ͬʱ�ж��ж����ǰ׺����
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

/** �ж����߽�����˳���Ƿ���ȷ
 * @param indexDefs ����������Ϣ
 * @param numIndice ������������
 * return ��������˳����ȷ����true�����򷵻�false
 */
bool Table::isOnlineIndexOrder(const IndexDef **indexDefs, u16 numIndice) {
	int lastIndexNo = m_tableDef->getIndexNo(indexDefs[0]->m_name);
	if (lastIndexNo) {
		--lastIndexNo;
		//������������������ǰһ����������壬����ȷ��ǰһ������Ϊ��online
		const IndexDef *lastIndexDef = m_tableDef->getIndexDef(lastIndexNo);
		if (lastIndexDef->m_online) {
			return false;
		}
	}

	for (u16 i = 0; i < numIndice - 1; i++) {
		//ȷ��������������ĵ�i����������Num����i+1����������Num�ǽ�����
		if (m_tableDef->getIndexNo(indexDefs[i]->m_name) + 1 != m_tableDef->getIndexNo(indexDefs[i + 1]->m_name)) {
			return false;
		}
	}

	return true;
}

/** ��������
 *
 * @param session �Ự����
 * @param numIndice Ҫ���ӵ�����������ΪonlineΪfalseֻ��Ϊ1
 * @param indexDefs ����������
 * @throw NtseException �ӱ�����ʱ�������޷�֧�֣�����Ψһ������ʱ���ظ���ֵ��
 */
void Table::addIndex(Session *session, u16 numIndice, const IndexDef **indexDefs) throw(NtseException) {
	ftrace(ts.ddl, tout << session << m_tableDef->m_name << numIndice);
	//1.��ǰ����������ntse��������
	if (indexDefs[0]->m_online) {
		TblMntAlterIndex alter(this, numIndice, indexDefs, 0, NULL);
		alter.alterTable(session);
		return;
	}

	ILMode oldMetaLock = getMetaLock(session);
	upgradeMetaLock(session, oldMetaLock, IL_U, m_db->getConfig()->m_tlTimeout * 1000, __FILE__, __LINE__);

	//2.��ǰ����������mysql��������
	string idxInfo;

	//2.1 ��ǰ���Ϻ���δ��ɵ���������
	if (hasOnlineIndex(&idxInfo)) {
		if (isDoneOnlineIndex(indexDefs, numIndice)) {
			//�ж����߽�����˳���Ƿ���ȷ
			if (!isOnlineIndexOrder(indexDefs, numIndice)) {
				downgradeMetaLock(session, getMetaLock(session), oldMetaLock, __FILE__, __LINE__);
				NTSE_THROW(NTSE_EC_GENERIC, "Add online index order was wrong, %s.", idxInfo.c_str());
			}
			//��ɿ��ٴ������������������
			dualFlushAndBump(session, m_db->getConfig()->m_tlTimeout * 1000); //��������ὫmetaLock��IL_U��ΪIL_X
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

	//2.2 ��ǰ����û��δ��ɵ���������
	if (numIndice == 1) {
		ILMode oldLock = getLock(session);
		// ���������ڼ��ֹ�Ա����д����
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
			// ��֤ADD_INDEX��־��д����TODO: Ϊʲô?
			m_db->getTxnlog()->flush(session->getLastLsn());
			downgradeLock(session, getLock(session), oldLock, __FILE__, __LINE__);
			downgradeMetaLock(session, getMetaLock(session), oldMetaLock, __FILE__, __LINE__);
			throw e;
		}

		session->setTxnDurableLsn(session->getLastLsn());

		// ���ڽ�ֹ�Ա���ж���������ɱ����޸�
		// ��MySQL�ϲ㱣֤���ᷢ������
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

/** Inplace����������һ�׶Σ���Meta Lock U���ı����£���ȡ�����ݣ������������˲����Ϊ��ʱ
 *
 * @param session �Ự����
 * @param numIndice Ҫ���ӵ�����������Inplaceʱֻ��Ϊ1(��Online)
 * @param indexDefs ����������
 * @throw NtseException �ӱ�����ʱ�������޷�֧�֣�����Ψһ������ʱ���ظ���ֵ��
 */
void Table::addIndexPhaseOne(Session *session, u16 numIndice, const IndexDef **indexDefs) throw (NtseException) {
	// Inplace�´�����������������ֻ��Ϊ1
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
		// ��֤ADD_INDEX��־��д����TODO: Ϊʲô?
		m_db->getTxnlog()->flush(session->getLastLsn());
		throw e;
	}

	session->setTxnDurableLsn(session->getLastLsn());
}

/** Inplace���������ڶ��׶Σ���Meta Lock X���ı����£��޸�TableDef��Ϣ���˲���ܿ�
 *
 * @param session �Ự����
 * @param numIndice Ҫ���ӵ�����������Inplaceʱֻ��Ϊ1(��Online)
 * @param indexDefs ����������
 * @throw NtseException �ӱ�����ʱ�������޷�֧�֣�����Ψһ������ʱ���ظ���ֵ��
 */
void Table::addIndexPhaseTwo(Session *session, u16 numIndice, const IndexDef **indexDefs) throw(NtseException) {
	assert(numIndice == 1);

	const IndexDef *indexDef = indexDefs[0];

	m_indice->createIndexPhaseTwo(indexDef);

	m_tableDef->addIndex(indexDef);
	writeTableDef();

	// session��Ӧ��NTSE΢���������������ĵ�һ�׶ο�ʼ���˴��ύ
	session->endTransaction(true, true);

	nftrace(ts.ddl, tout << "add index succeed");
}

/** ��tabledef��д�붨���ļ�
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

/** ɾ������
 *
 * @param session �Ự����
 * @param idx Ҫɾ�����ǵڼ�������
 * @throw NtseException �ӱ�����ʱ
 */
void Table::dropIndex(Session *session, uint idx) throw(NtseException) {
	ftrace(ts.ddl, tout << session << m_tableDef->m_name << idx);
	assert(idx < m_tableDef->m_numIndice);

	ILMode oldMetaLock = getMetaLock(session);
	ILMode oldLock = getLock(session);
	
	upgradeMetaLock(session, oldMetaLock, IL_U, m_db->getConfig()->m_tlTimeout * 1000, __FILE__, __LINE__);	// ���ܳ�ʱ��ֱ���׳��쳣

	// ����binlog��Ҫ��¼����������û������ʱ���޷�����MMS���»��档Ϊʵ�ּ�㣬��������¶��������ڱ�û������ʱ
	// ����MMS���»���
	if (m_tableDef->m_indice[idx]->m_primaryKey && m_tableDef->m_cacheUpdate) {
		assert(m_tableDef->m_useMms);
		try {
			doAlterCacheUpdate(session, false, m_db->getConfig()->m_tlTimeout * 1000);
		} catch (NtseException &e) {
			downgradeMetaLock(session, getMetaLock(session), oldMetaLock, __FILE__, __LINE__);
			throw e;
		}
		// ��ʱ�Ѿ����˱�Ԫ����X����ˢ����������
		assert(getMetaLock(session) == IL_X);
	} else {
		// �Ȳ��ӱ���ˢһ�����ݣ����̺����ӱ�����ˢ������ʱ
		try {
			upgradeLock(session, oldLock, IL_IS, m_db->getConfig()->m_tlTimeout * 1000, __FILE__, __LINE__);
		} catch (NtseException &e) {
			downgradeMetaLock(session, getMetaLock(session), oldMetaLock, __FILE__, __LINE__);
			throw e;
		}
		flush(session);

		// �ټӱ���ˢ���ݣ�����ǰ��ո�ˢ��һ�Σ���λ�ȽϿ�
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

	// ���ڽ�ֹ�Ա�Ķ��������޸��ڴ��е���������
	try {
		upgradeMetaLock(session, getMetaLock(session), IL_X, m_db->getConfig()->m_tlTimeout * 1000, __FILE__, __LINE__);
		upgradeLock(session, getLock(session), IL_X, m_db->getConfig()->m_tlTimeout * 1000, __FILE__, __LINE__);
	} catch (NtseException &e) {
		downgradeLock(session, getLock(session), oldLock, __FILE__, __LINE__);
		downgradeMetaLock(session, getMetaLock(session), oldMetaLock, __FILE__, __LINE__);
		throw e;
	}

	/** �޸ı�汾�� */
	m_tableDef->m_version = System::microTime();

	m_indice->dropPhaseOne(session, idx);
	m_tableDef->removeIndex(idx);

	downgradeLock(session, getLock(session), IL_S, __FILE__, __LINE__);
	downgradeMetaLock(session, getMetaLock(session), IL_U, __FILE__, __LINE__);

	m_indice->dropPhaseTwo(session, idx);
	//Ϊ�˱�֤��д��tabledef�������ļ���ͷҳ��λͼҳҲ��ˢ��ȥ��
	//����ᵼ�´�����ʱ�������ļ�ͷҳ�е�����������tabledef�����ļ��е�����������һ��
	flush(session);

	writeTableDef();

	nftrace(ts.ddl, tout << "drop index succeed");

	downgradeLock(session, getLock(session), oldLock, __FILE__, __LINE__);
	downgradeMetaLock(session, getMetaLock(session), oldMetaLock, __FILE__, __LINE__);
}

//Inplace/Online ɾ������ʱˢ���ݵ�һ�׶�
void Table::dropIndexFlushPhaseOne(Session *session) throw(NtseException) {
	doAlterCacheUpdate(session, false, m_db->getConfig()->m_tlTimeout * 1000);
}
//Inplace/Online ɾ������ʱˢ���ݵ�һ�׶�
void Table::dropIndexFlushPhaseTwo(Session *session, ILMode oldLock) throw(NtseException) {
	// �Ȳ��ӱ���ˢһ�����ݣ����̺����ӱ�����ˢ������ʱ
	try {
		upgradeLock(session, oldLock, IL_IS, m_db->getConfig()->m_tlTimeout * 1000, __FILE__, __LINE__);
	} catch (NtseException &e) {
		throw e;
	}
	flush(session);

	// �ټӱ���ˢ���ݣ�����ǰ��ո�ˢ��һ�Σ���λ�ȽϿ�
	try {
		upgradeLock(session, getLock(session), IL_S, m_db->getConfig()->m_tlTimeout * 1000, __FILE__, __LINE__);
	} catch (NtseException &e) {
		downgradeLock(session, getLock(session), oldLock, __FILE__, __LINE__);
		throw e;
	}
	flush(session);
}


//Inplace/Online ɾ�������ڶ��׶Σ��ı��壩
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
//Inplace/Online ɾ�����������׶Σ�ɾ������
void Table::dropIndexPhaseTwo(Session *session, uint idx, ILMode oldLock) throw(NtseException) {
	m_indice->dropPhaseTwo(session, idx);
	//Ϊ�˱�֤��д��tabledef�������ļ���ͷҳ��λͼҳҲ��ˢ��ȥ��
	//����ᵼ�´�����ʱ�������ļ�ͷҳ�е�����������tabledef�����ļ��е�����������һ��
	flush(session);

	writeTableDef();

	nftrace(ts.ddl, tout << "drop index succeed");

	downgradeLock(session, getLock(session), oldLock, __FILE__, __LINE__);
}

/** ����ɾ����������
 *
 * @param session �Ự
 * @param lsn ��־LSN
 * @param log LOG_IDX_DROP_INDEX��־
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

/** �Ż�������
 *
 * @param session   �Ự
 * @param keepDict  �Ƿ�ָ���˱������ֵ�(�����ȫ���ֵ�Ļ�)
 * @param newHasDic OUT �±��Ƿ���ȫ���ֵ��ļ�
 * @param cancelFlag ����ȡ����־
 * @throw NtseException �ӱ�����ʱ���ļ������쳣���ڷ����쳣ʱ����������֤ԭ�����ݲ��ᱻ�ƻ�
 */
void Table::optimize(Session *session, bool keepDict, bool *newHasDic, bool *cancelFlag) throw(NtseException) {
	if (m_tableDef->m_indexOnly)
		NTSE_THROW(NTSE_EC_NOT_SUPPORT, "Index only table can not been optimized");

	// OPTIMIZEͨ��һ�����������κ��ֶ�Ҳ��ɾ���κ��ֶε�������ɾ�ֶβ���ʵ��
	TblMntOptimizer optimizer(this, session->getConnection(), cancelFlag, keepDict);
	optimizer.alterTable(session);

	(*newHasDic) = optimizer.isNewTbleHasDict();
}

/** �ӱ��ļ�·������ȡ��������SCHEMA��
 * @param path ���ļ�·��
 * @param schemaName OUT��SCHEMA�����ڴ�ʹ��new����
 * @param tableName OUT���������ڴ�ʹ��new����
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

/** �������ߴ����������������ߴ�������֮ǰ����ñ��ӿ�֪ͨ�����ģ��
 * ����־�м�¼�����漰�Ĵ����������ԡ�
 * @pre ����Ԫ����X��
 * 
 * @param session �Ự
 * @param numIndice ������������
 * @param indice ����������������ʹ��
 */
void Table::setOnlineAddingIndice(Session *session, u16 numIndice, const IndexDef **indice) {
	UNREFERENCED_PARAMETER(session);
	assert(m_metaLock.isLocked(session->getId(), IL_X));
	assert(m_tableDefWithAddingIndice == m_tableDef && numIndice > 0);
	m_tableDefWithAddingIndice = new TableDef(m_tableDef);
	for (u16 i = 0; i < numIndice; i++)
		m_tableDefWithAddingIndice->addIndex(indice[i]);
}

/** ������ߴ����������������ߴ����������ʱ���ñ��ӿڡ�
 * @pre ����Ԫ����X��
 *
 * @param session �Ự
 */
void Table::resetOnlineAddingIndice(Session *session) {
	UNREFERENCED_PARAMETER(session);
	assert(m_metaLock.isLocked(session->getId(), IL_X));
	assert(m_tableDefWithAddingIndice != m_tableDef);
	delete m_tableDefWithAddingIndice;
	m_tableDefWithAddingIndice = m_tableDef;
}

/** ��ʼ��ɨ��
 * @pre ��tblLockΪtrue������Ѿ��Ӻ��˱���
 *
 * @param session �Ự����
 * @param opType ��������
 * @param numReadCols Ҫ��ȡ��������������Ϊ0
 * @param readCols Ҫ��ȡ�ĸ����Ժţ���0��ʼ�������ź��򡣲���ΪNULL
 *   �ڴ�ʹ��Լ��������
 * @param tblLock �Ƿ�Ҫ�ӱ���
 * @param lobCtx ���ڴ洢�����صĴ�������ݵ��ڴ���������ģ���ΪNULL����ʹ��Session::getLobContext��
 *   ��ʹ��Session::getLobContext������������ʱ���ڻ�ȡһ������¼��endScanʱ����һ�ڴ�ᱻ�Զ��ͷţ�
 *   �������������ݵ��ڴ�������������ⲿָ���������ⲿ�����ͷ�
 * @throw NtseException �ӱ�����ʱ
 * @return ɨ����
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

/** ��ʼ����ɨ��
 * @pre ��tblLockΪtrue������Ѿ��Ӻ��˱���
 *
 * @param session �Ự����
 * @param opType ��������
 * @param cond ɨ����������������ǳ����һ�ݡ�������Ϊ�գ�������cond->m_keyΪNULL��
 *   ������cond->m_key��ΪNULL������m_dataΪNULL��m_numColsΪ0��
 * @param numReadCols Ҫ��ȡ��������������Ϊ0
 * @param readCols Ҫ��ȡ�ĸ����Ժţ���0��ʼ�������ź��򡣲���ΪNULL
 *   �ڴ�ʹ��Լ��������
 * @param tblLock �Ƿ�Ҫ�ӱ���
 * @param lobCtx ���ڴ洢�����صĴ�������ݵ��ڴ���������ģ���tableScan˵��
 * @throw NtseException �ӱ�����ʱ
 * @return ɨ����
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
		// redRow���ڸ�ʽԭ�򣬲��ܴ洢�����ֶε�ʵ�����ݣ���˲��ܺ�idxKey����ռ�
		void *p = session->getMemoryContext()->alloc(sizeof(SubRecord));
		scan->m_redRow = new (p)SubRecord(REC_REDUNDANT, scan->m_readCols.m_size, scan->m_readCols.m_cols, NULL, m_tableDef->m_maxRecSize);
		// �������������ɨ�裬mysqlRow��redundantRow���Թ���ռ�
		scan->m_mysqlRow = scan->m_redRow;
		if (opType != OP_READ)
			scan->m_recInfo = m_records->beginBulkUpdate(session, opType, scan->m_readCols);
	} else {
		Records::BulkFetch *fetch = m_records->beginBulkFetch(session, opType, scan->m_readCols, lobCtx, scan->m_rowLock);
		scan->m_recInfo = fetch;
		scan->m_mysqlRow = fetch->getMysqlRow();
		scan->m_redRow = fetch->getRedRow();
	}

	//  �����Ƿ��������ǣ�����Ҫ������PAD��ʽ��idxKey�У� idxKey����handler��ȶ�����ʱ�õ�
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

/** ��ʼָ��RIDȡ��¼�Ķ�λɨ�裬�ӳٸ���ʱʹ��
 * @pre ��tblLockΪtrue������Ѿ��Ӻ��˱���
 *
 * @param session �Ự����
 * @param opType ��������
 * @param numReadCols Ҫ��ȡ��������������Ϊ0
 * @param readCols Ҫ��ȡ�ĸ����Ժţ���0��ʼ�������ź��򡣲���ΪNULL
 *   �ڴ�ʹ��Լ��������
 * @param tblLock �Ƿ�Ҫ�ӱ���
 * @param lobCtx ���ڴ洢�����صĴ�������ݵ��ڴ���������ģ���tableScan˵��
 * @throw NtseException �ӱ�����ʱ
 * @return ɨ����
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

/** ��ʼɨ�裬��ɸ���ɨ�蹲ͬ�ĳ�ʼ��
 * @param session �Ự
 * @param type ɨ������
 * @param opType ��������
 * @param numReadCols Ҫ��ȡ��������������Ϊ0
 * @param readCols Ҫ��ȡ�ĸ����Ժţ���0��ʼ�������ź��򡣲���ΪNULL
 *   �ڴ�ʹ��Լ��������
 * @param tblLock �Ƿ�Ҫ�ӱ���
 * @return ɨ����
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

/** ��λ��ɨ�����һ����¼����һ����¼�ϵ����ʹ����ռ�õ��ڴ潫���Զ��ͷţ�
 * ��SI_READģʽ�µı�ɨ��򸲸�������ɨ�費�������
 * @post ������䡢��λɨ���Ǹ���������ɨ��ʱ��ǰ��������
 * @post ��Ϊ����������ǰ�е����Ѿ��ͷţ�����������Կ���
 * @post ��һ����¼ռ�õ�������Դ�Ѿ����ͷ�
 *
 * @param scan ɨ����
 * @param mysqlRowUpperLayer OUT���洢��¼���ݣ��洢ΪREC_UPPMYSQL��ʽ
 * @param rid ��ѡ������ֻ�ڶ�λɨ��ʱȡָ���ļ�¼
 * @param needConvertToUppMysqlFormat ��ѡ������ѡ���Ƿ���Ҫ��mysqlRowUpperLayerת���ϲ��ʽ��ֻ����Ҫ����mysql�ϲ�ʱ������Ϊtrue
 * @return �Ƿ�λ��һ����¼
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
			// һ������������ɨ�裬������������д�����У�������Խ�PAD��ʽֱ��ת��REDUNDANT��ʽ
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

	// �����ͷ���������ֹ����������ʱ�����
	if (scan->m_opType == OP_READ)
		releaseLastRow(scan, true);
	
	// ��mysqlrow��ʽת�����ϲ�ĸ�ʽ
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

/** ���µ�ǰɨ��ļ�¼
 * @pre ��¼����X������
 * @post ��¼�ϵ����Ѿ��ͷţ�����������Կ���
 *
 * @param scan ɨ����
 * @param update Ҫ���µ����Լ���ֵ��ʹ��REC_MYSQL��ʽ
 * @param isUpdateEngineFormat Ҫ���µ�update�Ƿ���������MYSQL��ʽ(��Գ����䳤�ֶ�)
 * @param dupIndex OUT������Ψһ��������ͻʱ���ص��³�ͻ�������š���ΪNULL�򲻸���
 * @param oldRow ����ΪNULL����ָ����¼��ǰ��ΪREC_MYSQL��ʽ
 * @param cbParam	������Ҫ�ص������,ʹ�øò�����Ϊ�ص�����,Ĭ��ΪNULL
 * @throw NtseException ��¼���������Բ����
 * @return �ɹ�����true������Ψһ��������ͻʧ�ܷ���false
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
	// ����ǰ����ܺ��г����ֶΣ������Ҫ��ת����������ʽ֮���ǰ��
	SubRecord updatePreImage(REC_MYSQL, rsUpdate->m_updateMysql.m_numCols, rsUpdate->m_updateMysql.m_columns, (byte*)scan->m_mysqlRow->m_data, rsUpdate->m_updateMysql.m_size);
	nftrace(ts.dml, tout << "Update preImage: " << t_srec(m_tableDef, &updatePreImage) << " postImage: " << t_srec(m_tableDef, &rsUpdate->m_updateMysql));

	if (!m_tableDef->m_useMms || (!m_db->getConfig()->m_enableMmsCacheUpdate || !scan->m_recInfo->tryMmsCachedUpdate())) {
		size_t preUpdateLogSize;
		byte *preUpdateLog = session->constructPreUpdateLog(m_tableDef, scan->m_redRow, &rsUpdate->m_updateMysql,
			rsUpdate->m_updLob, tsMod->m_indexPreImage, &preUpdateLogSize);	// �쳣ֱ���׳�
		
		session->startTransaction(TXN_UPDATE, m_tableDef->m_id);
		session->writePreUpdateLog(m_tableDef, preUpdateLog, preUpdateLogSize);
		SYNCHERE(SP_TBL_UPDATE_AFTER_STARTTXN_LOG);
		
		if (tsMod->m_updIndex) {
			uint dupIndex2;
			// �����Ƿ���´�������ж��Ƿ��������»��������purge	
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
		
		// �����������ɹ�֮��Ϳ��Լ�¼binlog��������Ϊ�����漰������Ƚ���
		// NTSE�ڲ���Ϊ�������³ɹ��ò���һ���ɹ���������´����ϵͳ�������ײ�����ͬ��
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

/** ɾ����ǰɨ��ļ�¼
 * @pre ��¼����X������
 * @post ��¼���Ѿ��ͷţ�����������Կ���
 *
 * @param scan ɨ����
 * @param cbParam	������Ҫ�ص������,ʹ�øò�����Ϊ�ص�����,Ĭ��ΪNULL
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

	// TODO: �����ֶ������дɾ��binlog��ʱ����������
	if (!TableDef::tableIdIsTemp(m_tableDef->m_id) && !m_mysqlTmpTable)
		m_db->getNTSECallbackManager()->callback(ROW_DELETE, m_tableDef, scan->m_mysqlRow, NULL, cbParam);

	session->endTransaction(true);

	releaseLastRow(scan, true);
	session->incOpStat(OPS_ROW_DELETE);
	if (m_estimateRows > 0)
		m_estimateRows--;
}


/** ����һ��ɨ�衣����ɨ��֮ǰ����ǰ��ռ�õ�������Դ�����ͷš�
 * @post ����ʼɨ��ʱָ��tblLockΪtrue�򱾺�������ʱ�����Ѿ��ͷ�
 *
 * @param scan ɨ�����������ͷ�
 */
void Table::endScan(TblScan *scan) {
	ftrace(ts.dml, tout << scan);

	PROFILE(PI_Table_endScan);

	releaseLastRow(scan, false);
	stopUnderlyingScan(scan);
	if (scan->m_tblLock)
		unlockTable(scan->m_session, scan->m_opType);
}

/** ����һ����¼
 *
 * @param session �Ự����
 * @param record Ҫ����ļ�¼��ʹ��REC_MYSQL/REC_UPPMYSQL��ʽ
 * @param isRecordEngineFormat �Ƿ���������ʽ
 * @param dupIndex ������������³�ͻ���������
 * @param tblLock �Ƿ�ӱ�����Ĭ�����ڽ���INSERT����ʱ�Զ��ӱ��������ϲ��Ѿ����˱�������Ӧָ��Ϊfalse
 * @param cbParam	������Ҫ�ص������,ʹ�øò�����Ϊ�ص�����,Ĭ��ΪNULL
 * @throw NtseException �ӱ�����ʱ�����¼����
 * @return �ɹ����ؼ�¼RID������Ψһ��������ͻʧ�ܷ���INVALID_ROW_ID
 */
RowId Table::insert(Session *session, const byte *record, bool isRecordEngineFormat, uint *dupIndex, bool tblLock, void *cbParam) throw(NtseException) {
	ftrace(ts.dml, tout << session << record);

	// assert(tblLock || checkLock(session, OP_WRITE));

	PROFILE(PI_Table_insert);

	McSavepoint savepoint(session->getMemoryContext());
	RowLockHandle *rlh = NULL;
	Record *redRec = NULL;

	Record mysqlRec(INVALID_ROW_ID, REC_UPPMYSQL, (byte *)record, m_tableDef->m_maxMysqlRecSize);

	// ������г����䳤�ֶΣ���Ҫ��mysqlRow��ʽ����ת��
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
			// ��ԭNtse ��table::insert ����Ҫ�����������������״�
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

/** ִ��REPLACE��INSERT ... ON DUPLICATE KEY UPDATE���ʱ�����Ų����¼
 * ע: ���������ܴ�session���ڴ�����������з���ռ䣬�����һ��session��
 * ���ñ�������ο��ܵ����ڴ���������
 *
 * @param session �Ự����
 * @param record Ҫ����ļ�¼��һ����MySQL��ʽ
 * @param tblLock �Ƿ�ӱ�����Ĭ�����ڽ���INSERT����ʱ�Զ��ӱ��������ϲ��Ѿ����˱�������Ӧָ��Ϊfalse
 * @param cbParam	������Ҫ�ص������,ʹ�øò�����Ϊ�ص�����,Ĭ��ΪNULL
 * @throw NtseException �ӱ�����ʱ����¼����
 * @return �ɹ�����NULL��ʧ�ܷ���IDU��������
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

		// ��������а������������Ҫƴװ������PAD��ʽ��
		Array<LobPair*> lobArray;
		if (index->hasLob()) {
			RecordOper::extractLobFromM(session, m_tableDef, index, &rec, &lobArray);
		}

		RecordOper::extractKeyRP(m_tableDef, index, &rec, &lobArray, key);

		// TODO ֻ��ȡ��ѯ��Ҫ������
		ColList readCols = ColList::generateAscColList(session->getMemoryContext(), 0, m_tableDef->m_numCols);
		IndexScanCond cond((u16 )dupIndex, key, true, true, index == m_tableDef->m_pkey);
		TblScan *scan = indexScan(session, OP_WRITE, &cond, readCols.m_size, readCols.m_cols, tblLock);
		if (getNext(scan, iuSeq->m_mysqlRow)) {
			iuSeq->m_scan = scan;
			return iuSeq;
		}
		// ����INSERT��ʱ��û�м������������Ҳ�����¼��
		// ��ʱ�����Բ���
		endScan(scan);
		session->getMemoryContext()->resetToSavepoint(mcSavepoint);
	}
}

/** ִ��REPLACE��INSERT ... ON DUPLICATE KEY UPDATE���ʱ��������ͻ����¼�¼
 *
 * @param iuSeq INSERT ... ON DUPLICATE KEY UPDATE��������
 * @param update ������ͻ�ļ�¼��Ҫ�����³ɵ�ֵ��ΪREC_MYSQL��ʽ
 * @param numUpdateCols Ҫ���µ����Ը���
 * @param updateCols Ҫ���µ�����
 * @param dupIndex OUT��Ψһ�Գ�ͻʱ���ص��³�ͻ�������ţ���ΪNULL�򲻷���������
 * @param cbParam	������Ҫ�ص������,ʹ�øò�����Ϊ�ص�����,Ĭ��ΪNULL
 * @throw NtseException ��¼����
 * @return ���³ɹ�����true������Ψһ��������ͻʧ�ܷ���false
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
		// �����Ƿ���´�������жϾ����Ƿ������ĸ��»��������ĸ���
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

/** ִ��REPLACE��INSERT ... ON DUPLICATE KEY UPDATE���ʱ��������ͻ��ɾ��ԭ��¼��
 * ��ָ�����Զ�����IDʱ�������һ����������updateDuplicate
 *
 * @param iuSeq INSERT ... ON DUPLICATE KEY UPDATE��������
 * @param cbParam	������Ҫ�ص������,ʹ�øò�����Ϊ�ص�����,Ĭ��ΪNULL
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

/** ֱ���ͷ�INSERT ... ON DUPLICATE KEY UPDATE��������
 * @param iduSeq INSERT ... ON DUPLICATE KEY UPDATE��������
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

/** ���±��¼��������Ϣ
 * @param session �Ự
 */
void Table::refreshRows(Session *session) {
	m_records->getHeap()->updateExtendStatus(session, 4);
	m_estimateRows = m_records->getHeap()->getStatusEx().m_numRows;
}

/** �õ�����(Ԥ�Ƶ�)��¼��
 *
 * @return ����(Ԥ�Ƶ�)��¼��
 */
u64 Table::getRows() {
	s64 r = m_estimateRows;
	return r >= 0? r: 0;
}

/** �õ���¼ռ�ô洢�ռ��С
 * @pre �Ѿ����˱�Ԫ������
 *
 * @param session �Ự
 * @return ��¼ռ�ô洢�ռ��С
 */
u64 Table::getDataLength(Session *session) {
	UNREFERENCED_PARAMETER(session);
	assert(getMetaLock(session) != IL_NO);
	return m_records->getDataLength();
}

/** �õ�����ռ�ô洢�ռ��С
 * @pre �Ѿ����˱�Ԫ������
 *
 * @param session �Ự
 * @param includeMeta �Ƿ��������ͷ��ҳ�����λͼ�ȷ�����ҳ
 * @return ����ռ�ô洢�ռ��С
 */
u64 Table::getIndexLength(Session *session, bool includeMeta) {
	UNREFERENCED_PARAMETER(session);
	assert(getMetaLock(session) != IL_NO);
	return m_indice->getDataLength(includeMeta);
}

/** ˢ������������
 *
 * @param session �Ự
 */
void Table::flush(Session *session) {
	flushComponent(session, true, true, true, true);
}

/**
* ˢ���������
* @pre �Ѿ���ֹ�Ա��д����
*
* @param session �Ự
* @param flushHeap, flushIndice, flushMms, flushLob  �Ƿ��̳��������
*/
void Table::flushComponent(Session *session, bool flushHeap, bool flushIndice, bool flushMms, bool flushLob) {
	m_records->flush(session, flushHeap, flushMms, flushLob);
	if (flushIndice)
		m_indice->flush(session);
}

/** �ָ�REDOʱд�����������ݡ�ˢ�����ݹ����в�Ӧ��д���κ���־
 * @param session �Ự
 */
void Table::redoFlush(Session *session) {
	u64 beforeLsn = session->getLastLsn();
	flush(session);
	NTSE_ASSERT(beforeLsn == session->getLastLsn());
}

/**
 * Ϊ����ѹ��ȫ���ֵ�
 *
 * @pre �ϲ���б�Ԫ����U��
 * @param session       �Ự
 * @param metaLockMode  OUT��������ִ����ı�Ԫ����������
 * @param dataLockMode  OUT��������ִ����ı�����������
 * @throw NtseException ��������
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
			//���潫�޸ı��ȫ���ֵ䣬������Ҫ������Ԫ������ΪX����Ȼ��ӱ�����X������ֹ�Ա���ж�д
			upgradeMetaLock(session, IL_U, IL_X, -1, __FILE__, __LINE__);
			*metaLockMode = getMetaLock(session);
			lock(session, IL_X, -1, __FILE__, __LINE__);
			*dataLockMode = getLock(session);
		} catch (NtseException &) { assert(false); }

		//������ʱ�ֵ��ļ�
		createTmpCompressDictFile(tmpDictFilePath.c_str(), tmpDic);

		//д�����ֵ���־
		m_db->getTxnlog()->flush(writeCreateDictLog(session));

		//�滻ѹ�����
		m_records->resetCompressComponent(tblFullPath.c_str());
		if (tmpDic) {
			tmpDic->close();
			delete tmpDic;
			tmpDic = NULL;
		}

		//�޸�Database�����ļ�
		m_db->alterCtrlFileHasCprsDic(session, m_tableDef->m_id, true);
		m_hasCprsDict = true;

		//ɾ����ʱ�ֵ��ļ�
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
		//ɾ����ʱ�ֵ��ļ�
		File tmpFile(tmpDictFilePath.c_str());
		tmpFile.remove();

		throw e;
	}
}

/**
 * ������ʱѹ���ֵ��ļ�
 *
 * @param dicFullPath   ��ʱѹ���ֵ�����·��������׺
 * @param tmpDict       ��ʱ�ֵ�
 * @throw NtseException �ļ���������
 */
void Table::createTmpCompressDictFile(const char *dicFullPath, const RCDictionary *tmpDict) throw(NtseException) {
	if (File::isExist(dicFullPath)) {//����������ļ�����ɾ��֮
		File tmpFile(dicFullPath);
		u64 errCode = tmpFile.remove();
		UNREFERENCED_PARAMETER(errCode);
		assert(File::getNtseError(errCode) == File::E_NO_ERROR);
	}
	m_records->createTmpDictFile(dicFullPath, tmpDict);
}

/**
 * ������ȡѹ���ֵ�
 *
 * @param session  �Ự
 * @return         ѹ���ֵ�(�ڲ�new���䣬�ɵ��÷��ر��ֵ�ͻ����ڴ�)
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
 * ��ñ���ص����ݶ���״̬
 * @return ���ر������ݶ����б�
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
 * @see Mms.h��MMSBinlogWriter�Ķ���
 */
void Table::binlogMmsUpdate( Session *session, SubRecord *dirtyRecord, void *data ) {
	UNREFERENCED_PARAMETER(session);
	Table *table = (Table*)data;
	assert(!TableDef::tableIdIsTemp(table->m_tableDef->m_id) && !table->m_mysqlTmpTable);
	table->m_db->getNTSECallbackManager()->callback(ROW_UPDATE, table->getTableDef(), dirtyRecord, dirtyRecord, NULL);
}

#ifdef TNT_ENGINE
/** ɾ������󷽷� �������ݿ���������µ�ɾ��
 * @param session �Ự
 * @param lobId	  ������
 */
void Table::deleteLob(Session* session,  LobId lobId){
	session->startTransaction(TXN_LOB_DELETE,m_tableDef->m_id);
	writePreDeleteLobLog(session,lobId);
	m_records->getLobStorage()->del(session,lobId);
	session->endTransaction(true);
}

/** ɾ������󷽷� �����ظ�ɾ��ͬһ��LobId, ������purge���汾�ش������ջָ�ʱʹ��
 * @param session �Ự
 * @param lobId	  ������
 */
void Table::deleteLobAllowDupDel(Session* session,  LobId lobId){
	session->startTransaction(TXN_LOB_DELETE,m_tableDef->m_id);
	writePreDeleteLobLog(session,lobId);
	m_records->getLobStorage()->delAtCrash(session,lobId);
	session->endTransaction(true);
}
#endif
}
