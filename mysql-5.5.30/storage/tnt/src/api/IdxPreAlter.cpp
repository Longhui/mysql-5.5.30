/**
 * 在线修改索引
 *
 * @author 辛颖伟(xinyingwei@corp.netease.com, xinyingwei@163.org)
 */
#include <string>
#include <vector>
#include <algorithm>
#include "api/IdxPreAlter.h"
#include "api/TNTDatabase.h"
#include "util/Portable.h"
#include "util/SmartPtr.h"
#include "api/TNTTable.h"
#include "misc/TableDef.h"
#include "misc/ParFileParser.h"

using namespace std;

namespace tnt {

const char * IdxPreAlter::STR_INDEX = "index";
const char * IdxPreAlter::STR_ON = "on";
const char * IdxPreAlter::STR_DOT = ".";
const char * IdxPreAlter::STR_COMMA = ",";
const char * IdxPreAlter::STR_LEFT_BRACKET = "(";
const char * IdxPreAlter::STR_RIGHT_BRACKET = ")";
const char * IdxPreAlter::STR_COL_DELIMITER = "),";

// 用于解析前缀索引命令
const char * IdxPreAlter::STR_COL_DELIMITER_WITH_PREFIX = ")),";
const char * IdxPreAlter::STR_DOUBLE_RIGHT_BRACKEY = "))";


const char * IdxPreAlter::ALLOC_ADD_SESSION_NAME = "Add index session";
const char * IdxPreAlter::ALLOC_DROP_SESSION_NAME = "Drop index session";

const char * IdxPreAlter::MSG_EXCEPTION_FORMAT_ADD_ERROR = "Add index command format was wrong.";
const char * IdxPreAlter::MSG_EXCEPTION_FORMAT_DROP_ERROR = "Drop index command format was wrong.";
const char * IdxPreAlter::MSG_EXCEPTION_INVALID_COLUMN_NAME = "Invalid column name, or make sure it was exist.";
const char * IdxPreAlter::MSG_EXCEPTION_INVALID_ONLINE_INDEX = "Invalid online index name, or make sure it was available.";

// 前缀错误信息
const char * IdxPreAlter::MSG_EXCEPTION_INVALID_PREFIX = "Invalid online index prefix.";

// 不支持大对象列在线建索引的信息
const char * IdxPreAlter::MSG_EXCEPTION_INVALID_LOB_COLUMN = "Lob or long varchar column was not supported in online operation.";

/**
 * 构造函数
 *
 * @param db		数据库
 * @param conn		数据库连接
 * @param parser	命令解析
 */
IdxPreAlter::IdxPreAlter(TNTDatabase *db, Connection *conn, Parser *parser) {
	assert(NULL != db);
	assert(NULL != conn);
	assert(NULL != parser);
	m_db = db;
	m_conn = conn;
	m_parser = parser;
}

/**
 * 析构函数
 *
 * 关闭表，释放会话和数据库连接，释放解析器。
 */
IdxPreAlter::~IdxPreAlter() {
	if (NULL != m_parser) {
		delete m_parser;
		m_parser = NULL;
	}
}

/**
 * 解析创建索引命令
 *
 * @param add 创建索引所需信息
 *
 * @throw NtseException 若修改操作失败
 */
void IdxPreAlter::parAddCmd(AddIndexInfo &add) throw(NtseException) {
	//命令格式：add index on schema.table idx_name0(name0,name1,...), idx_name1(name2), ...
	m_parser->match(STR_INDEX);
	m_parser->match(STR_ON);

	//解析schema.table idx_name0(name0,name1,...), idx_name1(name2), ...
	try {
		AutoPtr<char> schemaName(System::strdup(m_parser->nextToken()), true);
		add.schemaName = schemaName;

		m_parser->match(STR_DOT);
		AutoPtr<char> tableName(System::strdup(m_parser->nextToken()), true);
		add.tableName = tableName;
		while (true) {
			bool isEnd = false;
			AutoPtr<char> idxName(System::strdup(m_parser->nextToken()), true);
			m_parser->match(STR_LEFT_BRACKET);
			vector<string> attribute;
			vector<u32> prefixArr;

			while (true) {
				AutoPtr<char> attrib(System::strdup(m_parser->nextToken()), true);
				attribute.push_back(string(attrib));
				AutoPtr<char> next(System::strdup(m_parser->nextToken()), true);
				if (System::stricmp(next, STR_LEFT_BRACKET) == 0) {	// 如果属性指定前缀长度
					AutoPtr<char> prefix(System::strdup(m_parser->nextToken()), true);
					u32 prefixLen = Parser::parseInt(prefix);
					if (prefixLen == 0)
						// 过滤前缀长度为0的索引
						NTSE_THROW(NTSE_EC_NOT_SUPPORT, MSG_EXCEPTION_INVALID_PREFIX);
					prefixArr.push_back(prefixLen);
					AutoPtr<char> delimiter(System::strdup(m_parser->nextToken()), true);
					if (System::stricmp(delimiter, STR_DOUBLE_RIGHT_BRACKEY) == 0) {
						isEnd = true;
						break;
					} else if (System::stricmp(delimiter, STR_COL_DELIMITER_WITH_PREFIX) == 0) {
						break;
					} else if (System::stricmp(delimiter, STR_COL_DELIMITER) != 0) {
						NTSE_THROW(NTSE_EC_GENERIC, MSG_EXCEPTION_FORMAT_ADD_ERROR);
					}
				} else {
					prefixArr.push_back(0);
					if (System::stricmp(next, STR_RIGHT_BRACKET) == 0) {
						isEnd = true;
						break;
					} else if (System::stricmp(next, STR_COL_DELIMITER) == 0) {
						break;
					} else if (System::stricmp(next, STR_COMMA) != 0) {
						NTSE_THROW(NTSE_EC_GENERIC, MSG_EXCEPTION_FORMAT_ADD_ERROR);
					}
				}
			}
			Idx maico = {string(idxName), attribute, prefixArr};
			add.idxs.push_back(maico);
			if (isEnd) {
				if (m_parser->hasNextToken()) {
					NTSE_THROW(NTSE_EC_GENERIC, MSG_EXCEPTION_FORMAT_ADD_ERROR);
				}
				break;
			}
		}
	} catch (NtseException &) {
		NTSE_THROW(NTSE_EC_GENERIC, MSG_EXCEPTION_FORMAT_ADD_ERROR);
	}
}

/**
 * 解析删除索引命令
 *
 * @param drop 删除索引所需信息
 *
 * @throw NtseException 若修改操作失败
 */
void IdxPreAlter::parDropCmd(DropIndexInfo &drop) throw(NtseException) {
	//命令格式：drop index on schema.table idx_name0, idx_name1, ...
	m_parser->match(STR_INDEX);
	m_parser->match(STR_ON);

	//解析schema.table idx_name0, idx_name1, ...
	try {
		AutoPtr<char> schemaName(System::strdup(m_parser->nextToken()), true);
		drop.schemaName = schemaName;
		m_parser->match(STR_DOT);

		AutoPtr<char> tableName(System::strdup(m_parser->nextToken()), true);
		drop.tableName = tableName;
		while (true) {
			AutoPtr<char> idxName(System::strdup(m_parser->nextToken()), true);
			drop.idxNames.push_back(string(idxName));
			if (!m_parser->hasNextToken()) {
				break;
			}
			m_parser->match(STR_COMMA);
		}
	} catch (NtseException &) {
		NTSE_THROW(NTSE_EC_GENERIC, MSG_EXCEPTION_FORMAT_DROP_ERROR);
	}
}

/**
 * 在线创建索引
 *
 * @throw NtseException 若修改操作失败
 */
void IdxPreAlter::createOnlineIndex() throw(NtseException) {
	assert((NULL != m_db) && (NULL != m_parser));
	Database *ntsedb = m_db->getNtseDb();
	int i = 0, j = 0;
	size_t mbMinLen = 0, mbMaxLen = 0; // 用于前缀索引计算字符集的字符大小
	AddIndexInfo add;
	parAddCmd(add);
	Session *session = NULL;
	TNTTransaction *trx = NULL;
	TNTTable *table = NULL;
	IndexDef **indexDefs = NULL;
	std::vector<string> parFiles;
	try {
		//获取会话和表实例
		session = ntsedb->getSessionManager()->allocSession(ALLOC_ADD_SESSION_NAME, m_conn);
		trx = m_db->getTransSys()->allocTrx();

		string path = string(".") + NTSE_PATH_SEP + add.schemaName + NTSE_PATH_SEP + add.tableName;
		ParFileParser parser(path.c_str());
		parser.parseParFile(parFiles);		
	} catch (NtseException &e) {
		if (trx != NULL) {
			m_db->getTransSys()->freeTrx(trx);
		}

		ntsedb->getSessionManager()->freeSession(session);
		session = NULL;

		m_db->getTNTSyslog()->log(EL_ERROR, "create OnlineIndex error %s", e.getMessage());
		throw e;
	}

	try {
		for (uint k = 0; k < parFiles.size(); k++) {
			session->setTrans(trx);
			table = m_db->openTable(session, parFiles[k].c_str());
			if (m_conn->isTrx()) {
				trx->startTrxIfNotStarted(m_conn, true);
			} else {
				session->setTrans(NULL);
			}

			indexDefs = (IndexDef **)session->getMemoryContext()->alloc(sizeof(IndexDef *) * add.idxs.size());

			table->lockMeta(session, IL_S, -1, __FILE__, __LINE__);
			if (add.idxs.size() + table->getNtseTable()->getTableDef()->m_numIndice > Limits::MAX_INDEX_NUM) {
				table->unlockMeta(session, table->getMetaLock(session));
				NTSE_THROW(NTSE_EC_TOO_MANY_INDEX, "Too Many Index, Index Limit is %d", Limits::MAX_INDEX_NUM);
			}

			// 检测表中原有的索引中是否包含大对象列
			// TODO: 目前暂不支持包含大对象列的在线索引操作
			for (i = 0; i < table->getNtseTable()->getTableDef()->m_numIndice; i++) {
				if (table->getNtseTable()->getTableDef()->getIndexDef(i)->hasLob()) {
					table->unlockMeta(session, table->getMetaLock(session));
					NTSE_THROW(NTSE_EC_GENERIC, MSG_EXCEPTION_INVALID_COLUMN_NAME);
				}
			}
			
			// 检测新增加的索引的合法性
			for (i = 0; i < (int)add.idxs.size(); i++)
			{
				ColumnDef **columns = new (session->getMemoryContext()->alloc(sizeof(ColumnDef *) * add.idxs[i].attribute.size())) ColumnDef *[add.idxs[i].attribute.size()];
				u32 *prefixlens = new (session->getMemoryContext()->alloc(sizeof(u32) * add.idxs[i].prefixLenArr.size())) u32[add.idxs[i].prefixLenArr.size()];
				for (j = 0; j < (int)add.idxs[i].attribute.size(); j++)
				{
					columns[j] = table->getNtseTable()->getTableDef(false, session)->getColumnDef(add.idxs[i].attribute[j].c_str());
					if (columns[j] == NULL) {
						table->unlockMeta(session, table->getMetaLock(session));
						NTSE_THROW(NTSE_EC_GENERIC, MSG_EXCEPTION_INVALID_COLUMN_NAME);
					}

					if (columns[j]->isLob()){
						table->unlockMeta(session, table->getMetaLock(session));
						NTSE_THROW(NTSE_EC_NOT_SUPPORT, MSG_EXCEPTION_INVALID_LOB_COLUMN);
					}

					Collation::getMinMaxLen(columns[j]->m_collation, &mbMinLen, &mbMaxLen);				
					prefixlens[j] = add.idxs[i].prefixLenArr[j] * mbMaxLen;
					if (prefixlens[j] > 0) {
						// 检测具有前缀的列是否是变长字段
						if (columns[j]->m_type != CT_VARCHAR && columns[j]->m_type != CT_VARBINARY &&
							columns[j]->m_type != CT_CHAR && columns[j]->m_type != CT_BINARY &&
							columns[j]->m_type != CT_SMALLLOB && columns[j]->m_type != CT_MEDIUMLOB) {
								table->unlockMeta(session, table->getMetaLock(session));
								NTSE_THROW(NTSE_EC_NOT_SUPPORT, MSG_EXCEPTION_INVALID_PREFIX);
						}
						// 检测前缀长度的合法性
						if (prefixlens[j] == (columns[j]->m_size - columns[j]->m_lenBytes)) {
							// 前缀长度和变长字段长度相同认为不是前缀索引
							prefixlens[j] = 0; 
						} else if(prefixlens[j] > columns[j]->m_size - columns[j]->m_lenBytes){
							table->unlockMeta(session, table->getMetaLock(session));
							NTSE_THROW(NTSE_EC_NOT_SUPPORT, MSG_EXCEPTION_INVALID_PREFIX);
						}
					} else {
						// 如果超长VARCHAR字段且不指定前缀长度，默认前缀为引擎支持的最大长度
						if (columns[j]->isLongVar())
							prefixlens[j] = Limits::DEF_MAX_VAR_SIZE; 
					}
				}
				indexDefs[i] = new IndexDef(add.idxs[i].idxName.c_str(), (u16)add.idxs[i].attribute.size(), columns, prefixlens, false, false, true);
			}

			//在线创建索引
			m_db->addIndex(session, table, (u16)add.idxs.size(), (const IndexDef **)indexDefs);

			if (m_conn->isTrx()) {
				trx->commitTrx(CS_INNER);
			}
			table->unlockMeta(session, table->getMetaLock(session));

			for (i--; i >= 0; i--) {
				delete indexDefs[i];
			}
			m_db->closeTable(session, table);
			table = NULL;
		}

		m_db->getTransSys()->freeTrx(trx);
		ntsedb->getSessionManager()->freeSession(session);
		session = NULL;
	} catch (NtseException &e) {
		for (i--; i >= 0; i--) {
			delete indexDefs[i];
		}

		if (table != NULL && table->getMetaLock(session) != IL_NO) {
			table->unlockMeta(session, table->getMetaLock(session));
		}

		if (m_conn->isTrx()) {
			assert(session->getTrans() == trx);
			trx->rollbackTrx(RBS_INNER, session);
		}

		if (table) {
			m_db->closeTable(session, table);
		}

		m_db->getTransSys()->freeTrx(trx);
		ntsedb->getSessionManager()->freeSession(session);
		throw e;
	}
}

/**
 * 在线删除索引
 *
 * @throw NtseException 若修改操作失败
 */
void IdxPreAlter::deleteOnlineIndex() throw(NtseException) {
	assert((NULL != m_db) && (NULL != m_parser));
	Database *ntsedb = m_db->getNtseDb();
	DropIndexInfo drop;
	parDropCmd(drop);
	Session *session = NULL;
	TNTTransaction *trx = NULL;
	TNTTable *table = NULL;
	std::vector<string> parFiles;

	try {
		//获取会话和表实例
		session = ntsedb->getSessionManager()->allocSession(ALLOC_DROP_SESSION_NAME, m_conn);
		trx = m_db->getTransSys()->allocTrx();
		session->setTrans(trx);
		string path = string(".") + NTSE_PATH_SEP + drop.schemaName + NTSE_PATH_SEP + drop.tableName;
		ParFileParser parser(path.c_str());
		parser.parseParFile(parFiles);		
	} catch (NtseException &e) {
		if (trx != NULL) {
			m_db->getTransSys()->freeTrx(trx);
		}

		ntsedb->getSessionManager()->freeSession(session);
		session = NULL;

		m_db->getTNTSyslog()->log(EL_PANIC, "Parse the partition table .par file error: %s", e.getMessage());
		throw e;
	}

	try {
		for (uint k = 0; k < parFiles.size(); k++) {
			session->setTrans(trx);
			table = m_db->openTable(session, parFiles[k].c_str());

			if (m_conn->isTrx()) {
				trx->startTrxIfNotStarted(m_conn, true);
			} else {
				session->setTrans(NULL);
			}

			table->lockMeta(session, IL_S, -1, __FILE__, __LINE__);
			vector<uint> ids;
			//验证每一个要删除索引的正确性
			for (uint i = 0; i < drop.idxNames.size(); i++) {
				const IndexDef *indexDef = table->getNtseTable()->getTableDef(false, session)->getIndexDef(drop.idxNames[i].c_str());
				if (indexDef == NULL || indexDef->m_online == false) {
					table->unlockMeta(session, table->getMetaLock(session));
					NTSE_THROW(NTSE_EC_GENERIC, MSG_EXCEPTION_INVALID_ONLINE_INDEX);
				}
				uint id = table->getNtseTable()->getTableDef(false, session)->getIndexNo(drop.idxNames[i].c_str());
				ids.push_back(id);
			}

			//检查权限
			table->getNtseTable()->checkPermission(session, false);

			//在线删除索引
			sort(ids.begin(), ids.end());
			for (int j = ids.size() - 1; j >= 0; --j) {
				m_db->dropIndex(session, table, ids[j]);
			}

			if (m_conn->isTrx()) {
				trx->commitTrx(CS_INNER);
			}
			table->unlockMeta(session, table->getMetaLock(session));

			m_db->closeTable(session, table);
			table = NULL;
		}

		m_db->getTransSys()->freeTrx(trx);
		ntsedb->getSessionManager()->freeSession(session);
		session = NULL;
	} catch (NtseException &e) {
		if (table != NULL && table->getMetaLock(session) != IL_NO) {
			table->unlockMeta(session, table->getMetaLock(session));
		}

		if (m_conn->isTrx()) {
			assert(session->getTrans() == trx);
			trx->rollbackTrx(RBS_INNER, session);
		}

		if (table) {
			m_db->closeTable(session, table);
		}

		m_db->getTransSys()->freeTrx(trx);
		if (session)
			ntsedb->getSessionManager()->freeSession(session);
		throw e;
	}
}


}
