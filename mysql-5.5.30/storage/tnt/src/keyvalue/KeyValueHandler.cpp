/**
* KeyValue服务端代码
*
* @author 廖定柏(liaodingbai@corp.netease.com)
*/

#ifdef NTSE_KEYVALUE_SERVER

#include <vector>
#include <map>
#include <algorithm>
#include "gen-cpp/KV_types.h"
#include <sstream>
#include <boost/regex.hpp>
#include "KeyValueServer.h"
#include "KeyValueHandler.h"
#include "api/Table.h"
#include "api/Database.h"
#include "misc/RecordHelper.h"
#include "misc/Session.h"
#include "misc/Record.h"
#include "misc/Global.h"
#include "misc/TableDef.h"
#include "misc/Profile.h"

#define INVALID_INDEX_NO -1	/** 无效的索引号 */

#define MOD_NUMBER_BY_COND(TYPE, STRING_VALUE, CHANGETYPE, ORIGNAL)	\
	TYPE DELTA = *(TYPE*)STRING_VALUE.c_str();	\
	CHANGETYPE ? (ORIGNAL += DELTA) : (ORIGNAL -= DELTA);	\

using namespace std;

namespace ntse	{

	/**
	*	条件判断中，操作类型对应的字符串表达
	*/
	static const char *conditionOPs[] = {
		"==", ">", "<", ">=", "<=", "<>", "like", "<=>", "IS NULL"
	};

	/** 比较两个填充格式的搜索键的函数对象 */
	class KeyComparator	{
	public:
		KeyComparator(TableDef *tableDef) : m_tableDef(tableDef)	{}

		bool operator()(const SubRecord *a , const SubRecord *b) const {
			return RecordOper::compareKeyPP(m_tableDef, a, b) < 0;
		}
	private:
		TableDef *m_tableDef; /** 表定义 */
	};

	/**
	*	对Key-Value客户端的API统计信息的修改操作的封装
	*/
	class ThreadInfoMonitor {
	public:
		/**
		*	Constructor
		*	@param	table 表信息
		*	@param	method	API的名称
		*/
		ThreadInfoMonitor(const KVTableInfo& table, InvokedMethod method) {
			stringstream strStream;
			strStream<<"./"<<table.m_schemaName<<"/"<<table.m_name;

			currentThreadInfo->status = PROCESS_BEGIN;
			currentThreadInfo->apiStartTime = System::microTime();
			currentThreadInfo->api = method;
			currentThreadInfo->tablePath = strStream.str();
			currentThreadInfo->information.str(""); // 清空内容，非clear()
		}

		/**
		*	析构函数，改变线程局部信息
		*/
		~ThreadInfoMonitor() {
			currentThreadInfo->api = NONE;
			currentThreadInfo->apiStartTime = -1;
			currentThreadInfo->information.str(""); // 清空内容，非clear()
			currentThreadInfo->status = FREE;
		}

		/**
		*	设置线程局部信息的状态为处理进行中，标志参数解析完毕
		*/
		inline void setProcessingStatus() {
			currentThreadInfo->status = PROCESSING;
		}

		/**
		*	设置线程局部信息的状态为处理结束状态
		*/
		inline void setEndStatus() {
			currentThreadInfo->status = PROCESS_END;
		}

		/**
		*	设置线程局部信息的状态为异常状态
		*/
		inline void setExceptionStatus() {
			currentThreadInfo->status = PROCESS_EXCEPTION;
		}
	};

	/**
	 *	keyvalue主动管理表锁的实现类
	 */
	class TblLocker {
	public:
		/**
		 *	构造函数
		 *	@param	session 会话对象
		 *	@param	table	表对象
		 *	@param	opType	对表的操作类型
		 *	@param	config	数据库的配置函数
		 */
		TblLocker(Session *session, Table *table, OpType opType, Config *config)
			: m_table(table), m_session(session), m_opType(opType) {
			bool metaLocked = false;
			try {
				m_table->lockMeta(m_session, IL_S, config->m_tlTimeout * 1000, __FILE__, __LINE__);
				metaLocked = true;
				m_table->lock(m_session, m_opType == OP_READ ? IL_IS: IL_IX, config->m_tlTimeout * 1000, __FILE__, __LINE__);
				m_currentMode = (m_opType == OP_READ) ? IL_IS : IL_IX;
			} catch (NtseException &e) {
				if (metaLocked) {
					m_table->unlockMeta(m_session, IL_S);
				}
				SERVER_EXCEPTION_THROW(exceptToKVErrorCode(e.getErrorCode()), e.getMessage())
			}
		}

		/**
		 *	析构函数
		 *	对加锁的表进行释放
		 */
		~TblLocker()	{
			m_table->unlock(m_session, m_currentMode);
			m_table->unlockMeta(m_session, IL_S);
		}

		/**
		 *	@pre	该表已经加锁
		 *
		 *	@param	clientVersion	客户端操作表的版本号
		 *
		 *	得到表的定义
		 */
		TableDef* getTableDef(u64 clientVersion) {
			TableDef* tableDef = m_table->getTableDef();
			if (tableDef->m_version != clientVersion) {
				SERVER_EXCEPTION_THROW(KVErrorCode::KV_EC_TBLDEF_NOT_MATCH, "The version of table mismatched.")
			}
			return tableDef;
		}
	private:
		Table	*m_table;			/** 表对象 */
		Session *m_session;		/** 会话对象 */
		OpType	m_opType;		/** 操作类型 */
		ILMode	m_currentMode;	/**	当前表已经加上的锁类型 */
	};


	/**
	*	构造函数，创建session和connection
	*	@param	database
	*	@param	stats
	*/
	KeyValueHandler::KeyValueHandler(Database *database)
		: m_database(database)	{
#ifdef NTSE_UNIT_TEST
			currentThreadInfo = new ThreadLocalInfo();
			currentThreadInfo->connection = m_database->getConnection(false);
#endif
	}

	/**
	*	析构函数，释放资源
	*/
	KeyValueHandler::~KeyValueHandler()	{
#ifdef NTSE_UNIT_TEST
		m_database->freeConnection(currentThreadInfo->connection);
		delete currentThreadInfo;
		currentThreadInfo = NULL;
#endif
	}

	/**
	*	获取主键=key的记录，返回该记录的attrs这些属性.
	*	
	*	@exception	ServerException	打开表出错，或者调用NTSE接口出现异常等。
	*
	*	@param	table	表信息
	*	@param	key		主键key	
	*	@param	attrs	将要取值的属性
	*	@param	version	表版本
	*
	*	@return	attrs属性对应的值。第一项为空值位图，后面项则
	*			对应各个属性值。
	*/
	void KeyValueHandler::get(vector<string> & _return, const KVTableInfo& table, const Attrs& key,
		const vector<int16_t> & attrs, const int64_t version) throw (ServerException)	{
			ThreadInfoMonitor infoMonitor(table, GET_API);

			/** Profile信息 */
			PROFILE(PI_KeyValue_get);

			SesSavePoint sessionSP(m_database, currentThreadInfo->connection);
			Session *session = sessionSP.getSession();

			Table *currentTable = openTable(session, table);

			/**	表锁管理，以下API类同 */
			TblLocker tblLocker(session, currentTable, OP_READ, m_database->getConfig());
			TableDef* tableDef = tblLocker.getTableDef(version);

			/**	序列化参数信息 */
			serializeAttributes(tableDef, key.attrlist, true);
			serializeAttrNOs(attrs);
			infoMonitor.setProcessingStatus();

			ColList colList;
			colList.m_size = attrs.size();
			colList.m_cols = (u16*)session->getMemoryContext()->alloc(attrs.size() * sizeof(u16));
			copy(attrs.begin(), attrs.end(), colList.m_cols);

			byte *mysqlRow = (byte*)session->getMemoryContext()->calloc(tableDef->m_maxRecSize * sizeof(byte));

			/*
			 *	索引扫描
			 */
			TblScan *tblScan = getTblScanByKey(session, currentTable, key.attrlist, colList, OP_READ);
			bool exist = false;
			try {
				exist = currentTable->getNext(tblScan, mysqlRow);

				currentTable->endScan(tblScan);
			} catch (NtseException &e) {
				/**	设置局部线程信息 */
				currentTable->endScan(tblScan);

				++KeyValueServer::m_serverStats.getFailed;
				infoMonitor.setExceptionStatus();

				SERVER_EXCEPTION_THROW(exceptToKVErrorCode(e.getErrorCode()), e.getMessage())
			}

			/*
			 *	存在记录的话，取出返回，结果都是二进制
			 */
			if (exist) {
				extractResult(session, tableDef, mysqlRow, attrs, _return);
				++KeyValueServer::m_serverStats.getHits;
			} else	{
				++KeyValueServer::m_serverStats.getMisses;
			}
			infoMonitor.setEndStatus();
	}

	/**
	*	获取主键in (keys)的记录，返回该记录的attrs这些属性
	*	
	*	@exception	ServerException	打开表出错，或者调用NTSE接口出现异常等。
	*
	*	@param	table	表信息
	*	@param	keys	主键key集合
	*	@param	attrs	将要取值的属性
	*	@param	version	表版本
	*
	*	@return	返回所有keys对应的记录。map中第一项是记录结果对应的key在keys中的序列号，第二项是属性对应的值集合（与get相同）
	*/
	void KeyValueHandler::multi_get(map<int16_t, std::vector<std::string> > & _return, const KVTableInfo& table,
		const vector<Attrs> & keys, const vector<int16_t> & attrs, const int64_t version) throw (ServerException)	{
			ThreadInfoMonitor infoMonitor(table, MULTI_GET_API);

			/** Profile信息 */
			PROFILE(PI_KeyValue_multiGet);

			SesSavePoint sessionSP(m_database, currentThreadInfo->connection);
			Session *session = sessionSP.getSession();

			Table *currentTable = openTable(session, table);
			
			TblLocker tblLocker(session, currentTable, OP_READ, m_database->getConfig());
			TableDef* tableDef = tblLocker.getTableDef(version);

			typedef pair<SubRecord*, IndexOfKey> keyPair;
			typedef vector<Attrs>::const_iterator keyIt;
			typedef map<SubRecord*, IndexOfKey, KeyComparator>::iterator keyMapIt;

			for (keyIt it = keys.begin(); it != keys.end(); ++it) {
				serializeAttributes(tableDef, it->attrlist, true);
			}
			serializeAttrNOs(attrs);
			infoMonitor.setProcessingStatus();

			int indexNo = tableDef->getIndexNo(tableDef->m_pkey->m_name);

			/** 根据key的大小，对map进行排序 */
			map<SubRecord*, IndexOfKey, KeyComparator> keyMap((KeyComparator(tableDef)));

			/** 插入key到map中，map会自动排序 */
			size_t index = 0;
			IndexDef *pkIndex = tableDef->m_pkey;

			/**	空间统一分配，然后逐个初始化 */
			SubRecord *p = (SubRecord*)session->getMemoryContext()->alloc(sizeof(SubRecord) * keys.size());
			for (keyIt it = keys.begin(); it != keys.end(); ++index, ++it) {
				SubRecord *key = new (p + index)SubRecord(KEY_PAD, pkIndex->m_numCols, pkIndex->m_columns,
					(byte *)session->getMemoryContext()->alloc(pkIndex->m_maxKeySize), pkIndex->m_maxKeySize);

				transKey(tableDef, it->attrlist, key);
				keyMap.insert(keyPair(key, index));
			}

			u16 *readCols = (u16 *)session->getMemoryContext()->alloc(attrs.size() * sizeof(u16));
			copy(attrs.begin(), attrs.end(), readCols);

			byte *mysqlRow = (byte*)session->getMemoryContext()->calloc(tableDef->m_maxRecSize * sizeof(byte));

			/** 根据key的大小顺序，取出RowId */
			for (keyMapIt it = keyMap.begin(); it != keyMap.end(); ++it) {
				TblScan *indexScan = NULL;
				try {
					IndexScanCond cond(indexNo, it->first, true, true, true);
					sort(readCols, readCols + attrs.size());

					indexScan = currentTable->indexScan(session, OP_READ, &cond, attrs.size(), readCols, false);
					bool exist = currentTable->getNext(indexScan, mysqlRow);
					currentTable->endScan(indexScan);

					if (exist) {
						++KeyValueServer::m_serverStats.getHits;

						vector<string> subResult;
						extractResult(session, tableDef, mysqlRow, attrs, subResult);
						_return.insert(make_pair(it->second, subResult));
					} else	{
						++KeyValueServer::m_serverStats.getMisses;
					}
				} catch (NtseException &e) {
					currentTable->endScan(indexScan);
					keyMap.clear();
					infoMonitor.setExceptionStatus();

					SERVER_EXCEPTION_THROW(exceptToKVErrorCode(e.getErrorCode()), e.getMessage())
				}
			}
			keyMap.clear();
			infoMonitor.setEndStatus();
	}

	/**
	*	插入一条记录，只在指定主键不存在时插入
	*	
	*	@exception	ServerException	打开表出错，或者调用NTSE接口出现异常等。
	*
	*	@param	table	表信息
	*	@param	key		主键key
	*	@param	values	插入记录对应属性的值
	*	@param	version	表版本
	*
	*	@return	返回操作影响的记录行数（put成功返回1，否则0或者异常）
	*/
	int8_t KeyValueHandler::put(const KVTableInfo& table, const Attrs& key, const Attrs& values,
		const int64_t version) throw (ServerException)	{
			/** 线程局部信息 */
			ThreadInfoMonitor infoMonitor(table, PUT_API);

			/** Profile信息 */
			PROFILE(PI_KeyValue_put);

			SesSavePoint sessionSP(m_database, currentThreadInfo->connection);
			Session *session = sessionSP.getSession();

			Table *currentTable = openTable(session, table);

			TblLocker tblLocker(session, currentTable, OP_WRITE, m_database->getConfig());
			TableDef* tableDef = tblLocker.getTableDef(version);

			serializeAttributes(tableDef, key.attrlist, true);
			serializeAttributes(tableDef, values.attrlist, false);
			infoMonitor.setProcessingStatus();

			set<u16> existCols;		/**	参数信息中包括的列 */
			byte *mysqlRow = (byte*)session->getMemoryContext()->calloc(tableDef->m_maxRecSize * sizeof(byte));
			buildRedRecord(tableDef, mysqlRow, key.attrlist, &existCols);

			Bitmap valueBitmap((void*)values.bmp.c_str(), values.bmp.size() * 8);
			buildRedRecord(tableDef, mysqlRow, values.attrlist, &existCols, &valueBitmap, true);

			int8_t effectRow = 0;
			try {
				uint duplicateRow;
				RowId newRowId = currentTable->insert(session, mysqlRow, &duplicateRow, false);
				if (newRowId != INVALID_ROW_ID) {
					++KeyValueServer::m_serverStats.putSuccess;
					effectRow = 1;
				} else {
					++KeyValueServer::m_serverStats.putConflict;
				}
			} catch (NtseException &e) {
				++KeyValueServer::m_serverStats.putFailed;
				infoMonitor.setExceptionStatus();

				SERVER_EXCEPTION_THROW(exceptToKVErrorCode(e.getErrorCode()), e.getMessage())
			}

			infoMonitor.setEndStatus();
			return effectRow;
	}

	/**
	*	设置一条记录，在主键不存在时插入，存在时替换
	*	
	*	@exception	ServerException	打开表出错，或者调用NTSE接口出现异常等。
	*
	*	@param	table	表信息
	*	@param	key		主键key
	*	@param	values	set记录对应属性的值
	*	@param	version	表版本
	*
	*	@return	返回操作影响的记录行数（put或者update成功返回1，否则0或者异常）
	*/
	int8_t KeyValueHandler::setrec(const KVTableInfo& table, const Attrs& key, const Attrs& values,
		const int64_t version) throw (ServerException)	{
			/** 线程局部信息 */
			ThreadInfoMonitor infoMonitor(table, SET_API);

			/** Profile信息 */
			PROFILE(PI_KeyValue_set);

			SesSavePoint sessionSP(m_database, currentThreadInfo->connection);
			Session *session = sessionSP.getSession();

			Table *currentTable = openTable(session, table);

			TblLocker tblLocker(session, currentTable, OP_WRITE, m_database->getConfig());
			TableDef* tableDef = tblLocker.getTableDef(version);

			serializeAttributes(tableDef, key.attrlist, true);
			serializeAttributes(tableDef, values.attrlist, false);
			infoMonitor.setProcessingStatus();

			set<u16> existCols;		/**	参数信息中包括的列 */

			byte *mysqlRow = (byte*)session->getMemoryContext()->calloc(tableDef->m_maxRecSize * sizeof(byte));
			buildRedRecord(tableDef, mysqlRow, key.attrlist, &existCols);

			Bitmap valueBitmap((void*)values.bmp.c_str(), values.bmp.size() * 8);
			buildRedRecord(tableDef, mysqlRow, values.attrlist, &existCols, &valueBitmap, true);

			bool success = false;
			IUSequence *iuSeq = NULL;
			try {
				/** 尝试insert */
				iuSeq = currentTable->insertForDupUpdate(session, mysqlRow, false);
				success = true;
				if (iuSeq) {	/** 如果存在冲突记录，则对冲突记录采取更新操作 */
					++KeyValueServer::m_serverStats.putConflict;

					set<u16> colSet;
					ColList colList = extractColList(values.attrlist, colSet, session);

					success = currentTable->updateDuplicate(iuSeq, mysqlRow, colList.m_size, colList.m_cols, NULL);

					success ? ++KeyValueServer::m_serverStats.updateSuccess : ++KeyValueServer::m_serverStats.updateFailed;
				} else {
					++KeyValueServer::m_serverStats.putSuccess;
				}
			} catch (NtseException &e) {
				(iuSeq == NULL) ? ++KeyValueServer::m_serverStats.putFailed : ++KeyValueServer::m_serverStats.updateFailed;
				infoMonitor.setExceptionStatus();

				SERVER_EXCEPTION_THROW(exceptToKVErrorCode(e.getErrorCode()), e.getMessage())
			}

			infoMonitor.setEndStatus();
			return success;
	}

	/**
	*	更新整条记录，只在主键存在时替换
	*	
	*	@exception	ServerException	打开表出错，或者调用NTSE接口出现异常等。
	*
	*	@param	table	表信息
	*	@param	key		主键key
	*	@param	values	更新列对应的属性值
	*	@param	version	表版本
	*
	*	@return	返回操作影响的记录行数（update成功返回1，否则0或者异常）
	*/
	int8_t KeyValueHandler::replace(const KVTableInfo& table, const Attrs& key, const Attrs& values,
		const int64_t version) throw (ServerException)	{
			/** 线程局部信息 */
			ThreadInfoMonitor infoMonitor(table, REPLACE_API);

			/** Profile信息 */
			PROFILE(PI_KeyValue_replace);

			SesSavePoint sessionSP(m_database, currentThreadInfo->connection);
			Session *session = sessionSP.getSession();

			Table *currentTable = openTable(session, table);
			
			TblLocker tblLocker(session, currentTable, OP_UPDATE, m_database->getConfig());
			TableDef* tableDef = tblLocker.getTableDef(version);

			serializeAttributes(tableDef, key.attrlist, true);
			serializeAttributes(tableDef, values.attrlist, false);
			infoMonitor.setProcessingStatus();

			set<u16> colSet;
			// update操作相关的列
			ColList updateCols = extractColList(values.attrlist, colSet, session);

			getIndiceCols(tableDef, colSet);
			// 构造需要得到的所有列的集合
			ColList colList = extractColList(values.attrlist, colSet, session);

			byte *mysqlRow = (byte*)session->getMemoryContext()->calloc(tableDef->m_maxRecSize * sizeof(byte));

			bool success = false;
			TblScan *tblScan = getTblScanByKey(session, currentTable, key.attrlist, colList, OP_UPDATE);
			try {
				bool exist = currentTable->getNext(tblScan, mysqlRow);

				if (exist) {
					// 保存记录的前像
					byte *oldRow = (byte*)session->getMemoryContext()->dup(mysqlRow, tableDef->m_maxRecSize * sizeof(byte));

					Bitmap valueBitmap((void*)values.bmp.c_str(), values.bmp.size() * 8);
					buildRedRecord(tableDef, mysqlRow, values.attrlist, NULL, &valueBitmap);

					tblScan->setUpdateColumns(updateCols.m_size, updateCols.m_cols);
					success = currentTable->updateCurrent(tblScan, mysqlRow, NULL, oldRow);

					success ? ++KeyValueServer::m_serverStats.updateSuccess : ++KeyValueServer::m_serverStats.updateFailed;
				}
			} catch (NtseException &e) {
				currentTable->endScan(tblScan);
				++KeyValueServer::m_serverStats.updateFailed;
				infoMonitor.setExceptionStatus();

				SERVER_EXCEPTION_THROW(exceptToKVErrorCode(e.getErrorCode()), e.getMessage())
			} catch(ServerException &se) {
				currentTable->endScan(tblScan);
				++KeyValueServer::m_serverStats.updateFailed;
				infoMonitor.setExceptionStatus();

				throw se;
			}

			currentTable->endScan(tblScan);
			infoMonitor.setEndStatus();
			return success;
	}

	/**
	*	删除一条记录
	*	
	*	@exception	ServerException	打开表出错，或者调用NTSE接口出现异常等。
	*
	*	@param	table	表信息
	*	@param	key	主键key
	*	@param	version	表版本
	*
	*	@return	返回操作影响的记录行数（删除成功返回1，否则0或者异常）
	*/
	int8_t KeyValueHandler::remove(const KVTableInfo& table, const Attrs& key, const int64_t version) throw (ServerException)	{
		/** 线程局部信息 */
		ThreadInfoMonitor infoMonitor(table, REMOVE_API);

		/** Profile信息 */
		PROFILE(PI_KeyValue_remove);

		SesSavePoint sessionSP(m_database, currentThreadInfo->connection);
		Session *session = sessionSP.getSession();

		Table *currentTable = openTable(session, table);

		TblLocker tblLocker(session, currentTable, OP_DELETE, m_database->getConfig());
		TableDef* tableDef = tblLocker.getTableDef(version);

		serializeAttributes(tableDef, key.attrlist, true);
		infoMonitor.setProcessingStatus();

		set<u16> colSet;
		/*
		 *	必须得到所有的索引列，不然发生删除时，更新索引，索引值将不正确
		 */
		getIndiceCols(tableDef, colSet);
		ColList colList = extractColList(key.attrlist, colSet, session);

		TblScan* tblScan = getTblScanByKey(session, currentTable, key.attrlist, colList, OP_DELETE);

		int effectRows = 0;
		try {
			byte *mysqlRow = (byte*)session->getMemoryContext()->calloc(tableDef->m_maxRecSize * sizeof(byte));
			bool exist = currentTable->getNext(tblScan, mysqlRow);

			/** 存在则删除 */
			if (exist) {
				currentTable->deleteCurrent(tblScan);
				effectRows = 1;
				++KeyValueServer::m_serverStats.deleteSuccess;
			}

		} catch (NtseException &e) {
			currentTable->endScan(tblScan);
			++KeyValueServer::m_serverStats.deleteFailed;
			infoMonitor.setExceptionStatus();

			SERVER_EXCEPTION_THROW(exceptToKVErrorCode(e.getErrorCode()), e.getMessage())
		}

		currentTable->endScan(tblScan);
		infoMonitor.setEndStatus();
		return effectRows;
	}

	/**
	*	条件性更新一条记录。可同时更新多个属性，每个属性可进行以下四类更新：
	*		1.	set v:将该属性的值设为v
	*		2.	incr/decr delta: 将该属性的值增加或减少delta，只在属性为整数类型时才可用。
	*		3.	append suffiix: 将该属性的值后面追加suffix，只在属性为字符串类型时才可用。
	*		4.	prepend prefix: 将该属性的值前面追加prefix，只在属性为字符串类型时才可用。
	*	
	*	@exception	ServerException	打开表出错，或者调用NTSE接口出现异常等。
	*
	*	@param	table	表信息
	*	@param	key		主键key
	*	@param	conds	更新条件
	*	@param	updatemodes	更新方式
	*	@param	version 表版本
	*
	*	@return	返回操作影响的记录行数（更新成功返回1，否则0或者异常）
	*/
	int8_t KeyValueHandler::update(const KVTableInfo& table, const Attrs& key, const vector<Cond> & conds,
		const vector<DriverUpdateMode> &updatemodes, const int64_t version) throw (ServerException)	{
			/** 线程局部信息 */
			ThreadInfoMonitor infoMonitor(table, UPDATE_API);

			/** Profile信息 */
			PROFILE(PI_KeyValue_update);

			SesSavePoint sessionSP(m_database, currentThreadInfo->connection);
			Session *session = sessionSP.getSession();
			Table *currentTable = openTable(session, table);

			TblLocker tblLocker(session, currentTable, OP_UPDATE, m_database->getConfig());
			TableDef* tableDef = tblLocker.getTableDef(version);

			serializeAttributes(tableDef, key.attrlist, true);
			serializeConditions(tableDef, conds);
			serializeAttributes(tableDef, updatemodes, false);
			infoMonitor.setProcessingStatus();

			set<u16> colsSet;

			// update操作相关的列
			ColList updateCols = extractColList(updatemodes, colsSet, session);

			getIndiceCols(tableDef, colsSet);

			/*
			*	由于判断条件里面可能引用到updatemodes中没有涉及到的列，所以需要提取conds中涉及
			*	的列。
			*/
			for (vector<Cond>::const_iterator it = conds.begin(); it != conds.end(); ++it) {
				assert(it->valueOne.dataType == DataType::KV_COL);
				colsSet.insert(*(u16*)it->valueOne.dataValue.c_str());

				if (it->valueTwo.dataType == DataType::KV_COL) {
					colsSet.insert(*(u16*)it->valueTwo.dataValue.c_str());
				}
			}

			/**	得到操作中涉及到的所有列集合 */
			ColList colList = extractColList(updatemodes, colsSet, session);

			TblScan* tblScan = getTblScanByKey(session, currentTable, key.attrlist, colList, OP_UPDATE);

			bool success = false;
			try {
				byte *mysqlRow = (byte*)session->getMemoryContext()->calloc(tableDef->m_maxRecSize * sizeof(byte));

				bool exist = currentTable->getNext(tblScan, mysqlRow);

				/** 如果该记录存在，条件性更新 */
				if (exist) {
					if(validateByCond(tableDef, mysqlRow, conds))	{ /** 判定条件 */
						++KeyValueServer::m_serverStats.updateConfirmedByCond;

						// 保存记录的前像
						byte *oldRow = (byte*)session->getMemoryContext()->dup(mysqlRow, tableDef->m_maxRecSize * sizeof(byte));
						updateRowByMode(tableDef, mysqlRow, updatemodes);

						tblScan->setUpdateColumns(updateCols.m_size, updateCols.m_cols);
						success = currentTable->updateCurrent(tblScan, mysqlRow, NULL, oldRow);

						success ? ++KeyValueServer::m_serverStats.updateSuccess : ++KeyValueServer::m_serverStats.updateFailed;
					}
				} 
			} catch (NtseException &e) {
				currentTable->endScan(tblScan);
				++KeyValueServer::m_serverStats.updateFailed;
				infoMonitor.setExceptionStatus();

				SERVER_EXCEPTION_THROW(exceptToKVErrorCode(e.getErrorCode()), e.getMessage())
			} catch(ServerException &se) {
				currentTable->endScan(tblScan);
				++KeyValueServer::m_serverStats.updateFailed;
				infoMonitor.setExceptionStatus();

				throw se;
			}

			currentTable->endScan(tblScan);
			infoMonitor.setEndStatus();
			return success;
	}

	/**
	*	插入或更新一条记录，若指定主键不存在则插入，若存在则无条件性更新
	*	
	*	@exception	ServerException	打开表出错，或者调用NTSE接口出现异常等。
	*
	*	@param	table	表信息
	*	@param	key		主键key
	*	@param	updatemodes	更新方式
	*	@param	version	表版本
	*
	*	@return	返回操作影响的记录行数（put或者更新成功返回1，否则0或者异常）
	*/
	int8_t KeyValueHandler::put_or_update(const KVTableInfo& table, const Attrs& key, const Attrs& values,
		const vector<DriverUpdateMode> & updatemodes, const int64_t version) throw (ServerException)	{
			/** 线程局部信息 */
			ThreadInfoMonitor infoMonitor(table, PUT_OR_UPDATE_API);

			/** Profile信息 */
			PROFILE(PI_KeyValue_putOrUpdate);

			SesSavePoint sessionSP(m_database, currentThreadInfo->connection);
			Session *session = sessionSP.getSession();

			Table *currentTable = openTable(session, table);

			TblLocker tblLocker(session, currentTable, OP_WRITE, m_database->getConfig());
			TableDef* tableDef = tblLocker.getTableDef(version);

			serializeAttributes(tableDef, key.attrlist, true);
			serializeAttributes(tableDef, values.attrlist, false);
			serializeAttributes(tableDef, updatemodes, false);
			infoMonitor.setProcessingStatus();

			set<u16> existCols;		/**	参数信息中包括的列 */

			byte *mysqlRow = (byte*)session->getMemoryContext()->calloc(tableDef->m_maxRecSize * sizeof(byte));
			buildRedRecord(tableDef, mysqlRow, key.attrlist, &existCols);

			// 构造插入记录
			Bitmap valueBitmap((void*)values.bmp.c_str(), values.bmp.size() * 8);
			buildRedRecord(tableDef, mysqlRow, values.attrlist, &existCols, &valueBitmap, true);

			bool success = false;

			IUSequence *iuSeq = NULL;
			try {
				/** 尝试插入记录 */
				iuSeq = currentTable->insertForDupUpdate(session, mysqlRow, false);

				success = true;
				if (iuSeq) {	/** 如果存在冲突记录，则无条件地更新该记录 */
					++KeyValueServer::m_serverStats.putConflict;

					success = false;
					byte *dupRow = (byte*)session->getMemoryContext()->calloc(tableDef->m_maxRecSize * sizeof(byte));	/** 保存冲突记录内容 */
					iuSeq->getDupRow(dupRow);
					updateRowByMode(tableDef, dupRow, updatemodes);

					set<u16> emptySet;
					ColList colList = extractColList(updatemodes, emptySet, session);
					success = currentTable->updateDuplicate(iuSeq, dupRow, colList.m_size, colList.m_cols);

					success ? ++KeyValueServer::m_serverStats.updateSuccess : ++KeyValueServer::m_serverStats.updateFailed;
				} else {
					++KeyValueServer::m_serverStats.putSuccess;
				}
			} catch (NtseException &e) {
				(iuSeq == NULL) ? ++KeyValueServer::m_serverStats.putFailed : ++KeyValueServer::m_serverStats.updateFailed;
				infoMonitor.setExceptionStatus();

				SERVER_EXCEPTION_THROW(exceptToKVErrorCode(e.getErrorCode()), e.getMessage())
			} catch(ServerException &se) {
				++KeyValueServer::m_serverStats.updateFailed;
				infoMonitor.setExceptionStatus();

				throw se;
			}

			infoMonitor.setEndStatus();
			return success;
	}

	/**
	*	获得表定义
	*	
	*	@exception	ServerException	打开表出错，或者调用NTSE接口出现异常等。
	*
	*	@param	table	表信息
	*
	*	@return	返回获得的表定义
	*/
	void KeyValueHandler::getTableDef(KVTableDef& _return, const KVTableInfo& table) throw (ServerException)	{
		/** 线程局部信息 */
		ThreadInfoMonitor infoMonitor(table, GET_TABLEDEF_API);

		/** Profile信息 */
		PROFILE(PI_KeyValue_getTableDef);

		SesSavePoint sessionSP(m_database, currentThreadInfo->connection);
		Session *session = sessionSP.getSession();

		Table *currentTable = openTable(session, table);

		/**	只加元数据锁即可 */
		try{
			currentTable->lockMeta(session, IL_S, m_database->getConfig()->m_tlTimeout * 1000, __FILE__, __LINE__);
		} catch (NtseException &e) {
			SERVER_EXCEPTION_THROW(exceptToKVErrorCode(e.getErrorCode()), e.getMessage())
		}

		TableDef* tableDef = currentTable->getTableDef();
		IndexDef *pkIndex = tableDef->m_pkey;

		_return.m_name = table.m_name;
		_return.version = (s64)tableDef->m_version;
		_return.m_schemaName = table.m_schemaName;
		_return.m_bmBytes = tableDef->m_bmBytes;

		if (!pkIndex) {
			currentTable->unlockMeta(session, IL_S);
			SERVER_EXCEPTION_THROW(KVErrorCode::KV_EC_KEY_ERROR, "Table does not have primary key.")
		}

		/** IndexDef */
		_return.m_pkey.m_name = pkIndex->m_name;
		_return.m_pkey.m_columns.resize(pkIndex->m_numCols);
		copy(pkIndex->m_columns, pkIndex->m_columns + pkIndex->m_numCols, _return.m_pkey.m_columns.begin());

		/** ColumnDef */
		for (u16 i = 0; i < tableDef->m_numCols; ++i) {
			KVColumnDef colDef;
			ColumnDef *ntseColDef = tableDef->m_columns[i];
			colDef.m_name = ntseColDef->m_name;
			colDef.m_no = ntseColDef->m_no;
			colDef.m_nullable = ntseColDef->m_nullable;
			colDef.m_nullBitmapOffset = ntseColDef->m_nullBitmapOffset;
			colDef.m_type = convertNtseColType(ntseColDef->m_type);
			_return.m_columns.push_back(colDef);
		}
		currentTable->unlockMeta(session, IL_S);
		infoMonitor.setEndStatus();
	}

	/**
	*	根据条件验证给定记录
	*
	*	@param	tableDef	记录所在的表的定义
	*	@param	oldContent	验证记录，使用REC_MYSQL格式
	*	@param	conds		验证条件
	*
	*	@return	如果记录通过验证返回true，否则false
	*/
	bool KeyValueHandler::validateByCond(TableDef *tableDef, byte *oldContent, const vector<Cond> &conds)	{
		for (vector<Cond>::const_iterator it = conds.begin(); it != conds.end(); ++it) {
			assert(it->valueOne.dataType == DataType::KV_COL);

			u16 colNumOne = *(u16*)it->valueOne.dataValue.c_str();
			ColumnDef *colDefOne = tableDef->m_columns[colNumOne];
			ColumnType colTypeOne = colDefOne->m_type;

			/*
			 *	如果是IS NULL操作，直接判断
			 */
			if (it->op == Op::ISNULL) {
				if (!RedRecord::isNull(tableDef, oldContent, colNumOne)) {
					return false;
				}
				continue;
			}

			/**
			*	获取判定值一
			*	数值类型的列统一用double类型存储，减少冗余代码
			*/
			double numberOne;	
			bool numberOneExist = false;
			string stringOne;	
			bool stringOneExist = false;
			void *memStreamOne = NULL;
			bool memOneExist = false;
			size_t memLengthOne = 0;

			extractCondValues(tableDef, oldContent, colNumOne, colTypeOne, &numberOne, &numberOneExist, 
				&stringOne, &stringOneExist, &memStreamOne, &memLengthOne, &memOneExist);

			/**
			*	获取判定值二
			*/
			ColumnDef *colDefTwo = NULL;
			DataType::type typeTwo = it->valueTwo.dataType;

			double numberTwo;	
			bool numberTwoExist = false;
			string stringTwo = it->valueTwo.dataValue;	
			bool stringTwoExist = false;
			void *memStreamTwo = NULL;	
			bool memeTwoExist = false;
			size_t memLengthTwo = 0;

			/*
			 *	如果判断条件中第二值也是表列的值，直接从记录中获取
			 */
			if (typeTwo == DataType::KV_COL) {
				colDefTwo = tableDef->m_columns[*(u16*)it->valueTwo.dataValue.c_str()];
				extractCondValues(tableDef, oldContent, *(u16*)it->valueTwo.dataValue.c_str(), colDefTwo->m_type,
					&numberTwo, &numberTwoExist, &stringTwo, &stringTwoExist, &memStreamTwo, &memLengthTwo, &memeTwoExist);
			} else if (it->op != Op::NULLSAFEEQ || typeTwo != DataType::KV_NULL) {
				if (typeTwo >= DataType::KV_TINYINT && typeTwo <= DataType::KV_DOUBLE) {
					switch(typeTwo)	{
						case DataType::KV_TINYINT:
							numberTwo = *(s8*)stringTwo.c_str();
							break;
						case DataType::KV_SMALLINT:
							numberTwo = *(s16*)stringTwo.c_str();
							break;
						case DataType::KV_MEDIUMINT:
						case DataType::KV_INT:
							numberTwo = *(s32*)stringTwo.c_str();
							break;
						case DataType::KV_BIGINT:
							numberTwo = *(s64*)stringTwo.c_str();
							break;
						case DataType::KV_FLOAT:
							numberTwo = *(float*)stringTwo.c_str();
							break;
						case DataType::KV_DOUBLE:
							numberTwo = *(double*)stringTwo.c_str();
							break;
					}
					numberTwoExist = true;
				} else if (typeTwo == DataType::KV_CHAR || typeTwo == DataType::KV_VARCHAR) {
					stringTwoExist = true;
				} else if(typeTwo == DataType::KV_BINARY || typeTwo == DataType::KV_VARBINARY || typeTwo == DataType::KV_BLOB) {
					memeTwoExist = true;
					memStreamTwo = (void*)stringTwo.c_str();
					memLengthTwo = stringTwo.size();
				}
			}

			/**
			*	开始判定
			*/
			switch(it->op)	{
				case Op::EQ:
				case Op::GRATER:
				case Op::LESS:
				case Op::EQGRATER:
				case Op::EQLESS:
				case Op::NOTEQ:
					{
						/** 数值比较 */
						if (colTypeOne == CT_TINYINT || colTypeOne == CT_SMALLINT || colTypeOne == CT_INT
							|| colTypeOne == CT_BIGINT || colTypeOne == CT_MEDIUMINT
							|| colTypeOne == CT_FLOAT || colTypeOne == CT_DOUBLE) {								
								if (!numberOneExist|| !numberTwoExist) {
									if (it->op == Op::NOTEQ) return true;
									return false;
								}					
								if (!compareNumber(numberOne, numberTwo, it->op)) {
									return false;
								}
								break;
						} else if (colTypeOne == CT_CHAR || colTypeOne == CT_VARCHAR) {
							/** 字符串比较 */						
							if (!stringOneExist|| !stringTwoExist) {
								if (it->op == Op::NOTEQ) return true;
								return false;
							}
							if (!compareString(stringOne, stringTwo, it->op)) {
								return false;
							}
							break;
						} else if ((colTypeOne == CT_BINARY || colTypeOne == CT_VARBINARY || colTypeOne == CT_SMALLLOB
							|| colTypeOne == CT_MEDIUMLOB) && (it->op == Op::EQ || it->op == Op::NOTEQ) ) {
								/** 内存内容比较 */
								
								if (!memOneExist|| !memeTwoExist) {
									if (it->op == Op::NOTEQ) return true;
									return false;
								}
								bool isNotEq = it->op == Op::NOTEQ ? true : false; 
								int result = memcmp(memStreamOne, memStreamTwo,
									memLengthOne > memLengthTwo ? memLengthOne : memLengthTwo);
								if (isNotEq ^ result) {
									return false;
								}
								break;
						}
					}
				case Op::NULLSAFEEQ:
					{
						/** 检查数值类型NULLSAFEEQ操作 */
						if (colTypeOne == CT_TINYINT || colTypeOne == CT_SMALLINT || colTypeOne == CT_INT
							|| colTypeOne == CT_BIGINT || colTypeOne == CT_MEDIUMINT
							|| colTypeOne == CT_FLOAT || colTypeOne == CT_DOUBLE) {
								bool eitherNull = (!numberOneExist) ^ (!numberTwoExist);	/** 当两者只有一个为NULL，返回false */
								bool bothNull = (!numberOneExist) && (!numberTwoExist);		/** 当两者都有为NULL，返回true */
								if (eitherNull) {
									return false;
								} else if (bothNull) {
									break;
								}

								/** 当两者都不为NULL，进行比较 */
								if (!compareNumber(numberOne, numberTwo, Op::EQ)) {
									return false;
								}
								break;
						} else if (colTypeOne == CT_CHAR || colTypeOne == CT_VARCHAR) {
							/** 检查字符串类型的NULLSAFEEQ操作，流程同上 */
							bool eitherNull = (!stringOneExist) ^ (!stringTwoExist);
							bool bothNull = (!stringOneExist) && (!stringTwoExist);
							if (eitherNull) {
								return false;
							} else if (bothNull) {
								break;
							}

							if (!compareString(stringOne, stringTwo, Op::EQ)) {
								return false;
							}
							break;
						} else if (colTypeOne == CT_BINARY || colTypeOne == CT_VARBINARY || colTypeOne == CT_SMALLLOB
							|| colTypeOne == CT_MEDIUMLOB) {
								/** 检查内存字节流类型的NULLSAFEEQ操作，流程同上 */
								bool eitherNull = (!memOneExist) ^ (!memeTwoExist);
								bool bothNull = (!memOneExist) && (!memeTwoExist);
								if (eitherNull) {
									return false;
								} else if (bothNull) {
									break;
								}

								int result = memcmp(memStreamOne, memStreamTwo,
									memLengthOne > memLengthTwo ? memLengthOne : memLengthTwo);
								if (result!=0) {
									return false;
								}
								break;
						}
					}
				case Op::LIKE:
					{
						assert(colDefOne->m_type == CT_CHAR || colDefOne->m_type == CT_VARCHAR);
						assert(it->valueTwo.dataType == DataType::KV_CHAR
							|| it->valueTwo.dataType == DataType::KV_VARCHAR);
						assert(stringOneExist && stringTwoExist);
						if (!stringOneExist) return false;

						/** 匹配字符串解析，具体如下：
						*	1)	pattern		--->	^pattern$
						*	2)	%pattern	--->	(.)*pattern$
						*	3)	pattern%	--->	^pattern(.)*
						*	4)	%pat%tern%	--->	(.)*pat(.)*tern(.)*
						*	4)	patt_ern	--->	patt(.){1}ern
						*/
						stringReplace(stringTwo, "%", "(.)*");
						stringReplace(stringTwo, "_", "(.){1}");

						/** 采用boost正则表达式的库函数 */
						boost::regex expression(stringTwo, boost::regex::perl|boost::regex::icase);
						if(!boost::regex_match(stringOne, expression))
							return false;
					}
					break;
			}
		}
		return true;
	}

	/**
	* 从冗余记录里提取条件性更新时所需要的判定值
	*
	*	@param	tableDef	表定义
	*	@param	mysqlRow	冗余记录
	*	@param	colNo		属性号
	*	@param	colType		属性类别
	*	@param	numberResult OUT，如果为数值类型，则返回数值
	*	@param	numberExist	OUT，如果为数值型，置为true
	*	@param	stringResult OUT，如果为string类型，返回字符串
	*	@param	stringExist	OUT，如果为string型，置为true
	*	@param	memStreamResult OUT，如果为内存字节流等，返回内存首地址
	*	@param	memLength OUT，如果为内存字节流等，返回字节流长度，否则0
	*	@param	memStreamExist	OUT，如果为内存字节流等，置为true
	*/
	void KeyValueHandler::extractCondValues(TableDef *tableDef, byte *mysqlRow, u16 colNo, ColumnType colType,
		double* numberResult, bool *numberExist, string *stringResult, bool *stringExist,
		void **memStreamResult, size_t *memLength, bool *memStreamExist)	{

			if (RedRecord::isNull(tableDef, mysqlRow, colNo)) 
				return;

			/**
			*	对应列为数值类型
			*/
			if (colType >= CT_TINYINT && colType <= CT_DOUBLE) {
				switch(colType)	{
				case CT_TINYINT:
					*numberResult = RedRecord::readTinyInt(tableDef, mysqlRow, colNo);
					break;
				case CT_SMALLINT:
					*numberResult = RedRecord::readSmallInt(tableDef, mysqlRow, colNo);
					break;
				case CT_MEDIUMINT:
					*numberResult = RedRecord::readMediumInt(tableDef, mysqlRow, colNo);
					break;
				case CT_INT:
					*numberResult = RedRecord::readInt(tableDef, mysqlRow, colNo);
					break;
				case CT_BIGINT:
					*numberResult = RedRecord::readBigInt(tableDef, mysqlRow, colNo);
					break;
				case CT_FLOAT:
					*numberResult = RedRecord::readFloat(tableDef, mysqlRow, colNo);
					break;
				case CT_DOUBLE:
					*numberResult = RedRecord::readDouble(tableDef, mysqlRow, colNo);
					break;
				}

				*numberExist = true;
				return;
			}

			/** 如果为string型，从Record中读取string */
			byte *strBegin = NULL;
			size_t length = 0;
			if (colType == CT_CHAR) {
				strBegin = (byte*)RedRecord::readChar(tableDef, mysqlRow, colNo, NULL, &length);
			} else if (colType == CT_VARCHAR) {
				strBegin = (byte*)RedRecord::readVarchar(tableDef, mysqlRow, colNo, NULL, &length);
			}
			if (strBegin != NULL) {
				*stringExist = true;
				bytes2String(*stringResult, strBegin, length);

				return;
			}

			/** 如果为Binary或者Lob，从Record中读取字节流 */
			if (colType == CT_BINARY || colType == CT_VARBINARY) {
				*memStreamExist = true;
				*memStreamResult = RedRecord::readRaw(tableDef, mysqlRow, colNo, NULL, memLength);
			} else if (colType == CT_SMALLLOB || colType == CT_MEDIUMLOB) {
				*memStreamExist = true;
				RedRecord::readLob(tableDef, mysqlRow, colNo, memStreamResult, memLength);
			}
	}

	/**
	 *	解析更新操作的条件成字符串，并不计算逻辑值
	 *	
	 *	@param	tableDef	表定义
	 *	@param	conds		条件
	 */
	void KeyValueHandler::serializeConditions(TableDef *tableDef, const vector<Cond> &conds) {
		currentThreadInfo->information<<"Conditions:[";
		for (vector<Cond>::const_iterator it = conds.begin(); it != conds.end(); ++it) {
			u16 colNumOne = *(u16*)it->valueOne.dataValue.c_str();
			ColumnDef *colDefOne = tableDef->m_columns[colNumOne];
			currentThreadInfo->information<<" c:"<<colDefOne->m_name<<conditionOPs[it->op];

			DataType::type typeTwo = it->valueTwo.dataType;
			switch(typeTwo)	{
			case DataType::KV_COL:
				{
					ColumnDef *colDefTwo = tableDef->m_columns[*(u16*)it->valueTwo.dataValue.c_str()];
					currentThreadInfo->information<<"c:"<<colDefTwo->m_name;
				}

			case DataType::KV_TINYINT:
				{
					s8 tinyInt = *(s8*)it->valueTwo.dataValue.c_str();
					currentThreadInfo->information<<tinyInt;
				}
				break;
			case DataType::KV_SMALLINT:
				{
					s16 smallInt = *(s16*)it->valueTwo.dataValue.c_str();
					currentThreadInfo->information<<smallInt;
				}
				break;
			case DataType::KV_MEDIUMINT:
			case DataType::KV_INT:
				{
					s32 intValue = *(s32*)it->valueTwo.dataValue.c_str();
					currentThreadInfo->information<<intValue;
				}
				break;
			case DataType::KV_BIGINT:
				{
					s64 bigInt = *(s64*)it->valueTwo.dataValue.c_str();
					currentThreadInfo->information<<bigInt;
				}
				break;
			case DataType::KV_FLOAT:
				{
					float floatValue = *(float*)it->valueTwo.dataValue.c_str();
					currentThreadInfo->information<<floatValue;
				}
				break;
			case DataType::KV_DOUBLE:
				{
					double doubleValue = *(double*)it->valueTwo.dataValue.c_str();
					currentThreadInfo->information<<doubleValue;
				}
				break;
			case DataType::KV_CHAR:
			case DataType::KV_VARCHAR:
			case DataType::KV_BINARY:
			case DataType::KV_VARBINARY:
			case DataType::KV_BLOB:
				currentThreadInfo->information<<it->valueTwo.dataValue;
				break;
			case DataType::KV_NULL:
				currentThreadInfo->information<<"#null#";
				break;
			}
		}
		currentThreadInfo->information<<"]; ";
	}

	/**
	*	从冗余记录里提取给定属性的值
	*	
	*	@param session 会话资源
	*	@param tableDef	表定义
	*	@param content	冗余记录
	*	@param col	属性号
	*	@param length OUT，记录长度
	*
	*	@return	记录首地址
	*/
	byte* KeyValueHandler::extractRedRecord(Session * session, TableDef *tableDef, byte *content, int col, size_t *length)	{
		assert(col < tableDef->m_numCols);

		ColumnDef *colDef = tableDef->m_columns[col];

		switch(colDef->m_type)	{
			/** 对于数值类型，直接采用偏移 */
			case CT_TINYINT:
				{
					*length = 1;
					return colDef->m_offset + content;
				}
			case CT_SMALLINT:
				{
					*length = 2;
					return colDef->m_offset + content;
				}
			case CT_MEDIUMINT:
				{	
					*length = 4;
					s32 mediumInt= RedRecord::readMediumInt(tableDef, content, col);
					byte *mediumBytes = (byte*)session->getMemoryContext()->calloc(sizeof(byte) * (*length));
					copy((byte*)&mediumInt, (byte*)&mediumInt + *length, mediumBytes);
					return mediumBytes;
				}
			case CT_FLOAT:
			case CT_INT:
				{
					*length = 4;
					return colDef->m_offset + content;
				}
				/*case CT_RID:
				{
				*length = 6;
				return colDef->m_offset + content;
				}*/
			case CT_BIGINT:
			case CT_DOUBLE:
				{
					*length = 8;
					return colDef->m_offset + content;
				}
				/*case CT_DECIMAL:
				break;*/
			case CT_CHAR:
				{	
					byte* charValue = (byte*)RedRecord::readChar(tableDef, content, col, NULL, length);
					size_t last =  *length - 1;
					for (; last > 0; last--) {
						if (*(charValue + last) != 0x20) break; 
					}
					*length = last + 1;
					return charValue;						
				}
			case CT_VARCHAR:
				{
					return (byte*)RedRecord::readVarchar(tableDef, content, col, NULL, length);
				}
			case CT_BINARY:
				{
					return (byte*)RedRecord::readRaw(tableDef, content, col, NULL, length);
				}
			case CT_VARBINARY:
				{
					return (byte*)RedRecord::readVarchar(tableDef, content, col, NULL, length);
				}
			case CT_SMALLLOB:
			case CT_MEDIUMLOB:
				{
					byte *lob;
					RedRecord::readLob(tableDef, content, col, (void**)&lob, length);
					return lob;
				}
		}
		return NULL;
	}

	/**
	*	根据更新方式updatemodes更新给定的冗余记录
	*	
	*	@param tableDef	表定义
	*	@param content	冗余记录
	*	@param updatemodes	更新方式
	*
	*	@exception ServerException	如果某列超长，抛异常
	*/
	void KeyValueHandler::updateRowByMode(TableDef *tableDef, byte *content, const vector<DriverUpdateMode> &updatemodes)	{
		vector<DriverUpdateMode> setmodes;	/** SET操作的列 */

		for (vector<DriverUpdateMode>::const_iterator it = updatemodes.begin(); it != updatemodes.end(); ++it) {
			ColumnDef* colDef = tableDef->m_columns[it->attrNo];
			ColumnType colType = colDef->m_type;
			switch(it->mod)	{
				case Mode::INCR:
				case Mode::DECR:
					{
						/** 整型数值的递增和递减 */
						assert(colType == CT_TINYINT || colType == CT_SMALLINT || colType == CT_SMALLINT
							|| colType == CT_MEDIUMINT || colType == CT_INT || colType == CT_BIGINT);
						bool shouldIncr = it->mod == Mode::INCR ? 1 : 0; 
						if (RedRecord::isNull(tableDef, content, it->attrNo)) {
							stringstream msg;
							msg<<"can't incr/decr null value!";
							SERVER_EXCEPTION_THROW(KVErrorCode::KV_EC_UNSUPPORTED_OP, msg.str())
						}
						switch (colType)	{
							case CT_TINYINT:
								{
									s8 tinyInt = RedRecord::readTinyInt(tableDef, content, it->attrNo);
									MOD_NUMBER_BY_COND(s8, it->value, shouldIncr, tinyInt);
									RedRecord::writeNumber(tableDef, it->attrNo, content, tinyInt);
								}
								break;
							case CT_SMALLINT:
								{
									s16 smallInt = RedRecord::readSmallInt(tableDef, content, it->attrNo);
									MOD_NUMBER_BY_COND(s16, it->value, shouldIncr, smallInt);
									RedRecord::writeNumber(tableDef, it->attrNo, content, smallInt);
								}
								break;
							case CT_MEDIUMINT:
								{
									s32 mediumInt = RedRecord::readMediumInt(tableDef, content, it->attrNo);
									MOD_NUMBER_BY_COND(s32, it->value, shouldIncr, mediumInt);
									RedRecord::writeMediumInt(tableDef, content, it->attrNo, mediumInt);
								}
								break;
							case CT_INT:
								{
									s32 intValue = RedRecord::readInt(tableDef, content, it->attrNo);
									MOD_NUMBER_BY_COND(s32, it->value, shouldIncr, intValue);
									RedRecord::writeNumber(tableDef, it->attrNo, content, intValue);
								}
								break;
							case CT_BIGINT:
								{
									s64 bigInt = RedRecord::readBigInt(tableDef, content, it->attrNo);
									MOD_NUMBER_BY_COND(s64, it->value, shouldIncr, bigInt);
									RedRecord::writeNumber(tableDef, it->attrNo, content, bigInt);
								}
								break;
						}
					}
					break;
				case Mode::SET:
					/** 直接replace操作 */
					setmodes.push_back(*it);
					break;
				case Mode::SETNULL:
					RedRecord::setNull(tableDef, content, it->attrNo);
					break;
				case Mode::PREPEND:
				case Mode::APPEND:
					{
						/** 字符串类型的update */
						assert(colType == CT_CHAR || colType == CT_VARCHAR);

						size_t length = 0;
						char* charVal = NULL;
						switch (colType)	{
							case CT_CHAR:
								charVal = (char*)RedRecord::readChar(tableDef, content, it->attrNo, NULL, &length);
								break;
							case CT_VARCHAR:
								charVal = (char*)RedRecord::readVarchar(tableDef, content, it->attrNo, NULL, &length);
								break;
						}
						std::string originalStr(charVal, length);
						std::stringstream sstream;
						if (it->mod == Mode::PREPEND) {
							sstream<<it->value<<originalStr;
						}
						else {
							sstream<<originalStr<<it->value;
						}

						if (colType == CT_CHAR)	{
							checkColSize(tableDef, it->attrNo, sstream.str().length(), false);
							RedRecord::writeChar(tableDef, content, it->attrNo, sstream.str().c_str());
						} else {
							checkColSize(tableDef, it->attrNo, sstream.str().length(), true);
							RedRecord::writeVarchar(tableDef, content, it->attrNo, sstream.str().c_str());
						}
					}
					break;
			}
		}

		/** 对Set的Update操作的列进行批量更新 */
		if (!setmodes.empty()) {
			buildRedRecord(tableDef, content, setmodes);
		}
	}

	/**
	*	将给定的key转化成NTSE格式KEY_PAD的搜索索引键
	*
	*	@param	tableDef	表定义
	*	@param	idx			索引号
	*	@param	key			给定的key值
	*	@param	outKey		IN&&OUT，IN表示要取出的属性，OUT为属性值及占用空间大小
	*   调用者必须为保存输出内容分配足够多的内存，并且通过设置key->m_size告知
	*   已经分配的内存大小，防止越界。其m_format一定为KEY_PAD
	*/
	void KeyValueHandler::transKey(TableDef *tableDef, const vector<Attr> &key, SubRecord *outKey)	{
		RedRecord redRecord(tableDef, REC_REDUNDANT);
		buildRedRecord(tableDef, redRecord.getRecord()->m_data, key);
		RecordOper::extractKeyRP(tableDef, redRecord.getRecord(), outKey);
	}

	/**
	*	从冗余记录里提取给定属性列的值，并封装成thrift的binary类型(string)
	*
	*	@param	session		会话对象
	*	@param	tableDef	表定义
	*	@param	mysqlRow	冗余记录
	*	@param	attrs		需要提取值的属性列数组
	*
	*	@return	attrs属性对应的值。第一项为空值位图，后面项则对应各个属性值。
	*/
	void KeyValueHandler::extractResult(Session *session, TableDef *tableDef, byte *mysqlRow, const vector<int16_t> & attrs,
		vector<string> & _return)	{

			_return.resize(attrs.size() + 1);	/** 分配足够大小的返回值空间 */

			size_t bmByteLenght = (attrs.size() + 7) / 8;
			byte* bitmapMem = (byte*)session->getMemoryContext()->calloc(bmByteLenght * sizeof(byte));
			Bitmap bitmap(bitmapMem, bmByteLenght * 8);

			string *pResult = NULL;
			for (u32 i = 0; i < attrs.size(); ++i) {
				bool isColNull = RedRecord::isNull(tableDef, mysqlRow, attrs[i]);
				byte *ptr = NULL;
				size_t len = 0;
				if (!isColNull) {
					ptr = extractRedRecord(session, tableDef, mysqlRow, attrs[i], &len);
					bitmap.clearBit(i);
				} else	{
					bitmap.setBit(i);
				}

				bytes2String(_return[i + 1], ptr, len);
			}

			bytes2String(_return[0], bitmapMem, bmByteLenght);
	}

	/*
	*	打开表操作，先在表哈希中查找，如没有再打开对应的表，抛出异常
	*	
	*	@param	session 会话资源
	*	@param tabelInfo 表信息
	*	@return 打开表的结构
	*/
	Table* KeyValueHandler::openTable(Session *session, const KVTableInfo &tableInfo)	{
#ifdef NTSE_UNIT_TEST
		string tableName = tableInfo.m_name;
#else
		stringstream strStream;
		strStream<<"./"<<tableInfo.m_schemaName<<"/"<<tableInfo.m_name;
		string tableName = strStream.str();
#endif

		return KeyValueServer::getTable(m_database, session, tableName.c_str());
	}

	/**
	*	数据写入时，检查记录是否超长，如果是，则抛异常
	*
	*	@param	tableDef	服务端得到的表定义
	*	@param	columnNo	列号
	*	@param	newSize		写入数据的长度
	*	@param	var			是否为var类型
	*	
	*	@exception	如果超长，抛异常
	*/
	void KeyValueHandler::checkColSize(TableDef *tableDef, u16 columnNo, size_t newSize, bool var)	{
		ColumnDef* colDef = tableDef->m_columns[columnNo];
		byte lenBytes = var ? colDef->m_lenBytes : 0;
		if (newSize + lenBytes > colDef->m_size) {
			stringstream msg;
			msg<<"Column : "<<colDef->m_name<<" has exceed the defined max lenght";
			SERVER_EXCEPTION_THROW(KVErrorCode::KV_EC_VALUE_OVERFLOW, msg.str())
		}
	}

	/*
	 *	解析表定义中所有索引相关的列
	 *
	 *	param	tableDef	表定义
	 *	param	colsSet		存放列号的集合
	 */
	void KeyValueHandler::getIndiceCols(TableDef *tableDef, set<u16> &colsSet) {
		for (u8 index = 0; index < tableDef->m_numIndice; ++index) {
			IndexDef *currentIndex = tableDef->m_indice[index];
			colsSet.insert(currentIndex->m_columns, currentIndex->m_columns + currentIndex->m_numCols);
		}
	}

#ifdef NTSE_UNIT_TEST
	/**
	*	清除缓存的表结构
	*/
	void KeyValueHandler::clearOpenTables()	{
		KeyValueServer::clearCachedTable();
	}
#endif
	/**
	*	获得索引扫描句柄
	*
	*	@exception ServerException	索引不存在，或者调用NTSE接口错误
	*
	*	@param	session	会话对象
	*	@param	activeTable	表结构
	*	@param	key		索引键对应的键值
	*	@param	attrs	要读取的属性列
	*	@param	operType	操作类型
	*
	*	@return		索引扫描句柄
	*/
	TblScan* KeyValueHandler::getTblScanByKey(Session *session, Table *activeTable, const vector<Attr> & key, const ColList &attrs,
		OpType operType)	{
			assert(activeTable && key.size() > 0);

			TableDef* tableDef = activeTable->getTableDef();

			u8 activeIndexNo = tableDef->getIndexNo(tableDef->m_pkey->m_name);
			if (activeIndexNo == INVALID_INDEX_NO) {
				SERVER_EXCEPTION_THROW(KVErrorCode::KV_EC_NONEINDEX, "Cannot find the index of the given key.")
			}

			/** 根据Key的属性列去查询表定义中的索引号 */
			void *p = session->getMemoryContext()->alloc(sizeof(SubRecord));
			SubRecord *indexKey = new (p)SubRecord(KEY_PAD, tableDef->m_pkey->m_numCols,tableDef->m_pkey->m_columns,
				(byte *)session->getMemoryContext()->alloc(tableDef->m_pkey->m_maxKeySize),tableDef->m_pkey->m_maxKeySize);
			transKey(tableDef, key, indexKey);	

			/** 索引扫描 */
			try {
				IndexScanCond cond(activeIndexNo, indexKey, true, true, true);
				sort(attrs.m_cols, attrs.m_cols + attrs.m_size);
				return activeTable->indexScan(session, operType, &cond, attrs.m_size, attrs.m_cols, false, session->getLobContext());
			} catch(NtseException &e) {
				SERVER_EXCEPTION_THROW(exceptToKVErrorCode(e.getErrorCode()), e.getMessage())
			}
	}
}

#endif
