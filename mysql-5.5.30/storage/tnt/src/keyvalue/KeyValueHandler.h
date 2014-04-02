/**
* KeyValue Server 处理器
*
* @author 廖定柏(liaodingbai@corp.netease.com)
*/

#ifndef _KEYVALUE_HANDLER_H_
#define _KEYVALUE_HANDLER_H_

#include <string>
#include <set>
#include <sstream>
#include <queue>
#include "gen-cpp/KV_types.h"
#include "KeyValueHelper.h"
#include "KeyValueEventHandler.h"
#include "KeyValueServer.h"
#include "util/Thread.h"
#include "util/Bitmap.h"
#include "rec/Records.h"
#include "api/Table.h"
#include "api/Database.h"
#include <stddef.h>

using namespace std;

namespace ntse {
	class Session;
	class Table;
	class SubRecord;
	class TableDef;
	class TblScan;
	class RedRecord;

	/**
	*	KeyValueHandler中Session的自动分配和释放
	*/
	class SesSavePoint {
	public:
		/**
		*	构造函数
		*	@param	db 数据库
		*	@param	connection 链接
		*/
		SesSavePoint(Database *db, Connection *connection) : m_db(db)	{
			m_session = m_db->getSessionManager()->allocSession("KeyValueHandler", connection,
				m_timeoutMs);
			if (!m_session) {
				SERVER_EXCEPTION_THROW(keyvalue::ErrCode::KV_EC_TOO_MANY_SESSION, "Cannot alloc a session.")
			}
		}

		/**
		*	析构函数
		*/
		~SesSavePoint()	{
			m_db->getSessionManager()->freeSession(m_session);
		}

		/**
		*	得到分配的会话资源
		*
		*	@return 会话资源
		*/
		Session* getSession()	{
			return m_session;
		}
	private:
		Database *m_db;	/** DataBase实例 */
		Session *m_session;			/** 当前分配的会话 */
		static const int m_timeoutMs = 2000;	/** 请求会话的超时时间 */
	};

	/** 定义KeyValue服务操作 */
	class KeyValueHandler : virtual public KVIf {
	public:
		KeyValueHandler(Database *database);

		~KeyValueHandler();

		void get(vector<string> & _return, const KVTableInfo& table, const Attrs& key,
			const vector<int16_t> & attrs, const int64_t version) throw (ServerException);

		void multi_get(map<int16_t, vector<string> > & _return, const KVTableInfo& table,
			const vector<Attrs> & keys, const vector<int16_t> & attrs, const int64_t version) throw (ServerException);

		int8_t put(const KVTableInfo& table, const Attrs& key, const Attrs& values,
			const int64_t version) throw (ServerException);

		int8_t setrec(const KVTableInfo& table, const Attrs& key, const Attrs& values,
			const int64_t version) throw (ServerException);

		int8_t replace(const KVTableInfo& table, const Attrs& key, const Attrs& values,
			const int64_t version) throw (ServerException);

		int8_t remove(const KVTableInfo& table, const Attrs& key, const int64_t version) throw (ServerException);

		int8_t update(const KVTableInfo& table, const Attrs& key, const vector<Cond> & conds,
			const vector<DriverUpdateMode> & updatemodes, const int64_t version) throw (ServerException);

		int8_t put_or_update(const KVTableInfo& table, const Attrs& key, const Attrs& values,
			const vector<DriverUpdateMode> & updatemodes, const int64_t version) throw (ServerException);

		void getTableDef(KVTableDef& _return, const KVTableInfo& table) throw (ServerException);

#ifdef NTSE_UNIT_TEST
		void clearOpenTables();
#endif
	private:
		Table* openTable(Session* session, const KVTableInfo &tabelInfo);

		static TblScan* getTblScanByKey(Session* session, Table *activeTable, const vector<Attr> & key, const ColList &attrs, OpType operType);
		static byte* extractRedRecord(Session* session, TableDef *tableDef, byte *content, int col, size_t *length);
		static void extractResult(Session* session, TableDef *tableDef, byte *content, const vector<int16_t> &attrs, vector<string> &_return);

		static void getIndiceCols(TableDef *tableDef, set<u16> &colsSet);
		static void transKey(TableDef *tableDef, const vector<Attr> &key, SubRecord *outKey);
		static bool validateByCond(TableDef* tableDef, byte* oldContent, const vector<Cond> &conds);
		static void updateRowByMode(TableDef *tableDef, byte *content, const vector<DriverUpdateMode> &updatemodes);
		static void checkColSize(TableDef *tableDef, u16 columnNo, size_t newSize, bool var);
		static void extractCondValues(TableDef *tableDef, byte *oldContent, u16 colNo, ColumnType colType,
			double* numberResult, bool *numberExist, string *stringResult, bool *stringExist,
			void **memStreamResult, size_t *memLength, bool *memStreamExist);

		static void serializeConditions(TableDef *tableDef, const vector<Cond> &conds);

		/**
		 *	解析Get操作中，返回值需要的属性成字符串
		 *
		 *	@param attrs	需要解析的属性s
		 */
		inline static void serializeAttrNOs(const vector<int16_t> & attrs) {
			currentThreadInfo->information<<"Fetch Attributes:[";

			size_t index = 0;
			for (; index < attrs.size() - 1; ++index) {
				currentThreadInfo->information<<attrs[index]<<",";
			}
			currentThreadInfo->information<<attrs[index]<<"]; ";
		}

		/**
		*	解析操作中需要的属性成字符串
		*	
		*	@param	tableDef	表定义
		*	@param	attributes	需要解析的属性s
		*	@param	isKey		是否为主键属性
		*/
		template<class T> static void serializeAttributes(TableDef *tableDef, const vector<T> &attributes, bool isKey = false) {
			if (isKey) {
				currentThreadInfo->information<<"KEY:[";
			} else {
				currentThreadInfo->information<<"ATTRIBUTES:[";
			}

			typename vector<T>::const_iterator it;
			for (it = attributes.begin(); it != attributes.end(); ++it) {
				int columnNo = it->attrNo;
				ColumnDef *colDef = tableDef->m_columns[columnNo];

				currentThreadInfo->information<<" <"<<colDef->m_name<<", ";
				if (!isKey) {
					u16 nextColumnSize = (columnNo == tableDef->m_numCols - 1) ? 0 : tableDef->m_columns[columnNo + 1]->m_size;
					currentThreadInfo->information<<it->value.size()<<", "<<nextColumnSize<<">";
					continue;
				}
				
				switch (colDef->m_type)	{
					case CT_TINYINT:
						{
							s8 tinyInt = *(s8*)it->value.c_str();
							currentThreadInfo->information<<tinyInt;
						}
						break;
					case CT_SMALLINT:
						{
							s16 smallInt = *(s16*)it->value.c_str();
							currentThreadInfo->information<<smallInt;
						}
						break;
					case CT_MEDIUMINT:
					case CT_INT:
						{
							s32 intNum = *(s32*)it->value.c_str();
							currentThreadInfo->information<<intNum;
						}
						break;
					case CT_FLOAT:
						{
							float floatNum = *(float*)it->value.c_str();
							currentThreadInfo->information<<floatNum;
						}
						break;
					case CT_DOUBLE:
						{
							double doubleValue = *(double*)it->value.c_str();
							currentThreadInfo->information<<doubleValue;
						}
						break;
					case CT_BIGINT:
						{
							s64 bigInt = *(s64*)it->value.c_str();
							currentThreadInfo->information<<bigInt;
						}
						break;
					case CT_CHAR:
					case CT_VARCHAR:
					case CT_BINARY:
					case CT_VARBINARY:
					case CT_SMALLLOB:
					case CT_MEDIUMLOB:
						currentThreadInfo->information<<it->value;
						break;
				}
				currentThreadInfo->information<<">";
			}

			currentThreadInfo->information<<" ]; ";
		}

		/**
		*	根据给定的属性列填充冗余记录。如果空值位图表不为NULL，则可以判断填充的属性列不是主键索引相关列，
		*	此时如果tableDef中缺少的列被默认置为NULL
		*
		*	@param	tableDef	表定义
		*	@param	mysqlRow	将要被填充的冗余记录
		*	@param	columns		填充的属性列
		*	@param	existCols	保存已经有相关信息的列 (BUG #46058 )
		*	@param	nullBitmap	空值位图表
		*	@param	insertAction该记录是否用于插入记录操作（区别于更新操作）	
		*
		*	@exception ServerException	如果某列超长，抛异常
		*/
		template<class T> static void buildRedRecord(TableDef* tableDef, byte *mysqlRow,
			const vector<T> &columns, set<u16> *existCols = NULL, Bitmap *nullBitmap = NULL,
			bool insertAction = false)	{
				size_t bmIndex = 0;		/**	位图表的索引 */
				typename vector<T>::const_iterator it;		/**	解决GCC编译问题的写法 */
				for (it = columns.begin(); it != columns.end(); ++it, ++bmIndex) {
					int columnNo = it->attrNo;
					/**	将当前列号插入set中，在置缺少列为NULL操作中不再更新 */
					if (existCols) {
						existCols->insert(columnNo);
					}

					/*
					 *	如果当前列对应空值位图表被设置，该列内容置为NULL
					 */
					if (nullBitmap != NULL && nullBitmap->isSet(bmIndex)) {
						RedRecord::setNull(tableDef, mysqlRow, columnNo);
						continue;
					}

					switch (tableDef->m_columns[columnNo]->m_type)	{
					case CT_TINYINT:
						RedRecord::writeNumber(tableDef, columnNo, mysqlRow, *(s8*)it->value.c_str());
						break;
					case CT_SMALLINT:
						RedRecord::writeNumber(tableDef, columnNo, mysqlRow, *(s16*)it->value.c_str());
						break;
					case CT_MEDIUMINT:
						RedRecord::writeMediumInt(tableDef, mysqlRow, columnNo, *(s32*)it->value.c_str());
						break;
					case CT_FLOAT:
						RedRecord::writeNumber(tableDef, columnNo, mysqlRow, *(float*)it->value.c_str());
						break;
					case CT_INT:
						RedRecord::writeNumber(tableDef, columnNo, mysqlRow, *(s32*)it->value.c_str());
						break;
					case CT_DOUBLE:
						RedRecord::writeNumber(tableDef, columnNo, mysqlRow, *(double*)it->value.c_str());
						break;
					case CT_BIGINT:
						RedRecord::writeNumber(tableDef, columnNo, mysqlRow, *(s64*)it->value.c_str());
						break;
						/*case CT_DECIMAL:
						break;*/
					case CT_CHAR:
						checkColSize(tableDef, columnNo, it->value.length(), false);
						RedRecord::writeChar(tableDef, mysqlRow, columnNo, it->value.c_str());
						break;
					case CT_VARCHAR:
						checkColSize(tableDef, columnNo, it->value.length(), true);
						RedRecord::writeVarchar(tableDef, mysqlRow, columnNo, (byte*)it->value.c_str(), it->value.length());
						break;
					case CT_BINARY:
						checkColSize(tableDef, columnNo, it->value.length(), false);
						RedRecord::writeRaw(tableDef, mysqlRow, columnNo, (void*)it->value.c_str(), it->value.length());
						break;
					case CT_VARBINARY:
						checkColSize(tableDef, columnNo, it->value.length(), true);
						RedRecord::writeVarchar(tableDef, mysqlRow, columnNo, (byte*)it->value.c_str(), it->value.length());
						break;
					case CT_SMALLLOB:
					case CT_MEDIUMLOB:
						RedRecord::writeLob(tableDef, mysqlRow, columnNo, (byte*)it->value.c_str(), it->value.length());
						break;
					}
				}

				/*
				 *	如果该列内容为被作为插入记录时，需要对缺少信息的列置NULL操作
				 */
				if (insertAction) {
					vector<u16> tableCols(tableDef->m_numCols);
					for (u16 index = 0; index < tableDef->m_numCols; tableCols[index] = index, ++index);

					vector<u16> differentCols(tableDef->m_numCols);	/**	保存没有信息的列，即将要被置为NULL的列 */
					vector<u16>::iterator diffEnd = set_difference(tableCols.begin(), tableCols.end(),
						existCols->begin(), existCols->end(), differentCols.begin());
					for (vector<u16>::iterator diffIt = differentCols.begin(); diffIt != diffEnd; ++diffIt) {
						ColumnDef *colDef = tableDef->m_columns[*diffIt];
						if (colDef->m_nullable) {
							RedRecord::setNull(tableDef, mysqlRow, *diffIt);
						} else {
							/**	如果该列不能为null，则应抛出异常 */
							stringstream msg;
							msg<<"Column : "<<colDef->m_name<<" cannot be null";
							SERVER_EXCEPTION_THROW(KVErrorCode::KV_EC_COL_NOT_NULLABLE, msg.str())
						}
					}
				}
		}

	private:
		Database *m_database;	/** thrift操作的数据库 */
	};
}

#endif