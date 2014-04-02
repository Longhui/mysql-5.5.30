/**
* KeyValue����˴���
*
* @author �ζ���(liaodingbai@corp.netease.com)
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

#define INVALID_INDEX_NO -1	/** ��Ч�������� */

#define MOD_NUMBER_BY_COND(TYPE, STRING_VALUE, CHANGETYPE, ORIGNAL)	\
	TYPE DELTA = *(TYPE*)STRING_VALUE.c_str();	\
	CHANGETYPE ? (ORIGNAL += DELTA) : (ORIGNAL -= DELTA);	\

using namespace std;

namespace ntse	{

	/**
	*	�����ж��У��������Ͷ�Ӧ���ַ������
	*/
	static const char *conditionOPs[] = {
		"==", ">", "<", ">=", "<=", "<>", "like", "<=>", "IS NULL"
	};

	/** �Ƚ���������ʽ���������ĺ������� */
	class KeyComparator	{
	public:
		KeyComparator(TableDef *tableDef) : m_tableDef(tableDef)	{}

		bool operator()(const SubRecord *a , const SubRecord *b) const {
			return RecordOper::compareKeyPP(m_tableDef, a, b) < 0;
		}
	private:
		TableDef *m_tableDef; /** ���� */
	};

	/**
	*	��Key-Value�ͻ��˵�APIͳ����Ϣ���޸Ĳ����ķ�װ
	*/
	class ThreadInfoMonitor {
	public:
		/**
		*	Constructor
		*	@param	table ����Ϣ
		*	@param	method	API������
		*/
		ThreadInfoMonitor(const KVTableInfo& table, InvokedMethod method) {
			stringstream strStream;
			strStream<<"./"<<table.m_schemaName<<"/"<<table.m_name;

			currentThreadInfo->status = PROCESS_BEGIN;
			currentThreadInfo->apiStartTime = System::microTime();
			currentThreadInfo->api = method;
			currentThreadInfo->tablePath = strStream.str();
			currentThreadInfo->information.str(""); // ������ݣ���clear()
		}

		/**
		*	�����������ı��ֲ߳̾���Ϣ
		*/
		~ThreadInfoMonitor() {
			currentThreadInfo->api = NONE;
			currentThreadInfo->apiStartTime = -1;
			currentThreadInfo->information.str(""); // ������ݣ���clear()
			currentThreadInfo->status = FREE;
		}

		/**
		*	�����ֲ߳̾���Ϣ��״̬Ϊ��������У���־�����������
		*/
		inline void setProcessingStatus() {
			currentThreadInfo->status = PROCESSING;
		}

		/**
		*	�����ֲ߳̾���Ϣ��״̬Ϊ�������״̬
		*/
		inline void setEndStatus() {
			currentThreadInfo->status = PROCESS_END;
		}

		/**
		*	�����ֲ߳̾���Ϣ��״̬Ϊ�쳣״̬
		*/
		inline void setExceptionStatus() {
			currentThreadInfo->status = PROCESS_EXCEPTION;
		}
	};

	/**
	 *	keyvalue�������������ʵ����
	 */
	class TblLocker {
	public:
		/**
		 *	���캯��
		 *	@param	session �Ự����
		 *	@param	table	�����
		 *	@param	opType	�Ա�Ĳ�������
		 *	@param	config	���ݿ�����ú���
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
		 *	��������
		 *	�Լ����ı�����ͷ�
		 */
		~TblLocker()	{
			m_table->unlock(m_session, m_currentMode);
			m_table->unlockMeta(m_session, IL_S);
		}

		/**
		 *	@pre	�ñ��Ѿ�����
		 *
		 *	@param	clientVersion	�ͻ��˲�����İ汾��
		 *
		 *	�õ���Ķ���
		 */
		TableDef* getTableDef(u64 clientVersion) {
			TableDef* tableDef = m_table->getTableDef();
			if (tableDef->m_version != clientVersion) {
				SERVER_EXCEPTION_THROW(KVErrorCode::KV_EC_TBLDEF_NOT_MATCH, "The version of table mismatched.")
			}
			return tableDef;
		}
	private:
		Table	*m_table;			/** ����� */
		Session *m_session;		/** �Ự���� */
		OpType	m_opType;		/** �������� */
		ILMode	m_currentMode;	/**	��ǰ���Ѿ����ϵ������� */
	};


	/**
	*	���캯��������session��connection
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
	*	�����������ͷ���Դ
	*/
	KeyValueHandler::~KeyValueHandler()	{
#ifdef NTSE_UNIT_TEST
		m_database->freeConnection(currentThreadInfo->connection);
		delete currentThreadInfo;
		currentThreadInfo = NULL;
#endif
	}

	/**
	*	��ȡ����=key�ļ�¼�����ظü�¼��attrs��Щ����.
	*	
	*	@exception	ServerException	�򿪱�������ߵ���NTSE�ӿڳ����쳣�ȡ�
	*
	*	@param	table	����Ϣ
	*	@param	key		����key	
	*	@param	attrs	��Ҫȡֵ������
	*	@param	version	��汾
	*
	*	@return	attrs���Զ�Ӧ��ֵ����һ��Ϊ��ֵλͼ����������
	*			��Ӧ��������ֵ��
	*/
	void KeyValueHandler::get(vector<string> & _return, const KVTableInfo& table, const Attrs& key,
		const vector<int16_t> & attrs, const int64_t version) throw (ServerException)	{
			ThreadInfoMonitor infoMonitor(table, GET_API);

			/** Profile��Ϣ */
			PROFILE(PI_KeyValue_get);

			SesSavePoint sessionSP(m_database, currentThreadInfo->connection);
			Session *session = sessionSP.getSession();

			Table *currentTable = openTable(session, table);

			/**	������������API��ͬ */
			TblLocker tblLocker(session, currentTable, OP_READ, m_database->getConfig());
			TableDef* tableDef = tblLocker.getTableDef(version);

			/**	���л�������Ϣ */
			serializeAttributes(tableDef, key.attrlist, true);
			serializeAttrNOs(attrs);
			infoMonitor.setProcessingStatus();

			ColList colList;
			colList.m_size = attrs.size();
			colList.m_cols = (u16*)session->getMemoryContext()->alloc(attrs.size() * sizeof(u16));
			copy(attrs.begin(), attrs.end(), colList.m_cols);

			byte *mysqlRow = (byte*)session->getMemoryContext()->calloc(tableDef->m_maxRecSize * sizeof(byte));

			/*
			 *	����ɨ��
			 */
			TblScan *tblScan = getTblScanByKey(session, currentTable, key.attrlist, colList, OP_READ);
			bool exist = false;
			try {
				exist = currentTable->getNext(tblScan, mysqlRow);

				currentTable->endScan(tblScan);
			} catch (NtseException &e) {
				/**	���þֲ��߳���Ϣ */
				currentTable->endScan(tblScan);

				++KeyValueServer::m_serverStats.getFailed;
				infoMonitor.setExceptionStatus();

				SERVER_EXCEPTION_THROW(exceptToKVErrorCode(e.getErrorCode()), e.getMessage())
			}

			/*
			 *	���ڼ�¼�Ļ���ȡ�����أ�������Ƕ�����
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
	*	��ȡ����in (keys)�ļ�¼�����ظü�¼��attrs��Щ����
	*	
	*	@exception	ServerException	�򿪱�������ߵ���NTSE�ӿڳ����쳣�ȡ�
	*
	*	@param	table	����Ϣ
	*	@param	keys	����key����
	*	@param	attrs	��Ҫȡֵ������
	*	@param	version	��汾
	*
	*	@return	��������keys��Ӧ�ļ�¼��map�е�һ���Ǽ�¼�����Ӧ��key��keys�е����кţ��ڶ��������Զ�Ӧ��ֵ���ϣ���get��ͬ��
	*/
	void KeyValueHandler::multi_get(map<int16_t, std::vector<std::string> > & _return, const KVTableInfo& table,
		const vector<Attrs> & keys, const vector<int16_t> & attrs, const int64_t version) throw (ServerException)	{
			ThreadInfoMonitor infoMonitor(table, MULTI_GET_API);

			/** Profile��Ϣ */
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

			/** ����key�Ĵ�С����map�������� */
			map<SubRecord*, IndexOfKey, KeyComparator> keyMap((KeyComparator(tableDef)));

			/** ����key��map�У�map���Զ����� */
			size_t index = 0;
			IndexDef *pkIndex = tableDef->m_pkey;

			/**	�ռ�ͳһ���䣬Ȼ�������ʼ�� */
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

			/** ����key�Ĵ�С˳��ȡ��RowId */
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
	*	����һ����¼��ֻ��ָ������������ʱ����
	*	
	*	@exception	ServerException	�򿪱�������ߵ���NTSE�ӿڳ����쳣�ȡ�
	*
	*	@param	table	����Ϣ
	*	@param	key		����key
	*	@param	values	�����¼��Ӧ���Ե�ֵ
	*	@param	version	��汾
	*
	*	@return	���ز���Ӱ��ļ�¼������put�ɹ�����1������0�����쳣��
	*/
	int8_t KeyValueHandler::put(const KVTableInfo& table, const Attrs& key, const Attrs& values,
		const int64_t version) throw (ServerException)	{
			/** �ֲ߳̾���Ϣ */
			ThreadInfoMonitor infoMonitor(table, PUT_API);

			/** Profile��Ϣ */
			PROFILE(PI_KeyValue_put);

			SesSavePoint sessionSP(m_database, currentThreadInfo->connection);
			Session *session = sessionSP.getSession();

			Table *currentTable = openTable(session, table);

			TblLocker tblLocker(session, currentTable, OP_WRITE, m_database->getConfig());
			TableDef* tableDef = tblLocker.getTableDef(version);

			serializeAttributes(tableDef, key.attrlist, true);
			serializeAttributes(tableDef, values.attrlist, false);
			infoMonitor.setProcessingStatus();

			set<u16> existCols;		/**	������Ϣ�а������� */
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
	*	����һ����¼��������������ʱ���룬����ʱ�滻
	*	
	*	@exception	ServerException	�򿪱�������ߵ���NTSE�ӿڳ����쳣�ȡ�
	*
	*	@param	table	����Ϣ
	*	@param	key		����key
	*	@param	values	set��¼��Ӧ���Ե�ֵ
	*	@param	version	��汾
	*
	*	@return	���ز���Ӱ��ļ�¼������put����update�ɹ�����1������0�����쳣��
	*/
	int8_t KeyValueHandler::setrec(const KVTableInfo& table, const Attrs& key, const Attrs& values,
		const int64_t version) throw (ServerException)	{
			/** �ֲ߳̾���Ϣ */
			ThreadInfoMonitor infoMonitor(table, SET_API);

			/** Profile��Ϣ */
			PROFILE(PI_KeyValue_set);

			SesSavePoint sessionSP(m_database, currentThreadInfo->connection);
			Session *session = sessionSP.getSession();

			Table *currentTable = openTable(session, table);

			TblLocker tblLocker(session, currentTable, OP_WRITE, m_database->getConfig());
			TableDef* tableDef = tblLocker.getTableDef(version);

			serializeAttributes(tableDef, key.attrlist, true);
			serializeAttributes(tableDef, values.attrlist, false);
			infoMonitor.setProcessingStatus();

			set<u16> existCols;		/**	������Ϣ�а������� */

			byte *mysqlRow = (byte*)session->getMemoryContext()->calloc(tableDef->m_maxRecSize * sizeof(byte));
			buildRedRecord(tableDef, mysqlRow, key.attrlist, &existCols);

			Bitmap valueBitmap((void*)values.bmp.c_str(), values.bmp.size() * 8);
			buildRedRecord(tableDef, mysqlRow, values.attrlist, &existCols, &valueBitmap, true);

			bool success = false;
			IUSequence *iuSeq = NULL;
			try {
				/** ����insert */
				iuSeq = currentTable->insertForDupUpdate(session, mysqlRow, false);
				success = true;
				if (iuSeq) {	/** ������ڳ�ͻ��¼����Գ�ͻ��¼��ȡ���²��� */
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
	*	����������¼��ֻ����������ʱ�滻
	*	
	*	@exception	ServerException	�򿪱�������ߵ���NTSE�ӿڳ����쳣�ȡ�
	*
	*	@param	table	����Ϣ
	*	@param	key		����key
	*	@param	values	�����ж�Ӧ������ֵ
	*	@param	version	��汾
	*
	*	@return	���ز���Ӱ��ļ�¼������update�ɹ�����1������0�����쳣��
	*/
	int8_t KeyValueHandler::replace(const KVTableInfo& table, const Attrs& key, const Attrs& values,
		const int64_t version) throw (ServerException)	{
			/** �ֲ߳̾���Ϣ */
			ThreadInfoMonitor infoMonitor(table, REPLACE_API);

			/** Profile��Ϣ */
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
			// update������ص���
			ColList updateCols = extractColList(values.attrlist, colSet, session);

			getIndiceCols(tableDef, colSet);
			// ������Ҫ�õ��������еļ���
			ColList colList = extractColList(values.attrlist, colSet, session);

			byte *mysqlRow = (byte*)session->getMemoryContext()->calloc(tableDef->m_maxRecSize * sizeof(byte));

			bool success = false;
			TblScan *tblScan = getTblScanByKey(session, currentTable, key.attrlist, colList, OP_UPDATE);
			try {
				bool exist = currentTable->getNext(tblScan, mysqlRow);

				if (exist) {
					// �����¼��ǰ��
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
	*	ɾ��һ����¼
	*	
	*	@exception	ServerException	�򿪱�������ߵ���NTSE�ӿڳ����쳣�ȡ�
	*
	*	@param	table	����Ϣ
	*	@param	key	����key
	*	@param	version	��汾
	*
	*	@return	���ز���Ӱ��ļ�¼������ɾ���ɹ�����1������0�����쳣��
	*/
	int8_t KeyValueHandler::remove(const KVTableInfo& table, const Attrs& key, const int64_t version) throw (ServerException)	{
		/** �ֲ߳̾���Ϣ */
		ThreadInfoMonitor infoMonitor(table, REMOVE_API);

		/** Profile��Ϣ */
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
		 *	����õ����е������У���Ȼ����ɾ��ʱ����������������ֵ������ȷ
		 */
		getIndiceCols(tableDef, colSet);
		ColList colList = extractColList(key.attrlist, colSet, session);

		TblScan* tblScan = getTblScanByKey(session, currentTable, key.attrlist, colList, OP_DELETE);

		int effectRows = 0;
		try {
			byte *mysqlRow = (byte*)session->getMemoryContext()->calloc(tableDef->m_maxRecSize * sizeof(byte));
			bool exist = currentTable->getNext(tblScan, mysqlRow);

			/** ������ɾ�� */
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
	*	�����Ը���һ����¼����ͬʱ���¶�����ԣ�ÿ�����Կɽ�������������£�
	*		1.	set v:�������Ե�ֵ��Ϊv
	*		2.	incr/decr delta: �������Ե�ֵ���ӻ����delta��ֻ������Ϊ��������ʱ�ſ��á�
	*		3.	append suffiix: �������Ե�ֵ����׷��suffix��ֻ������Ϊ�ַ�������ʱ�ſ��á�
	*		4.	prepend prefix: �������Ե�ֵǰ��׷��prefix��ֻ������Ϊ�ַ�������ʱ�ſ��á�
	*	
	*	@exception	ServerException	�򿪱�������ߵ���NTSE�ӿڳ����쳣�ȡ�
	*
	*	@param	table	����Ϣ
	*	@param	key		����key
	*	@param	conds	��������
	*	@param	updatemodes	���·�ʽ
	*	@param	version ��汾
	*
	*	@return	���ز���Ӱ��ļ�¼���������³ɹ�����1������0�����쳣��
	*/
	int8_t KeyValueHandler::update(const KVTableInfo& table, const Attrs& key, const vector<Cond> & conds,
		const vector<DriverUpdateMode> &updatemodes, const int64_t version) throw (ServerException)	{
			/** �ֲ߳̾���Ϣ */
			ThreadInfoMonitor infoMonitor(table, UPDATE_API);

			/** Profile��Ϣ */
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

			// update������ص���
			ColList updateCols = extractColList(updatemodes, colsSet, session);

			getIndiceCols(tableDef, colsSet);

			/*
			*	�����ж���������������õ�updatemodes��û���漰�����У�������Ҫ��ȡconds���漰
			*	���С�
			*/
			for (vector<Cond>::const_iterator it = conds.begin(); it != conds.end(); ++it) {
				assert(it->valueOne.dataType == DataType::KV_COL);
				colsSet.insert(*(u16*)it->valueOne.dataValue.c_str());

				if (it->valueTwo.dataType == DataType::KV_COL) {
					colsSet.insert(*(u16*)it->valueTwo.dataValue.c_str());
				}
			}

			/**	�õ��������漰���������м��� */
			ColList colList = extractColList(updatemodes, colsSet, session);

			TblScan* tblScan = getTblScanByKey(session, currentTable, key.attrlist, colList, OP_UPDATE);

			bool success = false;
			try {
				byte *mysqlRow = (byte*)session->getMemoryContext()->calloc(tableDef->m_maxRecSize * sizeof(byte));

				bool exist = currentTable->getNext(tblScan, mysqlRow);

				/** ����ü�¼���ڣ������Ը��� */
				if (exist) {
					if(validateByCond(tableDef, mysqlRow, conds))	{ /** �ж����� */
						++KeyValueServer::m_serverStats.updateConfirmedByCond;

						// �����¼��ǰ��
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
	*	��������һ����¼����ָ����������������룬���������������Ը���
	*	
	*	@exception	ServerException	�򿪱�������ߵ���NTSE�ӿڳ����쳣�ȡ�
	*
	*	@param	table	����Ϣ
	*	@param	key		����key
	*	@param	updatemodes	���·�ʽ
	*	@param	version	��汾
	*
	*	@return	���ز���Ӱ��ļ�¼������put���߸��³ɹ�����1������0�����쳣��
	*/
	int8_t KeyValueHandler::put_or_update(const KVTableInfo& table, const Attrs& key, const Attrs& values,
		const vector<DriverUpdateMode> & updatemodes, const int64_t version) throw (ServerException)	{
			/** �ֲ߳̾���Ϣ */
			ThreadInfoMonitor infoMonitor(table, PUT_OR_UPDATE_API);

			/** Profile��Ϣ */
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

			set<u16> existCols;		/**	������Ϣ�а������� */

			byte *mysqlRow = (byte*)session->getMemoryContext()->calloc(tableDef->m_maxRecSize * sizeof(byte));
			buildRedRecord(tableDef, mysqlRow, key.attrlist, &existCols);

			// ��������¼
			Bitmap valueBitmap((void*)values.bmp.c_str(), values.bmp.size() * 8);
			buildRedRecord(tableDef, mysqlRow, values.attrlist, &existCols, &valueBitmap, true);

			bool success = false;

			IUSequence *iuSeq = NULL;
			try {
				/** ���Բ����¼ */
				iuSeq = currentTable->insertForDupUpdate(session, mysqlRow, false);

				success = true;
				if (iuSeq) {	/** ������ڳ�ͻ��¼�����������ظ��¸ü�¼ */
					++KeyValueServer::m_serverStats.putConflict;

					success = false;
					byte *dupRow = (byte*)session->getMemoryContext()->calloc(tableDef->m_maxRecSize * sizeof(byte));	/** �����ͻ��¼���� */
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
	*	��ñ���
	*	
	*	@exception	ServerException	�򿪱�������ߵ���NTSE�ӿڳ����쳣�ȡ�
	*
	*	@param	table	����Ϣ
	*
	*	@return	���ػ�õı���
	*/
	void KeyValueHandler::getTableDef(KVTableDef& _return, const KVTableInfo& table) throw (ServerException)	{
		/** �ֲ߳̾���Ϣ */
		ThreadInfoMonitor infoMonitor(table, GET_TABLEDEF_API);

		/** Profile��Ϣ */
		PROFILE(PI_KeyValue_getTableDef);

		SesSavePoint sessionSP(m_database, currentThreadInfo->connection);
		Session *session = sessionSP.getSession();

		Table *currentTable = openTable(session, table);

		/**	ֻ��Ԫ���������� */
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
	*	����������֤������¼
	*
	*	@param	tableDef	��¼���ڵı�Ķ���
	*	@param	oldContent	��֤��¼��ʹ��REC_MYSQL��ʽ
	*	@param	conds		��֤����
	*
	*	@return	�����¼ͨ����֤����true������false
	*/
	bool KeyValueHandler::validateByCond(TableDef *tableDef, byte *oldContent, const vector<Cond> &conds)	{
		for (vector<Cond>::const_iterator it = conds.begin(); it != conds.end(); ++it) {
			assert(it->valueOne.dataType == DataType::KV_COL);

			u16 colNumOne = *(u16*)it->valueOne.dataValue.c_str();
			ColumnDef *colDefOne = tableDef->m_columns[colNumOne];
			ColumnType colTypeOne = colDefOne->m_type;

			/*
			 *	�����IS NULL������ֱ���ж�
			 */
			if (it->op == Op::ISNULL) {
				if (!RedRecord::isNull(tableDef, oldContent, colNumOne)) {
					return false;
				}
				continue;
			}

			/**
			*	��ȡ�ж�ֵһ
			*	��ֵ���͵���ͳһ��double���ʹ洢�������������
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
			*	��ȡ�ж�ֵ��
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
			 *	����ж������еڶ�ֵҲ�Ǳ��е�ֵ��ֱ�ӴӼ�¼�л�ȡ
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
			*	��ʼ�ж�
			*/
			switch(it->op)	{
				case Op::EQ:
				case Op::GRATER:
				case Op::LESS:
				case Op::EQGRATER:
				case Op::EQLESS:
				case Op::NOTEQ:
					{
						/** ��ֵ�Ƚ� */
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
							/** �ַ����Ƚ� */						
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
								/** �ڴ����ݱȽ� */
								
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
						/** �����ֵ����NULLSAFEEQ���� */
						if (colTypeOne == CT_TINYINT || colTypeOne == CT_SMALLINT || colTypeOne == CT_INT
							|| colTypeOne == CT_BIGINT || colTypeOne == CT_MEDIUMINT
							|| colTypeOne == CT_FLOAT || colTypeOne == CT_DOUBLE) {
								bool eitherNull = (!numberOneExist) ^ (!numberTwoExist);	/** ������ֻ��һ��ΪNULL������false */
								bool bothNull = (!numberOneExist) && (!numberTwoExist);		/** �����߶���ΪNULL������true */
								if (eitherNull) {
									return false;
								} else if (bothNull) {
									break;
								}

								/** �����߶���ΪNULL�����бȽ� */
								if (!compareNumber(numberOne, numberTwo, Op::EQ)) {
									return false;
								}
								break;
						} else if (colTypeOne == CT_CHAR || colTypeOne == CT_VARCHAR) {
							/** ����ַ������͵�NULLSAFEEQ����������ͬ�� */
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
								/** ����ڴ��ֽ������͵�NULLSAFEEQ����������ͬ�� */
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

						/** ƥ���ַ����������������£�
						*	1)	pattern		--->	^pattern$
						*	2)	%pattern	--->	(.)*pattern$
						*	3)	pattern%	--->	^pattern(.)*
						*	4)	%pat%tern%	--->	(.)*pat(.)*tern(.)*
						*	4)	patt_ern	--->	patt(.){1}ern
						*/
						stringReplace(stringTwo, "%", "(.)*");
						stringReplace(stringTwo, "_", "(.){1}");

						/** ����boost������ʽ�Ŀ⺯�� */
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
	* �������¼����ȡ�����Ը���ʱ����Ҫ���ж�ֵ
	*
	*	@param	tableDef	����
	*	@param	mysqlRow	�����¼
	*	@param	colNo		���Ժ�
	*	@param	colType		�������
	*	@param	numberResult OUT�����Ϊ��ֵ���ͣ��򷵻���ֵ
	*	@param	numberExist	OUT�����Ϊ��ֵ�ͣ���Ϊtrue
	*	@param	stringResult OUT�����Ϊstring���ͣ������ַ���
	*	@param	stringExist	OUT�����Ϊstring�ͣ���Ϊtrue
	*	@param	memStreamResult OUT�����Ϊ�ڴ��ֽ����ȣ������ڴ��׵�ַ
	*	@param	memLength OUT�����Ϊ�ڴ��ֽ����ȣ������ֽ������ȣ�����0
	*	@param	memStreamExist	OUT�����Ϊ�ڴ��ֽ����ȣ���Ϊtrue
	*/
	void KeyValueHandler::extractCondValues(TableDef *tableDef, byte *mysqlRow, u16 colNo, ColumnType colType,
		double* numberResult, bool *numberExist, string *stringResult, bool *stringExist,
		void **memStreamResult, size_t *memLength, bool *memStreamExist)	{

			if (RedRecord::isNull(tableDef, mysqlRow, colNo)) 
				return;

			/**
			*	��Ӧ��Ϊ��ֵ����
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

			/** ���Ϊstring�ͣ���Record�ж�ȡstring */
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

			/** ���ΪBinary����Lob����Record�ж�ȡ�ֽ��� */
			if (colType == CT_BINARY || colType == CT_VARBINARY) {
				*memStreamExist = true;
				*memStreamResult = RedRecord::readRaw(tableDef, mysqlRow, colNo, NULL, memLength);
			} else if (colType == CT_SMALLLOB || colType == CT_MEDIUMLOB) {
				*memStreamExist = true;
				RedRecord::readLob(tableDef, mysqlRow, colNo, memStreamResult, memLength);
			}
	}

	/**
	 *	�������²������������ַ��������������߼�ֵ
	 *	
	 *	@param	tableDef	����
	 *	@param	conds		����
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
	*	�������¼����ȡ�������Ե�ֵ
	*	
	*	@param session �Ự��Դ
	*	@param tableDef	����
	*	@param content	�����¼
	*	@param col	���Ժ�
	*	@param length OUT����¼����
	*
	*	@return	��¼�׵�ַ
	*/
	byte* KeyValueHandler::extractRedRecord(Session * session, TableDef *tableDef, byte *content, int col, size_t *length)	{
		assert(col < tableDef->m_numCols);

		ColumnDef *colDef = tableDef->m_columns[col];

		switch(colDef->m_type)	{
			/** ������ֵ���ͣ�ֱ�Ӳ���ƫ�� */
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
	*	���ݸ��·�ʽupdatemodes���¸����������¼
	*	
	*	@param tableDef	����
	*	@param content	�����¼
	*	@param updatemodes	���·�ʽ
	*
	*	@exception ServerException	���ĳ�г��������쳣
	*/
	void KeyValueHandler::updateRowByMode(TableDef *tableDef, byte *content, const vector<DriverUpdateMode> &updatemodes)	{
		vector<DriverUpdateMode> setmodes;	/** SET�������� */

		for (vector<DriverUpdateMode>::const_iterator it = updatemodes.begin(); it != updatemodes.end(); ++it) {
			ColumnDef* colDef = tableDef->m_columns[it->attrNo];
			ColumnType colType = colDef->m_type;
			switch(it->mod)	{
				case Mode::INCR:
				case Mode::DECR:
					{
						/** ������ֵ�ĵ����͵ݼ� */
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
					/** ֱ��replace���� */
					setmodes.push_back(*it);
					break;
				case Mode::SETNULL:
					RedRecord::setNull(tableDef, content, it->attrNo);
					break;
				case Mode::PREPEND:
				case Mode::APPEND:
					{
						/** �ַ������͵�update */
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

		/** ��Set��Update�������н����������� */
		if (!setmodes.empty()) {
			buildRedRecord(tableDef, content, setmodes);
		}
	}

	/**
	*	��������keyת����NTSE��ʽKEY_PAD������������
	*
	*	@param	tableDef	����
	*	@param	idx			������
	*	@param	key			������keyֵ
	*	@param	outKey		IN&&OUT��IN��ʾҪȡ�������ԣ�OUTΪ����ֵ��ռ�ÿռ��С
	*   �����߱���Ϊ����������ݷ����㹻����ڴ棬����ͨ������key->m_size��֪
	*   �Ѿ�������ڴ��С����ֹԽ�硣��m_formatһ��ΪKEY_PAD
	*/
	void KeyValueHandler::transKey(TableDef *tableDef, const vector<Attr> &key, SubRecord *outKey)	{
		RedRecord redRecord(tableDef, REC_REDUNDANT);
		buildRedRecord(tableDef, redRecord.getRecord()->m_data, key);
		RecordOper::extractKeyRP(tableDef, redRecord.getRecord(), outKey);
	}

	/**
	*	�������¼����ȡ���������е�ֵ������װ��thrift��binary����(string)
	*
	*	@param	session		�Ự����
	*	@param	tableDef	����
	*	@param	mysqlRow	�����¼
	*	@param	attrs		��Ҫ��ȡֵ������������
	*
	*	@return	attrs���Զ�Ӧ��ֵ����һ��Ϊ��ֵλͼ�����������Ӧ��������ֵ��
	*/
	void KeyValueHandler::extractResult(Session *session, TableDef *tableDef, byte *mysqlRow, const vector<int16_t> & attrs,
		vector<string> & _return)	{

			_return.resize(attrs.size() + 1);	/** �����㹻��С�ķ���ֵ�ռ� */

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
	*	�򿪱���������ڱ��ϣ�в��ң���û���ٴ򿪶�Ӧ�ı��׳��쳣
	*	
	*	@param	session �Ự��Դ
	*	@param tabelInfo ����Ϣ
	*	@return �򿪱�Ľṹ
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
	*	����д��ʱ������¼�Ƿ񳬳�������ǣ������쳣
	*
	*	@param	tableDef	����˵õ��ı���
	*	@param	columnNo	�к�
	*	@param	newSize		д�����ݵĳ���
	*	@param	var			�Ƿ�Ϊvar����
	*	
	*	@exception	������������쳣
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
	 *	��������������������ص���
	 *
	 *	param	tableDef	����
	 *	param	colsSet		����кŵļ���
	 */
	void KeyValueHandler::getIndiceCols(TableDef *tableDef, set<u16> &colsSet) {
		for (u8 index = 0; index < tableDef->m_numIndice; ++index) {
			IndexDef *currentIndex = tableDef->m_indice[index];
			colsSet.insert(currentIndex->m_columns, currentIndex->m_columns + currentIndex->m_numCols);
		}
	}

#ifdef NTSE_UNIT_TEST
	/**
	*	�������ı�ṹ
	*/
	void KeyValueHandler::clearOpenTables()	{
		KeyValueServer::clearCachedTable();
	}
#endif
	/**
	*	�������ɨ����
	*
	*	@exception ServerException	���������ڣ����ߵ���NTSE�ӿڴ���
	*
	*	@param	session	�Ự����
	*	@param	activeTable	��ṹ
	*	@param	key		��������Ӧ�ļ�ֵ
	*	@param	attrs	Ҫ��ȡ��������
	*	@param	operType	��������
	*
	*	@return		����ɨ����
	*/
	TblScan* KeyValueHandler::getTblScanByKey(Session *session, Table *activeTable, const vector<Attr> & key, const ColList &attrs,
		OpType operType)	{
			assert(activeTable && key.size() > 0);

			TableDef* tableDef = activeTable->getTableDef();

			u8 activeIndexNo = tableDef->getIndexNo(tableDef->m_pkey->m_name);
			if (activeIndexNo == INVALID_INDEX_NO) {
				SERVER_EXCEPTION_THROW(KVErrorCode::KV_EC_NONEINDEX, "Cannot find the index of the given key.")
			}

			/** ����Key��������ȥ��ѯ�����е������� */
			void *p = session->getMemoryContext()->alloc(sizeof(SubRecord));
			SubRecord *indexKey = new (p)SubRecord(KEY_PAD, tableDef->m_pkey->m_numCols,tableDef->m_pkey->m_columns,
				(byte *)session->getMemoryContext()->alloc(tableDef->m_pkey->m_maxKeySize),tableDef->m_pkey->m_maxKeySize);
			transKey(tableDef, key, indexKey);	

			/** ����ɨ�� */
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
