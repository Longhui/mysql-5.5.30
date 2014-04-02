/**
* KeyValue����˴���
*
* @author �ζ���(liaodingbai@corp.netease.com)
*/

#ifdef NTSE_KEYVALUE_SERVER

#include "KeyValueServer.h"
#include "KeyValueEventHandler.h"
#include "KeyValueHandler.h"
#include "api/Table.h"
#include "api/Database.h"
#include "util/Sync.h"
#include "util/Thread.h"
#include <algorithm>

#ifndef __WIN__
using namespace apache::thrift::epoll;
#endif

using namespace std;
/*
 *	mysql�����ͱ���
 */
extern int closefrm(TABLE*, bool);
extern uint create_table_def_key(THD *thd, char *key, TABLE_LIST *table_list, bool tmp_table);
extern int open_table_from_share(THD *thd, TABLE_SHARE *share, const char *alias,
								 uint db_stat, uint prgflag, uint ha_open_flags,
								 TABLE *outparam, bool is_create_table);
extern TABLE_SHARE *get_table_share(THD *thd, TABLE_LIST *table_list, char *key,
									uint key_length, uint db_flags, int *error);
extern void release_table_share(TABLE_SHARE *share, enum release_type type);
extern pthread_mutex_t LOCK_open;

namespace ntse	{

	/** ����״̬��Ӧ���ַ��� */
	const char* processingStatusString[] = {
		"CREATE_CONTEXT", "PROCESS_CONTEXT", "PROCESS_BEGIN", "PROCESSING", "PROCESS_EXCEPTION",
		"PROCESS_END", "FREE", "DELETE_CONTEXT"
	};

	/** �ͻ����������API��Ӧ���ַ��� */
	const char* invokedMethodString[] = {
		"GET", "MULTI_GET", "PUT", "SET", "REPLACE", "REMOVE", "UPDATE", "PUT_OR_UPDATE", "GET_TABLEDEF",
		"CREATE_CONTEXT", "PROCESS_CONTEXT", "DELETE_CONTEXT", "NONE"
	};

	/**
	 *	������ڱ�ɾ���ı�
	 *	@note: ����ע����̵߳Ĳ����޸�
	 */
	const char* KeyValueServer::m_waitDDLTable = NULL;

	KeyValueServerStats KeyValueServer::m_serverStats;	/** �����ͳ����Ϣ */

	KVTblHasher KeyValueServer::m_cachedTables;		/** ����򿪵�ntse table. */

	list<ThreadLocalInfo*> KeyValueServer::m_threadInfos; /** ����˴���ͻ����̵߳���Ϣ */

	/** �����Ѿ��򿪵ı���� */
	Mutex KeyValueServer::m_tableDefLock("KeyValueServer::cachedTables Lock", __FILE__, __LINE__);

	/** �����ͻ����߳���Ϣ���� */
	Mutex KeyValueServer::m_threadInfoLock("KeyValueServer::threadInfos Lock", __FILE__, __LINE__);

	Hash<u16, TABLE*> KeyValueServer::m_idToMysqlTable(1000);	/** ����򿪵�binlog table. */
	Mutex KeyValueServer::mysqlTableMutex("MysqlTableMutex", __FILE__, __LINE__);	/**	����keyvalue binlog�򿪵ı� */

	/**
	 *	����Server���ã���KeyValue�����
	 */
	void KeyValueServer::run()	{
		/** ���ΪThreadPoolʱ����Ҫ�趨pool size */
		int workerCount = m_serverConfig->threadNum;
		ThriftServerType serverType = (ThriftServerType)m_serverConfig->serverType;

		boost::shared_ptr<TProtocolFactory> protocolFactory(new TBinaryProtocolFactory());
		boost::shared_ptr<KeyValueHandler> handler(new KeyValueHandler(m_database));
		boost::shared_ptr<TProcessor> processor(new KVProcessor(handler));
		boost::shared_ptr<TServerTransport> serverTransport(new TServerSocket(m_serverConfig->port));
		boost::shared_ptr<TTransportFactory> transportFactory(new TBufferedTransportFactory());
		boost::shared_ptr<TServerEventHandler> keyvalueEventHandler(new KeyValueEventHandler(m_database));
		/** ���ݲ�ͬ�ķ��������ѡ�� */
		switch (serverType)
		{
		case SIMPLE:
			{
				m_thriftServer = new  TSimpleServer(processor, serverTransport, transportFactory, protocolFactory);

				printf("Starting the simple server...\n");
			}
			break;

		case POOL:
			{
				boost::shared_ptr<ThreadManager> threadManager = ThreadManager::newSimpleThreadManager(workerCount);
				boost::shared_ptr<PosixThreadFactory> threadFactory = boost::shared_ptr<PosixThreadFactory>(new PosixThreadFactory());
				threadManager->threadFactory(threadFactory);
				threadManager->start();

				m_thriftServer = new TThreadPoolServer(processor,serverTransport,transportFactory,protocolFactory,threadManager);	

				printf("Starting the ThreadPoolServer server...\n");
			}
			break;
		case THREADED:
			{
				m_thriftServer = new TThreadedServer(processor,serverTransport,transportFactory,protocolFactory);	

				printf("Starting the threaded server...\n");
			}
			break;
		case NOBLOCK:
			{
				//boost::shared_ptr<ThreadManager> threadManager = ThreadManager::newSimpleThreadManager(workerCount);
				//boost::shared_ptr<PosixThreadFactory> threadFactory =  boost::shared_ptr<PosixThreadFactory>(new PosixThreadFactory());
				//threadManager->threadFactory(threadFactory);
				//threadManager->start();

				//m_thriftServer = new TNonblockingServer(processor, protocolFactory, m_serverConfig->port, threadManager);
				m_thriftServer = new TNonblockingServer(processor, m_serverConfig->port);
				printf("Starting the NonblockingServer server...\n");
			}
			break;
#ifndef __WIN__
		case EPOLL:
			{
				config conf;
				stringstream ss;

				ss << workerCount;
				conf["num_threads"] = ss.str();

				ss.str("");
				ss << m_serverConfig->port;
				conf["port"] = ss.str();
				m_thriftServer = new TEpollServer(processor, conf);
				printf("Starting the EpollServer server...\n");
			}
			break;
#endif
		}
		
		m_hasOpen = true;
		m_thriftServer->setServerEventHandler(keyvalueEventHandler);
		m_thriftServer->serve();
	}

	/**
	 *	�ر�Thrift��������
	 */
	void KeyValueServer::close()	{
		if (m_hasOpen) {
			m_thriftServer->stop();
		}
		Thread::msleep(1000);
		m_hasOpen = false;
	}

	/**
	 *	����Handler����Ҫ��Table�ṹ
	 *
	 *	@param	db			���ݿ����
	 *	@param	session		�Ự��Դ
	 *	@param	tablePath	��·��
	 *
	 *	@return	�õ��ı�ṹ	
	 */
	Table* KeyValueServer::getTable(Database *db, Session *session, const char* tablePath) {
		KVTableInfoEx* tableInfo = NULL;

		if (m_waitDDLTable && !System::stricmp(tablePath, m_waitDDLTable)) {
			SERVER_EXCEPTION_THROW(KVErrorCode::KV_EC_TABLE_IN_DROPPING, "The table is being delete")
		}
		
		LOCK(&m_tableDefLock);
		/** �����cache�в�ѯ����ֱ�ӷ��� */
		tableInfo = m_cachedTables.get(tablePath);
		if (!tableInfo || !tableInfo->m_table) {
			Table *ntseTable = NULL;
			try	{
				ntseTable = db->openTable(session, tablePath);
			} catch(NtseException &exp)	{
				UNLOCK(&m_tableDefLock);
				SERVER_EXCEPTION_THROW(exceptToKVErrorCode(exp.getErrorCode()), exp.getMessage())
			}

			tableInfo = new KVTableInfoEx(tablePath, ntseTable);
			m_cachedTables.put(tableInfo);
		}
		UNLOCK(&m_tableDefLock);

		return tableInfo->m_table;
	}

	/**
	*	�ȴ����еĿͻ����߳���ɶ�����ɾ����Ĳ���
	*
	*	@param	path ����ɾ�����·��
	*/
	void KeyValueServer::waitTableFinished(const char *path) {
		ThreadLocalInfo processThread;
		processThread.tablePath = path;

		while(true) {
			list<ThreadLocalInfo*>::iterator result = m_threadInfos.begin();
			result = find_if(result, m_threadInfos.end(), ThreadStatusComparator(&processThread));
			if (result == m_threadInfos.end()) {
				break;
			}
		}
	}

	/**
	 *	Ϊbinlog�Ĳ�����һ�ű��������ֲ��keyvalue�������Ϊkeyvalue������mysql�ϲ㣬binlog�����ı�
	 *  û���ܱ��򿪣���
	 *
	 *	@param	thd		�߳�
	 *	@param	path	��ID
	 *	@param	db		���Ӧ�����ݿ�
	 *	@param	table_name	����
	 *	@return ��mysql��Ľṹ
	 */
	TABLE* KeyValueServer::openMysqlTable(THD *thd, u16 tableId, const char *db, const char *table_name) {
		int error = 0;
		TABLE *tmp_table;
		TABLE_SHARE *share;
		char cache_key[MAX_DBKEY_LENGTH];
		uint key_length;
		TABLE_LIST table_list;

		/** ����Ѿ������ڻ����У�ֱ��ȡ���� */
		MutexGuard guard(&mysqlTableMutex, __FILE__, __LINE__);
		if(!(tmp_table = m_idToMysqlTable.get(tableId)))	{
			table_list.db=         (char*) db;
			table_list.table_name= (char*) table_name;
			/* Create the cache_key for temporary tables */
			key_length= create_table_def_key(thd, cache_key, &table_list, 0);

			if (!(tmp_table= (TABLE*) my_malloc(sizeof(*tmp_table), MYF(MY_WME))))
				return NULL;

			pthread_mutex_lock(&LOCK_open);
			if (!(share = (get_table_share(thd, &table_list, cache_key, key_length, 0, &error)))) {
				pthread_mutex_unlock(&LOCK_open);
				return NULL;
			}

			if (open_table_from_share(thd, share, table_name, 0, 0, 0, tmp_table, false)) {
				release_table_share(share, RELEASE_NORMAL);
				pthread_mutex_unlock(&LOCK_open);
				return NULL;
			}
			pthread_mutex_unlock(&LOCK_open);

			m_idToMysqlTable.put(tableId, tmp_table);
		}

		return tmp_table;
	}

	/*
	 *	�ر�binlog�򿪵ı�
	 *	@param	tableId ��ID
	 */
	void KeyValueServer::closeMysqlTable(u16 tableId) {
		TABLE *keyvalueTable;
		LOCK(&mysqlTableMutex);
		if( NULL != (keyvalueTable = m_idToMysqlTable.get(tableId))) {
			closefrm(keyvalueTable, true);
			m_idToMysqlTable.remove(tableId);
		}
		UNLOCK(&mysqlTableMutex);
	}

	/**
	 *	�ر�keyvalue�򿪵ı�
	 *
	 *	@param	db			���ݿ�
	 *	@param	session		�Ự
	 *	@param	tablePath	��·��
	 */
	bool KeyValueServer::closeDDLTable(Database *db, Session *session, const char* tablePath, bool ntseClose/* = true */) {
		m_waitDDLTable = tablePath;
		waitTableFinished(tablePath);

		LOCK(&m_tableDefLock);
		KVTableInfoEx *cachedTable = m_cachedTables.get(tablePath);
		UNLOCK(&m_tableDefLock);

		/** ����Ӧ�ò���Ҫm_tableDefLock��Ҳ�ǰ�ȫ�� */
		if (ntseClose && cachedTable != NULL) {
			db->closeTable(session, cachedTable->m_table);
		}

		LOCK(&m_tableDefLock);
		m_cachedTables.remove(tablePath);
		UNLOCK(&m_tableDefLock);

		return cachedTable != NULL;
	}

}

#endif
