/**
* KeyValue服务端代码
*
* @author 廖定柏(liaodingbai@corp.netease.com)
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
 *	mysql函数和变量
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

	/** 处理状态对应的字符串 */
	const char* processingStatusString[] = {
		"CREATE_CONTEXT", "PROCESS_CONTEXT", "PROCESS_BEGIN", "PROCESSING", "PROCESS_EXCEPTION",
		"PROCESS_END", "FREE", "DELETE_CONTEXT"
	};

	/** 客户端请求调用API对应的字符串 */
	const char* invokedMethodString[] = {
		"GET", "MULTI_GET", "PUT", "SET", "REPLACE", "REMOVE", "UPDATE", "PUT_OR_UPDATE", "GET_TABLEDEF",
		"CREATE_CONTEXT", "PROCESS_CONTEXT", "DELETE_CONTEXT", "NONE"
	};

	/**
	 *	标记正在被删除的表
	 *	@note: 需有注意多线程的并发修改
	 */
	const char* KeyValueServer::m_waitDDLTable = NULL;

	KeyValueServerStats KeyValueServer::m_serverStats;	/** 服务端统计信息 */

	KVTblHasher KeyValueServer::m_cachedTables;		/** 缓存打开的ntse table. */

	list<ThreadLocalInfo*> KeyValueServer::m_threadInfos; /** 服务端处理客户端线程的信息 */

	/** 保护已经打开的表的锁 */
	Mutex KeyValueServer::m_tableDefLock("KeyValueServer::cachedTables Lock", __FILE__, __LINE__);

	/** 保护客户端线程信息的锁 */
	Mutex KeyValueServer::m_threadInfoLock("KeyValueServer::threadInfos Lock", __FILE__, __LINE__);

	Hash<u16, TABLE*> KeyValueServer::m_idToMysqlTable(1000);	/** 缓存打开的binlog table. */
	Mutex KeyValueServer::mysqlTableMutex("MysqlTableMutex", __FILE__, __LINE__);	/**	保护keyvalue binlog打开的表 */

	/**
	 *	根据Server配置，打开KeyValue服务端
	 */
	void KeyValueServer::run()	{
		/** 如果为ThreadPool时，需要设定pool size */
		int workerCount = m_serverConfig->threadNum;
		ThriftServerType serverType = (ThriftServerType)m_serverConfig->serverType;

		boost::shared_ptr<TProtocolFactory> protocolFactory(new TBinaryProtocolFactory());
		boost::shared_ptr<KeyValueHandler> handler(new KeyValueHandler(m_database));
		boost::shared_ptr<TProcessor> processor(new KVProcessor(handler));
		boost::shared_ptr<TServerTransport> serverTransport(new TServerSocket(m_serverConfig->port));
		boost::shared_ptr<TTransportFactory> transportFactory(new TBufferedTransportFactory());
		boost::shared_ptr<TServerEventHandler> keyvalueEventHandler(new KeyValueEventHandler(m_database));
		/** 根据不同的服务端类型选择 */
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
	 *	关闭Thrift服务连接
	 */
	void KeyValueServer::close()	{
		if (m_hasOpen) {
			m_thriftServer->stop();
		}
		Thread::msleep(1000);
		m_hasOpen = false;
	}

	/**
	 *	返回Handler层需要的Table结构
	 *
	 *	@param	db			数据库对象
	 *	@param	session		会话资源
	 *	@param	tablePath	表路径
	 *
	 *	@return	得到的表结构	
	 */
	Table* KeyValueServer::getTable(Database *db, Session *session, const char* tablePath) {
		KVTableInfoEx* tableInfo = NULL;

		if (m_waitDDLTable && !System::stricmp(tablePath, m_waitDDLTable)) {
			SERVER_EXCEPTION_THROW(KVErrorCode::KV_EC_TABLE_IN_DROPPING, "The table is being delete")
		}
		
		LOCK(&m_tableDefLock);
		/** 如果在cache中查询到，直接返回 */
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
	*	等待现有的客户端线程完成对正在删除表的操作
	*
	*	@param	path 正在删除表的路径
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
	 *	为binlog的操作打开一张表（仅仅针对植入keyvalue服务后，因为keyvalue不调用mysql上层，binlog操作的表并
	 *  没有能被打开）。
	 *
	 *	@param	thd		线程
	 *	@param	path	表ID
	 *	@param	db		表对应的数据库
	 *	@param	table_name	表名
	 *	@return 打开mysql表的结构
	 */
	TABLE* KeyValueServer::openMysqlTable(THD *thd, u16 tableId, const char *db, const char *table_name) {
		int error = 0;
		TABLE *tmp_table;
		TABLE_SHARE *share;
		char cache_key[MAX_DBKEY_LENGTH];
		uint key_length;
		TABLE_LIST table_list;

		/** 如果已经存在在缓存中，直接取返回 */
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
	 *	关闭binlog打开的表
	 *	@param	tableId 表ID
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
	 *	关闭keyvalue打开的表
	 *
	 *	@param	db			数据库
	 *	@param	session		会话
	 *	@param	tablePath	表路径
	 */
	bool KeyValueServer::closeDDLTable(Database *db, Session *session, const char* tablePath, bool ntseClose/* = true */) {
		m_waitDDLTable = tablePath;
		waitTableFinished(tablePath);

		LOCK(&m_tableDefLock);
		KVTableInfoEx *cachedTable = m_cachedTables.get(tablePath);
		UNLOCK(&m_tableDefLock);

		/** 这里应该不需要m_tableDefLock，也是安全的 */
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
