/**
* KeyValue服务端
*
* @author 廖定柏(liaodingbai@corp.netease.com)
*/

#ifndef _KEYVALUE_SERVER__H_
#define _KEYVALUE_SERVER__H_

#include <concurrency/ThreadManager.h>
#include <concurrency/PosixThreadFactory.h>
#include <protocol/TBinaryProtocol.h>
#include <server/TSimpleServer.h>
#include <server/TThreadPoolServer.h>
#include <server/TNonblockingServer.h>
#include <TProcessor.h>
#include <server/TThreadedServer.h>
#include <transport/TServerSocket.h>
#include <list>
#include <sstream>
#include "util/Hash.h"

#ifndef WIN32
#include <epoll/TEpollServer.h>
#include <epoll/config.h>

using namespace apache::thrift::epoll;
#endif

using namespace apache::thrift;
using namespace apache::thrift::protocol;
using namespace apache::thrift::transport;
using namespace apache::thrift::server;
using namespace apache::thrift::concurrency;
using namespace std;

struct st_table;
typedef struct st_table TABLE;
class THD;

namespace ntse	{

	extern const char* processingStatusString[];
	extern const char* invokedMethodString[];
	extern const char* droppingTablePath;

	class Database;
	class Table;
	class Session;
	class Connection;
	struct Mutex;

	/** 标记服务端处理客户端请求的处理状态 */
	enum ProcessingStatus {
		CREATE_CONTEXT = 0,	/** TServerEventHandler::ClientBegin */
		PROCESS_CONTEXT = 1,
		PROCESS_BEGIN = 2,
		PROCESSING = 3,		/** Processor处理中 */
		PROCESS_EXCEPTION = 4,		/** Processor发生异常 */
		PROCESS_END = 5,
		FREE = 6,
		DELETE_CONTEXT = 7		/** TServerEventHandler::ClientEnd */
	};

	/** 客户端请求调用的API */
	enum InvokedMethod {
		GET_API = 0,
		MULTI_GET_API = 1,
		PUT_API = 2,
		SET_API = 3,
		REPLACE_API = 4,
		REMOVE_API = 5,
		UPDATE_API = 6,
		PUT_OR_UPDATE_API = 7,
		GET_TABLEDEF_API = 8,
		CREATE_CONTEXT_API = 9,
		PROCESS_CONTEXT_API = 10,
		DELETE_CONTEXT_API = 11,
		NONE = 12
	};

	/** 处理客户端请求的线程信息 */
	typedef struct ThreadLocalInfo {
		string clientIP;
		int port;
		Connection *connection;
		string tablePath;
		InvokedMethod api;
		u64 apiStartTime;
		ProcessingStatus status;
		stringstream information;

		ThreadLocalInfo() {
			clientIP = tablePath = "";
			port = apiStartTime = -1;
			connection = NULL;
		}
	} ThreadLocalInfo;

	/** 比较线程局部信息是否相等的函数对象 */
	class  ThreadStatusComparator {
	public:
		ThreadStatusComparator(ThreadLocalInfo *info) : m_info(info)	{}

		inline bool operator()(const ThreadLocalInfo *rhs) const {
			/*
			 *	此处状态不用相等，为了避免发生在PROCESSBEGIN~PROCESSEND之间的操作引起r64917中提到的一个问题
			 */
			return m_info->tablePath == rhs->tablePath
				&& (rhs->status >= PROCESS_BEGIN && rhs->status <= PROCESS_END);
		}
	private:
		ThreadLocalInfo *m_info;
	};

	/** thrift服务端的类型 */
	enum ThriftServerType	{
		SIMPLE = 0,			/** Simple Server */
		POOL = 1,			/** Thread Pool Server */
		THREADED = 2,		/** Threaded Server */
		NOBLOCK = 3,		/** Non-blocking Server */
		EPOLL = 4			/** Epoll Server */
	};

	/** thrift服务端详细配置 */
	typedef struct ThriftConfig	{
		int serverType;		/** 对应于ThriftServerType的值 */
		int port;			/** 端口号 */
		int threadNum;		/** 线程数（针对thread pool） */
	} ThriftConfig;

	/** Key value服务端的一些状态统计 */
	typedef struct KeyValueServerStats	{
		/** Get操作的一些统计 */
		long getHits;
		long getMisses;
		long getFailed;

		/** Put操作的统计 */
		long putSuccess;
		long putFailed;
		long putConflict;

		/** Update操作的统计 */
		long updateSuccess;
		long updateFailed;
		long updateConfirmedByCond;

		/** Delete操作的统计 */
		long deleteSuccess;
		long deleteFailed;

		KeyValueServerStats()	{
			getHits = getMisses = getFailed = 0;
			putConflict = putFailed = putSuccess = 0;
			updateConfirmedByCond = updateFailed = updateSuccess = 0;
			deleteFailed = deleteSuccess = 0;
		}
	} KeyValueServerStats;

	/**
	 *	Keyvalue使用中的表的额外信息
	 */
	struct KVTableInfoEx	{
		char	*m_path;		/** 表对应的路径 */
		Table	*m_table;		/** NTSE内部表对象 */

	public:
		KVTableInfoEx(const char *path, Table *table) {
			m_path = System::strdup(path);
			m_table = table;
		}

		~KVTableInfoEx() {
			delete []m_path;
			m_path = NULL;
		}
	};

	/** 比较路径与Keyvalue Table是否相等的函数对象 */
	class KVTableEqualer {
	public:
		inline bool operator()(const char *path, const KVTableInfoEx *table) const {
			return strcmp(path, table->m_path) == 0;
		}
	};

	/** 计算Keyvalue Table对象哈希值的函数对象 */
	class KVTableHasher {
	public:
		inline unsigned int operator()(const KVTableInfoEx *table) const {
			return m_strHasher.operator ()(table->m_path);
		}

	private:
		Hasher<const char *>	m_strHasher;
	};

	typedef DynHash<const char*, KVTableInfoEx*, KVTableHasher, Hasher<const char*>, KVTableEqualer> KVTblHasher;

	/** KeyValue服务端的管理 */
	class KeyValueServer : public Thread	{
	public:
		static KeyValueServerStats m_serverStats; /** 服务端统计信息 */
		static list<ThreadLocalInfo*> m_threadInfos;
		static Mutex	m_threadInfoLock;

	public:
		/**
		*	构造函数
		*
		*	@param	config	服务端配置
		*	@param	database	ntse数据库实例
		*/
		KeyValueServer(ThriftConfig *config, Database *database)
			: Thread("KeyValue Server Thread"), m_database(database), m_serverConfig(config), m_hasOpen(false) {
		}

		/**
		 *	释放资源
		 */
		~KeyValueServer()	{
			close();
			delete m_thriftServer;
			m_thriftServer = NULL;
		}

		void run();
		void close();

		static Table* getTable(Database *db, Session *session, const char* tablePath);
		static void waitTableFinished(const char *path);
		static bool closeDDLTable(Database *db, Session *session, const char* tablePath, bool ntseClose = true);
		static TABLE* openMysqlTable(THD *thd, u16 tableId, const char *db, const char *table_name);
		static void closeMysqlTable(u16 tableId);

		/**
		 *	清空缓存的表结构
		 */
		inline static void clearCachedTable()	{
			LOCK(&m_tableDefLock);
			// To Do: memory leak
			m_cachedTables.clear();
			UNLOCK(&m_tableDefLock);
		}

		/*
		 *	当DDL操作结束，重置被DDL操作的表
		 */
		inline static void resetDDLTable() {
			m_waitDDLTable = NULL;
		}

		/**
		*	返回服务端的状态（关闭或者打开）
		*
		*	@return True,如果服务端打开，否则false
		*/
		inline bool isOpen()	{
			return m_hasOpen;
		}

		/**
		 *	得到服务端的统计信息
		 *
		 *	@return	服务端的统计信息
		 */
		inline KeyValueServerStats getServerStats()	{
			return m_serverStats;
		}
	private:
		ThriftConfig *m_serverConfig;	/** 服务端配置选项 */
		Database *m_database;			/** thrift操作的数据库 */
		TServer *m_thriftServer;		/** Key-value服务端 */
		bool m_hasOpen;					/** 服务端是否打开 */

		static Hash<u16, TABLE*> m_idToMysqlTable;
		static Mutex mysqlTableMutex;
		static const char* m_waitDDLTable;
		static KVTblHasher m_cachedTables;
		static Mutex	m_tableDefLock;
	};
}
#endif