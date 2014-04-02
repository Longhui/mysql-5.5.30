/**
* KeyValue�����
*
* @author �ζ���(liaodingbai@corp.netease.com)
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

	/** ��Ƿ���˴���ͻ�������Ĵ���״̬ */
	enum ProcessingStatus {
		CREATE_CONTEXT = 0,	/** TServerEventHandler::ClientBegin */
		PROCESS_CONTEXT = 1,
		PROCESS_BEGIN = 2,
		PROCESSING = 3,		/** Processor������ */
		PROCESS_EXCEPTION = 4,		/** Processor�����쳣 */
		PROCESS_END = 5,
		FREE = 6,
		DELETE_CONTEXT = 7		/** TServerEventHandler::ClientEnd */
	};

	/** �ͻ���������õ�API */
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

	/** ����ͻ���������߳���Ϣ */
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

	/** �Ƚ��ֲ߳̾���Ϣ�Ƿ���ȵĺ������� */
	class  ThreadStatusComparator {
	public:
		ThreadStatusComparator(ThreadLocalInfo *info) : m_info(info)	{}

		inline bool operator()(const ThreadLocalInfo *rhs) const {
			/*
			 *	�˴�״̬������ȣ�Ϊ�˱��ⷢ����PROCESSBEGIN~PROCESSEND֮��Ĳ�������r64917���ᵽ��һ������
			 */
			return m_info->tablePath == rhs->tablePath
				&& (rhs->status >= PROCESS_BEGIN && rhs->status <= PROCESS_END);
		}
	private:
		ThreadLocalInfo *m_info;
	};

	/** thrift����˵����� */
	enum ThriftServerType	{
		SIMPLE = 0,			/** Simple Server */
		POOL = 1,			/** Thread Pool Server */
		THREADED = 2,		/** Threaded Server */
		NOBLOCK = 3,		/** Non-blocking Server */
		EPOLL = 4			/** Epoll Server */
	};

	/** thrift�������ϸ���� */
	typedef struct ThriftConfig	{
		int serverType;		/** ��Ӧ��ThriftServerType��ֵ */
		int port;			/** �˿ں� */
		int threadNum;		/** �߳��������thread pool�� */
	} ThriftConfig;

	/** Key value����˵�һЩ״̬ͳ�� */
	typedef struct KeyValueServerStats	{
		/** Get������һЩͳ�� */
		long getHits;
		long getMisses;
		long getFailed;

		/** Put������ͳ�� */
		long putSuccess;
		long putFailed;
		long putConflict;

		/** Update������ͳ�� */
		long updateSuccess;
		long updateFailed;
		long updateConfirmedByCond;

		/** Delete������ͳ�� */
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
	 *	Keyvalueʹ���еı�Ķ�����Ϣ
	 */
	struct KVTableInfoEx	{
		char	*m_path;		/** ���Ӧ��·�� */
		Table	*m_table;		/** NTSE�ڲ������ */

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

	/** �Ƚ�·����Keyvalue Table�Ƿ���ȵĺ������� */
	class KVTableEqualer {
	public:
		inline bool operator()(const char *path, const KVTableInfoEx *table) const {
			return strcmp(path, table->m_path) == 0;
		}
	};

	/** ����Keyvalue Table�����ϣֵ�ĺ������� */
	class KVTableHasher {
	public:
		inline unsigned int operator()(const KVTableInfoEx *table) const {
			return m_strHasher.operator ()(table->m_path);
		}

	private:
		Hasher<const char *>	m_strHasher;
	};

	typedef DynHash<const char*, KVTableInfoEx*, KVTableHasher, Hasher<const char*>, KVTableEqualer> KVTblHasher;

	/** KeyValue����˵Ĺ��� */
	class KeyValueServer : public Thread	{
	public:
		static KeyValueServerStats m_serverStats; /** �����ͳ����Ϣ */
		static list<ThreadLocalInfo*> m_threadInfos;
		static Mutex	m_threadInfoLock;

	public:
		/**
		*	���캯��
		*
		*	@param	config	���������
		*	@param	database	ntse���ݿ�ʵ��
		*/
		KeyValueServer(ThriftConfig *config, Database *database)
			: Thread("KeyValue Server Thread"), m_database(database), m_serverConfig(config), m_hasOpen(false) {
		}

		/**
		 *	�ͷ���Դ
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
		 *	��ջ���ı�ṹ
		 */
		inline static void clearCachedTable()	{
			LOCK(&m_tableDefLock);
			// To Do: memory leak
			m_cachedTables.clear();
			UNLOCK(&m_tableDefLock);
		}

		/*
		 *	��DDL�������������ñ�DDL�����ı�
		 */
		inline static void resetDDLTable() {
			m_waitDDLTable = NULL;
		}

		/**
		*	���ط���˵�״̬���رջ��ߴ򿪣�
		*
		*	@return True,�������˴򿪣�����false
		*/
		inline bool isOpen()	{
			return m_hasOpen;
		}

		/**
		 *	�õ�����˵�ͳ����Ϣ
		 *
		 *	@return	����˵�ͳ����Ϣ
		 */
		inline KeyValueServerStats getServerStats()	{
			return m_serverStats;
		}
	private:
		ThriftConfig *m_serverConfig;	/** ���������ѡ�� */
		Database *m_database;			/** thrift���������ݿ� */
		TServer *m_thriftServer;		/** Key-value����� */
		bool m_hasOpen;					/** ������Ƿ�� */

		static Hash<u16, TABLE*> m_idToMysqlTable;
		static Mutex mysqlTableMutex;
		static const char* m_waitDDLTable;
		static KVTblHasher m_cachedTables;
		static Mutex	m_tableDefLock;
	};
}
#endif