/**
* KeyValue Server事件监管
*
* @author 廖定柏(liaodingbai@corp.netease.com)
*/

#ifdef NTSE_KEYVALUE_SERVER

#include "KeyValueEventHandler.h"
#include "KeyValueServer.h"
#include <transport/TBufferTransports.h>
#include <transport/TSocket.h>
#include <util/Portable.h>
#include <util/System.h>
#include <api/Database.h>

using boost::shared_ptr;
using apache::thrift::protocol::TProtocol;
using apache::thrift::transport::TSocket;
using apache::thrift::transport::TBufferedTransport;
using apache::thrift::server::TServerEventHandler;

using namespace ntse;

namespace ntse {

	TLS	ThreadLocalInfo* currentThreadInfo;

	void* KeyValueEventHandler::createContext(shared_ptr<TProtocol> input, shared_ptr<TProtocol> output) {
		ThreadLocalInfo *threadInfo = new ThreadLocalInfo();
		threadInfo->connection = m_database->getConnection(false);
		threadInfo->apiStartTime = -1;
		threadInfo->status = CREATE_CONTEXT;
		threadInfo->api = CREATE_CONTEXT_API;

		LOCK(&KeyValueServer::m_threadInfoLock);
		KeyValueServer::m_threadInfos.push_back(threadInfo);
		UNLOCK(&KeyValueServer::m_threadInfoLock);

		return threadInfo;
	}

	void KeyValueEventHandler::deleteContext(void* serverContext, shared_ptr<TProtocol>input, shared_ptr<TProtocol>output) {
		ThreadLocalInfo *threadInfo = (ThreadLocalInfo*)serverContext;
		threadInfo->status = DELETE_CONTEXT;
		threadInfo->api = DELETE_CONTEXT_API;
		threadInfo->information.str("");
		m_database->freeConnection(threadInfo->connection);

		LOCK(&KeyValueServer::m_threadInfoLock);
		KeyValueServer::m_threadInfos.remove(threadInfo);
		UNLOCK(&KeyValueServer::m_threadInfoLock);

		delete threadInfo;
		threadInfo = NULL;
	}

	void KeyValueEventHandler::processContext(void* serverContext, shared_ptr<TTransport> transport) {
		ThreadLocalInfo *threadInfo = (ThreadLocalInfo*)serverContext;
		
		TSocket *inSoc = (TSocket*)transport.get();
		threadInfo->clientIP = inSoc->getPeerAddress();
		threadInfo->port = inSoc->getPeerPort();
		threadInfo->status = PROCESS_CONTEXT;
		threadInfo->api = PROCESS_CONTEXT_API;

		/**	将当前的线程信息赋给局部线程信息 */
		currentThreadInfo = threadInfo;
	}
}

#endif