/**
* KeyValue Server事件监管
*
* @author 廖定柏(liaodingbai@corp.netease.com)
*/

#ifndef _KEYVALUE_EVENT_HANDLER_H
#define _KEYVALUE_EVENT_HANDLER_H

#include <server/TServer.h>
#include <transport/TTransport.h>
#include <protocol/TProtocol.h>
#include <util/Portable.h>

using boost::shared_ptr;
using apache::thrift::server::TServerEventHandler;
using apache::thrift::protocol::TProtocol;
using apache::thrift::transport::TTransport;

namespace ntse {
	
	class Database;
	struct ThreadLocalInfo;

	/**
	*	该类主要负责监管服务端处理客户端请求时的一些有效信息，例如客户端Host、Port等，以及服务调用时间和状态等
	*/
	class KeyValueEventHandler : public TServerEventHandler {
	public:
		KeyValueEventHandler(Database *database)
			: m_database(database) {}

		void* createContext(shared_ptr<TProtocol> input, shared_ptr<TProtocol> output);
		void deleteContext(void* serverContext,	shared_ptr<TProtocol>input, shared_ptr<TProtocol>output);
		void processContext(void* serverContext, shared_ptr<TTransport> transport);
	private:
		Database *m_database;	/** 数据库对象，为每个客户端分配Connection */
	};

	extern TLS ThreadLocalInfo* currentThreadInfo;
}


#endif