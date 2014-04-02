/**
* KeyValue Server�¼����
*
* @author �ζ���(liaodingbai@corp.netease.com)
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
	*	������Ҫ�����ܷ���˴���ͻ�������ʱ��һЩ��Ч��Ϣ������ͻ���Host��Port�ȣ��Լ��������ʱ���״̬��
	*/
	class KeyValueEventHandler : public TServerEventHandler {
	public:
		KeyValueEventHandler(Database *database)
			: m_database(database) {}

		void* createContext(shared_ptr<TProtocol> input, shared_ptr<TProtocol> output);
		void deleteContext(void* serverContext,	shared_ptr<TProtocol>input, shared_ptr<TProtocol>output);
		void processContext(void* serverContext, shared_ptr<TTransport> transport);
	private:
		Database *m_database;	/** ���ݿ����Ϊÿ���ͻ��˷���Connection */
	};

	extern TLS ThreadLocalInfo* currentThreadInfo;
}


#endif