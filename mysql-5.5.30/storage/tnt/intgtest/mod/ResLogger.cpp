#include "ResLogger.h"


/**
 * @param db 数据库
 * @param interval 输出时间间隔，单位秒
 * @param filename 输出文件名
 */
ResLogger::ResLogger(Database *db, uint interval, const char *filename)
	: m_db(db), m_interval(interval){
	m_os.open(filename);
	m_os << "Time\tSessions(#)\tMemoryCtx(K)" << endl;
	m_task = new ResLoggerTask(this, 1000 * interval);
	m_task->start();
}


ResLogger::~ResLogger() {
	m_task->stop();
	delete m_task;
	m_os.close();
}

void ResLogger::log() {
	SessionManager *sm = m_db->getSessionManager();
	SesScanHandle* hdl = sm->scanSessions();
	const Session *session;
	
	u64 totalMemory = 0;
	while ((session = sm->getNext(hdl)) != 0) {
		totalMemory += session->getLobContext()->getMemUsage();
		totalMemory += session->getMemoryContext()->getMemUsage();
	}
	sm->endScan(hdl);

	u16 numSessions = sm->getActiveSessions();

	time_t now = time(NULL);
	char timestr[30];
	System::formatTime(timestr, sizeof(timestr), &now);
	m_os << timestr << "\t" << numSessions << "\t" << totalMemory / 1024 << endl;
}

void ResLoggerTask::run() {
	m_logger->log();
}

