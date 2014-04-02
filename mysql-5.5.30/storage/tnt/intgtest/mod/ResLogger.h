#ifndef _NTSESBLTEST_RES_LOGGER_H_
#define _NTSESBLTEST_RES_LOGGER_H_

#include "api/Database.h"
#include "util/Thread.h"
#include <fstream>

using namespace ntse;
using namespace std;

class ResLogger;

class ResLoggerTask : public Task{
public:
	ResLoggerTask(ResLogger *logger, uint interval)
		: m_logger(logger), Task("ResLoggerTask", interval) {

	}

	void run();

private:
	ResLogger *m_logger;
};
/**
 * 资源记录类
 * 定时记录数据库资源占用
 */
class ResLogger {
	friend class ResLoggerTask;

public:
	ResLogger(Database *db, uint interval, const char *filename);
	~ResLogger();

protected:
	void log();

private:
	Database *m_db;
	int	m_interval;
	ofstream m_os;
	ResLoggerTask* m_task;
};

#endif // _NTSESBLTEST_RES_LOGGER_H_

