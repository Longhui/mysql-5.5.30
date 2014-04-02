/**
 * Wait Event Monitor
 *
 * @author  聂明军(niemingjun@corp.netease.com, niemingjun@163.org)
 */

#ifndef _NTSE_EVENTMONITOR_H_
#define _NTSE_EVENTMONITOR_H_

#include "util/Thread.h"

namespace ntse {
/** 事件监控协助类. */
class EventMonitorHelper {
public:
	EventMonitorHelper();
	~EventMonitorHelper();
	static void wakeUpEvents();
	static void pauseMonitor();
	static void resumeMonitor();
	
	static Task* m_task; /* 扫描任务线程 */
};


/** 定期进行检查唤醒Event的后台线程 */
class EventMonitor: public Task {
public:
	EventMonitor(uint interval);
	virtual void run();

private:
	u32			m_interval;	/** 检查时间间隔，单位秒 */
	u32			m_lastChkTime;	/** 上一次完成检查时间, 单位秒 */
};

/** 等待事件结构 */
struct WaitEvent {
	Event *evt;		/** 等候事件 */
	u32 wStartTime;		/** 等候开始时间 */	
};

}


#endif
