/**
 * Wait Event Monitor
 *
 * @author  ������(niemingjun@corp.netease.com, niemingjun@163.org)
 */

#ifndef _NTSE_EVENTMONITOR_H_
#define _NTSE_EVENTMONITOR_H_

#include "util/Thread.h"

namespace ntse {
/** �¼����Э����. */
class EventMonitorHelper {
public:
	EventMonitorHelper();
	~EventMonitorHelper();
	static void wakeUpEvents();
	static void pauseMonitor();
	static void resumeMonitor();
	
	static Task* m_task; /* ɨ�������߳� */
};


/** ���ڽ��м�黽��Event�ĺ�̨�߳� */
class EventMonitor: public Task {
public:
	EventMonitor(uint interval);
	virtual void run();

private:
	u32			m_interval;	/** ���ʱ��������λ�� */
	u32			m_lastChkTime;	/** ��һ����ɼ��ʱ��, ��λ�� */
};

/** �ȴ��¼��ṹ */
struct WaitEvent {
	Event *evt;		/** �Ⱥ��¼� */
	u32 wStartTime;		/** �Ⱥ�ʼʱ�� */	
};

}


#endif
