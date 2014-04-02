/**
 * ��װһЩϵͳȫ�ֺ�������ϵͳ����ֲ����صĺ�����
 *
 * @author ��Դ(wangyuan@corp.netease.com, wy@163.org)
 */

#ifndef _NTSE_SYSTEM_H_
#define _NTSE_SYSTEM_H_

#include <stdarg.h>
#include <malloc.h>
#include <time.h>
#include "util/Portable.h"

namespace ntse {
extern u32 g_currentTimeSeconds;

class Task;
class System {
public:
	System();
	virtual ~System();

	static u64 currentTimeMillis();
	static u64 microTime();
	static u64 clockCycles();
	/**
	 * �õ�ϵͳ��ǰʱ�䣬��λ��������һ������time()������Ⱦ��и�������
	 * 
	 * @return ϵͳ��ǰʱ��
	 */
	static u32 fastTime() {
		return g_currentTimeSeconds;
	}
	static void formatTime(char *buf, size_t size, const time_t *time);
	static void* virtualAlloc(size_t size);
	static void virtualFree(void *p);
	static int snprintf_mine(char *buf, size_t size, const char *fmt, ...);
	static int vsnprintf(char *buf, size_t size, const char *fmt, va_list args);
	static void srandom(unsigned int seed);
	static int random();
	static int stricmp(const char *s1, const char *s2);
	static int strnicmp(const char *s1, const char *s2, size_t n);
	static char* strdup(const char *s);
	static int chdir(const char *dir);
	static char* getcwd(char *buf, size_t size);
	static uint currentOSThreadID();

private:
#ifdef WIN32
	struct PerformanceCounter {
		PerformanceCounter();
		u64		m_performanceCounterFreq;
		u64		m_startTimeMillis;
		u64		m_startPerformanceCounter;
	};
	static PerformanceCounter m_pc;
#endif

	static Task*	m_timerTask;
};

/** �ڶ�ջ�Ϸ���ռ� */
#ifdef WIN32
#define alloca	_alloca
#endif

}

#endif
