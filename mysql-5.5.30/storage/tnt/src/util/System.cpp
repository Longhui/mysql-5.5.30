/**
 * ��װһЩϵͳȫ�ֺ�������ϵͳ����ֲ����صĺ�����
 *
 * @author ��Դ(wangyuan@corp.netease.com, wy@163.org)
 */

#include "util/System.h"
#include "util/Thread.h"
#include "misc/EventMonitor.h"
#include <time.h>
#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include <time.h>
#ifdef WIN32
#include <Windows.h>
#include <sys/timeb.h>
#include <direct.h>
#else
#include <sys/time.h>
#endif

namespace ntse {

Task *System::m_timerTask = NULL;
u32 g_currentTimeSeconds = 0;

#ifdef WIN32
System::PerformanceCounter System::m_pc;
System::PerformanceCounter::PerformanceCounter() {

	LARGE_INTEGER t;
	QueryPerformanceFrequency(&t);
	m_performanceCounterFreq = t.QuadPart;
	QueryPerformanceCounter(&t);
	m_startPerformanceCounter = t.QuadPart;

	struct __timeb64 tb;
	_ftime64(&tb);
	m_startTimeMillis = (u64 )tb.time * 1000 + tb.millitm;
}
#endif // WIN32

/** ��ʱ�߳� */
class TimerTask: public Task {
public:
	TimerTask(): Task("Timer", 100) { }
	void run() {
		g_currentTimeSeconds = (u32)time(NULL);
	}
};



System::System() {
	g_currentTimeSeconds = (u32)time(NULL);
	m_timerTask = new TimerTask();
	m_timerTask->start();
}

System::~System() {
	m_timerTask->stop();
	m_timerTask->join(-1);
	delete m_timerTask;
}

/**
 * �õ�ϵͳ��ǰʱ�䣬��λ����
 *
 * @return ϵͳ��ǰʱ�䣬��λ����
 */
u64 System::currentTimeMillis() {
	return microTime() / 1000;
}

/**
 * �õ�ϵͳ��ǰʱ�䣬��λ΢��
 *
 * @return ϵͳ��ǰʱ�䣬��λ΢��
 */
u64 System::microTime() {
#ifdef WIN32
	LARGE_INTEGER t;
	QueryPerformanceCounter(&t);
	u64 diffMicroTime = (t.QuadPart - m_pc.m_startPerformanceCounter) * 1000000 / m_pc.m_performanceCounterFreq;
	return m_pc.m_startTimeMillis * 1000 + diffMicroTime;
#else
	struct timeval tv;
	gettimeofday(&tv, NULL);
	return tv.tv_sec * 1000000 + tv.tv_usec;
#endif
}

/**
 * �õ�ϵͳ��ǰʱ�䣬��λΪCPUʱ��������
 * 
 * @return ϵͳ��ǰʱ�䣬��λΪCPUʱ��������
 */
u64 System::clockCycles() {
#ifdef WIN32
	__asm _emit 0x0F
	__asm _emit 0x31
#else
	unsigned int low, high;
	__asm__ __volatile__("rdtsc": "=a"(low),"=d"(high));
	return (u64)low | (((u64)high) << 32);
#endif
}

/**
 * ��ʽ��ʱ���ַ���
 *
 * @param buf �洢��ʽ�����
 * @param size buf������ڴ�
 * @param time ʱ��
 */
void System::formatTime(char *buf, size_t size, const time_t *time) {
#ifdef WIN32
	ctime_s(buf, size, time);
#else
	ctime_r(time, buf);
#endif
	buf[strlen(buf) - 1] = '\0'; // ȥ������
}

/**
 * �������ڴ��з���ռ�
 *
 * @param size Ҫ����Ŀռ��С
 * @return �ռ��ַ
 */
void* System::virtualAlloc(size_t size) {
#ifdef WIN32
	return VirtualAlloc(NULL, size, MEM_COMMIT, PAGE_READWRITE);
#else
	return valloc(size);
#endif
}

/**
 * �ͷ������ڴ�ռ�
 *
 * @param p Ҫ�ͷŵĿռ�
 */
void System::virtualFree(void *p) {
#ifdef WIN32
	VirtualFree(p, 0, MEM_RELEASE);
#else
	free(p);
#endif
}

/**
 * �ɱ������ʽ���ִ�
 * 
 * @param buf �洢��ʽ�����������
 * @param size buf�Ĵ�С
 * @param fmt ��ʽ���ִ�
 * @param ... �ɱ����
 * @return �ɹ�����д�����ֽ�����ʧ�ܻ򱻽ضϷ���-1
 */
int System::snprintf_mine(char *buf, size_t size, const char *fmt, ...) {
	va_list va;
	va_start(va, fmt);
	int r = vsnprintf(buf, size, fmt, va);
	va_end(va);
	return r;
}

/**
 * �ɱ������ʽ���ִ�
 * 
 * @param buf �洢��ʽ�����������
 * @param size buf�Ĵ�С
 * @param fmt ��ʽ���ִ�
 * @param args �ɱ����
 * @return �ɹ�����д�����ֽ�����ʧ�ܻ򱻽ضϷ���-1
 */
int System::vsnprintf(char *buf, size_t size, const char *fmt, va_list args) {
#ifdef WIN32
	return _vsnprintf_s(buf, size, _TRUNCATE, fmt, args);
#else
	return ::vsnprintf(buf, size, fmt, args);
#endif
}

/**
 * ��ʼ�����������
 *
 * @param seed ���������
 */
void System::srandom(unsigned int seed) {
#ifdef WIN32
	::srand(seed);
#else
	::srandom(seed);
#endif
}

/**
 * ���������
 *
 * @return �����
 */
int System::random() {
#ifdef WIN32
	return ::rand();
#else
	return ::random();
#endif
}

/**
 * �����ִ�Сд�Ƚ������ַ���
 *
 * @param s1 �ַ���1
 * @param s2 �ַ���2
 * @return s1 < s2ʱ����<0��s1 = s2ʱ����0��s1 > s2ʱ����1
 */
int System::stricmp(const char *s1, const char *s2) {
#ifdef WIN32
	return ::_stricmp(s1, s2);
#else
	return strcasecmp(s1, s2);
#endif
}

/**
 * �����ִ�Сд�Ƚ������ַ���
 *
 * @param s1 �ַ���1
 * @param s2 �ַ���2
 * @param n ���Ƚ���ô���ֽ�
 * @return s1 < s2ʱ����<0��s1 = s2ʱ����0��s1 > s2ʱ����1
 */
int System::strnicmp(const char *s1, const char *s2, size_t n) {
#ifdef WIN32
	return ::_strnicmp(s1, s2, n);
#else
	return strncasecmp(s1, s2, n);
#endif
}

/**
 * ����һ���ַ���
 *
 * @param s ԭ�ַ���������ΪNULL
 * @return ԭ�ַ����Ŀ�����ʹ��new []�����ڴ�
 */
char* System::strdup(const char *s) {
	if (!s)
		return NULL;
	size_t size = strlen(s) + 1;
	char *ret = new char[size];
	memcpy(ret, s, size);
	return ret;
}

/**
 * �л�����Ŀ¼
 * @param dir ����Ŀ¼
 * @return 0��ʾ�ɹ�
 */
int System::chdir(const char *dir) {
#ifdef WIN32
	return _chdir(dir);
#else
	return -1;
#endif
}

/**
 * ��ȡ��ǰĿ¼
 * @param buf Ŀ¼�ַ�������
 * @param size �����С
 * @return ���ص�ǰ����Ŀ¼��NULL��ʾʧ��
 */
char* System::getcwd(char *buf, size_t size) {
#ifdef WIN32
	return _getcwd(buf, (int)size);
#else
	return 0;
#endif
}

/** �õ���ǰ�߳��ڲ���ϵͳ�ڵ�ID
 *
 * @return ��ǰ�߳��ڲ���ϵͳ�ڵ�ID
 */
uint System::currentOSThreadID() {
	return Thread::currentOSThreadID();
}

}

