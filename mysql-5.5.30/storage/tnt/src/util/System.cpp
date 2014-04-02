/**
 * 封装一些系统全局函数及与系统可移植性相关的函数等
 *
 * @author 汪源(wangyuan@corp.netease.com, wy@163.org)
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

/** 计时线程 */
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
 * 得到系统当前时间，单位毫秒
 *
 * @return 系统当前时间，单位毫秒
 */
u64 System::currentTimeMillis() {
	return microTime() / 1000;
}

/**
 * 得到系统当前时间，单位微秒
 *
 * @return 系统当前时间，单位微秒
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
 * 得到系统当前时间，单位为CPU时钟周期数
 * 
 * @return 系统当前时间，单位为CPU时钟周期数
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
 * 格式化时间字符串
 *
 * @param buf 存储格式化结果
 * @param size buf分配的内存
 * @param time 时间
 */
void System::formatTime(char *buf, size_t size, const time_t *time) {
#ifdef WIN32
	ctime_s(buf, size, time);
#else
	ctime_r(time, buf);
#endif
	buf[strlen(buf) - 1] = '\0'; // 去掉换行
}

/**
 * 从虚拟内存中分配空间
 *
 * @param size 要分配的空间大小
 * @return 空间地址
 */
void* System::virtualAlloc(size_t size) {
#ifdef WIN32
	return VirtualAlloc(NULL, size, MEM_COMMIT, PAGE_READWRITE);
#else
	return valloc(size);
#endif
}

/**
 * 释放虚拟内存空间
 *
 * @param p 要释放的空间
 */
void System::virtualFree(void *p) {
#ifdef WIN32
	VirtualFree(p, 0, MEM_RELEASE);
#else
	free(p);
#endif
}

/**
 * 可变参数格式化字串
 * 
 * @param buf 存储格式化结果的数组
 * @param size buf的大小
 * @param fmt 格式化字串
 * @param ... 可变参数
 * @return 成功返回写出的字节数，失败或被截断返回-1
 */
int System::snprintf_mine(char *buf, size_t size, const char *fmt, ...) {
	va_list va;
	va_start(va, fmt);
	int r = vsnprintf(buf, size, fmt, va);
	va_end(va);
	return r;
}

/**
 * 可变参数格式化字串
 * 
 * @param buf 存储格式化结果的数组
 * @param size buf的大小
 * @param fmt 格式化字串
 * @param args 可变参数
 * @return 成功返回写出的字节数，失败或被截断返回-1
 */
int System::vsnprintf(char *buf, size_t size, const char *fmt, va_list args) {
#ifdef WIN32
	return _vsnprintf_s(buf, size, _TRUNCATE, fmt, args);
#else
	return ::vsnprintf(buf, size, fmt, args);
#endif
}

/**
 * 初始化随机数序列
 *
 * @param seed 随机数种子
 */
void System::srandom(unsigned int seed) {
#ifdef WIN32
	::srand(seed);
#else
	::srandom(seed);
#endif
}

/**
 * 生成随机数
 *
 * @return 随机数
 */
int System::random() {
#ifdef WIN32
	return ::rand();
#else
	return ::random();
#endif
}

/**
 * 不区分大小写比较两个字符串
 *
 * @param s1 字符串1
 * @param s2 字符串2
 * @return s1 < s2时返回<0，s1 = s2时返回0，s1 > s2时返回1
 */
int System::stricmp(const char *s1, const char *s2) {
#ifdef WIN32
	return ::_stricmp(s1, s2);
#else
	return strcasecmp(s1, s2);
#endif
}

/**
 * 不区分大小写比较两个字符串
 *
 * @param s1 字符串1
 * @param s2 字符串2
 * @param n 最多比较这么多字节
 * @return s1 < s2时返回<0，s1 = s2时返回0，s1 > s2时返回1
 */
int System::strnicmp(const char *s1, const char *s2, size_t n) {
#ifdef WIN32
	return ::_strnicmp(s1, s2, n);
#else
	return strncasecmp(s1, s2, n);
#endif
}

/**
 * 复制一个字符串
 *
 * @param s 原字符串，允许为NULL
 * @return 原字符串的拷贝，使用new []分配内存
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
 * 切换工作目录
 * @param dir 工作目录
 * @return 0表示成功
 */
int System::chdir(const char *dir) {
#ifdef WIN32
	return _chdir(dir);
#else
	return -1;
#endif
}

/**
 * 获取当前目录
 * @param buf 目录字符串缓存
 * @param size 缓存大小
 * @return 返回当前工作目录，NULL表示失败
 */
char* System::getcwd(char *buf, size_t size) {
#ifdef WIN32
	return _getcwd(buf, (int)size);
#else
	return 0;
#endif
}

/** 得到当前线程在操作系统内的ID
 *
 * @return 当前线程在操作系统内的ID
 */
uint System::currentOSThreadID() {
	return Thread::currentOSThreadID();
}

}

