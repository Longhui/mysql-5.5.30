/**
 * 多线程同步的一些实现
 *
 * @author 汪源(wangyuan@corp.netease.com, wy@163.org)
 */
#include "util/Sync.h"
#include "util/System.h"
#include "util/Thread.h"
#include <iostream>
#include "util/Hash.h"
#include "misc/EventMonitor.h"
#include <signal.h>
#include <errno.h>

using namespace std;

namespace ntse {

Task* EventMonitorHelper::m_task = NULL;

const bool Lock::CONFLICT_MATRIX[IL_MAX][IL_MAX] = { {false, false, false, false, false, false, false},
													 {false, false, false, false, false, false, true},
													 {false, false, false, true, true, true, true},
													 {false, false, true, false, true, false, true},
													 {false, true, true, true, true, true, true},
													 {false, false, true, false, true, true, true},
													 {true, true, true, true, true, true, true} };

/** 
 * 构造函数
 */
EventMonitorHelper::EventMonitorHelper() {
	Event::m_waitEventList = new DList<WaitEvent *>();
#ifdef WIN32
	::InitializeCriticalSection(&Event::m_eventListLock);
#else
	pthread_mutex_init(&Event::m_eventListLock, NULL);
#endif // WIN32
	m_task = new EventMonitor( Event::WAIT_UP_PERIOD);
	m_task->start();	
}

/** 
 * 析构函数
 */
EventMonitorHelper::~EventMonitorHelper() {
	if (NULL != m_task) {
		m_task->stop();
		delete m_task;
		m_task = NULL;
	}

#ifdef WIN32
	::DeleteCriticalSection(&Event::m_eventListLock);
#else
	pthread_mutex_destroy(&Event::m_eventListLock);
#endif // WIN32

	if (NULL != Event::m_waitEventList) {
		delete Event::m_waitEventList;
		Event::m_waitEventList = NULL;
	}
}

/** 
 * 唤醒等待队列中的事件
 */
void EventMonitorHelper::wakeUpEvents() {
#ifdef WIN32
	EnterCriticalSection(&Event::m_eventListLock);
#else
	pthread_mutex_lock(&Event::m_eventListLock);
#endif

	DLink<WaitEvent *> *cur = Event::m_waitEventList->getHeader()->getNext();
	//对于队列中等候时间不到WAIT_TIME_THRESHOLD秒的事件，扫描线程不进行激活。
	while (cur != Event::m_waitEventList->getHeader()) {
		WaitEvent * pEvt = (*cur).get();
		assert(pEvt);
		assert(pEvt->evt);
		if (pEvt->wStartTime + Event::WAIT_TIME_THRESHOLD > System::fastTime()) {
			break;
		}
		pEvt->evt->signal(true);
		cur = cur->getNext();
	}	

#ifdef WIN32
	LeaveCriticalSection(&Event::m_eventListLock);
#else	
	pthread_mutex_unlock(&Event::m_eventListLock);	
#endif	
}

/**
 * 暂停后台唤醒扫描线程
 */
void EventMonitorHelper::pauseMonitor() {
	assert(NULL != m_task);
	m_task->pause();
}

/**
 * 继续后台唤醒扫描线程
 */
void EventMonitorHelper::resumeMonitor() {
	assert(NULL != m_task);
	m_task->resume();
}

/**
 * 构造函数
 *
 * @param interval 唤醒时间周期，单位秒
 */
EventMonitor::EventMonitor(uint interval)
: Task("EventMonitor", interval * 1000) {
	m_lastChkTime = System::fastTime();
	m_interval = interval;
}

/**
 * 运行
 * @see <code>Task::run()</code> 方法
 */
void EventMonitor::run() {
	EventMonitorHelper::wakeUpEvents();
	m_lastChkTime = System::fastTime();		
}


///////////////////////////////////////////////////////////////////////////////
// Event ///////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////////

#ifdef WIN32
CRITICAL_SECTION Event::m_eventListLock;	
#else
pthread_mutex_t Event::m_eventListLock;
#endif


/* 等待队列*/
DList<WaitEvent *> *Event::m_waitEventList = NULL;

/** 
 * 构造函数
 * 
 * @param autoReset 等待操作是否自动将事件重置为非触发状态
 */
Event::Event(bool autoReset) {
#ifdef WIN32
	m_osEvent = ::CreateEvent((LPSECURITY_ATTRIBUTES)0, !autoReset, FALSE, (LPCSTR)0);
	::InitializeCriticalSection(&m_lock);	
#else
	pthread_cond_init(&m_cond, NULL);
	pthread_mutex_init(&m_lock, NULL);	
	m_set = false;
#endif
	m_autoReset = autoReset;
	m_sigToken = 1;
	m_usage = new EventUsage();
}


/**
 * 析构函数
 */
Event::~Event() {
#ifdef WIN32
	::CloseHandle(m_osEvent);
	::DeleteCriticalSection(&m_lock);
#else
	pthread_cond_destroy(&m_cond);
	pthread_mutex_destroy(&m_lock);
#endif
	delete m_usage;
	m_usage = NULL;
}

/**
 * 获取事件统计信息
 * 
 * @return 返回<code>EventUsage</code> 实例表示事件统计信息
 */
const EventUsage& Event::getUsage() const
{
	assert (NULL != m_usage);
	return *m_usage;
}

/** 
 * 重置事件为非触发状态，返回当前事件触发令牌值，该值可用于后续等待操作，
 * 事件触发令牌值在每次调用signal时会加1
 *
 * @pre		事件创建时指定autoReset为false
 * @return	当前事件触发令牌值
 */
long Event::reset() {
	assert(!m_autoReset);

	long r;
	lock();

#ifndef WIN32
	m_set = false;
#endif
	r = m_sigToken;
	unlock();
#ifdef WIN32
	::ResetEvent(m_osEvent);
#endif
	return r;
}

/**
 * 检查事件等待动作
 *
 * @param resetToken	事件触发令牌值
 * @return 检查是否等待。true表示事件不需要wait； false表示事件需要执行操作系统的wait
 */
bool Event::checkNoWait(long resetToken /* = UNSPECIFIED_TOKEN */) {
#ifdef WIN32
	if ((!m_autoReset) && (resetToken != UNSPECIFIED_TOKEN && m_sigToken > resetToken)) {		
		return true;
	}
#else
	if (m_autoReset) {
		if (m_set) {
			m_set = false;
			return true;
		}
	} else if (m_set || (resetToken != UNSPECIFIED_TOKEN && m_sigToken > resetToken)) {		
		return true;
	}	
#endif
	return false;	
}

/**
 * 将等待事件加入等待事件队列
 *
 * @param plnk 等待事件
 * @param noAction 是否不加入等待队列
 */
void Event::addWaitEvent(DLink<WaitEvent *> *plnk, bool noAction) {	
	assert(plnk);
	if (noAction) {
		return;
	}

#ifdef WIN32
	EnterCriticalSection(&Event::m_eventListLock);
#else
	pthread_mutex_lock(&Event::m_eventListLock);
#endif	

	m_waitEventList->addLast(plnk);

#ifdef WIN32
	LeaveCriticalSection(&Event::m_eventListLock);
#else	
	pthread_mutex_unlock(&Event::m_eventListLock);
#endif	
}

/**
 * 将等待事件从等待事件队列中移除
 *
 * @param plnk 等待事件
 * @param noAction 是否不执行移除等待队列
 */
void Event::removeWaitEvent(DLink<WaitEvent *> *plnk, bool noAction) {
	assert(plnk);
	if (noAction) {
		return;
	}

#ifdef WIN32
	EnterCriticalSection(&Event::m_eventListLock);
#else
	pthread_mutex_lock(&Event::m_eventListLock);
#endif	
	assert(Event::m_waitEventList);	
	plnk->unLink();

#ifdef WIN32
	LeaveCriticalSection(&Event::m_eventListLock);
#else	
	pthread_mutex_unlock(&Event::m_eventListLock);
#endif	
}
/**
 * 等待事件
 *
 * @param timeoutMs		超时时间，单位为毫秒，若<=0则不超时；若>0则限时等待
 * @param resetToken	事件触发令牌值，通常为先前调用reset的返回值，
 *   若为UNSPECIFIED_TOKEN则表示未指定，此时wait在以下情况返回（除了超时）
 *   1. 调用了signal后还没有人wait过，第一个wait的人马上返回
 *   2. wait后，有人调用了signal
 *   若token不是UNSPECIFIED_TOKEN，则如果事件当前触发令牌值大于token，wait
 *   就会马上返回
 * @pre 若事件创建时指定autoReset为true，则resetToken必须为UNSPECIFIED_TOKEN
 *
 * @return true表示是被激活返回，false表示是超时返回，注意即使在没有应用代码
 *   激活此事件时，系统有时也会激活事件
 */
bool Event::wait(int timeoutMs, long resetToken) {
	assert(resetToken == UNSPECIFIED_TOKEN || !m_autoReset);

	m_usage->m_waitCnt++;
	
	//检查是否需要真正wait
	lock();
	bool noWait = checkNoWait(resetToken);
	if (noWait) {
		unlock();
		return true;
	}
	unlock(); //Note:由于这里进行了解锁，对于Linux的实现，在执行真正等待前还必须在check下是否需要等待。

	//初始化WaitEvent实例。
	DLink<WaitEvent *> lnk;	
	WaitEvent we;
	we.evt = this;
	we.wStartTime = System::fastTime();
	lnk.set(&we);

	bool noWaitListAction = (timeoutMs > 0 && timeoutMs < 5000);
	//加入事件到等待队列		
	addWaitEvent(&lnk, noWaitListAction);
	

#ifdef WIN32
	if (timeoutMs <= 0)
		timeoutMs = INFINITE;	
	//执行等待
	DWORD code = ::WaitForSingleObject(m_osEvent, timeoutMs);
	//从等待队列中移出
	removeWaitEvent(&lnk, noWaitListAction);
	//返回是否等待成功
	return code != WAIT_TIMEOUT;
#else
	int r;
	struct timeval now;
	struct timespec timeout;
	
	if (timeoutMs <= 0) {	//无限等待	
		//检查是否需要真正执行wait
		lock();
		bool noWait = checkNoWait(resetToken);
		if (noWait) {
			unlock();
			//从等待队列中移出
			removeWaitEvent(&lnk, noWaitListAction);
			return true;
		}
		//执行等待
		r = pthread_cond_wait(&m_cond, &m_lock);
		
	} else {	//限时等待
		//计算超时时间
		gettimeofday(&now, NULL);
		now.tv_sec += timeoutMs / 1000;
		now.tv_usec += (timeoutMs % 1000) * 1000;
		if (now.tv_usec >= 1000000) {
			now.tv_sec++;
			now.tv_usec -= 1000000;
		}
		timeout.tv_sec = now.tv_sec;
		timeout.tv_nsec = now.tv_usec * 1000;

		//检查是否需要真正执行wait
		lock();
		bool noWait = checkNoWait(resetToken);
		if (noWait) {
			unlock();
			removeWaitEvent(&lnk, noWaitListAction);
			return true;
		}
		
		//执行限时等待
		r = pthread_cond_timedwait(&m_cond, &m_lock, &timeout);	
	}
	if (r == 0 && m_autoReset) {
		m_set = false;
	}
	unlock();
	//从等待队列中移出
	removeWaitEvent(&lnk, noWaitListAction);
	
	//返回是否等待成功
	return r == 0;
#endif
}

/** 
 * 触发事件，唤醒那些等待这一事件的线程
 * 
 * @param 唤醒等待事件的所有线程或只唤醒一个（一般情况下只唤醒一个但不绝对保证）
 *   此参数在Windows平台中无效
 */
void Event::signal(bool broadcast) {
	m_usage->m_sigCnt++;

#ifdef WIN32
	UNREFERENCED_PARAMETER(broadcast);
#endif

	lock();
#ifdef WIN32
	m_sigToken++;
	::SetEvent(m_osEvent);
#else
	if (!m_set) {
		m_set = true;
		m_sigToken++;
		if (broadcast)
			pthread_cond_broadcast(&m_cond);
		else
			pthread_cond_signal(&m_cond);
	}
#endif
	unlock();
}

/**
 * 加锁，保护内部成员多线程访问
 */
void Event::lock() {
#ifdef WIN32
	EnterCriticalSection(&m_lock);
#else
	pthread_mutex_lock(&m_lock);
#endif
}

/**
 * 解锁，解禁内部成员锁保护
 */
void Event::unlock() {
#ifdef WIN32
	LeaveCriticalSection(&m_lock);
#else
	pthread_mutex_unlock(&m_lock);
#endif
}

/** 构造一个锁对象
 * @param name 锁对象名称，拷贝使用
 * @param file 创建锁对象时的代码文件
 * @param line 创建锁对象时的行号
 */
Lock::Lock(const char *name, const char *file, uint line) {
	m_name = System::strdup(name);
	m_file = file;
	m_line = line;
}

/** 销毁锁对象 */
Lock::~Lock() {
	delete []m_name;	
}


#ifdef NTSE_UNIT_TEST
#define SET_WAITING_LOCK								\
	Thread *currentThread = Thread::currentThread();	\
	if (currentThread)									\
		currentThread->setWaitingLock(this);

#define CLEAR_WAITING_LOCK								\
	if (currentThread)									\
		currentThread->setWaitingLock(NULL);
#else
#define SET_WAITING_LOCK
#define CLEAR_WAITING_LOCK
#endif

/**
 * 构造函数
 *
 * @param name 锁对象名称
 * @param allocFile 调用构造函数的代码文件
 * @param allocLine 调用构造函数的代码行号
 */
LockUsage::LockUsage(const char *name, const char *allocFile, uint allocLine) {
	m_name = System::strdup(name);
	m_allocFile = allocFile;
	m_allocLine = allocLine;
	m_spinCnt = m_waitCnt = 0;
	m_waitTime = 0;
	m_instanceCnt = 1;
	m_shared = true;
}

/** 销毁对象 */
LockUsage::~LockUsage() {
	delete []m_name;
}

/** 打印锁对象使用统计信息
 * @param out 输出流
 */
void LockUsage::print(std::ostream &out) const {
	out << "Lock: " << m_name << "[" << m_allocFile << ":" << m_allocLine << "]" << endl;
	out << "  Instances: " << m_instanceCnt << endl;
	out << "  Spin count: " << m_spinCnt << endl;
	out << "  Wait count: " << m_waitCnt << endl;
	out << "  Wait time: " << m_waitTime << endl;
}

/** 锁使用情况统计对象的标志 */
struct LockUsageKey {
	const char	*m_allocFile;	/** 锁对象分配时的代码文件名 */
	uint		m_allocLine;	/** 锁对象分配时的代码行号 */
	const char	*m_name;		/** 锁对象名称 */

	LockUsageKey(const char *name, const char *allocFile, uint allocLine) {
		m_name = name;
		m_allocFile = allocFile;
		m_allocLine = allocLine;
	}
};

/** 计算LockUsageKey类型对象哈希值的函数对象 */
class LukHasher {
public:
	inline unsigned int operator ()(const LockUsageKey &usageKey) const {
		unsigned int h = m_strHasher.operator ()(usageKey.m_allocFile);
		h += m_strHasher.operator ()(usageKey.m_name);
		return h + usageKey.m_allocLine;
	}

private:
	Hasher<const char *>	m_strHasher;
};

/** 计算LockUsage类型对象哈希值的函数对象 */
class LuHasher {
public:
	inline unsigned int operator ()(const LockUsage *usage) const {
		return m_keyHasher.operator()(LockUsageKey(usage->m_name, usage->m_allocFile, usage->m_allocLine));
	}

private:
	LukHasher	m_keyHasher;
};

/** 判断LockUsageKey与LockUsage对象相等性的函数对象 */
class LukLuEqualer {
public:
	inline bool operator()(const LockUsageKey &usageKey, const LockUsage *usage) const {
		return !strcmp(usageKey.m_allocFile, usage->m_allocFile) && usageKey.m_allocLine == usage->m_allocLine
			&& !strcmp(usageKey.m_name, usage->m_name);
	}
};

static bool	g_mutexUsagesReady = false;	/** Mutex使用情况统计对象哈希表是否已经初始化 */
class MuHash: public DynHash<LockUsageKey, LockUsage *, LuHasher, LukHasher, LukLuEqualer> {
public:
	MuHash() {
		g_mutexUsagesReady = true;
	}
};

static MuHash		g_mutexUsages;	/** Mutex使用情况统计对象哈希表 */
static Atomic<int>	g_muLock;		/** 保护上述哈希表并发的锁 */

/**
 * 返回简洁的路径，即相对于ntse代码根目录的路径
 *
 * @param file 代码文件路径
 * @return 简洁的路径
 */
static const char* concisedFile(const char *file) {
	const char *p = strstr(file, "/ntse/");
	if (!p)
		p = strstr(file, "\\ntse\\");
	if (p)
		return p + 6;
	p = strstr(file, "./");
	if (!p)
		p = strstr(file, ".\\");
	if (p)
		return p + 2;
	return file;
}

/**
 * 分配一个MutexUsage对象，若有相同的代码行号的对象存在则增加引用计数
 *
 * @param name 锁对象名称
 * @param file 调用构造函数的代码文件
 * @param line 调用构造函数的代码行号
 */
static MutexUsage* allocMutexUsage(const char *name, const char *file, uint line) {
	if (!g_mutexUsagesReady) {
		MutexUsage *usage = new MutexUsage(name, file, line);
		usage->m_shared = false;
		return usage;
	}
	while (!g_muLock.compareAndSwap(0, 1)) {}
	MutexUsage *usage = (MutexUsage *)g_mutexUsages.get(LockUsageKey(name, file, line));
	if (!usage) {
		usage = new MutexUsage(name, file, line);
		g_mutexUsages.put((LockUsage *)usage);
	} else
		usage->m_instanceCnt++;
	g_muLock.set(0);
	return usage;
}

/**
 * 释放一个MutexUsage对象，减少引用计数，若计数减为0则释放之
 *
 * @param usage MutexUsage对象
 */
static void freeMutexUsage(MutexUsage *usage) {
	if (!usage->m_shared) {
		delete usage;
		return;
	}
	while (!g_muLock.compareAndSwap(0, 1)) {}
	if (--usage->m_instanceCnt == 0) {
		MutexUsage *usage2 = (MutexUsage *)g_mutexUsages.remove(LockUsageKey(usage->m_name, usage->m_allocFile, usage->m_allocLine));
		assert(usage == usage2);
		delete usage2;
	}
	g_muLock.set(0);
}

/**
 * 构造函数
 *
 * @param name 锁对象名称
 * @param allocFile 调用构造函数的代码文件
 * @param allocLine 调用构造函数的代码行号
 */
MutexUsage::MutexUsage(const char *name, const char *allocFile, uint allocLine):
	LockUsage(name, allocFile, allocLine) {
	m_lockCnt = 0;
}

/** 打印互斥锁使用统计信息
 * @param out 输出流
 */
void MutexUsage::print(std::ostream &out) const {
	LockUsage::print(out);
	out << "  Lock count: " << m_lockCnt << endl;
}

/** 打印所有互斥锁使用统计信息
 * @param out 输出流
 */
void MutexUsage::printAll(std::ostream &out) {
	MuScanHandle h;
	const MutexUsage *usage;

	out << "== mutex usage ================================================================" << endl;
	
	beginScan(&h);
	while ((usage = getNext(&h)) != NULL) {
		usage->print(out);
	}
	endScan(&h);
}

/**
 * 开始扫描Mutex统计信息，扫描过程中将不允许Mutex对象创建与析构
 *
 * @param h IN/OUT 扫描句柄
 */
void MutexUsage::beginScan(MuScanHandle *h) {
	while (!g_muLock.compareAndSwap(0, 1)) {}
	h->m_curPos = 0;
}

/**
 * 获取下一个Mutex统计对象
 *
 * @param h IN/OUT 扫描句柄
 * @return 下一个Mutex统计对象，扫描完时返回NULL
 */
const MutexUsage* MutexUsage::getNext(MuScanHandle *h) {
	if (h->m_curPos < g_mutexUsages.getSize())
		return (MutexUsage *)g_mutexUsages.getAt(h->m_curPos++);
	else
		return NULL;
}

/**
 * 结束扫描Mutex统计信息，重新允许Mutex对象创建与析构
 *
 * @param h IN/OUT 扫描句柄
 */
void MutexUsage::endScan(MuScanHandle *h) {
	UNREFERENCED_PARAMETER(h);
	g_muLock.set(0);
}

#pragma region NEW_MUTEX

/**
 * 构造函数.
 *
 * @param name 锁对象名称，拷贝使用
 * @param file 调用构造函数的代码文件
 * @param line 调用构造函数的代码行号
 */
Mutex::Mutex(const char *name, const char *file, uint line): Lock(name, file, line), m_event(false) {
	m_usage = allocMutexUsage(m_name, concisedFile(file), line);
	m_file = NULL;
	m_line = 0;
}

/** 
 * 析构函数
 */
Mutex::~Mutex() {
	freeMutexUsage(m_usage);
	m_usage = NULL;
}

/**
 * 获得使用情况统计信息。
 *
 * @return 使用情况统计信息
 */
const MutexUsage* Mutex::getUsage() const {
	return m_usage;
}

/**
 * 处理锁冲突
 *
 * @param timeoutMs 超时时间，单位毫秒。若>0为超时时间，若<0表示不超时，若=0立即返回结果。
 * @return 是否加锁成功。 false, 加锁超时；true, 加锁成功。
 */
bool Mutex::lockConflict(int timeoutMs) {
	uint i;	  /* spin round count */
	// 自旋
	m_usage->m_spinCnt++;

	// 是否Event wait超时
	bool timeoutFlag = false;
	// 是否执行过Event wait
	bool hasWaited = false;

mutex_loop:
	i = 0;
spin_loop:		
	while ((m_lockWord.get() != 0) && (i < Mutex::SPIN_COUNT)) {
		if (Mutex::SPIN_DELAY) {
			Thread::delay(System::random() % Mutex::SPIN_DELAY);
		}
		i++;
	}
	if (i == Mutex::SPIN_COUNT) {
		Thread::yield();
	} 

	if (m_lockWord.compareAndSwap(0, 1)) {
		return true;
	}

	i++;
	if (i < Mutex::SPIN_COUNT) {
		goto spin_loop;
	}

	long sigToken = m_event.reset();

	m_waiting.set(1);

	/*Try to  reserve still a few times*/
	for (int j = 0; j < 4; j++) {
		if (m_lockWord.compareAndSwap(0, 1)) {
			return true;
		}
	}

	//每Thread执行conflictwait,m_waitCnt只加1.
	if (!hasWaited) {
		m_usage->m_waitCnt++;
	}

	u64 before = System::currentTimeMillis();	
	timeoutFlag = m_event.wait(timeoutMs, sigToken);
	if (!hasWaited) {
		hasWaited = true;
	}
	u64 now = System::currentTimeMillis();

	//更新LockUage的等待时间统计
	m_usage->m_waitTime += (now - before);

	//等待超时的，直接返回
	if (!timeoutFlag) {
		return false;
	}

	if (timeoutMs > 0) {
		//超时锁，更新超时时间
		timeoutMs -= (int)(now - before);
		if (timeoutMs <= 0) {
			//超时退出
			return false;
		}
	}
	goto mutex_loop;
}

#pragma endregion //Mutex的实现


///////////////////////////////////////////////////////////////////////////////
// RWLock /////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////////
static bool g_rwlUsagesReady = false;	/** RWLock使用情况统计对象哈希表是否已经初始化 */
class RwluHash: public DynHash<LockUsageKey, LockUsage *, LuHasher, LukHasher, LukLuEqualer> {
public:
	RwluHash() {
		g_rwlUsagesReady = true;
	}
};

static RwluHash		g_rwlUsages;	/** RWLock使用情况统计对象哈希表(包括BiasedRWLock) */
static Atomic<int>	g_rwlLock;		/** 保护上述哈希表并发的锁 */

/**
 * 分配一个RWLockUsage对象，若有相同的代码行号的对象存在则增加引用计数
 *
 * @param name 锁对象名称
 * @param file 调用构造函数的代码文件
 * @param line 调用构造函数的代码行号
 */
static RWLockUsage* allocRWLockUsage(const char *name, const char *file, uint line) {
	if (!g_rwlUsagesReady) {
		RWLockUsage *usage = new RWLockUsage(name, file, line);
		usage->m_shared = false;
		return usage;
	}
	while (!g_muLock.compareAndSwap(0, 1)) {}
	RWLockUsage *usage = (RWLockUsage *)g_rwlUsages.get(LockUsageKey(name, file, line));
	if (!usage) {
		usage = new RWLockUsage(name, file, line);
		g_rwlUsages.put((LockUsage *)usage);
	} else
		usage->m_instanceCnt++;
	g_muLock.set(0);
	return usage;
}

/**
 * 释放一个RWLockUsage对象，减少引用计数，若计数减为0则释放之
 *
 * @param usage RWLockUsage对象
 */
static void freeRWLockUsage(RWLockUsage *usage) {
	if (!usage->m_shared) {
		delete usage;
		return;
	}
	while (!g_muLock.compareAndSwap(0, 1)) {}
	if (--usage->m_instanceCnt == 0) {
		RWLockUsage *usage2 = (RWLockUsage *)g_rwlUsages.remove(LockUsageKey(usage->m_name, usage->m_allocFile, usage->m_allocLine));
		assert(usage == usage2);
		delete usage2;
	}
	g_muLock.set(0);
}

/**
 * 构造函数
 *
 * @param name 锁对象名称
 * @param allocFile 调用构造函数的代码文件
 * @param allocLine 调用构造函数的代码行号
 */
RWLockUsage::RWLockUsage(const char *name, const char *allocFile, uint allocLine):
	LockUsage(name, allocFile, allocLine) {
	m_rlockCnt = m_wlockCnt = 0;
}

/** 打印读写锁使用统计信息 */
void RWLockUsage::print(std::ostream &out) const {
	LockUsage::print(out);
	out << "  Read locks: " << m_rlockCnt << endl;
	out << "  Write locks: " << m_wlockCnt << endl;
}

/**
 * 开始扫描读写锁统计信息，扫描过程中将不允许读写锁对象创建与析构
 *
 * @param h IN/OUT 扫描句柄
 */
void RWLockUsage::beginScan(RwluScanHandle *h) {
	while (!g_rwlLock.compareAndSwap(0, 1)) {}
	h->m_curPos = 0;
}

/** 打印所有互斥锁使用统计信息
 * @param out 输出流
 */
void RWLockUsage::printAll(std::ostream &out) {
	RwluScanHandle h;
	const RWLockUsage *usage;

	out << "== rwlock usage ===============================================================" << endl;
	
	beginScan(&h);
	while ((usage = getNext(&h)) != NULL) {
		usage->print(out);
	}
	endScan(&h);
}

/**
 * 获取下一个读写锁统计对象
 *
 * @param h IN/OUT 扫描句柄
 * @return 下一个读写锁统计对象，扫描完时返回NULL
 */
const RWLockUsage* RWLockUsage::getNext(RwluScanHandle *h) {
	if (h->m_curPos < g_rwlUsages.getSize())
		return (RWLockUsage *)g_rwlUsages.getAt(h->m_curPos++);
	else
		return NULL;
}

/**
 * 结束扫描读写锁统计信息，重新允许读写锁对象创建与析构
 *
 * @param h IN/OUT 扫描句柄
 */
void RWLockUsage::endScan(RwluScanHandle *h) {
	UNREFERENCED_PARAMETER(h);
	g_rwlLock.set(0);
}

///////////////////////////////////////////////////////////////////////////////
// RWLock /////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////////
/**
 * 构造函数
 *
 * @param name 锁对象名称，拷贝使用
 * @param file 调用构造函数的代码文件
 * @param line 调用构造函数的代码行号
 * @param perferWriter 是否给予写锁以更高优先级，给予写锁以更高优先级
 *   在读多写少的情况下可以有效防止加写锁纯种饿死现象，但可能导致加
 *   读锁的平均时间增加
 */
RWLock::RWLock(const char *name, const char *file, uint line, bool preferWriter): Lock(name, file, line), 
	m_lockWord(WLOCK_DEC), m_forNextWriter(false), m_forOthers(false) {
	m_rlockFile = m_wlockFile = NULL;
	m_rlockLine = m_wlockLine = 0;
	m_usage = allocRWLockUsage(m_name, concisedFile(file), line);
	m_preferWriter = preferWriter;
}

RWLock::~RWLock() {
	freeRWLockUsage(m_usage);
	m_usage = NULL;
}

/**
 * 尝试进行锁升级
 *
 * @param file 文件名
 * @param line 行号
 * @return 是否升级成功
 */
bool RWLock::tryUpgrade(const char *file, uint line) {
	assert(isLocked(Shared));
	m_usage->m_wlockCnt++;
	if (!m_lockWord.compareAndSwap(WLOCK_DEC - RLOCK_DEC, 0))
		return false;
	m_wlockFile = file;
	m_wlockLine = line;
	return true;
}

/**
 * 判断读写锁是否已经被用指定的模式锁定
 *
 * @param lockMode 锁模式
 * @return 读写锁是否已经被用指定的模式锁定
 */
bool RWLock::isLocked(LockMode lockMode) const {
	assert(lockMode == Shared || lockMode == Exclusived);
	int lockWord = m_lockWord.get();
	if (lockMode == Shared)
		return (lockWord < WLOCK_DEC && lockWord > 0)	// 加了读锁，没有人想加写锁
			|| (lockWord < 0 && lockWord > -WLOCK_DEC);	// 加了读锁，有人在等待加写锁
	else
		return lockWord == 0;
}

/**
 * 获得当前加的读写锁的模式
 * @return 
 */
LockMode RWLock::getLockMode() const {
	int lockWord = m_lockWord.get();
	return WLOCK_DEC == lockWord ? None : (0 == lockWord ? Exclusived : Shared);
}

/**
 * 获得使用情况统计
 *
 * @return 使用情况统计
 */
const RWLockUsage* RWLock::getUsage() const {
	return m_usage;
}

/**
 * 加读锁时处理冲突
 *
 * @param file 文件名
 * @param line 行号
 * @param absTimeout 为NEVER_TIMEOUT表示永不超时，否则为毫秒表示的绝对的超时时间
 * @return 是否加锁成功
 */
bool RWLock::rlockConflict(const char *file, uint line, u64 absTimeout) {
	m_usage->m_spinCnt++;
	uint i = 0;
	bool first = true;

	m_usage->m_spinCnt++;
_restart:
	while (i < Mutex::SPIN_COUNT && m_lockWord.get() <= 0) {
		if (Mutex::SPIN_DELAY) {
			Thread::delay(System::random() % Mutex::SPIN_DELAY);
		}
		i++;
	}

	if (i == Mutex::SPIN_COUNT) {
		Thread::yield();
	}

	if (tryLockR(file, line)) {
		return true;
	} else {
		if (i < Mutex::SPIN_COUNT)
			goto _restart;

		if (first) {
			m_usage->m_waitCnt++;
			first = false;
		}

		long sigToken = m_forOthers.reset();
		m_waiters.compareAndSwap(0, 1);
		if (tryLockR(file, line))
			return true;
		if (!doWait(&m_forOthers, absTimeout, sigToken))
			return false;

		i = 0;
		goto _restart;
	}
}

/** 进行等待
 * @param evt 等待这个事件
 * @param absTimeout 为NEVER_TIMEOUT表示永不超时，否则为毫秒表示的绝对的超时时间
 * @param token 事件触发令牌
 * @return true表示没有超时，false表示超时了
 */
bool RWLock::doWait(Event *evt, u64 absTimeout, long token) {
	u64 before = System::currentTimeMillis();
	if (absTimeout != NEVER_TIMEOUT && before >= absTimeout)
		return false;

	SET_WAITING_LOCK;
	evt->wait(absTimeout == NEVER_TIMEOUT? -1: (int)(absTimeout - before), token);
	u64 after = System::currentTimeMillis();
	CLEAR_WAITING_LOCK;

	m_usage->m_waitTime += after - before;
	bool isTimeout = absTimeout != NEVER_TIMEOUT && after >= absTimeout;
	return !isTimeout;
}

/** 加写锁时处理冲突
 * @param file 文件名
 * @param line 行号
 * @param absTimeout 为NEVER_TIMEOUT表示永不超时，否则为毫秒表示的绝对的超时时间
 * @return 是否成功
 */
bool RWLock::wlockConflict(const char *file, uint line, u64 absTimeout) {
	uint i = 0;
	bool hasWaitA = false, hasWait = false;

	m_usage->m_spinCnt++;
__restart:
	hasWaitA = false;
	if (acquireLockWordW(absTimeout, &hasWaitA)) {
		if (hasWait || hasWaitA)
			m_usage->m_waitCnt++;
		m_wlockFile = file;
		m_wlockLine = line;
		return true;
	} 

	if (hasWaitA) {
		m_usage->m_waitCnt++;
		return false;
	}

	while (i < Mutex::SPIN_COUNT && m_lockWord.get() <= 0) {
		if (Mutex::SPIN_DELAY) {
			Thread::delay(System::random() % Mutex::SPIN_DELAY);
		}
		i++;
	}
	if (i == Mutex::SPIN_COUNT) {
		Thread::yield();
	} else {
		goto __restart;
	}

	long sigToken = m_forOthers.reset();
	m_waiters.compareAndSwap(0, 1);
	if (acquireLockWordW(absTimeout, &hasWaitA)) {
		if (hasWait || hasWaitA)
			m_usage->m_waitCnt++;
		m_wlockFile = file;
		m_wlockLine = line;
		return true;
	}

	hasWait = true;
	if (!doWait(&m_forOthers, absTimeout, sigToken)) {
		m_usage->m_waitCnt++;
		return false;
	}

	i = 0;
	goto __restart;
}

/** 尝试获取m_lockWord，加了锁或成为加写锁的候选人
 * @param absTimeout 为NEVER_TIMEOUT表示永不超时，否则为毫秒表示的绝对的超时时间
 * @param hasWait OUT，是否进行了等待
 * @return 是否成功
 */
bool RWLock::acquireLockWordW(u64 absTimeout, bool *hasWait) {
	if (decrLockWord(WLOCK_DEC)) {
		if (m_lockWord.get() == 0 || waitForReaders(absTimeout, hasWait))
			return true;
		incrLockWord(WLOCK_DEC);
		// 这里应该唤醒，因为我可能阻塞了其它人
		if (m_waiters.get()) {
			m_waiters.compareAndSwap(1, 0);
			m_forOthers.signal(true);
		}
		return false;
	}
	return false;
}

/** 成为写锁唯一候选人之后，等待已经加的读锁释放
 * @param absTimeout 为NEVER_TIMEOUT表示永不超时，否则为毫秒表示的绝对的超时时间
 * @param hasWait OUT，是否进行了等待
 * @return 是否成功
 */
bool RWLock::waitForReaders(u64 absTimeout, bool *hasWait) {
	assert(m_lockWord.get() <= 0);
	uint i = 0;
	*hasWait = false;

	while (m_lockWord.get() < 0) {
		if (Mutex::SPIN_DELAY) {
			Thread::delay(System::random() % Mutex::SPIN_DELAY);
		}
		if (i < Mutex::SPIN_COUNT) {
			i++;
			continue;
		}

		i = 0;
		*hasWait = true;
		long sigToken = m_forNextWriter.reset();
		if (m_lockWord.get() < 0) {
			if (!doWait(&m_forNextWriter, absTimeout, sigToken))
				return false;
		}
	}
	return true;
}

///////////////////////////////////////////////////////////////////////////////
// LockerInfo /////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////////
/** 构造函数 */
LockerInfo::LockerInfo() {
	reset();
}

/** 设置锁持有者信息
 * @param mode 锁模式
 * @param file 代码文件
 * @param line 代码行号
 * @param threadId 线程ID
 * @param waitFor 等待的用户ID
 */
void LockerInfo::set(u16 mode, const char *file, uint line, uint threadId, u16 waitFor) {
	m_mode = mode;
	m_file = file;
	m_line = line;
	m_threadId = threadId;
	m_waitFor = waitFor;
}

/** 重置锁持有者信息 */
void LockerInfo::reset() {
	set(0, NULL, 0, 0, WAIT_FOR_NOTHING);
}

///////////////////////////////////////////////////////////////////////////////
// LURWLock ///////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////////
/** 构造函数
 * @param maxUserId 锁用户ID的最大值
 * @param name 锁对象名称
 * @param file 创建锁对象所在代码文件名
 * @param line 创建锁对象所在代码行号
 * @param preferWriter 是否给予写锁以更高优先级
 */
LURWLock::LURWLock(u16 maxUserId, const char *name, const char *file, uint line, bool preferWriter): Lock(name, file, line),
	m_rwLock(name, file, line, preferWriter) {
#ifdef NTSE_SYNC_DEBUG
	m_maxUserId = maxUserId;
	m_userInfos = new LockerInfo[maxUserId + 1];
#else
	UNREFERENCED_PARAMETER(maxUserId);
#endif
}

/** 析构函数 */
LURWLock::~LURWLock() {
#ifdef NTSE_SYNC_DEBUG
	delete m_userInfos;
#endif
}

#ifdef NTSE_SYNC_DEBUG
/** 计算与指定用户相冲突的用户ID
 * @param userId 用户ID
 * @param mode 想加的锁
 * @return 与指定用户相冲突的用户ID，若找不到返回WAIT_FOR_NOTHING
 */
u16 LURWLock::calcWaitFor(u16 userId, LockMode mode) {
	for (u16 i = 0; i <= m_maxUserId; i++) {
		if (i == userId)
			continue;
		if (m_userInfos[i].m_mode == None)
			continue;
		if (mode == Exclusived || m_userInfos[i].m_mode == Exclusived)
			return i;
	}
	return LockerInfo::WAIT_FOR_NOTHING;
}
#endif

///////////////////////////////////////////////////////////////////////////////
//// IntentionLockUsage ///////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////////
static bool	g_intentionLockUsagesReady = false;	/** IntentionLock使用情况统计对象哈希表是否已经初始化 */
class IlHash: public DynHash<LockUsageKey, LockUsage *, LuHasher, LukHasher, LukLuEqualer> {
public:
	IlHash() {
		g_intentionLockUsagesReady = true;
	}
};

static IlHash		g_intentionLockUsages;	/** IntentionLock使用情况统计对象哈希表 */
static Atomic<int>	g_ilLock;		/** 保护上述哈希表并发的锁 */

/**
 * 分配一个IntentionLockUsage对象，若有相同的代码行号的对象存在则增加引用计数
 *
 * @param name 锁对象名称
 * @param file 调用构造函数的代码文件
 * @param line 调用构造函数的代码行号
 */
static IntentionLockUsage* allocIntentionLockUsage(const char *name, const char *file, uint line) {
	if (!g_intentionLockUsagesReady) {
		IntentionLockUsage *usage = new IntentionLockUsage(name, file, line);
		usage->m_shared = false;
		return usage;
	}
	while (!g_ilLock.compareAndSwap(0, 1)) {}
	IntentionLockUsage *usage = (IntentionLockUsage *)g_intentionLockUsages.get(LockUsageKey(name, file, line));
	if (!usage) {
		usage = new IntentionLockUsage(name, file, line);
		g_intentionLockUsages.put((LockUsage *)usage);
	} else
		usage->m_instanceCnt++;
	g_ilLock.set(0);
	return usage;
}

/**
 * 释放一个IntentionLockUsage对象，减少引用计数，若计数减为0则释放之
 *
 * @param usage IntentionLockUsage对象
 */
static void freeIntentionLockUsage(IntentionLockUsage *usage) {
	if (!usage->m_shared) {
		delete usage;
		return;
	}
	while (!g_ilLock.compareAndSwap(0, 1)) {}
	if (--usage->m_instanceCnt == 0) {
		IntentionLockUsage *usage2 = (IntentionLockUsage *)g_intentionLockUsages.remove(LockUsageKey(usage->m_name, usage->m_allocFile, usage->m_allocLine));
		assert(usage == usage2);
		delete usage2;
	}
	g_ilLock.set(0);
}

/**
 * 构造函数
 *
 * @param name 锁对象名称
 * @param allocFile 调用构造函数的代码文件
 * @param allocLine 调用构造函数的代码行号
 */
IntentionLockUsage::IntentionLockUsage(const char *name, const char *allocFile, uint allocLine):
	LockUsage(name, allocFile, allocLine) {
	memset(&m_lockCnt, 0 , IL_MAX * sizeof(u64));
	m_failCnt = 0;
}

/** 打印IntentionLock使用统计信息
 * @param out 输出流
 */
void IntentionLockUsage::print(std::ostream &out) const {
	LockUsage::print(out);
	out << "  Lock count: " << m_lockCnt << endl;
}

/** 打印所有IntentionLock使用统计信息
 * @param out 输出流
 */
void IntentionLockUsage::printAll(std::ostream &out) {
	IntentionLockUsageScanHandle h;
	const IntentionLockUsage *usage;

	out << "== intention lock  usage ================================================================" << endl;
	
	beginScan(&h);
	while ((usage = getNext(&h)) != NULL) {
		usage->print(out);
	}
	endScan(&h);
}

/**
 * 开始扫描IntentionLock统计信息，扫描过程中将不允许IntentionLock对象创建与析构
 *
 * @param h IN/OUT 扫描句柄
 */
void IntentionLockUsage::beginScan(IntentionLockUsageScanHandle *h) {
	while (!g_ilLock.compareAndSwap(0, 1)) {}
	h->m_curPos = 0;
}

/**
 * 获取下一个IntentionLock统计对象
 *
 * @param h IN/OUT 扫描句柄
 * @return 下一个IntentionLock统计对象，扫描完时返回NULL
 */
const IntentionLockUsage* IntentionLockUsage::getNext(IntentionLockUsageScanHandle *h) {
	if (h->m_curPos < g_intentionLockUsages.getSize())
		return (IntentionLockUsage *)g_intentionLockUsages.getAt(h->m_curPos++);
	else
		return NULL;
}

/**
 * 结束扫描IntentionLock统计信息，重新允许IntentionLock对象创建与析构
 *
 * @param h IN/OUT 扫描句柄
 */
void IntentionLockUsage::endScan(IntentionLockUsageScanHandle *h) {
	UNREFERENCED_PARAMETER(h);
	g_ilLock.set(0);
}
///////////////////////////////////////////////////////////////////////////////
// IntentionLock //////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////////
/** 构造函数
 * @param maxUsers 使用该锁对象的最大用户数
 * @param name 锁名称，拷贝引用
 * @param file 创建锁对象时的代码文件
 * @param line 创建锁对象时的代码行号
 */
IntentionLock::IntentionLock(u16 maxUsers, const char *name, const char *file, uint line): Lock(name, file, line),
	m_lock("IntentionLock", file, line) {
	m_usage = allocIntentionLockUsage(m_name, concisedFile(file), line);
	m_maxUsers = maxUsers;
	m_state = new LockerInfo[maxUsers + 1];
	memset(m_wants, 0, sizeof(m_wants));
	memset(m_gots, 0, sizeof(m_gots));
}

/** 析构函数 */
IntentionLock::~IntentionLock() {
	freeIntentionLockUsage(m_usage);
	m_usage = NULL;
	delete []m_state;
}

/** 加锁
 * @param userId 用户ID，必须在[1,maxUsers]之间
 * @param mode 要加的锁模式
 * @param timeoutMs >0表示超时时间，单位毫秒，=0行为为尝试加锁，即不能马上加上锁即立即超时，<0不超时
 * @param file 代码文件
 * @param line 代码行号
 * @return 是否成功
 */
bool IntentionLock::lock(u16 userId, ILMode mode, int timeoutMs, const char *file, uint line) {
	assert(userId > 0 && userId <= m_maxUsers);
	assert(m_state[userId].m_mode == 0);

	m_usage->m_lockCnt[mode]++;
	setWant(mode);
	uint i, n = timeoutMs == 0? 1: Mutex::SPIN_COUNT;
	for (i = 0; i < n; i++) {
		if (tryLock(userId, mode, file, line))
			break;
	}
	if (i > 0)
		m_usage->m_spinCnt++;
	if (i < n) {
		return true;
	} else if (timeoutMs == 0) {
		resetWant(mode);
		m_usage->m_failCnt++;
		return false;
	}
	
	setWait(userId, mode);
	u64 before = System::currentTimeMillis();
	while (true) {
		m_waiting.increment();
		m_event.wait(100);
		m_waiting.decrement();
		if (tryLock(userId, mode, file, line))
			break;
		u64 now = System::currentTimeMillis();
		if (timeoutMs > 0 && now - before >= (u64)timeoutMs) {
			m_usage->m_waitCnt++;
			m_usage->m_waitTime += now - before;
			m_usage->m_failCnt++;
			resetWant(mode);
			resetWait(userId);
			return false;
		}
	}
	m_usage->m_waitCnt++;
	m_usage->m_waitTime += System::currentTimeMillis() - before;
	resetWait(userId);
	return true;
}

/** 判断锁冲突是否全部来自本线程其他session的伪冲突
 * @param mode 当前session欲加锁模式
 * @return 冲突全部来自同一线程不同session时返回true,其他返回false
 */
bool IntentionLock::isSelfConflict(ILMode mode) {
	uint currThd = Thread::currentOSThreadID();
	for (u16 j = m_maxUsers; j >= 1; j--) {
		if (m_state[j].m_mode != IL_NO && CONFLICT_MATRIX[mode][m_state[j].m_mode] && m_state[j].m_threadId != currThd)
			return false;
	}
	return true;
}

/** 尝试加锁
 * @param userId 用户ID
 * @param mode 锁类型
 * @param file 代码文件
 * @param line 代码行号
 * @return 是否成功
 */
bool IntentionLock::tryLock(u16 userId, ILMode mode, const char *file, uint line) {
	LOCK(&m_lock);
	bool succ = false;
	switch (mode) {
	case IL_IS:
		succ = m_wants[IL_X] == 0;
		break;
	case IL_IX:
		succ = m_wants[IL_S] == 0 && m_wants[IL_SIX] == 0 && m_wants[IL_U] == 0 && m_wants[IL_X] == 0;
		break;
	case IL_S:
		succ = m_gots[IL_IX] == 0 && m_gots[IL_SIX] == 0 && m_wants[IL_X] == 0;
		break;
	case IL_SIX:
		succ = m_gots[IL_IX] == 0 && m_gots[IL_S] == 0 && m_gots[IL_SIX] == 0 && m_wants[IL_U] == 0 && m_wants[IL_X] == 0;
		break;
	case IL_U:
		succ = m_gots[IL_IX] == 0 && m_gots[IL_SIX] == 0 && m_gots[IL_U] == 0 && m_gots[IL_X] == 0;
		break;
	case IL_X:
		succ = m_gots[IL_IS] == 0 && m_gots[IL_IX] == 0 && m_gots[IL_S] == 0 && m_gots[IL_SIX] == 0 && m_gots[IL_U] == 0 && m_gots[IL_X] == 0;
		break;
	default:
		assert(false);
	}
	if (!succ && isSelfConflict(mode))
		succ = true;
	if (succ) {
		m_state[userId].m_mode = (u16)mode;
		m_state[userId].m_threadId = Thread::currentOSThreadID();
		setGot(mode);
	}

	UNLOCK(&m_lock);
	if (succ) {
		m_state[userId].m_file = file;
		m_state[userId].m_line = line;
	}
	return succ;
}

/** 在需要时设置试图加锁标志
 * @pre 没有加m_lock
 *
 * @param mode 试图要加的锁
 */
void IntentionLock::setWant(ILMode mode) {
	LOCK(&m_lock);
	m_wants[mode]++;
	UNLOCK(&m_lock);
}

/** 在需要时重置试图加锁标志
 * @pre 没有加m_lock
 *
 * @param mode 原试图要加的锁
 */
void IntentionLock::resetWant(ILMode mode) {
	LOCK(&m_lock);
	m_wants[mode]--;
	UNLOCK(&m_lock);
}

/** 根据锁模式设置相应的得到锁标志
 * @pre 已经加了m_lock
 *
 * @param mode 已经加上的锁
 */
void IntentionLock::setGot(ILMode mode) {
	assert(m_lock.isLocked());
	m_gots[mode]++;
}

/** 根据锁模式重置相应的得到锁标志
 * @pre 已经加了m_lock
 *
 * @param mode 已经释放的锁
 */
void IntentionLock::resetGot(ILMode mode) {
	assert(m_lock.isLocked());
	m_gots[mode]--;
}

/** 设置等锁信息
 * @param userId 想加锁的用户
 * @param mode 想加的锁模式
 */
void IntentionLock::setWait(u16 userId, ILMode mode) {
	assert(m_state[userId].m_waitFor == LockerInfo::WAIT_FOR_NOTHING);
	u16 i;
	for (i = 1; i <= m_maxUsers; i++) {
		if (i != userId && isConflict(mode, (ILMode)m_state[i].m_mode)) {
			m_state[userId].m_waitFor = i;
			break;
		}
	}
}

/** 清除等锁信息
 * @param userId 用户
 */
void IntentionLock::resetWait(u16 userId) {
	m_state[userId].m_waitFor = LockerInfo::WAIT_FOR_NOTHING;
}

/** 锁升级
 * @param userId 用户ID，必须在[1,maxUsers]之间
 * @param oldMode 原来加的锁，可以是IL_NO
 * @param newMode 要升级成这个锁，必须是比oldMode更高级的锁
 * @param timeoutMs >0表示超时时间，单位毫秒，=0行为为尝试加锁，即不能马上加上锁即立即超时，<0不超时
 * @param file 代码文件
 * @param line 代码行号
 * @return 是否成功
 */
bool IntentionLock::upgrade(u16 userId, ILMode oldMode, ILMode newMode, int timeoutMs, const char *file, uint line) {
	assert(userId > 0 && userId <= m_maxUsers);
	assert(m_state[userId].m_mode == oldMode);
	assert(isHigher(oldMode, newMode));
	if (oldMode == IL_NO)
		return lock(userId, newMode, timeoutMs, file, line);
	
	m_usage->m_lockCnt[newMode]++;
	setWant(newMode);
	uint i, n = timeoutMs == 0? 1: Mutex::SPIN_COUNT;
	for (i = 0; i < n; i++) {
		if (tryUpgrade(userId, oldMode, newMode, file, line))
			break;
	}
	if (i > 0)
		m_usage->m_spinCnt++;
	if (i < n) {
		return true;
	} else if (timeoutMs == 0) {
		resetWant(newMode);
		return false;
	}

	setWait(userId, newMode);
	u64 before = System::currentTimeMillis();
	while (true) {
		m_waiting.increment();
		m_event.wait(100);
		m_waiting.decrement();
		if (tryUpgrade(userId, oldMode, newMode, file, line))
			break;
		u64 now = System::currentTimeMillis();
		if (timeoutMs > 0 && now - before >= (u64)timeoutMs) {
			m_usage->m_waitCnt++;
			m_usage->m_waitTime += now - before;
			resetWant(newMode);
			resetWait(userId);
			return false;
		}
	}
	m_usage->m_waitCnt++;
	m_usage->m_waitTime += System::currentTimeMillis() - before;
	resetWait(userId);
	return true;
}

/** 尝试锁升级
 * @pre 新模式的want标志已经被设置
 * @post 若成功则旧模式的want/got标志已经被清除，新模式的got标志已经被设置
 *
 * @param userId 用户ID，必须在[1,maxUsers]之间
 * @param oldMode 原来加的锁，可以是IL_NO
 * @param newMode 要升级成这个锁
 * @param file 代码文件
 * @param line 代码行号
 * @return 是否成功
 */
bool IntentionLock::tryUpgrade(u16 userId, ILMode oldMode, ILMode newMode, const char *file, uint line) {
	assert(oldMode != IL_NO);
	bool succ = false;
	LOCK(&m_lock);
	switch (newMode) {
	case IL_IX:
		assert(oldMode == IL_IS);
		succ = m_wants[IL_S] == 0 && m_wants[IL_SIX] == 0 && m_wants[IL_U] == 0 && m_wants[IL_X] == 0;
		break;
	case IL_S:
		assert(oldMode == IL_IS);
		succ = m_gots[IL_IX] == 0 && m_gots[IL_SIX] == 0 && m_wants[IL_X] == 0;
		break;
	case IL_SIX:
		assert(oldMode == IL_IS || oldMode == IL_IX || oldMode == IL_S);
		succ = m_gots[IL_IX] == (uint)(oldMode == IL_IX? 1: 0)
			&& m_gots[IL_S] == (uint)(oldMode == IL_S? 1: 0)
			&& m_gots[IL_SIX] == 0 && m_wants[IL_U] == 0 && m_wants[IL_X] == 0;
		break;
	case IL_U:
		assert(oldMode == IL_IS || oldMode == IL_S);
		succ = m_gots[IL_IX] == 0 && m_gots[IL_SIX] == 0 && m_gots[IL_U] == 0 && m_gots[IL_X] == 0;
		break;
	case IL_X:
		succ = m_gots[IL_IS] == (uint)(oldMode == IL_IS? 1: 0)
			&& m_gots[IL_IX] == (uint)(oldMode == IL_IX? 1: 0)
			&& m_gots[IL_S] == (uint)(oldMode == IL_S? 1: 0)
			&& m_gots[IL_SIX] == (uint)(oldMode == IL_SIX? 1: 0)
			&& m_gots[IL_U] == (uint)(oldMode == IL_U? 1: 0)
			&& m_gots[IL_X] == 0;
		break;
	default:
		assert(false);
	}
	if (!succ && isSelfConflict(newMode))
		succ = true;
	if (succ) {
		assert(m_state[userId].m_threadId == Thread::currentOSThreadID());
		m_state[userId].m_mode = (u16)newMode;
		setGot(newMode);
		resetGot(oldMode);
	}
	UNLOCK(&m_lock);
	if (succ) {
		resetWant(oldMode);
		m_state[userId].m_file = file;
		m_state[userId].m_line = line;
	}
	return succ;
}

/** 解锁
 * @param userId 用户ID，必须在[1,maxUsers]之间
 * @param mode 锁模式
 */
void IntentionLock::unlock(u16 userId, ILMode mode) {
	assert(userId > 0 && userId <= m_maxUsers);
	assert(m_state[userId].m_mode == mode);

	LOCK(&m_lock);
	m_state[userId].m_mode = IL_NO;
	m_state[userId].m_threadId = 0;
	resetGot(mode);
	UNLOCK(&m_lock);

	resetWant(mode);
	
	if (m_waiting.get() > 0)
		m_event.signal();
}

/** 锁降级
 * @param userId 用户ID，必须在[1,maxUsers]之间
 * @param oldMode 原来加的锁
 * @param newMode 要降级成这个锁，必须是比oldMode更低级的锁，若为IL_NO则相当于解锁
 */
void IntentionLock::downgrade(u16 userId, ILMode oldMode, ILMode newMode, const char *file, uint line) {
	assert(userId > 0 && userId <= m_maxUsers);
	assert(m_state[userId].m_mode == oldMode);
	assert(isHigher(newMode, oldMode));
	
	if (newMode == IL_NO) {
		unlock(userId, oldMode);
		return;
	}

	m_usage->m_lockCnt[newMode]++;
	LOCK(&m_lock);
	resetGot(oldMode);
	m_state[userId].m_mode = (u16)newMode;
	assert(m_state[userId].m_threadId == Thread::currentOSThreadID());
	setGot(newMode);
	UNLOCK(&m_lock);

	resetWant(oldMode);
	setWant(newMode);
	
	m_state[userId].m_file = file;
	m_state[userId].m_line = line;
	if (newMode == IL_NO)
		m_state[userId].m_threadId = 0;
	
	if (m_waiting.get() > 0)
		m_event.signal();
}

/** 是否加了指定的锁
 * @param userId 用户ID，必须在[1,maxUsers]之间或0，若为0则判断是否有某个用户加了指定的锁
 * @param mode 锁模式
 * @return 是否加了指定的锁
 */
bool IntentionLock::isLocked(u16 userId, ILMode mode) {
	assert(userId <= m_maxUsers);
	if (userId > 0)
		return m_state[userId].m_mode == mode;
	else
		return m_gots[mode] > 0;
}

/** 返回指定用户所加的锁模式
 * @param userId 用户ID，必须在[1,maxUsers]之间
 * @return 该用户加的锁模式，若没加锁则返回IL_NO
 */
ILMode IntentionLock::getLock(u16 userId) const {
	assert(userId > 0 && userId <= m_maxUsers);
	return (ILMode)m_state[userId].m_mode;
}

// /** 获得锁使用情况统计信息
//  * @return 锁使用情况统计信息，指向内部数据，外界请不要修改或释放
//  */
// const ILUsage* IntentionLock::getUsage() const {
// 	return &m_usage;
// }

/** 得到锁模式的字符串表示
 * @mode 锁模式
 * @return 字符串表示，为字符串常量
 */
const char* IntentionLock::getLockStr(ILMode mode) {
	if (mode == IL_NO)
		return "NO";
	else if (mode == IL_IS)
		return "IS";
	else if (mode == IL_IX)
		return "IX";
	else if (mode == IL_S)
		return "S";
	else if (mode == IL_U)
		return "U";
	else if (mode == IL_X)
		return "X";
	else {
		assert(mode == IL_SIX);
		return "SIX";
	}
}

/** 判断指定的两个锁模式是否冲突
 * @param mode1 锁模式一
 * @param mode2 锁模式二
 * @return 是否冲突
 */
bool IntentionLock::isConflict(ILMode mode1, ILMode mode2) {
	if (mode1 == IL_NO || mode2 == IL_NO)
		return false;
	else if (mode1 == IL_IS)
		return mode2 == IL_X;
	else if (mode1 == IL_IX)
		return mode2 != IL_IS && mode2 != IL_IX;
	else if (mode1 == IL_S)
		return mode2 != IL_S && mode2 != IL_IS && mode2 != IL_U;
	else if (mode1 == IL_SIX)
		return mode2 != IL_IS;
	else if (mode1 == IL_U)
		return mode2 != IL_IS && mode2 != IL_S;
	else {
		assert(mode1 == IL_X);
		return true;
	}
}

/** 判断high模式是否是比low更高级别的锁
 * @param low 锁模式一
 * @param high 锁模式二
 * @return high是否比low更级
 */
bool IntentionLock::isHigher(ILMode low, ILMode high) {
	if (low == high)
		return false;
	else if (low == IL_IX && high == IL_S)
		return false;
	else if (low == IL_SIX && high == IL_U)
		return false;
	else if (low == IL_IX && high == IL_U)
		return false;
	else
		return high > low;
}

/** 判断指定的意向锁是否为读锁
 * @param mode 锁模式
 * @return 是否为读锁
 */
bool IntentionLock::isRead(ILMode mode) {
	return mode == IL_IS || mode == IL_S;
}

/** 判断指定的意向锁是否为写锁
 * @param mode 锁模式
 * @return 是否为写锁
 */
bool IntentionLock::isWrite(ILMode mode) {
	return mode == IL_U || mode == IL_IX || mode == IL_SIX || mode == IL_X;
}

/**
 * 获得使用情况统计信息。
 *
 * @return 使用情况统计信息
 */
const IntentionLockUsage* IntentionLock::getUsage() const {
	return m_usage;
}

}