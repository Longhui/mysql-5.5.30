/**
 * ���߳�ͬ����һЩʵ��
 *
 * @author ��Դ(wangyuan@corp.netease.com, wy@163.org)
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
 * ���캯��
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
 * ��������
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
 * ���ѵȴ������е��¼�
 */
void EventMonitorHelper::wakeUpEvents() {
#ifdef WIN32
	EnterCriticalSection(&Event::m_eventListLock);
#else
	pthread_mutex_lock(&Event::m_eventListLock);
#endif

	DLink<WaitEvent *> *cur = Event::m_waitEventList->getHeader()->getNext();
	//���ڶ����еȺ�ʱ�䲻��WAIT_TIME_THRESHOLD����¼���ɨ���̲߳����м��
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
 * ��ͣ��̨����ɨ���߳�
 */
void EventMonitorHelper::pauseMonitor() {
	assert(NULL != m_task);
	m_task->pause();
}

/**
 * ������̨����ɨ���߳�
 */
void EventMonitorHelper::resumeMonitor() {
	assert(NULL != m_task);
	m_task->resume();
}

/**
 * ���캯��
 *
 * @param interval ����ʱ�����ڣ���λ��
 */
EventMonitor::EventMonitor(uint interval)
: Task("EventMonitor", interval * 1000) {
	m_lastChkTime = System::fastTime();
	m_interval = interval;
}

/**
 * ����
 * @see <code>Task::run()</code> ����
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


/* �ȴ�����*/
DList<WaitEvent *> *Event::m_waitEventList = NULL;

/** 
 * ���캯��
 * 
 * @param autoReset �ȴ������Ƿ��Զ����¼�����Ϊ�Ǵ���״̬
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
 * ��������
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
 * ��ȡ�¼�ͳ����Ϣ
 * 
 * @return ����<code>EventUsage</code> ʵ����ʾ�¼�ͳ����Ϣ
 */
const EventUsage& Event::getUsage() const
{
	assert (NULL != m_usage);
	return *m_usage;
}

/** 
 * �����¼�Ϊ�Ǵ���״̬�����ص�ǰ�¼���������ֵ����ֵ�����ں����ȴ�������
 * �¼���������ֵ��ÿ�ε���signalʱ���1
 *
 * @pre		�¼�����ʱָ��autoResetΪfalse
 * @return	��ǰ�¼���������ֵ
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
 * ����¼��ȴ�����
 *
 * @param resetToken	�¼���������ֵ
 * @return ����Ƿ�ȴ���true��ʾ�¼�����Ҫwait�� false��ʾ�¼���Ҫִ�в���ϵͳ��wait
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
 * ���ȴ��¼�����ȴ��¼�����
 *
 * @param plnk �ȴ��¼�
 * @param noAction �Ƿ񲻼���ȴ�����
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
 * ���ȴ��¼��ӵȴ��¼��������Ƴ�
 *
 * @param plnk �ȴ��¼�
 * @param noAction �Ƿ�ִ���Ƴ��ȴ�����
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
 * �ȴ��¼�
 *
 * @param timeoutMs		��ʱʱ�䣬��λΪ���룬��<=0�򲻳�ʱ����>0����ʱ�ȴ�
 * @param resetToken	�¼���������ֵ��ͨ��Ϊ��ǰ����reset�ķ���ֵ��
 *   ��ΪUNSPECIFIED_TOKEN���ʾδָ������ʱwait������������أ����˳�ʱ��
 *   1. ������signal��û����wait������һ��wait�������Ϸ���
 *   2. wait�����˵�����signal
 *   ��token����UNSPECIFIED_TOKEN��������¼���ǰ��������ֵ����token��wait
 *   �ͻ����Ϸ���
 * @pre ���¼�����ʱָ��autoResetΪtrue����resetToken����ΪUNSPECIFIED_TOKEN
 *
 * @return true��ʾ�Ǳ�����أ�false��ʾ�ǳ�ʱ���أ�ע�⼴ʹ��û��Ӧ�ô���
 *   ������¼�ʱ��ϵͳ��ʱҲ�ἤ���¼�
 */
bool Event::wait(int timeoutMs, long resetToken) {
	assert(resetToken == UNSPECIFIED_TOKEN || !m_autoReset);

	m_usage->m_waitCnt++;
	
	//����Ƿ���Ҫ����wait
	lock();
	bool noWait = checkNoWait(resetToken);
	if (noWait) {
		unlock();
		return true;
	}
	unlock(); //Note:������������˽���������Linux��ʵ�֣���ִ�������ȴ�ǰ��������check���Ƿ���Ҫ�ȴ���

	//��ʼ��WaitEventʵ����
	DLink<WaitEvent *> lnk;	
	WaitEvent we;
	we.evt = this;
	we.wStartTime = System::fastTime();
	lnk.set(&we);

	bool noWaitListAction = (timeoutMs > 0 && timeoutMs < 5000);
	//�����¼����ȴ�����		
	addWaitEvent(&lnk, noWaitListAction);
	

#ifdef WIN32
	if (timeoutMs <= 0)
		timeoutMs = INFINITE;	
	//ִ�еȴ�
	DWORD code = ::WaitForSingleObject(m_osEvent, timeoutMs);
	//�ӵȴ��������Ƴ�
	removeWaitEvent(&lnk, noWaitListAction);
	//�����Ƿ�ȴ��ɹ�
	return code != WAIT_TIMEOUT;
#else
	int r;
	struct timeval now;
	struct timespec timeout;
	
	if (timeoutMs <= 0) {	//���޵ȴ�	
		//����Ƿ���Ҫ����ִ��wait
		lock();
		bool noWait = checkNoWait(resetToken);
		if (noWait) {
			unlock();
			//�ӵȴ��������Ƴ�
			removeWaitEvent(&lnk, noWaitListAction);
			return true;
		}
		//ִ�еȴ�
		r = pthread_cond_wait(&m_cond, &m_lock);
		
	} else {	//��ʱ�ȴ�
		//���㳬ʱʱ��
		gettimeofday(&now, NULL);
		now.tv_sec += timeoutMs / 1000;
		now.tv_usec += (timeoutMs % 1000) * 1000;
		if (now.tv_usec >= 1000000) {
			now.tv_sec++;
			now.tv_usec -= 1000000;
		}
		timeout.tv_sec = now.tv_sec;
		timeout.tv_nsec = now.tv_usec * 1000;

		//����Ƿ���Ҫ����ִ��wait
		lock();
		bool noWait = checkNoWait(resetToken);
		if (noWait) {
			unlock();
			removeWaitEvent(&lnk, noWaitListAction);
			return true;
		}
		
		//ִ����ʱ�ȴ�
		r = pthread_cond_timedwait(&m_cond, &m_lock, &timeout);	
	}
	if (r == 0 && m_autoReset) {
		m_set = false;
	}
	unlock();
	//�ӵȴ��������Ƴ�
	removeWaitEvent(&lnk, noWaitListAction);
	
	//�����Ƿ�ȴ��ɹ�
	return r == 0;
#endif
}

/** 
 * �����¼���������Щ�ȴ���һ�¼����߳�
 * 
 * @param ���ѵȴ��¼��������̻߳�ֻ����һ����һ�������ֻ����һ���������Ա�֤��
 *   �˲�����Windowsƽ̨����Ч
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
 * �����������ڲ���Ա���̷߳���
 */
void Event::lock() {
#ifdef WIN32
	EnterCriticalSection(&m_lock);
#else
	pthread_mutex_lock(&m_lock);
#endif
}

/**
 * ����������ڲ���Ա������
 */
void Event::unlock() {
#ifdef WIN32
	LeaveCriticalSection(&m_lock);
#else
	pthread_mutex_unlock(&m_lock);
#endif
}

/** ����һ��������
 * @param name ���������ƣ�����ʹ��
 * @param file ����������ʱ�Ĵ����ļ�
 * @param line ����������ʱ���к�
 */
Lock::Lock(const char *name, const char *file, uint line) {
	m_name = System::strdup(name);
	m_file = file;
	m_line = line;
}

/** ���������� */
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
 * ���캯��
 *
 * @param name ����������
 * @param allocFile ���ù��캯���Ĵ����ļ�
 * @param allocLine ���ù��캯���Ĵ����к�
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

/** ���ٶ��� */
LockUsage::~LockUsage() {
	delete []m_name;
}

/** ��ӡ������ʹ��ͳ����Ϣ
 * @param out �����
 */
void LockUsage::print(std::ostream &out) const {
	out << "Lock: " << m_name << "[" << m_allocFile << ":" << m_allocLine << "]" << endl;
	out << "  Instances: " << m_instanceCnt << endl;
	out << "  Spin count: " << m_spinCnt << endl;
	out << "  Wait count: " << m_waitCnt << endl;
	out << "  Wait time: " << m_waitTime << endl;
}

/** ��ʹ�����ͳ�ƶ���ı�־ */
struct LockUsageKey {
	const char	*m_allocFile;	/** ���������ʱ�Ĵ����ļ��� */
	uint		m_allocLine;	/** ���������ʱ�Ĵ����к� */
	const char	*m_name;		/** ���������� */

	LockUsageKey(const char *name, const char *allocFile, uint allocLine) {
		m_name = name;
		m_allocFile = allocFile;
		m_allocLine = allocLine;
	}
};

/** ����LockUsageKey���Ͷ����ϣֵ�ĺ������� */
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

/** ����LockUsage���Ͷ����ϣֵ�ĺ������� */
class LuHasher {
public:
	inline unsigned int operator ()(const LockUsage *usage) const {
		return m_keyHasher.operator()(LockUsageKey(usage->m_name, usage->m_allocFile, usage->m_allocLine));
	}

private:
	LukHasher	m_keyHasher;
};

/** �ж�LockUsageKey��LockUsage��������Եĺ������� */
class LukLuEqualer {
public:
	inline bool operator()(const LockUsageKey &usageKey, const LockUsage *usage) const {
		return !strcmp(usageKey.m_allocFile, usage->m_allocFile) && usageKey.m_allocLine == usage->m_allocLine
			&& !strcmp(usageKey.m_name, usage->m_name);
	}
};

static bool	g_mutexUsagesReady = false;	/** Mutexʹ�����ͳ�ƶ����ϣ���Ƿ��Ѿ���ʼ�� */
class MuHash: public DynHash<LockUsageKey, LockUsage *, LuHasher, LukHasher, LukLuEqualer> {
public:
	MuHash() {
		g_mutexUsagesReady = true;
	}
};

static MuHash		g_mutexUsages;	/** Mutexʹ�����ͳ�ƶ����ϣ�� */
static Atomic<int>	g_muLock;		/** ����������ϣ�������� */

/**
 * ���ؼ���·�����������ntse�����Ŀ¼��·��
 *
 * @param file �����ļ�·��
 * @return ����·��
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
 * ����һ��MutexUsage����������ͬ�Ĵ����кŵĶ���������������ü���
 *
 * @param name ����������
 * @param file ���ù��캯���Ĵ����ļ�
 * @param line ���ù��캯���Ĵ����к�
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
 * �ͷ�һ��MutexUsage���󣬼������ü�������������Ϊ0���ͷ�֮
 *
 * @param usage MutexUsage����
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
 * ���캯��
 *
 * @param name ����������
 * @param allocFile ���ù��캯���Ĵ����ļ�
 * @param allocLine ���ù��캯���Ĵ����к�
 */
MutexUsage::MutexUsage(const char *name, const char *allocFile, uint allocLine):
	LockUsage(name, allocFile, allocLine) {
	m_lockCnt = 0;
}

/** ��ӡ������ʹ��ͳ����Ϣ
 * @param out �����
 */
void MutexUsage::print(std::ostream &out) const {
	LockUsage::print(out);
	out << "  Lock count: " << m_lockCnt << endl;
}

/** ��ӡ���л�����ʹ��ͳ����Ϣ
 * @param out �����
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
 * ��ʼɨ��Mutexͳ����Ϣ��ɨ������н�������Mutex���󴴽�������
 *
 * @param h IN/OUT ɨ����
 */
void MutexUsage::beginScan(MuScanHandle *h) {
	while (!g_muLock.compareAndSwap(0, 1)) {}
	h->m_curPos = 0;
}

/**
 * ��ȡ��һ��Mutexͳ�ƶ���
 *
 * @param h IN/OUT ɨ����
 * @return ��һ��Mutexͳ�ƶ���ɨ����ʱ����NULL
 */
const MutexUsage* MutexUsage::getNext(MuScanHandle *h) {
	if (h->m_curPos < g_mutexUsages.getSize())
		return (MutexUsage *)g_mutexUsages.getAt(h->m_curPos++);
	else
		return NULL;
}

/**
 * ����ɨ��Mutexͳ����Ϣ����������Mutex���󴴽�������
 *
 * @param h IN/OUT ɨ����
 */
void MutexUsage::endScan(MuScanHandle *h) {
	UNREFERENCED_PARAMETER(h);
	g_muLock.set(0);
}

#pragma region NEW_MUTEX

/**
 * ���캯��.
 *
 * @param name ���������ƣ�����ʹ��
 * @param file ���ù��캯���Ĵ����ļ�
 * @param line ���ù��캯���Ĵ����к�
 */
Mutex::Mutex(const char *name, const char *file, uint line): Lock(name, file, line), m_event(false) {
	m_usage = allocMutexUsage(m_name, concisedFile(file), line);
	m_file = NULL;
	m_line = 0;
}

/** 
 * ��������
 */
Mutex::~Mutex() {
	freeMutexUsage(m_usage);
	m_usage = NULL;
}

/**
 * ���ʹ�����ͳ����Ϣ��
 *
 * @return ʹ�����ͳ����Ϣ
 */
const MutexUsage* Mutex::getUsage() const {
	return m_usage;
}

/**
 * ��������ͻ
 *
 * @param timeoutMs ��ʱʱ�䣬��λ���롣��>0Ϊ��ʱʱ�䣬��<0��ʾ����ʱ����=0�������ؽ����
 * @return �Ƿ�����ɹ��� false, ������ʱ��true, �����ɹ���
 */
bool Mutex::lockConflict(int timeoutMs) {
	uint i;	  /* spin round count */
	// ����
	m_usage->m_spinCnt++;

	// �Ƿ�Event wait��ʱ
	bool timeoutFlag = false;
	// �Ƿ�ִ�й�Event wait
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

	//ÿThreadִ��conflictwait,m_waitCntֻ��1.
	if (!hasWaited) {
		m_usage->m_waitCnt++;
	}

	u64 before = System::currentTimeMillis();	
	timeoutFlag = m_event.wait(timeoutMs, sigToken);
	if (!hasWaited) {
		hasWaited = true;
	}
	u64 now = System::currentTimeMillis();

	//����LockUage�ĵȴ�ʱ��ͳ��
	m_usage->m_waitTime += (now - before);

	//�ȴ���ʱ�ģ�ֱ�ӷ���
	if (!timeoutFlag) {
		return false;
	}

	if (timeoutMs > 0) {
		//��ʱ�������³�ʱʱ��
		timeoutMs -= (int)(now - before);
		if (timeoutMs <= 0) {
			//��ʱ�˳�
			return false;
		}
	}
	goto mutex_loop;
}

#pragma endregion //Mutex��ʵ��


///////////////////////////////////////////////////////////////////////////////
// RWLock /////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////////
static bool g_rwlUsagesReady = false;	/** RWLockʹ�����ͳ�ƶ����ϣ���Ƿ��Ѿ���ʼ�� */
class RwluHash: public DynHash<LockUsageKey, LockUsage *, LuHasher, LukHasher, LukLuEqualer> {
public:
	RwluHash() {
		g_rwlUsagesReady = true;
	}
};

static RwluHash		g_rwlUsages;	/** RWLockʹ�����ͳ�ƶ����ϣ��(����BiasedRWLock) */
static Atomic<int>	g_rwlLock;		/** ����������ϣ�������� */

/**
 * ����һ��RWLockUsage����������ͬ�Ĵ����кŵĶ���������������ü���
 *
 * @param name ����������
 * @param file ���ù��캯���Ĵ����ļ�
 * @param line ���ù��캯���Ĵ����к�
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
 * �ͷ�һ��RWLockUsage���󣬼������ü�������������Ϊ0���ͷ�֮
 *
 * @param usage RWLockUsage����
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
 * ���캯��
 *
 * @param name ����������
 * @param allocFile ���ù��캯���Ĵ����ļ�
 * @param allocLine ���ù��캯���Ĵ����к�
 */
RWLockUsage::RWLockUsage(const char *name, const char *allocFile, uint allocLine):
	LockUsage(name, allocFile, allocLine) {
	m_rlockCnt = m_wlockCnt = 0;
}

/** ��ӡ��д��ʹ��ͳ����Ϣ */
void RWLockUsage::print(std::ostream &out) const {
	LockUsage::print(out);
	out << "  Read locks: " << m_rlockCnt << endl;
	out << "  Write locks: " << m_wlockCnt << endl;
}

/**
 * ��ʼɨ���д��ͳ����Ϣ��ɨ������н��������д�����󴴽�������
 *
 * @param h IN/OUT ɨ����
 */
void RWLockUsage::beginScan(RwluScanHandle *h) {
	while (!g_rwlLock.compareAndSwap(0, 1)) {}
	h->m_curPos = 0;
}

/** ��ӡ���л�����ʹ��ͳ����Ϣ
 * @param out �����
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
 * ��ȡ��һ����д��ͳ�ƶ���
 *
 * @param h IN/OUT ɨ����
 * @return ��һ����д��ͳ�ƶ���ɨ����ʱ����NULL
 */
const RWLockUsage* RWLockUsage::getNext(RwluScanHandle *h) {
	if (h->m_curPos < g_rwlUsages.getSize())
		return (RWLockUsage *)g_rwlUsages.getAt(h->m_curPos++);
	else
		return NULL;
}

/**
 * ����ɨ���д��ͳ����Ϣ�����������д�����󴴽�������
 *
 * @param h IN/OUT ɨ����
 */
void RWLockUsage::endScan(RwluScanHandle *h) {
	UNREFERENCED_PARAMETER(h);
	g_rwlLock.set(0);
}

///////////////////////////////////////////////////////////////////////////////
// RWLock /////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////////
/**
 * ���캯��
 *
 * @param name ���������ƣ�����ʹ��
 * @param file ���ù��캯���Ĵ����ļ�
 * @param line ���ù��캯���Ĵ����к�
 * @param perferWriter �Ƿ����д���Ը������ȼ�������д���Ը������ȼ�
 *   �ڶ���д�ٵ�����¿�����Ч��ֹ��д�����ֶ������󣬵����ܵ��¼�
 *   ������ƽ��ʱ������
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
 * ���Խ���������
 *
 * @param file �ļ���
 * @param line �к�
 * @return �Ƿ������ɹ�
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
 * �ж϶�д���Ƿ��Ѿ�����ָ����ģʽ����
 *
 * @param lockMode ��ģʽ
 * @return ��д���Ƿ��Ѿ�����ָ����ģʽ����
 */
bool RWLock::isLocked(LockMode lockMode) const {
	assert(lockMode == Shared || lockMode == Exclusived);
	int lockWord = m_lockWord.get();
	if (lockMode == Shared)
		return (lockWord < WLOCK_DEC && lockWord > 0)	// ���˶�����û�������д��
			|| (lockWord < 0 && lockWord > -WLOCK_DEC);	// ���˶����������ڵȴ���д��
	else
		return lockWord == 0;
}

/**
 * ��õ�ǰ�ӵĶ�д����ģʽ
 * @return 
 */
LockMode RWLock::getLockMode() const {
	int lockWord = m_lockWord.get();
	return WLOCK_DEC == lockWord ? None : (0 == lockWord ? Exclusived : Shared);
}

/**
 * ���ʹ�����ͳ��
 *
 * @return ʹ�����ͳ��
 */
const RWLockUsage* RWLock::getUsage() const {
	return m_usage;
}

/**
 * �Ӷ���ʱ�����ͻ
 *
 * @param file �ļ���
 * @param line �к�
 * @param absTimeout ΪNEVER_TIMEOUT��ʾ������ʱ������Ϊ�����ʾ�ľ��Եĳ�ʱʱ��
 * @return �Ƿ�����ɹ�
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

/** ���еȴ�
 * @param evt �ȴ�����¼�
 * @param absTimeout ΪNEVER_TIMEOUT��ʾ������ʱ������Ϊ�����ʾ�ľ��Եĳ�ʱʱ��
 * @param token �¼���������
 * @return true��ʾû�г�ʱ��false��ʾ��ʱ��
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

/** ��д��ʱ�����ͻ
 * @param file �ļ���
 * @param line �к�
 * @param absTimeout ΪNEVER_TIMEOUT��ʾ������ʱ������Ϊ�����ʾ�ľ��Եĳ�ʱʱ��
 * @return �Ƿ�ɹ�
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

/** ���Ի�ȡm_lockWord�����������Ϊ��д���ĺ�ѡ��
 * @param absTimeout ΪNEVER_TIMEOUT��ʾ������ʱ������Ϊ�����ʾ�ľ��Եĳ�ʱʱ��
 * @param hasWait OUT���Ƿ�����˵ȴ�
 * @return �Ƿ�ɹ�
 */
bool RWLock::acquireLockWordW(u64 absTimeout, bool *hasWait) {
	if (decrLockWord(WLOCK_DEC)) {
		if (m_lockWord.get() == 0 || waitForReaders(absTimeout, hasWait))
			return true;
		incrLockWord(WLOCK_DEC);
		// ����Ӧ�û��ѣ���Ϊ�ҿ���������������
		if (m_waiters.get()) {
			m_waiters.compareAndSwap(1, 0);
			m_forOthers.signal(true);
		}
		return false;
	}
	return false;
}

/** ��Ϊд��Ψһ��ѡ��֮�󣬵ȴ��Ѿ��ӵĶ����ͷ�
 * @param absTimeout ΪNEVER_TIMEOUT��ʾ������ʱ������Ϊ�����ʾ�ľ��Եĳ�ʱʱ��
 * @param hasWait OUT���Ƿ�����˵ȴ�
 * @return �Ƿ�ɹ�
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
/** ���캯�� */
LockerInfo::LockerInfo() {
	reset();
}

/** ��������������Ϣ
 * @param mode ��ģʽ
 * @param file �����ļ�
 * @param line �����к�
 * @param threadId �߳�ID
 * @param waitFor �ȴ����û�ID
 */
void LockerInfo::set(u16 mode, const char *file, uint line, uint threadId, u16 waitFor) {
	m_mode = mode;
	m_file = file;
	m_line = line;
	m_threadId = threadId;
	m_waitFor = waitFor;
}

/** ��������������Ϣ */
void LockerInfo::reset() {
	set(0, NULL, 0, 0, WAIT_FOR_NOTHING);
}

///////////////////////////////////////////////////////////////////////////////
// LURWLock ///////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////////
/** ���캯��
 * @param maxUserId ���û�ID�����ֵ
 * @param name ����������
 * @param file �������������ڴ����ļ���
 * @param line �������������ڴ����к�
 * @param preferWriter �Ƿ����д���Ը������ȼ�
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

/** �������� */
LURWLock::~LURWLock() {
#ifdef NTSE_SYNC_DEBUG
	delete m_userInfos;
#endif
}

#ifdef NTSE_SYNC_DEBUG
/** ������ָ���û����ͻ���û�ID
 * @param userId �û�ID
 * @param mode ��ӵ���
 * @return ��ָ���û����ͻ���û�ID�����Ҳ�������WAIT_FOR_NOTHING
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
static bool	g_intentionLockUsagesReady = false;	/** IntentionLockʹ�����ͳ�ƶ����ϣ���Ƿ��Ѿ���ʼ�� */
class IlHash: public DynHash<LockUsageKey, LockUsage *, LuHasher, LukHasher, LukLuEqualer> {
public:
	IlHash() {
		g_intentionLockUsagesReady = true;
	}
};

static IlHash		g_intentionLockUsages;	/** IntentionLockʹ�����ͳ�ƶ����ϣ�� */
static Atomic<int>	g_ilLock;		/** ����������ϣ�������� */

/**
 * ����һ��IntentionLockUsage����������ͬ�Ĵ����кŵĶ���������������ü���
 *
 * @param name ����������
 * @param file ���ù��캯���Ĵ����ļ�
 * @param line ���ù��캯���Ĵ����к�
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
 * �ͷ�һ��IntentionLockUsage���󣬼������ü�������������Ϊ0���ͷ�֮
 *
 * @param usage IntentionLockUsage����
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
 * ���캯��
 *
 * @param name ����������
 * @param allocFile ���ù��캯���Ĵ����ļ�
 * @param allocLine ���ù��캯���Ĵ����к�
 */
IntentionLockUsage::IntentionLockUsage(const char *name, const char *allocFile, uint allocLine):
	LockUsage(name, allocFile, allocLine) {
	memset(&m_lockCnt, 0 , IL_MAX * sizeof(u64));
	m_failCnt = 0;
}

/** ��ӡIntentionLockʹ��ͳ����Ϣ
 * @param out �����
 */
void IntentionLockUsage::print(std::ostream &out) const {
	LockUsage::print(out);
	out << "  Lock count: " << m_lockCnt << endl;
}

/** ��ӡ����IntentionLockʹ��ͳ����Ϣ
 * @param out �����
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
 * ��ʼɨ��IntentionLockͳ����Ϣ��ɨ������н�������IntentionLock���󴴽�������
 *
 * @param h IN/OUT ɨ����
 */
void IntentionLockUsage::beginScan(IntentionLockUsageScanHandle *h) {
	while (!g_ilLock.compareAndSwap(0, 1)) {}
	h->m_curPos = 0;
}

/**
 * ��ȡ��һ��IntentionLockͳ�ƶ���
 *
 * @param h IN/OUT ɨ����
 * @return ��һ��IntentionLockͳ�ƶ���ɨ����ʱ����NULL
 */
const IntentionLockUsage* IntentionLockUsage::getNext(IntentionLockUsageScanHandle *h) {
	if (h->m_curPos < g_intentionLockUsages.getSize())
		return (IntentionLockUsage *)g_intentionLockUsages.getAt(h->m_curPos++);
	else
		return NULL;
}

/**
 * ����ɨ��IntentionLockͳ����Ϣ����������IntentionLock���󴴽�������
 *
 * @param h IN/OUT ɨ����
 */
void IntentionLockUsage::endScan(IntentionLockUsageScanHandle *h) {
	UNREFERENCED_PARAMETER(h);
	g_ilLock.set(0);
}
///////////////////////////////////////////////////////////////////////////////
// IntentionLock //////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////////
/** ���캯��
 * @param maxUsers ʹ�ø������������û���
 * @param name �����ƣ���������
 * @param file ����������ʱ�Ĵ����ļ�
 * @param line ����������ʱ�Ĵ����к�
 */
IntentionLock::IntentionLock(u16 maxUsers, const char *name, const char *file, uint line): Lock(name, file, line),
	m_lock("IntentionLock", file, line) {
	m_usage = allocIntentionLockUsage(m_name, concisedFile(file), line);
	m_maxUsers = maxUsers;
	m_state = new LockerInfo[maxUsers + 1];
	memset(m_wants, 0, sizeof(m_wants));
	memset(m_gots, 0, sizeof(m_gots));
}

/** �������� */
IntentionLock::~IntentionLock() {
	freeIntentionLockUsage(m_usage);
	m_usage = NULL;
	delete []m_state;
}

/** ����
 * @param userId �û�ID��������[1,maxUsers]֮��
 * @param mode Ҫ�ӵ���ģʽ
 * @param timeoutMs >0��ʾ��ʱʱ�䣬��λ���룬=0��ΪΪ���Լ��������������ϼ�������������ʱ��<0����ʱ
 * @param file �����ļ�
 * @param line �����к�
 * @return �Ƿ�ɹ�
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

/** �ж�����ͻ�Ƿ�ȫ�����Ա��߳�����session��α��ͻ
 * @param mode ��ǰsession������ģʽ
 * @return ��ͻȫ������ͬһ�̲߳�ͬsessionʱ����true,��������false
 */
bool IntentionLock::isSelfConflict(ILMode mode) {
	uint currThd = Thread::currentOSThreadID();
	for (u16 j = m_maxUsers; j >= 1; j--) {
		if (m_state[j].m_mode != IL_NO && CONFLICT_MATRIX[mode][m_state[j].m_mode] && m_state[j].m_threadId != currThd)
			return false;
	}
	return true;
}

/** ���Լ���
 * @param userId �û�ID
 * @param mode ������
 * @param file �����ļ�
 * @param line �����к�
 * @return �Ƿ�ɹ�
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

/** ����Ҫʱ������ͼ������־
 * @pre û�м�m_lock
 *
 * @param mode ��ͼҪ�ӵ���
 */
void IntentionLock::setWant(ILMode mode) {
	LOCK(&m_lock);
	m_wants[mode]++;
	UNLOCK(&m_lock);
}

/** ����Ҫʱ������ͼ������־
 * @pre û�м�m_lock
 *
 * @param mode ԭ��ͼҪ�ӵ���
 */
void IntentionLock::resetWant(ILMode mode) {
	LOCK(&m_lock);
	m_wants[mode]--;
	UNLOCK(&m_lock);
}

/** ������ģʽ������Ӧ�ĵõ�����־
 * @pre �Ѿ�����m_lock
 *
 * @param mode �Ѿ����ϵ���
 */
void IntentionLock::setGot(ILMode mode) {
	assert(m_lock.isLocked());
	m_gots[mode]++;
}

/** ������ģʽ������Ӧ�ĵõ�����־
 * @pre �Ѿ�����m_lock
 *
 * @param mode �Ѿ��ͷŵ���
 */
void IntentionLock::resetGot(ILMode mode) {
	assert(m_lock.isLocked());
	m_gots[mode]--;
}

/** ���õ�����Ϣ
 * @param userId ��������û�
 * @param mode ��ӵ���ģʽ
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

/** ���������Ϣ
 * @param userId �û�
 */
void IntentionLock::resetWait(u16 userId) {
	m_state[userId].m_waitFor = LockerInfo::WAIT_FOR_NOTHING;
}

/** ������
 * @param userId �û�ID��������[1,maxUsers]֮��
 * @param oldMode ԭ���ӵ�����������IL_NO
 * @param newMode Ҫ������������������Ǳ�oldMode���߼�����
 * @param timeoutMs >0��ʾ��ʱʱ�䣬��λ���룬=0��ΪΪ���Լ��������������ϼ�������������ʱ��<0����ʱ
 * @param file �����ļ�
 * @param line �����к�
 * @return �Ƿ�ɹ�
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

/** ����������
 * @pre ��ģʽ��want��־�Ѿ�������
 * @post ���ɹ����ģʽ��want/got��־�Ѿ����������ģʽ��got��־�Ѿ�������
 *
 * @param userId �û�ID��������[1,maxUsers]֮��
 * @param oldMode ԭ���ӵ�����������IL_NO
 * @param newMode Ҫ�����������
 * @param file �����ļ�
 * @param line �����к�
 * @return �Ƿ�ɹ�
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

/** ����
 * @param userId �û�ID��������[1,maxUsers]֮��
 * @param mode ��ģʽ
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

/** ������
 * @param userId �û�ID��������[1,maxUsers]֮��
 * @param oldMode ԭ���ӵ���
 * @param newMode Ҫ������������������Ǳ�oldMode���ͼ���������ΪIL_NO���൱�ڽ���
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

/** �Ƿ����ָ������
 * @param userId �û�ID��������[1,maxUsers]֮���0����Ϊ0���ж��Ƿ���ĳ���û�����ָ������
 * @param mode ��ģʽ
 * @return �Ƿ����ָ������
 */
bool IntentionLock::isLocked(u16 userId, ILMode mode) {
	assert(userId <= m_maxUsers);
	if (userId > 0)
		return m_state[userId].m_mode == mode;
	else
		return m_gots[mode] > 0;
}

/** ����ָ���û����ӵ���ģʽ
 * @param userId �û�ID��������[1,maxUsers]֮��
 * @return ���û��ӵ���ģʽ����û�����򷵻�IL_NO
 */
ILMode IntentionLock::getLock(u16 userId) const {
	assert(userId > 0 && userId <= m_maxUsers);
	return (ILMode)m_state[userId].m_mode;
}

// /** �����ʹ�����ͳ����Ϣ
//  * @return ��ʹ�����ͳ����Ϣ��ָ���ڲ����ݣ�����벻Ҫ�޸Ļ��ͷ�
//  */
// const ILUsage* IntentionLock::getUsage() const {
// 	return &m_usage;
// }

/** �õ���ģʽ���ַ�����ʾ
 * @mode ��ģʽ
 * @return �ַ�����ʾ��Ϊ�ַ�������
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

/** �ж�ָ����������ģʽ�Ƿ��ͻ
 * @param mode1 ��ģʽһ
 * @param mode2 ��ģʽ��
 * @return �Ƿ��ͻ
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

/** �ж�highģʽ�Ƿ��Ǳ�low���߼������
 * @param low ��ģʽһ
 * @param high ��ģʽ��
 * @return high�Ƿ��low����
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

/** �ж�ָ�����������Ƿ�Ϊ����
 * @param mode ��ģʽ
 * @return �Ƿ�Ϊ����
 */
bool IntentionLock::isRead(ILMode mode) {
	return mode == IL_IS || mode == IL_S;
}

/** �ж�ָ�����������Ƿ�Ϊд��
 * @param mode ��ģʽ
 * @return �Ƿ�Ϊд��
 */
bool IntentionLock::isWrite(ILMode mode) {
	return mode == IL_U || mode == IL_IX || mode == IL_SIX || mode == IL_X;
}

/**
 * ���ʹ�����ͳ����Ϣ��
 *
 * @return ʹ�����ͳ����Ϣ
 */
const IntentionLockUsage* IntentionLock::getUsage() const {
	return m_usage;
}

}