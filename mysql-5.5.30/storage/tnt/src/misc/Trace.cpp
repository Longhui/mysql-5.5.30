/**
 * ������Ϊ��¼TRACE����
 *
 * @author ��Դ(wangyuan@corp.netease.com, wy@163.org)
 */

#include <iostream>

#include "heap/Heap.h"
#include "heap/FixedLengthRecordHeap.h"
#include "heap/VariableLengthRecordHeap.h"
#include "misc/Session.h"
#include "api/TblMnt.h"
#include "util/Thread.h"
#include "util/System.h"
#include "util/Sync.h"
#include "util/PagePool.h"
#include "util/File.h"
#include "util/Stream.h"
#include "misc/Record.h"
#include "misc/TableDef.h"
#include "misc/Txnlog.h"
#include "misc/Buffer.h"
#include "misc/Trace.h"

#if !defined(WIN32)/* && !defined(__NETWARE__)*/
#include <signal.h>
#endif

using namespace std;
using namespace ntse;

namespace ntse {

/** ȫ��ΨһTRACE���� */
TraceSetting	ts;

#if !defined(WIN32)/* && !defined(__NETWARE__)*/
/** ȫ�������������ݿ�ϵͳԭʼ���õ��źŴ����� */
static struct sigaction m_sysDefaultSA;
#endif

TraceSetting::TraceSetting() {
	memset(this, 0, sizeof(TraceSetting));
	//file = true;
	//rl = true;
	//buf = true;
	//pool = true;
	hp = true;
	//idx = true;
	mms = true;
	//lob = true;
	//ddl = true;
	//mysql = true;
	//dml = true;
	//recv = true;
	log = true;
	//log = true;
}

/**
 * �ر�����trace����
 */
void TraceSetting::closeAll() {
	memset(this, 0, sizeof(TraceSetting));
}

/******************************************************************************
 * TRACE������ʵ��
 *****************************************************************************/
#ifdef NTSE_TRACE
TLS Tracer	tout;
#endif

/// ��Щ����������Ҫ���г�ʼ�����޷�����ΪTrace��ĳ�Ա
static TLS int	gt_funcIndent = 0;		/** ��ǰ�̺߳���Ƕ�ײ�� */
static TLS size_t	gt_bufTail = 0;		/** ��ǰ�̻߳�����βָ�� */
static Mutex	*g_Lock = NULL;			/** ���ƶ�ȫ��TRACE��Ϣ�������ʵ��� */
static FILE		*g_traceFile = NULL;	/** TRACE�ļ� */
static size_t	g_traceFileSize = 0;	/** TRACE�ļ��Ѿ�ʹ�õĴ�С */
static u8		TRACE_TYPE_MASK = 0x80;	/** trace���ͱ�ʶ�����λΪ1��ʾΪftrace������Ϊnftrace */

const char *Tracer::DEFAULT_TRACE_FILE_MARK = "NTSE TRACE";
const char *Tracer::DEFAULT_TRACE_FILE_PREFIX = "ntse.trace";
const size_t Tracer::DEFAULT_TRACE_FILE_SIZE = 100 * 1024 * 1024;	/** Ĭ��Ϊ100M */

FuncReturn::FuncReturn() {
	gt_funcIndent++;
}

FuncReturn::~FuncReturn() {
	gt_funcIndent--;
}

/**
 * ��ʼ��TRACEϵͳ�����û�ж���NTSE_TRACE����ʲôҲ���ɡ�
 *
 */
bool Tracer::init() {
#ifdef NTSE_TRACE
	if (!newOrSwitchTraceFile())
		return false;
	g_Lock = new Mutex("Tracer::g_Lock", __FILE__, __LINE__);
	initSignals();
#endif
	return true;
}

/**
 * �ر�����trace����, ��ntse_trace_dump��������Ҫ�õ�
 */
void Tracer::closeAll() {
#ifdef NTSE_TRACE
	ts.closeAll();
#endif
}


/**
 * ˢ����ǰȫ��TRACE��Ϣ�����е�������Ϣ
 */
void Tracer::flush() {
#ifdef NTSE_TRACE
	MutexGuard guard(g_Lock, __FILE__, __LINE__);
	if (g_traceFile) {
		fflush(g_traceFile);
	}
#endif
}

/**
 * �˳�TRACEϵͳ�����û�ж���NTSE_TRACE����ʲôҲ���ɡ�
 */
void Tracer::exit() {
#ifdef NTSE_TRACE
	if (g_traceFile) {
		fflush(g_traceFile);
		fclose(g_traceFile);
	}
	if (g_Lock) {
		delete g_Lock;
		g_Lock = NULL;
	}
#endif
}

void Tracer::beginFunc(const char *func) {
	// ÿ�ζ��ӻ���0λ�ÿ�ʼд
	gt_bufTail = TRACE_MAX_SIZE_LEN;
	size_t leadingSpaces = caculateLeadingSpace(false);
	// Linux�º���������ϸ���������������Ϣ��ȥ����Щ��Ϣ
#ifndef WIN32
	char buf[256];
	strncpy(buf, func, sizeof(buf) - 1);
	buf[255] = '\0';
	char *start = buf;
	char *ppos = strchr(buf, '(');
	if (ppos) {
		*ppos = '\0';
		start = strstr(buf, "ntse::");
		if (start && start < ppos) {
			while (true) {
				char *nspos = strstr(start + 6, "ntse::");
				if (!nspos || nspos > ppos)
					break;
				start = nspos;
			}
			start += 6;
		} else
			start = buf;
	}
	printBegin(true, getThreadId(), isInternalThread()? "I": "E", leadingSpaces, gt_funcIndent, 0, start);
#else
	if (!strncmp(func, "ntse::", 6))
		func = func + 6;
	printBegin(true, getThreadId(), isInternalThread()? "I": "E", leadingSpaces, gt_funcIndent, 0, func);
#endif
	m_firstItem = true;
}

void Tracer::beginNonFunc(int line) {
	gt_bufTail = TRACE_MAX_SIZE_LEN;
	size_t leadingSpace = caculateLeadingSpace(true);
	printBegin(false, getThreadId(), isInternalThread()? "I": "E", leadingSpace, gt_funcIndent, line);
}


/**
 * ����һ��TRACE��Ϣ
 */
void Tracer::end() {
	// д�������ʶ
	Stream s(m_buffer + gt_bufTail, sizeof(m_buffer) - gt_bufTail);
	s.write((TRACE_DATA_TYPE)TRACE_END);
	gt_bufTail += s.getSize();

	// ��д��trace��ʵ�ʳ���
	Stream sl(m_buffer, TRACE_MAX_SIZE_LEN);
	sl.write((u16)gt_bufTail);

	publish(m_buffer, gt_bufTail);
}

/** ����ǰ�������ո�
 * @post m_leadingSpace�б���ǰ�������ո�
 *
 * @param nonFunc �Ƿ���ftrace
 * @return ������Ҫ���ٸ��ո�
 */
size_t Tracer::caculateLeadingSpace(bool nonFunc) {
	size_t indentSize = (gt_funcIndent <= MAX_INDENT? gt_funcIndent: MAX_INDENT);
	if (nonFunc)
		indentSize++;

	return indentSize * 2;
}

/**
 * �������߳�˽�е�TRACE��Ϣ��ȫ��TRACE��Ϣ��
 *
 * @param msg TRACE��Ϣ
 * @param size  TRACE��Ϣ����
 */
void Tracer::publish(const byte *msg, size_t size) {
	MutexGuard guard(g_Lock, __FILE__, __LINE__);
	fwrite(msg, size, 1, g_traceFile);
	// �������쳣���󲶻���ƴ����ᱣ֤ˢ���������ݣ����ﲻ��Ҫÿ�ζ�fflush
	g_traceFileSize += size;
	switchTraceFileIfNecessary();
}

int Tracer::getThreadId() {
	Thread *th = Thread::currentThread();
	if (th)
		return (int )th->getId();
	return Thread::currentOSThreadID();
}

bool Tracer::isInternalThread() {
	return Thread::currentThread() != NULL;
}

Tracer& Tracer::operator << (const char *s) {
	Stream stream(&m_buffer[gt_bufTail], sizeof(m_buffer) - gt_bufTail);
	stream.write((TRACE_DATA_TYPE)TRACE_CHARS);
	stream.write(s);
	gt_bufTail += stream.getSize();
	return *this;
}

Tracer& Tracer::operator << (char n) {
	Stream s(&m_buffer[gt_bufTail], sizeof(m_buffer) - gt_bufTail);
	s.write((TRACE_DATA_TYPE)TRACE_CHAR);
	s.write(n);
	gt_bufTail += s.getSize();
	return *this;
}

Tracer& Tracer::operator << (byte n) {
	Stream s(&m_buffer[gt_bufTail], sizeof(m_buffer) - gt_bufTail);
	s.write((TRACE_DATA_TYPE)TRACE_BYTE);
	s.write(n);
	gt_bufTail += s.getSize();
	return *this;
}

Tracer& Tracer::operator << (u16 n) {
	Stream s(&m_buffer[gt_bufTail], sizeof(m_buffer) - gt_bufTail);
	s.write((TRACE_DATA_TYPE)TRACE_U16);
	s.write(n);
	gt_bufTail += s.getSize();
	return *this;
}

Tracer& Tracer::operator << (int n) {
	Stream s(&m_buffer[gt_bufTail], sizeof(m_buffer) - gt_bufTail);
	s.write((TRACE_DATA_TYPE)TRACE_INT);
	s.write(n);
	gt_bufTail += s.getSize();
	return *this;
}

Tracer& Tracer::operator << (uint n) {
	Stream s(&m_buffer[gt_bufTail], sizeof(m_buffer) - gt_bufTail);
	s.write((TRACE_DATA_TYPE)TRACE_UINT);
	s.write(n);
	gt_bufTail += s.getSize();
	return *this;
}

Tracer& Tracer::operator << (s64 n) {
	Stream s(&m_buffer[gt_bufTail], sizeof(m_buffer) - gt_bufTail);
	s.write((TRACE_DATA_TYPE)TRACE_S64);
	s.write(n);
	gt_bufTail += s.getSize();
	return *this;
}

Tracer& Tracer::operator << (u64 n) {
	Stream s(&m_buffer[gt_bufTail], sizeof(m_buffer) - gt_bufTail);
	s.write((TRACE_DATA_TYPE)TRACE_U64);
	s.write(n);
	gt_bufTail += s.getSize();
	return *this;
}

Tracer& Tracer::operator << (double n) {
	Stream s(&m_buffer[gt_bufTail], sizeof(m_buffer) - gt_bufTail);
	s.write((TRACE_DATA_TYPE)TRACE_DOUBLE);
	s.write(n);
	gt_bufTail += s.getSize();
	return *this;
}

Tracer& Tracer::operator << (bool n) {
	Stream s(&m_buffer[gt_bufTail], sizeof(m_buffer) - gt_bufTail);
	s.write((TRACE_DATA_TYPE)TRACE_BOOL);
	s.write(n);
	gt_bufTail += s.getSize();
	return *this;
}

Tracer& Tracer::operator << (const void *p) {
	Stream s(&m_buffer[gt_bufTail], sizeof(m_buffer) - gt_bufTail);
	s.write((TRACE_DATA_TYPE)TRACE_VOID);
	s.write((void*)p);
	gt_bufTail += s.getSize();
	return *this;
}

Tracer& Tracer::operator << (const File *file) {
	if (file)
		(*this) << file->getPath();
	else
		(*this) << "null";
	return *this;
}

Tracer& Tracer::operator << (const u16a &arr) {
	Stream s(&m_buffer[gt_bufTail], sizeof(m_buffer) - gt_bufTail);
	s.write((TRACE_DATA_TYPE)TRACE_U16A);
	s.write((u16)arr.m_size);
	s.write((byte*)arr.m_data, sizeof(u16) * arr.m_size);
	gt_bufTail += s.getSize();
	return *this;
}

Tracer& Tracer::operator << (const barr &ba) {
	Stream s(&m_buffer[gt_bufTail], sizeof(m_buffer) - gt_bufTail);

	s.write((TRACE_DATA_TYPE)TRACE_BARR);
	s.write((uint)ba.m_size);
	if (ba.m_size <= BARR_DUMP_SZ) {
		s.write(ba.m_data, ba.m_size);
	} else {
		int edgeSize = BARR_DUMP_SZ / 2;
		s.write(ba.m_data, edgeSize);
		s.write(ba.m_data + ba.m_size - edgeSize, edgeSize);
	}
	gt_bufTail += s.getSize();

	return *this;
}

Tracer& Tracer::operator << (rid n) {
	Stream s(&m_buffer[gt_bufTail], sizeof(m_buffer) - gt_bufTail);
	s.write((TRACE_DATA_TYPE)TRACE_RID);
	s.writeRid(n.m_rid);
	gt_bufTail += s.getSize();
	return *this;
}

Tracer& Tracer::operator << (TraceDataType type) {
	Stream s(&m_buffer[gt_bufTail], sizeof(m_buffer) - gt_bufTail);
	s.write((TRACE_DATA_TYPE)type);
	gt_bufTail += s.getSize();
	return *this;
}

/**
 * ���������л��µ�trace�ļ��������ǰ�Ѿ�����һ���ļ�������Ҫ�ȹر����е�trace�ļ�
 * @pre �Ѿ���ȫ�ֻ���������ͬ��
 * @return true��ʾ���ļ������ɹ���false��ʾʧ��
 */
bool Tracer::newOrSwitchTraceFile() {
	if (g_traceFile) {
		fflush(g_traceFile);
		fclose(g_traceFile);
	}
	
	time_t now;
	time(&now);
	char traceFile[255];
	sprintf(traceFile, "%s.%lu", DEFAULT_TRACE_FILE_PREFIX, now);

	g_traceFile = fopen(traceFile, "wb");

	// д��NTSE TRACE�����б�־��Ϣ
	g_traceFileSize = fwrite(DEFAULT_TRACE_FILE_MARK, 1, strlen(DEFAULT_TRACE_FILE_MARK), g_traceFile);

	return (g_traceFile != NULL);
}

/**
 * �����Ҫ������£���trace�ļ���С����ָ������DEFAULT_TRACE_FILE_SIZE����һ���µ��ļ�дtrace
 * @pre �Ѿ���ȫ�ֻ���������ͬ��
 */
void Tracer::switchTraceFileIfNecessary() {
	if (g_traceFileSize >= DEFAULT_TRACE_FILE_SIZE) {
		newOrSwitchTraceFile();
		g_traceFileSize = 0;
	}
}

/**
 * ���������ʱ����Ҫ���øú����������������ڴ浱�е�trace
 */
void Tracer::segfaultHandler( int sig ) {
	UNREFERENCED_PARAMETER(sig);

	{	// �ȱ�֤��ĿǰΪֹ��trace���Ѿ�д��������Ϊ�˱�֤����д�ҵ�
		MutexGuard guard(g_Lock, __FILE__, __LINE__);
		fflush(g_traceFile);
	}

	// ����Ҫ����Tracer::exit()����Ϊ�������쳣�˳���������Tracer::exit()�ᵼ��g_Lock����ɾ����
	// ����������ִ�е��̻߳��г�ͻ��ͬʱ����涨�������ʱ��ֻˢ������ǰΪֹ�Ļ���trace��

#if !defined(WIN32)/* && !defined(__NETWARE__)*/
	// ���������ϵͳ��ʼ����Ĵ�����,������Ҫ�ص���ʼ������
	if (m_sysDefaultSA.sa_handler)
		m_sysDefaultSA.sa_handler(sig);
#endif
}

/** ע���źŴ�����
 */
void Tracer::initSignals() {
#if !defined(WIN32)/* && !defined(__NETWARE__)*/
#ifndef SA_RESETHAND
#define SA_RESETHAND 0
#endif
#ifndef SA_NODEFER
#define SA_NODEFER 0
#endif

#ifndef EMBEDDED_LIBRARY
	// ���ȱ���ϵͳԭ�е��źŴ�����
	sigaction(SIGSEGV, NULL, &m_sysDefaultSA);

	//int set;
	struct sigaction sa;

	//my_sigset(THR_SERVER_ALARM,print_signal_warning); // Should never be called!

	sa.sa_flags = SA_RESETHAND | SA_NODEFER;
	sigemptyset(&sa.sa_mask);
	sigprocmask(SIG_SETMASK,&sa.sa_mask,NULL);

//#ifdef HAVE_STACKTRACE
//	my_init_stacktrace();
//#endif

#if defined(__amiga__)
	sa.sa_handler = (void(*)())segfaultHandler;
#else
	sa.sa_handler = segfaultHandler;
#endif

	sigaction(SIGSEGV, &sa, NULL);
	sigaction(SIGABRT, &sa, NULL);
#ifdef SIGBUS
	sigaction(SIGBUS, &sa, NULL);
#endif
	sigaction(SIGILL, &sa, NULL);
	sigaction(SIGFPE, &sa, NULL);

#endif /*!EMBEDDED_LIBRARY*/
#endif	/* WIN32 */
}

/**
 * ��ӡһ��trace����ʼ��Ϣ
 * @param fTrace		true��ʾftrace��false��ʾnftrace
 * @param threadId		�̺߳�
 * @param thdType		�ڲ��̻߳����ⲿ�̱߳�ʶ
 * @param leadingSpace	��ʼ�ո���
 * @param fIdent		������ʶ
 * @param line			дtrace�ļ����к�
 * @param func			��������Ϣ��Ĭ��ΪNULL��ֻ��fTraceΪtrueʱ����
 */
void Tracer::printBegin(bool fTrace, u64 threadId, const char *thdType, size_t leadingSpaces, int fIdent, int line/* = 0*/, const char *func/* = NULL*/) {
	Stream s(&m_buffer[gt_bufTail], sizeof(m_buffer) - gt_bufTail);

	s.write((TRACE_DATA_TYPE)TRACE_START);

	s.write(threadId);
	s.write(thdType);

	assert(leadingSpaces < (size_t)(TRACE_TYPE_MASK - 1));
	u8 typeNSpaces = fTrace ? (TRACE_TYPE_MASK | (u8)leadingSpaces) : (~TRACE_TYPE_MASK & (u8)leadingSpaces);
	s.write(typeNSpaces);

	s.write(fIdent);
	if (!fTrace)
		s.write(line);

	if (fTrace)
		s.write(func);

	gt_bufTail += s.getSize();
}

/** ��ʼ��¼һ�����ӽṹ��Ϣ
 * @param name	�ṹ����
 */
void Tracer::beginComplex(const char *name) {
	*this << TRACE_COMPLEX << name;
}

/** ����һ�����ӽṹ��¼
 */
void Tracer::endComplex() {
	*this << TRACE_COMPLEX_END;
}

/**
 * �����¼��TRACE��
 * @param tracer TRACE �����
 * @param r ��¼
 * @return ���ز���tracer���Ա㼶������
 */
Tracer& operator << (Tracer& tracer, const t_rec& r) {
	return tracer << r.m_record;
}
/**
 * ����Ӽ�¼��TRACE��
 * @param tracer TRACE �����
 * @param sr ���ּ�¼
 * @return ���ز���tracer���Ա㼶������
 */
Tracer& operator << (Tracer& tracer, const t_srec& sr) {
	tracer.beginComplex("T_SREC");

	if (sr.m_subRec) {
		const SubRecord *subRecord = sr.m_subRec;
		const TableDef *tableDef = sr.m_tableDef;

		tracer << "Addr:" << (void*)subRecord
			<< "Rid:" << rid(subRecord->m_rowId)
			<< "Fmt:" << subRecord->m_format
			<< "Size:" << subRecord->m_size;
		// TODO �����������ʹ���
		if (subRecord->m_format == REC_MYSQL || subRecord->m_format == REC_REDUNDANT || subRecord->m_format == REC_UPPMYSQL) {
			for (int i = 0; i < subRecord->m_numCols; i++) {
				u16 cno = subRecord->m_columns[i];
				ColumnDef *col = tableDef->m_columns[cno];
				tracer << "Name:" << col->m_name;
				if (RecordOper::isNullR(tableDef, subRecord, cno))
					tracer << "Data:" << "\\N";
				else {
					tracer << "Data:" << barr(col->m_size, subRecord->m_data + col->m_offset);
				}
			}
		} else {
			tracer	<< "Cols:" << u16a(subRecord->m_numCols, subRecord->m_columns)
				<< "Data:" << barr(subRecord->m_size, subRecord->m_data);
		}
	} else {
		tracer << "Addr:" << (void*)0;
	}

	tracer.endComplex();

	return tracer;
}

// TODO: ��Ӧ�õ�����������������Ӧʹ��t_rec��t_srec����ӡRecord/SubRecord
Tracer& operator << (Tracer& tracer, const Record *r) {
	tracer.beginComplex("Record");

	if (r) {
		tracer << "Addr:" << (void*)r
			<< "Rid:" << rid(r->m_rowId)
			<< "Fmt:" << r->m_format
			<< "Size:" << r->m_size
			<< "Data:" << barr(r->m_size, r->m_data);
	} else {
		tracer << "Addr:" << (void*)0;
	}

	tracer.endComplex();

	//	assert_always(false);
	return tracer;
}

Tracer& operator << (Tracer& tracer, const SubRecord *sr) {
	tracer.beginComplex("SubRecord");

	if (sr) {
		tracer << "Addr:" << (void*)sr
			<< "Rid:" << rid(sr->m_rowId)
			<< "Fmt:" << sr->m_format
			<< "Size" << sr->m_size
			<< "Cols:" << u16a(sr->m_numCols, sr->m_columns)
			<< "Data:" << barr(sr->m_size, sr->m_data);
	} else {
		tracer << "Addr:" << (void*)0;
	}

	tracer.endComplex();
	return tracer;
}


/** д��LogEntry��Ϣ��TRACE����
 * @param tracer	ָ����trace
 * @param logEntry	��־��Ŀ����
 * @return ����trace���ڼ�������
 */
Tracer& operator << (Tracer &tracer, const LogEntry *logEntry) {
	tracer.beginComplex("LogEntry");

	if (logEntry) {
		tracer << "Addr:" << (void*)logEntry
			<< "TxnID:" << logEntry->m_txnId
			<< "TblID:" << logEntry->m_tableId 
			<< "LogType:" << logEntry->m_logType
			<< "Size:" << (int)logEntry->m_size
			<< "CpstLSN:" << logEntry->m_cpstForLsn;
	} else {
		tracer << "Addr:" << (void*)0;
	}

	tracer.endComplex();
	return tracer;
}

/**
 * �������ҳ����Ϣ��TRACE��
 *
 * @param tracer TRACE��
 * @param bcb ҳ����ƿ�
 * @return ����tracer���ڼ�������
 */
Tracer& operator << (Tracer &tracer, const Bcb *bcb) {
	tracer.beginComplex("Bcb");

	if (bcb) {
		tracer << "Addr:" << (void*)bcb
			<< "File:" << bcb->m_pageKey.m_file
			<< "PageID:" << bcb->m_pageKey.m_pageId
			<< "Dirty:" << bcb->m_dirty
			<< "PinCnt:" << bcb->m_pinCount
			<< "Page:" << bcb->m_page;
	} else {
		tracer << (void *)0;
	}

	tracer.endComplex();
	return tracer;
}


Tracer& operator << (Tracer& tracer, const HeapHeaderPageInfo *headerPage) {
	tracer.beginComplex("HeapHeaderPageInfo");

	if (headerPage) {
		tracer << "Addr:" << (void*)headerPage;
		switch (headerPage->m_version) {
			case HEAP_VERSION_FLR:
				tracer << "Ver:" << "FLR heap: ";
				break;
			case HEAP_VERSION_VLR:
				tracer << "Ver:" << "VLR heap: ";
				break;
		}
		tracer << "MaxUsed:" << headerPage->m_maxUsed 
			<< "PageNum:" << headerPage->m_pageNum;
	}
	else
		tracer << "Addr:" << (void *)0;

	tracer.endComplex();
	return tracer;
}

Tracer& operator << (Tracer& tracer, FixedLengthRecordHeap *heap) {
	tracer.beginComplex("Heap");

	if (heap) {
		tracer << "Addr:" << (void*)heap;

		tracer << "FLR Heap:" << true
			<< "MaxUsedPageNum:" << heap->m_maxUsedPageNum
			<< "MaxPageNum:" << heap->m_maxPageNum
			<< "SlotLen" << heap->m_slotLength;
	}
	else
		tracer << "Addr:" << (void *)0;

	tracer.endComplex();
	return tracer;
}

Tracer& operator << (Tracer& tracer, VariableLengthRecordHeap *heap) {
	tracer.beginComplex("Heap");

	if (heap) {
		tracer << "Addr:" << (void*)heap;

		tracer << "VLR Heap:" << false
			<< "MaxUsedPageNum:" << heap->m_maxUsedPageNum
			<< "MaxPageNum:" << heap->m_maxPageNum
			<< "PctFree:" << heap->m_pctFree;
	}
	else
		tracer << "Addr:" << (void *)0;

	tracer.endComplex();
	return tracer;
}



/**
 * ����Ự��Ϣ��TRACE��
 *
 * @param tracer TRACE�����
 * @param session �Ự
 * @return ����tracer���ڼ�������
 */
Tracer& operator << (Tracer& tracer, const Session *session) {
	if (session)
		tracer << session->getId();
	else
		tracer << (void *)0;
	return tracer;
}


Tracer& operator << (Tracer& tracer, const IndexScanCond *cond) {
	tracer.beginComplex("IndexScanCond");

	if (cond) {
		tracer << "Addr:" << (void*)cond
			<< "Idx:" << cond->m_idx
			<< "Key:" << cond->m_key
			<< "Forward:" << cond->m_forward
			<< "includeKey:" << cond->m_includeKey
			<< "singleFetch:" << cond->m_singleFetch;
	} else {
		tracer << "Addr:" << (void*)0;
	}

	tracer.endComplex();
	return tracer;
}


Tracer& operator << (Tracer& tracer, const TblScan *scan) {
	tracer.beginComplex("TblScan");

	if (scan) {
		tracer << "Addr:" << (void*)scan
			<< "Type:" << scan->m_type
			<< "OpType:" << scan->m_opType
			<< "Rid:" << rid(scan->m_redRow->m_rowId);
	} else {
		tracer << "Addr:" << (void*)0;
	}

	tracer.endComplex();
	return tracer;
}


Tracer& operator << (Tracer& tracer, BitmapPage *bmp) {
	tracer.beginComplex("BitmapPage");

	if (bmp) {
		tracer << "Addr:" << (void*)bmp
			<< "PageCount00:" << bmp->u.m_header.m_pageCount[0]
		<< "PageCount01:" << bmp->u.m_header.m_pageCount[1]
		<< "PageCount10:" << bmp->u.m_header.m_pageCount[2];
	} else {
		tracer << (void *)0;
	}

	tracer.endComplex();
	return tracer;
}


/**
 * ���һϵ�д�����ͬһ�ļ���ҳ���б���Ϣ��TRACE��
 *
 * @param tracer TRACE��
 * @param list ������ͬһ�ļ���ҳ���б���Ϣ
 * @return ����tracer���ڼ�������
 */
Tracer& operator << (Tracer &tracer, const std::vector<Bcb *> *list) {
	tracer.beginComplex("BcbList");

	if (list) {
		tracer << "Addr:" << (void*)list
			<< "Size:" << (uint)list->size();
		if (!list->empty()) {
			tracer << "File:" << list->at(0)->m_pageKey.m_file
				<< "PageId:" << list->at(0)->m_pageKey.m_pageId;
			for (size_t i = 1; i < list->size(); i++) {
				assert(list->at(i)->m_pageKey.m_file == list->at(0)->m_pageKey.m_file);
				tracer << "PageId:" << list->at(i)->m_pageKey.m_pageId;
			}
		}
	} else {
		tracer << (void *)0;
	}

	tracer.endComplex();
	return tracer;
}

Tracer& operator << (Tracer& tracer, PageType type) {
	Stream s(&tracer.m_buffer[gt_bufTail], sizeof(tracer.m_buffer) - gt_bufTail);
	s.write((TRACE_DATA_TYPE)TRACE_ENUM_PAGETYPE);
	s.write((u8)type);
	gt_bufTail += s.getSize();
	return tracer;
}


Tracer& operator << (Tracer& tracer, ScanType type) {
	Stream s(&tracer.m_buffer[gt_bufTail], sizeof(tracer.m_buffer) - gt_bufTail);
	s.write((TRACE_DATA_TYPE)TRACE_ENUM_SCANTYPE);
	s.write((u8)type);
	gt_bufTail += s.getSize();
	return tracer;
}

Tracer& Tracer::operator << (LockMode lockMode) {
	Stream s(&m_buffer[gt_bufTail], sizeof(m_buffer) - gt_bufTail);
	s.write((TRACE_DATA_TYPE)TRACE_ENUM_LOCKMODE);
	s.write((u8)lockMode);
	gt_bufTail += s.getSize();
	return *this;
}

Tracer& operator << (Tracer& tracer, RecFormat format) {
	Stream s(&tracer.m_buffer[gt_bufTail], sizeof(tracer.m_buffer) - gt_bufTail);
	s.write((TRACE_DATA_TYPE)TRACE_ENUM_FORMAT);
	s.write((u8)format);
	gt_bufTail += s.getSize();
	return tracer;
}


Tracer& operator << (Tracer& tracer, LogType type) {
	Stream s(&tracer.m_buffer[gt_bufTail], sizeof(tracer.m_buffer) - gt_bufTail);
	s.write((TRACE_DATA_TYPE)TRACE_ENUM_LOGTYPE);
	s.write((u8)type);
	gt_bufTail += s.getSize();
	return tracer;
}


Tracer& operator << (Tracer& tracer, OpType type) {
	Stream s(&tracer.m_buffer[gt_bufTail], sizeof(tracer.m_buffer) - gt_bufTail);
	s.write((TRACE_DATA_TYPE)TRACE_ENUM_OPTYPE);
	s.write((u8)type);
	gt_bufTail += s.getSize();
	return tracer;
}


/** ������trace�������캯��
 * @param trace	Ҫ������trace��ʼ��ַ
 * @param tSize	Ҫ������trace����󳤶�
 * @param result	��Ž�������Ļ���
 * @param size		�����С
 * @param filter	����������
 */
TraceParser::TraceParser( byte *trace, size_t tSize, byte *result, size_t size, ParseFilter *filter) {
	m_trace = trace;
	m_tSize = tSize;
	m_bOffset = 0;
	m_buffer = result;
	m_bSize = size;
	m_ftrace = false;
	m_filter = filter;
	m_traceRealSize = 0;
}

/** �ж϶�����trace�ǲ����������ǲ�����ȷ
 * @return �����ǰtrace�����ݳ���С��TRACE_MAX_SIZE_LEN�������ݳ���С�ڶ�ȡ������trace���ȷ���TPR_TRACE_UNCOMPLETE
 *�������ǰtrace���ȶ�ȡ���������˻����С�����ڼ����˻����㹻�󣬸�����ǲ�����ģ�����TPR_PARSE_ERROR
 */
TraceParseResult TraceParser::check() {
	// ����������trace��ǰTRACE_MAX_SIZE_LEN���ֽڱ�ʾ��trace�ĳ���
	if (m_tSize < TRACE_MAX_SIZE_LEN)
		return TPR_TRACE_INCOMPLETE;

	size_t size = getTraceLength();

	if (size > m_bSize) {
		printf("Parse trace error because of trace length is larger than buffer size.");
		return TPR_PARSE_ERROR;
	}
	
	if (size > m_tSize)
		return TPR_TRACE_INCOMPLETE;
	
	return TPR_SUCCESS;
}


/** ����ָ���Ķ�����trace�����õ�һ��trace��Ϣ
 * @return ���ݽ����������TraceParseResult��һ��
 */
TraceParseResult TraceParser::parse() {
	TraceParseResult result = check();
	if (result != TPR_SUCCESS)
		return result;

	m_bOffset = 0;	// ��֤�����ε���parse���������ܵõ���ͬ�Ľ��
	Stream s(m_trace, m_tSize);

	// ��ȡtrace����������������Ϣ
	s.skip(TRACE_MAX_SIZE_LEN);

	TraceParseResult tpr;
	if ((tpr = parseTraceHeader(&s)) != TPR_SUCCESS)
		return tpr;

	if ((tpr = parseTraceBody(&s)) != TPR_SUCCESS)
		return tpr;

	if ((tpr = parseTraceTail(&s)) != TPR_SUCCESS)
		return tpr;

	m_bOffset = s.getSize();

	return TPR_SUCCESS;
}

/** ����trace����ʼ��Ϣ
 * @param s	trace������
 * @return ����TraceParseResult�ĸ��ֽ��
 */
TraceParseResult TraceParser::parseTraceHeader( Stream *s ) {
	TRACE_DATA_TYPE type;
	s->read(&type);
	if (type != TRACE_START) {
		printf("Parse trace header failed, expect TRACE_START, but %d is read.", type);
		return TPR_PARSE_ERROR;
	}

	u64 threadId;
	s->read(&threadId);

	char *thdType;
	s->readString(&thdType);

	u8 typeNSpaces;
	s->read(&typeNSpaces);
	bool ftrace = ((TRACE_TYPE_MASK & typeNSpaces) != 0);
	m_ftrace = ftrace;
	size_t leadingSpaces = (~TRACE_TYPE_MASK & typeNSpaces);

	int fIndent;
	s->read(&fIndent);

	int line = 0;
	if (!ftrace) {
		s->read(&line);
	}

	char *func = NULL;
	if (ftrace)
		s->readString(&func);

	// �жϸ�trace�Ƿ�����Թ�
	if (m_filter && !m_filter->TraceHeaderFilter(ftrace, threadId, thdType, fIndent, func))
		return TPR_SKIPPED;

	char *blanks = (char*)alloca(leadingSpaces + 1);
	memset(blanks, ' ', leadingSpaces);
	blanks[leadingSpaces] = '\0';

	if (ftrace) {
		writeParsed("%d%s%s[%d]:%s(", (int)threadId, thdType, blanks, fIndent, func);
	} else {
		writeParsed("%ld%s%s[I%d/L%d]:", (int)threadId, thdType, blanks, fIndent, line);
	}

	delete [] func;

	return TPR_SUCCESS;
}

/** ����trace��β����Ϣ, ����֮��m_bOffset��ʾ���ǵ�ǰ����ʹ�ó���
 * @param s	trace������
 * @return ����TPR_SUCCESS
 */
TraceParseResult TraceParser::parseTraceTail( Stream *s ) {
	UNREFERENCED_PARAMETER(s);
	if (m_ftrace)
		writeParsed(")");

	writeParsed("%c", '\0');
	return TPR_SUCCESS;
}

/**
 * ����ָ��������trace�����е�trace��Ϣ
 * @param s	������trace���ڵ���
 * @return ����TraceParseResult�ĸ�������
 */
TraceParseResult TraceParser::parseTraceBody( Stream *s ) {
	bool parsed = false;
	while (true) {
		TRACE_DATA_TYPE type;
		s->read(&type);

		if (type != TRACE_END) {
			// �����Ѿ�parse��һ�����ݵ�ftrace���,��Ҫ�������һ������
			if (parsed && m_ftrace)
				writeGap();

			if (parseOneDataType(s, (TraceDataType)type) == TPR_PARSE_ERROR)
				return TPR_PARSE_ERROR;

			parsed = true;
		} else
			break;
	}

	return TPR_SUCCESS;
}


/** ���������ݵ��е�trace��Ϣ��ֻ������������һ�����ͣ������complex���ͣ����������complex�ṹ
 * @param s	���trace��Ϣ����
 * @return ����TraceParseResult�ĸ�������
 */
TraceParseResult TraceParser::parseComplexType( Stream *s ) {
	// ���ȶ�ȡCOMPLEX�ṹ��������Ϣ
	// �Թ�COMPLEX�ṹ�������ַ�����������Ϣ
	TRACE_DATA_TYPE type;
	s->read(&type);
	assert(type == TRACE_CHARS);
	char *name = s->readString();
	writeParsed(" %s[", name);
	delete [] name;

	bool firstItem = true;
	while (true) {
		s->read(&type);
		if (type == TRACE_COMPLEX_END) {
			// ���ӽṹ�������
			writeParsed("]");
			break;
		}
		// һ����"AttrName:" + Value�ĸ�ʽ
		assert(type == TRACE_CHARS);
		// ����ÿ����ֵ�Լ���ķֺ�
		if (!firstItem)
			writeParsed("; ");

		// ����AttrName��Ϣ
		if (parseOneDataType(s, (TraceDataType)type) == TPR_PARSE_ERROR)
			return TPR_PARSE_ERROR;
		// ����Value��Ϣ
		s->read(&type);
		assert(type != TRACE_START && type != TRACE_END);
		if (parseOneDataType(s, (TraceDataType)type) == TPR_PARSE_ERROR)
			return TPR_PARSE_ERROR;

		firstItem = false;
	}

	return TPR_SUCCESS;
}

/** ����һ��ָ����Trace����������Ϣ
 * @param s		trace���ڵ���
 * @param type	trace��������
 * @return ����TraceParseResult�ĸ�������
 */
TraceParseResult TraceParser::parseOneDataType( Stream *s, TraceDataType type ) {
	switch (type) {
		case TRACE_BOOL:
			{
				bool tf;
				s->read(&tf);
				writeParsed("%s", tf  ? "true" : "false");
			}
			break;
		case TRACE_CHAR:
		case TRACE_BYTE:
			{
				u8 c;
				s->read(&c);
				writeParsed("%d", c);
			}
			break;
		case TRACE_CHARS:
		case TRACE_FILE:
			{
				char *str =	s->readString();
				writeParsed("%s", str);
				delete [] str;
			}
			break;
		case TRACE_DOUBLE:
			{
				double d;
				s->read(&d);
				writeParsed("%f", d);
			}
			break;
		case TRACE_U16:
			{
				u16 i;
				s->read(&i);
				writeParsed("%d", i);
			}
			break;
		case TRACE_INT:
			{
				int i;
				s->read(&i);
				writeParsed("%d", i);
			}
			break;
		case TRACE_UINT:
			{
				uint i;
				s->read(&i);
				writeParsed("%u", i);
			}
			break;
		case TRACE_S64:
			{
				s64 i;
				s->read(&i);
				writeParsed(I64FORMAT"d", i);
			}
			break;
		case TRACE_U64:
			{
				u64 i;
				s->read(&i);
				writeParsed(I64FORMAT"u", i);
			}
			break;
		case TRACE_VOID:
			{
				void *p = NULL;
				s->readPtr(&p);
				writeParsed("%p", p);
			}
			break;
		case TRACE_ENUM_LOCKMODE:
			{
				u8 mode;
				s->read(&mode);
				writeParsed("%s", RWLock::getModeStr((LockMode)mode));
			}
			break;
		case TRACE_ENUM_FORMAT:
			{
				u8 format;
				s->read(&format);
				writeParsed("%s", getRecFormatStr((RecFormat)format));
			}
			break;
		case TRACE_ENUM_LOGTYPE:
			{
				u8 type;
				s->read(&type);
				writeParsed("%s", Txnlog::getLogTypeStr((LogType)type));
			}
			break;
		case TRACE_ENUM_OPTYPE:
			{
				u8 type;
				s->read(&type);
				writeParsed("%s", getOpTypeStr((OpType)type));
			}
			break;
		case TRACE_ENUM_SCANTYPE:
			{
				u8 type;
				s->read(&type);
				writeParsed("%s", getScanTypeStr((ScanType)type));
			}
			break;
		case TRACE_ENUM_PAGETYPE:
			{
				u8 type;
				s->read(&type);
				writeParsed("%s", getPageTypeStr((PageType)type));
			}
			break;
		case TRACE_RID:
			{
				RowId rid;
				s->readRid(&rid);
				writeParsed(I64FORMAT"u", rid);
			}
			break;
		case TRACE_U16A:
			parseU16Array(s);
			break;
		case TRACE_BARR:
			parseByteArray(s);
			break;
		case TRACE_COMPLEX:
			if (parseComplexType(s) == TPR_PARSE_ERROR)
				return TPR_PARSE_ERROR;
			break;
		default:
			// ��Ӧ�ö���TRACE_START/TRACE_END/TRACE_COMPLEX_END�ı��
			printf("Parse trace failed, unexpected token meet : %c\n", type);
			return TPR_PARSE_ERROR;
	}

	return TPR_SUCCESS;
}

/** ����U16������trace���������д�뵱ǰ�������
 * @param s	trace������
 */
void TraceParser::parseU16Array( Stream * s ) {
	u16 cols;
	s->read(&cols);
	u16 columns[Limits::MAX_COL_NUM];
	s->readBytes((byte*)columns, cols * sizeof(u16));
	for (uint i = 0; i < cols; i++)
		writeParsed("%d ", columns[i]);
}


/** �����ֽ���������trace
 * @param s		trace������
 */
void TraceParser::parseByteArray( Stream * s ) {
	uint totalSize;
	size_t size;
	s->read(&totalSize);
	bool omitted = totalSize > Tracer::BARR_DUMP_SZ;
	if (omitted) {
		size = Tracer::BARR_DUMP_SZ;
	} else
		size = totalSize;

	byte data[Tracer::BARR_DUMP_SZ];
	char output[Tracer::BARR_DUMP_SZ * 3 + 30];
	s->readBytes(data, size);

	if (omitted) {
		int edgeSize = Tracer::BARR_DUMP_SZ / 2;
		char *ptr = getHexString(data, edgeSize, output);
		ptr += sprintf(ptr, "...(Omit %d bytes)", (uint)(totalSize - Tracer::BARR_DUMP_SZ));
		ptr = getHexString(data + edgeSize, edgeSize, ptr);
		*ptr = '\0';
		writeParsed("%s", output);
	} else {
		char *ptr = getHexString(data, size, output);
		*ptr = '\0';
		data[size] = '\0';
		writeParsed("%s", output);
	}
}



/** д��������trace��Ϣ������
 * д��֮��m_bOffset����Ӧ���޸�Ϊ��ǰд��λ��
 * @param fmt	trace��Ϣ��ʽ
 * @param ...	��Ϣ���ݲ����б�
 */
void TraceParser::writeParsed( const char *fmt, ... ) {
	va_list va;
	va_start(va, fmt);

	int	written = System::vsnprintf((char*)m_buffer + m_bOffset, m_bSize - m_bOffset, fmt, va);
	m_bOffset += written;
	assert(m_bOffset < m_bSize);

	va_end(va);
}


char* TraceParser::getHexString(const byte *ba, size_t size, char *str) {
	char *ptr = str;
	for (size_t i = 0; i < size; i++) {
		byte b = ba[i];
		if ((b >= 'a' && b <= 'z') || (b >= 'A' && b <= 'Z') || (b >= '0' && b <= '9'))
			*ptr++ = (char)b;
		else if (b == '\n') {
			*ptr++ = '\\';
			*ptr++ = 'n';
		} else if (b == '\r') {
			*ptr++ = '\\';
			*ptr++ = 'r';
		} else if (b == '\t') {
			*ptr++ = '\\';
			*ptr++ = 't';
		} else if (b == '\b') {
			*ptr++ = '\\';
			*ptr++ = 'b';
		} else {
			*ptr++ = '\\';
			*ptr++ = getHexChar(b >> 4);
			*ptr++ = getHexChar(b & 0x0F);
		}
	}
	return ptr;
}

static char HEX_MAP[16] = {'0','1','2','3','4','5','6','7','8','9','A','B','C','D','E','F'};

char TraceParser::getHexChar(int v) {
	assert(v >= 0 && v <= 15);
	return HEX_MAP[v];
}

/** �õ���ǰtrace�ĳ���
 * @return trace����
 */
size_t TraceParser::getTraceLength() {
	if (m_traceRealSize == 0) {
		Stream s(m_trace, m_tSize);
		u16 realSize;
		s.read(&realSize);
		m_traceRealSize = realSize;
	}
	return m_traceRealSize;
}

/** �ڵ�ǰ���ݻ��洦дһ��", " */
void TraceParser::writeGap() {
	char *curr = (char*)m_buffer + m_bOffset;
	*curr++ = ',';
	*curr++ = ' ';
	m_bOffset += 2;
}

/** ����һ���µ�trace���ݹ�����
 * ʹ������ӿ���Ҫע����ǣ��������治�ı䣬���parse֮���������ԭ�е����ݻᱻ���
 * @param trace	�µ�trace����
 * @param tSize	trace���ݵ���󳤶�
 */
void TraceParser::resetTrace( byte *trace, size_t tSize ) {
	m_trace = trace;
	m_tSize = tSize;
	m_traceRealSize = 0;
}

}
