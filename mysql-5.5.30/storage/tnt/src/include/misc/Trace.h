/**
 * ������Ϊ��¼TRACE����
 *
 * @author ��Դ(wangyuan@corp.netease.com, wy@163.org)
 */

#ifndef _NTSE_TRACE_H_
#define _NTSE_TRACE_H_

#include "util/Portable.h"
#include "util/Sync.h"
#include "misc/Global.h"
#include <vector>

/**
== TRACE������ʹ�� ==

TRACE�Ǽ�¼������Ϊ�����������Եļ��������ļ�ʵ��NTSE�е�TRACE��ܡ�
��һ�����Ҫ�ṩ��������ܡ�

1. ftrace/nftrace: ftrace/nftraceΪ�������TRACE��Ϣ�ĺ꣬�ڶ�����NTSE_TRACE
   ���б���ʱ��Щ�걻��չΪ�ж�ָ����ģ���Ƿ���TRACE����������ִ��ָ��
   ��TRACE����һϵ�в����������Զ��������������/�кŵȹ��ܡ���û�ж���
   NTSE_TRACE���б���ʱnstrace�궨��Ϊ�գ�
2. ts: ts��ȫ��TRACE���ò�����Ϊһ��TraceSetting�ṹ���ʵ��������ʵ�ֶ�̬
   �򿪻�رո���ģ���TRACE���ܣ�
3. tout: tout������cout����ר�������TRACE��Ϣ�����������ʹ����<<������
   �������Ͱ�ȫ�ĸ�ʽ�������

һ�������ʹ�ñ����ʵ��TRACE���÷����£�

{{{
ftrace(ts.aspect, tout << xxx << yyy;);
}}}
��
{{{
nftrace(ts.aspect, tout << xxx << yyy;);
}}}

����aspectΪTraceSetting�ṹ��ĳ����Ա������ָ��TRACE��Ϣ���������

ftrace��ר���������������ʱ��TRACE��Ϣ���Զ���������������Ӻ�������
�����Ϣ������Ĭ������»��Զ��ö��ŷָ��������õĸ���������
{{{
ftrace(ts.aspect, tout << xxx << yyy;);
}}}
�����TRACE��Ϣ��ʽ���£�
{{{
�߳�ID[�������ò��]:������(xxx,yyy)
}}}
ͨ���������ò�Σ����ԶԺ������ý���������������ʾ��

nftrace����������Ǻ�������TRACE��Ϣ���Զ�����кţ������Ӻ������ò����Ϣ��
Ҳ�����Զ��ö��ŷָ��������ݡ�
{{{
nftrace(ts.aspect, tout << xxx << yyy;);
}}}
�����TRACE��Ϣ��ʽ���£�
{{{
�߳�ID[L�к�]:xxxyyy
}}}

== TRACEϵͳ�ĳ�ʼ�������� ==
ʹ��TRACE�������κ�TRACE��Ϣ֮ǰ�������Tracer::init()�������г�ʼ����
�����˳�ǰ�����Tracer::exit()������������

== ������tout�ڽ���֧�ֵ��������� ==

tout�ڽ��ĸ�ʽ��������ܰ�������ֵ���ַ����ȳ������͵�֧�֣����ĳ������
����toutĬ�ϲ�֧�֣������ͨ���ṩ���µĺ�����չ�书�ܣ�

{{{
Tracer& operator << (Tracer &tracer, T t);
}}}

����TΪ�������͡�

����Table.h�е�ScanIntentionΪһ��ö�����ͣ���TRACEʱ��Ҫ�������Ϊ�ַ�����
����Table.h�ж�����һ��������

{{{
Tracer& operator << (Tracer &tracer, ScanIntention intention);
}}}

Ȼ����Table.cpp������ʵ����һ������

{{{
Tracer& operator << (Tracer& tracer, ScanIntention intention) {
	if (intention == SI_READ)
		tracer << "READ";
	else if (intention == SI_UPDATE)
		tracer << "UPDATE";
	else {
		assert(intention == SI_DELETE);
		tracer << "DELETE";
	}
	return tracer;
}
}}}

*/

namespace ntse {
class FixedLengthRecordHeap;
class VariableLengthRecordHeap;

/** ����TRACE�Ƿ�򿪵����� */
struct TraceSetting {
public:
	TraceSetting();
	void closeAll();

public:
	bool	file;		/** �Ƿ��¼�ļ����� */
	bool	rl;			/** �Ƿ��¼�������� */
	bool	irl;		/** �Ƿ��¼����ҳ�������� */
	bool	pool;		/** �Ƿ��¼�ڴ�ҳ�ز��� */
	bool	buf;		/** �Ƿ��¼ҳ�滺����� */
	bool	hp;			/** �Ƿ��¼�Ѳ��� */
	bool	idx;		/** �Ƿ��¼�������� */
	bool	mms;		/** �Ƿ��¼MMS���� */
	bool	lob;		/** �Ƿ��¼�������� */
	bool	ddl;		/** �Ƿ��¼API��DDL���� */
	bool	dml;		/** �Ƿ��¼API��DML���� */
	bool	recv;		/** �Ƿ��¼API��ָ����� */
	bool	mysql;		/** �Ƿ��¼MySQL�ӿڲ���� */
	bool	intg;		/** �Ƿ��¼���ɲ��Ե�����Ϣ���� */
	bool	log;		/** �Ƿ��¼��־ģ����� */
	bool    mIdx;       /** �Ƿ��¼�ڴ��������� */
};

extern TraceSetting	ts;

/** ����C++��������ʱ�Զ������ֲ������Ļ����ں�������ʱ����TRACE�����
 * �ṹ��
 */
struct FuncReturn {
	FuncReturn();
	~FuncReturn();
};

#ifdef NTSE_TRACE
/** ���������������TRACE��Ϣ�ĺ꣬�Զ��������������¼�������ò����Ϣ��
 * �����Զ��ö��ŷָ��������õĸ�������
 *
 * @param aspect ����ts.memeber��ָ��TRACE��Ϣ���������
 * @param block aspectΪtrueʱ�������TRACE��Ϣ�Ĵ���
 */
#define ftrace(aspect, block)				\
	FuncReturn __func_return__;				\
	do {									\
		if (aspect)	{						\
			tout.beginFunc(__FUNC__);	\
			block;							\
			tout.end();						\
		}									\
	} while(0)

/**
 * ��������Ǻ�������TRACE��Ϣ�ĺ꣬�Զ�����кţ����Զ��ö��ŷָ���
 * ����
 *
 * @param aspect ����ts.memeber��ָ��TRACE��Ϣ���������
 * @param block aspectΪtrueʱ�������TRACE��Ϣ�Ĵ���
 */
#define nftrace(aspect, block)				\
	do {									\
		if (aspect) {						\
			tout.beginNonFunc(__LINE__);	\
			block;							\
			tout.end();						\
		}									\
	} while(0)
#else
#define ftrace(aspect, block)
#define nftrace(aspect, block)
#endif

/** �о�����Trace�������� */
enum TraceDataType {
	TRACE_START = 0,
	TRACE_END,

	TRACE_BOOL,
	TRACE_CHAR,
	TRACE_BYTE,
	TRACE_DOUBLE,
	TRACE_INT,
	TRACE_U16,
	TRACE_UINT,
	TRACE_S64,
	TRACE_U64,
	TRACE_CHARS,
	TRACE_FILE,
	TRACE_VOID,

	TRACE_BARR,
	TRACE_U16A,
	TRACE_RID,

	TRACE_ENUM_LOCKMODE,
	TRACE_ENUM_PAGETYPE,
	TRACE_ENUM_SCANTYPE,
	TRACE_ENUM_LOGTYPE,
	TRACE_ENUM_FORMAT,
	TRACE_ENUM_OPTYPE,

	TRACE_COMPLEX,
	TRACE_COMPLEX_END
};

#define TRACE_DATA_TYPE	u8		/** ����trace���ݵ�����*/
#define TRACE_MAX_SIZE_LEN 2	/** ������ʾtrace���ȵı������ռ4���ֽ� */

//////////////////////////////////////////////////////////////////////////////
// ר�������TRACE�Ľṹ
//////////////////////////////////////////////////////////////////////////////

/** ö������ 
 * Ϊ�˽��ö�����͵��µ�ͷ�ļ����ܱ���������
 * ����Ҫ������ö�����Ϳ�����һ�ָ������Ķ�����ʽ�����ͬʱ��ö�����Ͷ����ƻ���ʱ���ܸ������Ч
 * ׼����Trace��ͷ�ļ�������һ��ö�����ͣ����������ھ�̬ע����Ҫʵ��trace���ܵ�ö������
 * ���һ��������Ҫ
 */

//enum TracableEnums {
//
//};

/** �ֽ����� */
struct barr {
	const byte	*m_data;
	size_t		m_size;

	barr(size_t size, const void *data) {
		m_size = size;
		m_data = (const byte *)data;
	}
};

/** u16���͵����� */
struct u16a {
	size_t	m_size;
	const u16		*m_data;

	u16a(size_t size, const u16 *data) {
		m_size = size;
		m_data = data;
	}
};

/** ��¼ID */
struct rid {
	RowId	m_rid;

	rid(RowId id) {
		m_rid = id;
	}
};

class Record;
class TableDef;
/** ��������ļ�¼ */
struct t_rec {
	const TableDef *m_tableDef;
	const Record *m_record;

	t_rec(const TableDef *tableDef, const Record *record) {
		m_tableDef = tableDef;
		m_record = record;
	}
};

class SubRecord;
class TableDef;
/** ��������Ĳ��ּ�¼ */
struct t_srec {
	const TableDef *m_tableDef;
	const SubRecord *m_subRec;

	t_srec(const TableDef *tableDef, const SubRecord *subRec) {
		m_tableDef = tableDef;
		m_subRec = subRec;
	}
};

class File;
struct Tracer {
public:
	static bool init();
	static void closeAll();
	static void flush();
	static void exit();
	void beginFunc(const char *func);
	void beginNonFunc(int line);
	void end();
	Tracer& operator << (const char *s);
	Tracer& operator << (char n);
	Tracer& operator << (byte n);
	Tracer& operator << (u16 n);
	Tracer& operator << (int n);
	Tracer& operator << (uint n);
	Tracer& operator << (s64 n);
	Tracer& operator << (u64 n);
	Tracer& operator << (double n);
	Tracer& operator << (bool n);
	Tracer& operator << (LockMode lockMode);
	Tracer& operator << (const void *p);
	Tracer& operator << (const File *file);
	Tracer& operator << (const u16a &arr);
	Tracer& operator << (const barr &ba);
	Tracer& operator << (rid n);
	Tracer& operator << (TraceDataType type);

	// ���µĺ���Ӧ������Ϊ˽�У������ڲ���������˽�г�Ա���߳�˽�ж���
	// ֻ������Ϊ����
	static bool newOrSwitchTraceFile();
	static void segfaultHandler(int sig);
	static void initSignals();

	void beginComplex(const char *name);
	void endComplex();
	void autoComma();
	int getThreadId();
	bool isInternalThread();
	void printBegin(bool fTrace, u64 threadId, const char *thdType, size_t leadingSpaces, int fIdent, int line = 0, const char *func = NULL);
	void print(const char *fmt, ...);
	void publish(const byte *msg, size_t size);
	void switchTraceFileIfNecessary();
	size_t caculateLeadingSpace(bool nonFunc);

	/** ÿ���߳�TRACE�����С */
	static const size_t BUFSZ = 4096;
	/** �����Ƕ�ײ�� */
	static const int MAX_INDENT = 64;
	/** ÿ��TRACE��Ϣ�������˴�С����ܱ��ض� */
	static const size_t LINESZ = 1024;
	/** Trace�ļ�Ĭ��ǰ׺���ƣ�ʵ��ʹ�õ�ʱ�������ʱ����Ϊ��׺ */
	static const char *DEFAULT_TRACE_FILE_PREFIX;
	/** Trace�ļ�Ĭ�ϴ�С�������ô�С�ᴴ��һ���µ��ļ� */
	static const size_t DEFAULT_TRACE_FILE_SIZE;
	/** �ֽ���DUMP��Trace��ʱ����¼�����ֽڣ������˴�Сʱ����ʾͷβʡ���м����� */
	static const size_t BARR_DUMP_SZ = 32;
	/** NTSE TRACE�ļ����еı�־��д���ļ�ͷ����������Ӧ�ó���ʶ�� */
	static const char *DEFAULT_TRACE_FILE_MARK;

	bool	m_firstItem;		/** �Ƿ��ǵ�һ������ */
	byte	m_buffer[BUFSZ];	/** ��ǰ�߳�TRACE��Ϣ�Ļ����� */
};

#ifdef NTSE_TRACE
extern TLS Tracer	tout;
#endif

Tracer& operator << (Tracer& tracer, const t_rec& r);
Tracer& operator << (Tracer& tracer, const t_srec& sr);

Tracer& operator << (Tracer& tracer, const Record *r);
Tracer& operator << (Tracer& tracer, const SubRecord *sr);

struct Bcb;
struct LogEntry;
struct HeapHeaderPageInfo;
struct IndexScanCond;
struct Stream;
class BitmapPage;
class DrsHeap;
class TblScan;
class Session;

Tracer& operator << (Tracer& tracer, const Bcb *bcb);
Tracer& operator << (Tracer& tracer, const BitmapPage *bmp);
Tracer& operator << (Tracer& tracer, FixedLengthRecordHeap *heap);
Tracer& operator << (Tracer& tracer, VariableLengthRecordHeap *heap);
Tracer& operator << (Tracer& tracer, const HeapHeaderPageInfo *headerPage);
Tracer& operator << (Tracer& tracer, const TblScan *scan);
Tracer& operator << (Tracer& tracer, const IndexScanCond *cond);
Tracer& operator << (Tracer& tracer, const Session *session);
Tracer& operator << (Tracer& tracer, const LogEntry *logEntry);
Tracer& operator << (Tracer &tracer, const std::vector<Bcb *> *list);


enum TraceParseResult {
	TPR_SUCCESS,
	TPR_TRACE_INCOMPLETE,
	TPR_PARSE_ERROR,
	TPR_SKIPPED
};


class ParseFilter {
public:
	virtual ~ParseFilter() {}
	// ���ڸ���trace��ͷ����Ϣ������trace�ĺ�����true��ʾ���ܺ��ԣ�false��ʾ��traceӦ�ñ�����
	virtual bool TraceHeaderFilter(bool ftrace, u64 threadid, const char *thdType, int fIndent, const char *func) = 0;
	// ���ڸ���trace��������Ϣ������trace�ĺ�����true��ʾ���ܺ��ԣ�false��ʾ��traceӦ�ñ�����
	virtual bool TraceBodyFilter(const char *trace) = 0;
};


class TraceParser {
public:
	TraceParser(byte *trace, size_t tSize, byte *result, size_t size, ParseFilter *filter);
	TraceParseResult parse();
	size_t getTraceLength();
	void resetTrace(byte *trace, size_t tSize);

private:
	TraceParseResult parseTraceHeader(Stream *s);
	TraceParseResult parseTraceTail(Stream *s);
	TraceParseResult parseTraceBody(Stream *s);
	TraceParseResult parseComplexType(Stream *s);
	TraceParseResult parseOneDataType(Stream *s, TraceDataType type);

	void parseByteArray( Stream * s );
	void parseU16Array( Stream * s );

	void writeGap();
	void writeParsed(const char *fmt, ...);

	TraceParseResult check();

	char* getHexString(const byte *ba, size_t size, char *str);
	inline char getHexChar(int v);

private:
	byte *m_trace;			/** Ҫ������trace����ʼ��ַ */
	size_t m_tSize;			/** trace����󳤶� */
	byte *m_buffer;			/** ��Ż������� */
	size_t m_bOffset;		/** ������浱ǰƫ���� */
	size_t m_bSize;			/** �����ܴ�С */
	ParseFilter *m_filter;	/** ���������� */
	bool m_ftrace;			/** Ҫ�������Ƿ�ftrace */
	size_t m_traceRealSize;	/** ���浱ǰtrace�ĳ��� */
};

}

#endif

