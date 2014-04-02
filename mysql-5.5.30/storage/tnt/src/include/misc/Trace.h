/**
 * 程序行为记录TRACE功能
 *
 * @author 汪源(wangyuan@corp.netease.com, wy@163.org)
 */

#ifndef _NTSE_TRACE_H_
#define _NTSE_TRACE_H_

#include "util/Portable.h"
#include "util/Sync.h"
#include "misc/Global.h"
#include <vector>

/**
== TRACE框架如何使用 ==

TRACE是记录程序行为，方便程序调试的技术，本文件实现NTSE中的TRACE框架。
这一框架主要提供以下三项功能。

1. ftrace/nftrace: ftrace/nftrace为用于输出TRACE信息的宏，在定义了NTSE_TRACE
   进行编译时这些宏被扩展为判断指定的模块是否开启TRACE，若开启则执行指定
   的TRACE语句等一系列操作，包括自动的输出函数名称/行号等功能。在没有定义
   NTSE_TRACE进行编译时nstrace宏定义为空；
2. ts: ts是全局TRACE配置参数，为一个TraceSetting结构体的实例，用于实现动态
   打开或关闭各个模块的TRACE功能；
3. tout: tout类似于cout，是专用于输出TRACE信息的输出流对象，使用其<<操作符
   进行类型安全的格式化输出。

一般情况下使用本框架实现TRACE的用法如下：

{{{
ftrace(ts.aspect, tout << xxx << yyy;);
}}}
或
{{{
nftrace(ts.aspect, tout << xxx << yyy;);
}}}

其中aspect为TraceSetting结构的某个成员，用于指定TRACE信息所属的类别。

ftrace宏专用于输出函数调用时的TRACE信息，自动输出函数名，增加函数调用
层次信息，并且默认情况下会自动用逗号分隔函数调用的各个参数。
{{{
ftrace(ts.aspect, tout << xxx << yyy;);
}}}
输出的TRACE信息格式如下：
{{{
线程ID[函数调用层次]:函数名(xxx,yyy)
}}}
通过函数调用层次，可以对函数调用进行缩进的树形显示。

nftrace宏用于输出非函数调用TRACE信息，自动输出行号，不增加函数调用层次信息，
也不会自动用逗号分隔各项内容。
{{{
nftrace(ts.aspect, tout << xxx << yyy;);
}}}
输出的TRACE信息格式如下：
{{{
线程ID[L行号]:xxxyyy
}}}

== TRACE系统的初始化与清理 ==
使用TRACE框架输出任何TRACE信息之前必须调用Tracer::init()函数进行初始化，
程序退出前需调用Tracer::exit()函数进行清理。

== 如何输出tout内建不支持的数据类型 ==

tout内建的格式化输出功能包含对数值、字符串等常见类型的支持，如果某种数据
类型tout默认不支持，则可以通过提供以下的函数扩展其功能：

{{{
Tracer& operator << (Tracer &tracer, T t);
}}}

其中T为数据类型。

比如Table.h中的ScanIntention为一个枚举类型，在TRACE时需要将其输出为字符串，
则在Table.h中定义了一个方法：

{{{
Tracer& operator << (Tracer &tracer, ScanIntention intention);
}}}

然后在Table.cpp中如下实现这一方法：

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

/** 各项TRACE是否打开的配置 */
struct TraceSetting {
public:
	TraceSetting();
	void closeAll();

public:
	bool	file;		/** 是否记录文件操作 */
	bool	rl;			/** 是否记录行锁操作 */
	bool	irl;		/** 是否记录索引页面锁操作 */
	bool	pool;		/** 是否记录内存页池操作 */
	bool	buf;		/** 是否记录页面缓存操作 */
	bool	hp;			/** 是否记录堆操作 */
	bool	idx;		/** 是否记录索引操作 */
	bool	mms;		/** 是否记录MMS操作 */
	bool	lob;		/** 是否记录大对象操作 */
	bool	ddl;		/** 是否记录API层DDL操作 */
	bool	dml;		/** 是否记录API层DML操作 */
	bool	recv;		/** 是否记录API层恢复过程 */
	bool	mysql;		/** 是否记录MySQL接口层操作 */
	bool	intg;		/** 是否记录集成测试调试信息操作 */
	bool	log;		/** 是否记录日志模块操作 */
	bool    mIdx;       /** 是否记录内存索引操作 */
};

extern TraceSetting	ts;

/** 利用C++函数返回时自动析构局部变量的机制在函数返回时进行TRACE处理的
 * 结构。
 */
struct FuncReturn {
	FuncReturn();
	~FuncReturn();
};

#ifdef NTSE_TRACE
/** 用于输出函数调用TRACE信息的宏，自动输出函数名，记录函数调用层次信息，
 * 并且自动用逗号分隔函数调用的各个参数
 *
 * @param aspect 形如ts.memeber，指定TRACE信息所属的类别
 * @param block aspect为true时用于输出TRACE信息的代码
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
 * 用于输出非函数调用TRACE信息的宏，自动输出行号，不自动用逗号分隔各
 * 输出项。
 *
 * @param aspect 形如ts.memeber，指定TRACE信息所属的类别
 * @param block aspect为true时用于输出TRACE信息的代码
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

/** 列举所有Trace数据类型 */
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

#define TRACE_DATA_TYPE	u8		/** 定义trace数据的类型*/
#define TRACE_MAX_SIZE_LEN 2	/** 用来表示trace长度的变量最多占4个字节 */

//////////////////////////////////////////////////////////////////////////////
// 专用于输出TRACE的结构
//////////////////////////////////////////////////////////////////////////////

/** 枚举类型 
 * 为了解决枚举类型导致的头文件可能被包含问题
 * 更主要的是让枚举类型可以以一种更便于阅读的形式输出，同时让枚举类型二进制化的时候能更快更有效
 * 准备在Trace的头文件再声明一个枚举类型，该类型用于静态注册需要实现trace功能的枚举类型
 * 如果一个类型需要
 */

//enum TracableEnums {
//
//};

/** 字节数组 */
struct barr {
	const byte	*m_data;
	size_t		m_size;

	barr(size_t size, const void *data) {
		m_size = size;
		m_data = (const byte *)data;
	}
};

/** u16类型的数组 */
struct u16a {
	size_t	m_size;
	const u16		*m_data;

	u16a(size_t size, const u16 *data) {
		m_size = size;
		m_data = data;
	}
};

/** 记录ID */
struct rid {
	RowId	m_rid;

	rid(RowId id) {
		m_rid = id;
	}
};

class Record;
class TableDef;
/** 包括表定义的记录 */
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
/** 包括表定义的部分记录 */
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

	// 以下的函数应该声明为私有，但由于不能声明带私有成员的线程私有对象
	// 只能声明为公有
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

	/** 每个线程TRACE缓存大小 */
	static const size_t BUFSZ = 4096;
	/** 最大函数嵌套层次 */
	static const int MAX_INDENT = 64;
	/** 每行TRACE信息若超过此大小则可能被截断 */
	static const size_t LINESZ = 1024;
	/** Trace文件默认前缀名称，实际使用的时候会增加时间作为后缀 */
	static const char *DEFAULT_TRACE_FILE_PREFIX;
	/** Trace文件默认大小，超过该大小会创建一个新的文件 */
	static const size_t DEFAULT_TRACE_FILE_SIZE;
	/** 字节流DUMP到Trace中时最大记录多少字节，超过此大小时将显示头尾省略中间内容 */
	static const size_t BARR_DUMP_SZ = 32;
	/** NTSE TRACE文件特有的标志，写在文件头，便于其他应用程序识别 */
	static const char *DEFAULT_TRACE_FILE_MARK;

	bool	m_firstItem;		/** 是否是第一项内容 */
	byte	m_buffer[BUFSZ];	/** 当前线程TRACE信息的缓冲区 */
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
	// 用于根据trace的头部信息来过滤trace的函数，true表示不能忽略，false表示该trace应该被忽略
	virtual bool TraceHeaderFilter(bool ftrace, u64 threadid, const char *thdType, int fIndent, const char *func) = 0;
	// 用于根据trace的内容信息来过滤trace的函数，true表示不能忽略，false表示该trace应该被忽略
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
	byte *m_trace;			/** 要解析的trace的起始地址 */
	size_t m_tSize;			/** trace的最大长度 */
	byte *m_buffer;			/** 存放缓存结果的 */
	size_t m_bOffset;		/** 结果缓存当前偏移量 */
	size_t m_bSize;			/** 缓存总大小 */
	ParseFilter *m_filter;	/** 解析过滤器 */
	bool m_ftrace;			/** 要解析的是否ftrace */
	size_t m_traceRealSize;	/** 保存当前trace的长度 */
};

}

#endif

