#include <stdlib.h>
#include <iostream>
#include <vector>
#include <string>
#include <algorithm>

#include <my_global.h>
#include <my_sys.h>
#include <my_getopt.h>

#include "util/Portable.h"
#include "util/File.h"
#include "misc/Global.h"
#include "misc/Trace.h"

using namespace ntse;
using namespace std;

static const u64 NOT_SPECIFIED_THREAD_ID = (u64)-1;

static bool opt_ntse_td_help = 0, opt_ntse_td_force = 0;
static char *opt_ntse_td_before, *opt_ntse_td_after;
static bool opt_ntse_td_ftraceonly = 0, opt_ntse_td_nftraceonly = 0;
static u64 opt_ntse_td_threadid = NOT_SPECIFIED_THREAD_ID;

static bool filter_trace_file = 0;
static u64 dump_before_time = 0, dump_after_time = 0;
static vector<char*> trace_files;

enum options_ntse_trace_dump
{
	OPT_DUMP_BEFORE = 256,
	OPT_DUMP_AFTER,
	OPT_TRACES,
	OPT_FTRACE_ONLY,
	OPT_NFTRACE_ONLY
};

struct my_option my_long_options[] =
{
	{"help", '?', "Display this help and exit",
		&opt_ntse_td_help, &opt_ntse_td_help, 0, GET_BOOL, NO_ARG, 0, 0, 0, 0,
		0, 0},
	{"force", 'f', "Force to dump every trace files, ignore any error",
		&opt_ntse_td_force, &opt_ntse_td_force, 0, GET_BOOL, NO_ARG, 0, 0, 0, 0, 0, 0},
	{"before", 'b', "Dump the trace files generated before this time. The format of time is [[[YYYY-]MM-]DD/]HH:MM:SS.YYYY is [1900, this year + 1], MM is [1, 12], DD is [0, 31], HH is [0, 23], MM and SS are both [0, 60]. Make sure the filenames' format is like '[XXX]ntse.trace.timevalue' and the timevalue must be valid, or else the option will be ignored.",
		&opt_ntse_td_before, &opt_ntse_td_before, 0, GET_STR, REQUIRED_ARG, 0, 0, 0, 0, 0, 0},
	{"after", 'a', "Dump the trace files generated after this time. The format of time is the same as described in before option.",
		&opt_ntse_td_after, &opt_ntse_td_after, 0, GET_STR, REQUIRED_ARG, 0, 0, 0, 0, 0, 0},
	{"threadid", 't', "Dump traces written by the specified thread.",
		&opt_ntse_td_threadid, &opt_ntse_td_threadid, 0, GET_ULONG, REQUIRED_ARG, NOT_SPECIFIED_THREAD_ID, 0, 0, 0, 0, 0},
	{"ftrace-only", OPT_FTRACE_ONLY, "Only dump the traces which are ftraces in all trace files. It's exclusive with --nftrace-only.",
		&opt_ntse_td_ftraceonly, &opt_ntse_td_ftraceonly, 0, GET_NO_ARG, NO_ARG, 0, 0, 0, 0, 0, 0},
	{"nftrace-only", OPT_NFTRACE_ONLY, "Only dump the traces which are nftraces in all trace files. It's exclusive with --ftrace-only.",
		&opt_ntse_td_nftraceonly, &opt_ntse_td_nftraceonly, 0, GET_NO_ARG, NO_ARG, 0, 0, 0, 0, 0, 0},
	{0, 0, 0, 0, 0, 0, GET_NO_ARG, NO_ARG, 0, 0, 0, 0, 0, 0}
};


enum TFPMessageCode {
	/** 没错误 */
	TFP_NO_ERROR,
	/** 已经读到TRACE文件末尾 */
	TFP_END_OF_TRACE_FILE,
	/** 标识文件错误 */
	TFP_NTSE_FILE_ERROR,
	/** 标识文件不是NTSE TRACE文件 */
	TFP_FILE_NOT_NTSE_TRACE,
	/** 表示文件内容出现不一致，如trace内容不完整无法解析等 */
	TFP_FILE_NOT_CONSISTENCY,
	/** trace解析失败 */
	TFP_TRACE_PARSE_FAIL
};

class TraceFileParser {
public:
	TraceFileParser(const char *traceFile, ParseFilter *filter) {
		m_traceFile = traceFile;
		m_file = NULL;
		m_fileOffset = 0;
		m_bufferPos = 0;
		m_bufferUsed = 0;
		m_filter = filter;
		m_TFPError = openFileAndCheck();
	}

	~TraceFileParser() {
		u64 error;
		if (m_file != NULL) {
			error = m_file->close();
			if (ntse::File::getNtseError(error) != ntse::File::E_NO_ERROR)
				printFileError("close", m_traceFile, error);

			delete m_file;
			m_file = NULL;
		}
	}

	TFPMessageCode parseTraces();

	/** 返回上一次操作的错误码 */
	TFPMessageCode getLastParseMessage() const {
		return m_TFPError;
	}

private:
	void parseNextTrace();
	TFPMessageCode openFileAndCheck();
	TFPMessageCode readFollowingTraces();
	bool checkFinish();
	void printFileError(const char *oper, const char *fileName, u64 error);

private:
	static const uint PARSER_BUFFER_SIZE = Limits::PAGE_SIZE;	/** 缓存大小 */

	const char *m_traceFile;		/** 要解析的trace文件名 */
	ntse::File *m_file;				/** 操作文件句柄 */
	u64 m_fileOffset;				/** 当前读取文件偏移量 */
	u64 m_fileSize;					/** 当前文件的总大小 */
	TFPMessageCode m_TFPError;		/** 标识文件是否是trace文件 */
	char m_trace[PARSER_BUFFER_SIZE];	/** 缓存当前解析的trace内容 */
	byte m_buffer[PARSER_BUFFER_SIZE];	/** 读取trace文件数据的缓存 */
	size_t m_bufferPos;					/** 当前缓存解析的位置 */
	size_t m_bufferUsed;				/** 当前缓存使用的大小 */

	ParseFilter *m_filter;			/** 解析trace的过滤器 */
};


/** 解析当前文件的所有trace信息
 * @return 返回错误码
 */
TFPMessageCode TraceFileParser::parseTraces() {
	if (checkFinish()) {
		if (m_TFPError == TFP_END_OF_TRACE_FILE)
			m_TFPError = TFP_NO_ERROR;
		return m_TFPError;
	} else if (m_TFPError != TFP_NO_ERROR)
		return m_TFPError;

	while (true) {
		parseNextTrace();
		if (m_TFPError != TFP_NO_ERROR) {
			if (m_TFPError == TFP_END_OF_TRACE_FILE)
				m_TFPError = TFP_NO_ERROR;
			return m_TFPError;
		}
		puts(m_trace);
	}
}


/** 解析下一条trace信息
 * @return 返回解析过的trace信息，如果为NULL，表示下一条信息无法解析或者当前文件已经解析完毕
 *		可以通过getLastParseMessage来得到具体信息，如果NULL并且是TFP_NO_ERROR表示文件解析完毕
 */
void TraceFileParser::parseNextTrace() {
	assert(m_TFPError == TFP_NO_ERROR);

	TraceParser parser(m_buffer + m_bufferPos, m_bufferUsed - m_bufferPos, (byte*)m_trace, PARSER_BUFFER_SIZE, m_filter);
	TraceParseResult tpr;
	while ((tpr = parser.parse()) != TPR_SUCCESS) {
		if (tpr == TPR_PARSE_ERROR) {
			// 解析出错，直接退出
			m_buffer[m_bufferPos] = '\0';
			printf("Parse trace: %s failed, buffer offset is : %lu, the trace file offset is : %llu\n", m_buffer, m_bufferPos, m_fileOffset);
			printf("Parsing trace is: %s", m_buffer);
			m_TFPError = TFP_TRACE_PARSE_FAIL;
			return;
		} 

		bool skipToBufferEnd = false;
		if (tpr == TPR_SKIPPED) {
			assert(tpr == TPR_SKIPPED);
			m_bufferPos += parser.getTraceLength();
			parser.resetTrace(m_buffer + m_bufferPos, m_bufferUsed - m_bufferPos);
			skipToBufferEnd = (m_bufferPos >= m_bufferUsed);
		}

		// 需要特别处理skip之后导致缓存被耗尽的情况
		if (tpr == TPR_TRACE_INCOMPLETE || skipToBufferEnd) {
			// 当前trace长度不足，读取后续的trace进行处理
			if (readFollowingTraces() == TFP_END_OF_TRACE_FILE) {
				// 当前文件读取完毕，可能存在两种情况，缓存中没有其他数据，表示正确解析完成
				// 如果缓存当中还有数据，说明这段数据时不完整的
				if (m_bufferPos < m_bufferUsed) {
					m_TFPError = TFP_TRACE_PARSE_FAIL;
					m_buffer[m_bufferPos] = '\0';
					printf("Parse trace: %s failed because some trace maybe incomplete, buffer offset is : %lu, the trace file offset is : %llu\n", m_buffer, m_bufferPos, m_fileOffset);
					printf("Parsing trace is: %s", m_buffer);
				} else
					m_TFPError = TFP_END_OF_TRACE_FILE;
				return;
			}
			parser.resetTrace(m_buffer + m_bufferPos, m_bufferUsed - m_bufferPos);
		} 
	}

	m_bufferPos += parser.getTraceLength();
	return;
}

/** 打开文件，并且检查文件是否是trace文件
 * @return TFPMessageCode	返回TFP_NO_ERROR表示成功，否则表示失败
 */
TFPMessageCode TraceFileParser::openFileAndCheck() {
	u64 error;
	m_file = new ntse::File(m_traceFile);
	error = m_file->open(false);
	if (ntse::File::getNtseError(error) != ntse::File::E_NO_ERROR) {
		printFileError("open", m_traceFile, error);
		return TFP_NTSE_FILE_ERROR;
	}

	error = m_file->getSize(&m_fileSize);
	if (ntse::File::getNtseError(error) != ntse::File::E_NO_ERROR) {
		printFileError("get size in header check", m_traceFile, error);
		return TFP_NTSE_FILE_ERROR;
	}

	// 检查文件头几个字节是否包含NTSE TRACE文件的标志
	u8 markLen = strlen(Tracer::DEFAULT_TRACE_FILE_MARK);
	m_fileOffset += markLen;
	byte *mark = new byte[markLen];
	error = m_file->read(0, strlen(Tracer::DEFAULT_TRACE_FILE_MARK), mark);
	if (ntse::File::getNtseError(error) != ntse::File::E_NO_ERROR) {
		printFileError("read header tag", m_traceFile, error);
		return TFP_NTSE_FILE_ERROR;
	}

	if (memcmp(mark, Tracer::DEFAULT_TRACE_FILE_MARK, markLen)) {
		printf("The file %s is not a ntse trace file because it doesn't start with %s\n", m_traceFile, Tracer::DEFAULT_TRACE_FILE_MARK);
		return TFP_FILE_NOT_NTSE_TRACE;
	}

	return TFP_NO_ERROR;
}

/** 读取trace文件后续的部分，填充到缓存
 * 缓存此时可能还有部分数据未解析，所以需要把未解析的部分拷贝到缓存开始，然后再读取文件的数据
 * @return TFP_NO_ERROR表示读取成功，其他情况表示读取失败
 */
TFPMessageCode TraceFileParser::readFollowingTraces() {
	if (checkFinish())
		return TFP_END_OF_TRACE_FILE;

	size_t unreadBufferSize = m_bufferUsed - m_bufferPos;
	memcpy(&m_buffer[0], &m_buffer[m_bufferPos], unreadBufferSize);
	
	size_t readSize = PARSER_BUFFER_SIZE - unreadBufferSize;
	if (readSize > m_fileSize - m_fileOffset)
		readSize = (size_t)(m_fileSize - m_fileOffset);

	u64 error;
	error = m_file->read(m_fileOffset, readSize, &m_buffer[unreadBufferSize]);
	if (ntse::File::getNtseError(error) != ntse::File::E_NO_ERROR) {
		printFileError("read", m_traceFile, error);
		return TFP_NTSE_FILE_ERROR;
	}

	m_bufferUsed = unreadBufferSize + readSize;
	m_bufferPos = 0;
	assert(m_bufferUsed - m_bufferPos > 0);
	m_fileOffset += readSize;

	return TFP_NO_ERROR;
}

/** 打印文件操作错误信息
 * @param oper		文件操作类型
 * @param fileName	文件名
 * @param error		错误码
 */
void TraceFileParser::printFileError( const char *oper, const char *fileName, u64 error ) {
	printf("File %s %s failed. Error: %s\n", fileName, oper, ntse::File::explainErrno(error));
}

/** 检查文件是否读完
 * @return true表示文件读完，false表示没读完
 */
bool TraceFileParser::checkFinish() {
	return (m_fileOffset == m_fileSize);
}

/** 将字符串格式的时间信息转换成长整型
 * @param t	字符串的时间信息，标准格式为[[[YYYY-]MM-]DD/]HH:MM:SS
 * @return 返回转换好的时间的长整型格式，如果时间格式错误，返回-1
 */
static time_t parseTimeToLong(const char *t) {
	string stime(t);
	int pos, rpos;
	size_t timeStrlen = stime.length();
	if (timeStrlen < 8 || timeStrlen > 19)
		goto error;

	time_t now;
	struct tm *stm;
	time(&now);
	stm = localtime(&now);

	// 根据设置，修改timenow里面的数据
	rpos = pos = stime.find('/');
	if (timeStrlen - pos - 1 != 8)
		// 保证/后面的时间长度正确
		goto error;

	if (pos != -1) {	// 表示包括了日期
		if (pos < 2)
			goto error;

		stm->tm_yday = stm->tm_wday = stm->tm_isdst = 0;
		stm->tm_mday = atoi(stime.substr(pos - 2, 2).c_str());
		if (stm->tm_mday < 1 || stm->tm_mday > 31)	// TODO: 没有根据月份处理天数
			goto error;
		pos -= 2;

		if (pos > 0) {
			if (--pos < 2)
				goto error;

			stm->tm_mon = atoi(stime.substr(pos - 2, 2).c_str()) - 1;
			if (stm->tm_mon < 1 || stm->tm_mon > 12)
				goto error;
			pos -= 2;
		}

		if (pos > 0) {
			if (--pos != 4)
				goto error;

			int year = atoi(stime.substr(pos - 4, 4).c_str());
			if (year > stm->tm_year + 1901 || year < 1900)
				goto error;
			stm->tm_year = year - 1900;
		}
	}

	stm->tm_hour = atoi(stime.substr(++rpos, 2).c_str());
	rpos +=3;
	stm->tm_min = atoi(stime.substr(rpos, 2).c_str());
	rpos += 3;
	stm->tm_sec = atoi(stime.substr(rpos, 2).c_str());
	assert((size_t)(rpos + 2) == timeStrlen);
	if (stm->tm_hour >= 24 || stm->tm_hour < 1 ||
		stm->tm_min < 0 || stm->tm_min >= 60 ||
		stm->tm_sec < 0 || stm->tm_sec >= 60)
		goto error;

	return mktime(stm);

error:
	printf("Parse time %s error. Use help to see right time format\n", t);
	return (time_t)-1;
}


/**
 * 使用帮助函数
 */
static void usage() {
	puts("\
		 Use this tool to dump ntse binary-format trace files to readable text format.");
	printf("Usage: %s [OPTIONS]\n", my_progname);
	puts("");

	/* Print out all the options including plugin supplied options */
	my_print_help(my_long_options);
	my_print_variables(my_long_options);
}


/**
 * 处理应用程序参数的回调函数
 * @param optid		参数类型
 * @param opt		参数定义
 * @param argument	参数读出来的值
 * @return true表示解析有错误,false表示解析没错误
 */
static my_bool
ntse_trace_dump_get_one_option(int optid,
					  const struct my_option *opt __attribute__((unused)),
					  char *argument) {
	int error = 0;
	switch (optid) {
		case '?':
			usage();
			exit(0);
			break;
		case 'b':
			filter_trace_file = true;
			dump_before_time = parseTimeToLong(argument);
			if (dump_before_time == (u64)-1) {
				printf("Parse before time failed: %s\n", argument);
				error = 1;
			}
			break;
		case 'a':
			filter_trace_file = true;
			dump_after_time = parseTimeToLong(argument);
			if (dump_after_time == (u64)-1) {
				printf("Parse after time failed: %s\n", argument);
				error = 1;
			}
			break;
		case 't':
			opt_ntse_td_threadid = atol(argument);
			break;
		case 'f':
			break;
		case OPT_FTRACE_ONLY:
			opt_ntse_td_ftraceonly = true;
			assert(argument == NULL);
			break;
		case OPT_NFTRACE_ONLY:
			opt_ntse_td_nftraceonly = true;
			assert(argument == NULL);
			break;
		default:
			break;
	}

	return error;
}


/**
 * 取得所有参数选项信息并检查参数有效性
 * @return 0表示正确,非0表示错误
 */
static int getOptions(int *argc,char ***argv) {
	int error = handle_options(argc, argv, my_long_options,
		ntse_trace_dump_get_one_option);

	// 检查参数的正确性
	if (dump_after_time == (u64)-1 || dump_before_time == (u64)-1)
		return 1;
	if (dump_after_time != 0 && dump_before_time != 0 && dump_after_time > dump_before_time) {
		printf("Dump time setting is wrong. After time should be earlier than before time.\n");
		return 1;
	}

	return error;
}


/** 得到trace文件名中包含的创建时间，如果格式错误，返回0
 * 该文件只解析[XXX]ntse.trace.xxx(时间值)格式的文件,[XXX]表示任意字符串，主要会是路径信息，如果不是这个格式，不进行时间过滤
 * 如果最后的时间值不是合法的时间，也不进行过滤，比如包括了非法的字符等
 * @param fileName	要解析的trace文件
 * @return 非0表示创建时间，0表示文件名格式不合预期
 */
static u64 getCreateTime(const char *fileName) {
	string s(fileName);
	size_t lastDotPos = s.find_last_of('.');
	if (lastDotPos == string::npos)
		return 0;

	size_t prefixPos = s.find_last_of(ntse::Tracer::DEFAULT_TRACE_FILE_PREFIX);
	if (prefixPos != lastDotPos)
		return 0;

	const char *time = fileName + lastDotPos + 1;
	return strtoul(time, NULL, 10);
}

/** 判断指定文件时间上是否满足，这里将根据文件的创建时间来过滤
 * @param fileName	trace文件名
 * @return true表示需要解析,false表示文件被过滤了
 */
static bool timeEligible(const char *fileName) {
	if (dump_before_time == 0 && dump_after_time == 0)
		return true;

	u64 time = getCreateTime(fileName);
	if (time == 0)
		return false;
	else if ((dump_before_time != 0 && time > dump_before_time) ||
		(dump_after_time != 0 && time < dump_after_time))
		return false;

	return true;
}


static bool compareFileName(const char *fileName1, const char *fileName2) {
	u64 time1 = getCreateTime(fileName1);
	u64 time2 = getCreateTime(fileName2);

	return time1 < time2;
}

/** 初始化公共变量
 * @param argc	变量个数
 * @param argv	变量内容
 * @return 0表示正确,非0表示错误
 */
static int initCommonVariables(int *argc, char **argv) {
	int error = 0;
	if ((error = getOptions(argc, &argv)))
		return error;

	// 此时argv里面排在前面的argc项表示的是非--option参数的内容
	// 在我们的使用当中，就是要解析的trace文件
	for (int i = 0; i < *argc; i++)
		trace_files.push_back(argv[i]);
	// 按照时间排序，时间早的排前面
	std::sort(trace_files.begin(), trace_files.end(), compareFileName);

	return error;
}


/** 根据过滤条件,判断指定的trace文件是不是会被过滤掉
 * @param fileName	trace文件名
 * @return true表示需要解析,false表示文件被过滤了
 */
static bool traceFileNeedDumped(const char *fileName) {
	assert(filter_trace_file);

	// 判断时间是否在范围内
	if (!timeEligible(fileName))
		return false;

	return true;
}

class DumpParseFilter : public ParseFilter {
public:
	DumpParseFilter(bool ftraceOnly, bool nftraceOnly, u64 threadId) : m_ftraceOnly(ftraceOnly), m_nftraceOnly(nftraceOnly), m_threadId(threadId) {}
	virtual bool TraceHeaderFilter(bool ftrace, u64 threadid, const char *thdType, int fIndent, const char *func) {
		if ((m_ftraceOnly && !ftrace) || (m_nftraceOnly && ftrace))
			return false;
		if (m_threadId != NOT_SPECIFIED_THREAD_ID && threadid != m_threadId)
			return false;
		return true;
	}

	virtual bool TraceBodyFilter(const char *trace) {
		// 不过滤trace本身
		return true;
	}

private:
	bool	m_ftraceOnly;	/** true表示过滤掉nftrace */
	bool	m_nftraceOnly;	/** true表示过滤掉ftrace */
	u64		m_threadId;		/** 只解析该线程的信息，NOT_SPECIFIED_THREAD_ID表示不加条件 */
};


/** 解析一个指定的trace文件
 * @param filter	过滤器
 * @return 0表示正确,非0表示错误
 */
static int parseOneTraceFile(const char *fileName, ParseFilter *filter) {
	TraceFileParser parser(fileName, filter);
	TFPMessageCode error = parser.parseTraces();

	if (error != TFP_NO_ERROR) {
		printf("Parse %s failed, ErrorCode is %d\n", fileName, parser.getLastParseMessage());
		return error;
	}

	return error;
}


/** 从全局变量trace_files里面读取各个trace文件来解析
 * @return 0表示正确,非0表示错误
 */
static int parseTraceFiles() {
	int error = 0;
	DumpParseFilter filter(opt_ntse_td_ftraceonly, opt_ntse_td_nftraceonly, opt_ntse_td_threadid);

	for (uint i = 0; i < trace_files.size(); i++) {
		char *fileName = trace_files[i];
		if (filter_trace_file && !traceFileNeedDumped(fileName)) {
			// 条件不满足的文件，直接略过
			continue;
		}

		if ((error = parseOneTraceFile(fileName, &filter))) {
			if (!opt_ntse_td_force) {
				printf("Parse trace file %s failed.\n", fileName);
				return error;
			}
		}
	}

	return error;
}

int main(int argc, char *argv[]) {
	MY_INIT(argv[0]);

	Tracer::closeAll();//在本工程中将所有的trace开关关闭

	int error;

	if ((error = initCommonVariables(&argc, argv))) {
		printf("Init common variables failed and program exit.\n");
		return error;
	}

	if (trace_files.empty()) {
		printf("Please input valid trace files. No trace files found.\n");
		printf("To see how to use this tool, please type ntse_trace_dump --help\n");
		return 1;
	}

	if ((error = parseTraceFiles())) {
		printf("Parse trace files failed.\n");
		return 1;
	}

	return 0;
}
