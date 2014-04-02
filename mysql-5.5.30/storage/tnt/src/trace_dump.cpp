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
	/** û���� */
	TFP_NO_ERROR,
	/** �Ѿ�����TRACE�ļ�ĩβ */
	TFP_END_OF_TRACE_FILE,
	/** ��ʶ�ļ����� */
	TFP_NTSE_FILE_ERROR,
	/** ��ʶ�ļ�����NTSE TRACE�ļ� */
	TFP_FILE_NOT_NTSE_TRACE,
	/** ��ʾ�ļ����ݳ��ֲ�һ�£���trace���ݲ������޷������� */
	TFP_FILE_NOT_CONSISTENCY,
	/** trace����ʧ�� */
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

	/** ������һ�β����Ĵ����� */
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
	static const uint PARSER_BUFFER_SIZE = Limits::PAGE_SIZE;	/** �����С */

	const char *m_traceFile;		/** Ҫ������trace�ļ��� */
	ntse::File *m_file;				/** �����ļ���� */
	u64 m_fileOffset;				/** ��ǰ��ȡ�ļ�ƫ���� */
	u64 m_fileSize;					/** ��ǰ�ļ����ܴ�С */
	TFPMessageCode m_TFPError;		/** ��ʶ�ļ��Ƿ���trace�ļ� */
	char m_trace[PARSER_BUFFER_SIZE];	/** ���浱ǰ������trace���� */
	byte m_buffer[PARSER_BUFFER_SIZE];	/** ��ȡtrace�ļ����ݵĻ��� */
	size_t m_bufferPos;					/** ��ǰ���������λ�� */
	size_t m_bufferUsed;				/** ��ǰ����ʹ�õĴ�С */

	ParseFilter *m_filter;			/** ����trace�Ĺ����� */
};


/** ������ǰ�ļ�������trace��Ϣ
 * @return ���ش�����
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


/** ������һ��trace��Ϣ
 * @return ���ؽ�������trace��Ϣ�����ΪNULL����ʾ��һ����Ϣ�޷��������ߵ�ǰ�ļ��Ѿ��������
 *		����ͨ��getLastParseMessage���õ�������Ϣ�����NULL������TFP_NO_ERROR��ʾ�ļ��������
 */
void TraceFileParser::parseNextTrace() {
	assert(m_TFPError == TFP_NO_ERROR);

	TraceParser parser(m_buffer + m_bufferPos, m_bufferUsed - m_bufferPos, (byte*)m_trace, PARSER_BUFFER_SIZE, m_filter);
	TraceParseResult tpr;
	while ((tpr = parser.parse()) != TPR_SUCCESS) {
		if (tpr == TPR_PARSE_ERROR) {
			// ��������ֱ���˳�
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

		// ��Ҫ�ر���skip֮���»��汻�ľ������
		if (tpr == TPR_TRACE_INCOMPLETE || skipToBufferEnd) {
			// ��ǰtrace���Ȳ��㣬��ȡ������trace���д���
			if (readFollowingTraces() == TFP_END_OF_TRACE_FILE) {
				// ��ǰ�ļ���ȡ��ϣ����ܴ������������������û���������ݣ���ʾ��ȷ�������
				// ������浱�л������ݣ�˵���������ʱ��������
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

/** ���ļ������Ҽ���ļ��Ƿ���trace�ļ�
 * @return TFPMessageCode	����TFP_NO_ERROR��ʾ�ɹ��������ʾʧ��
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

	// ����ļ�ͷ�����ֽ��Ƿ����NTSE TRACE�ļ��ı�־
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

/** ��ȡtrace�ļ������Ĳ��֣���䵽����
 * �����ʱ���ܻ��в�������δ������������Ҫ��δ�����Ĳ��ֿ��������濪ʼ��Ȼ���ٶ�ȡ�ļ�������
 * @return TFP_NO_ERROR��ʾ��ȡ�ɹ������������ʾ��ȡʧ��
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

/** ��ӡ�ļ�����������Ϣ
 * @param oper		�ļ���������
 * @param fileName	�ļ���
 * @param error		������
 */
void TraceFileParser::printFileError( const char *oper, const char *fileName, u64 error ) {
	printf("File %s %s failed. Error: %s\n", fileName, oper, ntse::File::explainErrno(error));
}

/** ����ļ��Ƿ����
 * @return true��ʾ�ļ����꣬false��ʾû����
 */
bool TraceFileParser::checkFinish() {
	return (m_fileOffset == m_fileSize);
}

/** ���ַ�����ʽ��ʱ����Ϣת���ɳ�����
 * @param t	�ַ�����ʱ����Ϣ����׼��ʽΪ[[[YYYY-]MM-]DD/]HH:MM:SS
 * @return ����ת���õ�ʱ��ĳ����͸�ʽ�����ʱ���ʽ���󣬷���-1
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

	// �������ã��޸�timenow���������
	rpos = pos = stime.find('/');
	if (timeStrlen - pos - 1 != 8)
		// ��֤/�����ʱ�䳤����ȷ
		goto error;

	if (pos != -1) {	// ��ʾ����������
		if (pos < 2)
			goto error;

		stm->tm_yday = stm->tm_wday = stm->tm_isdst = 0;
		stm->tm_mday = atoi(stime.substr(pos - 2, 2).c_str());
		if (stm->tm_mday < 1 || stm->tm_mday > 31)	// TODO: û�и����·ݴ�������
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
 * ʹ�ð�������
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
 * ����Ӧ�ó�������Ļص�����
 * @param optid		��������
 * @param opt		��������
 * @param argument	������������ֵ
 * @return true��ʾ�����д���,false��ʾ����û����
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
 * ȡ�����в���ѡ����Ϣ����������Ч��
 * @return 0��ʾ��ȷ,��0��ʾ����
 */
static int getOptions(int *argc,char ***argv) {
	int error = handle_options(argc, argv, my_long_options,
		ntse_trace_dump_get_one_option);

	// ����������ȷ��
	if (dump_after_time == (u64)-1 || dump_before_time == (u64)-1)
		return 1;
	if (dump_after_time != 0 && dump_before_time != 0 && dump_after_time > dump_before_time) {
		printf("Dump time setting is wrong. After time should be earlier than before time.\n");
		return 1;
	}

	return error;
}


/** �õ�trace�ļ����а����Ĵ���ʱ�䣬�����ʽ���󣬷���0
 * ���ļ�ֻ����[XXX]ntse.trace.xxx(ʱ��ֵ)��ʽ���ļ�,[XXX]��ʾ�����ַ�������Ҫ����·����Ϣ��������������ʽ��������ʱ�����
 * �������ʱ��ֵ���ǺϷ���ʱ�䣬Ҳ�����й��ˣ���������˷Ƿ����ַ���
 * @param fileName	Ҫ������trace�ļ�
 * @return ��0��ʾ����ʱ�䣬0��ʾ�ļ�����ʽ����Ԥ��
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

/** �ж�ָ���ļ�ʱ�����Ƿ����㣬���ｫ�����ļ��Ĵ���ʱ��������
 * @param fileName	trace�ļ���
 * @return true��ʾ��Ҫ����,false��ʾ�ļ���������
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

/** ��ʼ����������
 * @param argc	��������
 * @param argv	��������
 * @return 0��ʾ��ȷ,��0��ʾ����
 */
static int initCommonVariables(int *argc, char **argv) {
	int error = 0;
	if ((error = getOptions(argc, &argv)))
		return error;

	// ��ʱargv��������ǰ���argc���ʾ���Ƿ�--option����������
	// �����ǵ�ʹ�õ��У�����Ҫ������trace�ļ�
	for (int i = 0; i < *argc; i++)
		trace_files.push_back(argv[i]);
	// ����ʱ������ʱ�������ǰ��
	std::sort(trace_files.begin(), trace_files.end(), compareFileName);

	return error;
}


/** ���ݹ�������,�ж�ָ����trace�ļ��ǲ��ǻᱻ���˵�
 * @param fileName	trace�ļ���
 * @return true��ʾ��Ҫ����,false��ʾ�ļ���������
 */
static bool traceFileNeedDumped(const char *fileName) {
	assert(filter_trace_file);

	// �ж�ʱ���Ƿ��ڷ�Χ��
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
		// ������trace����
		return true;
	}

private:
	bool	m_ftraceOnly;	/** true��ʾ���˵�nftrace */
	bool	m_nftraceOnly;	/** true��ʾ���˵�ftrace */
	u64		m_threadId;		/** ֻ�������̵߳���Ϣ��NOT_SPECIFIED_THREAD_ID��ʾ�������� */
};


/** ����һ��ָ����trace�ļ�
 * @param filter	������
 * @return 0��ʾ��ȷ,��0��ʾ����
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


/** ��ȫ�ֱ���trace_files�����ȡ����trace�ļ�������
 * @return 0��ʾ��ȷ,��0��ʾ����
 */
static int parseTraceFiles() {
	int error = 0;
	DumpParseFilter filter(opt_ntse_td_ftraceonly, opt_ntse_td_nftraceonly, opt_ntse_td_threadid);

	for (uint i = 0; i < trace_files.size(); i++) {
		char *fileName = trace_files[i];
		if (filter_trace_file && !traceFileNeedDumped(fileName)) {
			// ������������ļ���ֱ���Թ�
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

	Tracer::closeAll();//�ڱ������н����е�trace���عر�

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
