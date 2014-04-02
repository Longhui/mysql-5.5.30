/**
 * 分区表的.par文件解析器
 *
 * @author 王公仆(hzwanggongpu@corp.netease.com)
 */

#ifndef _NTSE_PARFILEPARSER_H_
#define _NTSE_PARFILEPARSER_H_

#include <limits.h>
#include <string>
#include <vector>
#include "misc/Global.h"
#include "util/Portable.h"

#define PARTITION_TABLE_LABLE "#P#"

namespace ntse {
	
/**   
 *   分区表的.par文件解析器
 *     
 */
class ParFileParser {
public:
	ParFileParser(const char *path);
	~ParFileParser();
	static bool isPartitionByPhyicTabName(const char *phyTabName);
	static bool parseLogicTabName(const char *phyTabName, char *logTabName, u32 logTabNameLen);
	bool isPartitionByLogicTabPath();
	void parseParFile(std::vector<std::string> &pars) throw(NtseException);
	
private:
	const char *m_filePath;									/** 物理表或逻辑表全路径*/
};

}

#endif

