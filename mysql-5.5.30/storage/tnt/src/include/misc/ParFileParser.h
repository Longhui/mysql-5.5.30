/**
 * �������.par�ļ�������
 *
 * @author ������(hzwanggongpu@corp.netease.com)
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
 *   �������.par�ļ�������
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
	const char *m_filePath;									/** �������߼���ȫ·��*/
};

}

#endif

