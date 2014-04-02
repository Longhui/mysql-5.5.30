/**
 * 一个功能简单的文件解析器
 *
 * @author 王公仆(hzwanggongpu@corp.netease.com)
 */

#include <string.h>
#include "misc/ParFileParser.h"
#include "util/SmartPtr.h"
#include "util/File.h"

using namespace ntse;

namespace ntse {

/** 
 * 构造函数创建一个用于解析指定路径的文件的文件解析器对象
 * @param path 待解析文件的路径字符串
 */
ParFileParser::ParFileParser(const char *path) {
	m_filePath = path;
}

/** 析构函数 */
ParFileParser::~ParFileParser() {}

/** 
 * 判断给定物理表是否为分区表
 * @param  phyTabName 物理表表名称
 * @return 分区表返回true，非分区表false
 */
bool ParFileParser::isPartitionByPhyicTabName(const char *phyTabName) {
	assert(phyTabName);
	string tmp(phyTabName);
	return (string::npos != tmp.find(PARTITION_TABLE_LABLE));
}

/** 
 * 判断给定物理表名解析出对应的逻辑表名
 * @param  phyTabName 物理表表名称
 * @param  logTabName OUT 物理表对应的逻辑表表名称
 * @param  logTabNameLen 逻辑表logTabName字节数
 * @return 成功返回true，失败返回false
 */
bool ParFileParser::parseLogicTabName(const char *phyTabName, char *logTabName, u32 logTabNameLen) {
	assert(phyTabName);
	assert(logTabName);
	if (isPartitionByPhyicTabName(phyTabName)) {		
		string tmp = string(phyTabName).erase(string(phyTabName).find(PARTITION_TABLE_LABLE));
		NTSE_ASSERT(logTabNameLen >= strlen(tmp.c_str()));
		memset(logTabName, 0x0, logTabNameLen);
		memcpy(logTabName, tmp.c_str(), strlen(tmp.c_str()));
		return true;
	} 
	return false;
}

/** 
 * 判断给定逻辑表全路径判断是否为该逻辑表为分区表
 * @return 是分区表返回true，非分区表返回false
 */
bool ParFileParser::isPartitionByLogicTabPath() {
	string oldPath(m_filePath);
	if(string::npos != oldPath.find(PARTITION_TABLE_LABLE)) {
		oldPath.erase(oldPath.find(PARTITION_TABLE_LABLE));
	}
	string parFile = oldPath + ".par";
	return (File::isExist(parFile.c_str()));
}

/**
 * 为了支持分区通过传入该表的全路径，判断是否存在.par文件如果存在则为分区表不存在则为非分区表
 * 如果为分区表，读取各分区名称，将所有分区表名全路径依次全部存入pars容器
 * 如果为非分区表，存入原始全路径不改动,注意非分区表路径字符串不含#P#；
 * @param  pars string对象vector，将所有子分区全路径存入其中，其size表示分区总数
 */
void ParFileParser::parseParFile(std::vector<std::string> &pars)  throw(NtseException) {
	uint totalWords = 0;
	uint totalParts = 0;
	uint totalPartitionWords = 0;
	char *ptrNameBuf = NULL;

	string oldPath(m_filePath);
	if(string::npos != oldPath.find(PARTITION_TABLE_LABLE)) {
		oldPath.erase(oldPath.find(PARTITION_TABLE_LABLE));
	}
	string parFile = oldPath + ".par";
	pars.clear();
	
	u64 errCode = File::E_OTHER;
	AutoPtr<File> fp(new File(parFile.c_str()));
	if ((errCode = fp->open(false)) == File::E_NO_ERROR) {
		//	Partition Table .par File format:
		//	Length in words              4 byte
		//	Checksum                     4 byte
		//	Total number of partitions   4 byte
		//	Array of engine types        n * 4 bytes where
		//	n = (number of partitions + 3)/4
		//	Length of name part in bytes 4 bytes
		//	Name part                    m * 4 bytes where
		//	m = ((Length of name part + 3)/4)*4
		//	All padding bytes are zeroed		
		NTSE_ASSERT(fp->read(0, 4, &totalWords) == File::E_NO_ERROR);
		byte *fileBuffer = new byte[4 * totalWords];
		AutoPtr<byte> autoFileBuf(fileBuffer, true);
		if (fileBuffer == NULL) {
			fp->close();
		} else {
			memset(fileBuffer, 0x0, sizeof(byte) * 4 * totalWords);
			NTSE_ASSERT(fp->read(0, 4 * totalWords, fileBuffer) == File::E_NO_ERROR);
			totalParts = (*((unsigned int *) ((fileBuffer) + 8))); 
			totalPartitionWords = (totalParts + 3) / 4;
			ptrNameBuf = (char *)fileBuffer + 16 + 4 * totalPartitionWords;
			fp->close();
		}
		for (uint k = 0;  k < totalParts; k++) {
			//分区和子分区都是通过#P#连接逻辑表名称和分区表子名称的；
			string partFileName = oldPath + PARTITION_TABLE_LABLE + ptrNameBuf;
			ptrNameBuf += strlen(ptrNameBuf) + 1;	
			pars.push_back(partFileName);
		}
	} else if(fp->getNtseError(errCode) == File::E_NOT_EXIST) {
		pars.push_back(oldPath);
	} else {
		NTSE_THROW(NTSE_EC_FILE_FAIL, "Partition Table .par is existed but open failed.");
	}
}

}

