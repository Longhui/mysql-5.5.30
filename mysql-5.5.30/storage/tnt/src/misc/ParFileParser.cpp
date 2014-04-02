/**
 * һ�����ܼ򵥵��ļ�������
 *
 * @author ������(hzwanggongpu@corp.netease.com)
 */

#include <string.h>
#include "misc/ParFileParser.h"
#include "util/SmartPtr.h"
#include "util/File.h"

using namespace ntse;

namespace ntse {

/** 
 * ���캯������һ�����ڽ���ָ��·�����ļ����ļ�����������
 * @param path �������ļ���·���ַ���
 */
ParFileParser::ParFileParser(const char *path) {
	m_filePath = path;
}

/** �������� */
ParFileParser::~ParFileParser() {}

/** 
 * �жϸ���������Ƿ�Ϊ������
 * @param  phyTabName ����������
 * @return ��������true���Ƿ�����false
 */
bool ParFileParser::isPartitionByPhyicTabName(const char *phyTabName) {
	assert(phyTabName);
	string tmp(phyTabName);
	return (string::npos != tmp.find(PARTITION_TABLE_LABLE));
}

/** 
 * �жϸ������������������Ӧ���߼�����
 * @param  phyTabName ����������
 * @param  logTabName OUT ������Ӧ���߼��������
 * @param  logTabNameLen �߼���logTabName�ֽ���
 * @return �ɹ�����true��ʧ�ܷ���false
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
 * �жϸ����߼���ȫ·���ж��Ƿ�Ϊ���߼���Ϊ������
 * @return �Ƿ�������true���Ƿ�������false
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
 * Ϊ��֧�ַ���ͨ������ñ��ȫ·�����ж��Ƿ����.par�ļ����������Ϊ������������Ϊ�Ƿ�����
 * ���Ϊ��������ȡ���������ƣ������з�������ȫ·������ȫ������pars����
 * ���Ϊ�Ƿ���������ԭʼȫ·�����Ķ�,ע��Ƿ�����·���ַ�������#P#��
 * @param  pars string����vector���������ӷ���ȫ·���������У���size��ʾ��������
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
			//�������ӷ�������ͨ��#P#�����߼������ƺͷ����������Ƶģ�
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

