#include <vector>
#include <sstream>
#include <iostream>
#ifdef WIN32
#include <my_global.h>
#include <sql_priv.h>
#include <sql_class.h>
#endif //WIN32

#include "misc/Global.h"
#include "misc/GlobalFactory.h"
#include "util/File.h"
#include "misc/ControlFile.h"
#include "misc/Syslog.h"
#include "misc/ParFileParser.h"

#ifndef WIN32
#include <my_global.h>
#include <sql_priv.h>
#include <sql_class.h>
#endif //WIN32

using namespace ntse;
//using namespace tnt;
using namespace std;

void short_usage(const char *prog) {
	cout << prog << " dataDir" << endl;
	exit(1);
}

/** ���ļ�
 * @param path �ļ�·��
 * return �ļ�ָ��
 */
ntse::File *openFile(const char *path) throw(NtseException) {
	ntse::File *file = new ntse::File(path);
	u64 errCode = file->open(false);
	if (ntse::File::E_NO_ERROR != errCode) {
		delete file;
		NTSE_THROW(errCode, "open frm file error %s", path);
	}
	return file;
}

/** ��ȡ�ļ��Ĺ̶�����
 * @param file �ļ�ָ��
 * return �ļ�����
 */
u64 getFileSize(ntse::File *file) throw(NtseException) {
	u64 len = 0;
	u64 errCode = file->getSize(&len);
	if (ntse::File::E_NO_ERROR != errCode) {
		NTSE_THROW(errCode, "getFileSize error %s", file->getPath());
	}

	return len;
}

/**�ر��ļ�
 * @param file �ļ�ָ��
 * return ������
 */
u64 closeFile(ntse::File *file) {
	u64 errCode = file->close();
	return errCode;
}

/** ��ȡ�ļ�
 * @param file �ļ�ָ��
 * @param offset �ļ�ƫ����
 * @param buffer ��ȡ���ݻ���
 * @param len �ļ�����
 */
void readFile(ntse::File *file, u64 offset, void *buffer, u32 len) throw(NtseException) {
	u64 errCode = file->read(offset, len, buffer);
	if (ntse::File::E_NO_ERROR != errCode) {
		NTSE_THROW(errCode, "read frm file error %s", file->getPath());
	}
}

/** д�ļ�
 * @param file �ļ�ָ��
 * @param offset �ļ�ƫ����
 * @param buffer д���ݻ���
 * @param len ��Ҫд�����ݳ���
 */
void writeFile(ntse::File *file, u64 offset, void *buffer, u32 len) throw(NtseException) {
	u64 errCode = file->write(offset, len, buffer);
	if (ntse::File::E_NO_ERROR != errCode) {
		NTSE_THROW(errCode, "read frm file error %s", file->getPath());
	}
} /* writefrm */


/** ����par�ļ�
 * @param name ��Ҫ������par�ļ�name
 */
void upgradeParFile(const char *name) throw (NtseException) {
	string parPath(name);
	parPath.append(".par");

	u64 err = 0;
	ntse::File *file = NULL;
	try {
		file = openFile(parPath.c_str());
	} catch (NtseException &e) {
		throw e;
	}
	uchar *data = NULL;
	uint i = 0, lenBytes, lenWords, totPartitionWords, totParts, chkSum = 0;
	/*
     File format:
     Length in words              4 byte
     Checksum                     4 byte
     Total number of partitions   4 byte
     Array of engine types        n * 4 bytes where
     n = (m_tot_parts + 3)/4
     Length of name part in bytes 4 bytes
     Name part                    m * 4 bytes where
     m = ((length_name_part + 3)/4)*4

     All padding bytes are zeroed
	*/
	//����μ�sql/ha_partition.cc��2145�У�����checksum�ļ���
	//��������ʵ�ֲ���ha_partition::get_from_handler_file
	try {
		char buffer[8];
		readFile(file, 0, buffer, 8);
		lenWords= uint4korr(buffer);
		lenBytes= 4 * lenWords;
		data = (uchar*) malloc(lenBytes*sizeof(char));
		readFile(file, 0, data, lenBytes);
		totParts= uint4korr(data + 8);
		totPartitionWords= (totParts + 3) / 4;

		for (i= 0; i < totParts; i++) {
			uchar *pLegacyDbType = (uchar *) (data + 12 + i);
			enum legacy_db_type dbType = (enum legacy_db_type)*pLegacyDbType;
			if (dbType == DB_TYPE_FIRST_DYNAMIC) {
				*pLegacyDbType = (uchar)DB_TYPE_TNT;
			}
		}

		//�ȸ�checksum�ֶθ�ֵΪ0
		int4store(data + 4, 0);
		//����checksum
		for (i= 0; i < lenWords; i++)
			chkSum^= uint4korr(data + 4 * i);
		int4store(data + 4, chkSum);
		writeFile(file, 0, data, lenBytes);
	} catch (NtseException &e) {
		closeFile(file);
		free(data);
		delete file;
		throw e;
	}

	closeFile(file);
	free(data);
	delete file;
}

/**���Ҳ��滻�ַ���
 * @param src Դ�ַ���
 * @param find ��Ҫ���ҵ��ַ���
 * @param rep ��Ҫ�滻���ַ���
 * return �滻����ַ������
 */
string findAndReplace(const char *src, const char *find, const char *rep) {
	string ret;
	string str(src);
	uint findLen = strlen(find);
	uint repLen = strlen(rep);

	int offset = 0;
	int position = str.find(find); // find first period
	while (position != string::npos ) {
		ret.append(str.substr(offset, position - offset)).append(rep);
		offset = position + findLen;
		position = str.find(find, offset);
	}
	
	ret.append(str.substr(offset, str.length() - offset));

	return ret;
}

/** ����frm��par�ļ�
 * @param ��Ҫ�����ļ���name
 */
void upgradeFrmAndParFile(const char *name) throw (NtseException) {
	string frmPath(name);
	frmPath.append(".frm");

	u64 err = 0;
	ntse::File *file = openFile(frmPath.c_str());
	u32 len = 0;
	uchar *data = NULL;

	try {
		len  = (u32)getFileSize(file);
		data = (uchar *)malloc((size_t)len * sizeof(uchar));
		readFile(file, 0, data, len);

		//frm�ļ���ʽ�μ�http://dev.mysql.com/doc/internals/en/frm-file-format.html
		uchar *pLegacyDbType = data + 3;
		uchar *pDefaultPartDbType = data  + 61;
		enum legacy_db_type legacyDbType = (enum legacy_db_type)(*pLegacyDbType);
		enum legacy_db_type defaultPartDbType = (enum legacy_db_type)(*pDefaultPartDbType);
		//����strDbType, partition_info�߼�����sql/table.cc 887�д��߼�
		ulong recLength = uint2korr((data + 16));
		ulong recOffset= (ulong) (uint2korr(data+6)+ ((uint2korr(data+14) == 0xffff ?uint4korr(data+47) : uint2korr(data+14))));
		uint extraSize = uint4korr(data + 55);
		uchar *extraBuff = data + recLength + recOffset;
		uchar *buff = extraBuff;
		size_t conStrLength = 0;
		uint strDbTypeLength = 0;
		uint movSize = 0;

		if (legacyDbType == DB_TYPE_PARTITION_DB) { //����Ƿ�����
			if (defaultPartDbType == DB_TYPE_FIRST_DYNAMIC) {
				upgradeParFile(name);
				//��ʱ�����жϱ�Ϊntse����������upgradeNtse2Tnt��throw exception��Ҳ�Ͳ���ִ�е�upgradefrm
				//����ntseû�и�ֵdb_type������Ĭ�ϵ�db_typeΪDB_TYPE_FIRST_DYNAMIC
				*pDefaultPartDbType = (uchar)DB_TYPE_TNT;

				//�޸�partition_info��Ϣ���������sql/table.cc 887�к�sql/sql_partition.cc 1952��add_engine����
				buff = extraBuff;
				conStrLength= uint2korr(buff);
				//��buff��λֵdbType
				buff += 2 + conStrLength;
				movSize += 2 + conStrLength;
				strDbTypeLength = uint2korr(buff);
				buff += 2 + strDbTypeLength;
				movSize += 2 + strDbTypeLength;

				//��ʼ����partition_info��Ϣ
				uint partInfoLen = uint4korr(buff);
				char *partInfoStr = (char *)malloc((partInfoLen+1)*sizeof(char));
				strcpy(partInfoStr, (char *)buff + 4);
				NTSE_ASSERT(partInfoLen = strlen(partInfoStr));
				char autoPartitioned = *(buff + 4 + partInfoLen + 1);
				movSize += 4/**partInfoLen*/ + (partInfoLen + 1)/**partInfoStr*/ + 1/**autoPartitioned*/;
				NTSE_ASSERT(extraSize >= movSize);
				string repStr = findAndReplace(partInfoStr, "ENGINE = Ntse", "ENGINE = Tnt");
				int reduceSize = partInfoLen - repStr.length();
				NTSE_ASSERT(reduceSize > 0);

				int4store(buff, repStr.length());
				buff += 4;
				strcpy((char *)buff, repStr.c_str());
				buff += repStr.length();
				*buff = 0;
				buff++;
				*buff = autoPartitioned;
				buff++;

				//extraSize - movSizeΪpartition_info��ʣ��extra segment�ĳ���
				memmove(buff, buff + reduceSize, extraSize - movSize);
				//extraSize - reduceSizeΪ�滻���µ�extra segment�ĳ���
				memset(extraBuff + extraSize - reduceSize, 0, reduceSize);

				int4store(data + 55, extraSize - reduceSize);
			} else {
				NTSE_THROW(NTSE_EC_GENERIC, "partition table is not ntse, partDbType is %d", defaultPartDbType);
			}
		} else if (legacyDbType == DB_TYPE_FIRST_DYNAMIC) {
			//��ʱΪntse�ķǷ�����
			*pLegacyDbType = (uchar)DB_TYPE_TNT;
			//����strDbType�߼�����sql/table.cc 887�д��߼�
			buff = extraBuff;
			conStrLength= uint2korr(buff);
			//��buff��λֵdbType
			buff += 2 + conStrLength;
			movSize += 2 + conStrLength;
			strDbTypeLength = uint2korr(buff);
			movSize += 2 + strDbTypeLength;
			LEX_STRING name;
			name.str = (char*) buff + 2;
			name.length = strDbTypeLength;
			if (name.length != 4 || System::stricmp(name.str, "ntse") != 0) {
				NTSE_THROW(NTSE_EC_GENERIC, "frm dbType is %s", name.str);
			}
			movSize += 2 + name.length;

			memset(buff + 2, 0, name.length);
			char *tnt = "Tnt";
			size_t tntLen = strlen(tnt);
			int reduceSize = name.length - tntLen;
			NTSE_ASSERT(reduceSize > 0);
			int2store(buff, tntLen);
			strcpy((char *)(buff + 2), tnt);
			buff += 2 + tntLen;
			NTSE_ASSERT(extraSize >= movSize);
			memmove(buff, buff + reduceSize, extraSize - movSize);
			memset(extraBuff + extraSize - reduceSize, 0, reduceSize);

			int4store(data + 55, extraSize - reduceSize);
		} else {
			NTSE_THROW(NTSE_EC_GENERIC, "frm show this table is not ntse, dbType is %d", legacyDbType);
		}

		writeFile(file, 0, data, len);
	} catch (NtseException &e) {
		closeFile(file);
		delete file;
		free(data);
		throw e;
	}

	closeFile(file);
	delete file;
	free(data);
}

/** ����ntse��
 * @param ��Ҫ������ntse���name
 */
void upgradeNtseTable(const char *name) throw(NtseException) {
	try {
		upgradeFrmAndParFile(name);
	} catch (NtseException &e) {
		throw e;
	}
}

/** ����ntseΪtnt
 * @param dataDir ����·��
 */
void upgradeNtse2Tnt(const char *dataDir) throw(NtseException) {
	string ctrlPath(dataDir);
	ctrlPath.append("ntse_ctrl");
	Syslog syslog(NULL, EL_LOG, true, true);
	ControlFile *ctrlFile = NULL;
	try {
		ctrlFile = ControlFile::open(ctrlPath.c_str(), &syslog, false);
	} catch (NtseException &e) {
		UNREFERENCED_PARAMETER(e);
		//��ʱ�п���ctrl�Ѿ������ɹ��ˣ���ntse��δ��ȫ������tnt
		ctrlFile = ControlFile::open(ctrlPath.c_str(), &syslog, true);
	}

	if (!ctrlFile->isCleanClosed()) {
		//���ܽ�ԭ��cleancloseΪfalse����Ϊtrue
		ctrlFile->close(false, false);
		delete ctrlFile;
		NTSE_THROW(NTSE_EC_GENERIC, "Ntse is not clean close");
	}

	u16 tableNum = ctrlFile->getNumTables();
	u16 *tableIds = (u16 *)malloc(tableNum*sizeof(tableNum));
	ctrlFile->listAllTables(tableIds, tableNum);
	set<string>  upgradeTables;
	for (u16 i = 0; i < tableNum; i++) {
		string tablePath = ctrlFile->getTablePath(tableIds[i]);
		string tableFullPath(dataDir);
		tableFullPath.append(tablePath.c_str());

		ParFileParser parFileParser(tableFullPath.c_str());
		if (parFileParser.isPartitionByLogicTabPath()) {
			tablePath.erase(tablePath.find(PARTITION_TABLE_LABLE));
			tableFullPath.clear();
			tableFullPath.append(dataDir).append(tablePath.c_str());
		}

		if (upgradeTables.find(tablePath) == upgradeTables.end()) {
			upgradeTables.insert(tablePath);
		} else {
			continue;
		}

		try {
			upgradeNtseTable(tableFullPath.c_str());
			cout << "Upgrade ntse Table(" << tableFullPath.c_str() << ") to tnt successful" << endl;
		} catch (NtseException &e) {
			if (e.getErrorCode() != NTSE_EC_FILE_NOT_EXIST) {
				fprintf(stderr, "upgrade ntse failed, reason is %s\n", e.getMessage());
			}
		}
	}
	free(tableIds);
	ctrlFile->close();
	delete ctrlFile;
}

int main(int argc, char **argv) {
	// ����
	if (argc == 2 && (0 == strcmp(argv[1], "-h") || 0 == strcmp(argv[1], "--help"))) {
		short_usage(argv[0]);
	} else if (argc != 2) {
		short_usage(argv[0]);
	}

	string dataDir(argv[1]);
	dataDir.append(NTSE_PATH_SEP);
	try {
		GlobalFactory::getInstance();
		Tracer::init();

		upgradeNtse2Tnt(dataDir.c_str());

		Tracer::exit();
		GlobalFactory::freeInstance();
	} catch (NtseException &e) {
		Tracer::exit();
		GlobalFactory::freeInstance();
		cerr << "Upgrade ntse to tnt failed. " << e.getMessage() << endl;
		exit(-1);
	}

	cout << "Upgrade ntse to tnt successful" << endl;
	return 0;
}
