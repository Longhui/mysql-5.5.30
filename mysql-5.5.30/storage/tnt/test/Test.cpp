#include <string.h>
#include <assert.h>
#include "Test.h"
#include "util/File.h"
#include "util/SmartPtr.h"
#include "util/System.h"

/** 比较两段内存
 * @param buf1 内存1
 * @param buf2 内存2
 * @param size 大小
 * @return 相同返回-1，不同返回不同的字节位置
 */
static int compareMem(const byte *buf1, const byte *buf2, int size) {
	for (int i = 0; i < size; i++)
		if (buf1[i] != buf2[i])
			return i;
	return -1;
}

/**
 * 比较文件数据
 * @param file1 第一个文件
 * @param file2 第二个文件
 * @param start 起始偏移
 * @param len 比较长度
 * @param ignoreLsn 是否忽略比较LSN
 * @return 文件相同返回true；否则返回false
 */
bool compareFile(File *file1, File *file2, u64 start, u64 len, bool ignoreLsn) throw(NtseException) {
	u64 size1;
	u64 errNo = file1->getSize(&size1);
	if (errNo != File::E_NO_ERROR)
		NTSE_THROW(errNo, "getSize on file %s failed", file1->getPath());
	u64 size2;
	errNo = file2->getSize(&size2);
	if (errNo != File::E_NO_ERROR)
		NTSE_THROW(errNo, "getSize on file %s failed", file2->getPath());
	if (size1 != size2)
		return false;

	assert(start < size1);
	if (ignoreLsn) { // 检查参数有效性
		assert(start % Limits::PAGE_SIZE == 0 && size1 % Limits::PAGE_SIZE == 0);
		if (size1 - start >= len)
			assert(len % Limits::PAGE_SIZE == 0);
	}
	
	u64 end = min(len, size1 - start);

	u32 bufSize = 256 * Limits::PAGE_SIZE;
	byte *buf1 = (byte *)System::virtualAlloc(bufSize);
	byte *buf2 = (byte *)System::virtualAlloc(bufSize);
	for (u64 offset = start; offset < end;) {
		u32 curSize = min((u32)(end - offset), bufSize);
		if ((errNo = file1->read(offset, curSize, buf1)) != File::E_NO_ERROR)
			NTSE_THROW(errNo, "read file %s failed", file1->getPath());
		if ((errNo = file2->read(offset, curSize, buf2)) != File::E_NO_ERROR)
			NTSE_THROW(errNo, "read file %s failed", file2->getPath());
		if (!ignoreLsn) {
			int diffPos = compareMem(buf1, buf2, curSize);
			if (diffPos >= 0)
				return false;
		} else { // 忽略LSN比较
			for (u32 i = 0; (i + Limits::PAGE_SIZE) <= curSize; i += Limits::PAGE_SIZE) {
				int diffPos = compareMem(buf1 + i + 8, buf2 + i + 8, Limits::PAGE_SIZE - 8);
				if (diffPos >= 0)
					return false;
			}
		}
		offset += curSize;
	}
	System::virtualFree(buf1);
	System::virtualFree(buf2);
	return true;
}


bool compareFile(const char *f1, const char *f2) throw(NtseException) {
	File file1(f1);
	File file2(f2);
	bool res = false;
	try {
		u64 errNo = file1.open(false);
		if (errNo != File::E_NO_ERROR)
			NTSE_THROW(errNo, "open file %s failed", f1);
		if ((errNo = file2.open(false)) != File::E_NO_ERROR)
			NTSE_THROW(errNo, "open file %s failed", f2);
		res = compareFile(&file1, &file2);
	} catch(NtseException &e) {
		throw e;
		file1.close();
		file2.close();
	}
	file1.close();
	file2.close();
	return res;
}

/**
 * 生成指定长度的随机字符串
 *
 * @return 字符串，使用new分配内存
 */
char* randomStr(size_t size) {
	char *s = new char[size + 1];
	for (size_t i = 0; i < size; i++)
		s[i] = (char )('A' + System::random() % 26);
	s[size] = '\0';
	return s;
}

bool essentialOnly = false;

/** 返回是否只运行核心单元测试 */
bool isEssentialOnly() {
	return essentialOnly;
}

/** 设置是否只运行核心单元测试 */
void setEssentialOnly(bool v) {
	essentialOnly = v;
}
