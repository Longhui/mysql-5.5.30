/**
 * 文件操作在Windows平台上的实现
 *
 * @author 汪源(wangyuan@corp.netease.com, wy@163.org)
 */

#include "util/File.h"
#include <assert.h>
#include <limits.h>
#include <string.h>
#include "util/Thread.h"

namespace ntse {

#ifdef WIN32
#include <Windows.h>


File::File(const char *path): m_positionMutex("File::positionMutex", __FILE__, __LINE__) {
	assert(path != NULL);
	
	m_path = new char[strlen(path) + 1];
	strcpy(m_path, path);
	m_opened = false;
	m_file = INVALID_HANDLE_VALUE;
	m_size = -1;
}

File::~File() {
	if (m_opened)
		close();
	delete[] m_path;
}

u64 File::create(bool directIo, bool deleteOnClose) {
	assert(!m_opened);

	DWORD flagsAndAttributes = 0;
	if (!directIo && !deleteOnClose)
		flagsAndAttributes = FILE_ATTRIBUTE_NORMAL;
	else {
		if (deleteOnClose) {
			flagsAndAttributes |= FILE_ATTRIBUTE_TEMPORARY;
			flagsAndAttributes |= FILE_FLAG_DELETE_ON_CLOSE;
		}
		if (directIo) {
			flagsAndAttributes |= FILE_FLAG_NO_BUFFERING;
		}
	}
	m_file = ::CreateFile(
		(LPCSTR)m_path, 
		GENERIC_READ | GENERIC_WRITE,	// 打开后可以进行读写操作
		FILE_SHARE_READ,				// 只允许读
		NULL,
		CREATE_NEW, 
		flagsAndAttributes,
		NULL);
	if (m_file == INVALID_HANDLE_VALUE) {
		return translateError(::GetLastError(), 0);
	}

	m_opened = true;
	m_size = 0;
	m_directIo = directIo;

	return E_NO_ERROR;
}

u64 File::open(bool directIo) {
	assert(!m_opened);

	DWORD flagsAndAttributes = directIo? FILE_FLAG_NO_BUFFERING : FILE_ATTRIBUTE_NORMAL;
	m_file = ::CreateFile(
		(LPCSTR)m_path, 
		GENERIC_READ | GENERIC_WRITE,	// 打开后可以进行读写操作
		FILE_SHARE_READ,				// 只允许读
		NULL,
		OPEN_EXISTING, 
		flagsAndAttributes,
		NULL);
	if (m_file == INVALID_HANDLE_VALUE)
		return translateError(::GetLastError(), 0);

	m_opened = true;
	u64 size;
	u64 code = getSize(&size);
	if (code != E_NO_ERROR) {
		close();
		return code;
	}
	m_size = (s64)size;
	m_directIo = directIo;

	return E_NO_ERROR;
}

u64 File::close() {
	if (!m_opened)
		return E_NO_ERROR;

	u64 code = sync();
	if (code != E_NO_ERROR)
		return code;
	m_opened = false;
	if (::CloseHandle(m_file)) 
		return E_NO_ERROR;
	return translateError(::GetLastError(), 0);
}

u64 File::isExist(bool *exist) {
	if (m_opened) {
		*exist = true;
		return E_NO_ERROR;
	}

	if (::GetFileAttributes(m_path) == INVALID_FILE_ATTRIBUTES && (::GetLastError() == ERROR_FILE_NOT_FOUND
		|| ::GetLastError() == ERROR_PATH_NOT_FOUND))
		*exist = false;
	else
		*exist = true;
	return E_NO_ERROR;
}

u64 File::doRemove() {
	if (!::DeleteFile(m_path))
		return translateError(::GetLastError(), 0);
	return E_NO_ERROR;
}

u64 File::move(const char *newPath, bool overrideTarget) {
	if (m_opened)
		return E_IN_USE;
	DWORD flag = MOVEFILE_COPY_ALLOWED | MOVEFILE_WRITE_THROUGH;
	if (overrideTarget)
		flag |= MOVEFILE_REPLACE_EXISTING;
	if (!::MoveFileEx(m_path, newPath, flag))
		return translateError(::GetLastError(), 0);
	return E_NO_ERROR;
}

u64 File::read(u64 offset, u32 size, void *buffer) {
	assert(m_opened);
	assert(!m_directIo || (((size_t)buffer) % 512) == 0);
	assert(!m_directIo || (offset % 512) == 0);
	assert(!m_directIo || (size % 512) == 0);

	if (offset + size > (u64)m_size)
		return E_EOF;

	u32 read;
	LARGE_INTEGER liOffset;
	liOffset.QuadPart = offset;
	
	LOCK(&m_positionMutex);
	if (::SetFilePointerEx(m_file, liOffset, 0, FILE_BEGIN) != 0) {
		if (::ReadFile(m_file, buffer, (DWORD)size, (LPDWORD)&read, NULL) && read == size) {
			UNLOCK(&m_positionMutex);
			return E_NO_ERROR;
		}
	}
	UNLOCK(&m_positionMutex);
	return translateError(::GetLastError(), 1);
}

u64 File::write(u64 offset, u32 size, const void *buffer) {
	assert(m_opened);
	assert(!m_directIo || (((size_t)buffer) % 512) == 0);
	assert(!m_directIo || (offset % 512) == 0);
	assert(!m_directIo || (size % 512) == 0);

	if (offset + size > (u64)m_size)
		return E_EOF;

	u32 written;
	LARGE_INTEGER liOffset;
	liOffset.QuadPart = offset;
	
	LOCK(&m_positionMutex);
	if (::SetFilePointerEx(m_file, liOffset, NULL, FILE_BEGIN) != 0) {
		if (::WriteFile(m_file, buffer, size, (LPDWORD)&written, NULL) && written == size) {
			UNLOCK(&m_positionMutex);
			return E_NO_ERROR;
		}
	}
	UNLOCK(&m_positionMutex);
	return translateError(::GetLastError(), -1);
}

u64 File::sync() {
	assert(m_opened);

	if (::FlushFileBuffers(m_file))
		return E_NO_ERROR;
	return translateError(::GetLastError(), -1);
}

u64 File::getSize(u64 *size) {
	assert(m_opened);

	if (m_size >= 0) {
		*size = m_size;
		return E_NO_ERROR;
	}

	LARGE_INTEGER distance, liSize;
	distance.QuadPart = 0;
	LOCK(&m_positionMutex);
	if (::SetFilePointerEx(m_file, distance, &liSize, FILE_END)) {
		*size = liSize.QuadPart;
		m_size = liSize.QuadPart;
		UNLOCK(&m_positionMutex);
		return E_NO_ERROR;
	}
	UNLOCK(&m_positionMutex);
	return translateError(::GetLastError(), 0);
}

u64 File::setSize(u64 size) {
	assert(m_opened);

	LARGE_INTEGER liSize;
	liSize.QuadPart = size;

	LOCK(&m_positionMutex);
	if (::SetFilePointerEx(m_file, liSize, NULL, FILE_BEGIN) != 0) {
		if (::SetEndOfFile(m_file)) {
			m_size = size;
			UNLOCK(&m_positionMutex);
			return E_NO_ERROR;
		}
	}
	UNLOCK(&m_positionMutex);
	return translateError(::GetLastError(), 0);
}

u64 File::mkdirNonRecursive(const char *path) {
	if (::CreateDirectory(path, NULL) == 0) 
		return translateError(::GetLastError(), 0);
	return E_NO_ERROR;
}

u64 File::removeEmptyDirectory(const char *path) {
	if (::RemoveDirectory(path))
		return E_NO_ERROR;
	return translateError(::GetLastError(), 0);
}

u64 File::listFiles(list<string> *files, bool includeDirs) {
	WIN32_FIND_DATA findFileData;    
	HANDLE   findHandle;

	string dir(m_path);
	if (dir[dir.length() - 1] != '/' && dir[dir.length() - 1] != '\\')
		dir += "/";
	string findCond = dir + "*.*";
	findHandle = ::FindFirstFile(findCond.c_str(), &findFileData);
	if (INVALID_HANDLE_VALUE == findHandle)
		return translateError(::GetLastError(), 0);
	while (true) {
		if (findFileData.dwFileAttributes & FILE_ATTRIBUTE_DIRECTORY) {
			if (includeDirs && strcmp(findFileData.cFileName, ".") && strcmp(findFileData.cFileName, ".."))
				files->push_back(dir + findFileData.cFileName);
		} else
			files->push_back(dir + findFileData.cFileName);
		if (!::FindNextFile(findHandle, &findFileData)) {
			if (::GetLastError() == ERROR_NO_MORE_FILES)
				break;
			u64 code = translateError(::GetLastError(), 0);
			::FindClose(findHandle);
			return code;
		}
	}
	::FindClose(findHandle);
	return E_NO_ERROR;
}

u64 File::isDirectory(bool *isDir) {
	DWORD code = ::GetFileAttributes(m_path);
	if (code == INVALID_FILE_ATTRIBUTES)
		return translateError(::GetLastError(), 0);
	*isDir = (code & FILE_ATTRIBUTE_DIRECTORY) != 0;
	return E_NO_ERROR;
}

u64 File::copyFile(const char *destPath, const char *srcPath, bool overrideExist) {
	if (!::CopyFile(srcPath, destPath, !overrideExist))
		return translateError(::GetLastError(), 0);
	return E_NO_ERROR;
}

u64 File::translateError(u32 osErrno, int readWrite) {
	u32 ntseError;
	if (osErrno == ERROR_FILE_NOT_FOUND || osErrno == ERROR_PATH_NOT_FOUND) {
		ntseError = E_NOT_EXIST;
	} else if (osErrno == ERROR_SHARING_VIOLATION) {
		ntseError = E_IN_USE;
	} else if (osErrno == ERROR_HANDLE_DISK_FULL) {
		ntseError = E_DISK_FULL;
	} else if (osErrno == ERROR_FILE_EXISTS || osErrno == ERROR_ALREADY_EXISTS) {
		ntseError = E_EXIST;
	} else if (osErrno == ERROR_ACCESS_DENIED || osErrno == ERROR_WRITE_PROTECT) {
		ntseError = E_PERM_ERR;
	} else if (osErrno == ERROR_DIR_NOT_EMPTY) { 
		ntseError = E_NOT_EMPTY;
	} else {
		if (readWrite == 1)
			ntseError = E_READ;
		else if (readWrite == -1)
			ntseError = E_WRITE;
		else
			ntseError = E_OTHER;
	}
	return (((u64)osErrno) << 32) | ntseError;
}

#endif

/////////////////////////////////////////////////////////////////////////////
// 平台公共部分 ///////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////
u64 File::remove(int timeoutS) {
	assert(timeoutS >= 0);
	if (m_opened)
		return E_IN_USE;

	u64 before = System::currentTimeMillis();
	int sleepInterval = timeoutS * 100;
	u64 r;
	bool firstTry = true;
	while (true) {
		r = doRemove();
		if (!firstTry && getNtseError(r) == E_NOT_EXIST)
			r = E_NO_ERROR;
		if (getNtseError(r) != E_PERM_ERR)
			break;
		if (System::currentTimeMillis() >= before + (u64)timeoutS * 1000)
			break;
		Thread::msleep(sleepInterval);
		firstTry = false;
	}
	return r;
}

u64 File::mkdir() {
	assert(!m_opened);
#ifdef WIN32
	char parentPath[MAX_PATH];
#else
	char parentPath[PATH_MAX];
#endif
	size_t len = 0;
	do {
		len = strlen(m_path);
		if (m_path[len - 1] == '/' || m_path[len - 1] == '\\') {
			m_path[len - 1] = '\0';
		} else {
			break;
		}
	} while (true);

	char *pos1, *pos2;
	// 获取上一级目录
	pos1 = strrchr(m_path, '\\');
	pos2 = strrchr(m_path, '/');
	if ((pos1 != NULL) || (pos2 != NULL && pos2 != m_path)) {
		if (pos1 < pos2) {
			pos1 = pos2;
		}
		strncpy(parentPath, m_path, pos1 - m_path);
		parentPath[pos1 - m_path] = '\0';

		// 递归的创建上一级目录
		File parent(parentPath);
		u64 code = parent.mkdir();
		if (code != E_NO_ERROR && getNtseError(code) != E_EXIST)
			return code;
		
	}
	
	return mkdirNonRecursive(m_path);
}

u64 File::rmdir(bool recursive) {
	if (recursive) {
		u64 code;
		list<string> subFiles;
		if ((code = listFiles(&subFiles, true)) != E_NO_ERROR)
			return code;
		list<string>::const_iterator iter = subFiles.begin();
		for (; iter != subFiles.end(); ++iter) {
			File subFile((*iter).c_str());
			bool isDir;
			if ((code = subFile.isDirectory(&isDir)) != E_NO_ERROR)
				return code;
			if (isDir)
				code = subFile.rmdir(true);
			else
				code = subFile.remove();
			if (code != E_NO_ERROR)
				return code;
		}
	}
	return removeEmptyDirectory(m_path);
}

u64 File::isEmptyDirectory(bool *isEmptyDir) {
	bool isDir;
	u64 code = isDirectory(&isDir);
	if (code != E_NO_ERROR)
		return code;
	if (!isDir) {
		*isEmptyDir = false;
		return E_NO_ERROR;
	}
	list<string> subFiles;
	listFiles(&subFiles, true);
	*isEmptyDir = subFiles.size() == 0;
	return E_NO_ERROR;
}

u64 File::copyDir(const char *dest, const char *src, bool overrideExist, FILES_CALLBACK_FN filterFn) {
	u64 code = mkdirNonRecursive(dest);
	if (code != E_NO_ERROR && getNtseError(code) != E_EXIST)
		return code;
	
	File srcDir(src);
	list<string> subFiles;
	if ((code = srcDir.listFiles(&subFiles, true)) != E_NO_ERROR)
		return code;
	list<string>::const_iterator iter = subFiles.begin();
	for (; iter != subFiles.end(); ++iter) {
		File subFile((*iter).c_str());
		bool isDir;
		if ((code = subFile.isDirectory(&isDir)) != E_NO_ERROR)
			return code;
		string name = getNameFromPath(*iter);
		if (filterFn && !(*filterFn)(subFile.m_path, name.c_str(), isDir))
			continue;
		string target = string(dest) + "/" + name;
		if (isDir)
			code = copyDir(target.c_str(), subFile.m_path, overrideExist, filterFn);
		else
			code = copyFile(target.c_str(), subFile.m_path, overrideExist);
		if (code != E_NO_ERROR)
			return code;
	}
	return E_NO_ERROR;
}

string File::getNameFromPath(const string &path) {
	int sepPos = path.length() - 1;
	while (sepPos >= 0) {
		char ch = path[sepPos];
		if (ch == '/' || ch == '\\')
			break;
		sepPos--;
	}
	return path.substr(sepPos + 1);
}

}
