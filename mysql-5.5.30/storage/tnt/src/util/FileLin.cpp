/**
 * 文件操作在Linux平台上的实现
 *
 * @author 汪源(wangyuan@corp.netease.com, wy@163.org)
 */

#ifndef WIN32

#include "util/File.h"
#include <fcntl.h>
#include <unistd.h>
#include <string.h>
#include <errno.h>
#include <assert.h>
#include <stdio.h>
#include <sys/stat.h>
#include <dirent.h>
#include <sys/syscall.h>
#include <linux/aio_abi.h>
#include "misc/Trace.h"

namespace ntse {

File::File(const char *path) {
	assert(path != NULL);
	
	m_path = new char[strlen(path) + 1];
	strcpy(m_path, path);
	m_opened = false;
	m_file = -1;
	m_size = -1;
}

File::~File() {
	if (m_opened)
		close();
	delete[] m_path;
}

u64 File::create(bool directIo, bool deleteOnClose) {
	assert(!m_opened);
	ftrace(ts.file, tout << this << directIo << deleteOnClose);

	int flags = O_RDWR | O_CREAT | O_EXCL;
	m_file = open64(m_path, flags, 00640);
	if (m_file == -1)
		return translateError(errno, 0);
	if (directIo) {
		if (fcntl(m_file, F_SETFL, O_DIRECT) == -1)
			return translateError(errno, 0);
	}

	m_deleteOnClose = deleteOnClose;
	m_opened = true;
	m_size = 0;
	m_directIo = directIo;

	return E_NO_ERROR;
}

u64 File::open(bool directIo) {
	assert(!m_opened);
	ftrace(ts.file, tout << this << directIo);

	int flags = O_RDWR;
	m_file = open64(m_path, flags);
	if (m_file == -1)
		return translateError(errno, 0);
	if (directIo) {
		if (fcntl(m_file, F_SETFL, O_DIRECT) == -1)
			return translateError(errno, 0);
	}
	struct flock lock;
	lock.l_type = F_WRLCK;
	lock.l_start = 0;
	lock.l_whence = SEEK_SET;
	lock.l_len = 0;
	if (fcntl(m_file, F_SETLK, &lock) < 0) {
		::close(m_file);
		return translateError(errno, 0);
	}

	m_deleteOnClose = false;
	m_opened = true;
	u64 size = 0;
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
	ftrace(ts.file, tout << this);
	if (!m_opened)
		return E_NO_ERROR;

	u64 code = sync();
	if (code != E_NO_ERROR)
		return code;
	m_opened = false;
	if (::close(m_file) != 0) 
		return translateError(errno, 0);
	if (m_deleteOnClose)
		return remove();
	return E_NO_ERROR;
}

u64 File::isExist(bool *exist) {
	*exist = access(m_path, 0) == 0;
	return E_NO_ERROR;
}

u64 File::doRemove() {
	if (unlink(m_path) == -1) 
		return translateError(errno, 0);
	return E_NO_ERROR;
}

u64 File::move(const char *newPath, bool overrideTarget) {
	ftrace(ts.file, tout << this << newPath << overrideTarget);
	if (m_opened) {
		nftrace(ts.file, tout << "ERROR: Used by myself");
		return E_IN_USE;
	}
	File newFile(newPath);
	bool exist = false;
	newFile.isExist(&exist);
	if (exist) {
		if (!overrideTarget) {
			nftrace(ts.file, tout << "ERROR: Target exists");
			return E_EXIST;
		}
		newFile.remove();
	}
	if (rename(m_path, newPath) == -1)
		return translateError(errno, 0);
	return E_NO_ERROR;
}

u64 File::read(u64 offset, u32 size, void *buffer) {
	assert(m_opened);
	assert(!m_directIo || (((size_t)buffer) % 512) == 0);
	assert(!m_directIo || (offset % 512) == 0);
	assert(!m_directIo || (size % 512) == 0);
	ftrace(ts.file, tout << this << offset << size);

	if (offset + size > (u64)m_size) {
		nftrace(ts.file, tout << "ERROR: Out of file size range: " << m_size);
		return E_EOF;
	}

	if (pread64(m_file, buffer, size, offset) != size)
		return translateError(errno, 1);
	nftrace(ts.file, tout << barr(size, buffer));
	return E_NO_ERROR;
}

u64 File::write(u64 offset, u32 size, const void *buffer) {
	assert(m_opened);
	assert(!m_directIo || (((size_t)buffer) % 512) == 0);
	assert(!m_directIo || (offset % 512) == 0);
	assert(!m_directIo || (size % 512) == 0);
	ftrace(ts.file, tout << this << offset << size << barr(size, buffer));

	if (offset + size > (u64)m_size) {
		nftrace(ts.file, tout << "ERROR: Out of file size range: " << m_size);
		return E_EOF;
	}

	if (pwrite64(m_file, buffer, size, offset) != size)
		return translateError(errno, -1);
	return E_NO_ERROR;
}

u64 File::sync() {
	assert(m_opened);
	ftrace(ts.file, tout << this);

	if (fsync(m_file) == -1)
		return translateError(errno, -1);
    return E_NO_ERROR;
}

u64 File::getSize(u64 *size) {
	assert(m_opened);
	
	if (m_size >= 0) {
		*size = m_size;
		return E_NO_ERROR;
	}

	s64 sksize = lseek(m_file, 0, SEEK_END);
	if (sksize == -1) {
		return translateError(errno, 0);
	} else {
		*size = (u64)sksize;
	}
		
	m_size = *size;
	return E_NO_ERROR;
}

u64 File::setSize(u64 size) {
	assert(m_opened);
	ftrace(ts.file, tout << this << size);

	if (ftruncate64(m_file, size) == -1)
		return translateError(errno, 0);
	m_size = size;
	return E_NO_ERROR;
}

u64 File::mkdirNonRecursive(const char *path) {
	if (::mkdir(path, 0777) != 0) 
		return translateError(errno, 0);
	return E_NO_ERROR;
}

u64 File::removeEmptyDirectory(const char *path) {
	if (::rmdir(path) != 0)
		return translateError(errno, 0);
	return E_NO_ERROR;
}

u64 File::listFiles(list<string> *files, bool includeDirs) {
	DIR *dp;
	struct dirent *ep = NULL;

	string dir(m_path);
	if (dir[dir.length() - 1] != '/' && dir[dir.length() - 1] != '\\')
		dir += "/";
	dp = opendir(m_path);
	if (!dp)
		return translateError(errno, 0);
	ep = readdir(dp);
	while (ep) {
		string subPath = dir + ep->d_name;
		File subFile(subPath.c_str());
		bool isDir = false;
		if (subFile.isDirectory(&isDir) != E_NO_ERROR) {
			closedir(dp);
			return translateError(errno, 0);
		}
		if (isDir) {
			if (includeDirs && strcmp(ep->d_name, ".") && strcmp(ep->d_name, ".."))
				files->push_back(dir + ep->d_name);
		} else
			files->push_back(dir + ep->d_name);
		ep = readdir(dp);
	}
	closedir(dp);
	return E_NO_ERROR;
}

u64 File::isDirectory(bool *isDir) {
	struct stat fileStat;
	if (stat(m_path, &fileStat) != 0)
		return translateError(errno, 0);
	*isDir = S_ISDIR(fileStat.st_mode) != 0;
	return E_NO_ERROR;
}

u64 File::copyFile(const char *destPath, const char *srcPath, bool overrideExist) {
	ftrace(ts.file, tout << destPath << srcPath << overrideExist);
	File srcFile(srcPath);
	File destFile(destPath);

	bool destExist;
	u64 code;
	if ((code = destFile.isExist(&destExist)) != E_NO_ERROR)
		return code;
	if (destExist && !overrideExist) {
		nftrace(ts.file, tout << "ERROR: Target exists");
		return E_EXIST;
	}
	if ((code = srcFile.open(false)) != E_NO_ERROR)
		return code;
	if (destExist) {
		if ((code = destFile.open(false)) != E_NO_ERROR) {
			srcFile.close();
			return code;
		}
	} else {
		if ((code = destFile.create(false, false)) != E_NO_ERROR) {
			srcFile.close();
			return code;
		}
	}
	char buf[8192];
	u64 size = 0;
	srcFile.getSize(&size);
	if ((code = destFile.setSize(size)) != E_NO_ERROR) {
		srcFile.close();
		destFile.close();
		return code;
	}
	for (u64 offset = 0; offset < size; offset += sizeof(buf)) {
		u32 readSize = sizeof(buf);
		if (offset + readSize > size)
			readSize = (u32)(size - offset);
		if ((code = srcFile.read(offset, readSize, buf)) != E_NO_ERROR) {
			srcFile.close();
			destFile.close();
			return code;
		}
		if ((code = destFile.write(offset, readSize, buf)) != E_NO_ERROR) {
			srcFile.close();
			destFile.close();
			return code;
		}
	}
	srcFile.close();
	destFile.close();
	return E_NO_ERROR;
}

u64 File::translateError(u32 osErrno, int readWrite) {
	ftrace(ts.file, tout << osErrno << readWrite);
	u32 ntseError;
	if (osErrno == ENOENT) {
		ntseError = E_NOT_EXIST;
	} else if (osErrno == ENOSPC) {
		ntseError = E_DISK_FULL;
	} else if (osErrno == EEXIST) {
		ntseError = E_EXIST;
	} else if (osErrno == EACCES || osErrno == EROFS) {
		ntseError = E_PERM_ERR;
	} else if (osErrno == ENOTEMPTY) {
		ntseError = E_NOT_EMPTY;
	} else if (osErrno == EIO) {
		if (readWrite == 1)
			ntseError = E_READ;
		else if (readWrite == -1)
			ntseError = E_WRITE;
		else
			ntseError = E_OTHER;
	} else if (osErrno == EAGAIN) {
		ntseError = E_AIO_RESOURCE_NOT_ENOUGH;
	} else if (osErrno == EFAULT) {
		ntseError = E_AIO_DATA_INVALID;
	} else if (osErrno == EINVAL) {
		ntseError = E_AIO_ARGS_INVALID;
	} else if (osErrno == ENOMEM) {
		ntseError = E_AIO_KERNEL_RESOUCE_NOT_ENOUGH;
	} else if (osErrno == ENOSYS) {
		ntseError = E_AIO_SYSTEM_NOT_SUPPORT;
	} else if (osErrno == EBADF) {
		ntseError = E_AIO_FILE_INVALID;
	} else if (osErrno == EINTR) {
		ntseError = E_AIO_INTERUPTED;
	} else {
		ntseError = E_OTHER;
	}
	return (((u64)osErrno) << 32) | ntseError;
}




AioSlot::AioSlot() {
	m_control = new struct iocb();
}


AioSlot::~AioSlot() {
	delete m_control;
}


AioArray::AioArray() {
	m_slots = new AioSlot[AIO_BATCH_SIZE];
	m_aioEvents = new io_event[AIO_BATCH_SIZE];
	m_cbs = new iocb*[AIO_BATCH_SIZE];
}

AioArray::~AioArray() {
	delete []m_slots;
	delete []m_aioEvents;
	delete []m_cbs;
}

void AioArray::fillAioHandlerRead(struct iocb *iocb, File *file, const void *buffer, u32 size, u64 offset) {
	io_prep_pread(iocb, file->m_file, buffer, size, offset);
}


void AioArray::fillAioHandlerWrite(struct iocb *iocb, File *file, const void *buffer, u32 size, u64 offset) {
	io_prep_pwrite(iocb, file->m_file, buffer, size, offset);
}


AioSlot* AioArray::getSlotFromEvent(u32 index) {
	return (AioSlot*)((struct iocb*)m_aioEvents[index].obj)->aio_data;
}

void* AioArray::getDataFromEvent(u32 index) {
	return (void*)((struct iocb*)m_aioEvents[index].obj)->aio_buf;
}


void AioArray::io_prep_pread(struct iocb *iocb, int fd, const void *buf, size_t count, long long offset) {
	memset(iocb, 0, sizeof(*iocb));
	iocb->aio_fildes = fd;
	iocb->aio_lio_opcode = IOCB_CMD_PREAD;
	iocb->aio_buf = (u64)buf;
	iocb->aio_offset = offset;
	iocb->aio_nbytes = count;
}


void AioArray::io_prep_pwrite(struct iocb *iocb, int fd, const void *buf, size_t count, long long offset) {
	memset(iocb, 0, sizeof(*iocb));
	iocb->aio_fildes = fd;
	iocb->aio_lio_opcode = IOCB_CMD_PWRITE;
	iocb->aio_buf = (u64)buf;
	iocb->aio_offset = offset;
	iocb->aio_nbytes = count;
}


int AioArray::io_setup(unsigned nr, aio_context_t *ctxp){
	return syscall(__NR_io_setup, nr, ctxp);
}


int AioArray::io_submit(aio_context_t ctx, long nr,  struct iocb **iocbpp){
	return syscall(__NR_io_submit, ctx, nr, iocbpp);
}


int AioArray::io_getevents(aio_context_t ctx, long min_nr, long max_nr,
struct io_event *events, struct timespec *timeout){
	return syscall(__NR_io_getevents, ctx, min_nr, max_nr, events, timeout);
}


int AioArray::io_destroy(aio_context_t ctx){
	return syscall(__NR_io_destroy, ctx);
}



u32  AioArray::getReservedSlotNum() {
	return m_numReserved;
}


u64 AioArray::aioInit() {
	memset(&m_ctx, 0, sizeof(m_ctx));  
	m_numReserved = 0;
	int ret = io_setup(AIO_BATCH_SIZE, &m_ctx);
	if (ret != 0) {
		return File::translateError(-ret, 0);
	}

	for(u32 i = 0; i < AIO_BATCH_SIZE; i++) {
		m_slots[i].m_indexNum = i;
		m_slots[i].m_isReserved = false;
//		m_cbs[i] = m_slots[i].m_control;
		memset((m_slots[i].m_control), 0, sizeof(*(m_slots[i].m_control)));
	}
	return File::E_NO_ERROR;
}

AioSlot* AioArray::aioReserveSlot(AioOpType type, File *file, void *buf, u64 offset, u32 size, void *data) {
	AioSlot *slot = NULL;
	struct iocb *iocb;
	assert(m_numReserved != AIO_BATCH_SIZE);

	for(u32 i = 0; i < AioArray::AIO_BATCH_SIZE; i++) {
		slot = &(m_slots[i]);
		if (slot->m_isReserved == false)
			goto Found;
	}
	return NULL;
Found:
	m_numReserved++;

	// 填充Aio槽信息
	slot->m_isReserved = true;
	slot->m_file = file;
	slot->m_len = size;
	slot->m_opType = type;
	slot->m_buffer = (byte*)buf;
	slot->m_offset = offset;
	slot->m_data = data;

	iocb = slot->m_control;
	if(type == AIO_READ) {
		fillAioHandlerRead(iocb, file, buf, size, offset);
	} else {
		fillAioHandlerWrite(iocb, file, buf, size, offset);
	}

	iocb->aio_data = (u64)slot;

	return slot;
}

u64 AioArray::aioDispatchGroup(u32 number){
	int ret = io_submit(m_ctx, number, m_cbs);
	if(ret != number)
		return File::translateError(-ret, 0);
	return File::E_NO_ERROR;
}


u64 AioArray::aioDispatch(AioSlot *slot){
	struct iocb *iocb;
	assert(slot->m_isReserved);

	if (slot->m_offset + slot->m_len > (u64)slot->m_file->m_size) {
		nftrace(ts.file, tout << "ERROR: Out of file size range: " << slot->m_file->m_size);
		return File::E_EOF;
	}
	iocb = slot->m_control;
	int ret = io_submit(m_ctx, 1, &iocb);
	if(ret != 1)
		return File::translateError(-ret, 0);
	return File::E_NO_ERROR;
}

void AioArray::aioFreeSlot(AioSlot *slot) {
	memset(slot->m_control, 0, sizeof(*(slot->m_control)));
	slot->m_isReserved = false;
	m_numReserved--;
}

u64 AioArray::aioWaitFinish(u32 minRequestCnt, u32 *numIoComplete){
	struct io_event *events = m_aioEvents;
	u32 needWaitIoNum = minRequestCnt;
	u32 base = 0;
	u32 totalIoComplete = 0;
	if(m_numReserved == 0)
		return File::E_NO_ERROR;
	
	memset(events, 0, sizeof(*events) * AIO_BATCH_SIZE);

	if(minRequestCnt > m_numReserved)
		return File::E_AIO_ARGS_INVALID;
	while(needWaitIoNum > 0) {
		int ret = io_getevents(m_ctx, needWaitIoNum, AIO_BATCH_SIZE - base, events + base, NULL);
		if (ret > 0) {
			totalIoComplete += ret;
			if(needWaitIoNum >= ret)
				needWaitIoNum -= ret;
			else
				needWaitIoNum = 0;

			for(int i = 0; i < ret; i++) {
				struct iocb *control = (struct iocb*)events[i + base].obj;
				assert(control != NULL);
				AioSlot *slot = (AioSlot*)control->aio_data;
				assert(slot != NULL && slot->m_isReserved);
				assert(slot->m_len == events[i + base].res && 0 == events[i + base].res2);
			}
			base += ret;
		} else{
			switch (ret) {
				case -EAGAIN:
				case -EINTR:
				case 0:
					continue;
			}
			return File::translateError(-ret, 0);
		}
	}
	*numIoComplete = totalIoComplete;
	assert(totalIoComplete <= AIO_BATCH_SIZE);
	return File::E_NO_ERROR;
}


u64 AioArray::aioDeInit(){
	int ret = io_destroy(m_ctx);
	if(ret != 0)
		return File::translateError(-ret, 0);
	return File::E_NO_ERROR;
}

}

#endif
