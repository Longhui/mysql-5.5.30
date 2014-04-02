/**
 * 测试文件操作
 *
 * @author 汪源(wangyuan@corp.netease.com, wy@163.org)
 */

#include <cppunit/config/SourcePrefix.h>
#include "util/TestFile.h"
#include "util/System.h"
#include "Test.h"
#include <iostream>

using namespace std;

const char* FileTestCase::getName() {
	return "File test";
}

const char* FileTestCase::getDescription() {
	return "Test file operations.";
}

bool FileTestCase::isBig() {
	return false;
}

void FileTestCase::setUp() {
	File f("ftest");
	f.remove();
	File f2("test2");
	f2.remove();
	File dir("testdir");
	dir.rmdir(true);
	File dir2("testdir2");
	dir2.rmdir(true);
}

void FileTestCase::tearDown() {
	File f("ftest");
	f.remove();
	File f2("test2");
	f2.remove();
	File dir("testdir");
	dir.rmdir(true);
	File dir2("testdir2");
	dir2.rmdir(true);
}

/** 测试创建、打开、删除等基本文件操作 */
void FileTestCase::testBasic() {
	File *f = new File("ftest");
	
	bool exist;
	f->isExist(&exist);
	CPPUNIT_ASSERT(!exist);
	CPPUNIT_ASSERT(strcmp(f->getPath(), "ftest") == 0);

	// 创建文件
	u64 code;
	code = f->create(true, false);
	CPPUNIT_ASSERT(code == File::E_NO_ERROR);
	f->isExist(&exist);	// 文件已经打开时判断文件是否存在
	CPPUNIT_ASSERT(exist);
	
	// 打开与关闭
	code = f->close();
	CPPUNIT_ASSERT(code == File::E_NO_ERROR);
	f->isExist(&exist);	// 文件没有打开时判断文件是否存在
	CPPUNIT_ASSERT(exist);

	// 关闭已经关闭的文件
	code = f->close();
	CPPUNIT_ASSERT(code == File::E_NO_ERROR);

	// 再次创建同一文件，验证创建会失败，且返回正确的错误码
	File f2("ftest");
	code = f2.create(true, false);
	CPPUNIT_ASSERT(File::getNtseError(code) == File::E_EXIST);

	code = f->open(true);
	CPPUNIT_ASSERT(code == File::E_NO_ERROR);

	// 打开已经被别人打开的文件
	code = f2.open(true);
#ifdef WIN32
	CPPUNIT_ASSERT(File::getNtseError(code) == File::E_IN_USE);
#else
	CPPUNIT_ASSERT(code == File::E_NO_ERROR);
#endif

	// 删除
	code = f->remove();
	CPPUNIT_ASSERT(File::getNtseError(code) == File::E_IN_USE);
	f->close();
	code = f->remove();
	CPPUNIT_ASSERT(code == File::E_NO_ERROR);
	f->isExist(&exist);
	CPPUNIT_ASSERT(!exist);

	// 删除不存在的文件
	code = f->remove();
	CPPUNIT_ASSERT(File::getNtseError(code) == File::E_NOT_EXIST);

	// 打开不存在的文件
	code = f->open(true);
	CPPUNIT_ASSERT(File::getNtseError(code) == File::E_NOT_EXIST);

	delete f;

	// 测试explainErrno
	CPPUNIT_ASSERT(!strcmp(File::explainErrno(File::E_NO_ERROR), "no error"));
	CPPUNIT_ASSERT(strcmp(File::explainErrno(File::E_DISK_FULL), "other reasons"));
	CPPUNIT_ASSERT(strcmp(File::explainErrno(File::E_EOF), "other reasons"));
	CPPUNIT_ASSERT(strcmp(File::explainErrno(File::E_EXIST), "other reasons"));
	CPPUNIT_ASSERT(strcmp(File::explainErrno(File::E_IN_USE), "other reasons"));
	CPPUNIT_ASSERT(strcmp(File::explainErrno(File::E_NOT_EXIST), "other reasons"));
	CPPUNIT_ASSERT(!strcmp(File::explainErrno(File::E_OTHER), "other reasons"));
	CPPUNIT_ASSERT(strcmp(File::explainErrno(File::E_PERM_ERR), "other reasons"));
	CPPUNIT_ASSERT(strcmp(File::explainErrno(File::E_READ), "other reasons"));
	CPPUNIT_ASSERT(strcmp(File::explainErrno(File::E_WRITE), "other reasons"));

	// 测试错误码
	CPPUNIT_ASSERT(File::getOsError(1LL << 32) == 1);
}

/** 测试关闭时自动删除的临时文件 */
void FileTestCase::testAutoDelete() {
	File f("ftest");
	
	u64 code = f.create(true, true);
	CPPUNIT_ASSERT(code == File::E_NO_ERROR);

	f.close();
	bool exist;
	f.isExist(&exist);
	CPPUNIT_ASSERT(!exist);
}

/** 测试重命名 */
void FileTestCase::testRename() {
	File f("ftest");
	File f2("test2");

	u64 code = f.create(true, false);
	CPPUNIT_ASSERT(code == File::E_NO_ERROR);

	// 未关闭就重命名会失败
	code = f.move("test2");
	CPPUNIT_ASSERT(File::getNtseError(code) == File::E_IN_USE);

	f.close();

	bool exist;
	f2.isExist(&exist);
	CPPUNIT_ASSERT(!exist);

	f.move("test2");
	f2.isExist(&exist);
	CPPUNIT_ASSERT(exist);
	CPPUNIT_ASSERT(!strcmp(f.getPath(), "ftest"));

	code = f.create(true, false);
	CPPUNIT_ASSERT(code == File::E_NO_ERROR);
	f.close();

	// 不覆盖模式，目标文件存在时失败
	code = f.move("test2");
	CPPUNIT_ASSERT(File::getNtseError(code) == File::E_EXIST);

	// 覆盖模式，目标文件存在时成功
	code = f.move("test2", true);
	CPPUNIT_ASSERT(code == File::E_NO_ERROR);
}

/** 测试得到或修改文件大小操作 */
void FileTestCase::testSetSize() {
	File f("ftest");
	f.create(true, false);

	u64 size;
	u64 code = f.getSize(&size);
	CPPUNIT_ASSERT(code == File::E_NO_ERROR);
	CPPUNIT_ASSERT(size == 0);

	code = f.setSize(1024);
	CPPUNIT_ASSERT(code == File::E_NO_ERROR);

	code = f.getSize(&size);
	CPPUNIT_ASSERT(code == File::E_NO_ERROR);
	CPPUNIT_ASSERT(size == 1024);
}

/** 测试读写操作 */
void FileTestCase::testReadWrite() {
	// 测试非DIRECT_IO读写
	File f("ftest");
	f.create(false, false);
	
	f.setSize(2048);
	char *buf1 = new char[1024];
	char *buf2 = new char[1024];
	memset(buf1, 0, 1024);
	memset(buf2, 0, 1024);

	doReadWriteTest(&f, buf1, buf2);

	delete [] buf1;
	delete [] buf2;
	f.close();
	f.remove();

	// 测试DIRECT_IO读写
	File f2("ftest");
	f2.create(true, false);
	f2.setSize(2048);
	buf1 = (char *)System::virtualAlloc(1024);
	buf2 = (char *)System::virtualAlloc(1024);
	memset(buf1, 0, 1024);
	memset(buf2, 0, 1024);

	doReadWriteTest(&f2, buf1, buf2);

	System::virtualFree(buf1);
	System::virtualFree(buf2);
	f2.close();
	f2.remove();
}

void FileTestCase::doReadWriteTest(File *file, char *buf1, char *buf2) {
	for (int i = 0; i < sizeof(buf1); i++)
		buf1[i] = 'x';
	for (int i = 0; i < sizeof(buf2); i++)
		buf2[i] = 'y';

	u64 code = file->write(0, 1024, buf1);
	CPPUNIT_ASSERT(code == File::E_NO_ERROR);
	code = file->write(1024, 1024, buf2);
	CPPUNIT_ASSERT(code == File::E_NO_ERROR);

	code = file->read(0, 1024, buf2);
	CPPUNIT_ASSERT(code == File::E_NO_ERROR);
	CPPUNIT_ASSERT(buf2[0] == 'x');

	code = file->read(1024, 1024, buf1);
	CPPUNIT_ASSERT(code == File::E_NO_ERROR);
	CPPUNIT_ASSERT(buf1[0] == 'y');

	// 越界读
	code = file->read(2048, 1024, buf1);
	CPPUNIT_ASSERT(File::getNtseError(code) == File::E_EOF);
	// 越界写
	code = file->write(2048, 1024, buf1);
	CPPUNIT_ASSERT(File::getNtseError(code) == File::E_EOF);

	code = file->sync();
	CPPUNIT_ASSERT(code == File::E_NO_ERROR);
}

/**
 * 测试异步IO写文件
 */
void FileTestCase::testAioWrite() {
#ifndef WIN32
	File f("ftest");
	f.create(true, true);

	f.setSize(2048);
	char *buf1, *buf2;  
	buf1 = (char*)memalign(512, 1024);
	buf2 = (char*)memalign(512, 1024);
	memset(buf1, 0, 1024);
	memset(buf2, 0, 1024);

	for (int i = 0; i < sizeof(buf1); i++)
		buf1[i] = 'x';
	for (int i = 0; i < sizeof(buf2); i++)
		buf2[i] = 'y';

	AioArray aioArray;
	// 测试AIOSetup
	u64 errCode = aioArray.aioInit();
	CPPUNIT_ASSERT(errCode == File::E_NO_ERROR);

	// 构建写请求
	AioSlot *slot1 = aioArray.aioReserveSlot(AIO_WRITE, &f, buf1, 
		0, 1024, NULL);

	errCode = aioArray.aioDispatch(slot1);

	CPPUNIT_ASSERT(errCode == File::E_NO_ERROR);


	// 测试越界写
	AioSlot *slot2 = aioArray.aioReserveSlot(AIO_WRITE, &f, buf2,
		2048, 1024, NULL);
	errCode = aioArray.aioDispatch(slot2);
	CPPUNIT_ASSERT(errCode == File::E_EOF);
	aioArray.aioFreeSlot(slot2);

	// 测试等待异步IO写
	uint numIoComplete = 0;
	// 测试等待IO数量超过实际请求数量
	errCode = aioArray.aioWaitFinish(2, &numIoComplete);
	CPPUNIT_ASSERT(errCode == File::E_AIO_ARGS_INVALID);

	// 等待请求完成
	errCode = aioArray.aioWaitFinish(1, &numIoComplete);
	CPPUNIT_ASSERT(errCode == File::E_NO_ERROR);


	aioArray.aioDeInit();

	System::virtualFree(buf1);
	System::virtualFree(buf2);
	f.close();
	f.remove();
#endif
}


/**
 * 测试目录操作，包括递归创建子目录，枚举目录文件，删除空目录与递归删除目录等
 */
void FileTestCase::testDir() {
	bool exist;

	File dir("testdir");
	CPPUNIT_ASSERT(dir.isExist(&exist) == File::E_NO_ERROR);
	CPPUNIT_ASSERT(!exist);

	File subDir1("testdir/sub1");
	CPPUNIT_ASSERT(subDir1.isExist(&exist) == File::E_NO_ERROR);
	CPPUNIT_ASSERT(!exist);

	File subDir2("testdir/sub2");
	CPPUNIT_ASSERT(subDir2.isExist(&exist) == File::E_NO_ERROR);
	CPPUNIT_ASSERT(!exist);

	File subFile("testdir/a.txt");
	CPPUNIT_ASSERT(subFile.isExist(&exist) == File::E_NO_ERROR);
	CPPUNIT_ASSERT(!exist);

	// 递归创建目录
	u64 code = subDir1.mkdir();
	CPPUNIT_ASSERT(code == File::E_NO_ERROR);
	CPPUNIT_ASSERT(dir.isExist(&exist) == File::E_NO_ERROR);
	CPPUNIT_ASSERT(exist);
	CPPUNIT_ASSERT(subDir1.isExist(&exist) == File::E_NO_ERROR);
	CPPUNIT_ASSERT(exist);

	code = subDir2.mkdir();
	CPPUNIT_ASSERT(code == File::E_NO_ERROR);
	CPPUNIT_ASSERT(subDir2.isExist(&exist) == File::E_NO_ERROR);
	CPPUNIT_ASSERT(exist);

	// 创建一个已经存储的目录
	code = dir.mkdir();
	CPPUNIT_ASSERT(File::getNtseError(code) == File::E_EXIST);

	subFile.create(false, false);
	CPPUNIT_ASSERT(subFile.isExist(&exist) == File::E_NO_ERROR);
	CPPUNIT_ASSERT(exist);
	subFile.close();

	// 测试文件枚举功能
	list<string> subFiles;
	CPPUNIT_ASSERT(dir.listFiles(&subFiles, false) == File::E_NO_ERROR);
	CPPUNIT_ASSERT(subFiles.size() == 1);
	subFiles.clear();
	CPPUNIT_ASSERT(dir.listFiles(&subFiles, true) == File::E_NO_ERROR);
	CPPUNIT_ASSERT(subFiles.size() == 3);

	// 非递归删除非空目录将失败
	code = dir.rmdir(false);
	CPPUNIT_ASSERT(File::getNtseError(code) == File::E_NOT_EMPTY);
	dir.isExist(&exist);
	CPPUNIT_ASSERT(exist);

	// 非递归删除空目录将成功
	code = subDir1.rmdir(false);
	CPPUNIT_ASSERT(File::getNtseError(code) == File::E_NO_ERROR);
	subDir1.isExist(&exist);
	CPPUNIT_ASSERT(!exist);

	// 递归删除空目录将成功
	code = dir.rmdir(true);
	CPPUNIT_ASSERT(File::getNtseError(code) == File::E_NO_ERROR);
	dir.isExist(&exist);
	CPPUNIT_ASSERT(!exist);
}

/**
 * 测试文件拷贝功能
 */
void FileTestCase::testCopy() {
	// 拷贝一个文件，验证拷贝过去的数据正确
	File f1("ftest");
	f1.create(false, false);

	char buf[10];
	memset(buf, 'a', sizeof(buf));

	f1.setSize(sizeof(buf));
	f1.write(0, sizeof(buf), buf);
	f1.close();
	
	CPPUNIT_ASSERT(File::copyFile("test2", "ftest", false) == File::E_NO_ERROR);
	File f2("test2");
	f2.open(false);
	u64 size;
	CPPUNIT_ASSERT(f2.getSize(&size) == File::E_NO_ERROR);
	CPPUNIT_ASSERT(size == sizeof(buf));

	char buf2[sizeof(buf)];
	f2.read(0, sizeof(buf), buf2);
	CPPUNIT_ASSERT(!memcmp(buf, buf2, sizeof(buf)));
	f2.close();

	// 不覆盖时目标文件已存在，出错
	u64 code = File::copyFile("test2", "ftest", false);
	CPPUNIT_ASSERT(File::getNtseError(code) == File::E_EXIST);

	// 覆盖时目标文件被改写
	f1.open(false);
	memset(buf, 'b', sizeof(buf));
	f1.write(0, sizeof(buf), buf);
	f1.close();

	CPPUNIT_ASSERT(File::copyFile("test2", "ftest", true) == File::E_NO_ERROR);

	f2.open(false);
	f2.read(0, sizeof(buf), buf2);
	CPPUNIT_ASSERT(!memcmp(buf, buf2, sizeof(buf)));
	f2.close();

	f1.remove();
	f2.remove();
}

void FileTestCase::testCopyDir() {
	File srcDir("testdir");
	srcDir.mkdir();
	File subFile("testdir/file1");
	subFile.create(false, false);
	subFile.close();
	File subDir("testdir/dir1");
	subDir.mkdir();
	File subsubFile("testdir/dir1/file1");
	subsubFile.create(false, false);
	subsubFile.close();

	// 目标目录不存在，拷贝成功
	u64 code = File::copyDir("testdir2", "testdir", false);
	CPPUNIT_ASSERT(code == File::E_NO_ERROR);
	CPPUNIT_ASSERT(File::isExist("testdir2"));
	CPPUNIT_ASSERT(File::isExist("testdir2/file1"));
	CPPUNIT_ASSERT(File::isExist("testdir2/dir1"));
	CPPUNIT_ASSERT(File::isExist("testdir2/dir1/file1"));

	// 目标文件存在，不覆盖时拷贝不成功
	code = File::copyDir("testdir2", "testdir", false);
	CPPUNIT_ASSERT(File::getNtseError(code) == File::E_EXIST);

	// 目标文件部分存在，覆盖时拷贝成功
	File destSubsubFile("testdir2/dir1/file1");
	destSubsubFile.remove();
	CPPUNIT_ASSERT(!File::isExist("testdir2/dir1/file1"));
	code = File::copyDir("testdir2", "testdir", true);
	CPPUNIT_ASSERT(File::getNtseError(code) == File::E_NO_ERROR);
	CPPUNIT_ASSERT(File::isExist("testdir2/dir1/file1"));

	// 目标目录存在，为空目录
	File destDir("testdir2");
	destDir.rmdir(true);
	destDir.mkdir();
	code = File::copyDir("testdir2", "testdir", false);
	CPPUNIT_ASSERT(code == File::E_NO_ERROR);
	CPPUNIT_ASSERT(File::isExist("testdir2"));
	CPPUNIT_ASSERT(File::isExist("testdir2/file1"));
	CPPUNIT_ASSERT(File::isExist("testdir2/dir1"));
	CPPUNIT_ASSERT(File::isExist("testdir2/dir1/file1"));
}

const char* FileBigTest::getName() {
	return "File performance test";
}

const char* FileBigTest::getDescription() {
	return "Test performance of sequential and random read write of file";
}

bool FileBigTest::isBig() {
	return true;
}

void FileBigTest::setUp() {
	File f("ftest");
	f.remove();
}

void FileBigTest::testSeq() {
	File f("ftest");
	f.create(true, false);
	u64 fileSize = (u64)1024 * 1024 * 2048;
	f.setSize(fileSize);

	for (int l = 0; l < 3; l++) {
		for (u32 blockSize = 4096; blockSize <= 256 * 1024; blockSize *= 2) {
			cout << "Test sequential read/write performance of block size " << blockSize << endl;
			char *p = (char *)System::virtualAlloc(blockSize + 1024);
			char *buf = p + 1024 - ((size_t)p) % 1024;

			u32 before = System::fastTime();
			for (u64 i = 0; i < fileSize / blockSize; i++) {
				CPPUNIT_ASSERT(f.write(i * blockSize, blockSize, buf) == File::E_NO_ERROR);
			}
			u32 after = System::fastTime();
			cout << "  write: " << fileSize / (after - before) / 1024 << " KB/s" << endl;

			before = System::fastTime();
			for (u64 i = 0; i < fileSize / blockSize; i++) {
				CPPUNIT_ASSERT(f.read(i * blockSize, blockSize, buf) == File::E_NO_ERROR);
			}
			after = System::fastTime();
			cout << "  read: " << fileSize / (after - before) / 1024 << " KB/s" << endl;
		}
	}

	f.close();
	f.remove();
}

void FileBigTest::testRandom() {
}
