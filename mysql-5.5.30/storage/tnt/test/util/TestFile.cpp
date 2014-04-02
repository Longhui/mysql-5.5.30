/**
 * �����ļ�����
 *
 * @author ��Դ(wangyuan@corp.netease.com, wy@163.org)
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

/** ���Դ������򿪡�ɾ���Ȼ����ļ����� */
void FileTestCase::testBasic() {
	File *f = new File("ftest");
	
	bool exist;
	f->isExist(&exist);
	CPPUNIT_ASSERT(!exist);
	CPPUNIT_ASSERT(strcmp(f->getPath(), "ftest") == 0);

	// �����ļ�
	u64 code;
	code = f->create(true, false);
	CPPUNIT_ASSERT(code == File::E_NO_ERROR);
	f->isExist(&exist);	// �ļ��Ѿ���ʱ�ж��ļ��Ƿ����
	CPPUNIT_ASSERT(exist);
	
	// ����ر�
	code = f->close();
	CPPUNIT_ASSERT(code == File::E_NO_ERROR);
	f->isExist(&exist);	// �ļ�û�д�ʱ�ж��ļ��Ƿ����
	CPPUNIT_ASSERT(exist);

	// �ر��Ѿ��رյ��ļ�
	code = f->close();
	CPPUNIT_ASSERT(code == File::E_NO_ERROR);

	// �ٴδ���ͬһ�ļ�����֤������ʧ�ܣ��ҷ�����ȷ�Ĵ�����
	File f2("ftest");
	code = f2.create(true, false);
	CPPUNIT_ASSERT(File::getNtseError(code) == File::E_EXIST);

	code = f->open(true);
	CPPUNIT_ASSERT(code == File::E_NO_ERROR);

	// ���Ѿ������˴򿪵��ļ�
	code = f2.open(true);
#ifdef WIN32
	CPPUNIT_ASSERT(File::getNtseError(code) == File::E_IN_USE);
#else
	CPPUNIT_ASSERT(code == File::E_NO_ERROR);
#endif

	// ɾ��
	code = f->remove();
	CPPUNIT_ASSERT(File::getNtseError(code) == File::E_IN_USE);
	f->close();
	code = f->remove();
	CPPUNIT_ASSERT(code == File::E_NO_ERROR);
	f->isExist(&exist);
	CPPUNIT_ASSERT(!exist);

	// ɾ�������ڵ��ļ�
	code = f->remove();
	CPPUNIT_ASSERT(File::getNtseError(code) == File::E_NOT_EXIST);

	// �򿪲����ڵ��ļ�
	code = f->open(true);
	CPPUNIT_ASSERT(File::getNtseError(code) == File::E_NOT_EXIST);

	delete f;

	// ����explainErrno
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

	// ���Դ�����
	CPPUNIT_ASSERT(File::getOsError(1LL << 32) == 1);
}

/** ���Թر�ʱ�Զ�ɾ������ʱ�ļ� */
void FileTestCase::testAutoDelete() {
	File f("ftest");
	
	u64 code = f.create(true, true);
	CPPUNIT_ASSERT(code == File::E_NO_ERROR);

	f.close();
	bool exist;
	f.isExist(&exist);
	CPPUNIT_ASSERT(!exist);
}

/** ���������� */
void FileTestCase::testRename() {
	File f("ftest");
	File f2("test2");

	u64 code = f.create(true, false);
	CPPUNIT_ASSERT(code == File::E_NO_ERROR);

	// δ�رվ���������ʧ��
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

	// ������ģʽ��Ŀ���ļ�����ʱʧ��
	code = f.move("test2");
	CPPUNIT_ASSERT(File::getNtseError(code) == File::E_EXIST);

	// ����ģʽ��Ŀ���ļ�����ʱ�ɹ�
	code = f.move("test2", true);
	CPPUNIT_ASSERT(code == File::E_NO_ERROR);
}

/** ���Եõ����޸��ļ���С���� */
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

/** ���Զ�д���� */
void FileTestCase::testReadWrite() {
	// ���Է�DIRECT_IO��д
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

	// ����DIRECT_IO��д
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

	// Խ���
	code = file->read(2048, 1024, buf1);
	CPPUNIT_ASSERT(File::getNtseError(code) == File::E_EOF);
	// Խ��д
	code = file->write(2048, 1024, buf1);
	CPPUNIT_ASSERT(File::getNtseError(code) == File::E_EOF);

	code = file->sync();
	CPPUNIT_ASSERT(code == File::E_NO_ERROR);
}

/**
 * �����첽IOд�ļ�
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
	// ����AIOSetup
	u64 errCode = aioArray.aioInit();
	CPPUNIT_ASSERT(errCode == File::E_NO_ERROR);

	// ����д����
	AioSlot *slot1 = aioArray.aioReserveSlot(AIO_WRITE, &f, buf1, 
		0, 1024, NULL);

	errCode = aioArray.aioDispatch(slot1);

	CPPUNIT_ASSERT(errCode == File::E_NO_ERROR);


	// ����Խ��д
	AioSlot *slot2 = aioArray.aioReserveSlot(AIO_WRITE, &f, buf2,
		2048, 1024, NULL);
	errCode = aioArray.aioDispatch(slot2);
	CPPUNIT_ASSERT(errCode == File::E_EOF);
	aioArray.aioFreeSlot(slot2);

	// ���Եȴ��첽IOд
	uint numIoComplete = 0;
	// ���Եȴ�IO��������ʵ����������
	errCode = aioArray.aioWaitFinish(2, &numIoComplete);
	CPPUNIT_ASSERT(errCode == File::E_AIO_ARGS_INVALID);

	// �ȴ��������
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
 * ����Ŀ¼�����������ݹ鴴����Ŀ¼��ö��Ŀ¼�ļ���ɾ����Ŀ¼��ݹ�ɾ��Ŀ¼��
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

	// �ݹ鴴��Ŀ¼
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

	// ����һ���Ѿ��洢��Ŀ¼
	code = dir.mkdir();
	CPPUNIT_ASSERT(File::getNtseError(code) == File::E_EXIST);

	subFile.create(false, false);
	CPPUNIT_ASSERT(subFile.isExist(&exist) == File::E_NO_ERROR);
	CPPUNIT_ASSERT(exist);
	subFile.close();

	// �����ļ�ö�ٹ���
	list<string> subFiles;
	CPPUNIT_ASSERT(dir.listFiles(&subFiles, false) == File::E_NO_ERROR);
	CPPUNIT_ASSERT(subFiles.size() == 1);
	subFiles.clear();
	CPPUNIT_ASSERT(dir.listFiles(&subFiles, true) == File::E_NO_ERROR);
	CPPUNIT_ASSERT(subFiles.size() == 3);

	// �ǵݹ�ɾ���ǿ�Ŀ¼��ʧ��
	code = dir.rmdir(false);
	CPPUNIT_ASSERT(File::getNtseError(code) == File::E_NOT_EMPTY);
	dir.isExist(&exist);
	CPPUNIT_ASSERT(exist);

	// �ǵݹ�ɾ����Ŀ¼���ɹ�
	code = subDir1.rmdir(false);
	CPPUNIT_ASSERT(File::getNtseError(code) == File::E_NO_ERROR);
	subDir1.isExist(&exist);
	CPPUNIT_ASSERT(!exist);

	// �ݹ�ɾ����Ŀ¼���ɹ�
	code = dir.rmdir(true);
	CPPUNIT_ASSERT(File::getNtseError(code) == File::E_NO_ERROR);
	dir.isExist(&exist);
	CPPUNIT_ASSERT(!exist);
}

/**
 * �����ļ���������
 */
void FileTestCase::testCopy() {
	// ����һ���ļ�����֤������ȥ��������ȷ
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

	// ������ʱĿ���ļ��Ѵ��ڣ�����
	u64 code = File::copyFile("test2", "ftest", false);
	CPPUNIT_ASSERT(File::getNtseError(code) == File::E_EXIST);

	// ����ʱĿ���ļ�����д
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

	// Ŀ��Ŀ¼�����ڣ������ɹ�
	u64 code = File::copyDir("testdir2", "testdir", false);
	CPPUNIT_ASSERT(code == File::E_NO_ERROR);
	CPPUNIT_ASSERT(File::isExist("testdir2"));
	CPPUNIT_ASSERT(File::isExist("testdir2/file1"));
	CPPUNIT_ASSERT(File::isExist("testdir2/dir1"));
	CPPUNIT_ASSERT(File::isExist("testdir2/dir1/file1"));

	// Ŀ���ļ����ڣ�������ʱ�������ɹ�
	code = File::copyDir("testdir2", "testdir", false);
	CPPUNIT_ASSERT(File::getNtseError(code) == File::E_EXIST);

	// Ŀ���ļ����ִ��ڣ�����ʱ�����ɹ�
	File destSubsubFile("testdir2/dir1/file1");
	destSubsubFile.remove();
	CPPUNIT_ASSERT(!File::isExist("testdir2/dir1/file1"));
	code = File::copyDir("testdir2", "testdir", true);
	CPPUNIT_ASSERT(File::getNtseError(code) == File::E_NO_ERROR);
	CPPUNIT_ASSERT(File::isExist("testdir2/dir1/file1"));

	// Ŀ��Ŀ¼���ڣ�Ϊ��Ŀ¼
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
