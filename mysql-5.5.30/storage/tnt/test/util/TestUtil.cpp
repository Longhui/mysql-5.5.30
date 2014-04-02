/**
 * 测试NTSE中的辅助工具类
 *
 * @author 汪源(wangyuan@corp.netease.com, wy@163.org)
 */

#include <iostream>
#include "util/TestUtil.h"
#include "util/DList.h"
#include "util/Array.h"
#include "util/SmartPtr.h"
#include "util/System.h"
#include "util/ObjectPool.h"
#include "util/Stream.h"

using namespace std;

const char* UtilTestCase::getName() {
	return "Utilities test";
}

const char* UtilTestCase::getDescription() {
	return "Test some base utilities such as DList, Array";
}

bool UtilTestCase::isBig() {
	return false;
}

void UtilTestCase::testDList() {
	DList<int> l;
	CPPUNIT_ASSERT(l.isEmpty());
	CPPUNIT_ASSERT(l.getSize() == 0);
	CPPUNIT_ASSERT(l.getHeader()->getNext() == l.getHeader());
	CPPUNIT_ASSERT(l.removeFirst() == NULL);
	CPPUNIT_ASSERT(l.removeLast() == NULL);

	DLink<int> n1(1);
	CPPUNIT_ASSERT(n1.get() == 1);
	DLink<int> n2(2);
	CPPUNIT_ASSERT(n2.get() == 2);
	n2.set(3);
	CPPUNIT_ASSERT(n2.get() == 3);

	// 测试addFirst和addLast
	l.addFirst(&n1);
	CPPUNIT_ASSERT(l.getSize() == 1);
	CPPUNIT_ASSERT(!l.isEmpty());
	CPPUNIT_ASSERT(l.getHeader()->getNext() == &n1);
	CPPUNIT_ASSERT(l.getHeader()->getPrev() == &n1);
	CPPUNIT_ASSERT(l.getHeader()->getNext()->getNext() == l.getHeader());
	CPPUNIT_ASSERT(n1.getList() == &l);
	CPPUNIT_ASSERT(n1.getNext() == l.getHeader());
	CPPUNIT_ASSERT(n1.getPrev() == l.getHeader());

	l.addLast(&n2);
	CPPUNIT_ASSERT(l.getSize() == 2);
	CPPUNIT_ASSERT(!l.isEmpty());
	CPPUNIT_ASSERT(l.getHeader()->getNext() == &n1);
	CPPUNIT_ASSERT(l.getHeader()->getPrev() == &n2);
	CPPUNIT_ASSERT(l.getHeader()->getNext()->getNext() == &n2);
	CPPUNIT_ASSERT(l.getHeader()->getNext()->getNext()->getNext() == l.getHeader());
	CPPUNIT_ASSERT(n1.getNext() == &n2);
	CPPUNIT_ASSERT(n1.getPrev() == l.getHeader());
	CPPUNIT_ASSERT(n2.getList() == &l);
	CPPUNIT_ASSERT(n2.getNext() == l.getHeader());
	CPPUNIT_ASSERT(n2.getPrev() == &n1);

	// 测试moveToFirst和moveToLast
	l.moveToFirst(&n2);
	CPPUNIT_ASSERT(l.getSize() == 2);
	CPPUNIT_ASSERT(!l.isEmpty());
	CPPUNIT_ASSERT(l.getHeader()->getNext() == &n2);
	CPPUNIT_ASSERT(l.getHeader()->getPrev() == &n1);
	CPPUNIT_ASSERT(l.getHeader()->getNext()->getNext() == &n1);
	CPPUNIT_ASSERT(l.getHeader()->getNext()->getNext()->getNext() == l.getHeader());
	CPPUNIT_ASSERT(n1.getPrev() == &n2);
	CPPUNIT_ASSERT(n1.getNext() == l.getHeader());
	CPPUNIT_ASSERT(n2.getPrev() == l.getHeader());
	CPPUNIT_ASSERT(n2.getNext() == &n1);

	l.moveToLast(&n2);
	CPPUNIT_ASSERT(l.getSize() == 2);
	CPPUNIT_ASSERT(!l.isEmpty());
	CPPUNIT_ASSERT(l.getHeader()->getNext() == &n1);
	CPPUNIT_ASSERT(l.getHeader()->getPrev() == &n2);
	CPPUNIT_ASSERT(l.getHeader()->getNext()->getNext() == &n2);
	CPPUNIT_ASSERT(l.getHeader()->getNext()->getNext()->getNext() == l.getHeader());
	CPPUNIT_ASSERT(n1.getNext() == &n2);
	CPPUNIT_ASSERT(n1.getPrev() == l.getHeader());
	CPPUNIT_ASSERT(n2.getNext() == l.getHeader());
	CPPUNIT_ASSERT(n2.getPrev() == &n1);

	// 测试removeFirst
	CPPUNIT_ASSERT(l.removeFirst() == &n1);
	CPPUNIT_ASSERT(n1.getList() == NULL);
	CPPUNIT_ASSERT(n1.getNext() == NULL);
	CPPUNIT_ASSERT(n1.getPrev() == NULL);
	CPPUNIT_ASSERT(l.getSize() == 1);
	CPPUNIT_ASSERT(!l.isEmpty());
	CPPUNIT_ASSERT(l.getHeader()->getNext() == &n2);
	CPPUNIT_ASSERT(l.getHeader()->getPrev() == &n2);
	CPPUNIT_ASSERT(l.getHeader()->getNext()->getNext() == l.getHeader());
	CPPUNIT_ASSERT(n2.getNext() == l.getHeader());
	CPPUNIT_ASSERT(n2.getPrev() == l.getHeader());

	CPPUNIT_ASSERT(l.removeFirst() == &n2);
	CPPUNIT_ASSERT(n2.getList() == NULL);
	CPPUNIT_ASSERT(n2.getNext() == NULL);
	CPPUNIT_ASSERT(n2.getPrev() == NULL);
	CPPUNIT_ASSERT(l.isEmpty());
	CPPUNIT_ASSERT(l.getSize() == 0);
	CPPUNIT_ASSERT(l.getHeader()->getNext() == l.getHeader());

	// 测试removeLast
	l.addLast(&n1);
	n1.addAfter(&n2);
	CPPUNIT_ASSERT(l.removeLast() == &n2);
	CPPUNIT_ASSERT(n2.getList() == NULL);
	CPPUNIT_ASSERT(n2.getNext() == NULL);
	CPPUNIT_ASSERT(n2.getPrev() == NULL);
	CPPUNIT_ASSERT(l.getSize() == 1);
	CPPUNIT_ASSERT(!l.isEmpty());
	CPPUNIT_ASSERT(l.getHeader()->getNext() == &n1);
	CPPUNIT_ASSERT(l.getHeader()->getPrev() == &n1);
	CPPUNIT_ASSERT(l.getHeader()->getNext()->getNext() == l.getHeader());
	CPPUNIT_ASSERT(n1.getNext() == l.getHeader());
	CPPUNIT_ASSERT(n1.getPrev() == l.getHeader());

	CPPUNIT_ASSERT(l.removeLast() == &n1);
	CPPUNIT_ASSERT(n1.getList() == NULL);
	CPPUNIT_ASSERT(n1.getNext() == NULL);
	CPPUNIT_ASSERT(n1.getPrev() == NULL);
	CPPUNIT_ASSERT(l.isEmpty());
	CPPUNIT_ASSERT(l.getSize() == 0);
	CPPUNIT_ASSERT(l.getHeader()->getNext() == l.getHeader());

	// 测试DLink::unLink
	l.addLast(&n1);
	n1.addAfter(&n2);
	n1.unLink();
	n1.unLink();
	CPPUNIT_ASSERT(n1.getList() == NULL);
	CPPUNIT_ASSERT(n1.getNext() == NULL);
	CPPUNIT_ASSERT(n1.getPrev() == NULL);
	CPPUNIT_ASSERT(l.getSize() == 1);
	CPPUNIT_ASSERT(!l.isEmpty());
	CPPUNIT_ASSERT(l.getHeader()->getNext() == &n2);
	CPPUNIT_ASSERT(l.getHeader()->getPrev() == &n2);
	CPPUNIT_ASSERT(l.getHeader()->getNext()->getNext() == l.getHeader());
	CPPUNIT_ASSERT(n2.getNext() == l.getHeader());
	CPPUNIT_ASSERT(n2.getPrev() == l.getHeader());
}

class UtilTestPoolUser: public PagePoolUser {
public:
	UtilTestPoolUser(uint targetSize, PagePool *pool): PagePoolUser(targetSize, pool) {
	}

	virtual uint freeSomePages(u16 userId, uint numPages) {
		return 0;
	}
};

struct ArrayTestElem {
	int	i;
	int	j;
	int	k;

	ArrayTestElem() {
		//cout << "construction" << endl;
		memset(this, 0, sizeof(*this));
	}

	~ArrayTestElem() {
		//cout << "destruction" << endl;
	}
};

void UtilTestCase::testArray() {
	uint pageSize = 32;
	uint numPages = 32;
	PagePool pool(0, pageSize);
	UtilTestPoolUser poolUser(numPages, &pool);
	pool.registerUser(&poolUser);
	pool.init();

	Array<u64> a(&poolUser, PAGE_HEAP);
	doArrayTest(a, pageSize);

	// 测试内存不足时的情况
	CPPUNIT_ASSERT(a.setReservedSize(pageSize / sizeof(u64) * numPages));
	CPPUNIT_ASSERT(!a.setReservedSize(pageSize / sizeof(u64) * numPages + 1));
	// 插满后再插入会失败，删除掉一些后会恢复
	a.clear();
	for (uint i = 0; i < pageSize / sizeof(u64) * numPages; i++) {
		a.push(i + 1);
	}
	CPPUNIT_ASSERT(!a.push(0));
	a.pop();
	CPPUNIT_ASSERT(a.push(0));

	// 不使用内存页池的数组
	Array<u64> a2;
	doArrayTest(a2, Array<u64>::PAGE_SIZE);

	// 测试一页中元素个数不是2的整数次幂的情况
	Array<ArrayTestElem> a3;
	for (uint i = 0; i < 1000; i++) {
		ArrayTestElem e;
		a3.push(e);
		CPPUNIT_ASSERT(!a3.isEmpty());
		CPPUNIT_ASSERT(a3.getSize() == i + 1);
	}
	for (uint i = 0; i < 1000; i++)
		a3.pop();
	CPPUNIT_ASSERT(a3.isEmpty());
	CPPUNIT_ASSERT(a3.getSize() == 0);
}

void UtilTestCase::doArrayTest(Array<u64> &a, uint pageSize) {
	CPPUNIT_ASSERT(a.isEmpty());
	CPPUNIT_ASSERT(a.getSize() == 0);
	CPPUNIT_ASSERT(a.getReservedSize() == 0);
	CPPUNIT_ASSERT(a.getCapacity() == 0);

	// 插入元素，直到满一个页
	for (uint i = 0; i < pageSize / sizeof(u64); i++) {
		a.push(i + 1);
		CPPUNIT_ASSERT(!a.isEmpty());
		CPPUNIT_ASSERT(a.getSize() == i + 1);
		CPPUNIT_ASSERT(a.getReservedSize() == 0);
		CPPUNIT_ASSERT(a.getCapacity() == pageSize / sizeof(u64));
		CPPUNIT_ASSERT(a.last() == i + 1);
	}
	for (uint i = 0; i < a.getSize(); i++) {
		CPPUNIT_ASSERT(a[i] == i + 1);
	}

	// 用expand插入元素，满两个页
	for (uint i = pageSize / sizeof(u64); i < pageSize / sizeof(u64) * 2; i++) {
		a.expand();
		CPPUNIT_ASSERT(!a.isEmpty());
		CPPUNIT_ASSERT(a.getSize() == i + 1);
		CPPUNIT_ASSERT(a.getReservedSize() == 0);
		CPPUNIT_ASSERT(a.getCapacity() == pageSize / sizeof(u64) * 2);
		a[i] = i + 1;
	}
	for (uint i = 0; i < a.getSize(); i++) {
		CPPUNIT_ASSERT(a[i] == i + 1);
	}

	// 删除元素，最后只剩下一页
	for (uint i = 0; i < pageSize / sizeof(u64) - 1; i++) {
		a.pop();
		CPPUNIT_ASSERT(!a.isEmpty());
		CPPUNIT_ASSERT(a.getSize() == pageSize / sizeof(u64) * 2 - i - 1);
		CPPUNIT_ASSERT(a.getReservedSize() == 0);
		CPPUNIT_ASSERT(a.getCapacity() == pageSize / sizeof(u64) * 2);
	}
	a.pop();
	CPPUNIT_ASSERT(a.getCapacity() == pageSize / sizeof(u64));

	// 测试reservedSize
	a.setReservedSize(pageSize / sizeof(u64) * 8);
	CPPUNIT_ASSERT(a.getSize() == pageSize / sizeof(u64));
	CPPUNIT_ASSERT(a.getReservedSize() == pageSize / sizeof(u64) * 8);
	CPPUNIT_ASSERT(a.getCapacity() == a.getReservedSize());

	a.setReservedSize(pageSize / sizeof(u64));
	CPPUNIT_ASSERT(a.getSize() == pageSize / sizeof(u64));
	CPPUNIT_ASSERT(a.getReservedSize() == pageSize / sizeof(u64));
	CPPUNIT_ASSERT(a.getCapacity() == a.getReservedSize());

	a.setReservedSize(0);
	a.clear();
	a.setReservedSize(1);
	CPPUNIT_ASSERT(a.getReservedSize() == 1);
	for (uint i = 0; i < pageSize / sizeof(u64) * 2; i++) {
		a.push(i + 1);
	}
	CPPUNIT_ASSERT(a.getCapacity() == pageSize / sizeof(u64) * 2);
	a.clear();
	CPPUNIT_ASSERT(a.getSize() == 0);
	CPPUNIT_ASSERT(a.isEmpty());
	CPPUNIT_ASSERT(a.getCapacity() == pageSize / sizeof(u64));
}

void UtilTestCase::testSmartPtr() {
	byte *a1 = new byte[100];
	AutoPtr<byte> ap1(a1, true);
	CPPUNIT_ASSERT((byte *)ap1 == a1);
	CPPUNIT_ASSERT(ap1.operator ->() == a1);

	AutoPtr<byte> ap2(new byte(2));
	byte *a = new byte[20];
	AutoPtr<byte> ap3(a, true);
	CPPUNIT_ASSERT(ap3.detatch() == a);
	delete []a;
}

void UtilTestCase::testSystem() {
	// 计时函数
	u64 timeMs = System::currentTimeMillis();
	u64 microTime = System::microTime();
	u32 fastTime = System::fastTime();
	time_t now = time(NULL);
	CPPUNIT_ASSERT(fastTime >= now - 1 && fastTime <= now + 1);
	CPPUNIT_ASSERT(timeMs / 1000 >= fastTime - 1 && timeMs / 1000 <= fastTime + 1);
	CPPUNIT_ASSERT(microTime / 1000 >= timeMs -1 && microTime / 1000 <= timeMs + 1);

	System::clockCycles();

	time_t ecpo = 0;
	char buf[100];
	System::formatTime(buf, sizeof(buf), &ecpo);
#ifdef WIN32
	CPPUNIT_ASSERT(!strcmp(buf, "Thu Jan 01 08:00:00 1970"));
#else
	CPPUNIT_ASSERT(!strcmp(buf, "Thu Jan  1 08:00:00 1970"));
#endif

	void *p = System::virtualAlloc(10000);
	CPPUNIT_ASSERT(((size_t)p) % 512 == 0);
	System::virtualFree(p);

	System::snprintf_mine(buf, sizeof(buf), "hello %d", 100);
	CPPUNIT_ASSERT(!strcmp(buf, "hello 100"));

	CPPUNIT_ASSERT(System::stricmp("BBB", "aaa") > 0);
	CPPUNIT_ASSERT(System::stricmp("BBB", "ccc") < 0);
	CPPUNIT_ASSERT(System::stricmp("BBB", "bbb") == 0);
	CPPUNIT_ASSERT(System::strnicmp("BBBxx", "aaayy", 3) > 0);
	CPPUNIT_ASSERT(System::strnicmp("BBBxx", "cccyy", 3) < 0);
	CPPUNIT_ASSERT(System::strnicmp("BBBxx", "bbbyy", 3) == 0);

	char *dup = System::strdup("hello");
	CPPUNIT_ASSERT(!strcmp(dup, "hello"));
	delete []dup;

	System::srandom(1);
	int v = System::random();
	System::srandom(1);
	CPPUNIT_ASSERT(System::random() == v);
	System::srandom(2);
	CPPUNIT_ASSERT(System::random() != v);
}

void UtilTestCase::testObjectPool() {
	ObjectPool<int> pool;
	for (int i = 0; i < 10; i++) {
		CPPUNIT_ASSERT(pool.alloc() == i);
		pool[i] = i + 1;
	}
	pool.free(1);
	pool.free(3);
	pool.free(5);
	CPPUNIT_ASSERT(pool.alloc() == 5);
	CPPUNIT_ASSERT(pool.alloc() == 3);
	CPPUNIT_ASSERT(pool.alloc() == 1);
	CPPUNIT_ASSERT(pool.alloc() == 10);

	pool.clear();
	for (int i = 0; i < 10; i++) {
		CPPUNIT_ASSERT(pool.alloc() == i);
		pool[i] = i + 1;
	}
}

void UtilTestCase::testStream() {
	u8 vU8 = 1, vU82;
	s8 vS8 = -2, vS82;
	u16 vU16 = 3, vU162;
	s16 vS16 = -4, vS162;
	u32 vU32 = 5, vU322;
	s32 vS32 = -6, vS322;
	u64 vU64 = 7, vU642;
	s64 vS64 = -8, vS642;
	bool vBool = true, vBool2;
	RowId rid = RID(10, 23), rid2;
	char *str = "hello", *str2;
	byte buf[10], buf2[10];
	memset(buf, 0, sizeof(buf));

	byte strBuf[1000];
	Stream s(strBuf, sizeof(strBuf));
	
	size_t size = 0;

	CPPUNIT_ASSERT(s.write(vU8) == &s);
	size += sizeof(u8);
	CPPUNIT_ASSERT(s.getSize() == size);

	CPPUNIT_ASSERT(s.write(vS8) == &s);
	size += sizeof(s8);
	CPPUNIT_ASSERT(s.getSize() == size);

	CPPUNIT_ASSERT(s.write(vU16) == &s);
	size += sizeof(u16);
	CPPUNIT_ASSERT(s.getSize() == size);

	CPPUNIT_ASSERT(s.write(vS16) == &s);
	size += sizeof(s16);
	CPPUNIT_ASSERT(s.getSize() == size);

	CPPUNIT_ASSERT(s.write(vU32) == &s);
	size += sizeof(u32);
	CPPUNIT_ASSERT(s.getSize() == size);

	CPPUNIT_ASSERT(s.write(vS32) == &s);
	size += sizeof(s32);
	CPPUNIT_ASSERT(s.getSize() == size);

	CPPUNIT_ASSERT(s.write(vU64) == &s);
	size += sizeof(u64);
	CPPUNIT_ASSERT(s.getSize() == size);

	CPPUNIT_ASSERT(s.write(vS64) == &s);
	size += sizeof(s64);
	CPPUNIT_ASSERT(s.getSize() == size);

	CPPUNIT_ASSERT(s.write(vBool) == &s);
	size += sizeof(u8);
	CPPUNIT_ASSERT(s.getSize() == size);

	CPPUNIT_ASSERT(s.writeRid(rid) == &s);
	size += RID_BYTES;
	CPPUNIT_ASSERT(s.getSize() == size);

	CPPUNIT_ASSERT(s.write(str) == &s);
	size += sizeof(u32) + strlen(str) + 1;
	CPPUNIT_ASSERT(s.getSize() == size);

	CPPUNIT_ASSERT(s.write(buf, sizeof(buf)) == &s);
	size += sizeof(buf);
	CPPUNIT_ASSERT(s.getSize() == size);

	size = 0;
	Stream s2(strBuf, sizeof(strBuf));

	CPPUNIT_ASSERT(s2.read(&vU82) == &s2);
	size += sizeof(u8);
	CPPUNIT_ASSERT(s2.getSize() == size);
	CPPUNIT_ASSERT(vU82 == vU8);

	CPPUNIT_ASSERT(s2.read(&vS82) == &s2);
	size += sizeof(s8);
	CPPUNIT_ASSERT(s2.getSize() == size);
	CPPUNIT_ASSERT(vS82 == vS8);

	CPPUNIT_ASSERT(s2.read(&vU162) == &s2);
	size += sizeof(u16);
	CPPUNIT_ASSERT(s2.getSize() == size);
	CPPUNIT_ASSERT(vU162 == vU16);

	CPPUNIT_ASSERT(s2.read(&vS162) == &s2);
	size += sizeof(s16);
	CPPUNIT_ASSERT(s2.getSize() == size);
	CPPUNIT_ASSERT(vS162 == vS16);

	CPPUNIT_ASSERT(s2.read(&vU322) == &s2);
	size += sizeof(u32);
	CPPUNIT_ASSERT(s2.getSize() == size);
	CPPUNIT_ASSERT(vU322 == vU32);

	CPPUNIT_ASSERT(s2.read(&vS322) == &s2);
	size += sizeof(s32);
	CPPUNIT_ASSERT(s2.getSize() == size);
	CPPUNIT_ASSERT(vU322 == vU32);

	CPPUNIT_ASSERT(s2.read(&vU642) == &s2);
	size += sizeof(u64);
	CPPUNIT_ASSERT(s2.getSize() == size);
	CPPUNIT_ASSERT(vU642 == vU64);

	CPPUNIT_ASSERT(s2.read(&vS642) == &s2);
	size += sizeof(s64);
	CPPUNIT_ASSERT(s2.getSize() == size);
	CPPUNIT_ASSERT(vU642 == vU64);

	CPPUNIT_ASSERT(s2.read(&vBool2) == &s2);
	size += sizeof(u8);
	CPPUNIT_ASSERT(s2.getSize() == size);
	CPPUNIT_ASSERT(vBool2 == vBool);

	CPPUNIT_ASSERT(s2.readRid(&rid2) == &s2);
	size += RID_BYTES;
	CPPUNIT_ASSERT(s2.getSize() == size);
	CPPUNIT_ASSERT(rid == rid2);

	CPPUNIT_ASSERT(s2.readString(&str2) == &s2);
	size += sizeof(u32) + strlen(str2) + 1;
	CPPUNIT_ASSERT(s2.getSize() == size);
	CPPUNIT_ASSERT(!strcmp(str, str2));
	delete []str2;

	CPPUNIT_ASSERT(s2.readBytes(buf2, sizeof(buf2)) == &s2);
	size += sizeof(buf2);
	CPPUNIT_ASSERT(s2.getSize() == size);
	CPPUNIT_ASSERT(!memcmp(buf, buf2, sizeof(buf)));

	// 测试越界异常
	Stream s3(strBuf, 7);
	s3.write((u32)1);
	try {
		s3.write((u32)2);
		CPPUNIT_FAIL("Should not reach here");
	} catch (NtseException &e) {
		CPPUNIT_ASSERT(e.getErrorCode() == NTSE_EC_OVERFLOW);
	}
}

