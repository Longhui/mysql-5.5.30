/**
 * ���Ը����ϣ��ʵ��
 *
 * @author ��Դ(wangyuan@corp.netease.com, wy@163.org)
 */

#include "util/TestHash.h"
#include "util/Hash.h"
#include "util/System.h"
#include <vector>

using namespace std;
using namespace ntse;

const char* HashTestCase::getName() {
	return "Hashtable test";
}

const char* HashTestCase::getDescription() {
	return "Test all kinds of hashtables.";
}

bool HashTestCase::isBig() {
	return false;
}

class DynHashTestPoolUser: public PagePoolUser {
public:
	DynHashTestPoolUser(uint targetSize, PagePool *pool): PagePoolUser(targetSize, pool) {
	}

	virtual uint freeSomePages(u16 userId, uint numPages) {
		return 0;
	}
};


void HashTestCase::testDynHash() {
	// �������ܲ���
	DynHash<int, int> h;
	vector<int> used;
	vector<int> unused;
	int count = 100;
	int loop = 200;
	for (int i = 0; i < count; i++)
		unused.push_back(i + 1);
	for (int i = 0; i < loop; i++) {
		int op = System::random() % 100;
		if (op <= 50 && unused.size() > 0) {
			size_t idx = System::random() % unused.size();
			int v = unused[idx];
			h.put(v);
			unused.erase(unused.begin() + idx);
			used.push_back(v);
		} else if (used.size() > 0) {
			size_t idx = System::random() % used.size();
			int v = used[idx];
			CPPUNIT_ASSERT(h.remove(v) == v);
			used.erase(used.begin() + idx);
			unused.push_back(v);
		}
		CPPUNIT_ASSERT(h.getSize() == used.size());
		if (loop % 10 == 0)
			h.check();
		for (size_t j = 0; j < used.size(); j++)
			CPPUNIT_ASSERT(h.get(used[j]) == used[j]);
	}

	h.clear();
	CPPUNIT_ASSERT(h.getSize() == 0);

	// reserveSize
	h.reserveSize(10);
	CPPUNIT_ASSERT(h.getSize() == 0);

	// ʹ���ڴ�ҳ�صĹ�ϣ��
	uint pageSize = 32;
	uint numPages = 32;
	PagePool pool(0, pageSize);
	DynHashTestPoolUser poolUser(numPages, &pool);
	pool.registerUser(&poolUser);
	pool.init();
	DynHash<int, int> h2(&poolUser, PAGE_HEAP);

	uint capacity = (pageSize / sizeof(DHElem<int>)) * numPages;
	// ��������ֱ�����������ڴ�
	for (uint i = 0; i < capacity; i++) {
		CPPUNIT_ASSERT(h2.put(i + 1));
	}
	for (uint i = 0; i < capacity; i++) {
		CPPUNIT_ASSERT(h2.get(i + 1) == i + 1);
	}
	h2.check();
	// ����֮�󣬲����ٲ���
	CPPUNIT_ASSERT(!h2.put(100));
	CPPUNIT_ASSERT(!h2.reserveSize(h2.getSize() + 1));
	// ɾ���������������²���
	for (uint i = 0; i < capacity / 2; i++) {
		CPPUNIT_ASSERT(h2.remove(i + 1) == i + 1);
	}
	for (uint i = 0; i < capacity / 2; i++) {
		CPPUNIT_ASSERT(h2.put(i + 1));
	}
	for (uint i = 0; i < capacity; i++) {
		CPPUNIT_ASSERT(h2.get(i + 1) == i + 1);
	}
	h2.check();

	// �쳣���
	h.clear();
	CPPUNIT_ASSERT(h.get(1) == 0);
	CPPUNIT_ASSERT(h.remove(1) == 0);
	h.put(1);
	h.put(2);
	h.put(3);
	h.print();
	CPPUNIT_ASSERT(h.get(4) == 0);
	CPPUNIT_ASSERT(h.remove(4) == 0);

	// ������������
	DynHash<long, long> h3;
	for (uint i = 0; i < 1000; i++) {
		CPPUNIT_ASSERT(h3.put(i + 1));
	}
	for (uint i = 0; i < 1000; i++) {
		CPPUNIT_ASSERT(h3.get(i + 1) == i + 1);
	}

	DynHash<u64, u64> h4;
	for (uint i = 0; i < 1000; i++) {
		CPPUNIT_ASSERT(h4.put(i + 1));
	}
	for (uint i = 0; i < 1000; i++) {
		CPPUNIT_ASSERT(h4.get(i + 1) == i + 1);
	}

	DynHash<u16, u16> h5;
	for (uint i = 0; i < 1000; i++) {
		CPPUNIT_ASSERT(h5.put(i + 1));
	}
	for (uint i = 0; i < 1000; i++) {
		CPPUNIT_ASSERT(h5.get(i + 1) == i + 1);
	}

	DynHash<char *, char *> h6;
	char **sa = (char **)malloc(sizeof(char *) * 1000);
	for (uint i = 0; i < 1000; i++) {
		sa[i] = new char[2];
		sa[i][1] = '\0';
		sa[i][0] = 'A' + (i % 52);
		CPPUNIT_ASSERT(h6.put(sa[i]));
	}
	for (uint i = 0; i < 1000; i++) {
		CPPUNIT_ASSERT(!strcmp(h6.get(sa[i]), sa[i]));
	}
	for (uint i = 0; i < 1000; i++) {
		delete []sa[i];
	}
	free(sa);
}

void HashTestCase::testHash() {
	// �������ܲ���
	int capacity = 100;
	Hash<int, int> h(capacity);
	vector<int> used;
	vector<int> unused;
	int count = 200;
	int loop = 500;
	for (int i = 0; i < count; i++)
		unused.push_back(System::random() % 200);
	for (int i = 0; i < loop; i++) {
		int op = System::random() % 100;
		if (op <= 50 && unused.size() > 0) {
			size_t idx = System::random() % unused.size();
			int v = unused[idx];
			h.put(v, v);
			unused.erase(unused.begin() + idx);
			used.push_back(v);
		} else if (used.size() > 0) {
			size_t idx = System::random() % used.size();
			int v = used[idx];
			CPPUNIT_ASSERT(h.remove(v) == v);
			used.erase(used.begin() + idx);
			unused.push_back(v);
		}
		CPPUNIT_ASSERT(h.getSize() == used.size());
		for (size_t j = 0; j < used.size(); j++)
			CPPUNIT_ASSERT(h.get(used[j]) == used[j]);
	}
	int *keys = new int[h.getSize()];
	int *values = new int[h.getSize()];
	h.elements(keys, values);
	for (size_t i = 0; i < h.getSize(); i++) {
		CPPUNIT_ASSERT(keys[i] == values[i]);
		size_t j;
		for (j = 0; j < used.size(); j++)
			if (keys[i] == used[j])
				break;
		CPPUNIT_ASSERT(j < h.getSize());
	}
	for (size_t i = 0; i < used.size(); i++) {
		size_t j;
		for (j = 0; j < h.getSize(); j++)
			if (keys[j] == used[i])
				break;
		CPPUNIT_ASSERT(j < h.getSize());
	}
	delete []keys;
	delete []values;

	h.clear();
	CPPUNIT_ASSERT(h.getSize() == 0);

	// �쳣���
	h.clear();
	CPPUNIT_ASSERT(h.get(1) == 0);
	CPPUNIT_ASSERT(h.remove(1) == 0);
	h.put(1, 1);
	h.put(2, 2);
	h.put(3, 3);
	CPPUNIT_ASSERT(h.get(4) == 0);
	CPPUNIT_ASSERT(h.remove(4) == 0);

	h.clear();
	for (int i = 0; i < capacity; i++)
		CPPUNIT_ASSERT(h.put(3 * i + 1, i + 1));
	for (int i = 0; i < capacity; i++)
		CPPUNIT_ASSERT(h.get(3 * i + 1) == i + 1);
	CPPUNIT_ASSERT(!h.put(100, 100));
	for (int i = 0; i < capacity / 2; i++)
		CPPUNIT_ASSERT(h.remove(3 * i + 1) == i + 1);
	for (int i = 0; i < capacity / 2; i++)
		CPPUNIT_ASSERT(h.put(3 * i + 1, i + 1));
	for (int i = 0; i < capacity; i++)
		CPPUNIT_ASSERT(h.get(3 * i + 1) == i + 1);
}

const char* HashBigTest::getName() {
	return "Hashtable performance test";
}

const char* HashBigTest::getDescription() {
	return "Test the performance of hash table operations.";
}

bool HashBigTest::isBig() {
	return true;
}

/** ������־
 * 2008/6/12
 *   ���ʵ��
 *     insert: 1426
 *     delete: 682
 *     search: 86
 *           
 *   �޸ļ����ϣͰ�ŷ�����ʹ��λ��������ȡģ
 *     insert: 1355
 *     delete: 554
 *     search: 85
 *
 *   �޸�Array�����ҳ�ź�ҳ��ƫ���㷨����ÿҳԪ�ظ���Ϊ2���ݴ�ʱʹ����λ��λ����ʵ��
 *     insert: 496
 *     delete: 201
 *     search: 26
 *
 *   �޸Ĺ�ϣ����ʱ�����㷨��ʹ��λ�����ж�Ԫ��Ӧ�����ڷ��Ѻ���Ǹ�Ͱ
 *     insert: 326
 *     delete: 201
 *     search: 26
 *
 *   ������������[]��������֪����ͻ�����׵�ַʱ������
 *     insert: 291
 *     delete: 167
 *     search: 25
 *
 *   ��һ����������[]�����������������в���Ҫ��[]����
 *     insert: 171
 *     delete: 138
 *     search: 35
 */

void HashBigTest::testDynHash() {
	DynHash<long, long> h;
	vector<long> values;
	int count = 1000000;
	int loop = 10;

	h.reserveSize(count);
	for (int i = 0; i < count; i++)
		values.push_back(i + 1);
	for (int i = 0; i < count; i++) {
		int i1 = (int)(System::random() % values.size());
		int i2 = (int)(System::random() % values.size());
		int v = values[i2];
		values[i2] = values[i1];
		values[i1] = v;
	}
	cout << "  Test performance of insert" << endl;
	u64 before = System::clockCycles();
	u64 beforeUs = System::microTime();
	for (int i = 0; i < count; i++) {
		h.put(values[i]);
	}
	u64 after = System::clockCycles();
	u64 afterUs = System::microTime();
	h.check();
	cout << "  clock cycles per insertion: " << (after - before) / count << endl;
	cout << "  insertion rate(n/s): " << count * 1000000.0 / (afterUs - beforeUs) << endl;

	cout << "  Test performance of delete" << endl;
	before = System::clockCycles();
	beforeUs = System::microTime();
	for (int i = 0; i < count; i++) {
		h.remove(values[i]);
	}
	after = System::clockCycles();
	afterUs = System::microTime();
	cout << "  clock cycles per deletion: " << (after - before) / count << endl;
	cout << "  deletion rate(n/s): " << count * 1000000.0 / (afterUs - beforeUs) << endl;
	
	cout << "  Test performance of search" << endl;
	for (int i = 0; i < count; i++) {
		h.put(values[i]);
	}
	h.check();
	double avgConflict;
	size_t maxConflict;
	h.getConflictStatus(&avgConflict, &maxConflict);
	cout << " avg conflict: " << avgConflict << ", max conflict: " << maxConflict << endl;

	before = System::clockCycles();
	beforeUs = System::microTime();
	int sum = 0;
	for (int l = 0; l < loop; l++) {
		for (int i = 0; i < count; i++) {
			int v = h.get(values[i]);
			sum += v;
		}
	}
	after = System::clockCycles();
	afterUs = System::microTime();
	cout << "  clock cycles per search: " << (after - before) / count / loop << endl;
	cout << "  search rate(n/s): " << count * loop * 1000000.0 / (afterUs - beforeUs) << endl;
	cout << "  sum = " << sum << endl;
}

void HashBigTest::testHash() {
	int count = 10000;
	Hash<long, long> h(count);
	vector<long> values;
	int loop = 1000;

	for (int i = 0; i < count; i++)
		values.push_back(i + 1);
	for (int i = 0; i < count; i++) {
		int i1 = (int)(System::random() % values.size());
		int i2 = (int)(System::random() % values.size());
		int v = values[i2];
		values[i2] = values[i1];
		values[i1] = v;
	}

	cout << "  Test performance of insert" << endl;
	u64 before = System::clockCycles();
	u64 beforeUs = System::microTime();
	for (int i = 0; i < count; i++) {
		h.put(values[i], values[i]);
	}
	u64 after = System::clockCycles();
	u64 afterUs = System::microTime();
	cout << "  clock cycles per insertion: " << (after - before) / count << endl;
	cout << "  insertion rate(n/us): " << count / (afterUs - beforeUs) << endl;

	cout << "  Test performance of delete" << endl;
	before = System::clockCycles();
	beforeUs = System::microTime();
	for (int i = 0; i < count; i++) {
		h.remove(values[i]);
	}
	after = System::clockCycles();
	afterUs = System::microTime();
	cout << "  clock cycles per deletion: " << (after - before) / count << endl;
	cout << "  deletion rate(n/us): " << count / (afterUs - beforeUs) << endl;
	
	cout << "  Test performance of search" << endl;
	for (int i = 0; i < count; i++) {
		h.put(values[i], values[i]);
	}
	before = System::clockCycles();
	afterUs = System::microTime();
	int sum = 0;
	for (int l = 0; l < loop; l++) {
		for (int i = 0; i < count; i++) {
			int v = h.get(values[i]);
			sum += v;
		}
	}
	after = System::clockCycles();
	afterUs = System::microTime();
	cout << "  clock cycles per search: " << (after - before) / count / loop << endl;
	cout << "  search rate(n/us): " << count * loop / (afterUs - beforeUs) << endl;
	cout << "  sum = " << sum << endl;
}

