#include "util/TestNumCom.h"
#include "util/NumberCompress.h"
#include "util/System.h"
#include <iostream>
using namespace std;

using namespace ntse;

const char* NumberCompressTestCase::getName() {
	return "Number Compress test";
}

const char* NumberCompressTestCase::getDescription() {
	return "Test Order Preserving Number Compress Algorithm.";
}

bool NumberCompressTestCase::isBig() {
	return false;
}


template<typename T> static void verify(T v) {
	assert(sizeof(T) <= sizeof(s64));

	byte input[8];
	byte output[9];
	byte decomBuf[8];
	*((T *)input) = v;
	//changeEndian(input, (const byte *)&v, sizeof(T));
	size_t size = 0;
	CPPUNIT_ASSERT(NumberCompressor::compress(input, sizeof(T), output, sizeof(output), &size));
	NumberCompressor::decompress(output, size, decomBuf, sizeof(T));
	CPPUNIT_ASSERT(memcmp(input, decomBuf, sizeof(T)) == 0);
}

void NumberCompressTestCase::testCompress() {
	byte input[8];
	byte output[9];
	byte decomBuf[8];

	
	// 一、SHORT
	const size_t intSize = 2; // 整形长度
	size_t outSize = sizeof(output);
	size_t size = 0;
	memset(input, 0, intSize);
	memset(output, 0, outSize);
	// 0
	input[0] = 0;
	input[1] = 0;
	size = 0;
	CPPUNIT_ASSERT(NumberCompressor::compress(input, intSize, output, outSize, &size));
	CPPUNIT_ASSERT(size == 1);
	CPPUNIT_ASSERT(output[0] == 0x80);
	NumberCompressor::decompress(output, size, decomBuf, intSize);
	CPPUNIT_ASSERT(memcmp(input, decomBuf, intSize) == 0);

	// 1
	input[0] = 1;
	input[1] = 0;
	size = 0;
	CPPUNIT_ASSERT(NumberCompressor::compress(input, intSize, output, outSize, &size));
	CPPUNIT_ASSERT(size == 1);
	CPPUNIT_ASSERT(output[0] == 0x81);
	NumberCompressor::decompress(output, size, decomBuf, intSize);
	CPPUNIT_ASSERT(memcmp(input, decomBuf, intSize) == 0);

	// -1
	input[0] = 0xFF;
	input[1] = 0xFF;
	size = 0;
	CPPUNIT_ASSERT(NumberCompressor::compress(input, intSize, output, outSize, &size));
	CPPUNIT_ASSERT(size == 1);
	CPPUNIT_ASSERT(output[0] == 0x7F);
	NumberCompressor::decompress(output, size, decomBuf, intSize);
	CPPUNIT_ASSERT(memcmp(input, decomBuf, intSize) == 0);
	// -7
	input[0] = 0xF9;
	input[1] = 0xFF;
	size = 0;
	CPPUNIT_ASSERT(NumberCompressor::compress(input, intSize, output, outSize, &size));
	CPPUNIT_ASSERT(size == 1);
	CPPUNIT_ASSERT(output[0] == 0x79);
	NumberCompressor::decompress(output, size, decomBuf, intSize);
	CPPUNIT_ASSERT(memcmp(input, decomBuf, intSize) == 0);
	// 7
	input[0] = 0x07;
	input[1] = 0x00;
	size = 0;
	CPPUNIT_ASSERT(NumberCompressor::compress(input, intSize, output, outSize, &size));
	CPPUNIT_ASSERT(size == 1);
	CPPUNIT_ASSERT(output[0] == 0x87);
	NumberCompressor::decompress(output, size, decomBuf, intSize);
	CPPUNIT_ASSERT(memcmp(input, decomBuf, intSize) == 0);
	// -8
	input[0] = 0xF8;
	input[1] = 0xFF;
	size = 0;
	CPPUNIT_ASSERT(NumberCompressor::compress(input, intSize, output, outSize, &size));
	CPPUNIT_ASSERT(size == 2);
	CPPUNIT_ASSERT(output[0] == 0x77);
	CPPUNIT_ASSERT(output[1] == 0xF8);
	NumberCompressor::decompress(output, size, decomBuf, intSize);
	CPPUNIT_ASSERT(memcmp(input, decomBuf, intSize) == 0);
	// 8
	input[0] = 0x08;
	input[1] = 0x00;
	size = 0;
	CPPUNIT_ASSERT(NumberCompressor::compress(input, intSize, output, outSize, &size));
	CPPUNIT_ASSERT(size == 2);
	CPPUNIT_ASSERT(output[0] == 0x88);
	CPPUNIT_ASSERT(output[1] == 0x08);
	NumberCompressor::decompress(output, size, decomBuf, intSize);
	CPPUNIT_ASSERT(memcmp(input, decomBuf, intSize) == 0);

	// 0x0102
	input[0] = 0x02;
	input[1] = 0x01;
	size = 0;
	CPPUNIT_ASSERT(NumberCompressor::compress(input, intSize, output, outSize, &size));
	CPPUNIT_ASSERT(size == 2);
	CPPUNIT_ASSERT(output[0] == 0x89);
	CPPUNIT_ASSERT(output[1] == 0x02);
	NumberCompressor::decompress(output, size, decomBuf, intSize);
	CPPUNIT_ASSERT(memcmp(input, decomBuf, intSize) == 0);

	// 0xfffffffffffffff0
	
	verify((s64)0xfffffffffffffff0LL);
	verify((s64)0xfffffffffffffff1LL);

	byte tmp[8];

	for (input[0] = 0; ; ++input[0]) {
		for (input[1] = 0; ; ++input[1]) {
			memset(output, 0 ,outSize);
			NumberCompressor::compress(input, intSize, output, outSize, &size);
			NumberCompressor::decompress(output, size, tmp, intSize);
			CPPUNIT_ASSERT(0 == memcmp(input, tmp, intSize));
			if (input[1] == 0xFF)
				break;
		}
		if (input[0] == 0xFF)
			break;
	}

	// 验证顺序
	for (byte v = 0; v < 0x7F - 1; ++v) {
		byte output1[9], output2[9];
		memset(output1, 0 ,sizeof(output1));
		memset(output2, 0 ,sizeof(output2));
		size_t size1, size2;
		NumberCompressor::compress(&v, sizeof(v), output1, sizeof(output1), &size1);
		byte next = v + 1;
		NumberCompressor::compress(&next, sizeof(next), output2, sizeof(output2), &size2);
		CPPUNIT_ASSERT(size2 >= size1);
		int res = memcmp(output1, output2, size1);
		CPPUNIT_ASSERT((res == 0 && size1 < size2) || (res < 0));
	}

	for (s8 v = -128; v > -1; ++v) {
		byte output1[9], output2[9];
		memset(output1, 0 ,sizeof(output1));
		memset(output2, 0 ,sizeof(output2));
		size_t size1, size2;
		NumberCompressor::compress((byte*)&v, sizeof(v), output1, sizeof(output1), &size1);
		s8 next = v + 1;
		NumberCompressor::compress((byte*)&next, sizeof(next), output2, sizeof(output2), &size2);
		CPPUNIT_ASSERT(size2 >= size1);
		int res = memcmp(output1, output2, size1);
		CPPUNIT_ASSERT((res == 0 && size1 < size2) || (res < 0));
	}
}

const char* NumberCompressBigTest::getName() {
	return "Number Compress/Decompress Performance Test";
}

const char* NumberCompressBigTest::getDescription() {
	return "Test Order Preserving Number Compress Algorithm.";
}

bool NumberCompressBigTest::isBig() {
	return true;
}

void NumberCompressBigTest::testMisc() {
	cout << "Test performance of compress/decompress" << endl;
	int n = 1000;
	int loop = 10000;
	size_t sizeArr[3] = {2, 4, 8};
	s64 maxArr[3] = {0x7777, 0x77777777, 0x7777777777777777LL};
	s64 sum = 0;
	byte **compressedValues = new byte *[n];
	size_t *compressedSizes = new size_t[n];
	for (int i = 0; i < n; i++)
		compressedValues[i] = new byte[9];

	int run = 0;
_restart:
	cout << "run " << run << endl;

	for (int s = 0; s < sizeof(sizeArr) / sizeof(sizeArr[0]); s++) {
		size_t size = sizeArr[s];
		s64 max = maxArr[s];
		s64 step = maxArr[s] / n;

		cout << "  size " << size << endl;
		
		cout << "    compress/decompress positive values" << endl;
		u64 before = System::clockCycles();
		for (int l = 0; l < loop; l++) {
			s64 v = 0;
			for (int i = 0; i < n; i++) {
				//byte bigEndianBuf[8];
				//changeEndian(bigEndianBuf, (byte *)&v, size);
				//NumberCompressor::compress(bigEndianBuf, size, compressedValues[i], 9, compressedSizes + i);
				NumberCompressor::compress((byte *)&v, size, compressedValues[i], 9, compressedSizes + i);
				v += step;
			}
		}
		u64 after = System::clockCycles();
		cout << "      cc per compress: " << (after - before) / n / loop << endl;

		before = System::clockCycles();
		for (int l = 0; l < loop; l++) {
			s64 v = 0;
			for (int i = 0; i < n; i++) {
				NumberCompressor::decompress(compressedValues[i], compressedSizes[i], (byte *)&v, size);
				//byte bigEndianBuf[8];
				//changeEndian(bigEndianBuf, (byte *)&v, size);
			}
		}
		after = System::clockCycles();
		cout << "      cc per decompress: " << (after - before) / n / loop << endl;

		cout << "    compress/decompress negative values" << endl;
		before = System::clockCycles();
		for (int l = 0; l < loop; l++) {
			s64 v = 0;
			for (int i = 0; i < n; i++) {
				//byte bigEndianBuf[8];
				//changeEndian(bigEndianBuf, (byte *)&v, size);
				//NumberCompressor::compress(bigEndianBuf, size, compressedValues[i], 9, compressedSizes + i);
				NumberCompressor::compress((byte *)&v, size, compressedValues[i], 9, compressedSizes + i);
				v -= step;
			}
		}
		after = System::clockCycles();
		cout << "      cc per compress: " << (after - before) / n / loop << endl;

		before = System::clockCycles();
		for (int l = 0; l < loop; l++) {
			s64 v = 0;
			for (int i = 0; i < n; i++) {
				NumberCompressor::decompress(compressedValues[i], compressedSizes[i], (byte *)&v, size);
				//byte bigEndianBuf[8];
				//changeEndian(bigEndianBuf, (byte *)&v, size);
			}
		}
		after = System::clockCycles();
		cout << "      cc per decompress: " << (after - before) / n / loop << endl;
	}

	run++;
	if (run < 3)
		goto _restart;
}

