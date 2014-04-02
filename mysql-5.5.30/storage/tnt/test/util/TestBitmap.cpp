#include "util/TestBitmap.h"
#include "util/Bitmap.h"

using namespace ntse;

const char* BitmapTestCase::getName() {
	return "Bitmap test";
}

const char* BitmapTestCase::getDescription() {
	return "Test bitmap class and bitmap operations.";
}

bool BitmapTestCase::isBig() {
	return false;
}


void BitmapTestCase::testBitmapOper() {
	byte bitmap[16];
	memset(bitmap, 0, sizeof(bitmap));
	
	for (size_t bmSize = 8; bmSize <= sizeof(bitmap) * 8; bmSize += 8) {
		for (size_t offset = 0; offset < bmSize; ++offset) {
			BitmapOper::setBit(bitmap, bmSize, offset);
			CPPUNIT_ASSERT(BitmapOper::isSet(bitmap, bmSize, offset));
			BitmapOper::clearBit(bitmap, bmSize, offset);
			CPPUNIT_ASSERT(!BitmapOper::isSet(bitmap, bmSize, offset));
		}
	}

	// 测试nextSet接口
	memset(bitmap, 0, sizeof(bitmap));
	size_t bmSize = 16;

	CPPUNIT_ASSERT( -1 == BitmapOper::nextSet(bitmap, bmSize, 0));

	size_t nullPos[] = {1, 6, 8, 15};
	for (size_t i = 0; i < sizeof(nullPos) / sizeof(nullPos[0]); ++i)
		BitmapOper::setBit(bitmap, bmSize, nullPos[i]);
	size_t nextNull = 0;
	for (size_t i = 0; i < sizeof(nullPos) / sizeof(nullPos[0]); ++i) {
		nextNull = BitmapOper::nextSet(bitmap, bmSize, nextNull);
		CPPUNIT_ASSERT(nextNull == nullPos[i]);
		nextNull ++;
	}

	// 测试isZero接口
	memset(bitmap, 0, sizeof(bitmap));
	for (size_t bmBytes = 1; bmBytes <=8; bmBytes ++)
		CPPUNIT_ASSERT(BitmapOper::isZero(bitmap, bmBytes));

}


void BitmapTestCase::testBitmap() {
	for (size_t bmSize = 0; bmSize <= 16 * 8; bmSize += 8 ) {
		Bitmap bitmap(bmSize);
		for (size_t offset = 0; offset < bmSize; ++offset) {
			bitmap.setBit(offset);
			CPPUNIT_ASSERT(bitmap.isSet(offset));
			bitmap.clearBit(offset);
			CPPUNIT_ASSERT(!bitmap.isSet(offset));
		}
	}
	
}


