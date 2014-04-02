/**
*	实现对建索引键值外部排序
*
*	author: naturally (naturally@163.org)
*/

// OuterSorter.cpp: implementation of the RunSet and Sorter MinHeap classes.
//
//////////////////////////////////////////////////////////////////////

#include <iostream>
#include <algorithm>
#include "btree/OuterSorter.h"
#include "btree/IndexKey.h"
#include "util/File.h"
#include "misc/Record.h"
#include "misc/ControlFile.h"
#include "api/Database.h"
#include "api/Table.h"
#include "heap/Heap.h"


namespace ntse {


/**
 * 最小堆析构函数
 */
MinHeap::~MinHeap() {
	delete[] m_RSNos;
	delete[] m_content;
}


/**
 * 插入一个元素到堆，如果堆空间不够，翻倍扩展
 *
 * @param RSNo		RunSet号
 * @param content	对应的实质内容
 */
void MinHeap::push(uint RSNo, void *content) {
	if (m_cur == m_size)	// 空间不够，需要扩展
		extendHeap();

	assert(m_cur < m_size);

	// 堆排序
	uint child = m_cur++;
	while (child > 0) {
		uint parent = (child - 1) >> 1;
		s32 result = compare(m_content[parent], content);
		if (result <= 0)
			break;

		m_RSNos[child] = m_RSNos[parent];
		m_content[child] = m_content[parent];
		child = parent;
	}

	m_RSNos[child] = RSNo;
	m_content[child] = content;
}



/**
 * 扩展堆空间到现有大小的一倍
 */
void MinHeap::extendHeap() {
	m_size *= 2;
	uint *newRSNo = new uint[m_size];
	void **newContent = new void*[m_size];

	memcpy(newRSNo, m_RSNos, sizeof(uint) * m_cur);
	memcpy(newContent, m_content, sizeof(void*) * m_cur);

	delete[] m_RSNos;
	delete[] m_content;

	m_RSNos = newRSNo;
	m_content = newContent;
}


/**
 * 从最小堆获取堆顶的数据
 * @return 堆顶项的RunSet号，如果堆为空，返回-1
 */
uint MinHeap::pop() {
	if (m_cur == 0)
		return (uint)-1;

	uint popNo = m_RSNos[0];

	m_cur--;
	uint RSNo = m_RSNos[m_cur];
	void *content = m_content[m_cur];
	m_content[m_cur] = NULL;

	uint parent = 0;
	while (true) {
		s32 result;
		uint child = (parent << 1) + 1;	// 左儿子
		if (child >= m_cur)
			break;

		if (child + 1 < m_cur) {	// 首先比较右儿子
			result = compare(m_content[child], m_content[child + 1]);
			if (result > 0)
				++child;
		}
		result = compare(content, m_content[child]);
		if (result <= 0)
			break;

		m_RSNos[parent] = m_RSNos[child];
		m_content[parent] = m_content[child];
		parent = child;
	}

	m_RSNos[parent] = RSNo;
	m_content[parent] = content;

	if (m_cur < m_size / 4)	// 为了防止抖动，只有空间需求在现在容量的1/4以下才缩小空间
		shrinkHeap();

	return popNo;
}



/**
 * 收缩最小堆空间到原有大小的一半
 */
void MinHeap::shrinkHeap() {
	assert(m_size >= m_cur * 2);

	m_size /= 2;
	uint *newRSNo = new uint[m_size];
	void **newContent = new void*[m_size];

	memcpy(newRSNo, m_RSNos, sizeof(uint) * m_cur);
	memcpy(newContent, m_content, sizeof(void*) * m_cur);

	delete[] m_RSNos;
	delete[] m_content;

	m_RSNos = newRSNo;
	m_content = newContent;
}



/**
 * 清空当前堆内容
 */
void MinHeap::reset() {
	m_cur = 0;
}


/**
 * 返回当前堆元素个数
 * @return 堆元素个数
 */
uint MinHeap::getValidSize() {
	return m_cur;
}


/**
 * 根据指定大小初始化堆
 *
 * @param size	堆初始大小
 */
void MinHeap::_init(uint size) {
	m_size = size;
	m_cur = 0;
	m_RSNos = new uint[m_size];
	m_content = new void*[m_size];
}


/**
 * MinHeapForData类比较堆中两个元素的大小
 *
 * @pre content必须是指向某段保存了记录长度和数据内容的地址
 * @param content1	比较内容1
 * @param content2	比较内容2
 * @return 返回1，0，-1表示content1大于、等于、小于content2
 */
s32 MinHeapForData::compare(void *content1, void *content2) {
	// 分别读取两条记录的长度、数据、rowId信息
	Sorter::readKeyFromMem(&key1, (byte*)content1);
	Sorter::readKeyFromMem(&key2, (byte*)content2);

	int result = m_comparator(m_tableDef, &key1, &key2, m_indexDef);
	if (result == 0)
		return key1.m_rowId > key2.m_rowId ? 1 : key1.m_rowId == key2.m_rowId ? 0 : -1;
	else
		return result;
}


/**
 * MinHeapForKey类比较堆中两个元素的大小
 *
 * @pre content必须是SubRecord类型的指针
 * @param content1	比较内容1
 * @param content2	比较内容2
 * @return 返回1，0，-1表示content1大于、等于、小于content2
 */
s32 MinHeapForKey::compare(void *content1, void *content2) {
	SubRecord *key1 = (SubRecord*)content1;
	SubRecord *key2 = (SubRecord*)content2;

	int result = m_comparator(m_tableDef, key1, key2, m_indexDef);
	if (result == 0)
		return key1->m_rowId > key2->m_rowId ? 1 : key1->m_rowId == key2->m_rowId ? 0 : -1;
	else
		return result;
}


/**
 * MinHeapForRunSet类比较堆中两个元素的大小
 *
 * @pre content必须是SubRecord类型的指针
 * @param content1	比较内容1
 * @param content2	比较内容2
 * @return 返回1，0，-1表示content1大于、等于、小于content2
 */
s32 MinHeapForRunSet::compare(void *content1, void *content2) {
	u64 size1 = (u64)content1;
	u64 size2 = (u64)content2;

	return size1 > size2;
}


//////////////////////////////////////////////////////////////////////////////////////////////////////////////


/**
 * 排序器构造函数
 * @param cfile			控制文件句柄，用于分配回收临时文件
 * @param lobStorage	大对象管理器
 * @param indexDef		要创建索引的定义
 * @param tableDef		创建索引所属的堆表定义
 */
Sorter::Sorter(ControlFile *cfile, LobStorage *lobStorage, const IndexDef *indexDef, const TableDef *tableDef) {
	m_cfile = cfile;
	m_recordMaxLen = indexDef->m_maxKeySize + RID_BYTES + sizeof(u16) + 1;
	m_tableDef = tableDef;
	m_keyNeedCompress = RecordOper::isFastCCComparable(m_tableDef, indexDef, (u16)indexDef->m_numCols, indexDef->m_columns);
	m_indexDef = indexDef;
	m_lobStorage = lobStorage;

	m_template = new SubRecord();
	IndexKey::initialKey(m_template, m_keyNeedCompress ? KEY_COMPRESS : KEY_NATURAL, indexDef->m_numCols, indexDef->m_columns, m_recordMaxLen - sizeof(u16), INVALID_ROW_ID);

	uint totalSize = MERGE_PAGE_NUM * MERGE_PAGE_SIZE;
	m_memory = (byte*)System::virtualAlloc(MERGE_PAGE_NUM * MERGE_PAGE_SIZE);
	m_runSetHeap = new MinHeapForRunSet(MERGE_PAGE_NUM);
	m_dataHeap = new MinHeapForData(m_tableDef, m_indexDef, m_keyNeedCompress, m_template, totalSize / m_recordMaxLen);
	m_keyHeap = new MinHeapForKey(m_tableDef, m_indexDef, m_keyNeedCompress, MERGE_PAGE_NUM);
	memset(m_runSets, 0, sizeof(inuseRSParis) * MERGE_PAGE_NUM);
}


/**
 * 排序器析构函数
 */
Sorter::~Sorter() {
	if (m_template->m_data != NULL)
		delete[] m_template->m_data;
	if (m_template != NULL)
		delete m_template;
	if (m_runSetHeap != NULL)
		delete m_runSetHeap;
	if (m_dataHeap != NULL)
		delete m_dataHeap;
	if (m_keyHeap != NULL)
		delete m_keyHeap;

	try {
		closeRunSets(MERGE_PAGE_NUM);
	} catch (NtseException &e) {
		if (m_memory != NULL)
			System::virtualFree(m_memory);
		unregisterRSFiles();
		throw e;
	}

	if (m_memory != NULL)
		System::virtualFree(m_memory);

	// 注销删除所有临时文件
	unregisterRSFiles();
}


/**
 * 排序器的主排序函数，将会初始化生成各个RunSets，归并排序直到最后生成一个可获取结果的状态
 * @param session	会话句柄
 * @param heap		要创建索引的堆
 */
void Sorter::sort(Session *session, DrsHeap *heap) throw(NtseException) {
	createRunSets(session, heap);
	mergeSort();
	prepareLast();
}


/**
 * 读取表中的所有数据构造索引键值，排序生成各个RunSet文件，等待归并
 *
 * @param session	会话句柄
 * @param heap		堆对象
 * @throw 文件操作异常
 */
void Sorter::createRunSets(Session *session, DrsHeap *heap) throw(NtseException) {
	char *schemaName = m_tableDef->m_schemaName;
	char *tableName = m_tableDef->m_name;
	
	SubRecord record;	// 用于从表中读取数据
	u16 columns[Limits::MAX_COL_NUM];
	memcpy(columns, m_template->m_columns, m_template->m_numCols * sizeof(u16));
	std::sort(columns, columns + m_template->m_numCols);
	IndexKey::initialKey(&record, REC_REDUNDANT, m_template->m_numCols, columns, m_tableDef->m_maxRecSize, INVALID_ROW_ID);

	ExtractKey extractor = m_keyNeedCompress ? RecordOper::extractKeyRC : RecordOper::extractKeyRN;
	CompareKey comparator = m_keyNeedCompress ? RecordOper::compareKeyCC : RecordOper::compareKeyNN;

	RunSet *runSet = NULL;
	DrsHeapScanHandle *scanHandle = NULL;
	char *rsPath = NULL;
	try {
		SubRecord *lastWritten = NULL, curKey;
		memcpy(&curKey, m_template, sizeof(SubRecord));

		bool writing = false;
		uint RSNo = 0, recordNo = 0;
		byte *freeStart = m_memory;
		byte *end = m_memory + (MERGE_PAGE_NUM - 1) * MERGE_PAGE_SIZE;
		byte *cache = end;
		rsPath = m_cfile->allocTempFile(schemaName, tableName);
		runSet = new RunSet(rsPath, cache, MERGE_PAGE_SIZE, m_template, m_recordMaxLen - sizeof(u16), false);

		// 打开一个表扫描读取表的每条记录
		SubrecExtractor *srExtractor = SubrecExtractor::createInst(session, m_tableDef, &record, (uint)-1, heap->getCompressRcdExtrator());
		assert(srExtractor != NULL);

		// 如果调用者保证加表锁，那么这里的扫描参数returnLinkSrc可以设为false提高性能
		// 但是在线建索引模块会在不加表锁的情况下使用本方法创建索引，出于正确性考虑设为true最保险，期待后续改进 by: naturally
		scanHandle = heap->beginScan(session, srExtractor, None, NULL, true);
		while (true) {
			McSavepoint lobSavepoint(session->getLobContext());

			if (!heap->getNext(scanHandle, &record))
				break;

			Array<LobPair*> lobArray;
			if (m_indexDef->hasLob()) {
				RecordOper::extractLobFromR(session, m_tableDef, m_indexDef, m_lobStorage, &record, &lobArray);
			}

			// 从冗余格式的表记录提取合适格式的索引键值
			makeIndexKey(m_tableDef, m_indexDef, &record, &lobArray, extractor);
		
			if (!writing) {	// 缓存还没有满，正常排序
				dealWithNewKey(&freeStart, recordNo, m_recordMaxLen);
				recordNo++;
				if (freeStart + m_recordMaxLen > end) {	// 当前缓存使用满，写出最小记录
					writing = true;
					recordNo = flushMinKey(&freeStart, runSet, &curKey);
					lastWritten = &curKey;
				}
			} else {	// 缓存满，分情况处理
				if (comparator(m_tableDef, lastWritten, m_template, m_indexDef) > 0) {	// 无序
					// 一个RunSet生成，将信息存入堆中
					flushAllMem(runSet);
					RSNo = finishRunSetCreation(runSet, RSNo);

					// 重置环境
					freeStart = m_memory;
					writing = false;
					delete runSet;
					rsPath = m_cfile->allocTempFile(schemaName, tableName);
					runSet = new RunSet(rsPath, cache, MERGE_PAGE_SIZE, m_template, m_recordMaxLen - sizeof(u16), false);
					rsPath = NULL;
					m_dataHeap->reset();
					lastWritten = NULL;

					// 保存当前记录
					dealWithNewKey(&freeStart, 0, m_recordMaxLen);
					recordNo = 1;
				} else {	// 加入排序，刷出最小记录
					dealWithNewKey(&freeStart, recordNo, 0);
					recordNo = flushMinKey(&freeStart, runSet, &curKey);
					lastWritten = &curKey;
				}
			}
		}

		// 刷出缓存中所有的记录，保存RS信息到堆排序
		flushAllMem(runSet);
		m_RSNo = finishRunSetCreation(runSet, RSNo);
		rsPath = NULL;

		heap->endScan(scanHandle);
		scanHandle = NULL;

		delete[] record.m_data;
		delete runSet;
	} catch (NtseException &e) {
		if (scanHandle != NULL)
			heap->endScan(scanHandle);
		delete[] record.m_data;
		if (runSet != NULL) {
			runSet->close();
			delete runSet;
		}
		unregisterRSFiles();
		if (rsPath != NULL)
			delete [] rsPath;
		throw e;
	}
}


/**
 * 从表中读取一条记录之后的处理函数，记录保存在m_template当中，存入缓存，放入记录堆
 *
 * @param start		指向内存地址的指针
 * @param keyNo		当前记录的序号
 * @param offset	内存地址需要调整的偏移量
 */
void Sorter::dealWithNewKey(byte **start, uint keyNo, u16 offset) {
	saveKeyToMem(*start, m_template);
	m_dataHeap->push(keyNo, *start);
	(*start) += offset;
}



/**
 * 刷出当前键值堆的最小键值
 *
 * @param start		IN/OUT	返回最小键值存储的内存地址
 * @param runSet	指定的RunSet
 * @param minKey	OUT	保存最小键值内容
 * @return 最小键值序号
 */
uint Sorter::flushMinKey(byte **start, RunSet *runSet, SubRecord *minKey) {
	// 取得最小记录序号
	uint keyNo = m_dataHeap->pop();
	assert(keyNo != (uint)-1);

	// 写出该记录
	*start = m_memory + m_recordMaxLen * keyNo;
	readKeyFromMem(minKey, *start);
	runSet->writeKey(minKey);

	return keyNo;
}



/**
 * 在创建初始RunSet过程中，对于一个RunSet数据准备完毕之后的刷磁盘，将RunSet加入堆操作
 *
 * @param runSet	准备结束的RunSet
 * @param RSNo		当前RunSet的编号
 * @return 下一个可使用的RunSet编号
 * @throw 抛出文件操作异常
 */
uint Sorter::finishRunSetCreation(RunSet *runSet, uint RSNo) throw(NtseException) {
	runSet->close();
	m_runSetHeap->push(RSNo, (void*)runSet->getFileSize());
	m_rsMap.insert(rsParis(RSNo, (char*)runSet->getFileName()));

	return RSNo + 1;
}



/**
 * 进行归并排序，直到剩下的RunSet数可以在一趟归并里面完成
 * @pre 必须首先调用createRunSets创建好各个RunSets
 *
 * @throw	文件读写异常
 */
void Sorter::mergeSort() throw(NtseException) {
	uint leftRSNum;
	byte *cache = m_memory + MERGE_PAGE_SIZE * (MERGE_PAGE_NUM - 1);
	char *schemaName = m_tableDef->m_schemaName, *tableName = m_tableDef->m_name;
	char *rsfile;

	while ((leftRSNum = m_runSetHeap->getValidSize()) > MERGE_PAGE_NUM) {	// 循环归并直到RunSet剩下MERGE_PAGE_NUM个或者更少
		uint runSetNum = (leftRSNum >= 2 * MERGE_PAGE_NUM - 1) ? MERGE_PAGE_NUM - 1 : leftRSNum - MERGE_PAGE_NUM + 1;
		assert(runSetNum <= leftRSNum && runSetNum >= 2);
		runSetNum = getProperRunSets(runSetNum);	// 这里应该保证如果剩下的RunSet不足MERGE_PAGE_NUM * 2 - 1，那么必须保证最后一次归并要使用MERGE_PAGE_NUM个RunSet
		assert(runSetNum == m_keyHeap->getValidSize());
		assert(runSetNum < MERGE_PAGE_NUM);

		try {
			rsfile = m_cfile->allocTempFile(schemaName, tableName);
			RunSet runSet(rsfile, cache, MERGE_PAGE_SIZE, m_template, m_recordMaxLen, false);

			uint RSNo;
			while ((RSNo = m_keyHeap->pop()) != (uint)-1) {	// 从各个RunSet中取最小记录写入排序缓存，再读取该RS下一个记录插入堆排序
				SubRecord *record = m_runSets[RSNo].second->getLastRecord();
				runSet.writeKey(record);

				record = m_runSets[RSNo].second->getNextRecord();
				if (record != NULL)
					m_keyHeap->push(RSNo, record);
			}

			// 将新的RunSet加入排序
			m_RSNo = finishRunSetCreation(&runSet, m_RSNo);
			closeRunSets(runSetNum);
		} catch (NtseException &e) {
			closeRunSets(runSetNum);
			assert(false);
			throw e;
		}
	}
}



/**
 * 准备开始进行最后一趟归并直接读数据返回
 * @pre 必须调用mergeSort将各个RunSets归并剩下指定个数
 * @throw 文件读取出错
 */
void Sorter::prepareLast() throw(NtseException) {
	uint leftRSNum = m_runSetHeap->getValidSize();
	assert(leftRSNum <= MERGE_PAGE_NUM && leftRSNum > 0);
	getProperRunSets(leftRSNum);
}



/**
 * 在最后一趟归并当中获得下一个最小键值，返回的键值一定是压缩过的
 * @return 返回下一个最小键值，如果没有返回NULL
 * @throw 文件操作异常
 */
SubRecord* Sorter::getSortedNextKey() throw(NtseException) {
	uint RSNo = m_keyHeap->pop();
	if (RSNo == (uint)-1)
		return NULL;

	// 处理当前RS的键值
	SubRecord *record = m_runSets[RSNo].second->getLastRecord();
	if (!m_keyNeedCompress) {	// 需要先压缩再返回
		m_template->m_format = KEY_COMPRESS;
		m_template->m_size = m_recordMaxLen - sizeof(u16);
		IndexKey::compressKey(m_tableDef, m_indexDef, record, m_template, false);
	} else {
		IndexKey::copyKey(m_template, record, false);
	}

	// 取下一个键值放入堆排序
	SubRecord *nextRecord = m_runSets[RSNo].second->getNextRecord();
	if (nextRecord != NULL)
		m_keyHeap->push(RSNo, nextRecord);

	return m_template;
}


/**
 * 获取指定数目的RunSet保存到m_runSets，如果剩余的RunSet不够，就只返回剩余的RunSet
 *
 * @param runSetNum
 * @return 获取的RunSet数目
 * @throw 文件IO异常等
 */
uint Sorter::getProperRunSets(u32 runSetNum) throw(NtseException) {
	uint realRSNum = 0;
	char *rspath;
	rsItor itor;

	m_keyHeap->reset();
	// 初始化每一个RunSet，读取每个RS的第一条记录创建初始最小堆
	for (uint i = 0; i < runSetNum;) {
		uint RSNo = m_runSetHeap->pop();
		if (RSNo == (uint)-1)
			return realRSNum;

		try {
			itor = m_rsMap.find(RSNo);
			assert(itor != m_rsMap.end());
			rspath = itor->second;
			m_runSets[realRSNum] = inuseRSParis(RSNo, new RunSet(rspath, m_memory + MERGE_PAGE_SIZE * i, MERGE_PAGE_SIZE, m_template, m_recordMaxLen, true));
		} catch (NtseException &e) {
			closeRunSets(realRSNum);
			throw e;
		}

		SubRecord *record = m_runSets[realRSNum].second->getNextRecord();
		if (record != NULL)
			m_keyHeap->push(realRSNum, record);
		else {
			delete m_runSets[realRSNum].second;
			m_runSets[realRSNum].second = NULL;
			continue;
		}

		realRSNum++;
		i++;
	}

	return realRSNum;
}



/**
 * 根据m_keyHeap顺序刷出当前m_memory中的所有索引键值
 *
 * @param runSet	要操作的RunSet
 * @throw	文件操作异常
 */
void Sorter::flushAllMem(RunSet *runSet) throw(NtseException) {
	uint recordNo;
	SubRecord curKey;
	memcpy(&curKey, m_template, sizeof(SubRecord));

	while ((recordNo = m_dataHeap->pop()) != (uint)-1) {	// 依次从堆中读取有序数据写入文件
		readKeyFromMem(&curKey, m_memory + m_recordMaxLen * recordNo);
		runSet->writeKey(&curKey);
	}
}



/**
 * 将指定的SubRecord以冗余形式存在指定内存，只存储两个字节的size以及最大表示长度的data的内容
 *
 * @param freeStart	保存的内存起始地址
 * @param record	要保存的记录
 */
void Sorter::saveKeyToMem(byte *freeStart, SubRecord *record) {
	u16 size = (u16)record->m_size;
	memcpy(freeStart, (byte*)&size, sizeof(u16));
	memcpy(freeStart + sizeof(u16), record->m_data, size);
	RID_WRITE(record->m_rowId, freeStart + sizeof(u16) + size);
	*(freeStart + sizeof(u16) + size + RID_BYTES) = '\0';	// 标识数据的结束
}


/**
 * 从指定内存地址读取键值的size、data和rowId信息
 *
 * @param key		索引键值
 * @param memory	指定的内存
 */
void Sorter::readKeyFromMem(SubRecord *key, byte *memory) {
	key->m_size = *(u16*)memory;
	key->m_data = memory + sizeof(u16);
	key->m_rowId = RID_READ(memory + sizeof(u16) + key->m_size);
}


/**
 * 将从Heap读取的SubRecord构造出索引键值表达形式保存到m_template
 *
 * @param tableDef	表定义
 * @param indexDef	索引定义
 * @param record	Heap读取的记录
 * @param lobArray	构造索引键所需大对象队列
 * @param extractor	索引键值生成器
 */
void Sorter::makeIndexKey(const TableDef *tableDef, const IndexDef *indexDef, SubRecord *record, Array<LobPair*> *lobArray, ExtractKey extractor) {
	Record tableRecord;	// 用于构造出来供extractor使用
	tableRecord.m_format = REC_REDUNDANT;

	tableRecord.m_rowId = record->m_rowId;
	tableRecord.m_size = record->m_size;
	tableRecord.m_data = record->m_data;

	m_template->m_size = m_recordMaxLen - sizeof(u16);	// 为了让extractor知道可用长度
	m_template->m_rowId = tableRecord.m_rowId;
	extractor(tableDef, indexDef, &tableRecord, lobArray, m_template);
}


/**
 * 关闭指定数量的Sorter所拥有的RunSet
 *
 * @param size	指定要关闭RunSet的数量
 * @throw 文件操作异常
 */
void Sorter::closeRunSets(uint size) throw (NtseException) {
	assert(size <= MERGE_PAGE_NUM);
	for (uint i = 0; i < size; i++)
		if (m_runSets[i].second != NULL) {
			char* rspath = (char*)m_runSets[i].second->getFileName();
			uint RSNo = m_runSets[i].first;
			m_runSets[i].second->close();
			delete m_runSets[i].second;
			m_runSets[i].second = NULL;
			// 从map当中删除
			rsItor itor = m_rsMap.find(RSNo);
			assert(itor != m_rsMap.end());
			m_rsMap.erase(itor);
			m_cfile->unregisterTempFile(rspath);
			delete [] rspath;
		}
}

/**
 * 集中将申请的RS文件全部注销掉
 * 即使出现文件错误，也会尝试所有都注销一次
 */
void Sorter::unregisterRSFiles() {
	if (!m_rsMap.empty()) {
		for (rsItor itor = m_rsMap.begin(); itor != m_rsMap.end(); itor++) {
			File *file = new File(itor->second);
			file->remove();	// 这里没法处理出错的情况，一定要硬着头皮删除光
			delete file;
			m_cfile->unregisterTempFile(itor->second);
			delete [] itor->second;
		}
	}
}


//////////////////////////////////////////////////////////////////////////////////////////////////////////////


/**
 * RunSet构造函数
 * @param path			指定使用的文件路径
 * @param memory		指定可以使用的内存空间
 * @param size			内存大小
 * @param indexKey		索引键格式模板
 * @param maxKeySize	索引键最大长度
 * @param readOnly		是否是读的RunSet，否则为写的
 */
RunSet::RunSet(const char *path, byte *memory, uint size, SubRecord *indexKey, uint maxKeySize, bool readOnly) {
	m_readOnly = readOnly;

	m_lastRecord = NULL;
	m_path = path;
	m_file = new File(path);
	if (readOnly) {	// 用于读的RunSet文件本身已经存在，只需要打开
		u64 errNo = m_file->open(true);
		if (File::getNtseError(errNo) != File::E_NO_ERROR) {
			delete m_file;
			NTSE_THROW(errNo, "Cannot OPEN temp file %s when doing merge sort. %s", path, File::explainErrno(errNo));
		}

		errNo = m_file->getSize(&m_fileSize);
		if (File::getNtseError(errNo) != File::E_NO_ERROR) {
			delete m_file;
			NTSE_ASSERT(false);
		}
	} else {	// 用于写的文件需要重新建立
		u64 errNo = m_file->create(true, false);
		if (File::getNtseError(errNo) != File::E_NO_ERROR) {
			delete m_file;
			NTSE_THROW(errNo, "Cannot CREATE temp file %s when doing merge sort. %s", path, File::explainErrno(errNo));
		}
	}

	m_fileOpen = true;
	m_memory = memory;
	m_memSize = size;
	memset(m_memory, 0xFF, m_memSize);
	m_read = m_memory;
	m_curBlockNo = 0;
	m_lastRecord = new SubRecord();
	IndexKey::initialKey(m_lastRecord, indexKey->m_format, indexKey->m_numCols, indexKey->m_columns, maxKeySize, INVALID_ROW_ID);
	m_lastRecord->m_size = 0;	// 第一次比较需要标识为空记录
}


/**
 * RunSet析构函数
 */
RunSet::~RunSet() {
	if (m_fileOpen)
		close();

	if (m_lastRecord) {
		delete[] m_lastRecord->m_data;
		delete m_lastRecord;
	}
	delete m_file;
}




/**
 * 调用该函数保证RunSet对应的文件被关闭
 * @throw 文件操作异常
 */
void RunSet::close() throw(NtseException) {
	if (!m_fileOpen)
		return;

	if (!m_readOnly)
		flushCache();

	u64 errNo = m_file->close();
	if (File::getNtseError(errNo) != File::E_NO_ERROR) {
		NTSE_THROW(errNo, "Cannot CLOSE temp file when doing merge sort. %s", File::explainErrno(errNo));
	}

	if (m_readOnly) {	// 如果是写数据状态不需要删除文件，读数据状态必须删除
		errNo = m_file->remove();
		if (File::getNtseError(errNo) != File::E_NO_ERROR) {
			NTSE_THROW(errNo, "Cannot REMOVE temp file when doing merge sort. %s", File::explainErrno(errNo));
		}
	}

	m_fileOpen = false;
}



/**
 * 取得当前RunSet的下一条记录
 *
 * @pre readOnly为true
 * @return 下一条记录
 * @throw	文件读取异常
 */
SubRecord* RunSet::getNextRecord() throw(NtseException) {
	assert(m_readOnly);

	if (m_read >= m_memory + m_memSize || *m_read == 0xFF) {	// 当前页读完，尝试读取下一个页面
		if (!readNextBlock())
			return NULL;
		if (*m_read == 0xFF)
			return NULL;
	}

	IndexKey *indexKey = (IndexKey*)m_read;
	m_read += indexKey->extractKey(m_lastRecord->m_data, m_lastRecord, false);
	assert(m_read <= m_memory + m_memSize);

	return m_lastRecord;
}



/**
 * 取得当前RunSet的上一条记录
 *
 * @pre readOnly为true
 * @return 上一条记录
 */
SubRecord* RunSet::getLastRecord() {
	assert(m_readOnly);
	return m_lastRecord;
}



/**
 * 读取文件中的下一个块的数据
 *
 * @pre readOnly为true
 * @return	是否读取了下一个数据块
 * @throw	文件读取异常
 */
bool RunSet::readNextBlock() throw(NtseException) {
	assert(m_readOnly);

	if (m_curBlockNo * m_memSize >= m_fileSize) {	// 文件读到末尾，关闭文件
		// 关闭并根据情况删除关联文件
		close();
		return false;
	}

	u64 errNo = m_file->read(m_curBlockNo * m_memSize, m_memSize, m_memory);
	if (File::getNtseError(errNo) != File::E_NO_ERROR)
		NTSE_THROW(errNo, "Cannot READ temp file when doing merge sort. %s", File::explainErrno(errNo));

	m_curBlockNo++;
	m_read = m_memory;

	return true;
}



/**
 * 向RunSet当中写入一个键值，本地处理前缀压缩
 *
 * @pre readOnly为false
 * @param record	要插入的键值
 * @throw 文件操作异常
 */
void RunSet::writeKey(SubRecord *record) throw(NtseException) {
	assert(!m_readOnly);

	u16 prefixLen = IndexKey::computePrefix(m_lastRecord, record);
	u16 needSpace = IndexKey::computeSpace(record, prefixLen, false);

	assert(m_read < m_memory + m_memSize);
	if (isCacheFull(needSpace)) {	// 空间不够，首先刷当前缓存
		flushCache();
		prefixLen = 0;
		memset(m_memory, 0xFF, m_memSize);
		m_read = m_memory;
	}

	// 当前位置依次写入前缀、后缀长度以及后缀数据和rowId信息
	IndexKey *indexKey = (IndexKey*)m_read;
	needSpace = (u16)(indexKey->compressAndSaveKey(record, prefixLen, false) - m_read);
	m_read += needSpace;

	// 维护RunSet最后插入键值
	IndexKey::copyKey(m_lastRecord, record, false);
}


/**
 * 判断当前缓存是否空间不够写入下一条记录
 *
 * @param	needSpace	下一条件记录需要的空间
 * @return	true表示缓存已满不够写入，false相反
 */
bool RunSet::isCacheFull(u16 needSpace) {
	return m_memory + m_memSize <= m_read + needSpace;
}



/**
 * 将当前页面刷出到磁盘
 *
 * @pre readOnly为false，文件当前大小已满，必须扩展新的页面
 * @throw 文件读写异常
 */
void RunSet::flushCache() throw(NtseException) {
	assert(!m_readOnly);

	if (m_read == m_memory)	// 这种情况不写入数据，本来就是空的页
		return;

	u64 size = (++m_curBlockNo) * m_memSize;
	u64 errNo = m_file->setSize(size);
	if (File::getNtseError(errNo) != File::E_NO_ERROR)
		NTSE_THROW(errNo, "Cannot SET SIZE from temp file when doing merge sort. %s", File::explainErrno(errNo));

	errNo = m_file->write(size - m_memSize, m_memSize, m_memory);
	if (File::getNtseError(errNo) != File::E_NO_ERROR)
		NTSE_THROW(errNo, "Cannot WRITE temp file when doing merge sort. %s", File::explainErrno(errNo));
}



/**
 * 获得关联文件的当前大小
 *
 * @pre readOnly为false
 * @return 关联文件的大小
 */
u64 RunSet::getFileSize() const {
	return m_curBlockNo * m_memSize;
}

/**
 * 返回runset的文件名
 */
const char* RunSet::getFileName() const {
	return m_path;
}

}

