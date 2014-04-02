/**
*	ʵ�ֶԽ�������ֵ�ⲿ����
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
 * ��С����������
 */
MinHeap::~MinHeap() {
	delete[] m_RSNos;
	delete[] m_content;
}


/**
 * ����һ��Ԫ�ص��ѣ�����ѿռ䲻����������չ
 *
 * @param RSNo		RunSet��
 * @param content	��Ӧ��ʵ������
 */
void MinHeap::push(uint RSNo, void *content) {
	if (m_cur == m_size)	// �ռ䲻������Ҫ��չ
		extendHeap();

	assert(m_cur < m_size);

	// ������
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
 * ��չ�ѿռ䵽���д�С��һ��
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
 * ����С�ѻ�ȡ�Ѷ�������
 * @return �Ѷ����RunSet�ţ������Ϊ�գ�����-1
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
		uint child = (parent << 1) + 1;	// �����
		if (child >= m_cur)
			break;

		if (child + 1 < m_cur) {	// ���ȱȽ��Ҷ���
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

	if (m_cur < m_size / 4)	// Ϊ�˷�ֹ������ֻ�пռ�����������������1/4���²���С�ռ�
		shrinkHeap();

	return popNo;
}



/**
 * ������С�ѿռ䵽ԭ�д�С��һ��
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
 * ��յ�ǰ������
 */
void MinHeap::reset() {
	m_cur = 0;
}


/**
 * ���ص�ǰ��Ԫ�ظ���
 * @return ��Ԫ�ظ���
 */
uint MinHeap::getValidSize() {
	return m_cur;
}


/**
 * ����ָ����С��ʼ����
 *
 * @param size	�ѳ�ʼ��С
 */
void MinHeap::_init(uint size) {
	m_size = size;
	m_cur = 0;
	m_RSNos = new uint[m_size];
	m_content = new void*[m_size];
}


/**
 * MinHeapForData��Ƚ϶�������Ԫ�صĴ�С
 *
 * @pre content������ָ��ĳ�α����˼�¼���Ⱥ��������ݵĵ�ַ
 * @param content1	�Ƚ�����1
 * @param content2	�Ƚ�����2
 * @return ����1��0��-1��ʾcontent1���ڡ����ڡ�С��content2
 */
s32 MinHeapForData::compare(void *content1, void *content2) {
	// �ֱ��ȡ������¼�ĳ��ȡ����ݡ�rowId��Ϣ
	Sorter::readKeyFromMem(&key1, (byte*)content1);
	Sorter::readKeyFromMem(&key2, (byte*)content2);

	int result = m_comparator(m_tableDef, &key1, &key2, m_indexDef);
	if (result == 0)
		return key1.m_rowId > key2.m_rowId ? 1 : key1.m_rowId == key2.m_rowId ? 0 : -1;
	else
		return result;
}


/**
 * MinHeapForKey��Ƚ϶�������Ԫ�صĴ�С
 *
 * @pre content������SubRecord���͵�ָ��
 * @param content1	�Ƚ�����1
 * @param content2	�Ƚ�����2
 * @return ����1��0��-1��ʾcontent1���ڡ����ڡ�С��content2
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
 * MinHeapForRunSet��Ƚ϶�������Ԫ�صĴ�С
 *
 * @pre content������SubRecord���͵�ָ��
 * @param content1	�Ƚ�����1
 * @param content2	�Ƚ�����2
 * @return ����1��0��-1��ʾcontent1���ڡ����ڡ�С��content2
 */
s32 MinHeapForRunSet::compare(void *content1, void *content2) {
	u64 size1 = (u64)content1;
	u64 size2 = (u64)content2;

	return size1 > size2;
}


//////////////////////////////////////////////////////////////////////////////////////////////////////////////


/**
 * ���������캯��
 * @param cfile			�����ļ���������ڷ��������ʱ�ļ�
 * @param lobStorage	����������
 * @param indexDef		Ҫ���������Ķ���
 * @param tableDef		�������������Ķѱ���
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
 * ��������������
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

	// ע��ɾ��������ʱ�ļ�
	unregisterRSFiles();
}


/**
 * �����������������������ʼ�����ɸ���RunSets���鲢����ֱ���������һ���ɻ�ȡ�����״̬
 * @param session	�Ự���
 * @param heap		Ҫ���������Ķ�
 */
void Sorter::sort(Session *session, DrsHeap *heap) throw(NtseException) {
	createRunSets(session, heap);
	mergeSort();
	prepareLast();
}


/**
 * ��ȡ���е��������ݹ���������ֵ���������ɸ���RunSet�ļ����ȴ��鲢
 *
 * @param session	�Ự���
 * @param heap		�Ѷ���
 * @throw �ļ������쳣
 */
void Sorter::createRunSets(Session *session, DrsHeap *heap) throw(NtseException) {
	char *schemaName = m_tableDef->m_schemaName;
	char *tableName = m_tableDef->m_name;
	
	SubRecord record;	// ���ڴӱ��ж�ȡ����
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

		// ��һ����ɨ���ȡ���ÿ����¼
		SubrecExtractor *srExtractor = SubrecExtractor::createInst(session, m_tableDef, &record, (uint)-1, heap->getCompressRcdExtrator());
		assert(srExtractor != NULL);

		// ��������߱�֤�ӱ�������ô�����ɨ�����returnLinkSrc������Ϊfalse�������
		// �������߽�����ģ����ڲ��ӱ����������ʹ�ñ���������������������ȷ�Կ�����Ϊtrue��գ��ڴ������Ľ� by: naturally
		scanHandle = heap->beginScan(session, srExtractor, None, NULL, true);
		while (true) {
			McSavepoint lobSavepoint(session->getLobContext());

			if (!heap->getNext(scanHandle, &record))
				break;

			Array<LobPair*> lobArray;
			if (m_indexDef->hasLob()) {
				RecordOper::extractLobFromR(session, m_tableDef, m_indexDef, m_lobStorage, &record, &lobArray);
			}

			// �������ʽ�ı��¼��ȡ���ʸ�ʽ��������ֵ
			makeIndexKey(m_tableDef, m_indexDef, &record, &lobArray, extractor);
		
			if (!writing) {	// ���滹û��������������
				dealWithNewKey(&freeStart, recordNo, m_recordMaxLen);
				recordNo++;
				if (freeStart + m_recordMaxLen > end) {	// ��ǰ����ʹ������д����С��¼
					writing = true;
					recordNo = flushMinKey(&freeStart, runSet, &curKey);
					lastWritten = &curKey;
				}
			} else {	// �����������������
				if (comparator(m_tableDef, lastWritten, m_template, m_indexDef) > 0) {	// ����
					// һ��RunSet���ɣ�����Ϣ�������
					flushAllMem(runSet);
					RSNo = finishRunSetCreation(runSet, RSNo);

					// ���û���
					freeStart = m_memory;
					writing = false;
					delete runSet;
					rsPath = m_cfile->allocTempFile(schemaName, tableName);
					runSet = new RunSet(rsPath, cache, MERGE_PAGE_SIZE, m_template, m_recordMaxLen - sizeof(u16), false);
					rsPath = NULL;
					m_dataHeap->reset();
					lastWritten = NULL;

					// ���浱ǰ��¼
					dealWithNewKey(&freeStart, 0, m_recordMaxLen);
					recordNo = 1;
				} else {	// ��������ˢ����С��¼
					dealWithNewKey(&freeStart, recordNo, 0);
					recordNo = flushMinKey(&freeStart, runSet, &curKey);
					lastWritten = &curKey;
				}
			}
		}

		// ˢ�����������еļ�¼������RS��Ϣ��������
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
 * �ӱ��ж�ȡһ����¼֮��Ĵ���������¼������m_template���У����뻺�棬�����¼��
 *
 * @param start		ָ���ڴ��ַ��ָ��
 * @param keyNo		��ǰ��¼�����
 * @param offset	�ڴ��ַ��Ҫ������ƫ����
 */
void Sorter::dealWithNewKey(byte **start, uint keyNo, u16 offset) {
	saveKeyToMem(*start, m_template);
	m_dataHeap->push(keyNo, *start);
	(*start) += offset;
}



/**
 * ˢ����ǰ��ֵ�ѵ���С��ֵ
 *
 * @param start		IN/OUT	������С��ֵ�洢���ڴ��ַ
 * @param runSet	ָ����RunSet
 * @param minKey	OUT	������С��ֵ����
 * @return ��С��ֵ���
 */
uint Sorter::flushMinKey(byte **start, RunSet *runSet, SubRecord *minKey) {
	// ȡ����С��¼���
	uint keyNo = m_dataHeap->pop();
	assert(keyNo != (uint)-1);

	// д���ü�¼
	*start = m_memory + m_recordMaxLen * keyNo;
	readKeyFromMem(minKey, *start);
	runSet->writeKey(minKey);

	return keyNo;
}



/**
 * �ڴ�����ʼRunSet�����У�����һ��RunSet����׼�����֮���ˢ���̣���RunSet����Ѳ���
 *
 * @param runSet	׼��������RunSet
 * @param RSNo		��ǰRunSet�ı��
 * @return ��һ����ʹ�õ�RunSet���
 * @throw �׳��ļ������쳣
 */
uint Sorter::finishRunSetCreation(RunSet *runSet, uint RSNo) throw(NtseException) {
	runSet->close();
	m_runSetHeap->push(RSNo, (void*)runSet->getFileSize());
	m_rsMap.insert(rsParis(RSNo, (char*)runSet->getFileName()));

	return RSNo + 1;
}



/**
 * ���й鲢����ֱ��ʣ�µ�RunSet��������һ�˹鲢�������
 * @pre �������ȵ���createRunSets�����ø���RunSets
 *
 * @throw	�ļ���д�쳣
 */
void Sorter::mergeSort() throw(NtseException) {
	uint leftRSNum;
	byte *cache = m_memory + MERGE_PAGE_SIZE * (MERGE_PAGE_NUM - 1);
	char *schemaName = m_tableDef->m_schemaName, *tableName = m_tableDef->m_name;
	char *rsfile;

	while ((leftRSNum = m_runSetHeap->getValidSize()) > MERGE_PAGE_NUM) {	// ѭ���鲢ֱ��RunSetʣ��MERGE_PAGE_NUM�����߸���
		uint runSetNum = (leftRSNum >= 2 * MERGE_PAGE_NUM - 1) ? MERGE_PAGE_NUM - 1 : leftRSNum - MERGE_PAGE_NUM + 1;
		assert(runSetNum <= leftRSNum && runSetNum >= 2);
		runSetNum = getProperRunSets(runSetNum);	// ����Ӧ�ñ�֤���ʣ�µ�RunSet����MERGE_PAGE_NUM * 2 - 1����ô���뱣֤���һ�ι鲢Ҫʹ��MERGE_PAGE_NUM��RunSet
		assert(runSetNum == m_keyHeap->getValidSize());
		assert(runSetNum < MERGE_PAGE_NUM);

		try {
			rsfile = m_cfile->allocTempFile(schemaName, tableName);
			RunSet runSet(rsfile, cache, MERGE_PAGE_SIZE, m_template, m_recordMaxLen, false);

			uint RSNo;
			while ((RSNo = m_keyHeap->pop()) != (uint)-1) {	// �Ӹ���RunSet��ȡ��С��¼д�����򻺴棬�ٶ�ȡ��RS��һ����¼���������
				SubRecord *record = m_runSets[RSNo].second->getLastRecord();
				runSet.writeKey(record);

				record = m_runSets[RSNo].second->getNextRecord();
				if (record != NULL)
					m_keyHeap->push(RSNo, record);
			}

			// ���µ�RunSet��������
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
 * ׼����ʼ�������һ�˹鲢ֱ�Ӷ����ݷ���
 * @pre �������mergeSort������RunSets�鲢ʣ��ָ������
 * @throw �ļ���ȡ����
 */
void Sorter::prepareLast() throw(NtseException) {
	uint leftRSNum = m_runSetHeap->getValidSize();
	assert(leftRSNum <= MERGE_PAGE_NUM && leftRSNum > 0);
	getProperRunSets(leftRSNum);
}



/**
 * �����һ�˹鲢���л����һ����С��ֵ�����صļ�ֵһ����ѹ������
 * @return ������һ����С��ֵ�����û�з���NULL
 * @throw �ļ������쳣
 */
SubRecord* Sorter::getSortedNextKey() throw(NtseException) {
	uint RSNo = m_keyHeap->pop();
	if (RSNo == (uint)-1)
		return NULL;

	// ����ǰRS�ļ�ֵ
	SubRecord *record = m_runSets[RSNo].second->getLastRecord();
	if (!m_keyNeedCompress) {	// ��Ҫ��ѹ���ٷ���
		m_template->m_format = KEY_COMPRESS;
		m_template->m_size = m_recordMaxLen - sizeof(u16);
		IndexKey::compressKey(m_tableDef, m_indexDef, record, m_template, false);
	} else {
		IndexKey::copyKey(m_template, record, false);
	}

	// ȡ��һ����ֵ���������
	SubRecord *nextRecord = m_runSets[RSNo].second->getNextRecord();
	if (nextRecord != NULL)
		m_keyHeap->push(RSNo, nextRecord);

	return m_template;
}


/**
 * ��ȡָ����Ŀ��RunSet���浽m_runSets�����ʣ���RunSet��������ֻ����ʣ���RunSet
 *
 * @param runSetNum
 * @return ��ȡ��RunSet��Ŀ
 * @throw �ļ�IO�쳣��
 */
uint Sorter::getProperRunSets(u32 runSetNum) throw(NtseException) {
	uint realRSNum = 0;
	char *rspath;
	rsItor itor;

	m_keyHeap->reset();
	// ��ʼ��ÿһ��RunSet����ȡÿ��RS�ĵ�һ����¼������ʼ��С��
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
 * ����m_keyHeap˳��ˢ����ǰm_memory�е�����������ֵ
 *
 * @param runSet	Ҫ������RunSet
 * @throw	�ļ������쳣
 */
void Sorter::flushAllMem(RunSet *runSet) throw(NtseException) {
	uint recordNo;
	SubRecord curKey;
	memcpy(&curKey, m_template, sizeof(SubRecord));

	while ((recordNo = m_dataHeap->pop()) != (uint)-1) {	// ���δӶ��ж�ȡ��������д���ļ�
		readKeyFromMem(&curKey, m_memory + m_recordMaxLen * recordNo);
		runSet->writeKey(&curKey);
	}
}



/**
 * ��ָ����SubRecord��������ʽ����ָ���ڴ棬ֻ�洢�����ֽڵ�size�Լ�����ʾ���ȵ�data������
 *
 * @param freeStart	������ڴ���ʼ��ַ
 * @param record	Ҫ����ļ�¼
 */
void Sorter::saveKeyToMem(byte *freeStart, SubRecord *record) {
	u16 size = (u16)record->m_size;
	memcpy(freeStart, (byte*)&size, sizeof(u16));
	memcpy(freeStart + sizeof(u16), record->m_data, size);
	RID_WRITE(record->m_rowId, freeStart + sizeof(u16) + size);
	*(freeStart + sizeof(u16) + size + RID_BYTES) = '\0';	// ��ʶ���ݵĽ���
}


/**
 * ��ָ���ڴ��ַ��ȡ��ֵ��size��data��rowId��Ϣ
 *
 * @param key		������ֵ
 * @param memory	ָ�����ڴ�
 */
void Sorter::readKeyFromMem(SubRecord *key, byte *memory) {
	key->m_size = *(u16*)memory;
	key->m_data = memory + sizeof(u16);
	key->m_rowId = RID_READ(memory + sizeof(u16) + key->m_size);
}


/**
 * ����Heap��ȡ��SubRecord�����������ֵ�����ʽ���浽m_template
 *
 * @param tableDef	����
 * @param indexDef	��������
 * @param record	Heap��ȡ�ļ�¼
 * @param lobArray	���������������������
 * @param extractor	������ֵ������
 */
void Sorter::makeIndexKey(const TableDef *tableDef, const IndexDef *indexDef, SubRecord *record, Array<LobPair*> *lobArray, ExtractKey extractor) {
	Record tableRecord;	// ���ڹ��������extractorʹ��
	tableRecord.m_format = REC_REDUNDANT;

	tableRecord.m_rowId = record->m_rowId;
	tableRecord.m_size = record->m_size;
	tableRecord.m_data = record->m_data;

	m_template->m_size = m_recordMaxLen - sizeof(u16);	// Ϊ����extractor֪�����ó���
	m_template->m_rowId = tableRecord.m_rowId;
	extractor(tableDef, indexDef, &tableRecord, lobArray, m_template);
}


/**
 * �ر�ָ��������Sorter��ӵ�е�RunSet
 *
 * @param size	ָ��Ҫ�ر�RunSet������
 * @throw �ļ������쳣
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
			// ��map����ɾ��
			rsItor itor = m_rsMap.find(RSNo);
			assert(itor != m_rsMap.end());
			m_rsMap.erase(itor);
			m_cfile->unregisterTempFile(rspath);
			delete [] rspath;
		}
}

/**
 * ���н������RS�ļ�ȫ��ע����
 * ��ʹ�����ļ�����Ҳ�᳢�����ж�ע��һ��
 */
void Sorter::unregisterRSFiles() {
	if (!m_rsMap.empty()) {
		for (rsItor itor = m_rsMap.begin(); itor != m_rsMap.end(); itor++) {
			File *file = new File(itor->second);
			file->remove();	// ����û���������������һ��ҪӲ��ͷƤɾ����
			delete file;
			m_cfile->unregisterTempFile(itor->second);
			delete [] itor->second;
		}
	}
}


//////////////////////////////////////////////////////////////////////////////////////////////////////////////


/**
 * RunSet���캯��
 * @param path			ָ��ʹ�õ��ļ�·��
 * @param memory		ָ������ʹ�õ��ڴ�ռ�
 * @param size			�ڴ��С
 * @param indexKey		��������ʽģ��
 * @param maxKeySize	��������󳤶�
 * @param readOnly		�Ƿ��Ƕ���RunSet������Ϊд��
 */
RunSet::RunSet(const char *path, byte *memory, uint size, SubRecord *indexKey, uint maxKeySize, bool readOnly) {
	m_readOnly = readOnly;

	m_lastRecord = NULL;
	m_path = path;
	m_file = new File(path);
	if (readOnly) {	// ���ڶ���RunSet�ļ������Ѿ����ڣ�ֻ��Ҫ��
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
	} else {	// ����д���ļ���Ҫ���½���
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
	m_lastRecord->m_size = 0;	// ��һ�αȽ���Ҫ��ʶΪ�ռ�¼
}


/**
 * RunSet��������
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
 * ���øú�����֤RunSet��Ӧ���ļ����ر�
 * @throw �ļ������쳣
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

	if (m_readOnly) {	// �����д����״̬����Ҫɾ���ļ���������״̬����ɾ��
		errNo = m_file->remove();
		if (File::getNtseError(errNo) != File::E_NO_ERROR) {
			NTSE_THROW(errNo, "Cannot REMOVE temp file when doing merge sort. %s", File::explainErrno(errNo));
		}
	}

	m_fileOpen = false;
}



/**
 * ȡ�õ�ǰRunSet����һ����¼
 *
 * @pre readOnlyΪtrue
 * @return ��һ����¼
 * @throw	�ļ���ȡ�쳣
 */
SubRecord* RunSet::getNextRecord() throw(NtseException) {
	assert(m_readOnly);

	if (m_read >= m_memory + m_memSize || *m_read == 0xFF) {	// ��ǰҳ���꣬���Զ�ȡ��һ��ҳ��
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
 * ȡ�õ�ǰRunSet����һ����¼
 *
 * @pre readOnlyΪtrue
 * @return ��һ����¼
 */
SubRecord* RunSet::getLastRecord() {
	assert(m_readOnly);
	return m_lastRecord;
}



/**
 * ��ȡ�ļ��е���һ���������
 *
 * @pre readOnlyΪtrue
 * @return	�Ƿ��ȡ����һ�����ݿ�
 * @throw	�ļ���ȡ�쳣
 */
bool RunSet::readNextBlock() throw(NtseException) {
	assert(m_readOnly);

	if (m_curBlockNo * m_memSize >= m_fileSize) {	// �ļ�����ĩβ���ر��ļ�
		// �رղ��������ɾ�������ļ�
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
 * ��RunSet����д��һ����ֵ�����ش���ǰ׺ѹ��
 *
 * @pre readOnlyΪfalse
 * @param record	Ҫ����ļ�ֵ
 * @throw �ļ������쳣
 */
void RunSet::writeKey(SubRecord *record) throw(NtseException) {
	assert(!m_readOnly);

	u16 prefixLen = IndexKey::computePrefix(m_lastRecord, record);
	u16 needSpace = IndexKey::computeSpace(record, prefixLen, false);

	assert(m_read < m_memory + m_memSize);
	if (isCacheFull(needSpace)) {	// �ռ䲻��������ˢ��ǰ����
		flushCache();
		prefixLen = 0;
		memset(m_memory, 0xFF, m_memSize);
		m_read = m_memory;
	}

	// ��ǰλ������д��ǰ׺����׺�����Լ���׺���ݺ�rowId��Ϣ
	IndexKey *indexKey = (IndexKey*)m_read;
	needSpace = (u16)(indexKey->compressAndSaveKey(record, prefixLen, false) - m_read);
	m_read += needSpace;

	// ά��RunSet�������ֵ
	IndexKey::copyKey(m_lastRecord, record, false);
}


/**
 * �жϵ�ǰ�����Ƿ�ռ䲻��д����һ����¼
 *
 * @param	needSpace	��һ������¼��Ҫ�Ŀռ�
 * @return	true��ʾ������������д�룬false�෴
 */
bool RunSet::isCacheFull(u16 needSpace) {
	return m_memory + m_memSize <= m_read + needSpace;
}



/**
 * ����ǰҳ��ˢ��������
 *
 * @pre readOnlyΪfalse���ļ���ǰ��С������������չ�µ�ҳ��
 * @throw �ļ���д�쳣
 */
void RunSet::flushCache() throw(NtseException) {
	assert(!m_readOnly);

	if (m_read == m_memory)	// ���������д�����ݣ��������ǿյ�ҳ
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
 * ��ù����ļ��ĵ�ǰ��С
 *
 * @pre readOnlyΪfalse
 * @return �����ļ��Ĵ�С
 */
u64 RunSet::getFileSize() const {
	return m_curBlockNo * m_memSize;
}

/**
 * ����runset���ļ���
 */
const char* RunSet::getFileName() const {
	return m_path;
}

}

