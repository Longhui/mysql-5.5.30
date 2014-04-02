/**
*	ʵ�ֶԽ�������ֵ�ⲿ����
*
*	author: naturally (naturally@163.org)
*/

#ifndef _NTSE_OUTER_SORTER_H_
#define _NTSE_OUTER_SORTER_H_

#include "misc/Global.h"
#include "misc/Record.h"
#include <map>

using namespace std;

namespace ntse {

class File;
class Sorter;
class RunSet;
class MinHeap;
class IndexDef;
class Session;
class DrsHeap;
class ControlFile;

typedef void (*ExtractKey)(const TableDef *tableDef, const IndexDef *indexDef, const Record *record, Array<LobPair*> *lobArray, SubRecord *key);			/** ��ѹ��ֵ����ָ�� */
typedef int (*CompareKey)(const TableDef *tableDef, const SubRecord *key1, const SubRecord *key2, const IndexDef *indexDef);	/** �Ƚϼ�ֵ����ָ�� */


/**
 * ��ʾ�鲢���������ÿ���鲢���У���������һ������д��Ͷ�ȡ������
 * һ���鲢���ж�Ӧһ�������ļ���һ���ڴ滺���
 * Ϊ�˼��ٴ���I/O�������������ֵ�������ԣ��鲢���в���ǰ׺ѹ����ʽ�洢
 */
class RunSet {
public:
	RunSet(const char *path, byte *memory, uint size, SubRecord *indexKey, uint maxKeySize, bool readOnly);
	~RunSet();

	SubRecord* getNextRecord() throw(NtseException);
	SubRecord* getLastRecord();

	void writeKey(SubRecord *curKey) throw(NtseException);
	void flushCache() throw(NtseException);
	bool isCacheFull(u16 needSpace);
	void close() throw(NtseException);
	u64 getFileSize() const;
	const char* getFileName() const;

private:
	bool readNextBlock() throw(NtseException) ;

private:
	const char *m_path;			/** �����ļ�·�� */
	File *m_file;				/** �����ļ���� */
	u64 m_fileSize;				/** �ļ���С */
	byte *m_memory;				/** ����������ڴ桪�������ռ䣬ʹ���߷���! */
	byte *m_read;				/** ��ǰ��ȡ�������λ�� */
	uint m_memSize;				/** ��ʹ���ڴ��С */
	uint m_curBlockNo;			/** ��ǰ��ȡ/д����ļ����� */
	SubRecord *m_lastRecord;	/** ��һ�ζ�ȡ�ļ�¼���� */
	bool m_readOnly;			/** ��RunSet����������д */
	bool m_fileOpen;			/** ��־�ļ��Ƿ�� */
};



/**
 * ������������
 * ��Heap���е�������һ�����������ֵ������������������ֵ���д�С���������
 * ������Ҫ�������׶Σ�	1.��ʼ�׶θ����������ڴ��С���ɸ���RunSet�鲢����
 *						2.���й鲢���򣬽�RunSet�������ٵ�������С
 *						3.���һ�˹鲢����һ���ظ���������ֵ
 */
typedef pair <uint, char*> rsParis;
typedef pair <uint, RunSet*> inuseRSParis;
typedef map <uint, char*>::iterator rsItor;

class Sorter {
public:
	Sorter(ControlFile *cfile, LobStorage *lobStorage, const IndexDef *indexDef, const TableDef *tableDef);
	~Sorter();

	void sort(Session *session, DrsHeap *heap) throw(NtseException);
	SubRecord* getSortedNextKey() throw(NtseException);

private:
	void createRunSets(Session *session, DrsHeap *heap) throw(NtseException);
	void mergeSort() throw(NtseException);
	void prepareLast() throw(NtseException);

	uint getProperRunSets(u32 runSetNum) throw(NtseException);
	void saveKeyToMem(byte *freeStart, SubRecord *record);
	void flushAllMem(RunSet *runSet) throw(NtseException);
	uint finishRunSetCreation(RunSet *runSet, uint RSNo) throw(NtseException);

	void makeIndexKey(const TableDef *tableDef, const IndexDef *indexDef, SubRecord *record, Array<LobPair *> *lobArray, ExtractKey extractor);
	void dealWithNewKey(byte **start, uint keyNo, u16 offset);
	uint flushMinKey(byte **start, RunSet *runSet, SubRecord *minKey);

	void closeRunSets(uint size) throw (NtseException);
	void unregisterRSFiles();

public:
	static void readKeyFromMem(SubRecord *key, byte *memory);

public:
#ifdef NTSE_UNIT_TEST
	static const uint MERGE_PAGE_SIZE = 4096;	/** ���ڹ鲢����ҳ���С */
	static const uint MERGE_PAGE_NUM = 8;		/** ���ڹ鲢����ҳ������ */
#else
	static const uint MERGE_PAGE_SIZE = 65536;	/** ���ڹ鲢����ҳ���С */
	static const uint MERGE_PAGE_NUM = 16;		/** ���ڹ鲢����ҳ������ */
#endif

private:
	static const uint INDEX_KEY_SIZE = sizeof(SubRecord);	/** ���峣����ʾSubRecord�ṹ��С */

	ControlFile *m_cfile;					/** ���ݿ�����ļ��������ڷ��������ʱ�ļ� */
	const TableDef *m_tableDef;				/** �������ݶ�Ӧ���� */
	const IndexDef *m_indexDef;				/** Ҫ���������Ķ��� */
	LobStorage	   *m_lobStorage;			/** ����������Ҫ�õ��Ĵ����洢 */
	map<uint, char*> m_rsMap;				/** ����ά������runset�ı�ź����ֶԵ����� */
	inuseRSParis m_runSets[MERGE_PAGE_NUM];	/** ��������/�����RunSet */
	byte *m_memory;							/** ��������Ҫ�����ڴ� */
	MinHeap *m_runSetHeap;					/** ����ά����Сrunset����С�� */
	MinHeap *m_dataHeap;					/** ����ά����С���¼���ݵ���С�� */
	MinHeap *m_keyHeap;						/** ����ά����С������ֵ����С�� */
	SubRecord *m_template;					/** ģ���Ӽ�¼ */
	u16 m_recordMaxLen;						/** ������ֵSubRecord������󳤶� */
	bool m_keyNeedCompress;					/** �Ƚ�ʱ�������Ƿ���Ҫѹ�� */
	uint m_RSNo;							/** ÿ��RunSet��ӦΨһһ����ţ��ñ�ż�1���� */
};



/**
 * ʵ��һ��������С�ѣ��������������й鲢�ȳ���ʹ��
 * ���ѿռ䲻����ʱ����Զ���չ��ԭ�пռ��һ��
 * ���ѿռ���̳���������3/4��ʱ�򣬶ѿռ���Զ�����һ��
 */
class MinHeap {
public:
	virtual ~MinHeap();

	void push(uint RSNo, void *content);
	uint pop();
	void reset();
	uint getValidSize();

protected:
	virtual s32 compare(void *content1, void *content2) = 0;
	void extendHeap();
	void shrinkHeap();

	void _init(uint size);

private:
	uint m_size;		/** ���п�ʹ�ö������ֵ */
	uint m_cur;			/** ��ǰװ�صĶ������ */
	uint *m_RSNos;		/** ���е�RunSet���� */
	void **m_content;	/** ����RunSet����Ƚϵ���Ŀ */

protected:
	static const u16 DEFAULT_HEAP_SIZE = 20;		/** �ѵ�Ĭ�ϴ�С */
};


/**
 * ���ڴ洢�����Ǽ�¼���ݳ��Ⱥ����ݵ���С��
 * �ڹ鲢�����ȡ�����������ʱ��ʹ��
 */
class MinHeapForData : public MinHeap {
public:
	MinHeapForData(const TableDef *tableDef, const IndexDef *indexDef, bool keyNeedCompress, SubRecord *templet, uint size = DEFAULT_HEAP_SIZE) {
		_init(size);
		_initSelf(tableDef, indexDef, keyNeedCompress, templet);
	}

	/**
	 * ��ʼ������
	 * @param tableDef			����
	 * @param indexDef			��������
	 * @param keyNeedCompress	������ֵ�費��Ҫѹ��
	 * @param template			һ����ֵģ�壬���ڳ�ʼ����key2
	 */
	void _initSelf(const TableDef *tableDef, const IndexDef *indexDef, bool keyNeedCompress, SubRecord *templet) {
		m_comparator = keyNeedCompress ? RecordOper::compareKeyCC : RecordOper::compareKeyNN;
		m_tableDef = tableDef;
		key1.m_columns = key2.m_columns = templet->m_columns;
		key1.m_format = key2.m_format = templet->m_format;
		key1.m_numCols = key2.m_numCols = templet->m_numCols;
		key1.m_size = key2.m_size = 0;
		key1.m_data = key2.m_data = NULL;
		m_indexDef = indexDef;
	}

private:
	virtual s32 compare(void *content1, void *content2);

private:
	SubRecord key1, key2;			/** �Ƚϼ�ֵ */
	CompareKey m_comparator;		/** �ȽϺ��� */
	const TableDef *m_tableDef;		/** �������ڱ��� */
	const IndexDef *m_indexDef;		/** �������� */
};


/**
 * ���ڴ洢������SubRecord����С��
 */
class MinHeapForKey : public MinHeap {
public:
	MinHeapForKey(const TableDef *tableDef, const IndexDef *indexDef, bool keyNeedCompress, uint size = DEFAULT_HEAP_SIZE) {
		_init(size);
		m_comparator = keyNeedCompress ? RecordOper::compareKeyCC : RecordOper::compareKeyNN;
		m_tableDef = tableDef;
		m_indexDef = indexDef;
	}

private:
	virtual s32 compare(void *content1, void *content2);

private:
	CompareKey m_comparator;		/** �ȽϺ���ָ�� */
	const TableDef *m_tableDef;		/** ���� */
	const IndexDef *m_indexDef;		/** �������� */
};



/**
 * ���ڴ洢������RunSet��С����С��
 */
class MinHeapForRunSet : public MinHeap {
public:
	MinHeapForRunSet(uint size = DEFAULT_HEAP_SIZE) {
		_init(size);
	}

private:
	virtual s32 compare(void *content1, void *content2);
};

}

#endif


