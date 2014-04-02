/**
*	实现对建索引键值外部排序
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

typedef void (*ExtractKey)(const TableDef *tableDef, const IndexDef *indexDef, const Record *record, Array<LobPair*> *lobArray, SubRecord *key);			/** 解压键值函数指针 */
typedef int (*CompareKey)(const TableDef *tableDef, const SubRecord *key1, const SubRecord *key2, const IndexDef *indexDef);	/** 比较键值函数指针 */


/**
 * 表示归并排序过程中每个归并队列，负责将这样一个队列写入和读取出磁盘
 * 一个归并队列对应一个单独文件，一个内存缓存块
 * 为了减少磁盘I/O量，结合索引键值的有序性，归并队列采用前缀压缩形式存储
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
	const char *m_path;			/** 所属文件路径 */
	File *m_file;				/** 所属文件句柄 */
	u64 m_fileSize;				/** 文件大小 */
	byte *m_memory;				/** 用来缓存的内存——连续空间，使用者分配! */
	byte *m_read;				/** 当前读取到缓存的位置 */
	uint m_memSize;				/** 可使用内存大小 */
	uint m_curBlockNo;			/** 当前读取/写入的文件块数 */
	SubRecord *m_lastRecord;	/** 上一次读取的记录内容 */
	bool m_readOnly;			/** 该RunSet用来读还是写 */
	bool m_fileOpen;			/** 标志文件是否打开 */
};



/**
 * 外排序排序器
 * 将Heap当中的数据逐一构造出索引键值，利用外排序将索引键值进行从小到大的排序
 * 排序主要分三个阶段：	1.初始阶段根据排序总内存大小生成各个RunSet归并队列
 *						2.进行归并排序，将RunSet数量减少到缓存块大小
 *						3.最后一趟归并，逐一返回各个索引键值
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
	static const uint MERGE_PAGE_SIZE = 4096;	/** 用于归并缓存页面大小 */
	static const uint MERGE_PAGE_NUM = 8;		/** 用于归并缓存页面数量 */
#else
	static const uint MERGE_PAGE_SIZE = 65536;	/** 用于归并缓存页面大小 */
	static const uint MERGE_PAGE_NUM = 16;		/** 用于归并缓存页面数量 */
#endif

private:
	static const uint INDEX_KEY_SIZE = sizeof(SubRecord);	/** 定义常量表示SubRecord结构大小 */

	ControlFile *m_cfile;					/** 数据库控制文件对象，用于分配回收临时文件 */
	const TableDef *m_tableDef;				/** 排序数据对应表定义 */
	const IndexDef *m_indexDef;				/** 要创建索引的定义 */
	LobStorage	   *m_lobStorage;			/** 创建索引需要用到的大对象存储 */
	map<uint, char*> m_rsMap;				/** 用于维护所有runset的编号和名字对的数组 */
	inuseRSParis m_runSets[MERGE_PAGE_NUM];	/** 用于排序/输出的RunSet */
	byte *m_memory;							/** 排序所需要的总内存 */
	MinHeap *m_runSetHeap;					/** 用于维护最小runset的最小堆 */
	MinHeap *m_dataHeap;					/** 用于维护最小表记录数据的最小堆 */
	MinHeap *m_keyHeap;						/** 用于维护最小索引键值的最小堆 */
	SubRecord *m_template;					/** 模板子记录 */
	u16 m_recordMaxLen;						/** 索引键值SubRecord所需最大长度 */
	bool m_keyNeedCompress;					/** 比较时索引键是否需要压缩 */
	uint m_RSNo;							/** 每个RunSet对应唯一一个编号，该编号加1递增 */
};



/**
 * 实现一个简易最小堆，用于在外排序中归并等场合使用
 * 当堆空间不够的时候会自动扩展成原有空间的一倍
 * 当堆空间过程超过堆总量3/4的时候，堆空间会自动缩减一般
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
	uint m_size;		/** 堆中可使用对象最大值 */
	uint m_cur;			/** 当前装载的对象个数 */
	uint *m_RSNos;		/** 堆中的RunSet数组 */
	void **m_content;	/** 各个RunSet参与比较的项目 */

protected:
	static const u16 DEFAULT_HEAP_SIZE = 20;		/** 堆的默认大小 */
};


/**
 * 用于存储内容是记录数据长度和内容的最小堆
 * 在归并排序读取堆数据排序的时候使用
 */
class MinHeapForData : public MinHeap {
public:
	MinHeapForData(const TableDef *tableDef, const IndexDef *indexDef, bool keyNeedCompress, SubRecord *templet, uint size = DEFAULT_HEAP_SIZE) {
		_init(size);
		_initSelf(tableDef, indexDef, keyNeedCompress, templet);
	}

	/**
	 * 初始化函数
	 * @param tableDef			表定义
	 * @param indexDef			索引定义
	 * @param keyNeedCompress	索引键值需不需要压缩
	 * @param template			一个键值模板，用于初始化和key2
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
	SubRecord key1, key2;			/** 比较键值 */
	CompareKey m_comparator;		/** 比较函数 */
	const TableDef *m_tableDef;		/** 索引所在表定义 */
	const IndexDef *m_indexDef;		/** 索引定义 */
};


/**
 * 用于存储内容是SubRecord的最小堆
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
	CompareKey m_comparator;		/** 比较函数指针 */
	const TableDef *m_tableDef;		/** 表定义 */
	const IndexDef *m_indexDef;		/** 索引定义 */
};



/**
 * 用于存储内容是RunSet大小的最小堆
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


