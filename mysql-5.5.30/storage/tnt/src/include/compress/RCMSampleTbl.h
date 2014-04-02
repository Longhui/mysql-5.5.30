/**
 * 压缩表采样
 *
 * @author 李伟钊(liweizhao@corp.netease.com, liweizhao@163.org)
 */
#ifndef _NTSE_RCM_SAMPLE_TBL_H_
#define _NTSE_RCM_SAMPLE_TBL_H_

#include <list>
#include "misc/Global.h"
#include "misc/TableDef.h"
#include "misc/Session.h"
#include "misc/Record.h"
#include "util/DList.h"
#include "api/Database.h"
#include "rec/Records.h"
#include "compress/SmplTrie.h"
#include "misc/RecordHelper.h"

using namespace std;

namespace ntse {
	/** 取模运算 */
	#define MOD(a, b) ((a) & ((b) - 1))
	//#define MOD(a, b) ((a) % (b))

	/** 
	 * 启用表采样模式 
	 */
	enum SampleTblMode {
		AUTO_SMPL = 0,  /** 自动启用表采样 */
		MANUAL_SMPL,    /** 手动启用表采样 */
	};

	/**
	 * 表记录采样策略 
	 */
	enum SmplTblStrategy {
		SEQUENCE_GET_ROWS = 0,/** 从表的第一条记录开始顺序读取记录 */
		PARTED_GET_ROWS,      /** 采用30-70策略，从表的前70%记录中采样30%， 从剩下的30%记录中采样70% */
		DISCRETE_GET_ROWS,   /** 离散采样，越往后采样越密集 */
		VALID_SAMPLE_STRATEGY_NUM,
	};

	/**
	* 表记录采样策略名称 
	*/
	const static char* SMPL_STRATEGY_NAME[] = {
		"SEQUENCE",
		"PARTED",
		"DISCRETE",
	};

	/**
	 * 表采样错误码
	 */
	enum SmpTblErrCode {
		SMP_NO_ERR = 0, //采样成功
		SMP_NO_ENOUGH_ROWS,   //表中没有足够的采样记录
		SMP_SCAN_ROWS_FAILD,  //扫描纪录出错
		SMP_NO_ENOUGH_MEM,    //内存不足
		SMP_UNKNOW_ERR,       //未知错误
	};

	/**
	 * 表采样错误信息
	 */
	const static char* SmplTblErrMsg[] = {
		"no error occured",
		"there is not enough rows in the table",
		"error occured when scanning rows",
		"There is not enough memory left to sample table and create dictionary"
		"unknow error"
	};

	/**
	 * 查找缓冲区数据单元 
	 */
	typedef struct _RCMSearchBufUnit {
		byte m_data;            /** 字节数据 */
		bool m_colGrpBoundary;  /** 是否是记录边界 */
		bool m_neekSeek;        /** 是否需要从该位置开始查找 */
	} RCMSearchBufUnit;

	class RCMSlidingWindow;
	class RCMSearchBuffer;
	class RCMSearchResultBuf;

	/**
	 * 滑动窗口哈希表 
	 */
	typedef DLink<uint> HashNode; 
	class SlidingWinHashTbl {
		const static u16 MTX_PAGE_SIZE = 1024;
		const static u8  DFL_RESERVE_PAGES = 4;
	public:
		SlidingWinHashTbl(u8 memLevel, uint winSize) throw(NtseException);
		~SlidingWinHashTbl();
		/**
		 * 向哈希表中添加一个元素
		 * @param hashCode 要添加的元素的哈希码
		 * @param offset   添加到哈希表中的元素
		 */
		inline void put(const uint& hashCode, const uint& offset) {
			uint index = hashCode & (m_hashSize - 1);
			if (NULL == m_hashTblEntry[index]) {
				m_hashTblEntry[index] = new (m_mtx->alloc(sizeof(DList<uint>)))DList<uint>();
			}
			m_hashTblEntry[index]->addLast(new (m_hashNodePool->getFree())HashNode(offset));
		}
		/**
		 * 移除指定哈希值对应的入口项的第一个元素
		 * @param hashCode 指定的哈希值
		 * @param offset   入口项的第一个元素，删除之后验证与此相等
		 */
		inline void removeSlotHead(const uint& hashCode, const uint& offset) {
			uint index = hashCode & (m_hashSize - 1);
			assert(m_hashTblEntry[index] != NULL);
			DLink<uint>* revNode = m_hashTblEntry[index]->removeFirst();
			assert(revNode && revNode->get() == offset);
			revNode->~DLink();
			m_hashNodePool->markFree(revNode);
		}
		/**
		 * 获得指定哈希值对应的入口项
		 * @param hashCode 指定的哈希值
		 */
		inline DList<uint>* getSlot(const uint& hashCode) {
			assert(m_hashTblEntry != NULL);
			return m_hashTblEntry[hashCode & (m_hashSize - 1)];
		}
		//获得哈希表入口项数目
		inline uint hashSize() const {
			return m_hashSize;
		}
	private:
		MemoryContext     *m_mtx;               /** 内存分配上下文 */
		u8                m_memLevel;           /** 哈希表内存使用水平 */
		uint              m_hashSize;           /** 哈希表入口项数目 */
		DList<uint>       **m_hashTblEntry;     /** 哈希表入口 */
		ObjMemoryPool<HashNode> *m_hashNodePool;/** 哈希表链表节点内存池 */
	};//class SlidingWinHashTbl

	/** 滑动窗口类 */
	class RCMSlidingWindow {
	public:
		RCMSlidingWindow(RCMSearchBuffer *searchBuffer, u8 winDetectTimes);
		~RCMSlidingWindow();
		void init(u8 memLevel, const uint& winSize, const uint& begin, const uint& end) throw (NtseException);
		void scrollWin(const uint& size);
		void searchInWindow(const uint& seekStart, RCMSearchResultBuf* result);
		uint hashCode(const RCMSearchBufUnit* data, const uint& offset, const uint& len);
	private:
		uint              m_beginPos;         /** 滑动窗口起始位置 */
		uint              m_endPos;           /** 滑动窗口结束位置 */
		uint              m_winSize;          /** 滑动窗口大小 */
		RCMSearchBuffer   *m_searchBuffer;    /** 滑动窗口所在的查找缓冲区 */
		SlidingWinHashTbl *m_hashTbl;         /** 滑动窗口哈希表 */
		u8                m_winDetectTimes;   /** 滑动窗口内匹配项探测次数 */
		uint              m_headHashCode;     /** 滑动窗口第一个字节为首的N个字节的哈希值, N为匹配项的最小长度 */
	};//class RCMSlidingWindow

	/** 
	 * 查找缓冲区
	 * 查找缓冲区使用循环队列实现，长度大于两倍滑动窗口大小
	 */
	class RCMSearchBuffer {
	public:
		RCMSearchBuffer(uint size, u16 minMatchLen, u16 maxMatchLen);
		~RCMSearchBuffer();
		void init(const RCMSearchBufUnit *buf, const uint& size);
		void createWindow(uint winSize, u8 winDetectTimes, u8 memLevel) throw (NtseException);
		/**
		 * 是否是之前查找跳过的位置
		 * @param offset 相对于查找缓冲区头的偏移量
		 */
		inline bool needSeek(const uint& offset) {
			assert(m_searchData != NULL);
			size_t pos = MOD((m_head + offset), m_size);
			return m_searchData[pos].m_neekSeek;
		}
		/**
		 * 标记查找跳过的位置
		 * @param offset 相对于查找缓冲区头的偏移量 
		 */
		inline void setSeekPos(const uint& offset) {
			assert(m_searchData != NULL);
			size_t pos = MOD((m_head + offset), m_size);
			m_searchData[pos].m_neekSeek = true;
		}
		/**
		 * 获得指定的查找缓冲区单元
		 * @param offset 相对于查找缓冲区头的偏移量
		 * @return 对应的查找缓冲区单元
		 */
		inline const RCMSearchBufUnit& getUnit(const uint& offset) const {
			assert(m_searchData != NULL);
			uint pos = MOD((m_head + offset), m_size);
			return m_searchData[pos];
		}
		/**
		 * 比较两个缓冲区单元字节数据是否相等
		 * @param first 参与比较者1的偏移量
		 * @param second 参与比较者2的偏移量
		 */
		inline bool isEqual(const uint& first, const uint& second) const {
			assert(m_searchData != NULL);
			assert(first < m_size && second < m_size);
			return m_searchData[first].m_data == m_searchData[second].m_data;
		}
		
		inline bool isRecordEnd(const uint& position) const {
			assert(m_searchData != NULL);
			assert(position < m_size);
			return m_searchData[position].m_colGrpBoundary;
		}
		/**
		 * 移动滑动窗口
		 * @param size 移动距离
		 */
		inline void scrollWin(const uint& size) {
			assert(m_win != NULL);
			m_win->scrollWin(size);
		}
		inline u16 minMatchLen() const {
			return m_minMatchLen;
		}
		inline u16 maxMatchLen() const {
			return m_maxMatchLen;
		}
		inline uint size() const {
			return m_size;
		}
		inline RCMSearchBufUnit* getSearchData() const {
			return m_searchData;
		}
		/** 
		 * 在查找缓冲区中查找匹配项输出结果
		 * @param result OUT 输出参数，结果缓存
		 */
		inline void runSearch(RCMSearchResultBuf *result) {
			m_win->searchInWindow(m_head, result);
		}
		/**
		 * 移动查找缓冲区并且补充一个新的单元
		 * @param data             单元数据
		 * @param isColGrpBoundary 单元是否为记录边界
		 * @param needSeek         是否从此单元开始查找
		 */
		inline void moveOnce(const byte data, bool isColGrpBoundary, bool needSeek) {
			assert(m_searchData != NULL);
			m_head = MOD((m_head + 1), m_size);
			m_tail = MOD((m_tail + 1), m_size);
			m_searchData[m_tail].m_data = data;
			m_searchData[m_tail].m_colGrpBoundary = isColGrpBoundary;
			m_searchData[m_tail].m_neekSeek = needSeek;
		}
	private:
		RCMSearchBufUnit *m_searchData; /** 查找缓冲区 */
		uint              m_head;       /** 查找缓冲区起始位置 */
		uint              m_tail;       /** 查找缓冲区结束位置 */
		uint              m_size;       /** 查找缓冲区大小(须为2的整数次方) */
		RCMSlidingWindow  *m_win;       /** 滑动窗口 */
		u16               m_minMatchLen;/** 最短匹配长度限制 */
		u16               m_maxMatchLen;/** 最长匹配长度限制 */
	};//class RCMSearchBuffer

	/** 
	 * 查找结果缓存
	 * 查找结果缓存用于避免在滑动窗口查找匹配项返回结果时频繁创建销毁对象，
	 * 重置之后就可以用于下一次返回结果
	 */
	class RCMSearchResultBuf {
	public:
		const static uint MAX_SKIP_LEN_CAPACITY = 256;

	public:
		/**
		 * 构造查找结果缓存
		 * @param mtx              内存分配上下文
		 * @param matchBufCapacity 匹配字节缓存容量
		 * @param skipLenCapacity  跳过长度缓存容量
		 */
		RCMSearchResultBuf(u16 matchBufCapacity, uint skipLenCapacity = MAX_SKIP_LEN_CAPACITY):
		  m_matchBufCapacity(matchBufCapacity), m_matchByteSize(0), m_skipLenBufCapacity(skipLenCapacity), m_skipLenBufSize(0) {
			  m_matchByteBuf = new byte[matchBufCapacity];
			  m_skipLenBuf = new uint[skipLenCapacity];
		  }
		  ~RCMSearchResultBuf() {
			  delete []m_matchByteBuf;
			  m_matchByteBuf = NULL;
			  delete []m_skipLenBuf;
			  m_skipLenBuf = NULL;
		  }
		  /**
		   * 重置查找结果缓冲区
		   */
		  inline void reset() {
			  m_matchByteSize = 0;
			  m_skipLenBufSize = 0;
		  }
		  /**
		   * 查找结果是否为空
		   */
		  inline bool empty() {
			  return m_matchByteSize == 0;
		  }
		  /**
		   * 获得匹配字节缓存起始地址
		   */
		  inline const byte* getMatchResult() {
			  return m_matchByteBuf;
		  }
		  /**
		   * 获得匹配字节缓存实际大小
		   */
		  inline u16 getMatchResultSize() const {
			  return m_matchByteSize;
		  }
		  /**
		  * 获得当前跳过长度缓存实际大小
		  */
		  inline uint getSkipLenBufSize() const {
			  return m_skipLenBufSize;
		  }
		  /**
		  * 获得指定的跳过长度
		  * @param i 跳过长度下标
		  * @return
		  */
		  inline uint getSkipLen(uint i) const {
			  assert(i < m_skipLenBufSize);
			  return m_skipLenBuf[i];
		  }
		  /**
		  * 在查找结果缓冲区添加一个匹配项
		  * @param buf    匹配项地址
		  * @param offset 匹配项起始偏移量
		  * @param len    匹配项长度
		  */
		  void addMatch(RCMSearchBuffer *buf, const uint& offset, const u16& len) {
			  assert(buf != NULL && len > 0);
			  assert(m_matchByteBuf != NULL);

			  if (len > m_matchByteSize) {
				  m_skipLenBuf[m_skipLenBufSize++] = len;
				  for (uint i = m_matchByteSize; i < len; i++) {
					  m_matchByteBuf[i] = buf->getUnit(offset + i).m_data;
				  }
				  m_matchByteSize = len;//实际大小现在为len
			  }
		  }
	private:
		byte       *m_matchByteBuf;           /** 匹配字节缓存 */
		u16        m_matchBufCapacity;        /** 匹配字节缓存容量 */
		u16        m_matchByteSize;           /** 匹配字节实际大小 */
		uint       *m_skipLenBuf;             /** 跳过长度缓存 */
		uint       m_skipLenBufCapacity;      /** 跳过长度缓存容量 */
		uint       m_skipLenBufSize;          /** 跳过长度缓存实际大小 */
		friend class RCMSlidingWindow;
	};//class RCMSearchResultBuf

	/** 
	 * 采样记录扫描
	 */
	class SmplRowScan {
		friend class RCMSampleTbl;

	public:
		SmplRowScan(Session *session, Database *db, const TableDef *tableDef, 
			Records *records, SmplTblStrategy strategy, RecordConvert *converter = NULL);
		static SmplRowScan* createRowScanInst(Session *session, Database *db, const TableDef *tableDef, 
			Records *records, RecordConvert *converter = NULL);
		virtual ~SmplRowScan();
		void beginScan();
		bool getNextRow();
		void endScan();
		u64 getCurScanPagesCount() const {
			assert(m_scan);
			return m_scan->getCurScanPagesCount();
		}

	protected:
		virtual bool doGetNextRow();
		void convRedSubRcdToCompressOrder(const SubRecord *redRow);
		static SmplTblStrategy getSampleStrategyType(const char *strategyStr);

		/**
		* 在扫描到下条记录之后进行的操作
		* @param redRow 扫描到的冗余格式子记录
		*/
		inline void afterGetRow(SubRecord *redRow) {
			convRedSubRcdToCompressOrder(redRow);
			assert(m_redRcdCache.m_size > 0);
			m_session->unlockRow(&m_rowLockHnd);//解除对记录的共享锁
			m_hasScanedSize++;
		}

		/**
		* 增加当前采样大小
		*/
		inline void incrSampleSize() {
			m_hasSmplSize++;
		}

	protected:
		Session           *m_session;               /** 会话 */
		Database          *m_db;                    /** 采样的表所属数据库 */
		const TableDef    *m_tableDef;              /** 采样的表的定义 */
		const TableDef    *m_newTableDef;           /** 新表定义 */
		Records		      *m_records;               /** 采样表的记录管理器 */
		RecordConvert     *m_converter;             /** 记录转化器 */
		Records::Scan     *m_scan;                  /** 堆扫描 */
		SmplTblStrategy   m_strategy;               /** 表采样策略 */
		byte              *m_mysqlRcdCacheData;     /** REC_MYSQL格式记录缓存数据 */
		Record            m_redRcdCache;            /** 冗余格式记录缓存 */
		CompressOrderRecord m_cmprsOrderRcdCache;   /** 压缩排序格式记录缓存 */
		uint              m_cmprsOrderRcdCacheLeft; /** 压缩排序格式记录缓存剩余未被读取长度(位图除外) */
		size_t            *m_segBoundary;           /** 压缩排序格式记录各个段的边界 */
		u8                m_curSeg;                 /** 当前缓存所处的待压缩段 */
		RowLockHandle     *m_rowLockHnd;            /** 当前行锁句柄 */
		HeapStatusEx      m_heapStatusEx;           /** 堆扩展统计信息 */               
		u64               m_totalSmplRows;          /** 总共需要采样的记录数 */
		u64               m_hasScanedSize;          /** 已经扫描的数据大小 */
		u64               m_hasSmplSize;            /** 已经采样的数据大小 */
	};//class SmplRowScan

	/** 
	 * 分区采样记录扫描
	 */
	class PartedRowScan : public SmplRowScan {
	public:
		static const double FIRST_PART_PCNT;   /** 采样表记录第一部分占总的比重 */
		static const double SECOND_PART_PCNT;  /** 采样表记录第二部分占总的比重 */

	public:
		PartedRowScan(Session *session, Database *db, const TableDef *tableDef, 
			Records *records, RecordConvert *converter = NULL);
	protected:
		virtual bool doGetNextRow();

	protected:
		u64               m_firstPartSize;          /** 如果采用30-70策略，前70%记录的数据大小 */
		u64               m_firstPartSampleSize;    /** 如果采用30-70策略, 前70%记录需要采样的数据大小 */
	};

	/**
	 * 离散采样记录扫描
	 */
	class DiscreteRowScan : public SmplRowScan {
	public:
		DiscreteRowScan(Session *session, Database *db, const TableDef *tableDef, 
			Records *records, RecordConvert *converter = NULL);
	protected:
		virtual bool doGetNextRow();

	protected:
		u64 m_samplePartitionNum;  /** 采样分区数 */
		u64 m_sampleSizePart;      /** 每个分区需要采样的记录数 */
		u64 m_needSkipSize;        /** 当前分区需要跳过的记录数 */
		u64 m_curSampleSize;	   /** 当前分区已采样记录数 */
		u64 m_curSkipSize;         /** 当前分区已跳过的记录数 */
	};

	/** 顺序采样记录扫描 */
	typedef SmplRowScan SequenceRowScan;

	/**
	 * 记录压缩表采样
	 */
	class RCMSampleTbl {
	public:
		static const double MIN_SMPL_PCT_THRESHOLD;  /** 达到采样大小的此比例可认为采样成功 */

	public:
		RCMSampleTbl(Session *session, Database *db, const TableDef *tableDef, 
			Records *records, RecordConvert *converter = NULL) throw (NtseException);
		~RCMSampleTbl();
		SmpTblErrCode beginSampleTbl();
		void endSampleTbl();
		RCDictionary* createDictionary(Session *session) throw(NtseException);

	private:
		SmpTblErrCode init();
		bool refreshSearchBuf(const uint& size);
		bool isSampleDataEnough();

	private:
		Session         *m_session;         /** 会话 */
		Database        *m_db;              /** 采样的表所属数据库 */
		const TableDef  *m_tableDef;        /** 采样的表的定义 */
		Records		    *m_records;         /** 采样表的记录管理器 */
		RecordConvert   *m_converter;       /** 记录格式转换器，如果为NULL则表示对采用的记录不进行转换 */
		RCMSearchBuffer *m_searchBuf;       /** 查找缓冲区 */
		SmplTrie        *m_sampleTrie;      /** 采样用Trie树 */
		RowCompressCfg  *m_rowCpsCfg;       /** 记录压缩配置 */
		SmplRowScan     *m_rowScan;         /** 采样记录扫描对象 */
	};//class RCMSampleTbl
}
#endif 