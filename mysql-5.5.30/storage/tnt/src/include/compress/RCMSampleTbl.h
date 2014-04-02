/**
 * ѹ�������
 *
 * @author ��ΰ��(liweizhao@corp.netease.com, liweizhao@163.org)
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
	/** ȡģ���� */
	#define MOD(a, b) ((a) & ((b) - 1))
	//#define MOD(a, b) ((a) % (b))

	/** 
	 * ���ñ����ģʽ 
	 */
	enum SampleTblMode {
		AUTO_SMPL = 0,  /** �Զ����ñ���� */
		MANUAL_SMPL,    /** �ֶ����ñ���� */
	};

	/**
	 * ���¼�������� 
	 */
	enum SmplTblStrategy {
		SEQUENCE_GET_ROWS = 0,/** �ӱ�ĵ�һ����¼��ʼ˳���ȡ��¼ */
		PARTED_GET_ROWS,      /** ����30-70���ԣ��ӱ��ǰ70%��¼�в���30%�� ��ʣ�µ�30%��¼�в���70% */
		DISCRETE_GET_ROWS,   /** ��ɢ������Խ�������Խ�ܼ� */
		VALID_SAMPLE_STRATEGY_NUM,
	};

	/**
	* ���¼������������ 
	*/
	const static char* SMPL_STRATEGY_NAME[] = {
		"SEQUENCE",
		"PARTED",
		"DISCRETE",
	};

	/**
	 * �����������
	 */
	enum SmpTblErrCode {
		SMP_NO_ERR = 0, //�����ɹ�
		SMP_NO_ENOUGH_ROWS,   //����û���㹻�Ĳ�����¼
		SMP_SCAN_ROWS_FAILD,  //ɨ���¼����
		SMP_NO_ENOUGH_MEM,    //�ڴ治��
		SMP_UNKNOW_ERR,       //δ֪����
	};

	/**
	 * �����������Ϣ
	 */
	const static char* SmplTblErrMsg[] = {
		"no error occured",
		"there is not enough rows in the table",
		"error occured when scanning rows",
		"There is not enough memory left to sample table and create dictionary"
		"unknow error"
	};

	/**
	 * ���һ��������ݵ�Ԫ 
	 */
	typedef struct _RCMSearchBufUnit {
		byte m_data;            /** �ֽ����� */
		bool m_colGrpBoundary;  /** �Ƿ��Ǽ�¼�߽� */
		bool m_neekSeek;        /** �Ƿ���Ҫ�Ӹ�λ�ÿ�ʼ���� */
	} RCMSearchBufUnit;

	class RCMSlidingWindow;
	class RCMSearchBuffer;
	class RCMSearchResultBuf;

	/**
	 * �������ڹ�ϣ�� 
	 */
	typedef DLink<uint> HashNode; 
	class SlidingWinHashTbl {
		const static u16 MTX_PAGE_SIZE = 1024;
		const static u8  DFL_RESERVE_PAGES = 4;
	public:
		SlidingWinHashTbl(u8 memLevel, uint winSize) throw(NtseException);
		~SlidingWinHashTbl();
		/**
		 * ���ϣ�������һ��Ԫ��
		 * @param hashCode Ҫ��ӵ�Ԫ�صĹ�ϣ��
		 * @param offset   ��ӵ���ϣ���е�Ԫ��
		 */
		inline void put(const uint& hashCode, const uint& offset) {
			uint index = hashCode & (m_hashSize - 1);
			if (NULL == m_hashTblEntry[index]) {
				m_hashTblEntry[index] = new (m_mtx->alloc(sizeof(DList<uint>)))DList<uint>();
			}
			m_hashTblEntry[index]->addLast(new (m_hashNodePool->getFree())HashNode(offset));
		}
		/**
		 * �Ƴ�ָ����ϣֵ��Ӧ�������ĵ�һ��Ԫ��
		 * @param hashCode ָ���Ĺ�ϣֵ
		 * @param offset   �����ĵ�һ��Ԫ�أ�ɾ��֮����֤������
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
		 * ���ָ����ϣֵ��Ӧ�������
		 * @param hashCode ָ���Ĺ�ϣֵ
		 */
		inline DList<uint>* getSlot(const uint& hashCode) {
			assert(m_hashTblEntry != NULL);
			return m_hashTblEntry[hashCode & (m_hashSize - 1)];
		}
		//��ù�ϣ���������Ŀ
		inline uint hashSize() const {
			return m_hashSize;
		}
	private:
		MemoryContext     *m_mtx;               /** �ڴ���������� */
		u8                m_memLevel;           /** ��ϣ���ڴ�ʹ��ˮƽ */
		uint              m_hashSize;           /** ��ϣ���������Ŀ */
		DList<uint>       **m_hashTblEntry;     /** ��ϣ����� */
		ObjMemoryPool<HashNode> *m_hashNodePool;/** ��ϣ������ڵ��ڴ�� */
	};//class SlidingWinHashTbl

	/** ���������� */
	class RCMSlidingWindow {
	public:
		RCMSlidingWindow(RCMSearchBuffer *searchBuffer, u8 winDetectTimes);
		~RCMSlidingWindow();
		void init(u8 memLevel, const uint& winSize, const uint& begin, const uint& end) throw (NtseException);
		void scrollWin(const uint& size);
		void searchInWindow(const uint& seekStart, RCMSearchResultBuf* result);
		uint hashCode(const RCMSearchBufUnit* data, const uint& offset, const uint& len);
	private:
		uint              m_beginPos;         /** ����������ʼλ�� */
		uint              m_endPos;           /** �������ڽ���λ�� */
		uint              m_winSize;          /** �������ڴ�С */
		RCMSearchBuffer   *m_searchBuffer;    /** �����������ڵĲ��һ����� */
		SlidingWinHashTbl *m_hashTbl;         /** �������ڹ�ϣ�� */
		u8                m_winDetectTimes;   /** ����������ƥ����̽����� */
		uint              m_headHashCode;     /** �������ڵ�һ���ֽ�Ϊ�׵�N���ֽڵĹ�ϣֵ, NΪƥ�������С���� */
	};//class RCMSlidingWindow

	/** 
	 * ���һ�����
	 * ���һ�����ʹ��ѭ������ʵ�֣����ȴ��������������ڴ�С
	 */
	class RCMSearchBuffer {
	public:
		RCMSearchBuffer(uint size, u16 minMatchLen, u16 maxMatchLen);
		~RCMSearchBuffer();
		void init(const RCMSearchBufUnit *buf, const uint& size);
		void createWindow(uint winSize, u8 winDetectTimes, u8 memLevel) throw (NtseException);
		/**
		 * �Ƿ���֮ǰ����������λ��
		 * @param offset ����ڲ��һ�����ͷ��ƫ����
		 */
		inline bool needSeek(const uint& offset) {
			assert(m_searchData != NULL);
			size_t pos = MOD((m_head + offset), m_size);
			return m_searchData[pos].m_neekSeek;
		}
		/**
		 * ��ǲ���������λ��
		 * @param offset ����ڲ��һ�����ͷ��ƫ���� 
		 */
		inline void setSeekPos(const uint& offset) {
			assert(m_searchData != NULL);
			size_t pos = MOD((m_head + offset), m_size);
			m_searchData[pos].m_neekSeek = true;
		}
		/**
		 * ���ָ���Ĳ��һ�������Ԫ
		 * @param offset ����ڲ��һ�����ͷ��ƫ����
		 * @return ��Ӧ�Ĳ��һ�������Ԫ
		 */
		inline const RCMSearchBufUnit& getUnit(const uint& offset) const {
			assert(m_searchData != NULL);
			uint pos = MOD((m_head + offset), m_size);
			return m_searchData[pos];
		}
		/**
		 * �Ƚ�������������Ԫ�ֽ������Ƿ����
		 * @param first ����Ƚ���1��ƫ����
		 * @param second ����Ƚ���2��ƫ����
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
		 * �ƶ���������
		 * @param size �ƶ�����
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
		 * �ڲ��һ������в���ƥ����������
		 * @param result OUT ����������������
		 */
		inline void runSearch(RCMSearchResultBuf *result) {
			m_win->searchInWindow(m_head, result);
		}
		/**
		 * �ƶ����һ��������Ҳ���һ���µĵ�Ԫ
		 * @param data             ��Ԫ����
		 * @param isColGrpBoundary ��Ԫ�Ƿ�Ϊ��¼�߽�
		 * @param needSeek         �Ƿ�Ӵ˵�Ԫ��ʼ����
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
		RCMSearchBufUnit *m_searchData; /** ���һ����� */
		uint              m_head;       /** ���һ�������ʼλ�� */
		uint              m_tail;       /** ���һ���������λ�� */
		uint              m_size;       /** ���һ�������С(��Ϊ2�������η�) */
		RCMSlidingWindow  *m_win;       /** �������� */
		u16               m_minMatchLen;/** ���ƥ�䳤������ */
		u16               m_maxMatchLen;/** �ƥ�䳤������ */
	};//class RCMSearchBuffer

	/** 
	 * ���ҽ������
	 * ���ҽ���������ڱ����ڻ������ڲ���ƥ����ؽ��ʱƵ���������ٶ���
	 * ����֮��Ϳ���������һ�η��ؽ��
	 */
	class RCMSearchResultBuf {
	public:
		const static uint MAX_SKIP_LEN_CAPACITY = 256;

	public:
		/**
		 * ������ҽ������
		 * @param mtx              �ڴ����������
		 * @param matchBufCapacity ƥ���ֽڻ�������
		 * @param skipLenCapacity  �������Ȼ�������
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
		   * ���ò��ҽ��������
		   */
		  inline void reset() {
			  m_matchByteSize = 0;
			  m_skipLenBufSize = 0;
		  }
		  /**
		   * ���ҽ���Ƿ�Ϊ��
		   */
		  inline bool empty() {
			  return m_matchByteSize == 0;
		  }
		  /**
		   * ���ƥ���ֽڻ�����ʼ��ַ
		   */
		  inline const byte* getMatchResult() {
			  return m_matchByteBuf;
		  }
		  /**
		   * ���ƥ���ֽڻ���ʵ�ʴ�С
		   */
		  inline u16 getMatchResultSize() const {
			  return m_matchByteSize;
		  }
		  /**
		  * ��õ�ǰ�������Ȼ���ʵ�ʴ�С
		  */
		  inline uint getSkipLenBufSize() const {
			  return m_skipLenBufSize;
		  }
		  /**
		  * ���ָ������������
		  * @param i ���������±�
		  * @return
		  */
		  inline uint getSkipLen(uint i) const {
			  assert(i < m_skipLenBufSize);
			  return m_skipLenBuf[i];
		  }
		  /**
		  * �ڲ��ҽ�����������һ��ƥ����
		  * @param buf    ƥ�����ַ
		  * @param offset ƥ������ʼƫ����
		  * @param len    ƥ�����
		  */
		  void addMatch(RCMSearchBuffer *buf, const uint& offset, const u16& len) {
			  assert(buf != NULL && len > 0);
			  assert(m_matchByteBuf != NULL);

			  if (len > m_matchByteSize) {
				  m_skipLenBuf[m_skipLenBufSize++] = len;
				  for (uint i = m_matchByteSize; i < len; i++) {
					  m_matchByteBuf[i] = buf->getUnit(offset + i).m_data;
				  }
				  m_matchByteSize = len;//ʵ�ʴ�С����Ϊlen
			  }
		  }
	private:
		byte       *m_matchByteBuf;           /** ƥ���ֽڻ��� */
		u16        m_matchBufCapacity;        /** ƥ���ֽڻ������� */
		u16        m_matchByteSize;           /** ƥ���ֽ�ʵ�ʴ�С */
		uint       *m_skipLenBuf;             /** �������Ȼ��� */
		uint       m_skipLenBufCapacity;      /** �������Ȼ������� */
		uint       m_skipLenBufSize;          /** �������Ȼ���ʵ�ʴ�С */
		friend class RCMSlidingWindow;
	};//class RCMSearchResultBuf

	/** 
	 * ������¼ɨ��
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
		* ��ɨ�赽������¼֮����еĲ���
		* @param redRow ɨ�赽�������ʽ�Ӽ�¼
		*/
		inline void afterGetRow(SubRecord *redRow) {
			convRedSubRcdToCompressOrder(redRow);
			assert(m_redRcdCache.m_size > 0);
			m_session->unlockRow(&m_rowLockHnd);//����Լ�¼�Ĺ�����
			m_hasScanedSize++;
		}

		/**
		* ���ӵ�ǰ������С
		*/
		inline void incrSampleSize() {
			m_hasSmplSize++;
		}

	protected:
		Session           *m_session;               /** �Ự */
		Database          *m_db;                    /** �����ı��������ݿ� */
		const TableDef    *m_tableDef;              /** �����ı�Ķ��� */
		const TableDef    *m_newTableDef;           /** �±��� */
		Records		      *m_records;               /** ������ļ�¼������ */
		RecordConvert     *m_converter;             /** ��¼ת���� */
		Records::Scan     *m_scan;                  /** ��ɨ�� */
		SmplTblStrategy   m_strategy;               /** ��������� */
		byte              *m_mysqlRcdCacheData;     /** REC_MYSQL��ʽ��¼�������� */
		Record            m_redRcdCache;            /** �����ʽ��¼���� */
		CompressOrderRecord m_cmprsOrderRcdCache;   /** ѹ�������ʽ��¼���� */
		uint              m_cmprsOrderRcdCacheLeft; /** ѹ�������ʽ��¼����ʣ��δ����ȡ����(λͼ����) */
		size_t            *m_segBoundary;           /** ѹ�������ʽ��¼�����εı߽� */
		u8                m_curSeg;                 /** ��ǰ���������Ĵ�ѹ���� */
		RowLockHandle     *m_rowLockHnd;            /** ��ǰ������� */
		HeapStatusEx      m_heapStatusEx;           /** ����չͳ����Ϣ */               
		u64               m_totalSmplRows;          /** �ܹ���Ҫ�����ļ�¼�� */
		u64               m_hasScanedSize;          /** �Ѿ�ɨ������ݴ�С */
		u64               m_hasSmplSize;            /** �Ѿ����������ݴ�С */
	};//class SmplRowScan

	/** 
	 * ����������¼ɨ��
	 */
	class PartedRowScan : public SmplRowScan {
	public:
		static const double FIRST_PART_PCNT;   /** �������¼��һ����ռ�ܵı��� */
		static const double SECOND_PART_PCNT;  /** �������¼�ڶ�����ռ�ܵı��� */

	public:
		PartedRowScan(Session *session, Database *db, const TableDef *tableDef, 
			Records *records, RecordConvert *converter = NULL);
	protected:
		virtual bool doGetNextRow();

	protected:
		u64               m_firstPartSize;          /** �������30-70���ԣ�ǰ70%��¼�����ݴ�С */
		u64               m_firstPartSampleSize;    /** �������30-70����, ǰ70%��¼��Ҫ���������ݴ�С */
	};

	/**
	 * ��ɢ������¼ɨ��
	 */
	class DiscreteRowScan : public SmplRowScan {
	public:
		DiscreteRowScan(Session *session, Database *db, const TableDef *tableDef, 
			Records *records, RecordConvert *converter = NULL);
	protected:
		virtual bool doGetNextRow();

	protected:
		u64 m_samplePartitionNum;  /** ���������� */
		u64 m_sampleSizePart;      /** ÿ��������Ҫ�����ļ�¼�� */
		u64 m_needSkipSize;        /** ��ǰ������Ҫ�����ļ�¼�� */
		u64 m_curSampleSize;	   /** ��ǰ�����Ѳ�����¼�� */
		u64 m_curSkipSize;         /** ��ǰ�����������ļ�¼�� */
	};

	/** ˳�������¼ɨ�� */
	typedef SmplRowScan SequenceRowScan;

	/**
	 * ��¼ѹ�������
	 */
	class RCMSampleTbl {
	public:
		static const double MIN_SMPL_PCT_THRESHOLD;  /** �ﵽ������С�Ĵ˱�������Ϊ�����ɹ� */

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
		Session         *m_session;         /** �Ự */
		Database        *m_db;              /** �����ı��������ݿ� */
		const TableDef  *m_tableDef;        /** �����ı�Ķ��� */
		Records		    *m_records;         /** ������ļ�¼������ */
		RecordConvert   *m_converter;       /** ��¼��ʽת���������ΪNULL���ʾ�Բ��õļ�¼������ת�� */
		RCMSearchBuffer *m_searchBuf;       /** ���һ����� */
		SmplTrie        *m_sampleTrie;      /** ������Trie�� */
		RowCompressCfg  *m_rowCpsCfg;       /** ��¼ѹ������ */
		SmplRowScan     *m_rowScan;         /** ������¼ɨ����� */
	};//class RCMSampleTbl
}
#endif 