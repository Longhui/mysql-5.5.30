/**
 * 记录压缩表采样
 *
 * @author 李伟钊(liweizhao@corp.netease.com, liweizhao@163.org)
 */
#include <algorithm>
#include <math.h>
#include "compress/RCMSampleTbl.h"
#include "compress/SmplTrie.h"
#include "misc/RecordHelper.h"

using namespace std;

namespace ntse {
	/************************************************************************/
	/* 滑动窗口哈希表相关                                                    */
	/************************************************************************/
	
	/**
	 * 构造滑动窗口哈希表
	 * @param mtx      内存分配上下文
	 * @param memLevel 哈希表内存使用水平
	 * @param winSize  滑动窗口大小           
	 */
	SlidingWinHashTbl::SlidingWinHashTbl(u8 memLevel, uint winSize) throw(NtseException) 
		: m_memLevel(memLevel) {
		m_hashNodePool = new ObjMemoryPool<HashNode>(winSize + 256);
		m_mtx = new MemoryContext(MTX_PAGE_SIZE, DFL_RESERVE_PAGES);
		m_hashSize = 1 << (m_memLevel + 7);
		m_hashTblEntry = new (m_mtx->alloc(m_hashSize * sizeof(DList<uint> *)))DList<uint> *[m_hashSize];
		for (uint i = 0; i < m_hashSize; i++) {
			m_hashTblEntry[i] = NULL;
		}
	}

	SlidingWinHashTbl::~SlidingWinHashTbl() {
		delete m_hashNodePool;
		for (uint i = 0; i < m_hashSize; i++) {
			m_hashTblEntry[i]->~DList<uint>();
			m_hashTblEntry[i] = NULL;
		}
		m_mtx->reset();
		delete m_mtx;
	}


	/************************************************************************/
	/* 滑动窗口相关                                                                    */
	/************************************************************************/

	/** 
	 * 构造滑动窗口
	 * @pre 滑动窗口的大小必须不大于查找缓冲区的一半大小
	 * @param searchBuffer 所在的查找缓冲区
	 * @param winDetectTimes 查找时在窗口内的最大探测次数
	 */
	RCMSlidingWindow::RCMSlidingWindow(RCMSearchBuffer *searchBuffer, u8 winDetectTimes): 
		m_beginPos(0), m_endPos(0), m_winSize(0),m_searchBuffer(searchBuffer), m_hashTbl(NULL), m_winDetectTimes(winDetectTimes) {
	}

	RCMSlidingWindow::~RCMSlidingWindow() {
		if (m_hashTbl != NULL) {
			delete m_hashTbl;
			m_hashTbl = NULL;
		}
	}

	/**
	 * 初始化滑动窗口
	 * @param mtx     内存分配上下文
     * @param memleve 哈希表的内存使用水平
	 * @param winSize 窗口大小
	 * @param begin   窗口起始位置
	 * @param end     窗口终止位置
	 */
	void RCMSlidingWindow::init(u8 memLevel, const uint& winSize, const uint& begin, 
		const uint& end) throw (NtseException) {
		m_beginPos = begin;
		m_endPos = end;
		m_winSize = winSize;

		assert(m_hashTbl == NULL);
		m_hashTbl = new SlidingWinHashTbl(memLevel, winSize);
		assert(m_hashTbl);
		assert(2 * m_winSize <= m_searchBuffer->size());

		//计算当前窗口中各个项的哈希值
		uint initSize = m_winSize - m_searchBuffer->minMatchLen() + 1;
		for (uint i = 0, currentPos = m_beginPos; i < initSize; i++) {
			uint hscode = hashCode(m_searchBuffer->getSearchData(), currentPos, m_searchBuffer->minMatchLen());
			assert(currentPos < m_searchBuffer->size());
			m_hashTbl->put(hscode, currentPos);
			currentPos = MOD(currentPos + 1, m_searchBuffer->size());
			if (unlikely(0 == i))
				m_headHashCode = hscode;
		}
	}

	/**
	 * 向前移动窗口
	 * @param size 前进大小 
	 */
	void RCMSlidingWindow::scrollWin(const uint& size) {
		/** 计算移出窗口的字节相关的哈希值 */
		s32 beginScroll = size;
		for (s32 current = m_beginPos, i = 0; i < beginScroll; i++) {
			m_hashTbl->removeSlotHead(m_headHashCode, current);
			uint nextPos = MOD(current + 1, m_searchBuffer->size());
			m_headHashCode = hashCode(m_searchBuffer->getSearchData(), nextPos, m_searchBuffer->minMatchLen());
			current = nextPos;
		}
		m_beginPos = MOD(m_beginPos + size, m_searchBuffer->size());

		/** 计算移入窗口的字节相关的哈希值 */
		s32 endScroll = size;
		s32 cur = m_endPos - m_searchBuffer->minMatchLen() + 1;
		if (unlikely(cur < 0))
			cur += m_searchBuffer->size();
		for (int i = 0; i < endScroll; i++) {
			uint nextPos = MOD(cur + 1, m_searchBuffer->size());
			uint hscode = hashCode(m_searchBuffer->getSearchData(), nextPos, m_searchBuffer->minMatchLen());
			assert(nextPos < m_searchBuffer->size());
			m_hashTbl->put(hscode, nextPos);
			cur = nextPos;
		}
		m_endPos = MOD(m_endPos + size, m_searchBuffer->size());
	}

	/**
	 * 在窗口中查找匹配串
	 * @param seekStart 查找缓冲区的查找起始偏移量
	 * @param result    OUT 输出参数，保存匹配结果
	 */
	void RCMSlidingWindow::searchInWindow(const uint& seekStart, RCMSearchResultBuf* result) {
		// Fix Bug #107914
		// 由于现在的Boundary，是Column Group的第一个字符，或者是记录的第一个字符，因此
		// 不能通过Boundary判断属性组的分段末尾，而是属性组的起始字符

		// if (unlikely(m_searchBuffer->isRecordEnd(seekStart)))//要查找的位置是属性组分段末尾
		// 	return;

		uint hc = hashCode(m_searchBuffer->getSearchData(), seekStart, m_searchBuffer->minMatchLen());
		DList<uint> *slotList = m_hashTbl->getSlot(hc);
		if (slotList != NULL && slotList->getSize() > 0) {//如果存在可能匹配的位置
			uint count = 0;
			DLink<uint> *header = slotList->getHeader();
			for (DLink<uint> *it = header->getNext(); it != header; it = it->getNext()) {
				assert(it->get() < m_searchBuffer->size());
				int srcToDest = it->get() - seekStart;
				if (unlikely(srcToDest  < 0)) {
					srcToDest += m_searchBuffer->size();
				}
				assert(srcToDest >= 0);
				uint lengthStrict = min((uint)srcToDest, (uint)m_searchBuffer->maxMatchLen());
				int destToEnd = m_endPos - it->get();
				if (unlikely(destToEnd < 0)) {
					destToEnd += m_searchBuffer->size();
				}
				assert(destToEnd >= 0);
				lengthStrict = min(lengthStrict, (uint)destToEnd);
				
				uint src = seekStart;
				uint dest = it->get();
				u16 matchLen = 0;

				// Fix Bug #107914
				// 由于Boundary是属性组的第一个字符，因此需要提出循环进行判断
				if (m_searchBuffer->isEqual(src, dest)) {
					src = MOD(src + 1, m_searchBuffer->size());
					dest = MOD(dest + 1, m_searchBuffer->size());
					++matchLen;
					
					// 循环判断第一个字符之后，有多少连续匹配的字符串
					// 循环匹配的结束条件：
					// 1. 无论是源，还是目标字符串，达到了下一个属性组的起始字符，当前属性组遍历完毕；
					// 2. 连续匹配字符串的长度，超过了预定义的长度上限

					while(m_searchBuffer->isEqual(src, dest) &&//源与目的地匹配
						!m_searchBuffer->isRecordEnd(src) && //源没到达记录末尾
						!m_searchBuffer->isRecordEnd(dest) &&//目的地没有到达记录末尾
						matchLen < lengthStrict) {//匹配长度小于限制
							src = MOD(src + 1, m_searchBuffer->size());
							dest = MOD(dest + 1, m_searchBuffer->size());
							++matchLen;
					}//while
				}

				if (matchLen > m_searchBuffer->minMatchLen()) {
					result->addMatch(m_searchBuffer, seekStart, matchLen);
					count++;
					if (count >= m_winDetectTimes)//符合要求的匹配次数超过最大探测次数
						break;
				}
			}//for
		}
	}

	/**
	 * 计算若干字节的哈希值
	 * @param data   数据起始地址
	 * @param offset 起始位置
	 * @param len    要哈希的长度
	 */
	inline uint RCMSlidingWindow::hashCode(const RCMSearchBufUnit* data, const uint& offset, const uint& len) {
		uint hashCode = 0;
		for (uint i = 0; i < len; i++) {
			hashCode *= 16777619;
			uint j = MOD(offset + i, m_searchBuffer->size());
			hashCode ^= data[j].m_data;
		}
		return hashCode;
	}

	/************************************************************************/
	/* 查找缓冲区相关                                                       */
	/************************************************************************/
	/**
	 * 构造一个查找缓冲区
	 * @param size        查找缓冲区的大小
	 * @param minMatchLen 匹配项最小长度限制
	 * @param maxMatchLen 匹配项最大长度限制
	 */
	RCMSearchBuffer::RCMSearchBuffer(uint size, u16 minMatchLen, u16 maxMatchLen): 
	m_size(size), m_minMatchLen(minMatchLen), m_maxMatchLen(maxMatchLen) {
		m_searchData = new RCMSearchBufUnit[m_size];
		m_head = m_tail = 0;
		m_win = NULL;
	}
	RCMSearchBuffer::~RCMSearchBuffer() {
		m_head = m_tail = 0;
		delete m_win;
		delete []m_searchData;
	}

	/**
	 * 初始化查找缓冲区
	 * @param buf  要复制的查找缓冲区数据地址
	 * @param size 缓冲区数据大小
	 */
	void RCMSearchBuffer::init(const RCMSearchBufUnit *buf, const uint& size) {
		assert(size == m_size);
		m_head = 0;
		m_tail = m_size - 1;
		memcpy(m_searchData, buf, size * sizeof(RCMSearchBufUnit));
	}

	/**
	 * 在查找缓冲区上创建滑动窗口
	 * @param winSize        滑动窗口大小
	 * @param winDetectTimes 滑动窗口内匹配项查找探测次数
	 * @param memLevel       滑动窗口哈希表内存使用水平
	 */
	void RCMSearchBuffer::createWindow(uint winSize, u8 winDetectTimes, u8 memLevel) throw (NtseException) {
		assert(m_win == NULL);
		assert(m_searchData != NULL);
		assert(winSize <= m_size / 2 );
		m_win = new RCMSlidingWindow(this, winDetectTimes);
		m_win->init(memLevel, winSize, m_minMatchLen, MOD(m_minMatchLen + winSize - 1, m_size));
	}

	/************************************************************************/
	/* 采样记录扫描相关                                                                     */
	/************************************************************************/	

	/**
	* 构造采样记录扫描对象
	* @param session   会话
	* @param db        所属数据库
	* @param tableDef  表定义
	* @param records   记录管理
	* @param strategy  采样策略
	* @param converter 记录转化器
	*/
	SmplRowScan::SmplRowScan(Session *session, Database *db, const TableDef *tableDef, Records *records, 
		SmplTblStrategy strategy, RecordConvert *converter /*= NULL*/) : m_session(session), m_db(db),
		m_tableDef(tableDef),m_records(records), m_converter(converter), m_strategy(strategy) {
			m_scan = NULL;
			m_rowLockHnd = NULL;
			m_hasSmplSize = 0;
			m_hasScanedSize = 0;
			m_curSeg = 0;
			m_cmprsOrderRcdCacheLeft = 0;
			const LocalConfig *localConfig = session->getConnection()->getLocalConfig();
			uint updateStatusPages = (uint)((localConfig->m_tblSmplPct * 0.01) * (m_records->getHeap()->getUsedSize() / NTSE_PAGE_SIZE) + 1);
			m_records->getHeap()->updateExtendStatus(session, updateStatusPages);
			m_heapStatusEx = m_records->getHeap()->getStatusEx();

			assert(localConfig->m_tblSmplPct <= 100);
			m_totalSmplRows = (u64)(localConfig->m_tblSmplPct * 0.01 * m_heapStatusEx.m_numRows) + 1;
			assert(m_totalSmplRows > 0);

			m_newTableDef = m_converter ? m_converter->getNewTableDef() : m_tableDef;
			m_mysqlRcdCacheData = (byte *)m_session->getMemoryContext()->calloc(m_tableDef->m_maxRecSize);
			m_redRcdCache.m_data = m_mysqlRcdCacheData;
			m_redRcdCache.m_size = m_newTableDef->m_maxRecSize;

			m_redRcdCache.m_format = REC_REDUNDANT;

			m_cmprsOrderRcdCache.m_data = (byte *)m_session->getMemoryContext()->calloc(m_newTableDef->m_maxRecSize);
			m_cmprsOrderRcdCache.m_size = m_newTableDef->m_maxRecSize;
			m_cmprsOrderRcdCache.m_numSeg = m_newTableDef->m_numColGrps;
			m_cmprsOrderRcdCache.m_segSizes = (size_t *)m_session->getMemoryContext()->calloc(m_cmprsOrderRcdCache.m_numSeg * sizeof(size_t));

			m_segBoundary = (size_t *)m_session->getMemoryContext()->calloc(m_cmprsOrderRcdCache.m_numSeg * sizeof(size_t));
	}

	SmplRowScan::~SmplRowScan() {
		if (m_newTableDef != m_tableDef) {
			delete m_newTableDef;
			m_newTableDef = NULL;
		}
	}

	/** 
	 * 生成采样记录扫描实例
	 * @param session 会话
	 * @param db 所属数据库
	 * @param tableDef 表定义
	 * @param records 记录管理
	 * @param converter 记录转化器
	 */
	SmplRowScan* SmplRowScan::createRowScanInst(Session *session, Database *db, const TableDef *tableDef, 
		Records *records, RecordConvert *converter/*= NULL*/) {
			const LocalConfig *localConfig = session->getConnection()->getLocalConfig();
			SmplTblStrategy strategy = getSampleStrategyType(localConfig->m_smplStrategy);
			NTSE_ASSERT(VALID_SAMPLE_STRATEGY_NUM != strategy);

			switch (strategy) {
				case SEQUENCE_GET_ROWS:
					return new SequenceRowScan(session, db, tableDef, records, strategy, converter);
				case PARTED_GET_ROWS:
					return new PartedRowScan(session, db, tableDef, records, converter);
				case DISCRETE_GET_ROWS:
					return new DiscreteRowScan(session, db, tableDef, records, converter);
				default:
					NTSE_ASSERT(false);
			}
			return NULL;
	}

	/**
	* 开始表扫描
	*/
	void SmplRowScan::beginScan() {
		assert(m_tableDef->m_isCompressedTbl);
		assert(m_scan == NULL);

		/** 设置读取所有的属性 */
		u16 numReadCols = m_tableDef->m_numCols;
		u16 *readCols = (u16*)m_session->getMemoryContext()->calloc(numReadCols * sizeof(u16));
		assert(readCols != NULL);
		for (u16 i = 0; i < numReadCols; i++) {
			readCols[i] = i;
		}

		//FIXME:最后一个参数，扫描时是在遇到链接源还是链接目的时返回记录, 怎么理解？
		m_scan = m_records->beginScan(m_session, OP_READ, ColList(numReadCols, readCols),
			NULL, Shared, &m_rowLockHnd, false);//扫描过程对记录加共享锁
		m_scan->setNotReadLob();//大对象不参与记录压缩，可以不读取大对象内容

		m_db->getSyslog()->log(EL_LOG, "Begin records scan, sample size: "I64FORMAT"d, sample strategy: %s.",
			m_totalSmplRows, SMPL_STRATEGY_NAME[m_strategy]);
	}

	/**
	 * 获取下一条记录
	 * @return 获取是否成功
	 */
	inline bool SmplRowScan::getNextRow() {
		assert(m_scan != NULL);
		m_scan->releaseLastRow(false);//释放上一条记录占用的资源
		return doGetNextRow();
	}

	/**
	 * 实际获取下一条记录
	 * @return 获取是否成功
	 */
	bool SmplRowScan::doGetNextRow() {
		if (m_scan->getNext(m_mysqlRcdCacheData)) {
			SubRecord *redRow = m_scan->getRedRow();
			afterGetRow(redRow);
			incrSampleSize();
			return true;
		} else {
			return false;
		}
	}

	/**
	* 结束表扫描
	*/
	void SmplRowScan::endScan() {
		if (NULL != m_scan) {
			m_scan->releaseLastRow(false);
			m_scan->end();
			m_scan = NULL;//m_scan是在session的memcontext中分配，这里无需释放内存
		}
		m_rowLockHnd = NULL;
		m_mysqlRcdCacheData = NULL;
		m_segBoundary = NULL;

		m_db->getSyslog()->log(EL_LOG, "End records scan.");
	}

	/**
	* 将冗余格式子记录转化为压缩排序格式的记录
	* @param redRow 冗余格式子记录
	*/
	void SmplRowScan::convRedSubRcdToCompressOrder(const SubRecord *redRow) {
		assert(REC_REDUNDANT == redRow->m_format);
		/* 这里把SubRecord转化为Redundant格式的Record,
		* 注:构造扫描对象时需要指定读取所有的列才能进行这样的转化,
		* 转换后记录数据的地址直接指向子记录的的地址，因为都是指向
		* 同一个在session中分配的内存，直接使用应该是可以的
		*/
		if (m_converter) {
			assert(m_redRcdCache.m_size == m_newTableDef->m_maxRecSize);
			Record tmpRedRcd(redRow->m_rowId, REC_REDUNDANT, redRow->m_data, redRow->m_size);
			m_redRcdCache.m_data = m_converter->convertMysqlOrRedRec(&tmpRedRcd, m_session->getMemoryContext());
		} else {
			assert(m_redRcdCache.m_size == redRow->m_size);
			m_redRcdCache.m_rowId = redRow->m_rowId;
			m_redRcdCache.m_data = redRow->m_data;
		}
		m_cmprsOrderRcdCache.m_size = m_tableDef->m_maxRecSize;

		RecordOper::convRecordRedToCO(m_newTableDef, &m_redRcdCache, &m_cmprsOrderRcdCache);

		size_t sum = m_newTableDef->m_bmBytes;
		for (u8 i = 0; i < m_cmprsOrderRcdCache.m_numSeg; i++) {
			sum += m_cmprsOrderRcdCache.m_segSizes[i];
			m_segBoundary[i] = sum;
		}

		m_curSeg = 0;
		m_cmprsOrderRcdCacheLeft = m_cmprsOrderRcdCache.m_size - m_tableDef->m_bmBytes;
	}

	/**
	* 根据字符串解析采样策略类型
	* @param strategyStr 要解析的字符串地址
	* @return            采样策略
	*/
	SmplTblStrategy SmplRowScan::getSampleStrategyType(const char *strategyStr) {
		for (uint i = 0; i < VALID_SAMPLE_STRATEGY_NUM; i++)
			if (!System::stricmp(strategyStr, SMPL_STRATEGY_NAME[i]))
				return (SmplTblStrategy)i;
		return VALID_SAMPLE_STRATEGY_NUM;
	}


	const double PartedRowScan::FIRST_PART_PCNT = 0.7;
	const double PartedRowScan::SECOND_PART_PCNT = 1 - PartedRowScan::FIRST_PART_PCNT;
	
	/**
	 * 分区采样记录扫描构造函数
	 * @param session   会话
	 * @param db        所属数据库
	 * @param tableDef  表定义
	 * @param records   记录管理
	 * @param converter 记录转化器
	 */
	PartedRowScan::PartedRowScan(Session *session, Database *db, const TableDef *tableDef, 
		Records *records, RecordConvert *converter/* = NULL */) : 
		SmplRowScan(session, db, tableDef, records, PARTED_GET_ROWS, converter) {
			m_firstPartSize = (u64)(m_heapStatusEx.m_numRows * FIRST_PART_PCNT);
			m_firstPartSampleSize = (u64)(m_totalSmplRows * SECOND_PART_PCNT);
	}

	/**
	 * @see SmplRowScan::doGetNextRow()
	 */
	bool PartedRowScan::doGetNextRow() {
		assert(PARTED_GET_ROWS == m_strategy);
		//分两个区域获取采样记录
		while (m_scan->getNext(m_mysqlRcdCacheData)) {
			SubRecord *redRow = m_scan->getRedRow();
			afterGetRow(redRow);
			if (m_hasScanedSize <= m_firstPartSize //还没扫描完前70%的记录
				&& m_hasSmplSize >= m_firstPartSampleSize) {//已经采样完第一部分需要采样的数据
					m_scan->releaseLastRow(false);
					continue;
			} else {
				incrSampleSize();
				return true;
			}
		}
		m_cmprsOrderRcdCacheLeft = 0;
		//m_db->getSyslog()->log(EL_ERROR, "Failed to scan table because no enough records available!");
		return false;
	}

	/** 
	 * 离散采样记录扫描构造函数
	 * @param session   会话
	 * @param db        所属数据库
	 * @param tableDef  表定义
	 * @param records   记录管理
	 * @param converter 记录转化器
	 */
	DiscreteRowScan::DiscreteRowScan(Session *session, Database *db, const TableDef *tableDef, 
		Records *records, RecordConvert *converter/* = NULL */) : 
		SmplRowScan(session, db, tableDef, records, DISCRETE_GET_ROWS, converter) {
			m_samplePartitionNum = (u64)(log((double)m_heapStatusEx.m_numRows - m_totalSmplRows + 1) / log(2.0));
			if (m_samplePartitionNum > 0) {
				m_sampleSizePart = m_totalSmplRows > m_samplePartitionNum ? m_totalSmplRows / m_samplePartitionNum : 1;
				assert(m_sampleSizePart > 0);
				m_needSkipSize = (u64)pow(2.0, (int)m_samplePartitionNum - 1);
			} else {
				m_sampleSizePart = m_totalSmplRows;
				m_needSkipSize = 0;
			}
			m_curSkipSize = 0;
			m_curSampleSize = 0;
	}

	/**
	 * @see SmplRowScan::doGetNextRow()
	 */
	bool DiscreteRowScan::doGetNextRow() {
		assert(DISCRETE_GET_ROWS == m_strategy);
		while (m_scan->getNext(m_mysqlRcdCacheData)) {
			SubRecord *redRow = m_scan->getRedRow();
			afterGetRow(redRow);		

			if (m_needSkipSize < 1 || m_curSampleSize <= m_sampleSizePart) {
				incrSampleSize();
				m_curSampleSize++;
				return true;
			} else {
				m_scan->releaseLastRow(false);
				m_curSkipSize++;
				if (m_curSkipSize >= m_needSkipSize) {
					m_curSkipSize = 0;
					m_curSampleSize = 0;
					m_needSkipSize >>= 1;//每个采样区域的间隔逐步减半
				}
			}
		}
		m_cmprsOrderRcdCacheLeft = 0;
		//m_db->getSyslog()->log(EL_ERROR, "Failed to scan table because no enough records available!");
		return false;
	}

	/************************************************************************/
	/* 记录压缩表采样相关                                                   */
	/************************************************************************/

	const double RCMSampleTbl::MIN_SMPL_PCT_THRESHOLD = 0.95;
	
	/**
	 * 构造表采样对象
	 * @param session  会话
	 * @param db       被采样表所属的数据库
	 * @param tableDef 被采样表的定义
	 * @param records  被采样表的记录管理器
	 * @param convert  记录转化器，如果为NULL表示扫描的记录不需要转化
	 */
	RCMSampleTbl::RCMSampleTbl(Session *session, Database *db, const TableDef *tableDef, Records *records, 
		RecordConvert *converter /* = NULL */) throw (NtseException) : 
		m_session(session), m_db(db), m_tableDef(tableDef), m_records(records), m_converter(converter) {
			assert(m_tableDef->m_rowCompressCfg);

			m_rowCpsCfg = m_tableDef->m_rowCompressCfg;//表记录压缩配置
			const LocalConfig *localConfig = session->getConnection()->getLocalConfig();
			m_sampleTrie = new SmplTrie(session->getMemoryContext(), m_rowCpsCfg->dicSize(), 
				m_rowCpsCfg->dicItemMinLen(), m_rowCpsCfg->dicItemMaxLen(), localConfig->m_smplTrieBatchDelSize, 
				localConfig->m_smplTrieCte);

			/** 保证查找缓冲区大小不低于两倍滑动窗口大小 */
			uint searchBufSize = 1;
			uint doubleWinSize = 2 * localConfig->m_tblSmplWinSize;
			while (searchBufSize < doubleWinSize) {
				searchBufSize <<= 1;
			}
			m_searchBuf = new RCMSearchBuffer(searchBufSize, m_rowCpsCfg->dicItemMinLen(), m_rowCpsCfg->dicItemMaxLen());

			m_rowScan = SmplRowScan::createRowScanInst(session, db, tableDef, records, converter);
	}

	RCMSampleTbl::~RCMSampleTbl() {
		delete m_searchBuf;
		delete m_sampleTrie;
		delete m_rowScan;
	}

	/**
	 * 是否已经采样足够数据
	 * @return
	 */
	bool RCMSampleTbl::isSampleDataEnough() {
		return m_rowScan->m_hasSmplSize >= m_rowScan->m_totalSmplRows;
		//return m_rowScan->getCurScanPagesCount() > m_totalSmplPages;
	}

	/**
	* 表采样初始化
	*
	* 主要完成的工作是创建缓存扫描纪录的空间，创建扫描对象，以及初始化查找缓冲区
	* @return 初始化结果，SMP_NONE_ERR表示初始化成功，其他表示采样失败错误码
	*/
	SmpTblErrCode RCMSampleTbl::init() {
		assert(m_rowScan != NULL);
		assert(m_searchBuf != NULL);

		const LocalConfig *localConfig = m_session->getConnection()->getLocalConfig();

		m_rowScan->beginScan();//开始表扫描
		
		RCMSearchBufUnit * seartchBuf = new RCMSearchBufUnit[m_searchBuf->size()];
		for (uint i = 0; i < m_searchBuf->size(); i++) {
			if (m_rowScan->m_cmprsOrderRcdCacheLeft == 0) {//如果缓存的数据已经用完了，则读取扫描的下一条记录
				if(unlikely(!m_rowScan->getNextRow())) {
					//读取记录失败，初始化不成功
					delete []seartchBuf;
					seartchBuf = NULL;
					return SMP_NO_ENOUGH_ROWS;
				}
			}
			//位图不进行压缩，所以也不做采样
			uint curOffset = m_rowScan->m_cmprsOrderRcdCache.m_size - m_rowScan->m_cmprsOrderRcdCacheLeft;
			bool isColGrpBoundary = false;
			assert(m_rowScan->m_curSeg < m_tableDef->m_numColGrps);
			
			// Fix Bug #107914
			// 每一条记录的的第一个字符，一定是一个Boundary字符，因为是记录级压缩，因此采集跨越记录的数据字典，并无意义
			// 由于Null Bitmap不参与记录压缩的数据字典采集，因此记录的第一个字符，为Null Bitmap之后的字符
			// 若指定了Column Groups，则每一个Column Group的第一个字符，为Boundary字符
			if (curOffset == m_tableDef->m_bmBytes) {
				isColGrpBoundary = true;
			}

			if (curOffset == m_rowScan->m_segBoundary[m_rowScan->m_curSeg]) {
				isColGrpBoundary = true;
				m_rowScan->m_curSeg++;
			}
			RCMSearchBufUnit newUnit;
			newUnit.m_data = m_rowScan->m_cmprsOrderRcdCache.m_data[curOffset];
			newUnit.m_colGrpBoundary = isColGrpBoundary;
			newUnit.m_neekSeek = false;
			seartchBuf[i] = newUnit;
			m_rowScan->m_cmprsOrderRcdCacheLeft--;
		}
		m_searchBuf->init(seartchBuf, m_searchBuf->size());
		delete [] seartchBuf;
		seartchBuf = NULL;

		try {
			m_searchBuf->createWindow(localConfig->m_tblSmplWinSize, localConfig->m_tblSmplWinDetectTimes,
				localConfig->m_tblSmplWinMemLevel);
		} catch (NtseException &e) {
			UNREFERENCED_PARAMETER(e);
			return SMP_NO_ENOUGH_MEM;
		}

		return SMP_NO_ERR;
	}

	/** 
	 * 开始进行表采样
	 * @return 采样结果错误码，SMP_NONE_ERR表示采样成功
	 */
	SmpTblErrCode RCMSampleTbl::beginSampleTbl() {
		assert(m_searchBuf != NULL);
		assert(m_sampleTrie != NULL);
		assert(m_rowScan != NULL);

		SmpTblErrCode code = init();
		if(unlikely(code != SMP_NO_ERR)) {//如果初始化不成功
			m_db->getSyslog()->log(EL_LOG, "Failed to initialize when begin sample table %s for compression!", m_tableDef->m_name);
			return code;
		}

		m_db->getSyslog()->log(EL_LOG, "Begin sample table %s.%s, sample rows: %d.", 
			m_tableDef->m_schemaName, m_tableDef->m_name, m_rowScan->m_totalSmplRows);
		u32 start = System::fastTime();

		u64 lastHasSampleSize = 0;
		uint skip = 0;//当前被跳过的总次数
		RCMSearchResultBuf result(m_rowCpsCfg->dicItemMaxLen());
		while (true) {
			if (unlikely(isSampleDataEnough())) {//已经采样足够的数据
				m_db->getSyslog()->log(EL_LOG, "Sample table %s.%s finished 100%%", 
					m_tableDef->m_schemaName, m_tableDef->m_name);
				break;
			}
			if (unlikely((m_rowScan->m_hasSmplSize - lastHasSampleSize) * 1.0 / m_rowScan->m_totalSmplRows >= 0.2)) {
				double finishPct = (double)m_rowScan->m_hasSmplSize * 100.0 / m_rowScan->m_totalSmplRows;
				m_db->getSyslog()->log(EL_LOG, "Sample table %s.%s finished %.0f%%", 
					m_tableDef->m_schemaName, m_tableDef->m_name, finishPct);
				lastHasSampleSize = m_rowScan->m_hasSmplSize;
			}			
			result.reset();
			m_searchBuf->runSearch(&result);
			if(!result.empty()) {//搜索结果不为空
				m_sampleTrie->addItem(result.getMatchResult(), result.getMatchResultSize());
				m_sampleTrie->checkSize();
			}
			skip += result.getSkipLenBufSize();
			if (skip == 0) {//不存在跳过的位置
				if(unlikely(!refreshSearchBuf(1))) {
					break;
				}
			} else if (skip > 0) {//存在要跳过的位置
				uint skipLenBufSize = result.getSkipLenBufSize();
				for (uint i = 0; i < skipLenBufSize; i++)
				{
					if (m_searchBuf->needSeek(result.getSkipLen(i))) {//位置已经被标识过
						--skip;
					} else {
						m_searchBuf->setSeekPos(result.getSkipLen(i));
					}
				}//for
				//计算要跳过的长度
				uint scrollLen = 1;
				while(!m_searchBuf->needSeek(scrollLen)) {
					scrollLen++;
				}
				if(unlikely(!refreshSearchBuf(scrollLen))) {
					break;
				}
				--skip;
			}
		}//end while
		if (m_rowScan->m_hasSmplSize * 1.0 / m_rowScan->m_totalSmplRows < MIN_SMPL_PCT_THRESHOLD) {
			m_db->getSyslog()->log(EL_ERROR, "Failed to scan records when sample table. Sample Stratege: %s," \
				" table estimate number of rows: "I64FORMAT"d, has scaned rows: "I64FORMAT"d," \
				" has sample rows: "I64FORMAT"d, total sample rows: "I64FORMAT"d.", 
				SMPL_STRATEGY_NAME[m_rowScan->m_strategy], m_rowScan->m_heapStatusEx.m_numRows, 
				m_rowScan->m_hasScanedSize, m_rowScan->m_hasSmplSize, m_rowScan->m_totalSmplRows);
			return SMP_NO_ENOUGH_ROWS;
		}

		u32 stop = System::fastTime();
		m_db->getSyslog()->log(EL_LOG, "Finish sample table %s.%s, waste time : %d seconds.",
			m_tableDef->m_schemaName, m_tableDef->m_name, stop - start);

		return SMP_NO_ERR;
	}

	/**
	 * 结束表采样
	 */
	void RCMSampleTbl::endSampleTbl() {
		m_rowScan->endScan();
	}

	/**
	* 刷新查找缓冲区
	* @param size 刷新的字节数
	* @return     如果能够获取剩余记录内容返回true，否则返回false
	*/
	inline bool RCMSampleTbl::refreshSearchBuf(const uint& size) {
		assert(size <= m_tableDef->m_maxRecSize);
		assert(size <= m_tableDef->m_rowCompressCfg->dicItemMaxLen());

		//在刷新查找缓冲区之前需要先移动滑动窗口
		m_searchBuf->scrollWin(size);

		//填充新的记录数据
		uint curOffset = m_rowScan->m_cmprsOrderRcdCache.m_size - m_rowScan->m_cmprsOrderRcdCacheLeft;
		for (uint i = 0; i < size; i++) {
			if (unlikely(m_rowScan->m_cmprsOrderRcdCacheLeft == 0)) {
				if(unlikely(!m_rowScan->getNextRow())) {			
					return false;
				}
				curOffset = m_rowScan->m_cmprsOrderRcdCache.m_size - m_rowScan->m_cmprsOrderRcdCacheLeft;
			}
			assert(m_rowScan->m_cmprsOrderRcdCacheLeft <= m_rowScan->m_cmprsOrderRcdCache.m_size);
			bool isColGrpBoundary = false;
			assert(m_rowScan->m_curSeg < m_tableDef->m_numColGrps);

			// Fix Bug #107914
			// 每一条记录的的第一个字符，一定是一个Boundary字符，因为是记录级压缩，因此采集跨越记录的数据字典，并无意义
			// 由于Null Bitmap不参与记录压缩的数据字典采集，因此记录的第一个字符，为Null Bitmap之后的字符
			// 若指定了Column Groups，则每一个Column Group的第一个字符，为Boundary字符
			if (unlikely(curOffset == m_tableDef->m_bmBytes)) {
				isColGrpBoundary = true;
			}
			if (unlikely(curOffset == m_rowScan->m_segBoundary[m_rowScan->m_curSeg])) {
				isColGrpBoundary = true;
				m_rowScan->m_curSeg++;
			}
			m_searchBuf->moveOnce(m_rowScan->m_cmprsOrderRcdCache.m_data[curOffset], isColGrpBoundary, false);
			m_rowScan->m_cmprsOrderRcdCacheLeft--;
			curOffset++;
		}
		return true;
	}

	/**
	* 创建字典
	* @pre 已经进行过表采样
	* @param session 会话
	* @return        全局字典，在函数内部分配, 调用方负责回收
	*/
	RCDictionary* RCMSampleTbl::createDictionary(Session *session) throw(NtseException) {
		assert(m_sampleTrie != NULL);

		MemoryContext *mtx = session->getMemoryContext();
		McSavepoint savePoint(mtx);
		u32 start = 0;
		RCDictionary *dictionary = NULL;

		vector<SmplTrieNode *> *dicNodesList = new vector<SmplTrieNode *>(m_tableDef->m_rowCompressCfg->dicSize());
		assert(dicNodesList != NULL);
		uint actualDicSize = m_sampleTrie->extractDictionary(dicNodesList);

		if (actualDicSize > 0) {
			dictionary = new RCDictionary(m_tableDef->m_id, NULL, actualDicSize);

			byte *dicItemDataBuf = (byte *)mtx->alloc(m_tableDef->m_rowCompressCfg->dicItemMaxLen());
			assert(dicItemDataBuf != NULL);
			uint buildSize = 0;
			for (vector<SmplTrieNode *>::iterator it = dicNodesList->begin(); 
				buildSize < actualDicSize && it != dicNodesList->end(); it++) {
					u8 dicItemDataLen = m_sampleTrie->getFullKey(*it, dicItemDataBuf);
					dictionary->setDicItem(buildSize, dicItemDataBuf, dicItemDataLen);
					++buildSize;
			}
			assert(buildSize == actualDicSize);

			m_db->getSyslog()->log(EL_LOG, "Is building global dictionary...");
			start = System::fastTime();

			try {
#ifdef NTSE_READABLE_DICTIONARY
				string path = string(m_db->getConfig()->m_basedir) + NTSE_PATH_SEP + 
					string(m_tableDef->m_schemaName) + NTSE_PATH_SEP + string(m_tableDef->m_name);
				dictionary->buildDat(path.c_str());
#else 
				dictionary->buildDat();
#endif
			} catch (NtseException &e) {
				dictionary->close();
				delete dictionary;
				dictionary = NULL;

				delete dicNodesList;
				dicNodesList = NULL;

				throw e;
			}
		} else {
			delete dicNodesList;
			dicNodesList = NULL;
			assert(!dictionary);
			NTSE_THROW(NTSE_EC_GENERIC, "Failed to create dictionary, can not find dictionary items. " \
				"Maybe sample size is too small or the record data is too random.");
		}

		delete dicNodesList;
		dicNodesList = NULL;

		u32 stop = System::fastTime();
		m_db->getSyslog()->log(EL_LOG, "Finished building global dictionary. Waste time: %d seconds.", stop - start);

		return dictionary;
	}
}
