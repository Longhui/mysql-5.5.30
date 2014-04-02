/**
 * ��¼ѹ�������
 *
 * @author ��ΰ��(liweizhao@corp.netease.com, liweizhao@163.org)
 */
#include <algorithm>
#include <math.h>
#include "compress/RCMSampleTbl.h"
#include "compress/SmplTrie.h"
#include "misc/RecordHelper.h"

using namespace std;

namespace ntse {
	/************************************************************************/
	/* �������ڹ�ϣ�����                                                    */
	/************************************************************************/
	
	/**
	 * ���컬�����ڹ�ϣ��
	 * @param mtx      �ڴ����������
	 * @param memLevel ��ϣ���ڴ�ʹ��ˮƽ
	 * @param winSize  �������ڴ�С           
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
	/* �����������                                                                    */
	/************************************************************************/

	/** 
	 * ���컬������
	 * @pre �������ڵĴ�С���벻���ڲ��һ�������һ���С
	 * @param searchBuffer ���ڵĲ��һ�����
	 * @param winDetectTimes ����ʱ�ڴ����ڵ����̽�����
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
	 * ��ʼ����������
	 * @param mtx     �ڴ����������
     * @param memleve ��ϣ����ڴ�ʹ��ˮƽ
	 * @param winSize ���ڴ�С
	 * @param begin   ������ʼλ��
	 * @param end     ������ֹλ��
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

		//���㵱ǰ�����и�����Ĺ�ϣֵ
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
	 * ��ǰ�ƶ�����
	 * @param size ǰ����С 
	 */
	void RCMSlidingWindow::scrollWin(const uint& size) {
		/** �����Ƴ����ڵ��ֽ���صĹ�ϣֵ */
		s32 beginScroll = size;
		for (s32 current = m_beginPos, i = 0; i < beginScroll; i++) {
			m_hashTbl->removeSlotHead(m_headHashCode, current);
			uint nextPos = MOD(current + 1, m_searchBuffer->size());
			m_headHashCode = hashCode(m_searchBuffer->getSearchData(), nextPos, m_searchBuffer->minMatchLen());
			current = nextPos;
		}
		m_beginPos = MOD(m_beginPos + size, m_searchBuffer->size());

		/** �������봰�ڵ��ֽ���صĹ�ϣֵ */
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
	 * �ڴ����в���ƥ�䴮
	 * @param seekStart ���һ������Ĳ�����ʼƫ����
	 * @param result    OUT �������������ƥ����
	 */
	void RCMSlidingWindow::searchInWindow(const uint& seekStart, RCMSearchResultBuf* result) {
		// Fix Bug #107914
		// �������ڵ�Boundary����Column Group�ĵ�һ���ַ��������Ǽ�¼�ĵ�һ���ַ������
		// ����ͨ��Boundary�ж�������ķֶ�ĩβ���������������ʼ�ַ�

		// if (unlikely(m_searchBuffer->isRecordEnd(seekStart)))//Ҫ���ҵ�λ����������ֶ�ĩβ
		// 	return;

		uint hc = hashCode(m_searchBuffer->getSearchData(), seekStart, m_searchBuffer->minMatchLen());
		DList<uint> *slotList = m_hashTbl->getSlot(hc);
		if (slotList != NULL && slotList->getSize() > 0) {//������ڿ���ƥ���λ��
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
				// ����Boundary��������ĵ�һ���ַ��������Ҫ���ѭ�������ж�
				if (m_searchBuffer->isEqual(src, dest)) {
					src = MOD(src + 1, m_searchBuffer->size());
					dest = MOD(dest + 1, m_searchBuffer->size());
					++matchLen;
					
					// ѭ���жϵ�һ���ַ�֮���ж�������ƥ����ַ���
					// ѭ��ƥ��Ľ���������
					// 1. ������Դ������Ŀ���ַ������ﵽ����һ�����������ʼ�ַ�����ǰ�����������ϣ�
					// 2. ����ƥ���ַ����ĳ��ȣ�������Ԥ����ĳ�������

					while(m_searchBuffer->isEqual(src, dest) &&//Դ��Ŀ�ĵ�ƥ��
						!m_searchBuffer->isRecordEnd(src) && //Դû�����¼ĩβ
						!m_searchBuffer->isRecordEnd(dest) &&//Ŀ�ĵ�û�е����¼ĩβ
						matchLen < lengthStrict) {//ƥ�䳤��С������
							src = MOD(src + 1, m_searchBuffer->size());
							dest = MOD(dest + 1, m_searchBuffer->size());
							++matchLen;
					}//while
				}

				if (matchLen > m_searchBuffer->minMatchLen()) {
					result->addMatch(m_searchBuffer, seekStart, matchLen);
					count++;
					if (count >= m_winDetectTimes)//����Ҫ���ƥ������������̽�����
						break;
				}
			}//for
		}
	}

	/**
	 * ���������ֽڵĹ�ϣֵ
	 * @param data   ������ʼ��ַ
	 * @param offset ��ʼλ��
	 * @param len    Ҫ��ϣ�ĳ���
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
	/* ���һ��������                                                       */
	/************************************************************************/
	/**
	 * ����һ�����һ�����
	 * @param size        ���һ������Ĵ�С
	 * @param minMatchLen ƥ������С��������
	 * @param maxMatchLen ƥ������󳤶�����
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
	 * ��ʼ�����һ�����
	 * @param buf  Ҫ���ƵĲ��һ��������ݵ�ַ
	 * @param size ���������ݴ�С
	 */
	void RCMSearchBuffer::init(const RCMSearchBufUnit *buf, const uint& size) {
		assert(size == m_size);
		m_head = 0;
		m_tail = m_size - 1;
		memcpy(m_searchData, buf, size * sizeof(RCMSearchBufUnit));
	}

	/**
	 * �ڲ��һ������ϴ�����������
	 * @param winSize        �������ڴ�С
	 * @param winDetectTimes ����������ƥ�������̽�����
	 * @param memLevel       �������ڹ�ϣ���ڴ�ʹ��ˮƽ
	 */
	void RCMSearchBuffer::createWindow(uint winSize, u8 winDetectTimes, u8 memLevel) throw (NtseException) {
		assert(m_win == NULL);
		assert(m_searchData != NULL);
		assert(winSize <= m_size / 2 );
		m_win = new RCMSlidingWindow(this, winDetectTimes);
		m_win->init(memLevel, winSize, m_minMatchLen, MOD(m_minMatchLen + winSize - 1, m_size));
	}

	/************************************************************************/
	/* ������¼ɨ�����                                                                     */
	/************************************************************************/	

	/**
	* ���������¼ɨ�����
	* @param session   �Ự
	* @param db        �������ݿ�
	* @param tableDef  ����
	* @param records   ��¼����
	* @param strategy  ��������
	* @param converter ��¼ת����
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
	 * ���ɲ�����¼ɨ��ʵ��
	 * @param session �Ự
	 * @param db �������ݿ�
	 * @param tableDef ����
	 * @param records ��¼����
	 * @param converter ��¼ת����
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
	* ��ʼ��ɨ��
	*/
	void SmplRowScan::beginScan() {
		assert(m_tableDef->m_isCompressedTbl);
		assert(m_scan == NULL);

		/** ���ö�ȡ���е����� */
		u16 numReadCols = m_tableDef->m_numCols;
		u16 *readCols = (u16*)m_session->getMemoryContext()->calloc(numReadCols * sizeof(u16));
		assert(readCols != NULL);
		for (u16 i = 0; i < numReadCols; i++) {
			readCols[i] = i;
		}

		//FIXME:���һ��������ɨ��ʱ������������Դ��������Ŀ��ʱ���ؼ�¼, ��ô��⣿
		m_scan = m_records->beginScan(m_session, OP_READ, ColList(numReadCols, readCols),
			NULL, Shared, &m_rowLockHnd, false);//ɨ����̶Լ�¼�ӹ�����
		m_scan->setNotReadLob();//����󲻲����¼ѹ�������Բ���ȡ���������

		m_db->getSyslog()->log(EL_LOG, "Begin records scan, sample size: "I64FORMAT"d, sample strategy: %s.",
			m_totalSmplRows, SMPL_STRATEGY_NAME[m_strategy]);
	}

	/**
	 * ��ȡ��һ����¼
	 * @return ��ȡ�Ƿ�ɹ�
	 */
	inline bool SmplRowScan::getNextRow() {
		assert(m_scan != NULL);
		m_scan->releaseLastRow(false);//�ͷ���һ����¼ռ�õ���Դ
		return doGetNextRow();
	}

	/**
	 * ʵ�ʻ�ȡ��һ����¼
	 * @return ��ȡ�Ƿ�ɹ�
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
	* ������ɨ��
	*/
	void SmplRowScan::endScan() {
		if (NULL != m_scan) {
			m_scan->releaseLastRow(false);
			m_scan->end();
			m_scan = NULL;//m_scan����session��memcontext�з��䣬���������ͷ��ڴ�
		}
		m_rowLockHnd = NULL;
		m_mysqlRcdCacheData = NULL;
		m_segBoundary = NULL;

		m_db->getSyslog()->log(EL_LOG, "End records scan.");
	}

	/**
	* �������ʽ�Ӽ�¼ת��Ϊѹ�������ʽ�ļ�¼
	* @param redRow �����ʽ�Ӽ�¼
	*/
	void SmplRowScan::convRedSubRcdToCompressOrder(const SubRecord *redRow) {
		assert(REC_REDUNDANT == redRow->m_format);
		/* �����SubRecordת��ΪRedundant��ʽ��Record,
		* ע:����ɨ�����ʱ��Ҫָ����ȡ���е��в��ܽ���������ת��,
		* ת�����¼���ݵĵ�ֱַ��ָ���Ӽ�¼�ĵĵ�ַ����Ϊ����ָ��
		* ͬһ����session�з�����ڴ棬ֱ��ʹ��Ӧ���ǿ��Ե�
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
	* �����ַ�������������������
	* @param strategyStr Ҫ�������ַ�����ַ
	* @return            ��������
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
	 * ����������¼ɨ�蹹�캯��
	 * @param session   �Ự
	 * @param db        �������ݿ�
	 * @param tableDef  ����
	 * @param records   ��¼����
	 * @param converter ��¼ת����
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
		//�����������ȡ������¼
		while (m_scan->getNext(m_mysqlRcdCacheData)) {
			SubRecord *redRow = m_scan->getRedRow();
			afterGetRow(redRow);
			if (m_hasScanedSize <= m_firstPartSize //��ûɨ����ǰ70%�ļ�¼
				&& m_hasSmplSize >= m_firstPartSampleSize) {//�Ѿ��������һ������Ҫ����������
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
	 * ��ɢ������¼ɨ�蹹�캯��
	 * @param session   �Ự
	 * @param db        �������ݿ�
	 * @param tableDef  ����
	 * @param records   ��¼����
	 * @param converter ��¼ת����
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
					m_needSkipSize >>= 1;//ÿ����������ļ���𲽼���
				}
			}
		}
		m_cmprsOrderRcdCacheLeft = 0;
		//m_db->getSyslog()->log(EL_ERROR, "Failed to scan table because no enough records available!");
		return false;
	}

	/************************************************************************/
	/* ��¼ѹ����������                                                   */
	/************************************************************************/

	const double RCMSampleTbl::MIN_SMPL_PCT_THRESHOLD = 0.95;
	
	/**
	 * ������������
	 * @param session  �Ự
	 * @param db       �����������������ݿ�
	 * @param tableDef ��������Ķ���
	 * @param records  ��������ļ�¼������
	 * @param convert  ��¼ת���������ΪNULL��ʾɨ��ļ�¼����Ҫת��
	 */
	RCMSampleTbl::RCMSampleTbl(Session *session, Database *db, const TableDef *tableDef, Records *records, 
		RecordConvert *converter /* = NULL */) throw (NtseException) : 
		m_session(session), m_db(db), m_tableDef(tableDef), m_records(records), m_converter(converter) {
			assert(m_tableDef->m_rowCompressCfg);

			m_rowCpsCfg = m_tableDef->m_rowCompressCfg;//���¼ѹ������
			const LocalConfig *localConfig = session->getConnection()->getLocalConfig();
			m_sampleTrie = new SmplTrie(session->getMemoryContext(), m_rowCpsCfg->dicSize(), 
				m_rowCpsCfg->dicItemMinLen(), m_rowCpsCfg->dicItemMaxLen(), localConfig->m_smplTrieBatchDelSize, 
				localConfig->m_smplTrieCte);

			/** ��֤���һ�������С�����������������ڴ�С */
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
	 * �Ƿ��Ѿ������㹻����
	 * @return
	 */
	bool RCMSampleTbl::isSampleDataEnough() {
		return m_rowScan->m_hasSmplSize >= m_rowScan->m_totalSmplRows;
		//return m_rowScan->getCurScanPagesCount() > m_totalSmplPages;
	}

	/**
	* �������ʼ��
	*
	* ��Ҫ��ɵĹ����Ǵ�������ɨ���¼�Ŀռ䣬����ɨ������Լ���ʼ�����һ�����
	* @return ��ʼ�������SMP_NONE_ERR��ʾ��ʼ���ɹ���������ʾ����ʧ�ܴ�����
	*/
	SmpTblErrCode RCMSampleTbl::init() {
		assert(m_rowScan != NULL);
		assert(m_searchBuf != NULL);

		const LocalConfig *localConfig = m_session->getConnection()->getLocalConfig();

		m_rowScan->beginScan();//��ʼ��ɨ��
		
		RCMSearchBufUnit * seartchBuf = new RCMSearchBufUnit[m_searchBuf->size()];
		for (uint i = 0; i < m_searchBuf->size(); i++) {
			if (m_rowScan->m_cmprsOrderRcdCacheLeft == 0) {//�������������Ѿ������ˣ����ȡɨ�����һ����¼
				if(unlikely(!m_rowScan->getNextRow())) {
					//��ȡ��¼ʧ�ܣ���ʼ�����ɹ�
					delete []seartchBuf;
					seartchBuf = NULL;
					return SMP_NO_ENOUGH_ROWS;
				}
			}
			//λͼ������ѹ��������Ҳ��������
			uint curOffset = m_rowScan->m_cmprsOrderRcdCache.m_size - m_rowScan->m_cmprsOrderRcdCacheLeft;
			bool isColGrpBoundary = false;
			assert(m_rowScan->m_curSeg < m_tableDef->m_numColGrps);
			
			// Fix Bug #107914
			// ÿһ����¼�ĵĵ�һ���ַ���һ����һ��Boundary�ַ�����Ϊ�Ǽ�¼��ѹ������˲ɼ���Խ��¼�������ֵ䣬��������
			// ����Null Bitmap�������¼ѹ���������ֵ�ɼ�����˼�¼�ĵ�һ���ַ���ΪNull Bitmap֮����ַ�
			// ��ָ����Column Groups����ÿһ��Column Group�ĵ�һ���ַ���ΪBoundary�ַ�
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
	 * ��ʼ���б����
	 * @return ������������룬SMP_NONE_ERR��ʾ�����ɹ�
	 */
	SmpTblErrCode RCMSampleTbl::beginSampleTbl() {
		assert(m_searchBuf != NULL);
		assert(m_sampleTrie != NULL);
		assert(m_rowScan != NULL);

		SmpTblErrCode code = init();
		if(unlikely(code != SMP_NO_ERR)) {//�����ʼ�����ɹ�
			m_db->getSyslog()->log(EL_LOG, "Failed to initialize when begin sample table %s for compression!", m_tableDef->m_name);
			return code;
		}

		m_db->getSyslog()->log(EL_LOG, "Begin sample table %s.%s, sample rows: %d.", 
			m_tableDef->m_schemaName, m_tableDef->m_name, m_rowScan->m_totalSmplRows);
		u32 start = System::fastTime();

		u64 lastHasSampleSize = 0;
		uint skip = 0;//��ǰ���������ܴ���
		RCMSearchResultBuf result(m_rowCpsCfg->dicItemMaxLen());
		while (true) {
			if (unlikely(isSampleDataEnough())) {//�Ѿ������㹻������
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
			if(!result.empty()) {//���������Ϊ��
				m_sampleTrie->addItem(result.getMatchResult(), result.getMatchResultSize());
				m_sampleTrie->checkSize();
			}
			skip += result.getSkipLenBufSize();
			if (skip == 0) {//������������λ��
				if(unlikely(!refreshSearchBuf(1))) {
					break;
				}
			} else if (skip > 0) {//����Ҫ������λ��
				uint skipLenBufSize = result.getSkipLenBufSize();
				for (uint i = 0; i < skipLenBufSize; i++)
				{
					if (m_searchBuf->needSeek(result.getSkipLen(i))) {//λ���Ѿ�����ʶ��
						--skip;
					} else {
						m_searchBuf->setSeekPos(result.getSkipLen(i));
					}
				}//for
				//����Ҫ�����ĳ���
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
	 * ���������
	 */
	void RCMSampleTbl::endSampleTbl() {
		m_rowScan->endScan();
	}

	/**
	* ˢ�²��һ�����
	* @param size ˢ�µ��ֽ���
	* @return     ����ܹ���ȡʣ���¼���ݷ���true�����򷵻�false
	*/
	inline bool RCMSampleTbl::refreshSearchBuf(const uint& size) {
		assert(size <= m_tableDef->m_maxRecSize);
		assert(size <= m_tableDef->m_rowCompressCfg->dicItemMaxLen());

		//��ˢ�²��һ�����֮ǰ��Ҫ���ƶ���������
		m_searchBuf->scrollWin(size);

		//����µļ�¼����
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
			// ÿһ����¼�ĵĵ�һ���ַ���һ����һ��Boundary�ַ�����Ϊ�Ǽ�¼��ѹ������˲ɼ���Խ��¼�������ֵ䣬��������
			// ����Null Bitmap�������¼ѹ���������ֵ�ɼ�����˼�¼�ĵ�һ���ַ���ΪNull Bitmap֮����ַ�
			// ��ָ����Column Groups����ÿһ��Column Group�ĵ�һ���ַ���ΪBoundary�ַ�
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
	* �����ֵ�
	* @pre �Ѿ����й������
	* @param session �Ự
	* @return        ȫ���ֵ䣬�ں����ڲ�����, ���÷��������
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
