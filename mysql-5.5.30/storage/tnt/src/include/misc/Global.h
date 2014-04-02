/**
 * NTSE�洢��������õĶ���
 *
 * @author ��Դ(wangyuan@corp.netease.com, wy@163.org)
 */

#ifndef _NTSE_GLOBAL_H_
#define _NTSE_GLOBAL_H_

#include "util/Portable.h"
#include <stddef.h>
#include <stdio.h>
#include <assert.h>
#include <vector>
#include <string>

#define TNT_ENGINE

namespace ntse {

#define NTSE_PATH_SEP "/"
#define NTSE_PATH_SEP_CHAR '/'

/** ��¼ID������35λ��ҳ�ź�13λ�ļ�¼�ۺţ����32G������ҳ��ÿҳ���8192����¼����ռ��6�ֽ� */
typedef u64	RowId;

/** ����ҳ��ID */
typedef u64 PageId;

/** �����ID */
typedef u64 LobId;

/** ��ID */
typedef u16 TableId;

#ifdef TNT_ENGINE
typedef u64 TrxId;  /** TNT����ID */
#define INVALID_TRX_ID ((TrxId)-1) /** ��Ч��TNT����ID */

typedef u16 RowIdVersion; /** RowId�汾�ţ�������֤RowId�Ƿ������� */
#define INVALID_VERSION 0

#define INVALID_VERSION_POOL_INDEX ((u8)(-1))
#define INVALID_SAVE_POINT         ((u64)(-1))
#endif

#define	XA_OK		 0		/*!< normal execution */
#define	XAER_ASYNC	-2		/*!< asynchronous operation already
								outstanding */
#define	XAER_RMERR	-3		/*!< a resource manager error
								occurred in the transaction
								branch */
#define	XAER_NOTA	-4		/*!< the XID is not valid */
#define	XAER_INVAL	-5		/*!< invalid arguments were given */
#define	XAER_PROTO	-6		/*!< routine invoked in an improper
								context */
#define	XAER_RMFAIL	-7		/*!< resource manager unavailable */
#define	XAER_DUPID	-8		/*!< the XID already exists */
#define	XAER_OUTSIDE	-9	/*!< resource manager doing
								work outside transaction */

#define INVALID_ROW_ID	((RowId)(-1))		/** ��Ч�ļ�¼ID */
#define INVALID_PAGE_ID	((PageId)(-1))		/** ��Ч��ҳ��ID */
#define INVALID_LSN		((u64)(-1))			/** ��Ч��LSN�� */
#define INVALID_TOKEN   ((int)(-1))         /** ��Ч��token */

#define PAGE_BITS	35		/** ҳ��ռ��λ�� */
#define SLOT_BITS	13		/** ��¼�ۺ�ռ��λ�� */
#define RID_BYTES	6		/** ���л�һ��RIDռ�õ��ֽ��� */
#define RID_BITS	48		/** RIDռ��λ�� */

/** ����RID�õ�ҳ�ţ�ҳ��Ϊu64���ͣ���ʵ����ֻ�ܱ�ʾ2^35��ҳ */
#define RID_GET_PAGE(rid)	((u64)(((rid) >> SLOT_BITS) & ((((u64)1) << PAGE_BITS) - 1)))
/** ����RID�õ���¼�ۺţ���¼�ۺ�Ϊu16���ͣ���ʵ����ֻ�ܱ�ʾ2^13����¼ */
#define RID_GET_SLOT(rid)	((u16)((rid) & (u64)0x1FFF))
/** ����ҳ�źͼ�¼�ۺŵõ�RID */
#define RID(page, slot)		((RowId)(((page) << SLOT_BITS) | (u64)slot))

/** ���������غ궨��*/
#define MY_ALIGN(A,L)	(((A) + (L) - 1) & ~((L) - 1))
#define ALIGN_SIZE(A)	MY_ALIGN((A),sizeof(double))

/** ��ȡһ��RID��RIDռ��RID_BYTES���ֽ� */
inline RowId RID_READ(byte *buf) {
	u64 v = (u64)(*(u32 *)buf);
	v = v << 16;
	v += *(u16 *)(buf + 4);
	return v;
}

/** д��һ��RID��RIDռ��RID_BYTES���ֽ� */
inline void RID_WRITE(RowId rid, byte *buf) {
	*(u32 *)buf = (u32)((rid >> 16) & 0xFFFFFFFF);
	*(u16 *)(buf + 4) = (u16)(rid & 0xFFFF);
}

/**
 * ΪROWID�����ṩ�����ϣֵ���ܵĺ�������
 * 
 */
class RidHasher {
public:
	inline unsigned int operator()(const RowId &v) const {
		return hashCode(v);
	}

	static unsigned int hashCode(const RowId &v) {
		u16 slot = RID_GET_SLOT(v);
		u64 page = RID_GET_PAGE(v);
		return (unsigned int)((page << m_ridShift.m_size2shift[slot & ((u16)0x7FF)]) | slot);
	}

	class RidShift { //�ڲ��࣬���ڼ���λ������
	public:
		RidShift() {
			// ����λ�Ʊ�
			u8 shift = 1;
			int slotThreshold = 2;

			m_size2shift = new u8[2048];
			for (int slotSize = 0; slotSize < 2048; slotSize++) {
				if (slotSize >= slotThreshold) {
					assert (slotSize == slotThreshold);
					shift++;
					slotThreshold <<= 1;
				}
				m_size2shift[slotSize] = (u8) shift;
			}
		}

		~RidShift() {
			delete [] m_size2shift;
			m_size2shift = NULL;
		}

		u8	*m_size2shift;			/** λ�Ʊ�(����RidMap) */ 
	};

	static RidShift m_ridShift;
};

/** NTSE���ô���� */
enum ErrorCode {
	NTSE_EC_GENERIC,		/** �������� */
	NTSE_EC_OUT_OF_MEM,		/** �ڴ治�� */
	NTSE_EC_FILE_NOT_EXIST,	/** �ļ������� */
	NTSE_EC_FILE_PERM_ERROR,/** �ļ�ûȨ�޲��� */
	NTSE_EC_DISK_FULL,		/** �������� */
	NTSE_EC_FILE_EXIST,		/** �ļ��Ѿ����� */
	NTSE_EC_FILE_IN_USE,	/** �ļ���ʹ���� */
	NTSE_EC_FILE_EOF,		/** ��ȡ��д�����ʱָ����ƫ���������ļ���С */
	NTSE_EC_READ_FAIL,		/** �����ݳ��� */
	NTSE_EC_WRITE_FAIL,		/** д���ݳ��� */
	NTSE_EC_FILE_FAIL,		/** �����ļ��������� */
    NTSE_EC_ACCESS_OUT_OF_PAGE, /** ��д����������ҳ�߽� */
	NTSE_EC_PAGE_DAMAGE,	/** ҳ�����ݱ��ƻ� */
	NTSE_EC_FORMAT_ERROR,/** �����ļ����ƻ� */
	NTSE_EC_OVERFLOW,		/** Խ����� */
	NTSE_EC_INDEX_BROKEN,	/** �����ṹ���� */
	NTSE_EC_INDEX_UNQIUE_VIOLATION,	/** ����Υ��Ψһ��Լ�� */
	NTSE_EC_NOT_LOCKED,			/** ��ͼ�޸�δ��������Դ */
	NTSE_EC_TOO_MANY_ROWLOCK,	/** ������Ŀ���������С */
	NTSE_EC_TOO_MANY_SESSION,	/** ���лỰ����ʹ���� */
	NTSE_EC_NOT_SUPPORT,		/** NTSE�洢���治֧�ֵ��������͡����ԡ������� */
	NTSE_EC_EXCEED_LIMIT,		/** ��ID�ȸ�������������С�ȳ������� */
	NTSE_EC_CORRUPTED_LOGFILE,  /** ��־�ļ����� */
	NTSE_EC_MISSING_LOGFILE,	/** ��־�ļ������� */
	NTSE_EC_DUP_TABLEID,	/** ��ID�ظ� */
	NTSE_EC_INVALID_BACKUP,	/** ��Ч�ı����ļ�����Ŀ¼ */
	NTSE_EC_LOCK_TIMEOUT,	/** ������ʱ */
	NTSE_EC_LOCK_FAIL,		/** ����ʧ�� */
	NTSE_EC_SYNTAX_ERROR,	/** �﷨���� */
	NTSE_EC_ROW_TOO_LONG,	/** ��¼���� */
	NTSE_EC_NONEINDEX,		/** û����Ҫ������������ */
	NTSE_EC_DUPINDEX,		/** �ظ������� */
	NTSE_EC_COLDEF_ERROR,	/** �ж������ */
	NTSE_EC_CANCELED,		/** ������ȡ�� */
	NTSE_EC_CORRUPTED,		/** ������ */
	NTSE_EC_TABLEDEF_ERROR, /** tabledef�Ķ����ļ������ϸ�ʽ*/
	NTSE_EC_INVALID_COL_GRP,/** �Ƿ��������鶨�� */
	NTSE_EC_NO_DICTIONARY,  /** ѹ��ȫ���ֵ䲻���� */
	NTSE_EC_NONECONNECTION, /** ָ�������Ӳ����� */
	NTSE_EC_DEADLOCK,		/** TNT���񣬼�⵽����*/
	NTSE_EC_IN_DUMPING,		/** TNT��ǰ���ڽ���dump */
	NTSE_EC_IN_PURGING,		/** TNT��ǰ���ڽ���purge */	
	NTSE_EC_IN_DEFRAGING,	/** TNT��ǰ���ڽ���defrag */
	NTSE_EC_ONLINE_DDL,     /** TNT���ڽ���online ddl����*/
	NTSE_EC_DUMP_FAILED,	/** TNT dump ʧ�� */
	NTSE_EC_TOO_MANY_TRX,   /** TNT��ǰ��Ծ���񳬹����� */
	NTSE_EC_OPEN_SYS_TBL,   /** tnt���ܴ�ϵͳ��*/
	NTSE_EC_TRX_ABORT,      /** tnt������Ҫ���ж� */
	NTSE_EC_TOO_MANY_INDEX, /** ���������������� */
	NTSE_EC_TOO_MANY_COLUMN /** ������������ */
};

/** NTSE�洢�����ڲ����ֵ��κ��쳣 */
class NtseException {
public:
	NtseException(const char *file, uint line);
	NtseException(const NtseException &copy);
	virtual ~NtseException();
	NtseException& operator() (ErrorCode errorCode, const char *fmt, ...);
	NtseException& operator() (u64 fileCode, const char *fmt, ...);
	ErrorCode getErrorCode();
	const char* getMessage();	
	const char* getFile();
	uint getLine();

private:
	static ErrorCode getFileExcptCode(u64 code);
	
	const char	*m_file;		/** �׳��쳣�Ĵ���λ������Դ�ļ� */
	uint		m_line;			/** �׳��쳣�Ĵ����к� */
	ErrorCode	m_errorCode;	/** ����� */
	char		*m_msg;			/** �쳣��Ϣ */
};

/** ʹ�����涨��ĺ����׳��쳣����Ҫֱ��ʹ��NtseException�� */
#define NTSE_THROW	throw (NtseException(__FILE__, __LINE__))

/** ҳ��С��ʹ��������Ŀ���ǿ���ͨ�����������ָ��ҳ��С��
 * NTSE�Ĵ����в�Ӧʹ������꣬��Ӧʹ��Limits::PAGE_SIZE
 */
#ifndef NTSE_PAGE_SIZE
#define NTSE_PAGE_SIZE	8192
#endif

/** ���༫�޺ͳ������� */
class Limits {
public:
	/** ÿ���ļ�������ҳ�� */
	static const u64	MAX_PAGE = ((u64)1) << PAGE_BITS;
	/** ÿ��ҳ�����洢�ļ�¼�� */
	static const u32	MAX_SLOT = 1 << SLOT_BITS;
	/** ҳ��С */
	static const u32	PAGE_SIZE = NTSE_PAGE_SIZE;
	/** һ��������ж��ٸ����� */
	static const u16	MAX_INDEX_NUM = 16;
	/** ����ʱ�����������¼��С */
	static const uint	DEF_MAX_REC_SIZE = 65535;
	/** ����䳤�ֶ�תlob�洢�Ĵ�С�ٽ�ֵ */
	static const uint	DEF_MAX_VAR_SIZE = 1024;
	/** ʵ�ʵ�����¼��С(���������Ҫ�����ڱ䳤�ѵ�ʵ�֣�����22Ϊ�䳤��ҳͷ��
	 * 6�Ǳ䳤�Ѽ�¼����ͷ��4�Ǽ�¼ͷ) */
	static const uint	MAX_REC_SIZE = (uint)(Limits::PAGE_SIZE * 0.7 - 22 - 6 - 4);
	/** һ�ű����������� */
	static const u16	MAX_COL_NUM = 256;
	/** һ�����������ٸ����� */
	static const u16	MAX_INDEX_KEYS = 15;
	/** B+������������ */
	static const u32	MAX_BTREE_LEVEL = 10;
	/** �����������Ե����ݿ����������󳤶� */
	static const u16	MAX_NAME_LEN = 64;
	/** lsn����󳤶�*/
	static const u16	LSN_BUFFER_SIZE = 30;
	/** ��Ԥ�ƶ����һ�οռ�*/
	static const uint   MAX_FREE_MALLOC = 128;

	/** �ļ�·����󳤶� */
	static const uint	MAX_PATH_LEN = 1024;
	static const char	*NAME_IDX_EXT;
	static const u32	NAME_IDX_EXT_LEN;
	static const char	*NAME_TMP_IDX_EXT;
	static const char	*NAME_HEAP_EXT;
	static const u32	NAME_HEAP_EXT_LEN;
	static const char   *NAME_TBLDEF_EXT;
	static const u32    NAME_TBLDEF_EXT_LEN;
	static const char	*NAME_SOBH_EXT;
	static const u32	NAME_SOBH_EXT_LEN;
	static const char   *NAME_SOBH_TBLDEF_EXT;
	static const u32    NAME_SOBH_TBLDEF_EXT_LEN;
	static const char	*NAME_LOBI_EXT;
	static const u32	NAME_LOBI_EXT_LEN;
	static const char	*NAME_LOBD_EXT;
	static const u32	NAME_LOBD_EXT_LEN;
	static const char   *NAME_GLBL_DIC_EXT;
	static const u32    NAME_GLBL_DIC_EXT_LEN;
	static const char	*NAME_CTRL_FILE;
	static const char	*NAME_TNT_CTRL_FILE;
	static const char	*NAME_CTRL_SWAP_FILE_EXT;
	static const char	*NAME_TXNLOG;
	//static const char	*NAME_TNT_TXNLOG;
	static const char	*NAME_SYSLOG;
	static const char	*NAME_TNT_SYSLOG;
	static const char	*TEMP_FILE_PREFIX;
	static const char	*NAME_TEMP_TABLE;
	static const char   *NAME_TEMP_GLBL_DIC_EXT;
	static const char   *NAME_DUMP_EXT;
	static const char   *NAME_CONFIG_BACKUP;
	static const char   *NAME_TNTCONFIG_BACKUP;
	/** �����ļ���չ�� */
	static const char *	EXTS[];
	static const int EXTNUM;		/** �д����������ļ����� */
	static const int EXTNUM_NOLOB;	/** �޴����������ļ����� */
};

/** ���κ�����¶���Ч��assert */
#define NTSE_ASSERT(expr) do {																\
	if (!(expr)) {																			\
		fprintf(stderr, "Ntse: assertion failed in file %s line %d\n", __FILE__, __LINE__);	\
		fprintf(stderr, "Ntse: failing assertion: %s\n", #expr);							\
		*((char *)0) = 0;																	\
	}																						\
} while(0)
	
///////////////////////////////////////////////////////////////////////////////
// NTSE������� //////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////////
/**
NTSE_UNIT_TEST: ����������ʾ���ڵ�Ԫ����ģʽ
NTSE_MEM_CHECK: ʹ��Debugģʽ���ڴ����������
NTSE_HEAP_INC_PAGES: ��һ����չ��ҳ��
NTSE_PAGE_SIZE: ҳ��С
NTSE_TRACE: ����������ʾ����TRACE��������Ϊ��¼������
NTSE_VERIFY_EX: ����������ʾ������չ������һ���Լ�鹦��
NTSE_PROFILE: ����������ʾ���ú�����������������֧��
NTSE_SYNC_DEBUG: ����������ʾ����ͬ�����ĵ��Թ���
*/

#ifdef NTSE_UNIT_TEST
/** �����ﶨ�����е�ͬ���� */
enum SyncPoint {
	SP_NONE = 0,		/** �������һ��ͬ���� */

	SP_BGTASK_GET_SESSION_START,	/** BgTask::run��������Ự֮ǰ */		
	SP_BGTASK_GET_SESSION_END,		/** BgTask::run��������Ự֮�� */

	SP_BUF_BATCH_WRITE_LOCK_FAIL,

	SP_HEAP_AFTER_GET_HEADER_PAGE,

	SP_HEAP_FLR_FINDFREEPAGE_1ST_LOCK_HEADER_PAGE,
	SP_HEAP_FLR_FINDFREEPAGE_BEFORE_GET_HEAPDER_PAGE,
	SP_HEAP_FLR_FINDFREEPAGE_JUST_RELEASE_HEADER_PAGE,
	SP_HEAP_FLR_BEFORE_EXTEND_HEAP,
	SP_HEAP_FLR_GET_GOT_PAGE,
	SP_HEAP_FLR_FINISH_INSERT,
	SP_HEAP_FLR_INSERT_BEFORE_REVERSE_LOCK_HEADER_PAGE,
	SP_HEAP_FLR_INSERT_BEFORE_LOCK_A_USEFULL_PAGE,
	SP_HEAP_FLR_FINDFREEPAGE_WANT_TO_EXTEND,
	SP_HEAP_FLR_INSERT_BEFORE_TRY_LOCK_ROW,
	SP_HEAP_FLR_AFTER_TRYLOCK_UNLOCKPAGE,
	SP_HEAP_FLR_BEFORE_UNLOCKROW,
	SP_HEAP_FLR_INSERT_TRY_LOCK_ROW_FAILED,
	SP_HEAP_FLR_INSERT_UNLOCK_PAGE_AFTER_TRY_LOCK_ROW_FAILED,
	SP_HEAP_FLR_INSERT_No_Free_Slot_After_Relock,

	SP_HEAP_VLR_FINDFREEPAGE_BEFORE_EXTEND_HEAP,
	SP_HEAP_VLR_LOCKSECONDPAGE_AFTER_RELEASE_FIRST,
	SP_HEAP_VLR_LOCKSECONDPAGE_BEFORE_TRYLOCK,
	SP_HEAP_VLR_LOCKTHIRDPAGE_AFTER_RELEASE,
	SP_HEAP_VLR_DEL_LOCKED_THE_PAGE,
	SP_HEAP_VLR_LOCK3RDPAGE_BEFORE_TRYLOCK,
	SP_HEAP_VLR_GETNEXT_BEFORE_LOCKROW,
	SP_HEAP_VLR_GETNEXT_AFTER_LOCKROW,
	SP_HEAP_VLR_GET_AFTER_GET_PAGE,
	SP_HEAP_VLR_UPDATEBITMAP_BEFORE_LOCK_HEADER_PAGE,
	SP_HEAP_VLR_GETNEXT_UNLOCKPAGE_TO_GET_ROWLOCK,
	SP_HEAP_VLR_GETNEXT_BEFORE_LOCK_SECOND_PAGE,
	SP_HEAP_VLR_UPDATESAMEBMP_BEFORE_LOCK_HEADER_PAGE,
	SP_HEAP_VLR_FINDFREEINBMP_BEFORE_REWINDSEARCH,
	SP_HEAP_VLR_INSERT_BEFORE_TRY_LOCK_ROW,
	SP_HEAP_VLR_INSERT_TRY_LOCK_ROW_FAILED,
	SP_HEAP_VLR_INSERT_UNLOCK_PAGE_AFTER_TRY_LOCK_ROW_FAILED,
	SP_HEAP_VLR_INSERT_BEFORE_REFIND_FREE_SLOT,
	SP_HEAP_VLR_INSERT_SLOT_FREE_BUT_NO_SPACE,
	SP_HEAP_VLR_DOGET_AFTER_RELEASE_SOURCE_PAGE,
	SP_HEAP_VLR_GETNEXT_BEFORE_EXTRACTTARGET,
	SP_HEAP_VLR_BEFORE_GET_MIN_NEWPAGE,
	SP_HEAP_VLR_BEFORE_RELOCK_SECOND_PAGE_NEW_SMALLER_THAN_SRC,
	SP_HEAP_VLR_BEFORE_GET_MAX_NEWPAGE,

	SP_MMS_DOTOUCH_LOCK,	/** MmsTable::doTouch����lockͬ����	*/
	SP_MMS_DOTOUCH_UNLOCK,	/** MmsTable::doTouch����unlockͬ���� */
	SP_MMS_RID_LOCKROW,		/** MmsTable::getByRid����lockRowͬ���� */
	SP_MMS_RID_UNLOCKROW,	/** MmsTable::getByRid����unlockRowͬ���� */
	SP_MMS_RID_DISABLEPG,	/** MmsTable::getByRid����disablePageͬ���� */
	SP_MMS_RID_TRYLOCK,		/** MmsTable::getByRid����trylockͬ���� */
	SP_MMS_PK_LOCKROW,		/** MmsTable::getByPrimaryKey����lockRowͬ���� */
	SP_MMS_PK_UNLOCKROW,	/** MmsTable::getByPrimaryKey����unlockRowͬ���� */
	SP_MMS_FL,					/** MmsTable::flushLog����flushLogͬ���� */
	SP_MMS_FL_LOCK_PG,			/** MmsTable::flushLog����lockPageͬ���� */
	SP_MMS_FL_UNLOCK_PG,		/** MmsTable::flushLog����unlockPageͬ���� */
	SP_MMS_SF_LOCK_PG,		/** MmsTable::sortAndFlush����lockPageͬ���� */
	SP_MMS_SF_UNLOCK_PG,	/** MmsTable::sortAndFlush����unlockPageͬ���� */
	SP_MMS_PUT_DISABLEPG,
	SP_MMS_SUBRECORD_DISABLEPG,
	SP_MMS_GET_DIRTY_REC_1ST,
	SP_MMS_GET_DIRTY_REC_1ST_END,
	SP_MMS_GET_DIRTY_REC_2ND,
	SP_MMS_GET_DIRTY_REC_2ND_END,
	SP_MMS_GET_PIN_WHEN_REPLACEMENT,
	SP_MMS_GET_PIN_WHEN_REPLACEMENT_END,
	SP_MMS_NULL_VICTIM_WHEN_REPLACEMENT,
	SP_MMS_RANDOM_REPLACE,
	SP_MMS_RANDOM_REPLACE_1ST_GET_TASK,
	SP_MMS_RANDOM_REPLACE_1ST_EXEC_TASK,
	SP_MMS_RANDOM_REPLACE_2ND_GET_TASK,
	SP_MMS_RANDOM_REPLACE_2ND_EXEC_TASK,
	SP_MMS_RANDOM_REPLACE_3RD_GET_TASK,
	SP_MMS_RANDOM_REPLACE_3RD_EXEC_TASK,
	SP_MMS_RANDOM_REPLACE_4TH_GET_TASK,
	SP_MMS_RANDOM_REPLACE_4TH_EXEC_TASK,
	SP_MMS_RANDOM_REPLACE_5TH_GET_TASK,
	SP_MMS_RANDOM_REPLACE_5TH_EXEC_TASK,
	// SP_MMS_AMP_SESSION,
	SP_MMS_AMP_GET_TABLE,
	SP_MMS_AMP_GET_TABLE_END,
	SP_MMS_AMP_GET_PAGE,
	SP_MMS_AMP_GET_PAGE_END,
	SP_MMS_AMP_PIN_PAGE,
	SP_MMS_AMP_PIN_PAGE_END,
	SP_MMS_ALLOC_PAGE,
	SP_MMS_ALLOC_PAGE_END,
	SP_MMS_AMR_LOCK_TBL,
	SP_MMS_AMR_LOCK_TBL_END,
	SP_MMS_AMR_LOCK_PG,
	SP_MMS_AMR_LOCK_PG_END,

	SP_LOB_BIG_INSERT_LOG_REVERSE_1,
	SP_LOB_BIG_INSERT_LOG_REVERSE_2,
	SP_LOB_BIG_READ_DEFRAG_READ,
	SP_LOB_BIG_DEL_DEFRAG_DEL,
	SP_LOB_BIG_DEL_DEFRAG_UPDATE,
	SP_LOB_BIG_READ_DEFRAG_DEFRAG,
	SP_LOB_BIG_GET_FREE_PAGE,
	SP_LOB_BIG_GET_FREE_PAGE_FINISH,
	SP_LOB_BIG_NO_FREE_PAGE,
	SP_LOB_BIG_NOT_FIRST_FREE_PAGE,
	SP_LOB_BIG_OTHER_PUT_FREE_SLOT,

	SP_IDX_CHECK_BEFORE_DELETE,
	SP_IDX_FINISH_A_DELETE,
	SP_IDX_WANT_TO_GET_PAGE1,
	SP_IDX_WANT_TO_GET_PAGE2,
	SP_IDX_WANT_TO_GET_PAGE3,
	SP_IDX_WANT_TO_GET_PAGE4,
	SP_IDX_BEFORE_CLEAR_SMO_BIT,
	SP_IDX_WAIT_FOR_SMO_BIT,
	SP_IDX_WAIT_FOR_INSERT,
	SP_IDX_WAIT_TO_LOCK,
	SP_IDX_ALLOCED_ROOT_PAGE,
	SP_IDX_WAIT_FOR_GET_NEXT,
	SP_IDX_WAIT_FOR_PAGE_LOCK,
	SP_IDX_RESEARCH_PARENT_IN_SMO,
	SP_IDX_TO_LOCK_SMO,
	SP_IDX_WAIT_TO_FIND_SPECIAL_LEVEL_PAGE,

	SP_ILT_AFTER_WAKEUP,

	/** Database */
	SP_DB_CREATE_TABLE_AFTER_LOG,	/** Database::createTableд��־֮�󣬸��¿����ļ�֮ǰ */
	SP_DB_BEFORE_BACKUP_LOG,		/** ������־֮ǰ, TODO: Rename it */
	SP_DB_BEFORE_BACKUP_CTRLFILE,	/** ���ݿ����ļ�֮ǰ, TODO: Rename it */
	SP_DB_BACKUP_BEFORE_HEAP,		/** ���ݶ��ļ�֮ǰ */
	SP_DB_BACKUPFILE_AFTER_GETSIZE,	/** �����ļ���ȡ�ļ��ų���֮�󣬿����ļ�����֮ǰ */
	
	SP_TBL_TRUNCATE_AFTER_DROP,		/** Table::truncate�Ѿ�drop�ٻ�û��create */
	SP_TBL_REPLACE_AFTER_DUP,		/** Table::insertForUpdate������insertʧ��֮�� */
	SP_REC_BF_AFTER_SEARCH_MMS,		/** BulkFetch::getNext�����е�һ������MMS֮�� */
	
	SP_TBL_MASSINSERTBLOG_BEFORE_LOCKMETA,
	SP_TBL_MASSINSERTBLOG_BEFORE_LOCK,
	SP_TBL_MASSINSERTBLOG_BEFORE_UNLOCK,
	SP_TBL_MASSINSERTBLOG_BEFORE_UNLOCKMETA,
	SP_TBL_MASSINSERTBLOG_AFTER_UNLOCKMETA,

	SP_TBL_MASSINSERTBLOGCOUNT_BEFORE_LOCKMETA,
	SP_TBL_MASSINSERTBLOGCOUNT_BEFORE_LOCK,
	SP_TBL_MASSINSERTBLOGCOUNT_AFTER_UNLOCKMETA,
	
	SP_TBL_SCANDELETE_BEFORE_LOCKMETA,
	SP_TBL_SCANDELETE_BEFORE_LOCK,
	SP_TBL_SCANDELETE_BEFORE_UNLOCK,
	SP_TBL_SCANDELETE_BEFORE_UNLOCKMETA,
	SP_TBL_SCANDELETE_AFTER_UNLOCKMETA,
	SP_TBL_SCANDELETE_AFTER_DELETEROW,

	SP_TBL_SCANUPDATE_BEFORE_LOCKMETA,
	SP_TBL_SCANUPDATE_BEFORE_LOCK,
	SP_TBL_SCANUPDATE_BEFORE_UNLOCK,
	SP_TBL_SCANUPDATE_BEFORE_UNLOCKMETA,
	SP_TBL_SCANUPDATE_AFTER_UNLOCKMETA,
	SP_TBL_SCANUPDATE_AFTER_UPDATEROW,

	SP_TBL_ALTIDX_AFTER_U_METALOCK,
	SP_TBL_ALTIDX_BEFORE_GET_LSNSTART,
	SP_TBL_ALTIDX_AFTER_GET_LSNSTART,
	SP_TBL_ALTIDX_BEFORE_GET_LSNEND,
	SP_MNT_ALTERINDICE_BEFORE_ADDIND_ONLINE_INDICE,
	SP_MNT_ALTERINDICE_ADDIND_ONLINE_INDICE_UPDMETA_FAIL,
	SP_MNT_ALTERINDICE_BEFORE_UPD_METALOCK,
	SP_MNT_ALTERINDICE_UPD_METALOCK_FAIL,

	SP_TBL_ALTCOL_AFTER_U_METALOCK,
	SP_TBL_ALTCOL_BEFORE_GET_LSNSTART,
	SP_TBL_ALTCOL_AFTER_GET_LSNSTART,
	SP_TBL_ALTCOL_BEFORE_GET_LSNEND,

	SP_TBL_UPDATE_AFTER_STARTTXN_LOG,

	/*** �������ݿ�ά�� ***/
	SP_MNT_ALTERINDICE_JUST_WRITE_LOG,
	SP_MNT_ALTERINDICE_JUST_WRITE_TABLEDEF,
	SP_MNT_ALTERINDICE_INDICE_REPLACED,
	SP_MNT_ALTERINDICE_FINISH,

	SP_MNT_ALTERCOLUMN_JUST_WRITE_LOG,
	SP_MNT_ALTERCOLUMN_TABLE_REPLACED,
	SP_MNT_ALTERCOLUMN_FINISH,
	SP_MNT_ALTERCOLUMN_TMPTABLE_CREATED,
	SP_MNT_ALTERCOLUMN_BEFORE_SCAN_IS_LOCK,
	SP_MNT_ALTERCOLUMN_SCAN_IS_LOCK_FAIL,
	SP_MNT_ALTERCOLUMN_BEFORE_BEGINSCAN,
	SP_MNT_ALTERCOLUMN_BEFORE_BEGINSCAN_FAIL,
	SP_MNT_ALTERCOLUMN_BEFORE_S_LOCKTABLE,
	SP_MNT_ALTERCOLUMN_S_LOCKTABLE_FAIL,
	SP_MNT_ALTERCOLUMN_BEFORE_X_METALOCK,
	SP_MNT_ALTERCOLUMN_X_METALOCK_FAIL,

	SP_MRECS_GETRECORD_PTR_MODIFY,   //�ڿ��ն�ͬʱ����¼��ɾ������������
	SP_MRECS_PREPAREUD_PTR_MODIFY,   //��¼��prepareUDʱ����¼������
	SP_MRECS_UPDATE_PTR_MODIFY,      //��¼updateʱ��ͬʱ�ü�¼������
	SP_MRECS_UPDATEMEM_PTR_MODIFY,   //��¼�ڸ����ڴ�ʱ���ü�¼�ѱ������ƶ�����
	SP_MRECS_REMOVE_PTR_MODIFY_BEFORE_VERSIONPOOL,      //��¼��ɾ��ʱ���ڲ���汾����ǰͬʱ�ü�¼������
	SP_MRECS_REMOVE_PTR_MODIFY_BEFORE_MHEAP,      //��¼��ɾ��ʱ���ڸ����ڴ����ǰͬʱ�ü�¼������
	SP_MRECS_DUMP_AFTER_DUMPREALWORK,            //dump�У���dumpRealWork���������ȴ�
	SP_MRECS_ROLLBACK_BEFORE_GETHEAPREC,          //rollback�����У���getHeapRedRecǰ�ȴ�

	SP_MHEAP_DUMPREALWORK_REC_MODIFY, //��dump�����У���һ��ҳ�ļ�¼�������Ӹ��±��ƶ�����dump��ҳ��ȥ��
	SP_MHEAP_DUMPREALWORK_BEGIN,
	SP_MHEAP_PURGENEXT_REC_MODIFY,    //��purge�����У���һҳ�л�δ��purge�ļ�¼��������
	SP_MHEAP_ALLOC_PAGE_RACE,         //��alloc�����У���Ҫʹ�õ�ҳ������߳�����ʹ��
	SP_MHEAP_DEFRAG_WAIT,
	SP_MHEAP_DUMPCOMPENSATE_REC_MODIFY,//������������ʱ����¼������������
	SP_MHEAP_GETSLOTANDLATCHPAGE_REC_MODIFY,//�ڻ�ȡ��¼���ڵ�ҳ��ʱ����¼�������ƶ�
	SP_MHEAP_UPDATEHEAPRECORDANDHASH_REC_MODIFY, //��UPDATE��¼���ڵ�ҳ��ʱ����¼�������ƶ�
	SP_MHEAP_UPDATE_WAIT_DUMP_FINISH, //update�����еȴ�dump�Ľ���
	SP_MHEAP_PURGE_PHASE2_PICKROW_AFTER, //purge phase2 pick row�Ժ�ժ��hash������ǰ
	SP_MHEAP_BEGIN_PURGE_COPY_PAGE_AFTER, // begin purge ʱ����ҳ�濽������ʱҳ��֮��

	SP_TNTDB_ACQUIREPERMISSION_WAIT,  //��ȡpurge����dump����defragȨ�޺����ȴ�
	SP_TNTDB_PURGEWAITTRX_WAIT,       //��purge�ڶ��׶ο�ʼǰ�ȴ���ǰ��Ծ����Ľ���
	SP_TNTDB_PURGE_PHASE2_BEFORE,    //purge�ڶ��׶�ǰ��ͣ

	SP_LOB_ROLLBACK_DELETE,			//������ع���ʱ������ID������
	SP_LOB_PURGE_DELETE,			//��purge��ʱ������ID������
	SP_LOB_RECLAIM_DELETE,			//��Reclaim��ʱ������ID������

	/*** �����ڴ����� ***/
	SP_MEM_INDEX_TRYLOCK,			//�ڴ��������Լ���ʱ����ͻ�ж�ʱ��
	SP_MEM_INDEX_REPAIRNOTRIGMOST,	//repairNotRightMost����
	SP_MEM_INDEX_REPAIRNOTRIGMOST1,
	SP_MEM_INDEX_SHIFTBACKSCAN,		//����ɨ�賢�Լ���ʱʹ��
	SP_MEM_INDEX_SHIFTBACKSCAN1,
	SP_MEM_INDEX_SHIFTBACKSCAN2,
	SP_MEM_INDEX_SHIFTBACKSCAN3,
	SP_MEM_INDEX_UPGREADLATCH,		//����latchʱʹ��
	SP_MEM_INDEX_UPGREADLATCH1,
	SP_MEM_INDEX_UPGREADLATCH2,

	SP_MEM_INDEX_CHECKPAGE,			//����checkHandlePageʱʹ��
	SP_MEM_INDEX_CHECKPAGE1,

	SP_MEM_INDEX_SHIFT_FORWARD_KEY,	//����ǰ��ȡ��ֵʱʹ��
	SP_MEM_INDEX_SHIFT_BACKWARD_KEY,

	/*** ����TNT���� ***/
	SP_TNT_INDEX_LOCK,
	SP_TNT_INDEX_LOCK1,
	SP_TNT_INDEX_LOCK2,

	SP_TNT_TABLE_PURGE_PHASE2,
	SP_TNT_TABLE_PURGE_PHASE3,

	/** ��Դ�ز��� */
	SP_RESOURCE_POOL_AFTER_FATCH_DONE,

	/** ��TNT������ */
	SP_DLD_LOCK_LOCK_FIRST,
	SP_DLD_LOCK_ENQUEUE_BEFORE_WAIT,

	/** TNTTable getNext������¼������ͻ����*/
	SP_ROW_LOCK_BEFORE_LOCK,
	SP_ROW_LOCK_AFTER_TRYLOCK,

	/** TNTDatabase OpenTable������Truncate����ͬ����*/
	SP_DB_TRUNCATE_CURRENT_OPEN,

	SP_MAX			/** ���Ҳ����ͬ���㣬ֻ������ʾͬ�����ж��ٸ� */
};

/** �ȴ�ͬ����
 * ����Ӧ��ͬ������Ч��ͨ������Thread::enableSyncPoint����ȴ���һ���߳�
 * ����Thread::notifySyncPoint֮��ż���ִ�С�����������²��ȴ���
 */
#define SYNCHERE(syncPoint)                             \
	do {                                                \
		Thread *current = Thread::currentThread();      \
		if (current) current->waitSyncPoint(syncPoint); \
	} while (0)
#else
#define SYNCHERE(syncPoint)
#endif

/**
 * �������͡�ʹ��64λ��FVN��ϣ�㷨
 *
 * @param buf ����
 * @param size ���ݴ�С
 * @return �����
 */
inline u64 checksum64(const byte *buf, size_t size) {
	u64 hash = (u64)14695981039346656037ULL;
	for (size_t i = 0; i < size; i++) {
		hash = hash * 1099511628211LL;
		hash = hash ^ buf[i];
	}
	return hash;
}

#ifndef MIN
#define MIN(x,y) (((x)<(y))?(x):(y))
#endif

#ifndef MAX
#define MAX(x,y) (((x)>(y))?(x):(y))
#endif

#if !defined(__GNUC__) || (__GNUC__ == 2 && __GNUC_MINOR__ < 96)
#define __builtin_expect(x, expected_value) (x)
#endif

#define likely(x)	__builtin_expect((x),1)
#define unlikely(x)	__builtin_expect((x),0)

}

#pragma warning(disable: 4290)	// �쳣�淶������

#endif

