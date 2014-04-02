/**
 * NTSE存储引擎中最常用的定义
 *
 * @author 汪源(wangyuan@corp.netease.com, wy@163.org)
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

/** 记录ID，包含35位的页号和13位的记录槽号，最多32G个数据页，每页最多8192条记录，共占用6字节 */
typedef u64	RowId;

/** 索引页面ID */
typedef u64 PageId;

/** 大对象ID */
typedef u64 LobId;

/** 表ID */
typedef u16 TableId;

#ifdef TNT_ENGINE
typedef u64 TrxId;  /** TNT事务ID */
#define INVALID_TRX_ID ((TrxId)-1) /** 无效的TNT事务ID */

typedef u16 RowIdVersion; /** RowId版本号，用于验证RowId是否发生重用 */
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

#define INVALID_ROW_ID	((RowId)(-1))		/** 无效的记录ID */
#define INVALID_PAGE_ID	((PageId)(-1))		/** 无效的页面ID */
#define INVALID_LSN		((u64)(-1))			/** 无效的LSN号 */
#define INVALID_TOKEN   ((int)(-1))         /** 无效的token */

#define PAGE_BITS	35		/** 页号占用位数 */
#define SLOT_BITS	13		/** 记录槽号占用位数 */
#define RID_BYTES	6		/** 序列化一个RID占用的字节数 */
#define RID_BITS	48		/** RID占用位数 */

/** 根据RID得到页号，页号为u64类型，但实际上只能表示2^35个页 */
#define RID_GET_PAGE(rid)	((u64)(((rid) >> SLOT_BITS) & ((((u64)1) << PAGE_BITS) - 1)))
/** 根据RID得到记录槽号，记录槽号为u16类型，但实际上只能表示2^13个记录 */
#define RID_GET_SLOT(rid)	((u16)((rid) & (u64)0x1FFF))
/** 根据页号和记录槽号得到RID */
#define RID(page, slot)		((RowId)(((page) << SLOT_BITS) | (u64)slot))

/** 对齐操作相关宏定义*/
#define MY_ALIGN(A,L)	(((A) + (L) - 1) & ~((L) - 1))
#define ALIGN_SIZE(A)	MY_ALIGN((A),sizeof(double))

/** 读取一个RID，RID占用RID_BYTES个字节 */
inline RowId RID_READ(byte *buf) {
	u64 v = (u64)(*(u32 *)buf);
	v = v << 16;
	v += *(u16 *)(buf + 4);
	return v;
}

/** 写入一个RID，RID占用RID_BYTES个字节 */
inline void RID_WRITE(RowId rid, byte *buf) {
	*(u32 *)buf = (u32)((rid >> 16) & 0xFFFFFFFF);
	*(u16 *)(buf + 4) = (u16)(rid & 0xFFFF);
}

/**
 * 为ROWID类型提供计算哈希值功能的函数对象
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

	class RidShift { //内部类，用于计算位移数组
	public:
		RidShift() {
			// 计算位移表
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

		u8	*m_size2shift;			/** 位移表(用于RidMap) */ 
	};

	static RidShift m_ridShift;
};

/** NTSE所用错误号 */
enum ErrorCode {
	NTSE_EC_GENERIC,		/** 其它错误 */
	NTSE_EC_OUT_OF_MEM,		/** 内存不足 */
	NTSE_EC_FILE_NOT_EXIST,	/** 文件不存在 */
	NTSE_EC_FILE_PERM_ERROR,/** 文件没权限操作 */
	NTSE_EC_DISK_FULL,		/** 磁盘已满 */
	NTSE_EC_FILE_EXIST,		/** 文件已经存在 */
	NTSE_EC_FILE_IN_USE,	/** 文件在使用中 */
	NTSE_EC_FILE_EOF,		/** 读取或写入操作时指定的偏移量超出文件大小 */
	NTSE_EC_READ_FAIL,		/** 读数据出错 */
	NTSE_EC_WRITE_FAIL,		/** 写数据出错 */
	NTSE_EC_FILE_FAIL,		/** 其它文件操作出错 */
    NTSE_EC_ACCESS_OUT_OF_PAGE, /** 读写操作超出了页边界 */
	NTSE_EC_PAGE_DAMAGE,	/** 页面数据被破坏 */
	NTSE_EC_FORMAT_ERROR,/** 控制文件被破坏 */
	NTSE_EC_OVERFLOW,		/** 越界错误 */
	NTSE_EC_INDEX_BROKEN,	/** 索引结构错误 */
	NTSE_EC_INDEX_UNQIUE_VIOLATION,	/** 索引违反唯一性约束 */
	NTSE_EC_NOT_LOCKED,			/** 试图修改未锁定的资源 */
	NTSE_EC_TOO_MANY_ROWLOCK,	/** 行锁数目超出锁表大小 */
	NTSE_EC_TOO_MANY_SESSION,	/** 所有会话都在使用中 */
	NTSE_EC_NOT_SUPPORT,		/** NTSE存储引擎不支持的数据类型、特性、操作等 */
	NTSE_EC_EXCEED_LIMIT,		/** 表ID等各类对象个数、大小等超出限制 */
	NTSE_EC_CORRUPTED_LOGFILE,  /** 日志文件错误 */
	NTSE_EC_MISSING_LOGFILE,	/** 日志文件不存在 */
	NTSE_EC_DUP_TABLEID,	/** 表ID重复 */
	NTSE_EC_INVALID_BACKUP,	/** 无效的备份文件或者目录 */
	NTSE_EC_LOCK_TIMEOUT,	/** 加锁超时 */
	NTSE_EC_LOCK_FAIL,		/** 加锁失败 */
	NTSE_EC_SYNTAX_ERROR,	/** 语法错误 */
	NTSE_EC_ROW_TOO_LONG,	/** 记录超长 */
	NTSE_EC_NONEINDEX,		/** 没有需要操作的索引列 */
	NTSE_EC_DUPINDEX,		/** 重复的索引 */
	NTSE_EC_COLDEF_ERROR,	/** 列定义错误 */
	NTSE_EC_CANCELED,		/** 操作被取消 */
	NTSE_EC_CORRUPTED,		/** 数据损坏 */
	NTSE_EC_TABLEDEF_ERROR, /** tabledef的定义文件不符合格式*/
	NTSE_EC_INVALID_COL_GRP,/** 非法的属性组定义 */
	NTSE_EC_NO_DICTIONARY,  /** 压缩全局字典不存在 */
	NTSE_EC_NONECONNECTION, /** 指定的连接不存在 */
	NTSE_EC_DEADLOCK,		/** TNT事务，检测到死锁*/
	NTSE_EC_IN_DUMPING,		/** TNT当前正在进行dump */
	NTSE_EC_IN_PURGING,		/** TNT当前正在进行purge */	
	NTSE_EC_IN_DEFRAGING,	/** TNT当前正在进行defrag */
	NTSE_EC_ONLINE_DDL,     /** TNT正在进行online ddl操作*/
	NTSE_EC_DUMP_FAILED,	/** TNT dump 失败 */
	NTSE_EC_TOO_MANY_TRX,   /** TNT当前活跃事务超过限制 */
	NTSE_EC_OPEN_SYS_TBL,   /** tnt不能打开系统表*/
	NTSE_EC_TRX_ABORT,      /** tnt事务需要被中断 */
	NTSE_EC_TOO_MANY_INDEX, /** 索引个数超过限制 */
	NTSE_EC_TOO_MANY_COLUMN /** 列数超过限制 */
};

/** NTSE存储引擎内部出现的任何异常 */
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
	
	const char	*m_file;		/** 抛出异常的代码位置所在源文件 */
	uint		m_line;			/** 抛出异常的代码行号 */
	ErrorCode	m_errorCode;	/** 错误号 */
	char		*m_msg;			/** 异常信息 */
};

/** 使用下面定义的宏来抛出异常，不要直接使用NtseException类 */
#define NTSE_THROW	throw (NtseException(__FILE__, __LINE__))

/** 页大小，使用这个宏的目的是可以通过编译参数来指定页大小。
 * NTSE的代码中不应使用这个宏，而应使用Limits::PAGE_SIZE
 */
#ifndef NTSE_PAGE_SIZE
#define NTSE_PAGE_SIZE	8192
#endif

/** 各类极限和常数定义 */
class Limits {
public:
	/** 每个文件中最大的页数 */
	static const u64	MAX_PAGE = ((u64)1) << PAGE_BITS;
	/** 每个页中最多存储的记录数 */
	static const u32	MAX_SLOT = 1 << SLOT_BITS;
	/** 页大小 */
	static const u32	PAGE_SIZE = NTSE_PAGE_SIZE;
	/** 一个表最多有多少个索引 */
	static const u16	MAX_INDEX_NUM = 16;
	/** 定义时所允许的最大记录大小 */
	static const uint	DEF_MAX_REC_SIZE = 65535;
	/** 定义变长字段转lob存储的大小临界值 */
	static const uint	DEF_MAX_VAR_SIZE = 1024;
	/** 实际的最大记录大小(这个限制主要来自于变长堆的实现，其中22为变长堆页头，
	 * 6是变长堆记录链接头，4是记录头) */
	static const uint	MAX_REC_SIZE = (uint)(Limits::PAGE_SIZE * 0.7 - 22 - 6 - 4);
	/** 一张表最多多少属性 */
	static const u16	MAX_COL_NUM = 256;
	/** 一个索引最多多少个属性 */
	static const u16	MAX_INDEX_KEYS = 15;
	/** B+树索引最大层数 */
	static const u32	MAX_BTREE_LEVEL = 10;
	/** 表、索引、属性等数据库对象名称最大长度 */
	static const u16	MAX_NAME_LEN = 64;
	/** lsn的最大长度*/
	static const u16	LSN_BUFFER_SIZE = 30;
	/** 比预计多分配一段空间*/
	static const uint   MAX_FREE_MALLOC = 128;

	/** 文件路径最大长度 */
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
	/** 数据文件扩展名 */
	static const char *	EXTS[];
	static const int EXTNUM;		/** 有大对象的数据文件个数 */
	static const int EXTNUM_NOLOB;	/** 无大对象的数据文件个数 */
};

/** 在任何情况下都生效的assert */
#define NTSE_ASSERT(expr) do {																\
	if (!(expr)) {																			\
		fprintf(stderr, "Ntse: assertion failed in file %s line %d\n", __FILE__, __LINE__);	\
		fprintf(stderr, "Ntse: failing assertion: %s\n", #expr);							\
		*((char *)0) = 0;																	\
	}																						\
} while(0)
	
///////////////////////////////////////////////////////////////////////////////
// NTSE编译参数 //////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////////
/**
NTSE_UNIT_TEST: 定义这个宏表示是在单元测试模式
NTSE_MEM_CHECK: 使用Debug模式的内存分配上下文
NTSE_HEAP_INC_PAGES: 堆一次扩展的页数
NTSE_PAGE_SIZE: 页大小
NTSE_TRACE: 定义这个宏表示启用TRACE（程序行为记录）功能
NTSE_VERIFY_EX: 定义这个宏表示启用扩展的数据一致性检查功能
NTSE_PROFILE: 定义这个宏表示启用函数级性能剖析功能支持
NTSE_SYNC_DEBUG: 定义这个宏表示启用同步锁的调试功能
*/

#ifdef NTSE_UNIT_TEST
/** 在这里定义所有的同步点 */
enum SyncPoint {
	SP_NONE = 0,		/** 这个不是一个同步点 */

	SP_BGTASK_GET_SESSION_START,	/** BgTask::run函数申请会话之前 */		
	SP_BGTASK_GET_SESSION_END,		/** BgTask::run函数申请会话之后 */

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

	SP_MMS_DOTOUCH_LOCK,	/** MmsTable::doTouch函数lock同步点	*/
	SP_MMS_DOTOUCH_UNLOCK,	/** MmsTable::doTouch函数unlock同步点 */
	SP_MMS_RID_LOCKROW,		/** MmsTable::getByRid函数lockRow同步点 */
	SP_MMS_RID_UNLOCKROW,	/** MmsTable::getByRid函数unlockRow同步点 */
	SP_MMS_RID_DISABLEPG,	/** MmsTable::getByRid函数disablePage同步点 */
	SP_MMS_RID_TRYLOCK,		/** MmsTable::getByRid函数trylock同步点 */
	SP_MMS_PK_LOCKROW,		/** MmsTable::getByPrimaryKey函数lockRow同步点 */
	SP_MMS_PK_UNLOCKROW,	/** MmsTable::getByPrimaryKey函数unlockRow同步点 */
	SP_MMS_FL,					/** MmsTable::flushLog函数flushLog同步点 */
	SP_MMS_FL_LOCK_PG,			/** MmsTable::flushLog函数lockPage同步点 */
	SP_MMS_FL_UNLOCK_PG,		/** MmsTable::flushLog函数unlockPage同步点 */
	SP_MMS_SF_LOCK_PG,		/** MmsTable::sortAndFlush函数lockPage同步点 */
	SP_MMS_SF_UNLOCK_PG,	/** MmsTable::sortAndFlush函数unlockPage同步点 */
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
	SP_DB_CREATE_TABLE_AFTER_LOG,	/** Database::createTable写日志之后，更新控制文件之前 */
	SP_DB_BEFORE_BACKUP_LOG,		/** 备份日志之前, TODO: Rename it */
	SP_DB_BEFORE_BACKUP_CTRLFILE,	/** 备份控制文件之前, TODO: Rename it */
	SP_DB_BACKUP_BEFORE_HEAP,		/** 备份堆文件之前 */
	SP_DB_BACKUPFILE_AFTER_GETSIZE,	/** 备份文件获取文件才长度之后，拷贝文件数据之前 */
	
	SP_TBL_TRUNCATE_AFTER_DROP,		/** Table::truncate已经drop再还没有create */
	SP_TBL_REPLACE_AFTER_DUP,		/** Table::insertForUpdate函数中insert失败之后 */
	SP_REC_BF_AFTER_SEARCH_MMS,		/** BulkFetch::getNext函数中第一次搜索MMS之后 */
	
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

	/*** 在线数据库维护 ***/
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

	SP_MRECS_GETRECORD_PTR_MODIFY,   //在快照读同时，记录被删除或增长更新
	SP_MRECS_PREPAREUD_PTR_MODIFY,   //记录在prepareUD时，记录被整理
	SP_MRECS_UPDATE_PTR_MODIFY,      //记录update时，同时该记录被整理
	SP_MRECS_UPDATEMEM_PTR_MODIFY,   //记录在更新内存时，该记录已被整理移动到别处
	SP_MRECS_REMOVE_PTR_MODIFY_BEFORE_VERSIONPOOL,      //记录在删除时，在插入版本池以前同时该记录被整理
	SP_MRECS_REMOVE_PTR_MODIFY_BEFORE_MHEAP,      //记录在删除时，在更新内存堆以前同时该记录被整理
	SP_MRECS_DUMP_AFTER_DUMPREALWORK,            //dump中，在dumpRealWork结束后发生等待
	SP_MRECS_ROLLBACK_BEFORE_GETHEAPREC,          //rollback过程中，在getHeapRedRec前等待

	SP_MHEAP_DUMPREALWORK_REC_MODIFY, //在dump过程中，下一个页的记录发生增加更新被移动到被dump过页上去了
	SP_MHEAP_DUMPREALWORK_BEGIN,
	SP_MHEAP_PURGENEXT_REC_MODIFY,    //在purge过程中，下一页中还未被purge的记录发生更新
	SP_MHEAP_ALLOC_PAGE_RACE,         //在alloc过程中，将要使用的页被别的线程抢先使用
	SP_MHEAP_DEFRAG_WAIT,
	SP_MHEAP_DUMPCOMPENSATE_REC_MODIFY,//在做补偿更新时，记录发生增长更新
	SP_MHEAP_GETSLOTANDLATCHPAGE_REC_MODIFY,//在获取记录所在的页面时，记录发生了移动
	SP_MHEAP_UPDATEHEAPRECORDANDHASH_REC_MODIFY, //在UPDATE记录所在的页面时，记录发生了移动
	SP_MHEAP_UPDATE_WAIT_DUMP_FINISH, //update过程中等待dump的结束
	SP_MHEAP_PURGE_PHASE2_PICKROW_AFTER, //purge phase2 pick row以后，摘除hash索引以前
	SP_MHEAP_BEGIN_PURGE_COPY_PAGE_AFTER, // begin purge 时，将页面拷贝到临时页面之后

	SP_TNTDB_ACQUIREPERMISSION_WAIT,  //获取purge或者dump或者defrag权限后发生等待
	SP_TNTDB_PURGEWAITTRX_WAIT,       //在purge第二阶段开始前等待当前活跃事务的结束
	SP_TNTDB_PURGE_PHASE2_BEFORE,    //purge第二阶段前暂停

	SP_LOB_ROLLBACK_DELETE,			//在事务回滚的时候大对象ID被重用
	SP_LOB_PURGE_DELETE,			//在purge的时候大对象ID被重用
	SP_LOB_RECLAIM_DELETE,			//在Reclaim的时候大对象ID被重用

	/*** 测试内存索引 ***/
	SP_MEM_INDEX_TRYLOCK,			//内存索引尝试加锁时所冲突判断时用
	SP_MEM_INDEX_REPAIRNOTRIGMOST,	//repairNotRightMost触发
	SP_MEM_INDEX_REPAIRNOTRIGMOST1,
	SP_MEM_INDEX_SHIFTBACKSCAN,		//反向扫描尝试加锁时使用
	SP_MEM_INDEX_SHIFTBACKSCAN1,
	SP_MEM_INDEX_SHIFTBACKSCAN2,
	SP_MEM_INDEX_SHIFTBACKSCAN3,
	SP_MEM_INDEX_UPGREADLATCH,		//升级latch时使用
	SP_MEM_INDEX_UPGREADLATCH1,
	SP_MEM_INDEX_UPGREADLATCH2,

	SP_MEM_INDEX_CHECKPAGE,			//测试checkHandlePage时使用
	SP_MEM_INDEX_CHECKPAGE1,

	SP_MEM_INDEX_SHIFT_FORWARD_KEY,	//测试前后取键值时使用
	SP_MEM_INDEX_SHIFT_BACKWARD_KEY,

	/*** 测试TNT索引 ***/
	SP_TNT_INDEX_LOCK,
	SP_TNT_INDEX_LOCK1,
	SP_TNT_INDEX_LOCK2,

	SP_TNT_TABLE_PURGE_PHASE2,
	SP_TNT_TABLE_PURGE_PHASE3,

	/** 资源池测试 */
	SP_RESOURCE_POOL_AFTER_FATCH_DONE,

	/** 带TNT锁机制 */
	SP_DLD_LOCK_LOCK_FIRST,
	SP_DLD_LOCK_ENQUEUE_BEFORE_WAIT,

	/** TNTTable getNext方法记录加锁冲突测试*/
	SP_ROW_LOCK_BEFORE_LOCK,
	SP_ROW_LOCK_AFTER_TRYLOCK,

	/** TNTDatabase OpenTable操作与Truncate并发同步点*/
	SP_DB_TRUNCATE_CURRENT_OPEN,

	SP_MAX			/** 这个也不是同步点，只用来表示同步点有多少个 */
};

/** 等待同步点
 * 若对应的同步点生效（通过调用Thread::enableSyncPoint）则等待另一个线程
 * 调用Thread::notifySyncPoint之后才继续执行。在其它情况下不等待。
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
 * 计算检验和。使用64位的FVN哈希算法
 *
 * @param buf 数据
 * @param size 数据大小
 * @return 检验和
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

#pragma warning(disable: 4290)	// 异常规范被忽略

#endif

