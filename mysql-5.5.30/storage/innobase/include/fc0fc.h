/**************************************************//**
@file fc/fc0fc.c
Flash Cache for InnoDB

Created	24/4/2012 David Jiang (jiangchengyao@gmail.com)
*******************************************************/

#ifndef fc0fc_h
#define fc0fc_h

#include "univ.i"
#include "fil0fil.h"
#include "ha0ha.h"
#include "fc0type.h"
#include "trx0sys.h"
#include "buf0buf.h"

/** flash cache variables */
extern ulint	srv_flash_cache_size;
extern ulint	srv_flash_read_cache_size;
extern char*	srv_flash_cache_file;
extern char*	srv_flash_cache_warmup_table;
extern char*	srv_flash_cache_warmup_file;
extern my_bool	srv_flash_cache_enable_move;
extern my_bool	srv_flash_cache_enable_migrate;
extern my_bool	srv_flash_cache_is_raw;
extern my_bool	srv_flash_cache_adaptive_flushing;
extern ulong	srv_fc_io_capacity;
extern my_bool  srv_flash_cache_enable_write;
extern my_bool  srv_flash_cache_safest_recovery;
extern my_bool  srv_flash_cache_backuping;
extern char*    srv_flash_cache_backup_dir;
extern my_bool  srv_flash_cache_lru_move_batch;
extern ulong  	srv_flash_cache_write_mode;
extern my_bool  srv_flash_cache_fast_shutdown;

/** flash cache status */
extern ulint	srv_flash_cache_read;
extern ulint	srv_flash_cache_write;
extern ulint	srv_flash_cache_flush;
extern ulint	srv_flash_cache_merge_write;
extern ulint	srv_flash_cache_move;
extern ulint	srv_flash_cache_pages_per_read;
extern ulong	srv_flash_cache_write_cache_pct;
extern ulong	srv_flash_cache_do_full_io_pct;
extern ulong	srv_flash_cache_move_limit;
extern ulint	srv_flash_read_cache_page;
extern ulint	srv_flash_cache_read_detail[FIL_PAGE_TYPE_ZBLOB2+1];
extern ulint	srv_flash_cache_write_detail[FIL_PAGE_TYPE_ZBLOB2+1];
extern ulint	srv_flash_cache_flush_detail[FIL_PAGE_TYPE_ZBLOB2+1];
extern ulint	srv_flash_cache_used;
extern ulint	srv_flash_cache_migrate;
extern ulint	srv_flash_cache_wait_for_aio;
extern ulint	srv_flash_cache_aio_read;

extern my_bool	srv_flash_cache_load_from_dump_file;
extern const char srv_flash_cache_log_file_name[16];
extern const char*	srv_flash_cache_thread_op_info;



extern fc_t*	fc;

enum flash_cache_write_mode_enum{
	WRITE_BACK,
	WRITE_THROUGH
};


/* flash cache block status */
enum flash_cache_block_state{
	BLOCK_NOT_USED,	/*!< block not used */
	BLOCK_READY_FOR_FLUSH,	/*!< ready for flush to disk */
	BLOCK_READ_CACHE,	/*!< block migrate or warmup to flash cache */
	BLOCK_FLUSHED,	/*!< block has been flushed */
	BLOCK_NEED_INSERT  /* in fc_move_migrate_to_flash_cache: block should insert to fc->hash_table latter*/
};

/** flash cache block struct */
struct fc_block_struct{
	ulint		space:32;			/*!< tablespace id */
	ulint		offset:32;			/*!< page number */
	ulint		fil_offset:32;		/*!< flash cache page number */
	ulint		state:3;			/*!< flash cache block state */
	fc_block_t* hash;				/*!< hash chain */
	unsigned	is_aio_reading:1; 	/*!< if is in aio reading status */
};

/** flash cache struct */
struct fc_struct{
	mutex_t			mutex; /*!< mutex protecting flash cache */
	hash_table_t*	hash_table; /*!< hash table of flash cache blocks */
	mutex_t			hash_mutex; /* mutex protecting flash cache hash table */
	ulint			size; /*!< flash cache size */
	ulint			write_off; /*!< write to flash cache offset */
	ulint			flush_off; /*!< flush to disk this offset */
	ulint			write_round; /*!< write round */
	ulint			flush_round; /*!< flush round */
	fc_block_t* 	block; /*!< flash cache block */
	byte*			read_buf_unalign; /*!< unalign read buf */
	byte*			read_buf;	/*!< read buf */
	
	/******** used for move & migration optimization */
	fc_block_t*		move_migrate_blocks;  /* protected by mutex and hash_mutex*/	
	byte*			move_migrate_un_aligned_pages; 
	byte*			move_migrate_pages;
	ulint			move_migrate_n_pages;	/* 2 * 64 */
	ulint			move_migrate_next_write_pos;        /* protected by mutex */	
	
};

#define flash_cache_mutex_enter() (mutex_enter(&fc->mutex))
#define flash_cache_mutex_exit()  (mutex_exit(&fc->mutex))
#define flash_cache_hash_mutex_enter(space,offset) (mutex_enter(&fc->hash_mutex))
#define flash_cache_hash_mutex_exit(space,offset) (mutex_exit(&fc->hash_mutex))

/**************************************************************//**
Initialize flash cache struct.*/
UNIV_INTERN
void
fc_create(
/*=========*/
);
/**************************************************************//**
Start flash cache.*/
UNIV_INTERN
void
fc_start(
/*=========*/
);
/**************************************************************//**
Free flash cache struct.*/
UNIV_INTERN
void
fc_destroy(
/*=========*/
);
/**************************************************************//**
Check whether flash cache is enable.*/
UNIV_INTERN
my_bool
fc_is_enabled(
/*=========*/
);
/**************************************************************//**
Get flash cache size.
@return number of flash cache blocks*/
UNIV_INTERN
ulint
fc_get_size(
/*=========*/
);
/**************************************************************//**
Set flash cache size. */
UNIV_INTERN
void
fc_set_size(
/*=========*/
	ulint size	/*!< in: flash cache size */
);
/********************************************************************//**
Write double write buffer to flash cache file at start offset
@return: count of async read flash cache block*/
ulint
fc_write_doublewrite_to_flash_cache_start(
/*===========================*/
	trx_doublewrite_t* trx_doublewrite,	/*!< in: doublewrite structure */
	ulint start_off	/*!< in: flash cache write position */
);
/********************************************************************//**
When srv_flash_cache_enable_write is FALSE, doublewrite buffer will behave as deault. 
So if any page in doublewrite buffer now(newer) is also in flash cache already(olded),
it must be removed  from the flash cache before doublewrite buffer write to disk.
@return: pages removed in flash cache */
ulint
fc_remove_pages_in_dwb(trx_doublewrite_t* trx_doublewrite);
/********************************************************************//**
Flush double write buffer to flash cache block.
@return: count of async read flash cache block*/
ulint
fc_write_doublewrite_to_flash_cache(
/*===========================*/
	trx_doublewrite_t* trx_doublewrite	/*!< in: doublewrite structure */
);
/********************************************************************//**
Flush flash cache block to flash cache file.
@return: number of pages to flush*/
UNIV_INTERN
ulint	
fc_flush_to_disk_estimate(
/*==================*/
	ulint start_offset,	/*!< in: start flush offset */
	ulint* c_flush,		/*!< out: actually flush pages */
	ibool do_full_io	/*!< in: whether do full io capacity */
);
/********************************************************************//**
Flush flash cache block to flash cache file using adaptive algorithms.
@return: number of pages to flush*/
UNIV_INTERN
ulint	
fc_flush_to_disk_adaptive(
/*==================*/
	ulint start_offset,	/*!< in: start flush offset */
	ulint* c_flush,		/*!< out: actually flush pages */
	ibool do_full_io	/*!< in: whether do full io capacity */
);
/******************************************************************//**
Flush pages from flash cache.
@return	number of pages to be flush to tablespace */
UNIV_INTERN
ulint
fc_flush_to_disk(
/*===================*/
	ibool do_full_io	/*<! whether do full io flush */
);
/**********************************************************************//**
Move to flash cache if possible */
UNIV_INTERN
void
fc_LRU_move(
/*=========================*/
	buf_page_t* bpage	/*!< in: page flush out from buffer pool */
);
/********************************************************************//**
Read page from flash cache block, if not found in flash cache, read from disk.																	  
Note: ibuf page must read in aio mode to avoid deadlock
@return 1 if read request is issued. 0 if it is not */
UNIV_INTERN
ulint
fc_read_page(
/*==============*/
	ibool	sync,	/*!< in: TRUE if synchronous aio is desired */
	ulint	space,	/*!< in: space id */
	ulint	zip_size,/*!< in: compressed page size, or 0 */
	ibool	unzip,	/*!< in: TRUE=request uncompressed page */
	ulint	offset,	/*!< in: page number */
	ulint	wake_later,	/*!< wake later flag */
	void*	buf,		/*!< in/out: buffer where to store read data
				or from where to write; in aio this must be
				appropriately aligned */
	buf_page_t*	bpage	/*!< in/out: read flash cache block to this page */
);
/********************************************************************//**
Compelete flash cache async read. */
UNIV_INTERN
void
fc_compelete_read(
/*==============*/
	buf_page_t* bpage	/*!< page to compelete */
);

/****************************************************************//**
Initialize flash cache log.*/
UNIV_INTERN
void
fc_log_update(
/*==========================================*/
ulint backup
);

/*********************************************************************//**
Flash cache log commit operation.*/
UNIV_INTERN
void
fc_log_commit(
/*==========================================*/
);

/********************************************************************//**
Print flash cache status. */
UNIV_INTERN
void
fc_status
(
/*=================================*/
	ulint page_read_delta,
	ulint n_ra_pages_read,
	FILE* file
);
/********************************************************************//**
fc_backup_thread 
*/
UNIV_INTERN
os_thread_ret_t
fc_backup_thread(void*);
/********************************************************************//**
return TRUE if no need to flush from flash cache to disk
@return TRUE if offset and round are equal.*/
UNIV_INTERN
my_bool
fc_finish_flush(
/*==============*/
);

/* for use of fc_block  sort*/
#define ASCENDING 0
#define DESCENDING 1
/* sort according (space_id, page_no) */
void fc_block_sort(fc_block_t** base, ulint len, ulint type);



UNIV_INTERN
void
fc_LRU_move_optimization(buf_page_t * bpage);


#endif
