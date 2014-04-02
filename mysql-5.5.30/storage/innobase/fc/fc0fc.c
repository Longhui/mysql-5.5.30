/**************************************************//**
@file fc/fc0fc.c
Flash Cache for InnoDB

Created	24/4/2012 David Jiang (jiangchengyao@gmail.com)
*******************************************************/

#include "fc0fc.h"

#include "ut0ut.h"
#include "os0file.h"
#include "fc0log.h"
#include "srv0srv.h"
#include "log0recv.h"
#include "ibuf0ibuf.h"
#include "fc0dump.h"
#include "fc0recv.h"
#include "srv0start.h"
#include "fsp0types.h"


/* flash cache size */
UNIV_INTERN ulint	srv_flash_cache_size = ULINT_MAX; 
/* flash cache file */
UNIV_INTERN char*	srv_flash_cache_file = NULL;
/* flash cache warmup table when startup */
UNIV_INTERN char*	srv_flash_cache_warmup_table = NULL;
/* flash cache warmup from file when startup */
UNIV_INTERN char*	srv_flash_cache_warmup_file = NULL;
/* read number of page per operation in recovery and warmup */
UNIV_INTERN ulint	srv_flash_cache_pages_per_read = 512;
/* flash cache write cache percentage */
UNIV_INTERN ulong	srv_flash_cache_write_cache_pct = 30;
/* flash cache do full IO percentage */
UNIV_INTERN ulong	srv_flash_cache_do_full_io_pct = 90;
/* flash cache block move limit */
UNIV_INTERN ulong	srv_flash_cache_move_limit = 90;
/* whether enable flash cache block move */
UNIV_INTERN my_bool	srv_flash_cache_enable_move = TRUE;
/* whether enable flash cache safest recovery */
UNIV_INTERN my_bool	srv_flash_cache_safest_recovery = FALSE;
/* whether enable flash cache block migrate */
UNIV_INTERN my_bool	srv_flash_cache_enable_migrate = TRUE;
/* use flash cache device as raw */
UNIV_INTERN my_bool	srv_flash_cache_is_raw = FALSE;
/* adaptive flush for flash cache */
UNIV_INTERN my_bool srv_flash_cache_adaptive_flushing = FALSE;
/* flash cache io capacity */
UNIV_INTERN ulong srv_fc_io_capacity = 500;
/* doublewrite buffer flush to flash cache or behave as default */
UNIV_INTERN my_bool  srv_flash_cache_enable_write = TRUE;
/* When 'innodb_flash_cache_enable_write' is first set to FALSE and then 
     'innodb_flash_cache_backup' is set to TRUE, begin flash backup. */
UNIV_INTERN my_bool  srv_flash_cache_backuping = FALSE;
/* which directory to store  ib_fc_file, must including the terminating '/' */
UNIV_INTERN char*  srv_flash_cache_backup_dir = NULL;
/* whetert use fc_LRU_move_optimization '/' */
UNIV_INTERN my_bool  srv_flash_cache_lru_move_batch =TRUE;

/* write-back: WRITE_BACK, write-through: WRITE_THROUGH*/
UNIV_INTERN ulong  srv_flash_cache_write_mode = WRITE_BACK;

/* whether use fast shutdown when MySQL is close */
UNIV_INTERN my_bool srv_flash_cache_fast_shutdown = TRUE;

/** flash cache status */
/* pages reads from flash cache */
UNIV_INTERN ulint	srv_flash_cache_read = 0;
/* pages async read from flash cache */
UNIV_INTERN ulint	srv_flash_cache_aio_read = 0;
/* pages async read wait */
UNIV_INTERN ulint	srv_flash_cache_wait_for_aio = 0;
/* pages write to doublewrite from flash cache */
UNIV_INTERN ulint	srv_flash_cache_write = 0;
/* pages flush to disk from flash cache */
UNIV_INTERN ulint	srv_flash_cache_flush = 0;
/* pages merged in flash cache */
UNIV_INTERN ulint	srv_flash_cache_merge_write = 0;
/* pages move */
UNIV_INTERN ulint	srv_flash_cache_move = 0;
/* pages migrate */
UNIV_INTERN ulint	srv_flash_cache_migrate = 0;
/* read detail info */
UNIV_INTERN ulint	srv_flash_cache_read_detail[FIL_PAGE_TYPE_ZBLOB2+1];
/* write detail info */
UNIV_INTERN ulint	srv_flash_cache_write_detail[FIL_PAGE_TYPE_ZBLOB2+1];
/* flush detail info */
UNIV_INTERN ulint	srv_flash_cache_flush_detail[FIL_PAGE_TYPE_ZBLOB2+1];
/* used flash cache block */
UNIV_INTERN ulint	srv_flash_cache_used = 0;
/* flash cache log file name */
UNIV_INTERN const char srv_flash_cache_log_file_name[16] = "flash_cache.log";
/* flash cache thread info */
UNIV_INTERN const char*	srv_flash_cache_thread_op_info = "";

/* whether flash cache has warmuped from dump file */
UNIV_INTERN my_bool srv_flash_cache_load_from_dump_file = FALSE;


/* flash cache structure */
UNIV_INTERN fc_t* fc = NULL;

#define FLASH_CACHE_MOVE_HIGH_LIMIT (1.0*srv_flash_cache_move_limit*fc_get_size()/100)
#define FLASH_CACHE_MOVE_LOW_LIMIT (1.0*(100-srv_flash_cache_move_limit)*fc_get_size()/100)

typedef struct flash_cache_stat_struct flash_cache_stat_t;
struct flash_cache_stat_struct{
	ulint write_off;
	ulint write_round;
	ulint flush_off;
	ulint flush_round;
	ulint n_pages_write;
	ulint n_pages_flush;
	ulint n_pages_merge_write;
	ulint n_pages_read;
	ulint n_pages_migrate;
	ulint n_pages_move;
	time_t last_printout_time;
};

/** flash cache status info */
UNIV_INTERN flash_cache_stat_t flash_cache_stat;


/**************************************************************//**
Initialize flash cache struct.*/
UNIV_INTERN
void
fc_create(
/*=========*/
)
{
	ulong i ;

	fc = (fc_t*)ut_malloc(sizeof(fc_t));

	if ( fc == NULL ){
		ut_print_timestamp(stderr);
		fprintf(stderr," InnoDB: Can not allocate memory for flash cache.\n");
		ut_error;
	}

	fc->write_off = 0;
	fc->flush_off = 0;
	fc->size = srv_flash_cache_size >> UNIV_PAGE_SIZE_SHIFT ; 
	fc->hash_table = hash_create(2 * fc->size);

	fc->write_round = 0;
	fc->flush_round = 0;
	fc->read_buf_unalign = (byte*)ut_malloc((srv_fc_io_capacity+1)*UNIV_PAGE_SIZE);
	fc->read_buf = (byte*)ut_align(fc->read_buf_unalign,UNIV_PAGE_SIZE);

	mutex_create(PFS_NOT_INSTRUMENTED,
		&fc->mutex, SYNC_DOUBLEWRITE);
	mutex_create(PFS_NOT_INSTRUMENTED,
		&fc->hash_mutex, SYNC_DOUBLEWRITE);

	fc->block = (fc_block_t*)ut_malloc(sizeof(fc_block_t)*fc->size);

	for(i=0;i<fc->size;i++){
		fc->block[i].fil_offset = i;
		fc->block[i].hash = NULL;
		fc->block[i].space = 0;
		fc->block[i].offset = 0;
		fc->block[i].state = BLOCK_NOT_USED;
		fc->block[i].is_aio_reading = FALSE;
	}

	/* move & migrate optimization */
	fc->move_migrate_n_pages = 2 * FSP_EXTENT_SIZE;
	fc->move_migrate_blocks = (fc_block_t*) ut_malloc(sizeof(fc_block_t) * fc->move_migrate_n_pages);
	fc->move_migrate_un_aligned_pages = (byte*) ut_malloc((fc->move_migrate_n_pages + 1)* UNIV_PAGE_SIZE);
	fc->move_migrate_pages = (byte*) ut_align(fc->move_migrate_un_aligned_pages, UNIV_PAGE_SIZE);
	fc->move_migrate_next_write_pos = 0;
	
	for(i=0; i<fc->move_migrate_n_pages; i++){
		fc->move_migrate_blocks[i].fil_offset = fc->size + i;  /* when lookuped in hash table,  used for distinguish*/
		fc->move_migrate_blocks[i].hash = NULL;
		fc->move_migrate_blocks[i].space = 0;
		fc->move_migrate_blocks[i].offset = 0;
		fc->move_migrate_blocks[i].state = BLOCK_NOT_USED;
		fc->move_migrate_blocks[i].is_aio_reading = FALSE; /* when page is in fc->move_migrate_n_pages, read 
															   is just an operation of coping, so this filed will always be FALSE */
	}
	
}

/**************************************************************//**
Start flash cache.*/
UNIV_INTERN
void
fc_start(
/*=========*/
)
{
	ut_ad(srv_flash_cache_size > 0);

	fc_create();
	fc_log_create();

	if ( access("flash_cache.warmup",F_OK ) != -1 ){  

	} else {
		if ( srv_flash_cache_load_from_dump_file == FALSE ){
			/* warmup flash cache use file flash_cache.dump. */
			fc_load();
		}
		if ( srv_flash_cache_load_from_dump_file == FALSE ){
			/*
            * if we run here, it means we need scan flash cache file
            * to recovery the flash cache block.
            */
            fil_load_single_table_tablespaces();
			fc_recv();
		}
		if ( recv_needed_recovery ){
            /*
			* Note: if in redo log recovery, we flush flash cache block to disk
			* in this step to avoid recovery occupy flash cache block
			* that do not flush to disk
			*/
			os_thread_create(&srv_flash_cache_thread, NULL, NULL);
		}
	}

	/* flash cache has started. if is load, commit log */
	if (fc_log->first_use == FALSE) {
		flash_cache_mutex_enter();
		fc_log_update(FALSE);
		fc_log->write_offset_bck = 0XFFFFFFFFUL;
		fc_log_commit();
		flash_cache_mutex_exit();
	}


}
/**************************************************************//**
Free flash cache struct.*/
UNIV_INTERN
void
fc_destroy(
/*=========*/
)
{
	ut_free(fc->read_buf_unalign);
	ut_free(fc->block);
	hash_table_free(fc->hash_table);
	mutex_free(&fc->hash_mutex);
	mutex_free(&fc->mutex);

	/* move & migrate optimization */
	ut_free(fc->move_migrate_blocks);
	ut_free(fc->move_migrate_un_aligned_pages);
	/* move & migrate optimization */
	
	ut_free(fc);
}

/*for use of fc_block_sort */
static 
int 
fc_page_cmp_ascending(const void* p1, const void* p2)
{	
	const fc_block_t* p_fc_block_1 = *((fc_block_t**) p1);	
	const fc_block_t* p_fc_block_2 = *((fc_block_t**) p2);
	
	if(p_fc_block_1->space < p_fc_block_2->space)	
		return -1;	
	if(p_fc_block_1->space > p_fc_block_2->space)	
		return 1;		
	if(p_fc_block_1->offset < p_fc_block_2->offset)	
		return -1;		
	if(p_fc_block_1->offset > p_fc_block_2->offset)	
		return 1;		
	return 0;	
}

static 
int 
fc_page_cmp_descending(const void* p1, const void* p2)
{	
	const fc_block_t* p_fc_block_1 = *((fc_block_t**) p1);	
	const fc_block_t* p_fc_block_2 = *((fc_block_t**) p2);
	
	if(p_fc_block_1->space > p_fc_block_2->space)	
		return -1;	
	if(p_fc_block_1->space < p_fc_block_2->space)	
		return 1;		
	if(p_fc_block_1->offset > p_fc_block_2->offset)	
		return -1;		
	if(p_fc_block_1->offset < p_fc_block_2->offset)	
		return 1;		
	return 0;	
}


void fc_block_sort(fc_block_t** base, ulint len, ulint type)
{
	ut_a(base);
	ut_a(len);
	ut_a(ASCENDING == type || DESCENDING == type);
	if(ASCENDING == type)
		qsort(base, len, sizeof(fc_block_t*), fc_page_cmp_ascending);
	else
		qsort(base, len, sizeof(fc_block_t*), fc_page_cmp_descending);
}



/**************************************************************//**
Check whether flash cache is enable.*/
UNIV_INTERN
my_bool
fc_is_enabled(
/*=========*/
)
{
	if ( srv_flash_cache_size > 0 )
		return TRUE;
	ut_ad(srv_flash_cache_size == 0);
	return FALSE;
}

/**************************************************************//**
Get flash cache size
@return number of flash cache blocks*/
UNIV_INTERN
ulint
fc_get_size(
/*=========*/
)
{
	ut_ad(fc != NULL);
	return fc->size;
}

/**************************************************************//**
Set flash cache size. */
UNIV_INTERN
void
fc_set_size(
/*=========*/
	ulint size	/*!< in: flash cache size */
)
{
	ut_ad(fc != NULL);
	fc->size = size;
}

/********************************************************************//**
Flush a batch of writes to the datafiles that have already been
written by the OS. */
static
void
fc_flush_sync_datafiles(void)
/*==========================*/
{
	/* Wake possible simulated aio thread to actually post the
	writes to the operating system */
	os_aio_simulated_wake_handler_threads();

	/* Wait that all async writes to tablespaces have been posted to
	the OS */
	os_aio_wait_until_no_pending_writes();

	/* Now we flush the data to disk (for example, with fsync) */
	fil_flush_file_spaces(FIL_TABLESPACE);

	return;
}

/********************************************************************//**
Flush a batch of writes to the datafiles that have already been
written by the flash cache. */
static
void
fc_sync_datafiles(
/*===========================*/
)
{
	/* Wake possible simulated aio thread to actually post the
	writes to the operating system */
	os_aio_simulated_wake_handler_threads();

	/* Wait that all async writes to tablespaces have been posted to
	the OS */
	os_aio_wait_until_no_pending_fc_writes();

	/* Now we flush the data to disk (for example, with fsync) */
	fil_flush_file_spaces(FIL_FLASH_CACHE);
}

/********************************************************************//**
Write double write buffer to flash cache file at start offset.
@return: count of async read flash cache block*/
ulint
fc_write_doublewrite_to_flash_cache_start(
/*===========================*/
	trx_doublewrite_t* trx_doublewrite,	/*!< in: doublewrite structure */
	ulint start_off	/*!< in: flash cache write position */
)
{
	ulint i;
	ulint off;
	buf_block_t* block;
	fc_block_t* b;
	fc_block_t* b2;
	ulint page_type;
	ulint aio_wait=0;
	ulint ret;
#ifdef UNIV_FLASH_DEBUG
    ulint zip_size = 0;
#endif
    
	ut_ad(mutex_own(&fc->mutex));

	for(i = 0; i < trx_doublewrite->first_free; i++){
		block = (buf_block_t*)trx_doublewrite->buf_block_arr[i];

		if (!block->page.zip.data){
			if (UNIV_UNLIKELY(memcmp(block->frame + (FIL_PAGE_LSN + 4),
						 block->frame
						 + (UNIV_PAGE_SIZE
							- FIL_PAGE_END_LSN_OLD_CHKSUM + 4),
						 4))) {
				ut_print_timestamp(stderr);
				fprintf(stderr,
					"  InnoDB: ERROR: The page to be written"
					" seems corrupt!\n"
					"InnoDB: The lsn fields do not match!"
					" Noticed in the buffer pool\n"
					"InnoDB: after posting and flushing"
					" the doublewrite buffer.\n"
					"InnoDB: Page buf fix count %lu,"
					" io fix %lu, state %lu\n",
					(ulong)block->page.buf_fix_count,
					(ulong)buf_block_get_io_fix(block),
					(ulong)buf_block_get_state(block));
			}
		}

next:
		off = (start_off + i + aio_wait) % fc_get_size();

		b = &fc->block[off];

		flash_cache_hash_mutex_enter(b->space,b->offset);
		if ( b->is_aio_reading ){
			srv_flash_cache_wait_for_aio++;
			aio_wait++;
			flash_cache_hash_mutex_exit(b->space,b->offset);
			goto next;
		}

		if ( b->state != BLOCK_NOT_USED ){
			ut_a( b->state != BLOCK_READY_FOR_FLUSH );
			/* alread used, remove it from the hash table */
			HASH_DELETE(fc_block_t,hash,fc->hash_table,
				buf_page_address_fold(b->space, b->offset),b);
			b->state = BLOCK_NOT_USED;
			srv_flash_cache_used = srv_flash_cache_used - 1;
#ifdef UNIV_FLASH_DEBUG
			ut_print_timestamp(stderr);
			fprintf(stderr,"	InnoDB: write - %lu, %lu.\n",b->space,b->offset);
#endif
		}
		flash_cache_hash_mutex_exit(b->space,b->offset);

		flash_cache_hash_mutex_enter(block->page.space,block->page.offset);
		/* following we do not need mutex to protect because bpage is in the buffer pool */
        page_type = fil_page_get_type(trx_doublewrite->write_buf + i*UNIV_PAGE_SIZE);
		if ( page_type == FIL_PAGE_INDEX ){
			page_type = 1;
		}
		srv_flash_cache_write_detail[page_type]++;
		b->space = block->page.space;
		b->offset = block->page.offset;
		b->state = BLOCK_READY_FOR_FLUSH;

		/* search the same space offset in hash table */
		HASH_SEARCH(hash,fc->hash_table,
			buf_page_address_fold(block->page.space,block->page.offset),
			fc_block_t*,b2,
			ut_ad(1),
			block->page.space == b2->space && block->page.offset == b2->offset);

		if ( b2 ){
			ut_a( b2->state != BLOCK_NOT_USED );
			ut_a( b2->is_aio_reading == FALSE );
			b2->state = BLOCK_NOT_USED;
			/* alread used, remove it from the hash table */
			HASH_DELETE(fc_block_t,hash,fc->hash_table,
				buf_page_address_fold(b2->space, b2->offset),
				b2);
			//srv_flash_cache_merge_write++;
			if(b2->fil_offset < fc->size)
				srv_flash_cache_used = srv_flash_cache_used - 1;
#ifdef UNIV_FLASH_DEBUG
		ut_print_timestamp(stderr);
		fprintf(stderr,"	InnoDB: write - %lu, %lu.\n",b2->space,b2->offset);
#endif
		}

		/* insert to hash table */
		HASH_INSERT(fc_block_t,hash,fc->hash_table,
			buf_page_address_fold(b->space, b->offset),
			b);
		srv_flash_cache_used = srv_flash_cache_used + 1;
		flash_cache_hash_mutex_exit(block->page.space,block->page.offset);

#ifdef UNIV_FLASH_DEBUG
		ut_print_timestamp(stderr);
		fprintf(stderr,"	InnoDB: write + %lu, %lu.\n",b->space,b->offset);

        zip_size = fil_space_get_zip_size(block->page.space);
        if (zip_size){
            if(buf_page_is_corrupted(trx_doublewrite->write_buf + i*UNIV_PAGE_SIZE,zip_size)){
                buf_page_print(trx_doublewrite->write_buf + i*UNIV_PAGE_SIZE, zip_size);
                ut_error;
            }
        }
#endif
		/* write to flash cache */
		ret = fil_io(OS_FILE_WRITE,FALSE,FLASH_CACHE_SPACE,0,off,0,UNIV_PAGE_SIZE,trx_doublewrite->write_buf + i*UNIV_PAGE_SIZE,NULL);

		if ( ret != DB_SUCCESS ){
			ut_print_timestamp(stderr);
			fprintf(stderr," InnoDB Error: fail to aio write. Page offset is:%lu.\n",off);
		}
	}

	fc_sync_datafiles();

	return aio_wait;
}

/********************************************************************//**
Flush double write buffer to flash cache block.
@return: count of async read flash cache block*/
ulint
fc_write_doublewrite_to_flash_cache(
/*===========================*/
	trx_doublewrite_t* trx_doublewrite	/*!< in: doublewrite structure */
)
{
	ulint start_off = 0;
	ulint aio_wait = 0;
	ulint i = 0;
	ibool retry = FALSE;

	ut_ad(!mutex_own(&fc->mutex));

total_retry:
	flash_cache_mutex_enter();
	if (retry == FALSE){
		srv_flash_cache_write += trx_doublewrite->first_free;
	}
	start_off = fc->write_off;
	if ( fc->write_round == fc->flush_round ){
		/* in the same round */
		if (fc->write_off + trx_doublewrite->first_free < fc_get_size()){
			aio_wait = fc_write_doublewrite_to_flash_cache_start(trx_doublewrite,start_off);
			fc->write_off = fc->write_off +  trx_doublewrite->first_free + aio_wait;  
			if ( fc->write_off >= fc_get_size() ){
				fc->write_off = fc->write_off % fc->size;
				fc->write_round = fc->write_round + 1;
			}
		}
		else {
			ulint len1;
			ulint len2;

			len1 = fc_get_size() - fc->write_off;
			len2 = trx_doublewrite->first_free - len1;

			if ( len2 > fc->flush_off ){ 
				ut_print_timestamp(stderr);
				fprintf(stderr,
				" InnoDB: No space for write cache, waiting(retry1). flash cache write offset: %lu, flush offset %lu.\n",
				fc->write_off,
				fc->flush_off);
				flash_cache_mutex_exit();
				os_thread_sleep(1000000);
				retry = TRUE;
				goto total_retry;
			}
			aio_wait = fc_write_doublewrite_to_flash_cache_start(trx_doublewrite,start_off);
			fc->write_off = len2 + aio_wait;
			fc->write_round = fc->write_round + 1;
		}
	}
	else{
			
		ut_ad(fc->flush_round + 1 == fc->write_round);
		ut_ad(fc->flush_off >= fc->write_off );

		if ( fc->write_off + trx_doublewrite->first_free >= fc->flush_off ){
			ut_print_timestamp(stderr);
			fprintf(stderr,
				"  InnoDB WARNING: No space for write cache, waiting(retry). flash cache write offset: %lu, flush offset %lu.\n",
				fc->write_off,
				fc->flush_off);
				flash_cache_mutex_exit();
				os_thread_sleep(1000000);
				retry = TRUE;
				goto total_retry;
		}
		aio_wait = fc_write_doublewrite_to_flash_cache_start(trx_doublewrite,start_off); 
		fc->write_off = fc->write_off + trx_doublewrite->first_free + aio_wait; 
	}
	fc_log_update(FALSE);
	fc_log_commit();
	flash_cache_mutex_exit();

	for( i = 0; i < trx_doublewrite->first_free; i++ ){
		buf_page_io_complete( &(((buf_block_t*) trx_doublewrite->buf_block_arr[i])->page),TRUE );	
	}

	return aio_wait;
}

/********************************************************************//**
When srv_flash_cache_enable_write is FALSE, doublewrite buffer will behave as deault. 
So if any page in doublewrite buffer now(newer) is also in flash cache already(olded),
it must be removed  from the flash cache before doublewrite buffer write to disk.
@return: pages removed in flash cache */
ulint
fc_remove_pages_in_dwb(trx_doublewrite_t* trx_doublewrite)
{
	ulint i;
	fc_block_t* fc_block;
	ulint space_id;
	ulint page_no;
	ulint removed_pages = 0;
	
	for(i = 0; i < trx_doublewrite->first_free; ++i) {
		space_id = mach_read_from_4(trx_doublewrite->write_buf + i * UNIV_PAGE_SIZE + FIL_PAGE_ARCH_LOG_NO_OR_SPACE_ID);
		page_no  = mach_read_from_4(trx_doublewrite->write_buf + i * UNIV_PAGE_SIZE + FIL_PAGE_OFFSET);
		
		flash_cache_mutex_enter();
		flash_cache_hash_mutex_enter(space_id, page_no);
		HASH_SEARCH(hash,fc->hash_table,
				buf_page_address_fold(space_id,page_no),
				fc_block_t*,fc_block,ut_ad(1),
				space_id == fc_block->space && page_no == fc_block->offset);
		if(fc_block){
			HASH_DELETE(fc_block_t,hash,fc->hash_table,buf_page_address_fold(space_id, page_no),fc_block);
			fc_block->state = BLOCK_NOT_USED;
			if(fc_block->fil_offset < fc->size)
				--srv_flash_cache_used;
			++removed_pages;
			
		}	
		flash_cache_hash_mutex_exit(space_id,page_no);
		flash_cache_mutex_exit();
	}
	return removed_pages;
}

/******************************************************************//**
Get distance between flush offset and write offset .
@return	number of pages*/ 
static
ulint
fc_get_distance(
/*==================*/
)
{

	ut_ad(mutex_own(&fc->mutex));

	if (fc->write_round == fc->flush_round){
		return (fc->write_off - fc->flush_off);
	}
	else{
		return (fc_get_size() + fc->write_off - fc->flush_off);
	}
}

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
){
	ulint ret;
	ulint n_flush;
	ulint i = 0;
	ulint j = 0;
	byte* page;
	ulint space;
	ulint offset;
	ulint page_type; 
	ulint zip_size;

#ifdef UNIV_FLASH_DEBUG
	ulint lsn;
	ulint lsn2;
	byte page2[UNIV_PAGE_SIZE];
	ulint space2;
	ulint offset2;
#endif

	ut_ad(!mutex_own(&fc->mutex));

	flash_cache_mutex_enter();
	if ( fc->flush_round == fc->write_round ){
		if ( fc->flush_off + srv_fc_io_capacity <= fc->write_off ) {
			n_flush = srv_fc_io_capacity;
		}
		else{
			/* no enough space to flush */
			n_flush = fc->write_off - fc->flush_off;
			if ( n_flush == 0 ){
				flash_cache_mutex_exit();
				return (0);
			}
		}

		if ( (fc->write_off - fc->flush_off) < ( 1.0*srv_flash_cache_write_cache_pct/100 )*fc_get_size()
				&& !do_full_io){
			flash_cache_mutex_exit();
			return (0);
		}
		else if ( (fc->write_off - fc->flush_off) < ( 1.0*srv_flash_cache_do_full_io_pct/100 )*fc_get_size()
				&& !do_full_io){
			n_flush = ut_min(PCT_IO(10),n_flush);
			if ( n_flush == 0 ){
				flash_cache_mutex_exit();
				return (0);
			}
		}
	}
	else{
		if ( fc->flush_off + srv_fc_io_capacity <= fc_get_size() ) {
			n_flush = srv_fc_io_capacity;
		}
		else{
			n_flush = fc_get_size() - fc->flush_off;
		}
		if ( (fc->write_off - fc->flush_off + fc_get_size() ) < ( 1.0*srv_flash_cache_write_cache_pct/100 )*fc_get_size()
				&& !do_full_io){
			flash_cache_mutex_exit();
			return (0);
		}
		else if ( (fc->write_off - fc->flush_off + fc_get_size() )  < ( 1.0*srv_flash_cache_do_full_io_pct/100 )*fc_get_size()
				&& !do_full_io){
			n_flush = ut_min(PCT_IO(10),n_flush);
			if ( n_flush == 0 ){
				flash_cache_mutex_exit();
				return (0);
			}
		}
	}
	flash_cache_mutex_exit();

	srv_flash_cache_flush += n_flush;

	for(i = 0; i < n_flush; i++){
		flash_cache_mutex_enter();
		if ( fc->block[start_offset+i].state == BLOCK_NOT_USED
			|| fc->block[start_offset+i].state == BLOCK_READ_CACHE ){
			/* if readonly or merge write */
			if (fc->block[start_offset+i].state == BLOCK_NOT_USED){
				srv_flash_cache_merge_write++;
			}
			flash_cache_mutex_exit();
			continue;
		}
		else if ( fc->block[start_offset+i].state == BLOCK_FLUSHED ){
			flash_cache_mutex_exit();
			continue;
		}
		ut_a( fc->block[start_offset+i].state == BLOCK_READY_FOR_FLUSH );
		fc->block[start_offset+i].state = BLOCK_FLUSHED;
		flash_cache_mutex_exit();
		page = fc->read_buf + j*UNIV_PAGE_SIZE;
		ret = fil_io(OS_FILE_READ, TRUE,
			FLASH_CACHE_SPACE, 0,
			fc->flush_off + i, 0, UNIV_PAGE_SIZE,
			page, NULL);
		if ( ret != DB_SUCCESS ){
			ut_print_timestamp(stderr);
			fprintf(
				stderr,"InnoDB: Flash cache [Error]: unable to read page from flash cache.\n"
				"flash cache flush offset is:%lu.\n",
				(ulong)fc->flush_off + i
				);
			ut_error;
		}		
		space = mach_read_from_4( page + FIL_PAGE_ARCH_LOG_NO_OR_SPACE_ID );
		zip_size = fil_space_get_zip_size(space);
		if (zip_size == ULINT_UNDEFINED){
			/* table has been droped */
			continue;
		}
		offset = mach_read_from_4( page + FIL_PAGE_OFFSET );

		ut_a( space == fc->block[start_offset+i].space );
		ut_a( offset == fc->block[start_offset+i].offset );

		if (buf_page_is_corrupted(page, zip_size)){
			buf_page_print(page,zip_size,BUF_PAGE_PRINT_NO_CRASH);
			ut_error;
		}
		page_type = fil_page_get_type(page);
		if (page_type == FIL_PAGE_INDEX){
			page_type = 1;
		}
		srv_flash_cache_flush_detail[page_type]++;
		ret = fil_io(OS_FILE_WRITE | OS_AIO_SIMULATED_WAKE_LATER,FALSE,space,zip_size,offset,0,zip_size ? zip_size : UNIV_PAGE_SIZE,page,NULL);
		if ( ret != DB_SUCCESS && ret != DB_TABLESPACE_DELETED ){
			ut_print_timestamp(stderr);
			fprintf(stderr," InnoDB Error: write flash cache(%lu) to disk(%lu,%lu) Error.\n",fc->flush_off+i,space,offset);
			ut_error;
		}
		j++;
	}

	fc_flush_sync_datafiles();

	*c_flush = n_flush;

	return n_flush;
	
}

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
){
	ulint distance;
	byte* page;
	ulint ret;
	ulint space;
	ulint offset;
	ulint page_type;
	ulint n_flush = 0;
	ulint i = 0;
	ulint pos = 0;
	ulint zip_size;
    ulint write_offset = 0;
    
	ut_ad(!mutex_own(&fc->mutex));

	flash_cache_mutex_enter();

	distance = fc_get_distance();
    write_offset = fc->write_off;
    
	if ( distance == 0 ){
		flash_cache_mutex_exit();
		return 0;
	}
	else if ( recv_recovery_on ){
		if ( distance < ( 1.0*srv_flash_cache_write_cache_pct/100 )*fc_get_size() )
			n_flush = 0;
		else if ( distance < ( 1.0*srv_flash_cache_do_full_io_pct/100 )*fc_get_size() )
			n_flush = ut_min(PCT_IO(10), distance);
		else
			n_flush = ut_min(PCT_IO(100), distance);
	}
	else if ( distance < ( 1.0*srv_flash_cache_write_cache_pct/100 )*fc_get_size() && !do_full_io ){
		flash_cache_mutex_exit();
		return 0;
	}
	else if ( distance < ( 1.0*srv_flash_cache_do_full_io_pct/100 )*fc_get_size() && !do_full_io ){
		n_flush = PCT_IO(10);
		if ( n_flush == 0 ){
			flash_cache_mutex_exit();
			return 0;
		}
	}
	else{
		ut_ad( ( fc_get_distance() > ( 1.0*srv_flash_cache_do_full_io_pct/100 )*fc_get_size() ) || do_full_io );
		n_flush = ut_min(PCT_IO(100), distance);
	}

	flash_cache_mutex_exit();

	while(*c_flush < n_flush){
		pos = ( start_offset + i ) % fc_get_size() ;
        
        /*
         Note: maybe there are not n_flush pages need to flush back to disk.
         So we add condition (start_offset + i) % fc_get_size() != write_offset
        */
        if (pos == write_offset)
            break;
        
		flash_cache_mutex_enter();
		if ( fc->block[pos].state != BLOCK_READY_FOR_FLUSH ){
			/* if readonly or merge write or aready flushed */
			ut_ad ( fc->block[pos].state == BLOCK_NOT_USED
				|| fc->block[pos].state == BLOCK_READ_CACHE
				|| fc->block[pos].state == BLOCK_FLUSHED );
			if (fc->block[pos].state == BLOCK_NOT_USED){
				srv_flash_cache_merge_write++;
			}
			i++;
			flash_cache_mutex_exit();
			continue;
		}
		fc->block[pos].state = BLOCK_FLUSHED;
		flash_cache_mutex_exit();
		page = fc->read_buf + *c_flush*UNIV_PAGE_SIZE;
		ret = fil_io(OS_FILE_READ, TRUE,
			FLASH_CACHE_SPACE, 0,
			pos, 0, UNIV_PAGE_SIZE,
			page, NULL);
		if ( ret != DB_SUCCESS ){
			ut_print_timestamp(stderr);
			fprintf(
				stderr," InnoDB: Flash cache [Error]: unable to read page from flash cache.\n"
				"flash cache flush offset is:%lu.\n",
				(ulong)fc->flush_off + i
				);
			ut_error;
		}		
		space = mach_read_from_4( page + FIL_PAGE_ARCH_LOG_NO_OR_SPACE_ID );
		zip_size = fil_space_get_zip_size(space);
		if (zip_size == ULINT_UNDEFINED){
			/* table has been droped */
			continue;
		}
		ut_a( space == fc->block[pos].space );
		offset = mach_read_from_4( page + FIL_PAGE_OFFSET );
		ut_a( offset == fc->block[pos].offset );
		if ( buf_page_is_corrupted(page, zip_size) ){
			buf_page_print(page,zip_size,BUF_PAGE_PRINT_NO_CRASH);
			ut_error;
		}
		page_type = fil_page_get_type(page);
		if ( page_type == FIL_PAGE_INDEX ){
			page_type = 1;
		}
		srv_flash_cache_flush_detail[page_type]++;
		ret = fil_io(OS_FILE_WRITE | OS_AIO_SIMULATED_WAKE_LATER,FALSE,space,zip_size,offset,0,zip_size ? zip_size : UNIV_PAGE_SIZE,page,NULL);
		if (ret != DB_SUCCESS && ret != DB_TABLESPACE_DELETED){
			ut_print_timestamp(stderr);
			fprintf(stderr," InnoDB Error: write flash cache(%lu) to disk(%lu,%lu) Error.\n",fc->flush_off+i,space,offset);
			ut_error;
		}
		i++;
		*c_flush = *c_flush + 1;
	}
	
	fc_flush_sync_datafiles();

	srv_flash_cache_flush = srv_flash_cache_flush + i;

#ifdef UNIV_FLASH_DEBUG
	ut_print_timestamp(stderr);
	fprintf(stderr," InnoDB: flush page from flash to disk, flush %lu page, actually flush %lu page, merge write ratio %.2f.\n",i,*c_flush, 1.0*(*c_flush)/i);
#endif

	return i;
	
}

/******************************************************************//**
Flush pages from flash cache.
@return	number of pages to be flush to tablespace */
UNIV_INTERN
ulint
fc_flush_to_disk(
/*===================*/
	ibool do_full_io	/*<! whether do full io flush */
){
	ulint n_flush = 0;
	ulint c_flush = 0;
	ulint start_offset = fc->flush_off;

	ut_ad(!mutex_own(&fc->mutex));

	if ( srv_flash_cache_adaptive_flushing ){
		n_flush = fc_flush_to_disk_adaptive(start_offset,&c_flush,do_full_io);
	} else{
		n_flush = fc_flush_to_disk_estimate(start_offset,&c_flush,do_full_io);
	}

	if ( n_flush > 0 ){
		ut_ad(n_flush >= c_flush);

		flash_cache_mutex_enter();
		fc->flush_off =  ( fc->flush_off + n_flush ) % fc_get_size();
		if ( fc->flush_off < start_offset ){
			fc->flush_round++;
		}

		fc_log_update(FALSE);
		fc_log_commit();
		flash_cache_mutex_exit();
	}

#ifdef UNIV_FLASH_DEBUG
	buf_flush_flash_cache_validate();
#endif
	
	return c_flush;

}

/**********************************************************************//**
Sync flash cache hash table from LRU remove page opreation */ 
static
void
fc_LRU_sync_hash_table(
/*==========================*/
	fc_block_t* b, /*!< flash cache block to be removed */
	buf_page_t* bpage /*!< frame to be written */
){
	/* block to be written */
	fc_block_t* b2;

	ut_ad(mutex_own(&fc->mutex));

	b2 = &fc->block[fc->write_off];

	if ( b != NULL ){
		HASH_DELETE(fc_block_t,hash,fc->hash_table,
			buf_page_address_fold(b->space, b->offset),
			b);
		b->state = BLOCK_NOT_USED;
		srv_flash_cache_used = srv_flash_cache_used - 1;
#ifdef UNIV_FLASH_DEBUG
		ut_print_timestamp(stderr);
		fprintf(stderr,"	InnoDB: lru - %lu, %lu.\n",b->space,b->offset);
#endif
	}

	if ( b2->state != BLOCK_NOT_USED ){
		HASH_DELETE(fc_block_t,hash,fc->hash_table,
			buf_page_address_fold(b2->space, b2->offset),
			b2);
		srv_flash_cache_used = srv_flash_cache_used - 1;
#ifdef UNIV_FLASH_DEBUG
		ut_print_timestamp(stderr);
		fprintf(stderr,"	InnoDB: lru - %lu, %lu.\n",b2->space,b2->offset);
#endif
	}

	b2->space = bpage->space;
	b2->offset = bpage->offset;
	b2->state = BLOCK_READ_CACHE;
	/* insert to hash table */
	HASH_INSERT(fc_block_t,hash,fc->hash_table,
		buf_page_address_fold(bpage->space, bpage->offset),
		b2);
	srv_flash_cache_used = srv_flash_cache_used + 1;
#ifdef UNIV_FLASH_DEBUG
	ut_print_timestamp(stderr);
	fprintf(stderr,"	InnoDB: lru + %lu, %lu.\n",b2->space,b2->offset);
#endif
}


/**********************************************************************//**
Check whether migration operation possible */ 
static
ibool
fc_LRU_flash_cache_avaliable(
/*=========================*/
)
{
	ut_ad(mutex_own(&fc->mutex));

	if ( fc->write_round == fc->flush_round ){
		return TRUE;
	}
	else{
		if ( fc->write_off + 1 < fc->flush_off ) {
			return TRUE;
		}
	}
	return FALSE;
}

/**********************************************************************//**
Move to flash cache if possible */
UNIV_INTERN
void
fc_LRU_move(
/*=========================*/
	buf_page_t* bpage	/*!< in: page flush out from buffer pool */
)
{
	fc_block_t* b;
    page_t*	page;
	ulint ret ;
	ulint write_offset;
	ulint zip_size;

	ut_ad(!mutex_own(&fc->mutex));

	if ( recv_no_ibuf_operations ){
		return;
	}

	zip_size = fil_space_get_zip_size(bpage->space);
	if (zip_size == ULINT_UNDEFINED){
		/* table has been droped, do not need move to flash cache */
		return;
	}

	if (zip_size){
		ut_ad(bpage->zip.data);
		page = bpage->zip.data;
        if(buf_page_is_corrupted(page,zip_size)){
            ut_print_timestamp(stderr);
            fprintf(stderr," InnoDB: compressed page is corrupted in LRU_move.\n");
            ut_error;
        }
	}
	else{
		page = ((buf_block_t*) bpage)->frame;
	}

	if ( fil_page_get_type(page) != FIL_PAGE_INDEX
		&& fil_page_get_type(page) != FIL_PAGE_INODE ){
			return;
	}
	
retry:
	flash_cache_mutex_enter();
	flash_cache_hash_mutex_enter(bpage->space,bpage->offset);	
	/* search the same space offset in hash table */
	HASH_SEARCH(hash,fc->hash_table,
		buf_page_address_fold(bpage->space,bpage->offset),
		fc_block_t*,b,
		ut_ad(1),
		bpage->space == b->space && bpage->offset == b->offset);

	write_offset = fc->write_off;

	if ( fc->block[fc->write_off].is_aio_reading ){
		srv_flash_cache_wait_for_aio++;
		flash_cache_hash_mutex_exit(bpage->space,bpage->offset);
	}
	else if (
             b == NULL
             && (bpage->access_time != 0 || zip_size)
             && srv_flash_cache_enable_migrate
             ){
		/* 
			flash cache migrate:
			move page not changed in buffer pool to flash cache block 
		*/
		if ( !fc_LRU_flash_cache_avaliable()  ){
			ut_print_timestamp(stderr);
			fprintf(stderr,"	InnoDB: sleep for free space.write offset %lu, flush offset %lu.Thread id is %lu.\n",
                    fc->write_off,fc->flush_off,(ulong)os_thread_get_curr_id());
			flash_cache_hash_mutex_exit(bpage->space,bpage->offset);	
			flash_cache_mutex_exit();
			os_thread_sleep(100000);
			goto retry;
		}
		fc_LRU_sync_hash_table(NULL,bpage);
		srv_flash_cache_write++;
		srv_flash_cache_migrate++;
		flash_cache_hash_mutex_exit(bpage->space,bpage->offset);
		ret = fil_io(OS_FILE_WRITE,TRUE,FLASH_CACHE_SPACE,0,fc->write_off,0,zip_size ? zip_size : UNIV_PAGE_SIZE,page,NULL);
		if ( ret != DB_SUCCESS ){
			ut_print_timestamp(stderr);
			fprintf(stderr,"	InnoDB: Error to migrate from buffer pool to flash cache, space:%u, offset %u",bpage->space,bpage->offset);
			ut_error;
		}
		fc->write_off = ( fc->write_off + 1 ) % fc_get_size();
		if ( write_offset > fc->write_off ){
			fc->write_round = fc->write_round + 1;
		}
	}
	else if ( b && srv_flash_cache_enable_move ){
		/* 
			flash cache move:
			move page already in flash cache block to new location
			for the sake of geting more high read ratio 
		*/
		if ( (((fc->write_off > b->fil_offset) && (fc->write_off - b->fil_offset ) >= FLASH_CACHE_MOVE_HIGH_LIMIT)
			 || ((fc->write_off < b->fil_offset) && ( b->fil_offset - fc->write_off ) <= FLASH_CACHE_MOVE_LOW_LIMIT))
				&& fc_LRU_flash_cache_avaliable()
				&& b->state != BLOCK_READY_FOR_FLUSH
				&& (bpage->space != 0 && bpage->offset != 0) ){

			ut_ad( b->state == BLOCK_FLUSHED || b->state == BLOCK_READ_CACHE );
			fc_LRU_sync_hash_table(b,bpage);
			srv_flash_cache_write++;
			srv_flash_cache_move++;
			flash_cache_hash_mutex_exit(bpage->space,bpage->offset);
			ret = fil_io(OS_FILE_WRITE,TRUE,FLASH_CACHE_SPACE,0,fc->write_off,0,zip_size ? zip_size : UNIV_PAGE_SIZE,page,NULL);
			if ( ret != DB_SUCCESS ){
				ut_print_timestamp(stderr);
				fprintf(stderr,"	InnoDB: Error to migrate from buffer pool to flash cache, space:%lu, offset %lu",
						(unsigned long)bpage->space, (unsigned long)bpage->offset);
				ut_error;
			}
			fc->write_off = ( fc->write_off + 1 ) % fc_get_size();
			if ( write_offset > fc->write_off ){
				fc->write_round = fc->write_round + 1;
			}
		}
		else{
			flash_cache_hash_mutex_exit(bpage->space,bpage->offset);
		}
	}
	else{
		flash_cache_hash_mutex_exit(bpage->space,bpage->offset);
	}
	flash_cache_mutex_exit();
#ifdef UNIV_FLASH_DEBUG
	buf_flush_flash_cache_validate();
#endif
}

/********************************************************************//**
Read page from flash cache block, if not found in flash cache, read from disk.
Note: ibuf page must read in aio mode to avoid deadlock
@return DB_SUCCESS is success, 1 if read request is issued. 0 if it is not */
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
)
{
	ulint err;

	fc_block_t* b;
	ibool using_ibuf_aio = FALSE;
#ifdef UNIV_FLASH_DEBUG
	fc_block_t* b2;
#endif
	
	if (fc == NULL){
		if (zip_size){
			err = fil_io(OS_FILE_READ | wake_later,
			      sync, space, zip_size, offset, 0, zip_size,
			      buf, bpage);
		}
		else{
			err = fil_io(OS_FILE_READ | wake_later,
						sync, space, 0, offset, 0, UNIV_PAGE_SIZE,
						buf, bpage);
		}
		return err;
	}

	ut_ad(!mutex_own(&fc->mutex));

	flash_cache_hash_mutex_enter(space,offset);		
	HASH_SEARCH(hash,fc->hash_table,
		buf_page_address_fold(space,offset),
		fc_block_t*,b,
		ut_ad(1),
		space == b->space && offset == b->offset);
	if ( b ){
		ulint page_type;
#ifdef UNIV_DEBUG
		ulint _offset;
		ulint _space;
#endif
 		ut_a( b->state != BLOCK_NOT_USED );
		srv_flash_cache_read++;
		ut_ad( b->is_aio_reading == FALSE );
		ut_ad( bpage->fc_block == NULL );

		if(b->fil_offset >= fc->size)
		{
			memcpy(buf, fc->move_migrate_pages + UNIV_PAGE_SIZE * (b->fil_offset - fc->size),
					zip_size ? zip_size : UNIV_PAGE_SIZE);
			page_type = fil_page_get_type(buf);
			if ( page_type == FIL_PAGE_INDEX ){
				page_type = 1;
			}
			srv_flash_cache_read_detail[page_type]++;
			flash_cache_hash_mutex_exit(space,offset);
            if (!sync){
                buf_page_io_complete(bpage, TRUE);
            }
			return DB_SUCCESS;
		}
		
		b->is_aio_reading = TRUE;
		if ( ibuf_bitmap_page(zip_size,b->offset) || trx_sys_hdr_page(b->space,b->offset) ){
			sync = TRUE;
		}
		if ( !sync ){
			bpage->fc_block = b;
			srv_flash_cache_aio_read++;
		}
		flash_cache_hash_mutex_exit(space,offset);		

		if ( !recv_no_ibuf_operations && ibuf_page(b->space, zip_size, b->offset, NULL)){
			using_ibuf_aio = TRUE;
		}
		if ( using_ibuf_aio ){
			err = fil_io(OS_FILE_READ | wake_later | OS_FORCE_IBUF_AIO,
				sync, FLASH_CACHE_SPACE, 0, b->fil_offset, 0, zip_size ? zip_size : UNIV_PAGE_SIZE,
				buf, bpage);
		}
		else{
			err = fil_io(OS_FILE_READ | wake_later ,
				sync, FLASH_CACHE_SPACE, 0, b->fil_offset, 0, zip_size ? zip_size : UNIV_PAGE_SIZE,
				buf, bpage);
		}

		if ( sync ){
			flash_cache_hash_mutex_enter(space,offset);
			b->is_aio_reading = FALSE;
			flash_cache_hash_mutex_exit(space,offset);
			page_type = fil_page_get_type(buf);
			if ( page_type == FIL_PAGE_INDEX ){
				page_type = 1;
			}
			srv_flash_cache_read_detail[page_type]++;
#ifdef UNIV_DEBUG
			_offset = mach_read_from_4((byte*)buf + FIL_PAGE_OFFSET);
			_space = mach_read_from_4((byte*)buf + FIL_PAGE_ARCH_LOG_NO_OR_SPACE_ID);
			if ( _offset != offset || _space != space ){
				ut_error;
			}
#endif
		}
	}
	else{
		flash_cache_hash_mutex_exit(space,offset);
		err = fil_io(OS_FILE_READ | wake_later,
					sync, space, zip_size, offset, 0, zip_size ? zip_size : UNIV_PAGE_SIZE,
					buf, bpage);
        if (zip_size && sync){
            /* 
             use write-alloc for compressed page, 
             since compressed page is marked corrupted when swap out from buffer pool
            */
            srv_flash_cache_lru_move_batch ? 
				fc_LRU_move_optimization(bpage): fc_LRU_move(bpage);
        }
	}

	return err;

}

/********************************************************************//**
Compelete flash cache async read. */
UNIV_INTERN
void
fc_compelete_read(
/*==============*/
	buf_page_t* bpage	/*!< page to compelete */
)
{
	ulint page_type;

	ut_ad(!mutex_own(&fc->mutex));
	ut_ad(((fc_block_t*)(bpage->fc_block))->is_aio_reading);
	
	flash_cache_hash_mutex_enter(bpage->space,bpage->offset);

	((fc_block_t*)(bpage->fc_block))->is_aio_reading = FALSE;
	bpage->fc_block = NULL;

	flash_cache_hash_mutex_exit(bpage->space,bpage->offset);

	if ( bpage->zip.data ){
		page_type = fil_page_get_type(bpage->zip.data);
        /*
         use write-alloc for compressed page,
         since compressed page is marked corrupted when swap out from buffer pool
         */
        	srv_flash_cache_lru_move_batch ? 
				fc_LRU_move_optimization(bpage): fc_LRU_move(bpage);
	}
	else{
		page_type = fil_page_get_type(((buf_block_t*)bpage)->frame);
	}
	if ( page_type == FIL_PAGE_INDEX ){
		page_type = 1;
	}
	srv_flash_cache_read_detail[page_type]++;

}

/********************************************************************//**
backup pages haven't flushed to disk from flash cache file. 
@return number of pages backuped if success */
static inline
ulint
fc_backup(ibool* success)/* out */
{
	char* backup_dir;
	char* backup_file_path;
	char* backup_file_path_final;
	ulint backup_file_path_len;
	ulint backup_file_path_final_len;
	ulint n_backuped_pages = 0;
	os_file_t backup_fd;
	ibool flag = FALSE;
	byte* unaligned_buf;
	byte* buf;
	fc_block_t** sorted_fc_blocks;
	ulint n_f_2_w;
	ulint i;
	ulint n_ready_for_flush_pages;
	ulint flush_off;
	ulint write_off;
	ulint offset_high;
	ulint offset_low;
	ulint cur_collected_pages;	

	flush_off = fc->flush_off;
	write_off = fc->write_off;
	n_f_2_w = (write_off >= flush_off) ? (write_off - flush_off):(fc->size - flush_off + write_off);
	if(n_f_2_w)
	{
		sorted_fc_blocks = ut_malloc(n_f_2_w * sizeof(*sorted_fc_blocks));
		n_ready_for_flush_pages = 0;
		for(i = flush_off; i != write_off; i = (i + 1) % fc->size)
		{
			if(fc->block[i].state != BLOCK_READY_FOR_FLUSH)
				continue;
			sorted_fc_blocks[n_ready_for_flush_pages++] = fc->block + i;
		}
		if(n_ready_for_flush_pages)
		{
			ut_a(n_ready_for_flush_pages <= n_f_2_w);
			ut_print_timestamp(stderr);
			fprintf(stderr, " Inoodb: flash cache is backuping...\n");
			
			fc_block_sort(sorted_fc_blocks, n_ready_for_flush_pages, ASCENDING);

			backup_dir = srv_flash_cache_backup_dir ? srv_flash_cache_backup_dir : srv_data_home;	
			backup_file_path_len = strlen(backup_dir) + sizeof("/ib_fc_backup_creating");
			backup_file_path_final_len = strlen(backup_dir) + sizeof("/ib_fc_backup");
			backup_file_path = ut_malloc(backup_file_path_len);
			backup_file_path_final = ut_malloc(backup_file_path_final_len);
			ut_snprintf(backup_file_path, backup_file_path_len, "%s/%s", backup_dir, "ib_fc_backup_creating");
			ut_snprintf(backup_file_path_final, backup_file_path_final_len, "%s/%s", backup_dir, "ib_fc_backup");
			srv_normalize_path_for_win(backup_file_path);
			srv_normalize_path_for_win(backup_file_path_final);
	
			backup_fd = os_file_create(innodb_file_data_key, backup_file_path, OS_FILE_CREATE, OS_FILE_NORMAL, OS_DATA_FILE, &flag);
			if(!flag) {
				ut_print_timestamp(stderr);
				fprintf(stderr, " Inoodb: Error: create file '%s' failed, check if it has existed already,\n", backup_file_path);
				fprintf(stderr, " Inoodb: Error: which means the flash cache backup  may have started already.\n");
				ut_free(backup_file_path);
				ut_free(backup_file_path_final);
				ut_free(sorted_fc_blocks);
				*success = FALSE;
				return 0;
			}

			unaligned_buf = ut_malloc((FSP_EXTENT_SIZE + 1)* UNIV_PAGE_SIZE);
			buf = ut_align(unaligned_buf, UNIV_PAGE_SIZE);
			i = 0;
			while(i < n_ready_for_flush_pages)  /* don't ++i, or some page will not be backuped for i acutally increased 2 times*/
			{
				for(cur_collected_pages = 0; cur_collected_pages < FSP_EXTENT_SIZE && i < n_ready_for_flush_pages; ++i)
				{
					if(sorted_fc_blocks[i]->state != BLOCK_READY_FOR_FLUSH)
						continue;
					fil_io(OS_FILE_READ, TRUE, FLASH_CACHE_SPACE, 0, sorted_fc_blocks[i]->fil_offset,
						   0, UNIV_PAGE_SIZE, buf + cur_collected_pages * UNIV_PAGE_SIZE, NULL);
					++cur_collected_pages;
				}
				
				offset_high = (n_backuped_pages >> (32 - UNIV_PAGE_SIZE_SHIFT));
				offset_low  = ((n_backuped_pages << UNIV_PAGE_SIZE_SHIFT) & 0xFFFFFFFFUL);
				if(!(os_file_write(backup_file_path, backup_fd, buf, offset_low, offset_high, UNIV_PAGE_SIZE * cur_collected_pages))) {
					if(os_file_get_last_error(FALSE) == OS_FILE_DISK_FULL) {
						ut_print_timestamp(stderr);
						fprintf(stderr, " Inoodb: Error: disk is full, reset srv_flash_cache_backup_dir to another\n");
						fprintf(stderr, "Inoodb: Error: disk or partion where space is large enough to store the flash\n"); 
						fprintf(stderr, "Inoodb: Error: backup pages, and then reset innodb_flash_cache_backup to TRUE to backup again.\n");
						}
					else {
						ut_print_timestamp(stderr);
						fprintf(stderr, " Inoodb: Error: error occured while backuping unflushed flash cache pages\n");
						fprintf(stderr, "Inoodb: Error: try reset innodb_flash_cache_backup to TRUE if wanna do backup again.\n");
						}
					ut_free(sorted_fc_blocks);
					ut_free(unaligned_buf);
					ut_free(backup_file_path);
					ut_free(backup_file_path_final);
					*success = FALSE;
					os_file_close(backup_fd);
					os_file_delete(backup_file_path);
					return 0;
					}
				n_backuped_pages += cur_collected_pages;
								
			}	
			
			os_file_flush(backup_fd);
			os_file_close(backup_fd);
			os_file_rename(innodb_file_data_key,backup_file_path,backup_file_path_final);
			ut_free(sorted_fc_blocks);
			ut_free(unaligned_buf);
			ut_free(backup_file_path);
			ut_free(backup_file_path_final);
			*success = TRUE;
			ut_print_timestamp(stderr);
			fprintf(stderr, " Inoodb: flash cache completed backup.\n");
			return n_backuped_pages;	
		}
		else
		{
			ut_free(sorted_fc_blocks);
		}
	}
	
	/*no pages need to be backuped*/
	ut_print_timestamp(stderr);
	fprintf(stderr, " Inoodb: no pages in flash cache need to be backuped\n");
	*success = TRUE;
	return 0;

}

/********************************************************************//**
fc_backup_thread 
*/
UNIV_INTERN
os_thread_ret_t
fc_backup_thread(void* args)
{
	ibool sucess;
	fc_backup(&sucess);
	if(!sucess)
	{
		ut_print_timestamp(stderr);
		fprintf(stderr, " Inoodb: Error: backup flash cache file failed\n");
	}
	srv_flash_cache_backuping = FALSE;
	os_thread_exit(NULL);
}



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
)
{
		
	time_t cur_time = ut_time();
	lint cdiff = fc->write_off -  fc->flush_off;
	ulint fc_read_point = 0;
	ulint z;
	for( z=0; z<=FIL_PAGE_TYPE_ZBLOB2; z++){
		fc_read_point = fc_read_point + srv_flash_cache_read_detail[z];
	}
	if ( cdiff < 0 ){
		cdiff = cdiff + fc_get_size();
	}
	fputs("----------------------\n"
	"FLASH CACHE INFO\n"
	"----------------------\n", file);
	fprintf(file,	"flash cache thread status: %s \n"
					"flash cache size: %lu \n"
					"flash cache location is: %lu(%lu), flush to %lu(%lu), distance %lu (%.2f%%), used %lu(%.2f%%), wait aio %lu.\n"
					"flash cache reads %lu:, aio read %lu, writes %lu, flush %lu(%lu), migrate %lu, move %lu\n"
					"FIL_PAGE_INDEX reads: %lu(%.2f%%): writes: %lu, flush: %lu, merge raio %.2f%%\n"
					"FIL_PAGE_INODE reads: %lu(%.2f%%): writes: %lu, flush: %lu, merge raio %.2f%%\n"
					"FIL_PAGE_UNDO_LOG reads: %lu(%.2f%%): writes: %lu, flush: %lu, merge raio %.2f%%\n"
					"FIL_PAGE_TYPE_SYS reads: %lu(%.2f%%): writes: %lu, flush: %lu, merge raio %.2f%%\n"
					"FIL_PAGE_TYPE_TRX_SYS reads: %lu(%.2f%%): writes: %lu, flush: %lu, merge raio %.2f%%\n"
					"FIL_PAGE_OTHER reads: %lu(%.2f%%): writes: %lu, flush: %lu\n"
					"flash cache read hit ratio %.2f%% in %lu second(total %.2f%%), merge write ratio %.2f%%\n"
					"flash cache %.2f reads/s, %.2f writes/s. %.2f flush/s, %.2f merge writes/s, %.2f migrate/s, %.2f move/s\n",
					srv_flash_cache_thread_op_info,
					(ulong)fc_get_size(),
					(ulong)fc->write_off,
					(ulong)fc->write_round,
					(ulong)fc->flush_off,
					(ulong)fc->flush_round,
					(ulong)cdiff,
					(100.0*cdiff)/fc_get_size(),
					(ulong)srv_flash_cache_used,
					(100.0*srv_flash_cache_used)/fc_get_size(),
					(ulong)srv_flash_cache_wait_for_aio,
					(ulong)srv_flash_cache_read,
					(ulong)srv_flash_cache_aio_read,
					(ulong)srv_flash_cache_write,
					(ulong)srv_flash_cache_flush,
					(ulong)srv_flash_cache_merge_write,
					(ulong)srv_flash_cache_migrate,
					(ulong)srv_flash_cache_move,
					(ulong)srv_flash_cache_read_detail[1],(100.0*srv_flash_cache_read_detail[1])/(fc_read_point),
					(ulong)srv_flash_cache_write_detail[1],(ulong)srv_flash_cache_flush_detail[1],100.0-(100.0*srv_flash_cache_flush_detail[1])/srv_flash_cache_write_detail[1],
					(ulong)srv_flash_cache_read_detail[FIL_PAGE_INODE],(100.0*srv_flash_cache_read_detail[FIL_PAGE_INODE])/(fc_read_point),
					(ulong)srv_flash_cache_write_detail[FIL_PAGE_INODE],(ulong)srv_flash_cache_flush_detail[FIL_PAGE_INODE],100.0-(100.0*srv_flash_cache_flush_detail[FIL_PAGE_INODE])/srv_flash_cache_write_detail[FIL_PAGE_INODE],
					(ulong)srv_flash_cache_read_detail[FIL_PAGE_UNDO_LOG],(100.0*srv_flash_cache_read_detail[FIL_PAGE_UNDO_LOG])/(fc_read_point),
					(ulong)srv_flash_cache_write_detail[FIL_PAGE_UNDO_LOG],(ulong)srv_flash_cache_flush_detail[FIL_PAGE_UNDO_LOG],100.0-(100.0*srv_flash_cache_flush_detail[FIL_PAGE_UNDO_LOG])/srv_flash_cache_write_detail[FIL_PAGE_UNDO_LOG],
					(ulong)srv_flash_cache_read_detail[FIL_PAGE_TYPE_SYS],(100.0*srv_flash_cache_read_detail[FIL_PAGE_TYPE_SYS])/(fc_read_point),
					(ulong)srv_flash_cache_write_detail[FIL_PAGE_TYPE_SYS],(ulong)srv_flash_cache_flush_detail[FIL_PAGE_TYPE_SYS],100.0-(100.0*srv_flash_cache_flush_detail[FIL_PAGE_TYPE_SYS])/srv_flash_cache_write_detail[FIL_PAGE_TYPE_SYS],
					(ulong)srv_flash_cache_read_detail[FIL_PAGE_TYPE_TRX_SYS],(100.0*srv_flash_cache_read_detail[FIL_PAGE_TYPE_TRX_SYS])/(fc_read_point),
					(ulong)srv_flash_cache_write_detail[FIL_PAGE_TYPE_TRX_SYS],(ulong)srv_flash_cache_flush_detail[FIL_PAGE_TYPE_TRX_SYS],100.0-(100.0*srv_flash_cache_flush_detail[FIL_PAGE_TYPE_TRX_SYS])/srv_flash_cache_write_detail[FIL_PAGE_TYPE_TRX_SYS],
					(ulong)(srv_flash_cache_read_detail[FIL_PAGE_IBUF_FREE_LIST] + srv_flash_cache_read_detail[FIL_PAGE_TYPE_ALLOCATED]
								+ srv_flash_cache_read_detail[FIL_PAGE_IBUF_BITMAP] + srv_flash_cache_read_detail[FIL_PAGE_TYPE_FSP_HDR]
								+ srv_flash_cache_read_detail[FIL_PAGE_TYPE_XDES] + srv_flash_cache_read_detail[FIL_PAGE_TYPE_BLOB]
								+ srv_flash_cache_read_detail[FIL_PAGE_TYPE_ZBLOB] + srv_flash_cache_read_detail[FIL_PAGE_TYPE_ZBLOB2]
							),
					(100.*(srv_flash_cache_read_detail[FIL_PAGE_IBUF_FREE_LIST] + srv_flash_cache_read_detail[FIL_PAGE_TYPE_ALLOCATED]
								+ srv_flash_cache_read_detail[FIL_PAGE_IBUF_BITMAP] + srv_flash_cache_read_detail[FIL_PAGE_TYPE_FSP_HDR]
								+ srv_flash_cache_read_detail[FIL_PAGE_TYPE_XDES] + srv_flash_cache_read_detail[FIL_PAGE_TYPE_BLOB]
								+ srv_flash_cache_read_detail[FIL_PAGE_TYPE_ZBLOB] + srv_flash_cache_read_detail[FIL_PAGE_TYPE_ZBLOB2]
							))/(fc_read_point),
					(ulong)(srv_flash_cache_write_detail[FIL_PAGE_IBUF_FREE_LIST] + srv_flash_cache_write_detail[FIL_PAGE_TYPE_ALLOCATED]
								+ srv_flash_cache_write_detail[FIL_PAGE_IBUF_BITMAP] + srv_flash_cache_write_detail[FIL_PAGE_TYPE_FSP_HDR]
								+ srv_flash_cache_write_detail[FIL_PAGE_TYPE_XDES] + srv_flash_cache_write_detail[FIL_PAGE_TYPE_BLOB]
								+ srv_flash_cache_write_detail[FIL_PAGE_TYPE_ZBLOB] + srv_flash_cache_write_detail[FIL_PAGE_TYPE_ZBLOB2]
							),
					(ulong)(srv_flash_cache_flush_detail[FIL_PAGE_IBUF_FREE_LIST] + srv_flash_cache_flush_detail[FIL_PAGE_TYPE_ALLOCATED]
								+ srv_flash_cache_flush_detail[FIL_PAGE_IBUF_BITMAP] + srv_flash_cache_flush_detail[FIL_PAGE_TYPE_FSP_HDR]
								+ srv_flash_cache_flush_detail[FIL_PAGE_TYPE_XDES] + srv_flash_cache_flush_detail[FIL_PAGE_TYPE_BLOB]
								+ srv_flash_cache_flush_detail[FIL_PAGE_TYPE_ZBLOB] + srv_flash_cache_flush_detail[FIL_PAGE_TYPE_ZBLOB2]
							),
					(ulong)(page_read_delta == 0)?0:100.0*( srv_flash_cache_read - flash_cache_stat.n_pages_read ) / ( page_read_delta ),
					(ulong)difftime(cur_time,flash_cache_stat.last_printout_time),
					(ulong)(srv_flash_cache_read==0)?0:(100.0*srv_flash_cache_read)/(srv_buf_pool_reads + n_ra_pages_read ),
					(100.0*srv_flash_cache_merge_write)/(srv_flash_cache_write - srv_flash_cache_migrate - srv_flash_cache_move),
					( srv_flash_cache_read - flash_cache_stat.n_pages_read ) / difftime(cur_time,flash_cache_stat.last_printout_time),
					( srv_flash_cache_write - flash_cache_stat.n_pages_write ) / difftime(cur_time,flash_cache_stat.last_printout_time),
					( srv_flash_cache_flush - flash_cache_stat.n_pages_flush ) / difftime(cur_time,flash_cache_stat.last_printout_time),
					( srv_flash_cache_merge_write - flash_cache_stat.n_pages_merge_write ) / difftime(cur_time,flash_cache_stat.last_printout_time),
					( srv_flash_cache_migrate - flash_cache_stat.n_pages_migrate ) / difftime(cur_time,flash_cache_stat.last_printout_time),
					( srv_flash_cache_move - flash_cache_stat.n_pages_move ) / difftime(cur_time,flash_cache_stat.last_printout_time)
		);
	flash_cache_stat.flush_off = fc->flush_off;
	flash_cache_stat.flush_round = fc->flush_round;
	flash_cache_stat.write_off = fc->write_off;
	flash_cache_stat.write_round = fc->write_round;
	flash_cache_stat.n_pages_write = srv_flash_cache_write;
	flash_cache_stat.n_pages_flush = srv_flash_cache_flush;
	flash_cache_stat.n_pages_merge_write = srv_flash_cache_merge_write;
	flash_cache_stat.n_pages_read = srv_flash_cache_read;
	flash_cache_stat.n_pages_move = srv_flash_cache_move;
	flash_cache_stat.n_pages_migrate = srv_flash_cache_migrate;
	flash_cache_stat.last_printout_time = ut_time();
	return;
}


/**************** move and migrate ***************************************/
/*whether bpage should be moved in flash cache 
@return TRUE if need do move operation */
static inline
ibool
fc_need_move
(
	fc_block_t* b, /* 'returned' by HASH_SEARCH*/
	buf_page_t* bpage
)
{
	ut_ad(bpage);
	ut_ad(mutex_own(&fc->mutex));
	ut_ad(mutex_own(&fc->hash_mutex));

	return (b && b->fil_offset < fc->size && srv_flash_cache_enable_move &&
			(((fc->write_off > b->fil_offset) && (fc->write_off - b->fil_offset ) >= FLASH_CACHE_MOVE_HIGH_LIMIT)
			|| ((fc->write_off < b->fil_offset) && ( b->fil_offset - fc->write_off ) <= FLASH_CACHE_MOVE_LOW_LIMIT))
			&& b->state != BLOCK_READY_FOR_FLUSH && b->is_aio_reading == FALSE
			&& (bpage->space != 0 && bpage->offset != 0));
}

/*whether bpage should be migrate to flash cache 
@return TRUE if need do migrate operation */
static inline
ibool
fc_need_migrate(
	fc_block_t* b, /* 'returned' by HASH_SEARCH*/
	buf_page_t* bpage,
	ulint zip_size)
{
	ut_ad(bpage);
	ut_ad(mutex_own(&fc->mutex));
	ut_ad(mutex_own(&fc->hash_mutex));

	return (b == NULL && (bpage->access_time != 0 || zip_size) && srv_flash_cache_enable_migrate);
}

/* When fc->move_migrate_blocks is full, write it back to Flash Cache file */
static 
void 
fc_move_migrate_to_flash_cache()
{
	ulint i;
	ulint zip_size;
	fc_block_t* b;
	fc_block_t* b2;
	ulint		write_start = ULINT32_UNDEFINED;
	ulint		write_end;
	
	ut_ad(mutex_own(&fc->mutex));
	ut_ad(!mutex_own(&fc->hash_mutex)); 
	ut_ad(fc->move_migrate_next_write_pos == fc->move_migrate_n_pages);

	/* this loop is used to write pages in fc->move_migrate_pages to Flash Cache file, but still keep 
	fc->move_migrate_blocks in fc->hash_table and release fc->hash_mutex each round to make 
	fc_read_page won't be blocked for too long */
	for(i = 0; i < fc->move_migrate_n_pages; ++i)
	{
		b2 = fc->move_migrate_blocks + i;
		if(b2->state == BLOCK_NOT_USED)	/* have been deleted from fc->hash_table because of double write */
			continue;
		ut_ad(b2->state == BLOCK_READ_CACHE && b2->is_aio_reading == FALSE); 
		zip_size = fil_space_get_zip_size(b2->space);
		if(ULINT_UNDEFINED == zip_size) /*table has been droped, no write, but need delete from fc->hash_table latter*/
			continue;
		
		/* looks for the write pos */
		while(((fc->write_round == 1 + fc->flush_round) ? (fc->write_off < fc->flush_off) : 1)
			 && fc->block[fc->write_off].is_aio_reading)
		{
			++srv_flash_cache_wait_for_aio;
			++fc->write_off;
			if(fc->write_off == fc->size)
			{
				fc->write_off = 0;
				++fc->write_round;
			}
		}
		
		if(write_start == ULINT32_UNDEFINED)
			write_start = fc->write_off;
		
		if((fc->write_round == 1 +  fc->flush_round) && (fc->write_off == fc->flush_off))
		{
			write_end = fc->write_off;
			goto cannot_write;	/* cann't write, we choose just ignore the move/migrate pages from this one to the 
								fc->move_migrate_n_pages - 1*/
		}

		
		/* here we got the write pos */
		b = fc->block + fc->write_off;
		if(b->state != BLOCK_NOT_USED)
		{
			ut_ad(b->state == BLOCK_READ_CACHE || b->state == BLOCK_FLUSHED);
			flash_cache_hash_mutex_enter(b->space,b->offset);
			HASH_DELETE(fc_block_t,hash,fc->hash_table,
						buf_page_address_fold(b->space, b->offset),b);
			--srv_flash_cache_used;
			b->state = BLOCK_NOT_USED;
			flash_cache_hash_mutex_exit(b->space, b->offset);
		}
		b->space = b2->space;
		b->offset = b2->offset;
		b->state = BLOCK_NEED_INSERT;
		/*b->is_aio_reading = FALSE; */ /* alerady in this status */
	
		++srv_flash_cache_write;

		/* using AIO write */
		fil_io(OS_FILE_WRITE, FALSE, FLASH_CACHE_SPACE, 0, fc->write_off, 0, UNIV_PAGE_SIZE, 
				fc->move_migrate_pages + i* UNIV_PAGE_SIZE, NULL); /* even page is zipped, also write using 16K */

		++fc->write_off;
		if(fc->write_off == fc->size)
		{
			fc->write_off = 0;
			++fc->write_round;
		}
		
	}
	write_end = fc->write_off; /* if reach here, indicates all valid pages in fc->move_migrate_pages has been written*/
	
cannot_write:  
	fc_sync_datafiles();
	flash_cache_hash_mutex_enter(0, 0);  /* no IO operation, can be finished quickly*/

	/* delete all fc->move_migrate_blocks (even some of them hasn't written to Flash Cache
	file because of cannot_write) from fc->hash_table */
	for(i = 0; i < fc->move_migrate_n_pages; ++i)
	{
		b2 = fc->move_migrate_blocks + i;
		if(b2->state == BLOCK_NOT_USED)	/* have been deleted from fc->hash_table because of double write */
			continue;
		ut_ad(b2->state == BLOCK_READ_CACHE && b2->is_aio_reading == FALSE);
		
		HASH_DELETE(fc_block_t,hash,fc->hash_table,
					buf_page_address_fold(b2->space, b2->offset),b2);
		b2->state = BLOCK_NOT_USED;
	}
	fc->move_migrate_next_write_pos = 0;

	/* insert blocks with state equals to BLOCK_NEED_INSERT in range [write_start, write_end)  to fc->hash_table */
	while(write_start != write_end)
	{
		b = fc->block + write_start;
		if(b->state == BLOCK_NEED_INSERT)
		{
			++srv_flash_cache_used;
			b->state = BLOCK_READ_CACHE;
			HASH_INSERT(fc_block_t,hash,fc->hash_table,
						buf_page_address_fold(b->space, b->offset), b);
		}
		write_start = (write_start + 1) % fc->size;
	}

	/*flash_cache_mutex_exit(); */   /* release fc->hash_mutex in fc_LRU_move_optimization */
}
	
UNIV_INTERN
void
fc_LRU_move_optimization(buf_page_t * bpage)
{
	fc_block_t* b; /* used in HASH_SEARCH*/
	fc_block_t* b2; /*used in fc->move_migrate_blocks */
    page_t*	page;
	ulint zip_size;

	ut_ad(!mutex_own(&fc->mutex));

	if ( recv_no_ibuf_operations ){
		return;
	}

	zip_size = fil_space_get_zip_size(bpage->space);
	if (zip_size == ULINT_UNDEFINED){
		/* table has been droped, do not need move to flash cache */
		return;
	}

	if (zip_size){
		ut_ad(bpage->zip.data);
		page = bpage->zip.data;
        if(buf_page_is_corrupted(page,zip_size)){
            ut_print_timestamp(stderr);
            fprintf(stderr," InnoDB: compressed page is corrupted in LRU_move.\n");
            ut_error;
        }
	}
	else{
		page = ((buf_block_t*) bpage)->frame;
	}

	if ( fil_page_get_type(page) != FIL_PAGE_INDEX
		&& fil_page_get_type(page) != FIL_PAGE_INODE ){
			return;
	}
	
	
	flash_cache_mutex_enter();
	flash_cache_hash_mutex_enter(bpage->space,bpage->offset);

	ut_a(fc->move_migrate_next_write_pos < fc->move_migrate_n_pages);

	/* search the same (space, offset) in hash table */
	HASH_SEARCH(hash,fc->hash_table,
				buf_page_address_fold(bpage->space,bpage->offset),
				fc_block_t*,b,ut_ad(1),bpage->space == b->space && bpage->offset == b->offset);
	
	if (fc_need_move(b, bpage)){ /* flash cache move: first do delete*/
		ut_ad( b->state == BLOCK_FLUSHED || b->state == BLOCK_READ_CACHE );
		ut_d(fprintf(stderr, "move from offset %u\n", b->fil_offset));
		HASH_DELETE(fc_block_t,hash,fc->hash_table,
					buf_page_address_fold(b->space, b->offset),b);
		b->state = BLOCK_NOT_USED;
		--srv_flash_cache_used;
		++srv_flash_cache_move;
	}
	else if (fc_need_migrate(b, bpage, zip_size)){ /* migrate */
		srv_flash_cache_migrate++;
	}
	else {
		flash_cache_hash_mutex_exit(bpage->space,bpage->offset);
		flash_cache_mutex_exit();
		return ;
	}
		
	/* common operation for both move and migrate */
	b2 = fc->move_migrate_blocks + fc->move_migrate_next_write_pos;
	ut_ad(b2->state == BLOCK_NOT_USED);
	b2->space = bpage->space;
	b2->offset = bpage->offset;
	b2->state = BLOCK_READ_CACHE;
	/*b2->is_aio_reading = FALSE; */ /* need not do this, for after fc_create, never will it  be modified*/
	HASH_INSERT(fc_block_t,hash,fc->hash_table,
				buf_page_address_fold(b2->space, b2->offset), b2);
	memcpy(fc->move_migrate_pages + fc->move_migrate_next_write_pos * UNIV_PAGE_SIZE, 
			    page, zip_size ? zip_size : UNIV_PAGE_SIZE);
	++fc->move_migrate_next_write_pos;
	
	if(fc->move_migrate_next_write_pos == fc->move_migrate_n_pages)
	{
		flash_cache_hash_mutex_exit(bpage->space,bpage->offset); /* can do fc_read_page but not others*/
		fc_move_migrate_to_flash_cache(); /*fc->move_migrate_next_write_pos will be reset to 0, and both mutex will be freed*/
	}
	flash_cache_hash_mutex_exit(bpage->space,bpage->offset);
	flash_cache_mutex_exit();
	
}

/********************************************************************//**
return TRUE if no need to flush from flash cache to disk
@return TRUE if offset and round are equal.*/
UNIV_INTERN
my_bool
fc_finish_flush(){
    ulint ret = FALSE;
    
    flash_cache_mutex_enter();
    if ( fc->flush_off == fc->write_off
        && fc->flush_round == fc->write_round){
        ret = TRUE;
    }
    flash_cache_mutex_exit();
    
    return ret;
}
	

