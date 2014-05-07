/**************************************************//**
@file fc/fc0fill.c
Flash Cache(L2 Cache) for InnoDB

Created	24/10/2013 Thomas Wen (wenzhenghu.zju@gmail.com)
*******************************************************/
#include "fc0fill.h"
#include "fc0log.h"

#ifdef UNIV_NONINL
#include "fc0fill.ic"
#endif

#include "log0recv.h"
#include "ibuf0ibuf.h"


/**********************************************************************//**
Sync L2 Cache hash table from LRU remove page opreation */ 
UNIV_INTERN
void
fc_LRU_sync_hash_table(
/*==========================*/
	buf_page_t* bpage) /*!< in: frame to be written to L2 Cache */
{
	/* block to be written */
	fc_block_t* wf_block;

	ut_ad(mutex_own(&fc->mutex));

	/* the fc->write_off has not update by fc_block_find_replaceable, is just the block we find */
	wf_block = &fc->block[fc->write_off];

	ut_a(wf_block->state == BLOCK_NOT_USED);

	/* the block have attached in fc_block_find_replaceable,
	  just need update space, offset and statement */
	wf_block->space = bpage->space;
	wf_block->offset = bpage->offset;
	
	wf_block->state = BLOCK_READ_CACHE;

	/* insert to hash table */
	fc_block_insert_into_hash(wf_block);

	srv_flash_cache_used += fc_block_get_data_size(wf_block);
	srv_flash_cache_used_nocompress += fc_block_get_orig_size(wf_block);

#ifdef UNIV_FLASH_DEBUG
	ut_print_timestamp(stderr);
	fprintf(stderr,"	InnoDB: lru + %lu, %lu.\n", wf_block->space, wf_block->offset);
#endif

}

/**********************************************************************//**
Move to L2 Cache if possible */
UNIV_INTERN
void
fc_LRU_move(
/*=========================*/
	buf_page_t* bpage)	/*!< in: page LRU out from buffer pool */
{
	fc_block_t *old_block;

    page_t*	page;
	ulint ret ;
	ulint zip_size;
	ulint blk_size;
	ulint fc_blk_size;
	ulint block_offset;
	ulint byte_offset;
	ulint need_compress;
	fc_block_t* wf_block = NULL;
	byte* zip_buf_unalign = NULL;
	byte* zip_buf = NULL;
	ulint move_flag = 0;

	ut_ad(!mutex_own(&fc->mutex));

	if (recv_no_ibuf_operations) {
		return;
	}
	
	zip_size = fil_space_get_zip_size(bpage->space);
	if (zip_size == ULINT_UNDEFINED) {
		/* table has been droped, do not need move to L2 Cache */

#ifdef UNIV_FLASH_CACHE_TRACE
		ut_print_timestamp(fc->f_debug);
		fprintf(fc->f_debug, "space:%lu is droped, the page(%lu, %lu) will not move to L2 Cache.\n",
			(ulong)bpage->space, (ulong)bpage->space, (ulong)bpage->offset);
#endif
		return;
	}

	if (zip_size) {
		ut_a(bpage->zip.data);
		page = bpage->zip.data;

		
	} else {
		page = ((buf_block_t*)bpage)->frame;
	}

#ifdef UNIV_FLASH_CACHE_TRACE
	if (buf_page_is_corrupted(page, zip_size)) {
		ut_print_timestamp(stderr);
		fprintf(stderr," InnoDB: page is corrupted in LRU_move. page type %lu, size %lu\n",
			(ulong)fil_page_get_type(page), (ulong)zip_size);
		/* the page into lru move may be dirty(in case when dropping a table and so on) , we dump mysqld just for debug */
		ut_error;
	}
#endif

	fc_blk_size = fc_get_block_size();
	blk_size = fc_calc_block_size(zip_size) / fc_blk_size;
	
	if ((fil_page_get_type(page) != FIL_PAGE_INDEX)
		&& (fil_page_get_type(page) != FIL_PAGE_INODE)) {
		return;
	}
	
	rw_lock_x_lock(&fc->hash_rwlock);	

	/* find if this bpage should move or migrate to L2 Cache */
	old_block = fc_block_search_in_hash(bpage->space, bpage->offset);
	if (fc_LRU_need_migrate(old_block, bpage)) {
		/* go on */
		move_flag = 2;
	} else if (old_block && srv_flash_cache_enable_move) {
		flash_block_mutex_enter(old_block->fil_offset);		
		if (!fc_LRU_need_move(old_block)) {
			flash_block_mutex_exit(old_block->fil_offset);	
			rw_lock_x_unlock(&fc->hash_rwlock);
			return;
		}
		flash_block_mutex_exit(old_block->fil_offset);	
		/* go on */
		move_flag = 1;
	} else {
		rw_lock_x_unlock(&fc->hash_rwlock);
		return;
	}
	
	rw_lock_x_unlock(&fc->hash_rwlock);
	
	/* the bpage should move or migrate to L2 Cache */

	/* if need compress, compress the data now */
	need_compress = fc_block_need_compress(bpage);
	if (need_compress == TRUE) {
    ulint cp_size;
		zip_buf_unalign = (byte*)ut_malloc(3 * UNIV_PAGE_SIZE);
		zip_buf = (byte*)ut_align(zip_buf_unalign, UNIV_PAGE_SIZE);
		cp_size = fc_block_do_compress(FALSE, bpage, zip_buf);
		cp_size = cp_size + FC_ZIP_PAGE_META_SIZE;
		if (cp_size >= (UNIV_PAGE_SIZE - fc_get_block_size() * KILO_BYTE)) {
			need_compress = FALSE;
		} else {
			blk_size = fc_block_compress_align(cp_size);
		}
	}

#ifdef UNIV_FLASH_CACHE_FOR_RECOVERY_SAFE
retry:
	flash_cache_mutex_enter();
	if (fc->is_doing_doublewrite == 1) {
		if (move_flag == 1) {
			fc_wait_for_aio_dw_launch();
			goto retry;
		} else {
			if (zip_buf_unalign) {
				ut_free(zip_buf_unalign);
			}
			
			flash_cache_mutex_exit();
			return;
		}
	}
#else
	flash_cache_mutex_enter();
#endif

	/* to reduce the risk that doublewrite should wait for space */
	if (fc_get_available() <= (FC_LEAST_AVIABLE_BLOCK_FOR_RECV / 2
							+ PAGE_SIZE_KB / fc_get_block_size())) {
		/*no enough space for fill the block*/
		if (zip_buf_unalign) {
			ut_free(zip_buf_unalign);
		}

		flash_cache_mutex_exit();
		return;
	}

	if (fc->is_finding_block == 1) {
		if (zip_buf_unalign) {
			ut_free(zip_buf_unalign);
		}
		flash_cache_mutex_exit();
		return;
	}
	
	rw_lock_x_lock(&fc->hash_rwlock);
	/* search the same space and offset in hash table again to make sure */
	old_block = fc_block_search_in_hash(bpage->space, bpage->offset);

	if (fc_LRU_need_migrate(old_block, bpage)) {
		/* 
		 * migrate: the page have not in flash cache
		 * move page not changed in buffer pool to L2 Cache block
		 */

		wf_block = fc_block_find_replaceable(TRUE, blk_size);
		ut_a(wf_block != NULL);

		flash_block_mutex_enter(wf_block->fil_offset);
		ut_a(fc_block_get_data_size(wf_block) == blk_size);

		if (need_compress == TRUE) {
			wf_block->zip_size = wf_block->size;
			wf_block->size = PAGE_SIZE_KB / fc_blk_size;
		}

		fc_LRU_sync_hash_table(bpage);
		srv_flash_cache_write++;
		srv_flash_cache_migrate++;
		
		/* block state is safe as block mutex hold */
		rw_lock_x_unlock(&fc->hash_rwlock); 
		fc_inc_write_off(blk_size);
		srv_fc_flush_should_commit_log_write += blk_size;
		flash_cache_mutex_exit();	

		/* do compress package if necessary */
		if (need_compress == TRUE) {
			fc_block_pack_compress(wf_block, zip_buf);
		}

		fc_io_offset(wf_block->fil_offset, &block_offset, &byte_offset);

		if (need_compress == TRUE) {
#ifdef UNIV_FLASH_CACHE_TRACE
				byte* tmp_unalign = (byte*)ut_malloc(2 * UNIV_PAGE_SIZE);
				byte* tmp = (byte*)ut_align(tmp_unalign, UNIV_PAGE_SIZE);
				fc_block_compress_check(zip_buf, wf_block);
				fc_block_do_decompress(DECOMPRESS_READ_SSD, zip_buf, tmp);
				if (buf_page_is_corrupted(tmp, zip_size)) {
					fc_block_print(wf_block);
					ut_error;
				}
				ut_free(tmp_unalign);
				
#endif		
			ret = fil_io(OS_FILE_WRITE, TRUE, FLASH_CACHE_SPACE, 0, block_offset,
					byte_offset, blk_size * KILO_BYTE * fc_blk_size, zip_buf, NULL);
		} else {
			ret = fil_io(OS_FILE_WRITE, TRUE, FLASH_CACHE_SPACE, 0, block_offset,
					byte_offset, blk_size * KILO_BYTE * fc_blk_size, page, NULL);
		}

		if (ret != DB_SUCCESS) {
			ut_print_timestamp(stderr);
			fprintf(stderr, "InnoDB: Error to migrate from buffer pool to L2 Cache," 
				"space:%u, offset %u", bpage->space, bpage->offset);
			ut_error;
		}

		goto commit_log;
		
	} else if (old_block && srv_flash_cache_enable_move) {
		/* 
		 * move:
		 * move page already in L2 Cache block to new location
		 * for the sake of geting more high read ratio
		 */
		flash_block_mutex_enter(old_block->fil_offset);	

		ut_a(old_block->state != BLOCK_BEEN_ATTACHED);
		
		if (fc_LRU_need_move(old_block)) {
			ut_ad(old_block->state == BLOCK_FLUSHED
					|| old_block->state == BLOCK_READ_CACHE);
			fc_block_delete_from_hash(old_block);

#ifdef UNIV_FLASH_CACHE_TRACE
			fc_print_used();
#endif
			srv_flash_cache_used -= fc_block_get_data_size(old_block);

			srv_flash_cache_used_nocompress -= fc_block_get_orig_size(old_block);
			fc_block_detach(FALSE, old_block);

			flash_block_mutex_exit(old_block->fil_offset);	

			wf_block = fc_block_find_replaceable(TRUE, blk_size);
			ut_a(wf_block != NULL);

			flash_block_mutex_enter(wf_block->fil_offset);
			ut_a(fc_block_get_data_size(wf_block) == blk_size);

			if (need_compress == TRUE) {
				wf_block->zip_size = wf_block->size;
				wf_block->size = PAGE_SIZE_KB / fc_blk_size;
			}

			fc_LRU_sync_hash_table(bpage);
			srv_flash_cache_write++;
			srv_flash_cache_move++;

			rw_lock_x_unlock(&fc->hash_rwlock);
			fc_inc_write_off(blk_size);	
			srv_fc_flush_should_commit_log_write += blk_size;

			flash_cache_mutex_exit();

			/* do compress package if necessary */
			if (need_compress == TRUE) {
				fc_block_pack_compress(wf_block, zip_buf);
			}

			fc_io_offset(wf_block->fil_offset, &block_offset, &byte_offset);
			
			if (need_compress == TRUE) {
#ifdef UNIV_FLASH_CACHE_TRACE
				byte* tmp_unalign = (byte*)ut_malloc(2 * UNIV_PAGE_SIZE);
				byte* tmp = (byte*)ut_align(tmp_unalign, UNIV_PAGE_SIZE);
				fc_block_compress_check(zip_buf, wf_block);
				fc_block_do_decompress(DECOMPRESS_READ_SSD, zip_buf, tmp);
				if (buf_page_is_corrupted(tmp, zip_size)) {
					fc_block_print(wf_block);
					ut_error;
				}
				ut_free(tmp_unalign);
#endif
				ret = fil_io(OS_FILE_WRITE, TRUE, FLASH_CACHE_SPACE, 0, block_offset,
						byte_offset, blk_size * KILO_BYTE * fc_blk_size, zip_buf, NULL);
			} else {
				ret = fil_io(OS_FILE_WRITE, TRUE, FLASH_CACHE_SPACE, 0, block_offset,
						byte_offset, blk_size * KILO_BYTE * fc_blk_size, page, NULL);
			}
			
			if ( ret != DB_SUCCESS ){
				ut_print_timestamp(stderr);
				fprintf(stderr,"InnoDB: Error to migrate from buffer pool to L2 Cache,"
					"space:%lu, offset %lu",
					(unsigned long)bpage->space, (unsigned long)bpage->offset);
				ut_error;
			}
				
			goto commit_log;
		} else {
			flash_block_mutex_exit(old_block->fil_offset);
			rw_lock_x_unlock(&fc->hash_rwlock);
			flash_cache_mutex_exit();
			if (zip_buf_unalign) {
				ut_free(zip_buf_unalign);
			}
			return;
		}
	} else {
		rw_lock_x_unlock(&fc->hash_rwlock);
		flash_cache_mutex_exit();
		if (zip_buf_unalign) {
			ut_free(zip_buf_unalign);
		}
		return;
	}

commit_log:
	fc_sync_fcfile();

	if (zip_buf_unalign) {
		ut_free(zip_buf_unalign);
	}

	/* async io compeleted, so the data has write into ssd, now, release the mutex */
	flash_block_mutex_exit(wf_block->fil_offset);

	/* 
	  * if we commit the log here, may be at this time the doublewrite has update the writeoff, and after commit the 
	  * server corrupted, and doublewrite has not finished, when recv, we may recovery the unfinished doublewrite page
	  * we should make sure at this time, doublewrite has finished fsync or has not yet enter.
	  */
#ifdef UNIV_FLASH_CACHE_FOR_RECOVERY_SAFE
	if ((srv_fc_flush_should_commit_log_write >= FC_BLOCK_MM_NO_COMMIT)
			&& (fc->is_doing_doublewrite == 0)) {
		flash_cache_mutex_enter();
		/* make sure is_doing_doublewrite is 0 */
		if ((fc->is_doing_doublewrite != 0)) {
			flash_cache_mutex_exit();
			return;
		}
		/* this function will release the fc mutex */
		fc_log_commit_when_update_writeoff();
	}
#endif

}
/**********************************************************************//**
When fc->mm_buf is full, write it back to flash cache file */
UNIV_INTERN
void 
fc_LRU_move_batch_low(void)
/*=========================*/
{
	ulint i;
	ulint write_start = ULINT32_UNDEFINED;
	ulint write_end;
	ulint block_offset;
	ulint byte_offset;
	ulint blk_size = 0;
	ulint fc_size = fc_get_size();
	byte* mm_page;
	fc_block_t* wf_block = NULL;
	fc_block_t* mm_block = NULL;

	ut_ad(mutex_own(&fc->mm_mutex));

	ut_a((fc->mm_buf->free_pos + UNIV_PAGE_SIZE / fc_get_block_size_byte()) 
		> fc->mm_buf->size);

retry:
	flash_cache_mutex_enter();

	/* to reduce the risk that doublewrite should wait for space */
	if (fc_get_available() <= FC_LEAST_AVIABLE_BLOCK_FOR_RECV) {
		if (write_start == ULINT32_UNDEFINED)
			write_start = fc->write_off;
		write_end = fc->write_off;
		flash_cache_mutex_exit();
		goto write_finished;	
	}

	if ((fc->is_finding_block == 1) || (fc->is_doing_doublewrite == 1)
		) {
		flash_cache_mutex_exit();
		goto retry;
	}
	
	/* step 1: write pages in fc->mm_buf pages to ssd file */
	i = 0;
	while (i < fc->mm_buf->size) {
		mm_block = fc->mm_blocks + i;
		mm_page = fc->mm_buf->buf + i * fc_get_block_size_byte();
		rw_lock_x_lock(&fc->hash_rwlock);
		ut_a((mm_block->fil_offset - fc_get_size()) < fc->mm_buf->size);
		flash_block_mutex_enter(mm_block->fil_offset);
		ut_a(mm_block->state != BLOCK_BEEN_ATTACHED);
		if ((mm_block->state == BLOCK_NOT_USED) 
			|| ((mm_block->state == BLOCK_READ_CACHE) 
				&& (ULINT_UNDEFINED == fil_space_get_zip_size(mm_block->space)))) {
			/* have been deleted from fc->hash_table because of double write.
			    or table has been droped, no write, but need delete from fc->hash_table latter */
			i += fc_block_get_data_size(mm_block);
			flash_block_mutex_exit(mm_block->fil_offset);
			rw_lock_x_unlock(&fc->hash_rwlock);		
			continue;
		}

		blk_size = fc_block_get_data_size(mm_block);
		
		/* combine the write off position with blk_size blocks */
		
		/*
		 * TODO?: add a flag to tell fc_block_find_replaceable return null,
		 * when encounter io_fix at move batch condition?
		 */
		wf_block = fc_block_find_replaceable(FALSE, blk_size);

		if (write_start == ULINT32_UNDEFINED) {
			write_start = fc->write_off;
		}

		if (wf_block == NULL) {
			write_end = fc->write_off;
			flash_block_mutex_exit(mm_block->fil_offset);
			rw_lock_x_unlock(&fc->hash_rwlock);	
			flash_cache_mutex_exit();
			goto write_finished;
		}

		flash_block_mutex_enter(wf_block->fil_offset);
		ut_a(fc_block_get_data_size(wf_block) == blk_size);
		/* here we got the write pos, update the write block info */
		wf_block->space = mm_block->space;
		wf_block->offset = mm_block->offset;

		if (mm_block->zip_size) {
			wf_block->size = UNIV_PAGE_SIZE / fc_get_block_size_byte();
			wf_block->zip_size = mm_block->zip_size;
#ifdef UNIV_FLASH_CACHE_TRACE
			fc_block_compress_check(mm_page, mm_block);
#endif
		} else {
#ifdef UNIV_FLASH_CACHE_TRACE
			ulint _offset = mach_read_from_4(mm_page + FIL_PAGE_OFFSET);
			ulint _space = mach_read_from_4(mm_page + FIL_PAGE_ARCH_LOG_NO_OR_SPACE_ID);
			if (_offset != mm_block->offset || _space != mm_block->space) {
				ut_error;
			}
#endif
			wf_block->zip_size = 0;
		}

		/* just remain the mm_block in hash table until io is finished */
		wf_block->io_fix |= IO_FIX_LRU_MOVE;

		srv_flash_cache_write++;

		/* allow read page to filfull */
		rw_lock_x_unlock(&fc->hash_rwlock);
		fc_inc_write_off(blk_size);
	
		fc_io_offset(wf_block->fil_offset, &block_offset, &byte_offset);
		/* using async write */
		fil_io(OS_FILE_WRITE, FALSE, FLASH_CACHE_SPACE, 0, block_offset, byte_offset, 
			blk_size * fc_get_block_size_byte(), mm_page, NULL);

		i += blk_size;
		
		flash_block_mutex_exit(wf_block->fil_offset);
		flash_block_mutex_exit(mm_block->fil_offset);			
	}

	/* if reach here, indicates all valid pages in fc->mm_buf has been written */
	write_end = fc->write_off; 

	/* as it is async io, release fc mutex at this time is not expensive */
	flash_cache_mutex_exit();
	
write_finished:  

	fc_sync_fcfile();

	/* no IO operation, can be finished quickly*/
	rw_lock_x_lock(&fc->hash_rwlock);
	
	/* 
	 * step 2: delete all fc->mm_blocks (even some of them hasn't written to flash cache
	 * file because of cannot_write) from fc->hash_table
	 */
	i = 0;
	while (i <  fc->mm_buf->size) {
    ulint data_size;
		mm_block = fc->mm_blocks + i;
		ut_a((mm_block->fil_offset - fc_get_size()) < fc->mm_buf->size);
		flash_block_mutex_enter(mm_block->fil_offset);
		ut_a(mm_block->state != BLOCK_BEEN_ATTACHED);
		data_size = fc_block_get_data_size(mm_block);
		if(mm_block->state == BLOCK_NOT_USED) {
			/* have been deleted from fc->hash_table because of double write */
			i += data_size;
			fc_block_detach(TRUE, mm_block);
			flash_block_mutex_exit(mm_block->fil_offset);
			continue;
		}

		ut_a((mm_block->io_fix & IO_FIX_READ) == IO_FIX_NO_IO);
		fc_block_delete_from_hash(mm_block);
		
		i += data_size;
		
		fc_block_detach(TRUE, mm_block);
		flash_block_mutex_exit(mm_block->fil_offset);
	}
	
	fc->mm_buf->free_pos = 0;

	/* step 3: insert blocks with io_fix is IO_FIX_LRU_MOVE in range [write_start, write_end)  to hash */
	while (write_start != write_end) {
    ulint data_size;
		wf_block = fc->block + write_start;
		flash_block_mutex_enter(wf_block->fil_offset);
		ut_a(wf_block->state != BLOCK_BEEN_ATTACHED);
		data_size = fc_block_get_data_size(wf_block);
		if (wf_block->io_fix & IO_FIX_LRU_MOVE) {
			mm_block = NULL;
			mm_block = fc_block_search_in_hash(wf_block->space, wf_block->offset);

			if (mm_block == NULL) {
				srv_flash_cache_used += data_size;
				srv_flash_cache_used_nocompress += fc_block_get_orig_size(wf_block);
				wf_block->state = BLOCK_READ_CACHE;
				wf_block->io_fix = IO_FIX_NO_IO;
				fc_block_insert_into_hash(wf_block);

			} else {
				wf_block->state = BLOCK_NOT_USED;
				fc_block_detach(FALSE, wf_block);
				ut_print_timestamp(stderr);
          		fprintf(stderr," InnoDB:L2 cache:find a block which is delete"
					 " after make block lru move batch by doublewrite\n");
			}
			
		}
		
		ut_a(data_size != 0);
		write_start = (write_start + data_size) % fc_size;
		flash_block_mutex_exit(wf_block->fil_offset);	
	}
	
	rw_lock_x_unlock(&fc->hash_rwlock);

	return;
}

/**********************************************************************//**
Move to L2 Cache memory buffer, if possible */
UNIV_INTERN
void
fc_LRU_move_batch(
/*==================*/
	buf_page_t * bpage)	/*!< in: page LRU out from buffer pool */
{
    page_t*	page;
	byte* mm_page;
	ulint zip_size;
	ulint need_compress;
	fc_block_t* old_block = NULL; /* used in HASH_SEARCH */
	fc_block_t* mm_block = NULL;

	ut_ad(!mutex_own(&fc->mutex));

	if (recv_no_ibuf_operations) {
		return;
	}
	
	zip_size = fil_space_get_zip_size(bpage->space);
	if (zip_size == ULINT_UNDEFINED) {
		/* table has been droped, do not need move to L2 Cache */	
#ifdef UNIV_FLASH_CACHE_TRACE
		ut_print_timestamp(fc->f_debug);
		fprintf(fc->f_debug, " space:%lu is droped, the page(%lu, %lu) will not move to L2 Cache.\n",
			(ulong)bpage->space, (ulong)bpage->space, (ulong)bpage->offset);
#endif
		return;
	}

	if (zip_size) {
		ut_ad(bpage->zip.data);
		page = bpage->zip.data;
	
        if(buf_page_is_corrupted(page, zip_size)) {
        	ut_print_timestamp(stderr);
        	fprintf(stderr," InnoDB: compressed page is corrupted in LRU_move_batch.\n");
            	ut_error;
        }

	} else {
		page = ((buf_block_t*) bpage)->frame;
	}

	if ((fil_page_get_type(page) != FIL_PAGE_INDEX)
		&& (fil_page_get_type(page) != FIL_PAGE_INODE)) {
		return;
	}

	fc_mm_mutex_enter();
	flash_cache_mutex_enter();

	rw_lock_x_lock(&fc->hash_rwlock);

	ut_a((fc->mm_buf->free_pos + UNIV_PAGE_SIZE / fc_get_block_size_byte()) 
		<= fc->mm_buf->size);

	/* search the same (space, offset) in hash table */
	old_block = fc_block_search_in_hash(bpage->space, bpage->offset);

	if (fc_LRU_need_migrate(old_block, bpage)) {
		srv_flash_cache_migrate++;
		flash_cache_mutex_exit();
	} else if (old_block &&  srv_flash_cache_enable_move) {
		flash_block_mutex_enter(old_block->fil_offset);	
		if (fc_LRU_need_move(old_block)) { 
			/* flash cache need move: first remove the old block*/
			ut_a( old_block->state == BLOCK_FLUSHED
				|| old_block->state == BLOCK_READ_CACHE);
			ut_a((old_block->io_fix & IO_FIX_READ) == IO_FIX_NO_IO);
			srv_flash_cache_move++;		

#ifdef UNIV_FLASH_CACHE_TRACE
			fc_print_used();
#endif
			srv_flash_cache_used -= fc_block_get_data_size(old_block);

			srv_flash_cache_used_nocompress -= fc_block_get_orig_size(old_block);
			flash_cache_mutex_exit();
			
			fc_block_delete_from_hash(old_block);

			fc_block_detach(FALSE, old_block);
			flash_block_mutex_exit(old_block->fil_offset);	
		} else {
			flash_block_mutex_exit(old_block->fil_offset);
			rw_lock_x_unlock(&fc->hash_rwlock);
			flash_cache_mutex_exit();
			fc_mm_mutex_exit();
			return; 
		}
			
	} else {
		rw_lock_x_unlock(&fc->hash_rwlock);
		flash_cache_mutex_exit();
		fc_mm_mutex_exit();
		return; 
	}

	/* common operation for both move and migrate */
	mm_page = fc->mm_buf->buf + fc->mm_buf->free_pos * fc_get_block_size_byte();
	mm_block = &(fc->mm_blocks[fc->mm_buf->free_pos]);

	ut_a((mm_block->fil_offset - fc_get_size()) < fc->mm_buf->size);
	flash_block_mutex_enter(mm_block->fil_offset);
	ut_a(mm_block->state == BLOCK_NOT_USED);
	ut_a((mm_block->io_fix & IO_FIX_READ) == IO_FIX_NO_IO);
	mm_block->space = bpage->space;
	mm_block->offset = bpage->offset;
	ut_a(mm_block->zip_size == 0);

	need_compress = fc_block_need_compress(bpage);
	if (need_compress == TRUE) {
		/* use the mm_buf to store the compressed data, avoid a memcpy operation */
		ulint cp_size = fc_block_do_compress(FALSE, bpage, mm_page);
		cp_size = cp_size + FC_ZIP_PAGE_META_SIZE;
		if (cp_size >= (UNIV_PAGE_SIZE - fc_get_block_size() * KILO_BYTE)) {
			need_compress = FALSE;
		} else {
			mm_block->size = fc_block_compress_align(cp_size);
		}
	}

	if (need_compress == FALSE) {
		mm_block->size = fc_calc_block_size(zip_size) / fc_get_block_size();
	}

	mm_block->state = BLOCK_READ_CACHE;
	fc_block_attach(TRUE, mm_block);

	ut_a(mm_block->size <= UNIV_PAGE_SIZE / fc_get_block_size_byte());

	fc_block_insert_into_hash(mm_block);
	
	rw_lock_x_unlock(&fc->hash_rwlock);
	
	/* need not do this, for after fc_create, never will it be modified*/
	//mm_block->io_fix = FALSE;
	
	if (need_compress == TRUE) {
		mm_block->zip_size = mm_block->size;
		mm_block->size = UNIV_PAGE_SIZE / fc_get_block_size_byte();
		fc_block_pack_compress(mm_block, mm_page);
		/* the compressed data has already in mm_buf */
	} else {
		ut_a(mm_block->zip_size == 0);
		ut_a(mm_block->size <= UNIV_PAGE_SIZE / fc_get_block_size_byte());
		memcpy(mm_page, page, fc_get_block_size_byte() * mm_block->size);
	}

	fc->mm_buf->free_pos += fc_block_get_data_size(mm_block);
	flash_block_mutex_exit(mm_block->fil_offset);
	
	/*
	 * if the buf could not fill a 16KB page next time,
	 * write the buf to SSD and clean it this time
	 */
	if((fc->mm_buf->free_pos + UNIV_PAGE_SIZE / fc_get_block_size_byte()) > fc->mm_buf->size) {
		fc_LRU_move_batch_low();
	}

	fc_mm_mutex_exit();

	return;
}
