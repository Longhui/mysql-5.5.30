/**************************************************//**
@file fc/fc0recv.c
Flash Cache log recovery

Created	24/4/2012 David Jiang (jiangchengyao@gmail.com)
*******************************************************/

#include "fc0recv.h"
#include "page0page.h"
#include "trx0sys.h"
#include "fc0fc.h"


ib_uint64_t* lsns_in_fc;


/*********************************************************************//**
Read flash cache block to hash table when recovery.*/
static
void
fc_recv_read_block_to_hash_table(
/*==========================================*/
	ulint f_offset,			/*<! in: flash cache offset */
	ulint n_read,			/*<! in: number of flash cache block to read */
	byte* buf,				/*<! in: read buffer */
	ulint* n_pages_recovery,/*<! in/out: number of pages recovered */
	ulint state				/*<! in: flash cache block state */
){
	ulint ret;
	ulint j;
	byte* page;
	ulint space;
	ulint offset;
	fc_block_t* b;
	ibool need_remove;
	
	/* read n_read pages */
	ret = fil_io(OS_FILE_READ,TRUE,FLASH_CACHE_SPACE,0,f_offset,0,n_read*UNIV_PAGE_SIZE,buf,NULL);
	if ( ret != DB_SUCCESS ){
		ut_print_timestamp(stderr);
		fprintf(stderr," InnoDB [Error]: Can not read flash cache, offset is %lu, read %lu pages.\n",
			f_offset,srv_flash_cache_pages_per_read);
		ut_error;
	}

	for( j=0; j<n_read; j++){

		page = buf + j*UNIV_PAGE_SIZE;
		
		/* read space, offset from flash cache page header */
		space = mach_read_from_4(page+FIL_PAGE_ARCH_LOG_NO_OR_SPACE_ID);
		offset = mach_read_from_4(page+FIL_PAGE_OFFSET);
		lsns_in_fc[f_offset + j] = mach_read_from_8(page + FIL_PAGE_LSN);

		/* if table has droped, do nothing */
		if (fil_space_get_zip_size(space) == ULINT_UNDEFINED){
			continue;
		}

		if ( buf_page_is_corrupted(page,fil_space_get_zip_size(space)) ){
			/* if page is corrupt */
			fprintf(stderr,
				"InnoDB: [Error]: database page"
					" corruption.\n");
			fprintf(stderr,
				"InnoDB: Dump of"
				" corresponding page\n");
			buf_page_print(page, page_is_comp(page),BUF_PAGE_PRINT_NO_CRASH);
			ut_error;
		}

		/* search the same space offset in hash table */
		HASH_SEARCH(hash,fc->hash_table,
			buf_page_address_fold(space,offset),
			fc_block_t*,b,
			ut_ad(1),
			space == b->space && offset == b->offset);
		
		need_remove = b && (lsns_in_fc[b->fil_offset] < lsns_in_fc[f_offset + j]);
		if (need_remove)
		{
		
			/* if found in hash table, remove it first */
			b->state = BLOCK_NOT_USED;
			/* delete info in hash table */
			HASH_DELETE(fc_block_t,hash,fc->hash_table,
				buf_page_address_fold(b->space, b->offset),
				b);
			--srv_flash_cache_used;
			--*n_pages_recovery;	/* for diferent versions of same page, just count it once*/
		}
		if(!b || need_remove)
		{
			/* insert to hash table */
			b =  &fc->block[f_offset+j];
			b->space = space;
			b->offset = offset;
			b->state = state;
			HASH_INSERT(fc_block_t,hash,fc->hash_table,
				buf_page_address_fold(b->space, b->offset),b);

			++srv_flash_cache_used;
			++*n_pages_recovery;
		}
		else
		{
			b =  &fc->block[f_offset+j];
			b->state = BLOCK_NOT_USED;
		}
		
	}
}


/****************************************************************//**
Recovery flash cache log between start and end offset																 
*/
static
void
fc_recv_blocks(
/*==========================================*/
	ulint start_offset,	/*<! start offset of flash cache block */
	ulint end_offset,	/*<! end offset of flash cache block */
	ulint state			/*<! flash cache block state */
){
	ulint i;
	byte* buf_unaligned;
	byte* buf;

	ulint n_read;
	ulint n_pages_recovery = 0;

	i = start_offset;
	buf_unaligned = (byte*)ut_malloc(UNIV_PAGE_SIZE*(srv_flash_cache_pages_per_read+1));
	buf = (byte*)ut_align(buf_unaligned,UNIV_PAGE_SIZE);

	while( i + srv_flash_cache_pages_per_read < end_offset ){
		fc_recv_read_block_to_hash_table(i,srv_flash_cache_pages_per_read,buf,&n_pages_recovery, state);
		i = i + srv_flash_cache_pages_per_read;
	}

	if ( end_offset - i != 0 ){
		ut_a ( end_offset > i );
		n_read = end_offset - i;
		fc_recv_read_block_to_hash_table(i,n_read,buf,&n_pages_recovery, state);
	}

	ut_print_timestamp(stderr);
	fprintf(stderr," InnoDB: Should recover pages %lu, actually recovered %lu\n",end_offset-start_offset,n_pages_recovery);

	ut_free(buf_unaligned);
}


/********************************************************************//**
when perform recovery, if any page in doublewrite buffer is newer than that in disk,
then write it to disk. After calling this function, there may be pages in flash cache older than that 
in disk, if this is TRUE, this page shoule be removed from flash cache's hash table */
static
void
fc_recv_dwb_pages_to_disk()
{
	ulint i;
	ulint space_id;
	ulint page_no;
	unsigned zip_size;
	ib_uint64_t lsn_in_dwb;
	ib_uint64_t lsn_in_disk;
	byte  unaligned_read_buf[2 * UNIV_PAGE_SIZE];
	byte* read_buf = ut_align(unaligned_read_buf, UNIV_PAGE_SIZE);
	byte* page;

	
	fil_io(OS_FILE_READ, TRUE, TRX_SYS_SPACE, 0, trx_doublewrite->block1, 0,
	       	TRX_SYS_DOUBLEWRITE_BLOCK_SIZE * UNIV_PAGE_SIZE,
	      	 trx_doublewrite->write_buf, NULL);
	fil_io(OS_FILE_READ, TRUE, TRX_SYS_SPACE, 0, trx_doublewrite->block2, 0,
	      	 TRX_SYS_DOUBLEWRITE_BLOCK_SIZE * UNIV_PAGE_SIZE,
	      	 trx_doublewrite->write_buf + TRX_SYS_DOUBLEWRITE_BLOCK_SIZE * UNIV_PAGE_SIZE, NULL);
	
	for(i = 0; i < TRX_SYS_DOUBLEWRITE_BLOCK_SIZE * 2; ++i) {
		page = trx_doublewrite->write_buf + i * UNIV_PAGE_SIZE;
		space_id = mach_read_from_4(page + FIL_PAGE_ARCH_LOG_NO_OR_SPACE_ID);
		page_no  = mach_read_from_4(page + FIL_PAGE_OFFSET);
		if(!fil_tablespace_exists_in_mem(space_id))
			;
		else if(!fil_check_adress_in_tablespace(space_id,page_no))
		{
			fprintf(stderr,
				"InnoDB: Warning: a page in the"
				" doublewrite buffer is not within space\n"
				"InnoDB: bounds; space id %lu"
				" page number %lu, page %lu in"
				" doublewrite buf.\n",
				(ulong) space_id, (ulong) page_no, (ulong) i);

		} 
		else
		{
			lsn_in_dwb = mach_read_from_8(page + FIL_PAGE_LSN);
			zip_size = fil_space_get_zip_size(space_id);
			ut_ad(ULINT_UNDEFINED != zip_size);

			if(buf_page_is_corrupted(page, zip_size))
			{
				ut_print_timestamp(stderr);
				fprintf(stderr," InnoDB: The page in the"
						" doublewrite buffer"
						" is corrupt.\n"
						"InnoDB: Cannot continue"
						" operation.\n"
						"InnoDB: You can try to"
						" recover the database"
						" with the my.cnf\n"
						"InnoDB: option:\n"
						"InnoDB:"
						" innodb_force_recovery=6\n");
				exit(1);
			}
			
			fil_io(OS_FILE_READ, TRUE,
                   space_id, zip_size, page_no, 0,
                   zip_size ? zip_size : UNIV_PAGE_SIZE, read_buf, NULL);
			lsn_in_disk  = mach_read_from_8(read_buf + FIL_PAGE_LSN);
			
			if(buf_page_is_corrupted(read_buf, zip_size) || lsn_in_dwb > lsn_in_disk) 
			{
                /* write back from doublewrite buffer to disk */
				fil_io(OS_FILE_WRITE, TRUE,
                       space_id, zip_size, page_no, 0,
                       zip_size ? zip_size : UNIV_PAGE_SIZE, page, NULL);
			}		 
		}
	}
}


/****************************************************************//**
Start flash cache log recovery.																  
*/
UNIV_INTERN
void
fc_recv(
/*==========================================*/
){
	ulint fc_size;
	ulint flush_offset;
	ulint write_offset;
	ulint flush_round;
	ulint write_round;
	
	unsigned 		i;
	byte			unaligned_disk_buf[2 * UNIV_PAGE_SIZE];
	byte*			disk_buf;
	ib_uint64_t 	lsn_in_disk;
	unsigned 		zip_size;
	ulint 			space_id;
	ulint 			page_no;
	fc_block_t* 	fc_block;
	ulint			n_removed_pages_for_wrong_version;
	fc_block_t** 	sorted_fc_blocks;
	ulint			n_newest_version_in_fcl; /* after scanning flash cache file in, the numbe of page in flash cache hash table */
	
	ut_a(fc_log->first_use == FALSE);

	ut_print_timestamp(stderr);
	fprintf(stderr, " InnoDB: BEGIN flash cache recovery!!!\n");

	ut_ad(trx_doublewrite);
    
	if (fc_log->enable_write_curr == FALSE) {
		 //first check doublewrite
		fc_recv_dwb_pages_to_disk();
		fil_flush_file_spaces(FIL_TABLESPACE);
	}
	
	flush_offset = fc_log->flush_offset;
	write_offset = fc_log->write_offset;
	flush_round = fc_log->flush_round;
	write_round = fc_log->write_round;
	fc_size = srv_flash_cache_size >> UNIV_PAGE_SIZE_SHIFT;

	ut_print_timestamp(stderr);
	fprintf(stderr,
		" InnoDB: flash cache write round: %lu flush round: %lu write offset: %lu flush offset:%lu write round bck: %lu, write offset bck: %lu\n",
		write_round,
		flush_round,
		write_offset,
		flush_offset,
		fc_log->write_round_bck,
		fc_log->write_offset_bck);

	ut_ad(fc_size);
	lsns_in_fc = ut_malloc(sizeof(ib_uint64_t) * fc_size);
	ut_a(lsns_in_fc);

	if(srv_flash_cache_write_mode == WRITE_THROUGH){
		fc_recv_blocks(0,fc_size,BLOCK_READ_CACHE);
    }
	else
	{
		if (flush_round == write_round ){
			ut_a(write_offset >= flush_offset);
			fc_recv_blocks(0,flush_offset, BLOCK_READ_CACHE);
			fc_recv_blocks(flush_offset,write_offset, BLOCK_READY_FOR_FLUSH);
			fc_recv_blocks(write_offset,fc_size, BLOCK_READ_CACHE);
		}
		else{
			ut_a(flush_round + 1 == write_round );
			fc_recv_blocks(write_offset,flush_offset,BLOCK_READ_CACHE);
			fc_recv_blocks(flush_offset,fc_size,BLOCK_READY_FOR_FLUSH);
			fc_recv_blocks(0,write_offset,BLOCK_READY_FOR_FLUSH);
		}
	}

	
	if ((fc_log->enable_write_curr == TRUE) && (srv_flash_cache_safest_recovery == FALSE)) {
		ut_print_timestamp(stderr);
		fprintf(stderr, " InnoDB: flash cache fc_log->enable_write_curr == TRUE. \n"); 	
		if (fc_log->write_offset_bck == 0XFFFFFFFFUL) {
			/*keep enable_write from innodb start, no need to compare data */
			ut_print_timestamp(stderr);
			fprintf(stderr, " InnoDB: flash cache no need to remove pages for wrong:1.\n");
			ut_print_timestamp(stderr);
			fprintf(stderr," InnoDB: RECOVERY from flash cache has finished!!!!\n");
			return;
		}

		if (write_round > (fc_log->write_round_bck + 1)) {
			/*it is more than a round when enable_write from FALSE to TRUE,
		   so the data in ssd is uptodate now, just return */
			ut_print_timestamp(stderr);
			fprintf(stderr, " InnoDB: flash cache no need to remove pages for wrong:2.\n");
			ut_print_timestamp(stderr);
			fprintf(stderr," InnoDB: RECOVERY from flash cache has finished!!!!\n");
			return;
		}

		if ((write_round == (fc_log->write_round_bck + 1)) && (write_offset >= fc_log->write_offset_bck)) {
			/*it is more than a round when enable_write from FALSE to TRUE,
		   so the data in ssd is uptodate now, just return */
			ut_print_timestamp(stderr);
			fprintf(stderr, " InnoDB: flash cache no need to remove pages for wrong:3.\n");
			ut_print_timestamp(stderr);
			fprintf(stderr," InnoDB: RECOVERY from flash cache has finished!!!!\n");
			return;
		}
	}
	
	/* compare the ssd page data to disk data, and remove the outmoded data in ssd */
	ut_print_timestamp(stderr);
	fprintf(stderr," InnoDB: flash cache start compare the ssd data with disk data.\n");
	sorted_fc_blocks = ut_malloc(fc_size * sizeof(fc_block_t*));
	ut_ad(sorted_fc_blocks);
	for(n_newest_version_in_fcl = 0, i = 0; i < fc_size; ++i)
	{
		if(fc->block[i].state == BLOCK_NOT_USED)
			continue;
		sorted_fc_blocks[n_newest_version_in_fcl++] = fc->block + i;
	}
    
    if (n_newest_version_in_fcl > 0){
        
        //sort flash cache block by (space,page_no), so read page frome disk can be more sequential
        fc_block_sort(sorted_fc_blocks, n_newest_version_in_fcl, ASCENDING);
    }
		
#ifdef UNIV_DEBUG
	ut_print_timestamp(stderr);
	fprintf(stderr, " InnoDB: pages need removed from flash cache for its wrong version are listed below\n"); 				
#endif
		
	n_removed_pages_for_wrong_version = 0;
	disk_buf = ut_align(unaligned_disk_buf,UNIV_PAGE_SIZE);
	for(i = 0; i < n_newest_version_in_fcl; ++i)
	{
		fc_block = sorted_fc_blocks[i];
		space_id = fc_block->space;
		page_no = fc_block->offset;
		zip_size = fil_space_get_zip_size(space_id);
		ut_ad(ULINT_UNDEFINED != zip_size);
		
		fil_io(OS_FILE_READ, TRUE, space_id, zip_size, page_no, 0,
			  zip_size ? zip_size : UNIV_PAGE_SIZE, disk_buf, NULL);
		lsn_in_disk = mach_read_from_8(disk_buf + FIL_PAGE_LSN);
		if(lsn_in_disk > lsns_in_fc[fc_block->fil_offset])
		{
			HASH_DELETE(fc_block_t, hash, fc->hash_table, buf_page_address_fold(space_id, page_no),fc_block);
			--srv_flash_cache_used;
			fc_block->state = BLOCK_NOT_USED;
			++n_removed_pages_for_wrong_version;
			
#ifdef UNIV_DEBUG 
			fprintf(stderr, "InnoDB: space_id: %.10lu page_no: %.10lu lsn_in_fc: %.20llu lsn_in_disk: %.20llu\n", 
					space_id, page_no, lsns_in_fc[fc_block->fil_offset], lsn_in_disk);
#endif
		}
	}

	ut_free(sorted_fc_blocks);
	ut_print_timestamp(stderr);
	fprintf(stderr," InnoDB: %lu pages have been removed from flash cache for its wrong version\n", n_removed_pages_for_wrong_version);
	

	ut_free(lsns_in_fc);
	ut_print_timestamp(stderr);
	fprintf(stderr," InnoDB: RECOVERY from flash cache has finished!!!!\n");

	return;
}
