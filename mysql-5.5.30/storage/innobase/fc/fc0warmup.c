/**************************************************//**
@file fc/fc0warmup.c
Flash Cache warmup

Created by 24/4/2012 David Jiang (jiangchengyao@gmail.com)
Modified by 24/10/2013 Thomas Wen (wenzhenghu.zju@gmail.com)
*******************************************************/

#include "fc0warmup.h"

#ifdef UNIV_NONINL
#include "fc0warmup.ic"
#endif

#include "srv0srv.h"
#include "srv0start.h"
#include "os0file.h"
#include "fil0fil.h"
#include "fc0log.h"


/******************************************************************//**
Load blocks from L2 Cache warmup file
@return: how many blocks have load into L2 Cache*/
UNIV_INTERN
ulint
fc_warmup_load_blocks(
/*==================*/
	FILE*	f,				/*<! L2 Cache warmup file handler */
	char*	full_filename,	/*<! L2 Cache warmup file name */
	ulint	count,			/*<! the number of blocks to warmup */
	byte*	page)			/*<! the page buffer to temply store the warmup data */
{
	ulint	space_id;
	ulint	page_no;
	int		fscanf_ret;
	ulint	ret;
	fc_block_t* b;
	int		i;
	ulint	start_offset;
	ulint buf_offset = 0;
	ulint zip_size;
	ulint page_len;
	ulint fc_blk_size = fc_get_block_size();
	ulint block_offset, byte_offset;

	ut_a(fc->write_off == fc->flush_off);

	start_offset = fc->write_off;

	os_aio_simulated_put_read_threads_to_sleep();

	for (i = 0; i < count; i++) {
		fscanf_ret = fscanf(f, "%lu,%lu", &space_id, &page_no);
		if (fscanf_ret != 2) {
			if (feof(f)) {
				break;
			}
			fclose(f);
			fprintf(stderr, " InnoDB: Error parsing '%s', unable to load buffer pool (stage 2).\n",
				full_filename);
			ut_error;
		}

		if (space_id > ULINT32_MASK || page_no > ULINT32_MASK) {
			fclose(f);
			/* error found, we should not continue */
			ut_error;
		}

		zip_size = fil_space_get_zip_size(space_id);

		if (zip_size == ULINT_UNDEFINED) {
			ut_print_timestamp(stderr);
			fprintf(stderr," InnoDB: the table :%lu is droped in disk.\n", (ulong)space_id);
			ut_error;
		}

		page_len = fc_calc_block_size(zip_size);
		if ((buf_offset + page_len) > fc_get_size()) {
			count = i;
			fc->write_off = 0;
			fc->write_round++;
			goto write_blocks;
		}
		
		ret = fil_io(OS_FILE_READ | OS_AIO_SIMULATED_WAKE_LATER, FALSE, space_id, 0, 
				page_no, 0, page_len * KILO_BYTE, &page[buf_offset * KILO_BYTE], NULL);
		if (ret != DB_SUCCESS) {
			ut_print_timestamp(stderr);
			fprintf(stderr," InnoDB: can read from disk.\n");
			ut_error;
		}

		buf_offset += page_len;

		//if ((fc->write_off + page_len / fc_blk_size) > fc_get_size()) {
		//	fc->write_off = 0;
		//	fc->write_round++;
		//}
		
		b = &fc->block[fc->write_off];
		b->offset = page_no;
		b->space = space_id;
		b->size = page_len / fc_blk_size;
		b->state = BLOCK_READ_CACHE;
		//rw_lock_x_lock(&fc->hash_rwlock);

#ifdef UNIV_DEBUG
    {
		  ulint space;
		  ulint offset;

		  offset = mach_read_from_4(page + buf_offset * KILO_BYTE
			  + FIL_PAGE_OFFSET);
		  space = mach_read_from_4(page + buf_offset * KILO_BYTE
			  + FIL_PAGE_ARCH_LOG_NO_OR_SPACE_ID);

		  ut_ad(b->space == space);
		  ut_ad(b->offset == offset);
    }
#endif

		fc_block_attach(FALSE, b);
		/* insert to hash table */
		fc_block_insert_into_hash(b);
		//rw_lock_x_unlock(&fc->hash_rwlock);

		srv_flash_cache_used += fc_block_get_data_size(b);
		srv_flash_cache_used_nocompress += fc_block_get_orig_size(b);
		fc_inc_write_off(b->size);
		fc_inc_flush_off(b->size);
	}

write_blocks:
	os_aio_simulated_wake_handler_threads();
	os_aio_wait_until_no_pending_reads();


	fc_io_offset(start_offset, &block_offset, &byte_offset);
	ret = fil_io(OS_FILE_WRITE, TRUE, FLASH_CACHE_SPACE, 0, block_offset, byte_offset, 
			buf_offset * KILO_BYTE, page, NULL);
	if ( ret != DB_SUCCESS ) {
		ut_print_timestamp(stderr);
		fprintf(stderr," InnoDB: Error in write page to L2 Cache.\n");
		ut_error;
	}

	return count;
}

/******************************************************************//**
Load flash cache from warmup file */
UNIV_INTERN
void
fc_load_warmup_file(void)
/*==================*/
{
	char	full_filename[OS_FILE_MAX_PATH];
	FILE*	f;
	ulint	dump_n;
	ulint	space_id;
	ulint	page_no;
	byte*	page_unalign;
	byte*	page;
	ulint	len;
	ulint read_count;

	ut_snprintf(full_filename, sizeof(full_filename),
		"%s/%s", srv_data_home, "flash_cache.warmup");
	srv_normalize_path_for_win(full_filename);

	f = fopen(full_filename, "r");
	if (f == NULL) {
		ut_print_timestamp(stderr);
		fprintf(stderr,
			" InnoDB: Cannot open '%s' for warmup: %s.\n", full_filename, strerror(errno));
		return;
	}

	dump_n = 0;
	while (fscanf(f, "%lu,%lu", &space_id, &page_no) == 2) {
		dump_n++;
	}
	
	if (!feof(f)) {
		/* fscanf() returned != 4 */
		const char*	what;
		if (ferror(f)) {
			what = "reading";
		} else {
			what = "parsing";
		}
		fclose(f);
		fprintf(stderr, " InnoDB: Error %s '%s', unable to load buffer pool (stage 1).\n",
			what, full_filename);
		return;
	}

	rewind(f);

	len = dump_n;
	page_unalign = (byte*)ut_malloc((srv_flash_cache_pages_per_read + 1) * UNIV_PAGE_SIZE);
	page = (byte*)ut_align(page_unalign, UNIV_PAGE_SIZE);

	ut_print_timestamp(stderr);
	fprintf(stderr," InnoDB: warmup L2 Cache from flash_cache.warmup	");
	while (len > srv_flash_cache_pages_per_read) {
		read_count = 
			fc_warmup_load_blocks(f, full_filename, srv_flash_cache_pages_per_read, page);
		len = len - read_count;
		fprintf(stderr, "%lu ", (unsigned long)(100 * (dump_n-len) / dump_n));
	}

	if (len > 0) {
		fc_warmup_load_blocks(f, full_filename, len, page);
		fprintf(stderr,"100.\n");
	}

	fclose(f);
	
	rename("flash_cache.warmup","flash_cache.warmup.old");

	ut_print_timestamp(stderr);
	fprintf(stderr," InnoDB: L2 Cache load completed.\n");

	srv_flash_cache_load_from_dump_file = TRUE;

	if (page_unalign) {
		ut_free(page_unalign);
	}
}

/********************************************************************//**
Warm up tablespace to flash cache block.
@return: if we can still warmup other tablespace */
static
ibool
fc_warmup_tablespace(
/*=============================*/
	os_file_t	file,		/*!< in: file of tablespace */
	const char* dbname,		/*!< in: database name */
	const char*	tablename,	/*!< in: tablespace to load tpcc.*:test.mysql */
	ulint space_id)			/*!< in: tablespace space id */
{

		byte*	buf_unaligned;
		byte*	buf;
		byte*	page;
		ibool	success = FALSE;
		char*	token;
		char	name[128];
		char	name2[128];
		char	str[128];
		ulint	n_pages;
		ulint	i = 0;
		ulint	zip_size;
		ulint	size;
		ulint	size_high;
		ulint	foffset;
		ulint	foffset_high;
		ulint	j;
		fc_block_t* b;
		ulint	offset;

		sprintf(name,"%s.%s",dbname,tablename);
		sprintf(name2,"%s.*",dbname);
		ut_strcpy(str,srv_flash_cache_warmup_table);
		token = strtok(str,":");
		while (token != NULL && !success) {
			if (ut_strcmp(token,name) == 0 || ut_strcmp(token,name2) == 0)
				success = TRUE;
			
			token = strtok(NULL,":");
		}

		if (!success) {
			return TRUE;
		}

		/* get zip size */
		zip_size = fil_space_get_zip_size(space_id);
		if (zip_size == 0) {
			zip_size = UNIV_PAGE_SIZE;
		}

		/* calc the fc distance can contain how many pages */
		n_pages = fc_get_distance();	
		/* sub 1 in case of the end of fc file is not align with block size */
		n_pages = n_pages * fc_get_block_size() / (zip_size / KILO_BYTE) - 1;

		/* get file size */
		os_file_get_size(file, &size, &size_high);
		/* malloc memory for page to read */
		buf_unaligned = (byte*)ut_malloc((srv_flash_cache_pages_per_read + 1) * zip_size);
		buf = (byte*)ut_align(buf_unaligned, zip_size);

		ut_print_timestamp(stderr);
		fprintf(stderr," InnoDB: start to warm up tablespace %s.%s to L2 Cache.\n",
			dbname, tablename);

		while ((i + srv_flash_cache_pages_per_read) < n_pages) {
			foffset = ((ulint)(i * zip_size)) & 0xFFFFFFFFUL;
			foffset_high = (ib_uint64_t)(i * zip_size) >> 32;
			success = os_file_read_no_error_handling(file, buf, foffset, foffset_high, 
						zip_size * srv_flash_cache_pages_per_read);
			if (!success) {
				ut_free(buf_unaligned);
				return (TRUE);
			}
			
			for (j=0; j < srv_flash_cache_pages_per_read; j++) {
				page = buf + j * zip_size;
				if ((fil_page_get_type(page) != FIL_PAGE_INDEX)
						&& (fil_page_get_type(page) != FIL_PAGE_INODE)) {
					continue;
				}

				offset = mach_read_from_4(page + FIL_PAGE_OFFSET);
				ut_ad(mach_read_from_4(
					page + FIL_PAGE_ARCH_LOG_NO_OR_SPACE_ID) == space_id);	

				rw_lock_x_lock(&fc->hash_rwlock);
				b = fc_block_search_in_hash(space_id, offset);
				
				if (b) {
#ifdef UNIV_FLASH_DEBUG
					/* if found in hash table, remove it first */
					ulint		ret3;
					ulint		space3;
					ulint		offset3;
					ib_uint64_t	lsn3;
					ib_uint64_t	lsn;
					ulint block_offset;
					ulint byte_offset;
					byte		read_buf[UNIV_PAGE_SIZE];
					lsn = mach_read_from_8(page + FIL_PAGE_LSN);
					
					/* lsn in hash table should smaller than this */
					fc_io_offset(b->fil_offset, &block_offset, &byte_offset);
					ret3 = fil_io(OS_FILE_READ, TRUE, FLASH_CACHE_SPACE, 0, 
						block_offset, byte_offset, zip_size, &read_buf, NULL);
					
					ut_ad(ret3);
					
					lsn3 = mach_read_from_8(read_buf + FIL_PAGE_LSN);
					space3 = mach_read_from_4(
						read_buf + FIL_PAGE_ARCH_LOG_NO_OR_SPACE_ID);
					offset3 = mach_read_from_4(read_buf + FIL_PAGE_OFFSET);
					
					ut_ad(space3 == space_id);
					ut_ad(offset3 == offset);
					ut_ad(lsn3 >= lsn);
#endif
					/* page in flash cache should always newer than page in disk */
					rw_lock_x_unlock(&fc->hash_rwlock);
					continue;
				} else { 
					ulint block_offset, byte_offset;
					if ((fc->write_off + zip_size / KILO_BYTE / fc_get_block_size()) 
						> fc_get_size()) {
						fc->write_off = 0;
						fc->write_round++;
						fc->flush_off = 0;
						fc->flush_round++;
					}
					
					b = &fc->block[fc->write_off];
					ut_a( b->state == BLOCK_NOT_USED);
					b->space = space_id;
					b->offset = offset;
					b->size = zip_size / KILO_BYTE / fc_get_block_size();
					b->state = BLOCK_READ_CACHE;
					fc_block_attach(FALSE, b);
#ifdef UNIV_FLASH_DEBUG
					fprintf(stderr," warmup space: %lu, page no: %lu.\n", space_id, offset);
#endif
					fc_block_insert_into_hash(b);

					fc_io_offset(b->fil_offset, &block_offset, &byte_offset);
					success = fil_io(OS_FILE_WRITE | OS_AIO_SIMULATED_WAKE_LATER, FALSE,
							FLASH_CACHE_SPACE, 0, block_offset, byte_offset, zip_size, page, NULL);
					if (success != DB_SUCCESS) {
						ut_print_timestamp(stderr);
						fprintf(stderr," InnoDB [Error]: Can not recover tablespace %lu, "
							"offset is %lu.\n", space_id, offset);
						ut_error;
					}
					srv_flash_cache_used += fc_block_get_data_size(b);
					srv_flash_cache_used_nocompress += fc_block_get_orig_size(b);
				}

				fc_inc_write_off(b->size);
				fc_inc_flush_off(b->size);
				rw_lock_x_unlock(&fc->hash_rwlock);
				if (fc->write_off == 0) {
					os_aio_simulated_wake_handler_threads();
					os_aio_wait_until_no_pending_fc_writes();
					fil_flush_file_spaces(FIL_FLASH_CACHE);
					ut_free(buf_unaligned);
					ut_print_timestamp(stderr);
					fprintf(stderr,
							" InnoDB: warm up table %s.%s to space: %lu offset %lu.(100%%)\n",
							dbname,tablename,space_id,i);
					ut_print_timestamp(stderr);
					fprintf(stderr," InnoDB: L2 Cache is full, warmup stop.\n");
					return FALSE;
				}
			}

			os_aio_simulated_wake_handler_threads();
			os_aio_wait_until_no_pending_fc_writes();
			fil_flush_file_spaces(FIL_FLASH_CACHE);
			i = i + srv_flash_cache_pages_per_read;
		}

		ut_free(buf_unaligned);
		return (TRUE);
}

/********************************************************************//**
Warm up tablespaces to flash cache block.,stop if no space left. */
UNIV_INTERN
void
fc_warmup_tablespaces(void)
/*=============================*/
{
	int		ret;
	char*	dbpath	= NULL;
	ulint	dbpath_len	= 100;
	ulint	err	= DB_SUCCESS;
	os_file_dir_t	dir;
	os_file_dir_t	dbdir;
	os_file_stat_t	dbinfo;
	os_file_stat_t	fileinfo;

	if (srv_flash_cache_size == 0 || !fc_log->first_use) {
		return;
	}

	/* The datadir of MySQL is always the default directory of mysqld */

	dir = os_file_opendir(fil_path_to_mysql_datadir, TRUE);

	if (dir == NULL) {
		return;
	}

	dbpath = (char*)mem_alloc(dbpath_len);

	/* Scan all directories under the datadir. They are the database
	directories of MySQL. */

	ret = fil_file_readdir_next_file(&err, fil_path_to_mysql_datadir, dir,
					 &dbinfo);
	while (ret == 0) {
		ulint len;
		/* printf("Looking at %s in datadir\n", dbinfo.name); */

		if (dbinfo.type == OS_FILE_TYPE_FILE || dbinfo.type == OS_FILE_TYPE_UNKNOWN) {
			goto next_datadir_item;
		}

		/*
		 * We found a symlink or a directory; try opening it to see
		 * if a symlink is a directory
		 */

		len = strlen(fil_path_to_mysql_datadir) + strlen (dbinfo.name) + 2;
		if (len > dbpath_len) {
			dbpath_len = len;

			if (dbpath) {
				mem_free(dbpath);
			}

			dbpath = (char*)mem_alloc(dbpath_len);
		}
		sprintf(dbpath, "%s/%s", fil_path_to_mysql_datadir, dbinfo.name);
		srv_normalize_path_for_win(dbpath);

		dbdir = os_file_opendir(dbpath, FALSE);

		if (dbdir != NULL) {
			/* printf("Opened dir %s\n", dbinfo.name); */

			/*
			 * We found a database directory; loop through it,
			 * looking for possible .ibd files in it
			 */
			ret = fil_file_readdir_next_file(&err, dbpath, dbdir, &fileinfo);
			while (ret == 0) {
				/* printf(" Looking at file %s\n", fileinfo.name); */

				if (fileinfo.type == OS_FILE_TYPE_DIR) {
					goto next_file_item;
				}

				/* We found a symlink or a file */
				if (strlen(fileinfo.name) > 4
				    && 0 == strcmp(fileinfo.name + strlen(fileinfo.name) - 4, ".ibd")) {
					/* The name ends in .ibd; try opening the file */
				   	char*		filepath;
					os_file_t	file;
					ibool		success;
					byte*		buf2;
					byte*		page;
					ulint		space_id;
					/* Initialize file path */
					filepath = (char*)mem_alloc(strlen(dbinfo.name) + strlen(fileinfo.name)
									+ strlen(fil_path_to_mysql_datadir) + 3);
					sprintf(filepath, "%s/%s/%s", fil_path_to_mysql_datadir, dbinfo.name,
						fileinfo.name);
					srv_normalize_path_for_win(filepath);
					//dict_casedn_str(filepath);

					/* Get file handler */
					file = os_file_create_simple_no_error_handling(innodb_file_data_key,
						filepath, OS_FILE_OPEN, OS_FILE_READ_ONLY, &success);

					/* Get space id */
					buf2 = (byte*)ut_malloc(2 * UNIV_PAGE_SIZE);
					/* Align the memory for file i/o if we might have O_DIRECT set */
					page = (byte*)ut_align(buf2, UNIV_PAGE_SIZE);
					os_file_read(file, page, 0, 0, UNIV_PAGE_SIZE);
					/* We have to read the tablespace id from the file */
					space_id = fsp_header_get_space_id(page);

					/* Preload to L2 Cache */
					if (fc_warmup_tablespace(file,dbinfo.name,strtok(fileinfo.name,"."),space_id)
							== FALSE) {
						goto finish;	
					}
					
					os_file_close(file);
					ut_free(buf2);
					mem_free(filepath);
				}
next_file_item:
				ret = fil_file_readdir_next_file(&err, dbpath, dbdir, &fileinfo);
			}

			if (0 != os_file_closedir(dbdir)) {
				fputs("InnoDB: Warning: could not close database directory ", stderr);
				ut_print_filename(stderr, dbpath);
				putc('\n', stderr);
				err = DB_ERROR;
			}
		}

next_datadir_item:
		ret = fil_file_readdir_next_file(&err, fil_path_to_mysql_datadir, dir, &dbinfo);
	}

finish:

	ut_print_timestamp(stderr);
	fprintf(stderr," InnoDB: flash cache warm up finish.\n");
//#ifdef UNIV_FLASH_DEBUG
//	fc_validate();
//#endif

	mem_free(dbpath);
}
