/**************************************************//**
@file fc/fc0dump.c
Flash Cache dump and load

Created	24/4/2012 David Jiang (jiangchengyao@gmail.com)
*******************************************************/

#include "fc0dump.h"
#include "srv0srv.h"
#include "srv0start.h"
#include "os0file.h"
#include "fil0fil.h"
#include "fc0log.h"

/******************************************************************//**
Dump blocks from flash cache to file*/
UNIV_INTERN
void
fc_dump(
/*==================*/
)
{

	char	full_filename[OS_FILE_MAX_PATH];
	char	tmp_filename[OS_FILE_MAX_PATH];
	FILE*	f;
	ulint	i;
	int	ret;
	fc_block_t* b;

	ut_snprintf(full_filename, sizeof(full_filename),
		"%s/%s", srv_data_home, "flash_cache.dump");
	srv_normalize_path_for_win(full_filename);

	ut_snprintf(tmp_filename, sizeof(tmp_filename),
		"%s.incomplete", full_filename);
	srv_normalize_path_for_win(tmp_filename);

	f = fopen(tmp_filename, "w");
	if (f == NULL) {
		fprintf(stderr," InnoDB: Cannot open '%s' for writing: %s.\n",tmp_filename, strerror(errno));
		return;
	}
	/* dump flash cache block info */
	for ( i = 0; i < fc_get_size(); i++ ){
		b = &fc->block[i];
		if ( b->state != BLOCK_NOT_USED ){
			/* only dump ~BLOCK_NOT_USED */
	
			ret = fprintf(f, "%lu,%lu,%lu,%lu\n",
				(unsigned long)b->space,
				(unsigned long)b->offset,
				(unsigned long)b->fil_offset,
				(unsigned long)b->state);
			if (ret < 0) {
				fclose(f);
				fprintf(stderr,
					" InnoDB: Cannot write to '%s': %s.\n",
					tmp_filename, strerror(errno));
				/* leave tmp_filename to exist */
				return;
			}
		}
	}
	ret = fclose(f);
	if (ret != 0) {
		fprintf(stderr,
			"	InnoDB: Cannot close '%s': %s.\n",
			tmp_filename, strerror(errno));
		return;
	}
	ret = unlink(full_filename);
	if (ret != 0 && errno != ENOENT) {
		fprintf(stderr,
			" InnoDB: Cannot delete '%s': %s.\n",
			full_filename, strerror(errno));
		/* leave tmp_filename to exist */
		return;
	}
	ret = rename(tmp_filename, full_filename);
	if (ret != 0) {
		fprintf(stderr,
			" InnoDB: Cannot rename '%s' to '%s': %s.\n",
			tmp_filename, full_filename,
			strerror(errno));
		/* leave tmp_filename to exist */
		return;
	}

	ut_print_timestamp(stderr);

	fprintf(stderr,
		" InnoDB: flash cache dump completed.\n");
}

/******************************************************************//**
Load flash cache from dump file */
UNIV_INTERN
void
fc_load(
/*==================*/
)
{

	char	full_filename[OS_FILE_MAX_PATH];
	ulint	i;
	FILE*	f;
	ulint	dump_n;
	int		fscanf_ret;
	fc_block_t* b;
	ulint	space_id = 0;
	ulint	page_no = 0;
	ulint	fil_offset = 0;
	ulint	state = 0;

	ut_snprintf(full_filename, sizeof(full_filename),
		"%s/%s", srv_data_home, "flash_cache.dump");
	srv_normalize_path_for_win(full_filename);

	f = fopen(full_filename, "r");
	if (f == NULL) {
		if(fc_log->first_use)
		{
			srv_flash_cache_load_from_dump_file = TRUE; /* no recovery is needed*/
			return ;	/* prevent below output*/
		}
		ut_print_timestamp(stderr);
		fprintf(stderr,
			" InnoDB: Cannot open '%s' for reading: %s.\n",
			full_filename, strerror(errno));
		fprintf(stderr, "InnoDB: flash cache did not shutdown correctly,"
			"scan flash cache file %s to recover.\n",
			srv_flash_cache_file);
		return;
	}

	dump_n = 0;
	while (fscanf(f, "%lu,%lu,%lu,%lu", &space_id, &page_no, &fil_offset, &state) == 4 ) {
		dump_n++;
	}
	
	if ( !feof(f) ) {
		/* fscanf() returned != 4 */
		const char*	what;
		if (ferror(f)) {
			what = "reading";
		} else {
			what = "parsing";
		}
		fclose(f);
		fprintf(stderr, "	InnoDB: Error %s '%s', "
			"unable to load buffer pool (stage 1).\n",
			what, full_filename);
		return;
	}

	rewind(f);

	for (i = 0; i < dump_n; i++) {
		fscanf_ret = fscanf(f, "%lu,%lu,%lu,%lu",&space_id, &page_no, &fil_offset, &state);
		if (fscanf_ret != 4) {
			if (feof(f)) {
				break;
			}
			fclose(f);
			fprintf(stderr,
				"	InnoDB: Error parsing '%s', unable "
				"to load buffer pool (stage 2).\n",
				full_filename);
			return;
		}

		if (space_id > ULINT32_MASK || page_no > ULINT32_MASK) {
			fclose(f);
			/* error found, we should not continue */
			ut_error;
		}

		ut_ad ( state != BLOCK_NOT_USED );
		b = &fc->block[fil_offset];
		b->offset = page_no;
		b->space = space_id;
		b->state = state;
		flash_cache_hash_mutex_enter(space_id,page_no);

		/* insert to hash table */
		HASH_INSERT(fc_block_t,hash,fc->hash_table,
			buf_page_address_fold(b->space, b->offset),
			b);
		flash_cache_hash_mutex_exit(space_id,page_no);

		srv_flash_cache_used++;
	}

	fclose(f);
	
	ut_a(os_file_delete(full_filename));

	ut_print_timestamp(stderr);

	fprintf(stderr,
		"	InnoDB: flash cache load completed.\n");

	srv_flash_cache_load_from_dump_file = TRUE;

#ifdef UNIV_DEBUG 
	if(srv_flash_cache_write_mode == WRITE_THROUGH)
	{
  		ulint n_newest_version_in_fcl;
		ulint i;
		fc_block_t* fc_block;
		ulint space_id;
		ulint page_no;
		ulint zip_size;
		ib_uint64_t 	lsn_in_disk;
		ib_uint64_t 	lsn_in_ssd;
		fc_block_t** sort_blocks;
		byte	disk_buf[2 * UNIV_PAGE_SIZE];
		byte	ssd_buf[2 * UNIV_PAGE_SIZE];
		byte*   disk_page = ut_align(disk_buf, UNIV_PAGE_SIZE);
		byte*	ssd_page = ut_align(ssd_buf, UNIV_PAGE_SIZE);

		sort_blocks = ut_malloc(fc->size * sizeof(fc_block_t*));
	    ut_a(sort_blocks);
		for(n_newest_version_in_fcl = 0, i = 0; i < fc->size; ++i)
		{
			ut_a(fc->block[i].state == BLOCK_READ_CACHE || fc->block[i].state == BLOCK_NOT_USED);
			if(fc->block[i].state != BLOCK_READ_CACHE)
				continue;
			sort_blocks[n_newest_version_in_fcl++] = fc->block + i;
		}
        
        if (n_newest_version_in_fcl > 0){
            fc_block_sort(sort_blocks, n_newest_version_in_fcl, ASCENDING);
        }
		
		for(i = 0; i < n_newest_version_in_fcl; ++i)
		{
			fc_block = sort_blocks[i];
			space_id = fc_block->space;
			page_no = fc_block->offset;
			zip_size = fil_space_get_zip_size(space_id);
			ut_a(ULINT_UNDEFINED != zip_size);
		
			fil_io(OS_FILE_READ, TRUE, space_id, zip_size, page_no, 0,
			  zip_size ? zip_size : UNIV_PAGE_SIZE, disk_page, NULL);
			fil_io(OS_FILE_READ, TRUE, FLASH_CACHE_SPACE, 0, fc_block->fil_offset, 0,
			  zip_size ? zip_size : UNIV_PAGE_SIZE, ssd_page, NULL);
			lsn_in_disk = mach_read_from_8(disk_page + FIL_PAGE_LSN);
			lsn_in_ssd  = mach_read_from_8(ssd_page + FIL_PAGE_LSN);
			if(lsn_in_disk != lsn_in_ssd)
			{
				ut_print_timestamp(stderr);
				fprintf(stderr, " InnoDB: page(%lu, %lu): LSN_in_SSD(%llu) at pos %lu; LSN_in_DISK(%llu)\n", 
					    space_id, page_no, lsn_in_ssd, fc_block->fil_offset, lsn_in_disk);
				fprintf(stderr, " InnoDB: should be the same page version in write through mode\n");
				ut_free(sort_blocks);
				ut_error;
			}
		}
		ut_free(sort_blocks);
  	}
#endif

}

/******************************************************************//**
Dump blocks from flash cache to file*/
static
void
fc_load_blocks(
/*==================*/
	FILE*	f,				/*<! flash cache file */
	char*	full_filename,
	ulint	count,
	byte*	page
){
	ulint	space_id;
	ulint	page_no;
	int		fscanf_ret;
	ulint	ret;
	fc_block_t* b;
	int		i;
	ulint	start_offset;

	ut_a(fc->write_off == fc->flush_off);

	start_offset = fc->write_off;

	os_aio_simulated_put_read_threads_to_sleep();

	for( i = 0; i < count; i++){

		fscanf_ret = fscanf(f, "%lu,%lu",&space_id, &page_no);
		if (fscanf_ret != 2) {
			if (feof(f)) {
				break;
			}
			fclose(f);
			fprintf(stderr,
				"	InnoDB: Error parsing '%s', unable "
				"to load buffer pool (stage 2).\n",
				full_filename);
			return;
		}

		if (space_id > ULINT32_MASK || page_no > ULINT32_MASK) {
			fclose(f);
			/* error found, we should not continue */
			ut_error;
		}

		ret = fil_io(OS_FILE_READ | OS_AIO_SIMULATED_WAKE_LATER,FALSE,space_id,0,page_no,0,UNIV_PAGE_SIZE,&page[i*UNIV_PAGE_SIZE],NULL);
		if ( ret != DB_SUCCESS ){
			ut_print_timestamp(stderr);
			fprintf(stderr,"	InnoDB: can read from disk.\n");
			ut_error;
		}

		b = &fc->block[fc->write_off];
		b->offset = page_no;
		b->space = space_id;
		b->state = BLOCK_READ_CACHE;
		flash_cache_hash_mutex_enter(space_id,page_no);

		/* insert to hash table */
		HASH_INSERT(fc_block_t,hash,fc->hash_table,
			buf_page_address_fold(b->space, b->offset),
			b);
		flash_cache_hash_mutex_exit(space_id,page_no);

		srv_flash_cache_used++;
		fc->write_off = ( fc->write_off + 1 ) % fc_get_size();
		fc->flush_off = ( fc->flush_off + 1 ) % fc_get_size();
	}

	os_aio_simulated_wake_handler_threads();
	os_aio_wait_until_no_pending_reads();

#ifdef UNIV_DEBUG
	for ( i = 0; i < count; i++ ){

		ulint space;
		ulint offset;
		fc_block_t* b3;

		offset = mach_read_from_4(page + i*UNIV_PAGE_SIZE + FIL_PAGE_OFFSET);
		space = mach_read_from_4(page + i*UNIV_PAGE_SIZE + FIL_PAGE_ARCH_LOG_NO_OR_SPACE_ID);

		b3 = &fc->block[start_offset + i];

		ut_ad(b3->space == space);
		ut_ad(b3->offset == offset);
	}
#endif
	ret = fil_io(OS_FILE_WRITE ,TRUE,FLASH_CACHE_SPACE,0,start_offset,0,UNIV_PAGE_SIZE*count,page,NULL);
	if ( ret != DB_SUCCESS ){
			ut_print_timestamp(stderr);
			fprintf(stderr,"	InnoDB: Error in write page to flash cache.\n");
			ut_error;
	}

}

/******************************************************************//**
Load flash cache from warmup file */
UNIV_INTERN
void
fc_load_warmup_file(
/*==================*/
)
{

	char	full_filename[OS_FILE_MAX_PATH];
	FILE*	f;
	ulint	dump_n;
	ulint	space_id;
	ulint	page_no;
	byte*	page_unalign;
	byte*	page;
	ulint	len;

	ut_snprintf(full_filename, sizeof(full_filename),
		"%s/%s", srv_data_home, "flash_cache.warmup");
	srv_normalize_path_for_win(full_filename);

	f = fopen(full_filename, "r");
	if (f == NULL) {
		ut_print_timestamp(stderr);
		fprintf(stderr,
			"	InnoDB: Cannot open '%s' for warmup: %s.\n",
			full_filename, strerror(errno));
		return;
	}

	dump_n = 0;
	while (fscanf(f, "%lu,%lu", &space_id, &page_no) == 2 ) {
		dump_n++;
	}
	
	if ( !feof(f) ) {
		/* fscanf() returned != 4 */
		const char*	what;
		if (ferror(f)) {
			what = "reading";
		} else {
			what = "parsing";
		}
		fclose(f);
		fprintf(stderr, "	InnoDB: Error %s '%s', "
			"unable to load buffer pool (stage 1).\n",
			what, full_filename);
		return;
	}

	rewind(f);

	len = dump_n;
	page_unalign = (byte*)ut_malloc((srv_flash_cache_pages_per_read+1)*UNIV_PAGE_SIZE);
	page = (byte*)ut_align(page_unalign,UNIV_PAGE_SIZE);

	ut_print_timestamp(stderr);
	fprintf(stderr,"	InnoDB: warmup flash cache from flash_cache.warmup	");
	while ( len > srv_flash_cache_pages_per_read ){
		fc_load_blocks(f,full_filename,srv_flash_cache_pages_per_read,page);
		len = len - srv_flash_cache_pages_per_read;
		fprintf(stderr,"%lu ",(unsigned long)100*(dump_n-len)/dump_n);
	}

	if ( len > 0 ){
		fc_load_blocks(f,full_filename,len,page);
		fprintf(stderr,"100.\n");
	}

	fclose(f);
	
	rename("flash_cache.warmup","flash_cache.warmup.old");

	ut_print_timestamp(stderr);
	fprintf(stderr,
		"	InnoDB: flash cache load completed.\n");

	srv_flash_cache_load_from_dump_file = TRUE;

	if ( page_unalign ){
		ut_free(page_unalign);
	}

}

/********************************************************************//**
Warm up tablespace to flash cache block. */
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
		ulint	write_off;
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
		while( token != NULL && !success ){
			if ( ut_strcmp(token,name) == 0 || ut_strcmp(token,name2) == 0 )
				success = TRUE;
			token = strtok(NULL,":");
		}

		if ( !success ){
			return TRUE;
		}


		if ( fc->write_round == fc->flush_round ){
			n_pages = fc_get_size() - ( fc->write_off - fc->flush_off ) ;
		}
		else{
			ut_a(fc->write_round = fc->flush_round+1);
			n_pages = fc->flush_off - fc->write_off;
		}

		/* get zip size */
		zip_size = fil_space_get_zip_size(space_id);
		if (zip_size == 0){
			zip_size = UNIV_PAGE_SIZE;
		}

		/* start write offset */
		write_off = fc->write_off;
		/* get file size */
		os_file_get_size(file,&size,&size_high);
		/* malloc memory for page to read */
		buf_unaligned = (byte*)ut_malloc((srv_flash_cache_pages_per_read+1)*UNIV_PAGE_SIZE);
		buf = (byte*)ut_align(buf_unaligned,zip_size);

		ut_print_timestamp(stderr);
		fprintf(stderr," InnoDB: start to warm up tablespace %s.%s to flash cache.\n",dbname,tablename);

		while( i + srv_flash_cache_pages_per_read < n_pages ){
			foffset = ((ulint)(i*zip_size)) & 0xFFFFFFFFUL;
			foffset_high = (ib_uint64_t)(i*zip_size) >> 32;
			success = os_file_read_no_error_handling(file, buf, foffset, foffset_high, zip_size*srv_flash_cache_pages_per_read);
			if ( !success ){
				ut_free(buf_unaligned);
				return (TRUE);
			}
			for( j=0; j<srv_flash_cache_pages_per_read; j++ ){
				page = buf + j*zip_size;
				if ( fil_page_get_type(page) != FIL_PAGE_INDEX 
							&& fil_page_get_type(page) != FIL_PAGE_INODE 
							){
					continue;
				}
				offset = mach_read_from_4(page+FIL_PAGE_OFFSET);
				ut_ad( mach_read_from_4(page+FIL_PAGE_ARCH_LOG_NO_OR_SPACE_ID) == space_id );
				flash_cache_hash_mutex_enter(space_id,offset);		
				HASH_SEARCH(hash,fc->hash_table,
					buf_page_address_fold(space_id,offset),
					fc_block_t*,b,
					ut_ad(1),
					space_id == b->space && offset == b->offset);
				if ( b ){
#ifdef UNIV_FLASH_DEBUG
					/* if found in hash table, remove it first */
					ulint		ret3;
					ulint		space3;
					ulint		offset3;
					ib_uint64_t	lsn3;
					ib_uint64_t	lsn;
					byte		read_buf[UNIV_PAGE_SIZE];
					lsn = mach_read_from_8(page+FIL_PAGE_LSN);
					/* lsn in hash table should smaller than this */
					ret3 = fil_io(OS_FILE_READ,TRUE,FLASH_CACHE_SPACE,0,b->fil_offset,0,UNIV_PAGE_SIZE,&read_buf,NULL);
					ut_ad(ret3);
					lsn3 = mach_read_from_8(read_buf+FIL_PAGE_LSN);
					space3 = mach_read_from_4(read_buf+FIL_PAGE_ARCH_LOG_NO_OR_SPACE_ID);
					offset3 = mach_read_from_4(read_buf+FIL_PAGE_OFFSET);
					ut_ad( space3 == space_id );
					ut_ad( offset3 == offset );
					ut_ad(lsn3>=lsn);
#endif
					/* page in flash cache should always newer than page in disk */
					flash_cache_hash_mutex_exit(space_id,offset);
					continue;
				}
				else{
					b = &fc->block[(fc->write_off)%fc_get_size()];
					ut_a( b->state == BLOCK_NOT_USED );
					b->space = space_id;
					b->offset = offset;
					b->state = BLOCK_READ_CACHE;
#ifdef UNIV_FLASH_DEBUG
					fprintf(stderr," warmup space: %lu, page no: %lu.\n",space_id,offset);
#endif
					HASH_INSERT(fc_block_t,hash,fc->hash_table,
						buf_page_address_fold(b->space, b->offset),
						b);
					success = fil_io(OS_FILE_WRITE | OS_AIO_SIMULATED_WAKE_LATER,FALSE,FLASH_CACHE_SPACE,0,b->fil_offset,0,UNIV_PAGE_SIZE,page,NULL);	
					if ( success != DB_SUCCESS ){
						ut_print_timestamp(stderr);
						fprintf(stderr," InnoDB [Error]: Can not recover tablespace %lu, offset is %lu.\n",
							space_id,offset);
						ut_error;
					}
					srv_flash_cache_used = srv_flash_cache_used + 1;
				}
				flash_cache_hash_mutex_exit(space_id,offset);
				fc->write_off = (fc->write_off + 1) % fc_get_size();
				fc->flush_off = (fc->flush_off + 1) % fc_get_size(); 
				if ( fc->write_off == 0 ){
					os_aio_simulated_wake_handler_threads();
					os_aio_wait_until_no_pending_fc_writes();
					fil_flush_file_spaces(FIL_FLASH_CACHE);
					ut_free(buf_unaligned);
					ut_print_timestamp(stderr);
					fprintf(stderr," InnoDB: warm up table %s.%s to space: %lu offset %lu.(100%%)\n",dbname,tablename,space_id,i);
					ut_print_timestamp(stderr);
					fprintf(stderr," InnoDB: flash cache is full, warm up stop.\n");
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
fc_warmup_tablespaces(
/*=============================*/
)
{
	int		ret;
	char*		dbpath		= NULL;
	ulint		dbpath_len	= 100;
	os_file_dir_t	dir;
	os_file_dir_t	dbdir;
	os_file_stat_t	dbinfo;
	os_file_stat_t	fileinfo;
	ulint		err		= DB_SUCCESS;

	if ( srv_flash_cache_size == 0 || !fc_log->first_use){
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

		if (dbinfo.type == OS_FILE_TYPE_FILE
		    || dbinfo.type == OS_FILE_TYPE_UNKNOWN) {

			goto next_datadir_item;
		}

		/* We found a symlink or a directory; try opening it to see
		if a symlink is a directory */

		len = strlen(fil_path_to_mysql_datadir)
			+ strlen (dbinfo.name) + 2;
		if (len > dbpath_len) {
			dbpath_len = len;

			if (dbpath) {
				mem_free(dbpath);
			}

			dbpath = (char*)mem_alloc(dbpath_len);
		}
		sprintf(dbpath, "%s/%s", fil_path_to_mysql_datadir,
			dbinfo.name);
		srv_normalize_path_for_win(dbpath);

		dbdir = os_file_opendir(dbpath, FALSE);

		if (dbdir != NULL) {
			/* printf("Opened dir %s\n", dbinfo.name); */

			/* We found a database directory; loop through it,
			looking for possible .ibd files in it */

			ret = fil_file_readdir_next_file(&err, dbpath, dbdir,
							 &fileinfo);
			while (ret == 0) {
				/* printf(
				"     Looking at file %s\n", fileinfo.name); */

				if (fileinfo.type == OS_FILE_TYPE_DIR) {

					goto next_file_item;
				}

				/* We found a symlink or a file */
				if (strlen(fileinfo.name) > 4
				    && 0 == strcmp(fileinfo.name
						   + strlen(fileinfo.name) - 4,
						   ".ibd")) {
					/* The name ends in .ibd; try opening
					the file */
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

					/* Preload to flash cache */
					if  (fc_warmup_tablespace(file,dbinfo.name,strtok(fileinfo.name,"."),space_id) == FALSE ){
						goto finish;	
					}
					
					os_file_close(file);
					ut_free(buf2);
					mem_free(filepath);
				}
next_file_item:
				ret = fil_file_readdir_next_file(&err,
								 dbpath, dbdir,
								 &fileinfo);
			}

			if (0 != os_file_closedir(dbdir)) {
				fputs("InnoDB: Warning: could not"
				      " close database directory ", stderr);
				ut_print_filename(stderr, dbpath);
				putc('\n', stderr);

				err = DB_ERROR;
			}
		}

next_datadir_item:
		ret = fil_file_readdir_next_file(&err,
						 fil_path_to_mysql_datadir,
						 dir, &dbinfo);
	}
finish:
	flash_cache_mutex_enter();
	fc_log_update(FALSE);
	fc_log_commit();
	flash_cache_mutex_exit();
	ut_print_timestamp(stderr);
	fprintf(stderr," InnoDB: flash cache warm up finish.\n");
#ifdef UNIV_FLASH_DEBUG
	buf_flush_flash_cache_validate();
#endif

	mem_free(dbpath);
}