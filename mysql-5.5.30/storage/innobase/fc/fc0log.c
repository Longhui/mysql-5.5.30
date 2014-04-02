/**************************************************//**
@file fc/fc0log.c
Flash Cache log

Created	24/4/2012 David Jiang (jiangchengyao@gmail.com)
*******************************************************/

#include "fc0log.h"
#include "fc0fc.h"

/* flash cache log structure */
UNIV_INTERN fc_log_t* fc_log = NULL;
/* flash cache key */
 UNIV_INTERN  mysql_pfs_key_t innodb_flash_cache_file_key;

/*********************************************************************//**
Creates or opens the flash cache data files and closes them.
@return	DB_SUCCESS or error code */
static
ulint
fc_open_or_create_file()
{	
	ibool	ret;
	ulint	size;
	ulint	size_high;
	ulint	low32;
	ulint	high32;
	os_file_t file;
	ulint flash_cache_size = srv_flash_cache_size / UNIV_PAGE_SIZE;
	low32 = (0xFFFFFFFFUL & (flash_cache_size << UNIV_PAGE_SIZE_SHIFT));
	high32 = (flash_cache_size >> (32 - UNIV_PAGE_SIZE_SHIFT));
	file = os_file_create(innodb_flash_cache_file_key, srv_flash_cache_file, OS_FILE_CREATE, OS_FILE_NORMAL, OS_LOG_FILE, &ret);
	if (ret == FALSE)
	{
		if (os_file_get_last_error(FALSE) != OS_FILE_ALREADY_EXISTS
#ifdef UNIV_AIX
		/* AIX 5.1 after security patch ML7 may have errno set
		to 0 here, which causes our function to return 100;
		work around that AIX problem */
		&& os_file_get_last_error(FALSE) != 100
#endif
		) {
			ut_print_timestamp(stderr);
			fprintf(stderr," InnoDB: Error in creating or opening %s\n", srv_flash_cache_file);
			return(DB_ERROR);
		}
			
		file = os_file_create(innodb_flash_cache_file_key, srv_flash_cache_file,OS_FILE_OPEN, OS_FILE_AIO,OS_LOG_FILE, &ret);
		if (!ret)
		{
			ut_print_timestamp(stderr);
			fprintf(stderr,	" InnoDB: Error in opening %s\n", srv_flash_cache_file);
			return(DB_ERROR);
		}
		ret = os_file_get_size(file, &size, &size_high);
		if(high32 > size_high || (high32 == size_high && low32 > size))
		//if (size != low32 || size_high != high32)
		{
			ut_print_timestamp(stderr);
			fprintf(stderr,
				" InnoDB: Error: flash cache file %s is"
				" of smaller size %lu %lu bytes\n"
				"InnoDB: than specified in the .cnf"
				" file %lu %lu bytes!\n",
				srv_flash_cache_file, (ulong) size_high, (ulong) size,	
				(ulong)low32,(ulong)high32);
			return(DB_ERROR);
		}
	}
	else
	{
		ut_print_timestamp(stderr);
		fprintf(stderr,
		"  InnoDB: flash cache file %s did not exist:"
		" new to be created\n",
		srv_flash_cache_file);
		fprintf(stderr, "InnoDB: Setting flash cache file %s size to %lu MB\n",
		srv_flash_cache_file, (ulong) flash_cache_size >> (20 - UNIV_PAGE_SIZE_SHIFT));
		fprintf(stderr,
		"InnoDB: Database physically writes the file"
		" full: wait...\n");
		ret = os_file_set_size(srv_flash_cache_file, file,low32,high32);
		if (!ret)
		{
			ut_print_timestamp(stderr);
			fprintf(stderr,
			" InnoDB: Error in creating %s:"
			" probably out of disk space\n",
			srv_flash_cache_file);
			return(DB_ERROR);
		}
	}
	ret = os_file_close(file);
	return(DB_SUCCESS);
}

/****************************************************************//**
Initialize flash cache log.*/
UNIV_INTERN
void
fc_log_create(
/*==========================================*/
){
	ulint ret;

	fc_log = (fc_log_t*)ut_malloc(sizeof(fc_log_t));
	fc_log->buf_unaligned = (byte*)ut_malloc(FLASH_CACHE_BUFFER_SIZE*2);
	fc_log->buf = (byte*)ut_align(fc_log->buf_unaligned,FLASH_CACHE_BUFFER_SIZE);

	fc_log->file = os_file_create(innodb_file_data_key, srv_flash_cache_log_file_name,
				  OS_FILE_CREATE, OS_FILE_NORMAL,
				  OS_DATA_FILE, &ret);
	
	if ( ret ){
		memset(fc_log->buf,'\0',FLASH_CACHE_BUFFER_SIZE);
		/* Create file success, it is the first time to create log file. */

		fc_log->enable_write_curr = (ulint)srv_flash_cache_enable_write;
		fc_log->write_offset_bck = 0XFFFFFFFFUL;/* for recovery */

		mach_write_to_4(fc_log->buf+FLASH_CACHE_LOG_WRITE_MODE, srv_flash_cache_write_mode);
		mach_write_to_4(fc_log->buf+FLASH_CACHE_LOG_CHKSUM, FLASH_CACHE_LOG_CHECKSUM);
		mach_write_to_4(fc_log->buf+FLASH_CACHE_LOG_CHKSUM2, FLASH_CACHE_LOG_CHECKSUM);
		mach_write_to_4(fc_log->buf+FLASH_CACHE_LOG_ENABLE_WRITE, fc_log->enable_write_curr);
		mach_write_to_4(fc_log->buf+FLASH_CACHE_LOG_WRITE_OFFSET_BCK,fc_log->write_offset_bck);
		
		os_file_write(srv_flash_cache_log_file_name,fc_log->file,fc_log->buf,0,0,FLASH_CACHE_BUFFER_SIZE);
		os_file_flush(fc_log->file);
		
		fc_log->first_use = TRUE;

	}
	else{
		/* We need to open the file */
		fc_log->file = os_file_create(innodb_file_data_key, srv_flash_cache_log_file_name,
					OS_FILE_OPEN, OS_FILE_NORMAL,
					OS_DATA_FILE, &ret);
		if ( !ret ){
			ut_print_timestamp(stderr);
			fprintf(stderr," InnoDB [Error]: Can't open flash cache log.\n");
			ut_error;
		}
		os_file_read(fc_log->file,fc_log->buf,0,0,FLASH_CACHE_BUFFER_SIZE);

		ut_a(mach_read_from_4(fc_log->buf+FLASH_CACHE_LOG_CHKSUM) == 
			 mach_read_from_4(fc_log->buf+FLASH_CACHE_LOG_CHKSUM2));
		
		/* don't allow to change write mode*/
		if(srv_flash_cache_write_mode != mach_read_from_4(fc_log->buf+FLASH_CACHE_LOG_WRITE_MODE))
		{
			ut_print_timestamp(stderr);
			fprintf(stderr," InnoDB: cann't change flash cache write mode from %lu to %lu, just ignore the change\n", 
				mach_read_from_4(fc_log->buf+FLASH_CACHE_LOG_WRITE_MODE), srv_flash_cache_write_mode);
			
			srv_flash_cache_write_mode = mach_read_from_4(fc_log->buf+FLASH_CACHE_LOG_WRITE_MODE);
		}

		/* use fc_log in disk to init the fc_log memory object*/
		fc_log->first_use = FALSE;
		fc_log->flush_offset = mach_read_from_4(fc_log->buf + FLASH_CACHE_LOG_FLUSH_OFFSET );
		fc_log->flush_round = mach_read_from_4(fc_log->buf + FLASH_CACHE_LOG_FLUSH_ROUND );
		fc_log->write_offset = mach_read_from_4(fc_log->buf + FLASH_CACHE_LOG_WRITE_OFFSET );
		fc_log->write_round = mach_read_from_4(fc_log->buf + FLASH_CACHE_LOG_WRITE_ROUND );
		fc_log->enable_write_curr = mach_read_from_4(fc_log->buf + FLASH_CACHE_LOG_ENABLE_WRITE);
		fc_log->write_offset_bck = mach_read_from_4(fc_log->buf + FLASH_CACHE_LOG_WRITE_OFFSET_BCK);
		fc_log->write_round_bck = mach_read_from_4(fc_log->buf + FLASH_CACHE_LOG_WRITE_ROUND_BCK);

		/* use fc_log in memory object to init the fc memory object*/
		fc->write_round = fc_log->write_round;
		fc->write_off = fc_log->write_offset;
		fc->flush_off = fc_log->flush_offset;
		fc->flush_round = fc_log->flush_round;
		
	}
	
	if(!srv_flash_cache_is_raw)
	{
		fc_open_or_create_file();
	}
	ret = fil_space_create(srv_flash_cache_file, FLASH_CACHE_SPACE, 0, FIL_FLASH_CACHE);
	if ( !ret ){
		ut_print_timestamp(stderr);
		fprintf(stderr," InnoDB [Error]: fail to create flash cache file.\n");
		ut_error;
	} 
	fil_node_create(srv_flash_cache_file, srv_flash_cache_size, FLASH_CACHE_SPACE, srv_flash_cache_is_raw);
}

/****************************************************************//**
Initialize flash cache log.*/
UNIV_INTERN
void
fc_log_update(
/*==========================================*/
ulint backup
){
	fc_log->flush_offset = fc->flush_off;
	fc_log->flush_round = fc->flush_round;
	fc_log->write_offset = fc->write_off;
	fc_log->write_round = fc->write_round;
	fc_log->enable_write_curr = srv_flash_cache_enable_write;

	if (backup)
	{
		fc_log->write_offset_bck = fc->write_off;
		fc_log->write_round_bck = fc->write_round;
		ut_print_timestamp(stderr);
		fprintf(stderr," InnoDB: flash cache write_offset/round_bck(%d,%d) updated.\n", fc->write_off, fc->write_round);
	}
	
	return;
}

/****************************************************************//**
Free flash cache log.*/
UNIV_INTERN
void
fc_log_destroy(
/*==========================================*/
){
	ut_free(fc_log->buf_unaligned);
	os_file_close(fc_log->file);
	ut_free(fc_log);
}

/*********************************************************************//**
write flash cache log to log buffer and commit.*/
UNIV_INTERN
void
fc_log_commit(
/*==========================================*/
){
	ut_ad(mutex_own(&fc->mutex));

	mach_write_to_4(fc_log->buf + FLASH_CACHE_LOG_CHKSUM, FLASH_CACHE_LOG_CHECKSUM);
	mach_write_to_4(fc_log->buf + FLASH_CACHE_LOG_FLUSH_OFFSET, fc_log->flush_offset);
	mach_write_to_4(fc_log->buf + FLASH_CACHE_LOG_FLUSH_ROUND, fc_log->flush_round);
	mach_write_to_4(fc_log->buf + FLASH_CACHE_LOG_WRITE_OFFSET, fc_log->write_offset);
	mach_write_to_4(fc_log->buf + FLASH_CACHE_LOG_WRITE_ROUND, fc_log->write_round);

	mach_write_to_4(fc_log->buf + FLASH_CACHE_LOG_ENABLE_WRITE, fc_log->enable_write_curr);
	mach_write_to_4(fc_log->buf + FLASH_CACHE_LOG_WRITE_OFFSET_BCK, fc_log->write_offset_bck);
	mach_write_to_4(fc_log->buf + FLASH_CACHE_LOG_WRITE_ROUND_BCK, fc_log->write_round_bck);

	mach_write_to_4(fc_log->buf + FLASH_CACHE_LOG_CHKSUM2, FLASH_CACHE_LOG_CHECKSUM);

	os_file_write(srv_flash_cache_log_file_name, fc_log->file, fc_log->buf, 0, 0, FLASH_CACHE_BUFFER_SIZE);
	os_file_flush(fc_log->file);
}
