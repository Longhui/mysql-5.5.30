/**************************************************//**
@file fc/fc0log.c
Flash Cache log

Created	24/4/2012 David Jiang (jiangchengyao@gmail.com)
*******************************************************/

#ifndef fc0log_h
#define fc0log_h

#include "univ.i"

#define FLASH_CACHE_BUFFER_SIZE			512UL

#define FLASH_CACHE_LOG_CHKSUM				0
#define FLASH_CACHE_LOG_FLUSH_OFFSET		4
#define FLASH_CACHE_LOG_WRITE_OFFSET		8
#define FLASH_CACHE_LOG_FLUSH_ROUND			12
#define FLASH_CACHE_LOG_WRITE_ROUND			16
#define FLASH_CACHE_LOG_WRITE_MODE			20
#define FLASH_CACHE_LOG_ENABLE_WRITE		24	
#define FLASH_CACHE_LOG_WRITE_ROUND_BCK		28
#define FLASH_CACHE_LOG_WRITE_OFFSET_BCK	32
#define FLASH_CACHE_LOG_CHKSUM2			( FLASH_CACHE_BUFFER_SIZE - 4 )

#define FLASH_CACHE_LOG_CHECKSUM		4294967291UL


typedef struct fc_log_struct fc_log_t;

extern fc_log_t* fc_log;

/** Flash cache log */
struct fc_log_struct
{
#ifdef __WIN__
	void*	file;				/*<! file handle */
#else
	int			file;			/*<! file handle */
#endif
	byte*		buf;			/*<! log buffer(512 bytes) */
	byte*		buf_unaligned;	/*<! unaligned log buffer */
	ulint		flush_offset;	/*<! flash cache flush offset */
	ulint		flush_round;	/*<! flash cache flush round */
	ulint		write_offset;	/*<! flash cache write offset */
	ulint		write_round;	/*<! flash cache write round */
	ulint		write_offset_bck;	/*<! write offset when last time switch enable_write from false to true, for recovery */
	ulint		write_round_bck;	/*<! write round when last time switch enable_write from false to true, for recovery */
	ulint		enable_write_curr;	/*<! current enable_write value, when enable_write is changed */
	ibool		first_use;		/*<! whether flash cache is used the first time. if true, can do tablespace warmup,and no recovery is done */
};

/****************************************************************//**
Initialize flash cache log.*/
UNIV_INTERN
void
fc_log_create(
/*==========================================*/
);

/****************************************************************//**
Free flash cache log.*/
UNIV_INTERN
void
fc_log_destroy(
/*==========================================*/
);

#endif