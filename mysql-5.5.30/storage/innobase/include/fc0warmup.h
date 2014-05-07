/**************************************************//**
@file fc/fc0warmup.c
Flash Cache warmup

Created by 24/4/2012 David Jiang (jiangchengyao@gmail.com)
Modified by 24/10/2013 Thomas Wen (wenzhenghu.zju@gmail.com)
*******************************************************/

#ifndef fc0warmup_h
#define fc0warmup_h

#include "fc0fc.h"


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
	byte*	page);			/*<! the page buffer to temply store the warmup data */

/******************************************************************//**
Load flash cache from warmup file */
UNIV_INTERN
void
fc_load_warmup_file(void);
/*==================*/

/********************************************************************//**
Warm up tablespaces to flash cache block.,stop if no space left. */
UNIV_INTERN
void
fc_warmup_tablespaces(void);
/*=============================*/

#ifndef UNIV_NONINL
#include "fc0warmup.ic"
#endif

#endif
