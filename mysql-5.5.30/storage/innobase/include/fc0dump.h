/**************************************************//**
@file fc/fc0dump.c
Flash Cache dump and load

Created	24/4/2012 David Jiang (jiangchengyao@gmail.com)
*******************************************************/

#ifndef fc0dump_h
#define fc0dump_h

#include "fc0fc.h"

/******************************************************************//**
Dump blocks from flash cache to file*/
UNIV_INTERN
void
fc_dump(
/*==================*/
);
/******************************************************************//**
Load flash cache from dump file */
UNIV_INTERN
void
fc_load(
/*==================*/
);
/******************************************************************//**
Load flash cache from warmup file */
UNIV_INTERN
void
fc_load_warmup_file(
/*==================*/
);
/********************************************************************//**
Warm up tablespaces to flash cache block.,stop if no space left. */
UNIV_INTERN
void
fc_warmup_tablespaces(
/*=============================*/
);

#endif