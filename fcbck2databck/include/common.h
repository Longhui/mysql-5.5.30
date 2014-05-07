#ifndef INNODB_COMMON_TYPE_H
#define INNODB_COMMON_TYPE_H

/* For different length of int */
#include <stdint.h> 
#include <stdio.h> 

#define TRUE 1
#define FALSE 0

typedef unsigned char BOOL;
typedef unsigned char byte;
typedef byte* page;

#define SPACE_PAGE_TYPE 24
#define SPACE_ID_OFFSET 34
#define SPACE_PAGE_NO_OFFSET 4
#define SPACE_PAGE_LSN_OFFSET  16
#define SPACE_FLAGS_OFFSET	54


#define UNIV_PAGE_SIZE 16384
#define KILO_BYTE	1024
#define PATH_NAME_MAX_LEN 4096

#define OS_FILE_READ 0
#define OS_FILE_WRITE 1

unsigned mach_read_from_2(byte* p);

unsigned mach_read_from_4(byte* p);

uint64_t mach_read_from_8(byte* p);

void* ut_align(const void* ptr, unsigned long align_no);

/**********************************************************//**
Prints a timestamp to a file. */
void ut_print_timestamp(FILE*  file); /*!< in: file where to print */


#endif
