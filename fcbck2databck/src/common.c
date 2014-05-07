#include "common.h"
#include <time.h>
#include <stdio.h>

unsigned mach_read_from_2(byte* p)
{
	    return ( ((unsigned) p[0] << 8) |
		         ((unsigned) p[1] ) );
}

unsigned mach_read_from_4(byte* p)
{
	    return ( ((unsigned) p[0] << 24) |
		         ((unsigned) p[1] << 16) |
				 ((unsigned) p[2] << 8)  |
				 ((unsigned) p[3]) );
}

uint64_t mach_read_from_8(byte* p)
{
	    return ( (uint64_t) mach_read_from_4(p) << 32  |
		         (uint64_t) mach_read_from_4(p + 4) );
}

void* ut_align(const void* ptr, unsigned long align_no)	
{
	return((void*)((((unsigned long) ptr) + align_no - 1) & ~(align_no - 1)));
}

/**********************************************************//**
Prints a timestamp to a file. */
void
ut_print_timestamp(
/*===============*/
	FILE*  file) /*!< in: file where to print */
{

	struct tm  cal_tm;
	struct tm* cal_tm_ptr;
	time_t	   tm;

	time(&tm);
	cal_tm_ptr = localtime(&tm);

	fprintf(file,"%02d%02d%02d %2d:%02d:%02d",
		cal_tm_ptr->tm_year % 100,
		cal_tm_ptr->tm_mon + 1,
		cal_tm_ptr->tm_mday,
		cal_tm_ptr->tm_hour,
		cal_tm_ptr->tm_min,
		cal_tm_ptr->tm_sec);
}


