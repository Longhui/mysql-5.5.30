#include "sql_iostat.h"
#include "sql_class.h"

const static my_bool *is_io_stat_used = NULL;
  
void thd_io_incr(uint stat_type)
{
	THD *thd = current_thd;
	if(thd == NULL 
	 || (((thd->variables.option_bits & OPTION_PROFILING) == 0)
	 && (is_io_stat_used == NULL || !(*is_io_stat_used)))) return;
	switch(stat_type)
	{
		case LOG_READ:thd->logical_reads++;break;
		case PHY_READ:thd->physical_reads++;break;
		default:break;
	}
}

void set_io_stat_flag(const my_bool *flag)
{
	is_io_stat_used = flag;
}
