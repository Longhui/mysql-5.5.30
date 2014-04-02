/**
 * NTSE自定义函数
 *
 * @author 余利华 (yulihua@corp.netease.com, ylh@163.org)
 */
#include "my_global.h"
#include "mysql.h"
#include <string.h>

/***************************************************************************
** UDF string function.
** Arguments:
** initid	Structure filled by xxx_init
** args		The same structure as to xxx_init. This structure
**		contains values for all parameters.
**		Note that the functions MUST check and convert all
**		to the type it wants!  Null values are represented by
**		a NULL pointer
** result	Possible buffer to save result. At least 255 byte long.
** length	Pointer to length of the above buffer.	In this the function
**		should save the result length
** is_null	If the result is null, one should store 1 here.
** error	If something goes fatally wrong one should store 1 here.
**
** This function should return a pointer to the result string.
** Normally this is 'result' but may also be an alloced string.
***************************************************************************/
extern "C" const char* ntse_backup_str(UDF_INIT *initid __attribute__((unused)),
				  UDF_ARGS *args, char *result, unsigned long *length,
				  char *is_null, char *error __attribute__((unused))) {
	const char *msg = "Backup successfully: ";
	const size_t len = strlen(msg);	
	// 检查参数的有效性
	if (args->arg_count != 1)
		return "bad parameter";
	if (args->arg_type[0] != STRING_RESULT)
		return "bad parameter";
	
	*length = (long)(len + args->lengths[0]);
	initid->ptr = (char *)malloc(*length);
	memcpy(initid->ptr, msg, len);
	memcpy(initid->ptr + len, args->args[0], args->lengths[0]);	
	return initid->ptr;
}


/*
At least one of _init/_deinit is needed unless the server is started
with --allow_suspicious_udfs.
*/
extern "C" my_bool ntse_backup_str_init(UDF_INIT *initid __attribute__((unused)),
						UDF_ARGS *args __attribute__((unused)),
						char *message __attribute__((unused)))
{
	initid->ptr = 0;
	return 0;
}


extern "C" void ntse_backup_str_deinit(UDF_INIT *initid)
{
	if (initid->ptr)
		free(initid->ptr);
}


/***************************************************************************
** UDF string function.
** Arguments:
** initid	Structure filled by xxx_init
** args		The same structure as to xxx_init. This structure
**		contains values for all parameters.
**		Note that the functions MUST check and convert all
**		to the type it wants!  Null values are represented by
**		a NULL pointer
** result	Possible buffer to save result. At least 255 byte long.
** length	Pointer to length of the above buffer.	In this the function
**		should save the result length
** is_null	If the result is null, one should store 1 here.
** error	If something goes fatally wrong one should store 1 here.
**
** This function should return a pointer to the result string.
** Normally this is 'result' but may also be an alloced string.
***************************************************************************/
extern "C" longlong ntse_backup_int(UDF_INIT *initid __attribute__((unused)),
				  UDF_ARGS *args, char *result, unsigned long *length,
				  char *is_null, char *error __attribute__((unused))) {

	// 检查参数的有效性
	if (args->arg_count != 1)
	  return -1;
	if (args->arg_type[0] != STRING_RESULT)
	  return -1;
	return 0;
}


/*
At least one of _init/_deinit is needed unless the server is started
with --allow_suspicious_udfs.
*/
extern "C" my_bool ntse_backup_int_init(UDF_INIT *initid __attribute__((unused)),
						 UDF_ARGS *args __attribute__((unused)),
						 char *message __attribute__((unused)))
{
	return 0;
}
