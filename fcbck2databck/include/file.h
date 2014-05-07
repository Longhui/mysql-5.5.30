#ifndef INNODB_FILE_H
#define INNODB_FILE_H

#include "common.h"


BOOL init(char* datadir);
void deinit();

unsigned	get_space_id(page p);
unsigned	get_page_no(page p);
uint64_t	get_page_lsn(page p);
unsigned 	get_page_type(page p);



/* use (space_id, page_no) to read/write page from/to the ibdata or ibd file.
 * return TRUE if sucess, FALSE if fail */
BOOL	read_page_from_tablespace(unsigned space_id, unsigned page_no, page p);
BOOL	write_page_to_tablespace(unsigned space_id, unsigned page_no, page p);


#endif
