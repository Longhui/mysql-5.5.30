#ifndef INNODB_HASH_H
#define INNODB_HASH_H

#include <stdlib.h>
#include <stdio.h>



/* 'hash_table' should be defined before this macro is called. ie, type* *hash_table; 
 * This description is also TRUE for the successive macros below. */
#define HASH_TABLE_CREATE_AND_INIT(hash_table, n)\
do\
{\
	hash_table = calloc(n, sizeof(void*));\
}while(0)


#define HASH_TABLE_DESTORY(hash_table)\
do\
{\
	if(hash_table)\
	{\
		free(hash_table);\
		hash_table = NULL;\
	}\
}while(0)
 
/* must be sure to insert at most n items.
 * 'item' is a pointer, and its struct must
 * have an  member to determine insert, delete
 * and serch, and 'id_name' is that member's name. */

#define HASH_TABLE_INSERT(hash_table, n, item, id_name)\
do\
{\
	size_t pos = item->id_name % (n);\
	while(hash_table[pos])\
		pos = (pos + 1) % (n);\
	hash_table[pos] = item;\
}while(0)

/* 'id' is a number*/
#define HASH_TABLE_DELETE(hash_table, n , id_name, id)\
do\
{\
	size_t pos = (id) % (n);\
	size_t count = 0;\
	while(hash_table[pos]->id_name != (id))\
	{\
		if(++count == (n))\
			break;\
		pos = (pos + 1) % (n);\
	}\
	if(count == (n))\
		fprintf(stderr, "no item deleted from the hash table for id %d is not exist\n", (id));\
	else\
	{\
		hash_table[pos] = NULL;\
	}\
}while(0)

/* use 'id' to find the 'item' */
#define HASH_TABLE_SEARCH(hash_table, n , item, id_name, id)\
do\
{\
	size_t pos = (id) % (n);\
	size_t count = 0;\
	while(hash_table[pos]->id_name != (id))\
	{\
		if(++count == (n))\
			break;\
		pos = (pos + 1) % (n);\
	}\
	if(count == (n))\
		item = NULL;\
	else\
	{\
		item = hash_table[pos];\
	}\
}while(0)

#endif
