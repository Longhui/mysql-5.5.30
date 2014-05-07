#ifndef INNODB_LIST_H
#define INNODB_LIST_H
#include <string.h>

/* we use list as the LRU structure, the newest is at the end, while 
the oldest i sat the start */
#define List(type)\
struct\
{\
	unsigned	count;\
	type*		first;\
	type*		last;\
}

/* ls is a pointer to a list_struct of known type */
#define list_init(ls)\
do\
{\
	ls = calloc(1, sizeof(*ls));\
}while(0)

#define list_deinit(ls)\
do\
{\
	if(ls)\
		free(ls);\
}while(0)

/* link_name is a member of node used to link the next node 
     we don't use this macro in this program */
#define list_add_2_first(ls, node, link_name)\
do\
{\
	node->link_name = ls->first;\
	ls->first = node;\
	if(!ls->count)\
		ls->last = node;\
	++ls->count;\
}while(0)

#define list_add_2_last(ls, node, link_name)\
do\
{\
	node->link_name = NULL;\
	if(ls->count)\
	{\
		ls->last->link_name = node;\
	    ls->last = node;\
	}\
	else\
		ls->first = ls->last = node;\
	++ls->count;\
}while(0)

/* be sure the list  is not empty */
#define list_delete_first(ls, first_node, link_name)\
do\
{\
	first_node = ls->first;\
	ls->first = first_node->link_name;\
	first_node->link_name = NULL;\
	--ls->count;\
	if(!ls->count)\
		ls->last = NULL;\
}while(0)

#define list_is_empty(ls) !ls->count


#endif
