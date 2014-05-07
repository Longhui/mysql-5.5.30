#include "file.h"
#include "common.h"
#include "hash.h"
#include "list.h"
#include <stdlib.h>
#include <sys/stat.h>
#include <dirent.h>
#include <string.h>
#include <stdio.h>
#include <errno.h>
#include <fcntl.h>
#include <assert.h>
#include <unistd.h>



typedef struct ibd_struct ibd_t;
typedef struct ibdata_struct ibdata_t;
typedef struct file_node_struct file_node_t;


struct file_node_struct
{
	char*			file_name;	/* dynamically allocated */
	int				fd;			/* only when file_node is in lru list, fd is avaliable. */
	unsigned		file_size;	/* size in page */
	file_node_t*	lru;		/*  file is open  <==> file_node is in the lru list:  <==>   (file_node->lru != NULL || file_node_list>last != file_node*/
};

struct ibd_struct
{
	file_node_t	file_node;
	unsigned	space_id;
	unsigned	flags;
};

struct ibdata_struct
{
	file_node_t*	file_nodes; /* an array of n_ibdata dimension(s) */
	unsigned		n_ibdata;	/* the number of ibdata files */	
	unsigned		size;		/* system tablespace' size in page */
};



/**********************************************************/
/* global variables */
ibd_t** 			hash_table_of_ibd;
ibd_t*  			ibds;
ibdata_t*			system_tablespace;
unsigned 			n_ibd; /* number of .ibd files, it's set by function get_n_ibd_and_ibdata */
List(file_node_t)*	file_node_list;

/**********************************************************/


static inline char* numbrs_to_str(unsigned n)
{
	static char str[12];
	sprintf(str, "%u", n);
	return str;
}
static inline unsigned flags_to_page_zip_size(unsigned flags)
{
	if(!flags)
		return UNIV_PAGE_SIZE;
	else
		return 512 << ((flags & 30) >> 1);
}

/* file_name is the basename of fiel path */
static inline BOOL file_is_ibd(char* file_name)
{
	return !memcmp(file_name + strlen(file_name) - 4, ".ibd", 4);
}

static inline BOOL file_is_ibdata(char* file_name)
{
	return !memcmp(file_name, "ibdata", 6);
}


/* scan the directory 'datadir' to find the number of ibd files and ibdata(s) 
 return TRUE if sucess 
 */
static BOOL get_n_ibd_and_ibdata
(
	char*     datadir, /* without terminating '/' */
	unsigned* n_ibdata
)
{
	DIR*			datadir_stream;
	struct stat 	stat_info;
	struct dirent*	dir_entry;
	int 			old_errno;
	DIR*			inner_dir_stream;
	struct stat 	inner_stat_info;
	struct dirent*	inner_dir_entry;
	char			file_path_1[PATH_NAME_MAX_LEN];
	char			file_path_2[PATH_NAME_MAX_LEN];

	n_ibd = *n_ibdata = 0;
	if(!(datadir_stream= opendir(datadir)))
	{
		fprintf(stderr, "Error: cann't open the directory %s to read\n", datadir);
		return FALSE;
	}
	old_errno = errno;
	while(dir_entry = readdir(datadir_stream))
	{
		if(dir_entry->d_name[0] == '.' || !strcmp(dir_entry->d_name, "mysql") || !strcmp(dir_entry->d_name, "performance_schema"))
			continue;
		/* we assume file path name is less than 4096 bytes */
		sprintf(file_path_1, "%s/%s", datadir, dir_entry->d_name);
		stat(file_path_1, &stat_info);
		if(S_ISREG(stat_info.st_mode) && file_is_ibdata(dir_entry->d_name))
			++*n_ibdata;	
		else if(S_ISDIR(stat_info.st_mode) || S_ISLNK(stat_info.st_mode))
		{
			
			if(!(inner_dir_stream = opendir(file_path_1)))
				continue;
			while(inner_dir_entry = readdir(inner_dir_stream))
			{
				sprintf(file_path_2, "%s/%s", file_path_1, inner_dir_entry->d_name);
				stat(file_path_2, &inner_stat_info); 
				if(S_ISREG(inner_stat_info.st_mode) && file_is_ibd(inner_dir_entry->d_name))
					++n_ibd;
			}
			if(errno != old_errno)
			{
				fprintf(stderr, "Error: cann't read entries from the stream of directory %s\n", file_path_1);
				fprintf(stderr, "Error: the stream is invalid\n");
				return FALSE;
			}
			closedir(inner_dir_stream);
			
		}
		
	}
	if(errno != old_errno)
	{
		fprintf(stderr, "Error: cann't read entries from the stream of directory %s\n", datadir);
		fprintf(stderr, "Error: the stream is invalid\n");
		return FALSE;
	}
	closedir(datadir_stream);
	
	return TRUE;
}

/* check if lru is NULL to determine whether the file is open or not,
** cann't test if fd is -1 to determine whether the file is open or not,
** for if the file is removed form the LRU list, then it is closed, but the
** fd value keep unchanged */
static void system_tablespace_init(char* datadir, unsigned n_ibdata)
{
	unsigned i;
	struct stat file_stat_info;

	system_tablespace = calloc(1, sizeof(ibdata_t));
	system_tablespace->file_nodes = calloc(n_ibdata, sizeof(file_node_t));
	system_tablespace->n_ibdata = n_ibdata;
	for(i = 1; i<= n_ibdata; ++i)
	{
		system_tablespace->file_nodes[i - 1].file_name = malloc(PATH_NAME_MAX_LEN);
		sprintf(system_tablespace->file_nodes[i - 1].file_name, "%s/%s%s", datadir, "ibdata", numbrs_to_str(i));
		system_tablespace->file_nodes[i - 1].fd = open(system_tablespace->file_nodes[i - 1].file_name, O_RDWR);
		if(system_tablespace->file_nodes[i - 1].fd != -1)
		{
			list_add_2_last(file_node_list, (system_tablespace->file_nodes + i - 1), lru);
		}
		stat(system_tablespace->file_nodes[i - 1].file_name, &file_stat_info);
		system_tablespace->file_nodes[i - 1].file_size = file_stat_info.st_size / UNIV_PAGE_SIZE;
		system_tablespace->size += system_tablespace->file_nodes[i - 1].file_size;
	}
}


/* check if lru is NULL to determine whether the file is open or not,
** cann't test if fd is -1 to determine whether the file is open or not,
** for if the file is removed form the LRU list, then it is closed, but the
** fd value keep unchanged */
static void ibds_init(char* datadir)
{
	unsigned 		i;
	DIR*			datadir_stream;
	struct stat 	stat_info;
	struct dirent*	dir_entry;
	int 			old_errno;
	DIR*			inner_dir_stream;
	struct stat 	inner_stat_info;
	struct dirent*	inner_dir_entry;
	char			file_path_1[PATH_NAME_MAX_LEN];
	char			file_path_2[PATH_NAME_MAX_LEN];
	file_node_t*	first_node;
	byte			un_aligned_buf[2 * UNIV_PAGE_SIZE];
	byte*			buf;
	
	ibds = calloc(n_ibd, sizeof(ibd_t));
	buf  = ut_align(un_aligned_buf, UNIV_PAGE_SIZE);

	if(!(datadir_stream= opendir(datadir)))
	{
		fprintf(stderr, "Error: cann't open the directory %s to read\n", datadir);
		return ;
	}
	old_errno = errno;
	i = 0;
	while(dir_entry = readdir(datadir_stream))
	{
		if(dir_entry->d_name[0] == '.' || !strcmp(dir_entry->d_name, "mysql") || !strcmp(dir_entry->d_name, "performance_schema"))
			continue;
		/* we assume file path name is less than 4096 bytes */
		sprintf(file_path_1, "%s/%s", datadir, dir_entry->d_name);
		stat(file_path_1, &stat_info);	
		if(S_ISDIR(stat_info.st_mode) || S_ISLNK(stat_info.st_mode))
		{
			if(!(inner_dir_stream = opendir(file_path_1)))
				continue;
			while(inner_dir_entry = readdir(inner_dir_stream))
			{
				sprintf(file_path_2, "%s/%s", file_path_1, inner_dir_entry->d_name);
				stat(file_path_2, &inner_stat_info); 
				if(S_ISREG(inner_stat_info.st_mode) && file_is_ibd(inner_dir_entry->d_name))
				{
					ibds[i].file_node.file_name = malloc(PATH_NAME_MAX_LEN);
					strcpy(ibds[i].file_node.file_name, file_path_2);

loop:
					fprintf(stderr, "info: open ibd file: %s \n", ibds[i].file_node.file_name);
					ibds[i].file_node.fd = open(ibds[i].file_node.file_name, O_RDWR); /* must open it to read */
					fprintf(stderr, "info:open ibd file: %s end \n",ibds[i].file_node.file_name);
					if(ibds[i].file_node.fd == -1)
					{
						list_delete_first(file_node_list, first_node, lru);
						fprintf(stderr,"failed to open the file: %s,try again\n",ibds[i].file_node.file_name);
						//fprintf(stderr, "warning:ibd fd : %s end \n",ibds[i].file_node.file_name);
						close(first_node->fd);
						goto loop;	
					}
					fprintf(stderr, "info:open ibd file: %s end \n",ibds[i].file_node.file_name);
					list_add_2_last(file_node_list,(&ibds[i].file_node),lru);
					pread(ibds[i].file_node.fd, buf, UNIV_PAGE_SIZE, 0);
					ibds[i].space_id = mach_read_from_4(buf + SPACE_ID_OFFSET);
					ibds[i].flags = mach_read_from_4(buf + SPACE_FLAGS_OFFSET);
					ibds[i].file_node.file_size = inner_stat_info.st_size / flags_to_page_zip_size(ibds[i].flags);
					fprintf(stderr, "idb info: space_id: %d, flags:%d add to hash table\n", ibds[i].space_id, ibds[i].flags);
					HASH_TABLE_INSERT(hash_table_of_ibd, n_ibd ,(ibds + i),space_id);
					++i;
				}
			}
			if(errno != old_errno)
			{
				fprintf(stderr, "Error: cann't read entries from the stream of directory %s\n", file_path_1);
				fprintf(stderr, "Error: the stream is invalid\n");
				return ;
			}	
			closedir(inner_dir_stream);
		}
	}
	if(errno != old_errno)
	{
		fprintf(stderr, "Error: cann't read entries from the stream of directory %s\n", datadir);
		fprintf(stderr, "Error: the stream is invalid\n");
		return ;
	}	
	closedir(datadir_stream);
}


/* return the number of *.ibd files for function deinit*/	
BOOL init(char* datadir)
{
	unsigned n_ibdata;
	
	if(!get_n_ibd_and_ibdata(datadir, &n_ibdata))
		return FALSE;
 
	list_init(file_node_list);	/* file_node_list must be innitialized first */
	HASH_TABLE_CREATE_AND_INIT(hash_table_of_ibd,n_ibd);
	
	system_tablespace_init(datadir, n_ibdata);
	ibds_init(datadir);
	return TRUE;
}

/* check if lru is NULL to determine whether the file is open or not,
** cann't test if fd is -1 to determine whether the file is open or not,
** for if the file is removed form the LRU list, then it is closed, but the
** fd value keep unchanged */
void deinit()
{
	unsigned i;
	file_node_t*	first_node;

	/* free hash table */
	HASH_TABLE_DESTORY(hash_table_of_ibd);

	/* close all opened files */
	while(!list_is_empty(file_node_list))
	{
		list_delete_first(file_node_list, first_node, lru);
		fsync(first_node->fd);
		close(first_node->fd);
	}
	list_deinit(file_node_list);
	
	/* free system_tablespace */
	for(i = 0; i < system_tablespace->n_ibdata; ++i)
	{
		free(system_tablespace->file_nodes[i].file_name);
	}
	free(system_tablespace->file_nodes);
	free(system_tablespace);

	/* free ibds*/
	for(i = 0; i < n_ibd; ++i)
	{
		free(ibds[i].file_node.file_name);
	}
	free(ibds);
	
}





unsigned	get_space_id(page p)
{
	return mach_read_from_4(p + SPACE_ID_OFFSET);
}

unsigned	get_page_no(page p)
{
	return mach_read_from_4(p + SPACE_PAGE_NO_OFFSET);
}

uint64_t	get_page_lsn(page p)
{
	return mach_read_from_8(p + SPACE_PAGE_LSN_OFFSET);
} 

unsigned 	get_page_type(page p)
{
	return mach_read_from_2(p + SPACE_PAGE_TYPE);
}


/* check if lru is NULL to determine whether the file is open or not,
** cann't test if fd is -1 to determine whether the file is open or not,
** for if the file is removed form the LRU list, then it is closed, but the
** fd value keep unchanged */
static BOOL tablespace_page_io(unsigned type, unsigned space_id, unsigned page_no, page p)
{
	file_node_t* first_node;	/* first file_node in lru list, which will be removed first */
	file_node_t* wanted_file_node;
	unsigned     file_pos; /* in page_size */
	unsigned 	 page_size;
	unsigned 	 i;	/*   look up ibdata */
	ibd_t*	 	 ibd;

	assert(type == OS_FILE_READ || type == OS_FILE_WRITE);
	
	if(0 == space_id) 	/* system_tablespace */
	{
		assert(page_no < system_tablespace->size);

		i = 0;
		file_pos =  page_no;
		while(file_pos >= system_tablespace->file_nodes[i].file_size)
		{
			file_pos -= system_tablespace->file_nodes[i].file_size;
			++i;
		}
		assert(i < system_tablespace->n_ibdata);
		wanted_file_node = system_tablespace->file_nodes + i;
		page_size = UNIV_PAGE_SIZE;
	}
	else
	{
		HASH_TABLE_SEARCH(hash_table_of_ibd, n_ibd, ibd, space_id, space_id);
		//assert(ibd);
		if (ibd == NULL)
		{
			fprintf(stderr, "panic: ibd is null in tablespace_page_io, type:%d, space_id:%d, page_no:%d\n", type, space_id, page_no);
			return FALSE;
		}
		
		assert(page_no < ibd->file_node.file_size);
		wanted_file_node = &ibd->file_node;
		page_size = flags_to_page_zip_size(ibd->flags);
		file_pos =  page_no;
	}

	if(!wanted_file_node->lru && file_node_list->last != wanted_file_node)  /* Caution: when wanted_file_node is in the end of file_node_list, lru is NULL*/
	{
		list_delete_first(file_node_list, first_node,lru);
		fsync(first_node->fd);
		close(first_node->fd);
		wanted_file_node->fd = open(wanted_file_node->file_name, O_RDWR);
		assert(wanted_file_node->fd != -1);
		list_add_2_last(file_node_list,wanted_file_node,lru);
	}
	
		if(type == OS_FILE_READ) 
		{
			if(pread(wanted_file_node->fd, p, page_size, (off_t) file_pos * page_size) != page_size)
			{
				fprintf(stderr, "Error: read error occured from file %s (space_id = %u, page_no = %u): ", wanted_file_node->file_name, space_id, page_no);
				perror(NULL);
				return FALSE;
			}	
		}
		
		else	/* type == OS_FILE_WRITE */
		{
			if(pwrite(wanted_file_node->fd, p, page_size, (off_t) file_pos * page_size) != page_size)
			{
				fprintf(stderr, "Error: write error occured to file %s (space_id = %u, page_no = %u): ", wanted_file_node->file_name, space_id, page_no);
				perror(NULL);
				return FALSE;
			}
		}	
	return TRUE;
}

/* no matter it's a normal page or a zipped page, store it in page p, which lenth is UNIV_PAGE_SIZE */
BOOL read_page_from_tablespace(unsigned space_id, unsigned page_no, page p)
{
	return tablespace_page_io(OS_FILE_READ, space_id, page_no, p);
}

/* no matter it's a normal page or a zipped page, store it in page p, which lenth is UNIV_PAGE_SIZE */
BOOL write_page_to_tablespace(unsigned space_id, unsigned page_no, page p)
{
	return tablespace_page_io(OS_FILE_WRITE, space_id, page_no, p);
}

