#include "common.h"
#include "file.h"
#include <stdio.h>
#include <string.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <assert.h>

typedef struct fc_bkp_info_struct fc_bkp_info_t;
typedef struct fc_bkp_blkmeta_strcut fc_bkp_blkmeta_t;

/** flash cache backup file metadata struct */
struct fc_bkp_info_struct
{
	unsigned long int	bkp_page_pos;		/*!<the position store the first block data*/			
	unsigned long int	bkp_blk_meta_pos;	/*!< the position store the block metadata */
	unsigned long int	bkp_blk_count;		/*!< the total number of blocks in backup file */
};

/** flash cache backup file block metadata struct */
struct fc_bkp_blkmeta_strcut
{
	unsigned long int blk_space:32; 	/*!<block space id*/
	unsigned long int blk_offset:32; 	/*!<block offset in the space*/
	unsigned long int blk_size; 	  	/*!<block size, with kb*/
};

int main(int argc, char** argv)
{
	char* 		data_dir;
	char* 		fc_bck_path;
	int			fc_bck_fd;
	unsigned 	i;
	unsigned 	fc_bck_file_size;
	byte 		unaligned_fc_buf[2 * UNIV_PAGE_SIZE];
	byte 		unaligned_file_node_buf[2 * UNIV_PAGE_SIZE];
	page 		fc_buf;
	page 		file_node_buf;
	uint64_t 	lsn_in_fc;
	uint64_t 	lsn_in_disk;
	unsigned 	space_id;
	unsigned 	page_no;
	unsigned		blkmeta_size;
	unsigned		blkmeta_offset;
	fc_bkp_info_t* bkp_info = (fc_bkp_info_t *)malloc(sizeof(fc_bkp_info_t));
	fc_bkp_blkmeta_t* blkmeta_array;
	unsigned		bck_page_offset;
	unsigned		blk_size;
	//struct stat stat_info;
	unsigned 	n_restored_from_fc = 0;
	

	/* cmdline arguments parse: BEGIN */
	
	if(1 == argc) {
		fprintf(stderr, "No option specified\n");
		fprintf(stderr, "Use 'fcbck2databck -h' or 'fcbck2databck --help' for help\n");
		return 1;
	}
	
	if(2 == argc) {
		if(!strcmp(argv[1], "-h") || !strcmp(argv[1], "--help")) {
			fprintf(stderr, "Usage: fcbck2databck option1 option2\n");
			fprintf(stderr, "Option's order in the command line arguments list is not matter,\n");
			fprintf(stderr, "but both 2 options must be specified.\n\n");
			fprintf(stderr, "Options are list below:\n");
			fprintf(stderr, "  --data_bck_dir=DATA_BCK_DIR\t DATA_BCK_DIR specifies which directory the backuped MySQL data located\n");
			fprintf(stderr, "  --fc_bck_path=FC_BCK_PATH\t FC_BCK_PATH specifies the path name of the backuped flash cache file\n");
		} else {
			fprintf(stderr, "Option specified is not available\n");
			fprintf(stderr, "Use 'fcbck2databck -h' or 'fcbck2databck --help' for help\n");
		}
		return 1;
	}

	if(argc > 3) {
		fprintf(stderr, "Options specified are not available\n");
		fprintf(stderr, "Use 'fcbck2databck -h' or 'fcbck2databck --help' for help\n");
		return 1;
	}

	/* argc == 3*/
	if(!strncmp(argv[1], "--data_bck_dir=", strlen("--data_bck_dir=")) &&
	    !strncmp(argv[2], "--fc_bck_path=", strlen("--fc_bck_path="))) {
		data_dir = argv[1] + strlen("--data_bck_dir=");
		fc_bck_path  = argv[2] + strlen("--fc_bck_path="); 
	} else if(!strncmp(argv[2], "--data_bck_dir=", strlen("--data_bck_dir=")) &&
	    	!strncmp(argv[1], "--fc_bck_path=", strlen("--fc_bck_path="))) {
		data_dir = argv[2] + strlen("--data_bck_dir=");
		fc_bck_path  = argv[1] + strlen("--fc_bck_path="); 
	} else {
		fprintf(stderr, "Options specified are not available\n");
		fprintf(stderr, "Use 'fcbck2databck -h' or 'fcbck2databck --help' for help\n");
		return 1;
	}
	/* cmdline arguments parse: END */

	/* flash cache backup file must be opened first and keep open unitl program terminated */
	if(-1 == (fc_bck_fd = open(fc_bck_path, O_RDONLY))) {
		fprintf(stderr, "Error: cann't open file %s to read: ", fc_bck_path);
		perror(NULL);
		fprintf(stderr, "Restore from flash cache backuped file failed, try again later\n");
		return 1;
	}
	
	fc_buf = ut_align(unaligned_fc_buf, UNIV_PAGE_SIZE);
	file_node_buf = ut_align(unaligned_file_node_buf, UNIV_PAGE_SIZE);

	if(pread(fc_bck_fd, fc_buf, KILO_BYTE, (off_t)0) != KILO_BYTE) {
		fprintf(stderr, "Error: read error when get metadata from flash cache backuped file:%s", 
			fc_bck_path);
		perror(NULL);
		fprintf(stderr, "Restore from flash cache backuped file failed, try again later\n");
		return 1;
	}

	memcpy(bkp_info, fc_buf, sizeof(fc_bkp_info_t));
	
	fc_bck_file_size = bkp_info->bkp_blk_count;
	blkmeta_offset = bkp_info->bkp_blk_meta_pos * KILO_BYTE;
	
	fprintf(stderr, "the blk count:%d, blk_meta_pos:%d\n", fc_bck_file_size, blkmeta_offset);
	
	blkmeta_size = fc_bck_file_size * sizeof(*blkmeta_array);
	blkmeta_array = (fc_bkp_blkmeta_t *)malloc(blkmeta_size);

	if(pread(fc_bck_fd, (void *)blkmeta_array, blkmeta_size, (off_t)blkmeta_offset) != blkmeta_size) {
		fprintf(stderr, "Error: read error when get block metadata from flash cache backuped file:%s", 
			fc_bck_path);
		perror(NULL);
		fprintf(stderr, "Restore from flash cache backuped file failed, try again later\n");
		return 1;
	}
	
	if(!init(data_dir)) {
		fprintf(stderr, "Restore from flash cache backuped file failed, try again later\n");
		return 1;
	}
	
	ut_print_timestamp(stdout);
	printf(" Begin restore flash cache backup file %s to MySQL data backuped files in directory %s\n", fc_bck_path, data_dir);
	printf("%u pages should be restored\n", fc_bck_file_size);

	bck_page_offset = KILO_BYTE;
	for(i = 0; i < fc_bck_file_size; ++i) {
		blk_size = blkmeta_array[i].blk_size * KILO_BYTE;
		fprintf(stderr, "the offset:%d, blksize:%d\n", i, blk_size);
		if(pread(fc_bck_fd, fc_buf, blk_size, (off_t)(bck_page_offset)) != blk_size) {
			fprintf(stderr, "Error: read error occured from flash cache backuped file:"
				"%s at offset %u(in page): ", fc_bck_path, i);
			perror(NULL);
			deinit();
			fprintf(stderr, "Restore from flash cache backuped file failed, try again later\n");
			return 1;
		}

		space_id = get_space_id(fc_buf);
		page_no  = get_page_no(fc_buf);

		assert((space_id == blkmeta_array[i].blk_space) 
			&& (page_no == blkmeta_array[i].blk_offset)); 
		
		lsn_in_fc = get_page_lsn(fc_buf);
		
		bck_page_offset += blk_size;	
		
		if(!read_page_from_tablespace(space_id, page_no, file_node_buf)) {
			fprintf(stderr, "error when read page(space_id:%lu,page_no:%lu) from tablespace, "
				"just skip it\n", space_id, page_no);
			continue;
		}
		assert((space_id == get_space_id(file_node_buf) && page_no == get_page_no(file_node_buf)) ||
				 !get_page_type(file_node_buf));   /* Caution: if the page is not allocated in disk(page type is 0), but exists in flash cache: indicates this
				 									     page need to be writen to disk from flash cache */
		
		lsn_in_disk = get_page_lsn(file_node_buf);
		if(lsn_in_fc > lsn_in_disk) {
			if(!write_page_to_tablespace(space_id, page_no, fc_buf)) {
				deinit();
				close(fc_bck_fd);
				fprintf(stderr, "Restore from flash cache backuped file failed, try again later\n");
				return 1;
			}
			++n_restored_from_fc;
		}
	}

	free(bkp_info);
	free(blkmeta_array);
	deinit();
	close(fc_bck_fd);
	ut_print_timestamp(stdout);
	printf(" Restore finished! %u pages actually restored from flash cache file %s\n", n_restored_from_fc, fc_bck_path);
	return 0;
}
