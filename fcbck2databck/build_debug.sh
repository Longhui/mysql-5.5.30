#! /bin/sh
gcc -g -o fcbck2databck_debug -D_FILE_OFFSET_BITS=64 -I ./include  ./src/main.c ./src/file.c ./src/common.c 