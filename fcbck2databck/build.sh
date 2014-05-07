#! /bin/sh
gcc -o fcbck2databck -D_FILE_OFFSET_BITS=64 -I ./include -O3 ./src/main.c ./src/file.c ./src/common.c 