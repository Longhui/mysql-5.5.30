#!/bin/sh

prefix=/styx/home/hzjianghongxiang/jianghx/bazzar/trunk/build/jemalloc
exec_prefix=/styx/home/hzjianghongxiang/jianghx/bazzar/trunk/build/jemalloc
libdir=${exec_prefix}/lib

LD_PRELOAD=${libdir}/libjemalloc.so.1
export LD_PRELOAD
exec "$@"
