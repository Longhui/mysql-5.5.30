#!/bin/bash

OUTPUT="unitperf.txt"

function doTest() {
	echo $1 >> $OUTPUT
	total=0
	for suite in 1 2 3 4 5 6 7
	do 
		echo -ne "$suite\t" >> $OUTPUT
		before=`date +%s`
		$2 $suite ||  echo -ne "failed\t" >> $OUTPUT
		after=`date +%s`
		interval=$(($after-$before))
		total=$(($total + $interval))
		echo $interval >> $OUTPUT
	done
	echo Total:$total >> $OUTPUT
}

if [[ $# == 1 && $1 == "build" ]]; then
	./buildsetup -d
	./buildsetup
	./buildsetup -db
	./buildsetup -b
	./buildsetup -dm
fi

> $OUTPUT

doTest Debug[UT] ./ntsetest_dbg
doTest Release[UT] ./ntsetest
doTest Debug[UT,BP] ./ntsetest_bp_dbg
doTest Release[UT,BP] ./ntsetest_bp
doTest Debug[UT,MC] "valgrind --suppressions=../../mysql-test/valgrind.supp --log-file=err --leak-check=full ./ntsetest_dbg_mem"



