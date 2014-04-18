# script to run all the tnt and tnt_binlog test cases 
# author bblei
# param $1 [--big-test]

day=`date +%Y-%m-%d`
time=`date +%H:%M:%S`
mkdir log/

echo "main mysql-test/t(r)......................"
./mysql-test-run.pl --suite=main --force --big-test --max-test-fail=80  --testcase-timeout=360  --retry=1 $1 | tee log/${day}_${time}.log

echo "binlog mysql-test/suite/binlog/t(r)......................"
./mysql-test-run.pl --suite=binlog  --force --big-test --max-test-fail=80  --testcase-timeout=360  --retry=1 $1 | tee -a  log/${day}_${time}.log

echo "funcs_1 mysql-test/suite/funcs_1/t(r)......................"
./mysql-test-run.pl --suite=funcs_1    --force --big-test --max-test-fail=80  --testcase-timeout=360  --retry=1 $1 | tee -a  log/${day}_${time}.log

echo "funcs_2 mysql-test/suite/funcs_2/t(r)......................"
./mysql-test-run.pl --suite=funcs_2    --force --big-test --max-test-fail=80  --testcase-timeout=360  --retry=1 $1 | tee -a  log/${day}_${time}.log

echo "innodb mysql-test/suite/innodb/t(r)......................"
./mysql-test-run.pl --suite=innodb    --force --big-test --max-test-fail=80  --testcase-timeout=360  --retry=1 $1 | tee -a log/${day}_${time}.log

echo "jp mysql-test/suite/jp/t(r)......................"
./mysql-test-run.pl --suite=jp    --force --big-test --max-test-fail=80  --testcase-timeout=360  --retry=1 $1 | tee -a  log/${day}_${time}.log

echo "large_tests mysql-test/suite/large_tests /t(r)......................"
./mysql-test-run.pl --suite=large_tests    --force --big-test --max-test-fail=80  --testcase-timeout=360  --retry=1 $1 | tee -a  log/${day}_${time}.log

echo "manual mysql-test/suite/manual/t(r)......................"
./mysql-test-run.pl --suite=manual    --force --big-test --max-test-fail=80  --testcase-timeout=360  --retry=1 $1 | tee -a log/${day}_${time}.log

echo "parts mysql-test/suite/parts/t(r)......................"
./mysql-test-run.pl --suite=parts    --force --big-test --max-test-fail=80  --testcase-timeout=360  --retry=1 $1 | tee -a log/${day}_${time}.log

echo "parts mysql-test/suite/innosql/t(r)......................"
./mysql-test-run.pl --suite=innosql    --force --big-test --max-test-fail=80  --testcase-timeout=360  --retry=1 $1 | tee -a log/${day}_${time}.log

echo "parts mysql-test/suite/federated/t(r)......................"
./mysql-test-run.pl --suite=federated    --force --big-test --max-test-fail=80  --testcase-timeout=360  --retry=1 $1 | tee -a log/${day}_${time}.log

echo "parts mysql-test/suite/engines/t(r)......................"
./mysql-test-run.pl --suite=engines    --force --big-test --max-test-fail=80  --testcase-timeout=360  --retry=1 $1 | tee -a log/${day}_${time}.log

echo "perfschema mysql-test/suite/perfschema/t(r)......................"
./mysql-test-run.pl --suite=perfschema    --force --big-test --max-test-fail=80  --testcase-timeout=360  --retry=1 $1 | tee -a  log/${day}_${time}.log

echo "perfschema_stress mysql-test/suite/perfschema_stress/t(r)......................"
./mysql-test-run.pl --suite=perfschema_stress    --force --big-test --max-test-fail=80  --testcase-timeout=360  --retry=1 $1 | tee -a log/${day}_${time}.log

echo "rpl mysql-test/suite/rpl/t(r)......................"
./mysql-test-run.pl --suite=rpl    --force --big-test --max-test-fail=80  --testcase-timeout=360  --retry=1 $1 | tee -a  log/${day}_${time}.log

echo "stress mysql-test/suite/stress/t(r)......................"
./mysql-test-run.pl --suite=stress    --force --big-test --max-test-fail=80  --testcase-timeout=360  --retry=1 $1 | tee -a  log/${day}_${time}.log

echo "sys_vars mysql-test/suite/sys_vars/t(r)......................"
./mysql-test-run.pl --suite=sys_vars    --force --big-test --max-test-fail=80  --testcase-timeout=360  --retry=1 $1 | tee -a  log/${day}_${time}.log

echo "tokudb mysql-test/suite/tokudb/t(r)......................"
./mysql-test-run.pl --suite=tokudb    --force --big-test --max-test-fail=80  --testcase-timeout=360  --retry=1 $1 | tee -a  log/${day}_${time}.log

echo "tokudb.add_index mysql-test/suite/tokudb.add_index/t(r)......................"
./mysql-test-run.pl --suite=tokudb.add_index    --force --big-test --max-test-fail=80  --testcase-timeout=360  --retry=1 $1 | tee -a  log/${day}_${time}.log

echo "tokudb.alter_table mysql-test/suite/tokudb.alter_table/t(r)......................"
./mysql-test-run.pl --suite=tokudb.alter_table    --force --big-test --max-test-fail=80  --testcase-timeout=360  --retry=1 $1 | tee -a  log/${day}_${time}.log

echo "tokudb.bugs mysql-test/suite/tokudb.bugs/t(r)......................"
./mysql-test-run.pl --suite=tokudb.bugs    --force --big-test --max-test-fail=80  --testcase-timeout=360  --retry=1 $1 | tee -a  log/${day}_${time}.log


longBit=`getconf LONG_BIT`
if [ "$longBit" = "64" ]; then
	echo "tnt......................"
	./mysql-test-run.pl --suite=tnt/tnt --force --max-test-fail=20 --nowarnings --testcase-timeout=60 --mysqld=--default-storage-engine=tnt $1 | tee -a log/${day}_${time}.log
	./mysql-test-run.pl --suite=tnt/jp --force --max-test-fail=20 --nowarnings --testcase-timeout=60 --mysqld=--default-storage-engine=tnt $1 | tee -a log/${day}_${time}.log
fi

if [ "$longBit" = "32" ]; then
	echo "tnt......................"
	./mysql-test-run.pl --suite=tnt/tnt --force --max-test-fail=20 --nowarnings --testcase-timeout=60 --mysqld=--default-storage-engine=tnt --skip-test=tnt_max_sessions_ $1 | tee -a log/${day}_${time}.log
	./mysql-test-run.pl --suite=tnt/jp --force --max-test-fail=20 --nowarnings --testcase-timeout=60 --mysqld=--default-storage-engine=tnt --skip-test=tnt_max_sessions_ $1 | tee -a log/${day}_${time}.log
fi

echo "tnt binlog and rpl................................."
./mysql-test-run.pl --suite=tnt/binlog --force --max-test-fail=20 --nowarnings --testcase-timeout=60 --mysqld=--binlog_format=row --mysqld=--default-storage-engine=tnt $1 | tee -a log/${day}_${time}.log
	
./mysql-test-run.pl --suite=tnt/binlog --force --max-test-fail=20 --nowarnings --testcase-timeout=60 --mysqld=--binlog_format=mixed --mysqld=--default-storage-engine=tnt $1 | tee -a log/${day}_${time}.log
	
./mysql-test-run.pl --suite=tnt/rpl --force --max-test-fail=20 --nowarnings --testcase-timeout=60 --mysqld=--binlog_format=row --mysqld=--default-storage-engine=tnt $1 | tee -a log/${day}_${time}.log
	
./mysql-test-run.pl --suite=tnt/rpl --force --max-test-fail=20 --nowarnings --testcase-timeout=60 --mysqld=--binlog_format=mixed --mysqld=--default-storage-engine=tnt $1 | tee -a log/${day}_${time}.log


echo "all tests cases done!........................."

exit 0
