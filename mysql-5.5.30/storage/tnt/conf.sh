make distclean
aclocal
#autoheader
autoconf
automake -a -f
./configure
#./configure --with-mysql=/home/ylh/mysql-5.1.26-rc --libdir=/home/ylh/opt/mysql5.1.26/lib/mysql --with-debug=full
#make
