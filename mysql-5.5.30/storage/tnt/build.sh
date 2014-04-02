#!/bin/bash
#############################################################################
# build ntse into dynamically                                               #
#############################################################################

short_usage() {
	echo "Usage ./build.sh [MYSQL_SOURCE_DIR] MYSQL_INSTALL_DIR"
	echo "Try './build.sh --help' for more information."
}

usage() {
	echo "Usage:"
	echo "  ./build [MYSQL_SOURCE_DIR] MYSQL_INSTALL_DIR"
	echo "Arguments"
	echo "  MYSQL_SOURCE_DIR	The directory of MySQL's source code, default ../.."
	echo "  MYSQL_INSTALL_DIR 	The direcotry of MySQL's installation"
}

[[ $# > 0 && "$1" == "--help" ]] && { usage; exit 1; }
[[ $# < 1 || $# > 2 ]] && { short_usage; exit 1; }

declare source_dir
declare install_dir

if [[ $# == 1 ]]; then
	source_dir="../.."
	install_dir=$1
else
	source_dir=$1
	install_dir=$2
fi

make distclean
aclocal
autoconf
automake -a -f
./configure --with-debug=full --with-mysql=$source_dir --libdir=$install_dir/lib/mysql
make
