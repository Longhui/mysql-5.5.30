cc=gcc
cxx=g++
build_type=RelWithDebInfo
build_dir=build
package=0
jemalloc_src=$PWD/jemalloc
ft_index_src=$PWD/ft-index
mysql_src=$PWD/mysql-5.5.30

while [ $# -gt 0 ]
do
  arg=$1
  shift
  if [[ $arg =~ --(.*)=(.*) ]]
  then
    eval ${BASH_REMATCH[1]}=${BASH_REMATCH[2]}
  else
    break
  fi
done

if [ ! -d $build_dir ]
then
  mkdir -p $build_dir
fi

pushd $build_dir
build_dir=$PWD
jemalloc_prefix=$PWD/jemalloc
fractal_tree_prefix=$PWD/ft-index
mysql_prefix=$PWD/mysql
popd

function get_ncpus()
{
  if [ -f /proc/cpuinfo ]
  then
    grep bogomips /proc/cpuinfo | wc -l
  else
    echo 1
  fi
}

makejobs=$(get_ncpus)

function build_jemalloc()
{
  if [ -d $jemalloc_src ]
  then
    pushd $jemalloc_src
    CC=$cc
    if [ ! -x configure ]
    then
      chmod u+x configure
    fi
    if [ ! -x include/jemalloc/internal/size_classes.sh ]
    then
      chmod u+x include/jemalloc/internal/size_classes.sh
    fi
    ./configure --with-private-namespace=jemalloc_ --prefix=$jemalloc_prefix
    make install -j$makejobs
    popd
  else
    echo "jemalloc not exist"
    exit 1
  fi
}

function build_fractal_tree()
{
  if [ -d $ft_index_src ] 
  then
    pushd $ft_index_src
    if [ ! -x third_party/xz-4.999.9beta/configure ]
    then
      chmod u+x third_party/xz-4.999.9beta/configure 
    fi
    mkdir -p bld
    pushd bld
    CC=$cc CXX=$cxx
      cmake \
      -D CMAKE_BUILD_TYPE=$build_type \
      -D JEMALLOC_SOURCE_DIR=../../jemalloc \
      -D CMAKE_INSTALL_PREFIX=$fractal_tree_prefix \
      -D BUILD_TESTING=OFF \
      -D USE_GTAGS=OFF \
      -D USE_CTAGS=OFF \
      -D USE_ETAGS=OFF \
      -D USE_CSCOPE=OFF \
      ..
    make install -j$makejobs
    popd
    popd
  else
    echo "source of ft-index not exist"
    exit 1
  fi
}

function build_mysql_server()
{
  local jemalloc_lib=$jemalloc_prefix/lib
  local fractal_tree_lib=$fractal_tree_prefix/lib
  export TOKUFRACTALTREE=$fractal_tree_prefix
  export TOKUFRACTALTREE_LIBNAME=tokudb_static
  export TOKUPORTABILITY_LIBNAME=tokuportability_static
  export CC=$cc
  export CXX=$cxx
  if [ -d $mysql_src ]
  then
    pushd $mysql_src
    mkdir -p bld
    pushd bld
    if [ $package -eq 1 ]
    then
      cmake .. -DBUILD_CONFIG=mysql_release -DCMAKE_BUILD_TYPE=$build_type -DWITH_EMBEDDED_SERVER=0 -DTOKU_JEMALLOC_LIBRARY="-Wl,--whole-archive $jemalloc_lib/libjemalloc.a -Wl,-no-whole-archive"
    else
      cmake .. -DCMAKE_INSTALL_PREFIX=$mysql_prefix -DBUILD_CONFIG=mysql_release -DCMAKE_BUILD_TYPE=$build_type -DWITH_EMBEDDED_SERVER=0 -DTOKU_JEMALLOC_LIBRARY="-Wl,--whole-archive $jemalloc_lib/libjemalloc.a -Wl,-no-whole-archive"
    fi
    if [ $? != 0 ]
    then
      exit 1
    fi
    if [ $package -eq 1 ]
    then
      make package -j$makejobs
    else
      make install -j$makejobs
    fi
    popd
    popd
  else
    echo "source of mysql not exist, exit"
    exit 1
  fi
}

function make_release_package()
{
  if [ $package -eq 1 ]
  then
    pushd $mysql_src/bld
    cp -f *.tar.gz $build_dir
    popd
    pushd $build_dir
    tarball=$(ls *.tar.gz)
    if [[ $tarball =~ (.*).tar.gz ]]
    then
      ball_dir=${BASH_REMATCH[1]}
      if [ -d $ball_dir ] 
      then
        rm -rf $ball_dir
      else
        echo "unzip the package and copy the ftdump"
        tar -zxf $tarball
        cp $fractal_tree_prefix/bin/ftdump $ball_dir/bin/tokuftdump
        rm -rf $tarball
        echo "create the package..."
        find $ball_dir -type d -name ".svn"|xargs rm -rf
        tar czf $tarball $ball_dir
        rm -rf $ball_dir
        echo "calcuate the md5..."
        md5sum $tarball > $tarball.md5
      fi
    fi
    popd
  else
    cp $fractal_tree_prefix/bin/ftdump $mysql_prefix/bin/tokuftdump
  fi
}

build_jemalloc
build_fractal_tree
build_mysql_server
make_release_package
