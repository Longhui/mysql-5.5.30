#/usr/bin/env bash
makejobs=1
cc=gcc47
cxx=g++47
while [ $# -gt 0 ] ; do
    arg=$1; shift
    if [[ $arg =~ --(.*)=(.*) ]] ; then
        eval ${BASH_REMATCH[1]}=${BASH_REMATCH[2]}
    else
        break
    fi
done
md5sum --check mysql-5.5.30-tokudb-7.0.1.tar.gz.md5
if [ $? != 0 ] ; then exit 1; fi
md5sum --check tokufractaltreeindex-7.0.1-linux-x86_64.tar.gz.md5
if [ $? != 0 ] ; then exit 1; fi
if [ ! -d mysql-5.5.30-tokudb-7.0.1 ] ; then
    tar xzf mysql-5.5.30-tokudb-7.0.1.tar.gz
    if [ $? != 0 ] ; then exit 1; fi
fi
if [ ! -d tokufractaltreeindex-7.0.1-linux-x86_64 ] ;then
    tar xzf tokufractaltreeindex-7.0.1-linux-x86_64.tar.gz
    if [ $? != 0 ] ; then exit 1; fi
fi
export TOKUDB_VERSION=7.0.1
export TOKUFRACTALTREE=$PWD/tokufractaltreeindex-7.0.1-linux-x86_64
export TOKUDB_PATCHES=1
export TOKUFRACTALTREE_LIBNAME=tokufractaltreeindex-7.0.1_static
export TOKUPORTABILITY_LIBNAME=tokuportability-7.0.1_static
export CC=$cc; export CXX=$cxx
pushd mysql-5.5.30-tokudb-7.0.1
if [ $? != 0 ] ; then exit 1; fi
mkdir build.RelWithDebInfo; pushd build.RelWithDebInfo
if [ $? != 0 ] ; then exit 1; fi
cmake .. -DBUILD_CONFIG=mysql_release -DCMAKE_BUILD_TYPE=RelWithDebInfo -DTOKU_JEMALLOC_LIBRARY="-Wl,--whole-archive $TOKUFRACTALTREE/lib/libjemalloc.a -Wl,-no-whole-archive"
if [ $? != 0 ] ; then exit 1; fi
make package -j$makejobs
if [ $? != 0 ] ; then exit 1; fi
if [ ! -f mysql-5.5.30-tokudb-7.0.1-linux-x86_64.tar.gz ] ; then
    oldtb=$(ls *.gz)
    if [[ $oldtb =~ (.*).tar.gz ]] ; then oldmysqldir=${BASH_REMATCH[1]}; fi
    tar xzf $oldtb
    if [ $? != 0 ] ; then exit 1; fi
    mv $oldmysqldir mysql-5.5.30-tokudb-7.0.1-linux-x86_64
    tar czf mysql-5.5.30-tokudb-7.0.1-linux-x86_64.tar.gz mysql-5.5.30-tokudb-7.0.1-linux-x86_64
    if [ $? != 0 ] ; then exit 1; fi
fi
tar xzf mysql-5.5.30-tokudb-7.0.1-linux-x86_64.tar.gz
if [ $? != 0 ] ; then exit 1; fi
cp ../../tokufractaltreeindex-7.0.1-linux-x86_64/bin/ftdump mysql-5.5.30-tokudb-7.0.1-linux-x86_64/bin/tokuftdump
if [ $? != 0 ] ; then exit 1; fi
rm mysql-5.5.30-tokudb-7.0.1-linux-x86_64.tar.gz
if [ $? != 0 ] ; then exit 1; fi
tar czf mysql-5.5.30-tokudb-7.0.1-linux-x86_64.tar.gz mysql-5.5.30-tokudb-7.0.1-linux-x86_64
if [ $? != 0 ] ; then exit 1; fi
if [ -f mysql-5.5.30-tokudb-7.0.1-linux-x86_64.tar.gz ] ; then
    md5sum mysql-5.5.30-tokudb-7.0.1-linux-x86_64.tar.gz >mysql-5.5.30-tokudb-7.0.1-linux-x86_64.tar.gz.md5
    if [ $? != 0 ] ; then exit 1; fi
fi
popd
mkdir build.RelWithDebInfo.rpms; pushd build.RelWithDebInfo.rpms
if [ $? != 0 ] ; then exit 1; fi
mkdir -p {tmp,BUILD,RPMS,SOURCES,SPECS,SRPMS}
if [[ mysql-5.5.30-tokudb-7.0.1 =~ (.*)-([0-9]+\.[0-9]+\.[0-9]+.*)-tokudb-(.*) ]] ; then
    mysql_distro=${BASH_REMATCH[1]}
    mysql_version=${BASH_REMATCH[2]}
    mysql_distro_version=${mysql_distro}-${mysql_version}
    tokudb_distro=tokudb
    tokudb_version=${BASH_REMATCH[3]}
    tokudb_distro_version=${tokudb_distro}-${tokudb_version}
else
    exit 1
fi
cp ../../mysql-5.5.30-tokudb-7.0.1.tar.gz SOURCES/mysql-5.5.30-tokudb-7.0.1.tar.gz
if [ $? != 0 ] ; then exit 1; fi
specfile=${mysql_distro}.${mysql_version}.spec
cp ../support-files/$specfile SPECS/$specfile
if [ $? != 0 ] ; then exit 1; fi
sed -i -e"s/^%define mysql_version.*/&-${tokudb_distro_version}/" SPECS/$specfile
sed -i -e"s/^%define release.*/%define release $(echo ${tokudb_distro_version}|tr - .)/" SPECS/$specfile
sed -i -e"s/README/& %{src_dir}\/README-TOKUDB/" SPECS/$specfile
sed -i -e"s/^\(.*-DMYSQL_SERVER_SUFFIX=.*\)$/& -DTOKU_JEMALLOC_LIBRARY=\"-Wl,--whole-archive \$\{TOKUFRACTALTREE\}\/lib\/libjemalloc.a -Wl,-no-whole-archive\"/" SPECS/$specfile
sed -i -e"s/%{_datadir}\/mysql\/$/&\n%attr(755, root, root) %{_libdir}\/libHotBackup*.so/" SPECS/$specfile
export MYSQL_BUILD_MAKE_JFLAG=-j$makejobs
rpmbuild -v --define="_topdir $PWD" --define="_tmppath $PWD/tmp" --define="src_base $mysql_distro" -ba SPECS/$specfile
if [ $? != 0 ] ; then exit 1; fi
pushd RPMS/$(uname -m)
if [ $? != 0 ] ; then exit 1; fi
mv *.rpm ../..
if [ $? != 0 ] ; then exit 1; fi
popd
pushd SRPMS
if [ $? != 0 ] ; then exit 1; fi
mv *.rpm ..
if [ $? != 0 ] ; then exit 1; fi
popd
rpmfiles=$(ls *.rpm)
for x in $rpmfiles; do
    md5sum $x >$x.md5
    if [ $? != 0 ] ; then exit 1; fi
done
popd
popd
