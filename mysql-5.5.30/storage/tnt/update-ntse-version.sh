#!/bin/bash
# Update build revision of NTSE
maxver=0
for ver in `svn info --xml -R | grep "revision=" | cut -d \" -f 2`; do
	if [[ $ver -gt $maxver ]]; then
		maxver=$ver
	fi
done
echo "maxver: ${maxver}"
cp src/ntse_version.h ntse_version.tmp.h
sed -e "/NTSE_REVISION/ c\#define NTSE_REVISION\t${maxver}" ntse_version.tmp.h > src/ntse_version.h

