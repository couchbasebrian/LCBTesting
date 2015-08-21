#!/bin/bash
# August 21, 2015
#
clear
# Assumes a git pull for libcouchbase was done in the current directory
LIBS=./libcouchbase/build/lib
#
echo "---> Removing old executable file"
rm -rf ./lcbtesting
#
echo "---> Invoking gcc"
gcc -I./libcouchbase/build/generated -I./libcouchbase/include -I./libcouchbase/include/libcouchbase -L$LIBS  lcbtesting.c -lcouchbase -o lcbtesting
#
if [ -r ./lcbtesting ]
then
    echo "---> This is the newly built executable"
    ls -l ./lcbtesting
    export LD_LIBRARY_PATH=./libcouchbase/build/lib
    echo "---> Running the executable:"
    ./lcbtesting
    echo "---> Done running the executable."
else
    echo "---> Failed to build the executable"
fi
#
echo "---> Goodbye"
exit 0

