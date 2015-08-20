#!/bin/bash
LIBS=./libcouchbase/build/lib
rm -rf ./hello
gcc -I./libcouchbase/build/generated -I./libcouchbase/include -I./libcouchbase/include/libcouchbase -L$LIBS  hello.c -lcouchbase -o hello
if [ -r ./hello ]
then
    ls -l ./hello
    export LD_LIBRARY_PATH=./libcouchbase/build/lib
    ./hello
else
    echo "There is no hello"
fi
