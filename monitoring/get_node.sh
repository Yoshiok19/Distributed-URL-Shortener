#!/bin/bash

if [ $# -ne 2 ]; then
    echo "Usage: $0 <dh2020pcXX> <port>"
    exit 1
fi
CWD=`pwd`

ssh $1 "source ~/.bashrc && cd \"$CWD\"/../serverSqlite && ./runit.bash $2 $1"
exit 0;
