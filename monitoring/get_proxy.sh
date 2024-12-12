#!/bin/bash

if [ $# -ne 3 ]; then
    echo "Usage: $0 <dh2020pcXX> <port> <monitorport>"
    exit 1
fi
CWD=`pwd`

ssh $1 "source ~/.bashrc && cd \"$CWD\"/../proxyServer && ./runit.bash $2 $3"
exit 0;
