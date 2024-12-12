#!/bin/bash

if [ $# -ne 2 ]; then
    echo "Usage: $0 <port> <monitorport>"
    exit 1
fi

/opt/jdk-22.0.1/bin/javac SimpleProxyServer.java
nohup /opt/jdk-22.0.1/bin/java SimpleProxyServer $1 $2> /dev/null 2>&1 &
echo "$!"
echo `ifconfig -a | grep "eno1" -A 1 | grep inet | awk '{print $2}'`

exit 0
