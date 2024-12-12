#!/bin/bash

if [ $# -ne 2 ]; then
    echo "Usage: $0 <port> <hostname>"
    exit 1
fi

NAME=$(hostname)

mkdir /virtual/a1group09
rm /virtual/a1group09/$NAME.db

sqlite3 /virtual/a1group09/$NAME.db < schema.sql

/opt/jdk-22.0.1/bin/javac URLShortner.java
nohup /opt/jdk-22.0.1/bin/java -classpath ".:sqlite-jdbc-3.39.3.0.jar" URLShortner $1 $NAME> /dev/null 2>&1 &
echo "$!"
echo `ifconfig -a | grep "eno1" -A 1 | grep inet | awk '{print $2}'`

exit 0
