if [ $# -ne 2 ]; then
    echo "Usage: $0 <dh2020pcXX> <pid>"
    exit 1
fi

ssh $1 "kill -9 $2" &> /dev/null
exit 0;
