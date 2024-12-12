#!/bin/bash
if [ $# -ne 4 ]; then
    echo "Usage: $0 <proxy dh2020pcXX> <proxy port> <proxy monitor port> <monitor port>"
    exit 1
fi
dh2010pc49 5001 5002 3001 dh2010pc48 4001 dh2010pc46 4001
