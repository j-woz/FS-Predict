#!/bin/bash
set -eu

if (( ${#} != 1 ))
then
  echo "Provide SOCK - the local unix socket"
  exit 1
fi

SOCK=$1

set -x

python client.py -s $SOCK insert  -i lambda.csv
python client.py -s $SOCK predict -i workload.csv -o predicted.csv
cat predicted.csv
