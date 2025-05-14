#!/bin/bash
set -eu
set -x

python client.py -s /tmp/woz/xfer.s insert  -i lambda.csv
python client.py -s /tmp/woz/xfer.s predict -i workload.csv -o predicted.csv
cat predicted.csv
