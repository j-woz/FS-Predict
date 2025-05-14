#!/bin/zsh
set -eu

# ALIVE SH
# Keep SSH connection alive

DELAY=60

DATE_FMT="%D{%s.%06.} %D{%Y-%m-%d} %D{%H:%M}"
while true
do
  print ${(%)DATE_FMT} "ALIVE"
  read -t $DELAY || true
done
