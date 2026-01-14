#!/bin/zsh
set -eu

# TEST TOPIC FIFO
# Make the Diaspora CTL FIFO

if (( ${#*} != 1 )) {
  print "diaspora-fifo: Provide ROOT_PATH"
  return 1
}

ROOT_PATH=$1

diaspora-ctl fifo --driver files                     \
                  --driver.root_path $ROOT_PATH      \
                  --control-file     $ROOT_PATH.fifo &
PID=${$}

print "diaspora-fifo: FIFO is PID $PID"

wait
