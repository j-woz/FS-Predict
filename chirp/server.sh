#!/bin/zsh
set -eu

# SERVER SH

THIS=${0:A:h}
source $THIS/tools.sh

args PORT DIR - ${*}

make_log $DIR "server"
log "server.sh: START"

# Run at low priority
renice --priority 10 ${$}

DARSHAN=~/Public/sfw/darshan.jw
LIBDARSHAN=$DARSHAN/lib/libdarshan.so
DARSHAN=(
  LD_PRELOAD=$LIBDARSHAN
  DARSHAN_ENABLE_NONMPI=1
  # DARSHAN_DUMP_CONFIG=1
  DXT_ENABLE_IO_TRACE=1
  DARSHAN_MODMEM=128
  DARSHAN_LOGPATH=$HOME/chirp-work/darshan
)

CHIRP=(
  -p $PORT
  -r $DIR
  -d local
  -d process
  -d login
)

# Run the server!
env $DARSHAN chirp_server $CHIRP -o $LOG_FILE &
PID=${!}
sleep 1
print PID=$PID
print

# Loop until the user wants to kill the server
# This allows us to add the final log lines after chirp_server exits
while true
do
  # Type 'x' to quit
  print  "LOG_FILE:" $LOG_FILE
  printf "$PID > "
  read P || true
  case $P {
    "") ;;
    "x") break ;;
    "p") ps -p $PID ;;
    *)   print "Use 'x' to exit or 'p' to print status." ;;
  }
  print
done

print "kill" $PID
kill $PID

sleep 2

log "server.sh: STOP"
