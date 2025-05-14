#!/bin/zsh
set -eu

# SLICE SH
# Extract a time slice of the logs

THIS=${0:A:h}
source $THIS/tools.sh

if (( ${#*} < 3 )) {
  print "Provide START STOP LOGS..."
  return 1
}
START=$1
STOP=$2
shift 2
LOGS=( ${*} )

foreach LOG ( $LOGS )
  printf "slice: %s " $LOG
  SLICE=${LOG%.csv}-$START-$STOP.csv
  print "\t-> $SLICE"
  awk -f $THIS/slice.awk -v START=$START STOP=$STOP < $LOG > $SLICE
end
