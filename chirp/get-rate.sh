#!/bin/zsh
set -eu

# GET RATE SH
# Translate timestamps+sizes to rates for plotting

THIS=${0:A:h}
source $THIS/tools.sh

if (( ${#*} < 1 )) {
  print "Provide LOGS..."
  return 1
}
LOGS=( ${*} )

foreach LOG ( $LOGS )
  DATA=${LOG%.csv}.data
  print "rate:" $DATA
  awk -f $THIS/get-rate.awk < $LOG > $DATA
end
