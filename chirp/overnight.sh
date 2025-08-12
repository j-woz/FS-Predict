#!/bin/zsh
set -eu

# OVERNIGHT SH

THIS=${0:A:h}
source $THIS/tools.sh

# DELAY: Max delay in minutes
args -v DIR DELAY LIMIT FILE_SRC LOOPS - ${*}

check-existence -p overnight.sh -v $FILE_SRC || return 1

zmodload zsh/mathfunc

make_log -s $DIR workflow
print "LOG_FILE:" $LOG_FILE

log "OVERNIGHT: START"
report "OVERNIGHT" DIR DELAY LIMIT FILE_SRC LOOPS >> $LOG_FILE
log ""

float -F 3 R
for (( COUNT=1 ; COUNT <= LIMIT ; COUNT++ ))
do
  log "RUN: START $COUNT/$LIMIT"
  $THIS/workflow.sh $FILE_SRC $LOOPS >>& $LOG_FILE
  log "RUN: STOP  $COUNT/$LIMIT"
  R=$(( rand48() * DELAY * 60.0 ))
  log "RUN: SLEEP $R"
  sleep $R
  log ""
done

log "OVERNIGHT: STOP"
