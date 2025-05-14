#!/bin/zsh
set -eu

# WORKFLOW SH

THIS=${0:A:h}
source $THIS/tools.sh

# FILE_SRC: File to copy
# LOOPS:    Number of times to copy
args -v FILE_SRC LOOPS - ${*}

# SSH hosts
HOST2=compute-240-12
# THIS on HOST2
THIS2=/home/woz/proj/xfer_m/chirp

# Chirp servers
CS1=localhost:9094 # local host  (lambda)
CS2=localhost:9095 # middle host (compute-240-12.cels.anl.gov)
CS3=localhost:9096 # final host  (polaris.alcf.anl.gov)

FILE=${FILE_SRC:t}

# These must be in PATH for remote execution
COPY=copy.sh
COPY3=copy3.sh

if [[ ! -f $FILE_SRC ]] error "file not found: $FILE_SRC"

msg "workflow.sh: init: loops=$LOOPS"
$COPY $CS1 $FILE_SRC $FILE

for (( i=1 ; i <= LOOPS ; i++ ))
do
  msg "workflow.sh: start: $i/$LOOPS"
  CMD=( $COPY3 $CS1 $CS2 $FILE )
  $CMD

  CMD=( $THIS2/$COPY3 $CS2 $CS3 $FILE )
  ssh $HOST2 $CMD
  msg "workflow.sh: stop:  $i/$LOOPS"
done

msg "workflow.sh: completed $LOOPS loops."
