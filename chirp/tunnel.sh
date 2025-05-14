#!/bin/zsh
set -eu

# TUNNEL SH
# User interface for SSH tunnel
# [-R]: remote forward
# HOST: remote host
# PORT: local/remote port
# LOGS: directory for logs

THIS=${0:A:h}
source $THIS/tools.sh

zparseopts -D -E R=R

args HOST PORT LOGS - ${*}

LOG_FMT="$LOGS/log/%D{%Y-%m-%d}/ssh-%D{%H:%M}.txt"
LOG_FILE=${(%)LOG_FMT}

# dirname of LOG_FILE
mkdir -pv ${LOG_FILE:h}

DATE_FMT="%D{%s.%06.} %D{%Y-%m-%d} %D{%H:%M}"

print
print ${(%)DATE_FMT} "START" $HOST | tee -a $LOG_FILE
print

SPEC=${PORT}:localhost:${PORT}

# The ssh may reuse an existing ControlMaster
# If so, we have no PID to kill, but we can cancel the tunnel
(
  set -x
  # Need -tt for TTY for read in remote script
  ./ssh-tunnel.sh ${R} $SPEC $HOST $LOG_FILE &
)
sleep 1

# grep errors are not errors:
TOKENS=( $( set +e
	    ps -AH -o pid,cmd    | \
	      grep -v grep       | \
	      grep -- "-L $SPEC"
	    true
	 ) )

if (( ${#TOKENS} )) {
  print "FOUND:" $TOKENS
  PID=${TOKENS[1]}
  print PID=$PID
} else {
  print "MASTER."
  PID="master"
}

P=""
while true
do
  # Type 'x' to quit
  printf "PID=$PID > "
  read P || true
  case $P {
    "x") break ;;
    *)   print "Press x to cancel." ;;
  }
done

if [[ $PID == "master" ]] {
  @ ssh -O cancel -L $SPEC $HOST
} else {
  @ kill $PID
}

print ${(%)DATE_FMT} "STOP" >> $LOG_FILE
