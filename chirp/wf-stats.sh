#!/bin/zsh

# WF-STATS SH
# Report the key lines from a workflow log

THIS=${0:A:h}
source $THIS/tools.sh

args -v LOG_FILE - ${*}

grep "debug:\|OVERNIGHT:\|RUN:" $LOG_FILE
