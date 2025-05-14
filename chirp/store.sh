#!/bin/zsh
set -eu

# STORE SH
# Store the latest log to Dunedin

THIS=${0:A:h}
source $THIS/tools.sh

args -v NAME TYPE DIR - ${*}

if [[ ! -d $DIR ]] error "DIR does not exist: $DIR"

cd $DIR
LOG=( $TYPE-*(om[1]) )
cd -

if (( ${#LOG} == 0 )) error "No log found!"

# Basename of DIR
DATE=${DIR:A:t}

print
report "store.sh" DATE LOG
print

renice --priority 19 ${$} > /dev/null

@ ssh dunedin mkdir -pv proj/xfer-m-data/log/$DATE
@ scp $DIR/$LOG dunedin:proj/xfer-m-data/log/$DATE/$NAME-$LOG
