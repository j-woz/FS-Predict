#!/bin/zsh
set -eu

# COPY 3 SH

THIS=${0:A:h}
source $THIS/tools.sh

args HOST1 HOST2 FILE - ${*}

@ chirp -a hostname $HOST1 thirdput $FILE $HOST2 $FILE
