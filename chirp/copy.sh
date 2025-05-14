#!/bin/zsh
set -eu

# COPY SH

THIS=${0:A:h}
source $THIS/tools.sh

args HOST FILE1 FILE2:_UNSET_ - ${*}

if [[ ${FILE2} == "_UNSET_" ]] FILE2=${FILE1:h}

@ chirp -a hostname $HOST put $FILE1 $FILE2
