#!/bin/zsh
set -eu

# SSH TUNNEL SH
# Wrapper around SSH tunnel with logging
# To be called by ./tunnel.sh

THIS=${0:A:h}
source $THIS/tools.sh

zparseopts -D -E R=R
args SPEC HOST LOG_FILE - ${*}

DATE_FMT="%D{%s.%06.} %D{%Y-%m-%d} %D{%H:%M}"
print ${(%)DATE_FMT} "SSH TUNNEL START" >> $LOG_FILE

# Port forward: Local or Remote
if (( ${#R} )) {
  LR="-R"  # Remote
} else {
  LR="-L"  # Local
}

if ssh -tt -o ControlMaster=auto $LR $SPEC $HOST    \
       zsh -l -c alive.sh                        >> \
       $LOG_FILE
then
  CODE=0
else
  CODE=${?}
fi

print ${(%)DATE_FMT} "SSH TUNNEL EXIT CODE: $CODE" >> $LOG_FILE

DATE_FMT="%D{%s.%06.} %D{%Y-%m-%d} %D{%H:%M}"
print ${(%)DATE_FMT} "SSH TUNNEL STOP " >> $LOG_FILE
