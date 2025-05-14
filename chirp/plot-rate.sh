#!/bin/zsh
set -eu

# PLOT RATE SH
# Provide -c to get a PDF

THIS=${0:A:h}
source $THIS/tools.sh

zparseopts -D -E c=CONVERT

if (( ${#*} < 1 )) {
  print "Provide DATA..."
  return 1
}
DATA=( ${*} )

DEFINES=()
i=0
foreach F ( $DATA )
  (( ++i ))
  # Cut off everything after the first dash:
  HOST=${F/-*/}
  # Substitute underscore for colon:
  F=${F/:/_}
  DEFINES+=( -D FILE$i=$F -D HOST$i=$HOST )
end

m4 $DEFINES $THIS/rate.cfg.m4 > rate.cfg

@ jwplot rate.eps rate.cfg $DATA
if (( ${#CONVERT} )) @ convert rate.{eps,pdf}
