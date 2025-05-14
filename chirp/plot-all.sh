#!/bin/zsh
set -eu

# PLOT ALL
# Plot all *.txt logs in $PWD

THIS=${0:A:h}
source $THIS/tools.sh

renice --priority 19 ${$}

LOGS=( *.txt )
print "LOGS:" ${#LOGS}
print

PWD=$THIS:$PWD

foreach LOG ( $LOGS )
  FILENAME=${LOG%.txt}
  report plot-all FILENAME
  @ extract.py $LOG README,zoom.deb $FILENAME.csv
  @ get-rate.sh $FILENAME.csv
  print
end

@ plot-rate.sh ${LOGS/txt/data}
