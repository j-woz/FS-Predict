
# TOOLS SH
# Sets up shell scripts
# Sources THIS/settings.sh if applicable
# Loads shell tool functions for errors, directories, logs, etc.

# Allow for local self-configuration:
if [[ ${THIS:-} != "" ]] && [[ -r $THIS/settings.sh ]] {
  source $THIS/settings.sh
}

@()
# Verbose command
{
  print ${*}
  ${*}
}

alias error='ERROR $0'

ERROR()
{
  local SELF=$1
  shift
  print ${SELF}: ${*}
  exit 1
}

here()
# Make given DIR into a relative path with respect to PWD
# Stores result in REPLY
{
  local DIR=${1:-${PWD}}
  DIR=${DIR:A}
  # WD is the absolute version of PWD
  # Include trailing slash
  local WD=${PWD:A}/
  # Cut WD from DIR:
  DIR=${DIR/${WD}}
  REPLY=${DIR}
}

# Expects 0 arguments:
alias args0='args - ${*}'
alias args='ARGS $0'

ARGS()
# File argument signature handling for shell scripts
# Example from within user script called script.sh:
# ARGS script.sh X Y Z - ${*}
# This assigns the arguments to script.sh to X, Y, and Z
# and check for argument counts, etc.
# Usage:
# -e : exit instead of return on errors
# -v : verbose output
# First argument: SELF
# Variable names (optionally with defaults X1:20 X2:30 ...)
# -
# Variable values (typically ${*})
{
  local E="" H="" VERBOSE="" L
  zparseopts -D -E e=E h=H v=VERBOSE

  L=() # Required argument name list

  if (( ${#*} == 1 )) {
    _args_help
    if (( ${#E} )) exit 1
    return 1
  }

  # Setup SELF
  here $1
  SELF=$REPLY
  if (( ${#VERBOSE} )) print "${SELF}: ARGS: START"
  shift

  # Help?
  if (( ${#H} )) {
    print "${SELF}: usage: ${L}"
    if (( ${#E} )) exit 0
    return
  }

  # Find variable names and dash
  while true
  do
    if (( ${#*} == 0 )) {
      print "$SELF: ARGS(): Could not find dash delimiter!"
      if (( ${#E} )) exit 1
      return 1
    }
    if [[ $1 == "-" ]] {
      shift
      break
    }
    # Found a variable name:
    L+=$1
    shift
  done

  local GIVEN=${#*}
  if (( ${#L} < ${GIVEN} )) {
    print "${SELF}: Requires ${#L} arguments, given ${#*}"
    if (( ${#L} > 0 )) print "${SELF}: Arguments: ${L}"
    if (( ${#E} )) exit 1
    return 1
  }
  local N=${#*} KV kv K V i
  # This does colon splitting:
  typeset -T KV kv
  # Handle required arguments provided with values:
  for (( i=1 ; i<=N ; i++ ))
  do
    KV=${L[i]}
    K=${kv[1]}
    eval "${K}='${1}'"
    if (( ${#VERBOSE} )) print "${SELF}: ARGS: ${K}='${1}'"
    shift
  done
  # Handle remaining required arguments: must have defaults!
  for (( ; i<=${#L} ; i++ ))
  do
    KV=${L[i]}
    K=${kv[1]}
    if (( ${#kv} == 2 )) {
      V=${kv[2]}
      eval ${K}=${V}
      if (( ${#VERBOSE} )) print "ARGS: ${K}='${V}' (default)"
    } else {
      print "${SELF}: Requires ${#L} arguments, given ${GIVEN}"
      print "${SELF}: Arguments: ${L}"
      print "${SELF}: Argument has no default: ${K}"
      if (( ${#E} )) exit 1
      return 1
    }
  done
  if (( ${#VERBOSE} )) print "${SELF}: ARGS: STOP"
}

_args_help()
{
  print "ARGS(): usage: "
  print "  -e : exit instead of return on errors"
  print "  -v : enable verbose output"
  print "       First argument: SELF"
  print "       Variable names (optionally with defaults X1:20 X2:30 ...)"
  print -- "  -"
  print '       Variable values (typically ${*})'
}

make_log()
# Sets global LOG_FILE
# Provide directory DIR and name token NAME
# Provide -s for seconds resolution
{
  local S SECS=""
  zparseopts -D -E s=S
  if (( ${#*} != 2 )) {
    print "Provide DIR NAME!"
    return 1
  }
  local DIR=$1
  local NAME=$2
  if (( ${#S} )) SECS=":%S"
  local LOG_FMT="$DIR/log/%D{%Y-%m-%d}/$NAME-%D{%H:%M${SECS}}.txt"
  LOG_FILE=${(%)LOG_FMT}
  # dirname of LOG_FILE
  mkdir -pv ${LOG_FILE:h}
  touch $LOG_FILE
}

# Accurate to microseconds, as in chirp_server:
DATE_FMT="%D{%s.%06.} %D{%Y-%m-%d} %D{%H:%M}"

log()
# Uses global LOG_FILE
{
  msg ${*} >> $LOG_FILE
}

msg()
# Basic timestamped message
{
  print "#" ${(%)DATE_FMT} ${*}
}

report()
# Print given variables after PREFIX
{
  if (( ${#*} < 1 )) {
    print "Provide PREFIX NAME..."
    return 1
  }
  local PREFIX NAME
  PREFIX=$1
  shift
  for NAME in ${*}
  do
    print "$PREFIX: $NAME=${(P)NAME}"
  done
}
