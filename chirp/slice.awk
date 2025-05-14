
# SLICE AWK
# Caller must provide START, STOP
# Print records with timestamp between START and STOP

BEGIN {
  FOUND = 0
}

$1 > START { FOUND=1  }
$1 > STOP  { exit     }
FOUND == 1 { print $0 }
