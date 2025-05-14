
# GET RATE AWK
# Convert seconds to milliseconds for plotting
# Convert B       to MB           for plotting

BEGIN {
  FS = ","
}

{
  timestamp = $1 * 1000
  bytes     = $6
  if (bytes > 0) {
    duration = $7
    rate     = bytes / duration / 1024.0 / 1024.0
    printf("%0.3f %18.6f\n", timestamp, rate)
  }
}
