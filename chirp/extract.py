#!/usr/bin/env python

"""
EXTRACT PY

See extract.py --help
"""


def parse_args():
    import argparse

    parser = argparse.ArgumentParser(description="Extract log info")
    parser.add_argument("logfile",
                        help="The Chirp server log file")
    parser.add_argument("datafiles",
                        help="Comma-separated list of files we want")
    parser.add_argument("output",
                        help="Output CSV file")

    args = parser.parse_args()
    return args


def main():
    args = parse_args()
    try:
        datafiles = args.datafiles.split(",")
        extract(args.logfile, datafiles, args.output)
    except FileNotFoundError as e:
        abort(str(e))

def extract(logfile, datafiles, output):
    import sys
    report("input:  " + logfile)
    report("output: " + output)
    with open(logfile) as fp_in:
        if output == "-":
            fp_out = sys.stdout
        else:
            fp_out = open(output, "w")
        counts = extract_fps(fp_in, datafiles, fp_out)
    if output != "-":
        fp_out.close()
    report("servers: %i" % counts["servers"])
    report("files:   %i" % counts["files"])
    report("lines:   %i" % counts["lines"])


from enum import Enum, IntEnum, auto


class OpCode(IntEnum):
    READ  = 0
    WRITE = 1


class ServerState(Enum):
    """ Possible states the server can be in """
    # File is being opened
    OPEN0   = auto()
    # File has been opened
    OPEN1   = auto()
    # Server I/O (pread or pwrite) in progress
    IO      = auto()
    # Server other operation in progress
    OTHER   = auto()
    # File is being closed
    CLOSE0  = auto()
    # File has been closed
    CLOSE1  = auto()


class Server:
    """ A simulated server that is replayed in the log """

    def __init__(self, pid, fp_out):
        """ Create a new server process """
        # Start in OTHER because init() is in flight
        self.state  = ServerState.OTHER
        # Server PID: Same as index in servers dict
        self.pid    = pid
        self.op     = None
        self.start  = None
        self.fp_out = fp_out

    def begin(self, op, start):
        """ Begin a server operation """
        self.op    = op
        self.start = start
        # Record that an I/O operation is in flight:
        self.state = ServerState.IO

    def end(self, tokens):
        """ End a server I/O operation and write CSV line """
        assert self.state == ServerState.IO
        stop_s = tokens[0]
        # tokens[1] is the server PID
        stop_day, stop_h, stop_m = tokens[2:5]
        # tokens[6] is the log message type
        size = tokens[7]
        stop  = float(stop_s)
        duration = stop - self.start
        duration_s = "%0.6f" % duration

        record = [stop_s, stop_day, stop_h, stop_m,
                  str(self.op.value), size, duration_s]
        line = ",".join(record)
        # Write good data line!
        self.fp_out.write(line + "\n")
        # Record that no I/O operation is in flight:
        self.state = ServerState.OPEN1


# We make this a big flat loop for speed
# pylint: disable=too-many-branches,too-many-statements,too-many-locals
def extract_fps(fp_in, datafiles, fp_out):
    # The line we are on in the input log:
    line_count = 0
    # All servers seen, not just live ones:
    server_count = 0
    # Count of our files opened:
    file_count = 0
    servers = {}

    # Log format is:
    # timestamp server DY HR MN local: text...
    while True:
        try:
            line = fp_in.readline()
        except UnicodeDecodeError as e:
            report("UnicodeDecodeError!")
            print(str(e))
            print("line: %i" % line_count)
            exit(1)

        if len(line) == 0: break
        line_count += 1

        # Comment:
        if line.startswith("#"): continue

        if " init(" in line:
            tokens = line.split()
            pid = int(tokens[1])
            if (pid in servers and
                       servers[pid].state != ServerState.CLOSE1):
                abort("reusing PID: %i line: %i" % (pid, line_count))
            server = Server(pid, fp_out)
            servers[pid] = server
            server_count += 1
            continue

        if " open(" in line:
            found, tokens, pid = do_open(datafiles, line)
            if found:
                file_count += 1
                server = servers[pid]
                server.state = ServerState.OPEN0
            else:
                continue

        if "=> " in line:
            # Some operation finished.
            # Lookup server to see what happened.
            tokens = line.split()
            pid = int(tokens[1])
            server = servers[pid]
            if   server.state == ServerState.OPEN0:
                code = int(tokens[7])
                assert code == 0
                server.state = ServerState.OPEN1
            elif server.state in (ServerState.OTHER,
                                  ServerState.OPEN1,
                                  ServerState.CLOSE1):
                # Assume this is the result of some other operation
                pass
            elif server.state == ServerState.IO:
                server.end(tokens)
            elif server.state == ServerState.CLOSE0:
                assert len(tokens) >= 8, "line: %i" % line_count
                code = int(tokens[7])
                assert code == 0
                server.state = ServerState.CLOSE1
            else:
                print("unexpected result: line: %i: %s" %
                      (line_count, line))
                print("pid: %i state: %s" % (pid, str(server.state)))
                exit(1)
            continue

        # Look for I/O operations:
        op = None
        if   " pread("  in line:
            op = OpCode.READ
        elif " pwrite(" in line:
            op = OpCode.WRITE
        if op is not None:
            # Start an I/O operation:
            tokens = line.split()
            pid    = int(tokens[1])
            # Proceed only if we have one of our files open:
            if pid not in servers: continue
            server = servers[pid]
            if server.state != ServerState.OPEN1: continue
            start  = float(tokens[0])
            server = servers[pid]
            server.begin(op, start)
            continue
        if " close("  in line:
            tokens = line.split()
            pid    = int(tokens[1])
            server = servers[pid]
            server.state = ServerState.CLOSE0

    for server in servers.values():
        if server.state in (ServerState.OPEN0,
                            ServerState.CLOSE0,
                            ServerState.IO):
            print("warning: server is in flight: %i" % server.pid)

    return {"servers": server_count,
            "files":   file_count,
            "lines":   line_count}


def do_open(datafiles, line):
    """
    Do the file open.
    Returns found=True if this is one of our data files.
    Splits the line for convenience.
    @return found, tokens, pid
    """
    found = False
    # Make sure this open is on one of our data files:
    for datafile in datafiles:
        if datafile in line:
            found = True
            break
    if not found: return False, None, None  # Not one of our files
    tokens = line.split()
    pid    = int(tokens[1])
    return True, tokens, pid


def report(msg):
    print("extract.py: " + msg)


def abort(msg):
    report("abort: " + msg)
    exit(1)


if __name__ == "__main__":
    main()
