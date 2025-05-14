
"""
XFER SERVER
"""

import os, socket, sys, time
# from collections import deque

from utils import send, recv, recv_line

from predictor import Predictor


cancelled = False
# The socket
sock = None
# The filename of the socket
sockfile = None
predictor = None


def main():
    global sock, predictor
    args = parse_args()
    predictor = Predictor(args.model)
    if predictor.model is None: exit(1)
    sock = make_socket(args)
    if sock is None: exit(1)
    code = run_server(args)
    shutdown(args, code)


def msg(m):
    print("xfer-server: " + str(m))
    sys.stdout.flush()


def warn(m):
    msg("warning: " + str(m))


def abort(m):
    msg("abort: " + str(m))


def parse_args():
    import argparse
    parser = argparse.ArgumentParser(description="Run server")
    parser.add_argument("-m", "--model", default="rng",
                        help="The model to import by name")
    parser.add_argument("-s", "--socket",
                        help="The local socket")
    args = parser.parse_args()
    return args


def make_socket(args):
    import random
    global sockfile
    tmp = get_tmp()
    if tmp is None:
        return None
    index = random.randint(1, 1000)
    if args.socket is None:
        sockfile = tmp + "/xfer-sock." + str(index) + ".s"
    else:
        sockfile = args.socket
    if not make_sock_dir(sockfile): return None
    if not reset_sock(sockfile):    return None
    msg("opening socket: " + sockfile)
    global sock
    sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    try:
        sock.bind(sockfile)
    except Exception as e:
        abort("could not open socket at '%s': %s\n" %
              (sockfile, str(e)))
        sock = None
    return sock


def get_tmp():
    tmp = None
    if os.getenv("XFER_TMP") is not None:
        tmp = os.getenv("XFER_TMP")
    elif os.getenv("TMPDIR") is not None:
        tmp = os.getenv("TMPDIR") + "/xfer-tmp"
    elif os.getenv("TMP") is not None:
        tmp = os.getenv("TMP") + "/xfer-tmp"
    else:
        tmp = "/tmp/" + os.getenv("USER") + "/xfer-tmp"
    try:
        os.makedirs(tmp, exist_ok=True)
    except Exception as e:
        abort(str(e))
        tmp = None
    return tmp


def make_sock_dir(sockfile):
    directory = os.path.dirname(sockfile)
    try:
        os.makedirs(directory, exist_ok=True)
    except Exception as e:
        abort("failed making socket directory: " + str(e))
        return False
    return True


def reset_sock(sockfile):
    if os.path.exists(sockfile):
        msg("resetting: " + sockfile)
        try:
            os.remove(sockfile)
        except Exception as e:
            abort(str(e))
            return False
    return True


def run_server(args):
    global sock
    if sock is None:
        shutdown(args, 1)
    handlers = setup_handlers()
    sock.settimeout(0.1)
    while not cancelled:
        # msg("listen")
        sock.listen(1)
        msg("accept")
        connected = False
        while not cancelled and not connected:
            try:
                conn, addr = sock.accept()
                connected = True
            except socket.timeout:
                pass
            except Exception as e:
                abort("accept failed: " + str(e))
                return 1
        if cancelled:
            msg("cancelled accept loop...")
            break
        try:
            datagram = conn.recv(1024)
        except Exception as e:
            warn("connection dropped: " + str(e))
            datagram = None
        if datagram is not None:
            tokens = datagram.decode().strip().split()
            # msg(str(tokens))
            handle(handlers, conn, tokens)
        time.sleep(1)
    return 1


def setup_handlers():
    # Signal handler for Ctrl+C, SIGHUP
    # Allow SIGTERM to kill us (the default for the kill comand).
    import signal
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGHUP, signal_handler)

    # Methods:
    handlers = { "insert"  : do_insert,
                 "predict" : do_predict,
                 "quit"    : do_quit
                }
    return handlers


def signal_handler(unused_sig, unused_frame):
    global cancelled, sock
    print("\nxfer-server: cancelled!\n")
    if cancelled:
        print("\nxfer-server: closing socket!\n")
        sock.close()
    cancelled = True


def handle(handlers, conn, tokens):
    # msg("handle")
    if len(tokens) != 1:
        b = send(conn, "ERROR")
        if not b: return
    method = tokens[0]
    # msg("method: " + method)
    if method not in handlers:
        warn("unknown method: '%s'" % method)
        conn.send(("ERROR: unknown method: " + method).encode())
        return
    handlers[method](conn, tokens)


def do_insert(conn, tokens):
    global cancelled
    msg("do_insert()...")
    send(conn, "OK\n")
    done = False
    L = []
    while not done and not cancelled:
        line = recv_line(conn, L)
        if line is None:
            msg("connection dropped")
            return
        # msg("insert: line: " + line.strip())
        if line == "EOF\n":
            msg("insert: EOF")
            break
        b = predictor.insert(line.strip())
        if not b: break
        time.sleep(0.1)
    msg("do_insert(): done.")


def do_predict(conn, tokens):
    global cancelled
    msg("do_predict()...")
    send(conn, "OK\n")
    done = False
    L = []
    while not done and not cancelled:
        line = recv_line(conn, L)
        if line is None:
            msg("connection dropped")
            return
        # msg("predict: line: " + line.strip())
        if line == "EOF":
            msg("predict: EOF")
            break
        b, value = predictor.predict(line.strip())
        if not b: break
        send(conn, str(value) + "\n")
        time.sleep(0.1)
    msg("do_predict(): done.")


def do_quit(conn, tokens):
    global cancelled
    msg("do_quit()...")
    send(conn, "OK\n")
    cancelled = True


def shutdown(args, code):
    global sock, sockfile
    if sock is not None:
        msg("closing socket")
        sock.close()
    if args.socket is None:
        # msg("removing: " + sockfile)
        os.remove(sockfile)
    msg("shutdown.")
    exit(code)


if __name__ == "__main__":
    main()
