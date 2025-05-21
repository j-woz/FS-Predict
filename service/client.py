
"""
XFER CLIENT MAIN
"""

import socket
import os
# from collections import deque

from utils import send, recv_line, send_file


def main():
    args = parse_args()
    # print(str(args))
    if args.socket is None:
        abort("provide the socket file!")
    sock = connect(args)
    do_method(args, sock)
    msg("OK.")


def parse_args():
    import argparse
    parser = argparse.ArgumentParser(description="Query server")
    parser.add_argument("-s", "--socket",
                        help="The local socket file")
    parser.add_argument("method",
                        help="The query method to invoke")
    parser.add_argument("-i", "--input",
                        help="The input query data to send")
    parser.add_argument("-o", "--output",
                        help="The output data to save")
    args = parser.parse_args()
    return args


def msg(m):
    print("xfer-client: " + str(m))


def abort(m):
    msg("abort: " + str(m))
    exit(1)


def connect(args):
    try:
        sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        sock.connect(args.socket)
    except socket.error as e:
        abort("error connecting: " + str(e))
    msg("connected")
    return sock


def do_method(args, sock):
    if   args.method == "insert":
        do_insert(args, sock)
    elif args.method == "predict":
        do_predict(args, sock)
    elif args.method == "quit":
        do_quit(sock)
    else:
        abort("unknown method: '%s'" % args.method)


def do_insert(args, sock):
    check_input(args)
    send(sock, "insert\n")
    try:
        L = []
        line = recv_line(sock, L)
    except Exception as e:
        abort("recvd bad line: " + str(e))
        return False
    if line is None: abort("connection dropped.")
    msg("response: '%s'" % line.strip())
    if len(line) == 0: abort("received empty response!")
    if line.startswith("ERROR"): abort(line)

    send_file(sock, args.input)

    return True


def do_predict(args, sock):
    """
    1) Send 'predict' + wait OK
    2) Stream input file rows + EOF
    3) Read back predictions until server EOF
    4) Write <orig_row>,<pred> into output
    """
    if args.input is None:
        abort("provide input file!")
    if not os.path.exists(args.input):
        abort(f"input does not exist: '{args.input}'")

    # 1) Kick off prediction
    msg("client: sending predict command...")
    send(sock, "predict\n")
    line = recv_line(sock, [])
    msg(f"client: server response after predict -> '{line.strip()}'")
    if line is None or not line.strip():
        abort("no response from server")
    if line.startswith("ERROR"):
        abort(line)

    # 2) Read & send all input rows, then EOF
    msg(f"client: reading input file {args.input}")
    raw_lines = open(args.input).read().splitlines()
    msg(f"client: read {len(raw_lines)} lines from input")
    msg("client: streaming lines to server...")
    for l in raw_lines:
        send(sock, (l + "\n"))
    send(sock, "EOF\n")
    msg("client: sent EOF")

    # 3) Receive until server sends EOF
    msg("client: receiving predictions...")
    with open(args.output, "w") as fp_out:
        while True:
            pred_line = recv_line(sock, [])
            if pred_line is None:
                abort("connection dropped before all predictions received")
            # server’s EOF marker
            if pred_line.strip() == "EOF":
                msg("client: received server EOF, stopping")
                break

            # pred_line is now "TIMESTAMP_last,pred_value\n"
            fp_out.write(pred_line)
            msg(f"client: wrote '{pred_line.strip()}'")

    msg(f"client: Wrote predictions to {args.output}")
    return True


def check_input(args):
    if args.input is None: abort("provide input file!")
    if not os.path.exists(args.input):
        abort("input does not exist: '%s'" % args.input)


def check_io(args):
    check_input(args)
    if args.output is None: abort("provide output file!")


def do_quit(sock):
    send(sock, "quit\n")
    try:
        L = []
        line = recv_line(sock, L)
    except Exception as e:
        abort("recvd bad line: " + str(e))
        return False
    if line is None: abort("connection dropped.")
    msg("response: '%s'" % line.strip())
    if len(line) == 0: abort("received empty response!")
    if line.startswith("ERROR"): abort(line)
    return True


if __name__ == "__main__": main()
