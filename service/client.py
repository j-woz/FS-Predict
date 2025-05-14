
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
    if args.input is None: abort("provide input file!")
    if not os.path.exists(args.input):
        abort("input does not exist: '%s'" % args.input)

    send(sock, "predict\n")
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

    with open(args.output, "w") as fp_out:
        with open(args.input, "r") as fp_in:
            while True:
                line = fp_in.readline()
                if len(line) == 0: break
                send(sock, line)
                value = recv_line(sock, [])
                line = line.strip()
                # Reduce precision:
                value_string = "%0.6f" % float(value)
                # msg("prediction: %s -> %s" % (line, value_string))
                fp_out.write("%s,%s\n" % (line, value_string))
            send(sock, "EOF\n")
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
