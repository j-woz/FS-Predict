
"""
XFER CLIENT MAIN
"""

import socket
import os
import time
# from collections import deque

from utils import send, recv, recv_line, send_file


def main():
    args = parse_args()
    # print(str(args))
    if args.socket is None:
        abort("provide the socket file!")
    try:
        sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        sock.connect(args.socket)
        print("connected")
    except socket.error as e:
        print("error connecting: " + str(e))
    if args.method == "insert":
        do_insert(args, sock)
    else:
        abort("unknown method: '%s'" % args.method)
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
    args = parser.parse_args()
    return args


def msg(m):
    print("xfer-client: " + str(m))


def abort(m):
    msg("abort: " + str(m))
    exit(1)


def do_insert(args, sock):
    if args.input is None:
        abort("provide input file!")
    if not os.path.exists(args.input):
        abort("input does not exist: '%s'" % args.input)

    send(sock, "insert\n")
    try:
        L = []
        line = recv_line(sock, L)
    except Exception as e:
        abort("recvd bad line: " + str(e))
        return False
    if line is None:
        abort("connection dropped.")
    msg("response: '%s'" % line.strip())
    if len(line) == 0:
        abort("received empty response!")
    if line.startswith("ERROR"):
        abort(line)

    send_file(sock, args.input)
    # for i in range(0,5):
    #     send(sock, "data: %i\n" % i)
    # send(sock, "EOF\n")
    return True


if __name__ == "__main__":
    main()
