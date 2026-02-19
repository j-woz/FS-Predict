"""
XFER CLIENT MAIN (OBSERVE + PREDICT)
"""

import socket
import os

from utils import send, recv_line, send_file


def main():
    args = parse_args()
    if args.socket is None:
        abort("provide the socket file!")
    sock = connect(args)
    do_method(args, sock)
    msg("OK.")


def parse_args():
    import argparse
    parser = argparse.ArgumentParser(description="Query server")
    parser.add_argument("-s", "--socket", help="The local socket file")
    parser.add_argument("method", help="The query method to invoke")
    parser.add_argument("-i", "--input", help="The input query data to send")
    parser.add_argument("-o", "--output", help="The output data to save (predict only)")
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
    if args.method == "observe":
        do_observe(args, sock)
    elif args.method == "predict":
        do_predict(args, sock)
    elif args.method == "quit":
        do_quit(sock)
    else:
        abort(f"unknown method: '{args.method}'")


def _check_input_file(path: str):
    if path is None:
        abort("provide input file!")
    if not os.path.exists(path):
        abort(f"input does not exist: '{path}'")


def _handshake(sock, cmd: str):
    """
    Send command, wait for OK or ERROR line.
    Returns first response line (string).
    """
    msg(f"client: sending {cmd} command...")
    send(sock, f"{cmd}\n")
    line = recv_line(sock, [])
    if line is None or not line.strip():
        abort("no response from server")
    msg(f"client: server response -> '{line.strip()}'")
    if line.startswith("ERROR"):
        abort(line.strip())
    return line


def do_observe(args, sock):
    """
    OBSERVE:
      1) Send 'observe' + wait OK
      2) Send entire input file (7-col rows including DURATION) + EOF
      3) Read until server EOF (server usually just returns EOF)
    """
    _check_input_file(args.input)

    _handshake(sock, "observe")

    msg(f"client: sending OBSERVE data from {args.input}")
    send_file(sock, args.input)
    msg("client: sent EOF for observe")

    # read until server EOF
    while True:
        line = recv_line(sock, [])
        if line is None:
            abort("connection dropped during observe response")
        if line.strip() == "EOF":
            msg("client: observe complete (server EOF)")
            break
        if line.startswith("ERROR"):
            abort(line.strip())

    return True


def do_predict(args, sock):
    """
    PREDICT (future covariates only):
      1) Send 'predict' + wait OK
      2) Stream input file rows + EOF
      3) Receive predictions until server EOF
      4) Write "<timestamp>,<pred>" lines to output file
    """
    _check_input_file(args.input)
    if args.output is None:
        abort("provide output file (-o) for predict!")

    _handshake(sock, "predict")

    msg(f"client: sending PREDICT data from {args.input}")
    send_file(sock, args.input)
    msg("client: sent EOF for predict")

    msg("client: receiving predictions...")

    # IMPORTANT: persistent buffer for recv_line()
    buf = []

    with open(args.output, "w") as fp_out:
        while True:
            pred_line = recv_line(sock, buf)

            if pred_line is None:
                abort("connection dropped before all predictions received")

            line = pred_line.strip()

            if line == "EOF":
                msg("client: received server EOF, stopping")
                break

            if line.startswith("ERROR"):
                msg(f"client: server error -> {line}")
                continue

            # pred_line already includes '\n' in most implementations of recv_line()
            fp_out.write(pred_line)

            msg(f"Received prediction: {line}")

    msg(f"client: Wrote predictions to {args.output}")
    return True

def do_quit(sock):
    send(sock, "quit\n")
    line = recv_line(sock, [])
    if line is None:
        abort("connection dropped.")
    msg("response: '%s'" % line.strip())
    if not line.strip():
        abort("received empty response!")
    if line.startswith("ERROR"):
        abort(line.strip())
    return True


if __name__ == "__main__":
    main()
