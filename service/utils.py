
"""
UTILS
"""

import time


def send(sock, s):
    # print("send: '%s'" % s.strip())
    try:
        sock.send(s.encode())
    except BrokenPipeError as e:
        print("send: " + str(e))
        return False
    return True


def recv(sock):
    try:
        s = sock.recv(1024).decode()
    except ConnectionResetError as e:
        print("recv(): " + str(e))
        return None
    return s

def recv_line(sock, L):
    """
    Read exactly one line (ending with '\\n') from the socket, even across fragmented recv() calls.
    L should be passed in as a list (initially empty)—we’ll keep any leftover bytes in L[0].
    """


    # Initialize or grab the existing buffer
    if not L:
        L.append("")
    buffer = L[0]

    while True:
        # If we already have a newline, split it out
        if "\n" in buffer:
            line, rest = buffer.split("\n", 1)
            L[0] = rest
            return line + "\n"

        # Otherwise, read more data
        chunk = recv(sock)
        if chunk is None:
            return None   # connection closed
        if chunk == "":
            # no data this moment, wait a bit
            time.sleep(0.1)
            continue

        buffer += chunk
        L[0] = buffer


def send_file(sock, filename):
    with open(filename, "r") as fp:
        while True:
            line = fp.readline()
            if len(line) == 0: break
            send(sock, line)
    send(sock, "EOF\n")
