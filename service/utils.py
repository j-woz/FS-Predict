
"""
UTILS
"""

import time


def send(sock, s):
    print("send: '%s'" % s.strip())
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
    for i in range(0, len(L)):
        # print("check: %i" % i)
        s = L[i]
        if "\n" in s:
            # print("NL")
            result = ""
            for _ in range(0, i+1):
                t = L.pop(0)
                # print("+ '%s'" % t.strip())
                result += t
            return result

    while True:
        # print("recv")
        s = recv(sock)
        if s is None: return None
        if len(s) == 0:
            time.sleep(0.1)
            continue
        tokens = s.split("\n")
        if len(tokens) > 1:
            t = "".join(L)
            t += tokens[0]
            L.clear()
            for token in tokens[1:]:
                L.append(token + "\n")
                # print("append: %i" % len(L))
            return t
        # print("chunk")
        L.append(s)
    return None


def send_file(sock, filename):
    with open(filename, "r") as fp:
        while True:
            line = fp.readline()
            if len(line) == 0:
                break
            send(sock, line)
    send(sock, "EOF\n")
