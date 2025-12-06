import socket
import pickle

RECV_BUFFER = 4096

def recv_all(sock, buffer_size= None, timeout= 5.0):
    if buffer_size is None:
        buffer_size = RECV_BUFFER

    data = b''
    prev_timeout = sock.gettimeout()
    try:
        sock.settimeout(timeout)
        while True:
            try:
                part = sock.recv(buffer_size)
            except socket.timeout:
                break
            if not part:
                break
            data += part
            try:
                return pickle.loads(data)
            except Exception:
                continue
    finally:
        try:
            sock.settimeout(prev_timeout)
        except Exception:
            pass

    if data:
        try:
            return pickle.loads(data)
        except Exception:
            return None
    return None

def rpc_send(addr, msg, timeout=3.0, buffer_size=None):
    if buffer_size is None:
        buffer_size = RECV_BUFFER

    s = None
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)   #IPv4, TCP
        s.settimeout(timeout)
        s.connect(addr)
        s.sendall(pickle.dumps(msg))
        resp = recv_all(s, buffer_size=buffer_size, timeout=timeout)
        return resp
    except Exception:
        return None
    finally:
        if s is not None:
            try:
                s.close()
            except Exception:
                pass
