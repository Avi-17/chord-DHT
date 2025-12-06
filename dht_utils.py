import hashlib
from typing import Any, List, Optional
from networking import rpc_send


DEFAULT_SUCCESSOR_LIST_SIZE = 10
MAX_NODES = 2 ** 10


def getHash(key):
    h = hashlib.sha1(key.encode()).hexdigest()
    return int(h, 16) % MAX_NODES


def update_successor_list(node, size=DEFAULT_SUCCESSOR_LIST_SIZE):
    try:
        if not getattr(node, "successor", None):
            node.successor_list = [node.address]
            return
        resp = rpc_send(node.successor, [6])
        if isinstance(resp, list) and len(resp) > 0:
            # Prepend our successor and take up to (size-1) entries from their list
            node.successor_list = [node.successor] + resp[: max(0, size - 1)]
        else:
            node.successor_list = [node.successor]
    except Exception:
        # Best-effort: on any error leave successor_list as-is (or set to self)
        try:
            node.successor_list = getattr(node, "successor_list", [node.address])
        except Exception:
            pass


def _is_between(a, x, b):
    if a < b:
        return a < x <= b
    else:
        # wrap-around
        return x > a or x <= b

def lookup_successor(node, contact_addr, keyID):
    current = contact_addr
    tries = 0
    while tries < 20:
        tries += 1
        resp = rpc_send(current, [3, keyID])
        if resp is None:
            # fallback: use successor list
            for s in node.successor_list:
                if s != current:
                    current = s
                    break
            else:
                return None
            continue

        flag, addr = resp
        if flag == 0:
            return addr
        else:
            current = addr

    return None 