import socket
import threading
import pickle
import time
import sys
import os
import hashlib
from collections import OrderedDict
from networking import rpc_send, recv_all
from dht_utils import getHash, _is_between, update_successor_list, lookup_successor
import filemanager


#------DEFAULT VALUES ---------------
DEFAULT_IP = "127.0.0.1" #localhost
DEFAULT_PORT = 2000
RECV_BUFFER = 4096  #receiver buffer size

MAX_BITS = 10
MAX_NODES = 2**MAX_BITS

SUCCESSOR_LIST_SIZE = 10
STABILIZE_INTERVAL= 2.0
FIX_FINGERS_INTERVAL = 3.0
PING_INTERVAL = 2.0

class Node:
    def __init__(self, ip: str, port: int):
        self.ip = ip
        self.port = port
        self.address = (ip, port)
        self.id = getHash(f"{ip}:{port}")
        self.m_bits = MAX_BITS
        self.predecessor = self.address
        self.predecessor_id = self.id
        self.successor = self.address
        self.successor_id = self.id
        self.successor_list = [self.address]
        self.finger_table = OrderedDict()
        self.filename_list = []
        self.stop = False              #if True all bg processes stops
        self.last_fix_index = 0         #one finger updated per iteration
        self.lock = threading.Lock()    #Prevents race conditions between threads

        try:
            self.server = socket.socket(socket.AF_INET, socket.SOCK_STREAM) #IPv4, TCP
            self.server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            self.server.bind((self.ip, self.port))  #specify IP and port
            self.server.listen()
        except socket.error as e:
            print("Socket bind/listen failed:", e)
            raise 
        
    def handle_join_request(self, conn, addr, req):
        try:
            new_node = req[1]           # (ip, port) of joining node
            new_id = getHash(f"{new_node[0]}:{new_node[1]}")
        except Exception:
            try:
                conn.sendall(pickle.dumps([None]))
            except Exception:
                pass
            return

        with self.lock:
            old_pred = self.predecessor
            self.predecessor = new_node
            self.predecessor_id = new_id

        try:
            conn.sendall(pickle.dumps([old_pred]))
        except Exception:
            pass

        try:
            self.updateFTable()
        except Exception:
            pass
        try:
            self.updateOtherFTables()
        except Exception:
            pass
    
    #--------------------------------RPC HANDLERS--------------------------------
    def listen_loop(self):
        while not self.stop:
            try:
                conn, addr = self.server.accept()
                conn.settimeout(10.0)
                threading.Thread(target=self.connection_thread, args=(conn, addr), daemon=True).start() #start a new thread to handle this connection
            except Exception:
                continue

    def connection_thread(self, conn, addr):
        req = recv_all(conn)
        if req is None:
            try:
                conn.close()
            except Exception:
                pass
            return
        #req[0]->conn type, req[1]->addr(ip, port)
        try:
            connectionType = req[0]
        except Exception:
            try:
                conn.close()
            except Exception:
                pass
            return

        #Peer join req (type:0)
        if connectionType == 0:
            self.handle_join_request(conn, addr, req)
            self.print_menu()
        # Client upload/download (type:1)
        elif connectionType == 1:
            try:
                filemanager.transfer_file(self, conn, addr, req)
            except Exception:
                pass
            self.print_menu()
        # Ping (type:2)- return predecessor
        elif connectionType == 2:
            try:
                conn.sendall(pickle.dumps(self.predecessor))
            except Exception:
                pass
        # Lookup (type 3): return [flag, addr]
        elif connectionType == 3:
            self.lookupID(conn, addr, req)
        # Update successor/pred (type 4)
        elif connectionType == 4:
            if req[1] == 1:
                self.updateSucc(req)
            else:
                self.updatePred(req)
        # Update finger table request (type 5) -> returns my successor
        elif connectionType == 5:
            self.updateFTable()
            try:
                conn.sendall(pickle.dumps(self.successor))
            except Exception:
                pass
        # Request successor list (type:6)
        elif connectionType == 6:
            try:
                conn.sendall(pickle.dumps(self.successor_list))
            except Exception:
                pass
        else:
            pass
        try:
            conn.close()
        except Exception:
            pass

 # -------------------- Join / Leave --------------------
    def sendJoinRequest(self, ip, port):
        bootstrap = (ip, port)
        #ask bootstrap for successor of node's id
        succ = self.getSuccessor(bootstrap, self.id)
        self.successor = succ
        self.successor_id = getHash(addr)
        rpc_send(self.successor, ['notify', self.address])
        if not recv:
            print("Couldn't connect to bootstrap")
            return
        try:
            peerSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            peerSocket.settimeout(3.0)
            peerSocket.connect(recv)
            peerSocket.sendall(pickle.dumps([0, self.address]))
            resp = recv_all(peerSocket)
            peerSocket.close()
            if resp:
                oldPred = resp[0]
                self.predecessor = oldPred
                self.predecessor_id = getHash(f"{oldPred[0]}:{oldPred[1]}")
            self.successor = recv
            self.successor_id = getHash(f"{recv[0]}:{recv[1]}")
            # tell predecessor to update its successor to me
            if self.predecessor:
                rpc_send(self.predecessor, [4, 1, self.address])
            self.updateFTable()
            self.updateOtherFTables()
            print("Joined network. pred:", self.predecessor, "succ:", self.successor)
        except Exception as e:
            print("sendJoinRequest error:", e)
    
    def leaveNetwork(self):
        try:
            if self.successor and self.successor != self.address:
                rpc_send(self.successor, [4, 0, self.predecessor])
            if self.predecessor and self.predecessor != self.address:
                rpc_send(self.predecessor, [4, 1, self.successor])
        except Exception:
            pass

        # replicate files to successor
        if self.filename_list and self.successor and self.successor != self.address:
            for filename in list(self.filename_list):
                try:
                    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    s.settimeout(5)
                    s.connect(self.successor)
                    s.sendall(pickle.dumps([1, 1, filename]))  # replicate flag = 1
                    resp = recv_all(s)
                    with open(filename, 'rb') as f:         #read as binary since files can be jpeg, audio
                        while True:
                            chunk = f.read(RECV_BUFFER)
                            if not chunk:
                                break
                            s.sendall(chunk)
                    s.close()
                    print("Replicated", filename, "to", self.successor)
                except Exception:
                    print("Replication failed for", filename)
        self.predecessor = None
        self.predecessor_id = self.id
        self.successor = self.address
        self.successor_id = self.id
        self.successor_list = [self.address]
        self.finger_table.clear()
        print(self.address, "left network.")

# -------------------- Lookup logic --------------------
    def lookupID(self, conn, addr, req):
        """Handle lookup requests (type 3). Reply with [flag, address]."""
        keyID = req[1]
        with self.lock:
            if self.id == keyID or (self.predecessor and self.predecessor_id < keyID <= self.id) or (self.predecessor and self.id < self.predecessor_id and (keyID > self.predecessor_id or keyID <= self.id)):
                #flag 0 => I am the successor
                try:
                    conn.sendall(pickle.dumps([0, self.address]))
                except Exception:
                    pass
                return
            #only node in network
            if self.successor == self.address:
                try:
                    conn.sendall(pickle.dumps([0, self.address]))
                except Exception:
                    pass
                return
            # find closest preceding node for keyID using finger table
            candidate = None
            for start, (nid, addr_entry) in reversed(self.finger_table.items()):
                if nid is None:
                    continue
                if _is_between(self.id, nid, keyID):
                    candidate = addr_entry
                    break
            if not candidate:
                candidate = self.successor
            try:
                conn.sendall(pickle.dumps([1, candidate]))
            except Exception:
                pass
            return

    def getSuccessor(self, contact_addr, keyID):
        return lookup_successor(self, contact_addr, keyID)

# -------------------- Finger table maintenance --------------------
    def updateFTable(self):
        with self.lock:
            self.finger_table.clear()
            for i in range(self.m_bits):
                start = (self.id + (2**i)) % MAX_NODES
                #find successor for start
                if self.successor == self.address:
                    # only 1 node
                    self.finger_table[start] = (self.id, self.address)
                else:
                    succ = self.getSuccessor(self.successor, start)
                    if succ:
                        succ_id = getHash(f"{succ[0]}:{succ[1]}")
                        self.finger_table[start] = (succ_id, succ)
                    else:
                        # fallback: point to current successor
                        self.finger_table[start] = (self.successor_id, self.successor)

    def updateOtherFTables(self):
        """Ask nodes around the ring to refresh (type 5)."""
        visited = set()
        here = self.successor
        while here and here != self.address and here not in visited:
            visited.add(here)
            resp = rpc_send(here, [5])
            if resp is None:
                break
            here = resp
            if here == self.successor:
                break

    def fix_fingers_incremental(self):
        """Fix one finger per call (incrementally)."""
        with self.lock:
            i = self.last_fix_index
            start = (self.id + (2**i)) % MAX_NODES
            if self.successor == self.address:
                self.finger_table[start] = (self.id, self.address)
            else:
                succ = self.getSuccessor(self.successor, start)
                if succ:
                    succ_id = getHash(f"{succ[0]}:{succ[1]}")
                    self.finger_table[start] = (succ_id, succ)
            self.last_fix_index = (i + 1) % self.m_bits
 

    def stabilize(self):
        """Periodically ensure successor/predecessor pointers are correct."""
        try:
            if self.successor == self.address:
                update_successor_list(self)
                return
            resp = rpc_send(self.successor, [2])  # ping returns successor's predecessor
            succ_pred = resp
            if succ_pred:
                succ_pred_id = getHash(f"{succ_pred[0]}:{succ_pred[1]}")
                if _is_between(self.id, succ_pred_id, self.successor_id):
                    self.successor = succ_pred
                    self.successor_id = succ_pred_id
            # notify the new succ to update the pred
            rpc_send(self.successor, [4, 0, self.address])
            update_successor_list(self)
        except Exception:
            # on any error, attempt to pick a new successor from successor_list
            self._failover_successor()

    def notify(self, candidate):
        """Called remotely: when a node tells me 'I might be your predecessor'."""
        with self.lock:
            if self.predecessor is None:
                self.predecessor = candidate
                self.predecessor_id = getHash(f"{candidate[0]}:{candidate[1]}")
                return
            cand_id = getHash(f"{candidate[0]}:{candidate[1]}")
            if _is_between(self.predecessor_id, cand_id, self.id):
                self.predecessor = candidate
                self.predecessor_id = cand_id

    def _failover_successor(self):
        """If current successor appears dead, pick next from successor_list or finger_table."""
        with self.lock:
            for s in self.successor_list:
                if s and s != self.successor and s != self.address:
                    if rpc_send(s, [2]) is not None:
                        self.successor = s
                        self.successor_id = getHash(f"{s[0]}:{s[1]}")
                        break
            else:
                # fallback, single node
                self.successor = self.address
                self.successor_id = self.id
                self.successor_list = [self.address]

# -------------------- RPC update handlers --------------------
    def updateSucc(self, req):
        """Req format:[4, 1, newSucc]-> update successor"""
        new_succ = req[2]
        self.successor = new_succ
        self.successor_id = getHash(f"{new_succ[0]}:{new_succ[1]}")
        update_successor_list(self)

    def updatePred(self, req):
        """Req format:[4, 0, newPred] -> update my predecessor"""
        new_pred = req[2]
        self.predecessor = new_pred
        self.predecessor_id = getHash(f"{new_pred[0]}:{new_pred[1]}")

# -------------------- Maintenance loops --------------------
    def stabilize_loop(self):
        while not self.stop:
            try:
                self.stabilize()
            except Exception:
                pass
            time.sleep(STABILIZE_INTERVAL)

    def fix_fingers_loop(self):
        while not self.stop:
            try:
                self.fix_fingers_incremental()
            except Exception:
                pass
            time.sleep(FIX_FINGERS_INTERVAL)

    def start_background(self):
        threading.Thread(target=self.stabilize_loop, daemon=True).start()
        threading.Thread(target=self.fix_fingers_loop, daemon=True).start()
        threading.Thread(target=self.pingSucc_loop, daemon=True).start()

# -------------------- Ping-based fail detection using successor_list --------------------
    def pingSucc_loop(self):
        while not self.stop:
            time.sleep(PING_INTERVAL)
            if self.successor == self.address:
                continue
            try:
                resp = rpc_send(self.successor, [2])  # ping -> returns predecessor
                if resp is None:
                    # failover
                    print("\nDetected offline successor; attempting failover...")
                    self._failover_successor()  #new succ 
                    # inform new successor to update its pred
                    if self.successor and self.successor != self.address:
                        rpc_send(self.successor, [4, 0, self.address])
                        self.updateFTable()
                        self.updateOtherFTables()
            except Exception:
                continue

# -------------------- Helpers --------------------
    def print_menu(self):
        print("\n1. Join Network\n2. Leave Network\n3. Upload File\n4. Download File")
        print("5. Print Finger Table\n6. Print my predecessor and successor\n")

    def printFTable(self):
        print("Printing F Table")
        for key, value in self.finger_table.items():
            print("StartID:", key, " -> ", value)

    def stop(self):
        self.stop = True
        try:
            self.server.close()
        except Exception:
            pass

# -------------------- Run as script --------------------
if __name__ == "__main__":
    ip = DEFAULT_IP
    port = DEFAULT_PORT
    if len(sys.argv) >= 3:
        ip = sys.argv[1]
        port = int(sys.argv[2])
    elif len(sys.argv) == 2:
        port = int(sys.argv[1])

    node = Node(ip, port)
    print("Node started:", node.address, "ID:", node.id)
    # start server threads
    threading.Thread(target=node.listen_loop, daemon=True).start()
    node.start_background()

    # simple interactive loop (non-blocking)
    try:
        while True:
            node.print_menu()
            cmd = input("> ").strip()
            if cmd == "1":
                b_ip = input("Bootstrap IP: ").strip() or DEFAULT_IP
                b_port = int(input("Bootstrap Port: ").strip() or DEFAULT_PORT)
                node.sendJoinRequest(b_ip, b_port)
            elif cmd == "2":
                node.leaveNetwork()
            elif cmd == "3":
                fname = input("Filename to upload: ").strip()
                # find responsible node for this filename
                fileID = getHash(fname)
                recv = node.getSuccessor(node.successor, fileID)
                if recv:
                    # call filemanager upload helper
                    filemanager.upload_file(node, fname, recv, replicate=True)
                else:
                    print("Could not find successor to upload.")
            elif cmd == "4":
                fname = input("Filename to download: ").strip()
                filemanager.download_file(node, fname)
            elif cmd == "5":
                node.updateFTable()
                node.printFTable()
            elif cmd == "6":
                print("ID:", node.id, "Pred:", node.predecessor, node.predecessor_id, "Succ:", node.successor, node.successor_id)
            elif cmd in ("quit", "exit"):
                node.stop()
                break
            else:
                print("Unknown command.")
    except KeyboardInterrupt:
        node.stop()
        print("Exiting.")
