import socket
import threading
import pickle
import time
import sys
import os
import hashlib
from collections import OrderedDict

#------DEFAULT VALUES ---------------
DEFAULT_IP = "127.0.0.1" #localhost, locally testing chord ring
DEFAULT_PORT = 2000
RECV_BUFFER = 4096  #TCP receive buffer size

MAX_BITS = 10
MAX_NODES = 2**MAX_BITS

SUCCESSOR_LIST_SIZE = 10    #log 
STABILIZE_INTERVAL= 2.0
FIX_FINGERS_INTERVAL = 3.0
PING_INTERVAL = 2.0

# ---------------------UTILITY FUNCTIONS -------------------
def getHash(key: str) -> int:
    h = hashlib.sha1(key.encode()).hexdigest()
    return int(h, 16) % MAX_NODES

def recv_all(sock):
    data = b''      #empty byte buffer
    sock.settimeout(5.0)
    while True:
        try:
            part = sock.recv(RECV_BUFFER)
        except socket.timeout:
            break
        if not part:
            break
        data += part
        try:
            obj = pickle.loads(data)
            return obj
        except Exception:
            continue
    
    try:
        return pickle.loads(data) if data else None
    except Exception:
        return None

def rpc_send(addr, msg, timeout=3.0):
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(timeout)
        s.connect(addr)
        s.sendall(pickle.dumps(msg))
        resp = recv_all(s)
        s.close()
        return resp
    except Exception:
        return None

# --------------------------- DEFINING NODE ----------------------
class Node:
    def __init__(self, ip: str, port: int):
        self.ip = ip
        self.port = port
        self.address = (ip, port)
        self.id = getHash(f"{ip}:{port}")
        self.m_bits = MAX_BITS
        self.predecessor = None
        self.predecessor_id = self.id
        self.successor = self.address
        self.successor_id = self.id
        self.successor_list = [self.address]
        self.finger_table = OrderedDict()
        self.filename_list = []
        self._stop = False              #if True all bg processes stops
        self.last_fix_index = 0         #one finger updated per iteration
        self.lock = threading.Lock()    #Prevents race conditions between threads

        # server socket, bind to THIS node's ip/port
        try:
            self.server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            # allow quick restart
            self.server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            self.server.bind((self.ip, self.port))
            self.server.listen()
        except socket.error as e:
            print("Socket bind/listen failed:", e)
            raise
        
    def _handle_join_request(self, conn, addr, req):
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
            # Save the old predecessor to send back to the newcomer
            old_pred = self.predecessor if self.predecessor is not None else self.address
            old_pred_id = getHash(f"{old_pred[0]}:{old_pred[1]}") if old_pred else self.id

            # Set this node's predecessor to the new joining node
            self.predecessor = new_node
            self.predecessor_id = new_id

        # Send the old predecessor back to the joining node (so it can set its pred)
        try:
            conn.sendall(pickle.dumps([old_pred]))
        except Exception:
            pass

        try:
            # update F-TABLE
            self.updateFTable()
        except Exception:
            pass

        try:
            #update other's F-TABLES
            self.updateOtherFTables()
        except Exception:
            pass
    
    #--------------------------------RPC HANDLERS--------------------------------
    def listen_loop(self):      #listens for incoming connections
        while not self._stop:
            try:
                conn, addr = self.server.accept()
                conn.settimeout(10.0)
                threading.Thread(target=self.connection_thread, args=(conn, addr), daemon=True).start() #start a new thread to handle this connection
            except Exception:
                continue

    def connection_thread(self, conn: socket.socket, addr):
        req = recv_all(conn)
        if req is None:
            conn.close()
            return
        #first element is connectionType, req[1] should be peer address tuple (ip,port)
        try:
            connectionType = req[0]
        except Exception:
            conn.close()
            return

        #Peer join req type:0
        if connectionType == 0:
            self._handle_join_request(conn, addr, req)
            self.print_menu()
        # Client upload/download (type 1)
        elif connectionType == 1:
            self.transferFile(conn, addr, req)
            self.print_menu()
        # Ping (type 2) -> return my predecessor
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
        # Request successor list (type 6)
        elif connectionType == 6:
            try:
                conn.sendall(pickle.dumps(self.successor_list))
            except Exception:
                pass
        else:
            # unknown type
            pass
        try:
            conn.close()
        except Exception:
            pass

 # -------------------- Join / Leave --------------------
    def sendJoinRequest(self, ip, port):
        bootstrap = (ip, port)
        #ask bootstrap for successor of node's id
        recv = self.getSuccessor(bootstrap, self.id)
        if not recv:
            print("Couldn't connect to bootstrap")
            return
        try:
            peerSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            peerSocket.settimeout(3.0)
            peerSocket.connect(recv)
            # send join request type 0 with my address
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
            # update fingertable nearby peers
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
        if self.filenameList and self.successor and self.successor != self.address:
            for filename in list(self.filenameList):
                try:
                    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    s.settimeout(5)
                    s.connect(self.successor)
                    s.sendall(pickle.dumps([1, 1, filename]))  # replicate flag = 1
                    # wait for ack
                    _ = recv_all(s)  # we ignore content
                    # send file bytes
                    with open(filename, 'rb') as f:
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
            # Case: I'm responsible
            if self.id == keyID or (self.predecessor and self.predecessor_id < keyID <= self.id) or (self.predecessor and self.id < self.predecessor_id and (keyID > self.predecessor_id or keyID <= self.id)):
                # flag 0 => I am successor/responsible
                try:
                    conn.sendall(pickle.dumps([0, self.address]))
                except Exception:
                    pass
                return
            # If only node in network
            if self.successor == self.address:
                try:
                    conn.sendall(pickle.dumps([0, self.address]))
                except Exception:
                    pass
                return
            # Otherwise forward using finger table (closest preceding finger)
            # find closest preceding node for keyID
            candidate = None
            for start, (nid, addr_entry) in reversed(self.finger_table.items()):
                if nid is None:
                    continue
                if self._is_between(self.id, nid, keyID):
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
        """Iterative lookup: ask contact_addr for successor of keyID. Returns an address or None."""
        current = contact_addr
        tries = 0
        while True:
            tries += 1
            if tries > 20:
                return None
            resp = rpc_send(current, [3, keyID])
            if resp is None:
                # try next candidate (if we have fingers) else fail
                # fallback: try successor_list entries
                if current == contact_addr and self.successor_list:
                    # try the first successor in our list (if different)
                    for s in self.successor_list:
                        if s != current:
                            current = s
                            break
                    else:
                        return None
                else:
                    return None
            else:
                flag = resp[0]
                addr = resp[1]
                if flag == 0:
                    return addr
                else:
                    # continue with provided addr
                    current = addr

# -------------------- Finger table maintenance --------------------
    def updateFTable(self):
        """Recompute finger table entries by querying network for each start."""
        with self.lock:
            self.finger_table.clear()
            for i in range(self.m_bits):
                start = (self.id + (1 << i)) % MAX_NODES
                # find successor for start
                if self.successor == self.address:
                    # only node
                    self.finger_table[start] = (self.id, self.address)
                else:
                    succ = self.getSuccessor(self.successor, start)
                    if succ:
                        succ_id = getHash(f"{succ[0]}:{succ[1]}")
                        self.finger_table[start] = (succ_id, succ)
                    else:
                        self.finger_table[start] = (None, None)

    def updateOtherFTables(self):
        """Ask nodes around the ring to refresh (type 5). We iterate successors until we get back to self."""
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
            start = (self.id + (1 << i)) % MAX_NODES
            if self.successor == self.address:
                self.finger_table[start] = (self.id, self.address)
            else:
                succ = self.getSuccessor(self.successor, start)
                if succ:
                    succ_id = getHash(f"{succ[0]}:{succ[1]}")
                    self.finger_table[start] = (succ_id, succ)
            self.last_fix_index = (i + 1) % self.m_bits

# -------------------- Stabilization and successor list --------------------
    def update_successor_list(self):
        """Ask my successor for its successor_list and update mine."""
        try:
            if not self.successor:
                self.successor_list = [self.address]
                return
            resp = rpc_send(self.successor, [6])
            if isinstance(resp, list) and len(resp) > 0:
                new_list = [self.successor] + resp[:SUCCESSOR_LIST_SIZE - 1]
                self.successor_list = new_list
            else:
                self.successor_list = [self.successor]
        except Exception:
            pass

    def stabilize(self):
        """Periodically ensure successor/predecessor pointers are correct."""
        try:
            if self.successor == self.address:
                self.update_successor_list()
                return
            # ask successor for its predecessor
            resp = rpc_send(self.successor, [2])  # ping returns successor's predecessor
            succ_pred = resp
            if succ_pred:
                succ_pred_id = getHash(f"{succ_pred[0]}:{succ_pred[1]}")
                # if succ_pred is between me and my successor, then it should be my successor
                if self._is_between(self.id, succ_pred_id, self.successor_id):
                    # update successor to succ_pred
                    self.successor = succ_pred
                    self.successor_id = succ_pred_id
            # notify successor that I might be its predecessor
            rpc_send(self.successor, [4, 0, self.address])
            # refresh successor list
            self.update_successor_list()
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
            if self._is_between(self.predecessor_id, cand_id, self.id):
                self.predecessor = candidate
                self.predecessor_id = cand_id

    def _failover_successor(self):
        """If current successor appears dead, pick next from successor_list or finger_table."""
        with self.lock:
            for s in self.successor_list:
                if s and s != self.successor and s != self.address:
                    # test reachability
                    if rpc_send(s, [2]) is not None:
                        self.successor = s
                        self.successor_id = getHash(f"{s[0]}:{s[1]}")
                        break
            else:
                # fallback: single node
                self.successor = self.address
                self.successor_id = self.id
                self.successor_list = [self.address]

# -------------------- RPC update handlers --------------------
    def updateSucc(self, req):
        """Request format: [4, 1, newSucc] -> update my successor to newSucc"""
        new_succ = req[2]
        self.successor = new_succ
        self.successor_id = getHash(f"{new_succ[0]}:{new_succ[1]}")
        # refresh list
        self.update_successor_list()

    def updatePred(self, req):
        """Request format: [4, 0, newPred] -> update my predecessor"""
        new_pred = req[2]
        self.predecessor = new_pred
        self.predecessor_id = getHash(f"{new_pred[0]}:{new_pred[1]}")

# -------------------- File transfer handlers (kept similar to your repo) --------------------
    def transferFile(self, connection, address, rDataList):
        # Choice: 0 = download, 1 = upload (replicate), -1 = upload single without replicate
        choice = rDataList[1]
        filename = rDataList[2]
        # IF client wants to download file
        if choice == 0:
            print("Download request for file:", filename)
            try:
                if filename not in self.filenameList:
                    connection.sendall(b"NotFound")
                else:
                    connection.sendall(b"Found")
                    self.sendFile(connection, filename)
            except ConnectionResetError as error:
                print("Client disconnected during download")
        # ELSE IF client wants to upload something to network
        elif choice == 1 or choice == -1:
            print("Receiving file:", filename)
            # If replicate: don't add to filenameList multiple times
            if filename not in self.filenameList:
                self.filenameList.append(filename)
            # respond back to sender (ack)
            try:
                connection.sendall(b"OK")
                self.receiveFile(connection, filename)
                print("Upload complete:", filename)
            except Exception as e:
                print("Error receiving file:", e)
            # if replicate flag == 1 and successor != self, replicate further
            if choice == 1 and self.successor and self.successor != self.address:
                try:
                    self.uploadFile(filename, self.successor, False)
                except Exception:
                    pass

    def sendFile(self, connection, filename):
        print("Sending file:", filename)
        try:
            with open(filename, 'rb') as file:
                while True:
                    chunk = file.read(RECV_BUFFER)
                    if not chunk:
                        break
                    connection.sendall(chunk)
            print("File sent:", filename)
        except Exception as e:
            print("sendFile error:", e)

    def receiveFile(self, connection, filename):
        # Avoid overwriting an existing non-empty file
        if os.path.exists(filename) and os.path.getsize(filename) > 0:
            print("File already present locally:", filename)
            return
        try:
            with open(filename, 'wb') as f:
                while True:
                    data = connection.recv(RECV_BUFFER)
                    if not data:
                        break
                    f.write(data)
            print("Saved file:", filename)
        except ConnectionResetError:
            print("Transfer interrupted for:", filename)
            # cleanup partial file
            try:
                os.remove(filename)
            except Exception:
                pass

# -------------------- Upload/download API --------------------
    def uploadFile(self, filename, recvIPport, replicate=True):
        print("Uploading file", filename, "to", recvIPport)
        if not os.path.exists(filename):
            print("File not present:", filename)
            return
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.settimeout(5)
            s.connect(recvIPport)
            s.sendall(pickle.dumps([1, 1 if replicate else -1, filename]))
            # wait ack
            ack = recv_all(s)
            # Now stream file bytes
            with open(filename, 'rb') as f:
                while True:
                    chunk = f.read(RECV_BUFFER)
                    if not chunk:
                        break
                    s.sendall(chunk)
            s.close()
            print("Upload finished.")
        except Exception as e:
            print("uploadFile error:", e)

    def downloadFile(self, filename):
        print("Downloading file", filename)
        fileID = getHash(filename)
        recv = self.getSuccessor(self.successor, fileID)
        if not recv:
            print("Could not locate responsible node for file.")
            return
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.settimeout(5)
            s.connect(recv)
            s.sendall(pickle.dumps([1, 0, filename]))
            resp = recv_all(s)
            if resp == b"NotFound" or resp is None:
                print("File not found:", filename)
                s.close()
                return
            else:
                self.receiveFile(s, filename)
                s.close()
                print("Download completed.")
        except Exception as e:
            print("downloadFile error:", e)

# -------------------- Maintenance loops --------------------
    def _stabilize_loop(self):
        while not self._stop:
            try:
                self.stabilize()
            except Exception:
                pass
            time.sleep(STABILIZE_INTERVAL)

    def _fix_fingers_loop(self):
        while not self._stop:
            try:
                self.fix_fingers_incremental()
            except Exception:
                pass
            time.sleep(FIX_FINGERS_INTERVAL)

    def start_background(self):
        threading.Thread(target=self._stabilize_loop, daemon=True).start()
        threading.Thread(target=self._fix_fingers_loop, daemon=True).start()
        threading.Thread(target=self.pingSucc_loop, daemon=True).start()

# -------------------- Ping-based fail detection using successor_list --------------------
    def pingSucc_loop(self):
        while not self._stop:
            time.sleep(PING_INTERVAL)
            if self.successor == self.address:
                continue
            try:
                resp = rpc_send(self.successor, [2])  # ping -> returns predecessor
                if resp is None:
                    # failover
                    print("\nDetected offline successor; attempting failover...")
                    self._failover_successor()
                    # inform new successor to update its pred
                    if self.successor and self.successor != self.address:
                        rpc_send(self.successor, [4, 0, self.address])
                        self.updateFTable()
                        self.updateOtherFTables()
            except Exception:
                continue

# -------------------- Helpers --------------------
    def _is_between(self, a, x, b):
        """Return True if x in (a, b] in modular ring."""
        if a < b:
            return a < x <= b
        else:
            # wrap-around
            return x > a or x <= b

    def print_menu(self):
        print("\n1. Join Network\n2. Leave Network\n3. Upload File\n4. Download File")
        print("5. Print Finger Table\n6. Print my predecessor and successor\n")

    def printFTable(self):
        print("Printing F Table")
        for key, value in self.finger_table.items():
            print("StartID:", key, " -> ", value)

    def stop(self):
        self._stop = True
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
                    node.uploadFile(fname, recv, True)
                else:
                    print("Could not find successor to upload.")
            elif cmd == "4":
                fname = input("Filename to download: ").strip()
                node.downloadFile(fname)
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