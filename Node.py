import socket
import threading
import pickle
import hashlib
import os
import sys
import time
import random
import base64

# ==============================================================================
# CONFIGURATION & CONSTANTS
# ==============================================================================
M = 10  # Size of the Chord ring identifier space (2^10 = 1024 IDs)
BUFFER_SIZE = 4096

def get_sha1_hash(key_str):
    """
    Helper: Generates a SHA-1 hash of the input string and maps it 
    to the ID space 2^M.
    """
    result = hashlib.sha1(key_str.encode())
    return int(result.hexdigest(), 16) % (2**M)

# ==============================================================================
# MODULE 1: SECURITY LAYER
# Implements File Integrity (Hashing) and End-to-End Encryption (Placeholder/Basic)
# ==============================================================================
class SecurityUtils:
    
    # --- Functionality: File Integrity Check ---
    @staticmethod
    def calculate_file_hash(data_bytes):
        """
        Calculates SHA-256 hash of file data for integrity verification.
        """
        sha256_hash = hashlib.sha256()
        sha256_hash.update(data_bytes)
        return sha256_hash.hexdigest()

    # --- Functionality: End-to-End Encryption (E2EE) ---
    @staticmethod
    def encrypt_data(data_bytes, key="secret_key"):
        """
        Basic XOR encryption for demonstration. 
        In production, use libraries like 'cryptography.fernet'.
        """
        key_bytes = key.encode()
        encrypted = bytearray()
        for i, byte in enumerate(data_bytes):
            encrypted.append(byte ^ key_bytes[i % len(key_bytes)])
        return bytes(encrypted)

    @staticmethod
    def decrypt_data(data_bytes, key="secret_key"):
        """
        Decryption logic (Symmetric XOR).
        """
        return SecurityUtils.encrypt_data(data_bytes, key)

# ==============================================================================
# MAIN CLASS: CHORD NODE
# ==============================================================================
class Node:
    def __init__(self, ip, port, known_node_ip=None, known_node_port=None):
        self.ip = ip
        self.port = int(port)
        self.address = (self.ip, self.port)
        
        # ID generation based on Port for simplicity in local testing
        # In real scenarios, use IP:Port string
        self.id = get_sha1_hash(f"{self.ip}:{self.port}")
        
        # Chord State
        self.finger_table = {}  # Map: i -> (node_id, address)
        self.predecessor = None
        self.successor = (self.id, self.address) # (id, (ip, port))
        
        # File Storage (Simulating Disk)
        self.storage_path = f"node_storage_{self.id}"
        if not os.path.exists(self.storage_path):
            os.makedirs(self.storage_path)

        # Threading Flags
        self.running = True
        self.mutex = threading.Lock()

        # --- Functionality: Networking Initialization ---
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server_socket.bind(self.address)
        self.server_socket.listen(10)
        
        print(f"[INIT] Node {self.id} started at {self.address}")

        # Join the network if a known node is provided
        if known_node_ip and known_node_port:
            self.join(known_node_ip, int(known_node_port))
        else:
            # First node in the ring
            print(f"[NETWORK] Created new Chord Ring.")
        
        # Start Threads
        threading.Thread(target=self.start_server, daemon=True).start()
        threading.Thread(target=self.stabilize_loop, daemon=True).start()
        threading.Thread(target=self.fix_fingers_loop, daemon=True).start()
        threading.Thread(target=self.check_predecessor_loop, daemon=True).start()
        
        # Start User Interface
        self.user_interface()

    # ==============================================================================
    # MODULE 2: NETWORKING LAYER
    # Handles low-level socket operations and object serialization
    # ==============================================================================
    
    def start_server(self):
        """
        Listens for incoming TCP connections from other nodes or clients.
        """
        while self.running:
            try:
                conn, addr = self.server_socket.accept()
                threading.Thread(target=self.handle_connection, args=(conn, addr)).start()
            except Exception as e:
                print(f"[ERROR] Server error: {e}")

    def send_request(self, target_address, message):
        """
        Sends a pickled message to a target node with a STRICT TIMEOUT.
        """
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            # CRITICAL FIX: Add a timeout. 
            # If a node doesn't reply in 3 seconds, assume it's dead.
            sock.settimeout(3) 
            
            sock.connect(target_address)
            sock.sendall(pickle.dumps(message))
            
            response_data = sock.recv(BUFFER_SIZE)
            sock.close()
            
            if response_data:
                return pickle.loads(response_data)
        except Exception as e:
            # Connection refused, timeout, or host unreachable
            return None
        return None

    def handle_connection(self, conn, addr):
        """
        Routes incoming requests to appropriate DHT or File logic.
        """
        try:
            data = conn.recv(BUFFER_SIZE)
            if not data: return
            
            request = pickle.loads(data)
            cmd = request.get('cmd')
            response = {'status': 'ERROR', 'msg': 'Unknown Command'}

            # --- DHT Routing Commands ---
            if cmd == 'FIND_SUCCESSOR':
                result_node = self.find_successor(request['id'])
                response = {'status': 'OK', 'node': result_node}
            
            elif cmd == 'GET_PREDECESSOR':
                response = {'status': 'OK', 'node': self.predecessor}
            
            elif cmd == "PING":
                response = {'status': 'OK'}

            elif cmd == 'NOTIFY':
                self.notify(request['node'])
                response = {'status': 'OK'}
            
            # --- File Management Commands ---
            elif cmd == 'STORE_FILE':
                self.handle_store_file(request)
                response = {'status': 'OK', 'msg': 'File stored'}
            
            elif cmd == 'RETRIEVE_FILE':
                response = self.handle_retrieve_file(request)
                
            elif cmd == 'MIGRATE_DATA':
                self.handle_migration(request['data'])
                response = {'status': 'OK'}

            conn.sendall(pickle.dumps(response))
        except Exception as e:
            print(f"[ERROR] Handling connection: {e}")
        finally:
            conn.close()

    # ==============================================================================
    # MODULE 3: DHT PROTOCOL (CHORD LOGIC)
    # Core logic for routing, joining, and stabilization
    # ==============================================================================

    def join(self, ip, port):
        """
        Functionality: Node Join
        Bootstrap by asking a known node to find my successor.
        """
        print(f"[DHT] Joining network via {ip}:{port}...")
        known_address = (ip, port)
        response = self.send_request(known_address, {'cmd': 'FIND_SUCCESSOR', 'id': self.id})
        
        if response and response['status'] == 'OK':
            self.successor = response['node']
            print(f"[DHT] Join Successful. Successor is Node {self.successor[0]}")
            
            # Functionality: Data Migration on Join
            # Ask successor for keys that now belong to me
            # (Simplified implementation triggers in stabilize/notify usually, 
            # but explicit request can be added here)
        else:
            print("[DHT] Failed to find successor. Running as standalone.")

    def find_successor(self, id):
        """
        Functionality: File Lookup / Routing
        Finds the node responsible for a specific ID.
        """
        if self.is_between(id, self.id, self.successor[0], inclusive_end=True):
            return self.successor
        else:
            n0 = self.closest_preceding_node(id)
            if n0[0] == self.id:
                return self.successor
            
            # Recursive / Forwarding call
            response = self.send_request(n0[1], {'cmd': 'FIND_SUCCESSOR', 'id': id})
            if response and response['status'] == 'OK':
                return response['node']
            return self.successor # Fallback
    
    def check_predecessor_loop(self):
        """
        Periodically checks if predecessor is alive.
        """
        while self.running:
            try:
                if self.predecessor:
                    resp = self.send_request(self.predecessor[1], {'cmd': 'PING'})
                    if resp is None:
                        print(f"[DETECTED] Predecessor {self.predecessor[0]} failed. Clearing.")
                        self.predecessor = None
            except Exception:
                pass
            time.sleep(3)

    def closest_preceding_node(self, id):
        """
        Scans finger table (from furthest to nearest) to find best next hop.
        """
        for i in range(M - 1, -1, -1):
            if i in self.finger_table:
                node = self.finger_table[i]
                if node and self.is_between(node[0], self.id, id):
                    return node
        return (self.id, self.address)

    def is_between(self, id, start, end, inclusive_end=False):
        """
        Helper for circular ID space logic.
        """
        if start < end:
            return (start < id <= end) if inclusive_end else (start < id < end)
        else: # Wrapping around 0
            return (start < id) or (id <= end) if inclusive_end else (start < id) or (id < end)

    def stabilize_loop(self):
        """
        Final Robust Stabilization: Handles 1-node, 2-node, and N-node failures.
        """
        while self.running:
            try:
                # 1. PING SUCCESSOR
                # If I am my own successor, I don't need to ping myself (optimization),
                # but I DO need to check for new nodes (GET_PREDECESSOR).
                response = self.send_request(self.successor[1], {'cmd': 'GET_PREDECESSOR'})
                
                if response is None:
                    print(f"[DETECTED] Successor {self.successor[0]} failed. Initiating repair...")
                    
                    # --- FAILOVER LOGIC ---
                    found_backup = False
                    
                    # Get all unique nodes from finger table, excluding myself
                    # Use a set to remove duplicates, then convert to list
                    candidates = []
                    for idx in self.finger_table:
                        node = self.finger_table[idx]
                        # Only add if it's not me and not the dead successor
                        if node[0] != self.id and node[0] != self.successor[0]:
                             candidates.append(node)
                    
                    # Sort candidates? (Optional, but simple is better here)
                    # We just need ONE alive node.
                    
                    for cand in candidates:
                        print(f"[REPAIR] Trying candidate {cand[0]}...")
                        if self.send_request(cand[1], {'cmd': 'PING'}):
                            print(f"[REPAIR] Success! New successor is {cand[0]}")
                            self.successor = cand
                            found_backup = True
                            break
                    
                    if not found_backup:
                        # If we are here, Successor is dead, and Finger Table is either empty
                        # or all dead. We are the last node.
                        if self.successor[0] != self.id:
                            print(f"[REPAIR] All known nodes dead. Reverting to Self-Loop (ID: {self.id}).")
                            self.successor = (self.id, self.address)
                            # Also clear finger table to prevent retrying dead nodes immediately
                            self.finger_table = {} 
                
                else:
                    # --- STANDARD CHORD LOGIC (Successor is Alive) ---
                    x = response['node']
                    if x:
                        # If successor is self, take x.
                        if self.successor[0] == self.id:
                            self.successor = x
                        # If x is strictly between me and my successor
                        elif self.is_between(x[0], self.id, self.successor[0]):
                            self.successor = x
                            
                    # Notify successor
                    self.send_request(self.successor[1], {'cmd': 'NOTIFY', 'node': (self.id, self.address)})
                    
            except Exception as e:
                print(f"[ERROR] In stabilize: {e}")
            
            time.sleep(3)

    def notify(self, node):
        """
        Update predecessor if the notifying node is a better predecessor.
        """
        if self.predecessor is None or self.is_between(node[0], self.predecessor[0], self.id):
            self.predecessor = node
            # Trigger migration check here (simplified) logic usually goes here

    def fix_fingers_loop(self):
        """
        Functionality: Optimized Finger Table Repair
        Periodically updates random finger table entries.
        """
        i = 0
        while self.running:
            try:
                i = (i + 1) % M
                target_id = (self.id + 2**i) % (2**M)
                node = self.find_successor(target_id)
                if node:
                    self.finger_table[i] = node
            except Exception:
                pass
            time.sleep(1)

    # ==============================================================================
    # MODULE 4: FILE MANAGER
    # Handles storage, retrieval, migration, integrity, and encryption
    # ==============================================================================

    def handle_store_file(self, request):
        """
        Saves encrypted file data to local storage.
        """
        filename = request['filename']
        enc_data = request['data']
        file_hash = request['hash']
        
        # Verify integrity before storing (if we were verifying on receipt)
        # Note: We store encrypted data directly.
        
        path = os.path.join(self.storage_path, filename)
        
        # Metadata storage (simple text file sidecar)
        meta_path = path + ".meta"
        with open(meta_path, "w") as f:
            f.write(f"Owner: {request['owner']}\nHash: {file_hash}\nSize: {len(enc_data)}")

        with open(path, "wb") as f:
            f.write(enc_data)
        
        print(f"[FILE] Stored file '{filename}' (ID: {request['key_id']})")

    def handle_retrieve_file(self, request):
        """
        Reads file from disk and returns it.
        """
        filename = request['filename']
        path = os.path.join(self.storage_path, filename)
        
        if os.path.exists(path):
            with open(path, "rb") as f:
                data = f.read()
            
            # Read metadata to get original hash
            meta_hash = "UNKNOWN"
            if os.path.exists(path + ".meta"):
                with open(path + ".meta", "r") as f:
                    for line in f:
                        if line.startswith("Hash:"):
                            meta_hash = line.split(":")[1].strip()

            return {'status': 'OK', 'data': data, 'hash': meta_hash}
        else:
            return {'status': 'ERROR', 'msg': 'File not found'}

    def handle_migration(self, incoming_data_dict):
        """
        Functionality: Data Migration on Join
        Receive bulk files from a node (usually on Join).
        """
        for filename, file_data in incoming_data_dict.items():
            path = os.path.join(self.storage_path, filename)
            with open(path, "wb") as f:
                f.write(file_data)
        print(f"[MIGRATION] Received {len(incoming_data_dict)} files.")

    def upload_file(self, filepath):
        """
        Client Action: Upload a file.
        1. Integrity Hash -> 2. Encrypt -> 3. Locate Node -> 4. Send
        """
        if not os.path.exists(filepath):
            print("File does not exist.")
            return

        filename = os.path.basename(filepath)
        key_id = get_sha1_hash(filename)
        
        # 1. Integrity Check
        with open(filepath, "rb") as f:
            raw_data = f.read()
        file_hash = SecurityUtils.calculate_file_hash(raw_data)
        
        # 2. End-to-End Encryption
        encrypted_data = SecurityUtils.encrypt_data(raw_data)
        
        # 3. Locate Node
        print(f"[UPLOAD] Hashed '{filename}' to ID {key_id}. Locating host...")
        target_node = self.find_successor(key_id)
        
        # 4. Send
        payload = {
            'cmd': 'STORE_FILE',
            'key_id': key_id,
            'filename': filename,
            'data': encrypted_data,
            'hash': file_hash,
            'owner': self.id
        }
        
        print(f"[UPLOAD] Sending to Node {target_node[0]} at {target_node[1]}...")
        response = self.send_request(target_node[1], payload)
        if response and response['status'] == 'OK':
            print("[UPLOAD] Success.")
        else:
            print("[UPLOAD] Failed.")

    def download_file(self, filename):
        """
        Client Action: Download a file.
        1. Locate Node -> 2. Fetch -> 3. Decrypt -> 4. Verify Integrity
        """
        key_id = get_sha1_hash(filename)
        
        # 1. Locate Node
        print(f"[DOWNLOAD] Locating host for '{filename}' (ID {key_id})...")
        target_node = self.find_successor(key_id)
        
        # 2. Fetch
        payload = {'cmd': 'RETRIEVE_FILE', 'filename': filename}
        response = self.send_request(target_node[1], payload)
        
        if response and response['status'] == 'OK':
            enc_data = response['data']
            original_hash = response['hash']
            
            # 3. Decrypt
            dec_data = SecurityUtils.decrypt_data(enc_data)
            
            # 4. Verify Integrity
            current_hash = SecurityUtils.calculate_file_hash(dec_data)
            
            if current_hash == original_hash:
                save_name = f"downloaded_{self.id}_{filename}"
                with open(save_name, "wb") as f:
                    f.write(dec_data)
                print(f"[DOWNLOAD] Success. Integrity Verified. Saved as '{save_name}'")
            else:
                print(f"[SECURITY WARNING] Hash mismatch! File may be corrupted or tampered.")
                print(f"Expected: {original_hash}")
                print(f"Calculated: {current_hash}")
        else:
            print("[DOWNLOAD] File not found or Node unreachable.")

    # ==============================================================================
    # MODULE 5: APP INTERFACE
    # CLI Menu for user interaction
    # ==============================================================================
    
    def user_interface(self):
        print("\n" + "="*40)
        print(f" SECURE CHORD NODE {self.id} ")
        print("="*40)
        print("Commands: upload <file> | download <file> | info | exit")
        
        while self.running:
            try:
                cmd_input = input(f"\nNode-{self.id} > ").strip().split()
                if not cmd_input: continue
                
                cmd = cmd_input[0].lower()
                
                if cmd == 'upload' and len(cmd_input) > 1:
                    self.upload_file(cmd_input[1])
                
                elif cmd == 'download' and len(cmd_input) > 1:
                    self.download_file(cmd_input[1])
                
                elif cmd == 'info':
                    print(f"ID: {self.id}")
                    print(f"Successor: {self.successor[0]}")
                    print(f"Predecessor: {self.predecessor[0] if self.predecessor else 'None'}")
                    print(f"Finger Table: {[f'{k}:{v[0]}' for k,v in self.finger_table.items()]}")
                    files = os.listdir(self.storage_path)
                    print(f"Stored Files: {files}")
                
                elif cmd == 'exit':
                    self.running = False
                    self.server_socket.close()
                    sys.exit(0)
                
                else:
                    print("Invalid command.")
            
            except KeyboardInterrupt:
                self.running = False
                sys.exit(0)
            except Exception as e:
                print(f"UI Error: {e}")

# ==============================================================================
# ENTRY POINT
# ==============================================================================
if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: python Node.py <MY_IP> <MY_PORT> [KNOWN_IP] [KNOWN_PORT]")
        print("Example: python Node.py 192.168.1.5 5000")
        sys.exit(1)
        
    my_ip = sys.argv[1]       # ARG 1: My own LAN IP
    my_port = sys.argv[2]     # ARG 2: My own Port
    
    known_ip = None
    known_port = None
    
    if len(sys.argv) > 4:
        known_ip = sys.argv[3] # ARG 3: Known Node IP
        known_port = sys.argv[4] # ARG 4: Known Node Port
        
    node = Node(my_ip, my_port, known_ip, known_port)