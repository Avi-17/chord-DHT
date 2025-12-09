import sys
import time
import threading
import os

from dht_utils import M, get_sha1_hash, is_between, SecurityUtils
from filemanager import FileManager
from networking import NetworkManager

class Node:
    def __init__(self, ip, port, known_node_ip=None, known_node_port=None):
        self.ip = ip
        self.port = int(port)
        self.address = (self.ip, self.port)
        
        self.id = get_sha1_hash(f"{self.ip}:{self.port}")
        
        self.finger_table = {} 
        self.predecessor = None
        self.successor = (self.id, self.address)
        
        self.fm = FileManager(base_directory=f"node_storage_{self.id}")
        self.net = NetworkManager(self.ip, self.port, self.process_request)
        
        self.running = True
        
        print(f"[INIT] Node {self.id} started at {self.address}")
        
        # Start Server
        self.net.start_listener()

        if known_node_ip and known_node_port:
            self.join(known_node_ip, int(known_node_port))
        else:
            print(f"[NETWORK] Created new Chord Ring.")
        
        #Maintenance Threads
        threading.Thread(target=self.stabilize_loop, daemon=True).start()
        threading.Thread(target=self.fix_fingers_loop, daemon=True).start()
        threading.Thread(target=self.check_predecessor_loop, daemon=True).start()
        
        self.user_interface()

    def process_request(self, request):
        cmd = request.get('cmd')
        response = {'status': 'ERROR', 'msg': 'Unknown Command'}

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
        
        elif cmd == 'STORE_FILE':
            self.handle_store_file(request)
            response = {'status': 'OK', 'msg': 'File stored'}
        
        elif cmd == 'RETRIEVE_FILE':
            response = self.handle_retrieve_file(request)
            
        elif cmd == 'MIGRATE_DATA':
            self.handle_migration(request['data'])
            response = {'status': 'OK'}

        return response

    
    # DHT LOGIC
    def join(self, ip, port):
        print(f"[DHT] Joining network via {ip}:{port}...")
        known_address = (ip, port)
        response = self.net.send_request(known_address, {'cmd': 'FIND_SUCCESSOR', 'id': self.id})
        
        if response and response['status'] == 'OK':
            self.successor = response['node']
            print(f"[DHT] Join Successful. Successor is Node {self.successor[0]}")
        else:
            print("[DHT] Failed to find successor. Running as standalone.")

    def find_successor(self, id):
        if is_between(id, self.id, self.successor[0], inclusive_end=True):
            return self.successor
        else:
            n0 = self.closest_preceding_node(id)
            if n0[0] == self.id:
                return self.successor
            
            response = self.net.send_request(n0[1], {'cmd': 'FIND_SUCCESSOR', 'id': id})
            if response and response['status'] == 'OK':
                return response['node']
            return self.successor

    def closest_preceding_node(self, id):
        for i in range(M - 1, -1, -1):
            if i in self.finger_table:
                node = self.finger_table[i]
                if node and is_between(node[0], self.id, id):
                    return node
        return (self.id, self.address)

    def stabilize_loop(self):
        while self.running:
            try:
                response = self.net.send_request(self.successor[1], {'cmd': 'GET_PREDECESSOR'})
                
                if response is None:
                    print(f"[DETECTED] Successor {self.successor[0]} failed.")
                    if self.successor[0] != self.id:
                         self.successor = (self.id, self.address)
                else:
                    x = response['node']
                    if x and is_between(x[0], self.id, self.successor[0]):
                        self.successor = x
                    
                    self.net.send_request(self.successor[1], {'cmd': 'NOTIFY', 'node': (self.id, self.address)})
            except Exception:
                pass
            time.sleep(3)

    def notify(self, node):
        if self.predecessor is None or is_between(node[0], self.predecessor[0], self.id):
            self.predecessor = node

    def fix_fingers_loop(self):
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

    def check_predecessor_loop(self):
        while self.running:
            if self.predecessor:
                if not self.net.send_request(self.predecessor[1], {'cmd': 'PING'}):
                    self.predecessor = None
            time.sleep(3)

    
    # FILE OPERATIONS
    def handle_store_file(self, request):
        filename = request['filename']
        enc_data = request['data']
        
        # Store Data
        self.fm.write_bytes(filename, enc_data)
        
        # Store Metadata
        meta = {"Owner": request['owner'], "Hash": request['hash'], "Size": len(enc_data)}
        self.fm.write_json(f"{filename}.meta", meta)
        
        print(f"[FILE] Stored file '{filename}'")

    def handle_retrieve_file(self, request):
        filename = request['filename']
        data = self.fm.read_bytes(filename)
        
        if data:
            meta = self.fm.read_json(f"{filename}.meta")
            return {'status': 'OK', 'data': data, 'hash': meta.get("Hash", "UNKNOWN")}
        return {'status': 'ERROR', 'msg': 'File not found'}

    def handle_migration(self, incoming_data_dict):
        for filename, file_data in incoming_data_dict.items():
            self.fm.write_bytes(filename, file_data)
        print(f"[MIGRATION] Received {len(incoming_data_dict)} files.")

  
    #UPLOAD DOWNLOAD
    def upload_file(self, filepath):
        if not os.path.exists(filepath): return print("File not found.")
        
        filename = os.path.basename(filepath)
        key_id = get_sha1_hash(filename)
        
        with open(filepath, "rb") as f: raw_data = f.read()
            
        file_hash = SecurityUtils.calculate_file_hash(raw_data)
        encrypted_data = SecurityUtils.encrypt_data(raw_data)
        
        print(f"[UPLOAD] Hashed to {key_id}. Locating host...")
        target_node = self.find_successor(key_id)
        
        payload = {
            'cmd': 'STORE_FILE', 'key_id': key_id, 'filename': filename,
            'data': encrypted_data, 'hash': file_hash, 'owner': self.id
        }
        
        if self.net.send_request(target_node[1], payload):
            print("[UPLOAD] Success.")
        else:
            print("[UPLOAD] Failed.")

    def download_file(self, filename):
        key_id = get_sha1_hash(filename)
        print(f"[DOWNLOAD] Locating host for {key_id}...")
        target_node = self.find_successor(key_id)
        
        resp = self.net.send_request(target_node[1], {'cmd': 'RETRIEVE_FILE', 'filename': filename})
        
        if resp and resp['status'] == 'OK':
            dec_data = SecurityUtils.decrypt_data(resp['data'])
            if SecurityUtils.calculate_file_hash(dec_data) == resp['hash']:
                with open(f"downloaded_{filename}", "wb") as f: f.write(dec_data)
                print(f"[DOWNLOAD] Success.")
            else:
                print("[SECURITY] Hash mismatch!")
        else:
            print("[DOWNLOAD] Failed.")

    # UI
    def user_interface(self):
        print(f"\nNode {self.id} Ready. Commands: upload <file>, download <file>, info, exit")
        while self.running:
            try:
                cmd_input = input(f"\nNode-{self.id} > ").strip().split()
                if not cmd_input: continue
                cmd = cmd_input[0].lower()
                
                if cmd == 'upload' and len(cmd_input) > 1: self.upload_file(cmd_input[1])
                elif cmd == 'download' and len(cmd_input) > 1: self.download_file(cmd_input[1])
                elif cmd == 'info':
                    print(f"ID: {self.id} | Succ: {self.successor[0]} | Pred: {self.predecessor[0] if self.predecessor else 'None'}")
                    print(f"Files: {self.fm.list_files()}")
                elif cmd == 'exit':
                    self.running = False
                    self.net.stop()
                    sys.exit(0)
            except Exception as e: print(f"Error: {e}")

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: python Node.py <MY_IP> <MY_PORT> [KNOWN_IP] [KNOWN_PORT]")
        sys.exit(1)
    
    known_ip = sys.argv[3] if len(sys.argv) > 4 else None
    known_port = sys.argv[4] if len(sys.argv) > 4 else None
    
    Node(sys.argv[1], sys.argv[2], known_ip, known_port)