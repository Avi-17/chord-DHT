import socket
import pickle
import threading
from dht_utils import BUFFER_SIZE

class NetworkManager:
    def __init__(self, ip, port, message_handler_callback):
        self.ip = ip
        self.port = int(port)
        self.address = (self.ip, self.port)
        self.handler_callback = message_handler_callback # The function in Node.py to call
        self.running = True
        
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)  #IPv4, TCP
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1) # Reuse address 
        self.server_socket.bind(self.address)
        self.server_socket.listen(10)   #10 -> backlog

    def start_listener(self):
        threading.Thread(target=self._accept_loop, daemon=True).start()

    def _accept_loop(self):
        while self.running:
            try:
                conn, addr = self.server_socket.accept()
                threading.Thread(target=self._handle_client, args=(conn, addr)).start()
            except Exception as e:
                if self.running: print(f"[NET] Server error: {e}")

    def _handle_client(self, conn, addr):
        try:
            data = conn.recv(BUFFER_SIZE)
            if not data: return

            request = pickle.loads(data)
            response = self.handler_callback(request)
            conn.sendall(pickle.dumps(response))
        except Exception as e:
            print(f"[NET] Error handling client: {e}")
        finally:
            conn.close()

    def send_request(self, target_address, message, timeout=3):
        """Sends a pickled message to a target."""
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(timeout)
            sock.connect(target_address)
            sock.sendall(pickle.dumps(message))
            
            response_data = sock.recv(BUFFER_SIZE)
            sock.close()
            
            if response_data:
                return pickle.loads(response_data)
        except Exception:
            return None # Timeout or Connection Refused
        return None

    def stop(self):
        self.running = False
        try:
            self.server_socket.close()
        except:
            pass