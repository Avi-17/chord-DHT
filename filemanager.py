import os
import socket
import pickle
from typing import Tuple, Any
from networking import recv_all, rpc_send, RECV_BUFFER as NETWORK_RECV_BUFFER
from dht_utils import getHash


RECV_BUFFER = NETWORK_RECV_BUFFER


# -------------------- RPC/handler for incoming transfer --------------------
def transfer_file(node, connection, address, rDataList):
    """
    Handle incoming RPC file transfer request.
    rDataList format: [1, choice, filename]
      - choice: 0 download request, 1 upload+replicate, -1 upload single (no replicate)
    node: Node instance (provides node.filename_list etc.)
    """
    choice = rDataList[1]
    filename = rDataList[2]

    # DOWNLOAD request: remote wants to download from *this* node
    if choice == 0:
        print("Download request for file:", filename)
        try:
            if filename not in node.filename_list:
                try:
                    connection.sendall(b"NotFound")
                except Exception:
                    pass
                return
            else:
                try:
                    connection.sendall(b"Found")
                except Exception:
                    pass
                # send the file bytes
                send_file(connection, filename)
        except ConnectionResetError:
            print("Client disconnected during download")
        return

    # UPLOAD request: remote is sending a file to this node
    elif choice == 1 or choice == -1:
        print("Receiving file:", filename)
        # ensure node.filename_list exists and use it
        if filename not in node.filename_list:
            node.filename_list.append(filename)

        # send ACK to sender
        try:
            connection.sendall(b"OK")
        except Exception:
            pass

        # receive file bytes and save
        try:
            receive_file(connection, filename)
            print("Upload complete:", filename)
        except Exception as e:
            print("Error receiving file:", e)
            # remove partially-written file if any
            try:
                if os.path.exists(filename) and os.path.getsize(filename) == 0:
                    os.remove(filename)
            except Exception:
                pass

        # If this was an initial upload (replicate flag == 1), replicate to our successor
        # (Do not replicate if successor is self or replicate flag indicates no further replication)
        try:
            if choice == 1 and getattr(node, "successor", None) and node.successor != node.address:
                # replicate to successor; set replicate=False to avoid infinite replication
                upload_file(node, filename, node.successor, replicate=False)
        except Exception:
            # best-effort: ignore replication errors
            pass
        return

    else:
        # Unknown request type
        return


# -------------------- Low-level send/receive file bytes --------------------
def send_file(connection: socket.socket, filename: str):
    """Stream file bytes over an already-established socket connection."""
    print("Sending file:", filename)
    try:
        with open(filename, "rb") as f:
            while True:
                chunk = f.read(RECV_BUFFER)
                if not chunk:
                    break
                connection.sendall(chunk)
        print("File sent:", filename)
    except FileNotFoundError:
        print("send_file: file not found:", filename)
    except Exception as e:
        print("sendFile error:", e)


def receive_file(connection: socket.socket, filename: str):
    """Receive bytes from connection and write to filename until peer closes connection."""
    # Avoid overwriting an existing non-empty file
    if os.path.exists(filename) and os.path.getsize(filename) > 0:
        print("File already present locally:", filename)
        return

    # Write to a temp file first, then atomically rename
    tmp_name = filename + ".part"
    try:
        with open(tmp_name, "wb") as f:
            while True:
                data = connection.recv(RECV_BUFFER)
                if not data:
                    break
                f.write(data)
        # move temp -> final
        os.replace(tmp_name, filename)
        print("Saved file:", filename)
    except ConnectionResetError:
        print("Transfer interrupted for:", filename)
        try:
            if os.path.exists(tmp_name):
                os.remove(tmp_name)
        except Exception:
            pass
    except Exception as e:
        print("receive_file error:", e)
        try:
            if os.path.exists(tmp_name):
                os.remove(tmp_name)
        except Exception:
            pass


# -------------------- Client-side upload/download helpers --------------------
def upload_file(node: Any, filename: str, recvIPport: Tuple[str, int], replicate: bool = True):
    """
    Upload a local file to the given recvIPport.
    If replicate=True, the request uses replicate flag (1); otherwise -1.
    """
    print("Uploading file", filename, "to", recvIPport)
    if not os.path.exists(filename):
        print("File not present:", filename)
        return

    s = None
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(5.0)
        s.connect(recvIPport)
        flag = 1 if replicate else -1
        s.sendall(pickle.dumps([1, flag, filename]))
        # wait ack from receiver
        ack = recv_all(s)
        if ack is None:
            # receiver didn't ack; still try sending bytes as best-effort
            pass
        # Now stream file bytes
        with open(filename, "rb") as f:
            while True:
                chunk = f.read(RECV_BUFFER)
                if not chunk:
                    break
                s.sendall(chunk)
        print("Upload finished.")
    except Exception as e:
        print("upload_file error:", e)
    finally:
        if s is not None:
            try:
                s.close()
            except Exception:
                pass


def download_file(node: Any, filename: str):
    """
    Download `filename` from the node responsible for it.
    Uses node.getSuccessor(contact_addr, keyID) to locate responsible node.
    """
    print("Downloading file", filename)
    fileID = getHash(filename)
    # Use node.getSuccessor wrapper (Node should provide it or dht_utils.lookup_successor)
    try:
        contact = getattr(node, "successor", node.address)
        recv = node.getSuccessor(contact, fileID)
    except Exception:
        recv = None

    if not recv:
        print("Could not locate responsible node for file.")
        return

    s = None
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(5.0)
        s.connect(recv)
        s.sendall(pickle.dumps([1, 0, filename]))
        resp = recv_all(s)
        # receiver uses raw bytes "NotFound" or "Found" in our design
        if resp == b"NotFound" or resp is None:
            print("File not found:", filename)
            return
        else:
            # If resp was not a marker but we have a stream, call receive_file on the same socket
            receive_file(s, filename)
            print("Download completed.")
    except Exception as e:
        print("download_file error:", e)
    finally:
        if s is not None:
            try:
                s.close()
            except Exception:
                pass
