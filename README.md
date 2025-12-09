🏆 README: 
Secure Chord DHT Implementation
This repository contains a modularized Python implementation of the Chord Distributed Hash Table (DHT) protocol, designed for secure, peer-to-peer (P2P) file storage and retrieval with strong fault tolerance.

✨ Features
Consistent Hashing: Uses SHA-1 mapping to distribute keys uniformly across the ring (M=10).
Logarithmic Lookups: Achieves O(log N) lookup time via Finger Tables.
Self-Healing: Continuous stabilization and finger-fixing loops ensure the ring remains functional as nodes join and leave
Security (E2EE): Files are hashed (SHA-256) for integrity and encrypted (XOR basic encryption) before being stored.
Modular Architecture: Separation of concerns across four distinct Python files for clarity and testability.

🛠️ Prerequisites:
Python 3.x
All required libraries (socket, threading, hashlib, pickle, pathlib, json, etc.) are included in Python's standard library. No external installation required.

🧩 Project Architecture:
The code is divided into four files based on functionality:
Node.py: Handles the DHT state, orchestrates the stabilization loops, and acts as the central router (process_request).
networking.py: Handles low-level socket operations (send_request, server listening) and serialization/deserialization (pickling).
filemanager.py: Manages disk I/O, pathing (pathlib), and creates the node's storage sandbox. Supports binary (file content) and JSON (metadata) I/O.
dht_utils.py: Contains hashing functions (get_sha1_hash), security routines (SecurityUtils), constants (M, BUFFER_SIZE), and math helpers (is_between).


🚀 Usage (How to Run)
The application requires direct use of your Local Area Network (LAN) IP address. Do not use localhost or 127.0.0.1 when testing between different machines.

1. Start the First Node (Leader)
The first node initiates the Chord ring.
Command Syntax: python3 Node.py <MY_IP> <MY_PORT>

2. Start the Second Node (Joiner)
The second node joins the existing ring by specifying the leader's address.
Command Syntax: python3 Node.py <MY_IP> <MY_PORT> <KNOWN_IP> <KNOWN_PORT>

3. Client Commands
Once nodes are running, interact via the command-line interface:

Command: upload <filename>
Description: Hashes the file, encrypts it, locates the correct successor, and stores it.

Command: download <filename>
Description: Displays Node ID, Successor/Predecessor, Finger Table entries, and locally stored files.

Command: info
Description: Displays Node ID, Successor/Predecessor, Finger Table entries, and locally stored files.


⚠️ Important Note on Security
The SecurityUtils class uses a basic XOR implementation for encryption solely to demonstrate the E2EE architecture. Do not use this code for production or sensitive data. A production system would use a robust library like cryptography.fernet.