import hashlib

M = 10
BUFFER_SIZE = 4096

def get_sha1_hash(key_str):
    res = hashlib.sha1(key_str.encode())
    return int(res.hexdigest(), 16) % (2**M)

def is_between(id, start, end, inclusive_end=False):
    if start < end:
        return (start < id <= end) if inclusive_end else (start < id < end)
    else:
        return (start < id) or (id <= end) if inclusive_end else (start < id) or (id < end)


class SecurityUtils:

    #check file integrity
    @staticmethod
    def calculate_file_hash(data_bytes):
        sha256_hash = hashlib.sha256()
        sha256_hash.update(data_bytes)
        return sha256_hash.hexdigest()
    
    #E2EE encryption
    @staticmethod
    def encrypt_data(data_bytes, key="secret_key"):
        key_bytes = key.encode()
        encrypted = bytearray()
        for i, byte in enumerate(data_bytes):
            encrypted.append(byte ^ key_bytes[i % len(key_bytes)])
        return encrypted

    @staticmethod
    def decrypt_data(data_bytes, key="secret_key"):
        return SecurityUtils.encrypt_data(data_bytes, key)
    