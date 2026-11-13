import hashlib
import os

ID_BITS = 160

def generate_node_id() -> int:
    return int.from_bytes(os.urandom(20), "big")

def id_from_key(key) -> int:
   
    if isinstance(key, str):
        key = key.encode()
    return int.from_bytes(hashlib.sha1(key).digest(), "big")

def distance(a: int, b: int) -> int:
    
    return a ^ b

def bucket_index(own_id: int, other_id: int) -> int:
    
    d = own_id ^ other_id
    if d == 0:
        raise ValueError("A node has no bucket for itself")
    return d.bit_length() - 1