# generate_keys.py
import os
import json
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.backends import default_backend

def generate_keypair():
    private = rsa.generate_private_key(public_exponent=65537, key_size=2048, backend=default_backend())
    public = private.public_key()
    return private, public

def serialize_private_key(private_key):
    return private_key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption()
    ).decode('utf-8')

def serialize_public_key(public_key):
    return public_key.public_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PublicFormat.SubjectPublicKeyInfo
    ).decode('utf-8')

os.makedirs('keys', exist_ok=True)
keys = {}
for i in range(5):
    priv, pub = generate_keypair()
    keys[str(i)] = {
        'private': serialize_private_key(priv),
        'public': serialize_public_key(pub)
    }
with open('keys/keys.json', 'w') as f:
    json.dump(keys, f, indent=2)
print("Keys generated in keys/keys.json")