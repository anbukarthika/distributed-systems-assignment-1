import json
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa, padding
from cryptography.hazmat.backends import default_backend

def generate_keypair():
    private_key = rsa.generate_private_key(
        public_exponent=65537,
        key_size=2048,
        backend=default_backend()
    )
    public_key = private_key.public_key()
    return private_key, public_key

def serialize_public_key(public_key):
    return public_key.public_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PublicFormat.SubjectPublicKeyInfo
    ).decode('utf-8')

def deserialize_public_key(pem_data):
    return serialization.load_pem_public_key(pem_data.encode('utf-8'))

def sign(message: bytes, private_key) -> bytes:
    return private_key.sign(
        message,
        padding.PSS(
            mgf=padding.MGF1(hashes.SHA256()),
            salt_length=padding.PSS.MAX_LENGTH
        ),
        hashes.SHA256()
    )

def verify(message: bytes, signature: bytes, public_key) -> bool:
    try:
        public_key.verify(
            signature,
            message,
            padding.PSS(
                mgf=padding.MGF1(hashes.SHA256()),
                salt_length=padding.PSS.MAX_LENGTH
            ),
            hashes.SHA256()
        )
        return True
    except Exception:
        return False

def load_keys_from_json(filepath):
    with open(filepath, 'r') as f:
        data = json.load(f)
    priv = {}
    pub = {}
    for node_id, keys in data.items():
        priv[int(node_id)] = serialization.load_pem_private_key(
            keys['private'].encode(), password=None, backend=default_backend()
        )
        pub[int(node_id)] = deserialize_public_key(keys['public'])
    return priv, pub

def save_keys_to_json(filepath, private_keys, public_keys):
    data = {}
    for node_id in private_keys:
        data[node_id] = {
            'private': private_keys[node_id].private_bytes(
                encoding=serialization.Encoding.PEM,
                format=serialization.PrivateFormat.PKCS8,
                encryption_algorithm=serialization.NoEncryption()
            ).decode('utf-8'),
            'public': serialize_public_key(public_keys[node_id])
        }
    with open(filepath, 'w') as f:
        json.dump(data, f, indent=2)