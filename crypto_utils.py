from Crypto.Cipher import AES
from Crypto.Util.py3compat import bchr, bord
import functools

def aes_enc(key, raw, iv):
    padding_len = 16 - len(raw) % 16
    padding = bchr(padding_len) * padding_len
    cipher = AES.new(key,AES.MODE_CBC,iv)
    return cipher.encrypt(raw + padding)

def aes_dec(key, ctext, iv):
    cipher = AES.new(key,AES.MODE_CBC,iv)
    padded_result = cipher.decrypt(ctext)
    padding_len = bord(padded_result[-1])
    return padded_result[:-padding_len]

def bytes2astr(b: bytes) -> str:
    return functools.reduce(lambda a, b: chr(a >> 4 + 97) + chr(a & 0x0f + 97) + chr(b >> 4 + 97) + chr(b & 0x0f + 97), b)

