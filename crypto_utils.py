from Crypto.Cipher import AES
from Crypto.Util.py3compat import bchr, bord

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
    result = ''
    for c in b:
        result += chr((c >> 4) + 97)
        result += chr((c & 0x0f) + 97)

    return result

def astr2bytes(s: str) -> bytes:
    result = b''
    for i in range(0, len(s), 2):
        result += (((ord(s[i]) - 97) << 4) + ord(s[i+1]) - 97).to_bytes(1, 'big')

    return result

