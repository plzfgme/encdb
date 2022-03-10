from Crypto.Cipher import AES
from Crypto.Util.py3compat import bchr, bord

def enc_doc(key, raw, iv):
    padding_len = 16 - len(raw) % 16
    padding = bchr(padding_len) * padding_len
    cipher = AES.new(key,AES.MODE_CBC,iv)
    return cipher.encrypt(raw + padding)

def dec_doc(key, ctext, iv):
    cipher = AES.new(key,AES.MODE_CBC,iv)
    padded_result = cipher.decrypt(ctext)
    padding_len = bord(padded_result[-1])
    return padded_result[:-padding_len]
