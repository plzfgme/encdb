from Crypto.Cipher import AES

def enc_doc(key, raw, iv):
    cipher = AES.new(key,AES.MODE_CBC,iv)
    return cipher.encrypt(raw)

def dec_doc(key, ctext, iv):
    cipher = AES.new(key,AES.MODE_CBC,iv)
    return cipher.decrypt(ctext)
