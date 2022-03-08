from Crypto.Cipher import AES
from Crypto.Hash import SHA256

# def _pad_str(s, bs=32):
    # return s + (bs - len(s) % bs) * chr(bs - len(s) % bs)

# def _unpad_str(s):
    # return s[:-ord(s[len(s)-1:])]
    
def primitive_hash_h(msg):
    hash = SHA256.new() 
    hash.update(msg)
    hash_msg = hash.digest()
    return hash_msg

def primitive_hash_h_1(msg):
    return primitive_hash_h(msg + b'1')

def primitive_hash_h_2(msg):
    return primitive_hash_h(msg + b'2')
    
def pseudo_permutation_P(key, raw, iv):
    cipher = AES.new(key,AES.MODE_CBC,iv) #raw must be multiple of 16
    return cipher.encrypt(raw)
    
def pseudo_inverse_permutation_P(key, ctext,iv):
    cipher = AES.new(key,AES.MODE_CBC,iv)
    return cipher.decrypt(ctext)   
    
def pseudo_function_F(key, raw, iv):
    cipher = AES.new(key,AES.MODE_CBC,iv)
    return cipher.encrypt(raw)
    
def pseudo_inverse_function_F(key, ctext, iv):
    cipher = AES.new(key,AES.MODE_CBC,iv)
    return cipher.decrypt(ctext)
