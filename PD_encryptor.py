import base64

def decrypt_pd(en_pd):
    decoded_bytes = base64.b64decode(en_pd.encode('utf-8'))
    decrypted_pd = decoded_bytes.decode('utf-8')
    return decrypted_pd