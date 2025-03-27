from cryptography.fernet import Fernet

fernet = Fernet(b'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx')
# Generate random key
# key = Fernet.generate_key()
# print(key.decode())


def encrypt_text(text: str) -> str:
    """
    Will encrypt text
    """
    return fernet.encrypt(bytes(text, 'utf-8')).decode('utf-8')


#print(encrypt_text('edpfalcon!'))


def decrypt_text(text: str) -> str:
    """
    Will de-crypt text
    """
    return fernet.decrypt(bytes(text, 'utf-8')).decode('utf-8')
