import pickle
from pathlib import Path
import streamlit_authenticator as stauth

hashed_passwords = stauth.Hasher(['hannah_vicreturn_hqr23!']).generate()
print(hashed_passwords)
