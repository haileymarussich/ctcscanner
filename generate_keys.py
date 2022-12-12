# -*- coding: utf-8 -*-
"""
Created on Thu Sep 29 11:53:32 2022

@author: haile
"""

import pickle
from pathlib import Path
import streamlit_authenticator as stauth

names = ["Glenn Hay-Roe", "Jesse Parsons", "Hailey Marussich"]
usernames = ["ghayroe", "jparsons", "hmarussich"]
passwords = ["XXX", "XXX", "XXX"]

hashed_passwords = stauth.Hasher(passwords).generate()

file_path = Path(__file__).parent / "hashed_pw.pk1"
with file_path.open("wb") as file:
    pickle.dump(hashed_passwords, file)