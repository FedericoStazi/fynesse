from .config import *

"""These are the types of import we might expect in this file
import httplib2
import oauth2
import mongodb
import sqlite"""

import yaml
from ipywidgets import interact_manual, Text, Password

# This file accesses the data

"""
Place commands in this file to access the data electronically. 
Don't remove any missing values, or deal with outliers. 
Make sure you have legalities correct, both intellectual property and personal data privacy rights. 
Beyond the legal side also think about the ethical issues around this data. 
"""


def credentials_interact():
    """Create an interactive prompt for the sql username and password"""
    def write_credentials(username, password):
        with open("credentials.yaml", "w") as credentials_file:
            credentials_dict = {'username': username,
                                'password': password}
            yaml.dump(credentials_dict, credentials_file)
            print(f"Credentials saved.\nUsername: {username}\nPassword: {'*' * len(password)}")

    interact_manual(write_credentials,
                    username=Text(description="Username:"),
                    password=Password(description="Password:"))


def data():
    """Read the data from the web or local file, returning structured format such as a data frame"""
    raise NotImplementedError
