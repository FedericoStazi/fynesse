from .config import *

"""These are the types of import we might expect in this file
import httplib2
import oauth2
import mongodb
import sqlite"""

import yaml
from ipywidgets import interact_manual, Text, Password
import pymysql
from tables import pp_data_table
from tables import postcode_data_table

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
        credentials_filename = "credentials.yaml";
        with open(credentials_filename, "w") as credentials_file:
            credentials_dict = {'username': username,
                                'password': password}
            yaml.dump(credentials_dict, credentials_file)
            print(f"Credentials saved in {credentials_filename}.\n"
                  f"Username: {username}\n"
                  f"Password: {'*' * len(password)}")

    interact_manual(write_credentials,
                    username=Text(description="Username:"),
                    password=Password(description="Password:"))


class Connection:
    """ A database connection to the MariaDB database
            specified by the host url and database name.
        :param username: username
        :param password: password
        :param host: host url
        :param port: port number
    """

    def __init__(self, *, username, password, host, port):
        self.username = username
        self.password = password
        self.host = host
        self.port = port
        self.connection = None

        try:
            self.connection = pymysql.connect(
                user=username,
                passwd=password,
                host=host,
                port=port,
                local_infile=1,
                db=None,
                client_flag=pymysql.constants.CLIENT.MULTI_STATEMENTS
            )

            print("Connected successfully")

        except Exception as e:
            print(f"Error connecting to the MariaDB Server: {e}")

    def create_database(self, *, database):
        try:
            cursor = self.connection.cursor()
            cursor.execute(f"""
                SET sql_mode = 'NO_AUTO_VALUE_ON_ZERO';
                SET time_zone = '+00:00';
                CREATE DATABASE IF NOT EXISTS `{database}` 
                    DEFAULT CHARACTER SET utf8 COLLATE utf8_bin;
                USE `{database}`;
            """)

            print(f"Database {database} created.")

        except Exception as e:
            print(f"Error creating the database: {e}")

    def get_cursor(self):
        return self.connection.cursor()


def data():
    """Read the data from the web or local file, returning structured format such as a data frame"""
    raise NotImplementedError
