from .config import *

"""These are the types of import we might expect in this file
import httplib2
import oauth2
import mongodb
import sqlite"""

import yaml
from ipywidgets import interact_manual, Text, Password
import pymysql
from urllib import request


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


class PPDataTable:
    def __int__(self, connection):
        self.connection = connection

    def create_table(self):
        cur = self.connection.cursor()
        cur.execute(f"""
            DROP TABLE IF EXISTS `pp_data`;
            CREATE TABLE IF NOT EXISTS `pp_data` (
                `transaction_unique_identifier` tinytext COLLATE utf8_bin NOT NULL,
                `price` int(10) unsigned NOT NULL,
                `date_of_transfer` date NOT NULL,
                `postcode` varchar(8) COLLATE utf8_bin NOT NULL,
                `property_type` varchar(1) COLLATE utf8_bin NOT NULL,
                `new_build_flag` varchar(1) COLLATE utf8_bin NOT NULL,
                `tenure_type` varchar(1) COLLATE utf8_bin NOT NULL,
                `primary_addressable_object_name` tinytext COLLATE utf8_bin NOT NULL,
                `secondary_addressable_object_name` tinytext COLLATE utf8_bin NOT NULL,
                `street` tinytext COLLATE utf8_bin NOT NULL,
                `locality` tinytext COLLATE utf8_bin NOT NULL,
                `town_city` tinytext COLLATE utf8_bin NOT NULL,
                `district` tinytext COLLATE utf8_bin NOT NULL,
                `county` tinytext COLLATE utf8_bin NOT NULL,
                `ppd_category_type` varchar(2) COLLATE utf8_bin NOT NULL,
                `record_status` varchar(2) COLLATE utf8_bin NOT NULL,
                `db_id` bigint(20) unsigned NOT NULL
            ) DEFAULT CHARSET=utf8 COLLATE=utf8_bin AUTO_INCREMENT=1;
        """)

        rows = cur.fetchall()
        return rows

    def create_indices(self):
        cur = self.connection.cursor()
        cur.execute(f"""
            ALTER TABLE `pp_data`
            ADD PRIMARY KEY (`db_id`),
            MODIFY `db_id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,AUTO_INCREMENT=1;
            CREATE INDEX `pp.postcode` USING HASH
                ON `pp_data`
                    (postcode);
            CREATE INDEX `pp.date` USING HASH
                ON `pp_data` 
                    (date_of_transfer);
        """)

        rows = cur.fetchall()
        return rows

    def load_data(self, *, start_year=1995, end_year=2021):
        cur = self.connection.cursor()
        for year in range(start_year, end_year + 1):
            for part in range(1, 3):
                print(f"\rLoading year {year} part {part}...", end="")

                filename = f"pp-{year}-part{part}.csv"
                url = f"http://prod.publicdata.landregistry.gov.uk.s3-website-eu-west-1.amazonaws.com/{filename}"
                request.urlretrieve(url, filename)

                cur.execute(f"""
                        LOAD DATA LOCAL INFILE '{filename}' INTO TABLE pp_data
                            FIELDS TERMINATED BY ','
                            LINES STARTING BY '' TERMINATED BY '\\n';
                    """)

                os.remove(filename)

        rows = cur.fetchall()
        return rows


class PostcodeDataTable:
    def __int__(self, connection):
        self.connection = connection

    def create_table(self, connection):
        cur = self.connection.cursor()
        cur.execute(f"""
            DROP TABLE IF EXISTS `postcode_data`;
            CREATE TABLE IF NOT EXISTS `postcode_data` (
            `postcode` varchar(8) COLLATE utf8_bin NOT NULL,
            `status` enum('live','terminated') NOT NULL,
            `usertype` enum('small', 'large') NOT NULL,
            `easting` int unsigned,
            `northing` int unsigned,
            `positional_quality_indicator` int NOT NULL,
            `country` enum('England', 'Wales', 'Scotland', 'Northern Ireland', 'Channel Islands', 'Isle of Man') NOT NULL,
            `lattitude` decimal(11,8) NOT NULL,
            `longitude` decimal(10,8) NOT NULL,
            `postcode_no_space` tinytext COLLATE utf8_bin NOT NULL,
            `postcode_fixed_width_seven` varchar(7) COLLATE utf8_bin NOT NULL,
            `postcode_fixed_width_eight` varchar(8) COLLATE utf8_bin NOT NULL,
            `postcode_area` varchar(2) COLLATE utf8_bin NOT NULL,
            `postcode_district` varchar(4) COLLATE utf8_bin NOT NULL,
            `postcode_sector` varchar(6) COLLATE utf8_bin NOT NULL,
            `outcode` varchar(4) COLLATE utf8_bin NOT NULL,
            `incode` varchar(3)  COLLATE utf8_bin NOT NULL,
            `db_id` bigint(20) unsigned NOT NULL
            ) DEFAULT CHARSET=utf8 COLLATE=utf8_bin;
        """)

        rows = cur.fetchall()
        return rows

    def create_indices(self, connection):
        cur = self.connection.cursor()
        cur.execute(f"""
            ALTER TABLE `postcode_data`
            DROP INDEX IF EXISTS `PRIMARY`,
            DROP INDEX IF EXISTS `po.postcode`,
            ADD PRIMARY KEY (`db_id`),
            MODIFY `db_id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,AUTO_INCREMENT=1;
            CREATE INDEX `po.postcode` USING HASH
                ON `postcode_data`
                    (postcode);
        """)

        rows = cur.fetchall()
        return rows

    def load_data(self, connection):
        filename = "open_postcode_geo.csv.zip"
        url = "https://www.getthedata.com/downloads/open_postcode_geo.csv.zip"
        request.urlretrieve(url, filename)

        cur = self.connection.cursor()
        cur.execute("""
                ALTER TABLE `postcode_data`
                DROP INDEX IF EXISTS `PRIMARY`,
                DROP INDEX IF EXISTS `po.postcode`,
                ADD PRIMARY KEY (`db_id`),
                MODIFY `db_id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,AUTO_INCREMENT=1;
                CREATE INDEX `po.postcode` USING HASH
                    ON `postcode_data`
                        (postcode);
            """)

        os.remove(filename)

        rows = cur.fetchall()
        return rows


def data():
    """Read the data from the web or local file, returning structured format such as a data frame"""
    raise NotImplementedError
