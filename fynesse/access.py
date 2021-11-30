from .config import *

import yaml
from ipywidgets import interact_manual, Text, Password
import pymysql
from urllib import request
from zipfile import ZipFile
import pandas as pd
import shapely
import geopandas
import osmnx as ox
import overpass

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
        credentials_filename = "credentials.yaml"
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

    def query(self, query):
        cursor = self.connection.cursor()
        cursor.execute(query)
        rows = cursor.fetchall()
        columns = [x for x, *_ in cursor.description] if cursor.description else []
        return pd.DataFrame(rows, columns=columns)


class PPDataTable:
    def __init__(self, connection):
        self.connection = connection

    def create_table(self):
        return self.connection.query(f"""
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

    def create_indices(self):
        return self.connection.query(f"""
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

    def load_data(self, *, start_year=1995, end_year=2021):
        for year in range(start_year, end_year + 1):
            for part in range(1, 3):
                print(f"\rLoading year {year} part {part}...", end="")

                filename = f"pp-{year}-part{part}.csv"
                url = f"http://prod.publicdata.landregistry.gov.uk.s3-website-eu-west-1.amazonaws.com/{filename}"
                request.urlretrieve(url, filename)

                self.connection.query(f"""
                    LOAD DATA LOCAL INFILE '{filename}' INTO TABLE pp_data
                        FIELDS TERMINATED BY ','
                        OPTIONALLY ENCLOSED BY '"'
                        LINES STARTING BY '' TERMINATED BY '\\n';
                """)

                os.remove(filename)

        print("")


class PostcodeDataTable:
    def __init__(self, connection):
        self.connection = connection

    def create_table(self):
        return self.connection.query(f"""
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

    def create_indices(self):
        return self.connection.query(f"""
            ALTER TABLE `postcode_data`
            DROP INDEX IF EXISTS `PRIMARY`,
            DROP INDEX IF EXISTS `po.postcode`,
            ADD PRIMARY KEY (`db_id`),
            MODIFY `db_id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,AUTO_INCREMENT=1;
            CREATE INDEX `po.postcode` USING HASH
                ON `postcode_data`
                    (postcode);
        """)

    def load_data(self):
        filename = "open_postcode_geo.csv"
        url = "https://www.getthedata.com/downloads/open_postcode_geo.csv.zip"
        request.urlretrieve(url, f"{filename}.zip")

        with ZipFile(f"{filename}.zip", 'r') as zip_file:
            zip_file.extractall()

        self.connection.query(f"""
            LOAD DATA LOCAL INFILE '{filename}' INTO TABLE postcode_data
            FIELDS TERMINATED BY ',' 
            LINES STARTING BY '' TERMINATED BY '\n';
        """)

        os.remove(filename)
        os.remove(f"{filename}.zip")


def get_houses(connection, *, postcode=None, bbox=None, sold_after=None, sold_before=None):
    conditions = ["TRUE"]

    if postcode:
        if postcode[-1].isdigit():
            rpostcode = f"'^{postcode}([^[:digit:]]|$)'"
        else:
            rpostcode = f"'^{postcode}([^[:alpha:]]|$)'"
        conditions.append(f"pp_data.postcode RLIKE {rpostcode}")

    if bbox:
        (lat, lon, dist) = bbox
        conditions.append(f"lattitude > {lat - dist / 2}")
        conditions.append(f"lattitude < {lat + dist / 2}")
        conditions.append(f"longitude > {lon - dist / 2}")
        conditions.append(f"longitude < {lon + dist / 2}")

    if sold_after:
        conditions.append(f"date_of_transfer >= \"{sold_after}\"")

    if sold_before:
        conditions.append(f"date_of_transfer <= \"{sold_before}\"")

    houses = connection.query(f"""
            SELECT 
                price, 
                date_of_transfer as date, 
                property_type as type, 
                postcode, 
                postcode_district as district, 
                lattitude as lat, 
                longitude as lon
            FROM pp_data
            INNER JOIN postcode_data
            USING (postcode)
            WHERE {" AND ".join(conditions)}
        """)
    houses["geometry"] = houses[["longitude", "lattitude"]].apply(shapely.geometry.Point, axis=1)
    return geopandas.GeoDataFrame(houses, crs=4326)


def get_districts(connection):
    districts = connection.query("""
        SELECT 
            postcode_district AS district, 
            AVG(lattitude) as lattitude, 
            AVG(longitude) as longitude FROM postcode_data
        WHERE postcode_district != "" AND status = "live"
        GROUP BY district
    """)
    districts["geometry"] = districts[["longitude", "lattitude"]].apply(shapely.geometry.Point, axis=1)
    return geopandas.GeoDataFrame(districts, crs=4326)


def get_pois(*, bbox, tags=None):
    (lat, lon, dist) = bbox
    return ox.geometries_from_point((lat, lon), dist=dist, tags=tags)


def get_pois_fast(*, bbox=None, tags=None):
    if bbox:
        (lat, lon, dist) = bbox
        (minx, miny, maxx, maxy) = (lat - dist, lon - dist,
                                    lat + dist, lon + dist)
    else:
        (minx, miny, maxx, maxy) = (50.0, -11.0, 63.0, 2.0)

    query_bbox = f"{minx},{miny},{maxx},{maxy}"
    query_tag_only = "".join(f"node[\"{key}\"=\"{val}\"]({query_bbox});"
                             for key, val in tags.items() if type(val) is str)
    query_tag_val = "".join(f"node[\"{key}\"]({query_bbox});"
                            for key, val in tags.items() if val is True)

    pois = overpass.API(max_retry_count=10, retry_timeout=5).Get(f"({query_tag_only}{query_tag_val})")
    return geopandas.GeoDataFrame.from_features(pois['features'], crs=4326)
