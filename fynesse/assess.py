from .config import *

from fynesse import access

import osmnx as ox
# QUESTION is this library ok?
import plotly.express as px

"""These are the types of import we might expect in this file
import pandas
import bokeh
import matplotlib.pyplot as plt
import sklearn.decomposition as decomposition
import sklearn.feature_extraction"""

"""
Place commands in this file to assess the data you have downloaded. 
How are missing values encoded, how are outliers encoded? 
What do columns represent? Make sure they are correctly labeled. 
How is the data indexed. Crete visualisation routines to assess the data (e.g. in bokeh). 
Ensure that date formats are correct and correctly time-zoned.
"""


def _get_houses(connection, condition):
    return connection.query(f"""
        SELECT pp_data.*, lattitude, longitude
        FROM pp_data
        INNER JOIN postcode_data
        ON pp_data.postcode = postcode_data.postcode
        WHERE {condition}
    """)


def get_house_by_id(connection, house_id):
    return _get_houses(connection, f"pp_data.db_id = {house_id}")


def get_houses_by_postcode(connection, postcode):
    if not postcode:
        # QUESTION: is it ok? should it also work outside of London?
        rpostcode = "'^(WC|EC|N|E|SE|SW|W|NW)([^[:alpha:]]|$)'"
    elif postcode[-1].isdigit():
        rpostcode = f"'^{postcode}([^[:digit:]]|$)'"
    else:
        rpostcode = f"'^{postcode}([^[:alpha:]]|$)'"
    return _get_houses(connection, f"pp_data.postcode RLIKE {rpostcode}")


def get_houses_by_bbox(connection, lat, lon, dist):
    return _get_houses(connection, f"""
        lattitude > {lat - dist / 2} AND
        lattitude < {lat + dist / 2} AND
        longitude > {lon - dist / 2} AND
        longitude < {lon + dist / 2}
    """)


def get_pois_by_bbox(lat, lon, dist, tags):
    return ox.geometries_from_point((lat, lon), dist=dist, tags=tags)


def _plot(df, lat, lon, name=None):
    fig = px.scatter_mapbox(df, lat=lat, lon=lon, hover_name=name, zoom=10)
    fig.update_layout(title="London", title_x=0.5)
    fig.update_layout(mapbox_style="carto-positron")
    fig.show()


def plot_pois(df):
    _plot(df, df.geometry.centroid.x, df.geometry.centroid.y, df.name)


def plot_houses(df):
    _plot(df, df.lattitude, df.longitude, df.postcode)


def data():
    """
    Load the data from access and ensure missing values are correctly encoded as well as indices correct, column names
    informative, date and times correctly formatted. Return a structured data structure such as a data frame.
    """
    df = access.data()
    raise NotImplementedError


def query(data):
    """Request user input for some aspect of the data."""
    raise NotImplementedError


def view(data):
    """Provide a view of the data that allows the user to verify some aspect of its quality."""
    raise NotImplementedError


def labelled(data):
    """Provide a labelled set of data ready for supervised learning."""
    raise NotImplementedError
