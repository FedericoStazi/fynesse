from .config import *

from fynesse import access

import bokeh.io
import bokeh.plotting
import bokeh.tile_providers

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


def geo_plot(df, *, label=None):
    df_t = df.to_crs(3857)
    bokeh.io.output_notebook()

    if label:
        source = bokeh.plotting.ColumnDataSource(data=dict(
            x=df_t.geometry.centroid.x,
            y=df_t.geometry.centroid.y,
            label=df_t[label],
        ))
        tooltips = [(label, "@label")]
    else:
        source = bokeh.plotting.ColumnDataSource(data=dict(
            x=df_t.geometry.centroid.x,
            y=df_t.geometry.centroid.y,
        ))
        tooltips = None

    p = bokeh.plotting.figure(
        width=600, height=600, tooltips=tooltips,
        x_axis_type="mercator", y_axis_type="mercator",
        tools="pan,wheel_zoom")

    p.add_tile(bokeh.tile_providers.get_provider(bokeh.tile_providers.CARTODBPOSITRON))
    p.circle('x', 'y', size=10, alpha=0.7, source=source)

    bokeh.plotting.show(p)


def get_bbox_around(df, *, padding=0.1):
    minx, miny, maxx, maxy = df.to_crs(4326).total_bounds
    return (miny + maxy) / 2, (minx + maxx) / 2, max(maxx - minx, maxy - miny) + padding


def get_distances_from_closest(df, targets):
    return df.geometry.apply(targets.distance).min(axis=1)


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
