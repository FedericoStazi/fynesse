""" Place commands in this file to assess the data you have downloaded.
How are missing values encoded, how are outliers encoded?
What do columns represent? Make sure they are correctly labeled.
How is the data indexed. Crete visualisation routines to assess the data (e.g. in bokeh).
Ensure that date formats are correct and correctly time-zoned.
"""

import pandas as pd
import bokeh.io
import bokeh.plotting
import bokeh.tile_providers
import bokeh.palettes


def assess_database(connection, database):
    """ Displays some information about the database to quickly identify potential issues
    :param connection: the connection to the database
    :param database: the database name
    """
    return connection.query(f"""
        SELECT table_schema, table_name, table_rows, index_length, create_time, update_time, table_collation, temporary
        FROM information_schema.tables 
        WHERE table_schema="{database}"
    """)


def assess_table(connection, database, table):
    """ Displays some information about the table to quickly identify potential issues
    :param connection: the connection to the database
    :param database: the database name
    :param table: the table name
    """
    return connection.query(f"""
        SELECT table_schema, table_name, column_name, ordinal_position, column_type, is_nullable 
        FROM information_schema.columns col
        JOIN information_schema.tables
        USING (table_name, table_schema)
        WHERE col.table_schema="{database}" AND table_name="{table}"
    """)


def assess_dataframe(df, *, enumerations=None, dates=None):
    """ Displays some information about the dataframe to quickly identify potential issues
    :param df: the dataframe
    :param enumerations: a list of strings, the columns that are expected to be enumerations
    :param dates: a list of strings, the columns that are expected to be strings
    """

    if enumerations is None:
        enumerations = []
    if dates is None:
        dates = []

    # Check 1: number or rows (to avoid empty dataframes or other construction issues)
    print(f"The number of rows is: {len(df.index)}.")

    # Check 2: columns containing NaN (to avoid errors with operations later)
    is_na = df.isna().any()
    if is_na.any():
        cols = " ".join(f"\"{col}\"" for col in df.columns[is_na])
        print(f"These columns have missing elements (NaN): {cols}.")
    else:
        print(f"No column has missing elements (NaN).")

    # Check 3: elements in an enumeration (to avoid issues with elements that should not be in the enumeration)
    for col in enumerations:
        print(f"Column \"{col}\" only contains: {df[col].unique()}.")

    # Check 4: dates (to avoid issues with the date format and date range)
    for col in dates:
        print(f"Column \"{col}\" contains dates from {df[col].min()} to {df[col].max()}.")


def line_plot(x, y, *, name_x="", name_y="", title=""):
    """ The plot of a line of lines
    :param x: the x coordinates
    :param y: the y coordinates
    :param name_x: the name of the x-axis on the plot
    :param name_y: the name of the y-axis on the plot
    :param title: the name of the plot
    """

    p = bokeh.plotting.figure(title=title, width=600, height=600)

    p.line(x, y, line_width=2)

    p.xaxis.axis_label = name_x
    p.yaxis.axis_label = name_y

    return p


def hist_plot(h, *, groups=None, name_h="", title="", bins=10000):
    """ The plot of a distribution of a series or group of series
    :param h: the series
    :param groups: the grouping of data
    :param name_h: the name of the x-axis on the plot
    :param title: the name of the plot
    :param bins: the number of bins
    """

    p = bokeh.plotting.figure(title=title, width=600, height=600)

    ht = ((h - h.min()) / (h.max() - h.min()) * bins).astype(int)
    x = pd.Series(range(0, bins)) / bins * (h.max() - h.min()) + h.min()

    if groups is None:
        y = pd.Series((ht == i).sum() / bins for i in range(0, bins))
        p.line(x, y, line_width=2)
    else:
        idx = 0
        for (gl, gh) in ht.groupby(groups):
            gy = pd.Series((gh == i).sum() / bins for i in range(0, bins))
            p.line(x, gy, line_width=2, color=bokeh.palettes.Category10[10][idx], legend_label=gl)
            idx += 1
        p.legend.location = "top_right"
        p.legend.click_policy = "hide"

    p.xaxis.axis_label = name_h

    return p


def scatter_plot(x, y, *, groups=None, name_x="", name_y="", title="", line_diagonal=False, line_horizontal=False):
    """ The scatter plot of 2D points
    :param x: the x coordinates
    :param y: the y coordinates
    :param groups: the grouping of data
    :param name_x: the name of the x-axis on the plot
    :param name_y: the name of the y-axis on the plot
    :param title: the name of the plot
    :param line_diagonal: if true, plots a line y = x
    :param line_horizontal: if true, plots a line y = 0
    """

    p = bokeh.plotting.figure(title=title, width=600, height=600)
    if groups is None:
        p.circle(x, y, size=2, alpha=0.7)
    else:
        idx = 0
        for (gl, gx), (_, gy) in zip(x.groupby(groups), y.groupby(groups)):
            p.circle(gx, gy, size=2, alpha=0.7, color=bokeh.palettes.Category10[10][idx], legend_label=gl)
            idx += 1
        p.legend.location = "top_right"
        p.legend.click_policy = "hide"

    if line_diagonal:
        p.line([0, min(x.max(), y.max())], [0, min(x.max(), y.max())], line_width=2)
    if line_horizontal:
        p.line([0, min(x.max(), y.max())], [0, 0], line_width=2)

    p.xaxis.axis_label = name_x
    p.yaxis.axis_label = name_y

    return p


def geo_plot(df, *, labels=None, title=""):
    """ The plot of a GeoDataFrame
    :param df: the GeoDataFrame
    :param labels: the labels for the points
    :param title: the name of the plot
    """

    df_t = df.to_crs(3857)

    if labels is None:
        source = bokeh.plotting.ColumnDataSource(data=dict(
            x=df_t.geometry.centroid.x,
            y=df_t.geometry.centroid.y,
        ))
        tooltips = None
    else:
        source = bokeh.plotting.ColumnDataSource(data=dict(
            x=df_t.geometry.centroid.x,
            y=df_t.geometry.centroid.y,
            name=labels,
        ))
        tooltips = [("name", "@name")]

    p = bokeh.plotting.figure(
        width=600, height=600, tooltips=tooltips,
        x_axis_type="mercator", y_axis_type="mercator",
        tools="pan,wheel_zoom", title=title)

    p.add_tile(bokeh.tile_providers.get_provider(bokeh.tile_providers.CARTODBPOSITRON))
    p.circle('x', 'y', size=10, alpha=0.7, source=source)

    return p


def show(plots):
    """ Shows a list of plots arranged horizontally
    :param plots: the list of plots
    """
    bokeh.io.output_notebook()
    bokeh.plotting.show(bokeh.layouts.row(*plots))


def get_bbox_around(df, *, padding=0.1):
    """ Returns a bbox including all points in a GeoDataFrame
        Useful when searching for pois around a set of geometries
    :param df: the GeoDataFrame
    :param padding: additional padding for the bbox around the geometries
    """
    minx, miny, maxx, maxy = df.to_crs(4326).total_bounds
    return (miny + maxy) / 2, (minx + maxx) / 2, max(maxx - minx, maxy - miny) + padding


def get_distances_from_closest(sources, targets):
    """ Returns a Series of distances between each source and their closest target
        This can be used when approximating the location of the houses to their postcode district
    :param targets: the GeoDataFrame of targets
    :param sources: the GeoDataFrame of sources
    """
    return sources.geometry.apply(targets.distance).min(axis=1)
