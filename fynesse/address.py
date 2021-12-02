""" This file contains code for supporting addressing questions in the data """

import pandas as pd
import numpy as np


def one_hot_encoding(df, column, *, values=None):
    """ Creates a dataframe containing one hot encodings of a column
    :param df: the original dataframe
    :param column: the name of the column
    :param values: the values column can take
    """
    if values is None:
        values = df[column].unique()
    return pd.DataFrame({f"{column}_is_{val}": (df[column] == val).astype(float) for val in values})


def make_design(columns, df):
    """ Create a design matrix from a dataframe and a list of its columns
    :param columns: the list of columns in the dataframe used in the design
    :param df: the dataframe
    """
    design = []
    for col in columns:
        design.append(df[col].values.reshape(-1, 1))
    return np.concatenate(design, axis=1)
