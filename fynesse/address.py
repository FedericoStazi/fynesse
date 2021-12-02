# This file contains code for supporting addressing questions in the data

import pandas as pd
import numpy as np



def one_hot_encoding(df, column, *, values=None):
    """ Creates a DataFrame containing one hot encodings of a column
    :param df: the original DataFrame
    :param column: the name of the column
    :param values: the values column can take
    """
    if values is None:
        values = df[column].unique()
    return pd.DataFrame({f"{column}_is_{val}": (df[column] == val).astype(float) for val in values})


def make_design(columns, data):
    design = []
    for col in columns:
        design.append(data[col].values.reshape(-1, 1))
    return np.concatenate(design, axis=1)
