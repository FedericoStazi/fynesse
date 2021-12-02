# This file contains code for supporting addressing questions in the data

"""# Here are some of the imports we might expect 
import sklearn.model_selection  as ms
import sklearn.linear_model as lm
import sklearn.svm as svm
import sklearn.naive_bayes as naive_bayes
import sklearn.tree as tree

import GPy
import torch
import tensorflow as tf

# Or if it's a statistical analysis
import scipy.stats"""

import pandas as pd
import numpy as np

"""Address a particular question that arises from the data"""


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
