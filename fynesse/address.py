""" This file contains code for supporting addressing questions in the data """

from . import access
from . import assess
import pandas as pd
import statsmodels.api as sm


def one_hot_encoding(df, column, *, values=None):
    """ Creates a dataframe containing one hot encodings of a column
    :param df: the original dataframe
    :param column: the name of the column
    :param values: the values column can take
    """
    if values is None:
        values = df[column].unique()
    return pd.DataFrame({f"{column}_is_{val}": (df[column] == val).astype(float) for val in values})


def test_model(connection, year, postcode, *, response, family, make_design):
    data = access.get_houses(connection, postcode=postcode,
                             sold_after=f"{year-1}-06-01",
                             sold_before=f"{year+1}-06-01)")
    print(f"Training on {len(data.index)} samples")
    train = data.sample(frac=0.8)
    test = data.drop(train.index)
    model = sm.GLM(train[response], make_design(train), family=family).fit()

    actual = test["price"]
    predicted = model.get_prediction(make_design(test)).summary_frame(alpha=0.1)["mean"]

    print(model.summary())

    assess.show((
        assess.scatter_plot(predicted, actual, name_x="Predicted", name_y="Actual", line_diagonal=True),
        assess.scatter_plot(predicted, actual - predicted, name_x="Predicted", name_y="Error", line_horizontal=True),
    ))