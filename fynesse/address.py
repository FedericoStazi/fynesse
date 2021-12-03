""" This file contains code for supporting addressing questions in the data """

from . import access
from . import assess
import pandas as pd
import statsmodels.api as sm
import shapely


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
    """ Tests a design by taking all data from a year in a postcode, 
        training on 80% of it and testing on 20%.
        :param connection: the database connection
        :param year: the year of this data
        :param postcode: the postcode of this data
        :param response: the response column ("price" for this task)
        :param family: the family of the model
        :param make_design: the method creating the design matrix
    """
    
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


def predict_price(connection, latitude, longitude, year, property_type, threshold=100):
    """ Predicts the price for a certain house by training the model on houses near in space and time
        :param connection: the database connection
        :param latitude: the latitude of the house
        :param longitude: the longitude of the house
        :param year: the year of this house
        :param property_type: the type of property
        :param threshold: the minimal amount of houses required for training
    """

    input = pd.DataFrame.from_dict({
        "date": [f"{year}-01-01"],
        "property_type": [property_type],
        "geometry": [shapely.geometry.Point(longitude, latitude)]
    })

    # Search for a distance with at least threshold houses
    distance = 0.001
    data = None
    while distance < 1.0:
        print(f"Trying distance = {distance}...")
        data = access.get_houses(connection,
                                 bbox=(latitude, longitude, distance),
                                 sold_after=f"{year - 1}-06-01",
                                 sold_before=f"{year + 1}-06-01")

        if len(data.index) > threshold:
            break
        else:
            distance *= 1.4

    print(f"Training the model with distance = {distance}")
    if len(data.index) < threshold:
        print(f"Only {len(data.index)} datapoints were found in the area, "
              f"the results may be less accurate")

    model = sm.GLM(data["price"], make_design(data), family=family).fit()
    print(model.summary())

    return model.get_prediction(make_design(input)).summary_frame(alpha=0.1)["mean"]
