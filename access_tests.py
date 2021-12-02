""" These tests should be checking the known access problems,
    such as whether an excel file has turned a gene to a date or has more than 1,048,576 rows.
"""


def table_creation(connection, table):
    """ Tests if the table was created and can be accessed
    :param connection: the connection to the database
    :param table: the table name
    """
    query = connection.query(f"SELECT * FROM {table} LIMIT 1")
    assert len(query.index), f"Empty response from {table}"
    print("The table was created successfully")
