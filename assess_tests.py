""" These tests assess the contents of the database and dataframes """


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