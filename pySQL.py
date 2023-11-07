import pandas as pd
from sqlalchemy import create_engine
import urllib.parse
from sqlalchemy import text


class SQL:
    def __init__(self, server, database, driver="{ODBC Driver 17 for SQL Server}"):
        self.server = server
        self.database = database
        self.driver = driver

    def get_data(self, query):
        connection_string = (
            f"Driver={self.driver};"
            f"Server={self.server};"
            f"Database={self.database};"
            "Trusted_Connection=yes;"
        )
        engine = create_engine(
            f"mssql+pyodbc:///?odbc_connect={urllib.parse.quote_plus(connection_string)}"
        )
        with engine.connect() as conn:
            query_obj = text(query)
            df = pd.read_sql_query(query_obj, conn)
        return df


if "__main__" == __name__:
    sql = SQL(server="localhost", database="testDB")
    e = sql.get_data(query="select * from testDB.dbo.test_table")
    print(e)

    # df = sql.get_data(query="select * from testDB.dbo.test_table")
    # print(df)
