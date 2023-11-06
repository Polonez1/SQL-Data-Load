import pandas as pd
from sqlalchemy import create_engine
import urllib.parse
from sqlalchemy import text

pd.set_option("display.max_rows", None)
pd.set_option("display.max_columns", None)


class SQL:
    def __init__(self, server, Database):
        self.server = server
        self.Database = Database

    def get_data(self, query):
        connection_string = (
            "Driver={ODBC Driver 17 for SQL Server};"
            f"Server={self.server};"
            f"Database={self.Database};"
            "Trusted_Connection=yes;"
        )
        engine = create_engine(
            f"mssql+pyodbc:///?odbc_connect={urllib.parse.quote_plus(connection_string)}"
        )
        with engine.connect() as conn:
            query_obj = text(query)
            df = pd.read_sql_query(query_obj, conn)
        return df

    def load_data_to_SQL(
        self, DF, table, truncate=False, batch_size=1000, max_overflow=100
    ):
        connection_string = (
            "Driver={ODBC Driver 17 for SQL Server};"
            f"Server={self.server};"
            f"Database={self.Database};"
            "Trusted_Connection=yes;"
        )
        engine = create_engine(
            f"mssql+pyodbc:///?odbc_connect={urllib.parse.quote_plus(connection_string)}",
            connect_args={"SQLALCHEMY_MAX_OVERFLOW": max_overflow},
            # fast_executemany=True
        )

        if truncate:
            print("Patvirtinti TRUNCATE?")
            choice = str(input())
            if choice == "Taip":
                with engine.begin() as connection:
                    connection.execute(f"TRUNCATE TABLE {self.Database}.dbo.{table};")
            else:
                pass

        batches = [DF[i : i + batch_size] for i in range(0, len(DF), batch_size)]
        for batch in batches:
            batch.to_sql(
                table,
                engine,
                schema="dbo",
                if_exists="append",
                index=False,
                method="multi",
            )

    def execute_query(self, query):
        connection_string = (
            "Driver={ODBC Driver 17 for SQL Server};"
            f"Server={self.server};"
            f"Database={self.Database};"
            "Trusted_Connection=yes;"
        )

        engine = create_engine(
            f"mssql+pyodbc:///?odbc_connect={urllib.parse.quote_plus(connection_string)}"
        )
        with engine.begin() as connection:
            query_obj = text(query)
            connection.execute(query_obj)
