import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy import text

# import mysql.connector
import logging
import urllib.parse

import log_config as logc

logc.log


def test_connection(engine):
    try:
        with engine.connect() as connection:
            logging.info(f"{logc.green} Connect succes {logc.reset}")
            return True
    except Exception as e:
        logging.error(f"{logc.red} Connect Error: {e} {logc.reset}")
        return False


class MySQL:
    def __init__(self, host, database, user, password, port="3306"):
        self.host = host
        self.database = database
        self.user = user
        self.password = password
        self.port = port
        self.engine = self.create_mysql_engine()
        self.connect = test_connection(engine=self.engine)

    def create_mysql_engine(self):
        engine = create_engine(
            f"mysql+mysqlconnector://{self.user}:{self.password}@{self.host}/{self.database}"
        )
        return engine


class MsSQL:
    def __init__(self, host, database, driver="{ODBC Driver 17 for SQL Server}"):
        self.host = host
        self.database = database
        self.driver = driver
        self.engine = self.create_mssql_engine()
        self.connect = test_connection(engine=self.engine)

    def create_mssql_engine(self):
        connection_string = (
            f"Driver={self.driver};"
            f"Server={self.host};"
            f"Database={self.database};"
            "Trusted_Connection=yes;"
        )
        engine = create_engine(
            f"mssql+pyodbc:///?odbc_connect={urllib.parse.quote_plus(connection_string)}"
        )

        return engine


class SQL:
    def __init__(
        self,
        host: str,
        database: str,
        user: str,
        password: str,
        connect_type: str,
        port: str = "3306",
        driver: str = "{ODBC Driver 17 for SQL Server}",
    ):
        if connect_type == "MySQL":
            mysql_object = MySQL(host, database, user, password, port)
            self.engine = mysql_object.engine
        if connect_type == "MsSQL":
            mssql_object = MsSQL(host, database, driver)
            self.engine = mssql_object.engine

    def get_data(self, table: str, columns: list = None) -> pd.DataFrame:
        """Select table from database by query
        Args:
            table: table in SQL database which one you want to choose
            columns: columns which one you want select
                            By default: * (all)
        Returns:
            pd.DataFrame: table
        """
        if columns:
            columns_str = ", ".join(columns)
        else:
            columns_str = "*"

        query = f"SELECT {columns_str} from {table}"
        conn = self.engine.connect()
        df = pd.read_sql(text(query), conn)
        return df

    def read_query(self, query: str) -> object:
        """execute sql queries

        Args:
            query (str): SQL query command

        Returns: None or object
        """
        with self.engine.begin() as connection:
            query_obj = text(query)
            output = connection.execute(query_obj)
            if output:
                return output

    def load_data_to_SQL(
        self,
        df: pd.DataFrame,
        table: str,
        truncate=False,
        batch_size=1000,
        max_overflow=100,
    ):
        if truncate:
            self.read_query(query=f"TRUNCATE TABLE {self.database}.{table};")
            logging.info(
                f"  Table: [{logc.bold}{table}{logc.reset}] {logc.green} TRUNCATE SUCCES {logc.reset}"
            )
        else:
            pass

        batches = [df[i : i + batch_size] for i in range(0, len(df), batch_size)]
        for batch in batches:
            batch.to_sql(
                table, self.engine, if_exists="append", index=False, method="multi"
            )
        logging.info(
            f"  LOAD table: [{logc.bold}{table}{logc.reset}] {logc.green} SUCCES {logc.reset}"
        )


if __name__ == "__main__":
    sql = SQL(
        host="localhost",
        database="testDB",
        user="polonez",
        password="polonez",
        connect_type="MsSQL",
    )

    df = sql.get_data(table="dbo.test_table")
    print(df)