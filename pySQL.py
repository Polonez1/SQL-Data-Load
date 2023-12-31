import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy import text
import sshtunnel
import MySQLdb

# import mysql.connector
import logging
import urllib.parse

import log_config as logc

logc.log


def test_connection(engine):
    try:
        with engine.connect() as connection:
            logging.info(f"{logc.green} connected successfully {logc.reset}")
            return True
    except Exception as e:
        logging.error(f"{logc.red} Connect Error: {e} {logc.reset}")
        return False


class SSHtunnel:
    sshtunnel.SSH_TIMEOUT = 5.0
    sshtunnel.TUNNEL_TIMEOUT = 5.0

    def __init__(
        self,
        ssh_host: str,
        ssh_username: str,
        ssh_password: str,
        remote_bind_address: tuple,
    ):
        self.ssh_host = ssh_host
        self.ssh_user = ssh_username
        self.ssh_password = ssh_password
        self.remote_bind_address = remote_bind_address

    def create_tunnel(self):
        "after create use tunnel.start() and tunnel.close()"
        tunnel = sshtunnel.SSHTunnelForwarder(
            ("ssh.pythonanywhere.com"),
            ssh_username=self.ssh_user,
            ssh_password=self.ssh_password,
            remote_bind_address=self.remote_bind_address,
        )
        return tunnel


class MySQL:
    def __init__(self, host, database, user, password, port):
        self.host = host
        self.database = database
        self.user = user
        self.password = password
        self.port = port
        self.engine = self.__create_mysql_engine()
        self.connect = test_connection(engine=self.engine)

    def __create_mysql_engine(self):
        engine = create_engine(
            f"mysql+mysqldb://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"
        )
        return engine


class MsSQL:
    def __init__(self, host, database, driver="{ODBC Driver 17 for SQL Server}"):
        self.host = host
        self.database = database
        self.driver = driver
        self.engine = self.__create_mssql_engine()
        self.connect = test_connection(engine=self.engine)

    def __create_mssql_engine(self):
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
        # host: str,
        # database: str,
        # user: str,
        # password: str,
        connect_type: str,
        driver: str = "{ODBC Driver 17 for SQL Server}",
        **kwargs,
    ):
        """Initialize class objects
        Args:
            connect_type (str): MySQL || MsSQL
            port (str, optional): port. Defaults to "3306".
            driver (str, optional): driver, if you connect to MsSQL. Defaults to "{ODBC Driver 17 for SQL Server}".
        Kwargs:
            host (str): server name or ip
            database (str): database
            user (str, optional): user name (only MySQL, MsSQL use a Windows auth)
            password (str, optional): password (only MySQL, MsSQL use a Windows auth)
        """
        if connect_type == "MySQL":
            mysql_object = MySQL(**kwargs)
            self.engine = mysql_object.engine
        if connect_type == "MsSQL":
            mssql_object = MsSQL(**kwargs)
            self.engine = mssql_object.engine

    def get_data(
        self, table: str = None, columns: list = None, query: str = None
    ) -> pd.DataFrame:
        """Select table from database by query

        Args:
            table: table in SQL database which one you want to choose
                            By default: None
            columns: columns which one you want select
                            By default: * (all)
            query: select table from query (select * from table;)
                            By default: None
        Returns:
            pd.DataFrame: table
        """
        if columns:
            columns_str = ", ".join(columns)
        else:
            columns_str = "*"

        query_by_table = f"SELECT {columns_str} from {table}"
        conn = self.engine.connect()
        if table is not None:
            df = pd.read_sql(text(query_by_table), conn)
        elif query is not None:
            df = pd.read_sql(text(query), conn)

        conn.close()
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
            logging.info(
                f"  Query: [{logc.bold}{query[:26]}...{logc.reset}] {logc.green} query read successfully {logc.reset}"
            )
            if output:
                return output

    def load_data_to_SQL(
        self,
        df: pd.DataFrame,
        table: str,
        truncate=False,
        batch_size: int = 1000,
        max_overflow: int = 100,
    ):
        if truncate:
            self.read_query(query=f"TRUNCATE TABLE {self.database}.{table};")
            logging.info(
                f"  Table: [{logc.bold}{table}{logc.reset}] {logc.green} TRUNCATE successfully {logc.reset}"
            )
        else:
            pass

        batches = [df[i : i + batch_size] for i in range(0, len(df), batch_size)]
        for batch in batches:
            batch.to_sql(
                table, self.engine, if_exists="append", index=False, method="multi"
            )
        logging.info(
            f"  LOAD table: [{logc.bold}{table}{logc.reset}] {logc.green} loaded successfully {logc.reset}"
        )
        
    def get_data_from_query(self, query: str, params: dict = None) -> pd.DataFrame:
        if params is None:
            params = {}
        query = query.replace("{ ", "{").replace(" }", "}")
        with self.engine.connect() as conn:
            query_obj = text(query.format(**params))
            df = pd.read_sql_query(query_obj, conn, params=params)
        return df


if __name__ == "__main__":
    pass
