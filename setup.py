from setuptools import setup, find_packages

setup(
    name="pySQL",
    version="1.0.0",
    description="The project is designed to work with MySQL and MSSQL databases. By integrating this project into other python projects, it is possible to select boards, load them and initiate sql queries using the SQL language.",
    author="Darjus Vasiukevic",
    packages=find_packages(),
    install_requires=[
        "pandas==1.5.3",
        "SQLAlchemy==2.0.23",
        "pyodbc==5.0.1",
        "urllib3==1.26.12",
        "sshtunnel",
        "MySQL-python",
        "mysql-connector-python"
    ],
)
