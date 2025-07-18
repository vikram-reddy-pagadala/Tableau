"""Module containing the implementation and factory method for Connections.

[Purpose]:
The Connection class is the representation of Database Connection.

It uses information from both a configuration file (located in ../../conf/*.ini) or information
from obtained from a MetadataConnection.

[Initialization]:
Initialization should take place through the the `create_connection` factory function below,
which takes a `db_type` as a positional argument, as well as a keyword argument `use_jdbc`.

[Implementations]:
    * HiveConnection
    * JDBCConnection
    * MySQLConnection
    * PostgreSQLConnection
    * SQLServerConnection
"""

# Standard
import logging
import sys
import os
from abc import ABC, abstractmethod
from typing import Any

# Third Party
import impala.dbapi
import jaydebeapi
import jpype
import mysql.connector
import psycopg2
#import pymssql

# Source
from accelerator.common import utils

logger = logging.getLogger(__name__)


class Connection(ABC):
    """
    Connection(connection_name: str)

    This is used as the AbstractBaseClass for a general connection.

    In order to implement the ABC, one must define:
        [Methods]
        * _get_connection()

        [Attributes]
        * _connection_name
        * _connection_package
    """

    _connection_name: str = None
    _connection_package: Any = None

    def __init__(self, connection_name: str = None, log = None):
        """Initialization function.
        :param connection_name: Optional name for the connection, which is used to index into the configuration file.
        """
        # If connection name was given, set it
        if log:
            self._logger = log
        else:
            self._logger = logger


        if connection_name:
            self._connection_name = connection_name

        self._connection: Any = None
        self._cursor: Any = None

        # Initialize empty list of statements that were run
        self._run_statements: list = []
        self._connect()
        self._create_cursor()

    @abstractmethod
    def _get_connection(self, conn_info: dict) -> dict:
        """Method to be implemented that returns all needed connection information from the config dictionary."""
        raise NotImplementedError("You should implement this method.")

    def _connect(self):
        """
        Establish a connection to this Connection
        Process:
            1) Index the config using the `_connection_name`
            2) Get the connection information
            3) Ensure the connection is valid
            4) Set the connection
        """
        self._logger.debug(f"Connecting to {self._connection_name}...")

        config = utils.get_config()
        try:
            conn_info: dict = config[self._connection_name]
        except KeyError:
            self._logger.exception(
                f"Error in configuration file: No section {self._connection_name} in .ini"
            )
            print("Exiting...")
            sys.exit(-1)

        try:
            connection: dict = self._get_connection(conn_info)
        except KeyError as e:
            raise Exception(
                f"Unable to make connection for {self._connection_name} because {e} didn't exist in the configuration."
            )

        #self._logger.debug(f"{self._connection_name} credentials: {connection}")

        self._set_connection(connection)

        self._logger.debug(f"Successfully connected to {self._connection_name}.")

    def _create_cursor(self):
        """Create a cursor for the hive connection."""
        self._logger.debug(f"Trying to make {self._connection_name} cursor...")
        try:
            # NOTE: Consider setting buffered=True so that we can close even when there are more
            # results in result set
            self._cursor = self._connection.cursor()
        except Exception as e:
            self._logger.exception(f"Failed to make {self._connection_name} cursor")
            self._connection.close()
            raise e from None

        self._logger.debug(f"Successfully made {self._connection_name} cursor.")

    def connection_validation(self, connection):
        """Checking if the connection is valid/exist else reconnecting to the DB"""
        connection_status = connection._cursor.closed
        if connection_status:
            self._connect()
            self._create_cursor()  

    def _set_connection(self, connection: dict):
        """Set connection."""
        self._connection = self._connection_package.connect(**connection)

    def close(self):
        """Close the connection and cursor."""
        self._logger.debug(f"Trying to close {self._connection_name} Connection...")
        if self._cursor is not None:
            self._cursor.close()
        if self._connection is not None:
            self._connection.close()

        self._logger.info(f"Successfully closed {self._connection_name} Connection.")

    def commit(self):
        """Commit the transaction."""
        self._logger.debug(f"Trying to commit {self._connection_name} transaction...")
        if self._connection is not None:
            self._connection.commit()

        self._logger.info(f"Successfully committed {self._connection_name} transaction.")


    def execute(self, statement: str) -> Any:
        """Execute the given statement."""
        self._logger.debug(f"Executing: {statement}")
        try:
            self._cursor.execute(statement)
        except Exception as e:
            self._logger.exception(f"Error in executing statement {statement}")
            self._logger.info(f"Closing {self._connection_name}...")
            self.close()
            raise e from None

        self._run_statements.append(statement)
        self._logger.info(f"`{statement}` executed successfully")

    def fetchone(self, check_not_empty: bool = True):
        """Fetch one result from the cursor and ensure it is not empty."""
        results = self._cursor.fetchone()  # type: ignore
        if check_not_empty and not results:
            raise Exception("Result set was empty")

        return results

    def get_release_environment(self):
        env = os.getenv('ENV')
        return env


class HiveConnection(Connection):
    """
    Class to be used for a HiveConnection, inheriting from the Connection class.
    """

    _connection_name = "Hive"
    _connection_package = impala.dbapi

    def _get_connection(self, conn_info: dict) -> dict:
        """Gets connection information needed for HiveConnection.

        A valid HiveConnection needs:
            * host
            * port
            * user
            * password
            * database
            * auth_mechanism
        """
        connection: dict = {
            #"host": conn_info["host"],
            "host": os.environ["HIVE_HOST"],
            "port": conn_info["port"],
            #"user": conn_info["user"],
            "user": os.environ["HIVE_USER"],
            #"password": conn_info["password"],

            # HACK need to remove unicode zero size whitespace and replace double backslash w/single
            "password": os.environ["HIVE_PWD"].replace('\u200b', '').replace('\\\\', '\\'),

            "auth_mechanism": conn_info["auth_mechanism"],
            "use_ssl" : "True"
        }

        # Cast port to int because it's needed for the proper connection
        try:
            connection["port"] = int(connection["port"])
        except ValueError:
            self._logger.exception(
                f"Port variable not an integer but needs to be. Value: {connection['port']}"
            )
        except KeyError:
            self._logger.info("No port variable included. Proceeding without one..")

        return connection


class MySQLConnection(Connection):
    """
    Class to be used for a MySQLConnection, inheriting from the Connection class.
    """

    _connection_name = "MySQL"
    _connection_package = mysql.connector

    def _get_connection(self, conn_info: dict) -> dict:
        """Gets connection information needed for MySQLConnection.

        A valid MySQLConnection needs:
            * user
            * password
            * database
            * host
        """
        connection: dict = {
            "user": conn_info["user"],
            "password": conn_info["password"],
            "database": conn_info["database"],
            "host": conn_info["host"],
            "autocommit": True,
        }

        return connection


#class SQLServerConnection(Connection):
#    """
#    Class to be used for a SQLServerConnection, inheriting from the Connection class.
#    """
#
#    _connection_name = "SQLServer"
#    _connection_package = pymssql
#
#    def _get_connection(self, conn_info: dict) -> dict:
#        """Gets connection information needed for SQLServerConnection.
#
#        A valid SQLServerConnection needs:
#            * user
#            * password
#            * server
#            * autocommit
#        """
#        connection: dict = {
#            "user": conn_info["user"],
#            "password": conn_info["password"],
#            "server": conn_info["host"],
#            "port": conn_info["port"],
#            "autocommit": conn_info.get("autocommit", True),
#        }
#
#        return connection


class JDBCConnection(Connection):
    """
    Class to be used for a JDBCConnection, inheriting from the Connection class.
    """

    _connection = "JDBC"
    _connection_package = jaydebeapi

    def _get_connection(self, conn_info: dict) -> dict:
        """Gets connection information needed for JDBCConnection.

        A valid JDBCConnection needs:
            * user
            * password
            * database
            * host
            * port
            * classpath
            * jdbc_type
            * jarpath
        """

        # Create base dictionary that has information we'll need for
        # constructing the real conneciton
        _connection: dict = {
            "user": conn_info["user"],
            "password": conn_info["password"],
            "database": conn_info["database"],
            "host": conn_info["host"],
            "port": conn_info["port"],
            "classpath": conn_info["classpath"],
            "jdbc_type": conn_info["jdbc_type"],
            "jarpath": conn_info["jarpath"],
        }

        jdbc_type: int = _connection["jdbc_type"].lower()

        # Get addition due to syntactical differences
        if jdbc_type in {"mysql", "hive2"}:
            database_url_addition: str = "/"
        elif jdbc_type in {"sqlserver"}:
            database_url_addition = f";database="
        else:
            raise Exception(f"Improper jdbc_type: {jdbc_type}")

        # Construct the url using the jdbc_type, host, port, and which database
        # to use
        url: str = f"jdbc:{jdbc_type}://{_connection['host']}:{_connection['port']}{database_url_addition}{_connection['database']}"

        connection: dict = {
            "jclassname": _connection["classpath"],
            "url": url,
            "driver_args": [_connection["user"], _connection["password"]],
            "jars": _connection["jarpath"],
        }

        # Start connection
        jvm = jpype.getDefaultJVMPath()
        args: str = f"-Djava.class.path={connection['jars']}"
        jpype.startJVM(jvm, args)

        return connection


class PostgreSQLConnection(Connection):
    """
    Class to be used for a PostgreSQLConnection, inheriting from the Connection class.
    """

    _connection_name = "PostgreSQL"
    _connection_package = psycopg2

    def _get_connection(self, conn_info: dict) -> dict:
        """Gets connection information needed for PostgreSQLConnection.
        A valid PostgreSQLConnection needs:
            * user
            * password
            * database
            * host
            * port
        """

        #HACK if not found just assume the relative location
        edp_config_path = os.environ.get("EDP_CONFIG_PATH", "EDP_CONFIG_PATH is not set")
        connection: dict = {
            "user": conn_info["user"],
            #"user": os.environ["RDS_USRNAME"],
            "password": '',
            #"password": conn_info["password"],
            "database": conn_info["database"],
            #"database": os.environ["RDS_DB"],
            #"host": conn_info["host"],
            "host": os.environ["RDS_ENDPT"],
            #"port": conn_info["port"],
            "port": os.environ["RDS_PORT"],
            #"options": f"--search_path={conn_info['schema']}",
            "options": f"--search_path={os.environ['RDS_SCHEMA']}",

            "sslmode": 'verify-ca',

            # refer to centralized ssl cert in the config directory
            "sslrootcert": edp_config_path+"/rds-combined-ca-bundle.pem" 
        }

        #adjust ssl certification path if it is deployed to a user account
        if not (os.getcwd().startswith('/mnt1/ibm/') or os.getcwd().startswith('/opt/Hadoop')):
            connection["sslrootcert"] = edp_config_path+"/rds-combined-ca-bundle.pem" 

        # Need to get the temporary password token using boto3
        import boto3
        client = boto3.client('rds', region_name=os.environ["REG"])
        token = client.generate_db_auth_token(DBHostname=connection["host"], Port=connection["port"], DBUsername=connection["user"], Region=os.environ["REG"])
        if token is not None:
            connection["password"] = str(token)

        return connection


def create_connection(db_type: str, use_jdbc: bool = False, log = None) -> Connection:
    """
    Use to create a Connection class
    :param connection_type: The type of database connection we want to create
    :param use_jdbc: To be used as a keyword argumnent to allow overriding
    :returns Connection: A valid connection
    :raises Exception if `connection_type` not in __connections dict
    """


    # Mapping of possible databases to our Connection class implementation
    __connections: dict = {
        "hive": HiveConnection,
        "jdbc": JDBCConnection,
        "mysql": MySQLConnection,
        "postgresql": PostgreSQLConnection,
        #"sqlserver": SQLServerConnection,
    }

    # Ensure case insensitive by changing to lower
    connection_type: str = db_type.lower() if not use_jdbc else "jdbc"

    try:
        ConnectionClass = __connections[connection_type]
    except KeyError:
        raise Exception(f"Not a valid connection_type: {db_type}") from None

    return ConnectionClass(connection_type, log = log)
