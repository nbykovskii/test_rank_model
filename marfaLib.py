import pandas as pd
import pymysql
import sshtunnel


# noinspection PyGlobalUndefined

def connect_to_ssh(
        ssh_host = 'datamart.wake-app.net',
        ssh_port = 22,
        ssh_username = None,
        ssh_password = None,
        ssh_path = None
) -> None:

    global server

    server = sshtunnel.SSHTunnelForwarder(
        (ssh_host, ssh_port),
        ssh_host_key=None,
        ssh_username=ssh_username,
        ssh_password=ssh_password,
        ssh_private_key=ssh_path,
        ssh_private_key_password=ssh_password,
        remote_bind_address=('127.0.0.1', 3306))

    return server.start()


def disconnect_ssh() -> None:
    return server.close()


def connect_to_mysql(database_username = None,
                     database_password = None,
                     database_name = None
) -> None:

    global connection

    connection = pymysql.connect(
        host='127.0.0.1',
        user=database_username,
        passwd=database_password,
        db=database_name,
        port=server.local_bind_port
    )


def disconnect_mysql() -> None:
    return connection.close()


def run_mysql_query(sql) -> pd.DataFrame:
    return pd.read_sql_query(sql, connection)