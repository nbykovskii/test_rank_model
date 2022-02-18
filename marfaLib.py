import pandas as pd
import pymysql
import sshtunnel


def connect_to_ssh(
        ssh_host = 'datamart.wake-app.net',
        ssh_port = 22,
        ssh_username = 'nbykovskij',
        ssh_password = 'MooJoob21',
        ssh_path = '~/.ssh/id_ed25519'
):
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


def disconnect_ssh():
    return server.close()


def connect_to_mysql(database_username = 'nbykovskij',
                     database_password = 'PqFu2yH67rnruDESz9Y',
                     database_name = 'bireport_db'
):

    global connection

    connection = pymysql.connect(
        host='127.0.0.1',
        user=database_username,
        passwd=database_password,
        db=database_name,
        port=server.local_bind_port
    )


def disconnect_mysql():
    return connection.close()


def run_mysql_query(sql):
    return pd.read_sql_query(sql, connection)