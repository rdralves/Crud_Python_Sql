from multiprocessing import connection
import mysql.connector
from mysql.connector import Error
import pandas as pd


def create_db_connection(host, user, password, db):
    connection = None
    try:
        connection = mysql.connector.connect(
            host=host,
            user=user,
            password='rdr3',
            db=db
        )
        print("Mysql DataBase connection succesful")
    except Error as e:
        print(f'Erro: "{e}"')

    return connection


def create_databse(connection, query):
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        print('database created successfully')
    except Error as e:
        print(f"Error: {e}")


def execute_query(connection, query):
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        connection.commit()
        print("Query successfull")
    except Error as e:
        print("Erro: {e}")


def read_query(connection, query):
    cursor = connection.cursor()
    result = None
    try:
        cursor.execute(query)
        result = cursor.fetchall()
        return result
    except Error as err:
        print(f"Error: '{err}'")


def execute_list_query(connection, sql, val):
    cursor = connection.cursor()
    try:
        cursor.executemany(sql, val)
        connection.commit()
        print("Query successful")
    except Error as err:
        print(f"Error: '{err}'")
