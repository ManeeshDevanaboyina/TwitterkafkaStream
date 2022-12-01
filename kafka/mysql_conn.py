import mysql.connector
from mysql.connector import Error
import pandas as pd


def create_server_connection(host_name, user_name, user_password):
    connection = None
    try:
        connection = mysql.connector.connect(host=host_name, user=user_name, passwd=user_password)
        print("Database connection has been established")
    except Error as err:
        print(f"Error: '{err}'")
    return connection


# connection= create_server_connection("localhost","root","")

# Creating Database
# db="twitter_data_ingestion"
def create_database(connection, query):
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        print("Database created successfully")
    except Error as err:
        print(f"Error: '{err}'")


# create_database_query="Create database twitter_data_ingestion"
# create_database(connection,create_database_query)

# Connecting to Database

def create_db_connection(host_name, user_name, user_password, db_name):
    connection = None
    try:
        connection = mysql.connector.connect(host=host_name, user=user_name, passwd=user_password, db=db_name)
        print("mySQL Database connection successful")
    except Error as err:
        print(f"Error: '{err}'")
    return connection


create_server_connection("localhost","root","")
create_db_connection("localhost","root","","twitter_data_ingestion")
# Execute SQL queries

def execute_query(connection, query):
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        connection.commit()
        print("Query was Successful")
    except Error as err:
        print(f"Error: '{err}'")


# create_tweetdata_topic1 = """
# create table orders

def read_query(connection, query):
    cursor = connection.cursor()
    result = None
    try:
        cursor.execute(query)
        result = cursor.fetchall()
        return result
    except Error as err:
        print(f"Error: '{err}'")
