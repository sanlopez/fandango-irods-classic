import mysql.connector
import configparser
import os

config = configparser.ConfigParser()
config.read(os.path.join(os.path.dirname(os.path.dirname(__file__)), 'config.yaml'))

ddbb_host = config['DDBB'].get('HOST')
ddbb_user = config['DDBB'].get('USER')
ddbb_name = config['DDBB'].get('DATABASE')
ddbb_pass = config['DDBB'].get('PASS')

def connect_to_ddbb():
    connection = mysql.connector.connect(host=ddbb_host, user=ddbb_user, password=ddbb_pass, database=ddbb_name)
    return connection

def create_ddbb_data():
    connection = None
    try:
        print("Connecting to database...")
        connection = connect_to_ddbb()
        cursor = connection.cursor()
        print("Creating table/s...")
        cursor.execute('''CREATE TABLE IF NOT EXISTS collection (
                            collection_id INT AUTO_INCREMENT,
                            project_id INT NOT NULL,
                            location TEXT NOT NULL,
                            ticket_id TEXT NOT NULL,
                            PRIMARY KEY(collection_id));''')
        connection.commit()
        print('... tables properly created :)')
    except Exception as e:
        print(f'... Problem creating tables!: {e}')
    finally:
        if connection:
            connection.close()


create_ddbb_data()
