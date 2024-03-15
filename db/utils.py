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

def create_new_collection(project_id, location, ticket_id):
    connection = None
    try:
        print("Connecting to database...")
        connection = connect_to_ddbb()
        cursor = connection.cursor()
        cursor.execute('INSERT INTO collection (project_id, location, ticket_id) VALUES (%s, %s, %s)', (project_id, location, ticket_id))
        connection.commit()
        print(f'... collection created for project wih id {project_id} with ticket id {ticket_id}')
    except Exception as e:
        print(f'... collection could not be created because of: {e}')
    finally:
        if connection:
            connection.close()
