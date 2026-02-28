from flask import Flask, jsonify, request
import os
import pymysql
from pymysql.err import OperationalError
import logging
from flask_cors import CORS

application = Flask(__name__)
CORS(application)
logging.basicConfig(level=logging.INFO)

#Endpoint: Health Check
@application.route('/health', methods=['GET'])
def health():
    """
    This endpoint is used by the autograder to confirm that the backend deployment is healthy.
    """
    return jsonify({"status": "healthy"}), 200

#Endpoint: Data Insertion
@application.route('/events', methods=['POST'])
def create_event():
    """
    This endpoint should eventually insert data into the database.
    The database communication is currently stubbed out.
    You must implement insert_data_into_db() function to integrate with your MySQL RDS Instance.
    """
    try:
        payload = request.get_json()
        required_fields = ["title", "date"]
        if not payload or not all(field in payload for field in required_fields):
            return jsonify({"error": "Missing required fields: 'title' and 'date'"}), 400

        insert_data_into_db(payload)
        return jsonify({"message": "Event created successfully"}), 201
    except NotImplementedError as nie:
        return jsonify({"error": str(nie)}), 501
    except Exception as e:
        logging.exception("Error occurred during event creation")
        return jsonify({
            "error": "During event creation",
            "detail": str(e)
        }), 500

#Endpoint: Data Retrieval
@application.route('/data', methods=['GET'])
def get_data():
    """
    This endpoint should eventually provide data from the database.
    The database communication is currently stubbed out.
    You must implement the fetch_data_from_db() function to integrate with your MySQL RDS Instance.
    """
    try:
        data = fetch_data_from_db()
        return jsonify({"data": data}), 200
    except NotImplementedError as nie:
        return jsonify({"error": str(nie)}), 501
    except Exception as e:
        logging.exception("Error occurred during data retrieval")
        return jsonify({
            "error": "During data retrieval",
            "detail": str(e)
        }), 500

def get_db_connection():
    """
    Establish and return a connection to the RDS MySQL database.
    The following variables should be added to the Elastic Beanstalk Environment Properties for better security. Follow guidelines for more info.
      - DB_HOST
      - DB_USER
      - DB_PASSWORD
      - DB_NAME
    """
    required_vars = ["DB_HOST", "DB_USER", "DB_PASSWORD", "DB_NAME"]
    missing = [var for var in required_vars if not os.environ.get(var)]
    if missing:
        msg = f"Missing environment variables: {', '.join(missing)}"
        logging.error(msg)
        raise EnvironmentError(msg)
    
    # First try to connect to the specific database
    try:
        connection = pymysql.connect(
            host=os.environ.get("DB_HOST"),
            user=os.environ.get("DB_USER"),
            password=os.environ.get("DB_PASSWORD"),
            db=os.environ.get("DB_NAME")
        )
        return connection
    except OperationalError as e:
        # If database doesn't exist, create it
        if "Unknown database" in str(e):
            logging.info(f"Database {os.environ.get('DB_NAME')} doesn't exist. Creating it...")
            try:
                # Connect without specifying database
                temp_connection = pymysql.connect(
                    host=os.environ.get("DB_HOST"),
                    user=os.environ.get("DB_USER"),
                    password=os.environ.get("DB_PASSWORD")
                )
                with temp_connection.cursor() as cursor:
                    cursor.execute(f"CREATE DATABASE IF NOT EXISTS `{os.environ.get('DB_NAME')}`")
                temp_connection.commit()
                temp_connection.close()
                logging.info(f"Database {os.environ.get('DB_NAME')} created successfully")
                
                # Now connect to the newly created database
                connection = pymysql.connect(
                    host=os.environ.get("DB_HOST"),
                    user=os.environ.get("DB_USER"),
                    password=os.environ.get("DB_PASSWORD"),
                    db=os.environ.get("DB_NAME")
                )
                return connection
            except Exception as create_error:
                logging.error(f"Failed to create database: {create_error}")
                raise ConnectionError(f"Failed to create database: {create_error}")
        else:
            raise ConnectionError(f"Failed to connect to the database: {e}")

def create_db_table():
    connection = get_db_connection()
    try:
        with connection.cursor() as cursor:
            create_table_sql = """
            CREATE TABLE IF NOT EXISTS events (
                id INT AUTO_INCREMENT PRIMARY KEY,
                title VARCHAR(255) NOT NULL,
                description TEXT,
                image_url VARCHAR(255),
                date DATE NOT NULL,
                location VARCHAR(255)
            )
            """
            cursor.execute(create_table_sql)
        connection.commit()
        logging.info("Events table created or already exists")
    except Exception as e:
        logging.exception("Failed to create or verify the events table")
        raise RuntimeError(f"Table creation failed: {str(e)}")
    finally:
        connection.close()

def insert_data_into_db(payload):
    """
    Inserts an event record into the RDS MySQL database.
    Expected payload keys:
      - title (str)
      - description (str)
      - image_url (str)
      - date (YYYY-MM-DD)
      - location (str)
    """
    create_db_table()  # ensure table exists
    connection = get_db_connection()
    try:
        with connection.cursor() as cursor:
            insert_sql = """
                INSERT INTO events (title, description, image_url, date, location)
                VALUES (%s, %s, %s, %s, %s)
            """
            cursor.execute(
                insert_sql,
                (
                    payload.get("title"),
                    payload.get("description"),
                    payload.get("image_url"),
                    payload.get("date"),
                    payload.get("location"),
                ),
            )
        connection.commit()
        logging.info("Event inserted successfully")
    except Exception as e:
        logging.exception("Error inserting data into DB")
        raise RuntimeError(f"Insert failed: {str(e)}")
    finally:
        connection.close()


def fetch_data_from_db():
    """
    Fetches all event records from the database in ascending order of date.
    """
    create_db_table()  # ensure table exists
    connection = get_db_connection()
    try:
        with connection.cursor(pymysql.cursors.DictCursor) as cursor:
            select_sql = """
                SELECT id, title, description, image_url, date, location
                FROM events
                ORDER BY date ASC
            """
            cursor.execute(select_sql)
            rows = cursor.fetchall()
            return rows
    except Exception as e:
        logging.exception("Error fetching data from DB")
        raise RuntimeError(f"Fetch failed: {str(e)}")
    finally:
        connection.close()

if __name__ == '__main__':
    application.run(host='0.0.0.0', port=int(os.environ.get('PORT', 5000)))
