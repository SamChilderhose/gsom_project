import json
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
from .base import Base

def initialize_db():
    try:
        db_url = get_db_url()
        if db_url:
            engine = create_engine(db_url)
            connection = engine.connect()

            # Check if the tables exist
            tables_to_check = ['weather_observations', 'weather_stations', 'weather_summary']
            existing_tables = [table_name for table_name in tables_to_check if not engine.dialect.has_table(connection, table_name)]

            if existing_tables:
                Base.metadata.create_all(bind=engine)
                print("Database initialization successful!")
            else:
                print("All required database tables already exist. Skipping initialization.")

            connection.close()
    except SQLAlchemyError as e:
        print(f"Error initializing the database: {e}")

def get_db_url():
    try:
        with open('config.json', 'r') as config_file:
            config = json.load(config_file)

        # Extract the database connection information
        db_config = config.get("database")

        if db_config:
            user = db_config.get("user")
            password = db_config.get("password")
            host = db_config.get("host")
            port = db_config.get("port")
            name = db_config.get("name")

            if user and password and host and port and name:
                # Reconstruct the database URL
                db_url = f"postgresql://{user}:{password}@{host}:{port}/{name}"
                return db_url
            else:
                print("Incomplete database configuration in the configuration file.")
                return None
        else:
            print("Database configuration not found in the configuration file.")
            return None
    except FileNotFoundError:
        print("Config file 'config.json' not found.")
        return None
