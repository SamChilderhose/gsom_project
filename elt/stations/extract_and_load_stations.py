from db.initialize_db import get_db_url
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from db.models.weather_station_model import WeatherStation
from datetime import datetime

def extract(weather_station_data_filepath):
    stations = []

    # Read and parse the data from the file
    with open(weather_station_data_filepath, 'r') as file:
        for line in file:
            line = line.strip()
            data_fields = line.split()

            # Determine the values for creating a WeatherStation instance
            station_id = data_fields[0]
            latitude = float(data_fields[1])
            longitude = float(data_fields[2])
            elevation = float(data_fields[3])
            # country = get_country_from_coordinates(latitude, longitude)
            country = "N/A"

            gsn = None
            code = None
            # Has code
            if data_fields[-1].isnumeric():
                code = int(data_fields[-1])
                # Has GSN and code
                if data_fields[-2] == 'GSN':
                    gsn = 'GSN'
                    name = ' '.join(data_fields[4:-2])
                # Has only code
                else:
                    name = ' '.join(data_fields[4:-1])
            # Has GSN but no code
            elif data_fields[-1] == 'GSN':
                gsn = 'GSN'
                name = ' '.join(data_fields[4:-1])
            # Neither GSN or code
            else:
                name = ' '.join(data_fields[4:])

            # Create a WeatherStation instance and append to the list
            weather_station = WeatherStation(
                id=station_id,
                latitude=latitude,
                longitude=longitude,
                elevation=elevation,
                name=name,
                gsn=gsn,
                code=code,
                country=country
            )
            stations.append(weather_station)

    return stations

def load(stations, session):
    for weather_station in stations:
        session.add(weather_station)
        session.commit()

def extract_and_load_stations():
    weather_station_data_filepath = '../data_to_load/ghcnd-stations.txt'
    engine = create_engine(get_db_url())
    Session = sessionmaker(bind=engine)
    session = Session()

    try:
        stations = extract(weather_station_data_filepath)
        load(stations, session)
    except Exception as e:
        session.rollback()
        print(f"An error occurred while loading station data: {e}")
    finally:
        print("Completed Loading Weather Station Data")
        session.close()
