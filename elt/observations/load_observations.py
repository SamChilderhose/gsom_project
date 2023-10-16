import datetime
import csv
from db.initialize_db import get_db_url
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from db.models.weather_observation_model import WeatherObservation

header = [
    "STATION", "DATE", "LATITUDE", "LONGITUDE", "ELEVATION", "NAME",
    "PRCP", "PRCP_ATTRIBUTES", "EMXP", "EMXP_ATTRIBUTES",
    "TAVG", "TAVG_ATTRIBUTES",
    "TMAX", "TMAX_ATTRIBUTES", "TMIN", "TMIN_ATTRIBUTES",
    "EMNT", "EMNT_ATTRIBUTES", "EMXT", "EMXT_ATTRIBUTES"
]

date_format = "%Y-%m"

def load_weather_data(csv_data):
    weather_station_data_filepath = '../data_to_load/ghcnd-stations.txt'
    engine = create_engine(get_db_url())
    Session = sessionmaker(bind=engine)
    session = Session()
    
    for data in csv_data:
        csv_reader = csv.reader(data)

        # Skip the header row
        csv_fields = next(csv_reader)
        field_index_map = {header_name: csv_fields.index(header_name) for header_name in header if header_name in csv_fields}

        for row in csv_reader:
            observation_values = {
                "EMXP": None,
                "PRCP": None,
                "TAVG": None,
                "TMAX": None,
                "EMXT": None,
                "TMIN": None,
                "EMNT": None
            }

            try:
                for field_name, field_index in field_index_map.items():
                    if field_name in observation_values:
                        field_value = row[field_index]
                        if field_value:
                            observation_values[field_name] = float(field_value)

                station_id = row[field_index_map["STATION"]]
                observation_date = datetime.datetime.strptime(row[field_index_map["DATE"]], date_format)
                precipitation_highest_daily = observation_values["EMXP"]
                precipitation_total = observation_values["PRCP"]
                temperature_avg = observation_values["TAVG"]
                temperature_max = observation_values["TMAX"]
                temperature_extreme_max = observation_values["EMXT"]
                temperature_min = observation_values["TMIN"]
                temperature_extreme_min = observation_values["EMNT"]
                days_missing = 0

                observation = WeatherObservation(
                    observation_date=observation_date,
                    precipitation_highest_daily=precipitation_highest_daily,
                    precipitation_total=precipitation_total,
                    temperature_avg=temperature_avg,
                    temperature_max=temperature_max,
                    temperature_extreme_max=temperature_extreme_max,
                    temperature_min=temperature_min,
                    temperature_extreme_min=temperature_extreme_min,
                    days_missing=days_missing,
                    station_id=station_id
                )

                session.add(observation)
            except Exception as e:
                session.rollback()
                print("An error occurred:", e)
                print(observation)
        session.commit()
    session.close()