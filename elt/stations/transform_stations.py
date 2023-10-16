from db.initialize_db import get_db_url
import geopandas as gpd
from shapely.geometry import Point
from sqlalchemy import create_engine, Column, String, Integer, Float, Boolean
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from datetime import datetime
from db.models.weather_station_model import WeatherStation

# Load the GeoJSON file
world = gpd.read_file(gpd.datasets.get_path('naturalearth_lowres'))

def get_country_from_coordinates(latitude, longitude):
    gdf = gpd.GeoDataFrame(
        geometry=[Point(longitude, latitude)],
        crs=world.crs  # Use the same coordinate reference system as the world GeoDataFrame
    )

    # Perform a spatial join to find the country
    result = gpd.sjoin(gdf, world, how="left")

    if not result.empty:
        country = result.iloc[0]['name']
        return country
    else:
        return "Country not found"

def update_weather_stations_country(session):
    stations = session.query(WeatherStation).all()

    for station in stations:
        country = get_country_from_coordinates(station.latitude, station.longitude)
        station.country = country
        session.commit()

def transform_stations():
    engine = create_engine(get_db_url())
    Session = sessionmaker(bind=engine)
    session = Session()

    try:
        update_weather_stations_country(session)
        print("Countries updated successfully!")
    except Exception as e:
        session.rollback()
        print(f"An error occurred: {e}")
    finally:
        session.close()
