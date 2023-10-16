from datetime import datetime
from sqlalchemy import Column, Integer, String, Float, Boolean
from sqlalchemy.orm import declarative_base
from sqlalchemy import CheckConstraint
from sqlalchemy.orm import sessionmaker
from .base import Base

from models.create_database_engine import create_database_engine

# Base = declarative_base()

class WeatherSummary(Base):
    __tablename__ = 'weather_summary'

    id = Column(Integer, primary_key=True)
    year = Column(Integer)
    season = Column(Integer)
    avg_total_precipitation = Column(Float)
    num_precipitation_entries = Column(Integer)
    avg_temperature = Column(Float)
    num_temperature_entries = Column(Integer)
    total_entries = Column(Integer)

    def __init__(self, year:int, season:int, avg_total_precipitation:float, num_precipitation_entries:int, avg_temperature:float, num_temperature_entries:int, total_entries:int):
        self.year = year
        self.season = season
        self.avg_total_precipitation = avg_total_precipitation
        self.num_precipitation_entries = num_precipitation_entries
        self.avg_temperature = avg_temperature
        self.num_temperature_entries = num_temperature_entries
        self.total_entries = total_entries

    def __repr__(self):
        return f"WeatherSummary(year={self.year}, season='{self.season}', avg_total_precipitation={self.avg_total_precipitation}, num_precipitation_entries={self.num_precipitation_entries}, avg_temperature={self.avg_temperature}, num_temperature_entries={self.num_temperature_entries}, total_entries={self.total_entries})"
