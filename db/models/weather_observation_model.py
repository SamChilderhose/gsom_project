from sqlalchemy import Boolean, CheckConstraint, ForeignKey, Column, Integer, Date, Float, String, Sequence, case, func, literal
from sqlalchemy.sql import text
from sqlalchemy.orm import sessionmaker, declarative_base
from datetime import datetime
from .base import Base

class WeatherObservation(Base):
    __tablename__ = 'weather_observations'

    id = Column(Integer, Sequence('weather_observation_id_seq'), primary_key=True)
    observation_date = Column(Date)
    season = Column(Integer, CheckConstraint('season >= 0 AND season <= 3'))
    precipitation_highest_daily = Column(Float)
    precipitation_total = Column(Float)
    temperature_avg = Column(Float)
    temperature_max = Column(Float)
    temperature_extreme_max = Column(Float)
    temperature_min = Column(Float)
    temperature_extreme_min = Column(Float)
    days_missing = Column(Integer)
    station_id = Column(String(11), ForeignKey('weather_stations.id'), nullable=True)
    created_date = Column(String, default=str(datetime.now()))

    def __init__(self, observation_date: datetime, precipitation_highest_daily:float, precipitation_total:float, 
                 temperature_avg:float, temperature_max: float, temperature_extreme_max: float, 
                 temperature_min:float, temperature_extreme_min:float, days_missing:int, station_id:str):
        self.observation_date = observation_date

        # Logic for assigning season for northern hemisphere
        month = observation_date.month
        if 3 <= month <= 5:
            season = 0
        elif 6 <= month <= 8:
            season = 1
        elif 9 <= month <= 11:
            season = 2
        else:
            season = 3

        self.season = season
        self.precipitation_highest_daily = precipitation_highest_daily
        self.precipitation_total = precipitation_total
        self.temperature_avg = temperature_avg
        self.temperature_max = temperature_max
        self.temperature_extreme_max = temperature_extreme_max
        self.temperature_min = temperature_min
        self.temperature_extreme_min = temperature_extreme_min
        self.days_missing = days_missing
        self.station_id = station_id
    
    @property
    def season_name(self):
        season_names = {0: 'Spring', 1: 'Summer', 2: 'Fall', 3: 'Winter'}
        return season_names.get(self.season, None)

    def __repr__(self):
        return f"WeatherObservation(id={self.id}, observation_date={self.observation_date}, season={self.season}, season_name={self.season_name} precipitation_highest_daily={self.precipitation_highest_daily}, precipitation_total={self.precipitation_total}, temperature_avg={self.temperature_avg}, temperature_max={self.temperature_max}, temperature_extreme_max={self.temperature_extreme_max}, temperature_min={self.temperature_min}, temperature_extreme_min={self.temperature_extreme_min}, days_missing={self.days_missing}, station_id={self.station_id})"
