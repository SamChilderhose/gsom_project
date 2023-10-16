from sqlalchemy import Boolean, CheckConstraint, ForeignKey, Column, Integer, Date, Float, String, Sequence, case, func, literal
from sqlalchemy.sql import text
from sqlalchemy.orm import sessionmaker, declarative_base
from datetime import datetime
from models.create_database_engine import create_database_engine
from .base import Base

# Base = declarative_base()

class WeatherStation(Base):
    __tablename__ = 'weather_stations'

    id = Column(String(11), primary_key=True)
    latitude = Column(Float, CheckConstraint('latitude >= -90.0 AND latitude <= 90.0'))
    longitude = Column(Float, CheckConstraint('longitude >= -180.0 AND longitude <= 180.0'))
    elevation = Column(Float)
    name = Column(String(50))
    gsn = Column(String(3))
    code = Column(Integer)
    country = Column(String(60))
    is_northern_hemisphere = Column(Boolean, default=True)
    created_date = Column(String, default=str(datetime.now()))

    def __init__(self, id: str, latitude: float, longitude: float, elevation: float, name: str, gsn: str, code: int, country: str):
        self.id = id
        self.latitude = latitude
        self.longitude = longitude
        self.elevation = elevation
        self.name = name
        self.gsn = gsn
        self.code = code
        self.country = country
        self.is_northern_hemisphere = latitude >= 0
    
    def __repr__(self):
        return f"WeatherStation(id={self.id}, latitude={self.latitude}, longitude={self.longitude}, elevation={self.elevation}, name={self.name}, gsn={self.gsn}, code={self.code}, is_northern_hemisphere={self.is_northern_hemisphere})"
