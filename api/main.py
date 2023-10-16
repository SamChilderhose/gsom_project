from typing import List, Optional
from fastapi import FastAPI, HTTPException, Query, Depends
from sqlalchemy import create_engine, select, func, extract, and_
from sqlalchemy.orm import sessionmaker
from db.models.weather_summary_model import WeatherSummary
from db.models.weather_station_model import WeatherStation
from db.models.weather_observation_model import WeatherObservation
from db.initialize_db import get_db_url

app = FastAPI()

# Create a SQLAlchemy database engine
engine = create_engine(get_db_url())

# Create a session factory
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# Dependency to get the current database session
def get_db() -> SessionLocal:
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

@app.get("/average_temp/")
async def get_average_temp(
    season: Optional[int] = None, 
    year: Optional[int] = None,
    db: SessionLocal = Depends(get_db)
):
    """
    Get average temperature and average total precipitation for a specific season and year.
    """

    # Construct the query using the model
    query = db.query(
        WeatherSummary.year,
        WeatherSummary.season,
        WeatherSummary.avg_total_precipitation,
        WeatherSummary.avg_temperature
    )

    # Apply filters for the specified season and year
    if season is not None:
        query = query.filter(WeatherSummary.season == season)
    if year is not None:
        query = query.filter(WeatherSummary.year == year)

    # Execute the query
    results = query.all()

    # Process the results
    data = []
    for row in results:
        year, season, avg_precip, avg_temp = row
        data.append({
            "year": year,
            "season": season,
            "avg_total_precipitation": avg_precip,
            "avg_temperature": avg_temp
        })

    return data

@app.get("/stations_data/")
async def get_stations_data(
    season: Optional[str] = None, 
    year: Optional[int] = None,
    db: SessionLocal = Depends(get_db)
):
    """
    Get weather stations and available datapoints for a specific season and year.
    """
    season_map = {'Spring': 0, 'Summer': 1, 'Fall': 2, 'Winter': 3}
    if season and season not in season_map:
        raise HTTPException(status_code=400, detail="Invalid season name")
    
    query = (
        db.query(
            WeatherStation.id.label("station_id"),
            WeatherStation.name.label("station_name"),
            extract('year', WeatherObservation.observation_date).label("year"),
            WeatherObservation.season,
            func.count(WeatherObservation.id).label("num_observations")
        )
        .join(WeatherObservation, WeatherStation.id == WeatherObservation.station_id)
    )

    if season:
        query = query.filter(WeatherObservation.season == season_map[season])

    if year:
        query = query.filter(extract('year', WeatherObservation.observation_date) == year)

    query = (
        query.group_by(
            WeatherStation.id,
            WeatherStation.name,
            extract('year', WeatherObservation.observation_date),
            WeatherObservation.season
        )
    )
    
    results = query.all()
    
    return [
        {
            "station_id": result.station_id,
            "station_name": result.station_name,
            "year": int(result.year),
            "season": result.season,
            "num_observations": result.num_observations
        }
        for result in results
    ]


@app.get("/area_avg_temp/")
async def get_area_avg_temp(
    lat1: float, lon1: float, 
    lat2: float, lon2: float, 
    startYear: int, endYear: int,
    db: SessionLocal = Depends(get_db)
):
    """
    Get average temperature and available datapoints in a rectangular area 
    defined by (lat1, lon1) and (lat2, lon2), averaged over startYear-endYear range.
    """

    # Ensure coordinates are valid
    if not (-90 <= lat1 <= 90) or not (-90 <= lat2 <= 90) or not (-180 <= lon1 <= 180) or not (-180 <= lon2 <= 180):
        raise HTTPException(status_code=400, detail="Invalid coordinates")
    
    # Define the bounds of latitude and longitude
    min_lat, max_lat = min(lat1, lat2), max(lat1, lat2)
    min_lon, max_lon = min(lon1, lon2), max(lon1, lon2)

    # Define the query to fetch average temperature and count within the area and time range
    avg_temp = func.avg(WeatherObservation.temperature_avg)
    
    query = (
        db.query(
            avg_temp.label("average_temperature"),
            func.count(WeatherObservation.id).label("num_observations")
        )
        .join(WeatherStation, WeatherStation.id == WeatherObservation.station_id)
        .filter(
            and_(
                WeatherStation.latitude.between(min_lat, max_lat),
                WeatherStation.longitude.between(min_lon, max_lon),
                extract('year', WeatherObservation.observation_date).between(startYear, endYear)
            )
        )
    )

    # Execute the query and fetch the result
    result = query.one_or_none()

    if not result:
        raise HTTPException(status_code=404, detail="No data found for the given criteria")

    return {
        "average_temperature": result.average_temperature,
        "num_observations": result.num_observations
    }
