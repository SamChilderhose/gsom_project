from db.initialize_db import get_db_url
from sqlalchemy import create_engine, func
from sqlalchemy.orm import sessionmaker
from db.models.weather_observation_model import WeatherObservation
from db.models.weather_summary_model import WeatherSummary
from datetime import datetime

def create_session():
    engine = create_engine(get_db_url())
    Session = sessionmaker(bind=engine)
    session = Session()
    return session

def clear_weather_summary(session):
    session.query(WeatherSummary).delete()
    session.commit()

def get_season_dates(target_year, target_season):
    season_start_end_map = {
        # 0: SPRING
        0: {
            "START": datetime(target_year, 3, 1),
            "END": datetime(target_year, 5, 31)
        },
        # 1: SUMMER
        1: {
            "START": datetime(target_year, 6, 1),
            "END": datetime(target_year, 8, 31)
        },
        # 2: FALL
        2: {
            "START": datetime(target_year, 9, 1),
            "END": datetime(target_year, 11, 30)
        },
        # 3: WINTER
        3: {
            "START": datetime(target_year, 12, 1),
            "END": datetime(target_year, 2, 28)  # This doesn't handle leap years
        },
    }
    return season_start_end_map[target_season]

def summarize_weather(target_year, target_season, session):
    season_dates = get_season_dates(target_year, target_season)

    avg_temp = func.avg(WeatherObservation.temperature_avg)
    avg_precip = func.avg(WeatherObservation.precipitation_total)

    query = session.query(
        avg_temp,
        avg_precip,
        func.count(WeatherObservation.id),
        func.count(WeatherObservation.temperature_avg).filter(WeatherObservation.temperature_avg.isnot(None)),
        func.count(WeatherObservation.precipitation_total).filter(WeatherObservation.precipitation_total.isnot(None))
    ).filter(
        WeatherObservation.observation_date >= season_dates["START"],
        WeatherObservation.observation_date <= season_dates["END"],
        WeatherObservation.season == target_season
    )

    result = query.first()

    summary_entry = WeatherSummary(
        year=target_year,
        season=target_season,
        avg_total_precipitation=result[1],
        num_precipitation_entries=result[4],
        avg_temperature=result[0],
        num_temperature_entries=result[3],
        total_entries=result[2]
    )

    session.add(summary_entry)
    session.commit()

def run_summarize_weather():
    max_year = 2023
    min_year = 1763
    seasons = [0, 1, 2, 3]

    session = create_session()

    clear_weather_summary(session)

    for target_year in range(min_year, max_year + 1):
        for target_season in seasons:
            summarize_weather(target_year, target_season, session)

    session.close()
