from elt.observations.extract_observations import extract_weather_data
from elt.observations.load_observations import load_weather_data


def run_observations_elt():
    weather_observations_tar_filepath = "data/gsom-latest.tar.gz"

    for csv_data in extract_weather_data(weather_observations_tar_filepath):
        load_weather_data(csv_data)