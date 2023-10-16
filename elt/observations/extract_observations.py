import tarfile

def extract_weather_data(tar_filepath):
    with tarfile.open(tar_filepath, 'r:gz') as tar:
        for member in tar.getmembers():
            weather_csv = tar.extractfile(member)
            csv_data = weather_csv.read()  # This reads the compressed data directly
            csv_data = csv_data.decode('utf-8').splitlines()
            yield csv_data