from scripts.datamarts.coordinates import Coordinates
from scripts.utils.csv import Csv
from scripts.utils.file_keys import get_next_normalized_file_key

def create_location_datamart(bucket_name):
    file_key = get_next_normalized_file_key()
    if file_key == None:
        return
        
    file_name = Csv.get_file_name_from_file_key(file_key, prefix="")
    df = Csv.read_local_csv_to_dataframe(file_name, nrows=9999)
    df.reset_index()
    location = Coordinates(df)
    location.create_coordinates_table()
    
    location.dataframe.reset_index()

    file_name = Csv.get_file_name_from_file_key(file_key, prefix='location_')
    file_name = file_name.replace("normalized_", "")
    Csv.write_local_csv_from_dataframe(location.dataframe, file_name)
    Csv.upload_csv_to_s3(bucket_name, file_name, s3_folder='datamart/location/')